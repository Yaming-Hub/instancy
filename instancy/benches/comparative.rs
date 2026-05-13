//! Comparative benchmarks  5 scenarios matching the sustained cross-process benchmark.
//!
//! Both frameworks use identical worker counts for fair comparison. Timely uses
//! `Config::process(N)` (not `execute_directly`) so both sides pay thread-management
//! overhead. Scenarios have no cross-node exchange  single-process only.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use tokio::runtime::Runtime;

mod data {
    use serde::{Deserialize, Serialize};

    /// TPC-H lineitem-like record
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LineItem {
        pub order_key: u64,
        pub part_key: u64,
        pub quantity: i64,
        pub price: i64,
        pub discount: i64,
        pub tax: i64,
        pub ship_date: u64,
        pub return_flag: u8,
        pub line_status: u8,
    }

    /// Graph edge for iterative benchmarks
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Edge {
        pub src: u64,
        pub dst: u64,
    }

    /// Generate `count` LineItem records with deterministic pseudo-random data.
    pub fn generate_lineitems(count: usize) -> Vec<LineItem> {
        let mut seed: u64 = 42;
        let mut next = || -> u64 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            seed
        };
        (0..count)
            .map(|_| LineItem {
                order_key: next() % 1_500_000,
                part_key: next() % 200_000,
                quantity: (next() % 50 + 1) as i64,
                price: (next() % 100_000 + 100) as i64,
                discount: (next() % 11) as i64,
                tax: (next() % 9) as i64,
                ship_date: 10_000 + (next() % 2_500),
                return_flag: (next() % 3) as u8,
                line_status: (next() % 2) as u8,
            })
            .collect()
    }

    /// Generate a graph with `num_vertices` nodes and `num_edges` random edges.
    pub fn generate_graph(num_vertices: u64, num_edges: usize) -> Vec<Edge> {
        let mut seed: u64 = 123;
        let mut next = || -> u64 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            seed
        };
        (0..num_edges)
            .map(|_| Edge {
                src: next() % num_vertices,
                dst: next() % num_vertices,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAP_CHAIN_STAGES: usize = 20;
const PAGERANK_ITERATIONS: usize = 10;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_runtime(worker_threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads,
        ..Default::default()
    })
    .unwrap()
}

fn compute_pagerank(edges: &[data::Edge], num_vertices: u64, iterations: usize) -> Vec<(u64, f64)> {
    let n = num_vertices as usize;
    let mut adjacency = vec![Vec::new(); n];
    for edge in edges {
        adjacency[edge.src as usize].push(edge.dst);
    }

    let base_rank = 1.0 / n as f64;
    let mut ranks = vec![base_rank; n];

    for _ in 0..iterations {
        let dangling_sum: f64 = adjacency
            .iter()
            .enumerate()
            .filter(|(_, outs)| outs.is_empty())
            .map(|(src, _)| ranks[src])
            .sum();
        let dangling_contrib = dangling_sum * 0.85 / n as f64;
        let teleport = (1.0 - 0.85) / n as f64;
        let mut next = vec![teleport + dangling_contrib; n];

        for (src, outs) in adjacency.iter().enumerate() {
            if outs.is_empty() {
                continue;
            }
            let share = ranks[src] * 0.85 / outs.len() as f64;
            for &dst in outs {
                next[dst as usize] += share;
            }
        }

        ranks = next;
    }

    ranks
        .into_iter()
        .enumerate()
        .map(|(idx, rank)| (idx as u64, rank))
        .collect()
}

fn make_multi_epoch_batches(num_epochs: u64, batch_size: u64) -> Vec<(u64, Vec<u64>)> {
    (0..num_epochs)
        .map(|epoch| {
            let base = epoch * batch_size;
            let batch: Vec<u64> = (base..base + batch_size).collect();
            (epoch, batch)
        })
        .collect()
}

// =============================================================================
// Scenario 1: ScanFilterAgg  filter by ship_date, aggregate by (flag, status)
// =============================================================================

fn instancy_scan_filter_agg(rt: &RuntimeHandle, items: &[data::LineItem]) {
    let builder = DataflowBuilder::<u64>::new("scan-filter-agg");
    builder
        .source("src", vec![(0, items.to_vec())])
        .filter("date_filter", |_t, item| item.ship_date < 11_000)
        .unary_notify::<((u8, u8), (i64, i64)), _>("aggregate", {
            let mut groups: HashMap<(u8, u8), (i64, i64)> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    for item in data {
                        let key = (item.return_flag, item.line_status);
                        let entry = groups.entry(key).or_default();
                        entry.0 += item.quantity;
                        entry.1 += item.price;
                    }
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    let results: Vec<_> = groups.drain().collect();
                    if !results.is_empty() {
                        output.push_vec(time, results);
                    }
                }
                Ok(())
            }
        })
        .for_each("sink", |_t, value| {
            black_box(value);
        });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_scan_filter_agg(items: &[data::LineItem], config: &timely::Config) {
    let items = Arc::new(items.to_vec());
    timely::execute(timely_config_clone(config), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Filter, Input, Inspect, Probe};

        let items = Arc::clone(&items);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<data::LineItem>();
            let probe = stream
                .filter(|item| item.ship_date < 11_000)
                .unary_notify(Pipeline, "aggregate", None, {
                    let mut groups: HashMap<(u8, u8), (i64, i64)> = HashMap::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            for item in data.iter() {
                                let key = (item.return_flag, item.line_status);
                                let entry = groups.entry(key).or_default();
                                entry.0 += item.quantity;
                                entry.1 += item.price;
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results: Vec<_> = groups.drain().collect();
                            if !results.is_empty() {
                                output.session(&time).give_vec(&mut results);
                            }
                        });
                    }
                })
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = (*items).clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

// =============================================================================
// Scenario 2: PageRank  batch PageRank computation
// =============================================================================

fn instancy_pagerank(rt: &RuntimeHandle, edges: &[data::Edge], num_vertices: u64) {
    let builder = DataflowBuilder::<u64>::new("pagerank");
    builder
        .source("src", vec![(0, edges.to_vec())])
        .unary_notify::<(u64, f64), _>("pagerank", {
            let mut buffered = Vec::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    buffered.extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    let mut results =
                        compute_pagerank(&buffered, num_vertices, PAGERANK_ITERATIONS);
                    if !results.is_empty() {
                        output.push_vec(time, std::mem::take(&mut results));
                    }
                    buffered.clear();
                }
                Ok(())
            }
        })
        .for_each("sink", |_t, value| {
            black_box(value);
        });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_pagerank(edges: &[data::Edge], num_vertices: u64, config: &timely::Config) {
    let edges = Arc::new(edges.to_vec());
    timely::execute(timely_config_clone(config), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Input, Inspect, Probe};

        let edges = Arc::clone(&edges);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<data::Edge>();
            let probe = stream
                .unary_notify(Pipeline, "pagerank", None, {
                    let mut buffered = Vec::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            buffered.extend(data.iter().cloned());
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results =
                                compute_pagerank(&buffered, num_vertices, PAGERANK_ITERATIONS);
                            if !results.is_empty() {
                                output.session(&time).give_vec(&mut results);
                            }
                            buffered.clear();
                        });
                    }
                })
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = (*edges).clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

// =============================================================================
// Scenario 3: MapChain  20-stage map(+1) chain
// =============================================================================

fn instancy_map_chain(rt: &RuntimeHandle, values: &[i64]) {
    let builder = DataflowBuilder::<u64>::new("map-chain");
    let mut pipe = builder.source("src", vec![(0, values.to_vec())]);
    for idx in 0..MAP_CHAIN_STAGES {
        pipe = pipe.map(format!("step_{idx}"), |_t, value| value + 1);
    }
    pipe.for_each("sink", |_t, value| {
        black_box(value);
    });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_map_chain(values: &[i64], config: &timely::Config) {
    let values = Arc::new(values.to_vec());
    timely::execute(timely_config_clone(config), move |worker| {
        use timely::dataflow::operators::{Input, Inspect, Map, Probe};

        let values = Arc::clone(&values);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, mut stream) = scope.new_input::<i64>();
            for _ in 0..MAP_CHAIN_STAGES {
                stream = stream.map(|value| value + 1);
            }
            let probe = stream
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = (*values).clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

// =============================================================================
// Scenario 4: MultiEpoch  filter across multiple epochs
// =============================================================================

fn instancy_multi_epoch(rt: &RuntimeHandle, batches: &[(u64, Vec<u64>)], threshold: u64) {
    let builder = DataflowBuilder::<u64>::new("multi-epoch");
    builder
        .source("src", batches.to_vec())
        .filter("threshold", move |_t, value| *value > threshold)
        .for_each("sink", |_t, value| {
            black_box(value);
        });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_multi_epoch(batches: &[(u64, Vec<u64>)], threshold: u64, config: &timely::Config) {
    let batches = Arc::new(batches.to_vec());
    timely::execute(timely_config_clone(config), move |worker| {
        use timely::dataflow::operators::{Filter, Input, Inspect, Probe};

        let batches = Arc::clone(&batches);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<u64>();
            let probe = stream
                .filter(move |value| *value > threshold)
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        for (time, batch) in batches.iter() {
            input.advance_to(*time);
            let mut batch = batch.clone();
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

// =============================================================================
// Scenario 5: SmallPipeline  3-stage map(+1, *2, -1) chain
// =============================================================================

fn instancy_small_pipeline(rt: &RuntimeHandle, values: &[i64]) {
    let builder = DataflowBuilder::<u64>::new("small-pipeline");
    builder
        .source("src", vec![(0, values.to_vec())])
        .map("add1", |_t, value| value + 1)
        .map("mul2", |_t, value| value * 2)
        .map("sub1", |_t, value| value - 1)
        .for_each("sink", |_t, value| {
            black_box(value);
        });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_small_pipeline(values: &[i64], config: &timely::Config) {
    let values = Arc::new(values.to_vec());
    timely::execute(timely_config_clone(config), move |worker| {
        use timely::dataflow::operators::{Input, Inspect, Map, Probe};

        let values = Arc::clone(&values);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<i64>();
            let probe = stream
                .map(|value| value + 1)
                .map(|value| value * 2)
                .map(|value| value - 1)
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = (*values).clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

// =============================================================================
// Timely config helper
// =============================================================================

fn timely_config_clone(template: &timely::Config) -> timely::Config {
    match template.communication {
        timely::CommunicationConfig::Process(threads) => timely::Config::process(threads),
        _ => unreachable!("benchmark templates use timely::Config::process"),
    }
}

// =============================================================================
// Criterion benchmark groups
// =============================================================================

fn bench_scan_filter_agg(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(1);
    let timely_cfg = timely::Config::process(1);

    let mut group = c.benchmark_group("ScanFilterAgg");
    for &count in &[100_000u64, 1_000_000, 10_000_000] {
        let items = data::generate_lineitems(count as usize);
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_scan_filter_agg(&instancy_rt, &items))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_scan_filter_agg(&items, &timely_cfg))
        });
    }
    group.finish();
}

fn bench_pagerank(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(1);
    let timely_cfg = timely::Config::process(1);

    let mut group = c.benchmark_group("PageRank");
    for &(vertices, edge_count) in &[(1_000u64, 10_000usize), (10_000, 100_000)] {
        let edges = data::generate_graph(vertices, edge_count);
        group.throughput(Throughput::Elements(edge_count as u64));
        group.bench_with_input(
            BenchmarkId::new("instancy", edge_count),
            &edge_count,
            |b, _| b.iter(|| instancy_pagerank(&instancy_rt, &edges, vertices)),
        );
        group.bench_with_input(
            BenchmarkId::new("timely", edge_count),
            &edge_count,
            |b, _| b.iter(|| timely_pagerank(&edges, vertices, &timely_cfg)),
        );
    }
    group.finish();
}

fn bench_map_chain(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(1);
    let timely_cfg = timely::Config::process(1);

    let mut group = c.benchmark_group("MapChain20");
    for &count in &[10_000u64, 100_000, 1_000_000] {
        let values: Vec<i64> = (0..count as i64).collect();
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_map_chain(&instancy_rt, &values))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_map_chain(&values, &timely_cfg))
        });
    }
    group.finish();
}

fn bench_multi_epoch(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(1);
    let timely_cfg = timely::Config::process(1);

    let mut group = c.benchmark_group("MultiEpoch");
    for &(epochs, batch_size) in &[(16u64, 256u64), (16, 4096)] {
        let batches = make_multi_epoch_batches(epochs, batch_size);
        let total = epochs * batch_size;
        let threshold = total / 2;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::new("instancy", total), &total, |b, _| {
            b.iter(|| instancy_multi_epoch(&instancy_rt, &batches, threshold))
        });
        group.bench_with_input(BenchmarkId::new("timely", total), &total, |b, _| {
            b.iter(|| timely_multi_epoch(&batches, threshold, &timely_cfg))
        });
    }
    group.finish();
}

fn bench_small_pipeline(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(1);
    let timely_cfg = timely::Config::process(1);

    let mut group = c.benchmark_group("SmallPipeline");
    for &count in &[1_000u64, 10_000, 100_000] {
        let values: Vec<i64> = (0..count as i64).collect();
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_small_pipeline(&instancy_rt, &values))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_small_pipeline(&values, &timely_cfg))
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_scan_filter_agg,
    bench_pagerank,
    bench_map_chain,
    bench_multi_epoch,
    bench_small_pipeline
);
criterion_main!(benches);
