//! Dataflow throughput and latency benchmarks.
//!
//! Measures core dataflow execution performance:
//! - Pipeline throughput (map chain, single worker)
//! - Multi-worker exchange throughput
//! - Dataflow spawn latency
//! - Stateful operator throughput (unary, reduce)
//! - Branching (branch + merge) throughput
//!
//! **Note:** Each iteration includes graph construction + execution. The
//! `DataflowBuilder` creates a new graph per iteration because `LogicalDataflow`
//! is consumed on spawn. This matches real-world usage patterns.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

// ---------------------------------------------------------------------------
// Pipeline throughput: source → N maps → output (single worker)
// ---------------------------------------------------------------------------

fn bench_pipeline_throughput(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("pipeline_throughput");

    for &element_count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(element_count));
        group.bench_with_input(
            BenchmarkId::new("map_chain_3", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let builder = DataflowBuilder::<u64>::new("bench-pipeline");
                    let data: Vec<(u64, Vec<i64>)> = vec![(0, (0..count as i64).collect())];
                    builder
                        .source("src", data)
                        .map("add1", |_t, x| x + 1)
                        .map("mul2", |_t, x| x * 2)
                        .map("sub1", |_t, x| x - 1)
                        .for_each("sink", |_t, v| { black_box(v); });

                    let dataflow = builder.build().unwrap();
                    instancy_rt
                        .spawn(dataflow, SpawnOptions::default())
                        .unwrap()
                        .join_blocking()
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("map_chain_10", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let builder = DataflowBuilder::<u64>::new("bench-pipeline-10");
                    let data: Vec<(u64, Vec<i64>)> = vec![(0, (0..count as i64).collect())];
                    let mut pipe = builder.source("src", data);
                    for i in 0..10 {
                        pipe = pipe.map(format!("step{i}"), |_t, x| x + 1);
                    }
                    pipe.for_each("sink", |_t, v| { black_box(v); });

                    let dataflow = builder.build().unwrap();
                    instancy_rt
                        .spawn(dataflow, SpawnOptions::default())
                        .unwrap()
                        .join_blocking()
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Multi-worker exchange throughput
// ---------------------------------------------------------------------------

fn bench_exchange_throughput(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("exchange_throughput");

    for &element_count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(element_count));

        for &num_workers in &[2usize, 4] {
            group.bench_with_input(
                BenchmarkId::new(format!("workers_{num_workers}"), element_count),
                &element_count,
                |b, &count| {
                    b.iter(|| {
                        let handle = instancy_rt
                            .spawn_multi(
                                "bench-exchange",
                                num_workers,
                                move |_worker_idx, builder| {
                                    let data: Vec<(u64, Vec<i64>)> =
                                        vec![(0, (0..count as i64).collect())];
                                    builder
                                        .source("src", data)
                                        .exchange("partition", |v: &i64| *v as u64)
                                        .map("inc", |_t, x| x + 1)
                                        .for_each("sink", |_t, v| { black_box(v); });
                                    Ok(())
                                },
                                SpawnOptions::default(),
                            )
                            .unwrap();

                        handle.join_blocking().unwrap();
                    });
                },
            );
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Spawn latency: how fast we can create + complete a trivial dataflow
// ---------------------------------------------------------------------------

fn bench_spawn_latency(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("spawn_latency");

    group.bench_function("trivial_dataflow", |b| {
        b.iter(|| {
            let builder = DataflowBuilder::<u64>::new("bench-spawn");
            builder
                .source("src", vec![(0u64, vec![1i32])])
                .for_each("sink", |_t, v| { black_box(v); });

            let dataflow = builder.build().unwrap();
            instancy_rt
                .spawn(dataflow, SpawnOptions::default())
                .unwrap()
                .join_blocking()
                .unwrap();
        });
    });

    group.bench_function("empty_dataflow", |b| {
        b.iter(|| {
            let builder = DataflowBuilder::<u64>::new("bench-empty");
            builder
                .source::<i32>("src", vec![])
                .for_each("sink", |_t, v| { black_box(v); });

            let dataflow = builder.build().unwrap();
            instancy_rt
                .spawn(dataflow, SpawnOptions::default())
                .unwrap()
                .join_blocking()
                .unwrap();
        });
    });

    group.bench_function("multi_worker_2", |b| {
        b.iter(|| {
            let handle = instancy_rt
                .spawn_multi(
                    "bench-multi",
                    2,
                    |_worker_idx, builder| {
                        builder
                            .source("src", vec![(0u64, vec![1i32])])
                            .for_each("sink", |_t, v| { black_box(v); });
                        Ok(())
                    },
                    SpawnOptions::default(),
                )
                .unwrap();

            handle.join_blocking().unwrap();
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Stateful operator (unary) throughput — measures per-element overhead
// ---------------------------------------------------------------------------

fn bench_stateful_operator(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("stateful_operator");

    for &element_count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(element_count));

        group.bench_with_input(
            BenchmarkId::new("unary_running_sum", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let builder = DataflowBuilder::<u64>::new("bench-unary");
                    let data: Vec<(u64, Vec<i64>)> = vec![(0, (0..count as i64).collect())];
                    builder
                        .source("src", data)
                        .unary::<i64, _>("running_sum", {
                            let mut sum: i64 = 0;
                            move |input, output| {
                                while let Some((time, batch)) = input.next() {
                                    let results: Vec<i64> = batch
                                        .iter()
                                        .map(|&v| {
                                            sum += v;
                                            sum
                                        })
                                        .collect();
                                    output.push_vec(time, results);
                                }
                                Ok(())
                            }
                        })
                        .for_each("sink", |_t, v| { black_box(v); });

                    let dataflow = builder.build().unwrap();
                    instancy_rt
                        .spawn(dataflow, SpawnOptions::default())
                        .unwrap()
                        .join_blocking()
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("reduce_sum", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let builder = DataflowBuilder::<u64>::new("bench-reduce");
                    let data: Vec<(u64, Vec<i64>)> = vec![(0, (0..count as i64).collect())];
                    builder
                        .source("src", data)
                        .reduce("sum", |a, b| a + b)
                        .for_each("sink", |_t, v| { black_box(v); });

                    let dataflow = builder.build().unwrap();
                    instancy_rt
                        .spawn(dataflow, SpawnOptions::default())
                        .unwrap()
                        .join_blocking()
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Branching throughput: branch + merge overhead
// ---------------------------------------------------------------------------

fn bench_branching(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("branching");

    for &element_count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(element_count));

        group.bench_with_input(
            BenchmarkId::new("branch_merge", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let builder = DataflowBuilder::<u64>::new("bench-branch");
                    let data: Vec<(u64, Vec<i64>)> = vec![(0, (0..count as i64).collect())];
                    let (evens, odds) =
                        builder.source("src", data).branch("split", |_t, v| v % 2 == 0);
                    let merged = evens
                        .map("double_even", |_t, x| x * 2)
                        .merge(odds.map("triple_odd", |_t, x| x * 3));
                    merged.for_each("sink", |_t, v| { black_box(v); });

                    let dataflow = builder.build().unwrap();
                    instancy_rt
                        .spawn(dataflow, SpawnOptions::default())
                        .unwrap()
                        .join_blocking()
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_pipeline_throughput,
    bench_exchange_throughput,
    bench_spawn_latency,
    bench_stateful_operator,
    bench_branching,
);
criterion_main!(benches);
