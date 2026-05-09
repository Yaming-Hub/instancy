//! Comparative benchmarks for Instancy and Timely.
//!
//! Each benchmark iteration builds the dataflow graph and runs it to completion in both
//! frameworks. Instancy reuses a pre-created async runtime across iterations, while Timely
//! recreates its execution context per iteration; for single-worker cases this uses
//! `execute_directly`, which is lightweight and avoids thread spawning. Inputs are fed to both
//! frameworks in batch form. Q5 (`Q5_pagerank_batch`) intentionally uses a shared sequential
//! PageRank implementation instead of either framework's native iteration operators so both sides
//! execute the same batch-style algorithm.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
enum JoinEvent {
    Left { order_key: u64, revenue: i64 },
    Right { order_key: u64 },
}

fn build_runtime(worker_threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads,
        ..Default::default()
    })
    .unwrap()
}

fn partition_round_robin<T: Clone>(items: &[T], workers: usize) -> Vec<Vec<T>> {
    let mut partitions = vec![Vec::new(); workers];
    for (idx, item) in items.iter().cloned().enumerate() {
        partitions[idx % workers].push(item);
    }
    partitions
}

fn build_join_inputs(
    items: &[data::LineItem],
    workers: usize,
) -> (Vec<Vec<(u64, i64)>>, Vec<Vec<u64>>, Vec<Vec<JoinEvent>>) {
    let left: Vec<(u64, i64)> = items
        .iter()
        .filter(|item| item.ship_date < 11_200)
        .map(|item| (item.order_key, line_revenue(item)))
        .collect();
    let right: Vec<u64> = items
        .iter()
        .filter(|item| item.quantity >= 25)
        .map(|item| item.order_key)
        .collect();

    let left_partitions = partition_round_robin(&left, workers);
    let right_partitions = partition_round_robin(&right, workers);
    let event_partitions = left_partitions
        .iter()
        .zip(right_partitions.iter())
        .map(|(left_part, right_part)| {
            let mut events = Vec::with_capacity(left_part.len() + right_part.len());
            events.extend(
                left_part
                    .iter()
                    .cloned()
                    .map(|(order_key, revenue)| JoinEvent::Left { order_key, revenue }),
            );
            events.extend(
                right_part
                    .iter()
                    .copied()
                    .map(|order_key| JoinEvent::Right { order_key }),
            );
            events
        })
        .collect();

    (left_partitions, right_partitions, event_partitions)
}

fn make_small_batches(iterations: u64, batch_size: u64) -> Vec<(u64, Vec<u64>)> {
    (0..iterations)
        .map(|time| {
            let base = time * batch_size;
            let batch = (0..batch_size).map(|offset| base + offset).collect();
            (time, batch)
        })
        .collect()
}

fn line_revenue(item: &data::LineItem) -> i64 {
    item.price * item.discount
}

fn timely_process_config(template: &timely::Config) -> timely::Config {
    match template.communication {
        timely::CommunicationConfig::Process(threads) => timely::Config::process(threads),
        _ => unreachable!("benchmark templates use timely::Config::process"),
    }
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

fn instancy_q1(rt: &RuntimeHandle, items: &[data::LineItem], cutoff: u64) {
    let builder = DataflowBuilder::<u64>::new("q1-instancy");
    builder
        .source("src", vec![(0, items.to_vec())])
        .filter("date_filter", move |_t, item| item.ship_date < cutoff)
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

fn timely_q1(items: &[data::LineItem], cutoff: u64) {
    let items = items.to_vec();
    timely::execute_directly(move |worker| {
        // `execute_directly` includes lightweight per-iteration worker setup. Instancy also
        // measures dataflow build/spawn/join inside the iteration, so both sides include setup.
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::channels::pact::Pipeline;
            use timely::dataflow::operators::generic::Operator;
            use timely::dataflow::operators::{Filter, Input, Inspect, Probe};

            let (input, stream) = scope.new_input::<data::LineItem>();
            let probe = stream
                .filter(move |item| item.ship_date < cutoff)
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

        let mut batch = items;
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    });
}

fn instancy_q2(
    rt: &RuntimeHandle,
    left_partitions: &[Vec<(u64, i64)>],
    right_partitions: &[Vec<u64>],
    workers: usize,
) {
    let mut handle = rt
        .spawn_multi(
            "q2-instancy",
            workers,
            |builder| {
                let left = builder
                    .input::<(u64, i64)>("left").unwrap()
                    .exchange_by_hash("left_exchange", |(order_key, _)| *order_key);
                let right = builder
                    .input::<u64>("right").unwrap()
                    .exchange_by_hash("right_exchange", |order_key| *order_key);

                left.binary::<u64, (u64, i64), _>(right, "join", {
                    let mut left_state: HashMap<u64, Vec<i64>> = HashMap::new();
                    let mut right_counts: HashMap<u64, usize> = HashMap::new();
                    move |left_in, right_in, output| {
                        while let Some((time, data)) = left_in.next() {
                            let mut matched = Vec::new();
                            for (order_key, revenue) in data {
                                if let Some(count) = right_counts.get(&order_key) {
                                    for _ in 0..*count {
                                        matched.push((order_key, revenue));
                                    }
                                }
                                left_state.entry(order_key).or_default().push(revenue);
                            }
                            if !matched.is_empty() {
                                output.push_vec(time, matched);
                            }
                        }

                        while let Some((time, data)) = right_in.next() {
                            let mut matched = Vec::new();
                            for order_key in data {
                                if let Some(revenues) = left_state.get(&order_key) {
                                    matched.extend(
                                        revenues
                                            .iter()
                                            .copied()
                                            .map(|revenue| (order_key, revenue)),
                                    );
                                }
                                *right_counts.entry(order_key).or_default() += 1;
                            }
                            if !matched.is_empty() {
                                output.push_vec(time, matched);
                            }
                        }
                        Ok(())
                    }
                })
                .unary_notify::<(u64, i64), _>("sum_revenue", {
                    let mut sums: HashMap<u64, i64> = HashMap::new();
                    move |input, output, ctx| {
                        while let Some((time, data)) = input.next() {
                            for (order_key, revenue) in data {
                                *sums.entry(order_key).or_default() += revenue;
                            }
                            ctx.notify_at(time);
                        }
                        while let Some(time) = ctx.next_notification() {
                            let results: Vec<_> = sums.drain().collect();
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
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let left_senders = handle.take_all_inputs::<(u64, i64)>("left").unwrap();
    let right_senders = handle.take_all_inputs::<u64>("right").unwrap();

    for (sender, left_batch) in left_senders.into_iter().zip(left_partitions.iter()) {
        if !left_batch.is_empty() {
            sender.send(0, left_batch.clone()).unwrap();
        }
        sender.close();
    }

    for (sender, right_batch) in right_senders.into_iter().zip(right_partitions.iter()) {
        if !right_batch.is_empty() {
            sender.send(0, right_batch.clone()).unwrap();
        }
        sender.close();
    }

    handle.join_blocking().unwrap();
}

fn timely_q2(event_partitions: &[Vec<JoinEvent>], config: &timely::Config) {
    let event_partitions = Arc::new(event_partitions.to_vec());
    timely::execute(timely_process_config(config), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};

        let worker_index = worker.index();
        let partitions = Arc::clone(&event_partitions);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<JoinEvent>();
            let probe = stream
                .exchange(|event| match event {
                    JoinEvent::Left { order_key, .. } | JoinEvent::Right { order_key } => {
                        *order_key
                    }
                })
                .unary_notify(Pipeline, "join", None, {
                    let mut left_state: HashMap<u64, Vec<i64>> = HashMap::new();
                    let mut right_counts: HashMap<u64, usize> = HashMap::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            let mut matched = Vec::new();
                            for event in data.iter().cloned() {
                                match event {
                                    JoinEvent::Left { order_key, revenue } => {
                                        if let Some(count) = right_counts.get(&order_key) {
                                            for _ in 0..*count {
                                                matched.push((order_key, revenue));
                                            }
                                        }
                                        left_state.entry(order_key).or_default().push(revenue);
                                    }
                                    JoinEvent::Right { order_key } => {
                                        if let Some(revenues) = left_state.get(&order_key) {
                                            matched.extend(
                                                revenues
                                                    .iter()
                                                    .copied()
                                                    .map(|revenue| (order_key, revenue)),
                                            );
                                        }
                                        *right_counts.entry(order_key).or_default() += 1;
                                    }
                                }
                            }
                            if !matched.is_empty() {
                                output.session(&time).give_vec(&mut matched);
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|_, _, _| {});
                    }
                })
                .unary_notify(Pipeline, "sum_revenue", None, {
                    let mut sums: HashMap<u64, i64> = HashMap::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            for (order_key, revenue) in data.iter() {
                                *sums.entry(*order_key).or_default() += *revenue;
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results: Vec<_> = sums.drain().collect();
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

        let mut batch = partitions[worker_index].clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

fn instancy_q3(
    rt: &RuntimeHandle,
    items: &[data::LineItem],
    min_ship: u64,
    max_ship: u64,
    min_discount: i64,
    max_discount: i64,
    qty_threshold: i64,
) {
    let builder = DataflowBuilder::<u64>::new("q3-instancy");
    builder
        .source("src", vec![(0, items.to_vec())])
        .filter("q6_filter", move |_t, item| {
            item.ship_date >= min_ship
                && item.ship_date < max_ship
                && item.discount >= min_discount
                && item.discount <= max_discount
                && item.quantity < qty_threshold
        })
        .map("revenue", |_t, item| line_revenue(&item))
        .reduce("sum", |a, b| a + b)
        .for_each("sink", |_t, value| {
            black_box(value);
        });

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();
}

fn timely_q3(
    items: &[data::LineItem],
    min_ship: u64,
    max_ship: u64,
    min_discount: i64,
    max_discount: i64,
    qty_threshold: i64,
) {
    let items = items.to_vec();
    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::channels::pact::Pipeline;
            use timely::dataflow::operators::generic::Operator;
            use timely::dataflow::operators::{Filter, Input, Inspect, Map, Probe};

            let (input, stream) = scope.new_input::<data::LineItem>();
            let probe = stream
                .filter(move |item| {
                    item.ship_date >= min_ship
                        && item.ship_date < max_ship
                        && item.discount >= min_discount
                        && item.discount <= max_discount
                        && item.quantity < qty_threshold
                })
                .map(|item| line_revenue(&item))
                .unary_notify(Pipeline, "sum", None, {
                    let mut total = 0i64;
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            for value in data.iter() {
                                total += *value;
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results = vec![total];
                            output.session(&time).give_vec(&mut results);
                            total = 0;
                        });
                    }
                })
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = items;
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    });
}

fn instancy_q4(rt: &RuntimeHandle, values: &[i64]) {
    let builder = DataflowBuilder::<u64>::new("q4-instancy");
    let mut pipe = builder.source("src", vec![(0, values.to_vec())]);
    for idx in 0..10 {
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

fn timely_q4(values: &[i64]) {
    let values = values.to_vec();
    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::{Input, Inspect, Map, Probe};

            let (input, mut stream) = scope.new_input::<i64>();
            for _ in 0..10 {
                stream = stream.map(|value| value + 1);
            }
            let probe = stream
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = values;
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    });
}

fn instancy_q5(rt: &RuntimeHandle, edges: &[data::Edge], num_vertices: u64) {
    let builder = DataflowBuilder::<u64>::new("q5-instancy");
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
                    let mut results = compute_pagerank(&buffered, num_vertices, 10);
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

fn timely_q5(edges: &[data::Edge], num_vertices: u64) {
    let edges = edges.to_vec();
    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::channels::pact::Pipeline;
            use timely::dataflow::operators::generic::Operator;
            use timely::dataflow::operators::{Input, Inspect, Probe};

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
                            let mut results = compute_pagerank(&buffered, num_vertices, 10);
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

        let mut batch = edges;
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    });
}

fn instancy_q6(rt: &RuntimeHandle, batches: &[(u64, Vec<u64>)], threshold: u64) {
    let builder = DataflowBuilder::<u64>::new("q6-instancy");
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

fn timely_q6(batches: &[(u64, Vec<u64>)], threshold: u64) {
    let batches = batches.to_vec();
    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::{Filter, Input, Inspect, Probe};

            let (input, stream) = scope.new_input::<u64>();
            let probe = stream
                .filter(move |value| *value > threshold)
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        for (time, batch) in batches {
            input.advance_to(time);
            let mut batch = batch;
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
    });
}

fn instancy_q7(rt: &RuntimeHandle, partitions: &[Vec<u64>], workers: usize) {
    let mut handle = rt
        .spawn_multi(
            "q7-instancy",
            workers,
            |builder| {
                builder
                    .input::<u64>("data").unwrap()
                    .exchange_by_hash("route", |value| *value)
                    .reduce("sum", |a, b| a + b)
                    .for_each("sink", |_t, value| {
                        black_box(value);
                    });
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let senders = handle.take_all_inputs::<u64>("data").unwrap();
    for (sender, batch) in senders.into_iter().zip(partitions.iter()) {
        if !batch.is_empty() {
            sender.send(0, batch.clone()).unwrap();
        }
        sender.close();
    }
    handle.join_blocking().unwrap();
}

fn timely_q7(partitions: &[Vec<u64>], config: &timely::Config) {
    let partitions = Arc::new(partitions.to_vec());
    timely::execute(timely_process_config(config), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};

        let worker_index = worker.index();
        let partitions = Arc::clone(&partitions);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<u64>();
            let probe = stream
                .exchange(|value| *value)
                .unary_notify(Pipeline, "sum", None, {
                    let mut total = 0u64;
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            for value in data.iter() {
                                total += *value;
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results = vec![total];
                            output.session(&time).give_vec(&mut results);
                            total = 0;
                        });
                    }
                })
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        let mut batch = partitions[worker_index].clone();
        input.send_batch(&mut batch);
        input.close();
        worker.step_while(|| !probe.done());
    })
    .unwrap();
}

fn bench_q1(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(2);

    let mut group = c.benchmark_group("Q1_scan_filter_agg");
    for &count in &[10_000u64, 100_000, 1_000_000, 10_000_000] {
        let items = data::generate_lineitems(count as usize);
        let cutoff = 11_000u64;
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_q1(&instancy_rt, &items, cutoff))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_q1(&items, cutoff))
        });
    }
    group.finish();
}

fn bench_q2(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(4);

    let mut group = c.benchmark_group("Q2_two_way_join");
    for &count in &[10_000u64, 100_000] {
        let items = data::generate_lineitems(count as usize);
        group.throughput(Throughput::Elements(count));
        for &workers in &[2usize, 4] {
            let (left_partitions, right_partitions, event_partitions) =
                build_join_inputs(&items, workers);
            let timely_config = timely::Config::process(workers);
            group.bench_with_input(
                BenchmarkId::new(format!("instancy_{workers}w"), count),
                &count,
                |b, _| {
                    b.iter(|| {
                        instancy_q2(&instancy_rt, &left_partitions, &right_partitions, workers)
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("timely_{workers}w"), count),
                &count,
                |b, _| b.iter(|| timely_q2(&event_partitions, &timely_config)),
            );
        }
    }
    group.finish();
}

fn bench_q3(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(2);

    let mut group = c.benchmark_group("Q3_multistage_pipeline");
    for &count in &[10_000u64, 100_000, 1_000_000, 10_000_000] {
        let items = data::generate_lineitems(count as usize);
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_q3(&instancy_rt, &items, 10_500, 11_500, 3, 7, 24))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_q3(&items, 10_500, 11_500, 3, 7, 24))
        });
    }
    group.finish();
}

fn bench_q4(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(2);

    let mut group = c.benchmark_group("Q4_map_chain");
    for &count in &[1_000u64, 10_000, 100_000, 1_000_000] {
        let values: Vec<i64> = (0..count as i64).collect();
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::new("instancy", count), &count, |b, _| {
            b.iter(|| instancy_q4(&instancy_rt, &values))
        });
        group.bench_with_input(BenchmarkId::new("timely", count), &count, |b, _| {
            b.iter(|| timely_q4(&values))
        });
    }
    group.finish();
}

fn bench_q5(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(2);

    let mut group = c.benchmark_group("Q5_pagerank_batch");
    for &(vertices, edges_count) in &[(1_000u64, 10_000usize), (10_000, 100_000)] {
        let edges = data::generate_graph(vertices, edges_count);
        group.throughput(Throughput::Elements(edges_count as u64));
        group.bench_with_input(
            BenchmarkId::new("instancy", edges_count),
            &edges_count,
            |b, _| b.iter(|| instancy_q5(&instancy_rt, &edges, vertices)),
        );
        group.bench_with_input(
            BenchmarkId::new("timely", edges_count),
            &edges_count,
            |b, _| b.iter(|| timely_q5(&edges, vertices)),
        );
    }
    group.finish();
}

fn bench_q6(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(2);

    let mut group = c.benchmark_group("Q6_high_rps_filter");
    for &(iterations, batch_size) in &[(256u64, 64u64), (1024, 64)] {
        let batches = make_small_batches(iterations, batch_size);
        let total = iterations * batch_size;
        let threshold = total / 2;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::new("instancy", total), &total, |b, _| {
            b.iter(|| instancy_q6(&instancy_rt, &batches, threshold))
        });
        group.bench_with_input(BenchmarkId::new("timely", total), &total, |b, _| {
            b.iter(|| timely_q6(&batches, threshold))
        });
    }
    group.finish();
}

fn bench_q7(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();
    let instancy_rt = build_runtime(4);

    let mut group = c.benchmark_group("Q7_exchange_reduce");
    for &count in &[10_000u64, 100_000, 1_000_000] {
        let values: Vec<u64> = (0..count).collect();
        group.throughput(Throughput::Elements(count));
        for &workers in &[2usize, 4] {
            let partitions = partition_round_robin(&values, workers);
            let timely_config = timely::Config::process(workers);
            group.bench_with_input(
                BenchmarkId::new(format!("instancy_{workers}w"), count),
                &count,
                |b, _| b.iter(|| instancy_q7(&instancy_rt, &partitions, workers)),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("timely_{workers}w"), count),
                &count,
                |b, _| b.iter(|| timely_q7(&partitions, &timely_config)),
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches, bench_q1, bench_q2, bench_q3, bench_q4, bench_q5, bench_q6, bench_q7
);
criterion_main!(benches);
