//! Dataflow throughput and latency benchmarks.
//!
//! Measures core dataflow execution performance:
//! - Pipeline throughput (map chain, single worker)
//! - Multi-worker exchange throughput
//! - Dataflow spawn latency
//! - Stateful operator throughput (unary, reduce)
//! - Branching (branch + merge) throughput
//! - Staged exchange (stages with different parallelism) throughput
//!
//! **Note:** Each iteration includes graph construction + execution. The
//! `DataflowBuilder` creates a new graph per iteration because `LogicalDataflow`
//! is consumed on spawn. This matches real-world usage patterns.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use instancy::{DataflowBuilder, IoMode, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Exercises the external input path, including one-element batches where
/// per-batch validation overhead is least amortized.
fn bench_input_frontier(c: &mut Criterion) {
    let tokio_rt = Runtime::new().unwrap();
    let _guard = tokio_rt.enter();
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let mut group = c.benchmark_group("input_frontier");
    let batches = 16_384u64;
    for batch_size in [1, 1024] {
        let batch = vec![1u64; batch_size];
        group.throughput(Throughput::Elements(batches * batch_size as u64));
        for io_mode in [IoMode::Sync, IoMode::Async] {
            for advance in [false, true] {
                let backend = if matches!(io_mode, IoMode::Sync) {
                    "sync"
                } else {
                    "async"
                };
                let progress = if advance {
                    "advance_each_batch"
                } else {
                    "data_only"
                };
                group.bench_function(
                    BenchmarkId::new(format!("{backend}_{progress}"), batch_size),
                    |b| {
                        b.iter(|| {
                            let builder = DataflowBuilder::<u64>::new("input-frontier");
                            builder.input::<u64>("data").unwrap().for_each_batch(
                                "sink",
                                |_t, data| {
                                    black_box(data);
                                },
                            );
                            let mut handle = rt
                                .spawn(
                                    builder.build().unwrap(),
                                    SpawnOptions::new().io_mode(io_mode),
                                )
                                .unwrap();
                            match io_mode {
                                IoMode::Sync => {
                                    let sender = handle.take_input::<u64>("data").unwrap();
                                    for time in 0..batches {
                                        sender.send(time, batch.clone()).unwrap();
                                        if advance {
                                            sender.advance_to(time + 1).unwrap();
                                        }
                                    }
                                    drop(sender);
                                    handle.join_blocking().unwrap();
                                }
                                IoMode::Async => tokio_rt.block_on(async {
                                    let sender = handle.take_async_input::<u64>("data").unwrap();
                                    for time in 0..batches {
                                        sender.send(time, batch.clone()).await.unwrap();
                                        if advance {
                                            sender.advance_to(time + 1).await.unwrap();
                                        }
                                    }
                                    drop(sender);
                                    handle.join().await.unwrap();
                                }),
                            }
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

/// Cap worker threads at the machine's available parallelism to avoid
/// oversubscription noise in benchmarks.
fn max_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

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
                        .for_each("sink", |_t, v| {
                            black_box(v);
                        });

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
                    pipe.for_each("sink", |_t, v| {
                        black_box(v);
                    });

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

    let max_w = max_workers();
    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: max_w,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("exchange_throughput");

    for &element_count in &[1_000u64, 10_000, 100_000] {
        // Each worker processes element_count elements independently via
        // spawn_multi, so filter worker counts that exceed available cores.
        for &num_workers in &[1usize, 2, 4, 8, 16] {
            if num_workers > max_w {
                continue;
            }
            // Total elements = element_count * num_workers (each worker
            // gets its own copy of the source data).
            let total = element_count * num_workers as u64;
            group.throughput(Throughput::Elements(total));
            group.bench_with_input(
                BenchmarkId::new(format!("workers_{num_workers}"), element_count),
                &element_count,
                |b, &count| {
                    b.iter(|| {
                        let handle = instancy_rt
                            .spawn_multi(
                                "bench-exchange",
                                num_workers,
                                move |builder| {
                                    let data: Vec<(u64, Vec<i64>)> =
                                        vec![(0, (0..count as i64).collect())];
                                    builder
                                        .source("src", data)
                                        .exchange("partition", |v: &i64| *v as u64)
                                        .map("inc", |_t, x| x + 1)
                                        .for_each("sink", |_t, v| {
                                            black_box(v);
                                        });
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
                .for_each("sink", |_t, v| {
                    black_box(v);
                });

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
                .for_each("sink", |_t, v| {
                    black_box(v);
                });

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
                    |builder| {
                        builder.source("src", vec![(0u64, vec![1i32])]).for_each(
                            "sink",
                            |_t, v| {
                                black_box(v);
                            },
                        );
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
                        .for_each("sink", |_t, v| {
                            black_box(v);
                        });

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
                        .for_each("sink", |_t, v| {
                            black_box(v);
                        });

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
                    let (evens, odds) = builder
                        .source("src", data)
                        .branch("split", |_t, v| v % 2 == 0);
                    let merged = evens
                        .map("double_even", |_t, x| x * 2)
                        .merge(odds.map("triple_odd", |_t, x| x * 3))
                        .unwrap();
                    merged.for_each("sink", |_t, v| {
                        black_box(v);
                    });

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
// Worker scaling: exchange-heavy throughput vs worker count
// ---------------------------------------------------------------------------

/// Measures how exchange throughput scales with worker count.
///
/// Uses a fixed large batch size per worker to highlight Mutex<VecDeque>
/// contention in bounded exchange channels. Each worker emits the same
/// volume — reporting throughput as elements_per_worker/time so that
/// per-worker degradation from contention is directly visible.
fn bench_worker_scaling(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let max_w = max_workers();
    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: max_w,
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("worker_scaling");
    let elements_per_worker: u64 = 50_000;
    // Report per-worker throughput so contention is directly visible:
    // ideal scaling shows constant throughput; contention shows decline.
    group.throughput(Throughput::Elements(elements_per_worker));

    for &num_workers in &[1usize, 2, 4, 8, 16] {
        if num_workers > max_w {
            continue;
        }
        group.bench_with_input(
            BenchmarkId::new("exchange_per_worker", num_workers),
            &num_workers,
            |b, &nw| {
                b.iter(|| {
                    let handle = instancy_rt
                        .spawn_multi(
                            "bench-scaling",
                            nw,
                            move |builder| {
                                let data: Vec<(u64, Vec<i64>)> =
                                    vec![(0, (0..elements_per_worker as i64).collect())];
                                builder
                                    .source("src", data)
                                    .exchange("repartition", |v: &i64| *v as u64)
                                    .map("work", |_t, x| black_box(x.wrapping_mul(7)))
                                    .exchange("reshuffle", |v: &i64| (*v as u64).wrapping_add(1))
                                    .for_each("sink", |_t, v| {
                                        black_box(v);
                                    });
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

    group.finish();
}

// ---------------------------------------------------------------------------
// Staged exchange: stages with different parallelism
// ---------------------------------------------------------------------------

/// An input on 2 workers feeding a stage of 4 workers through `exchange_to`:
/// the per-stage parallelism path. Every run checks that every record was
/// delivered.
fn bench_staged_exchange(c: &mut Criterion) {
    let rt_tokio = Runtime::new().unwrap();
    let _guard = rt_tokio.enter();

    let instancy_rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: max_workers().min(4),
        ..Default::default()
    })
    .unwrap();

    let mut group = c.benchmark_group("staged_exchange");
    let input_workers = 2usize;
    let batch_size = 1024usize;

    for &element_count in &[10_000u64, 100_000] {
        group.throughput(Throughput::Elements(element_count));
        group.bench_with_input(
            BenchmarkId::new("input_2_to_4", element_count),
            &element_count,
            |b, &count| {
                b.iter(|| {
                    let mut multi = instancy_rt
                        .spawn_multi(
                            "bench-staged",
                            input_workers,
                            |builder: &mut DataflowBuilder<u64>| {
                                builder
                                    .input::<i64>("in")?
                                    .map("inc", |_t, x| x + 1)
                                    .exchange_to("to-4", 4, |v: &i64| *v as u64)?
                                    .map("double", |_t, x| x * 2)
                                    .output("out")?;
                                Ok(())
                            },
                            SpawnOptions::default(),
                        )
                        .unwrap();
                    let collectors: Vec<_> = multi
                        .take_all_outputs::<i64>("out")
                        .unwrap()
                        .into_iter()
                        .map(|receiver| {
                            std::thread::spawn(move || {
                                let mut records = 0usize;
                                while let Some(event) = receiver.recv() {
                                    if let instancy::dataflow::OutputEvent::Data { data, .. } =
                                        event
                                    {
                                        records += data.len();
                                    }
                                }
                                records
                            })
                        })
                        .collect();
                    let mut senders = multi.take_all_inputs::<i64>("in").unwrap();
                    senders.truncate(input_workers);
                    let per_worker = count as usize / input_workers;
                    for (w, sender) in senders.iter().enumerate() {
                        let start = (w * per_worker) as i64;
                        let values: Vec<i64> = (start..start + per_worker as i64).collect();
                        for batch in values.chunks(batch_size) {
                            sender.send(0, batch.to_vec()).unwrap();
                        }
                    }
                    drop(senders);
                    multi.join_blocking().unwrap();
                    let delivered: usize = collectors.into_iter().map(|c| c.join().unwrap()).sum();
                    assert_eq!(delivered, per_worker * input_workers, "records lost");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_input_frontier,
    bench_pipeline_throughput,
    bench_exchange_throughput,
    bench_spawn_latency,
    bench_stateful_operator,
    bench_branching,
    bench_worker_scaling,
    bench_staged_exchange,
);
criterion_main!(benches);
