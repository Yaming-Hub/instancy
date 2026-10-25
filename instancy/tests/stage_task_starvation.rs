//! Regression tests for stage-task starvation in the dataflow executor.
//!
//! The executor groups each worker's operators into stage tasks, split at
//! exchange edges, and runs them in sweeps that share one activation budget.
//! Two scheduler bugs combined into a livelock:
//!
//! 1. An operator blocked on backpressure counted as progress, so its stage
//!    task kept re-running it until the whole sweep budget was used up.
//! 2. When the budget ran out, the remaining stage tasks were skipped, and the
//!    next sweep started from the first task again.
//!
//! Once an exchange channel held about 1024 envelopes (its capacity), the
//! stage task that drains it never ran again and the dataflow spun at 100% CPU
//! forever, even with a single worker. These tests push more envelopes through
//! exchange edges than the channels can hold (or otherwise keep an upstream
//! stage busy), and check that the dataflow completes with every record
//! delivered exactly once.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(60);

/// Runs `f` on a helper thread and panics if it does not finish in time, so a
/// livelock shows up as a test failure instead of a hung test run.
fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(TIMEOUT) {
        Ok(r) => r,
        Err(_) => panic!("{what}: did not finish within {TIMEOUT:?} (livelock?)"),
    }
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn sorted_records<T>(batches: Vec<(T, Vec<u64>)>) -> Vec<u64> {
    let mut all: Vec<u64> = batches.into_iter().flat_map(|(_, d)| d).collect();
    all.sort_unstable();
    all
}

// ---------------------------------------------------------------------------
// Single worker (`spawn`): the exchange is a local channel, but the graph is
// still split into two stage tasks at the exchange edge.
// ---------------------------------------------------------------------------

#[test]
fn single_worker_input_exchange_many_single_record_batches() {
    const N: u64 = 20_000;
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("single-worker-input-exchange");
    builder
        .input::<u64>("data")
        .unwrap()
        .exchange("by_val", |v: &u64| *v)
        .map("id", |_t, v| v)
        .output("out")
        .unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let rx = handle.take_output::<u64>("out").unwrap();
    let tx = handle.take_input::<u64>("data").unwrap();
    let collector = std::thread::spawn(move || sorted_records(rx.collect_data()));
    let feeder = std::thread::spawn(move || {
        for i in 0..N {
            tx.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || handle.join_blocking()).unwrap();
    feeder.join().unwrap();
    assert_eq!(collector.join().unwrap(), (0..N).collect::<Vec<_>>());
}

#[test]
fn single_worker_source_exchange_more_envelopes_than_channel_capacity() {
    const N: u64 = 5_000;
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("single-worker-source-exchange");
    let data: Vec<(u64, Vec<u64>)> = (0..N).map(|i| (0, vec![i])).collect();
    builder
        .source("src", data)
        .map("id", |_t, x| x)
        .exchange_by_hash("x", |x: &u64| *x)
        .output("out")
        .unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let rx = handle.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || sorted_records(rx.collect_data()));
    with_timeout("join", move || handle.join_blocking()).unwrap();
    assert_eq!(collector.join().unwrap(), (0..N).collect::<Vec<_>>());
}

/// An output sink in the first stage task is blocked because nobody reads its
/// receiver yet. The second stage task (behind an exchange) must still run and
/// deliver its records. The consumer reads the second output first, so if the
/// blocked sink starves the second stage, the test never gets its records.
#[test]
fn blocked_output_in_first_stage_does_not_starve_later_stage() {
    // Fits in the blocked branch's buffers (256-event output channel plus a
    // 1024-envelope pipeline edge), so the input itself never blocks.
    const N: u64 = 1_000;
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("blocked-sink-branch");
    let input = builder.input::<u64>("data").unwrap();
    input.clone().output("early").unwrap();
    input
        .exchange("ex", |x: &u64| *x)
        .map("id", |_t, x| x)
        .output("late")
        .unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let early = handle.take_output::<u64>("early").unwrap();
    let late = handle.take_output::<u64>("late").unwrap();
    let tx = handle.take_input::<u64>("data").unwrap();
    let feeder = std::thread::spawn(move || {
        for i in 0..N {
            tx.send(0, vec![i]).unwrap();
        }
    });

    let late_records = with_timeout("read late output", move || {
        let mut got = Vec::new();
        while (got.len() as u64) < N {
            match late.recv_timeout(TIMEOUT) {
                Some(OutputEvent::Data { data, .. }) => got.extend(data),
                Some(_) => {}
                None => break,
            }
        }
        got.sort_unstable();
        (got, late)
    });
    let (late_records, late) = late_records;
    assert_eq!(late_records, (0..N).collect::<Vec<_>>());

    let early_records = sorted_records(early.collect_data());
    assert_eq!(early_records, (0..N).collect::<Vec<_>>());
    with_timeout("join", move || handle.join_blocking()).unwrap();
    feeder.join().unwrap();
    drop(late);
}

/// An upstream stage that reports progress on every activation without the
/// downstream stage running (here: an async operator with queued input,
/// whose in-flight task waits until the downstream stage has seen the
/// previous record) must not starve the downstream stage. The exchange never
/// fills, so this checks that a sweep that runs out of budget gives the
/// skipped stage tasks the first turn next time.
#[test]
fn busy_upstream_stage_does_not_starve_downstream_stage() {
    const N: u64 = 200;
    // `unary_async` runs its tasks on the ambient tokio runtime.
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = tokio_rt.enter();
    let rt = runtime();
    let (seen_tx, seen_rx) = tokio::sync::watch::channel(0u64);
    let seen_tx = Arc::new(seen_tx);
    let builder = DataflowBuilder::<u64>::new("busy-upstream-stage");
    let gate = Arc::new(move |_t: u64, batch: Vec<u64>| {
        let mut seen = seen_rx.clone();
        async move {
            for &x in &batch {
                seen.wait_for(|n| *n >= x)
                    .await
                    .map_err(|e| instancy::Error::Io(std::io::Error::other(e.to_string())))?;
            }
            Ok(batch)
        }
    });
    builder
        .input::<u64>("data")
        .unwrap()
        .unary_async("gate", 1, gate)
        .exchange("ex", |x: &u64| *x)
        .inspect("seen", move |_t, _x| {
            seen_tx.send_modify(|n| *n += 1);
        })
        .output("out")
        .unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let rx = handle.take_output::<u64>("out").unwrap();
    let tx = handle.take_input::<u64>("data").unwrap();
    let collector = std::thread::spawn(move || sorted_records(rx.collect_data()));
    let feeder = std::thread::spawn(move || {
        for i in 0..N {
            tx.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || handle.join_blocking()).unwrap();
    feeder.join().unwrap();
    assert_eq!(collector.join().unwrap(), (0..N).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Multiple workers in one process (`spawn_multi`).
// ---------------------------------------------------------------------------

/// Feeds `records` records from worker 0 in batches of `batch`, through
/// `input -> exchange -> map -> output`, and returns every delivered record.
fn spawn_multi_exchange(workers: usize, records: u64, batch: u64) -> Vec<u64> {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "multi-worker-exchange",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .exchange("by_val", |v: &u64| *v)
                    .map("id", |_t, v| v)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    let mut senders = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
        senders.push(multi.take_input::<u64>(w, "data").unwrap());
    }
    let feeder = std::thread::spawn(move || {
        let mut i = 0;
        while i < records {
            let end = (i + batch).min(records);
            senders[0].send(0, (i..end).collect()).unwrap();
            i = end;
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let mut all: Vec<u64> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .collect();
    all.sort_unstable();
    all
}

#[test]
fn multi_worker_exchange_many_single_record_batches() {
    assert_eq!(
        spawn_multi_exchange(2, 20_000, 1),
        (0..20_000).collect::<Vec<_>>()
    );
}

#[test]
fn three_workers_exchange_many_single_record_batches() {
    assert_eq!(
        spawn_multi_exchange(3, 20_000, 1),
        (0..20_000).collect::<Vec<_>>()
    );
}

/// Control: the same volume in fewer, larger batches (fewer envelopes than
/// the exchange channel holds).
#[test]
fn multi_worker_exchange_large_batches() {
    assert_eq!(
        spawn_multi_exchange(2, 20_000, 100),
        (0..20_000).collect::<Vec<_>>()
    );
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Plain,
    Exchange,
    Notify,
    ExchangeNotify,
}

/// Worker 0 sends `sends` two-record batches spread evenly over `epochs`
/// epochs; the other workers' inputs are closed at once. Returns the number
/// of records that reached the outputs (for the notify shapes, the sum of the
/// per-epoch counts).
fn envelope_volume(workers: usize, shape: Shape, sends: u64, epochs: u64) -> u64 {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "envelope-volume",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                let s = b.input::<u64>("data")?.map("m", |_t, x| x);
                let s = match shape {
                    Shape::Plain | Shape::Notify => s,
                    Shape::Exchange | Shape::ExchangeNotify => s.exchange_by_hash("x", |x| *x),
                };
                let s = match shape {
                    Shape::Plain | Shape::Exchange => s,
                    Shape::Notify | Shape::ExchangeNotify => s.unary_notify("count", {
                        let mut stash: HashMap<u64, u64> = HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                *stash.entry(time).or_default() += data.len() as u64;
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                let n = stash.remove(&time).unwrap_or(0);
                                output.push_vec(time, vec![n]);
                            }
                            Ok(())
                        }
                    }),
                };
                s.output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let sender = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    let outs: Vec<_> = (0..workers)
        .map(|w| {
            let o = multi.take_output::<u64>(w, "out").unwrap();
            std::thread::spawn(move || o.collect_data())
        })
        .collect();
    let per_epoch = sends / epochs;
    let feeder = std::thread::spawn(move || {
        for e in 0..epochs {
            for i in 0..per_epoch {
                sender.send(e, vec![i, i + 1000]).unwrap();
            }
            sender.advance_to(e + 1).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let mut total = 0;
    for o in outs {
        for (_t, d) in o.join().unwrap() {
            total += match shape {
                Shape::Plain | Shape::Exchange => d.len() as u64,
                Shape::Notify | Shape::ExchangeNotify => d.iter().sum::<u64>(),
            };
        }
    }
    total
}

macro_rules! envelope_volume_case {
    ($name:ident, $workers:expr, $shape:expr, $sends:expr, $epochs:expr) => {
        #[test]
        fn $name() {
            assert_eq!(
                envelope_volume($workers, $shape, $sends, $epochs),
                2 * $sends
            );
        }
    };
}

// 1100 envelopes is just over one exchange channel's capacity (1024).
envelope_volume_case!(envelopes_1w_plain_1100, 1, Shape::Plain, 1100, 1);
envelope_volume_case!(envelopes_2w_plain_1100, 2, Shape::Plain, 1100, 1);
envelope_volume_case!(envelopes_1w_exchange_1100, 1, Shape::Exchange, 1100, 1);
envelope_volume_case!(envelopes_2w_exchange_1000, 2, Shape::Exchange, 1000, 1);
envelope_volume_case!(envelopes_2w_exchange_1100, 2, Shape::Exchange, 1100, 1);
envelope_volume_case!(envelopes_2w_notify_1100, 2, Shape::Notify, 1100, 1);
envelope_volume_case!(
    envelopes_2w_exchange_notify_1100,
    2,
    Shape::ExchangeNotify,
    1100,
    1
);
envelope_volume_case!(
    envelopes_2w_exchange_notify_1100_over_100_epochs,
    2,
    Shape::ExchangeNotify,
    1100,
    100
);

/// Worker 0 feeds `batches` batches of `per_batch` records through
/// `input [-> capacity override] -> exchange -> inspect -> output`.
fn exchange_with_capacity(
    workers: usize,
    edge_capacity: Option<usize>,
    batches: u64,
    per_batch: u64,
) {
    let rt = runtime();
    let received = Arc::new(AtomicU64::new(0));
    let r2 = Arc::clone(&received);
    let mut multi = rt
        .spawn_multi(
            "exchange-capacity",
            workers,
            move |b: &mut DataflowBuilder<u64>| -> Result<()> {
                let r3 = Arc::clone(&r2);
                let input = b.input::<u64>("data")?;
                let input = match edge_capacity {
                    Some(c) => input.with_capacity(c)?,
                    None => input,
                };
                input
                    .exchange("ex", |x: &u64| *x)
                    .inspect("count", move |_t, _x| {
                        r3.fetch_add(1, Ordering::Relaxed);
                    })
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut outs = Vec::new();
    let mut feeder = None;
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        outs.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
        let s = multi.take_input::<u64>(w, "data").unwrap();
        if w == 0 {
            feeder = Some(std::thread::spawn(move || {
                for k in 0..batches {
                    s.send(0, (0..per_batch).map(|i| k * per_batch + i).collect())
                        .unwrap();
                }
            }));
        }
    }
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.unwrap().join().unwrap();
    let mut all: Vec<u64> = outs.into_iter().flat_map(|o| o.join().unwrap()).collect();
    all.sort_unstable();
    assert_eq!(all, (0..batches * per_batch).collect::<Vec<_>>());
    assert_eq!(received.load(Ordering::Relaxed), batches * per_batch);
}

#[test]
fn multi_worker_small_exchange_capacity() {
    exchange_with_capacity(2, Some(2), 2_000, 16);
}

#[test]
fn single_worker_small_exchange_capacity() {
    exchange_with_capacity(1, Some(2), 2_000, 16);
}

#[test]
fn multi_worker_default_capacity_burst() {
    exchange_with_capacity(2, None, 20_000, 8);
}

/// A capacity-1 edge in the first stage task, followed by an exchange that
/// sends everything to worker 0. With one slot, the upstream `map` is blocked
/// almost all the time, so the stage task that drains the exchange on the
/// same worker must still get its turn.
fn capacity_before_exchange(capacity: usize) {
    const N: u64 = 50;
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "capacity-before-exchange",
            2,
            move |b: &mut DataflowBuilder<u64>| -> Result<()> {
                b.input::<u64>("data")?
                    .map("id", |_t, x| x)
                    .with_capacity(capacity)?
                    .exchange_by_hash("to-w0", |_x: &u64| 0)
                    .map("id2", |_t, x| x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    drop(multi.take_input::<u64>(1, "data").unwrap());
    let c0 = std::thread::spawn(move || sorted_records(out0.collect_data()));
    let c1 = std::thread::spawn(move || sorted_records(out1.collect_data()));
    let feeder = std::thread::spawn(move || {
        for i in 0..N {
            in0.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    assert_eq!(c0.join().unwrap(), (0..N).collect::<Vec<_>>());
    assert!(c1.join().unwrap().is_empty());
}

#[test]
fn capacity_one_edge_before_exchange() {
    capacity_before_exchange(1);
}

/// Control: same traffic with a default-sized edge.
#[test]
fn default_capacity_edge_before_exchange() {
    capacity_before_exchange(1024);
}

/// Worker 0 sends `total` single-record batches, all routed back to worker 0
/// itself through the exchange (or through a plain `map` if `with_exchange`
/// is false).
fn burst_to_worker_zero(workers: usize, total: u64, with_exchange: bool) {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "burst-to-worker-zero",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                let p = b.input::<u64>("data")?.map("id", |_t, x| x);
                let p = if with_exchange {
                    p.exchange_by_hash("to-w0", |_x: &u64| 0)
                } else {
                    p.map("id-mid", |_t, x| x)
                };
                p.map("id2", |_t, x| x).output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
    }
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    let feeder = std::thread::spawn(move || {
        for i in 0..total {
            in0.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let per_worker: Vec<Vec<u64>> = collectors.into_iter().map(|c| c.join().unwrap()).collect();
    assert_eq!(per_worker[0], (0..total).collect::<Vec<_>>());
    assert!(per_worker[1..].iter().all(Vec::is_empty));
}

#[test]
fn burst_to_self_through_exchange_two_workers() {
    burst_to_worker_zero(2, 5_000, true);
}

#[test]
fn burst_to_self_through_exchange_single_worker() {
    burst_to_worker_zero(1, 5_000, true);
}

/// Control: the same burst with no exchange edge (a single stage task).
#[test]
fn burst_without_exchange() {
    burst_to_worker_zero(1, 5_000, false);
}

/// Control: a burst small enough to fit in the exchange channel.
#[test]
fn small_burst_through_exchange() {
    burst_to_worker_zero(1, 100, true);
}

/// Each batch has one record for worker 0 (the sender itself) and one for
/// worker 1, so the sender's own exchange channel fills up while it is still
/// sending to its peer.
///
/// A worker's `map` can hold records in its pending output (blocked on a full
/// exchange channel), so the peer that drained the channel must not decide the
/// dataflow is complete while those records are pending (audit C1/H1, fixed by
/// port-counted progress).
#[test]
fn exchange_self_channel_full_two_workers() {
    const BATCHES: u64 = 3_000;
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "self-channel-full",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .exchange_by_hash("by_val", |x: &u64| *x)
                    .map("pass", |_t, x| x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let collectors: Vec<_> = (0..2)
        .map(|w| {
            let rx = multi.take_output::<u64>(w, "out").unwrap();
            std::thread::spawn(move || sorted_records(rx.collect_data()))
        })
        .collect();
    drop(multi.take_input::<u64>(1, "data").unwrap());
    let s0 = multi.take_input::<u64>(0, "data").unwrap();
    let feeder = std::thread::spawn(move || {
        for b in 0..BATCHES {
            s0.send(0, vec![2 * b, 2 * b + 1]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let per_worker: Vec<Vec<u64>> = collectors.into_iter().map(|c| c.join().unwrap()).collect();
    assert_eq!(
        per_worker[0],
        (0..BATCHES).map(|b| 2 * b).collect::<Vec<_>>()
    );
    assert_eq!(
        per_worker[1],
        (0..BATCHES).map(|b| 2 * b + 1).collect::<Vec<_>>()
    );
}

/// Three exchanges in a row give each worker four stage tasks. Every record
/// is sent as its own batch and bounces between workers (or back to the
/// same worker) at each hop.
fn exchange_chain(workers: usize, records: u64) {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "exchange-chain",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .exchange_by_hash("hop1", |x: &u64| *x)
                    .map("m1", |_t, x| x)
                    .exchange_by_hash("hop2", |x: &u64| x / 3)
                    .map("m2", |_t, x| x)
                    .exchange_by_hash("hop3-to-w0", |_x: &u64| 0)
                    .map("m3", |_t, x| x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    let mut feeders = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
        let tx = multi.take_input::<u64>(w, "data").unwrap();
        let w = w as u64;
        let n = workers as u64;
        feeders.push(std::thread::spawn(move || {
            for i in (w..records).step_by(n as usize) {
                tx.send(0, vec![i]).unwrap();
            }
        }));
    }
    with_timeout("join", move || multi.join_blocking()).unwrap();
    for f in feeders {
        f.join().unwrap();
    }
    let per_worker: Vec<Vec<u64>> = collectors.into_iter().map(|c| c.join().unwrap()).collect();
    assert_eq!(per_worker[0], (0..records).collect::<Vec<_>>());
    assert!(per_worker[1..].iter().all(Vec::is_empty));
}

#[test]
fn exchange_chain_single_worker() {
    exchange_chain(1, 10_000);
}

#[test]
fn exchange_chain_two_workers() {
    exchange_chain(2, 10_000);
}

#[test]
fn exchange_chain_three_workers() {
    exchange_chain(3, 10_000);
}

// ---------------------------------------------------------------------------
// Adversarial shapes: feedback loops across stage tasks, many stage tasks,
// many workers, merges across stages, eager quiescence, async inputs.
// ---------------------------------------------------------------------------

/// An `iterate` loop with an exchange inside it, so the loop's feedback edge
/// runs from a later stage task back to an earlier one. Every record is its
/// own batch and loops about a dozen times.
fn iterate_with_exchange(workers: usize, records: u64) {
    use instancy::dataflow::dataflow_builder::IterateResult;
    use instancy::order::Product;

    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "iterate-exchange-burst",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                let out = b
                    .input::<(u64, u64)>("data")?
                    .iterate::<u32>("halve-loop", 1u32, |v| {
                        let halved = v
                            .map("halve", |_t: &Product<u64, u32>, (id, x)| (id, x / 2))
                            .exchange_by_hash("spread", |(id, x): &(u64, u64)| id + x);
                        let done = halved.clone().filter("done", |_t, (_, x)| *x == 0);
                        let again = halved.filter("again", |_t, (_, x)| *x > 0);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    });
                out.map("id", |_t, (id, _)| id).output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
    }
    let tx = multi.take_input::<(u64, u64)>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<(u64, u64)>(w, "data").unwrap());
    }
    let feeder = std::thread::spawn(move || {
        for id in 0..records {
            tx.send(0, vec![(id, 1000 + id)]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let mut all: Vec<u64> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (0..records).collect::<Vec<_>>());
}

#[test]
fn iterate_with_exchange_burst_single_worker() {
    iterate_with_exchange(1, 3_000);
}

// With two or more workers, this loop used to hang occasionally because one
// worker declared the whole dataflow complete (and force-closed its
// operators) while another worker still had records in the loop (audit C2,
// fixed by port-counted progress).

#[test]
fn iterate_with_exchange_burst_two_workers() {
    iterate_with_exchange(2, 3_000);
}

#[test]
fn iterate_with_exchange_burst_three_workers() {
    iterate_with_exchange(3, 3_000);
}

/// Eight exchanges in a row: nine stage tasks per worker, so a sweep that
/// runs out of budget can skip many tasks at once.
fn long_exchange_chain(workers: usize, records: u64) {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "long-exchange-chain",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                let mut s = b.input::<u64>("data")?.map("m0", |_t, x| x);
                for hop in 0..8u64 {
                    s = s
                        .exchange_by_hash(format!("hop{hop}"), move |x: &u64| x.wrapping_add(hop))
                        .map(format!("m{}", hop + 1), |_t, x| x);
                }
                s.output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
    }
    let tx = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    let feeder = std::thread::spawn(move || {
        for i in 0..records {
            tx.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    feeder.join().unwrap();
    let mut all: Vec<u64> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (0..records).collect::<Vec<_>>());
}

#[test]
fn long_exchange_chain_single_worker() {
    long_exchange_chain(1, 5_000);
}

#[test]
fn long_exchange_chain_two_workers() {
    long_exchange_chain(2, 5_000);
}

/// Six workers all send single-record batches to each other at once.
#[test]
fn all_to_all_exchange_six_workers() {
    const WORKERS: u64 = 6;
    const PER_WORKER: u64 = 3_000;
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "all-to-all",
            WORKERS as usize,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .map("m", |_t, x| x)
                    .exchange_by_hash("spread", |x: &u64| *x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    let mut feeders = Vec::new();
    for w in 0..WORKERS {
        let rx = multi.take_output::<u64>(w as usize, "out").unwrap();
        collectors.push(std::thread::spawn(move || {
            sorted_records(rx.collect_data())
        }));
        let tx = multi.take_input::<u64>(w as usize, "data").unwrap();
        feeders.push(std::thread::spawn(move || {
            for i in 0..PER_WORKER {
                tx.send(0, vec![i * WORKERS + w]).unwrap();
            }
        }));
    }
    with_timeout("join", move || multi.join_blocking()).unwrap();
    for f in feeders {
        f.join().unwrap();
    }
    for (w, c) in collectors.into_iter().enumerate() {
        let got = c.join().unwrap();
        let want: Vec<u64> = (0..WORKERS * PER_WORKER)
            .filter(|x| x % WORKERS == w as u64)
            .collect();
        assert_eq!(got, want, "worker {w}");
    }
}

/// Two branches of one input, one of them through an exchange, merged with
/// `concat` and exchanged again, so the merge operator's inputs come from
/// different stage tasks.
#[test]
fn concat_of_exchanged_and_local_branches() {
    use instancy::dataflow::dataflow_builder::Pipe;

    const N: u64 = 4_000;
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "concat-across-stages",
            2,
            |b: &mut DataflowBuilder<u64>| {
                let (even, odd) = b
                    .input::<u64>("data")?
                    .map("m", |_t, x| x)
                    .branch("parity", |_t, x| x % 2 == 0);
                let even = even.exchange_by_hash("even-spread", |x: &u64| *x / 2);
                let odd = odd.map("odd-local", |_t, x| x);
                Pipe::concat(vec![even, odd])?
                    .exchange_by_hash("to-w0", |_x: &u64| 0)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let rx0 = multi.take_output::<u64>(0, "out").unwrap();
    let rx1 = multi.take_output::<u64>(1, "out").unwrap();
    let c0 = std::thread::spawn(move || sorted_records(rx0.collect_data()));
    let c1 = std::thread::spawn(move || sorted_records(rx1.collect_data()));
    let tx0 = multi.take_input::<u64>(0, "data").unwrap();
    let tx1 = multi.take_input::<u64>(1, "data").unwrap();
    let f0 = std::thread::spawn(move || {
        for i in 0..N {
            tx0.send(0, vec![2 * i]).unwrap();
        }
    });
    let f1 = std::thread::spawn(move || {
        for i in 0..N {
            tx1.send(0, vec![2 * i + 1]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    f0.join().unwrap();
    f1.join().unwrap();
    assert_eq!(c0.join().unwrap(), (0..2 * N).collect::<Vec<_>>());
    assert!(c1.join().unwrap().is_empty());
}

/// The smallest quiescence window (`max_idle_sweeps(1)`) while stage tasks
/// keep getting blocked: a blocked stage must keep the dataflow alive, and
/// skipping its extra passes must not make the executor look idle.
///
/// One idle sweep must not be enough for worker 0 to complete while worker 1's
/// blocked `map` still holds records (audit C1/H1, fixed by port-counted
/// progress).
#[test]
fn eager_quiescence_with_blocked_stage_tasks() {
    const N: u64 = 5_000;
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "eager-quiescence-burst",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.max_idle_sweeps(1);
                b.input::<u64>("data")?
                    .map("m", |_t, x| x)
                    .exchange_by_hash("to-w0", |_x: &u64| 0)
                    .map("m2", |_t, x| x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let rx0 = multi.take_output::<u64>(0, "out").unwrap();
    let rx1 = multi.take_output::<u64>(1, "out").unwrap();
    let c0 = std::thread::spawn(move || sorted_records(rx0.collect_data()));
    let c1 = std::thread::spawn(move || sorted_records(rx1.collect_data()));
    let tx0 = multi.take_input::<u64>(0, "data").unwrap();
    let tx1 = multi.take_input::<u64>(1, "data").unwrap();
    let f0 = std::thread::spawn(move || {
        for i in 0..N {
            tx0.send(0, vec![i]).unwrap();
        }
    });
    let f1 = std::thread::spawn(move || {
        for i in N..2 * N {
            tx1.send(0, vec![i]).unwrap();
        }
    });
    with_timeout("join", move || multi.join_blocking()).unwrap();
    f0.join().unwrap();
    f1.join().unwrap();
    assert_eq!(c0.join().unwrap(), (0..2 * N).collect::<Vec<_>>());
    assert!(c1.join().unwrap().is_empty());
}

/// Async input path (`IoMode::Async`) feeding a burst through an exchange.
#[test]
fn async_input_burst_through_exchange() {
    const N: u64 = 20_000;
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = tokio_rt.enter();
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("async-input-exchange");
    builder
        .input::<u64>("data")
        .unwrap()
        .exchange("by_val", |v: &u64| *v)
        .map("id", |_t, v| v)
        .output("out")
        .unwrap();
    let mut handle = rt
        .spawn(
            builder.build().unwrap(),
            SpawnOptions::new().io_mode(instancy::IoMode::Async),
        )
        .unwrap();
    let mut rx = handle.take_async_output::<u64>("out").unwrap();
    let tx = handle.take_async_input::<u64>("data").unwrap();
    let records = tokio_rt.block_on(async move {
        let feeder = tokio::spawn(async move {
            for i in 0..N {
                tx.send(0, vec![i]).await.unwrap();
            }
        });
        let mut got = Vec::new();
        let collect = async {
            while let Some(event) = rx.recv().await {
                if let OutputEvent::Data { data, .. } = event {
                    got.extend(data);
                }
            }
        };
        tokio::time::timeout(TIMEOUT, collect)
            .await
            .expect("async output did not finish (livelock?)");
        feeder.await.unwrap();
        got.sort_unstable();
        got
    });
    with_timeout("join", move || handle.join_blocking()).unwrap();
    assert_eq!(records, (0..N).collect::<Vec<_>>());
}
/// A slow output consumer behind a capacity-1 sink edge, in both I/O modes.
/// The consumer pauses for a while mid-stream, so every stage on worker 0
/// ends up blocked on the full sink, then drains it slowly. The executor
/// must keep (or resume) running when the consumer frees space.
fn slow_output_consumer(io_mode: instancy::IoMode, workers: usize) {
    const N: u64 = 2_000;
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = tokio_rt.enter();
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "slow-output-consumer",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .map("m", |_t, x| x)
                    .exchange_by_hash("to-w0", |_x: &u64| 0)
                    .map("m2", |_t, x| x)
                    .with_capacity(1)?
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new().io_mode(io_mode),
        )
        .unwrap();
    let async_io = matches!(io_mode, instancy::IoMode::Async);
    let consume = |got: &mut Vec<u64>, data: Vec<u64>| {
        got.extend(data);
        if got.len() == 500 {
            std::thread::sleep(Duration::from_millis(300));
        } else if got.len() % 50 == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    };
    let collector = if async_io {
        let mut rx = multi.take_async_output::<u64>(0, "out").unwrap();
        let handle = tokio_rt.handle().clone();
        std::thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(event) = handle.block_on(rx.recv()) {
                if let OutputEvent::Data { data, .. } = event {
                    consume(&mut got, data);
                }
            }
            got.sort_unstable();
            got
        })
    } else {
        let rx = multi.take_output::<u64>(0, "out").unwrap();
        std::thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(event) = rx.recv() {
                if let OutputEvent::Data { data, .. } = event {
                    consume(&mut got, data);
                }
            }
            got.sort_unstable();
            got
        })
    };
    let mut others = Vec::new();
    for w in 1..workers {
        if async_io {
            others.push(Box::new(multi.take_async_output::<u64>(w, "out").unwrap())
                as Box<dyn std::any::Any + Send>);
        } else {
            others.push(Box::new(multi.take_output::<u64>(w, "out").unwrap()));
        }
    }
    let feeder = if async_io {
        let txs: Vec<_> = (0..workers)
            .map(|w| multi.take_async_input::<u64>(w, "data").unwrap())
            .collect();
        tokio_rt.spawn(async move {
            for i in 0..N {
                txs[(i as usize) % txs.len()]
                    .send(0, vec![i])
                    .await
                    .unwrap();
            }
        });
        None
    } else {
        let txs: Vec<_> = (0..workers)
            .map(|w| multi.take_input::<u64>(w, "data").unwrap())
            .collect();
        Some(std::thread::spawn(move || {
            for i in 0..N {
                txs[(i as usize) % txs.len()].send(0, vec![i]).unwrap();
            }
        }))
    };
    with_timeout("join", move || multi.join_blocking()).unwrap();
    if let Some(f) = feeder {
        f.join().unwrap();
    }
    assert_eq!(collector.join().unwrap(), (0..N).collect::<Vec<_>>());
    drop(others);
}

#[test]
fn slow_output_consumer_sync_single_worker() {
    slow_output_consumer(instancy::IoMode::Sync, 1);
}

#[test]
fn slow_output_consumer_sync_two_workers() {
    slow_output_consumer(instancy::IoMode::Sync, 2);
}

#[test]
fn slow_output_consumer_async_single_worker() {
    slow_output_consumer(instancy::IoMode::Async, 1);
}

#[test]
fn slow_output_consumer_async_two_workers() {
    slow_output_consumer(instancy::IoMode::Async, 2);
}

// ---------------------------------------------------------------------------
// Small exchange channels (1, 2 and 4 slots) with every routing pattern.
// Any full channel, not only a 1024-envelope one, could trip the livelock.
// Batches hold one record each, so every push has a single target worker.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Route {
    /// Back to the producing worker.
    ToSelf,
    /// To the next worker (wrapping).
    ToNext,
    /// Spread by record index.
    Spread,
    /// Everything to worker 0.
    ToZero,
    /// Never to worker 0.
    AwayFromZero,
}

fn producer_of(x: &u64) -> u64 {
    x >> 32
}

fn index_of(x: &u64) -> u64 {
    x & 0xffff_ffff
}

/// How a small-capacity run ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finish {
    /// Keep every input open until every record has reached an output, then
    /// cancel. With inputs open, no worker can declare the dataflow complete,
    /// so this checks only that the backlog drains.
    Delivery,
    /// Close every input and join the dataflow.
    Join,
}

/// Every worker sends `PER_WORKER` single-record batches through two
/// exchanges whose channels hold `capacity` envelopes: the first routed by
/// `route`, the second spread by index.
fn small_capacity_exchange(workers: usize, capacity: usize, route: Route, finish: Finish) {
    const PER_WORKER: u64 = 400;
    let n = workers as u64;
    let what = format!("workers={workers} capacity={capacity} route={route:?} {finish:?}");
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "small-capacity-exchange",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("data")?
                    .map("pre", |_t, x| x)
                    .with_capacity(capacity)?
                    .exchange_by_hash("hop1", move |x: &u64| match route {
                        Route::ToSelf => producer_of(x),
                        Route::ToNext => producer_of(x) + 1,
                        Route::Spread => index_of(x),
                        Route::ToZero => 0,
                        Route::AwayFromZero => 1 + index_of(x) % (n - 1),
                    })
                    .map("mid", |_t, x| x)
                    .with_capacity(capacity)?
                    .exchange_by_hash("hop2", |x: &u64| index_of(x) / 7)
                    .map("post", |_t, x| x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let total = n * PER_WORKER;
    let delivered = Arc::new(AtomicU64::new(0));
    let mut collectors = Vec::new();
    let mut feeders = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        let delivered = Arc::clone(&delivered);
        collectors.push(std::thread::spawn(move || {
            let mut got = Vec::new();
            match finish {
                Finish::Join => got.extend(rx.collect_data().into_iter().flat_map(|(_, d)| d)),
                Finish::Delivery => {
                    let deadline = std::time::Instant::now() + TIMEOUT;
                    while delivered.load(Ordering::SeqCst) < total
                        && std::time::Instant::now() < deadline
                    {
                        if let Some(OutputEvent::Data { data, .. }) =
                            rx.recv_timeout(Duration::from_millis(20))
                        {
                            delivered.fetch_add(data.len() as u64, Ordering::SeqCst);
                            got.extend(data);
                        }
                    }
                }
            }
            got.sort_unstable();
            (got, rx)
        }));
        let tx = multi.take_input::<u64>(w, "data").unwrap();
        let g = w as u64;
        feeders.push(std::thread::spawn(move || {
            for i in 0..PER_WORKER {
                tx.send(i % 3, vec![(g << 32) | i]).unwrap();
            }
            (finish == Finish::Delivery).then_some(tx)
        }));
    }
    let multi = match finish {
        Finish::Join => {
            with_timeout(&what, move || multi.join_blocking()).unwrap();
            None
        }
        Finish::Delivery => Some(multi),
    };
    // Check the outputs before joining the feeders: if the dataflow is stuck,
    // the feeders are blocked on full input channels.
    let mut all = Vec::new();
    let mut outputs = Vec::new();
    for c in collectors {
        let (got, rx) = c.join().unwrap();
        all.extend(got);
        outputs.push(rx);
    }
    all.sort_unstable();
    let mut expected: Vec<u64> = (0..n)
        .flat_map(|g| (0..PER_WORKER).map(move |i| (g << 32) | i))
        .collect();
    expected.sort_unstable();
    assert_eq!(all.len(), expected.len(), "{what}: record count");
    assert!(all == expected, "{what}: records lost or duplicated");
    let open_inputs: Vec<_> = feeders.into_iter().map(|f| f.join().unwrap()).collect();
    // Dropping the handle cancels a dataflow that is still running.
    drop(multi);
    drop(open_inputs);
    drop(outputs);
}

fn small_capacity_matrix(route: Route, finish: Finish) {
    let min_workers = if matches!(route, Route::AwayFromZero) {
        2
    } else {
        1
    };
    for workers in min_workers..=4 {
        for capacity in [1, 2, 4] {
            small_capacity_exchange(workers, capacity, route, finish);
        }
    }
}

#[test]
fn small_capacity_exchange_to_self() {
    small_capacity_matrix(Route::ToSelf, Finish::Delivery);
}

#[test]
fn small_capacity_exchange_to_next_worker() {
    small_capacity_matrix(Route::ToNext, Finish::Delivery);
}

#[test]
fn small_capacity_exchange_spread() {
    small_capacity_matrix(Route::Spread, Finish::Delivery);
}

#[test]
fn small_capacity_exchange_to_worker_zero() {
    small_capacity_matrix(Route::ToZero, Finish::Delivery);
}

#[test]
fn small_capacity_exchange_away_from_worker_zero() {
    small_capacity_matrix(Route::AwayFromZero, Finish::Delivery);
}

/// Same matrix, also waiting for the dataflow to complete. With two or more
/// workers (every time with four workers all routing to worker 0) this used
/// to fail: a worker declared the dataflow complete while operators blocked on
/// full channels still held records (audit C1/H1, fixed by port-counted
/// progress).
#[test]
fn small_capacity_exchange_then_complete() {
    for route in [
        Route::ToSelf,
        Route::ToNext,
        Route::Spread,
        Route::ToZero,
        Route::AwayFromZero,
    ] {
        small_capacity_matrix(route, Finish::Join);
    }
}
