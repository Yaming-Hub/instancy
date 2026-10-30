//! Capabilities and their justification (V3): work an operator keeps across
//! logic calls is covered by a capability, and every output and capability
//! request must be justified by the call that makes it.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use instancy::dataflow::{OutputEvent, Retained};
use instancy::progress::frontier::Antichain;
use instancy::{
    DataflowBuilder, IterateResult, OutputReceiver, Pipe, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

/// An operator's input frontier and the records it still held, at the end
/// of each of its calls.
type HeldLog = Arc<Mutex<Vec<(Antichain<u64>, usize)>>>;

/// Each release of held records: their time and the input frontier then.
type ReleaseLog = Arc<Mutex<Vec<(u64, Antichain<u64>)>>>;

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig::default()).unwrap()
}

/// Run `f` on another thread and fail the test if it takes over 30 s.
fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what}: did not finish within 30s"))
}

/// Counts the records of each time once the input frontier passes it.
fn count_per_time(input: Pipe<u64, u64>) -> Pipe<u64, u64> {
    input.unary_notify("count", {
        let mut counts: HashMap<u64, u64> = HashMap::new();
        move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                *counts.entry(time).or_default() += data.len() as u64;
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(count) = counts.remove(&time) {
                    output.push_vec(time, vec![count]);
                }
            }
            Ok(())
        }
    })
}

/// Build `body` on `input` and run it with `batches` sent to the input.
/// Returns the join result and the output's batches.
fn run_single(
    name: &str,
    body: impl FnOnce(Pipe<u64, u64>) -> Pipe<u64, u64>,
    batches: Vec<(u64, Vec<u64>)>,
) -> (Result<()>, Vec<(u64, Vec<u64>)>) {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new(name);
    body(builder.input::<u64>("in").unwrap())
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    for (time, data) in batches {
        sender.send(time, data).unwrap();
    }
    drop(sender);
    let result = with_timeout(name, move || spawned.join_blocking());
    (result, collector.join().unwrap())
}

fn assert_unjustified(result: Result<()>) {
    let error = result.expect_err("the unjustified operator must fail the dataflow");
    assert!(
        error.to_string().contains("unjustified"),
        "expected a V3 error, got: {error}"
    );
}

// ---------------------------------------------------------------------------
// V3 rejections
// ---------------------------------------------------------------------------

#[test]
fn output_at_a_time_the_call_does_not_justify_fails_the_dataflow() {
    let (result, _) = run_single(
        "v3-late-output",
        |input| {
            input.unary("late", |input, output| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time - 1, data);
                }
                Ok(())
            })
        },
        vec![(5, vec![1])],
    );
    assert_unjustified(result);
}

/// The progress checker records the rejection as V3, at the operator's
/// output, without panicking: the production error already fails the run.
#[cfg(any(debug_assertions, feature = "progress-check"))]
#[test]
fn the_checker_records_an_unjustified_call_as_v3() {
    use instancy::progress::check::{self, Check};
    let mark = check::mark();
    let (result, _) = run_single(
        "v3-recorded",
        |input| {
            input.unary("late-v3", |input, output| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time - 1, data);
                }
                Ok(())
            })
        },
        vec![(5, vec![1])],
    );
    assert_unjustified(result);
    let recorded: Vec<_> = check::violations_since(mark)
        .into_iter()
        .filter(|v| v.check == Check::V3)
        .collect();
    assert!(
        recorded
            .iter()
            .any(|v| v.kind == "unjustified" && v.location.as_deref() == Some("late-v3[out 0]")),
        "{recorded:#?}"
    );
}

#[test]
fn notify_at_justified_only_by_the_frontier_fails_the_dataflow() {
    let (result, _) = run_single(
        "v3-notify-frontier",
        |input| {
            input.unary_notify("early-notify", |input, _output, ctx| {
                while let Some((time, _)) = input.next() {
                    // The input frontier is still at 0, which is at or
                    // before `time - 1`; the consumed batch is not.
                    ctx.notify_at(time - 1);
                }
                while ctx.next_notification().is_some() {}
                Ok(())
            })
        },
        vec![(5, vec![1])],
    );
    assert_unjustified(result);
}

/// R36: input consumed in one logic call never justifies output in a later
/// call, even when both calls fall between the same two commits.
#[test]
fn stashed_data_emitted_in_a_later_call_without_retain_fails_the_dataflow() {
    let (result, _) = run_single(
        "v3-stash",
        |input| {
            input.unary("stash", {
                let mut stash: Vec<(u64, Vec<u64>)> = Vec::new();
                move |input, output| {
                    // One batch per call: emit the previous call's batch.
                    if let Some(batch) = input.next() {
                        for (time, data) in stash.drain(..) {
                            output.push_vec(time, data);
                        }
                        stash.push(batch);
                    }
                    Ok(())
                }
            })
        },
        vec![(1, vec![1]), (2, vec![2])],
    );
    assert_unjustified(result);
}

// ---------------------------------------------------------------------------
// Legitimate patterns
// ---------------------------------------------------------------------------

/// Stash every batch and emit it once the input frontier is empty, without
/// `retain`: the anti-pattern `retain` exists for.
fn stash_until_frontier_empty<T: instancy::Timestamp + Copy>(input: Pipe<T, u64>) -> Pipe<T, u64> {
    input.unary("stash-no-retain", {
        let mut stash: Vec<(T, Vec<u64>)> = Vec::new();
        move |input, output| {
            while let Some(batch) = input.next() {
                stash.push(batch);
            }
            if input.frontier().is_empty() {
                for (time, data) in stash.drain(..) {
                    output.push_vec(time, data);
                }
            }
            Ok(())
        }
    })
}

/// R45: every closure gets a final call when its input frontier becomes
/// empty, so stashing without `retain` and emitting then fails loudly (V3)
/// instead of silently dropping the data.
#[test]
fn stash_and_flush_without_retain_fails_loudly() {
    let (result, out) = run_single(
        "stash-no-retain",
        stash_until_frontier_empty,
        vec![(1, vec![1, 2]), (2, vec![3])],
    );
    assert!(out.is_empty(), "nothing may be emitted late: {out:?}");
    assert_unjustified(result);
}

/// The same inside a loop body: its frontier empties once the loop's input
/// is closed and nothing circulates.
#[test]
fn stash_and_flush_without_retain_inside_a_loop_body_fails_loudly() {
    let (result, out) = run_single(
        "stash-no-retain-in-loop",
        |input| {
            input.iterate::<u32>("loop", 1u32, |var| {
                let flushed = stash_until_frontier_empty(var);
                IterateResult {
                    feedback: flushed.clone().filter("never", |_t, _x| false),
                    output: flushed,
                }
            })
        },
        vec![(1, vec![1, 2]), (2, vec![3])],
    );
    assert!(out.is_empty(), "nothing may be emitted late: {out:?}");
    assert_unjustified(result);
}

/// A notification handler re-arms the time it handles, later.
#[test]
fn notify_rearm_after_next_notification_completes() {
    let (result, out) = run_single(
        "v3-rearm",
        |input| {
            input.unary_notify("rearm", {
                let mut counts: HashMap<u64, u64> = HashMap::new();
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        *counts.entry(time + 100).or_default() += data.len() as u64;
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        if time < 100 {
                            ctx.notify_at(time + 100);
                        } else if let Some(count) = counts.remove(&time) {
                            output.push_vec(time, vec![count]);
                        }
                    }
                    Ok(())
                }
            })
        },
        vec![(1, vec![1, 2]), (2, vec![3])],
    );
    result.unwrap();
    let mut out = out;
    out.sort();
    assert_eq!(out, vec![(101, vec![2]), (102, vec![1])]);
}

/// `delay` emits each record at a later time: justified by the consumed
/// input, whose time is at or before it.
#[test]
fn delay_completes_with_exact_counts() {
    let (result, out) = run_single(
        "v3-delay",
        |input| count_per_time(input.delay("delay", |t, _x| t + 2)),
        vec![(0, vec![1, 2]), (1, vec![3])],
    );
    result.unwrap();
    let mut out = out;
    out.sort();
    assert_eq!(out, vec![(2, vec![2]), (3, vec![1])]);
}

/// A source downgrades its capability as its epoch advances: every epoch is
/// counted exactly, once the source has moved past it.
#[test]
fn source_epoch_downgrade_completes_with_exact_counts() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("v3-source-downgrade");
    count_per_time(builder.input::<u64>("in").unwrap())
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    for epoch in 0..5u64 {
        sender.send(epoch, vec![epoch; epoch as usize + 1]).unwrap();
        sender.advance_to(epoch + 1).unwrap();
    }
    drop(sender);
    with_timeout("source downgrade", move || spawned.join_blocking()).unwrap();
    let mut out = collector.join().unwrap();
    out.sort();
    let expected: Vec<(u64, Vec<u64>)> = (0..5).map(|e| (e, vec![e + 1])).collect();
    assert_eq!(out, expected);
}

/// The retained work of a closure. A fresh instance per worker: cloning
/// happens only when the dataflow is materialized, before any call.
#[derive(Default)]
struct Held(Vec<(Retained<u64>, Vec<u64>)>);

impl Clone for Held {
    fn clone(&self) -> Self {
        Held::default()
    }
}

/// Keep every batch under a capability until the input frontier passes its
/// time, then emit it.
fn hold_until_frontier(input: Pipe<u64, u64>) -> Pipe<u64, u64> {
    input.unary("hold", {
        let mut held = Held::default();
        move |input, output| {
            // Capture the whole (Clone) wrapper, not just its field.
            let held = &mut held;
            while let Some((time, data)) = input.next() {
                held.0.push((output.retain(time), data));
            }
            let frontier = input.frontier().clone();
            held.0.retain_mut(|(retained, data)| {
                if frontier.less_equal(retained.time()) {
                    return true;
                }
                output.push_vec(*retained.time(), std::mem::take(data));
                false
            });
            Ok(())
        }
    })
}

/// `retain`, then emit in a later call: every record arrives, with exact
/// per-time counts.
#[test]
fn retain_then_emit_completes_with_exact_counts() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("retain-then-emit");
    count_per_time(hold_until_frontier(builder.input::<u64>("in").unwrap()))
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    for epoch in 0..5u64 {
        for i in 0..=epoch {
            sender.send(epoch, vec![i]).unwrap();
        }
        sender.advance_to(epoch + 1).unwrap();
    }
    drop(sender);
    with_timeout("retain-then-emit", move || spawned.join_blocking()).unwrap();
    let mut out = collector.join().unwrap();
    out.sort();
    let expected: Vec<(u64, Vec<u64>)> = (0..5u64).map(|e| (e, vec![e + 1])).collect();
    assert_eq!(out, expected);
}

/// A plain `unary` that retains its records' support is called again when
/// its input frontier advances, with no new records: the advance alone
/// releases the held records, and they arrive while the input stays open.
///
/// For each of three epochs, records are sent at the input's current time
/// and the test waits for the operator's own witness that a call ended
/// holding them. Nothing has been released or output by then. Then the input
/// advances, without another batch, and the probe after the operator reaches
/// exactly the next time, the operator has released that epoch with the
/// frontier past it, and its records have arrived, all before the input
/// closes.
#[test]
fn retained_records_are_released_by_a_frontier_advance_alone() {
    let rt = runtime();
    let held_at_end: HeldLog = Arc::default();
    let released: ReleaseLog = Arc::default();
    let builder = DataflowBuilder::<u64>::new("release-on-advance");
    let (stream, probe) = builder
        .input::<u64>("in")
        .unwrap()
        .unary("hold", {
            let mut held = Held::default();
            let held_at_end = Arc::clone(&held_at_end);
            let released = Arc::clone(&released);
            move |input, output| {
                let held = &mut held;
                while let Some((time, data)) = input.next() {
                    held.0.push((output.retain(time), data));
                }
                let frontier = input.frontier().clone();
                held.0.retain_mut(|(retained, data)| {
                    if frontier.less_equal(retained.time()) {
                        return true;
                    }
                    released
                        .lock()
                        .unwrap()
                        .push((*retained.time(), frontier.clone()));
                    output.push_vec(*retained.time(), std::mem::take(data));
                    false
                });
                let still_held = held.0.iter().map(|(_, data)| data.len()).sum();
                held_at_end.lock().unwrap().push((frontier, still_held));
                Ok(())
            }
        })
        .probe();
    stream.output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let mut received: Vec<(u64, u64)> = Vec::new();
    for epoch in 0..3u64 {
        let records: Vec<u64> = (0..=epoch).map(|i| epoch * 10 + i).collect();
        sender.send(epoch, records.clone()).unwrap();
        // A call has taken the records and ended holding them, at `epoch`.
        wait_until("a call ending with the epoch's records held", || {
            held_at_end.lock().unwrap().iter().any(|(frontier, held)| {
                *frontier == Antichain::from_elem(epoch) && *held == records.len()
            })
        });
        assert_eq!(
            released.lock().unwrap().len() as u64,
            epoch,
            "released while epoch {epoch}'s time could still arrive"
        );
        assert!(
            receiver.try_recv().is_none(),
            "output while epoch {epoch} is held"
        );
        assert_eq!(probe.frontier(), Antichain::from_elem(epoch));

        // Advance alone, with no new records.
        sender.advance_to(epoch + 1).unwrap();
        wait_until("the probe reaching the next epoch", || {
            let frontier = probe.frontier();
            assert!(
                !frontier.is_empty() && frontier.elements().iter().all(|t| *t <= epoch + 1),
                "probe at {frontier:?} while the input is open at {}",
                epoch + 1
            );
            frontier.elements() == [epoch + 1]
        });
        while let Some(event) = receiver.try_recv() {
            if let OutputEvent::Data { time, data } = event {
                received.extend(data.into_iter().map(|x| (time, x)));
            }
        }
        received.sort_unstable();
        let want: Vec<(u64, u64)> = (0..=epoch)
            .flat_map(|e| (0..=e).map(move |i| (e, e * 10 + i)))
            .collect();
        assert_eq!(
            received, want,
            "records once the input advanced past {epoch}, with the input open"
        );
        let release = released.lock().unwrap()[epoch as usize].clone();
        assert_eq!(release.0, epoch, "released time");
        assert!(
            !release.1.less_equal(&epoch),
            "released with the input frontier at {:?}",
            release.1
        );
    }
    drop(sender);
    let (joined, rest) = with_timeout("release-on-advance", move || {
        (spawned.join_blocking(), receiver.collect_data())
    });
    joined.unwrap();
    assert!(rest.is_empty(), "records after the input closed: {rest:?}");
    assert!(probe.is_done());
    assert_eq!(released.lock().unwrap().len(), 3, "one release per epoch");
}

/// Behind an exchange on three workers, each instance of a plain `unary`
/// holds its records until every worker's input has passed their time, and
/// a frontier advance alone, with no new records, then releases them while
/// the inputs stay open.
///
/// For each of three epochs, workers 0 and 1 send records and advance past
/// the epoch; then worker 2, which lags, sends its records at the epoch
/// without advancing. The exchange gives each instance one record from each
/// worker. Each instance's own witness shows its call ending with all three
/// held at the epoch, and nothing has been released. Then worker 2 advances
/// alone: the probe reaches exactly the next epoch, each worker has exactly
/// the records routed to it, and every release was made with the
/// instance's input frontier past the epoch, before the inputs close.
#[test]
fn retained_records_behind_an_exchange_are_released_when_the_last_worker_advances() {
    const WORKERS: usize = 3;
    let rt = runtime();
    // Each instance's latest call end: its input frontier and the records it
    // still held. Instances are numbered in the order they are first called.
    let instances: HeldLog = Arc::default();
    let released: ReleaseLog = Arc::default();
    let probe = std::cell::OnceCell::new();
    let mut dataflow = rt
        .spawn_multi(
            "release-behind-exchange",
            WORKERS,
            |b: &mut DataflowBuilder<u64>| {
                let (stream, handle) = b
                    .input::<u64>("in")?
                    .exchange_by_hash("route", |x: &u64| *x)
                    .unary("hold", {
                        let mut held = Held::default();
                        let mut instance: Option<usize> = None;
                        let instances = Arc::clone(&instances);
                        let released = Arc::clone(&released);
                        move |input, output| {
                            let held = &mut held;
                            while let Some((time, data)) = input.next() {
                                held.0.push((output.retain(time), data));
                            }
                            let frontier = input.frontier().clone();
                            held.0.retain_mut(|(retained, data)| {
                                if frontier.less_equal(retained.time()) {
                                    return true;
                                }
                                released
                                    .lock()
                                    .unwrap()
                                    .push((*retained.time(), frontier.clone()));
                                output.push_vec(*retained.time(), std::mem::take(data));
                                false
                            });
                            let still_held = held.0.iter().map(|(_, data)| data.len()).sum();
                            let mut instances = instances.lock().unwrap();
                            let i = *instance.get_or_insert_with(|| {
                                instances.push((frontier.clone(), still_held));
                                instances.len() - 1
                            });
                            instances[i] = (frontier, still_held);
                            Ok(())
                        }
                    })
                    .probe();
                probe.set(handle).expect("the dataflow is built once");
                stream.output("out")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(false),
        )
        .unwrap();
    let probe = probe.into_inner().expect("probe");
    let inputs = dataflow.take_all_inputs::<u64>("in").unwrap();
    let outputs = dataflow.take_all_outputs::<u64>("out").unwrap();
    // Worker `w` sends `epoch * 100 + w * 10 + 0..3` at `epoch`: three
    // consecutive values, one routed to each worker (`value % 3`).
    let records = |epoch: u64, worker: u64| -> Vec<u64> {
        (0..3).map(|i| epoch * 100 + worker * 10 + i).collect()
    };
    let mut received: Vec<Vec<(u64, u64)>> = vec![Vec::new(); WORKERS];
    let mut expected: Vec<Vec<(u64, u64)>> = vec![Vec::new(); WORKERS];
    for epoch in 0..3u64 {
        for worker in 0..2u64 {
            inputs[worker as usize]
                .send(epoch, records(epoch, worker))
                .unwrap();
            inputs[worker as usize].advance_to(epoch + 1).unwrap();
        }
        // Worker 2 lags: it sends at the epoch and stays there.
        inputs[2].send(epoch, records(epoch, 2)).unwrap();
        for worker in 0..3u64 {
            for value in records(epoch, worker) {
                expected[value as usize % WORKERS].push((epoch, value));
            }
        }
        wait_until(
            "every instance ending a call with its three records held",
            || {
                let instances = instances.lock().unwrap();
                instances.len() == WORKERS
                    && instances.iter().all(|(frontier, held)| {
                        *frontier == Antichain::from_elem(epoch) && *held == 3
                    })
            },
        );
        assert!(
            released
                .lock()
                .unwrap()
                .iter()
                .all(|(time, _)| *time < epoch),
            "released {epoch} while worker 2 is still at it"
        );
        assert_eq!(
            probe.frontier(),
            Antichain::from_elem(epoch),
            "probe while worker 2 lags"
        );

        // Worker 2 advances alone, with no new records.
        inputs[2].advance_to(epoch + 1).unwrap();
        wait_until("the probe reaching the next epoch", || {
            let frontier = probe.frontier();
            assert!(
                !frontier.is_empty() && frontier.elements().iter().all(|t| *t <= epoch + 1),
                "probe at {frontier:?} while the inputs are open at {}",
                epoch + 1
            );
            frontier.elements() == [epoch + 1]
        });
        for (output, received) in outputs.iter().zip(&mut received) {
            while let Some(event) = output.try_recv() {
                if let OutputEvent::Data { time, data } = event {
                    received.extend(data.into_iter().map(|x| (time, x)));
                }
            }
            received.sort_unstable();
        }
        for (worker, (got, want)) in received.iter().zip(&expected).enumerate() {
            let mut want = want.clone();
            want.sort_unstable();
            assert_eq!(
                got, &want,
                "worker {worker}'s records once every input passed {epoch}, with the inputs open"
            );
        }
        // The epoch's held batches were released, each with its instance's
        // input frontier past the epoch.
        let released = released.lock().unwrap();
        let this_epoch: Vec<&(u64, Antichain<u64>)> =
            released.iter().filter(|(time, _)| *time == epoch).collect();
        assert!(!this_epoch.is_empty(), "nothing released at {epoch}");
        assert!(
            this_epoch
                .iter()
                .all(|(_, frontier)| !frontier.less_equal(&epoch)),
            "released before every input passed {epoch}: {released:?}"
        );
    }
    drop(inputs);
    let (joined, rest) = with_timeout("release-behind-exchange", move || {
        let joined = dataflow.join_blocking();
        let rest: Vec<_> = outputs.iter().map(|o| o.collect_data()).collect();
        (joined, rest)
    });
    joined.unwrap();
    assert!(
        rest.iter().all(Vec::is_empty),
        "records after the inputs closed: {rest:?}"
    );
    assert!(probe.is_done());
    let released = released.lock().unwrap();
    assert!(
        released
            .iter()
            .all(|(time, frontier)| !frontier.less_equal(time)),
        "a release with the input frontier not past its time: {released:?}"
    );
}

/// A loop feeding a downstream `retain` whose final flush happens when its
/// input frontier empties (never only on channel exhaustion: inside a loop,
/// end-of-stream would wait for the capability).
#[test]
fn loop_feeding_a_retain_that_flushes_when_its_frontier_empties_completes() {
    let (result, out) = run_single(
        "loop-retain-flush",
        |input| {
            let looped = input.iterate::<u32>("grow", 1u32, |v| {
                let feedback = v
                    .clone()
                    .filter("small", |_t, x: &u64| *x < 20)
                    .map("double", |_t, x| x * 2);
                IterateResult {
                    feedback,
                    output: v,
                }
            });
            hold_until_frontier(looped)
        },
        vec![(0, vec![1, 3])],
    );
    result.unwrap();
    let mut values: Vec<u64> = out.into_iter().flat_map(|(_, data)| data).collect();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4, 6, 8, 12, 16, 24, 32]);
}

/// Dropping a `Retained` on another thread releases it and wakes the
/// operator's executor; the downstream count fires only after the release.
#[test]
fn retained_dropped_on_another_thread_is_released_and_wakes_the_worker() {
    let released = Arc::new(AtomicBool::new(false));
    let (retained_tx, retained_rx) = mpsc::channel::<Retained<u64>>();
    let dropper = {
        let released = Arc::clone(&released);
        std::thread::spawn(move || {
            // The input is one batch, so the operator retains once.
            let retained = retained_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            released.store(true, Ordering::SeqCst);
            drop(retained);
        })
    };

    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("retain-elsewhere");
    let held = builder.input::<u64>("in").unwrap().unary("hand-off", {
        let retained_tx = retained_tx.clone();
        move |input, output| {
            while let Some((time, data)) = input.next() {
                retained_tx.send(output.retain(time)).unwrap();
                output.push_vec(time, data);
            }
            Ok(())
        }
    });
    drop(retained_tx);
    count_per_time(held).output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let collector = {
        let released = Arc::clone(&released);
        std::thread::spawn(move || {
            let mut seen = Vec::new();
            while let Some(event) = receiver.recv() {
                if let OutputEvent::Data { time, data } = event {
                    seen.push((time, data, released.load(Ordering::SeqCst)));
                }
            }
            seen
        })
    };
    sender.send(0, vec![1, 2]).unwrap();
    drop(sender);
    with_timeout("retain-elsewhere", move || spawned.join_blocking()).unwrap();
    let seen = collector.join().unwrap();
    assert_eq!(
        seen,
        vec![(0, vec![2], true)],
        "the count must fire once, after the retained capability was released"
    );
    dropper.join().unwrap();
}

// ---------------------------------------------------------------------------
// Output at a later time, justified by consumed input
// ---------------------------------------------------------------------------

/// Retained work of any shape, fresh per worker like [`Held`].
#[derive(Default)]
struct Fresh<V>(V);

impl<V: Default> Clone for Fresh<V> {
    fn clone(&self) -> Self {
        Fresh::default()
    }
}

/// Every record of `batches` with its time, sorted.
fn records<D: Ord>(batches: Vec<(u64, Vec<D>)>) -> Vec<(u64, D)> {
    let mut records: Vec<(u64, D)> = batches
        .into_iter()
        .flat_map(|(time, data)| data.into_iter().map(move |x| (time, x)))
        .collect();
    records.sort();
    records
}

/// The next batch `receiver` delivers; fails the test after 30 s.
fn next_batch<D: std::fmt::Debug + Send + 'static>(
    receiver: &OutputReceiver<u64, D>,
) -> (u64, Vec<D>) {
    match receiver.recv_timeout(Duration::from_secs(30)) {
        Some(OutputEvent::Data { time, data }) => (time, data),
        other => panic!("expected an output batch within 30s, got {other:?}"),
    }
}

/// Polls `condition` until it holds, failing the test after 30 s. The
/// condition is the witness, never the time waited.
fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline, "{what}: not reached within 30s");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Input consumed in a call justifies output at any later time in that call,
/// and that output is delivered at once, while the input is still open at the
/// earlier time; records retained at the earlier time are held until the
/// input frontier passes it, and are then released once.
///
/// `0..10` at 0 feed an operator that gives 12 at 12 in its first call that
/// consumes input, and stashes each batch under support retained at its time
/// until the input frontier passes it. The 12 arrives while the input is
/// still open at 0 and the records are held; once the input closes, the whole
/// output is the records at 0 and exactly one 12 at 12.
#[test]
fn consumed_input_justifies_one_later_output_while_its_records_wait_for_the_frontier() {
    let rt = runtime();
    // The operator's own record of each call's end: its input frontier and
    // how many original records it still held.
    let held_at_end: HeldLog = Arc::default();
    // Each release of held records: their time and the input frontier then.
    let released: ReleaseLog = Arc::default();
    let builder = DataflowBuilder::<u64>::new("later-output-held-records");
    let (stream, probe) = builder
        .input::<u64>("in")
        .unwrap()
        .unary("later-output-and-hold", {
            let mut sent = false;
            let mut held = Held::default();
            let held_at_end = Arc::clone(&held_at_end);
            let released = Arc::clone(&released);
            move |input, output| {
                let held = &mut held;
                while let Some((time, data)) = input.next() {
                    if !sent {
                        output.push_vec(12, vec![12]);
                        sent = true;
                    }
                    held.0.push((output.retain(time), data));
                }
                let frontier = input.frontier().clone();
                held.0.retain_mut(|(retained, data)| {
                    if frontier.less_equal(retained.time()) {
                        return true;
                    }
                    released
                        .lock()
                        .unwrap()
                        .push((*retained.time(), frontier.clone()));
                    output.push_vec(*retained.time(), std::mem::take(data));
                    false
                });
                let still_held = held.0.iter().map(|(_, data)| data.len()).sum();
                held_at_end.lock().unwrap().push((frontier, still_held));
                Ok(())
            }
        })
        .probe();
    stream.output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    sender.send(0, (0..10).collect()).unwrap();
    let first = next_batch(&receiver);
    assert_eq!(first, (12, vec![12]));
    // The input is still open at 0: a call that consumed the records ended
    // with all ten held at frontier 0, and none has been released.
    wait_until("a call ending with the records held", || {
        held_at_end
            .lock()
            .unwrap()
            .iter()
            .any(|(frontier, held)| *frontier == Antichain::from_elem(0) && *held == 10)
    });
    assert_eq!(
        *released.lock().unwrap(),
        Vec::<(u64, Antichain<u64>)>::new(),
        "records were released while their time could still arrive"
    );
    assert!(!probe.done_with(&0));
    drop(sender);
    let (joined, rest) = with_timeout("later-output-held-records", move || {
        (spawned.join_blocking(), receiver.collect_data())
    });
    joined.unwrap();
    let mut batches = vec![first];
    batches.extend(rest);
    let mut expected: Vec<(u64, u64)> = (0..10).map(|x| (0, x)).collect();
    expected.push((12, 12));
    assert_eq!(records(batches), expected);
    // The records were released once, after the frontier had passed 0.
    let released = released.lock().unwrap();
    assert_eq!(released.len(), 1, "releases: {released:?}");
    let (time, frontier) = &released[0];
    assert_eq!(*time, 0);
    assert!(
        !frontier.less_equal(&0),
        "released with the input frontier at {frontier:?}"
    );
}

// ---------------------------------------------------------------------------
// Two inputs: support held until both input frontiers pass
// ---------------------------------------------------------------------------

/// What a two-input operator has done, updated at the end of each call. A
/// test waits until the operator has seen its first input's frontier move
/// (the witness that it processed that side) before checking what it has
/// released.
#[derive(Debug, Default)]
struct TwoInputCalls {
    /// The first input's frontier at the end of the latest call, if any.
    first_frontier: Option<Vec<u64>>,
    /// Each time whose support the operator released, in release order.
    released: Vec<u64>,
}

fn wait_for_first_frontier(calls: &Mutex<TwoInputCalls>, frontier: &[u64]) {
    wait_until("the operator seeing its first input's frontier", || {
        calls.lock().unwrap().first_frontier.as_deref() == Some(frontier)
    });
}

/// A `binary` operator is called on one input's data alone, and what it gives
/// is delivered at once, while its other input is still behind. Each input
/// reports its own frontier, so support the operator retains per time is
/// released only once both input frontiers have passed that time, and until
/// then it holds the probe.
///
/// For `i` in `1..10` each input in turn sends `i - 1` and advances to `i`.
/// Every batch is forwarded at once and retains support at its time; each
/// time's support is released once, in order, after both inputs pass it.
/// Then the first input sends 9 and closes while the second is still open at
/// 9: an empty frontier on one input does not release time 9 until the other
/// input passes it too.
#[test]
fn binary_forwards_at_once_and_releases_each_time_once_both_input_frontiers_pass_it() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("binary-forward-and-release");
    let calls: Arc<Mutex<TwoInputCalls>> = Arc::default();
    let first = builder.input::<u64>("in1").unwrap();
    let second = builder.input::<u64>("in2").unwrap();
    let (forwarded, probe) = first
        .binary(second, "forward-and-release", {
            let calls = Arc::clone(&calls);
            let mut support = Fresh::<BTreeMap<u64, Retained<u64>>>::default();
            move |input1, input2, output| {
                let support = &mut support;
                while let Some((time, data)) = input1.next() {
                    support.0.entry(time).or_insert_with(|| output.retain(time));
                    output.push_vec(time, data);
                }
                while let Some((time, data)) = input2.next() {
                    support.0.entry(time).or_insert_with(|| output.retain(time));
                    output.push_vec(time, data);
                }
                let (frontier1, frontier2) = (input1.frontier(), input2.frontier());
                let mut calls = calls.lock().unwrap();
                support.0.retain(|time, _support| {
                    if frontier1.less_equal(time) || frontier2.less_equal(time) {
                        return true;
                    }
                    calls.released.push(*time);
                    false
                });
                calls.first_frontier = Some(frontier1.elements().to_vec());
                Ok(())
            }
        })
        .unwrap()
        .probe();
    forwarded.output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let in1 = spawned.take_input::<u64>("in1").unwrap();
    let in2 = spawned.take_input::<u64>("in2").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let mut batches = Vec::new();
    for i in 1..10u64 {
        in1.send(i - 1, vec![i - 1]).unwrap();
        in1.advance_to(i).unwrap();
        // Forwarded at once, while the second input is still at i - 1.
        let batch = next_batch(&receiver);
        assert_eq!(batch, (i - 1, vec![i - 1]));
        batches.push(batch);
        wait_for_first_frontier(&calls, &[i]);
        assert_eq!(
            calls.lock().unwrap().released,
            (0..i - 1).collect::<Vec<_>>(),
            "released {} while the second input was still at it",
            i - 1
        );
        assert!(!probe.done_with(&(i - 1)));
        in2.send(i - 1, vec![i - 1]).unwrap();
        in2.advance_to(i).unwrap();
        let batch = next_batch(&receiver);
        assert_eq!(batch, (i - 1, vec![i - 1]));
        batches.push(batch);
        wait_until("both inputs passing the round's time", || {
            probe.done_with(&(i - 1))
        });
        assert_eq!(calls.lock().unwrap().released, (0..i).collect::<Vec<_>>());
    }
    // One input closes while the other is still open: the first input sends 9
    // and closes, so its frontier is empty, but the second is still at 9, so
    // time 9's support stays held until the second input closes too.
    in1.send(9, vec![9]).unwrap();
    drop(in1);
    let batch = next_batch(&receiver);
    assert_eq!(batch, (9, vec![9]));
    batches.push(batch);
    wait_for_first_frontier(&calls, &[]);
    assert_eq!(
        calls.lock().unwrap().released,
        (0..9).collect::<Vec<_>>(),
        "released 9 while the second input was still open at it"
    );
    assert!(!probe.done_with(&9));
    in2.send(9, vec![9]).unwrap();
    drop(in2);
    let (joined, rest) = with_timeout("binary-forward-and-release", move || {
        (spawned.join_blocking(), receiver.collect_data())
    });
    joined.unwrap();
    batches.extend(rest);
    let expected: Vec<(u64, u64)> = (0..10).flat_map(|t| [(t, t), (t, t)]).collect();
    assert_eq!(records(batches), expected);
    assert_eq!(calls.lock().unwrap().released, (0..10).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// unary_async
// ---------------------------------------------------------------------------

/// A count after `unary_async` waits for a suspended task: the task's
/// capability holds the frontier after `unary_async` (and the count) until
/// its result is collected, even while another task at the same time has
/// already finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_after_unary_async_waits_for_a_suspended_task() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("async-suspended");
    let task_gate = Arc::clone(&gate);
    let (slow, probe) = builder
        .input::<u64>("in")
        .unwrap()
        .unary_async(
            "suspended",
            2,
            Arc::new(move |_t: u64, data: Vec<u64>| {
                let gate = Arc::clone(&task_gate);
                async move {
                    // Only the batch holding 1 waits.
                    if data.contains(&1) {
                        let _permit = gate.acquire().await.unwrap();
                    }
                    Ok(data)
                }
            }),
        )
        .probe();
    count_per_time(slow).output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.send(0, vec![4]).unwrap();
    sender.advance_to(1).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !probe.done_with(&0),
        "the frontier after unary_async passed 0 while a task at 0 was suspended"
    );
    assert!(
        receiver.try_recv().is_none(),
        "the count fired while its input was in a suspended task"
    );
    gate.add_permits(1);
    drop(sender);
    let result = tokio::time::timeout(Duration::from_secs(30), spawned.join()).await;
    assert!(matches!(result, Ok(Ok(()))), "join: {result:?}");
    let out = tokio::task::spawn_blocking(move || receiver.collect_data())
        .await
        .unwrap();
    assert_eq!(out, vec![(0, vec![4])]);
}

/// A running task holds its time until it fails, and a failed task fails
/// the dataflow with the task's error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_after_a_failed_unary_async_task_never_fires() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("async-failure");
    let (failing, probe) = builder
        .input::<u64>("in")
        .unwrap()
        .unary_async(
            "fails-at-1",
            2,
            Arc::new(|time: u64, data: Vec<u64>| async move {
                if time == 1 {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    Err(instancy::Error::operator(
                        "fails-at-1",
                        std::io::Error::other("task failed on purpose"),
                    ))
                } else {
                    Ok(data)
                }
            }),
        )
        .probe();
    count_per_time(failing).output("out").unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    sender.send(1, vec![1, 2]).unwrap();
    sender.advance_to(2).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !probe.done_with(&1),
        "the frontier after unary_async passed 1 while its task at 1 was running"
    );
    let result = tokio::time::timeout(Duration::from_secs(30), spawned.join()).await;
    drop(sender);
    let error = match result {
        Ok(Err(error)) => error,
        other => panic!("the failed task must fail the dataflow: {other:?}"),
    };
    assert!(
        error.to_string().contains("task failed on purpose"),
        "unexpected error: {error}"
    );
    let out = tokio::task::spawn_blocking(move || receiver.collect_data())
        .await
        .unwrap();
    assert!(
        out.iter().all(|(time, _)| *time != 1),
        "the count for the failed task's time fired: {out:?}"
    );
}

/// At its concurrency limit, with batches waiting for a task slot and no
/// task finishing, `unary_async` waits for a task to finish instead of
/// being activated over and over (a busy loop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_async_at_its_concurrency_limit_waits_without_spinning() {
    use instancy::metrics::{DataflowMetrics, MetricsConfig};

    fn activations(metrics: &DataflowMetrics) -> u64 {
        metrics
            .operator_snapshots()
            .iter()
            .filter(|op| op.name.contains("at-limit"))
            .map(|op| op.activations)
            .sum()
    }

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("async-at-limit");
    let task_gate = Arc::clone(&gate);
    builder
        .input::<u64>("in")
        .unwrap()
        .unary_async(
            "at-limit",
            1,
            Arc::new(move |_t: u64, data: Vec<u64>| {
                let gate = Arc::clone(&task_gate);
                async move {
                    let _permit = gate.acquire().await.unwrap();
                    Ok(data)
                }
            }),
        )
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(
            builder.build().unwrap(),
            SpawnOptions::new().metrics(MetricsConfig::full()),
        )
        .unwrap();
    let metrics = Arc::clone(spawned.metrics().unwrap());
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    // One task runs (blocked on the gate); four batches wait for its slot.
    for i in 0..5 {
        sender.send(0, vec![i]).unwrap();
    }
    // Wait until the operator has taken every batch and the worker parked:
    // its activation count stops changing.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut settled = activations(&metrics);
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = activations(&metrics);
        if now == settled && now > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the operator was still being activated after 10 s ({now} activations)"
        );
        settled = now;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = activations(&metrics) - settled;
    assert!(
        idle < 10,
        "{idle} activations in 1 s at the concurrency limit with no task finishing"
    );
    gate.add_permits(5);
    drop(sender);
    let result = tokio::time::timeout(Duration::from_secs(30), spawned.join()).await;
    assert!(matches!(result, Ok(Ok(()))), "join: {result:?}");
    let mut out: Vec<u64> = tokio::task::spawn_blocking(move || receiver.collect_data())
        .await
        .unwrap()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    out.sort();
    assert_eq!(out, vec![0, 1, 2, 3, 4]);
    assert!(
        activations(&metrics) > settled,
        "the operator ran again once its tasks finished"
    );
}

// ---------------------------------------------------------------------------
// Egress
// ---------------------------------------------------------------------------

/// A paused external receiver never lets the dataflow complete before every
/// record was handed to it: the sink keeps the records it could not send
/// counted at its input.
#[test]
fn paused_output_receiver_never_completes_early() {
    const N: u64 = 2_000;
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("paused-receiver");
    builder
        .input::<u64>("in")
        .unwrap()
        .map("id", |_t, x| x)
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let feeder = std::thread::spawn(move || {
        for i in 0..N {
            sender.send(0, vec![i]).unwrap();
        }
    });
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(spawned.join_blocking());
    });
    let early = done_rx.recv_timeout(Duration::from_millis(500)).ok();
    let mut got: Vec<u64> = Vec::new();
    if let Some(result) = early {
        // Completing while paused is only correct if everything was already
        // handed to the receiver.
        result.unwrap();
        while let Some(event) = receiver.try_recv() {
            if let OutputEvent::Data { data, .. } = event {
                got.extend(data);
            }
        }
        assert_eq!(
            got.len() as u64,
            N,
            "completed before handing over every record"
        );
    } else {
        got.extend(receiver.collect_data().into_iter().flat_map(|(_, d)| d));
        done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the dataflow did not complete once the receiver drained")
            .unwrap();
    }
    feeder.join().unwrap();
    got.sort_unstable();
    assert_eq!(got, (0..N).collect::<Vec<_>>());
}
