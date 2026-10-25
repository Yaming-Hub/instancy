//! Capabilities and their justification (V3): work an operator keeps across
//! logic calls is covered by a capability, and every output and capability
//! request must be justified by the call that makes it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use instancy::dataflow::{OutputEvent, Retained};
use instancy::{
    DataflowBuilder, IterateResult, Pipe, Result, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

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
