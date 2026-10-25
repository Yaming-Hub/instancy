//! Nested loop scopes (design §5.1.7): each `iterate` loop is its own
//! progress scope, bridged to its parent by the import (the parent's frontier
//! at the loop's input holds the loop's boundary) and the export (the loop's
//! internal work holds its output in the parent).

use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Duration;

use instancy::dataflow::DataflowBuilderConfig;
use instancy::order::Product;
use instancy::{
    DataflowBuilder, IterateResult, Pipe, Result, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig::default()).unwrap()
}

fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what}: did not finish within 30s"))
}

type Inner = Product<u64, u32>;

// ---------------------------------------------------------------------------
// Identity cycles (R38 C5)
// ---------------------------------------------------------------------------

fn loop_with_summary(summary: u32) -> Result<instancy::LogicalDataflow<u64>> {
    let builder = DataflowBuilder::<u64>::new("identity");
    builder
        .input::<u64>("in")?
        .iterate::<u32>("spin", summary, |var| {
            let next = var.map("inc", |_t: &Inner, x| x + 1);
            IterateResult {
                feedback: next.clone().filter("again", |_t, x| *x < 10),
                output: next.filter("done", |_t, x| *x >= 10),
            }
        })
        .output("out")?;
    builder.build()
}

#[test]
fn a_loop_whose_summary_does_not_advance_is_rejected() {
    let error = match loop_with_summary(0) {
        Ok(_) => panic!("summary 0 is an identity cycle and must not build"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(
        message.contains("spin") && message.contains("does not advance"),
        "{message}"
    );
}

#[test]
fn a_loop_whose_summary_advances_builds() {
    loop_with_summary(1).expect("summary 1 advances the iteration counter");
}

// ---------------------------------------------------------------------------
// The import: a loop's boundary holds while its parent input is open
// ---------------------------------------------------------------------------

/// The parent's input stays open at epoch 0 while every record sent so far
/// has gone through the loop, so the loop holds no message. An aggregate
/// inside the loop must still not fire for epoch 0: more records may enter
/// at `(0, 0)` (design §5.1.7, the first scope-bridge regression).
#[test]
fn an_aggregate_inside_a_loop_waits_for_its_parent_input() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("import-holds");
    builder
        .input::<u64>("in")
        .unwrap()
        .iterate::<u32>("loop", 1u32, |var| {
            let counted = var.clone().unary_notify("count-in-loop", {
                let mut counts: HashMap<Inner, u64> = HashMap::new();
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
            });
            IterateResult {
                feedback: var.filter("never", |_t, _x| false),
                output: counted,
            }
        })
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        receiver.try_recv().is_none(),
        "the aggregate inside the loop fired while its parent input was still open"
    );
    sender.send(0, vec![4, 5]).unwrap();
    drop(sender);
    with_timeout("import-holds", move || spawned.join_blocking()).unwrap();
    assert_eq!(receiver.collect_data(), vec![(0, vec![5])]);
}

// ---------------------------------------------------------------------------
// The export: a nested scope on an outer feedback path drains
// ---------------------------------------------------------------------------

/// A probe inside a loop body follows the loop's own frontiers while the
/// dataflow runs: once epoch 0 has left the loop, it passes every `(0, i)`,
/// while the open input still holds `(1, 0)`.
#[test]
fn a_probe_inside_a_loop_body_advances_while_the_dataflow_runs() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::new("loop-body-probe");
    let probe = std::rc::Rc::new(std::cell::RefCell::new(None));
    let slot = std::rc::Rc::clone(&probe);
    builder
        .input::<u64>("in")
        .unwrap()
        .iterate::<u32>("loop", 1u32, move |var| {
            let (doubled, p) = var.map("double", |_t: &Inner, x| x * 2).probe();
            *slot.borrow_mut() = Some(p);
            IterateResult {
                feedback: doubled.clone().filter("small", |_t, x| *x < 64),
                output: doubled.filter("large", |_t, x| *x >= 64),
            }
        })
        .output("out")
        .unwrap();
    let probe = probe.borrow_mut().take().unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    sender.send(0, vec![1, 3]).unwrap();
    sender.advance_to(1).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !probe.done_with(&Product::new(0, 100)) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        probe.done_with(&Product::new(0, 100)),
        "the loop-body probe never passed epoch 0 while the input was open: {:?}",
        probe.frontier()
    );
    assert!(
        !probe.done_with(&Product::new(1, 0)),
        "the loop-body probe passed (1, 0) while the input was open at 1"
    );
    drop(sender);
    with_timeout("loop-body-probe", move || spawned.join_blocking()).unwrap();
    let mut out: Vec<u64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    out.sort_unstable();
    assert_eq!(out, vec![64, 96]);
    assert!(probe.is_done());
}

// ---------------------------------------------------------------------------
// The export: a nested scope on an outer feedback path drains
// ---------------------------------------------------------------------------

/// An inner loop on the outer loop's feedback path: its export (its internal
/// work, seen from the outer loop) must become empty once all real work has
/// drained, or the outer loop, and the dataflow, never complete (design
/// §5.1.7, the second scope-bridge regression).
fn nested_on_feedback_path(input: Pipe<u64, u64>) -> Pipe<u64, u64> {
    input.iterate::<u32>("outer", 1u32, |var| {
        let inner = var.iterate::<u32>("inner", 1u32, |w| {
            let next = w.map("inc", |_t: &Product<Inner, u32>, x| x + 1);
            IterateResult {
                feedback: next.clone().filter("not-multiple", |_t, x| *x % 3 != 0),
                output: next.filter("multiple", |_t, x| *x % 3 == 0),
            }
        });
        IterateResult {
            feedback: inner.clone().filter("small", |_t, x| *x < 30),
            output: inner.filter("large", |_t, x| *x >= 30),
        }
    })
}

#[test]
fn a_nested_scope_on_an_outer_feedback_path_completes() {
    for workers in [1usize, 2] {
        let rt = runtime();
        let mut multi = rt
            .spawn_multi(
                "export-drains",
                workers,
                |b: &mut DataflowBuilder<u64>| {
                    let input = b
                        .input::<u64>("in")?
                        .exchange_by_hash("spread", |x: &u64| *x);
                    let (out, _probe) = nested_on_feedback_path(input).probe();
                    out.output("out")?;
                    Ok(())
                },
                SpawnOptions::new(),
            )
            .unwrap();
        let senders = multi.take_all_inputs::<u64>("in").unwrap();
        let receivers = multi.take_all_outputs::<u64>("out").unwrap();
        for epoch in 0..3u64 {
            senders[0].send(epoch, vec![1, 2, 4, 31]).unwrap();
            senders[0].advance_to(epoch + 1).unwrap();
        }
        drop(senders);
        with_timeout("export-drains", move || multi.join_blocking()).unwrap();
        let mut out: Vec<(u64, u64)> = receivers
            .into_iter()
            .flat_map(|r| r.collect_data())
            .flat_map(|(t, data)| data.into_iter().map(move |x| (t, x)))
            .collect();
        out.sort_unstable();
        let mut expected: Vec<(u64, u64)> = (0..3u64)
            .flat_map(|e| [(e, 30), (e, 30), (e, 30), (e, 33)])
            .collect();
        expected.sort_unstable();
        assert_eq!(out, expected, "{workers} worker(s)");
    }
}

// ---------------------------------------------------------------------------
// Backpressure: a loop with one-batch channels does not deadlock
// ---------------------------------------------------------------------------

#[test]
fn a_loop_with_one_batch_channels_completes() {
    let rt = runtime();
    let builder = DataflowBuilder::<u64>::with_config(
        "one-batch-loop",
        DataflowBuilderConfig {
            channel_capacity: 1,
            ..DataflowBuilderConfig::default()
        },
    );
    builder
        .input::<u64>("in")
        .unwrap()
        .iterate::<u32>("loop", 1u32, |var| {
            let stepped = var.map("inc", |_t: &Inner, x| x + 1);
            IterateResult {
                feedback: stepped.clone().filter("again", |_t, x| x % 5 != 0),
                output: stepped.filter("done", |_t, x| x % 5 == 0),
            }
        })
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let sender = spawned.take_input::<u64>("in").unwrap();
    let receiver = spawned.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    for epoch in 0..4u64 {
        for i in 0..24 {
            sender.send(epoch, vec![epoch * 1000 + i]).unwrap();
        }
        sender.advance_to(epoch + 1).unwrap();
    }
    drop(sender);
    with_timeout("one-batch-loop", move || spawned.join_blocking()).unwrap();
    let mut out: Vec<(u64, u64)> = collector
        .join()
        .unwrap()
        .into_iter()
        .flat_map(|(t, data)| data.into_iter().map(move |x| (t, x)))
        .collect();
    out.sort_unstable();
    assert_eq!(out, next_multiples_of_5());
}

/// Each of the 96 records `(epoch, epoch * 1000 + i)` of the one-batch loop
/// tests, stepped up to the next multiple of 5, sorted.
fn next_multiples_of_5() -> Vec<(u64, u64)> {
    let mut expected: Vec<(u64, u64)> = (0..4u64)
        .flat_map(|e| (0..24u64).map(move |i| (e, (e * 1000 + i) / 5 * 5 + 5)))
        .collect();
    expected.sort_unstable();
    expected
}

/// R44 on two workers with an exchange inside the loop body: every physical
/// cycle passes through some worker's back-edge, which never pushes back.
#[test]
fn a_loop_with_one_batch_channels_and_an_exchange_completes_on_two_workers() {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "one-batch-loop-exchange",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .with_capacity(1)?
                    .iterate::<u32>("loop", 1u32, |var| {
                        let stepped = var
                            .with_capacity(1)
                            .unwrap()
                            .map("inc", |_t: &Inner, x| x + 1)
                            .with_capacity(1)
                            .unwrap()
                            .exchange_by_hash("spread", |x: &u64| *x)
                            .with_capacity(1)
                            .unwrap();
                        IterateResult {
                            feedback: stepped
                                .clone()
                                .filter("again", |_t, x| x % 5 != 0)
                                .with_capacity(1)
                                .unwrap(),
                            output: stepped
                                .filter("done", |_t, x| x % 5 == 0)
                                .with_capacity(1)
                                .unwrap(),
                        }
                    })
                    .with_capacity(1)?
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let senders = multi.take_all_inputs::<u64>("in").unwrap();
    let receivers = multi.take_all_outputs::<u64>("out").unwrap();
    for epoch in 0..4u64 {
        for i in 0..24u64 {
            senders[(i % 2) as usize]
                .send(epoch, vec![epoch * 1000 + i])
                .unwrap();
        }
        for sender in &senders {
            sender.advance_to(epoch + 1).unwrap();
        }
    }
    drop(senders);
    let collectors: Vec<_> = receivers
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    with_timeout("one-batch-loop-exchange", move || multi.join_blocking()).unwrap();
    let mut out: Vec<(u64, u64)> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .flat_map(|(t, data)| data.into_iter().map(move |x| (t, x)))
        .collect();
    out.sort_unstable();
    assert_eq!(out, next_multiples_of_5());
}

// ---------------------------------------------------------------------------
// Stage inference (R18)
// ---------------------------------------------------------------------------

/// A loop after a parallelism change (`exchange_to(1)`, then `iterate`) on
/// two workers: the loop runs in the one-worker stage, its feedback edge with
/// it, and the dataflow spawns and completes with exact output.
#[test]
fn a_loop_after_a_parallelism_change_spawns_and_completes() {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "loop-after-exchange-to",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .exchange_to("to-one", 1, |x: &u64| *x)?
                    .iterate::<u32>("loop", 1u32, |var| {
                        let stepped = var.map("inc", |_t: &Inner, x| x + 1);
                        IterateResult {
                            feedback: stepped.clone().filter("again", |_t, x| x % 5 != 0),
                            output: stepped.filter("done", |_t, x| x % 5 == 0),
                        }
                    })
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(true),
        )
        .unwrap();
    let senders = multi.take_all_inputs::<u64>("in").unwrap();
    assert_eq!(senders.len(), 2, "the input stage runs on both workers");
    let receivers = multi.take_all_outputs::<u64>("out").unwrap();
    for epoch in 0..4u64 {
        for i in 0..24u64 {
            senders[(i % 2) as usize]
                .send(epoch, vec![epoch * 1000 + i])
                .unwrap();
        }
        for sender in &senders {
            sender.advance_to(epoch + 1).unwrap();
        }
    }
    drop(senders);
    let collectors: Vec<_> = receivers
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    with_timeout("loop-after-exchange-to", move || multi.join_blocking()).unwrap();
    let mut out: Vec<(u64, u64)> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .flat_map(|(t, data)| data.into_iter().map(move |x| (t, x)))
        .collect();
    out.sort_unstable();
    assert_eq!(out, next_multiples_of_5());
}
