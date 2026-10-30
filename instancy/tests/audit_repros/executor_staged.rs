//! Audit: frontier/notification behaviour of the staged (per-stage
//! parallelism) executor path.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// input (par 1) → exchange_to(2) → unary_notify(sum per epoch) → output.
/// After `advance_to(1)` epoch 0 is sealed, so the notification for epoch 0
/// should fire while the input is still open (as it does for single-worker
/// dataflows, see progress_tracking::notification_waits_for_all_input).
#[test]
fn staged_notify_fires_on_frontier_advance_while_input_open() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "staged-notify",
            1,
            |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<u64>("data").unwrap();
                input
                    .exchange_to("scatter", 2, |v: &u64| *v)
                    .unwrap()
                    .unary_notify("sum", {
                        let mut stash: HashMap<u64, u64> = HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                *stash.entry(time).or_default() += data.iter().sum::<u64>();
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(sum) = stash.remove(&time) {
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results")
                    .unwrap();
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    eprintln!("num_workers = {}", multi.num_workers());

    let sender = multi.take_input::<u64>(0, "data").unwrap();
    let mut receivers = Vec::new();
    for w in 0..multi.num_workers() {
        if let Ok(r) = multi.take_output::<u64>(w, "results") {
            receivers.push((w, r));
        }
    }
    eprintln!(
        "output receivers on workers: {:?}",
        receivers.iter().map(|(w, _)| *w).collect::<Vec<_>>()
    );

    sender.send(0, vec![1, 2, 3, 4]).unwrap();
    sender.advance_to(1).unwrap();

    // Epoch 0 is sealed. Wait up to 5s for any epoch-0 output while input open.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got: Vec<(u64, Vec<u64>)> = Vec::new();
    while Instant::now() < deadline && got.is_empty() {
        for (_, r) in &receivers {
            while let Some(ev) = r.try_recv() {
                if let instancy::dataflow::OutputEvent::Data { time, data } = ev {
                    got.push((time, data));
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    eprintln!("outputs before close: {got:?}");
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let joined = rx.recv_timeout(Duration::from_secs(20));
    let mut after: Vec<(u64, Vec<u64>)> = Vec::new();
    for (_, r) in &receivers {
        while let Some(ev) = r.try_recv() {
            if let instancy::dataflow::OutputEvent::Data { time, data } = ev {
                after.push((time, data));
            }
        }
    }
    eprintln!("join: {joined:?}; outputs after close: {after:?}");
    assert!(
        !got.is_empty(),
        "epoch 0 was sealed by advance_to(1) but no notification fired while input open"
    );
}

/// Same topology, but epoch 0 is NOT sealed (no advance_to). Any epoch-0
/// output before the frontier passes 0 is a premature notification; a second
/// batch at epoch 0 then yields a second partial sum for the same epoch.
#[test]
fn staged_notify_must_not_fire_before_epoch_sealed() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "staged-notify-premature",
            1,
            |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<u64>("data").unwrap();
                input
                    .exchange_to("scatter", 2, |_v: &u64| 0)
                    .unwrap()
                    .unary_notify("sum", {
                        let mut stash: HashMap<u64, u64> = HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                *stash.entry(time).or_default() += data.iter().sum::<u64>();
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(sum) = stash.remove(&time) {
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results")
                    .unwrap();
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let sender = multi.take_input::<u64>(0, "data").unwrap();
    let mut receivers = Vec::new();
    for w in 0..multi.num_workers() {
        if let Ok(r) = multi.take_output::<u64>(w, "results") {
            receivers.push(r);
        }
    }
    sender.send(0, vec![1, 2, 3]).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    sender.send(0, vec![4]).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let joined = rx.recv_timeout(Duration::from_secs(20));
    let mut out: Vec<(u64, Vec<u64>)> = Vec::new();
    for r in &receivers {
        while let Some(ev) = r.try_recv() {
            if let instancy::dataflow::OutputEvent::Data { time, data } = ev {
                out.push((time, data));
            }
        }
    }
    eprintln!("join: {joined:?}; outputs: {out:?}");
    assert_eq!(
        out,
        vec![(0, vec![10])],
        "epoch 0 notified before it was sealed"
    );
}

fn drain_run(staged: bool) -> (instancy::Result<()>, usize) {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(10));
    let mut multi = rt
        .spawn_multi(
            "drain-staged",
            2,
            move |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<u64>("data").unwrap();
                let ex = if staged {
                    // Different parallelism => staged (StageExecutor) path.
                    input.exchange_to("scatter", 3, |v: &u64| *v).unwrap()
                } else {
                    input.exchange("scatter", |v: &u64| *v)
                };
                ex.map("slowish", |_t, v| {
                    std::hint::black_box((0..200).fold(v, |a, b| a.wrapping_add(b)))
                })
                .output("results")
                .unwrap();
                Ok(())
            },
            opts,
        )
        .unwrap();
    let mut receivers = Vec::new();
    for w in 0..multi.num_workers() {
        if let Ok(r) = multi.take_output::<u64>(w, "results") {
            receivers.push(r);
        }
    }
    let collectors: Vec<_> = receivers
        .into_iter()
        .map(|r| {
            std::thread::spawn(move || r.collect_data().iter().map(|(_, d)| d.len()).sum::<usize>())
        })
        .collect();
    let mut senders = Vec::new();
    for w in 0..multi.num_workers() {
        if let Ok(s) = multi.take_input::<u64>(w, "data") {
            senders.push(s);
        }
    }
    for i in 0..2_000u64 {
        senders[0].send(0, vec![i]).unwrap();
    }
    drop(senders); // all inputs closed: nothing new can arrive
    multi.cancel(); // drain_on_cancel: in-flight data should still complete
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let res = rx.recv_timeout(Duration::from_secs(30)).expect("join hung");
    let total: usize = collectors.into_iter().map(|c| c.join().unwrap()).sum();
    (res, total)
}

/// drain_on_cancel must let in-flight data finish after inputs are closed.
/// Works on the uniform (DataflowExecutor) path; the staged path ignores it.
#[test]
fn drain_on_cancel_honoured_by_staged_path() {
    let (uniform, n_uniform) = drain_run(false);
    eprintln!("uniform path: {uniform:?}, delivered {n_uniform}/2000");
    let (staged, n_staged) = drain_run(true);
    eprintln!("staged path: {staged:?}, delivered {n_staged}/2000");
    assert!(uniform.is_ok() && n_uniform == 2000, "uniform drain failed");
    assert!(
        staged.is_ok() && n_staged == 2000,
        "staged path ignored drain_on_cancel: {staged:?}, delivered {n_staged}/2000"
    );
}
