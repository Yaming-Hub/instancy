//! Audit (distributed progress protocol): the exchange consumer records `-n`
//! when the exchange pass-through operator PULLS a batch from `ExchangePull`,
//! but the operator may then hold the batch in its `pending_output` (output
//! channel full) with no capability and no message count. The message
//! pointstamp reaches zero while the data is still buffered, so the
//! downstream frontier can advance past `t` and a notification for `t` fires
//! before the buffered data at `t` is delivered — the remainder then arrives
//! "behind" the frontier and is reported as a second, partial result.
//!
//! Setup (3 workers, all records routed to worker 0, capacity-1 channels):
//! worker 0's output is not read at first, so the backpressure chain
//! sink(256) -> notify -> exchange-op pending -> exchange channel fills up.
//! The user then drains worker 0's output slowly.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};

const MARK: u64 = 1_000_000;

fn build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .with_capacity(1)?
        .exchange_by_hash("to-w0", |_x: &u64| 0)
        .with_capacity(1)?
        .unary_notify("echo-and-count", {
            let mut stash: HashMap<u64, u64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((t, data)) = input.next() {
                    *stash.entry(t).or_default() += data.len() as u64;
                    ctx.notify_at(t);
                    // Echo each batch so the external output channel fills up.
                    output.push_vec(t, data);
                }
                while let Some(t) = ctx.next_notification() {
                    if let Some(c) = stash.remove(&t) {
                        output.push_vec(t, vec![MARK + c]);
                    }
                }
                Ok(())
            }
        })
        .output("out")?;
    Ok(())
}

fn run_once(per_worker: u64) -> (Vec<u64>, u64) {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 3,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("audit-pending", 3, build, SpawnOptions::new())
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let _out1 = multi.take_output::<u64>(1, "out").unwrap();
    let _out2 = multi.take_output::<u64>(2, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();
    let in2 = multi.take_input::<u64>(2, "data").unwrap();
    drop(in0);

    let mut senders = Vec::new();
    for input in [in1, in2] {
        senders.push(std::thread::spawn(move || {
            for i in 0..per_worker {
                input.send(0, vec![i]).unwrap();
            }
            input.advance_to(1).unwrap();
            drop(input);
        }));
    }

    // Let the backpressure chain fill up, then drain worker 0's output slowly.
    std::thread::sleep(Duration::from_millis(800));
    let mut markers = Vec::new();
    let mut echoed = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        match out0.recv_timeout(Duration::from_millis(500)) {
            Some(OutputEvent::Data { time, data }) => {
                assert_eq!(time, 0);
                for v in data {
                    if v >= MARK {
                        markers.push(v - MARK);
                    } else {
                        echoed += 1;
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Some(_) => {}
            None => {
                if echoed == 2 * per_worker && !markers.is_empty() {
                    break;
                }
            }
        }
    }
    for s in senders {
        s.join().unwrap();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let _ = rx.recv_timeout(Duration::from_secs(10));
    (markers, echoed)
}

#[test]
fn notification_fires_while_consumed_exchange_data_is_still_buffered() {
    let per_worker = 300;
    let mut failures = Vec::new();
    for attempt in 0..5 {
        let (markers, echoed) = run_once(per_worker);
        eprintln!("attempt {attempt}: markers={markers:?} echoed={echoed}");
        if markers != vec![2 * per_worker] || echoed != 2 * per_worker {
            failures.push((attempt, markers, echoed));
        }
    }
    assert!(
        failures.is_empty(),
        "epoch 0 must be counted exactly once as {}; got {failures:?}",
        2 * per_worker
    );
}

/// Same pattern, SINGLE worker, no exchange at all:
/// input -> map(id) -[cap 1]-> echo-and-count -> output.
/// The input operator releases its capability at epoch 0 as soon as it has
/// pushed its records into the (untracked) input->map channel; `map` holds
/// no capability, so while records sit in local channels / `pending_output`
/// under backpressure the notify operator's frontier is already past 0.
#[test]
fn single_worker_notification_fires_while_data_buffered_in_local_channel() {
    fn build1(builder: &mut DataflowBuilder<u64>) -> Result<()> {
        builder
            .input::<u64>("data")?
            .map("id", |_t, x| x)
            .with_capacity(1)?
            .unary_notify("echo-and-count", {
                let mut stash: HashMap<u64, u64> = HashMap::new();
                move |input, output, ctx| {
                    while let Some((t, data)) = input.next() {
                        *stash.entry(t).or_default() += data.len() as u64;
                        ctx.notify_at(t);
                        output.push_vec(t, data);
                    }
                    while let Some(t) = ctx.next_notification() {
                        if let Some(c) = stash.remove(&t) {
                            output.push_vec(t, vec![MARK + c]);
                        }
                    }
                    Ok(())
                }
            })
            .output("out")?;
        Ok(())
    }
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("audit-pending-1w", 1, build1, SpawnOptions::new())
        .unwrap();
    let out = multi.take_output::<u64>(0, "out").unwrap();
    let input = multi.take_input::<u64>(0, "data").unwrap();
    let n = 600u64;
    let sender = std::thread::spawn(move || {
        for i in 0..n {
            input.send(0, vec![i]).unwrap();
        }
        input.advance_to(1).unwrap();
        drop(input);
    });
    std::thread::sleep(Duration::from_millis(800));
    let mut markers = Vec::new();
    let mut echoed = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        match out.recv_timeout(Duration::from_millis(500)) {
            Some(OutputEvent::Data { data, .. }) => {
                for v in data {
                    if v >= MARK {
                        markers.push(v - MARK);
                    } else {
                        echoed += 1;
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Some(_) => {}
            None => {
                if echoed == n && !markers.is_empty() {
                    break;
                }
            }
        }
    }
    sender.join().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let _ = rx.recv_timeout(Duration::from_secs(10));
    eprintln!(
        "single worker: markers={} first={:?} echoed={echoed}",
        markers.len(),
        markers.first()
    );
    assert_eq!(markers, vec![n], "epoch 0 must be counted exactly once");
}
