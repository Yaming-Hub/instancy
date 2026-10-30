//! Audit (executor consumer side): stage-task scheduling livelock.
//!
//! `FusedStageTask::activate` counts `BlockedOnBackpressure` as a productive
//! activation and keeps multi-passing until the per-sweep budget
//! (`max_activations_per_step` = 1024) is exhausted;
//! `run_stage_task_activation` then skips every later stage task
//! (`remaining_budget == 0 -> break`). If an operator in an earlier stage is
//! blocked on a channel whose consumer lives in a LATER stage task of the
//! same worker, the consumer never runs again: livelock.

use std::time::Duration;

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn build_cap(builder: &mut DataflowBuilder<u64>, cap: usize) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .with_capacity(cap)?
        .exchange_by_hash("to-w0", |_x: &u64| 0)
        .map("id2", |_t, x| x)
        .output("out")?;
    Ok(())
}

fn run(cap: usize) -> (bool, usize) {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-stage-starvation",
            2,
            move |b: &mut DataflowBuilder<u64>| build_cap(b, cap),
            SpawnOptions::new(),
        )
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let _out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();
    drop(in1);
    let collector = std::thread::spawn(move || {
        let mut n = 0usize;
        while let Some(ev) = out0.recv_timeout(Duration::from_secs(5)) {
            if let OutputEvent::Data { data, .. } = ev {
                n += data.len();
            }
        }
        n
    });
    for i in 0..50u64 {
        in0.send(0, vec![i]).unwrap();
    }
    drop(in0);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(10));
    let n = collector.join().unwrap();
    eprintln!("cap={cap} join={join:?} delivered={n}");
    (matches!(join, Ok(Ok(()))), n)
}

/// Control: default-sized exchange channel, same traffic.
#[test]
fn control_large_exchange_channel_completes() {
    assert_eq!(run(1024), (true, 50));
}

#[test]
fn upstream_stage_backpressure_starves_downstream_stage() {
    let (ok, n) = run(1);
    assert!(ok, "livelock: dataflow never completed, delivered {n}/50");
    assert_eq!(n, 50);
}

/// Default channel capacity (1024): a burst of more envelopes than the
/// exchange channel holds, all routed to the sending worker itself.
#[test]
fn default_capacity_burst_to_self_through_exchange() {
    burst(2);
}

/// Same burst with a single worker (exchange degenerates to a local channel,
/// but stages are still split at the exchange edge).
#[test]
fn default_capacity_burst_single_worker() {
    burst(1);
}

fn burst(workers: usize) {
    burst_with(workers, 5000, true)
}

/// Controls: same burst without the exchange edge (single stage), and a
/// small burst through the exchange.
#[test]
fn control_burst_without_exchange_completes() {
    burst_with(1, 5000, false);
}

#[test]
fn control_small_burst_through_exchange_completes() {
    burst_with(1, 100, true);
}

fn burst_with(workers: usize, total: u64, with_exchange: bool) {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-stage-burst",
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
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let mut others = Vec::new();
    for w in 1..workers {
        others.push(multi.take_output::<u64>(w, "out").unwrap());
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    let collector = std::thread::spawn(move || {
        let mut n = 0u64;
        while let Some(ev) = out0.recv_timeout(Duration::from_secs(5)) {
            if let OutputEvent::Data { data, .. } = ev {
                n += data.len() as u64;
            }
        }
        n
    });
    let feeder = std::thread::spawn(move || {
        for i in 0..total {
            if in0.send(0, vec![i]).is_err() {
                break;
            }
        }
    });
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        feeder.join().unwrap();
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(20));
    let n = collector.join().unwrap();
    eprintln!("burst workers={workers}: join={join:?} delivered={n}/{total}");
    assert!(
        matches!(join, Ok(Ok(()))),
        "burst livelock: delivered {n}/{total}"
    );
    assert_eq!(n, total);
}
