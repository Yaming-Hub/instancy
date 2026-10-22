//! Audit: frontier (probe) behaviour when one output feeds two exchange edges.
//!
//! In-flight reporters are looked up by the exchange edge's source `(op, output)`
//! (`SubgraphBuilder::materialization_reporters`), so two exchanges off the same
//! output collide and the producer records `+n` for *both* edges on one
//! reporter. Expectation: after `advance_to(1)` and all data delivered, every
//! probe frontier should pass time 0 while the input is still open.

use instancy::{DataflowBuilder, ProbeHandle, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Probes = Arc<Mutex<Vec<(String, ProbeHandle<u64>)>>>;

fn wait_all_done_with(probes: &Probes, t: u64, timeout: Duration) -> Vec<(String, String)> {
    let deadline = Instant::now() + timeout;
    loop {
        let stuck: Vec<(String, String)> = probes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p)| p.subscribe().has_changed().is_ok() && !p.done_with(&t))
            .map(|(n, p)| (n.clone(), format!("{:?}", p.frontier().elements())))
            .collect();
        if stuck.is_empty() || Instant::now() > deadline {
            return stuck;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run(workers: usize, mode: u8) -> Vec<(String, String)> {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let probes: Probes = Arc::new(Mutex::new(Vec::new()));
    let p2 = probes.clone();
    let mut multi = rt
        .spawn_multi(
            "audit-fanout-probe",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("data").unwrap();
                let mapped = input.map("m", |_t, x| x);
                let a = if mode >= 1 {
                    mapped.clone().exchange_by_hash("x1", |x: &u64| *x)
                } else {
                    mapped.clone().map("m1", |_t, x| x)
                };
                let (a, pa) = a.probe();
                a.output("o1").unwrap();
                p2.lock().unwrap().push(("x1".into(), pa));
                if mode == 2 {
                    let bb = mapped.exchange_by_hash("x2", |x: &u64| x.wrapping_mul(7));
                    let (bb, pb) = bb.probe();
                    bb.output("o2").unwrap();
                    p2.lock().unwrap().push(("x2".into(), pb));
                } else {
                    let (bb, pb) = mapped.probe();
                    bb.output("o2").unwrap();
                    p2.lock().unwrap().push(("pipe".into(), pb));
                }
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let sender = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    let mut outs = Vec::new();
    for w in 0..workers {
        outs.push(multi.take_output::<u64>(w, "o1").unwrap());
        outs.push(multi.take_output::<u64>(w, "o2").unwrap());
    }
    sender.send(0, (0..50).collect()).unwrap();
    sender.advance_to(1).unwrap();
    let stuck = wait_all_done_with(&probes, 0, Duration::from_secs(5));
    {
        let g = probes.lock().unwrap();
        let live = g
            .iter()
            .filter(|(_, p)| p.subscribe().has_changed().is_ok())
            .count();
        eprintln!("probes total = {}, live = {}", g.len(), live);
    }
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("join hang")
        .expect("join error");
    stuck
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn baseline_one_exchange_two_workers_frontier_passes_zero() {
    let stuck = run(2, 1);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn two_exchanges_one_output_single_worker_frontier_passes_zero() {
    let stuck = run(1, 2);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}

#[test]
#[ignore = "expected failure (H12); see EXPECTED in tests/audit_repros.rs"]
fn two_exchanges_one_output_two_workers_frontier_passes_zero() {
    let stuck = run(2, 2);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn baseline_no_exchange_single_worker() {
    let stuck = run(1, 0);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn baseline_no_exchange_two_workers() {
    let stuck = run(2, 0);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn baseline_one_exchange_single_worker() {
    let stuck = run(1, 1);
    assert!(stuck.is_empty(), "probes stuck at <= 0: {stuck:?}");
}
