//! Audit: `Tracker::tracking_anything()` is `total_counts != 0`, a raw sum of
//! pointstamp diffs over *all* locations and times. Positive capabilities at
//! one location can cancel negative (in-flight) counts at another, so a worker
//! can conclude "complete" while a peer still holds a capability. The executor
//! then force-closes all of that worker's operators (quiescence path), and
//! later data routed to it is lost.
//!
//! Deterministic trigger: with `input -> exchange` the producer side of the
//! in-flight accounting is never recorded (only the consumer's `-n`), so after
//! worker 1 consumes exactly one exchanged record its tracker sees
//! `+1 (worker 0's open input capability) + -1 (consumed)` == 0.

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::{Duration, Instant};

fn run(map_first: bool, initial_to_w1: usize) -> (Vec<u64>, Result<(), String>) {
    run_ext(map_first, initial_to_w1, false)
}

/// `slow_sibling`: worker 0 also runs a slow operator on the input in the same
/// sweep, delaying its progress broadcast (the `+1` in-flight for records it
/// just exchanged to worker 1).
fn run_ext(
    map_first: bool,
    initial_to_w1: usize,
    slow_sibling: bool,
) -> (Vec<u64>, Result<(), String>) {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-total-counts",
            2,
            move |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("data").unwrap();
                if slow_sibling {
                    input
                        .clone()
                        .filter("slow", |_t, _x| {
                            std::thread::sleep(Duration::from_millis(100));
                            false
                        })
                        .output("sink")
                        .unwrap();
                }
                let s = if map_first {
                    input.map("m", |_t, x| x)
                } else {
                    input
                };
                s.exchange_by_hash("x", |x: &u64| *x).output("out").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let sender = multi.take_input::<u64>(0, "data").unwrap();
    drop(multi.take_input::<u64>(1, "data").unwrap());
    let o0 = multi.take_output::<u64>(0, "out").unwrap();
    let o1 = multi.take_output::<u64>(1, "out").unwrap();
    if slow_sibling {
        for w in 0..2 {
            let s = multi.take_output::<u64>(w, "sink").unwrap();
            std::thread::spawn(move || s.collect_data());
        }
    }
    let mut got = Vec::new();
    // Find a value that routes to worker 1: send candidates one at a time
    // and watch which output receives it.
    let mut odd_routed = None;
    for v in 1..50u64 {
        sender.send(0, vec![v]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut where_ = None;
        while Instant::now() < deadline && where_.is_none() {
            if let Some(OutputEvent::Data { data, .. }) = o1.recv_timeout(Duration::from_millis(5))
            {
                got.extend(data);
                where_ = Some(1);
            } else if let Some(OutputEvent::Data { data, .. }) =
                o0.recv_timeout(Duration::from_millis(5))
            {
                got.extend(data);
                where_ = Some(0);
            }
        }
        if where_ == Some(1) {
            odd_routed = Some(v);
            break;
        }
    }
    let first = odd_routed.expect("no value routed to worker 1");
    // Optionally route more records to worker 1 before going idle.
    for k in 1..initial_to_w1 as u64 {
        sender.send(0, vec![first + 2 * k * 100]).unwrap();
    }
    // Give worker 1 plenty of idle sweeps with its view summing to zero.
    std::thread::sleep(Duration::from_millis(500));
    // Send more values that go to worker 1 (same residue as `first`).
    for k in 1..=5u64 {
        let v = first + 2 * k * 1000;
        sender.send(0, vec![v]).expect("send failed");
    }
    std::thread::sleep(Duration::from_millis(300));
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("join error: {e}")),
        Err(_) => Err("join hang".into()),
    };
    for (_, d) in o0.collect_data().into_iter().chain(o1.collect_data()) {
        got.extend(d);
    }
    got.sort();
    (got, join)
}

#[test]
#[ignore = "expected failure (C1); see EXPECTED in tests/audit_repros.rs"]
fn input_exchange_late_records_to_idle_worker_are_delivered() {
    let (got, join) = run(false, 1);
    eprintln!("got {got:?} join {join:?}");
    assert!(join.is_ok(), "{join:?}");
    assert!(
        got.len() >= 6,
        "records lost after worker 1 spuriously completed: got {got:?}"
    );
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn map_exchange_late_records_to_idle_worker_are_delivered() {
    let (got, join) = run(true, 1);
    eprintln!("got {got:?} join {join:?}");
    assert!(join.is_ok(), "{join:?}");
    assert!(got.len() >= 6, "records lost: got {got:?}");
}

/// Control: after consuming TWO records worker 1's sum is `+1 - 2 = -1 != 0`,
/// so it does not spuriously complete and late records are delivered. This
/// isolates the exact-zero cancellation as the trigger.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn input_exchange_two_initial_records_control() {
    let (got, join) = run(false, 2);
    eprintln!("got {got:?} join {join:?}");
    assert!(join.is_ok(), "{join:?}");
    assert!(got.len() >= 7, "records lost: got {got:?}");
}

/// Correctly wired `map -> exchange`; worker 0's progress broadcast is delayed
/// by a slow sibling operator, so worker 1 transiently sums to zero.
#[test]
#[ignore = "expected failure (C1, intermittent); see EXPECTED in tests/audit_repros.rs"]
fn map_exchange_slow_sibling_late_records_delivered() {
    let mut losses = 0;
    let mut sample = Vec::new();
    for _ in 0..5 {
        let (got, join) = run_ext(true, 1, true);
        assert!(join.is_ok(), "{join:?}");
        if got.len() < 6 {
            losses += 1;
            sample = got;
        }
    }
    assert_eq!(
        losses, 0,
        "records lost in {losses}/5 rounds (e.g. got {sample:?})"
    );
}
