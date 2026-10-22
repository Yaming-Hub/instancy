//! Audit: exchange fed by an operator whose factory does not call
//! `wire_inflight_source` (e.g. the `input` source, `unary_notify`, `concat`).
//!
//! The exchange consumer always records `-n` on the in-flight reporter, but the
//! producer never records `+n`, so the tracker's in-flight count for that
//! exchange target goes permanently negative (`total_counts != 0`) and the
//! tracker can never report completion. Feedback loops are only torn down by
//! the executor's force-close path, which requires `tracker.is_completed()`.

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Src {
    Input,
    Map,
    Notify,
}

fn run(workers: usize, src: Src) -> Result<Vec<u64>, String> {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-unwired-exchange-loop",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("data").unwrap();
                let s = match src {
                    Src::Input => input,
                    Src::Map => input.map("m", |_t, x| x),
                    Src::Notify => input.unary_notify("pass", move |i, o, ctx| {
                        while let Some((t, d)) = i.next() {
                            o.push_vec(t, d);
                            let _ = &ctx;
                        }
                        Ok(())
                    }),
                };
                let x = s.exchange_by_hash("x", |x: &u64| *x);
                let out = x.iterate::<u32>("loop", 1u32, |v| {
                    let inc = v.map("inc", |_t: &Product<u64, u32>, x: u64| x + 1);
                    let done = inc.clone().filter("done", |_t, x| *x % 4 == 0);
                    let again = inc.filter("again", |_t, x| *x % 4 != 0);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                });
                out.output("out").unwrap();
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
    sender.send(0, (1..=20).collect()).unwrap();
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("join error: {e}")),
        Err(_) => {
            return Err(format!(
                "HANG: {src:?} -> exchange -> iterate with {workers} workers did not complete in 15s"
            ));
        }
    }
    let mut v: Vec<u64> = outs
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .flat_map(|(_, d)| d)
        .collect();
    v.sort();
    Ok(v)
}

fn expected() -> Vec<u64> {
    let mut v: Vec<u64> = (1..=20u64).map(|x| (x / 4 + 1) * 4).collect();
    v.sort();
    v
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn map_exchange_iterate_two_workers_completes() {
    assert_eq!(run(2, Src::Map).unwrap(), expected());
}

#[test]
#[ignore = "expected failure (C2); see EXPECTED in tests/audit_repros.rs"]
fn input_exchange_iterate_two_workers_completes() {
    assert_eq!(run(2, Src::Input).unwrap(), expected());
}

#[test]
#[ignore = "expected failure (C2); see EXPECTED in tests/audit_repros.rs"]
fn notify_exchange_iterate_two_workers_completes() {
    assert_eq!(run(2, Src::Notify).unwrap(), expected());
}
