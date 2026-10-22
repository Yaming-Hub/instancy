//! Audit (builder/broadcast): `add_broadcast_internal` registers the broadcast
//! operator with an initial capability at `T::minimum()` that is only released
//! when its input is exhausted. Everything downstream therefore has its
//! frontier pinned at `T::minimum()` for as long as the input stream is open.

use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn rt() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Wait up to `timeout` for a `Data` event on the receiver.
fn wait_data(
    r: &instancy::OutputReceiver<u64, usize>,
    timeout: Duration,
) -> Option<(u64, Vec<usize>)> {
    let deadline = std::time::Instant::now() + timeout;
    while let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) {
        match r.recv_timeout(left) {
            Some(instancy::dataflow::OutputEvent::Data { time, data }) => {
                return Some((time, data));
            }
            Some(_) => continue,
            None => return None,
        }
    }
    None
}

fn run(use_broadcast: bool, workers: usize) -> Option<(u64, Vec<usize>)> {
    let rt = rt();
    let mut multi = rt
        .spawn_multi(
            "bcast-frontier",
            workers,
            move |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("data")?;
                let s = if use_broadcast {
                    input.broadcast("b")
                } else {
                    input.exchange_by_hash("x", |x: &u64| *x)
                };
                s.count("cnt").output("counts")?;
                Ok(())
            },
            SpawnOptions::new()
                .per_stage_parallelism(false)
                .auto_parallelism(false),
        )
        .unwrap();
    let mut senders = Vec::new();
    for w in 0..workers {
        senders.push(multi.take_input::<u64>(w, "data").unwrap());
    }
    let out0 = multi.take_output::<usize>(0, "counts").unwrap();
    senders[0].send(0, vec![0, 2, 4, 6]).unwrap();
    for s in &senders {
        s.advance_to(1).unwrap();
    }
    // Epoch 0 is closed on every worker; its count should be emitted while the
    // inputs remain open.
    let got = wait_data(&out0, Duration::from_secs(5));
    multi.cancel();
    drop(senders);
    std::mem::forget(rt);
    got
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn audit_builder_exchange_epoch_completes_while_input_open() {
    let got = run(false, 2);
    eprintln!("exchange: {got:?}");
    assert!(got.is_some());
}

#[test]
#[ignore = "expected failure (H10); see EXPECTED in tests/audit_repros.rs"]
fn audit_builder_broadcast_epoch_never_completes_while_input_open_multi() {
    let got = run(true, 2);
    eprintln!("broadcast (2 workers): {got:?}");
    assert!(
        got.is_some(),
        "count for epoch 0 downstream of broadcast never emitted while input open"
    );
}

#[test]
#[ignore = "expected failure (H10); see EXPECTED in tests/audit_repros.rs"]
fn audit_builder_broadcast_epoch_never_completes_while_input_open_single() {
    let got = run(true, 1);
    eprintln!("broadcast (1 worker): {got:?}");
    assert!(
        got.is_some(),
        "count for epoch 0 downstream of broadcast never emitted while input open"
    );
}

/// Broadcast inside an iterate body (2 workers). The broadcast's initial
/// capability is registered in the inner SubgraphBuilder and is dropped when
/// the loop body is merged (merge uses add_operator without capabilities), and
/// broadcast registers no in-flight binding either.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn audit_builder_broadcast_inside_iterate_multi() {
    use instancy::dataflow::dataflow_builder::IterateResult;
    use instancy::order::Product;
    let mut bad = Vec::new();
    for round in 0..10 {
        let rt = rt();
        let mut multi = rt
            .spawn_multi(
                "bcast-loop",
                2,
                |b: &mut DataflowBuilder<u64>| {
                    let input = b.input::<u64>("data")?;
                    let out = input.iterate::<u32>("loop", 1u32, |v| {
                        let s = v
                            .map("inc", |_t: &Product<u64, u32>, x: u64| x + 1)
                            .broadcast("b");
                        let done = s.clone().filter("done", |_t, x| *x >= 3);
                        let again = s.filter("again", |_t, x| *x < 3);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    });
                    out.output("results")?;
                    Ok(())
                },
                SpawnOptions::new()
                    .per_stage_parallelism(false)
                    .auto_parallelism(false),
            )
            .unwrap();
        let s0 = multi.take_input::<u64>(0, "data").unwrap();
        drop(multi.take_input::<u64>(1, "data").unwrap());
        let outs: Vec<_> = (0..2)
            .map(|w| multi.take_output::<u64>(w, "results").unwrap())
            .collect();
        let drains: Vec<_> = outs
            .into_iter()
            .map(|r| std::thread::spawn(move || r.collect_data()))
            .collect();
        s0.send(0, vec![0]).unwrap();
        drop(s0);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(multi.join_blocking());
        });
        let res = rx.recv_timeout(Duration::from_secs(20));
        let ok = matches!(res, Ok(Ok(())));
        let n: usize = if ok {
            drains
                .into_iter()
                .flat_map(|d| d.join().unwrap())
                .map(|(_, d)| d.len())
                .sum()
        } else {
            usize::MAX
        };
        std::mem::forget(rt);
        if n != 8 {
            bad.push((round, n, format!("{res:?}")));
        }
    }
    eprintln!("bad rounds (round, outputs, join): {bad:?}");
    assert!(bad.is_empty(), "expected 8 outputs (value 3) per round");
}
