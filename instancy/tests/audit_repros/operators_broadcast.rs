//! Audit tests (operator semantics): broadcast / gather / rebalance / exchange.
//!
//! Each test encodes the *expected* (correct) behaviour; a failing test is a
//! reproduced bug.

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(20);

fn with_timeout<R: Send + 'static>(
    what: &str,
    timeout: Duration,
    f: impl FnOnce() -> R + Send + 'static,
) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(timeout) {
        Ok(r) => r,
        Err(_) => panic!("{what}: did not finish within {timeout:?} (hang)"),
    }
}

fn rt() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Streaming aggregation after broadcast: once the input frontier passes
/// epoch 0, the per-epoch count for epoch 0 must be emitted while the input is
/// still open (it must not wait for the input to be closed).
fn broadcast_then_count_emits_before_close(workers: usize) {
    let rt = rt();
    let mut multi = rt
        .spawn_multi(
            "bcast_count",
            workers,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .broadcast("bc")
                    .count("cnt")
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let mut senders = Vec::new();
    for w in 0..workers {
        senders.push(multi.take_input::<u64>(w, "in").unwrap());
    }
    let mut rxs = Vec::new();
    for w in 0..workers {
        rxs.push(multi.take_output::<usize>(w, "out").unwrap());
    }
    senders[0].send(0, vec![1, 2, 3]).unwrap();
    for s in &senders {
        s.advance_to(1).unwrap();
    }
    // Every worker received 3 records at epoch 0 and its input frontier is 1.
    let mut early = Vec::new();
    for rx in &rxs {
        early.push(rx.recv_timeout(Duration::from_secs(3)));
    }
    drop(senders);
    with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
    for (w, e) in early.iter().enumerate() {
        assert!(
            e.is_some(),
            "workers={workers}: worker {w} produced no epoch-0 count within 3s after the frontier passed 0 (input still open)"
        );
    }
}

#[test]
#[ignore = "expected failure (H10); see EXPECTED in tests/audit_repros.rs"]
fn broadcast_then_count_emits_before_close_1_worker() {
    broadcast_then_count_emits_before_close(1);
}

#[test]
#[ignore = "expected failure (H10); see EXPECTED in tests/audit_repros.rs"]
fn broadcast_then_count_emits_before_close_2_workers() {
    broadcast_then_count_emits_before_close(2);
}

/// Control: same shape with `exchange` instead of `broadcast` (expected to pass).
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn exchange_then_count_emits_before_close_2_workers() {
    let rt = rt();
    let mut multi = rt
        .spawn_multi(
            "xchg_count",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .map("id", |_t, x| x)
                    .exchange_by_hash("x", |_x: &u64| 0)
                    .count("cnt")
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let s0 = multi.take_input::<u64>(0, "in").unwrap();
    let s1 = multi.take_input::<u64>(1, "in").unwrap();
    let rx0 = multi.take_output::<usize>(0, "out").unwrap();
    let _rx1 = multi.take_output::<usize>(1, "out").unwrap();
    s0.send(0, vec![1, 2, 3]).unwrap();
    s0.advance_to(1).unwrap();
    s1.advance_to(1).unwrap();
    let early = rx0.recv_timeout(Duration::from_secs(3));
    drop(s0);
    drop(s1);
    with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
    assert!(early.is_some(), "exchange control: no early count");
}

/// Collect `(worker, time, value)` outputs from a multi-worker dataflow whose
/// worker w receives `per_worker_input[w]`.
fn run_collect(
    name: &str,
    workers: usize,
    build: impl Fn(&mut DataflowBuilder<u64>) -> instancy::Result<()> + Send + Sync + 'static,
    per_worker_input: Vec<Vec<(u64, Vec<u64>)>>,
) -> Vec<Vec<(u64, u64)>> {
    let rt = rt();
    let mut multi = rt
        .spawn_multi(name, workers, build, SpawnOptions::default())
        .unwrap();
    let mut senders = Vec::new();
    for w in 0..workers {
        senders.push(multi.take_input::<u64>(w, "in").unwrap());
    }
    let mut cs = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        cs.push(std::thread::spawn(move || rx.collect_data()));
    }
    for (w, batches) in per_worker_input.into_iter().enumerate() {
        for (t, d) in batches {
            senders[w].send(t, d).unwrap();
        }
    }
    drop(senders);
    let n = name.to_string();
    with_timeout(&n, TIMEOUT, move || multi.join_blocking()).unwrap();
    cs.into_iter()
        .map(|c| {
            let mut v: Vec<(u64, u64)> = c
                .join()
                .unwrap()
                .into_iter()
                .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
                .collect();
            v.sort();
            v
        })
        .collect()
}

fn inputs(workers: usize) -> (Vec<Vec<(u64, Vec<u64>)>>, Vec<(u64, u64)>) {
    let mut per = vec![Vec::new(); workers];
    let mut all = Vec::new();
    for w in 0..workers {
        for epoch in 0..3u64 {
            let d: Vec<u64> = (0..50u64)
                .map(|i| w as u64 * 10_000 + epoch * 100 + i)
                .collect();
            all.extend(d.iter().map(|x| (epoch, *x)));
            per[w].push((epoch, d));
        }
    }
    all.sort();
    (per, all)
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn broadcast_every_record_to_every_worker_exactly_once() {
    for workers in [2usize, 3, 4] {
        let (per, all) = inputs(workers);
        let out = run_collect(
            "bcast_exact",
            workers,
            |b| {
                b.input::<u64>("in")?.broadcast("bc").output("out")?;
                Ok(())
            },
            per,
        );
        for (w, got) in out.iter().enumerate() {
            assert_eq!(got, &all, "workers={workers} worker={w}");
        }
    }
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn rebalance_and_gather_lose_nothing() {
    for workers in [2usize, 3, 4] {
        let (per, all) = inputs(workers);
        let out = run_collect(
            "rebalance_exact",
            workers,
            |b| {
                b.input::<u64>("in")?
                    .map("id", |_t, x| x)
                    .rebalance("rb")
                    .output("out")?;
                Ok(())
            },
            per.clone(),
        );
        let mut merged: Vec<(u64, u64)> = out.concat();
        merged.sort();
        assert_eq!(merged, all, "rebalance workers={workers}");

        let out = run_collect(
            "gather_exact",
            workers,
            |b| {
                b.input::<u64>("in")?
                    .map("id", |_t, x| x)
                    .gather("g")
                    .output("out")?;
                Ok(())
            },
            per,
        );
        assert_eq!(out[0], all, "gather workers={workers}");
        for w in 1..workers {
            assert!(out[w].is_empty(), "gather leaked to worker {w}");
        }
    }
}

/// `unary_async` (no capability while tasks are in flight) feeding an exchange
/// in a 2-worker dataflow. Worker 1 has no local data; it must not finish
/// before worker 0's async results are routed to it.
fn unary_async_then_exchange(with_map: bool) {
    use std::sync::Arc;
    let rt = rt();
    let _enter = rt.tokio_handle().enter();
    let mut multi = rt
        .spawn_multi(
            "async_xchg",
            2,
            move |b: &mut DataflowBuilder<u64>| {
                let s = b.input::<u64>("in")?.unary_async(
                    "slow_lookup",
                    4,
                    Arc::new(|_t: u64, batch: Vec<u64>| async move {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        Ok(batch)
                    }),
                );
                let s = if with_map { s.map("id", |_t, x| x) } else { s };
                s.exchange_by_hash("x", |x: &u64| *x).output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let s0 = multi.take_input::<u64>(0, "in").unwrap();
    drop(multi.take_input::<u64>(1, "in").unwrap());
    let mut cs = Vec::new();
    for w in 0..2 {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        cs.push(std::thread::spawn(move || rx.collect_data()));
    }
    s0.send(0, (0..10).collect()).unwrap();
    drop(s0);
    with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
    let mut all: Vec<u64> = cs
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .flat_map(|(_, d)| d)
        .collect();
    all.sort();
    assert_eq!(all, (0..10).collect::<Vec<u64>>(), "with_map={with_map}");
}

#[test]
#[ignore = "expected failure (O-4/C2); see EXPECTED in tests/audit_repros.rs"]
fn unary_async_then_exchange_loses_nothing() {
    unary_async_then_exchange(false);
}

#[test]
#[ignore = "expected failure (O-4/H1); see EXPECTED in tests/audit_repros.rs"]
fn unary_async_map_then_exchange_loses_nothing() {
    unary_async_then_exchange(true);
}

/// Per-key aggregation after exchange directly from an input (no map in
/// between) in a multi-worker dataflow: counts per epoch must be exact.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn input_exchange_count_is_exact_per_epoch() {
    let workers = 3;
    let (per, all) = inputs(workers);
    let mut expected: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    for (t, x) in &all {
        *expected.entry((*t, x % 7)).or_default() += 1;
    }
    let out = run_collect(
        "input_xchg_count",
        workers,
        |b| {
            b.input::<u64>("in")?
                .exchange_by_hash("by_key", |x: &u64| x % 7)
                .map("key", |_t, x| x % 7)
                .unary_notify("count_per_key", {
                    let mut stash: BTreeMap<u64, BTreeMap<u64, u64>> = BTreeMap::new();
                    move |input, output, ctx| {
                        while let Some((t, d)) = input.next() {
                            let e = stash.entry(t).or_default();
                            for k in d {
                                *e.entry(k).or_default() += 1;
                            }
                            ctx.notify_at(t);
                        }
                        while let Some(t) = ctx.next_notification() {
                            if let Some(m) = stash.remove(&t) {
                                output.push_vec(
                                    t,
                                    m.into_iter().map(|(k, c)| k * 1_000_000 + c).collect(),
                                );
                            }
                        }
                        Ok(())
                    }
                })
                .output("out")?;
            Ok(())
        },
        per,
    );
    let mut got: BTreeMap<(u64, u64), Vec<u64>> = BTreeMap::new();
    for v in out {
        for (t, packed) in v {
            got.entry((t, packed / 1_000_000))
                .or_default()
                .push(packed % 1_000_000);
        }
    }
    for (k, c) in &expected {
        assert_eq!(got.get(k), Some(&vec![*c]), "key {k:?}");
    }
    assert_eq!(got.len(), expected.len());
}

/// In-process control for the cluster backpressure test: 2 workers, 3000
/// batches each holding records for both workers.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn exchange_high_volume_in_process_exactly_once() {
    let mut per = vec![Vec::new(), Vec::new()];
    let mut all = Vec::new();
    let nb: u64 = std::env::var("AUDIT_XV_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);
    for i in 0..nb {
        let d: Vec<u64> = (0..8).map(|j| i * 8 + j).collect();
        all.extend(d.iter().map(|x| (0u64, *x)));
        per[0].push((0u64, d));
    }
    all.sort();
    let out = run_collect(
        "xchg_volume",
        2,
        |b| {
            b.input::<u64>("in")?
                .map("id", |_t, x| x)
                .exchange_by_hash("x", |x: &u64| {
                    match std::env::var("AUDIT_XV_ROUTE").as_deref() {
                        Ok("0") => 0,
                        Ok("1") => 1,
                        _ => *x,
                    }
                })
                .output("out")?;
            Ok(())
        },
        per,
    );
    let mut merged = out.concat();
    merged.sort();
    assert_eq!(merged.len(), all.len());
    assert_eq!(merged, all);
}
