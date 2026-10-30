//! Audit tests (operator semantics): iterate / feedback loops.
//!
//! Each test encodes the *expected* (correct) behaviour; a failing test is a
//! reproduced bug.

use std::sync::mpsc;
use std::time::Duration;

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
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

type PT = Product<u64, u32>;

/// Run `build` on `workers` workers (spawn_multi), feed `data` into worker 0's
/// "in" port (other workers' inputs closed), and return all "out" records
/// sorted.
fn run_multi(
    name: &str,
    workers: usize,
    build: impl Fn(&mut DataflowBuilder<u64>) -> instancy::Result<()> + Send + Sync + 'static,
    data: Vec<(u64, Vec<i64>)>,
) -> Vec<(u64, i64)> {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(name, workers, build, SpawnOptions::default())
        .unwrap();
    let tx = multi.take_input::<i64>(0, "in").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<i64>(w, "in").unwrap());
    }
    let mut collectors = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<i64>(w, "out").unwrap();
        collectors.push(std::thread::spawn(move || rx.collect_data()));
    }
    for (t, d) in data {
        tx.send(t, d).unwrap();
    }
    drop(tx);
    let name = name.to_string();
    with_timeout(&name, TIMEOUT, move || multi.join_blocking()).unwrap();
    let mut out: Vec<(u64, i64)> = Vec::new();
    for c in collectors {
        for (t, d) in c.join().unwrap() {
            out.extend(d.into_iter().map(|x| (t, x)));
        }
    }
    out.sort();
    out
}

/// `distinct` (a notify-based operator) inside an iterate body. Per (outer,
/// inner) time duplicates are removed, everything else must flow normally.
///
/// input [0,0,1]: iter0 -> inc [1,1,2] -> distinct [1,2] -> ... both reach 5.
#[test]
fn distinct_inside_iterate_body() {
    let out = run_multi(
        "distinct_in_loop",
        1,
        |b| {
            b.input::<i64>("in")?
                .iterate::<u32>("loop", 1u32, |v| {
                    let s = v.map("inc", |_t: &PT, x: i64| x + 1).distinct("dedup");
                    let done = s.clone().filter("done", |_t, x| *x >= 5);
                    let again = s.filter("again", |_t, x| *x < 5);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                })
                .output("out")?;
            Ok(())
        },
        vec![(0, vec![0, 0, 1])],
    );
    assert_eq!(out, vec![(0, 5), (0, 5)]);
}

/// Identity `delay_batch` (buffer until the frontier passes t) inside an
/// iterate body must not change the result of the loop.
#[test]
fn identity_delay_inside_iterate_body() {
    let out = run_multi(
        "delay_in_loop",
        1,
        |b| {
            b.input::<i64>("in")?
                .iterate::<u32>("loop", 1u32, |v| {
                    let s = v
                        .map("inc", |_t: &PT, x: i64| x + 1)
                        .delay_batch("hold", |t: &PT| *t);
                    let done = s.clone().filter("done", |_t, x| *x >= 3);
                    let again = s.filter("again", |_t, x| *x < 3);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                })
                .output("out")?;
            Ok(())
        },
        vec![(0, vec![0, 1])],
    );
    assert_eq!(out, vec![(0, 3), (0, 3)]);
}

fn collatz_steps(mut x: i64) -> i64 {
    let mut s = 0;
    while x != 1 {
        x = if x % 2 == 0 { x / 2 } else { 3 * x + 1 };
        s += 1;
    }
    s
}

/// Baseline (expected to pass): Collatz step counting with `map` + exchange in
/// the loop body, 1/2/4 workers, several epochs; outputs must be at the right
/// outer epoch and identical to a sequential oracle.
#[test]
fn collatz_map_exchange_matches_oracle() {
    let mut data = Vec::new();
    let mut expected = Vec::new();
    for epoch in 0..3u64 {
        let vals: Vec<i64> = (1..40).map(|v| v + epoch as i64 * 100).collect();
        for v in &vals {
            expected.push((epoch, collatz_steps(*v) * 1_000_000 + *v));
        }
        data.push((epoch, vals));
    }
    expected.sort();
    for workers in [1usize, 2, 4] {
        let out = run_multi(
            "collatz",
            workers,
            |b| {
                b.input::<i64>("in")?
                    .map("init", |_t, v: i64| ((v, 0i64), v))
                    .iterate::<u32>("loop", 1u32, |v| {
                        let s = v
                            .map("step", |_t: &PT, ((orig, n), x): ((i64, i64), i64)| {
                                if x == 1 {
                                    ((orig, n), x)
                                } else if x % 2 == 0 {
                                    ((orig, n + 1), x / 2)
                                } else {
                                    ((orig, n + 1), 3 * x + 1)
                                }
                            })
                            .exchange_by_hash("route", |r: &((i64, i64), i64)| r.1 as u64);
                        let done = s.clone().filter("done", |_t, r| r.1 == 1);
                        let again = s.filter("again", |_t, r| r.1 != 1);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    })
                    .map("fmt", |_t, ((orig, n), _x): ((i64, i64), i64)| {
                        n * 1_000_000 + orig
                    })
                    .output("out")?;
                Ok(())
            },
            data.clone(),
        );
        assert_eq!(out, expected, "workers={workers}");
    }
}

/// Aggregation downstream of a loop: the loop must hold the outer epoch open
/// while records of that epoch are still circulating, so a per-epoch `count`
/// after the loop emits exactly one value per epoch. Single worker.
#[test]
fn count_after_long_running_loop_single_value_per_epoch() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("count_after_loop");
    builder
        .input::<i64>("in")
        .unwrap()
        .iterate::<u32>("countdown", 1u32, |v| {
            let s = v.map("dec", |_t: &PT, x: i64| x - 1);
            let done = s.clone().filter("done", |_t, x| *x <= 0);
            let again = s.filter("again", |_t, x| *x > 0);
            IterateResult {
                feedback: again,
                output: done,
            }
        })
        .count("per_epoch")
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<i64>("in").unwrap();
    let rx = h.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    for epoch in 0..3u64 {
        tx.send(epoch, vec![3, 2000]).unwrap();
        tx.advance_to(epoch + 1).unwrap();
    }
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let mut by_epoch: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
    for (t, d) in collector.join().unwrap() {
        by_epoch.entry(t).or_default().extend(d);
    }
    for epoch in 0..3u64 {
        assert_eq!(
            by_epoch.get(&epoch).cloned().unwrap_or_default(),
            vec![2],
            "epoch {epoch}: all = {by_epoch:?}"
        );
    }
}

/// Exchange inside the loop body fed by a generic `unary` operator (instead of
/// `map`). Must behave exactly like the `map` version.
#[test]
fn unary_then_exchange_inside_iterate_two_workers() {
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                let s = v
                    .unary("inc", |input, output| {
                        while let Some((t, d)) = input.next() {
                            output.push_vec(t, d.into_iter().map(|x: i64| x + 1).collect());
                        }
                        Ok(())
                    })
                    .exchange_by_hash("route", |x: &i64| *x as u64);
                let done = s.clone().filter("done", |_t, x| *x >= 10);
                let again = s.filter("again", |_t, x| *x < 10);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    let single = run_multi("unary_xchg_1", 1, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(single, vec![(0, 10); 4]);
    let multi = run_multi("unary_xchg_2", 2, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(multi, single);
}

/// Exchange sourced directly by the input (no `map` in between), followed by a
/// plain map-only loop. Common "partition then iterate" shape.
#[test]
fn input_exchange_then_iterate_two_workers() {
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .exchange_by_hash("partition", |x: &i64| *x as u64)
            .iterate::<u32>("loop", 1u32, |v| {
                let s = v.map("inc", |_t: &PT, x: i64| x + 1);
                let done = s.clone().filter("done", |_t, x| *x >= 10);
                let again = s.filter("again", |_t, x| *x < 10);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    let single = run_multi("in_xchg_loop_1", 1, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(single, vec![(0, 10); 4]);
    let multi = run_multi("in_xchg_loop_2", 2, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(multi, single);
}

/// Loop followed by an exchange (e.g. to aggregate the loop's results by key):
/// the exchange is sourced by the loop's `leave` operator.
#[test]
fn iterate_then_exchange_two_workers() {
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                let s = v.map("inc", |_t: &PT, x: i64| x + 1);
                let done = s.clone().filter("done", |_t, x| *x >= 10);
                let again = s.filter("again", |_t, x| *x < 10);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .exchange_by_hash("by_key", |x: &i64| *x as u64)
            .output("out")?;
        Ok(())
    };
    let single = run_multi("loop_xchg_1", 1, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(single, vec![(0, 10); 4]);
    let multi = run_multi("loop_xchg_2", 2, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(multi, single);
}

/// Exchange on the feedback path fed by `branch` (the idiomatic way to split
/// done/again). Must behave exactly like the single-worker run.
#[test]
fn branch_then_exchange_inside_iterate_two_workers() {
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                let (done, again) = v
                    .map("inc", |_t: &PT, x: i64| x + 1)
                    .branch("split", |_t, x| *x >= 10);
                IterateResult {
                    feedback: again.exchange_by_hash("route", |x: &i64| *x as u64),
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    let single = run_multi("branch_xchg_1", 1, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(single, vec![(0, 10); 4]);
    let multi = run_multi("branch_xchg_2", 2, build, vec![(0, vec![1, 2, 3, 4])]);
    assert_eq!(multi, single);
}

/// Broadcast inside a loop body, 2 workers: every record is cloned to both
/// workers each iteration, so a value that loops k times appears 2^k times.
#[test]
fn broadcast_inside_iterate_two_workers() {
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                let s = v.map("inc", |_t: &PT, x: i64| x + 1).broadcast("bc");
                let done = s.clone().filter("done", |_t, x| *x >= 3);
                let again = s.filter("again", |_t, x| *x < 3);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    // 1 -> 2 (x2 copies) -> 3 (x4 copies)
    let multi = run_multi("bcast_loop_2", 2, build, vec![(0, vec![1])]);
    assert_eq!(multi, vec![(0, 3); 4]);
}

/// Data loss in a multi-worker loop: worker 1 holds loop data in its local
/// feedback channel (not tracked by progress: no capability, no in-flight
/// exchange count) across a sweep boundary and then spends a long time in a
/// slow operator before re-exchanging it. Worker 0 meanwhile sees a "complete"
/// global tracker, force-closes after its idle sweeps, and the records routed
/// back to it in the next iteration are dropped.
#[test]
fn loop_data_crossing_sweep_not_lost_two_workers() {
    let n: i64 = std::env::var("AUDIT_LOOP_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let sleep_ms: u64 = std::env::var("AUDIT_LOOP_SLEEP_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let workers: usize = std::env::var("AUDIT_LOOP_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let build = move |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                // iteration k produces values in [k*1000+1000, ...); route by
                // the thousands digit parity so consecutive iterations
                // alternate between workers.
                let s = v
                    .map("slow", move |_t: &PT, x: i64| {
                        std::thread::sleep(Duration::from_millis(sleep_ms));
                        x + 1000
                    })
                    .exchange_by_hash("route", |x: &i64| ((*x / 1000) % 2) as u64);
                let done = s.clone().filter("done", |_t, x| *x >= 3000);
                let again = s.filter("again", |_t, x| *x < 3000);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    let data: Vec<(u64, Vec<i64>)> = vec![(0u64, (0..n).collect())];
    let out = run_multi("loop_sweep_2", workers, build, data);
    let expected: Vec<(u64, i64)> = (0..n).map(|i| (0u64, i + 3000)).collect();
    assert_eq!(
        out.len(),
        expected.len(),
        "lost {} of {} records",
        expected.len() as i64 - out.len() as i64,
        n
    );
    assert_eq!(out, expected);
}

/// Diagnostic companion of `loop_data_crossing_sweep_not_lost_two_workers`:
/// counts slow-map invocations per iteration (value range) and per worker
/// thread, and reports them when the dataflow fails to complete.
#[test]
fn diag_loop_data_crossing_sweep_counters() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let n: i64 = 50;
    let iter_counts: Arc<Vec<AtomicUsize>> =
        Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect());
    let arrivals: Arc<Vec<AtomicUsize>> = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect());
    let ic = iter_counts.clone();
    let ar = arrivals.clone();
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(
            "diag_loop",
            2,
            move |b: &mut DataflowBuilder<u64>| {
                let ic = ic.clone();
                let ar = ar.clone();
                if let Some(s) = std::env::var("AUDIT_IDLE_SWEEPS")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    b.max_idle_sweeps(s);
                }
                b.input::<i64>("in")?
                    .iterate::<u32>("loop", 1u32, move |v| {
                        let s = v
                            .map("slow", move |_t: &PT, x: i64| {
                                ic[(x / 1000).clamp(0, 3) as usize].fetch_add(1, Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(20));
                                x + 1000
                            })
                            .exchange_by_hash("route", |x: &i64| ((*x / 1000) % 2) as u64)
                            .inspect("arrive", move |_t, x| {
                                ar[(*x / 1000).clamp(0, 3) as usize].fetch_add(1, Ordering::SeqCst);
                            });
                        let done = s.clone().filter("done", |_t, x| *x >= 3000);
                        let again = s.filter("again", |_t, x| *x < 3000);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    })
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let tx = multi.take_input::<i64>(0, "in").unwrap();
    drop(multi.take_input::<i64>(1, "in").unwrap());
    let rx0 = multi.take_output::<i64>(0, "out").unwrap();
    let rx1 = multi.take_output::<i64>(1, "out").unwrap();
    let c0 = std::thread::spawn(move || rx0.collect_data());
    let c1 = std::thread::spawn(move || rx1.collect_data());
    tx.send(0, (0..n).collect()).unwrap();
    drop(tx);
    let (jtx, jrx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = jtx.send(multi.join_blocking());
    });
    let res = jrx.recv_timeout(Duration::from_secs(15));
    let snap = |v: &Arc<Vec<AtomicUsize>>| -> Vec<usize> {
        v.iter().map(|a| a.load(Ordering::SeqCst)).collect()
    };
    eprintln!(
        "slow-map invocations by input range [0..1000,1000..2000,2000..3000,3000+]: {:?}",
        snap(&iter_counts)
    );
    eprintln!(
        "post-exchange arrivals by range [0..1000,1000..2000,2000..3000,3000+]: {:?}",
        snap(&arrivals)
    );
    match res {
        Ok(r) => {
            r.unwrap();
            let total: usize = c0
                .join()
                .unwrap()
                .into_iter()
                .chain(c1.join().unwrap())
                .map(|(_, d)| d.len())
                .sum();
            assert_eq!(total, n as usize);
        }
        Err(_) => panic!("hang: see counters above"),
    }
}

/// Staged (per-stage parallelism) loop whose body drops some records (a very
/// common pattern: e.g. BFS / dedup discards records inside the loop).
fn staged_loop_with_drops(drop_seven: bool) -> Vec<i64> {
    staged_loop(drop_seven, 10)
}

fn staged_loop(drop_seven: bool, threshold: i64) -> Vec<i64> {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(
            "staged-drop",
            2,
            move |b: &mut DataflowBuilder<u64>| {
                b.input::<i64>("in")?
                    .iterate::<u32>("loop", 1u32, move |v| {
                        let s = v
                            .map("inc", |_t: &PT, x: i64| x + 1)
                            .exchange_by_hash("redistribute", |x: &i64| *x as u64)
                            .filter("maybe_drop", move |_t, x| !(drop_seven && *x == 7));
                        let done = s.clone().filter("done", move |_t, x| *x >= threshold);
                        let again = s.filter("again", move |_t, x| *x < threshold);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    })
                    .exchange_to("gather", 1, |x: &i64| *x as u64)?
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(true),
        )
        .unwrap();
    let tx = multi.take_input::<i64>(0, "in").unwrap();
    drop(multi.take_input::<i64>(1, "in").unwrap());
    let mut cs = Vec::new();
    for w in 0..multi.num_workers() {
        let rx = multi.take_output::<i64>(w, "out").unwrap();
        cs.push(std::thread::spawn(move || rx.collect_data()));
    }
    tx.send(0, vec![1, 8]).unwrap();
    drop(tx);
    with_timeout("staged join", TIMEOUT, move || multi.join_blocking()).unwrap();
    let mut out: Vec<i64> = cs
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .flat_map(|(_, d)| d)
        .collect();
    out.sort();
    out
}

#[test]
fn staged_loop_control_without_drops() {
    assert_eq!(staged_loop_with_drops(false), vec![10, 10]);
}

#[test]
fn staged_loop_with_dropped_records_terminates() {
    // 1 -> ... -> 7 (dropped); 8 -> 9 -> 10 (exits)
    assert_eq!(staged_loop_with_drops(true), vec![10]);
}

/// No drops, but records leave the loop on a different worker than the one
/// they entered on (final value 11 is routed to worker 1; all input enters on
/// worker 0).
#[test]
fn staged_loop_exit_on_other_worker_terminates() {
    assert_eq!(staged_loop(false, 11), vec![11, 11]);
}

/// Data loss in a multi-worker loop: worker 0 holds loop data in an operator's
/// pending (backpressured) output while every capability is released and
/// every exchange message has been consumed. Worker 1 then sees a "complete"
/// global tracker, force-closes after its idle sweeps, and records later routed
/// to it are dropped.
#[test]
fn backpressured_loop_data_not_lost_two_workers() {
    let n: i64 = std::env::var("AUDIT_BP_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let build = |b: &mut DataflowBuilder<u64>| {
        b.input::<i64>("in")?
            .iterate::<u32>("loop", 1u32, |v| {
                // values < 1000 are routed to worker 0, values >= 1000 to worker 1
                let s = v
                    .exchange_by_hash("route", |x: &i64| (*x >= 1000) as u64)
                    .with_capacity(1)
                    .unwrap()
                    .map("slow", |_t: &PT, x: i64| {
                        std::thread::sleep(Duration::from_millis(25));
                        x + 1000
                    });
                let done = s.clone().filter("done", |_t, x| *x >= 2000);
                let again = s.filter("again", |_t, x| *x < 2000);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("out")?;
        Ok(())
    };
    let data: Vec<(u64, Vec<i64>)> = (0..n).map(|i| (0u64, vec![i])).collect();
    let out = run_multi("bp_loop_2", 2, build, data);
    let expected: Vec<(u64, i64)> = (0..n).map(|i| (0u64, i + 2000)).collect();
    assert_eq!(
        out.len(),
        expected.len(),
        "lost {} of {} records",
        expected.len() as i64 - out.len() as i64,
        n
    );
    assert_eq!(out, expected);
}
