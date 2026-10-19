//! Regression tests: nested `iterate` scopes, and probes / outputs declared
//! inside loop bodies (previously nested loops failed to materialize and
//! probes/outputs in a loop body were silently dropped).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::dataflow::probe::ProbeHandle;
use instancy::order::Product;
use instancy::{DataflowBuilder, IoMode, RuntimeConfig, RuntimeHandle, SpawnOptions};

type P1 = Product<u64, u32>;
type P2 = Product<P1, u32>;
/// The shared vector returned by Pipe::collect.
type Collected<T> = Arc<Mutex<Vec<(T, Vec<i64>)>>>;

fn rt() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn join_spawned<T: instancy::progress::timestamp::Timestamp>(
    s: instancy::runtime::SpawnedDataflow<T>,
) -> instancy::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(s.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("dataflow did not complete within 20s (hang)")
}

/// Inner loop: +1 until a multiple of 5. Outer loop: +1 after the inner loop
/// until the value reaches 20.
fn nested_body(
    outer: instancy::Pipe<P1, i64>,
    inner_exchange: bool,
    outer_exchange: bool,
) -> IterateResult<P1, i64> {
    let inner_out = outer.iterate::<u32>("inner", 1u32, move |inner| {
        let mut stepped = inner.map("inc", |_t: &P2, x: i64| x + 1);
        if inner_exchange {
            stepped = stepped.exchange_by_hash("inner-x", |x: &i64| *x as u64);
        }
        let done = stepped.clone().filter("idone", |_t, x| *x % 5 == 0);
        let again = stepped.filter("iagain", |_t, x| *x % 5 != 0);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    let mut stepped = inner_out.map("plus1", |_t: &P1, x: i64| x + 1);
    if outer_exchange {
        stepped = stepped.exchange_by_hash("outer-x", |x: &i64| *x as u64);
    }
    let done = stepped.clone().filter("odone", |_t, x| *x >= 20);
    let again = stepped.filter("oagain", |_t, x| *x < 20);
    IterateResult {
        feedback: again,
        output: done,
    }
}

// ---------------------------------------------------------------------------
// Moved repros (audit M10)
// ---------------------------------------------------------------------------

/// Doubly-nested iterate without any exchange. The loop-body merge used to
/// compute the feedback-factory offset after appending the body's feedback
/// edges, leaving the inner loop's feedback edge without a factory.
#[test]
fn nested_iterate_without_exchange() {
    let builder = DataflowBuilder::<u64>::new("nested-no-exchange");
    let input = builder.input::<i64>("data").unwrap();
    let out = input.iterate::<u32>("outer", 1u32, |outer| nested_body(outer, false, false));
    out.output("results").unwrap();
    let df = builder.build().expect("build");

    let rt = rt();
    let mut spawned = match rt.spawn(df, SpawnOptions::default()) {
        Ok(s) => s,
        Err(e) => panic!("nested iterate (no exchange) failed to spawn: {e} / {e:?}"),
    };
    let tx = spawned.take_input::<i64>("data").unwrap();
    let rx = spawned.take_output::<i64>("results").unwrap();
    tx.send(0, vec![1, 7]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    let mut got: Vec<i64> = rx.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    got.sort();
    // 1 -> inner to 5 -> +1=6 -> inner to 10 -> 11 -> 15 -> 16 -> 20 -> 21 >= 20 done
    // 7 -> 10 -> 11 -> 15 -> 16 -> 20 -> 21
    assert_eq!(got, vec![21, 21]);
}

/// `output()` called inside an iterate body used to register the port only in
/// the throw-away inner builder state, so it was unreachable via `take_output`.
/// It is now exposed, delivering batches at the dataflow's (outer) timestamp.
#[test]
fn output_inside_iterate_body_is_exposed() {
    let builder = DataflowBuilder::<u64>::new("output-in-loop");
    let input = builder.input::<i64>("data").unwrap();
    let out = input.iterate::<u32>("loop", 1u32, |v| {
        let stepped = v.map("inc", |_t: &P1, x: i64| x + 1);
        stepped
            .clone()
            .map("tap", |_t, x| x)
            .output("inner_tap")
            .expect("output inside loop");
        let done = stepped.clone().filter("done", |_t, x| *x >= 3);
        let again = stepped.filter("again", |_t, x| *x < 3);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    out.output("results").unwrap();
    let df = builder.build().expect("build");
    assert!(df.output_names().contains(&"inner_tap"));
    let rt = rt();
    let mut spawned = rt.spawn(df, SpawnOptions::default()).unwrap();
    let tap = spawned
        .take_output::<i64>("inner_tap")
        .expect("output declared inside iterate body must be exposed");
    let tx = spawned.take_input::<i64>("data").unwrap();
    tx.send(4, vec![0]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    // Every iteration's value, reported at the outer epoch 4.
    assert_eq!(
        tap.collect_data(),
        vec![(4, vec![1]), (4, vec![2]), (4, vec![3])]
    );
}

/// `probe()` inside an iterate body used to be registered only in the inner
/// state and never wired to a tracker.
#[test]
fn probe_inside_iterate_body_completes() {
    let probe_slot: Arc<Mutex<Option<ProbeHandle<P1>>>> = Arc::new(Mutex::new(None));
    let builder = DataflowBuilder::<u64>::new("probe-in-loop");
    let input = builder.input::<i64>("data").unwrap();
    let ps = probe_slot.clone();
    let out = input.iterate::<u32>("loop", 1u32, move |v| {
        let stepped = v.map("inc", |_t: &P1, x: i64| x + 1);
        let (stepped, probe) = stepped.probe();
        *ps.lock().unwrap() = Some(probe);
        let done = stepped.clone().filter("done", |_t, x| *x >= 3);
        let again = stepped.filter("again", |_t, x| *x < 3);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    out.output("results").unwrap();
    let df = builder.build().expect("build");
    let rt = rt();
    let mut spawned = rt.spawn(df, SpawnOptions::default()).unwrap();
    let tx = spawned.take_input::<i64>("data").unwrap();
    let rx = spawned.take_output::<i64>("results").unwrap();
    tx.send(0, vec![0]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    let got: Vec<i64> = rx.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    assert_eq!(got, vec![3]);
    let probe = probe_slot.lock().unwrap().take().unwrap();
    assert!(
        probe.is_done(),
        "probe inside iterate body never reports done after the dataflow completed"
    );
}

// ---------------------------------------------------------------------------
// Adversarial
// ---------------------------------------------------------------------------

/// Nested loops with exchanges inside the inner and/or outer body, on one and
/// on two workers (a `map` separates each loop's output from an exchange; see
/// `multi_worker_nested_iterate_inner_exchange` for the shape that still hangs).
/// With two workers an exchange in the inner body used to run without in-flight
/// accounting (its binding was projected through one scope level only), so the
/// outer epoch could complete early and drop results.
#[test]
fn nested_iterate_with_exchanges_single_and_multi_worker() {
    for workers in [1usize, 2] {
        for (ix, ox) in [(false, false), (true, false), (false, true), (true, true)] {
            for _ in 0..3 {
                let (done_tx, done_rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let rt = rt();
                    let mut multi = rt
                        .spawn_multi(
                            "nested-x",
                            workers,
                            move |b| {
                                let input = b.input::<i64>("data")?;
                                input
                                    .iterate::<u32>("outer", 1u32, |o| nested_body(o, ix, ox))
                                    .output("results")?;
                                Ok(())
                            },
                            SpawnOptions::default(),
                        )
                        .unwrap();
                    let tx = multi.take_input::<i64>(0, "data").unwrap();
                    for w in 1..workers {
                        drop(multi.take_input::<i64>(w, "data").unwrap());
                    }
                    let rxs: Vec<_> = (0..workers)
                        .map(|w| multi.take_output::<i64>(w, "results").unwrap())
                        .collect();
                    tx.send(0, vec![1, 2, 3, 7, 13]).unwrap();
                    tx.send(1, vec![19]).unwrap();
                    drop(tx);
                    let r = multi.join_blocking();
                    let mut all: Vec<(u64, i64)> = rxs
                        .into_iter()
                        .flat_map(|r| r.collect_data())
                        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
                        .collect();
                    all.sort();
                    let _ = done_tx.send(r.map(|_| all).map_err(|e| e.to_string()));
                });
                let got = done_rx
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap_or_else(|_| panic!("workers={workers} ix={ix} ox={ox}: hang"))
                    .unwrap();
                let mut expected: Vec<(u64, i64)> = vec![(0, 21); 5];
                expected.push((1, 21));
                assert_eq!(got, expected, "workers={workers} ix={ix} ox={ox}");
            }
        }
    }
}

/// Three loop levels with an exchange in the innermost body, on two workers:
/// the exchange's in-flight accounting must be projected through all three
/// scopes onto the root epoch.
#[test]
fn triple_nested_iterate_with_innermost_exchange() {
    type P3 = Product<P2, u32>;
    for _ in 0..5 {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = rt();
            let mut multi = rt
                .spawn_multi(
                    "triple",
                    2,
                    |b| {
                        let input = b.input::<i64>("data")?;
                        input
                            .iterate::<u32>("l1", 1u32, |v1| {
                                let out2 = v1.iterate::<u32>("l2", 1u32, |v2| {
                                    let out3 = v2.iterate::<u32>("l3", 1u32, |v3| {
                                        let s = v3
                                            .map("inc3", |_t: &P3, x: i64| x + 1)
                                            .exchange_by_hash("x3", |x: &i64| *x as u64);
                                        IterateResult {
                                            feedback: s.clone().filter("a3", |_t, x| *x % 3 != 0),
                                            output: s.filter("d3", |_t, x| *x % 3 == 0),
                                        }
                                    });
                                    let s = out3.map("inc2", |_t: &P2, x: i64| x + 1);
                                    IterateResult {
                                        feedback: s.clone().filter("a2", |_t, x| *x % 7 != 0),
                                        output: s.filter("d2", |_t, x| *x % 7 == 0),
                                    }
                                });
                                let s = out2.map("inc1", |_t: &P1, x: i64| x + 1);
                                IterateResult {
                                    feedback: s.clone().filter("a1", |_t, x| *x < 30),
                                    output: s.filter("d1", |_t, x| *x >= 30),
                                }
                            })
                            .output("results")?;
                        Ok(())
                    },
                    SpawnOptions::default(),
                )
                .unwrap();
            let tx = multi.take_input::<i64>(0, "data").unwrap();
            drop(multi.take_input::<i64>(1, "data").unwrap());
            let rxs: Vec<_> = (0..2)
                .map(|w| multi.take_output::<i64>(w, "results").unwrap())
                .collect();
            tx.send(0, vec![0, 1, 2, 5]).unwrap();
            drop(tx);
            let r = multi.join_blocking();
            let mut all: Vec<i64> = rxs
                .into_iter()
                .flat_map(|r| r.collect_data())
                .flat_map(|(_, d)| d)
                .collect();
            all.sort();
            let _ = done_tx.send(r.map(|_| all).map_err(|e| e.to_string()));
        });
        let got = done_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("triple nested loop hung")
            .unwrap();
        // Single-threaded reference of the same arithmetic.
        let reference = |mut x: i64| loop {
            loop {
                loop {
                    x += 1;
                    if x % 3 == 0 {
                        break;
                    }
                }
                x += 1;
                if x % 7 == 0 {
                    break;
                }
            }
            x += 1;
            if x >= 30 {
                return x;
            }
        };
        let mut expected: Vec<i64> = [0, 1, 2, 5].into_iter().map(reference).collect();
        expected.sort();
        assert_eq!(got, expected);
    }
}

/// Probes and outputs at every nesting level: in the outer body, in the inner
/// body, and after each loop. Every probe must finish, inner outputs must be
/// exposed at the root timestamp, and their contents must match the loops'
/// arithmetic.
#[test]
fn probes_and_outputs_at_every_nesting_level() {
    let outer_probe: Arc<Mutex<Option<ProbeHandle<P1>>>> = Arc::default();
    let inner_probe: Arc<Mutex<Option<ProbeHandle<P2>>>> = Arc::default();
    let after_inner_probe: Arc<Mutex<Option<ProbeHandle<P1>>>> = Arc::default();
    let (op, ip, aip) = (
        Arc::clone(&outer_probe),
        Arc::clone(&inner_probe),
        Arc::clone(&after_inner_probe),
    );
    let builder = DataflowBuilder::<u64>::new("nested-probes");
    let input = builder.input::<i64>("data").unwrap();
    let (out, root_probe) = input
        .iterate::<u32>("outer", 1u32, move |outer| {
            let (outer, p) = outer.probe();
            *op.lock().unwrap() = Some(p);
            let inner_out = outer.iterate::<u32>("inner", 1u32, move |inner| {
                let stepped = inner.map("inc", |_t: &P2, x: i64| x + 1);
                let (stepped, p) = stepped.probe();
                *ip.lock().unwrap() = Some(p);
                stepped
                    .clone()
                    .output("inner-every-step")
                    .expect("inner output");
                let done = stepped.clone().filter("idone", |_t, x| *x % 5 == 0);
                let again = stepped.filter("iagain", |_t, x| *x % 5 != 0);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            let (inner_out, p) = inner_out.probe();
            *aip.lock().unwrap() = Some(p);
            inner_out
                .clone()
                .output("inner-loop-exits")
                .expect("outer-body output");
            let stepped = inner_out.map("plus1", |_t: &P1, x: i64| x + 1);
            let done = stepped.clone().filter("odone", |_t, x| *x >= 20);
            let again = stepped.filter("oagain", |_t, x| *x < 20);
            IterateResult {
                feedback: again,
                output: done,
            }
        })
        .probe();
    out.output("results").unwrap();
    let df = builder.build().unwrap();
    let mut names = df.output_names();
    names.sort();
    assert_eq!(
        names,
        vec!["inner-every-step", "inner-loop-exits", "results"]
    );

    let rt = rt();
    let mut spawned = rt.spawn(df, SpawnOptions::default()).unwrap();
    let tx = spawned.take_input::<i64>("data").unwrap();
    let results = spawned.take_output::<i64>("results").unwrap();
    let steps = spawned.take_output::<i64>("inner-every-step").unwrap();
    let exits = spawned.take_output::<i64>("inner-loop-exits").unwrap();
    let c1 = std::thread::spawn(move || results.collect_data());
    let c2 = std::thread::spawn(move || steps.collect_data());
    let c3 = std::thread::spawn(move || exits.collect_data());
    tx.send(3, vec![1]).unwrap();
    tx.advance_to(4).unwrap();
    tx.send(4, vec![16]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();

    let results = c1.join().unwrap();
    let mut steps: Vec<(u64, i64)> = c2
        .join()
        .unwrap()
        .into_iter()
        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
        .collect();
    steps.sort();
    let mut exits: Vec<(u64, i64)> = c3
        .join()
        .unwrap()
        .into_iter()
        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
        .collect();
    exits.sort();
    let mut results: Vec<(u64, i64)> = results
        .into_iter()
        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
        .collect();
    results.sort();
    assert_eq!(results, vec![(3, 21), (4, 21)]);
    // Epoch 3: 1->2..5, 6->7..10, 11->12..15, 16->17..20. Epoch 4: 16->17..20.
    let mut expected_steps: Vec<(u64, i64)> =
        (2..=20).filter(|x| x % 5 != 1).map(|x| (3, x)).collect();
    expected_steps.extend((17..=20).map(|x| (4, x)));
    expected_steps.sort();
    assert_eq!(steps, expected_steps);
    assert_eq!(exits, vec![(3, 5), (3, 10), (3, 15), (3, 20), (4, 20)]);

    for (name, done) in [
        ("root", root_probe.is_done()),
        (
            "outer body",
            outer_probe.lock().unwrap().as_ref().unwrap().is_done(),
        ),
        (
            "inner body",
            inner_probe.lock().unwrap().as_ref().unwrap().is_done(),
        ),
        (
            "after inner loop",
            after_inner_probe
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_done(),
        ),
    ] {
        assert!(done, "{name} probe not done after completion");
    }
}

/// An output inside a loop body works with async I/O and with several
/// workers (each worker gets its own receiver).
#[test]
fn loop_body_output_async_and_multi_worker() {
    let rt = rt();
    let mut multi = rt
        .spawn_multi(
            "loop-out-async",
            2,
            |b| {
                let input = b.input::<i64>("data")?;
                input
                    .iterate::<u32>("loop", 1u32, |v| {
                        let s = v
                            .map("inc", |_t: &P1, x: i64| x + 1)
                            .exchange_by_hash("x", |x: &i64| *x as u64);
                        s.clone().output("tap").unwrap();
                        let done = s.clone().filter("done", |_t, x| *x >= 4);
                        let again = s.filter("again", |_t, x| *x < 4);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    })
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new().io_mode(IoMode::Async),
        )
        .unwrap();
    let mut taps = multi.take_all_async_outputs::<i64>("tap").unwrap();
    let mut results = multi.take_all_async_outputs::<i64>("results").unwrap();
    let inputs = multi.take_all_async_inputs::<i64>("data").unwrap();
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (tap_data, result_data) = tokio_rt.block_on(async move {
        let mut inputs = inputs.into_iter();
        let first = inputs.next().unwrap();
        drop(inputs);
        first.send(7, vec![0, 1]).await.unwrap();
        drop(first);
        let mut tap_data = Vec::new();
        for t in &mut taps {
            tap_data.extend(t.collect_data().await);
        }
        let mut result_data = Vec::new();
        for r in &mut results {
            result_data.extend(r.collect_data().await);
        }
        (tap_data, result_data)
    });
    with_join(multi);
    let mut taps: Vec<(u64, i64)> = tap_data
        .into_iter()
        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
        .collect();
    taps.sort();
    assert_eq!(
        taps,
        vec![(7, 1), (7, 2), (7, 2), (7, 3), (7, 3), (7, 4), (7, 4)]
    );
    let mut results: Vec<(u64, i64)> = result_data
        .into_iter()
        .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
        .collect();
    results.sort();
    assert_eq!(results, vec![(7, 4), (7, 4)]);
}

fn with_join(multi: instancy::runtime::MultiSpawnedDataflow<u64>) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("hang")
        .unwrap();
}

/// An output name used both inside a loop body and outside it is rejected
/// at build time, in either declaration order.
#[test]
fn duplicate_output_names_across_loop_scopes_are_rejected() {
    let builder = DataflowBuilder::<u64>::new("dup-after");
    let input = builder.input::<i64>("data").unwrap();
    let out = input.iterate::<u32>("loop", 1u32, |v| {
        let s = v.map("inc", |_t: &P1, x: i64| x + 1);
        s.clone().output("same").unwrap();
        IterateResult {
            feedback: s.clone().filter("again", |_t, x| *x < 2),
            output: s.filter("done", |_t, x| *x >= 2),
        }
    });
    assert!(out.output("same").is_err());
    drop(builder);

    let builder = DataflowBuilder::<u64>::new("dup-before");
    let input = builder.input::<i64>("data").unwrap();
    input.clone().output("same").unwrap();
    let out = input.iterate::<u32>("loop", 1u32, |v| {
        let s = v.map("inc", |_t: &P1, x: i64| x + 1);
        s.clone().output("same").unwrap();
        IterateResult {
            feedback: s.clone().filter("again", |_t, x| *x < 2),
            output: s.filter("done", |_t, x| *x >= 2),
        }
    });
    out.output("results").unwrap();
    assert!(builder.build().is_err());
}

/// `collect()` inside a loop body still collects (with loop timestamps) and
/// does not collide with collectors outside the loop.
#[cfg(feature = "test-utils")]
#[test]
fn collect_inside_loop_body_keeps_working() {
    let builder = DataflowBuilder::<u64>::new("collect-in-loop");
    let input = builder.source("src", vec![(0u64, vec![0i64])]);
    let outer_collect = input.clone().collect();
    let inner_collect: Arc<Mutex<Option<Collected<P1>>>> = Arc::default();
    let ic = Arc::clone(&inner_collect);
    let out = input.iterate::<u32>("loop", 1u32, move |v| {
        let s = v.map("inc", |_t: &P1, x: i64| x + 1);
        *ic.lock().unwrap() = Some(s.clone().collect());
        IterateResult {
            feedback: s.clone().filter("again", |_t, x| *x < 3),
            output: s.filter("done", |_t, x| *x >= 3),
        }
    });
    let final_collect = out.collect();
    let df = builder.build().unwrap();
    instancy::SimpleRuntime::new().run(df).unwrap();
    assert_eq!(*outer_collect.lock().unwrap(), vec![(0, vec![0])]);
    assert_eq!(*final_collect.lock().unwrap(), vec![(0, vec![3])]);
    let inner = inner_collect.lock().unwrap().take().unwrap();
    let mut inner: Vec<(P1, Vec<i64>)> = inner.lock().unwrap().clone();
    inner.sort();
    assert_eq!(
        inner,
        vec![
            (P1::new(0, 0), vec![1]),
            (P1::new(0, 1), vec![2]),
            (P1::new(0, 2), vec![3])
        ]
    );
}
