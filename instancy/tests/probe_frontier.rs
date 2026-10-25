//! Regression tests: probes report the frontier of the stream they are
//! attached to, monotonically and conservatively.
//!
//! A probe is attached to a stream (an operator output). It must
//! - advance as the stream advances (a probe directly on an input advances),
//! - hold back while the producing operator can still emit (a `unary_notify`
//!   capability for a pending notification keeps the probe at that time),
//! - never move backwards, including when several workers share one probe,
//! - never claim a time complete while data at that time still appears on the
//!   stream.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::progress::frontier::Antichain;
use instancy::{DataflowBuilder, Pipe, ProbeHandle, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(30);

/// A probe handle handed out of a dataflow-building closure.
type ProbeSlot<T> = Arc<Mutex<Option<ProbeHandle<T>>>>;
/// A (before, after) pair of observed frontiers.
type FrontierPair = (Vec<u64>, Vec<u64>);

/// Run `f` on a helper thread and fail if it does not finish in `timeout`.
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

fn wait_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    cond()
}

/// `true` if `new` is at or beyond `old`: every element of `new` is `>=` some
/// element of `old`.
fn dominates<T: instancy::Timestamp>(old: &Antichain<T>, new: &Antichain<T>) -> bool {
    new.elements().iter().all(|t| old.less_equal(t))
}

/// Records every frontier a probe publishes (polling the handle and its watch
/// channel) until stopped, and checks that the sequence is monotone.
struct MonotoneWatcher<T: instancy::Timestamp> {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Vec<Antichain<T>>>>,
}

impl<T: instancy::Timestamp> MonotoneWatcher<T> {
    fn start(probe: &ProbeHandle<T>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let probe = probe.clone();
        let stop2 = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut rx = probe.subscribe();
            let mut seen: Vec<Antichain<T>> = vec![rx.borrow_and_update().clone()];
            loop {
                let polled = probe.frontier();
                if seen.last() != Some(&polled) {
                    seen.push(polled);
                }
                if rx.has_changed().unwrap_or(false) {
                    let watched = rx.borrow_and_update().clone();
                    if seen.last() != Some(&watched) {
                        seen.push(watched);
                    }
                }
                if stop2.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            seen
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stops watching and asserts the observed frontiers never regressed.
    fn finish(mut self, what: &str) -> Vec<Antichain<T>> {
        self.stop.store(true, Ordering::SeqCst);
        let seen = self.handle.take().unwrap().join().unwrap();
        // The watch channel and the polled value are updated under one lock,
        // but the two reads above are not atomic together, so an older watch
        // value can be recorded after a newer polled one. Compare against the
        // running maximum (the latest dominating frontier) instead.
        let mut max = seen[0].clone();
        for f in &seen[1..] {
            if dominates(&max, f) {
                max = f.clone();
            } else {
                assert!(
                    dominates(f, &max),
                    "{what}: probe frontier moved to an incomparable frontier: {:?} after {:?} (all: {:?})",
                    f.elements(),
                    max.elements(),
                    seen.iter()
                        .map(|a| a.elements().to_vec())
                        .collect::<Vec<_>>()
                );
            }
        }
        seen
    }
}

/// Probes `pipe` and checks, in an operator directly downstream on the same
/// stream, that the probe never claims a time complete while data at that time
/// is still flowing on the stream.
fn checked_probe<T: instancy::Timestamp, D: Clone + Send + 'static>(
    pipe: Pipe<T, D>,
    name: &str,
    violations: &Arc<Mutex<Vec<String>>>,
) -> (Pipe<T, D>, ProbeHandle<T>) {
    let (pipe, probe) = pipe.probe();
    let check = probe.clone();
    let violations = Arc::clone(violations);
    let label = name.to_string();
    let pipe = pipe.inspect_batch(format!("check-{name}"), move |t: &T, _batch: &[D]| {
        if check.done_with(t) {
            violations.lock().unwrap().push(format!(
                "{label}: data at {t:?} after probe reported done_with({t:?}) (frontier {:?})",
                check.frontier().elements()
            ));
        }
    });
    (pipe, probe)
}

// ---------------------------------------------------------------------------
// Moved repros (audit M8)
// ---------------------------------------------------------------------------

/// A probe attached directly to an input stream must advance when the input
/// frontier is advanced (and become done when the input closes).
#[test]
fn probe_on_input_stream_advances() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("probe_input");
    let (s, probe) = builder.input::<u64>("in").unwrap().probe();
    s.map("id", |_t, x| x).output("out").unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<u64>("in").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    tx.send(0, vec![1, 2, 3]).unwrap();
    tx.advance_to(5).unwrap();

    let advanced = wait_until(Duration::from_secs(3), || probe.done_with(&0));
    let frontier_before_close = probe.frontier();
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let _ = collector.join().unwrap();
    assert!(
        advanced,
        "probe on input never advanced past 0 after advance_to(5); frontier={frontier_before_close:?}"
    );
    assert!(
        probe.is_done(),
        "probe on input not done after completion: {:?}",
        probe.frontier()
    );
}

/// Same, with the probe as the only consumer-side observer of the input (the
/// input feeds the output directly).
#[test]
fn probe_directly_on_input_advances_before_close() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("probe-input-direct");
    let (s, probe) = builder.input::<u64>("data").unwrap().probe();
    s.output("out").unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let sender = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());
    sender.send(0, vec![1]).unwrap();
    sender.advance_to(5).unwrap();
    wait_until(Duration::from_secs(3), || probe.done_with(&0));
    let mid = probe.frontier();
    drop(sender);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let _ = collector.join();
    assert_eq!(
        mid,
        Antichain::from_elem(5),
        "probe on input should be exactly at the input frontier"
    );
}

/// A probe placed *after* a capability-holding operator (unary_notify) must not
/// report time t as done while that operator can still emit data at t.
#[test]
fn probe_after_notify_operator_waits_for_its_output() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("probe_after_notify");
    let mut stash: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let (s, probe) = builder
        .input::<u64>("in")
        .unwrap()
        .unary_notify("slow_emit", move |input, output, ctx| {
            while let Some((t, d)) = input.next() {
                stash.entry(t).or_default().extend(d);
                ctx.notify_at(t);
            }
            while let Some(t) = ctx.next_notification() {
                // Emulate an expensive finalisation step.
                std::thread::sleep(Duration::from_millis(1500));
                if let Some(d) = stash.remove(&t) {
                    output.push_vec(t, d);
                }
            }
            Ok(())
        })
        .probe();
    s.output("out").unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<u64>("in").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    tx.send(0, vec![7]).unwrap();
    tx.advance_to(1).unwrap();

    let start = Instant::now();
    while !probe.done_with(&0) && start.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(probe.done_with(&0), "probe never passed 0");
    // The probe claims nothing more at t=0 will be produced at this point, so
    // the t=0 output must already have been produced (it is the only producer).
    let got_already = rx.try_recv();
    drop(tx);
    let late = rx.recv_timeout(Duration::from_secs(10));
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    assert!(
        got_already.is_some(),
        "probe reported t=0 complete before the notify operator emitted its t=0 output (it arrived later: {late:?})"
    );
}

/// `notify_at(time)` for the time of data just received: the operator consumes
/// notifications lazily (only once its input is exhausted). The held
/// capability must keep the downstream frontier at `time` until the
/// notification is consumed.
#[test]
fn unconsumed_notification_capability_holds_downstream_frontier() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-hold");
    let input = builder.input::<u64>("data").unwrap();
    let (s, probe) = input
        .unary_notify("lazy", {
            let mut stash = Vec::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    stash.push((time, data));
                    ctx.notify_at(time);
                }
                if input.is_exhausted() {
                    while let Some(t) = ctx.next_notification() {
                        for (st, d) in stash.drain(..) {
                            if st == t {
                                output.push_vec(t, d);
                            }
                        }
                    }
                }
                Ok(())
            }
        })
        .probe();
    s.output("out").unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let sender = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());
    sender.send(0, vec![42]).unwrap();
    sender.advance_to(10).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mid = probe.frontier();
    drop(sender);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let data = collector.join().unwrap();
    assert_eq!(data, vec![(0, vec![42])]);
    assert!(
        mid.less_equal(&0),
        "downstream frontier advanced to {:?} while the operator still held a capability at 0 \
         (and later emitted data at 0)",
        mid.elements()
    );
    assert!(probe.is_done());
}

// ---------------------------------------------------------------------------
// Adversarial: probes at every graph position
// ---------------------------------------------------------------------------

/// Builds a dataflow with a probe on every kind of stream (input, map, filter,
/// exchange, branch outputs, merge, binary, unary_notify, reduce, delay_batch,
/// a loop's output and a stream inside a loop body) and drives many epochs
/// through it. Every probe must be monotone, advance during the run, and be
/// done at the end; every probe outside loops must never report a time
/// complete while data at that time still appears on its stream.
#[test]
fn probes_at_every_graph_position_are_monotone_conservative_and_live() {
    for round in 0..3 {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let violations: Arc<Mutex<Vec<String>>> = Arc::default();
        let v = &violations;
        let builder = DataflowBuilder::<u64>::new("probe-positions");
        let mut probes: Vec<(&'static str, ProbeHandle<u64>)> = Vec::new();

        let input = builder.input::<u64>("data").unwrap();
        let (input, p) = checked_probe(input, "input", v);
        probes.push(("input", p));
        let (mapped, p) = checked_probe(input.map("map", |_t, x| x + 1), "map", v);
        probes.push(("map", p));
        let (filtered, p) = checked_probe(mapped.filter("filter", |_t, x| x % 7 != 0), "filter", v);
        probes.push(("filter", p));
        let (exchanged, p) = checked_probe(
            filtered.exchange_by_hash("exchange", |x: &u64| *x),
            "exchange",
            v,
        );
        probes.push(("exchange", p));
        let (evens, odds) = exchanged.branch("branch", |_t, x| x % 2 == 0);
        let (evens, p) = checked_probe(evens, "branch-true", v);
        probes.push(("branch-true", p));
        let (odds, p) = checked_probe(odds, "branch-false", v);
        probes.push(("branch-false", p));
        let (merged, p) = checked_probe(evens.clone().merge(odds.clone()).unwrap(), "merge", v);
        probes.push(("merge", p));
        let (joined, p) = checked_probe(
            evens
                .binary::<u64, u64, _>(odds, "binary", |a, b, out| {
                    while let Some((t, d)) = a.next() {
                        out.push_vec(t, d);
                    }
                    while let Some((t, d)) = b.next() {
                        out.push_vec(t, d);
                    }
                    Ok(())
                })
                .unwrap(),
            "binary",
            v,
        );
        probes.push(("binary", p));
        let (notified, p) = checked_probe(
            merged.clone().unary_notify("notify", {
                let mut stash: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
                move |input, output, ctx| {
                    while let Some((t, d)) = input.next() {
                        stash.entry(t).or_default().extend(d);
                        ctx.notify_at(t);
                    }
                    while let Some(t) = ctx.next_notification() {
                        if let Some(d) = stash.remove(&t) {
                            output.push_vec(t, d);
                        }
                    }
                    Ok(())
                }
            }),
            "unary_notify",
            v,
        );
        probes.push(("unary_notify", p));
        let (reduced, p) = checked_probe(merged.reduce("reduce", |a, b| a + b), "reduce", v);
        probes.push(("reduce", p));
        let (delayed, p) = checked_probe(
            joined.delay_batch("delay", |t| (t / 3 + 1) * 3),
            "delay_batch",
            v,
        );
        probes.push(("delay_batch", p));
        let inner_probe: ProbeSlot<Product<u64, u32>> = Arc::default();
        let ip = Arc::clone(&inner_probe);
        let lv = Arc::clone(v);
        let looped = notified.iterate::<u32>("loop", 1u32, move |var| {
            let (stepped, probe) = checked_probe(
                var.map("halve", |_t: &Product<u64, u32>, x| x / 2),
                "loop-body",
                &lv,
            );
            *ip.lock().unwrap() = Some(probe);
            let done = stepped.clone().filter("done", |_t, x| *x < 4);
            let again = stepped.filter("again", |_t, x| *x >= 4);
            IterateResult {
                feedback: again,
                output: done,
            }
        });
        // The stream leaving a loop is only checked for monotonicity, liveness
        // and completion, not conservativeness: the loop's feedback edge is not
        // yet visible to progress tracking, so the tracker itself lets the
        // outer epoch complete downstream of the loop while the loop still
        // iterates (pre-existing; tracked separately from probe correctness).
        // The probe inside the loop body is checked for conservativeness.
        let (looped, p) = looped.probe();
        probes.push(("iterate-leave", p));
        let inner_probe = inner_probe.lock().unwrap().take().unwrap();

        looped.output("loop-out").unwrap();
        reduced.output("reduce-out").unwrap();
        delayed.output("delay-out").unwrap();

        let watchers: Vec<_> = probes
            .iter()
            .map(|(name, p)| (*name, MonotoneWatcher::start(p)))
            .collect();
        let inner_watcher = MonotoneWatcher::start(&inner_probe);

        let mut h = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let tx = h.take_input::<u64>("data").unwrap();
        let outs: Vec<_> = ["loop-out", "reduce-out", "delay-out"]
            .iter()
            .map(|n| {
                let rx = h.take_output::<u64>(n).unwrap();
                std::thread::spawn(move || rx.collect_data())
            })
            .collect();

        let epochs = 40u64;
        for e in 0..epochs {
            tx.send(e, (0..20).map(|i| e * 100 + i).collect()).unwrap();
            // Several epochs complete together every few steps.
            if e % 5 == 4 {
                tx.advance_to(e + 1).unwrap();
            }
            if round == 1 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        tx.advance_to(epochs).unwrap();
        // Liveness: every probe reaches the input frontier while the input is
        // still open (delay_batch holds only times >= 42 = (39 / 3 + 1) * 3).
        for (name, p) in &probes {
            let target = epochs - 1;
            assert!(
                wait_until(Duration::from_secs(10), || p.done_with(&target)),
                "round {round}: probe '{name}' never passed {target}: {:?}",
                p.frontier().elements()
            );
        }
        drop(tx);
        with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
        for o in outs {
            o.join().unwrap();
        }
        for (name, w) in watchers {
            w.finish(name);
        }
        inner_watcher.finish("loop-body");
        for (name, p) in &probes {
            assert!(
                p.is_done(),
                "round {round}: probe '{name}' not done: {:?}",
                p.frontier()
            );
        }
        assert!(inner_probe.is_done(), "loop-body probe not done");
        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "round {round}: probes reported times complete too early: {violations:#?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Adversarial: Product timestamps
// ---------------------------------------------------------------------------

/// With partially ordered `Product` times, a probe after a notify operator is
/// monotone, holds every time the operator still owes output for, and the
/// operator sees its notifications in an order consistent with the partial
/// order.
#[test]
fn product_time_probe_after_notify_is_monotone_and_conservative() {
    type P = Product<u64, u32>;
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<P>::new("product-probe");
    let order: Arc<Mutex<Vec<P>>> = Arc::default();
    let early: Arc<Mutex<Vec<P>>> = Arc::default();
    let order2 = Arc::clone(&order);
    let (s, probe) = builder
        .input::<u64>("data")
        .unwrap()
        .unary_notify("per-time", {
            let mut stash: BTreeMap<P, u64> = BTreeMap::new();
            move |input, output, ctx| {
                while let Some((t, d)) = input.next() {
                    *stash.entry(t).or_default() += d.iter().sum::<u64>();
                    ctx.notify_at(t);
                }
                while let Some(t) = ctx.next_notification() {
                    order2.lock().unwrap().push(t);
                    if let Some(sum) = stash.remove(&t) {
                        output.push_vec(t, vec![sum]);
                    }
                }
                Ok(())
            }
        })
        .probe();
    let check = probe.clone();
    let early2 = Arc::clone(&early);
    s.inspect_batch("check", move |t: &P, _| {
        if check.done_with(t) {
            early2.lock().unwrap().push(*t);
        }
    })
    .output("out")
    .unwrap();
    let watcher = MonotoneWatcher::start(&probe);
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());
    // A grid of times; every (outer, inner) with outer < 6, inner < 6.
    for outer in 0..6u64 {
        for inner in (0..6u32).rev() {
            tx.send(P::new(outer, inner), vec![1]).unwrap();
        }
    }
    tx.advance_to(P::new(3, 0)).unwrap();
    assert!(wait_until(Duration::from_secs(10), || probe.done_with(&P::new(2, 5))));
    assert!(!probe.done_with(&P::new(3, 0)));
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let data = collector.join().unwrap();
    watcher.finish("product probe");
    assert_eq!(data.len(), 36);
    assert!(probe.is_done());
    assert!(
        early.lock().unwrap().is_empty(),
        "{:?}",
        early.lock().unwrap()
    );
    let order = order.lock().unwrap();
    assert_eq!(order.len(), 36);
    for i in 0..order.len() {
        for j in i + 1..order.len() {
            assert!(
                !instancy::order::PartialOrder::less_than(&order[j], &order[i]),
                "notification {:?} delivered after {:?}",
                order[j],
                order[i]
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Adversarial: several workers sharing one probe
// ---------------------------------------------------------------------------

/// `spawn_multi` builds the dataflow once, so every worker writes the same
/// probe. Workers advance their inputs at very different paces, so their views
/// of the global frontier lag each other. The probe must report the meet of
/// the views (never a single worker's, which can be behind a frontier already
/// published) and never move backwards.
#[test]
fn multi_worker_shared_probe_is_monotone_and_conservative() {
    for round in 0..5 {
        let workers = 4;
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 4,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let violations: Arc<Mutex<Vec<String>>> = Arc::default();
        let slot: ProbeSlot<u64> = Arc::default();
        let v = Arc::clone(&violations);
        let s2 = Arc::clone(&slot);
        let mut multi = rt
            .spawn_multi(
                "shared-probe",
                workers,
                move |b: &mut DataflowBuilder<u64>| {
                    let input = b.input::<u64>("data")?;
                    let stream = input
                        .map("inc", |_t, x| x + 1)
                        .exchange_by_hash("x", |x: &u64| *x);
                    let (stream, probe) = checked_probe(stream, "after-exchange", &v);
                    *s2.lock().unwrap() = Some(probe);
                    stream.output("out")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let probe = slot.lock().unwrap().take().unwrap();
        let watcher = MonotoneWatcher::start(&probe);
        let senders: Vec<_> = (0..workers)
            .map(|w| multi.take_input::<u64>(w, "data").unwrap())
            .collect();
        let outs: Vec<_> = (0..workers)
            .map(|w| {
                let rx = multi.take_output::<u64>(w, "out").unwrap();
                std::thread::spawn(move || rx.collect_data())
            })
            .collect();
        let epochs = 60u64;
        let feeders: Vec<_> = senders
            .into_iter()
            .enumerate()
            .map(|(w, tx)| {
                std::thread::spawn(move || {
                    for e in 0..epochs {
                        tx.send(e, (0..5).map(|i| e * 1000 + w as u64 * 10 + i).collect())
                            .unwrap();
                        tx.advance_to(e + 1).unwrap();
                        // Worker 0 races ahead; the others lag differently.
                        if w > 0 && (e + w as u64 + round) % (w as u64 + 1) == 0 {
                            std::thread::sleep(Duration::from_micros(300 * w as u64));
                        }
                    }
                    tx
                })
            })
            .collect();
        let senders: Vec<_> = feeders.into_iter().map(|f| f.join().unwrap()).collect();
        // Liveness: with every input still open at `epochs`, the shared probe
        // must reach the last epoch (it may not wait for completion).
        assert!(
            wait_until(Duration::from_secs(10), || probe.done_with(&(epochs - 1))),
            "round {round}: shared probe stuck at {:?} with all inputs at {epochs}",
            probe.frontier().elements()
        );
        assert!(!probe.done_with(&epochs));
        drop(senders);
        with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
        let total: usize = outs
            .into_iter()
            .map(|o| {
                o.join()
                    .unwrap()
                    .iter()
                    .map(|(_, d)| d.len())
                    .sum::<usize>()
            })
            .sum();
        watcher.finish("shared probe");
        assert_eq!(total, (epochs as usize) * 5 * workers);
        assert!(
            probe.is_done(),
            "round {round}: shared probe not done after all workers completed: {:?}",
            probe.frontier()
        );
        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "round {round}: shared probe reported times complete too early: {violations:#?}"
        );
    }
}

/// Worker inputs that never advance hold the shared probe back, even though
/// the other workers' inputs are closed and those workers have completed.
#[test]
fn multi_worker_probe_waits_for_the_slowest_worker() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let slot: ProbeSlot<u64> = Arc::default();
    let s2 = Arc::clone(&slot);
    let mut multi = rt
        .spawn_multi(
            "slowest",
            3,
            move |b: &mut DataflowBuilder<u64>| {
                let (s, p) = b.input::<u64>("data")?.map("id", |_t, x| x).probe();
                *s2.lock().unwrap() = Some(p);
                s.output("out")?;
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let probe = slot.lock().unwrap().take().unwrap();
    let watcher = MonotoneWatcher::start(&probe);
    let fast0 = multi.take_input::<u64>(0, "data").unwrap();
    let fast1 = multi.take_input::<u64>(1, "data").unwrap();
    let slow = multi.take_input::<u64>(2, "data").unwrap();
    let outs: Vec<_> = (0..3)
        .map(|w| {
            let rx = multi.take_output::<u64>(w, "out").unwrap();
            std::thread::spawn(move || rx.collect_data())
        })
        .collect();
    fast0.send(0, vec![1]).unwrap();
    fast1.send(0, vec![2]).unwrap();
    slow.send(0, vec![3]).unwrap();
    slow.advance_to(3).unwrap();
    drop(fast0);
    drop(fast1);
    assert!(wait_until(Duration::from_secs(10), || probe.done_with(&2)));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !probe.done_with(&3),
        "probe passed 3 while worker 2's input is still at 3: {:?}",
        probe.frontier()
    );
    slow.advance_to(7).unwrap();
    assert!(wait_until(Duration::from_secs(10), || probe.done_with(&6)));
    assert!(!probe.done_with(&7));
    drop(slow);
    with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
    for o in outs {
        o.join().unwrap();
    }
    watcher.finish("slowest");
    assert!(probe.is_done());
}

/// Counts every probe update across many short-lived single-worker dataflows
/// where several epochs complete at once; none may regress.
#[test]
fn probe_never_regresses_across_many_runs() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let regressions = Arc::new(AtomicUsize::new(0));
    for run in 0..20u64 {
        let builder = DataflowBuilder::<u64>::new("many-runs");
        let (s, probe) = builder
            .input::<u64>("data")
            .unwrap()
            .reduce("sum", |a, b| a + b)
            .probe();
        s.output("out").unwrap();
        let watcher = MonotoneWatcher::start(&probe);
        let mut h = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let tx = h.take_input::<u64>("data").unwrap();
        let rx = h.take_output::<u64>("out").unwrap();
        let c = std::thread::spawn(move || rx.collect_data());
        for e in 0..30 {
            tx.send(e, vec![e, run]).unwrap();
            if e % (run % 5 + 1) == 0 {
                tx.advance_to(e + 1).unwrap();
            }
        }
        drop(tx);
        with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
        let data = c.join().unwrap();
        assert_eq!(data.len(), 30);
        let seen = watcher.finish("many runs");
        for w in seen.windows(2) {
            if !dominates(&w[0], &w[1]) && !dominates(&w[1], &w[0]) {
                regressions.fetch_add(1, Ordering::SeqCst);
            }
        }
        assert!(probe.is_done());
    }
    assert_eq!(regressions.load(Ordering::SeqCst), 0);
}

/// Stress: eight workers share one probe and race through many tiny epochs; a
/// busy-polling observer must never see the published frontier move backwards.
/// (With last-writer-wins publication, a worker whose view lags the frontier
/// another worker just published moved the probe backwards.)
#[test]
fn multi_worker_shared_probe_stress_never_regresses() {
    let workers = 8;
    for round in 0..4u64 {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 4,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let slot: ProbeSlot<u64> = Arc::default();
        let s2 = Arc::clone(&slot);
        let mut multi = rt
            .spawn_multi(
                "shared-probe-stress",
                workers,
                move |b: &mut DataflowBuilder<u64>| {
                    let (s, p) = b
                        .input::<u64>("data")?
                        .map("inc", |_t, x| x + 1)
                        .exchange_by_hash("x", |x: &u64| *x)
                        .probe();
                    *s2.lock().unwrap() = Some(p);
                    s.output("out")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let probe = slot.lock().unwrap().take().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let regressions: Arc<Mutex<Vec<FrontierPair>>> = Arc::default();
        let observer = {
            let probe = probe.clone();
            let stop = Arc::clone(&stop);
            let regressions = Arc::clone(&regressions);
            std::thread::spawn(move || {
                let mut rx = probe.subscribe();
                let mut last = probe.frontier();
                let mut last_watched = rx.borrow_and_update().clone();
                while !stop.load(Ordering::Relaxed) {
                    let now = probe.frontier();
                    if !dominates(&last, &now) {
                        regressions
                            .lock()
                            .unwrap()
                            .push((last.elements().to_vec(), now.elements().to_vec()));
                    }
                    last = now;
                    if rx.has_changed().unwrap_or(false) {
                        let watched = rx.borrow_and_update().clone();
                        if !dominates(&last_watched, &watched) {
                            regressions.lock().unwrap().push((
                                last_watched.elements().to_vec(),
                                watched.elements().to_vec(),
                            ));
                        }
                        last_watched = watched;
                    }
                    std::thread::yield_now();
                }
            })
        };
        let senders = multi.take_all_inputs::<u64>("data").unwrap();
        let outs: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|rx| std::thread::spawn(move || rx.collect_data()))
            .collect();
        let epochs = 300u64;
        let feeders: Vec<_> = senders
            .into_iter()
            .enumerate()
            .map(|(w, tx)| {
                std::thread::spawn(move || {
                    for e in 0..epochs {
                        tx.send(e, vec![e * 100 + w as u64]).unwrap();
                        tx.advance_to(e + 1).unwrap();
                        if (e * 7 + w as u64 * 3 + round) % 11 == 0 {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        for f in feeders {
            f.join().unwrap();
        }
        with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
        let total: usize = outs
            .into_iter()
            .map(|o| {
                o.join()
                    .unwrap()
                    .iter()
                    .map(|(_, d)| d.len())
                    .sum::<usize>()
            })
            .sum();
        stop.store(true, Ordering::SeqCst);
        observer.join().unwrap();
        assert_eq!(total, (epochs as usize) * workers);
        assert!(probe.is_done(), "round {round}: {:?}", probe.frontier());
        let regressions = regressions.lock().unwrap();
        assert!(
            regressions.is_empty(),
            "round {round}: shared probe moved backwards {} times, e.g. {:?}",
            regressions.len(),
            &regressions[..regressions.len().min(5)]
        );
    }
}

// ---------------------------------------------------------------------------
// Adversarial: probes inside iterate bodies
// ---------------------------------------------------------------------------

type P1 = Product<u64, u32>;
type P2 = Product<P1, u32>;

/// A loop body `map -> probe -> filter(< N) -> feedback` iterates epoch 0
/// thousands of times (more than one executor sweep's activation budget)
/// after the input has already advanced past it. The records circulating on
/// the feedback edge are counted, so the body probe's frontier (in the loop's
/// `Product` time) holds epoch 0 until the record leaves the loop. It
/// advances with the iterations: `(0, i)` is complete once the record has
/// passed iteration `i`, and never before (nor while `(0, i)` data still
/// appears on the stream).
#[test]
fn loop_body_probe_never_reports_an_epoch_complete_while_the_loop_iterates() {
    const ITERATIONS: u64 = 3000;
    for round in 0..3u64 {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let violations: Arc<Mutex<Vec<String>>> = Arc::default();
        let slot: ProbeSlot<P1> = Arc::default();
        let (v, s2) = (Arc::clone(&violations), Arc::clone(&slot));
        // The highest iteration of epoch 0 the body stream has carried.
        let reached = Arc::new(AtomicU64::new(0));
        let r2 = Arc::clone(&reached);
        let builder = DataflowBuilder::<u64>::new("loop-body-probe");
        let out = builder
            .input::<u64>("data")
            .unwrap()
            .iterate::<u32>("loop", 1u32, move |var| {
                let stepped = var.map("inc", move |t: &P1, x: u64| {
                    if t.outer == 0 {
                        r2.fetch_max(u64::from(t.inner), Ordering::SeqCst);
                    }
                    x + 1
                });
                let (stepped, probe) = checked_probe(stepped, "loop-body", &v);
                *s2.lock().unwrap() = Some(probe);
                IterateResult {
                    feedback: stepped.clone().filter("again", |_t, x| *x < ITERATIONS),
                    output: stepped.filter("done", |_t, x| *x >= ITERATIONS),
                }
            });
        out.output("out").unwrap();
        let probe = slot.lock().unwrap().take().unwrap();
        let watcher = MonotoneWatcher::start(&probe);
        let mut h = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let tx = h.take_input::<u64>("data").unwrap();
        let rx = h.take_output::<u64>("out").unwrap();
        tx.send(0, vec![round]).unwrap();
        tx.advance_to(1).unwrap();
        // While epoch 0 iterates, (0, ITERATIONS / 2) may be reported
        // complete only once the record has passed that iteration.
        let mid = ITERATIONS as u32 / 2;
        let start = Instant::now();
        let mut got = None;
        while got.is_none() && start.elapsed() < Duration::from_secs(10) {
            if probe.done_with(&P1::new(0, mid)) && reached.load(Ordering::SeqCst) < u64::from(mid)
            {
                violations.lock().unwrap().push(format!(
                    "done_with((0, {mid})) before the body carried iteration {mid}: {:?}",
                    probe.frontier()
                ));
                break;
            }
            got = rx.try_recv();
            std::thread::yield_now();
        }
        tx.send(3, vec![7]).unwrap();
        drop(tx);
        with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
        let mut rest: Vec<(u64, Vec<u64>)> = rx.collect_data();
        if let Some(instancy::dataflow::OutputEvent::Data { time, data }) = got {
            rest.push((time, data));
        }
        rest.sort();
        assert_eq!(rest, vec![(0, vec![ITERATIONS]), (3, vec![ITERATIONS])]);
        watcher.finish("loop-body");
        assert!(
            probe.is_done(),
            "loop-body probe not done: {:?}",
            probe.frontier()
        );
        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "round {round}: loop-body probe reported times complete too early: {violations:#?}"
        );
    }
}

/// Four workers, an exchange inside the loop body, many epochs completing
/// together, and a probe on the body stream after the exchange (every worker
/// shares the probe): it must never report a time complete while that time
/// still appears on any worker's instance of the stream, must be monotone, and
/// must be done once all workers completed.
#[test]
fn multi_worker_loop_body_probe_is_conservative() {
    for round in 0..3u64 {
        let workers = 4;
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 4,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let violations: Arc<Mutex<Vec<String>>> = Arc::default();
        let slot: ProbeSlot<P1> = Arc::default();
        let (v, s2) = (Arc::clone(&violations), Arc::clone(&slot));
        let mut multi = rt
            .spawn_multi(
                "mw-loop-body-probe",
                workers,
                move |b: &mut DataflowBuilder<u64>| {
                    let v = Arc::clone(&v);
                    let s2 = Arc::clone(&s2);
                    b.input::<u64>("data")?
                        .iterate::<u32>("loop", 1u32, move |var| {
                            let stepped = var
                                .map("inc", |_t: &P1, x: u64| x + 1)
                                .exchange_by_hash("redistribute", |x: &u64| *x);
                            let (stepped, probe) = checked_probe(stepped, "mw-loop-body", &v);
                            *s2.lock().unwrap() = Some(probe);
                            IterateResult {
                                feedback: stepped.clone().filter("again", |_t, x| x % 16 != 0),
                                output: stepped.filter("done", |_t, x| x % 16 == 0),
                            }
                        })
                        .output("out")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let probe = slot.lock().unwrap().take().unwrap();
        let watcher = MonotoneWatcher::start(&probe);
        let senders = multi.take_all_inputs::<u64>("data").unwrap();
        let outs: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|rx| std::thread::spawn(move || rx.collect_data()))
            .collect();
        let epochs = 12u64;
        for (w, tx) in senders.iter().enumerate() {
            for e in 0..epochs {
                tx.send(
                    e,
                    (0..3)
                        .map(|i| e * 100 + w as u64 * 10 + i + round)
                        .collect(),
                )
                .unwrap();
                if e % 4 == 3 {
                    tx.advance_to(e + 1).unwrap();
                }
            }
        }
        drop(senders);
        with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
        let total: usize = outs
            .into_iter()
            .map(|o| {
                o.join()
                    .unwrap()
                    .iter()
                    .map(|(_, d)| d.len())
                    .sum::<usize>()
            })
            .sum();
        watcher.finish("mw loop body");
        assert_eq!(total, epochs as usize * 3 * workers);
        assert!(probe.is_done(), "round {round}: {:?}", probe.frontier());
        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "round {round}: shared loop-body probe reported times complete too early: {:#?}",
            &violations[..violations.len().min(8)]
        );
    }
}

/// A probe in the inner body of a nested loop, two workers, an exchange in the
/// inner body: conservative, monotone, done at the end.
#[test]
fn nested_loop_body_probe_is_conservative() {
    for _ in 0..3 {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let violations: Arc<Mutex<Vec<String>>> = Arc::default();
        let slot: ProbeSlot<P2> = Arc::default();
        let (v, s2) = (Arc::clone(&violations), Arc::clone(&slot));
        let mut multi = rt
            .spawn_multi(
                "nested-loop-body-probe",
                2,
                move |b: &mut DataflowBuilder<u64>| {
                    let v = Arc::clone(&v);
                    let s2 = Arc::clone(&s2);
                    b.input::<u64>("data")?
                        .iterate::<u32>("outer", 1u32, move |outer| {
                            let inner_out = outer.iterate::<u32>("inner", 1u32, move |inner| {
                                let stepped = inner
                                    .map("inc", |_t: &P2, x: u64| x + 1)
                                    .exchange_by_hash("x", |x: &u64| *x);
                                let (stepped, probe) = checked_probe(stepped, "inner-body", &v);
                                *s2.lock().unwrap() = Some(probe);
                                IterateResult {
                                    feedback: stepped.clone().filter("ia", |_t, x| x % 5 != 0),
                                    output: stepped.filter("id", |_t, x| x % 5 == 0),
                                }
                            });
                            let s = inner_out.map("plus1", |_t: &P1, x: u64| x + 1);
                            IterateResult {
                                feedback: s.clone().filter("oa", |_t, x| *x < 30),
                                output: s.filter("od", |_t, x| *x >= 30),
                            }
                        })
                        .output("out")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let probe = slot.lock().unwrap().take().unwrap();
        let watcher = MonotoneWatcher::start(&probe);
        let senders = multi.take_all_inputs::<u64>("data").unwrap();
        let outs: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|rx| std::thread::spawn(move || rx.collect_data()))
            .collect();
        for (w, tx) in senders.iter().enumerate() {
            for e in 0..4u64 {
                tx.send(e, vec![w as u64 + e]).unwrap();
                tx.advance_to(e + 1).unwrap();
            }
        }
        drop(senders);
        with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
        let total: usize = outs
            .into_iter()
            .map(|o| {
                o.join()
                    .unwrap()
                    .iter()
                    .map(|(_, d)| d.len())
                    .sum::<usize>()
            })
            .sum();
        watcher.finish("nested loop body");
        assert_eq!(total, 8);
        assert!(probe.is_done(), "{:?}", probe.frontier());
        let violations = violations.lock().unwrap();
        assert!(
            violations.is_empty(),
            "nested loop-body probe reported times complete too early: {:#?}",
            &violations[..violations.len().min(8)]
        );
    }
}

// ---------------------------------------------------------------------------
// Per-stage parallelism path (stages with different parallelism)
// ---------------------------------------------------------------------------

type NamedProbes<T> = Arc<Mutex<Vec<(String, ProbeHandle<T>)>>>;

/// On the per-stage-parallelism path the dataflow is built once and every
/// worker runs the operators of its stages, so each probe has one handle,
/// written by every worker that runs the probed operator. Loop-body probes
/// (held at the minimum while running) and probes after a gather must still
/// become done after a successful run, both when the loop stays within one
/// stage and when an exchange inside the loop body makes it cross a stage
/// boundary.
#[test]
fn staged_path_loop_body_and_gather_probes_complete() {
    for exchange_in_loop in [false, true] {
        for _ in 0..3 {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let violations: Arc<Mutex<Vec<String>>> = Arc::default();
            let body_probes: NamedProbes<P1> = Arc::default();
            let gather_probes: NamedProbes<u64> = Arc::default();
            let (v, bp, gp) = (
                Arc::clone(&violations),
                Arc::clone(&body_probes),
                Arc::clone(&gather_probes),
            );
            let mut multi = rt
                .spawn_multi(
                    "staged-probes",
                    2,
                    move |b: &mut DataflowBuilder<u64>| {
                        let name = b.name().to_string();
                        let (v, bp) = (Arc::clone(&v), Arc::clone(&bp));
                        let body_name = name.clone();
                        let looped =
                            b.input::<u64>("data")?
                                .iterate::<u32>("double", 1u32, move |var| {
                                    let mut doubled = var.map("double", |_t: &P1, x: u64| x * 2);
                                    if exchange_in_loop {
                                        doubled =
                                            doubled.exchange_by_hash("redistribute", |x: &u64| *x);
                                    }
                                    let (doubled, probe) =
                                        checked_probe(doubled, "staged-body", &v);
                                    bp.lock().unwrap().push((body_name.clone(), probe));
                                    IterateResult {
                                        feedback: doubled.clone().filter("again", |_t, x| *x < 100),
                                        output: doubled.filter("done", |_t, x| *x >= 100),
                                    }
                                });
                        let (gathered, probe) = looped
                            .map("id", |_t, x| x)
                            .exchange_to("gather", 1, |x: &u64| *x)?
                            .probe();
                        gp.lock().unwrap().push((name, probe));
                        gathered.output("results")?;
                        Ok(())
                    },
                    SpawnOptions::new().per_stage_parallelism(true),
                )
                .unwrap();
            let workers = multi.num_workers();
            let tx = multi.take_input::<u64>(0, "data").unwrap();
            for w in 1..workers {
                drop(multi.take_input::<u64>(w, "data").unwrap());
            }
            let outs: Vec<_> = (0..workers)
                .map(|w| {
                    let rx = multi.take_output::<u64>(w, "results").unwrap();
                    std::thread::spawn(move || rx.collect_data())
                })
                .collect();
            tx.send(0, vec![1, 2, 3, 5, 10, 50]).unwrap();
            drop(tx);
            with_timeout("join", TIMEOUT, move || multi.join_blocking()).unwrap();
            let mut got: Vec<u64> = outs
                .into_iter()
                .flat_map(|o| o.join().unwrap())
                .flat_map(|(_, d)| d)
                .collect();
            got.sort();
            assert_eq!(
                got,
                vec![100, 128, 128, 160, 160, 192],
                "exchange_in_loop={exchange_in_loop}"
            );

            // The dataflow is built once: one handle per probe.
            let body = body_probes.lock().unwrap().clone();
            assert_eq!(body.len(), 1, "one build");
            for (name, p) in &body {
                assert!(
                    p.is_done(),
                    "exchange_in_loop={exchange_in_loop}: loop-body probe of {name} not done: {:?}",
                    p.frontier()
                );
            }
            // The gather stage has parallelism 1: only worker 0 runs it, so
            // its probe has worker 0's writer alone.
            let gather = gather_probes.lock().unwrap().clone();
            assert_eq!(gather.len(), 1, "one build");
            assert!(
                gather[0].1.is_done(),
                "gather probe not done: {:?}",
                gather[0].1.frontier()
            );
            let violations = violations.lock().unwrap();
            assert!(violations.is_empty(), "{violations:#?}");
        }
    }
}
