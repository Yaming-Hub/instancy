//! Audit tests (operator semantics): notifications / probes / pipeline gaps.
//!
//! Each test encodes the *expected* (correct) behaviour; a failing test is a
//! reproduced bug.

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(20);

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

fn group<T: Ord + Clone, D: Clone>(v: &[(T, Vec<D>)]) -> BTreeMap<T, Vec<Vec<D>>> {
    let mut m: BTreeMap<T, Vec<Vec<D>>> = BTreeMap::new();
    for (t, d) in v {
        m.entry(t.clone()).or_default().push(d.clone());
    }
    m
}

/// A per-time `count` must emit exactly ONE value per timestamp, equal to the
/// number of records at that time. Here the upstream `map` is backpressured
/// (capacity-1 edge) so that the executor's per-sweep activation budget runs
/// out while time-0 records are still buffered in `map`'s pending output, after
/// the source has already released its capability.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn count_emits_single_result_per_time_under_backpressure() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("gap_count");
    let data: Vec<(u64, Vec<u64>)> = (0..1000).map(|_| (0u64, vec![1u64])).collect();
    builder
        .source("src", data)
        .map("id", |_t, x| x)
        .with_capacity(1)
        .unwrap()
        .count("cnt")
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let rx = h.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let out = collector.join().unwrap();
    let g = group(&out);
    let t0: Vec<usize> = g.get(&0).cloned().unwrap_or_default().concat();
    assert_eq!(
        t0,
        vec![1000],
        "count at t=0 must be a single value 1000; got {} values (first few: {:?})",
        t0.len(),
        &t0[..t0.len().min(8)]
    );
}

/// Same as above but with an external input + explicit advance_to, the common
/// streaming shape (input -> map -> count).
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn count_emits_single_result_per_epoch_with_input_advance() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("gap_count_input");
    builder
        .input::<u64>("in")
        .unwrap()
        .map("id", |_t, x| x)
        .with_capacity(1)
        .unwrap()
        .count("cnt")
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<u64>("in").unwrap();
    let rx = h.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    for epoch in 0..3u64 {
        for _ in 0..1000 {
            tx.send(epoch, vec![epoch]).unwrap();
        }
        tx.advance_to(epoch + 1).unwrap();
    }
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let out = collector.join().unwrap();
    let g = group(&out);
    for epoch in 0..3u64 {
        let v: Vec<usize> = g.get(&epoch).cloned().unwrap_or_default().concat();
        assert_eq!(
            v,
            vec![1000],
            "epoch {epoch}: expected one count of 1000, got {} values (sum {})",
            v.len(),
            v.iter().sum::<usize>()
        );
    }
}

/// Same shape with DEFAULT channel capacities and a sustained stream of many
/// small epochs: input -> map -> count. Every epoch must produce exactly one
/// count.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn count_per_epoch_default_capacities_high_volume() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("gap_default_caps");
    builder
        .input::<u64>("in")
        .unwrap()
        .map("a", |_t, x| x)
        .map("b", |_t, x| x)
        .count("cnt")
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<u64>("in").unwrap();
    let rx = h.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    let epochs = 400u64;
    let per_epoch = 500usize;
    for epoch in 0..epochs {
        for _ in 0..per_epoch {
            tx.send(epoch, vec![epoch]).unwrap();
        }
        tx.advance_to(epoch + 1).unwrap();
    }
    drop(tx);
    with_timeout("join", Duration::from_secs(60), move || h.join_blocking()).unwrap();
    let g = group(&collector.join().unwrap());
    let bad: Vec<(u64, Vec<usize>)> = g
        .iter()
        .map(|(t, v)| (*t, v.concat()))
        .filter(|(_, v)| v != &vec![per_epoch])
        .take(5)
        .collect();
    assert!(
        bad.is_empty() && g.len() == epochs as usize,
        "{} epochs with split/incorrect counts (first: {:?}); epochs seen {}",
        g.iter()
            .filter(|(_, v)| v.concat() != vec![per_epoch])
            .count(),
        bad,
        g.len()
    );
}

/// A probe attached directly to an input stream must advance when the input
/// frontier is advanced (and become done when the input closes).
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
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

    let start = Instant::now();
    while !probe.done_with(&0) && start.elapsed() < Duration::from_secs(3) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let advanced = probe.done_with(&0);
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

/// A probe placed *after* a capability-holding operator (unary_notify) must not
/// report time t as done while that operator can still emit data at t.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
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

/// `delay` must reject (or at least not emit) delays to an earlier/incomparable
/// time; for a totally ordered time the check works, here we check that the
/// per-time `reduce` after `delay_batch` still sees every record exactly once
/// when many epochs are delayed into one window.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn delay_batch_window_then_count_is_exact() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_window_count");
    builder
        .input::<u64>("in")
        .unwrap()
        .delay_batch("win", |t| (t / 10 + 1) * 10)
        .count("cnt")
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let tx = h.take_input::<u64>("in").unwrap();
    let rx = h.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    for t in 0..30u64 {
        tx.send(t, vec![t; 3]).unwrap();
        tx.advance_to(t + 1).unwrap();
    }
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let g = group(&collector.join().unwrap());
    for w in [10u64, 20, 30] {
        assert_eq!(
            g.get(&w).cloned().unwrap_or_default().concat(),
            vec![30usize],
            "window {w}"
        );
    }
}

/// Single worker: input -> map -> exchange -> output with more batches than
/// one exchange-channel capacity (1024 envelopes) buffered at once.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn single_worker_exchange_more_than_channel_capacity() {
    let n: u64 = std::env::var("AUDIT_SW_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("sw_xchg_volume");
    let data: Vec<(u64, Vec<u64>)> = (0..n).map(|i| (0u64, vec![i])).collect();
    builder
        .source("src", data)
        .map("id", |_t, x| x)
        .exchange_by_hash("x", |x: &u64| *x)
        .output("out")
        .unwrap();
    let df = builder.build().unwrap();
    let mut h = rt.spawn(df, SpawnOptions::new()).unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || rx.collect_data());
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let total: usize = collector.join().unwrap().iter().map(|(_, d)| d.len()).sum();
    assert_eq!(total, n as usize);
}
