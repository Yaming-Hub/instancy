//! Early-finisher scenarios found by the exchange back-pressure work (PR 2):
//! the node that finishes first must not tear down while records, close
//! markers or progress it produced are still queued for a peer.
//!
//! Unlike PR 2's tests, every input here is closed as soon as it has been
//! fed. No node is held open until its peers have caught up, so the first
//! finisher really does reach teardown with frames still in flight. Each
//! scenario runs on Dedicated, per-dataflow Shared and pooled Shared
//! transports. Every round must succeed on every node and deliver every
//! record exactly once to the worker it belongs to: a lost, duplicated or
//! misrouted record, an error or a hang fails the round.
//!
//! The frontier-observing variants feed the same shapes over several epochs
//! and watch the receiving side's frontier: a per-epoch count after the
//! exchange, or a probe on the exchange's output. A receiver must never see
//! an epoch as complete while records of that epoch are still on their way
//! to it.
//!
//! The feeding node runs a single worker.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::progress::frontier::Antichain;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, ProbeHandle, Result};

use support::{
    Managers, drop_runtimes, fmt_join, join_with_timeout, spawn_node, tcp_mesh, tcp_shared_mesh,
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    /// Shared managers owned by the dataflow (released at its teardown).
    Shared,
    /// Long-lived shared managers that outlive the dataflow.
    SharedPooled,
}

const MODES: [Mode; 3] = [Mode::Dedicated, Mode::Shared, Mode::SharedPooled];

/// The scenarios run one at a time: each drives several nodes at full speed,
/// and running them side by side on a small machine (such as a CI runner)
/// only makes every round slower without testing anything else.
static ONE_AT_A_TIME: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn one_at_a_time() -> tokio::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// What the receiving side of the exchange computes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Observe {
    /// Passes the records on: `exchange -> map -> output`. The whole input is
    /// fed at time 0.
    Records,
    /// Counts its records per epoch: `exchange -> count -> map -> output`. A
    /// count is emitted once the worker's frontier has passed its epoch, so an
    /// epoch completed while some of its records were still on their way
    /// shows up as a short count (and a second count, or a failure, for the
    /// rest).
    Count,
    /// Probes the exchange's output: `exchange -> probe -> check -> map ->
    /// output`. `check` records every batch that arrives at a time its
    /// worker's probe already reported complete.
    Probe,
}

/// Batches per epoch when the receiving side observes frontiers
/// ([`Observe::Count`] and [`Observe::Probe`]).
const EPOCH_BATCHES: u64 = 100;

/// The probes of an [`Observe::Probe`] round (each worker builds its own) and
/// what their checks saw.
#[derive(Default)]
struct Probes {
    handles: Mutex<Vec<ProbeHandle<u64>>>,
    /// Batches that arrived at a time their worker's probe had reported
    /// complete.
    early: Mutex<Vec<String>>,
}

/// `input -> [map] -> exchange(route) -> ... -> output` (see [`Observe`]), at
/// the default edge capacity.
fn exchange(
    pre_map: bool,
    route: Route,
    observe: Observe,
    probes: Arc<Probes>,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        let input = b.input::<u64>("in")?;
        let input = if pre_map {
            input.map("pre", |_t, x| x)
        } else {
            input
        };
        let exchanged = input.exchange_by_hash("route", route);
        let out = match observe {
            Observe::Records => exchanged.map("post", |_t, x| x),
            Observe::Count => exchanged
                .count("count")
                .map("post", |_t, n: usize| n as u64),
            Observe::Probe => {
                let (exchanged, probe) = exchanged.probe();
                probes.handles.lock().unwrap().push(probe.clone());
                let probes = Arc::clone(&probes);
                exchanged
                    .inspect_batch("check", move |t: &u64, batch: &[u64]| {
                        if probe.done_with(t) {
                            probes.early.lock().unwrap().push(format!(
                                "{} records at {t} arrived after the probe reported {t} \
                                 complete (frontier {:?})",
                                batch.len(),
                                probe.frontier().elements()
                            ));
                        }
                    })
                    .map("post", |_t, x| x)
            }
        };
        out.output("out")?;
        Ok(())
    }
}

/// Routes a record: it belongs to worker `route(x) % total workers`.
type Route = fn(&u64) -> u64;

fn to_worker_1(_: &u64) -> u64 {
    1
}

fn by_value(x: &u64) -> u64 {
    *x
}

/// Output `(time, value)` pairs per worker, updated live (for diagnosing
/// hangs).
struct Sink {
    records: Arc<Mutex<Vec<(u64, u64)>>>,
    closed: std::sync::mpsc::Receiver<()>,
}

fn sink(output: instancy::OutputReceiver<u64, u64>) -> Sink {
    let records = Arc::new(Mutex::new(Vec::new()));
    let (tx, closed) = std::sync::mpsc::channel();
    let thread_records = Arc::clone(&records);
    std::thread::spawn(move || {
        while let Some(event) = output.recv() {
            if let instancy::dataflow::OutputEvent::Data { time, data } = event {
                thread_records
                    .lock()
                    .unwrap()
                    .extend(data.into_iter().map(|x| (time, x)));
            }
        }
        let _ = tx.send(());
    });
    Sink { records, closed }
}

/// Polls a probe until `stop` is set; returns the frontiers it reported, in
/// order.
fn watch(
    probe: ProbeHandle<u64>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Vec<Antichain<u64>>> {
    std::thread::spawn(move || {
        let mut seen = vec![probe.frontier()];
        loop {
            let frontier = probe.frontier();
            if seen.last() != Some(&frontier) {
                seen.push(frontier);
            }
            if stop.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        seen
    })
}

/// Whether `new` is at or beyond `old`.
fn dominates(old: &Antichain<u64>, new: &Antichain<u64>) -> bool {
    new.elements().iter().all(|t| old.less_equal(t))
}

/// Stops the probe watchers when a round ends, however it ends.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// What is wrong with the records the workers received: record `x` (below
/// `produced`) must reach worker `route(x) % workers` exactly once.
fn check_records(per_worker: &[Vec<(u64, u64)>], produced: u64, route: Route) -> String {
    let workers = per_worker.len() as u64;
    let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
    let mut misrouted = 0;
    for (w, out) in per_worker.iter().enumerate() {
        for &(_, x) in out {
            if x >= produced || route(&x) % workers != w as u64 {
                misrouted += 1;
            }
            *counts.entry(x).or_default() += 1;
        }
    }
    let duplicated = counts.values().filter(|c| **c > 1).count();
    let mut missing = vec![0usize; per_worker.len()];
    for x in (0..produced).filter(|x| !counts.contains_key(x)) {
        missing[(route(&x) % workers) as usize] += 1;
    }
    if duplicated == 0 && misrouted == 0 && missing.iter().all(|m| *m == 0) {
        return String::new();
    }
    format!("missing per worker {missing:?}, {duplicated} duplicated, {misrouted} misrouted")
}

/// What is wrong with the per-epoch counts: each worker must emit exactly one
/// count for every epoch it received records at, equal to their number, and
/// nothing else.
fn check_counts(
    per_worker: &[Vec<(u64, u64)>],
    produced: u64,
    per_epoch: u64,
    route: Route,
) -> String {
    let workers = per_worker.len() as u64;
    let mut expected: BTreeMap<(usize, u64), u64> = BTreeMap::new();
    for x in 0..produced {
        let worker = (route(&x) % workers) as usize;
        *expected.entry((worker, x / per_epoch)).or_default() += 1;
    }
    let mut got: BTreeMap<(usize, u64), Vec<u64>> = BTreeMap::new();
    for (w, out) in per_worker.iter().enumerate() {
        for &(epoch, count) in out {
            got.entry((w, epoch)).or_default().push(count);
        }
    }
    let keys: BTreeSet<&(usize, u64)> = expected.keys().chain(got.keys()).collect();
    let wrong: Vec<String> = keys
        .into_iter()
        .filter_map(|key| {
            let want: Vec<u64> = expected.get(key).into_iter().copied().collect();
            let have = got.get(key).cloned().unwrap_or_default();
            (want != have).then(|| {
                format!(
                    "worker {} epoch {}: counts {have:?}, expected {want:?}",
                    key.0, key.1
                )
            })
        })
        .collect();
    if wrong.is_empty() {
        return String::new();
    }
    format!(
        "{} wrong per-epoch counts, first: {}",
        wrong.len(),
        wrong[..wrong.len().min(5)].join(", ")
    )
}

/// Feeds `batches x per_batch` distinct records into global worker 0 through
/// `input -> [map] -> exchange_by_hash(route) -> ... -> output` (see
/// [`Observe`]) and closes every input as soon as it has been fed. Record `x`
/// belongs to worker `route(x) % total workers`. Batch `b` is fed at epoch
/// `b / EPOCH_BATCHES` when the receiving side observes frontiers, and at
/// time 0 otherwise.
///
/// The round is exact, and anything else is an error, if every node
/// succeeds, the whole input was fed, and every worker received exactly its
/// records. With [`Observe::Count`], every worker emitted exactly one count
/// per epoch it received records at, equal to their number. With
/// [`Observe::Probe`], no record arrived at a time its worker's probe had
/// reported complete, every probe only moved forward, and every probe is
/// done once the round succeeded.
#[allow(clippy::too_many_arguments)]
async fn run(
    workers: &[usize],
    mode: Mode,
    capacity: usize,
    pre_map: bool,
    route: Route,
    batches: u64,
    per_batch: u64,
    observe: Observe,
) -> std::result::Result<(), String> {
    let probes = Arc::new(Probes::default());
    let build = exchange(pre_map, route, observe, Arc::clone(&probes));
    let names: Vec<String> = (0..workers.len()).map(|i| format!("node-{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let topology = ClusterTopology::multi_node(
        names
            .iter()
            .zip(workers)
            .map(|(n, w)| NodeConfig::new(n.clone(), *w))
            .collect(),
    )
    .unwrap();
    let total_workers: usize = workers.iter().sum();
    let id = DataflowId::new();
    let mut pool: Vec<Managers> = Vec::new();
    let mut handles = Vec::new();
    match mode {
        Mode::Dedicated => {
            let mut conns = tcp_mesh(&name_refs).await;
            for name in &names {
                let transport =
                    ClusterSpawnTransport::dedicated(conns.remove(name).unwrap(), capacity);
                handles.push(spawn_node(
                    topology.clone(),
                    name,
                    id,
                    transport,
                    2,
                    build.clone(),
                ));
            }
        }
        Mode::Shared | Mode::SharedPooled => {
            let mut managers = tcp_shared_mesh(&name_refs, 1).await;
            for name in &names {
                let m = managers.remove(name).unwrap();
                if mode == Mode::SharedPooled {
                    pool.push(Arc::clone(&m));
                }
                handles.push(spawn_node(
                    topology.clone(),
                    name,
                    id,
                    ClusterSpawnTransport::shared(m, capacity),
                    2,
                    build.clone(),
                ));
            }
        }
    }
    let mut runtimes = Vec::new();
    let mut outputs = Vec::new();
    let mut joins = Vec::new();
    let mut feed = None;
    for handle in handles {
        let (rt, cluster) = handle.await.unwrap();
        let mut cluster = cluster.map_err(|e| format!("spawn: {e}"))?;
        for local in 0..cluster.num_local_workers() {
            outputs.push(sink(cluster.take_output::<u64>(local, "out").unwrap()));
            let input = cluster.take_input::<u64>(local, "in").unwrap();
            // Global worker 0 feeds; every other input closes now.
            if feed.is_none() {
                feed = Some(input);
            }
        }
        runtimes.push(rt);
        joins.push(join_with_timeout(cluster, Duration::from_secs(60)));
    }
    let input = feed.expect("worker 0 exists");
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_watchers = StopOnDrop(Arc::clone(&stop));
    let watchers: Vec<_> = probes
        .handles
        .lock()
        .unwrap()
        .iter()
        .map(|probe| watch(probe.clone(), Arc::clone(&stop)))
        .collect();
    let epoch_batches = if observe == Observe::Records {
        batches
    } else {
        EPOCH_BATCHES
    };
    // Returns how many batches it fed.
    let feeder = std::thread::spawn(move || {
        for b in 0..batches {
            let epoch = b / epoch_batches;
            let advanced = if b > 0 && b % epoch_batches == 0 {
                input.advance_to(epoch)
            } else {
                Ok(())
            };
            let batch = (b * per_batch..(b + 1) * per_batch).collect();
            if let Err(e) = advanced.and_then(|()| input.send(epoch, batch)) {
                return (b, Some(format!("feeding batch {b} failed: {e}")));
            }
        }
        (batches, None)
    });
    let mut results = Vec::new();
    for j in joins {
        results.push(j.await.unwrap());
    }
    let joined = results
        .iter()
        .enumerate()
        .map(|(i, r)| format!("node-{i}={}", fmt_join(r)))
        .collect::<Vec<_>>()
        .join(" ");
    if results.iter().any(Option::is_none) {
        let received: Vec<usize> = outputs
            .iter()
            .map(|o| o.records.lock().unwrap().len())
            .collect();
        let fed = feeder.is_finished();
        // A wedged dataflow can block runtime or transport teardown (and the
        // feeder) forever.
        std::mem::forget((runtimes, pool, feeder));
        return Err(format!(
            "{mode:?} {observe:?}: {joined}; received per worker {received:?}; input fed: {fed}"
        ));
    }
    // Every worker has stopped (successfully or not): its output closes.
    let mut per_worker = Vec::new();
    for (w, out) in outputs.into_iter().enumerate() {
        out.closed
            .recv_timeout(Duration::from_secs(20))
            .map_err(|_| {
                format!("{mode:?} {observe:?}: {joined}; output of worker {w} never closed")
            })?;
        per_worker.push(std::mem::take(&mut *out.records.lock().unwrap()));
    }
    let succeeded = results.iter().all(|r| matches!(r, Some(Ok(()))));
    let mut problems = Vec::new();
    if observe == Observe::Probe {
        let handles = probes.handles.lock().unwrap().clone();
        if handles.len() != total_workers {
            problems.push(format!(
                "{} probes for {total_workers} workers",
                handles.len()
            ));
        }
        if succeeded {
            for (i, probe) in handles.iter().enumerate() {
                let waited =
                    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done()).await;
                if !probe.is_done() || !matches!(waited, Ok(Ok(()))) {
                    problems.push(format!(
                        "probe {i} not done after a successful run: frontier {:?}, \
                         wait_until_done -> {waited:?}",
                        probe.frontier().elements()
                    ));
                }
            }
        }
        stop.store(true, Ordering::SeqCst);
        for (i, watcher) in watchers.into_iter().enumerate() {
            let seen = watcher.join().unwrap();
            if let Some(pair) = seen.windows(2).find(|p| !dominates(&p[0], &p[1])) {
                problems.push(format!(
                    "probe {i} moved backwards: {:?} -> {:?}",
                    pair[0].elements(),
                    pair[1].elements()
                ));
            }
        }
        let early = probes.early.lock().unwrap();
        if !early.is_empty() {
            problems.push(format!(
                "{} batches arrived at a time their worker's probe had reported complete, \
                 first: {}",
                early.len(),
                early[..early.len().min(3)].join(", ")
            ));
        }
    }
    drop_runtimes(runtimes).await;
    drop(pool);
    let (fed_batches, feed_error) = feeder.join().unwrap();
    let produced = fed_batches * per_batch;
    if !succeeded {
        problems.push("a node failed".to_string());
    }
    problems.extend(feed_error);
    let wrong = match observe {
        Observe::Records | Observe::Probe => check_records(&per_worker, produced, route),
        Observe::Count => check_counts(&per_worker, produced, epoch_batches * per_batch, route),
    };
    if !wrong.is_empty() {
        problems.push(wrong);
    }
    if problems.is_empty() {
        return Ok(());
    }
    let received: Vec<usize> = per_worker.iter().map(Vec::len).collect();
    Err(format!(
        "{mode:?} {observe:?}: {joined}; fed {fed_batches}/{batches} batches; \
         outputs per worker {received:?}; {}",
        problems.join("; ")
    ))
}

/// Scenario 1: every record is routed to node-1 while node-0 closes its input
/// right after the burst. Before the shutdown handshake, node-0 joined `Ok`
/// with about 30% of the records still queued for node-1, which then hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn single_target_burst_from_early_finisher_arrives_in_full() {
    let _serial = one_at_a_time().await;
    let mut failures = Vec::new();
    let mut exact = Exact::default();
    for mode in MODES {
        for pre_map in [false, true] {
            for round in 0..2 {
                let result = run(
                    &[1, 1],
                    mode,
                    1024,
                    pre_map,
                    to_worker_1,
                    3000,
                    7,
                    Observe::Records,
                )
                .await;
                if let Err(e) = exact.round(result) {
                    failures.push(format!("pre_map={pre_map} round {round}: {e}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    exact.require(12);
}

/// Batches fed per split-routing round. Every batch crosses nodes, so the
/// sender finishes with frames in flight.
const SPLIT_BATCHES: u64 = 1000;

/// Scenario 2: records are spread over both nodes and all of them arrive,
/// but the node that finishes first used to drop its final progress frames on
/// teardown, leaving its peer waiting forever (about 5% of Shared runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delivery_is_followed_by_completion_everywhere() {
    let _serial = one_at_a_time().await;
    let mut failures = Vec::new();
    let mut exact = Exact::default();
    for mode in MODES {
        for round in 0..8 {
            let workers: &[usize] = if round % 2 == 0 { &[1, 1] } else { &[1, 2, 2] };
            let result = run(
                workers,
                mode,
                1024,
                true,
                by_value,
                SPLIT_BATCHES * 2,
                16,
                Observe::Records,
            )
            .await;
            if let Err(e) = exact.round(result) {
                failures.push(format!("{workers:?} round {round}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    exact.require(24);
}

/// Scenario 3: Dedicated transport under sustained back-pressure (tiny
/// per-peer queues); the tail of the stream (e.g. the last 8 records) used to
/// be lost silently when the sender finished first. Congested senders hold
/// records in blocked operators until their links drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn congested_dedicated_links_do_not_lose_the_tail() {
    let _serial = one_at_a_time().await;
    let mut failures = Vec::new();
    let mut exact = Exact::default();
    for capacity in [1, 2, 16] {
        for (round, workers) in [&[1usize, 1][..], &[1, 2], &[1, 2, 2]].iter().enumerate() {
            let result = run(
                workers,
                Mode::Dedicated,
                capacity,
                true,
                by_value,
                SPLIT_BATCHES,
                16,
                Observe::Records,
            )
            .await;
            if let Err(e) = exact.round(result) {
                failures.push(format!(
                    "capacity {capacity} {workers:?} round {round}: {e}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    exact.require(9);
}

/// The early-finisher scenarios fed over several epochs, with `observe` on
/// the receiving side: scenario 1's single-target burst and scenario 2's
/// split routing twice on every transport, then scenario 3's congested
/// Dedicated links. Unblocked bursts move the receivers' frontiers across
/// several epochs at once; congested links make them step through every
/// epoch while later records are still on their way. Every round must be
/// exact.
async fn frontier_observing_rounds(observe: Observe) {
    let _serial = one_at_a_time().await;
    let mut failures = Vec::new();
    let mut exact = Exact::default();
    let shapes: [(&str, &[usize], Route, u64, u64); 2] = [
        ("single target", &[1, 1], to_worker_1, 3000, 7),
        ("split", &[1, 2, 2], by_value, SPLIT_BATCHES * 2, 16),
    ];
    for mode in MODES {
        for (shape, workers, route, batches, per_batch) in shapes {
            for round in 0..2 {
                let result = run(
                    workers, mode, 1024, true, route, batches, per_batch, observe,
                )
                .await;
                if let Err(e) = exact.round(result) {
                    failures.push(format!("{shape} {workers:?} round {round}: {e}"));
                }
            }
        }
    }
    for capacity in [1, 2, 16] {
        let result = run(
            &[1, 2, 2],
            Mode::Dedicated,
            capacity,
            true,
            by_value,
            SPLIT_BATCHES,
            16,
            observe,
        )
        .await;
        if let Err(e) = exact.round(result) {
            failures.push(format!("congested, capacity {capacity}: {e}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    exact.require(15);
}

/// The receiving side counts its records per epoch after the exchange. A
/// worker whose frontier passed an epoch while records of that epoch were
/// still on their way emits a short count; every count must be exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn per_epoch_counts_behind_an_early_finisher_are_exact() {
    frontier_observing_rounds(Observe::Count).await;
}

/// A probe on the exchange's output, on every worker: it must only move
/// forward, never report an epoch complete while records of that epoch still
/// arrive, and be done once the round succeeded. Every record must arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn probe_behind_an_early_finisher_is_monotone_conservative_and_done() {
    frontier_observing_rounds(Observe::Probe).await;
}

/// Counts the rounds run and the rounds that were exact.
#[derive(Default)]
struct Exact {
    exact: usize,
    rounds: usize,
}

impl Exact {
    fn round(
        &mut self,
        result: std::result::Result<(), String>,
    ) -> std::result::Result<(), String> {
        self.rounds += 1;
        self.exact += usize::from(result.is_ok());
        result
    }

    /// Require that `rounds` rounds ran and every one of them was exact.
    fn require(&self, rounds: usize) {
        eprintln!(
            "{} of {} rounds delivered every record",
            self.exact, self.rounds
        );
        assert!(
            self.exact == rounds && self.rounds == rounds,
            "{} of {} rounds delivered every record (expected all of {rounds})",
            self.exact,
            self.rounds
        );
    }
}
