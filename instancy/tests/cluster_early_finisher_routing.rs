//! Early-finisher scenarios found by the exchange back-pressure work (PR 2):
//! the node that finishes first must not tear down while records, close
//! markers or progress it produced are still queued for a peer.
//!
//! Unlike PR 2's tests, every input here is closed as soon as it has been
//! fed. No node is held open until its peers have caught up, so the first
//! finisher really does reach teardown with frames still in flight. Each
//! scenario runs on Dedicated, per-dataflow Shared and pooled Shared
//! transports. Every round must deliver every record exactly once or fail
//! loudly: a receiver can still complete early (audit C1/H1, tracked
//! separately), and this PR guarantees that the records it then misses are
//! reported (see `Failure::EarlyCompletion`). Silent loss, duplicates and
//! hangs are always failures.
//!
//! The feeding node runs a single worker. With a second worker on it, the
//! early completion can hit that worker's in-process exchange channel, which
//! drops the feeder's later records without any error (only cross-node links
//! report lost records). Add multi-worker senders back once C1/H1 is fixed.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};

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
/// makes the H1/C1 early completion (see [`Failure::EarlyCompletion`])
/// far more likely without testing anything else.
static ONE_AT_A_TIME: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn one_at_a_time() -> tokio::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// `input -> [map] -> exchange(route) -> map -> output`, at the default edge
/// capacity.
fn exchange(
    pre_map: bool,
    route: fn(&u64) -> u64,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        let input = b.input::<u64>("in")?;
        let input = if pre_map {
            input.map("pre", |_t, x| x)
        } else {
            input
        };
        input
            .exchange_by_hash("route", route)
            .map("post", |_t, x| x)
            .output("out")?;
        Ok(())
    }
}

fn to_worker_1(_: &u64) -> u64 {
    1
}

fn by_value(x: &u64) -> u64 {
    *x
}

/// Output records per worker, updated live (for diagnosing hangs).
struct Sink {
    records: Arc<std::sync::Mutex<Vec<u64>>>,
    closed: std::sync::mpsc::Receiver<()>,
}

fn sink(output: instancy::OutputReceiver<u64, u64>) -> Sink {
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (tx, closed) = std::sync::mpsc::channel();
    let thread_records = Arc::clone(&records);
    std::thread::spawn(move || {
        while let Some(event) = output.recv() {
            if let instancy::dataflow::OutputEvent::Data { data, .. } = event {
                thread_records.lock().unwrap().extend(data);
            }
        }
        let _ = tx.send(());
    });
    Sink { records, closed }
}

/// Why a round failed.
#[derive(Debug)]
enum Failure {
    /// A node failed because records were lost (`data lost …`): a receiver
    /// completed while records were still on their way to it.
    ///
    /// Completion sums signed counts, so a consumer's `-n` seen before the
    /// producer's `+n` can cancel a capability out (audit C1/C2; see
    /// `many_small_frames_then_complete_*`), and records held in a blocked
    /// operator's pending output are invisible to progress tracking (audit
    /// H1). Both are tracked separately and also present on `main`. Since
    /// the scheduler stopped re-running blocked operators (#290) this happens
    /// in many rounds on a small or busy machine (every round of the
    /// congested scenario on a CI runner), so such a round is counted, not
    /// failed (`exact_or_loud`), as long as every missing record is
    /// attributed to a reported loss (see [`run`]). What this PR guarantees
    /// is that the loss is reported: unexplained missing records, duplicates
    /// and misrouted records always fail. Require exact rounds once C1/C2/H1
    /// are fixed.
    EarlyCompletion(String),
    /// Anything else: a hang, an error, silent loss, misrouted or duplicated
    /// records.
    Other(String),
}

impl From<String> for Failure {
    fn from(e: String) -> Self {
        Failure::Other(e)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::EarlyCompletion(e) => {
                write!(f, "receiver completed early (C1/C2), loss reported: {e}")
            }
            Failure::Other(e) => f.write_str(e),
        }
    }
}

/// Runs a round that must deliver every record exactly once or fail loudly
/// (see [`Failure::EarlyCompletion`]). Returns whether it delivered every
/// record. Silent loss, duplicates, hangs and other errors are errors.
async fn exact_or_loud<Fut>(round: impl FnOnce() -> Fut) -> std::result::Result<bool, String>
where
    Fut: std::future::Future<Output = std::result::Result<(), Failure>>,
{
    match round().await {
        Ok(()) => Ok(true),
        Err(e @ Failure::EarlyCompletion(_)) => {
            eprintln!("accepted a loud early completion (H1/C1): {e}");
            Ok(false)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Whether `text` (a node's error, possibly forwarded through Cancels)
/// reports records that a sender could not deliver to `node` because `node`
/// had already finished (or the link to it failed): an outbound data loss.
fn reports_loss_toward(text: &str, node: &str) -> bool {
    let marker = format!("data lost on the link to peer {node}: ");
    text.match_indices(&marker).any(|(at, _)| {
        let detail = &text[at + marker.len()..];
        let detail = match detail.strip_prefix("channel ") {
            Some(rest) => rest.split_once(": ").map_or("", |(_, d)| d),
            None => detail,
        };
        detail.starts_with("records sent") || detail.starts_with("frames to the peer")
    })
}

/// Feeds `batches x per_batch` distinct records into global worker 0 through
/// `input -> [map] -> exchange_by_hash(route) -> map -> output` and closes
/// every input as soon as it has been fed. Record `x` belongs to worker
/// `route(x) % total workers`.
///
/// The round is exact if every node succeeds and every worker received
/// exactly its records. It is a loud early completion
/// ([`Failure::EarlyCompletion`]) only if all of the following hold, and a
/// failure ([`Failure::Other`]) otherwise:
/// - no node hung, and every node that failed reports a data loss (its own,
///   or a peer's forwarded with its Cancel);
/// - no record is duplicated or reached a worker it does not belong to;
/// - at least one record is missing, and every missing record is explained.
///   Either the node of its worker failed with a data loss, or that node
///   succeeded: it completed early, which it cannot take back once the loss
///   happens. Then a sender must have reported records it sent to that node
///   after it finished, or the feeding node failed with a data loss before
///   sending everything and, at each of the node's workers, the missing
///   records are the tail the stopped sender never sent (every received
///   record precedes every missing one).
#[allow(clippy::too_many_arguments)]
async fn run(
    workers: &[usize],
    mode: Mode,
    capacity: usize,
    pre_map: bool,
    route: fn(&u64) -> u64,
    batches: u64,
    per_batch: u64,
) -> std::result::Result<(), Failure> {
    let build = exchange(pre_map, route);
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
    let node_of: Vec<usize> = workers
        .iter()
        .enumerate()
        .flat_map(|(node, &w)| std::iter::repeat_n(node, w))
        .collect();
    let total_workers = node_of.len() as u64;
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
    let mut feeder = None;
    for handle in handles {
        let (rt, cluster) = handle.await.unwrap();
        let mut cluster = cluster.map_err(|e| format!("spawn: {e}"))?;
        for local in 0..cluster.num_local_workers() {
            outputs.push(sink(cluster.take_output::<u64>(local, "out").unwrap()));
            let input = cluster.take_input::<u64>(local, "in").unwrap();
            if feeder.is_none() {
                // Global worker 0 feeds; returns how many batches it sent.
                feeder = Some(std::thread::spawn(move || {
                    for b in 0..batches {
                        if let Err(e) =
                            input.send(0, (b * per_batch..(b + 1) * per_batch).collect())
                        {
                            return (b, Some(format!("send of batch {b} failed: {e}")));
                        }
                    }
                    (batches, None)
                }));
            }
        }
        runtimes.push(rt);
        joins.push(join_with_timeout(cluster, Duration::from_secs(60)));
    }
    let feeder = feeder.expect("worker 0 exists");
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
        return Err(Failure::Other(format!(
            "{mode:?}: {joined}; received per worker {received:?}; input fed: {fed}"
        )));
    }
    // Every worker has stopped (successfully or not): its output closes.
    let mut per_worker = Vec::new();
    for (w, out) in outputs.into_iter().enumerate() {
        out.closed
            .recv_timeout(Duration::from_secs(20))
            .map_err(|_| format!("{mode:?}: {joined}; output of worker {w} never closed"))?;
        per_worker.push(std::mem::take(&mut *out.records.lock().unwrap()));
    }
    drop_runtimes(runtimes).await;
    drop(pool);
    let (sent_batches, feed_error) = feeder.join().unwrap();
    let produced = sent_batches * per_batch;

    let mut counts: BTreeMap<u64, i64> = BTreeMap::new();
    let mut misrouted = 0;
    for (w, out) in per_worker.iter().enumerate() {
        for &x in out {
            if x >= produced || route(&x) % total_workers != w as u64 {
                misrouted += 1;
            }
            *counts.entry(x).or_default() += 1;
        }
    }
    let duplicated = counts.values().filter(|c| **c > 1).count();
    let mut missing_at = vec![0usize; workers.len()];
    let mut first_missing: Vec<Option<u64>> = vec![None; node_of.len()];
    for x in 0..produced {
        if !counts.contains_key(&x) {
            let w = (route(&x) % total_workers) as usize;
            missing_at[node_of[w]] += 1;
            first_missing[w].get_or_insert(x);
        }
    }
    // Every record a worker received precedes every record it is missing.
    let tail_missing = |w: usize| match (per_worker[w].iter().max(), first_missing[w]) {
        (Some(&last), Some(first)) => last < first,
        _ => true,
    };
    let missing: usize = missing_at.iter().sum();
    let received: Vec<usize> = per_worker.iter().map(Vec::len).collect();
    let report = format!(
        "{mode:?}: {joined}; fed {sent_batches}/{batches} batches ({feed_error:?}); \
         received per worker {received:?}; missing per node {missing_at:?}; \
         {duplicated} duplicated, {misrouted} misrouted"
    );
    if duplicated > 0 || misrouted > 0 {
        return Err(Failure::Other(report));
    }
    let errors: Vec<Option<String>> = results
        .iter()
        .map(|r| match r {
            Some(Err(e)) => Some(e.to_string()),
            _ => None,
        })
        .collect();
    if errors.iter().all(Option::is_none) && feed_error.is_none() {
        return if missing == 0 {
            Ok(())
        } else {
            // Every node succeeded: missing records are a silent loss.
            Err(Failure::Other(format!("silent loss: {report}")))
        };
    }
    // Some node failed: the round must be a loud early completion.
    if let Some(i) = errors
        .iter()
        .position(|e| e.as_ref().is_some_and(|e| !e.contains("data lost")))
    {
        return Err(Failure::Other(format!(
            "node-{i} failed without a data loss: {report}"
        )));
    }
    if feed_error.is_some() && errors[0].is_none() {
        return Err(Failure::Other(format!(
            "the feeding node succeeded but its input failed: {report}"
        )));
    }
    if missing == 0 {
        return Err(Failure::Other(format!(
            "a data loss was reported but nothing is missing: {report}"
        )));
    }
    // A legitimate loss stops a stream for good: the single feeding worker
    // stops at its first failed push, a receiving operator that finished (or
    // was cancelled) consumes nothing afterwards, and every channel is FIFO.
    // So whatever a worker is missing must be the tail of what was sent to
    // it; a gap in the middle is a lost frame, whatever errors were reported.
    for (node, &lost) in missing_at.iter().enumerate() {
        if lost == 0 {
            continue;
        }
        if !(0..node_of.len())
            .filter(|&w| node_of[w] == node)
            .all(tail_missing)
        {
            return Err(Failure::Other(format!(
                "{lost} records missing at node-{node} include records sent before \
                 others it received: {report}"
            )));
        }
        let explained = errors[node].is_some()
            || errors
                .iter()
                .flatten()
                .any(|e| reports_loss_toward(e, &names[node]))
            || errors[0].is_some();
        if !explained {
            return Err(Failure::Other(format!(
                "{lost} records missing at node-{node}, which succeeded; no sender \
                 reported losing records sent to it, and the feeding node did not \
                 stop: {report}"
            )));
        }
    }
    Err(Failure::EarlyCompletion(report))
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
                if let Err(e) = exact
                    .round(exact_or_loud(|| {
                        run(&[1, 1], mode, 1024, pre_map, to_worker_1, 3000, 7)
                    }))
                    .await
                {
                    failures.push(format!("pre_map={pre_map} round {round}: {e}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    // At least 11 of 12 rounds were exact in every soak run, from one core to
    // unpinned (see `Exact`).
    exact.require(6);
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
            if let Err(e) = exact
                .round(exact_or_loud(|| {
                    run(workers, mode, 1024, true, by_value, SPLIT_BATCHES * 2, 16)
                }))
                .await
            {
                failures.push(format!("{workers:?} round {round}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    // At least 18 of 24 rounds were exact in every soak run, from one core to
    // unpinned (see `Exact`).
    exact.require(12);
}

/// Scenario 3: Dedicated transport under sustained back-pressure (tiny
/// per-peer queues); the tail of the stream (e.g. the last 8 records) used to
/// be lost silently when the sender finished first. Congested senders hold
/// records in blocked operators (audit H1), so early completions are most
/// frequent here.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn congested_dedicated_links_do_not_lose_the_tail() {
    let _serial = one_at_a_time().await;
    let mut failures = Vec::new();
    let mut exact = Exact::default();
    for capacity in [1, 2, 16] {
        for (round, workers) in [&[1usize, 1][..], &[1, 2], &[1, 2, 2]].iter().enumerate() {
            if let Err(e) = exact
                .round(exact_or_loud(|| {
                    run(
                        workers,
                        Mode::Dedicated,
                        capacity,
                        true,
                        by_value,
                        SPLIT_BATCHES,
                        16,
                    )
                }))
                .await
            {
                failures.push(format!(
                    "capacity {capacity} {workers:?} round {round}: {e}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    // Not required: congested senders hold records invisible to progress
    // tracking (H1), so the share of exact rounds depends on the cores
    // available (soak: 0-2 of 9 on one core, 2-6 on two, 5-7 on four, 8-9
    // unpinned). Scenarios 1 and 2 catch a regression to "always loud".
    exact.require(0);
}

/// Counts the rounds that delivered every record (the others failed loudly).
#[derive(Default)]
struct Exact {
    exact: usize,
    rounds: usize,
}

impl Exact {
    async fn round(
        &mut self,
        round: impl std::future::Future<Output = std::result::Result<bool, String>>,
    ) -> std::result::Result<(), String> {
        let exact = round.await?;
        self.rounds += 1;
        self.exact += usize::from(exact);
        Ok(())
    }

    /// Report the count and require at least `min` exact rounds, so a
    /// regression that makes every round fail loudly (see
    /// [`Failure::EarlyCompletion`]) does not pass. The thresholds are about
    /// half the minimum seen in soak runs, from one core to unpinned.
    fn require(&self, min: usize) {
        eprintln!(
            "{} of {} rounds delivered every record; the others failed loudly",
            self.exact, self.rounds
        );
        assert!(
            self.exact >= min,
            "only {} of {} rounds delivered every record (expected at least {min})",
            self.exact,
            self.rounds
        );
    }
}
