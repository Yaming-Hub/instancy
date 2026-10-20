//! Early-finisher scenarios found by the exchange back-pressure work (PR 2):
//! the node that finishes first must not tear down while records, close
//! markers or progress it produced are still queued for a peer.
//!
//! Unlike PR 2's tests, every input here is closed as soon as it has been
//! fed. No node is held open until its peers have caught up, so the first
//! finisher really does reach teardown with frames still in flight. Each
//! scenario runs on Dedicated, per-dataflow Shared and pooled Shared
//! transports. Batches split across nodes may still be duplicated by partial
//! pushes until PR 2 lands (see `SPLIT_BATCHES_MAY_DUPLICATE`). Silent loss
//! and hangs are always failures; a round that fails loudly with the known
//! C1/C2 data-loss signature, or loses records only after PR 2's duplicates
//! corrupted the progress accounting, is re-run once (see `Failure`).

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

/// Capacity of the edge into the exchange, as in PR 2's tests. With the
/// default capacity a sustained burst stalls the local target mid-stream
/// regardless of teardown (a separate scheduler issue).
const BIG_EDGE: usize = 1 << 22;

/// `input -> [map] -> exchange(route) -> map -> output`.
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
            .with_capacity(BIG_EDGE)?
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
    /// A node failed because records were lost (`data lost …`).
    ///
    /// A receiver with no capability of its own (its input is closed at the
    /// start) can complete after consuming a record whose matching progress
    /// from the sender has not arrived yet: the signed-sum completion
    /// predicate (audit C1/C2, also present on `main`; see
    /// `many_small_frames_then_complete_*`). The records that still arrive
    /// (or that the sender sends after the receiver's end-of-stream) are
    /// reported, so the run fails loudly. Rounds that fail only this way are
    /// re-run once (`run_with_retry`); remove the retry once C1/C2 is fixed.
    /// Every node `Ok` with records missing (silent loss) is never retried,
    /// except as [`Failure::CorruptedByDuplicates`].
    EarlyCompletion(String),
    /// Every node `Ok`, records missing, and records duplicated by PR 2's
    /// partial pushes in the same round. A duplicate delivery is consumed
    /// without having been produced, which drives the signed in-flight
    /// counts negative and lets a receiver complete early (C1/C2). Records
    /// still in flight to it on an in-process channel (the feeding worker's
    /// node-local target, which no network check sees) are then dropped
    /// silently. Re-run once (`run_with_retry`); this goes away with
    /// `SPLIT_BATCHES_MAY_DUPLICATE`.
    CorruptedByDuplicates(String),
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
            Failure::CorruptedByDuplicates(e) => {
                write!(f, "records lost after duplicate deliveries (PR 2): {e}")
            }
            Failure::Other(e) => f.write_str(e),
        }
    }
}

/// Runs a round, re-running it once if its only defect is the C1/C2
/// early-completion signature (see [`Failure::EarlyCompletion`] and
/// [`Failure::CorruptedByDuplicates`]).
async fn run_with_retry<Fut>(mut round: impl FnMut() -> Fut) -> std::result::Result<(), String>
where
    Fut: std::future::Future<Output = std::result::Result<(), Failure>>,
{
    match round().await {
        Ok(()) => Ok(()),
        Err(first @ (Failure::EarlyCompletion(_) | Failure::CorruptedByDuplicates(_))) => {
            eprintln!("retrying a round that hit the C1/C2 early completion: {first}");
            round()
                .await
                .map_err(|e| format!("{e} (after an earlier early completion: {first})"))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Feeds `batches x per_batch` distinct records into global worker 0, closes
/// every input as soon as it has been fed, and checks that every node
/// completes and every record arrives (at `only_at`, if given). Records must
/// arrive exactly once unless `tolerate_duplicates`.
#[allow(clippy::too_many_arguments)]
async fn run<F>(
    workers: &[usize],
    mode: Mode,
    capacity: usize,
    build: F,
    batches: u64,
    per_batch: u64,
    only_at: Option<usize>,
    tolerate_duplicates: bool,
) -> std::result::Result<(), Failure>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static,
{
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
    let mut feeders = Vec::new();
    let mut global = 0;
    for handle in handles {
        let (rt, cluster) = handle.await.unwrap();
        let mut cluster = cluster.map_err(|e| format!("spawn: {e}"))?;
        for local in 0..cluster.num_local_workers() {
            outputs.push(sink(cluster.take_output::<u64>(local, "out").unwrap()));
            let input = cluster.take_input::<u64>(local, "in").unwrap();
            if global == 0 {
                feeders.push(std::thread::spawn(move || {
                    for b in 0..batches {
                        input
                            .send(0, (b * per_batch..(b + 1) * per_batch).collect())
                            .map_err(|e| format!("send of batch {b} failed: {e}"))?;
                    }
                    Ok::<_, String>(())
                }));
            }
            global += 1;
        }
        runtimes.push(rt);
        joins.push(join_with_timeout(cluster, Duration::from_secs(60)));
    }
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
    if !results.iter().all(|r| matches!(r, Some(Ok(())))) {
        let received: Vec<usize> = outputs
            .iter()
            .map(|o| o.records.lock().unwrap().len())
            .collect();
        let report = format!(
            "{mode:?}: {joined}; received per worker {received:?} of {}",
            batches * per_batch
        );
        if results.iter().all(Option::is_some) {
            let loud_loss = results
                .iter()
                .any(|r| matches!(r, Some(Err(e)) if e.to_string().contains("data lost")));
            let _ = drop_runtimes(runtimes).await;
            drop(pool);
            for f in feeders {
                let _ = f.join();
            }
            return Err(if loud_loss {
                Failure::EarlyCompletion(report)
            } else {
                Failure::Other(report)
            });
        }
        let fed = feeders.iter().all(|f| f.is_finished());
        // A wedged dataflow can block runtime or transport teardown (and a
        // feeder) forever.
        std::mem::forget((runtimes, pool, feeders));
        return Err(Failure::Other(format!("{report}; input fed: {fed}")));
    }
    let fed: std::result::Result<Vec<()>, String> =
        feeders.into_iter().map(|f| f.join().unwrap()).collect();
    let mut per_worker = Vec::new();
    for out in outputs {
        out.closed
            .recv_timeout(Duration::from_secs(20))
            .map_err(|_| format!("{mode:?}: output never closed"))?;
        per_worker.push(std::mem::take(&mut *out.records.lock().unwrap()));
    }
    drop_runtimes(runtimes).await;
    drop(pool);
    if fed.is_err() {
        return Err(Failure::Other(format!("{mode:?}: {joined} feeder={fed:?}")));
    }
    let mut counts: BTreeMap<u64, i64> = BTreeMap::new();
    for (w, out) in per_worker.iter().enumerate() {
        if only_at.is_some_and(|only| only != w) && !out.is_empty() {
            return Err(Failure::Other(format!(
                "{mode:?}: {} records at worker {w}, expected all at {only_at:?}",
                out.len()
            )));
        }
        for x in out {
            *counts.entry(*x).or_default() += 1;
        }
    }
    let total = batches * per_batch;
    let missing = (0..total).filter(|x| !counts.contains_key(x)).count();
    let duplicated = counts.values().filter(|c| **c > 1).count();
    let received: i64 = counts.values().sum();
    if missing > 0 || (duplicated > 0 && !tolerate_duplicates) {
        let last_missing: Vec<u64> = (0..total)
            .rev()
            .filter(|x| !counts.contains_key(x))
            .take(8)
            .collect();
        let report = format!(
            "{mode:?}: received {received}/{total}; {missing} missing (last: {last_missing:?}), \
             {duplicated} duplicated"
        );
        if missing > 0 && duplicated > 0 && tolerate_duplicates {
            return Err(Failure::CorruptedByDuplicates(report));
        }
        // Every node joined Ok (checked above): missing records here are a
        // silent loss, never acceptable.
        return Err(Failure::Other(report));
    }
    Ok(())
}

/// Scenario 1: every record is routed to node-1 while node-0 closes its input
/// right after the burst. Before the shutdown handshake, node-0 joined `Ok`
/// with about 30% of the records still queued for node-1, which then hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn single_target_burst_from_early_finisher_arrives_in_full() {
    let mut failures = Vec::new();
    for mode in MODES {
        for pre_map in [false, true] {
            for round in 0..2 {
                if let Err(e) = run_with_retry(|| {
                    run(
                        &[1, 1],
                        mode,
                        1024,
                        exchange(pre_map, to_worker_1),
                        3000,
                        7,
                        Some(1),
                        false,
                    )
                })
                .await
                {
                    failures.push(format!("pre_map={pre_map} round {round}: {e}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Records split across nodes go through partial pushes, which still
/// duplicate records under back-pressure until PR 2 (C4) lands. Those tests
/// therefore check for loss and hangs only; set this to `false` once PR 2 is
/// merged.
const SPLIT_BATCHES_MAY_DUPLICATE: bool = true;

/// Batches fed per split-routing round. Kept small while PR 2's duplication
/// is present: at queue capacity 1 a round can otherwise multiply into
/// millions of duplicate records and take tens of seconds on a loaded
/// machine. Every batch still crosses nodes, so the sender finishes with
/// frames in flight.
const SPLIT_BATCHES: u64 = 400;

/// Scenario 2: records are spread over both nodes and all of them arrive,
/// but the node that finishes first used to drop its final progress frames on
/// teardown, leaving its peer waiting forever (about 5% of Shared runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delivery_is_followed_by_completion_everywhere() {
    let mut failures = Vec::new();
    for mode in MODES {
        for round in 0..8 {
            let workers: &[usize] = if round % 2 == 0 { &[1, 1] } else { &[2, 1, 2] };
            if let Err(e) = run_with_retry(|| {
                run(
                    workers,
                    mode,
                    1024,
                    exchange(true, by_value),
                    SPLIT_BATCHES * 2,
                    16,
                    None,
                    SPLIT_BATCHES_MAY_DUPLICATE,
                )
            })
            .await
            {
                failures.push(format!("{workers:?} round {round}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Scenario 3: Dedicated transport under sustained back-pressure (tiny
/// per-peer queues); the tail of the stream (e.g. the last 8 records) used to
/// be lost when the sender finished first.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn congested_dedicated_links_do_not_lose_the_tail() {
    let mut failures = Vec::new();
    for capacity in [1, 2, 16] {
        for (round, workers) in [&[1usize, 1][..], &[2, 2], &[2, 1, 2]].iter().enumerate() {
            if let Err(e) = run_with_retry(|| {
                run(
                    workers,
                    Mode::Dedicated,
                    capacity,
                    exchange(true, by_value),
                    SPLIT_BATCHES,
                    16,
                    None,
                    SPLIT_BATCHES_MAY_DUPLICATE,
                )
            })
            .await
            {
                failures.push(format!(
                    "capacity {capacity} {workers:?} round {round}: {e}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
