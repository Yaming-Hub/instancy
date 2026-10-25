//! Adversarial cluster failure suite: every way a link can die or carry
//! garbage, at every phase, in both transport modes, with 2-4 nodes.
//!
//! Invariant under test: a link that ends without its clean end-of-stream
//! (and is not retired), or that carries a corrupt frame, fails the dataflow
//! on every node that still needs it — never `Ok` with missing data, never a
//! hang — while healthy runs never fail spuriously.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

#[path = "support/faults.rs"]
mod faults;

use std::sync::Arc;
use std::time::{Duration, Instant};

use faults::{FaultMesh, FrameKind};
use instancy::communication::ClusterSpawnTransport;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    CancellationReason, ClusterTopology, DataflowBuilder, DataflowId, Error, NodeConfig, Result,
    RuntimeHandle, SpawnOptions,
};
use support::{Managers, drop_runtimes, fmt_join, join_with_timeout, runtime};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    Shared,
}

const MODES: [Mode; 2] = [Mode::Dedicated, Mode::Shared];

/// Generous bound on failure detection (shared managers retry a lost
/// connection for about 3 s) on a loaded machine.
const DETECT: Duration = Duration::from_secs(20);

const NAMES: [&str; 4] = ["a", "b", "c", "d"];

/// Every node sums what it receives: `input -> map -> exchange -> map -> output`.
fn exchange(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("pre", |_t, x| x)
        .exchange_by_hash("route", |x: &u64| *x)
        .map("post", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// No exchange: each node's workers finish on their own.
fn pipeline(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// Fails the worker whose own input contains the record `u64::MAX` (before
/// the exchange, so two nodes can fail independently).
fn failing_exchange(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .unary_notify("maybe_fail", |input, output, _ctx| {
            while let Some((t, data)) = input.next() {
                if data.contains(&u64::MAX) {
                    return Err(Error::Runtime(instancy::RuntimeError::InvalidConfig(
                        "deliberate operator failure".into(),
                    )));
                }
                output.push_vec(t, data);
            }
            Ok(())
        })
        .exchange_by_hash("route", |x: &u64| *x)
        .map("post", |_t, x| x)
        .output("out")?;
    Ok(())
}

struct Node {
    name: String,
    rt: Option<RuntimeHandle>,
    df: Option<ClusterSpawnedDataflow<u64>>,
    input: Option<instancy::InputSender<u64, u64>>,
    output: Option<support::Collector<u64>>,
}

struct Cluster {
    mesh: FaultMesh,
    nodes: Vec<Node>,
    managers: Vec<Managers>,
}

impl Cluster {
    async fn start(
        mode: Mode,
        n: usize,
        build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
    ) -> Cluster {
        let names = &NAMES[..n];
        let mut mesh = FaultMesh::new(names, n, mode == Mode::Shared);
        let topo =
            ClusterTopology::multi_node(names.iter().map(|n| NodeConfig::new(*n, 1)).collect())
                .unwrap();
        let id = DataflowId::new();
        let mut managers = Vec::new();
        let handles: Vec<_> = names
            .iter()
            .map(|name| match mode {
                Mode::Dedicated => support::spawn_node(
                    topo.clone(),
                    name,
                    id,
                    mesh.dedicated(name, 1024),
                    2,
                    build,
                ),
                Mode::Shared => {
                    let m = mesh.managers(name);
                    managers.push(Arc::clone(&m));
                    support::spawn_node(
                        topo.clone(),
                        name,
                        id,
                        ClusterSpawnTransport::shared(m, 1024),
                        2,
                        build,
                    )
                }
            })
            .collect();
        let mut nodes = Vec::new();
        for (name, handle) in names.iter().zip(handles) {
            let (rt, df) = handle.await.unwrap();
            let mut df = df.unwrap_or_else(|e| panic!("{mode:?}: {name} failed to spawn: {e}"));
            let input = df.take_input::<u64>(0, "data").unwrap();
            let output = support::collect(df.take_output::<u64>(0, "out").unwrap());
            nodes.push(Node {
                name: name.to_string(),
                rt: Some(rt),
                df: Some(df),
                input: Some(input),
                output: Some(output),
            });
        }
        Cluster {
            mesh,
            nodes,
            managers,
        }
    }

    fn node(&mut self, name: &str) -> &mut Node {
        self.nodes.iter_mut().find(|n| n.name == name).unwrap()
    }

    fn send(&mut self, name: &str, records: impl Iterator<Item = u64>) {
        let input = self.node(name).input.as_ref().unwrap();
        input.send(0, records.collect()).unwrap();
    }

    /// Like [`send`](Self::send), for tests where the dataflow may already
    /// have failed (and closed the input) because of an earlier send.
    fn send_if_open(&mut self, name: &str, records: impl Iterator<Item = u64>) {
        let input = self.node(name).input.as_ref().unwrap();
        let _ = input.send(0, records.collect());
    }

    fn close(&mut self, name: &str) {
        self.node(name).input.take();
    }

    fn close_all(&mut self) {
        for node in &mut self.nodes {
            node.input.take();
        }
    }

    fn join(
        &mut self,
        name: &str,
        timeout: Duration,
    ) -> tokio::task::JoinHandle<Option<Result<()>>> {
        join_with_timeout(self.node(name).df.take().unwrap(), timeout)
    }

    async fn join_all(&mut self, timeout: Duration) -> Vec<(String, Option<Result<()>>)> {
        let joins: Vec<_> = self
            .nodes
            .iter_mut()
            .filter_map(|n| {
                n.df.take()
                    .map(|df| (n.name.clone(), join_with_timeout(df, timeout)))
            })
            .collect();
        let mut results = Vec::new();
        for (name, join) in joins {
            results.push((name, join.await.unwrap()));
        }
        results
    }

    /// Sum of every node's output (waits for each output to close).
    fn total(&mut self) -> std::result::Result<u64, String> {
        let mut total = 0;
        for node in &mut self.nodes {
            if let Some(out) = node.output.take() {
                total += out.join()?.into_iter().map(|(_, x)| x).sum::<u64>();
            }
        }
        Ok(total)
    }

    async fn shutdown(mut self) {
        self.mesh.cut_all();
        let mut rts = Vec::new();
        for node in &mut self.nodes {
            node.input.take();
            node.df.take();
            node.output.take();
            if let Some(rt) = node.rt.take() {
                rts.push(rt);
            }
        }
        drop_runtimes(rts).await;
        self.managers.clear();
    }
}

fn describe(results: &[(String, Option<Result<()>>)]) -> String {
    results
        .iter()
        .map(|(n, r)| format!("{n}={}", fmt_join(r)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn all_err(results: &[(String, Option<Result<()>>)]) -> bool {
    results.iter().all(|(_, r)| matches!(r, Some(Err(_))))
}

fn reason(result: &Option<Result<()>>) -> Option<&CancellationReason> {
    match result {
        Some(Err(Error::Cancelled { reason })) => reason.as_ref(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Healthy runs: no spurious failures
// ---------------------------------------------------------------------------

/// Clean runs with 2-4 nodes in both modes: every node succeeds and every
/// record arrives exactly once. Guards against the failure rules firing on a
/// clean shutdown (EOF after retirement, data channels closed by Drop, ...).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn healthy_soak_never_fails() {
    for round in 0..4 {
        for mode in MODES {
            for n in 2..=4 {
                let build = if round % 2 == 0 { exchange } else { pipeline };
                let mut cluster = Cluster::start(mode, n, build).await;
                let mut expected = 0;
                for (i, name) in NAMES[..n].iter().enumerate() {
                    let records = (i as u64 * 1000)..(i as u64 * 1000 + 200);
                    expected += records.clone().sum::<u64>();
                    cluster.send(name, records);
                }
                cluster.close_all();
                let results = cluster.join_all(DETECT).await;
                assert!(
                    results.iter().all(|(_, r)| matches!(r, Some(Ok(())))),
                    "{mode:?} n={n} round {round}: {}",
                    describe(&results)
                );
                assert_eq!(
                    cluster.total(),
                    Ok(expected),
                    "{mode:?} n={n} round {round}"
                );
                cluster.shutdown().await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Link cut at each phase
// ---------------------------------------------------------------------------

/// Running: every node's input is open, one link is cut. The two endpoints
/// detect the loss; every other node is cancelled by them. All fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_while_running_fails_every_node() {
    for mode in MODES {
        for n in 2..=4 {
            let mut cluster = Cluster::start(mode, n, exchange).await;
            for (i, name) in NAMES[..n].iter().enumerate() {
                cluster.send(name, (i as u64 * 100)..(i as u64 * 100 + 50));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            cluster.mesh.cut("a", "b");
            let results = cluster.join_all(DETECT).await;
            assert!(all_err(&results), "{mode:?} n={n}: {}", describe(&results));
            cluster.shutdown().await;
        }
    }
}

type SpawnJoin = tokio::task::JoinHandle<(RuntimeHandle, Result<ClusterSpawnedDataflow<u64>>)>;

/// Spawns every node on its own blocking thread with its own instancy
/// runtime (spawn blocks until the ready barrier), as a deployment would.
fn spawn_blocking_all(
    mode: Mode,
    mesh: &mut FaultMesh,
    names: &[&str],
    handshake_timeout: Duration,
    managers: &mut Vec<Managers>,
) -> Vec<SpawnJoin> {
    let topo = ClusterTopology::multi_node(names.iter().map(|n| NodeConfig::new(*n, 1)).collect())
        .unwrap();
    let id = DataflowId::new();
    names
        .iter()
        .map(|name| {
            let handle = tokio::runtime::Handle::current();
            let topo = topo.clone();
            let name = name.to_string();
            match mode {
                Mode::Dedicated => {
                    let transport = mesh.dedicated(&name, 1024);
                    tokio::task::spawn_blocking(move || {
                        let rt = runtime(2);
                        let r = rt.spawn_cluster(
                            "adversarial",
                            topo,
                            &name,
                            id,
                            transport,
                            handshake_timeout,
                            exchange,
                            &handle,
                            SpawnOptions::new(),
                        );
                        (rt, r)
                    })
                }
                Mode::Shared => {
                    let m = mesh.managers(&name);
                    managers.push(Arc::clone(&m));
                    tokio::task::spawn_blocking(move || {
                        let rt = runtime(2);
                        let r = rt.spawn_cluster(
                            "adversarial",
                            topo,
                            &name,
                            id,
                            ClusterSpawnTransport::shared(m, 1024),
                            handshake_timeout,
                            exchange,
                            &handle,
                            SpawnOptions::new(),
                        );
                        (rt, r)
                    })
                }
            }
        })
        .collect()
}

/// Awaits every spawn: each must fail, or (with its input left open) its
/// dataflow must fail within [`DETECT`]. No node may run to success without
/// its peers.
async fn assert_no_node_runs(
    label: &str,
    names: &[&str],
    spawns: Vec<SpawnJoin>,
) -> Vec<RuntimeHandle> {
    let mut rts = Vec::new();
    for (name, spawn) in names.iter().zip(spawns) {
        let (rt, r) = spawn.await.unwrap();
        if let Ok(mut df) = r {
            let _input = df.take_input::<u64>(0, "data").unwrap();
            let _out = df.take_output::<u64>(0, "out").unwrap();
            let j = join_with_timeout(df, DETECT).await.unwrap();
            assert!(
                matches!(j, Some(Err(_))),
                "{label}: {name} ran without its peers: {}",
                fmt_join(&j)
            );
        }
        rts.push(rt);
    }
    rts
}

/// Handshake: a link is cut before the nodes spawn. No node may run: spawn
/// fails everywhere (or the dataflow fails), within the handshake timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_before_handshake_fails_every_node() {
    for mode in MODES {
        for n in 2..=3 {
            let names = &NAMES[..n];
            let mut mesh = FaultMesh::new(names, n, mode == Mode::Shared);
            mesh.cut("a", "b");
            let mut managers = Vec::new();
            let start = Instant::now();
            let spawns = spawn_blocking_all(
                mode,
                &mut mesh,
                names,
                Duration::from_secs(2),
                &mut managers,
            );
            let rts = assert_no_node_runs(&format!("{mode:?} n={n}"), names, spawns).await;
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "{mode:?} n={n}: took {:?}",
                start.elapsed()
            );
            mesh.cut_all();
            drop_runtimes(rts).await;
            drop(managers);
        }
    }
}

/// Ready barrier: c's handshake reaches a, then c's `Ready` to a is held back
/// and the a-c link is cut. a fails its barrier and tells b; b and c may pass
/// theirs, but must then fail (a's spawn-failure `Cancel`, the lost link).
/// Nothing runs to success without a, and nothing waits for a timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_during_ready_barrier_fails_every_node() {
    for mode in MODES {
        let names = &NAMES[..3];
        let mut mesh = FaultMesh::new(names, 3, mode == Mode::Shared);
        // Control frames from c to a: 1 = Handshake, 2 = Ready.
        let c_to_a = mesh.ctl("c", "a");
        c_to_a.hold_from(FrameKind::Control, 2);
        let mut managers = Vec::new();
        let start = Instant::now();
        let spawns = spawn_blocking_all(
            mode,
            &mut mesh,
            names,
            Duration::from_secs(30),
            &mut managers,
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while !c_to_a.held() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(c_to_a.held(), "{mode:?}: precondition: c's Ready to a held");
        tokio::time::sleep(Duration::from_millis(100)).await;
        mesh.cut("a", "c");
        let rts = assert_no_node_runs(&format!("{mode:?}"), names, spawns).await;
        // Well below the 30 s handshake timeout: detected, not timed out.
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "{mode:?}: took {:?}",
            start.elapsed()
        );
        mesh.cut_all();
        drop_runtimes(rts).await;
        drop(managers);
    }
}

/// Draining: node a's workers finished while b still runs (b waits for a's
/// last releases, held with a's goodbye behind the paused a → b link); the
/// link is then cut. a keeps its (complete) local result; b, which never got
/// a's goodbye, must fail rather than hang or succeed.
///
/// On a cluster every node hosts every operator, so a node finishes only once
/// every node's input has closed (R47). Old trigger: a's input closed while
/// b's stayed open. Now b's input closes too, and a's link to b is paused
/// before a's input closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_while_draining_keeps_finished_result_and_fails_the_rest() {
    for mode in MODES {
        let mut cluster = Cluster::start(mode, 2, pipeline).await;
        let a_to_b = cluster.mesh.ctl("a", "b");
        a_to_b.pause();
        cluster.send("b", 100..110);
        cluster.close("b");
        cluster.send("a", 0..10);
        cluster.close("a");
        let ja = cluster.join("a", DETECT);
        let jb = cluster.join("b", DETECT);
        let a_out = cluster.node("a").output.as_mut().unwrap();
        assert!(
            a_out.wait_closed(DETECT),
            "{mode:?}: a's workers did not finish"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(a_to_b.holding(), "{mode:?}: the paused link held nothing");
        assert!(
            !jb.is_finished(),
            "{mode:?}: b finished without a's releases"
        );
        cluster.mesh.cut("a", "b");
        let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
        assert!(matches!(ra, Some(Ok(()))), "{mode:?}: a {}", fmt_join(&ra));
        assert!(matches!(rb, Some(Err(_))), "{mode:?}: b {}", fmt_join(&rb));
        cluster.shutdown().await;
    }
}

/// After retirement: node a finished and every peer retired it; its links
/// then die. The others are unaffected and finish with every record.
///
/// R47: a node finishes only once every node's input has closed. Old
/// trigger: a finished while b's and c's inputs stayed open. Now every input
/// closes while b and c cannot hear each other (b ↔ c paused): a gets every
/// release and retires, while b and c each wait for the other's.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_after_retirement_is_benign() {
    for mode in MODES {
        let mut cluster = Cluster::start(mode, 3, pipeline).await;
        let (b_to_c, c_to_b) = (cluster.mesh.ctl("b", "c"), cluster.mesh.ctl("c", "b"));
        b_to_c.pause();
        c_to_b.pause();
        let jb = cluster.join("b", DETECT);
        let jc = cluster.join("c", DETECT);
        cluster.send("a", 0..30);
        cluster.send("b", 100..110);
        cluster.send("c", 200..210);
        cluster.close_all();
        let ra = cluster.join("a", DETECT).await.unwrap();
        assert!(matches!(ra, Some(Ok(()))), "{mode:?}: a {}", fmt_join(&ra));
        assert!(
            b_to_c.holding() && c_to_b.holding(),
            "{mode:?}: the paused links held nothing"
        );
        assert!(
            !jb.is_finished() && !jc.is_finished(),
            "{mode:?}: b and c must still be running"
        );
        cluster.mesh.cut("a", "b");
        cluster.mesh.cut("a", "c");
        tokio::time::sleep(Duration::from_millis(300)).await;
        b_to_c.resume();
        c_to_b.resume();
        let (rb, rc) = (jb.await.unwrap(), jc.await.unwrap());
        assert!(
            matches!(rb, Some(Ok(()))) && matches!(rc, Some(Ok(()))),
            "{mode:?}: b={} c={}",
            fmt_join(&rb),
            fmt_join(&rc)
        );
        let expected = (0..30).sum::<u64>() + (100..110).sum::<u64>() + (200..210).sum::<u64>();
        assert_eq!(cluster.total(), Ok(expected), "{mode:?}");
        cluster.shutdown().await;
    }
}
// ---------------------------------------------------------------------------
// Corruption
// ---------------------------------------------------------------------------

/// A corrupt data, progress or control frame on one link fails every node of
/// a 3-node cluster; the receiving node reports a network error.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn corrupt_frame_of_each_kind_fails_every_node() {
    for mode in MODES {
        for kind in [FrameKind::Data, FrameKind::Progress, FrameKind::Control] {
            let mut cluster = Cluster::start(mode, 3, exchange).await;
            // Control frames are rare while running: corrupt c's Cancel to b.
            let (from, to) = if kind == FrameKind::Control {
                ("c", "b")
            } else {
                ("a", "b")
            };
            let ctl = cluster.mesh.ctl(from, to);
            ctl.corrupt_next(kind);
            // The corrupt frame may already fail the dataflow (and close the
            // other inputs) while the later nodes are being fed.
            for (i, name) in ["a", "b", "c"].iter().enumerate() {
                cluster.send_if_open(name, (i as u64 * 100)..(i as u64 * 100 + 50));
            }
            if kind == FrameKind::Progress {
                // Progress frames follow every capability change.
                let input = cluster.node("a").input.as_ref().unwrap();
                let _ = input.advance_to(5);
            }
            if kind == FrameKind::Control {
                tokio::time::sleep(Duration::from_millis(100)).await;
                cluster.node("c").df.as_ref().unwrap().cancel();
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ctl.corrupted() && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(ctl.corrupted(), "{mode:?} {kind:?}: precondition");
            let results = cluster.join_all(DETECT).await;
            assert!(
                all_err(&results),
                "{mode:?} {kind:?}: {}",
                describe(&results)
            );
            let b = &results.iter().find(|(n, _)| n == "b").unwrap().1;
            // A corrupt data or progress frame is the first failure anywhere,
            // so b reports it. A corrupt control frame is c's Cancel to b,
            // which a may relay to b first: either reason is a failure.
            let detected = match b {
                Some(Err(Error::Communication(instancy::CommunicationError::DataLoss {
                    peer_id,
                    ..
                }))) => peer_id == from,
                _ => {
                    matches!(reason(b), Some(CancellationReason::PeerCancelled { .. }))
                        && kind == FrameKind::Control
                }
            };
            assert!(
                detected,
                "{mode:?} {kind:?}: b detected the corruption: {}",
                fmt_join(b)
            );
            cluster.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Simultaneous failures
// ---------------------------------------------------------------------------

/// Every link of a 4-node cluster dies at once: no node can tell anyone;
/// all must still fail (no hang).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn all_links_cut_at_once_fails_every_node() {
    for mode in MODES {
        let mut cluster = Cluster::start(mode, 4, exchange).await;
        for (i, name) in NAMES.iter().enumerate() {
            cluster.send(name, (i as u64 * 100)..(i as u64 * 100 + 50));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        cluster.mesh.cut_all();
        let results = cluster.join_all(DETECT).await;
        assert!(all_err(&results), "{mode:?}: {}", describe(&results));
        cluster.shutdown().await;
    }
}

/// Two nodes' workers fail at the same time (crossing Cancels) while two
/// links are also cut: every node fails, nothing hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn simultaneous_worker_failures_and_cuts_fail_every_node() {
    for mode in MODES {
        let mut cluster = Cluster::start(mode, 4, failing_exchange).await;
        for (i, name) in NAMES.iter().enumerate() {
            cluster.send(name, (i as u64 * 100)..(i as u64 * 100 + 50));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        // a's failure may cancel d before d's own failing record is sent.
        cluster.send("a", std::iter::once(u64::MAX));
        cluster.send_if_open("d", std::iter::once(u64::MAX));
        cluster.mesh.cut("b", "c");
        cluster.mesh.cut("a", "c");
        let results = cluster.join_all(DETECT).await;
        assert!(all_err(&results), "{mode:?}: {}", describe(&results));
        cluster.shutdown().await;
    }
}

/// Node b has closed its input; then a's worker fails. a's exchange outputs
/// are dropped with the failed worker and must not close cleanly: otherwise
/// b sees every input exhausted and finishes `Ok` without a's data, racing
/// a's `Cancel`. Repeated so the race gets a chance.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failed_worker_never_lets_a_peer_with_closed_input_succeed() {
    for mode in MODES {
        for round in 0..10 {
            let mut cluster = Cluster::start(mode, 2, failing_exchange).await;
            cluster.send("b", 0..50);
            cluster.close("b");
            let jb = cluster.join("b", DETECT);
            cluster.send("a", 100..150);
            cluster.send("a", std::iter::once(u64::MAX));
            let ra = cluster.join("a", DETECT).await.unwrap();
            let rb = jb.await.unwrap();
            assert!(
                matches!(ra, Some(Err(_))) && matches!(rb, Some(Err(_))),
                "{mode:?} round {round}: a={} b={}",
                fmt_join(&ra),
                fmt_join(&rb)
            );
            cluster.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime drop on one node
// ---------------------------------------------------------------------------

/// Node b's instancy runtime is dropped mid-run. b broadcasts its
/// cancellation; every other node fails promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn runtime_drop_on_one_node_fails_the_others() {
    for mode in MODES {
        let mut cluster = Cluster::start(mode, 3, exchange).await;
        for (i, name) in ["a", "b", "c"].iter().enumerate() {
            cluster.send(name, (i as u64 * 100)..(i as u64 * 100 + 50));
        }
        let ja = cluster.join("a", DETECT);
        let jc = cluster.join("c", DETECT);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rt_b = cluster.node("b").rt.take().unwrap();
        drop_runtimes(vec![rt_b]).await;
        let (ra, rc) = (ja.await.unwrap(), jc.await.unwrap());
        for (name, r) in [("a", &ra), ("c", &rc)] {
            assert!(
                matches!(r, Some(Err(_))),
                "{mode:?}: {name} must fail when b's runtime is dropped: {}",
                fmt_join(r)
            );
        }
        cluster.shutdown().await;
    }
}

/// The tokio runtime that hosts node b's transport is dropped mid-run: b
/// vanishes without a word. The others detect the loss and fail.
#[test]
fn tokio_runtime_drop_on_one_node_fails_the_others() {
    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
    let b_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let names = ["a", "b", "c"];
    let mut mesh = main_rt.block_on(async { FaultMesh::new(&names, 3, false) });
    let topo = ClusterTopology::multi_node(names.iter().map(|n| NodeConfig::new(*n, 1)).collect())
        .unwrap();
    let id = DataflowId::new();
    let mut threads = Vec::new();
    for name in names {
        let transport = mesh.dedicated(name, 1024);
        let handle = if name == "b" {
            b_rt.handle().clone()
        } else {
            main_rt.handle().clone()
        };
        let topo = topo.clone();
        threads.push(std::thread::spawn(move || {
            let rt = runtime(2);
            let df = rt
                .spawn_cluster(
                    "adversarial",
                    topo,
                    name,
                    id,
                    transport,
                    Duration::from_secs(10),
                    exchange,
                    &handle,
                    SpawnOptions::new(),
                )
                .unwrap();
            (rt, df)
        }));
    }
    let mut nodes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for (i, (_, df)) in nodes.iter_mut().enumerate() {
        let input = df.take_input::<u64>(0, "data").unwrap();
        input
            .send(0, ((i as u64 * 100)..(i as u64 * 100 + 50)).collect())
            .unwrap();
        inputs.push(input);
        outputs.push(df.take_output::<u64>(0, "out").unwrap());
    }
    std::thread::sleep(Duration::from_millis(200));
    b_rt.shutdown_timeout(Duration::from_secs(1));
    let (rt_a, df_a) = nodes.remove(0);
    let (rt_b, df_b) = nodes.remove(0);
    let (rt_c, df_c) = nodes.remove(0);
    let results = main_rt.block_on(async {
        let ja = join_with_timeout(df_a, DETECT);
        let jc = join_with_timeout(df_c, DETECT);
        (ja.await.unwrap(), jc.await.unwrap())
    });
    assert!(
        matches!(results.0, Some(Err(_))) && matches!(results.1, Some(Err(_))),
        "a={} c={}",
        fmt_join(&results.0),
        fmt_join(&results.1)
    );
    drop(inputs);
    drop(outputs);
    drop(df_b);
    main_rt.block_on(async {
        mesh.cut_all();
        drop_runtimes(vec![rt_a, rt_b, rt_c]).await;
    });
}
