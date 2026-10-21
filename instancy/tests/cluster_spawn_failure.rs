//! A node whose `spawn_cluster` fails tells its peers, so they fail too
//! instead of waiting for their handshake timeout or running (and reporting
//! success) without it. Async sources start only after the ready barrier, so
//! a failed spawn never runs user producers (audit H5).

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

#[path = "support/faults.rs"]
mod faults;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use faults::FaultMesh;
use instancy::communication::ClusterSpawnTransport;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, Error, HandshakeError, NodeConfig, Result,
    RuntimeError, RuntimeHandle, SpawnOptions,
};
use support::{Managers, drop_runtimes, fmt_join, join_with_timeout, runtime};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    Shared,
}

const MODES: [Mode; 2] = [Mode::Dedicated, Mode::Shared];

fn topology(nodes: &[&str]) -> ClusterTopology {
    ClusterTopology::multi_node(nodes.iter().map(|n| NodeConfig::new(*n, 1)).collect()).unwrap()
}

/// `Clone` that blocks: the `source` operator clones its data when it is
/// materialized (Phase 7), which makes that node slow to reach the barrier.
#[derive(Debug)]
struct SlowClone {
    v: u64,
    delay_ms: u64,
}

impl Clone for SlowClone {
    fn clone(&self) -> Self {
        std::thread::sleep(Duration::from_millis(self.delay_ms));
        SlowClone {
            v: self.v,
            delay_ms: self.delay_ms,
        }
    }
}

fn slow_build(delay_ms: u64) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        b.source("src", vec![(0u64, vec![SlowClone { v: 7, delay_ms }])])
            .map("unwrap", |_t, s: SlowClone| s.v)
            .filter("none", |_t, _x: &u64| false)
            .exchange_by_hash("x", |x: &u64| *x)
            .output("out")?;
        Ok(())
    }
}

/// An async source that records whether its producer ran, next to a slow
/// source.
fn producer_build(
    delay_ms: u64,
    produced: Arc<AtomicUsize>,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        let produced = Arc::clone(&produced);
        b.source_async::<u64, _, _>("producer", move |sender| async move {
            // A side effect visible outside the dataflow (e.g. consuming a queue).
            produced.fetch_add(1, Ordering::SeqCst);
            sender.send(0, vec![1]).await?;
            Ok(())
        })
        .output("o1")?;
        b.source("slow", vec![(0u64, vec![SlowClone { v: 7, delay_ms }])])
            .map("unwrap", |_t, s: SlowClone| s.v)
            .exchange_by_hash("x", |x: &u64| *x)
            .output("o2")?;
        Ok(())
    }
}

type Spawned = (RuntimeHandle, Result<ClusterSpawnedDataflow<u64>>, Duration);

/// Spawn `node` with its own handshake timeout; also returns how long
/// `spawn_cluster` took.
#[allow(clippy::too_many_arguments)]
fn spawn<F>(
    mesh: &mut FaultMesh,
    mode: Mode,
    managers: &mut Vec<Managers>,
    topology: ClusterTopology,
    node: &str,
    id: DataflowId,
    handshake_timeout: Duration,
    build: F,
) -> tokio::task::JoinHandle<Spawned>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    let node = node.to_string();
    match mode {
        Mode::Dedicated => {
            let transport = mesh.dedicated(&node, 1024);
            tokio::task::spawn_blocking(move || {
                let rt = runtime(2);
                let start = Instant::now();
                let r = rt.spawn_cluster(
                    "spawn-failure",
                    topology,
                    &node,
                    id,
                    transport,
                    handshake_timeout,
                    build,
                    &handle,
                    SpawnOptions::new(),
                );
                (rt, r, start.elapsed())
            })
        }
        Mode::Shared => {
            // Managers outlive the dataflow, as in a real deployment.
            let m = mesh.managers(&node);
            managers.push(Arc::clone(&m));
            tokio::task::spawn_blocking(move || {
                let rt = runtime(2);
                let start = Instant::now();
                let r = rt.spawn_cluster(
                    "spawn-failure",
                    topology,
                    &node,
                    id,
                    ClusterSpawnTransport::shared(m, 1024),
                    handshake_timeout,
                    build,
                    &handle,
                    SpawnOptions::new(),
                );
                (rt, r, start.elapsed())
            })
        }
    }
}

fn spawn_error(result: &Result<ClusterSpawnedDataflow<u64>>) -> Option<&HandshakeError> {
    match result {
        Err(Error::Runtime(RuntimeError::Handshake(e))) => Some(e),
        _ => None,
    }
}

/// node-b fails its ready barrier (short timeout) after sending its own
/// `Ready`; node-a, slow to materialize, receives that `Ready`. node-a must
/// not run without node-b: it fails to spawn, or its dataflow fails.
/// Previously node-a spawned and then reported success.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ready_barrier_failure_fails_peer_that_passed_barrier() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut managers = Vec::new();
        let id = DataflowId::new();
        let topo = topology(&names);
        let ha = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo.clone(),
            "node-a",
            id,
            Duration::from_secs(10),
            slow_build(1500),
        );
        let hb = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo,
            "node-b",
            id,
            Duration::from_millis(300),
            slow_build(0),
        );
        let ((rt_a, ra, _), (rt_b, rb, _)) = (ha.await.unwrap(), hb.await.unwrap());
        assert!(
            rb.is_err(),
            "{mode:?}: precondition: node-b fails its barrier"
        );
        let outcome = match ra {
            Err(e) => format!("spawn failed: {e}"),
            Ok(mut ca) => {
                let _oa = ca.take_output::<u64>(0, "out").unwrap();
                let ja = join_with_timeout(ca, Duration::from_secs(10))
                    .await
                    .unwrap();
                assert!(
                    matches!(ja, Some(Err(_))),
                    "{mode:?}: node-b never ran, so node-a must fail: {}",
                    fmt_join(&ja)
                );
                fmt_join(&ja)
            }
        };
        eprintln!("{mode:?}: node-a {outcome}");
        drop_runtimes(vec![rt_a, rt_b]).await;
        drop(managers);
    }
}

/// node-a's ready barrier times out (node-b is slow to materialize). node-a's
/// user async-source producer must never run: `spawn_cluster` failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn async_source_producer_does_not_run_when_barrier_fails() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut managers = Vec::new();
        let id = DataflowId::new();
        let topo = topology(&names);
        let produced_a = Arc::new(AtomicUsize::new(0));
        let produced_b = Arc::new(AtomicUsize::new(0));
        let ha = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo.clone(),
            "node-a",
            id,
            Duration::from_millis(300),
            producer_build(0, Arc::clone(&produced_a)),
        );
        let hb = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo,
            "node-b",
            id,
            Duration::from_secs(10),
            producer_build(1500, Arc::clone(&produced_b)),
        );
        let ((rt_a, ra, _), (rt_b, rb, _)) = (ha.await.unwrap(), hb.await.unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            ra.is_err(),
            "{mode:?}: precondition: node-a's barrier times out"
        );
        assert_eq!(
            produced_a.load(Ordering::SeqCst),
            0,
            "{mode:?}: node-a's spawn failed, so its producer must not have run"
        );
        if let Ok(mut cb) = rb {
            let _o1 = cb.take_output::<u64>(0, "o1").unwrap();
            let _o2 = cb.take_output::<u64>(0, "o2").unwrap();
            let jb = join_with_timeout(cb, Duration::from_secs(10))
                .await
                .unwrap();
            assert!(
                matches!(jb, Some(Err(_))),
                "{mode:?}: node-a never ran, so node-b must fail: {}",
                fmt_join(&jb)
            );
        }
        drop_runtimes(vec![rt_a, rt_b]).await;
        drop(managers);
    }
}

/// node-a fails before its handshake (its dataflow does not build). node-b,
/// with a long handshake timeout, must fail promptly: a dropped dedicated
/// connection, or the `Cancel` node-a sends through its shared managers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failed_build_on_one_node_fails_peers_promptly() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut managers = Vec::new();
        let id = DataflowId::new();
        let topo = topology(&names);
        let ha = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo.clone(),
            "node-a",
            id,
            Duration::from_secs(10),
            |_b: &mut DataflowBuilder<u64>| -> Result<()> {
                Err(Error::Runtime(RuntimeError::InvalidConfig(
                    "deliberate build failure".into(),
                )))
            },
        );
        let hb = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo,
            "node-b",
            id,
            Duration::from_secs(10),
            slow_build(0),
        );
        let ((rt_a, ra, _), (rt_b, rb, b_took)) = (ha.await.unwrap(), hb.await.unwrap());
        assert!(ra.is_err(), "{mode:?}: precondition");
        assert!(
            matches!(
                spawn_error(&rb),
                Some(
                    HandshakeError::PeerCancelled { .. } | HandshakeError::PeerDisconnected { .. }
                )
            ),
            "{mode:?}: node-b must learn that node-a failed: {:?}",
            rb.as_ref().map(|_| ())
        );
        assert!(
            b_took < Duration::from_secs(5),
            "{mode:?}: node-b waited {b_took:?} (handshake timeout is 10 s)"
        );
        drop_runtimes(vec![rt_a, rt_b]).await;
        drop(managers);
    }
}

/// Three nodes. node-c is slow to materialize. node-a (short timeout) sends
/// `Ready`, gives up waiting for node-c and cancels. node-b, which already
/// has node-a's `Ready` and is still waiting for node-c, must fail at once
/// on node-a's `Cancel` instead of waiting out its own 10 s timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn barrier_failure_after_ready_fails_waiting_peers_promptly() {
    for mode in MODES {
        let names = ["node-a", "node-b", "node-c"];
        let mut mesh = FaultMesh::new(&names, 3, mode == Mode::Shared);
        let mut managers = Vec::new();
        let id = DataflowId::new();
        let topo = topology(&names);
        let ha = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo.clone(),
            "node-a",
            id,
            Duration::from_millis(500),
            slow_build(0),
        );
        let hb = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo.clone(),
            "node-b",
            id,
            Duration::from_secs(10),
            slow_build(0),
        );
        let hc = spawn(
            &mut mesh,
            mode,
            &mut managers,
            topo,
            "node-c",
            id,
            Duration::from_secs(10),
            slow_build(3000),
        );
        let (rt_a, ra, _) = ha.await.unwrap();
        let (rt_b, rb, b_took) = hb.await.unwrap();
        let (rt_c, rc, _) = hc.await.unwrap();
        assert!(ra.is_err(), "{mode:?}: precondition: node-a times out");
        assert!(
            matches!(
                spawn_error(&rb),
                Some(HandshakeError::PeerCancelled { peer_id, .. }) if peer_id == "node-a"
            ),
            "{mode:?}: node-b must fail on node-a's Cancel: {:?}",
            rb.as_ref().map(|_| ())
        );
        assert!(
            b_took < Duration::from_millis(2500),
            "{mode:?}: node-b waited {b_took:?} for a peer that had already failed"
        );
        if let Ok(mut cc) = rc {
            let _oc = cc.take_output::<u64>(0, "out").unwrap();
            let jc = join_with_timeout(cc, Duration::from_secs(10))
                .await
                .unwrap();
            assert!(
                matches!(jc, Some(Err(_))),
                "{mode:?}: node-c must fail: {}",
                fmt_join(&jc)
            );
        }
        drop_runtimes(vec![rt_a, rt_b, rt_c]).await;
        drop(managers);
    }
}
