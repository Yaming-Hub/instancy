//! A cluster-wide requested stop (`drain_on_cancel`) that can no longer
//! complete must end on every node at once instead of each draining peer
//! waiting for its drain deadline:
//!
//! - a node whose worker fails while it drains forwards the failure, which
//!   stops every peer's drain;
//! - a node that stops without finishing its part (it does not drain, or its
//!   runtime is dropped) says so, which stops the drains that need its data;
//! - a peer that cancelled and then disconnected before its goodbye stops
//!   the drains that need its data.
//!
//! Control: a drain that can complete still succeeds with every record (and
//! PR7's `peer_cancel_followed_by_local_drain_success_does_not_hang`: a
//! drain that needs nothing from a stopped peer still succeeds).

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

#[path = "support/faults.rs"]
mod faults;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use faults::FaultMesh;
use instancy::communication::ClusterSpawnTransport;
use instancy::dataflow::channel_operators::InputSender;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    CancellationReason, ClusterTopology, DataflowBuilder, DataflowId, Error, NodeConfig, Result,
    RuntimeHandle, SpawnOptions,
};
use support::{
    Managers, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node_with_options,
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    Shared,
}

const MODES: [Mode; 2] = [Mode::Dedicated, Mode::Shared];

/// Every node drains this long on cancel; the tests require far less.
const DRAIN: Duration = Duration::from_secs(60);
/// Bound on stopping a hopeless drain (shared managers retry a lost
/// connection for about 3 s before reporting it).
const PROMPT: Duration = Duration::from_secs(20);
/// Batches node-a sends; each takes 20 ms in its slow operator.
const BATCHES: u64 = 100;

/// input -> slow (fails once `fail` is set) -> exchange (all to node-b) ->
/// output: node-b's drain needs node-a's close sentinel.
fn pipeline(builder: &mut DataflowBuilder<u64>, fail: &Arc<AtomicBool>) -> Result<()> {
    let fail = Arc::clone(fail);
    builder
        .input::<u64>("data")?
        .unary("slow", move |input, output| {
            // One batch per activation, so the executor observes a runtime
            // shutdown between batches.
            if let Some((time, data)) = input.next() {
                if fail.load(Ordering::SeqCst) {
                    return Err(Error::operator(
                        "slow",
                        std::io::Error::other("deliberate failure during the drain"),
                    ));
                }
                std::thread::sleep(Duration::from_millis(20));
                output.push_vec(time, data);
            }
            Ok(())
        })
        .exchange_by_hash("to_b", |_x: &u64| 1)
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// input -> exchange (all to node-a) -> slow -> output: node-a's drain needs
/// nothing from node-b once node-b's input is closed (node-b then closes its
/// exchange output at once), and node-a's own pushes to node-b close as soon
/// as its input does.
fn local_pipeline(builder: &mut DataflowBuilder<u64>, _fail: &Arc<AtomicBool>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .exchange_by_hash("to_a", |_x: &u64| 0)
        .unary("slow", move |input, output| {
            if let Some((time, data)) = input.next() {
                std::thread::sleep(Duration::from_millis(20));
                output.push_vec(time, data);
            }
            Ok(())
        })
        .output("out")?;
    Ok(())
}

/// input -> slow -> exchange (all to node-a) -> output: like
/// `local_pipeline`, but node-a's push to node-b stays open for the whole
/// drain, although it never carries a record.
fn late_exchange_pipeline(
    builder: &mut DataflowBuilder<u64>,
    _fail: &Arc<AtomicBool>,
) -> Result<()> {
    builder
        .input::<u64>("data")?
        .unary("slow", move |input, output| {
            if let Some((time, data)) = input.next() {
                std::thread::sleep(Duration::from_millis(20));
                output.push_vec(time, data);
            }
            Ok(())
        })
        .exchange_by_hash("to_a", |_x: &u64| 0)
        .output("out")?;
    Ok(())
}

type Build = fn(&mut DataflowBuilder<u64>, &Arc<AtomicBool>) -> Result<()>;

struct Setup {
    build: Build,
    /// Whether node-a / node-b drain on cancel.
    drains: [bool; 2],
    /// node-b's input stays open (the drain closes it).
    b_input_open: bool,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            build: pipeline,
            drains: [true, true],
            b_input_open: true,
        }
    }
}

struct Node {
    rt: Option<RuntimeHandle>,
    df: Option<ClusterSpawnedDataflow<u64>>,
    _managers: Option<Managers>,
    _input: Option<InputSender<u64, u64>>,
}

impl Node {
    fn df(&mut self) -> &mut ClusterSpawnedDataflow<u64> {
        self.df.as_mut().expect("dataflow already joined")
    }

    fn join(&mut self, timeout: Duration) -> tokio::task::JoinHandle<Option<Result<()>>> {
        join_with_timeout(self.df.take().expect("dataflow already joined"), timeout)
    }
}

/// node-a and node-b (one worker each), per `setup`. node-a's input gets
/// `BATCHES` single-record batches (about 2 s of work); both outputs are
/// collected.
async fn start(
    mesh: &mut FaultMesh,
    mode: Mode,
    fail: &Arc<AtomicBool>,
    setup: Setup,
) -> (Vec<Node>, [support::Collector<u64>; 2]) {
    let names = ["node-a", "node-b"];
    let topo = ClusterTopology::multi_node(names.iter().map(|n| NodeConfig::new(*n, 1)).collect())
        .unwrap();
    let id = DataflowId::new();
    let mut managers = Vec::new();
    let handles: Vec<_> = names
        .iter()
        .map(|n| {
            let fail = Arc::clone(fail);
            let pipeline = setup.build;
            let build = move |b: &mut DataflowBuilder<u64>| pipeline(b, &fail);
            let drains = setup.drains[usize::from(*n == "node-b")];
            let options = if drains {
                SpawnOptions::new().drain_on_cancel(DRAIN)
            } else {
                SpawnOptions::new()
            };
            match mode {
                Mode::Dedicated => {
                    managers.push(None);
                    let transport = mesh.dedicated(n, 1024);
                    spawn_node_with_options(topo.clone(), n, id, transport, 2, build, options)
                }
                Mode::Shared => {
                    let m = mesh.managers(n);
                    managers.push(Some(Arc::clone(&m)));
                    let transport = ClusterSpawnTransport::shared(m, 1024);
                    spawn_node_with_options(topo.clone(), n, id, transport, 2, build, options)
                }
            }
        })
        .collect();
    let mut nodes = Vec::new();
    for ((name, handle), managers) in names.iter().zip(handles).zip(managers) {
        let (rt, df) = handle.await.unwrap();
        nodes.push(Node {
            rt: Some(rt),
            df: Some(df.unwrap_or_else(|e| panic!("{mode:?}: {name} failed to spawn: {e}"))),
            _managers: managers,
            _input: None,
        });
    }
    let oa = collect(nodes[0].df().take_output::<u64>(0, "out").unwrap());
    let ob = collect(nodes[1].df().take_output::<u64>(0, "out").unwrap());
    let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
    if setup.b_input_open {
        nodes[1]._input = Some(sb);
    } else {
        drop(sb);
    }
    let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
    for i in 0..BATCHES {
        sa.send(0, vec![i]).unwrap();
    }
    (nodes, [oa, ob])
}

async fn shutdown(nodes: Vec<Node>) {
    let mut rts = Vec::new();
    for mut node in nodes {
        drop(node.df.take());
        drop(node._input.take());
        drop(node._managers.take());
        rts.extend(node.rt.take());
    }
    drop_runtimes(rts).await;
}

/// node-b stopped its drain because node-a stopped without finishing.
fn stopped_for_node_a(result: &Option<Result<()>>) -> bool {
    matches!(
        result,
        Some(Err(Error::Cancelled {
            reason: Some(CancellationReason::NetworkError { detail }),
        })) if detail.contains("node-a")
    )
}

/// node-b stopped with a failure node-a forwarded.
fn stopped_by_forwarded_failure(result: &Option<Result<()>>) -> bool {
    matches!(
        result,
        Some(Err(Error::Cancelled {
            reason: Some(CancellationReason::PeerCancelled { peer_id, detail }),
        })) if peer_id == "node-a" && detail.starts_with("worker failed: ")
    )
}

/// A worker fails while every node drains on node-a's requested stop. The
/// failed worker never closes its channels, so node-b's drain cannot
/// complete: node-a forwards the failure and node-b stops at once, with it,
/// instead of after its 60 s drain deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failure_during_a_drain_is_forwarded_to_draining_peers() {
    for mode in MODES {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let (mut nodes, _outputs) = start(&mut mesh, mode, &fail, Setup::default()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        nodes[0].df().cancel();
        tokio::time::sleep(Duration::from_millis(200)).await;
        fail.store(true, Ordering::SeqCst);

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        let ra = ja.await.unwrap();
        assert!(
            stopped_by_forwarded_failure(&rb),
            "{mode:?}: node-b must stop with node-a's forwarded failure: {}",
            fmt_join(&rb)
        );
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?}: node-a failed: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// node-a's runtime is dropped mid-stream. It announces "runtime shutdown"
/// (a requested stop, so node-b drains), but node-a does not drain: its
/// workers stop without closing their channels. node-a says so (a second
/// `Cancel`), and node-b, whose drain needs node-a's data, stops at once
/// instead of after its drain deadline. (A node that drains on cancel
/// finishes its part even as its runtime handle is dropped, and its peers
/// then succeed.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn runtime_drop_during_a_drain_releases_draining_peers() {
    for mode in MODES {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let a_stops = Setup {
            drains: [false, true],
            ..Setup::default()
        };
        let (mut nodes, _outputs) = start(&mut mesh, mode, &fail, a_stops).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let jb = nodes[1].join(PROMPT);
        let rt = nodes[0].rt.take().unwrap();
        drop_runtimes(vec![rt]).await;

        let rb = jb.await.unwrap();
        assert!(
            stopped_for_node_a(&rb),
            "{mode:?}: node-b must stop once node-a's runtime is gone: {}",
            fmt_join(&rb)
        );
        let ra = nodes[0].join(PROMPT).await.unwrap();
        assert!(
            matches!(ra, Some(Err(Error::Cancelled { .. }))),
            "{mode:?}: node-a: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// node-a requests a stop and then disconnects (crashes) before it could
/// finish its drain or forward anything. node-b drains on the stop, but
/// node-a's data channels never close cleanly: node-b stops once the link
/// is gone, not after its drain deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_disconnecting_after_its_cancel_stops_the_drain() {
    for mode in MODES {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let (mut nodes, _outputs) = start(&mut mesh, mode, &fail, Setup::default()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        nodes[0].df().cancel();
        // node-b receives the Cancel and drains.
        tokio::time::sleep(Duration::from_millis(300)).await;
        mesh.cut("node-a", "node-b");

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        let ra = ja.await.unwrap();
        assert!(
            stopped_for_node_a(&rb),
            "{mode:?}: node-b must stop once node-a is gone: {}",
            fmt_join(&rb)
        );
        assert!(ra.is_some(), "{mode:?}: node-a hung");
        shutdown(nodes).await;
    }
}

/// node-a is cancelled but does not drain: its workers stop at once without
/// closing their channels. node-b drains on node-a's `Cancel` and needs its
/// data; node-a says it stopped unfinished, so node-b stops at once instead
/// of after its drain deadline, in both transport modes (in Shared mode the
/// connection stays up, so nothing else would tell node-b).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_stopping_without_a_drain_releases_drains_that_need_it() {
    for mode in MODES {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let a_stops = Setup {
            drains: [false, true],
            ..Setup::default()
        };
        let (mut nodes, _outputs) = start(&mut mesh, mode, &fail, a_stops).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        nodes[0].df().cancel();

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        let ra = ja.await.unwrap();
        assert!(
            stopped_for_node_a(&rb),
            "{mode:?}: node-b must stop once node-a stopped: {}",
            fmt_join(&rb)
        );
        assert!(
            matches!(ra, Some(Err(Error::Cancelled { .. }))),
            "{mode:?}: node-a: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// Control: a requested stop whose drain can complete still succeeds on
/// every node with every record, and no failure is forwarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn completed_drain_succeeds_without_a_forwarded_failure() {
    for mode in MODES {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let (mut nodes, [_oa, ob]) = start(&mut mesh, mode, &fail, Setup::default()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        nodes[0].df().cancel();

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        let ra = ja.await.unwrap();
        assert!(
            matches!(ra, Some(Ok(()))) && matches!(rb, Some(Ok(()))),
            "{mode:?}: a={} b={}",
            fmt_join(&ra),
            fmt_join(&rb)
        );
        let got = ob.join().unwrap();
        assert_eq!(got.len() as u64, BATCHES, "{mode:?}: node-b's records");
        shutdown(nodes).await;
    }
}

/// node-a drains a pipeline that no longer needs node-b: node-b's input is
/// closed (so its exchange output to node-a closes at once) and node-a never
/// routes a record to node-b (its push to node-b closes with node-a's input,
/// or stays open but idle). node-b stops
/// without draining (it says so), and the link is then cut while node-a
/// still drains (node-b's side would otherwise keep reading for a while):
/// none of that may stop node-a's drain, which completes with every record,
/// in both transport modes. (That a failed send of progress alone does not
/// count is pinned by the lifecycle machine's unit tests.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn drain_that_needs_nothing_from_a_stopped_peer_still_succeeds() {
    let pipelines: [(&str, Build); 2] = [
        ("exchange first", local_pipeline),
        ("exchange last", late_exchange_pipeline),
    ];
    for (mode, (shape, build)) in MODES
        .into_iter()
        .flat_map(|mode| pipelines.map(|p| (mode, p)))
    {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let setup = Setup {
            build,
            drains: [true, false],
            b_input_open: false,
        };
        let (mut nodes, [oa, _ob]) = start(&mut mesh, mode, &fail, setup).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        nodes[0].df().cancel();

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        mesh.cut("node-a", "node-b");
        let ra = ja.await.unwrap();
        assert!(
            matches!(ra, Some(Ok(()))),
            "{mode:?} {shape}: node-a's drain needs nothing from node-b: a={} b={}",
            fmt_join(&ra),
            fmt_join(&rb)
        );
        assert_eq!(oa.join().unwrap().len() as u64, BATCHES, "{mode:?} {shape}");
        assert!(rb.is_some(), "{mode:?} {shape}: node-b hung");
        shutdown(nodes).await;
    }
}
