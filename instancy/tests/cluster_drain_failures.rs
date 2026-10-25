//! A cluster-wide requested stop (`drain_on_cancel`) that can no longer
//! complete must end on every node at once instead of each draining peer
//! waiting for its drain deadline:
//!
//! - a node whose worker fails while it drains forwards the failure, which
//!   stops every peer's drain;
//! - a node that stops without finishing its part (it does not drain, or its
//!   runtime is dropped) says so, which stops the drains that need its data;
//! - a peer that cancelled and then disconnected before its goodbye stops
//!   the drains that need its data;
//! - outside a drain, a peer that stops mid-run (its worker fails, or its node
//!   crashes) fails the survivors promptly with the peer failure.
//!
//! Whether a drain still needs a stopped peer is decided by progress (R48):
//! once no update can arrive and the draining worker's view still holds
//! work, it stops, naming the peer.
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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use faults::{FaultMesh, FrameKind};
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
/// Attempts per round when rounds end in an early completion (audit C1/C2,
/// PR4/PR5; see `early_completion`): seen only on a starved machine.
const ATTEMPTS: usize = 3;

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

/// Records `counted_pipeline`'s `slow` has emitted (one test uses it).
static SLOW_RECORDS: AtomicUsize = AtomicUsize::new(0);

/// `pipeline` without the failure switch, counting the records `slow`
/// emits.
fn counted_pipeline(builder: &mut DataflowBuilder<u64>, _fail: &Arc<AtomicBool>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .unary("slow", move |input, output| {
            if let Some((time, data)) = input.next() {
                std::thread::sleep(Duration::from_millis(20));
                SLOW_RECORDS.fetch_add(data.len(), Ordering::SeqCst);
                output.push_vec(time, data);
            }
            Ok(())
        })
        .exchange_by_hash("to_b", |_x: &u64| 1)
        .map("id", |_t, x| x)
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
    /// node-a's input stays open after its batches (holding its capability).
    a_input_open: bool,
    /// Batches node-a's input gets.
    a_batches: u64,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            build: pipeline,
            drains: [true, true],
            b_input_open: true,
            a_input_open: false,
            a_batches: BATCHES,
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
/// `setup.a_batches` single-record batches (`BATCHES`: about 2 s of work);
/// both outputs are collected.
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
    for i in 0..setup.a_batches {
        sa.send(0, vec![i]).unwrap();
    }
    if setup.a_input_open {
        nodes[0]._input = Some(sa);
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

/// node-b stopped (its drain, or its run) because node-a stopped without
/// finishing its part: never `Ok` and never `DataLoss` (under port-counted
/// progress node-a's unreleased work keeps node-b's view non-zero, R48), and
/// within `PROMPT`, not at the drain deadline.
fn stopped_for_node_a(result: &Option<Result<()>>) -> bool {
    matches!(
        result,
        Some(Err(Error::Cancelled {
            reason: Some(CancellationReason::NetworkError { detail }),
        })) if detail.contains("node-a")
    )
}

/// An early completion (audit C1/C2, PR4/PR5): a node finished while records
/// were still on their way to it, and the push or receive was reported.
fn early_completion(result: &Option<Result<()>>) -> bool {
    let text = fmt_join(result);
    [
        "records sent after the peer finished",
        "records sent while the peer was finishing",
        "records arrived after the receiving operator finished",
        "records arrived as the receiving operator finished",
    ]
    .iter()
    .any(|symptom| text.contains(symptom))
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
        for attempt in 1..=ATTEMPTS {
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
            // node-b can only succeed if it completed while node-a still had
            // records for it (node-a's failure means its part never finished).
            let early =
                matches!(rb, Some(Ok(()))) || early_completion(&ra) || early_completion(&rb);
            if early && attempt < ATTEMPTS {
                shutdown(nodes).await;
                continue;
            }
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
            break;
        }
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
            "{mode:?}: node-b must not wait once node-a's runtime is gone: {}",
            fmt_join(&rb)
        );
        let ra = nodes[0].join(PROMPT).await.unwrap();
        assert!(
            matches!(ra, Some(Err(_))),
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
            "{mode:?}: node-b must not wait once node-a is gone: {}",
            fmt_join(&rb)
        );
        assert!(ra.is_some(), "{mode:?}: node-a hung");
        shutdown(nodes).await;
    }
}

/// node-a is cancelled but does not drain: its workers stop at once,
/// leaving work node-b's drain needs: records it never consumed, or (with
/// nothing sent) the capability of its still-open input. node-b drains on
/// node-a's `Cancel`; node-a's progress lanes close without a seal, and
/// node-b stops promptly with the peer named instead of after its drain
/// deadline (R48), in both transport modes (in Shared mode the connection
/// stays up, so nothing else would tell node-b).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_stopping_without_a_drain_releases_drains_that_need_it() {
    let variants = [
        ("records unconsumed", false, BATCHES),
        ("capability held", true, 0),
    ];
    for (mode, (left, a_input_open, a_batches)) in MODES
        .into_iter()
        .flat_map(|mode| variants.map(|v| (mode, v)))
    {
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let a_stops = Setup {
            drains: [false, true],
            a_input_open,
            a_batches,
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
            "{mode:?} ({left}): node-b must stop promptly, naming node-a: {}",
            fmt_join(&rb)
        );
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?} ({left}): node-a: {}",
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
        // A round that ends in an early completion (C1/C2, PR4/PR5: node-b
        // finishes while node-a still has records for it, and the late
        // records are reported) is not this protocol's failure: run it
        // again, as PR7's routing test does.
        for attempt in 1..=ATTEMPTS {
            let fail = Arc::new(AtomicBool::new(false));
            let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
            let (mut nodes, [_oa, ob]) = start(&mut mesh, mode, &fail, Setup::default()).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            nodes[0].df().cancel();

            let ja = nodes[0].join(PROMPT);
            let rb = nodes[1].join(PROMPT).await.unwrap();
            let ra = ja.await.unwrap();
            if attempt < ATTEMPTS && (early_completion(&ra) || early_completion(&rb)) {
                shutdown(nodes).await;
                continue;
            }
            assert!(
                matches!(ra, Some(Ok(()))) && matches!(rb, Some(Ok(()))),
                "{mode:?}: a={} b={}",
                fmt_join(&ra),
                fmt_join(&rb)
            );
            let got = ob.join().unwrap();
            assert_eq!(got.len() as u64, BATCHES, "{mode:?}: node-b's records");
            shutdown(nodes).await;
            break;
        }
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
            ..Setup::default()
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

/// A peer stops mid-run (no cancellation): its worker fails, or its node
/// crashes (the link is cut). node-b still holds open inbound channels from
/// node-a (node-a's exchange is still sending), and must fail promptly with
/// the peer failure: never a progress residual, never a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_stopping_mid_run_fails_the_survivor_promptly() {
    for (mode, crash) in MODES
        .into_iter()
        .flat_map(|mode| [false, true].map(|crash| (mode, crash)))
    {
        let how = if crash { "crash" } else { "worker failure" };
        let fail = Arc::new(AtomicBool::new(false));
        let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, mode == Mode::Shared);
        let running = Setup {
            drains: [false, false],
            ..Setup::default()
        };
        let (mut nodes, _outputs) = start(&mut mesh, mode, &fail, running).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        if crash {
            mesh.cut("node-a", "node-b");
        } else {
            fail.store(true, Ordering::SeqCst);
        }

        let ja = nodes[0].join(PROMPT);
        let rb = nodes[1].join(PROMPT).await.unwrap();
        let ra = ja.await.unwrap();
        let peer_failure = if crash {
            stopped_for_node_a(&rb)
        } else {
            stopped_by_forwarded_failure(&rb)
        };
        assert!(
            peer_failure && !fmt_join(&rb).contains("residual"),
            "{mode:?} ({how}): node-b must fail promptly with node-a's failure: {}",
            fmt_join(&rb)
        );
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?} ({how}): node-a: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// R50: node-a stops right after sending, and its records reach node-b after
/// its stop (node-a's data is held back while its stop announcement and
/// progress overtake it), within the grace. node-b's drain must wait for
/// them, then succeed with every record: the grace exists for this window.
/// Dedicated framing (the fault mesh reorders frames only there).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn drain_waits_for_a_stopped_peers_records_still_on_their_way() {
    const SENT: u64 = 20;
    let fail = Arc::new(AtomicBool::new(false));
    let mut mesh = FaultMesh::new(&["node-a", "node-b"], 2, false);
    let a_to_b = mesh.ctl("node-a", "node-b");
    a_to_b.hold_kind(FrameKind::Data);
    let a_stops = Setup {
        build: counted_pipeline,
        drains: [false, true],
        a_batches: SENT,
        ..Setup::default()
    };
    let (mut nodes, [_oa, ob]) = start(&mut mesh, Mode::Dedicated, &fail, a_stops).await;
    // node-a has sent everything: its records all passed `slow`, and the
    // frames carrying them are parked. (Its exchange cannot close yet:
    // node-b's input is open, R47.)
    let deadline = tokio::time::Instant::now() + PROMPT;
    let mut parked = 0;
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "node-a never finished sending"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = a_to_b.parked();
        if SLOW_RECORDS.load(Ordering::SeqCst) as u64 == SENT && now > 0 && now == parked {
            break;
        }
        parked = now;
    }
    nodes[0].df().cancel();
    // node-b learns that node-a stopped while its records are still held,
    // and releases them well within its grace.
    tokio::time::sleep(Duration::from_millis(250)).await;
    a_to_b.release_kind();

    let ja = nodes[0].join(PROMPT);
    let rb = nodes[1].join(PROMPT).await.unwrap();
    let ra = ja.await.unwrap();
    assert!(
        matches!(rb, Some(Ok(()))),
        "node-b's drain needs only records already sent: {}",
        fmt_join(&rb)
    );
    assert_eq!(ob.join().unwrap().len() as u64, SENT, "node-b's records");
    assert!(ra.is_some(), "node-a hung");
    shutdown(nodes).await;
}
