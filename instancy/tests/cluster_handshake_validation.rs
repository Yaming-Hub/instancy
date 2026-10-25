//! Regression tests for cluster handshake and topology validation.
//!
//! Every node of a cluster builds its own copy of the dataflow graph and pairs
//! cross-node exchange edges by position. These tests check that mismatched
//! graphs, mismatched topologies, mislabelled peer connections and invalid
//! spawn arguments are rejected with an error during `spawn_cluster` instead
//! of passing the handshake and then hanging, losing records, or
//! reinterpreting bytes as a different type.
//!
//! Scenarios run over loopback TCP in both Dedicated and Shared transport
//! modes. Anything that could hang is bounded by a timeout.

#![cfg(feature = "transport")]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, OutputReceiver, Result,
    RuntimeConfig, RuntimeHandle, SpawnOptions, Timestamp, TokioMode,
};

/// Handshake/ready-barrier timeout used by mismatch scenarios. Shared mode has
/// no connection EOF to signal a failed peer, so a node whose peer rejected the
/// handshake only fails once this expires.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
/// Upper bound for a whole spawn attempt in a mismatch scenario.
const SPAWN_BOUND: Duration = Duration::from_secs(20);
/// Upper bound for a successful cluster run to complete.
const JOIN_BOUND: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
enum Mode {
    Dedicated,
    Shared,
}

type TcpConn = (OwnedReadHalf, OwnedWriteHalf);

/// One end of a loopback TCP link. `label` is the peer the local node
/// *believes* is on the other end.
struct Link {
    label: String,
    conn: TcpConn,
}

async fn tcp_pair() -> (TcpConn, TcpConn) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    let a = accepted.0;
    let b = connected;
    a.set_nodelay(true).unwrap();
    b.set_nodelay(true).unwrap();
    (a.into_split(), b.into_split())
}

/// Full mesh of correctly labelled loopback links.
async fn tcp_mesh(nodes: &[&str]) -> HashMap<String, Vec<Link>> {
    let mut mesh: HashMap<String, Vec<Link>> =
        nodes.iter().map(|n| (n.to_string(), Vec::new())).collect();
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            let (ci, cj) = tcp_pair().await;
            mesh.get_mut(nodes[i]).unwrap().push(Link {
                label: nodes[j].to_string(),
                conn: ci,
            });
            mesh.get_mut(nodes[j]).unwrap().push(Link {
                label: nodes[i].to_string(),
                conn: cj,
            });
        }
    }
    mesh
}

struct PreEstablishedFactory {
    connections: Mutex<VecDeque<TcpConn>>,
}

impl ConnectionFactory for PreEstablishedFactory {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.connections
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "no more pre-established connections".into())
    }
}

fn shared_config() -> SharedConnectionConfig {
    SharedConnectionConfig {
        connections: 1,
        probe_interval: Duration::from_secs(3600),
        ..SharedConnectionConfig::default()
    }
}

/// A node's runtime plus its shared peer managers. Real deployments keep peer
/// managers for the lifetime of the process; the test does the same so a node
/// that finishes first does not tear down connections its peers still use
/// (early-finisher teardown is tracked separately).
struct NodeRuntime {
    rt: RuntimeHandle,
    _managers: Option<Arc<HashMap<String, SharedPeerManager>>>,
}

impl NodeRuntime {
    fn shutdown(&self) {
        self.rt.shutdown();
    }
}

type SpawnOutcome<T> = (NodeRuntime, Result<ClusterSpawnedDataflow<T>>);

/// Spawn one node of a cluster on a blocking thread.
fn spawn_node<T, F>(
    mode: Mode,
    topology: ClusterTopology,
    node_id: &str,
    df: DataflowId,
    links: Vec<Link>,
    capacity: usize,
    build: F,
) -> JoinHandle<SpawnOutcome<T>>
where
    T: Timestamp + instancy::communication::ExchangeData,
    F: Fn(&mut DataflowBuilder<T>) -> Result<()> + Send + 'static,
{
    let node_id = node_id.to_string();
    let handle = tokio::runtime::Handle::current();
    match mode {
        Mode::Dedicated => {
            let conns: Vec<PeerConnection<OwnedReadHalf, OwnedWriteHalf>> = links
                .into_iter()
                .map(|l| PeerConnection {
                    node_id: l.label,
                    reader: l.conn.0,
                    writer: l.conn.1,
                })
                .collect();
            tokio::task::spawn_blocking(move || {
                let rt = new_runtime_on(&handle);
                let r = rt.spawn_cluster(
                    "handshake-validation",
                    topology,
                    &node_id,
                    df,
                    ClusterSpawnTransport::dedicated(conns, capacity),
                    HANDSHAKE_TIMEOUT,
                    build,
                    &handle,
                    SpawnOptions::new(),
                );
                (
                    NodeRuntime {
                        rt,
                        _managers: None,
                    },
                    r,
                )
            })
        }
        Mode::Shared => {
            let mut managers = HashMap::new();
            for l in links {
                let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory {
                    connections: Mutex::new(VecDeque::from([l.conn])),
                });
                let mgr = SharedPeerManager::new(
                    l.label.clone(),
                    shared_config(),
                    factory,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap();
                managers.insert(l.label, mgr);
            }
            let managers = Arc::new(managers);
            tokio::task::spawn_blocking(move || {
                let rt = new_runtime_on(&handle);
                let r = rt.spawn_cluster(
                    "handshake-validation",
                    topology,
                    &node_id,
                    df,
                    ClusterSpawnTransport::shared(Arc::clone(&managers), capacity),
                    HANDSHAKE_TIMEOUT,
                    build,
                    &handle,
                    SpawnOptions::new(),
                );
                (
                    NodeRuntime {
                        rt,
                        _managers: Some(managers),
                    },
                    r,
                )
            })
        }
    }
}

fn new_runtime_on(handle: &tokio::runtime::Handle) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        tokio_mode: TokioMode::External(handle.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Await every spawn and assert that *all* nodes rejected the cluster with an
/// error (none may hang or succeed). Returns the error messages.
async fn expect_all_rejected<T: Timestamp>(
    handles: Vec<JoinHandle<SpawnOutcome<T>>>,
    what: &str,
) -> Vec<String> {
    let mut errors = Vec::new();
    let mut runtimes = Vec::new();
    let mut accepted = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        let (rt, r) = tokio::time::timeout(SPAWN_BOUND, h)
            .await
            .unwrap_or_else(|_| panic!("{what}: node {i} spawn hung for {SPAWN_BOUND:?}"))
            .unwrap();
        match r {
            Ok(c) => accepted.push((i, c)),
            Err(e) => errors.push(e.to_string()),
        }
        runtimes.push(rt);
    }
    let accepted_idx: Vec<usize> = accepted.iter().map(|(i, _)| *i).collect();
    for (_, c) in accepted {
        c.cancel();
    }
    for rt in runtimes {
        rt.shutdown();
    }
    assert!(
        accepted_idx.is_empty(),
        "{what}: nodes {accepted_idx:?} accepted the cluster; errors from the others: {errors:?}"
    );
    errors
}

fn drain<T: Timestamp, D: Send + 'static>(o: &OutputReceiver<T, D>) -> Vec<D> {
    let mut v = Vec::new();
    while let Some(ev) = o.try_recv() {
        if let OutputEvent::Data { data, .. } = ev {
            v.extend(data);
        }
    }
    v
}

fn two_nodes() -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap()
}

fn exchange_i64(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<i64>("data")?
        .exchange("by_val", |x: &i64| *x as u64)
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

fn exchange_u64(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<u64>("data")?
        .exchange("by_val", |x: &u64| *x)
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// Run a two-node scenario where each node builds with its own closure and
/// assert both nodes reject it.
async fn two_node_mismatch<FA, FB>(mode: Mode, build_a: FA, build_b: FB, what: &str) -> Vec<String>
where
    FA: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
    FB: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let df = DataflowId::new();
    let ha = spawn_node(
        mode,
        two_nodes(),
        "node-a",
        df,
        mesh.remove("node-a").unwrap(),
        1024,
        build_a,
    );
    let hb = spawn_node(
        mode,
        two_nodes(),
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        build_b,
    );
    expect_all_rejected(vec![ha, hb], what).await
}

// ---------------------------------------------------------------------------
// Invalid spawn arguments: errors, not panics
// ---------------------------------------------------------------------------

fn catch_spawn_panic<F: FnOnce() -> Result<()>>(f: F) -> Result<()> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(p) => {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!("spawn_cluster panicked instead of returning an error: {msg}");
        }
    }
}

/// `ClusterTopology::single_node(0)` is constructible; spawning it must fail
/// with an error rather than panic inside the library.
#[test]
fn zero_worker_local_node_is_an_error_not_a_panic() {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let handle = tokio_rt.handle().clone();
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        tokio_mode: TokioMode::External(handle.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let r = catch_spawn_panic(|| {
        let conns: Vec<PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>> =
            Vec::new();
        rt.spawn_cluster(
            "zero",
            ClusterTopology::single_node(0),
            "local",
            DataflowId::new(),
            ClusterSpawnTransport::dedicated(conns, 64),
            Duration::from_secs(1),
            exchange_i64,
            &handle,
            SpawnOptions::new(),
        )
        .map(|_| ())
    });
    let err = r.expect_err("spawn_cluster accepted a 0-worker local node");
    assert!(
        matches!(
            err,
            instancy::Error::Topology(instancy::TopologyError::InvalidNodeConfig { .. })
        ),
        "unexpected error: {err}"
    );
    drop(rt);
    tokio_rt.shutdown_timeout(Duration::from_secs(1));
}

/// A remote node with zero workers is just as invalid as a local one.
#[test]
fn zero_worker_remote_node_is_an_error_not_a_panic() {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let handle = tokio_rt.handle().clone();
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        tokio_mode: TokioMode::External(handle.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut topology = ClusterTopology::multi_node(vec![NodeConfig::new("node-a", 1)]).unwrap();
    topology.nodes.push(NodeConfig::new("node-b", 0));
    let (a_to_b, _b_from_a) = tokio::io::duplex(1 << 16);
    let (_b_to_a, a_from_b) = tokio::io::duplex(1 << 16);
    let r = catch_spawn_panic(|| {
        rt.spawn_cluster(
            "zero-remote",
            topology,
            "node-a",
            DataflowId::new(),
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-b".to_string(),
                    reader: a_from_b,
                    writer: a_to_b,
                }],
                64,
            ),
            Duration::from_millis(200),
            exchange_i64,
            &handle,
            SpawnOptions::new(),
        )
        .map(|_| ())
    });
    // The topology must be rejected up front, not via a later handshake timeout.
    let err = r.expect_err("spawn_cluster accepted a 0-worker remote node");
    assert!(
        matches!(
            err,
            instancy::Error::Topology(instancy::TopologyError::InvalidNodeConfig { .. })
        ),
        "unexpected error: {err}"
    );
    drop(rt);
    tokio_rt.shutdown_timeout(Duration::from_secs(1));
}

/// A transport capacity of 0 must be rejected with an error rather than
/// panicking inside `tokio::sync::mpsc::channel(0)`.
#[test]
fn zero_capacity_dedicated_transport_is_an_error_not_a_panic() {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let handle = tokio_rt.handle().clone();
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        tokio_mode: TokioMode::External(handle.clone()),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let (a_to_b, _b_from_a) = tokio::io::duplex(1 << 16);
    let (_b_to_a, a_from_b) = tokio::io::duplex(1 << 16);
    let r = catch_spawn_panic(|| {
        rt.spawn_cluster(
            "zero-cap",
            two_nodes(),
            "node-a",
            DataflowId::new(),
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-b".to_string(),
                    reader: a_from_b,
                    writer: a_to_b,
                }],
                0,
            ),
            Duration::from_millis(200),
            exchange_i64,
            &handle,
            SpawnOptions::new(),
        )
        .map(|_| ())
    });
    assert!(r.is_err(), "spawn_cluster accepted transport capacity 0");
    drop(rt);
    tokio_rt.shutdown_timeout(Duration::from_secs(1));
}

/// Same as above for the shared transport (per-channel receiver capacity 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_capacity_shared_transport_is_an_error_not_a_panic() {
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let h = spawn_node(
        Mode::Shared,
        two_nodes(),
        "node-a",
        DataflowId::new(),
        mesh.remove("node-a").unwrap(),
        0,
        exchange_i64,
    );
    let (rt, r) = tokio::time::timeout(SPAWN_BOUND, h)
        .await
        .expect("spawn hung")
        .expect("spawn_cluster panicked with capacity 0");
    let accepted = r.is_ok();
    rt.shutdown();
    assert!(!accepted, "shared spawn_cluster accepted capacity 0");
}

// ---------------------------------------------------------------------------
// Duplicate node ids
// ---------------------------------------------------------------------------

#[test]
fn multi_node_rejects_duplicate_node_ids() {
    let r = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 2),
        NodeConfig::new("node-a", 1),
    ]);
    assert!(r.is_err(), "multi_node accepted duplicate node ids: {r:?}");
}

/// `ClusterTopology::nodes` is a public field, so duplicates can bypass
/// `multi_node`. spawn_cluster must reject them: otherwise the duplicate entry
/// becomes a phantom worker that receives (and loses) exchanged records while
/// the run still reports success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spawn_rejects_duplicate_node_ids_in_topology() {
    let mut topology = ClusterTopology::multi_node(vec![NodeConfig::new("node-a", 1)]).unwrap();
    topology.nodes.push(NodeConfig::new("node-a", 1));
    let df = DataflowId::new();
    let h = spawn_node(
        Mode::Dedicated,
        topology,
        "node-a",
        df,
        Vec::new(),
        1024,
        exchange_i64,
    );
    let (rt, r) = tokio::time::timeout(SPAWN_BOUND, h).await.unwrap().unwrap();
    let mut cluster = match r {
        Err(_) => {
            rt.shutdown();
            return;
        }
        Ok(c) => c,
    };
    // Accepted: at minimum the run must not silently lose records.
    let out = cluster.take_output::<i64>(0, "out").unwrap();
    let input = cluster.take_input::<i64>(0, "data").unwrap();
    input.send(0, (0..20).collect()).unwrap();
    drop(input);
    let joined = tokio::time::timeout(Duration::from_secs(5), cluster.join().unwrap()).await;
    let mut got = drain(&out);
    got.sort();
    rt.shutdown();
    panic!(
        "duplicate node ids were accepted (join={:?}); outputs={got:?}",
        joined.map(|r| r.map_err(|e| e.to_string()))
    );
}

// ---------------------------------------------------------------------------
// Topology mismatches across nodes
// ---------------------------------------------------------------------------

/// node-a believes {a:2, b:1}; node-b believes {a:1, b:2}. Same total worker
/// count, so the old fingerprint matched and both nodes hung.
async fn per_node_worker_split_mismatch(mode: Mode) {
    let topo_a = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 2),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let topo_b = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 2),
    ])
    .unwrap();
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let df = DataflowId::new();
    let ha = spawn_node(
        mode,
        topo_a,
        "node-a",
        df,
        mesh.remove("node-a").unwrap(),
        1024,
        exchange_i64,
    );
    let hb = spawn_node(
        mode,
        topo_b,
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        exchange_i64,
    );
    expect_all_rejected(vec![ha, hb], "per-node worker split mismatch").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_node_worker_split_mismatch_is_rejected_dedicated() {
    per_node_worker_split_mismatch(Mode::Dedicated).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_node_worker_split_mismatch_is_rejected_shared() {
    per_node_worker_split_mismatch(Mode::Shared).await;
}

// ---------------------------------------------------------------------------
// Peer identity
// ---------------------------------------------------------------------------

/// node-a's two links are mislabelled: the one it calls "node-b" really goes
/// to node-c and vice versa. Previously all three nodes passed the handshake
/// and ready barrier, then every node hung because frames landed on
/// unregistered channels.
async fn miswired_peer_connections(mode: Mode) {
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
        NodeConfig::new("node-c", 1),
    ])
    .unwrap();
    let mut mesh = tcp_mesh(&["node-a", "node-b", "node-c"]).await;
    let mut links_a = mesh.remove("node-a").unwrap();
    for l in &mut links_a {
        l.label = if l.label == "node-b" {
            "node-c".into()
        } else {
            "node-b".into()
        };
    }
    let df = DataflowId::new();
    let ha = spawn_node(
        mode,
        topo.clone(),
        "node-a",
        df,
        links_a,
        1024,
        exchange_i64,
    );
    let hb = spawn_node(
        mode,
        topo.clone(),
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        exchange_i64,
    );
    let hc = spawn_node(
        mode,
        topo,
        "node-c",
        df,
        mesh.remove("node-c").unwrap(),
        1024,
        exchange_i64,
    );
    expect_all_rejected(vec![ha, hb, hc], "miswired peer connections").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn miswired_peer_connections_are_rejected_dedicated() {
    miswired_peer_connections(Mode::Dedicated).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn miswired_peer_connections_are_rejected_shared() {
    miswired_peer_connections(Mode::Shared).await;
}

// ---------------------------------------------------------------------------
// Graph mismatches across nodes
// ---------------------------------------------------------------------------

/// node-a exchanges `i64`, node-b exchanges `u64` (e.g. a type change during
/// a rolling upgrade). Previously node-b decoded -20 as 18446744073709551596.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn exchange_data_type_mismatch_is_rejected_dedicated() {
    two_node_mismatch(
        Mode::Dedicated,
        exchange_i64,
        exchange_u64,
        "i64 vs u64 exchange",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn exchange_data_type_mismatch_is_rejected_shared() {
    two_node_mismatch(
        Mode::Shared,
        exchange_i64,
        exchange_u64,
        "i64 vs u64 exchange",
    )
    .await;
}

/// One branch = filter -> exchange -> output. Both branches have the same
/// shape, so operator/edge counts and exchange edge indices are identical
/// whichever order the branches are declared in.
fn add_branch(input: &instancy::Pipe<u64, u64>, name: &'static str, want_even: bool) -> Result<()> {
    input
        .clone()
        .filter(name, move |_t, x: &u64| (*x % 2 == 0) == want_even)
        .exchange_by_hash(format!("{name}-ex"), |x: &u64| *x / 2)
        .output(name)?;
    Ok(())
}

fn evens_then_odds(b: &mut DataflowBuilder<u64>) -> Result<()> {
    let input = b.input::<u64>("data")?;
    add_branch(&input, "evens", true)?;
    add_branch(&input, "odds", false)?;
    Ok(())
}

fn odds_then_evens(b: &mut DataflowBuilder<u64>) -> Result<()> {
    let input = b.input::<u64>("data")?;
    add_branch(&input, "odds", false)?;
    add_branch(&input, "evens", true)?;
    Ok(())
}

/// node-a declares [evens, odds] and node-b [odds, evens] (e.g. iterating a
/// HashMap of per-key configs). Exchange edges are paired by position, so the
/// old handshake cross-wired the two logical streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn branch_declaration_order_mismatch_is_rejected_dedicated() {
    two_node_mismatch(
        Mode::Dedicated,
        evens_then_odds,
        odds_then_evens,
        "branch order",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn branch_declaration_order_mismatch_is_rejected_shared() {
    two_node_mismatch(
        Mode::Shared,
        evens_then_odds,
        odds_then_evens,
        "branch order",
    )
    .await;
}

// ---------------------------------------------------------------------------
// Adversarial: graphs or topologies that differ in exactly one attribute
// ---------------------------------------------------------------------------

/// Run an async scenario in both transport modes.
macro_rules! both_modes {
    ($name:ident, $scenario:path) => {
        mod $name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
            async fn dedicated() {
                $scenario(Mode::Dedicated).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
            async fn shared() {
                $scenario(Mode::Shared).await;
            }
        }
    };
}

fn op_map(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<i64>("data")?
        .map("op", |_t, x| x)
        .exchange("by_val", |x: &i64| *x as u64)
        .output("out")?;
    Ok(())
}

fn op_filter(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<i64>("data")?
        .filter("op", |_t, _x| true)
        .exchange("by_val", |x: &i64| *x as u64)
        .output("out")?;
    Ok(())
}

/// Same operator name, ports, edges and types; only `map` vs `filter` differs.
async fn operator_kind_mismatch(mode: Mode) {
    two_node_mismatch(mode, op_map, op_filter, "map vs filter").await;
}
both_modes!(operator_kind_mismatch_is_rejected, operator_kind_mismatch);

fn wired(
    left_exchanged: bool,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        let l = b.input::<u64>("l")?;
        let r = b.input::<u64>("r")?;
        let (ex, direct) = if left_exchanged { (l, r) } else { (r, l) };
        ex.exchange("x", |x: &u64| *x).output("o1")?;
        direct.output("o2")?;
        Ok(())
    }
}

/// Same operators everywhere; only which input feeds the exchange differs.
async fn edge_endpoint_mismatch(mode: Mode) {
    two_node_mismatch(mode, wired(true), wired(false), "edge endpoints").await;
}
both_modes!(edge_endpoint_mismatch_is_rejected, edge_endpoint_mismatch);

fn routed(broadcast: bool) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        let input = b.input::<u64>("data")?;
        let routed = if broadcast {
            input.broadcast("route")
        } else {
            input.exchange("route", |x: &u64| *x)
        };
        routed.output("out")?;
        Ok(())
    }
}

async fn exchange_vs_broadcast(mode: Mode) {
    two_node_mismatch(mode, routed(false), routed(true), "exchange vs broadcast").await;
}
both_modes!(
    exchange_vs_broadcast_mismatch_is_rejected,
    exchange_vs_broadcast
);

fn named_exchange(
    name: &'static str,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        b.input::<u64>("data")?
            .exchange(name, |x: &u64| *x)
            .output("out")?;
        Ok(())
    }
}

/// Only the name of the exchange differs.
async fn exchange_name_mismatch(mode: Mode) {
    two_node_mismatch(
        mode,
        named_exchange("by_user"),
        named_exchange("by_session"),
        "exchange name",
    )
    .await;
}
both_modes!(exchange_name_mismatch_is_rejected, exchange_name_mismatch);

/// Same node ids and per-node worker counts, but listed in a different order
/// (`nodes` is public), which assigns different global worker ranges.
async fn node_order_mismatch(mode: Mode) {
    let topo_a = two_nodes();
    let mut topo_b = two_nodes();
    topo_b.nodes.reverse();
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let df = DataflowId::new();
    let ha = spawn_node(
        mode,
        topo_a,
        "node-a",
        df,
        mesh.remove("node-a").unwrap(),
        1024,
        exchange_i64,
    );
    let hb = spawn_node(
        mode,
        topo_b,
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        exchange_i64,
    );
    expect_all_rejected(vec![ha, hb], "node order").await;
}
both_modes!(node_order_mismatch_is_rejected, node_order_mismatch);

/// node-b runs under a different id ("node-x") than node-a expects on that
/// connection. Worker counts, graphs and dataflow ids all match.
async fn peer_runs_under_unexpected_id(mode: Mode) {
    let topo_x = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-x", 1),
    ])
    .unwrap();
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let mut links_b = mesh.remove("node-b").unwrap();
    for l in &mut links_b {
        assert_eq!(l.label, "node-a");
    }
    let df = DataflowId::new();
    let ha = spawn_node(
        mode,
        two_nodes(),
        "node-a",
        df,
        mesh.remove("node-a").unwrap(),
        1024,
        exchange_i64,
    );
    let hx = spawn_node(mode, topo_x, "node-x", df, links_b, 1024, exchange_i64);
    let errors = expect_all_rejected(vec![ha, hx], "unexpected peer id").await;
    assert!(
        errors.iter().any(|e| e.contains("identity")),
        "expected a peer identity error, got {errors:?}"
    );
}
both_modes!(
    peer_running_under_unexpected_id_is_rejected,
    peer_runs_under_unexpected_id
);

/// The timestamp type differs (u64 vs u32); everything else matches.
async fn timestamp_type_mismatch(mode: Mode) {
    let mut mesh = tcp_mesh(&["node-a", "node-b"]).await;
    let df = DataflowId::new();
    let ha = spawn_node::<u64, _>(
        mode,
        two_nodes(),
        "node-a",
        df,
        mesh.remove("node-a").unwrap(),
        1024,
        |b: &mut DataflowBuilder<u64>| {
            b.input::<u64>("data")?
                .exchange("x", |x: &u64| *x)
                .output("out")?;
            Ok(())
        },
    );
    let hb = spawn_node::<u32, _>(
        mode,
        two_nodes(),
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        |b: &mut DataflowBuilder<u32>| {
            b.input::<u64>("data")?
                .exchange("x", |x: &u64| *x)
                .output("out")?;
            Ok(())
        },
    );
    expect_all_rejected(vec![ha], "timestamp type (node-a)").await;
    expect_all_rejected(vec![hb], "timestamp type (node-b)").await;
}
both_modes!(timestamp_type_mismatch_is_rejected, timestamp_type_mismatch);

/// node-a is handed two connections labelled "node-b" (one really goes to
/// node-c). node-a rejects the configuration; its peers must fail too rather
/// than hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn duplicated_connection_label_is_rejected_dedicated() {
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
        NodeConfig::new("node-c", 1),
    ])
    .unwrap();
    let mut mesh = tcp_mesh(&["node-a", "node-b", "node-c"]).await;
    let mut links_a = mesh.remove("node-a").unwrap();
    for l in &mut links_a {
        l.label = "node-b".into();
    }
    let df = DataflowId::new();
    let ha = spawn_node(
        Mode::Dedicated,
        topo.clone(),
        "node-a",
        df,
        links_a,
        1024,
        exchange_i64,
    );
    let hb = spawn_node(
        Mode::Dedicated,
        topo.clone(),
        "node-b",
        df,
        mesh.remove("node-b").unwrap(),
        1024,
        exchange_i64,
    );
    let hc = spawn_node(
        Mode::Dedicated,
        topo,
        "node-c",
        df,
        mesh.remove("node-c").unwrap(),
        1024,
        exchange_i64,
    );
    expect_all_rejected(vec![ha, hb, hc], "duplicated connection label").await;
}

// ---------------------------------------------------------------------------
// Adversarial: hand-crafted peers speaking other protocol versions
// ---------------------------------------------------------------------------

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn with_crc(mut payload: Vec<u8>) -> Vec<u8> {
    let crc = crc32(&payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

/// Spawn node-a against a fake node-b that answers node-a's handshake with
/// `reply` and returns node-a's spawn error.
async fn spawn_against_fake_peer(reply: impl FnOnce(DataflowId) -> Vec<u8>) -> String {
    use instancy::communication::transport::{Frame, FramedReader, FramedWriter};

    let ((ra, wa), (rb, wb)) = tcp_pair().await;
    let df = DataflowId::new();
    let payload = reply(df);
    let fake = tokio::spawn(async move {
        let mut reader = FramedReader::new(rb);
        let mut writer = FramedWriter::new(wb);
        // Wait for node-a's handshake, then answer it.
        let _ = reader.read_frame().await;
        let _ = writer
            .write_frame(&Frame {
                dataflow_id: df,
                channel_id: 0,
                payload,
            })
            .await;
        // Keep the connection open until node-a gives up.
        let _ = reader.read_frame().await;
    });
    let h = spawn_node(
        Mode::Dedicated,
        two_nodes(),
        "node-a",
        df,
        vec![Link {
            label: "node-b".into(),
            conn: (ra, wa),
        }],
        1024,
        exchange_i64,
    );
    let errors = expect_all_rejected(vec![h], "fake peer").await;
    fake.abort();
    errors.into_iter().next().unwrap()
}

/// A 0.1.x peer sends the unversioned handshake layout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unversioned_legacy_peer_is_rejected_with_version_error() {
    let err = spawn_against_fake_peer(|df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&0xFEED_u64.to_le_bytes());
        p.extend_from_slice(df.as_bytes());
        with_crc(p)
    })
    .await;
    assert!(
        err.contains("protocol version mismatch") && err.contains("remote=1"),
        "{err}"
    );
}

/// A future peer sends a higher version with a layout this build cannot parse.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn future_version_peer_is_rejected_with_version_error() {
    let err = spawn_against_fake_peer(|_df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&99u32.to_le_bytes());
        p.extend_from_slice(b"some future layout");
        with_crc(p)
    })
    .await;
    assert!(
        err.contains("protocol version mismatch") && err.contains("remote=99"),
        "{err}"
    );
}

/// A peer on cluster protocol version 2 (versioned handshake, but no clean
/// shutdown handshake frames) sends a well-formed handshake: it must be
/// rejected, since it would neither send nor understand the end-of-stream
/// markers and acknowledgements that teardown waits for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_2_peer_without_shutdown_handshake_is_rejected() {
    assert_eq!(instancy::communication::CLUSTER_PROTOCOL_VERSION, 6);
    let err = spawn_against_fake_peer(|df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&2u32.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(df.as_bytes());
        p.extend_from_slice(&6u32.to_le_bytes());
        p.extend_from_slice(b"node-b");
        with_crc(p)
    })
    .await;
    assert!(
        err.contains("protocol version mismatch") && err.contains("remote=2"),
        "{err}"
    );
}

/// A peer on cluster protocol version 3 sends its initial capabilities on
/// its progress stream instead of seeding them from the physical plan: it
/// must be rejected, since this node would count them twice (and the peer
/// would never see this node's).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_3_peer_without_plan_seeding_is_rejected() {
    let err = spawn_against_fake_peer(|df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&3u32.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(df.as_bytes());
        p.extend_from_slice(&6u32.to_le_bytes());
        p.extend_from_slice(b"node-b");
        with_crc(p)
    })
    .await;
    assert!(
        err.contains("protocol version mismatch") && err.contains("remote=3"),
        "{err}"
    );
}

/// A peer on cluster protocol version 5 may spread one dataflow's frames
/// over several pooled connections, and sends frames without checksums: it
/// must be rejected at the handshake (a well-formed v5 handshake carried in
/// today's framing is refused by version).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_5_peer_is_rejected() {
    let err = spawn_against_fake_peer(|df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&5u32.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(df.as_bytes());
        p.extend_from_slice(&6u32.to_le_bytes());
        p.extend_from_slice(b"node-b");
        with_crc(p)
    })
    .await;
    assert!(
        err.contains("protocol version mismatch") && err.contains("remote=5"),
        "{err}"
    );
}

/// A version-5 peer frames its handshake without frame checksums: the
/// handshake fails (loudly, and within the bound), it is never accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_without_frame_checksums_is_rejected() {
    use tokio::io::AsyncWriteExt;

    let ((ra, wa), (rb, mut wb)) = tcp_pair().await;
    let df = DataflowId::new();
    let fake = tokio::spawn(async move {
        let _keep_open = rb;
        let mut payload = vec![0u8];
        payload.extend_from_slice(&5u32.to_le_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());
        payload.extend_from_slice(df.as_bytes());
        payload.extend_from_slice(&6u32.to_le_bytes());
        payload.extend_from_slice(b"node-b");
        let payload = with_crc(payload);
        // The version-5 frame layout: header, then the payload.
        let mut frame = df.as_bytes().to_vec();
        frame.extend_from_slice(&0u64.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        let _ = wb.write_all(&frame).await;
        std::future::pending::<()>().await;
    });
    let h = spawn_node(
        Mode::Dedicated,
        two_nodes(),
        "node-a",
        df,
        vec![Link {
            label: "node-b".into(),
            conn: (ra, wa),
        }],
        1024,
        exchange_i64,
    );
    let errors = expect_all_rejected(vec![h], "peer without frame checksums").await;
    fake.abort();
    assert!(!errors[0].is_empty());
}
/// A current-version peer that claims to be a different node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_claiming_another_identity_is_rejected() {
    let err = spawn_against_fake_peer(|df| {
        let mut p = vec![0u8];
        p.extend_from_slice(&instancy::communication::CLUSTER_PROTOCOL_VERSION.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(df.as_bytes());
        p.extend_from_slice(&6u32.to_le_bytes());
        p.extend_from_slice(b"node-c");
        with_crc(p)
    })
    .await;
    assert!(err.contains("identity"), "{err}");
}

// ---------------------------------------------------------------------------
// Adversarial: cluster sizes at the limits of the channel-id space
// ---------------------------------------------------------------------------

/// A remote node with an absurd worker count must be rejected quickly with an
/// error, not overflow channel ids or try to allocate per-worker-pair state.
async fn oversized_cluster_is_rejected(
    build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
    remote: usize,
) -> Vec<String> {
    let mut topo = ClusterTopology::multi_node(vec![NodeConfig::new("node-a", 1)]).unwrap();
    topo.nodes.push(NodeConfig::new("node-b", remote));
    let (a, _b) = tcp_pair().await;
    let h = spawn_node(
        Mode::Dedicated,
        topo,
        "node-a",
        DataflowId::new(),
        vec![Link {
            label: "node-b".into(),
            conn: a,
        }],
        1024,
        build,
    );
    let started = std::time::Instant::now();
    let errors = expect_all_rejected(vec![h], "oversized cluster").await;
    assert!(
        started.elapsed() < HANDSHAKE_TIMEOUT,
        "rejected only after {:?}: {errors:?}",
        started.elapsed()
    );
    assert!(errors[0].contains("channel id space"), "{errors:?}");
    errors
}

#[cfg(target_pointer_width = "64")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_channel_id_overflow_is_rejected() {
    // N = 2^31 workers: progress ids (< N^2 = 2^62) still fit, but the
    // second exchange edge's data ids (up to 2 * N^2) do not.
    fn two_exchanges(b: &mut DataflowBuilder<u64>) -> Result<()> {
        b.input::<u64>("data")?
            .exchange("x1", |x: &u64| *x)
            .exchange("x2", |x: &u64| *x)
            .output("out")?;
        Ok(())
    }
    let errors = oversized_cluster_is_rejected(two_exchanges, (1 << 31) - 1).await;
    assert!(errors[0].contains("data channel id"), "{errors:?}");
}

#[cfg(target_pointer_width = "64")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progress_channel_id_overflow_is_rejected() {
    // N = 2^31 + 1 workers: progress ids alone exceed their namespace.
    fn no_exchange(b: &mut DataflowBuilder<u64>) -> Result<()> {
        b.input::<u64>("data")?.map("id", |_t, x| x).output("out")?;
        Ok(())
    }
    let errors = oversized_cluster_is_rejected(no_exchange, 1 << 31).await;
    assert!(errors[0].contains("progress channel id"), "{errors:?}");
}

// ---------------------------------------------------------------------------
// Positive controls: matching graphs and topologies still work
// ---------------------------------------------------------------------------

/// Each node builds the graph with its *own* closure instance; the graphs are
/// identical, so the handshake must pass and every record must arrive.
async fn matching_exchange_completes(mode: Mode, nodes: &[&str]) {
    let topo = ClusterTopology::multi_node(
        nodes
            .iter()
            .enumerate()
            .map(|(i, n)| NodeConfig::new(*n, 1 + i % 2))
            .collect(),
    )
    .unwrap();
    let mut mesh = tcp_mesh(nodes).await;
    let df = DataflowId::new();
    let mut handles = Vec::new();
    for n in nodes {
        let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
            b.input::<i64>("data")?
                .exchange("by_val", |x: &i64| *x as u64)
                .map("id", |_t, x| x)
                .output("out")?;
            Ok(())
        };
        handles.push(spawn_node(
            mode,
            topo.clone(),
            n,
            df,
            mesh.remove(*n).unwrap(),
            1024,
            build,
        ));
    }
    let mut clusters = Vec::new();
    let mut runtimes = Vec::new();
    for h in handles {
        let (rt, r) = tokio::time::timeout(SPAWN_BOUND, h).await.unwrap().unwrap();
        clusters.push(r.expect("matching graphs must pass the handshake"));
        runtimes.push(rt);
    }
    let mut outs = Vec::new();
    let mut expected = Vec::new();
    for (ni, c) in clusters.iter_mut().enumerate() {
        for w in 0..c.num_local_workers() {
            outs.push(c.take_output::<i64>(w, "out").unwrap());
            let s = c.take_input::<i64>(w, "data").unwrap();
            let base = (ni as i64) * 1000 + (w as i64) * 100;
            let batch: Vec<i64> = (base - 20..base + 20).collect();
            expected.extend(batch.iter().copied());
            s.send(0, batch).unwrap();
        }
    }
    let joins: Vec<_> = clusters
        .into_iter()
        .map(|c| {
            tokio::spawn(async move { tokio::time::timeout(JOIN_BOUND, c.join().unwrap()).await })
        })
        .collect();
    for j in joins {
        let r = j.await.unwrap();
        assert!(
            matches!(r, Ok(Ok(()))),
            "matching cluster did not complete cleanly: {:?}",
            r.map(|x| x.map_err(|e| e.to_string()))
        );
    }
    let mut got: Vec<i64> = outs.iter().flat_map(drain).collect();
    got.sort();
    expected.sort();
    for rt in runtimes {
        rt.shutdown();
    }
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn matching_graphs_two_nodes_complete_dedicated() {
    matching_exchange_completes(Mode::Dedicated, &["node-a", "node-b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn matching_graphs_two_nodes_complete_shared() {
    matching_exchange_completes(Mode::Shared, &["node-a", "node-b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn matching_graphs_three_nodes_complete_dedicated() {
    matching_exchange_completes(Mode::Dedicated, &["node-a", "node-b", "node-c"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn matching_graphs_three_nodes_complete_shared() {
    matching_exchange_completes(Mode::Shared, &["node-a", "node-b", "node-c"]).await;
}

/// A graph using most builder features (branch, filter, map, both exchange
/// flavours, concat, iterate, reduce, broadcast, gather, count, binary).
fn complex_graph(b: &mut DataflowBuilder<u64>) -> Result<()> {
    let input = b.input::<u64>("data")?;
    let side = b.input::<String>("side")?;
    let (evens, odds) = input.clone().branch("parity", |_t, x| x % 2 == 0);
    let halves = evens
        .map("half", |_t, x| x / 2)
        .exchange("by_val", |x: &u64| *x);
    let bigs = odds
        .filter("big", |_t, x| *x > 3)
        .exchange_by_hash("by_hash", |x: &u64| *x);
    let merged = instancy::Pipe::concat(vec![halves, bigs])?;
    let looped = merged.iterate::<u32>("loop", 1u32, |v| {
        let step = v.map("dec", |_t, x: u64| x.saturating_sub(1));
        let (again, done) = step.branch("done", |_t, x| *x > 0);
        instancy::IterateResult {
            feedback: again,
            output: done,
        }
    });
    looped.reduce("sum", |a, b| a + b).output("sum")?;
    let lens = side
        .broadcast("cfg")
        .map("len", |_t, s: String| s.len() as u64);
    input
        .gather("all")
        .binary(lens, "join", |left, right, output| {
            while let Some((t, d)) = left.next() {
                output.push_vec(t, d);
            }
            while let Some((t, d)) = right.next() {
                output.push_vec(t, d);
            }
            Ok(())
        })?
        .count("n")
        .output("count")?;
    Ok(())
}

/// Identical complex graphs built independently on every node must produce
/// identical fingerprints (no false mismatch from nondeterministic ordering).
async fn complex_identical_graphs_pass_handshake(mode: Mode) {
    let nodes = ["node-a", "node-b", "node-c"];
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 2),
        NodeConfig::new("node-b", 1),
        NodeConfig::new("node-c", 3),
    ])
    .unwrap();
    for _round in 0..3 {
        let mut mesh = tcp_mesh(&nodes).await;
        let df = DataflowId::new();
        let handles: Vec<_> = nodes
            .iter()
            .map(|n| {
                spawn_node(
                    mode,
                    topo.clone(),
                    n,
                    df,
                    mesh.remove(*n).unwrap(),
                    1024,
                    complex_graph,
                )
            })
            .collect();
        let mut results = Vec::new();
        for h in handles {
            let (rt, r) = tokio::time::timeout(SPAWN_BOUND, h).await.unwrap().unwrap();
            results.push((rt, r));
        }
        let errors: Vec<String> = results
            .iter()
            .filter_map(|(_, r)| r.as_ref().err().map(|e| e.to_string()))
            .collect();
        for (rt, r) in results {
            if let Ok(c) = r {
                c.cancel();
            }
            rt.shutdown();
        }
        assert!(errors.is_empty(), "identical graphs rejected: {errors:?}");
    }
}
both_modes!(
    complex_identical_graphs_pass_handshake_on_three_nodes,
    complex_identical_graphs_pass_handshake
);
