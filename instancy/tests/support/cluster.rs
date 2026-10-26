//! Loopback-TCP cluster harness for exchange/broadcast backpressure tests.
//!
//! Spawns one dataflow on every node of a cluster wired with real TCP
//! connections, in either Dedicated or Shared transport mode, feeds input
//! batches into chosen workers, and collects every worker's output. Joins are
//! bounded by a timeout and never block the test on a hung cluster.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

type Transport = ClusterSpawnTransport<OwnedReadHalf, OwnedWriteHalf>;

/// Input batches: `(time, records)`.
pub type Batches = Vec<(u64, Vec<u64>)>;
/// Batches per global worker index.
pub type WorkerInputs = Vec<(usize, Batches)>;
type SharedManagers = Arc<HashMap<String, SharedPeerManager>>;

/// Transport mode between nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// One dedicated TCP connection per peer for the dataflow.
    Dedicated,
    /// Pooled connections owned by long-lived `SharedPeerManager`s. The
    /// per-peer payload queue holds 1024 frames regardless of `capacity`.
    Shared,
}

/// Cluster shape and transport settings.
#[derive(Clone, Debug)]
pub struct ClusterSpec {
    /// Workers per node; node `i` is named `node-{i}`.
    pub workers: Vec<usize>,
    pub mode: Mode,
    /// Transport channel capacity (Dedicated: the per-peer frame queue).
    pub capacity: usize,
    /// Upper bound for every node's join.
    pub timeout: Duration,
    /// Keep every input open until this many output records have arrived (or
    /// the timeout passes), so no node can finish while records are still on
    /// their way to another node.
    ///
    /// Sidesteps a known premature-completion bug, tracked separately (audit
    /// C1/H1): once the inputs close, a worker can declare completion while
    /// records are still in flight or held by a blocked operator.
    pub hold_inputs_until: Option<usize>,
}

impl ClusterSpec {
    pub fn new(workers: &[usize], mode: Mode, capacity: usize) -> Self {
        Self {
            workers: workers.to_vec(),
            mode,
            capacity,
            timeout: Duration::from_secs(if mode == Mode::Shared { 30 } else { 60 }),
            hold_inputs_until: None,
        }
    }

    pub fn total_workers(&self) -> usize {
        self.workers.iter().sum()
    }

    fn node_name(i: usize) -> String {
        format!("node-{i}")
    }
}

/// Result of one cluster run.
pub struct Outcome {
    mode: Mode,
    /// Join result per node (`Err` holds a description, including timeouts).
    pub joins: Vec<std::result::Result<(), String>>,
    /// Output records per global worker, in arrival order.
    pub outputs: Vec<Vec<(u64, u64)>>,
}

impl Outcome {
    pub fn all_joined(&self) -> bool {
        self.joins.iter().all(|j| j.is_ok())
    }

    /// Every output record of every worker, sorted.
    pub fn all_records(&self) -> Vec<(u64, u64)> {
        let mut all: Vec<(u64, u64)> = self.outputs.iter().flatten().copied().collect();
        all.sort_unstable();
        all
    }

    /// Panics unless every node joined and the union of all outputs equals
    /// `expected` (sorted) as a multiset.
    pub fn assert_exactly(&self, expected: &[(u64, u64)], what: &str) {
        assert_multiset_eq(&self.all_records(), expected, what);
        self.assert_joined(what);
    }

    /// Panics unless every node joined and every worker received `expected`
    /// (sorted) as a multiset.
    pub fn assert_each_worker_exactly(&self, expected: &[(u64, u64)], what: &str) {
        for (w, out) in self.outputs.iter().enumerate() {
            let mut got = out.clone();
            got.sort_unstable();
            assert_multiset_eq(&got, expected, &format!("{what}: worker {w}"));
        }
        self.assert_joined(what);
    }

    fn assert_joined(&self, what: &str) {
        assert!(self.all_joined(), "{what}: joins = {:?}", self.joins);
    }
}

const TIMEOUT: &str = "did not complete";

/// Compares two sorted multisets and reports missing and duplicated records.
pub fn assert_multiset_eq(got: &[(u64, u64)], expected: &[(u64, u64)], what: &str) {
    if got == expected {
        return;
    }
    let mut counts: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    for r in got {
        *counts.entry(*r).or_default() += 1;
    }
    for r in expected {
        *counts.entry(*r).or_default() -= 1;
    }
    let extra: Vec<_> = counts.iter().filter(|(_, c)| **c > 0).collect();
    let missing: Vec<_> = counts.iter().filter(|(_, c)| **c < 0).collect();
    panic!(
        "{what}: got {} records, expected {}; {} duplicated or unexpected (sample {:?}), {} missing (sample {:?})",
        got.len(),
        expected.len(),
        extra.len(),
        &extra[..extra.len().min(5)],
        missing.len(),
        &missing[..missing.len().min(5)],
    );
}

/// `batches` batches of `per_batch` distinct records at time 0, numbered from 0.
pub fn numbered_batches(batches: u64, per_batch: u64) -> Batches {
    (0..batches)
        .map(|b| (0, (b * per_batch..(b + 1) * per_batch).collect()))
        .collect()
}

/// The `(time, record)` pairs contained in `batches`, sorted.
pub fn expected_records(batches: &[(u64, Vec<u64>)]) -> Vec<(u64, u64)> {
    let mut all: Vec<(u64, u64)> = batches
        .iter()
        .flat_map(|(t, d)| d.iter().map(move |x| (*t, *x)))
        .collect();
    all.sort_unstable();
    all
}

/// Runs `build` on every node. The dataflow must have a `u64` input named
/// `"in"` and a `u64` output named `"out"`. `inputs` maps a global worker
/// index to the batches fed into that worker; all other inputs are closed
/// immediately (or once `hold_inputs_until` is satisfied).
pub async fn run<F>(spec: &ClusterSpec, build: F, inputs: WorkerInputs) -> Outcome
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static,
{
    let topology = ClusterTopology::multi_node(
        spec.workers
            .iter()
            .enumerate()
            .map(|(i, w)| NodeConfig::new(ClusterSpec::node_name(i), *w))
            .collect(),
    )
    .unwrap();
    let (transports, managers) = transports(spec).await;
    let dataflow_id = DataflowId::new();
    let tokio_handle = tokio::runtime::Handle::current();

    let mut spawns = Vec::new();
    for (i, transport) in transports.into_iter().enumerate() {
        let topology = topology.clone();
        let tokio_handle = tokio_handle.clone();
        let build = build.clone();
        spawns.push(tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..RuntimeConfig::default()
            })?;
            let cluster = rt.spawn_cluster(
                "backpressure",
                topology,
                &ClusterSpec::node_name(i),
                dataflow_id,
                transport,
                Duration::from_secs(20),
                build,
                &tokio_handle,
                SpawnOptions::new(),
            )?;
            Ok::<_, instancy::Error>((rt, cluster))
        }));
    }
    let mut nodes: Vec<(RuntimeHandle, ClusterSpawnedDataflow<u64>)> = Vec::new();
    for s in spawns {
        nodes.push(s.await.unwrap().unwrap());
    }

    let mut outputs = Vec::new();
    for (_, cluster) in nodes.iter_mut() {
        for local in 0..cluster.num_local_workers() {
            let rx = cluster.take_output::<u64>(local, "out").unwrap();
            let sink = Arc::new(Mutex::new(Vec::new()));
            let thread_sink = Arc::clone(&sink);
            std::thread::spawn(move || {
                while let Some(event) = rx.recv() {
                    if let Some((t, data)) = event.as_data() {
                        thread_sink
                            .lock()
                            .unwrap()
                            .extend(data.iter().map(|x| (*t, *x)));
                    }
                }
            });
            outputs.push(sink);
        }
    }

    // Blocks until the outputs hold `expected` records in total (or the
    // timeout passes); a no-op without `hold_inputs_until`.
    let hold = {
        let sinks: Vec<_> = outputs.iter().map(Arc::clone).collect();
        let expected = spec.hold_inputs_until;
        let give_up = Instant::now() + spec.timeout;
        move || {
            let Some(expected) = expected else { return };
            while Instant::now() < give_up
                && sinks.iter().map(|s| s.lock().unwrap().len()).sum::<usize>() < expected
            {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    };

    let mut inputs: HashMap<usize, Batches> = inputs.into_iter().collect();
    let mut global = 0;
    for (_, cluster) in nodes.iter_mut() {
        for local in 0..cluster.num_local_workers() {
            let tx = cluster.take_input::<u64>(local, "in").unwrap();
            let batches = inputs.remove(&global).unwrap_or_default();
            let hold = hold.clone();
            // A plain thread: `send` blocks while the input is full and must
            // not hide a wedged dataflow from the join timeout.
            std::thread::spawn(move || {
                for (t, data) in batches {
                    if tx.send(t, data).is_err() {
                        return;
                    }
                }
                hold();
                drop(tx);
            });
            global += 1;
        }
    }
    assert!(
        inputs.is_empty(),
        "inputs for unknown workers: {:?}",
        inputs.keys()
    );
    drop(hold);

    let timeout = spec.timeout;
    let deadline = Instant::now() + timeout;
    let mut waits = Vec::new();
    let mut runtimes = Vec::new();
    for (rt, cluster) in nodes {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(cluster.join_blocking());
        });
        waits.push(rx);
        runtimes.push(rt);
    }
    let joins: Vec<std::result::Result<(), String>> = tokio::task::spawn_blocking(move || {
        waits
            .into_iter()
            .map(
                |rx| match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("join failed: {e}")),
                    Err(_) => Err(format!("{TIMEOUT} within {timeout:?}")),
                },
            )
            .collect()
    })
    .await
    .unwrap();

    if joins.iter().all(|j| j.is_ok()) {
        // Outputs close once their dataflow completes; let the drain threads
        // pick up the last events.
        let settle = Instant::now() + Duration::from_secs(5);
        while Instant::now() < settle && outputs.iter().any(|o| Arc::strong_count(o) > 1) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(runtimes);
        drop(managers);
    } else {
        // A wedged dataflow can block runtime or transport teardown forever.
        std::mem::forget(runtimes);
        std::mem::forget(managers);
    }

    Outcome {
        mode: spec.mode,
        joins,
        outputs: outputs.iter().map(|o| o.lock().unwrap().clone()).collect(),
    }
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.0, connected)
}

struct PreEstablished {
    connections: Mutex<VecDeque<(OwnedReadHalf, OwnedWriteHalf)>>,
}

impl ConnectionFactory for PreEstablished {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.connections.lock().unwrap().pop_front().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "no more pre-established connections",
            ))
        })
    }
}

fn shared_config(connections: usize) -> SharedConnectionConfig {
    SharedConnectionConfig {
        connections,
        probe_interval: Duration::from_secs(3600),
        rtt_ema_alpha: 0.2,
        ..SharedConnectionConfig::default()
    }
}

/// Builds one transport per node. In Shared mode the peer managers are also
/// returned so they outlive the dataflow on every node, as long-lived managers
/// do in a real deployment.
async fn transports(spec: &ClusterSpec) -> (Vec<Transport>, Vec<SharedManagers>) {
    let n = spec.workers.len();
    match spec.mode {
        Mode::Dedicated => {
            let mut conns: Vec<Vec<PeerConnection<OwnedReadHalf, OwnedWriteHalf>>> =
                (0..n).map(|_| Vec::new()).collect();
            for i in 0..n {
                for j in (i + 1)..n {
                    let (a, b) = tcp_pair().await;
                    let (ra, wa) = a.into_split();
                    let (rb, wb) = b.into_split();
                    conns[i].push(PeerConnection {
                        node_id: ClusterSpec::node_name(j),
                        reader: ra,
                        writer: wa,
                    });
                    conns[j].push(PeerConnection {
                        node_id: ClusterSpec::node_name(i),
                        reader: rb,
                        writer: wb,
                    });
                }
            }
            let transports = conns
                .into_iter()
                .map(|c| ClusterSpawnTransport::dedicated(c, spec.capacity))
                .collect();
            (transports, Vec::new())
        }
        Mode::Shared => {
            const CONNECTIONS: usize = 2;
            let handle = tokio::runtime::Handle::current();
            let mut managers: Vec<HashMap<String, SharedPeerManager>> =
                (0..n).map(|_| HashMap::new()).collect();
            for i in 0..n {
                for j in (i + 1)..n {
                    let mut ci = VecDeque::new();
                    let mut cj = VecDeque::new();
                    for _ in 0..CONNECTIONS {
                        let (a, b) = tcp_pair().await;
                        ci.push_back(a.into_split());
                        cj.push_back(b.into_split());
                    }
                    for (me, peer, conns) in [(i, j, ci), (j, i, cj)] {
                        let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablished {
                            connections: Mutex::new(conns),
                        });
                        let manager = SharedPeerManager::new(
                            ClusterSpec::node_name(peer),
                            shared_config(CONNECTIONS),
                            factory,
                            &handle,
                            tokio::sync::broadcast::channel(16).0,
                        )
                        .unwrap();
                        managers[me].insert(ClusterSpec::node_name(peer), manager);
                    }
                }
            }
            let managers: Vec<SharedManagers> = managers.into_iter().map(Arc::new).collect();
            let transports = managers
                .iter()
                .map(|m| Transport::Shared {
                    peer_managers: Arc::clone(m),
                    capacity: spec.capacity,
                })
                .collect();
            (transports, managers)
        }
    }
}
