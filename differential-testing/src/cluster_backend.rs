//! instancy in cluster mode: every graph runs on two nodes, each with its own
//! instancy runtime, connected over loopback TCP with either dedicated
//! (per-dataflow) or shared (pooled) transport. The same native builders as
//! the in-process engine are used; the timely reference is unchanged.
//!
//! A node builds the dataflow once per local worker, so each worker owns a
//! probe whose view is global; the engine's view of a probe is the meet of
//! every worker's view on both nodes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::codec::ExchangeData;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Product, Result as IResult,
    RuntimeHandle, SpawnOptions, Timestamp,
};

use crate::instancy_backend::{
    Drain, Hooks, IInput, JoinState, ProbeView, Senders, build_nested, build_product_totals,
    build_totals, observe, output_drain, runtime,
};
use crate::{
    Action, CHECKPOINT, Coords, Engine, Frontier, INSTANCY_POOL_THREADS, INSTANCY_TOKIO_THREADS,
    Obs, Observe, Rec, Scenario, meet,
};

/// Bound on each graph's handshake and ready barrier.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
/// Threads of the Tokio runtime that carries the loopback connections.
pub const TRANSPORT_TOKIO_THREADS: usize = 2;
/// Per-connection channel capacity passed to `ClusterSpawnTransport`.
pub const TRANSPORT_CHANNEL_CAPACITY: usize = 1024;

/// `(total logical workers, instancy capacity)` for cluster seeds 1 through
/// 8. Two nodes need at least two workers; timely runs the same total.
pub const CLUSTER_SETTINGS: [(usize, usize); 8] = [
    (2, 1),
    (3, 1),
    (4, 1),
    (3, 2),
    (4, 2),
    (2, 1024),
    (3, 1024),
    (4, 1024),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Dedicated,
    Shared,
}

/// Odd seeds use dedicated connections, even seeds a shared pool.
pub fn transport_for(seed: u64) -> Transport {
    if seed % 2 == 1 {
        Transport::Dedicated
    } else {
        Transport::Shared
    }
}

/// Workers on `node-a` and `node-b`: 2 -> 1+1, 3 -> 1+2, 4 -> 2+2.
pub fn split(workers: usize) -> (usize, usize) {
    (workers / 2, workers - workers / 2)
}

type Halves = (OwnedReadHalf, OwnedWriteHalf);
type Managers = Arc<HashMap<String, SharedPeerManager>>;
type Spawned<T> = instancy::runtime::ClusterSpawnedDataflow<T>;

async fn tcp_pair() -> std::io::Result<(Halves, Halves)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (accepted, connected) = tokio::try_join!(listener.accept(), TcpStream::connect(addr))?;
    let accepted = accepted.0;
    accepted.set_nodelay(true)?;
    connected.set_nodelay(true)?;
    Ok((accepted.into_split(), connected.into_split()))
}

/// Hands out connections established before the pool asks for them.
struct PreEstablished(Mutex<VecDeque<Halves>>);

impl ConnectionFactory for PreEstablished {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer: &str,
    ) -> Result<(OwnedReadHalf, OwnedWriteHalf), Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "no more pre-established connections".into())
    }
}

fn manager(peer: &str, halves: Halves, handle: &tokio::runtime::Handle) -> Managers {
    let factory: Arc<dyn DynConnectionFactory> =
        Arc::new(PreEstablished(Mutex::new(VecDeque::from([halves]))));
    let config = SharedConnectionConfig {
        connections: 1,
        probe_interval: Duration::from_secs(3600),
        rtt_ema_alpha: 0.2,
        ..SharedConnectionConfig::default()
    };
    let manager = SharedPeerManager::new(
        peer.to_string(),
        config,
        factory,
        handle,
        tokio::sync::broadcast::channel(16).0,
    )
    .expect("shared peer manager");
    Arc::new(HashMap::from([(peer.to_string(), manager)]))
}

pub struct ClusterEngine {
    name: &'static str,
    transport: Transport,
    tokio: Option<tokio::runtime::Runtime>,
    nodes: Option<(RuntimeHandle, RuntimeHandle)>,
    /// The shared pool, created once and used by every graph.
    shared: Option<(Managers, Managers)>,
    workers: usize,
    inputs: Vec<Option<Box<dyn IInput>>>,
    probes: Vec<Vec<ProbeView>>,
    drains: Vec<Drain>,
    joins: Vec<JoinState>,
    receipts: Arc<Vec<AtomicU64>>,
}

impl ClusterEngine {
    pub fn new(sc: &Scenario, transport: Transport) -> Self {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(TRANSPORT_TOKIO_THREADS)
            .enable_all()
            .build()
            .expect("transport tokio runtime");
        let shared = (transport == Transport::Shared).then(|| {
            let handle = tokio.handle().clone();
            let (a, b) = tokio.block_on(tcp_pair()).expect("loopback pair");
            (manager("node-b", a, &handle), manager("node-a", b, &handle))
        });
        ClusterEngine {
            name: match transport {
                Transport::Dedicated => "instancy-cluster-dedicated",
                Transport::Shared => "instancy-cluster-shared",
            },
            transport,
            tokio: Some(tokio),
            nodes: Some((
                runtime(INSTANCY_POOL_THREADS, INSTANCY_TOKIO_THREADS),
                runtime(INSTANCY_POOL_THREADS, INSTANCY_TOKIO_THREADS),
            )),
            shared,
            workers: sc.workers,
            inputs: (0..sc.inputs.len()).map(|_| None).collect(),
            probes: (0..sc.probes.len()).map(|_| Vec::new()).collect(),
            drains: Vec::new(),
            joins: Vec::new(),
            receipts: Arc::new((0..sc.graphs).map(|_| AtomicU64::new(0)).collect()),
        }
    }

    /// Fresh dedicated connections for one graph.
    fn dedicated(
        &self,
    ) -> IResult<(
        ClusterSpawnTransport<OwnedReadHalf, OwnedWriteHalf>,
        ClusterSpawnTransport<OwnedReadHalf, OwnedWriteHalf>,
    )> {
        let tokio = self.tokio.as_ref().expect("tokio");
        let ((ra, wa), (rb, wb)) = tokio.block_on(tcp_pair()).map_err(|e| {
            instancy::Error::Dataflow(instancy::DataflowError::InvalidConfig(format!(
                "loopback: {e}"
            )))
        })?;
        Ok((
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-b".into(),
                    reader: ra,
                    writer: wa,
                }],
                TRANSPORT_CHANNEL_CAPACITY,
            ),
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-a".into(),
                    reader: rb,
                    writer: wb,
                }],
                TRANSPORT_CHANNEL_CAPACITY,
            ),
        ))
    }

    /// Spawn one graph on both nodes concurrently (they handshake).
    #[allow(clippy::too_many_arguments)]
    fn spawn_both<T, F, R, W>(
        &self,
        name: &str,
        topology: ClusterTopology,
        id: DataflowId,
        transports: (ClusterSpawnTransport<R, W>, ClusterSpawnTransport<R, W>),
        hooks: (&Hooks, &Hooks),
        build: &F,
    ) -> (IResult<Spawned<T>>, IResult<Spawned<T>>)
    where
        T: Timestamp + Coords + ExchangeData,
        F: Fn(&DataflowBuilder<T>, &Hooks) -> IResult<()>,
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let tokio = self.tokio.as_ref().expect("tokio");
        let handle = tokio.handle().clone();
        let (rt_a, rt_b) = self.nodes.as_ref().expect("nodes");
        let (ta, tb) = transports;
        tokio.block_on(async {
            tokio::join!(
                rt_a.spawn_cluster_async(
                    name,
                    topology.clone(),
                    "node-a",
                    id,
                    ta,
                    HANDSHAKE_TIMEOUT,
                    |b: &mut DataflowBuilder<T>| build(b, hooks.0),
                    &handle,
                    SpawnOptions::new(),
                ),
                rt_b.spawn_cluster_async(
                    name,
                    topology,
                    "node-b",
                    id,
                    tb,
                    HANDSHAKE_TIMEOUT,
                    |b: &mut DataflowBuilder<T>| build(b, hooks.1),
                    &handle,
                    SpawnOptions::new(),
                ),
            )
        })
    }

    /// Spawn graph `graph` on both nodes, with the same dataflow id and
    /// topology, and wire its global workers in topology order.
    pub fn spawn_graph<T, F>(
        &mut self,
        sc: &Scenario,
        graph: usize,
        mode: &Observe,
        build: F,
    ) -> IResult<()>
    where
        T: Timestamp + Coords + ExchangeData,
        F: Fn(&DataflowBuilder<T>, &Hooks) -> IResult<()>,
    {
        let (wa, wb) = split(self.workers);
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", wa),
            NodeConfig::new("node-b", wb),
        ])?;
        let id = DataflowId::new();
        let hooks_a = Hooks::new(graph, mode, sc.capacity, &self.receipts);
        let hooks_b = Hooks::new(graph, mode, sc.capacity, &self.receipts);
        let name = format!("{}-g{graph}", sc.recipe);
        let hooks = (&hooks_a, &hooks_b);
        let (a, b) = match &self.shared {
            Some((ma, mb)) => {
                let transports = (
                    ClusterSpawnTransport::shared(Arc::clone(ma), TRANSPORT_CHANNEL_CAPACITY),
                    ClusterSpawnTransport::shared(Arc::clone(mb), TRANSPORT_CHANNEL_CAPACITY),
                );
                self.spawn_both(&name, topology, id, transports, hooks, &build)
            }
            None => {
                let transports = self.dedicated()?;
                self.spawn_both(&name, topology, id, transports, hooks, &build)
            }
        };
        let (mut a, mut b) = (a?, b?);
        let mut senders: Vec<Vec<Option<instancy::InputSender<T, Rec>>>> = sc
            .inputs
            .iter()
            .map(|_| (0..self.workers).map(|_| None).collect())
            .collect();
        for (node, hooks) in [(&mut a, hooks_a), (&mut b, hooks_b)] {
            let base = node.local_worker_range().0;
            let mut outputs = hooks.outputs.into_inner();
            outputs.sort();
            outputs.dedup();
            for local in 0..node.num_local_workers() {
                let global = base + local;
                for (i, spec) in sc.inputs.iter().enumerate() {
                    if spec.graph == graph {
                        senders[i][global] = Some(node.take_input::<Rec>(local, &spec.name)?);
                    }
                }
                for (probe, name) in &outputs {
                    let rx = node.take_output(local, name)?;
                    self.drains.push(output_drain::<T>(*probe, global, rx));
                }
            }
            for (p, view) in hooks.probes.into_inner() {
                self.probes[p].push(view);
            }
        }
        for (i, spec) in sc.inputs.iter().enumerate() {
            if spec.graph == graph {
                let all = std::mem::take(&mut senders[i])
                    .into_iter()
                    .map(|s| s.expect("a sender per global worker"))
                    .collect();
                self.inputs[i] = Some(Box::new(Senders(all)));
            }
        }
        self.joins.push(JoinState::Pending(Box::new(move || {
            let ja = std::thread::spawn(move || a.join_blocking());
            let jb = std::thread::spawn(move || b.join_blocking());
            let ra = ja.join().map_err(|_| join_panic("node-a"))?;
            let rb = jb.join().map_err(|_| join_panic("node-b"))?;
            ra.and(rb)
        })));
        Ok(())
    }
}

fn join_panic(node: &str) -> instancy::Error {
    instancy::Error::Dataflow(instancy::DataflowError::InvalidConfig(format!(
        "{node} join panicked"
    )))
}

impl Engine for ClusterEngine {
    fn name(&self) -> &'static str {
        self.name
    }

    fn apply(&mut self, action: &Action) -> Result<(), String> {
        match action {
            Action::Send {
                input,
                worker,
                time,
                records,
            } => self.inputs[*input].as_mut().ok_or("unknown input")?.send(
                *worker,
                time,
                records.clone(),
            ),
            Action::Advance { input, time } => self.inputs[*input]
                .as_mut()
                .ok_or("unknown input")?
                .advance(time),
            Action::Close { input } => {
                self.inputs[*input].as_mut().ok_or("unknown input")?.close();
                Ok(())
            }
        }
    }

    fn frontier(&mut self, probe: usize) -> Result<Option<Frontier>, String> {
        let views = &self.probes[probe];
        if views.len() != self.workers {
            return Err(format!(
                "probe {probe} has {} worker views, expected {}",
                views.len(),
                self.workers
            ));
        }
        Ok(Some(meet(&views.iter().map(|v| v()).collect::<Vec<_>>())))
    }

    fn receipts(&self, graph: usize) -> u64 {
        self.receipts[graph].load(Ordering::SeqCst)
    }

    fn graph_completed(&mut self, graph: usize) -> Result<bool, String> {
        self.joins[graph].poll()
    }

    fn drain(&mut self, into: &mut Vec<Obs>) -> Result<(), String> {
        for drain in &mut self.drains {
            drain(into)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), String> {
        for input in self.inputs.iter_mut().flatten() {
            input.close();
        }
        let deadline = std::time::Instant::now() + CHECKPOINT + Duration::from_secs(1);
        loop {
            let mut all = true;
            for g in 0..self.joins.len() {
                match self.joins[g].poll() {
                    Ok(done) => all &= done,
                    Err(e) => {
                        self.abandon();
                        return Err(format!("graph {g}: {e}"));
                    }
                }
            }
            if all {
                break;
            }
            if std::time::Instant::now() > deadline {
                self.abandon();
                return Err("cluster dataflows did not join".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut rest = Vec::new();
        if let Err(e) = self.drain(&mut rest) {
            self.abandon();
            return Err(e);
        }
        if !rest.is_empty() {
            self.abandon();
            return Err(format!("records after the final cut: {rest:?}"));
        }
        if let Some((a, b)) = self.nodes.take() {
            a.shutdown();
            b.shutdown();
        }
        self.shared = None;
        if let Some(tokio) = self.tokio.take() {
            tokio.shutdown_timeout(Duration::from_secs(1));
        }
        Ok(())
    }

    fn abandon(self: Box<Self>) {
        // Failure-only: runtimes, transport and live dataflows are never joined.
        std::mem::forget(self);
    }
}

impl ClusterEngine {
    pub fn transport(&self) -> Transport {
        self.transport
    }
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

pub fn start_totals(sc: &Scenario, mode: Observe, transport: Transport) -> Box<dyn Engine> {
    let mut engine = ClusterEngine::new(sc, transport);
    engine
        .spawn_graph::<u64, _>(sc, 0, &mode, |b, hooks| {
            let totals = build_totals(b, hooks)?;
            observe(totals, 0, "totals", hooks)
        })
        .expect("spawn cluster totals");
    Box::new(engine)
}

pub fn start_product(sc: &Scenario, mode: Observe, transport: Transport) -> Box<dyn Engine> {
    let mut engine = ClusterEngine::new(sc, transport);
    engine
        .spawn_graph::<Product<u64, u64>, _>(sc, 0, &mode, |b, hooks| {
            let totals = build_product_totals(b, hooks)?;
            observe(totals, 0, "totals", hooks)
        })
        .expect("spawn cluster product totals");
    Box::new(engine)
}

pub fn start_nested(sc: &Scenario, mode: Observe, transport: Transport) -> Box<dyn Engine> {
    let mut engine = ClusterEngine::new(sc, transport);
    for g in 0..sc.graphs {
        let probes = (
            sc.probe_index(g, "inner-totals"),
            sc.probe_index(g, "root-totals"),
        );
        let outer = sc.outer_iterations;
        engine
            .spawn_graph::<u64, _>(sc, g, &mode, move |b, hooks| {
                build_nested(b, hooks, probes, outer)
            })
            .expect("spawn cluster nested");
    }
    Box::new(engine)
}

pub fn start_breadth(sc: &Scenario, mode: Observe, transport: Transport) -> Box<dyn Engine> {
    let mut engine = ClusterEngine::new(sc, transport);
    for g in 0..sc.graphs {
        let base = sc.probe_index(g, "broadcast-totals");
        engine
            .spawn_graph::<u64, _>(sc, g, &mode, move |b, hooks| {
                hooks.set_probe_base(base);
                crate::instancy_backend::build_breadth(b, hooks)
            })
            .expect("spawn cluster breadth");
    }
    Box::new(engine)
}

pub fn start_program(sc: &Scenario, mode: Observe, transport: Transport) -> Box<dyn Engine> {
    let mut engine = ClusterEngine::new(sc, transport);
    let program = sc.program().clone();
    let base = sc.probe_index(0, "pass");
    engine
        .spawn_graph::<u64, _>(sc, 0, &mode, move |b, hooks| {
            hooks.set_probe_base(base);
            crate::instancy_backend::build_program(b, hooks, &program)
        })
        .expect("spawn cluster program");
    Box::new(engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instancy_backend::Wire;
    use instancy::communication::codec::Codec;

    #[test]
    fn wire_records_round_trip_and_reject_truncation() {
        for record in [
            Wire((vec![], vec![3, -8])),
            Wire((vec![7, 2, 1], vec![4, -5, 9])),
        ] {
            let codec = Wire::codec();
            let mut buf = Vec::new();
            codec.encode(&record, &mut buf).unwrap();
            buf.push(0xAA);
            let (back, used) = codec.decode(&buf).unwrap();
            assert_eq!(back, record);
            assert_eq!(used, buf.len() - 1);
            assert!(codec.decode(&buf[..used - 1]).is_err());
        }
    }

    #[test]
    fn every_cluster_setting_splits_across_two_nodes() {
        for (workers, _) in CLUSTER_SETTINGS {
            let (a, b) = split(workers);
            assert!(a >= 1 && b >= 1 && a + b == workers);
        }
        assert_eq!(transport_for(1), Transport::Dedicated);
        assert_eq!(transport_for(2), Transport::Shared);
    }
}
