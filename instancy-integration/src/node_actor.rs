//! DataflowAgent — dactor actor that manages instancy runtimes in a node process.
//!
//! Handles control commands from the coordinator: binding listeners, connecting
//! to peers, spawning cluster dataflows, feeding data, collecting output, and
//! membership events.
//!
//! Dataflows run over one of two transports:
//!
//! - Dedicated: `BindListener`, `ConnectPeers` and `SpawnDataflow` set up
//!   connections for one dataflow.
//! - Shared: the node binds one peer listener for its life
//!   (`BindPeerListener`), and keeps one set of `SharedPeerManager`s per node
//!   set, created by the first `SpawnShared` on that node set and shared by
//!   every later one. A spawn needs managers that match its topology's peers
//!   exactly, managers can't be cloned, and dropping one closes its pool:
//!   hence one set per node set, kept until `Shutdown` drops them all.
//!
//! dactor runs handlers one at a time. So a handler only touches the actor's
//! state and starts any long work (peer handshakes, spawns, sends that may
//! block) as a task. Its reply is a receiver that the caller awaits outside
//! the mailbox, which keeps the node serving commands while that work runs.

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dactor::message::Message;
use dactor::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use instancy::communication::transport_session::PeerConnection;
use instancy::communication::{ClusterSpawnTransport, SharedConnectionConfig, SharedPeerManager};
use instancy::dataflow::channel_operators::InputSender;
use instancy::dataflow::id::DataflowId;
use instancy::dataflow::OutputEvent;
use instancy::runtime::{ClusterSpawnedDataflow, RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::{ClusterTopology, NodeConfig};

use crate::dataflows;
use crate::peer_link::{node_set_key, PeerLinks};
use crate::protocol::*;

/// Handshake and ready-barrier timeout passed to `spawn_cluster`, unless the
/// coordinator gives one.
pub const SPAWN_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Channel capacity of every spawn's transport.
const TRANSPORT_CAPACITY: usize = 64;

// ---------------------------------------------------------------------------
// Messages for the actor
// ---------------------------------------------------------------------------

/// Wrapper message carrying a control command from the coordinator.
///
/// The reply resolves to the command's response once any work the command
/// started has finished.
pub struct HandleCommand {
    pub command: NodeCommand,
}

impl Message for HandleCommand {
    type Reply = oneshot::Receiver<NodeResponse>;
}

// ---------------------------------------------------------------------------
// Per-dataflow state
// ---------------------------------------------------------------------------

struct ListenerState {
    listener: TcpListener,
    topology: SerializableTopology,
}

type DedicatedPeer =
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

/// Tracks peer connections established for a dataflow before spawn.
struct ConnectionState {
    topology: SerializableTopology,
    connections: Vec<DedicatedPeer>,
}

/// Type-erased input senders, keyed by (worker_local_index, port_name).
type InputSenders = HashMap<(usize, String), Box<dyn Any + Send>>;

/// Output drained from one port so far: `(timestamp, bincode-serialized batch)`.
type OutputBuffer = Arc<Mutex<Vec<(u64, Vec<u8>)>>>;

/// A send to an input port, prepared by the actor and run off its mailbox.
type FeedJob = Box<dyn FnOnce() -> std::result::Result<(), String> + Send>;

/// State for a spawned dataflow. Kept after it completes, so its output can
/// still be collected.
struct ActiveDataflow {
    /// What type of dataflow this is (needed for type-aware I/O).
    dataflow_type: DataflowType,
    /// The spawn's cancellation token.
    cancel: CancellationToken,
    inputs: Mutex<InputSenders>,
    /// Keyed by (worker_local_index, port_name). Filled by one thread per port.
    outputs: HashMap<(usize, String), OutputBuffer>,
}

type ActiveDataflows = Arc<Mutex<HashMap<String, Arc<ActiveDataflow>>>>;

/// The managers every dataflow on one node set shares, keyed by peer ID.
type PeerManagers = Arc<HashMap<String, SharedPeerManager>>;

/// What `BindPeerListener` and `ConfigurePeers` set up for the Shared transport.
struct SharedTransport {
    links: PeerLinks,
    /// Pool size of the manager sets created from now on; set by `ConfigurePeers`.
    connections: Option<usize>,
    /// Node-set key → its managers, kept until `Shutdown`.
    node_sets: HashMap<String, PeerManagers>,
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

pub struct DataflowAgent {
    node_id: String,
    runtime: Arc<RuntimeHandle>,
    tokio_handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<NodeEvent>,
    listeners: HashMap<String, ListenerState>,
    connections: Arc<Mutex<HashMap<String, ConnectionState>>>,
    active: ActiveDataflows,
    shared: Option<SharedTransport>,
}

impl Actor for DataflowAgent {
    type Args = DataflowAgentArgs;
    type Deps = ();

    fn create(args: Self::Args, _deps: ()) -> Self {
        let config = RuntimeConfig {
            worker_threads: args.worker_threads,
            ..Default::default()
        };
        let runtime = RuntimeHandle::new(config).expect("failed to create instancy runtime");
        Self {
            node_id: args.node_id,
            runtime: Arc::new(runtime),
            tokio_handle: args.tokio_handle,
            events: args.events,
            listeners: HashMap::new(),
            connections: Arc::default(),
            active: Arc::default(),
            shared: None,
        }
    }
}

pub struct DataflowAgentArgs {
    pub node_id: String,
    pub worker_threads: usize,
    pub tokio_handle: tokio::runtime::Handle,
    /// Where the agent reports events such as a dataflow's completion.
    pub events: mpsc::UnboundedSender<NodeEvent>,
}

#[async_trait]
impl Handler<HandleCommand> for DataflowAgent {
    async fn handle(
        &mut self,
        msg: HandleCommand,
        _ctx: &mut ActorContext,
    ) -> oneshot::Receiver<NodeResponse> {
        let (reply, response) = oneshot::channel();
        self.handle_command(msg.command, reply).await;
        response
    }
}

impl DataflowAgent {
    async fn handle_command(&mut self, cmd: NodeCommand, reply: oneshot::Sender<NodeResponse>) {
        let response = match cmd {
            NodeCommand::BindListener {
                dataflow_id,
                topology,
            } => self.handle_bind_listener(dataflow_id, topology).await,

            NodeCommand::ConnectPeers {
                dataflow_id,
                peer_addresses,
            } => return self.start_connect_peers(dataflow_id, peer_addresses, reply),

            NodeCommand::SpawnDataflow {
                dataflow_id,
                dataflow_type,
            } => return self.start_spawn_dataflow(dataflow_id, dataflow_type, reply),

            NodeCommand::FeedData {
                dataflow_id,
                worker_idx,
                port_name,
                timestamp,
                data,
            } => {
                return self.start_feed_data(
                    dataflow_id,
                    (worker_idx, port_name),
                    timestamp,
                    data,
                    reply,
                )
            }

            NodeCommand::CloseInputs {
                dataflow_id,
                worker_idx,
            } => self.handle_close_inputs(&dataflow_id, worker_idx),

            NodeCommand::CollectOutput {
                dataflow_id,
                worker_idx,
                port_name,
            } => self.handle_collect_output(&dataflow_id, (worker_idx, port_name)),

            NodeCommand::CancelDataflow { dataflow_id } => {
                self.handle_cancel_dataflow(&dataflow_id)
            }

            NodeCommand::BindPeerListener => self.handle_bind_peer_listener().await,

            NodeCommand::ConfigurePeers {
                peers,
                incarnation,
                connections,
            } => self.handle_configure_peers(peers, incarnation, connections),

            NodeCommand::SpawnShared {
                dataflow_id,
                dataflow_type,
                topology,
                handshake_timeout_ms,
            } => {
                return self.start_spawn_shared(
                    dataflow_id,
                    dataflow_type,
                    topology,
                    Duration::from_millis(handshake_timeout_ms),
                    reply,
                )
            }

            NodeCommand::ReportNodeLeave { node_id } => NodeResponse::NodeLeaveReported {
                cancelled: self.runtime.report_node_leave(&node_id),
            },

            NodeCommand::ReportNodeJoin { node_id } => NodeResponse::NodeJoinReported {
                was_left: self.runtime.report_node_join(&node_id),
            },

            NodeCommand::SnapshotPools => NodeResponse::Pools {
                pools: self.pool_snapshots(),
            },

            NodeCommand::SetShutdownBehavior(_) => NodeResponse::Error {
                message: "SetShutdownBehavior is handled by the node process, not its agent".into(),
            },

            NodeCommand::Shutdown => return self.start_shutdown(reply),
        };
        let _ = reply.send(response);
    }

    /// Drop this node's Shared-transport managers (instancy closes their
    /// connections in the background once no running dataflow holds them),
    /// then shut the runtime down and wait until every dataflow has ended.
    /// Replies once it has. The process exits right after the reply, which
    /// can cut the managers' close short.
    fn start_shutdown(&mut self, reply: oneshot::Sender<NodeResponse>) {
        if let Some(shared) = self.shared.as_mut() {
            shared.node_sets.clear();
        }
        let runtime = Arc::clone(&self.runtime);
        tokio::spawn(async move {
            runtime.shutdown_async().await;
            let _ = reply.send(NodeResponse::Error {
                message: "shutdown".into(),
            });
        });
    }

    /// Every Shared-transport manager this node holds, ordered by node set
    /// and peer, with its live connections (by id, as instancy lists them).
    fn pool_snapshots(&self) -> Vec<PoolSnapshot> {
        let Some(shared) = &self.shared else {
            return Vec::new();
        };
        let mut pools: Vec<PoolSnapshot> = shared
            .node_sets
            .iter()
            .flat_map(|(node_set, managers)| {
                managers.iter().map(move |(peer, manager)| PoolSnapshot {
                    node_set: node_set.clone(),
                    peer: peer.clone(),
                    connections: manager
                        .metrics_snapshot()
                        .into_iter()
                        .map(|conn| PooledConnection {
                            id: conn.id,
                            queued_frames: conn.queued_frames,
                            pinned_lanes: conn.pinned_lanes,
                            total_frames: conn.total_frames,
                            total_bytes: conn.total_bytes,
                        })
                        .collect(),
                })
            })
            .collect();
        pools.sort_by(|x, y| (&x.node_set, &x.peer).cmp(&(&y.node_set, &y.peer)));
        pools
    }

    fn active_dataflow(
        &self,
        dataflow_id: &str,
    ) -> std::result::Result<Arc<ActiveDataflow>, String> {
        self.active
            .lock()
            .unwrap()
            .get(dataflow_id)
            .cloned()
            .ok_or_else(|| format!("no active dataflow {dataflow_id}"))
    }

    async fn handle_bind_listener(
        &mut self,
        dataflow_id: String,
        topology: SerializableTopology,
    ) -> NodeResponse {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => {
                let addr = listener.local_addr().unwrap();
                self.listeners
                    .insert(dataflow_id.clone(), ListenerState { listener, topology });
                NodeResponse::ListenerReady {
                    dataflow_id,
                    listen_addr: addr,
                }
            }
            Err(e) => NodeResponse::Error {
                message: format!("bind failed: {e}"),
            },
        }
    }

    fn start_connect_peers(
        &mut self,
        dataflow_id: String,
        peer_addresses: HashMap<String, SocketAddr>,
        reply: oneshot::Sender<NodeResponse>,
    ) {
        let Some(listener_state) = self.listeners.remove(&dataflow_id) else {
            let _ = reply.send(NodeResponse::Error {
                message: format!("no listener for dataflow {dataflow_id}"),
            });
            return;
        };
        let node_id = self.node_id.clone();
        let connections = Arc::clone(&self.connections);
        tokio::spawn(async move {
            let response =
                match connect_peers(&node_id, &listener_state.listener, &peer_addresses).await {
                    Ok(peers) => {
                        connections.lock().unwrap().insert(
                            dataflow_id.clone(),
                            ConnectionState {
                                topology: listener_state.topology,
                                connections: peers,
                            },
                        );
                        NodeResponse::PeersConnected { dataflow_id }
                    }
                    Err(message) => NodeResponse::Error { message },
                };
            let _ = reply.send(response);
        });
    }

    fn start_spawn_dataflow(
        &mut self,
        dataflow_id: String,
        dataflow_type: DataflowType,
        reply: oneshot::Sender<NodeResponse>,
    ) {
        let conn_state = self.connections.lock().unwrap().remove(&dataflow_id);
        let Some(conn_state) = conn_state else {
            let _ = reply.send(NodeResponse::Error {
                message: format!("no connections for dataflow {dataflow_id}"),
            });
            return;
        };
        let spawner = self.spawner();
        tokio::spawn(async move {
            let transport =
                ClusterSpawnTransport::dedicated(conn_state.connections, TRANSPORT_CAPACITY);
            let response = spawner
                .spawn(
                    dataflow_id,
                    dataflow_type,
                    conn_state.topology,
                    transport,
                    SPAWN_HANDSHAKE_TIMEOUT,
                )
                .await;
            let _ = reply.send(response);
        });
    }

    fn spawner(&self) -> Spawner {
        Spawner {
            node_id: self.node_id.clone(),
            runtime: Arc::clone(&self.runtime),
            tokio_handle: self.tokio_handle.clone(),
            events: self.events.clone(),
            active: Arc::clone(&self.active),
        }
    }

    async fn handle_bind_peer_listener(&mut self) -> NodeResponse {
        if self.shared.is_some() {
            return NodeResponse::Error {
                message: "the peer listener is already bound".into(),
            };
        }
        // The incarnation comes with ConfigurePeers, before anything is dialed.
        match PeerLinks::start(&self.node_id, 0).await {
            Ok((links, addr)) => {
                self.shared = Some(SharedTransport {
                    links,
                    connections: None,
                    node_sets: HashMap::new(),
                });
                NodeResponse::PeerListenerReady { addr }
            }
            Err(e) => NodeResponse::Error {
                message: format!("binding the peer listener failed: {e}"),
            },
        }
    }

    fn handle_configure_peers(
        &mut self,
        peers: HashMap<String, SocketAddr>,
        incarnation: u64,
        connections: usize,
    ) -> NodeResponse {
        let Some(shared) = self.shared.as_mut() else {
            return NodeResponse::Error {
                message: "ConfigurePeers before BindPeerListener".into(),
            };
        };
        shared.links.set_incarnation(incarnation);
        shared.links.set_peers(peers);
        shared.connections = Some(connections);
        NodeResponse::PeersConfigured
    }

    fn start_spawn_shared(
        &mut self,
        dataflow_id: String,
        dataflow_type: DataflowType,
        topology: SerializableTopology,
        handshake_timeout: Duration,
        reply: oneshot::Sender<NodeResponse>,
    ) {
        let managers = match self.peer_managers(&topology) {
            Ok(managers) => managers,
            Err(message) => {
                let _ = reply.send(NodeResponse::Error { message });
                return;
            }
        };
        let spawner = self.spawner();
        tokio::spawn(async move {
            let transport = ClusterSpawnTransport::shared(managers, TRANSPORT_CAPACITY);
            let response = spawner
                .spawn(
                    dataflow_id,
                    dataflow_type,
                    topology,
                    transport,
                    handshake_timeout,
                )
                .await;
            let _ = reply.send(response);
        });
    }

    /// The managers of `topology`'s node set, created the first time a
    /// dataflow uses it: one per peer, each with its own pool, all
    /// connecting through this node set's factory.
    fn peer_managers(
        &mut self,
        topology: &SerializableTopology,
    ) -> std::result::Result<PeerManagers, String> {
        let shared = self
            .shared
            .as_mut()
            .ok_or("SpawnShared before BindPeerListener")?;
        let connections = shared
            .connections
            .ok_or("SpawnShared before ConfigurePeers")?;
        let key = node_set_key(topology.nodes.iter().map(|n| n.node_id.as_str()));
        if let Some(managers) = shared.node_sets.get(&key) {
            return Ok(Arc::clone(managers));
        }
        if !topology.nodes.iter().any(|n| n.node_id == self.node_id) {
            return Err(format!(
                "node {} is not in the topology {key}",
                self.node_id
            ));
        }
        let mut managers = HashMap::new();
        for peer in topology.nodes.iter().filter(|n| n.node_id != self.node_id) {
            let config = SharedConnectionConfig {
                connections,
                ..Default::default()
            };
            let manager = SharedPeerManager::new(
                peer.node_id.clone(),
                config,
                shared.links.factory(key.clone()),
                &self.tokio_handle,
                self.runtime.health_tx(),
            )
            .map_err(|e| format!("creating the manager for peer {}: {e}", peer.node_id))?;
            managers.insert(peer.node_id.clone(), manager);
        }
        let managers = Arc::new(managers);
        shared.node_sets.insert(key, Arc::clone(&managers));
        Ok(managers)
    }

    fn start_feed_data(
        &mut self,
        dataflow_id: String,
        key: (usize, String),
        timestamp: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<NodeResponse>,
    ) {
        let job = self.active_dataflow(&dataflow_id).and_then(|active| {
            let inputs = active.inputs.lock().unwrap();
            prepare_feed(&active.dataflow_type, &inputs, &key, timestamp, &data)
        });
        match job {
            // A full input channel blocks the send, so it runs on a blocking thread.
            Ok(job) => {
                tokio::task::spawn_blocking(move || {
                    let response = match job() {
                        Ok(()) => NodeResponse::DataFed,
                        Err(message) => NodeResponse::Error { message },
                    };
                    let _ = reply.send(response);
                });
            }
            Err(message) => {
                let _ = reply.send(NodeResponse::Error { message });
            }
        }
    }

    fn handle_close_inputs(&self, dataflow_id: &str, worker_idx: Option<usize>) -> NodeResponse {
        let active = match self.active_dataflow(dataflow_id) {
            Ok(active) => active,
            Err(message) => return NodeResponse::Error { message },
        };
        let mut inputs = active.inputs.lock().unwrap();
        match worker_idx {
            // Close all inputs for a specific worker.
            Some(idx) => inputs.retain(|(worker, _), _| *worker != idx),
            // Close all inputs for all workers.
            None => inputs.clear(),
        }
        NodeResponse::InputsClosed
    }

    /// Return the output drained from a port so far, and clear it.
    fn handle_collect_output(&self, dataflow_id: &str, key: (usize, String)) -> NodeResponse {
        let active = match self.active_dataflow(dataflow_id) {
            Ok(active) => active,
            Err(message) => return NodeResponse::Error { message },
        };
        match active.outputs.get(&key) {
            Some(buffer) => NodeResponse::OutputData {
                data: std::mem::take(&mut *buffer.lock().unwrap()),
            },
            None => NodeResponse::Error {
                message: format!("no output collector for worker {} port '{}'", key.0, key.1),
            },
        }
    }

    fn handle_cancel_dataflow(&self, dataflow_id: &str) -> NodeResponse {
        match self.active_dataflow(dataflow_id) {
            Ok(active) => {
                active.cancel.cancel();
                NodeResponse::Error {
                    message: format!("cancelled {dataflow_id}"),
                }
            }
            Err(message) => NodeResponse::Error { message },
        }
    }
}

/// Accept connections from peers with higher node IDs and connect to those
/// with lower ones.
async fn connect_peers(
    node_id: &str,
    listener: &TcpListener,
    peer_addresses: &HashMap<String, SocketAddr>,
) -> std::result::Result<Vec<DedicatedPeer>, String> {
    let mut connections = Vec::new();

    // Determine which peers we accept (lower node_id) vs connect to (higher node_id).
    let mut accept_count = 0usize;
    let mut connect_to: Vec<(String, SocketAddr)> = Vec::new();

    for (peer_id, addr) in peer_addresses {
        if peer_id.as_str() < node_id {
            // We have the higher node_id — we connect to them
            connect_to.push((peer_id.clone(), *addr));
        } else {
            // We have the lower node_id — they will connect to us
            accept_count += 1;
        }
    }

    // Accept incoming connections. With 3+ nodes, connections can arrive
    // in any order, so we identify each peer via a lightweight handshake:
    // the connector writes its node_id (length-prefixed), the acceptor reads it.
    for _ in 0..accept_count {
        match tokio::time::timeout(Duration::from_secs(10), listener.accept()).await {
            Ok(Ok((stream, _addr))) => {
                let (mut reader, mut writer) = stream.into_split();
                // Read peer's node_id (length-prefixed handshake)
                let peer_id = read_peer_id(&mut reader)
                    .await
                    .map_err(|e| format!("peer handshake read failed: {e}"))?;
                // Send our node_id back
                write_peer_id(&mut writer, node_id)
                    .await
                    .map_err(|e| format!("peer handshake write failed: {e}"))?;
                connections.push(PeerConnection {
                    node_id: peer_id,
                    reader,
                    writer,
                });
            }
            Ok(Err(e)) => return Err(format!("accept failed: {e}")),
            Err(_) => return Err("timeout accepting peer connections".into()),
        }
    }

    // Connect to peers with lower node_ids
    for (peer_id, addr) in &connect_to {
        match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                let (mut reader, mut writer) = stream.into_split();
                // Write our node_id so the acceptor can identify us
                write_peer_id(&mut writer, node_id)
                    .await
                    .map_err(|e| format!("peer handshake write to {peer_id} failed: {e}"))?;
                // Read their node_id back for verification
                let remote_id = read_peer_id(&mut reader)
                    .await
                    .map_err(|e| format!("peer handshake read from {peer_id} failed: {e}"))?;
                if remote_id != *peer_id {
                    return Err(format!(
                        "peer ID mismatch: expected {peer_id}, got {remote_id}"
                    ));
                }
                connections.push(PeerConnection {
                    node_id: peer_id.clone(),
                    reader,
                    writer,
                });
            }
            Ok(Err(e)) => return Err(format!("connect to {peer_id} at {addr} failed: {e}")),
            Err(_) => return Err(format!("timeout connecting to {peer_id} at {addr}")),
        }
    }

    Ok(connections)
}

/// What a spawn task needs from the actor.
struct Spawner {
    node_id: String,
    runtime: Arc<RuntimeHandle>,
    tokio_handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<NodeEvent>,
    active: ActiveDataflows,
}

impl Spawner {
    /// Spawn the dataflow over `transport`, start draining its outputs, and
    /// report its completion as an event once they are drained.
    async fn spawn<R, W>(
        self,
        dataflow_id: String,
        dataflow_type: DataflowType,
        topology: SerializableTopology,
        transport: ClusterSpawnTransport<R, W>,
        handshake_timeout: Duration,
    ) -> NodeResponse
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let topo_nodes: Vec<NodeConfig> = topology
            .nodes
            .iter()
            .map(|n| NodeConfig::new(&n.node_id, n.num_workers))
            .collect();

        let topology = match ClusterTopology::multi_node(topo_nodes) {
            Ok(t) => t,
            Err(e) => {
                return NodeResponse::Error {
                    message: format!("invalid topology: {e}"),
                }
            }
        };

        // Both nodes must use the same DataflowId for the transport session to
        // route messages correctly. Derive a deterministic UUID from the string name.
        let df_id = DataflowId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            dataflow_id.as_bytes(),
        ));
        let cancel = CancellationToken::new();
        let options = SpawnOptions::new().cancellation_token(cancel.clone());

        // spawn_cluster calls block_on() internally for the handshake,
        // which panics inside async context. Use spawn_blocking to run it
        // on a dedicated OS thread outside the tokio runtime.
        let runtime = Arc::clone(&self.runtime);
        let node_id = self.node_id.clone();
        let tokio_handle = self.tokio_handle.clone();
        let name = dataflow_id.clone();
        let build_dataflow_type = dataflow_type.clone();
        let spawn_result = tokio::task::spawn_blocking(move || {
            runtime.spawn_cluster(
                &name,
                topology,
                &node_id,
                df_id,
                transport,
                handshake_timeout,
                move |builder| {
                    let (_inputs, _output) =
                        dataflows::build_dataflow(&build_dataflow_type, builder)?;
                    Ok(())
                },
                &tokio_handle,
                options,
            )
        })
        .await
        .unwrap_or_else(|e| {
            Err(instancy::error::RuntimeError::SpawnFailed {
                context: format!("spawn_blocking join: {e}"),
                source: None,
            }
            .into())
        });

        let mut cluster_handle = match spawn_result {
            Ok(handle) => handle,
            Err(e) => {
                return NodeResponse::Error {
                    message: format!("spawn_cluster failed: {e}"),
                }
            }
        };
        let num_local_workers = cluster_handle.num_local_workers();
        let first_worker = cluster_handle.local_worker_range().0;

        // Extract I/O handles based on dataflow type. Every drain thread holds
        // a clone of `drained_tx`, so `drained_rx` closes once they have all ended.
        let (input_names, output_name) = dataflows::port_names(&dataflow_type);
        let (drained_tx, mut drained_rx) = mpsc::channel::<()>(1);
        let mut inputs = InputSenders::new();
        let mut outputs = HashMap::new();
        for local_idx in 0..num_local_workers {
            for port in &input_names {
                if let Some(sender) =
                    extract_input_sender(&dataflow_type, &mut cluster_handle, local_idx, port)
                {
                    inputs.insert((local_idx, port.clone()), sender);
                }
            }
            let buffer = OutputBuffer::default();
            if start_output_drain(
                &dataflow_type,
                &mut cluster_handle,
                local_idx,
                &output_name,
                Arc::clone(&buffer),
                drained_tx.clone(),
            ) {
                outputs.insert((local_idx, output_name.clone()), buffer);
            }
        }
        drop(drained_tx);

        self.active.lock().unwrap().insert(
            dataflow_id.clone(),
            Arc::new(ActiveDataflow {
                dataflow_type,
                cancel,
                inputs: Mutex::new(inputs),
                outputs,
            }),
        );

        let completion = cluster_handle.join();
        let events = self.events;
        let completed_id = dataflow_id.clone();
        tokio::spawn(async move {
            let result = match completion {
                Ok(completion) => completion.await,
                Err(e) => Err(e),
            };
            // Wait for every output to be drained, so a collect after the
            // event sees all of it.
            while drained_rx.recv().await.is_some() {}
            let (success, error) = match result {
                Ok(()) => (true, None),
                Err(e) => (false, Some(e.to_string())),
            };
            let _ = events.send(NodeEvent::DataflowCompleted {
                dataflow_id: completed_id,
                success,
                error,
            });
        });

        NodeResponse::DataflowSpawned {
            dataflow_id,
            num_local_workers,
            first_worker,
        }
    }
}

// ---------------------------------------------------------------------------
// Type-aware I/O extraction helpers
// ---------------------------------------------------------------------------

/// Extract an input sender from a cluster handle based on the dataflow type.
/// Returns a type-erased Box<dyn Any + Send> wrapping the concrete InputSender.
/// Types MUST match those declared in dataflows::build_* functions.
fn extract_input_sender(
    dataflow_type: &DataflowType,
    handle: &mut ClusterSpawnedDataflow<u64>,
    local_idx: usize,
    port: &str,
) -> Option<Box<dyn Any + Send>> {
    match dataflow_type {
        DataflowType::PassThrough => handle
            .take_input::<Vec<u8>>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn Any + Send>),
        DataflowType::ExchangeRoundTrip => handle
            .take_input::<(u64, String)>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn Any + Send>),
        DataflowType::MultiEpochExchange | DataflowType::IterativeFilter => handle
            .take_input::<(u64, i64)>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn Any + Send>),
        DataflowType::DistributedWordCount => handle
            .take_input::<String>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn Any + Send>),
        DataflowType::StagedFanOutFanIn { .. }
        | DataflowType::FilterAggregate { .. }
        | DataflowType::BranchMerge
        | DataflowType::DelayedAggregation { .. }
        | DataflowType::IterativeExchange { .. } => handle
            .take_input::<i64>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn Any + Send>),
        DataflowType::DistributedJoin => {
            if port == "left" {
                handle
                    .take_input::<(u64, String)>(local_idx, port)
                    .ok()
                    .map(|s| Box::new(s) as Box<dyn Any + Send>)
            } else {
                handle
                    .take_input::<(u64, i64)>(local_idx, port)
                    .ok()
                    .map(|s| Box::new(s) as Box<dyn Any + Send>)
            }
        }
    }
}

/// Take a local worker's output receiver and drain it on its own thread into
/// `buffer`. Returns `false` if the worker has no such output.
/// Types MUST match those declared in dataflows::build_* functions.
fn start_output_drain(
    dataflow_type: &DataflowType,
    handle: &mut ClusterSpawnedDataflow<u64>,
    local_idx: usize,
    port: &str,
    buffer: OutputBuffer,
    drained: mpsc::Sender<()>,
) -> bool {
    match dataflow_type {
        DataflowType::PassThrough => {
            drain_output::<Vec<u8>>(handle, local_idx, port, buffer, drained)
        }
        DataflowType::ExchangeRoundTrip => {
            drain_output::<(u64, String)>(handle, local_idx, port, buffer, drained)
        }
        DataflowType::MultiEpochExchange | DataflowType::IterativeFilter => {
            drain_output::<(u64, i64)>(handle, local_idx, port, buffer, drained)
        }
        DataflowType::DistributedWordCount => {
            drain_output::<(String, u64)>(handle, local_idx, port, buffer, drained)
        }
        DataflowType::StagedFanOutFanIn { .. }
        | DataflowType::FilterAggregate { .. }
        | DataflowType::BranchMerge
        | DataflowType::DelayedAggregation { .. }
        | DataflowType::IterativeExchange { .. } => {
            drain_output::<i64>(handle, local_idx, port, buffer, drained)
        }
        DataflowType::DistributedJoin => {
            drain_output::<(u64, String, i64)>(handle, local_idx, port, buffer, drained)
        }
    }
}

/// Append every data batch the output port produces to `buffer`, until the
/// port's channel ends. The output channel is bounded, so the dataflow can
/// only finish if its output is read as it is produced.
fn drain_output<D: Serialize + Send + 'static>(
    handle: &mut ClusterSpawnedDataflow<u64>,
    local_idx: usize,
    port: &str,
    buffer: OutputBuffer,
    drained: mpsc::Sender<()>,
) -> bool {
    let Ok(receiver) = handle.take_output::<D>(local_idx, port) else {
        return false;
    };
    std::thread::spawn(move || {
        let _drained = drained;
        while let Some(event) = receiver.recv() {
            if let OutputEvent::Data { time, data } = event {
                let bytes = bincode::serialize(&data).expect("failed to serialize an output batch");
                buffer.lock().unwrap().push((time, bytes));
            }
        }
    });
    true
}

// ---------------------------------------------------------------------------
// Type-aware FeedData helpers
// ---------------------------------------------------------------------------

/// Prepare a send of `data` to a type-erased InputSender, using dataflow_type
/// to select the concrete type.
fn prepare_feed(
    dataflow_type: &DataflowType,
    senders: &InputSenders,
    key: &(usize, String),
    timestamp: u64,
    data: &[u8],
) -> std::result::Result<FeedJob, String> {
    let sender = senders
        .get(key)
        .ok_or_else(|| format!("no input sender for worker {} port '{}'", key.0, key.1))?
        .as_ref();

    match dataflow_type {
        DataflowType::PassThrough => feed_job::<Vec<u8>>(sender, timestamp, data, "PassThrough"),
        DataflowType::ExchangeRoundTrip => {
            feed_job::<(u64, String)>(sender, timestamp, data, "ExchangeRoundTrip")
        }
        DataflowType::MultiEpochExchange => {
            feed_job::<(u64, i64)>(sender, timestamp, data, "MultiEpochExchange")
        }
        DataflowType::DistributedWordCount => {
            feed_job::<String>(sender, timestamp, data, "DistributedWordCount")
        }
        DataflowType::IterativeFilter => {
            feed_job::<(u64, i64)>(sender, timestamp, data, "IterativeFilter")
        }
        DataflowType::StagedFanOutFanIn { .. }
        | DataflowType::FilterAggregate { .. }
        | DataflowType::BranchMerge
        | DataflowType::DelayedAggregation { .. }
        | DataflowType::IterativeExchange { .. } => {
            feed_job::<i64>(sender, timestamp, data, "scalar-i64")
        }
        // Port "left" → (u64, String), port "right" → (u64, i64)
        DataflowType::DistributedJoin => {
            if key.1 == "left" {
                feed_job::<(u64, String)>(sender, timestamp, data, "DistributedJoin left")
            } else {
                feed_job::<(u64, i64)>(sender, timestamp, data, "DistributedJoin right")
            }
        }
    }
}

fn feed_job<D: DeserializeOwned + Clone + Send + 'static>(
    sender: &(dyn Any + Send),
    timestamp: u64,
    data: &[u8],
    what: &str,
) -> std::result::Result<FeedJob, String> {
    let sender = sender
        .downcast_ref::<InputSender<u64, D>>()
        .ok_or_else(|| format!("downcast failed for {what} input"))?
        .clone();
    let items: Vec<D> = bincode::deserialize(data).map_err(|e| format!("deserialize: {e}"))?;
    Ok(Box::new(move || {
        sender
            .send(timestamp, items)
            .map_err(|e| format!("send: {e}"))
    }))
}

// ---------------------------------------------------------------------------
// Peer-ID handshake helpers for ConnectPeers
// ---------------------------------------------------------------------------

/// Write a node_id as a length-prefixed string (u16 big-endian + UTF-8 bytes).
async fn write_peer_id(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    node_id: &str,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let bytes = node_id.as_bytes();
    let len = bytes.len() as u16;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a node_id from a length-prefixed string (u16 big-endian + UTF-8 bytes).
async fn read_peer_id(reader: &mut tokio::net::tcp::OwnedReadHalf) -> std::io::Result<String> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    if len > 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer ID too long",
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
