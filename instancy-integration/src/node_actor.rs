//! DataflowAgent — dactor actor that manages instancy runtimes in a node process.
//!
//! Handles control commands from the coordinator: binding listeners, connecting
//! to peers, spawning cluster dataflows, feeding data, collecting output.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dactor::prelude::*;
use dactor::message::Message;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::id::DataflowId;
use instancy::execute::{ClusterTopology, NodeConfig};
use instancy::runtime::{RuntimeConfig, RuntimeHandle};

use crate::dataflows;
use crate::protocol::*;

// ---------------------------------------------------------------------------
// Messages for the actor
// ---------------------------------------------------------------------------

/// Wrapper message carrying a control command from the coordinator.
pub struct HandleCommand {
    pub envelope: Envelope,
}

impl Message for HandleCommand {
    type Reply = Envelope;
}

// ---------------------------------------------------------------------------
// Per-dataflow state
// ---------------------------------------------------------------------------

struct ListenerState {
    listener: TcpListener,
    topology: SerializableTopology,
}

/// Tracks peer connections established for a dataflow before spawn.
struct ConnectionState {
    topology: SerializableTopology,
    connections: Vec<PeerConnection<
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    >>,
}

/// State for a running dataflow.
struct ActiveDataflow {
    /// The cluster handle — must be kept alive for transport session.
    /// Taken (consumed) by WaitForCompletion.
    cluster_handle: Option<instancy::runtime::ClusterSpawnedDataflow<u64>>,
    /// Input senders, keyed by (worker_local_index, port_name).
    input_senders: HashMap<(usize, String), Box<dyn std::any::Any + Send>>,
    /// Output collectors, keyed by (worker_local_index, port_name).
    output_collectors: HashMap<(usize, String), Box<dyn std::any::Any + Send>>,
    /// What type of dataflow this is (needed for type-aware I/O).
    dataflow_type: DataflowType,
    /// Number of local workers.
    num_local_workers: usize,
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

pub struct DataflowAgent {
    node_id: String,
    runtime: Arc<std::sync::Mutex<RuntimeHandle>>,
    tokio_handle: tokio::runtime::Handle,
    listeners: HashMap<String, ListenerState>,
    connections: HashMap<String, ConnectionState>,
    active: HashMap<String, Arc<Mutex<ActiveDataflow>>>,
}

impl Actor for DataflowAgent {
    type Args = DataflowAgentArgs;
    type Deps = ();

    fn create(args: Self::Args, _deps: ()) -> Self {
        let config = RuntimeConfig {
            worker_threads: args.worker_threads,
            ..Default::default()
        };
        let runtime = RuntimeHandle::new(config)
            .expect("failed to create instancy runtime");
        Self {
            node_id: args.node_id,
            runtime: Arc::new(std::sync::Mutex::new(runtime)),
            tokio_handle: args.tokio_handle,
            listeners: HashMap::new(),
            connections: HashMap::new(),
            active: HashMap::new(),
        }
    }
}

pub struct DataflowAgentArgs {
    pub node_id: String,
    pub worker_threads: usize,
    pub tokio_handle: tokio::runtime::Handle,
}

#[async_trait]
impl Handler<HandleCommand> for DataflowAgent {
    async fn handle(&mut self, msg: HandleCommand, _ctx: &mut ActorContext) -> Envelope {
        let request_id = msg.envelope.request_id;
        match msg.envelope.kind {
            MessageKind::Command(cmd) => {
                let resp = self.handle_command(cmd).await;
                response_envelope(request_id, resp)
            }
            _ => response_envelope(
                request_id,
                NodeResponse::Error {
                    message: "expected Command, got Response/Event".into(),
                },
            ),
        }
    }
}

impl DataflowAgent {
    async fn handle_command(&mut self, cmd: NodeCommand) -> NodeResponse {
        match cmd {
            NodeCommand::BindListener {
                dataflow_id,
                topology,
            } => self.handle_bind_listener(dataflow_id, topology).await,

            NodeCommand::ConnectPeers {
                dataflow_id,
                peer_addresses,
            } => self.handle_connect_peers(dataflow_id, peer_addresses).await,

            NodeCommand::SpawnDataflow {
                dataflow_id,
                dataflow_type,
            } => self.handle_spawn_dataflow(dataflow_id, dataflow_type).await,

            NodeCommand::FeedData {
                dataflow_id,
                worker_idx,
                port_name,
                timestamp,
                data,
            } => self.handle_feed_data(dataflow_id, worker_idx, port_name, timestamp, data),

            NodeCommand::CloseInputs {
                dataflow_id,
                worker_idx,
            } => self.handle_close_inputs(dataflow_id, worker_idx).await,

            NodeCommand::CollectOutput {
                dataflow_id,
                worker_idx,
                port_name,
            } => self.handle_collect_output(dataflow_id, worker_idx, port_name).await,

            NodeCommand::CancelDataflow { dataflow_id } => {
                self.handle_cancel_dataflow(dataflow_id).await
            }

            NodeCommand::WaitForCompletion { dataflow_id } => {
                self.handle_wait_for_completion(dataflow_id).await
            }

            NodeCommand::Shutdown => {
                self.runtime.lock().unwrap().shutdown();
                NodeResponse::Error {
                    message: "shutdown".into(),
                }
            }
        }
    }

    async fn handle_bind_listener(
        &mut self,
        dataflow_id: String,
        topology: SerializableTopology,
    ) -> NodeResponse {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => {
                let addr = listener.local_addr().unwrap();
                self.listeners.insert(
                    dataflow_id.clone(),
                    ListenerState { listener, topology },
                );
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

    async fn handle_connect_peers(
        &mut self,
        dataflow_id: String,
        peer_addresses: HashMap<String, SocketAddr>,
    ) -> NodeResponse {
        let listener_state = match self.listeners.remove(&dataflow_id) {
            Some(s) => s,
            None => {
                return NodeResponse::Error {
                    message: format!("no listener for dataflow {dataflow_id}"),
                }
            }
        };

        let _topology = &listener_state.topology;
        let mut connections = Vec::new();

        // Determine which peers we accept (lower node_id) vs connect to (higher node_id).
        let mut accept_count = 0usize;
        let mut connect_to: Vec<(String, SocketAddr)> = Vec::new();

        for (peer_id, addr) in &peer_addresses {
            if peer_id.as_str() < self.node_id.as_str() {
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
            match tokio::time::timeout(
                Duration::from_secs(10),
                listener_state.listener.accept(),
            )
            .await
            {
                Ok(Ok((stream, _addr))) => {
                    let (mut reader, mut writer) = stream.into_split();
                    // Read peer's node_id (length-prefixed handshake)
                    let peer_id = match read_peer_id(&mut reader).await {
                        Ok(id) => id,
                        Err(e) => {
                            return NodeResponse::Error {
                                message: format!("peer handshake read failed: {e}"),
                            }
                        }
                    };
                    // Send our node_id back
                    if let Err(e) = write_peer_id(&mut writer, &self.node_id).await {
                        return NodeResponse::Error {
                            message: format!("peer handshake write failed: {e}"),
                        };
                    }
                    connections.push(PeerConnection {
                        node_id: peer_id,
                        reader,
                        writer,
                    });
                }
                Ok(Err(e)) => {
                    return NodeResponse::Error {
                        message: format!("accept failed: {e}"),
                    }
                }
                Err(_) => {
                    return NodeResponse::Error {
                        message: format!("timeout accepting peer connections"),
                    }
                }
            }
        }

        // Connect to peers with lower node_ids
        for (peer_id, addr) in &connect_to {
            match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    let (mut reader, mut writer) = stream.into_split();
                    // Write our node_id so the acceptor can identify us
                    if let Err(e) = write_peer_id(&mut writer, &self.node_id).await {
                        return NodeResponse::Error {
                            message: format!("peer handshake write to {peer_id} failed: {e}"),
                        };
                    }
                    // Read their node_id back for verification
                    match read_peer_id(&mut reader).await {
                        Ok(remote_id) => {
                            if remote_id != *peer_id {
                                return NodeResponse::Error {
                                    message: format!(
                                        "peer ID mismatch: expected {peer_id}, got {remote_id}"
                                    ),
                                };
                            }
                        }
                        Err(e) => {
                            return NodeResponse::Error {
                                message: format!("peer handshake read from {peer_id} failed: {e}"),
                            }
                        }
                    }
                    connections.push(PeerConnection {
                        node_id: peer_id.clone(),
                        reader,
                        writer,
                    });
                }
                Ok(Err(e)) => {
                    return NodeResponse::Error {
                        message: format!("connect to {peer_id} at {addr} failed: {e}"),
                    }
                }
                Err(_) => {
                    return NodeResponse::Error {
                        message: format!("timeout connecting to {peer_id} at {addr}"),
                    }
                }
            }
        }

        self.connections.insert(
            dataflow_id.clone(),
            ConnectionState {
                topology: listener_state.topology,
                connections,
            },
        );

        NodeResponse::PeersConnected { dataflow_id }
    }

    async fn handle_spawn_dataflow(
        &mut self,
        dataflow_id: String,
        dataflow_type: DataflowType,
    ) -> NodeResponse {
        let conn_state = match self.connections.remove(&dataflow_id) {
            Some(s) => s,
            None => {
                return NodeResponse::Error {
                    message: format!("no connections for dataflow {dataflow_id}"),
                }
            }
        };

        let topo_nodes: Vec<NodeConfig> = conn_state
            .topology
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

        // spawn_cluster calls block_on() internally for the handshake,
        // which panics inside async context. Use spawn_blocking to run it
        // on a dedicated OS thread outside the tokio runtime.
        let rt_clone = Arc::clone(&self.runtime);
        let node_id = self.node_id.clone();
        let tokio_handle = self.tokio_handle.clone();
        let connections = conn_state.connections;
        let df_id_str = dataflow_id.clone();
        // Both nodes must use the same DataflowId for the transport session to
        // route messages correctly. Derive a deterministic UUID from the string name.
        let df_id = DataflowId::from_uuid(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            dataflow_id.as_bytes(),
        ));

        let spawn_result = tokio::task::spawn_blocking(move || {
            let rt = rt_clone.lock().unwrap();
            rt.spawn_cluster(
                &df_id_str,
                topology,
                &node_id,
                df_id,
                connections,
                64,
                Duration::from_secs(15),
                move |worker_idx, builder| {
                    let (_inputs, _output) =
                        dataflows::build_dataflow(dataflow_type, worker_idx, builder)?;
                    Ok(())
                },
                &tokio_handle,
            )
        })
        .await
        .unwrap_or_else(|e| Err(instancy::error::Error::Custom(format!("spawn_blocking join: {e}"))));

        match spawn_result {
            Ok(mut cluster_handle) => {
                let num_local = cluster_handle.num_local_workers();

                // Extract I/O handles based on dataflow type.
                let (input_names, output_name) =
                    dataflows::port_names(dataflow_type);
                let mut input_senders: HashMap<(usize, String), Box<dyn std::any::Any + Send>> =
                    HashMap::new();
                let mut output_collectors: HashMap<(usize, String), Box<dyn std::any::Any + Send>> =
                    HashMap::new();

                for local_idx in 0..num_local {
                    // Extract inputs based on DataflowType's concrete type.
                    for port in &input_names {
                        let sender = extract_input_sender(
                            dataflow_type,
                            &mut cluster_handle,
                            local_idx,
                            port,
                        );
                        if let Some(s) = sender {
                            input_senders.insert((local_idx, port.clone()), s);
                        }
                    }
                    // Extract output.
                    let receiver = extract_output_receiver(
                        dataflow_type,
                        &mut cluster_handle,
                        local_idx,
                        &output_name,
                    );
                    if let Some(r) = receiver {
                        output_collectors.insert((local_idx, output_name.clone()), r);
                    }
                }

                let active = ActiveDataflow {
                    cluster_handle: Some(cluster_handle),
                    input_senders,
                    output_collectors,
                    dataflow_type,
                    num_local_workers: num_local,
                };

                self.active
                    .insert(dataflow_id.clone(), Arc::new(Mutex::new(active)));

                NodeResponse::DataflowSpawned {
                    dataflow_id,
                    num_local_workers: num_local,
                }
            }
            Err(e) => NodeResponse::Error {
                message: format!("spawn_cluster failed: {e}"),
            },
        }
    }

    fn handle_feed_data(
        &mut self,
        _dataflow_id: String,
        _worker_idx: usize,
        _port_name: String,
        _timestamp: u64,
        _data: Vec<u8>,
    ) -> NodeResponse {
        // TODO: type-aware feed — will implement when we need data-flow tests
        NodeResponse::DataFed
    }

    async fn handle_close_inputs(
        &mut self,
        dataflow_id: String,
        worker_idx: Option<usize>,
    ) -> NodeResponse {
        let active = match self.active.get(&dataflow_id) {
            Some(a) => a.clone(),
            None => {
                return NodeResponse::Error {
                    message: format!("no active dataflow {dataflow_id}"),
                }
            }
        };
        let mut guard = active.lock().await;
        match worker_idx {
            Some(idx) => {
                // Close all inputs for a specific worker.
                let keys: Vec<_> = guard
                    .input_senders
                    .keys()
                    .filter(|(w, _)| *w == idx)
                    .cloned()
                    .collect();
                for k in keys {
                    guard.input_senders.remove(&k);
                }
            }
            None => {
                // Close all inputs for all workers.
                guard.input_senders.clear();
            }
        }
        NodeResponse::InputsClosed
    }

    async fn handle_collect_output(
        &mut self,
        _dataflow_id: String,
        _worker_idx: usize,
        _port_name: String,
    ) -> NodeResponse {
        // TODO: type-aware collect
        NodeResponse::OutputData { data: vec![] }
    }

    async fn handle_cancel_dataflow(&mut self, dataflow_id: String) -> NodeResponse {
        if let Some(active) = self.active.get(&dataflow_id) {
            let guard = active.lock().await;
            if let Some(ref handle) = guard.cluster_handle {
                handle.cancel();
            }
            NodeResponse::Error {
                message: format!("cancelled {dataflow_id}"),
            }
        } else {
            NodeResponse::Error {
                message: format!("no active dataflow {dataflow_id}"),
            }
        }
    }

    async fn handle_wait_for_completion(&mut self, dataflow_id: String) -> NodeResponse {
        let active = match self.active.get(&dataflow_id) {
            Some(a) => a.clone(),
            None => {
                return NodeResponse::Error {
                    message: format!("no active dataflow {dataflow_id}"),
                }
            }
        };

        // Take the cluster handle out — join_blocking consumes it.
        let cluster_handle = {
            let mut guard = active.lock().await;
            guard.cluster_handle.take()
        };

        match cluster_handle {
            Some(handle) => {
                // join_blocking() blocks the current thread, so use spawn_blocking.
                let result = tokio::task::spawn_blocking(move || handle.join_blocking()).await;
                match result {
                    Ok(Ok(())) => NodeResponse::DataflowCompleted {
                        dataflow_id,
                        success: true,
                        error: None,
                    },
                    Ok(Err(e)) => NodeResponse::DataflowCompleted {
                        dataflow_id,
                        success: false,
                        error: Some(format!("{e}")),
                    },
                    Err(e) => NodeResponse::DataflowCompleted {
                        dataflow_id,
                        success: false,
                        error: Some(format!("join panicked: {e}")),
                    },
                }
            }
            None => NodeResponse::Error {
                message: format!("cluster handle already taken for {dataflow_id}"),
            },
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
    dataflow_type: DataflowType,
    handle: &mut instancy::runtime::ClusterSpawnedDataflow<u64>,
    local_idx: usize,
    port: &str,
) -> Option<Box<dyn std::any::Any + Send>> {
    match dataflow_type {
        DataflowType::PassThrough => handle
            .take_input::<Vec<u8>>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>),
        DataflowType::ExchangeRoundTrip => handle
            .take_input::<(u64, String)>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>),
        DataflowType::MultiEpochExchange => handle
            .take_input::<(u64, i64)>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>),
        DataflowType::DistributedWordCount => handle
            .take_input::<String>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>),
        DataflowType::IterativeFilter => handle
            .take_input::<(u64, i64)>(local_idx, port)
            .ok()
            .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>),
        DataflowType::DistributedJoin => {
            if port == "left" {
                handle
                    .take_input::<(u64, String)>(local_idx, port)
                    .ok()
                    .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>)
            } else {
                handle
                    .take_input::<(u64, i64)>(local_idx, port)
                    .ok()
                    .map(|s| Box::new(s) as Box<dyn std::any::Any + Send>)
            }
        }
    }
}

/// Extract an output receiver from a cluster handle based on the dataflow type.
/// Types MUST match those declared in dataflows::build_* functions.
fn extract_output_receiver(
    dataflow_type: DataflowType,
    handle: &mut instancy::runtime::ClusterSpawnedDataflow<u64>,
    local_idx: usize,
    port: &str,
) -> Option<Box<dyn std::any::Any + Send>> {
    match dataflow_type {
        DataflowType::PassThrough => handle
            .take_output::<Vec<u8>>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
        DataflowType::ExchangeRoundTrip => handle
            .take_output::<(u64, String)>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
        DataflowType::MultiEpochExchange => handle
            .take_output::<(u64, i64)>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
        DataflowType::DistributedWordCount => handle
            .take_output::<(String, u64)>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
        DataflowType::IterativeFilter => handle
            .take_output::<(u64, i64)>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
        DataflowType::DistributedJoin => handle
            .take_output::<(u64, String, i64)>(local_idx, port)
            .ok()
            .map(|r| Box::new(r) as Box<dyn std::any::Any + Send>),
    }
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
async fn read_peer_id(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> std::io::Result<String> {
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
    String::from_utf8(buf).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })
}
