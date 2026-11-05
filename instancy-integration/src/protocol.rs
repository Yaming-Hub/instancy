//! Length-prefixed JSON control protocol for coordinator ↔ node communication.
//!
//! Wire format: `[4 bytes: payload length (u32 big-endian)] [JSON payload]`
//!
//! Every message is wrapped in an [`Envelope`] carrying a `request_id` for correlation.

use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Top-level envelope wrapping all control messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub request_id: u64,
    pub kind: MessageKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageKind {
    Command(NodeCommand),
    Response(NodeResponse),
    Event(NodeEvent),
}

/// Serializable cluster topology (mirrors instancy's ClusterTopology).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableTopology {
    pub nodes: Vec<SerializableNodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableNodeConfig {
    pub node_id: String,
    pub num_workers: usize,
}

/// Enum selecting a predefined dataflow builder.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataflowType {
    /// source → map(identity) → output. No exchange.
    PassThrough,
    /// source → exchange_by_hash → map → output.
    ExchangeRoundTrip,
    /// source → exchange → unary_notify (per-epoch aggregate) → output, many epochs.
    MultiEpochExchange,
    /// source → flat_map → exchange(word) → unary_notify(count) → output.
    DistributedWordCount,
    /// source → iterate(filter + exchange) → output.
    IterativeFilter,
    /// two sources → binary join via exchange → output.
    DistributedJoin,
    /// source → map → exchange_to(N) → map → gather → output.
    /// Tests per-stage parallelism with explicit fan-out and fan-in.
    StagedFanOutFanIn { fan_out_parallelism: usize },
    /// source → filter(>threshold) → exchange → reduce(sum) → output.
    /// Tests filter + aggregation pipeline across nodes.
    FilterAggregate { threshold: i64 },
    /// source → branch(even/odd) → map(path-specific) → merge → exchange → reduce → output.
    /// Tests branch + merge operators across cluster.
    BranchMerge,
    /// source → delay_batch(+offset) → exchange → unary_notify(sum per epoch) → output.
    /// Tests delay operator across cluster boundaries.
    DelayedAggregation { delay_offset: u64 },
    /// source → iterate(exchange → map(double) → filter(<threshold) → feedback) → output.
    /// Graph-style iterative computation with exchange inside the loop.
    IterativeExchange { threshold: i64 },
}

// ---------------------------------------------------------------------------
// Commands (coordinator → node)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeCommand {
    /// Phase 1: Bind a TCP listener for peer connections.
    BindListener {
        dataflow_id: String,
        topology: SerializableTopology,
    },

    /// Phase 2: Connect to peers using their reported addresses.
    ConnectPeers {
        dataflow_id: String,
        peer_addresses: HashMap<String, SocketAddr>,
    },

    /// Phase 3: Build and spawn a cluster dataflow.
    SpawnDataflow {
        dataflow_id: String,
        dataflow_type: DataflowType,
    },

    /// Feed data to a specific worker's named input port.
    FeedData {
        dataflow_id: String,
        worker_idx: usize,
        port_name: String,
        timestamp: u64,
        /// Bincode-serialized `Vec<T>` payload.
        data: Vec<u8>,
    },

    /// Close input ports for a dataflow.
    CloseInputs {
        dataflow_id: String,
        /// If None, close all workers' inputs.
        worker_idx: Option<usize>,
    },

    /// Collect output from a specific worker's output port.
    CollectOutput {
        dataflow_id: String,
        worker_idx: usize,
        port_name: String,
    },

    /// Cancel a running dataflow.
    CancelDataflow { dataflow_id: String },

    /// Bind this process's listener for Shared-transport peer connections.
    /// Once per process.
    BindPeerListener,

    /// Configure the Shared transport: the address this node dials for each
    /// peer with a lower ID (a relay address), this process's incarnation,
    /// and the pool size of every manager set created from now on.
    ConfigurePeers {
        peers: HashMap<String, SocketAddr>,
        incarnation: u64,
        connections: usize,
    },

    /// Build and spawn a cluster dataflow over the Shared transport, with the
    /// managers of the topology's node set.
    SpawnShared {
        dataflow_id: String,
        dataflow_type: DataflowType,
        topology: SerializableTopology,
        handshake_timeout_ms: u64,
    },

    /// Report a node as left the cluster (`RuntimeHandle::report_node_leave`).
    ReportNodeLeave { node_id: String },

    /// Report a node as joined the cluster (`RuntimeHandle::report_node_join`).
    ReportNodeJoin { node_id: String },

    /// Report every Shared-transport peer manager the node holds, with its
    /// live pooled connections (`SharedPeerManager::metrics_snapshot`).
    SnapshotPools,

    /// Harness tests only: how this process answers a later `Shutdown`, so a
    /// test can show that the coordinator catches a node that misbehaves at
    /// exit. Handled by the process, not its agent.
    SetShutdownBehavior(ShutdownBehavior),

    /// Shut the node process down: drop its Shared-transport managers, wait
    /// for its runtime to shut down (`RuntimeHandle::shutdown_async`), reply,
    /// then exit at once. instancy closes a dropped manager's connections in
    /// the background, and the exit right after the reply can cut that close
    /// short: the managers are not known to close gracefully.
    Shutdown,
}

/// How a node process answers `Shutdown` (see
/// [`NodeCommand::SetShutdownBehavior`]).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShutdownBehavior {
    /// Whether it replies once its runtime has shut down.
    pub reply: bool,
    /// The code it then exits with, or `None` to never exit.
    pub exit_code: Option<i32>,
}

impl ShutdownBehavior {
    /// Reply, then exit with success.
    pub const NORMAL: ShutdownBehavior = ShutdownBehavior {
        reply: true,
        exit_code: Some(0),
    };
}

/// One Shared-transport peer manager a node holds (see
/// [`NodeCommand::SnapshotPools`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// The key of the node set whose dataflows share the manager
    /// (`peer_link::node_set_key`).
    pub node_set: String,
    /// The peer the manager's connections lead to.
    pub peer: String,
    /// Its live pooled connections, by id.
    pub connections: Vec<PooledConnection>,
}

/// One live pooled connection of a manager: the fields of instancy's
/// `ConnectionSnapshot` the harness uses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct PooledConnection {
    /// Its id in the manager's pool.
    pub id: usize,
    /// Frames queued for its writer and not yet written.
    pub queued_frames: usize,
    /// Dataflow lanes pinned to it.
    pub pinned_lanes: usize,
    /// Frames written to it so far, probes included.
    pub total_frames: u64,
    /// Frame payload bytes written to it so far, probes included.
    pub total_bytes: u64,
}

// ---------------------------------------------------------------------------
// Responses (node → coordinator, correlated by request_id)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeResponse {
    /// Listener bound successfully.
    ListenerReady {
        dataflow_id: String,
        listen_addr: SocketAddr,
    },

    /// All peer connections established for this dataflow.
    PeersConnected { dataflow_id: String },

    /// Dataflow spawned successfully.
    DataflowSpawned {
        dataflow_id: String,
        num_local_workers: usize,
        /// Global index of this node's first worker.
        first_worker: usize,
    },

    /// Data fed to input port.
    DataFed,

    /// Input ports closed.
    InputsClosed,

    /// Collected output data: `(timestamp, bincode-serialized records)`.
    OutputData { data: Vec<(u64, Vec<u8>)> },

    /// The Shared-transport peer listener is bound.
    PeerListenerReady { addr: SocketAddr },

    /// The Shared transport is configured.
    PeersConfigured,

    /// The node was reported left; this many dataflows were newly cancelled.
    NodeLeaveReported { cancelled: usize },

    /// The node was reported joined; `was_left` says if it had been left.
    NodeJoinReported { was_left: bool },

    /// Every Shared-transport peer manager the node holds, ordered by node
    /// set and peer.
    Pools { pools: Vec<PoolSnapshot> },

    /// `SetShutdownBehavior` took effect.
    ShutdownBehaviorSet,

    /// An error occurred while processing a command.
    Error { message: String },
}

// ---------------------------------------------------------------------------
// Events (node → coordinator, unsolicited)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    /// A dataflow completed (naturally or via cancellation). Sent once the
    /// node has drained all of the dataflow's outputs.
    DataflowCompleted {
        dataflow_id: String,
        success: bool,
        error: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Framed I/O helpers
// ---------------------------------------------------------------------------

/// Maximum control message size (16 MB — generous for test payloads).
const MAX_MESSAGE_SIZE: u32 = 16 * 1024 * 1024;

/// Write a length-prefixed JSON envelope to an async writer.
pub async fn write_envelope<W: AsyncWrite + Unpin>(
    writer: &mut W,
    envelope: &Envelope,
) -> std::io::Result<()> {
    let json = serde_json::to_vec(envelope)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = json.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON envelope from an async reader.
///
/// Returns `None` on clean EOF (connection closed).
pub async fn read_envelope<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Envelope>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("control message too large: {len} bytes"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let envelope: Envelope = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(envelope))
}

/// Helper to create a command envelope with a given request_id.
pub fn command_envelope(request_id: u64, cmd: NodeCommand) -> Envelope {
    Envelope {
        request_id,
        kind: MessageKind::Command(cmd),
    }
}

/// Helper to create a response envelope.
pub fn response_envelope(request_id: u64, resp: NodeResponse) -> Envelope {
    Envelope {
        request_id,
        kind: MessageKind::Response(resp),
    }
}

/// Helper to create an event envelope (request_id = 0 for unsolicited events).
pub fn event_envelope(event: NodeEvent) -> Envelope {
    Envelope {
        request_id: 0,
        kind: MessageKind::Event(event),
    }
}
