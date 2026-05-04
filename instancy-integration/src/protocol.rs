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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

    /// Wait for a dataflow to complete (blocks until all workers finish).
    WaitForCompletion { dataflow_id: String },

    /// Shut down the node process gracefully.
    Shutdown,
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
    },

    /// Data fed to input port.
    DataFed,

    /// Input ports closed.
    InputsClosed,

    /// Collected output data: `(timestamp, bincode-serialized records)`.
    OutputData { data: Vec<(u64, Vec<u8>)> },

    /// An error occurred while processing a command.
    Error { message: String },

    /// Dataflow completed.
    DataflowCompleted {
        dataflow_id: String,
        success: bool,
        error: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Events (node → coordinator, unsolicited)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    /// A dataflow completed (naturally or via cancellation).
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
