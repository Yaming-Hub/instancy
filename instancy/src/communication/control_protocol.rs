//! Control protocol for cross-node cluster coordination.
//!
//! Provides handshake (protocol version, peer identity, and graph fingerprint
//! validation) and ready barrier messages exchanged via
//! [`TransportSession`](super::TransportSession)'s control channel before
//! dataflow execution begins.
//!
//! # Protocol Flow
//!
//! ```text
//! Node A                                          Node B
//!   |-- Handshake {version, fp, df_id, "A"} ------->|
//!   |<------- Handshake {version, fp, df_id, "B"} --|
//!   |   (both validate version, identity, df_id, fp)|
//!   |                                               |
//!   |   ... materialize workers ...                 |
//!   |                                               |
//!   |-- Ready {"A"} ------------------------------->|
//!   |<------------------------------- Ready {"B"} --|
//!   |   (both validate identity, proceed)           |
//! ```
//!
//! Every node checks that each peer speaks [`PROTOCOL_VERSION`], that the
//! node id the peer reports matches the id the local node assigned to that
//! connection, that both run the same dataflow id, and that both built the
//! same graph for the same topology (see [`graph_fingerprint`]).
//!
//! # Wire Format
//!
//! Each message is a single frame on control channel (ID 0):
//! ```text
//! [msg_type: u8][payload...][crc32: u32]
//! ```
//!
//! - Handshake (type 0): `[0][protocol_version: u32 LE][fingerprint: u64 LE]
//!   [dataflow_id: 16 bytes UUID][node_id_len: u32 LE][node_id: UTF-8 bytes]`.
//!   The version always comes first so future layouts can be recognised. The
//!   unversioned 0.1.x handshake (`[0][fingerprint: u64][dataflow_id: 16]`) is
//!   reported as protocol version 1.
//! - Ready (type 1): `[1][node_id_len: u32 LE][node_id: UTF-8 bytes]`
//! - Cancel (type 2): `[2][reason_len: u32 LE][reason: UTF-8 bytes]`
//!
//! The clean-shutdown acknowledgement is not a control message: it travels
//! in-band on a progress channel, behind the sender's data (see
//! `docs/design/cluster.md` §5.5.3).

#[cfg(feature = "transport")]
use crate::cancellation::{CancellationReason, CancellationToken};
#[cfg(feature = "transport")]
use crate::dataflow::id::DataflowId;
use crate::wire;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur when decoding a control protocol message.
#[cfg(feature = "transport")]
#[derive(Debug, thiserror::Error)]
pub enum ControlProtocolError {
    /// The message is too short to contain the minimum required fields.
    #[error("control message too short: {len} bytes (minimum 5)")]
    TooShort { len: usize },

    /// CRC32 integrity check failed.
    #[error("CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    /// The payload size does not match the expected size for the message type.
    #[error("payload size mismatch: expected {expected}, got {actual}")]
    InvalidPayloadSize { expected: usize, actual: usize },

    /// The node_id field contains invalid UTF-8.
    #[error("invalid UTF-8 in node_id: {source}")]
    InvalidUtf8 {
        #[source]
        source: std::str::Utf8Error,
    },

    /// The message type byte is not recognized.
    #[error("unknown control message type: {msg_type}")]
    UnknownMessageType { msg_type: u8 },

    /// An error occurred reading fixed-width fields from the wire.
    #[error("wire read error: {0}")]
    WireRead(Box<crate::Error>),

    /// A handshake uses a cluster protocol version this build does not speak.
    #[error("unsupported cluster protocol version {version}")]
    UnsupportedProtocolVersion { version: u32 },
}

/// Version of the cluster wire protocol.
///
/// Covers everything two nodes must agree on to run a dataflow together: the
/// frame layout, the channel-id namespaces, the control messages, the graph
/// fingerprint, the key-routing hash, and what a progress stream carries.
/// Nodes reject peers whose version differs during the handshake. Version 1
/// is the unversioned 0.1.x protocol; version 2 added the versioned
/// handshake, namespaced channel ids and header CRCs; version 3 added the
/// in-band progress end-of-stream and acknowledgement frames of the clean
/// shutdown handshake (see `docs/design/cluster.md` §5.5.3); version 4 seeds
/// every worker's initial capabilities from the physical plan instead of
/// sending them, so a progress stream carries only changes after the initial
/// state (a version-3 peer would count initial capabilities twice or never),
/// and the graph fingerprint covers the plan's digest.
pub const PROTOCOL_VERSION: u32 = 4;

/// Protocol version reported for an unversioned (0.1.x) handshake.
#[cfg(feature = "transport")]
const LEGACY_PROTOCOL_VERSION: u32 = 1;

#[cfg(feature = "transport")]
impl From<std::str::Utf8Error> for ControlProtocolError {
    fn from(source: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8 { source }
    }
}

/// Errors that can occur while coordinating control-protocol handshakes.
#[cfg(feature = "transport")]
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("handshake/ready timeout waiting for peer {peer_id}")]
    Timeout { peer_id: String },

    #[error("peer {peer_id} disconnected during handshake/ready barrier")]
    PeerDisconnected { peer_id: String },

    #[error("failed to send control message to peer {peer_id}")]
    SendFailed { peer_id: String },

    #[error("no control sender for peer {peer_id}")]
    NoSender { peer_id: String },

    #[error("fingerprint mismatch with peer {peer_id}: local={local:#018x}, remote={remote:#018x}")]
    FingerprintMismatch {
        peer_id: String,
        local: u64,
        remote: u64,
    },

    /// The peer speaks a different cluster protocol version.
    #[error(
        "cluster protocol version mismatch with peer {peer_id}: local={local}, remote={remote}"
    )]
    ProtocolVersionMismatch {
        peer_id: String,
        local: u32,
        remote: u32,
    },

    /// The node at the other end of the connection the local node assigned to
    /// `peer_id` identified itself as `reported` (a miswired connection).
    #[error("peer identity mismatch: connection for {peer_id} is connected to node {reported:?}")]
    PeerIdentityMismatch { peer_id: String, reported: String },

    #[error("dataflow_id mismatch with peer {peer_id}")]
    DataflowIdMismatch { peer_id: String },

    #[error("unexpected control message from peer {peer_id}: expected {expected}, got {got}")]
    UnexpectedMessage {
        peer_id: String,
        expected: String,
        got: String,
    },

    #[error("failed to decode control message from peer {peer_id}: {source}")]
    Decode {
        peer_id: String,
        source: ControlProtocolError,
    },

    /// The peer abandoned the dataflow (it failed to spawn or was cancelled)
    /// while this node was still in the handshake or ready barrier.
    #[error("peer {peer_id} cancelled the dataflow during handshake/ready barrier: {reason}")]
    PeerCancelled { peer_id: String, reason: String },
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Control protocol message types.
#[cfg(feature = "transport")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    /// Handshake with the sender's protocol version, graph fingerprint and
    /// identity, for compatibility validation.
    Handshake {
        protocol_version: u32,
        fingerprint: u64,
        dataflow_id: DataflowId,
        /// Node id of the sender.
        node_id: String,
    },
    /// Ready signal indicating the node has materialized all workers.
    Ready { node_id: String },
    /// Cancellation request from a peer node.
    Cancel {
        /// Human-readable reason for cancellation.
        reason: String,
    },
}

#[cfg(feature = "transport")]
const MSG_TYPE_HANDSHAKE: u8 = 0;
#[cfg(feature = "transport")]
const MSG_TYPE_READY: u8 = 1;
#[cfg(feature = "transport")]
const MSG_TYPE_CANCEL: u8 = 2;

// ---------------------------------------------------------------------------
// Encode / Decode with CRC32
// ---------------------------------------------------------------------------

/// Encode a control message into bytes (with CRC32 trailer).
#[cfg(feature = "transport")]
pub fn encode_control_message(msg: &ControlMessage) -> Vec<u8> {
    let mut buf = Vec::new();
    match msg {
        ControlMessage::Handshake {
            protocol_version,
            fingerprint,
            dataflow_id,
            node_id,
        } => {
            buf.push(MSG_TYPE_HANDSHAKE);
            buf.extend_from_slice(&protocol_version.to_le_bytes());
            buf.extend_from_slice(&fingerprint.to_le_bytes());
            buf.extend_from_slice(dataflow_id.as_bytes());
            let id_bytes = node_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(id_bytes);
        }
        ControlMessage::Ready { node_id } => {
            buf.push(MSG_TYPE_READY);
            let id_bytes = node_id.as_bytes();
            buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(id_bytes);
        }
        ControlMessage::Cancel { reason } => {
            buf.push(MSG_TYPE_CANCEL);
            let reason_bytes = reason.as_bytes();
            buf.extend_from_slice(&(reason_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(reason_bytes);
        }
    }
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode a control message from bytes (verifies CRC32 trailer).
#[cfg(feature = "transport")]
pub fn decode_control_message(data: &[u8]) -> Result<ControlMessage, ControlProtocolError> {
    // Minimum: 1 byte type + 4 bytes CRC
    if data.len() < 5 {
        return Err(ControlProtocolError::TooShort { len: data.len() });
    }

    // Verify CRC32
    let (payload, crc_bytes) = data.split_at(data.len() - 4);
    let expected_crc =
        wire::read_u32(crc_bytes, 0).map_err(|e| ControlProtocolError::WireRead(Box::new(e)))?;
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(ControlProtocolError::CrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    match payload[0] {
        MSG_TYPE_HANDSHAKE => {
            // 1 (type) + 8 (fingerprint) + 16 (dataflow_id): the unversioned
            // 0.1.x layout. No versioned handshake can be this short.
            if payload.len() == 25 {
                return Err(ControlProtocolError::UnsupportedProtocolVersion {
                    version: LEGACY_PROTOCOL_VERSION,
                });
            }
            // The version comes first so that any future layout is reported
            // as a version mismatch rather than a malformed message.
            if payload.len() < 5 {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 5,
                    actual: payload.len(),
                });
            }
            let wire_err = |e| ControlProtocolError::WireRead(Box::new(e));
            let protocol_version = wire::read_u32(payload, 1).map_err(wire_err)?;
            if protocol_version != PROTOCOL_VERSION {
                return Err(ControlProtocolError::UnsupportedProtocolVersion {
                    version: protocol_version,
                });
            }
            // 1 (type) + 4 (version) + 8 (fingerprint) + 16 (dataflow_id) + 4 (id len)
            const FIXED: usize = 33;
            if payload.len() < FIXED {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: FIXED,
                    actual: payload.len(),
                });
            }
            let fingerprint = wire::read_u64(payload, 5).map_err(wire_err)?;
            let dataflow_id =
                DataflowId::from_bytes(wire::read_array::<16>(payload, 13).map_err(wire_err)?);
            let id_len = wire::read_u32(payload, 29).map_err(wire_err)? as usize;
            if payload.len() - FIXED != id_len {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: FIXED.saturating_add(id_len),
                    actual: payload.len(),
                });
            }
            let node_id = std::str::from_utf8(&payload[FIXED..])?.to_string();
            Ok(ControlMessage::Handshake {
                protocol_version,
                fingerprint,
                dataflow_id,
                node_id,
            })
        }
        MSG_TYPE_READY => {
            // 1 (type) + 4 (len) + node_id bytes
            if payload.len() < 5 {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 5,
                    actual: payload.len(),
                });
            }
            let id_len = wire::read_u32(payload, 1)
                .map_err(|e| ControlProtocolError::WireRead(Box::new(e)))?
                as usize;
            if payload.len() != 5 + id_len {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 5 + id_len,
                    actual: payload.len(),
                });
            }
            let node_id = std::str::from_utf8(&payload[5..5 + id_len])?.to_string();
            Ok(ControlMessage::Ready { node_id })
        }
        MSG_TYPE_CANCEL => {
            if payload.len() < 5 {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 5,
                    actual: payload.len(),
                });
            }
            let reason_len = wire::read_u32(payload, 1)
                .map_err(|e| ControlProtocolError::WireRead(Box::new(e)))?
                as usize;
            if payload.len() != 5 + reason_len {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 5 + reason_len,
                    actual: payload.len(),
                });
            }
            let reason = std::str::from_utf8(&payload[5..5 + reason_len])?.to_string();
            Ok(ControlMessage::Cancel { reason })
        }
        other => Err(ControlProtocolError::UnknownMessageType { msg_type: other }),
    }
}

// ---------------------------------------------------------------------------
// Fingerprint computation
// ---------------------------------------------------------------------------

/// Stable digest of everything two nodes must agree on to run a dataflow
/// together.
///
/// Exchange edges are paired across nodes by position and progress updates
/// name operators and ports by index, so a node that builds a different graph
/// or uses a different topology would silently misroute data. The fingerprint
/// covers:
///
/// - the timestamp type;
/// - the topology: every node id, in order, with its worker count;
/// - every operator, ordered by index: kind (creating builder method), name,
///   and port counts, plus any explicit exchange parallelism;
/// - every edge and feedback edge, in order: endpoints and channel kind;
/// - every network exchange edge, in order: graph edge index, routing kind,
///   name, timestamp type (the loop's `Product` type inside `iterate`), and
///   record type;
/// - every named input and output port with its record type;
/// - the digest of the physical plan (`plan_digest`, see
///   `PhysicalPlan::digest`): which workers run each operator, and so how
///   many copies of each initial capability every worker seeds.
///
/// It is computed with [`StableHasher`](crate::hash::StableHasher), so it is
/// identical across platforms and Rust releases. Type names come from
/// [`std::any::type_name`]; nodes built from different crate versions of a
/// record type may therefore disagree, which fails the handshake.
#[cfg(feature = "transport")]
pub(crate) fn graph_fingerprint<T: crate::progress::timestamp::Timestamp>(
    dataflow: &crate::dataflow::LogicalDataflow<T>,
    topology: &crate::execute::ClusterTopology,
    plan_digest: u64,
) -> u64 {
    use crate::hash::StableHasher;
    use std::hash::Hasher;

    struct Digest(StableHasher);
    impl Digest {
        fn num(&mut self, v: usize) {
            self.0.write_u64(v as u64);
        }
        fn text(&mut self, s: &str) {
            self.0.write(s.as_bytes());
        }
        fn section(&mut self, tag: &str, len: usize) {
            self.text(tag);
            self.num(len);
        }
        fn edge(&mut self, e: &crate::dataflow::graph::EdgeInfo) {
            self.num(e.source.operator_index);
            self.num(e.source.slot_index);
            self.num(e.target.operator_index);
            self.num(e.target.slot_index);
            self.num(match e.channel_kind {
                crate::dataflow::graph::ChannelKind::Pipeline => 0,
                crate::dataflow::graph::ChannelKind::Exchange => 1,
            });
        }
    }

    let mut d = Digest(StableHasher::new());
    d.text("instancy-cluster-graph");
    d.num(PROTOCOL_VERSION as usize);
    d.text(std::any::type_name::<T>());

    d.section("nodes", topology.nodes.len());
    for node in &topology.nodes {
        d.text(&node.node_id);
        d.num(node.logical_workers);
    }

    let graph = dataflow.graph();
    let mut operators: Vec<_> = graph.operators().collect();
    operators.sort_by_key(|op| op.index);
    d.section("operators", operators.len());
    for op in operators {
        d.num(op.index);
        d.text(graph.operator_kind(op.index).unwrap_or(""));
        d.text(&op.name);
        d.num(op.input_count);
        d.num(op.output_count);
        d.num(graph.exchange_parallelism(op.index).map_or(0, |p| p + 1));
    }

    d.section("edges", graph.edges().len());
    for e in graph.edges() {
        d.edge(e);
    }
    d.section("feedback", graph.feedback_edges().len());
    for e in graph.feedback_edges() {
        d.edge(e);
    }

    d.section("exchanges", dataflow.exchange_network_creators.len());
    for (edge_idx, _capacity, creator) in &dataflow.exchange_network_creators {
        d.num(*edge_idx);
        d.text(creator.kind());
        d.text(creator.name());
        d.text(creator.timestamp_type_name());
        d.text(creator.data_type_name());
    }

    d.section("inputs", dataflow.input_ports.len());
    for port in &dataflow.input_ports {
        d.text(&port.name);
        d.num(port.operator_index);
        d.text(port.type_name);
    }
    d.section("outputs", dataflow.output_ports.len());
    for port in &dataflow.output_ports {
        d.text(&port.name);
        d.num(port.operator_index);
        d.text(port.type_name);
    }

    d.section("plan", 1);
    d.0.write_u64(plan_digest);

    d.0.finish()
}

// ---------------------------------------------------------------------------
// High-level handshake + ready barrier
// ---------------------------------------------------------------------------

/// Wait for one control message from every peer, from all peers at once,
/// until `deadline`.
///
/// The first problem ends the wait: a peer's `Cancel`
/// ([`HandshakeError::PeerCancelled`]), a closed control channel, an
/// undecodable message, or a message `check` rejects (`check` receives the
/// raw message: it decodes and validates it). A peer that fails is
/// therefore noticed at once even while another peer is slow. On timeout the
/// error names a peer that has not answered.
///
/// With `watch_answered`, peers that already answered are still watched for
/// `Cancel`, a closed channel or an undecodable message (which may have been
/// a `Cancel`); duplicates of earlier messages are ignored. This is used by
/// the ready barrier, after which a peer sends nothing else on its control
/// channel until it cancels. Without it (the handshake), a peer's later
/// messages stay queued for the next phase.
#[cfg(feature = "transport")]
async fn receive_from_each_peer(
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    deadline: tokio::time::Instant,
    watch_answered: bool,
    mut check: impl FnMut(&str, &[u8]) -> Result<(), HandshakeError>,
) -> Result<(), HandshakeError> {
    use std::task::Poll;

    let mut answered = std::collections::HashSet::new();
    while answered.len() < control_receivers.len() {
        let next = tokio::time::timeout_at(
            deadline,
            std::future::poll_fn(|cx| {
                for (peer_id, rx) in control_receivers.iter_mut() {
                    let done = answered.contains(peer_id);
                    if done && !watch_answered {
                        continue;
                    }
                    if let Poll::Ready(message) = rx.poll_recv(cx) {
                        return Poll::Ready((peer_id.clone(), message, done));
                    }
                }
                Poll::Pending
            }),
        )
        .await;
        let (peer_id, message, already_answered) = match next {
            Ok(next) => next,
            Err(_) => {
                let mut waiting: Vec<&String> = control_receivers
                    .keys()
                    .filter(|peer| !answered.contains(*peer))
                    .collect();
                waiting.sort();
                return Err(HandshakeError::Timeout {
                    peer_id: waiting.first().map(|p| p.to_string()).unwrap_or_default(),
                });
            }
        };
        let Some(data) = message else {
            return Err(HandshakeError::PeerDisconnected { peer_id });
        };
        match decode_control_message(&data) {
            Ok(ControlMessage::Cancel { reason }) => {
                return Err(HandshakeError::PeerCancelled { peer_id, reason });
            }
            // A duplicate of an earlier message.
            Ok(_) if already_answered => {}
            // It may have been a Cancel.
            Err(source) if already_answered => {
                return Err(HandshakeError::Decode { peer_id, source });
            }
            _ => {
                check(&peer_id, &data)?;
                answered.insert(peer_id);
            }
        }
    }
    if watch_answered {
        // Every peer answered. A peer that failed right after answering has
        // its Cancel (or closed channel) queued already: apply it now rather
        // than start running without that peer.
        for (peer_id, rx) in control_receivers.iter_mut() {
            loop {
                match rx.try_recv() {
                    Ok(data) => match decode_control_message(&data) {
                        Ok(ControlMessage::Cancel { reason }) => {
                            return Err(HandshakeError::PeerCancelled {
                                peer_id: peer_id.clone(),
                                reason,
                            });
                        }
                        Ok(_) => {}
                        // It may have been a Cancel.
                        Err(source) => {
                            return Err(HandshakeError::Decode {
                                peer_id: peer_id.clone(),
                                source,
                            });
                        }
                    },
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return Err(HandshakeError::PeerDisconnected {
                            peer_id: peer_id.clone(),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}
/// Validate the `Handshake` received on the connection the local node
/// assigned to `peer_id`.
#[cfg(feature = "transport")]
fn validate_peer_handshake(
    peer_id: &str,
    data: &[u8],
    local_fingerprint: u64,
    dataflow_id: DataflowId,
) -> Result<(), HandshakeError> {
    let peer_msg = match decode_control_message(data) {
        Ok(msg) => msg,
        Err(ControlProtocolError::UnsupportedProtocolVersion { version }) => {
            return Err(HandshakeError::ProtocolVersionMismatch {
                peer_id: peer_id.to_string(),
                local: PROTOCOL_VERSION,
                remote: version,
            });
        }
        Err(source) => {
            return Err(HandshakeError::Decode {
                peer_id: peer_id.to_string(),
                source,
            });
        }
    };
    match peer_msg {
        ControlMessage::Handshake {
            protocol_version,
            fingerprint,
            dataflow_id: peer_df_id,
            node_id,
        } => {
            if protocol_version != PROTOCOL_VERSION {
                return Err(HandshakeError::ProtocolVersionMismatch {
                    peer_id: peer_id.to_string(),
                    local: PROTOCOL_VERSION,
                    remote: protocol_version,
                });
            }
            // Identity first: on a miswired connection the other checks
            // compare against the wrong node.
            if node_id != peer_id {
                return Err(HandshakeError::PeerIdentityMismatch {
                    peer_id: peer_id.to_string(),
                    reported: node_id,
                });
            }
            if peer_df_id != dataflow_id {
                return Err(HandshakeError::DataflowIdMismatch {
                    peer_id: peer_id.to_string(),
                });
            }
            if fingerprint != local_fingerprint {
                return Err(HandshakeError::FingerprintMismatch {
                    peer_id: peer_id.to_string(),
                    local: local_fingerprint,
                    remote: fingerprint,
                });
            }
            Ok(())
        }
        other => Err(HandshakeError::UnexpectedMessage {
            peer_id: peer_id.to_string(),
            expected: "Handshake".into(),
            got: format!("{other:?}"),
        }),
    }
}

/// Validate the `Ready` received on the connection the local node assigned
/// to `peer_id`.
#[cfg(feature = "transport")]
fn validate_peer_ready(peer_id: &str, data: &[u8]) -> Result<(), HandshakeError> {
    let peer_msg = decode_control_message(data).map_err(|source| HandshakeError::Decode {
        peer_id: peer_id.to_string(),
        source,
    })?;
    match peer_msg {
        ControlMessage::Ready { node_id } if node_id == peer_id => Ok(()),
        ControlMessage::Ready { node_id } => Err(HandshakeError::PeerIdentityMismatch {
            peer_id: peer_id.to_string(),
            reported: node_id,
        }),
        other => Err(HandshakeError::UnexpectedMessage {
            peer_id: peer_id.to_string(),
            expected: "Ready".into(),
            got: format!("{other:?}"),
        }),
    }
}

/// Perform the control handshake with all remote peers.
///
/// Sends a `Handshake` message to each peer and waits to receive one back.
/// Validates each peer's protocol version, that it identifies as the node the
/// connection is assigned to, its dataflow id, and its graph fingerprint.
///
/// # Errors
///
/// Returns an error if:
/// - A peer speaks a different protocol version, reports a different node id,
///   or reports a different dataflow id or fingerprint
/// - A timeout expires before all peers respond
/// - A peer sends an invalid or unexpected message
#[cfg(feature = "transport")]
pub async fn perform_handshake(
    session: &super::TransportSession,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_node_id: &str,
    local_fingerprint: u64,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Handshake {
        protocol_version: PROTOCOL_VERSION,
        fingerprint: local_fingerprint,
        dataflow_id,
        node_id: local_node_id.to_string(),
    };
    let payload = encode_control_message(&msg);

    // Send handshake to all peers.
    for peer_id in session.peer_node_ids().collect::<Vec<_>>() {
        let sender = session
            .control_sender(peer_id)
            .ok_or_else(|| HandshakeError::NoSender {
                peer_id: peer_id.to_string(),
            })?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| HandshakeError::SendFailed {
                peer_id: peer_id.to_string(),
            })?;
    }

    // Receive handshake from all peers.
    let deadline = tokio::time::Instant::now() + timeout;
    receive_from_each_peer(control_receivers, deadline, false, |peer_id, data| {
        validate_peer_handshake(peer_id, data, local_fingerprint, dataflow_id)
    })
    .await
}

/// Perform the ready barrier with all remote peers.
///
/// Sends a `Ready` signal to each peer and waits to receive one back.
/// This ensures all nodes have materialized their workers before any
/// node begins execution. Each peer's `Ready` must carry the node id the
/// connection is assigned to.
#[cfg(feature = "transport")]
pub async fn perform_ready_barrier(
    session: &super::TransportSession,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_node_id: &str,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Ready {
        node_id: local_node_id.to_string(),
    };
    let payload = encode_control_message(&msg);

    // Send Ready to all peers.
    for peer_id in session.peer_node_ids().collect::<Vec<_>>() {
        let sender = session
            .control_sender(peer_id)
            .ok_or_else(|| HandshakeError::NoSender {
                peer_id: peer_id.to_string(),
            })?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| HandshakeError::SendFailed {
                peer_id: peer_id.to_string(),
            })?;
    }

    // Receive Ready from all peers.
    let deadline = tokio::time::Instant::now() + timeout;
    receive_from_each_peer(control_receivers, deadline, true, validate_peer_ready).await
}

/// Perform the handshake protocol using a [`ClusterTransport`](super::cluster_transport::ClusterTransport).
///
/// Functionally identical to [`perform_handshake`] but accepts the unified
/// transport abstraction instead of a raw `TransportSession`.
#[cfg(feature = "transport")]
pub async fn perform_handshake_with_transport(
    transport: &super::cluster_transport::ClusterTransport,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_node_id: &str,
    local_fingerprint: u64,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Handshake {
        protocol_version: PROTOCOL_VERSION,
        fingerprint: local_fingerprint,
        dataflow_id,
        node_id: local_node_id.to_string(),
    };
    let payload = encode_control_message(&msg);

    for peer_id in transport.peer_node_ids() {
        let sender =
            transport
                .control_sender(&peer_id)
                .ok_or_else(|| HandshakeError::NoSender {
                    peer_id: peer_id.clone(),
                })?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| HandshakeError::SendFailed {
                peer_id: peer_id.clone(),
            })?;
    }

    let deadline = tokio::time::Instant::now() + timeout;
    receive_from_each_peer(control_receivers, deadline, false, |peer_id, data| {
        validate_peer_handshake(peer_id, data, local_fingerprint, dataflow_id)
    })
    .await
}

/// Perform the ready barrier using a [`ClusterTransport`](super::cluster_transport::ClusterTransport).
///
/// Functionally identical to [`perform_ready_barrier`] but accepts the unified
/// transport abstraction instead of a raw `TransportSession`.
#[cfg(feature = "transport")]
pub async fn perform_ready_barrier_with_transport(
    transport: &super::cluster_transport::ClusterTransport,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_node_id: &str,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Ready {
        node_id: local_node_id.to_string(),
    };
    let payload = encode_control_message(&msg);

    for peer_id in transport.peer_node_ids() {
        let sender =
            transport
                .control_sender(&peer_id)
                .ok_or_else(|| HandshakeError::NoSender {
                    peer_id: peer_id.clone(),
                })?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| HandshakeError::SendFailed {
                peer_id: peer_id.clone(),
            })?;
    }

    let deadline = tokio::time::Instant::now() + timeout;
    receive_from_each_peer(control_receivers, deadline, true, validate_peer_ready).await
}

/// Broadcast a cancellation message to all peers using a [`ClusterTransport`].
///
/// Called when the local dataflow is cancelled. Sending is best-effort: peers
/// may already have disconnected by the time cancellation propagates. Every
/// peer is sent to concurrently, so a peer whose control queue is stuck does
/// not delay the others; callers bound the whole broadcast with a timeout.
#[cfg(feature = "transport")]
pub async fn broadcast_cancel_with_transport(
    transport: &super::cluster_transport::ClusterTransport,
    dataflow_id: DataflowId,
    reason: &str,
) {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let payload = encode_control_message(&ControlMessage::Cancel {
        reason: reason.to_string(),
    });

    let sends: Vec<_> = transport
        .peer_node_ids()
        .into_iter()
        .filter_map(|peer_id| transport.control_sender(&peer_id).cloned())
        .map(|sender| {
            let frame = Frame {
                dataflow_id,
                channel_id: CONTROL_CHANNEL_ID,
                payload: payload.clone(),
            };
            async move {
                let _ = sender.send(frame).await;
            }
        })
        .collect();
    join_all(sends).await;
}

/// Drive every future to completion concurrently and return their outputs in
/// order.
#[cfg(feature = "transport")]
pub(crate) async fn join_all<F: std::future::Future>(futures: Vec<F>) -> Vec<F::Output> {
    use std::task::Poll;

    let mut futures: Vec<std::pin::Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    outputs.into_iter().flatten().collect()
}

/// Spawn the per-dataflow control dispatcher on `runtime_handle`.
///
/// For each peer's control channel:
/// - `Cancel` cancels the local token with
///   [`CancellationReason::PeerCancelled`] (or, when it forwards a failure
///   after a requested stop, escalates it so a drain in progress stops) and
///   reports
///   [`LinkEvent::CancelReceived`](super::peer_link::LinkEvent::CancelReceived).
/// - An undecodable message reports
///   [`LinkEvent::ControlUndecodable`](super::peer_link::LinkEvent::ControlUndecodable).
/// - A closed channel reports
///   [`LinkEvent::ControlClosed`](super::peer_link::LinkEvent::ControlClosed);
///   events from one peer are reported in arrival order.
///
/// The dispatcher keeps running after local cancellation (a peer's Cancel
/// may still arrive during the shutdown handshake) and exits when `shutdown`
/// fires (teardown) or every channel has closed.
///
/// # Security
///
/// This function assumes all peers in `control_receivers` are authenticated at
/// the connection layer (e.g., via mTLS in the application's `ConnectionManager`).
/// The `Cancel` message itself carries no cryptographic signature — peer identity
/// verification is the responsibility of the transport layer.
#[cfg(feature = "transport")]
pub(crate) fn spawn_control_dispatcher(
    control_receivers: std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    cancel_token: CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
    events: super::peer_link::LifecycleEventSender,
    runtime_handle: &tokio::runtime::Handle,
) -> tokio::task::JoinHandle<()> {
    use super::peer_link::{LinkEvent, report_link_event};

    runtime_handle.spawn(async move {
        let mut set = tokio::task::JoinSet::new();

        for (peer_id, mut rx) in control_receivers {
            let token = cancel_token.clone();
            let shut = shutdown.clone();
            let events = events.clone();
            set.spawn(async move {
                loop {
                    tokio::select! {
                        // Messages first: a Cancel queued before teardown
                        // must still be applied.
                        biased;
                        maybe_data = rx.recv() => {
                            let Some(data) = maybe_data else {
                                report_link_event(&events, &peer_id, LinkEvent::ControlClosed);
                                return;
                            };
                            match decode_control_message(&data) {
                                Ok(ControlMessage::Cancel { reason }) => {
                                    // A failure the peer forwards after its
                                    // requested stop also ends our drain.
                                    token.escalate(CancellationReason::PeerCancelled {
                                        peer_id: peer_id.clone(),
                                        detail: reason,
                                    });
                                    report_link_event(&events, &peer_id, LinkEvent::CancelReceived);
                                }
                                // Late handshake/ready duplicates carry no new information.
                                Ok(_) => {}
                                Err(e) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!(peer = %peer_id, "undecodable control message: {e}");
                                    report_link_event(
                                        &events,
                                        &peer_id,
                                        LinkEvent::ControlUndecodable(e.to_string()),
                                    );
                                }
                            }
                        }
                        _ = shut.cancelled() => return,
                    }
                }
            });
        }

        while set.join_next().await.is_some() {}
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "transport")]
mod tests {
    use super::*;

    use crate::communication::transport::FramedReader;
    use crate::communication::transport_session::PeerConnection;

    fn handshake(fingerprint: u64, dataflow_id: DataflowId, node_id: &str) -> ControlMessage {
        ControlMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            fingerprint,
            dataflow_id,
            node_id: node_id.to_string(),
        }
    }

    #[test]
    fn handshake_roundtrip() {
        for node_id in ["node-a", "", "ノード-✓"] {
            let msg = handshake(0xDEADBEEF_CAFEBABE, DataflowId::new(), node_id);
            let encoded = encode_control_message(&msg);
            let decoded = decode_control_message(&encoded).unwrap();
            assert_eq!(msg, decoded);
        }
    }

    #[test]
    fn handshake_from_other_protocol_version_is_reported_as_such() {
        let mut msg = handshake(1, DataflowId::new(), "node-a");
        if let ControlMessage::Handshake {
            protocol_version, ..
        } = &mut msg
        {
            *protocol_version = PROTOCOL_VERSION + 1;
        }
        let encoded = encode_control_message(&msg);
        assert!(matches!(
            decode_control_message(&encoded),
            Err(ControlProtocolError::UnsupportedProtocolVersion { version })
                if version == PROTOCOL_VERSION + 1
        ));

        // A future version may use any layout after the version field.
        let mut buf = vec![MSG_TYPE_HANDSHAKE];
        buf.extend_from_slice(&(PROTOCOL_VERSION + 7).to_le_bytes());
        buf.extend_from_slice(&[0xAA; 3]);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_control_message(&buf),
            Err(ControlProtocolError::UnsupportedProtocolVersion { .. })
        ));
    }

    /// The unversioned 0.1.x handshake is recognised and rejected as version 1.
    #[test]
    fn legacy_unversioned_handshake_is_rejected_as_version_one() {
        let mut buf = vec![MSG_TYPE_HANDSHAKE];
        buf.extend_from_slice(&0x1234_u64.to_le_bytes());
        buf.extend_from_slice(DataflowId::new().as_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let err = validate_peer_handshake("node-b", &buf, 0x1234, DataflowId::new()).unwrap_err();
        assert!(
            matches!(
                err,
                HandshakeError::ProtocolVersionMismatch {
                    local: PROTOCOL_VERSION,
                    remote: 1,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn handshake_validation_checks_identity_dataflow_and_fingerprint() {
        let df = DataflowId::new();
        let ok = encode_control_message(&handshake(7, df, "node-b"));
        assert!(validate_peer_handshake("node-b", &ok, 7, df).is_ok());

        let err = validate_peer_handshake("node-c", &ok, 7, df).unwrap_err();
        assert!(
            matches!(&err, HandshakeError::PeerIdentityMismatch { peer_id, reported }
                if peer_id == "node-c" && reported == "node-b"),
            "{err}"
        );
        assert!(matches!(
            validate_peer_handshake("node-b", &ok, 7, DataflowId::new()),
            Err(HandshakeError::DataflowIdMismatch { .. })
        ));
        assert!(matches!(
            validate_peer_handshake("node-b", &ok, 8, df),
            Err(HandshakeError::FingerprintMismatch { .. })
        ));
        let ready = encode_control_message(&ControlMessage::Ready {
            node_id: "node-b".into(),
        });
        assert!(matches!(
            validate_peer_handshake("node-b", &ready, 7, df),
            Err(HandshakeError::UnexpectedMessage { .. })
        ));
    }

    #[test]
    fn ready_validation_checks_identity() {
        let ready = encode_control_message(&ControlMessage::Ready {
            node_id: "node-b".into(),
        });
        assert!(validate_peer_ready("node-b", &ready).is_ok());
        assert!(matches!(
            validate_peer_ready("node-c", &ready),
            Err(HandshakeError::PeerIdentityMismatch { .. })
        ));
    }

    #[test]
    fn handshake_rejects_truncated_or_padded_node_id() {
        let encoded = encode_control_message(&handshake(1, DataflowId::new(), "node-a"));
        // Re-encode the payload with one byte dropped / added, keeping a valid CRC.
        let payload = &encoded[..encoded.len() - 4];
        for variant in [payload[..payload.len() - 1].to_vec(), {
            let mut v = payload.to_vec();
            v.push(b'x');
            v
        }] {
            let mut buf = variant;
            let crc = crc32fast::hash(&buf);
            buf.extend_from_slice(&crc.to_le_bytes());
            assert!(matches!(
                decode_control_message(&buf),
                Err(ControlProtocolError::InvalidPayloadSize { .. })
            ));
        }
    }

    #[test]
    fn ready_roundtrip() {
        let msg = ControlMessage::Ready {
            node_id: "node-42".to_string(),
        };
        let encoded = encode_control_message(&msg);
        let decoded = decode_control_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn cancel_message_roundtrip() {
        let msg = ControlMessage::Cancel {
            reason: "operator requested shutdown".to_string(),
        };
        let encoded = encode_control_message(&msg);
        let decoded = decode_control_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn cancel_empty_reason_roundtrip() {
        let msg = ControlMessage::Cancel {
            reason: String::new(),
        };
        let encoded = encode_control_message(&msg);
        let decoded = decode_control_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn decode_rejects_corrupted_crc() {
        let msg = handshake(42, DataflowId::new(), "node-a");
        let mut encoded = encode_control_message(&msg);
        // Flip a bit in the payload.
        encoded[5] ^= 0x01;
        let result = decode_control_message(&encoded);
        assert!(matches!(
            result,
            Err(ControlProtocolError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn decode_rejects_too_short() {
        let result = decode_control_message(&[1, 2, 3]);
        assert!(matches!(result, Err(ControlProtocolError::TooShort { .. })));
    }

    #[test]
    fn decode_rejects_unknown_type() {
        // Craft valid CRC for unknown type byte.
        let mut buf = vec![0xFF_u8];
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let result = decode_control_message(&buf);
        assert!(matches!(
            result,
            Err(ControlProtocolError::UnknownMessageType { msg_type: 0xFF })
        ));
    }

    mod fingerprint {
        use super::super::graph_fingerprint;
        use crate::dataflow::plan::{Parallelism, PhysicalPlan};
        use crate::dataflow::{DataflowBuilder, LogicalDataflow};
        use crate::execute::{ClusterTopology, NodeConfig};
        use crate::progress::timestamp::Timestamp;

        fn topo(nodes: &[(&str, usize)]) -> ClusterTopology {
            ClusterTopology::multi_node(
                nodes.iter().map(|(n, w)| NodeConfig::new(*n, *w)).collect(),
            )
            .unwrap()
        }

        fn build<T: Timestamp>(f: impl Fn(&DataflowBuilder<T>)) -> LogicalDataflow<T> {
            let b = DataflowBuilder::<T>::new("fp");
            f(&b);
            b.build().unwrap()
        }

        /// The fingerprint `spawn_cluster` computes: with the digest of the
        /// cluster's (uniform) plan.
        fn cluster_fp<T: Timestamp>(g: &LogicalDataflow<T>, t: &ClusterTopology) -> u64 {
            let plan =
                PhysicalPlan::new(&g.graph, &g.stages, Parallelism::Uniform(t.total_workers()))
                    .unwrap();
            graph_fingerprint(g, t, plan.digest())
        }

        fn fp<T: Timestamp>(f: impl Fn(&DataflowBuilder<T>)) -> u64 {
            cluster_fp(&build(f), &topo(&[("a", 1), ("b", 1)]))
        }

        fn base(b: &DataflowBuilder<u64>) {
            b.input::<u64>("in")
                .unwrap()
                .map("m", |_t, x| x)
                .exchange("x", |x: &u64| *x)
                .output("out")
                .unwrap();
        }

        #[test]
        fn identical_graphs_match() {
            assert_eq!(fp(base), fp(base));
            let g = build(base);
            let t = topo(&[("a", 2), ("b", 3)]);
            assert_eq!(cluster_fp(&g, &t), cluster_fp(&g, &t.clone()));
        }

        #[test]
        fn topology_changes_are_detected() {
            let g = build(base);
            let f = |t: ClusterTopology| cluster_fp(&g, &t);
            let reference = f(topo(&[("a", 2), ("b", 1)]));
            assert_ne!(reference, f(topo(&[("a", 1), ("b", 2)])), "worker split");
            assert_ne!(reference, f(topo(&[("a", 2), ("c", 1)])), "node id");
            let mut reordered = topo(&[("a", 2), ("b", 1)]);
            reordered.nodes.reverse();
            assert_ne!(reference, f(reordered), "node order");
            assert_ne!(
                reference,
                f(topo(&[("a", 2), ("b", 1), ("c", 1)])),
                "extra node"
            );
        }

        /// Two nodes with the same graph and topology whose physical plans
        /// differ would seed different multiplicities: the fingerprints
        /// differ, so the handshake fails.
        #[test]
        fn plan_changes_are_detected() {
            let staged = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m", |_t, x| x)
                    .exchange_to("x", 1, |x: &u64| *x)
                    .unwrap()
                    .output("out")
                    .unwrap();
            };
            let g = build(staged);
            let t = topo(&[("a", 1), ("b", 1)]);
            let digest = |parallelism| {
                PhysicalPlan::new(&g.graph, &g.stages, parallelism)
                    .unwrap()
                    .digest()
            };
            // Stage 0 on 2 workers, the exchange's stage on 1, against both
            // on 1: same graph, same topology, different hosting.
            let two_to_one = digest(Parallelism::PerStage { default: 2 });
            let one = digest(Parallelism::Uniform(1));
            assert_ne!(two_to_one, one);
            assert_ne!(
                graph_fingerprint(&g, &t, two_to_one),
                graph_fingerprint(&g, &t, one)
            );
            assert_eq!(
                graph_fingerprint(&g, &t, two_to_one),
                graph_fingerprint(&g, &t, digest(Parallelism::PerStage { default: 2 }))
            );
        }

        #[test]
        fn operator_kind_is_detected() {
            let filter = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .filter("m", |_t, _x| true)
                    .exchange("x", |x: &u64| *x)
                    .output("out")
                    .unwrap();
            };
            assert_ne!(fp(base), fp(filter));
        }

        #[test]
        fn operator_name_is_detected() {
            let renamed = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m2", |_t, x| x)
                    .exchange("x", |x: &u64| *x)
                    .output("out")
                    .unwrap();
            };
            assert_ne!(fp(base), fp(renamed));
        }

        #[test]
        fn exchange_record_type_is_detected() {
            let signed = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m", |_t, x| x as i64)
                    .exchange("x", |x: &i64| *x as u64)
                    .map("back", |_t, x| x as u64)
                    .output("out")
                    .unwrap();
            };
            let unsigned = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m", |_t, x| x)
                    .exchange("x", |x: &u64| *x)
                    .map("back", |_t, x| x)
                    .output("out")
                    .unwrap();
            };
            assert_ne!(fp(signed), fp(unsigned));
        }

        #[test]
        fn timestamp_type_is_detected() {
            let small = |b: &DataflowBuilder<u32>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m", |_t, x| x)
                    .exchange("x", |x: &u64| *x)
                    .output("out")
                    .unwrap();
            };
            assert_ne!(fp(base), fp(small));
        }

        #[test]
        fn exchange_vs_broadcast_is_detected() {
            let bcast = |b: &DataflowBuilder<u64>| {
                b.input::<u64>("in")
                    .unwrap()
                    .map("m", |_t, x| x)
                    .broadcast("x")
                    .output("out")
                    .unwrap();
            };
            assert_ne!(fp(base), fp(bcast));
        }

        #[test]
        fn edge_endpoints_are_detected() {
            // Same operators and names; only which input feeds the exchange differs.
            let wire = |left_exchanged: bool| {
                move |b: &DataflowBuilder<u64>| {
                    let l = b.input::<u64>("l").unwrap();
                    let r = b.input::<u64>("r").unwrap();
                    let (ex, direct) = if left_exchanged { (l, r) } else { (r, l) };
                    ex.exchange("x", |x: &u64| *x).output("o1").unwrap();
                    direct.output("o2").unwrap();
                }
            };
            assert_ne!(fp(wire(true)), fp(wire(false)));
        }

        #[test]
        fn branch_order_is_detected() {
            let branches = |first_even: bool| {
                move |b: &DataflowBuilder<u64>| {
                    let input = b.input::<u64>("in").unwrap();
                    let order = if first_even {
                        [("evens", true), ("odds", false)]
                    } else {
                        [("odds", false), ("evens", true)]
                    };
                    for (name, even) in order {
                        input
                            .clone()
                            .filter(name, move |_t, x: &u64| (x % 2 == 0) == even)
                            .exchange_by_hash(format!("{name}-ex"), |x: &u64| *x)
                            .output(name)
                            .unwrap();
                    }
                }
            };
            assert_ne!(fp(branches(true)), fp(branches(false)));
        }

        #[test]
        fn graphs_inside_iterate_are_covered() {
            let looped = |kind_map: bool| {
                move |b: &DataflowBuilder<u64>| {
                    let input = b.input::<u64>("in").unwrap();
                    input
                        .iterate::<u32>("loop", 1u32, move |v| {
                            let body = if kind_map {
                                v.map("step", |_t, x: u64| x)
                            } else {
                                v.filter("step", |_t, _x| true)
                            };
                            let (fb, out) = body.branch("split", |_t, x| *x > 0);
                            crate::dataflow::dataflow_builder::IterateResult {
                                feedback: fb,
                                output: out,
                            }
                        })
                        .output("out")
                        .unwrap();
                }
            };
            assert_eq!(fp(looped(true)), fp(looped(true)));
            assert_ne!(fp(looped(true)), fp(looped(false)));
        }

        /// An exchange inside a loop carries `Product<T, TInner>` timestamps on
        /// the wire, so the inner timestamp type must be part of the fingerprint.
        #[test]
        fn loop_timestamp_type_of_inner_exchange_is_detected() {
            fn looped<TInner>(summary: TInner::Summary) -> impl Fn(&DataflowBuilder<u64>)
            where
                TInner: Timestamp + crate::communication::codec::ExchangeData,
                TInner::Summary: Clone,
                crate::order::Product<u64, TInner>:
                    Timestamp + crate::communication::codec::ExchangeData,
            {
                move |b: &DataflowBuilder<u64>| {
                    b.input::<u64>("in")
                        .unwrap()
                        .iterate::<TInner>("loop", summary.clone(), |v| {
                            let (fb, out) = v
                                .exchange("x", |x: &u64| *x)
                                .map("dec", |_t, x: u64| x.saturating_sub(1))
                                .branch("split", |_t, x| *x > 0);
                            crate::dataflow::dataflow_builder::IterateResult {
                                feedback: fb,
                                output: out,
                            }
                        })
                        .output("out")
                        .unwrap();
                }
            }
            assert_eq!(fp(looped::<u32>(1u32)), fp(looped::<u32>(1u32)));
            assert_ne!(fp(looped::<u32>(1u32)), fp(looped::<u64>(1u64)));
        }
    }

    #[test]
    fn ready_with_empty_node_id() {
        let msg = ControlMessage::Ready {
            node_id: String::new(),
        };
        let encoded = encode_control_message(&msg);
        let decoded = decode_control_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn retired_end_of_stream_ack_type_is_unknown() {
        // 0x10 was a pre-release control-channel ack; acks now travel in-band
        // on progress channels, so the type must not be accepted.
        let mut payload = vec![0x10];
        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_control_message(&payload),
            Err(ControlProtocolError::UnknownMessageType { msg_type: 0x10 })
        ));
    }

    fn dispatcher_with_peer() -> (
        tokio::sync::mpsc::Sender<Vec<u8>>,
        CancellationToken,
        tokio_util::sync::CancellationToken,
        tokio::sync::mpsc::UnboundedReceiver<crate::communication::peer_link::LifecycleEvent>,
        tokio::task::JoinHandle<()>,
    ) {
        let cancel_token = CancellationToken::new();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (events, events_rx) = tokio::sync::mpsc::unbounded_channel();
        let dispatcher = spawn_control_dispatcher(
            std::collections::HashMap::from([("node-b".to_string(), rx)]),
            cancel_token.clone(),
            shutdown.clone(),
            events,
            &tokio::runtime::Handle::current(),
        );
        (tx, cancel_token, shutdown, events_rx, dispatcher)
    }

    #[tokio::test]
    async fn control_dispatcher_cancels_local_token() {
        use crate::communication::peer_link::{LifecycleEvent, LinkEvent};

        let (tx, cancel_token, _shutdown, mut events_rx, dispatcher) = dispatcher_with_peer();
        tx.send(encode_control_message(&ControlMessage::Cancel {
            reason: "user requested shutdown".to_string(),
        }))
        .await
        .unwrap();
        drop(tx);

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            cancel_token.cancelled_async(),
        )
        .await
        .expect("cancel token should be cancelled");
        dispatcher.await.unwrap();

        assert_eq!(
            cancel_token.reason(),
            Some(CancellationReason::PeerCancelled {
                peer_id: "node-b".to_string(),
                detail: "user requested shutdown".to_string(),
            })
        );
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::CancelReceived,
                ..
            })
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ControlClosed,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn control_dispatcher_keeps_reporting_after_local_cancel() {
        use crate::communication::peer_link::{LifecycleEvent, LinkEvent};

        let (tx, cancel_token, _shutdown, mut events_rx, dispatcher) = dispatcher_with_peer();
        // A local cancellation must not stop the dispatcher: a peer's Cancel
        // can still arrive while the shutdown handshake runs.
        cancel_token.cancel();
        tx.send(encode_control_message(&ControlMessage::Cancel {
            reason: "peer failed".to_string(),
        }))
        .await
        .unwrap();
        drop(tx);
        dispatcher.await.unwrap();

        let first = events_rx.recv().await;
        assert!(
            matches!(
                first,
                Some(LifecycleEvent::Link { ref peer, event: LinkEvent::CancelReceived }) if peer == "node-b"
            ),
            "got {first:?}"
        );
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ControlClosed,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn control_dispatcher_reports_undecodable_messages() {
        use crate::communication::peer_link::{LifecycleEvent, LinkEvent};

        let (tx, cancel_token, _shutdown, mut events_rx, dispatcher) = dispatcher_with_peer();
        let mut corrupted = encode_control_message(&ControlMessage::Cancel {
            reason: "x".to_string(),
        });
        corrupted[0] ^= 0xFF;
        tx.send(corrupted).await.unwrap();
        drop(tx);
        dispatcher.await.unwrap();
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ControlUndecodable(_),
                ..
            })
        ));
        assert!(!cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn control_dispatcher_applies_queued_cancel_before_shutdown() {
        let (tx, cancel_token, shutdown, _events_rx, dispatcher) = dispatcher_with_peer();
        tx.send(encode_control_message(&ControlMessage::Cancel {
            reason: "peer failed".to_string(),
        }))
        .await
        .unwrap();
        // Teardown races with the queued Cancel; the Cancel must win.
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), dispatcher)
            .await
            .expect("dispatcher should exit")
            .unwrap();
        assert!(cancel_token.is_cancelled());
        drop(tx);
    }

    #[tokio::test]
    async fn control_dispatcher_exits_on_shutdown() {
        let (_tx, cancel_token, shutdown, mut events_rx, dispatcher) = dispatcher_with_peer();

        // Signal teardown — dispatcher should exit promptly without
        // cancelling the dataflow token or reporting a closed link.
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(2), dispatcher)
            .await
            .expect("dispatcher should exit on shutdown")
            .unwrap();

        assert!(
            !cancel_token.is_cancelled(),
            "dataflow token should not be cancelled on clean shutdown"
        );
        assert!(events_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn broadcast_cancel_sends_to_all_peers() {
        let dataflow_id = DataflowId::new();
        let runtime = tokio::runtime::Handle::current();

        let (peer_a_local, peer_a_remote) = tokio::io::duplex(64 * 1024);
        let (peer_b_local, peer_b_remote) = tokio::io::duplex(64 * 1024);
        let (peer_a_reader, peer_a_writer) = tokio::io::split(peer_a_local);
        let (peer_b_reader, peer_b_writer) = tokio::io::split(peer_b_local);

        let (session, _) = crate::communication::transport_session::TransportSession::new(
            dataflow_id,
            vec![
                PeerConnection {
                    node_id: "node-a".into(),
                    reader: peer_a_reader,
                    writer: peer_a_writer,
                },
                PeerConnection {
                    node_id: "node-b".into(),
                    reader: peer_b_reader,
                    writer: peer_b_writer,
                },
            ],
            &[],
            &[],
            16,
            &runtime,
        )
        .unwrap();
        let transport = crate::communication::cluster_transport::ClusterTransport::Dedicated(
            std::sync::Arc::new(session),
        );

        broadcast_cancel_with_transport(&transport, dataflow_id, "worker failed").await;

        let mut remote_a = FramedReader::new(peer_a_remote);
        let mut remote_b = FramedReader::new(peer_b_remote);
        let frame_a = remote_a.read_frame().await.unwrap();
        let frame_b = remote_b.read_frame().await.unwrap();

        assert_eq!(frame_a.dataflow_id, dataflow_id);
        assert_eq!(frame_b.dataflow_id, dataflow_id);
        assert_eq!(
            decode_control_message(&frame_a.payload).unwrap(),
            ControlMessage::Cancel {
                reason: "worker failed".to_string(),
            }
        );
        assert_eq!(
            decode_control_message(&frame_b.payload).unwrap(),
            ControlMessage::Cancel {
                reason: "worker failed".to_string(),
            }
        );
    }

    // --- Concurrent handshake / barrier receive ---

    mod concurrent_receive {
        use super::super::*;
        use std::collections::HashMap;
        use tokio::sync::mpsc;

        type Senders = HashMap<&'static str, mpsc::Sender<Vec<u8>>>;

        fn peers(names: &[&'static str]) -> (Senders, HashMap<String, mpsc::Receiver<Vec<u8>>>) {
            let mut senders = HashMap::new();
            let mut receivers = HashMap::new();
            for name in names {
                let (tx, rx) = mpsc::channel(8);
                senders.insert(*name, tx);
                receivers.insert(name.to_string(), rx);
            }
            (senders, receivers)
        }

        fn ready(node: &str) -> Vec<u8> {
            encode_control_message(&ControlMessage::Ready {
                node_id: node.into(),
            })
        }

        fn cancel() -> Vec<u8> {
            encode_control_message(&ControlMessage::Cancel {
                reason: "worker failed: boom".into(),
            })
        }

        fn in_secs(secs: u64) -> tokio::time::Instant {
            tokio::time::Instant::now() + std::time::Duration::from_secs(secs)
        }

        #[tokio::test]
        async fn completes_when_every_peer_answers() {
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["c"].send(ready("c")).await.unwrap();
            tx["b"].send(ready("b")).await.unwrap();
            // A later message stays queued for the next phase.
            tx["b"].send(cancel()).await.unwrap();
            receive_from_each_peer(&mut rx, in_secs(5), false, validate_peer_ready)
                .await
                .unwrap();
            assert_eq!(rx.get_mut("b").unwrap().recv().await, Some(cancel()));
        }

        #[tokio::test]
        async fn peer_cancel_fails_fast_while_another_peer_is_silent() {
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["c"].send(cancel()).await.unwrap();
            let start = tokio::time::Instant::now();
            let err = receive_from_each_peer(&mut rx, in_secs(30), false, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::PeerCancelled { peer_id, .. } if peer_id == "c"),
                "{err:?}"
            );
            assert!(start.elapsed() < std::time::Duration::from_secs(5));
            drop(tx);
        }

        #[tokio::test]
        async fn barrier_watches_peers_that_already_answered() {
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["b"].send(ready("b")).await.unwrap();
            tx["b"].send(cancel()).await.unwrap();
            let err = receive_from_each_peer(&mut rx, in_secs(30), true, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::PeerCancelled { peer_id, .. } if peer_id == "b"),
                "{err:?}"
            );
            // A peer that answered and then disconnected is noticed too.
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["b"].send(ready("b")).await.unwrap();
            let mut tx = tx;
            tx.remove("b");
            let err = receive_from_each_peer(&mut rx, in_secs(30), true, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::PeerDisconnected { peer_id } if peer_id == "b"),
                "{err:?}"
            );
        }

        #[tokio::test]
        async fn barrier_fails_on_a_corrupt_message_from_an_answered_peer() {
            let mut corrupt = cancel();
            corrupt[2] ^= 0xFF;
            // While another peer is still awaited...
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["b"].send(ready("b")).await.unwrap();
            tx["b"].send(corrupt.clone()).await.unwrap();
            let err = receive_from_each_peer(&mut rx, in_secs(30), true, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::Decode { peer_id, .. } if peer_id == "b"),
                "{err:?}"
            );
            // ...and queued behind the last answer.
            let (tx, mut rx) = peers(&["b"]);
            tx["b"].send(ready("b")).await.unwrap();
            tx["b"].send(corrupt).await.unwrap();
            let err = receive_from_each_peer(&mut rx, in_secs(5), true, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::Decode { peer_id, .. } if peer_id == "b"),
                "{err:?}"
            );
        }

        #[tokio::test]
        async fn barrier_applies_a_cancel_queued_behind_the_last_answer() {
            let (tx, mut rx) = peers(&["b"]);
            tx["b"].send(ready("b")).await.unwrap();
            tx["b"].send(cancel()).await.unwrap();
            let err = receive_from_each_peer(&mut rx, in_secs(5), true, validate_peer_ready)
                .await
                .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::PeerCancelled { peer_id, .. } if peer_id == "b"),
                "{err:?}"
            );
        }

        #[tokio::test]
        async fn timeout_names_a_peer_that_did_not_answer() {
            let (tx, mut rx) = peers(&["b", "c"]);
            tx["b"].send(ready("b")).await.unwrap();
            let err = receive_from_each_peer(
                &mut rx,
                tokio::time::Instant::now() + std::time::Duration::from_millis(50),
                true,
                validate_peer_ready,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, HandshakeError::Timeout { peer_id } if peer_id == "c"),
                "{err:?}"
            );
        }

        #[tokio::test]
        async fn unexpected_message_is_rejected() {
            let (tx, mut rx) = peers(&["b"]);
            tx["b"].send(ready("b")).await.unwrap();
            let err =
                receive_from_each_peer(&mut rx, in_secs(5), false, |peer_id: &str, data: &[u8]| {
                    validate_peer_handshake(peer_id, data, 1, DataflowId::new())
                })
                .await
                .unwrap_err();
            assert!(
                matches!(err, HandshakeError::UnexpectedMessage { .. }),
                "{err:?}"
            );
        }
    }
}
