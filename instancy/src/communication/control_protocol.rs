//! Control protocol for cross-node cluster coordination.
//!
//! Provides handshake (fingerprint validation) and ready barrier messages
//! exchanged via [`TransportSession`](super::TransportSession)'s control
//! channel before dataflow execution begins.
//!
//! # Protocol Flow
//!
//! ```text
//! Node A                              Node B
//!   |-- Handshake {fp, df_id} ------->|
//!   |<------- Handshake {fp, df_id} --|
//!   |   (both validate fingerprints)  |
//!   |                                 |
//!   |   ... materialize workers ...   |
//!   |                                 |
//!   |-- Ready {node_id} ------------->|
//!   |<----------- Ready {node_id} ----|
//!   |   (both proceed to execution)   |
//! ```
//!
//! # Wire Format
//!
//! Each message is a single frame on control channel (ID 0):
//! ```text
//! [msg_type: u8][payload...][crc32: u32]
//! ```
//!
//! - Handshake (type 0): `[0][fingerprint: u64 LE][dataflow_id: 16 bytes UUID]`
//! - Ready (type 1): `[1][node_id_len: u32 LE][node_id: UTF-8 bytes]`
//! - Cancel (type 2): `[2][reason_len: u32 LE][reason: UTF-8 bytes]`

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
        #[from]
        source: std::str::Utf8Error,
    },

    /// The message type byte is not recognized.
    #[error("unknown control message type: {msg_type}")]
    UnknownMessageType { msg_type: u8 },

    /// An error occurred reading fixed-width fields from the wire.
    #[error("wire read error: {0}")]
    WireRead(Box<crate::Error>),
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
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

/// Control protocol message types.
#[cfg(feature = "transport")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    /// Handshake with graph fingerprint for compatibility validation.
    Handshake {
        fingerprint: u64,
        dataflow_id: DataflowId,
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
            fingerprint,
            dataflow_id,
        } => {
            buf.push(MSG_TYPE_HANDSHAKE);
            buf.extend_from_slice(&fingerprint.to_le_bytes());
            buf.extend_from_slice(dataflow_id.as_bytes());
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
            // 1 (type) + 8 (fingerprint) + 16 (dataflow_id UUID) = 25
            if payload.len() != 25 {
                return Err(ControlProtocolError::InvalidPayloadSize {
                    expected: 25,
                    actual: payload.len(),
                });
            }
            let fingerprint = wire::read_u64(payload, 1)
                .map_err(|e| ControlProtocolError::WireRead(Box::new(e)))?;
            let dataflow_id = DataflowId::from_bytes(
                wire::read_array::<16>(payload, 9)
                    .map_err(|e| ControlProtocolError::WireRead(Box::new(e)))?,
            );
            Ok(ControlMessage::Handshake {
                fingerprint,
                dataflow_id,
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

/// Compute a deterministic fingerprint for a dataflow graph.
///
/// The fingerprint captures the structural identity of the graph: operator
/// count, edges, exchange channels, feedback loops, and total worker count.
/// Two nodes running compatible dataflows will produce the same fingerprint.
///
/// This is used during the handshake to detect graph mismatches before
/// any data is exchanged.
#[cfg(feature = "transport")]
pub fn compute_fingerprint(
    operator_count: usize,
    edge_count: usize,
    exchange_edge_indices: &[usize],
    feedback_edge_count: usize,
    total_workers: usize,
) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    operator_count.hash(&mut hasher);
    edge_count.hash(&mut hasher);
    feedback_edge_count.hash(&mut hasher);
    total_workers.hash(&mut hasher);
    // Sort exchange indices for determinism.
    let mut sorted_exchanges = exchange_edge_indices.to_vec();
    sorted_exchanges.sort_unstable();
    for idx in &sorted_exchanges {
        idx.hash(&mut hasher);
    }
    hasher.finish()
}

// ---------------------------------------------------------------------------
// High-level handshake + ready barrier
// ---------------------------------------------------------------------------

/// Perform the control handshake with all remote peers.
///
/// Sends a `Handshake` message to each peer and waits to receive one back.
/// Validates that all peers report the same fingerprint.
///
/// # Errors
///
/// Returns an error if:
/// - Any peer's fingerprint doesn't match the local fingerprint
/// - A timeout expires before all peers respond
/// - A peer sends an invalid or unexpected message
#[cfg(feature = "transport")]
pub async fn perform_handshake(
    session: &super::TransportSession,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_fingerprint: u64,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Handshake {
        fingerprint: local_fingerprint,
        dataflow_id,
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
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| HandshakeError::Timeout {
                peer_id: peer_id.clone(),
            })?
            .ok_or_else(|| HandshakeError::PeerDisconnected {
                peer_id: peer_id.clone(),
            })?;

        let peer_msg = decode_control_message(&data).map_err(|source| HandshakeError::Decode {
            peer_id: peer_id.clone(),
            source,
        })?;

        match peer_msg {
            ControlMessage::Handshake {
                fingerprint,
                dataflow_id: peer_df_id,
            } => {
                if fingerprint != local_fingerprint {
                    return Err(HandshakeError::FingerprintMismatch {
                        peer_id: peer_id.clone(),
                        local: local_fingerprint,
                        remote: fingerprint,
                    });
                }
                if peer_df_id != dataflow_id {
                    return Err(HandshakeError::DataflowIdMismatch {
                        peer_id: peer_id.clone(),
                    });
                }
            }
            other => {
                return Err(HandshakeError::UnexpectedMessage {
                    peer_id: peer_id.clone(),
                    expected: "Handshake".into(),
                    got: format!("{other:?}"),
                });
            }
        }
    }

    Ok(())
}

/// Perform the ready barrier with all remote peers.
///
/// Sends a `Ready` signal to each peer and waits to receive one back.
/// This ensures all nodes have materialized their workers before any
/// node begins execution.
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
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| HandshakeError::Timeout {
                peer_id: peer_id.clone(),
            })?
            .ok_or_else(|| HandshakeError::PeerDisconnected {
                peer_id: peer_id.clone(),
            })?;

        let peer_msg = decode_control_message(&data).map_err(|source| HandshakeError::Decode {
            peer_id: peer_id.clone(),
            source,
        })?;

        match peer_msg {
            ControlMessage::Ready { .. } => {
                // Peer is ready.
            }
            other => {
                return Err(HandshakeError::UnexpectedMessage {
                    peer_id: peer_id.clone(),
                    expected: "Ready".into(),
                    got: format!("{other:?}"),
                });
            }
        }
    }

    Ok(())
}

/// Perform the handshake protocol using a [`ClusterTransport`](super::cluster_transport::ClusterTransport).
///
/// Functionally identical to [`perform_handshake`] but accepts the unified
/// transport abstraction instead of a raw `TransportSession`.
#[cfg(feature = "transport")]
pub async fn perform_handshake_with_transport(
    transport: &super::cluster_transport::ClusterTransport,
    control_receivers: &mut std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    local_fingerprint: u64,
    dataflow_id: DataflowId,
    timeout: std::time::Duration,
) -> Result<(), HandshakeError> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Handshake {
        fingerprint: local_fingerprint,
        dataflow_id,
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
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| HandshakeError::Timeout {
                peer_id: peer_id.clone(),
            })?
            .ok_or_else(|| HandshakeError::PeerDisconnected {
                peer_id: peer_id.clone(),
            })?;

        let peer_msg = decode_control_message(&data).map_err(|source| HandshakeError::Decode {
            peer_id: peer_id.clone(),
            source,
        })?;

        match peer_msg {
            ControlMessage::Handshake {
                fingerprint,
                dataflow_id: peer_df_id,
            } => {
                if fingerprint != local_fingerprint {
                    return Err(HandshakeError::FingerprintMismatch {
                        peer_id: peer_id.clone(),
                        local: local_fingerprint,
                        remote: fingerprint,
                    });
                }
                if peer_df_id != dataflow_id {
                    return Err(HandshakeError::DataflowIdMismatch {
                        peer_id: peer_id.clone(),
                    });
                }
            }
            other => {
                return Err(HandshakeError::UnexpectedMessage {
                    peer_id: peer_id.clone(),
                    expected: "Handshake".into(),
                    got: format!("{other:?}"),
                });
            }
        }
    }

    Ok(())
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
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| HandshakeError::Timeout {
                peer_id: peer_id.clone(),
            })?
            .ok_or_else(|| HandshakeError::PeerDisconnected {
                peer_id: peer_id.clone(),
            })?;

        let peer_msg = decode_control_message(&data).map_err(|source| HandshakeError::Decode {
            peer_id: peer_id.clone(),
            source,
        })?;

        match peer_msg {
            ControlMessage::Ready { .. } => {}
            other => {
                return Err(HandshakeError::UnexpectedMessage {
                    peer_id: peer_id.clone(),
                    expected: "Ready".into(),
                    got: format!("{other:?}"),
                });
            }
        }
    }

    Ok(())
}

/// Broadcast a cancellation message to all peers.
///
/// Called when the local dataflow is cancelled. Sending is best-effort: peers
/// may already have disconnected by the time cancellation propagates.
#[cfg(feature = "transport")]
pub async fn broadcast_cancel(
    session: &super::TransportSession,
    dataflow_id: DataflowId,
    reason: &str,
) {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let payload = encode_control_message(&ControlMessage::Cancel {
        reason: reason.to_string(),
    });

    for peer_id in session.peer_node_ids().collect::<Vec<_>>() {
        if let Some(sender) = session.control_sender(peer_id) {
            let frame = Frame {
                dataflow_id,
                channel_id: CONTROL_CHANNEL_ID,
                payload: payload.clone(),
            };
            let _ = sender.send(frame).await;
        }
    }
}

/// Broadcast a cancellation message to all peers using a [`ClusterTransport`].
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

    for peer_id in transport.peer_node_ids() {
        if let Some(sender) = transport.control_sender(&peer_id) {
            let frame = Frame {
                dataflow_id,
                channel_id: CONTROL_CHANNEL_ID,
                payload: payload.clone(),
            };
            let _ = sender.send(frame).await;
        }
    }
}

/// Spawn tasks that listen for peer cancellation messages.
///
/// When any peer sends `Cancel`, the local cancellation token is cancelled with
/// [`CancellationReason::PeerCancelled`]. The task exits when all receivers are
/// closed or when the local cancellation token is already cancelled.
#[cfg(feature = "transport")]
pub fn spawn_cancel_listener(
    control_receivers: std::collections::HashMap<String, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    cancel_token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut set = tokio::task::JoinSet::new();

        for (peer_id, mut rx) in control_receivers {
            let token = cancel_token.clone();
            set.spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled_async() => return,
                        maybe_data = rx.recv() => {
                            let Some(data) = maybe_data else {
                                return;
                            };
                            if let Ok(ControlMessage::Cancel { reason }) = decode_control_message(&data) {
                                token.cancel_with_reason(CancellationReason::PeerCancelled {
                                    peer_id,
                                    detail: reason,
                                });
                                return;
                            }
                        }
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

    #[test]
    fn handshake_roundtrip() {
        let msg = ControlMessage::Handshake {
            fingerprint: 0xDEADBEEF_CAFEBABE,
            dataflow_id: DataflowId::new(),
        };
        let encoded = encode_control_message(&msg);
        let decoded = decode_control_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
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
        let msg = ControlMessage::Handshake {
            fingerprint: 42,
            dataflow_id: DataflowId::new(),
        };
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

    #[test]
    fn fingerprint_deterministic() {
        let fp1 = compute_fingerprint(5, 4, &[1, 3], 1, 8);
        let fp2 = compute_fingerprint(5, 4, &[3, 1], 1, 8); // different order, same set
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn fingerprint_differs_on_structure_change() {
        let fp1 = compute_fingerprint(5, 4, &[1, 3], 1, 8);
        let fp2 = compute_fingerprint(6, 4, &[1, 3], 1, 8); // different operator count
        assert_ne!(fp1, fp2);

        let fp3 = compute_fingerprint(5, 4, &[1, 3], 1, 4); // different worker count
        assert_ne!(fp1, fp3);
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

    #[tokio::test]
    async fn spawn_cancel_listener_cancels_local_token() {
        let cancel_token = CancellationToken::new();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut control_receivers = std::collections::HashMap::new();
        control_receivers.insert("node-b".to_string(), rx);

        let listener = spawn_cancel_listener(control_receivers, cancel_token.clone());
        tx.send(encode_control_message(&ControlMessage::Cancel {
            reason: "user requested shutdown".to_string(),
        }))
        .await
        .unwrap();
        drop(tx);

        tokio::time::timeout(std::time::Duration::from_secs(2), cancel_token.cancelled_async())
            .await
            .expect("cancel token should be cancelled");
        listener.await.unwrap();

        assert_eq!(
            cancel_token.reason(),
            Some(CancellationReason::PeerCancelled {
                peer_id: "node-b".to_string(),
                detail: "user requested shutdown".to_string(),
            })
        );
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
        );

        broadcast_cancel(&session, dataflow_id, "worker failed").await;

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
}
