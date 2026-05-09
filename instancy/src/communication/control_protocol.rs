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

#[cfg(feature = "transport")]
use crate::dataflow::id::DataflowId;

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
}

#[cfg(feature = "transport")]
const MSG_TYPE_HANDSHAKE: u8 = 0;
#[cfg(feature = "transport")]
const MSG_TYPE_READY: u8 = 1;

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
    }
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode a control message from bytes (verifies CRC32 trailer).
#[cfg(feature = "transport")]
pub fn decode_control_message(data: &[u8]) -> Result<ControlMessage, String> {
    // Minimum: 1 byte type + 4 bytes CRC
    if data.len() < 5 {
        return Err(format!("control message too short: {} bytes", data.len()));
    }

    // Verify CRC32
    let (payload, crc_bytes) = data.split_at(data.len() - 4);
    let expected_crc = u32::from_le_bytes(crc_bytes.try_into().expect("CRC trailer is 4 bytes"));
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(format!(
            "CRC32 mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        ));
    }

    match payload[0] {
        MSG_TYPE_HANDSHAKE => {
            // 1 (type) + 8 (fingerprint) + 16 (dataflow_id UUID) = 25
            if payload.len() != 25 {
                return Err(format!(
                    "handshake payload wrong size: expected 25, got {}",
                    payload.len()
                ));
            }
            let fingerprint =
                u64::from_le_bytes(payload[1..9].try_into().expect("fingerprint is 8 bytes"));
            let df_bytes: [u8; 16] = payload[9..25]
                .try_into()
                .map_err(|_| "invalid dataflow_id bytes in handshake".to_string())?;
            let dataflow_id = DataflowId::from_bytes(df_bytes);
            Ok(ControlMessage::Handshake {
                fingerprint,
                dataflow_id,
            })
        }
        MSG_TYPE_READY => {
            // 1 (type) + 4 (len) + node_id bytes
            if payload.len() < 5 {
                return Err("ready payload too short".to_string());
            }
            let id_len = u32::from_le_bytes(
                payload[1..5]
                    .try_into()
                    .expect("node ID length prefix is 4 bytes"),
            ) as usize;
            if payload.len() != 5 + id_len {
                return Err(format!(
                    "ready payload size mismatch: expected {}, got {}",
                    5 + id_len,
                    payload.len()
                ));
            }
            let node_id = std::str::from_utf8(&payload[5..5 + id_len])
                .map_err(|e| format!("invalid UTF-8 in node_id: {e}"))?
                .to_string();
            Ok(ControlMessage::Ready { node_id })
        }
        other => Err(format!("unknown control message type: {other}")),
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
) -> Result<(), String> {
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
            .ok_or_else(|| format!("no control sender for peer {peer_id}"))?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| format!("failed to send handshake to peer {peer_id}"))?;
    }

    // Receive handshake from all peers.
    let deadline = tokio::time::Instant::now() + timeout;
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| format!("handshake timeout waiting for peer {peer_id}"))?
            .ok_or_else(|| format!("peer {peer_id} disconnected during handshake"))?;

        let peer_msg = decode_control_message(&data)
            .map_err(|e| format!("invalid handshake from peer {peer_id}: {e}"))?;

        match peer_msg {
            ControlMessage::Handshake {
                fingerprint,
                dataflow_id: peer_df_id,
            } => {
                if fingerprint != local_fingerprint {
                    return Err(format!(
                        "fingerprint mismatch with peer {peer_id}: \
                         local={local_fingerprint:#018x}, remote={fingerprint:#018x}"
                    ));
                }
                if peer_df_id != dataflow_id {
                    return Err(format!("dataflow_id mismatch with peer {peer_id}"));
                }
            }
            other => {
                return Err(format!(
                    "unexpected control message from peer {peer_id}: expected Handshake, got {other:?}"
                ));
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
) -> Result<(), String> {
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
            .ok_or_else(|| format!("no control sender for peer {peer_id}"))?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| format!("failed to send Ready to peer {peer_id}"))?;
    }

    // Receive Ready from all peers.
    let deadline = tokio::time::Instant::now() + timeout;
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| format!("ready barrier timeout waiting for peer {peer_id}"))?
            .ok_or_else(|| format!("peer {peer_id} disconnected during ready barrier"))?;

        let peer_msg = decode_control_message(&data)
            .map_err(|e| format!("invalid Ready from peer {peer_id}: {e}"))?;

        match peer_msg {
            ControlMessage::Ready { .. } => {
                // Peer is ready.
            }
            other => {
                return Err(format!(
                    "unexpected control message from peer {peer_id}: expected Ready, got {other:?}"
                ));
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
) -> Result<(), String> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Handshake {
        fingerprint: local_fingerprint,
        dataflow_id,
    };
    let payload = encode_control_message(&msg);

    for peer_id in transport.peer_node_ids() {
        let sender = transport
            .control_sender(&peer_id)
            .ok_or_else(|| format!("no control sender for peer {peer_id}"))?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| format!("failed to send handshake to peer {peer_id}"))?;
    }

    let deadline = tokio::time::Instant::now() + timeout;
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| format!("handshake timeout waiting for peer {peer_id}"))?
            .ok_or_else(|| format!("peer {peer_id} disconnected during handshake"))?;

        let peer_msg = decode_control_message(&data)
            .map_err(|e| format!("invalid handshake from peer {peer_id}: {e}"))?;

        match peer_msg {
            ControlMessage::Handshake {
                fingerprint,
                dataflow_id: peer_df_id,
            } => {
                if fingerprint != local_fingerprint {
                    return Err(format!(
                        "fingerprint mismatch with peer {peer_id}: \
                         local={local_fingerprint:#018x}, remote={fingerprint:#018x}"
                    ));
                }
                if peer_df_id != dataflow_id {
                    return Err(format!("dataflow_id mismatch with peer {peer_id}"));
                }
            }
            other => {
                return Err(format!(
                    "unexpected control message from peer {peer_id}: expected Handshake, got {other:?}"
                ));
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
) -> Result<(), String> {
    use crate::communication::transport::Frame;
    use crate::communication::transport_session::CONTROL_CHANNEL_ID;

    let msg = ControlMessage::Ready {
        node_id: local_node_id.to_string(),
    };
    let payload = encode_control_message(&msg);

    for peer_id in transport.peer_node_ids() {
        let sender = transport
            .control_sender(&peer_id)
            .ok_or_else(|| format!("no control sender for peer {peer_id}"))?;
        let frame = Frame {
            dataflow_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: payload.clone(),
        };
        sender
            .send(frame)
            .await
            .map_err(|_| format!("failed to send Ready to peer {peer_id}"))?;
    }

    let deadline = tokio::time::Instant::now() + timeout;
    for (peer_id, rx) in control_receivers.iter_mut() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let data = tokio::time::timeout(remaining, rx.recv())
            .await
            .map_err(|_| format!("ready barrier timeout waiting for peer {peer_id}"))?
            .ok_or_else(|| format!("peer {peer_id} disconnected during ready barrier"))?;

        let peer_msg = decode_control_message(&data)
            .map_err(|e| format!("invalid Ready from peer {peer_id}: {e}"))?;

        match peer_msg {
            ControlMessage::Ready { .. } => {}
            other => {
                return Err(format!(
                    "unexpected control message from peer {peer_id}: expected Ready, got {other:?}"
                ));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "transport")]
mod tests {
    use super::*;

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
    fn decode_rejects_corrupted_crc() {
        let msg = ControlMessage::Handshake {
            fingerprint: 42,
            dataflow_id: DataflowId::new(),
        };
        let mut encoded = encode_control_message(&msg);
        // Flip a bit in the payload.
        encoded[5] ^= 0x01;
        let result = decode_control_message(&encoded);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CRC32 mismatch"));
    }

    #[test]
    fn decode_rejects_too_short() {
        let result = decode_control_message(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_rejects_unknown_type() {
        // Craft valid CRC for unknown type byte.
        let mut buf = vec![0xFF_u8];
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let result = decode_control_message(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown control message type"));
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
}
