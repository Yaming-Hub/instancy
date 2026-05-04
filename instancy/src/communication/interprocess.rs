//! Inter-process communication layer.
//!
//! This module provides the bridge between the local Push/Pull channel
//! abstractions and the network transport layer. It enables dataflow edges
//! to transparently span process boundaries.
//!
//! # Architecture
//!
//! ```text
//! Local Operator → NetworkPush → Codec::encode → MuxerSender → Wire → Demuxer
//!                                                                        ↓
//! Local Operator ← NetworkPull ← Codec::decode ← ChannelReceiver ←──────┘
//! ```
//!
//! Each inter-process edge gets a unique `channel_id` on the wire. The
//! sending side serializes data batches and sends them through the muxer.
//! The receiving side deserializes from the demuxer's channel receiver.
//!
//! # Routing
//!
//! [`RoutingTable`] maps target worker indices to the appropriate
//! `RemoteEndpoint` (peer + channel_id), using the cluster topology to
//! determine which workers are local vs remote.

use std::collections::HashMap;

use crate::communication::codec::{Codec, CodecError};
use crate::communication::connection::PeerId;
use crate::execute::ClusterTopology;
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerId;

/// A unique identifier for a logical channel on the wire.
///
/// Each dataflow edge that crosses a process boundary is assigned a unique
/// channel ID. This ID is used in the frame header to multiplex/demultiplex
/// frames on a shared physical connection.
///
/// # Scope and Consistency
///
/// - **Per-dataflow**: Channel IDs are scoped to a single dataflow. Two different
///   dataflows may reuse the same numeric channel ID without conflict because
///   frames are always tagged with the `DataflowId` in the wire header.
/// - **Cluster-consistent**: The same logical edge gets the same `ChannelId` on
///   every node in the cluster. This is guaranteed by the deterministic assignment
///   during graph construction — all nodes construct the same graph and therefore
///   assign the same IDs.
/// - **Reserved**: `ChannelId = 0` (`PROGRESS_CHANNEL_ID`) is reserved for the
///   progress exchange protocol and must not be used for data channels.
///
/// # Assignment
///
/// Channel IDs are assigned sequentially by [`crate::communication::DataflowSession::allocate_channel`]
/// starting at 1 (since 0 is reserved). The allocation is deterministic given
/// the same graph construction order.
///
/// This is a **physical** wire-protocol concept — it maps a logical dataflow
/// edge to a specific multiplexed channel on the physical TCP connection.
pub type ChannelId = u64;

/// Describes a physical remote endpoint: which peer process and which wire channel.
///
/// This is a **physical** delivery target — it tells the transport layer exactly
/// where to send frames: which physical peer (PeerId) and which multiplexed
/// channel on that peer's connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteEndpoint {
    /// The physical peer process hosting the target worker.
    pub peer_id: PeerId,
    /// The physical wire channel ID for this edge on the shared connection.
    pub channel_id: ChannelId,
}

/// Maps logical worker indices to physical remote endpoints for a single dataflow edge.
///
/// This bridges **logical → physical**: given a logical target worker index,
/// it resolves the physical (peer_id, channel_id) needed to deliver data
/// over the network. Workers on the local node have no entry (handled in-process).
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// The local physical node identity in the cluster.
    local_node_id: String,
    /// Map from logical worker index to physical remote endpoint.
    /// Only contains entries for workers on remote physical nodes.
    remote_targets: HashMap<usize, RemoteEndpoint>,
    /// Total logical workers in the cluster.
    total_workers: usize,
}

impl RoutingTable {
    /// Build a routing table for a specific dataflow edge.
    ///
    /// `base_channel_id` is the starting channel ID for this edge.
    /// Each remote worker gets `base_channel_id + target_worker_index`.
    /// Must be > 0 since channel ID 0 is reserved for progress protocol.
    ///
    /// `peer_map` maps node_id → PeerId for all remote nodes.
    ///
    /// # Panics
    ///
    /// Panics if `base_channel_id` is 0 (reserved for progress).
    pub fn new(
        topology: &ClusterTopology,
        local_node_id: impl Into<String>,
        base_channel_id: ChannelId,
        peer_map: &HashMap<String, PeerId>,
    ) -> Self {
        assert!(
            base_channel_id > PROGRESS_CHANNEL_ID,
            "base_channel_id must be > 0 (channel 0 is reserved for progress)"
        );

        let local_node_id = local_node_id.into();
        let mut remote_targets = HashMap::new();

        for node in &topology.nodes {
            if node.node_id == local_node_id {
                continue;
            }
            if let Some(&peer_id) = peer_map.get(&node.node_id) {
                if let Some((start, end)) = topology.worker_range(&node.node_id) {
                    for worker_idx in start..end {
                        remote_targets.insert(
                            worker_idx,
                            RemoteEndpoint {
                                peer_id,
                                channel_id: base_channel_id + worker_idx as u64,
                            },
                        );
                    }
                }
            }
        }

        Self {
            local_node_id,
            remote_targets,
            total_workers: topology.total_workers(),
        }
    }

    /// Check if a target worker is remote (on a different physical node).
    pub fn is_remote(&self, logical_worker_index: usize) -> bool {
        self.remote_targets.contains_key(&logical_worker_index)
    }

    /// Get the physical remote endpoint for a logical target worker.
    /// Returns `None` if the worker is local (same physical node).
    pub fn endpoint(&self, logical_worker_index: usize) -> Option<&RemoteEndpoint> {
        self.remote_targets.get(&logical_worker_index)
    }

    /// Get the local node identity.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Total number of workers in the cluster.
    pub fn total_workers(&self) -> usize {
        self.total_workers
    }

    /// Iterator over all remote endpoints.
    pub fn remote_endpoints(&self) -> impl Iterator<Item = (&usize, &RemoteEndpoint)> {
        self.remote_targets.iter()
    }
}

/// Wire format for a data batch message.
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ source_worker: u32 LE │ time_len: u32 LE │ time_bytes           │
/// │ num_records: u32 LE   │ [record_len: u32 LE │ record_bytes]...  │
/// └─────────────────────────────────────────────────────────────────┘
/// ```
///
/// Each record in the batch is individually length-prefixed to allow
/// heterogeneous-size records within a single batch.

/// Encode a data batch (time + `Vec<D>`) into wire bytes.
///
/// The source_worker is included so the receiver knows which logical worker
/// produced this batch (important for exchange routing verification).
pub fn encode_data_batch<T, D, TC, DC>(
    source_worker: WorkerId,
    time: &T,
    data: &[D],
    time_codec: &TC,
    data_codec: &DC,
    buf: &mut Vec<u8>,
) -> Result<(), CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
    DC: Codec<D>,
{
    buf.clear();

    // Source worker (4 bytes)
    let worker_u32 = u32::try_from(source_worker.index())
        .map_err(|_| CodecError::Custom("source_worker index exceeds u32".into()))?;
    buf.extend_from_slice(&worker_u32.to_le_bytes());

    // Encode timestamp (length-prefixed)
    let time_start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder for time_len
    time_codec.encode(time, buf)?;
    let time_len = u32::try_from(buf.len() - time_start - 4)
        .map_err(|_| CodecError::Custom("encoded timestamp exceeds u32 length".into()))?;
    buf[time_start..time_start + 4].copy_from_slice(&time_len.to_le_bytes());

    // Number of records
    let num_records = u32::try_from(data.len())
        .map_err(|_| CodecError::Custom("batch record count exceeds u32".into()))?;
    buf.extend_from_slice(&num_records.to_le_bytes());

    // Each record (length-prefixed)
    for record in data {
        let rec_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // placeholder for record_len
        data_codec.encode(record, buf)?;
        let rec_len = u32::try_from(buf.len() - rec_start - 4)
            .map_err(|_| CodecError::Custom("encoded record exceeds u32 length".into()))?;
        buf[rec_start..rec_start + 4].copy_from_slice(&rec_len.to_le_bytes());
    }

    Ok(())
}

/// Decoded data batch: source worker, timestamp, and records.
#[derive(Debug, Clone, PartialEq)]
pub struct DataBatch<T, D> {
    /// The logical worker that produced this batch.
    pub source_worker: WorkerId,
    /// The timestamp for this batch.
    pub time: T,
    /// The data records.
    pub data: Vec<D>,
}

/// Decode a data batch from wire bytes.
pub fn decode_data_batch<T, D, TC, DC>(
    bytes: &[u8],
    time_codec: &TC,
    data_codec: &DC,
) -> Result<DataBatch<T, D>, CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
    DC: Codec<D>,
{
    if bytes.len() < 4 {
        return Err(CodecError::InsufficientData {
            needed: 4,
            available: bytes.len(),
        });
    }

    let source_worker = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;

    // Decode timestamp
    if offset + 4 > bytes.len() {
        return Err(CodecError::InsufficientData {
            needed: offset + 4,
            available: bytes.len(),
        });
    }
    let time_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    if offset + time_len > bytes.len() {
        return Err(CodecError::InsufficientData {
            needed: offset + time_len,
            available: bytes.len(),
        });
    }
    let (time, consumed) = time_codec.decode(&bytes[offset..offset + time_len])?;
    if consumed != time_len {
        return Err(CodecError::InvalidData(format!(
            "time codec consumed {consumed} bytes but header declared {time_len}"
        )));
    }
    offset += time_len;

    // Number of records
    if offset + 4 > bytes.len() {
        return Err(CodecError::InsufficientData {
            needed: offset + 4,
            available: bytes.len(),
        });
    }
    let num_records = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    // Cap pre-allocation to prevent DoS from malicious count.
    // Each record needs at least 4 bytes (length prefix), so cap at remaining/4.
    let remaining = bytes.len().saturating_sub(offset);
    let safe_capacity = num_records.min(remaining / 4);
    let mut data = Vec::with_capacity(safe_capacity);
    for _ in 0..num_records {
        if offset + 4 > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 4,
                available: bytes.len(),
            });
        }
        let rec_len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + rec_len > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + rec_len,
                available: bytes.len(),
            });
        }
        let (record, consumed) = data_codec.decode(&bytes[offset..offset + rec_len])?;
        if consumed != rec_len {
            return Err(CodecError::InvalidData(format!(
                "data codec consumed {consumed} bytes but header declared {rec_len}"
            )));
        }
        offset += rec_len;
        data.push(record);
    }

    Ok(DataBatch {
        source_worker: WorkerId::new(source_worker),
        time,
        data,
    })
}

/// Progress update message exchanged between processes.
///
/// Each process periodically sends its frontier changes to peers so they
/// can update their global view of progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressMessage<T> {
    /// The node identity that produced this update.
    pub source_node_id: String,
    /// Per-operator frontier changes: (operator_index, timestamp, delta).
    /// Positive delta = new capability, negative = released capability.
    pub changes: Vec<(usize, T, i64)>,
}

/// Well-known channel ID reserved for progress protocol messages.
pub const PROGRESS_CHANNEL_ID: ChannelId = 0;

/// Encode a progress message into wire bytes.
pub fn encode_progress<T, TC>(
    msg: &ProgressMessage<T>,
    time_codec: &TC,
    buf: &mut Vec<u8>,
) -> Result<(), CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
{
    buf.clear();

    // Source node_id (length-prefixed string: 4 bytes len + UTF-8 bytes)
    let node_bytes = msg.source_node_id.as_bytes();
    let node_len = u32::try_from(node_bytes.len())
        .map_err(|_| CodecError::Custom("source_node_id length exceeds u32".into()))?;
    buf.extend_from_slice(&node_len.to_le_bytes());
    buf.extend_from_slice(node_bytes);

    // Number of changes (4 bytes)
    let count_u32 = u32::try_from(msg.changes.len())
        .map_err(|_| CodecError::Custom("changes count exceeds u32".into()))?;
    buf.extend_from_slice(&count_u32.to_le_bytes());

    // Each change: operator_index (4 bytes) + time (length-prefixed) + delta (8 bytes)
    for (op_idx, time, delta) in &msg.changes {
        let idx_u32 = u32::try_from(*op_idx)
            .map_err(|_| CodecError::Custom("operator_index exceeds u32".into()))?;
        buf.extend_from_slice(&idx_u32.to_le_bytes());

        let time_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // placeholder
        time_codec.encode(time, buf)?;
        let time_len = u32::try_from(buf.len() - time_start - 4)
            .map_err(|_| CodecError::Custom("encoded timestamp exceeds u32 length".into()))?;
        buf[time_start..time_start + 4].copy_from_slice(&time_len.to_le_bytes());

        buf.extend_from_slice(&delta.to_le_bytes());
    }

    Ok(())
}

/// Decode a progress message from wire bytes.
pub fn decode_progress<T, TC>(
    bytes: &[u8],
    time_codec: &TC,
) -> Result<ProgressMessage<T>, CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
{
    if bytes.len() < 4 {
        return Err(CodecError::InsufficientData {
            needed: 4,
            available: bytes.len(),
        });
    }

    // Read source_node_id (length-prefixed string)
    let node_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    // Cap node_id length to prevent DoS from malicious peers
    const MAX_NODE_ID_LEN: usize = 512;
    if node_len > MAX_NODE_ID_LEN {
        return Err(CodecError::InvalidData(format!(
            "source_node_id length {node_len} exceeds maximum {MAX_NODE_ID_LEN}"
        )));
    }
    let mut offset = 4;
    if offset + node_len > bytes.len() {
        return Err(CodecError::InsufficientData {
            needed: offset + node_len,
            available: bytes.len(),
        });
    }
    let source_node_id = String::from_utf8(bytes[offset..offset + node_len].to_vec())
        .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8 in source_node_id: {e}")))?;
    offset += node_len;

    // Read count
    if offset + 4 > bytes.len() {
        return Err(CodecError::InsufficientData {
            needed: offset + 4,
            available: bytes.len(),
        });
    }
    let count = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    // Cap pre-allocation: each change needs at least 4+4+time+8 bytes (min ~16)
    let remaining = bytes.len().saturating_sub(offset);
    let safe_capacity = count.min(remaining / 16);
    let mut changes = Vec::with_capacity(safe_capacity);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 4,
                available: bytes.len(),
            });
        }
        let op_idx = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + 4 > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 4,
                available: bytes.len(),
            });
        }
        let time_len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + time_len > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + time_len,
                available: bytes.len(),
            });
        }
        let (time, consumed) = time_codec.decode(&bytes[offset..offset + time_len])?;
        if consumed != time_len {
            return Err(CodecError::InvalidData(format!(
                "progress: time codec consumed {consumed} but header says {time_len}"
            )));
        }
        offset += time_len;

        if offset + 8 > bytes.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: bytes.len(),
            });
        }
        let delta = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        changes.push((op_idx, time, delta));
    }

    Ok(ProgressMessage {
        source_node_id,
        changes,
    })
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::codec::FixedSizeCodec;
    use crate::execute::NodeConfig;

    // --- RoutingTable tests ---

    #[test]
    fn routing_table_single_node_no_remotes() {
        let topology = ClusterTopology::single_node(4);
        let peer_map = HashMap::new();
        let table = RoutingTable::new(&topology, "local", 100, &peer_map);

        assert_eq!(table.total_workers(), 4);
        assert_eq!(table.local_node_id(), "local");
        for i in 0..4 {
            assert!(!table.is_remote(i));
            assert!(table.endpoint(i).is_none());
        }
    }

    #[test]
    fn routing_table_multi_node() {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 2),
            NodeConfig::new("node-1", 3),
            NodeConfig::new("node-2", 1),
        ])
        .unwrap();

        let mut peer_map = HashMap::new();
        peer_map.insert("node-1".into(), PeerId(100));
        peer_map.insert("node-2".into(), PeerId(200));

        let table = RoutingTable::new(&topology, "node-0", 1000, &peer_map);

        assert_eq!(table.total_workers(), 6);

        // Workers 0,1 are local (node 0)
        assert!(!table.is_remote(0));
        assert!(!table.is_remote(1));

        // Workers 2,3,4 are on node 1 (peer 100)
        assert!(table.is_remote(2));
        assert!(table.is_remote(3));
        assert!(table.is_remote(4));
        assert_eq!(
            table.endpoint(2),
            Some(&RemoteEndpoint {
                peer_id: PeerId(100),
                channel_id: 1002,
            })
        );
        assert_eq!(
            table.endpoint(4),
            Some(&RemoteEndpoint {
                peer_id: PeerId(100),
                channel_id: 1004,
            })
        );

        // Worker 5 is on node 2 (peer 200)
        assert!(table.is_remote(5));
        assert_eq!(
            table.endpoint(5),
            Some(&RemoteEndpoint {
                peer_id: PeerId(200),
                channel_id: 1005,
            })
        );
    }

    #[test]
    fn routing_table_from_non_zero_node() {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 2),
            NodeConfig::new("node-1", 2),
        ])
        .unwrap();

        let mut peer_map = HashMap::new();
        peer_map.insert("node-0".into(), PeerId(10));

        // We are node 1
        let table = RoutingTable::new(&topology, "node-1", 500, &peer_map);

        // Workers 0,1 are remote (node 0, peer 10)
        assert!(table.is_remote(0));
        assert!(table.is_remote(1));
        assert_eq!(
            table.endpoint(0),
            Some(&RemoteEndpoint {
                peer_id: PeerId(10),
                channel_id: 500,
            })
        );

        // Workers 2,3 are local (node 1)
        assert!(!table.is_remote(2));
        assert!(!table.is_remote(3));
    }

    #[test]
    fn routing_table_remote_endpoints_iter() {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 1),
            NodeConfig::new("node-1", 2),
        ])
        .unwrap();

        let mut peer_map = HashMap::new();
        peer_map.insert("node-1".into(), PeerId(42));

        let table = RoutingTable::new(&topology, "node-0", 1, &peer_map);

        let endpoints: Vec<_> = table.remote_endpoints().collect();
        assert_eq!(endpoints.len(), 2);
    }

    #[test]
    #[should_panic(expected = "base_channel_id must be > 0")]
    fn routing_table_rejects_channel_id_zero() {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 1),
            NodeConfig::new("node-1", 2),
        ])
        .unwrap();
        let mut peer_map = HashMap::new();
        peer_map.insert("node-1".into(), PeerId(42));
        let _table = RoutingTable::new(&topology, "node-0", 0, &peer_map);
    }

    // --- Data batch encode/decode tests ---

    #[test]
    fn encode_decode_data_batch_u64() {
        let time_codec = FixedSizeCodec::<u64>::new();
        let data_codec = FixedSizeCodec::<u64>::new();

        let mut buf = Vec::new();
        encode_data_batch(
            WorkerId::new(3),
            &42u64,
            &[100u64, 200, 300],
            &time_codec,
            &data_codec,
            &mut buf,
        )
        .unwrap();

        let batch = decode_data_batch::<u64, u64, _, _>(&buf, &time_codec, &data_codec).unwrap();
        assert_eq!(batch.source_worker, WorkerId::new(3));
        assert_eq!(batch.time, 42);
        assert_eq!(batch.data, vec![100, 200, 300]);
    }

    #[test]
    fn encode_decode_data_batch_empty() {
        let time_codec = FixedSizeCodec::<u64>::new();
        let data_codec = FixedSizeCodec::<u64>::new();

        let mut buf = Vec::new();
        let empty: &[u64] = &[];
        encode_data_batch(
            WorkerId::new(0),
            &0u64,
            empty,
            &time_codec,
            &data_codec,
            &mut buf,
        )
        .unwrap();

        let batch = decode_data_batch::<u64, u64, _, _>(&buf, &time_codec, &data_codec).unwrap();
        assert_eq!(batch.source_worker, WorkerId::new(0));
        assert_eq!(batch.time, 0);
        assert!(batch.data.is_empty());
    }

    #[test]
    fn encode_decode_data_batch_single_record() {
        let time_codec = FixedSizeCodec::<u32>::new();
        let data_codec = FixedSizeCodec::<i64>::new();

        let mut buf = Vec::new();
        encode_data_batch(
            WorkerId::new(7),
            &99u32,
            &[-1i64],
            &time_codec,
            &data_codec,
            &mut buf,
        )
        .unwrap();

        let batch = decode_data_batch::<u32, i64, _, _>(&buf, &time_codec, &data_codec).unwrap();
        assert_eq!(batch.source_worker, WorkerId::new(7));
        assert_eq!(batch.time, 99);
        assert_eq!(batch.data, vec![-1i64]);
    }

    #[test]
    fn decode_data_batch_too_short() {
        let time_codec = FixedSizeCodec::<u64>::new();
        let data_codec = FixedSizeCodec::<u64>::new();

        let result = decode_data_batch::<u64, u64, _, _>(&[1, 2, 3], &time_codec, &data_codec);
        assert!(result.is_err());
    }

    #[test]
    fn decode_data_batch_truncated_records() {
        let time_codec = FixedSizeCodec::<u64>::new();
        let data_codec = FixedSizeCodec::<u64>::new();

        let mut buf = Vec::new();
        encode_data_batch(
            WorkerId::new(0),
            &1u64,
            &[2u64, 3, 4],
            &time_codec,
            &data_codec,
            &mut buf,
        )
        .unwrap();

        // Truncate to remove last record
        buf.truncate(buf.len() - 8);
        let result = decode_data_batch::<u64, u64, _, _>(&buf, &time_codec, &data_codec);
        assert!(result.is_err());
    }

    // --- Progress message encode/decode tests ---

    #[test]
    fn encode_decode_progress_empty() {
        let msg = ProgressMessage::<u64> {
            source_node_id: "node-0".into(),
            changes: vec![],
        };

        let codec = FixedSizeCodec::<u64>::new();
        let mut buf = Vec::new();
        encode_progress(&msg, &codec, &mut buf).unwrap();

        let decoded = decode_progress::<u64, _>(&buf, &codec).unwrap();
        assert_eq!(decoded.source_node_id, "node-0");
        assert!(decoded.changes.is_empty());
    }

    #[test]
    fn encode_decode_progress_multiple_changes() {
        let msg = ProgressMessage::<u64> {
            source_node_id: "node-2".into(),
            changes: vec![(0, 10, 1), (1, 20, -1), (3, 5, 2)],
        };

        let codec = FixedSizeCodec::<u64>::new();
        let mut buf = Vec::new();
        encode_progress(&msg, &codec, &mut buf).unwrap();

        let decoded = decode_progress::<u64, _>(&buf, &codec).unwrap();
        assert_eq!(decoded.source_node_id, "node-2");
        assert_eq!(decoded.changes.len(), 3);
        assert_eq!(decoded.changes[0], (0, 10, 1));
        assert_eq!(decoded.changes[1], (1, 20, -1));
        assert_eq!(decoded.changes[2], (3, 5, 2));
    }

    #[test]
    fn decode_progress_too_short() {
        let codec = FixedSizeCodec::<u64>::new();
        let result = decode_progress::<u64, _>(&[1, 2, 3], &codec);
        assert!(result.is_err());
    }

    #[test]
    fn decode_progress_truncated_change() {
        let codec = FixedSizeCodec::<u64>::new();

        // Valid header: empty node_id (len=0) + count=1 but no change data
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes()); // node_id len = 0
        // no node_id bytes
        buf.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        // No actual change data

        let result = decode_progress::<u64, _>(&buf, &codec);
        assert!(result.is_err());
    }

    #[test]
    fn progress_channel_id_is_zero() {
        assert_eq!(PROGRESS_CHANNEL_ID, 0);
    }

    // --- Round-trip consistency tests ---

    #[test]
    fn data_batch_encode_is_deterministic() {
        let tc = FixedSizeCodec::<u64>::new();
        let dc = FixedSizeCodec::<u64>::new();

        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_data_batch(WorkerId::new(7), &99u64, &[42u64], &tc, &dc, &mut buf1).unwrap();
        encode_data_batch(WorkerId::new(7), &99u64, &[42u64], &tc, &dc, &mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn progress_encode_is_deterministic() {
        let msg = ProgressMessage::<u64> {
            source_node_id: "node-1".into(),
            changes: vec![(0, 5, 1), (2, 10, -1)],
        };

        let tc = FixedSizeCodec::<u64>::new();
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        encode_progress(&msg, &tc, &mut buf1).unwrap();
        encode_progress(&msg, &tc, &mut buf2).unwrap();
        assert_eq!(buf1, buf2);
    }

    #[test]
    fn data_batch_wire_size_is_reasonable() {
        let tc = FixedSizeCodec::<u64>::new();
        let dc = FixedSizeCodec::<u64>::new();

        let mut buf = Vec::new();
        encode_data_batch(WorkerId::new(0), &0u64, &[0u64], &tc, &dc, &mut buf).unwrap();
        // worker(4) + time_len(4) + time(8) + num_records(4) + rec_len(4) + rec(8) = 32
        assert_eq!(buf.len(), 32);
    }

    #[test]
    fn data_batch_many_records() {
        let tc = FixedSizeCodec::<u64>::new();
        let dc = FixedSizeCodec::<u32>::new();

        let records: Vec<u32> = (0..1000).collect();
        let mut buf = Vec::new();
        encode_data_batch(WorkerId::new(0), &100u64, &records, &tc, &dc, &mut buf).unwrap();

        let batch = decode_data_batch::<u64, u32, _, _>(&buf, &tc, &dc).unwrap();
        assert_eq!(batch.data.len(), 1000);
        assert_eq!(batch.data[999], 999);
    }
}
