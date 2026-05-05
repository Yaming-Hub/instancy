//! Inter-process progress exchange.
//!
//! The [`ProgressExchange`] component handles sending local progress updates
//! to remote peers and receiving remote progress updates to integrate into
//! the local progress tracker.
//!
//! # Architecture
//!
//! ```text
//! Local ProgressTracker
//!       │ (outbound changes)
//!       ▼
//! ProgressExchange::broadcast_local_changes()
//!       │
//!       ├─► encode_progress → FrameSender (to peer 1)
//!       ├─► encode_progress → FrameSender (to peer 2)
//!       └─► ...
//!
//! Remote Peer
//!       │ (inbound frames on PROGRESS_CHANNEL_ID)
//!       ▼
//! ProgressExchange::receive_remote_changes()
//!       │
//!       ▼
//!   decode_progress → Vec<ProgressMessage<T>>
//! ```
//!
//! # Priority
//!
//! Progress messages use a dedicated `FrameSender` with its own capacity,
//! separate from data channels. This ensures progress traffic is not blocked
//! by data backpressure, preventing deadlocks where data flow stalls because
//! frontiers cannot advance.

use std::sync::Arc;

use crate::communication::codec::Codec;
use crate::communication::interprocess::{
    PROGRESS_CHANNEL_ID, ProgressMessage, decode_progress, encode_progress,
};
use crate::communication::remote_push::{FrameReceiver, FrameSender, OutboundFrame};
use crate::dataflow::id::DataflowId;
use crate::error::Error;
use crate::progress::timestamp::Timestamp;

/// Configuration for the progress exchange component.
#[derive(Debug, Clone)]
pub struct ProgressExchangeConfig {
    /// Buffer capacity for the progress frame sender (per peer).
    /// Separate from data to avoid head-of-line blocking.
    /// Default: 256.
    pub progress_buffer_capacity: usize,
}

impl Default for ProgressExchangeConfig {
    fn default() -> Self {
        Self {
            progress_buffer_capacity: 256,
        }
    }
}

/// Per-peer progress sender.
///
/// One instance per remote peer, used to send progress updates.
#[derive(Debug)]
pub struct PeerProgressSender {
    /// The peer node identity this sender targets.
    pub peer_node_id: String,
    /// Frame sender (dedicated progress channel, separate from data).
    sender: FrameSender,
}

impl PeerProgressSender {
    /// Create a new peer progress sender.
    ///
    /// Returns the sender and its corresponding receiver (for the mux task).
    pub fn new(peer_node_id: impl Into<String>, buffer_capacity: usize) -> (Self, FrameReceiver) {
        let (sender, receiver) = FrameSender::channel(buffer_capacity);
        (
            Self {
                peer_node_id: peer_node_id.into(),
                sender,
            },
            receiver,
        )
    }
}

/// Manages inter-process progress exchange for a single dataflow.
///
/// Each dataflow gets its own `ProgressExchange` instance. Progress frames
/// are tagged with the dataflow's ID and sent on `PROGRESS_CHANNEL_ID`.
pub struct ProgressExchange<T, TC> {
    /// The dataflow this exchange serves.
    dataflow_id: DataflowId,
    /// This node's identity (used as source_node in outbound messages).
    local_node_id: String,
    /// Per-peer progress senders.
    peer_senders: Vec<PeerProgressSender>,
    /// Codec for encoding/decoding timestamps.
    time_codec: Arc<TC>,
    /// Type witness for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

impl<T, TC> ProgressExchange<T, TC>
where
    T: Timestamp,
    TC: Codec<T>,
{
    /// Create a new progress exchange.
    ///
    /// # Arguments
    ///
    /// * `dataflow_id` — The dataflow this exchange serves
    /// * `local_node_id` — This node's identity
    /// * `peer_senders` — Pre-created senders for each remote peer
    /// * `time_codec` — Codec for timestamp serialization
    pub fn new(
        dataflow_id: DataflowId,
        local_node_id: impl Into<String>,
        peer_senders: Vec<PeerProgressSender>,
        time_codec: Arc<TC>,
    ) -> Self {
        Self {
            dataflow_id,
            local_node_id: local_node_id.into(),
            peer_senders,
            time_codec,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Broadcast local progress changes to all remote peers.
    ///
    /// Takes a set of frontier changes from the local progress tracker
    /// and sends them to every peer. Uses try_send — if any peer's progress
    /// buffer is full, returns `Error::Backpressure` so the caller can
    /// retry with the same changes later (progress deltas are non-idempotent
    /// and must not be dropped).
    ///
    /// Returns the number of peers that accepted the update, or an error
    /// if delivery failed (backpressure or transport error).
    pub fn broadcast_local_changes(&self, changes: &[(usize, T, i64)]) -> Result<usize, Error> {
        if changes.is_empty() || self.peer_senders.is_empty() {
            return Ok(0);
        }

        let msg = ProgressMessage {
            source_node_id: self.local_node_id.clone(),
            changes: changes.to_vec(),
        };

        let mut buf = Vec::new();
        encode_progress(&msg, self.time_codec.as_ref(), &mut buf).map_err(Error::codec)?;

        let mut accepted = 0;
        for peer in &self.peer_senders {
            let frame = OutboundFrame {
                dataflow_id: self.dataflow_id,
                channel_id: PROGRESS_CHANNEL_ID,
                payload: buf.clone(),
            };
            match peer.sender.try_send(frame) {
                Ok(()) => accepted += 1,
                Err(Error::Backpressure) => {
                    // Progress deltas are non-idempotent — cannot drop them.
                    // Return backpressure so caller retains changes for retry.
                    return Err(Error::Backpressure);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(accepted)
    }

    /// Decode a received progress frame payload into a ProgressMessage.
    ///
    /// Called by the demux layer when a frame arrives on PROGRESS_CHANNEL_ID
    /// for this dataflow.
    pub fn decode_remote_progress(&self, payload: &[u8]) -> Result<ProgressMessage<T>, Error> {
        decode_progress(payload, self.time_codec.as_ref()).map_err(Error::codec)
    }

    /// Get this exchange's dataflow ID.
    pub fn dataflow_id(&self) -> DataflowId {
        self.dataflow_id
    }

    /// Get the number of remote peers.
    pub fn peer_count(&self) -> usize {
        self.peer_senders.len()
    }
}

impl<T, TC> std::fmt::Debug for ProgressExchange<T, TC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressExchange")
            .field("dataflow_id", &self.dataflow_id)
            .field("local_node_id", &self.local_node_id)
            .field("peer_count", &self.peer_senders.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::codec::CodecError;
    use crate::communication::interprocess::decode_progress;

    /// Simple u64 timestamp codec for testing.
    struct U64Codec;

    impl Codec<u64> for U64Codec {
        fn encode(&self, value: &u64, buf: &mut Vec<u8>) -> Result<(), CodecError> {
            buf.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }

        fn decode(&self, buf: &[u8]) -> Result<(u64, usize), CodecError> {
            if buf.len() < 8 {
                return Err(CodecError::InsufficientData {
                    needed: 8,
                    available: buf.len(),
                });
            }
            let val = u64::from_le_bytes(buf[..8].try_into().unwrap());
            Ok((val, 8))
        }
    }

    #[test]
    fn broadcast_to_multiple_peers() {
        let config = ProgressExchangeConfig::default();
        let dataflow_id = DataflowId::from_bytes([1u8; 16]);

        let (sender1, receiver1) =
            PeerProgressSender::new("node-1", config.progress_buffer_capacity);
        let (sender2, receiver2) =
            PeerProgressSender::new("node-2", config.progress_buffer_capacity);

        let exchange = ProgressExchange::new(
            dataflow_id,
            "node-0",
            vec![sender1, sender2],
            Arc::new(U64Codec),
        );

        let changes = vec![
            (0, 10u64, 1),  // operator 0, time 10, +1
            (1, 20u64, -1), // operator 1, time 20, -1
        ];

        let accepted = exchange.broadcast_local_changes(&changes).unwrap();
        assert_eq!(accepted, 2);

        // Both peers received the message
        let frame1 = receiver1.recv().unwrap();
        let frame2 = receiver2.recv().unwrap();

        assert_eq!(frame1.dataflow_id, dataflow_id);
        assert_eq!(frame1.channel_id, PROGRESS_CHANNEL_ID);
        assert_eq!(frame1.payload, frame2.payload);

        // Decode and verify
        let msg = decode_progress::<u64, U64Codec>(&frame1.payload, &U64Codec).unwrap();
        assert_eq!(msg.source_node_id, "node-0");
        assert_eq!(msg.changes.len(), 2);
        assert_eq!(msg.changes[0], (0, 10u64, 1));
        assert_eq!(msg.changes[1], (1, 20u64, -1));
    }

    #[test]
    fn broadcast_empty_changes_is_noop() {
        let (sender, _receiver) = PeerProgressSender::new("node-1", 16);
        let exchange = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![sender],
            Arc::new(U64Codec),
        );

        let accepted = exchange.broadcast_local_changes(&[]).unwrap();
        assert_eq!(accepted, 0);
    }

    #[test]
    fn broadcast_no_peers_is_noop() {
        let exchange: ProgressExchange<u64, U64Codec> = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![],
            Arc::new(U64Codec),
        );

        let changes = vec![(0, 1u64, 1)];
        let accepted = exchange.broadcast_local_changes(&changes).unwrap();
        assert_eq!(accepted, 0);
    }

    #[test]
    fn broadcast_handles_full_peer_buffer() {
        // Create a sender with capacity 1
        let (sender, receiver) = PeerProgressSender::new("node-1", 1);
        let exchange = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![sender],
            Arc::new(U64Codec),
        );

        let changes = vec![(0, 1u64, 1)];

        // First send succeeds
        let accepted = exchange.broadcast_local_changes(&changes).unwrap();
        assert_eq!(accepted, 1);

        // Second send — buffer is full, should return Error::Backpressure
        let result = exchange.broadcast_local_changes(&changes);
        assert!(matches!(result, Err(Error::Backpressure)));

        // Drain the receiver
        let frame = receiver.recv().unwrap();
        assert_eq!(frame.channel_id, PROGRESS_CHANNEL_ID);
    }

    #[test]
    fn decode_remote_progress_roundtrip() {
        let exchange: ProgressExchange<u64, U64Codec> = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![],
            Arc::new(U64Codec),
        );

        // Manually encode a progress message
        let msg = ProgressMessage {
            source_node_id: "node-3".into(),
            changes: vec![(5, 100u64, -2)],
        };
        let mut buf = Vec::new();
        encode_progress(&msg, &U64Codec, &mut buf).unwrap();

        // Decode via exchange
        let decoded = exchange.decode_remote_progress(&buf).unwrap();
        assert_eq!(decoded.source_node_id, "node-3");
        assert_eq!(decoded.changes, vec![(5, 100u64, -2)]);
    }

    #[test]
    fn decode_remote_progress_invalid_data() {
        let exchange: ProgressExchange<u64, U64Codec> = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![],
            Arc::new(U64Codec),
        );

        // Too short
        let result = exchange.decode_remote_progress(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn progress_exchange_debug() {
        let (sender, _) = PeerProgressSender::new("node-1", 8);
        let exchange = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![sender],
            Arc::new(U64Codec),
        );
        let dbg = format!("{exchange:?}");
        assert!(dbg.contains("ProgressExchange"));
        assert!(dbg.contains("peer_count: 1"));
    }

    #[test]
    fn peer_count() {
        let (s1, _) = PeerProgressSender::new("node-1", 8);
        let (s2, _) = PeerProgressSender::new("node-2", 8);
        let exchange = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            vec![s1, s2],
            Arc::new(U64Codec),
        );
        assert_eq!(exchange.peer_count(), 2);
    }
}
