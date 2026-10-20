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

use std::sync::{Arc, Mutex};

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
    /// Delivery state shared by every caller of `broadcast_local_changes`.
    state: Mutex<BroadcastState>,
    /// Type witness for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

#[derive(Default)]
struct BroadcastState {
    /// An update that some peers accepted while others pushed back. Its retry
    /// goes only to the peers still owed it.
    pending: Option<PendingBroadcast>,
    /// A peer's channel closed: no later update can reach every peer.
    closed: bool,
}

/// An encoded progress update and the peers that have not accepted it yet.
struct PendingBroadcast {
    payload: Vec<u8>,
    owed: Vec<bool>,
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
            state: Mutex::new(BroadcastState::default()),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Broadcast local progress changes to all remote peers.
    ///
    /// Takes a set of frontier changes from the local progress tracker
    /// and sends them to every peer with `try_send`. If a peer's progress
    /// buffer is full, returns `Error::Backpressure`; the caller must then
    /// retry with exactly the same changes before broadcasting anything else
    /// (progress deltas are non-idempotent and must not be dropped, merged
    /// into later changes, or reordered).
    ///
    /// Peers that accepted the update before the failure are remembered: the
    /// retry only goes to the peers still owed it, so every peer applies the
    /// delta exactly once. Until that retry succeeds, other changes (for
    /// example from another caller sharing this exchange) are refused with
    /// `Error::Backpressure` and nothing is sent, so no peer sees them ahead
    /// of the pending update.
    ///
    /// If a peer's channel is closed, returns `Error::ChannelClosed`, now and
    /// for every later call: that peer can no longer receive updates in order.
    ///
    /// Returns the number of peers that have accepted the update, or an error
    /// if delivery is incomplete (backpressure or transport error).
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

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(Error::ChannelClosed);
        }
        let mut owed = match state.pending.take() {
            None => vec![true; self.peer_senders.len()],
            Some(p) if p.payload == buf => p.owed,
            Some(p) => {
                // Peers must see the pending update before anything newer.
                state.pending = Some(p);
                return Err(Error::Backpressure);
            }
        };

        let mut backpressure = false;
        for (peer, owed) in self.peer_senders.iter().zip(owed.iter_mut()) {
            if !*owed {
                continue;
            }
            let frame = OutboundFrame {
                dataflow_id: self.dataflow_id,
                channel_id: PROGRESS_CHANNEL_ID,
                payload: buf.clone(),
            };
            match peer.sender.try_send(frame) {
                Ok(()) => *owed = false,
                // Keep offering the update to the other peers.
                Err(Error::Backpressure) => backpressure = true,
                Err(err) => {
                    state.closed = true;
                    return Err(err);
                }
            }
        }

        if !backpressure {
            return Ok(self.peer_senders.len());
        }
        // Remember who already has the update. If nobody does, there is no
        // partial delivery to protect and other callers need not wait.
        if owed.iter().any(|owed| !owed) {
            state.pending = Some(PendingBroadcast { payload: buf, owed });
        }
        Err(Error::Backpressure)
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

    fn exchange_with_capacities(
        capacities: &[usize],
    ) -> (ProgressExchange<u64, U64Codec>, Vec<FrameReceiver>) {
        let (senders, receivers) = capacities
            .iter()
            .enumerate()
            .map(|(i, &cap)| PeerProgressSender::new(format!("node-{}", i + 1), cap))
            .unzip();
        let exchange = ProgressExchange::new(
            DataflowId::from_bytes([1u8; 16]),
            "node-0",
            senders,
            Arc::new(U64Codec),
        );
        (exchange, receivers)
    }

    fn received_changes(receiver: &FrameReceiver) -> Vec<Vec<(usize, u64, i64)>> {
        receiver
            .drain()
            .into_iter()
            .map(|f| {
                decode_progress::<u64, U64Codec>(&f.payload, &U64Codec)
                    .unwrap()
                    .changes
            })
            .collect()
    }

    #[test]
    fn retry_after_partial_broadcast_delivers_each_peer_once() {
        let (exchange, receivers) = exchange_with_capacities(&[16, 1, 16]);
        let fill = [(0usize, 0u64, 1i64)];
        exchange.broadcast_local_changes(&fill).unwrap();
        for r in [&receivers[0], &receivers[2]] {
            assert_eq!(received_changes(r), vec![fill.to_vec()]);
        }

        // node-2 is full: node-1 and node-3 accept, node-2 pushes back.
        let delta = [(1usize, 7u64, -1i64)];
        assert!(matches!(
            exchange.broadcast_local_changes(&delta),
            Err(Error::Backpressure)
        ));
        assert!(matches!(
            exchange.broadcast_local_changes(&delta),
            Err(Error::Backpressure)
        ));
        assert_eq!(received_changes(&receivers[1]), vec![fill.to_vec()]);

        // The documented retry with the same changes completes delivery.
        assert_eq!(exchange.broadcast_local_changes(&delta).unwrap(), 3);
        for r in &receivers {
            assert_eq!(
                received_changes(r),
                vec![delta.to_vec()],
                "each peer must apply the non-idempotent delta exactly once"
            );
        }

        // Later broadcasts go to every peer again.
        let next = [(2usize, 8u64, 1i64)];
        assert_eq!(exchange.broadcast_local_changes(&next).unwrap(), 3);
        for r in &receivers {
            assert_eq!(received_changes(r), vec![next.to_vec()]);
        }
    }

    #[test]
    fn retry_resumes_with_every_peer_still_owed() {
        let (exchange, receivers) = exchange_with_capacities(&[1, 1, 1]);
        exchange.broadcast_local_changes(&[(0, 0u64, 1)]).unwrap();
        let delta = [(3usize, 5u64, -1i64)];
        assert!(exchange.broadcast_local_changes(&delta).is_err());

        // Peers drain one at a time; each retry delivers to whoever has room.
        for (i, r) in receivers.iter().enumerate() {
            assert_eq!(received_changes(r).len(), 1);
            let result = exchange.broadcast_local_changes(&delta);
            if i + 1 < receivers.len() {
                assert!(matches!(result, Err(Error::Backpressure)));
            } else {
                assert_eq!(result.unwrap(), 3);
            }
        }
        for r in &receivers {
            assert_eq!(received_changes(r), vec![delta.to_vec()]);
        }
    }

    #[test]
    fn different_changes_wait_while_a_partial_broadcast_is_pending() {
        let (exchange, receivers) = exchange_with_capacities(&[16, 1]);
        exchange.broadcast_local_changes(&[(0, 0u64, 1)]).unwrap();
        let delta = [(1usize, 7u64, -1i64)];
        assert!(exchange.broadcast_local_changes(&delta).is_err());
        receivers[0].drain();

        // Another caller sharing the exchange is told to retry later, and
        // nothing overtakes the pending delta, even once node-2 has room.
        receivers[1].drain();
        let other = [(1usize, 9u64, 1i64)];
        assert!(matches!(
            exchange.broadcast_local_changes(&other),
            Err(Error::Backpressure)
        ));
        assert!(receivers[0].try_recv().is_none());
        assert!(receivers[1].try_recv().is_none());

        assert_eq!(exchange.broadcast_local_changes(&delta).unwrap(), 2);
        assert_eq!(received_changes(&receivers[0]), Vec::<Vec<_>>::new());
        assert_eq!(received_changes(&receivers[1]), vec![delta.to_vec()]);
        assert_eq!(exchange.broadcast_local_changes(&other).unwrap(), 2);
        for r in &receivers {
            assert_eq!(received_changes(r), vec![other.to_vec()]);
        }
    }

    #[test]
    fn an_update_no_peer_accepted_does_not_hold_back_other_callers() {
        // A shares the exchange with B. A's update reached nobody, so B's
        // changes cannot overtake it anywhere and need not wait.
        let (exchange, receivers) = exchange_with_capacities(&[1, 1]);
        exchange.broadcast_local_changes(&[(0, 0u64, 1)]).unwrap();
        let from_a = [(1usize, 1u64, 1i64)];
        assert!(exchange.broadcast_local_changes(&from_a).is_err());
        for r in &receivers {
            r.drain();
        }
        let from_b = [(2usize, 2u64, 1i64)];
        assert_eq!(exchange.broadcast_local_changes(&from_b).unwrap(), 2);
        for r in &receivers {
            assert_eq!(received_changes(r), vec![from_b.to_vec()]);
        }
        assert_eq!(exchange.broadcast_local_changes(&from_a).unwrap(), 2);
        for r in &receivers {
            assert_eq!(received_changes(r), vec![from_a.to_vec()]);
        }
    }

    #[test]
    fn a_closed_peer_closes_the_exchange_for_every_caller() {
        let (exchange, mut receivers) = exchange_with_capacities(&[16, 16, 16]);
        drop(receivers.remove(1));
        let delta = [(0usize, 3u64, 1i64)];
        assert!(matches!(
            exchange.broadcast_local_changes(&delta),
            Err(Error::ChannelClosed)
        ));
        // Neither the retry nor anyone else's changes hang on backpressure.
        assert!(matches!(
            exchange.broadcast_local_changes(&delta),
            Err(Error::ChannelClosed)
        ));
        assert!(matches!(
            exchange.broadcast_local_changes(&[(1, 4u64, 1)]),
            Err(Error::ChannelClosed)
        ));
        assert_eq!(received_changes(&receivers[0]), vec![delta.to_vec()]);
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
