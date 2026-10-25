//! Message sequencing primitives.
//!
//! In Shared connection mode every payload frame of a (dataflow, peer) lane
//! carries a sequence number. The lane is pinned to one pooled connection,
//! so frames arrive in order, and the receiver checks that each frame
//! carries exactly the next number: a gap (a lost frame) or a repeat (a
//! duplicated one) fails the dataflow instead of being delivered.
//!
//! # Concepts
//!
//! - **Logical stream**: A unique `(DataflowId, channel_id)` pair representing
//!   one directional data flow. `channel_id` encodes (stage/edge, src_worker,
//!   dst_worker), so per-stage parallelism is correctly handled.
//!
//! - **Sequence ID**: A monotonically increasing `u64` per stream. Each frame
//!   sent is stamped with the next sequence number.
//!
//! # Wire overhead
//!
//! The sequence_id adds 8 bytes per frame (u64 little-endian), bringing the
//! header from 28 bytes to 36 bytes.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::dataflow::id::DataflowId;
use crate::error::Error;
use crate::wire;

// ─── SequenceCounter ─────────────────────────────────────────────────────────

/// A thread-safe monotonically increasing sequence counter.
///
/// Each logical stream `(DataflowId, channel_id)` gets its own counter.
/// The counter starts at 0 and increments by 1 for each frame sent.
///
/// # Thread Safety
///
/// Uses `AtomicU64` with `Relaxed` ordering — correctness requires that only
/// one task sends on a given logical stream at a time (which is guaranteed by
/// the dataflow model: one source worker per edge endpoint). The atomic is
/// used for interior mutability, not cross-thread synchronization.
#[derive(Debug)]
pub struct SequenceCounter {
    next: AtomicU64,
}

impl SequenceCounter {
    /// Create a new counter starting at 0.
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }

    /// Get the next sequence ID and advance the counter.
    pub fn next_seq(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// Peek at the next sequence ID without advancing.
    pub fn peek(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// Reset the counter to 0 (for testing).
    ///
    /// # Safety
    ///
    /// Caller must ensure no concurrent `next_seq()` calls during reset.
    /// This takes `&mut self` to enforce exclusive access.
    pub fn reset(&mut self) {
        *self.next.get_mut() = 0;
    }
}

impl Default for SequenceCounter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── SequencedFrame ──────────────────────────────────────────────────────────

use crate::communication::transport::Frame;

/// A frame with an attached sequence ID for ordering in shared connection mode.
///
/// This extends the base [`Frame`] with a per-logical-stream sequence number.
/// The `sequence_id` is scoped to `(dataflow_id, channel_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedFrame {
    /// The underlying frame (dataflow_id, channel_id, payload).
    pub frame: Frame,
    /// Sequence number within the logical stream `(dataflow_id, channel_id)`.
    pub sequence_id: u64,
}

impl SequencedFrame {
    /// Create a new sequenced frame.
    pub fn new(frame: Frame, sequence_id: u64) -> Self {
        Self { frame, sequence_id }
    }

    /// Convenience: get the dataflow ID.
    pub fn dataflow_id(&self) -> DataflowId {
        self.frame.dataflow_id
    }

    /// Convenience: get the channel ID.
    pub fn channel_id(&self) -> u64 {
        self.frame.channel_id
    }

    /// Convenience: get the payload.
    pub fn payload(&self) -> &[u8] {
        &self.frame.payload
    }

    /// The logical stream key for this frame.
    pub fn stream_key(&self) -> (DataflowId, u64) {
        (self.frame.dataflow_id, self.frame.channel_id)
    }
}

/// Header size for sequenced frames: 28 (base) + 8 (sequence_id) = 36 bytes.
pub const SEQUENCED_HEADER_SIZE: usize = 36;

/// Write a sequenced frame to a byte buffer (for wire serialization).
///
/// Layout: `dataflow_id(16) | channel_id(8) | sequence_id(8) | length(4) | payload`
///
/// # Panics
///
/// Panics if `payload.len() > u32::MAX`. Callers should validate payload size
/// upstream (the transport layer already rejects payloads > MAX_MESSAGE_SIZE).
pub fn encode_sequenced_header(frame: &SequencedFrame) -> [u8; SEQUENCED_HEADER_SIZE] {
    // SAFETY: transport layer enforces the wire-format payload size bound before encoding
    assert!(
        frame.frame.payload.len() <= u32::MAX as usize,
        "payload too large for wire format: {} bytes",
        frame.frame.payload.len()
    );
    let mut header = [0u8; SEQUENCED_HEADER_SIZE];
    header[..16].copy_from_slice(frame.frame.dataflow_id.as_bytes());
    header[16..24].copy_from_slice(&frame.frame.channel_id.to_le_bytes());
    header[24..32].copy_from_slice(&frame.sequence_id.to_le_bytes());
    header[32..36].copy_from_slice(&(frame.frame.payload.len() as u32).to_le_bytes());
    header
}

/// Decode a sequenced frame header from bytes.
///
/// Returns `(dataflow_id, channel_id, sequence_id, payload_length)`.
pub fn decode_sequenced_header(
    header: &[u8; SEQUENCED_HEADER_SIZE],
) -> Result<(DataflowId, u64, u64, u32), Error> {
    let dataflow_id = DataflowId::from_bytes(wire::read_array::<16>(header, 0)?);
    let channel_id = wire::read_u64(header, 16)?;
    let sequence_id = wire::read_u64(header, 24)?;
    let length = wire::read_u32(header, 32)?;
    Ok((dataflow_id, channel_id, sequence_id, length))
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- SequenceCounter tests ---

    #[test]
    fn counter_starts_at_zero() {
        let counter = SequenceCounter::new();
        assert_eq!(counter.peek(), 0);
    }

    #[test]
    fn counter_increments_monotonically() {
        let counter = SequenceCounter::new();
        assert_eq!(counter.next_seq(), 0);
        assert_eq!(counter.next_seq(), 1);
        assert_eq!(counter.next_seq(), 2);
        assert_eq!(counter.peek(), 3);
    }

    #[test]
    fn counter_reset() {
        let mut counter = SequenceCounter::new();
        counter.next_seq();
        counter.next_seq();
        counter.reset();
        assert_eq!(counter.next_seq(), 0);
    }

    // --- SequencedFrame / wire format tests ---

    #[test]
    fn sequenced_header_roundtrip() {
        let frame = SequencedFrame {
            frame: Frame {
                dataflow_id: DataflowId::new(),
                channel_id: 42,
                payload: vec![1, 2, 3, 4],
            },
            sequence_id: 1234567890,
        };

        let header = encode_sequenced_header(&frame);
        let (df_id, ch_id, seq_id, len) = decode_sequenced_header(&header).unwrap();

        assert_eq!(df_id, frame.frame.dataflow_id);
        assert_eq!(ch_id, 42);
        assert_eq!(seq_id, 1234567890);
        assert_eq!(len, 4);
    }

    #[test]
    fn stream_key_encodes_dataflow_and_channel() {
        let df1 = DataflowId::new();
        let df2 = DataflowId::new();

        let f1 = SequencedFrame::new(
            Frame {
                dataflow_id: df1,
                channel_id: 10,
                payload: vec![],
            },
            0,
        );
        let f2 = SequencedFrame::new(
            Frame {
                dataflow_id: df1,
                channel_id: 20,
                payload: vec![],
            },
            0,
        );
        let f3 = SequencedFrame::new(
            Frame {
                dataflow_id: df2,
                channel_id: 10,
                payload: vec![],
            },
            0,
        );

        // Same dataflow, different channel → different stream
        assert_ne!(f1.stream_key(), f2.stream_key());
        // Different dataflow, same channel → different stream
        assert_ne!(f1.stream_key(), f3.stream_key());
        // Same dataflow and channel → same stream
        assert_eq!(f1.stream_key(), (df1, 10));
    }

    #[test]
    fn counter_default_trait() {
        let counter = SequenceCounter::default();
        assert_eq!(counter.next_seq(), 0);
    }
}
