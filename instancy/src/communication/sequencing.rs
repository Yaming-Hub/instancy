//! Message sequencing for shared connection mode.
//!
//! When multiple dataflows share pooled connections, messages from a single
//! logical stream may travel over different TCP connections, breaking TCP's
//! FIFO guarantee. This module provides sequencing primitives that restore
//! ordering at the receiver.
//!
//! # Concepts
//!
//! - **Logical stream**: A unique `(DataflowId, channel_id)` pair representing
//!   one directional data flow. `channel_id` encodes (stage/edge, src_worker,
//!   dst_worker), so per-stage parallelism is correctly handled.
//!
//! - **Sequence ID**: A monotonically increasing `u64` per logical stream.
//!   Each frame sent is stamped with the next sequence number. Receivers
//!   deliver frames in sequence order, buffering out-of-order arrivals.
//!
//! - **ReorderBuffer**: Per logical stream at the receiver. Delivers in-order
//!   frames immediately, buffers gaps, times out if a gap persists too long.
//!
//! # Wire overhead
//!
//! The sequence_id adds 8 bytes per frame (u64 little-endian), bringing the
//! header from 28 bytes to 36 bytes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

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

    /// Reset the counter to 0 (for testing or reconnection scenarios).
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

// ─── ReorderBuffer ───────────────────────────────────────────────────────────

/// Error returned when the reorder buffer detects an unrecoverable gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReorderError {
    /// A gap in the sequence was not filled within the timeout.
    /// Contains the expected sequence ID that was never received.
    GapTimeout { expected_seq: u64 },
}

impl std::fmt::Display for ReorderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GapTimeout { expected_seq } => {
                write!(
                    f,
                    "reorder buffer timeout: missing sequence {}",
                    expected_seq
                )
            }
        }
    }
}

impl std::error::Error for ReorderError {}

/// Outcome of inserting a frame into the reorder buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum InsertResult {
    /// The frame (and possibly buffered successors) are ready to deliver.
    /// Contains the number of frames ready (call `drain_ready` to retrieve them).
    Ready(usize),
    /// The frame was buffered — a gap exists before it.
    Buffered,
    /// The frame was a duplicate (sequence_id < next_expected) and was discarded.
    Duplicate,
}

/// A reorder buffer that delivers frames in sequence order.
///
/// Frames arriving in order are delivered immediately. Out-of-order frames
/// are buffered. If a gap persists longer than `timeout`, the buffer reports
/// an error (data loss — unrecoverable).
///
/// # Usage
///
/// ```ignore
/// let mut buf = ReorderBuffer::new(Duration::from_millis(50));
///
/// match buf.insert(seq_id, payload) {
///     InsertResult::Ready(n) => {
///         for frame in buf.drain_ready() {
///             process(frame);
///         }
///     }
///     InsertResult::Buffered => { /* wait for missing frame */ }
///     InsertResult::Duplicate => { /* discard */ }
/// }
/// ```
#[derive(Debug)]
pub struct ReorderBuffer<T> {
    /// The next expected sequence ID.
    next_expected: u64,
    /// Buffered out-of-order frames, keyed by sequence_id.
    pending: BTreeMap<u64, T>,
    /// Timeout for gap detection.
    timeout: Duration,
    /// When the current gap started (None if no gap).
    /// Reset whenever `next_expected` advances to a new gap head.
    gap_start: Option<Instant>,
    /// Frames ready to deliver (in order, drained by caller).
    ready: Vec<T>,
    /// Maximum number of frames that can be buffered (prevents unbounded growth).
    max_buffered: usize,
}

/// Error returned when the reorder buffer exceeds its capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferOverflow {
    /// Too many out-of-order frames buffered.
    TooManyPending { count: usize, max: usize },
}

impl std::fmt::Display for BufferOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyPending { count, max } => {
                write!(f, "reorder buffer overflow: {} pending (max {})", count, max)
            }
        }
    }
}

impl std::error::Error for BufferOverflow {}

impl<T> ReorderBuffer<T> {
    /// Create a new reorder buffer with the given gap timeout.
    ///
    /// Uses a default max buffer size of 65536 frames.
    pub fn new(timeout: Duration) -> Self {
        Self::with_capacity(timeout, 65536)
    }

    /// Create a new reorder buffer with the given gap timeout and max buffer size.
    pub fn with_capacity(timeout: Duration, max_buffered: usize) -> Self {
        Self {
            next_expected: 0,
            pending: BTreeMap::new(),
            timeout,
            gap_start: None,
            ready: Vec::new(),
            max_buffered,
        }
    }

    /// Insert a frame with the given sequence ID.
    ///
    /// Returns the result indicating whether frames are ready, buffered, or duplicate.
    /// Returns `Err(BufferOverflow)` if the buffer would exceed `max_buffered`.
    pub fn insert(&mut self, seq_id: u64, item: T) -> Result<InsertResult, BufferOverflow> {
        if seq_id < self.next_expected {
            // Duplicate — already delivered
            return Ok(InsertResult::Duplicate);
        }

        if seq_id == self.next_expected {
            // In order — deliver immediately
            self.ready.push(item);
            self.next_expected += 1;

            // Check if buffered frames are now contiguous
            while let Some(next) = self.pending.remove(&self.next_expected) {
                self.ready.push(next);
                self.next_expected += 1;
            }

            // If gap resolved, clear timer. If still pending, reset timer
            // for the new gap head (the new next_expected is what we're waiting for).
            if self.pending.is_empty() {
                self.gap_start = None;
            } else {
                // Gap head advanced — restart timeout for new missing sequence
                self.gap_start = Some(Instant::now());
            }

            Ok(InsertResult::Ready(self.ready.len()))
        } else {
            // Out of order — check if already buffered (duplicate of pending frame)
            if self.pending.contains_key(&seq_id) {
                return Ok(InsertResult::Duplicate);
            }

            // Check capacity
            if self.pending.len() >= self.max_buffered {
                return Err(BufferOverflow::TooManyPending {
                    count: self.pending.len(),
                    max: self.max_buffered,
                });
            }

            self.pending.insert(seq_id, item);

            // Start gap timer if not already running
            if self.gap_start.is_none() {
                self.gap_start = Some(Instant::now());
            }

            Ok(InsertResult::Buffered)
        }
    }

    /// Drain all frames that are ready to deliver (in sequence order).
    ///
    /// Returns an iterator over the ready frames. After draining, the
    /// internal ready buffer is empty.
    pub fn drain_ready(&mut self) -> std::vec::Drain<'_, T> {
        self.ready.drain(..)
    }

    /// Check whether the gap timeout has been exceeded.
    ///
    /// Returns `Ok(())` if no gap or timeout hasn't elapsed, or
    /// `Err(ReorderError::GapTimeout)` if a gap has persisted too long.
    pub fn check_timeout(&self) -> Result<(), ReorderError> {
        if let Some(start) = self.gap_start {
            if start.elapsed() >= self.timeout {
                return Err(ReorderError::GapTimeout {
                    expected_seq: self.next_expected,
                });
            }
        }
        Ok(())
    }

    /// Returns the next expected sequence ID.
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    /// Returns the number of buffered (out-of-order) frames.
    pub fn buffered_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns whether there is an active gap (missing frames).
    pub fn has_gap(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Returns the duration since the current gap started, or None if no gap.
    pub fn gap_duration(&self) -> Option<Duration> {
        self.gap_start.map(|s| s.elapsed())
    }

    /// Returns the number of frames ready to be drained.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Reset the buffer to initial state (for reconnection scenarios).
    pub fn reset(&mut self) {
        self.next_expected = 0;
        self.pending.clear();
        self.gap_start = None;
        self.ready.clear();
    }
}

// ─── SequencedFrame ──────────────────────────────────────────────────────────

use crate::communication::transport::Frame;
use crate::dataflow::id::DataflowId;

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
pub fn decode_sequenced_header(header: &[u8; SEQUENCED_HEADER_SIZE]) -> (DataflowId, u64, u64, u32) {
    let dataflow_id = DataflowId::from_bytes(header[..16].try_into().unwrap());
    let channel_id = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let sequence_id = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let length = u32::from_le_bytes(header[32..36].try_into().unwrap());
    (dataflow_id, channel_id, sequence_id, length)
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

    // --- ReorderBuffer tests ---

    #[test]
    fn in_order_delivery() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));

        assert_eq!(buf.insert(0, "a").unwrap(), InsertResult::Ready(1));
        let ready: Vec<_> = buf.drain_ready().collect();
        assert_eq!(ready, vec!["a"]);

        assert_eq!(buf.insert(1, "b").unwrap(), InsertResult::Ready(1));
        let ready: Vec<_> = buf.drain_ready().collect();
        assert_eq!(ready, vec!["b"]);

        assert_eq!(buf.next_expected(), 2);
        assert!(!buf.has_gap());
    }

    #[test]
    fn out_of_order_buffering() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));

        // Receive seq 1 before seq 0
        assert_eq!(buf.insert(1, "b").unwrap(), InsertResult::Buffered);
        assert_eq!(buf.buffered_count(), 1);
        assert!(buf.has_gap());

        // Now receive seq 0 — both should be ready
        assert_eq!(buf.insert(0, "a").unwrap(), InsertResult::Ready(2));
        let ready: Vec<_> = buf.drain_ready().collect();
        assert_eq!(ready, vec!["a", "b"]);
        assert_eq!(buf.next_expected(), 2);
        assert!(!buf.has_gap());
    }

    #[test]
    fn large_gap_fills_in_order() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));

        // Receive 4, 2, 3, 1, 0
        assert_eq!(buf.insert(4, "e").unwrap(), InsertResult::Buffered);
        assert_eq!(buf.insert(2, "c").unwrap(), InsertResult::Buffered);
        assert_eq!(buf.insert(3, "d").unwrap(), InsertResult::Buffered);
        assert_eq!(buf.insert(1, "b").unwrap(), InsertResult::Buffered);
        assert_eq!(buf.buffered_count(), 4);

        // seq 0 fills the gap — all 5 delivered in order
        assert_eq!(buf.insert(0, "a").unwrap(), InsertResult::Ready(5));
        let ready: Vec<_> = buf.drain_ready().collect();
        assert_eq!(ready, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn duplicate_detection() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));

        buf.insert(0, "a").unwrap();
        buf.drain_ready().for_each(drop);

        // Duplicate of seq 0 (already delivered)
        assert_eq!(buf.insert(0, "a-dup").unwrap(), InsertResult::Duplicate);
        assert_eq!(buf.ready_count(), 0);
    }

    #[test]
    fn duplicate_of_buffered_is_ignored() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));

        // Buffer seq 2
        assert_eq!(buf.insert(2, "first").unwrap(), InsertResult::Buffered);
        // Insert seq 2 again — should be detected as duplicate, not overwrite
        assert_eq!(buf.insert(2, "second").unwrap(), InsertResult::Duplicate);
        assert_eq!(buf.buffered_count(), 1);

        // Fill gap
        buf.insert(0, "a").unwrap();
        buf.drain_ready().for_each(drop);
        assert_eq!(buf.insert(1, "b").unwrap(), InsertResult::Ready(2));
        let ready: Vec<_> = buf.drain_ready().collect();
        // Gets the first insertion (not overwritten)
        assert_eq!(ready, vec!["b", "first"]);
    }

    #[tokio::test]
    async fn gap_timeout_detection() {
        let mut buf = ReorderBuffer::new(Duration::from_millis(10));

        // Create a gap: receive seq 1 but not seq 0
        buf.insert(1, "b").unwrap();
        assert!(buf.check_timeout().is_ok());

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(15)).await;

        let err = buf.check_timeout().unwrap_err();
        assert_eq!(err, ReorderError::GapTimeout { expected_seq: 0 });
    }

    #[tokio::test]
    async fn gap_timeout_resets_when_head_advances() {
        let mut buf = ReorderBuffer::new(Duration::from_millis(20));

        // Create gap: have seq 1 and 3, missing 0 and 2
        buf.insert(1, "b").unwrap();
        buf.insert(3, "d").unwrap();

        // Wait 10ms (within timeout)
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(buf.check_timeout().is_ok());

        // Now deliver seq 0 — head advances to 2, timer should reset
        buf.insert(0, "a").unwrap();
        buf.drain_ready().for_each(drop);
        assert_eq!(buf.next_expected(), 2); // still missing seq 2

        // Timer just reset, so even after 15ms total elapsed, timeout not hit
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(buf.check_timeout().is_ok());

        // Wait full timeout from reset point
        tokio::time::sleep(Duration::from_millis(15)).await;
        let err = buf.check_timeout().unwrap_err();
        assert_eq!(err, ReorderError::GapTimeout { expected_seq: 2 });
    }

    #[test]
    fn no_timeout_without_gap() {
        let mut buf: ReorderBuffer<&str> = ReorderBuffer::new(Duration::from_millis(1));
        assert!(buf.check_timeout().is_ok());
        // In-order delivery — no gap
        buf.insert(0, "a").unwrap();
        buf.drain_ready().for_each(drop);
        assert!(buf.check_timeout().is_ok());
    }

    #[test]
    fn buffer_overflow_protection() {
        let mut buf = ReorderBuffer::with_capacity(Duration::from_secs(1), 3);

        // Buffer 3 frames (at capacity)
        buf.insert(1, "a").unwrap();
        buf.insert(2, "b").unwrap();
        buf.insert(3, "c").unwrap();

        // 4th should fail
        let err = buf.insert(4, "d").unwrap_err();
        assert_eq!(
            err,
            BufferOverflow::TooManyPending { count: 3, max: 3 }
        );
    }

    #[test]
    fn reset_clears_state() {
        let mut buf = ReorderBuffer::new(Duration::from_secs(1));
        buf.insert(2, "c").unwrap();
        buf.insert(0, "a").unwrap();
        buf.drain_ready().for_each(drop);

        buf.reset();
        assert_eq!(buf.next_expected(), 0);
        assert_eq!(buf.buffered_count(), 0);
        assert!(!buf.has_gap());
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
        let (df_id, ch_id, seq_id, len) = decode_sequenced_header(&header);

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
