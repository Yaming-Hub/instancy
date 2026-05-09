//! RTT probing and adaptive scaling for shared connection pools.
//!
//! This module provides the runtime machinery for:
//! - Sending periodic probe messages on each connection
//! - Measuring round-trip time from probe responses
//! - Driving scaling decisions based on measured RTT
//!
//! # Probe Protocol
//!
//! Probe messages travel at the **same priority as data** to measure the
//! latency that data actually experiences. A probe bypassing the data queue
//! would underestimate congestion.
//!
//! ```text
//! ┌──────────────────┬────────────────┬───────────────┐
//! │ PROBE_REQUEST(2) │ probe_seq: u64 │ send_ts: u64  │
//! └──────────────────┴────────────────┴───────────────┘
//!          ↓ peer echoes back:
//! ┌──────────────────┬────────────────┬───────────────┐
//! │ PROBE_REPLY(3)   │ probe_seq: u64 │ send_ts: u64  │
//! └──────────────────┴────────────────┴───────────────┘
//! ```
//!
//! - `probe_seq` (u64): monotonically increasing per connection, for matching
//!   replies to requests (handles out-of-order or lost probes).
//! - `send_ts` (u64): nanosecond timestamp from sender's monotonic clock.
//!   The receiver echoes it back unchanged; RTT = now - send_ts.
//!
//! # Scaling Driver
//!
//! The [`ScalingDriver`] runs as a background task per peer, periodically:
//! 1. Sends probes on each connection
//! 2. Processes probe replies and updates RTT metrics
//! 3. Evaluates scaling decisions
//! 4. Notifies the caller of scale-up/scale-down actions via a channel

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use super::shared_pool::{ConnectionMetrics, PeerPool, ScalingDecision, SharedConnectionConfig};

// ─── Probe Wire Format ───────────────────────────────────────────────────────

/// Message type byte for probe request.
pub const PROBE_REQUEST_TYPE: u8 = 2;
/// Message type byte for probe reply.
pub const PROBE_REPLY_TYPE: u8 = 3;

/// Size of a probe message: 1 (type) + 8 (probe_seq) + 8 (send_ts) = 17 bytes.
pub const PROBE_MESSAGE_SIZE: usize = 17;

/// A probe request or reply message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeMessage {
    /// Whether this is a request or reply.
    pub kind: ProbeKind,
    /// Monotonically increasing sequence number per connection.
    pub probe_seq: u64,
    /// Nanosecond timestamp from sender's monotonic clock.
    pub send_ts: u64,
}

/// Probe message direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeKind {
    Request,
    Reply,
}

impl ProbeMessage {
    /// Create a new probe request with the given sequence and timestamp.
    ///
    /// Use a [`ProbeTimestamp`] source to get the `send_ts`:
    /// ```ignore
    /// let ts = ProbeTimestamp::new();
    /// let msg = ProbeMessage::new_request(counter.next_seq(), ts.now_nanos());
    /// ```
    pub fn new_request(probe_seq: u64, send_ts: u64) -> Self {
        Self {
            kind: ProbeKind::Request,
            probe_seq,
            send_ts,
        }
    }

    /// Create a reply from a received request (echoes probe_seq and send_ts).
    pub fn reply_to(request: &ProbeMessage) -> Self {
        Self {
            kind: ProbeKind::Reply,
            probe_seq: request.probe_seq,
            send_ts: request.send_ts,
        }
    }

    /// Encode the probe message to bytes.
    pub fn encode(&self) -> [u8; PROBE_MESSAGE_SIZE] {
        let mut buf = [0u8; PROBE_MESSAGE_SIZE];
        buf[0] = match self.kind {
            ProbeKind::Request => PROBE_REQUEST_TYPE,
            ProbeKind::Reply => PROBE_REPLY_TYPE,
        };
        buf[1..9].copy_from_slice(&self.probe_seq.to_le_bytes());
        buf[9..17].copy_from_slice(&self.send_ts.to_le_bytes());
        buf
    }

    /// Decode a probe message from bytes.
    ///
    /// Returns `None` if the buffer is too short or the type byte is invalid.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < PROBE_MESSAGE_SIZE {
            return None;
        }
        let kind = match data[0] {
            PROBE_REQUEST_TYPE => ProbeKind::Request,
            PROBE_REPLY_TYPE => ProbeKind::Reply,
            _ => return None,
        };
        let probe_seq =
            u64::from_le_bytes(data[1..9].try_into().expect("probe sequence is 8 bytes"));
        let send_ts =
            u64::from_le_bytes(data[9..17].try_into().expect("probe timestamp is 8 bytes"));
        Some(Self {
            kind,
            probe_seq,
            send_ts,
        })
    }

    /// Check if this is a probe message by looking at the type byte.
    pub fn is_probe(data: &[u8]) -> bool {
        !data.is_empty() && (data[0] == PROBE_REQUEST_TYPE || data[0] == PROBE_REPLY_TYPE)
    }
}

// ─── Probe Sequence Counter ──────────────────────────────────────────────────

/// Per-connection probe sequence counter.
#[derive(Debug)]
pub struct ProbeCounter {
    next: AtomicU64,
}

impl ProbeCounter {
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for ProbeCounter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Scaling Events ──────────────────────────────────────────────────────────

/// Event emitted by the scaling driver to notify the pool manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingEvent {
    /// A new connection should be established to this peer.
    ScaleUp,
    /// The specified connection should be drained and closed.
    ScaleDown { connection_id: usize },
    /// A connection has failed (writer or reader error).
    ///
    /// The connection has already been marked dead in the pool.
    /// The handler should remove it from writer_channels and attempt
    /// to establish a replacement via ConnectionFactory.
    ConnectionFailed { connection_id: usize },
}

// ─── Scaling Driver ──────────────────────────────────────────────────────────

/// Drives RTT-based adaptive scaling for a single peer pool.
///
/// The driver is a background task that:
/// 1. Periodically sends probe requests on each connection
/// 2. Processes probe replies to update RTT metrics
/// 3. Evaluates scaling decisions based on configured thresholds
/// 4. Emits scaling events via a channel
///
/// # Usage
///
/// ```ignore
/// let (driver, event_rx) = ScalingDriver::new(pool, config);
/// tokio::spawn(driver.run(probe_sender, reply_receiver));
///
/// while let Some(event) = event_rx.recv().await {
///     match event {
///         ScalingEvent::ScaleUp => { /* establish new connection */ }
///         ScalingEvent::ScaleDown { id } => { /* drain and close */ }
///     }
/// }
/// ```
pub struct ScalingDriver {
    config: SharedConnectionConfig,
    /// Reference to the probe send timestamp for RTT calculation.
    /// Indexed by probe_seq % PROBE_WINDOW.
    probe_timestamps: Vec<AtomicU64>,
    /// Monotonic timestamp source for consistent RTT measurement.
    timestamp: ProbeTimestamp,
    /// Event sender for scaling notifications.
    event_tx: mpsc::Sender<ScalingEvent>,
}

/// Size of the circular buffer tracking outstanding probe timestamps.
const PROBE_WINDOW: usize = 256;

impl ScalingDriver {
    /// Create a new scaling driver.
    ///
    /// Returns the driver and a receiver for scaling events.
    pub fn new(config: SharedConnectionConfig) -> (Self, mpsc::Receiver<ScalingEvent>) {
        let (event_tx, event_rx) = mpsc::channel(16);
        let probe_timestamps = (0..PROBE_WINDOW).map(|_| AtomicU64::new(0)).collect();
        (
            Self {
                config,
                probe_timestamps,
                timestamp: ProbeTimestamp::new(),
                event_tx,
            },
            event_rx,
        )
    }

    /// Get the timestamp source for creating probe requests.
    pub fn timestamp(&self) -> &ProbeTimestamp {
        &self.timestamp
    }

    /// Record the send timestamp for a probe.
    ///
    /// Stores `send_ts + 1` internally so that `0` remains a valid sentinel for "empty slot".
    pub fn record_probe_sent(&self, probe_seq: u64, send_ts: u64) {
        let idx = (probe_seq as usize) % PROBE_WINDOW;
        // Store send_ts + 1 so that 0 means "empty" (send_ts=0 is valid at epoch start)
        self.probe_timestamps[idx].store(send_ts.wrapping_add(1), Ordering::Release);
    }

    /// Process a probe reply and update the connection's RTT metrics.
    ///
    /// Returns the measured RTT duration, or None if the probe is stale/unknown.
    /// Uses the locally stored timestamp (not the reply payload) to prevent spoofing.
    pub fn process_probe_reply(
        &self,
        reply: &ProbeMessage,
        connection: &Arc<ConnectionMetrics>,
    ) -> Option<Duration> {
        // Only process Reply messages
        if reply.kind != ProbeKind::Reply {
            return None;
        }

        let idx = (reply.probe_seq as usize) % PROBE_WINDOW;

        // Load stored value without clearing (we'll CAS it away only if valid)
        let stored_val = self.probe_timestamps[idx].load(Ordering::Acquire);
        if stored_val == 0 {
            // Empty slot — stale or already consumed
            return None;
        }

        // Recover original send_ts (we stored send_ts + 1)
        let stored_ts = stored_val.wrapping_sub(1);

        // Verify the reply matches our stored timestamp to detect slot collision
        if stored_ts != reply.send_ts {
            return None;
        }

        // CAS to claim this slot — prevents double-processing
        if self.probe_timestamps[idx]
            .compare_exchange(stored_val, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // Another thread consumed it concurrently
            return None;
        }

        // RTT = current time - locally stored send timestamp (same epoch)
        let rtt = self.timestamp.rtt_since(stored_ts);
        connection.record_rtt(rtt);
        Some(rtt)
    }

    /// Evaluate the pool and emit a scaling event if needed.
    pub async fn evaluate_and_emit(&self, pool: &PeerPool) {
        let decision = pool.evaluate_scaling().await;
        let event = match decision {
            ScalingDecision::None => return,
            ScalingDecision::ScaleUp => ScalingEvent::ScaleUp,
            ScalingDecision::ScaleDown { connection_id } => {
                ScalingEvent::ScaleDown { connection_id }
            }
        };
        // Best-effort send — if the receiver is full, skip this cycle
        let _ = self.event_tx.try_send(event);
    }

    /// Emit a scaling event directly (e.g., connection failure notification).
    pub async fn emit_event(&self, event: ScalingEvent) {
        let _ = self.event_tx.try_send(event);
    }

    /// Get the configured probe interval.
    pub fn probe_interval(&self) -> Duration {
        self.config.probe_interval
    }

    /// Get the configured reorder timeout.
    pub fn reorder_timeout(&self) -> Duration {
        self.config.reorder_timeout
    }
}

// ─── Probe Timestamp Source ──────────────────────────────────────────────────

/// A monotonic timestamp source for probe messages.
///
/// Uses `tokio::time::Instant` anchored at creation time to provide
/// nanosecond timestamps that can be echoed in probe replies for RTT
/// calculation.
#[derive(Debug, Clone)]
pub struct ProbeTimestamp {
    epoch: Instant,
}

impl ProbeTimestamp {
    /// Create a new timestamp source anchored at the current instant.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// Get the current timestamp in nanoseconds since epoch.
    pub fn now_nanos(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    /// Calculate RTT from an echoed send_ts.
    pub fn rtt_since(&self, send_ts: u64) -> Duration {
        let now = self.now_nanos();
        Duration::from_nanos(now.saturating_sub(send_ts))
    }
}

impl Default for ProbeTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_message_encode_decode_request() {
        let msg = ProbeMessage {
            kind: ProbeKind::Request,
            probe_seq: 42,
            send_ts: 1_000_000_000,
        };
        let bytes = msg.encode();
        let decoded = ProbeMessage::decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn probe_message_encode_decode_reply() {
        let msg = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 99,
            send_ts: 2_000_000_000,
        };
        let bytes = msg.encode();
        let decoded = ProbeMessage::decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn probe_message_reply_to() {
        let req = ProbeMessage {
            kind: ProbeKind::Request,
            probe_seq: 7,
            send_ts: 12345,
        };
        let reply = ProbeMessage::reply_to(&req);
        assert_eq!(reply.kind, ProbeKind::Reply);
        assert_eq!(reply.probe_seq, 7);
        assert_eq!(reply.send_ts, 12345);
    }

    #[test]
    fn probe_message_is_probe() {
        let req = ProbeMessage {
            kind: ProbeKind::Request,
            probe_seq: 0,
            send_ts: 0,
        };
        let bytes = req.encode();
        assert!(ProbeMessage::is_probe(&bytes));

        // Non-probe data
        assert!(!ProbeMessage::is_probe(&[0, 1, 2, 3]));
        assert!(!ProbeMessage::is_probe(&[1, 0, 0, 0])); // type 1 = Ready, not probe
        assert!(!ProbeMessage::is_probe(&[]));
    }

    #[test]
    fn probe_message_decode_too_short() {
        assert!(ProbeMessage::decode(&[2, 0, 0]).is_none());
    }

    #[test]
    fn probe_message_decode_invalid_type() {
        let mut bytes = [0u8; PROBE_MESSAGE_SIZE];
        bytes[0] = 99; // invalid type
        assert!(ProbeMessage::decode(&bytes).is_none());
    }

    #[test]
    fn probe_counter_increments() {
        let counter = ProbeCounter::new();
        assert_eq!(counter.next_seq(), 0);
        assert_eq!(counter.next_seq(), 1);
        assert_eq!(counter.next_seq(), 2);
    }

    #[test]
    fn probe_timestamp_rtt() {
        let ts = ProbeTimestamp::new();
        let send = ts.now_nanos();
        // Simulate some work
        std::thread::sleep(Duration::from_millis(1));
        let rtt = ts.rtt_since(send);
        assert!(rtt >= Duration::from_millis(1));
        assert!(rtt < Duration::from_millis(50)); // sanity bound
    }

    #[tokio::test]
    async fn scaling_driver_records_and_processes() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_up_threshold: Duration::from_millis(10),
            probe_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let (driver, _event_rx) = ScalingDriver::new(config.clone());

        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        // Record probe sent using driver's timestamp source
        let send_ts = driver.timestamp().now_nanos();
        driver.record_probe_sent(0, send_ts);

        // Simulate some delay
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Process reply (echoes the original send_ts)
        let reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 0,
            send_ts,
        };
        let rtt = driver.process_probe_reply(&reply, &conn);
        assert!(rtt.is_some());
        let rtt = rtt.unwrap();
        assert!(rtt >= Duration::from_millis(4), "RTT was {:?}", rtt);

        // Connection now has RTT
        assert!(conn.average_rtt().is_some());
    }

    #[tokio::test]
    async fn scaling_driver_stale_probe_ignored() {
        let config = SharedConnectionConfig::default();
        let (driver, _event_rx) = ScalingDriver::new(config.clone());

        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        // Process reply without having recorded a send — stale
        let reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 42,
            send_ts: 0,
        };
        let rtt = driver.process_probe_reply(&reply, &conn);
        assert!(rtt.is_none());
        assert!(conn.average_rtt().is_none());
    }

    #[tokio::test]
    async fn scaling_driver_emits_scale_up() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_up_threshold: Duration::from_millis(5),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config.clone());

        let (driver, mut event_rx) = ScalingDriver::new(config);

        // Inject high RTT directly
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(10));

        // Evaluate
        driver.evaluate_and_emit(&pool).await;

        // Should receive ScaleUp event
        let event = event_rx.try_recv().unwrap();
        assert_eq!(event, ScalingEvent::ScaleUp);
    }

    #[tokio::test]
    async fn scaling_driver_no_event_when_healthy() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_up_threshold: Duration::from_millis(10),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config.clone());

        let (driver, mut event_rx) = ScalingDriver::new(config);

        // Healthy RTT
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(2));

        driver.evaluate_and_emit(&pool).await;

        // No event
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn scaling_driver_rejects_request_kind() {
        let config = SharedConnectionConfig::default();
        let (driver, _event_rx) = ScalingDriver::new(config.clone());
        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        let send_ts = driver.timestamp().now_nanos();
        driver.record_probe_sent(0, send_ts);

        // Send a Request (not Reply) — should be rejected
        let request = ProbeMessage {
            kind: ProbeKind::Request,
            probe_seq: 0,
            send_ts,
        };
        assert!(driver.process_probe_reply(&request, &conn).is_none());
        // Slot should still be occupied (not consumed)
        // Sending the correct reply should still work
        let reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 0,
            send_ts,
        };
        assert!(driver.process_probe_reply(&reply, &conn).is_some());
    }

    #[tokio::test]
    async fn scaling_driver_send_ts_zero_works() {
        // Regression: send_ts=0 is valid (happens at epoch start)
        let config = SharedConnectionConfig::default();
        let (driver, _event_rx) = ScalingDriver::new(config.clone());
        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        // Use send_ts = 0 explicitly
        driver.record_probe_sent(0, 0);
        std::thread::sleep(Duration::from_millis(1));

        let reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 0,
            send_ts: 0,
        };
        let rtt = driver.process_probe_reply(&reply, &conn);
        assert!(rtt.is_some(), "send_ts=0 should be valid");
        assert!(rtt.unwrap() >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn scaling_driver_slot_wrap_collision_detected() {
        // If probe_seq wraps around (0 and 256 map to same slot), the newer
        // probe overwrites the old. The old reply should be rejected because
        // its send_ts won't match the stored (newer) timestamp.
        let config = SharedConnectionConfig::default();
        let (driver, _event_rx) = ScalingDriver::new(config.clone());
        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        let old_ts = driver.timestamp().now_nanos();
        driver.record_probe_sent(0, old_ts); // slot 0

        std::thread::sleep(Duration::from_millis(1));

        let new_ts = driver.timestamp().now_nanos();
        driver.record_probe_sent(256, new_ts); // also slot 0, overwrites

        // Reply for old probe (seq=0) arrives — send_ts doesn't match stored (new_ts)
        let stale_reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 0,
            send_ts: old_ts,
        };
        assert!(
            driver.process_probe_reply(&stale_reply, &conn).is_none(),
            "stale reply after slot overwrite should be rejected"
        );

        // Reply for new probe (seq=256) still works
        let valid_reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 256,
            send_ts: new_ts,
        };
        assert!(driver.process_probe_reply(&valid_reply, &conn).is_some());
    }

    #[tokio::test]
    async fn scaling_driver_mismatched_send_ts_rejected() {
        // A reply with correct probe_seq but wrong send_ts (spoofed) is rejected
        let config = SharedConnectionConfig::default();
        let (driver, _event_rx) = ScalingDriver::new(config.clone());
        let conn = Arc::new(ConnectionMetrics::new(0, config.rtt_ema_alpha));

        let send_ts = driver.timestamp().now_nanos();
        driver.record_probe_sent(5, send_ts);

        let spoofed_reply = ProbeMessage {
            kind: ProbeKind::Reply,
            probe_seq: 5,
            send_ts: send_ts + 999, // wrong timestamp
        };
        assert!(
            driver.process_probe_reply(&spoofed_reply, &conn).is_none(),
            "mismatched send_ts should be rejected"
        );
    }
}
