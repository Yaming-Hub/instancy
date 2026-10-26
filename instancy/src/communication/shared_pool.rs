//! Configuration and per-connection metrics for Shared connection mode.
//!
//! In Shared mode a [`SharedPeerManager`](super::shared_transport::SharedPeerManager)
//! keeps a **fixed** pool of [`SharedConnectionConfig::connections`]
//! connections to one peer, shared by every dataflow that runs with that
//! peer. Each (dataflow, peer) lane is pinned to one pooled connection when
//! the dataflow registers, so the dataflow's frames to the peer stay in
//! order (TCP order) and a failed connection fails exactly the dataflows
//! pinned to it (see the [`shared_transport`](super::shared_transport)
//! module documentation).
//!
//! The pool is not resized automatically: each node decides alone, and a
//! connection that one side opens or closes on its own is a connection the
//! other side never reads or a link failure. Coordinated resizing, agreed by
//! both sides, may come later.
//!
//! RTT probes measure each connection's round-trip time for
//! [`ConnectionSnapshot`]s; they never drive any decision.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::RuntimeError;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Configuration of a Shared-mode connection pool to one peer.
#[derive(Debug, Clone)]
pub struct SharedConnectionConfig {
    /// Number of pooled connections to the peer (at least 1). Every dataflow
    /// with the peer uses one of them. Connections are established when the
    /// first dataflow registers, and a failed one is replaced when a later
    /// dataflow registers.
    ///
    /// Both peers of a pool must use the same value: the application's
    /// [`ConnectionFactory`](super::shared_transport::ConnectionFactory)
    /// pairs each connection one node establishes with one the peer
    /// establishes.
    pub connections: usize,
    /// How long establishing one connection through the application's
    /// factory may take before registration fails.
    pub connect_timeout: Duration,
    /// Interval between RTT probes on each connection (metrics only).
    pub probe_interval: Duration,
    /// EMA smoothing factor for RTT (0.0–1.0, higher = more weight to recent).
    pub rtt_ema_alpha: f64,
}

impl Default for SharedConnectionConfig {
    fn default() -> Self {
        Self {
            connections: 1,
            connect_timeout: Duration::from_secs(10),
            probe_interval: Duration::from_secs(1),
            rtt_ema_alpha: 0.2,
        }
    }
}

impl SharedConnectionConfig {
    /// Check the configuration.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidConfig`] if `connections` is 0, or
    /// `connect_timeout` or `probe_interval` is zero.
    pub fn validate(&self) -> crate::Result<()> {
        let invalid = |reason: &str| {
            Err(crate::Error::Runtime(RuntimeError::InvalidConfig(
                reason.to_string(),
            )))
        };
        if self.connections == 0 {
            return invalid("a shared connection pool needs at least 1 connection");
        }
        if self.connect_timeout.is_zero() {
            return invalid("connect_timeout must be positive");
        }
        if self.probe_interval.is_zero() {
            return invalid("probe_interval must be positive");
        }
        Ok(())
    }
}

/// Connection mode selection for the transport layer.
///
/// Determines whether each dataflow gets dedicated connections or shares
/// a pool with other dataflows.
#[derive(Debug, Clone, Default)]
pub enum ConnectionMode {
    /// Each dataflow gets its own connection(s) per peer. (Default)
    #[default]
    Dedicated,
    /// Dataflows share a fixed pool of connections per peer; each dataflow's
    /// lane to a peer is pinned to one pooled connection.
    Shared(SharedConnectionConfig),
}
// ─── RTT Measurement ─────────────────────────────────────────────────────────

/// Exponential moving average RTT tracker.
///
/// Thread-safe via compare-and-swap for concurrent RTT recordings.
/// Stores RTT in nanoseconds as an atomic u64.
#[derive(Debug)]
pub struct RttTracker {
    /// Current EMA RTT in nanoseconds (0 = no measurement yet).
    avg_nanos: AtomicU64,
    /// Smoothing factor (stored as fixed-point: alpha * 1000).
    alpha_fp: u64,
}

impl RttTracker {
    /// Create a new RTT tracker with the given EMA smoothing factor.
    ///
    /// `alpha` is clamped to (0.0, 1.0]. Higher values weight recent samples more.
    pub fn new(alpha: f64) -> Self {
        let clamped = alpha.clamp(0.001, 1.0);
        let alpha_fp = (clamped * 1000.0) as u64;
        Self {
            avg_nanos: AtomicU64::new(0),
            alpha_fp,
        }
    }

    /// Record a new RTT sample (thread-safe via CAS loop).
    pub fn record(&self, rtt: Duration) {
        let sample = rtt.as_nanos() as u64;
        loop {
            let current = self.avg_nanos.load(Ordering::Relaxed);
            let new_avg = if current == 0 {
                sample
            } else {
                let alpha = self.alpha_fp;
                (alpha * sample + (1000 - alpha) * current) / 1000
            };
            match self.avg_nanos.compare_exchange_weak(
                current,
                new_avg,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue, // Retry with updated current
            }
        }
    }

    /// Get the current average RTT.
    ///
    /// Returns `None` if no samples have been recorded.
    pub fn average(&self) -> Option<Duration> {
        let nanos = self.avg_nanos.load(Ordering::Relaxed);
        if nanos == 0 {
            None
        } else {
            Some(Duration::from_nanos(nanos))
        }
    }

    /// Get the current average RTT in nanoseconds (0 if no samples).
    pub fn average_nanos(&self) -> u64 {
        self.avg_nanos.load(Ordering::Relaxed)
    }

    /// Reset the tracker.
    pub fn reset(&self) {
        self.avg_nanos.store(0, Ordering::Relaxed);
    }
}

// ─── Connection metrics ──────────────────────────────────────────────────────

/// Metrics of one pooled connection.
#[derive(Debug)]
pub struct ConnectionMetrics {
    /// Unique ID within the peer's pool.
    pub id: usize,
    /// RTT tracker for this connection.
    rtt: RttTracker,
    /// Total payload bytes written through this connection.
    bytes_written: AtomicU64,
    /// Total frames written through this connection.
    frames_written: AtomicU64,
    /// Timestamp (nanos since UNIX epoch) of the last write of user traffic.
    last_activity_nanos: AtomicU64,
}

impl ConnectionMetrics {
    /// Create metrics for a new connection with the given ID and RTT alpha.
    pub fn new(id: usize, rtt_alpha: f64) -> Self {
        Self {
            id,
            rtt: RttTracker::new(rtt_alpha),
            bytes_written: AtomicU64::new(0),
            frames_written: AtomicU64::new(0),
            last_activity_nanos: AtomicU64::new(now_nanos()),
        }
    }

    /// Record a frame written to the connection. Probe frames pass
    /// `is_user_traffic = false` so they don't count as activity.
    pub fn record_write(&self, payload_size: usize, is_user_traffic: bool) {
        self.bytes_written
            .fetch_add(payload_size as u64, Ordering::Relaxed);
        self.frames_written.fetch_add(1, Ordering::Relaxed);
        if is_user_traffic {
            self.last_activity_nanos
                .store(now_nanos(), Ordering::Relaxed);
        }
    }

    /// Record an RTT probe result.
    pub fn record_rtt(&self, rtt: Duration) {
        self.rtt.record(rtt);
    }

    /// Current average RTT (None if no probe has returned yet).
    pub fn average_rtt(&self) -> Option<Duration> {
        self.rtt.average()
    }

    /// Total payload bytes written.
    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Total frames written.
    pub fn total_frames_written(&self) -> u64 {
        self.frames_written.load(Ordering::Relaxed)
    }

    /// Time since the last write of user traffic (or since the connection
    /// was established). `None` if the clock cannot be read.
    pub fn idle_duration(&self) -> Option<Duration> {
        let last = self.last_activity_nanos.load(Ordering::Relaxed);
        if last == 0 {
            return None;
        }
        Some(Duration::from_nanos(now_nanos().saturating_sub(last)))
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Diagnostic snapshot of one pooled connection.
#[derive(Debug, Clone)]
pub struct ConnectionSnapshot {
    /// Connection ID within the peer's pool.
    pub id: usize,
    /// Frames queued for the connection's writer, not yet written.
    pub queued_frames: usize,
    /// Dataflow lanes pinned to the connection.
    pub pinned_lanes: usize,
    /// Current average RTT (None if no probe has returned yet).
    pub avg_rtt: Option<Duration>,
    /// Total payload bytes written since the connection was established.
    pub total_bytes: u64,
    /// Total frames written since the connection was established.
    pub total_frames: u64,
    /// Time since the last write of user traffic.
    pub idle_duration: Option<Duration>,
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rtt_tracker_first_sample() {
        let tracker = RttTracker::new(0.2);
        assert_eq!(tracker.average(), None);

        tracker.record(Duration::from_millis(5));
        assert_eq!(tracker.average(), Some(Duration::from_millis(5)));
    }

    #[test]
    fn rtt_tracker_ema_smoothing() {
        let tracker = RttTracker::new(0.5); // alpha=0.5 for easy math

        tracker.record(Duration::from_millis(10)); // avg = 10
        tracker.record(Duration::from_millis(20)); // avg = 0.5*20 + 0.5*10 = 15

        let avg = tracker.average().unwrap();
        assert_eq!(avg.as_millis(), 15);
    }

    #[test]
    fn rtt_tracker_reset() {
        let tracker = RttTracker::new(0.2);
        tracker.record(Duration::from_millis(5));
        tracker.reset();
        assert_eq!(tracker.average(), None);
    }

    #[test]
    fn connection_metrics_record_writes() {
        let m = ConnectionMetrics::new(0, 0.2);
        m.record_write(100, true);
        m.record_write(17, false);
        assert_eq!(m.total_bytes_written(), 117);
        assert_eq!(m.total_frames_written(), 2);
        assert!(m.idle_duration().is_some());
    }

    #[test]
    fn default_config_is_valid_and_one_connection() {
        let config = SharedConnectionConfig::default();
        assert_eq!(config.connections, 1);
        config.validate().unwrap();
    }

    #[test]
    fn invalid_configs_are_rejected() {
        for config in [
            SharedConnectionConfig {
                connections: 0,
                ..Default::default()
            },
            SharedConnectionConfig {
                connect_timeout: Duration::ZERO,
                ..Default::default()
            },
            SharedConnectionConfig {
                probe_interval: Duration::ZERO,
                ..Default::default()
            },
        ] {
            assert!(config.validate().is_err(), "{config:?}");
        }
    }

    #[test]
    fn connection_mode_default_is_dedicated() {
        assert!(matches!(
            ConnectionMode::default(),
            ConnectionMode::Dedicated
        ));
    }
}
