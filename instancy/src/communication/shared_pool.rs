//! Adaptive connection pool for shared connection mode.
//!
//! This module provides an adaptive pool of TCP connections per peer that
//! scales based on measured load (RTT probes). Multiple dataflows share
//! the pool, with frames load-balanced across connections using a
//! least-loaded strategy.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                  SharedPool                          │
//! │                                                     │
//! │  peer "node-b" ──► PeerPool                         │
//! │                      ├─ ConnectionHandle #0         │
//! │                      │    write_queue: 3 pending    │
//! │                      │    avg_rtt: 1.2ms            │
//! │                      ├─ ConnectionHandle #1         │
//! │                      │    write_queue: 1 pending    │
//! │                      │    avg_rtt: 0.8ms            │
//! │                      └─ ConnectionHandle #2         │
//! │                           write_queue: 5 pending    │
//! │                           avg_rtt: 4.1ms (scaling?) │
//! │                                                     │
//! │  peer "node-c" ──► PeerPool                         │
//! │                      └─ ConnectionHandle #0         │
//! │                           write_queue: 0 pending    │
//! │                           avg_rtt: 0.3ms            │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Scaling
//!
//! - **Scale up**: When any connection's RTT exceeds `rtt_scale_up_threshold`,
//!   a new connection is established (up to `max_connections`).
//! - **Scale down**: When all connections' RTT stays below `rtt_scale_down_threshold`
//!   for longer than `cooldown_period`, one connection is drained and closed
//!   (down to `min_connections`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;

use crate::error::LockResultExt;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Configuration for the shared connection pool.
///
/// Controls connection scaling behavior, RTT measurement, and reorder buffering.
#[derive(Debug, Clone)]
pub struct SharedConnectionConfig {
    /// Minimum connections to maintain per peer (pre-warmed).
    pub min_connections: usize,
    /// Maximum connections allowed per peer.
    pub max_connections: usize,
    /// Scale up when probe RTT exceeds this threshold.
    pub rtt_scale_up_threshold: Duration,
    /// Scale down when probe RTT stays below this for `cooldown_period`.
    pub rtt_scale_down_threshold: Duration,
    /// How long RTT must stay below scale-down threshold before removing a connection.
    pub cooldown_period: Duration,
    /// Interval between probe messages per connection.
    pub probe_interval: Duration,
    /// Timeout for reorder buffer gap detection.
    pub reorder_timeout: Duration,
    /// EMA smoothing factor for RTT (0.0–1.0, higher = more weight to recent).
    pub rtt_ema_alpha: f64,
    /// Close idle connections after this duration of inactivity.
    ///
    /// A connection is considered idle when it has had no frames written
    /// for longer than this timeout. Idle connections are removed down to
    /// `min_connections`. Set to `None` to disable idle cleanup.
    pub idle_timeout: Option<Duration>,
}

impl Default for SharedConnectionConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 8,
            rtt_scale_up_threshold: Duration::from_millis(5),
            rtt_scale_down_threshold: Duration::from_millis(1),
            cooldown_period: Duration::from_secs(30),
            probe_interval: Duration::from_millis(100),
            reorder_timeout: Duration::from_millis(50),
            rtt_ema_alpha: 0.2,
            idle_timeout: Some(Duration::from_secs(60)),
        }
    }
}

/// Connection mode selection for the transport layer.
///
/// Determines whether each dataflow gets dedicated connections or shares
/// an adaptive pool with other dataflows.
#[derive(Debug, Clone, Default)]
pub enum ConnectionMode {
    /// Each dataflow gets its own connection(s) per peer. (Default, current behavior)
    ///
    /// Simple, correct, sufficient for moderate scale. No sequencing overhead.
    #[default]
    Dedicated,
    /// Dataflows share adaptive pooled connections with sequenced frames.
    ///
    /// Scales connections based on measured RTT. Provides resilience (retry on
    /// alternate connection) and higher throughput (parallel congestion windows).
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

    /// Reset the tracker (e.g., after a reconnection).
    pub fn reset(&self) {
        self.avg_nanos.store(0, Ordering::Relaxed);
    }
}

// ─── ConnectionHandle ────────────────────────────────────────────────────────

/// Metrics and state for a single connection in the pool.
///
/// The actual I/O is handled externally (the pool provides routing decisions);
/// this struct tracks the load metrics used for selection and scaling.
#[derive(Debug)]
pub struct ConnectionMetrics {
    /// Unique ID within the peer pool.
    pub id: usize,
    /// Number of frames currently pending in the write queue.
    pending_writes: AtomicUsize,
    /// RTT tracker for this connection.
    rtt: RttTracker,
    /// Total bytes written through this connection.
    bytes_written: AtomicU64,
    /// Total frames written through this connection.
    frames_written: AtomicU64,
    /// Timestamp (nanos since UNIX epoch) of the last write activity.
    /// Updated on each `dequeue()`. 0 means no activity yet.
    last_activity_nanos: AtomicU64,
    /// Whether this connection is alive (true) or has been marked dead (false).
    /// Dead connections are skipped by selection and excluded from scaling.
    alive: std::sync::atomic::AtomicBool,
}

impl ConnectionMetrics {
    /// Create metrics for a new connection with the given ID and RTT alpha.
    pub fn new(id: usize, rtt_alpha: f64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            id,
            pending_writes: AtomicUsize::new(0),
            rtt: RttTracker::new(rtt_alpha),
            bytes_written: AtomicU64::new(0),
            frames_written: AtomicU64::new(0),
            last_activity_nanos: AtomicU64::new(now),
            alive: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Increment pending write count (called when frame is enqueued).
    pub fn enqueue(&self) {
        self.pending_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Rollback a reservation that could not be fulfilled.
    ///
    /// Called when a frame could not be sent to the writer channel
    /// (e.g., channel closed, connection dead). Decrements the pending
    /// count that was incremented by `enqueue()`.
    pub fn rollback_reservation(&self) {
        // CAS loop to avoid underflow
        loop {
            let current = self.pending_writes.load(Ordering::Relaxed);
            if current == 0 {
                return;
            }
            if self
                .pending_writes
                .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Mark this connection as dead. Returns `true` if this call
    /// transitioned from alive to dead (first failure notification),
    /// `false` if already dead (duplicate notification).
    pub fn mark_dead(&self) -> bool {
        self.alive
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Whether this connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Decrement pending write count (called when frame is written to TCP).
    ///
    /// Set `is_user_traffic` to `true` for data/control/progress frames.
    /// Probe frames should pass `false` so they don't reset the idle timer.
    pub fn dequeue(&self, payload_size: usize, is_user_traffic: bool) {
        self.pending_writes.fetch_sub(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(payload_size as u64, Ordering::Relaxed);
        self.frames_written.fetch_add(1, Ordering::Relaxed);
        if is_user_traffic {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            self.last_activity_nanos.store(now, Ordering::Relaxed);
        }
    }

    /// Get current pending write queue depth.
    pub fn pending_writes(&self) -> usize {
        self.pending_writes.load(Ordering::Relaxed)
    }

    /// Record an RTT probe result.
    pub fn record_rtt(&self, rtt: Duration) {
        self.rtt.record(rtt);
    }

    /// Get current average RTT (None if no probes received yet).
    pub fn average_rtt(&self) -> Option<Duration> {
        self.rtt.average()
    }

    /// Get total bytes written.
    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get total frames written.
    pub fn total_frames_written(&self) -> u64 {
        self.frames_written.load(Ordering::Relaxed)
    }

    /// The load score for this connection (lower = less loaded).
    ///
    /// Primary: pending write queue depth. Tiebreaker: RTT (lower is better).
    pub fn load_score(&self) -> (usize, u64) {
        (self.pending_writes(), self.rtt.average_nanos())
    }

    /// Duration since the last write activity on this connection.
    ///
    /// Returns `None` if the clock cannot be read or the connection has
    /// never been used (in which case idle time is measured from creation).
    pub fn idle_duration(&self) -> Option<Duration> {
        let last = self.last_activity_nanos.load(Ordering::Relaxed);
        if last == 0 {
            return None;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos() as u64;
        Some(Duration::from_nanos(now.saturating_sub(last)))
    }
}

// ─── PeerPool ────────────────────────────────────────────────────────────────

/// Scaling decision made by the peer pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalingDecision {
    /// No action needed.
    None,
    /// Should establish a new connection to this peer.
    ScaleUp,
    /// Should drain and close one connection (returns the connection ID to drain).
    ScaleDown { connection_id: usize },
}

/// Per-peer connection pool managing multiple connections with load-based selection.
///
/// This is the core scheduling component: given multiple connections to a peer,
/// it picks the least-loaded one for each frame and monitors RTT to recommend
/// scaling actions.
///
/// Connection IDs are stable — they don't change when other connections are
/// added or removed.
pub struct PeerPool {
    /// Connection metrics keyed by stable connection ID.
    connections: RwLock<HashMap<usize, Arc<ConnectionMetrics>>>,
    /// Next ID to assign.
    next_id: AtomicUsize,
    /// Configuration for scaling decisions.
    config: SharedConnectionConfig,
    /// Timestamp when all connections dropped below scale-down threshold.
    /// Protected by mutex because scaling decisions are infrequent.
    cooldown_start: Mutex<Option<tokio::time::Instant>>,
}

impl PeerPool {
    /// Create a new peer pool with the given initial connection count.
    ///
    /// # Panics
    ///
    /// Panics if `config.min_connections < 1` or `config.min_connections > config.max_connections`.
    pub fn new(
        initial_connections: usize,
        config: SharedConnectionConfig,
    ) -> crate::Result<Self> {
        if config.min_connections < 1 {
            return Err(crate::Error::InvalidConfig(
                "min_connections must be at least 1".into(),
            ));
        }
        if config.min_connections > config.max_connections {
            return Err(crate::Error::InvalidConfig(format!(
                "min_connections ({}) must be <= max_connections ({})",
                config.min_connections, config.max_connections
            )));
        }

        let count = initial_connections
            .max(config.min_connections)
            .min(config.max_connections);
        let mut connections = HashMap::new();
        for id in 0..count {
            connections.insert(
                id,
                Arc::new(ConnectionMetrics::new(id, config.rtt_ema_alpha)),
            );
        }

        Ok(Self {
            next_id: AtomicUsize::new(count),
            connections: RwLock::new(connections),
            config,
            cooldown_start: Mutex::new(None),
        })
    }

    /// Select a connection and atomically reserve it.
    ///
    /// **Skips dead connections** — connections marked dead via
    /// `ConnectionMetrics::mark_dead()` are excluded from selection.
    /// Returns `None` if all connections are dead.
    ///
    /// **Low-load packing**: when total pending writes across live
    /// connections is below the live connection count, traffic is
    /// concentrated onto the fewest connections to let others go idle.
    ///
    /// **High-load spreading**: when the pool is busy, the least-loaded
    /// live connection is selected to balance throughput.
    ///
    /// Atomically increments `pending_writes` on the selected connection.
    /// The caller must call `dequeue()` after the frame has been written,
    /// or `rollback_reservation()` if the send could not be completed.
    pub fn select_and_reserve(&self) -> Option<Arc<ConnectionMetrics>> {
        let live: Vec<_> = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections
                .values()
                .filter(|c| c.is_alive())
                .cloned()
                .collect(),
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return None;
            }
        };

        if live.is_empty() {
            return None;
        }

        let total_pending: usize = live.iter().map(|c| c.pending_writes()).sum();

        let conn = if total_pending < live.len() {
            // Low load — pack onto fewest connections to let others go idle.
            live.into_iter()
                .max_by_key(|c| {
                    let (pending, rtt) = c.load_score();
                    (pending, std::cmp::Reverse(rtt))
                })
                // SAFETY: emptiness checked above while holding the same lock
                .expect("live connection set is non-empty after empty check")
        } else {
            // High load — spread across connections.
            live.into_iter()
                .min_by_key(|c| c.load_score())
                // SAFETY: emptiness checked above while holding the same lock
                .expect("live connection set is non-empty after empty check")
        };

        conn.enqueue(); // Atomic reservation
        Some(conn)
    }

    /// Select the least-loaded live connection without reserving.
    ///
    /// Use [`Self::select_and_reserve`] for production send paths to avoid
    /// concurrent senders all choosing the same connection.
    /// This method is useful for read-only inspection or diagnostics.
    pub fn select_connection(&self) -> Option<Arc<ConnectionMetrics>> {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return None;
            }
        };
        connections
            .values()
            .filter(|c| c.is_alive())
            .min_by_key(|c| c.load_score())
            .cloned()
    }

    /// Select a live connection with the lowest load, excluding specified IDs.
    pub fn select_connection_excluding(
        &self,
        exclude: &HashSet<usize>,
    ) -> Option<Arc<ConnectionMetrics>> {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return None;
            }
        };
        connections
            .values()
            .filter(|c| c.is_alive() && !exclude.contains(&c.id))
            .min_by_key(|c| c.load_score())
            .cloned()
    }

    /// Count of live (non-dead) connections.
    pub fn live_connection_count(&self) -> usize {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return 0;
            }
        };
        connections.values().filter(|c| c.is_alive()).count()
    }

    /// Get metrics for a specific connection by ID.
    pub fn connection(&self, id: usize) -> Option<Arc<ConnectionMetrics>> {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return None;
            }
        };
        connections.get(&id).cloned()
    }

    /// Current number of tracked connections.
    pub fn connection_count(&self) -> usize {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return 0;
            }
        };
        connections.len()
    }

    /// Add a new connection to the pool. Returns its metrics handle,
    /// or `None` if already at `max_connections` live connections.
    pub fn add_connection(&self) -> Option<Arc<ConnectionMetrics>> {
        let mut connections = match self.connections.write().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return None;
            }
        };
        let live_count = connections.values().filter(|c| c.is_alive()).count();
        if live_count >= self.config.max_connections {
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let metrics = Arc::new(ConnectionMetrics::new(id, self.config.rtt_ema_alpha));
        connections.insert(id, metrics.clone());
        Some(metrics)
    }

    /// Remove a connection from the pool by ID.
    ///
    /// Returns true if removed, false if ID not found or removing it would put
    /// the live connection count below `min_connections`.
    pub fn remove_connection(&self, id: usize) -> bool {
        let mut connections = match self.connections.write().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return false;
            }
        };
        let Some(metrics) = connections.get(&id) else {
            return false;
        };
        if metrics.is_alive()
            && connections.values().filter(|conn| conn.is_alive()).count()
                <= self.config.min_connections
        {
            return false;
        }
        connections.remove(&id).is_some()
    }

    /// Evaluate whether scaling is needed based on current RTT and idle state.
    ///
    /// This should be called periodically (e.g., after each probe response).
    /// Dead connections are excluded from all scaling decisions.
    /// Connections without any RTT measurement are excluded from RTT-based
    /// scaling decisions. Idle connections (no write activity for longer than
    /// `idle_timeout`) are candidates for removal regardless of RTT.
    pub async fn evaluate_scaling(&self) -> ScalingDecision {
        let threshold_up = self.config.rtt_scale_up_threshold;
        let threshold_down = self.config.rtt_scale_down_threshold;
        let live: Vec<_> = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections
                .values()
                .filter(|c| c.is_alive())
                .cloned()
                .collect(),
            Err(_) => {
                // TODO: propagate poisoned pool locks once evaluate_scaling can return Result.
                return ScalingDecision::None;
            }
        };
        let live_count = live.len();

        // Check if any live connection with measurements exceeds the scale-up threshold
        let any_overloaded = live
            .iter()
            .any(|c| c.average_rtt().is_some_and(|rtt| rtt > threshold_up));

        if any_overloaded && live_count < self.config.max_connections {
            // Reset cooldown since we're scaling up
            *self.cooldown_start.lock().await = None;
            return ScalingDecision::ScaleUp;
        }

        // Check for idle live connections (no traffic for idle_timeout).
        // Remove the longest-idle connection, but never below min_connections.
        if let Some(idle_timeout) = self.config.idle_timeout {
            if live_count > self.config.min_connections {
                let longest_idle = live
                    .iter()
                    .filter(|c| c.pending_writes() == 0)
                    .filter_map(|c| c.idle_duration().map(|d| (c.id, d)))
                    .filter(|(_, d)| *d >= idle_timeout)
                    .max_by_key(|(_, d)| *d);

                if let Some((conn_id, _)) = longest_idle {
                    return ScalingDecision::ScaleDown {
                        connection_id: conn_id,
                    };
                }
            }
        }

        // Check if ALL live connections have measurements and are below scale-down threshold.
        let all_measured = live.iter().all(|c| c.average_rtt().is_some());

        if !all_measured {
            return ScalingDecision::None;
        }

        let all_underloaded = live.iter().all(|c| {
            c.average_rtt()
                // SAFETY: RTT is recorded for every connection upon creation
                .expect("all live connections have RTT measurements")
                < threshold_down
        });

        if all_underloaded && live_count > self.config.min_connections {
            let mut cooldown = self.cooldown_start.lock().await;
            match *cooldown {
                None => {
                    // Start cooldown timer
                    *cooldown = Some(tokio::time::Instant::now());
                    ScalingDecision::None
                }
                Some(start) => {
                    if start.elapsed() >= self.config.cooldown_period {
                        // Cooldown expired — scale down the live connection with highest RTT
                        *cooldown = None;
                        let worst = live
                            .iter()
                            .max_by_key(|c| c.rtt.average_nanos())
                            .map(|c| c.id)
                            .unwrap_or(0);
                        ScalingDecision::ScaleDown {
                            connection_id: worst,
                        }
                    } else {
                        ScalingDecision::None
                    }
                }
            }
        } else {
            // Not all underloaded — reset cooldown
            *self.cooldown_start.lock().await = None;
            ScalingDecision::None
        }
    }

    /// Get a snapshot of all connection metrics for diagnostics.
    pub fn metrics_snapshot(&self) -> Vec<ConnectionSnapshot> {
        let connections = match self.connections.read().or_poison("peer pool connections") {
            Ok(connections) => connections,
            Err(_) => {
                // TODO: propagate poisoned pool locks once these accessors can return Result.
                return Vec::new();
            }
        };
        let mut snaps: Vec<_> = connections
            .values()
            .map(|c| ConnectionSnapshot {
                id: c.id,
                pending_writes: c.pending_writes(),
                avg_rtt: c.average_rtt(),
                total_bytes: c.total_bytes_written(),
                total_frames: c.total_frames_written(),
                idle_duration: c.idle_duration(),
            })
            .collect();
        snaps.sort_by_key(|s| s.id);
        snaps
    }
}

/// Diagnostic snapshot of a single connection's state.
#[derive(Debug, Clone)]
pub struct ConnectionSnapshot {
    /// Connection ID within the peer pool.
    pub id: usize,
    /// Current pending write queue depth.
    pub pending_writes: usize,
    /// Current average RTT (None if no probes yet).
    pub avg_rtt: Option<Duration>,
    /// Total bytes written since connection established.
    pub total_bytes: u64,
    /// Total frames written since connection established.
    pub total_frames: u64,
    /// Duration since the last write activity (None if never used).
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
    fn connection_metrics_enqueue_dequeue() {
        let m = ConnectionMetrics::new(0, 0.2);
        assert_eq!(m.pending_writes(), 0);

        m.enqueue();
        m.enqueue();
        assert_eq!(m.pending_writes(), 2);

        m.dequeue(100, true);
        assert_eq!(m.pending_writes(), 1);
        assert_eq!(m.total_bytes_written(), 100);
        assert_eq!(m.total_frames_written(), 1);
    }

    #[test]
    fn connection_metrics_load_score() {
        let m = ConnectionMetrics::new(0, 0.2);
        m.enqueue();
        m.enqueue();
        m.record_rtt(Duration::from_millis(3));

        let (pending, rtt_nanos) = m.load_score();
        assert_eq!(pending, 2);
        assert_eq!(rtt_nanos, 3_000_000); // 3ms in nanos
    }

    #[test]
    fn peer_pool_select_least_loaded() {
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 8,
            ..Default::default()
        };
        let pool = PeerPool::new(3, config).unwrap();

        // Connection 0: 3 pending
        pool.connection(0).unwrap().enqueue();
        pool.connection(0).unwrap().enqueue();
        pool.connection(0).unwrap().enqueue();
        // Connection 1: 1 pending
        pool.connection(1).unwrap().enqueue();
        // Connection 2: 0 pending

        let selected = pool.select_connection().unwrap();
        assert_eq!(selected.id, 2); // least loaded
    }

    #[test]
    fn peer_pool_select_tiebreak_by_rtt() {
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 8,
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        // Both have 0 pending, but different RTT
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(5));
        pool.connection(1)
            .unwrap()
            .record_rtt(Duration::from_millis(2));

        let selected = pool.select_connection().unwrap();
        assert_eq!(selected.id, 1); // lower RTT
    }

    #[tokio::test]
    async fn peer_pool_scale_up_on_high_rtt() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_up_threshold: Duration::from_millis(5),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config).unwrap();

        // No RTT yet — no scaling
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);

        // Record high RTT
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(10));
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::ScaleUp);
    }

    #[tokio::test]
    async fn peer_pool_no_scale_up_at_max() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 2,
            rtt_scale_up_threshold: Duration::from_millis(5),
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(10));
        // Already at max — no scale up
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);
    }

    #[tokio::test]
    async fn peer_pool_scale_down_after_cooldown() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_down_threshold: Duration::from_millis(5),
            cooldown_period: Duration::from_millis(10),
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        // Both connections have low RTT
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(1));
        pool.connection(1)
            .unwrap()
            .record_rtt(Duration::from_millis(2));

        // First evaluation starts cooldown
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);

        // Wait for cooldown
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Now should scale down (remove highest-RTT connection)
        let decision = pool.evaluate_scaling().await;
        assert_eq!(decision, ScalingDecision::ScaleDown { connection_id: 1 });
    }

    #[tokio::test]
    async fn peer_pool_no_scale_down_at_min() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_down_threshold: Duration::from_millis(5),
            cooldown_period: Duration::from_millis(1),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config).unwrap();
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(1));

        tokio::time::sleep(Duration::from_millis(5)).await;
        // Already at min — no scale down
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);
    }

    #[tokio::test]
    async fn peer_pool_no_scale_down_without_measurements() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_down_threshold: Duration::from_millis(5),
            cooldown_period: Duration::from_millis(1),
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();
        // No RTT recorded on either connection

        tokio::time::sleep(Duration::from_millis(5)).await;
        // No measurements — don't make scaling decisions
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);
    }

    #[test]
    fn peer_pool_add_remove_connection() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            ..Default::default()
        };
        let pool = PeerPool::new(1, config).unwrap();
        assert_eq!(pool.connection_count(), 1);

        let new_conn = pool.add_connection().unwrap();
        assert_eq!(new_conn.id, 1);
        assert_eq!(pool.connection_count(), 2);

        // Remove connection 1
        assert!(pool.remove_connection(1));
        assert_eq!(pool.connection_count(), 1);

        // Can't go below min
        assert!(!pool.remove_connection(0));

        // Add another — gets unique ID (2, not reusing 1)
        let another = pool.add_connection().unwrap();
        assert_eq!(another.id, 2);
    }

    #[test]
    fn peer_pool_add_connection_at_max() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 2,
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();
        assert_eq!(pool.connection_count(), 2);

        // Already at max — returns None
        assert!(pool.add_connection().is_none());
    }

    #[test]
    fn peer_pool_select_and_reserve() {
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 8,
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        // Connection 0: load up
        pool.connection(0).unwrap().enqueue();
        pool.connection(0).unwrap().enqueue();

        // select_and_reserve picks conn 1 (less loaded) AND increments its pending
        let selected = pool.select_and_reserve().unwrap();
        assert_eq!(selected.id, 1);
        assert_eq!(selected.pending_writes(), 1); // was 0, now 1 from reservation
    }

    #[test]
    fn select_packs_under_low_load() {
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 8,
            ..Default::default()
        };
        let pool = PeerPool::new(3, config).unwrap();

        // All connections idle (total_pending=0 < 3 connections) → low-load packing.
        // First select should pick ONE connection and keep using it.
        let first = pool.select_and_reserve().unwrap();
        let first_id = first.id;
        // Now total_pending=1 < 3 → still low-load → pack onto same connection
        let second = pool.select_and_reserve().unwrap();
        assert_eq!(
            second.id, first_id,
            "low-load packing should reuse same connection"
        );
        // Now total_pending=2 < 3 → still low-load
        let third = pool.select_and_reserve().unwrap();
        assert_eq!(
            third.id, first_id,
            "low-load packing should still reuse same connection"
        );
        // Now total_pending=3 >= 3 → high-load → spreads to least-loaded
        let fourth = pool.select_and_reserve().unwrap();
        assert_ne!(
            fourth.id, first_id,
            "high-load should spread to a different connection"
        );
    }

    #[test]
    fn peer_pool_stable_ids_after_removal() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 8,
            ..Default::default()
        };
        let pool = PeerPool::new(3, config).unwrap();

        // Remove middle connection (id=1)
        pool.remove_connection(1);

        // Connection 0 and 2 still accessible by their original IDs
        assert!(pool.connection(0).is_some());
        assert!(pool.connection(1).is_none()); // removed
        assert!(pool.connection(2).is_some());
    }

    #[test]
    fn peer_pool_metrics_snapshot() {
        let config = SharedConnectionConfig::default();
        let pool = PeerPool::new(2, config).unwrap();

        pool.connection(0).unwrap().enqueue();
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(3));
        pool.connection(1).unwrap().enqueue();
        pool.connection(1).unwrap().enqueue();

        let snap = pool.metrics_snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].pending_writes, 1);
        assert_eq!(snap[0].avg_rtt, Some(Duration::from_millis(3)));
        assert_eq!(snap[1].pending_writes, 2);
        assert_eq!(snap[1].avg_rtt, None);
    }

    #[test]
    fn connection_mode_default_is_dedicated() {
        assert!(matches!(
            ConnectionMode::default(),
            ConnectionMode::Dedicated
        ));
    }

    #[test]
    fn shared_config_default_values() {
        let config = SharedConnectionConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 8);
        assert_eq!(config.rtt_scale_up_threshold, Duration::from_millis(5));
        assert_eq!(config.rtt_scale_down_threshold, Duration::from_millis(1));
        assert_eq!(config.cooldown_period, Duration::from_secs(30));
        assert_eq!(config.probe_interval, Duration::from_millis(100));
        assert_eq!(config.reorder_timeout, Duration::from_millis(50));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn idle_connection_scaled_down() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            idle_timeout: Some(Duration::from_millis(10)),
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        // Both connections are freshly created — not yet idle
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);

        // Wait for the idle timeout to elapse
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Now one connection should be identified as idle and removed
        let decision = pool.evaluate_scaling().await;
        assert!(
            matches!(decision, ScalingDecision::ScaleDown { .. }),
            "expected ScaleDown for idle connection, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn active_connection_not_idle() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            idle_timeout: Some(Duration::from_millis(20)),
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        // Wait a bit but keep one connection active
        tokio::time::sleep(Duration::from_millis(25)).await;
        pool.connection(0).unwrap().dequeue(100, true); // refreshes last_activity

        // Connection 0 is active, connection 1 is idle
        let decision = pool.evaluate_scaling().await;
        assert_eq!(
            decision,
            ScalingDecision::ScaleDown { connection_id: 1 },
            "should remove idle conn 1, not active conn 0"
        );
    }

    #[tokio::test]
    async fn idle_respects_min_connections() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            idle_timeout: Some(Duration::from_millis(10)),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config).unwrap();

        tokio::time::sleep(Duration::from_millis(15)).await;

        // At min_connections — should not scale down even if idle
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);
    }

    #[tokio::test]
    async fn idle_disabled_when_none() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            idle_timeout: None,
            ..Default::default()
        };
        let pool = PeerPool::new(2, config).unwrap();

        tokio::time::sleep(Duration::from_millis(15)).await;

        // idle_timeout is None — no idle scale-down
        assert_eq!(pool.evaluate_scaling().await, ScalingDecision::None);
    }

    #[test]
    fn mark_dead_skips_in_selection() {
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            ..Default::default()
        };
        let pool = PeerPool::new(3, config).unwrap();

        // Mark connection 0 as dead
        pool.connection(0).unwrap().mark_dead();

        // select_connection should skip dead connections
        let selected = pool.select_connection().unwrap();
        assert_ne!(selected.id, 0, "dead connection should be skipped");
        assert_eq!(pool.live_connection_count(), 2);
    }

    #[test]
    fn mark_dead_idempotent() {
        let config = SharedConnectionConfig::default();
        let pool = PeerPool::new(1, config).unwrap();

        let conn = pool.connection(0).unwrap();
        assert!(conn.is_alive());

        // First mark_dead returns true
        assert!(conn.mark_dead());
        assert!(!conn.is_alive());

        // Second mark_dead returns false (already dead)
        assert!(!conn.mark_dead());
    }

    #[test]
    fn select_and_reserve_returns_none_all_dead() {
        let config = SharedConnectionConfig::default();
        let pool = PeerPool::new(2, config).unwrap();

        pool.connection(0).unwrap().mark_dead();
        pool.connection(1).unwrap().mark_dead();

        assert!(pool.select_and_reserve().is_none());
        assert!(pool.select_connection().is_none());
        assert_eq!(pool.live_connection_count(), 0);
    }

    #[test]
    fn rollback_reservation_decrements_pending() {
        let config = SharedConnectionConfig::default();
        let pool = PeerPool::new(1, config).unwrap();

        let conn = pool.connection(0).unwrap();
        conn.enqueue();
        conn.enqueue();
        assert_eq!(conn.pending_writes(), 2);

        conn.rollback_reservation();
        assert_eq!(conn.pending_writes(), 1);

        // Rollback at 0 should not underflow
        conn.rollback_reservation();
        conn.rollback_reservation();
        assert_eq!(conn.pending_writes(), 0);
    }

    #[test]
    fn select_connection_excluding_skips_specified() {
        let config = SharedConnectionConfig::default();
        let pool = PeerPool::new(3, config).unwrap();

        let mut exclude = HashSet::new();
        exclude.insert(0);
        exclude.insert(1);

        let selected = pool.select_connection_excluding(&exclude).unwrap();
        assert_eq!(selected.id, 2, "should only select non-excluded connection");

        exclude.insert(2);
        assert!(
            pool.select_connection_excluding(&exclude).is_none(),
            "all excluded should return None"
        );
    }
}
