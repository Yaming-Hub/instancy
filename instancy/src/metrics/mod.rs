//! Observability metrics for dataflow execution.
//!
//! Provides per-dataflow and per-operator metrics including CPU time,
//! activation counts, records processed, and backpressure statistics.
//!
//! Key components:
//! - [`DataflowMetrics`] — aggregate metrics for a complete dataflow execution
//! - [`MetricsConfig`] — granularity controls (which categories to collect)
//! - [`OperatorMetricsCollector`] — lock-free per-operator metrics accumulator
//! - [`ChannelMetricsCollector`] — lock-free per-exchange-edge counters
//! - [`ActivationGuard`] — RAII timer for measuring operator activation cost
//! - [`DataflowResult`] — execution result bundled with collected metrics

pub mod activation;
pub mod tracing_integration;

pub use activation::ActivationGuard;
pub use tracing_integration::TracingConfig;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// MetricsConfig — granularity controls
// ---------------------------------------------------------------------------

/// Controls what observability data is collected during dataflow execution.
///
/// Each category can be independently enabled. By default only
/// `operator_summary` is on. Enable more categories for deeper analysis
/// at the cost of increased overhead.
///
/// # Example
///
/// ```
/// use instancy::metrics::MetricsConfig;
///
/// // Cheap operator-level stats only (the default):
/// let cfg = MetricsConfig::default();
/// assert!(cfg.operator_summary);
/// assert!(!cfg.channel_counters);
///
/// // Full collection for post-execution analysis:
/// let cfg = MetricsConfig::full();
/// assert!(cfg.operator_summary);
/// assert!(cfg.channel_counters);
/// ```
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    /// Aggregate per-operator stats: activations, CPU time, records processed,
    /// backpressure counts. ~1% overhead (atomic increments).
    pub operator_summary: bool,

    /// Per-edge transfer counters: items and bytes sent through each exchange
    /// channel. ~2-3% overhead (atomic add per batch push).
    pub channel_counters: bool,
}

impl MetricsConfig {
    /// No collection at all. Zero overhead.
    pub fn none() -> Self {
        Self {
            operator_summary: false,
            channel_counters: false,
        }
    }

    /// Cheap aggregate operator stats only (the default).
    pub fn summary_only() -> Self {
        Self {
            operator_summary: true,
            channel_counters: false,
        }
    }

    /// All currently implemented categories enabled.
    pub fn full() -> Self {
        Self {
            operator_summary: true,
            channel_counters: true,
        }
    }

    /// Returns `true` if any metrics category is enabled.
    #[inline]
    pub fn any_enabled(&self) -> bool {
        self.operator_summary || self.channel_counters
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self::summary_only()
    }
}

/// Per-operator metrics.
#[derive(Debug, Clone)]
pub struct OperatorMetrics {
    /// Human-readable operator name.
    pub name: String,
    /// Operator index in the dataflow.
    pub index: usize,
    /// Number of times this operator has been activated.
    pub activations: u64,
    /// Total CPU time spent in this operator.
    pub cpu_time: Duration,
    /// Total records processed by this operator.
    pub records_processed: u64,
    /// Backpressure statistics for this operator.
    pub backpressure: BackpressureMetrics,
}

/// Backpressure statistics for a single operator.
#[derive(Debug, Clone, Default)]
pub struct BackpressureMetrics {
    /// Number of times this operator was blocked by downstream backpressure.
    pub blocked_count: u64,
    /// Total time spent blocked waiting for downstream capacity.
    pub blocked_duration: Duration,
    /// Maximum single blocking duration observed.
    pub max_blocked_duration: Duration,
}

impl OperatorMetrics {
    /// Create empty metrics for an operator.
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
            activations: 0,
            cpu_time: Duration::ZERO,
            records_processed: 0,
            backpressure: BackpressureMetrics::default(),
        }
    }
}

/// Shared, atomic metrics collector for a single operator.
///
/// Used by worker threads to report metrics without locking.
#[derive(Debug)]
pub struct OperatorMetricsCollector {
    /// Human-readable operator name.
    name: String,
    /// Operator index.
    index: usize,
    /// Activation count.
    activations: AtomicU64,
    /// CPU time in nanoseconds.
    cpu_time_nanos: AtomicU64,
    /// Records processed.
    records_processed: AtomicU64,
    /// Backpressure blocked count.
    bp_blocked_count: AtomicU64,
    /// Backpressure total blocked duration in nanoseconds.
    bp_blocked_nanos: AtomicU64,
    /// Backpressure max single blocked duration in nanoseconds.
    bp_max_blocked_nanos: AtomicU64,
}

impl OperatorMetricsCollector {
    /// Create a new metrics collector for an operator.
    pub fn new(name: impl Into<String>, index: usize) -> Self {
        Self {
            name: name.into(),
            index,
            activations: AtomicU64::new(0),
            cpu_time_nanos: AtomicU64::new(0),
            records_processed: AtomicU64::new(0),
            bp_blocked_count: AtomicU64::new(0),
            bp_blocked_nanos: AtomicU64::new(0),
            bp_max_blocked_nanos: AtomicU64::new(0),
        }
    }

    /// Get the operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Record one activation with the given CPU time and records processed.
    pub fn record_activation(&self, cpu_time: Duration, records: u64) {
        self.activations.fetch_add(1, Ordering::Relaxed);
        self.cpu_time_nanos
            .fetch_add(cpu_time.as_nanos() as u64, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
    }

    /// Record a backpressure event with the duration the operator was blocked.
    pub fn record_backpressure(&self, blocked_duration: Duration) {
        self.bp_blocked_count.fetch_add(1, Ordering::Relaxed);
        let nanos = blocked_duration.as_nanos() as u64;
        self.bp_blocked_nanos.fetch_add(nanos, Ordering::Relaxed);
        // Update max using CAS loop
        loop {
            let current_max = self.bp_max_blocked_nanos.load(Ordering::Relaxed);
            if nanos <= current_max {
                break;
            }
            if self
                .bp_max_blocked_nanos
                .compare_exchange_weak(current_max, nanos, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Snapshot the current metrics.
    pub fn snapshot(&self) -> OperatorMetrics {
        OperatorMetrics {
            name: self.name.clone(),
            index: self.index,
            activations: self.activations.load(Ordering::Relaxed),
            cpu_time: Duration::from_nanos(self.cpu_time_nanos.load(Ordering::Relaxed)),
            records_processed: self.records_processed.load(Ordering::Relaxed),
            backpressure: BackpressureMetrics {
                blocked_count: self.bp_blocked_count.load(Ordering::Relaxed),
                blocked_duration: Duration::from_nanos(
                    self.bp_blocked_nanos.load(Ordering::Relaxed),
                ),
                max_blocked_duration: Duration::from_nanos(
                    self.bp_max_blocked_nanos.load(Ordering::Relaxed),
                ),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// ChannelMetrics — per-exchange-edge counters
// ---------------------------------------------------------------------------

/// Snapshot of per-exchange-edge transfer counters.
#[derive(Debug, Clone)]
pub struct ChannelMetrics {
    /// Edge index in the dataflow graph.
    pub edge_index: usize,
    /// Human-readable label (e.g., `"exchange[0] op2→op3"`).
    pub label: String,
    /// Total items (records) transferred through this edge.
    pub items_transferred: u64,
    /// Total bytes transferred (estimated from `std::mem::size_of_val` per item).
    pub bytes_transferred: u64,
}

/// Shared, atomic channel metrics collector for a single exchange edge.
///
/// One instance is shared by all source workers pushing through the same
/// logical edge. Lock-free via `AtomicU64` — overhead is two relaxed
/// atomic adds per batch push (~2-3% when enabled).
#[derive(Debug)]
pub struct ChannelMetricsCollector {
    /// Edge index in the dataflow graph.
    edge_index: usize,
    /// Human-readable label.
    label: String,
    /// Total items transferred.
    items_transferred: AtomicU64,
    /// Total bytes transferred.
    bytes_transferred: AtomicU64,
}

impl ChannelMetricsCollector {
    /// Create a new collector for an exchange edge.
    pub fn new(edge_index: usize, label: impl Into<String>) -> Self {
        Self {
            edge_index,
            label: label.into(),
            items_transferred: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
        }
    }

    /// Record a batch of items pushed through this edge.
    #[inline]
    pub fn record_push(&self, items: u64, bytes: u64) {
        self.items_transferred.fetch_add(items, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Snapshot the current counters.
    pub fn snapshot(&self) -> ChannelMetrics {
        ChannelMetrics {
            edge_index: self.edge_index,
            label: self.label.clone(),
            items_transferred: self.items_transferred.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
        }
    }
}

/// Aggregate metrics for an entire dataflow execution.
#[derive(Debug)]
pub struct DataflowMetrics {
    /// Dataflow name.
    name: String,
    /// Wall-clock time since the dataflow started.
    wall_time_nanos: AtomicU64,
    /// Per-operator metrics collectors.
    operators: Vec<Arc<OperatorMetricsCollector>>,
    /// Per-exchange-edge channel metrics collectors.
    channels: Vec<Arc<ChannelMetricsCollector>>,
}

impl DataflowMetrics {
    /// Create a new dataflow metrics container.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            wall_time_nanos: AtomicU64::new(0),
            operators: Vec::new(),
            channels: Vec::new(),
        }
    }

    /// Register an operator and get its metrics collector.
    pub fn register_operator(
        &mut self,
        name: impl Into<String>,
        index: usize,
    ) -> Arc<OperatorMetricsCollector> {
        let collector = Arc::new(OperatorMetricsCollector::new(name, index));
        self.operators.push(collector.clone());
        collector
    }

    /// Register an exchange edge and get its channel metrics collector.
    pub fn register_channel(
        &mut self,
        edge_index: usize,
        label: impl Into<String>,
    ) -> Arc<ChannelMetricsCollector> {
        let collector = Arc::new(ChannelMetricsCollector::new(edge_index, label));
        self.channels.push(collector.clone());
        collector
    }

    /// Register a pre-existing channel metrics collector (shared across workers).
    pub fn register_existing_channel(&mut self, collector: Arc<ChannelMetricsCollector>) {
        self.channels.push(collector);
    }

    /// Set the wall-clock time.
    pub fn set_wall_time(&self, duration: Duration) {
        self.wall_time_nanos
            .store(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the wall-clock time.
    pub fn wall_time(&self) -> Duration {
        Duration::from_nanos(self.wall_time_nanos.load(Ordering::Relaxed))
    }

    /// Total CPU time across all operators.
    pub fn total_cpu_time(&self) -> Duration {
        self.operators.iter().map(|op| op.snapshot().cpu_time).sum()
    }

    /// Total activations across all operators.
    pub fn total_activations(&self) -> u64 {
        self.operators
            .iter()
            .map(|op| op.snapshot().activations)
            .sum()
    }

    /// Total records processed across all operators.
    pub fn total_records_processed(&self) -> u64 {
        self.operators
            .iter()
            .map(|op| op.snapshot().records_processed)
            .sum()
    }

    /// Snapshot all operator metrics.
    pub fn operator_snapshots(&self) -> Vec<OperatorMetrics> {
        self.operators.iter().map(|op| op.snapshot()).collect()
    }

    /// Number of registered operators.
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Snapshot all channel metrics.
    pub fn channel_snapshots(&self) -> Vec<ChannelMetrics> {
        self.channels.iter().map(|ch| ch.snapshot()).collect()
    }

    /// Total items transferred across all exchange edges.
    pub fn total_items_transferred(&self) -> u64 {
        self.channels
            .iter()
            .map(|ch| ch.items_transferred.load(Ordering::Relaxed))
            .sum()
    }

    /// Total bytes transferred across all exchange edges.
    pub fn total_bytes_transferred(&self) -> u64 {
        self.channels
            .iter()
            .map(|ch| ch.bytes_transferred.load(Ordering::Relaxed))
            .sum()
    }

    /// Number of registered channel collectors.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
}

/// Result of a dataflow execution, bundling the computation result with metrics.
///
/// Returned by `execute()` when the dataflow completes (or errors).
/// Provides access to both the output value and the collected performance data.
#[derive(Debug)]
pub struct DataflowResult<R> {
    /// The computation result (Ok on success, Err on failure).
    pub result: Result<R, crate::error::Error>,
    /// Metrics collected during execution.
    pub metrics: Arc<DataflowMetrics>,
}

impl<R> DataflowResult<R> {
    /// Create a new dataflow result.
    pub fn new(result: Result<R, crate::error::Error>, metrics: Arc<DataflowMetrics>) -> Self {
        Self { result, metrics }
    }

    /// Check if the dataflow completed successfully.
    pub fn is_ok(&self) -> bool {
        self.result.is_ok()
    }

    /// Get the metrics regardless of success/failure.
    pub fn metrics(&self) -> &DataflowMetrics {
        &self.metrics
    }

    /// Unwrap the result, discarding metrics.
    pub fn into_result(self) -> Result<R, crate::error::Error> {
        self.result
    }

    /// Decompose into result and metrics (useful when you need both).
    pub fn into_parts(self) -> (Result<R, crate::error::Error>, Arc<DataflowMetrics>) {
        (self.result, self.metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_metrics_creation() {
        let m = OperatorMetrics::new("map", 0);
        assert_eq!(m.name, "map");
        assert_eq!(m.index, 0);
        assert_eq!(m.activations, 0);
        assert_eq!(m.cpu_time, Duration::ZERO);
        assert_eq!(m.records_processed, 0);
    }

    #[test]
    fn operator_collector_records_activations() {
        let collector = OperatorMetricsCollector::new("filter", 1);

        collector.record_activation(Duration::from_micros(100), 50);
        collector.record_activation(Duration::from_micros(200), 30);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.name, "filter");
        assert_eq!(snapshot.index, 1);
        assert_eq!(snapshot.activations, 2);
        assert_eq!(snapshot.cpu_time, Duration::from_micros(300));
        assert_eq!(snapshot.records_processed, 80);
    }

    #[test]
    fn dataflow_metrics_aggregation() {
        let mut metrics = DataflowMetrics::new("test_df");

        let op0 = metrics.register_operator("source", 0);
        let op1 = metrics.register_operator("map", 1);
        let op2 = metrics.register_operator("sink", 2);

        op0.record_activation(Duration::from_micros(100), 1000);
        op1.record_activation(Duration::from_micros(50), 1000);
        op1.record_activation(Duration::from_micros(50), 1000);
        op2.record_activation(Duration::from_micros(30), 2000);

        assert_eq!(metrics.operator_count(), 3);
        assert_eq!(metrics.total_activations(), 4);
        assert_eq!(metrics.total_cpu_time(), Duration::from_micros(230));
        assert_eq!(metrics.total_records_processed(), 5000);
    }

    #[test]
    fn dataflow_metrics_wall_time() {
        let metrics = DataflowMetrics::new("wall_test");
        metrics.set_wall_time(Duration::from_secs(5));
        assert_eq!(metrics.wall_time(), Duration::from_secs(5));
    }

    #[test]
    fn dataflow_metrics_name() {
        let metrics = DataflowMetrics::new("my_dataflow");
        assert_eq!(metrics.name(), "my_dataflow");
    }

    #[test]
    fn operator_snapshots() {
        let mut metrics = DataflowMetrics::new("snapshot_test");
        let op0 = metrics.register_operator("a", 0);
        let op1 = metrics.register_operator("b", 1);

        op0.record_activation(Duration::from_nanos(500), 10);
        op1.record_activation(Duration::from_nanos(300), 5);

        let snapshots = metrics.operator_snapshots();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].name, "a");
        assert_eq!(snapshots[0].records_processed, 10);
        assert_eq!(snapshots[1].name, "b");
        assert_eq!(snapshots[1].records_processed, 5);
    }

    #[test]
    fn collector_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OperatorMetricsCollector>();
        assert_send_sync::<Arc<OperatorMetricsCollector>>();
        assert_send_sync::<ChannelMetricsCollector>();
        assert_send_sync::<Arc<ChannelMetricsCollector>>();
    }

    #[test]
    fn channel_collector_records_and_snapshots() {
        let collector = ChannelMetricsCollector::new(0, "exchange[0]");

        collector.record_push(100, 800);
        collector.record_push(50, 400);

        let snap = collector.snapshot();
        assert_eq!(snap.edge_index, 0);
        assert_eq!(snap.label, "exchange[0]");
        assert_eq!(snap.items_transferred, 150);
        assert_eq!(snap.bytes_transferred, 1200);
    }

    #[test]
    fn dataflow_metrics_channel_registration() {
        let mut metrics = DataflowMetrics::new("ch_test");

        let ch0 = metrics.register_channel(0, "exchange[0]");
        let ch1 = metrics.register_channel(1, "exchange[1]");

        ch0.record_push(10, 80);
        ch1.record_push(20, 160);

        assert_eq!(metrics.channel_count(), 2);
        assert_eq!(metrics.total_items_transferred(), 30);
        assert_eq!(metrics.total_bytes_transferred(), 240);

        let snapshots = metrics.channel_snapshots();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].edge_index, 0);
        assert_eq!(snapshots[0].items_transferred, 10);
        assert_eq!(snapshots[1].edge_index, 1);
        assert_eq!(snapshots[1].items_transferred, 20);
    }

    #[test]
    fn register_existing_channel_collector() {
        let mut metrics = DataflowMetrics::new("existing_ch");

        let collector = Arc::new(ChannelMetricsCollector::new(5, "shared-edge"));
        collector.record_push(42, 336);

        metrics.register_existing_channel(Arc::clone(&collector));

        assert_eq!(metrics.channel_count(), 1);
        assert_eq!(metrics.total_items_transferred(), 42);

        // Further pushes via original collector are visible.
        collector.record_push(8, 64);
        assert_eq!(metrics.total_items_transferred(), 50);
    }
}
