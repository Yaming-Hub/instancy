//! Observability metrics for dataflow execution.
//!
//! Provides per-dataflow and per-operator metrics including CPU time,
//! activation counts, and records processed.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

    /// Record one activation with the given CPU time and records processed.
    pub fn record_activation(&self, cpu_time: Duration, records: u64) {
        self.activations.fetch_add(1, Ordering::Relaxed);
        self.cpu_time_nanos
            .fetch_add(cpu_time.as_nanos() as u64, Ordering::Relaxed);
        self.records_processed
            .fetch_add(records, Ordering::Relaxed);
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
            if self.bp_max_blocked_nanos
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

/// Aggregate metrics for an entire dataflow execution.
#[derive(Debug)]
pub struct DataflowMetrics {
    /// Dataflow name.
    name: String,
    /// Wall-clock time since the dataflow started.
    wall_time_nanos: AtomicU64,
    /// Per-operator metrics collectors.
    operators: Vec<Arc<OperatorMetricsCollector>>,
}

impl DataflowMetrics {
    /// Create a new dataflow metrics container.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            wall_time_nanos: AtomicU64::new(0),
            operators: Vec::new(),
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
        self.operators
            .iter()
            .map(|op| op.snapshot().cpu_time)
            .sum()
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
    }
}
