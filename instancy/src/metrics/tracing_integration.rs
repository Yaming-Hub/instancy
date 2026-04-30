//! Tracing integration for structured observability.
//!
//! Provides configuration and helper functions for emitting structured tracing
//! events at key points in the dataflow execution lifecycle.
//!
//! When the `tracing` feature is enabled, the runtime automatically instruments:
//! - Operator activations (start/complete/backpressure)
//! - Progress updates (frontier advances)
//! - Connection events (establish/evict/health-check)
//! - Dataflow lifecycle (start/complete/error)

use std::sync::Arc;
use std::time::Duration;

use super::{DataflowMetrics, OperatorMetrics};

/// Configuration for tracing behavior.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Whether to emit per-activation trace events (high volume).
    /// Default: false (only summary at completion).
    pub trace_activations: bool,
    /// Whether to emit progress update events.
    /// Default: true.
    pub trace_progress: bool,
    /// Whether to emit connection lifecycle events.
    /// Default: true.
    pub trace_connections: bool,
    /// Minimum activation duration to emit a trace event (filters noise).
    /// Default: 0 (emit all).
    pub min_activation_duration: Duration,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            trace_activations: false,
            trace_progress: true,
            trace_connections: true,
            min_activation_duration: Duration::ZERO,
        }
    }
}

impl TracingConfig {
    /// Enable per-activation tracing (high volume, useful for debugging).
    pub fn with_activation_tracing(mut self) -> Self {
        self.trace_activations = true;
        self
    }

    /// Only trace activations longer than the given duration.
    pub fn with_min_activation_duration(mut self, duration: Duration) -> Self {
        self.min_activation_duration = duration;
        self
    }
}

/// Emit a tracing event when a dataflow starts execution.
#[cfg(feature = "tracing")]
pub fn trace_dataflow_started(name: &str, total_workers: usize) {
    tracing::info!(
        dataflow = name,
        workers = total_workers,
        "dataflow execution started"
    );
}

/// Emit a tracing event when a dataflow completes successfully.
#[cfg(feature = "tracing")]
pub fn trace_dataflow_completed(metrics: &DataflowMetrics) {
    tracing::info!(
        dataflow = metrics.name(),
        wall_time_ms = metrics.wall_time().as_millis() as u64,
        total_cpu_ms = metrics.total_cpu_time().as_millis() as u64,
        total_activations = metrics.total_activations(),
        total_records = metrics.total_records_processed(),
        operators = metrics.operator_count(),
        "dataflow execution completed"
    );
}

/// Emit a tracing event when a dataflow fails.
#[cfg(feature = "tracing")]
pub fn trace_dataflow_failed(name: &str, error: &crate::error::Error) {
    tracing::error!(
        dataflow = name,
        error = %error,
        "dataflow execution failed"
    );
}

/// Emit per-operator summary metrics at dataflow completion.
#[cfg(feature = "tracing")]
pub fn trace_operator_summary(dataflow_name: &str, op: &OperatorMetrics) {
    tracing::info!(
        dataflow = dataflow_name,
        operator = op.name.as_str(),
        operator_index = op.index,
        activations = op.activations,
        cpu_time_us = op.cpu_time.as_micros() as u64,
        records = op.records_processed,
        backpressure_count = op.backpressure.blocked_count,
        backpressure_total_us = op.backpressure.blocked_duration.as_micros() as u64,
        backpressure_max_us = op.backpressure.max_blocked_duration.as_micros() as u64,
        "operator metrics summary"
    );
}

/// Emit a tracing event for a progress frontier advance.
#[cfg(feature = "tracing")]
pub fn trace_progress_update(dataflow_name: &str, operator_index: usize, description: &str) {
    tracing::debug!(
        dataflow = dataflow_name,
        operator = operator_index,
        update = description,
        "progress frontier advanced"
    );
}

/// Emit a tracing event for a connection lifecycle event.
#[cfg(feature = "tracing")]
pub fn trace_connection_event(peer_id: usize, event: &str) {
    tracing::debug!(
        peer = peer_id,
        event = event,
        "connection pool event"
    );
}

/// No-op versions when tracing is disabled.
#[cfg(not(feature = "tracing"))]
pub fn trace_dataflow_started(_name: &str, _total_workers: usize) {}

#[cfg(not(feature = "tracing"))]
pub fn trace_dataflow_completed(_metrics: &DataflowMetrics) {}

#[cfg(not(feature = "tracing"))]
pub fn trace_dataflow_failed(_name: &str, _error: &crate::error::Error) {}

#[cfg(not(feature = "tracing"))]
pub fn trace_operator_summary(_dataflow_name: &str, _op: &OperatorMetrics) {}

#[cfg(not(feature = "tracing"))]
pub fn trace_progress_update(_dataflow_name: &str, _operator_index: usize, _description: &str) {}

#[cfg(not(feature = "tracing"))]
pub fn trace_connection_event(_peer_id: usize, _event: &str) {}

/// Emit all operator metrics summaries for a completed dataflow.
pub fn emit_completion_metrics(metrics: &Arc<DataflowMetrics>) {
    trace_dataflow_completed(metrics);
    for op in metrics.operator_snapshots() {
        trace_operator_summary(metrics.name(), &op);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_config_defaults() {
        let config = TracingConfig::default();
        assert!(!config.trace_activations);
        assert!(config.trace_progress);
        assert!(config.trace_connections);
        assert_eq!(config.min_activation_duration, Duration::ZERO);
    }

    #[test]
    fn tracing_config_builder() {
        let config = TracingConfig::default()
            .with_activation_tracing()
            .with_min_activation_duration(Duration::from_millis(10));
        assert!(config.trace_activations);
        assert_eq!(config.min_activation_duration, Duration::from_millis(10));
    }

    #[test]
    fn emit_completion_does_not_panic() {
        let mut metrics = DataflowMetrics::new("test");
        let op = metrics.register_operator("op1", 0);
        op.record_activation(Duration::from_micros(100), 50);
        let metrics = Arc::new(metrics);
        // Should not panic even without a tracing subscriber
        emit_completion_metrics(&metrics);
    }
}
