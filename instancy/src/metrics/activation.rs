//! Activation guard for timing operator executions.
//!
//! The [`ActivationGuard`] wraps an operator activation, automatically measuring
//! CPU time and reporting it to the operator's metrics collector on drop.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::OperatorMetricsCollector;

/// RAII guard that measures operator activation CPU time.
///
/// Created before executing an operator's logic and dropped after completion.
/// On drop, reports the elapsed time and records processed to the associated
/// [`OperatorMetricsCollector`].
///
/// # Example
///
/// ```rust,ignore
/// let guard = ActivationGuard::new(collector.clone());
/// // ... execute operator logic ...
/// guard.finish(records_processed);
/// ```
pub struct ActivationGuard {
    collector: Arc<OperatorMetricsCollector>,
    start: Instant,
    finished: bool,
}

impl ActivationGuard {
    /// Create a new activation guard that starts timing immediately.
    pub fn new(collector: Arc<OperatorMetricsCollector>) -> Self {
        #[cfg(feature = "tracing")]
        tracing::trace!(operator = collector.name(), "operator activation started");

        Self {
            collector,
            start: Instant::now(),
            finished: false,
        }
    }

    /// Finish the activation, reporting the elapsed time and records processed.
    ///
    /// This is the preferred way to end an activation since it allows specifying
    /// the record count. If the guard is simply dropped, `records_processed` is 0.
    pub fn finish(mut self, records_processed: u64) {
        let elapsed = self.start.elapsed();
        self.collector.record_activation(elapsed, records_processed);
        self.finished = true;

        #[cfg(feature = "tracing")]
        tracing::trace!(
            operator = self.collector.name(),
            elapsed_us = elapsed.as_micros() as u64,
            records = records_processed,
            "operator activation completed"
        );
    }

    /// Finish the activation and additionally record a backpressure event.
    ///
    /// Use when the operator was blocked waiting for downstream capacity.
    pub fn finish_with_backpressure(mut self, records_processed: u64, blocked_duration: Duration) {
        let elapsed = self.start.elapsed();
        self.collector.record_activation(elapsed, records_processed);
        self.collector.record_backpressure(blocked_duration);
        self.finished = true;

        #[cfg(feature = "tracing")]
        tracing::warn!(
            operator = self.collector.name(),
            elapsed_us = elapsed.as_micros() as u64,
            records = records_processed,
            blocked_us = blocked_duration.as_micros() as u64,
            "operator activation completed with backpressure"
        );
    }

    /// Get the elapsed time since this activation started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for ActivationGuard {
    fn drop(&mut self) {
        if !self.finished {
            // Fallback: if finish() was not called (e.g., panic unwind),
            // still record the activation with 0 records.
            let elapsed = self.start.elapsed();
            self.collector.record_activation(elapsed, 0);

            #[cfg(feature = "tracing")]
            tracing::warn!(
                operator = self.collector.name(),
                elapsed_us = elapsed.as_micros() as u64,
                "operator activation dropped without finish (possible panic)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_guard_finish_reports_metrics() {
        let collector = Arc::new(OperatorMetricsCollector::new("test_op", 0));

        let guard = ActivationGuard::new(collector.clone());
        std::thread::sleep(Duration::from_millis(1));
        guard.finish(42);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.activations, 1);
        assert!(snapshot.cpu_time.as_nanos() > 0);
        assert_eq!(snapshot.records_processed, 42);
    }

    #[test]
    fn activation_guard_drop_reports_zero_records() {
        let collector = Arc::new(OperatorMetricsCollector::new("drop_op", 1));

        {
            let _guard = ActivationGuard::new(collector.clone());
            std::thread::sleep(Duration::from_millis(1));
            // guard dropped without calling finish()
        }

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.activations, 1);
        assert!(snapshot.cpu_time.as_nanos() > 0);
        assert_eq!(snapshot.records_processed, 0);
    }

    #[test]
    fn activation_guard_backpressure() {
        let collector = Arc::new(OperatorMetricsCollector::new("bp_op", 2));

        let guard = ActivationGuard::new(collector.clone());
        guard.finish_with_backpressure(10, Duration::from_millis(5));

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.activations, 1);
        assert_eq!(snapshot.records_processed, 10);
        assert_eq!(snapshot.backpressure.blocked_count, 1);
        assert!(snapshot.backpressure.blocked_duration.as_millis() >= 5);
        assert!(snapshot.backpressure.max_blocked_duration.as_millis() >= 5);
    }

    #[test]
    fn multiple_activations_accumulate() {
        let collector = Arc::new(OperatorMetricsCollector::new("multi", 0));

        for i in 0..5u64 {
            let guard = ActivationGuard::new(collector.clone());
            guard.finish(i * 10);
        }

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.activations, 5);
        assert_eq!(snapshot.records_processed, 0 + 10 + 20 + 30 + 40);
    }

    #[test]
    fn elapsed_during_activation() {
        let collector = Arc::new(OperatorMetricsCollector::new("elapsed", 0));
        let guard = ActivationGuard::new(collector.clone());
        std::thread::sleep(Duration::from_millis(1));
        assert!(guard.elapsed().as_nanos() > 0);
        guard.finish(0);
    }
}
