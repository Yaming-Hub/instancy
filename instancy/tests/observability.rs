//! Integration tests for observability + metrics wiring.
//!
//! Verifies that:
//! - ActivationGuard correctly times and reports metrics
//! - DataflowResult bundles result + metrics
//! - Tracing events are emitted at key lifecycle points
//! - End-to-end: multiple operators accumulate correct metrics

use std::sync::Arc;
use std::time::Duration;

use instancy::metrics::{
    DataflowMetrics, DataflowResult, OperatorMetricsCollector,
    activation::ActivationGuard,
    tracing_integration::{self, TracingConfig},
};

#[test]
fn dataflow_result_success() {
    let mut metrics = DataflowMetrics::new("success_df");
    let op = metrics.register_operator("source", 0);
    op.record_activation(Duration::from_millis(10), 100);
    let metrics = Arc::new(metrics);

    let result: DataflowResult<i32> = DataflowResult::new(Ok(42), metrics.clone());
    assert!(result.is_ok());
    assert_eq!(result.metrics().total_records_processed(), 100);
    assert_eq!(result.into_result().unwrap(), 42);
}

#[test]
fn dataflow_result_failure() {
    let metrics = Arc::new(DataflowMetrics::new("fail_df"));
    let result: DataflowResult<()> = DataflowResult::new(
        Err(instancy::error::RuntimeError::EmptyDataflow.into()),
        metrics.clone(),
    );
    assert!(!result.is_ok());
    assert!(result.into_result().is_err());
}

#[test]
fn end_to_end_multi_operator_metrics() {
    let mut metrics = DataflowMetrics::new("pipeline");

    let source = metrics.register_operator("source", 0);
    let map = metrics.register_operator("map", 1);
    let sink = metrics.register_operator("sink", 2);

    // Simulate activations using ActivationGuard
    for _ in 0..10 {
        let guard = ActivationGuard::new(source.clone());
        std::thread::sleep(Duration::from_micros(50));
        guard.finish(100);
    }

    for _ in 0..10 {
        let guard = ActivationGuard::new(map.clone());
        std::thread::sleep(Duration::from_micros(30));
        guard.finish(100);
    }

    for _ in 0..10 {
        let guard = ActivationGuard::new(sink.clone());
        std::thread::sleep(Duration::from_micros(20));
        guard.finish(100);
    }

    assert_eq!(metrics.total_activations(), 30);
    assert_eq!(metrics.total_records_processed(), 3000);
    assert!(metrics.total_cpu_time().as_micros() > 0);

    let snapshots = metrics.operator_snapshots();
    assert_eq!(snapshots.len(), 3);
    for snap in &snapshots {
        assert_eq!(snap.activations, 10);
        assert_eq!(snap.records_processed, 1000);
        assert!(snap.cpu_time.as_micros() > 0);
    }

    metrics.set_wall_time(Duration::from_millis(50));
    let metrics = Arc::new(metrics);
    let result: DataflowResult<()> = DataflowResult::new(Ok(()), metrics.clone());
    assert!(result.metrics().wall_time().as_millis() >= 50);
}

#[test]
fn backpressure_chain_metrics() {
    let mut metrics = DataflowMetrics::new("bp_pipeline");
    let op = metrics.register_operator("slow_consumer", 0);

    for i in 1..=5u64 {
        let guard = ActivationGuard::new(op.clone());
        guard.finish_with_backpressure(10, Duration::from_millis(i * 2));
    }

    let snapshot = op.snapshot();
    assert_eq!(snapshot.backpressure.blocked_count, 5);
    assert!(snapshot.backpressure.blocked_duration.as_millis() >= 30); // 2+4+6+8+10
    assert!(snapshot.backpressure.max_blocked_duration.as_millis() >= 10);
}

#[test]
fn tracing_config_controls() {
    let config = TracingConfig::default();
    assert!(!config.trace_activations);

    let config = config.with_activation_tracing();
    assert!(config.trace_activations);

    let config = config.with_min_activation_duration(Duration::from_millis(5));
    assert_eq!(config.min_activation_duration, Duration::from_millis(5));
}

#[test]
fn concurrent_metrics_accumulation() {
    let collector = Arc::new(instancy::metrics::OperatorMetricsCollector::new(
        "concurrent_op",
        0,
    ));
    let num_threads = 8;
    let activations_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let c = collector.clone();
            std::thread::spawn(move || {
                for _ in 0..activations_per_thread {
                    let guard = instancy::metrics::activation::ActivationGuard::new(c.clone());
                    guard.finish(10);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.activations, num_threads * activations_per_thread);
    assert_eq!(
        snapshot.records_processed,
        num_threads * activations_per_thread * 10
    );
    assert!(snapshot.cpu_time.as_nanos() > 0);
}

#[test]
fn concurrent_backpressure_max_tracking() {
    let collector = Arc::new(instancy::metrics::OperatorMetricsCollector::new(
        "bp_concurrent",
        0,
    ));
    let num_threads = 4;

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let c = collector.clone();
            std::thread::spawn(move || {
                for j in 0..50u64 {
                    c.record_backpressure(Duration::from_micros(i * 100 + j));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.backpressure.blocked_count, num_threads * 50);
    // Max should be at least (num_threads-1)*100 + 49 = 349 micros
    assert!(snapshot.backpressure.max_blocked_duration.as_micros() >= 349);
}

#[test]
fn dataflow_result_into_parts() {
    let mut metrics = DataflowMetrics::new("parts_test");
    metrics.register_operator("op", 0);
    let metrics = Arc::new(metrics);

    let result: DataflowResult<i32> = DataflowResult::new(Ok(99), metrics);
    let (res, m) = result.into_parts();
    assert_eq!(res.unwrap(), 99);
    assert_eq!(m.name(), "parts_test");
}

#[test]
fn emit_completion_metrics_does_not_panic() {
    let mut metrics = DataflowMetrics::new("emit_test");
    let op = metrics.register_operator("test_op", 0);
    op.record_activation(Duration::from_millis(1), 50);
    let metrics = Arc::new(metrics);
    tracing_integration::emit_completion_metrics(&metrics);
}

#[cfg(feature = "tracing")]
mod tracing_tests {
    use super::*;
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt;

    /// Serialize tracing tests — `with_default` sets a thread-local subscriber
    /// that can be overridden by another test running in parallel on the same
    /// thread, causing log capture to miss events.
    static TRACING_LOCK: Mutex<()> = Mutex::new(());

    /// Simple layer that captures formatted event messages.
    struct CaptureLayer {
        logs: Arc<Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = MessageVisitor(String::new());
            event.record(&mut visitor);
            self.logs.lock().unwrap().push(visitor.0);
        }
    }

    struct MessageVisitor(String);
    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            write!(self.0, "{}={:?} ", field.name(), value).ok();
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            use std::fmt::Write;
            write!(self.0, "{}={} ", field.name(), value).ok();
        }
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            use std::fmt::Write;
            write!(self.0, "{}={} ", field.name(), value).ok();
        }
    }

    fn with_captured_logs(f: impl FnOnce()) -> Vec<String> {
        let _guard = TRACING_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let logs = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureLayer { logs: logs.clone() };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, f);
        logs.lock().unwrap().clone()
    }

    #[test]
    fn trace_dataflow_lifecycle_events() {
        let logs = with_captured_logs(|| {
            tracing_integration::trace_dataflow_started("test_df", 4);

            let mut metrics = DataflowMetrics::new("test_df");
            metrics.register_operator("op1", 0);
            metrics.set_wall_time(Duration::from_millis(100));
            tracing_integration::trace_dataflow_completed(&metrics);
        });

        let all = logs.join(" ");
        assert!(all.contains("dataflow execution started"), "got: {all}");
        assert!(all.contains("dataflow execution completed"), "got: {all}");
    }

    #[test]
    fn trace_operator_activation() {
        let logs = with_captured_logs(|| {
            let collector = Arc::new(OperatorMetricsCollector::new("traced_op", 0));
            let guard = ActivationGuard::new(collector.clone());
            guard.finish(42);
        });

        let all = logs.join(" ");
        assert!(all.contains("operator activation"), "got: {all}");
    }

    #[test]
    fn trace_connection_event() {
        let logs = with_captured_logs(|| {
            tracing_integration::trace_connection_event(5, "established");
        });

        let all = logs.join(" ");
        assert!(all.contains("connection pool event"), "got: {all}");
    }

    #[test]
    fn trace_progress_update() {
        let logs = with_captured_logs(|| {
            tracing_integration::trace_progress_update("df1", 3, "frontier advanced to t=5");
        });

        let all = logs.join(" ");
        assert!(all.contains("progress frontier advanced"), "got: {all}");
    }
}
