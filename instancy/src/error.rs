//! Error types for instancy.

/// The main error type for instancy operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A serialization or deserialization error.
    #[error("Serialization error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    /// A connection-level error.
    #[error("Connection error: target {target}: {source}")]
    Connection {
        /// Description of the target endpoint (e.g., node address or identifier).
        target: String,
        /// The underlying error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The dataflow was cancelled via its cancellation token.
    ///
    /// The optional [`crate::CancellationReason`] indicates why cancellation occurred.
    /// Use [`crate::CancellationReason`] variants to distinguish user-initiated
    /// cancellation from system-level causes (network errors, worker failures, etc.).
    #[error("Dataflow cancelled{}", reason.as_ref().map(|r| format!(": {r}")).unwrap_or_default())]
    Cancelled {
        /// The reason for cancellation, if one was provided.
        reason: Option<crate::cancellation::CancellationReason>,
    },

    /// An error in progress tracking.
    #[error("Progress tracking error: {0}")]
    Progress(String),

    /// An error produced by an operator.
    #[error("Operator error in '{operator}'{}: {source}",
        worker_index.map(|w| format!(" (worker {w})")).unwrap_or_default())]
    Operator {
        /// The name of the operator that failed.
        operator: String,
        /// The worker index where the error occurred, if known.
        worker_index: Option<usize>,
        /// The underlying error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The channel is full and cannot accept more data.
    /// The scheduler should re-queue the task and retry later.
    #[error("Channel backpressure: buffer is full")]
    Backpressure,

    /// The channel has been closed (sender or receiver disconnected).
    #[error("Channel closed")]
    ChannelClosed,

    /// An operator panicked during activation.
    ///
    /// This is only returned when `catch_panics` is enabled in `ExecutorConfig`.
    /// The panic payload is captured as a message string. After a panic, the
    /// operator is in an unknown state and the dataflow is terminated.
    ///
    /// Note: `panic = "abort"` builds will not reach this variant — the process
    /// exits before the panic can be caught.
    #[error("Operator '{operator}' panicked{}: {message}",
        worker_index.map(|w| format!(" (worker {w})")).unwrap_or_default())]
    OperatorPanic {
        /// The name of the operator that panicked.
        operator: String,
        /// The worker index where the panic occurred, if known.
        worker_index: Option<usize>,
        /// The panic message extracted from the payload.
        message: String,
    },

    /// A custom error message.
    #[error("{0}")]
    Custom(String),

    /// A configuration or topology error detected at build time.
    #[error("Configuration error: {0}")]
    InvalidConfig(String),

    /// A remote node was lost (disconnected or removed from cluster).
    /// Contains the node identity that departed.
    #[error("Node lost: node '{node_id}' departed ({reason})")]
    NodeLost {
        /// The node identity that was lost.
        node_id: String,
        /// Human-readable reason for the departure.
        reason: String,
    },
}

impl Error {
    /// Create a codec error from any error type.
    pub fn codec(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Codec(Box::new(err))
    }

    /// Create a connection error.
    pub fn connection(
        target: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Connection {
            target: target.into(),
            source: Box::new(err),
        }
    }

    /// Create an operator error.
    pub fn operator(
        name: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Operator {
            operator: name.into(),
            worker_index: None,
            source: Box::new(err),
        }
    }

    /// Create an operator error with worker context.
    pub fn operator_with_context(
        name: impl Into<String>,
        worker_index: usize,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Operator {
            operator: name.into(),
            worker_index: Some(worker_index),
            source: Box::new(err),
        }
    }

    /// Wrap an existing error with operator context.
    ///
    /// If `self` is already an `Operator` variant with a `worker_index`,
    /// returns it unchanged (preserves original context). If the existing
    /// `Operator` has `worker_index: None`, the worker index is backfilled.
    /// For all other error variants, wraps as a new `Operator` error.
    pub fn with_operator_context(self, operator: impl Into<String>, worker_index: usize) -> Self {
        match self {
            Error::Operator {
                operator: op_name,
                worker_index: None,
                source,
            } => Self::Operator {
                operator: op_name,
                worker_index: Some(worker_index),
                source,
            },
            Error::Operator { .. } => self,
            // Preserve OperatorPanic as-is (already has operator context).
            Error::OperatorPanic {
                operator: op_name,
                worker_index: None,
                message,
            } => Self::OperatorPanic {
                operator: op_name,
                worker_index: Some(worker_index),
                message,
            },
            Error::OperatorPanic { .. } => self,
            other => Self::Operator {
                operator: operator.into(),
                worker_index: Some(worker_index),
                source: Box::new(other),
            },
        }
    }
}

/// A convenience type alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Extension trait to convert `PoisonError` into `Error`.
pub(crate) trait LockResultExt<T> {
    /// Convert a poisoned lock result into an `Error::Custom`.
    fn or_poison(self, context: &str) -> std::result::Result<T, Error>;
}

impl<T> LockResultExt<T> for std::result::Result<T, std::sync::PoisonError<T>> {
    fn or_poison(self, context: &str) -> std::result::Result<T, Error> {
        self.map_err(|_| Error::Custom(format!("lock poisoned: {context}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: Error = io_err.into();
        assert!(err.to_string().contains("I/O error"));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn error_display_codec() {
        let err = Error::codec(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad bytes",
        ));
        assert!(err.to_string().contains("Serialization error"));
    }

    #[test]
    fn error_display_connection() {
        let err = Error::connection(
            "node-2:9090",
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        );
        let msg = err.to_string();
        assert!(msg.contains("node-2:9090"));
        assert!(msg.contains("refused"));
    }

    #[test]
    fn error_display_cancelled() {
        let err = Error::Cancelled { reason: None };
        assert_eq!(err.to_string(), "Dataflow cancelled");
    }

    #[test]
    fn error_display_progress() {
        let err = Error::Progress("stalled frontier".into());
        assert!(err.to_string().contains("stalled frontier"));
    }

    #[test]
    fn error_display_operator() {
        let err = Error::operator("my_filter", std::io::Error::other("oops"));
        let msg = err.to_string();
        assert!(msg.contains("my_filter"));
        assert!(msg.contains("oops"));
    }

    #[test]
    fn error_display_custom() {
        let err = Error::Custom("something went wrong".into());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn lock_result_ext_ok() {
        let mutex = std::sync::Mutex::new(42);
        let val = mutex.lock().or_poison("test").unwrap();
        assert_eq!(*val, 42);
    }

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }

    #[test]
    fn error_display_cancelled_with_reason() {
        use crate::cancellation::CancellationReason;
        let err = Error::Cancelled {
            reason: Some(CancellationReason::NetworkError("timeout".into())),
        };
        assert_eq!(
            err.to_string(),
            "Dataflow cancelled: network error: timeout"
        );
    }

    #[test]
    fn error_cancelled_matches_with_struct_pattern() {
        let err = Error::Cancelled { reason: None };
        assert!(matches!(err, Error::Cancelled { reason: None }));

        use crate::cancellation::CancellationReason;
        let err2 = Error::Cancelled {
            reason: Some(CancellationReason::UserRequested),
        };
        assert!(matches!(
            err2,
            Error::Cancelled {
                reason: Some(CancellationReason::UserRequested)
            }
        ));
    }

    #[test]
    fn error_operator_with_context() {
        let err =
            Error::operator_with_context("hash_join", 3, std::io::Error::other("key mismatch"));
        let msg = err.to_string();
        assert!(msg.contains("hash_join"), "should contain operator name");
        assert!(msg.contains("worker 3"), "should contain worker index");
        assert!(msg.contains("key mismatch"), "should contain source error");
    }

    #[test]
    fn error_with_operator_context_wraps_non_operator() {
        let err = Error::Custom("something failed".into());
        let wrapped = err.with_operator_context("my_op", 2);
        let msg = wrapped.to_string();
        assert!(msg.contains("my_op"), "should contain operator name");
        assert!(msg.contains("worker 2"), "should contain worker index");
        assert!(
            msg.contains("something failed"),
            "should contain original error"
        );
    }

    #[test]
    fn error_with_operator_context_preserves_existing_operator() {
        // Existing Operator with worker_index: None gets backfilled
        let err = Error::operator("original_op", std::io::Error::other("original cause"));
        let wrapped = err.with_operator_context("wrapper_op", 5);
        let msg = wrapped.to_string();
        assert!(
            msg.contains("original_op"),
            "should preserve original operator name"
        );
        assert!(
            !msg.contains("wrapper_op"),
            "should not overwrite with wrapper"
        );
        assert!(msg.contains("worker 5"), "should backfill worker index");
    }

    #[test]
    fn error_with_operator_context_preserves_existing_worker_index() {
        // Existing Operator with worker_index already set is fully preserved
        let err =
            Error::operator_with_context("original_op", 7, std::io::Error::other("original cause"));
        let wrapped = err.with_operator_context("wrapper_op", 99);
        let msg = wrapped.to_string();
        assert!(
            msg.contains("original_op"),
            "should preserve original operator"
        );
        assert!(
            msg.contains("worker 7"),
            "should keep original worker index"
        );
        assert!(
            !msg.contains("worker 99"),
            "should not overwrite worker index"
        );
    }

    #[test]
    fn error_operator_no_worker_index() {
        // Error::operator() without context should not show worker info
        let err = Error::operator("my_filter", std::io::Error::other("oops"));
        let msg = err.to_string();
        assert!(!msg.contains("worker"), "no worker info without context");
        assert!(msg.contains("my_filter"));
    }
}
