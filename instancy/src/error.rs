//! Error types for instancy.
//!
//! # Error Organization
//!
//! Errors are organized by **source module**:
//!
//! - [`TopologyError`] — cluster topology management (from `execute` module).
//! - [`DataflowError`] — dataflow construction and wiring (from `dataflow` module).
//! - [`RuntimeError`] — runtime lifecycle and cluster setup (from `runtime` module).
//! - [`CommunicationError`] — serialization and wire protocol (from `communication` module).
//!
//! Cross-cutting errors that occur in many modules stay as root [`Error`] variants
//! (e.g., [`Error::LockPoisoned`], [`Error::ChannelClosed`]).
//!
//! **Adding new errors**: put them in the sub-enum of the module that produces them.
//! Only add a root variant if the error genuinely occurs in 3+ unrelated modules.

/// The main error type for instancy operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The dataflow was cancelled via its cancellation token.
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
    #[error("Channel backpressure: buffer is full")]
    Backpressure,

    /// The channel has been closed (sender or receiver disconnected).
    #[error("Channel closed")]
    ChannelClosed,

    /// An operator panicked during activation.
    ///
    /// Only returned when `catch_panics` is enabled in `ExecutorConfig`.
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

    // ── Cross-cutting (occurs in many modules) ─────────────────────────
    /// A mutex or RwLock was poisoned because another thread panicked
    /// while holding it.
    #[error("Lock poisoned: {context}")]
    LockPoisoned {
        /// Which lock was poisoned.
        context: String,
    },

    // ── Module sub-enums ───────────────────────────────────────────────
    /// Cluster topology errors (from `execute` / topology module).
    #[error(transparent)]
    Topology(#[from] TopologyError),

    /// Dataflow construction and wiring errors (from `dataflow` module).
    #[error(transparent)]
    Dataflow(#[from] DataflowError),

    /// Runtime lifecycle and cluster setup errors (from `runtime` module).
    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    /// Communication / serialization errors (from `communication` module).
    #[error(transparent)]
    Communication(#[from] CommunicationError),
}

impl Error {
    /// Create a codec error from any error type.
    pub fn codec(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        CommunicationError::Codec(Box::new(err)).into()
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

/// Errors related to cluster topology management.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    /// A node with the given ID already exists in the topology.
    #[error("node '{node_id}' already exists in topology")]
    NodeAlreadyExists { node_id: String },

    /// A node with the given ID was not found in the topology.
    #[error("node '{node_id}' not found in topology")]
    NodeNotFound { node_id: String },

    /// The topology would be empty after this operation.
    #[error("empty topology: {reason}")]
    EmptyTopology { reason: String },

    /// A node configuration is invalid.
    #[error("invalid node config for '{node_id}': {reason}")]
    InvalidNodeConfig { node_id: String, reason: String },
}

/// Errors raised during dataflow construction, graph validation, or channel wiring.
#[derive(Debug, thiserror::Error)]
pub enum DataflowError {
    #[error("invalid dataflow config: {0}")]
    InvalidConfig(String),

    #[error("invalid dataflow graph: {0}")]
    InvalidGraph(String),

    #[error("missing endpoint for operator '{operator}', port '{port}'")]
    MissingEndpoint { operator: String, port: String },

    #[error("type mismatch for operator '{operator}', port '{port}'")]
    TypeMismatch { operator: String, port: String },

    #[error("endpoint already taken: {0}")]
    EndpointTaken(String),

    #[error("missing factory for edge index {edge_index}")]
    MissingFactory { edge_index: usize },
}

/// Errors raised by runtime lifecycle and cluster setup code.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid runtime config: {0}")]
    InvalidConfig(String),

    #[error("spawn failed: {0}")]
    SpawnFailed(String),

    #[error("cluster setup failed: {0}")]
    ClusterSetup(String),

    #[error("resource already consumed: {resource}")]
    AlreadyConsumed { resource: String },

    #[error("empty dataflow")]
    EmptyDataflow,
}

/// Errors raised by communication / serialization code.
#[derive(Debug, thiserror::Error)]
pub enum CommunicationError {
    #[error("Serialization error: {0}")]
    Codec(Box<dyn std::error::Error + Send + Sync>),

    #[cfg(feature = "transport")]
    #[error("Control protocol error: {0}")]
    Protocol(#[from] crate::communication::control_protocol::ControlProtocolError),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Setup error: {0}")]
    InvalidSetup(String),
}

/// Extension trait to convert `PoisonError` into `Error`.
pub(crate) trait LockResultExt<T> {
    /// Convert a poisoned lock result into an [`Error::LockPoisoned`].
    fn or_poison(self, context: &str) -> std::result::Result<T, Error>;
}

impl<T> LockResultExt<T> for std::result::Result<T, std::sync::PoisonError<T>> {
    fn or_poison(self, context: &str) -> std::result::Result<T, Error> {
        self.map_err(|_| Error::LockPoisoned {
            context: context.into(),
        })
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
        assert!(
            err.to_string().contains("serialization error")
                || err.to_string().contains("Serialization error")
        );
    }

    #[test]
    fn error_display_topology() {
        let err: Error = TopologyError::NodeAlreadyExists {
            node_id: "node-1".into(),
        }
        .into();
        assert!(err.to_string().contains("node-1"));
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn error_display_dataflow() {
        let err: Error = DataflowError::MissingEndpoint {
            operator: "my_op".into(),
            port: "input".into(),
        }
        .into();
        let msg = err.to_string();
        assert!(msg.contains("my_op"));
        assert!(msg.contains("input"));
    }

    #[test]
    fn error_display_runtime() {
        let err: Error = RuntimeError::EmptyDataflow.into();
        assert!(err.to_string().contains("empty dataflow"));
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
    fn error_display_lock_poisoned() {
        let err = Error::LockPoisoned {
            context: "test mutex".into(),
        };
        assert!(err.to_string().contains("Lock poisoned"));
        assert!(err.to_string().contains("test mutex"));
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
            reason: Some(CancellationReason::NetworkError {
                detail: "timeout".into(),
            }),
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
        let err: Error = DataflowError::InvalidConfig("something failed".into()).into();
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

    #[test]
    fn topology_error_matches() {
        let err: Error = TopologyError::NodeNotFound {
            node_id: "x".into(),
        }
        .into();
        assert!(matches!(
            err,
            Error::Topology(TopologyError::NodeNotFound { .. })
        ));
    }

    #[test]
    fn sub_enums_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TopologyError>();
        assert_send_sync::<DataflowError>();
        assert_send_sync::<RuntimeError>();
        assert_send_sync::<CommunicationError>();
    }
}
