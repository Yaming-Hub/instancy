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
    /// The optional [`CancellationReason`] indicates why cancellation occurred.
    /// Use [`CancellationReason`] variants to distinguish user-initiated
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
    #[error("Operator error in '{operator}': {source}")]
    Operator {
        /// The name of the operator that failed.
        operator: String,
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

    /// A custom error message.
    #[error("{0}")]
    Custom(String),

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
            source: Box::new(err),
        }
    }
}

/// A convenience type alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

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
        let err = Error::operator(
            "my_filter",
            std::io::Error::new(std::io::ErrorKind::Other, "oops"),
        );
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
        assert_eq!(err.to_string(), "Dataflow cancelled: network error: timeout");
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
}
