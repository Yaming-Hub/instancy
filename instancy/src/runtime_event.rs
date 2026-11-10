use std::fmt;

/// Events emitted by the runtime to notify the hosting application of
/// transport conditions.
///
/// The hosting application subscribes via [`crate::RuntimeHandle::health_events()`].
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// A pooled connection to a peer failed (Shared mode): an I/O error, the
    /// peer closing it while dataflows used it, or the liveness rule (see
    /// [`SharedConnectionConfig::silence_timeout`](crate::communication::SharedConnectionConfig::silence_timeout)).
    /// The dataflows using it have failed, each with its own error; the pool
    /// replaces it when the next dataflow registers.
    ///
    /// **No action is needed**, and restarting the runtime would fail every
    /// running dataflow. It never means a node left: that is the
    /// application's to report
    /// ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
    TransportDegraded {
        /// The peer node of the failed connection.
        peer_id: String,
        /// Human-readable description of what went wrong.
        detail: String,
    },
}

impl fmt::Display for RuntimeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransportDegraded { peer_id, detail } => {
                write!(f, "transport degraded for peer '{peer_id}': {detail}")
            }
        }
    }
}
