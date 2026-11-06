use std::fmt;

/// Events emitted by the runtime to notify the hosting application of
/// transport conditions.
///
/// The hosting application subscribes via [`crate::RuntimeHandle::health_events()`].
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// A pooled connection to a peer failed (Shared mode).
    ///
    /// Emitted for every failure of a connection in a
    /// [`SharedPeerManager`](crate::communication::SharedPeerManager)'s pool:
    /// a read or write error, the peer closing the connection while dataflows
    /// were using it, or the liveness rule finding that the connection
    /// stopped delivering (see
    /// [`SharedConnectionConfig::silence_timeout`](crate::communication::SharedConnectionConfig::silence_timeout)).
    /// The dataflows that used the connection have failed, each with its own
    /// error; no other dataflow is affected, and the pool replaces the
    /// connection when the next dataflow registers.
    ///
    /// **No action is needed**: the runtime keeps working. Do not restart it
    /// on this event, which would fail every running dataflow. Use it for
    /// monitoring; re-run failed dataflows (with a fresh
    /// [`DataflowId`](crate::DataflowId)) from their own results. It never
    /// means a node left: that is the application's to report
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
