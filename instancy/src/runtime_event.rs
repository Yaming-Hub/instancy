use std::fmt;

/// Events emitted by the runtime to notify the hosting application of
/// conditions that may require intervention.
///
/// The hosting application subscribes via [`crate::RuntimeHandle::health_events()`]
/// and monitors for events that indicate the runtime is degraded beyond
/// self-recovery.
#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    /// A shared transport component is permanently degraded.
    ///
    /// This typically occurs when a background thread panics while holding a
    /// lock on shared transport state (e.g., `PeerPool`). The runtime cannot
    /// self-recover from this condition — future dataflows using this peer
    /// will fail.
    ///
    /// **Recommended action:** Shut down the runtime via
    /// [`RuntimeHandle::shutdown()`](crate::RuntimeHandle::shutdown) and
    /// create a fresh [`RuntimeHandle`](crate::RuntimeHandle).
    TransportDegraded {
        /// The peer node whose transport is degraded.
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
