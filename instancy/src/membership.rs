//! Dynamic cluster membership for runtime topology updates.
//!
//! The hosting application implements [`ClusterMembership`] to provide a stream
//! of [`MembershipEvent`]s describing node joins and departures. The runtime
//! subscribes to this stream and automatically updates its internal topology,
//! connection state, and active dataflows.
//!
//! # Design
//!
//! - **Node joins** update the live [`ClusterTopology`](crate::ClusterTopology)
//!   so that subsequent `spawn_cluster` calls include the new node. Already-running
//!   dataflows are **not** affected — they continue with their original topology.
//! - **Node departures** cancel all dataflows with workers on the departed node
//!   (via [`CancellationReason::PeerDown`](crate::CancellationReason::PeerDown))
//!   and remove the node from the live topology.
//! - The membership provider is **optional** — applications can still use the
//!   imperative [`RuntimeHandle::report_node_join`] / [`report_node_leave`]
//!   API directly.
//!
//! # Example
//!
//! ```ignore
//! use instancy::membership::{ClusterMembership, MembershipEvent, ChannelMembership};
//!
//! // Simple channel-based provider:
//! let membership = ChannelMembership::new();
//! let tx = membership.sender();
//!
//! // Or implement the trait for a custom discovery system:
//! struct K8sMembership { /* ... */ }
//! impl ClusterMembership for K8sMembership {
//!     fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>> {
//!         // Return a receiver fed by a Kubernetes pod watcher
//!         unimplemented!()
//!     }
//! }
//! ```

use std::fmt;

/// Events describing changes to the physical cluster topology.
///
/// The hosting application produces these events; the runtime consumes them
/// to update its internal topology, routing tables, and connection state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEvent {
    /// A new physical node has joined the cluster and is ready to host logical workers.
    ///
    /// The runtime will:
    /// 1. Clear any "left" state for this node (if it was previously departed).
    /// 2. Add the node to the live [`ClusterTopology`](crate::ClusterTopology).
    /// 3. Future `spawn_cluster` calls will include this node automatically.
    ///
    /// Already-running dataflows are **not** affected.
    NodeJoined {
        /// Physical node identity (e.g., hostname, IP:port, pod name).
        node_id: String,
        /// Number of logical workers the node will host.
        logical_workers: usize,
    },

    /// A physical node has left the cluster (graceful shutdown or detected failure).
    ///
    /// The runtime will:
    /// 1. Cancel all dataflows with workers on this node.
    /// 2. Remove the node from the live [`ClusterTopology`](crate::ClusterTopology).
    /// 3. Mark the node as "left" — future `spawn_cluster` calls that include
    ///    this node will be immediately cancelled.
    NodeLeft {
        /// Physical node identity.
        node_id: String,
        /// Why the node departed.
        reason: NodeDepartureReason,
    },
}

impl fmt::Display for MembershipEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeJoined {
                node_id,
                logical_workers,
            } => write!(f, "NodeJoined({node_id}, {logical_workers} workers)"),
            Self::NodeLeft { node_id, reason } => {
                write!(f, "NodeLeft({node_id}, {reason})")
            }
        }
    }
}

/// Why a node departed from the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDepartureReason {
    /// Graceful shutdown — the node drained its work before leaving.
    Graceful,
    /// Connection lost or health check failed — unexpected departure.
    ConnectionLost,
    /// Application-initiated removal (e.g., scale-down decision).
    Removed,
}

impl fmt::Display for NodeDepartureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Graceful => write!(f, "graceful"),
            Self::ConnectionLost => write!(f, "connection_lost"),
            Self::Removed => write!(f, "removed"),
        }
    }
}

/// Application-implemented trait for providing cluster membership changes.
///
/// The runtime subscribes to the membership event stream at startup (or when
/// [`RuntimeHandle::set_membership`] is called). The application is free to use
/// any discovery mechanism: Kubernetes watch, Consul, ZooKeeper, gossip protocol,
/// or manual operator commands.
///
/// # Contract
///
/// - The stream may produce events at any time; the runtime processes them
///   asynchronously.
/// - If the stream ends (returns `None`), the runtime continues operating
///   with the last known topology. No error is raised.
/// - The implementation must be `Send + Sync + 'static` so the runtime can
///   subscribe from any task.
///
/// # Example
///
/// ```ignore
/// use instancy::membership::*;
/// use tokio::sync::mpsc;
///
/// /// Channel-based membership provider for testing or simple deployments.
/// pub struct ChannelMembership {
///     tx: mpsc::UnboundedSender<MembershipEvent>,
///     rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<MembershipEvent>>>,
/// }
///
/// impl ChannelMembership {
///     pub fn new() -> Self {
///         let (tx, rx) = mpsc::unbounded_channel();
///         Self { tx, rx: std::sync::Mutex::new(Some(rx)) }
///     }
///
///     pub fn sender(&self) -> mpsc::UnboundedSender<MembershipEvent> {
///         self.tx.clone()
///     }
/// }
///
/// impl ClusterMembership for ChannelMembership {
///     fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>> {
///         self.rx.lock().unwrap().take()
///     }
/// }
/// ```
pub trait ClusterMembership: Send + Sync + 'static {
    /// Take the membership event receiver.
    ///
    /// Returns `Some(receiver)` on the first call; subsequent calls return `None`
    /// (the runtime takes ownership of the receiver). This design avoids requiring
    /// `Stream` trait imports while maintaining a clean async interface.
    ///
    /// The runtime will poll this receiver in a background task to process
    /// membership changes.
    fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>>;
}

/// A simple channel-based [`ClusterMembership`] implementation.
///
/// Create with [`ChannelMembership::new()`], then send events via the
/// [`sender()`](ChannelMembership::sender). Useful for testing, manual
/// administration, or bridging from external event sources.
///
/// # Example
///
/// ```
/// use instancy::membership::{ChannelMembership, MembershipEvent};
///
/// let membership = ChannelMembership::new();
/// let tx = membership.sender();
///
/// // Send a join event (would typically come from service discovery)
/// tx.send(MembershipEvent::NodeJoined {
///     node_id: "node-2".into(),
///     logical_workers: 4,
/// }).unwrap();
/// ```
pub struct ChannelMembership {
    tx: tokio::sync::mpsc::UnboundedSender<MembershipEvent>,
    rx: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>>>,
}

impl ChannelMembership {
    /// Create a new channel-based membership provider.
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx: std::sync::Mutex::new(Some(rx)),
        }
    }

    /// Get a sender for injecting membership events.
    ///
    /// The sender can be cloned and shared across threads.
    pub fn sender(&self) -> tokio::sync::mpsc::UnboundedSender<MembershipEvent> {
        self.tx.clone()
    }
}

impl Default for ChannelMembership {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterMembership for ChannelMembership {
    fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>> {
        self.rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_event_display() {
        let join = MembershipEvent::NodeJoined {
            node_id: "node-1".into(),
            logical_workers: 4,
        };
        assert_eq!(join.to_string(), "NodeJoined(node-1, 4 workers)");

        let leave = MembershipEvent::NodeLeft {
            node_id: "node-2".into(),
            reason: NodeDepartureReason::ConnectionLost,
        };
        assert_eq!(leave.to_string(), "NodeLeft(node-2, connection_lost)");
    }

    #[test]
    fn departure_reason_display() {
        assert_eq!(NodeDepartureReason::Graceful.to_string(), "graceful");
        assert_eq!(
            NodeDepartureReason::ConnectionLost.to_string(),
            "connection_lost"
        );
        assert_eq!(NodeDepartureReason::Removed.to_string(), "removed");
    }

    #[test]
    fn membership_event_clone_eq() {
        let event = MembershipEvent::NodeJoined {
            node_id: "node-1".into(),
            logical_workers: 2,
        };
        assert_eq!(event, event.clone());

        let event2 = MembershipEvent::NodeLeft {
            node_id: "node-1".into(),
            reason: NodeDepartureReason::Graceful,
        };
        assert_ne!(
            std::mem::discriminant(&event),
            std::mem::discriminant(&event2)
        );
    }

    #[test]
    fn channel_membership_basic() {
        let membership = ChannelMembership::new();
        let tx = membership.sender();

        // First call returns receiver
        let rx = membership.events();
        assert!(rx.is_some());

        // Second call returns None (already taken)
        assert!(membership.events().is_none());

        // Can still send
        tx.send(MembershipEvent::NodeJoined {
            node_id: "n1".into(),
            logical_workers: 1,
        })
        .unwrap();
    }

    #[tokio::test]
    async fn channel_membership_receive() {
        let membership = ChannelMembership::new();
        let tx = membership.sender();
        let mut rx = membership.events().unwrap();

        tx.send(MembershipEvent::NodeJoined {
            node_id: "node-1".into(),
            logical_workers: 4,
        })
        .unwrap();

        tx.send(MembershipEvent::NodeLeft {
            node_id: "node-2".into(),
            reason: NodeDepartureReason::Removed,
        })
        .unwrap();

        let e1 = rx.recv().await.unwrap();
        assert!(matches!(e1, MembershipEvent::NodeJoined { ref node_id, .. } if node_id == "node-1"));

        let e2 = rx.recv().await.unwrap();
        assert!(
            matches!(e2, MembershipEvent::NodeLeft { ref node_id, .. } if node_id == "node-2")
        );
    }

    #[test]
    fn channel_membership_default() {
        let membership = ChannelMembership::default();
        assert!(membership.events().is_some());
    }
}
