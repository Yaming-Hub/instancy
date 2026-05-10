//! Runtime configuration and dataflow execution types.
//!
//! Provides [`ClusterTopology`], [`NodeConfig`], [`DataflowConfig`], and
//! [`ExecutionConfig`] for configuring multi-node dataflow clusters.
//!
//! # Dynamic Membership
//!
//! [`ClusterTopology`] can optionally hold a [`ClusterMembership`] provider
//! that feeds node join/leave events to the runtime. When a topology with
//! membership is passed to [`RuntimeConfig`](crate::RuntimeConfig), the
//! runtime automatically starts a background listener that keeps the
//! topology up to date.

use std::fmt;
use std::time::Duration;

use crate::cancellation::CancellationToken;
use crate::error::{Error, TopologyError};
use crate::scheduler::batching::BatchingPolicy;
use crate::worker::WorkerId;
use crate::worker_pool::WorkerPoolConfig;

/// Configuration for the low-level execution engine.
///
/// This controls the worker thread pool and progress tracking mode.
/// Most users should prefer [`crate::RuntimeConfig`] (from the
/// `runtime` module) which provides a higher-level API.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Worker thread pool configuration.
    pub worker_pool: WorkerPoolConfig,
    /// Progress tracking mode.
    pub progress_mode: ProgressMode,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            worker_pool: WorkerPoolConfig::default(),
            progress_mode: ProgressMode::Eager,
        }
    }
}

/// How progress information is exchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressMode {
    /// Progress is computed and exchanged after every operator activation.
    Eager,
    /// Progress is batched and exchanged periodically.
    Batched {
        /// Maximum delay before flushing progress updates.
        max_delay: Duration,
    },
}

/// Configuration for a single dataflow execution.
#[derive(Debug, Clone)]
pub struct DataflowConfig {
    /// The cluster topology for this dataflow.
    pub topology: ClusterTopology,
    /// Error handling policy.
    pub error_policy: ErrorPolicy,
    /// Cancellation token for graceful shutdown.
    /// Cancel this token to request the dataflow to stop.
    pub cancellation_token: CancellationToken,
    /// Message batching policy for operator activations.
    /// Controls how messages are coalesced before dispatching.
    pub batching_policy: BatchingPolicy,
    /// Human-readable name for this dataflow (for metrics/logging).
    pub name: String,
}

impl DataflowConfig {
    /// Create a single-node dataflow config with the given number of workers.
    pub fn single_node(workers: usize, name: impl Into<String>) -> Self {
        Self {
            topology: ClusterTopology::single_node(workers),
            error_policy: ErrorPolicy::default(),
            cancellation_token: CancellationToken::new(),
            batching_policy: BatchingPolicy::default(),
            name: name.into(),
        }
    }
}

/// Per-dataflow error handling policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ErrorPolicy {
    /// Stop the dataflow on the first error (default).
    #[default]
    Stop,
    /// Ignore errors and continue processing.
    /// An optional callback name can be used for logging/alerting.
    Ignore {
        /// Description of what to do on error (for debugging).
        description: String,
    },
}

/// Configuration for a physical node (OS process) in the cluster.
///
/// This is a **physical** concept — each `NodeConfig` corresponds to one
/// OS process. A node hosts one or more logical workers. The `node_id`
/// identifies the process within the physical cluster topology (typically
/// an IP:port or hostname that is stable across reconnections).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    /// Physical node identity — a stable string identifying this OS process
    /// (e.g., "192.168.1.5:8080", a hostname, or a pod name).
    pub node_id: String,
    /// Number of logical workers hosted by this physical node.
    pub logical_workers: usize,
}

impl NodeConfig {
    /// Create a new node config.
    pub fn new(node_id: impl Into<String>, logical_workers: usize) -> Self {
        Self {
            node_id: node_id.into(),
            logical_workers,
        }
    }
}

/// Physical cluster topology: describes all nodes and their logical worker assignments.
///
/// This is a **physical** layout — it describes how many OS processes exist
/// and how logical workers are distributed across them. The runtime uses this
/// to determine which workers are local vs remote and to assign global worker indices.
///
/// **Cardinality**: One per process (updated on membership changes).
/// **Lifetime**: Process lifetime (mutable via membership events).
pub struct ClusterTopology {
    /// Configuration for each physical node in the cluster.
    pub nodes: Vec<NodeConfig>,
    /// Optional dynamic membership provider.
    membership: Option<Box<dyn ClusterMembership>>,
}

impl std::fmt::Debug for ClusterTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTopology")
            .field("nodes", &self.nodes)
            .field("has_membership", &self.membership.is_some())
            .finish()
    }
}

impl Clone for ClusterTopology {
    /// Clone the topology (without the membership provider).
    ///
    /// Membership providers are one-shot and cannot be cloned — the clone
    /// contains only the static node list.
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            membership: None,
        }
    }
}

impl PartialEq for ClusterTopology {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes
    }
}

impl Eq for ClusterTopology {}

impl ClusterTopology {
    /// Create a single-node topology with the given number of logical workers.
    pub fn single_node(logical_workers: usize) -> Self {
        Self {
            nodes: vec![NodeConfig::new("local", logical_workers)],
            membership: None,
        }
    }

    /// Create a multi-node topology from a list of node configs.
    /// Nodes are sorted by `node_id` to ensure consistent worker range assignment.
    pub fn multi_node(mut configs: Vec<NodeConfig>) -> Result<Self, Error> {
        if configs.is_empty() {
            return Err(Error::Topology(TopologyError::EmptyTopology {
                reason: "cluster must have at least one node".into(),
            }));
        }
        for config in &configs {
            if config.logical_workers == 0 {
                return Err(Error::Topology(TopologyError::InvalidNodeConfig {
                    node_id: config.node_id.clone(),
                    reason: "must have at least 1 worker".into(),
                }));
            }
        }
        configs.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Self {
            nodes: configs,
            membership: None,
        })
    }

    /// Total number of logical workers across all physical nodes.
    pub fn total_workers(&self) -> usize {
        self.nodes.iter().map(|n| n.logical_workers).sum()
    }

    /// Get the range of global logical worker IDs for a given physical node.
    ///
    /// Returns `(start, end)` where logical workers are `start..end`.
    pub fn worker_range(&self, node_id: &str) -> Option<(usize, usize)> {
        let mut offset = 0;
        for node in &self.nodes {
            if node.node_id == node_id {
                return Some((offset, offset + node.logical_workers));
            }
            offset += node.logical_workers;
        }
        None
    }

    /// Determine which physical node a logical worker belongs to.
    /// Returns the node_id of the node hosting the given worker.
    pub fn node_for_worker(&self, worker_id: WorkerId) -> Option<&str> {
        let mut offset = 0;
        for node in &self.nodes {
            if worker_id.index() < offset + node.logical_workers {
                return Some(&node.node_id);
            }
            offset += node.logical_workers;
        }
        None
    }

    /// Get all worker IDs for a specific node.
    pub fn workers_for_node(&self, node_id: &str) -> Vec<WorkerId> {
        if let Some((start, end)) = self.worker_range(node_id) {
            (start..end).map(WorkerId::new).collect()
        } else {
            Vec::new()
        }
    }

    /// Check whether a node with the given ID is in the topology.
    pub fn contains_node(&self, node_id: &str) -> bool {
        self.nodes.iter().any(|n| n.node_id == node_id)
    }

    /// Add a node to the topology.
    ///
    /// The node list is re-sorted after insertion to maintain consistent
    /// worker range assignment. Returns an error if the node already exists
    /// or has zero workers.
    ///
    /// **Note**: Adding a node does not affect already-running dataflows.
    /// Only subsequent `spawn_cluster` calls will include the new node.
    pub fn add_node(&mut self, config: NodeConfig) -> Result<(), Error> {
        if config.logical_workers == 0 {
            return Err(Error::Topology(TopologyError::InvalidNodeConfig {
                node_id: config.node_id.clone(),
                reason: "must have at least 1 worker".into(),
            }));
        }
        if self.contains_node(&config.node_id) {
            return Err(Error::Topology(TopologyError::NodeAlreadyExists {
                node_id: config.node_id.clone(),
            }));
        }
        self.nodes.push(config);
        self.nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(())
    }

    /// Remove a node from the topology by its ID.
    ///
    /// Returns the removed [`NodeConfig`], or an error if the node was not found
    /// or removal would leave the topology empty.
    ///
    /// **Note**: Removing a node does not cancel already-running dataflows.
    /// Use [`RuntimeHandle::report_node_leave`] to cancel affected dataflows.
    pub fn remove_node(&mut self, node_id: &str) -> Result<NodeConfig, Error> {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.node_id == node_id)
            .ok_or_else(|| {
                Error::Topology(TopologyError::NodeNotFound {
                    node_id: node_id.into(),
                })
            })?;
        if self.nodes.len() == 1 {
            return Err(Error::Topology(TopologyError::EmptyTopology {
                reason: "cannot remove the last node from topology".into(),
            }));
        }
        Ok(self.nodes.remove(idx))
    }

    /// Returns the number of nodes in the topology.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Attach a dynamic membership provider to this topology.
    ///
    /// When this topology is passed to [`RuntimeConfig`](crate::RuntimeConfig),
    /// the runtime will start a background listener that processes
    /// [`MembershipEvent`]s and keeps the topology up to date.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use instancy::{ClusterTopology, NodeConfig};
    /// use instancy::execute::{ChannelMembership, MembershipEvent};
    ///
    /// let membership = ChannelMembership::new();
    /// let tx = membership.sender();
    ///
    /// let topology = ClusterTopology::single_node(4)
    ///     .with_membership(membership);
    /// ```
    pub fn with_membership(mut self, provider: impl ClusterMembership) -> Self {
        self.membership = Some(Box::new(provider));
        self
    }

    /// Take the membership provider, if one was attached.
    ///
    /// Returns `Some` on the first call; subsequent calls return `None`.
    /// This is called by the runtime during construction.
    pub(crate) fn take_membership(&mut self) -> Option<Box<dyn ClusterMembership>> {
        self.membership.take()
    }

    /// Returns `true` if a membership provider is attached.
    pub fn has_membership(&self) -> bool {
        self.membership.is_some()
    }
}

// ── Dynamic Membership ─────────────────────────────────────────────────

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
    /// 2. Add the node to the live [`ClusterTopology`].
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
    /// 2. Remove the node from the live [`ClusterTopology`].
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
/// The runtime subscribes to the membership event stream when a topology
/// with membership is provided via [`RuntimeConfig`](crate::RuntimeConfig).
/// The application is free to use any discovery mechanism: Kubernetes watch,
/// Consul, ZooKeeper, gossip protocol, or manual operator commands.
///
/// # Contract
///
/// - The stream may produce events at any time; the runtime processes them
///   asynchronously.
/// - If the stream ends (returns `None`), the runtime continues operating
///   with the last known topology. No error is raised.
/// - The implementation must be `Send + Sync + 'static` so the runtime can
///   subscribe from any task.
pub trait ClusterMembership: Send + Sync + 'static {
    /// Take the membership event receiver.
    ///
    /// Returns `Some(receiver)` on the first call; subsequent calls return `None`
    /// (the runtime takes ownership of the receiver). This design avoids requiring
    /// `Stream` trait imports while maintaining a clean async interface.
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
/// use instancy::execute::{ChannelMembership, MembershipEvent};
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
        self.rx.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

/// Bootstrap a dataflow execution.
///
/// This is the main entry point. It:
/// 1. Validates configuration
/// 2. Creates the worker thread pool  
/// 3. Sets up the execution context
/// 4. Returns a handle for monitoring/control
///
/// The actual dataflow construction happens via the returned handle
/// and the scope/stream APIs.
#[cfg(test)]
fn execute(
    runtime_config: &ExecutionConfig,
    dataflow_config: DataflowConfig,
) -> Result<String, Error> {
    // Validate configs
    runtime_config
        .worker_pool
        .validate()
        .map_err(Error::Runtime)?;

    if dataflow_config.topology.total_workers() == 0 {
        return Err(Error::Runtime(crate::error::RuntimeError::InvalidConfig(
            "dataflow must have at least one worker".into(),
        )));
    }

    Ok(dataflow_config.name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_topology_single_node() {
        let topo = ClusterTopology::single_node(4);
        assert_eq!(topo.total_workers(), 4);
        assert_eq!(topo.worker_range("local"), Some((0, 4)));
        assert_eq!(topo.worker_range("other"), None);
    }

    #[test]
    fn cluster_topology_multi_node() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 4),
            NodeConfig::new("node-1", 2),
            NodeConfig::new("node-2", 6),
        ])
        .unwrap();

        assert_eq!(topo.total_workers(), 12);
        assert_eq!(topo.worker_range("node-0"), Some((0, 4)));
        assert_eq!(topo.worker_range("node-1"), Some((4, 6)));
        assert_eq!(topo.worker_range("node-2"), Some((6, 12)));
    }

    #[test]
    fn cluster_topology_node_for_worker() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 4),
            NodeConfig::new("node-1", 2),
            NodeConfig::new("node-2", 6),
        ])
        .unwrap();

        assert_eq!(topo.node_for_worker(WorkerId::new(0)), Some("node-0"));
        assert_eq!(topo.node_for_worker(WorkerId::new(3)), Some("node-0"));
        assert_eq!(topo.node_for_worker(WorkerId::new(4)), Some("node-1"));
        assert_eq!(topo.node_for_worker(WorkerId::new(5)), Some("node-1"));
        assert_eq!(topo.node_for_worker(WorkerId::new(6)), Some("node-2"));
        assert_eq!(topo.node_for_worker(WorkerId::new(11)), Some("node-2"));
        assert_eq!(topo.node_for_worker(WorkerId::new(12)), None);
    }

    #[test]
    fn cluster_topology_workers_for_node() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 3),
            NodeConfig::new("node-1", 2),
        ])
        .unwrap();

        let w0 = topo.workers_for_node("node-0");
        assert_eq!(
            w0,
            vec![WorkerId::new(0), WorkerId::new(1), WorkerId::new(2)]
        );

        let w1 = topo.workers_for_node("node-1");
        assert_eq!(w1, vec![WorkerId::new(3), WorkerId::new(4)]);

        let w_none = topo.workers_for_node("node-5");
        assert!(w_none.is_empty());
    }

    #[test]
    fn cluster_topology_heterogeneous() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 4),
            NodeConfig::new("node-1", 1),
            NodeConfig::new("node-2", 8),
        ])
        .unwrap();
        assert_eq!(topo.total_workers(), 13);
    }

    #[test]
    fn cluster_topology_validation() {
        // Empty cluster
        assert!(ClusterTopology::multi_node(vec![]).is_err());
        // Zero workers
        assert!(ClusterTopology::multi_node(vec![NodeConfig::new("node-0", 0)]).is_err());
    }

    #[test]
    fn cluster_topology_contains_node() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 2),
            NodeConfig::new("node-1", 3),
        ])
        .unwrap();
        assert!(topo.contains_node("node-0"));
        assert!(topo.contains_node("node-1"));
        assert!(!topo.contains_node("node-2"));
    }

    #[test]
    fn cluster_topology_add_node() {
        let mut topo = ClusterTopology::single_node(2);
        assert_eq!(topo.node_count(), 1);
        assert_eq!(topo.total_workers(), 2);

        // Add a new node
        topo.add_node(NodeConfig::new("node-b", 3)).unwrap();
        assert_eq!(topo.node_count(), 2);
        assert_eq!(topo.total_workers(), 5);
        assert!(topo.contains_node("node-b"));

        // Worker ranges are recalculated (sorted by node_id)
        // "local" < "node-b" alphabetically
        assert_eq!(topo.worker_range("local"), Some((0, 2)));
        assert_eq!(topo.worker_range("node-b"), Some((2, 5)));
    }

    #[test]
    fn cluster_topology_add_node_duplicate() {
        let mut topo = ClusterTopology::single_node(2);
        let result = topo.add_node(NodeConfig::new("local", 3));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn cluster_topology_add_node_zero_workers() {
        let mut topo = ClusterTopology::single_node(2);
        let result = topo.add_node(NodeConfig::new("node-b", 0));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least 1 worker")
        );
    }

    #[test]
    fn cluster_topology_remove_node() {
        let mut topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 2),
            NodeConfig::new("node-b", 3),
            NodeConfig::new("node-c", 4),
        ])
        .unwrap();

        let removed = topo.remove_node("node-b").unwrap();
        assert_eq!(removed.node_id, "node-b");
        assert_eq!(removed.logical_workers, 3);
        assert_eq!(topo.node_count(), 2);
        assert_eq!(topo.total_workers(), 6);
        assert!(!topo.contains_node("node-b"));

        // Worker ranges recalculated
        assert_eq!(topo.worker_range("node-a"), Some((0, 2)));
        assert_eq!(topo.worker_range("node-c"), Some((2, 6)));
    }

    #[test]
    fn cluster_topology_remove_node_not_found() {
        let mut topo = ClusterTopology::single_node(2);
        let result = topo.remove_node("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn cluster_topology_remove_last_node() {
        let mut topo = ClusterTopology::single_node(2);
        let result = topo.remove_node("local");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("last node"));
    }

    #[test]
    fn cluster_topology_node_count() {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("a", 1),
            NodeConfig::new("b", 1),
            NodeConfig::new("c", 1),
        ])
        .unwrap();
        assert_eq!(topo.node_count(), 3);
    }

    #[test]
    fn error_policy_default_is_stop() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::Stop);
    }

    #[test]
    fn error_policy_ignore_variant() {
        let policy = ErrorPolicy::Ignore {
            description: "log and continue".into(),
        };
        match policy {
            ErrorPolicy::Ignore { description } => {
                assert_eq!(description, "log and continue");
            }
            _ => panic!("expected Ignore"),
        }
    }

    #[test]
    fn execution_config_default() {
        let config = ExecutionConfig::default();
        assert_eq!(config.progress_mode, ProgressMode::Eager);
        assert!(config.worker_pool.validate().is_ok());
    }

    #[test]
    fn dataflow_config_single_node() {
        let config = DataflowConfig::single_node(4, "test_df");
        assert_eq!(config.name, "test_df");
        assert_eq!(config.topology.total_workers(), 4);
        assert_eq!(config.error_policy, ErrorPolicy::Stop);
        assert!(!config.cancellation_token.is_cancelled());
    }

    #[test]
    fn dataflow_config_cancellation_token() {
        let config = DataflowConfig::single_node(4, "cancelable");
        let token = config.cancellation_token.clone();

        assert!(!token.is_cancelled());
        token.cancel();
        assert!(config.cancellation_token.is_cancelled());
    }

    #[test]
    fn execute_smoke_test() {
        let runtime = ExecutionConfig::default();
        let df = DataflowConfig::single_node(4, "smoke");
        let name = execute(&runtime, df).unwrap();
        assert_eq!(name, "smoke");
    }

    #[test]
    fn execute_rejects_zero_workers() {
        let runtime = ExecutionConfig::default();
        // single_node doesn't validate worker count, so we can create 0-worker topologies
        let df = DataflowConfig {
            topology: ClusterTopology::single_node(0),
            error_policy: ErrorPolicy::Stop,
            cancellation_token: CancellationToken::new(),
            batching_policy: BatchingPolicy::default(),
            name: "bad".into(),
        };
        assert!(execute(&runtime, df).is_err());
    }

    #[test]
    fn execute_rejects_bad_pool_config() {
        let runtime = ExecutionConfig {
            worker_pool: WorkerPoolConfig {
                min_threads: 0,
                ..WorkerPoolConfig::default()
            },
            progress_mode: ProgressMode::Eager,
        };
        let df = DataflowConfig::single_node(4, "bad_pool");
        assert!(execute(&runtime, df).is_err());
    }

    #[test]
    fn progress_mode_batched() {
        let mode = ProgressMode::Batched {
            max_delay: Duration::from_millis(100),
        };
        match mode {
            ProgressMode::Batched { max_delay } => {
                assert_eq!(max_delay, Duration::from_millis(100));
            }
            _ => panic!("expected Batched"),
        }
    }

    // ── Membership type tests (moved from membership.rs) ──

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
        assert!(
            matches!(e1, MembershipEvent::NodeJoined { ref node_id, .. } if node_id == "node-1")
        );

        let e2 = rx.recv().await.unwrap();
        assert!(matches!(e2, MembershipEvent::NodeLeft { ref node_id, .. } if node_id == "node-2"));
    }

    #[test]
    fn channel_membership_default() {
        let membership = ChannelMembership::default();
        assert!(membership.events().is_some());
    }

    #[test]
    fn cluster_topology_with_membership() {
        let membership = ChannelMembership::new();
        let topo = ClusterTopology::single_node(2).with_membership(membership);
        assert!(topo.has_membership());
    }

    #[test]
    fn cluster_topology_take_membership() {
        let membership = ChannelMembership::new();
        let mut topo = ClusterTopology::single_node(2).with_membership(membership);
        assert!(topo.has_membership());

        let taken = topo.take_membership();
        assert!(taken.is_some());
        assert!(!topo.has_membership());

        // Second take returns None
        assert!(topo.take_membership().is_none());
    }
}
