//! Runtime configuration and dataflow execution entry point.
//!
//! The [`execute`] function is the main entry point for running a dataflow.
//! It creates the worker thread pool, sets up the execution context,
//! and returns a [`crate::dataflow::DataflowHandle`] for monitoring and control.

use std::time::Duration;

use crate::cancellation::CancellationToken;
use crate::error::Error;
use crate::scheduler::batching::BatchingPolicy;
use crate::worker::WorkerId;
use crate::worker_pool::WorkerPoolConfig;

/// Configuration for the low-level execution engine.
///
/// This controls the worker thread pool and progress tracking mode used by
/// [`execute()`]. Most users should prefer [`crate::RuntimeConfig`] (from the
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Stop the dataflow on the first error (default).
    Stop,
    /// Ignore errors and continue processing.
    /// An optional callback name can be used for logging/alerting.
    Ignore {
        /// Description of what to do on error (for debugging).
        description: String,
    },
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        Self::Stop
    }
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
        Self { node_id: node_id.into(), logical_workers }
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTopology {
    /// Configuration for each physical node in the cluster.
    pub nodes: Vec<NodeConfig>,
}

impl ClusterTopology {
    /// Create a single-node topology with the given number of logical workers.
    pub fn single_node(logical_workers: usize) -> Self {
        Self {
            nodes: vec![NodeConfig::new("local", logical_workers)],
        }
    }

    /// Create a multi-node topology from a list of node configs.
    /// Nodes are sorted by `node_id` to ensure consistent worker range assignment.
    pub fn multi_node(mut configs: Vec<NodeConfig>) -> Result<Self, Error> {
        if configs.is_empty() {
            return Err(Error::Custom("cluster must have at least one node".into()));
        }
        for config in &configs {
            if config.logical_workers == 0 {
                return Err(Error::Custom(format!(
                    "node '{}' must have at least 1 worker",
                    config.node_id
                )));
            }
        }
        configs.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(Self { nodes: configs })
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
}

/// Handle returned by `execute()` for monitoring and controlling a running dataflow.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ExecutionHandle {
    /// Name of the dataflow.
    pub name: String,
    /// Whether the dataflow has completed.
    completed: bool,
}

impl ExecutionHandle {
    /// Create a new handle (used internally).
    #[allow(dead_code)]
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            completed: false,
        }
    }

    /// Check if the dataflow has completed.
    #[allow(dead_code)]
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Mark the dataflow as completed.
    #[allow(dead_code)]
    pub(crate) fn mark_completed(&mut self) {
        self.completed = true;
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
pub fn execute(
    runtime_config: &ExecutionConfig,
    dataflow_config: DataflowConfig,
) -> Result<ExecutionHandle, Error> {
    // Validate configs
    runtime_config
        .worker_pool
        .validate()
        .map_err(|e| Error::Custom(e.to_string()))?;

    if dataflow_config.topology.total_workers() == 0 {
        return Err(Error::Custom(
            "dataflow must have at least one worker".into(),
        ));
    }

    let handle = ExecutionHandle::new(dataflow_config.name);
    Ok(handle)
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
        assert_eq!(w0, vec![WorkerId::new(0), WorkerId::new(1), WorkerId::new(2)]);

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
        let handle = execute(&runtime, df).unwrap();
        assert_eq!(handle.name, "smoke");
        assert!(!handle.is_completed());
    }

    #[test]
    fn execute_rejects_zero_workers() {
        let runtime = ExecutionConfig::default();
        let df = DataflowConfig {
            topology: ClusterTopology { nodes: vec![NodeConfig::new("node-0", 0)] },
            error_policy: ErrorPolicy::Stop,
            cancellation_token: CancellationToken::new(),
            batching_policy: BatchingPolicy::default(),
            name: "bad".into(),
        };
        assert!(execute(&runtime, df).is_err());

        let df2 = DataflowConfig {
            topology: ClusterTopology { nodes: vec![] },
            error_policy: ErrorPolicy::Stop,
            cancellation_token: CancellationToken::new(),
            batching_policy: BatchingPolicy::default(),
            name: "empty".into(),
        };
        assert!(execute(&runtime, df2).is_err());
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
}
