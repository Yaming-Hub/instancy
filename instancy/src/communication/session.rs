//! Per-dataflow session for channel allocation and wiring.
//!
//! A [`DataflowSession`] is created for each running dataflow. It owns the
//! dataflow's [`DataflowId`], manages channel ID allocation, and coordinates
//! local vs remote channel wiring. This keeps per-dataflow state separate from
//! the global [`TransportProvider`](crate::providers::transport::TransportProvider).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::communication::interprocess::ChannelId;
use crate::dataflow::id::DataflowId;
use crate::execute::ClusterTopology;
use crate::worker::WorkerId;

/// Per-dataflow session managing channel allocation and wiring state.
///
/// Created once per dataflow execution. Holds the [`DataflowId`] and tracks
/// which channels exist, which workers they connect, and whether they are
/// local (in-process) or remote (cross-process).
#[derive(Debug)]
pub struct DataflowSession {
    /// Cluster-unique dataflow identifier.
    dataflow_id: DataflowId,
    /// Cluster topology for local/remote decisions.
    topology: ClusterTopology,
    /// This node's identity in the cluster.
    local_node_id: String,
    /// Next channel ID to allocate (starts at 1; 0 is reserved for progress).
    next_channel_id: AtomicU64,
    /// Registry of allocated channels and their wiring metadata.
    channels: Mutex<HashMap<ChannelId, ChannelInfo>>,
}

/// Metadata about an allocated channel.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// The allocated channel ID.
    pub channel_id: ChannelId,
    /// Source worker (who sends data on this channel).
    pub source_worker: WorkerId,
    /// Target worker (who receives data on this channel).
    pub target_worker: WorkerId,
    /// Whether source and target are on the same node.
    pub is_local: bool,
}

impl DataflowSession {
    /// Create a new session for a dataflow.
    ///
    /// # Arguments
    ///
    /// * `dataflow_id` — Cluster-unique ID for this dataflow
    /// * `topology` — The cluster topology (for local/remote decisions)
    /// * `local_node_id` — This process's node identity
    pub fn new(
        dataflow_id: DataflowId,
        topology: ClusterTopology,
        local_node_id: impl Into<String>,
    ) -> Self {
        Self {
            dataflow_id,
            topology,
            local_node_id: local_node_id.into(),
            next_channel_id: AtomicU64::new(1), // 0 reserved for progress
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Get this session's DataflowId.
    pub fn dataflow_id(&self) -> DataflowId {
        self.dataflow_id
    }

    /// Get the cluster topology.
    pub fn topology(&self) -> &ClusterTopology {
        &self.topology
    }

    /// Get the local node identity.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Allocate a new channel between two workers.
    ///
    /// Returns the [`ChannelInfo`] describing the allocated channel.
    /// The channel ID is unique within this dataflow.
    pub fn allocate_channel(
        &self,
        source_worker: WorkerId,
        target_worker: WorkerId,
    ) -> ChannelInfo {
        let channel_id = self.next_channel_id.fetch_add(1, Ordering::Relaxed);

        let source_node = self.topology.node_for_worker(source_worker);
        let target_node = self.topology.node_for_worker(target_worker);
        let is_local = source_node == target_node;

        let info = ChannelInfo {
            channel_id,
            source_worker,
            target_worker,
            is_local,
        };

        self.channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(channel_id, info.clone());
        info
    }

    /// Check if a worker is local to this node.
    pub fn is_worker_local(&self, worker: WorkerId) -> bool {
        self.topology.node_for_worker(worker) == Some(self.local_node_id.as_str())
    }

    /// Get all allocated channels.
    pub fn channels(&self) -> Vec<ChannelInfo> {
        self.channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Get all remote channels (cross-node).
    pub fn remote_channels(&self) -> Vec<ChannelInfo> {
        self.channels
            .lock()
            .unwrap()
            .values()
            .filter(|c| !c.is_local)
            .cloned()
            .collect()
    }

    /// Get a specific channel's info.
    pub fn channel_info(&self, channel_id: ChannelId) -> Option<ChannelInfo> {
        self.channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&channel_id)
            .cloned()
    }

    /// Get the DataflowId as bytes (for wire protocol frames).
    pub fn dataflow_id_bytes(&self) -> &[u8; 16] {
        self.dataflow_id.as_bytes()
    }
}

/// Builder for creating a [`DataflowSession`] with proper topology integration.
pub struct DataflowSessionBuilder {
    topology: ClusterTopology,
    local_node_id: String,
}

impl DataflowSessionBuilder {
    /// Create a builder for the given topology and local node.
    pub fn new(topology: ClusterTopology, local_node_id: impl Into<String>) -> Self {
        Self {
            topology,
            local_node_id: local_node_id.into(),
        }
    }

    /// Build a session using the provided DataflowId.
    pub fn build(self, dataflow_id: DataflowId) -> DataflowSession {
        DataflowSession::new(dataflow_id, self.topology, self.local_node_id)
    }
}

/// Shared reference to a DataflowSession (thread-safe).
pub type SharedSession = Arc<DataflowSession>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::{ClusterTopology, NodeConfig};

    fn two_node_topology() -> ClusterTopology {
        ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 2), // node 0: workers 0, 1
            NodeConfig::new("node-1", 2), // node 1: workers 2, 3
        ])
        .unwrap()
    }

    #[test]
    fn session_allocates_channels_starting_at_one() {
        let topo = two_node_topology();
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        let ch1 = session.allocate_channel(WorkerId::new(0), WorkerId::new(1));
        let ch2 = session.allocate_channel(WorkerId::new(0), WorkerId::new(2));

        assert_eq!(ch1.channel_id, 1);
        assert_eq!(ch2.channel_id, 2);
    }

    #[test]
    fn session_detects_local_channels() {
        let topo = two_node_topology();
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        // Both workers on node 0
        let local = session.allocate_channel(WorkerId::new(0), WorkerId::new(1));
        assert!(local.is_local);

        // Workers on different nodes
        let remote = session.allocate_channel(WorkerId::new(0), WorkerId::new(2));
        assert!(!remote.is_local);
    }

    #[test]
    fn session_is_worker_local() {
        let topo = two_node_topology();
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        assert!(session.is_worker_local(WorkerId::new(0)));
        assert!(session.is_worker_local(WorkerId::new(1)));
        assert!(!session.is_worker_local(WorkerId::new(2)));
        assert!(!session.is_worker_local(WorkerId::new(3)));
    }

    #[test]
    fn session_channels_list() {
        let topo = two_node_topology();
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        session.allocate_channel(WorkerId::new(0), WorkerId::new(1));
        session.allocate_channel(WorkerId::new(0), WorkerId::new(2));
        session.allocate_channel(WorkerId::new(1), WorkerId::new(3));

        assert_eq!(session.channels().len(), 3);
        assert_eq!(session.remote_channels().len(), 2);
    }

    #[test]
    fn session_channel_info_lookup() {
        let topo = two_node_topology();
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        let ch = session.allocate_channel(WorkerId::new(0), WorkerId::new(2));
        let info = session.channel_info(ch.channel_id).unwrap();
        assert_eq!(info.source_worker, WorkerId::new(0));
        assert_eq!(info.target_worker, WorkerId::new(2));
        assert!(!info.is_local);

        assert!(session.channel_info(999).is_none());
    }

    #[test]
    fn session_dataflow_id_raw() {
        let id = DataflowId::from_bytes([1u8; 16]);
        let topo = ClusterTopology::single_node(2);
        let session = DataflowSession::new(id, topo, "local");
        assert_eq!(*session.dataflow_id_bytes(), *id.as_bytes());
    }

    #[test]
    fn session_builder() {
        let topo = two_node_topology();
        let builder = DataflowSessionBuilder::new(topo, "node-0");
        let id = DataflowId::from_bytes([42u8; 16]);
        let session = builder.build(id);
        assert_eq!(session.dataflow_id(), id);
        assert_eq!(session.local_node_id(), "node-0");
    }

    #[test]
    fn single_node_all_local() {
        let topo = ClusterTopology::single_node(4);
        let session = DataflowSession::new(DataflowId::from_bytes([1u8; 16]), topo, "node-0");

        for i in 0..4 {
            for j in 0..4 {
                let ch = session.allocate_channel(WorkerId::new(i), WorkerId::new(j));
                assert!(ch.is_local, "workers {i}->{j} should be local");
            }
        }
    }
}
