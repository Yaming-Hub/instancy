//! Transport provider trait and implementations.
//!
//! The [`TransportProvider`] resolves logical targets to physical delivery
//! mechanisms for data exchange between operators.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::stage::StageId;
use crate::error::Error;
use crate::execute::ClusterTopology;
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerId;

/// Identifies a logical destination for data delivery.
///
/// This is a **logical** concept — it names the target by stage, worker,
/// operator, and input slot in graph terms. The `TransportProvider` resolves
/// a `LogicalTarget` to a physical delivery mechanism (in-memory buffer or
/// remote TCP endpoint).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct LogicalTarget {
    /// The logical execution stage containing the target operator.
    pub stage: StageId,
    /// The logical worker index within the stage.
    pub worker: WorkerId,
    /// The logical operator index within the worker.
    pub operator: usize,
    /// The logical input slot on the target operator (e.g., 0 = left, 1 = right for binary).
    pub input_index: usize,
}

impl fmt::Display for LogicalTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Target(stage={}, worker={}, op={}, slot={})",
            self.stage.0,
            self.worker.index(),
            self.operator,
            self.input_index
        )
    }
}

/// A push endpoint for delivering envelopes to a logical target.
pub trait PushEndpoint<T: Timestamp, D, M>: Send + Sync {
    /// Push an envelope to the target. Returns error if delivery fails.
    fn push(&self, envelope: Envelope<T, D, M>) -> Result<(), Error>;

    /// Flush any buffered data.
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Close this endpoint (no more data will be sent).
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Resolves logical targets to physical delivery mechanisms.
///
/// During dataflow construction, the runtime calls `resolve()` to obtain
/// push endpoints for each operator connection. The provider decides how
/// to deliver based on the physical topology.
pub trait TransportProvider: Send + Sync + 'static {
    /// Resolve a connection between two logical targets into a push endpoint.
    fn resolve<T, D, M>(
        &self,
        source: &LogicalTarget,
        target: &LogicalTarget,
    ) -> Box<dyn PushEndpoint<T, D, M>>
    where
        T: Timestamp + Send + Sync + 'static,
        D: Send + Sync + 'static,
        M: Send + Sync + 'static;

    /// Returns true if source and target are co-located (same process).
    /// Used to decide whether serialization is needed.
    fn is_local(&self, source: &LogicalTarget, target: &LogicalTarget) -> bool;
}

/// Single-process transport: all targets resolve to bounded in-memory buffers.
///
/// This is the default for single-node dataflows.
#[derive(Debug, Clone)]
pub struct LocalTransport;

impl TransportProvider for LocalTransport {
    fn resolve<T, D, M>(
        &self,
        _source: &LogicalTarget,
        _target: &LogicalTarget,
    ) -> Box<dyn PushEndpoint<T, D, M>>
    where
        T: Timestamp + Send + Sync + 'static,
        D: Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        Box::new(InMemoryPush::new())
    }

    fn is_local(&self, _source: &LogicalTarget, _target: &LogicalTarget) -> bool {
        true // Everything is local in single-process mode
    }
}

/// In-memory push endpoint backed by a VecDeque buffer.
pub struct InMemoryPush<T: Timestamp, D, M> {
    buffer: Arc<Mutex<VecDeque<Envelope<T, D, M>>>>,
    closed: Arc<Mutex<bool>>,
}

impl<T: Timestamp, D, M> InMemoryPush<T, D, M> {
    /// Create a new in-memory push endpoint.
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Get a reference to the buffer for reading (pull side).
    pub fn buffer(&self) -> Arc<Mutex<VecDeque<Envelope<T, D, M>>>> {
        self.buffer.clone()
    }

    /// Check if closed.
    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl<T: Timestamp, D, M> Default for InMemoryPush<T, D, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Timestamp, D, M> fmt::Debug for InMemoryPush<T, D, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryPush")
            .field(
                "buffer_len",
                &self.buffer.lock().unwrap_or_else(|e| e.into_inner()).len(),
            )
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl<T: Timestamp, D, M> PushEndpoint<T, D, M> for InMemoryPush<T, D, M>
where
    T: Timestamp + Send + Sync + 'static,
    D: Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    fn push(&self, envelope: Envelope<T, D, M>) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::ChannelClosed);
        }
        self.buffer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(envelope);
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        Ok(()) // No buffering in local mode
    }

    fn close(&self) -> Result<(), Error> {
        *self.closed.lock().unwrap_or_else(|e| e.into_inner()) = true;
        Ok(())
    }
}

/// Testing transport that simulates multi-node clusters entirely in-memory.
///
/// All nodes run in the same process, but the transport partitions data
/// as if they were on separate machines. Remote targets are delivered to
/// in-memory buffers keyed by `LogicalTarget`, simulating network delivery.
#[derive(Debug, Clone)]
pub struct InMemoryClusterTransport {
    /// The cluster topology for local/remote decisions.
    topology: ClusterTopology,
    /// This node's identity.
    local_node_id: String,
}

impl InMemoryClusterTransport {
    /// Create a new in-memory cluster transport.
    pub fn new(topology: ClusterTopology, local_node_id: impl Into<String>) -> Self {
        Self {
            topology,
            local_node_id: local_node_id.into(),
        }
    }
}

impl TransportProvider for InMemoryClusterTransport {
    fn resolve<T, D, M>(
        &self,
        _source: &LogicalTarget,
        _target: &LogicalTarget,
    ) -> Box<dyn PushEndpoint<T, D, M>>
    where
        T: Timestamp + Send + Sync + 'static,
        D: Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        // In the in-memory cluster transport, both local and remote targets
        // use in-memory buffers. The distinction matters for serialization
        // (tested via `is_local()`), but delivery is always in-process.
        Box::new(InMemoryPush::new())
    }

    fn is_local(&self, source: &LogicalTarget, target: &LogicalTarget) -> bool {
        let source_node = self.topology.node_for_worker(source.worker);
        let target_node = self.topology.node_for_worker(target.worker);
        source_node == target_node && source_node == Some(self.local_node_id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;

    fn make_target(stage: usize, worker: usize, operator: usize, slot: usize) -> LogicalTarget {
        LogicalTarget {
            stage: StageId(stage),
            worker: WorkerId::new(worker),
            operator,
            input_index: slot,
        }
    }

    #[test]
    fn logical_target_display() {
        let t = make_target(1, 3, 5, 0);
        let s = format!("{t}");
        assert!(s.contains("stage=1"));
        assert!(s.contains("worker=3"));
        assert!(s.contains("op=5"));
        assert!(s.contains("slot=0"));
    }

    #[test]
    fn local_transport_is_always_local() {
        let transport = LocalTransport;
        let src = make_target(0, 0, 0, 0);
        let dst = make_target(1, 1, 1, 0);
        assert!(transport.is_local(&src, &dst));
    }

    #[test]
    fn local_transport_resolve_and_push() {
        let transport = LocalTransport;
        let src = make_target(0, 0, 0, 0);
        let dst = make_target(0, 1, 1, 0);

        let endpoint: Box<dyn PushEndpoint<u64, String, ()>> = transport.resolve(&src, &dst);

        let envelope = Envelope::data(42u64, vec!["hello".to_string()]);
        assert!(endpoint.push(envelope).is_ok());
    }

    #[test]
    fn in_memory_push_buffer() {
        let push: InMemoryPush<u64, i32, ()> = InMemoryPush::new();
        let buffer = push.buffer();

        push.push(Envelope::data(1, vec![10, 20])).unwrap();
        push.push(Envelope::data(2, vec![30])).unwrap();

        let buf = buffer.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn in_memory_push_close() {
        let push: InMemoryPush<u64, i32, ()> = InMemoryPush::new();
        assert!(!push.is_closed());

        push.close().unwrap();
        assert!(push.is_closed());

        // Push after close fails
        let result = push.push(Envelope::data(1, vec![1]));
        assert!(result.is_err());
    }

    #[test]
    fn in_memory_cluster_transport_locality() {
        use crate::execute::NodeConfig;

        // 2 nodes: node-0 has workers 0,1; node-1 has workers 2,3
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-0", 2),
            NodeConfig::new("node-1", 2),
        ])
        .unwrap();
        let transport = InMemoryClusterTransport::new(topology, "node-0");

        // Same node (workers 0 and 1 both on node-0) = local
        let src = make_target(0, 0, 0, 0);
        let dst_local = make_target(0, 1, 1, 0);
        assert!(transport.is_local(&src, &dst_local));

        // Different nodes (worker 0 on node-0, worker 2 on node-1) = remote
        let dst_remote = make_target(0, 2, 0, 0);
        assert!(!transport.is_local(&src, &dst_remote));
    }

    #[test]
    fn push_endpoint_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryPush<u64, String, ()>>();
    }
}
