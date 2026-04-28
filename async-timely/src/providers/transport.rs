//! Transport provider trait and implementations.
//!
//! The [`TransportProvider`] resolves logical targets to physical delivery
//! mechanisms for data exchange between operators.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::region::RegionId;
use crate::error::Error;
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerId;

/// Identifies a logical destination for data delivery.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct LogicalTarget {
    /// The execution region containing the target operator.
    pub region: RegionId,
    /// The logical worker index within the region.
    pub worker: WorkerId,
    /// The operator index within the worker.
    pub operator: usize,
    /// The input slot on the target operator (e.g., 0 = left, 1 = right for binary).
    pub input_index: usize,
}

impl fmt::Display for LogicalTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Target(region={}, worker={}, op={}, slot={})",
            self.region.0, self.worker.index(), self.operator, self.input_index
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
        *self.closed.lock().unwrap()
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
            .field("buffer_len", &self.buffer.lock().unwrap().len())
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
        self.buffer.lock().unwrap().push_back(envelope);
        Ok(())
    }

    fn flush(&self) -> Result<(), Error> {
        Ok(()) // No buffering in local mode
    }

    fn close(&self) -> Result<(), Error> {
        *self.closed.lock().unwrap() = true;
        Ok(())
    }
}

/// Testing transport that simulates multi-node clusters entirely in-memory.
///
/// All nodes run in the same process, but the transport partitions data
/// as if they were on separate machines.
#[derive(Debug, Clone)]
pub struct InMemoryClusterTransport {
    /// Number of nodes in the simulated cluster.
    pub num_nodes: usize,
}

impl InMemoryClusterTransport {
    /// Create a new in-memory cluster transport.
    pub fn new(num_nodes: usize) -> Self {
        Self { num_nodes }
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
        // In a real implementation, this would route to the appropriate
        // simulated node's buffer. For now, use a simple in-memory buffer.
        Box::new(InMemoryPush::new())
    }

    fn is_local(&self, source: &LogicalTarget, target: &LogicalTarget) -> bool {
        // In cluster mode, locality is determined by region/worker mapping
        // For the in-memory cluster, everything is technically local
        // but we pretend remote targets need serialization
        source.region == target.region
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;

    fn make_target(region: usize, worker: usize, operator: usize, slot: usize) -> LogicalTarget {
        LogicalTarget {
            region: RegionId(region),
            worker: WorkerId::new(worker),
            operator,
            input_index: slot,
        }
    }

    #[test]
    fn logical_target_display() {
        let t = make_target(1, 3, 5, 0);
        let s = format!("{t}");
        assert!(s.contains("region=1"));
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

        let endpoint: Box<dyn PushEndpoint<u64, String, ()>> =
            transport.resolve(&src, &dst);

        let envelope = Envelope::data(42u64, vec!["hello".to_string()]);
        assert!(endpoint.push(envelope).is_ok());
    }

    #[test]
    fn in_memory_push_buffer() {
        let push: InMemoryPush<u64, i32, ()> = InMemoryPush::new();
        let buffer = push.buffer();

        push.push(Envelope::data(1, vec![10, 20])).unwrap();
        push.push(Envelope::data(2, vec![30])).unwrap();

        let buf = buffer.lock().unwrap();
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
        let transport = InMemoryClusterTransport::new(3);

        // Same region = local
        let src = make_target(0, 0, 0, 0);
        let dst_local = make_target(0, 1, 1, 0);
        assert!(transport.is_local(&src, &dst_local));

        // Different region = remote
        let dst_remote = make_target(1, 2, 0, 0);
        assert!(!transport.is_local(&src, &dst_remote));
    }

    #[test]
    fn push_endpoint_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InMemoryPush<u64, String, ()>>();
    }
}
