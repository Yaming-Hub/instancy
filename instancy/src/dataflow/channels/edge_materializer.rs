//! Edge materializer — pluggable physical transport for exchange edges.
//!
//! An [`EdgeMaterializer`] produces the raw push/pull endpoint pairs that
//! connect source workers to target workers for a single exchange edge.
//! The exchange routing logic ([`ExchangePush`]/[`ExchangePull`]) wraps
//! these endpoints — the materializer only provides the underlying transport.
//!
//! This is the key abstraction for **logical/physical separation**: the
//! dataflow layer declares exchange edges; the runtime chooses the physical
//! transport per worker pair based on cluster topology.
//!
//! # Implementations
//!
//! - [`LocalEdgeMaterializer`]: In-process bounded channels (wraps
//!   [`ExchangeChannelSet`]). Used for single-node or when all workers share
//!   a process.
//! - Future: `MockNetworkEdgeMaterializer` — serialize/deserialize through
//!   [`Codec`] over in-memory channels (for distributed testing without a
//!   real network).
//! - Future: `NetworkEdgeMaterializer` — real TCP/QUIC transport via
//!   [`ConnectionPool`] and [`Muxer`]/[`Demuxer`].
//!
//! [`ExchangePush`]: super::exchange_channel::ExchangePush
//! [`ExchangePull`]: super::exchange_channel::ExchangePull
//! [`ExchangeChannelSet`]: super::exchange_channel::ExchangeChannelSet

use crate::error::Result;
use crate::progress::timestamp::Timestamp;

use super::exchange_channel::ExchangeChannelSet;
use super::pushpull::{Pull, Push};

// ---------------------------------------------------------------------------
// EdgeMaterializer trait
// ---------------------------------------------------------------------------

/// Produces raw push/pull endpoint pairs for one exchange edge.
///
/// An edge materializer is responsible for creating the physical channel
/// endpoints that connect source workers to target workers for a single
/// exchange edge. The exchange routing logic (`ExchangePush`/`ExchangePull`)
/// wraps these endpoints — the materializer only provides the raw transport.
///
/// # Contract
///
/// - `materialize_worker(i)` must be called exactly once per worker index
///   in `0..num_workers`, in any order.
/// - Returns `num_workers` push endpoints (index j → send to worker j)
///   and `num_workers` pull endpoints (index j → receive from worker j).
/// - After all workers are materialized, the materializer is consumed.
///
/// # Mixing transports
///
/// A materializer may return a mix of local and remote endpoints. For
/// example, in a cluster with 4 workers where workers 0-1 are on node A
/// and workers 2-3 are on node B:
/// - Worker 0's push endpoints: \[local(0), local(1), remote(2), remote(3)\]
/// - Worker 0's pull endpoints: \[local(0), local(1), remote(2), remote(3)\]
///
/// The `ExchangePush`/`ExchangePull` wrappers are agnostic to the transport
/// behind each endpoint — they only use the `Push`/`Pull` trait interface.
///
/// # Wake semantics
///
/// The `SharedWakeRegistry` (created by `build_exchange_factories`) handles
/// in-process wakes: when worker i pushes to worker j, worker j is woken.
/// For **network-backed endpoints**, the network `Pull` implementation is
/// responsible for waking the local worker when remote data arrives (e.g.,
/// by holding a `WakeHandle` obtained at construction time). The
/// `SharedWakeRegistry` only covers in-process notification — remote wakes
/// are the materializer's responsibility.
pub trait EdgeMaterializer<T: Timestamp, D: Send + 'static>: Send {
    /// Returns the number of workers this materializer is configured for.
    ///
    /// Used by callers to validate consistency and size shared structures
    /// (e.g., `SharedWakeRegistry`).
    fn num_workers(&self) -> usize;

    /// Produce the push and pull endpoints for the given worker.
    ///
    /// Returns `(pushers, pullers)` where both Vecs have length exactly
    /// [`num_workers()`](Self::num_workers):
    /// - `pushers[j]` sends data to worker j
    /// - `pullers[j]` receives data from worker j
    ///
    /// # Invariant
    ///
    /// Both returned Vecs **must** have length exactly `num_workers()`.
    /// Violating this will cause out-of-bounds panics in
    /// `ExchangePush`/`ExchangePull` during routing.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker index is out of range or if the
    /// worker's endpoints have already been taken. After all workers
    /// are materialized, further calls must return an error.
    fn materialize_worker(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)>;
}

// ---------------------------------------------------------------------------
// LocalEdgeMaterializer — in-process bounded channels
// ---------------------------------------------------------------------------

/// Edge materializer for single-node (all workers in one process).
///
/// Wraps [`ExchangeChannelSet`] to provide in-process bounded channels.
/// This is the default materializer used when no cluster topology is
/// configured — it produces the same channels as before the
/// `EdgeMaterializer` abstraction was introduced.
pub struct LocalEdgeMaterializer<T: Timestamp, D: Send + 'static> {
    channel_set: ExchangeChannelSet<T, D>,
    num_workers: usize,
}

impl<T: Timestamp, D: Send + 'static> LocalEdgeMaterializer<T, D> {
    /// Create a new local materializer for `num_workers` workers.
    ///
    /// Allocates an N×N matrix of bounded in-process channels with the
    /// given capacity per channel.
    pub fn new(num_workers: usize, capacity: usize) -> Self {
        Self {
            channel_set: ExchangeChannelSet::new(num_workers, capacity),
            num_workers,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> EdgeMaterializer<T, D>
    for LocalEdgeMaterializer<T, D>
{
    fn num_workers(&self) -> usize {
        self.num_workers
    }

    fn materialize_worker(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)> {
        self.channel_set.take_pair(worker_idx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;
    use crate::dataflow::channels::pushpull::{Pull, Push};

    #[test]
    fn local_materializer_produces_endpoints() {
        let mut mat = LocalEdgeMaterializer::<u64, i32>::new(3, 16);

        // Materialize all 3 workers.
        let (push0, pull0) = mat.materialize_worker(0).unwrap();
        let (push1, pull1) = mat.materialize_worker(1).unwrap();
        let (push2, pull2) = mat.materialize_worker(2).unwrap();

        // Each worker gets 3 push endpoints and 3 pull endpoints.
        assert_eq!(push0.len(), 3);
        assert_eq!(pull0.len(), 3);
        assert_eq!(push1.len(), 3);
        assert_eq!(pull1.len(), 3);
        assert_eq!(push2.len(), 3);
        assert_eq!(pull2.len(), 3);
    }

    #[test]
    fn local_materializer_channels_are_connected() {
        let mut mat = LocalEdgeMaterializer::<u64, String>::new(2, 16);

        let (mut push0, _pull0) = mat.materialize_worker(0).unwrap();
        let (_push1, mut pull1) = mat.materialize_worker(1).unwrap();

        // Worker 0 pushes to worker 1 (push0[1] → pull1[0]).
        push0[1]
            .push(Envelope::data(42u64, vec!["hello".to_string()]))
            .unwrap();

        // Worker 1 pulls from worker 0 (pull1[0]).
        let env = pull1[0].pull().unwrap();
        assert_eq!(env.as_data(), Some((&42u64, &vec!["hello".to_string()])));
    }

    #[test]
    fn local_materializer_double_take_fails() {
        let mut mat = LocalEdgeMaterializer::<u64, i32>::new(2, 16);
        mat.materialize_worker(0).unwrap();
        // Taking worker 0 again should fail.
        assert!(mat.materialize_worker(0).is_err());
    }

    #[test]
    fn local_materializer_out_of_range_fails() {
        let mut mat = LocalEdgeMaterializer::<u64, i32>::new(2, 16);
        assert!(mat.materialize_worker(5).is_err());
    }

    #[test]
    fn local_materializer_arbitrary_order() {
        let mut mat = LocalEdgeMaterializer::<u64, i32>::new(3, 16);

        // Materialize in non-sequential order.
        let (push2, pull2) = mat.materialize_worker(2).unwrap();
        let (push0, pull0) = mat.materialize_worker(0).unwrap();
        let (push1, pull1) = mat.materialize_worker(1).unwrap();

        assert_eq!(push0.len(), 3);
        assert_eq!(push1.len(), 3);
        assert_eq!(push2.len(), 3);
        assert_eq!(pull0.len(), 3);
        assert_eq!(pull1.len(), 3);
        assert_eq!(pull2.len(), 3);
    }

    #[test]
    fn local_materializer_num_workers() {
        let mat = LocalEdgeMaterializer::<u64, i32>::new(4, 16);
        assert_eq!(mat.num_workers(), 4);
    }

    #[test]
    fn edge_materializer_is_object_safe() {
        // Proves the trait can be used as dyn EdgeMaterializer (required
        // by create_exchange_factories_with which takes Arc<Mutex<dyn ...>>).
        let mat: Box<dyn EdgeMaterializer<u64, i32>> =
            Box::new(LocalEdgeMaterializer::new(2, 16));
        assert_eq!(mat.num_workers(), 2);
    }
}
