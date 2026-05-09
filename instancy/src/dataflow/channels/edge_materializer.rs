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
//!   `ExchangeChannelSet`). Used for single-node or when all workers share a
//!   process.
//! - Future: `MockNetworkEdgeMaterializer` — serialize/deserialize through
//!   [`crate::communication::Codec`] over in-memory channels (for distributed
//!   testing without a real network).
//! - Future: `NetworkEdgeMaterializer` — real TCP/QUIC transport via
//!   [`crate::communication::ConnectionPool`] and
//!   [`crate::communication::Muxer`]/[`crate::communication::Demuxer`].
//!
//! [`ExchangePush`]: super::exchange_channel::ExchangePush
//! [`ExchangePull`]: super::exchange_channel::ExchangePull

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
/// # Symmetric vs Asymmetric
///
/// - **Symmetric** (M==N): All workers are both sources and targets.
///   Use `materialize_worker(i)` which returns both push and pull ends.
/// - **Asymmetric** (M≠N): Source workers (0..M) differ from target workers
///   (0..N). Use `materialize_source_worker(i)` for push endpoints and
///   `materialize_target_worker(j)` for pull endpoints.
///
/// # Contract
///
/// - For symmetric usage: `materialize_worker(i)` must be called exactly
///   once per worker index in `0..num_workers`, in any order.
/// - For asymmetric usage: `materialize_source_worker(i)` once per source
///   in `0..num_source_workers()`, `materialize_target_worker(j)` once per
///   target in `0..num_target_workers()`.
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
    /// Returns the number of workers for symmetric channels (asserts M==N).
    ///
    /// Used by callers to validate consistency and size shared structures
    /// (e.g., `SharedWakeRegistry`).
    fn num_workers(&self) -> usize {
        // SAFETY: worker counts validated during graph construction
        assert_eq!(
            self.num_source_workers(),
            self.num_target_workers(),
            "num_workers() requires symmetric materializer (M==N)"
        );
        self.num_source_workers()
    }

    /// Number of source workers (M dimension).
    fn num_source_workers(&self) -> usize;

    /// Number of target workers (N dimension).
    fn num_target_workers(&self) -> usize;

    /// Produce the push and pull endpoints for a worker in symmetric mode.
    ///
    /// Returns `(pushers, pullers)`:
    /// - `pushers[j]` sends data to target worker j (length = num_target_workers)
    /// - `pullers[j]` receives data from source worker j (length = num_source_workers)
    ///
    /// # Panics
    /// Default implementation panics if M ≠ N.
    fn materialize_worker(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)> {
        // SAFETY: worker counts validated during graph construction
        assert_eq!(
            self.num_source_workers(),
            self.num_target_workers(),
            "materialize_worker requires symmetric materializer"
        );
        let pushers = self.materialize_source_worker(worker_idx)?;
        let pullers = self.materialize_target_worker(worker_idx)?;
        Ok((pushers, pullers))
    }

    /// Produce push endpoints for source worker `src_idx`.
    ///
    /// Returns a Vec of length `num_target_workers()` — one push endpoint
    /// per target worker.
    fn materialize_source_worker(&mut self, src_idx: usize)
    -> Result<Vec<Box<dyn Push<T, D, ()>>>>;

    /// Produce pull endpoints for target worker `dst_idx`.
    ///
    /// Returns a Vec of length `num_source_workers()` — one pull endpoint
    /// per source worker.
    fn materialize_target_worker(&mut self, dst_idx: usize)
    -> Result<Vec<Box<dyn Pull<T, D, ()>>>>;
}

// ---------------------------------------------------------------------------
// LocalEdgeMaterializer — in-process bounded channels
// ---------------------------------------------------------------------------

/// Edge materializer for single-node (all workers in one process).
///
/// Wraps `ExchangeChannelSet` to provide in-process bounded channels.
/// This is the default materializer used when no cluster topology is
/// configured — it produces the same channels as before the
/// `EdgeMaterializer` abstraction was introduced.
pub struct LocalEdgeMaterializer<T: Timestamp, D: Send + 'static> {
    channel_set: ExchangeChannelSet<T, D>,
    num_sources: usize,
    num_targets: usize,
}

impl<T: Timestamp, D: Send + 'static> LocalEdgeMaterializer<T, D> {
    /// Create a new local materializer for `num_workers` workers (symmetric).
    ///
    /// Allocates an N×N matrix of bounded in-process channels with the
    /// given capacity per channel.
    pub fn new(num_workers: usize, capacity: usize) -> Self {
        Self::new_asymmetric(num_workers, num_workers, capacity)
    }

    /// Create a new local materializer for M source → N target workers.
    ///
    /// Allocates an M×N matrix of bounded in-process channels with the
    /// given capacity per channel.
    pub fn new_asymmetric(num_sources: usize, num_targets: usize, capacity: usize) -> Self {
        Self {
            channel_set: ExchangeChannelSet::new_asymmetric(num_sources, num_targets, capacity),
            num_sources,
            num_targets,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> EdgeMaterializer<T, D> for LocalEdgeMaterializer<T, D> {
    fn num_source_workers(&self) -> usize {
        self.num_sources
    }

    fn num_target_workers(&self) -> usize {
        self.num_targets
    }

    fn materialize_source_worker(
        &mut self,
        src_idx: usize,
    ) -> Result<Vec<Box<dyn Push<T, D, ()>>>> {
        self.channel_set.take_source_endpoints(src_idx)
    }

    fn materialize_target_worker(
        &mut self,
        dst_idx: usize,
    ) -> Result<Vec<Box<dyn Pull<T, D, ()>>>> {
        self.channel_set.take_target_endpoints(dst_idx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;

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
        assert_eq!(mat.num_source_workers(), 4);
        assert_eq!(mat.num_target_workers(), 4);
    }

    #[test]
    fn edge_materializer_is_object_safe() {
        // Proves the trait can be used as dyn EdgeMaterializer
        // (required by build_exchange_factories which takes Arc<Mutex<dyn ...>>).
        let mat: Box<dyn EdgeMaterializer<u64, i32>> = Box::new(LocalEdgeMaterializer::new(2, 16));
        assert_eq!(mat.num_source_workers(), 2);
        assert_eq!(mat.num_target_workers(), 2);
    }

    #[test]
    fn local_materializer_asymmetric() {
        // 2 sources → 3 targets
        let mut mat = LocalEdgeMaterializer::<u64, i32>::new_asymmetric(2, 3, 16);
        assert_eq!(mat.num_source_workers(), 2);
        assert_eq!(mat.num_target_workers(), 3);

        // Materialize source workers (push endpoints)
        let push0 = mat.materialize_source_worker(0).unwrap();
        let push1 = mat.materialize_source_worker(1).unwrap();
        assert_eq!(push0.len(), 3); // each source pushes to 3 targets
        assert_eq!(push1.len(), 3);

        // Materialize target workers (pull endpoints)
        let pull0 = mat.materialize_target_worker(0).unwrap();
        let pull1 = mat.materialize_target_worker(1).unwrap();
        let pull2 = mat.materialize_target_worker(2).unwrap();
        assert_eq!(pull0.len(), 2); // each target pulls from 2 sources
        assert_eq!(pull1.len(), 2);
        assert_eq!(pull2.len(), 2);
    }

    #[test]
    fn local_materializer_asymmetric_data_flows() {
        let mut mat = LocalEdgeMaterializer::<u64, String>::new_asymmetric(2, 3, 16);

        let mut push0 = mat.materialize_source_worker(0).unwrap();
        let _push1 = mat.materialize_source_worker(1).unwrap();
        let _pull0 = mat.materialize_target_worker(0).unwrap();
        let mut pull1 = mat.materialize_target_worker(1).unwrap();
        let _pull2 = mat.materialize_target_worker(2).unwrap();

        // Source 0 pushes to target 1
        push0[1]
            .push(Envelope::data(7u64, vec!["msg".to_string()]))
            .unwrap();

        // Target 1 pulls from source 0
        let env = pull1[0].pull().unwrap();
        assert_eq!(env.as_data(), Some((&7u64, &vec!["msg".to_string()])));
    }
}
