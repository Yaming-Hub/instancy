//! Logical worker identity, context, and operator activation types.
//!
//! A [`WorkerId`] represents a globally unique logical worker that is not tied
//! to any physical OS thread. The worker thread pool maps logical workers to
//! physical threads at runtime.
//!
//! A [`WorkerContext`] provides operator factories with the worker's identity
//! and the total worker count during materialization. This enables
//! worker-aware operator construction (e.g., exchange routing, stateful
//! partitioning).

use std::fmt;

/// A globally unique logical worker identity.
///
/// Workers are numbered sequentially across all nodes in the cluster.
/// A `WorkerId` determines data partitioning (exchange routing) and
/// ensures FIFO ordering for tasks assigned to the same worker.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkerId(pub usize);

impl WorkerId {
    /// Create a new worker ID.
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    /// Get the raw index.
    pub fn index(&self) -> usize {
        self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Worker({})", self.0)
    }
}

impl From<usize> for WorkerId {
    fn from(index: usize) -> Self {
        Self(index)
    }
}

// ---------------------------------------------------------------------------
// WorkerContext — passed to operator factories during materialization
// ---------------------------------------------------------------------------

/// Context provided to operator factories during materialization.
///
/// Each worker in a multi-worker dataflow receives a `WorkerContext` that
/// identifies which worker replica is being materialized and how many workers
/// exist in total. This enables:
///
/// - **Exchange routing**: operators can determine which peer to send data to
///   based on `num_workers` and hash-based partitioning.
/// - **Worker-local state**: stateful operators can key their internal state
///   by `worker_index` to avoid cross-worker interference.
/// - **Logging/metrics**: operators can tag metrics with the worker index.
///
/// For single-worker dataflows (spawned via [`crate::RuntimeHandle::spawn`] or
/// [`crate::SimpleRuntime::spawn`]), `worker_index` is 0 and `num_workers` is 1.
///
/// # Example
///
/// ```rust
/// use instancy::worker::WorkerContext;
///
/// let ctx = WorkerContext::single();
/// assert_eq!(ctx.worker_index(), 0);
/// assert_eq!(ctx.num_workers(), 1);
/// assert!(ctx.is_single_worker());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerContext {
    worker_index: usize,
    num_workers: usize,
}

impl WorkerContext {
    /// Create a new worker context.
    ///
    /// # Panics
    ///
    /// Panics if `worker_index >= num_workers` or `num_workers == 0`.
    /// This is `pub(crate)` because only the runtime constructs worker contexts;
    /// callers should never need to create one directly.
    pub(crate) fn new(worker_index: usize, num_workers: usize) -> Self {
        assert!(num_workers > 0, "num_workers must be >= 1");
        assert!(
            worker_index < num_workers,
            "worker_index ({worker_index}) must be < num_workers ({num_workers})"
        );
        Self {
            worker_index,
            num_workers,
        }
    }

    /// Create a context for a single-worker dataflow (index=0, count=1).
    pub fn single() -> Self {
        Self {
            worker_index: 0,
            num_workers: 1,
        }
    }

    /// The zero-based index of this worker within the dataflow.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// Total number of workers in this dataflow.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Whether this is a single-worker dataflow.
    pub fn is_single_worker(&self) -> bool {
        self.num_workers == 1
    }
}

impl fmt::Display for WorkerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Worker({}/{})",
            self.worker_index, self.num_workers
        )
    }
}

/// A queued work item for an operator.
///
/// An activation represents a unit of work: running an operator's logic
/// once with available input data. The worker thread pool executes
/// activations in FIFO order per logical worker.
pub struct OperatorActivation {
    /// The logical worker this activation belongs to.
    pub worker_id: WorkerId,
    /// Human-readable name for debugging/metrics.
    pub operator_name: String,
    /// The operator index within the dataflow.
    pub operator_index: usize,
    /// The closure that performs the operator's computation.
    /// Takes no arguments (input data is accessed via shared buffers).
    pub work: Box<dyn FnOnce() + Send>,
}

impl OperatorActivation {
    /// Create a new operator activation.
    pub fn new(
        worker_id: WorkerId,
        operator_name: impl Into<String>,
        operator_index: usize,
        work: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            worker_id,
            operator_name: operator_name.into(),
            operator_index,
            work: Box::new(work),
        }
    }

    /// Execute this activation's work closure.
    pub fn execute(self) {
        (self.work)();
    }
}

impl fmt::Debug for OperatorActivation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorActivation")
            .field("worker_id", &self.worker_id)
            .field("operator_name", &self.operator_name)
            .field("operator_index", &self.operator_index)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_id_creation_and_display() {
        let w = WorkerId::new(42);
        assert_eq!(w.index(), 42);
        assert_eq!(format!("{w}"), "Worker(42)");
    }

    #[test]
    fn worker_id_ordering() {
        let w0 = WorkerId::new(0);
        let w1 = WorkerId::new(1);
        let w2 = WorkerId::new(2);
        assert!(w0 < w1);
        assert!(w1 < w2);
    }

    #[test]
    fn worker_id_from_usize() {
        let w: WorkerId = 7.into();
        assert_eq!(w.index(), 7);
    }

    #[test]
    fn worker_id_hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(WorkerId::new(1));
        set.insert(WorkerId::new(2));
        set.insert(WorkerId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn operator_activation_executes() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let executed = Arc::new(AtomicBool::new(false));
        let executed_clone = executed.clone();

        let activation = OperatorActivation::new(
            WorkerId::new(0),
            "test_op",
            0,
            move || { executed_clone.store(true, Ordering::SeqCst); },
        );

        assert!(!executed.load(Ordering::SeqCst));
        activation.execute();
        assert!(executed.load(Ordering::SeqCst));
    }

    #[test]
    fn operator_activation_debug() {
        let activation = OperatorActivation::new(
            WorkerId::new(3),
            "map_op",
            5,
            || {},
        );
        let debug = format!("{activation:?}");
        assert!(debug.contains("WorkerId(3)"));
        assert!(debug.contains("map_op"));
        assert!(debug.contains("5"));
    }

    // --- WorkerContext tests ---

    #[test]
    fn worker_context_single() {
        let ctx = WorkerContext::single();
        assert_eq!(ctx.worker_index(), 0);
        assert_eq!(ctx.num_workers(), 1);
        assert!(ctx.is_single_worker());
        assert_eq!(format!("{ctx}"), "Worker(0/1)");
    }

    #[test]
    fn worker_context_multi() {
        let ctx = WorkerContext::new(2, 4);
        assert_eq!(ctx.worker_index(), 2);
        assert_eq!(ctx.num_workers(), 4);
        assert!(!ctx.is_single_worker());
        assert_eq!(format!("{ctx}"), "Worker(2/4)");
    }

    #[test]
    fn worker_context_boundary_values() {
        // worker 0 of 1 (single worker)
        let ctx = WorkerContext::new(0, 1);
        assert!(ctx.is_single_worker());

        // last worker
        let ctx = WorkerContext::new(7, 8);
        assert_eq!(ctx.worker_index(), 7);
        assert_eq!(ctx.num_workers(), 8);
    }

    #[test]
    #[should_panic(expected = "num_workers must be >= 1")]
    fn worker_context_zero_workers_panics() {
        WorkerContext::new(0, 0);
    }

    #[test]
    #[should_panic(expected = "worker_index (3) must be < num_workers (3)")]
    fn worker_context_index_out_of_range_panics() {
        WorkerContext::new(3, 3);
    }

    #[test]
    fn worker_context_equality() {
        let a = WorkerContext::new(1, 4);
        let b = WorkerContext::new(1, 4);
        let c = WorkerContext::new(2, 4);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
