//! Logical worker identity and operator activation types.
//!
//! A [`WorkerId`] represents a globally unique logical worker that is not tied
//! to any physical OS thread. The worker thread pool maps logical workers to
//! physical threads at runtime.

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
}
