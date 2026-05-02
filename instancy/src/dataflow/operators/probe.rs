//! Probe operator — frontier observation.
//!
//! A probe attaches to a `StreamEdge` and exposes its current frontier,
//! allowing external code to monitor the progress of a computation.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::dataflow::operators::handles::InputHandle;
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{StreamEdge, Slot};
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// Shared frontier state for a probe.
///
/// This is the inner state shared between the `ProbeOperator` (which updates
/// the frontier) and the `ProbeHandle` (which reads it).
#[derive(Debug)]
struct ProbeState<T: Timestamp> {
    /// The current frontier — timestamps that may still arrive.
    frontier: Antichain<T>,
    /// Whether the probe has seen all data (input exhausted).
    done: bool,
}

impl<T: Timestamp> ProbeState<T> {
    fn new() -> Self {
        Self {
            frontier: Antichain::from_elem(T::minimum()),
            done: false,
        }
    }
}

/// A handle for observing the frontier of a probed stream.
///
/// `ProbeHandle` is the external interface for monitoring progress.
/// It is cheap to clone and can be shared across threads.
#[derive(Clone)]
pub struct ProbeHandle<T: Timestamp> {
    state: Arc<Mutex<ProbeState<T>>>,
}

impl<T: Timestamp> ProbeHandle<T> {
    /// Create a new probe handle (paired with a `ProbeOperator`).
    fn new(state: Arc<Mutex<ProbeState<T>>>) -> Self {
        Self { state }
    }

    /// Returns `true` if the frontier is strictly less than `time`.
    ///
    /// This means all future data will have timestamps ≥ `time`,
    /// so all data at timestamps < `time` has been fully processed.
    pub fn less_than(&self, time: &T) -> bool {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.frontier.less_than(time)
    }

    /// Returns `true` if the frontier is less than or equal to `time`.
    pub fn less_equal(&self, time: &T) -> bool {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.frontier.less_equal(time)
    }

    /// Returns a copy of the current frontier.
    pub fn frontier(&self) -> Antichain<T> {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.frontier.clone()
    }

    /// Returns `true` if the probed stream is complete (no more data).
    pub fn done(&self) -> bool {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.done
    }

    /// Advance the frontier to the given antichain.
    ///
    /// This is called by the runtime/probe operator when progress is made.
    pub fn update_frontier(&self, new_frontier: Antichain<T>) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.frontier = new_frontier;
    }

    /// Mark the probe as done — the stream is complete.
    pub fn mark_done(&self) {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        guard.done = true;
        guard.frontier = Antichain::new();
    }
}

impl<T: Timestamp> fmt::Debug for ProbeHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("ProbeHandle")
            .field("frontier", &guard.frontier)
            .field("done", &guard.done)
            .finish()
    }
}

/// A registered probe operator.
///
/// The probe consumes input data (passing it through) and updates
/// a shared `ProbeHandle` with frontier information.
pub struct ProbeOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region.
    region_id: RegionId,
    /// The operator's input handle.
    input: InputHandle<T, D>,
    /// The shared probe state.
    handle: ProbeHandle<T>,
}

impl<T: Timestamp, D> ProbeOperator<T, D> {
    /// Create a new probe operator and its handle.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
    ) -> (Self, ProbeHandle<T>) {
        let state = Arc::new(Mutex::new(ProbeState::new()));
        let handle = ProbeHandle::new(Arc::clone(&state));
        let name = name.into();

        let operator = Self {
            input: InputHandle::new(format!("{name}:input")),
            name,
            index,
            region_id,
            handle: ProbeHandle::new(state),
        };

        (operator, handle)
    }

    /// Get the operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the region ID.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Get a reference to the probe handle (the operator's own copy).
    pub fn handle(&self) -> &ProbeHandle<T> {
        &self.handle
    }

    /// Execute the operator logic once.
    ///
    /// Drains all input data (probe doesn't produce output — it's a sink).
    /// In a fully wired dataflow, the runtime updates the frontier via the handle.
    pub fn activate(&mut self) {
        // Drain all input — probe is a terminal operator.
        while self.input.next().is_some() {}

        if self.input.is_done() {
            self.handle.mark_done();
        }
    }

    /// Whether the input is done.
    pub fn is_done(&self) -> bool {
        self.input.is_done()
    }
}

impl<T: Timestamp, D> fmt::Debug for ProbeOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .finish()
    }
}

/// Extension trait for attaching a probe to a `StreamEdge`.
pub trait ProbeExt<S: Scope, D> {
    /// Attach a probe to this stream.
    ///
    /// Returns a `ProbeHandle` that can be used to observe the frontier.
    /// The probe is a terminal operator — it does not produce an output stream.
    fn probe(&self, name: &str) -> ProbeHandle<S::Timestamp>;
}

impl<S: Scope, D: 'static> ProbeExt<S, D> for StreamEdge<S, D> {
    fn probe(&self, name: &str) -> ProbeHandle<S::Timestamp> {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();

        // Register operator and edge in the dataflow graph.
        // Probe is a terminal operator: 1 input, 0 outputs.
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, name, region_id, 1, 0,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            self.region_id(),
            region_id,
        ));

        let (_operator, handle) = ProbeOperator::<S::Timestamp, D>::new(
            name,
            op_index,
            region_id,
        );

        handle
    }
}

/// Create a probe handle and operator pair for manual wiring.
///
/// This is useful when you need the handle before building the dataflow graph.
pub fn probe_handle<T: Timestamp>() -> ProbeHandle<T> {
    let state = Arc::new(Mutex::new(ProbeState::new()));
    ProbeHandle::new(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;
    use crate::dataflow::stream::Slot;

    #[test]
    fn probe_handle_initial_state() {
        let handle = probe_handle::<u64>();

        // Initial frontier is at T::minimum() (0)
        assert!(!handle.done());
        assert!(handle.less_equal(&0));
        assert!(!handle.less_than(&0));
    }

    #[test]
    fn probe_handle_less_than() {
        let handle = probe_handle::<u64>();

        // Frontier is at {0}
        // less_than(1) means frontier < 1, i.e., all frontier elements < 1
        // Since frontier = {0}, and 0 < 1 → true
        assert!(handle.less_than(&1));

        // less_than(0) means frontier < 0, but 0 is not < 0 → false
        assert!(!handle.less_than(&0));
    }

    #[test]
    fn probe_handle_less_equal() {
        let handle = probe_handle::<u64>();

        // Frontier = {0}
        assert!(handle.less_equal(&0)); // 0 <= 0
        assert!(handle.less_equal(&1)); // 0 <= 1
        // less_equal with frontier = {5}: would 5 <= 4? no
    }

    #[test]
    fn probe_handle_frontier_advance() {
        let handle = probe_handle::<u64>();

        handle.update_frontier(Antichain::from_elem(5));

        assert!(handle.less_than(&6));
        assert!(!handle.less_than(&5));
        assert!(handle.less_equal(&5));
        assert!(!handle.less_equal(&4));
    }

    #[test]
    fn probe_handle_done() {
        let handle = probe_handle::<u64>();

        assert!(!handle.done());
        handle.mark_done();
        assert!(handle.done());

        // After done, frontier should be empty
        let f = handle.frontier();
        assert!(f.elements().is_empty());
    }

    #[test]
    fn probe_handle_clone_shares_state() {
        let handle1 = probe_handle::<u64>();
        let handle2 = handle1.clone();

        handle1.update_frontier(Antichain::from_elem(10));

        // handle2 should see the update
        assert!(handle2.less_than(&11));
        assert!(!handle2.less_than(&10));
    }

    #[test]
    fn probe_operator_creation() {
        let (op, handle) = ProbeOperator::<u64, i32>::new("test_probe", 0, RegionId::new(0));

        assert_eq!(op.name(), "test_probe");
        assert_eq!(op.index(), 0);
        assert!(!handle.done());
    }

    #[test]
    fn probe_operator_drains_input() {
        let (mut op, _handle) = ProbeOperator::<u64, i32>::new("drain_test", 0, RegionId::new(0));

        op.input_mut().push_vec(1, vec![10, 20]);
        op.input_mut().push_vec(2, vec![30]);

        op.activate();

        // All input should be consumed
        assert!(op.input_mut().next().is_none());
    }

    #[test]
    fn probe_operator_marks_done_on_exhaust() {
        let (mut op, handle) = ProbeOperator::<u64, i32>::new("done_test", 0, RegionId::new(0));

        assert!(!handle.done());

        op.input_mut().mark_exhausted();
        op.activate();

        assert!(handle.done());
    }

    #[test]
    fn probe_ext_allocates_operator() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, region_id);

        let handle = stream.probe("my_probe");
        assert!(!handle.done());
    }

    #[test]
    fn probe_handle_frontier_returns_copy() {
        let handle = probe_handle::<u64>();
        handle.update_frontier(Antichain::from_elem(42));

        let f = handle.frontier();
        assert_eq!(f.elements(), &[42]);
    }

    #[test]
    fn probe_handle_empty_frontier_after_done() {
        let handle = probe_handle::<u64>();
        handle.update_frontier(Antichain::from_elem(10));
        assert!(!handle.frontier().elements().is_empty());

        handle.mark_done();
        assert!(handle.frontier().elements().is_empty());
    }

    #[test]
    fn probe_handle_multiple_advances() {
        let handle = probe_handle::<u64>();

        handle.update_frontier(Antichain::from_elem(1));
        assert!(handle.less_than(&2));

        handle.update_frontier(Antichain::from_elem(5));
        assert!(!handle.less_than(&5));
        assert!(handle.less_than(&6));

        handle.update_frontier(Antichain::from_elem(100));
        assert!(handle.less_than(&101));
        assert!(!handle.less_than(&100));
    }
}
