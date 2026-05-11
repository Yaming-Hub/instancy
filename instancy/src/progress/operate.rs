//! Operator interface and progress reporting types.
//!
//! This module defines the contract between operators and the progress tracking system.
//! Every operator in the dataflow graph participates in progress tracking through two
//! mechanisms:
//!
//! 1. **Static shape declaration** (via [`OperatorCore`]): At construction time, each
//!    operator declares how many input/output ports it has and how timestamps transform
//!    through its internals. The reachability tracker uses this to build its propagation
//!    graph.
//!
//! 2. **Runtime capability accounting** (via [`ProgressReporter`]): During execution,
//!    operators create and drop capabilities (timestamps they are allowed to produce
//!    output at). Each create/clone/drop/downgrade is recorded as a +1/-1 change in
//!    the reporter, which the progress tracker drains periodically.
//!
//! Key types:
//! - [`PortConnectivity`] — describes how timestamps transform through an operator
//!   (a matrix of path summary antichains from inputs to outputs).
//! - [`OperatorProgress`] — shared buffer holding per-port capability reporters.
//! - [`ProgressReporter`] — thread-safe handle wrapping a [`ChangeBatch`] for atomic
//!   capability change recording.
//! - [`OperatorCore`] — trait that operators implement to declare their shape and
//!   initial capabilities.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::order::PartialOrder;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// PortConnectivity — operator internal path summaries
// ---------------------------------------------------------------------------

/// Describes how timestamps transform through an operator's internals.
///
/// For an operator with `I` inputs and `O` outputs, `connectivity[i][o]` is the
/// [`Antichain`] of [`crate::progress::timestamp::PathSummary`] values describing all
/// paths from input `i`
/// to output `o`. An empty antichain means there is no path.
///
/// # Why antichains of summaries?
///
/// A single input-to-output pair may have multiple internal paths (e.g., in a
/// subgraph with branches). Each path has its own summary (timestamp transformation).
/// Since timestamps form a partial order (not total), we cannot pick a single "best"
/// summary — instead we keep the antichain of minimal summaries. During propagation,
/// ALL summaries in the antichain are applied to compute the set of reachable
/// downstream timestamps.
///
/// # Common patterns
///
/// - **Pass-through operator** (map, filter): 1 input, 1 output, identity summary.
///   Use [`identity()`](Self::identity).
/// - **Binary operator** (join): 2 inputs, 1 output, identity summary on both paths.
/// - **Feedback/loop**: The ingress/egress operators increment a loop counter,
///   so their summary advances the inner timestamp dimension.
/// - **Sink** (no outputs): 0 outputs, all antichains trivially empty.
#[derive(Clone, Debug)]
pub struct PortConnectivity<S> {
    /// `connectivity[input][output]` — antichain of path summaries.
    connectivity: Vec<Vec<Antichain<S>>>,
    /// Explicitly stored input count.
    num_inputs: usize,
    /// Explicitly stored output count.
    num_outputs: usize,
}

impl<S: Clone + Debug + PartialEq + PartialOrder> PortConnectivity<S> {
    /// Creates a new connectivity matrix with the given dimensions.
    ///
    /// All paths are initially empty (no connectivity).
    pub fn new(inputs: usize, outputs: usize) -> Self {
        Self {
            connectivity: vec![vec![Antichain::new(); outputs]; inputs],
            num_inputs: inputs,
            num_outputs: outputs,
        }
    }

    /// Creates an identity connectivity for a pass-through operator
    /// (1 input, 1 output, default/identity summary).
    pub fn identity(summary: S) -> Self {
        let mut conn = Self::new(1, 1);
        conn.connectivity[0][0].insert(summary);
        conn
    }

    /// Returns the number of inputs.
    pub fn inputs(&self) -> usize {
        self.num_inputs
    }

    /// Returns the number of outputs.
    pub fn outputs(&self) -> usize {
        self.num_outputs
    }

    /// Returns the path summaries from `input` to `output`.
    pub fn path(&self, input: usize, output: usize) -> &Antichain<S> {
        &self.connectivity[input][output]
    }

    /// Returns a mutable reference to the path summaries from `input` to `output`.
    pub fn path_mut(&mut self, input: usize, output: usize) -> &mut Antichain<S> {
        &mut self.connectivity[input][output]
    }

    /// Iterates over all (input, output, summary_antichain) triples.
    pub fn iter(&self) -> impl Iterator<Item = (usize, usize, &Antichain<S>)> {
        self.connectivity.iter().enumerate().flat_map(|(i, row)| {
            row.iter()
                .enumerate()
                .map(move |(o, summary)| (i, o, summary))
        })
    }
}

// ---------------------------------------------------------------------------
// ProgressReporter — thread-safe handle for capability accounting
// ---------------------------------------------------------------------------

/// A thread-safe handle for recording capability changes on a single output port.
///
/// Shared between [`Capability`](super::capability::Capability) instances and the
/// progress tracking system. All capability create/clone/drop/downgrade operations
/// go through this handle so changes are observed atomically.
#[derive(Clone, Debug)]
pub struct ProgressReporter<T: Timestamp> {
    inner: Arc<Mutex<ChangeBatch<T>>>,
}

impl<T: Timestamp> ProgressReporter<T> {
    /// Creates a new reporter with an empty change batch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ChangeBatch::new())),
        }
    }

    /// Records a change: `+1` for a new capability, `-1` for a dropped one.
    pub fn update(&self, time: T, diff: i64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .update(time, diff);
    }

    /// Records a paired update atomically: `-1` at `old_time`, `+1` at `new_time`.
    ///
    /// Used by [`Capability::downgrade`](super::capability::Capability::downgrade) to
    /// ensure the frontier never sees an intermediate state.
    pub fn downgrade(&self, old_time: T, new_time: T) {
        let mut batch = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        batch.update(old_time, -1);
        batch.update(new_time, 1);
    }

    /// Drains all pending changes from this reporter, returning them as a vec.
    pub fn drain(&self) -> Vec<(T, i64)> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect()
    }

    /// Drains pending changes into the given `ChangeBatch`.
    pub fn drain_into(&self, target: &mut ChangeBatch<T>) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain_into(target);
    }

    /// Returns `true` if there are no pending (unread) changes.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
}

impl<T: Timestamp> Default for ProgressReporter<T> {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    static MATERIALIZATION_REPORTERS: RefCell<Option<Box<dyn Any>>> =
        RefCell::new(None);
}

pub(crate) struct MaterializationReporterGuard;

impl Drop for MaterializationReporterGuard {
    fn drop(&mut self) {
        MATERIALIZATION_REPORTERS.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }
}

pub(crate) fn install_materialization_reporters<T: Timestamp>(
    reporters: HashMap<(usize, usize), ProgressReporter<T>>,
) -> MaterializationReporterGuard {
    MATERIALIZATION_REPORTERS.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(reporters));
    });
    MaterializationReporterGuard
}

pub(crate) fn materialization_reporter<T: Timestamp>(
    operator: usize,
    output: usize,
) -> Option<ProgressReporter<T>> {
    MATERIALIZATION_REPORTERS.with(|slot| {
        slot.borrow()
            .as_ref()?
            .downcast_ref::<HashMap<(usize, usize), ProgressReporter<T>>>()?
            .get(&(operator, output))
            .cloned()
    })
}

// ---------------------------------------------------------------------------
// OperatorProgress — shared buffers between operator and progress tracker
// ---------------------------------------------------------------------------

/// Shared progress buffers between an operator and the progress tracker.
///
/// Each operator has one `OperatorProgress` instance. Capabilities write to
/// `internal` (via [`ProgressReporter`]) and the progress tracker drains those
/// changes during propagation.
///
/// The `consumed` and `produced` buffers are reserved for future use when
/// operators explicitly report message consumption/production for in-flight
/// message accounting. Currently, progress tracking relies solely on capability
/// accounting via the `internal` reporters.
#[derive(Debug, Clone)]
pub struct OperatorProgress<T: Timestamp> {
    /// Per-input consumed changes — reserved for future message-flight accounting.
    pub consumed: Vec<ChangeBatch<T>>,
    /// Per-output produced changes — reserved for future message-flight accounting.
    pub produced: Vec<ChangeBatch<T>>,
    /// Per-output capability reporters (capabilities write here via ProgressReporter).
    pub internal: Vec<ProgressReporter<T>>,
}

impl<T: Timestamp> OperatorProgress<T> {
    /// Creates progress buffers for an operator with the given port counts.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        Self {
            consumed: (0..inputs).map(|_| ChangeBatch::new()).collect(),
            produced: (0..outputs).map(|_| ChangeBatch::new()).collect(),
            internal: (0..outputs).map(|_| ProgressReporter::new()).collect(),
        }
    }

    /// Creates an independent deep copy with fresh [`ProgressReporter`]s.
    ///
    /// Unlike `clone()` (which shares `Arc`-backed reporters), this creates
    /// new reporters with empty change batches. Used when cloning a
    /// `SubgraphBuilder` for multi-worker materialization so each worker
    /// gets independent progress state.
    pub fn deep_clone(&self) -> Self {
        Self {
            consumed: self.consumed.clone(),
            produced: self.produced.clone(),
            internal: self
                .internal
                .iter()
                .map(|_| ProgressReporter::new())
                .collect(),
        }
    }

    /// Returns the reporter for the given output port.
    ///
    /// Capabilities for this output should be created using this reporter.
    pub fn reporter(&self, output: usize) -> &ProgressReporter<T> {
        &self.internal[output]
    }
}

// ---------------------------------------------------------------------------
// OperatorCore — trait operators implement
// ---------------------------------------------------------------------------

/// Trait for operator implementations to declare their shape and connectivity.
///
/// This is the **static** interface between operators and the progress system.
/// The runtime calls `get_internal_summary()` once during dataflow construction
/// to build the reachability graph. After that, progress tracking is driven
/// entirely by capability accounting through [`ProgressReporter`].
///
/// # Contract
///
/// - `inputs()` and `outputs()` must return stable values (never change after construction).
/// - `get_internal_summary()` must return a `PortConnectivity` whose dimensions
///   match `inputs() × outputs()`.
/// - The initial capabilities (second return value) declare which timestamps the
///   operator holds at construction time. For most operators this is empty. Input
///   source operators return `[(initial_time, +1)]` on their output port to
///   indicate they can produce data at the initial timestamp.
pub trait OperatorCore<T: Timestamp>: Send {
    /// Returns the number of input ports.
    fn inputs(&self) -> usize;

    /// Returns the number of output ports.
    fn outputs(&self) -> usize;

    /// Returns the internal path connectivity and initial capabilities.
    ///
    /// The connectivity describes all internal paths from inputs to outputs with
    /// their timestamp transformations. The initial capabilities are the timestamps
    /// the operator starts holding on each output (typically empty for most operators,
    /// non-empty for input operators).
    fn get_internal_summary(&self) -> (PortConnectivity<T::Summary>, Vec<ChangeBatch<T>>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_connectivity_new() {
        let conn = PortConnectivity::<usize>::new(2, 3);
        assert_eq!(conn.inputs(), 2);
        assert_eq!(conn.outputs(), 3);
        for (i, o, path) in conn.iter() {
            assert!(path.is_empty(), "path ({i},{o}) should be empty");
        }
    }

    #[test]
    fn port_connectivity_identity() {
        let conn = PortConnectivity::identity(0usize);
        assert_eq!(conn.inputs(), 1);
        assert_eq!(conn.outputs(), 1);
        assert!(!conn.path(0, 0).is_empty());
        assert_eq!(conn.path(0, 0).elements(), &[0usize]);
    }

    #[test]
    fn port_connectivity_insert_paths() {
        let mut conn = PortConnectivity::<usize>::new(2, 2);
        conn.path_mut(0, 0).insert(1);
        conn.path_mut(0, 1).insert(2);
        conn.path_mut(1, 1).insert(0);

        assert_eq!(conn.path(0, 0).elements(), &[1]);
        assert_eq!(conn.path(0, 1).elements(), &[2]);
        assert!(conn.path(1, 0).is_empty());
        assert_eq!(conn.path(1, 1).elements(), &[0]);
    }

    #[test]
    fn port_connectivity_zero_ports() {
        let conn = PortConnectivity::<usize>::new(0, 0);
        assert_eq!(conn.inputs(), 0);
        assert_eq!(conn.outputs(), 0);
        assert_eq!(conn.iter().count(), 0);
    }

    #[test]
    fn progress_reporter_update_and_drain() {
        let reporter = ProgressReporter::<u64>::new();
        reporter.update(10, 1);
        reporter.update(20, 1);
        reporter.update(10, -1);
        let changes = reporter.drain();
        // After compaction: (20, 1) remains; (10, +1-1=0) cancelled
        let non_zero: Vec<_> = changes.into_iter().filter(|(_, d)| *d != 0).collect();
        assert_eq!(non_zero, vec![(20, 1)]);
    }

    #[test]
    fn progress_reporter_atomic_downgrade() {
        let reporter = ProgressReporter::<u64>::new();
        reporter.update(10, 1); // hold cap at 10
        reporter.downgrade(10, 20); // move to 20
        let changes = reporter.drain();
        // Net effect: (10, +1-1=0), (20, +1)
        let non_zero: Vec<_> = changes.into_iter().filter(|(_, d)| *d != 0).collect();
        assert_eq!(non_zero, vec![(20, 1)]);
    }

    #[test]
    fn progress_reporter_drain_into() {
        let reporter = ProgressReporter::<u64>::new();
        reporter.update(5, 1);
        let mut target = ChangeBatch::new();
        target.update(5, 2);
        reporter.drain_into(&mut target);
        // target should now have (5, 3) after compaction
        let items: Vec<_> = target.iter().collect();
        assert_eq!(items, vec![&(5u64, 3i64)]);
    }

    #[test]
    fn progress_reporter_is_empty() {
        let reporter = ProgressReporter::<u64>::new();
        assert!(reporter.is_empty());
        reporter.update(1, 1);
        assert!(!reporter.is_empty());
    }

    #[test]
    fn operator_progress_new() {
        let progress = OperatorProgress::<u64>::new(2, 3);
        assert_eq!(progress.consumed.len(), 2);
        assert_eq!(progress.produced.len(), 3);
        assert_eq!(progress.internal.len(), 3);
    }

    #[test]
    fn operator_progress_reporter_independence() {
        let progress = OperatorProgress::<u64>::new(1, 2);
        progress.reporter(0).update(10, 1);
        progress.reporter(1).update(20, 1);
        assert!(!progress.reporter(0).is_empty());
        assert!(!progress.reporter(1).is_empty());

        let changes0 = progress.reporter(0).drain();
        assert!(progress.reporter(0).is_empty());
        assert!(!progress.reporter(1).is_empty());
        assert_eq!(changes0.len(), 1);
    }

    #[test]
    fn progress_reporter_clone_shares_state() {
        let reporter = ProgressReporter::<u64>::new();
        let clone = reporter.clone();
        reporter.update(10, 1);
        clone.update(20, 1);
        let changes = reporter.drain();
        assert_eq!(changes.len(), 2);
    }
}
