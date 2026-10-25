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

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
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
///
/// Besides the changes the tracker has not drained yet, the reporter keeps
/// the capabilities its port currently holds (`held`, never drained). The
/// operator shell reads it to justify output (V3) and to decide whether the
/// operator still holds a capability (S2a, seals). A release from another
/// task (a dropped [`Retained`](crate::dataflow::Retained)) lands here too:
/// the reporter is the port's release mailbox.
#[derive(Clone, Debug)]
pub struct ProgressReporter<T: Timestamp> {
    inner: Arc<Mutex<ReporterState<T>>>,
    /// Where the progress checker counts capabilities held through this
    /// reporter (set when the reporter's tracker is attached to an oracle).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check: Arc<std::sync::OnceLock<crate::progress::check::Port<T>>>,
}

/// A reporter's changes not yet drained, and the capabilities held.
#[derive(Debug)]
struct ReporterState<T: Timestamp> {
    pending: ChangeBatch<T>,
    /// The capabilities held, per time (no zero entries).
    held: BTreeMap<T, i64>,
    /// While a logic call runs (see `Justify`): the minimal times of the
    /// capabilities released during it, which still justify its output.
    released_in_call: Option<Antichain<T>>,
    /// Releases the progress checker still counts as held: a released
    /// capability stays in this mailbox until a commit drains it.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    released: Vec<(T, i64)>,
}

impl<T: Timestamp> ProgressReporter<T> {
    /// Creates a new reporter with an empty change batch.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ReporterState {
                pending: ChangeBatch::new(),
                held: BTreeMap::new(),
                released_in_call: None,
                #[cfg(any(debug_assertions, feature = "progress-check"))]
                released: Vec::new(),
            })),
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check: Arc::default(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ReporterState<T>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record `n` capabilities at `time` as held without reporting them: an
    /// initial capability, which every tracker seeds from the physical plan
    /// (A4), is held by the operator from the start.
    pub(crate) fn seed_held(&self, time: T, n: i64) {
        Self::hold(&mut self.state(), time, n);
    }

    /// Changes the held count at `time` by `diff`; a release during a logic
    /// call is remembered for the call's justification.
    fn hold(state: &mut ReporterState<T>, time: T, diff: i64) {
        if diff < 0 {
            if let Some(released) = &mut state.released_in_call {
                released.insert_ref(&time);
            }
        }
        match state.held.entry(time) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += diff;
                if *entry.get() == 0 {
                    entry.remove();
                }
            }
            Entry::Vacant(entry) => {
                if diff != 0 {
                    entry.insert(diff);
                }
            }
        }
    }

    /// The minimal times at which this port holds capabilities.
    pub(crate) fn held_frontier(&self) -> Antichain<T> {
        let state = self.state();
        let mut frontier = Antichain::new();
        for (time, count) in &state.held {
            if *count > 0 {
                frontier.insert_ref(time);
            }
        }
        frontier
    }

    /// Whether this port holds any capability.
    pub(crate) fn holds_any(&self) -> bool {
        !self.state().held.is_empty()
    }

    /// A logic call starts: remember the capabilities released during it
    /// (see [`justifies`](Self::justifies)).
    pub(crate) fn begin_call(&self) {
        self.state().released_in_call = Some(Antichain::new());
    }

    /// The logic call (and the check of its output) ended.
    pub(crate) fn end_call(&self) {
        self.state().released_in_call = None;
    }

    /// The minimal times of the capabilities released during the current
    /// logic call.
    pub(crate) fn released_in_call(&self) -> Antichain<T> {
        self.state().released_in_call.clone().unwrap_or_default()
    }

    /// Whether a capability this port holds, or released during the current
    /// logic call, is at or before `time`. Only times at or before `time` in
    /// `Ord` can be (`Ord` is a linear extension of the partial order), and
    /// the scan stops at the first that is: O(log n) for a total order.
    pub(crate) fn justifies(&self, time: &T) -> bool {
        let state = self.state();
        state
            .released_in_call
            .as_ref()
            .is_some_and(|released| released.less_equal(time))
            || state
                .held
                .range(..=time)
                .any(|(held, count)| *count > 0 && held.less_equal(time))
    }

    /// Count `diff` capabilities at `time` held through this reporter, for
    /// the progress checker's oracle. Called by capability objects and by
    /// operators that hold a capability without one (input sources); never
    /// by the accounting itself.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_hold(&self, time: &T, diff: i64) {
        if let Some(port) = self.check.get() {
            port.count(crate::progress::check::Store::Capability, time, diff);
        }
    }

    /// Count a capability at `time` released through this reporter, for the
    /// progress checker: it stays counted (as held) until a commit drains the
    /// release from this mailbox. Called with the state locked, together with
    /// the `-1` itself, so a drain (on another thread than a dropped
    /// `Retained`) never takes the `-1` without the release.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_release(&self, state: &mut ReporterState<T>, time: &T) {
        if self.check.get().is_some() {
            state.released.push((time.clone(), 1));
        }
    }

    /// The releases a drain took out of the mailbox leave the checker's count.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_released(&self, released: Vec<(T, i64)>) {
        if let Some(port) = self.check.get() {
            for (time, n) in released {
                port.count(crate::progress::check::Store::Capability, &time, -n);
            }
        }
    }

    /// Tell the progress checker which output this reporter is for.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn set_check_port(&self, port: crate::progress::check::Port<T>) {
        let _ = self.check.set(port);
    }

    /// Records a change: `+1` for a new capability, `-1` for a dropped one.
    pub fn update(&self, time: T, diff: i64) {
        let mut state = self.state();
        state.pending.update(time.clone(), diff);
        Self::hold(&mut state, time, diff);
    }

    /// Records a dropped capability at `time` (`-1`), and its release in the
    /// checker's mailbox, under one lock.
    pub(crate) fn release(&self, time: T) {
        let mut state = self.state();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_release(&mut state, &time);
        state.pending.update(time.clone(), -1);
        Self::hold(&mut state, time, -1);
    }

    /// Records a paired update atomically: `-1` at `old_time`, `+1` at `new_time`.
    ///
    /// Used by [`Capability::downgrade`](super::capability::Capability::downgrade) to
    /// ensure the frontier never sees an intermediate state.
    pub fn downgrade(&self, old_time: T, new_time: T) {
        let mut state = self.state();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_release(&mut state, &old_time);
        state.pending.update(old_time.clone(), -1);
        state.pending.update(new_time.clone(), 1);
        Self::hold(&mut state, old_time, -1);
        Self::hold(&mut state, new_time, 1);
    }

    /// Drains all pending changes from this reporter, returning them as a vec.
    pub fn drain(&self) -> Vec<(T, i64)> {
        let mut state = self.state();
        let changes = state.pending.drain().collect();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        {
            let released = std::mem::take(&mut state.released);
            drop(state);
            self.check_released(released);
        }
        changes
    }

    /// Drains pending changes into the given `ChangeBatch`.
    pub fn drain_into(&self, target: &mut ChangeBatch<T>) {
        let mut state = self.state();
        state.pending.drain_into(target);
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        {
            let released = std::mem::take(&mut state.released);
            drop(state);
            self.check_released(released);
        }
    }

    /// Returns `true` if there are no pending (unread) changes.
    pub fn is_empty(&self) -> bool {
        self.state().pending.is_empty()
    }
}

impl<T: Timestamp> Default for ProgressReporter<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// PortLedger — a port's consumed or produced counts
// ---------------------------------------------------------------------------

/// The port-count ledger of one operator port: records the operator shell
/// consumed at an input, or produced at an output, since the tracker last
/// drained it.
///
/// Only the shell writes to it, once per logic call and after the call's
/// output was validated; the tracker drains it at each commit. Data sitting
/// between a producing and a consuming port is therefore counted at the
/// consuming input from the moment the shell takes it from the producer's
/// logic until the consumer's logic takes it.
#[derive(Clone, Debug)]
pub struct PortLedger<T: Timestamp> {
    inner: Arc<Mutex<ChangeBatch<T>>>,
}

impl<T: Timestamp> PortLedger<T> {
    /// An empty ledger.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ChangeBatch::new())),
        }
    }

    /// Adds every change of `changes` to the ledger, draining `changes`.
    pub(crate) fn absorb(&self, changes: &mut ChangeBatch<T>) {
        if changes.is_empty() {
            return;
        }
        changes.drain_into(&mut self.inner.lock().unwrap_or_else(|e| e.into_inner()));
    }

    /// Adds `diff` records at `time`.
    pub(crate) fn update(&self, time: T, diff: i64) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .update(time, diff);
    }

    /// Drains the recorded counts.
    pub fn drain(&self) -> Vec<(T, i64)> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect()
    }

    /// Whether nothing is recorded.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
}

impl<T: Timestamp> Default for PortLedger<T> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// OperatorProgress — shared buffers between operator and progress tracker
// ---------------------------------------------------------------------------

/// Shared progress buffers between an operator and the progress tracker.
///
/// Each operator has one `OperatorProgress` instance per worker:
/// - `consumed[i]`: records the operator shell took from input `i`;
/// - `produced[o]`: records the shell took from the logic at output `o`;
/// - `internal[o]`: capability changes at output `o` (capabilities write
///   here through [`ProgressReporter`]).
///
/// The tracker drains all three at each commit: consumed as `-n` at the
/// input, produced as `+n` at every target of the output's edges, internal
/// at the output.
#[derive(Debug, Clone)]
pub struct OperatorProgress<T: Timestamp> {
    /// Per-input consumed records.
    pub consumed: Vec<PortLedger<T>>,
    /// Per-output produced records.
    pub produced: Vec<PortLedger<T>>,
    /// Per-output capability reporters (capabilities write here via ProgressReporter).
    pub internal: Vec<ProgressReporter<T>>,
}

impl<T: Timestamp> OperatorProgress<T> {
    /// Creates progress buffers for an operator with the given port counts.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        Self {
            consumed: (0..inputs).map(|_| PortLedger::new()).collect(),
            produced: (0..outputs).map(|_| PortLedger::new()).collect(),
            internal: (0..outputs).map(|_| ProgressReporter::new()).collect(),
        }
    }

    /// Creates an independent deep copy with fresh ledgers and
    /// [`ProgressReporter`]s.
    ///
    /// Unlike `clone()` (which shares `Arc`-backed reporters), this creates
    /// new reporters with empty change batches. Used when cloning a
    /// `SubgraphBuilder` for multi-worker materialization so each worker
    /// gets independent progress state.
    pub fn deep_clone(&self) -> Self {
        Self::new(self.consumed.len(), self.produced.len())
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

    /// During a call, a capability justifies output if it is held now or
    /// was released during the call; a release between calls does not carry
    /// into the next one.
    #[test]
    fn reporter_justifies_held_and_released_in_call() {
        let reporter = ProgressReporter::<u64>::new();
        reporter.update(10, 1);
        reporter.update(20, 1);
        assert_eq!(reporter.held_frontier().elements(), &[10]);
        assert!(reporter.justifies(&10) && reporter.justifies(&15));
        assert!(!reporter.justifies(&9));

        reporter.begin_call();
        reporter.update(10, -1);
        assert!(reporter.justifies(&10), "released in this call");
        reporter.end_call();
        assert!(!reporter.justifies(&15), "only 20 is held now");

        reporter.begin_call();
        reporter.downgrade(20, 30);
        assert!(reporter.justifies(&25), "downgraded from 20 in this call");
        assert_eq!(reporter.released_in_call().elements(), &[20]);
        reporter.end_call();

        assert!(!reporter.justifies(&25), "only 30 is held now");
        assert!(reporter.justifies(&30));
        reporter.update(30, -1);
        assert!(!reporter.holds_any());
        assert!(!reporter.justifies(&u64::MAX));
    }

    #[test]
    fn reporter_justifies_with_a_partial_order() {
        use crate::order::Product;
        let reporter = ProgressReporter::<Product<u64, u32>>::new();
        reporter.update(Product::new(0, 9), 1);
        reporter.update(Product::new(1, 7), 1);
        // (0, 9) comes first in `Ord` but is not at or before (1, 8): the
        // scan must go on to (1, 7), which is.
        assert!(reporter.justifies(&Product::new(1, 8)));
        // Nothing held is at or before (1, 6).
        assert!(!reporter.justifies(&Product::new(1, 6)));
        assert!(reporter.justifies(&Product::new(0, 10)), "after (0, 9)");
        assert!(!reporter.justifies(&Product::new(0, 7)));
    }
}
