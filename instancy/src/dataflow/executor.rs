//! Dataflow execution engine — materializes and runs a dataflow graph.
//!
//! The [`DataflowExecutor`] takes a built dataflow (graph + operator factories)
//! and executes it using an activation-queue reactor model:
//!
//! 1. **Materialization**: Creates typed channels for each edge, invokes operator
//!    factories with their channel endpoints, producing runnable operators.
//! 2. **Execution**: Maintains a ready-queue of operators. Operators are activated
//!    when they have input or are notified. The loop continues until all operators
//!    are done or cancellation is requested.
//!
//! # Cardinality & Lifetime
//!
//! One `DataflowExecutor` per dataflow execution. Created by [`execute()`](crate::execute)
//! and runs until completion or cancellation.

use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{
    ActivationOutcome, ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
};
use crate::error::{Error, Result};
use crate::progress::notificator::Notificator;
use crate::progress::subgraph::ProgressTracker;
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerContext;

// ---------------------------------------------------------------------------
// ExecutorConfig
// ---------------------------------------------------------------------------

/// Configuration for the dataflow executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum activations per step before yielding (prevents starvation).
    pub max_activations_per_step: usize,
    /// Maximum consecutive idle sweeps before declaring quiescence.
    pub max_idle_sweeps: usize,
    /// Maximum sweeps per `poll_run()` call before yielding.
    ///
    /// When the executor has been running for this many sweeps in a single
    /// `poll_run()` invocation, it self-notifies its `WakeHandle` and returns
    /// `Poll::Pending`. This ensures CPU-active dataflows don't monopolize
    /// a pool thread indefinitely, enabling cooperative multiplexing with
    /// other dataflows on the same thread.
    ///
    /// Set to `0` to disable the budget (poll until completion/quiescence).
    pub max_sweeps_per_poll: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_activations_per_step: 1024,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 64,
        }
    }
}

// ---------------------------------------------------------------------------
// SweepOutcome
// ---------------------------------------------------------------------------

/// Result of a single [`DataflowExecutor::run_one_sweep`] call.
///
/// # What is a "sweep"?
///
/// A **sweep** is one full pass through the executor's ready queue — it
/// activates each queued operator once (up to `max_activations_per_step`),
/// then propagates progress and updates quiescence counters. Think of it
/// as a single clock tick in a reactor loop:
///
/// - **Sweep** = scan all ready operators, activate each, propagate progress,
///   check quiescence.
/// - **Run** = repeated sweeps until completion or idle.
///
/// `SweepOutcome` tells the caller what happened in that single pass, so
/// `run()` (sync) and `poll_run()` (async) can each decide how to react
/// between sweeps — sleep, yield, or return `Pending`.
enum SweepOutcome {
    /// All operators are done — the dataflow completed normally.
    Completed,
    /// No operator made progress for `max_idle_sweeps` consecutive sweeps
    /// and no external inputs are open — the dataflow is quiescent.
    Quiescent,
    /// At least one operator made progress. More sweeps are likely needed.
    MadeProgress,
    /// No operator made progress this sweep, but the quiescence threshold
    /// has not been reached yet. More sweeps may be needed.
    Idle,
    /// The executor hit the idle threshold but external inputs are still open.
    /// In sync mode, the caller should sleep briefly. In async mode, the
    /// caller should register a waker and return Pending.
    WaitingForInput,
}

// ---------------------------------------------------------------------------
// DataflowExecutor
// ---------------------------------------------------------------------------

/// The runtime execution engine for a single dataflow.
///
/// Owns all operators and drives their activation until the dataflow completes
/// or is cancelled. Generic over `T` (timestamp type) to support progress tracking.
pub struct DataflowExecutor<T: Timestamp = u64> {
    /// Running operators, indexed by operator index.
    operators: Vec<Box<dyn SchedulableOperator>>,
    /// Ready queue: operator indices that need activation.
    ready_queue: VecDeque<usize>,
    /// Tracks which operator positions are currently in the ready queue (O(1) membership check).
    in_queue: Vec<bool>,
    /// Tracks which operators are done.
    done: Vec<bool>,
    /// Maps operator index → position in the operators vec (used for precise activation).
    index_to_pos: Vec<usize>,
    /// Configuration.
    config: ExecutorConfig,
    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,
    /// Optional progress tracker — when set, frontier advances trigger activation.
    progress_tracker: Option<ProgressTracker<T>>,
    /// Per-operator notificators, indexed by position in `operators` vec.
    /// Only populated when a progress tracker is attached.
    notificators: Vec<Option<Notificator<T>>>,
    /// Registered probes: (operator_index, probe_handle).
    /// Updated during progress propagation with the operator's input frontier.
    probes: Vec<(usize, ProbeHandle<T>)>,
    /// Number of external input sources still open (e.g., channel-based inputs
    /// fed by the caller via `DataflowHandle`). While > 0, the executor will
    /// not declare quiescence — it waits for external data instead.
    external_inputs_open: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Per-dataflow wake handle for async executor notifications.
    /// Channels notify this handle when data arrives or capacity is freed,
    /// allowing the executor to sleep (return `Poll::Pending`) when idle
    /// instead of busy-polling.
    wake_handle: WakeHandle,
    /// Count of consecutive idle sweeps (used for quiescence detection).
    /// Persisted across `poll_run` calls so the executor remembers how long
    /// it has been idle.
    consecutive_idle: usize,
    /// Phantom for the timestamp type.
    _phantom: PhantomData<T>,
}

impl<T: Timestamp> DataflowExecutor<T> {
    /// Materialize a dataflow from its graph, factories, and channel factories.
    ///
    /// This is the bridge between the build phase and the execution phase:
    /// - Creates channels for each edge using the channel factories.
    /// - Invokes operator factories with their assigned channel endpoints.
    /// - Returns a ready-to-run executor.
    ///
    /// If `external_wake_handle` is provided, the executor uses it instead of
    /// creating its own. This allows the runtime to share the same WakeHandle
    /// with InputSenders and CancellationTokens created before materialization.
    pub fn materialize(
        graph: &DataflowGraph,
        mut operator_factories: Vec<(usize, OperatorFactory)>,
        channel_factories: Vec<(usize, ChannelFactory)>,
        config: ExecutorConfig,
        cancel: CancellationToken,
        external_wake_handle: Option<WakeHandle>,
        worker_context: WorkerContext,
    ) -> Result<Self> {
        let edges = graph.edges();
        let feedback_edges = graph.feedback_edges();
        let total_edge_count = edges.len() + feedback_edges.len();

        // Phase 1: Create channels for each edge (regular + feedback).
        // channel_endpoints[edge_index] = (push_end, pull_end)
        // Regular edges: indices 0..edges.len()
        // Feedback edges: indices edges.len()..total_edge_count
        let mut push_ends: Vec<Option<Box<dyn std::any::Any + Send>>> = Vec::new();
        let mut pull_ends: Vec<Option<Box<dyn std::any::Any + Send>>> = Vec::new();

        // Channel factories are indexed by edge index.
        // Create a map from edge_index → factory.
        let mut factory_map: std::collections::HashMap<usize, ChannelFactory> =
            channel_factories.into_iter().collect();

        let wake_handle = external_wake_handle.unwrap_or_else(WakeHandle::new);

        for edge_idx in 0..total_edge_count {
            let mut factory = factory_map.remove(&edge_idx).ok_or_else(|| {
                Error::Custom(format!("No channel factory for edge index {edge_idx}"))
            })?;
            let capacity = 1024; // TODO: make configurable per edge
            let (push, pull) = factory.build(&worker_context, capacity, Some(wake_handle.clone()));
            push_ends.push(Some(push));
            pull_ends.push(Some(pull));
        }

        // Phase 2: Collect channel endpoints per operator.
        // For each operator, gather:
        //   - input_pullers: pull ends of edges targeting this operator (by target port)
        //   - output_pushers: push ends of edges sourced from this operator (by source port)
        let mut op_input_pullers: std::collections::HashMap<usize, Vec<(usize, Box<dyn std::any::Any + Send>)>> =
            std::collections::HashMap::new();
        let mut op_output_pushers: std::collections::HashMap<usize, Vec<(usize, Box<dyn std::any::Any + Send>)>> =
            std::collections::HashMap::new();

        // Process regular edges
        for (edge_idx, edge) in edges.iter().enumerate() {
            let pull = pull_ends[edge_idx].take().unwrap();
            let push = push_ends[edge_idx].take().unwrap();

            op_input_pullers
                .entry(edge.target.operator_index)
                .or_default()
                .push((edge.target.slot_index, pull));

            op_output_pushers
                .entry(edge.source.operator_index)
                .or_default()
                .push((edge.source.slot_index, push));
        }

        // Process feedback edges (same as regular edges for materialization purposes)
        for (i, edge) in feedback_edges.iter().enumerate() {
            let edge_idx = edges.len() + i;
            let pull = pull_ends[edge_idx].take().unwrap();
            let push = push_ends[edge_idx].take().unwrap();

            op_input_pullers
                .entry(edge.target.operator_index)
                .or_default()
                .push((edge.target.slot_index, pull));

            op_output_pushers
                .entry(edge.source.operator_index)
                .or_default()
                .push((edge.source.slot_index, push));
        }

        // Phase 3: Invoke operator factories with their endpoints.
        // Sort factories by operator index for deterministic creation.
        operator_factories.sort_by_key(|(idx, _)| *idx);

        let mut operators: Vec<Box<dyn SchedulableOperator>> = Vec::new();
        let mut index_to_pos: Vec<usize> = Vec::new();
        let max_index = operator_factories
            .iter()
            .map(|(idx, _)| *idx)
            .max()
            .unwrap_or(0);
        index_to_pos.resize(max_index + 1, usize::MAX);

        // TODO(multi-worker): For N-worker materialization, this loop runs N times
        // on the same factories. Currently all factories are SingleUseFactory (panics
        // on 2nd call). PR 39 will: (1) check is_replayable() for all factories and
        // return an error if N>1 with non-replayable factories, (2) change ownership
        // model to &mut LogicalDataflow so factories survive across materializations.
        for (op_idx, mut factory) in operator_factories {
            // Collect input pullers sorted by port index.
            let mut inputs = op_input_pullers.remove(&op_idx).unwrap_or_default();
            inputs.sort_by_key(|(port, _)| *port);
            let input_pullers: Vec<Box<dyn std::any::Any + Send>> =
                inputs.into_iter().map(|(_, pull)| pull).collect();

            // Collect output pushers sorted by port index.
            let mut outputs = op_output_pushers.remove(&op_idx).unwrap_or_default();
            outputs.sort_by_key(|(port, _)| *port);
            let output_pushers: Vec<Box<dyn std::any::Any + Send>> =
                outputs.into_iter().map(|(_, push)| push).collect();

            let endpoints = ChannelEndpoints {
                input_pullers,
                output_pushers,
            };

            let operator = factory.build(&worker_context, endpoints);
            let pos = operators.len();
            if op_idx < index_to_pos.len() {
                index_to_pos[op_idx] = pos;
            }
            operators.push(operator);
        }

        let done = vec![false; operators.len()];

        // Initially, all operators are ready (they may have initial input or
        // need to produce initial output like sources).
        let ready_queue: VecDeque<usize> = (0..operators.len()).collect();
        let in_queue = vec![true; operators.len()];

        Ok(Self {
            operators,
            ready_queue,
            in_queue,
            done,
            index_to_pos,
            config,
            cancel,
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle,
            consecutive_idle: 0,
            _phantom: PhantomData,
        })
    }


    /// Get a shared reference to the external inputs counter.
    ///
    /// Used by `ChannelSourceOperator` to decrement when its channel closes.
    pub fn external_inputs_counter(&self) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
        std::sync::Arc::clone(&self.external_inputs_open)
    }

    /// Set the number of external input sources.
    ///
    /// While this count is > 0, the executor will not declare quiescence,
    /// allowing channel-based input operators to wait for external data.
    pub fn set_external_inputs_open(&self, count: usize) {
        self.external_inputs_open
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get the wake handle for this executor.
    ///
    /// Share this handle with channels so they can notify the executor when
    /// data arrives or capacity is freed. The executor uses the handle to
    /// sleep (return `Poll::Pending`) when idle instead of busy-polling.
    ///
    /// When an external `WakeHandle` is passed to `materialize()`, this returns
    /// that same handle — ensuring InputSenders, CancellationTokens, and
    /// internal channels all share a single notification path.
    pub fn wake_handle(&self) -> WakeHandle {
        self.wake_handle.clone()
    }

    /// Replace the executor's external inputs counter with a shared one.
    ///
    /// This allows operators (e.g., `ChannelSourceOperator`) and the executor
    /// to share the same `Arc<AtomicUsize>`, so operator decrements are
    /// visible to the executor's quiescence check.
    pub fn replace_external_inputs_counter(
        &mut self,
        counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.external_inputs_open = counter;
    }

    /// Attach an initialized progress tracker to enable frontier-driven activation.
    ///
    /// When a progress tracker is attached, after each activation batch the executor
    /// propagates progress and adds operators with dirty frontiers to the ready queue.
    /// Also creates per-operator Notificators initialized with current input frontiers.
    ///
    /// # Panics
    ///
    /// Panics if the tracker has not been initialized via `initialize()`.
    pub fn set_progress_tracker(&mut self, tracker: ProgressTracker<T>) {
        assert!(
            tracker.is_initialized(),
            "ProgressTracker must be initialized before attaching to executor"
        );

        // Create per-operator notificators with initial frontiers.
        let mut notificators: Vec<Option<Notificator<T>>> = Vec::with_capacity(self.operators.len());
        for (_pos, op) in self.operators.iter().enumerate() {
            let op_idx = op.index();
            let frontier = tracker.operator_input_frontier_meet(op_idx);
            notificators.push(Some(Notificator::new(frontier)));
        }
        self.notificators = notificators;
        self.progress_tracker = Some(tracker);
    }

    /// Register a probe handle to observe the frontier at a specific operator.
    ///
    /// The probe's frontier is updated during each progress propagation step
    /// with the operator's input frontier (the combined frontier of all inputs).
    /// On registration, the probe is immediately seeded with the current frontier.
    pub fn register_probe(&mut self, operator_index: usize, probe: ProbeHandle<T>) {
        // Seed the probe with the current frontier from the tracker.
        if let Some(ref tracker) = self.progress_tracker {
            let frontier = tracker.operator_input_frontier_meet(operator_index);
            probe.update_frontier(&frontier);
        }
        self.probes.push((operator_index, probe));
    }

    /// Propagate progress and enqueue operators whose frontiers changed.
    ///
    /// Called after each activation batch when a progress tracker is present.
    /// Propagates progress and updates operator frontiers/notificators.
    ///
    /// After operators produce/consume data, capabilities change. This method:
    /// 1. Collects capability changes from all operators' ProgressReporters.
    /// 2. Runs the reachability tracker to compute new frontiers.
    /// 3. Delivers frontier updates to both:
    ///    a. The executor's per-operator notificators (legacy path for regular operators).
    ///    b. The operators themselves via `update_input_frontier()` (for notify-capable
    ///       operators like WiredUnaryNotifyOperator that manage their own notificator).
    /// 4. Re-enqueues operators that have ready notifications.
    ///
    /// Returns true if any operator was newly activated.
    fn propagate_progress(&mut self) -> bool {
        let tracker = match &mut self.progress_tracker {
            Some(t) => t,
            None => return false,
        };

        let dirty: Vec<usize> = tracker.propagate().to_vec();

        // Collect frontier updates for dirty operators before releasing tracker borrow.
        let frontier_updates: Vec<(usize, _)> = dirty
            .iter()
            .filter_map(|&op_idx| {
                if op_idx < self.index_to_pos.len() {
                    let pos = self.index_to_pos[op_idx];
                    if pos != usize::MAX && pos < self.operators.len() {
                        let frontier = tracker.operator_input_frontier_meet(op_idx);
                        return Some((pos, frontier));
                    }
                }
                None
            })
            .collect();

        // Apply frontier updates to:
        // 1. Executor-owned notificators (legacy path, for operators that don't
        //    override update_input_frontier — i.e., regular unary/binary operators).
        // 2. The operators themselves (for notify-capable operators that manage
        //    their own internal notificator). The default implementation is a no-op,
        //    so this is safe to call on all operators.
        for (pos, frontier) in &frontier_updates {
            // Legacy path: executor-owned notificator
            if *pos < self.notificators.len() {
                if let Some(notificator) = &mut self.notificators[*pos] {
                    notificator.update_frontier(frontier);
                }
            }
            // New path: operator-owned notificator (WiredUnaryNotifyOperator etc.)
            // The operator downcasts &dyn Any to &Antichain<T> internally.
            self.operators[*pos].update_input_frontier(frontier);
        }

        let mut activated = false;

        // Enqueue dirty operators
        for op_idx in dirty {
            if op_idx < self.index_to_pos.len() {
                let pos = self.index_to_pos[op_idx];
                if pos != usize::MAX && !self.done[pos] && !self.in_queue[pos] {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    activated = true;
                }
            }
        }

        // Check if any operator has ready notifications and should be re-enqueued.
        // This covers two cases:
        // 1. Executor-owned notificators (legacy): notifications fired by the
        //    frontier update above.
        // 2. Operator-owned notificators (new): the operator's has_ready_notifications()
        //    returns true after update_input_frontier() fired notifications.
        for pos in 0..self.operators.len() {
            if self.done[pos] || self.in_queue[pos] {
                continue;
            }
            // Check executor-owned notificator (legacy path)
            let executor_has_ready = if pos < self.notificators.len() {
                self.notificators
                    .get(pos)
                    .and_then(|n| n.as_ref())
                    .map_or(false, |n| n.has_ready())
            } else {
                false
            };
            // Check operator-owned notificator (new path)
            let operator_has_ready = self.operators[pos].has_ready_notifications();

            if executor_has_ready || operator_has_ready {
                self.ready_queue.push_back(pos);
                self.in_queue[pos] = true;
                activated = true;
            }
        }

        // Update registered probes with current frontiers.
        if !self.probes.is_empty() {
            let tracker = self.progress_tracker.as_ref().unwrap();
            for (op_idx, probe) in &self.probes {
                let frontier = tracker.operator_input_frontier_meet(*op_idx);
                probe.update_frontier(&frontier);
            }
        }

        activated
    }

    /// Run the dataflow to completion or cancellation using a simple single-threaded
    /// activation loop. This is a **testing/validation helper** — it does NOT use the
    /// Worker Thread Pool. In production, the orchestrator event loop drives operator
    /// activations via the TaskScheduler and Worker Thread Pool (see §5.4 in DESIGN.md).
    ///
    /// Returns `Ok(true)` on normal completion (all operators done).
    /// Returns `Ok(false)` on quiescence (no operator can make progress,
    /// but not all operators are done — e.g., waiting for external input).
    /// Returns `Err(Error::Cancelled)` if the cancellation token fires.
    /// Returns `Err(...)` if any operator produces an error.
    pub fn run(&mut self) -> Result<bool> {
        loop {
            match self.run_one_sweep()? {
                SweepOutcome::Completed => return Ok(true),
                SweepOutcome::Quiescent => return Ok(false),
                SweepOutcome::MadeProgress => {
                    // Continue immediately — there may be more work.
                }
                SweepOutcome::Idle => {
                    // No progress this sweep but not quiescent yet.
                    // In sync mode, just loop again (tight poll).
                }
                SweepOutcome::WaitingForInput => {
                    // External inputs still open. In sync mode, sleep briefly
                    // to avoid busy-polling.
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    /// Execute one sweep of the activation loop.
    ///
    /// A sweep processes the ready queue (up to `max_activations_per_step`),
    /// propagates progress, and updates quiescence counters. Returns a
    /// [`SweepOutcome`] indicating what happened.
    ///
    /// This is the core building block for both sync [`run()`](Self::run)
    /// and async [`poll_run()`](#method.poll_run).
    fn run_one_sweep(&mut self) -> Result<SweepOutcome> {
        // Check cancellation.
        self.cancel.check()?;

        // If all operators are done, we're finished.
        if self.done.iter().all(|&d| d) {
            return Ok(SweepOutcome::Completed);
        }

        // If the ready queue is empty, re-populate it with non-done operators.
        if self.ready_queue.is_empty() {
            for pos in 0..self.operators.len() {
                if !self.done[pos] {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                }
            }
            // If still empty, all operators are done.
            if self.ready_queue.is_empty() {
                return Ok(SweepOutcome::Completed);
            }
        }

        // Process the ready queue.
        let mut any_progress = false;

        let batch_size = self.ready_queue.len().min(self.config.max_activations_per_step);
        for _ in 0..batch_size {
            let Some(pos) = self.ready_queue.pop_front() else {
                break;
            };
            self.in_queue[pos] = false;

            if self.done[pos] {
                continue;
            }

            let outcome = self.operators[pos].activate()?;

            match outcome {
                ActivationOutcome::MadeProgress => {
                    any_progress = true;
                    // Re-queue: operator may have more work.
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                }
                ActivationOutcome::Idle => {
                    // Don't re-queue. Will be re-added on next sweep.
                }
                ActivationOutcome::BlockedOnBackpressure => {
                    // Re-queue at the back; downstream needs to drain first.
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    any_progress = true;
                }
                ActivationOutcome::Done => {
                    self.done[pos] = true;
                    self.propagate_completion(pos);
                    any_progress = true;
                }
            }
        }

        if any_progress {
            self.consecutive_idle = 0;
        } else {
            self.consecutive_idle += 1;
        }

        // After each batch, propagate progress and enqueue dirty operators.
        if self.propagate_progress() {
            self.consecutive_idle = 0;
        }

        if self.consecutive_idle >= self.config.max_idle_sweeps {
            // Check if external inputs are still open — if so, don't
            // declare quiescence, but signal the caller to wait.
            if self.external_inputs_open.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                self.consecutive_idle = 0;
                return Ok(SweepOutcome::WaitingForInput);
            }

            // If a progress tracker is attached and reports completion,
            // force-close remaining operators (they are in a feedback cycle
            // that quiesced). Treat as normal completion.
            //
            // With cross-worker progress exchange, the tracker's is_completed()
            // reflects GLOBAL state (all workers' capabilities), so this is safe
            // for both single-worker and multi-worker exchange dataflows.
            //
            // Defense-in-depth: also verify no peer progress is pending.
            // "Peer" = another logical worker/executor in the same dataflow,
            // regardless of physical location (same process or remote node).
            // After 64+ idle sweeps this should always be empty, but checking
            // guards against the narrow race where a peer sends progress
            // between our last propagate() and this force-close decision.
            if let Some(ref tracker) = self.progress_tracker {
                if tracker.is_completed() && !tracker.has_pending_peer_progress() {
                    for pos in 0..self.operators.len() {
                        if !self.done[pos] {
                            self.operators[pos].close_inputs();
                            self.done[pos] = true;
                        }
                    }
                    return Ok(SweepOutcome::Completed);
                }
            }

            // Quiescent — no operator made progress.
            return Ok(SweepOutcome::Quiescent);
        }

        if any_progress {
            Ok(SweepOutcome::MadeProgress)
        } else {
            Ok(SweepOutcome::Idle)
        }
    }

    /// Poll the executor as a [`Future`], driving sweeps until the dataflow
    /// completes, becomes quiescent, exhausts its poll budget, or needs to
    /// wait for input/notifications.
    ///
    /// # Async execution model
    ///
    /// Instead of busy-looping, the executor registers a waker with its
    /// [`WakeHandle`]. When channels push data, free capacity, or close,
    /// they notify the handle, which wakes this future to run more sweeps.
    ///
    /// # Poll budget
    ///
    /// To enable cooperative multiplexing on shared pool threads, the executor
    /// limits each `poll_run()` call to `config.max_sweeps_per_poll` sweeps.
    /// On budget exhaustion, it self-notifies its WakeHandle (ensuring it will
    /// be re-polled) and returns `Poll::Pending`. This lets other dataflow
    /// executors run on the same thread.
    ///
    /// # Race-safe protocol
    ///
    /// 1. Run sweeps until idle or budget exhausted
    /// 2. Register the waker
    /// 3. Re-check for notifications (handles the race where a channel
    ///    pushed data between step 1 and step 2)
    /// 4. Only return `Pending` if no notification is pending
    ///
    /// Returns `Poll::Ready(Ok(true))` on completion, `Poll::Ready(Ok(false))`
    /// on quiescence, or `Poll::Ready(Err(...))` on error/cancellation.
    pub fn poll_run(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        let budget = self.config.max_sweeps_per_poll;
        let mut sweeps_this_poll: usize = 0;

        loop {
            match self.run_one_sweep() {
                Ok(SweepOutcome::Completed) => {
                    self.wake_handle.clear_waker();
                    return Poll::Ready(Ok(true));
                }
                Ok(SweepOutcome::Quiescent) => {
                    self.wake_handle.clear_waker();
                    return Poll::Ready(Ok(false));
                }
                Ok(SweepOutcome::MadeProgress) => {
                    sweeps_this_poll += 1;
                    // Check poll budget (0 = unlimited)
                    if budget > 0 && sweeps_this_poll >= budget {
                        // Budget exhausted — self-notify so we get re-polled,
                        // then yield to let other executors run.
                        self.wake_handle.register_waker(cx.waker());
                        self.wake_handle.notify();
                        return Poll::Pending;
                    }
                    continue;
                }
                Ok(SweepOutcome::Idle) => {
                    sweeps_this_poll += 1;
                    if budget > 0 && sweeps_this_poll >= budget {
                        self.wake_handle.register_waker(cx.waker());
                        self.wake_handle.notify();
                        return Poll::Pending;
                    }
                    continue;
                }
                Ok(SweepOutcome::WaitingForInput) => {
                    // External inputs still open. Register waker first,
                    // then re-check for notifications (race-safe protocol).
                    self.wake_handle.register_waker(cx.waker());

                    // Race-safe re-check: if a notification arrived between
                    // the sweep and register_waker, we must sweep again.
                    if self.wake_handle.take_notification() {
                        continue;
                    }
                    return Poll::Pending;
                }
                Err(e) => {
                    self.wake_handle.clear_waker();
                    return Poll::Ready(Err(e));
                }
            }
        }
    }

    /// After an operator completes, check if downstream operators should
    /// have their inputs closed.
    fn propagate_completion(&mut self, completed_pos: usize) {
        let completed_idx = self.operators[completed_pos].index();

        // For each other operator, check if all its upstream operators are done.
        // This is a simplified approach — a full implementation would use the
        // graph edge topology. For now, close inputs of operators whose upstream
        // sources are all done.
        //
        // TODO: Use graph topology for precise propagation.
        // For now, we rely on channels: when the push end is dropped (operator done),
        // the pull end sees is_exhausted() = true, which the operator handles.
        let _ = completed_idx;
    }

    /// Get the number of operators.
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Check if all operators have completed.
    pub fn is_complete(&self) -> bool {
        self.done.iter().all(|&d| d)
    }

    /// Get the number of completed operators.
    pub fn completed_count(&self) -> usize {
        self.done.iter().filter(|&&d| d).count()
    }

    /// Request a notification for the operator at the given position
    /// when the frontier advances past the given time.
    ///
    /// The notification will fire once the input frontier meets (across all ports)
    /// advances past `time`.
    pub fn notify_at(&mut self, pos: usize, time: T) {
        if let Some(Some(notificator)) = self.notificators.get_mut(pos) {
            notificator.notify_at(time);
        }
    }

    /// Drain ready notifications for the operator at the given position.
    ///
    /// Returns an iterator of timestamps whose frontier has advanced past.
    pub fn drain_notifications(&mut self, pos: usize) -> Vec<T> {
        if let Some(Some(notificator)) = self.notificators.get_mut(pos) {
            let mut times = Vec::new();
            while let Some(fired) = notificator.next() {
                times.push(fired.into_time());
            }
            times
        } else {
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Future implementation
// ---------------------------------------------------------------------------

/// Implements [`Future`] so the executor can be driven by an async runtime.
///
/// Resolves to `Ok(true)` on normal completion, `Ok(false)` on quiescence,
/// or `Err(...)` on error/cancellation. Between polls, the executor sleeps
/// until a channel notifies its [`WakeHandle`].
impl<T: Timestamp> Future for DataflowExecutor<T> {
    type Output = Result<bool>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: DataflowExecutor is Unpin (no self-referential fields).
        let this = self.get_mut();
        this.poll_run(cx)
    }
}

// DataflowExecutor is Unpin because all its fields are either behind
// indirection (Arc, Box, Vec) or are plain data. The PhantomData<T> makes
// auto-Unpin conservative, but T only appears as a type parameter — no
// T values are stored inline that could create self-referential pointers.
impl<T: Timestamp> Unpin for DataflowExecutor<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::schedulable::ActivationOutcome;

    /// A trivial operator that counts activations and becomes done after N.
    struct CountingOperator {
        name: String,
        index: usize,
        region_id: crate::dataflow::region::RegionId,
        remaining: usize,
    }

    impl SchedulableOperator for CountingOperator {
        fn activate(&mut self) -> Result<ActivationOutcome> {
            if self.remaining == 0 {
                return Ok(ActivationOutcome::Done);
            }
            self.remaining -= 1;
            if self.remaining == 0 {
                Ok(ActivationOutcome::Done)
            } else {
                Ok(ActivationOutcome::MadeProgress)
            }
        }

        fn is_done(&self) -> bool {
            self.remaining == 0
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn index(&self) -> usize {
            self.index
        }

        fn region_id(&self) -> crate::dataflow::region::RegionId {
            self.region_id
        }

        fn close_inputs(&mut self) {
            self.remaining = 0;
        }
    }

    /// An operator that always returns Idle — never makes progress, never finishes.
    /// Used for testing async waiting behavior.
    struct IdleOperator {
        index: usize,
        region_id: crate::dataflow::region::RegionId,
        closed: bool,
    }

    impl SchedulableOperator for IdleOperator {
        fn activate(&mut self) -> Result<ActivationOutcome> {
            if self.closed {
                return Ok(ActivationOutcome::Done);
            }
            Ok(ActivationOutcome::Idle)
        }

        fn is_done(&self) -> bool {
            self.closed
        }

        fn name(&self) -> &str {
            "idle"
        }

        fn index(&self) -> usize {
            self.index
        }

        fn region_id(&self) -> crate::dataflow::region::RegionId {
            self.region_id
        }

        fn close_inputs(&mut self) {
            self.closed = true;
        }
    }

    #[test]
    fn executor_runs_single_operator_to_completion() {
        let mut executor = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "counter".into(),
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 3,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = executor.run();
        assert_eq!(result.unwrap(), true);
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_respects_cancellation() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let mut executor = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "infinite".into(),
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: usize::MAX,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel,
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = executor.run();
        assert!(matches!(result, Err(Error::Cancelled)));
    }

    #[test]
    fn executor_handles_multiple_operators() {
        let mut executor = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "a".into(),
                    index: 0,
                    region_id: crate::dataflow::region::RegionId::new(0),
                    remaining: 2,
                }),
                Box::new(CountingOperator {
                    name: "b".into(),
                    index: 1,
                    region_id: crate::dataflow::region::RegionId::new(0),
                    remaining: 5,
                }),
            ],
            ready_queue: VecDeque::from([0, 1]),
            in_queue: vec![true, true],
            done: vec![false, false],
            index_to_pos: vec![0, 1],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = executor.run();
        assert_eq!(result.unwrap(), true);
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_empty_dataflow_completes_immediately() {
        let mut executor = DataflowExecutor {
            operators: vec![],
            ready_queue: VecDeque::new(),
            in_queue: vec![],
            done: vec![],
            index_to_pos: vec![],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = executor.run();
        assert_eq!(result.unwrap(), true);
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_idle_operator_reaches_quiescence() {
        struct AlwaysIdle;
        impl SchedulableOperator for AlwaysIdle {
            fn activate(&mut self) -> Result<ActivationOutcome> {
                Ok(ActivationOutcome::Idle)
            }
            fn is_done(&self) -> bool { false }
            fn name(&self) -> &str { "idle" }
            fn index(&self) -> usize { 0 }
            fn region_id(&self) -> crate::dataflow::region::RegionId {
                crate::dataflow::region::RegionId::new(0)
            }
            fn close_inputs(&mut self) {}
        }

        let mut executor = DataflowExecutor {
            operators: vec![Box::new(AlwaysIdle)],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig { max_activations_per_step: 10, max_idle_sweeps: 3, max_sweeps_per_poll: 0 },
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Should terminate via quiescence, not infinite loop.
        // Returns Ok(false) because AlwaysIdle never completes.
        let result = executor.run();
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn executor_runs_wired_pipeline() {
        use crate::communication::allocator::ChannelAllocator;
        use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
        use crate::dataflow::region::RegionId;
        use crate::dataflow::wired_operators::{WiredSinkOperator, WiredSourceOperator, WiredUnaryOperator};

        let mut alloc = ChannelAllocator::new();
        let ch1 = alloc.allocate::<u64, i32, ()>();
        let ch2 = alloc.allocate::<u64, i32, ()>();

        let source: Box<dyn SchedulableOperator> = Box::new(WiredSourceOperator::new(
            "source", 0, RegionId::new(0),
            vec![(0u64, vec![1, 2, 3]), (1u64, vec![10, 20])],
            ch1.pusher,
        ));

        let double: Box<dyn SchedulableOperator> = Box::new(WiredUnaryOperator::new(
            "double", 1, RegionId::new(0),
            move |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            },
            ch1.puller,
            ch2.pusher,
        ));

        let sink: Box<dyn SchedulableOperator> = Box::new(WiredSinkOperator::new(
            "sink", 2, RegionId::new(0),
            ch2.puller,
        ));

        let mut executor = DataflowExecutor {
            operators: vec![source, double, sink],
            ready_queue: VecDeque::from([0, 1, 2]),
            in_queue: vec![true, true, true],
            done: vec![false, false, false],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = executor.run();
        assert_eq!(result.unwrap(), true);
        assert!(executor.is_complete());

        // Verify the sink collected the right data by checking it's done.
        assert_eq!(executor.completed_count(), 3);
    }

    #[test]
    fn executor_with_progress_tracker_attached() {
        use crate::progress::operate::PortConnectivity;
        use crate::progress::subgraph::SubgraphBuilder;

        // Build a minimal subgraph with one operator
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder.add_operator(
            1, // operator index
            "op",
            1, // inputs
            1, // outputs
            PortConnectivity::identity(0u64),
        );

        let mut tracker = builder.build();
        tracker.initialize();

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 1,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 2,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![usize::MAX, 0], // index 0 unused, index 1 → pos 0
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Attach progress tracker
        executor.set_progress_tracker(tracker);
        assert!(executor.progress_tracker.is_some());

        // Run should still complete normally
        let result = executor.run();
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn notify_at_and_drain_notifications() {
        use crate::progress::frontier::Antichain;
        use crate::progress::notificator::Notificator;

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 1,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 1,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![usize::MAX, 0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: vec![Some(Notificator::new(Antichain::from_elem(0u64)))],
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Request notification at time 5
        executor.notify_at(0, 5);

        // Nothing ready yet (frontier is still at 0, meaning time 5 not yet complete)
        let ready = executor.drain_notifications(0);
        assert!(ready.is_empty());

        // Advance frontier past time 5 (empty frontier = all times complete)
        if let Some(Some(notificator)) = executor.notificators.get_mut(0) {
            notificator.update_frontier(&Antichain::new());
        }

        // Now notification should be ready
        let ready = executor.drain_notifications(0);
        assert_eq!(ready, vec![5]);

        // Draining again yields nothing
        let ready = executor.drain_notifications(0);
        assert!(ready.is_empty());
    }

    #[test]
    fn notify_at_fires_immediately_when_frontier_already_past() {
        use crate::progress::frontier::Antichain;
        use crate::progress::notificator::Notificator;

        // Start with empty frontier (all times complete)
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 1,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 1,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![usize::MAX, 0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: vec![Some(Notificator::new(Antichain::new()))],
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Request notification at time 3 — frontier already past, fires immediately
        executor.notify_at(0, 3);
        let ready = executor.drain_notifications(0);
        assert_eq!(ready, vec![3]);
    }

    // -----------------------------------------------------------------------
    // Async executor / poll_run tests
    // -----------------------------------------------------------------------

    #[test]
    fn poll_run_completes_simple_dataflow() {
        // A dataflow with one operator that finishes in 2 activations
        // should resolve to Ready(Ok(true)) when polled.
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll, Wake};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: std::sync::Arc<Self>) {}
        }

        let waker = std::sync::Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "counter".into(),
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 2,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Poll the Future — should complete in one poll since all operators finish.
        let result = Pin::new(&mut executor).poll(&mut cx);
        assert!(matches!(result, Poll::Ready(Ok(true))));
    }

    #[test]
    fn poll_run_returns_pending_with_external_inputs() {
        // When external inputs are open, the executor should return Pending
        // (not quiescence) after reaching idle threshold.
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll, Wake};

        struct TrackingWaker {
            woken: std::sync::atomic::AtomicBool,
        }
        impl Wake for TrackingWaker {
            fn wake(self: std::sync::Arc<Self>) {
                self.woken.store(true, std::sync::atomic::Ordering::Release);
            }
        }

        let tracking = std::sync::Arc::new(TrackingWaker {
            woken: std::sync::atomic::AtomicBool::new(false),
        });
        let waker = tracking.clone().into();
        let mut cx = Context::from_waker(&waker);

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(IdleOperator {
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                closed: false,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 1024,
                max_idle_sweeps: 1, // reach idle threshold quickly
                max_sweeps_per_poll: 0, // no budget limit for this test
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // IdleOperator always returns Idle → hits idle threshold → WaitingForInput.
        // poll_run should register waker and return Pending.
        let mut got_pending = false;
        for _ in 0..200 {
            match Pin::new(&mut executor).poll(&mut cx) {
                Poll::Pending => {
                    got_pending = true;
                    break;
                }
                Poll::Ready(_) => {
                    panic!("Should not have resolved yet — external inputs open");
                }
            }
        }
        assert!(got_pending, "Expected Pending due to external inputs");
    }

    #[test]
    fn poll_run_wakes_on_notification() {
        // Verify that after returning Pending, notifying the wake handle
        // wakes the registered waker.
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll, Wake};

        struct TrackingWaker {
            woken: std::sync::atomic::AtomicBool,
        }
        impl Wake for TrackingWaker {
            fn wake(self: std::sync::Arc<Self>) {
                self.woken.store(true, std::sync::atomic::Ordering::Release);
            }
        }

        let tracking = std::sync::Arc::new(TrackingWaker {
            woken: std::sync::atomic::AtomicBool::new(false),
        });
        let waker = tracking.clone().into();
        let mut cx = Context::from_waker(&waker);

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(IdleOperator {
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                closed: false,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 1024,
                max_idle_sweeps: 1,
                max_sweeps_per_poll: 0,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let wake = executor.wake_handle();

        // Drive to Pending
        let mut reached_pending = false;
        for _ in 0..200 {
            if let Poll::Pending = Pin::new(&mut executor).poll(&mut cx) {
                reached_pending = true;
                break;
            }
        }
        assert!(reached_pending);
        assert!(!tracking.woken.load(std::sync::atomic::Ordering::Acquire));

        // Notify via wake handle — waker should fire
        wake.notify();
        assert!(tracking.woken.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn poll_run_cancelled_returns_error() {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll, Wake};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: std::sync::Arc<Self>) {}
        }

        let cancel = CancellationToken::new();
        cancel.cancel();

        let waker = std::sync::Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 0,
                region_id: crate::dataflow::region::RegionId::new(0),
                remaining: 5,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel,
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        let result = Pin::new(&mut executor).poll(&mut cx);
        match result {
            Poll::Ready(Err(Error::Cancelled)) => {} // expected
            other => panic!("Expected Cancelled error, got {:?}", other),
        }
    }

    #[test]
    fn bounded_channel_with_wake_notifies_on_push() {
        // Verify that pushing to a BoundedPush with WakeHandle notifies.
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;
        use crate::dataflow::channels::envelope::Envelope;
        use crate::dataflow::channels::pushpull::Push;

        let wake = WakeHandle::new();
        // Drain initial notification
        wake.take_notification();

        let (mut push, _pull) = bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()));

        // Push should notify
        push.push(Envelope::data(0, vec![1])).unwrap();
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_with_wake_notifies_on_close() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;
        use crate::dataflow::channels::pushpull::Push;

        let wake = WakeHandle::new();
        wake.take_notification();

        let (mut push, _pull) = bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()));

        push.close();
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_with_wake_notifies_on_drop() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;

        let wake = WakeHandle::new();
        wake.take_notification();

        let (push, _pull) = bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()));

        drop(push);
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_pull_notifies_when_freeing_capacity() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;
        use crate::dataflow::channels::envelope::Envelope;
        use crate::dataflow::channels::pushpull::{Pull, Push};

        let wake = WakeHandle::new();

        let (mut push, mut pull) = bounded_channel_with_wake::<u64, i32, ()>(2, Some(wake.clone()));

        // Fill channel to capacity
        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();
        assert!(push.push(Envelope::data(0, vec![3])).is_err()); // backpressure

        // Drain notification from pushes
        wake.take_notification();

        // Pull should notify (channel was full, now has space)
        let _ = pull.pull();
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_pull_no_notify_when_not_full() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;
        use crate::dataflow::channels::envelope::Envelope;
        use crate::dataflow::channels::pushpull::{Pull, Push};

        let wake = WakeHandle::new();

        let (mut push, mut pull) = bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()));

        // Push one item (channel not full)
        push.push(Envelope::data(0, vec![1])).unwrap();
        wake.take_notification(); // drain push notification

        // Pull from non-full channel — should NOT notify
        let _ = pull.pull();
        assert!(!wake.take_notification());
    }

    #[test]
    fn poll_budget_yields_after_max_sweeps() {
        // A CPU-active dataflow (operator always makes progress) should yield
        // after max_sweeps_per_poll sweeps, returning Pending and self-notifying
        // so it gets re-polled later.
        use std::task::{Context, Wake};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: std::sync::Arc<Self>) {}
        }

        let waker: std::task::Waker = std::sync::Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);

        let wake_handle = WakeHandle::new();
        let budget = 4;

        let mut executor = DataflowExecutor::<u64> {
            operators: vec![Box::new(CountingOperator {
                name: "busy".to_string(),
                index: 0,
                region_id: crate::dataflow::region::RegionId(0),
                remaining: 100, // way more than budget
            })],
            ready_queue: std::collections::VecDeque::from(vec![0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 1024,
                max_idle_sweeps: 3,
                max_sweeps_per_poll: budget,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: wake_handle.clone(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // First poll: should yield after `budget` sweeps (Pending, not Ready)
        let result = executor.poll_run(&mut cx);
        assert!(
            result.is_pending(),
            "Expected Pending after budget exhaustion, got {:?}",
            result
        );

        // WakeHandle should have been self-notified so the executor gets re-polled
        assert!(
            wake_handle.take_notification(),
            "Expected self-notification after budget exhaustion"
        );

        // The operator should still have remaining work (not fully consumed)
        assert!(!executor.operators[0].is_done());
    }

    #[test]
    fn poll_budget_zero_means_unlimited() {
        // With max_sweeps_per_poll = 0, the executor should run to completion
        // without yielding, regardless of how many sweeps it takes.
        use std::task::{Context, Poll, Wake};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: std::sync::Arc<Self>) {}
        }

        let waker: std::task::Waker = std::sync::Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);

        let mut executor = DataflowExecutor::<u64> {
            operators: vec![Box::new(CountingOperator {
                name: "busy".to_string(),
                index: 0,
                region_id: crate::dataflow::region::RegionId(0),
                remaining: 50,
            })],
            ready_queue: std::collections::VecDeque::from(vec![0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 1024,
                max_idle_sweeps: 3,
                max_sweeps_per_poll: 0, // unlimited
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Should run to completion without yielding
        let result = executor.poll_run(&mut cx);
        match result {
            Poll::Ready(Ok(true)) => {} // expected — completed
            other => panic!("Expected Ready(Ok(true)), got {:?}", other),
        }
    }
}
