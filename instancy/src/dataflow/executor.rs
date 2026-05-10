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

use std::sync::Arc;
use std::time::Instant;

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::control::{ControlReceiver, ControlSender};
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{
    ActivationOutcome, ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
};
use crate::dataflow::stage::FusedActivationOrder;
use crate::error::{DataflowError, Error, Result};
use crate::metrics::{DataflowMetrics, OperatorMetricsCollector};
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
    /// Whether to catch panics in operator `activate()` calls.
    ///
    /// When enabled, a panicking operator produces an `Error::OperatorPanic`
    /// instead of unwinding through the executor task. This is useful when
    /// operators run user-defined functions (UDFs) that may panic.
    ///
    /// **Caveats:**
    /// - After a caught panic, the operator is in an unknown state. The
    ///   dataflow is terminated immediately (the error propagates up).
    /// - `panic = "abort"` builds cannot catch panics — the process exits.
    /// - The default panic hook still prints the panic message to stderr.
    /// - Partial outputs produced before the panic are not rolled back.
    ///
    /// Default: `false` (panics unwind normally).
    pub catch_panics: bool,
    /// Whether to collect per-operator metrics (activation count, CPU time,
    /// records processed). Overhead is ~1 Instant::now() per activation.
    /// Default: false.
    pub collect_metrics: bool,
    /// When set, cancellation triggers a graceful drain phase instead of
    /// immediate termination. During the drain phase:
    ///
    /// 1. External inputs are closed (no new data accepted).
    /// 2. In-flight data continues to flow through operators.
    /// 3. The executor keeps running sweeps until all operators complete
    ///    or the timeout expires.
    ///
    /// If the drain completes before the timeout, the dataflow returns
    /// `Ok(true)` (normal completion). If the timeout expires, the
    /// dataflow returns `Err(Cancelled)` with the original reason.
    ///
    /// Default: `None` (immediate cancellation).
    pub drain_timeout: Option<std::time::Duration>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_activations_per_step: 1024,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 64,
            catch_panics: false,
            collect_metrics: false,
            drain_timeout: None,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
// Panic-guarded operator activation
// ---------------------------------------------------------------------------

/// Activate an operator, optionally catching panics.
///
/// When `catch_panics` is true, uses `std::panic::catch_unwind` to convert
/// panics into `Error::OperatorPanic`. The operator name is captured BEFORE
/// the activation call to avoid touching poisoned state after a panic.
///
/// When `catch_panics` is false, calls `activate()` directly (zero overhead).
fn activate_operator(
    op: &mut Box<dyn SchedulableOperator>,
    catch_panics: bool,
) -> Result<ActivationOutcome> {
    if catch_panics {
        // Capture name before activation — operator may be poisoned after panic.
        let op_name = op.name().to_string();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| op.activate())) {
            Ok(result) => result,
            Err(payload) => {
                let message = extract_panic_message(&payload);
                Err(Error::OperatorPanic {
                    operator: op_name,
                    worker_index: None,
                    message,
                })
            }
        }
    } else {
        op.activate()
    }
}

/// Extract a human-readable message from a panic payload.
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

// ---------------------------------------------------------------------------
// FusedStageTask
// ---------------------------------------------------------------------------

/// A group of operators from a single stage, activated together in topological order.
///
/// Instead of scheduling individual operators, the executor schedules `FusedStageTask`s.
/// Each task activation runs all its non-done operators in a single multi-pass fused
/// sweep, allowing data to flow through the stage pipeline without inter-activation
/// scheduling overhead.
///
/// # Scheduling unit
///
/// When stage-task mode is enabled, the executor's ready queue holds task indices
/// (not operator positions). One task enqueue → one full stage activation.
#[derive(Debug)]
pub(crate) struct FusedStageTask {
    /// Operator positions (in the executor's `operators` vec) in topological order.
    /// These are physical positions, not logical operator indices.
    pub operator_positions: Vec<usize>,
}

impl FusedStageTask {
    /// Activate all non-done operators in this stage in fused topological order.
    ///
    /// Uses multi-pass semantics: keeps iterating until no operator makes progress
    /// or the budget is exhausted. This matches the existing fused activation behavior.
    ///
    /// Returns `(any_progress, productive_activations, async_waiting_positions)`.
    #[allow(clippy::too_many_arguments)]
    fn activate(
        &self,
        operators: &mut [Box<dyn SchedulableOperator>],
        done: &mut [bool],
        budget: usize,
        worker_index: usize,
        control_sender: Option<&ControlSender>,
        catch_panics: bool,
        op_collectors: &[Option<Arc<OperatorMetricsCollector>>],
    ) -> Result<(bool, usize, Vec<usize>)> {
        let mut any_progress = false;
        let mut productive_activations = 0usize;
        // Track which operators are in WaitingForAsync state after their
        // most recent activation. Using a set avoids stale entries when an
        // operator transitions from WaitingForAsync to MadeProgress across
        // multi-pass iterations.
        let mut async_waiting_set: std::collections::HashSet<usize> =
            std::collections::HashSet::new();

        // Multi-pass: keep iterating until no operator makes progress or budget exhausted.
        let mut made_progress_this_pass = true;
        while made_progress_this_pass {
            made_progress_this_pass = false;

            for &pos in &self.operator_positions {
                if done[pos] {
                    continue;
                }
                if productive_activations >= budget {
                    return Ok((
                        any_progress,
                        productive_activations,
                        async_waiting_set.into_iter().collect(),
                    ));
                }

                let start = if op_collectors.get(pos).and_then(|c| c.as_ref()).is_some() {
                    Some(Instant::now())
                } else {
                    None
                };

                let op_name = operators[pos].name().to_string();
                let result = activate_operator(&mut operators[pos], catch_panics).map_err(|e| {
                    let enriched = e.with_operator_context(op_name.clone(), worker_index);
                    if let Some(ctrl) = control_sender {
                        ctrl.broadcast_error(op_name, format!("{enriched}"));
                    }
                    enriched
                });

                // Record metrics regardless of success/failure.
                if let Some(start) = start {
                    if let Some(collector) = op_collectors.get(pos).and_then(|c| c.as_ref()) {
                        collector.record_activation(start.elapsed(), 0);
                    }
                }

                let outcome = result?;

                match outcome {
                    ActivationOutcome::MadeProgress => {
                        productive_activations += 1;
                        any_progress = true;
                        made_progress_this_pass = true;
                        // Don't remove from async set — operator may still
                        // have in-flight tasks while collecting results.
                    }
                    ActivationOutcome::Idle => {
                        async_waiting_set.remove(&pos);
                    }
                    ActivationOutcome::WaitingForAsync => {
                        async_waiting_set.insert(pos);
                    }
                    ActivationOutcome::BlockedOnBackpressure => {
                        productive_activations += 1;
                        any_progress = true;
                        made_progress_this_pass = true;
                    }
                    ActivationOutcome::Done => {
                        productive_activations += 1;
                        done[pos] = true;
                        any_progress = true;
                        async_waiting_set.remove(&pos);
                    }
                }
            }
        }

        Ok((
            any_progress,
            productive_activations,
            async_waiting_set.into_iter().collect(),
        ))
    }

    /// Whether all operators in this task are done.
    fn is_done(&self, done: &[bool]) -> bool {
        self.operator_positions.iter().all(|&pos| done[pos])
    }
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
    /// Pre-computed successors by position: successors_by_pos[pos] = [successor positions].
    /// Built from graph topology during materialization.
    successors_by_pos: Vec<Vec<usize>>,
    /// Pre-computed predecessors by position: predecessors_by_pos[pos] = [predecessor positions].
    /// Built from graph topology during materialization.
    predecessors_by_pos: Vec<Vec<usize>>,
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
    /// Async notifiers for registered probes.
    /// Each probe has a corresponding notifier that wakes async waiters.
    /// Indices correspond 1:1 with `probes`.
    probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
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
    /// Number of operators currently waiting for async task completion.
    /// While > 0, the executor must not declare quiescence — in-flight work
    /// will produce results that need processing.
    async_waiting: Vec<bool>,
    /// Worker index for error context enrichment.
    worker_index: usize,
    /// Cross-worker control broadcast sender (for reporting errors to siblings).
    control_sender: Option<ControlSender>,
    /// Cross-worker control broadcast receiver (for learning about sibling errors).
    control_receiver: Option<ControlReceiver>,
    /// Optional fused activation order — when set, operators are activated in
    /// topological order rather than FIFO ready-queue order. This allows data to
    /// flow through the pipeline in a single sweep (source → ... → sink).
    fused_order: Option<FusedActivationOrder>,
    /// Stage tasks: when populated, the executor schedules stage tasks instead of
    /// individual operators. Each task runs its operators in fused topological order.
    /// Mutually exclusive with `fused_order` (stage_tasks supersedes it).
    stage_tasks: Vec<FusedStageTask>,
    /// Maps operator position → stage task index. Used by `propagate_progress()`
    /// to enqueue the correct task when an operator's frontier changes.
    /// Empty when stage_tasks is empty.
    op_pos_to_task: Vec<usize>,
    /// Per-task queue membership (mirrors `in_queue` but for stage tasks).
    task_in_queue: Vec<bool>,
    /// Aggregate dataflow metrics. None when collect_metrics is false.
    dataflow_metrics: Option<Arc<DataflowMetrics>>,
    /// Start time for live wall-clock metrics when collection is enabled.
    wall_start: Option<Instant>,
    /// Per-operator metrics collectors, indexed by position.
    op_collectors: Vec<Option<Arc<OperatorMetricsCollector>>>,
    /// Phantom for the timestamp type.
    _phantom: PhantomData<T>,
    /// Whether the executor is in the drain phase (cancellation received,
    /// processing in-flight data before stopping).
    draining: bool,
    /// Deadline for the drain phase. If `Some`, the executor entered drain
    /// mode and will return `Err(Cancelled)` if this deadline is exceeded.
    drain_deadline: Option<Instant>,
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

        let wake_handle = external_wake_handle.unwrap_or_default();

        for edge_idx in 0..total_edge_count {
            let mut factory = factory_map.remove(&edge_idx).ok_or_else(|| {
                Error::Dataflow(DataflowError::MissingFactory {
                    edge_index: edge_idx,
                })
            })?;
            let (push, pull) = factory.build(&worker_context, Some(wake_handle.clone()))?;
            push_ends.push(Some(push));
            pull_ends.push(Some(pull));
        }

        // Phase 2: Collect channel endpoints per operator.
        // For each operator, gather:
        //   - input_pullers: pull ends of edges targeting this operator (by target port)
        //   - output_pushers: push ends of edges sourced from this operator (by source port)
        let mut op_input_pullers: std::collections::HashMap<
            usize,
            Vec<(usize, Box<dyn std::any::Any + Send>)>,
        > = std::collections::HashMap::new();
        let mut op_output_pushers: std::collections::HashMap<
            usize,
            Vec<(usize, Box<dyn std::any::Any + Send>)>,
        > = std::collections::HashMap::new();

        // Process regular edges
        for (edge_idx, edge) in edges.iter().enumerate() {
            let pull = pull_ends[edge_idx].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "edge endpoint missing or already materialized".into(),
                ))
            })?;
            let push = push_ends[edge_idx].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "edge endpoint missing or already materialized".into(),
                ))
            })?;

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
            let pull = pull_ends[edge_idx].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "edge endpoint missing or already materialized".into(),
                ))
            })?;
            let push = push_ends[edge_idx].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "edge endpoint missing or already materialized".into(),
                ))
            })?;

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
        // on the same factories. Currently all factories are SingleUseFactory (returns
        // Err on 2nd call). PR 39 will: (1) check is_replayable() for all factories and
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
                wake_handle: Some(wake_handle.clone()),
            };

            let operator = factory.build(&worker_context, endpoints)?;
            let pos = operators.len();
            if op_idx < index_to_pos.len() {
                index_to_pos[op_idx] = pos;
            }
            operators.push(operator);
        }

        let n = operators.len();
        let mut successors_by_pos: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut predecessors_by_pos: Vec<Vec<usize>> = vec![Vec::new(); n];
        for edge in edges.iter().chain(feedback_edges.iter()) {
            let src_idx = edge.source.operator_index;
            let tgt_idx = edge.target.operator_index;
            if src_idx < index_to_pos.len() && tgt_idx < index_to_pos.len() {
                let src_pos = index_to_pos[src_idx];
                let tgt_pos = index_to_pos[tgt_idx];
                if src_pos != usize::MAX && tgt_pos != usize::MAX && src_pos < n && tgt_pos < n {
                    successors_by_pos[src_pos].push(tgt_pos);
                    predecessors_by_pos[tgt_pos].push(src_pos);
                }
            }
        }
        for v in &mut successors_by_pos {
            v.sort_unstable();
            v.dedup();
        }
        for v in &mut predecessors_by_pos {
            v.sort_unstable();
            v.dedup();
        }

        let done = vec![false; operators.len()];
        let async_waiting = vec![false; operators.len()];

        // Initialize per-operator metrics collectors when enabled.
        let (dataflow_metrics, op_collectors) = if config.collect_metrics {
            let mut dm = DataflowMetrics::new("dataflow");
            let collectors: Vec<Option<Arc<OperatorMetricsCollector>>> = operators
                .iter()
                .enumerate()
                .map(|(pos, op)| {
                    let c = dm.register_operator(op.name(), pos);
                    Some(c)
                })
                .collect();
            (Some(Arc::new(dm)), collectors)
        } else {
            (None, Vec::new())
        };

        // Initially, all operators are ready (they may have initial input or
        // need to produce initial output like sources).
        let ready_queue: VecDeque<usize> = (0..operators.len()).collect();
        let in_queue = vec![true; operators.len()];
        let wall_start = config.collect_metrics.then(Instant::now);

        Ok(Self {
            operators,
            ready_queue,
            in_queue,
            done,
            successors_by_pos,
            predecessors_by_pos,
            index_to_pos,
            config,
            cancel,
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle,
            consecutive_idle: 0,
            async_waiting,
            worker_index: worker_context.worker_index(),
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            dataflow_metrics,
            wall_start,
            op_collectors,
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        })
    }

    /// Get the collected dataflow metrics, if metrics collection was enabled.
    pub fn metrics(&self) -> Option<&Arc<DataflowMetrics>> {
        self.dataflow_metrics.as_ref()
    }

    fn update_wall_time_metric(&self) {
        if let (Some(start), Some(metrics)) =
            (self.wall_start.as_ref(), self.dataflow_metrics.as_ref())
        {
            metrics.set_wall_time(start.elapsed());
        }
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
        // SAFETY: ProgressTracker::initialize establishes this structural invariant before attach
        assert!(
            tracker.is_initialized(),
            "ProgressTracker must be initialized before attaching to executor"
        );

        // Create per-operator notificators with initial frontiers.
        let mut notificators: Vec<Option<Notificator<T>>> =
            Vec::with_capacity(self.operators.len());
        for op in self.operators.iter() {
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
    ///
    /// The notifier is stored to send async notifications when the frontier changes.
    pub(crate) fn register_probe(
        &mut self,
        operator_index: usize,
        probe: ProbeHandle<T>,
        notifier: crate::dataflow::probe::ProbeNotifier<T>,
    ) {
        // Seed the probe with the current frontier from the tracker.
        if let Some(ref tracker) = self.progress_tracker {
            let frontier = tracker.operator_input_frontier_meet(operator_index);
            probe.update_frontier(&frontier);
            notifier.notify(&frontier);
        }
        self.probes.push((operator_index, probe));
        self.probe_notifiers.push(notifier);
    }

    /// Attach cross-worker control broadcast sender and receiver.
    ///
    /// When set, the executor will:
    /// - Broadcast operator errors to sibling workers before propagating them.
    /// - Drain incoming control signals at the start of each sweep.
    ///
    /// Only used in multi-worker dataflows; single-worker dataflows skip this.
    pub fn set_control_broadcast(&mut self, sender: ControlSender, receiver: ControlReceiver) {
        self.control_sender = Some(sender);
        self.control_receiver = Some(receiver);
    }

    /// Enable fused operator activation using the provided topological order.
    ///
    /// When enabled, the executor activates operators in topological (pipeline)
    /// order within each sweep rather than using the FIFO ready-queue. This
    /// allows data to flow from source to sink in a single sweep pass, reducing
    /// scheduling overhead from O(operators) round-trips to O(1).
    ///
    /// # Arguments
    ///
    /// * `order` — Operator positions in topological order. Must contain exactly
    ///   the positions of all operators in the executor.
    ///
    /// # Panics
    ///
    /// Panics if the order length doesn't match the operator count.
    pub fn enable_fusion(&mut self, order: FusedActivationOrder) {
        // SAFETY: fused order is constructed from this executor's operators
        assert_eq!(
            order.len(),
            self.operators.len(),
            "FusedActivationOrder length ({}) must match operator count ({})",
            order.len(),
            self.operators.len(),
        );
        self.fused_order = Some(order);
    }

    /// Enable fused operator activation by computing topological order from
    /// the dataflow graph.
    ///
    /// This is a convenience method that extracts the topological order from
    /// the graph and maps operator indices to positions.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The graph contains a cycle.
    /// - The graph's operator set doesn't match the executor's operators
    ///   (positions count mismatch after index mapping).
    pub fn enable_fusion_from_graph(&mut self, graph: &DataflowGraph) -> Result<()> {
        let topo_indices = graph.topological_order()?;
        let positions: Vec<usize> = topo_indices
            .into_iter()
            .filter_map(|op_idx| {
                if op_idx < self.index_to_pos.len() {
                    let pos = self.index_to_pos[op_idx];
                    if pos != usize::MAX {
                        return Some(pos);
                    }
                }
                None
            })
            .collect();

        if positions.len() != self.operators.len() {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "Cannot enable fusion: graph produced {} operator positions but executor has {} operators",
                positions.len(),
                self.operators.len(),
            ))));
        }

        self.fused_order = Some(FusedActivationOrder::new(positions));
        Ok(())
    }

    /// Enable stage-task scheduling using stage metadata from the dataflow.
    ///
    /// Groups operators into `FusedStageTask`s based on the provided stage info.
    /// Each stage's operators are mapped to their physical positions in the executor
    /// and activated together in topological order.
    ///
    /// When stage tasks are enabled, the ready queue holds task indices instead
    /// of operator positions, and `fused_order` is ignored.
    ///
    /// # Arguments
    ///
    /// * `stages` — Stage metadata from `LogicalDataflow.stages()`. Each stage's
    ///   `operator_indices` contains logical graph indices that are mapped to
    ///   physical executor positions via `index_to_pos`.
    pub fn enable_stage_tasks(&mut self, stages: &[crate::dataflow::stage::StageInfo]) {
        let mut tasks = Vec::with_capacity(stages.len());
        let mut op_pos_to_task = vec![usize::MAX; self.operators.len()];

        for (task_idx, stage) in stages.iter().enumerate() {
            // Map logical operator indices to physical positions, filtering
            // any indices that don't exist in this executor (defensive).
            let positions: Vec<usize> = stage
                .operator_indices
                .iter()
                .filter_map(|&op_idx| {
                    if op_idx < self.index_to_pos.len() {
                        let pos = self.index_to_pos[op_idx];
                        if pos != usize::MAX && pos < self.operators.len() {
                            return Some(pos);
                        }
                    }
                    None
                })
                .collect();

            // Record mapping from operator position → task index.
            for &pos in &positions {
                op_pos_to_task[pos] = task_idx;
            }

            tasks.push(FusedStageTask {
                operator_positions: positions,
            });
        }

        let num_tasks = tasks.len();
        self.stage_tasks = tasks;
        self.op_pos_to_task = op_pos_to_task;
        self.task_in_queue = vec![false; num_tasks];

        // Initialize ready queue with all non-empty tasks.
        self.ready_queue.clear();
        for flag in self.in_queue.iter_mut() {
            *flag = false;
        }
        for (task_idx, task) in self.stage_tasks.iter().enumerate() {
            if !task.operator_positions.is_empty() && !task.is_done(&self.done) {
                self.ready_queue.push_back(task_idx);
                self.task_in_queue[task_idx] = true;
            }
        }
    }

    /// Whether stage-task scheduling is enabled.
    pub fn has_stage_tasks(&self) -> bool {
        !self.stage_tasks.is_empty()
    }

    /// Whether fused activation is enabled (either via fused_order or stage_tasks).
    pub fn is_fused(&self) -> bool {
        self.fused_order.is_some() || !self.stage_tasks.is_empty()
    }

    /// Propagate progress and enqueue operators whose frontiers changed.
    ///
    /// Called after each activation batch when a progress tracker is present.
    /// Propagates progress and updates operator frontiers/notificators.
    ///
    /// After operators produce/consume data, capabilities change. This method:
    /// 1. Collects capability changes from all operators' ProgressReporters.
    /// 2. Runs the reachability tracker to compute new frontiers.
    /// 3. Delivers frontier updates to the executor's per-operator notificators
    ///    and to notify-capable operators via `update_input_frontier()`.
    /// 4. Re-enqueues operators that have ready notifications.
    ///
    /// Returns true if any operator was newly activated.
    fn propagate_progress(&mut self) -> Result<bool> {
        let tracker = match &mut self.progress_tracker {
            Some(t) => t,
            None => return Ok(false),
        };

        let dirty: Vec<usize> = tracker.propagate()?.to_vec();

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

        // Enqueue dirty operators (or their owning stage tasks).
        if !self.stage_tasks.is_empty() {
            // Stage-task mode: enqueue the task that owns the dirty operator.
            for op_idx in dirty {
                if op_idx < self.index_to_pos.len() {
                    let pos = self.index_to_pos[op_idx];
                    if pos != usize::MAX && !self.done[pos] && pos < self.op_pos_to_task.len() {
                        let task_idx = self.op_pos_to_task[pos];
                        if task_idx != usize::MAX && !self.task_in_queue[task_idx] {
                            self.ready_queue.push_back(task_idx);
                            self.task_in_queue[task_idx] = true;
                            activated = true;
                        }
                    }
                }
            }

            // Check notifications — enqueue owning task.
            for pos in 0..self.operators.len() {
                if self.done[pos] {
                    continue;
                }
                let executor_has_ready = pos < self.notificators.len()
                    && self
                        .notificators
                        .get(pos)
                        .and_then(|n| n.as_ref())
                        .is_some_and(|n| n.has_ready());
                let operator_has_ready = self.operators[pos].has_ready_notifications();

                if (executor_has_ready || operator_has_ready) && pos < self.op_pos_to_task.len() {
                    let task_idx = self.op_pos_to_task[pos];
                    if task_idx != usize::MAX && !self.task_in_queue[task_idx] {
                        self.ready_queue.push_back(task_idx);
                        self.task_in_queue[task_idx] = true;
                        activated = true;
                    }
                }
            }
        } else {
            // Non-stage-task mode: enqueue individual operators.
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
            for pos in 0..self.operators.len() {
                if self.done[pos] || self.in_queue[pos] {
                    continue;
                }
                let executor_has_ready = pos < self.notificators.len()
                    && self
                        .notificators
                        .get(pos)
                        .and_then(|n| n.as_ref())
                        .is_some_and(|n| n.has_ready());
                let operator_has_ready = self.operators[pos].has_ready_notifications();

                if executor_has_ready || operator_has_ready {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    activated = true;
                }
            }
        }

        // Update registered probes with current frontiers.
        if !self.probes.is_empty() {
            let tracker = self.progress_tracker.as_ref().ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "progress tracker missing while probes are registered".into(),
                ))
            })?;
            for (i, (op_idx, probe)) in self.probes.iter().enumerate() {
                let frontier = tracker.operator_input_frontier_meet(*op_idx);
                probe.update_frontier(&frontier);
                if let Some(notifier) = self.probe_notifiers.get(i) {
                    notifier.notify(&frontier);
                }
            }
        }

        Ok(activated)
    }

    /// Run the dataflow to completion or cancellation using a simple single-threaded
    /// activation loop. This is a **testing/validation helper** — it does NOT use the
    /// Worker Thread Pool. In production, the orchestrator event loop drives operator
    /// activations via the TaskScheduler and Worker Thread Pool (see §5.4 in DESIGN.md).
    ///
    /// Returns `Ok(true)` on normal completion (all operators done).
    /// Returns `Ok(false)` on quiescence (no operator can make progress,
    /// but not all operators are done — e.g., waiting for external input).
    /// Returns `Err(Error::Cancelled { .. })` if the cancellation token fires.
    /// Returns `Err(...)` if any operator produces an error.
    pub fn run(&mut self) -> Result<bool> {
        let result = self.run_loop();
        self.update_wall_time_metric();
        result
    }

    fn run_loop(&mut self) -> Result<bool> {
        loop {
            match self.run_one_sweep()? {
                SweepOutcome::Completed => return Ok(true),
                SweepOutcome::Quiescent => return Ok(false),
                SweepOutcome::MadeProgress => {
                    // Continue immediately — there may be more work.
                }
                SweepOutcome::Idle => {
                    // No progress this sweep but not quiescent yet.
                    // During drain, sleep briefly to avoid 100% CPU spin while
                    // waiting for the drain deadline to fire.
                    if self.draining {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                SweepOutcome::WaitingForInput => {
                    // External inputs still open. In sync mode, sleep briefly
                    // to avoid busy-polling.
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    /// Activate an operator and record metrics if enabled.
    #[inline(always)]
    fn activate_with_metrics(&mut self, pos: usize) -> Result<ActivationOutcome> {
        let start = if self.config.collect_metrics {
            Some(Instant::now())
        } else {
            None
        };

        let op_name = self.operators[pos].name().to_string();
        let result =
            activate_operator(&mut self.operators[pos], self.config.catch_panics).map_err(|e| {
                let enriched = e.with_operator_context(op_name.clone(), self.worker_index);
                if let Some(ref ctrl) = self.control_sender {
                    ctrl.broadcast_error(op_name, format!("{enriched}"));
                }
                enriched
            });

        // Record metrics regardless of success/failure — failed activations
        // still consume CPU time and should be tracked.
        if let Some(start) = start {
            if let Some(collector) = self.op_collectors.get(pos).and_then(|c| c.as_ref()) {
                collector.record_activation(start.elapsed(), 0);
            }
        }

        result
    }

    /// Unfused activation: process operators from the ready queue in FIFO order.
    ///
    /// This is the original scheduling strategy. Each operator is independently
    /// scheduled; data flowing from op A to op B requires multiple sweeps.
    fn run_unfused_activation(&mut self) -> Result<bool> {
        // If the ready queue is empty, re-populate with non-done operators.
        // Also enqueue any operators waiting for async task completion —
        // a wake notification means their results may be ready to collect.
        if self.ready_queue.is_empty() {
            for pos in 0..self.operators.len() {
                if !self.done[pos] {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                }
            }
            if self.ready_queue.is_empty() {
                return Ok(false);
            }
        } else {
            // Queue not empty, but async-waiting operators may need activation
            // after a wake notification. Enqueue them if not already queued.
            for pos in 0..self.async_waiting.len() {
                if self.async_waiting[pos] && !self.in_queue[pos] && !self.done[pos] {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                }
            }
        }

        let mut any_progress = false;
        let batch_size = self
            .ready_queue
            .len()
            .min(self.config.max_activations_per_step);

        for _ in 0..batch_size {
            let Some(pos) = self.ready_queue.pop_front() else {
                break;
            };
            self.in_queue[pos] = false;

            if self.done[pos] {
                continue;
            }

            let outcome = self.activate_with_metrics(pos)?;

            match outcome {
                ActivationOutcome::MadeProgress => {
                    any_progress = true;
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    // Don't clear async_waiting here — the operator may still
                    // have in-flight tasks even while collecting results. The
                    // flag is only cleared when the operator returns Idle (no
                    // in-flight work) or Done.
                }
                ActivationOutcome::Idle => {
                    // No work and no in-flight tasks — safe to clear.
                    self.async_waiting[pos] = false;
                }
                ActivationOutcome::WaitingForAsync => {
                    // Don't re-queue immediately — wake_handle.notify() will
                    // wake the executor when a task completes. The per-operator
                    // async_waiting flag prevents premature quiescence and
                    // ensures re-enqueue on the next sweep.
                    self.async_waiting[pos] = true;
                }
                ActivationOutcome::BlockedOnBackpressure => {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    any_progress = true;
                }
                ActivationOutcome::Done => {
                    self.done[pos] = true;
                    self.async_waiting[pos] = false;
                    self.propagate_completion(pos);
                    any_progress = true;
                }
            }
        }

        Ok(any_progress)
    }

    /// Fused activation: process ALL non-done operators in topological order.
    ///
    /// Data flows through the pipeline in a single pass (source → ... → sink).
    /// When an upstream operator produces output, its downstream is activated
    /// immediately in the same sweep — no re-scheduling round-trip needed.
    ///
    /// Budget limits only *productive* activations (MadeProgress, BlockedOnBackpressure,
    /// Done). Idle activations are free — they represent operators with no input.
    fn run_fused_activation(&mut self) -> Result<bool> {
        // Clear queue state at the start — fused mode ignores the ready_queue
        // but propagate_progress() uses in_queue to gate enqueuing. Keep state
        // consistent regardless of how we exit (normal or early budget return).
        self.ready_queue.clear();
        for flag in self.in_queue.iter_mut() {
            *flag = false;
        }

        // Safety: we only read positions from fused_order, no mutation needed.
        let positions = self
            .fused_order
            .as_ref()
            .ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidGraph(
                    "executor fused order missing".into(),
                ))
            })?
            .positions()
            .to_vec();

        let mut any_progress = false;
        let mut productive_activations = 0usize;
        let budget = self.config.max_activations_per_step;

        // Multi-pass: keep iterating until no operator makes progress or budget exhausted.
        // This ensures that when op[i] pushes data, op[i+1..] can consume it in the same sweep.
        let mut made_progress_this_pass = true;
        while made_progress_this_pass {
            made_progress_this_pass = false;

            for &pos in &positions {
                if self.done[pos] {
                    continue;
                }
                if productive_activations >= budget {
                    // Budget exhausted — report progress so we get another sweep.
                    return Ok(any_progress);
                }

                let outcome = self.activate_with_metrics(pos)?;

                match outcome {
                    ActivationOutcome::MadeProgress => {
                        productive_activations += 1;
                        any_progress = true;
                        made_progress_this_pass = true;
                    }
                    ActivationOutcome::Idle => {
                        self.async_waiting[pos] = false;
                    }
                    ActivationOutcome::WaitingForAsync => {
                        self.async_waiting[pos] = true;
                    }
                    ActivationOutcome::BlockedOnBackpressure => {
                        productive_activations += 1;
                        any_progress = true;
                        made_progress_this_pass = true;
                    }
                    ActivationOutcome::Done => {
                        productive_activations += 1;
                        self.done[pos] = true;
                        self.async_waiting[pos] = false;
                        self.propagate_completion(pos);
                        any_progress = true;
                    }
                }
            }
        }

        Ok(any_progress)
    }

    /// Stage-task activation: process stage tasks from the ready queue.
    ///
    /// Each task runs its operators in fused topological order (multi-pass).
    /// All non-done tasks are activated every sweep (matching old fused semantics
    /// where all operators are considered each sweep). This prevents downstream
    /// starvation when an upstream stage keeps producing data.
    fn run_stage_task_activation(&mut self) -> Result<bool> {
        // Always repopulate with all non-done tasks at the start of each sweep.
        // This ensures downstream stages are activated even when upstream stages
        // keep making progress (preventing starvation via bounded channels).
        self.ready_queue.clear();
        for flag in self.task_in_queue.iter_mut() {
            *flag = false;
        }
        for (task_idx, task) in self.stage_tasks.iter().enumerate() {
            if !task.operator_positions.is_empty() && !task.is_done(&self.done) {
                self.ready_queue.push_back(task_idx);
                self.task_in_queue[task_idx] = true;
            }
        }
        if self.ready_queue.is_empty() {
            return Ok(false);
        }

        let mut any_progress = false;
        let budget = self.config.max_activations_per_step;
        let mut total_productive = 0usize;
        let batch_size = self.ready_queue.len();

        for _ in 0..batch_size {
            let Some(task_idx) = self.ready_queue.pop_front() else {
                break;
            };
            self.task_in_queue[task_idx] = false;

            if self.stage_tasks[task_idx].is_done(&self.done) {
                continue;
            }

            let remaining_budget = budget.saturating_sub(total_productive);
            if remaining_budget == 0 {
                // Budget exhausted — stop processing more tasks this sweep.
                break;
            }

            let (progress, productive, async_positions) = self.stage_tasks[task_idx].activate(
                &mut self.operators,
                &mut self.done,
                remaining_budget,
                self.worker_index,
                self.control_sender.as_ref(),
                self.config.catch_panics,
                &self.op_collectors,
            )?;

            total_productive += productive;
            // Update async_waiting flags for all operators in this task:
            // set true for positions still waiting, clear for the rest.
            for &op_pos in &self.stage_tasks[task_idx].operator_positions {
                self.async_waiting[op_pos] = false;
            }
            for pos in async_positions {
                self.async_waiting[pos] = true;
            }

            if progress {
                any_progress = true;
            }
        }

        Ok(any_progress)
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
        // Drain incoming control signals (non-blocking).
        // This is checked BEFORE the cancellation check so that control
        // signals are consumed even when cancellation is already in flight.
        if let Some(ref mut rx) = self.control_receiver {
            // Signals are drained; currently the executor doesn't act on
            // individual signals beyond what the CancellationToken provides.
            // Future extensions (LimitReached handling, etc.) hook here.
            let _signals = rx.try_recv();
        }

        // Check cancellation — either enter drain mode or return error.
        if self.cancel.is_cancelled() {
            if self.draining {
                // Already draining — check if deadline expired.
                if let Some(deadline) = self.drain_deadline {
                    if Instant::now() >= deadline {
                        // Drain timed out — propagate the cancellation error.
                        self.cancel.check()?;
                    }
                }
            } else if let Some(timeout) = self.config.drain_timeout {
                // Enter drain mode: set deadline. Instead of zeroing the
                // external_inputs_open counter (which would race with
                // ChannelSourceOperator::close_inputs() calling fetch_sub),
                // we skip the external-inputs-open check when draining.
                self.draining = true;
                self.drain_deadline = Some(Instant::now() + timeout);
            } else {
                // No drain configured — immediate cancellation.
                self.cancel.check()?;
            }
        }

        // If all operators are done, we're finished.
        if self.done.iter().all(|&d| d) {
            return Ok(SweepOutcome::Completed);
        }

        // Activate operators — stage-task, fused, or unfused path.
        let any_progress = if !self.stage_tasks.is_empty() {
            self.run_stage_task_activation()?
        } else if self.fused_order.is_some() {
            self.run_fused_activation()?
        } else {
            self.run_unfused_activation()?
        };

        if any_progress {
            self.consecutive_idle = 0;
        } else {
            self.consecutive_idle += 1;
        }

        // After each batch, propagate progress and enqueue dirty operators.
        if self.propagate_progress()? {
            self.consecutive_idle = 0;
        }

        if self.consecutive_idle >= self.config.max_idle_sweeps {
            // If any operators have in-flight async tasks, don't declare
            // quiescence — results will arrive and trigger a wake notification.
            if self.async_waiting.iter().any(|&w| w) {
                self.consecutive_idle = 0;
                return Ok(SweepOutcome::WaitingForInput);
            }

            // Check if external inputs are still open — if so, don't
            // declare quiescence, but signal the caller to wait.
            // During drain mode, we ignore external inputs (they should
            // close naturally; we don't want to wait for them).
            if !self.draining
                && self
                    .external_inputs_open
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
            {
                self.consecutive_idle = 0;
                return Ok(SweepOutcome::WaitingForInput);
            }

            if let Some(ref tracker) = self.progress_tracker {
                // If the tracker reports completion AND we've heard from all
                // peers AND no peer progress is pending, force-close any
                // remaining operators (feedback cycles that quiesced).
                if tracker.is_completed()
                    && !tracker.has_pending_peer_progress()?
                    && tracker.all_peers_synced()
                {
                    for pos in 0..self.operators.len() {
                        if !self.done[pos] {
                            self.operators[pos].close_inputs();
                            self.done[pos] = true;
                        }
                    }
                    return Ok(SweepOutcome::Completed);
                }

                // Any of these conditions means the global dataflow is still
                // active and we should wait rather than declare quiescence:
                // - Tracker not completed: outstanding capabilities somewhere
                // - Peers not synced: initial caps still in transit
                // - Pending peer progress: buffered updates not yet absorbed
                //
                // During drain mode, we don't wait — the drain deadline
                // controls when to give up, not the progress tracker.
                if !self.draining
                    && (!tracker.is_completed()
                        || !tracker.all_peers_synced()
                        || tracker.has_pending_peer_progress()?)
                {
                    self.consecutive_idle = 0;
                    return Ok(SweepOutcome::WaitingForInput);
                }
            }

            // Quiescent — no operator made progress.
            // During drain mode, quiescence means all in-flight data has been
            // processed — treat this as successful completion, BUT only if the
            // progress tracker agrees (if present). If the tracker shows
            // outstanding capabilities, the dataflow isn't truly done — keep
            // draining until the deadline.
            if self.draining {
                let tracker_active = self
                    .progress_tracker
                    .as_ref()
                    .is_some_and(|t| !t.is_completed());
                if tracker_active {
                    // Dataflow still has outstanding capabilities but no progress.
                    // Return Idle to let the drain deadline check fire next sweep.
                    // In sync mode, run_loop adds a brief sleep to avoid spin-busy.
                    // In async mode, Idle respects poll budget and self-wakes.
                    self.consecutive_idle = 0;
                    return Ok(SweepOutcome::Idle);
                }
                return Ok(SweepOutcome::Completed);
            }
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
                    self.update_wall_time_metric();
                    return Poll::Ready(Ok(true));
                }
                Ok(SweepOutcome::Quiescent) => {
                    self.wake_handle.clear_waker();
                    self.update_wall_time_metric();
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
                        self.update_wall_time_metric();
                        return Poll::Pending;
                    }
                    continue;
                }
                Ok(SweepOutcome::Idle) => {
                    sweeps_this_poll += 1;
                    if budget > 0 && sweeps_this_poll >= budget {
                        self.wake_handle.register_waker(cx.waker());
                        self.wake_handle.notify();
                        self.update_wall_time_metric();
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
                    self.update_wall_time_metric();
                    return Poll::Pending;
                }
                Err(e) => {
                    self.wake_handle.clear_waker();
                    self.update_wall_time_metric();
                    return Poll::Ready(Err(e));
                }
            }
        }
    }

    /// After an operator completes, check if downstream operators should
    /// have their inputs closed.
    fn propagate_completion(&mut self, completed_pos: usize) {
        if completed_pos >= self.successors_by_pos.len() {
            return;
        }

        let mut succ_idx = 0;
        while succ_idx < self.successors_by_pos[completed_pos].len() {
            let succ_pos = self.successors_by_pos[completed_pos][succ_idx];
            succ_idx += 1;

            if succ_pos >= self.done.len() || self.done[succ_pos] {
                continue;
            }

            let all_preds_done = if succ_pos < self.predecessors_by_pos.len() {
                self.predecessors_by_pos[succ_pos]
                    .iter()
                    .all(|&pred_pos| pred_pos < self.done.len() && self.done[pred_pos])
            } else {
                false
            };
            if !all_preds_done {
                continue;
            }

            if self.stage_tasks.is_empty() {
                if succ_pos < self.in_queue.len() && !self.in_queue[succ_pos] {
                    self.ready_queue.push_back(succ_pos);
                    self.in_queue[succ_pos] = true;
                }
                continue;
            }

            if succ_pos < self.op_pos_to_task.len() {
                let task_idx = self.op_pos_to_task[succ_pos];
                if task_idx != usize::MAX
                    && task_idx < self.task_in_queue.len()
                    && !self.task_in_queue[task_idx]
                {
                    self.ready_queue.push_back(task_idx);
                    self.task_in_queue[task_idx] = true;
                }
            }
        }
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

    /// Helper to create a test executor from a list of operators.
    impl DataflowExecutor<u64> {
        fn new_test(
            operators: Vec<Box<dyn SchedulableOperator>>,
            config: ExecutorConfig,
            worker_index: usize,
        ) -> Self {
            let n = operators.len();
            let index_to_pos: Vec<usize> = operators.iter().map(|op| op.index()).collect();
            Self {
                operators,
                ready_queue: VecDeque::from_iter(0..n),
                in_queue: vec![true; n],
                done: vec![false; n],
                index_to_pos,
                config,
                cancel: CancellationToken::new(),
                progress_tracker: None,
                notificators: Vec::new(),
                probes: Vec::new(),
                probe_notifiers: Vec::new(),
                external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                wake_handle: WakeHandle::new(),
                consecutive_idle: 0,
                async_waiting: vec![false; n],
                worker_index,
                control_sender: None,
                control_receiver: None,
                fused_order: None,
                stage_tasks: Vec::new(),
                op_pos_to_task: Vec::new(),
                task_in_queue: Vec::new(),
                successors_by_pos: Vec::new(),
                predecessors_by_pos: Vec::new(),
                dataflow_metrics: None,
                wall_start: None,
                op_collectors: Vec::new(),
                _phantom: PhantomData,
                draining: false,
                drain_deadline: None,
            }
        }
    }

    /// A trivial operator that counts activations and becomes done after N.
    struct CountingOperator {
        name: String,
        index: usize,
        stage_id: crate::dataflow::stage::StageId,
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

        fn stage_id(&self) -> crate::dataflow::stage::StageId {
            self.stage_id
        }

        fn close_inputs(&mut self) {
            self.remaining = 0;
        }
    }

    /// An operator that always returns Idle — never makes progress, never finishes.
    /// Used for testing async waiting behavior.
    struct IdleOperator {
        index: usize,
        stage_id: crate::dataflow::stage::StageId,
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

        fn stage_id(&self) -> crate::dataflow::stage::StageId {
            self.stage_id
        }

        fn close_inputs(&mut self) {
            self.closed = true;
        }
    }

    #[test]
    fn executor_runs_single_operator_to_completion() {
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "counter".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_respects_cancellation() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "infinite".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = executor.run();
        assert!(matches!(result, Err(Error::Cancelled { .. })));
    }

    #[test]
    fn executor_handles_multiple_operators() {
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "a".into(),
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 2,
                }),
                Box::new(CountingOperator {
                    name: "b".into(),
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_empty_dataflow_completes_immediately() {
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
    }

    #[test]
    fn executor_idle_operator_reaches_quiescence() {
        struct AlwaysIdle;
        impl SchedulableOperator for AlwaysIdle {
            fn activate(&mut self) -> Result<ActivationOutcome> {
                Ok(ActivationOutcome::Idle)
            }
            fn is_done(&self) -> bool {
                false
            }
            fn name(&self) -> &str {
                "idle"
            }
            fn index(&self) -> usize {
                0
            }
            fn stage_id(&self) -> crate::dataflow::stage::StageId {
                crate::dataflow::stage::StageId::new(0)
            }
            fn close_inputs(&mut self) {}
        }

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(AlwaysIdle)],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 10,
                max_idle_sweeps: 3,
                max_sweeps_per_poll: 0,
                catch_panics: false,
                collect_metrics: false,
                drain_timeout: None,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Should terminate via quiescence, not infinite loop.
        // Returns Ok(false) because AlwaysIdle never completes.
        let result = executor.run();
        assert!(!result.unwrap());
    }

    #[test]
    fn executor_runs_wired_pipeline() {
        use crate::communication::allocator::ChannelAllocator;
        use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
        use crate::dataflow::stage::StageId;
        use crate::dataflow::wired_operators::{
            WiredSinkOperator, WiredSourceOperator, WiredUnaryOperator,
        };

        let mut alloc = ChannelAllocator::new();
        let ch1 = alloc.allocate::<u64, i32, ()>();
        let ch2 = alloc.allocate::<u64, i32, ()>();

        let source: Box<dyn SchedulableOperator> = Box::new(WiredSourceOperator::new(
            "source",
            0,
            StageId::new(0),
            vec![(0u64, vec![1, 2, 3]), (1u64, vec![10, 20])],
            ch1.pusher,
        ));

        let double: Box<dyn SchedulableOperator> = Box::new(WiredUnaryOperator::new(
            "double",
            1,
            StageId::new(0),
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
            "sink",
            2,
            StageId::new(0),
            ch2.puller,
        ));

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = executor.run();
        assert!(result.unwrap());
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
        builder
            .add_operator(
                1, // operator index
                "op",
                1, // inputs
                1, // outputs
                PortConnectivity::identity(0u64),
            )
            .unwrap();

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 1,
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Attach progress tracker
        executor.set_progress_tracker(tracker);
        assert!(executor.progress_tracker.is_some());

        // Run should still complete normally
        let result = executor.run();
        assert!(result.unwrap());
    }

    #[test]
    fn notify_at_and_drain_notifications() {
        use crate::progress::frontier::Antichain;
        use crate::progress::notificator::Notificator;

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 1,
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
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
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
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
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
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
                stage_id: crate::dataflow::stage::StageId::new(0),
                closed: false,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 1024,
                max_idle_sweeps: 1,     // reach idle threshold quickly
                max_sweeps_per_poll: 0, // no budget limit for this test
                catch_panics: false,
                collect_metrics: false,
                drain_timeout: None,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // IdleOperator always returns Idle → hits idle threshold → WaitingForInput.
        // poll_run should register waker and return Pending.
        let mut got_pending = false;
        #[allow(clippy::never_loop)]
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
        use std::task::{Context, Wake};

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
                stage_id: crate::dataflow::stage::StageId::new(0),
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
                catch_panics: false,
                collect_metrics: false,
                drain_timeout: None,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let wake = executor.wake_handle();

        // Drive to Pending
        let mut reached_pending = false;
        for _ in 0..200 {
            if Pin::new(&mut executor).poll(&mut cx).is_pending() {
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
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        let result = Pin::new(&mut executor).poll(&mut cx);
        match result {
            Poll::Ready(Err(Error::Cancelled { .. })) => {} // expected
            other => panic!("Expected Cancelled error, got {:?}", other),
        }
    }

    #[test]
    fn propagate_completion_requeues_when_all_predecessors_complete() {
        let mut executor = DataflowExecutor::<u64>::new_test(
            vec![
                Box::new(CountingOperator {
                    name: "a".into(),
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "b".into(),
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "c".into(),
                    index: 2,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
            ],
            ExecutorConfig::default(),
            0,
        );
        executor.ready_queue.clear();
        executor.in_queue.fill(false);
        executor.successors_by_pos = vec![vec![1], vec![2], Vec::new()];
        executor.predecessors_by_pos = vec![Vec::new(), vec![0], vec![1]];

        executor.done[0] = true;
        executor.propagate_completion(0);
        assert_eq!(executor.ready_queue, VecDeque::from([1]));
        assert_eq!(executor.in_queue, vec![false, true, false]);

        let dequeued = executor.ready_queue.pop_front();
        assert_eq!(dequeued, Some(1));
        executor.in_queue[1] = false;

        executor.done[1] = true;
        executor.propagate_completion(1);
        assert_eq!(executor.ready_queue, VecDeque::from([2]));
        assert_eq!(executor.in_queue, vec![false, false, true]);
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

        let (mut push, _pull) =
            bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()), None);

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

        let (mut push, _pull) =
            bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()), None);

        push.close();
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_with_wake_notifies_on_drop() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;

        let wake = WakeHandle::new();
        wake.take_notification();

        let (push, _pull) = bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()), None);

        drop(push);
        assert!(wake.take_notification());
    }

    #[test]
    fn bounded_channel_pull_notifies_when_freeing_capacity() {
        use crate::dataflow::channels::bounded::bounded_channel_with_wake;
        use crate::dataflow::channels::envelope::Envelope;
        use crate::dataflow::channels::pushpull::{Pull, Push};

        let wake = WakeHandle::new();

        let (mut push, mut pull) =
            bounded_channel_with_wake::<u64, i32, ()>(2, Some(wake.clone()), None);

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

        let (mut push, mut pull) =
            bounded_channel_with_wake::<u64, i32, ()>(4, Some(wake.clone()), None);

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
                stage_id: crate::dataflow::stage::StageId(0),
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
                catch_panics: false,
                collect_metrics: false,
                drain_timeout: None,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: wake_handle.clone(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
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
                stage_id: crate::dataflow::stage::StageId(0),
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
                catch_panics: false,
                collect_metrics: false,
                drain_timeout: None,
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Should run to completion without yielding
        let result = executor.poll_run(&mut cx);
        match result {
            Poll::Ready(Ok(true)) => {} // expected — completed
            other => panic!("Expected Ready(Ok(true)), got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Fused activation tests
    // -----------------------------------------------------------------------

    #[test]
    fn fused_executor_runs_single_operator_to_completion() {
        use crate::dataflow::stage::FusedActivationOrder;

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "counter".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Enable fusion with topological order.
        executor.enable_fusion(FusedActivationOrder::new(vec![0]));
        assert!(executor.is_fused());

        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
    }

    #[test]
    fn fused_executor_runs_multiple_operators_in_topological_order() {
        use crate::dataflow::stage::FusedActivationOrder;

        // Three operators in a pipeline: op0(3 activations) → op1(2) → op2(1)
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "source".into(),
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 3,
                }),
                Box::new(CountingOperator {
                    name: "transform".into(),
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 2,
                }),
                Box::new(CountingOperator {
                    name: "sink".into(),
                    index: 2,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
            ],
            ready_queue: VecDeque::from([0, 1, 2]),
            in_queue: vec![true, true, true],
            done: vec![false, false, false],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Topological order: source → transform → sink
        executor.enable_fusion(FusedActivationOrder::new(vec![0, 1, 2]));

        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
        assert_eq!(executor.completed_count(), 3);
    }

    #[test]
    fn fused_executor_respects_activation_budget() {
        use crate::dataflow::stage::FusedActivationOrder;

        // Operator with many activations but tight budget.
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "counter".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
                remaining: 100,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_activations_per_step: 5, // tight budget
                ..ExecutorConfig::default()
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        executor.enable_fusion(FusedActivationOrder::new(vec![0]));

        // A single sweep should be budget-limited, not run all 100 activations.
        let outcome = executor.run_one_sweep().unwrap();
        assert_eq!(outcome, SweepOutcome::MadeProgress);
        // The operator should NOT be done yet (100 remaining, budget is 5).
        assert!(!executor.is_complete());
    }

    #[test]
    fn fused_executor_handles_cancellation() {
        use crate::dataflow::stage::FusedActivationOrder;

        let cancel = CancellationToken::new();
        cancel.cancel();

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
                remaining: 10,
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        executor.enable_fusion(FusedActivationOrder::new(vec![0]));

        let result = executor.run();
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Cancelled { .. } => {} // expected
            other => panic!("Expected Cancelled, got {:?}", other),
        }
    }

    #[test]
    fn fused_executor_handles_idle_operators() {
        use crate::dataflow::stage::FusedActivationOrder;

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(IdleOperator {
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
                closed: false,
            })],
            ready_queue: VecDeque::from([0]),
            in_queue: vec![true],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig {
                max_idle_sweeps: 2,
                ..ExecutorConfig::default()
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        executor.enable_fusion(FusedActivationOrder::new(vec![0]));

        // Idle operator → should eventually reach quiescence.
        let result = executor.run();
        assert!(!result.unwrap()); // quiescent, not completed
    }

    #[test]
    fn enable_fusion_from_graph_computes_topological_order() {
        use crate::dataflow::graph::{DataflowGraph, EdgeInfo, OperatorInfo};
        use crate::dataflow::stage::StageId;
        use crate::dataflow::stream::Slot;

        // Build a simple graph: op0 → op1 → op2
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "source", StageId::new(0), 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "map", StageId::new(0), 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "sink", StageId::new(0), 1, 0))
            .unwrap();
        graph.add_edge(EdgeInfo::new(
            Slot::new(0, 0),
            Slot::new(1, 0),
            StageId::new(0),
            StageId::new(0),
        ));
        graph.add_edge(EdgeInfo::new(
            Slot::new(1, 0),
            Slot::new(2, 0),
            StageId::new(0),
            StageId::new(0),
        ));

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "source".into(),
                    index: 0,
                    stage_id: StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "map".into(),
                    index: 1,
                    stage_id: StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "sink".into(),
                    index: 2,
                    stage_id: StageId::new(0),
                    remaining: 1,
                }),
            ],
            ready_queue: VecDeque::from([0, 1, 2]),
            in_queue: vec![true, true, true],
            done: vec![false, false, false],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Should succeed and enable fusion.
        executor.enable_fusion_from_graph(&graph).unwrap();
        assert!(executor.is_fused());

        // Run to completion.
        let result = executor.run();
        assert!(result.unwrap());
        assert!(executor.is_complete());
    }

    #[test]
    fn enable_fusion_from_graph_returns_error_on_mismatch() {
        use crate::dataflow::graph::{DataflowGraph, OperatorInfo};
        use crate::dataflow::stage::StageId;

        // Graph has 1 operator but executor has 2 — mismatch.
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "op0", StageId::new(0), 0, 0))
            .unwrap();

        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "op0".into(),
                    index: 0,
                    stage_id: StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "op1".into(),
                    index: 1,
                    stage_id: StageId::new(0),
                    remaining: 1,
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
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Should return error, not panic.
        let result = executor.enable_fusion_from_graph(&graph);
        assert!(result.is_err());
        assert!(!executor.is_fused());
    }

    #[test]
    fn fused_executor_idle_activations_do_not_consume_budget() {
        use crate::dataflow::stage::FusedActivationOrder;

        // 3 idle operators with budget=2. In old code, budget would be exhausted
        // after 2 idle activations. Now idle doesn't count, so all 3 get polled.
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(IdleOperator {
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    closed: false,
                }),
                Box::new(IdleOperator {
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    closed: false,
                }),
                Box::new(IdleOperator {
                    index: 2,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    closed: false,
                }),
            ],
            ready_queue: VecDeque::from([0, 1, 2]),
            in_queue: vec![true, true, true],
            done: vec![false, false, false],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig {
                max_activations_per_step: 2, // very tight budget
                max_idle_sweeps: 2,
                ..ExecutorConfig::default()
            },
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false, false, false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        executor.enable_fusion(FusedActivationOrder::new(vec![0, 1, 2]));

        // Even with budget=2, all idle operators should be reached (idle doesn't
        // consume budget). The sweep should return Idle, not get stuck.
        let outcome = executor.run_one_sweep().unwrap();
        assert_eq!(outcome, SweepOutcome::Idle);
    }

    // -----------------------------------------------------------------------
    // Stage-task scheduling tests
    // -----------------------------------------------------------------------

    #[test]
    fn stage_task_activation_runs_all_operators_in_stage() {
        // Two stages: stage 0 has ops [0, 1], stage 1 has op [2].
        // Op 0: 2 activations, Op 1: 1 activation, Op 2: 1 activation.
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "op0".into(),
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 2,
                }),
                Box::new(CountingOperator {
                    name: "op1".into(),
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
                Box::new(CountingOperator {
                    name: "op2".into(),
                    index: 2,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
            ],
            ready_queue: VecDeque::new(),
            in_queue: vec![false; 3],
            done: vec![false; 3],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false; 3],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        // Create stage info for two stages.
        use crate::dataflow::stage::{FusedActivationOrder, StageId, StageInfo};
        let stages = vec![
            StageInfo {
                id: StageId(0),
                parallelism: None,
                operator_indices: vec![0, 1],
                fused_order: FusedActivationOrder::new(vec![0, 1]),
            },
            StageInfo {
                id: StageId(1),
                parallelism: Some(4),
                operator_indices: vec![2],
                fused_order: FusedActivationOrder::new(vec![2]),
            },
        ];

        executor.enable_stage_tasks(&stages);
        assert!(executor.has_stage_tasks());
        assert_eq!(executor.stage_tasks.len(), 2);

        // Run to completion.
        let result = executor.run().unwrap();
        assert!(result); // completed
        assert!(executor.is_complete());
    }

    #[test]
    fn stage_task_single_stage_equivalent_to_fused() {
        // Single stage with 3 operators — should behave like fused activation.
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![
                Box::new(CountingOperator {
                    name: "a".into(),
                    index: 0,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 3,
                }),
                Box::new(CountingOperator {
                    name: "b".into(),
                    index: 1,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 2,
                }),
                Box::new(CountingOperator {
                    name: "c".into(),
                    index: 2,
                    stage_id: crate::dataflow::stage::StageId::new(0),
                    remaining: 1,
                }),
            ],
            ready_queue: VecDeque::new(),
            in_queue: vec![false; 3],
            done: vec![false; 3],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false; 3],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        use crate::dataflow::stage::{FusedActivationOrder, StageId, StageInfo};
        let stages = vec![StageInfo {
            id: StageId(0),
            parallelism: None,
            operator_indices: vec![0, 1, 2],
            fused_order: FusedActivationOrder::new(vec![0, 1, 2]),
        }];

        executor.enable_stage_tasks(&stages);
        let result = executor.run().unwrap();
        assert!(result);
        assert_eq!(executor.completed_count(), 3);
    }

    #[test]
    fn stage_task_empty_stage_skipped() {
        // Stage with operators that map to valid positions, plus an empty stage.
        let mut executor: DataflowExecutor<u64> = DataflowExecutor {
            operators: vec![Box::new(CountingOperator {
                name: "op0".into(),
                index: 0,
                stage_id: crate::dataflow::stage::StageId::new(0),
                remaining: 1,
            })],
            ready_queue: VecDeque::new(),
            in_queue: vec![false],
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
            progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            probe_notifiers: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_handle: WakeHandle::new(),
            consecutive_idle: 0,
            async_waiting: vec![false],
            worker_index: 0,
            control_sender: None,
            control_receiver: None,
            fused_order: None,
            stage_tasks: Vec::new(),
            op_pos_to_task: Vec::new(),
            task_in_queue: Vec::new(),
            successors_by_pos: Vec::new(),
            predecessors_by_pos: Vec::new(),
            dataflow_metrics: None,
            wall_start: None,
            op_collectors: Vec::new(),
            _phantom: PhantomData,
            draining: false,
            drain_deadline: None,
        };

        use crate::dataflow::stage::{FusedActivationOrder, StageId, StageInfo};
        let stages = vec![
            StageInfo {
                id: StageId(0),
                parallelism: None,
                operator_indices: vec![0],
                fused_order: FusedActivationOrder::new(vec![0]),
            },
            StageInfo {
                id: StageId(1),
                parallelism: Some(2),
                // Op index 99 doesn't exist — this stage will be empty.
                operator_indices: vec![99],
                fused_order: FusedActivationOrder::new(vec![99]),
            },
        ];

        executor.enable_stage_tasks(&stages);
        // Two tasks created, but second has no valid positions.
        assert_eq!(executor.stage_tasks.len(), 2);
        assert!(executor.stage_tasks[1].operator_positions.is_empty());

        let result = executor.run().unwrap();
        assert!(result);
    }

    // -----------------------------------------------------------------------
    // Panic recovery tests
    // -----------------------------------------------------------------------

    /// An operator that panics on its first activation.
    struct PanickingOperator {
        name: String,
        index: usize,
        stage_id: crate::dataflow::stage::StageId,
    }

    impl SchedulableOperator for PanickingOperator {
        fn activate(&mut self) -> Result<ActivationOutcome> {
            panic!("operator {} exploded", self.name);
        }
        fn is_done(&self) -> bool {
            false
        }
        fn name(&self) -> &str {
            &self.name
        }
        fn index(&self) -> usize {
            self.index
        }
        fn stage_id(&self) -> crate::dataflow::stage::StageId {
            self.stage_id
        }
        fn close_inputs(&mut self) {}
    }

    #[test]
    fn catch_panics_returns_operator_panic_error() {
        let config = ExecutorConfig {
            catch_panics: true,
            ..Default::default()
        };

        let ops: Vec<Box<dyn SchedulableOperator>> = vec![Box::new(PanickingOperator {
            name: "boom".to_string(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId::new(0),
        })];

        let mut executor = DataflowExecutor::<u64>::new_test(ops, config, 0);

        let err = executor.run().unwrap_err();
        match &err {
            Error::OperatorPanic {
                operator,
                message,
                worker_index,
            } => {
                assert_eq!(operator, "boom");
                assert!(
                    message.contains("exploded"),
                    "unexpected message: {message}"
                );
                // with_operator_context backfills worker_index
                assert_eq!(*worker_index, Some(0));
            }
            other => panic!("expected OperatorPanic, got: {other:?}"),
        }
    }

    #[test]
    fn catch_panics_disabled_propagates_panic() {
        let config = ExecutorConfig {
            catch_panics: false,
            ..Default::default()
        };

        let ops: Vec<Box<dyn SchedulableOperator>> = vec![Box::new(PanickingOperator {
            name: "boom".to_string(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId::new(0),
        })];

        let mut executor = DataflowExecutor::<u64>::new_test(ops, config, 0);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor.run()));
        assert!(result.is_err(), "expected panic to propagate");
    }

    #[test]
    fn activate_operator_helper_no_panic_passthrough() {
        let mut op: Box<dyn SchedulableOperator> = Box::new(CountingOperator {
            name: "ok_op".to_string(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId::new(0),
            remaining: 1,
        });

        // With catch_panics=true, normal returns pass through unchanged.
        let result = activate_operator(&mut op, true);
        assert!(matches!(result, Ok(ActivationOutcome::Done)));
    }

    #[test]
    fn extract_panic_message_str_and_string() {
        let str_payload: Box<dyn std::any::Any + Send> = Box::new("hello panic");
        assert_eq!(extract_panic_message(&str_payload), "hello panic");

        let string_payload: Box<dyn std::any::Any + Send> = Box::new(String::from("string panic"));
        assert_eq!(extract_panic_message(&string_payload), "string panic");

        let other_payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(
            extract_panic_message(&other_payload),
            "unknown panic payload"
        );
    }

    // -----------------------------------------------------------------------
    // Graceful drain tests
    // -----------------------------------------------------------------------

    #[test]
    fn drain_allows_inflight_data_to_complete() {
        // An operator with 3 remaining activations. Cancel immediately but
        // with drain enabled — the operator should finish all 3 activations.
        let op = CountingOperator {
            name: "drainer".into(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId(0),
            remaining: 3,
        };
        let config = ExecutorConfig {
            max_activations_per_step: 10,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 0,
            catch_panics: false,
            collect_metrics: false,
            drain_timeout: Some(std::time::Duration::from_secs(5)),
        };
        let mut executor = DataflowExecutor::new_test(vec![Box::new(op)], config, 0);

        // Cancel before running.
        executor.cancel.cancel();

        // Despite cancellation, drain mode should let the operator finish.
        let result = executor.run();
        assert!(result.is_ok(), "drain should complete successfully");
        assert!(result.unwrap(), "should report completion");
    }

    #[test]
    fn immediate_cancel_without_drain_returns_error() {
        // Without drain, cancellation should return Err(Cancelled).
        let op = CountingOperator {
            name: "no-drain".into(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId(0),
            remaining: 3,
        };
        let config = ExecutorConfig {
            max_activations_per_step: 10,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 0,
            catch_panics: false,
            collect_metrics: false,
            drain_timeout: None,
        };
        let mut executor = DataflowExecutor::new_test(vec![Box::new(op)], config, 0);
        executor.cancel.cancel();

        let result = executor.run();
        assert!(result.is_err(), "should return Err(Cancelled)");
        match result.unwrap_err() {
            Error::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got: {other:?}"),
        }
    }

    #[test]
    fn drain_timeout_expires_returns_cancelled() {
        // An operator that never finishes (always returns MadeProgress).
        struct InfiniteOperator;
        impl SchedulableOperator for InfiniteOperator {
            fn activate(&mut self) -> Result<ActivationOutcome> {
                Ok(ActivationOutcome::MadeProgress)
            }
            fn name(&self) -> &str {
                "infinite"
            }
            fn index(&self) -> usize {
                0
            }
            fn stage_id(&self) -> crate::dataflow::stage::StageId {
                crate::dataflow::stage::StageId(0)
            }
            fn close_inputs(&mut self) {}
            fn is_done(&self) -> bool {
                false
            }
        }

        let config = ExecutorConfig {
            max_activations_per_step: 10,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 0,
            catch_panics: false,
            collect_metrics: false,
            // Very short timeout so test doesn't hang.
            drain_timeout: Some(std::time::Duration::from_millis(50)),
        };
        let mut executor = DataflowExecutor::new_test(vec![Box::new(InfiniteOperator)], config, 0);
        executor.cancel.cancel();

        let result = executor.run();
        assert!(result.is_err(), "drain timeout should return Err");
        match result.unwrap_err() {
            Error::Cancelled { .. } => {}
            other => panic!("expected Cancelled, got: {other:?}"),
        }
    }

    #[test]
    fn drain_ignores_external_inputs_open() {
        // Verify that drain mode completes even when external_inputs_open > 0,
        // without zeroing the counter (avoids race with fetch_sub in operators).
        let op = CountingOperator {
            name: "drain-ignore".into(),
            index: 0,
            stage_id: crate::dataflow::stage::StageId(0),
            remaining: 1,
        };
        let config = ExecutorConfig {
            max_activations_per_step: 10,
            max_idle_sweeps: 3,
            max_sweeps_per_poll: 0,
            catch_panics: false,
            collect_metrics: false,
            drain_timeout: Some(std::time::Duration::from_secs(5)),
        };
        let mut executor = DataflowExecutor::new_test(vec![Box::new(op)], config, 0);

        // Simulate external input being open.
        executor
            .external_inputs_open
            .store(1, std::sync::atomic::Ordering::SeqCst);

        executor.cancel.cancel();
        let result = executor.run();
        assert!(result.is_ok(), "drain should complete despite open inputs");

        // Counter is NOT zeroed — operators close naturally via their own logic.
        assert_eq!(
            executor
                .external_inputs_open
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
