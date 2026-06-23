use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::executor::{ExecutorConfig, SweepOutcome};
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{
    ActivationOutcome, ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
    group_by_port,
};
use crate::dataflow::stage::StageId;
use crate::error::{DataflowError, Error, Result};
use crate::progress::frontier::Antichain;
use crate::progress::frontier_aggregator::FrontierAggregator;
use crate::progress::notificator::Notificator;
use crate::progress::subgraph::{ProgressTracker, SubgraphBuilder};
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerContext;

/// The runtime execution engine for a single `(stage, worker_index)` pair.
///
/// A `StageExecutor` owns only the operators that belong to one stage. Pipeline
/// channels within the stage stay local, while cross-stage communication is
/// represented by [`ExchangeInput`] and [`ExchangeOutput`] state.
pub(crate) struct StageExecutor<T: Timestamp> {
    /// Stage identity.
    stage_id: StageId,
    /// Worker index within this stage (0..parallelism).
    worker_index: usize,
    /// Running operators for this stage, indexed by position.
    operators: Vec<Box<dyn SchedulableOperator>>,
    /// Operator done flags.
    done: Vec<bool>,
    /// Fused activation order (topological) — operators activated in this order each sweep.
    fused_order: Vec<usize>,
    /// Cancellation token shared across the entire dataflow.
    cancel: CancellationToken,
    /// Exchange input ports — one per incoming exchange channel from upstream stages.
    exchange_inputs: Vec<ExchangeInput<T>>,
    /// Exchange output ports — one per outgoing exchange channel to downstream stages.
    exchange_outputs: Vec<ExchangeOutput<T>>,
    /// Optional progress tracker scoped to this stage's operators only.
    /// Used for intra-stage progress (pipeline channels within the stage).
    progress_tracker: Option<ProgressTracker<T>>,
    /// Per-operator notificators (for notify-capable operators).
    notificators: Vec<Option<Notificator<T>>>,
    /// Per-operator async-waiting flags. While any operator is waiting for
    /// async task completion, the executor must not declare quiescence.
    async_waiting: Vec<bool>,
    /// Consecutive idle sweep count for quiescence detection.
    consecutive_idle: usize,
    /// Configuration.
    config: ExecutorConfig,
    /// Probes registered for this stage.
    probes: Vec<(usize, ProbeHandle<T>)>,
    /// Probe notifiers for async waiters.
    probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
    /// Wake handle for async executor notifications.
    wake_handle: WakeHandle,
    /// Counter of external inputs that are still open. Prevents false
    /// quiescence while user code might still send data.
    external_inputs_open: Arc<AtomicUsize>,
    /// Number of active cross-stage feedback dependencies. When > 0,
    /// this stage must not quiesce because another stage may still
    /// push data through a feedback boundary channel.
    feedback_deps: Arc<AtomicUsize>,
    /// Worker index for error context.
    _phantom: PhantomData<T>,
}

/// An input port receiving `ExchangeMessage`s from an upstream stage.
pub(crate) struct ExchangeInput<T: Timestamp> {
    /// Source stage ID (for diagnostics).
    source_stage: StageId,
    /// Frontier aggregator tracking per-sender frontiers.
    aggregator: FrontierAggregator<T>,
    /// The last aggregated frontier that was delivered to operators.
    /// Used to detect when the frontier actually changes.
    last_delivered_frontier: Antichain<T>,
    /// Whether all senders on this input are done.
    all_done: bool,
}

/// An output port sending `ExchangeMessage`s to a downstream stage.
pub(crate) struct ExchangeOutput<T: Timestamp> {
    /// Target stage ID (for diagnostics).
    target_stage: StageId,
    /// The last frontier sent to downstream. Used to avoid sending
    /// redundant FrontierUpdate messages.
    last_sent_frontier: Antichain<T>,
    /// Whether this output has sent SenderDone.
    done_sent: bool,
}

/// Creates a `StageExecutor` from pre-filtered operator and channel factories.
///
/// This is the stage-scoped equivalent of `materialize_executor()`. It:
/// 1. Builds intra-stage channels from the provided channel factories
/// 2. Wires channel endpoints to operators
/// 3. Invokes operator factories to create concrete operators
/// 4. Builds a per-stage `ProgressTracker` from the `SubgraphBuilder`
/// 5. Returns a ready-to-run `StageExecutor`
///
/// Exchange channels (cross-stage) are NOT created here — the caller
/// creates them and provides `ExchangeInput`/`ExchangeOutput` objects.
///
/// **Boundary wiring:** For operators at stage boundaries, the caller must
/// provide pre-built channel endpoints via `boundary_input_pullers` and
/// `boundary_output_pushers`. These are merged with intra-stage endpoints
/// before operator construction.
///
/// **TODO(PR5):** Exchange frontier updates bypass the ProgressTracker,
/// so probe updates for exchange-input operators may lag. Either feed
/// exchange frontiers into the tracker or update probes directly from
/// exchange input state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn materialize_stage_executor<T: Timestamp>(
    stage_id: StageId,
    worker_index: usize,
    graph: &DataflowGraph,
    stage_operator_indices: &[usize],
    operator_factories: &mut [(usize, OperatorFactory)],
    channel_factories: &mut [(usize, ChannelFactory)],
    mut subgraph_builder: SubgraphBuilder<T>,
    cancel: CancellationToken,
    wake_handle: WakeHandle,
    worker_context: &WorkerContext,
    config: ExecutorConfig,
    exchange_inputs: Vec<ExchangeInput<T>>,
    exchange_outputs: Vec<ExchangeOutput<T>>,
    probes: Vec<(usize, ProbeHandle<T>)>,
    probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
    boundary_input_pullers: HashMap<usize, Vec<(usize, Box<dyn Any + Send>)>>,
    boundary_output_pushers: HashMap<usize, Vec<(usize, Box<dyn Any + Send>)>>,
    external_inputs_open: Arc<AtomicUsize>,
    feedback_deps: Arc<AtomicUsize>,
) -> Result<StageExecutor<T>> {
    let stage_operator_set: HashSet<usize> = stage_operator_indices.iter().copied().collect();
    subgraph_builder.retain_operators(&stage_operator_set);

    let intra_stage_edges: Vec<(usize, &crate::dataflow::graph::EdgeInfo)> = graph
        .edges()
        .iter()
        .enumerate()
        .filter(|(_, edge)| {
            stage_operator_set.contains(&edge.source.operator_index)
                && stage_operator_set.contains(&edge.target.operator_index)
        })
        .collect();
    let intra_stage_feedback_edges: Vec<(usize, &crate::dataflow::graph::EdgeInfo)> = graph
        .feedback_edges()
        .iter()
        .enumerate()
        .filter(|(_, edge)| {
            stage_operator_set.contains(&edge.source.operator_index)
                && stage_operator_set.contains(&edge.target.operator_index)
        })
        .map(|(idx, edge)| (graph.edges().len() + idx, edge))
        .collect();

    let factory_positions: HashMap<usize, usize> = channel_factories
        .iter()
        .enumerate()
        .map(|(pos, (edge_idx, _))| (*edge_idx, pos))
        .collect();
    let mut push_ends: HashMap<usize, Box<dyn Any + Send>> = HashMap::new();
    let mut pull_ends: HashMap<usize, Box<dyn Any + Send>> = HashMap::new();

    for (edge_idx, _) in intra_stage_edges
        .iter()
        .chain(intra_stage_feedback_edges.iter())
        .copied()
    {
        let pos = *factory_positions.get(&edge_idx).ok_or_else(|| {
            Error::Dataflow(DataflowError::MissingFactory {
                edge_index: edge_idx,
            })
        })?;
        let (_, factory) = &mut channel_factories[pos];
        let (push, pull) = factory.build(worker_context, Some(wake_handle.clone()))?;
        push_ends.insert(edge_idx, push);
        pull_ends.insert(edge_idx, pull);
    }

    let mut op_input_pullers: HashMap<usize, Vec<(usize, Box<dyn Any + Send>)>> = HashMap::new();
    let mut op_output_pushers: HashMap<usize, Vec<(usize, Box<dyn Any + Send>)>> = HashMap::new();

    for (edge_idx, edge) in &intra_stage_edges {
        let pull = pull_ends.remove(edge_idx).ok_or_else(|| {
            Error::Dataflow(DataflowError::InvalidGraph(
                "edge endpoint missing or already materialized".into(),
            ))
        })?;
        let push = push_ends.remove(edge_idx).ok_or_else(|| {
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

    for (edge_idx, edge) in &intra_stage_feedback_edges {
        let pull = pull_ends.remove(edge_idx).ok_or_else(|| {
            Error::Dataflow(DataflowError::InvalidGraph(
                "edge endpoint missing or already materialized".into(),
            ))
        })?;
        let push = push_ends.remove(edge_idx).ok_or_else(|| {
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

    // Merge pre-built boundary endpoints from exchange channels.
    for (op_idx, pullers) in boundary_input_pullers {
        op_input_pullers.entry(op_idx).or_default().extend(pullers);
    }
    for (op_idx, pushers) in boundary_output_pushers {
        op_output_pushers.entry(op_idx).or_default().extend(pushers);
    }

    let progress_reporters: Option<Arc<dyn Any + Send + Sync>> =
        Some(Arc::new(subgraph_builder.materialization_reporters()) as Arc<dyn Any + Send + Sync>);

    let mut factory_positions: Vec<usize> = operator_factories
        .iter()
        .enumerate()
        .filter_map(|(pos, (op_idx, _))| stage_operator_set.contains(op_idx).then_some(pos))
        .collect();
    factory_positions.sort_by_key(|&pos| operator_factories[pos].0);

    let mut operators: Vec<Box<dyn SchedulableOperator>> =
        Vec::with_capacity(factory_positions.len());
    let mut index_to_pos: HashMap<usize, usize> = HashMap::with_capacity(factory_positions.len());
    for factory_pos in factory_positions {
        let (op_idx, factory) = &mut operator_factories[factory_pos];
        let op_idx = *op_idx;

        let mut inputs = op_input_pullers.remove(&op_idx).unwrap_or_default();
        inputs.sort_by_key(|(port, _)| *port);
        let input_pullers = inputs.into_iter().map(|(_, pull)| pull).collect();

        let mut outputs = op_output_pushers.remove(&op_idx).unwrap_or_default();
        outputs.sort_by_key(|(port, _)| *port);
        let output_pushers = group_by_port(outputs);

        let endpoints = ChannelEndpoints {
            input_pullers,
            output_pushers,
            wake_handle: Some(wake_handle.clone()),
            progress_reporters: progress_reporters.clone(),
        };

        let operator = factory.build(worker_context, endpoints)?;
        index_to_pos.insert(op_idx, operators.len());
        operators.push(operator);
    }

    let mut tracker = subgraph_builder.build();
    tracker.initialize()?;

    // Build unique operator-pair edges for topological sort (Kahn's algorithm).
    // Multiple graph edges between the same operator pair (different ports) must
    // be counted as a single dependency edge to avoid false cycle detection.
    let mut edge_set: HashSet<(usize, usize)> = HashSet::new();
    for edge in &intra_stage_edges {
        let edge = edge.1;
        if index_to_pos.contains_key(&edge.source.operator_index)
            && index_to_pos.contains_key(&edge.target.operator_index)
        {
            edge_set.insert((edge.source.operator_index, edge.target.operator_index));
        }
    }

    let mut in_degree: HashMap<usize, usize> =
        index_to_pos.keys().copied().map(|idx| (idx, 0)).collect();
    let mut successors: HashMap<usize, Vec<usize>> = HashMap::new();
    for (source, target) in &edge_set {
        successors.entry(*source).or_default().push(*target);
        *in_degree.entry(*target).or_insert(0) += 1;
    }
    for next in successors.values_mut() {
        next.sort_unstable();
    }

    let mut zero_in_degree: Vec<usize> = in_degree
        .iter()
        .filter_map(|(idx, degree)| (*degree == 0).then_some(*idx))
        .collect();
    zero_in_degree.sort_unstable();
    let mut queue: VecDeque<usize> = zero_in_degree.into();
    let mut fused_order = Vec::with_capacity(operators.len());

    while let Some(op_idx) = queue.pop_front() {
        let pos = *index_to_pos.get(&op_idx).ok_or_else(|| {
            Error::Dataflow(DataflowError::InvalidGraph(format!(
                "stage {stage_id} missing materialized operator for index {op_idx}",
            )))
        })?;
        fused_order.push(pos);

        if let Some(targets) = successors.get(&op_idx) {
            for &target in targets {
                if let Some(degree) = in_degree.get_mut(&target) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(target);
                    }
                }
            }
        }
    }

    if fused_order.len() != operators.len() {
        return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
            "Cycle detected in stage {stage_id}: processed {} of {} operators",
            fused_order.len(),
            operators.len()
        ))));
    }

    let notificators = std::iter::repeat_with(|| None)
        .take(operators.len())
        .collect::<Vec<Option<Notificator<T>>>>();

    Ok(StageExecutor::new(
        stage_id,
        worker_index,
        operators,
        fused_order,
        cancel,
        config,
        exchange_inputs,
        exchange_outputs,
        Some(tracker),
        notificators,
        probes,
        probe_notifiers,
        wake_handle,
        external_inputs_open,
        feedback_deps,
    ))
}

impl<T: Timestamp> StageExecutor<T> {
    /// Creates a new stage executor for one `(stage, worker_index)` pair.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        stage_id: StageId,
        worker_index: usize,
        operators: Vec<Box<dyn SchedulableOperator>>,
        fused_order: Vec<usize>,
        cancel: CancellationToken,
        config: ExecutorConfig,
        exchange_inputs: Vec<ExchangeInput<T>>,
        exchange_outputs: Vec<ExchangeOutput<T>>,
        progress_tracker: Option<ProgressTracker<T>>,
        notificators: Vec<Option<Notificator<T>>>,
        probes: Vec<(usize, ProbeHandle<T>)>,
        probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
        wake_handle: WakeHandle,
        external_inputs_open: Arc<AtomicUsize>,
        feedback_deps: Arc<AtomicUsize>,
    ) -> Self {
        assert_eq!(
            fused_order.len(),
            operators.len(),
            "fused_order length ({}) must match operator count ({})",
            fused_order.len(),
            operators.len()
        );
        assert_eq!(
            notificators.len(),
            operators.len(),
            "notificators length ({}) must match operator count ({})",
            notificators.len(),
            operators.len()
        );
        assert_eq!(
            probes.len(),
            probe_notifiers.len(),
            "probes and probe_notifiers must have matching lengths"
        );

        let done = operators.iter().map(|op| op.is_done()).collect();
        let async_waiting = vec![false; operators.len()];
        let mut executor = Self {
            stage_id,
            worker_index,
            operators,
            done,
            fused_order,
            cancel,
            exchange_inputs,
            exchange_outputs,
            progress_tracker,
            notificators,
            async_waiting,
            consecutive_idle: 0,
            config,
            probes,
            probe_notifiers,
            wake_handle,
            external_inputs_open,
            feedback_deps,
            _phantom: PhantomData,
        };
        executor.sync_probes();
        executor
    }

    /// Returns the feedback dependency counter for this stage.
    /// Decremented by `CombinedStageExecutor` when a feedback-source stage completes.
    pub(crate) fn feedback_deps(&self) -> &Arc<AtomicUsize> {
        &self.feedback_deps
    }

    /// Runs one executor sweep.
    pub(crate) fn run_one_sweep(&mut self) -> Result<SweepOutcome> {
        self.cancel.check()?;

        if self.is_completed() {
            self.send_frontier_updates();
            return Ok(SweepOutcome::Completed);
        }

        let mut made_progress = false;

        if self.process_exchange_inputs() {
            made_progress = true;
        }

        if self.run_fused_activation()? {
            made_progress = true;
        }

        if self.propagate_intra_stage_progress()? {
            made_progress = true;
        }

        self.send_frontier_updates();

        // Force-close operators when the progress tracker reports completion
        // (all capabilities drained). This handles feedback loops that quiesce
        // without operators self-reporting Done.
        //
        // Guard: do NOT force-close while cross-stage feedback source stages
        // are still active — more data may arrive through the feedback
        // boundary channel even though the intra-stage tracker sees no
        // remaining capabilities.
        let fb = self.feedback_deps.load(std::sync::atomic::Ordering::SeqCst);
        let all_exchange_done = self.exchange_inputs.iter().all(|input| input.is_all_done());
        if fb == 0 && all_exchange_done {
            if let Some(ref tracker) = self.progress_tracker {
                if tracker.is_completed() {
                    for pos in 0..self.operators.len() {
                        if !self.done[pos] {
                            self.operators[pos].close_inputs();
                            self.done[pos] = true;
                        }
                    }
                }
            }
        }

        if self.is_completed() {
            self.consecutive_idle = 0;
            return Ok(SweepOutcome::Completed);
        }

        if made_progress {
            self.consecutive_idle = 0;
            self.wake_handle.notify();
            return Ok(SweepOutcome::MadeProgress);
        }

        self.consecutive_idle += 1;
        if self.consecutive_idle >= self.config.max_idle_sweeps {
            self.consecutive_idle = 0;

            // Don't declare quiescence while any operator has in-flight async work.
            if self.async_waiting.iter().any(|&w| w) {
                return Ok(SweepOutcome::WaitingForInput);
            }

            // Don't declare quiescence while external inputs (user-facing
            // InputSender / AsyncInputSender) are still open — more data
            // may arrive.
            if self
                .external_inputs_open
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
            {
                return Ok(SweepOutcome::WaitingForInput);
            }

            // Report quiescence even when cross-stage dependencies
            // (exchange_inputs, feedback_deps) are still open.
            // CombinedStageExecutor detects global quiescence across
            // all stages and handles coordinated termination.
            //
            // NOTE: This is per-worker quiescence, not global. With
            // in-process channels (single-machine staged execution),
            // data pushed by another worker is immediately available,
            // so 64 idle sweeps without activation means all workers
            // are idle — the loop has converged. With network channels
            // (cross-process), latency could cause premature quiescence;
            // cross-worker coordination would be needed in that case.
            Ok(SweepOutcome::Quiescent)
        } else {
            Ok(SweepOutcome::Idle)
        }
    }

    /// Processes exchange input frontier changes and delivers them to operators.
    ///
    /// TODO(PR3/PR4): Map each exchange input to specific operator input ports
    /// and compute per-operator combined input frontiers instead of broadcasting
    /// to all operators.
    pub(crate) fn process_exchange_inputs(&mut self) -> bool {
        let frontier_updates: Vec<Antichain<T>> = self
            .exchange_inputs
            .iter_mut()
            .filter_map(ExchangeInput::frontier_changed)
            .collect();

        if frontier_updates.is_empty() {
            return false;
        }

        for frontier in &frontier_updates {
            for (pos, operator) in self.operators.iter_mut().enumerate() {
                if let Some(notificator) = self.notificators.get_mut(pos).and_then(|n| n.as_mut()) {
                    notificator.update_frontier(frontier);
                }
                operator.update_input_frontier(frontier);
            }
        }

        self.wake_handle.notify();
        true
    }

    /// Records outbound frontier advances for exchange outputs.
    ///
    /// TODO(PR5): Track per-output source operator/port and compute
    /// per-exchange-edge frontiers for multi-output stages.
    pub(crate) fn send_frontier_updates(&mut self) {
        let completed = self.is_completed();
        let current_frontier = if completed {
            Antichain::new()
        } else {
            self.current_output_frontier()
        };

        for output in &mut self.exchange_outputs {
            if output.should_send_frontier(&current_frontier) {
                output.record_frontier_sent(current_frontier.clone());
            }
            if completed && !output.is_done() {
                output.mark_done();
            }
        }
    }

    /// Returns `true` when all local operators are done.
    ///
    /// Operators self-report as done when their input channels are exhausted
    /// (e.g., `is_exhausted()` returns true after the upstream push side drops).
    /// Exchange input frontier tracking (via `ExchangeInput`) is bookkeeping
    /// for future inline watermark support and does not gate completion.
    pub(crate) fn is_completed(&self) -> bool {
        self.done.iter().all(|done| *done)
    }

    /// Returns this executor's stage identifier.
    pub(crate) fn stage_id(&self) -> StageId {
        self.stage_id
    }

    /// Returns this executor's worker index within the stage.
    pub(crate) fn worker_index(&self) -> usize {
        self.worker_index
    }

    fn run_fused_activation(&mut self) -> Result<bool> {
        if self.fused_order.is_empty() || self.config.max_activations_per_step == 0 {
            return Ok(false);
        }

        let mut any_progress = false;
        let mut productive_activations = 0usize;
        let budget = self.config.max_activations_per_step;
        let positions = self.fused_order.clone();
        let mut made_progress_this_pass = true;

        while made_progress_this_pass {
            made_progress_this_pass = false;

            for pos in positions.iter().copied() {
                if pos >= self.operators.len() {
                    return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                        "stage {} fused order position {} out of bounds for {} operators",
                        self.stage_id,
                        pos,
                        self.operators.len()
                    ))));
                }
                if self.done[pos] {
                    continue;
                }
                if productive_activations >= budget {
                    return Ok(any_progress);
                }

                let outcome = activate_operator(
                    &mut self.operators[pos],
                    self.config.catch_panics,
                    self.worker_index,
                )?;

                match outcome {
                    ActivationOutcome::MadeProgress | ActivationOutcome::BlockedOnBackpressure => {
                        productive_activations += 1;
                        any_progress = true;
                        made_progress_this_pass = true;
                        // Don't clear async_waiting here — the operator may still
                        // have in-flight tasks even while collecting results. The
                        // flag is only cleared on Idle (no in-flight work) or Done.
                    }
                    ActivationOutcome::Idle => {
                        // No work and no in-flight tasks — safe to clear.
                        self.async_waiting[pos] = false;
                    }
                    ActivationOutcome::WaitingForAsync => {
                        self.async_waiting[pos] = true;
                    }
                    ActivationOutcome::Done => {
                        productive_activations += 1;
                        self.done[pos] = true;
                        self.async_waiting[pos] = false;
                        any_progress = true;
                        made_progress_this_pass = true;
                    }
                }
            }
        }

        Ok(any_progress)
    }

    fn propagate_intra_stage_progress(&mut self) -> Result<bool> {
        let operator_positions: Vec<(usize, usize)> = self
            .operators
            .iter()
            .enumerate()
            .map(|(pos, op)| (op.index(), pos))
            .collect();
        let probe_indices: Vec<usize> = self.probes.iter().map(|(op_idx, _)| *op_idx).collect();

        let (frontier_updates, probe_frontiers) = {
            let Some(tracker) = self.progress_tracker.as_mut() else {
                return Ok(false);
            };

            let dirty = tracker.propagate()?.to_vec();
            let frontier_updates = dirty
                .into_iter()
                .filter_map(|op_idx| {
                    operator_positions.iter().find_map(|(index, pos)| {
                        if *index == op_idx {
                            Some((*pos, tracker.operator_input_frontier_meet(op_idx)))
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>();
            let probe_frontiers = probe_indices
                .iter()
                .map(|op_idx| tracker.operator_input_frontier_meet(*op_idx))
                .collect::<Vec<_>>();
            (frontier_updates, probe_frontiers)
        };

        for (pos, frontier) in &frontier_updates {
            if let Some(notificator) = self.notificators.get_mut(*pos).and_then(|n| n.as_mut()) {
                notificator.update_frontier(frontier);
            }
            self.operators[*pos].update_input_frontier(frontier);
        }

        for (i, frontier) in probe_frontiers.iter().enumerate() {
            if let Some((_, probe)) = self.probes.get(i) {
                probe.update_frontier(frontier);
            }
            if let Some(notifier) = self.probe_notifiers.get(i) {
                notifier.notify(frontier);
            }
        }

        let notifications_ready = self
            .notificators
            .iter()
            .flatten()
            .any(Notificator::has_ready)
            || self
                .operators
                .iter()
                .enumerate()
                .any(|(pos, op)| !self.done[pos] && op.has_ready_notifications());

        let changed = !frontier_updates.is_empty() || notifications_ready;
        if changed {
            self.wake_handle.notify();
        }
        Ok(changed)
    }

    fn current_output_frontier(&self) -> Antichain<T> {
        let Some(tracker) = self.progress_tracker.as_ref() else {
            return if self.done.iter().all(|done| *done) {
                Antichain::new()
            } else {
                Antichain::from_elem(T::minimum())
            };
        };

        let mut frontier = Antichain::new();
        for operator in &self.operators {
            let op_idx = operator.index();
            if let Some(shape) = tracker.operator_shape(op_idx) {
                for output in 0..shape.outputs {
                    for time in tracker.output_frontier(op_idx, output).elements() {
                        frontier.insert(time.clone());
                    }
                }
            }
        }

        if frontier.is_empty() && !tracker.is_completed() {
            Antichain::from_elem(T::minimum())
        } else {
            frontier
        }
    }

    fn sync_probes(&mut self) {
        let Some(tracker) = self.progress_tracker.as_ref() else {
            return;
        };

        for (i, (op_idx, probe)) in self.probes.iter().enumerate() {
            let frontier = tracker.operator_input_frontier_meet(*op_idx);
            probe.update_frontier(&frontier);
            if let Some(notifier) = self.probe_notifiers.get(i) {
                notifier.notify(&frontier);
            }
        }
    }

    /// Async poll driver: runs sweeps until completion, quiescence, or budget
    /// exhaustion, then registers the waker and returns `Poll::Pending`.
    ///
    /// Mirrors the protocol in `DataflowExecutor::poll_run`:
    /// 1. Run sweeps until idle or budget exhausted
    /// 2. Register the waker
    /// 3. Re-check for notifications (race-safe)
    /// 4. Only return Pending if no notification is pending
    pub(crate) fn poll_run(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
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
                    if budget > 0 && sweeps_this_poll >= budget {
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
                    self.wake_handle.register_waker(cx.waker());
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
}

/// `StageExecutor` implements `Future` so it can be registered with the
/// executor task registry and polled cooperatively by the worker pool.
///
/// Resolves to `Ok(true)` on normal completion, `Ok(false)` on quiescence,
/// or `Err(...)` on error/cancellation.
impl<T: Timestamp> Future for StageExecutor<T> {
    type Output = Result<bool>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_run(cx)
    }
}

// StageExecutor is Unpin because all its fields are behind indirection
// (Arc, Box, Vec) or are plain data. PhantomData<T> does not store T inline.
impl<T: Timestamp> Unpin for StageExecutor<T> {}

/// Groups all `StageExecutor`s that belong to the **same worker** into a
/// single async task.  `CombinedStageExecutor` is topology-agnostic: it has
/// no knowledge of stage identities, exchange wiring, or operator logic.
/// Its only job is to poll each contained executor and drop it when done.
///
/// There is exactly **one** `CombinedStageExecutor` per worker, registered as
/// one tokio task.  For example, with Stage A (par=2) and Stage B (par=3)
/// across 3 workers:
///
/// ```text
/// Worker 0:  CombinedStageExecutor { StageExec(A,0), StageExec(B,0) }
/// Worker 1:  CombinedStageExecutor { StageExec(A,1), StageExec(B,1) }
/// Worker 2:  CombinedStageExecutor { StageExec(B,2) }     // A's par=2, so no A here
/// ```
///
/// Dropping a completed stage is critical: it releases the stage's
/// `ExchangePush` endpoints, closing the underlying channels so downstream
/// stages can detect end-of-input via `is_exhausted()`.
pub(crate) struct CombinedStageExecutor<T: Timestamp> {
    /// Each slot is `Some` while running, set to `None` on completion so
    /// the executor (and its boundary channels) are dropped immediately.
    stages: Vec<Option<StageExecutor<T>>>,
    wake_handle: WakeHandle,
    /// Maps stage index (in `stages` vec) → list of feedback_deps counters
    /// that should be decremented when that stage completes or is dropped.
    /// Populated from cross-stage feedback edges: when stage B feeds back
    /// to stage A, completing B decrements A's feedback_deps.
    feedback_release: HashMap<usize, Vec<Arc<AtomicUsize>>>,
    /// Shared loop in-flight counters, one per `iterate` scope. A feedback loop
    /// whose body crosses a stage boundary cannot be tracked by the per-stage
    /// progress trackers; these counters (recorded by enter/leave operators) let
    /// convergence wait until the loop is globally drained instead of declaring
    /// it done on local quiescence — the unsound shortcut that previously dropped
    /// in-flight loop data.
    loop_inflight: Vec<Arc<std::sync::atomic::AtomicI64>>,
}

impl<T: Timestamp> CombinedStageExecutor<T> {
    pub(crate) fn new(
        stages: Vec<StageExecutor<T>>,
        wake_handle: WakeHandle,
        feedback_release: HashMap<usize, Vec<Arc<AtomicUsize>>>,
        loop_inflight: Vec<Arc<std::sync::atomic::AtomicI64>>,
    ) -> Self {
        let stages = stages.into_iter().map(Some).collect();
        Self {
            stages,
            wake_handle,
            feedback_release,
            loop_inflight,
        }
    }
}

impl<T: Timestamp> Future for CombinedStageExecutor<T> {
    type Output = Result<bool>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let budget = 64usize;
        let mut polled = 0usize;
        let mut any_pending = false;
        let mut any_completed_this_poll = false;
        let mut all_remaining_quiesced = true;

        for i in 0..this.stages.len() {
            let Some(stage) = this.stages[i].as_mut() else {
                continue;
            };

            polled += 1;
            match Pin::new(stage).poll(cx) {
                Poll::Ready(Ok(true)) => {
                    // Release feedback dependencies: stages that were waiting
                    // for this stage's feedback can now proceed to quiescence.
                    if let Some(deps) = this.feedback_release.get(&i) {
                        for dep in deps {
                            dep.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    // Drop the completed stage to release its boundary channels
                    // (ExchangePush/Pull endpoints), unblocking downstream stages.
                    this.stages[i] = None;
                    any_completed_this_poll = true;
                }
                Poll::Ready(Ok(false)) => {
                    // Stage quiesced — keep it alive.Other stages in this
                    // CombinedStageExecutor may still produce data for it
                    // (e.g., cross-stage feedback loops). Only when ALL
                    // remaining stages quiesce do we know the loop converged.
                    any_pending = true;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    any_pending = true;
                    all_remaining_quiesced = false;
                }
            }

            if polled >= budget && this.stages.iter().any(|s| s.is_some()) {
                this.wake_handle.register_waker(cx.waker());
                this.wake_handle.notify();
                return Poll::Pending;
            }
        }

        // A feedback loop has truly converged only when every record that
        // entered it has left — i.e. all loop in-flight counters are zero. Local
        // quiescence alone is unsound: data may still be buffered in a
        // cross-stage exchange/feedback channel that no stage has drained yet,
        // and dropping the stages here would silently lose it.
        let loops_drained = this
            .loop_inflight
            .iter()
            .all(|c| c.load(std::sync::atomic::Ordering::SeqCst) <= 0);

        let all_done = this.stages.iter().all(|s| s.is_none());
        if all_done {
            this.wake_handle.clear_waker();
            Poll::Ready(Ok(true))
        } else if any_pending
            && all_remaining_quiesced
            && !any_completed_this_poll
            && loops_drained
            && !this.loop_inflight.is_empty()
        {
            // Quiescence-based convergence exists ONLY to break feedback loops:
            // a cycle never reaches input exhaustion on its own, so once every
            // loop in-flight counter is drained (`loops_drained`) and all stages
            // are quiescent, the loop has converged and its stages can be
            // dropped. The `!loop_inflight.is_empty()` guard restricts this to
            // dataflows that actually contain a loop.
            //
            // ACYCLIC staged dataflows must NOT converge this way. With no loop
            // counter to prove the cross-stage exchange channels are drained,
            // local quiescence is unsound: a downstream stage can be idle simply
            // because an upstream stage on another worker hasn't pushed its data
            // yet. Dropping it here strands that in-flight gather/scatter data
            // (issue: premature completion under aggressive quiescence). Acyclic
            // dataflows instead terminate via the exhaustion cascade — an
            // upstream stage completes, drops its `ExchangePush`, the downstream
            // boundary `ExchangePull` becomes exhausted, its operators drain and
            // finish — which is sound regardless of the idle-sweep budget.
            // Release all feedback deps before dropping stages.
            for (i, stage) in this.stages.iter().enumerate() {
                if stage.is_some() {
                    if let Some(deps) = this.feedback_release.get(&i) {
                        for dep in deps {
                            dep.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                }
            }
            // Drop all remaining stages to release their channels.
            for stage in &mut this.stages {
                *stage = None;
            }
            this.wake_handle.clear_waker();
            Poll::Ready(Ok(true))
        } else if any_pending || any_completed_this_poll {
            // If a stage completed this poll, wake immediately so downstream
            // stages can observe the newly-closed channels.
            this.wake_handle.register_waker(cx.waker());
            if any_completed_this_poll || this.wake_handle.take_notification() {
                this.wake_handle.notify();
            }
            Poll::Pending
        } else {
            this.wake_handle.clear_waker();
            Poll::Ready(Ok(false))
        }
    }
}

impl<T: Timestamp> Unpin for CombinedStageExecutor<T> {}

impl<T: Timestamp> ExchangeInput<T> {
    /// Creates a new exchange input for a fixed number of upstream senders.
    pub(crate) fn new(source_stage: StageId, num_senders: usize) -> Self {
        Self {
            source_stage,
            aggregator: FrontierAggregator::new(num_senders),
            last_delivered_frontier: Antichain::from_elem(T::minimum()),
            all_done: false,
        }
    }

    /// Updates one sender's frontier.
    pub(crate) fn update_frontier(&mut self, sender: usize, frontier: Antichain<T>) {
        self.aggregator.update_sender(sender, frontier);
        self.all_done = self.aggregator.is_all_done();
    }

    /// Marks one sender as finished.
    pub(crate) fn mark_sender_done(&mut self, sender: usize) {
        self.aggregator.mark_sender_done(sender);
        self.all_done = self.aggregator.is_all_done();
    }

    /// Returns the aggregated frontier when it differs from the last delivered value.
    pub(crate) fn frontier_changed(&mut self) -> Option<Antichain<T>> {
        let frontier = self.aggregator.frontier().clone();
        if frontier == self.last_delivered_frontier {
            None
        } else {
            self.last_delivered_frontier = frontier.clone();
            Some(frontier)
        }
    }

    /// Returns `true` if all upstream senders have sent `SenderDone`.
    pub(crate) fn is_all_done(&self) -> bool {
        self.all_done
    }
}

impl<T: Timestamp> ExchangeOutput<T> {
    /// Creates a new exchange output for one downstream stage.
    pub(crate) fn new(target_stage: StageId) -> Self {
        Self {
            target_stage,
            last_sent_frontier: Antichain::from_elem(T::minimum()),
            done_sent: false,
        }
    }

    /// Returns `true` when a new frontier should be sent downstream.
    pub(crate) fn should_send_frontier(&self, current_frontier: &Antichain<T>) -> bool {
        &self.last_sent_frontier != current_frontier
    }

    /// Records that a frontier update was sent downstream.
    pub(crate) fn record_frontier_sent(&mut self, frontier: Antichain<T>) {
        self.last_sent_frontier = frontier;
    }

    /// Marks this output as having sent `SenderDone`.
    pub(crate) fn mark_done(&mut self) {
        self.done_sent = true;
    }

    /// Returns `true` if `SenderDone` has already been sent.
    pub(crate) fn is_done(&self) -> bool {
        self.done_sent
    }
}

fn activate_operator(
    op: &mut Box<dyn SchedulableOperator>,
    catch_panics: bool,
    worker_index: usize,
) -> Result<ActivationOutcome> {
    if catch_panics {
        let op_name = op.name().to_string();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| op.activate())) {
            Ok(result) => result.map_err(|err| err.with_operator_context(op_name, worker_index)),
            Err(payload) => Err(Error::OperatorPanic {
                operator: op_name,
                worker_index: Some(worker_index),
                message: extract_panic_message(&payload),
            }),
        }
    } else {
        let op_name = op.name().to_string();
        op.activate()
            .map_err(|err| err.with_operator_context(op_name, worker_index))
    }
}

fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::dataflow::graph::OperatorInfo;
    use crate::progress::operate::PortConnectivity;

    struct MockOperator {
        name: String,
        index: usize,
        stage_id: StageId,
        done: bool,
    }

    impl SchedulableOperator for MockOperator {
        fn activate(&mut self) -> Result<ActivationOutcome> {
            if self.done {
                Ok(ActivationOutcome::Done)
            } else {
                Ok(ActivationOutcome::Idle)
            }
        }

        fn is_done(&self) -> bool {
            self.done
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn index(&self) -> usize {
            self.index
        }

        fn stage_id(&self) -> StageId {
            self.stage_id
        }

        fn close_inputs(&mut self) {
            self.done = true;
        }
    }

    #[test]
    fn exchange_input_new_starts_at_minimum() {
        let input = ExchangeInput::<u64>::new(StageId::new(1), 2);

        assert_eq!(input.source_stage, StageId::new(1));
        assert_eq!(input.last_delivered_frontier, Antichain::from_elem(0));
        assert!(!input.all_done);
    }

    #[test]
    fn exchange_input_update_frontier_reports_change() {
        let mut input = ExchangeInput::<u64>::new(StageId::new(1), 1);

        input.update_frontier(0, Antichain::from_elem(5));

        assert_eq!(input.frontier_changed(), Some(Antichain::from_elem(5)));
    }

    #[test]
    fn exchange_input_multiple_senders_track_minimum() {
        let mut input = ExchangeInput::<u64>::new(StageId::new(1), 2);

        input.update_frontier(0, Antichain::from_elem(5));
        assert_eq!(input.frontier_changed(), None);

        input.update_frontier(1, Antichain::from_elem(7));
        assert_eq!(input.frontier_changed(), Some(Antichain::from_elem(5)));

        input.update_frontier(0, Antichain::from_elem(8));
        assert_eq!(input.frontier_changed(), Some(Antichain::from_elem(7)));
    }

    #[test]
    fn exchange_input_all_senders_done_sets_all_done() {
        let mut input = ExchangeInput::<u64>::new(StageId::new(1), 2);

        input.mark_sender_done(0);
        assert!(!input.is_all_done());

        input.mark_sender_done(1);
        assert!(input.is_all_done());
    }

    #[test]
    fn exchange_input_frontier_changed_none_when_unchanged() {
        let mut input = ExchangeInput::<u64>::new(StageId::new(1), 1);

        assert_eq!(input.frontier_changed(), None);
        input.update_frontier(0, Antichain::from_elem(3));
        assert_eq!(input.frontier_changed(), Some(Antichain::from_elem(3)));
        assert_eq!(input.frontier_changed(), None);
    }

    #[test]
    fn exchange_output_new_starts_at_minimum_and_not_done() {
        let output = ExchangeOutput::<u64>::new(StageId::new(2));

        assert_eq!(output.target_stage, StageId::new(2));
        assert_eq!(output.last_sent_frontier, Antichain::from_elem(0));
        assert!(!output.is_done());
    }

    #[test]
    fn exchange_output_should_send_for_different_frontier() {
        let output = ExchangeOutput::<u64>::new(StageId::new(2));

        assert!(output.should_send_frontier(&Antichain::from_elem(4)));
    }

    #[test]
    fn exchange_output_record_frontier_sent_suppresses_duplicates() {
        let mut output = ExchangeOutput::<u64>::new(StageId::new(2));
        let frontier = Antichain::from_elem(6);

        output.record_frontier_sent(frontier.clone());

        assert!(!output.should_send_frontier(&frontier));
    }

    #[test]
    fn exchange_output_mark_done_sets_done() {
        let mut output = ExchangeOutput::<u64>::new(StageId::new(2));

        output.mark_done();

        assert!(output.is_done());
    }

    #[test]
    fn stage_executor_basic_construction() {
        let operators: Vec<Box<dyn SchedulableOperator>> = vec![Box::new(MockOperator {
            name: "mock".to_string(),
            index: 0,
            stage_id: StageId::new(7),
            done: false,
        })];

        let executor = StageExecutor::<u64>::new(
            StageId::new(7),
            3,
            operators,
            vec![0],
            CancellationToken::new(),
            ExecutorConfig::default(),
            vec![ExchangeInput::<u64>::new(StageId::new(1), 1)],
            vec![ExchangeOutput::<u64>::new(StageId::new(9))],
            None,
            vec![None],
            Vec::new(),
            Vec::new(),
            WakeHandle::new(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );

        assert_eq!(executor.stage_id(), StageId::new(7));
        assert_eq!(executor.worker_index(), 3);
        assert_eq!(executor.operators.len(), 1);
        assert_eq!(executor.done, vec![false]);
        assert_eq!(executor.exchange_inputs.len(), 1);
        assert_eq!(executor.exchange_outputs.len(), 1);
    }

    #[test]
    fn materialize_stage_executor_empty_stage() {
        let graph = DataflowGraph::new();
        graph.validate().unwrap();

        let executor = materialize_stage_executor::<u64>(
            StageId::new(11),
            0,
            &graph,
            &[],
            &mut [],
            &mut [],
            SubgraphBuilder::new(0, 0),
            CancellationToken::new(),
            WakeHandle::new(),
            &WorkerContext::single(),
            ExecutorConfig::default(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();

        assert_eq!(executor.stage_id(), StageId::new(11));
        assert!(executor.operators.is_empty());
        assert!(executor.fused_order.is_empty());
        assert!(executor.done.is_empty());
        assert!(executor.progress_tracker.is_some());
    }

    #[test]
    fn materialize_stage_executor_single_operator() {
        let stage_id = StageId::new(12);
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(1, "solo", stage_id, 0, 0))
            .unwrap();
        graph.validate().unwrap();

        let mut operator_factories = [(
            1usize,
            OperatorFactory::new(move |_ctx, endpoints| {
                assert!(endpoints.input_pullers.is_empty());
                assert!(endpoints.output_pushers.is_empty());
                assert!(endpoints.progress_reporters.is_some());
                Ok(Box::new(MockOperator {
                    name: "solo".to_string(),
                    index: 1,
                    stage_id,
                    done: false,
                }) as Box<dyn SchedulableOperator>)
            }),
        )];
        let mut subgraph_builder = SubgraphBuilder::new(0, 0);
        subgraph_builder
            .add_operator(1, "solo", 0, 0, PortConnectivity::new(0, 0))
            .unwrap();

        let executor = materialize_stage_executor::<u64>(
            stage_id,
            0,
            &graph,
            &[1],
            &mut operator_factories,
            &mut [],
            subgraph_builder,
            CancellationToken::new(),
            WakeHandle::new(),
            &WorkerContext::single(),
            ExecutorConfig::default(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
        .unwrap();

        assert_eq!(executor.stage_id(), stage_id);
        assert_eq!(executor.worker_index(), 0);
        assert_eq!(executor.operators.len(), 1);
        assert_eq!(executor.fused_order, vec![0]);
        assert_eq!(executor.done, vec![false]);
        assert_eq!(executor.notificators.len(), 1);
        assert!(executor.notificators[0].is_none());
        assert!(executor.progress_tracker.is_some());
    }

    #[test]
    fn stage_executor_is_completed_when_operators_done_and_inputs_done() {
        let operators: Vec<Box<dyn SchedulableOperator>> = vec![Box::new(MockOperator {
            name: "done".to_string(),
            index: 0,
            stage_id: StageId::new(4),
            done: true,
        })];
        let mut exchange_input = ExchangeInput::<u64>::new(StageId::new(1), 1);
        exchange_input.mark_sender_done(0);

        let executor = StageExecutor::<u64>::new(
            StageId::new(4),
            0,
            operators,
            vec![0],
            CancellationToken::new(),
            ExecutorConfig::default(),
            vec![exchange_input],
            Vec::new(),
            None,
            vec![None],
            Vec::new(),
            Vec::new(),
            WakeHandle::new(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );

        assert!(executor.is_completed());
    }
}
