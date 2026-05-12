use std::marker::PhantomData;

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::executor::{ExecutorConfig, SweepOutcome};
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::dataflow::stage::StageId;
use crate::error::{DataflowError, Error, Result};
use crate::progress::frontier::Antichain;
use crate::progress::frontier_aggregator::FrontierAggregator;
use crate::progress::notificator::Notificator;
use crate::progress::subgraph::ProgressTracker;
use crate::progress::timestamp::Timestamp;

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
            _phantom: PhantomData,
        };
        executor.sync_probes();
        executor
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

            if self.exchange_inputs.iter().any(|input| !input.is_all_done()) {
                Ok(SweepOutcome::WaitingForInput)
            } else {
                Ok(SweepOutcome::Quiescent)
            }
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

    /// Returns `true` when all local operators are done and all exchange inputs are drained.
    pub(crate) fn is_completed(&self) -> bool {
        self.done.iter().all(|done| *done) && self.exchange_inputs.iter().all(ExchangeInput::is_all_done)
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
                        self.async_waiting[pos] = false;
                    }
                    ActivationOutcome::Idle => {
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
}

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
        );

        assert_eq!(executor.stage_id(), StageId::new(7));
        assert_eq!(executor.worker_index(), 3);
        assert_eq!(executor.operators.len(), 1);
        assert_eq!(executor.done, vec![false]);
        assert_eq!(executor.exchange_inputs.len(), 1);
        assert_eq!(executor.exchange_outputs.len(), 1);
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
        );

        assert!(executor.is_completed());
    }
}
