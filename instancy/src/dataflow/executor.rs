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
use std::marker::PhantomData;

use crate::cancellation::CancellationToken;
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{
    ActivationOutcome, ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
};
use crate::error::{Error, Result};
use crate::progress::notificator::Notificator;
use crate::progress::subgraph::ProgressTracker;
use crate::progress::timestamp::Timestamp;

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
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_activations_per_step: 1024,
            max_idle_sweeps: 3,
        }
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
    pub fn materialize(
        graph: &DataflowGraph,
        mut operator_factories: Vec<(usize, OperatorFactory)>,
        channel_factories: Vec<(usize, ChannelFactory)>,
        config: ExecutorConfig,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let edges = graph.edges();

        // Phase 1: Create channels for each edge.
        // channel_endpoints[edge_index] = (push_end, pull_end)
        let mut push_ends: Vec<Option<Box<dyn std::any::Any + Send>>> = Vec::new();
        let mut pull_ends: Vec<Option<Box<dyn std::any::Any + Send>>> = Vec::new();

        // Channel factories are indexed by edge index.
        // Create a map from edge_index → factory.
        let mut factory_map: std::collections::HashMap<usize, ChannelFactory> =
            channel_factories.into_iter().collect();

        for (edge_idx, _edge) in edges.iter().enumerate() {
            let factory = factory_map.remove(&edge_idx).ok_or_else(|| {
                Error::Custom(format!("No channel factory for edge index {edge_idx}"))
            })?;
            let capacity = 1024; // TODO: make configurable per edge
            let (push, pull) = factory(capacity);
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

        for (op_idx, factory) in operator_factories {
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

            let operator = factory(endpoints);
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
    /// Updates per-operator notificators with new frontiers.
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
                    if pos != usize::MAX && pos < self.notificators.len() {
                        let frontier = tracker.operator_input_frontier_meet(op_idx);
                        return Some((pos, frontier));
                    }
                }
                None
            })
            .collect();

        // Apply frontier updates to notificators.
        for (pos, frontier) in frontier_updates {
            if let Some(notificator) = &mut self.notificators[pos] {
                notificator.update_frontier(&frontier);
            }
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

        // Also check if any operator has ready notifications (may not be dirty)
        for pos in 0..self.notificators.len() {
            if self.done[pos] || self.in_queue[pos] {
                continue;
            }
            if let Some(notificator) = &self.notificators[pos] {
                if notificator.has_ready() {
                    self.ready_queue.push_back(pos);
                    self.in_queue[pos] = true;
                    activated = true;
                }
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
        let mut consecutive_idle = 0;

        loop {
            // Check cancellation.
            self.cancel.check()?;

            // If all operators are done, we're finished.
            if self.done.iter().all(|&d| d) {
                return Ok(true);
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
                    return Ok(true);
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
                consecutive_idle = 0;
            } else {
                consecutive_idle += 1;
            }

            // After each batch, propagate progress and enqueue dirty operators.
            if self.propagate_progress() {
                consecutive_idle = 0;
            }

            if consecutive_idle >= self.config.max_idle_sweeps {
                // Check if external inputs are still open — if so, don't
                // declare quiescence. Sleep briefly to avoid busy-polling.
                if self.external_inputs_open.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    consecutive_idle = 0;
                    continue;
                }
                // Quiescent — no operator made progress. Return false
                // to distinguish from normal completion.
                return Ok(false);
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
            config: ExecutorConfig { max_activations_per_step: 10, max_idle_sweeps: 3 },
            cancel: CancellationToken::new(),
        progress_tracker: None,
            notificators: Vec::new(),
            probes: Vec::new(),
            external_inputs_open: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
            _phantom: std::marker::PhantomData::<u64>,
        };

        // Request notification at time 3 — frontier already past, fires immediately
        executor.notify_at(0, 3);
        let ready = executor.drain_notifications(0);
        assert_eq!(ready, vec![3]);
    }
}
