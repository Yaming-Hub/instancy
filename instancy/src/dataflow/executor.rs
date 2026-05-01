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

use crate::cancellation::CancellationToken;
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::schedulable::{
    ActivationOutcome, ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
};
use crate::error::{Error, Result};

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
/// or is cancelled.
pub struct DataflowExecutor {
    /// Running operators, indexed by operator index.
    operators: Vec<Box<dyn SchedulableOperator>>,
    /// Ready queue: operator indices that need activation.
    ready_queue: VecDeque<usize>,
    /// Tracks which operators are done.
    done: Vec<bool>,
    /// Maps operator index → position in the operators vec (used for precise activation).
    #[allow(dead_code)]
    index_to_pos: Vec<usize>,
    /// Configuration.
    config: ExecutorConfig,
    /// Cancellation token for graceful shutdown.
    cancel: CancellationToken,
}

impl DataflowExecutor {
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

        Ok(Self {
            operators,
            ready_queue,
            done,
            index_to_pos,
            config,
            cancel,
        })
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

                if self.done[pos] {
                    continue;
                }

                let outcome = self.operators[pos].activate()?;

                match outcome {
                    ActivationOutcome::MadeProgress => {
                        any_progress = true;
                        // Re-queue: operator may have more work.
                        self.ready_queue.push_back(pos);
                    }
                    ActivationOutcome::Idle => {
                        // Don't re-queue. Will be re-added on next sweep.
                    }
                    ActivationOutcome::BlockedOnBackpressure => {
                        // Re-queue at the back; downstream needs to drain first.
                        self.ready_queue.push_back(pos);
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
                if consecutive_idle >= self.config.max_idle_sweeps {
                    // Quiescent — no operator made progress. Return false
                    // to distinguish from normal completion.
                    return Ok(false);
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
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
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
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig::default(),
            cancel,
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
            done: vec![false, false],
            index_to_pos: vec![0, 1],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
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
            done: vec![],
            index_to_pos: vec![],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
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
            done: vec![false],
            index_to_pos: vec![0],
            config: ExecutorConfig { max_activations_per_step: 10, max_idle_sweeps: 3 },
            cancel: CancellationToken::new(),
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
            done: vec![false, false, false],
            index_to_pos: vec![0, 1, 2],
            config: ExecutorConfig::default(),
            cancel: CancellationToken::new(),
        };

        let result = executor.run();
        assert_eq!(result.unwrap(), true);
        assert!(executor.is_complete());

        // Verify the sink collected the right data by checking it's done.
        assert_eq!(executor.completed_count(), 3);
    }
}
