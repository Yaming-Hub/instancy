//! Dataflow construction and execution bridge.
//!
//! The [`build_and_run`] function provides the full pipeline from user-defined
//! graph construction to running the dataflow on the executor:
//!
//! 1. User calls `build_and_run()` with a closure that constructs the graph
//! 2. The closure registers operators and edges via `BuildContext`
//! 3. `BuildContext` collects operator/channel factories
//! 4. The graph is materialized into a `DataflowExecutor` with bounded channels
//! 5. The executor runs to completion
//!
//! This bridges the logical graph construction (§4.5) with the physical
//! execution engine (§5.4).

use std::sync::{Arc, Mutex};

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::bounded::bounded_channel;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::executor::{DataflowExecutor, ExecutorConfig};
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator};
use crate::dataflow::stream::Slot;
use crate::dataflow::wired_operators::{WiredSourceOperator, WiredUnaryOperator};
use crate::error::{Error, Result};
use crate::progress::change_batch::ChangeBatch;
use crate::progress::operate::PortConnectivity;
use crate::progress::reachability::Location;
use crate::progress::subgraph::SubgraphBuilder;
use crate::progress::timestamp::Timestamp;

/// Configuration for the dataflow builder.
#[derive(Debug, Clone)]
pub struct BuilderConfig {
    /// Default capacity for bounded channels between operators.
    pub channel_capacity: usize,
    /// Maximum consecutive idle sweeps before the executor terminates.
    pub max_idle_sweeps: usize,
}

impl Default for BuilderConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            max_idle_sweeps: 64,
        }
    }
}

/// Collects operator factories and channel factories during graph construction.
///
/// The user builds their dataflow by calling methods on the `BuildContext`
/// which records factories for deferred materialization.
pub struct BuildContext<T: Timestamp> {
    /// The dataflow graph (logical topology).
    graph: DataflowGraph,
    /// Operator factories indexed by operator index.
    operator_factories: Vec<(usize, OperatorFactory)>,
    /// Channel factories indexed by edge index.
    channel_factories: Vec<(usize, ChannelFactory)>,
    /// SubgraphBuilder for progress tracking.
    subgraph_builder: SubgraphBuilder<T>,
    /// Registered probes: (operator_index, probe_handle).
    probes: Vec<(usize, ProbeHandle<T>)>,
    /// Next available operator index (starts at 1, 0 reserved for scope boundary).
    next_operator_index: usize,
    /// Default channel capacity.
    channel_capacity: usize,
}

impl<T: Timestamp> BuildContext<T> {
    fn new(channel_capacity: usize) -> Self {
        Self {
            graph: DataflowGraph::new(),
            operator_factories: Vec::new(),
            channel_factories: Vec::new(),
            subgraph_builder: SubgraphBuilder::new(0, 0),
            probes: Vec::new(),
            next_operator_index: 1,
            channel_capacity,
        }
    }

    /// Allocate a new operator index.
    pub fn allocate_operator_index(&mut self) -> usize {
        let idx = self.next_operator_index;
        self.next_operator_index += 1;
        idx
    }

    /// Register a source operator (no inputs, one output) with pre-loaded data.
    ///
    /// The source will emit all data batches in order, then complete.
    /// Returns the operator index assigned.
    ///
    /// # Type Safety
    ///
    /// The data type `D` must match the type used by any downstream sink connected
    /// via [`add_sink`]. Mismatched types will cause a runtime panic during
    /// materialization. Future versions may enforce this at compile time via
    /// typed stream handles.
    pub fn add_source<D: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        data: Vec<(T, Vec<D>)>,
    ) -> usize {
        let name = name.into();
        let op_idx = self.allocate_operator_index();
        let region_id = RegionId::new(0);

        // Register in graph
        self.graph
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_idx, &name, region_id, 0, 1,
            ))
            .expect("operator index unique");

        // Register in subgraph builder with initial capability at T::minimum().
        // Source has 0 inputs, 1 output — holds capability until all data emitted.
        let mut initial_cap = ChangeBatch::new();
        initial_cap.update(T::minimum(), 1);
        let progress = self.subgraph_builder.add_operator_with_capabilities(
            op_idx,
            &name,
            0, // no inputs
            1, // one output
            PortConnectivity::new(0, 1),
            vec![initial_cap],
        );
        // Clone the reporter so the source operator can report capability drops.
        let reporter = progress.reporter(0).clone();

        // Create operator factory — receives output pusher from materializer.
        // Only single fan-out is supported (one edge from source output port 0).
        let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
            assert!(
                endpoints.output_pushers.len() <= 1,
                "source operator does not support fan-out (multiple edges from same output port)"
            );
            let output_pusher: Box<dyn Push<T, D>> = if !endpoints.output_pushers.is_empty() {
                *endpoints.output_pushers.into_iter().next().unwrap()
                    .downcast::<Box<dyn Push<T, D>>>()
                    .expect("source output pusher type mismatch — ensure sink D matches source D")
            } else {
                // No downstream consumer — use a no-op pusher that discards
                Box::new(NullPush)
            };

            Box::new(WiredSourceOperator::with_progress(
                name, op_idx, region_id, data, output_pusher, reporter,
            )) as Box<dyn SchedulableOperator>
        });

        self.operator_factories.push((op_idx, factory));
        op_idx
    }

    /// Register a unary operator (one input, one output) with a transformation closure.
    ///
    /// The closure receives each timestamped batch and returns a transformed batch.
    /// This is the primary way to add computation to a dataflow pipeline.
    ///
    /// # Parameters
    ///
    /// - `name`: Human-readable operator name for diagnostics.
    /// - `source_op_idx`: The upstream operator whose output feeds this operator's input.
    /// - `logic`: A closure `FnMut(T, Vec<D1>) -> Vec<D2>` that transforms each batch.
    ///
    /// # Returns
    ///
    /// The operator index for this unary operator, which can be passed to
    /// `add_sink`, `add_unary`, or `add_probe` to continue the pipeline.
    ///
    /// # Type Safety
    ///
    /// `D1` must match the output type of the upstream operator. `D2` becomes
    /// the output type visible to downstream operators. Mismatches cause a
    /// runtime panic during materialization.
    pub fn add_unary<D1, D2, F>(
        &mut self,
        name: impl Into<String>,
        source_op_idx: usize,
        logic: F,
    ) -> usize
    where
        D1: Send + 'static,
        D2: Send + 'static,
        F: FnMut(T, Vec<D1>) -> Vec<D2> + Send + 'static,
    {
        let name = name.into();
        let op_idx = self.allocate_operator_index();
        let region_id = RegionId::new(0);

        // Register in graph (1 input, 1 output)
        self.graph
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_idx, &name, region_id, 1, 1,
            ))
            .expect("operator index unique");

        // Register edge from upstream output to this operator's input
        self.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
            Slot::new(source_op_idx, 0),
            Slot::new(op_idx, 0),
            region_id,
            region_id,
        ));

        // Register in subgraph builder — unary has 1 input, 1 output with
        // identity path summary (timestamps pass through unchanged).
        self.subgraph_builder.add_operator(
            op_idx,
            &name,
            1, // one input
            1, // one output
            PortConnectivity::identity(T::Summary::default()),
        );

        // Register edge in subgraph builder for progress/frontier tracking.
        self.subgraph_builder.add_edge(
            Location::source(source_op_idx, 0),
            Location::target(op_idx, 0),
        );

        // Wrap the simple batch-transform closure into the WiredUnaryOperator's
        // full logic signature: FnMut(&mut InputHandle, &mut OutputHandle) -> Result<()>
        let mut logic = logic;
        let wired_logic = move |input: &mut crate::dataflow::operators::handles::InputHandle<T, D1>,
                                output: &mut crate::dataflow::operators::handles::OutputHandle<T, D2>|
                                -> Result<()> {
            while let Some((time, data)) = input.next() {
                let result = logic(time.clone(), data);
                if !result.is_empty() {
                    output.push_vec(time, result);
                }
            }
            Ok(())
        };

        // Operator factory — wires input puller and output pusher at materialization time.
        let name_clone = name.clone();
        let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
            let input_puller: Box<dyn Pull<T, D1>> = *endpoints.input_pullers
                .into_iter().next()
                .expect("unary must have input puller")
                .downcast::<Box<dyn Pull<T, D1>>>()
                .expect("unary input puller type mismatch");

            let output_pusher: Box<dyn Push<T, D2>> = if !endpoints.output_pushers.is_empty() {
                *endpoints.output_pushers.into_iter().next().unwrap()
                    .downcast::<Box<dyn Push<T, D2>>>()
                    .expect("unary output pusher type mismatch")
            } else {
                Box::new(NullPush)
            };

            Box::new(WiredUnaryOperator::new(
                name_clone, op_idx, region_id, wired_logic, input_puller, output_pusher,
            )) as Box<dyn SchedulableOperator>
        });
        self.operator_factories.push((op_idx, factory));

        // Channel factory for the input edge
        let edge_idx = self.graph.edges().len() - 1;
        let capacity = self.channel_capacity;
        let channel_factory: ChannelFactory = Box::new(move |_cap: usize| {
            let (push, pull) = bounded_channel::<T, D1, ()>(capacity);
            (
                Box::new(Box::new(push) as Box<dyn Push<T, D1>>) as Box<dyn std::any::Any + Send>,
                Box::new(Box::new(pull) as Box<dyn Pull<T, D1>>) as Box<dyn std::any::Any + Send>,
            )
        });
        self.channel_factories.push((edge_idx, channel_factory));

        op_idx
    }

    /// Register a sink operator (one input, no outputs) that collects received data.
    ///
    /// Must be connected to a source via `source_op_idx`.
    /// Returns `(operator_index, shared_collector)`.
    pub fn add_sink<D: Send + 'static>(
        &mut self,
        name: impl Into<String>,
        source_op_idx: usize,
    ) -> (usize, Arc<Mutex<Vec<(T, Vec<D>)>>>) {
        let name = name.into();
        let op_idx = self.allocate_operator_index();
        let region_id = RegionId::new(0);

        // Register in graph
        self.graph
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_idx, &name, region_id, 1, 0,
            ))
            .expect("operator index unique");

        // Register edge
        self.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
            Slot::new(source_op_idx, 0),
            Slot::new(op_idx, 0),
            region_id,
            region_id,
        ));

        // Register in subgraph builder — sink has 1 input, 0 outputs.
        self.subgraph_builder.add_operator(
            op_idx,
            &name,
            1, // one input
            0, // no outputs
            PortConnectivity::new(1, 0),
        );

        // Register edge in subgraph builder for progress/frontier tracking.
        self.subgraph_builder.add_edge(
            Location::source(source_op_idx, 0),
            Location::target(op_idx, 0),
        );

        // Shared collector for external access
        let collector: Arc<Mutex<Vec<(T, Vec<D>)>>> = Arc::new(Mutex::new(Vec::new()));
        let collector_clone = collector.clone();

        // Operator factory
        let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
            let input_puller: Box<dyn Pull<T, D>> = *endpoints.input_pullers
                .into_iter().next()
                .expect("sink must have input puller")
                .downcast::<Box<dyn Pull<T, D>>>()
                .expect("sink input puller type mismatch");

            Box::new(CollectingSink::new(name, op_idx, region_id, input_puller, collector_clone))
                as Box<dyn SchedulableOperator>
        });
        self.operator_factories.push((op_idx, factory));

        // Channel factory for the edge
        let edge_idx = self.graph.edges().len() - 1;
        let capacity = self.channel_capacity;
        let channel_factory: ChannelFactory = Box::new(move |_cap: usize| {
            let (push, pull) = bounded_channel::<T, D, ()>(capacity);
            (
                Box::new(Box::new(push) as Box<dyn Push<T, D>>) as Box<dyn std::any::Any + Send>,
                Box::new(Box::new(pull) as Box<dyn Pull<T, D>>) as Box<dyn std::any::Any + Send>,
            )
        });
        self.channel_factories.push((edge_idx, channel_factory));

        (op_idx, collector)
    }

    /// Register a probe at a specific operator to observe its input frontier.
    ///
    /// Returns a [`ProbeHandle`] that tracks the frontier at the given operator.
    /// Use `probe.done_with(&time)` to check if the operator has processed
    /// all data at or before `time`, or `probe.is_done()` for full completion.
    ///
    /// # Panics
    ///
    /// Panics if `operator_index` refers to a source (0-input) operator.
    /// Sources have no input frontier to observe — probe a downstream sink instead.
    pub fn add_probe(&mut self, operator_index: usize) -> ProbeHandle<T> {
        // Validate: operator must have at least 1 input for meaningful frontier.
        let op_info = self.graph.operator(operator_index)
            .expect("add_probe: invalid operator index");
        assert!(
            op_info.input_count > 0,
            "Cannot probe source operator (0 inputs) — probe a downstream operator instead"
        );
        let probe = ProbeHandle::new();
        self.probes.push((operator_index, probe.clone()));
        probe
    }

    /// Get the number of registered operators.
    pub fn operator_count(&self) -> usize {
        self.next_operator_index - 1
    }
}

// ---------------------------------------------------------------------------
// CollectingSink — sink operator that writes to an external Arc<Mutex<Vec>>
// ---------------------------------------------------------------------------

/// A sink operator that collects received data into a shared vector.
struct CollectingSink<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    input_puller: Box<dyn Pull<T, D>>,
    collector: Arc<Mutex<Vec<(T, Vec<D>)>>>,
    input_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> CollectingSink<T, D> {
    fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        input_puller: Box<dyn Pull<T, D>>,
        collector: Arc<Mutex<Vec<(T, Vec<D>)>>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            input_puller,
            collector,
            input_exhausted: false,
            done: false,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for CollectingSink<T, D> {
    fn activate(&mut self) -> crate::error::Result<crate::dataflow::schedulable::ActivationOutcome> {
        use crate::dataflow::channels::Payload;
        use crate::dataflow::schedulable::ActivationOutcome;

        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        let mut made_progress = false;
        while let Some(envelope) = self.input_puller.pull() {
            if let Payload::Data { time, data } = envelope.payload {
                self.collector.lock().unwrap().push((time, data));
                made_progress = true;
            }
        }

        if self.input_puller.is_exhausted() {
            self.input_exhausted = true;
            self.done = true;
            return Ok(ActivationOutcome::Done);
        }

        if made_progress {
            Ok(ActivationOutcome::MadeProgress)
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

    fn region_id(&self) -> RegionId {
        self.region_id
    }

    fn close_inputs(&mut self) {
        self.input_exhausted = true;
    }
}

// ---------------------------------------------------------------------------
// NullPush — discards all pushed envelopes (for unconnected source outputs)
// ---------------------------------------------------------------------------

struct NullPush;

impl<T: Timestamp, D: Send + 'static> Push<T, D> for NullPush {
    fn push(&mut self, _envelope: crate::dataflow::channels::Envelope<T, D>) -> Result<()> {
        Ok(())
    }

    fn try_push(
        &mut self,
        _envelope: crate::dataflow::channels::Envelope<T, D>,
    ) -> std::result::Result<(), (crate::error::Error, crate::dataflow::channels::Envelope<T, D>)> {
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) {}

    fn is_closed(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build and execute a dataflow to completion.
///
/// # Parameters
///
/// - `config`: Controls channel capacity and idle sweep limits (see [`BuilderConfig`]).
/// - `build_fn`: A closure that constructs the logical dataflow graph. It receives a
///   [`BuildContext`] for registering operators and edges (e.g., `add_source`, `add_sink`).
///   The closure returns a user-chosen value `R` — typically handles to result collectors
///   (like `Arc<Mutex<Vec<...>>>` from `add_sink`) so the caller can inspect output after
///   execution completes.
///
/// # Returns
///
/// `Ok(R)` — the value returned by `build_fn` — after the dataflow runs to completion.
/// `Err(Error::Cancelled)` if the cancellation token fires during execution.
/// `Err(Error::Custom(...))` if the dataflow reaches quiescence without completing.
///
/// # Example
///
/// ```ignore
/// let collector = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
///     let src = ctx.add_source("nums", vec![(0u64, vec![1, 2, 3])]);
///     let (_, collector) = ctx.add_sink::<i32>("output", src);
///     Ok(collector)
/// })?;
/// let results = collector.lock().unwrap();
/// ```
pub fn build_and_run<T, F, R>(config: BuilderConfig, build_fn: F) -> Result<R>
where
    T: Timestamp,
    F: FnOnce(&mut BuildContext<T>) -> Result<R>,
{
    build_and_run_with_cancel(config, CancellationToken::new(), build_fn)
}

/// Build and execute a dataflow with a specific cancellation token.
///
/// Same as [`build_and_run`] but accepts a [`CancellationToken`] for cooperative
/// shutdown. If the token is already cancelled before execution starts, the executor
/// returns `Err(Error::Cancelled)` immediately.
pub fn build_and_run_with_cancel<T, F, R>(
    config: BuilderConfig,
    cancel: CancellationToken,
    build_fn: F,
) -> Result<R>
where
    T: Timestamp,
    F: FnOnce(&mut BuildContext<T>) -> Result<R>,
{
    let mut ctx = BuildContext::new(config.channel_capacity);

    // Build the logical dataflow graph: the user's closure registers operators
    // and edges into the BuildContext, producing the logical topology.
    let user_result = build_fn(&mut ctx)?;

    // If no operators, return immediately
    if ctx.operator_count() == 0 {
        return Ok(user_result);
    }

    // Materialize executor
    let executor_config = ExecutorConfig {
        max_activations_per_step: 1024,
        max_idle_sweeps: config.max_idle_sweeps,
    };

    // Materialize: converts the logical graph (operators + edges) into a physical
    // executor with real channels and operator instances wired together.
    let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
        &ctx.graph,
        ctx.operator_factories,
        ctx.channel_factories,
        executor_config,
        cancel,
    )?;

    // Build and attach progress tracker
    let mut tracker = ctx.subgraph_builder.build();
    tracker.initialize();
    executor.set_progress_tracker(tracker);

    // Register probes so they receive frontier updates during propagation.
    for (op_idx, probe) in ctx.probes {
        executor.register_probe(op_idx, probe);
    }

    // Run to completion
    let completed = executor.run()?;
    if !completed {
        return Err(Error::Custom("dataflow did not complete (quiescence without termination)".into()));
    }

    Ok(user_result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_dataflow_completes() {
        let result = build_and_run::<u64, _, _>(BuilderConfig::default(), |_ctx| Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn source_to_sink_pipeline() {
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 2, 3]),
                (1u64, vec![4, 5]),
            ]);
            let (_sink_idx, collector) = ctx.add_sink::<i32>("output", source);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all_values: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all_values.len(), 5);
        assert!(all_values.contains(&1));
        assert!(all_values.contains(&5));
    }

    #[test]
    fn source_only_completes() {
        let result = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            ctx.add_source("numbers", vec![(0u64, vec![1i32, 2, 3])]);
            Ok(())
        });
        assert!(result.is_ok());
    }

    #[test]
    fn cancellation_stops_execution() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = build_and_run_with_cancel::<u64, _, _>(
            BuilderConfig::default(),
            cancel,
            |ctx| {
                ctx.add_source("numbers", vec![(0u64, vec![1i32, 2, 3])]);
                Ok(())
            },
        );
        // Pre-cancelled token causes executor to return Cancelled error
        assert!(matches!(result, Err(crate::error::Error::Cancelled)));
    }

    #[test]
    fn multiple_sources_to_sinks() {
        let (c1, c2) = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let src1 = ctx.add_source("src1", vec![(0u64, vec![10i32, 20])]);
            let src2 = ctx.add_source("src2", vec![(0u64, vec![30i32, 40])]);
            let (_, collector1) = ctx.add_sink::<i32>("sink1", src1);
            let (_, collector2) = ctx.add_sink::<i32>("sink2", src2);
            Ok((collector1, collector2))
        })
        .unwrap();

        let d1 = c1.lock().unwrap();
        let d2 = c2.lock().unwrap();
        let v1: Vec<i32> = d1.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        let v2: Vec<i32> = d2.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(v1, vec![10, 20]);
        assert_eq!(v2, vec![30, 40]);
    }

    #[test]
    fn backpressure_with_small_channel() {
        let config = BuilderConfig {
            channel_capacity: 2,
            max_idle_sweeps: 128,
        };

        let collected = build_and_run::<u64, _, _>(config, |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32]),
                (1u64, vec![2]),
                (2u64, vec![3]),
                (3u64, vec![4]),
                (4u64, vec![5]),
            ]);
            let (_, collector) = ctx.add_sink::<i32>("output", source);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all_values: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all_values.len(), 5);
    }

    #[test]
    fn probe_reflects_completion() {
        let probe = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 2]),
                (1u64, vec![3]),
            ]);
            let (sink_idx, _collector) = ctx.add_sink::<i32>("output", source);
            let probe = ctx.add_probe(sink_idx);
            Ok(probe)
        })
        .unwrap();

        // After completion, probe should show empty frontier (all done).
        assert!(probe.is_done());
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&u64::MAX));
    }

    #[test]
    fn probe_on_source_only_dataflow_completes() {
        // When there's only a source (no sink), the dataflow still completes.
        // Probing the sink in the source→sink case is the meaningful use.
        let result = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![(0u64, vec![1i32])]);
            let (sink_idx, _) = ctx.add_sink::<i32>("output", source);
            let probe = ctx.add_probe(sink_idx);
            Ok(probe)
        });
        let probe = result.unwrap();
        // After full completion, the sink probe shows done.
        assert!(probe.is_done());
    }

    #[test]
    fn unary_map_doubles_values() {
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 2, 3]),
                (1u64, vec![4, 5]),
            ]);
            let mapped = ctx.add_unary::<i32, i32, _>("double", source, |_time, data| {
                data.into_iter().map(|x| x * 2).collect()
            });
            let (_, collector) = ctx.add_sink::<i32>("output", mapped);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all.len(), 5);
        assert!(all.contains(&2));
        assert!(all.contains(&4));
        assert!(all.contains(&6));
        assert!(all.contains(&8));
        assert!(all.contains(&10));
    }

    #[test]
    fn unary_filter() {
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 2, 3, 4, 5, 6]),
            ]);
            let evens = ctx.add_unary::<i32, i32, _>("filter_even", source, |_time, data| {
                data.into_iter().filter(|x| x % 2 == 0).collect()
            });
            let (_, collector) = ctx.add_sink::<i32>("output", evens);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all, vec![2, 4, 6]);
    }

    #[test]
    fn unary_type_conversion() {
        // Source produces i32, unary converts to String, sink collects String.
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![42i32, 7]),
            ]);
            let stringified = ctx.add_unary::<i32, String, _>("to_string", source, |_time, data| {
                data.into_iter().map(|x| x.to_string()).collect()
            });
            let (_, collector) = ctx.add_sink::<String>("output", stringified);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all: Vec<String> = data.iter().flat_map(|(_, v)| v.clone()).collect();
        assert_eq!(all, vec!["42".to_string(), "7".to_string()]);
    }

    #[test]
    fn chained_unary_operators() {
        // source → double → add_one → sink
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 2, 3]),
            ]);
            let doubled = ctx.add_unary::<i32, i32, _>("double", source, |_t, data| {
                data.into_iter().map(|x| x * 2).collect()
            });
            let incremented = ctx.add_unary::<i32, i32, _>("add_one", doubled, |_t, data| {
                data.into_iter().map(|x| x + 1).collect()
            });
            let (_, collector) = ctx.add_sink::<i32>("output", incremented);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        // 1*2+1=3, 2*2+1=5, 3*2+1=7
        assert_eq!(all, vec![3, 5, 7]);
    }

    #[test]
    fn unary_with_empty_output() {
        // Filter that removes all items — empty batches should be handled gracefully.
        let collected = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![1i32, 3, 5]),
            ]);
            let filtered = ctx.add_unary::<i32, i32, _>("no_evens", source, |_t, data| {
                data.into_iter().filter(|x| x % 2 == 0).collect()
            });
            let (_, collector) = ctx.add_sink::<i32>("output", filtered);
            Ok(collector)
        })
        .unwrap();

        let data = collected.lock().unwrap();
        let all: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert!(all.is_empty());
    }

    #[test]
    fn unary_with_probe() {
        let (collector, probe) = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
            let source = ctx.add_source("numbers", vec![
                (0u64, vec![10i32, 20]),
                (1u64, vec![30]),
            ]);
            let doubled = ctx.add_unary::<i32, i32, _>("double", source, |_t, data| {
                data.into_iter().map(|x| x * 2).collect()
            });
            let (sink_idx, collector) = ctx.add_sink::<i32>("output", doubled);
            let probe = ctx.add_probe(sink_idx);
            Ok((collector, probe))
        })
        .unwrap();

        assert!(probe.is_done());
        let data = collector.lock().unwrap();
        let all: Vec<i32> = data.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&20));
        assert!(all.contains(&40));
        assert!(all.contains(&60));
    }
}
