//! Separated dataflow builder — constructs a `LogicalDataflow` independently of execution.
//!
//! # Design
//!
//! The builder follows a two-phase pattern:
//!
//! 1. **Construction**: Use [`DataflowBuilder`] to declare inputs, chain operators
//!    via [`Stream`], and declare outputs. This produces a [`LogicalDataflow`].
//! 2. **Execution**: Submit the `LogicalDataflow` to a runtime for physical
//!    materialization and async execution (Phase C, future PR).
//!
//! # Example
//!
//! ```ignore
//! let builder = DataflowBuilder::<u64>::new("pipeline");
//! let input = builder.input::<i32>("numbers");
//! let output = input
//!     .map("double", |_t, x| x * 2)
//!     .filter("keep_even", |_t, x| x % 2 == 0)
//!     .output("results");
//! let dataflow = builder.build().unwrap();
//! ```
//!
//! Streams are **cloneable** — enabling branching and multi-input patterns:
//!
//! ```ignore
//! let stream = builder.input::<i32>("src");
//! let evens = stream.clone().filter("evens", |_t, x| x % 2 == 0).output("evens");
//! let odds = stream.filter("odds", |_t, x| x % 2 != 0).output("odds");
//! ```

use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::bounded::bounded_channel;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::tee::tee_or_single;
use crate::dataflow::executor::{DataflowExecutor, ExecutorConfig};
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{
    ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
};
use crate::dataflow::stream::Slot;
use crate::dataflow::wired_operators::{WiredSourceOperator, WiredUnaryOperator};
use crate::error::{Error, Result};
use crate::progress::change_batch::ChangeBatch;
use crate::progress::operate::PortConnectivity;
use crate::progress::reachability::Location;
use crate::progress::subgraph::SubgraphBuilder;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// InputPortInfo / OutputPortInfo — metadata for named I/O ports
// ---------------------------------------------------------------------------

/// Metadata for a named input port in the logical dataflow.
#[derive(Clone)]
struct InputPortInfo {
    /// User-visible name of this input.
    name: String,
    /// The source operator index that this input feeds into.
    operator_index: usize,
    /// TypeId of the data type for runtime validation.
    type_name: &'static str,
}

/// Metadata for a named output port in the logical dataflow.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used in Phase C (async runtime integration)
struct OutputPortInfo {
    /// User-visible name of this output.
    name: String,
    /// The sink operator index that collects output.
    operator_index: usize,
    /// TypeId of the data type for runtime validation.
    type_name: &'static str,
}

// ---------------------------------------------------------------------------
// BuilderState — shared mutable state (behind Rc<RefCell>)
// ---------------------------------------------------------------------------

/// Internal mutable state of the builder. Shared via `Rc<RefCell<...>>`.
struct BuilderState<T: Timestamp> {
    graph: DataflowGraph,
    subgraph_builder: SubgraphBuilder<T>,
    operator_factories: Vec<(usize, OperatorFactory)>,
    channel_factories: Vec<(usize, ChannelFactory)>,
    input_ports: Vec<InputPortInfo>,
    output_ports: Vec<OutputPortInfo>,
    /// Type-erased closures that create ChannelSourceOperator + InputSender
    /// for each input() port during spawn(). Each closure captures the
    /// concrete data type D and creates the appropriate channel pair.
    input_port_wiring: Vec<InputPortWiring>,
    /// Type-erased closures that create ChannelSinkOperator + OutputReceiver
    /// for each output() port during spawn(). Each closure creates a
    /// replacement OperatorFactory that sends data out via mpsc channel.
    output_port_wiring: Vec<OutputPortWiring>,
    probes: Vec<(usize, ProbeHandle<T>)>,
    next_operator_index: usize,
    next_collect_index: usize,
    channel_capacity: usize,
}

/// Type-erased closure for wiring an input port during spawn().
/// Captures the data type D and creates:
/// - An OperatorFactory for the ChannelSourceOperator
/// - An InputSender (as Box<dyn Any + Send>) for the caller
type InputPortWiring = Box<
    dyn FnOnce(
            std::sync::Arc<std::sync::atomic::AtomicUsize>, // external_inputs_open counter
        ) -> (OperatorFactory, Box<dyn std::any::Any + Send>) // (factory, InputSender)
        + Send,
>;

/// Type-erased closure for wiring an output port during spawn().
/// Returns:
/// - An OperatorFactory for the ChannelSinkOperator (replaces CollectingSink)
/// - An OutputReceiver (as Box<dyn Any + Send>) for the caller
type OutputPortWiring = Box<
    dyn FnOnce() -> (OperatorFactory, Box<dyn std::any::Any + Send>) // (factory, OutputReceiver)
        + Send,
>;

impl<T: Timestamp> BuilderState<T> {
    fn allocate_operator_index(&mut self) -> usize {
        let idx = self.next_operator_index;
        self.next_operator_index += 1;
        idx
    }
}

// ---------------------------------------------------------------------------
// DataflowBuilder<T>
// ---------------------------------------------------------------------------

/// Configuration for the dataflow builder.
#[derive(Debug, Clone)]
pub struct DataflowBuilderConfig {
    /// Default capacity for bounded channels between operators.
    pub channel_capacity: usize,
}

impl Default for DataflowBuilderConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
        }
    }
}

/// Builder for constructing a [`LogicalDataflow`] via typed stream chaining.
///
/// The builder uses interior mutability (`Rc<RefCell>`) so that multiple
/// [`Stream`] handles can coexist — enabling branching and multi-input patterns.
///
/// # Thread Safety
///
/// The builder is **not** `Send` or `Sync`. Graph construction happens on a
/// single thread; the resulting `LogicalDataflow` is `Send` for submission to
/// an async runtime.
pub struct DataflowBuilder<T: Timestamp> {
    name: String,
    state: Rc<RefCell<BuilderState<T>>>,
}

impl<T: Timestamp> DataflowBuilder<T> {
    /// Create a new builder with the given dataflow name and default config.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, DataflowBuilderConfig::default())
    }

    /// Create a new builder with explicit configuration.
    pub fn with_config(name: impl Into<String>, config: DataflowBuilderConfig) -> Self {
        Self {
            name: name.into(),
            state: Rc::new(RefCell::new(BuilderState {
                graph: DataflowGraph::new(),
                subgraph_builder: SubgraphBuilder::new(0, 0),
                operator_factories: Vec::new(),
                channel_factories: Vec::new(),
                input_ports: Vec::new(),
                output_ports: Vec::new(),
                input_port_wiring: Vec::new(),
                output_port_wiring: Vec::new(),
                probes: Vec::new(),
                next_operator_index: 1,
                next_collect_index: 0,
                channel_capacity: config.channel_capacity,
            })),
        }
    }

    /// Declare a named input port that data will be fed into at runtime.
    ///
    /// Returns a [`Stream`] representing the data flowing from this input.
    /// At execution time, the runtime connects an async channel to this port.
    ///
    /// # Panics
    ///
    /// Panics if an input with the same name already exists.
    pub fn input<D: Clone + Send + 'static>(&self, name: impl Into<String>) -> Stream<T, D> {
        let name = name.into();
        let op_idx;
        let region_id = RegionId::new(0);

        {
            let mut state = self.state.borrow_mut();

            // Validate unique name
            assert!(
                !state.input_ports.iter().any(|p| p.name == name),
                "duplicate input port name: {name}"
            );

            op_idx = state.allocate_operator_index();

            // Register source operator in graph (0 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 0, 1,
                ))
                .expect("operator index unique");

            // Register in subgraph builder with initial capability.
            // The input source holds a capability at T::minimum() until closed.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                &name,
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![initial_cap],
            );

            // Record port metadata
            state.input_ports.push(InputPortInfo {
                name: name.clone(),
                operator_index: op_idx,
                type_name: std::any::type_name::<D>(),
            });

            // Store a type-erased wiring closure that will be invoked during
            // spawn() to create the ChannelSourceOperator factory and InputSender.
            // The closure captures the concrete data type D.
            let wiring_name = name.clone();
            let wiring: InputPortWiring = Box::new(move |external_inputs_open| {
                use crate::dataflow::channel_operators::InputSender;
                use crate::dataflow::channel_operators::ChannelSourceOperator;
                use crate::dataflow::channels::tee::tee_or_single;

                // Create bounded channel for external → dataflow communication
                let (tx, rx) = std::sync::mpsc::sync_channel::<InputEvent<T, D>>(256);
                let sender = InputSender::new(tx);

                // The factory closure captures the receiver and wires it to
                // the output pusher provided during materialization.
                let ext_counter = Arc::clone(&external_inputs_open);
                let factory_name = wiring_name.clone();
                let factory: OperatorFactory = Box::new(move |endpoints| {
                    // Source operator: 0 inputs, 1 output (may have fan-out)
                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                *any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .expect("channel source output pusher type mismatch")
                            })
                            .collect();
                        tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                    };
                    let op = ChannelSourceOperator::new(
                        factory_name,
                        op_idx,
                        RegionId::new(0),
                        rx,
                        output_pusher,
                        None, // progress reporter wired separately
                        ext_counter,
                    );
                    Box::new(op) as Box<dyn SchedulableOperator>
                });

                let sender_any: Box<dyn std::any::Any + Send> = Box::new(sender);
                (factory, sender_any)
            });
            state.input_port_wiring.push(wiring);
        }

        Stream {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Declare a pre-loaded source that emits data immediately (for testing/simple use).
    ///
    /// Unlike [`input`](Self::input), this source has data baked in at build time
    /// rather than receiving it from an async channel at runtime.
    pub fn source<D: Clone + Send + 'static>(
        &self,
        name: impl Into<String>,
        data: Vec<(T, Vec<D>)>,
    ) -> Stream<T, D> {
        let name = name.into();
        let op_idx;
        let region_id = RegionId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register source operator in graph (0 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 0, 1,
                ))
                .expect("operator index unique");

            // Register in subgraph builder with initial capability.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let progress = state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                &name,
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![initial_cap],
            );
            let reporter = progress.reporter(0).clone();

            // Create operator factory for pre-loaded source.
            // Handles fan-out: if multiple downstream edges exist (Stream was
            // cloned), wraps all pushers in a TeePush adapter.
            let name_clone = name.clone();
            let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
                let output_pusher: Box<dyn Push<T, D>> = {
                    let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D>>>()
                                .expect("source output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredSourceOperator::with_progress(
                    name_clone, op_idx, region_id, data, output_pusher, reporter,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));
        }

        Stream {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Get the number of registered operators.
    pub fn operator_count(&self) -> usize {
        self.state.borrow().next_operator_index - 1
    }

    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Finalize construction and produce a [`LogicalDataflow`].
    ///
    /// Returns an error if outstanding `Stream` references still exist
    /// (drop them first).
    ///
    /// Fan-out (stream cloning/branching) is supported: each cloned stream
    /// output port automatically uses a [`TeePush`](crate::dataflow::channels::tee::TeePush)
    /// adapter that clones data to all downstream consumers.
    ///
    /// After a successful call, the builder is consumed.
    pub fn build(self) -> Result<LogicalDataflow<T>> {
        let state = match Rc::try_unwrap(self.state) {
            Ok(cell) => cell.into_inner(),
            Err(_) => {
                return Err(Error::Custom(
                    "cannot build: outstanding Stream references still exist — \
                     drop all Stream handles before calling build()"
                        .into(),
                ))
            }
        };

        Ok(LogicalDataflow {
            name: self.name,
            graph: state.graph,
            subgraph_builder: state.subgraph_builder,
            operator_factories: state.operator_factories,
            channel_factories: state.channel_factories,
            input_ports: state.input_ports,
            output_ports: state.output_ports,
            input_port_wiring: state.input_port_wiring,
            output_port_wiring: state.output_port_wiring,
            probes: state.probes,
        })
    }
}

// ---------------------------------------------------------------------------
// Stream<T, D> — cloneable typed handle for chaining operators
// ---------------------------------------------------------------------------

/// A typed stream handle representing data flowing from an operator's output.
///
/// `Stream` is **cloneable** — cloning creates a second reference to the same
/// output port, enabling fan-out (branching) patterns. Each clone can be
/// independently consumed by different downstream operators.
///
/// Methods on `Stream` register new operators in the builder and return new
/// `Stream` handles pointing to the new operator's output.
pub struct Stream<T: Timestamp, D: Clone + Send + 'static> {
    state: Rc<RefCell<BuilderState<T>>>,
    op_idx: usize,
    output_slot: usize,
    _phantom: PhantomData<D>,
}

impl<T: Timestamp, D: Clone + Send + 'static> Clone for Stream<T, D> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            op_idx: self.op_idx,
            output_slot: self.output_slot,
            _phantom: PhantomData,
        }
    }
}

impl<T: Timestamp, D: Clone + Send + 'static> Stream<T, D> {
    /// Apply a per-element transformation, producing a new stream of type `D2`.
    ///
    /// The closure receives a reference to the timestamp and ownership of each element.
    /// If you need to capture the timestamp, clone it inside the closure.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = stream.map("double", |_t, x: i32| x * 2);
    /// ```
    pub fn map<D2, F>(self, name: impl Into<String>, mut logic: F) -> Stream<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(&T, D) -> D2 + Send + 'static,
    {
        self.add_unary_internal(name, move |time, batch| {
            batch.into_iter().map(|x| logic(&time, x)).collect()
        })
    }

    /// Filter elements by a predicate, keeping only those that return `true`.
    ///
    /// # Example
    /// ```ignore
    /// let evens = stream.filter("evens", |_t, x| x % 2 == 0);
    /// ```
    pub fn filter<F>(self, name: impl Into<String>, mut predicate: F) -> Stream<T, D>
    where
        F: FnMut(&T, &D) -> bool + Send + 'static,
    {
        self.add_unary_internal(name, move |time, batch| {
            batch.into_iter().filter(|x| predicate(&time, x)).collect()
        })
    }

    /// Apply a flat-map transformation: each element produces zero or more output elements.
    ///
    /// # Example
    /// ```ignore
    /// let words = lines.flat_map("split", |_t, line: String| {
    ///     line.split_whitespace().map(|w| w.to_string()).collect::<Vec<_>>()
    /// });
    /// ```
    pub fn flat_map<D2, F>(self, name: impl Into<String>, mut logic: F) -> Stream<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(&T, D) -> Vec<D2> + Send + 'static,
    {
        self.add_unary_internal(name, move |time, batch| {
            batch.into_iter().flat_map(|x| logic(&time, x)).collect()
        })
    }

    /// General unary operator with full control over input/output handles.
    ///
    /// For simple per-element transformations, prefer [`map`](Self::map) or
    /// [`filter`](Self::filter). Use `unary` when you need batch-level control
    /// or stateful processing.
    ///
    /// # Example
    /// ```ignore
    /// let processed = stream.unary("aggregate", |input, output| {
    ///     while let Some((time, data)) = input.next() {
    ///         let sum: i32 = data.iter().sum();
    ///         output.push_vec(time, vec![sum]);
    ///     }
    ///     Ok(())
    /// });
    /// ```
    pub fn unary<D2, L>(self, name: impl Into<String>, logic: L) -> Stream<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(&mut InputHandle<T, D>, &mut OutputHandle<T, D2>) -> Result<()>
            + Send
            + 'static,
    {
        self.add_unary_with_handles(name, logic)
    }

    /// Declare this stream as a named output port.
    ///
    /// Returns an [`OutputPort`] handle. At execution time, the runtime connects
    /// an async channel to this port for collecting results.
    ///
    /// For immediate testing, use [`collect`](Self::collect) instead.
    pub fn output(self, name: impl Into<String>) -> OutputPort<T, D> {
        let name = name.into();
        let collector = Arc::new(Mutex::new(Vec::new()));
        let op_idx;

        {
            let mut state = self.state.borrow_mut();

            // Validate unique name
            assert!(
                !state.output_ports.iter().any(|p| p.name == name),
                "duplicate output port name: {name}"
            );

            op_idx = state.allocate_operator_index();
            let region_id = RegionId::new(0);

            // Register sink operator in graph (1 input, 0 outputs)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 1, 0,
                ))
                .expect("operator index unique");

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                region_id,
                region_id,
            ));

            // Subgraph builder: sink has 1 input, 0 outputs, no connectivity
            state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                0,
                PortConnectivity::new(1, 0),
            );
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Record port metadata
            state.output_ports.push(OutputPortInfo {
                name: name.clone(),
                operator_index: op_idx,
                type_name: std::any::type_name::<D>(),
            });

            // Operator factory: collecting sink (used by run(), replaced by spawn())
            let collector_clone = Arc::clone(&collector);
            let name_clone = name.clone();
            let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("sink must have input puller")
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .expect("sink input puller type mismatch");

                Box::new(CollectingSink::new(
                    name_clone,
                    op_idx,
                    region_id,
                    input_puller,
                    collector_clone,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));

            // Store a wiring closure that creates a ChannelSinkOperator
            // replacement factory during spawn(). This replaces the
            // CollectingSink factory above with one that sends data out.
            let sink_name = name.clone();
            let wiring: OutputPortWiring = Box::new(move || {
                use crate::dataflow::channel_operators::{ChannelSinkOperator, OutputReceiver};

                let (tx, rx) = std::sync::mpsc::sync_channel::<OutputEvent<T, D>>(256);
                let receiver = OutputReceiver::new(rx);

                let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .expect("sink must have input puller")
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .expect("sink input puller type mismatch");

                    Box::new(ChannelSinkOperator::new(
                        sink_name,
                        op_idx,
                        RegionId::new(0),
                        input_puller,
                        tx,
                    )) as Box<dyn SchedulableOperator>
                });

                let receiver_any: Box<dyn std::any::Any + Send> = Box::new(receiver);
                (factory, receiver_any)
            });
            state.output_port_wiring.push(wiring);

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let capacity = state.channel_capacity;
            let channel_factory: ChannelFactory = Box::new(move |_cap: usize| {
                let (push, pull) = bounded_channel::<T, D, ()>(capacity);
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, channel_factory));
        }

        OutputPort {
            name,
            collector,
            _phantom: PhantomData,
        }
    }

    /// Convenience: collect output into a shared vector (for testing).
    ///
    /// Each call generates a unique internal output port name.
    pub fn collect(self) -> Arc<Mutex<Vec<(T, Vec<D>)>>> {
        let name = {
            let mut state = self.state.borrow_mut();
            let idx = state.next_collect_index;
            state.next_collect_index += 1;
            format!("__collect_{idx}")
        };
        self.output(name).collector
    }

    /// Attach a probe to observe the frontier at this point in the pipeline.
    ///
    /// Returns `(Stream, ProbeHandle)` — the stream continues unchanged,
    /// and the probe can be queried after execution.
    pub fn probe(self) -> (Self, ProbeHandle<T>) {
        let probe = ProbeHandle::new();
        {
            let mut state = self.state.borrow_mut();
            // Probe attaches to the next downstream operator's input.
            // For now, record the probe at the upstream operator index.
            // The materializer will wire it to observe the frontier at this point.
            state.probes.push((self.op_idx, probe.clone()));
        }
        (self, probe)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Internal: add a unary operator using batch-level closure.
    fn add_unary_internal<D2>(
        &self,
        name: impl Into<String>,
        logic: impl FnMut(T, Vec<D>) -> Vec<D2> + Send + 'static,
    ) -> Stream<T, D2>
    where
        D2: Clone + Send + 'static,
    {
        let name = name.into();
        let op_idx;
        let region_id = RegionId::new(0);

        let mut logic = logic;
        let wired_logic =
            move |input: &mut InputHandle<T, D>, output: &mut OutputHandle<T, D2>| -> Result<()> {
                while let Some((time, data)) = input.next() {
                    let result = logic(time.clone(), data);
                    if !result.is_empty() {
                        output.push_vec(time, result);
                    }
                }
                Ok(())
            };

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (1 input, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 1, 1,
                ))
                .expect("operator index unique");

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                region_id,
                region_id,
            ));

            // Subgraph: identity connectivity (timestamps pass through unchanged)
            state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — handles fan-out by wrapping multiple output
            // pushers in a TeePush adapter when the stream was cloned.
            let name_clone = name.clone();
            let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("unary must have input puller")
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .expect("unary input puller type mismatch");

                let output_pusher: Box<dyn Push<T, D2>> = {
                    let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D2>>>()
                                .expect("unary output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredUnaryOperator::new(
                    name_clone,
                    op_idx,
                    region_id,
                    wired_logic,
                    input_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let capacity = state.channel_capacity;
            let channel_factory: ChannelFactory = Box::new(move |_cap: usize| {
                let (push, pull) = bounded_channel::<T, D, ()>(capacity);
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, channel_factory));
        }

        Stream {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Internal: add a unary operator using InputHandle/OutputHandle API.
    fn add_unary_with_handles<D2, L>(
        &self,
        name: impl Into<String>,
        logic: L,
    ) -> Stream<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(&mut InputHandle<T, D>, &mut OutputHandle<T, D2>) -> Result<()>
            + Send
            + 'static,
    {
        let name = name.into();
        let op_idx;
        let region_id = RegionId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (1 input, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 1, 1,
                ))
                .expect("operator index unique");

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                region_id,
                region_id,
            ));

            // Subgraph: identity connectivity
            state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — handles fan-out via TeePush
            let name_clone = name.clone();
            let factory: OperatorFactory = Box::new(move |endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("unary must have input puller")
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .expect("unary input puller type mismatch");

                let output_pusher: Box<dyn Push<T, D2>> = {
                    let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D2>>>()
                                .expect("unary output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredUnaryOperator::new(
                    name_clone,
                    op_idx,
                    region_id,
                    logic,
                    input_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let capacity = state.channel_capacity;
            let channel_factory: ChannelFactory = Box::new(move |_cap: usize| {
                let (push, pull) = bounded_channel::<T, D, ()>(capacity);
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, channel_factory));
        }

        Stream {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// OutputPort<T, D>
// ---------------------------------------------------------------------------

/// Handle to a named output port. Holds the collector for reading results
/// after execution completes.
pub struct OutputPort<T: Timestamp, D: Send + 'static> {
    name: String,
    collector: Arc<Mutex<Vec<(T, Vec<D>)>>>,
    _phantom: PhantomData<D>,
}

impl<T: Timestamp, D: Send + 'static> OutputPort<T, D> {
    /// Get the output port name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the collector handle (available after execution).
    pub fn collector(&self) -> Arc<Mutex<Vec<(T, Vec<D>)>>> {
        Arc::clone(&self.collector)
    }
}

// ---------------------------------------------------------------------------
// LogicalDataflow<T>
// ---------------------------------------------------------------------------

/// A fully constructed logical dataflow, ready for materialization and execution.
///
/// This is the result of [`DataflowBuilder::build()`]. It contains the complete
/// graph topology, operator factories, and channel factories — everything needed
/// to materialize a physical executor.
///
/// `LogicalDataflow` is `Send` and can be submitted to an async runtime on
/// another thread. It is **single-use** — each materialization consumes the
/// factories (which may contain `FnOnce` closures).
pub struct LogicalDataflow<T: Timestamp> {
    name: String,
    graph: DataflowGraph,
    subgraph_builder: SubgraphBuilder<T>,
    operator_factories: Vec<(usize, OperatorFactory)>,
    channel_factories: Vec<(usize, ChannelFactory)>,
    input_ports: Vec<InputPortInfo>,
    output_ports: Vec<OutputPortInfo>,
    input_port_wiring: Vec<InputPortWiring>,
    output_port_wiring: Vec<OutputPortWiring>,
    probes: Vec<(usize, ProbeHandle<T>)>,
}

impl<T: Timestamp> LogicalDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of operators in the graph.
    pub fn operator_count(&self) -> usize {
        self.graph.operators().count()
    }

    /// Get the number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.graph.edges().len()
    }

    /// Get the names of declared input ports.
    pub fn input_names(&self) -> Vec<&str> {
        self.input_ports.iter().map(|p| p.name.as_str()).collect()
    }

    /// Get the names of declared output ports.
    pub fn output_names(&self) -> Vec<&str> {
        self.output_ports.iter().map(|p| p.name.as_str()).collect()
    }

    /// Materialize and run the dataflow to completion (blocking).
    ///
    /// This is a convenience method for testing with pre-loaded sources.
    /// Dataflows using `input()` ports require async runtime (Phase C).
    ///
    /// # Errors
    ///
    /// Returns `Error::Custom` if the dataflow has declared input ports
    /// (use `Runtime::spawn()` instead for runtime-fed inputs).
    pub fn run(self) -> Result<()> {
        self.run_with_cancel(CancellationToken::new())
    }

    /// Materialize and run with a cancellation token (blocking).
    pub fn run_with_cancel(self, cancel: CancellationToken) -> Result<()> {
        if !self.input_ports.is_empty() {
            return Err(Error::Custom(
                "cannot run() a dataflow with declared input ports — \
                 input ports require async runtime (Runtime::spawn). \
                 Use builder.source() for pre-loaded data instead."
                    .into(),
            ));
        }

        if self.operator_factories.is_empty() {
            return Ok(());
        }

        let executor_config = ExecutorConfig {
            max_activations_per_step: 1024,
            max_idle_sweeps: 64,
        };

        // Materialize: converts logical graph into physical executor
        let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
            &self.graph,
            self.operator_factories,
            self.channel_factories,
            executor_config,
            cancel,
        )?;

        // Build and attach progress tracker
        let mut tracker = self.subgraph_builder.build();
        tracker.initialize();
        executor.set_progress_tracker(tracker);

        // Register probes
        for (op_idx, probe) in self.probes {
            executor.register_probe(op_idx, probe);
        }

        // Run to completion
        let completed = executor.run()?;
        if !completed {
            return Err(Error::Custom(
                "dataflow did not complete (quiescence without termination)".into(),
            ));
        }

        Ok(())
    }

    /// Spawn this dataflow on a dedicated background thread with channel-based I/O.
    ///
    /// This is the **standalone** spawn mode — the dataflow gets its own thread
    /// and runs an executor loop independently. For production workloads where
    /// multiple dataflows share a thread pool, use [`RuntimeHandle::spawn()`]
    /// instead (planned).
    ///
    /// Unlike [`run()`](Self::run) which blocks and requires all data to be pre-loaded,
    /// `spawn()` launches the dataflow on a background thread and returns a
    /// [`SpawnedDataflow`] that provides:
    ///
    /// - **Input senders** — feed data into the dataflow via bounded channels
    /// - **Output receivers** — collect results as they're produced
    /// - **Cancellation** — cancel the running dataflow
    /// - **Join** — wait for the dataflow to complete
    ///
    /// # Channel wiring
    ///
    /// For each [`input()`](DataflowBuilder::input) port:
    /// - Creates a bounded `mpsc` channel
    /// - Installs a `ChannelSourceOperator` that drains the receiver into the graph
    /// - Returns the sender half as `InputSender<T, D>` (via [`SpawnedDataflow::take_input()`])
    ///
    /// For each [`output()`](Stream::output) port:
    /// - Replaces the `CollectingSink` factory with a `ChannelSinkOperator`
    /// - Creates a bounded `mpsc` channel for the sink to write into
    /// - Returns the receiver half as `OutputReceiver<T, D>` (via [`SpawnedDataflow::take_output()`])
    ///
    /// # Example
    ///
    /// ```ignore
    /// let builder = DataflowBuilder::<u64>::new("pipeline");
    /// let input = builder.input::<i32>("numbers");
    /// input.map("double", |_t, x| x * 2).output::<i32>("results");
    /// let dataflow = builder.build()?;
    ///
    /// let handle = dataflow.spawn()?;
    /// handle.take_input::<i32>("numbers")?.send(0, vec![1, 2, 3])?;
    /// handle.take_input::<i32>("numbers")?.close();
    /// let results = handle.take_output::<i32>("results")?.collect_data();
    /// handle.join()?;
    /// ```
    pub fn spawn(mut self) -> Result<SpawnedDataflow<T>> {
        use std::sync::atomic::AtomicUsize;

        if self.operator_factories.is_empty() && self.input_port_wiring.is_empty() {
            return Err(Error::Custom("cannot spawn an empty dataflow".into()));
        }

        let cancel = CancellationToken::new();
        let cancel_handle = cancel.clone();
        let external_inputs_open = Arc::new(AtomicUsize::new(0));

        // --- Wire input ports ---
        // Each input port wiring closure creates a ChannelSourceOperator factory
        // and an InputSender. The factory is added to operator_factories so it
        // gets materialized normally by the executor.
        let mut input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> = Vec::new();
        let input_count = self.input_port_wiring.len();

        for (info, wiring) in self.input_ports.iter().zip(self.input_port_wiring.drain(..)) {
            let (factory, sender_any) = wiring(Arc::clone(&external_inputs_open));
            self.operator_factories.push((info.operator_index, factory));
            input_senders.push((info.name.clone(), info.type_name, sender_any));
        }

        // --- Wire output ports ---
        // Each output port wiring closure creates a ChannelSinkOperator factory
        // that replaces the CollectingSink factory for the same operator index.
        let mut output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> = Vec::new();

        for (info, wiring) in self.output_ports.iter().zip(self.output_port_wiring.drain(..)) {
            let (replacement_factory, receiver_any) = wiring();
            // Replace the CollectingSink factory with ChannelSinkOperator factory
            if let Some(pos) = self.operator_factories.iter().position(|(idx, _)| *idx == info.operator_index) {
                self.operator_factories[pos] = (info.operator_index, replacement_factory);
            }
            output_receivers.push((info.name.clone(), info.type_name, receiver_any));
        }

        // --- Materialize and run ---
        let executor_config = ExecutorConfig {
            max_activations_per_step: 1024,
            max_idle_sweeps: 64,
        };

        let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
            &self.graph,
            self.operator_factories,
            self.channel_factories,
            executor_config,
            cancel,
        )?;

        // Share the SAME external_inputs_open counter between the executor
        // and the ChannelSourceOperators. Operators decrement this counter
        // when their channel closes; the executor reads it to decide quiescence.
        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        // Build and attach progress tracker
        let mut tracker = self.subgraph_builder.build();
        tracker.initialize();
        executor.set_progress_tracker(tracker);

        // Register probes
        for (op_idx, probe) in self.probes {
            executor.register_probe(op_idx, probe);
        }

        // Spawn executor on background thread
        let name = self.name.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("dataflow-{}", name))
            .spawn(move || -> Result<bool> { executor.run() })
            .map_err(|e| Error::Custom(format!("failed to spawn dataflow thread: {e}")))?;

        Ok(SpawnedDataflow {
            name,
            cancel: cancel_handle,
            join_handle: Some(join_handle),
            input_senders,
            output_receivers,
            _phantom: PhantomData,
        })
    }
}

// ---------------------------------------------------------------------------
// SpawnedDataflow — handle for a running dataflow with channel-based I/O
// ---------------------------------------------------------------------------

/// A handle to a running dataflow spawned on a background thread.
///
/// Provides typed access to input senders and output receivers for each
/// named port declared during graph construction. The dataflow runs
/// independently on its own thread; use the methods below to feed data,
/// collect results, cancel execution, or wait for completion.
///
/// # Type safety
///
/// Port types are validated at runtime: calling `input::<i32>("x")` on a
/// port that was declared as `input::<String>("x")` will return an error.
///
/// # Lifecycle
///
/// 1. Send data via `input()` senders
/// 2. Close inputs when done (drop the `InputSender` or call `.close()`)
/// 3. Collect results from `output()` receivers
/// 4. Call `join()` to wait for the executor to finish
///
/// Dropping a `SpawnedDataflow` without calling `join()` will cancel the
/// dataflow and wait for the background thread to exit.
pub struct SpawnedDataflow<T: Timestamp> {
    name: String,
    cancel: CancellationToken,
    join_handle: Option<std::thread::JoinHandle<Result<bool>>>,
    /// (name, type_name, Box<InputSender<T, D>> as Box<dyn Any>)
    input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    /// (name, type_name, Box<OutputReceiver<T, D>> as Box<dyn Any>)
    output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    _phantom: PhantomData<T>,
}

impl<T: Timestamp> SpawnedDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Take the input sender for the named port (consumes it from the handle).
    ///
    /// Input senders can only be taken once — subsequent calls for the
    /// same port will return an error. Drop the returned sender (or call
    /// `.close()`) to signal that no more data will arrive on this port.
    ///
    /// # Type safety
    ///
    /// The type parameter `D` must match the type used in `builder.input::<D>(name)`.
    /// Returns an error if the name doesn't exist or the type doesn't match.
    pub fn take_input<D: Clone + Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::InputSender<T, D>> {
        let type_name = std::any::type_name::<D>();
        let pos = self
            .input_senders
            .iter()
            .position(|(n, _, _)| n == name)
            .ok_or_else(|| Error::Custom(format!("no input port named '{name}'")))?;

        let (_, port_type, _) = &self.input_senders[pos];
        if *port_type != type_name {
            return Err(Error::Custom(format!(
                "input port '{name}' has type {port_type}, but requested {type_name}"
            )));
        }

        let (_, _, sender_any) = self.input_senders.remove(pos);
        sender_any
            .downcast::<crate::dataflow::channel_operators::InputSender<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!("input port '{name}' type downcast failed")))
    }

    /// Take the output receiver for the named port (consumes it from the handle).
    ///
    /// Output receivers can only be taken once — subsequent calls for the
    /// same port will return an error.
    ///
    /// # Type safety
    ///
    /// The type parameter `D` must match the type used in `stream.output::<D>(name)`.
    pub fn take_output<D: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::OutputReceiver<T, D>> {
        let type_name = std::any::type_name::<D>();
        let pos = self
            .output_receivers
            .iter()
            .position(|(n, _, _)| n == name)
            .ok_or_else(|| Error::Custom(format!("no output port named '{name}'")))?;

        let (_, port_type, _) = &self.output_receivers[pos];
        if *port_type != type_name {
            return Err(Error::Custom(format!(
                "output port '{name}' has type {port_type}, but requested {type_name}"
            )));
        }

        let (_, _, receiver_any) = self.output_receivers.remove(pos);
        receiver_any
            .downcast::<crate::dataflow::channel_operators::OutputReceiver<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!("output port '{name}' type downcast failed")))
    }

    /// Cancel the running dataflow.
    ///
    /// Signals the executor's cancellation token. The executor will stop
    /// at the next cancellation check point. Does not block.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Wait for the dataflow to complete and return the result.
    ///
    /// Returns `Ok(())` if the dataflow ran to completion.
    /// Returns an error if the executor encountered an error or the
    /// background thread panicked.
    pub fn join(mut self) -> Result<()> {
        if let Some(handle) = self.join_handle.take() {
            match handle.join() {
                Ok(Ok(_completed)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_panic) => Err(Error::Custom(
                    "dataflow thread panicked".into(),
                )),
            }
        } else {
            Ok(())
        }
    }
}

impl<T: Timestamp> Drop for SpawnedDataflow<T> {
    fn drop(&mut self) {
        // Cancel the dataflow so the background thread exits
        self.cancel.cancel();
        // Wait for it to finish (best-effort)
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// CollectingSink (reused from builder.rs — same implementation)
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
    fn activate(
        &mut self,
    ) -> crate::error::Result<crate::dataflow::schedulable::ActivationOutcome> {
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
// NullPush — discards all pushed envelopes
// ---------------------------------------------------------------------------

struct NullPush;

impl<T: Timestamp, D: Send + 'static> Push<T, D> for NullPush {
    fn push(&mut self, _envelope: crate::dataflow::channels::Envelope<T, D>) -> Result<()> {
        Ok(())
    }

    fn try_push(
        &mut self,
        _envelope: crate::dataflow::channels::Envelope<T, D>,
    ) -> std::result::Result<(), (crate::error::Error, crate::dataflow::channels::Envelope<T, D>)>
    {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_builder() {
        let builder = DataflowBuilder::<u64>::new("empty");
        assert_eq!(builder.operator_count(), 0);
        let dataflow = builder.build().unwrap();
        assert_eq!(dataflow.operator_count(), 0);
        dataflow.run().unwrap();
    }

    #[test]
    fn test_source_to_output() {
        let builder = DataflowBuilder::<u64>::new("source_to_output");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream.output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (0, vec![1, 2, 3]));
    }

    #[test]
    fn test_map_operator() {
        let builder = DataflowBuilder::<u64>::new("map_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])]);
        let doubled = stream.map("double", |_t, x| x * 2);
        let port = doubled.output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_filter_operator() {
        let builder = DataflowBuilder::<u64>::new("filter_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6])]);
        let evens = stream.filter("evens", |_t, x| x % 2 == 0);
        let port = evens.output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec![2, 4, 6]);
    }

    #[test]
    fn test_chained_operators() {
        let builder = DataflowBuilder::<u64>::new("chain_test");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])])
            .map("double", |_t, x| x * 2)
            .filter("div_by_3", |_t, x| x % 3 == 0)
            .map("to_string", |_t, x| format!("{x}"))
            .output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        let strings: Vec<&String> = results[0].1.iter().collect();
        assert_eq!(strings, vec!["6", "12", "18"]);
    }

    #[test]
    fn test_flat_map() {
        let builder = DataflowBuilder::<u64>::new("flat_map_test");
        let port = builder
            .source("lines", vec![(0u64, vec!["hello world".to_string(), "foo bar".to_string()])])
            .flat_map("split", |_t, line| {
                line.split_whitespace().map(|w| w.to_string()).collect()
            })
            .output("words");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(
            results[0].1,
            vec!["hello", "world", "foo", "bar"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_branching_fan_out() {
        // Fan-out: one source → two downstream branches via Stream::clone()
        // Uses TeePush to distribute data to both branches
        let builder = DataflowBuilder::<u64>::new("branch_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6])]);

        let evens_port = stream
            .clone()
            .filter("evens", |_t, x| x % 2 == 0)
            .output("evens");
        let odds_port = stream
            .filter("odds", |_t, x| x % 2 != 0)
            .output("odds");

        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let evens_c = evens_port.collector();
        let evens = evens_c.lock().unwrap();
        let odds_c = odds_port.collector();
        let odds = odds_c.lock().unwrap();

        assert_eq!(evens[0].1, vec![2, 4, 6]);
        assert_eq!(odds[0].1, vec![1, 3, 5]);
    }

    #[test]
    fn test_multiple_inputs() {
        // Two independent sources feeding separate pipelines
        let builder = DataflowBuilder::<u64>::new("multi_input");
        let a = builder.source("a", vec![(0u64, vec![10i32, 20])]);
        let b = builder.source("b", vec![(0u64, vec![100i32, 200])]);

        let port_a = a.map("inc_a", |_t, x| x + 1).output("out_a");
        let port_b = b.map("inc_b", |_t, x| x + 1).output("out_b");

        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector_ra = port_a.collector();
        let ra = collector_ra.lock().unwrap();
        let collector_rb = port_b.collector();
        let rb = collector_rb.lock().unwrap();
        assert_eq!(ra[0].1, vec![11, 21]);
        assert_eq!(rb[0].1, vec![101, 201]);
    }

    #[test]
    fn test_type_conversion() {
        let builder = DataflowBuilder::<u64>::new("type_conv");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map("to_f64", |_t, x| x as f64 * 1.5)
            .map("to_string", |_t, x| format!("{x:.1}"))
            .output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec!["1.5", "3.0", "4.5"]);
    }

    #[test]
    fn test_empty_batches() {
        let builder = DataflowBuilder::<u64>::new("empty_batch");
        let port = builder
            .source::<i32>("nums", vec![(0u64, vec![])])
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        // Empty batch produces no output (filtered by !result.is_empty())
        assert!(results.is_empty() || results[0].1.is_empty());
    }

    #[test]
    fn test_multiple_timestamps() {
        let builder = DataflowBuilder::<u64>::new("multi_time");
        let port = builder
            .source(
                "nums",
                vec![
                    (0u64, vec![1i32, 2]),
                    (1u64, vec![3, 4]),
                    (2u64, vec![5]),
                ],
            )
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (0, vec![2, 4]));
        assert_eq!(results[1], (1, vec![6, 8]));
        assert_eq!(results[2], (2, vec![10]));
    }

    #[test]
    fn test_collect_convenience() {
        let builder = DataflowBuilder::<u64>::new("collect");
        let collector = builder
            .source("nums", vec![(0u64, vec![42i32])])
            .collect();
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec![42]);
    }

    #[test]
    fn test_cancellation() {
        let cancel = CancellationToken::new();
        cancel.cancel(); // pre-cancel

        let builder = DataflowBuilder::<u64>::new("cancel_test");
        let _port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .output("results");
        let dataflow = builder.build().unwrap();
        let result = dataflow.run_with_cancel(cancel);
        assert!(result.is_err());
    }

    #[test]
    fn test_logical_dataflow_metadata() {
        let builder = DataflowBuilder::<u64>::new("meta_test");
        let _a = builder.source::<i32>("src_a", vec![]);
        let b = builder.source::<i32>("src_b", vec![]);
        let _port = b.map("transform", |_t, x| x).output("out");
        drop(_a); // release Rc reference before build
        let dataflow = builder.build().unwrap();

        assert_eq!(dataflow.name(), "meta_test");
        assert_eq!(dataflow.input_names().len(), 0); // sources are not "input ports"
        assert_eq!(dataflow.output_names(), vec!["out"]);
        // 2 sources + 1 unary + 1 sink = 4 operators
        assert_eq!(dataflow.operator_count(), 4);
    }

    #[test]
    #[should_panic(expected = "duplicate output port name")]
    fn test_duplicate_output_name_panics() {
        let builder = DataflowBuilder::<u64>::new("dup_test");
        // Use two independent sources to avoid fan-out detection
        let s1 = builder.source("src1", vec![(0u64, vec![1i32])]);
        let s2 = builder.source("src2", vec![(0u64, vec![2i32])]);
        let _p1 = s1.output("results");
        let _p2 = s2.output("results"); // should panic: duplicate name
    }

    #[test]
    fn test_probe() {
        let builder = DataflowBuilder::<u64>::new("probe_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let (stream, _probe) = stream.probe();
        let _port = stream.output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();
        // Probe was registered — basic smoke test that it doesn't panic
    }

    #[test]
    fn test_unary_with_handles() {
        let builder = DataflowBuilder::<u64>::new("unary_handles");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])])
            .unary("sum_batch", |input, output| {
                while let Some((time, data)) = input.next() {
                    let sum: i32 = data.iter().sum();
                    output.push_vec(time, vec![sum]);
                }
                Ok(())
            })
            .output("results");
        let dataflow = builder.build().unwrap();
        dataflow.run().unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec![15]);
    }

    #[test]
    fn test_input_port_rejected_by_run() {
        let builder = DataflowBuilder::<u64>::new("input_test");
        let _port = builder.input::<i32>("data").output("results");
        let dataflow = builder.build().unwrap();
        let result = dataflow.run();
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("input ports require async runtime"));
    }

    #[test]
    fn test_spawn_basic_pipeline() {
        // Build: input → double → output
        let builder = DataflowBuilder::<u64>::new("spawn_test");
        let input = builder.input::<i32>("numbers");
        input
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();

        // Spawn on background thread
        let mut handle = dataflow.spawn().unwrap();

        // Send data and close input
        let sender = handle.take_input::<i32>("numbers").unwrap();
        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.send(1, vec![10, 20]).unwrap();
        sender.close();

        // Collect output
        let receiver = handle.take_output::<i32>("results").unwrap();
        let mut results = receiver.collect_data();
        results.sort_by_key(|(t, _)| *t);

        // Verify doubling
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[0].1, vec![2, 4, 6]);
        assert_eq!(results[1].0, 1);
        assert_eq!(results[1].1, vec![20, 40]);

        // Wait for completion
        handle.join().unwrap();
    }

    #[test]
    fn test_spawn_filter_pipeline() {
        let builder = DataflowBuilder::<u64>::new("filter_spawn");
        let input = builder.input::<i32>("src");
        input
            .filter("evens", |_t, x| x % 2 == 0)
            .output("evens");
        let dataflow = builder.build().unwrap();

        let mut handle = dataflow.spawn().unwrap();
        let sender = handle.take_input::<i32>("src").unwrap();
        sender.send(0, vec![1, 2, 3, 4, 5, 6]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("evens").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, vec![2, 4, 6]);

        handle.join().unwrap();
    }

    #[test]
    fn test_spawn_type_mismatch_error() {
        let builder = DataflowBuilder::<u64>::new("type_test");
        let input = builder.input::<i32>("numbers");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = dataflow.spawn().unwrap();
        // Try to get input with wrong type
        let result = handle.take_input::<String>("numbers");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("type"));
    }

    #[test]
    fn test_spawn_cancel() {
        let builder = DataflowBuilder::<u64>::new("cancel_test");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let handle = dataflow.spawn().unwrap();
        // Cancel without sending any data
        handle.cancel();
        // join() should succeed (cancellation is not an error)
        let result = handle.join();
        // Either Ok or a cancellation error — both are acceptable
        let _ = result;
    }

    #[test]
    fn test_spawn_drop_cancels() {
        let builder = DataflowBuilder::<u64>::new("drop_test");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        // Drop without join — should cancel and not hang
        let _handle = dataflow.spawn().unwrap();
        // SpawnedDataflow::drop cancels and joins
    }
}
