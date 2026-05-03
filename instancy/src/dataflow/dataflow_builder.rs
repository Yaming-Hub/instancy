//! Separated dataflow builder — constructs a `LogicalDataflow` independently of execution.
//!
//! # Design
//!
//! The builder follows a two-phase pattern:
//!
//! 1. **Construction**: Use [`DataflowBuilder`] to declare inputs, chain operators
//!    via [`Pipe`], and declare outputs. This produces a [`LogicalDataflow`].
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
//! Pipes are **cloneable** — enabling branching and multi-input patterns:
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

use crate::dataflow::channels::bounded::bounded_channel_with_wake;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::tee::tee_or_single;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::channel_operators::ChannelMode;
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{
    ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator,
    channel_factory, single_use_factory,
};
use crate::dataflow::stream::Slot;
use crate::dataflow::wired_operators::{WiredBinaryOperator, WiredConcatOperator, WiredEnterOperator, WiredFeedbackOperator, WiredLeaveOperator, WiredSourceOperator, WiredUnaryOperator};
use crate::error::{Error, Result};
use crate::progress::change_batch::ChangeBatch;
use crate::progress::operate::PortConnectivity;
use crate::progress::reachability::Location;
use crate::progress::subgraph::SubgraphBuilder;
use crate::progress::timestamp::Timestamp;
use crate::order::Product;

// ---------------------------------------------------------------------------
// InputPortInfo / OutputPortInfo — metadata for named I/O ports
// ---------------------------------------------------------------------------

/// Metadata for a named input port in the logical dataflow.
#[derive(Clone)]
pub(crate) struct InputPortInfo {
    /// User-visible name of this input.
    pub(crate) name: String,
    /// The source operator index that this input feeds into.
    pub(crate) operator_index: usize,
    /// TypeId of the data type for runtime validation.
    pub(crate) type_name: &'static str,
}

/// Metadata for a named output port in the logical dataflow.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used in Phase C (async runtime integration)
pub(crate) struct OutputPortInfo {
    /// User-visible name of this output.
    pub(crate) name: String,
    /// The sink operator index that collects output.
    pub(crate) operator_index: usize,
    /// TypeId of the data type for runtime validation.
    pub(crate) type_name: &'static str,
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
    /// Channel factories for feedback edges, indexed by position in graph.feedback_edges.
    feedback_channel_factories: Vec<(usize, ChannelFactory)>,
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
    /// Type-erased exchange factory creators — one per exchange edge.
    exchange_creators:
        Vec<(usize, crate::dataflow::channels::exchange_channel::ExchangeFactoryCreatorFn)>,
    next_operator_index: usize,
    next_collect_index: usize,
    channel_capacity: usize,
}

/// Type-erased closure for wiring an input port during spawn().
/// Captures the data type D and creates:
/// - An OperatorFactory for the ChannelSourceOperator
/// - An InputSender (as Box<dyn Any + Send>) for the caller
///
/// Changed from `FnOnce` to `FnMut` to support multi-worker materialization:
/// each worker calls this once to get its own (factory, sender) pair.
pub(crate) type InputPortWiring = Box<
    dyn FnMut(
            std::sync::Arc<std::sync::atomic::AtomicUsize>, // external_inputs_open counter
            WakeHandle,                                      // executor wake handle
            ChannelMode,                                     // sync vs async channel backend
        ) -> (OperatorFactory, Box<dyn std::any::Any + Send>) // (factory, InputSender or AsyncInputSender)
        + Send,
>;

/// Type-erased closure for wiring an output port during spawn().
/// Returns:
/// - An OperatorFactory for the ChannelSinkOperator (replaces CollectingSink)
/// - An OutputReceiver or AsyncOutputReceiver (as Box<dyn Any + Send>) for the caller
///
/// Changed from `FnOnce` to `FnMut` to support multi-worker materialization.
pub(crate) type OutputPortWiring = Box<
    dyn FnMut(ChannelMode, Option<WakeHandle>) -> (OperatorFactory, Box<dyn std::any::Any + Send>)
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

/// Builder for constructing a [`LogicalDataflow`] via typed Pipe chaining.
///
/// The builder uses interior mutability (`Rc<RefCell>`) so that multiple
/// [`Pipe`] handles can coexist — enabling branching and multi-input patterns.
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
                feedback_channel_factories: Vec::new(),
                input_ports: Vec::new(),
                output_ports: Vec::new(),
                input_port_wiring: Vec::new(),
                output_port_wiring: Vec::new(),
                probes: Vec::new(),
                exchange_creators: Vec::new(),
                next_operator_index: 1,
                next_collect_index: 0,
                channel_capacity: config.channel_capacity,
            })),
        }
    }

    /// Declare a named input port that data will be fed into at runtime.
    ///
    /// Returns a [`Pipe`] representing the data flowing from this input.
    /// At execution time, the runtime connects an async channel to this port.
    ///
    /// # Panics
    ///
    /// Panics if an input with the same name already exists.
    pub fn input<D: Clone + Send + 'static>(&self, name: impl Into<String>) -> Pipe<T, D> {
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
            let wiring: InputPortWiring = Box::new(move |external_inputs_open, wake_handle, mode| {
                use crate::dataflow::channel_operators::{InputSender, ChannelSourceOperator, InputRecv};
                use crate::dataflow::channels::tee::tee_or_single;

                // Build the factory + sender based on channel mode.
                // The factory closure captures the receiver; the sender is
                // returned to the caller as a type-erased Box<dyn Any>.
                match mode {
                    ChannelMode::Sync => {
                        let (tx, rx) = std::sync::mpsc::sync_channel::<InputEvent<T, D>>(256);
                        let sender = InputSender::with_wake_handle(tx, wake_handle);

                        let ext_counter = Arc::clone(&external_inputs_open);
                        let factory_name = wiring_name.clone();
                        let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints| {
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
                                InputRecv::Std(rx),
                                output_pusher,
                                None,
                                ext_counter,
                            );
                            Box::new(op) as Box<dyn SchedulableOperator>
                        });

                        let sender_any: Box<dyn std::any::Any + Send> = Box::new(sender);
                        (factory, sender_any)
                    }
                    #[cfg(feature = "async-io")]
                    ChannelMode::Async => {
                        use crate::dataflow::channel_operators::AsyncInputSender;

                        let (tx, rx) = tokio::sync::mpsc::channel::<InputEvent<T, D>>(256);
                        let sender = AsyncInputSender::with_wake_handle(tx, wake_handle);

                        let ext_counter = Arc::clone(&external_inputs_open);
                        let factory_name = wiring_name.clone();
                        let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints| {
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
                                InputRecv::Tokio(rx),
                                output_pusher,
                                None,
                                ext_counter,
                            );
                            Box::new(op) as Box<dyn SchedulableOperator>
                        });

                        let sender_any: Box<dyn std::any::Any + Send> = Box::new(sender);
                        (factory, sender_any)
                    }
                }
            });
            state.input_port_wiring.push(wiring);
        }

        Pipe {
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
    ) -> Pipe<T, D> {
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
            // Handles fan-out: if multiple downstream edges exist (Pipe was
            // cloned), wraps all pushers in a TeePush adapter.
            let name_clone = name.clone();
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
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

        Pipe {
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
    /// Returns an error if outstanding `Pipe` references still exist
    /// (drop them first).
    ///
    /// Fan-out (Pipe cloning/branching) is supported: each cloned Pipe
    /// output port automatically uses a [`TeePush`](crate::dataflow::channels::tee::TeePush)
    /// adapter that clones data to all downstream consumers.
    ///
    /// After a successful call, the builder is consumed.
    pub fn build(self) -> Result<LogicalDataflow<T>> {
        let mut state = match Rc::try_unwrap(self.state) {
            Ok(cell) => cell.into_inner(),
            Err(_) => {
                return Err(Error::Custom(
                    "cannot build: outstanding Pipe references still exist — \
                     drop all Pipe handles before calling build()"
                        .into(),
                ))
            }
        };

        // Merge feedback channel factories with correct indices.
        // Feedback edges are materialized at indices: edges.len()..edges.len()+feedback_edges.len()
        let regular_edge_count = state.graph.edges().len();
        for (fb_position, factory) in state.feedback_channel_factories {
            state.channel_factories.push((regular_edge_count + fb_position, factory));
        }

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
            exchange_creators: state.exchange_creators,
        })
    }
}

// ---------------------------------------------------------------------------
// Pipe<T, D> — cloneable typed handle for chaining operators
// ---------------------------------------------------------------------------

/// A builder-time handle representing an operator's output in the dataflow graph.
///
/// `Pipe` is part of the construction plumbing layer (Layer 3). It holds a shared
/// reference to the builder's internal state and provides fluent method chaining
/// (`.map()`, `.filter()`, `.binary()`, `.output()`) to wire operators together.
///
/// Pipes are ephemeral — they exist only during `DataflowBuilder` construction
/// and are consumed when `.build()` produces the `LogicalDataflow`. They do not
/// exist at runtime.
///
/// See PLAN.md "Conceptual Architecture: Three Layers of a Dataflow" for how Pipe
/// relates to StreamEdge (Layer 2) and the abstract Dataflow Graph (Layer 1).
pub struct Pipe<T: Timestamp, D: Clone + Send + 'static> {
    state: Rc<RefCell<BuilderState<T>>>,
    op_idx: usize,
    output_slot: usize,
    _phantom: PhantomData<D>,
}

impl<T: Timestamp, D: Clone + Send + 'static> Clone for Pipe<T, D> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            op_idx: self.op_idx,
            output_slot: self.output_slot,
            _phantom: PhantomData,
        }
    }
}

/// Result of an iterate closure — specifies which data feeds back and which exits.
pub struct IterateResult<T: Timestamp, D: Clone + Send + 'static> {
    /// Data that feeds back for another iteration.
    pub feedback: Pipe<T, D>,
    /// Data that exits the loop.
    pub output: Pipe<T, D>,
}

impl<T: Timestamp, D: Clone + Send + 'static> Pipe<T, D> {
    /// Apply a per-element transformation, producing a new Pipe of type `D2`.
    ///
    /// The closure receives a reference to the timestamp and ownership of each element.
    /// If you need to capture the timestamp, clone it inside the closure.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = stream.map("double", |_t, x: i32| x * 2);
    /// ```
    pub fn map<D2, F>(self, name: impl Into<String>, mut logic: F) -> Pipe<T, D2>
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
    pub fn filter<F>(self, name: impl Into<String>, mut predicate: F) -> Pipe<T, D>
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
    pub fn flat_map<D2, F>(self, name: impl Into<String>, mut logic: F) -> Pipe<T, D2>
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
    pub fn unary<D2, L>(self, name: impl Into<String>, logic: L) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(&mut InputHandle<T, D>, &mut OutputHandle<T, D2>) -> Result<()>
            + Send
            + 'static,
    {
        self.add_unary_with_handles(name, logic)
    }

    /// Combine two streams with a binary operator.
    ///
    /// Creates an operator with two typed inputs and one typed output. Data
    /// may arrive on either input independently — the logic must handle
    /// partial availability (e.g., buffer data per-input if a cross-input
    /// join is needed).
    ///
    /// # Arguments
    ///
    /// - `other` — the second input Pipe (must belong to the same builder)
    /// - `name` — operator name for debugging and graph inspection
    /// - `logic` — closure receiving two `InputHandle`s and one `OutputHandle`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let joined = names.binary::<i32, String>(ages, "join", |names_in, ages_in, out| {
    ///     // Process both inputs and produce joined output
    ///     while let Some((t, data)) = names_in.next() {
    ///         out.push_vec(t, data.into_iter().map(|n| format!("{n}:?")).collect());
    ///     }
    ///     Ok(())
    /// });
    /// ```
    pub fn binary<D2, D3, L>(
        self,
        other: Pipe<T, D2>,
        name: impl Into<String>,
        logic: L,
    ) -> Pipe<T, D3>
    where
        D2: Clone + Send + 'static,
        D3: Clone + Send + 'static,
        L: FnMut(
                &mut InputHandle<T, D>,
                &mut InputHandle<T, D2>,
                &mut OutputHandle<T, D3>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        // Both streams must share the same builder state.
        assert!(
            Rc::ptr_eq(&self.state, &other.state),
            "binary operator streams must belong to the same DataflowBuilder"
        );

        let name = name.into();
        let op_idx;
        let region_id = RegionId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (2 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, region_id, 2, 1,
                ))
                .expect("operator index unique");

            // Edge from self → slot 0
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                region_id,
                region_id,
            ));
            let edge1_idx = state.graph.edges().len() - 1;

            // Edge from other → slot 1
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(other.op_idx, other.output_slot),
                Slot::new(op_idx, 1),
                region_id,
                region_id,
            ));
            let edge2_idx = state.graph.edges().len() - 1;

            // Subgraph: 2 inputs → 1 output, both paths active
            let mut connectivity = PortConnectivity::new(2, 1);
            connectivity.path_mut(0, 0).insert(T::Summary::default());
            connectivity.path_mut(1, 0).insert(T::Summary::default());
            state
                .subgraph_builder
                .add_operator(op_idx, &name, 2, 1, connectivity);
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );
            state.subgraph_builder.add_edge(
                Location::source(other.op_idx, other.output_slot),
                Location::target(op_idx, 1),
            );

            // Operator factory
            let name_clone = name.clone();
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let mut pullers = endpoints.input_pullers.into_iter();

                let input1_puller: Box<dyn Pull<T, D>> = *pullers
                    .next()
                    .expect("binary must have input puller 0")
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .expect("binary input1 puller type mismatch");

                let input2_puller: Box<dyn Pull<T, D2>> = *pullers
                    .next()
                    .expect("binary must have input puller 1")
                    .downcast::<Box<dyn Pull<T, D2>>>()
                    .expect("binary input2 puller type mismatch");

                let output_pusher: Box<dyn Push<T, D3>> = {
                    let pushers: Vec<Box<dyn Push<T, D3>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D3>>>()
                                .expect("binary output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredBinaryOperator::new(
                    name_clone,
                    op_idx,
                    region_id,
                    logic,
                    input1_puller,
                    input2_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));

            // Channel factories for both input edges
            let capacity = state.channel_capacity;
            let channel_factory1: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge1_idx, channel_factory1));

            let channel_factory2: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D2, ()>(capacity, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D2>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D2>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge2_idx, channel_factory2));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Merge this Pipe with another same-typed Pipe.
    ///
    /// Shorthand for `Pipe::concat(vec![self, other])`. Data from both
    /// streams is interleaved in the output.
    pub fn merge(self, other: Pipe<T, D>) -> Pipe<T, D> {
        Pipe::concat(vec![self, other])
    }

    /// Create an iterative loop scope.
    ///
    /// The input data enters the loop and is merged with the feedback stream.
    /// The closure receives the merged iteration variable (with nested timestamp
    /// `Product<T, TInner>`) and must return an [`IterateResult`] specifying
    /// which data feeds back and which exits the loop.
    ///
    /// `summary` describes how the inner timestamp advances per iteration
    /// (e.g., `1u32` means inner timestamp increments by 1 each iteration).
    ///
    /// # Type Parameters
    /// - `TInner`: The inner timestamp type for the loop (typically `u32`)
    ///
    /// # Example
    /// ```ignore
    /// let result = input.iterate::<u32>("loop", 1u32, |iter_var| {
    ///     let doubled = iter_var.map("double", |_t, x| x * 2);
    ///     let done = doubled.clone().filter("done", |_t, x| *x >= 100);
    ///     let again = doubled.filter("again", |_t, x| *x < 100);
    ///     IterateResult { feedback: again, output: done }
    /// });
    /// ```
    pub fn iterate<TInner>(
        self,
        name: impl Into<String>,
        summary: TInner::Summary,
        body: impl FnOnce(Pipe<Product<T, TInner>, D>) -> IterateResult<Product<T, TInner>, D>,
    ) -> Pipe<T, D>
    where
        TInner: Timestamp,
        Product<T, TInner>: Timestamp,
    {
        let name = name.into();
        let region_id = RegionId::new(0);
        type PT<T, TInner> = Product<T, TInner>;

        // Phase 1: Allocate enter, feedback, concat operators in parent state.
        // We also create a separate inner BuilderState for the loop body.
        let enter_idx;
        let feedback_idx;
        let concat_idx;
        let leave_idx;
        let inner_start_idx;
        let capacity;

        {
            let mut state = self.state.borrow_mut();
            capacity = state.channel_capacity;

            // Enter operator: 1 input (T,D), 1 output (Product<T,TInner>,D)
            enter_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    enter_idx, format!("{name}::enter"), region_id, 1, 1,
                ))
                .expect("operator index unique");
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(enter_idx, 0),
                region_id,
                region_id,
            ));
            // Subgraph registration for enter
            state.subgraph_builder.add_operator(
                enter_idx,
                &format!("{name}::enter"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(enter_idx, 0),
            );

            // Enter operator factory
            let enter_name = format!("{name}::enter");
            let enter_factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("enter must have input puller")
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .expect("enter input puller type mismatch");

                let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                    let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                .expect("enter output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredEnterOperator::<T, TInner, D>::new(
                    enter_name,
                    enter_idx,
                    region_id,
                    input_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((enter_idx, enter_factory));

            // Channel factory for enter's input edge
            let enter_edge_idx = state.graph.edges().len() - 1;
            let cap = capacity;
            let cf: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D, ()>(cap, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((enter_edge_idx, cf));

            // Feedback operator: 1 input (Product<T,TInner>,D), 1 output (Product<T,TInner>,D)
            feedback_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    feedback_idx, format!("{name}::feedback"), region_id, 1, 1,
                ))
                .expect("operator index unique");
            // Subgraph registration for feedback
            state.subgraph_builder.add_operator(
                feedback_idx,
                &format!("{name}::feedback"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );

            // Feedback operator factory
            let fb_name = format!("{name}::feedback");
            let fb_summary = summary.clone();
            let feedback_factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<PT<T, TInner>, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("feedback must have input puller")
                    .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                    .expect("feedback input puller type mismatch");

                let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                    let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                .expect("feedback output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredFeedbackOperator::<T, TInner, D>::new(
                    fb_name,
                    feedback_idx,
                    region_id,
                    fb_summary,
                    input_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((feedback_idx, feedback_factory));

            // Concat operator: 2 inputs (enter output + feedback output), 1 output
            concat_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    concat_idx, format!("{name}::concat"), region_id, 2, 1,
                ))
                .expect("operator index unique");
            // Edge: enter → concat input 0
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(enter_idx, 0),
                Slot::new(concat_idx, 0),
                region_id,
                region_id,
            ));
            let enter_concat_edge_idx = state.graph.edges().len() - 1;
            // Edge: feedback → concat input 1
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(feedback_idx, 0),
                Slot::new(concat_idx, 1),
                region_id,
                region_id,
            ));
            let fb_concat_edge_idx = state.graph.edges().len() - 1;

            // Subgraph for concat
            let mut concat_connectivity = PortConnectivity::new(2, 1);
            concat_connectivity.path_mut(0, 0).insert(T::Summary::default());
            concat_connectivity.path_mut(1, 0).insert(T::Summary::default());
            state.subgraph_builder.add_operator(
                concat_idx,
                &format!("{name}::concat"),
                2,
                1,
                concat_connectivity,
            );
            state.subgraph_builder.add_edge(
                Location::source(enter_idx, 0),
                Location::target(concat_idx, 0),
            );
            state.subgraph_builder.add_edge(
                Location::source(feedback_idx, 0),
                Location::target(concat_idx, 1),
            );

            // Concat operator factory
            let concat_name = format!("{name}::concat");
            let concat_factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_pullers: Vec<Box<dyn Pull<PT<T, TInner>, D>>> = endpoints
                    .input_pullers
                    .into_iter()
                    .map(|any_box| {
                        *any_box
                            .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                            .expect("concat input puller type mismatch")
                    })
                    .collect();

                let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                    let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                .expect("concat output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredConcatOperator::new(
                    concat_name,
                    concat_idx,
                    region_id,
                    input_pullers,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((concat_idx, concat_factory));

            // Channel factories for concat inputs
            let cap = capacity;
            let cf1: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((enter_concat_edge_idx, cf1));

            let cap = capacity;
            let cf2: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((fb_concat_edge_idx, cf2));

            // Leave operator: 1 input (Product<T,TInner>,D), 1 output (T,D)
            leave_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    leave_idx, format!("{name}::leave"), region_id, 1, 1,
                ))
                .expect("operator index unique");
            // Subgraph registration for leave
            state.subgraph_builder.add_operator(
                leave_idx,
                &format!("{name}::leave"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );

            // Leave factory will be added after we know the output pipe from body
            // For now, record where the inner builder should start
            inner_start_idx = state.next_operator_index;
        }

        // Phase 2: Create inner builder for loop body and call body closure.
        let inner_state: Rc<RefCell<BuilderState<Product<T, TInner>>>> =
            Rc::new(RefCell::new(BuilderState {
                graph: DataflowGraph::new(),
                subgraph_builder: SubgraphBuilder::new(0, 0),
                operator_factories: Vec::new(),
                channel_factories: Vec::new(),
                feedback_channel_factories: Vec::new(),
                input_ports: Vec::new(),
                output_ports: Vec::new(),
                input_port_wiring: Vec::new(),
                output_port_wiring: Vec::new(),
                probes: Vec::new(),
                exchange_creators: Vec::new(),
                next_operator_index: inner_start_idx,
                next_collect_index: 0,
                channel_capacity: capacity,
            }));

        // The iteration variable pipe points to concat's output
        let iter_var: Pipe<Product<T, TInner>, D> = Pipe {
            state: Rc::clone(&inner_state),
            op_idx: concat_idx,
            output_slot: 0,
            _phantom: PhantomData,
        };

        let result = body(iter_var);

        // Extract info from result pipes before dropping them
        let feedback_op_idx = result.feedback.op_idx;
        let feedback_output_slot = result.feedback.output_slot;
        let output_op_idx = result.output.op_idx;
        let output_output_slot = result.output.output_slot;
        drop(result);

        // Phase 3: Merge inner state into parent state.
        let inner = match Rc::try_unwrap(inner_state) {
            Ok(cell) => cell.into_inner(),
            Err(_) => panic!("iterate body must not hold Pipe references after returning"),
        };

        {
            let mut state = self.state.borrow_mut();

            // Advance parent's next_operator_index
            if inner.next_operator_index > state.next_operator_index {
                state.next_operator_index = inner.next_operator_index;
            }

            // Merge inner operators into parent graph
            for op in inner.graph.operators() {
                state
                    .graph
                    .register_operator(op.clone())
                    .expect("inner operator index conflict");
            }
            // Merge inner edges with offset: inner edge 0 becomes parent edge N
            let inner_edge_offset = state.graph.edges().len();
            for edge in inner.graph.edges() {
                state.graph.add_edge(edge.clone());
            }
            for edge in inner.graph.feedback_edges() {
                state.graph.add_feedback_edge(edge.clone());
            }

            // Merge inner subgraph builder operators and edges into parent.
            // Inner operators use Product<T,TInner> timestamps internally, but for
            // the parent's progress tracker we register them with identity T::Summary
            // connectivity (sufficient for activation scheduling).
            for shape in inner.subgraph_builder.operator_shapes() {
                let mut conn: PortConnectivity<T::Summary> = PortConnectivity::new(shape.inputs, shape.outputs);
                for i in 0..shape.inputs {
                    for o in 0..shape.outputs {
                        conn.path_mut(i, o).insert(T::Summary::default());
                    }
                }
                state.subgraph_builder.add_operator(
                    shape.index,
                    &shape.name,
                    shape.inputs,
                    shape.outputs,
                    conn,
                );
            }
            for (src, tgt) in inner.subgraph_builder.edges() {
                state.subgraph_builder.add_edge(src.clone(), tgt.clone());
            }

            // Merge factories (offset inner channel factory indices)
            state.operator_factories.extend(inner.operator_factories);
            for (edge_idx, factory) in inner.channel_factories {
                state.channel_factories.push((edge_idx + inner_edge_offset, factory));
            }
            // Merge inner feedback channel factories (offset by parent's existing feedback edges)
            let fb_offset = state.graph.feedback_edges().len();
            for (fb_idx, factory) in inner.feedback_channel_factories {
                state.feedback_channel_factories.push((fb_idx + fb_offset, factory));
            }

            // Wire output: result.output → leave_op input (regular edge)
            // Must be added before feedback edge so indices are sequential.
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(output_op_idx, output_output_slot),
                Slot::new(leave_idx, 0),
                region_id,
                region_id,
            ));
            let leave_edge_idx = state.graph.edges().len() - 1;

            // Subgraph edge for leave
            state.subgraph_builder.add_edge(
                Location::source(output_op_idx, output_output_slot),
                Location::target(leave_idx, 0),
            );

            // Wire feedback: result.feedback → feedback_op input (as feedback edge, not regular)
            state.graph.add_feedback_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(feedback_op_idx, feedback_output_slot),
                Slot::new(feedback_idx, 0),
                region_id,
                region_id,
            ));

            // NOTE: We do NOT add the feedback edge to subgraph_builder because it's
            // a back-edge. Adding it would create a cycle in the reachability graph
            // and prevent termination detection.

            // Channel factory for feedback edge.
            // Stored separately; merged with correct indices at build() time.
            // Index by position in feedback_edges (0-based).
            let fb_position = state.graph.feedback_edges().len() - 1;
            let cap = capacity;
            let cf_fb: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.feedback_channel_factories.push((fb_position, cf_fb));

            // Leave operator factory
            let leave_name = format!("{name}::leave");
            let leave_factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<PT<T, TInner>, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .expect("leave must have input puller")
                    .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                    .expect("leave input puller type mismatch");

                let output_pusher: Box<dyn Push<T, D>> = {
                    let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D>>>()
                                .expect("leave output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredLeaveOperator::<T, TInner, D>::new(
                    leave_name,
                    leave_idx,
                    region_id,
                    input_puller,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((leave_idx, leave_factory));

            // Channel factory for leave's input edge
            let cap = capacity;
            let cf_leave: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((leave_edge_idx, cf_leave));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx: leave_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Merge multiple same-typed streams into one.
    ///
    /// Creates a concat operator that forwards all data from every input
    /// Pipe to a single output Pipe. Data order within a timestamp is
    /// preserved per-input but interleaved across inputs.
    ///
    /// # Panics
    ///
    /// Panics if `streams` is empty or if streams belong to different builders.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let merged = Pipe::concat(vec![evens, odds, zeros]);
    /// ```
    pub fn concat(streams: Vec<Pipe<T, D>>) -> Pipe<T, D> {
        assert!(!streams.is_empty(), "concat requires at least one Pipe");

        // Verify all streams share the same builder.
        for s in &streams[1..] {
            assert!(
                Rc::ptr_eq(&streams[0].state, &s.state),
                "concat streams must belong to the same DataflowBuilder"
            );
        }

        let num_inputs = streams.len();
        let op_idx;
        let region_id = RegionId::new(0);
        let state_rc = Rc::clone(&streams[0].state);

        {
            let mut state = state_rc.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (N inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, "concat", region_id, num_inputs, 1,
                ))
                .expect("operator index unique");

            // Edges and channel factories for each input
            let mut edge_indices = Vec::with_capacity(num_inputs);
            for (i, s) in streams.iter().enumerate() {
                state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                    Slot::new(s.op_idx, s.output_slot),
                    Slot::new(op_idx, i),
                    region_id,
                    region_id,
                ));
                edge_indices.push(state.graph.edges().len() - 1);
            }

            // Subgraph: N inputs → 1 output, all paths active
            let mut connectivity = PortConnectivity::new(num_inputs, 1);
            for i in 0..num_inputs {
                connectivity.path_mut(i, 0).insert(T::Summary::default());
            }
            state
                .subgraph_builder
                .add_operator(op_idx, "concat", num_inputs, 1, connectivity);
            for (i, s) in streams.iter().enumerate() {
                state.subgraph_builder.add_edge(
                    Location::source(s.op_idx, s.output_slot),
                    Location::target(op_idx, i),
                );
            }

            // Operator factory
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_pullers: Vec<Box<dyn Pull<T, D>>> = endpoints
                    .input_pullers
                    .into_iter()
                    .map(|any_box| {
                        *any_box
                            .downcast::<Box<dyn Pull<T, D>>>()
                            .expect("concat input puller type mismatch")
                    })
                    .collect();

                let output_pusher: Box<dyn Push<T, D>> = {
                    let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                        .output_pushers
                        .into_iter()
                        .map(|any_box| {
                            *any_box
                                .downcast::<Box<dyn Push<T, D>>>()
                                .expect("concat output pusher type mismatch")
                        })
                        .collect();
                    tee_or_single(pushers).unwrap_or_else(|| Box::new(NullPush))
                };

                Box::new(WiredConcatOperator::new(
                    "concat",
                    op_idx,
                    region_id,
                    input_pullers,
                    output_pusher,
                )) as Box<dyn SchedulableOperator>
            });
            state.operator_factories.push((op_idx, factory));

            // Channel factories for each input edge
            let capacity = state.channel_capacity;
            for edge_idx in edge_indices {
                let chan_factory: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                    let (push, pull) = bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                    (
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    )
                });
                state.channel_factories.push((edge_idx, chan_factory));
            }
        }

        Pipe {
            state: state_rc,
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Declare this Pipe as a named output port.
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
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
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
            let wiring: OutputPortWiring = Box::new(move |mode, wake_handle| {
                use crate::dataflow::channel_operators::{ChannelSinkOperator, OutputReceiver, OutputSend};

                match mode {
                    ChannelMode::Sync => {
                        let (tx, rx) = std::sync::mpsc::sync_channel::<OutputEvent<T, D>>(256);
                        let receiver = OutputReceiver::new(rx);

                        let sink_name_inner = sink_name.clone();
                        let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                            let input_puller: Box<dyn Pull<T, D>> = *endpoints
                                .input_pullers
                                .into_iter()
                                .next()
                                .expect("sink must have input puller")
                                .downcast::<Box<dyn Pull<T, D>>>()
                                .expect("sink input puller type mismatch");

                            Box::new(ChannelSinkOperator::new(
                                sink_name_inner,
                                op_idx,
                                RegionId::new(0),
                                input_puller,
                                OutputSend::Std(tx),
                            )) as Box<dyn SchedulableOperator>
                        });

                        let receiver_any: Box<dyn std::any::Any + Send> = Box::new(receiver);
                        (factory, receiver_any)
                    }
                    #[cfg(feature = "async-io")]
                    ChannelMode::Async => {
                        use crate::dataflow::channel_operators::AsyncOutputReceiver;

                        let (tx, rx) = tokio::sync::mpsc::channel::<OutputEvent<T, D>>(256);
                        let receiver = if let Some(wh) = wake_handle {
                            AsyncOutputReceiver::with_wake_handle(rx, wh)
                        } else {
                            AsyncOutputReceiver::new(rx)
                        };

                        let sink_name_inner = sink_name.clone();
                        let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                            let input_puller: Box<dyn Pull<T, D>> = *endpoints
                                .input_pullers
                                .into_iter()
                                .next()
                                .expect("sink must have input puller")
                                .downcast::<Box<dyn Pull<T, D>>>()
                                .expect("sink input puller type mismatch");

                            Box::new(ChannelSinkOperator::new(
                                sink_name_inner,
                                op_idx,
                                RegionId::new(0),
                                input_puller,
                                OutputSend::Tokio(tx),
                            )) as Box<dyn SchedulableOperator>
                        });

                        let receiver_any: Box<dyn std::any::Any + Send> = Box::new(receiver);
                        (factory, receiver_any)
                    }
                }
            });
            state.output_port_wiring.push(wiring);

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let capacity = state.channel_capacity;
            let chan_factory: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>) as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>) as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, chan_factory));
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

    /// Repartition data across workers based on a hash function.
    ///
    /// Records with the same hash are routed to the same worker, enabling
    /// key-partitioned computations like group-by and join.
    ///
    /// In single-worker mode, this is a pass-through (no routing needed).
    /// In multi-worker mode (`spawn_multi`), the runtime creates shared
    /// exchange channels that route data between workers.
    ///
    /// # Example
    /// ```ignore
    /// let partitioned = stream.exchange("by_key", |record: &(u64, String)| record.0);
    /// ```
    pub fn exchange<K: std::hash::Hash + 'static>(
        self,
        name: impl Into<String>,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let exchange_fn =
            crate::dataflow::channels::pact::ExchangeFn::by_key(&name.into(), key_fn);
        self.add_exchange_internal(exchange_fn)
    }

    /// Repartition data using a direct hash function (returns u64).
    ///
    /// The returned u64 is reduced modulo the target worker count to
    /// determine routing.
    pub fn exchange_by_hash(
        self,
        name: impl Into<String>,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let exchange_fn =
            crate::dataflow::channels::pact::ExchangeFn::new(name, hash_fn);
        self.add_exchange_internal(exchange_fn)
    }

    /// Internal: add an exchange (repartition) operator.
    ///
    /// Creates a pass-through unary operator with an exchange channel
    /// on its input edge. For multi-worker execution, `spawn_multi`
    /// replaces the placeholder pipeline factory with shared exchange
    /// channel factories.
    fn add_exchange_internal(
        &self,
        exchange_fn: crate::dataflow::channels::pact::ExchangeFn<D>,
    ) -> Pipe<T, D> {
        let op_idx;
        let region_id = RegionId::new(0);

        // Identity pass-through logic: forward all input to output unchanged.
        let wired_logic =
            move |input: &mut InputHandle<T, D>, output: &mut OutputHandle<T, D>| -> Result<()> {
                while let Some((time, data)) = input.next() {
                    if !data.is_empty() {
                        output.push_vec(time, data);
                    }
                }
                Ok(())
            };

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register operator as "exchange" in the graph.
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, "exchange", region_id, 1, 1,
                ))
                .expect("operator index unique");

            // Edge from upstream — marked as Exchange.
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::exchange(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                region_id,
                region_id,
            ));

            // Subgraph connectivity.
            state.subgraph_builder.add_operator(
                op_idx,
                "exchange",
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            );
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — pass-through unary.
            let name_clone = String::from("exchange");
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .expect("exchange must have input puller")
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .expect("exchange input puller type mismatch");

                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                *any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .expect("exchange output pusher type mismatch")
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

            // Channel factory — pipeline placeholder for single-worker.
            // spawn_multi replaces this with shared exchange factories.
            let edge_idx = state.graph.edges().len() - 1;
            let capacity = state.channel_capacity;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                    (
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    )
                });
            state.channel_factories.push((edge_idx, chan_factory));

            // Store exchange factory creator for multi-worker.
            let creator = crate::dataflow::channels::exchange_channel::create_exchange_factory_creator::<T, D>(exchange_fn);
            state.exchange_creators.push((edge_idx, creator));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            _phantom: PhantomData,
        }
    }

    /// Attach a probe to observe the frontier at this point in the pipeline.
    ///
    /// Returns `(Pipe, ProbeHandle)` — the Pipe continues unchanged,
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
    ) -> Pipe<T, D2>
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
            // pushers in a TeePush adapter when the Pipe was cloned.
            let name_clone = name.clone();
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
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
            let chan_factory: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
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
    ) -> Pipe<T, D2>
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
            let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
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
            let chan_factory: ChannelFactory = channel_factory(move |_ctx, _cap: usize, wake: Option<WakeHandle>| {
                let (push, pull) = bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone());
                (
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                )
            });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
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
/// `LogicalDataflow` is `Send` and can be submitted to any runtime for
/// physical materialization and execution. It is **single-use** — each
/// materialization consumes the factories (which may contain `FnOnce` closures).
///
/// `LogicalDataflow` is a purely logical artifact — it knows nothing about
/// threads, channels, or execution strategy. All physical execution goes
/// through a runtime:
///
/// - [`SimpleRuntime`](crate::runtime::SimpleRuntime) — single-thread, for tests and simple scripts
/// - [`RuntimeHandle`](crate::runtime::RuntimeHandle) — shared worker pool, for production
pub struct LogicalDataflow<T: Timestamp> {
    pub(crate) name: String,
    pub(crate) graph: DataflowGraph,
    pub(crate) subgraph_builder: SubgraphBuilder<T>,
    pub(crate) operator_factories: Vec<(usize, OperatorFactory)>,
    pub(crate) channel_factories: Vec<(usize, ChannelFactory)>,
    pub(crate) input_ports: Vec<InputPortInfo>,
    pub(crate) output_ports: Vec<OutputPortInfo>,
    pub(crate) input_port_wiring: Vec<InputPortWiring>,
    pub(crate) output_port_wiring: Vec<OutputPortWiring>,
    pub(crate) probes: Vec<(usize, ProbeHandle<T>)>,
    /// Type-erased exchange factory creators — one per exchange edge.
    /// Consumed by `spawn_multi` to produce shared cross-worker channel factories.
    pub(crate) exchange_creators:
        Vec<(usize, crate::dataflow::channels::exchange_channel::ExchangeFactoryCreatorFn)>,
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

    /// Returns true if this dataflow has declared input ports.
    ///
    /// Dataflows with input ports require `spawn()` on a runtime;
    /// they cannot be run with `SimpleRuntime::run()`.
    pub fn has_input_ports(&self) -> bool {
        !self.input_ports.is_empty()
    }

    /// Get a reference to the dataflow graph.
    pub fn graph(&self) -> &DataflowGraph {
        &self.graph
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
                self.collector
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((time, data));
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
    use crate::cancellation::CancellationToken;
    use crate::runtime::SimpleRuntime;

    fn rt() -> SimpleRuntime {
        SimpleRuntime::new()
    }

    #[test]
    fn test_empty_builder() {
        let builder = DataflowBuilder::<u64>::new("empty");
        assert_eq!(builder.operator_count(), 0);
        let dataflow = builder.build().unwrap();
        assert_eq!(dataflow.operator_count(), 0);
        rt().run(dataflow).unwrap();
    }

    #[test]
    fn test_source_to_output() {
        let builder = DataflowBuilder::<u64>::new("source_to_output");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream.output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

        let evens_c = evens_port.collector();
        let evens = evens_c.lock().unwrap();
        let odds_c = odds_port.collector();
        let odds = odds_c.lock().unwrap();

        assert_eq!(evens[0].1, vec![2, 4, 6]);
        assert_eq!(odds[0].1, vec![1, 3, 5]);
    }

    #[test]
    fn test_multiple_inputs() {
        let builder = DataflowBuilder::<u64>::new("multi_input");
        let a = builder.source("a", vec![(0u64, vec![10i32, 20])]);
        let b = builder.source("b", vec![(0u64, vec![100i32, 200])]);

        let port_a = a.map("inc_a", |_t, x| x + 1).output("out_a");
        let port_b = b.map("inc_b", |_t, x| x + 1).output("out_b");

        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
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
        rt().run(dataflow).unwrap();

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
        rt().run(dataflow).unwrap();

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
        let result = SimpleRuntime::with_cancel(cancel).run(dataflow);
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
        assert_eq!(dataflow.operator_count(), 4);
    }

    #[test]
    #[should_panic(expected = "duplicate output port name")]
    fn test_duplicate_output_name_panics() {
        let builder = DataflowBuilder::<u64>::new("dup_test");
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
        rt().run(dataflow).unwrap();
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
        rt().run(dataflow).unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec![15]);
    }

    #[test]
    fn test_input_port_rejected_by_run() {
        let builder = DataflowBuilder::<u64>::new("input_test");
        let _port = builder.input::<i32>("data").output("results");
        let dataflow = builder.build().unwrap();
        let result = rt().run(dataflow);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("input ports"));
    }

    #[test]
    fn test_spawn_basic_pipeline() {
        let builder = DataflowBuilder::<u64>::new("spawn_test");
        let input = builder.input::<i32>("numbers");
        input
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();

        let mut handle = rt().spawn(dataflow).unwrap();

        let sender = handle.take_input::<i32>("numbers").unwrap();
        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.send(1, vec![10, 20]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("results").unwrap();
        let mut results = receiver.collect_data();
        results.sort_by_key(|(t, _)| *t);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[0].1, vec![2, 4, 6]);
        assert_eq!(results[1].0, 1);
        assert_eq!(results[1].1, vec![20, 40]);

        handle.join_blocking().unwrap();
    }

    #[test]
    fn test_spawn_filter_pipeline() {
        let builder = DataflowBuilder::<u64>::new("filter_spawn");
        let input = builder.input::<i32>("src");
        input
            .filter("evens", |_t, x| x % 2 == 0)
            .output("evens");
        let dataflow = builder.build().unwrap();

        let mut handle = rt().spawn(dataflow).unwrap();
        let sender = handle.take_input::<i32>("src").unwrap();
        sender.send(0, vec![1, 2, 3, 4, 5, 6]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("evens").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, vec![2, 4, 6]);

        handle.join_blocking().unwrap();
    }

    #[test]
    fn test_spawn_type_mismatch_error() {
        let builder = DataflowBuilder::<u64>::new("type_test");
        let input = builder.input::<i32>("numbers");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt().spawn(dataflow).unwrap();
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

        let handle = rt().spawn(dataflow).unwrap();
        handle.cancel();
        let _ = handle.join();
    }

    #[test]
    fn test_spawn_drop_cancels() {
        let builder = DataflowBuilder::<u64>::new("drop_test");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let _handle = rt().spawn(dataflow).unwrap();
        // SpawnedDataflow::drop cancels and joins
    }

    // --- Binary operator tests ---

    #[test]
    fn test_binary_merge_two_streams() {
        // Binary: combine two streams by pairing data at each timestamp
        let builder = DataflowBuilder::<u64>::new("binary_test");
        let names = builder.source("names", vec![
            (0u64, vec!["alice".to_string(), "bob".to_string()]),
        ]);
        let ages = builder.source("ages", vec![
            (0u64, vec![30i32, 25]),
        ]);

        let port = names.binary::<i32, String, _>(ages, "pair", |names_in, ages_in, out| {
            // Collect names at this timestamp
            let mut name_buf = Vec::new();
            while let Some((_t, data)) = names_in.next() {
                name_buf.extend(data.iter().cloned());
            }
            // Collect ages at this timestamp
            let mut age_buf = Vec::new();
            while let Some((_t, data)) = ages_in.next() {
                age_buf.extend(data.iter().cloned());
            }
            // Zip them together
            if !name_buf.is_empty() || !age_buf.is_empty() {
                let pairs: Vec<String> = name_buf.iter().zip(age_buf.iter())
                    .map(|(n, a)| format!("{n}={a}"))
                    .collect();
                out.push_vec(0, pairs);
            }
            Ok(())
        }).output("results");

        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec!["alice=30", "bob=25"]);
    }

    #[test]
    fn test_binary_different_types() {
        // Binary with different input types
        let builder = DataflowBuilder::<u64>::new("binary_types");
        let ints = builder.source("ints", vec![(0u64, vec![1i32, 2, 3])]);
        let strs = builder.source("strs", vec![(0u64, vec!["a".to_string(), "b".to_string()])]);

        let port = ints.binary::<String, String, _>(strs, "combine", |ints_in, strs_in, out| {
            let mut result = Vec::new();
            while let Some((t, data)) = ints_in.next() {
                for x in data {
                    result.push((t, format!("int:{x}")));
                }
            }
            while let Some((t, data)) = strs_in.next() {
                for s in data {
                    result.push((t, format!("str:{s}")));
                }
            }
            for (t, s) in result {
                out.push_vec(t, vec![s]);
            }
            Ok(())
        }).output("out");

        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<&str> = r.iter().flat_map(|(_, v)| v.iter().map(|s| s.as_str())).collect();
        all.sort();
        assert_eq!(all, vec!["int:1", "int:2", "int:3", "str:a", "str:b"]);
    }

    // --- Concat tests ---

    #[test]
    fn test_concat_two_streams() {
        let builder = DataflowBuilder::<u64>::new("concat_test");
        let a = builder.source("a", vec![(0u64, vec![1i32, 2])]);
        let b = builder.source("b", vec![(0u64, vec![3i32, 4])]);

        let port = Pipe::concat(vec![a, b]).output("merged");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_concat_three_streams() {
        let builder = DataflowBuilder::<u64>::new("concat3");
        let a = builder.source("a", vec![(0u64, vec![1i32])]);
        let b = builder.source("b", vec![(0u64, vec![2i32])]);
        let c = builder.source("c", vec![(0u64, vec![3i32])]);

        let port = Pipe::concat(vec![a, b, c]).output("merged");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let col = port.collector();
        let r = col.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_merge_two_streams() {
        let builder = DataflowBuilder::<u64>::new("merge_test");
        let evens = builder.source("evens", vec![(0u64, vec![2i32, 4, 6])]);
        let odds = builder.source("odds", vec![(0u64, vec![1i32, 3, 5])]);

        let port = evens.merge(odds).output("all");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_concat_then_process() {
        // Concat → downstream operator
        let builder = DataflowBuilder::<u64>::new("concat_chain");
        let a = builder.source("a", vec![(0u64, vec![1i32, 2])]);
        let b = builder.source("b", vec![(0u64, vec![3i32, 4])]);

        let port = Pipe::concat(vec![a, b])
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![2, 4, 6, 8]);
    }

    #[test]
    fn test_concat_multiple_timestamps() {
        let builder = DataflowBuilder::<u64>::new("concat_ts");
        let a = builder.source("a", vec![(0u64, vec![1i32]), (1u64, vec![10])]);
        let b = builder.source("b", vec![(0u64, vec![2i32]), (1u64, vec![20])]);

        let port = Pipe::concat(vec![a, b]).output("merged");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut t0: Vec<i32> = r.iter().filter(|(t, _)| *t == 0).flat_map(|(_, v)| v.iter().copied()).collect();
        let mut t1: Vec<i32> = r.iter().filter(|(t, _)| *t == 1).flat_map(|(_, v)| v.iter().copied()).collect();
        t0.sort();
        t1.sort();
        assert_eq!(t0, vec![1, 2]);
        assert_eq!(t1, vec![10, 20]);
    }

    // --- Iterate tests ---

    #[test]
    fn test_iterate_simple() {
        // Iterate: multiply by 2 until >= 100
        let builder = DataflowBuilder::<u64>::new("iterate_simple");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 3, 7])]);
        let port = stream
            .iterate::<u32>("loop", 1u32, |iter_var| {
                let doubled = iter_var.map("double", |_t, x| x * 2);
                let done = doubled.clone().filter("done", |_t, x| *x >= 100);
                let again = doubled.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        // 1 → 2 → 4 → 8 → 16 → 32 → 64 → 128 (7 iterations)
        // 3 → 6 → 12 → 24 → 48 → 96 → 192 (6 iterations)
        // 7 → 14 → 28 → 56 → 112 (4 iterations)
        assert_eq!(all, vec![112, 128, 192]);
    }

    #[test]
    fn test_iterate_convergence() {
        // Items converge at different rates
        let builder = DataflowBuilder::<u64>::new("iterate_conv");
        let stream = builder.source("nums", vec![(0u64, vec![50i32, 90, 10])]);
        let port = stream
            .iterate::<u32>("loop", 1u32, |iter_var| {
                // Add 10 each iteration; exit when >= 100
                let incremented = iter_var.map("add10", |_t, x| x + 10);
                let done = incremented.clone().filter("done", |_t, x| *x >= 100);
                let again = incremented.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        // 50 → 60 → 70 → 80 → 90 → 100 (5 iters)
        // 90 → 100 (1 iter)
        // 10 → 20 → 30 → 40 → 50 → 60 → 70 → 80 → 90 → 100 (9 iters)
        assert_eq!(all, vec![100, 100, 100]);
    }

    #[test]
    fn test_iterate_empty() {
        // Empty input should pass through without hanging
        let builder = DataflowBuilder::<u64>::new("iterate_empty");
        let stream = builder.source::<i32>("nums", vec![]);
        let port = stream
            .iterate::<u32>("loop", 1u32, |iter_var| {
                let done = iter_var.clone().filter("done", |_t, x| *x >= 100);
                let again = iter_var.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn test_iterate_multi_batch() {
        // Multiple batches at different timestamps should all iterate correctly
        let builder = DataflowBuilder::<u64>::new("iterate_multi");
        let stream = builder.source(
            "nums",
            vec![
                (0u64, vec![5i32]),   // 5 → 10 → 20 → 40 → 80 → 160 (5 iters)
                (1u64, vec![50i32]),  // 50 → 100 (1 iter)
            ],
        );
        let port = stream
            .iterate::<u32>("loop", 1u32, |iter_var| {
                let doubled = iter_var.map("double", |_t, x| x * 2);
                let done = doubled.clone().filter("done", |_t, x| *x >= 100);
                let again = doubled.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            })
            .output("results");
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![100, 160]);
    }
}
