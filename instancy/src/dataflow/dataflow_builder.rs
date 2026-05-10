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
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::dataflow::channel_operators::ChannelMode;
use crate::dataflow::channels::bounded::bounded_channel_with_wake;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::tee::tee_or_single;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::context::SharedContext;
use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::operators::handles::{InputHandle, NotifyContext, OutputHandle};
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::probe::ProbeHandle;
use crate::dataflow::schedulable::{
    ChannelEndpoints, ChannelFactory, OperatorFactory, SchedulableOperator, channel_factory,
    single_use_factory,
};
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::Slot;
use crate::dataflow::wired_operators::{
    AsyncLogicFn, WiredBinaryOperator, WiredConcatOperator, WiredEnterOperator,
    WiredFeedbackOperator, WiredLeaveOperator, WiredSourceOperator, WiredUnaryAsyncOperator,
    WiredUnaryNotifyOperator, WiredUnaryOperator,
};
use crate::error::LockResultExt;
use crate::error::{DataflowError, Error, Result};
use crate::order::Product;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::{PortConnectivity, ProgressReporter};
use crate::progress::reachability::Location;
use crate::progress::subgraph::SubgraphBuilder;
use crate::progress::timestamp::Timestamp;

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
    /// Type-erased closures for async source ports. Each closure creates a
    /// ChannelSourceOperator + pump task at spawn time.
    /// Tuple: (operator_index, wiring_closure).
    async_source_wiring: Vec<(usize, AsyncSourceWiring)>,
    probes: Vec<(usize, ProbeHandle<T>)>,
    probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
    /// Type-erased exchange factory creators — one per exchange edge.
    /// Tuple: (edge_index, capacity, creator_fn).
    exchange_creators: Vec<(
        usize,
        usize,
        crate::dataflow::channels::exchange_channel::ExchangeFactoryCreatorFn,
    )>,
    /// Network-capable exchange creators — one per exchange edge (transport feature only).
    /// Stored by `Pipe::exchange` when `transport` feature is enabled.
    /// Used by `spawn_cluster` to create network-backed exchange factories.
    /// Tuple: (edge_index, capacity, creator).
    #[cfg(feature = "transport")]
    exchange_network_creators: Vec<(
        usize,
        usize,
        Box<dyn crate::dataflow::channels::exchange_channel::NetworkExchangeCreator>,
    )>,
    next_operator_index: usize,
    next_collect_index: usize,
    channel_capacity: usize,
    channel_preallocate: Option<usize>,
    /// User-supplied typed context values, accessible via
    /// [`DataflowBuilder::get_context`]. Carried into [`LogicalDataflow`]
    /// on [`DataflowBuilder::build()`].
    contexts: SharedContext,
    /// Whether to catch panics in operator activation.
    catch_panics: bool,
    /// Errors encountered during graph construction. Checked in `build()`.
    builder_errors: Vec<Error>,
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
            WakeHandle,                                     // executor wake handle
            ChannelMode,                                    // sync vs async channel backend
        ) -> (OperatorFactory, Box<dyn std::any::Any + Send>)
        // (factory, InputSender or AsyncInputSender)
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

/// Type-erased closure for wiring an async source during spawn().
///
/// Unlike [`InputPortWiring`] (which returns an InputSender for external use),
/// this returns:
/// - An OperatorFactory for the ChannelSourceOperator
/// - A pump closure that drives the user's async producer into the channel
///
/// The pump closure is `FnOnce` — the runtime spawns it as a background task.
/// It captures the user's async producer, the tokio channel sender, and a
/// `WakeHandle` for notifying the executor.
pub(crate) type AsyncSourceWiring = Box<
    dyn FnOnce(
            std::sync::Arc<std::sync::atomic::AtomicUsize>, // external_inputs_open counter
            WakeHandle,                                     // executor wake handle
            crate::cancellation::CancellationToken,         // cancellation token
        ) -> (OperatorFactory, Box<dyn FnOnce() + Send>) // (factory, pump_task)
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
    /// Maximum number of items a channel can buffer before backpressure
    /// kicks in. When the buffer is full, upstream operators are stalled
    /// until downstream consumes data.
    ///
    /// This is the *logical* limit — it controls flow control, not memory
    /// allocation. Per-edge overrides are available via
    /// [`Pipe::with_capacity`].
    ///
    /// Default: `1024`.
    pub channel_capacity: usize,

    /// Initial physical allocation (in number of items) for channel buffers.
    ///
    /// When `Some(n)`, each channel pre-allocates space for `n` items at
    /// creation time. Use this for high-throughput dataflows where you want
    /// to avoid reallocation overhead during ramp-up.
    ///
    /// When `None` (the default), channels start with a small allocation
    /// (4 items) and grow via doubling as data arrives. This is ideal for
    /// dataflows with many edges where most channels are lightly used.
    ///
    /// The value is clamped to `channel_capacity` — pre-allocating more
    /// than the backpressure limit would be wasteful.
    pub channel_preallocate: Option<usize>,
}

impl Default for DataflowBuilderConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            channel_preallocate: None,
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
                async_source_wiring: Vec::new(),
                probes: Vec::new(),
                probe_notifiers: Vec::new(),
                exchange_creators: Vec::new(),
                #[cfg(feature = "transport")]
                exchange_network_creators: Vec::new(),
                next_operator_index: 1,
                next_collect_index: 0,
                channel_capacity: config.channel_capacity,
                channel_preallocate: config.channel_preallocate,
                contexts: SharedContext::new(),
                catch_panics: false,
                builder_errors: Vec::new(),
            })),
        }
    }

    /// Store a typed context value that operator closures can capture at build time.
    ///
    /// Context values are wrapped in `Arc<T>` internally, so [`get_context`](Self::get_context)
    /// returns a cheaply cloneable `Arc<T>` suitable for capturing in `move` closures.
    ///
    /// If a value of the same type was previously stored, it is replaced. Set all
    /// context values **before** creating operators to ensure consistent captures.
    ///
    /// # Example
    ///
    /// ```
    /// use instancy::dataflow::DataflowBuilder;
    ///
    /// struct MyConfig { pub batch_size: usize }
    ///
    /// let builder = DataflowBuilder::<u64>::new("example");
    /// builder.with_context(MyConfig { batch_size: 512 });
    ///
    /// let cfg = builder.get_context::<MyConfig>().unwrap();
    /// assert_eq!(cfg.batch_size, 512);
    /// ```
    pub fn with_context<C: Send + Sync + 'static>(&self, value: C) -> &Self {
        self.state.borrow_mut().contexts.insert(value);
        self
    }

    /// Store a pre-existing `Arc<C>` as context, avoiding double-wrapping.
    ///
    /// Use this when you already have an `Arc<C>` (e.g., a shared service
    /// handle, connection pool, or metrics collector) that you want to share
    /// across dataflow operators without an extra `Arc` layer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pool = Arc::new(DbPool::connect("...").await?);
    /// builder.with_context_arc(pool.clone());
    ///
    /// let p = builder.get_context::<DbPool>().unwrap();
    /// // p is Arc<DbPool> — same allocation as pool, no double-wrapping
    /// ```
    pub fn with_context_arc<C: Send + Sync + 'static>(&self, value: Arc<C>) -> &Self {
        self.state.borrow_mut().contexts.insert_arc(value);
        self
    }

    /// Retrieve a previously stored context value as `Arc<C>`.
    ///
    /// Returns `None` if no value of type `C` has been stored. The returned `Arc`
    /// can be captured in operator closures:
    ///
    /// ```ignore
    /// let cfg = builder.get_context::<MyConfig>().unwrap();
    /// input.map("transform", move |_t, data| {
    ///     // cfg is Arc<MyConfig> — shared and cheaply cloned
    ///     process(data, cfg.batch_size)
    /// });
    /// ```
    pub fn get_context<C: Send + Sync + 'static>(&self) -> Option<Arc<C>> {
        self.state.borrow().contexts.get::<C>()
    }

    /// Enable panic recovery for operator activations.
    ///
    /// When enabled, panics in operator `activate()` are caught with
    /// `std::panic::catch_unwind` and converted to [`Error::OperatorPanic`].
    /// This prevents a single misbehaving operator from crashing the entire
    /// process.
    ///
    /// Defaults to `false` (panics propagate normally).
    ///
    /// **Note:** This has no effect if the binary is built with `panic = "abort"`.
    ///
    /// [`Error::OperatorPanic`]: crate::error::Error::OperatorPanic
    pub fn catch_panics(&self, enable: bool) -> &Self {
        self.state.borrow_mut().catch_panics = enable;
        self
    }

    /// Declare a named input port that data will be fed into at runtime.
    ///
    /// Returns a [`Pipe`] representing the data flowing from this input.
    /// At execution time, the runtime connects an async channel to this port.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let builder = DataflowBuilder::<u64>::new("my_df");
    /// let stream = builder.input::<i32>("data");
    /// stream.map("double", |_t, x| x * 2).output("results");
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if an input with the same name already exists.
    pub fn input<D: Clone + Send + 'static>(&self, name: impl Into<String>) -> Result<Pipe<T, D>> {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);

        {
            let mut state = self.state.borrow_mut();

            // Validate unique name
            if state.input_ports.iter().any(|p| p.name == name) {
                return Err(Error::Dataflow(DataflowError::InvalidConfig(format!(
                    "duplicate input port name: {name}"
                ))));
            }

            op_idx = state.allocate_operator_index();

            // Register source operator in graph (0 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 0, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Register in subgraph builder with initial capability.
            // The input source holds a capability at T::minimum() until closed.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let source_reporter = match state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                &name,
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![initial_cap],
            ) {
                Ok(progress) => {
                    // Clone the progress reporter for the source's output port.
                    // The ChannelSourceOperator uses this to release the initial capability
                    // (reporter.update(T::minimum(), -1)) when its channel closes.
                    // Without this, the initial capability would never be released, and
                    // downstream frontiers would be stuck at T::minimum() forever —
                    // preventing frontier-based operators (unary_notify) from ever firing
                    // their notifications.
                    progress.reporter(0).clone()
                }
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

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
            let wiring: InputPortWiring =
                Box::new(move |external_inputs_open, wake_handle, mode| {
                    use crate::dataflow::channel_operators::{
                        ChannelSourceOperator, InputRecv, InputSender,
                    };
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
                            let reporter = source_reporter.clone();
                            let factory: OperatorFactory =
                                single_use_factory(move |_ctx, endpoints| {
                                    let output_pusher: Box<dyn Push<T, D>> = {
                                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                                            .output_pushers
                                            .into_iter()
                                            .map(|any_box| {
                                                any_box
                                                    .downcast::<Box<dyn Push<T, D>>>()
                                                    .map(|boxed| *boxed)
                                                    .map_err(|_| {
                                                        Error::Dataflow(
                                                            DataflowError::TypeMismatch {
                                                                operator: "channel source".into(),
                                                                port: "output pusher".into(),
                                                            },
                                                        )
                                                    })
                                            })
                                            .collect::<Result<_>>()?;
                                        tee_or_single(pushers)?
                                            .unwrap_or_else(|| Box::new(NullPush))
                                    };
                                    let op = ChannelSourceOperator::new(
                                        factory_name,
                                        op_idx,
                                        StageId::new(0),
                                        InputRecv::Std(rx),
                                        output_pusher,
                                        Some(reporter),
                                        ext_counter,
                                    );
                                    Ok(Box::new(op) as Box<dyn SchedulableOperator>)
                                });

                            let sender_any: Box<dyn std::any::Any + Send> = Box::new(sender);
                            (factory, sender_any)
                        }
                        ChannelMode::Async => {
                            use crate::dataflow::channel_operators::AsyncInputSender;

                            let (tx, rx) = tokio::sync::mpsc::channel::<InputEvent<T, D>>(256);
                            let sender = AsyncInputSender::with_wake_handle(tx, wake_handle);

                            let ext_counter = Arc::clone(&external_inputs_open);
                            let factory_name = wiring_name.clone();
                            let reporter = source_reporter.clone();
                            let factory: OperatorFactory =
                                single_use_factory(move |_ctx, endpoints| {
                                    let output_pusher: Box<dyn Push<T, D>> = {
                                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                                            .output_pushers
                                            .into_iter()
                                            .map(|any_box| {
                                                any_box
                                                    .downcast::<Box<dyn Push<T, D>>>()
                                                    .map(|boxed| *boxed)
                                                    .map_err(|_| {
                                                        Error::Dataflow(
                                                            DataflowError::TypeMismatch {
                                                                operator: "channel source".into(),
                                                                port: "output pusher".into(),
                                                            },
                                                        )
                                                    })
                                            })
                                            .collect::<Result<_>>()?;
                                        tee_or_single(pushers)?
                                            .unwrap_or_else(|| Box::new(NullPush))
                                    };
                                    let op = ChannelSourceOperator::new(
                                        factory_name,
                                        op_idx,
                                        StageId::new(0),
                                        InputRecv::Tokio(rx),
                                        output_pusher,
                                        Some(reporter),
                                        ext_counter,
                                    );
                                    Ok(Box::new(op) as Box<dyn SchedulableOperator>)
                                });

                            let sender_any: Box<dyn std::any::Any + Send> = Box::new(sender);
                            (factory, sender_any)
                        }
                    }
                });
            state.input_port_wiring.push(wiring);
        }

        Ok(Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        })
    }

    /// Declare a pre-loaded source that emits data immediately (for testing/simple use).
    ///
    /// Unlike [`input`](Self::input), this source has data baked in at build time
    /// rather than receiving it from an async channel at runtime.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stream = builder.source("numbers", vec![
    ///     (0u64, vec![1, 2, 3]),
    ///     (1u64, vec![4, 5]),
    /// ]);
    /// ```
    pub fn source<D: Clone + Send + 'static>(
        &self,
        name: impl Into<String>,
        data: Vec<(T, Vec<D>)>,
    ) -> Pipe<T, D> {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register source operator in graph (0 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 0, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Register in subgraph builder with initial capability.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let reporter = match state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                &name,
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![initial_cap],
            ) {
                Ok(progress) => progress.reporter(0).clone(),
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

            // Create operator factory for pre-loaded source.
            // Handles fan-out: if multiple downstream edges exist (Pipe was
            // cloned), wraps all pushers in a TeePush adapter.
            let name_clone = name.clone();
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "source".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredSourceOperator::with_progress(
                        name_clone,
                        op_idx,
                        stage_id,
                        data,
                        output_pusher,
                        reporter,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }

    /// Declare an async source that feeds data from a user-provided producer.
    ///
    /// The `producer` receives an [`crate::AsyncInputSender`] and drives data into the
    /// dataflow asynchronously. The runtime spawns the producer as a background
    /// task at spawn time and manages its lifecycle (cancellation, cleanup).
    ///
    /// Unlike [`input`](Self::input), there is no external sender handle — the
    /// producer closure **is** the data source.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pipe = builder.source_async::<i32, _, _>("events", |sender| async move {
    ///     for i in 0..100 {
    ///         sender.send(0, vec![i]).await?;
    ///     }
    ///     Ok(())
    /// });
    /// pipe.map("process", |_t, x| x * 2).output("results");
    /// ```
    ///
    /// # Backpressure
    ///
    /// The internal channel is bounded. When the downstream dataflow is slower
    /// than the producer, `sender.send()` yields (returns `Pending`) until
    /// capacity is available.
    ///
    /// # Cancellation
    ///
    /// When the dataflow is cancelled, the producer's `sender.send()` will
    /// return an error (channel closed). The producer should handle this by
    /// returning from the async block.
    ///
    /// # Errors
    ///
    /// If the producer returns `Err`, the pump task logs the error and
    /// closes the channel, causing the source operator to finish gracefully.
    pub fn source_async<D, F, Fut>(&self, name: impl Into<String>, producer: F) -> Pipe<T, D>
    where
        D: Clone + Send + 'static,
        F: FnOnce(crate::dataflow::channel_operators::AsyncInputSender<T, D>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = crate::error::Result<()>> + Send + 'static,
    {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register source operator in graph (0 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 0, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Register in subgraph builder with initial capability.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let source_reporter = match state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                &name,
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![initial_cap],
            ) {
                Ok(progress) => progress.reporter(0).clone(),
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

            // Create async source wiring closure. At spawn time, this creates
            // the tokio channel + ChannelSourceOperator + pump task.
            let wiring_name = name.clone();
            let channel_cap = state.channel_capacity;
            let wiring: AsyncSourceWiring =
                Box::new(move |external_inputs_open, wake_handle, cancel| {
                    use crate::dataflow::channel_operators::{
                        AsyncInputSender, ChannelSourceOperator, InputRecv,
                    };

                    let (tx, rx) = tokio::sync::mpsc::channel::<InputEvent<T, D>>(channel_cap);
                    let sender = AsyncInputSender::with_wake_handle(tx, wake_handle.clone());

                    let ext_counter = std::sync::Arc::clone(&external_inputs_open);
                    let factory_name = wiring_name.clone();
                    let reporter = source_reporter.clone();
                    let factory: OperatorFactory = single_use_factory(move |_ctx, endpoints| {
                        let output_pusher: Box<dyn Push<T, D>> = {
                            let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                                .output_pushers
                                .into_iter()
                                .map(|any_box| {
                                    any_box
                                        .downcast::<Box<dyn Push<T, D>>>()
                                        .map(|boxed| *boxed)
                                        .map_err(|_| {
                                            Error::Dataflow(DataflowError::TypeMismatch {
                                                operator: "async source".into(),
                                                port: "output pusher".into(),
                                            })
                                        })
                                })
                                .collect::<Result<_>>()?;
                            tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                        };
                        let op = ChannelSourceOperator::new(
                            factory_name,
                            op_idx,
                            StageId::new(0),
                            InputRecv::Tokio(rx),
                            output_pusher,
                            Some(reporter),
                            ext_counter,
                        );
                        Ok(Box::new(op) as Box<dyn SchedulableOperator>)
                    });

                    // Build pump task: runs the user's producer in a small tokio runtime.
                    let pump_wake = wake_handle;
                    let pump: Box<dyn FnOnce() + Send> = Box::new(move || {
                        let rt = match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(rt) => rt,
                            Err(err) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    source = %wiring_name,
                                    error = %err,
                                    "failed to create pump runtime"
                                );
                                let _ = err;
                                // Notify the executor so it can detect the dropped sender.
                                pump_wake.notify();
                                return;
                            }
                        };
                        rt.block_on(async move {
                            // Run producer with cancellation support.
                            tokio::select! {
                                biased;
                                _ = cancel.cancelled_async() => {
                                    // Dataflow cancelled — drop sender to close channel.
                                }
                                result = producer(sender) => {
                                    if let Err(e) = result {
                                        // Producer returned an error. Log it and close the
                                        // channel by dropping sender, causing the
                                        // ChannelSourceOperator to finish gracefully.
                                        #[cfg(feature = "tracing")]
                                        tracing::warn!(
                                            source = %wiring_name,
                                            error = %e,
                                            "async source producer failed"
                                        );
                                        let _ = e; // suppress unused warning when tracing is off
                                    }
                                    // On success, sender is dropped here too, which
                                    // closes the channel and signals completion.
                                }
                            }
                            // Notify executor that the source has finished/changed.
                            pump_wake.notify();
                        });
                    });

                    (factory, pump)
                });
            state.async_source_wiring.push((op_idx, wiring));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
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
    ///
    /// # Example
    ///
    /// ```ignore
    /// let builder = DataflowBuilder::<u64>::new("my_df");
    /// let input = builder.input::<i32>("data");
    /// input.map("double", |_t, x| x * 2).output("results");
    /// let dataflow = builder.build().expect("build failed");
    /// ```
    pub fn build(self) -> Result<LogicalDataflow<T>> {
        let mut state = match Rc::try_unwrap(self.state) {
            Ok(cell) => cell.into_inner(),
            Err(_) => {
                return Err(Error::Dataflow(DataflowError::InvalidConfig(
                    "cannot build: outstanding Pipe references still exist — \
                     drop all Pipe handles before calling build()"
                        .into(),
                )));
            }
        };

        // Merge feedback channel factories with correct indices.
        // Feedback edges are materialized at indices: edges.len()..edges.len()+feedback_edges.len()
        let regular_edge_count = state.graph.edges().len();
        for (fb_position, factory) in state.feedback_channel_factories {
            state
                .channel_factories
                .push((regular_edge_count + fb_position, factory));
        }

        // Infer stages from the graph topology (exchange edges form boundaries).
        let stages = crate::dataflow::stage::infer_stages(&state.graph)?;

        // Propagate inferred stage IDs back to OperatorInfo and EdgeInfo so
        // that downstream code (exchange channel wiring) sees correct stages
        // instead of the placeholder StageId(0) assigned at build time.
        {
            let mut op_stage_map = std::collections::HashMap::new();
            for stage in &stages {
                for &op_idx in &stage.operator_indices {
                    op_stage_map.insert(op_idx, stage.id);
                }
            }
            for op in state.graph.operators_mut() {
                if let Some(&sid) = op_stage_map.get(&op.index) {
                    op.stage_id = sid;
                }
            }
            for edge in state.graph.edges_mut() {
                if let Some(&sid) = op_stage_map.get(&edge.source.operator_index) {
                    edge.source_stage = sid;
                }
                if let Some(&sid) = op_stage_map.get(&edge.target.operator_index) {
                    edge.target_stage = sid;
                }
            }
        }

        // Surface any errors accumulated during graph construction.
        if let Some(err) = state.builder_errors.into_iter().next() {
            return Err(err);
        }

        // Validate the graph for structural correctness (missing endpoints,
        // port bounds, duplicate edges, cycles outside feedback scopes).
        state.graph.validate()?;

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
            async_source_wiring: state.async_source_wiring,
            probes: state.probes,
            probe_notifiers: state.probe_notifiers,
            exchange_creators: state.exchange_creators,
            #[cfg(feature = "transport")]
            exchange_network_creators: state.exchange_network_creators,
            contexts: state.contexts,
            stages,
            catch_panics: state.catch_panics,
            collect_metrics: false,
            drain_timeout: None,
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
    /// Per-edge capacity override for the next downstream channel.
    /// When `Some(n)`, the next edge-creating operation uses `n` instead
    /// of `DataflowBuilderConfig::channel_capacity`. Consumed on use;
    /// not propagated to the returned Pipe. Cleared on `clone()`.
    capacity_override: Option<usize>,
    _phantom: PhantomData<D>,
}

impl<T: Timestamp, D: Clone + Send + 'static> Clone for Pipe<T, D> {
    /// Clone this Pipe handle. The `capacity_override` is **not** copied —
    /// each branch independently defaults to the builder's global capacity.
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            op_idx: self.op_idx,
            output_slot: self.output_slot,
            capacity_override: None,
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
    /// Override the channel capacity for the **next** edge-creating operation.
    ///
    /// By default, all edges use `DataflowBuilderConfig::channel_capacity`
    /// (1024). Call `with_capacity` to set a different buffer size for the
    /// channel between this Pipe and the next downstream operator.
    ///
    /// The override is consumed by the next edge-creating method (`map`,
    /// `filter`, `unary`, `binary`, `output`, `exchange`, etc.) and is
    /// **not** propagated further. Methods that do not create an edge
    /// (e.g., `probe`) pass the override through unchanged.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    ///
    /// # Example
    ///
    /// ```ignore
    /// input
    ///     .map("double", |_t, x| x * 2)
    ///     .with_capacity(64)       // next edge uses 64-element buffer
    ///     .filter("positive", |_t, x| x > 0)
    ///     // filter's output edge uses the default (1024)
    ///     .output("results");
    /// ```
    pub fn with_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "channel capacity must be > 0".into(),
            )));
        }
        self.capacity_override = Some(capacity);
        Ok(self)
    }

    /// Resolve the effective channel capacity for the next edge.
    /// Consumes `capacity_override` if present, otherwise falls back
    /// to the builder's global `channel_capacity`.
    fn resolve_capacity(&mut self) -> usize {
        self.capacity_override
            .take()
            .unwrap_or_else(|| self.state.borrow().channel_capacity)
    }

    /// Apply a per-element transformation, producing a new Pipe of type `D2`.
    ///
    /// The closure receives a reference to the timestamp and ownership of each element.
    /// If you need to capture the timestamp, clone it inside the closure.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = stream.map("double", |_t, x: i32| x * 2);
    /// ```
    pub fn map<D2, F>(mut self, name: impl Into<String>, mut logic: F) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(&T, D) -> D2 + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| {
            batch.into_iter().map(|x| logic(&time, x)).collect()
        })
    }

    /// Filter elements by a predicate, keeping only those that return `true`.
    ///
    /// # Example
    /// ```ignore
    /// let evens = stream.filter("evens", |_t, x| x % 2 == 0);
    /// ```
    pub fn filter<F>(mut self, name: impl Into<String>, mut predicate: F) -> Pipe<T, D>
    where
        F: FnMut(&T, &D) -> bool + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| {
            batch.into_iter().filter(|x| predicate(&time, x)).collect()
        })
    }

    /// Split a stream into two based on a predicate.
    ///
    /// Returns `(true_pipe, false_pipe)` where:
    /// - `true_pipe` receives items for which the predicate returns `true`
    /// - `false_pipe` receives items for which the predicate returns `false`
    ///
    /// Each item is routed to exactly one of the two output pipes.
    ///
    /// # Note
    /// The predicate is evaluated once per item per branch (i.e., twice total).
    /// For expensive predicates, consider using [`map`](Self::map) to compute
    /// a `Result` or `Either` type, then split with [`branch_result`](super::Pipe::branch_result).
    ///
    /// # Example
    /// ```ignore
    /// let (evens, odds) = numbers.branch("parity", |_t, x| x % 2 == 0);
    /// evens.map("half", |_t, x| x / 2).output("halved");
    /// odds.output("odd_numbers");
    /// ```
    pub fn branch<F>(self, name: impl Into<String>, predicate: F) -> (Pipe<T, D>, Pipe<T, D>)
    where
        F: FnMut(&T, &D) -> bool + Send + 'static,
    {
        let name = name.into();
        let true_name = format!("{name}::true");
        let false_name = format!("{name}::false");
        let predicate = std::sync::Arc::new(std::sync::Mutex::new(predicate));

        let pred_true = predicate.clone();
        let true_pipe = self.clone().filter(true_name, move |t, x| {
            let mut guard = match pred_true.lock().or_poison("branch predicate") {
                Ok(guard) => guard,
                Err(_) => {
                    // NOTE: Cannot propagate lock poison here — closure signature is
                    // `FnMut(&T, &D) -> bool` and cannot return Result. Poisoned lock
                    // means another thread panicked; returning `false` is acceptable
                    // because the dataflow will be torn down.
                    return false;
                }
            };
            guard(t, x)
        });
        let pred_false = predicate;
        let false_pipe = self.filter(false_name, move |t, x| {
            let mut guard = match pred_false.lock().or_poison("branch predicate") {
                Ok(guard) => guard,
                Err(_) => {
                    // NOTE: Cannot propagate lock poison here — closure signature is
                    // `FnMut(&T, &D) -> bool` and cannot return Result. Poisoned lock
                    // means another thread panicked; returning `false` is acceptable
                    // because the dataflow will be torn down.
                    return false;
                }
            };
            !guard(t, x)
        });

        (true_pipe, false_pipe)
    }

    /// Observe each element flowing through without modifying the stream.
    ///
    /// The closure receives a reference to each element (and its timestamp).
    /// All data passes through unchanged. This is useful for debugging,
    /// logging, or accumulating side-effects.
    ///
    /// # Example
    /// ```ignore
    /// let stream = input
    ///     .inspect("log", |_t, x| println!("saw: {x:?}"))
    ///     .map("double", |_t, x| x * 2);
    /// ```
    pub fn inspect<F>(mut self, name: impl Into<String>, mut logic: F) -> Pipe<T, D>
    where
        F: FnMut(&T, &D) + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| {
            for item in &batch {
                logic(&time, item);
            }
            batch
        })
    }

    /// Observe each batch of elements flowing through without modifying the stream.
    ///
    /// Like [`inspect`](Self::inspect), but the closure receives the entire
    /// batch `&[D]` at once, which is more efficient when per-item overhead
    /// matters (e.g., acquiring a lock once per batch instead of per item).
    ///
    /// # Example
    /// ```ignore
    /// let stream = input
    ///     .inspect_batch("count", |_t, batch| println!("batch size: {}", batch.len()))
    ///     .output("results");
    /// ```
    pub fn inspect_batch<F>(mut self, name: impl Into<String>, mut logic: F) -> Pipe<T, D>
    where
        F: FnMut(&T, &[D]) + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| {
            logic(&time, &batch);
            batch
        })
    }

    /// Consume this stream, calling a closure on each element.
    ///
    /// This is a **terminal** operator — it does not produce an output stream.
    /// Use it when you want to perform a side-effect (e.g., writing to a
    /// database, sending to an external system) for every element without
    /// needing to chain further operators.
    ///
    /// For observation without consuming the stream, use
    /// [`inspect`](Self::inspect) instead.
    ///
    /// # Error handling
    ///
    /// If the closure panics, the executor catches it via `catch_unwind` and
    /// converts it to [`Error::OperatorPanic`],
    /// failing the dataflow gracefully. For recoverable errors, handle them
    /// inside the closure (e.g., log and continue).
    ///
    /// # Example
    /// ```ignore
    /// input
    ///     .map("double", |_t, x| x * 2)
    ///     .for_each("print", |_t, x| println!("got: {x:?}"));
    /// ```
    pub fn for_each<F>(self, name: impl Into<String>, mut logic: F)
    where
        F: FnMut(&T, &D) + Send + 'static,
    {
        self.for_each_sink(name, move |time: &T, batch: &[D]| {
            for item in batch {
                logic(time, item);
            }
        });
    }

    /// Consume this stream, calling a closure on each batch.
    ///
    /// Like [`for_each`](Self::for_each), this is a **terminal** operator
    /// that does not produce an output stream. The closure receives the
    /// entire batch `&[D]` at once, which is more efficient when per-item
    /// overhead matters (e.g., acquiring a lock once per batch, bulk-inserting
    /// into a database).
    ///
    /// # Example
    /// ```ignore
    /// input
    ///     .for_each_batch("bulk-insert", |_t, batch| db.insert_many(batch));
    /// ```
    pub fn for_each_batch<F>(self, name: impl Into<String>, logic: F)
    where
        F: FnMut(&T, &[D]) + Send + 'static,
    {
        self.for_each_sink(name, logic);
    }

    /// Internal: register a terminal sink operator that invokes a batch closure.
    fn for_each_sink<F>(self, name: impl Into<String>, logic: F)
    where
        F: FnMut(&T, &[D]) + Send + 'static,
    {
        let name = name.into();
        let capacity = self.capacity_override.unwrap_or(1024);
        let prealloc = self.state.borrow().channel_preallocate;

        let mut state = self.state.borrow_mut();
        let op_idx = state.allocate_operator_index();
        let stage_id = StageId::new(0);

        // Register sink operator in graph (1 input, 0 outputs)
        state
            .graph
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_idx, &name, stage_id, 1, 0,
            ))
            .unwrap_or_else(|e| state.builder_errors.push(e));

        // Edge from upstream
        state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
            Slot::new(self.op_idx, self.output_slot),
            Slot::new(op_idx, 0),
            stage_id,
            stage_id,
        ));

        // Subgraph: sink has 1 input, 0 outputs
        if let Err(e) =
            state
                .subgraph_builder
                .add_operator(op_idx, &name, 1, 0, PortConnectivity::new(1, 0))
        {
            state.builder_errors.push(e);
        }
        state.subgraph_builder.add_edge(
            Location::source(self.op_idx, self.output_slot),
            Location::target(op_idx, 0),
        );

        // Operator factory
        let name_clone = name.clone();
        let factory: OperatorFactory =
            single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                    .input_pullers
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        Error::Dataflow(DataflowError::MissingEndpoint {
                            operator: "for_each sink".into(),
                            port: "input puller".into(),
                        })
                    })?
                    .downcast::<Box<dyn Pull<T, D>>>()
                    .map_err(|_| {
                        Error::Dataflow(DataflowError::TypeMismatch {
                            operator: "for_each sink".into(),
                            port: "input puller".into(),
                        })
                    })?;

                Ok(Box::new(ForEachSink::new(
                    name_clone,
                    op_idx,
                    stage_id,
                    input_puller,
                    logic,
                )) as Box<dyn SchedulableOperator>)
            });
        state.operator_factories.push((op_idx, factory));

        // Channel factory for the input edge
        let edge_idx = state.graph.edges().len() - 1;
        let chan_factory: ChannelFactory =
            channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                let (push, pull) =
                    bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                Ok((
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                ))
            });
        state.channel_factories.push((edge_idx, chan_factory));
    }

    /// Reduce all elements at each timestamp to a single value.
    ///
    /// Buffers incoming data until a timestamp is complete (the input frontier
    /// advances past it), then applies the reducer to produce one output value
    /// per timestamp. This is the dataflow equivalent of `Iterator::reduce`.
    ///
    /// Returns `Pipe<T, D>` — the output stream contains at most one element
    /// per timestamp. If no data arrives for a timestamp, nothing is emitted.
    ///
    /// # When to use
    ///
    /// Use `reduce` after `exchange` in multi-worker dataflows so that each
    /// worker's subset is fully collected before reduction. Without exchange,
    /// each worker reduces only its local partition.
    ///
    /// # Example
    /// ```ignore
    /// // Sum all values per timestamp
    /// let sums = input.reduce("sum", |a, b| a + b);
    /// ```
    pub fn reduce<F>(mut self, name: impl Into<String>, mut reducer: F) -> Pipe<T, D>
    where
        F: FnMut(D, D) -> D + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        let mut stash: std::collections::BTreeMap<T, Vec<D>> = std::collections::BTreeMap::new();
        self.add_unary_notify_internal(name, capacity, move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                stash.entry(time.clone()).or_default().extend(data);
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(data) = stash.remove(&time) {
                    let reduced = data.into_iter().reduce(&mut reducer);
                    if let Some(value) = reduced {
                        output.push_vec(time, vec![value]);
                    }
                }
            }
            Ok(())
        })
    }

    /// Fold all elements at each timestamp into an accumulator.
    ///
    /// Like [`reduce`](Self::reduce), but starts with an initial value and
    /// can change the output type. Buffers data until a timestamp is complete,
    /// then folds left-to-right. Emits one value per timestamp that had data.
    /// If no data arrives for a timestamp, nothing is emitted (same as `reduce`).
    ///
    /// # Example
    /// ```ignore
    /// // Count elements per timestamp
    /// let counts = input.fold("count", 0usize, |acc, _item| acc + 1);
    ///
    /// // Collect into a sorted vec
    /// let sorted = input.fold("sort", Vec::new(), |mut v, x| { v.push(x); v.sort(); v });
    /// ```
    pub fn fold<D2, F>(mut self, name: impl Into<String>, init: D2, mut folder: F) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(D2, D) -> D2 + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        let mut stash: std::collections::BTreeMap<T, Vec<D>> = std::collections::BTreeMap::new();
        self.add_unary_notify_internal(name, capacity, move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                stash.entry(time.clone()).or_default().extend(data);
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                let data = stash.remove(&time).unwrap_or_default();
                let result = data.into_iter().fold(init.clone(), &mut folder);
                output.push_vec(time, vec![result]);
            }
            Ok(())
        })
    }

    /// Remove duplicate elements within each timestamp.
    ///
    /// Buffers data until a timestamp is complete (input frontier advances
    /// past it), then emits only unique elements. Uses a `HashSet` for
    /// deduplication, so `D` must implement `Hash + Eq`.
    ///
    /// Order of elements in the output is not guaranteed.
    ///
    /// # When to use
    ///
    /// Use `distinct` after `exchange` in multi-worker dataflows to ensure
    /// duplicates from different workers are eliminated. Without exchange,
    /// each worker deduplicates only its local partition.
    ///
    /// # Example
    /// ```ignore
    /// let unique = input.distinct("dedup");
    /// ```
    pub fn distinct(mut self, name: impl Into<String>) -> Pipe<T, D>
    where
        D: std::hash::Hash + Eq,
    {
        let capacity = self.resolve_capacity();
        let mut stash: std::collections::BTreeMap<T, Vec<D>> = std::collections::BTreeMap::new();
        self.add_unary_notify_internal(name, capacity, move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                stash.entry(time.clone()).or_default().extend(data);
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(data) = stash.remove(&time) {
                    let mut seen = std::collections::HashSet::with_capacity(data.len());
                    let unique: Vec<D> = data
                        .into_iter()
                        .filter(|item| seen.insert(item.clone()))
                        .collect();
                    if !unique.is_empty() {
                        output.push_vec(time, unique);
                    }
                }
            }
            Ok(())
        })
    }

    /// Count elements per timestamp.
    ///
    /// Convenience wrapper around [`fold`](Self::fold) that returns the
    /// number of elements received at each timestamp as a `usize`.
    ///
    /// # Example
    /// ```ignore
    /// let counts = input.count("count-per-epoch");
    /// ```
    pub fn count(self, name: impl Into<String>) -> Pipe<T, usize> {
        self.fold(name, 0usize, |acc, _item| acc + 1)
    }

    /// Delay data by re-assigning timestamps according to a per-item function.
    ///
    /// Each item is buffered and re-timestamped using `delay_fn(&time, &data) -> new_time`.
    /// The data is held until the input frontier advances past the **new** (delayed) timestamp,
    /// then emitted at that timestamp. This ensures downstream operators see correct
    /// frontier progress.
    ///
    /// The `delay_fn` must return a timestamp `>= time` (the new timestamp must not
    /// precede the original). Violating this will panic.
    ///
    /// # Use cases
    /// - **Per-item windowing**: assign items to windows based on content
    /// - **Priority-based delay**: delay low-priority items to later timestamps
    ///
    /// # Example
    /// ```ignore
    /// // Delay each item to the next 10-second window based on its value
    /// let windowed = stream.delay("window", |t, item| {
    ///     if item.priority > 5 { *t } else { *t + 10 }
    /// });
    /// ```
    pub fn delay<F>(mut self, name: impl Into<String>, delay_fn: F) -> Pipe<T, D>
    where
        F: Fn(&T, &D) -> T + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        let mut stash: std::collections::BTreeMap<T, Vec<D>> = std::collections::BTreeMap::new();
        self.add_unary_notify_internal(name, capacity, move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                for item in data {
                    let new_time = delay_fn(&time, &item);
                    if new_time < time {
                        return Err(Error::Dataflow(DataflowError::InvalidConfig(
                            "delay_fn must not return a timestamp earlier than the input".into(),
                        )));
                    }
                    stash.entry(new_time.clone()).or_default().push(item);
                    ctx.notify_at(new_time);
                }
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(data) = stash.remove(&time) {
                    output.push_vec(time, data);
                }
            }
            Ok(())
        })
    }

    /// Delay data by re-assigning timestamps according to a per-timestamp function.
    ///
    /// Simpler version of [`delay`](Self::delay) when the new timestamp depends only
    /// on the original timestamp, not the data content. All items at a given timestamp
    /// are re-assigned to the same new timestamp.
    ///
    /// The `delay_fn` must return a timestamp `>= t` (the new timestamp must not
    /// precede the original). Violating this will panic.
    ///
    /// # Use cases
    /// - **Fixed windowing**: `delay_batch("window", |t| t / 10 * 10)` groups into 10-unit windows
    /// - **Ordering guarantee**: `delay_batch("order", |t| *t)` (identity) buffers until frontier
    ///   confirms no more data at `t` will arrive
    /// - **Time shifting**: `delay_batch("shift", |t| t + 5)` shifts all data forward by 5 units
    ///
    /// # Example
    /// ```ignore
    /// // Group data into 100-unit windows
    /// let windowed = stream.delay_batch("window-100", |t| t / 100 * 100 + 100);
    /// ```
    pub fn delay_batch<F>(mut self, name: impl Into<String>, delay_fn: F) -> Pipe<T, D>
    where
        F: Fn(&T) -> T + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        let mut stash: std::collections::BTreeMap<T, Vec<D>> = std::collections::BTreeMap::new();
        self.add_unary_notify_internal(name, capacity, move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                let new_time = delay_fn(&time);
                if new_time < time {
                    return Err(Error::Dataflow(DataflowError::InvalidConfig(
                        "delay_fn must not return a timestamp earlier than the input".into(),
                    )));
                }
                stash.entry(new_time.clone()).or_default().extend(data);
                ctx.notify_at(new_time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(data) = stash.remove(&time) {
                    output.push_vec(time, data);
                }
            }
            Ok(())
        })
    }

    /// Pass through the first `count` elements, then stop.
    ///
    /// This is the dataflow equivalent of SQL `LIMIT`. After emitting `count`
    /// items, the operator completes and its output closes. Downstream
    /// operators in this branch will drain and finish, while other branches
    /// of the dataflow continue running.
    ///
    /// # Example
    /// ```ignore
    /// let first_10 = stream.take("limit-10", 10);
    /// ```
    pub fn take(mut self, name: impl Into<String>, count: usize) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        let mut remaining = count;
        self.add_unary_internal(name, capacity, move |_time, batch| {
            if remaining == 0 {
                return Vec::new();
            }
            if batch.len() <= remaining {
                remaining -= batch.len();
                batch
            } else {
                let taken: Vec<D> = batch.into_iter().take(remaining).collect();
                remaining = 0;
                taken
            }
        })
    }

    /// Pass through elements while a predicate returns `true`, then stop.
    ///
    /// Once the predicate returns `false` for any element, the operator
    /// stops emitting and completes. Remaining elements in the current
    /// batch and all future batches are discarded.
    ///
    /// This enables condition-based early termination of a branch without
    /// cancelling the entire dataflow.
    ///
    /// # Example
    /// ```ignore
    /// let until_large = stream.take_while("small-only", |_t, x| *x < 1000);
    /// ```
    pub fn take_while<F>(mut self, name: impl Into<String>, mut predicate: F) -> Pipe<T, D>
    where
        F: FnMut(&T, &D) -> bool + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        let mut stopped = false;
        self.add_unary_internal(name, capacity, move |time, batch| {
            if stopped {
                return Vec::new();
            }
            let mut result = Vec::with_capacity(batch.len());
            for item in batch {
                if predicate(&time, &item) {
                    result.push(item);
                } else {
                    stopped = true;
                    break;
                }
            }
            result
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
    pub fn flat_map<D2, F>(mut self, name: impl Into<String>, mut logic: F) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(&T, D) -> Vec<D2> + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| {
            batch.into_iter().flat_map(|x| logic(&time, x)).collect()
        })
    }

    /// Transform each batch as a whole, producing zero or more output items.
    ///
    /// Unlike [`flat_map`](Self::flat_map) which processes one element at a time,
    /// `map_batch` receives the entire batch for a timestamp at once. This
    /// enables efficient bulk operations like sorting, batch deduplication, or
    /// transformations that benefit from seeing all items together.
    ///
    /// # Example
    /// ```ignore
    /// // Sort each batch before emitting
    /// let sorted = stream.map_batch("sort", |_t, mut batch: Vec<i32>| {
    ///     batch.sort();
    ///     batch
    /// });
    /// ```
    pub fn map_batch<D2, F>(mut self, name: impl Into<String>, mut logic: F) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        F: FnMut(&T, Vec<D>) -> Vec<D2> + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_internal(name, capacity, move |time, batch| logic(&time, batch))
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
    pub fn unary<D2, L>(mut self, name: impl Into<String>, logic: L) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(&mut InputHandle<T, D>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_with_handles(name, capacity, logic)
    }

    /// Apply a unary operator with frontier-based notification support.
    ///
    /// Like [`unary`](Self::unary), but the closure receives a [`NotifyContext`] that
    /// enables buffering data and deferring emission until a timestamp is "complete"
    /// (the input frontier has advanced past it).
    ///
    /// # Why use this instead of `unary`?
    ///
    /// Use `unary_notify` when the operator needs to **buffer data and emit final
    /// results per timestamp**. For example:
    /// - Aggregation after exchange (must wait for all workers' contributions)
    /// - Window/batch operators (collect all data in a time range)
    /// - Sort/distinct (need complete data at a timestamp)
    ///
    /// The `NotifyContext` provides:
    /// - `ctx.notify_at(time)` — request a callback when time is complete, AND hold
    ///   an output capability to prevent downstream from advancing past `time`
    /// - `ctx.next_notification()` — consume fired notifications and drop capabilities
    ///
    /// # Progress safety
    ///
    /// When you call `ctx.notify_at(time)`, an output capability is created that
    /// prevents downstream frontiers from advancing past `time`. This capability
    /// is automatically dropped when you call `ctx.next_notification()`. If you
    /// never consume notifications, downstream will never make progress.
    ///
    /// # Example
    ///
    /// ```ignore
    /// input
    ///     .unary_notify("aggregate", {
    ///         let mut stash: HashMap<u64, Vec<i32>> = HashMap::new();
    ///         move |input, output, ctx| {
    ///             // Buffer incoming data and request notification
    ///             while let Some((time, data)) = input.next() {
    ///                 stash.entry(time).or_default().extend(data);
    ///                 ctx.notify_at(time);
    ///             }
    ///             // When a timestamp is complete, emit the buffered data
    ///             while let Some(time) = ctx.next_notification() {
    ///                 if let Some(data) = stash.remove(&time) {
    ///                     output.push_vec(time, data);
    ///                 }
    ///             }
    ///             Ok(())
    ///         }
    ///     })
    /// ```
    pub fn unary_notify<D2, L>(mut self, name: impl Into<String>, logic: L) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(
                &mut InputHandle<T, D>,
                &mut OutputHandle<T, D2>,
                &mut NotifyContext<'_, T>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let capacity = self.resolve_capacity();
        self.add_unary_notify_internal(name, capacity, logic)
    }

    /// Apply an async unary operator that spawns tokio tasks for each input batch.
    ///
    /// Unlike [`unary`](Self::unary) which processes data synchronously during
    /// operator activation, `unary_async` spawns a tokio task for each input
    /// batch. This is ideal for operators that perform async I/O (database
    /// lookups, HTTP requests, RPC calls).
    ///
    /// # Arguments
    ///
    /// - `name` — operator name for debugging and graph inspection
    /// - `max_concurrency` — maximum number of in-flight async tasks. Input
    ///   batches exceeding this limit are queued internally.
    /// - `logic` — an `Fn(T, Vec<D1>) -> Future<Output = Result<Vec<D2>>>` closure.
    ///   Must be `Send + Sync + 'static` since it is shared across spawned tasks.
    ///
    /// # Output ordering
    ///
    /// Output order matches **completion order**, not input order. If strict
    /// ordering is required, include sequence information in the data and sort
    /// downstream.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    ///
    /// let enriched = input.unary_async("http_lookup", 8, Arc::new(|_time, batch| {
    ///     Box::pin(async move {
    ///         let mut results = Vec::new();
    ///         for item in batch {
    ///             let resp = reqwest::get(format!("http://api/{item}")).await?;
    ///             results.push(resp.text().await?);
    ///         }
    ///         Ok(results)
    ///     })
    /// }));
    /// ```
    pub fn unary_async<D2, F, Fut>(
        mut self,
        name: impl Into<String>,
        max_concurrency: usize,
        logic: Arc<F>,
    ) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        F: Fn(T, Vec<D>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<D2>>> + Send + 'static,
    {
        let capacity = self.resolve_capacity();
        // Wrap into the type-erased AsyncLogicFn form.
        let logic: AsyncLogicFn<T, D, D2> = Arc::new(move |t, data| {
            let fut = logic(t, data);
            Box::pin(fut)
        });
        self.add_unary_async_internal(name, capacity, logic, max_concurrency)
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
        mut self,
        mut other: Pipe<T, D2>,
        name: impl Into<String>,
        logic: L,
    ) -> Result<Pipe<T, D3>>
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
        if !Rc::ptr_eq(&self.state, &other.state) {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "binary operator streams must belong to the same DataflowBuilder".into(),
            )));
        }

        let capacity1 = self.resolve_capacity();
        let capacity2 = other.resolve_capacity();
        let prealloc = self.state.borrow().channel_preallocate;

        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (2 inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 2, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from self → slot 0
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));
            let edge1_idx = state.graph.edges().len() - 1;

            // Edge from other → slot 1
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(other.op_idx, other.output_slot),
                Slot::new(op_idx, 1),
                stage_id,
                stage_id,
            ));
            let edge2_idx = state.graph.edges().len() - 1;

            // Subgraph: 2 inputs → 1 output, both paths active
            let mut connectivity = PortConnectivity::new(2, 1);
            connectivity.path_mut(0, 0).insert(T::Summary::default());
            connectivity.path_mut(1, 0).insert(T::Summary::default());
            if let Err(e) = state
                .subgraph_builder
                .add_operator(op_idx, &name, 2, 1, connectivity)
            {
                state.builder_errors.push(e);
            }
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
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let mut pullers = endpoints.input_pullers.into_iter();

                    let input1_puller: Box<dyn Pull<T, D>> = *pullers
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "binary".into(),
                                port: "input puller 0".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "binary".into(),
                                port: "input puller 0".into(),
                            })
                        })?;

                    let input2_puller: Box<dyn Pull<T, D2>> = *pullers
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "binary".into(),
                                port: "input puller 1".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D2>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "binary".into(),
                                port: "input puller 1".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D3>> = {
                        let pushers: Vec<Box<dyn Push<T, D3>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D3>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "binary".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredBinaryOperator::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        logic,
                        input1_puller,
                        input2_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factories for both input edges
            let channel_factory1: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity1, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge1_idx, channel_factory1));

            let channel_factory2: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D2, ()>(capacity2, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D2>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D2>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge2_idx, channel_factory2));
        }

        Ok(Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        })
    }

    /// Merge this Pipe with another same-typed Pipe.
    ///
    /// Shorthand for `Pipe::concat(vec![self, other])`. Data from both
    /// streams is interleaved in the output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let merged = evens.merge(odds);
    /// merged.output("all_numbers");
    /// ```
    pub fn merge(self, other: Pipe<T, D>) -> Result<Pipe<T, D>> {
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
        mut self,
        name: impl Into<String>,
        summary: TInner::Summary,
        body: impl FnOnce(Pipe<Product<T, TInner>, D>) -> IterateResult<Product<T, TInner>, D>,
    ) -> Pipe<T, D>
    where
        TInner: Timestamp,
        Product<T, TInner>: Timestamp,
    {
        let name = name.into();
        let stage_id = StageId::new(0);
        type PT<T, TInner> = Product<T, TInner>;

        // Resolve per-edge capacity for the enter edge; internal edges use global default.
        let enter_capacity = self.resolve_capacity();
        let prealloc = self.state.borrow().channel_preallocate;

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
                    enter_idx,
                    format!("{name}::enter"),
                    stage_id,
                    1,
                    1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(enter_idx, 0),
                stage_id,
                stage_id,
            ));
            // Subgraph registration for enter
            if let Err(e) = state.subgraph_builder.add_operator(
                enter_idx,
                format!("{name}::enter"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(enter_idx, 0),
            );

            // Enter operator factory
            let enter_name = format!("{name}::enter");
            let enter_factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "enter".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "enter".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                        let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "enter".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredEnterOperator::<T, TInner, D>::new(
                        enter_name,
                        enter_idx,
                        stage_id,
                        input_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((enter_idx, enter_factory));

            // Channel factory for enter's input edge (uses per-edge override if set)
            let enter_edge_idx = state.graph.edges().len() - 1;
            let cap = enter_capacity;
            let cf: ChannelFactory = channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                let (push, pull) =
                    bounded_channel_with_wake::<T, D, ()>(cap, wake.clone(), prealloc);
                Ok((
                    Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                        as Box<dyn std::any::Any + Send>,
                ))
            });
            state.channel_factories.push((enter_edge_idx, cf));

            // Feedback operator: 1 input (Product<T,TInner>,D), 1 output (Product<T,TInner>,D)
            feedback_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    feedback_idx,
                    format!("{name}::feedback"),
                    stage_id,
                    1,
                    1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));
            // Subgraph registration for feedback
            if let Err(e) = state.subgraph_builder.add_operator(
                feedback_idx,
                format!("{name}::feedback"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }

            // Feedback operator factory
            let fb_name = format!("{name}::feedback");
            let fb_summary = summary.clone();
            let feedback_factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<PT<T, TInner>, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "feedback".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "feedback".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                        let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "feedback".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredFeedbackOperator::<T, TInner, D>::new(
                        fb_name,
                        feedback_idx,
                        stage_id,
                        fb_summary,
                        input_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state
                .operator_factories
                .push((feedback_idx, feedback_factory));

            // Concat operator: 2 inputs (enter output + feedback output), 1 output
            concat_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    concat_idx,
                    format!("{name}::concat"),
                    stage_id,
                    2,
                    1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));
            // Edge: enter → concat input 0
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(enter_idx, 0),
                Slot::new(concat_idx, 0),
                stage_id,
                stage_id,
            ));
            let enter_concat_edge_idx = state.graph.edges().len() - 1;
            // Edge: feedback → concat input 1
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(feedback_idx, 0),
                Slot::new(concat_idx, 1),
                stage_id,
                stage_id,
            ));
            let fb_concat_edge_idx = state.graph.edges().len() - 1;

            // Subgraph for concat
            let mut concat_connectivity = PortConnectivity::new(2, 1);
            concat_connectivity
                .path_mut(0, 0)
                .insert(T::Summary::default());
            concat_connectivity
                .path_mut(1, 0)
                .insert(T::Summary::default());
            if let Err(e) = state.subgraph_builder.add_operator(
                concat_idx,
                format!("{name}::concat"),
                2,
                1,
                concat_connectivity,
            ) {
                state.builder_errors.push(e);
            }
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
            let concat_factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_pullers: Vec<Box<dyn Pull<PT<T, TInner>, D>>> = endpoints
                        .input_pullers
                        .into_iter()
                        .map(|any_box| {
                            any_box
                                .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                                .map(|boxed| *boxed)
                                .map_err(|_| {
                                    Error::Dataflow(DataflowError::TypeMismatch {
                                        operator: "concat".into(),
                                        port: "input puller".into(),
                                    })
                                })
                        })
                        .collect::<Result<_>>()?;

                    let output_pusher: Box<dyn Push<PT<T, TInner>, D>> = {
                        let pushers: Vec<Box<dyn Push<PT<T, TInner>, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<PT<T, TInner>, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "concat".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredConcatOperator::new(
                        concat_name,
                        concat_idx,
                        stage_id,
                        input_pullers,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((concat_idx, concat_factory));

            // Channel factories for concat inputs
            let cap = capacity;
            let cf1: ChannelFactory = channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                let (push, pull) =
                    bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone(), prealloc);
                Ok((
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                ))
            });
            state.channel_factories.push((enter_concat_edge_idx, cf1));

            let cap = capacity;
            let cf2: ChannelFactory = channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                let (push, pull) =
                    bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone(), prealloc);
                Ok((
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                ))
            });
            state.channel_factories.push((fb_concat_edge_idx, cf2));

            // Leave operator: 1 input (Product<T,TInner>,D), 1 output (T,D)
            leave_idx = state.allocate_operator_index();
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    leave_idx,
                    format!("{name}::leave"),
                    stage_id,
                    1,
                    1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));
            // Subgraph registration for leave
            if let Err(e) = state.subgraph_builder.add_operator(
                leave_idx,
                format!("{name}::leave"),
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }

            // Leave factory will be added after we know the output pipe from body
            // For now, record where the inner builder should start
            inner_start_idx = state.next_operator_index;
        }

        // Phase 2: Create inner builder for loop body and call body closure.
        // Inherit parent context so operators inside loops can access the same
        // context values. SharedContext::clone() is cheap (shares Arc pointers).
        let parent_contexts = self.state.borrow().contexts.clone();
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
                async_source_wiring: Vec::new(),
                probes: Vec::new(),
                probe_notifiers: Vec::new(),
                exchange_creators: Vec::new(),
                #[cfg(feature = "transport")]
                exchange_network_creators: Vec::new(),
                next_operator_index: inner_start_idx,
                next_collect_index: 0,
                channel_capacity: capacity,
                channel_preallocate: None,
                contexts: parent_contexts,
                catch_panics: false,
                builder_errors: Vec::new(),
            }));

        // The iteration variable pipe points to concat's output
        let iter_var: Pipe<Product<T, TInner>, D> = Pipe {
            state: Rc::clone(&inner_state),
            op_idx: concat_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        };

        let mut result = body(iter_var);

        // Extract info and capacity overrides from result pipes before dropping them
        let feedback_op_idx = result.feedback.op_idx;
        let feedback_output_slot = result.feedback.output_slot;
        let feedback_capacity = result.feedback.resolve_capacity();
        let output_op_idx = result.output.op_idx;
        let output_output_slot = result.output.output_slot;
        let output_capacity = result.output.resolve_capacity();
        drop(result);

        // Phase 3: Merge inner state into parent state.
        let inner = match Rc::try_unwrap(inner_state) {
            Ok(cell) => cell.into_inner(),
            Err(_) => {
                self.state.borrow_mut().builder_errors.push(Error::Dataflow(
                    DataflowError::InvalidConfig(
                        "iterate body must not hold Pipe references after returning".into(),
                    ),
                ));
                return Pipe {
                    state: Rc::clone(&self.state),
                    op_idx: leave_idx,
                    output_slot: 0,
                    capacity_override: None,
                    _phantom: PhantomData,
                };
            }
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
                    .unwrap_or_else(|e| state.builder_errors.push(e));
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
                let mut conn: PortConnectivity<T::Summary> =
                    PortConnectivity::new(shape.inputs, shape.outputs);
                for i in 0..shape.inputs {
                    for o in 0..shape.outputs {
                        conn.path_mut(i, o).insert(T::Summary::default());
                    }
                }
                if let Err(e) = state.subgraph_builder.add_operator(
                    shape.index,
                    &shape.name,
                    shape.inputs,
                    shape.outputs,
                    conn,
                ) {
                    state.builder_errors.push(e);
                }
            }
            for (src, tgt) in inner.subgraph_builder.edges() {
                state.subgraph_builder.add_edge(src.clone(), tgt.clone());
            }

            // Merge factories (offset inner channel factory indices)
            state.operator_factories.extend(inner.operator_factories);
            for (edge_idx, factory) in inner.channel_factories {
                state
                    .channel_factories
                    .push((edge_idx + inner_edge_offset, factory));
            }
            // Merge inner feedback channel factories (offset by parent's existing feedback edges)
            let fb_offset = state.graph.feedback_edges().len();
            for (fb_idx, factory) in inner.feedback_channel_factories {
                state
                    .feedback_channel_factories
                    .push((fb_idx + fb_offset, factory));
            }
            state.builder_errors.extend(inner.builder_errors);

            // Wire output: result.output → leave_op input (regular edge)
            // Must be added before feedback edge so indices are sequential.
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(output_op_idx, output_output_slot),
                Slot::new(leave_idx, 0),
                stage_id,
                stage_id,
            ));
            let leave_edge_idx = state.graph.edges().len() - 1;

            // Subgraph edge for leave
            state.subgraph_builder.add_edge(
                Location::source(output_op_idx, output_output_slot),
                Location::target(leave_idx, 0),
            );

            // Wire feedback: result.feedback → feedback_op input (as feedback edge, not regular)
            state
                .graph
                .add_feedback_edge(crate::dataflow::graph::EdgeInfo::new(
                    Slot::new(feedback_op_idx, feedback_output_slot),
                    Slot::new(feedback_idx, 0),
                    stage_id,
                    stage_id,
                ));

            // NOTE: We do NOT add the feedback edge to subgraph_builder because it's
            // a back-edge. Adding it would create a cycle in the reachability graph
            // and prevent termination detection.

            // Channel factory for feedback edge.
            // Stored separately; merged with correct indices at build() time.
            // Index by position in feedback_edges (0-based).
            let fb_position = state.graph.feedback_edges().len() - 1;
            let cap = feedback_capacity;
            let cf_fb: ChannelFactory = channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                let (push, pull) =
                    bounded_channel_with_wake::<PT<T, TInner>, D, ()>(cap, wake.clone(), prealloc);
                Ok((
                    Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                    Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>)
                        as Box<dyn std::any::Any + Send>,
                ))
            });
            state.feedback_channel_factories.push((fb_position, cf_fb));

            // Leave operator factory
            let leave_name = format!("{name}::leave");
            let leave_factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<PT<T, TInner>, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "leave".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<PT<T, TInner>, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "leave".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "leave".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredLeaveOperator::<T, TInner, D>::new(
                        leave_name,
                        leave_idx,
                        stage_id,
                        input_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((leave_idx, leave_factory));

            // Channel factory for leave's input edge
            let cap = output_capacity;
            let cf_leave: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) = bounded_channel_with_wake::<PT<T, TInner>, D, ()>(
                        cap,
                        wake.clone(),
                        prealloc,
                    );
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<PT<T, TInner>, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<PT<T, TInner>, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((leave_edge_idx, cf_leave));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx: leave_idx,
            output_slot: 0,
            capacity_override: None,
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
    pub fn concat(mut streams: Vec<Pipe<T, D>>) -> Result<Pipe<T, D>> {
        if streams.is_empty() {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "concat requires at least one Pipe".into(),
            )));
        }

        // Verify all streams share the same builder.
        for s in &streams[1..] {
            if !Rc::ptr_eq(&streams[0].state, &s.state) {
                return Err(Error::Dataflow(DataflowError::InvalidConfig(
                    "concat streams must belong to the same DataflowBuilder".into(),
                )));
            }
        }

        // Resolve per-edge capacity for each input before borrowing state.
        let capacities: Vec<usize> = streams.iter_mut().map(|s| s.resolve_capacity()).collect();

        let num_inputs = streams.len();
        let op_idx;
        let stage_id = StageId::new(0);
        let state_rc = Rc::clone(&streams[0].state);
        let prealloc = state_rc.borrow().channel_preallocate;

        {
            let mut state = state_rc.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (N inputs, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, "concat", stage_id, num_inputs, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edges and channel factories for each input
            let mut edge_indices = Vec::with_capacity(num_inputs);
            for (i, s) in streams.iter().enumerate() {
                state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                    Slot::new(s.op_idx, s.output_slot),
                    Slot::new(op_idx, i),
                    stage_id,
                    stage_id,
                ));
                edge_indices.push(state.graph.edges().len() - 1);
            }

            // Subgraph: N inputs → 1 output, all paths active
            let mut connectivity = PortConnectivity::new(num_inputs, 1);
            for i in 0..num_inputs {
                connectivity.path_mut(i, 0).insert(T::Summary::default());
            }
            if let Err(e) =
                state
                    .subgraph_builder
                    .add_operator(op_idx, "concat", num_inputs, 1, connectivity)
            {
                state.builder_errors.push(e);
            }
            for (i, s) in streams.iter().enumerate() {
                state.subgraph_builder.add_edge(
                    Location::source(s.op_idx, s.output_slot),
                    Location::target(op_idx, i),
                );
            }

            // Operator factory
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_pullers: Vec<Box<dyn Pull<T, D>>> = endpoints
                        .input_pullers
                        .into_iter()
                        .map(|any_box| {
                            any_box
                                .downcast::<Box<dyn Pull<T, D>>>()
                                .map(|boxed| *boxed)
                                .map_err(|_| {
                                    Error::Dataflow(DataflowError::TypeMismatch {
                                        operator: "concat".into(),
                                        port: "input puller".into(),
                                    })
                                })
                        })
                        .collect::<Result<_>>()?;

                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "concat".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredConcatOperator::new(
                        "concat",
                        op_idx,
                        stage_id,
                        input_pullers,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factories for each input edge (per-input capacity)
            for (edge_idx, capacity) in edge_indices.into_iter().zip(capacities) {
                let chan_factory: ChannelFactory =
                    channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                        let (push, pull) =
                            bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                        Ok((
                            Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                                as Box<dyn std::any::Any + Send>,
                            Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                                as Box<dyn std::any::Any + Send>,
                        ))
                    });
                state.channel_factories.push((edge_idx, chan_factory));
            }
        }

        Ok(Pipe {
            state: state_rc,
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        })
    }

    /// Declare this Pipe as a named output port.
    ///
    /// Returns an [`OutputPort`] handle. At execution time, the runtime connects
    /// an async channel to this port for collecting results.
    ///
    /// For immediate testing, use [`collect`](Self::collect) instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let port = stream.output("results");
    /// // After execution:
    /// let data = port.collector().lock().unwrap();
    /// for (time, batch) in data.iter() {
    ///     println!("t={time}: {batch:?}");
    /// }
    /// ```
    pub fn output(mut self, name: impl Into<String>) -> Result<OutputPort<T, D>> {
        let name = name.into();
        let collector = Arc::new(Mutex::new(Vec::new()));
        let op_idx;
        let capacity = self.resolve_capacity();
        let prealloc = self.state.borrow().channel_preallocate;

        {
            let mut state = self.state.borrow_mut();

            // Validate unique name
            if state.output_ports.iter().any(|p| p.name == name) {
                return Err(Error::Dataflow(DataflowError::InvalidConfig(format!(
                    "duplicate output port name: {name}"
                ))));
            }

            op_idx = state.allocate_operator_index();
            let stage_id = StageId::new(0);

            // Register sink operator in graph (1 input, 0 outputs)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 1, 0,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));

            // Subgraph builder: sink has 1 input, 0 outputs, no connectivity
            if let Err(e) = state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                0,
                PortConnectivity::new(1, 0),
            ) {
                state.builder_errors.push(e);
            }
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
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "sink".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "sink".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    Ok(Box::new(CollectingSink::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        input_puller,
                        collector_clone,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Store a wiring closure that creates a ChannelSinkOperator
            // replacement factory during spawn(). This replaces the
            // CollectingSink factory above with one that sends data out.
            let sink_name = name.clone();
            #[allow(unused_variables)]
            let wiring: OutputPortWiring = Box::new(move |mode, wake_handle| {
                use crate::dataflow::channel_operators::{
                    ChannelSinkOperator, OutputReceiver, OutputSend,
                };

                match mode {
                    ChannelMode::Sync => {
                        let (tx, rx) = std::sync::mpsc::sync_channel::<OutputEvent<T, D>>(256);
                        let receiver = OutputReceiver::new(rx);

                        let sink_name_inner = sink_name.clone();
                        let factory: OperatorFactory =
                            single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                                    .input_pullers
                                    .into_iter()
                                    .next()
                                    .ok_or_else(|| {
                                        Error::Dataflow(DataflowError::MissingEndpoint {
                                            operator: "sink".into(),
                                            port: "input puller".into(),
                                        })
                                    })?
                                    .downcast::<Box<dyn Pull<T, D>>>()
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "sink".into(),
                                            port: "input puller".into(),
                                        })
                                    })?;

                                Ok(Box::new(ChannelSinkOperator::new(
                                    sink_name_inner,
                                    op_idx,
                                    StageId::new(0),
                                    input_puller,
                                    OutputSend::Std(tx),
                                ))
                                    as Box<dyn SchedulableOperator>)
                            });

                        let receiver_any: Box<dyn std::any::Any + Send> = Box::new(receiver);
                        (factory, receiver_any)
                    }
                    ChannelMode::Async => {
                        use crate::dataflow::channel_operators::AsyncOutputReceiver;

                        let (tx, rx) = tokio::sync::mpsc::channel::<OutputEvent<T, D>>(256);
                        let receiver = if let Some(wh) = wake_handle {
                            AsyncOutputReceiver::with_wake_handle(rx, wh)
                        } else {
                            AsyncOutputReceiver::new(rx)
                        };

                        let sink_name_inner = sink_name.clone();
                        let factory: OperatorFactory =
                            single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                                let input_puller: Box<dyn Pull<T, D>> = *endpoints
                                    .input_pullers
                                    .into_iter()
                                    .next()
                                    .ok_or_else(|| {
                                        Error::Dataflow(DataflowError::MissingEndpoint {
                                            operator: "sink".into(),
                                            port: "input puller".into(),
                                        })
                                    })?
                                    .downcast::<Box<dyn Pull<T, D>>>()
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "sink".into(),
                                            port: "input puller".into(),
                                        })
                                    })?;

                                Ok(Box::new(ChannelSinkOperator::new(
                                    sink_name_inner,
                                    op_idx,
                                    StageId::new(0),
                                    input_puller,
                                    OutputSend::Tokio(tx),
                                ))
                                    as Box<dyn SchedulableOperator>)
                            });

                        let receiver_any: Box<dyn std::any::Any + Send> = Box::new(receiver);
                        (factory, receiver_any)
                    }
                }
            });
            state.output_port_wiring.push(wiring);

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Ok(OutputPort {
            name,
            collector,
            _phantom: PhantomData,
        })
    }

    /// Convenience: collect output into a shared vector (for testing).
    ///
    /// Each call generates a unique internal output port name.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let input = builder.source("nums", vec![(0u64, vec![1, 2, 3])]);
    /// let results = input.map("double", |_t, x| x * 2).collect();
    /// // After execution:
    /// let data = results.lock().unwrap();
    /// for (time, batch) in data.iter() {
    ///     println!("t={time}: {batch:?}");
    /// }
    /// ```
    pub fn collect(self) -> Arc<Mutex<Vec<(T, Vec<D>)>>> {
        let name = {
            let mut state = self.state.borrow_mut();
            let idx = state.next_collect_index;
            state.next_collect_index += 1;
            format!("__collect_{idx}")
        };
        // SAFETY: collect output port was just created by add_output_port above
        self.output(name)
            .expect("internal collect output should be valid")
            .collector
    }
}

// ---------------------------------------------------------------------------
// Result combinator methods — convenience for Pipe<T, Result<V, E>>
// ---------------------------------------------------------------------------

/// Combinators for streams carrying `Result<V, E>` data.
///
/// These methods provide ergonomic, zero-boilerplate handling of fallible
/// pipelines. Instead of manually matching `Ok`/`Err` in every `map` or
/// `filter` closure, use these combinators to operate on the success path
/// while automatically relaying errors downstream.
impl<T, V, E> Pipe<T, std::result::Result<V, E>>
where
    T: Timestamp,
    V: Clone + Send + 'static,
    E: Clone + Send + 'static,
{
    /// Apply a transformation to `Ok` values, passing `Err` values through unchanged.
    ///
    /// This is the `Result`-aware version of [`map`](Pipe::map). The closure
    /// receives each `Ok(v)` and produces a new value; `Err(e)` records are
    /// forwarded without invoking the closure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results: Pipe<u64, Result<i32, String>> = source.map_ok("double", |_t, v| v * 2);
    /// // Ok(5) → Ok(10), Err("bad") → Err("bad")
    /// ```
    pub fn map_ok<V2, F>(
        self,
        name: impl Into<String>,
        mut f: F,
    ) -> Pipe<T, std::result::Result<V2, E>>
    where
        V2: Clone + Send + 'static,
        F: FnMut(&T, V) -> V2 + Send + 'static,
    {
        self.map(name, move |t, item| match item {
            Ok(v) => Ok(f(t, v)),
            Err(e) => Err(e),
        })
    }

    /// Filter `Ok` values by a predicate, passing `Err` values through unchanged.
    ///
    /// `Ok(v)` records where the predicate returns `false` are dropped.
    /// `Err(e)` records are always forwarded regardless of the predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let positive: Pipe<u64, Result<i32, String>> =
    ///     source.filter_ok("positive", |_t, v| *v > 0);
    /// // Ok(5) → Ok(5), Ok(-1) → dropped, Err("x") → Err("x")
    /// ```
    pub fn filter_ok<F>(
        self,
        name: impl Into<String>,
        mut predicate: F,
    ) -> Pipe<T, std::result::Result<V, E>>
    where
        F: FnMut(&T, &V) -> bool + Send + 'static,
    {
        self.filter(name, move |t, item| match item {
            Ok(v) => predicate(t, v),
            Err(_) => true,
        })
    }

    /// Apply a fallible transformation to `Ok` values.
    ///
    /// The closure returns `Result<V2, E>`. If the input is `Ok(v)`, the
    /// closure's result is forwarded. If the input is `Err(e)`, it passes
    /// through without invoking the closure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let parsed: Pipe<u64, Result<i32, String>> =
    ///     strings.and_then("parse", |_t, s: String| s.parse::<i32>().map_err(|e| e.to_string()));
    /// ```
    pub fn and_then<V2, F>(
        self,
        name: impl Into<String>,
        mut f: F,
    ) -> Pipe<T, std::result::Result<V2, E>>
    where
        V2: Clone + Send + 'static,
        F: FnMut(&T, V) -> std::result::Result<V2, E> + Send + 'static,
    {
        self.map(name, move |t, item| match item {
            Ok(v) => f(t, v),
            Err(e) => Err(e),
        })
    }

    /// Split a `Result` stream into separate `Ok` and `Err` streams.
    ///
    /// Returns `(ok_pipe, err_pipe)` where:
    /// - `ok_pipe` carries only the unwrapped `V` values from `Ok(v)`
    /// - `err_pipe` carries only the unwrapped `E` values from `Err(e)`
    ///
    /// This is useful for routing errors to a side channel (logging, dead
    /// letter queue) while continuing to process successes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (good, bad) = results.branch_result("split");
    /// good.map("process", |_t, v| v * 2).output("results");
    /// bad.map("log_error", |_t, e| format!("ERROR: {e}")).output("errors");
    /// ```
    pub fn branch_result(self, name: impl Into<String>) -> (Pipe<T, V>, Pipe<T, E>) {
        let name = name.into();
        let ok_name = format!("{name}::ok");
        let err_name = format!("{name}::err");

        // Use the existing map + filter pattern to split:
        // Clone self so we can derive two downstream pipes.
        let ok_pipe = self.clone().flat_map(ok_name, |_t, item| match item {
            Ok(v) => vec![v],
            Err(_) => vec![],
        });
        let err_pipe = self.flat_map(err_name, |_t, item| match item {
            Ok(_) => vec![],
            Err(e) => vec![e],
        });

        (ok_pipe, err_pipe)
    }
}

/// Exchange methods that support both local and network-backed transport.
///
/// When the `transport` feature is enabled, exchange data types must implement
/// [`ExchangeData`](crate::communication::codec::ExchangeData) to support
/// potential network serialization in `spawn_cluster`. Common types (`i32`,
/// `u64`, `String`, tuples of ExchangeData, etc.) already implement this trait.
#[cfg(feature = "transport")]
impl<
    T: Timestamp + crate::communication::codec::ExchangeData,
    D: Clone + crate::communication::codec::ExchangeData,
> Pipe<T, D>
{
    /// Repartition data across workers based on a hash function.
    ///
    /// Records with the same hash are routed to the same worker, enabling
    /// key-partitioned computations like group-by and join.
    ///
    /// In single-worker mode, this is a pass-through (no routing needed).
    /// In multi-worker mode (`spawn_multi` / `spawn_cluster`), the runtime
    /// creates shared exchange channels that route data between workers.
    ///
    /// # Example
    /// ```ignore
    /// let partitioned = stream.exchange("by_key", |record: &(u64, String)| record.0);
    /// ```
    pub fn exchange<K: std::hash::Hash + 'static>(
        mut self,
        name: impl Into<String>,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::by_key(name.into(), key_fn);
        self.add_exchange_internal_networked(exchange_fn, capacity)
    }

    /// Repartition data with explicit downstream parallelism (cluster-wide total).
    ///
    /// Like [`exchange`](Self::exchange), but specifies the target stage's
    /// parallelism. This creates a stage boundary where the downstream operators
    /// run with `target_parallelism` workers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Route to 8 workers based on user_id
    /// let partitioned = stream.exchange_to("by_user", 8, |record| record.user_id);
    /// ```
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    pub fn exchange_to<K: std::hash::Hash + 'static>(
        mut self,
        name: impl Into<String>,
        target_parallelism: usize,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::by_key(name.into(), key_fn);
        let pipe = self.add_exchange_internal_networked(exchange_fn, capacity);
        // Record target parallelism on the exchange operator we just created.
        let exchange_op_idx = pipe.op_idx;
        pipe.state
            .borrow_mut()
            .graph
            .set_exchange_parallelism(exchange_op_idx, target_parallelism);
        Ok(pipe)
    }

    /// Repartition data using a direct hash function (returns u64).
    ///
    /// The returned u64 is reduced modulo the target worker count to
    /// determine routing.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let partitioned = stream.exchange_by_hash("by_id", |record| record.id as u64);
    /// ```
    pub fn exchange_by_hash(
        mut self,
        name: impl Into<String>,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::new(name, hash_fn);
        self.add_exchange_internal_networked(exchange_fn, capacity)
    }

    /// Repartition with direct hash and explicit downstream parallelism.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let partitioned = stream.exchange_by_hash_to("by_id", 4, |record| record.id as u64);
    /// ```
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    pub fn exchange_by_hash_to(
        mut self,
        name: impl Into<String>,
        target_parallelism: usize,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::new(name, hash_fn);
        let pipe = self.add_exchange_internal_networked(exchange_fn, capacity);
        let exchange_op_idx = pipe.op_idx;
        pipe.state
            .borrow_mut()
            .graph
            .set_exchange_parallelism(exchange_op_idx, target_parallelism);
        Ok(pipe)
    }

    /// Internal: add an exchange operator with network-capable factory creator.
    fn add_exchange_internal_networked(
        &self,
        exchange_fn: crate::dataflow::channels::pact::ExchangeFn<D>,
        capacity: usize,
    ) -> Pipe<T, D> {
        let pipe = self.add_exchange_internal(exchange_fn.clone(), capacity);

        // Store network exchange creator alongside the local one.
        {
            let mut state = self.state.borrow_mut();
            let edge_idx = state.graph.edges().len() - 1;
            let creator: Box<
                dyn crate::dataflow::channels::exchange_channel::NetworkExchangeCreator,
            > = Box::new(
                crate::dataflow::channels::exchange_channel::NetworkExchangeCreatorImpl::<T, D> {
                    exchange_fn,
                    _phantom: std::marker::PhantomData,
                },
            );
            state
                .exchange_network_creators
                .push((edge_idx, capacity, creator));
        }

        pipe
    }

    /// Route all data to worker 0 (global aggregation pattern).
    ///
    /// Equivalent to `exchange_by_hash(name, |_| 0)` — every item is sent
    /// to the same worker regardless of content. Use before global reduce/fold
    /// when you need a single aggregated result.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let global_sum = stream
    ///     .gather("collect-all")
    ///     .reduce("global-sum", |acc, x| acc + x);
    /// ```
    pub fn gather(self, name: impl Into<String>) -> Pipe<T, D> {
        self.exchange_by_hash(name, |_| 0u64)
    }

    /// Distribute data round-robin across workers for load balancing.
    ///
    /// Each item is sent to the next worker in sequence (modulo worker count),
    /// providing even distribution regardless of data content. Use when items
    /// are independent and you want uniform load across workers.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let balanced = stream
    ///     .rebalance("spread-load")
    ///     .map("process", |_t, x| expensive_computation(x));
    /// ```
    pub fn rebalance(self, name: impl Into<String>) -> Pipe<T, D> {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.exchange_by_hash(name, move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Distribute data round-robin with explicit downstream parallelism.
    ///
    /// Like [`rebalance`](Self::rebalance), but specifies the target stage's
    /// worker count. Use when you want to scale up or down the number of
    /// workers processing subsequent operators.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    ///
    /// # Example
    /// ```ignore
    /// let scaled = stream
    ///     .rebalance_to("fan-out", 8)
    ///     .map("process", |_t, x| expensive_computation(x));
    /// ```
    pub fn rebalance_to(
        self,
        name: impl Into<String>,
        target_parallelism: usize,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.exchange_by_hash_to(name, target_parallelism, move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Broadcast all data to every worker (fan-out).
    ///
    /// Each item is cloned and sent to ALL workers in the dataflow. This is
    /// useful for distributing small reference data, configuration, or lookup
    /// tables that every worker needs a complete copy of.
    ///
    /// **Warning:** Broadcast multiplies data volume by the worker count. Only
    /// use for small datasets or control signals, not for large data streams.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let config = config_stream.broadcast("share-config");
    /// // Every worker now has a copy of all config items.
    /// ```
    pub fn broadcast(mut self, name: impl Into<String>) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        self.add_broadcast_internal_networked(name, capacity)
    }

    fn add_broadcast_internal_networked(
        &self,
        name: impl Into<String>,
        capacity: usize,
    ) -> Pipe<T, D> {
        let pipe = self.add_broadcast_internal(name, capacity);

        // Store network broadcast creator alongside the local one.
        {
            let mut state = self.state.borrow_mut();
            let edge_idx = state.graph.edges().len() - 1;
            let creator: Box<
                dyn crate::dataflow::channels::exchange_channel::NetworkExchangeCreator,
            > = Box::new(
                crate::dataflow::channels::exchange_channel::NetworkBroadcastCreatorImpl::<T, D> {
                    _phantom: std::marker::PhantomData,
                },
            );
            state
                .exchange_network_creators
                .push((edge_idx, capacity, creator));
        }

        pipe
    }
}
#[cfg(not(feature = "transport"))]
impl<T: Timestamp, D: Clone + Send + 'static> Pipe<T, D> {
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
        mut self,
        name: impl Into<String>,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::by_key(&name.into(), key_fn);
        self.add_exchange_internal(exchange_fn, capacity)
    }

    /// Repartition data with explicit downstream parallelism (cluster-wide total).
    ///
    /// Like [`exchange`](Self::exchange), but specifies the target stage's
    /// parallelism. This creates a stage boundary where the downstream operators
    /// run with `target_parallelism` workers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Route to 8 workers based on user_id
    /// let partitioned = stream.exchange_to("by_user", 8, |record| record.user_id);
    /// ```
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    pub fn exchange_to<K: std::hash::Hash + 'static>(
        mut self,
        name: impl Into<String>,
        target_parallelism: usize,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::by_key(&name.into(), key_fn);
        let pipe = self.add_exchange_internal(exchange_fn, capacity);
        let exchange_op_idx = pipe.op_idx;
        pipe.state
            .borrow_mut()
            .graph
            .set_exchange_parallelism(exchange_op_idx, target_parallelism);
        Ok(pipe)
    }

    /// Repartition data using a direct hash function (returns u64).
    ///
    /// The returned u64 is reduced modulo the target worker count to
    /// determine routing.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let partitioned = stream.exchange_by_hash("by_id", |record| record.id as u64);
    /// ```
    pub fn exchange_by_hash(
        mut self,
        name: impl Into<String>,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::new(name, hash_fn);
        self.add_exchange_internal(exchange_fn, capacity)
    }

    /// Repartition with direct hash and explicit downstream parallelism.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let partitioned = stream.exchange_by_hash_to("by_id", 4, |record| record.id as u64);
    /// ```
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    pub fn exchange_by_hash_to(
        mut self,
        name: impl Into<String>,
        target_parallelism: usize,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let capacity = self.resolve_capacity();
        let exchange_fn = crate::dataflow::channels::pact::ExchangeFn::new(name, hash_fn);
        let pipe = self.add_exchange_internal(exchange_fn, capacity);
        let exchange_op_idx = pipe.op_idx;
        pipe.state
            .borrow_mut()
            .graph
            .set_exchange_parallelism(exchange_op_idx, target_parallelism);
        Ok(pipe)
    }

    /// Route all data to worker 0 (global aggregation pattern).
    ///
    /// Equivalent to `exchange_by_hash(name, |_| 0)` — every item is sent
    /// to the same worker regardless of content. Use before global reduce/fold
    /// when you need a single aggregated result.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let global_sum = stream
    ///     .gather("collect-all")
    ///     .reduce("global-sum", |acc, x| acc + x);
    /// ```
    pub fn gather(self, name: impl Into<String>) -> Pipe<T, D> {
        self.exchange_by_hash(name, |_| 0u64)
    }

    /// Distribute data round-robin across workers for load balancing.
    ///
    /// Each item is sent to the next worker in sequence (modulo worker count),
    /// providing even distribution regardless of data content. Use when items
    /// are independent and you want uniform load across workers.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let balanced = stream
    ///     .rebalance("spread-load")
    ///     .map("process", |_t, x| expensive_computation(x));
    /// ```
    pub fn rebalance(self, name: impl Into<String>) -> Pipe<T, D> {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.exchange_by_hash(name, move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Distribute data round-robin with explicit downstream parallelism.
    ///
    /// Like [`rebalance`](Self::rebalance), but specifies the target stage's
    /// worker count. Use when you want to scale up or down the number of
    /// workers processing subsequent operators.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    ///
    /// # Example
    /// ```ignore
    /// let scaled = stream
    ///     .rebalance_to("fan-out", 8)
    ///     .map("process", |_t, x| expensive_computation(x));
    /// ```
    pub fn rebalance_to(
        self,
        name: impl Into<String>,
        target_parallelism: usize,
    ) -> Result<Pipe<T, D>> {
        if target_parallelism == 0 {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        self.exchange_by_hash_to(name, target_parallelism, move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Broadcast all data to every worker (fan-out).
    ///
    /// Each item is cloned and sent to ALL workers in the dataflow. This is
    /// useful for distributing small reference data, configuration, or lookup
    /// tables that every worker needs a complete copy of.
    ///
    /// **Warning:** Broadcast multiplies data volume by the worker count. Only
    /// use for small datasets or control signals, not for large data streams.
    ///
    /// In single-worker mode, this is a no-op pass-through.
    ///
    /// # Example
    /// ```ignore
    /// let config = config_stream.broadcast("share-config");
    /// // Every worker now has a copy of all config items.
    /// ```
    pub fn broadcast(mut self, name: impl Into<String>) -> Pipe<T, D> {
        let capacity = self.resolve_capacity();
        self.add_broadcast_internal(name, capacity)
    }
}

/// Internal exchange implementation— shared by both transport and non-transport builds.
impl<T: Timestamp, D: Clone + Send + 'static> Pipe<T, D> {
    /// Internal: add an exchange (repartition) operator.
    ///
    /// Creates a pass-through unary operator with an exchange channel
    /// on its input edge. For multi-worker execution, `spawn_multi`
    /// replaces the placeholder pipeline factory with shared exchange
    /// channel factories.
    fn add_exchange_internal(
        &self,
        exchange_fn: crate::dataflow::channels::pact::ExchangeFn<D>,
        capacity: usize,
    ) -> Pipe<T, D> {
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;

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
                    op_idx, "exchange", stage_id, 1, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream — marked as Exchange.
            state
                .graph
                .add_edge(crate::dataflow::graph::EdgeInfo::exchange(
                    Slot::new(self.op_idx, self.output_slot),
                    Slot::new(op_idx, 0),
                    stage_id,
                    stage_id,
                ));

            // Subgraph connectivity.
            // Register with an initial capability at T::minimum(). This prevents
            // the progress tracker from declaring the dataflow complete while
            // data may be in transit on the exchange channel (especially across
            // network boundaries where data delivery is asynchronous).
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let exchange_reporter = match state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                "exchange",
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
                vec![initial_cap],
            ) {
                Ok(progress) => progress.reporter(0).clone(),
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — pass-through unary with progress reporter.
            let name_clone = String::from("exchange");
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "exchange".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "exchange".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "exchange".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredUnaryOperator::with_reporter(
                        name_clone,
                        op_idx,
                        stage_id,
                        wired_logic,
                        input_puller,
                        output_pusher,
                        exchange_reporter,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory — pipeline placeholder for single-worker.
            // spawn_multi replaces this with shared exchange factories.
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));

            // Store exchange factory creator for multi-worker.
            let creator =
                crate::dataflow::channels::exchange_channel::create_exchange_factory_creator::<T, D>(
                    exchange_fn,
                );
            state.exchange_creators.push((edge_idx, capacity, creator));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }

    /// Internal: add a broadcast (fan-out to all workers) operator.
    ///
    /// Like `add_exchange_internal` but uses `BroadcastPush` which clones
    /// each item to ALL target workers instead of routing to one.
    fn add_broadcast_internal(&self, name: impl Into<String>, capacity: usize) -> Pipe<T, D> {
        let _name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;

        // Identity pass-through logic (same as exchange).
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

            // Register operator as "broadcast" in the graph.
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx,
                    "broadcast",
                    stage_id,
                    1,
                    1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream — marked as Exchange (broadcast uses same infrastructure).
            state
                .graph
                .add_edge(crate::dataflow::graph::EdgeInfo::exchange(
                    Slot::new(self.op_idx, self.output_slot),
                    Slot::new(op_idx, 0),
                    stage_id,
                    stage_id,
                ));

            // Subgraph connectivity with initial capability.
            let mut initial_cap = ChangeBatch::new();
            initial_cap.update(T::minimum(), 1);
            let exchange_reporter = match state.subgraph_builder.add_operator_with_capabilities(
                op_idx,
                "broadcast",
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
                vec![initial_cap],
            ) {
                Ok(progress) => progress.reporter(0).clone(),
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — same pass-through unary as exchange.
            let name_clone = String::from("broadcast");
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "broadcast".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "broadcast".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D>> = {
                        let pushers: Vec<Box<dyn Push<T, D>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "broadcast".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredUnaryOperator::with_reporter(
                        name_clone,
                        op_idx,
                        stage_id,
                        wired_logic,
                        input_puller,
                        output_pusher,
                        exchange_reporter,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory — pipeline placeholder for single-worker.
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));

            // Store broadcast factory creator for multi-worker.
            let creator =
                crate::dataflow::channels::exchange_channel::create_broadcast_factory_creator::<T, D>(
                );
            state.exchange_creators.push((edge_idx, capacity, creator));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }
    ///
    /// Returns `(Pipe, ProbeHandle)` — the Pipe continues unchanged,
    /// and the probe can be queried after execution.
    ///
    /// The returned `ProbeHandle` supports async waiting via
    /// [`wait_until_done_with`](ProbeHandle::wait_until_done_with) and
    /// [`wait_until_done`](ProbeHandle::wait_until_done).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (stream, probe) = stream.probe();
    /// stream.output("results");
    /// // After execution:
    /// assert!(probe.is_done());
    /// ```
    pub fn probe(self) -> (Self, ProbeHandle<T>) {
        let (probe, notifier) = ProbeHandle::new();
        {
            let mut state = self.state.borrow_mut();
            state.probes.push((self.op_idx, probe.clone()));
            state.probe_notifiers.push(notifier);
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
        capacity: usize,
        logic: impl FnMut(T, Vec<D>) -> Vec<D2> + Send + 'static,
    ) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
    {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;

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
                    op_idx, &name, stage_id, 1, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));

            // Subgraph: identity connectivity (timestamps pass through unchanged)
            if let Err(e) = state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — handles fan-out by wrapping multiple output
            // pushers in a TeePush adapter when the Pipe was cloned.
            let name_clone = name.clone();
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "unary".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "unary".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D2>> = {
                        let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D2>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "unary".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredUnaryOperator::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        wired_logic,
                        input_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }

    /// Internal: add a unary operator using InputHandle/OutputHandle API.
    fn add_unary_with_handles<D2, L>(
        &self,
        name: impl Into<String>,
        capacity: usize,
        logic: L,
    ) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(&mut InputHandle<T, D>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
    {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (1 input, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 1, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));

            // Subgraph: identity connectivity
            if let Err(e) = state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — handles fan-out via TeePush
            let name_clone = name.clone();
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "unary".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "unary".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D2>> = {
                        let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D2>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "unary".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    Ok(Box::new(WiredUnaryOperator::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        logic,
                        input_puller,
                        output_pusher,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }

    /// Internal implementation for `unary_notify`.
    ///
    /// This is nearly identical to `add_unary_with_handles`, but:
    /// 1. Captures a `ProgressReporter<T>` from the subgraph builder's per-operator
    ///    progress buffers. This reporter is shared with the `ProgressTracker` and
    ///    enables the operator's `NotifyContext` to create output capabilities.
    /// 2. Creates a `WiredUnaryNotifyOperator` instead of `WiredUnaryOperator`.
    ///    The notify variant owns a `Notificator<T>` and manages held capabilities
    ///    to prevent premature downstream frontier advancement while data is buffered.
    fn add_unary_notify_internal<D2, L>(
        &self,
        name: impl Into<String>,
        capacity: usize,
        logic: L,
    ) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
        L: FnMut(
                &mut InputHandle<T, D>,
                &mut OutputHandle<T, D2>,
                &mut NotifyContext<'_, T>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;

        {
            let mut state = self.state.borrow_mut();
            op_idx = state.allocate_operator_index();

            // Register in graph (1 input, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 1, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));

            // Subgraph: identity connectivity (1 input → 1 output, default summary).
            let progress_reporter = match state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                Ok(progress) => {
                    // Clone the ProgressReporter for output port 0.
                    // This reporter is shared with the ProgressTracker — the tracker drains
                    // changes from it during `collect_operator_progress()`. The operator uses
                    // it via `NotifyContext::notify_at()` to create Capability<T> objects that
                    // write +1/-1 to this reporter, which the tracker sees as pointstamp
                    // changes that hold downstream frontiers.
                    progress.reporter(0).clone()
                }
                Err(e) => {
                    state.builder_errors.push(e);
                    ProgressReporter::default()
                }
            };

            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — creates a WiredUnaryNotifyOperator with the
            // captured progress reporter. The reporter is moved into the closure
            // and then into the operator; since ProgressReporter is Arc-based,
            // the ProgressTracker and operator share the same underlying buffer.
            let name_clone = name.clone();
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "unary_notify".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "unary_notify".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D2>> = {
                        let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D2>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "unary_notify".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    // The initial frontier is [T::minimum()] — the operator starts with
                    // the assumption that all timestamps are possible. The executor will
                    // call update_input_frontier() after the first progress propagation
                    // to set the actual frontier.
                    let initial_frontier = Antichain::from_elem(T::minimum());

                    Ok(Box::new(WiredUnaryNotifyOperator::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        logic,
                        input_puller,
                        output_pusher,
                        progress_reporter,
                        initial_frontier,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
            _phantom: PhantomData,
        }
    }

    /// Internal implementation for `unary_async`.
    fn add_unary_async_internal<D2>(
        &self,
        name: impl Into<String>,
        capacity: usize,
        logic: AsyncLogicFn<T, D, D2>,
        max_concurrency: usize,
    ) -> Pipe<T, D2>
    where
        D2: Clone + Send + 'static,
    {
        let name = name.into();
        let op_idx;
        let stage_id = StageId::new(0);
        let prealloc = self.state.borrow().channel_preallocate;
        // Capture the tokio handle at builder time. This requires that the caller
        // is within a tokio runtime context (e.g., inside #[tokio::main] or an async task).
        let tokio_handle = tokio::runtime::Handle::try_current();

        {
            let mut state = self.state.borrow_mut();

            if let Err(ref e) = tokio_handle {
                state
                    .builder_errors
                    .push(Error::Dataflow(DataflowError::InvalidConfig(format!(
                        "unary_async requires a tokio runtime context: {e}. \
                     Call from within #[tokio::main], #[tokio::test], or similar."
                    ))));
            }

            op_idx = state.allocate_operator_index();

            // Register in graph (1 input, 1 output)
            state
                .graph
                .register_operator(crate::dataflow::graph::OperatorInfo::new(
                    op_idx, &name, stage_id, 1, 1,
                ))
                .unwrap_or_else(|e| state.builder_errors.push(e));

            // Edge from upstream
            state.graph.add_edge(crate::dataflow::graph::EdgeInfo::new(
                Slot::new(self.op_idx, self.output_slot),
                Slot::new(op_idx, 0),
                stage_id,
                stage_id,
            ));

            // Subgraph: identity connectivity
            if let Err(e) = state.subgraph_builder.add_operator(
                op_idx,
                &name,
                1,
                1,
                PortConnectivity::identity(T::Summary::default()),
            ) {
                state.builder_errors.push(e);
            }
            state.subgraph_builder.add_edge(
                Location::source(self.op_idx, self.output_slot),
                Location::target(op_idx, 0),
            );

            // Operator factory — creates a WiredUnaryAsyncOperator
            let name_clone = name.clone();
            let factory: OperatorFactory =
                single_use_factory(move |_ctx, endpoints: ChannelEndpoints| {
                    let input_puller: Box<dyn Pull<T, D>> = *endpoints
                        .input_pullers
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::MissingEndpoint {
                                operator: "unary_async".into(),
                                port: "input puller".into(),
                            })
                        })?
                        .downcast::<Box<dyn Pull<T, D>>>()
                        .map_err(|_| {
                            Error::Dataflow(DataflowError::TypeMismatch {
                                operator: "unary_async".into(),
                                port: "input puller".into(),
                            })
                        })?;

                    let output_pusher: Box<dyn Push<T, D2>> = {
                        let pushers: Vec<Box<dyn Push<T, D2>>> = endpoints
                            .output_pushers
                            .into_iter()
                            .map(|any_box| {
                                any_box
                                    .downcast::<Box<dyn Push<T, D2>>>()
                                    .map(|boxed| *boxed)
                                    .map_err(|_| {
                                        Error::Dataflow(DataflowError::TypeMismatch {
                                            operator: "unary_async".into(),
                                            port: "output pusher".into(),
                                        })
                                    })
                            })
                            .collect::<Result<_>>()?;
                        tee_or_single(pushers)?.unwrap_or_else(|| Box::new(NullPush))
                    };

                    // tokio_handle is Ok if we had a runtime context at build time.
                    // If Err, build() will have returned the error before this factory
                    // runs. We use `?` as defense-in-depth to avoid panicking even in
                    // edge cases (e.g., if a future refactor changes the error-check order).
                    let handle = tokio_handle.map_err(|e| {
                        Error::Dataflow(DataflowError::InvalidConfig(format!(
                            "tokio runtime context missing at factory invocation: {e}"
                        )))
                    })?;
                    Ok(Box::new(WiredUnaryAsyncOperator::new(
                        name_clone,
                        op_idx,
                        stage_id,
                        logic,
                        max_concurrency,
                        input_puller,
                        output_pusher,
                        handle,
                        endpoints.wake_handle,
                    )) as Box<dyn SchedulableOperator>)
                });
            state.operator_factories.push((op_idx, factory));

            // Channel factory for the input edge
            let edge_idx = state.graph.edges().len() - 1;
            let chan_factory: ChannelFactory =
                channel_factory(move |_ctx, wake: Option<WakeHandle>| {
                    let (push, pull) =
                        bounded_channel_with_wake::<T, D, ()>(capacity, wake.clone(), prealloc);
                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                });
            state.channel_factories.push((edge_idx, chan_factory));
        }

        Pipe {
            state: Rc::clone(&self.state),
            op_idx,
            output_slot: 0,
            capacity_override: None,
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
/// - `SimpleRuntime` (feature `test-utils`) — single-thread, for tests and simple scripts
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
    /// Async source pump wiring — one per `source_async()` call.
    /// Consumed at spawn time to create pump tasks.
    /// Tuple: (operator_index, wiring_closure).
    pub(crate) async_source_wiring: Vec<(usize, AsyncSourceWiring)>,
    pub(crate) probes: Vec<(usize, ProbeHandle<T>)>,
    pub(crate) probe_notifiers: Vec<crate::dataflow::probe::ProbeNotifier<T>>,
    /// Type-erased exchange factory creators — one per exchange edge.
    /// Consumed by `spawn_multi` to produce shared cross-worker channel factories.
    /// Tuple: (edge_index, capacity, creator_fn).
    pub(crate) exchange_creators: Vec<(
        usize,
        usize,
        crate::dataflow::channels::exchange_channel::ExchangeFactoryCreatorFn,
    )>,
    /// Network-capable exchange creators — one per exchange edge (transport feature only).
    /// Consumed by `spawn_cluster` to produce network-backed exchange channel factories.
    /// Tuple: (edge_index, capacity, creator).
    #[cfg(feature = "transport")]
    pub(crate) exchange_network_creators: Vec<(
        usize,
        usize,
        Box<dyn crate::dataflow::channels::exchange_channel::NetworkExchangeCreator>,
    )>,
    /// User-supplied typed context values, carried from the builder.
    /// Available at materialization time and for future operator-level access.
    pub(crate) contexts: SharedContext,
    /// Auto-inferred stage metadata. Each stage groups operators connected
    /// by pipeline edges, with exchange edges forming stage boundaries.
    pub(crate) stages: Vec<crate::dataflow::stage::StageInfo>,
    /// Whether to catch panics in operator activation (see [`ExecutorConfig::catch_panics`]).
    pub(crate) catch_panics: bool,
    /// Whether to collect per-operator metrics (see [`ExecutorConfig::collect_metrics`]).
    /// Set by SpawnOptions at spawn time.
    pub(crate) collect_metrics: bool,
    /// Graceful drain timeout (see [`ExecutorConfig::drain_timeout`]).
    /// Set by SpawnOptions at spawn time.
    pub(crate) drain_timeout: Option<std::time::Duration>,
}

impl<T: Timestamp> LogicalDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Access the shared context values attached during graph construction.
    ///
    /// This allows code that has access to the `LogicalDataflow` (e.g., custom
    /// materialization logic) to retrieve context values set via
    /// [`DataflowBuilder::with_context`].
    pub fn contexts(&self) -> &SharedContext {
        &self.contexts
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

    /// Get the indices of exchange edges.
    ///
    /// These are the edge indices that use cross-worker exchange channels
    /// (as opposed to pipeline channels).
    pub fn exchange_edge_indices(&self) -> Vec<usize> {
        self.exchange_creators
            .iter()
            .map(|(idx, _, _)| *idx)
            .collect()
    }

    /// Get the number of feedback (loop) edges in the graph.
    pub fn feedback_edge_count(&self) -> usize {
        self.graph.feedback_edges().len()
    }

    /// Get the auto-inferred stages for this dataflow.
    ///
    /// Each stage groups operators connected by pipeline edges. Exchange edges
    /// form boundaries between stages. Stages are numbered sequentially from 0.
    pub fn stages(&self) -> &[crate::dataflow::stage::StageInfo] {
        &self.stages
    }

    /// Retain only the operators and channels for the given stages.
    ///
    /// Removes operator factories, channel factories, input/output port wiring,
    /// async source wiring, and probes for operators not in any of the specified
    /// stages. Exchange channel factories at stage boundaries are always retained
    /// (they provide cross-stage connectivity).
    ///
    /// Non-participating operators are kept as "ghost" operators in the
    /// SubgraphBuilder's reachability graph. This allows cross-stage frontier
    /// propagation: peer workers broadcast capability changes for ghost
    /// operators, and the local reachability graph propagates those changes
    /// through exchange edges to downstream materialized operators.
    ///
    /// This is used for per-stage materialization: each worker only
    /// materializes operators for stages it participates in.
    pub fn retain_stages(
        &mut self,
        participating_stage_ids: &std::collections::HashSet<crate::dataflow::stage::StageId>,
    ) {
        use std::collections::HashSet;

        // Collect operator indices belonging to participating stages.
        let participating_ops: HashSet<usize> = self
            .stages
            .iter()
            .filter(|s| participating_stage_ids.contains(&s.id))
            .flat_map(|s| s.operator_indices.iter().copied())
            .collect();

        // Determine which edges to keep and build an index remapping.
        // An edge is kept if:
        // 1. It's an exchange edge touching a participating stage, OR
        // 2. Both endpoints are in participating operators (pipeline within stage).
        let old_edges = self.graph.edges();
        let mut kept_old_indices: Vec<usize> = Vec::new();
        for (idx, e) in old_edges.iter().enumerate() {
            let keep = if e.is_exchange() {
                // Keep exchange edges where at least one side participates.
                participating_stage_ids.contains(&e.source_stage)
                    || participating_stage_ids.contains(&e.target_stage)
            } else {
                // Keep pipeline edges where both operators participate.
                participating_ops.contains(&e.source.operator_index)
                    && participating_ops.contains(&e.target.operator_index)
            };
            if keep {
                kept_old_indices.push(idx);
            }
        }

        // Build old_idx → new_idx remapping for regular edges.
        let new_regular_count = kept_old_indices.len();
        let mut old_to_new: std::collections::HashMap<usize, usize> =
            std::collections::HashMap::new();
        for (new_idx, &old_idx) in kept_old_indices.iter().enumerate() {
            old_to_new.insert(old_idx, new_idx);
        }

        // Determine which feedback edges to keep and build their remapping.
        // Feedback edge channel factory indices = old_regular_count + fb_position.
        // After filtering, new indices = new_regular_count + new_fb_position.
        let old_regular_count = self.graph.edges().len();
        let old_feedback_edges = self.graph.feedback_edges();
        let mut fb_new_position = 0usize;
        for (fb_pos, fb_edge) in old_feedback_edges.iter().enumerate() {
            let both_participating = participating_ops.contains(&fb_edge.source.operator_index)
                && participating_ops.contains(&fb_edge.target.operator_index);
            if both_participating {
                let old_factory_idx = old_regular_count + fb_pos;
                let new_factory_idx = new_regular_count + fb_new_position;
                old_to_new.insert(old_factory_idx, new_factory_idx);
                fb_new_position += 1;
            }
        }

        // Rebuild graph edges (new sequential indices).
        let kept_edges_set: HashSet<usize> = kept_old_indices.iter().copied().collect();
        self.graph.retain_edges(&kept_edges_set);

        // Remap channel_factories to new edge indices.
        let mut new_channel_factories = Vec::new();
        for (old_idx, factory) in std::mem::take(&mut self.channel_factories) {
            if let Some(&new_idx) = old_to_new.get(&old_idx) {
                new_channel_factories.push((new_idx, factory));
            }
        }
        self.channel_factories = new_channel_factories;

        // Filter operator factories.
        self.operator_factories
            .retain(|(idx, _)| participating_ops.contains(idx));

        // Filter input port wiring: only keep if stage 0 is participating.
        let stage0_participating =
            participating_stage_ids.contains(&crate::dataflow::stage::StageId::new(0));

        if !stage0_participating {
            self.input_ports.clear();
            self.input_port_wiring.clear();
            self.async_source_wiring.clear();
        }

        // Filter output ports: only keep if the sink operator is participating.
        {
            let old_ports = std::mem::take(&mut self.output_ports);
            let old_wiring = std::mem::take(&mut self.output_port_wiring);
            for (port, wiring) in old_ports.into_iter().zip(old_wiring) {
                if participating_ops.contains(&port.operator_index) {
                    self.output_ports.push(port);
                    self.output_port_wiring.push(wiring);
                }
            }
        }

        // Filter probes and probe_notifiers together (paired 1:1 by position).
        {
            let old_probes = std::mem::take(&mut self.probes);
            let old_notifiers = std::mem::take(&mut self.probe_notifiers);
            for (probe, notifier) in old_probes.into_iter().zip(old_notifiers) {
                if participating_ops.contains(&probe.0) {
                    self.probes.push(probe);
                    self.probe_notifiers.push(notifier);
                }
            }
        }

        // Mark non-participating operators as "ghost" in the SubgraphBuilder.
        // Ghost operators stay in the reachability graph for cross-stage
        // frontier propagation but have no local progress buffers or
        // initial capabilities (those come from peer workers).
        {
            let ghost_ops: std::collections::HashSet<usize> = self
                .subgraph_builder
                .operator_shapes()
                .map(|s| s.index)
                .filter(|idx| !participating_ops.contains(idx))
                .collect();
            if !ghost_ops.is_empty() {
                self.subgraph_builder.mark_ghost_operators(&ghost_ops);
            }
        }

        // Filter stages to only include participating ones.
        self.stages
            .retain(|s| participating_stage_ids.contains(&s.id));

        // Remove non-participating operators from the graph (including
        // feedback edges whose endpoints are in non-participating stages).
        self.graph.retain_feedback_edges(&participating_ops);
        self.graph.retain_operators(&participating_ops);
    }
}

// ---------------------------------------------------------------------------
// CollectingSink (reused from builder.rs — same implementation)
// ---------------------------------------------------------------------------

/// A sink operator that collects received data into a shared vector.
struct CollectingSink<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    stage_id: StageId,
    input_puller: Box<dyn Pull<T, D>>,
    collector: Arc<Mutex<Vec<(T, Vec<D>)>>>,
    input_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> CollectingSink<T, D> {
    fn new(
        name: impl Into<String>,
        index: usize,
        stage_id: StageId,
        input_puller: Box<dyn Pull<T, D>>,
        collector: Arc<Mutex<Vec<(T, Vec<D>)>>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            stage_id,
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

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn close_inputs(&mut self) {
        self.input_exhausted = true;
    }
}

// ---------------------------------------------------------------------------
// ForEachSink — terminal sink that invokes a user closure on each batch
// ---------------------------------------------------------------------------

struct ForEachSink<T: Timestamp, D: Send + 'static, F: FnMut(&T, &[D]) + Send + 'static> {
    name: String,
    index: usize,
    stage_id: StageId,
    input_puller: Box<dyn Pull<T, D>>,
    logic: F,
    input_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static, F: FnMut(&T, &[D]) + Send + 'static> ForEachSink<T, D, F> {
    fn new(
        name: impl Into<String>,
        index: usize,
        stage_id: StageId,
        input_puller: Box<dyn Pull<T, D>>,
        logic: F,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            stage_id,
            input_puller,
            logic,
            input_exhausted: false,
            done: false,
        }
    }
}

impl<T: Timestamp, D: Send + 'static, F: FnMut(&T, &[D]) + Send + 'static> SchedulableOperator
    for ForEachSink<T, D, F>
{
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
                (self.logic)(&time, &data);
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

    fn stage_id(&self) -> StageId {
        self.stage_id
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
    ) -> std::result::Result<
        (),
        (
            crate::error::Error,
            crate::dataflow::channels::Envelope<T, D>,
        ),
    > {
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
        let port = stream.output("results").unwrap();
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
        let port = doubled.output("results").unwrap();
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
        let port = evens.output("results").unwrap();
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
            .output("results")
            .unwrap();
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
            .source(
                "lines",
                vec![(0u64, vec!["hello world".to_string(), "foo bar".to_string()])],
            )
            .flat_map("split", |_t, line| {
                line.split_whitespace().map(|w| w.to_string()).collect()
            })
            .output("words")
            .unwrap();
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
            .output("evens")
            .unwrap();
        let odds_port = stream
            .filter("odds", |_t, x| x % 2 != 0)
            .output("odds")
            .unwrap();

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

        let port_a = a.map("inc_a", |_t, x| x + 1).output("out_a").unwrap();
        let port_b = b.map("inc_b", |_t, x| x + 1).output("out_b").unwrap();

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
            .output("results")
            .unwrap();
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
            .output("results")
            .unwrap();
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
                vec![(0u64, vec![1i32, 2]), (1u64, vec![3, 4]), (2u64, vec![5])],
            )
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
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
        let collector = builder.source("nums", vec![(0u64, vec![42i32])]).collect();
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
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        let result = SimpleRuntime::with_cancel(cancel).run(dataflow);
        assert!(result.is_err());
    }

    #[test]
    fn test_logical_dataflow_metadata() {
        let builder = DataflowBuilder::<u64>::new("meta_test");
        let _a = builder.source::<i32>("src_a", vec![]);
        let b = builder.source::<i32>("src_b", vec![]);
        let _port = b.map("transform", |_t, x| x).output("out").unwrap();
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
        let _p1 = s1.output("results").unwrap();
        let _p2 = s2.output("results").unwrap(); // should panic: duplicate name
    }

    #[test]
    fn test_probe() {
        let builder = DataflowBuilder::<u64>::new("probe_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let (stream, _probe) = stream.probe();
        let _port = stream.output("results").unwrap();
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
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let collector = port.collector();
        let results = collector.lock().unwrap();
        assert_eq!(results[0].1, vec![15]);
    }

    #[test]
    fn test_input_port_rejected_by_run() {
        let builder = DataflowBuilder::<u64>::new("input_test");
        let _port = builder
            .input::<i32>("data")
            .unwrap()
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        let result = rt().run(dataflow);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("input ports"));
    }

    #[test]
    fn test_spawn_basic_pipeline() {
        let builder = DataflowBuilder::<u64>::new("spawn_test");
        let input = builder.input::<i32>("numbers").unwrap();
        input
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
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
        let input = builder.input::<i32>("src").unwrap();
        input
            .filter("evens", |_t, x| x % 2 == 0)
            .output("evens")
            .unwrap();
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
        let input = builder.input::<i32>("numbers").unwrap();
        input.output("out").unwrap();
        let dataflow = builder.build().unwrap();

        let mut handle = rt().spawn(dataflow).unwrap();
        let result = handle.take_input::<String>("numbers");
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("type"));
    }

    #[test]
    fn test_spawn_cancel() {
        let builder = DataflowBuilder::<u64>::new("cancel_test");
        let input = builder.input::<i32>("data").unwrap();
        input.output("out").unwrap();
        let dataflow = builder.build().unwrap();

        let handle = rt().spawn(dataflow).unwrap();
        handle.cancel();
        handle.join_blocking().ok();
    }

    #[test]
    fn test_spawn_drop_cancels() {
        let builder = DataflowBuilder::<u64>::new("drop_test");
        let input = builder.input::<i32>("data").unwrap();
        input.output("out").unwrap();
        let dataflow = builder.build().unwrap();

        let _handle = rt().spawn(dataflow).unwrap();
        // SpawnedDataflow::drop cancels and joins
    }

    // --- Binary operator tests ---

    #[test]
    fn test_binary_merge_two_streams() {
        // Binary: combine two streams by pairing data at each timestamp
        let builder = DataflowBuilder::<u64>::new("binary_test");
        let names = builder.source(
            "names",
            vec![(0u64, vec!["alice".to_string(), "bob".to_string()])],
        );
        let ages = builder.source("ages", vec![(0u64, vec![30i32, 25])]);

        let port = names
            .binary::<i32, String, _>(ages, "pair", |names_in, ages_in, out| {
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
                    let pairs: Vec<String> = name_buf
                        .iter()
                        .zip(age_buf.iter())
                        .map(|(n, a)| format!("{n}={a}"))
                        .collect();
                    out.push_vec(0, pairs);
                }
                Ok(())
            })
            .unwrap()
            .output("results")
            .unwrap();

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

        let port = ints
            .binary::<String, String, _>(strs, "combine", |ints_in, strs_in, out| {
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
            })
            .unwrap()
            .output("out")
            .unwrap();

        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<&str> = r
            .iter()
            .flat_map(|(_, v)| v.iter().map(|s| s.as_str()))
            .collect();
        all.sort();
        assert_eq!(all, vec!["int:1", "int:2", "int:3", "str:a", "str:b"]);
    }

    // --- Concat tests ---

    #[test]
    fn test_concat_two_streams() {
        let builder = DataflowBuilder::<u64>::new("concat_test");
        let a = builder.source("a", vec![(0u64, vec![1i32, 2])]);
        let b = builder.source("b", vec![(0u64, vec![3i32, 4])]);

        let port = Pipe::concat(vec![a, b]).unwrap().output("merged").unwrap();
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

        let port = Pipe::concat(vec![a, b, c])
            .unwrap()
            .output("merged")
            .unwrap();
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

        let port = evens.merge(odds).unwrap().output("all").unwrap();
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
            .unwrap()
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
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

        let port = Pipe::concat(vec![a, b]).unwrap().output("merged").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut t0: Vec<i32> = r
            .iter()
            .filter(|(t, _)| *t == 0)
            .flat_map(|(_, v)| v.iter().copied())
            .collect();
        let mut t1: Vec<i32> = r
            .iter()
            .filter(|(t, _)| *t == 1)
            .flat_map(|(_, v)| v.iter().copied())
            .collect();
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
            .output("results")
            .unwrap();
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
            .output("results")
            .unwrap();
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
            .output("results")
            .unwrap();
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
                (0u64, vec![5i32]),  // 5 → 10 → 20 → 40 → 80 → 160 (5 iters)
                (1u64, vec![50i32]), // 50 → 100 (1 iter)
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
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        all.sort();
        assert_eq!(all, vec![100, 160]);
    }

    // -----------------------------------------------------------------------
    // unary_notify — frontier-based buffering + emission
    // -----------------------------------------------------------------------

    #[test]
    fn test_unary_notify_basic_passthrough() {
        // Simplest notify test: buffer data, emit on notification.
        // Single timestamp, single batch — notification fires when source closes
        // and frontier advances past t=0.
        let builder = DataflowBuilder::<u64>::new("notify_passthrough");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .unary_notify("buffer_emit", {
                let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                    std::collections::HashMap::new();
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        stash.entry(time).or_default().extend(data);
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        if let Some(data) = stash.remove(&time) {
                            output.push_vec(time, data);
                        }
                    }
                    Ok(())
                }
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_unary_notify_multiple_timestamps() {
        // Multiple timestamps — each gets its own notification.
        // Verifies that notifications fire per-timestamp as frontier advances.
        let builder = DataflowBuilder::<u64>::new("notify_multi_time");
        let port = builder
            .source(
                "data",
                vec![
                    (0u64, vec![10i32, 20]),
                    (1u64, vec![30, 40]),
                    (2u64, vec![50]),
                ],
            )
            .unary_notify("aggregate", {
                let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                    std::collections::HashMap::new();
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        stash.entry(time).or_default().extend(data);
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        if let Some(mut data) = stash.remove(&time) {
                            let sum: i32 = data.drain(..).sum();
                            output.push_vec(time, vec![sum]);
                        }
                    }
                    Ok(())
                }
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<(u64, i32)> = r
            .iter()
            .flat_map(|(t, vs)| vs.iter().map(move |v| (*t, *v)))
            .collect();
        results.sort();
        // t=0: 10+20=30, t=1: 30+40=70, t=2: 50
        assert_eq!(results, vec![(0, 30), (1, 70), (2, 50)]);
    }

    #[test]
    fn test_unary_notify_downstream_chain() {
        // Verify that a notify operator chains correctly with downstream operators.
        // The downstream map should receive data only after the notify operator emits.
        let builder = DataflowBuilder::<u64>::new("notify_chain");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4])])
            .unary_notify("sum", {
                let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                    std::collections::HashMap::new();
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        stash.entry(time).or_default().extend(data);
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        if let Some(data) = stash.remove(&time) {
                            let sum: i32 = data.iter().sum();
                            output.push_vec(time, vec![sum]);
                        }
                    }
                    Ok(())
                }
            })
            .map("format", |_t, x| format!("sum={x}"))
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<&str> = r
            .iter()
            .flat_map(|(_, v)| v.iter().map(|s| s.as_str()))
            .collect();
        assert_eq!(all, vec!["sum=10"]);
    }

    #[test]
    fn test_unary_notify_no_data_no_notification() {
        // If notify_at is never called, no notification fires.
        // The operator should still complete cleanly.
        let builder = DataflowBuilder::<u64>::new("notify_empty");
        let port = builder
            .source::<i32>("nums", vec![])
            .unary_notify::<i32, _>("noop", {
                move |input, _output, _ctx| {
                    // Drain input but never call notify_at
                    while input.next().is_some() {}
                    Ok(())
                }
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn test_unary_notify_duplicate_notify_at() {
        // Calling notify_at(t) multiple times for the same time should
        // result in exactly one notification firing for that time.
        let builder = DataflowBuilder::<u64>::new("notify_dedup");
        let port = builder
            .source("data", vec![(0u64, vec![1i32, 2, 3])])
            .unary_notify("count_notifs", {
                let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                    std::collections::HashMap::new();
                let mut notification_count: i32 = 0;
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        stash.entry(time).or_default().extend(data);
                        // Call notify_at multiple times for the same timestamp
                        ctx.notify_at(time);
                        ctx.notify_at(time);
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        notification_count += 1;
                        if stash.remove(&time).is_some() {
                            // Emit notification count to verify dedup
                            output.push_vec(time, vec![notification_count]);
                        }
                    }
                    Ok(())
                }
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        // The Notificator coalesces duplicate notify_at calls for the same time,
        // so only one notification fires per time. With the dedup guard in
        // NotifyContext::notify_at(), only ONE capability is created regardless
        // of how many times notify_at is called for the same timestamp. The key
        // invariant: data is emitted exactly once per timestamp.
        assert!(!all.is_empty(), "should have at least one notification");
        // Verify exactly one notification fired (notification_count == 1)
        assert_eq!(
            all,
            vec![1],
            "should fire exactly one notification for the deduplicated time"
        );
    }

    // --- with_capacity tests ---

    #[test]
    fn test_with_capacity_basic() {
        // with_capacity should not affect correctness — data still flows through
        let builder = DataflowBuilder::<u64>::new("cap_basic");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .with_capacity(4)
            .unwrap()
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![2, 4, 6]);
    }

    #[test]
    fn test_with_capacity_chained() {
        // Override only applies to the next edge; subsequent edges use default
        let builder = DataflowBuilder::<u64>::new("cap_chain");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4])])
            .with_capacity(8)
            .unwrap()
            .map("double", |_t, x| x * 2)
            // no with_capacity here → default capacity
            .filter("positive", |_t, &x| x > 0)
            .with_capacity(16)
            .unwrap()
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![2, 4, 6, 8]);
    }

    #[test]
    fn test_with_capacity_zero_panics() {
        let builder = DataflowBuilder::<u64>::new("cap_zero");
        assert!(
            builder
                .source("nums", vec![(0u64, vec![1i32])])
                .with_capacity(0)
                .is_err()
        );
    }

    #[test]
    fn test_with_capacity_not_propagated_by_clone() {
        // Cloning a Pipe should NOT copy the capacity_override
        let builder = DataflowBuilder::<u64>::new("cap_clone");
        let s = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .with_capacity(4)
            .unwrap();
        let s2 = s.clone();

        // s still has the override, s2 does not
        let port1 = s.map("double", |_t, x| x * 2).output("out1").unwrap();
        let port2 = s2.map("triple", |_t, x| x * 3).output("out2").unwrap();

        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c1 = port1.collector();
        let r1 = c1.lock().unwrap();
        assert_eq!(r1[0].1, vec![2, 4, 6]);

        let c2 = port2.collector();
        let r2 = c2.lock().unwrap();
        assert_eq!(r2[0].1, vec![3, 6, 9]);
    }

    #[test]
    fn test_with_capacity_small_buffer() {
        // Very small capacity (1) to stress backpressure — should still work
        let builder = DataflowBuilder::<u64>::new("cap_small");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])])
            .with_capacity(1)
            .unwrap()
            .map("inc", |_t, x| x + 1)
            .with_capacity(1)
            .unwrap()
            .filter("even", |_t, &x| x % 2 == 0)
            .with_capacity(1)
            .unwrap()
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut vals: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        vals.sort();
        assert_eq!(vals, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_with_capacity_unary() {
        let builder = DataflowBuilder::<u64>::new("cap_unary");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .with_capacity(2)
            .unwrap()
            .unary("sum", |input, output| {
                while let Some((time, data)) = input.next() {
                    let sum: i32 = data.iter().sum();
                    output.push_vec(time, vec![sum]);
                }
                Ok(())
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![6]);
    }

    #[test]
    fn test_with_capacity_concat() {
        let builder = DataflowBuilder::<u64>::new("cap_concat");
        let a = builder
            .source("a", vec![(0u64, vec![1i32, 2])])
            .with_capacity(4)
            .unwrap();
        let b = builder
            .source("b", vec![(0u64, vec![3i32, 4])])
            .with_capacity(8)
            .unwrap();
        let port = Pipe::concat(vec![a, b]).unwrap().output("merged").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut vals: Vec<i32> = r.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        vals.sort();
        assert_eq!(vals, vec![1, 2, 3, 4]);
    }

    // --- Result combinator tests ---

    #[test]
    fn test_map_ok() {
        let builder = DataflowBuilder::<u64>::new("map_ok");
        let stream = builder.source::<std::result::Result<i32, String>>(
            "data",
            vec![(0u64, vec![Ok(1), Err("bad".into()), Ok(3)])],
        );
        let port = stream
            .map_ok("double", |_t, v| v * 2)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![Ok(2), Err("bad".into()), Ok(6)]);
    }

    #[test]
    fn test_filter_ok() {
        let builder = DataflowBuilder::<u64>::new("filter_ok");
        let stream = builder.source::<std::result::Result<i32, String>>(
            "data",
            vec![(0u64, vec![Ok(1), Ok(2), Err("err".into()), Ok(3)])],
        );
        let port = stream
            .filter_ok("even", |_t, v| v % 2 == 0)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![Ok(2), Err("err".into())]);
    }

    #[test]
    fn test_and_then() {
        let builder = DataflowBuilder::<u64>::new("and_then");
        let stream = builder.source::<std::result::Result<String, String>>(
            "data",
            vec![(
                0u64,
                vec![
                    Ok("42".into()),
                    Err("upstream_err".into()),
                    Ok("not_a_number".into()),
                ],
            )],
        );
        let port = stream
            .and_then("parse", |_t, s: String| {
                s.parse::<i32>().map_err(|e| e.to_string())
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1.len(), 3);
        assert_eq!(r[0].1[0], Ok(42));
        assert_eq!(r[0].1[1], Err("upstream_err".into()));
        assert!(r[0].1[2].is_err()); // parse error
    }

    #[test]
    fn test_branch_result() {
        let builder = DataflowBuilder::<u64>::new("branch_result");
        let stream = builder.source::<std::result::Result<i32, String>>(
            "data",
            vec![(0u64, vec![Ok(1), Err("a".into()), Ok(2), Err("b".into())])],
        );
        let (ok_pipe, err_pipe) = stream.branch_result("split");
        let ok_port = ok_pipe.output("goods").unwrap();
        let err_port = err_pipe.output("bads").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let oc = ok_port.collector();
        let or = oc.lock().unwrap();
        let mut ok_vals: Vec<i32> = or.iter().flat_map(|(_, v)| v.iter().copied()).collect();
        ok_vals.sort();
        assert_eq!(ok_vals, vec![1, 2]);

        let ec = err_port.collector();
        let er = ec.lock().unwrap();
        let mut err_vals: Vec<String> = er.iter().flat_map(|(_, v)| v.iter().cloned()).collect();
        err_vals.sort();
        assert_eq!(err_vals, vec!["a", "b"]);
    }

    #[test]
    fn test_map_ok_chained() {
        // Chain: map_ok → filter_ok → output
        let builder = DataflowBuilder::<u64>::new("chain");
        let stream = builder.source::<std::result::Result<i32, String>>(
            "data",
            vec![(0u64, vec![Ok(1), Ok(2), Ok(3), Ok(4), Err("x".into())])],
        );
        let port = stream
            .map_ok("double", |_t, v| v * 2)
            .filter_ok("big", |_t, v| *v > 4)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![Ok(6), Ok(8), Err("x".into())]);
    }

    #[test]
    fn test_result_combinators_all_ok() {
        let builder = DataflowBuilder::<u64>::new("all_ok");
        let stream = builder
            .source::<std::result::Result<i32, String>>("data", vec![(0u64, vec![Ok(10), Ok(20)])]);
        let port = stream
            .map_ok("inc", |_t, v| v + 1)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![Ok(11), Ok(21)]);
    }

    #[test]
    fn test_result_combinators_all_err() {
        let builder = DataflowBuilder::<u64>::new("all_err");
        let stream = builder.source::<std::result::Result<i32, String>>(
            "data",
            vec![(0u64, vec![Err("a".into()), Err("b".into())])],
        );
        let port = stream
            .map_ok("inc", |_t, v: i32| v + 1)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![Err("a".into()), Err("b".into())]);
    }

    // ── take / take_while tests ──────────────────────────────────────

    #[test]
    fn test_take_fewer_than_available() {
        let builder = DataflowBuilder::<u64>::new("take_fewer");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])]);
        let port = stream.take("first3", 3).output("out").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_more_than_available() {
        let builder = DataflowBuilder::<u64>::new("take_more");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream.take("first10", 10).output("out").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_zero() {
        let builder = DataflowBuilder::<u64>::new("take_zero");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream.take("none", 0).output("out").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert!(all.is_empty());
    }

    #[test]
    fn test_take_across_batches() {
        let builder = DataflowBuilder::<u64>::new("take_batches");
        let stream = builder.source(
            "nums",
            vec![
                (0u64, vec![1i32, 2]),
                (1u64, vec![3, 4]),
                (2u64, vec![5, 6]),
            ],
        );
        let port = stream.take("first3", 3).output("out").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_while_stops_at_predicate() {
        let builder = DataflowBuilder::<u64>::new("take_while_stop");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])]);
        let port = stream
            .take_while("small", |_t, x| *x < 4)
            .output("out")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_while_all_pass() {
        let builder = DataflowBuilder::<u64>::new("take_while_all");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream
            .take_while("always", |_t, _x| true)
            .output("out")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_while_none_pass() {
        let builder = DataflowBuilder::<u64>::new("take_while_none");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        let port = stream
            .take_while("never", |_t, _x| false)
            .output("out")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert!(all.is_empty());
    }

    #[test]
    fn test_take_while_across_batches() {
        let builder = DataflowBuilder::<u64>::new("take_while_batches");
        let stream = builder.source(
            "nums",
            vec![(0u64, vec![1i32, 2]), (1u64, vec![3, 10]), (2u64, vec![20])],
        );
        let port = stream
            .take_while("under10", |_t, x| *x < 10)
            .output("out")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![1, 2, 3]);
    }

    #[test]
    fn test_take_with_downstream_map() {
        let builder = DataflowBuilder::<u64>::new("take_then_map");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])]);
        let port = stream
            .take("first2", 2)
            .map("double", |_t, x| x * 2)
            .output("out")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let all: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(all, vec![2, 4]);
    }

    // ── metrics tests ────────────────────────────────────────────────

    #[test]
    fn test_collect_metrics_disabled_by_default() {
        let builder = DataflowBuilder::<u64>::new("no_metrics");
        let stream = builder.source("nums", vec![(0u64, vec![1i32])]);
        stream.output("out").unwrap();
        let dataflow = builder.build().unwrap();
        assert!(!dataflow.collect_metrics);
    }

    #[test]
    fn test_collect_metrics_records_activations() {
        let builder = DataflowBuilder::<u64>::new("metrics_test");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2, 3])]);
        stream.map("double", |_t, x| x * 2).output("out").unwrap();
        let dataflow = builder.build().unwrap();

        let metrics = rt().run_with_metrics(dataflow).unwrap();
        let m = metrics.expect("metrics should be collected");
        assert!(m.wall_time() > std::time::Duration::ZERO);
        assert!(m.total_activations() > 0);
        assert!(m.operator_count() > 0);

        // Each operator should have been activated at least once
        let snapshots = m.operator_snapshots();
        assert!(!snapshots.is_empty());
        for op in &snapshots {
            assert!(
                op.activations > 0,
                "operator '{}' had 0 activations",
                op.name
            );
        }
    }

    #[test]
    fn test_collect_metrics_not_enabled() {
        let builder = DataflowBuilder::<u64>::new("no_metrics");
        let stream = builder.source("nums", vec![(0u64, vec![1i32, 2])]);
        stream.output("out").unwrap();
        let mut dataflow = builder.build().unwrap();
        // Explicitly disable (should already be false by default)
        dataflow.collect_metrics = false;

        // Use run() instead — no metrics
        rt().run(dataflow).unwrap();
    }

    // --- Inspect operator tests ---

    #[test]
    fn test_inspect_sees_all_items() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        let builder = DataflowBuilder::<u64>::new("inspect_test");
        let port = builder
            .source("nums", vec![(0u64, vec![10i32, 20, 30])])
            .inspect("spy", move |_t, x| {
                seen_clone.lock().unwrap().push(*x);
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        // Side-effect captured all items
        assert_eq!(*seen.lock().unwrap(), vec![10, 20, 30]);

        // Data passes through unchanged
        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![10, 20, 30]);
    }

    #[test]
    fn test_inspect_batch_sees_whole_batch() {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let batch_sizes_clone = batch_sizes.clone();

        let builder = DataflowBuilder::<u64>::new("inspect_batch_test");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![1i32, 2, 3]), (1u64, vec![4i32, 5])],
            )
            .inspect_batch("batch-spy", move |_t, batch| {
                batch_sizes_clone.lock().unwrap().push(batch.len());
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        // Two batches observed
        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(sizes.len(), 2);
        assert_eq!(sizes[0], 3);
        assert_eq!(sizes[1], 2);

        // Data passes through unchanged
        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0], (0, vec![1, 2, 3]));
        assert_eq!(r[1], (1, vec![4, 5]));
    }

    #[test]
    fn test_inspect_in_pipeline() {
        // inspect should not alter downstream results
        let builder = DataflowBuilder::<u64>::new("inspect_pipeline");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])])
            .inspect("tap", |_t, _x| { /* no-op */ })
            .filter("even", |_t, x| x % 2 == 0)
            .inspect("tap2", |_t, _x| { /* no-op */ })
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![4, 8]);
    }

    #[test]
    fn test_inspect_receives_timestamp() {
        let timestamps = Arc::new(Mutex::new(Vec::new()));
        let ts_clone = timestamps.clone();

        let builder = DataflowBuilder::<u64>::new("inspect_ts");
        let port = builder
            .source("nums", vec![(10u64, vec![1i32]), (20u64, vec![2i32])])
            .inspect("ts-spy", move |t, _x| {
                ts_clone.lock().unwrap().push(*t);
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let ts = timestamps.lock().unwrap();
        assert_eq!(*ts, vec![10, 20]);

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_for_each_sees_all_items() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        let builder = DataflowBuilder::<u64>::new("for_each_test");
        builder
            .source("nums", vec![(0u64, vec![10i32, 20, 30])])
            .for_each("consume", move |_t, x| {
                seen_clone.lock().unwrap().push(*x);
            });
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        assert_eq!(*seen.lock().unwrap(), vec![10, 20, 30]);
    }

    #[test]
    fn test_for_each_batch_sees_whole_batch() {
        let batch_sizes = Arc::new(Mutex::new(Vec::new()));
        let batch_sizes_clone = batch_sizes.clone();

        let builder = DataflowBuilder::<u64>::new("for_each_batch_test");
        builder
            .source(
                "nums",
                vec![(0u64, vec![1i32, 2, 3]), (1u64, vec![4i32, 5])],
            )
            .for_each_batch("count", move |_t, batch| {
                batch_sizes_clone.lock().unwrap().push(batch.len());
            });
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(sizes.iter().sum::<usize>(), 5);
    }

    #[test]
    fn test_for_each_receives_timestamp() {
        let timestamps = Arc::new(Mutex::new(Vec::new()));
        let ts_clone = timestamps.clone();

        let builder = DataflowBuilder::<u64>::new("for_each_ts_test");
        builder
            .source("nums", vec![(10u64, vec![1i32]), (20u64, vec![2i32])])
            .for_each("check-ts", move |t, _x| {
                ts_clone.lock().unwrap().push(*t);
            });
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let ts = timestamps.lock().unwrap();
        assert!(ts.contains(&10));
        assert!(ts.contains(&20));
    }

    #[test]
    fn test_for_each_after_map() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        let builder = DataflowBuilder::<u64>::new("for_each_after_map");
        builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map("double", |_t, x| x * 2)
            .for_each("consume", move |_t, x| {
                seen_clone.lock().unwrap().push(*x);
            });
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        assert_eq!(*seen.lock().unwrap(), vec![2, 4, 6]);
    }

    #[test]
    fn test_reduce_sums_per_timestamp() {
        let builder = DataflowBuilder::<u64>::new("reduce_sum");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![1i32, 2, 3]), (1u64, vec![10i32, 20])],
            )
            .reduce("sum", |a, b| a + b)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<(u64, i32)> = r
            .iter()
            .flat_map(|(t, d)| d.iter().map(move |v| (*t, *v)))
            .collect();
        results.sort();
        assert_eq!(results, vec![(0, 6), (1, 30)]);
    }

    #[test]
    fn test_reduce_single_element() {
        let builder = DataflowBuilder::<u64>::new("reduce_single");
        let port = builder
            .source("nums", vec![(0u64, vec![42i32])])
            .reduce("id", |a, b| a + b)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![42]);
    }

    #[test]
    fn test_reduce_empty_input() {
        let builder = DataflowBuilder::<u64>::new("reduce_empty");
        let port = builder
            .source::<i32>("nums", vec![])
            .reduce("sum", |a, b| a + b)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let total: usize = r.iter().map(|(_, d)| d.len()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_fold_count_elements() {
        let builder = DataflowBuilder::<u64>::new("fold_count");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![10i32, 20, 30]), (1u64, vec![40i32, 50])],
            )
            .fold("count", 0usize, |acc, _item| acc + 1)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<(u64, usize)> = r
            .iter()
            .flat_map(|(t, d)| d.iter().map(move |v| (*t, *v)))
            .collect();
        results.sort();
        assert_eq!(results, vec![(0, 3), (1, 2)]);
    }

    #[test]
    fn test_fold_changes_type() {
        let builder = DataflowBuilder::<u64>::new("fold_to_string");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .fold("join", String::new(), |mut acc, x| {
                if !acc.is_empty() {
                    acc.push(',');
                }
                acc.push_str(&x.to_string());
                acc
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec!["1,2,3"]);
    }

    #[test]
    fn test_reduce_then_map() {
        let builder = DataflowBuilder::<u64>::new("reduce_then_map");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4])])
            .reduce("sum", |a, b| a + b)
            .map("negate", |_t, x| -x)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![-10]);
    }

    #[test]
    fn test_distinct_removes_duplicates() {
        let builder = DataflowBuilder::<u64>::new("distinct_test");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 2, 3, 1, 3, 4])])
            .distinct("dedup")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        results.sort();
        assert_eq!(results, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_distinct_per_timestamp() {
        let builder = DataflowBuilder::<u64>::new("distinct_per_ts");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![1i32, 1, 2]), (1u64, vec![2i32, 2, 3])],
            )
            .distinct("dedup")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        // Each timestamp deduplicates independently
        let mut t0: Vec<i32> = r
            .iter()
            .filter(|(t, _)| *t == 0)
            .flat_map(|(_, d)| d.clone())
            .collect();
        let mut t1: Vec<i32> = r
            .iter()
            .filter(|(t, _)| *t == 1)
            .flat_map(|(_, d)| d.clone())
            .collect();
        t0.sort();
        t1.sort();
        assert_eq!(t0, vec![1, 2]);
        assert_eq!(t1, vec![2, 3]);
    }

    #[test]
    fn test_distinct_no_duplicates() {
        let builder = DataflowBuilder::<u64>::new("distinct_no_dups");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])])
            .distinct("dedup")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        results.sort();
        assert_eq!(results, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_distinct_empty() {
        let builder = DataflowBuilder::<u64>::new("distinct_empty");
        let port = builder
            .source::<i32>("nums", vec![])
            .distinct("dedup")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert!(r.iter().flat_map(|(_, d)| d.clone()).count() == 0);
    }

    #[test]
    fn test_count_elements_per_timestamp() {
        let builder = DataflowBuilder::<u64>::new("count_test");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![10i32, 20, 30]), (1u64, vec![40i32, 50])],
            )
            .count("count")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let mut results: Vec<(u64, usize)> = r
            .iter()
            .flat_map(|(t, d)| d.iter().map(move |v| (*t, *v)))
            .collect();
        results.sort();
        assert_eq!(results, vec![(0, 3), (1, 2)]);
    }

    #[test]
    fn test_distinct_then_count() {
        let builder = DataflowBuilder::<u64>::new("distinct_then_count");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 1, 2, 2, 3])])
            .distinct("dedup")
            .count("count")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<usize> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![3]);
    }

    #[test]
    fn test_gather_routes_all_to_single_output() {
        // In single-worker mode, gather is a pass-through.
        let builder = DataflowBuilder::<u64>::new("gather_test");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5])])
            .gather("collect-all")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_gather_then_reduce() {
        // gather → reduce produces a single global result.
        let builder = DataflowBuilder::<u64>::new("gather_reduce");
        let port = builder
            .source("nums", vec![(0u64, vec![10i32, 20, 30])])
            .gather("collect")
            .reduce("sum", |acc, x| acc + x)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![60]);
    }

    #[test]
    fn test_rebalance_passes_all_data() {
        // In single-worker mode, rebalance is a pass-through.
        let builder = DataflowBuilder::<u64>::new("rebalance_test");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6])])
            .rebalance("spread")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_rebalance_then_map() {
        // rebalance → map pipeline preserves all data.
        let builder = DataflowBuilder::<u64>::new("rebalance_map");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .rebalance("distribute")
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![2, 4, 6]);
    }

    #[test]
    fn test_gather_multi_epoch() {
        // gather preserves data across multiple epochs.
        let builder = DataflowBuilder::<u64>::new("gather_epochs");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2]), (1u64, vec![10, 20])])
            .gather("collect")
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![1, 2, 10, 20]);
    }

    #[test]
    fn test_rebalance_to_passes_all_data() {
        // rebalance_to with explicit parallelism preserves all data.
        let builder = DataflowBuilder::<u64>::new("rebalance_to_test");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4])])
            .rebalance_to("fan-out", 4)
            .unwrap()
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![1, 2, 3, 4]);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn test_rebalance_to_zero_panics() {
        let builder = DataflowBuilder::<u64>::new("rebalance_to_zero");
        builder
            .source("nums", vec![(0u64, vec![1i32])])
            .rebalance_to("bad", 0)
            .unwrap();
    }

    #[test]
    fn test_map_batch_sort() {
        // Sort each batch
        let builder = DataflowBuilder::<u64>::new("map_batch_sort");
        let port = builder
            .source("nums", vec![(0u64, vec![5i32, 3, 1, 4, 2])])
            .map_batch("sort", |_t, mut batch| {
                batch.sort();
                batch
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_map_batch_filter_and_transform() {
        // Filter even numbers and double them, batch-level
        let builder = DataflowBuilder::<u64>::new("map_batch_filter");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6])])
            .map_batch("even_doubled", |_t, batch| {
                batch
                    .into_iter()
                    .filter(|x| x % 2 == 0)
                    .map(|x| x * 2)
                    .collect()
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec![4, 8, 12]);
    }

    #[test]
    fn test_map_batch_empty_output() {
        // Return empty vec — filters out entire batch
        let builder = DataflowBuilder::<u64>::new("map_batch_empty");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map_batch("drop-all", |_t, _batch| Vec::<i32>::new())
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_map_batch_type_change() {
        // Change output type: Vec<i32> → Vec<String>
        let builder = DataflowBuilder::<u64>::new("map_batch_type");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map_batch("to_string", |_t, batch| {
                batch.into_iter().map(|x| format!("v{x}")).collect()
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<String> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(results, vec!["v1", "v2", "v3"]);
    }

    #[test]
    fn test_map_batch_multi_epoch() {
        // Each epoch's batch processed independently
        let builder = DataflowBuilder::<u64>::new("map_batch_epochs");
        let port = builder
            .source(
                "nums",
                vec![(0u64, vec![3i32, 1, 2]), (1u64, vec![6, 4, 5])],
            )
            .map_batch("sort", |_t, mut batch| {
                batch.sort();
                batch
            })
            .output("results")
            .unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        let results: Vec<i32> = r.iter().flat_map(|(_, d)| d.clone()).collect();
        // Each epoch sorted independently
        assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_branch_even_odd() {
        let builder = DataflowBuilder::<u64>::new("branch_even_odd");
        let (evens, odds) = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3, 4, 5, 6])])
            .branch("parity", |_t, x| x % 2 == 0);
        let even_port = evens.output("evens").unwrap();
        let odd_port = odds.output("odds").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let ec = even_port.collector();
        let er = ec.lock().unwrap();
        let even_results: Vec<i32> = er.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(even_results, vec![2, 4, 6]);

        let oc = odd_port.collector();
        let or_ = oc.lock().unwrap();
        let odd_results: Vec<i32> = or_.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(odd_results, vec![1, 3, 5]);
    }

    #[test]
    fn test_branch_all_true() {
        let builder = DataflowBuilder::<u64>::new("branch_all_true");
        let (matched, unmatched) = builder
            .source("nums", vec![(0u64, vec![10i32, 20, 30])])
            .branch("all", |_t, _x| true);
        let matched_port = matched.output("matched").unwrap();
        let unmatched_port = unmatched.output("unmatched").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let mc = matched_port.collector();
        let mr = mc.lock().unwrap();
        let matched_results: Vec<i32> = mr.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(matched_results, vec![10, 20, 30]);

        let uc = unmatched_port.collector();
        let ur = uc.lock().unwrap();
        let unmatched_results: Vec<i32> = ur.iter().flat_map(|(_, d)| d.clone()).collect();
        assert!(unmatched_results.is_empty());
    }

    #[test]
    fn test_branch_all_false() {
        let builder = DataflowBuilder::<u64>::new("branch_all_false");
        let (matched, unmatched) = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .branch("none", |_t, _x| false);
        let matched_port = matched.output("matched").unwrap();
        let unmatched_port = unmatched.output("unmatched").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let mc = matched_port.collector();
        let mr = mc.lock().unwrap();
        let matched_results: Vec<i32> = mr.iter().flat_map(|(_, d)| d.clone()).collect();
        assert!(matched_results.is_empty());

        let uc = unmatched_port.collector();
        let ur = uc.lock().unwrap();
        let unmatched_results: Vec<i32> = ur.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(unmatched_results, vec![1, 2, 3]);
    }

    #[test]
    fn test_branch_with_downstream_processing() {
        // Branch then process each side differently
        let builder = DataflowBuilder::<u64>::new("branch_process");
        let (big, small) = builder
            .source("nums", vec![(0u64, vec![1i32, 5, 10, 15, 20])])
            .branch("threshold", |_t, x| *x >= 10);
        let big_port = big.map("negate", |_t, x| -x).output("big").unwrap();
        let small_port = small.map("double", |_t, x| x * 2).output("small").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let bc = big_port.collector();
        let br = bc.lock().unwrap();
        let big_results: Vec<i32> = br.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(big_results, vec![-10, -15, -20]);

        let sc = small_port.collector();
        let sr = sc.lock().unwrap();
        let small_results: Vec<i32> = sr.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(small_results, vec![2, 10]);
    }

    #[test]
    fn test_branch_multi_epoch() {
        let builder = DataflowBuilder::<u64>::new("branch_epochs");
        let (pos, neg) = builder
            .source(
                "nums",
                vec![(0u64, vec![-2i32, -1, 0, 1, 2]), (1u64, vec![-10, 10])],
            )
            .branch("sign", |_t, x| *x >= 0);
        let pos_port = pos.output("positive").unwrap();
        let neg_port = neg.output("negative").unwrap();
        let dataflow = builder.build().unwrap();
        rt().run(dataflow).unwrap();

        let pc = pos_port.collector();
        let pr = pc.lock().unwrap();
        let pos_results: Vec<i32> = pr.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(pos_results, vec![0, 1, 2, 10]);

        let nc = neg_port.collector();
        let nr = nc.lock().unwrap();
        let neg_results: Vec<i32> = nr.iter().flat_map(|(_, d)| d.clone()).collect();
        assert_eq!(neg_results, vec![-2, -1, -10]);
    }
}
