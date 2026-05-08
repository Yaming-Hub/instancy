//! Type-erased operator scheduling trait and activation outcomes.
//!
//! This module defines the [`SchedulableOperator`] trait — the runtime's view of
//! a dataflow operator. Concrete operators (e.g., `UnaryOperator`) are wrapped
//! in types that implement this trait, hiding their generic type parameters
//! behind a uniform interface.
//!
//! # Cardinality & Lifetime
//!
//! One `Box<dyn SchedulableOperator>` per operator per worker. Created during
//! dataflow materialization and owned by the [`DataflowExecutor`](super::executor::DataflowExecutor).

use crate::dataflow::stage::StageId;
use crate::error::Result;
use crate::worker::WorkerContext;

// ---------------------------------------------------------------------------
// ActivationOutcome — result of a single operator activation
// ---------------------------------------------------------------------------

/// The result of activating an operator once.
///
/// Used by the executor to decide which operators to re-schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationOutcome {
    /// The operator processed data or made meaningful progress.
    /// It should be re-scheduled if more input might arrive.
    MadeProgress,

    /// The operator had no work to do (no input available).
    /// It should NOT be re-scheduled until new input arrives.
    Idle,

    /// The operator could not push all its output due to downstream backpressure.
    /// It should be re-scheduled once the downstream channel has space.
    BlockedOnBackpressure,

    /// The operator has completed all work and will not produce more output.
    /// It should not be activated again.
    Done,
}

// ---------------------------------------------------------------------------
// SchedulableOperator — the runtime's view of an operator
// ---------------------------------------------------------------------------

/// The type-erased runtime interface for a dataflow operator.
///
/// Each concrete operator (Unary, Binary, etc.) is wrapped in a struct that
/// implements this trait. The executor calls [`activate`](Self::activate) in
/// response to activation requests (new input arriving, timer, etc.).
///
/// # Implementation contract
///
/// - `activate()` must be non-blocking and bounded in work.
/// - An operator returning `Done` will never be activated again.
/// - `BlockedOnBackpressure` means the operator has unsent output; the executor
///   should retry after the downstream channel drains.
pub trait SchedulableOperator: Send {
    /// Execute the operator once.
    ///
    /// The operator should:
    /// 1. Pull available data from its input channels.
    /// 2. Run user logic on the pulled data.
    /// 3. Push output data to its output channels.
    /// 4. Return the outcome describing what happened.
    fn activate(&mut self) -> Result<ActivationOutcome>;

    /// Whether the operator has finished all work and will never produce more output.
    fn is_done(&self) -> bool;

    /// Human-readable operator name (for diagnostics).
    fn name(&self) -> &str;

    /// Operator index within the scope.
    fn index(&self) -> usize;

    /// The execution stage this operator belongs to.
    fn stage_id(&self) -> StageId;

    /// Close all input channels to signal no more data will arrive.
    ///
    /// Called by the executor when upstream operators have completed or
    /// the dataflow is shutting down.
    fn close_inputs(&mut self);

    /// Update this operator's input frontier from the progress tracker.
    ///
    /// Called by the executor after progress propagation for operators whose
    /// frontiers changed. The `frontier` is an `Antichain<T>` (the meet of
    /// all input port frontiers), passed as `&dyn Any` for type erasure.
    ///
    /// Notify-capable operators override this to update their internal
    /// [`Notificator`](crate::progress::notificator::Notificator), which
    /// fires ready notifications when the frontier advances past requested
    /// timestamps. Regular operators leave this as a no-op.
    fn update_input_frontier(&mut self, _frontier: &dyn std::any::Any) {}

    /// Whether this operator has ready notifications that need processing.
    ///
    /// The executor checks this after progress propagation to re-enqueue
    /// operators with fired notifications, even if they have no new input
    /// data. Regular operators return `false` (default).
    fn has_ready_notifications(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// OperatorFactory — deferred operator construction
// ---------------------------------------------------------------------------

/// A factory that creates a fully-wired [`SchedulableOperator`] when given
/// its channel endpoints.
///
/// Stored during the build phase (when concrete types are known) and invoked
/// during materialization (when channels have been allocated).
///
/// # Single-worker vs. multi-worker
///
/// For single-worker dataflows, `build()` is called exactly once. For
/// multi-worker dataflows, `build()` is called N times (once per worker),
/// each time with fresh channel endpoints. Implementations must produce
/// independent operator instances on each call.
///
/// Use [`SingleUseFactory`] for closures that can only be invoked once
/// (backward-compatible with existing `FnOnce` factories). Use
/// [`ReplayableFactory`] for multi-worker-capable factories.
pub trait OperatorBlueprint: Send {
    /// Create a wired operator instance for the given worker.
    ///
    /// `ctx` provides the worker's identity (index and total count).
    /// `endpoints` provides the input pullers and output pushers allocated
    /// for this worker's copy of the operator.
    ///
    /// # Errors
    ///
    /// Returns an error if the factory has been exhausted (single-use called
    /// twice) or if operator construction fails.
    fn build(
        &mut self,
        ctx: &WorkerContext,
        endpoints: ChannelEndpoints,
    ) -> crate::Result<Box<dyn SchedulableOperator>>;

    /// Whether this blueprint can produce multiple operator instances.
    ///
    /// Returns `false` for single-use factories (will return `Err` on second `build()`).
    /// Returns `true` for replayable factories.
    ///
    /// Currently unused — will be checked in the multi-worker materialization path
    /// (PR 39) to validate all factories before attempting N materializations.
    fn is_replayable(&self) -> bool;
}

/// Type alias for a boxed operator blueprint.
pub type OperatorFactory = Box<dyn OperatorBlueprint>;

/// Create an [`OperatorFactory`] from a single-use `FnOnce` closure.
///
/// This is the primary way to create operator factories in builder methods.
/// The resulting factory can be called exactly once during materialization;
/// a second call returns an error.
pub fn single_use_factory(
    f: impl FnOnce(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>> + Send + 'static,
) -> OperatorFactory {
    Box::new(SingleUseFactory(Some(Box::new(f))))
}

/// A single-use operator factory wrapping a `FnOnce` closure.
///
/// This is the default for all current builder methods. It can produce
/// exactly one operator instance. Calling `build()` a second time returns
/// an error.
pub struct SingleUseFactory(
    Option<
        Box<dyn FnOnce(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>> + Send>,
    >,
);

impl SingleUseFactory {
    /// Create a new single-use factory from a `FnOnce` closure.
    pub fn new(
        factory: impl FnOnce(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>>
        + Send
        + 'static,
    ) -> Self {
        Self(Some(Box::new(factory)))
    }

    /// Box this factory as an [`OperatorFactory`].
    pub fn boxed(self) -> OperatorFactory {
        Box::new(self)
    }
}

impl OperatorBlueprint for SingleUseFactory {
    fn build(
        &mut self,
        ctx: &WorkerContext,
        endpoints: ChannelEndpoints,
    ) -> crate::Result<Box<dyn SchedulableOperator>> {
        let factory = self
            .0
            .take()
            .ok_or_else(|| crate::Error::Custom("SingleUseFactory::build() called more than once".into()))?;
        factory(ctx, endpoints)
    }

    fn is_replayable(&self) -> bool {
        false
    }
}

/// A replayable operator factory that can produce multiple independent instances.
///
/// Wraps a `FnMut` that creates a fresh operator on each `build()` call.
/// Used for multi-worker dataflows where each worker needs its own operator.
///
/// # Example
///
/// ```ignore
/// ReplayableFactory::new(move |_ctx, endpoints| {
///     // Create fresh state for this worker
///     let logic = logic_factory();
///     Ok(Box::new(WiredUnaryOperator::new(name, idx, stage, logic, ...)))
/// })
/// ```
pub struct ReplayableFactory(
    Box<dyn FnMut(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>> + Send>,
);

impl ReplayableFactory {
    /// Create a new replayable factory from a `FnMut` closure.
    pub fn new(
        factory: impl FnMut(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>>
        + Send
        + 'static,
    ) -> Self {
        Self(Box::new(factory))
    }

    /// Box this factory as an [`OperatorFactory`].
    pub fn boxed(self) -> OperatorFactory {
        Box::new(self)
    }
}

impl OperatorBlueprint for ReplayableFactory {
    fn build(
        &mut self,
        ctx: &WorkerContext,
        endpoints: ChannelEndpoints,
    ) -> crate::Result<Box<dyn SchedulableOperator>> {
        (self.0)(ctx, endpoints)
    }

    fn is_replayable(&self) -> bool {
        true
    }
}

/// Channel endpoints provided to an operator factory during materialization.
///
/// Each entry is a `Box<dyn Any + Send>` that the factory downcasts to the
/// concrete `Box<dyn Pull<T, D, M>>` or `Box<dyn Push<T, D, M>>` type.
#[derive(Default)]
pub struct ChannelEndpoints {
    /// Input pullers, one per input port. Each is `Box<dyn Pull<T, D, M>>`.
    pub input_pullers: Vec<Box<dyn std::any::Any + Send>>,
    /// Output pushers, one per output port. Each entry is a `Vec<Box<dyn Push<T, D, M>>>`
    /// (multiple pushers per port when the output fans out to multiple targets).
    pub output_pushers: Vec<Box<dyn std::any::Any + Send>>,
}

impl std::fmt::Debug for ChannelEndpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelEndpoints")
            .field("input_count", &self.input_pullers.len())
            .field("output_count", &self.output_pushers.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// EdgeTypeInfo — type metadata for channel creation
// ---------------------------------------------------------------------------

/// Type information for a single edge, used to create properly-typed channels
/// during materialization.
///
/// Stores `TypeId`s so the materializer can validate type compatibility
/// between connected ports.
#[derive(Debug, Clone)]
pub struct EdgeTypeInfo {
    /// TypeId of the data type flowing through this edge.
    pub data_type_id: std::any::TypeId,
    /// Human-readable name of the data type (for error messages).
    pub data_type_name: &'static str,
    /// TypeId of the timestamp type.
    pub timestamp_type_id: std::any::TypeId,
    /// TypeId of the metadata type.
    pub metadata_type_id: std::any::TypeId,
}

/// A blueprint that creates a typed channel pair for an edge.
///
/// Takes `(wake_handle)` where:
/// - `wake_handle` is an optional [`crate::dataflow::channels::WakeHandle`] for async executor notifications.
///   When provided, channels notify the handle on push, close, drop, and
///   when pulling frees capacity (backpressure relief).
///
/// Returns `Result<(Box<dyn Any + Send>, Box<dyn Any + Send>)>` where the first
/// element is a `Box<dyn Push<T, D, M>>` and the second is a `Box<dyn Pull<T, D, M>>`.
///
/// Pipeline channel factories are stateless and can be called multiple times.
/// Exchange channel factories consume shared materializer state and must only
/// be called once per worker slot — a failed `build()` may leave the factory
/// in a partially consumed state that cannot be retried.
pub trait ChannelBlueprint: Send {
    /// Create a channel pair for the given worker and wake handle.
    ///
    /// For pipeline channels, the worker context is ignored (each worker gets
    /// an independent bounded channel). For exchange channels, the context
    /// determines which worker's Push/Pull pair to return from the shared
    /// cross-worker channel set.
    ///
    /// # Errors
    ///
    /// Returns an error if channel materialization fails (e.g., network
    /// connection unavailable, materializer state already consumed).
    fn build(
        &mut self,
        ctx: &WorkerContext,
        wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
    ) -> crate::Result<(Box<dyn std::any::Any + Send>, Box<dyn std::any::Any + Send>)>;
}

/// Type alias for a boxed channel blueprint.
pub type ChannelFactory = Box<dyn ChannelBlueprint>;

/// Type alias for the channel pair returned by [`ChannelBlueprint::build`].
pub type ChannelPair = (Box<dyn std::any::Any + Send>, Box<dyn std::any::Any + Send>);

/// Create a [`ChannelFactory`] from a closure.
pub fn channel_factory(
    f: impl FnMut(
        &WorkerContext,
        Option<crate::dataflow::channels::wake::WakeHandle>,
    ) -> crate::Result<ChannelPair>
    + Send
    + 'static,
) -> ChannelFactory {
    Box::new(ChannelBlueprintFn(Box::new(f)))
}

/// Wraps a `FnMut` as a [`ChannelBlueprint`].
pub struct ChannelBlueprintFn(
    Box<
        dyn FnMut(
                &WorkerContext,
                Option<crate::dataflow::channels::wake::WakeHandle>,
            ) -> crate::Result<ChannelPair>
            + Send,
    >,
);

impl ChannelBlueprintFn {
    /// Create a new channel blueprint from a closure.
    pub fn new(
        factory: impl FnMut(
            &WorkerContext,
            Option<crate::dataflow::channels::wake::WakeHandle>,
        ) -> crate::Result<ChannelPair>
        + Send
        + 'static,
    ) -> Self {
        Self(Box::new(factory))
    }

    /// Box this blueprint as a [`ChannelFactory`].
    pub fn boxed(self) -> ChannelFactory {
        Box::new(self)
    }
}

impl ChannelBlueprint for ChannelBlueprintFn {
    fn build(
        &mut self,
        ctx: &WorkerContext,
        wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
    ) -> crate::Result<ChannelPair> {
        (self.0)(ctx, wake_handle)
    }
}
