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

    /// The operator has in-flight async tasks but nothing to process locally.
    /// It should NOT be re-scheduled until a task completes (via wake handle),
    /// but the executor must NOT declare quiescence while this state is active.
    WaitingForAsync,

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
/// during materialization (when channels have been allocated). Wraps a
/// `FnMut` closure that produces a fresh operator on each call.
///
/// # Single-worker vs. multi-worker
///
/// For single-worker dataflows, `build()` is called exactly once. For
/// multi-worker dataflows, `build()` is called N times (once per worker),
/// each time with fresh channel endpoints. The closure must produce
/// independent operator instances on each call — user closures are cloned
/// per invocation to give each worker independent state.
///
/// For factories that capture one-shot resources (e.g., channel endpoints),
/// wrap the resource in `Option` and use `take()` inside the closure.
///
/// # Example
///
/// ```ignore
/// OperatorFactory::new(move |_ctx, endpoints| {
///     let logic = logic_factory();
///     Ok(Box::new(WiredUnaryOperator::new(name, idx, stage, logic, ...)))
/// })
/// ```
pub struct OperatorFactory(
    Box<
        dyn FnMut(&WorkerContext, ChannelEndpoints) -> crate::Result<Box<dyn SchedulableOperator>>
            + Send,
    >,
);

impl OperatorFactory {
    /// Create a new operator factory from a `FnMut` closure.
    pub fn new(
        factory: impl FnMut(
            &WorkerContext,
            ChannelEndpoints,
        ) -> crate::Result<Box<dyn SchedulableOperator>>
        + Send
        + 'static,
    ) -> Self {
        Self(Box::new(factory))
    }

    /// Create a wired operator instance for the given worker.
    ///
    /// `ctx` provides the worker's identity (index and total count).
    /// `endpoints` provides the input pullers and output pushers allocated
    /// for this worker's copy of the operator.
    ///
    /// # Errors
    ///
    /// Returns an error if operator construction fails, or if a one-shot
    /// resource captured by the factory has already been consumed.
    pub fn build(
        &mut self,
        ctx: &WorkerContext,
        endpoints: ChannelEndpoints,
    ) -> crate::Result<Box<dyn SchedulableOperator>> {
        (self.0)(ctx, endpoints)
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
    /// Wake handle for operators that need to notify the executor asynchronously
    /// (e.g., when an in-flight async task completes). Operators that don't need
    /// async waking can ignore this field.
    pub wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
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

/// A factory that creates a typed channel pair for an edge.
///
/// Stored during the build phase (when concrete edge types are known) and
/// invoked during materialization (when worker-local channel endpoints are
/// needed). Wraps a `FnMut` closure that produces a fresh channel pair on each
/// call.
///
/// Pipeline channel factories are stateless and can be called multiple times.
/// Exchange channel factories consume shared materializer state and must only
/// be called once per worker slot — a failed `build()` may leave the factory
/// in a partially consumed state that cannot be retried.
pub struct ChannelFactory(
    Box<
        dyn FnMut(
                &WorkerContext,
                Option<crate::dataflow::channels::wake::WakeHandle>,
            ) -> crate::Result<ChannelPair>
            + Send,
    >,
);

/// Type alias for the channel pair returned by [`ChannelFactory::build`].
pub type ChannelPair = (Box<dyn std::any::Any + Send>, Box<dyn std::any::Any + Send>);

impl ChannelFactory {
    /// Create a new channel factory from a `FnMut` closure.
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
    pub fn build(
        &mut self,
        ctx: &WorkerContext,
        wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
    ) -> crate::Result<ChannelPair> {
        (self.0)(ctx, wake_handle)
    }
}
