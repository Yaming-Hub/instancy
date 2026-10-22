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

use std::any::Any;
use std::sync::Arc;

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

    /// Stop accepting data from outside the dataflow (graceful drain).
    ///
    /// Called once when a drain begins. External input sources stop accepting
    /// new events but still deliver everything already accepted, then finish
    /// on their own. Operators without external inputs ignore this (default).
    fn close_external_input(&mut self) {}

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
    /// Output pushers grouped by output port.
    ///
    /// `output_pushers[port]` is a `Vec` of all pushers for that port
    /// (multiple pushers when the output fans out to multiple downstream
    /// consumers). Each inner element is a `Box<dyn Push<T, D, M>>`.
    pub output_pushers: Vec<Vec<Box<dyn std::any::Any + Send>>>,
    /// Wake handle for operators that need to notify the executor asynchronously
    /// (e.g., when an in-flight async task completes). Operators that don't need
    /// async waking can ignore this field.
    pub wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
    /// Progress reporters for this operator, keyed by `(operator_index, output_port)`.
    ///
    /// Populated during materialization from the `SubgraphBuilder` clone used
    /// to build the worker's progress tracker, so factories can bind operators
    /// to the same progress state the executor will later observe.
    pub progress_reporters: Option<Arc<dyn Any + Send + Sync>>,
}

impl ChannelEndpoints {
    /// Returns the progress reporter for the given operator output, if one was
    /// explicitly passed for this materialization.
    pub fn progress_reporter<T: crate::progress::timestamp::Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<crate::progress::operate::ProgressReporter<T>> {
        self.progress_reporters
            .as_ref()?
            .downcast_ref::<crate::progress::subgraph::MaterializationReporters>()?
            .internal::<T>(operator, output)
    }

    /// Returns the in-flight reporter for a boundary edge whose **source** is
    /// `(operator, output)` — i.e. the produced reporter the sender's output
    /// pusher should record into. `T` is the *channel's* timestamp (which is
    /// `Product<…>` for an exchange inside a loop, not the worker tracker's `T`).
    pub fn inflight_reporter_by_source<T: crate::progress::timestamp::Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<crate::progress::operate::ProgressReporter<T>> {
        self.progress_reporters
            .as_ref()?
            .downcast_ref::<crate::progress::subgraph::MaterializationReporters>()?
            .inflight_by_source::<T>(operator, output)
    }

    /// Returns the in-flight reporter for a boundary edge whose **target** is
    /// `(operator, input)` — i.e. the consumed reporter the consumer's input
    /// puller should record into.
    pub fn inflight_reporter_by_target<T: crate::progress::timestamp::Timestamp>(
        &self,
        operator: usize,
        input: usize,
    ) -> Option<crate::progress::operate::ProgressReporter<T>> {
        self.progress_reporters
            .as_ref()?
            .downcast_ref::<crate::progress::subgraph::MaterializationReporters>()?
            .inflight_by_target::<T>(operator, input)
    }

    /// Wire the **producing** side of a boundary (exchange) edge: if an
    /// in-flight reporter is registered for the edge sourced at
    /// `(operator, output)`, attach it to `pusher` so every batch it sends
    /// records `+count`.
    ///
    /// This is the single, canonical place to source an exchange's producing
    /// side. Centralizing the lookup+attach in one call (instead of the two
    /// separate steps at each operator factory) means the `set_inflight_reporter`
    /// can't be forgotten after the lookup: forgetting the `+count` while the
    /// consumer still records `-count` drives the in-flight net negative and
    /// hangs the dataflow (the `iter_var.exchange` failure mode). Any future
    /// exchange-sourcing operator factory should call this rather than re-deriving
    /// the pattern. No-op for non-exchange outputs.
    pub fn wire_inflight_source<T, D, M>(
        &self,
        operator: usize,
        output: usize,
        pusher: &mut (dyn crate::dataflow::channels::pushpull::Push<T, D, M> + '_),
    ) where
        T: crate::progress::timestamp::Timestamp,
    {
        if let Some(reporter) = self.inflight_reporter_by_source::<T>(operator, output) {
            pusher.set_inflight_reporter(reporter);
        }
    }

    /// Wire the **consuming** side of a boundary (exchange) edge: if an in-flight
    /// reporter is registered for the edge targeted at `(operator, input)`,
    /// attach it to `puller` so every delivered batch records `-count`.
    ///
    /// Counterpart to [`wire_inflight_source`](Self::wire_inflight_source); the
    /// producing `+count` and this consuming `-count` must be wired symmetrically.
    /// No-op for non-exchange inputs.
    pub fn wire_inflight_target<T, D, M>(
        &self,
        operator: usize,
        input: usize,
        puller: &mut (dyn crate::dataflow::channels::pushpull::Pull<T, D, M> + '_),
    ) where
        T: crate::progress::timestamp::Timestamp,
    {
        if let Some(reporter) = self.inflight_reporter_by_target::<T>(operator, input) {
            puller.set_inflight_reporter(reporter);
        }
    }

    /// Take all output pushers for the given port, leaving the slot empty.
    ///
    /// Returns an empty vec if the port index is out of range.
    pub fn take_port_pushers(&mut self, port: usize) -> Vec<Box<dyn std::any::Any + Send>> {
        if port < self.output_pushers.len() {
            std::mem::take(&mut self.output_pushers[port])
        } else {
            Vec::new()
        }
    }
}

impl std::fmt::Debug for ChannelEndpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_pushers: usize = self.output_pushers.iter().map(|v| v.len()).sum();
        f.debug_struct("ChannelEndpoints")
            .field("input_count", &self.input_pullers.len())
            .field("output_ports", &self.output_pushers.len())
            .field("output_pushers_total", &total_pushers)
            .field("has_progress_reporters", &self.progress_reporters.is_some())
            .finish()
    }
}

/// Group `(port, item)` tuples into a `Vec<Vec<item>>` indexed by port.
///
/// The input must be pre-sorted by port index. Returns one inner vec per
/// distinct port, ordered by ascending port index.
pub fn group_by_port<T>(sorted_items: Vec<(usize, T)>) -> Vec<Vec<T>> {
    let mut result: Vec<Vec<T>> = Vec::new();
    for (port, item) in sorted_items {
        // Extend to cover any gaps (e.g., port 0, then port 2 — port 1 gets an empty vec).
        while result.len() <= port {
            result.push(Vec::new());
        }
        result[port].push(item);
    }
    result
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
///
/// The factory is built from a typed closure, so the progress checker (debug
/// builds and the `progress-check` feature) can wrap every channel it makes
/// (see [`build_checked`](Self::build_checked)).
pub struct ChannelFactory {
    build: Box<
        dyn FnMut(
                &WorkerContext,
                Option<crate::dataflow::channels::wake::WakeHandle>,
            ) -> crate::Result<ChannelPair>
            + Send,
    >,
    /// Wraps a pair this factory built (monomorphized for its types).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_wrap: crate::progress::check::WrapFn,
    /// Projects the channel's timestamp onto the root timestamp (a
    /// `check::Projector<S>`); set for channels inside `iterate`.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_projector: Option<Arc<dyn Any + Send + Sync>>,
    /// Copies each pushed record becomes (target workers of a broadcast).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_fanout: i64,
    /// Whether the channel routes records between workers.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_cross_worker: bool,
}

/// Type alias for the channel pair returned by [`ChannelFactory::build`].
///
/// The first element is a `Box<dyn Push<T, D, M>>` (the output/sender side),
/// and the second is a `Box<dyn Pull<T, D, M>>` (the input/receiver side).
/// Both are type-erased via `Any` because the concrete types depend on the
/// data type `D` and timestamp type `T`, which are not known at the
/// factory-storage level.
pub type ChannelPair = (Box<dyn std::any::Any + Send>, Box<dyn std::any::Any + Send>);

/// The typed channel pair a [`ChannelFactory`] closure returns.
pub(crate) type TypedChannelPair<T, D, M> = (
    Box<dyn crate::dataflow::channels::pushpull::Push<T, D, M>>,
    Box<dyn crate::dataflow::channels::pushpull::Pull<T, D, M>>,
);

impl ChannelFactory {
    /// Create a new channel factory from a `FnMut` closure returning typed
    /// endpoints.
    pub(crate) fn new<T, D, M>(
        mut factory: impl FnMut(
            &WorkerContext,
            Option<crate::dataflow::channels::wake::WakeHandle>,
        ) -> crate::Result<TypedChannelPair<T, D, M>>
        + Send
        + 'static,
    ) -> Self
    where
        T: crate::progress::timestamp::Timestamp,
        D: 'static,
        M: 'static,
    {
        Self {
            build: Box::new(move |ctx, wake| {
                let (push, pull) = factory(ctx, wake)?;
                Ok((
                    Box::new(push) as Box<dyn Any + Send>,
                    Box::new(pull) as Box<dyn Any + Send>,
                ))
            }),
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_wrap: crate::progress::check::wrap_pair::<T, D, M>,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_projector: None,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_fanout: 1,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_cross_worker: false,
        }
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
        (self.build)(ctx, wake_handle)
    }

    /// [`build`](Self::build) for the edge `source -> target`, letting the
    /// progress checker wrap the pair when `check` holds its context.
    ///
    /// Every materialization point builds channels through this method, so
    /// the checker observes every channel. Without the checker compiled in,
    /// `check` is `()` and this is exactly `build`.
    pub(crate) fn build_checked(
        &mut self,
        ctx: &WorkerContext,
        wake_handle: Option<crate::dataflow::channels::wake::WakeHandle>,
        check: &crate::progress::CheckCtx,
        source: crate::dataflow::stream::Slot,
        target: crate::dataflow::stream::Slot,
    ) -> crate::Result<ChannelPair> {
        let pair = self.build(ctx, wake_handle)?;
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        if let Some(check) = check {
            let site = crate::progress::check::ChannelSite {
                ctx: check,
                source: (source.operator_index, source.slot_index),
                target: (target.operator_index, target.slot_index),
            };
            return Ok((self.check_wrap)(
                pair,
                &site,
                self.check_projector.as_deref(),
                self.check_fanout,
                self.check_cross_worker,
            ));
        }
        #[cfg(not(any(debug_assertions, feature = "progress-check")))]
        let _ = (check, source, target);
        Ok(pair)
    }

    /// Declare that this channel routes records between workers and that
    /// every record pushed into it is delivered `fanout` times (the target
    /// worker count for a broadcast, 1 for an exchange). Only the progress
    /// checker uses this; without it compiled in, a no-op.
    #[cfg_attr(
        not(any(debug_assertions, feature = "progress-check")),
        allow(unused_mut)
    )]
    pub(crate) fn with_check_cross_worker(mut self, fanout: usize) -> Self {
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        {
            self.check_fanout = fanout.max(1) as i64;
            self.check_cross_worker = true;
        }
        #[cfg(not(any(debug_assertions, feature = "progress-check")))]
        let _ = fanout;
        self
    }

    /// Give the checker the projection of this channel's timestamp onto the
    /// root timestamp, unless a deeper scope already did. Only the progress
    /// checker uses this; without it compiled in, a no-op.
    pub(crate) fn set_check_projector<S: crate::progress::timestamp::Timestamp>(
        &mut self,
        projector: &Arc<dyn Fn(&S) -> Box<dyn Any + Send> + Send + Sync>,
    ) {
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        if self.check_projector.is_none() {
            self.check_projector = Some(Arc::new(projector.clone()));
        }
        #[cfg(not(any(debug_assertions, feature = "progress-check")))]
        let _ = projector;
    }

    /// [`set_check_projector`](Self::set_check_projector), by value.
    pub(crate) fn with_check_projector<S: crate::progress::timestamp::Timestamp>(
        mut self,
        projector: &Arc<dyn Fn(&S) -> Box<dyn Any + Send> + Send + Sync>,
    ) -> Self {
        self.set_check_projector(projector);
        self
    }
}
