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

use crate::dataflow::region::RegionId;
use crate::error::Result;

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

    /// The execution region this operator belongs to.
    fn region_id(&self) -> RegionId;

    /// Close all input channels to signal no more data will arrive.
    ///
    /// Called by the executor when upstream operators have completed or
    /// the dataflow is shutting down.
    fn close_inputs(&mut self);
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
/// The `ChannelEndpoints` argument provides the operator's input pullers and
/// output pushers as type-erased `Box<dyn Any>` values. The factory downcasts
/// them to the concrete channel types.
pub type OperatorFactory = Box<dyn FnOnce(ChannelEndpoints) -> Box<dyn SchedulableOperator> + Send>;

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

/// A factory that creates a typed channel pair for an edge.
///
/// Takes `(capacity, wake_handle)` where:
/// - `capacity` is the channel buffer size
/// - `wake_handle` is an optional [`WakeHandle`] for async executor notifications.
///   When provided, channels notify the handle on push, close, drop, and
///   when pulling frees capacity (backpressure relief).
///
/// Returns `(Box<dyn Any + Send>, Box<dyn Any + Send>)` where the first
/// element is a `Box<dyn Push<T, D, M>>` and the second is a `Box<dyn Pull<T, D, M>>`.
pub type ChannelFactory = Box<dyn FnOnce(usize, Option<crate::dataflow::channels::wake::WakeHandle>) -> (Box<dyn std::any::Any + Send>, Box<dyn std::any::Any + Send>) + Send>;
