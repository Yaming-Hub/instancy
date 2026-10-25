//! Dataflow graph abstractions.
//!
//! Operators are built only through [`DataflowBuilder`] and its pipes, as
//! operator shells that count every record at their ports (design §5.2,
//! construction by type). The executor, the operator and factory traits and
//! raw channel endpoints are internal:
//!
//! ```compile_fail,E0432
//! use instancy::dataflow::SchedulableOperator;
//! ```
//!
//! ```compile_fail,E0432
//! use instancy::dataflow::DataflowExecutor;
//! ```

pub mod channel_operators;
pub mod channels;
pub mod context;
pub(crate) mod control;
pub mod dataflow_builder;
pub(crate) mod executor;
pub mod graph;
pub mod id;
pub mod operators;
pub(crate) mod plan;
pub mod probe;
#[allow(dead_code)]
pub(crate) mod schedulable;
pub mod scope;
pub(crate) mod shell;
pub(crate) mod spec;
pub mod stage;
pub mod stream;
#[allow(dead_code)]
pub(crate) mod wired_operators;

pub use channel_operators::{AsyncInputSender, AsyncOutputReceiver};
pub use channel_operators::{InputSender, OutputReceiver};
pub use channels::{ControlSignal, Envelope, PartitionStrategy, Payload};
pub use context::SharedContext;
pub use control::{ControlReceiver, ControlSender, WorkerControl};
pub use dataflow_builder::{
    DataflowBuilder, DataflowBuilderConfig, IterateInner, LogicalDataflow, OutputPort, Pipe,
};
pub use graph::{ChannelKind, DataflowGraph, Delivery, EdgeInfo, OperatorInfo};
pub use id::DataflowId;
pub use operators::handles::{InputHandle, NotificationHandle, OutputHandle, OutputSession};
pub use operators::input::{ChannelInput, InputEvent, TimestampedInput, VecInput};
pub use operators::output::{OutputEvent, OutputSender, OutputStream};
pub use scope::{ChildScope, RootScope, Scope, ScopeAddr};
pub use shell::Retained;
pub use spec::{DataflowHandle, DataflowInputs, DataflowSpec};
pub use stage::{FusedActivationOrder, StageId, StageInfo, infer_stages};
pub use stream::{Slot, StreamConnection, StreamEdge};
