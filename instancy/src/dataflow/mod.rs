//! Dataflow graph abstractions.

pub mod channel_operators;
pub mod channels;
pub mod context;
pub mod control;
pub mod dataflow_builder;
pub mod executor;
pub mod graph;
pub mod id;
pub mod operators;
pub mod probe;
pub mod schedulable;
pub mod scope;
pub mod spec;
pub mod stage;
pub mod stream;
pub mod wired_operators;

pub use channel_operators::{AsyncInputSender, AsyncOutputReceiver};
pub use channel_operators::{
    ChannelSinkOperator, ChannelSourceOperator, InputSender, OutputReceiver,
};
pub use channels::{ControlSignal, Envelope, PartitionStrategy, Payload};
pub use context::SharedContext;
pub use control::{ControlReceiver, ControlSender, WorkerControl};
pub use dataflow_builder::{
    DataflowBuilder, DataflowBuilderConfig, LogicalDataflow, OutputPort, Pipe,
};
pub use executor::{DataflowExecutor, ExecutorConfig};
pub use graph::{ChannelKind, DataflowGraph, EdgeInfo, OperatorInfo};
pub use id::DataflowId;
pub use operators::handles::{InputHandle, NotificationHandle, OutputHandle, OutputSession};
pub use operators::input::{ChannelInput, InputEvent, TimestampedInput, VecInput};
pub use operators::output::{OutputEvent, OutputSender, OutputStream};
pub use schedulable::{ActivationOutcome, ChannelEndpoints, OperatorFactory, SchedulableOperator};
pub use scope::{ChildScope, RootScope, Scope, ScopeAddr};
pub use spec::{DataflowHandle, DataflowInputs, DataflowSpec};
pub use stage::{FusedActivationOrder, StageId, StageInfo, infer_stages};
pub use stream::{Slot, StreamConnection, StreamEdge};
