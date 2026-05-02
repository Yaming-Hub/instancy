//! Dataflow graph abstractions.
//!
//! This module provides the core types for constructing dataflow graphs:
//! scopes, streams, execution regions, channels, routing strategies,
//! operators, and the dataflow specification builder.

pub mod channel_operators;
pub mod channels;
pub mod dataflow_builder;
pub mod executor;
pub mod graph;
pub mod id;
pub mod operators;
pub mod probe;
pub mod region;
pub mod schedulable;
pub mod scope;
pub mod spec;
pub mod stream;
pub mod wired_operators;

pub use channel_operators::{
    ChannelSinkOperator, ChannelSourceOperator, InputSender, OutputReceiver,
};
#[cfg(feature = "async-io")]
pub use channel_operators::{AsyncInputSender, AsyncOutputReceiver};
pub use channels::{ControlSignal, Envelope, PartitionStrategy, Payload};
pub use dataflow_builder::{DataflowBuilder, DataflowBuilderConfig, LogicalDataflow, OutputPort, Pipe};
pub use executor::{DataflowExecutor, ExecutorConfig};
pub use graph::{DataflowGraph, EdgeInfo, OperatorInfo};
pub use id::DataflowId;
pub use operators::handles::{InputHandle, NotificationHandle, OutputHandle, OutputSession};
pub use operators::input::{ChannelInput, InputEvent, TimestampedInput, VecInput};
pub use operators::output::{OutputEvent, OutputStream, OutputSender};
pub use region::{PlacementPolicy, Region, RegionAllocator, RegionId};
pub use schedulable::{ActivationOutcome, ChannelEndpoints, OperatorFactory, SchedulableOperator};
pub use scope::{ChildScope, RootScope, Scope, ScopeAddr};
pub use spec::{DataflowHandle, DataflowInputs, DataflowSpec};
pub use stream::{StreamEdge, Slot, StreamConnection};
