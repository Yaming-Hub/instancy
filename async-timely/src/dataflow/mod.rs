//! Dataflow graph abstractions.
//!
//! This module provides the core types for constructing dataflow graphs:
//! scopes, streams, execution regions, channels, routing strategies,
//! operators, and the dataflow specification builder.

pub mod channels;
pub mod operators;
pub mod region;
pub mod scope;
pub mod spec;
pub mod stream;

pub use channels::{ControlSignal, Envelope, PartitionStrategy, Payload};
pub use operators::handles::{InputHandle, NotificationHandle, OutputHandle, OutputSession};
pub use operators::input::{ChannelInput, InputEvent, TimestampedInput, VecInput};
pub use operators::output::{OutputEvent, OutputStream, OutputSender};
pub use region::{PlacementPolicy, Region, RegionAllocator, RegionId};
pub use scope::{ChildScope, RootScope, Scope, ScopeAddr};
pub use spec::{DataflowHandle, DataflowInputs, DataflowSpec};
pub use stream::{DataStream, Slot, StreamConnection};
