//! Dataflow graph abstractions.
//!
//! This module provides the core types for constructing dataflow graphs:
//! scopes, streams, execution regions, channels, and routing strategies.

pub mod channels;
pub mod region;
pub mod scope;
pub mod stream;

pub use channels::{ControlSignal, Envelope, PartitionStrategy, Payload};
pub use region::{PlacementPolicy, Region, RegionAllocator, RegionId};
pub use scope::{ChildScope, RootScope, Scope, ScopeAddr};
pub use stream::{Port, Stream, StreamConnection};
