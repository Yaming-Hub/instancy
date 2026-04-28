//! Channel abstractions for dataflow communication.
//!
//! This module provides the message types (envelopes), routing strategies (pact),
//! and push/pull abstractions for moving data between operators.

pub mod envelope;
pub mod pact;
pub mod pushpull;

pub use envelope::{ControlSignal, Envelope, Payload};
pub use pact::{ExchangeFn, PartitionStrategy, Router};
pub use pushpull::{ChannelPair, Pull, Push};
