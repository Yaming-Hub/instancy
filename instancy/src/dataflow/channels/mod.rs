//! Channel abstractions for dataflow communication.
//!
//! This module provides the message types (envelopes), routing strategies (pact),
//! push/pull abstractions, bounded in-process channels for moving data
//! between operators, and the [`WakeHandle`] notification primitive.

pub mod bounded;
pub mod envelope;
pub mod pact;
pub mod pushpull;
pub mod tee;
pub mod wake;

pub use bounded::{bounded_channel, default_channel, BoundedPull, BoundedPush};
pub use envelope::{ControlSignal, Envelope, Payload};
pub use pact::{ExchangeFn, PartitionStrategy, Router};
pub use pushpull::{ChannelPair, Pull, Push};
pub use tee::{tee_or_single, TeePush};
pub use wake::WakeHandle;
