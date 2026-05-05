//! Channel abstractions for dataflow communication.
//!
//! This module provides the message types (envelopes), routing strategies (pact),
//! push/pull abstractions, bounded in-process channels for moving data
//! between operators, and the [`WakeHandle`] notification primitive.

pub mod bounded;
pub mod edge_materializer;
pub mod envelope;
pub mod exchange_channel;
#[cfg(any(test, feature = "test-utils"))]
pub mod mock_network;
#[cfg(feature = "transport")]
pub mod network;
pub mod pact;
pub mod pushpull;
pub mod tee;
pub mod wake;

pub use bounded::{BoundedPull, BoundedPush, bounded_channel, default_channel};
pub use edge_materializer::{EdgeMaterializer, LocalEdgeMaterializer};
pub use envelope::{ControlSignal, Envelope, Payload};
pub use pact::{ExchangeFn, PartitionStrategy, Router};
pub use pushpull::{ChannelPair, Pull, Push};
pub use tee::{TeePush, tee_or_single};
pub use wake::WakeHandle;
