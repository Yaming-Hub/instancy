//! # instancy
//!
//! An async reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow)
//! built on [Tokio](https://tokio.rs/). instancy preserves timely's core theoretical model —
//! partially ordered timestamps, progress tracking via pointstamps, frontier-based notifications,
//! and nested scopes — while rearchitecting the runtime for async Rust.
//!
//! ## Key differences from timely-dataflow
//!
//! - **Async execution**: Shared worker pool instead of dedicated OS threads per worker.
//!   Multiple dataflows share threads for better resource utilization. Tokio is used
//!   for transport and async I/O.
//! - **Error handling**: `Result<T, Error>` throughout — operators return errors instead of panicking.
//! - **Cancellation**: Cooperative `CancellationToken` with
//!   `CancellationReason` for diagnosing why a dataflow stopped.
//! - **Pluggable networking**: Application provides connections via a `ConnectionManager` trait —
//!   supports mTLS, custom topologies, and connection pooling.
//! - **Pluggable serialization**: `Codec` trait replaces timely's Abomonation-based approach.
//!   Optional bincode support via `bincode-codec` feature.
//! - **Builder API**: Chainable `DataflowBuilder` API instead of closure-based scope nesting.
//!
//! ## Core concepts
//!
//! - **`DataflowBuilder`**: Constructs a dataflow graph with
//!   typed operators and edges.
//! - **`StreamEdge`**: A typed edge connecting operators,
//!   with chainable methods for `map`, `filter`, `exchange`, `unary`, `binary`, `iterate`, etc.
//! - **Timestamps & Frontiers**: Partially ordered timestamps tracked as antichains.
//!   Operators receive frontier notifications to know when all data for a time has arrived.
//! - **`RuntimeHandle`**: Spawns and manages dataflow execution,
//!   including multi-worker and multi-node clusters.
//! - **`CancellationToken`**: Cooperative shutdown signal
//!   for graceful dataflow termination.
//!
//! ## Feature flags
//!
//! - `tracing` *(default)* — structured logging via the `tracing` crate
//! - `transport` *(default)* — TCP-based cross-node communication
//! - `bincode-codec` — bincode serialization for exchange data
//! - `test-utils` — helpers for testing dataflow programs

// Enable automatic doc(cfg) annotations on docs.rs builds so that
// feature-gated items display which feature enables them.
#![cfg_attr(docsrs, feature(doc_cfg))]
// Prevent regressions: treat broken doc links as errors.
#![deny(rustdoc::broken_intra_doc_links)]
// Inherent to the trait-object-heavy dataflow design (Push/Pull/Codec generics).
#![allow(clippy::type_complexity)]

pub mod cancellation;
pub mod communication;
pub mod dataflow;
pub mod error;
pub mod execute;
pub(crate) mod executor_task;
pub mod metrics;
pub mod order;
pub mod progress;
pub mod providers;
pub mod runtime;
pub mod scheduler;
pub(crate) mod wire;
pub mod worker;
pub(crate) mod worker_pool;

// ── Crate-level re-exports for ergonomic use ──────────────────────────

// Dataflow construction
pub use dataflow::dataflow_builder::IterateResult;
pub use dataflow::graph::DataflowGraph;
pub use dataflow::id::DataflowId;
pub use dataflow::probe::ProbeHandle;
pub use dataflow::stream::StreamEdge;
pub use dataflow::{AsyncInputSender, AsyncOutputReceiver};
pub use dataflow::{ControlReceiver, ControlSender, WorkerControl};
pub use dataflow::{
    DataflowBuilder, DataflowBuilderConfig, LogicalDataflow, OutputPort, Pipe, SharedContext,
};
pub use dataflow::{InputSender, OutputReceiver};

// Runtime
#[cfg(feature = "transport")]
pub use runtime::ClusterCompletion;
#[cfg(feature = "transport")]
pub use runtime::ClusterSpawnedDataflow;
#[cfg(feature = "test-utils")]
pub use runtime::SimpleRuntime;
pub use runtime::{
    DataflowCompletion, IoMode, MultiDataflowCompletion, MultiSpawnedDataflow, RuntimeConfig,
    RuntimeHandle, SpawnOptions, SpawnedDataflow, TokioMode,
};

// Execution / cluster topology / membership
pub use execute::{
    ChannelMembership, ClusterMembership, ClusterTopology, MembershipEvent, NodeConfig,
    NodeDepartureReason,
};

// Cancellation
pub use cancellation::{CancellationReason, CancellationToken};

// Error handling
#[cfg(feature = "transport")]
pub use communication::control_protocol::{ControlProtocolError, HandshakeError};
pub use error::{
    CommunicationError, DataflowError, Error, ProgressError, Result, RuntimeError,
    TopologyError,
};

// Worker types
pub use worker::WorkerId;

// Progress tracking
pub use progress::timestamp::Timestamp;

// Scheduler policies
pub use scheduler::policy::{PriorityPolicy, PriorityWithAgingPolicy, SchedulePolicy};

// Timestamp ordering
pub use order::Product;
