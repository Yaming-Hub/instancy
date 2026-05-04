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
//! - `async-io` — async input/output channels
//! - `test-utils` — helpers for testing dataflow programs

// Enable automatic doc(cfg) annotations on docs.rs builds so that
// feature-gated items display which feature enables them.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod cancellation;
pub mod communication;
pub mod dataflow;
pub mod error;
pub mod execute;
pub mod executor_task;
pub mod metrics;
pub mod order;
pub mod progress;
pub mod providers;
pub mod runtime;
pub mod scheduler;
pub mod worker;
pub mod worker_pool;

// ── Crate-level re-exports for ergonomic use ──────────────────────────

// Dataflow construction
pub use dataflow::{DataflowBuilder, DataflowBuilderConfig, LogicalDataflow, OutputPort, Pipe};
pub use dataflow::id::DataflowId;
pub use dataflow::dataflow_builder::IterateResult;
pub use dataflow::stream::StreamEdge;
pub use dataflow::{InputSender, OutputReceiver};
#[cfg(feature = "async-io")]
pub use dataflow::{AsyncInputSender, AsyncOutputReceiver};

// Runtime
pub use runtime::{
    RuntimeConfig, RuntimeHandle, SimpleRuntime, SpawnedDataflow, DataflowCompletion,
};

// Cancellation
pub use cancellation::{CancellationToken, CancellationReason};

// Error handling
pub use error::{Error, Result};

// Timestamp ordering
pub use order::Product;
