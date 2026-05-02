//! # instancy
//!
//! A reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow)
//! with a custom worker thread pool, per-stage dynamic parallelism, structured
//! message envelopes, pluggable networking/serialization, and robust error handling.
//!
//! Key concepts:
//! - **Scope**: A region of the dataflow graph sharing a common timestamp type.
//! - **StreamEdge**: A typed edge connecting operators in the graph.
//! - **Region**: An execution region with its own parallelism level.
//! - **Envelope**: A structured message carrying data, control signals, and metadata.
//! - **Frontier/Antichain**: Progress tracking primitives.
//! - **WorkerPool**: Custom thread pool for synchronous operator execution.
//! - **Providers**: Pluggable transport and execution backends.
//! - **CancellationToken**: Cooperative shutdown signal for dataflows.

pub mod cancellation;
pub mod communication;
pub mod dataflow;
pub mod error;
pub mod execute;
pub mod metrics;
pub mod order;
pub mod progress;
pub mod providers;
pub mod runtime;
pub mod scheduler;
pub mod worker;
pub mod worker_pool;
