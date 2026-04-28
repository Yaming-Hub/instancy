//! # async-timely
//!
//! A reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow)
//! with a custom worker thread pool, per-stage dynamic parallelism, structured
//! message envelopes, pluggable networking/serialization, and robust error handling.
//!
//! Key concepts:
//! - **Scope**: A region of the dataflow graph sharing a common timestamp type.
//! - **Stream**: A typed edge connecting operators in the graph.
//! - **Region**: An execution region with its own parallelism level.
//! - **Envelope**: A structured message carrying data, control signals, and metadata.
//! - **Frontier/Antichain**: Progress tracking primitives.

pub mod communication;
pub mod dataflow;
pub mod error;
pub mod order;
pub mod progress;
