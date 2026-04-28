//! # async-timely
//!
//! An async, Tokio-based reimplementation of
//! [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow).
//!
//! This crate retains the core concepts of timely dataflow (timestamps, frontiers,
//! progress tracking, capabilities, scopes) while providing async-native execution,
//! pluggable networking, pluggable serialization, and robust error handling.

pub mod error;
pub mod order;
pub mod progress;
