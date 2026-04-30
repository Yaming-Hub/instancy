//! Operator implementations for the dataflow graph.
//!
//! This module provides built-in operators:
//! - [`input`]: Input event types and stream-driven input binding
//! - [`output`]: Output stream emission and handles
//! - [`handles`]: Input/output handles for operator logic
//! - [`unary`]: Single-input, single-output operator
//! - [`inspect`]: Side-effect observation (pass-through)
//! - [`probe`]: Frontier observation
//! - [`binary`]: Two-input, single-output operator
//! - [`concat`]: Merge multiple streams
//! - [`delay`]: Buffer records until frontier advances
//! - [`exchange`]: Hash-based repartitioning
//! - [`rebalance`]: Round-robin redistribution
//! - [`gather`]: Funnel all data to a single worker
//! - [`broadcast`]: Fan-out data to all workers

pub mod input;
pub mod output;
pub mod handles;
pub mod unary;
pub mod inspect;
pub mod probe;
pub mod binary;
pub mod concat;
pub mod delay;
pub mod exchange;
pub mod rebalance;
pub mod gather;
pub mod broadcast;

#[cfg(test)]
mod pipeline_tests;
