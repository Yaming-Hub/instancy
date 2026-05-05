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
//! - [`mod@concat`]: Merge multiple streams
//! - [`delay`]: Buffer records until frontier advances
//! - [`exchange`]: Hash-based repartitioning
//! - [`rebalance`]: Round-robin redistribution
//! - [`gather`]: Funnel all data to a single worker
//! - [`broadcast`]: Fan-out data to all workers
//! - [`branch`]: Conditional stream splitting (branch + ok_err)
//! - [`feedback`]: Loop-back edges for iterative computation

pub mod binary;
pub mod branch;
pub mod broadcast;
pub mod concat;
pub mod delay;
pub mod exchange;
pub mod feedback;
pub mod gather;
pub mod handles;
pub mod input;
pub mod inspect;
pub mod output;
pub mod probe;
pub mod rebalance;
pub mod unary;

#[cfg(test)]
mod pipeline_tests;
