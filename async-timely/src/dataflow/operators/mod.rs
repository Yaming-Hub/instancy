//! Operator implementations for the dataflow graph.
//!
//! This module provides built-in operators:
//! - [`input`]: Input event types and stream-driven input binding
//! - [`output`]: Output stream emission and handles
//! - [`handles`]: Input/output handles for operator logic
//! - [`unary`]: Single-input, single-output operator
//! - [`inspect`]: Side-effect observation (pass-through)
//! - [`probe`]: Frontier observation

pub mod input;
pub mod output;
pub mod handles;
pub mod unary;
pub mod inspect;
pub mod probe;
pub mod binary;
pub mod concat;
pub mod delay;

#[cfg(test)]
mod pipeline_tests;
