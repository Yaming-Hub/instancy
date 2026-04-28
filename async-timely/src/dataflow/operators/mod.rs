//! Operator implementations for the dataflow graph.
//!
//! This module provides built-in operators:
//! - [`input`]: Input event types and stream-driven input binding
//! - [`output`]: Output stream emission and handles

pub mod input;
pub mod output;
pub mod handles;
