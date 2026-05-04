//! Cross-process integration testing framework for instancy.
//!
//! This crate provides:
//! - A control protocol for coordinator ↔ node communication
//! - Predefined dataflow builders for integration tests
//! - A `DataflowAgent` actor (dactor) that manages instancy runtimes in node processes
//! - A `TestCoordinator` that orchestrates multi-process test scenarios
//!
//! **Not published** — this crate exists solely for integration testing.

pub mod protocol;
pub mod dataflows;
pub mod node_actor;
pub mod coordinator;
