//! Cross-process integration testing framework for instancy.
//!
//! This crate provides:
//! - A control protocol for coordinator ↔ node communication
//! - Predefined dataflow builders for integration tests
//! - A `DataflowAgent` actor (dactor) that manages instancy runtimes in node processes
//! - A `TestCoordinator` that orchestrates multi-process test scenarios
//! - A `Relay` that forwards node-to-node connections and injects faults
//! - `PeerLinks`, which pairs the Shared transport's connections between nodes
//! - Workloads: dataflow inputs with the exact output each worker must produce
//!
//! **Not published** — this crate exists solely for integration testing.

pub mod coordinator;
pub mod dataflows;
pub mod node_actor;
pub mod peer_link;
pub mod protocol;
pub mod relay;
pub mod test_helpers;
pub mod workloads;
