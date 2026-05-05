//! Stage-based operator grouping for fused execution.
//!
//! A **stage** is a set of operators that share the same parallelism (worker count)
//! and are connected exclusively by pipeline (worker-local) channels. Stage boundaries
//! are defined by repartition operators (exchange, gather, broadcast, rebalance).
//!
//! Within a stage, all operators for a given worker are fused into a single schedulable
//! unit — the executor polls them together in topological order, eliminating per-operator
//! scheduling overhead.
//!
//! # Current status
//!
//! This module provides the foundational types. Stage inference (auto-detecting
//! stages from the graph) and multi-stage execution are future work.

use std::fmt;

// ---------------------------------------------------------------------------
// StageId — unique identifier for a stage within a dataflow
// ---------------------------------------------------------------------------

/// Identifies a stage within a dataflow graph.
///
/// Stages are numbered sequentially starting from 0 (the source stage).
/// Each repartition operator creates a new stage boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StageId(pub usize);

impl StageId {
    /// The initial stage (stage 0) — contains source operators and
    /// all operators reachable without crossing a repartition boundary.
    pub const INITIAL: StageId = StageId(0);
}

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Stage({})", self.0)
    }
}

impl From<usize> for StageId {
    fn from(id: usize) -> Self {
        StageId(id)
    }
}

// ---------------------------------------------------------------------------
// FusedActivationOrder — topological order for fused execution
// ---------------------------------------------------------------------------

/// Operator positions in topological order for fused activation.
///
/// When fused execution is enabled, the executor activates operators in this
/// order rather than using the FIFO ready_queue. This ensures data flows
/// through the pipeline in a single sweep (source → ... → sink) rather than
/// requiring multiple sweeps for downstream operators to observe upstream output.
#[derive(Debug, Clone)]
pub struct FusedActivationOrder {
    /// Operator positions (indices into the executor's `operators` vec)
    /// in topological order.
    positions: Vec<usize>,
}

impl FusedActivationOrder {
    /// Create a new fused activation order from topological positions.
    ///
    /// `positions` must contain operator positions in topological order
    /// (every operator appears after all its predecessors).
    pub fn new(positions: Vec<usize>) -> Self {
        Self { positions }
    }

    /// Get the operator positions in topological order.
    pub fn positions(&self) -> &[usize] {
        &self.positions
    }

    /// Number of operators in the fused order.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the fused order is empty.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}
