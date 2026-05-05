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
//! # Stage inference
//!
//! The [`infer_stages`] function auto-detects stages from the dataflow graph by walking
//! operators in topological order and splitting at exchange edges. Each operator is
//! assigned to exactly one stage, and stages are numbered sequentially (0, 1, 2, ...).

use std::collections::HashMap;
use std::fmt;

use crate::dataflow::graph::DataflowGraph;
use crate::error::Result;

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

// ---------------------------------------------------------------------------
// StageInfo — metadata about an inferred stage
// ---------------------------------------------------------------------------

/// Metadata about a single stage in the dataflow graph.
///
/// A stage groups operators connected by pipeline edges (no exchange between them).
/// All operators in a stage share the same parallelism (worker count) and are
/// fused into a single schedulable unit per worker.
#[derive(Debug, Clone)]
pub struct StageInfo {
    /// Unique stage identifier (sequential, starting from 0).
    pub id: StageId,
    /// Target parallelism (cluster-wide worker count) for this stage.
    /// `None` means the stage inherits the dataflow's default parallelism.
    pub parallelism: Option<usize>,
    /// Operator indices belonging to this stage, in topological order.
    pub operator_indices: Vec<usize>,
    /// The fused activation order for this stage's operators.
    pub fused_order: FusedActivationOrder,
}

impl StageInfo {
    /// Number of operators in this stage.
    pub fn operator_count(&self) -> usize {
        self.operator_indices.len()
    }
}

// ---------------------------------------------------------------------------
// Stage inference — auto-detect stages from the graph
// ---------------------------------------------------------------------------

/// Infer stages from a dataflow graph by splitting at exchange edges.
///
/// The algorithm:
/// 1. Compute topological order of operators (excluding feedback edges).
/// 2. Walk operators in topological order. Each operator inherits its stage
///    from its predecessors via pipeline edges. If an operator is the target
///    of an exchange edge, it starts a new stage.
/// 3. Operators with no incoming edges (sources) are placed in stage 0.
///
/// # Returns
///
/// A vector of `StageInfo`, one per stage, sorted by stage ID. Each stage
/// contains its operators in topological order.
///
/// # Errors
///
/// Returns an error if the graph contains a cycle in its non-feedback edges.
pub fn infer_stages(graph: &DataflowGraph) -> Result<Vec<StageInfo>> {
    let topo_order = graph.topological_order()?;

    if topo_order.is_empty() {
        return Ok(Vec::new());
    }

    // Build a set of exchange-edge targets for quick lookup.
    // An operator that is the target of a forward exchange edge starts a new stage.
    // Feedback exchange edges are NOT included here because feedback loops don't
    // represent parallelism boundaries — they loop data back within the same
    // iterative computation.
    let mut exchange_targets: HashMap<usize, bool> = HashMap::new();
    for edge in graph.edges() {
        if edge.is_exchange() {
            exchange_targets.insert(edge.target.operator_index, true);
        }
    }

    // Assign each operator to a stage.
    // Strategy: walk in topo order. If an operator is an exchange target, it
    // gets a new stage ID. Otherwise, it inherits the MAX stage of its pipeline
    // predecessors (max ensures it joins the latest stage, preserving the
    // invariant that all pipeline-connected ops share a stage).
    // If it has no predecessors (source), it goes in stage 0.
    let mut op_stage: HashMap<usize, usize> = HashMap::new();
    let mut next_stage_id: usize = 0;

    // When multiple exchange targets share the same upstream source stage AND
    // the same target parallelism, they should share the same downstream stage.
    // Track: (source_stage, target_parallelism) → new_stage.
    let mut boundary_stage_map: HashMap<(usize, Option<usize>), usize> = HashMap::new();

    for &op_idx in &topo_order {
        if exchange_targets.contains_key(&op_idx) {
            // This operator is the target of an exchange edge — new stage boundary.
            // Determine the source stage from the exchange edge's source operator.
            let mut source_stage: Option<usize> = None;
            for edge in graph.edges() {
                if edge.target.operator_index == op_idx && edge.is_exchange() {
                    if let Some(&src_stg) = op_stage.get(&edge.source.operator_index) {
                        source_stage = Some(match source_stage {
                            Some(existing) => existing.max(src_stg),
                            None => src_stg,
                        });
                    }
                }
            }

            // Use boundary_stage_map to ensure all exchange targets from the same
            // source stage with the same parallelism share the same downstream stage.
            let src = source_stage.unwrap_or(0);
            let par = graph.exchange_parallelism(op_idx);
            let stage = *boundary_stage_map.entry((src, par)).or_insert_with(|| {
                if next_stage_id == 0 {
                    next_stage_id = 1;
                }
                let s = next_stage_id;
                next_stage_id += 1;
                s
            });

            op_stage.insert(op_idx, stage);
        } else {
            // Not an exchange target — inherit MAX stage from pipeline predecessors.
            let mut max_stage: Option<usize> = None;

            for edge in graph.edges() {
                if edge.target.operator_index == op_idx && !edge.is_exchange() {
                    if let Some(&pred_stage) = op_stage.get(&edge.source.operator_index) {
                        max_stage = Some(match max_stage {
                            Some(existing) => existing.max(pred_stage),
                            None => pred_stage,
                        });
                    }
                }
            }

            let stage = max_stage.unwrap_or(0);
            if next_stage_id == 0 {
                next_stage_id = 1;
            }
            op_stage.insert(op_idx, stage);
        }
    }

    // Group operators by stage, preserving topological order within each stage.
    let num_stages = next_stage_id;
    let mut stage_ops: Vec<Vec<usize>> = vec![Vec::new(); num_stages];
    for &op_idx in &topo_order {
        let stage = op_stage[&op_idx];
        stage_ops[stage].push(op_idx);
    }

    // Determine parallelism for each stage.
    // A stage's parallelism comes from the exchange operator that created it.
    // The exchange operator (the target of the exchange edge) stores its
    // target_parallelism via set_exchange_parallelism().
    let mut stage_parallelism: Vec<Option<usize>> = vec![None; num_stages];
    for edge in graph.edges() {
        if edge.is_exchange() {
            let target_op = edge.target.operator_index;
            if let Some(&stage_id) = op_stage.get(&target_op) {
                if let Some(par) = graph.exchange_parallelism(target_op) {
                    stage_parallelism[stage_id] = Some(par);
                }
            }
        }
    }

    // Build StageInfo for each non-empty stage.
    let stages: Vec<StageInfo> = stage_ops
        .into_iter()
        .enumerate()
        .filter(|(_, ops)| !ops.is_empty())
        .map(|(id, ops)| {
            let fused_order = FusedActivationOrder::new(ops.clone());
            StageInfo {
                id: StageId(id),
                parallelism: stage_parallelism[id],
                operator_indices: ops,
                fused_order,
            }
        })
        .collect();

    Ok(stages)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::graph::{EdgeInfo, OperatorInfo};
    use crate::dataflow::region::RegionId;
    use crate::dataflow::stream::Slot;

    fn make_op(index: usize, name: &str) -> OperatorInfo {
        OperatorInfo::new(index, name.to_string(), RegionId(0), 1, 1)
    }

    fn pipeline_edge(src: usize, tgt: usize) -> EdgeInfo {
        EdgeInfo::new(
            Slot::new(src, 0),
            Slot::new(tgt, 0),
            RegionId(0),
            RegionId(0),
        )
    }

    fn exchange_edge(src: usize, tgt: usize) -> EdgeInfo {
        EdgeInfo::exchange(
            Slot::new(src, 0),
            Slot::new(tgt, 0),
            RegionId(0),
            RegionId(0),
        )
    }

    #[test]
    fn test_infer_stages_empty_graph() {
        let graph = DataflowGraph::new();
        let stages = infer_stages(&graph).unwrap();
        assert!(stages.is_empty());
    }

    #[test]
    fn test_infer_stages_single_stage_linear() {
        // source(0) → map(1) → sink(2), all pipeline
        let mut graph = DataflowGraph::new();
        graph.register_operator(make_op(0, "source")).unwrap();
        graph.register_operator(make_op(1, "map")).unwrap();
        graph.register_operator(make_op(2, "sink")).unwrap();
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(pipeline_edge(1, 2));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].id, StageId(0));
        assert_eq!(stages[0].operator_indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_infer_stages_two_stages_with_exchange() {
        // source(0) → map(1) → [exchange] → agg(2) → sink(3)
        let mut graph = DataflowGraph::new();
        graph.register_operator(make_op(0, "source")).unwrap();
        graph.register_operator(make_op(1, "map")).unwrap();
        graph.register_operator(make_op(2, "agg")).unwrap();
        graph.register_operator(make_op(3, "sink")).unwrap();
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(exchange_edge(1, 2));
        graph.add_edge(pipeline_edge(2, 3));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);

        assert_eq!(stages[0].id, StageId(0));
        assert_eq!(stages[0].operator_indices, vec![0, 1]);

        assert_eq!(stages[1].id, StageId(1));
        assert_eq!(stages[1].operator_indices, vec![2, 3]);
    }

    #[test]
    fn test_infer_stages_three_stages() {
        // 0 → 1 → [exchange] → 2 → 3 → [exchange] → 4
        let mut graph = DataflowGraph::new();
        for i in 0..5 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(exchange_edge(1, 2));
        graph.add_edge(pipeline_edge(2, 3));
        graph.add_edge(exchange_edge(3, 4));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 3);

        assert_eq!(stages[0].operator_indices, vec![0, 1]);
        assert_eq!(stages[1].operator_indices, vec![2, 3]);
        assert_eq!(stages[2].operator_indices, vec![4]);
    }

    #[test]
    fn test_infer_stages_diamond_same_stage() {
        // Diamond: 0 → 1, 0 → 2, 1 → 3, 2 → 3 (all pipeline)
        let mut graph = DataflowGraph::new();
        for i in 0..4 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(pipeline_edge(0, 2));
        graph.add_edge(pipeline_edge(1, 3));
        graph.add_edge(pipeline_edge(2, 3));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].operator_indices, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_infer_stages_multiple_sources() {
        // Two sources merging: 0 → 2, 1 → 2 (pipeline), 2 → [exchange] → 3
        let mut graph = DataflowGraph::new();
        for i in 0..4 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 2));
        graph.add_edge(pipeline_edge(1, 2));
        graph.add_edge(exchange_edge(2, 3));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].operator_indices, vec![0, 1, 2]);
        assert_eq!(stages[1].operator_indices, vec![3]);
    }

    #[test]
    fn test_infer_stages_fused_order_matches_topo() {
        // Verify that fused_order.positions() matches operator_indices
        let mut graph = DataflowGraph::new();
        for i in 0..3 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(pipeline_edge(1, 2));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages[0].fused_order.positions(), &[0, 1, 2]);
    }

    #[test]
    fn test_infer_stages_single_operator() {
        let mut graph = DataflowGraph::new();
        graph.register_operator(make_op(0, "source")).unwrap();

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].operator_indices, vec![0]);
    }

    #[test]
    fn test_infer_stages_convergent_mixed_stages() {
        // Op 0 (stage 0) → pipeline → Op 3
        // Op 0 → pipeline → Op 1 → [exchange] → Op 2 (stage 1) → pipeline → Op 3
        // Op 3 should be in stage 1 (max of predecessors)
        let mut graph = DataflowGraph::new();
        for i in 0..4 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(exchange_edge(1, 2));
        graph.add_edge(pipeline_edge(2, 3));
        graph.add_edge(pipeline_edge(0, 3)); // direct pipeline from stage 0

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].operator_indices, vec![0, 1]);
        // Op 3 must be in stage 1 (inherits max from predecessors: stage 0 and stage 1)
        assert_eq!(stages[1].operator_indices, vec![2, 3]);
    }

    #[test]
    fn test_infer_stages_multiple_exchange_targets_share_stage() {
        // Op 0 → [exchange] → Op 1
        // Op 0 → [exchange] → Op 2
        // Both Op 1 and Op 2 should be in the same stage
        let mut graph = DataflowGraph::new();
        for i in 0..3 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        graph.add_edge(exchange_edge(0, 2));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].operator_indices, vec![0]);
        // Both targets share stage 1
        let mut stage1_ops = stages[1].operator_indices.clone();
        stage1_ops.sort();
        assert_eq!(stage1_ops, vec![1, 2]);
    }

    #[test]
    fn test_infer_stages_feedback_exchange_does_not_create_boundary() {
        // Op 0 → pipeline → Op 1 → pipeline → Op 2
        // Feedback exchange: 2 → 0 (loop back)
        // Feedback edges don't create stage boundaries — they represent
        // iterative loops, not parallelism changes.
        let mut graph = DataflowGraph::new();
        graph.register_operator(make_op(0, "source")).unwrap();
        graph.register_operator(make_op(1, "map")).unwrap();
        graph.register_operator(make_op(2, "sink")).unwrap();
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(pipeline_edge(1, 2));
        graph.add_feedback_edge(exchange_edge(2, 0));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].id, StageId(0));
        assert_eq!(stages[0].operator_indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_infer_stages_disconnected_subgraphs() {
        // Two disconnected pipelines: 0→1 and 2→3
        let mut graph = DataflowGraph::new();
        for i in 0..4 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(pipeline_edge(0, 1));
        graph.add_edge(pipeline_edge(2, 3));

        let stages = infer_stages(&graph).unwrap();
        // All sources default to stage 0, so all end up in stage 0
        assert_eq!(stages.len(), 1);
        let mut ops = stages[0].operator_indices.clone();
        ops.sort();
        assert_eq!(ops, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_infer_stages_fan_out_exchange_then_merge() {
        // 0 → [exchange] → 1, 0 → [exchange] → 2, 1 → pipeline → 3, 2 → pipeline → 3
        // All of 1, 2, 3 should be in the same stage (stage 1)
        let mut graph = DataflowGraph::new();
        for i in 0..4 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        graph.add_edge(exchange_edge(0, 2));
        graph.add_edge(pipeline_edge(1, 3));
        graph.add_edge(pipeline_edge(2, 3));

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].operator_indices, vec![0]);
        let mut stage1_ops = stages[1].operator_indices.clone();
        stage1_ops.sort();
        assert_eq!(stage1_ops, vec![1, 2, 3]);
    }

    #[test]
    fn test_infer_stages_parallelism_from_exchange() {
        // 0 → [exchange with par=4] → 1 → [exchange with par=2] → 2
        let mut graph = DataflowGraph::new();
        for i in 0..3 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        graph.set_exchange_parallelism(1, 4); // exchange target op 1 has par=4
        graph.add_edge(exchange_edge(1, 2));
        graph.set_exchange_parallelism(2, 2); // exchange target op 2 has par=2

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 3);
        // Stage 0: no parallelism set (inherits default)
        assert_eq!(stages[0].parallelism, None);
        // Stage 1: par=4 from exchange targeting op 1
        assert_eq!(stages[1].parallelism, Some(4));
        // Stage 2: par=2 from exchange targeting op 2
        assert_eq!(stages[2].parallelism, Some(2));
    }

    #[test]
    fn test_infer_stages_parallelism_none_when_not_set() {
        // Exchange without explicit parallelism → None
        let mut graph = DataflowGraph::new();
        for i in 0..2 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        // No set_exchange_parallelism call

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].parallelism, None);
        assert_eq!(stages[1].parallelism, None);
    }

    #[test]
    fn test_infer_stages_fan_out_different_parallelism_separate_stages() {
        // 0 → [exchange par=4] → 1, 0 → [exchange par=8] → 2
        // Because parallelism differs, ops 1 and 2 must be in DIFFERENT stages.
        let mut graph = DataflowGraph::new();
        for i in 0..3 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        graph.set_exchange_parallelism(1, 4);
        graph.add_edge(exchange_edge(0, 2));
        graph.set_exchange_parallelism(2, 8);

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 3); // stage 0, stage par=4, stage par=8
        assert_eq!(stages[0].parallelism, None);
        // Find stages by parallelism
        let par4 = stages.iter().find(|s| s.parallelism == Some(4)).unwrap();
        let par8 = stages.iter().find(|s| s.parallelism == Some(8)).unwrap();
        assert_eq!(par4.operator_indices, vec![1]);
        assert_eq!(par8.operator_indices, vec![2]);
        assert_ne!(par4.id, par8.id);
    }

    #[test]
    fn test_infer_stages_fan_out_same_parallelism_share_stage() {
        // 0 → [exchange par=4] → 1, 0 → [exchange par=4] → 2
        // Same parallelism from same source → share downstream stage.
        let mut graph = DataflowGraph::new();
        for i in 0..3 {
            graph.register_operator(make_op(i, &format!("op{}", i))).unwrap();
        }
        graph.add_edge(exchange_edge(0, 1));
        graph.set_exchange_parallelism(1, 4);
        graph.add_edge(exchange_edge(0, 2));
        graph.set_exchange_parallelism(2, 4);

        let stages = infer_stages(&graph).unwrap();
        assert_eq!(stages.len(), 2); // stage 0, shared stage par=4
        assert_eq!(stages[1].parallelism, Some(4));
        let mut ops = stages[1].operator_indices.clone();
        ops.sort();
        assert_eq!(ops, vec![1, 2]);
    }
}
