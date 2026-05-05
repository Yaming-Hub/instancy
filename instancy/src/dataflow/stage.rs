//! Stage-based operator grouping for fused execution.
//!
//! A **stage** is a set of operators that share the same parallelism (worker count)
//! and are connected exclusively by pipeline (worker-local) channels. Stage boundaries
//! are defined by repartition operators (exchange, gather, broadcast, rebalance).
//!
//! Within a stage, all operators for a given worker are fused into a single schedulable
//! unit  the executor polls them together in topological order, eliminating per-operator
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StageId(pub usize);

impl StageId {
    pub const INITIAL: StageId = StageId(0);

    pub fn new(id: usize) -> Self {
        Self(id)
    }

    pub fn id(self) -> Self {
        self
    }

    pub fn index(self) -> usize {
        self.0
    }
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

#[derive(Debug, Clone)]
pub struct FusedActivationOrder {
    positions: Vec<usize>,
}

impl FusedActivationOrder {
    pub fn new(positions: Vec<usize>) -> Self {
        Self { positions }
    }

    pub fn positions(&self) -> &[usize] {
        &self.positions
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct StageInfo {
    pub id: StageId,
    pub parallelism: Option<usize>,
    pub operator_indices: Vec<usize>,
    pub fused_order: FusedActivationOrder,
}

impl StageInfo {
    pub fn operator_count(&self) -> usize {
        self.operator_indices.len()
    }
}

pub fn infer_stages(graph: &DataflowGraph) -> Result<Vec<StageInfo>> {
    let topo_order = graph.topological_order()?;

    if topo_order.is_empty() {
        return Ok(Vec::new());
    }

    let mut exchange_targets: HashMap<usize, bool> = HashMap::new();
    for edge in graph.edges() {
        if edge.is_exchange() {
            exchange_targets.insert(edge.target.operator_index, true);
        }
    }

    let mut op_stage: HashMap<usize, usize> = HashMap::new();
    let mut next_stage_id: usize = 0;
    let mut boundary_stage_map: HashMap<(usize, Option<usize>), usize> = HashMap::new();

    for &op_idx in &topo_order {
        if exchange_targets.contains_key(&op_idx) {
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

            let src = source_stage.unwrap_or(0);
            let par = graph.exchange_parallelism(op_idx);
            let stage = *boundary_stage_map.entry((src, par)).or_insert_with(|| {
                if next_stage_id == 0 {
                    next_stage_id = 1;
                }
                let id = next_stage_id;
                next_stage_id += 1;
                id
            });
            op_stage.insert(op_idx, stage);
        } else {
            let mut stage = 0usize;
            for edge in graph.edges() {
                if edge.target.operator_index == op_idx {
                    if let Some(&pred_stage) = op_stage.get(&edge.source.operator_index) {
                        stage = stage.max(pred_stage);
                    }
                }
            }
            op_stage.insert(op_idx, stage);
        }
    }

    let max_stage = op_stage.values().copied().max().unwrap_or(0);
    let mut stage_ops: Vec<Vec<usize>> = vec![Vec::new(); max_stage + 1];
    for &op_idx in &topo_order {
        stage_ops[op_stage[&op_idx]].push(op_idx);
    }

    let mut result = Vec::new();
    for (stage_idx, operator_indices) in stage_ops.into_iter().enumerate() {
        let fused_order = FusedActivationOrder::new(operator_indices.clone());
        let parallelism = operator_indices
            .iter()
            .filter_map(|op_idx| graph.exchange_parallelism(*op_idx))
            .next();
        result.push(StageInfo {
            id: StageId::new(stage_idx),
            parallelism,
            operator_indices,
            fused_order,
        });
    }

    Ok(result)
}
