//! Dataflow graph registry for operator and edge tracking.
//!
//! During dataflow construction, operators and edges are registered in a
//! [`DataflowGraph`]. This graph captures the logical topology of the
//! computation — which operators exist, how they connect, and what stages
//! they belong to.
//!
//! # Two-phase construction
//!
//! 1. **Build phase**: Extension traits (e.g., `UnaryExt::unary()`) register
//!    operators and edges via the scope's graph methods.
//! 2. **Materialization phase** (PR 21): The execution engine reads the graph,
//!    creates channels for each edge, and wires operators into a runnable
//!    dataflow.
//!
//! # Relationship to SubgraphBuilder
//!
//! [`DataflowGraph`] tracks the *logical* topology (operator metadata + edges).
//! [`SubgraphBuilder`](crate::progress::subgraph::SubgraphBuilder) tracks
//! *progress* metadata (port connectivity, summaries, capabilities). Both are
//! populated during the build phase; the execution engine uses both.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::dataflow::stage::StageId;
use crate::dataflow::stream::Slot;
use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// OperatorInfo — metadata about a registered operator
// ---------------------------------------------------------------------------

/// Metadata about a registered operator in the dataflow graph.
///
/// This is a **logical** descriptor — it records what the operator is and where
/// it sits in the graph, but does not contain the operator's executable logic
/// (that is captured separately during materialization).
#[derive(Debug, Clone)]
pub struct OperatorInfo {
    /// Operator index within the scope (unique per scope).
    pub index: usize,
    /// Human-readable name (e.g., "double", "filter", "probe").
    pub name: String,
    /// The execution stage this operator belongs to.
    pub stage_id: StageId,
    /// Number of input ports.
    pub input_count: usize,
    /// Number of output ports.
    pub output_count: usize,
}

impl OperatorInfo {
    /// Create a new operator descriptor.
    pub fn new(
        index: usize,
        name: impl Into<String>,
        stage_id: StageId,
        input_count: usize,
        output_count: usize,
    ) -> Self {
        Self {
            index,
            name: name.into(),
            stage_id,
            input_count,
            output_count,
        }
    }
}

impl fmt::Display for OperatorInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Op[{}] '{}' (stage={}, in={}, out={})",
            self.index, self.name, self.stage_id, self.input_count, self.output_count,
        )
    }
}

// ---------------------------------------------------------------------------
// EdgeInfo — a directed edge between operator ports
// ---------------------------------------------------------------------------

/// The routing kind for a channel (edge) in the dataflow graph.
///
/// Pipeline channels deliver data within a single worker. Exchange channels
/// route data between workers based on a partition strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChannelKind {
    /// Data stays on the same worker (default for intra-worker edges).
    #[default]
    Pipeline,
    /// Data is routed between workers (requires multi-worker materialization).
    Exchange,
}


/// A directed edge connecting an output port of one operator to an input
/// port of another operator.
///
/// Edges are recorded during the build phase when extension traits wire
/// operators together (e.g., `stream.unary(...)` creates an edge from the
/// upstream operator's output to the new operator's input).
#[derive(Debug, Clone)]
pub struct EdgeInfo {
    /// The source output slot (operator index + port number).
    pub source: Slot,
    /// The target input slot (operator index + port number).
    pub target: Slot,
    /// Stage of the source operator.
    pub source_stage: StageId,
    /// Stage of the target operator.
    pub target_stage: StageId,
    /// Whether this edge is a pipeline (worker-local) or exchange (cross-worker).
    pub channel_kind: ChannelKind,
}

impl EdgeInfo {
    /// Create a new pipeline edge descriptor (worker-local channel).
    pub fn new(source: Slot, target: Slot, source_stage: StageId, target_stage: StageId) -> Self {
        Self {
            source,
            target,
            source_stage,
            target_stage,
            channel_kind: ChannelKind::Pipeline,
        }
    }

    /// Create a new exchange edge descriptor (cross-worker channel).
    pub fn exchange(
        source: Slot,
        target: Slot,
        source_stage: StageId,
        target_stage: StageId,
    ) -> Self {
        Self {
            source,
            target,
            source_stage,
            target_stage,
            channel_kind: ChannelKind::Exchange,
        }
    }

    /// Whether this edge requires cross-worker routing.
    pub fn is_exchange(&self) -> bool {
        self.channel_kind == ChannelKind::Exchange
    }
}

impl fmt::Display for EdgeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} → {}", self.source, self.target)
    }
}

// ---------------------------------------------------------------------------
// DataflowGraph — the logical topology
// ---------------------------------------------------------------------------

/// The logical topology of a dataflow graph.
///
/// Built during the construction phase by extension traits registering
/// operators and edges. The execution engine reads this graph during
/// materialization to create channels and wire the runtime.
///
/// # Cardinality & Lifetime
///
/// One instance per dataflow, per scope level. Created at graph construction
/// time and consumed by the execution engine during materialization.
#[derive(Debug, Clone)]
pub struct DataflowGraph {
    /// Registered operators, keyed by operator index.
    operators: HashMap<usize, OperatorInfo>,
    /// Directed edges between operator ports.
    edges: Vec<EdgeInfo>,
    /// Feedback edges — stored separately to avoid cycle detection.
    /// These edges are validated (endpoints exist) but excluded from
    /// topological ordering.
    feedback_edges: Vec<EdgeInfo>,
    /// Target parallelism for exchange operators (operator_index → parallelism).
    /// Set by exchange_to/rebalance_to/broadcast_to/gather to record the
    /// downstream stage's intended parallelism.
    exchange_parallelism: HashMap<usize, usize>,
}

impl DataflowGraph {
    /// Create an empty graph.
    pub fn new() -> Self {
        Self {
            operators: HashMap::new(),
            edges: Vec::new(),
            feedback_edges: Vec::new(),
            exchange_parallelism: HashMap::new(),
        }
    }

    // -- Registration (build phase) --

    /// Register an operator in the graph.
    ///
    /// # Errors
    ///
    /// Returns an error if an operator with the same index is already registered.
    pub fn register_operator(&mut self, info: OperatorInfo) -> Result<()> {
        if self.operators.contains_key(&info.index) {
            return Err(Error::Custom(format!(
                "Duplicate operator index {}: '{}' conflicts with existing '{}'",
                info.index, info.name, self.operators[&info.index].name,
            )));
        }
        self.operators.insert(info.index, info);
        Ok(())
    }

    /// Record an edge between two operator ports.
    ///
    /// Does not validate that the referenced operators exist — call
    /// [`validate`](Self::validate) after construction for that.
    pub fn add_edge(&mut self, edge: EdgeInfo) {
        self.edges.push(edge);
    }

    /// Record a feedback edge between two operator ports.
    ///
    /// Feedback edges are stored separately from regular edges and are
    /// excluded from topological ordering (cycle detection). They are
    /// still validated for endpoint existence.
    pub fn add_feedback_edge(&mut self, edge: EdgeInfo) {
        self.feedback_edges.push(edge);
    }

    /// Set the target parallelism for an exchange operator.
    ///
    /// Called by exchange_to/rebalance_to/broadcast_to/gather to record
    /// what parallelism the downstream stage should have.
    pub fn set_exchange_parallelism(&mut self, operator_index: usize, parallelism: usize) {
        self.exchange_parallelism
            .insert(operator_index, parallelism);
    }

    /// Get the target parallelism for an exchange operator, if set.
    pub fn exchange_parallelism(&self, operator_index: usize) -> Option<usize> {
        self.exchange_parallelism.get(&operator_index).copied()
    }

    /// All feedback edges.
    pub fn feedback_edges(&self) -> &[EdgeInfo] {
        &self.feedback_edges
    }

    /// Increment the input port count of an already-registered operator.
    ///
    /// Used by scope boundary operators whose port counts grow dynamically
    /// as `enter()`/`leave()` calls are made.
    ///
    /// Returns `false` if the operator is not registered.
    pub fn increment_input_count(&mut self, operator_index: usize) -> bool {
        if let Some(op) = self.operators.get_mut(&operator_index) {
            op.input_count += 1;
            true
        } else {
            false
        }
    }

    /// Increment the output port count of an already-registered operator.
    ///
    /// Used by scope boundary operators whose port counts grow dynamically
    /// as `enter()`/`leave()` calls are made.
    ///
    /// Returns `false` if the operator is not registered.
    pub fn increment_output_count(&mut self, operator_index: usize) -> bool {
        if let Some(op) = self.operators.get_mut(&operator_index) {
            op.output_count += 1;
            true
        } else {
            false
        }
    }

    // -- Queries --

    /// Get the number of registered operators.
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Get the number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Look up an operator by index.
    pub fn operator(&self, index: usize) -> Option<&OperatorInfo> {
        self.operators.get(&index)
    }

    /// All registered operators (unordered).
    pub fn operators(&self) -> impl Iterator<Item = &OperatorInfo> {
        self.operators.values()
    }

    /// All edges.
    pub fn edges(&self) -> &[EdgeInfo] {
        &self.edges
    }

    /// Mutable access to all edges.
    pub fn edges_mut(&mut self) -> &mut [EdgeInfo] {
        &mut self.edges
    }

    /// Mutable access to registered operators.
    pub fn operators_mut(&mut self) -> impl Iterator<Item = &mut OperatorInfo> {
        self.operators.values_mut()
    }

    /// Edges originating from the given operator index.
    pub fn edges_from(&self, operator_index: usize) -> Vec<&EdgeInfo> {
        self.edges
            .iter()
            .filter(|e| e.source.operator_index == operator_index)
            .collect()
    }

    /// Edges targeting the given operator index.
    pub fn edges_to(&self, operator_index: usize) -> Vec<&EdgeInfo> {
        self.edges
            .iter()
            .filter(|e| e.target.operator_index == operator_index)
            .collect()
    }

    /// Returns the operator indices that are immediate successors of the given operator.
    pub fn successors(&self, operator_index: usize) -> Vec<usize> {
        let mut succs: Vec<usize> = self
            .edges_from(operator_index)
            .iter()
            .map(|e| e.target.operator_index)
            .collect();
        succs.sort_unstable();
        succs.dedup();
        succs
    }

    /// Returns the operator indices that are immediate predecessors of the given operator.
    pub fn predecessors(&self, operator_index: usize) -> Vec<usize> {
        let mut preds: Vec<usize> = self
            .edges_to(operator_index)
            .iter()
            .map(|e| e.source.operator_index)
            .collect();
        preds.sort_unstable();
        preds.dedup();
        preds
    }

    /// Names of operator types that require cross-worker data routing.
    ///
    /// These operators are not yet supported in multi-worker execution
    /// (each worker runs an independent replica). Attempting to use them
    /// with `spawn_multi(N > 1)` will return an error.
    const EXCHANGE_OPERATOR_NAMES: &[&str] = &[
        "exchange",
        "rebalance",
        "gather",
        "broadcast",
        "broadcastlocal",
    ];

    /// Returns `true` if the graph contains any exchange edges that require
    /// cross-worker data routing.
    ///
    /// This checks the edge-level `ChannelKind` metadata — more reliable than
    /// name-based detection which is kept for backward compatibility.
    pub fn has_exchange_edges(&self) -> bool {
        self.edges.iter().any(|e| e.is_exchange())
            || self.feedback_edges.iter().any(|e| e.is_exchange())
    }

    /// Returns `true` if the graph contains any exchange/rebalance/gather/broadcast
    /// operators that require cross-worker data routing.
    ///
    /// Prefer [`has_exchange_edges()`](Self::has_exchange_edges) for reliable detection.
    pub fn has_exchange_operators(&self) -> bool {
        self.operators.values().any(|op| {
            let lower = op.name.to_lowercase();
            Self::EXCHANGE_OPERATOR_NAMES
                .iter()
                .any(|name| lower == *name)
        })
    }

    /// Returns the names of exchange operators present in the graph, if any.
    pub fn exchange_operator_names(&self) -> Vec<&str> {
        self.operators
            .values()
            .filter(|op| {
                let lower = op.name.to_lowercase();
                Self::EXCHANGE_OPERATOR_NAMES
                    .iter()
                    .any(|name| lower == *name)
            })
            .map(|op| op.name.as_str())
            .collect()
    }

    // -- Topological ordering --

    /// Compute a topological ordering of operators using Kahn's algorithm.
    ///
    /// Returns operator indices in an order such that every operator
    /// appears after all of its predecessors.
    ///
    /// # Errors
    ///
    /// Returns an error if the graph contains a cycle (which shouldn't happen
    /// in a valid dataflow, except for feedback edges that are not tracked as
    /// regular edges).
    pub fn topological_order(&self) -> Result<Vec<usize>> {
        let mut in_degree: HashMap<usize, usize> = HashMap::new();
        for &idx in self.operators.keys() {
            in_degree.insert(idx, 0);
        }
        for edge in &self.edges {
            *in_degree.entry(edge.target.operator_index).or_insert(0) += 1;
        }

        // Seed with zero in-degree nodes, sorted for determinism.
        let mut queue: Vec<usize> = in_degree
            .iter()
            .filter(|&(_, deg)| *deg == 0)
            .map(|(&idx, _)| idx)
            .collect();
        queue.sort_unstable();

        let mut order = Vec::with_capacity(self.operators.len());
        let mut idx = 0;
        while idx < queue.len() {
            let node = queue[idx];
            idx += 1;
            order.push(node);

            // Relax successors.
            let mut new_zeros = Vec::new();
            for edge in &self.edges {
                if edge.source.operator_index == node {
                    let target = edge.target.operator_index;
                    if let Some(deg) = in_degree.get_mut(&target) {
                        *deg -= 1;
                        if *deg == 0 {
                            new_zeros.push(target);
                        }
                    }
                }
            }
            new_zeros.sort_unstable();
            new_zeros.dedup();
            queue.extend(new_zeros);
        }

        if order.len() != self.operators.len() {
            return Err(Error::Custom(format!(
                "Cycle detected in dataflow graph: processed {} of {} operators",
                order.len(),
                self.operators.len(),
            )));
        }

        Ok(order)
    }

    // -- Validation --

    /// Validate the graph for structural correctness.
    ///
    /// Checks:
    /// - All edge endpoints reference registered operators.
    /// - No duplicate edges (same source + target).
    /// - Source port indices are within operator output count.
    /// - Target port indices are within operator input count.
    /// - No cycles (feedback edges are tracked separately, not as regular edges).
    /// - Feedback edge endpoints are also validated.
    pub fn validate(&self) -> Result<()> {
        // Check edge endpoints reference registered operators.
        for (i, edge) in self.edges.iter().enumerate() {
            let src_idx = edge.source.operator_index;
            let tgt_idx = edge.target.operator_index;

            let src_op = self.operators.get(&src_idx).ok_or_else(|| {
                Error::Custom(format!(
                    "Edge {i}: source operator {src_idx} is not registered"
                ))
            })?;

            let tgt_op = self.operators.get(&tgt_idx).ok_or_else(|| {
                Error::Custom(format!(
                    "Edge {i}: target operator {tgt_idx} is not registered"
                ))
            })?;

            // Check port bounds.
            if edge.source.slot_index >= src_op.output_count {
                return Err(Error::Custom(format!(
                    "Edge {i}: source port {} exceeds operator '{}' output count {}",
                    edge.source.slot_index, src_op.name, src_op.output_count,
                )));
            }
            if edge.target.slot_index >= tgt_op.input_count {
                return Err(Error::Custom(format!(
                    "Edge {i}: target port {} exceeds operator '{}' input count {}",
                    edge.target.slot_index, tgt_op.name, tgt_op.input_count,
                )));
            }
        }

        // Validate feedback edge endpoints.
        for (i, edge) in self.feedback_edges.iter().enumerate() {
            let src_idx = edge.source.operator_index;
            let tgt_idx = edge.target.operator_index;

            let src_op = self.operators.get(&src_idx).ok_or_else(|| {
                Error::Custom(format!(
                    "Feedback edge {i}: source operator {src_idx} is not registered"
                ))
            })?;

            let tgt_op = self.operators.get(&tgt_idx).ok_or_else(|| {
                Error::Custom(format!(
                    "Feedback edge {i}: target operator {tgt_idx} is not registered"
                ))
            })?;

            if edge.source.slot_index >= src_op.output_count {
                return Err(Error::Custom(format!(
                    "Feedback edge {i}: source port {} exceeds operator '{}' output count {}",
                    edge.source.slot_index, src_op.name, src_op.output_count,
                )));
            }
            if edge.target.slot_index >= tgt_op.input_count {
                return Err(Error::Custom(format!(
                    "Feedback edge {i}: target port {} exceeds operator '{}' input count {}",
                    edge.target.slot_index, tgt_op.name, tgt_op.input_count,
                )));
            }
        }

        // Check for duplicate edges.
        let mut seen = HashSet::new();
        for (i, edge) in self.edges.iter().enumerate() {
            let key = (edge.source, edge.target);
            if !seen.insert(key) {
                return Err(Error::Custom(format!(
                    "Duplicate edge: {} → {} at position {i}",
                    edge.source, edge.target,
                )));
            }
        }

        // Check for cycles (only on regular edges, not feedback edges).
        self.topological_order()?;

        Ok(())
    }

    // -- Visualization --

    /// Export the graph in Graphviz DOT format.
    ///
    /// Produces a directed graph where each operator is a node (labeled with
    /// its name and stage) and edges show data flow between operators.
    /// Exchange edges are rendered as dashed lines to distinguish them from
    /// pipeline (worker-local) edges. Feedback edges are drawn in red.
    ///
    /// Nodes are grouped into subgraphs by stage for visual clarity.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = builder.build()?.graph().clone();
    /// std::fs::write("dataflow.dot", graph.to_dot()).unwrap();
    /// // Then: dot -Tpng dataflow.dot -o dataflow.png
    /// ```
    pub fn to_dot(&self) -> String {
        self.to_dot_named("dataflow")
    }

    /// Export the graph in Graphviz DOT format with a custom graph name.
    pub fn to_dot_named(&self, name: &str) -> String {
        use std::fmt::Write;

        let mut out = String::new();
        writeln!(out, "digraph \"{}\" {{", dot_escape(name)).unwrap();
        writeln!(out, "    rankdir=TB;").unwrap();
        writeln!(out, "    node [shape=box, style=rounded, fontname=\"Helvetica\"];").unwrap();
        writeln!(out, "    edge [fontname=\"Helvetica\", fontsize=10];").unwrap();
        writeln!(out).unwrap();

        // Group operators by stage.
        let mut stages: HashMap<StageId, Vec<&OperatorInfo>> = HashMap::new();
        for op in self.operators.values() {
            stages.entry(op.stage_id).or_default().push(op);
        }

        // Sort stages by ID for deterministic output.
        let mut stage_ids: Vec<StageId> = stages.keys().copied().collect();
        stage_ids.sort_by_key(|s| s.0);

        for stage_id in &stage_ids {
            let ops = stages.get(stage_id).unwrap();
            let mut sorted_ops: Vec<&&OperatorInfo> = ops.iter().collect();
            sorted_ops.sort_by_key(|op| op.index);

            writeln!(out, "    subgraph cluster_stage_{} {{", stage_id.0).unwrap();
            writeln!(out, "        label=\"Stage {}\";", stage_id.0).unwrap();
            writeln!(out, "        style=dashed;").unwrap();
            writeln!(out, "        color=gray;").unwrap();

            for op in sorted_ops {
                writeln!(
                    out,
                    "        op_{} [label=\"{}\\n[{}]\"];",
                    op.index,
                    dot_escape(&op.name),
                    op.index,
                )
                .unwrap();
            }
            writeln!(out, "    }}").unwrap();
            writeln!(out).unwrap();
        }

        // Regular edges.
        for edge in &self.edges {
            let style = if edge.is_exchange() {
                " [style=dashed, label=\"exchange\"]"
            } else {
                ""
            };
            writeln!(
                out,
                "    op_{} -> op_{}{};",
                edge.source.operator_index, edge.target.operator_index, style
            )
            .unwrap();
        }

        // Feedback edges.
        for edge in &self.feedback_edges {
            writeln!(
                out,
                "    op_{} -> op_{} [style=bold, color=red, label=\"feedback\"];",
                edge.source.operator_index, edge.target.operator_index
            )
            .unwrap();
        }

        writeln!(out, "}}").unwrap();
        out
    }
}

/// Escape a string for use inside DOT double-quoted strings.
///
/// Replaces `\` with `\\`, `"` with `\"`, and newlines with `\n`.
fn dot_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

impl Default for DataflowGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DataflowGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "DataflowGraph ({} operators, {} edges)",
            self.operators.len(),
            self.edges.len()
        )?;
        // Print operators sorted by index.
        let mut ops: Vec<_> = self.operators.values().collect();
        ops.sort_by_key(|o| o.index);
        for op in ops {
            writeln!(f, "  {op}")?;
        }
        for edge in &self.edges {
            writeln!(f, "  {edge}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stage() -> StageId {
        StageId::new(0)
    }

    // -- OperatorInfo --

    #[test]
    fn operator_info_creation() {
        let info = OperatorInfo::new(0, "my_op", make_stage(), 1, 1);
        assert_eq!(info.index, 0);
        assert_eq!(info.name, "my_op");
        assert_eq!(info.input_count, 1);
        assert_eq!(info.output_count, 1);
    }

    #[test]
    fn operator_info_display() {
        let info = OperatorInfo::new(3, "filter", make_stage(), 1, 1);
        let s = format!("{info}");
        assert!(s.contains("Op[3]"));
        assert!(s.contains("filter"));
    }

    // -- EdgeInfo --

    #[test]
    fn edge_info_creation() {
        let edge = EdgeInfo::new(
            Slot::new(0, 0),
            Slot::new(1, 0),
            make_stage(),
            make_stage(),
        );
        assert_eq!(edge.source.operator_index, 0);
        assert_eq!(edge.target.operator_index, 1);
    }

    #[test]
    fn edge_info_display() {
        let edge = EdgeInfo::new(
            Slot::new(2, 0),
            Slot::new(3, 1),
            make_stage(),
            make_stage(),
        );
        let s = format!("{edge}");
        assert!(s.contains("Op2:Slot0"));
        assert!(s.contains("Op3:Slot1"));
    }

    // -- DataflowGraph registration --

    #[test]
    fn empty_graph() {
        let graph = DataflowGraph::new();
        assert_eq!(graph.operator_count(), 0);
        assert_eq!(graph.edge_count(), 0);
        assert!(graph.validate().is_ok());
    }

    #[test]
    fn register_operators() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "source", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "map", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "sink", r, 1, 0))
            .unwrap();

        assert_eq!(graph.operator_count(), 3);
        assert_eq!(graph.operator(0).unwrap().name, "source");
        assert_eq!(graph.operator(1).unwrap().name, "map");
        assert_eq!(graph.operator(2).unwrap().name, "sink");
        assert!(graph.operator(99).is_none());
    }

    #[test]
    fn duplicate_operator_rejected() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "first", r, 1, 1))
            .unwrap();
        let err = graph
            .register_operator(OperatorInfo::new(0, "second", r, 1, 1))
            .unwrap_err();
        assert!(err.to_string().contains("Duplicate operator index 0"));
    }

    #[test]
    fn add_edges() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        assert_eq!(graph.edge_count(), 1);
        assert_eq!(graph.edges()[0].source, Slot::new(0, 0));
        assert_eq!(graph.edges()[0].target, Slot::new(1, 0));
    }

    // -- Queries --

    #[test]
    fn edges_from_and_to() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 2))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "c", r, 1, 0))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(0, 1), Slot::new(2, 0), r, r));

        assert_eq!(graph.edges_from(0).len(), 2);
        assert_eq!(graph.edges_from(1).len(), 0);
        assert_eq!(graph.edges_to(1).len(), 1);
        assert_eq!(graph.edges_to(2).len(), 1);
        assert_eq!(graph.edges_to(0).len(), 0);
    }

    #[test]
    fn successors_and_predecessors() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "c", r, 1, 0))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(2, 0), r, r));

        assert_eq!(graph.successors(0), vec![1]);
        assert_eq!(graph.successors(1), vec![2]);
        assert_eq!(graph.successors(2), Vec::<usize>::new());

        assert_eq!(graph.predecessors(0), Vec::<usize>::new());
        assert_eq!(graph.predecessors(1), vec![0]);
        assert_eq!(graph.predecessors(2), vec![1]);
    }

    // -- Topological order --

    #[test]
    fn topological_order_linear() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "c", r, 1, 0))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(2, 0), r, r));

        let order = graph.topological_order().unwrap();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn topological_order_diamond() {
        //   0
        //  / \
        // 1   2
        //  \ /
        //   3
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "source", r, 0, 2))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "left", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "right", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(3, "join", r, 2, 0))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(0, 1), Slot::new(2, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(3, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(2, 0), Slot::new(3, 1), r, r));

        let order = graph.topological_order().unwrap();
        assert_eq!(order[0], 0); // source first
        assert_eq!(order[3], 3); // join last
        // 1 and 2 can be in either order, but both before 3
        assert!(order.contains(&1));
        assert!(order.contains(&2));
    }

    #[test]
    fn topological_order_disconnected() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 0))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 0, 0))
            .unwrap();

        let order = graph.topological_order().unwrap();
        assert_eq!(order, vec![0, 1]);
    }

    #[test]
    fn topological_order_cycle_detected() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(0, 0), r, r));

        let err = graph.topological_order().unwrap_err();
        assert!(err.to_string().contains("Cycle detected"));
    }

    // -- Validation --

    #[test]
    fn validate_valid_graph() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 0))
            .unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        assert!(graph.validate().is_ok());
    }

    #[test]
    fn validate_missing_source_operator() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 0))
            .unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        let err = graph.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("source operator 0 is not registered")
        );
    }

    #[test]
    fn validate_missing_target_operator() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        let err = graph.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("target operator 1 is not registered")
        );
    }

    #[test]
    fn validate_port_out_of_bounds() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 0))
            .unwrap();

        // Source port 1 but operator 0 only has 1 output (port 0).
        graph.add_edge(EdgeInfo::new(Slot::new(0, 1), Slot::new(1, 0), r, r));
        let err = graph.validate().unwrap_err();
        assert!(err.to_string().contains("source port 1 exceeds"));
    }

    #[test]
    fn validate_duplicate_edge() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 0))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        let err = graph.validate().unwrap_err();
        assert!(err.to_string().contains("Duplicate edge"));
    }

    #[test]
    fn validate_cycle() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "a", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "b", r, 1, 1))
            .unwrap();

        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(0, 0), r, r));

        let err = graph.validate().unwrap_err();
        assert!(err.to_string().contains("Cycle detected"));
    }

    // -- Display --

    #[test]
    fn display_graph() {
        let mut graph = DataflowGraph::new();
        let r = make_stage();
        graph
            .register_operator(OperatorInfo::new(0, "source", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "sink", r, 1, 0))
            .unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));

        let s = format!("{graph}");
        assert!(s.contains("2 operators"));
        assert!(s.contains("1 edges"));
        assert!(s.contains("source"));
        assert!(s.contains("sink"));
    }

    #[test]
    fn has_exchange_operators_false_for_pipeline() {
        let r = StageId::new(0);
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "source", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "map", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "sink", r, 1, 0))
            .unwrap();
        assert!(!graph.has_exchange_operators());
        assert!(graph.exchange_operator_names().is_empty());
    }

    #[test]
    fn has_exchange_operators_detects_exchange() {
        let r = StageId::new(0);
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "source", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "exchange", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "sink", r, 1, 0))
            .unwrap();
        assert!(graph.has_exchange_operators());
        assert_eq!(graph.exchange_operator_names(), vec!["exchange"]);
    }

    #[test]
    fn has_exchange_operators_detects_rebalance_gather_broadcast() {
        let r = StageId::new(0);
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "rebalance", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "gather", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "broadcast", r, 1, 1))
            .unwrap();
        assert!(graph.has_exchange_operators());
        let mut names = graph.exchange_operator_names();
        names.sort();
        assert_eq!(names, vec!["broadcast", "gather", "rebalance"]);
    }

    // -- ChannelKind + exchange edge metadata --

    #[test]
    fn channel_kind_default_is_pipeline() {
        assert_eq!(ChannelKind::default(), ChannelKind::Pipeline);
    }

    #[test]
    fn edge_info_new_creates_pipeline() {
        let edge = EdgeInfo::new(
            Slot::new(0, 0),
            Slot::new(1, 0),
            make_stage(),
            make_stage(),
        );
        assert_eq!(edge.channel_kind, ChannelKind::Pipeline);
        assert!(!edge.is_exchange());
    }

    #[test]
    fn edge_info_exchange_creates_exchange() {
        let edge = EdgeInfo::exchange(
            Slot::new(0, 0),
            Slot::new(1, 0),
            make_stage(),
            make_stage(),
        );
        assert_eq!(edge.channel_kind, ChannelKind::Exchange);
        assert!(edge.is_exchange());
    }

    #[test]
    fn has_exchange_edges_false_for_pipeline_only() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "src", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "sink", r, 1, 0))
            .unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        assert!(!graph.has_exchange_edges());
    }

    #[test]
    fn has_exchange_edges_true_when_exchange_present() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "src", r, 0, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(1, "mid", r, 1, 1))
            .unwrap();
        graph
            .register_operator(OperatorInfo::new(2, "sink", r, 1, 0))
            .unwrap();
        // One pipeline edge, one exchange edge.
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::exchange(Slot::new(1, 0), Slot::new(2, 0), r, r));
        assert!(graph.has_exchange_edges());
    }

    #[test]
    fn has_exchange_edges_empty_graph_is_false() {
        let graph = DataflowGraph::new();
        assert!(!graph.has_exchange_edges());
    }

    #[test]
    fn has_exchange_edges_true_for_feedback_exchange() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "loop_body", r, 1, 1))
            .unwrap();
        graph.add_feedback_edge(EdgeInfo::exchange(Slot::new(0, 0), Slot::new(0, 0), r, r));
        assert!(graph.has_exchange_edges());
    }

    // -- DOT export --

    #[test]
    fn to_dot_simple_pipeline() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph.register_operator(OperatorInfo::new(0, "source", r, 0, 1)).unwrap();
        graph.register_operator(OperatorInfo::new(1, "map", r, 1, 1)).unwrap();
        graph.register_operator(OperatorInfo::new(2, "sink", r, 1, 0)).unwrap();
        graph.add_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(1, 0), r, r));
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(2, 0), r, r));

        let dot = graph.to_dot();
        assert!(dot.starts_with("digraph \"dataflow\" {"));
        assert!(dot.contains("op_0 [label=\"source\\n[0]\"]"));
        assert!(dot.contains("op_1 [label=\"map\\n[1]\"]"));
        assert!(dot.contains("op_2 [label=\"sink\\n[2]\"]"));
        assert!(dot.contains("op_0 -> op_1;"));
        assert!(dot.contains("op_1 -> op_2;"));
        assert!(dot.contains("cluster_stage_0"));
        assert!(!dot.contains("exchange"));
    }

    #[test]
    fn to_dot_with_exchange_edge() {
        let s0 = StageId::new(0);
        let s1 = StageId::new(1);
        let mut graph = DataflowGraph::new();
        graph.register_operator(OperatorInfo::new(0, "parse", s0, 0, 1)).unwrap();
        graph.register_operator(OperatorInfo::new(1, "aggregate", s1, 1, 0)).unwrap();
        graph.add_edge(EdgeInfo::exchange(Slot::new(0, 0), Slot::new(1, 0), s0, s1));

        let dot = graph.to_dot();
        assert!(dot.contains("cluster_stage_0"));
        assert!(dot.contains("cluster_stage_1"));
        assert!(dot.contains("style=dashed, label=\"exchange\""));
        assert!(dot.contains("op_0 -> op_1"));
    }

    #[test]
    fn to_dot_with_feedback_edge() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph.register_operator(OperatorInfo::new(0, "iterate", r, 1, 1)).unwrap();
        graph.add_feedback_edge(EdgeInfo::new(Slot::new(0, 0), Slot::new(0, 0), r, r));

        let dot = graph.to_dot();
        assert!(dot.contains("color=red"));
        assert!(dot.contains("label=\"feedback\""));
    }

    #[test]
    fn to_dot_named_custom_name() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph.register_operator(OperatorInfo::new(0, "op", r, 0, 0)).unwrap();

        let dot = graph.to_dot_named("my_pipeline");
        assert!(dot.starts_with("digraph \"my_pipeline\" {"));
    }

    #[test]
    fn to_dot_empty_graph() {
        let graph = DataflowGraph::new();
        let dot = graph.to_dot();
        assert!(dot.contains("digraph \"dataflow\" {"));
        assert!(dot.contains("}"));
        // No operators or edges
        assert!(!dot.contains("op_"));
        assert!(!dot.contains("->"));
    }

    #[test]
    fn to_dot_escapes_special_characters() {
        let r = make_stage();
        let mut graph = DataflowGraph::new();
        graph
            .register_operator(OperatorInfo::new(0, "op with \"quotes\"", r, 0, 0))
            .unwrap();

        let dot = graph.to_dot_named("my \"pipeline\"");
        assert!(dot.contains(r#"digraph "my \"pipeline\""#));
        assert!(dot.contains(r#"op with \"quotes\""#));
        // Must not have unescaped quotes that break DOT syntax
        assert!(!dot.contains("op with \"quotes\"\\n"));
    }
}
