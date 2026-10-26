//! Subgraph builder and progress tracker, one per scope.
//!
//! [`SubgraphBuilder`] collects a scope's operators and edges during dataflow
//! construction, then compiles into a [`ProgressTracker`] that manages live
//! frontier propagation for the scope.
//!
//! # Scopes
//!
//! The dataflow is one root scope, and every `iterate` loop is a nested
//! scope with its own tracker in `Product<T, TInner>` time (design §5.1.7).
//! Execution stays flat; only progress is nested. In its parent, a loop is
//! one node `S` (a synthetic node id after every operator index, R38 C1) with
//! one input and one output, whose connectivity is the loop's own scope
//! summary projected onto the outer time. A loop's operators map onto its
//! scopes as follows:
//!
//! - `enter`'s input is counted at the parent's `Target(S, 0)`; its output is
//!   the child's boundary source `Source(0, 0)`. `enter` is a node of neither
//!   scope;
//! - `concat`, `feedback`, the body's operators and `leave` are child nodes.
//!   `leave`'s output, in the child, feeds the boundary target
//!   `Target(0, 0)`; its records are counted on the parent's edges from
//!   `Source(S, 0)`.
//!
//! Every worker keeps every scope's tracker (R38 C4). Each worker's commit
//! carries one section of raw port counts per scope, all in one batch (C3).
//! Two derived computations bridge the scopes, both local and never
//! broadcast:
//!
//! - **import:** the parent's frontier at `Target(S, i)` becomes the child's
//!   pointstamps at `Source(0, i)`, at `(t, TInner::minimum())`;
//! - **export:** the child's internal work (the pointstamp frontiers of every
//!   location but node 0's), carried along the paths to its outputs and
//!   projected onto the outer time, becomes the parent's pointstamps at
//!   `Source(S, o)`.
//!
//! A propagation applies every scope's raw changes, then the exports bottom-up,
//! then propagates top-down, importing each parent's frontiers into its
//! children before they propagate. Completion (exact zero, C2) looks at raw
//! counts only; derived ones are excluded.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::plan::PhysicalPlan;
use crate::dataflow::probe::{AttachedProbe, ProbeRegistration};
use crate::error::DataflowError;
use crate::order::Product;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::{OperatorProgress, PortConnectivity, PortLedger, ProgressReporter};
use crate::progress::progress_channel::{
    LaneState, ProgressBatch, ProgressChange, ProgressKind, WorkerProgressChannels,
};
use crate::progress::reachability::{Builder as ReachabilityBuilder, Location, Tracker};
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// OperatorShape — static metadata for a registered operator
// ---------------------------------------------------------------------------

/// Static metadata about an operator in the subgraph.
#[derive(Debug, Clone)]
pub struct OperatorShape {
    /// Operator index within the scope.
    pub index: usize,
    /// Human-readable name.
    pub name: String,
    /// Number of input ports.
    pub inputs: usize,
    /// Number of output ports.
    pub outputs: usize,
}

/// One scope's materialization-time handles, in the scope's time.
struct ScopeHandles<T: Timestamp> {
    internal: HashMap<(usize, usize), ProgressReporter<T>>,
    consumed: HashMap<(usize, usize), PortLedger<T>>,
    produced: HashMap<(usize, usize), PortLedger<T>>,
}

/// Materialization-time progress handles handed to operator factories:
/// each operator port's capability reporter and port ledger, keyed by
/// `(operator, port)` and looked up by the port's time.
///
/// This type is **non-generic** on purpose: an operator inside a loop is built
/// with timestamp `Product<T, _>`, and `enter` and `leave` have ports of two
/// times, each counted by the scope of that time. Every scope contributes its
/// handles in its own time.
pub(crate) struct MaterializationReporters {
    scopes: Vec<Box<dyn Any + Send + Sync>>,
}

impl MaterializationReporters {
    fn find<T: Timestamp, R>(&self, f: impl Fn(&ScopeHandles<T>) -> Option<R>) -> Option<R> {
        self.scopes
            .iter()
            .filter_map(|scope| scope.downcast_ref::<ScopeHandles<T>>())
            .find_map(f)
    }

    /// The per-output capability reporter for `(operator, output)`, if present.
    pub(crate) fn internal<T: Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<ProgressReporter<T>> {
        self.find::<T, _>(|s| s.internal.get(&(operator, output)).cloned())
    }

    /// The consumed ledger of input `input` of `operator`, if present.
    pub(crate) fn consumed<T: Timestamp>(
        &self,
        operator: usize,
        input: usize,
    ) -> Option<PortLedger<T>> {
        self.find::<T, _>(|s| s.consumed.get(&(operator, input)).cloned())
    }

    /// The produced ledger of output `output` of `operator`, if present.
    pub(crate) fn produced<T: Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<PortLedger<T>> {
        self.find::<T, _>(|s| s.produced.get(&(operator, output)).cloned())
    }
}

/// Which side of an operator a port is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Side {
    /// An input port (a `Target`).
    Input,
    /// An output port (a `Source`).
    Output,
}

/// Where a scope counts an operator port that is not simply the port of
/// one of its own nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PortAt {
    /// Another scope counts this port.
    Elsewhere,
    /// The scope's boundary node 0 (`Source(0, port)` for an output: a
    /// loop's `enter`, seen from inside the loop).
    Boundary(usize),
    /// The node of nested scope `scope` (`Target(S, port)` or
    /// `Source(S, port)`: a loop's `enter` input or `leave` output, seen
    /// from outside the loop).
    ScopeNode { scope: usize, port: usize },
}

/// A scope's location of every hosted port, for the worker's scope
/// computation: `(scope id, location)`.
type ScopeLocation = (usize, Location);

/// The reverse data graph of every scope, bridged between scopes.
type ReverseGraph = HashMap<ScopeLocation, Vec<ScopeLocation>>;

// ---------------------------------------------------------------------------
// SubgraphBuilder — collects operators and edges, builds progress tracker
// ---------------------------------------------------------------------------

/// Collects one scope's operator registrations and edges during dataflow
/// construction, then compiles them into a [`ProgressTracker`].
///
/// The builder accumulates the topology of the scope:
/// - **operators**: what they are (shape, name)
/// - **connectivity**: how timestamps transform through each operator
/// - **edges**: which output ports connect to which input ports
/// - **initial capabilities**: which operators start holding capabilities
/// - **progress buffers**: shared reporters and ledgers that operators write
///   to at runtime, including those of boundary operators whose ports this
///   scope counts without the operator being one of its nodes
/// - **nested scopes**: the builders of its `iterate` loops
///
/// Graph node 0 (a vertex in the dataflow graph, not a machine) is reserved
/// for the scope boundary (timely convention).
pub struct SubgraphBuilder<T: Timestamp> {
    /// Registered operator shapes (this scope's nodes).
    operators: HashMap<usize, OperatorShape>,
    /// Per-operator internal connectivity.
    connectivity: HashMap<usize, PortConnectivity<T::Summary>>,
    /// Per-operator initial capabilities (per output port).
    initial_capabilities: HashMap<usize, Vec<ChangeBatch<T>>>,
    /// Edges: (source_node, source_port) → (target_node, target_port).
    edges: Vec<(Location, Location)>,
    /// Edges every materialization keeps (a loop's `leave` → node 0).
    fixed_edges: Vec<(Location, Location)>,
    /// Where each produced record is counted: for each source location, the
    /// target of each of its edges and the edge's multiplicity. `None` until
    /// the edges come from the materialized graph (every edge counts once).
    routes: Option<HashMap<Location, Vec<(Location, i64)>>>,
    /// Per-operator shared progress buffers (created at registration), for
    /// this scope's nodes and for its boundary operators.
    progress_buffers: HashMap<usize, OperatorProgress<T>>,
    /// Boundary operators: counted here without being nodes.
    owners: HashMap<usize, OperatorShape>,
    /// Ports counted somewhere other than their operator's own node.
    port_map: HashMap<(Side, usize, usize), PortAt>,
    /// Number of scope-level inputs.
    scope_inputs: usize,
    /// Number of scope-level outputs.
    scope_outputs: usize,
    /// This scope's id: 0 for the root, then one per `iterate`, in the order
    /// the loops were built (identical on every worker).
    scope_id: usize,
    /// Nested scopes.
    children: Vec<Box<dyn ChildBuilder<T>>>,
}

/// Manual Clone that deep-copies progress buffers so each clone gets
/// independent [`ProgressReporter`](super::operate::ProgressReporter)s.
/// A derived Clone would share `Arc`-backed reporters across clones,
/// causing workers to drain each other's capability changes.
impl<T: Timestamp> Clone for SubgraphBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            operators: self.operators.clone(),
            connectivity: self.connectivity.clone(),
            initial_capabilities: self.initial_capabilities.clone(),
            edges: self.edges.clone(),
            fixed_edges: self.fixed_edges.clone(),
            routes: self.routes.clone(),
            progress_buffers: self
                .progress_buffers
                .iter()
                .map(|(k, v)| (*k, v.deep_clone()))
                .collect(),
            owners: self.owners.clone(),
            port_map: self.port_map.clone(),
            scope_inputs: self.scope_inputs,
            scope_outputs: self.scope_outputs,
            scope_id: self.scope_id,
            children: self.children.iter().map(|c| c.clone_box()).collect(),
        }
    }
}

impl<T: Timestamp> SubgraphBuilder<T> {
    /// Creates a new subgraph builder for the root scope.
    ///
    /// `scope_inputs` and `scope_outputs` define the scope boundary ports.
    pub fn new(scope_inputs: usize, scope_outputs: usize) -> Self {
        Self::nested(0, scope_inputs, scope_outputs)
    }

    /// Creates the builder of scope `scope_id` (a loop: one input, one
    /// output).
    pub(crate) fn nested(scope_id: usize, scope_inputs: usize, scope_outputs: usize) -> Self {
        Self {
            operators: HashMap::new(),
            connectivity: HashMap::new(),
            initial_capabilities: HashMap::new(),
            edges: Vec::new(),
            fixed_edges: Vec::new(),
            routes: None,
            progress_buffers: HashMap::new(),
            owners: HashMap::new(),
            port_map: HashMap::new(),
            scope_inputs,
            scope_outputs,
            scope_id,
            children: Vec::new(),
        }
    }

    /// Where this scope counts input `port` of `op`, if it does.
    fn input_location(&self, op: usize, port: usize) -> Option<Location> {
        locate(
            &self.port_map,
            &self.operators,
            &self.children_nodes(),
            Side::Input,
            op,
            port,
        )
    }

    /// Where this scope counts output `port` of `op`, if it does.
    fn output_location(&self, op: usize, port: usize) -> Option<Location> {
        locate(
            &self.port_map,
            &self.operators,
            &self.children_nodes(),
            Side::Output,
            op,
            port,
        )
    }

    fn children_nodes(&self) -> HashMap<usize, usize> {
        self.children
            .iter()
            .map(|c| (c.scope_id(), c.node()))
            .collect()
    }

    /// Replaces the edges with the data edges of the materialized `graph`
    /// that this scope (and each nested scope) counts, so the progress graph
    /// and the data graph can never diverge (P-8). Every produced record is
    /// counted once per edge.
    pub(crate) fn set_edges_from_graph(&mut self, graph: &DataflowGraph) -> crate::Result<()> {
        self.assign_graph(graph, None)
    }

    /// Like [`set_edges_from_graph`](Self::set_edges_from_graph), with each
    /// edge's multiplicity from `plan` (R35). Every edge, feedback edges
    /// included, must belong to exactly one scope.
    pub(crate) fn assign_graph(
        &mut self,
        graph: &DataflowGraph,
        plan: Option<&PhysicalPlan>,
    ) -> crate::Result<()> {
        let total = graph.edges().len() + graph.feedback_edges().len();
        let mut claimed = vec![false; total];
        self.claim_edges(graph, plan, &mut claimed)?;
        if let Some(index) = claimed.iter().position(|c| !c) {
            let edge = graph
                .edges()
                .iter()
                .chain(graph.feedback_edges())
                .nth(index)
                .map(|e| e.to_string())
                .unwrap_or_default();
            return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                format!("edge {index} ({edge}) belongs to no progress scope"),
            )));
        }
        Ok(())
    }

    fn claim_edges(
        &mut self,
        graph: &DataflowGraph,
        plan: Option<&PhysicalPlan>,
        claimed: &mut [bool],
    ) -> crate::Result<()> {
        let mut edges = self.fixed_edges.clone();
        let mut routes: HashMap<Location, Vec<(Location, i64)>> = HashMap::new();
        for (index, edge) in graph
            .edges()
            .iter()
            .chain(graph.feedback_edges())
            .enumerate()
        {
            let source = self.output_location(edge.source.operator_index, edge.source.slot_index);
            let target = self.input_location(edge.target.operator_index, edge.target.slot_index);
            match (source, target) {
                (Some(source), Some(target)) => {
                    if claimed[index] {
                        return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                            format!("edge {index} ({edge}) belongs to two progress scopes"),
                        )));
                    }
                    claimed[index] = true;
                    let multiplicity = match plan {
                        Some(plan) => plan
                            .edge(index)
                            .ok_or_else(|| {
                                crate::Error::Dataflow(DataflowError::InvalidGraph(format!(
                                    "edge {index} ({edge}) is not in the physical plan"
                                )))
                            })?
                            .multiplicity(),
                        None => 1,
                    };
                    routes
                        .entry(source.clone())
                        .or_default()
                        .push((target.clone(), multiplicity));
                    edges.push((source, target));
                }
                (None, None) => {}
                _ => {
                    return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                        format!(
                            "edge {index} ({edge}) crosses a progress scope boundary outside \
                         a loop's enter or leave"
                        ),
                    )));
                }
            }
        }
        self.edges = edges;
        self.routes = Some(routes);
        for child in &mut self.children {
            child.claim_edges(graph, plan, claimed)?;
        }
        Ok(())
    }

    /// Returns clones of every scope's per-output capability reporters and
    /// port ledgers for materialization-time wiring.
    pub(crate) fn materialization_reporters(&self) -> MaterializationReporters {
        let mut scopes = Vec::new();
        self.collect_handles(&mut scopes);
        MaterializationReporters { scopes }
    }

    fn collect_handles(&self, out: &mut Vec<Box<dyn Any + Send + Sync>>) {
        let mut handles = ScopeHandles::<T> {
            internal: HashMap::new(),
            consumed: HashMap::new(),
            produced: HashMap::new(),
        };
        for (&op, progress) in &self.progress_buffers {
            for (port, reporter) in progress.internal.iter().enumerate() {
                handles.internal.insert((op, port), reporter.clone());
            }
            for (port, ledger) in progress.consumed.iter().enumerate() {
                handles.consumed.insert((op, port), ledger.clone());
            }
            for (port, ledger) in progress.produced.iter().enumerate() {
                handles.produced.insert((op, port), ledger.clone());
            }
        }
        out.push(Box::new(handles));
        for child in &self.children {
            child.collect_handles(out);
        }
    }

    /// Registers an operator with its static shape and connectivity.
    ///
    /// Creates shared [`OperatorProgress`] buffers for the operator.
    ///
    /// # Errors
    ///
    /// The operator index is already registered or is 0 (reserved for the
    /// scope boundary; "node" here is a dataflow graph vertex, not a
    /// machine).
    pub fn add_operator(
        &mut self,
        index: usize,
        name: impl Into<String>,
        inputs: usize,
        outputs: usize,
        connectivity: PortConnectivity<T::Summary>,
    ) -> Result<&OperatorProgress<T>, crate::Error> {
        if index == 0 {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "operator index 0 is reserved for scope boundary".into(),
            )));
        }
        let name = name.into();
        if self.operators.contains_key(&index) || self.owners.contains_key(&index) {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                format!("operator index {index} ('{name}') already registered"),
            )));
        }

        self.operators.insert(
            index,
            OperatorShape {
                index,
                name,
                inputs,
                outputs,
            },
        );
        self.connectivity.insert(index, connectivity);
        self.progress_buffers
            .insert(index, OperatorProgress::new(inputs, outputs));
        Ok(self
            .progress_buffers
            .get(&index)
            // SAFETY: key was just inserted via progress_buffers.insert() above
            .expect("operator progress exists after registration"))
    }

    /// Registers an operator with initial capabilities on its output ports.
    ///
    /// Input operators typically hold an initial capability at `T::minimum()`.
    pub fn add_operator_with_capabilities(
        &mut self,
        index: usize,
        name: impl Into<String>,
        inputs: usize,
        outputs: usize,
        connectivity: PortConnectivity<T::Summary>,
        initial_caps: Vec<ChangeBatch<T>>,
    ) -> Result<&OperatorProgress<T>, crate::Error> {
        if initial_caps.len() != outputs {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                format!(
                    "initial_caps length ({}) must match outputs count ({outputs})",
                    initial_caps.len()
                ),
            )));
        }
        self.add_operator(index, name, inputs, outputs, connectivity)?;
        self.initial_capabilities.insert(index, initial_caps);
        Ok(self
            .progress_buffers
            .get(&index)
            // SAFETY: key was just inserted via progress_buffers.insert() above
            .expect("operator progress exists after registration"))
    }

    /// Registers a boundary operator (a loop's `enter` or `leave`): this scope
    /// counts some of its ports (see [`map_port`](Self::map_port)) without
    /// the operator being one of its nodes.
    pub(crate) fn add_port_owner(
        &mut self,
        index: usize,
        name: impl Into<String>,
        inputs: usize,
        outputs: usize,
    ) {
        self.owners.insert(
            index,
            OperatorShape {
                index,
                name: name.into(),
                inputs,
                outputs,
            },
        );
        self.progress_buffers
            .insert(index, OperatorProgress::new(inputs, outputs));
    }

    /// Counts `side` port `port` of `op` at `at` instead of at the operator's
    /// own node.
    pub(crate) fn map_port(&mut self, side: Side, op: usize, port: usize, at: PortAt) {
        self.port_map.insert((side, op, port), at);
    }

    /// Adds an edge that every materialization keeps (not from the data
    /// graph): a loop's `leave` output into the boundary node 0.
    pub(crate) fn add_fixed_edge(&mut self, source: Location, target: Location) {
        self.fixed_edges.push((source.clone(), target.clone()));
        self.edges.push((source, target));
    }

    /// Adds nested scope `child` (an `iterate` loop).
    pub(crate) fn add_child(&mut self, child: Box<dyn ChildBuilder<T>>) {
        self.children.push(child);
    }

    /// Gives every nested scope its node id: `base + scope id - 1`, after
    /// every operator index (`base` is the operator count, R38 C1), so a
    /// lookup that bypasses the port map fails loudly.
    pub(crate) fn assign_scope_nodes(&mut self, base: usize) {
        for child in &mut self.children {
            child.assign_nodes(base);
        }
    }

    /// Records an edge from source output to target input.
    pub fn add_edge(&mut self, source: Location, target: Location) {
        self.edges.push((source, target));
    }

    /// Returns a reference to the progress buffers for a registered operator.
    pub fn operator_progress(&self, index: usize) -> Option<&OperatorProgress<T>> {
        self.progress_buffers.get(&index)
    }

    /// Tell the progress checker which output each capability reporter is
    /// for, so it can count the capabilities held through them. Must run
    /// before operators are materialized (before any capability exists).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn attach_check_reporters(&self, ctx: &crate::progress::check::Ctx) {
        let sink = ctx.sink_for(self.scope_id);
        for (&op, progress) in &self.progress_buffers {
            for (output, reporter) in progress.internal.iter().enumerate() {
                if self.output_location(op, output).is_some() {
                    reporter.set_check_port(crate::progress::check::Port::new(
                        sink.clone(),
                        ctx.worker,
                        (op, output),
                        None,
                    ));
                }
            }
        }
        for child in &self.children {
            child.attach_check_reporters(ctx);
        }
    }

    /// Returns the number of registered operators (excluding scope boundary).
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Returns all registered operator shapes.
    pub fn operator_shapes(&self) -> impl Iterator<Item = &OperatorShape> {
        self.operators.values()
    }

    /// Returns all edges.
    pub fn edges(&self) -> &[(Location, Location)] {
        &self.edges
    }

    /// Compiles the subgraph into a live [`ProgressTracker`].
    ///
    /// # Panics
    ///
    /// The scope graph is invalid (see [`try_build`](Self::try_build)).
    pub fn build(self) -> ProgressTracker<T> {
        self.try_build()
            .expect("the progress scope graph should be valid")
    }

    /// Compiles the subgraph (and every nested scope) into a live
    /// [`ProgressTracker`].
    ///
    /// # Errors
    ///
    /// A scope graph has an identity cycle: a cycle whose summary does not
    /// advance the timestamp, which would keep its frontier from ever
    /// advancing (R38 C5).
    pub fn try_build(self) -> crate::Result<ProgressTracker<T>> {
        let mut children = Vec::with_capacity(self.children.len());
        for child in self.children {
            children.push(child.build()?);
        }

        // Build the reachability graph: this scope's nodes, then one node per
        // nested scope.
        let mut reachability_builder =
            ReachabilityBuilder::new(self.scope_inputs, self.scope_outputs);
        let mut names: HashMap<usize, String> = HashMap::new();
        for (&index, shape) in &self.operators {
            let conn = self
                .connectivity
                .get(&index)
                // SAFETY: connectivity inserted alongside progress_buffers in add_operator
                .expect("connectivity missing for registered operator");
            reachability_builder.add_node(index, shape.inputs, shape.outputs, conn.clone())?;
            names.insert(index, shape.name.clone());
        }
        for child in &children {
            reachability_builder.add_node(
                child.node(),
                child.inputs(),
                child.outputs(),
                child.outer_summary().clone(),
            )?;
            names.insert(child.node(), format!("loop scope {}", child.scope_id()));
        }
        for (source, target) in &self.edges {
            reachability_builder.add_edge(source.clone(), target.clone())?;
        }
        if let Some((location, summary)) = reachability_builder.find_identity_cycle() {
            let name = names
                .get(&location.node())
                .cloned()
                .unwrap_or_else(|| format!("node {}", location.node()));
            return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                format!(
                    "identity cycle through {name} ({location:?}): its summary {summary:?} does \
                 not advance the timestamp, so the frontier there could never advance"
                ),
            )));
        }

        let (tracker, scope_summary) = reachability_builder.build();

        let routes = match self.routes {
            Some(routes) => routes,
            None => {
                let mut routes: HashMap<Location, Vec<(Location, i64)>> = HashMap::new();
                for (source, target) in &self.edges {
                    routes
                        .entry(source.clone())
                        .or_default()
                        .push((target.clone(), 1));
                }
                routes
            }
        };

        // Where this scope counts each operator port.
        let child_nodes: HashMap<usize, usize> =
            children.iter().map(|c| (c.scope_id(), c.node())).collect();
        let shapes: Vec<&OperatorShape> = self
            .operators
            .values()
            .chain(self.owners.values())
            .collect();
        let mut input_ports: HashMap<usize, Vec<Location>> = HashMap::new();
        let mut output_ports: HashMap<(usize, usize), Location> = HashMap::new();
        for shape in &shapes {
            let inputs: Option<Vec<Location>> = (0..shape.inputs)
                .map(|i| {
                    locate(
                        &self.port_map,
                        &self.operators,
                        &child_nodes,
                        Side::Input,
                        shape.index,
                        i,
                    )
                })
                .collect();
            if let Some(inputs) = inputs {
                if !inputs.is_empty() || self.operators.contains_key(&shape.index) {
                    input_ports.insert(shape.index, inputs);
                }
            }
            for o in 0..shape.outputs {
                if let Some(location) = locate(
                    &self.port_map,
                    &self.operators,
                    &child_nodes,
                    Side::Output,
                    shape.index,
                    o,
                ) {
                    output_ports.insert((shape.index, o), location);
                }
            }
        }
        let op_inputs: HashMap<usize, Vec<Antichain<T>>> = input_ports
            .iter()
            .map(|(&op, locations)| (op, vec![Antichain::new(); locations.len()]))
            .collect();

        // Build per-operator frontier state.
        let mut operator_frontiers = HashMap::new();
        for (&index, shape) in &self.operators {
            operator_frontiers.insert(
                index,
                OperatorFrontierState {
                    output_frontiers: vec![Antichain::new(); shape.outputs],
                },
            );
        }

        // Derived locations: the import (a nested scope's node 0 sources) and
        // the exports (every nested scope's node sources).
        let mut derived: Vec<Location> = Vec::new();
        if self.scope_id != 0 {
            derived.extend((0..self.scope_inputs).map(|i| Location::source(0, i)));
        }
        for child in &children {
            derived.extend((0..child.outputs()).map(|o| Location::source(child.node(), o)));
        }
        // Exact zero judges raw counts only (R38 C2); every location is in
        // scope until a scope is assigned.
        let mut tracker = tracker;
        tracker.set_counted(|location| !derived.contains(location));

        let mut operator_indices: Vec<usize> = self.operators.keys().copied().collect();
        operator_indices.sort();
        let mut materialized_indices: Vec<usize> = self.progress_buffers.keys().copied().collect();
        materialized_indices.sort();

        Ok(ProgressTracker {
            tracker,
            scope_summary,
            scope_id: self.scope_id,
            scope_inputs: self.scope_inputs,
            operators: self.operators,
            connectivity: self.connectivity,
            progress_buffers: self.progress_buffers,
            initial_capabilities: self.initial_capabilities,
            operator_frontiers,
            input_ports,
            output_ports,
            op_inputs,
            operator_indices,
            materialized_indices,
            initialized: false,
            completed: false,
            dirty_operators: Vec::new(),
            progress_channels: None,
            peer_lanes: Vec::new(),
            sealed: false,
            peer_names: Vec::new(),
            activity: 0,
            stopped_quiet: None,
            local_changes: ChangeBatch::new(),
            edges: self.edges,
            routes,
            scope: None,
            derived,
            children,
            probes: Vec::new(),
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check: None,
            #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
            trace: TraceState::default(),
        })
    }
}

/// Where a scope counts `side` port `port` of `op`: per the port map, else
/// at the operator's own node if it is one of the scope's, else nowhere.
fn locate(
    port_map: &HashMap<(Side, usize, usize), PortAt>,
    operators: &HashMap<usize, OperatorShape>,
    child_nodes: &HashMap<usize, usize>,
    side: Side,
    op: usize,
    port: usize,
) -> Option<Location> {
    let at_node = |node: usize, port: usize| match side {
        Side::Input => Location::target(node, port),
        Side::Output => Location::source(node, port),
    };
    match port_map.get(&(side, op, port)) {
        Some(PortAt::Elsewhere) => None,
        Some(PortAt::Boundary(port)) => Some(at_node(0, *port)),
        Some(PortAt::ScopeNode { scope, port }) => {
            child_nodes.get(scope).map(|&node| at_node(node, *port))
        }
        None => operators.contains_key(&op).then(|| at_node(op, port)),
    }
}

impl<T: Timestamp> fmt::Debug for SubgraphBuilder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubgraphBuilder")
            .field("scope_id", &self.scope_id)
            .field("operators", &self.operators.len())
            .field("edges", &self.edges.len())
            .field("scope_inputs", &self.scope_inputs)
            .field("scope_outputs", &self.scope_outputs)
            .field("children", &self.children.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ProgressTracker — live propagation for one scope (and its nested scopes)
// ---------------------------------------------------------------------------

/// Manages live progress propagation for one scope and, through its nested
/// scopes' trackers, for every loop inside it.
///
/// The root tracker is the executor's. Each propagation it:
///
/// 1. **commits**: drains every scope's hosted ledgers and capability
///    reporters into raw port counts, compacted per scope, and self-applies
///    them;
/// 2. **sends** them to every peer as one batch, one section per scope, and
///    **applies** the peers' batches;
/// 3. **exports** each nested scope's internal work to its parent, bottom-up;
/// 4. **propagates** top-down, importing each scope's input frontiers into
///    its nested scopes before they propagate;
/// 5. **checks completion**: every raw count in the worker's scope, in every
///    scope, is exactly zero.
///
/// `initialize()` (or `initialize_for_plan`) must be called once before the
/// first propagation to seed initial capabilities.
pub struct ProgressTracker<T: Timestamp> {
    /// The reachability tracker of this scope's graph.
    tracker: Tracker<T>,
    /// Scope-level summary: how each scope input reaches each scope output.
    scope_summary: PortConnectivity<T::Summary>,
    /// This scope's id (0: the root).
    scope_id: usize,
    /// Number of scope inputs (a nested scope imports one frontier each).
    scope_inputs: usize,
    /// Per-operator internal connectivity (for the worker's scope).
    connectivity: HashMap<usize, PortConnectivity<T::Summary>>,
    /// This scope's operator nodes.
    operators: HashMap<usize, OperatorShape>,
    /// Per-operator shared progress buffers (nodes and boundary operators).
    progress_buffers: HashMap<usize, OperatorProgress<T>>,
    /// Per-operator initial capabilities.
    initial_capabilities: HashMap<usize, Vec<ChangeBatch<T>>>,
    /// Per-operator output frontiers (this scope's nodes).
    operator_frontiers: HashMap<usize, OperatorFrontierState<T>>,
    /// The operators whose inputs this scope counts, with each input's
    /// location.
    input_ports: HashMap<usize, Vec<Location>>,
    /// The operator outputs this scope counts, with their locations.
    output_ports: HashMap<(usize, usize), Location>,
    /// Each operator's current input frontiers (see `input_ports`).
    op_inputs: HashMap<usize, Vec<Antichain<T>>>,
    /// Sorted operator node indices for deterministic iteration.
    operator_indices: Vec<usize>,
    /// Operators with progress buffers (nodes and boundary operators).
    materialized_indices: Vec<usize>,
    /// Whether initial capabilities have been seeded.
    initialized: bool,
    /// Whether the worker's scope was exactly zero after the last
    /// propagation.
    completed: bool,
    /// Operators whose frontiers changed in the last propagation (every
    /// scope's, for the root).
    dirty_operators: Vec<usize>,
    /// Cross-worker progress exchange channels (the root tracker of a
    /// multi-worker dataflow).
    ///
    /// Capability and port changes of every scope are broadcast to all peer
    /// workers, and peer workers' changes are absorbed into every scope's
    /// tracker, so every view covers every worker.
    progress_channels: Option<WorkerProgressChannels<T>>,
    /// Each peer's lane state (by worker index), as of the batches applied.
    peer_lanes: Vec<Option<LaneState>>,
    /// This worker sealed its progress stream (the root tracker only).
    sealed: bool,
    /// Each worker's name for errors (see `set_peer_names`).
    peer_names: Vec<String>,
    /// Commits and peer batches applied so far: any change to the view.
    activity: u64,
    /// When a stopped peer was first seen to leave a drain waiting (R48),
    /// and `activity` then (see [`stopped_and_quiet`](Self::stopped_and_quiet)).
    stopped_quiet: Option<(std::time::Instant, u64)>,
    /// This worker's raw changes in this scope since the last commit,
    /// compacted worker-wide.
    local_changes: ChangeBatch<(ProgressKind, usize, usize, T)>,
    /// This scope's progress edges.
    edges: Vec<(Location, Location)>,
    /// Where produced records are counted: for each source location, the
    /// target of each of its data edges and the edge's multiplicity
    /// (`hosts(target)` for a broadcast, 1 otherwise).
    routes: HashMap<Location, Vec<(Location, i64)>>,
    /// The worker's scope (S2b(c)) within this progress scope: every
    /// location from which a location this worker hosts is reachable, across
    /// scopes. `None` means every location (a tracker initialized without a
    /// plan).
    scope: Option<Vec<Location>>,
    /// Locations holding derived counts (the import and the exports), never
    /// raw ones: excluded from completion (R38 C2) and never sent (C3).
    derived: Vec<Location>,
    /// Nested scopes' trackers.
    children: Vec<Box<dyn ChildScope<T>>>,
    /// Probes on streams this scope counts.
    probes: Vec<AttachedProbe<T>>,
    /// The progress checker's view of this tracker (see
    /// [`check`](crate::progress::check)): every propagation publishes the
    /// frontiers that changed.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check: Option<crate::progress::check::TrackerCheck<T>>,
    /// The flight recorder's positions (see
    /// [`trace`](crate::progress::trace)); the root tracker's.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    trace: TraceState,
}

/// What the flight recorder needs to number a worker's commits and the
/// batches it applies, so that a receiver's lane position maps to the
/// sender's commit (the sequence is derived on both sides, never sent).
#[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
#[derive(Debug, Default)]
struct TraceState {
    /// Non-empty commits sent so far (the last commit's sequence number).
    commits: u64,
    /// Frames sent on each network lane before the next commit: a commit
    /// over the batch cap is split, and every fragment is its own batch at
    /// the receiver.
    frames: u64,
    /// Per peer (by worker index): batches applied from its lane, and the
    /// lane's state as last recorded.
    lanes: Vec<(u64, LaneState)>,
}

/// Per-operator frontier state tracked by the progress tracker.
#[derive(Debug, Clone)]
struct OperatorFrontierState<T: Timestamp> {
    /// Current frontier at each output port.
    output_frontiers: Vec<Antichain<T>>,
}

impl<T: Timestamp> ProgressTracker<T> {
    /// Seeds the declared initial capabilities of a single-worker tracker
    /// (each once) and computes the initial frontiers. See
    /// [`initialize_for_plan`](Self::initialize_for_plan).
    ///
    /// # Errors
    ///
    /// A tracker with progress channels belongs to a multi-worker dataflow:
    /// seeding each capability once would undercount its peers' instances,
    /// so it must use `initialize_for_plan`.
    pub fn initialize(&mut self) -> Result<(), crate::Error> {
        if self.progress_channels.is_some() {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "a progress tracker with progress channels belongs to a multi-worker \
                 dataflow: initialize it from its physical plan (initialize_for_plan), \
                 not with initialize, which seeds each capability once"
                    .into(),
            )));
        }
        self.seed_and_initialize(1, |_| 1)
    }

    /// Seeds the initial capabilities from the physical plan (A4) and
    /// computes the initial frontiers. Must be called once, after
    /// [`set_progress_channels`](Self::set_progress_channels) and before the
    /// first [`propagate`](Self::propagate).
    ///
    /// Every operator's declared initial capabilities (see
    /// [`SubgraphBuilder::add_operator_with_capabilities`]), in every scope,
    /// are seeded once per worker that runs the operator
    /// ([`PhysicalPlan::seed_multiplicity`]), for every operator of the
    /// graph whether or not this worker runs it. Nothing is broadcast: every
    /// worker seeds the same counts, so each view holds every peer's initial
    /// capabilities before any worker runs, and only the releases of those
    /// capabilities travel. Seeding fewer lets a frontier pass work; seeding
    /// more never completes.
    ///
    /// With `graph`, the worker's scope (S2b(c)) is computed from the
    /// operators the plan places on `worker`.
    ///
    /// A tracker of a multi-worker dataflow must have progress channels
    /// (debug assertion): its view must be global.
    ///
    /// In debug builds, no operator may already have reported a capability
    /// through its reporter: one created at materialization instead of
    /// declared would reach peers only when broadcast, after they may have
    /// computed frontiers without it (audit H13).
    pub(crate) fn initialize_for_plan(
        &mut self,
        plan: &PhysicalPlan,
        graph: Option<&DataflowGraph>,
        worker: usize,
    ) -> Result<(), crate::Error> {
        if graph.is_some() {
            self.set_scope(|op| plan.runs(worker, op));
        }
        self.seed_and_initialize(plan.total_workers(), |op| plan.seed_multiplicity(op))
    }

    /// The worker's scope (S2b(c)) in every progress scope: every location
    /// from which a location of an operator `hosted` reports true for is
    /// reachable in the data graph, feedback edges and scope boundaries
    /// included and timestamps ignored.
    fn set_scope(&mut self, hosted: impl Fn(usize) -> bool) {
        let mut reverse = ReverseGraph::new();
        let mut seeds = Vec::new();
        self.reverse_graph(&mut reverse, &mut seeds, &hosted);
        let mut seen: HashSet<ScopeLocation> = HashSet::new();
        let mut stack = seeds;
        while let Some(location) = stack.pop() {
            if seen.insert(location.clone()) {
                if let Some(upstream) = reverse.get(&location) {
                    stack.extend(upstream.iter().cloned());
                }
            }
        }
        self.assign_scope(&seen);
    }

    /// Adds this scope's (and its nested scopes') reverse edges to
    /// `reverse`, and the locations of every port of a hosted operator to
    /// `seeds`.
    fn reverse_graph(
        &self,
        reverse: &mut ReverseGraph,
        seeds: &mut Vec<ScopeLocation>,
        hosted: &dyn Fn(usize) -> bool,
    ) {
        let id = self.scope_id;
        for (source, target) in &self.edges {
            reverse
                .entry((id, target.clone()))
                .or_default()
                .push((id, source.clone()));
        }
        let child_summaries = self
            .children
            .iter()
            .map(|c| (c.node(), c.outer_summary().clone()));
        for (op, connectivity) in self
            .connectivity
            .iter()
            .map(|(&op, c)| (op, c.clone()))
            .chain(child_summaries)
        {
            for (input, output, path) in connectivity.iter() {
                if !path.is_empty() {
                    reverse
                        .entry((id, Location::source(op, output)))
                        .or_default()
                        .push((id, Location::target(op, input)));
                }
            }
        }
        for child in &self.children {
            let (child_id, node) = (child.scope_id(), child.node());
            for input in 0..child.inputs() {
                reverse
                    .entry((child_id, Location::source(0, input)))
                    .or_default()
                    .push((id, Location::target(node, input)));
            }
            for output in 0..child.outputs() {
                reverse
                    .entry((id, Location::source(node, output)))
                    .or_default()
                    .push((child_id, Location::target(0, output)));
            }
            child.reverse_graph(reverse, seeds, hosted);
        }
        for (&op, locations) in &self.input_ports {
            if hosted(op) {
                seeds.extend(locations.iter().map(|l| (id, l.clone())));
            }
        }
        for (&(op, _), location) in &self.output_ports {
            if hosted(op) {
                seeds.push((id, location.clone()));
            }
        }
        // A hosted node's own ports, including those counted elsewhere (a
        // loop's `leave` output feeds the boundary inside the loop).
        for (&op, shape) in &self.operators {
            if hosted(op) {
                seeds.extend((0..shape.inputs).map(|i| (id, Location::target(op, i))));
                seeds.extend((0..shape.outputs).map(|o| (id, Location::source(op, o))));
            }
        }
    }

    /// Keeps the locations of `seen` that are in this scope (and so on for
    /// every nested scope).
    fn assign_scope(&mut self, seen: &HashSet<ScopeLocation>) {
        let mut scope: Vec<Location> = seen
            .iter()
            .filter(|(id, _)| *id == self.scope_id)
            .map(|(_, location)| location.clone())
            .collect();
        scope.sort();
        let derived = &self.derived;
        self.tracker.set_counted(|location| {
            scope.binary_search(location).is_ok() && !derived.contains(location)
        });
        self.scope = Some(scope);
        for child in &mut self.children {
            child.assign_scope(seen);
        }
    }

    /// Seeds each declared initial capability of every scope's operator
    /// `op` `multiplicity(op)` times in a `total_workers`-worker dataflow,
    /// then computes the initial frontiers: export bottom-up, then propagate
    /// top-down with the import (R38 C4).
    fn seed_and_initialize(
        &mut self,
        total_workers: usize,
        multiplicity: impl Fn(usize) -> usize,
    ) -> Result<(), crate::Error> {
        if self.initialized {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "ProgressTracker already initialized".into(),
            )));
        }
        debug_assert!(
            total_workers <= 1 || self.progress_channels.is_some(),
            "the progress tracker of a {total_workers}-worker dataflow has no progress \
             channels: every multi-worker tracker must see every worker's progress"
        );
        self.initialized = true;

        // Seed every declared initial capability once per worker that runs
        // its operator.
        self.seed(&multiplicity);

        // Every initial capability is declared: nothing may have been
        // reported yet. (Release builds account for it as before.)
        let undeclared = self.collect();
        debug_assert!(
            undeclared == 0,
            "{undeclared} progress change(s) were reported before the tracker was \
             initialized: an operator created a capability at materialization instead of \
             declaring it with `add_operator_with_capabilities` (audit H13)"
        );

        // Absorb anything a peer that already runs has sent.
        self.broadcast_local_changes();
        self.receive_peer_changes()?;

        self.export_and_propagate();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_publish(true);

        self.completed = self.raw_zero();
        self.debug_assert_derived_zero();
        Ok(())
    }

    /// Seeds this scope's (and every nested scope's) initial capabilities;
    /// every nested tracker is initialized with its root.
    fn seed(&mut self, multiplicity: &dyn Fn(usize) -> usize) {
        self.initialized = true;
        let initial_caps = std::mem::take(&mut self.initial_capabilities);
        for (index, mut caps) in initial_caps {
            let copies = multiplicity(index) as i64;
            for (output, batch) in caps.iter_mut().enumerate() {
                let Some(location) = self.output_ports.get(&(index, output)).cloned() else {
                    continue;
                };
                for (time, diff) in batch.drain() {
                    self.tracker.update_source(
                        location.node(),
                        location_port(&location),
                        time,
                        diff * copies,
                    );
                }
            }
        }
        for child in &mut self.children {
            child.seed(multiplicity);
        }
    }

    /// Returns whether this tracker has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }
}

/// The port of a location.
fn location_port(location: &Location) -> usize {
    match location {
        Location::Source { port, .. } | Location::Target { port, .. } => *port,
    }
}

impl<T: Timestamp> ProgressTracker<T> {
    /// Whether every raw count in the worker's scope is exactly zero, in this
    /// scope and every nested one, with no change pending (S2b(c), exact
    /// zero). Counts are checked one pointstamp at a time, never summed.
    /// Derived counts (the import and the exports) are not raw and excluded
    /// (R38 C2). A tracker initialized without a plan checks every location.
    pub fn scope_is_zero(&self) -> bool {
        self.raw_zero()
    }

    fn in_scope(&self, location: &Location) -> bool {
        self.scope
            .as_ref()
            .is_none_or(|scope| scope.binary_search(location).is_ok())
    }

    /// O(1) per scope: the tracker keeps the number of in-scope raw
    /// locations whose count is non-zero (see `Tracker::set_counted`).
    fn raw_zero(&self) -> bool {
        if self.tracker.has_pending() || !self.local_changes.is_empty_clean() {
            return false;
        }
        self.tracker.counted_zero() && self.children.iter().all(|c| c.raw_zero())
    }

    /// Whether every derived count in the worker's scope is zero (in this
    /// scope and every nested one).
    fn derived_zero(&self) -> bool {
        self.derived
            .iter()
            .filter(|location| self.in_scope(location))
            .all(|location| self.tracker.counts_zero_at(location))
            && self.children.iter().all(|c| c.derived_zero())
    }

    /// R38 C2: once every raw count in scope is zero, after every scope has
    /// propagated, every derived count in scope must be zero too.
    fn debug_assert_derived_zero(&self) {
        debug_assert!(
            !self.completed || self.derived_zero(),
            "every raw count in this worker's scope is zero, but a derived count \
             (a loop's import or export) is not: {:?}",
            self.derived_residual()
        );
    }

    fn derived_residual(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .tracker
            .nonzero_counts()
            .into_iter()
            .filter(|(location, _, _)| self.derived.contains(location))
            .map(|(location, time, count)| {
                format!("scope {} {location:?}@{time:?}: {count:+}", self.scope_id)
            })
            .collect();
        for child in &self.children {
            child.derived_residual(&mut out);
        }
        out
    }

    /// The non-zero raw counts in the worker's scope, in every scope (for
    /// residual reports).
    pub(crate) fn scope_residual(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.residual(&mut out);
        out
    }

    fn residual(&self, out: &mut Vec<String>) {
        let prefix = if self.scope_id == 0 {
            String::new()
        } else {
            format!("scope {} ", self.scope_id)
        };
        out.extend(
            self.tracker
                .nonzero_counts()
                .into_iter()
                .filter(|(location, _, _)| {
                    self.in_scope(location) && !self.derived.contains(location)
                })
                .map(|(location, time, count)| format!("{prefix}{location:?}@{time:?}: {count:+}")),
        );
        for child in &self.children {
            child.residual(out);
        }
    }

    /// Collects every scope's progress, exchanges it with the peers and
    /// propagates it (see the type-level docs).
    ///
    /// Returns the operators (of every scope) whose frontiers changed.
    pub fn propagate(&mut self) -> Result<&[usize], crate::Error> {
        if !self.initialized {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "must call initialize() first".into(),
            )));
        }

        // 1. Commit this worker's changes since the last commit, in every
        //    scope, and apply them to this view (self-application).
        if self.collect() > 0 {
            self.activity += 1;
        }

        // 2. Send the commit to every peer, as one batch.
        self.broadcast_local_changes();

        // 3. Receive and apply peer workers' changes.
        self.receive_peer_changes()?;

        // 4. Exports bottom-up, then propagation top-down with the import.
        self.export_and_propagate();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_publish(false);

        // 5. Exact zero over the worker's scope.
        self.completed = self.raw_zero();
        self.debug_assert_derived_zero();

        Ok(&self.dirty_operators)
    }

    /// Applies every scope's pending changes with the exports bottom-up,
    /// then propagates every scope top-down with the import, and gathers
    /// every scope's dirty operators.
    fn export_and_propagate(&mut self) {
        self.apply_and_export_children();
        self.tracker.apply_pending();
        self.propagate_and_import();
        let mut dirty = Vec::new();
        for child in &self.children {
            child.dirty_into(&mut dirty);
        }
        self.dirty_operators.extend(dirty);
        self.update_probes();
    }

    /// Phase 1 for the nested scopes: each applies its pending changes
    /// (after its own nested scopes'), and its export is applied to its node
    /// here.
    fn apply_and_export_children(&mut self) {
        for child in &mut self.children {
            let node = child.node();
            for (output, time, diff) in child.apply_and_export() {
                self.tracker.update_source(node, output, time, diff);
            }
        }
    }

    /// Phase 2: propagate this scope, then import its frontiers at each
    /// nested scope's node into that scope, which then propagates.
    fn propagate_and_import(&mut self) {
        self.tracker.propagate_worklist();
        self.update_operator_frontiers();
        for child in &mut self.children {
            let node = child.node();
            let inputs: Vec<Antichain<T>> = (0..child.inputs())
                .map(|input| {
                    Antichain::from_iter(self.tracker.target_frontier(node, input).iter().cloned())
                })
                .collect();
            child.import_and_propagate(&inputs);
        }
    }

    /// Whether the worker's scope was exactly zero after the last
    /// propagation (see [`scope_is_zero`](Self::scope_is_zero)).
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Whether this tracker has progress channels to peers (a multi-worker
    /// dataflow).
    pub(crate) fn has_peers(&self) -> bool {
        self.progress_channels.is_some()
    }

    /// Returns `true` if there are any outstanding capabilities or pending changes.
    pub fn is_tracking(&self) -> bool {
        self.tracker.tracking_anything() || self.children.iter().any(|c| c.is_tracking())
    }

    /// Attach cross-worker progress exchange channels.
    ///
    /// Must be called before [`initialize_for_plan()`](Self::initialize_for_plan).
    pub fn set_progress_channels(
        &mut self,
        channels: WorkerProgressChannels<T>,
    ) -> Result<(), crate::Error> {
        if self.initialized {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "set_progress_channels must be called before initialize()".into(),
            )));
        }
        self.progress_channels = Some(channels);
        Ok(())
    }

    // -- commit and exchange --

    /// Commits this worker's changes since the last commit, in this scope and
    /// every nested one: drains every ledger and capability reporter (release
    /// mailbox) into `local_changes`, compacted per scope, and applies them
    /// to this view.
    ///
    /// - `consumed[in]` becomes `Target(in's location): -n`;
    /// - `produced[out]` becomes `Target(edge.target): +n * mult` for every
    ///   data edge from `out`'s location;
    /// - `internal[out]` (capabilities) becomes `Source(out's location)`.
    ///
    /// A port this scope does not count is another scope's (a boundary
    /// operator's port of the other time); its ledger here stays empty.
    ///
    /// Returns the number of changes drained.
    fn collect(&mut self) -> usize {
        let mut collected = self.collect_operator_progress();
        for child in &mut self.children {
            collected += child.collect();
        }
        debug_assert!(
            !self.sealed || collected == 0,
            "{collected} progress update(s) after this worker sealed its progress: \
             {:?}",
            self.local_changes
        );
        collected
    }

    /// Whether a hosted ledger or capability reporter, in this scope or a
    /// nested one, holds changes the last commit did not collect. A
    /// capability released on another thread (a dropped `Retained`) updates
    /// its reporter's held count and pending change under one lock, so an
    /// operator seen holding nothing after a commit may still owe that
    /// release to the next one.
    pub(crate) fn uncollected(&self) -> bool {
        self.materialized_indices.iter().any(|index| {
            let progress = &self.progress_buffers[index];
            progress.consumed.iter().any(|ledger| !ledger.is_empty())
                || progress.produced.iter().any(|ledger| !ledger.is_empty())
                || progress
                    .internal
                    .iter()
                    .any(|reporter| !reporter.is_empty())
        }) || self.children.iter().any(|child| child.uncollected())
    }

    fn collect_operator_progress(&mut self) -> usize {
        let mut drained = ChangeBatch::new();
        for &index in &self.materialized_indices {
            let progress = &self.progress_buffers[&index];
            for (input, ledger) in progress.consumed.iter().enumerate() {
                let changes = ledger.drain();
                if changes.is_empty() {
                    continue;
                }
                let location = self.input_ports.get(&index).and_then(|l| l.get(input));
                debug_assert!(
                    location.is_some(),
                    "scope {} drained consumed input {input} of operator {index}, which it \
                     does not count",
                    self.scope_id
                );
                let Some(location) = location else { continue };
                for (time, n) in changes {
                    drained.update(
                        (
                            ProgressKind::Target,
                            location.node(),
                            location_port(location),
                            time,
                        ),
                        -n,
                    );
                }
            }
            for (output, ledger) in progress.produced.iter().enumerate() {
                let changes = ledger.drain();
                if changes.is_empty() {
                    continue;
                }
                let location = self.output_ports.get(&(index, output));
                debug_assert!(
                    location.is_some(),
                    "scope {} drained produced output {output} of operator {index}, which it \
                     does not count",
                    self.scope_id
                );
                let Some(location) = location else { continue };
                let targets = self
                    .routes
                    .get(location)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for (time, n) in changes {
                    for (target, mult) in targets {
                        drained.update(
                            (
                                ProgressKind::Target,
                                target.node(),
                                location_port(target),
                                time.clone(),
                            ),
                            n * mult,
                        );
                    }
                }
            }
            for (output, reporter) in progress.internal.iter().enumerate() {
                let changes = reporter.drain();
                if changes.is_empty() {
                    continue;
                }
                let location = self.output_ports.get(&(index, output));
                debug_assert!(
                    location.is_some(),
                    "scope {} drained capabilities of output {output} of operator {index}, \
                     which it does not count",
                    self.scope_id
                );
                let Some(location) = location else { continue };
                for (time, diff) in changes {
                    drained.update(
                        (
                            ProgressKind::Source,
                            location.node(),
                            location_port(location),
                            time,
                        ),
                        diff,
                    );
                }
            }
        }
        let mut collected = 0;
        for ((kind, node, port, time), diff) in drained.drain() {
            collected += 1;
            match kind {
                ProgressKind::Source => self.tracker.update_source(node, port, time.clone(), diff),
                ProgressKind::Target => self.tracker.update_target(node, port, time.clone(), diff),
            }
            self.local_changes.update((kind, node, port, time), diff);
        }
        collected
    }

    /// Takes this scope's committed raw changes as one section. Derived
    /// changes are never committed (R38 C3).
    fn take_section(&mut self) -> Vec<ProgressChange<T>> {
        let changes: Vec<ProgressChange<T>> = self
            .local_changes
            .drain()
            .map(|((kind, node, port, time), diff)| ProgressChange {
                kind,
                node,
                port,
                time,
                diff,
            })
            .collect();
        debug_assert!(
            changes.iter().all(|change| {
                change.kind == ProgressKind::Target
                    || !self
                        .derived
                        .contains(&Location::source(change.node, change.port))
            }),
            "scope {} would send a derived (import or export) count: {:?}",
            self.scope_id,
            changes
        );
        changes
    }

    /// Takes every nested scope's section (non-empty ones), in scope order.
    fn take_child_sections(&mut self, out: &mut Vec<(usize, Arc<dyn Any + Send + Sync>)>) {
        for child in &mut self.children {
            child.take_sections(out);
        }
    }

    /// Sends this worker's committed changes, every scope's section in one
    /// batch, to every peer worker. Without progress channels (a single
    /// worker), the committed changes are simply dropped.
    fn broadcast_local_changes(&mut self) {
        let root = self.take_section();
        let mut scopes = Vec::new();
        self.take_child_sections(&mut scopes);
        #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
        let commit = self.trace_commit(&root, &scopes);
        let Some(channels) = &self.progress_channels else {
            traced! {
                if let Some(commit) = commit {
                    self.trace_sent(commit, 0);
                }
            }
            return;
        };
        let batch = ProgressBatch { root, scopes };
        if batch.is_empty() {
            return;
        }
        traced! {
            crate::progress::trace::begin_sends(commit.as_ref().map_or(0, |c| c.0));
        }
        for sender in channels.senders.iter().flatten() {
            sender.send_batch(batch.clone());
        }
        traced! {
            let lanes = channels.senders.iter().flatten().count() as u64;
            if let Some(commit) = commit {
                self.trace_sent(commit, lanes);
            }
        }
    }

    /// Number this worker's commit and record its changes (every scope's).
    /// Returns `(sequence number, changes, sections)` for a non-empty
    /// commit.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_commit(
        &mut self,
        root: &[ProgressChange<T>],
        scopes: &[(usize, Arc<dyn Any + Send + Sync>)],
    ) -> Option<(u64, usize, usize)> {
        use crate::progress::trace::{self, Kind, Site};
        if root.is_empty() && scopes.is_empty() {
            return None;
        }
        self.trace.commits += 1;
        let seq = self.trace.commits;
        let mut changes = 0;
        trace::with(Site::Tracker, |recorder| {
            changes = trace_batch(
                recorder,
                Kind::CommitChange,
                self.scope_id,
                root,
                scopes,
                &self.children,
                0,
                seq,
            );
        });
        Some((seq, changes, scopes.len() + usize::from(!root.is_empty())))
    }

    /// Record a commit once it was sent on `lanes` lanes, with the frames
    /// each network lane got (see [`TraceState::frames`]).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_sent(&mut self, (seq, changes, sections): (u64, usize, usize), lanes: u64) {
        use crate::progress::trace::{self, Ev, Kind, Site};
        let (frames, network) = trace::end_sends();
        let before = self.trace.frames;
        self.trace.frames += frames;
        trace::with(Site::Tracker, |recorder| {
            recorder.record(
                Ev::new(Kind::Commit)
                    .a(sections)
                    .b(changes)
                    .w(0, seq)
                    .w(1, frames)
                    .w(2, before)
                    .w(3, lanes.saturating_sub(network) << 32 | network),
            );
        });
    }

    /// Receives and applies peer workers' batches: every section to its
    /// scope's tracker, all before any scope propagates.
    ///
    /// No-op if no progress channels are attached (single-worker mode).
    fn receive_peer_changes(&mut self) -> crate::Result<()> {
        let Some(channels) = &self.progress_channels else {
            return Ok(());
        };
        let mut batches = Vec::new();
        let mut lanes = Vec::new();
        for (worker, receiver) in channels.receivers.iter().enumerate() {
            if let Some(receiver) = receiver {
                let (drained, state) = receiver.drain_with_state()?;
                traced! {
                    trace_lane(
                        &mut self.trace,
                        self.scope_id,
                        &self.children,
                        worker,
                        &drained,
                        state,
                    );
                }
                batches.extend(drained);
                lanes.push((worker, state));
            }
        }
        self.activity += batches.len() as u64;
        for batch in batches {
            self.apply_changes(&batch.root);
            for (scope, section) in &batch.scopes {
                if !self.apply_section(*scope, section.as_ref()) {
                    return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                        format!(
                            "a peer sent progress for scope {scope}, which this worker does not have"
                        ),
                    )));
                }
            }
        }
        // A peer's seal counts once the batches before it are applied (here,
        // just above).
        for (worker, state) in lanes {
            if self.peer_lanes.len() <= worker {
                self.peer_lanes.resize(worker + 1, None);
            }
            self.peer_lanes[worker] = Some(state);
        }
        Ok(())
    }

    // -- seals --

    /// Seal this worker's progress stream (design §2.2): it can produce no
    /// further progress update. Its last commit was just sent; the seal
    /// follows it on every peer lane, and the worker's own seal is recorded
    /// here (there is no self lane).
    pub(crate) fn seal(&mut self) {
        debug_assert!(!self.sealed, "a worker sealed its progress twice");
        debug_assert!(
            self.local_changes.is_empty_clean(),
            "a worker sealed with uncommitted progress"
        );
        self.sealed = true;
        if let Some(channels) = &self.progress_channels {
            for sender in channels.senders.iter().flatten() {
                sender.seal();
            }
        }
        traced! {
            use crate::progress::trace::{self, Ev, Kind, Site};
            let lanes = self
                .progress_channels
                .as_ref()
                .map_or(0, |c| c.senders.iter().flatten().count());
            let commits = self.trace.commits;
            trace::with(Site::Tracker, |recorder| {
                recorder.record(Ev::new(Kind::SealSent).b(lanes).w(0, commits));
            });
        }
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_sealed();
    }

    /// This worker sealed: no store of its operators, in any scope, may
    /// hold work (V4).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_sealed(&self) {
        if let Some(check) = &self.check {
            check.sealed();
        }
        for child in &self.children {
            child.check_sealed();
        }
    }

    /// Name each worker for errors (by worker index; on a cluster, its
    /// node's id).
    pub(crate) fn set_peer_names(&mut self, names: Vec<String>) {
        self.peer_names = names;
    }

    /// Worker `worker`'s name for errors.
    pub(crate) fn peer_name(&self, worker: usize) -> String {
        match self.peer_names.get(worker) {
            Some(name) if !name.is_empty() => name.clone(),
            _ => format!("worker {worker}"),
        }
    }

    /// Whether this worker sealed its progress stream.
    pub(crate) fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Whether this worker has applied every worker's seal, its own
    /// included: no further progress update can arrive, so a non-zero raw
    /// count in its scope is a proven residual.
    pub(crate) fn all_sealed(&self) -> bool {
        self.sealed && self.peer_lanes_where(|lane| !lane.sealed).is_empty()
    }

    /// The peers whose progress lane closed without a seal: they stopped
    /// before finishing their part (failed, or cancelled without draining).
    pub(crate) fn stopped_peers(&self) -> Vec<usize> {
        self.peer_lanes_where(|lane| lane.closed && !lane.sealed)
    }

    /// Whether no further progress update can arrive from any peer: every
    /// peer's lane is sealed or closed.
    pub(crate) fn peer_lanes_final(&self) -> bool {
        self.peer_lanes_where(|lane| !lane.sealed && !lane.closed)
            .is_empty()
    }

    /// R48: a stopped peer leaves this worker's drain waiting. Called while
    /// that holds; true once it has held for `grace` without any change to
    /// the view. A stopped peer's records may still be on their way over the
    /// data path (a separate channel from its progress lane), and frames it
    /// sent before it stopped may still reach a lane already closed (they are
    /// applied all the same): either would change the view.
    pub(crate) fn stopped_and_quiet(&mut self, grace: std::time::Duration) -> bool {
        let now = std::time::Instant::now();
        match self.stopped_quiet {
            Some((since, seen)) if seen == self.activity => now.duration_since(since) >= grace,
            _ => {
                self.stopped_quiet = Some((now, self.activity));
                false
            }
        }
    }

    /// R48 no longer holds (see [`stopped_and_quiet`](Self::stopped_and_quiet)).
    pub(crate) fn not_stopped(&mut self) {
        self.stopped_quiet = None;
    }

    /// The peers (worker indices) whose lane state matches `pred`; a peer
    /// not heard from yet has an open, unsealed lane.
    fn peer_lanes_where(&self, pred: impl Fn(&LaneState) -> bool) -> Vec<usize> {
        let Some(channels) = &self.progress_channels else {
            return Vec::new();
        };
        channels
            .receivers
            .iter()
            .enumerate()
            .filter(|(_, receiver)| receiver.is_some())
            .map(|(worker, _)| worker)
            .filter(|&worker| {
                let state = self
                    .peer_lanes
                    .get(worker)
                    .copied()
                    .flatten()
                    .unwrap_or_default();
                pred(&state)
            })
            .collect()
    }

    fn apply_changes(&mut self, changes: &[ProgressChange<T>]) {
        for change in changes {
            match change.kind {
                ProgressKind::Source => self.tracker.update_source(
                    change.node,
                    change.port,
                    change.time.clone(),
                    change.diff,
                ),
                ProgressKind::Target => self.tracker.update_target(
                    change.node,
                    change.port,
                    change.time.clone(),
                    change.diff,
                ),
            }
        }
    }

    /// Applies a peer's section for scope `scope` (this one or a nested
    /// one). Returns whether some scope took it.
    fn apply_section(&mut self, scope: usize, section: &(dyn Any + Send + Sync)) -> bool {
        if scope == self.scope_id {
            match section.downcast_ref::<Vec<ProgressChange<T>>>() {
                Some(changes) => {
                    self.apply_changes(changes);
                    true
                }
                None => false,
            }
        } else {
            self.children
                .iter_mut()
                .any(|child| child.apply_section(scope, section))
        }
    }

    /// Updates this scope's per-operator frontiers from the tracker and
    /// records the operators whose frontiers changed.
    fn update_operator_frontiers(&mut self) {
        self.dirty_operators.clear();
        let mut dirty: HashSet<usize> = HashSet::new();

        for (&op, locations) in &self.input_ports {
            let frontiers = self
                .op_inputs
                .get_mut(&op)
                .expect("frontiers per counted input");
            for (input, location) in locations.iter().enumerate() {
                let new_frontier = Antichain::from_iter(
                    self.tracker
                        .target_frontier(location.node(), location_port(location))
                        .iter()
                        .cloned(),
                );
                if new_frontier != frontiers[input] {
                    frontiers[input] = new_frontier;
                    dirty.insert(op);
                }
            }
        }

        for &index in &self.operator_indices {
            let shape = &self.operators[&index];
            let state = self
                .operator_frontiers
                .get_mut(&index)
                .expect("frontier state exists for tracked operator");
            for output in 0..shape.outputs {
                let new_frontier = Antichain::from_iter(
                    self.tracker.source_frontier(index, output).iter().cloned(),
                );
                if new_frontier != state.output_frontiers[output] {
                    state.output_frontiers[output] = new_frontier;
                    dirty.insert(index);
                }
            }
        }
        let mut dirty: Vec<usize> = dirty.into_iter().collect();
        dirty.sort_unstable();
        self.dirty_operators = dirty;
    }
}

impl<T: Timestamp> ProgressTracker<T> {
    // -- frontiers --

    /// Returns the current input frontier of an operator whose inputs this
    /// scope counts.
    pub fn input_frontier(&self, operator: usize, input: usize) -> &Antichain<T> {
        debug_assert!(
            self.op_inputs.contains_key(&operator),
            "input_frontier: operator {operator} has no inputs in this scope"
        );
        &self.op_inputs[&operator][input]
    }

    /// Returns the current output frontier for an operator.
    pub fn output_frontier(&self, operator: usize, output: usize) -> &Antichain<T> {
        debug_assert!(
            self.operator_frontiers.contains_key(&operator),
            "output_frontier: operator {operator} not registered"
        );
        &self.operator_frontiers[&operator].output_frontiers[output]
    }

    /// Returns the frontier of the stream produced at output `output` of
    /// `operator`, a stream this scope counts: the times that may still
    /// appear on it, including records already on its edges. It is the meet
    /// of the input frontiers of the stream's targets (which include those
    /// records and everything upstream, the operator's own capabilities
    /// among them), or the output's own frontier if the stream has no
    /// consumer.
    ///
    /// Conservatively returns `[T::minimum()]` before initialization or for an
    /// unknown location, so a caller can never observe premature completion.
    pub fn stream_frontier(&self, operator: usize, output: usize) -> Antichain<T> {
        if !self.initialized {
            return Antichain::from_elem(T::minimum());
        }
        let Some(location) = self.output_ports.get(&(operator, output)) else {
            return Antichain::from_elem(T::minimum());
        };
        match self
            .routes
            .get(location)
            .filter(|targets| !targets.is_empty())
        {
            Some(targets) => {
                let mut meet = Antichain::new();
                for (target, _) in targets {
                    for time in self
                        .tracker
                        .target_frontier(target.node(), location_port(target))
                    {
                        meet.insert(time.clone());
                    }
                }
                meet
            }
            None => Antichain::from_iter(
                self.tracker
                    .source_frontier(location.node(), location_port(location))
                    .iter()
                    .cloned(),
            ),
        }
    }

    /// Returns the operators (of every scope) whose frontiers changed in the
    /// last propagation.
    pub fn dirty_operators(&self) -> &[usize] {
        &self.dirty_operators
    }

    fn dirty_into(&self, out: &mut Vec<usize>) {
        out.extend_from_slice(&self.dirty_operators);
        for child in &self.children {
            child.dirty_into(out);
        }
    }

    /// Returns a reference to the shared progress buffers for an operator.
    pub fn operator_progress(&self, index: usize) -> Option<&OperatorProgress<T>> {
        self.progress_buffers.get(&index)
    }

    /// Returns operator shape information.
    pub fn operator_shape(&self, index: usize) -> Option<&OperatorShape> {
        self.operators.get(&index)
    }

    /// Returns all registered operator indices (sorted).
    pub fn operator_indices(&self) -> &[usize] {
        &self.operator_indices
    }

    /// Returns each input's frontier for an operator whose inputs this scope
    /// counts (empty if unknown).
    pub fn operator_input_frontiers(&self, operator: usize) -> Vec<Antichain<T>> {
        self.op_inputs.get(&operator).cloned().unwrap_or_default()
    }

    /// Returns the meet (intersection) of all input frontiers for an
    /// operator whose inputs this scope counts.
    ///
    /// The meet represents the "combined" input frontier: a timestamp is
    /// complete only when ALL input ports have advanced past it. An operator
    /// without inputs gets `[T::minimum()]`, so its notifications never fire
    /// on their own. Returns an empty antichain for an unknown operator.
    pub fn operator_input_frontier_meet(&self, operator: usize) -> Antichain<T> {
        match self.op_inputs.get(&operator) {
            Some(frontiers) => meet(frontiers),
            None => Antichain::new(),
        }
    }

    // -- type-erased access for the executor (every scope) --

    /// Each input frontier of `operator`, in whichever scope counts its
    /// inputs, as `Antichain<_>` of that scope's time.
    pub(crate) fn input_frontiers_any(&self, operator: usize) -> Option<Vec<Box<dyn Any + Send>>> {
        match self.op_inputs.get(&operator) {
            Some(frontiers) => Some(
                frontiers
                    .iter()
                    .map(|f| Box::new(f.clone()) as Box<dyn Any + Send>)
                    .collect(),
            ),
            None => self
                .children
                .iter()
                .find_map(|c| c.input_frontiers_any(operator)),
        }
    }

    /// The meet of `operator`'s input frontiers (see
    /// [`operator_input_frontier_meet`](Self::operator_input_frontier_meet)),
    /// in whichever scope counts its inputs.
    pub(crate) fn input_meet_any(&self, operator: usize) -> Option<Box<dyn Any + Send>> {
        match self.op_inputs.get(&operator) {
            Some(frontiers) => Some(Box::new(meet(frontiers))),
            None => self
                .children
                .iter()
                .find_map(|c| c.input_meet_any(operator)),
        }
    }

    /// Whether every input frontier of `operator` is empty (S2a). An
    /// operator no scope counts inputs for has none.
    pub(crate) fn inputs_empty(&self, operator: usize) -> bool {
        self.inputs_empty_opt(operator).unwrap_or(true)
    }

    fn inputs_empty_opt(&self, operator: usize) -> Option<bool> {
        match self.op_inputs.get(&operator) {
            Some(frontiers) => Some(frontiers.iter().all(Antichain::is_empty)),
            None => self
                .children
                .iter()
                .find_map(|c| c.inputs_empty_opt(operator)),
        }
    }

    /// `operator`'s input frontier meet, formatted (for timelines).
    pub(crate) fn input_debug(&self, operator: usize) -> Option<String> {
        match self.op_inputs.get(&operator) {
            Some(frontiers) => Some(format!("{:?}", meet(frontiers))),
            None => self.children.iter().find_map(|c| c.input_debug(operator)),
        }
    }

    // -- probes --

    /// Attaches a writer of `registration` (a `ProbeRegistration` of the
    /// probed stream's time) to the scope that counts the stream. Returns
    /// whether a scope took it.
    pub(crate) fn register_probe(
        &mut self,
        operator: usize,
        output: usize,
        registration: &(dyn Any + Send + Sync),
        writers: usize,
    ) -> bool {
        if self.output_ports.contains_key(&(operator, output)) {
            if let Some(registration) = registration.downcast_ref::<ProbeRegistration<T>>() {
                let mut probe = AttachedProbe::new(registration, writers);
                probe.writer.update(&self.stream_frontier(operator, output));
                self.probes.push(probe);
                return true;
            }
        }
        self.children
            .iter_mut()
            .any(|c| c.register_probe(operator, output, registration, writers))
    }

    /// Updates every probe (of every scope) with its stream's frontier.
    fn update_probes(&mut self) {
        for i in 0..self.probes.len() {
            let (operator, output) = (self.probes[i].operator, self.probes[i].output);
            let frontier = self.stream_frontier(operator, output);
            self.probes[i].writer.update(&frontier);
        }
        for child in &mut self.children {
            child.update_probes();
        }
    }

    /// Reports this executor's completion to its probes (once), in every
    /// scope: `ok` finishes them with an empty frontier, as the worker
    /// retired with nothing left in its scope; otherwise with the last view.
    pub(crate) fn finish_probes(&mut self, ok: bool) {
        let probes = std::mem::take(&mut self.probes);
        for mut probe in probes {
            let last = if ok {
                Antichain::new()
            } else {
                self.stream_frontier(probe.operator, probe.output)
            };
            probe.writer.finish(Some(&last));
        }
        for child in &mut self.children {
            child.finish_probes(ok);
        }
    }
    /// Whether any probe is registered (in any scope).
    pub(crate) fn has_probes(&self) -> bool {
        !self.probes.is_empty() || self.children.iter().any(|c| c.has_probes())
    }
}

/// The meet of input frontiers: a time is complete only once every input
/// has advanced past it. No inputs: `[T::minimum()]`.
fn meet<T: Timestamp>(frontiers: &[Antichain<T>]) -> Antichain<T> {
    match frontiers {
        [] => Antichain::from_elem(T::minimum()),
        [one] => one.clone(),
        many => {
            let mut combined = Antichain::new();
            for frontier in many {
                for elem in frontier.elements() {
                    combined.insert(elem.clone());
                }
            }
            combined
        }
    }
}

impl<T: Timestamp> ProgressTracker<T> {
    // -- progress checker --

    /// Attach this tracker to the progress checker's oracle as `worker`'s
    /// view. Call before [`initialize`](Self::initialize) so the initial
    /// frontiers are published too.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn attach_check(
        &mut self,
        oracle: Arc<crate::progress::check::Oracle<T>>,
        worker: usize,
        label: &'static str,
    ) {
        self.check = Some(crate::progress::check::TrackerCheck::new(
            oracle, worker, label,
        ));
    }

    /// Attach every nested scope's tracker to its scope's oracle (from
    /// `ctx`), as `worker`'s view.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn attach_check_children(
        &mut self,
        ctx: &crate::progress::check::Ctx,
        label: &'static str,
    ) {
        for child in &mut self.children {
            child.attach_check(ctx, label);
        }
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn attach_check_any(&mut self, ctx: &crate::progress::check::Ctx, label: &'static str) {
        if let Some(oracle) = ctx.typed_oracle::<T>(self.scope_id) {
            self.check = Some(crate::progress::check::TrackerCheck::new(
                oracle, ctx.worker, label,
            ));
        }
        self.attach_check_children(ctx, label);
    }

    /// Publish this view's frontiers to the oracle: every operator port this
    /// scope counts, or only those of operators that changed in the last
    /// propagation. Nested scopes publish to their own oracles.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_publish(&self, all: bool) {
        if let Some(check) = &self.check {
            let dirty: HashSet<usize> = self.dirty_operators.iter().copied().collect();
            let mut targets = Vec::new();
            let mut sources = Vec::new();
            for (&op, frontiers) in &self.op_inputs {
                if all || dirty.contains(&op) {
                    for (port, frontier) in frontiers.iter().enumerate() {
                        targets.push(((op, port), frontier.clone()));
                    }
                }
            }
            for (&(op, port), location) in &self.output_ports {
                if all || dirty.contains(&op) {
                    let frontier = Antichain::from_iter(
                        self.tracker
                            .source_frontier(location.node(), location_port(location))
                            .iter()
                            .cloned(),
                    );
                    sources.push(((op, port), frontier));
                }
            }
            check.publish(targets, sources);
        }
        for child in &self.children {
            child.check_publish(all);
        }
    }

    /// Stop checking this dataflow (cancelled or failed).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_halt(&self, reason: &str) {
        if let Some(check) = &self.check {
            check.halt(reason);
        }
        for child in &self.children {
            child.check_halt(reason);
        }
    }

    /// This tracker's executor finished (`ok`: successfully). Reports every
    /// scope's residual raw counts (V5) when they are final (it succeeded, or
    /// failed with every seal applied: a proven residual) and retires the
    /// views; when `report_worker`, also tells each oracle the worker
    /// finished.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_finish(&mut self, ok: bool, report_worker: bool) {
        let proven = ok || self.all_sealed();
        self.check_finish_with(ok, proven, report_worker);
    }

    /// [`check_finish`](Self::check_finish), `proven`: the counts are final.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_finish_with(&mut self, ok: bool, proven: bool, report_worker: bool) {
        let residual: Vec<String> = if proven && self.check.is_some() {
            self.tracker
                .nonzero_pointstamps()
                .into_iter()
                // The raw counts of this worker's scope, as exact zero judges
                // them: a location outside it may legitimately hold work.
                .filter(|(location, _, _)| {
                    !self.derived.contains(location) && self.in_scope(location)
                })
                .map(|(location, time, count)| format!("{location:?}@{time:?}: {count:+}"))
                .collect()
        } else {
            Vec::new()
        };
        if let Some(check) = &mut self.check {
            check.finish(ok, residual, report_worker);
        }
        for child in &mut self.children {
            child.check_finish(ok, proven, report_worker);
        }
    }

    /// The oracle this tracker publishes to, if any.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_oracle(&self) -> Option<&Arc<crate::progress::check::Oracle<T>>> {
        self.check.as_ref().map(|c| c.oracle())
    }
}

// ---------------------------------------------------------------------------
// The flight recorder (see `progress::trace`)
// ---------------------------------------------------------------------------

/// Record every change of a batch (the root scope's and each nested
/// section's) as `kind`; returns how many.
#[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
#[allow(clippy::too_many_arguments)]
fn trace_batch<T: Timestamp>(
    recorder: &crate::progress::trace::Recorder,
    kind: crate::progress::trace::Kind,
    root_scope: usize,
    root: &[ProgressChange<T>],
    scopes: &[(usize, Arc<dyn Any + Send + Sync>)],
    children: &[Box<dyn ChildScope<T>>],
    from: usize,
    reference: u64,
) -> usize {
    use crate::progress::trace::Change;
    let mut n = 0;
    for c in root {
        recorder.change(
            kind,
            &Change::of(root_scope, c.kind, c.node, c.port, &c.time, c.diff),
            from,
            reference,
        );
        n += 1;
    }
    for (scope, section) in scopes {
        children.iter().any(|child| {
            child.trace_section(*scope, section.as_ref(), &mut |change| {
                recorder.change(kind, &change, from, reference);
                n += 1;
            })
        });
    }
    n
}

/// Record the batches drained from `worker`'s lane (every change, with the
/// lane position), and the lane's state if it changed.
#[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
fn trace_lane<T: Timestamp>(
    trace: &mut TraceState,
    root_scope: usize,
    children: &[Box<dyn ChildScope<T>>],
    worker: usize,
    drained: &[ProgressBatch<T>],
    state: LaneState,
) {
    use crate::progress::trace::{self, Ev, Kind, Site};
    if trace.lanes.len() <= worker {
        trace.lanes.resize(worker + 1, (0, LaneState::default()));
    }
    let lane = &mut trace.lanes[worker];
    let first = lane.0;
    lane.0 += drained.len() as u64;
    let changed = lane.1 != state;
    lane.1 = state;
    let batches = lane.0;
    if drained.is_empty() && !changed {
        return;
    }
    trace::with(Site::Tracker, |recorder| {
        for (i, batch) in drained.iter().enumerate() {
            let position = first + i as u64 + 1;
            let changes = trace_batch(
                recorder,
                Kind::BatchChange,
                root_scope,
                &batch.root,
                &batch.scopes,
                children,
                worker,
                position,
            );
            recorder.record(
                Ev::new(Kind::BatchApplied)
                    .a(batch.scopes.len() + usize::from(!batch.root.is_empty()))
                    .b(worker)
                    .w(0, position)
                    .w(1, changes as u64),
            );
        }
        if changed {
            recorder.record(
                Ev::new(Kind::LaneState)
                    .a(usize::from(state.sealed) | usize::from(state.closed) << 1)
                    .b(worker)
                    .w(0, batches),
            );
        }
    });
}

#[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
impl<T: Timestamp> ProgressTracker<T> {
    /// Commits and peer batches applied so far (any change to the view).
    pub(crate) fn trace_activity(&self) -> u64 {
        self.activity
    }

    /// Commits this worker sent so far.
    pub(crate) fn trace_commits(&self) -> u64 {
        self.trace.commits
    }

    /// Every non-zero raw count in the worker's scope, in every scope (what
    /// exact zero judges).
    pub(crate) fn trace_nonzero(&self, f: &mut dyn FnMut(crate::progress::trace::Change)) {
        for (location, time, count) in self.tracker.nonzero_counts() {
            if !self.in_scope(&location) || self.derived.contains(&location) {
                continue;
            }
            let (kind, node, port) = match location {
                Location::Source { node, port } => (ProgressKind::Source, node, port),
                Location::Target { node, port } => (ProgressKind::Target, node, port),
            };
            f(crate::progress::trace::Change::of(
                self.scope_id,
                kind,
                node,
                port,
                &time,
                count,
            ));
        }
        for child in &self.children {
            child.trace_nonzero(f);
        }
    }

    /// Every peer lane: `(worker, batches applied, state)`.
    pub(crate) fn trace_lanes(&self) -> Vec<(usize, u64, LaneState)> {
        let Some(channels) = &self.progress_channels else {
            return Vec::new();
        };
        channels
            .receivers
            .iter()
            .enumerate()
            .filter(|(_, receiver)| receiver.is_some())
            .map(|(worker, _)| {
                let batches = self.trace.lanes.get(worker).map_or(0, |lane| lane.0);
                let state = self
                    .peer_lanes
                    .get(worker)
                    .copied()
                    .flatten()
                    .unwrap_or_default();
                (worker, batches, state)
            })
            .collect()
    }

    /// Record the changes of a section of scope `scope` (this one or a
    /// nested one; see [`apply_section`](Self::apply_section)). Returns
    /// whether some scope took it.
    fn trace_section(
        &self,
        scope: usize,
        section: &(dyn Any + Send + Sync),
        f: &mut dyn FnMut(crate::progress::trace::Change),
    ) -> bool {
        if scope == self.scope_id {
            match section.downcast_ref::<Vec<ProgressChange<T>>>() {
                Some(changes) => {
                    for c in changes {
                        f(crate::progress::trace::Change::of(
                            scope, c.kind, c.node, c.port, &c.time, c.diff,
                        ));
                    }
                    true
                }
                None => false,
            }
        } else {
            self.children
                .iter()
                .any(|child| child.trace_section(scope, section, f))
        }
    }
}

impl<T: Timestamp> fmt::Debug for ProgressTracker<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgressTracker")
            .field("scope_id", &self.scope_id)
            .field("operators", &self.operators.len())
            .field("initialized", &self.initialized)
            .field("completed", &self.completed)
            .field("children", &self.children.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Nested scopes
// ---------------------------------------------------------------------------

/// The builder of a nested scope, seen from its parent (time `TP`).
pub(crate) trait ChildBuilder<TP: Timestamp>: Send + Sync {
    fn clone_box(&self) -> Box<dyn ChildBuilder<TP>>;
    fn scope_id(&self) -> usize;
    /// The scope's node id in the parent.
    fn node(&self) -> usize;
    /// Gives this scope (and every nested one) its node id (see
    /// [`SubgraphBuilder::assign_scope_nodes`]).
    fn assign_nodes(&mut self, base: usize);
    fn claim_edges(
        &mut self,
        graph: &DataflowGraph,
        plan: Option<&PhysicalPlan>,
        claimed: &mut [bool],
    ) -> crate::Result<()>;
    fn collect_handles(&self, out: &mut Vec<Box<dyn Any + Send + Sync>>);
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn attach_check_reporters(&self, ctx: &crate::progress::check::Ctx);
    fn build(self: Box<Self>) -> crate::Result<Box<dyn ChildScope<TP>>>;
}

/// A nested scope's tracker, seen from its parent (time `TP`): the bridge
/// (import and export) plus the time-erased operations every scope takes
/// part in.
pub(crate) trait ChildScope<TP: Timestamp>: Send {
    fn scope_id(&self) -> usize;
    fn node(&self) -> usize;
    fn inputs(&self) -> usize;
    fn outputs(&self) -> usize;
    /// The scope's summary from its inputs to its outputs, in the parent's
    /// time: the node's connectivity in the parent.
    fn outer_summary(&self) -> &PortConnectivity<TP::Summary>;

    fn seed(&mut self, multiplicity: &dyn Fn(usize) -> usize);
    fn collect(&mut self) -> usize;
    fn uncollected(&self) -> bool;
    /// Takes this scope's (and every nested scope's) non-empty section.
    fn take_sections(&mut self, out: &mut Vec<(usize, Arc<dyn Any + Send + Sync>)>);
    fn apply_section(&mut self, scope: usize, section: &(dyn Any + Send + Sync)) -> bool;
    /// Applies this scope's pending changes (after its nested scopes'
    /// exports) and returns its export, in the parent's time:
    /// `(output, time, diff)`.
    fn apply_and_export(&mut self) -> Vec<(usize, TP, i64)>;
    /// Imports the parent's frontiers at the scope's inputs, then
    /// propagates (and so on for every nested scope).
    fn import_and_propagate(&mut self, inputs: &[Antichain<TP>]);

    fn raw_zero(&self) -> bool;
    fn derived_zero(&self) -> bool;
    fn derived_residual(&self, out: &mut Vec<String>);
    fn residual(&self, out: &mut Vec<String>);
    fn is_tracking(&self) -> bool;
    fn dirty_into(&self, out: &mut Vec<usize>);
    fn input_frontiers_any(&self, operator: usize) -> Option<Vec<Box<dyn Any + Send>>>;
    fn input_meet_any(&self, operator: usize) -> Option<Box<dyn Any + Send>>;
    fn inputs_empty_opt(&self, operator: usize) -> Option<bool>;
    fn input_debug(&self, operator: usize) -> Option<String>;
    fn register_probe(
        &mut self,
        operator: usize,
        output: usize,
        registration: &(dyn Any + Send + Sync),
        writers: usize,
    ) -> bool;
    fn update_probes(&mut self);
    fn finish_probes(&mut self, ok: bool);
    fn has_probes(&self) -> bool;
    fn reverse_graph(
        &self,
        reverse: &mut ReverseGraph,
        seeds: &mut Vec<ScopeLocation>,
        hosted: &dyn Fn(usize) -> bool,
    );
    fn assign_scope(&mut self, seen: &HashSet<ScopeLocation>);

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn attach_check(&mut self, ctx: &crate::progress::check::Ctx, label: &'static str);
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_publish(&self, all: bool);
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_halt(&self, reason: &str);
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_finish(&mut self, ok: bool, proven: bool, report_worker: bool);
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_sealed(&self);

    /// The flight recorder: record a peer's section (see
    /// [`ProgressTracker::trace_section`]).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_section(
        &self,
        scope: usize,
        section: &(dyn Any + Send + Sync),
        f: &mut dyn FnMut(crate::progress::trace::Change),
    ) -> bool;
    /// The flight recorder: this scope's non-zero raw counts in the
    /// worker's scope (see [`ProgressTracker::trace_nonzero`]).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_nonzero(&self, f: &mut dyn FnMut(crate::progress::trace::Change));
}

/// The builder of an `iterate` loop's scope, in `Product<TP, TI>` time.
pub(crate) struct LoopBuilder<TP: Timestamp, TI: Timestamp> {
    node: usize,
    builder: SubgraphBuilder<Product<TP, TI>>,
}

impl<TP: Timestamp, TI: Timestamp> LoopBuilder<TP, TI> {
    /// `builder` is the loop body's scope (one input, one output).
    pub(crate) fn new(builder: SubgraphBuilder<Product<TP, TI>>) -> Self {
        Self {
            node: usize::MAX,
            builder,
        }
    }
}

impl<TP: Timestamp, TI: Timestamp> ChildBuilder<TP> for LoopBuilder<TP, TI> {
    fn clone_box(&self) -> Box<dyn ChildBuilder<TP>> {
        Box::new(Self {
            node: self.node,
            builder: self.builder.clone(),
        })
    }

    fn scope_id(&self) -> usize {
        self.builder.scope_id
    }

    fn node(&self) -> usize {
        self.node
    }

    fn assign_nodes(&mut self, base: usize) {
        self.node = base + self.builder.scope_id - 1;
        self.builder.assign_scope_nodes(base);
    }

    fn claim_edges(
        &mut self,
        graph: &DataflowGraph,
        plan: Option<&PhysicalPlan>,
        claimed: &mut [bool],
    ) -> crate::Result<()> {
        self.builder.claim_edges(graph, plan, claimed)
    }

    fn collect_handles(&self, out: &mut Vec<Box<dyn Any + Send + Sync>>) {
        self.builder.collect_handles(out);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn attach_check_reporters(&self, ctx: &crate::progress::check::Ctx) {
        self.builder.attach_check_reporters(ctx);
    }

    fn build(self: Box<Self>) -> crate::Result<Box<dyn ChildScope<TP>>> {
        debug_assert!(
            self.node != usize::MAX,
            "a loop scope must have its node id before it is built"
        );
        let tracker = self.builder.try_build()?;
        // The loop's connectivity in the parent: its scope summary projected
        // onto the outer time.
        let inner = &tracker.scope_summary;
        let mut outer = PortConnectivity::new(inner.inputs(), inner.outputs());
        for (input, output, path) in inner.iter() {
            for summary in path.elements() {
                outer.path_mut(input, output).insert(summary.outer.clone());
            }
        }
        let imported = vec![Antichain::new(); tracker.scope_inputs];
        Ok(Box::new(LoopScope {
            node: self.node,
            tracker,
            outer_summary: outer,
            imported,
        }))
    }
}

/// An `iterate` loop's tracker, in `Product<TP, TI>` time, bridged to its
/// parent's time `TP`.
struct LoopScope<TP: Timestamp, TI: Timestamp> {
    node: usize,
    tracker: ProgressTracker<Product<TP, TI>>,
    outer_summary: PortConnectivity<TP::Summary>,
    /// The derived pointstamps at each `Source(0, i)`: the parent's frontier
    /// at `Target(S, i)`, at `(t, TI::minimum())`.
    imported: Vec<Antichain<Product<TP, TI>>>,
}

impl<TP: Timestamp, TI: Timestamp> ChildScope<TP> for LoopScope<TP, TI> {
    fn scope_id(&self) -> usize {
        self.tracker.scope_id
    }

    fn node(&self) -> usize {
        self.node
    }

    fn inputs(&self) -> usize {
        self.outer_summary.inputs()
    }

    fn outputs(&self) -> usize {
        self.outer_summary.outputs()
    }

    fn outer_summary(&self) -> &PortConnectivity<TP::Summary> {
        &self.outer_summary
    }

    fn seed(&mut self, multiplicity: &dyn Fn(usize) -> usize) {
        self.tracker.seed(multiplicity);
    }

    fn collect(&mut self) -> usize {
        self.tracker.collect()
    }

    fn uncollected(&self) -> bool {
        self.tracker.uncollected()
    }

    fn take_sections(&mut self, out: &mut Vec<(usize, Arc<dyn Any + Send + Sync>)>) {
        let section = self.tracker.take_section();
        if !section.is_empty() {
            out.push((self.tracker.scope_id, Arc::new(section)));
        }
        self.tracker.take_child_sections(out);
    }

    fn apply_section(&mut self, scope: usize, section: &(dyn Any + Send + Sync)) -> bool {
        self.tracker.apply_section(scope, section)
    }

    fn apply_and_export(&mut self) -> Vec<(usize, TP, i64)> {
        self.tracker.apply_and_export_children();
        self.tracker.tracker.apply_pending();
        let mut exports = Vec::new();
        for output in 0..self.outputs() {
            let mut projected: ChangeBatch<TP> = ChangeBatch::new();
            for (time, diff) in self.tracker.tracker.drain_export_changes(output) {
                projected.update(time.outer, diff);
            }
            exports.extend(projected.drain().map(|(time, diff)| (output, time, diff)));
        }
        exports
    }

    fn import_and_propagate(&mut self, inputs: &[Antichain<TP>]) {
        for (input, frontier) in inputs.iter().enumerate() {
            let imported: Antichain<Product<TP, TI>> = frontier
                .elements()
                .iter()
                .map(|time| Product::new(time.clone(), TI::minimum()))
                .collect();
            let previous = std::mem::replace(&mut self.imported[input], imported);
            for time in previous.elements() {
                if !self.imported[input].elements().contains(time) {
                    self.tracker
                        .tracker
                        .update_source(0, input, time.clone(), -1);
                }
            }
            for time in self.imported[input].elements() {
                if !previous.elements().contains(time) {
                    self.tracker
                        .tracker
                        .update_source(0, input, time.clone(), 1);
                }
            }
        }
        // The import sits at node 0, which the export ignores.
        self.tracker.tracker.apply_pending();
        self.tracker.propagate_and_import();
    }

    fn raw_zero(&self) -> bool {
        self.tracker.raw_zero()
    }

    fn derived_zero(&self) -> bool {
        self.tracker.derived_zero()
    }

    fn derived_residual(&self, out: &mut Vec<String>) {
        out.extend(self.tracker.derived_residual());
    }

    fn residual(&self, out: &mut Vec<String>) {
        self.tracker.residual(out);
    }

    fn is_tracking(&self) -> bool {
        self.tracker.is_tracking()
    }

    fn dirty_into(&self, out: &mut Vec<usize>) {
        self.tracker.dirty_into(out);
    }

    fn input_frontiers_any(&self, operator: usize) -> Option<Vec<Box<dyn Any + Send>>> {
        self.tracker.input_frontiers_any(operator)
    }

    fn input_meet_any(&self, operator: usize) -> Option<Box<dyn Any + Send>> {
        self.tracker.input_meet_any(operator)
    }

    fn inputs_empty_opt(&self, operator: usize) -> Option<bool> {
        self.tracker.inputs_empty_opt(operator)
    }

    fn input_debug(&self, operator: usize) -> Option<String> {
        self.tracker.input_debug(operator)
    }

    fn register_probe(
        &mut self,
        operator: usize,
        output: usize,
        registration: &(dyn Any + Send + Sync),
        writers: usize,
    ) -> bool {
        self.tracker
            .register_probe(operator, output, registration, writers)
    }

    fn update_probes(&mut self) {
        self.tracker.update_probes();
    }

    fn finish_probes(&mut self, ok: bool) {
        self.tracker.finish_probes(ok);
    }

    fn has_probes(&self) -> bool {
        self.tracker.has_probes()
    }

    fn reverse_graph(
        &self,
        reverse: &mut ReverseGraph,
        seeds: &mut Vec<ScopeLocation>,
        hosted: &dyn Fn(usize) -> bool,
    ) {
        self.tracker.reverse_graph(reverse, seeds, hosted);
    }

    fn assign_scope(&mut self, seen: &HashSet<ScopeLocation>) {
        self.tracker.assign_scope(seen);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn attach_check(&mut self, ctx: &crate::progress::check::Ctx, label: &'static str) {
        self.tracker.attach_check_any(ctx, label);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_publish(&self, all: bool) {
        self.tracker.check_publish(all);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_halt(&self, reason: &str) {
        self.tracker.check_halt(reason);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_finish(&mut self, ok: bool, proven: bool, report_worker: bool) {
        self.tracker.check_finish_with(ok, proven, report_worker);
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_sealed(&self) {
        self.tracker.check_sealed();
    }

    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_section(
        &self,
        scope: usize,
        section: &(dyn Any + Send + Sync),
        f: &mut dyn FnMut(crate::progress::trace::Change),
    ) -> bool {
        self.tracker.trace_section(scope, section, f)
    }

    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_nonzero(&self, f: &mut dyn FnMut(crate::progress::trace::Change)) {
        self.tracker.trace_nonzero(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::capability::Capability;
    use crate::progress::reachability::Location;

    /// Helper: build a linear pipeline of N pass-through operators.
    /// Returns (SubgraphBuilder, operator_indices).
    fn linear_pipeline(n: usize) -> SubgraphBuilder<u64> {
        let mut builder = SubgraphBuilder::new(1, 1);

        // Register N operators with identity connectivity.
        for i in 1..=n {
            builder
                .add_operator(i, format!("op{i}"), 1, 1, PortConnectivity::identity(0u64))
                .unwrap();
        }

        // Wire: scope_input → op1 → op2 → ... → opN → scope_output
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        for i in 1..n {
            builder.add_edge(Location::source(i, 0), Location::target(i + 1, 0));
        }
        builder.add_edge(Location::source(n, 0), Location::target(0, 0));

        builder
    }

    // --- SubgraphBuilder tests ---

    #[test]
    fn builder_register_and_count() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder
            .add_operator(2, "op2", 2, 1, PortConnectivity::new(2, 1))
            .unwrap();
        assert_eq!(builder.operator_count(), 2);
    }

    #[test]
    fn builder_index_zero_returns_error() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        let result = builder.add_operator(0, "bad", 1, 1, PortConnectivity::identity(0u64));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved"));
    }

    #[test]
    fn builder_duplicate_index_returns_error() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let result = builder.add_operator(1, "op1_dup", 1, 1, PortConnectivity::identity(0u64));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already registered")
        );
    }

    #[test]
    fn builder_operator_progress_returned() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        let progress = builder
            .add_operator(1, "op1", 2, 3, PortConnectivity::new(2, 3))
            .unwrap();
        assert_eq!(progress.consumed.len(), 2);
        assert_eq!(progress.produced.len(), 3);
        assert_eq!(progress.internal.len(), 3);
    }

    #[test]
    fn builder_with_initial_capabilities() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 0);
        let mut cap_batch = ChangeBatch::new();
        cap_batch.update(0u64, 1);
        builder
            .add_operator_with_capabilities(
                1,
                "source",
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![cap_batch],
            )
            .unwrap();
        assert_eq!(builder.operator_count(), 1);
    }

    #[test]
    fn builder_initial_caps_length_mismatch_returns_error() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        let mut cap = ChangeBatch::new();
        cap.update(0u64, 1);
        // Operator has 2 outputs but only 1 initial_caps batch
        let result = builder.add_operator_with_capabilities(
            1,
            "bad",
            0,
            2,
            PortConnectivity::new(0, 2),
            vec![cap],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("initial_caps length")
        );
    }

    // --- ProgressTracker: linear pipeline ---

    #[test]
    fn tracker_linear_capability_advance() {
        let builder = linear_pipeline(3);
        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // No capabilities yet — should be completed.
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_linear_with_capability_hold() {
        // Build a 2-operator pipeline with initial capability on op1.
        let mut builder = SubgraphBuilder::new(1, 1);
        builder
            .add_operator_with_capabilities(
                1,
                "source",
                1,
                1,
                PortConnectivity::identity(0u64),
                vec![{
                    let mut b = ChangeBatch::new();
                    b.update(0u64, 1);
                    b
                }],
            )
            .unwrap();
        builder
            .add_operator(2, "sink", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // Capability at time 0 is held → not completed.
        assert!(!tracker.is_completed());
        assert!(tracker.is_tracking());

        // Op2's input frontier should include time 0.
        let frontier = tracker.input_frontier(2, 0);
        assert!(
            !frontier.is_empty(),
            "op2 input frontier should reflect capability at 0"
        );
    }

    #[test]
    fn tracker_capability_via_reporter() {
        // Build a simple 1-operator graph.
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let reporter = progress.reporter(0).clone();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // Initially empty — no capabilities.
        assert!(tracker.is_completed());

        // Create a capability via the reporter (simulating Capability::new).
        let _cap = Capability::new(5u64, reporter.clone());

        // Propagate — should see the capability.
        tracker.propagate().unwrap();
        assert!(!tracker.is_completed());

        // Drop the capability.
        drop(_cap);

        // Propagate — should be completed again.
        tracker.propagate().unwrap();
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_capability_downgrade_advances_frontier() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let reporter = progress.reporter(0).clone();
        builder
            .add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // Hold capability at time 0.
        let mut cap = Capability::new(0u64, reporter.clone());
        tracker.propagate().unwrap();

        // Op2 frontier should include time 0.
        let f1 = tracker.input_frontier(2, 0);
        assert!(!f1.is_empty());
        assert!(f1.less_equal(&0));

        // Downgrade to time 5.
        cap.downgrade(&5).unwrap();
        tracker.propagate().unwrap();

        let f2 = tracker.input_frontier(2, 0);
        assert!(!f2.less_equal(&4), "frontier should have advanced past 4");
        assert!(f2.less_equal(&5), "frontier should include 5");

        // Drop capability entirely.
        drop(cap);
        tracker.propagate().unwrap();

        let f3 = tracker.input_frontier(2, 0);
        assert!(f3.is_empty(), "frontier should be empty after cap drop");
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_fan_out() {
        // One source, two consumers.
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder
            .add_operator(1, "source", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let reporter = progress.reporter(0).clone();
        builder
            .add_operator(2, "sink_a", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder
            .add_operator(3, "sink_b", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        // Fan-out: op1 output → both op2 and op3 inputs.
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(1, 0), Location::target(3, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));
        // op3 output is a dead end for scope output (doesn't feed scope output).

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        let _cap = Capability::new(10u64, reporter.clone());
        tracker.propagate().unwrap();

        // Both sinks should see the capability.
        assert!(!tracker.input_frontier(2, 0).is_empty());
        assert!(!tracker.input_frontier(3, 0).is_empty());

        // Drop capability.
        drop(_cap);
        tracker.propagate().unwrap();

        assert!(tracker.input_frontier(2, 0).is_empty());
        assert!(tracker.input_frontier(3, 0).is_empty());
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_dirty_operators_reported() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let reporter = progress.reporter(0).clone();
        builder
            .add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // Add a capability — should dirty both operators.
        let _cap = Capability::new(0u64, reporter.clone());
        let dirty = tracker.propagate().unwrap();
        assert!(
            dirty.contains(&1) || dirty.contains(&2),
            "at least one operator should be dirty"
        );

        // Propagate again with no changes — no dirty operators.
        let dirty2 = tracker.propagate().unwrap();
        assert!(dirty2.is_empty(), "no changes, no dirty operators");
    }

    #[test]
    fn tracker_multiple_capabilities() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder
            .add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let reporter = progress.reporter(0).clone();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize().unwrap();

        // Hold capabilities at times 5 and 10.
        let cap5 = Capability::new(5u64, reporter.clone());
        let cap10 = Capability::new(10u64, reporter.clone());
        tracker.propagate().unwrap();
        assert!(!tracker.is_completed());

        // Drop cap at 5 — should still be tracking (cap at 10 remains).
        drop(cap5);
        tracker.propagate().unwrap();
        assert!(!tracker.is_completed());

        // Drop cap at 10 — now completed.
        drop(cap10);
        tracker.propagate().unwrap();
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_operator_indices_sorted() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator(5, "op5", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder
            .add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder
            .add_operator(8, "op8", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        let tracker = builder.build();
        assert_eq!(tracker.operator_indices(), &[2, 5, 8]);
    }

    #[test]
    fn tracker_operator_shape() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator(1, "my_op", 2, 3, PortConnectivity::new(2, 3))
            .unwrap();
        let tracker = builder.build();
        let shape = tracker.operator_shape(1).unwrap();
        assert_eq!(shape.name, "my_op");
        assert_eq!(shape.inputs, 2);
        assert_eq!(shape.outputs, 3);
    }

    /// upstream(op1) --edge--> downstream(op2), with scope ports.
    fn upstream_downstream(caps: bool) -> (SubgraphBuilder<u64>, Option<ProgressReporter<u64>>) {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let upstream = if caps {
            let mut held = ChangeBatch::new();
            held.update(0u64, 1);
            Some(
                builder
                    .add_operator_with_capabilities(
                        1,
                        "upstream",
                        1,
                        1,
                        PortConnectivity::identity(0u64),
                        vec![held],
                    )
                    .unwrap()
                    .reporter(0)
                    .clone(),
            )
        } else {
            builder
                .add_operator(1, "upstream", 1, 1, PortConnectivity::identity(0u64))
                .unwrap();
            None
        };
        builder
            .add_operator(2, "downstream", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));
        (builder, upstream)
    }

    /// A record produced on an edge and not yet consumed keeps the
    /// tracker incomplete, even though no operator holds a capability: it
    /// is counted at the edge's target from the producer's ledger to the
    /// consumer's.
    #[test]
    fn produced_record_keeps_tracker_incomplete_until_consumed() {
        let (builder, _) = upstream_downstream(false);
        let mut tracker = builder.build();
        tracker.initialize().unwrap();
        assert!(tracker.is_completed(), "no capabilities: starts completed");

        let produced = tracker.operator_progress(1).unwrap().produced[0].clone();
        let consumed = tracker.operator_progress(2).unwrap().consumed[0].clone();
        produced.update(5u64, 1);
        tracker.propagate().unwrap();
        assert!(
            !tracker.is_completed(),
            "an unconsumed record must keep the tracker incomplete"
        );
        assert!(tracker.input_frontier(2, 0).less_equal(&5));

        consumed.update(5u64, 1);
        tracker.propagate().unwrap();
        assert!(
            tracker.is_completed(),
            "once consumed, the tracker completes"
        );
        assert!(tracker.input_frontier(2, 0).is_empty());
    }

    /// Exact zero: a `-1` (consumed with no matching produced) and a `+1`
    /// elsewhere never cancel; neither completes the tracker.
    #[test]
    fn exact_zero_never_sums_counts() {
        let (builder, _) = upstream_downstream(false);
        let mut tracker = builder.build();
        tracker.initialize().unwrap();
        tracker.operator_progress(2).unwrap().consumed[0].update(5u64, 1);
        tracker.operator_progress(1).unwrap().produced[0].update(7u64, 1);
        tracker.propagate().unwrap();
        assert!(!tracker.is_completed());
        assert_eq!(tracker.scope_residual().len(), 2);
    }

    /// A produced batch counts once per edge (fan-out) and `mult` times on
    /// an edge with multiplicity `mult` (broadcast).
    #[test]
    fn produced_counts_once_per_edge_with_its_multiplicity() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        for op in 1..=3 {
            builder
                .add_operator(
                    op,
                    format!("op{op}"),
                    1,
                    1,
                    PortConnectivity::identity(0u64),
                )
                .unwrap();
        }
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(1, 0), Location::target(3, 0));
        let mut tracker = builder.build();
        tracker.routes.insert(
            Location::source(1, 0),
            vec![(Location::target(2, 0), 1), (Location::target(3, 0), 3)],
        );
        tracker.initialize().unwrap();
        tracker.operator_progress(1).unwrap().produced[0].update(4u64, 2);
        tracker.propagate().unwrap();
        let mut residual = tracker.scope_residual();
        residual.sort();
        assert_eq!(
            residual,
            vec![
                format!("{:?}@4: +2", Location::target(2, 0)),
                format!("{:?}@4: +6", Location::target(3, 0)),
            ]
        );
    }

    /// The worker's scope covers every location a hosted location is
    /// reachable from, and nothing else: a count downstream of everything
    /// this worker hosts does not keep it from completing.
    #[test]
    fn scope_excludes_locations_that_reach_nothing_hosted() {
        use crate::dataflow::graph::{DataflowGraph, EdgeInfo, OperatorInfo};
        use crate::dataflow::stage::StageId;
        use crate::dataflow::stream::Slot;
        let s = StageId::new(0);
        let mut graph = DataflowGraph::new();
        for op in 1..=2 {
            graph
                .register_operator(OperatorInfo::new(op, format!("op{op}"), s, 1, 1))
                .unwrap();
        }
        graph.add_edge(EdgeInfo::new(Slot::new(1, 0), Slot::new(2, 0), s, s));
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        for op in 1..=2 {
            builder
                .add_operator(
                    op,
                    format!("op{op}"),
                    1,
                    1,
                    PortConnectivity::identity(0u64),
                )
                .unwrap();
        }
        builder.set_edges_from_graph(&graph).unwrap();
        let mut tracker = builder.build();
        tracker.set_scope(|op| op == 1);
        tracker.initialize().unwrap();
        tracker.operator_progress(2).unwrap().consumed[0].update(1u64, 1);
        tracker.propagate().unwrap();
        assert!(tracker.is_completed(), "op2's input is out of op1's scope");
        tracker.operator_progress(1).unwrap().consumed[0].update(1u64, 1);
        tracker.propagate().unwrap();
        assert!(!tracker.is_completed(), "op1's input is in scope");
    }

    /// Across workers, with no data plane: two trackers joined only by
    /// in-process progress channels. Worker 0 produces a batch toward
    /// `downstream` and both workers drop their capabilities. Until worker 1
    /// consumes the batch, both workers' frontiers at that input must hold at
    /// the batch's time and neither may complete. Then worker 1 consumes it
    /// and both complete.
    #[test]
    fn produced_record_holds_every_workers_frontier_until_consumed() {
        let t = 3u64;
        let n = 7i64;
        let channels = crate::progress::progress_channel::create_progress_channels::<u64>(
            2,
            &[
                crate::dataflow::channels::wake::WakeHandle::new(),
                crate::dataflow::channels::wake::WakeHandle::new(),
            ],
        )
        .unwrap();
        let plan = crate::dataflow::plan::PhysicalPlan::for_hosts(2, &[(1, 2), (2, 2)]);
        type Worker = (ProgressTracker<u64>, Option<ProgressReporter<u64>>);
        let mut workers: Vec<Worker> = Vec::new();
        for channels in channels {
            let (builder, upstream) = upstream_downstream(true);
            let mut tracker = builder.build();
            tracker.set_progress_channels(channels).unwrap();
            workers.push((tracker, upstream));
        }
        for (w, (tracker, _)) in workers.iter_mut().enumerate() {
            tracker.initialize_for_plan(&plan, None, w).unwrap();
        }
        let exchange = |workers: &mut Vec<Worker>| {
            for _ in 0..2 {
                for (tracker, _) in workers.iter_mut() {
                    tracker.propagate().unwrap();
                }
            }
        };
        exchange(&mut workers);

        workers[0].0.operator_progress(1).unwrap().produced[0].update(t, n);
        for (_, capability) in &mut workers {
            capability.take().unwrap().update(0, -1);
        }
        exchange(&mut workers);
        for (w, (tracker, _)) in workers.iter().enumerate() {
            assert!(
                tracker.input_frontier(2, 0).less_equal(&t),
                "worker {w}: frontier passed t={t} while the batch is in flight: {:?}",
                tracker.input_frontier(2, 0)
            );
            assert!(
                !tracker.is_completed(),
                "worker {w} completed while a batch is in flight"
            );
        }

        workers[1].0.operator_progress(2).unwrap().consumed[0].update(t, n);
        exchange(&mut workers);
        for (w, (tracker, _)) in workers.iter().enumerate() {
            assert!(
                tracker.input_frontier(2, 0).is_empty(),
                "worker {w}: frontier {:?} after the batch was consumed",
                tracker.input_frontier(2, 0)
            );
            assert!(tracker.is_completed(), "worker {w} did not complete");
        }
    }

    // --- A4: seeding from the physical plan ---

    /// The plan of [`seeding_scenario`]: `a` (op 1) runs on both workers,
    /// `b` (op 2) and `sink` (op 3) only on worker 0.
    fn seeding_plan() -> crate::dataflow::plan::PhysicalPlan {
        crate::dataflow::plan::PhysicalPlan::for_hosts(2, &[(1, 2), (2, 1), (3, 1)])
    }

    /// Two trackers of a heterogeneous plan ([`seeding_plan`]), joined by
    /// in-process progress channels and no data plane. `a` and `b` each
    /// declare a capability at time 0; `sink` reads `a` on input 0 and `b`
    /// on input 1. Each tracker is seeded from the plan, or, for a mutant,
    /// with `mutant(worker, op)` copies of `op`'s capability.
    ///
    /// - Before any message is exchanged, every view already holds every
    ///   running instance's capability.
    /// - After one instance of `a` releases, both views still hold `a`'s
    ///   time until the other instance releases too.
    /// - Once every instance has released, both views are empty.
    fn seeding_scenario(mutant: Option<&dyn Fn(usize, usize) -> usize>) -> Result<(), String> {
        use crate::dataflow::channels::wake::WakeHandle;
        let plan = seeding_plan();
        let channels = crate::progress::progress_channel::create_progress_channels::<u64>(
            2,
            &[WakeHandle::new(), WakeHandle::new()],
        )
        .unwrap();
        let declared = || {
            let mut batch = ChangeBatch::new();
            batch.update(0u64, 1);
            vec![batch]
        };
        let mut trackers = Vec::new();
        // Per worker: the reporters through which its instances of `a` and
        // `b` release their capabilities.
        let mut releases = Vec::new();
        for (worker, channels) in channels.into_iter().enumerate() {
            let mut builder = SubgraphBuilder::<u64>::new(0, 0);
            let a = builder
                .add_operator_with_capabilities(
                    1,
                    "a",
                    0,
                    1,
                    PortConnectivity::new(0, 1),
                    declared(),
                )
                .unwrap()
                .reporter(0)
                .clone();
            let b = builder
                .add_operator_with_capabilities(
                    2,
                    "b",
                    0,
                    1,
                    PortConnectivity::new(0, 1),
                    declared(),
                )
                .unwrap()
                .reporter(0)
                .clone();
            builder
                .add_operator(3, "sink", 2, 0, PortConnectivity::new(2, 0))
                .unwrap();
            builder.add_edge(Location::source(1, 0), Location::target(3, 0));
            builder.add_edge(Location::source(2, 0), Location::target(3, 1));
            let mut tracker = builder.build();
            tracker.set_progress_channels(channels).unwrap();
            match mutant {
                None => tracker.initialize_for_plan(&plan, None, worker),
                Some(seed) => tracker.seed_and_initialize(2, |op| seed(worker, op)),
            }
            .map_err(|e| e.to_string())?;
            trackers.push(tracker);
            releases.push((a, b));
        }
        let exchange = |trackers: &mut Vec<ProgressTracker<u64>>| {
            for _ in 0..2 {
                for tracker in trackers.iter_mut() {
                    tracker.propagate().unwrap();
                }
            }
        };
        let expect = |trackers: &[ProgressTracker<u64>],
                      when: &str,
                      input: usize,
                      held: bool|
         -> Result<(), String> {
            for (w, tracker) in trackers.iter().enumerate() {
                let frontier = tracker.input_frontier(3, input);
                let ok = if held {
                    frontier.less_equal(&0)
                } else {
                    frontier.is_empty()
                };
                if !ok {
                    return Err(format!(
                        "{when}: worker {w}'s frontier at sink input {input} is {frontier:?}, \
                         expected {}",
                        if held { "to hold 0" } else { "empty" }
                    ));
                }
            }
            Ok(())
        };

        // Nothing exchanged yet: both views hold every instance.
        expect(&trackers, "before any message", 0, true)?;
        expect(&trackers, "before any message", 1, true)?;

        // Worker 0's instance of `a` releases; worker 1's still holds.
        releases[0].0.update(0, -1);
        exchange(&mut trackers);
        expect(
            &trackers,
            "after one of a's two instances released",
            0,
            true,
        )?;

        // Worker 1's instance of `a` releases.
        releases[1].0.update(0, -1);
        exchange(&mut trackers);
        expect(&trackers, "after both of a's instances released", 0, false)?;
        expect(&trackers, "while b's only instance holds", 1, true)?;

        // `b` runs only on worker 0, which releases it.
        releases[0].1.update(0, -1);
        exchange(&mut trackers);
        expect(&trackers, "after every instance released", 1, false)?;
        for (w, tracker) in trackers.iter().enumerate() {
            if !tracker.is_completed() {
                return Err(format!("worker {w} did not complete"));
            }
        }
        Ok(())
    }

    /// Two workers, each hosting an instance of a source `a` (capability at
    /// 0) that feeds `sink`, joined by in-process progress channels. Returns
    /// the trackers and, per worker, the reporter through which `a` releases.
    fn sealing_pair() -> (
        Vec<ProgressTracker<u64>>,
        Vec<crate::progress::operate::ProgressReporter<u64>>,
    ) {
        use crate::dataflow::channels::wake::WakeHandle;
        let channels = crate::progress::progress_channel::create_progress_channels::<u64>(
            2,
            &[WakeHandle::new(), WakeHandle::new()],
        )
        .unwrap();
        let mut trackers = Vec::new();
        let mut releases = Vec::new();
        for channels in channels {
            let mut builder = SubgraphBuilder::<u64>::new(0, 0);
            let mut declared = ChangeBatch::new();
            declared.update(0u64, 1);
            let a = builder
                .add_operator_with_capabilities(
                    1,
                    "a",
                    0,
                    1,
                    PortConnectivity::new(0, 1),
                    vec![declared],
                )
                .unwrap()
                .reporter(0)
                .clone();
            builder
                .add_operator(2, "sink", 1, 0, PortConnectivity::new(1, 0))
                .unwrap();
            builder.add_edge(Location::source(1, 0), Location::target(2, 0));
            let mut tracker = builder.build();
            tracker.set_progress_channels(channels).unwrap();
            tracker
                .seed_and_initialize(2, |op| usize::from(op == 1) * 2)
                .unwrap();
            trackers.push(tracker);
            releases.push(a);
        }
        (trackers, releases)
    }

    /// A worker's seal follows its last commit on every lane, and counts at a
    /// peer only once the batches before it are applied: `all_sealed` means
    /// no further update can arrive and the view is final.
    #[test]
    fn seals_count_after_the_batches_before_them() {
        let (mut trackers, releases) = sealing_pair();
        assert!(!trackers[0].uncollected());
        // A release made after a commit (a `Retained` dropped on another
        // thread) is uncollected until the next one: no seal before it.
        releases[0].update(0, -1);
        assert!(trackers[0].uncollected());
        trackers[0].propagate().unwrap();
        assert!(!trackers[0].uncollected());
        trackers[0].seal();
        assert!(trackers[0].is_sealed());
        assert!(!trackers[0].all_sealed(), "worker 1 has not sealed");
        assert!(!trackers[0].peer_lanes_final());

        // Worker 1 applies worker 0's release with its seal.
        trackers[1].propagate().unwrap();
        assert!(!trackers[1].all_sealed(), "worker 1 itself has not sealed");
        assert!(trackers[1].peer_lanes_final());
        assert!(trackers[1].input_frontier(2, 0).less_equal(&0));
        releases[1].update(0, -1);
        trackers[1].propagate().unwrap();
        trackers[1].seal();
        assert!(trackers[1].all_sealed());
        assert!(trackers[1].is_completed());
        assert!(trackers[1].stopped_peers().is_empty());

        trackers[0].propagate().unwrap();
        assert!(trackers[0].all_sealed());
        assert!(trackers[0].is_completed());
    }

    /// A worker that stops without sealing (R34: a failed worker never seals)
    /// closes its lanes: its peers see it stopped, and no further update can
    /// arrive from it, but nothing is sealed.
    #[test]
    fn a_worker_dropped_unsealed_is_a_stopped_peer() {
        let (mut trackers, _releases) = sealing_pair();
        trackers[0].propagate().unwrap();
        assert!(trackers[0].stopped_peers().is_empty());
        let failed = trackers.pop().unwrap();
        drop(failed);
        trackers[0].propagate().unwrap();
        assert_eq!(trackers[0].stopped_peers(), [1]);
        assert!(trackers[0].peer_lanes_final());
        assert!(!trackers[0].all_sealed());
        assert!(
            !trackers[0].is_completed(),
            "worker 1's capability is never released"
        );
        assert_eq!(trackers[0].peer_name(1), "worker 1");
        trackers[0].set_peer_names(vec!["node-a".into(), "node-b".into()]);
        assert_eq!(trackers[0].peer_name(1), "node-b");

        // R48 waits for the view to stay unchanged for its grace.
        let grace = std::time::Duration::ZERO;
        assert!(
            !trackers[0].stopped_and_quiet(grace),
            "the first sight starts the grace"
        );
        assert!(trackers[0].stopped_and_quiet(grace));
        trackers[0].not_stopped();
        assert!(!trackers[0].stopped_and_quiet(grace));
        // A commit restarts the grace.
        trackers[0].operator_progress(2).unwrap().consumed[0].update(0, 1);
        trackers[0].propagate().unwrap();
        assert!(!trackers[0].stopped_and_quiet(grace));
        assert!(trackers[0].stopped_and_quiet(grace));
    }
    /// A4: seeding from the plan holds every running instance's initial
    /// capability in every view, from before the first message until that
    /// instance releases.
    #[test]
    fn plan_seeding_holds_every_instance_until_it_releases() {
        seeding_scenario(None).unwrap();
    }

    /// The scenario catches both ways of getting the multiplicity wrong
    /// (the TLA+ scenarios `hetero_localseed` and `hetero_workerseed`).
    #[test]
    fn seeding_mutants_fail_the_scenario() {
        let plan = seeding_plan();
        // Each worker seeds only the operators it runs, once: unsafe.
        let local = |worker: usize, op: usize| usize::from(plan.runs(worker, op));
        let failure = seeding_scenario(Some(&local)).expect_err("local seeding passed");
        assert!(failure.contains("before any message"), "{failure}");
        // Every operator seeded once per worker, not per running worker:
        // `b`'s view never empties.
        let per_worker = |_worker: usize, _op: usize| plan.total_workers();
        let failure = seeding_scenario(Some(&per_worker)).expect_err("per-worker seeding passed");
        assert!(
            failure.contains("after every instance released"),
            "{failure}"
        );
    }

    /// A capability an operator creates at materialization instead of
    /// declaring it would miss every peer's seeding (audit H13): `initialize`
    /// rejects it in debug builds.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "instead of declaring it")]
    fn undeclared_capability_at_materialization_trips_initialize() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        let reporter = builder
            .add_operator(1, "source", 0, 1, PortConnectivity::new(0, 1))
            .unwrap()
            .reporter(0)
            .clone();
        let mut tracker = builder.build();
        // The operator's factory creates a capability.
        let _capability = Capability::new(0u64, reporter);
        let _ = tracker.initialize();
    }

    /// Every tracker of a multi-worker dataflow sees every worker's
    /// progress: one without progress channels is rejected in debug builds.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "has no progress channels")]
    fn multi_worker_tracker_without_progress_channels_trips_initialize() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator(1, "op", 0, 1, PortConnectivity::new(0, 1))
            .unwrap();
        let mut tracker = builder.build();
        let plan = crate::dataflow::plan::PhysicalPlan::for_hosts(2, &[(1, 2)]);
        let _ = tracker.initialize_for_plan(&plan, None, 0);
    }

    /// `initialize` seeds each capability once: a tracker with progress
    /// channels belongs to a multi-worker dataflow and would undercount its
    /// peers' instances, so it is rejected (brief trap T1), in release
    /// builds too.
    #[test]
    fn initialize_rejects_a_tracker_with_progress_channels() {
        use crate::dataflow::channels::wake::WakeHandle;
        let mut channels = crate::progress::progress_channel::create_progress_channels::<u64>(
            2,
            &[WakeHandle::new(), WakeHandle::new()],
        )
        .unwrap();
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder
            .add_operator_with_capabilities(
                1,
                "source",
                0,
                1,
                PortConnectivity::new(0, 1),
                vec![ChangeBatch::new_from(0u64, 1)],
            )
            .unwrap();
        let mut tracker = builder.build();
        tracker.set_progress_channels(channels.remove(0)).unwrap();
        let error = tracker.initialize().unwrap_err().to_string();
        assert!(error.contains("initialize_for_plan"), "{error}");
        assert!(!tracker.is_initialized());
    }

    /// A scope graph whose cycle does not advance the timestamp is rejected
    /// when the tracker is built (R38 C5); one that advances it builds.
    #[test]
    fn identity_cycle_in_a_scope_graph_is_rejected() {
        let cycle = |summary: u64| {
            let mut builder = SubgraphBuilder::<u64>::new(0, 0);
            let mut concat = PortConnectivity::new(2, 1);
            concat.path_mut(0, 0).insert(0);
            concat.path_mut(1, 0).insert(0);
            builder.add_operator(1, "concat", 2, 1, concat).unwrap();
            builder
                .add_operator(2, "feedback", 1, 1, PortConnectivity::identity(summary))
                .unwrap();
            builder.add_edge(Location::source(1, 0), Location::target(2, 0));
            builder.add_edge(Location::source(2, 0), Location::target(1, 1));
            builder.try_build()
        };
        let error = cycle(0).unwrap_err().to_string();
        assert!(error.contains("identity cycle"), "{error}");
        assert!(cycle(1).is_ok());
    }
}
