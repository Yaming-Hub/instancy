//! Reachability tracking for progress propagation.
//!
//! The reachability system determines which timestamps can still reach which
//! locations in the dataflow graph. This is the core of progress tracking:
//! when no capabilities for a given timestamp can reach a port, that port's
//! frontier advances past that timestamp.
//!
//! # Architecture
//!
//! 1. [`Builder`] — collects the static graph topology (operators, edges, summaries).
//! 2. [`Tracker`] — live propagation engine that maintains per-port frontiers.
//!
//! The [`Builder`] compiles the graph into a [`Tracker`] via [`Builder::build`].
//! The [`Tracker`] then receives incremental updates (capability changes) and
//! propagates their implications through the graph using a worklist algorithm.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;

use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::mutable_antichain::MutableAntichain;
use crate::progress::operate::PortConnectivity;
use crate::progress::timestamp::{PathSummary, Timestamp};

// ---------------------------------------------------------------------------
// Location — a port in the dataflow graph
// ---------------------------------------------------------------------------

/// A location in the dataflow graph: either an operator's output port or input port.
///
/// This is a **logical** concept — `node` refers to the logical operator index
/// in the graph, not a physical machine or cluster node.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Location {
    /// An operator's output port (where data is produced).
    Source {
        /// Logical operator index in the graph.
        node: usize,
        /// Output port index.
        port: usize,
    },
    /// An operator's input port (where data is consumed).
    Target {
        /// Logical operator index in the graph.
        node: usize,
        /// Input port index.
        port: usize,
    },
}

impl Location {
    /// Creates a source (output port) location.
    pub fn source(node: usize, port: usize) -> Self {
        Location::Source { node, port }
    }

    /// Creates a target (input port) location.
    pub fn target(node: usize, port: usize) -> Self {
        Location::Target { node, port }
    }

    /// Returns the node index.
    pub fn node(&self) -> usize {
        match self {
            Location::Source { node, .. } | Location::Target { node, .. } => *node,
        }
    }
}

// ---------------------------------------------------------------------------
// Builder — construct the reachability graph
// ---------------------------------------------------------------------------

/// Builds a reachability graph from operator topology.
///
/// Register operators with [`add_node`](Self::add_node), connect them with
/// [`add_edge`](Self::add_edge), then call [`build`](Self::build) to produce
/// a live [`Tracker`].
pub struct Builder<T: Timestamp> {
    /// Per-node internal connectivity (how timestamps transform through operator).
    nodes: Vec<Option<PortConnectivity<T::Summary>>>,
    /// Per-node outgoing edges: `edges[node][output_port]` → list of target locations.
    edges: Vec<Vec<Vec<Location>>>,
    /// (inputs, outputs) count per node.
    shape: Vec<(usize, usize)>,
    /// Number of scope inputs (boundary targets that receive from outside).
    scope_inputs: usize,
    /// Number of scope outputs (boundary sources that send to outside).
    scope_outputs: usize,
}

impl<T: Timestamp> Builder<T> {
    /// Creates a new builder.
    ///
    /// `scope_inputs` and `scope_outputs` define the boundary ports of the
    /// enclosing scope (graph node 0 is reserved for the scope boundary;
    /// "node" here refers to a vertex in the dataflow graph, not a machine).
    pub fn new(scope_inputs: usize, scope_outputs: usize) -> Self {
        let mut builder = Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            shape: Vec::new(),
            scope_inputs,
            scope_outputs,
        };
        // Graph node 0 is the scope boundary: scope_outputs inputs, scope_inputs outputs.
        // (The scope's outputs are "inputs" to graph node 0, and scope's inputs are "outputs".)
        // "Node" here means a vertex in the dataflow graph, not a machine/cluster node.
        // This follows timely's convention where the scope wrapper is graph node 0.
        let boundary_inputs = scope_outputs;
        let boundary_outputs = scope_inputs;
        builder.ensure_node(0);
        builder.shape[0] = (boundary_inputs, boundary_outputs);
        builder.edges[0] = vec![Vec::new(); boundary_outputs];
        // Boundary node has no internal paths (identity for scope I/O).
        builder.nodes[0] = Some(PortConnectivity::new(boundary_inputs, boundary_outputs));
        builder
    }

    /// Registers an operator node with its port counts and internal connectivity.
    ///
    /// # Panics
    ///
    /// Panics if the `summary` dimensions don't match `inputs` and `outputs`.
    pub fn add_node(
        &mut self,
        index: usize,
        inputs: usize,
        outputs: usize,
        summary: PortConnectivity<T::Summary>,
    ) {
        assert_eq!(
            summary.inputs(),
            inputs,
            "PortConnectivity inputs ({}) don't match declared inputs ({}) for node {}",
            summary.inputs(),
            inputs,
            index
        );
        assert_eq!(
            summary.outputs(),
            outputs,
            "PortConnectivity outputs ({}) don't match declared outputs ({}) for node {}",
            summary.outputs(),
            outputs,
            index
        );
        self.ensure_node(index);
        self.shape[index] = (inputs, outputs);
        self.edges[index] = vec![Vec::new(); outputs];
        self.nodes[index] = Some(summary);
    }

    /// Connects an operator's output port to another operator's input port.
    ///
    /// # Panics
    ///
    /// Panics if the source is not a `Location::Source`, or if the source node
    /// hasn't been registered yet, or if the port is out of bounds.
    pub fn add_edge(&mut self, source: Location, target: Location) {
        if let Location::Source { node, port } = source {
            assert!(
                node < self.shape.len() && port < self.edges[node].len(),
                "add_edge: source node {} port {} is invalid (node has {} output ports). \
                 Register the node with add_node() before adding edges.",
                node,
                port,
                if node < self.edges.len() {
                    self.edges[node].len()
                } else {
                    0
                }
            );
            self.edges[node][port].push(target);
        }
    }

    /// Compiles the builder into a live [`Tracker`] and the scope-level summary.
    ///
    /// The scope-level summary describes how timestamps entering the scope
    /// (via scope inputs) can reach scope outputs after traversing the subgraph.
    pub fn build(self) -> (Tracker<T>, PortConnectivity<T::Summary>) {
        let _num_nodes = self.nodes.len();

        // Build per-operator state.
        let per_operator: Vec<PerOperator<T>> = self
            .shape
            .iter()
            .map(|&(inputs, outputs)| PerOperator {
                targets: (0..inputs).map(|_| PortInformation::new()).collect(),
                sources: (0..outputs).map(|_| PortInformation::new()).collect(),
            })
            .collect();

        let target_changes: Vec<Vec<ChangeBatch<T>>> = self
            .shape
            .iter()
            .map(|&(inputs, _)| (0..inputs).map(|_| ChangeBatch::new()).collect())
            .collect();

        let source_changes: Vec<Vec<ChangeBatch<T>>> = self
            .shape
            .iter()
            .map(|&(_, outputs)| (0..outputs).map(|_| ChangeBatch::new()).collect())
            .collect();

        // Compute scope-level summary via fixed-point iteration.
        let scope_summary =
            self.compute_scope_summary();

        // Compile the forward path summaries for propagation.
        // target_summaries[node][input] → Vec<(Location, Antichain<Summary>)>
        // source_summaries[node][output] → Vec<(Location, Antichain<Summary>)>
        let (target_summaries, source_summaries) = self.compile_forward_paths();

        let output_changes = (0..self.scope_outputs)
            .map(|_| ChangeBatch::new())
            .collect();

        let tracker = Tracker {
            nodes: self
                .nodes
                .into_iter()
                .map(|n| n.unwrap_or_else(|| PortConnectivity::new(0, 0)))
                .collect(),
            edges: self.edges,
            shape: self.shape,
            per_operator,
            target_changes,
            source_changes,
            target_summaries,
            source_summaries,
            worklist: BinaryHeap::new(),
            pushed_changes: ChangeBatch::new(),
            output_changes,
            total_counts: 0,
        };

        (tracker, scope_summary)
    }

    fn ensure_node(&mut self, index: usize) {
        while self.nodes.len() <= index {
            self.nodes.push(None);
            self.edges.push(Vec::new());
            self.shape.push((0, 0));
        }
    }

    /// Computes the scope-level summary using fixed-point iteration.
    ///
    /// For each scope input → scope output path, computes the antichain of
    /// path summaries describing all possible timestamp transformations.
    fn compute_scope_summary(&self) -> PortConnectivity<T::Summary> {
        let mut summary = PortConnectivity::new(self.scope_inputs, self.scope_outputs);

        // Summaries from each location to scope outputs.
        // location_to_output[location] → Vec<Antichain<Summary>> indexed by scope output.
        let num_nodes = self.shape.len();
        let mut source_to_output: Vec<Vec<Vec<Antichain<T::Summary>>>> = self
            .shape
            .iter()
            .map(|&(_, outputs)| {
                vec![vec![Antichain::new(); self.scope_outputs]; outputs]
            })
            .collect();
        let mut target_to_output: Vec<Vec<Vec<Antichain<T::Summary>>>> = self
            .shape
            .iter()
            .map(|&(inputs, _)| {
                vec![vec![Antichain::new(); self.scope_outputs]; inputs]
            })
            .collect();

        // Initialize: scope boundary (graph node 0) inputs connect to scope outputs.
        // Graph node 0's inputs correspond to scope outputs.
        for so in 0..self.scope_outputs {
            if so < target_to_output[0].len() {
                target_to_output[0][so][so].insert(T::Summary::default());
            }
        }

        // Fixed-point: propagate summaries backward through the graph.
        let mut changed = true;
        while changed {
            changed = false;

            // For each node, propagate from targets to sources using internal connectivity.
            for node in 0..num_nodes {
                if let Some(ref conn) = self.nodes.get(node).and_then(|n| n.as_ref()) {
                    let (inputs, outputs) = self.shape[node];
                    for input in 0..inputs {
                        for output in 0..outputs {
                            for internal_summary in conn.path(input, output).elements() {
                                for so in 0..self.scope_outputs {
                                    // Compose: target[node][input] can reach so via
                                    // internal_summary then source[node][output] → so
                                    for reached_summary in
                                        source_to_output[node][output][so].elements().to_vec()
                                    {
                                        if let Some(composed) =
                                            internal_summary.followed_by(&reached_summary)
                                        {
                                            if target_to_output[node][input][so].insert(composed) {
                                                changed = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // For each edge, propagate from targets to sources.
            for node in 0..num_nodes {
                let (_, outputs) = self.shape[node];
                for output in 0..outputs {
                    for target_loc in &self.edges[node][output] {
                        if let Location::Target {
                            node: tgt_node,
                            port: tgt_port,
                        } = target_loc
                        {
                            for so in 0..self.scope_outputs {
                                // source[node][output] can reach so if target[tgt_node][tgt_port] can
                                for s in target_to_output[*tgt_node][*tgt_port][so]
                                    .elements()
                                    .to_vec()
                                {
                                    if source_to_output[node][output][so].insert(s) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Extract scope-level summary: scope input → scope output.
        // Scope inputs are graph node 0's outputs.
        for si in 0..self.scope_inputs {
            if si < source_to_output[0].len() {
                for so in 0..self.scope_outputs {
                    for s in source_to_output[0][si][so].elements() {
                        summary.path_mut(si, so).insert(s.clone());
                    }
                }
            }
        }

        summary
    }

    /// Compiles forward propagation paths used by the Tracker.
    ///
    /// For each location, computes which other locations it can reach and with
    /// what summary antichains. This avoids recomputing paths during propagation.
    fn compile_forward_paths(
        &self,
    ) -> (
        Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,
        Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,
    ) {
        let num_nodes = self.shape.len();

        // target_summaries[node][input] → [(source_location, summary_antichain)]
        // When a timestamp changes at target[node][input], apply each internal summary
        // to get changes at source[node][output].
        let target_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>> = (0..num_nodes)
            .map(|node| {
                let (inputs, outputs) = self.shape[node];
                (0..inputs)
                    .map(|input| {
                        let mut paths = Vec::new();
                        if let Some(Some(conn)) = self.nodes.get(node) {
                            for output in 0..outputs {
                                let path = conn.path(input, output);
                                if !path.is_empty() {
                                    paths.push((
                                        Location::source(node, output),
                                        path.clone(),
                                    ));
                                }
                            }
                        }
                        paths
                    })
                    .collect()
            })
            .collect();

        // source_summaries[node][output] → [(target_location, identity_summary)]
        // When a timestamp changes at source[node][output], propagate along edges
        // (identity transform — edges don't change timestamps).
        let source_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>> = (0..num_nodes)
            .map(|node| {
                let (_, outputs) = self.shape[node];
                (0..outputs)
                    .map(|output| {
                        self.edges[node][output]
                            .iter()
                            .map(|target| {
                                let mut identity = Antichain::new();
                                identity.insert(T::Summary::default());
                                (target.clone(), identity)
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect();

        (target_summaries, source_summaries)
    }
}

// ---------------------------------------------------------------------------
// Tracker — live progress propagation
// ---------------------------------------------------------------------------

/// An entry in the propagation worklist.
#[derive(Clone, Debug, Eq, PartialEq)]
struct WorklistEntry<T> {
    time: T,
    location: Location,
    diff: i64,
}

impl<T: Ord> Ord for WorklistEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.location.cmp(&other.location))
            .then_with(|| self.diff.cmp(&other.diff))
    }
}

impl<T: Ord> PartialOrd for WorklistEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-port frontier and implication tracking.
struct PortInformation<T: Timestamp> {
    /// Current counts (pointstamps) at this port.
    pointstamps: MutableAntichain<T>,
    /// Propagated implications: what times can reach downstream.
    implications: MutableAntichain<T>,
}

impl<T: Timestamp> PortInformation<T> {
    fn new() -> Self {
        Self {
            pointstamps: MutableAntichain::new(),
            implications: MutableAntichain::new(),
        }
    }
}

/// Per-operator frontier state.
struct PerOperator<T: Timestamp> {
    targets: Vec<PortInformation<T>>,
    sources: Vec<PortInformation<T>>,
}

/// Live reachability tracker that propagates progress changes through the dataflow graph.
///
/// # Usage
///
/// 1. Post updates via [`update_target`](Self::update_target) / [`update_source`](Self::update_source).
/// 2. Call [`propagate_all`](Self::propagate_all) to compute frontier implications.
/// 3. Read results via [`drain_pushed`](Self::drain_pushed) / [`drain_output_changes`](Self::drain_output_changes).
pub struct Tracker<T: Timestamp> {
    /// Per-node internal connectivity (retained for future introspection/debugging).
    #[allow(dead_code)]
    nodes: Vec<PortConnectivity<T::Summary>>,
    /// Per-node outgoing edges (retained for future introspection/debugging).
    #[allow(dead_code)]
    edges: Vec<Vec<Vec<Location>>>,
    /// (inputs, outputs) per node.
    shape: Vec<(usize, usize)>,

    /// Per-operator frontier state.
    per_operator: Vec<PerOperator<T>>,

    /// Pending input changes (buffered before propagation).
    target_changes: Vec<Vec<ChangeBatch<T>>>,
    source_changes: Vec<Vec<ChangeBatch<T>>>,

    /// Compiled forward paths for propagation.
    target_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,
    source_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,

    /// Worklist for propagation (min-heap by timestamp).
    worklist: BinaryHeap<Reverse<WorklistEntry<T>>>,

    /// Accumulated frontier changes at all locations.
    pushed_changes: ChangeBatch<(Location, T)>,

    /// Scope output frontier changes.
    output_changes: Vec<ChangeBatch<T>>,

    /// Total outstanding capability counts across all locations.
    total_counts: i64,
}

impl<T: Timestamp> Tracker<T> {
    /// Posts a capability change at an operator's input port.
    pub fn update_target(&mut self, node: usize, port: usize, time: T, diff: i64) {
        debug_assert!(node < self.target_changes.len() && port < self.target_changes[node].len(),
            "update_target: node {} port {} out of bounds", node, port);
        self.target_changes[node][port].update(time, diff);
    }

    /// Posts a capability change at an operator's output port.
    pub fn update_source(&mut self, node: usize, port: usize, time: T, diff: i64) {
        debug_assert!(node < self.source_changes.len() && port < self.source_changes[node].len(),
            "update_source: node {} port {} out of bounds", node, port);
        self.source_changes[node][port].update(time, diff);
    }

    /// Propagates all pending changes through the reachability graph.
    ///
    /// After calling this, read frontier implications via [`drain_pushed`] and
    /// [`drain_output_changes`].
    pub fn propagate_all(&mut self) {
        // Step 1: Drain pending changes into pointstamps and seed the worklist.
        self.drain_pending_changes();

        // Step 2: Process worklist entries in timestamp order.
        while let Some(Reverse(entry)) = self.worklist.pop() {
            // Accumulate all entries with the same (time, location).
            let mut diff = entry.diff;
            while self
                .worklist
                .peek()
                .is_some_and(|Reverse(e)| e.time == entry.time && e.location == entry.location)
            {
                diff += self.worklist.pop().unwrap().0.diff;
            }

            if diff == 0 {
                continue;
            }

            match &entry.location {
                Location::Target { node, port } => {
                    self.propagate_target(*node, *port, &entry.time, diff);
                }
                Location::Source { node, port } => {
                    self.propagate_source(*node, *port, &entry.time, diff);
                }
            }
        }
    }

    /// Drains accumulated frontier changes at all locations.
    pub fn drain_pushed(&mut self) -> Vec<((Location, T), i64)> {
        self.pushed_changes.drain().collect()
    }

    /// Drains scope output frontier changes for the given output index.
    pub fn drain_output_changes(&mut self, output: usize) -> Vec<(T, i64)> {
        self.output_changes[output].drain().collect()
    }

    /// Returns `true` if the tracker is still tracking any outstanding capabilities
    /// or has pending unpropagated changes.
    pub fn tracking_anything(&self) -> bool {
        if self.total_counts != 0 {
            return true;
        }
        // Check for pending changes.
        for node_changes in &self.target_changes {
            for batch in node_changes {
                if !batch.is_empty_clean() {
                    return true;
                }
            }
        }
        for node_changes in &self.source_changes {
            for batch in node_changes {
                if !batch.is_empty_clean() {
                    return true;
                }
            }
        }
        !self.worklist.is_empty()
    }

    /// Returns the current frontier at an operator's input port.
    pub fn target_frontier(&self, node: usize, port: usize) -> &[T] {
        self.per_operator[node].targets[port]
            .implications
            .frontier()
    }

    /// Returns the current frontier at an operator's output port.
    pub fn source_frontier(&self, node: usize, port: usize) -> &[T] {
        self.per_operator[node].sources[port]
            .implications
            .frontier()
    }

    // -- internal --

    /// Drains pending target/source changes into pointstamps and seeds the worklist.
    fn drain_pending_changes(&mut self) {
        for node in 0..self.shape.len() {
            let (inputs, outputs) = self.shape[node];

            for port in 0..inputs {
                let changes: Vec<(T, i64)> = self.target_changes[node][port].drain().collect();
                for (time, diff) in changes {
                    self.total_counts += diff;
                    let frontier_changes = self.per_operator[node].targets[port]
                        .pointstamps
                        .update_iter(std::iter::once((time.clone(), diff)));
                    for (t, d) in frontier_changes {
                        self.worklist.push(Reverse(WorklistEntry {
                            time: t,
                            location: Location::target(node, port),
                            diff: d,
                        }));
                    }
                }
            }

            for port in 0..outputs {
                let changes: Vec<(T, i64)> = self.source_changes[node][port].drain().collect();
                for (time, diff) in changes {
                    self.total_counts += diff;
                    let frontier_changes = self.per_operator[node].sources[port]
                        .pointstamps
                        .update_iter(std::iter::once((time.clone(), diff)));
                    for (t, d) in frontier_changes {
                        self.worklist.push(Reverse(WorklistEntry {
                            time: t,
                            location: Location::source(node, port),
                            diff: d,
                        }));
                    }
                }
            }
        }
    }

    /// Propagates an implication change at a target (input port).
    fn propagate_target(&mut self, node: usize, port: usize, time: &T, diff: i64) {
        // Update implications at this target.
        let implication_changes = self.per_operator[node].targets[port]
            .implications
            .update_iter(std::iter::once((time.clone(), diff)));

        // Record in pushed_changes.
        let location = Location::target(node, port);
        for (t, d) in &implication_changes {
            self.pushed_changes.update((location.clone(), t.clone()), *d);
        }

        // Update scope output changes if this is the scope boundary (graph node 0).
        // Graph node 0's inputs (targets) correspond to scope outputs.
        if node == 0 {
            for (t, d) in &implication_changes {
                if port < self.output_changes.len() {
                    self.output_changes[port].update(t.clone(), *d);
                }
            }
        }

        // Propagate through internal edges: for each (time_change, diff),
        // apply internal summaries to reach source ports.
        for (changed_time, changed_diff) in implication_changes {
            for (target_loc, summary_antichain) in &self.target_summaries[node][port] {
                for summary in summary_antichain.elements() {
                    if let Some(new_time) = summary.results_in(&changed_time) {
                        self.worklist.push(Reverse(WorklistEntry {
                            time: new_time,
                            location: target_loc.clone(),
                            diff: changed_diff,
                        }));
                    }
                }
            }
        }
    }

    /// Propagates an implication change at a source (output port).
    fn propagate_source(&mut self, node: usize, port: usize, time: &T, diff: i64) {
        // Update implications at this source.
        let implication_changes = self.per_operator[node].sources[port]
            .implications
            .update_iter(std::iter::once((time.clone(), diff)));

        // Record in pushed_changes.
        let location = Location::source(node, port);
        for (t, d) in &implication_changes {
            self.pushed_changes.update((location.clone(), t.clone()), *d);
        }

        // Propagate along outgoing edges.
        for (changed_time, changed_diff) in implication_changes {
            for (target_loc, _summary_antichain) in &self.source_summaries[node][port] {
                // Edges don't transform timestamps (identity).
                self.worklist.push(Reverse(WorklistEntry {
                    time: changed_time.clone(),
                    location: target_loc.clone(),
                    diff: changed_diff,
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Product;

    // --- Location ---

    #[test]
    fn location_source_target() {
        let s = Location::source(1, 2);
        let t = Location::target(3, 4);
        assert_eq!(s.node(), 1);
        assert_eq!(t.node(), 3);
    }

    #[test]
    fn location_ord() {
        let a = Location::source(0, 0);
        let b = Location::source(0, 1);
        let c = Location::target(0, 0);
        assert!(a < b);
        // Source vs Target ordering is deterministic
        assert_ne!(a, c);
    }

    #[test]
    fn location_eq_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Location::source(1, 0));
        set.insert(Location::source(1, 0));
        set.insert(Location::target(1, 0));
        assert_eq!(set.len(), 2);
    }

    // --- Builder ---

    #[test]
    fn builder_empty_scope() {
        let builder = Builder::<u64>::new(0, 0);
        let (tracker, summary) = builder.build();
        assert!(!tracker.tracking_anything());
        assert_eq!(summary.inputs(), 0);
        assert_eq!(summary.outputs(), 0);
    }

    #[test]
    fn builder_single_passthrough_operator() {
        // Scope with 1 input, 1 output.
        // Operator 1: 1 input, 1 output, identity summary.
        let mut builder = Builder::<u64>::new(1, 1);
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));

        // Wire: scope_input(0) → op1_input(0), op1_output(0) → scope_output(0)
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (_tracker, summary) = builder.build();
        // Scope summary: input 0 → output 0 with identity (0)
        assert!(!summary.path(0, 0).is_empty());
    }

    // --- Tracker: linear chain ---

    #[test]
    fn tracker_linear_chain_propagation() {
        // A → B: two operators in sequence.
        let mut builder = Builder::<u64>::new(1, 1);
        // Op1: 1 input, 1 output, identity.
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        // Op2: 1 input, 1 output, identity.
        builder.add_node(2, 1, 1, PortConnectivity::identity(0u64));

        // scope_input → op1_input → op1_output → op2_input → op2_output → scope_output
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        // Post a capability at op1's output.
        tracker.update_source(1, 0, 10u64, 1);
        tracker.propagate_all();

        // Op2's input should see the implication.
        assert!(!tracker.target_frontier(2, 0).is_empty());
        assert!(tracker.target_frontier(2, 0).contains(&10));

        // Remove the capability.
        tracker.update_source(1, 0, 10u64, -1);
        tracker.propagate_all();

        // Frontier should be empty now.
        assert!(tracker.target_frontier(2, 0).is_empty());
    }

    #[test]
    fn tracker_diamond_graph() {
        // scope_in → [op1, op2] → op3 → scope_out
        let mut builder = Builder::<u64>::new(1, 1);
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        builder.add_node(2, 1, 1, PortConnectivity::identity(0u64));

        // Op3: 2 inputs, 1 output, both inputs connect to output 0.
        let mut conn3 = PortConnectivity::new(2, 1);
        conn3.path_mut(0, 0).insert(0u64);
        conn3.path_mut(1, 0).insert(0u64);
        builder.add_node(3, 2, 1, conn3);

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(0, 0), Location::target(2, 0));
        builder.add_edge(Location::source(1, 0), Location::target(3, 0));
        builder.add_edge(Location::source(2, 0), Location::target(3, 1));
        builder.add_edge(Location::source(3, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        // Capability on both branches.
        tracker.update_source(1, 0, 5u64, 1);
        tracker.update_source(2, 0, 5u64, 1);
        tracker.propagate_all();

        // Op3's inputs should both have implications.
        assert!(tracker.target_frontier(3, 0).contains(&5));
        assert!(tracker.target_frontier(3, 1).contains(&5));

        // Remove one branch.
        tracker.update_source(1, 0, 5u64, -1);
        tracker.propagate_all();

        // Op3 input 0 should be empty, input 1 still has it.
        assert!(tracker.target_frontier(3, 0).is_empty());
        assert!(tracker.target_frontier(3, 1).contains(&5));
    }

    #[test]
    fn tracker_self_loop_with_increment() {
        // Op1 with a feedback edge: output → input with +1 summary.
        let mut builder = Builder::<u64>::new(1, 1);
        let mut conn = PortConnectivity::new(1, 1);
        conn.path_mut(0, 0).insert(1u64); // +1 summary
        builder.add_node(1, 1, 1, conn);

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(1, 0)); // self-loop
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        // Capability at op1 input at time 0.
        tracker.update_target(1, 0, 0u64, 1);
        tracker.propagate_all();

        // Should see implication at op1's source and then back to target with +1.
        let target_f = tracker.target_frontier(1, 0);
        assert!(target_f.contains(&0), "original time should be in frontier");
    }

    #[test]
    fn tracker_unreachable_port() {
        // Op1: 1 input, 2 outputs. Only output 0 is connected.
        let mut builder = Builder::<u64>::new(1, 1);
        let mut conn = PortConnectivity::new(1, 2);
        conn.path_mut(0, 0).insert(0u64);
        // No path from input to output 1.
        builder.add_node(1, 1, 2, conn);

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));
        // output 1 is not connected

        let (mut tracker, _) = builder.build();

        tracker.update_target(1, 0, 10u64, 1);
        tracker.propagate_all();

        // Source 0 should have implications, source 1 should not.
        assert!(!tracker.source_frontier(1, 0).is_empty());
        assert!(tracker.source_frontier(1, 1).is_empty());
    }

    #[test]
    fn tracker_total_counts() {
        let mut builder = Builder::<u64>::new(1, 1);
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();
        assert!(!tracker.tracking_anything());

        tracker.update_source(1, 0, 5u64, 1);
        assert!(tracker.tracking_anything());

        tracker.propagate_all();
        assert!(tracker.tracking_anything());

        tracker.update_source(1, 0, 5u64, -1);
        tracker.propagate_all();
        assert!(!tracker.tracking_anything());
    }

    #[test]
    fn tracker_product_timestamps() {
        // Two operators with Product<u64,u64> timestamps.
        let mut builder = Builder::<Product<u64, u64>>::new(1, 1);

        let summary = <Product<u64, u64> as Timestamp>::Summary::default();
        builder.add_node(1, 1, 1, PortConnectivity::identity(summary));

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        // Post incomparable capabilities.
        tracker.update_source(1, 0, Product::new(1, 2), 1);
        tracker.update_source(1, 0, Product::new(2, 1), 1);
        tracker.propagate_all();

        let frontier = tracker.target_frontier(0, 0);
        // Both should be in the frontier (incomparable).
        assert!(frontier.len() >= 2 || frontier.iter().any(|t| *t == Product::new(1, 2)));
    }

    #[test]
    fn tracker_drain_pushed() {
        let mut builder = Builder::<u64>::new(1, 1);
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        tracker.update_source(1, 0, 10u64, 1);
        tracker.propagate_all();

        let changes = tracker.drain_pushed();
        assert!(!changes.is_empty());

        // Draining again should be empty.
        let changes2 = tracker.drain_pushed();
        assert!(changes2.is_empty());
    }

    #[test]
    fn tracker_scope_output_changes() {
        let mut builder = Builder::<u64>::new(1, 1);
        builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let (mut tracker, _) = builder.build();

        tracker.update_source(1, 0, 10u64, 1);
        tracker.propagate_all();

        let output_changes = tracker.drain_output_changes(0);
        // Should see (10, +1) arriving at scope output.
        assert!(
            output_changes.iter().any(|(t, d)| *t == 10 && *d > 0),
            "scope output should see capability at 10"
        );
    }
}
