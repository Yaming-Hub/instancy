//! Reachability tracking for progress propagation.
//!
//! The reachability system determines which timestamps can still reach which
//! locations in the dataflow graph. This is the core of progress tracking:
//! when no capabilities for a given timestamp can reach a port, that port's
//! frontier advances past that timestamp.
//!
//! # Why reachability tracking exists
//!
//! In a streaming dataflow, operators need to know when a timestamp `t` is
//! "complete" — meaning no more data at `t` can ever arrive. This is what
//! enables aggregation operators to emit final results, and it's what allows
//! the system to free resources for processed timestamps.
//!
//! The key insight is that data at time `t` can only arrive at an operator's
//! input port if there exists some upstream capability at a time `s` that can
//! *reach* that port with a timestamp `<= t`. "Reach" means there is a path
//! through the dataflow graph, and the timestamps may be *transformed* along
//! the way by operator-internal path summaries (e.g., a loop feedback operator
//! might add +1 to the timestamp).
//!
//! When all such reachable capabilities disappear (are dropped or downgraded),
//! the port's frontier advances past `t`, signaling that `t` is complete.
//!
//! # Architecture
//!
//! 1. [`Builder`] — collects the static graph topology (operators, edges, path
//!    summaries) during dataflow construction.
//! 2. [`Tracker`] — live propagation engine that maintains per-port frontiers
//!    at runtime.
//!
//! The [`Builder`] compiles the graph into a [`Tracker`] via [`Builder::build`].
//! The [`Tracker`] then receives incremental capability changes (pointstamp
//! updates) and propagates their implications through the graph using a
//! min-heap worklist algorithm, computing new frontiers at every port.
//!
//! # Key concepts
//!
//! - **Pointstamp**: A count of outstanding capabilities at a specific
//!   `(location, timestamp)`. When an operator creates a `Capability<T>`,
//!   it increments the pointstamp at `(operator_output_port, T)` by +1.
//!   When the capability is dropped, it decrements by -1.
//!
//! - **Implication**: The set of timestamps that *could* still arrive at a
//!   location, computed by propagating pointstamps through path summaries.
//!   The frontier of implications at a port is what operators observe as
//!   their "input frontier."
//!
//! - **Path summary**: Describes how a timestamp transforms along a path.
//!   For example, an identity summary means the timestamp passes through
//!   unchanged. A loop feedback summary might map `t → t+1`. Summaries
//!   are composed via `followed_by()` and stored as antichains (because
//!   multiple paths with different summaries may exist between two ports).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::Debug;

use crate::error::DataflowError;
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
/// Locations identify where capabilities (pointstamps) exist and where frontiers
/// are tracked. The progress system models the dataflow as a bipartite graph of
/// locations: data flows from Source (output port) locations along edges to
/// Target (input port) locations, and from Target locations through operator
/// internals to Source locations.
///
/// This is a **logical** concept — `node` refers to the logical operator index
/// in the dataflow graph (assigned during construction), not a physical machine
/// or cluster node.
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
/// During dataflow construction, the builder collects three things per operator:
/// - **Shape**: how many input/output ports (stored in `shape`).
/// - **Internal connectivity**: how timestamps transform through the operator,
///   as a matrix of path summary antichains (stored in `nodes`).
/// - **Edges**: which output ports connect to which input ports (stored in `edges`).
///
/// Register operators with [`add_node`](Self::add_node), connect them with
/// [`add_edge`](Self::add_edge), then call [`build`](Self::build) to produce
/// a live [`Tracker`] and the scope-level summary.
pub struct Builder<T: Timestamp> {
    /// Per-node internal connectivity: how timestamps transform from input ports
    /// to output ports within each operator. `nodes[i]` is `Some(connectivity)`
    /// for registered operators and `None` for unregistered indices.
    nodes: Vec<Option<PortConnectivity<T::Summary>>>,
    /// Per-node outgoing edges: `edges[node][output_port]` → list of target
    /// locations that this output port feeds into. Edges represent inter-operator
    /// connections and use identity timestamp transformation (timestamps pass
    /// through unchanged along edges — only operator internals transform them).
    edges: Vec<Vec<Vec<Location>>>,
    /// Per-node port shape: `shape[node] = (num_inputs, num_outputs)`.
    /// Stored separately from `nodes` because we need shapes before connectivity
    /// is registered (edges reference port counts).
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
        // Graph node 0 is the scope boundary — a virtual operator that represents
        // the interface between this subgraph and its parent scope.
        //
        // The direction inversion is key: the scope's OUTPUT ports are node 0's
        // INPUTS (because scope outputs receive data from inside the subgraph),
        // and the scope's INPUT ports are node 0's OUTPUTS (because scope inputs
        // send data into the subgraph). This follows timely's convention.
        //
        // "Node" here means a vertex in the dataflow graph, not a machine/cluster node.
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
    ) -> crate::Result<()> {
        if summary.inputs() != inputs {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                format!(
                    "PortConnectivity inputs ({}) don't match declared inputs ({}) for node {}",
                    summary.inputs(),
                    inputs,
                    index
                ),
            )));
        }
        if summary.outputs() != outputs {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                format!(
                    "PortConnectivity outputs ({}) don't match declared outputs ({}) for node {}",
                    summary.outputs(),
                    outputs,
                    index
                ),
            )));
        }
        self.ensure_node(index);
        self.shape[index] = (inputs, outputs);
        self.edges[index] = vec![Vec::new(); outputs];
        self.nodes[index] = Some(summary);
        Ok(())
    }

    /// Connects an operator's output port to another operator's input port.
    ///
    /// # Panics
    ///
    /// Panics if the source is not a `Location::Source`, or if the source node
    /// hasn't been registered yet, or if the port is out of bounds.
    pub fn add_edge(&mut self, source: Location, target: Location) -> crate::Result<()> {
        if let Location::Source { node, port } = source {
            if !(node < self.shape.len() && port < self.edges[node].len()) {
                return Err(crate::Error::Dataflow(DataflowError::InvalidGraph(
                    format!(
                        "add_edge: source node {} port {} is invalid (node has {} output ports). Register the node with add_node() before adding edges.",
                        node,
                        port,
                        if node < self.edges.len() {
                            self.edges[node].len()
                        } else {
                            0
                        }
                    ),
                )));
            }
            self.edges[node][port].push(target);
        }
        Ok(())
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
        let scope_summary = self.compute_scope_summary();

        // Every location's paths to the scope outputs, for the export of
        // internal work to the parent scope. Boundary node 0 is excluded: the
        // parent's own input frontier, imported there, is not internal work.
        let (source_tables, target_tables) = self.backward_tables();
        let source_exports = export_table::<T>(source_tables);
        let target_exports = export_table::<T>(target_tables);

        // Compile the forward path summaries for propagation.
        // target_summaries[node][input] → Vec<(Location, Antichain<Summary>)>
        // source_summaries[node][output] → Vec<(Location, Antichain<Summary>)>
        let (target_summaries, source_summaries) = self.compile_forward_paths();

        let output_changes = (0..self.scope_outputs)
            .map(|_| ChangeBatch::new())
            .collect();

        let tracker = Tracker {
            _nodes: self
                .nodes
                .into_iter()
                .map(|n| n.unwrap_or_else(|| PortConnectivity::new(0, 0)))
                .collect(),
            _edges: self.edges,
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
            nonzero_counted: 0,
            source_exports,
            target_exports,
            export_changes: (0..self.scope_outputs)
                .map(|_| ChangeBatch::new())
                .collect(),
        };

        (tracker, scope_summary)
    }

    /// Reject identity cycles (design §5.1.7, R38 C5): a cycle whose
    /// composed summary does not advance the timestamp (at or below the
    /// default summary) lets a pointstamp support itself around the loop, so
    /// the frontier there could never advance. Returns the location the
    /// cycle passes through and the cycle's summary.
    pub(crate) fn find_identity_cycle(&self) -> Option<(Location, T::Summary)> {
        let identity = T::Summary::default();
        for node in 1..self.shape.len() {
            let (inputs, _) = self.shape[node];
            for input in 0..inputs {
                let start = Location::target(node, input);
                // Summaries of paths from `start` to every location (minimal
                // ones only), one step at a time.
                let mut reached: HashMap<Location, Antichain<T::Summary>> = HashMap::new();
                let mut stack: Vec<(Location, T::Summary)> =
                    vec![(start.clone(), identity.clone())];
                while let Some((location, summary)) = stack.pop() {
                    let next: Vec<(Location, T::Summary)> = match &location {
                        Location::Target { node, port } => {
                            let Some(Some(conn)) = self.nodes.get(*node) else {
                                continue;
                            };
                            let (_, outputs) = self.shape[*node];
                            (0..outputs)
                                .flat_map(|output| {
                                    conn.path(*port, output)
                                        .elements()
                                        .iter()
                                        .filter_map(|s| summary.followed_by(s))
                                        .map(move |s| (Location::source(*node, output), s))
                                        .collect::<Vec<_>>()
                                })
                                .collect()
                        }
                        Location::Source { node, port } => self
                            .edges
                            .get(*node)
                            .and_then(|ports| ports.get(*port))
                            .map(|targets| {
                                targets
                                    .iter()
                                    .map(|target| (target.clone(), summary.clone()))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    };
                    for (location, summary) in next {
                        if location == start {
                            if crate::order::PartialOrder::less_equal(&summary, &identity) {
                                return Some((start, summary));
                            }
                            continue;
                        }
                        if reached
                            .entry(location.clone())
                            .or_default()
                            .insert(summary.clone())
                        {
                            stack.push((location, summary));
                        }
                    }
                }
            }
        }
        None
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
    /// # What is a scope summary?
    ///
    /// A scope summary describes how timestamps entering the scope (via scope
    /// inputs) can reach scope outputs after traversing the internal subgraph.
    /// For each (scope_input, scope_output) pair, the summary is an antichain
    /// of path summaries representing all possible timestamp transformations.
    ///
    /// # Why fixed-point iteration?
    ///
    /// The subgraph may contain cycles (e.g., loop feedback edges). A timestamp
    /// entering at a scope input might traverse a loop N times before reaching a
    /// scope output, accumulating summary compositions at each step. We iterate
    /// until no new summary paths are discovered, which converges because:
    /// 1. Summary composition (`followed_by`) produces monotonically advancing times.
    /// 2. Antichains only grow (new summaries are inserted, never removed).
    /// 3. The timestamp lattice has no infinite descending chains.
    ///
    /// # Algorithm
    ///
    /// We compute "backward reachability" — for each location in the graph, which
    /// scope outputs can it reach, and with what composed summary? This is done
    /// by alternating two propagation steps until convergence:
    ///
    /// 1. **Internal propagation**: For each operator, compose the operator's
    ///    internal path summaries with the output-to-scope-output summaries to
    ///    derive input-to-scope-output summaries.
    /// 2. **Edge propagation**: For each edge (source → target), copy the
    ///    target-to-scope-output summaries to the source (edges are identity).
    ///
    /// Finally, scope input → scope output summaries are read from graph node 0's
    /// source-to-output entries.
    #[allow(clippy::needless_range_loop)]
    fn compute_scope_summary(&self) -> PortConnectivity<T::Summary> {
        let (source_to_output, _) = self.backward_tables();
        let mut summary = PortConnectivity::new(self.scope_inputs, self.scope_outputs);
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

    /// For every location, the summaries of its paths to each scope output
    /// (`[node][port][scope_output]`), for sources and for targets.
    #[allow(clippy::needless_range_loop, clippy::type_complexity)]
    fn backward_tables(
        &self,
    ) -> (
        Vec<Vec<Vec<Antichain<T::Summary>>>>,
        Vec<Vec<Vec<Antichain<T::Summary>>>>,
    ) {
        // Two parallel lookup tables for backward reachability:
        // - source_to_output[node][output_port][scope_output] = Antichain<Summary>
        //   "What summaries can reach scope output `so` from Source(node, output_port)?"
        // - target_to_output[node][input_port][scope_output] = Antichain<Summary>
        //   "What summaries can reach scope output `so` from Target(node, input_port)?"
        let num_nodes = self.shape.len();
        let mut source_to_output: Vec<Vec<Vec<Antichain<T::Summary>>>> = self
            .shape
            .iter()
            .map(|&(_, outputs)| vec![vec![Antichain::new(); self.scope_outputs]; outputs])
            .collect();
        let mut target_to_output: Vec<Vec<Vec<Antichain<T::Summary>>>> = self
            .shape
            .iter()
            .map(|&(inputs, _)| vec![vec![Antichain::new(); self.scope_outputs]; inputs])
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
                if let Some(conn) = self.nodes.get(node).and_then(|n| n.as_ref()) {
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

        (source_to_output, target_to_output)
    }

    /// Compiles forward propagation paths used by the Tracker at build time.
    ///
    /// This pre-computes a lookup table so that during propagation, when a frontier
    /// changes at some location, we can immediately look up which downstream
    /// locations are affected and what summary transformation to apply — without
    /// walking the graph structure on every propagation step.
    ///
    /// Returns two tables:
    /// - `target_summaries[node][input]` → list of `(Source(node, output), summary_antichain)`.
    ///   These represent internal operator paths: when a frontier changes at an
    ///   operator's input, these summaries tell us how timestamps transform to
    ///   reach each of the operator's outputs.
    /// - `source_summaries[node][output]` → list of `(Target(next_node, port), identity)`.
    ///   These represent inter-operator edges: when a frontier changes at an
    ///   operator's output, the timestamp propagates unchanged (identity summary)
    ///   to connected downstream inputs.
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
                                    paths.push((Location::source(node, output), path.clone()));
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

/// For every location of every node, its paths to each scope output:
/// `[node][port] -> [(scope output, summaries)]`.
type ExportTable<T> = Vec<Vec<Vec<(usize, Antichain<<T as Timestamp>::Summary>)>>>;

/// The export table of a `[node][port][scope output]` table of path
/// summaries, without boundary node 0 and without unreachable outputs.
fn export_table<T: Timestamp>(tables: Vec<Vec<Vec<Antichain<T::Summary>>>>) -> ExportTable<T> {
    tables
        .into_iter()
        .enumerate()
        .map(|(node, ports)| {
            ports
                .into_iter()
                .map(|outputs| {
                    if node == 0 {
                        return Vec::new();
                    }
                    outputs
                        .into_iter()
                        .enumerate()
                        .filter(|(_, summaries)| !summaries.is_empty())
                        .collect()
                })
                .collect()
        })
        .collect()
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

/// Per-port state tracking both direct pointstamps and propagated implications.
///
/// The distinction between pointstamps and implications is critical:
///
/// - **`pointstamps`**: Direct capability counts at THIS port. For a Source port,
///   this tracks capabilities held by the operator at that output. For a Target
///   port, this is less commonly used (capabilities are primarily at Sources).
///   Pointstamps are updated directly by `drain_pending_changes()`.
///
/// - **`implications`**: The set of timestamps that COULD reach this port from
///   ANY upstream capability, after applying all path summaries. This is computed
///   by the propagation algorithm. The frontier of implications is what operators
///   observe as their "input frontier" — it tells them which timestamps might
///   still receive new data.
///
/// Example: If operator A holds a capability at time 5, and there's an identity
/// path from A's output to B's input, then B's input Target has an implication
/// at time 5 (even though no pointstamp exists at B's input — the pointstamp
/// is at A's output Source).
struct PortInformation<T: Timestamp> {
    /// Direct capability counts at this port (the "local" view).
    pointstamps: MutableAntichain<T>,
    /// Propagated implications from all reachable upstream capabilities.
    /// The frontier of this antichain is the port's observable frontier.
    implications: MutableAntichain<T>,
    /// Whether some time has a non-zero pointstamp count here (positive or
    /// negative). Refreshed whenever the pointstamps change.
    nonzero: bool,
    /// Whether this port counts towards [`Tracker::counted_zero`].
    counted: bool,
}

impl<T: Timestamp> PortInformation<T> {
    fn new() -> Self {
        Self {
            pointstamps: MutableAntichain::new(),
            implications: MutableAntichain::new(),
            nonzero: false,
            counted: true,
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
/// # How progress flows through the system
///
/// 1. Operators create/drop capabilities → `ProgressReporter` records ±1 changes.
/// 2. `ProgressTracker::collect_operator_progress()` drains reporters into this tracker
///    via `update_source()`.
/// 3. `propagate_all()` processes the changes:
///    a. Updates pointstamp counts at source/target ports.
///    b. Computes frontier changes (which timestamps entered/left the pointstamp frontier).
///    c. Propagates frontier changes through the graph using path summaries.
///    d. Updates implication frontiers at every reachable port.
/// 4. `ProgressTracker::update_operator_frontiers()` reads the new frontiers and reports
///    which operators' frontiers changed ("dirty" operators).
/// 5. The executor re-activates dirty operators, which may observe advanced frontiers
///    and fire notifications.
///
/// # Usage
///
/// 1. Post updates via [`update_target`](Self::update_target) / [`update_source`](Self::update_source).
/// 2. Call [`propagate_all`](Self::propagate_all) to compute frontier implications.
/// 3. Read results via [`drain_pushed`](Self::drain_pushed) / [`drain_output_changes`](Self::drain_output_changes).
pub struct Tracker<T: Timestamp> {
    /// Per-node internal connectivity (retained for introspection/debugging).
    _nodes: Vec<PortConnectivity<T::Summary>>,
    /// Per-node outgoing edges (retained for introspection/debugging).
    _edges: Vec<Vec<Vec<Location>>>,
    /// (inputs, outputs) per node.
    shape: Vec<(usize, usize)>,

    /// Per-operator frontier state (pointstamps and implications at each port).
    per_operator: Vec<PerOperator<T>>,

    /// Pending capability changes buffered before propagation. These accumulate
    /// between `propagate_all()` calls. `target_changes[node][port]` holds changes
    /// at Target(node, port); `source_changes[node][port]` at Source(node, port).
    target_changes: Vec<Vec<ChangeBatch<T>>>,
    source_changes: Vec<Vec<ChangeBatch<T>>>,

    /// Pre-compiled forward propagation paths (computed once at build time).
    /// `target_summaries[node][input]` lists which Source locations can be
    /// reached from Target(node, input) and with what summary antichains.
    /// `source_summaries[node][output]` lists which Target locations can be
    /// reached from Source(node, output) (always identity — edges don't transform).
    target_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,
    source_summaries: Vec<Vec<Vec<(Location, Antichain<T::Summary>)>>>,

    /// Min-heap worklist for propagation. Entries are processed in timestamp
    /// order (smallest first via `Reverse`) to ensure that when we process a
    /// frontier change at time `t`, all changes at times `< t` have already
    /// been processed. Same-location entries are accumulated before processing.
    worklist: BinaryHeap<Reverse<WorklistEntry<T>>>,

    /// Accumulated frontier changes at all locations from the last propagation.
    /// Read by `ProgressTracker::update_operator_frontiers()` to determine which
    /// operators' frontiers changed. Format: `((Location, T), diff)`.
    pushed_changes: ChangeBatch<(Location, T)>,

    /// Scope output frontier changes — used when this subgraph is nested inside
    /// a parent scope and needs to report its output frontier to the parent.
    output_changes: Vec<ChangeBatch<T>>,

    /// The sum of the sizes of every location's pointstamp frontier, updated
    /// only on frontier changes (positive semantics, as in timely): a negative
    /// count never offsets a positive one. Completion is decided by exact
    /// zero over a worker's scope, not by this count.
    total_counts: i64,

    /// The number of counted ports (see [`set_counted`](Self::set_counted))
    /// whose pointstamps hold a non-zero count, kept up to date as counts
    /// change: exact zero over the counted ports is `== 0`, in O(1).
    nonzero_counted: usize,

    /// For every source and target of a node other than the boundary node 0,
    /// its paths to each scope output (`[node][port] -> [(output, summaries)]`).
    source_exports: ExportTable<T>,
    target_exports: ExportTable<T>,
    /// The scope's internal work, projected onto its outputs: changes of the
    /// pointstamp frontiers of every location but node 0's, carried along
    /// `*_exports` (design §5.1.7, the upward export). Positive semantics.
    export_changes: Vec<ChangeBatch<T>>,
}

impl<T: Timestamp> Tracker<T> {
    /// Posts a capability change at an operator's input port.
    pub fn update_target(&mut self, node: usize, port: usize, time: T, diff: i64) {
        debug_assert!(
            node < self.target_changes.len() && port < self.target_changes[node].len(),
            "update_target: node {} port {} out of bounds",
            node,
            port
        );
        self.target_changes[node][port].update(time, diff);
    }

    /// Posts a capability change at an operator's output port.
    pub fn update_source(&mut self, node: usize, port: usize, time: T, diff: i64) {
        debug_assert!(
            node < self.source_changes.len() && port < self.source_changes[node].len(),
            "update_source: node {} port {} out of bounds",
            node,
            port
        );
        self.source_changes[node][port].update(time, diff);
    }

    /// Propagates all pending changes through the reachability graph.
    ///
    /// This is the core algorithm. It works in two steps:
    ///
    /// **Step 1 — Drain pending changes**: Capability changes (from operators
    /// creating/dropping capabilities) are applied to pointstamp counts at each
    /// port. When a pointstamp change alters a port's frontier, the frontier
    /// delta is seeded into the worklist.
    ///
    /// **Step 2 — Process worklist in timestamp order**: The worklist is a
    /// min-heap, so we always process the smallest timestamp first. This is
    /// important because frontier changes at time `t` may be cancelled by
    /// changes at time `t' < t`. Processing in order ensures correctness.
    ///
    /// For each worklist entry `(location, time, diff)`:
    /// - If at a **Target** (input port): apply path summaries to propagate
    ///   the change to downstream Source (output port) locations.
    /// - If at a **Source** (output port): follow edges to propagate the
    ///   change to connected Target locations.
    /// - At each destination, update the implication frontier and, if it
    ///   changed, add new worklist entries for further propagation.
    ///
    /// Same `(time, location)` entries are accumulated before processing,
    /// so +1 and -1 for the same pointstamp cancel out without propagation.
    ///
    /// After calling this, read frontier implications via [`Self::drain_pushed`]
    /// and [`Self::drain_output_changes`].
    pub fn propagate_all(&mut self) {
        // Step 1: Drain pending changes into pointstamps and seed the worklist.
        self.drain_pending_changes();
        self.propagate_worklist();
    }

    /// Step 1 of [`propagate_all`](Self::propagate_all) alone: applies the
    /// posted changes to the pointstamps, records the resulting export
    /// changes and seeds the worklist. A nested scope runs this for every
    /// scope (bottom-up, so each scope's export reaches its parent) before
    /// any scope propagates.
    pub(crate) fn apply_pending(&mut self) {
        self.drain_pending_changes();
    }

    /// Step 2 of [`propagate_all`](Self::propagate_all) alone: propagates
    /// the seeded frontier changes to every implication.
    pub(crate) fn propagate_worklist(&mut self) {
        // Step 2: Process worklist entries in timestamp order.
        while let Some(Reverse(entry)) = self.worklist.pop() {
            // Accumulate all entries with the same (time, location).
            let mut diff = entry.diff;
            while self
                .worklist
                .peek()
                .is_some_and(|Reverse(e)| e.time == entry.time && e.location == entry.location)
            {
                diff += self
                    .worklist
                    .pop()
                    // SAFETY: peek().is_some_and() confirmed entry exists on the line above
                    .expect("worklist entry exists after peek")
                    .0
                    .diff;
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

    /// Drains the export changes of scope output `output`: how the scope's
    /// internal work (node 0 excluded) projected onto that output changed.
    pub(crate) fn drain_export_changes(&mut self, output: usize) -> Vec<(T, i64)> {
        self.export_changes[output].drain().collect()
    }

    /// Returns `true` if the tracker is still tracking any outstanding capabilities
    /// or has pending unpropagated changes.
    pub fn tracking_anything(&self) -> bool {
        if self.total_counts > 0 {
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

    /// Every pointstamp with a non-zero count in this view (for the progress
    /// checker's residual report).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn nonzero_pointstamps(&self) -> Vec<(Location, T, i64)> {
        self.nonzero_counts()
    }

    /// Every pointstamp with a non-zero count in this view.
    pub(crate) fn nonzero_counts(&self) -> Vec<(Location, T, i64)> {
        let mut out = Vec::new();
        for (node, per_op) in self.per_operator.iter().enumerate() {
            for (port, info) in per_op.targets.iter().enumerate() {
                for (time, count) in info.pointstamps.net_counts() {
                    out.push((Location::target(node, port), time, count));
                }
            }
            for (port, info) in per_op.sources.iter().enumerate() {
                for (time, count) in info.pointstamps.net_counts() {
                    out.push((Location::source(node, port), time, count));
                }
            }
        }
        out
    }

    /// Whether changes are posted but not yet propagated.
    pub(crate) fn has_pending(&self) -> bool {
        self.target_changes
            .iter()
            .chain(self.source_changes.iter())
            .any(|ports| ports.iter().any(|batch| !batch.is_empty_clean()))
            || !self.worklist.is_empty()
    }

    /// Whether every pointstamp count at `location` is exactly zero.
    pub(crate) fn counts_zero_at(&self, location: &Location) -> bool {
        let info = match location {
            Location::Target { node, port } => self
                .per_operator
                .get(*node)
                .and_then(|op| op.targets.get(*port)),
            Location::Source { node, port } => self
                .per_operator
                .get(*node)
                .and_then(|op| op.sources.get(*port)),
        };
        info.is_none_or(|info| !info.nonzero)
    }

    /// Whether every pointstamp count of this view is exactly zero.
    pub(crate) fn all_counts_zero(&self) -> bool {
        self.per_operator.iter().all(|op| {
            op.targets
                .iter()
                .chain(op.sources.iter())
                .all(|info| !info.nonzero)
        })
    }

    /// Decides which ports count towards [`counted_zero`](Self::counted_zero):
    /// those at whose location `counted` holds. Every port counts until
    /// this is called.
    pub(crate) fn set_counted(&mut self, counted: impl Fn(&Location) -> bool) {
        let mut nonzero_counted = 0;
        for (node, op) in self.per_operator.iter_mut().enumerate() {
            for (port, info) in op.targets.iter_mut().enumerate() {
                info.counted = counted(&Location::target(node, port));
                nonzero_counted += usize::from(info.counted && info.nonzero);
            }
            for (port, info) in op.sources.iter_mut().enumerate() {
                info.counted = counted(&Location::source(node, port));
                nonzero_counted += usize::from(info.counted && info.nonzero);
            }
        }
        self.nonzero_counted = nonzero_counted;
    }

    /// Whether every pointstamp count at every counted port is exactly zero,
    /// in O(1). Counts are judged one pointstamp at a time, never summed.
    pub(crate) fn counted_zero(&self) -> bool {
        self.nonzero_counted == 0
    }

    /// Refreshes `info.nonzero` after its pointstamps changed, and the
    /// count of non-zero counted ports with it.
    fn refresh_nonzero(info: &mut PortInformation<T>, nonzero_counted: &mut usize) {
        let nonzero = !info.pointstamps.is_zero();
        if nonzero != info.nonzero {
            info.nonzero = nonzero;
            if info.counted {
                if nonzero {
                    *nonzero_counted += 1;
                } else {
                    *nonzero_counted -= 1;
                }
            }
        }
    }

    // -- internal --

    /// Drains pending target/source changes into pointstamps and seeds the worklist.
    ///
    /// This is the bridge between external updates (operators creating/dropping
    /// capabilities) and the propagation algorithm. For each pending change:
    ///
    /// 1. Update `total_counts` (the sum of pointstamp frontier sizes).
    /// 2. Apply the change to the port's `pointstamps` MutableAntichain.
    /// 3. If the pointstamp frontier changed (i.e., the minimal set of active
    ///    timestamps shifted), emit frontier deltas into the worklist.
    ///
    /// The worklist entries are frontier *changes* (not raw pointstamp changes).
    /// This is crucial: if two capabilities exist at times 3 and 5, adding a
    /// third capability at time 4 does NOT change the frontier (3 is still
    /// minimal), so no propagation is needed.
    fn drain_pending_changes(&mut self) {
        for node in 0..self.shape.len() {
            let (inputs, outputs) = self.shape[node];

            for port in 0..inputs {
                if self.target_changes[node][port].is_empty_clean() {
                    continue;
                }
                // The port's whole batch at once, as in timely: at most one
                // frontier rebuild per port and commit, and the frontier
                // changes are the net ones.
                let changes: Vec<(T, i64)> = self.target_changes[node][port].drain().collect();
                let info = &mut self.per_operator[node].targets[port];
                let frontier_changes = info.pointstamps.update_iter(changes);
                Self::refresh_nonzero(info, &mut self.nonzero_counted);
                for (t, d) in frontier_changes {
                    self.total_counts += d;
                    for (output, summaries) in &self.target_exports[node][port] {
                        for summary in summaries.elements() {
                            if let Some(exported) = summary.results_in(&t) {
                                self.export_changes[*output].update(exported, d);
                            }
                        }
                    }
                    self.worklist.push(Reverse(WorklistEntry {
                        time: t,
                        location: Location::target(node, port),
                        diff: d,
                    }));
                }
            }

            for port in 0..outputs {
                if self.source_changes[node][port].is_empty_clean() {
                    continue;
                }
                let changes: Vec<(T, i64)> = self.source_changes[node][port].drain().collect();
                let info = &mut self.per_operator[node].sources[port];
                let frontier_changes = info.pointstamps.update_iter(changes);
                Self::refresh_nonzero(info, &mut self.nonzero_counted);
                for (t, d) in frontier_changes {
                    self.total_counts += d;
                    for (output, summaries) in &self.source_exports[node][port] {
                        for summary in summaries.elements() {
                            if let Some(exported) = summary.results_in(&t) {
                                self.export_changes[*output].update(exported, d);
                            }
                        }
                    }
                    self.worklist.push(Reverse(WorklistEntry {
                        time: t,
                        location: Location::source(node, port),
                        diff: d,
                    }));
                }
            }
        }
    }

    /// Propagates an implication change at a target (input port).
    ///
    /// When an implication frontier changes at Target(node, port), this method:
    /// 1. Updates the implication MutableAntichain at this port.
    /// 2. Records the change in `pushed_changes` (for external consumers).
    /// 3. If this is graph node 0 (scope boundary), records scope output changes.
    /// 4. For each resulting implication delta, applies the operator's internal
    ///    path summaries (`target_summaries`) to compute what Source locations
    ///    can be reached and with what transformed timestamps.
    ///
    /// The `results_in()` call on a summary transforms the input timestamp:
    /// e.g., a feedback summary might map time `t` to `t+1`, representing that
    /// data entering the loop body at time `t` can produce output at time `t+1`.
    fn propagate_target(&mut self, node: usize, port: usize, time: &T, diff: i64) {
        // Update implications at this target.
        let implication_changes = self.per_operator[node].targets[port]
            .implications
            .update_iter(std::iter::once((time.clone(), diff)));

        // Record in pushed_changes.
        let location = Location::target(node, port);
        for (t, d) in &implication_changes {
            self.pushed_changes
                .update((location.clone(), t.clone()), *d);
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
    ///
    /// When an implication frontier changes at Source(node, port), this method:
    /// 1. Updates the implication MutableAntichain at this port.
    /// 2. Records the change in `pushed_changes` (for external consumers).
    /// 3. Follows outgoing edges to connected Target locations. Edges use
    ///    identity transformation (timestamps pass through unchanged), so the
    ///    same `(time, diff)` is pushed to each connected Target's worklist entry.
    ///
    /// The asymmetry with `propagate_target` is intentional: edges between operators
    /// are simple wires (no timestamp transformation), while operator internals
    /// may have complex path summaries (e.g., feedback increments).
    fn propagate_source(&mut self, node: usize, port: usize, time: &T, diff: i64) {
        // Update implications at this source.
        let implication_changes = self.per_operator[node].sources[port]
            .implications
            .update_iter(std::iter::once((time.clone(), diff)));

        // Record in pushed_changes.
        let location = Location::source(node, port);
        for (t, d) in &implication_changes {
            self.pushed_changes
                .update((location.clone(), t.clone()), *d);
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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));

        // Wire: scope_input(0) → op1_input(0), op1_output(0) → scope_output(0)
        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        // Op2: 1 input, 1 output, identity.
        let _ = builder.add_node(2, 1, 1, PortConnectivity::identity(0u64));

        // scope_input → op1_input → op1_output → op2_input → op2_output → scope_output
        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        let _ = builder.add_edge(Location::source(2, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        let _ = builder.add_node(2, 1, 1, PortConnectivity::identity(0u64));

        // Op3: 2 inputs, 1 output, both inputs connect to output 0.
        let mut conn3 = PortConnectivity::new(2, 1);
        conn3.path_mut(0, 0).insert(0u64);
        conn3.path_mut(1, 0).insert(0u64);
        let _ = builder.add_node(3, 2, 1, conn3);

        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(0, 0), Location::target(2, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(3, 0));
        let _ = builder.add_edge(Location::source(2, 0), Location::target(3, 1));
        let _ = builder.add_edge(Location::source(3, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, conn);

        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(1, 0)); // self-loop
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 2, conn);

        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));
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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(summary));

        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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
        let _ = builder.add_node(1, 1, 1, PortConnectivity::identity(0u64));
        let _ = builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        let _ = builder.add_edge(Location::source(1, 0), Location::target(0, 0));

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

    /// A one-input, one-output scope around operator 1 (identity summary).
    fn scope_around_one_operator() -> Tracker<u64> {
        let mut builder = Builder::<u64>::new(1, 1);
        let mut conn = PortConnectivity::new(1, 1);
        conn.path_mut(0, 0).insert(0);
        builder.add_node(1, 1, 1, conn).unwrap();
        builder
            .add_edge(Location::source(0, 0), Location::target(1, 0))
            .unwrap();
        builder
            .add_edge(Location::source(1, 0), Location::target(0, 0))
            .unwrap();
        builder.build().0
    }

    fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
        v.sort();
        v
    }

    #[test]
    fn export_projects_internal_work_and_never_the_import() {
        let mut tracker = scope_around_one_operator();
        // The parent's frontier, imported at boundary node 0: never exported.
        tracker.update_source(0, 0, 3, 1);
        tracker.apply_pending();
        assert!(tracker.drain_export_changes(0).is_empty());
        tracker.propagate_worklist();
        // A capability of operator 1 and a record waiting at its input are
        // internal work, projected onto the scope output.
        tracker.update_source(1, 0, 5, 1);
        tracker.update_target(1, 0, 4, 1);
        tracker.apply_pending();
        assert_eq!(
            sorted(tracker.drain_export_changes(0)),
            vec![(4, 1), (5, 1)]
        );
        tracker.propagate_worklist();
        tracker.update_source(1, 0, 5, -1);
        tracker.update_target(1, 0, 4, -1);
        tracker.apply_pending();
        assert_eq!(
            sorted(tracker.drain_export_changes(0)),
            vec![(4, -1), (5, -1)]
        );
    }

    #[test]
    fn export_follows_frontier_changes_only() {
        let mut tracker = scope_around_one_operator();
        tracker.update_source(1, 0, 5, 1);
        tracker.apply_pending();
        assert_eq!(tracker.drain_export_changes(0), vec![(5, 1)]);
        // A second capability at a later time leaves the frontier unchanged.
        tracker.update_source(1, 0, 7, 1);
        tracker.apply_pending();
        assert!(tracker.drain_export_changes(0).is_empty());
        // Releasing the earlier one moves the frontier to 7.
        tracker.update_source(1, 0, 5, -1);
        tracker.apply_pending();
        assert_eq!(
            sorted(tracker.drain_export_changes(0)),
            vec![(5, -1), (7, 1)]
        );
    }

    /// A loop: concat (node 1, two inputs) feeds a feedback node (node 2)
    /// with `summary`, back into concat's second input.
    fn loop_with_summary<T: Timestamp>(summary: T::Summary) -> Builder<T> {
        let mut builder = Builder::<T>::new(0, 0);
        let mut concat = PortConnectivity::new(2, 1);
        concat.path_mut(0, 0).insert(T::Summary::default());
        concat.path_mut(1, 0).insert(T::Summary::default());
        builder.add_node(1, 2, 1, concat).unwrap();
        let mut feedback = PortConnectivity::new(1, 1);
        feedback.path_mut(0, 0).insert(summary);
        builder.add_node(2, 1, 1, feedback).unwrap();
        builder
            .add_edge(Location::source(1, 0), Location::target(2, 0))
            .unwrap();
        builder
            .add_edge(Location::source(2, 0), Location::target(1, 1))
            .unwrap();
        builder
    }

    #[test]
    fn identity_cycles_are_found() {
        assert!(loop_with_summary::<u64>(0).find_identity_cycle().is_some());
        assert!(loop_with_summary::<u64>(1).find_identity_cycle().is_none());
        assert!(
            loop_with_summary::<Product<u64, u32>>(Product::new(0, 0))
                .find_identity_cycle()
                .is_some()
        );
        assert!(
            loop_with_summary::<Product<u64, u32>>(Product::new(0, 1))
                .find_identity_cycle()
                .is_none()
        );
        // An acyclic graph has no cycle at all.
        assert!(
            scope_around_one_operator_builder()
                .find_identity_cycle()
                .is_none()
        );
    }

    /// Three one-input, one-output operators, nodes 1, 2 and 3 (node 0 is the
    /// scope boundary), in a ring with summaries 0, 0 and 1. The advancing
    /// ring is accepted and kept: a pointstamp at time 17 reaches the loop
    /// head at 18 and the other inputs at 17, and retracting it retracts
    /// exactly those implications.
    #[test]
    fn three_node_ring_propagates_the_increment_to_the_loop_head() {
        let mut builder = Builder::<u64>::new(0, 0);
        builder
            .add_node(1, 1, 1, PortConnectivity::identity(0))
            .unwrap();
        builder
            .add_node(2, 1, 1, PortConnectivity::identity(0))
            .unwrap();
        builder
            .add_node(3, 1, 1, PortConnectivity::identity(1))
            .unwrap();
        builder
            .add_edge(Location::source(1, 0), Location::target(2, 0))
            .unwrap();
        builder
            .add_edge(Location::source(2, 0), Location::target(3, 0))
            .unwrap();
        builder
            .add_edge(Location::source(3, 0), Location::target(1, 0))
            .unwrap();
        assert!(builder.find_identity_cycle().is_none());
        let (mut tracker, _) = builder.build();
        let target_changes = |tracker: &mut Tracker<u64>| {
            sorted(
                tracker
                    .drain_pushed()
                    .into_iter()
                    .filter(|((location, _), _)| matches!(location, Location::Target { .. }))
                    .collect::<Vec<_>>(),
            )
        };

        tracker.update_source(1, 0, 17, 1);
        tracker.propagate_all();
        assert_eq!(
            target_changes(&mut tracker),
            vec![
                ((Location::target(1, 0), 18), 1),
                ((Location::target(2, 0), 17), 1),
                ((Location::target(3, 0), 17), 1),
            ]
        );

        tracker.update_source(1, 0, 17, -1);
        tracker.propagate_all();
        assert_eq!(
            target_changes(&mut tracker),
            vec![
                ((Location::target(1, 0), 18), -1),
                ((Location::target(2, 0), 17), -1),
                ((Location::target(3, 0), 17), -1),
            ]
        );
    }

    /// A chain of three identity operators, nodes 1, 2 and 3, has no identity
    /// cycle until an edge closes it.
    #[test]
    fn closing_a_three_node_identity_chain_makes_an_identity_cycle() {
        let mut builder = Builder::<u64>::new(0, 0);
        for node in 1..=3 {
            builder
                .add_node(node, 1, 1, PortConnectivity::identity(0))
                .unwrap();
        }
        builder
            .add_edge(Location::source(1, 0), Location::target(2, 0))
            .unwrap();
        builder
            .add_edge(Location::source(2, 0), Location::target(3, 0))
            .unwrap();
        assert!(builder.find_identity_cycle().is_none());

        builder
            .add_edge(Location::source(3, 0), Location::target(1, 0))
            .unwrap();
        assert_eq!(
            builder.find_identity_cycle(),
            Some((Location::target(1, 0), 0))
        );
    }

    /// One operator, node 1, with paths in0 -> out0 (summary 0) and
    /// in1 -> out1 (summary 1), each output wired to the other input. The
    /// only cycle passes through both paths and so advances.
    #[test]
    fn crossed_ports_with_one_advancing_path_are_no_identity_cycle() {
        let mut builder = Builder::<u64>::new(0, 0);
        let mut conn = PortConnectivity::new(2, 2);
        conn.path_mut(0, 0).insert(0);
        conn.path_mut(1, 1).insert(1);
        builder.add_node(1, 2, 2, conn).unwrap();
        builder
            .add_edge(Location::source(1, 0), Location::target(1, 1))
            .unwrap();
        builder
            .add_edge(Location::source(1, 1), Location::target(1, 0))
            .unwrap();
        assert!(builder.find_identity_cycle().is_none());
    }

    /// The incremental zero tracking (`counts_zero_at`, `counted_zero`)
    /// agrees with a recount from the pointstamps after every propagation,
    /// under random updates (cancelling, negative, re-counted ports).
    #[test]
    fn incremental_zero_tracking_matches_a_recount() {
        let (mut tracker, _) = scope_around_one_operator_builder().build();
        let ports = [
            Location::target(1, 0),
            Location::source(1, 0),
            Location::target(2, 0),
            Location::source(2, 0),
        ];
        let mut counted: Vec<bool> = vec![true; ports.len()];
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        for step in 0..4000 {
            let port = next(ports.len() as u64) as usize;
            let time = next(6);
            let diff = [-2i64, -1, 1, 2][next(4) as usize];
            match &ports[port] {
                Location::Target { node, port } => tracker.update_target(*node, *port, time, diff),
                Location::Source { node, port } => tracker.update_source(*node, *port, time, diff),
            }
            if next(5) == 0 {
                let i = next(ports.len() as u64) as usize;
                counted[i] = !counted[i];
                let now = counted.clone();
                let ports = ports.clone();
                tracker.set_counted(|l| ports.iter().position(|p| p == l).is_none_or(|i| now[i]));
            }
            if next(3) != 0 {
                continue;
            }
            tracker.propagate_all();
            let mut any_counted_nonzero = false;
            for (i, location) in ports.iter().enumerate() {
                let nonzero = tracker
                    .nonzero_counts()
                    .iter()
                    .any(|(l, _, _)| l == location);
                assert_eq!(
                    tracker.counts_zero_at(location),
                    !nonzero,
                    "step {step}: {location:?}"
                );
                any_counted_nonzero |= counted[i] && nonzero;
            }
            assert_eq!(tracker.counted_zero(), !any_counted_nonzero, "step {step}");
            assert_eq!(
                tracker.all_counts_zero(),
                tracker.nonzero_counts().is_empty(),
                "step {step}"
            );
            // Drive ports back to exactly zero often, so zero and non-zero
            // states of counted and uncounted ports all occur.
            let zero_all = next(4) == 0;
            let zero_one = next(ports.len() as u64) as usize;
            for (location, time, count) in tracker.nonzero_counts() {
                if zero_all || location == ports[zero_one] {
                    match location {
                        Location::Target { node, port } => {
                            tracker.update_target(node, port, time, -count)
                        }
                        Location::Source { node, port } => {
                            tracker.update_source(node, port, time, -count)
                        }
                    }
                }
            }
        }
    }

    fn scope_around_one_operator_builder() -> Builder<u64> {
        let mut builder = Builder::<u64>::new(0, 0);
        let mut conn = PortConnectivity::new(1, 1);
        conn.path_mut(0, 0).insert(0);
        builder.add_node(1, 1, 1, conn.clone()).unwrap();
        builder.add_node(2, 1, 1, conn).unwrap();
        builder
            .add_edge(Location::source(1, 0), Location::target(2, 0))
            .unwrap();
        builder
    }
}
