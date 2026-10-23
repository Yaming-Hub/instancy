//! Subgraph builder and progress tracker.
//!
//! [`SubgraphBuilder`] collects operators and edges during dataflow construction,
//! then compiles into a [`ProgressTracker`] that manages live frontier propagation.
//!
//! # Architecture
//!
//! During graph construction:
//! 1. Operators register via [`add_operator`](SubgraphBuilder::add_operator).
//! 2. Edges are recorded via [`add_edge`](SubgraphBuilder::add_edge).
//! 3. [`build`](SubgraphBuilder::build) compiles the topology into a `ProgressTracker`.
//!
//! At runtime the `ProgressTracker`:
//! 1. Collects capability changes from operators (via [`OperatorProgress`] buffers).
//! 2. Propagates changes through the reachability graph.
//! 3. Delivers frontier updates to operators.
//! 4. Reports completion when all capabilities are drained.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::error::DataflowError;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::{OperatorProgress, PortConnectivity, PortLedger, ProgressReporter};
use crate::progress::progress_channel::{ProgressChange, ProgressKind, WorkerProgressChannels};
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

/// Materialization-time progress handles handed to operator factories:
/// each operator port's capability reporter and port ledger, keyed by
/// `(operator, port)`.
///
/// This type is **non-generic** on purpose: an operator inside a loop is built
/// with timestamp `Product<T, _>` while the worker tracker is `T`, so its factory
/// cannot downcast a `MaterializationReporters<T>`. Each map is type-erased as
/// the concrete `HashMap<(op, port), _<T>>` and looked up by the port's time.
pub(crate) struct MaterializationReporters {
    internal: Box<dyn Any + Send + Sync>,
    /// Consumed ledgers, `HashMap<(op, input), PortLedger<T>>`.
    consumed: Box<dyn Any + Send + Sync>,
    /// Produced ledgers, `HashMap<(op, output), PortLedger<T>>`.
    produced: Box<dyn Any + Send + Sync>,
}

impl MaterializationReporters {
    /// The per-output capability reporter for `(operator, output)`, if present.
    pub(crate) fn internal<T: Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<ProgressReporter<T>> {
        self.internal
            .downcast_ref::<HashMap<(usize, usize), ProgressReporter<T>>>()?
            .get(&(operator, output))
            .cloned()
    }

    /// The consumed ledger of input `input` of `operator`, if present.
    pub(crate) fn consumed<T: Timestamp>(
        &self,
        operator: usize,
        input: usize,
    ) -> Option<PortLedger<T>> {
        self.consumed
            .downcast_ref::<HashMap<(usize, usize), PortLedger<T>>>()?
            .get(&(operator, input))
            .cloned()
    }

    /// The produced ledger of output `output` of `operator`, if present.
    pub(crate) fn produced<T: Timestamp>(
        &self,
        operator: usize,
        output: usize,
    ) -> Option<PortLedger<T>> {
        self.produced
            .downcast_ref::<HashMap<(usize, usize), PortLedger<T>>>()?
            .get(&(operator, output))
            .cloned()
    }
}

// ---------------------------------------------------------------------------
// SubgraphBuilder — collects operators and edges, builds progress tracker
// ---------------------------------------------------------------------------

/// Collects operator registrations and edges during dataflow construction,
/// then compiles them into a [`ProgressTracker`].
///
/// The builder accumulates the topology of the dataflow graph:
/// - **operators**: what they are (shape, name)
/// - **connectivity**: how timestamps transform through each operator
/// - **edges**: which output ports connect to which input ports
/// - **initial capabilities**: which operators start holding capabilities
/// - **progress buffers**: shared reporters that operators will write to at runtime
///
/// Calling [`build()`](Self::build) consumes the builder and produces a
/// `ProgressTracker` that owns all this state and can perform live propagation.
///
/// Graph node 0 (a vertex in the dataflow graph, not a machine) is reserved
/// for the scope boundary (timely convention).
pub struct SubgraphBuilder<T: Timestamp> {
    /// Registered operator shapes.
    operators: HashMap<usize, OperatorShape>,
    /// Per-operator internal connectivity.
    connectivity: HashMap<usize, PortConnectivity<T::Summary>>,
    /// Per-operator initial capabilities (per output port).
    initial_capabilities: HashMap<usize, Vec<ChangeBatch<T>>>,
    /// Edges: (source_node, source_port) → (target_node, target_port).
    edges: Vec<(Location, Location)>,
    /// Per-operator shared progress buffers (created at registration).
    progress_buffers: HashMap<usize, OperatorProgress<T>>,
    /// Number of scope-level inputs.
    scope_inputs: usize,
    /// Number of scope-level outputs.
    scope_outputs: usize,

    /// Attaches the progress checker to the capability reporters of `iterate`
    /// bodies merged into this scope. Those operators keep their own
    /// (loop-time) reporters, which no tracker drains (audit H11), so the
    /// checker binds to them here, independently of the progress graph.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_loop_reporters: Vec<LoopCheckAttach>,
}

/// See [`SubgraphBuilder::adopt_loop_check_reporters`].
#[cfg(any(debug_assertions, feature = "progress-check"))]
type LoopCheckAttach = Arc<dyn Fn(&Arc<dyn crate::progress::check::Sink>, usize) + Send + Sync>;

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
            progress_buffers: self
                .progress_buffers
                .iter()
                .map(|(k, v)| (*k, v.deep_clone()))
                .collect(),
            scope_inputs: self.scope_inputs,
            scope_outputs: self.scope_outputs,

            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_loop_reporters: self.check_loop_reporters.clone(),
        }
    }
}

impl<T: Timestamp> SubgraphBuilder<T> {
    /// Creates a new subgraph builder.
    ///
    /// `scope_inputs` and `scope_outputs` define the scope boundary ports.
    pub fn new(scope_inputs: usize, scope_outputs: usize) -> Self {
        Self {
            operators: HashMap::new(),
            connectivity: HashMap::new(),
            initial_capabilities: HashMap::new(),
            edges: Vec::new(),
            progress_buffers: HashMap::new(),
            scope_inputs,
            scope_outputs,

            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_loop_reporters: Vec::new(),
        }
    }

    /// Replaces the edges with the regular (non-feedback) edges of the
    /// materialized `graph`, so the progress graph and the data graph can
    /// never diverge (P-8).
    pub(crate) fn set_edges_from_graph(&mut self, graph: &crate::dataflow::graph::DataflowGraph) {
        self.edges = graph
            .edges()
            .iter()
            .map(|edge| {
                (
                    Location::source(edge.source.operator_index, edge.source.slot_index),
                    Location::target(edge.target.operator_index, edge.target.slot_index),
                )
            })
            .collect();
    }

    /// Returns clones of the per-output progress reporters keyed by
    /// `(operator_index, output_port)` for materialization-time wiring, plus the
    /// in-flight reporters keyed by source and target for boundary channels.
    pub(crate) fn materialization_reporters(&self) -> MaterializationReporters {
        let internal: HashMap<(usize, usize), ProgressReporter<T>> = self
            .progress_buffers
            .iter()
            .flat_map(|(op_idx, progress)| {
                progress
                    .internal
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(move |(output, reporter)| ((*op_idx, output), reporter))
            })
            .collect();

        let ledgers = |pick: fn(&OperatorProgress<T>) -> &Vec<PortLedger<T>>| {
            let map: HashMap<(usize, usize), PortLedger<T>> = self
                .progress_buffers
                .iter()
                .flat_map(|(op_idx, progress)| {
                    pick(progress)
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(move |(port, ledger)| ((*op_idx, port), ledger))
                })
                .collect();
            Box::new(map) as Box<dyn Any + Send + Sync>
        };
        MaterializationReporters {
            internal: Box::new(internal),
            consumed: ledgers(|p| &p.consumed),
            produced: ledgers(|p| &p.produced),
        }
    }

    /// Registers an operator with its static shape and connectivity.
    ///
    /// Creates shared [`OperatorProgress`] buffers for the operator.
    ///
    /// # Panics
    ///
    /// Panics if the operator index is already registered or is 0
    /// (reserved for scope boundary; "node" here is a dataflow graph vertex,
    /// not a machine).
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
        if self.operators.contains_key(&index) {
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
    pub(crate) fn attach_check_reporters(
        &self,
        oracle: &Arc<crate::progress::check::Oracle<T>>,
        worker: usize,
    ) {
        let sink: Arc<dyn crate::progress::check::Sink> = oracle.clone();
        for (&op, progress) in &self.progress_buffers {
            for (output, reporter) in progress.internal.iter().enumerate() {
                reporter.set_check_port(crate::progress::check::Port::new(
                    sink.clone(),
                    worker,
                    (op, output),
                    None,
                ));
            }
        }
        for attach in &self.check_loop_reporters {
            attach(&sink, worker);
        }
    }

    /// Keep the capability reporters of the `iterate` body `inner` (merged
    /// into this scope) visible to the progress checker, with loop times
    /// projected onto the root time by `project`.
    ///
    /// A loop-body operator holds its capabilities through the reporter it
    /// captured when the body was built; the merged scope tracks the operator
    /// through a different reporter. The capability is counted for no worker
    /// in particular (the staged path shares one body across workers).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn adopt_loop_check_reporters<S: Timestamp>(
        &mut self,
        inner: &SubgraphBuilder<S>,
        project: &crate::progress::check::Projector<S>,
    ) {
        for (&op, progress) in &inner.progress_buffers {
            for (output, reporter) in progress.internal.iter().enumerate() {
                let reporter = reporter.clone();
                let project = Arc::clone(project);
                self.check_loop_reporters
                    .push(Arc::new(move |sink, worker| {
                        reporter.set_check_port(crate::progress::check::Port::new_shared(
                            Arc::clone(sink),
                            worker,
                            (op, output),
                            Some(Arc::clone(&project)),
                        ));
                    }));
            }
        }
        // Bodies of loops nested in `inner` project to the root already.
        self.check_loop_reporters
            .extend(inner.check_loop_reporters.iter().cloned());
    }

    /// Returns the number of registered operators (excluding scope boundary).
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Returns all registered operator shapes (for merging into a parent builder).
    pub fn operator_shapes(&self) -> impl Iterator<Item = &OperatorShape> {
        self.operators.values()
    }

    /// Returns all connectivity entries (for merging into a parent builder).
    pub fn connectivities(&self) -> impl Iterator<Item = (usize, &PortConnectivity<T::Summary>)> {
        self.connectivity.iter().map(|(&k, v)| (k, v))
    }

    /// Returns all edges (for merging into a parent builder).
    pub fn edges(&self) -> &[(Location, Location)] {
        &self.edges
    }

    /// Compiles the subgraph into a live [`ProgressTracker`].
    ///
    /// Consumes the builder and returns the tracker along with per-operator
    /// frontier handles.
    pub fn build(self) -> ProgressTracker<T> {
        // Build the reachability graph.
        let mut reachability_builder =
            ReachabilityBuilder::new(self.scope_inputs, self.scope_outputs);

        // Register all operators.
        for (&index, shape) in &self.operators {
            let conn = self
                .connectivity
                .get(&index)
                // SAFETY: connectivity inserted alongside progress_buffers in add_operator
                .expect("connectivity missing for registered operator");
            // SAFETY: operator was just registered with matching port counts
            reachability_builder
                .add_node(index, shape.inputs, shape.outputs, conn.clone())
                .expect("registered operator should satisfy reachability invariants");
        }

        // Add all edges.
        for (source, target) in &self.edges {
            // SAFETY: edges reference ports validated during add_edge
            reachability_builder
                .add_edge(source.clone(), target.clone())
                .expect("stored edge should reference a registered source port");
        }

        let (tracker, scope_summary) = reachability_builder.build();

        // Every produced record goes to each target of its output's edges,
        // once per edge (multiplicity 1 until the physical plan says more).
        let mut routes: HashMap<(usize, usize), Vec<(usize, usize, i64)>> = HashMap::new();
        for (source, target) in &self.edges {
            if let (
                Location::Source { node, port },
                Location::Target {
                    node: target_node,
                    port: target_port,
                },
            ) = (source, target)
            {
                routes
                    .entry((*node, *port))
                    .or_default()
                    .push((*target_node, *target_port, 1));
            }
        }

        // Build per-operator frontier state.
        let mut operator_frontiers = HashMap::new();
        for (&index, shape) in &self.operators {
            operator_frontiers.insert(
                index,
                OperatorFrontierState {
                    input_frontiers: vec![Antichain::new(); shape.inputs],
                    output_frontiers: vec![Antichain::new(); shape.outputs],
                },
            );
        }

        // Collect operator indices in sorted order for deterministic iteration.
        let mut operator_indices: Vec<usize> = self.operators.keys().copied().collect();
        operator_indices.sort();

        // Materialized indices: operators that have progress buffers.
        let materialized_indices: Vec<usize> = operator_indices
            .iter()
            .copied()
            .filter(|idx| self.progress_buffers.contains_key(idx))
            .collect();

        ProgressTracker {
            tracker,
            _scope_summary: scope_summary,
            operators: self.operators,
            connectivity: self.connectivity,
            progress_buffers: self.progress_buffers,
            initial_capabilities: self.initial_capabilities,
            operator_frontiers,
            operator_indices,
            materialized_indices,
            initialized: false,
            completed: false,
            dirty_operators: Vec::new(),
            progress_channels: None,
            local_changes: ChangeBatch::new(),
            routes,
            scope: Vec::new(),
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check: None,
        }
    }
}

impl<T: Timestamp> fmt::Debug for SubgraphBuilder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubgraphBuilder")
            .field("operators", &self.operators.len())
            .field("edges", &self.edges.len())
            .field("scope_inputs", &self.scope_inputs)
            .field("scope_outputs", &self.scope_outputs)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ProgressTracker — live progress propagation
// ---------------------------------------------------------------------------

/// Manages live frontier propagation for a subgraph.
///
/// This is the main orchestrator that bridges operators and the reachability tracker.
/// It performs a 5-step cycle on each [`propagate()`](Self::propagate) call:
///
/// 1. **Collect**: Drains ±1 capability changes from each operator's
///    [`ProgressReporter`](crate::progress::operate::ProgressReporter) into the
///    reachability tracker's pending change buffers.
/// 2. **Propagate**: Calls `Tracker::propagate_all()` to process pending changes,
///    update pointstamp frontiers, and compute implication frontier deltas.
/// 3. **Update frontiers**: Reads the propagated frontier deltas and updates
///    each operator's per-port frontier snapshots (`OperatorFrontierState`).
/// 4. **Identify dirty operators**: Operators whose frontiers changed are added
///    to the `dirty_operators` list so the executor can re-activate them.
/// 5. **Check completion**: If no outstanding capabilities remain and no pending
///    changes exist, marks the dataflow as completed.
///
/// The tracker also manages initialization: `initialize()` must be called once
/// before the first propagation to seed initial capabilities (e.g., input operators
/// that start with a capability at `T::minimum()`).
pub struct ProgressTracker<T: Timestamp> {
    /// The reachability tracker that does the heavy lifting.
    tracker: Tracker<T>,
    /// Scope-level summary (retained for introspection).
    _scope_summary: PortConnectivity<T::Summary>,
    /// Per-operator internal connectivity (for the worker's scope).
    connectivity: HashMap<usize, PortConnectivity<T::Summary>>,
    /// Registered operator shapes.
    operators: HashMap<usize, OperatorShape>,
    /// Per-operator shared progress buffers.
    progress_buffers: HashMap<usize, OperatorProgress<T>>,
    /// Per-operator initial capabilities.
    initial_capabilities: HashMap<usize, Vec<ChangeBatch<T>>>,
    /// Per-operator current frontiers (updated after propagation).
    operator_frontiers: HashMap<usize, OperatorFrontierState<T>>,
    /// Sorted operator indices for deterministic iteration.
    operator_indices: Vec<usize>,
    /// Operator indices that have progress buffers (materialized).
    /// Used by `collect_operator_progress` to iterate active operators.
    materialized_indices: Vec<usize>,
    /// Whether initial capabilities have been seeded.
    initialized: bool,
    /// Whether the dataflow has completed (no outstanding capabilities).
    completed: bool,
    /// Operators whose frontiers changed in the last propagation round.
    dirty_operators: Vec<usize>,
    /// Cross-worker progress exchange channels (None for single-worker).
    ///
    /// When present, capability changes from local operators are broadcast
    /// to all peer workers, and peer workers' changes are absorbed into
    /// this tracker. This makes `is_completed()` reflect GLOBAL state
    /// across all workers, not just this worker's capabilities.
    progress_channels: Option<WorkerProgressChannels<T>>,
    /// This worker's changes since the last commit, compacted worker-wide.
    /// Filled from the ledgers and reporters by `collect_operator_progress`
    /// and sent to every peer by `broadcast_local_changes`.
    local_changes: ChangeBatch<(ProgressKind, usize, usize, T)>,
    /// Where produced records are counted: for each operator output, the
    /// target input of each of its data edges and the edge's multiplicity
    /// (`hosts(target)` for a broadcast, 1 otherwise).
    routes: HashMap<(usize, usize), Vec<(usize, usize, i64)>>,
    /// The worker's scope (S2b(c)): every location from which a location
    /// this worker hosts is reachable in the data graph. Empty until
    /// `initialize_for_plan`; `None`-like meaning "every location" for a
    /// single-worker tracker initialized with `initialize`.
    scope: Vec<Location>,
    /// The progress checker's view of this tracker (see
    /// [`check`](crate::progress::check)): every propagation publishes the
    /// frontiers that changed.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check: Option<crate::progress::check::TrackerCheck<T>>,
}

/// Per-operator frontier state tracked by the progress tracker.
#[derive(Debug, Clone)]
struct OperatorFrontierState<T: Timestamp> {
    /// Current frontier at each input port.
    input_frontiers: Vec<Antichain<T>>,
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
    /// [`SubgraphBuilder::add_operator_with_capabilities`]) are seeded once
    /// per worker that runs the operator
    /// ([`PhysicalPlan::seed_multiplicity`]), for every operator of the
    /// graph whether or not this worker runs it. Nothing is broadcast: every
    /// worker seeds the same counts, so each view holds every peer's initial
    /// capabilities before any worker runs, and only the releases of those
    /// capabilities travel. Seeding fewer lets a frontier pass work; seeding
    /// more never completes.
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
        plan: &crate::dataflow::plan::PhysicalPlan,
        graph: Option<&crate::dataflow::graph::DataflowGraph>,
        worker: usize,
    ) -> Result<(), crate::Error> {
        if let Some(graph) = graph {
            self.set_routes(graph, plan)?;
            self.set_scope(graph, |op| plan.runs(worker, op));
        }
        self.seed_and_initialize(plan.total_workers(), |op| plan.seed_multiplicity(op))
    }

    /// Count produced records at the target of every data edge of the
    /// materialized graph, regular and feedback alike, with the edge's
    /// multiplicity from the plan (R35).
    fn set_routes(
        &mut self,
        graph: &crate::dataflow::graph::DataflowGraph,
        plan: &crate::dataflow::plan::PhysicalPlan,
    ) -> Result<(), crate::Error> {
        let mut routes: HashMap<(usize, usize), Vec<(usize, usize, i64)>> = HashMap::new();
        let edges = graph.edges().iter().chain(graph.feedback_edges());
        for (index, edge) in edges.enumerate() {
            let planned = plan.edge(index).ok_or_else(|| {
                crate::Error::Dataflow(DataflowError::InvalidGraph(format!(
                    "edge {index} ({edge}) is not in the physical plan"
                )))
            })?;
            routes
                .entry((edge.source.operator_index, edge.source.slot_index))
                .or_default()
                .push((
                    edge.target.operator_index,
                    edge.target.slot_index,
                    planned.multiplicity(),
                ));
        }
        self.routes = routes;
        Ok(())
    }

    /// The worker's scope (S2b(c)): every location from which a location
    /// of an operator `hosted` reports true for is reachable in the data
    /// graph, feedback edges included and timestamps ignored.
    fn set_scope(
        &mut self,
        graph: &crate::dataflow::graph::DataflowGraph,
        hosted: impl Fn(usize) -> bool,
    ) {
        let mut reverse: HashMap<Location, Vec<Location>> = HashMap::new();
        for edge in graph.edges().iter().chain(graph.feedback_edges()) {
            reverse
                .entry(Location::target(
                    edge.target.operator_index,
                    edge.target.slot_index,
                ))
                .or_default()
                .push(Location::source(
                    edge.source.operator_index,
                    edge.source.slot_index,
                ));
        }
        for (&op, connectivity) in &self.connectivity {
            for (input, output, path) in connectivity.iter() {
                if !path.is_empty() {
                    reverse
                        .entry(Location::source(op, output))
                        .or_default()
                        .push(Location::target(op, input));
                }
            }
        }
        let mut seen: std::collections::HashSet<Location> = std::collections::HashSet::new();
        let mut stack: Vec<Location> = Vec::new();
        for (&op, shape) in &self.operators {
            if hosted(op) {
                stack.extend((0..shape.inputs).map(|i| Location::target(op, i)));
                stack.extend((0..shape.outputs).map(|o| Location::source(op, o)));
            }
        }
        while let Some(location) = stack.pop() {
            if seen.insert(location.clone()) {
                if let Some(upstream) = reverse.get(&location) {
                    stack.extend(upstream.iter().cloned());
                }
            }
        }
        let mut scope: Vec<Location> = seen.into_iter().collect();
        scope.sort();
        self.scope = scope;
    }

    /// Seeds each declared initial capability of `op` `multiplicity(op)`
    /// times in a `total_workers`-worker dataflow, then computes the
    /// initial frontiers.
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
        let initial_caps = std::mem::take(&mut self.initial_capabilities);
        for (index, mut caps) in initial_caps {
            let copies = multiplicity(index) as i64;
            for (output, batch) in caps.iter_mut().enumerate() {
                for (time, diff) in batch.drain() {
                    self.tracker
                        .update_source(index, output, time, diff * copies);
                }
            }
        }

        // Every initial capability is declared: nothing may have been
        // reported yet. (Release builds account for it as before.)
        let undeclared = self.collect_operator_progress();
        debug_assert!(
            undeclared == 0,
            "{undeclared} progress change(s) were reported before the tracker was \
             initialized: an operator created a capability at materialization instead of \
             declaring it with `add_operator_with_capabilities` (audit H13)"
        );

        // Absorb anything a peer that already runs has sent.
        self.broadcast_local_changes();
        self.receive_peer_changes()?;

        // Run initial propagation.
        self.tracker.propagate_all();

        // Update frontiers from the initial propagation.
        self.update_operator_frontiers();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_publish(true);

        // Check initial completion.
        self.completed = self.scope_is_zero();
        Ok(())
    }

    /// Returns whether this tracker has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Whether every count in the worker's scope is exactly zero, with no
    /// change pending (S2b(c), exact zero). Counts are checked one
    /// pointstamp at a time, never summed. A tracker initialized without a
    /// plan checks every location.
    pub fn scope_is_zero(&self) -> bool {
        if self.tracker.has_pending() || !self.local_changes.is_empty_clean() {
            return false;
        }
        if self.scope.is_empty() {
            self.tracker.all_counts_zero()
        } else {
            self.scope
                .iter()
                .all(|location| self.tracker.counts_zero_at(location))
        }
    }

    /// The non-zero counts in the worker's scope (for residual reports).
    pub(crate) fn scope_residual(&self) -> Vec<String> {
        self.tracker
            .nonzero_counts()
            .into_iter()
            .filter(|(location, _, _)| self.scope.is_empty() || self.scope.contains(location))
            .map(|(location, time, count)| format!("{location:?}@{time:?}: {count:+}"))
            .collect()
    }

    /// Collects progress updates from all operators and propagates them.
    ///
    /// When cross-worker progress channels are attached, this also:
    /// - Broadcasts local capability changes to all peer workers
    /// - Absorbs peer workers' capability changes into this tracker
    ///
    /// This makes the completion check reflect GLOBAL state across all workers.
    ///
    /// Returns the list of operator indices whose frontiers changed.
    pub fn propagate(&mut self) -> Result<&[usize], crate::Error> {
        if !self.initialized {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "must call initialize() first".into(),
            )));
        }

        // 1. Collect this worker's changes since the last commit from every
        //    hosted ledger and reporter, compacted worker-wide, and apply
        //    them to this view (self-application).
        self.collect_operator_progress();

        // 2. Send the commit to every peer.
        self.broadcast_local_changes();

        // 3. Receive and apply peer workers' changes.
        self.receive_peer_changes()?;

        // 4. Propagate all changes through the reachability graph.
        self.tracker.propagate_all();

        // 5. Update per-operator frontiers.
        self.update_operator_frontiers();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        self.check_publish(false);

        // 6. Exact zero over the worker's scope.
        self.completed = self.scope_is_zero();

        Ok(&self.dirty_operators)
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
        self.tracker.tracking_anything()
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

    /// Returns the current input frontier for an operator.
    pub fn input_frontier(&self, operator: usize, input: usize) -> &Antichain<T> {
        debug_assert!(
            self.operator_frontiers.contains_key(&operator),
            "input_frontier: operator {operator} not registered"
        );
        &self.operator_frontiers[&operator].input_frontiers[input]
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
    /// `operator`: the times that may still appear on it. This covers the
    /// operator's own capabilities (e.g. held notification capabilities) and
    /// everything upstream that can still reach the stream, which is what a
    /// probe attached to the stream must report.
    ///
    /// Conservatively returns `[T::minimum()]` before initialization or for an
    /// unknown location, so a caller can never observe premature completion.
    pub fn stream_frontier(&self, operator: usize, output: usize) -> Antichain<T> {
        if !self.initialized {
            return Antichain::from_elem(T::minimum());
        }
        match self.operator_frontiers.get(&operator) {
            Some(state) if output < state.output_frontiers.len() => {
                state.output_frontiers[output].clone()
            }
            _ => Antichain::from_elem(T::minimum()),
        }
    }

    /// Returns the operators whose frontiers changed in the last propagation.
    pub fn dirty_operators(&self) -> &[usize] {
        &self.dirty_operators
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

    /// Returns each input's frontier for an operator (empty if unknown).
    pub fn operator_input_frontiers(&self, operator: usize) -> Vec<Antichain<T>> {
        self.operator_frontiers
            .get(&operator)
            .map(|state| state.input_frontiers.clone())
            .unwrap_or_default()
    }

    /// Returns the meet (intersection) of all input frontiers for an operator.
    ///
    /// The meet represents the "combined" input frontier: a timestamp is complete
    /// only when ALL input ports have advanced past it. For single-input operators,
    /// this is just the one input frontier.
    ///
    /// Returns an empty antichain if the operator is not registered.
    pub fn operator_input_frontier_meet(&self, operator: usize) -> Antichain<T> {
        let state = match self.operator_frontiers.get(&operator) {
            Some(s) => s,
            None => return Antichain::new(),
        };

        if state.input_frontiers.is_empty() {
            // Source operators have no inputs. Return Antichain::from_elem(T::minimum())
            // so that notifications are NOT fired immediately — the source's progress is
            // tracked via its output capabilities, and the frontier will advance when the
            // executor propagates progress from the source's capability drops.
            return Antichain::from_elem(T::minimum());
        }

        if state.input_frontiers.len() == 1 {
            return state.input_frontiers[0].clone();
        }

        // Combined frontier for multiple inputs: merge all elements into one antichain.
        // This gives us a frontier F where F.less_equal(t) iff ANY input frontier has
        // less_equal(t). A notification fires only when ALL inputs have advanced past
        // the requested time — i.e., when combined.less_equal(t) becomes false.
        // This is the correct semantics: the "join" in the frontier lattice.
        let mut combined = Antichain::new();
        for frontier in &state.input_frontiers {
            for elem in frontier.elements() {
                combined.insert(elem.clone());
            }
        }
        combined
    }

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

    /// Publish this view's frontiers to the oracle: every operator's, or
    /// only those that changed in the last propagation.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_publish(&self, all: bool) {
        let Some(check) = &self.check else {
            return;
        };
        let operators: &[usize] = if all {
            &self.operator_indices
        } else {
            &self.dirty_operators
        };
        let mut targets = Vec::new();
        let mut sources = Vec::new();
        for &op in operators {
            if let Some(state) = self.operator_frontiers.get(&op) {
                for (port, frontier) in state.input_frontiers.iter().enumerate() {
                    targets.push(((op, port), frontier.clone()));
                }
                for (port, frontier) in state.output_frontiers.iter().enumerate() {
                    sources.push(((op, port), frontier.clone()));
                }
            }
        }
        check.publish(targets, sources);
    }

    /// The executor is about to force-close its operators because this
    /// tracker reports completion (V4: nothing may be outstanding).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_force_close(&self) {
        if let Some(check) = &self.check {
            check.force_close();
        }
    }

    /// Stop checking this dataflow (cancelled or failed).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_halt(&self, reason: &str) {
        if let Some(check) = &self.check {
            check.halt(reason);
        }
    }

    /// This tracker's executor finished (`ok`: successfully). Reports the
    /// view's residual counts (V5) and retires the view; when
    /// `report_worker`, also tells the oracle the worker finished.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_finish(&mut self, ok: bool, report_worker: bool) {
        let residual: Vec<String> = if ok && self.check.is_some() {
            self.tracker
                .nonzero_pointstamps()
                .into_iter()
                .map(|(location, time, count)| format!("{location:?}@{time:?}: {count:+}"))
                .collect()
        } else {
            Vec::new()
        };
        if let Some(check) = &mut self.check {
            check.finish(ok, residual, report_worker);
        }
    }

    /// The oracle this tracker publishes to, if any.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_oracle(&self) -> Option<&Arc<crate::progress::check::Oracle<T>>> {
        self.check.as_ref().map(|c| c.oracle())
    }

    // -- internal --

    /// Commits this worker's changes since the last commit: drains every
    /// hosted operator's ledgers and capability reporters (its release
    /// mailboxes) into `local_changes`, compacted worker-wide, and applies
    /// them to this view.
    ///
    /// - `consumed[in]` becomes `Target(op, in): -n`;
    /// - `produced[out]` becomes `Target(edge.target): +n * mult` for every
    ///   data edge of `(op, out)`;
    /// - `internal[out]` (capabilities) becomes `Source(op, out)`.
    ///
    /// Returns the number of changes drained.
    fn collect_operator_progress(&mut self) -> usize {
        let mut drained = ChangeBatch::new();
        for &index in &self.materialized_indices {
            let progress = &self.progress_buffers[&index];
            for (input, ledger) in progress.consumed.iter().enumerate() {
                for (time, n) in ledger.drain() {
                    drained.update((ProgressKind::Target, index, input, time), -n);
                }
            }
            for (output, ledger) in progress.produced.iter().enumerate() {
                let changes = ledger.drain();
                if changes.is_empty() {
                    continue;
                }
                let targets = self
                    .routes
                    .get(&(index, output))
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                for (time, n) in changes {
                    for &(target, port, mult) in targets {
                        drained
                            .update((ProgressKind::Target, target, port, time.clone()), n * mult);
                    }
                }
            }
            for (output, reporter) in progress.internal.iter().enumerate() {
                for (time, diff) in reporter.drain() {
                    drained.update((ProgressKind::Source, index, output, time), diff);
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

    /// Sends this worker's committed changes to every peer worker, as one
    /// batch per peer. No-op without progress channels (single worker).
    fn broadcast_local_changes(&mut self) {
        let channels = match &self.progress_channels {
            Some(c) => c,
            None => {
                self.local_changes.clear();
                return;
            }
        };
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
        if changes.is_empty() {
            return;
        }
        for sender in channels.senders.iter().flatten() {
            sender.send(changes.clone());
        }
    }

    /// Receives and applies peer workers' capability changes to this tracker.
    ///
    /// Drains all pending progress batches from all peer receivers and
    /// applies each change via `tracker.update_source()`. This makes the
    /// local tracker aware of peer workers' capabilities.
    ///
    /// "Peer" means another logical worker/executor — physical location
    /// is irrelevant. The progress channels abstract over shared memory,
    /// network, or any other transport.
    ///
    /// No-op if no progress channels are attached (single-worker mode).
    fn receive_peer_changes(&mut self) -> crate::Result<()> {
        let channels = match &self.progress_channels {
            Some(c) => c,
            None => return Ok(()),
        };

        for receiver in channels.receivers.iter().flatten() {
            for batch in receiver.drain_all()? {
                for change in batch {
                    match change.kind {
                        ProgressKind::Source => self.tracker.update_source(
                            change.node,
                            change.port,
                            change.time,
                            change.diff,
                        ),
                        ProgressKind::Target => self.tracker.update_target(
                            change.node,
                            change.port,
                            change.time,
                            change.diff,
                        ),
                    }
                }
            }
        }
        Ok(())
    }

    /// Updates per-operator frontiers from the tracker's current state.
    fn update_operator_frontiers(&mut self) {
        self.dirty_operators.clear();

        for &index in &self.operator_indices {
            let shape = &self.operators[&index];
            let state = self
                .operator_frontiers
                .get_mut(&index)
                // SAFETY: frontier_states populated for all operators during initialize()
                .expect("frontier state exists for tracked operator");
            let mut changed = false;

            for input in 0..shape.inputs {
                let new_frontier = Antichain::from_iter(
                    self.tracker.target_frontier(index, input).iter().cloned(),
                );
                if new_frontier != state.input_frontiers[input] {
                    state.input_frontiers[input] = new_frontier;
                    changed = true;
                }
            }

            for output in 0..shape.outputs {
                let new_frontier = Antichain::from_iter(
                    self.tracker.source_frontier(index, output).iter().cloned(),
                );
                if new_frontier != state.output_frontiers[output] {
                    state.output_frontiers[output] = new_frontier;
                    changed = true;
                }
            }

            if changed {
                self.dirty_operators.push(index);
            }
        }
    }
}

impl<T: Timestamp> fmt::Debug for ProgressTracker<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgressTracker")
            .field("operators", &self.operator_indices)
            .field("initialized", &self.initialized)
            .field("completed", &self.completed)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        tracker.routes.insert((1, 0), vec![(2, 0, 1), (3, 0, 3)]);
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
        builder.set_edges_from_graph(&graph);
        let mut tracker = builder.build();
        tracker.set_scope(&graph, |op| op == 1);
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
}
