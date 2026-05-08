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

use std::collections::HashMap;
use std::fmt;

use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::{OperatorProgress, PortConnectivity};
use crate::progress::progress_channel::{ProgressChange, WorkerProgressChannels};
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
    ) -> &OperatorProgress<T> {
        assert_ne!(index, 0, "operator index 0 is reserved for scope boundary");
        assert!(
            !self.operators.contains_key(&index),
            "operator index {index} already registered"
        );

        let name = name.into();
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
        self.progress_buffers.get(&index).unwrap()
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
    ) -> &OperatorProgress<T> {
        assert_eq!(
            initial_caps.len(),
            outputs,
            "initial_caps length must match outputs count"
        );
        self.add_operator(index, name, inputs, outputs, connectivity);
        self.initial_capabilities.insert(index, initial_caps);
        self.progress_buffers.get(&index).unwrap()
    }

    /// Records an edge from source output to target input.
    pub fn add_edge(&mut self, source: Location, target: Location) {
        self.edges.push((source, target));
    }

    /// Returns a reference to the progress buffers for a registered operator.
    pub fn operator_progress(&self, index: usize) -> Option<&OperatorProgress<T>> {
        self.progress_buffers.get(&index)
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

    /// Remove all operators (and their edges/capabilities) not in `keep`.
    ///
    /// After calling this, only operators whose index is in `keep` remain.
    /// Edges where either endpoint references a removed operator are dropped.
    /// This is used for per-stage materialization: workers only keep the
    /// operators for stages they participate in.
    pub fn retain_operators(&mut self, keep: &std::collections::HashSet<usize>) {
        self.operators.retain(|idx, _| keep.contains(idx));
        self.connectivity.retain(|idx, _| keep.contains(idx));
        self.initial_capabilities.retain(|idx, _| keep.contains(idx));
        self.progress_buffers.retain(|idx, _| keep.contains(idx));
        self.edges.retain(|(src, tgt)| {
            keep.contains(&src.node()) && keep.contains(&tgt.node())
        });
    }

    /// Mark operators as "ghost" — present in the reachability graph for
    /// frontier propagation, but not materialized on this worker.
    ///
    /// Ghost operators keep their shapes, connectivity, and edges in the
    /// reachability graph so that peer progress updates can propagate
    /// frontiers across stage boundaries. Their initial capabilities and
    /// progress buffers are removed because:
    /// - Initial capabilities: peers provide theirs via progress channels
    /// - Progress buffers: no local operator writes to them
    ///
    /// The [`ProgressTracker`] built from this builder will skip ghost
    /// operators during local progress collection but accept peer updates
    /// for them, enabling correct cross-stage frontier propagation.
    pub fn mark_ghost_operators(&mut self, ghost: &std::collections::HashSet<usize>) {
        self.initial_capabilities.retain(|idx, _| !ghost.contains(idx));
        self.progress_buffers.retain(|idx, _| !ghost.contains(idx));
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
                .expect("connectivity missing for registered operator");
            reachability_builder.add_node(index, shape.inputs, shape.outputs, conn.clone());
        }

        // Add all edges.
        for (source, target) in &self.edges {
            reachability_builder.add_edge(source.clone(), target.clone());
        }

        let (tracker, scope_summary) = reachability_builder.build();

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

        // Materialized indices: operators that have progress buffers (not ghost).
        let materialized_indices: Vec<usize> = operator_indices
            .iter()
            .copied()
            .filter(|idx| self.progress_buffers.contains_key(idx))
            .collect();

        ProgressTracker {
            tracker,
            scope_summary,
            operators: self.operators,
            progress_buffers: self.progress_buffers,
            initial_capabilities: self.initial_capabilities,
            operator_frontiers,
            operator_indices,
            materialized_indices,
            initialized: false,
            completed: false,
            dirty_operators: Vec::new(),
            progress_channels: None,
            local_changes_buffer: Vec::new(),
            peers_heard_from: Vec::new(),
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
    #[allow(dead_code)]
    scope_summary: PortConnectivity<T::Summary>,
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
    /// Operator indices that have progress buffers (materialized, not ghost).
    /// Used by `collect_operator_progress` to skip ghost operators.
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
    /// Buffer for accumulating local changes to broadcast to peers.
    /// Populated during `collect_operator_progress()` and drained
    /// during `broadcast_local_changes()`.
    local_changes_buffer: Vec<ProgressChange<T>>,
    /// Tracks which peer workers have sent at least one progress message.
    /// Indexed by worker id. `true` for self (no receiver) and for peers
    /// that have sent data. `false` for peers we haven't heard from yet.
    /// Used to prevent premature completion: we must hear from all peers
    /// before declaring the dataflow complete, since their initial
    /// capabilities may still be in transit over the network.
    peers_heard_from: Vec<bool>,
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
    /// Seeds initial capabilities into the reachability tracker.
    ///
    /// Must be called once before the first [`propagate`](Self::propagate) call.
    ///
    /// If cross-worker progress channels are attached, this also broadcasts
    /// the initial capabilities to all peer workers and absorbs peers'
    /// initial capabilities. This ensures every worker's tracker starts
    /// with a global view of all capabilities across all workers.
    pub fn initialize(&mut self) {
        assert!(!self.initialized, "ProgressTracker already initialized");
        self.initialized = true;

        // Seed initial capabilities from the builder.
        let initial_caps = std::mem::take(&mut self.initial_capabilities);
        for (index, mut caps) in initial_caps {
            for (output, batch) in caps.iter_mut().enumerate() {
                for (time, diff) in batch.drain() {
                    self.tracker
                        .update_source(index, output, time.clone(), diff);
                    // Accumulate for broadcasting to peers.
                    self.local_changes_buffer.push((index, output, time, diff));
                }
            }
        }

        // Drain any capabilities that operators may have already reported
        // through their ProgressReporter (e.g., from Capability::new).
        self.collect_operator_progress();

        // Broadcast initial capabilities to peers and absorb theirs.
        self.broadcast_local_changes();
        self.receive_peer_changes();

        // Run initial propagation.
        self.tracker.propagate_all();

        // Update frontiers from the initial propagation.
        self.update_operator_frontiers();

        // Check initial completion.
        self.completed = !self.tracker.tracking_anything();
    }

    /// Returns whether this tracker has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
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
    pub fn propagate(&mut self) -> &[usize] {
        assert!(self.initialized, "must call initialize() first");

        // 1. Collect local capability changes from operators.
        //    Also accumulates changes into local_changes_buffer for broadcasting.
        self.collect_operator_progress();

        // 2. Broadcast local changes to all peer workers.
        self.broadcast_local_changes();

        // 3. Receive and apply peer workers' capability changes.
        self.receive_peer_changes();

        // 4. Propagate all changes through the reachability graph.
        self.tracker.propagate_all();

        // 5. Update per-operator frontiers.
        self.update_operator_frontiers();

        // 6. Check completion — now reflects global state if channels are attached.
        self.completed = !self.tracker.tracking_anything();

        &self.dirty_operators
    }

    /// Returns `true` if the dataflow has completed (no outstanding capabilities).
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Returns `true` if there are any outstanding capabilities or pending changes.
    pub fn is_tracking(&self) -> bool {
        self.tracker.tracking_anything()
    }

    /// Attach cross-worker progress exchange channels.
    ///
    /// Must be called before [`initialize()`](Self::initialize) so that
    /// initial capabilities are broadcast to peers during initialization.
    pub fn set_progress_channels(&mut self, channels: WorkerProgressChannels<T>) {
        assert!(
            !self.initialized,
            "set_progress_channels must be called before initialize()"
        );
        // Initialize peer tracking: mark peers with receivers as "not yet heard from".
        // Slots with `None` (self or non-existent peers) are pre-marked as heard.
        self.peers_heard_from = channels.receivers.iter().map(|r| r.is_none()).collect();
        self.progress_channels = Some(channels);
    }

    /// Returns `true` if any peer worker (logical worker in the same
    /// dataflow) has sent progress updates that haven't been absorbed yet.
    ///
    /// "Peer" means another logical worker/executor running the same
    /// dataflow graph — it may be on the same thread, a different thread,
    /// or a different machine. The progress channel abstraction is
    /// physical-layer independent.
    ///
    /// Used as a defense-in-depth check before force-close: even if
    /// `is_completed()` returns true, pending peer progress could
    /// invalidate that conclusion.
    pub fn has_pending_peer_progress(&self) -> bool {
        if let Some(ref channels) = self.progress_channels {
            channels.receivers.iter().any(|r| {
                if let Some(recv) = r {
                    recv.has_pending()
                } else {
                    false
                }
            })
        } else {
            false
        }
    }

    /// Returns `true` if all peer workers have sent at least one progress
    /// message. In multi-node clusters, initial capabilities are broadcast
    /// during tracker initialization. Until we've received those from all
    /// peers, we cannot safely declare the dataflow complete — remote
    /// capabilities may still be in transit over the network.
    ///
    /// Returns `true` when:
    /// - No progress channels are attached (single-worker mode), or
    /// - Every peer with a receiver has sent at least one progress batch.
    pub fn all_peers_synced(&self) -> bool {
        if self.peers_heard_from.is_empty() {
            return true; // no peers, single-worker mode
        }
        self.peers_heard_from.iter().all(|&heard| heard)
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

    // -- internal --

    /// Drains capability changes from all operators' ProgressReporters into the tracker.
    ///
    /// Also accumulates changes into `local_changes_buffer` for broadcasting
    /// to peer workers (if progress channels are attached).
    fn collect_operator_progress(&mut self) {
        let has_channels = self.progress_channels.is_some();

        // Only iterate materialized operators (not ghost). Ghost operators
        // have no progress buffers — their progress comes from peers.
        for &index in &self.materialized_indices {
            let progress = &self.progress_buffers[&index];
            let shape = &self.operators[&index];

            // Drain internal capability changes (from ProgressReporter).
            // These are direct capability hold/release on output ports,
            // reported via Capability::new/clone/drop/downgrade.
            for output in 0..shape.outputs {
                let changes = progress.internal[output].drain();
                for (time, diff) in changes {
                    self.tracker
                        .update_source(index, output, time.clone(), diff);
                    // Accumulate for cross-worker broadcast.
                    if has_channels {
                        self.local_changes_buffer.push((index, output, time, diff));
                    }
                }
            }
        }
    }

    /// Broadcasts accumulated local capability changes to all peer workers.
    ///
    /// Drains `local_changes_buffer` and sends to each peer's progress channel.
    /// No-op if no progress channels are attached (single-worker mode).
    fn broadcast_local_changes(&mut self) {
        let channels = match &self.progress_channels {
            Some(c) => c,
            None => {
                self.local_changes_buffer.clear();
                return;
            }
        };

        if self.local_changes_buffer.is_empty() {
            return;
        }

        let changes = std::mem::take(&mut self.local_changes_buffer);
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
    fn receive_peer_changes(&mut self) {
        let channels = match &self.progress_channels {
            Some(c) => c,
            None => return,
        };

        for (idx, receiver) in channels.receivers.iter().enumerate() {
            if let Some(r) = receiver {
                let batches = r.drain_all();
                if !batches.is_empty() {
                    // Mark this peer as heard from (for initial sync tracking).
                    if idx < self.peers_heard_from.len() {
                        self.peers_heard_from[idx] = true;
                    }
                    for batch in batches {
                        for (op_idx, output_port, time, diff) in batch {
                            self.tracker.update_source(op_idx, output_port, time, diff);
                        }
                    }
                }
            }
        }
    }

    /// Updates per-operator frontiers from the tracker's current state.
    fn update_operator_frontiers(&mut self) {
        self.dirty_operators.clear();

        for &index in &self.operator_indices {
            let shape = &self.operators[&index];
            let state = self.operator_frontiers.get_mut(&index).unwrap();
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
            builder.add_operator(i, format!("op{i}"), 1, 1, PortConnectivity::identity(0u64));
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
        builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        builder.add_operator(2, "op2", 2, 1, PortConnectivity::new(2, 1));
        assert_eq!(builder.operator_count(), 2);
    }

    #[test]
    #[should_panic(expected = "reserved")]
    fn builder_index_zero_panics() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder.add_operator(0, "bad", 1, 1, PortConnectivity::identity(0u64));
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn builder_duplicate_index_panics() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        builder.add_operator(1, "op1_dup", 1, 1, PortConnectivity::identity(0u64));
    }

    #[test]
    fn builder_operator_progress_returned() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        let progress = builder.add_operator(1, "op1", 2, 3, PortConnectivity::new(2, 3));
        assert_eq!(progress.consumed.len(), 2);
        assert_eq!(progress.produced.len(), 3);
        assert_eq!(progress.internal.len(), 3);
    }

    #[test]
    fn builder_with_initial_capabilities() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 0);
        let mut cap_batch = ChangeBatch::new();
        cap_batch.update(0u64, 1);
        builder.add_operator_with_capabilities(
            1,
            "source",
            0,
            1,
            PortConnectivity::new(0, 1),
            vec![cap_batch],
        );
        assert_eq!(builder.operator_count(), 1);
    }

    // --- ProgressTracker: linear pipeline ---

    #[test]
    fn tracker_linear_capability_advance() {
        let builder = linear_pipeline(3);
        let mut tracker = builder.build();
        tracker.initialize();

        // No capabilities yet — should be completed.
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_linear_with_capability_hold() {
        // Build a 2-operator pipeline with initial capability on op1.
        let mut builder = SubgraphBuilder::new(1, 1);
        builder.add_operator_with_capabilities(
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
        );
        builder.add_operator(2, "sink", 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize();

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
        let progress = builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        let reporter = progress.reporter(0).clone();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize();

        // Initially empty — no capabilities.
        assert!(tracker.is_completed());

        // Create a capability via the reporter (simulating Capability::new).
        let _cap = Capability::new(5u64, reporter.clone());

        // Propagate — should see the capability.
        tracker.propagate();
        assert!(!tracker.is_completed());

        // Drop the capability.
        drop(_cap);

        // Propagate — should be completed again.
        tracker.propagate();
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_capability_downgrade_advances_frontier() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        let reporter = progress.reporter(0).clone();
        builder.add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize();

        // Hold capability at time 0.
        let mut cap = Capability::new(0u64, reporter.clone());
        tracker.propagate();

        // Op2 frontier should include time 0.
        let f1 = tracker.input_frontier(2, 0);
        assert!(!f1.is_empty());
        assert!(f1.less_equal(&0));

        // Downgrade to time 5.
        cap.downgrade(&5).unwrap();
        tracker.propagate();

        let f2 = tracker.input_frontier(2, 0);
        assert!(!f2.less_equal(&4), "frontier should have advanced past 4");
        assert!(f2.less_equal(&5), "frontier should include 5");

        // Drop capability entirely.
        drop(cap);
        tracker.propagate();

        let f3 = tracker.input_frontier(2, 0);
        assert!(f3.is_empty(), "frontier should be empty after cap drop");
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_fan_out() {
        // One source, two consumers.
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder.add_operator(1, "source", 1, 1, PortConnectivity::identity(0u64));
        let reporter = progress.reporter(0).clone();
        builder.add_operator(2, "sink_a", 1, 1, PortConnectivity::identity(0u64));
        builder.add_operator(3, "sink_b", 1, 1, PortConnectivity::identity(0u64));

        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        // Fan-out: op1 output → both op2 and op3 inputs.
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(1, 0), Location::target(3, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));
        // op3 output is a dead end for scope output (doesn't feed scope output).

        let mut tracker = builder.build();
        tracker.initialize();

        let _cap = Capability::new(10u64, reporter.clone());
        tracker.propagate();

        // Both sinks should see the capability.
        assert!(!tracker.input_frontier(2, 0).is_empty());
        assert!(!tracker.input_frontier(3, 0).is_empty());

        // Drop capability.
        drop(_cap);
        tracker.propagate();

        assert!(tracker.input_frontier(2, 0).is_empty());
        assert!(tracker.input_frontier(3, 0).is_empty());
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_dirty_operators_reported() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        let reporter = progress.reporter(0).clone();
        builder.add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64));
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(2, 0));
        builder.add_edge(Location::source(2, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize();

        // Add a capability — should dirty both operators.
        let _cap = Capability::new(0u64, reporter.clone());
        let dirty = tracker.propagate();
        assert!(
            dirty.contains(&1) || dirty.contains(&2),
            "at least one operator should be dirty"
        );

        // Propagate again with no changes — no dirty operators.
        let dirty2 = tracker.propagate();
        assert!(dirty2.is_empty(), "no changes, no dirty operators");
    }

    #[test]
    fn tracker_multiple_capabilities() {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let progress = builder.add_operator(1, "op1", 1, 1, PortConnectivity::identity(0u64));
        let reporter = progress.reporter(0).clone();
        builder.add_edge(Location::source(0, 0), Location::target(1, 0));
        builder.add_edge(Location::source(1, 0), Location::target(0, 0));

        let mut tracker = builder.build();
        tracker.initialize();

        // Hold capabilities at times 5 and 10.
        let cap5 = Capability::new(5u64, reporter.clone());
        let cap10 = Capability::new(10u64, reporter.clone());
        tracker.propagate();
        assert!(!tracker.is_completed());

        // Drop cap at 5 — should still be tracking (cap at 10 remains).
        drop(cap5);
        tracker.propagate();
        assert!(!tracker.is_completed());

        // Drop cap at 10 — now completed.
        drop(cap10);
        tracker.propagate();
        assert!(tracker.is_completed());
    }

    #[test]
    fn tracker_operator_indices_sorted() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder.add_operator(5, "op5", 1, 1, PortConnectivity::identity(0u64));
        builder.add_operator(2, "op2", 1, 1, PortConnectivity::identity(0u64));
        builder.add_operator(8, "op8", 1, 1, PortConnectivity::identity(0u64));
        let tracker = builder.build();
        assert_eq!(tracker.operator_indices(), &[2, 5, 8]);
    }

    #[test]
    fn tracker_operator_shape() {
        let mut builder = SubgraphBuilder::<u64>::new(0, 0);
        builder.add_operator(1, "my_op", 2, 3, PortConnectivity::new(2, 3));
        let tracker = builder.build();
        let shape = tracker.operator_shape(1).unwrap();
        assert_eq!(shape.name, "my_op");
        assert_eq!(shape.inputs, 2);
        assert_eq!(shape.outputs, 3);
    }
}
