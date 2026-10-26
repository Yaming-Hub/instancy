//! Protocol property test: the progress protocol, run on the real
//! [`Tracker`](super::reachability::Tracker), keeps every worker's view safe.
//!
//! A pure model of the exchange protocol (Brun et al., ITP 2021):
//!
//! - workers host every operator of a random graph; sources hold
//!   capabilities, and records sit at operator inputs on each worker;
//! - operators take random *justified* steps: a source emits under a
//!   capability it holds (repeatedly), downgrades or drops it; any operator
//!   consumes a record and, in the same step, emits downstream records and
//!   may acquire a capability at or after the record's time (justified by
//!   the consumption), then later emits under it;
//! - each worker records its steps in an uncommitted batch, commits several
//!   at once, applies its own commit immediately and sends it to every peer
//!   as positives-first fragments over a per-pair FIFO channel;
//! - fragments are delivered in a random interleaving across channels;
//! - optionally one parent operator is a *nested scope* (design §5.1.7), on
//!   an outer feedback path or not: a chain of child operators runs inside
//!   it, between its boundary node 0's input (the scope's input) and output.
//!   Every worker keeps a view of the child scope too. A commit carries a
//!   parent and a child section, positives first across both. Nothing
//!   derived is sent: each worker derives the *import* (the parent frontier
//!   at the scope's input, a capability at the child's boundary input) and
//!   the *export* (the child's frontier at its boundary output from its own
//!   counts only, node 0 excluded, a capability at the scope's output) into
//!   its own views.
//!
//! After every step, every view must satisfy FORTE's `Safe`: if a record or
//! capability exists at `(location, t)`, the view's frontier at that location
//! is at or before `t`. After draining all work, every view must hold exactly
//! zero everywhere, derived counts included.
//!
//! Mutants of the protocol must be caught: an unjustified capability
//! acquisition, a dropped `+n`, a negatives-first split, broadcasting the
//! import or the export, deriving the export from node 0's counts too, and a
//! commit split by section (parent first or child first).

use std::collections::{BTreeMap, VecDeque};

use super::operate::PortConnectivity;
use super::reachability::{Builder, Location, Tracker};

/// Timestamps are `0..=T_MAX`; work that would land later is not created.
const T_MAX: u64 = 3;

/// A deterministic PRNG (xorshift64*).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutant {
    None,
    /// Acquire a capability at a time only the (possibly stale) frontier
    /// permits, with no consumption and no held capability justifying it.
    UnjustifiedAcquire,
    /// Emit records without recording their `+n`.
    DroppedPlus,
    /// Split commits negatives-first.
    NegativesFirst,
    /// Send the import's derived changes to peers (in the child section).
    BroadcastImport,
    /// Send the export's derived changes to peers (in the parent section).
    BroadcastExport,
    /// Derive the export from every child count, the import at boundary
    /// node 0 included.
    ExportNode0,
    /// Split commits by section: every parent change before any child one.
    SplitParentFirst,
    /// Split commits by section: every child change before any parent one.
    SplitChildFirst,
}

/// The scope a progress change belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Section {
    Parent,
    Child,
}

/// A progress change: `(section, location, time, diff)`.
type Change = (Section, Location, u64, i64);

/// One operator of the parent graph. Every operator has one output.
#[derive(Clone, Debug)]
struct Op {
    inputs: usize,
    /// The summary from each input to the output (for the nested scope, the
    /// sum of its chain's: its connectivity in the parent).
    summary: u64,
    /// Targets fed by the output.
    targets: Vec<(usize, usize)>,
}

/// A nested scope: parent operator `node`, with child operators
/// `1..=chain.len()` in a chain from the boundary's input to its output.
struct Scope {
    node: usize,
    /// Each child operator's summary.
    chain: Vec<u64>,
}

struct Graph {
    /// Operators `1..=ops.len()`.
    ops: Vec<Op>,
    sources: Vec<usize>,
    scope: Option<Scope>,
}

impl Graph {
    fn op(&self, index: usize) -> &Op {
        &self.ops[index - 1]
    }

    fn source_op() -> Op {
        Op {
            inputs: 0,
            summary: 0,
            targets: Vec::new(),
        }
    }

    /// A random acyclic graph.
    fn random(rng: &mut Rng) -> Self {
        let sources = 1 + rng.below(2);
        let others = 2 + rng.below(3);
        let mut ops: Vec<Op> = (0..sources).map(|_| Self::source_op()).collect();
        for _ in 0..others {
            ops.push(Op {
                inputs: 1 + usize::from(rng.chance(25)),
                summary: u64::from(rng.chance(25)),
                targets: Vec::new(),
            });
        }
        // Wire every input from an earlier operator's output (acyclic).
        for index in (sources + 1)..=ops.len() {
            for input in 0..ops[index - 1].inputs {
                let from = 1 + rng.below(index - 1);
                ops[from - 1].targets.push((index, input));
            }
        }
        Graph {
            sources: (1..=sources).collect(),
            ops,
            scope: None,
        }
    }

    /// A random graph with a nested scope: sources → `concat` → scope →
    /// `after`, plus a few operators downstream. With an outer feedback
    /// path, `after` (summary 1) also feeds `concat`'s second input, so the
    /// scope sits on a cycle of the parent (the TLA+ model `NestedScopes`).
    fn random_scoped(rng: &mut Rng) -> Self {
        let sources = 1 + rng.below(2);
        let cycle = rng.chance(50);
        let mut ops: Vec<Op> = (0..sources).map(|_| Self::source_op()).collect();
        let chain: Vec<u64> = (0..1 + rng.below(3))
            .map(|_| u64::from(rng.chance(30)))
            .collect();
        let concat = sources + 1;
        let scope = sources + 2;
        let after = sources + 3;
        ops.push(Op {
            inputs: 1 + usize::from(cycle),
            summary: 0,
            targets: vec![(scope, 0)],
        });
        ops.push(Op {
            inputs: 1,
            summary: chain.iter().sum(),
            targets: vec![(after, 0)],
        });
        ops.push(Op {
            inputs: 1,
            summary: if cycle { 1 } else { u64::from(rng.chance(30)) },
            targets: if cycle { vec![(concat, 1)] } else { Vec::new() },
        });
        let from = 1 + rng.below(sources);
        ops[from - 1].targets.push((concat, 0));
        for _ in 0..rng.below(3) {
            ops.push(Op {
                inputs: 1,
                summary: u64::from(rng.chance(25)),
                targets: Vec::new(),
            });
            let index = ops.len();
            let from = scope + rng.below(index - scope);
            ops[from - 1].targets.push((index, 0));
        }
        Graph {
            sources: (1..=sources).collect(),
            ops,
            scope: Some(Scope { node: scope, chain }),
        }
    }

    fn tracker(&self) -> Tracker<u64> {
        let mut builder = Builder::<u64>::new(0, 0);
        for (i, op) in self.ops.iter().enumerate() {
            let mut connectivity = PortConnectivity::new(op.inputs, 1);
            for input in 0..op.inputs {
                connectivity.path_mut(input, 0).insert(op.summary);
            }
            builder
                .add_node(i + 1, op.inputs, 1, connectivity)
                .expect("node");
        }
        for (i, op) in self.ops.iter().enumerate() {
            for &(target, port) in &op.targets {
                builder
                    .add_edge(Location::source(i + 1, 0), Location::target(target, port))
                    .expect("edge");
            }
        }
        builder.build().0
    }

    /// The nested scope's tracker: boundary node 0 (its input
    /// `Source(0, 0)`, its output `Target(0, 0)`) and the chain.
    fn child_tracker(&self) -> Option<Tracker<u64>> {
        let scope = self.scope.as_ref()?;
        let mut builder = Builder::<u64>::new(1, 1);
        let k = scope.chain.len();
        for (i, summary) in scope.chain.iter().enumerate() {
            let mut connectivity = PortConnectivity::new(1, 1);
            connectivity.path_mut(0, 0).insert(*summary);
            builder.add_node(i + 1, 1, 1, connectivity).expect("node");
        }
        builder
            .add_edge(Location::source(0, 0), Location::target(1, 0))
            .expect("edge");
        for i in 1..k {
            builder
                .add_edge(Location::source(i, 0), Location::target(i + 1, 0))
                .expect("edge");
        }
        builder
            .add_edge(Location::source(k, 0), Location::target(0, 0))
            .expect("edge");
        Some(builder.build().0)
    }
}

/// Net a batch, dropping zeros.
fn compact(changes: Vec<Change>) -> Vec<Change> {
    let mut net: BTreeMap<(Section, Location, u64), i64> = BTreeMap::new();
    for (section, location, time, diff) in changes {
        *net.entry((section, location, time)).or_insert(0) += diff;
    }
    net.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((section, location, time), diff)| (section, location, time, diff))
        .collect()
}

fn update(tracker: &mut Tracker<u64>, location: Location, time: u64, diff: i64) {
    match location {
        Location::Source { node, port } => tracker.update_source(node, port, time, diff),
        Location::Target { node, port } => tracker.update_target(node, port, time, diff),
    }
}

/// The child's boundary input, where the import lives.
const IMPORT: Location = Location::Source { node: 0, port: 0 };

struct Worker {
    view: Tracker<u64>,
    /// The child scope's view: its counts and the import.
    child: Option<Tracker<u64>>,
    /// The child scope's counts only (no import): what the export derives
    /// from.
    child_raw: Option<Tracker<u64>>,
    /// Steps taken but not committed.
    uncommitted: Vec<Change>,
    /// Records waiting at each input: `(section, op, port) -> times`.
    queues: BTreeMap<(Section, usize, usize), Vec<u64>>,
    /// Capabilities held: `(section, op) -> times`.
    caps: BTreeMap<(Section, usize), Vec<u64>>,
    /// The import and export this worker derived into its own views.
    import: BTreeMap<u64, i64>,
    export: BTreeMap<u64, i64>,
    /// Derived changes to send with the next commit (the broadcast mutants
    /// only).
    derived_out: Vec<Change>,
}

impl Worker {
    /// Apply changes to this worker's views. A child change at the import's
    /// location (only a broadcast mutant sends one) is not a count of the
    /// child's own.
    fn apply(&mut self, changes: &[Change]) {
        for (section, location, time, diff) in changes {
            let (time, diff) = (*time, *diff);
            match section {
                Section::Parent => update(&mut self.view, location.clone(), time, diff),
                Section::Child => {
                    let child = self
                        .child
                        .as_mut()
                        .expect("a child section without a scope");
                    update(child, location.clone(), time, diff);
                    if *location != IMPORT {
                        let raw = self.child_raw.as_mut().expect("child counts");
                        update(raw, location.clone(), time, diff);
                    }
                }
            }
        }
        self.view.propagate_all();
        for tracker in self.child.iter_mut().chain(self.child_raw.iter_mut()) {
            tracker.propagate_all();
        }
    }
}

struct Model {
    graph: Graph,
    mutant: Mutant,
    workers: Vec<Worker>,
    /// Per-pair FIFO progress channels: `channels[from][to]`, fragments.
    channels: Vec<Vec<VecDeque<Vec<Change>>>>,
}

/// A frontier's elements, each counted once.
fn derived(frontier: &[u64]) -> BTreeMap<u64, i64> {
    frontier.iter().map(|t| (*t, 1)).collect()
}

/// The changes that turn `have` into `want` at `location`.
fn delta(
    section: Section,
    location: Location,
    have: &BTreeMap<u64, i64>,
    want: &BTreeMap<u64, i64>,
) -> Vec<Change> {
    let mut out = Vec::new();
    for t in want.keys().chain(have.keys()) {
        let diff = want.get(t).copied().unwrap_or(0) - have.get(t).copied().unwrap_or(0);
        if diff != 0 && !out.iter().any(|(_, _, dt, _)| dt == t) {
            out.push((section, location.clone(), *t, diff));
        }
    }
    out
}

impl Model {
    fn new(graph: Graph, workers: usize, mutant: Mutant) -> Self {
        let mut model = Model {
            workers: (0..workers)
                .map(|_| Worker {
                    view: graph.tracker(),
                    child: graph.child_tracker(),
                    child_raw: graph.child_tracker(),
                    uncommitted: Vec::new(),
                    queues: BTreeMap::new(),
                    caps: BTreeMap::new(),
                    import: BTreeMap::new(),
                    export: BTreeMap::new(),
                    derived_out: Vec::new(),
                })
                .collect(),
            channels: (0..workers)
                .map(|_| (0..workers).map(|_| VecDeque::new()).collect())
                .collect(),
            graph,
            mutant,
        };
        // Agreed initial state: every worker holds a capability at 0 at
        // every source, and every view knows all of them.
        let initial: Vec<Change> = model
            .graph
            .sources
            .iter()
            .map(|&s| (Section::Parent, Location::source(s, 0), 0, workers as i64))
            .collect();
        for w in 0..workers {
            for &s in &model.graph.sources {
                model.workers[w]
                    .caps
                    .entry((Section::Parent, s))
                    .or_default()
                    .push(0);
            }
            model.workers[w].apply(&initial);
            model.rederive(w);
        }
        model
    }

    /// Records and capabilities, by section and location.
    fn truth(&self) -> BTreeMap<(Section, Location, u64), i64> {
        let mut truth: BTreeMap<(Section, Location, u64), i64> = BTreeMap::new();
        for worker in &self.workers {
            for (&(section, op, port), times) in &worker.queues {
                for &t in times {
                    *truth
                        .entry((section, Location::target(op, port), t))
                        .or_insert(0) += 1;
                }
            }
            for (&(section, op), times) in &worker.caps {
                for &t in times {
                    *truth
                        .entry((section, Location::source(op, 0), t))
                        .or_insert(0) += 1;
                }
            }
        }
        truth
    }

    /// FORTE's `Safe` for every view; returns the first violation.
    fn check_safe(&self) -> Result<(), String> {
        for ((section, location, t), n) in self.truth() {
            for (w, worker) in self.workers.iter().enumerate() {
                let view = match section {
                    Section::Parent => &worker.view,
                    Section::Child => worker.child.as_ref().expect("child view"),
                };
                let frontier = match location {
                    Location::Source { node, port } => view.source_frontier(node, port),
                    Location::Target { node, port } => view.target_frontier(node, port),
                };
                if !frontier.iter().any(|f| *f <= t) {
                    return Err(format!(
                        "worker {w}'s {section:?} frontier {frontier:?} at {location:?} is past \
                         {n} unit(s) at {t}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Derive worker `w`'s import and export once, into its own views (as
    /// the tracker does once per propagation). The broadcast mutants also
    /// queue the changes for peers. Returns whether anything changed.
    fn rederive(&mut self, w: usize) -> bool {
        let Some(scope) = &self.graph.scope else {
            return false;
        };
        let node = scope.node;
        let mutant = self.mutant;
        let worker = &mut self.workers[w];
        // Down: the parent frontier at the scope's input.
        let import = derived(worker.view.target_frontier(node, 0));
        let down = delta(Section::Child, IMPORT, &worker.import, &import);
        worker.import = import;
        if !down.is_empty() {
            let child = worker.child.as_mut().expect("child view");
            for (_, location, time, diff) in &down {
                update(child, location.clone(), *time, *diff);
            }
            child.propagate_all();
        }
        // Up: the child's frontier at its output, from its own counts (or,
        // for the mutant, from every count, the import included).
        let from = if mutant == Mutant::ExportNode0 {
            worker.child.as_ref()
        } else {
            worker.child_raw.as_ref()
        };
        let export = derived(from.expect("child view").target_frontier(0, 0));
        let up = delta(
            Section::Parent,
            Location::source(node, 0),
            &worker.export,
            &export,
        );
        worker.export = export;
        if !up.is_empty() {
            for (_, location, time, diff) in &up {
                update(&mut worker.view, location.clone(), *time, *diff);
            }
            worker.view.propagate_all();
        }
        if mutant == Mutant::BroadcastImport {
            worker.derived_out.extend(down.iter().cloned());
        }
        if mutant == Mutant::BroadcastExport {
            worker.derived_out.extend(up.iter().cloned());
        }
        !down.is_empty() || !up.is_empty()
    }

    /// Emit one record at `time` from parent operator `op`'s output to every
    /// target, on random destination workers.
    fn emit(&mut self, rng: &mut Rng, w: usize, op: usize, time: u64) {
        if time > T_MAX {
            return;
        }
        let targets = self.graph.op(op).targets.clone();
        for (target, port) in targets {
            self.place(rng, w, Section::Parent, target, port, time);
        }
    }

    /// Emit one record at `time` from child operator `op`: to the next one,
    /// or, from the last, out of the scope to its parent targets.
    fn emit_child(&mut self, rng: &mut Rng, w: usize, op: usize, time: u64) {
        let scope = self.graph.scope.as_ref().expect("scope");
        let (node, k) = (scope.node, scope.chain.len());
        if op < k {
            if time <= T_MAX {
                self.place(rng, w, Section::Child, op + 1, 0, time);
            }
        } else {
            self.emit(rng, w, node, time);
        }
    }

    /// Put a record at `(op, port)` on a random worker; worker `w`, which
    /// produced it, records its `+1`.
    fn place(&mut self, rng: &mut Rng, w: usize, section: Section, op: usize, port: usize, t: u64) {
        let dest = rng.below(self.workers.len());
        self.workers[dest]
            .queues
            .entry((section, op, port))
            .or_default()
            .push(t);
        if !(self.mutant == Mutant::DroppedPlus && rng.chance(50)) {
            self.workers[w]
                .uncommitted
                .push((section, Location::target(op, port), t, 1));
        }
    }

    fn summary(&self, section: Section, op: usize) -> u64 {
        match section {
            Section::Parent => self.graph.op(op).summary,
            Section::Child => self.graph.scope.as_ref().expect("scope").chain[op - 1],
        }
    }

    fn is_scope(&self, section: Section, op: usize) -> bool {
        section == Section::Parent && self.graph.scope.as_ref().is_some_and(|s| s.node == op)
    }

    /// Emit at `time` from `op`'s output, in its section.
    fn emit_from(&mut self, rng: &mut Rng, w: usize, section: Section, op: usize, time: u64) {
        match section {
            Section::Parent => self.emit(rng, w, op, time),
            Section::Child => self.emit_child(rng, w, op, time),
        }
    }

    /// One random step. Returns `false` if nothing could be done.
    fn step(&mut self, rng: &mut Rng, draining: bool) -> bool {
        let w = rng.below(self.workers.len());
        match rng.below(if draining { 4 } else { 6 }) {
            // Consume a record; emit downstream; maybe acquire a capability.
            0 => {
                let ready: Vec<(Section, usize, usize)> = self.workers[w]
                    .queues
                    .iter()
                    .filter(|(_, times)| !times.is_empty())
                    .map(|(k, _)| *k)
                    .collect();
                let Some(&(section, op, port)) = ready.get(rng.below(ready.len())) else {
                    return false;
                };
                let times = self.workers[w]
                    .queues
                    .get_mut(&(section, op, port))
                    .expect("queue");
                let t = times.remove(rng.below(times.len()));
                self.workers[w]
                    .uncommitted
                    .push((section, Location::target(op, port), t, -1));
                if self.is_scope(section, op) {
                    // Into the scope: a record at the child's first operator.
                    self.place(rng, w, Section::Child, 1, 0, t);
                    return true;
                }
                let out = t + self.summary(section, op);
                if !draining || rng.chance(50) {
                    for _ in 0..1 + rng.below(2) {
                        self.emit_from(rng, w, section, op, out);
                    }
                }
                if !draining && out <= T_MAX && rng.chance(20) {
                    let at = out + rng.below(2) as u64;
                    if at <= T_MAX {
                        self.workers[w]
                            .caps
                            .entry((section, op))
                            .or_default()
                            .push(at);
                        self.workers[w]
                            .uncommitted
                            .push((section, Location::source(op, 0), at, 1));
                    }
                }
                true
            }
            // Use, downgrade or drop a held capability.
            1 | 4 => {
                let held: Vec<(Section, usize, usize)> = self.workers[w]
                    .caps
                    .iter()
                    .flat_map(|(&(s, op), times)| (0..times.len()).map(move |i| (s, op, i)))
                    .collect();
                let Some(&(section, op, i)) = held.get(rng.below(held.len())) else {
                    return false;
                };
                let t = self.workers[w].caps[&(section, op)][i];
                match rng.below(if draining { 2 } else { 3 }) {
                    0 => {
                        // Drop.
                        self.workers[w]
                            .caps
                            .get_mut(&(section, op))
                            .expect("caps")
                            .remove(i);
                        self.workers[w]
                            .uncommitted
                            .push((section, Location::source(op, 0), t, -1));
                    }
                    1 if t < T_MAX => {
                        // Downgrade.
                        self.workers[w].caps.get_mut(&(section, op)).expect("caps")[i] = t + 1;
                        let worker = &mut self.workers[w];
                        worker
                            .uncommitted
                            .push((section, Location::source(op, 0), t, -1));
                        worker
                            .uncommitted
                            .push((section, Location::source(op, 0), t + 1, 1));
                    }
                    _ => {
                        // Emit under the capability (repeatable).
                        let at = t + rng.below(2) as u64;
                        self.emit_from(rng, w, section, op, at);
                    }
                }
                true
            }
            // Commit.
            2 => self.commit(rng, w),
            // Deliver one fragment.
            3 => self.deliver(rng),
            // Mutant: acquire a capability the frontier merely permits.
            _ => {
                if self.mutant != Mutant::UnjustifiedAcquire {
                    return false;
                }
                let op = 1 + rng.below(self.graph.ops.len());
                if self.graph.op(op).inputs == 0 || self.is_scope(Section::Parent, op) {
                    return false;
                }
                let frontier = self.workers[w].view.target_frontier(op, 0).to_vec();
                let Some(&t) = frontier.first() else {
                    return false;
                };
                self.workers[w]
                    .caps
                    .entry((Section::Parent, op))
                    .or_default()
                    .push(t);
                self.workers[w]
                    .uncommitted
                    .push((Section::Parent, Location::source(op, 0), t, 1));
                true
            }
        }
    }

    fn commit(&mut self, rng: &mut Rng, w: usize) -> bool {
        let batch = compact(std::mem::take(&mut self.workers[w].uncommitted));
        if batch.is_empty() && self.workers[w].derived_out.is_empty() {
            return false;
        }
        // Own view: apply at once.
        self.workers[w].apply(&batch);
        self.rederive(w);
        let derived = std::mem::take(&mut self.workers[w].derived_out);
        // Peers: fragments, positives first across both sections (or as
        // the mutant splits them).
        let mut ordered: Vec<Change> = batch.into_iter().chain(derived).collect();
        let mutant = self.mutant;
        ordered.sort_by_key(|&(section, _, _, diff)| match mutant {
            Mutant::NegativesFirst => (false, diff > 0),
            Mutant::SplitParentFirst => (section != Section::Parent, diff <= 0),
            Mutant::SplitChildFirst => (section != Section::Child, diff <= 0),
            _ => (false, diff <= 0),
        });
        let size = 1 + rng.below(3);
        for peer in 0..self.workers.len() {
            if peer == w {
                continue;
            }
            for fragment in ordered.chunks(size) {
                self.channels[w][peer].push_back(fragment.to_vec());
            }
        }
        true
    }

    fn deliver(&mut self, rng: &mut Rng) -> bool {
        let pending: Vec<(usize, usize)> = (0..self.workers.len())
            .flat_map(|a| (0..self.workers.len()).map(move |b| (a, b)))
            .filter(|&(a, b)| !self.channels[a][b].is_empty())
            .collect();
        let Some(&(from, to)) = pending.get(rng.below(pending.len())) else {
            return false;
        };
        let fragment = self.channels[from][to].pop_front().expect("fragment");
        self.workers[to].apply(&fragment);
        self.rederive(to);
        true
    }

    fn busy(&self) -> bool {
        self.workers.iter().any(|w| {
            !w.uncommitted.is_empty()
                || w.queues.values().any(|q| !q.is_empty())
                || w.caps.values().any(|c| !c.is_empty())
        }) || self.channels.iter().flatten().any(|c| !c.is_empty())
    }

    /// Every view holds exactly zero everywhere, derived counts included,
    /// once each worker's derivation has settled.
    fn check_quiescent(&mut self) -> Result<(), String> {
        for w in 0..self.workers.len() {
            let mut settled = false;
            for _ in 0..64 {
                if !self.rederive(w) {
                    settled = true;
                    break;
                }
            }
            if !settled {
                return Err(format!(
                    "worker {w} ends with residual counts: its import and export never settle"
                ));
            }
            let worker = &self.workers[w];
            let mut residual = worker.view.nonzero_counts();
            if let Some(child) = &worker.child {
                residual.extend(child.nonzero_counts());
            }
            if !residual.is_empty() {
                return Err(format!("worker {w} ends with residual counts {residual:?}"));
            }
        }
        Ok(())
    }
}

/// Run one random execution; `Err` describes the first violation.
fn run(seed: u64, mutant: Mutant, with_scope: bool, workers: usize) -> Result<(), String> {
    let mut rng = Rng::new(seed);
    let graph = if with_scope {
        Graph::random_scoped(&mut rng)
    } else {
        Graph::random(&mut rng)
    };
    let mut model = Model::new(graph, workers, mutant);
    for _ in 0..300 {
        model.step(&mut rng, false);
        model.check_safe()?;
    }
    // Drain: no new capabilities; finish all work, commit and deliver all.
    let mut guard = 0;
    while model.busy() {
        guard += 1;
        if guard > 100_000 {
            return Err("did not drain".into());
        }
        model.step(&mut rng, true);
        model.check_safe()?;
    }
    model.check_quiescent()
}

fn first_failure(mutant: Mutant, with_scope: bool, workers: usize, seeds: u64) -> Option<String> {
    (0..seeds).find_map(|seed| {
        run(seed, mutant, with_scope, workers)
            .err()
            .map(|e| format!("seed {seed}: {e}"))
    })
}

#[test]
fn protocol_keeps_every_view_safe_and_ends_at_exact_zero() {
    for workers in [2, 3] {
        for with_scope in [false, true] {
            if let Some(failure) = first_failure(Mutant::None, with_scope, workers, 200) {
                panic!(
                    "correct protocol violated ({workers} workers, scope: {with_scope}): {failure}"
                );
            }
        }
    }
}

/// The mutant is caught: some seed fails under it with one of `symptoms`
/// (the `Safe` check or the end-state residual), and the same seed passes
/// with the correct protocol, so the failure is the mutation's doing.
fn assert_caught(mutant: Mutant, with_scope: bool, workers: usize, seeds: u64, symptoms: &[&str]) {
    let caught = (0..seeds).find_map(|seed| {
        let error = run(seed, mutant, with_scope, workers).err()?;
        if !symptoms.iter().any(|s| error.contains(s)) {
            panic!("{mutant:?}, seed {seed}: failed through an unexpected check: {error}");
        }
        if let Err(baseline) = run(seed, Mutant::None, with_scope, workers) {
            panic!("seed {seed} fails without the mutant too: {baseline}");
        }
        Some(seed)
    });
    assert!(caught.is_some(), "{mutant:?} survived {seeds} seeds");
}

/// A frontier passed work (`Safe`).
const UNSAFE: &str = "is past";
/// A view did not end at exact zero.
const RESIDUAL: &str = "residual counts";

#[test]
fn mutant_unjustified_acquisition_is_caught() {
    assert_caught(Mutant::UnjustifiedAcquire, false, 3, 500, &[UNSAFE]);
}

#[test]
fn mutant_dropped_plus_is_caught() {
    assert_caught(Mutant::DroppedPlus, false, 2, 500, &[UNSAFE, RESIDUAL]);
}

#[test]
fn mutant_negatives_first_split_is_caught() {
    assert_caught(Mutant::NegativesFirst, false, 3, 500, &[UNSAFE]);
}

#[test]
fn mutant_broadcasting_the_import_is_caught() {
    assert_caught(Mutant::BroadcastImport, true, 3, 2000, &[UNSAFE, RESIDUAL]);
}

#[test]
fn mutant_broadcasting_the_export_is_caught() {
    assert_caught(Mutant::BroadcastExport, true, 3, 2000, &[UNSAFE, RESIDUAL]);
}

#[test]
fn mutant_exporting_boundary_node_0_is_caught() {
    assert_caught(Mutant::ExportNode0, true, 2, 2000, &[UNSAFE, RESIDUAL]);
}

#[test]
fn mutant_splitting_a_commit_parent_first_is_caught() {
    assert_caught(Mutant::SplitParentFirst, true, 3, 2000, &[UNSAFE]);
}

#[test]
fn mutant_splitting_a_commit_child_first_is_caught() {
    assert_caught(Mutant::SplitChildFirst, true, 3, 2000, &[UNSAFE]);
}
