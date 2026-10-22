//! Protocol property test: the progress protocol, run on the real
//! [`Tracker`](super::reachability::Tracker), keeps every worker's view safe.
//!
//! A pure model of the exchange protocol (Brun et al., ITP 2021):
//!
//! - workers host every operator of a random acyclic graph; sources hold
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
//! - optionally one operator is a *scope*: the tracker cannot see through it,
//!   and each worker instead derives an *import* (a capability at the scope's
//!   inside) from its own view of the scope's input frontier.
//!
//! After every step, every view must satisfy FORTE's `Safe`: if a record or
//! capability exists at `(location, t)`, the view's frontier at that location
//! is at or before `t`. After draining all work, every view must hold exactly
//! zero everywhere.
//!
//! Mutants of the protocol must be caught: an unjustified capability
//! acquisition, a dropped `+n`, a negatives-first split, and derived scope
//! updates broadcast to peers.

use std::collections::BTreeMap;

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
    /// Send the scope import's derived changes to peers.
    BroadcastDerived,
}

/// One operator of the model graph. Every operator has one output.
#[derive(Clone, Debug)]
struct Op {
    inputs: usize,
    /// Per input: the summary to the output (`None` for the scope, which
    /// the tracker cannot see through).
    summary: Option<u64>,
    /// Targets fed by the output.
    targets: Vec<(usize, usize)>,
}

struct Graph {
    /// Operators `1..=ops.len()` (node 0 is the scope boundary).
    ops: Vec<Op>,
    sources: Vec<usize>,
    /// The scope operator and the import source standing in for it.
    scope: Option<(usize, usize)>,
}

impl Graph {
    fn op(&self, index: usize) -> &Op {
        &self.ops[index - 1]
    }

    fn random(rng: &mut Rng, with_scope: bool) -> Self {
        let sources = 1 + rng.below(2);
        let others = 2 + rng.below(3);
        let mut ops = Vec::new();
        for _ in 0..sources {
            ops.push(Op {
                inputs: 0,
                summary: Some(0),
                targets: Vec::new(),
            });
        }
        for _ in 0..others {
            ops.push(Op {
                inputs: 1 + usize::from(rng.chance(25)),
                summary: Some(if rng.chance(25) { 1 } else { 0 }),
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
        let mut scope = None;
        if with_scope {
            // The scope: an operator with one input whose output feeds at
            // least one operator, and an import source feeding the same.
            let candidates: Vec<usize> = ((sources + 1)..=ops.len())
                .filter(|&i| ops[i - 1].inputs == 1 && !ops[i - 1].targets.is_empty())
                .collect();
            if let Some(&s) = candidates.get(rng.below(candidates.len())) {
                ops[s - 1].summary = None;
                let targets = ops[s - 1].targets.clone();
                ops.push(Op {
                    inputs: 0,
                    summary: Some(0),
                    targets,
                });
                scope = Some((s, ops.len()));
            }
        }
        Graph {
            sources: (1..=sources).collect(),
            ops,
            scope,
        }
    }

    fn tracker(&self) -> Tracker<u64> {
        let mut builder = Builder::<u64>::new(0, 0);
        for (i, op) in self.ops.iter().enumerate() {
            let mut connectivity = PortConnectivity::new(op.inputs, 1);
            if let Some(summary) = op.summary {
                for input in 0..op.inputs {
                    connectivity.path_mut(input, 0).insert(summary);
                }
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
}

/// A progress change: `(location, time, diff)`.
type Change = (Location, u64, i64);

fn apply(tracker: &mut Tracker<u64>, changes: &[Change]) {
    for (location, time, diff) in changes {
        match location {
            Location::Source { node, port } => tracker.update_source(*node, *port, *time, *diff),
            Location::Target { node, port } => tracker.update_target(*node, *port, *time, *diff),
        }
    }
    tracker.propagate_all();
}

/// Net a batch, dropping zeros.
fn compact(changes: Vec<Change>) -> Vec<Change> {
    let mut net: BTreeMap<(Location, u64), i64> = BTreeMap::new();
    for (location, time, diff) in changes {
        *net.entry((location, time)).or_insert(0) += diff;
    }
    net.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((location, time), diff)| (location, time, diff))
        .collect()
}

struct Worker {
    view: Tracker<u64>,
    /// Steps taken but not committed.
    uncommitted: Vec<Change>,
    /// Records waiting at each input: `(op, port) -> times`.
    queues: BTreeMap<(usize, usize), Vec<u64>>,
    /// Capabilities held: `op -> times`.
    caps: BTreeMap<usize, Vec<u64>>,
    /// The import this worker derived into its own view (`time -> count`).
    import: BTreeMap<u64, i64>,
    /// Derived import changes to send with the next commit (the
    /// `BroadcastDerived` mutant only).
    derived_out: Vec<Change>,
}

struct Model {
    graph: Graph,
    mutant: Mutant,
    workers: Vec<Worker>,
    /// Per-pair FIFO progress channels: `channels[from][to]`, fragments.
    channels: Vec<Vec<std::collections::VecDeque<Vec<Change>>>>,
}

impl Model {
    fn new(graph: Graph, workers: usize, mutant: Mutant) -> Self {
        let mut model = Model {
            workers: (0..workers)
                .map(|_| Worker {
                    view: graph.tracker(),
                    uncommitted: Vec::new(),
                    queues: BTreeMap::new(),
                    caps: BTreeMap::new(),
                    import: BTreeMap::new(),
                    derived_out: Vec::new(),
                })
                .collect(),
            channels: (0..workers)
                .map(|_| (0..workers).map(|_| Default::default()).collect())
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
            .map(|&s| (Location::source(s, 0), 0, workers as i64))
            .collect();
        for w in 0..workers {
            for &s in &model.graph.sources {
                model.workers[w].caps.entry(s).or_default().push(0);
            }
            apply(&mut model.workers[w].view, &initial);
            model.rederive(w);
        }
        model
    }

    /// Records at `(op, port)` across workers, and capabilities at `op`.
    fn truth(&self) -> BTreeMap<(Location, u64), i64> {
        let mut truth: BTreeMap<(Location, u64), i64> = BTreeMap::new();
        for worker in &self.workers {
            for (&(op, port), times) in &worker.queues {
                for &t in times {
                    *truth.entry((Location::target(op, port), t)).or_insert(0) += 1;
                }
            }
            for (&op, times) in &worker.caps {
                for &t in times {
                    *truth.entry((Location::source(op, 0), t)).or_insert(0) += 1;
                }
            }
        }
        truth
    }

    /// FORTE's `Safe` for every view; returns the first violation.
    fn check_safe(&self) -> Result<(), String> {
        for ((location, t), n) in self.truth() {
            for (w, worker) in self.workers.iter().enumerate() {
                let frontier = match location {
                    Location::Source { node, port } => worker.view.source_frontier(node, port),
                    Location::Target { node, port } => worker.view.target_frontier(node, port),
                };
                if !frontier.iter().any(|f| *f <= t) {
                    return Err(format!(
                        "worker {w}'s frontier {frontier:?} at {location:?} is past {n} unit(s) at {t}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Recompute worker `w`'s scope import from its own view: a capability
    /// at every element of the scope input's frontier. Applied to its own
    /// view; the `BroadcastDerived` mutant also sends the change to peers.
    fn rederive(&mut self, w: usize) {
        let Some((scope, import)) = self.graph.scope else {
            return;
        };
        let worker = &mut self.workers[w];
        let wanted: BTreeMap<u64, i64> = worker
            .view
            .target_frontier(scope, 0)
            .iter()
            .map(|t| (*t, 1))
            .collect();
        let mut delta = Vec::new();
        for t in wanted.keys().chain(worker.import.keys()) {
            let diff =
                wanted.get(t).copied().unwrap_or(0) - worker.import.get(t).copied().unwrap_or(0);
            if diff != 0 && !delta.iter().any(|(_, dt, _)| dt == t) {
                delta.push((Location::source(import, 0), *t, diff));
            }
        }
        if delta.is_empty() {
            return;
        }
        worker.import = wanted;
        apply(&mut worker.view, &delta);
        if self.mutant == Mutant::BroadcastDerived {
            // Sent to peers with the next commit.
            self.workers[w].derived_out.extend(delta);
        }
        // Applying the import can move frontiers again.
        self.rederive(w);
    }

    /// Emit one record at `time` from `op`'s output to every target, on
    /// random destination workers.
    fn emit(&mut self, rng: &mut Rng, w: usize, op: usize, time: u64) {
        if time > T_MAX {
            return;
        }
        let targets = self.graph.op(op).targets.clone();
        for (target, port) in targets {
            let dest = rng.below(self.workers.len());
            self.workers[dest]
                .queues
                .entry((target, port))
                .or_default()
                .push(time);
            if !(self.mutant == Mutant::DroppedPlus && rng.chance(50)) {
                self.workers[w]
                    .uncommitted
                    .push((Location::target(target, port), time, 1));
            }
        }
    }

    /// One random step. Returns `false` if nothing could be done.
    fn step(&mut self, rng: &mut Rng, draining: bool) -> bool {
        let w = rng.below(self.workers.len());
        match rng.below(if draining { 4 } else { 6 }) {
            // Consume a record; emit downstream; maybe acquire a capability.
            0 => {
                let ready: Vec<(usize, usize)> = self.workers[w]
                    .queues
                    .iter()
                    .filter(|(_, times)| !times.is_empty())
                    .map(|(k, _)| *k)
                    .collect();
                let Some(&(op, port)) = ready.get(rng.below(ready.len())) else {
                    return false;
                };
                let times = self.workers[w].queues.get_mut(&(op, port)).expect("queue");
                let t = times.remove(rng.below(times.len()));
                self.workers[w]
                    .uncommitted
                    .push((Location::target(op, port), t, -1));
                let out = t + self.graph.op(op).summary.unwrap_or(0);
                if !draining || rng.chance(50) {
                    for _ in 0..1 + rng.below(2) {
                        self.emit(rng, w, op, out);
                    }
                }
                // The scope only forwards (its inside is the import's).
                let is_scope = self.graph.scope.is_some_and(|(s, _)| s == op);
                if !draining && !is_scope && out <= T_MAX && rng.chance(20) {
                    let at = out + rng.below(2) as u64;
                    if at <= T_MAX {
                        self.workers[w].caps.entry(op).or_default().push(at);
                        self.workers[w]
                            .uncommitted
                            .push((Location::source(op, 0), at, 1));
                    }
                }
                true
            }
            // Use, downgrade or drop a held capability.
            1 | 4 => {
                let held: Vec<(usize, usize)> = self.workers[w]
                    .caps
                    .iter()
                    .flat_map(|(op, times)| (0..times.len()).map(move |i| (*op, i)))
                    .collect();
                let Some(&(op, i)) = held.get(rng.below(held.len())) else {
                    return false;
                };
                let t = self.workers[w].caps[&op][i];
                match rng.below(if draining { 2 } else { 3 }) {
                    0 => {
                        // Drop.
                        self.workers[w].caps.get_mut(&op).expect("caps").remove(i);
                        self.workers[w]
                            .uncommitted
                            .push((Location::source(op, 0), t, -1));
                    }
                    1 if t < T_MAX => {
                        // Downgrade.
                        self.workers[w].caps.get_mut(&op).expect("caps")[i] = t + 1;
                        self.workers[w]
                            .uncommitted
                            .push((Location::source(op, 0), t, -1));
                        self.workers[w]
                            .uncommitted
                            .push((Location::source(op, 0), t + 1, 1));
                    }
                    _ => {
                        // Emit under the capability (repeatable).
                        let at = t + rng.below(2) as u64;
                        self.emit(rng, w, op, at);
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
                if self.graph.op(op).inputs == 0 || Some(op) == self.graph.scope.map(|s| s.1) {
                    return false;
                }
                let frontier = self.workers[w].view.target_frontier(op, 0).to_vec();
                let Some(&t) = frontier.first() else {
                    return false;
                };
                self.workers[w].caps.entry(op).or_default().push(t);
                self.workers[w]
                    .uncommitted
                    .push((Location::source(op, 0), t, 1));
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
        apply(&mut self.workers[w].view, &batch);
        self.rederive(w);
        let derived = std::mem::take(&mut self.workers[w].derived_out);
        // Peers: fragments, positives-first (negatives-first for the mutant).
        let mut ordered: Vec<Change> = batch.into_iter().chain(derived).collect();
        let negatives_first = self.mutant == Mutant::NegativesFirst;
        ordered.sort_by_key(|(_, _, diff)| {
            if negatives_first {
                *diff > 0
            } else {
                *diff <= 0
            }
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
        apply(&mut self.workers[to].view, &fragment);
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

    /// Every view holds exactly zero everywhere (the import excepted, whose
    /// derived count must be zero too once the scope's input is done).
    fn check_quiescent(&self) -> Result<(), String> {
        for (w, worker) in self.workers.iter().enumerate() {
            let residual = worker.view.nonzero_pointstamps();
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
    let graph = Graph::random(&mut rng, with_scope);
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
fn mutant_broadcast_derived_scope_updates_is_caught() {
    assert_caught(Mutant::BroadcastDerived, true, 3, 2000, &[UNSAFE, RESIDUAL]);
}
