//! Local differential comparison of instancy against timely-dataflow.
//!
//! The reusable surface is: deterministic workloads ([`Scenario`]), the
//! native program builders in [`instancy_backend`] and [`timely_backend`],
//! and an independent comparator over plain coordinate tuples. The
//! checkpoint protocol in [`run_engine`] is the causal-cut contract of the
//! local brief: it is not a general test runner.

#[cfg(feature = "cluster")]
pub mod cluster_backend;
pub mod instancy_backend;
pub mod programs;
pub mod timely_backend;
pub mod workloads;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

/// Bound on reaching one checkpoint, and on each engine's teardown. A
/// hang/failure bound, not a performance assertion.
pub const CHECKPOINT: Duration = Duration::from_secs(10);

/// `(logical workers, instancy channel capacity)` for seeds 1 through 8.
pub const SETTINGS: [(usize, usize); 8] = [
    (1, 1),
    (2, 1),
    (3, 1),
    (2, 2),
    (3, 2),
    (1, 1024),
    (2, 1024),
    (3, 1024),
];

/// Instancy execution-pool threads used by the correctness examples.
pub const INSTANCY_POOL_THREADS: usize = 2;
/// Tokio threads owned by the instancy runtime in the correctness examples.
pub const INSTANCY_TOKIO_THREADS: usize = 2;

/// How the in-process instancy engine is configured. The default is the
/// configuration of the reviewed experiment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstancyConfig {
    pub pool_threads: usize,
    pub tokio_threads: usize,
    /// `SpawnOptions::per_stage_parallelism`.
    pub per_stage: bool,
    /// `IoMode::Async` inputs and outputs instead of the sync ones.
    pub io_async: bool,
}

impl Default for InstancyConfig {
    fn default() -> Self {
        InstancyConfig {
            pool_threads: INSTANCY_POOL_THREADS,
            tokio_threads: INSTANCY_TOKIO_THREADS,
            per_stage: false,
            io_async: false,
        }
    }
}

/// A timestamp as plain coordinates, outermost first.
pub type Time = Vec<u64>;
/// A frontier as a list of coordinate tuples; empty is complete.
pub type Frontier = Vec<Time>;
/// An input record: `(key, value)`.
pub type Rec = (u64, i64);
/// An observation record: `(native time coordinates, payload)`.
pub type Out = (Time, Vec<i64>);

/// The explicit routing hash, used by both engines.
pub fn route(record: &Rec) -> u64 {
    record.0
}

/// The same routing hash for in-loop records, whose payload starts with the
/// key.
pub fn route_out(record: &Out) -> u64 {
    record.1[0] as u64
}

/// Loop-body records keep `[key, value]`; drop the count of a total.
pub fn strip_count(record: Out) -> Out {
    (record.0, record.1[..2].to_vec())
}

// ---------------------------------------------------------------------------
// Coordinates
// ---------------------------------------------------------------------------

/// Conversion between an engine timestamp and plain coordinates.
pub trait Coords: Sized {
    fn push_coords(&self, out: &mut Vec<u64>);
    fn take_coords(c: &[u64]) -> (Self, &[u64]);

    fn to_coords(&self) -> Time {
        let mut out = Vec::new();
        self.push_coords(&mut out);
        out
    }

    fn from_coords(c: &[u64]) -> Self {
        let (time, rest) = Self::take_coords(c);
        assert!(rest.is_empty(), "too many coordinates: {c:?}");
        time
    }
}

impl Coords for u64 {
    fn push_coords(&self, out: &mut Vec<u64>) {
        out.push(*self);
    }
    fn take_coords(c: &[u64]) -> (Self, &[u64]) {
        (c[0], &c[1..])
    }
}

impl Coords for u32 {
    fn push_coords(&self, out: &mut Vec<u64>) {
        out.push(u64::from(*self));
    }
    fn take_coords(c: &[u64]) -> (Self, &[u64]) {
        (u32::try_from(c[0]).expect("u32 coordinate"), &c[1..])
    }
}

// ---------------------------------------------------------------------------
// Independent comparator (does not use either engine's order or antichains)
// ---------------------------------------------------------------------------

/// Componentwise order on equal-length coordinate tuples.
pub fn time_le(a: &[u64], b: &[u64]) -> bool {
    assert_eq!(a.len(), b.len(), "compared times of different shapes");
    a.iter().zip(b).all(|(x, y)| x <= y)
}

/// Whether no element is less than or equal to another (no duplicates).
pub fn is_antichain(f: &[Time]) -> bool {
    f.iter()
        .enumerate()
        .all(|(i, a)| f.iter().enumerate().all(|(j, b)| i == j || !time_le(a, b)))
}

/// The minimal elements, sorted for display and equality only.
pub fn canonical(f: &[Time]) -> Frontier {
    let mut out: Frontier = Vec::new();
    for (i, t) in f.iter().enumerate() {
        let dominated = f
            .iter()
            .enumerate()
            .any(|(j, u)| time_le(u, t) && (u != t || j < i));
        if !dominated {
            out.push(t.clone());
        }
    }
    out.sort();
    out
}

/// The meet of several frontier views.
pub fn meet(views: &[Frontier]) -> Frontier {
    canonical(&views.concat())
}

/// `a <= b`: every element of `b` has some element of `a` below it.
pub fn frontier_le(a: &[Time], b: &[Time]) -> bool {
    b.iter().all(|y| a.iter().any(|x| time_le(x, y)))
}

/// A time is complete when no frontier element is less than or equal to it.
pub fn is_complete(f: &[Time], t: &[u64]) -> bool {
    !f.iter().any(|x| time_le(x, t))
}

/// One observed record: probe (graph and observation point), actual worker,
/// native time and payload.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Obs {
    pub probe: usize,
    pub worker: usize,
    pub time: Time,
    pub payload: Vec<i64>,
}

/// Records with multiplicity; never deduplicated.
pub type Multiset = BTreeMap<Obs, usize>;
/// Worker-free model records: `(probe, time, payload)` with multiplicity.
pub type ModelSet = BTreeMap<(usize, Time, Vec<i64>), usize>;

/// The part of `set` at times complete under each probe's frontier.
pub fn completed_region(set: &Multiset, frontiers: &[Frontier]) -> Multiset {
    set.iter()
        .filter(|(o, _)| is_complete(&frontiers[o.probe], &o.time))
        .map(|(o, n)| (o.clone(), *n))
        .collect()
}

/// Multiset difference, both directions, for diagnostics.
pub fn multiset_diff<K: Ord + Clone + std::fmt::Debug>(
    left: &BTreeMap<K, usize>,
    right: &BTreeMap<K, usize>,
) -> Option<String> {
    let mut out = String::new();
    for (k, n) in left {
        let m = right.get(k).copied().unwrap_or(0);
        if *n != m {
            let _ = writeln!(out, "  {k:?}: {n} vs {m}");
        }
    }
    for (k, m) in right {
        if !left.contains_key(k) {
            let _ = writeln!(out, "  {k:?}: 0 vs {m}");
        }
    }
    (!out.is_empty()).then_some(out)
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// One input action, addressed by flat input index.
#[derive(Clone, Debug)]
pub enum Action {
    Send {
        input: usize,
        worker: usize,
        time: Time,
        records: Vec<Rec>,
    },
    Advance {
        input: usize,
        time: Time,
    },
    Close {
        input: usize,
    },
}

#[derive(Clone, Debug)]
pub struct InputSpec {
    pub graph: usize,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct ProbeSpec {
    pub graph: usize,
    pub name: String,
    /// Emits only at completed times, so unfinished-time output is an error.
    pub gated: bool,
}

/// A checkpoint: actions, then the prescribed frontier per probe, the
/// cumulative operator receipts per graph, and the graphs that must have
/// completed (every graph whose probes are all empty).
#[derive(Clone, Debug)]
pub struct Phase {
    pub name: String,
    pub actions: Vec<Action>,
    pub frontiers: Vec<Frontier>,
    pub receipts: Vec<u64>,
    pub completed: Vec<usize>,
}

/// A completion-gated held time: while `time` is unfinished at `probe`, it
/// must receive real records and then a later same-key addition.
#[derive(Clone, Debug)]
pub struct HeldSpec {
    pub graph: usize,
    pub probe: usize,
    pub time: Time,
}

#[derive(Clone, Debug)]
pub struct Scenario {
    pub recipe: &'static str,
    pub seed: u64,
    pub workers: usize,
    pub capacity: usize,
    pub graphs: usize,
    /// Outer loop iterations of the nested recipe (0 elsewhere).
    pub outer_iterations: u64,
    pub inputs: Vec<InputSpec>,
    pub probes: Vec<ProbeSpec>,
    pub phases: Vec<Phase>,
    /// Exact expected records of the modelled probes, without workers.
    pub model: ModelSet,
    pub modelled: Vec<usize>,
    pub held: Vec<HeldSpec>,
    pub instancy: InstancyConfig,
    /// The generated program of a random-program scenario.
    pub program: Option<programs::Program>,
}

impl Scenario {
    pub fn label(&self) -> String {
        format!(
            "{} seed={} workers={} capacity={} graphs={} outer_iterations={} instancy={:?}",
            self.recipe,
            self.seed,
            self.workers,
            self.capacity,
            self.graphs,
            self.outer_iterations,
            self.instancy
        )
    }

    /// Whether every declared held time first holds real records and later,
    /// still unfinished, receives a record with an already present key.
    pub fn check_witnesses(&self) -> Result<(), String> {
        for h in &self.held {
            let mut keys = std::collections::BTreeSet::new();
            let mut held_with_records = false;
            let mut witnessed = false;
            for phase in &self.phases {
                let unfinished = !is_complete(&phase.frontiers[h.probe], &h.time);
                let mut here = Vec::new();
                for a in &phase.actions {
                    if let Action::Send {
                        input,
                        time,
                        records,
                        ..
                    } = a
                        && self.inputs[*input].graph == h.graph
                        && *time == h.time
                    {
                        here.extend(records.iter().map(|r| r.0));
                    }
                }
                if held_with_records && unfinished && here.iter().any(|k| keys.contains(k)) {
                    witnessed = true;
                    break;
                }
                keys.extend(here);
                held_with_records |= !keys.is_empty() && unfinished;
            }
            if !witnessed {
                return Err(format!(
                    "{}: held time {h:?} has no same-key extension",
                    self.label()
                ));
            }
        }
        Ok(())
    }

    /// The complete input schedule, for failure reports.
    pub fn describe(&self) -> String {
        let mut out = format!("scenario {}\n", self.label());
        if let Some(program) = &self.program {
            let _ = writeln!(out, " program {program:?}");
        }
        for (k, phase) in self.phases.iter().enumerate() {
            let _ = writeln!(out, " phase {k} {}:", phase.name);
            for a in &phase.actions {
                let _ = match a {
                    Action::Send {
                        input,
                        worker,
                        time,
                        records,
                    } => writeln!(
                        out,
                        "  send {} w{worker} t{time:?} {records:?}",
                        self.inputs[*input].name
                    ),
                    Action::Advance { input, time } => {
                        writeln!(out, "  advance {} to {time:?}", self.inputs[*input].name)
                    }
                    Action::Close { input } => {
                        writeln!(out, "  close {}", self.inputs[*input].name)
                    }
                };
            }
            for (p, f) in phase.frontiers.iter().enumerate() {
                let _ = writeln!(out, "  expect {} F={f:?}", self.probe_name(p));
            }
            let _ = writeln!(out, "  expect receipts {:?}", phase.receipts);
        }
        out
    }

    pub fn probe_name(&self, p: usize) -> String {
        format!("g{}.{}", self.probes[p].graph, self.probes[p].name)
    }

    pub fn input_index(&self, graph: usize, name: &str) -> usize {
        self.inputs
            .iter()
            .position(|i| i.graph == graph && i.name == name)
            .expect("input")
    }

    pub fn probe_index(&self, graph: usize, name: &str) -> usize {
        self.probes
            .iter()
            .position(|p| p.graph == graph && p.name == name)
            .expect("probe")
    }

    /// Cumulative receipts per graph: every sent record reaches the graph's
    /// totals operator exactly once. Also derives the graphs each phase
    /// requires to have completed.
    pub(crate) fn fill_record_receipts(&mut self) {
        let mut total = vec![0u64; self.graphs];
        for phase in &mut self.phases {
            for a in &phase.actions {
                if let Action::Send { input, records, .. } = a {
                    total[self.inputs[*input].graph] += records.len() as u64;
                }
            }
            phase.receipts = total.clone();
            phase.completed = (0..self.graphs)
                .filter(|g| {
                    self.probes
                        .iter()
                        .zip(&phase.frontiers)
                        .all(|(p, f)| p.graph != *g || f.is_empty())
                })
                .collect();
        }
    }

    /// The model of a per-(time, key) totals probe: one `[key, sum, count]`
    /// per key and time, over every input of its graph.
    pub(crate) fn model_totals(&mut self, probe_name: &str) {
        let mut sums: BTreeMap<(usize, Time, u64), (i64, i64)> = BTreeMap::new();
        for phase in &self.phases {
            for a in &phase.actions {
                if let Action::Send {
                    input,
                    time,
                    records,
                    ..
                } = a
                {
                    let probe = self.probe_index(self.inputs[*input].graph, probe_name);
                    for (k, v) in records {
                        let e = sums.entry((probe, time.clone(), *k)).or_default();
                        e.0 += v;
                        e.1 += 1;
                    }
                }
            }
        }
        for ((probe, time, k), (sum, count)) in sums {
            *self
                .model
                .entry((probe, time, vec![k as i64, sum, count]))
                .or_default() += 1;
            if !self.modelled.contains(&probe) {
                self.modelled.push(probe);
            }
        }
    }
}

/// SplitMix64: a small deterministic generator, no dependency.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
    /// Skewed toward worker 0.
    pub fn worker(&mut self, workers: usize) -> usize {
        if workers == 1 || self.below(2) == 0 {
            0
        } else {
            self.below(workers as u64) as usize
        }
    }
    /// `n` records with keys 0..8 (skewed toward key 0) and values -8..=8.
    pub fn records(&mut self, n: u64) -> Vec<Rec> {
        (0..n)
            .map(|_| {
                let key = if self.below(4) == 0 { 0 } else { self.below(8) };
                (key, self.below(17) as i64 - 8)
            })
            .collect()
    }
}

/// Split `records` into batches of 1 to 4, each from a skewed worker.
pub(crate) fn sends(
    rng: &mut Rng,
    workers: usize,
    input: usize,
    time: &[u64],
    records: Vec<Rec>,
    out: &mut Vec<Action>,
) {
    let mut rest = &records[..];
    while !rest.is_empty() {
        let n = (1 + rng.below(4) as usize).min(rest.len());
        out.push(Action::Send {
            input,
            worker: rng.worker(workers),
            time: time.to_vec(),
            records: rest[..n].to_vec(),
        });
        rest = &rest[n..];
    }
}

pub(crate) fn advance_all(inputs: &[usize], time: &[u64]) -> Vec<Action> {
    inputs
        .iter()
        .map(|i| Action::Advance {
            input: *i,
            time: time.to_vec(),
        })
        .collect()
}

/// Exchanged two-input totals: a binary operator retains per-key sums and
/// counts per time until both inputs have passed the time.
pub fn totals_scenario(seed: u64, workers: usize, capacity: usize) -> Scenario {
    let mut rng = Rng::new(seed ^ 0x746f_7461_6c73);
    let (left, right) = (0, 1);
    let t0 = vec![0];
    let t1 = vec![1];
    let mut phases = Vec::new();
    let mut phase = |name: &str, actions: Vec<Action>, f: Frontier| {
        phases.push(Phase {
            name: name.into(),
            actions,
            frontiers: vec![f],
            receipts: Vec::new(),
            completed: Vec::new(),
        })
    };

    // Held at 0: real records on the left, possibly none on the right.
    let n = 1 + rng.below(12);
    let left_a = rng.records(n);
    let n = rng.below(13);
    let right_a = rng.records(n);
    let mut a = Vec::new();
    sends(&mut rng, workers, left, &t0, left_a.clone(), &mut a);
    sends(&mut rng, workers, right, &t0, right_a, &mut a);
    phase("held-0", a, vec![t0.clone()]);

    // Same time, same key: changes an existing total before release.
    let mut b_left = vec![(left_a[0].0, rng.below(17) as i64 - 8)];
    let n = rng.below(7);
    b_left.extend(rng.records(n));
    let n = rng.below(7);
    let b_right = rng.records(n);
    let mut b = Vec::new();
    sends(&mut rng, workers, left, &t0, b_left, &mut b);
    sends(&mut rng, workers, right, &t0, b_right, &mut b);
    phase("held-0-extended", b, vec![t0.clone()]);

    // The left input finishes; the right still holds 0 and sends ahead.
    let mut c = advance_all(&[left], &t1);
    c.push(Action::Close { input: left });
    let n = rng.below(7);
    let c_right0 = rng.records(n);
    sends(&mut rng, workers, right, &t0, c_right0, &mut c);
    let n = 1 + rng.below(8);
    let c_right1 = rng.records(n);
    sends(&mut rng, workers, right, &t1, c_right1.clone(), &mut c);
    phase("left-closed-right-holds-0", c, vec![t0.clone()]);

    // Time 0 completes; time 1 is held by the right input.
    phase(
        "released-0-held-1",
        advance_all(&[right], &t1),
        vec![t1.clone()],
    );

    let mut e = Vec::new();
    let mut e_right = vec![(c_right1[0].0, rng.below(17) as i64 - 8)];
    let n = rng.below(7);
    e_right.extend(rng.records(n));
    sends(&mut rng, workers, right, &t1, e_right, &mut e);
    phase("held-1-extended", e, vec![t1.clone()]);

    phase("closed", vec![Action::Close { input: right }], vec![]);

    let mut sc = Scenario {
        recipe: "two-input-totals",
        seed,
        workers,
        capacity,
        graphs: 1,
        outer_iterations: 0,
        inputs: vec![
            InputSpec {
                graph: 0,
                name: "left".into(),
            },
            InputSpec {
                graph: 0,
                name: "right".into(),
            },
        ],
        probes: vec![ProbeSpec {
            graph: 0,
            name: "totals".into(),
            gated: true,
        }],
        phases,
        model: ModelSet::new(),
        modelled: Vec::new(),
        held: [t0, t1]
            .into_iter()
            .map(|time| HeldSpec {
                graph: 0,
                probe: 0,
                time,
            })
            .collect(),
        instancy: InstancyConfig::default(),
        program: None,
    };
    sc.fill_record_receipts();
    sc.model_totals("totals");
    sc
}

/// Product-time notifications: two inputs whose frontiers become the
/// incomparable cut `{(0,2), (2,0)}`, which completes `(1,1)` only.
pub fn product_scenario(seed: u64, workers: usize, capacity: usize) -> Scenario {
    let mut rng = Rng::new(seed ^ 0x7072_6f64);
    let (a, b) = (0, 1);
    let t11 = vec![1, 1];
    let t02 = vec![0, 2];
    let t20 = vec![2, 0];
    let t22 = vec![2, 2];
    let mut phases = Vec::new();

    // Both inputs hold (0,0); records at three times, from either input.
    let mut p0 = Vec::new();
    let mut witnesses = Vec::new();
    for t in [&t11, &t02, &t20] {
        let n = 1 + rng.below(8);
        let recs = rng.records(n);
        witnesses.push(recs[0].0);
        for r in recs {
            let input = if rng.below(2) == 0 { a } else { b };
            sends(&mut rng, workers, input, t, vec![r], &mut p0);
        }
    }
    phases.push(("held-origin", p0, vec![vec![0, 0]]));

    let mut p1 = advance_all(&[a], &t02);
    p1.extend(advance_all(&[b], &t20));
    phases.push(("incomparable-cut", p1, vec![t02.clone(), t20.clone()]));

    // More records at each still-held time, through its supporting input.
    let mut p2 = Vec::new();
    for (input, t, w) in [(a, &t02, witnesses[1]), (b, &t20, witnesses[2])] {
        let mut recs = vec![(w, rng.below(17) as i64 - 8)];
        let n = rng.below(6);
        recs.extend(rng.records(n));
        sends(&mut rng, workers, input, t, recs, &mut p2);
    }
    phases.push((
        "incomparable-cut-extended",
        p2,
        vec![t02.clone(), t20.clone()],
    ));

    phases.push(("joined-cut", advance_all(&[a, b], &t22), vec![t22.clone()]));
    phases.push((
        "closed",
        vec![Action::Close { input: a }, Action::Close { input: b }],
        vec![],
    ));

    let mut sc = Scenario {
        recipe: "product-notifications",
        seed,
        workers,
        capacity,
        graphs: 1,
        outer_iterations: 0,
        inputs: vec![
            InputSpec {
                graph: 0,
                name: "a".into(),
            },
            InputSpec {
                graph: 0,
                name: "b".into(),
            },
        ],
        probes: vec![ProbeSpec {
            graph: 0,
            name: "totals".into(),
            gated: true,
        }],
        phases: phases
            .into_iter()
            .map(|(name, actions, f)| Phase {
                name: name.into(),
                actions,
                frontiers: vec![f],
                receipts: Vec::new(),
                completed: Vec::new(),
            })
            .collect(),
        model: ModelSet::new(),
        modelled: Vec::new(),
        held: [t02, t20]
            .into_iter()
            .map(|time| HeldSpec {
                graph: 0,
                probe: 0,
                time,
            })
            .collect(),
        instancy: InstancyConfig::default(),
        program: None,
    };
    sc.fill_record_receipts();
    sc.model_totals("totals");
    sc
}

/// Nested feedback with notification: a per-time notified aggregate, with
/// exchange, inside an inner finite loop (two inner iterations) on the
/// feedback path of an outer loop (`outer_iterations` iterations), then a
/// notified root aggregate. With `graphs > 1` the copies share one worker
/// set: graph 0 stays held while its siblings advance and the last sibling
/// completes, then graph 0 is extended and released.
pub fn nested_scenario(seed: u64, workers: usize, capacity: usize, graphs: usize) -> Scenario {
    let mut rng = Rng::new(seed ^ 0x6e65_7374 ^ ((graphs as u64) << 32));
    let outer_iterations = 1 + rng.below(3);
    let mut inputs = Vec::new();
    let mut probes = Vec::new();
    for g in 0..graphs {
        inputs.push(InputSpec {
            graph: g,
            name: "in".into(),
        });
        for name in ["inner-totals", "root-totals"] {
            probes.push(ProbeSpec {
                graph: g,
                name: name.into(),
                gated: true,
            });
        }
    }
    // Per graph: `Some(e)` while held at epoch `e`, `None` once closed.
    let cut = |state: &[Option<u64>]| -> Vec<Frontier> {
        state
            .iter()
            .flat_map(|s| match s {
                Some(e) => [vec![vec![*e, 0, 0]], vec![vec![*e]]],
                None => [vec![], vec![]],
            })
            .collect()
    };
    let mut phases = Vec::new();
    let mut held = Vec::new();
    let mut state = vec![Some(0u64); graphs];
    let mut first_keys = vec![[0u64; 2]; graphs];

    // Every graph holds real records at epoch 0.
    let mut p = Vec::new();
    for (g, keys) in first_keys.iter_mut().enumerate() {
        let n = 1 + rng.below(10);
        let recs = rng.records(n);
        keys[0] = recs[0].0;
        sends(&mut rng, workers, g, &[0], recs, &mut p);
        held.push(HeldSpec {
            graph: g,
            probe: 2 * g + 1,
            time: vec![0],
        });
    }
    phases.push(("held-0", p, cut(&state)));

    // Same time, same key, before any release.
    let mut p = Vec::new();
    for (g, keys) in first_keys.iter().enumerate() {
        let mut recs = vec![(keys[0], rng.below(17) as i64 - 8)];
        let n = rng.below(4);
        recs.extend(rng.records(n));
        sends(&mut rng, workers, g, &[0], recs, &mut p);
    }
    phases.push(("held-0-extended", p, cut(&state)));

    // Siblings send epoch 1 and release epoch 0; the last sibling completes.
    // Graph 0 alone stays held at epoch 0.
    let mut p = Vec::new();
    for g in 1..graphs {
        if g + 1 == graphs {
            p.push(Action::Close { input: g });
            state[g] = None;
        } else {
            let n = 1 + rng.below(8);
            let recs = rng.records(n);
            first_keys[g][1] = recs[0].0;
            sends(&mut rng, workers, g, &[1], recs, &mut p);
            p.extend(advance_all(&[g], &[1]));
            state[g] = Some(1);
            held.push(HeldSpec {
                graph: g,
                probe: 2 * g + 1,
                time: vec![1],
            });
        }
    }
    if graphs == 1 {
        let n = 1 + rng.below(8);
        let recs = rng.records(n);
        first_keys[0][1] = recs[0].0;
        sends(&mut rng, workers, 0, &[1], recs, &mut p);
        p.extend(advance_all(&[0], &[1]));
        state[0] = Some(1);
        held.push(HeldSpec {
            graph: 0,
            probe: 1,
            time: vec![1],
        });
    }
    phases.push((
        if graphs == 1 {
            "released-0-held-1"
        } else {
            "siblings-advance"
        },
        p,
        cut(&state),
    ));

    // More records at every still-held time with records, then release
    // graph 0.
    let mut p = Vec::new();
    for g in 0..graphs {
        if let Some(e) = state[g] {
            let key = first_keys[g][e as usize];
            let mut recs = vec![(key, rng.below(17) as i64 - 8)];
            let n = rng.below(4);
            recs.extend(rng.records(n));
            sends(&mut rng, workers, g, &[e], recs, &mut p);
        }
    }
    phases.push(("held-extended", p, cut(&state)));
    if graphs > 1 {
        state[0] = Some(1);
        phases.push(("released", advance_all(&[0], &[1]), cut(&state)));
    }

    let closes = (0..graphs)
        .filter(|g| state[*g].is_some())
        .map(|g| Action::Close { input: g })
        .collect();
    state.iter_mut().for_each(|s| *s = None);
    phases.push(("closed", closes, cut(&state)));

    let mut sc = Scenario {
        recipe: if graphs == 1 {
            "nested-feedback-notify"
        } else {
            "multi-dataflow-nested"
        },
        seed,
        workers,
        capacity,
        graphs,
        outer_iterations,
        inputs,
        probes,
        phases: phases
            .into_iter()
            .map(|(name, actions, frontiers)| Phase {
                name: name.into(),
                actions,
                frontiers,
                receipts: Vec::new(),
                completed: Vec::new(),
            })
            .collect(),
        model: ModelSet::new(),
        modelled: Vec::new(),
        held,
        instancy: InstancyConfig::default(),
        program: None,
    };
    sc.fill_record_receipts();
    sc.model_nested();
    sc
}

impl Scenario {
    /// Inner totals at `[e, i, j]`: the epoch's per-key sum and count at
    /// `[e, 0, 0]`, then one record per key (count 1) at every later inner
    /// and outer iteration; root totals at `[e]`, count 1.
    pub(crate) fn model_nested(&mut self) {
        let mut sums: BTreeMap<(usize, u64, u64), (i64, i64)> = BTreeMap::new();
        for phase in &self.phases {
            for a in &phase.actions {
                if let Action::Send {
                    input,
                    time,
                    records,
                    ..
                } = a
                {
                    for (k, v) in records {
                        let e = sums
                            .entry((self.inputs[*input].graph, time[0], *k))
                            .or_default();
                        e.0 += v;
                        e.1 += 1;
                    }
                }
            }
        }
        for ((g, e, k), (sum, count)) in sums {
            let inner = self.probe_index(g, "inner-totals");
            let root = self.probe_index(g, "root-totals");
            for i in 0..self.outer_iterations {
                for j in 0..2 {
                    let c = if i == 0 && j == 0 { count } else { 1 };
                    *self
                        .model
                        .entry((inner, vec![e, i, j], vec![k as i64, sum, c]))
                        .or_default() += 1;
                }
            }
            *self
                .model
                .entry((root, vec![e], vec![k as i64, sum, 1]))
                .or_default() += 1;
        }
        self.modelled = (0..self.probes.len()).collect();
    }
}

// ---------------------------------------------------------------------------
// Observation choice for builders
// ---------------------------------------------------------------------------

/// What a builder attaches at its observation points.
#[derive(Clone)]
pub enum Observe {
    /// Correctness: capture every record with its worker, and count receipts.
    Capture,
    /// Count records only; no capture, no receipts.
    Count(Arc<AtomicU64>),
    /// Discard records; probes only.
    Discard,
}

// ---------------------------------------------------------------------------
// Checkpoint protocol
// ---------------------------------------------------------------------------

/// One engine running one scenario's graphs.
pub trait Engine {
    fn name(&self) -> &'static str;
    fn apply(&mut self, action: &Action) -> Result<(), String>;
    /// The global view of a probe, or `None` if not every worker has
    /// reported one yet.
    fn frontier(&mut self, probe: usize) -> Result<Option<Frontier>, String>;
    fn receipts(&self, graph: usize) -> u64;
    /// Whether `graph` has completed on every worker. Only asked once the
    /// schedule has closed all of its inputs.
    fn graph_completed(&mut self, graph: usize) -> Result<bool, String>;
    fn drain(&mut self, into: &mut Vec<Obs>) -> Result<(), String>;
    /// Normal completion: every graph and worker finished successfully.
    fn finish(self: Box<Self>) -> Result<(), String>;
    /// Failure-only: skip destructors that could block on live engines.
    fn abandon(self: Box<Self>);
}

fn fail(sc: &Scenario, engine: &str, phase: &str, msg: String) -> String {
    format!(
        "[{engine}] {} phase {phase}: {msg}\n{}",
        sc.label(),
        sc.describe()
    )
}

/// Run every phase, checking each prescribed cut. On error the engine is
/// abandoned, never joined.
pub fn run_engine(mut engine: Box<dyn Engine>, sc: &Scenario) -> Result<Vec<Multiset>, String> {
    match drive(&mut *engine, sc, CHECKPOINT) {
        Ok(snapshots) => {
            let name = engine.name();
            engine.finish().map_err(|e| fail(sc, name, "finish", e))?;
            Ok(snapshots)
        }
        Err(e) => {
            engine.abandon();
            Err(e)
        }
    }
}

fn absorb(
    engine: &mut dyn Engine,
    sc: &Scenario,
    phase: &str,
    seen: &mut Multiset,
    snapshot_cut: Option<&Vec<Frontier>>,
) -> Result<(), String> {
    let mut fresh = Vec::new();
    engine
        .drain(&mut fresh)
        .map_err(|e| fail(sc, engine.name(), phase, e))?;
    for obs in fresh {
        if let Some(cut) = snapshot_cut
            && is_complete(&cut[obs.probe], &obs.time)
        {
            return Err(fail(
                sc,
                engine.name(),
                phase,
                format!(
                    "record {obs:?} at {} arrived in a region completed by the earlier snapshot cut {:?}",
                    sc.probe_name(obs.probe),
                    cut[obs.probe]
                ),
            ));
        }
        *seen.entry(obs).or_default() += 1;
    }
    Ok(())
}

fn drive(
    engine: &mut dyn Engine,
    sc: &Scenario,
    checkpoint: Duration,
) -> Result<Vec<Multiset>, String> {
    let name = engine.name();
    let mut seen = Multiset::new();
    let mut snapshot_cut: Option<Vec<Frontier>> = None;
    let mut snapshots = Vec::new();
    for phase in &sc.phases {
        let pname = phase.name.as_str();
        for f in &phase.frontiers {
            assert!(
                is_antichain(f),
                "fixture frontier {f:?} is not an antichain"
            );
        }
        for a in &phase.actions {
            engine
                .apply(a)
                .map_err(|e| fail(sc, name, pname, format!("{a:?}: {e}")))?;
        }
        let deadline = Instant::now() + checkpoint;
        loop {
            absorb(engine, sc, pname, &mut seen, snapshot_cut.as_ref())?;
            let mut ready = true;
            for g in 0..sc.graphs {
                let got = engine.receipts(g);
                let want = phase.receipts[g];
                if got > want {
                    return Err(fail(
                        sc,
                        name,
                        pname,
                        format!("graph {g} receipts {got} exceed {want}"),
                    ));
                }
                ready &= got == want;
            }
            let mut uncompleted = Vec::new();
            for g in &phase.completed {
                if !engine
                    .graph_completed(*g)
                    .map_err(|e| fail(sc, name, pname, format!("graph {g}: {e}")))?
                {
                    uncompleted.push(*g);
                }
            }
            ready &= uncompleted.is_empty();
            let mut observed = Vec::new();
            for (p, target) in phase.frontiers.iter().enumerate() {
                match engine.frontier(p).map_err(|e| fail(sc, name, pname, e))? {
                    None => ready = false,
                    Some(f) => {
                        if !is_antichain(&f) {
                            return Err(fail(
                                sc,
                                name,
                                pname,
                                format!("{} frontier {f:?} is not an antichain", sc.probe_name(p)),
                            ));
                        }
                        if !frontier_le(&f, target) {
                            return Err(fail(
                                sc,
                                name,
                                pname,
                                format!(
                                    "{} frontier {f:?} passed or is incomparable to target {target:?}",
                                    sc.probe_name(p)
                                ),
                            ));
                        }
                        ready &= canonical(&f) == canonical(target);
                        observed.push(f);
                    }
                }
            }
            if ready {
                break;
            }
            if Instant::now() > deadline {
                let receipts: Vec<u64> = (0..sc.graphs).map(|g| engine.receipts(g)).collect();
                return Err(fail(
                    sc,
                    name,
                    pname,
                    format!(
                        "did not reach the cut within {checkpoint:?}: frontiers {observed:?}, receipts {receipts:?}, required graphs not completed {uncompleted:?}"
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        // Completed-prefix records are available once the cut is read.
        absorb(engine, sc, pname, &mut seen, snapshot_cut.as_ref())?;
        for obs in seen.keys() {
            if sc.probes[obs.probe].gated && !is_complete(&phase.frontiers[obs.probe], &obs.time) {
                return Err(fail(
                    sc,
                    name,
                    pname,
                    format!(
                        "premature output {obs:?} at unfinished time under {} F={:?}",
                        sc.probe_name(obs.probe),
                        phase.frontiers[obs.probe]
                    ),
                ));
            }
        }
        check_model(sc, &seen, &phase.frontiers).map_err(|e| fail(sc, name, pname, e))?;
        snapshots.push(seen.clone());
        snapshot_cut = Some(phase.frontiers.clone());
    }
    Ok(snapshots)
}

/// The completed region of the modelled probes, without workers, must equal
/// the model exactly: one full total per key and time.
pub fn check_model(sc: &Scenario, seen: &Multiset, frontiers: &[Frontier]) -> Result<(), String> {
    let mut got = ModelSet::new();
    for (o, n) in completed_region(seen, frontiers) {
        if sc.modelled.contains(&o.probe) {
            *got.entry((o.probe, o.time, o.payload)).or_default() += n;
        }
    }
    let want: ModelSet = sc
        .model
        .iter()
        .filter(|((p, t, _), _)| is_complete(&frontiers[*p], t))
        .map(|(k, n)| (k.clone(), *n))
        .collect();
    match multiset_diff(&got, &want) {
        None => Ok(()),
        Some(d) => Err(format!(
            "completed region differs from the model (got vs want):\n{d}"
        )),
    }
}

/// Compare the two engines' completed regions at every phase.
pub fn compare(
    sc: &Scenario,
    reference: &[Multiset],
    candidate: &[Multiset],
) -> Result<(), String> {
    if reference.len() != candidate.len() {
        return Err(format!(
            "{}: {} vs {} snapshots",
            sc.label(),
            reference.len(),
            candidate.len()
        ));
    }
    for (k, phase) in sc.phases.iter().enumerate() {
        let r = completed_region(&reference[k], &phase.frontiers);
        let c = completed_region(&candidate[k], &phase.frontiers);
        if let Some(d) = multiset_diff(&r, &c) {
            return Err(fail(
                sc,
                "timely vs instancy",
                &phase.name,
                format!("completed regions differ (timely vs instancy):\n{d}"),
            ));
        }
    }
    Ok(())
}

/// Run one scenario on both engines and compare; panics on the first error.
///
/// This is a reference-first full replay: timely runs every phase, freezing
/// and checking each prescribed cut, then instancy does the same
/// independently; the retained per-phase snapshots are compared afterwards.
/// It is not a simultaneous cross-engine quiescence check.
pub fn differential(
    sc: &Scenario,
    timely: impl FnOnce(&Scenario) -> Box<dyn Engine>,
    instancy: impl FnOnce(&Scenario) -> Box<dyn Engine>,
) {
    let timing = std::env::var_os("DIFF_TIMING").is_some();
    let started = Instant::now();
    let reference = match run_engine(timely(sc), sc) {
        Ok(s) => s,
        Err(e) => panic!("reference/fixture failure:\n{e}"),
    };
    let between = Instant::now();
    let candidate = match run_engine(instancy(sc), sc) {
        Ok(s) => s,
        Err(e) => panic!("instancy failure:\n{e}"),
    };
    if timing {
        // Diagnostic wall-clock only, including checkpoint polling; not a
        // performance measurement.
        eprintln!(
            "timing {}: phases={} timely={:?} instancy={:?}",
            sc.label(),
            sc.phases.len(),
            between - started,
            between.elapsed()
        );
    }
    if let Err(e) = compare(sc, &reference, &candidate) {
        panic!("mismatch:\n{e}");
    }
}

/// Debug instancy builds include the progress checker; require it to panic.
pub fn require_progress_check_panic() {
    assert_eq!(
        std::env::var("INSTANCY_PROGRESS_CHECK").as_deref(),
        Ok("panic"),
        "run with INSTANCY_PROGRESS_CHECK=panic"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(probe: usize, worker: usize, time: &[u64], payload: &[i64]) -> Obs {
        Obs {
            probe,
            worker,
            time: time.to_vec(),
            payload: payload.to_vec(),
        }
    }

    fn set(items: &[Obs]) -> Multiset {
        let mut s = Multiset::new();
        for o in items {
            *s.entry(o.clone()).or_default() += 1;
        }
        s
    }

    fn one_phase(frontier: Frontier) -> Scenario {
        let mut sc = totals_scenario(1, 2, 1);
        sc.phases.truncate(1);
        sc.phases[0].frontiers = vec![frontier];
        sc
    }

    #[test]
    fn product_order_is_componentwise_not_lexicographic() {
        assert!(time_le(&[0, 2], &[1, 2]));
        assert!(!time_le(&[0, 2], &[1, 1]));
        assert!(!time_le(&[2, 0], &[1, 1]));
        assert!(is_complete(&[vec![0, 2], vec![2, 0]], &[1, 1]));
        assert!(!is_complete(&[vec![0, 2], vec![2, 0]], &[0, 3]));
        assert!(is_antichain(&[vec![0, 2], vec![2, 0]]));
        assert!(!is_antichain(&[vec![0, 2], vec![1, 2]]));
        assert!(!is_antichain(&[vec![1], vec![1]]));
        assert_eq!(
            meet(&[vec![vec![0, 2]], vec![vec![2, 0]], vec![vec![2, 2]]]),
            vec![vec![0, 2], vec![2, 0]]
        );
    }

    #[test]
    fn a_late_or_incomparable_frontier_is_not_below_the_target() {
        let target = vec![vec![0, 2], vec![2, 0]];
        assert!(frontier_le(&[vec![0, 0]], &target));
        assert!(frontier_le(&target, &target));
        assert!(!frontier_le(&[vec![0, 3], vec![2, 0]], &target));
        assert!(!frontier_le(&[vec![1, 1]], &target));
        assert!(!frontier_le(&[], &target));
        assert!(frontier_le(&[vec![5]], &[]));
    }

    #[test]
    fn wrong_values_duplicates_workers_and_graphs_are_differences() {
        let base = set(&[obs(0, 1, &[0], &[3, 5, 2])]);
        assert!(multiset_diff(&base, &base).is_none());
        for wrong in [
            set(&[obs(0, 1, &[0], &[3, 6, 2])]),
            set(&[obs(0, 1, &[0], &[3, 5, 2]), obs(0, 1, &[0], &[3, 5, 2])]),
            set(&[obs(0, 0, &[0], &[3, 5, 2])]),
            set(&[obs(1, 1, &[0], &[3, 5, 2])]),
            set(&[]),
        ] {
            assert!(multiset_diff(&base, &wrong).is_some(), "{wrong:?}");
        }
    }

    #[test]
    fn completed_regions_use_the_product_order() {
        let all = set(&[
            obs(0, 0, &[1, 1], &[1]),
            obs(0, 0, &[0, 2], &[2]),
            obs(0, 0, &[2, 0], &[3]),
        ]);
        let region = completed_region(&all, &[vec![vec![0, 2], vec![2, 0]]]);
        assert_eq!(region, set(&[obs(0, 0, &[1, 1], &[1])]));
    }

    /// A deliberately wrong engine, configured per test.
    struct Fake {
        frontier: Frontier,
        emitted: Vec<Obs>,
        completed: bool,
    }

    impl Engine for Fake {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn apply(&mut self, _: &Action) -> Result<(), String> {
            Ok(())
        }
        fn frontier(&mut self, _: usize) -> Result<Option<Frontier>, String> {
            Ok(Some(self.frontier.clone()))
        }
        fn receipts(&self, _: usize) -> u64 {
            u64::MAX
        }
        fn graph_completed(&mut self, _: usize) -> Result<bool, String> {
            Ok(self.completed)
        }
        fn drain(&mut self, into: &mut Vec<Obs>) -> Result<(), String> {
            into.append(&mut self.emitted);
            Ok(())
        }
        fn finish(self: Box<Self>) -> Result<(), String> {
            Ok(())
        }
        fn abandon(self: Box<Self>) {}
    }

    /// A deliberately wrong engine emits a partial total at a held time. The
    /// held-cut check rejects it immediately; the separate model test covers
    /// an early partial followed by a plausible final total.
    #[test]
    fn an_early_partial_total_fails_at_the_held_cut() {
        let mut sc = one_phase(vec![vec![0]]);
        sc.phases[0].receipts = vec![u64::MAX];
        let fake = Fake {
            frontier: vec![vec![0]],
            emitted: vec![obs(0, 0, &[0], &[0, 1, 1])],
            completed: false,
        };
        let err = run_engine(Box::new(fake), &sc).unwrap_err();
        assert!(err.contains("premature output"), "{err}");
    }

    /// Empty probes alone do not pass a cut that requires a graph to have
    /// completed: the engine must also report the graph's completion.
    #[test]
    fn empty_probes_without_graph_completion_do_not_pass_the_cut() {
        let mut sc = one_phase(vec![]);
        sc.phases[0].receipts = vec![u64::MAX];
        sc.phases[0].completed = vec![0];
        sc.model.clear();
        sc.modelled.clear();
        let fake = Fake {
            frontier: vec![],
            emitted: Vec::new(),
            completed: false,
        };
        let err = drive(&mut { fake }, &sc, Duration::from_millis(50)).unwrap_err();
        assert!(err.contains("required graphs not completed [0]"), "{err}");
        let mut done = Fake {
            frontier: vec![],
            emitted: Vec::new(),
            completed: true,
        };
        assert!(drive(&mut done, &sc, Duration::from_millis(50)).is_ok());
    }

    #[test]
    fn graphs_with_all_probes_empty_must_have_completed() {
        let sc = nested_scenario(1, 2, 1, 2);
        let advance = sc
            .phases
            .iter()
            .find(|p| p.name == "siblings-advance")
            .unwrap();
        assert_eq!(advance.completed, vec![1]);
        assert_eq!(sc.phases.last().unwrap().completed, vec![0, 1]);
        let sc = nested_scenario(2, 2, 1, 8);
        let advance = sc
            .phases
            .iter()
            .find(|p| p.name == "siblings-advance")
            .unwrap();
        assert_eq!(advance.completed, vec![7]);
    }

    #[test]
    fn the_model_requires_one_full_total_per_key_and_time() {
        let sc = totals_scenario(3, 2, 1);
        let last = sc.phases.last().unwrap();
        let mut good = Multiset::new();
        for ((p, t, payload), n) in &sc.model {
            *good.entry(obs(*p, 0, t, payload)).or_default() += n;
        }
        assert!(check_model(&sc, &good, &last.frontiers).is_ok());
        // A partial total that later became complete is still an extra record.
        let mut partial = good.clone();
        let (k, _) = good.iter().next().unwrap();
        let mut early = k.clone();
        early.payload[2] -= 1;
        *partial.entry(early).or_default() += 1;
        assert!(check_model(&sc, &partial, &last.frontiers).is_err());
        // A missing graph's records are missing totals.
        assert!(check_model(&sc, &Multiset::new(), &last.frontiers).is_err());
    }

    #[test]
    fn scenarios_keep_their_witnesses_and_bounds() {
        for (seed, (w, c)) in SETTINGS.iter().enumerate() {
            let seed = seed as u64 + 1;
            for sc in [
                totals_scenario(seed, *w, *c),
                product_scenario(seed, *w, *c),
                nested_scenario(seed, *w, *c, 1),
                nested_scenario(seed, *w, *c, 2),
                nested_scenario(seed, *w, *c, 8),
            ] {
                let mut per_input = vec![0usize; sc.inputs.len()];
                for phase in &sc.phases {
                    assert_eq!(phase.frontiers.len(), sc.probes.len());
                    for f in &phase.frontiers {
                        assert!(is_antichain(f));
                    }
                    for a in &phase.actions {
                        if let Action::Send {
                            input,
                            worker,
                            records,
                            ..
                        } = a
                        {
                            assert!(*worker < *w);
                            assert!(!records.is_empty());
                            per_input[*input] += records.len();
                            for (k, v) in records {
                                assert!(*k < 8 && (-8..=8).contains(v));
                            }
                        }
                    }
                }
                assert!(per_input.iter().all(|n| *n <= 64), "{per_input:?}");
                assert!((1..=3).contains(&sc.outer_iterations) || sc.outer_iterations == 0);
                assert!(!sc.held.is_empty(), "{}", sc.label());
                sc.check_witnesses().unwrap();
                let last = sc.phases.last().unwrap();
                assert!(last.frontiers.iter().all(|f| f.is_empty()));
            }
        }
    }

    #[test]
    fn a_held_time_without_a_same_key_extension_is_rejected() {
        let mut sc = totals_scenario(1, 2, 1);
        assert!(sc.check_witnesses().is_ok());
        // Only the first held cut and the final close: no later addition.
        let last = sc.phases.pop().unwrap();
        sc.phases.truncate(1);
        sc.phases.push(last);
        assert!(sc.check_witnesses().is_err());
    }
}
