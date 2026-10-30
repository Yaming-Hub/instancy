//! Streaming schedules for the operator-breadth, scaled and random-program
//! scenarios: many epochs, periodic held cuts with same-key extensions,
//! occasional future-time sends, and several graphs in lockstep.

use std::collections::{BTreeMap, BTreeSet};

use crate::programs::{Program, random_program};
use crate::{
    Action, Frontier, HeldSpec, InputSpec, InstancyConfig, ModelSet, Phase, ProbeSpec, Rec, Rng,
    Scenario, Time, advance_all, sends,
};

/// The size of a streaming schedule.
#[derive(Clone, Copy, Debug)]
pub struct StreamShape {
    pub epochs: u64,
    /// Records per graph per epoch, at least one.
    pub max_per_epoch: u64,
    /// Every this many epochs, hold the epoch and extend it before release.
    pub extend_every: u64,
}

/// The small shape used by the per-commit tests.
pub const SMALL: StreamShape = StreamShape {
    epochs: 4,
    max_per_epoch: 12,
    extend_every: 2,
};

/// The probes of one graph: name, whether output is completion-gated, and
/// its prescribed cut while the graph's input is held at epoch `e`.
pub type ProbeCut = (&'static str, bool, fn(u64) -> Time);

pub const BREADTH_PROBES: [ProbeCut; 5] = [
    ("broadcast-totals", true, root_cut),
    ("distinct", true, root_cut),
    ("delayed-reduce", true, root_cut),
    ("gathered-count", true, root_cut),
    ("rebalanced-totals", true, root_cut),
];

pub const NESTED_PROBES: [ProbeCut; 2] = [
    ("inner-totals", true, |e| vec![e, 0, 0]),
    ("root-totals", true, root_cut),
];

pub const PROGRAM_PROBES: [ProbeCut; 2] = [("pass", false, root_cut), ("totals", true, root_cut)];

fn root_cut(e: u64) -> Time {
    vec![e]
}

/// A streaming schedule over `graphs` graphs, each with one input `in` and
/// the given probes. `witness` names the gated root-time probe used for
/// held-time witnesses.
#[allow(clippy::too_many_arguments)]
pub fn stream_scenario(
    recipe: &'static str,
    seed: u64,
    workers: usize,
    capacity: usize,
    graphs: usize,
    shape: StreamShape,
    probes: &[ProbeCut],
    witness: &str,
) -> Scenario {
    let mut rng = Rng::new(seed ^ 0x7374_7265_616d ^ ((graphs as u64) << 40));
    let mut sc = Scenario {
        recipe,
        seed,
        workers,
        capacity,
        graphs,
        outer_iterations: 0,
        inputs: (0..graphs)
            .map(|graph| InputSpec {
                graph,
                name: "in".into(),
            })
            .collect(),
        probes: (0..graphs)
            .flat_map(|graph| {
                probes.iter().map(move |(name, gated, _)| ProbeSpec {
                    graph,
                    name: (*name).into(),
                    gated: *gated,
                })
            })
            .collect(),
        phases: Vec::new(),
        model: ModelSet::new(),
        modelled: Vec::new(),
        held: Vec::new(),
        instancy: InstancyConfig::default(),
        program: None,
    };
    let cut = |e: Option<u64>| -> Vec<Frontier> {
        (0..graphs)
            .flat_map(|_| {
                probes.iter().map(move |(_, _, at)| match e {
                    Some(e) => vec![at(e)],
                    None => vec![],
                })
            })
            .collect()
    };
    let mut phase = |name: String, actions: Vec<Action>, f: Vec<Frontier>| {
        sc.phases.push(Phase {
            name,
            actions,
            frontiers: f,
            receipts: Vec::new(),
            completed: Vec::new(),
        })
    };
    let inputs: Vec<usize> = (0..graphs).collect();
    let mut held = Vec::new();
    for e in 0..shape.epochs {
        let extend = e % shape.extend_every == 0;
        let mut actions = Vec::new();
        let mut first_keys = Vec::new();
        for g in 0..graphs {
            let n = 1 + rng.below(shape.max_per_epoch);
            let recs = rng.records(n);
            first_keys.push(recs[0].0);
            sends(&mut rng, workers, g, &[e], recs, &mut actions);
            if rng.below(4) == 0 {
                let n = 1 + rng.below(3);
                let ahead = rng.records(n);
                sends(&mut rng, workers, g, &[e + 1], ahead, &mut actions);
            }
        }
        if extend {
            phase(format!("held-{e}"), actions, cut(Some(e)));
            let mut actions = Vec::new();
            for (g, key) in first_keys.iter().enumerate() {
                let mut recs = vec![(*key, rng.below(17) as i64 - 8)];
                let n = rng.below(3);
                recs.extend(rng.records(n));
                sends(&mut rng, workers, g, &[e], recs, &mut actions);
                held.push((g, e));
            }
            phase(format!("held-{e}-extended"), actions, cut(Some(e)));
            phase(
                format!("released-{e}"),
                advance_all(&inputs, &[e + 1]),
                cut(Some(e + 1)),
            );
        } else {
            actions.extend(advance_all(&inputs, &[e + 1]));
            phase(format!("epoch-{e}"), actions, cut(Some(e + 1)));
        }
    }
    let closes = inputs.iter().map(|i| Action::Close { input: *i }).collect();
    phase("closed".into(), closes, cut(None));
    sc.held = held
        .into_iter()
        .map(|(graph, e)| HeldSpec {
            graph,
            probe: sc.probe_index(graph, witness),
            time: vec![e],
        })
        .collect();
    sc.fill_record_receipts();
    sc
}

/// Every record sent to each graph, by `(graph, epoch)`.
fn sent(sc: &Scenario) -> BTreeMap<(usize, u64), Vec<Rec>> {
    let mut out: BTreeMap<(usize, u64), Vec<Rec>> = BTreeMap::new();
    for phase in &sc.phases {
        for a in &phase.actions {
            if let Action::Send {
                input,
                time,
                records,
                ..
            } = a
            {
                out.entry((sc.inputs[*input].graph, time[0]))
                    .or_default()
                    .extend(records);
            }
        }
    }
    out
}

/// Operator breadth: broadcast, branch and merge, flat_map, distinct,
/// per-record delay, reduce, gather, count and rebalance (see the
/// backends' `build_breadth`).
pub fn breadth_scenario(
    seed: u64,
    workers: usize,
    capacity: usize,
    graphs: usize,
    shape: StreamShape,
) -> Scenario {
    let mut sc = stream_scenario(
        "operator-breadth",
        seed,
        workers,
        capacity,
        graphs,
        shape,
        &BREADTH_PROBES,
        "rebalanced-totals",
    );
    model_breadth(&mut sc);
    sc
}

/// The breadth model. Routing uses the explicit key hash `key % workers`,
/// which both engines are given; the model only fixes which records meet.
pub fn model_breadth(sc: &mut Scenario) {
    let w = sc.workers as u64;
    let mut model = ModelSet::new();
    let add = |model: &mut ModelSet, p: usize, t: Time, payload: Vec<i64>, n: usize| {
        *model.entry((p, t, payload)).or_default() += n;
    };
    for ((g, e), records) in sent(sc) {
        let p = |name| sc.probe_index(g, name);
        let mut totals: BTreeMap<u64, (i64, i64, i64)> = BTreeMap::new();
        let mut distinct: BTreeSet<(u64, i64)> = BTreeSet::new();
        for (k, v) in &records {
            let t = totals.entry(*k).or_default();
            t.0 += v;
            t.1 += 1;
            t.2 += 2 * v;
            let a = v.abs();
            distinct.insert((*k, a));
            distinct.insert((*k, a % 3));
        }
        for (k, (sum, count, doubled)) in &totals {
            let k = *k as i64;
            add(
                &mut model,
                p("broadcast-totals"),
                vec![e],
                vec![k, *sum, *count],
                sc.workers,
            );
            add(
                &mut model,
                p("rebalanced-totals"),
                vec![e],
                vec![k, *doubled, *count],
                1,
            );
        }
        for (k, x) in distinct {
            add(&mut model, p("distinct"), vec![e], vec![k as i64, x], 1);
        }
    }
    // Delay moves odd keys one epoch later; reduce is per worker per time.
    let mut delayed: BTreeMap<(usize, u64, u64), (u64, i64)> = BTreeMap::new();
    for ((g, e), records) in sent(sc) {
        for (k, v) in records {
            let t = e + k % 2;
            let r = delayed.entry((g, t, k % w)).or_insert((k, 0));
            r.0 = r.0.max(k);
            r.1 += v;
        }
    }
    let mut workers_per_time: BTreeMap<(usize, u64), i64> = BTreeMap::new();
    for ((g, t, _), (k, sum)) in delayed {
        let p = sc.probe_index(g, "delayed-reduce");
        add(&mut model, p, vec![t], vec![k as i64, sum], 1);
        *workers_per_time.entry((g, t)).or_default() += 1;
    }
    for ((g, t), n) in workers_per_time {
        let p = sc.probe_index(g, "gathered-count");
        add(&mut model, p, vec![t], vec![n], 1);
    }
    sc.model = model;
    sc.modelled = (0..sc.probes.len()).collect();
}

/// The nested recipe on a streaming schedule.
pub fn scaled_nested_scenario(
    seed: u64,
    workers: usize,
    capacity: usize,
    graphs: usize,
    shape: StreamShape,
    outer_iterations: u64,
) -> Scenario {
    let mut sc = stream_scenario(
        "nested-feedback-notify-stream",
        seed,
        workers,
        capacity,
        graphs,
        shape,
        &NESTED_PROBES,
        "root-totals",
    );
    sc.outer_iterations = outer_iterations;
    sc.model_nested();
    sc
}

/// A random program on a small streaming schedule. Outputs are compared
/// between the engines only; there is no independent model of programs.
pub fn program_scenario(
    seed: u64,
    workers: usize,
    capacity: usize,
    shape: StreamShape,
) -> Scenario {
    let mut sc = stream_scenario(
        "random-program",
        seed,
        workers,
        capacity,
        1,
        shape,
        &PROGRAM_PROBES,
        "totals",
    );
    sc.program = Some(random_program(seed));
    sc
}

impl Scenario {
    pub fn program(&self) -> &Program {
        self.program.as_ref().expect("a random-program scenario")
    }

    pub fn with_instancy(mut self, config: InstancyConfig) -> Self {
        self.instancy = config;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SETTINGS;

    #[test]
    fn streaming_scenarios_hold_extend_and_finish() {
        for (i, (w, c)) in SETTINGS.into_iter().enumerate() {
            let seed = i as u64 + 1;
            for sc in [
                breadth_scenario(seed, w, c, 2, SMALL),
                scaled_nested_scenario(seed, w, c, 3, SMALL, 2),
                program_scenario(seed, w, c, SMALL),
            ] {
                sc.check_witnesses().unwrap();
                assert!(!sc.held.is_empty());
                let last = sc.phases.last().unwrap();
                assert!(last.frontiers.iter().all(|f| f.is_empty()));
                assert_eq!(last.completed, (0..sc.graphs).collect::<Vec<_>>());
                for phase in &sc.phases {
                    for f in &phase.frontiers {
                        assert!(crate::is_antichain(f));
                    }
                }
            }
        }
    }

    #[test]
    fn the_breadth_model_counts_broadcast_copies_and_delays_odd_keys() {
        let sc = breadth_scenario(5, 3, 1, 1, SMALL);
        let bcast = sc.probe_index(0, "broadcast-totals");
        let reb = sc.probe_index(0, "rebalanced-totals");
        let copies: usize = sc
            .model
            .iter()
            .filter(|((p, _, _), _)| *p == bcast)
            .map(|(_, n)| *n)
            .sum();
        let keys: usize = sc
            .model
            .iter()
            .filter(|((p, _, _), _)| *p == reb)
            .map(|(_, n)| *n)
            .sum();
        assert_eq!(copies, 3 * keys);
        let delayed = sc.probe_index(0, "delayed-reduce");
        let last = sc
            .model
            .keys()
            .filter(|(p, _, _)| *p == delayed)
            .map(|(_, t, _)| t[0])
            .max()
            .unwrap();
        assert!(
            last <= SMALL.epochs + 1,
            "a delay moves records at most one epoch past the latest (possibly ahead-of-time) send"
        );
    }
}
