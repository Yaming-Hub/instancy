//! The physical plan: which workers run which operators.
//!
//! A [`PhysicalPlan`] is computed once per spawn, from the logical dataflow's
//! graph and stages, the spawn's worker count and its parallelism mode
//! ([`Parallelism`]). It is the only place that decides hosting: the
//! materialization of each worker, the seeding of every progress tracker's
//! initial capabilities, the probe writer counts and the cluster handshake
//! all read it.
//!
//! # Rules
//!
//! - Every stage `s` runs on workers `0..par(s)`, and an operator runs on the
//!   workers of its stage: `hosts(op) = 0..par(stage(op))`. The dataflow has
//!   `total_workers = max_s par(s)` workers.
//! - A hosted operator sees its stage's worker context
//!   `(worker, par(stage(op)))`, so exchange routing and `peers()` are those
//!   of its stage.
//! - An exchange or broadcast edge is built on every worker that hosts
//!   either end, from `par(source)` senders to `par(target)` receivers, with
//!   the dataflow-wide worker context `(worker, total_workers)`.
//! - A pipeline edge stays on its worker, so every worker that hosts its
//!   source must also host its target; otherwise the plan is rejected. A
//!   worker that hosts only the target gets an input that is already
//!   exhausted. A feedback edge (also worker-local) must have both ends on
//!   the same workers: a loop's stages need equal parallelism.
//! - Each operator's declared initial capabilities are seeded
//!   `|hosts(op)|` times into every worker's progress tracker, and a probe on
//!   `op` expects `|hosts(op)|` writers.
//!
//! The plan's [`digest`](PhysicalPlan::digest) is part of the cluster
//! handshake fingerprint, so two nodes can never seed different
//! multiplicities.

use std::collections::BTreeMap;
use std::hash::Hasher;

use crate::dataflow::graph::DataflowGraph;
use crate::dataflow::stage::StageInfo;
use crate::dataflow::stream::Slot;
use crate::error::{Error, Result, RuntimeError};
use crate::hash::StableHasher;
use crate::worker::WorkerContext;

/// How a spawn assigns parallelism to stages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Parallelism {
    /// One worker runs everything; explicit stage parallelism is ignored
    /// (`RuntimeHandle::spawn`, `SimpleRuntime::run` and `spawn`).
    Single,
    /// Every stage runs on all `n` workers. An explicit stage parallelism
    /// must equal `n`.
    Uniform(usize),
    /// A stage with explicit parallelism (`exchange_to`, `gather`,
    /// `rebalance_to`) runs on that many workers, the others on `default`.
    PerStage {
        /// Parallelism of stages without an explicit one.
        default: usize,
    },
}

impl Parallelism {
    /// The parallelism of `RuntimeHandle::spawn_multi(num_workers)` with
    /// `SpawnOptions::auto_parallelism(auto)` and
    /// `SpawnOptions::per_stage_parallelism(per_stage)`, for a dataflow with
    /// `physical_inputs` inputs and async sources.
    ///
    /// With `auto`, stages without explicit parallelism run on
    /// `max(physical_inputs, num_workers, 1)` workers; otherwise, with
    /// `per_stage`, on `num_workers`; otherwise every stage runs on
    /// `num_workers`.
    pub(crate) fn for_spawn_multi(
        num_workers: usize,
        auto: bool,
        per_stage: bool,
        physical_inputs: usize,
    ) -> Self {
        if auto {
            Parallelism::PerStage {
                default: physical_inputs.max(1).max(num_workers),
            }
        } else if per_stage {
            Parallelism::PerStage {
                default: num_workers,
            }
        } else {
            Parallelism::Uniform(num_workers)
        }
    }
}

/// How an edge moves records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EdgeRouting {
    /// Worker-local channel.
    Pipeline,
    /// Worker-local loop back-edge.
    Feedback,
    /// Cross-worker channel (exchange or broadcast).
    Exchange,
}

/// The plan of one edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EdgePlan {
    pub(crate) source: Slot,
    pub(crate) target: Slot,
    pub(crate) routing: EdgeRouting,
    /// Workers `0..source_hosts` run the source operator.
    pub(crate) source_hosts: usize,
    /// Workers `0..target_hosts` run the target operator.
    pub(crate) target_hosts: usize,
    /// Whether both ends are in the same stage.
    pub(crate) same_stage: bool,
}

impl EdgePlan {
    /// Whether `worker` builds this edge (it hosts either end).
    pub(crate) fn built_on(&self, worker: usize) -> bool {
        worker < self.source_hosts.max(self.target_hosts)
    }

    /// Whether `worker` runs the edge's source.
    pub(crate) fn runs_source(&self, worker: usize) -> bool {
        worker < self.source_hosts
    }

    /// Whether `worker` runs the edge's target.
    pub(crate) fn runs_target(&self, worker: usize) -> bool {
        worker < self.target_hosts
    }
}

/// Which stage an operator is in and how many workers run it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OperatorPlan {
    /// Index of the operator's stage, or `None` for an operator outside
    /// every stage (run on every worker).
    stage: Option<usize>,
    /// Workers `0..hosts` run the operator.
    hosts: usize,
}

/// The operator-to-worker assignment of one spawn (see the module docs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalPlan {
    total_workers: usize,
    /// Parallelism of each stage, by stage index.
    stage_parallelism: Vec<usize>,
    operators: BTreeMap<usize, OperatorPlan>,
    /// Regular edges, then feedback edges: the materialization edge order.
    edges: Vec<EdgePlan>,
    digest: u64,
}

impl PhysicalPlan {
    /// Computes the plan of a dataflow with this graph and these stages.
    ///
    /// # Errors
    ///
    /// - a worker count of 0;
    /// - with [`Parallelism::Uniform`], an explicit stage parallelism other
    ///   than the worker count;
    /// - a pipeline edge whose source runs on a worker that does not run its
    ///   target, or a feedback edge whose ends run on different workers.
    pub(crate) fn new(
        graph: &DataflowGraph,
        stages: &[StageInfo],
        parallelism: Parallelism,
    ) -> Result<Self> {
        let invalid = |msg: String| Error::Runtime(RuntimeError::InvalidConfig(msg));
        let stage_parallelism: Vec<usize> = match parallelism {
            Parallelism::Single => vec![1; stages.len()],
            Parallelism::Uniform(n) => {
                if n == 0 {
                    return Err(invalid("num_workers must be >= 1".into()));
                }
                for stage in stages {
                    if let Some(p) = stage.parallelism.filter(|&p| p != n) {
                        return Err(invalid(format!(
                            "stage {} has explicit parallelism {p} but every stage runs on \
                             all {n} workers",
                            stage.id.0
                        )));
                    }
                }
                vec![n; stages.len()]
            }
            Parallelism::PerStage { default } => {
                if default == 0 {
                    return Err(invalid("default_parallelism must be >= 1".into()));
                }
                stages
                    .iter()
                    .map(|stage| match stage.parallelism {
                        Some(0) => Err(invalid(format!(
                            "stage {} has explicit parallelism 0",
                            stage.id.0
                        ))),
                        Some(p) => Ok(p),
                        None => Ok(default),
                    })
                    .collect::<Result<_>>()?
            }
        };
        let total_workers = match parallelism {
            Parallelism::Single => 1,
            Parallelism::Uniform(n) => n,
            Parallelism::PerStage { default } => {
                stage_parallelism.iter().copied().max().unwrap_or(default)
            }
        };

        let mut operators = BTreeMap::new();
        for (stage_idx, stage) in stages.iter().enumerate() {
            for &op in &stage.operator_indices {
                operators.insert(
                    op,
                    OperatorPlan {
                        stage: Some(stage_idx),
                        hosts: stage_parallelism[stage_idx],
                    },
                );
            }
        }
        for op in graph.operators() {
            operators.entry(op.index).or_insert(OperatorPlan {
                stage: None,
                hosts: total_workers,
            });
        }

        let operator = |op: usize| {
            operators.get(&op).copied().unwrap_or(OperatorPlan {
                stage: None,
                hosts: total_workers,
            })
        };
        let name = |op: usize| {
            graph
                .operator(op)
                .map_or_else(|| format!("op{op}"), |info| format!("'{}'", info.name))
        };
        let stage_name = |plan: OperatorPlan| match plan.stage {
            Some(idx) => stages[idx].id.to_string(),
            None => "no stage".to_string(),
        };
        // Feedback edges first: a loop whose stages differ in parallelism is
        // reported as such, not through one of its internal pipeline edges.
        for edge in graph.feedback_edges() {
            let source = operator(edge.source.operator_index);
            let target = operator(edge.target.operator_index);
            if !edge.is_exchange() && source.hosts != target.hosts {
                // Feedback is 1:1 per worker: both ends must run on the same
                // workers.
                return Err(invalid(format!(
                    "cross-stage feedback edge from {} (par={}) to {} (par={}) requires \
                     equal parallelism; feedback is 1:1 per worker",
                    stage_name(source),
                    source.hosts,
                    stage_name(target),
                    target.hosts
                )));
            }
        }
        let regular = graph.edges().iter().map(|e| (e, false));
        let feedback = graph.feedback_edges().iter().map(|e| (e, true));
        let mut edges = Vec::with_capacity(graph.edges().len() + graph.feedback_edges().len());
        for (edge, is_feedback) in regular.chain(feedback) {
            let source = operator(edge.source.operator_index);
            let target = operator(edge.target.operator_index);
            let routing = if edge.is_exchange() {
                EdgeRouting::Exchange
            } else if is_feedback {
                EdgeRouting::Feedback
            } else {
                EdgeRouting::Pipeline
            };
            if routing == EdgeRouting::Pipeline && source.hosts > target.hosts {
                return Err(invalid(format!(
                    "cross-stage pipeline edge from {} (output {}, parallelism {}) to {} \
                     (input {}, parallelism {}) is worker-local, but workers {}..{} run its \
                     source and not its target, so their records would be lost; insert an \
                     exchange (for example `exchange_to`) between them or give both ends the \
                     same parallelism",
                    name(edge.source.operator_index),
                    edge.source.slot_index,
                    source.hosts,
                    name(edge.target.operator_index),
                    edge.target.slot_index,
                    target.hosts,
                    target.hosts,
                    source.hosts,
                )));
            }
            edges.push(EdgePlan {
                source: edge.source,
                target: edge.target,
                routing,
                source_hosts: source.hosts,
                target_hosts: target.hosts,
                same_stage: source.stage == target.stage,
            });
        }

        let mut plan = Self {
            total_workers,
            stage_parallelism,
            operators,
            edges,
            digest: 0,
        };
        plan.digest = plan.compute_digest();
        Ok(plan)
    }

    /// The plan of a single-worker dataflow.
    pub(crate) fn single(graph: &DataflowGraph, stages: &[StageInfo]) -> Result<Self> {
        Self::new(graph, stages, Parallelism::Single)
    }

    /// A plan with no edges whose operators run on `hosts` workers each
    /// (`(op, hosts)` pairs), for tracker-level tests.
    #[cfg(test)]
    pub(crate) fn for_hosts(total_workers: usize, hosts: &[(usize, usize)]) -> Self {
        let mut plan = Self {
            total_workers,
            stage_parallelism: Vec::new(),
            operators: hosts
                .iter()
                .map(|&(op, hosts)| (op, OperatorPlan { stage: None, hosts }))
                .collect(),
            edges: Vec::new(),
            digest: 0,
        };
        plan.digest = plan.compute_digest();
        plan
    }

    /// Number of workers of the dataflow (the largest stage parallelism).
    pub(crate) fn total_workers(&self) -> usize {
        self.total_workers
    }

    /// Whether every operator runs on every worker.
    pub(crate) fn is_uniform(&self) -> bool {
        self.operators
            .values()
            .all(|op| op.hosts == self.total_workers)
    }

    /// Number of workers that run `op` (workers `0..hosts(op)`).
    pub(crate) fn hosts(&self, op: usize) -> usize {
        self.operators
            .get(&op)
            .map_or(self.total_workers, |p| p.hosts)
    }

    /// Whether `worker` runs `op`.
    pub(crate) fn runs(&self, worker: usize, op: usize) -> bool {
        worker < self.hosts(op)
    }

    /// The worker context `op` sees on `worker`: its stage's.
    pub(crate) fn operator_context(&self, worker: usize, op: usize) -> Result<WorkerContext> {
        WorkerContext::new(worker, self.hosts(op))
    }

    /// The worker context the channel of `edge` is built with on `worker`:
    /// its stage's for a pipeline or feedback edge within one stage, the
    /// dataflow's otherwise.
    pub(crate) fn channel_context(&self, worker: usize, edge: &EdgePlan) -> Result<WorkerContext> {
        if edge.routing != EdgeRouting::Exchange && edge.same_stage {
            WorkerContext::new(worker, edge.source_hosts)
        } else {
            WorkerContext::new(worker, self.total_workers)
        }
    }

    /// The edge with materialization index `index` (regular edges, then
    /// feedback edges).
    pub(crate) fn edge(&self, index: usize) -> Option<&EdgePlan> {
        self.edges.get(index)
    }

    /// Every edge, in materialization order.
    #[cfg(test)]
    pub(crate) fn edges(&self) -> &[EdgePlan] {
        &self.edges
    }

    /// How many times every worker's progress tracker seeds each initial
    /// capability `op` declares: once per worker that runs `op`.
    pub(crate) fn seed_multiplicity(&self, op: usize) -> usize {
        self.hosts(op)
    }

    /// How many writers a probe on `op`'s output has: one per worker that
    /// runs `op`.
    pub(crate) fn probe_writers(&self, op: usize) -> usize {
        self.hosts(op)
    }

    /// A canonical digest of the plan, identical on every node that computes
    /// the same plan.
    pub(crate) fn digest(&self) -> u64 {
        self.digest
    }

    fn compute_digest(&self) -> u64 {
        let mut h = StableHasher::new();
        h.write(b"instancy-physical-plan");
        h.write_u64(self.total_workers as u64);
        h.write_u64(self.stage_parallelism.len() as u64);
        for &p in &self.stage_parallelism {
            h.write_u64(p as u64);
        }
        h.write_u64(self.operators.len() as u64);
        for (&op, plan) in &self.operators {
            h.write_u64(op as u64);
            h.write_u64(plan.stage.map_or(0, |s| s as u64 + 1));
            h.write_u64(plan.hosts as u64);
        }
        h.write_u64(self.edges.len() as u64);
        for edge in &self.edges {
            h.write_u64(edge.source.operator_index as u64);
            h.write_u64(edge.source.slot_index as u64);
            h.write_u64(edge.target.operator_index as u64);
            h.write_u64(edge.target.slot_index as u64);
            h.write_u64(match edge.routing {
                EdgeRouting::Pipeline => 0,
                EdgeRouting::Feedback => 1,
                EdgeRouting::Exchange => 2,
            });
            h.write_u64(edge.source_hosts as u64);
            h.write_u64(edge.target_hosts as u64);
            h.write_u64(u64::from(edge.same_stage));
        }
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::{DataflowBuilder, LogicalDataflow};

    /// input -> inc -> exchange_to(par) -> double -> output, or a plain
    /// exchange without `par`.
    fn exchange_shape(par: Option<usize>) -> LogicalDataflow<u64> {
        let b = DataflowBuilder::<u64>::new("plan");
        let input = b.input::<u64>("in").unwrap().map("inc", |_t, x| x + 1);
        let exchanged = match par {
            Some(par) => input.exchange_to("x", par, |x: &u64| *x).unwrap(),
            None => input.exchange("x", |x: &u64| *x),
        };
        exchanged
            .map("double", |_t, x| x * 2)
            .output("out")
            .unwrap();
        b.build().unwrap()
    }

    fn op(df: &LogicalDataflow<u64>, name: &str) -> usize {
        df.graph
            .operators()
            .find(|o| o.name == name)
            .unwrap_or_else(|| panic!("no operator {name}"))
            .index
    }

    fn plan_of(df: &LogicalDataflow<u64>, parallelism: Parallelism) -> PhysicalPlan {
        PhysicalPlan::new(&df.graph, &df.stages, parallelism).unwrap()
    }

    fn exchange_edge(plan: &PhysicalPlan) -> &EdgePlan {
        let mut exchanges = plan
            .edges()
            .iter()
            .filter(|e| e.routing == EdgeRouting::Exchange);
        let edge = exchanges.next().expect("an exchange edge");
        assert!(exchanges.next().is_none());
        edge
    }

    #[test]
    fn uniform_plan_runs_everything_everywhere() {
        let df = exchange_shape(None);
        let plan = plan_of(&df, Parallelism::Uniform(3));
        assert_eq!(plan.total_workers(), 3);
        assert!(plan.is_uniform());
        for info in df.graph.operators() {
            assert_eq!(plan.hosts(info.index), 3, "{}", info.name);
            assert_eq!(plan.seed_multiplicity(info.index), 3);
            assert_eq!(plan.probe_writers(info.index), 3);
            for w in 0..3 {
                assert!(plan.runs(w, info.index));
                assert_eq!(
                    plan.operator_context(w, info.index).unwrap(),
                    WorkerContext::new(w, 3).unwrap()
                );
            }
        }
        for edge in plan.edges() {
            assert_eq!((edge.source_hosts, edge.target_hosts), (3, 3));
            for w in 0..3 {
                assert!(edge.built_on(w) && edge.runs_source(w) && edge.runs_target(w));
                assert_eq!(
                    plan.channel_context(w, edge).unwrap(),
                    WorkerContext::new(w, 3).unwrap()
                );
            }
        }
        assert_eq!(exchange_edge(&plan).routing, EdgeRouting::Exchange);
    }

    #[test]
    fn two_to_one_plan() {
        let df = exchange_shape(Some(1));
        let plan = plan_of(&df, Parallelism::PerStage { default: 2 });
        assert_eq!(plan.total_workers(), 2);
        assert!(!plan.is_uniform());
        let (input, inc, double) = (op(&df, "in"), op(&df, "inc"), op(&df, "double"));
        let exchange = exchange_edge(&plan).target.operator_index;
        for upstream in [input, inc] {
            assert_eq!(plan.hosts(upstream), 2);
            assert!(plan.runs(0, upstream) && plan.runs(1, upstream));
            assert_eq!(
                plan.operator_context(1, upstream).unwrap(),
                WorkerContext::new(1, 2).unwrap()
            );
        }
        for downstream in [exchange, double] {
            assert_eq!(plan.hosts(downstream), 1);
            assert!(plan.runs(0, downstream) && !plan.runs(1, downstream));
            assert_eq!(
                plan.operator_context(0, downstream).unwrap(),
                WorkerContext::new(0, 1).unwrap()
            );
        }
        assert_eq!(plan.seed_multiplicity(input), 2);
        assert_eq!(plan.probe_writers(double), 1);
        // 2 senders, 1 receiver, built on both workers with the dataflow's
        // context.
        let edge = exchange_edge(&plan);
        assert_eq!((edge.source_hosts, edge.target_hosts), (2, 1));
        assert!(edge.built_on(1) && edge.runs_source(1) && !edge.runs_target(1));
        assert_eq!(
            plan.channel_context(1, edge).unwrap(),
            WorkerContext::new(1, 2).unwrap()
        );
        // Worker-local edges stay within a stage, with its context.
        for edge in plan
            .edges()
            .iter()
            .filter(|e| e.routing != EdgeRouting::Exchange)
        {
            assert!(edge.same_stage);
            assert_eq!(edge.source_hosts, edge.target_hosts);
            assert_eq!(
                plan.channel_context(0, edge).unwrap(),
                WorkerContext::new(0, edge.source_hosts).unwrap()
            );
        }
    }

    #[test]
    fn two_to_three_plan() {
        let df = exchange_shape(Some(3));
        let plan = plan_of(&df, Parallelism::PerStage { default: 2 });
        assert_eq!(plan.total_workers(), 3);
        let (input, double) = (op(&df, "in"), op(&df, "double"));
        assert_eq!(plan.hosts(input), 2);
        assert!(!plan.runs(2, input));
        assert_eq!(plan.seed_multiplicity(input), 2);
        assert_eq!(plan.hosts(double), 3);
        assert_eq!(
            plan.operator_context(2, double).unwrap(),
            WorkerContext::new(2, 3).unwrap()
        );
        let edge = exchange_edge(&plan);
        assert_eq!((edge.source_hosts, edge.target_hosts), (2, 3));
        assert!(edge.built_on(2) && !edge.runs_source(2) && edge.runs_target(2));
        assert_eq!(
            plan.channel_context(2, edge).unwrap(),
            WorkerContext::new(2, 3).unwrap()
        );
    }

    #[test]
    fn auto_parallelism_plans() {
        // Inputs floor the default parallelism.
        assert_eq!(
            Parallelism::for_spawn_multi(1, true, true, 3),
            Parallelism::PerStage { default: 3 }
        );
        assert_eq!(
            Parallelism::for_spawn_multi(0, true, false, 0),
            Parallelism::PerStage { default: 1 }
        );
        assert_eq!(
            Parallelism::for_spawn_multi(4, true, true, 1),
            Parallelism::PerStage { default: 4 }
        );
        assert_eq!(
            Parallelism::for_spawn_multi(2, false, true, 5),
            Parallelism::PerStage { default: 2 }
        );
        assert_eq!(
            Parallelism::for_spawn_multi(2, false, false, 5),
            Parallelism::Uniform(2)
        );
        // One input, `num_workers` 0: stage 0 on 1 worker, `exchange_to(4)`
        // on 4.
        let df = exchange_shape(Some(4));
        let parallelism = Parallelism::for_spawn_multi(0, true, true, 1);
        let plan = plan_of(&df, parallelism);
        assert_eq!(plan.total_workers(), 4);
        assert_eq!(plan.hosts(op(&df, "in")), 1);
        assert_eq!(plan.hosts(op(&df, "double")), 4);
        // Without explicit parallelism the auto plan is uniform.
        let df = exchange_shape(None);
        let plan = plan_of(&df, Parallelism::for_spawn_multi(2, true, true, 1));
        assert!(plan.is_uniform());
        assert_eq!(plan.total_workers(), 2);
    }

    #[test]
    fn uniform_rejects_other_explicit_parallelism() {
        let df = exchange_shape(Some(1));
        let err = PhysicalPlan::new(&df.graph, &df.stages, Parallelism::Uniform(2)).unwrap_err();
        assert!(err.to_string().contains("explicit parallelism 1"), "{err}");
        assert!(PhysicalPlan::new(&df.graph, &df.stages, Parallelism::Uniform(0)).is_err());
        assert!(
            PhysicalPlan::new(&df.graph, &df.stages, Parallelism::PerStage { default: 0 }).is_err()
        );
    }

    /// A pipeline edge from a stage on 3 workers to one on 2: workers 2's
    /// records would have no target. The plan names the edge.
    #[test]
    fn worker_local_edge_into_fewer_workers_is_rejected() {
        let b = DataflowBuilder::<u64>::new("plan");
        let input = b.input::<u64>("in").unwrap();
        let wide = input
            .clone()
            .exchange_to("wide", 3, |x: &u64| *x)
            .unwrap()
            .map("wide-map", |_t, x| x);
        let narrow = input.exchange_to("narrow", 2, |x: &u64| *x).unwrap();
        narrow.merge(wide).unwrap().output("out").unwrap();
        let df = b.build().unwrap();
        let err = PhysicalPlan::new(&df.graph, &df.stages, Parallelism::PerStage { default: 1 })
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("pipeline edge from 'wide-map'") && err.contains("parallelism 3"),
            "{err}"
        );
        // The reverse direction (2 into 3) is allowed.
        let b = DataflowBuilder::<u64>::new("plan");
        let input = b.input::<u64>("in").unwrap();
        let narrow = input
            .clone()
            .exchange_to("narrow", 2, |x: &u64| *x)
            .unwrap()
            .map("narrow-map", |_t, x| x);
        let wide = input.exchange_to("wide", 3, |x: &u64| *x).unwrap();
        wide.merge(narrow).unwrap().output("out").unwrap();
        let df = b.build().unwrap();
        let plan = plan_of(&df, Parallelism::PerStage { default: 1 });
        let edge = plan
            .edges()
            .iter()
            .find(|e| e.routing == EdgeRouting::Pipeline && !e.same_stage)
            .expect("a pipeline edge between stages");
        assert_eq!((edge.source_hosts, edge.target_hosts), (2, 3));
        assert!(edge.built_on(2) && !edge.runs_source(2) && edge.runs_target(2));
    }

    /// A loop whose body changes parallelism: its feedback edge would join
    /// stages on different workers. Feedback is 1:1 per worker, so it needs
    /// equal parallelism, in either direction.
    #[test]
    fn feedback_edge_between_parallelisms_is_rejected() {
        for (default, inner) in [(1, 2), (3, 2)] {
            let b = DataflowBuilder::<u64>::new("plan");
            b.input::<u64>("in")
                .unwrap()
                .iterate::<u32>("loop", 1u32, |var| {
                    let next = var
                        .map("inc", |_t: &crate::order::Product<u64, u32>, x: u64| x + 1)
                        .exchange_to("redistribute", inner, |x: &u64| *x)
                        .unwrap();
                    crate::dataflow::dataflow_builder::IterateResult {
                        feedback: next.clone().filter("again", |_t, x| *x < 10),
                        output: next.filter("done", |_t, x| *x >= 10),
                    }
                })
                .output("out")
                .unwrap();
            let df = b.build().unwrap();
            let err = PhysicalPlan::new(&df.graph, &df.stages, Parallelism::PerStage { default })
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("cross-stage feedback edge") && err.contains("equal parallelism"),
                "{default} -> {inner}: {err}"
            );
        }
    }

    /// A loop after a parallelism change (`exchange_to(1)` then `iterate` on 2
    /// workers) is rejected through its feedback edge, as before step 2: stage
    /// inference puts the loop's feedback operator, which has no regular
    /// input, in stage 0.
    #[test]
    fn loop_after_parallelism_change_reports_its_feedback_edge() {
        let b = DataflowBuilder::<u64>::new("plan");
        b.input::<u64>("in")
            .unwrap()
            .exchange_to("gather", 1, |x: &u64| *x)
            .unwrap()
            .iterate::<u32>("loop", 1u32, |var| {
                let next = var.map("inc", |_t: &crate::order::Product<u64, u32>, x: u64| x + 1);
                crate::dataflow::dataflow_builder::IterateResult {
                    feedback: next.clone().filter("again", |_t, x| *x < 10),
                    output: next.filter("done", |_t, x| *x >= 10),
                }
            })
            .output("out")
            .unwrap();
        let df = b.build().unwrap();
        let err = PhysicalPlan::new(&df.graph, &df.stages, Parallelism::PerStage { default: 2 })
            .unwrap_err()
            .to_string();
        assert!(err.contains("cross-stage feedback edge"), "{err}");
    }

    #[test]
    fn plan_and_digest_are_deterministic() {
        let a = plan_of(
            &exchange_shape(Some(3)),
            Parallelism::PerStage { default: 2 },
        );
        let b = plan_of(
            &exchange_shape(Some(3)),
            Parallelism::PerStage { default: 2 },
        );
        assert_eq!(a, b);
        assert_eq!(a.digest(), b.digest());
        let other = plan_of(
            &exchange_shape(Some(3)),
            Parallelism::PerStage { default: 3 },
        );
        assert_ne!(a.digest(), other.digest());
        let uniform = plan_of(&exchange_shape(None), Parallelism::Uniform(2));
        assert_ne!(
            uniform.digest(),
            plan_of(&exchange_shape(None), Parallelism::Uniform(3)).digest()
        );
    }
}
