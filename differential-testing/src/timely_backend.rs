//! Native timely-dataflow builders and checkpoint coordination.
//!
//! Timely's inputs and probes belong to its worker threads, so each worker
//! runs a command loop: it applies input actions sent over a channel, steps
//! with bounded parking, and publishes its probe views. Records are captured
//! synchronously before each probe with the actual `worker.index()`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use timely::Config;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::core::{UnorderedHandle, UnorderedInput};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::vec::{Filter, Map};
use timely::dataflow::operators::{
    ActivateCapability, Capability, Concat, ConnectLoop, Enter, Inspect, Leave, LoopVariable, Probe,
};
use timely::dataflow::{Scope, Stream};
use timely::execute::execute;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::worker::Worker;

use crate::{
    Action, CHECKPOINT, Coords, Engine, Frontier, Obs, Observe, Out, Rec, Scenario, route,
    route_out, strip_count,
};

impl<A: Coords, B: Coords> Coords for Product<A, B> {
    fn push_coords(&self, out: &mut Vec<u64>) {
        self.outer.push_coords(out);
        self.inner.push_coords(out);
    }
    fn take_coords(c: &[u64]) -> (Self, &[u64]) {
        let (outer, rest) = A::take_coords(c);
        let (inner, rest) = B::take_coords(rest);
        (Product::new(outer, inner), rest)
    }
}

/// State shared between the controller and every worker.
pub struct Shared {
    /// `views[probe][worker]`: each worker's latest view of each probe.
    views: Mutex<Vec<Vec<Option<Frontier>>>>,
    captured: Mutex<Vec<Obs>>,
    /// Inconsistencies seen inside workers (for example a record whose
    /// stamp differs from its embedded time).
    errors: Mutex<Vec<String>>,
    receipts: Vec<AtomicU64>,
    /// `retired[graph][worker]`: the graph's dataflow is no longer
    /// installed on that worker.
    retired: Mutex<Vec<Vec<bool>>>,
}

/// What a worker's builder needs: its index, the observation choice and the
/// shared state.
pub struct Ctx {
    pub index: usize,
    pub observe: Observe,
    shared: Arc<Shared>,
}

impl Ctx {
    /// Receipt counter for `graph`, only when capturing.
    pub fn receipts(&self, graph: usize) -> Option<Arc<Shared>> {
        matches!(self.observe, Observe::Capture).then(|| {
            let _ = graph;
            Arc::clone(&self.shared)
        })
    }
}

fn add_receipts(shared: &Option<Arc<Shared>>, graph: usize, n: u64) {
    if let Some(shared) = shared {
        shared.receipts[graph].fetch_add(n, Ordering::SeqCst);
    }
}

/// An input owned by a worker: its handle and its frontier capability.
pub trait TInput {
    fn send(&mut self, time: &[u64], records: Vec<Rec>);
    fn advance(&mut self, time: &[u64]);
    fn close(&mut self);
}

struct UInput<T: Timestamp> {
    handle: UnorderedHandle<T, CapacityContainerBuilder<Vec<Rec>>>,
    cap: Option<ActivateCapability<T>>,
}

impl<T: Timestamp + Coords> TInput for UInput<T> {
    fn send(&mut self, time: &[u64], records: Vec<Rec>) {
        let time = T::from_coords(time);
        let cap = self
            .cap
            .as_ref()
            .expect("send on a closed input")
            .delayed(&time);
        let mut active = self.handle.activate();
        active.session(&cap).give_iterator(records.into_iter());
    }
    fn advance(&mut self, time: &[u64]) {
        let time = T::from_coords(time);
        self.cap
            .as_mut()
            .expect("advance of a closed input")
            .downgrade(&time);
    }
    fn close(&mut self) {
        self.cap = None;
    }
}

/// A new unordered input whose frontier capability is held separately, so
/// data can be sent at future times without moving the frontier.
pub fn new_input<'s, T: Timestamp + Coords>(
    scope: Scope<'s, T>,
) -> (Box<dyn TInput>, Stream<'s, T, Vec<Rec>>) {
    let ((handle, cap), stream) = scope.new_unordered_input::<CapacityContainerBuilder<Vec<Rec>>>();
    (
        Box::new(UInput {
            handle,
            cap: Some(cap),
        }),
        stream,
    )
}

/// A probe reported as plain coordinates.
pub type ProbeView = Box<dyn Fn() -> Frontier>;

/// Attach an observation point: capture (with the actual worker), count or
/// discard, then the probe.
pub fn observe<'s, T: Timestamp + Coords>(
    stream: Stream<'s, T, Vec<Out>>,
    probe: usize,
    ctx: &Ctx,
) -> (ProbeView, Stream<'s, T, Vec<Out>>) {
    let stream = match &ctx.observe {
        Observe::Capture => {
            let shared = Arc::clone(&ctx.shared);
            let worker = ctx.index;
            stream.inspect_core(move |event| {
                if let Ok((stamp, data)) = event {
                    let stamped = stamp.as_singleton().map(|t| t.to_coords());
                    let mut captured = shared.captured.lock().unwrap();
                    for (time, payload) in data {
                        if stamped.as_ref() != Some(time) {
                            shared.errors.lock().unwrap().push(format!(
                                "probe {probe} worker {worker}: record time {time:?} under stamp {:?}",
                                stamp.elements()
                            ));
                        }
                        captured.push(Obs {
                            probe,
                            worker,
                            time: time.clone(),
                            payload: payload.clone(),
                        });
                    }
                }
            })
        }
        Observe::Count(n) => {
            let n = Arc::clone(n);
            stream.inspect_core(move |event| {
                if let Ok((_, data)) = event {
                    n.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
            })
        }
        Observe::Discard => stream,
    };
    let (handle, stream) = stream.probe();
    let view: ProbeView =
        Box::new(move || handle.with_frontier(|f| f.iter().map(|t| t.to_coords()).collect()));
    (view, stream)
}

/// Per-time, per-key `[key, sum, count]` totals waiting under a capability.
type Stash<T> = BTreeMap<T, (Capability<T>, BTreeMap<u64, (i64, i64)>)>;

fn total_records<T: Coords>(time: &T, sums: BTreeMap<u64, (i64, i64)>) -> Vec<Out> {
    let coords = time.to_coords();
    sums.into_iter()
        .map(|(k, (sum, count))| (coords.clone(), vec![k as i64, sum, count]))
        .collect()
}

/// Stash every record of `$input` under a capability for its time.
macro_rules! absorb {
    ($input:expr, $stash:expr, $port:expr, $receipts:expr, $graph:expr) => {
        $input.for_each_stamp(|cap, data| {
            let time = cap.stamp().expect_singleton().clone();
            let entry = $stash
                .entry(time.clone())
                .or_insert_with(|| (cap.delayed(&time, $port), BTreeMap::new()));
            let mut n = 0;
            for batch in data {
                for (k, v) in batch.drain(..) {
                    let e = entry.1.entry(k).or_insert((0, 0));
                    e.0 += v;
                    e.1 += 1;
                    n += 1;
                }
            }
            add_receipts(&$receipts, $graph, n);
        })
    };
}

/// Exchanged two-input totals: a binary operator retains per-key sums and
/// counts per time until neither input frontier can still reach the time.
pub fn build_totals<'s>(
    left: Stream<'s, u64, Vec<Rec>>,
    right: Stream<'s, u64, Vec<Rec>>,
    graph: usize,
    ctx: &Ctx,
) -> Stream<'s, u64, Vec<Out>> {
    let receipts = ctx.receipts(graph);
    left.binary_frontier::<_, CapacityContainerBuilder<Vec<Out>>, _, _, _, _>(
        right,
        Exchange::new(|r: &Rec| route(r)),
        Exchange::new(|r: &Rec| route(r)),
        "totals",
        move |_cap, _info| {
            let mut stash: Stash<u64> = BTreeMap::new();
            move |(in1, f1), (in2, f2), output| {
                let port = output.output_index();
                absorb!(in1, stash, port, receipts, graph);
                absorb!(in2, stash, port, receipts, graph);
                let ready: Vec<u64> = stash
                    .keys()
                    .filter(|t| !f1.frontier().less_equal(t) && !f2.frontier().less_equal(t))
                    .copied()
                    .collect();
                for t in ready {
                    let (cap, sums) = stash.remove(&t).expect("ready");
                    output
                        .session(&cap)
                        .give_iterator(total_records(&t, sums).into_iter());
                }
            }
        },
    )
}

/// Product-time notifications: the two inputs are concatenated, exchanged
/// and totalled per time once the input frontier has passed it.
pub fn build_product_totals<'s>(
    a: Stream<'s, Product<u64, u64>, Vec<Rec>>,
    b: Stream<'s, Product<u64, u64>, Vec<Rec>>,
    graph: usize,
    ctx: &Ctx,
) -> Stream<'s, Product<u64, u64>, Vec<Out>> {
    let receipts = ctx.receipts(graph);
    a.concat(b)
        .unary_frontier::<CapacityContainerBuilder<Vec<Out>>, _, _, _>(
            Exchange::new(|r: &Rec| route(r)),
            "totals",
            move |_cap, _info| {
                let mut stash: Stash<Product<u64, u64>> = BTreeMap::new();
                move |(input, frontier), output| {
                    let port = output.output_index();
                    absorb!(input, stash, port, receipts, graph);
                    let ready: Vec<Product<u64, u64>> = stash
                        .keys()
                        .filter(|t| !frontier.frontier().less_equal(t))
                        .cloned()
                        .collect();
                    for t in ready {
                        let (cap, sums) = stash.remove(&t).expect("ready");
                        output
                            .session(&cap)
                            .give_iterator(total_records(&t, sums).into_iter());
                    }
                }
            },
        )
}

/// Per-time notified totals over `[key, value]` records, exchanged by key:
/// one `[key, sum, count]` per key once the input frontier has passed the
/// time. Receipts count records at times where `count_when` holds.
pub fn notify_totals<'s, T: Timestamp + Coords>(
    stream: Stream<'s, T, Vec<Out>>,
    name: &str,
    receipts: Option<Arc<Shared>>,
    graph: usize,
    count_when: fn(&[u64]) -> bool,
) -> Stream<'s, T, Vec<Out>> {
    stream.unary_frontier::<CapacityContainerBuilder<Vec<Out>>, _, _, _>(
        Exchange::new(|o: &Out| route_out(o)),
        name,
        move |_cap, _info| {
            let mut stash: Stash<T> = BTreeMap::new();
            move |(input, frontier), output| {
                let port = output.output_index();
                input.for_each_stamp(|cap, data| {
                    let time = cap.stamp().expect_singleton().clone();
                    let counted = count_when(&time.to_coords());
                    let entry = stash
                        .entry(time.clone())
                        .or_insert_with(|| (cap.delayed(&time, port), BTreeMap::new()));
                    let mut n = 0;
                    for batch in data {
                        for (_, p) in batch.drain(..) {
                            let e = entry.1.entry(p[0] as u64).or_insert((0, 0));
                            e.0 += p[1];
                            e.1 += 1;
                            n += 1;
                        }
                    }
                    if counted {
                        add_receipts(&receipts, graph, n);
                    }
                });
                let ready: Vec<T> = stash
                    .keys()
                    .filter(|t| !frontier.frontier().less_equal(t))
                    .cloned()
                    .collect();
                for t in ready {
                    let (cap, sums) = stash.remove(&t).expect("ready");
                    output
                        .session(&cap)
                        .give_iterator(total_records(&t, sums).into_iter());
                }
            }
        },
    )
}

/// Nested feedback with notification: an exchanged, notified aggregate in
/// an inner loop (two inner iterations) on the feedback path of an outer
/// loop, then a notified root aggregate. Returns the body and root probes.
pub fn build_nested<'s>(
    input: Stream<'s, u64, Vec<Rec>>,
    graph: usize,
    probes: (usize, usize),
    outer_iterations: u64,
    ctx: &Ctx,
) -> (ProbeView, ProbeView) {
    let scope = input.scope();
    let entered = input.map(|(k, v): Rec| (Vec::new(), vec![k as i64, v]));
    let receipts = ctx.receipts(graph);
    let (inner_view, left) = scope.iterative::<u32, _, _>(|outer| {
        let (outer_handle, outer_cycle) = outer.loop_variable::<Vec<Out>>(1);
        let looping = entered.enter(outer).concat(outer_cycle);
        let (inner_view, from_inner) = outer.iterative::<u32, _, _>(|inner| {
            let (inner_handle, inner_cycle) = inner.loop_variable::<Vec<Out>>(1);
            let totals = notify_totals(
                looping.enter(inner).concat(inner_cycle),
                "inner-totals",
                receipts,
                graph,
                |t| t[1] == 0 && t[2] == 0,
            );
            let (view, totals) = observe(totals, probes.0, ctx);
            totals
                .clone()
                .filter(|o: &Out| o.0[2] < 1)
                .map(strip_count)
                .connect_loop(inner_handle);
            let done = totals
                .filter(|o: &Out| o.0[2] >= 1)
                .map(strip_count)
                .leave(outer);
            (view, done)
        });
        from_inner
            .clone()
            .filter(move |o: &Out| o.0[1] + 1 < outer_iterations)
            .connect_loop(outer_handle);
        let done = from_inner
            .filter(move |o: &Out| o.0[1] + 1 >= outer_iterations)
            .leave(scope);
        (inner_view, done)
    });
    let root = notify_totals(left, "root-totals", None, graph, |_| false);
    let (root_view, _) = observe(root, probes.1, ctx);
    (inner_view, root_view)
}

/// What a worker's builder returns: its inputs and probe views, both in the
/// scenario's flat order.
pub struct Built {
    pub inputs: Vec<Box<dyn TInput>>,
    pub probes: Vec<ProbeView>,
    /// Dataflow identifier of each graph, from `next_dataflow_index`.
    pub dataflows: Vec<usize>,
}

enum Cmd {
    Act(Action),
    Stop(Instant),
}

type Guards = timely::communication::WorkerGuards<Result<(), String>>;

pub struct TimelyEngine {
    txs: Vec<Sender<Cmd>>,
    shared: Arc<Shared>,
    guards: Option<Guards>,
    drained: usize,
}

fn publish(worker: &Worker, built: &Built, shared: &Shared, index: usize) {
    {
        let mut views = shared.views.lock().unwrap();
        for (p, view) in built.probes.iter().enumerate() {
            views[p][index] = Some(view());
        }
    }
    let installed = worker.installed_dataflows();
    let mut retired = shared.retired.lock().unwrap();
    for (g, id) in built.dataflows.iter().enumerate() {
        retired[g][index] = !installed.contains(id);
    }
}

fn apply(built: &mut Built, action: Action) {
    match action {
        Action::Send {
            input,
            time,
            records,
            ..
        } => built.inputs[input].send(&time, records),
        Action::Advance { input, time } => built.inputs[input].advance(&time),
        Action::Close { input } => built.inputs[input].close(),
    }
}

/// Start `workers` timely workers, each building the scenario's graphs with
/// `build`, inside one execution.
pub fn start<B>(sc: &Scenario, mode: Observe, build: B) -> Box<dyn Engine>
where
    B: Fn(&mut Worker, &Ctx) -> Built + Send + Sync + 'static,
{
    let workers = sc.workers;
    let shared = Arc::new(Shared {
        views: Mutex::new(vec![vec![None; workers]; sc.probes.len()]),
        captured: Mutex::new(Vec::new()),
        errors: Mutex::new(Vec::new()),
        receipts: (0..sc.graphs).map(|_| AtomicU64::new(0)).collect(),
        retired: Mutex::new(vec![vec![false; workers]; sc.graphs]),
    });
    let (txs, rxs): (Vec<Sender<Cmd>>, Vec<Receiver<Cmd>>) =
        (0..workers).map(|_| channel()).unzip();
    let rxs = Mutex::new(rxs.into_iter().map(Some).collect::<Vec<_>>());
    let worker_shared = Arc::clone(&shared);
    let guards = execute(Config::process(workers), move |worker| {
        let index = worker.index();
        let rx = rxs.lock().unwrap()[index]
            .take()
            .expect("one receiver per worker");
        let ctx = Ctx {
            index,
            observe: mode.clone(),
            shared: Arc::clone(&worker_shared),
        };
        let mut built = build(worker, &ctx);
        loop {
            loop {
                match rx.try_recv() {
                    Ok(Cmd::Act(action)) => apply(&mut built, action),
                    Ok(Cmd::Stop(deadline)) => {
                        built.inputs.clear();
                        while worker.has_dataflows() {
                            if Instant::now() > deadline {
                                return Err(format!("worker {index} dataflows did not complete"));
                            }
                            worker.step_or_park(Some(Duration::from_millis(1)));
                            publish(worker, &built, &worker_shared, index);
                        }
                        publish(worker, &built, &worker_shared, index);
                        return Ok(());
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        return Err(format!("worker {index}: controller disconnected"));
                    }
                }
            }
            worker.step_or_park(Some(Duration::from_millis(1)));
            publish(worker, &built, &worker_shared, index);
        }
    })
    .expect("timely execute");
    Box::new(TimelyEngine {
        txs,
        shared,
        guards: Some(guards),
        drained: 0,
    })
}

impl TimelyEngine {
    fn errors(&self) -> Result<(), String> {
        let errors = self.shared.errors.lock().unwrap();
        match errors.first() {
            None => Ok(()),
            Some(e) => Err(e.clone()),
        }
    }
}

impl Engine for TimelyEngine {
    fn name(&self) -> &'static str {
        "timely"
    }

    fn apply(&mut self, action: &Action) -> Result<(), String> {
        let targets: Vec<usize> = match action {
            Action::Send { worker, .. } => vec![*worker],
            _ => (0..self.txs.len()).collect(),
        };
        for w in targets {
            self.txs[w]
                .send(Cmd::Act(action.clone()))
                .map_err(|_| format!("timely worker {w} is gone"))?;
        }
        Ok(())
    }

    fn frontier(&mut self, probe: usize) -> Result<Option<Frontier>, String> {
        self.errors()?;
        let views = self.shared.views.lock().unwrap();
        let mut all = Vec::new();
        for view in &views[probe] {
            match view {
                None => return Ok(None),
                Some(f) => all.push(f.clone()),
            }
        }
        Ok(Some(crate::meet(&all)))
    }

    fn receipts(&self, graph: usize) -> u64 {
        self.shared.receipts[graph].load(Ordering::SeqCst)
    }

    fn graph_completed(&mut self, graph: usize) -> Result<bool, String> {
        self.errors()?;
        Ok(self.shared.retired.lock().unwrap()[graph]
            .iter()
            .all(|r| *r))
    }

    fn drain(&mut self, into: &mut Vec<Obs>) -> Result<(), String> {
        self.errors()?;
        let captured = self.shared.captured.lock().unwrap();
        into.extend(captured[self.drained..].iter().cloned());
        self.drained = captured.len();
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), String> {
        let deadline = Instant::now() + CHECKPOINT;
        for tx in &self.txs {
            if tx.send(Cmd::Stop(deadline)).is_err() {
                self.abandon();
                return Err("a timely worker exited before stop".into());
            }
        }
        let guards = self.guards.take().expect("guards");
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let _ = tx.send(guards.join());
        });
        let result = match rx.recv_timeout(CHECKPOINT + Duration::from_secs(1)) {
            Err(_) => Err("timely workers did not join".to_string()),
            Ok(results) => results
                .into_iter()
                .enumerate()
                .try_for_each(|(w, r)| match r {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(format!("timely worker {w} panicked: {e}")),
                }),
        };
        result?;
        self.errors()?;
        let mut rest = Vec::new();
        self.drain(&mut rest)?;
        if !rest.is_empty() {
            return Err(format!("records after the final cut: {rest:?}"));
        }
        Ok(())
    }

    fn abandon(self: Box<Self>) {
        // Failure-only: WorkerGuards join in Drop, and workers keep stepping.
        std::mem::forget(self);
    }
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

pub fn start_totals(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    start(sc, mode, |worker, ctx| {
        let id = worker.next_dataflow_index();
        let mut built = worker.dataflow::<u64, _, _>(|scope| {
            let (left_in, left) = new_input(scope);
            let (right_in, right) = new_input(scope);
            let totals = build_totals(left, right, 0, ctx);
            let (view, _) = observe(totals, 0, ctx);
            Built {
                inputs: vec![left_in, right_in],
                probes: vec![view],
                dataflows: Vec::new(),
            }
        });
        built.dataflows = vec![id];
        built
    })
}

/// Timely root timestamps must refine `()`, which `Product` does not, so the
/// Product-time graph lives in a `Product<u64, u64>` child scope of a `u64`
/// dataflow that has no other inputs; its inputs and probe are native to
/// that child scope.
pub fn start_product(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    start(sc, mode, |worker, ctx| {
        let id = worker.next_dataflow_index();
        let mut built = worker.dataflow::<u64, _, _>(|root| {
            root.scoped::<Product<u64, u64>, _, _>("product", |scope| {
                let (a_in, a) = new_input(scope);
                let (b_in, b) = new_input(scope);
                let totals = build_product_totals(a, b, 0, ctx);
                let (view, _) = observe(totals, 0, ctx);
                Built {
                    inputs: vec![a_in, b_in],
                    probes: vec![view],
                    dataflows: Vec::new(),
                }
            })
        });
        built.dataflows = vec![id];
        built
    })
}

/// Nested feedback with notification; with `graphs > 1`, one dataflow per
/// graph inside the same timely execution and worker set.
pub fn start_nested(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let spec = sc.clone();
    start(sc, mode, move |worker, ctx| {
        let mut inputs: Vec<Option<Box<dyn TInput>>> = spec.inputs.iter().map(|_| None).collect();
        let mut probes: Vec<Option<ProbeView>> = spec.probes.iter().map(|_| None).collect();
        let mut dataflows = Vec::new();
        for g in 0..spec.graphs {
            dataflows.push(worker.next_dataflow_index());
            let ip = spec.probe_index(g, "inner-totals");
            let rp = spec.probe_index(g, "root-totals");
            let (input, inner, root) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, stream) = new_input(scope);
                let (inner, root) = build_nested(stream, g, (ip, rp), spec.outer_iterations, ctx);
                (input, inner, root)
            });
            inputs[spec.input_index(g, "in")] = Some(input);
            probes[ip] = Some(inner);
            probes[rp] = Some(root);
        }
        Built {
            inputs: inputs.into_iter().map(|i| i.expect("input")).collect(),
            probes: probes.into_iter().map(|p| p.expect("probe")).collect(),
            dataflows,
        }
    })
}

// ---------------------------------------------------------------------------
// Operator breadth and random programs
// ---------------------------------------------------------------------------

use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::core::Exchange as ExchangeOp;
use timely::dataflow::operators::vec::Broadcast;

use crate::programs::{
    PRec, Program, Stage, add_value, duplicate, hops_at, negate, rotate_key, route_program,
    with_hops,
};

/// Hold each record under a capability for `retime(time, record)` until the
/// input frontier has passed that time, then emit `finish(time, records)`.
/// The timely-native counterpart of instancy's notify-based built-ins.
pub fn hold_until_complete<'s, T, D, D2, P, R, F>(
    stream: Stream<'s, T, Vec<D>>,
    pact: P,
    name: &str,
    retime: R,
    mut finish: F,
) -> Stream<'s, T, Vec<D2>>
where
    T: Timestamp,
    D: Clone + 'static,
    D2: Clone + 'static,
    P: ParallelizationContract<T, Vec<D>>,
    R: Fn(&T, &D) -> T + 'static,
    F: FnMut(&T, Vec<D>) -> Vec<D2> + 'static,
{
    stream.unary_frontier::<CapacityContainerBuilder<Vec<D2>>, _, _, _>(
        pact,
        name,
        move |_cap, _info| {
            let mut stash: BTreeMap<T, (Capability<T>, Vec<D>)> = BTreeMap::new();
            move |(input, frontier), output| {
                let port = output.output_index();
                input.for_each_stamp(|cap, data| {
                    let time = cap.stamp().expect_singleton().clone();
                    for batch in data {
                        for d in batch.drain(..) {
                            let at = retime(&time, &d);
                            stash
                                .entry(at.clone())
                                .or_insert_with(|| (cap.delayed(&at, port), Vec::new()))
                                .1
                                .push(d);
                        }
                    }
                });
                let ready: Vec<T> = stash
                    .keys()
                    .filter(|t| !frontier.frontier().less_equal(t))
                    .cloned()
                    .collect();
                for t in ready {
                    let (cap, items) = stash.remove(&t).expect("ready");
                    let out = finish(&t, items);
                    if !out.is_empty() {
                        output.session(&cap).give_iterator(out.into_iter());
                    }
                }
            }
        },
    )
}

/// A pass-through map that also sees each record's time.
pub fn with_time<'s, T, D, D2, F>(
    stream: Stream<'s, T, Vec<D>>,
    name: &str,
    f: F,
) -> Stream<'s, T, Vec<D2>>
where
    T: Timestamp,
    D: Clone + 'static,
    D2: Clone + 'static,
    F: Fn(&T, D) -> D2 + 'static,
{
    stream.unary::<CapacityContainerBuilder<Vec<D2>>, _, _, _>(Pipeline, name, move |_, _| {
        move |input, output| {
            input.for_each_stamp(|cap, data| {
                let time = cap.stamp().expect_singleton().clone();
                let mut session = output.session(&cap);
                for batch in data {
                    for d in batch.drain(..) {
                        session.give(f(&time, d));
                    }
                }
            });
        }
    })
}

fn rec_out(r: Rec) -> Out {
    (Vec::new(), vec![r.0 as i64, r.1])
}

/// Operator breadth; see the instancy `build_breadth` for the five points.
pub fn build_breadth<'s>(
    input: Stream<'s, u64, Vec<Rec>>,
    graph: usize,
    base: usize,
    ctx: &Ctx,
) -> Vec<ProbeView> {
    let mut views = Vec::new();

    let totals = hold_until_complete(
        input.clone().broadcast(),
        Pipeline,
        "broadcast-totals",
        |t: &u64, _| *t,
        |t: &u64, recs: Vec<Rec>| {
            let mut sums: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
            for (k, v) in recs {
                let e = sums.entry(k).or_insert((0, 0));
                e.0 += v;
                e.1 += 1;
            }
            total_records(t, sums)
        },
    );
    views.push(observe(totals, base, ctx).0);

    let pos = input.clone().filter(|r: &Rec| r.1 >= 0);
    let neg = input
        .clone()
        .filter(|r: &Rec| r.1 < 0)
        .map(|(k, v): Rec| (k, -v));
    let spread = pos
        .concat(neg)
        .flat_map(|(k, v): Rec| vec![(k, v), (k, v % 3)]);
    let distinct = hold_until_complete(
        spread,
        Exchange::new(|r: &Rec| route(r)),
        "distinct",
        |t: &u64, _| *t,
        |t: &u64, recs: Vec<Rec>| {
            let unique: std::collections::BTreeSet<Rec> = recs.into_iter().collect();
            unique
                .into_iter()
                .map(|(k, x)| (t.to_coords(), vec![k as i64, x]))
                .collect()
        },
    );
    views.push(observe(distinct, base + 1, ctx).0);

    let delayed = hold_until_complete(
        input.clone(),
        Pipeline,
        "delay-odd",
        |t: &u64, r: &Rec| t + r.0 % 2,
        |_t: &u64, recs: Vec<Rec>| recs,
    );
    let reduced = hold_until_complete(
        delayed,
        Exchange::new(|r: &Rec| route(r)),
        "reduce",
        |t: &u64, _| *t,
        |_t: &u64, recs: Vec<Rec>| {
            recs.into_iter()
                .reduce(|a, b| (a.0.max(b.0), a.1 + b.1))
                .into_iter()
                .collect()
        },
    );
    let shown = with_time(reduced.clone(), "reduce-out", |t: &u64, (k, v): Rec| {
        (t.to_coords(), vec![k as i64, v])
    });
    views.push(observe(shown, base + 2, ctx).0);
    let counted = hold_until_complete(
        reduced,
        Exchange::new(|_r: &Rec| 0),
        "gather-count",
        |t: &u64, _| *t,
        |t: &u64, recs: Vec<Rec>| vec![(t.to_coords(), vec![recs.len() as i64])],
    );
    views.push(observe(counted, base + 3, ctx).0);

    let mut next = 0u64;
    let spread = ExchangeOp::exchange(input, move |_r: &Rec| {
        next += 1;
        next
    })
    .map(|(k, v): Rec| rec_out((k, 2 * v)));
    let totals = notify_totals(
        spread,
        "rebalanced-totals",
        ctx.receipts(graph),
        graph,
        |_| true,
    );
    views.push(observe(totals, base + 4, ctx).0);
    views
}

/// A non-loop stage at any scope depth.
fn program_stage<'s, T: Timestamp>(
    s: Stream<'s, T, Vec<PRec>>,
    stage: &Stage,
) -> Stream<'s, T, Vec<PRec>> {
    match stage {
        Stage::AddValue(x) => {
            let x = *x;
            s.map(move |r| add_value(r, x))
        }
        Stage::RotateKey(x) => {
            let x = *x;
            s.map(move |r| rotate_key(r, x))
        }
        Stage::KeepValueAtLeast(x) => {
            let x = *x;
            s.filter(move |r: &PRec| r.1.0 >= x)
        }
        Stage::Exchange => ExchangeOp::exchange(s, |r: &PRec| route_program(r)),
        Stage::Duplicate => s.flat_map(duplicate),
        Stage::BranchNegate => {
            let pos = s.clone().filter(|r: &PRec| r.1.0 >= 0);
            let neg = s.filter(|r: &PRec| r.1.0 < 0).map(negate);
            pos.concat(neg)
        }
        Stage::Broadcast => s.broadcast(),
        Stage::DelayOddKeys | Stage::Loop { .. } => {
            unreachable!("handled at the scope level: {stage:?}")
        }
    }
}

fn count_hops<'s, T: Timestamp>(
    s: Stream<'s, T, Vec<PRec>>,
    depth: u32,
) -> Stream<'s, T, Vec<PRec>> {
    s.map(move |r: PRec| {
        (
            r.0,
            (r.1.0, with_hops(r.1.1, depth, hops_at(r.1.1, depth) + 1)),
        )
    })
}

fn reset_hops<'s, T: Timestamp>(
    s: Stream<'s, T, Vec<PRec>>,
    depth: u32,
) -> Stream<'s, T, Vec<PRec>> {
    s.map(move |r: PRec| (r.0, (r.1.0, with_hops(r.1.1, depth, 0))))
}

fn program_inner<'s>(
    s: Stream<'s, Product<u64, u32>, Vec<PRec>>,
    iterations: u64,
    body: &[Stage],
) -> Stream<'s, Product<u64, u32>, Vec<PRec>> {
    let outer = s.scope();
    let entered = reset_hops(s, 1);
    outer.iterative::<u32, _, _>(|inner| {
        let (handle, cycle) = inner.loop_variable::<Vec<PRec>>(1);
        let mut q = entered.enter(inner).concat(cycle);
        for stage in body {
            q = program_stage(q, stage);
        }
        let q = count_hops(q, 1);
        q.clone()
            .filter(move |r: &PRec| hops_at(r.1.1, 1) < iterations)
            .connect_loop(handle);
        q.filter(move |r: &PRec| hops_at(r.1.1, 1) >= iterations)
            .leave(outer)
    })
}

fn program_outer<'s>(
    s: Stream<'s, u64, Vec<PRec>>,
    iterations: u64,
    body: &[Stage],
) -> Stream<'s, u64, Vec<PRec>> {
    let root = s.scope();
    let entered = reset_hops(s, 0);
    root.iterative::<u32, _, _>(|outer| {
        let (handle, cycle) = outer.loop_variable::<Vec<PRec>>(1);
        let mut q = entered.enter(outer).concat(cycle);
        for stage in body {
            q = match stage {
                Stage::Loop { iterations, body } => program_inner(q, *iterations, body),
                _ => program_stage(q, stage),
            };
        }
        let q = count_hops(q, 0);
        q.clone()
            .filter(move |r: &PRec| hops_at(r.1.1, 0) < iterations)
            .connect_loop(handle);
        q.filter(move |r: &PRec| hops_at(r.1.1, 0) >= iterations)
            .leave(root)
    })
}

/// A random program; see the instancy `build_program`.
pub fn build_program<'s>(
    input: Stream<'s, u64, Vec<Rec>>,
    program: &Program,
    graph: usize,
    base: usize,
    ctx: &Ctx,
) -> Vec<ProbeView> {
    let receipts = ctx.receipts(graph);
    let mut s = input.map(move |(k, v): Rec| {
        add_receipts(&receipts, graph, 1);
        (k, (v, 0u64))
    });
    for stage in &program.stages {
        s = match stage {
            Stage::Loop { iterations, body } => program_outer(s, *iterations, body),
            Stage::DelayOddKeys => hold_until_complete(
                s,
                Pipeline,
                "delay-odd",
                |t: &u64, r: &PRec| t + r.0 % 2,
                |_t: &u64, recs: Vec<PRec>| recs,
            ),
            _ => program_stage(s, stage),
        };
    }
    let pass = with_time(s.clone(), "pass-out", |t: &u64, r: PRec| {
        (t.to_coords(), vec![r.0 as i64, r.1.0, r.1.1 as i64])
    });
    let mut views = vec![observe(pass, base, ctx).0];
    let totals = notify_totals(
        s.map(|r: PRec| (Vec::new(), vec![r.0 as i64, r.1.0])),
        "totals",
        None,
        graph,
        |_| false,
    );
    views.push(observe(totals, base + 1, ctx).0);
    views
}

pub fn start_breadth(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let spec = sc.clone();
    start(sc, mode, move |worker, ctx| {
        let mut inputs = Vec::new();
        let mut probes = Vec::new();
        let mut dataflows = Vec::new();
        for g in 0..spec.graphs {
            dataflows.push(worker.next_dataflow_index());
            let base = spec.probe_index(g, "broadcast-totals");
            let (input, views) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, stream) = new_input(scope);
                (input, build_breadth(stream, g, base, ctx))
            });
            inputs.push(input);
            probes.extend(views);
        }
        Built {
            inputs,
            probes,
            dataflows,
        }
    })
}

pub fn start_program(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let program = sc.program().clone();
    let base = sc.probe_index(0, "pass");
    start(sc, mode, move |worker, ctx| {
        let id = worker.next_dataflow_index();
        let (input, probes) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = new_input(scope);
            (input, build_program(stream, &program, 0, base, ctx))
        });
        Built {
            inputs: vec![input],
            probes,
            dataflows: vec![id],
        }
    })
}
