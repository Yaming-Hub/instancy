//! Native instancy builders and checkpoint coordination.
//!
//! Each graph is one `spawn_multi` dataflow on a shared runtime. Every
//! observation stream ends in `.probe()` directly followed by `.output()`:
//! the probe then covers the sink's outstanding input credit, so once a time
//! is complete, its records are in that worker's receiver.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::Duration;

use instancy::dataflow::{InputHandle, OutputEvent, OutputHandle, Retained};
use instancy::{
    AsyncInputSender, AsyncOutputReceiver, DataflowBuilder, InputSender, IoMode, IterateResult,
    OutputReceiver, Pipe, Product, Result as IResult, RuntimeConfig, RuntimeHandle, SpawnOptions,
    Timestamp, TokioMode,
};

use crate::{
    Action, CHECKPOINT, Coords, Engine, Frontier, InstancyConfig, Obs, Observe, Out, Rec, Scenario,
    route, route_out, strip_count,
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

/// A probe reported as plain coordinates.
pub type ProbeView = Box<dyn Fn() -> Frontier>;

/// What a graph's builder needs: its graph id, observation choice, exposed
/// edge capacity, receipts and the observation points it declares. A
/// cluster node calls its build once per local worker, so a probe may be
/// declared once per worker; its views are met.
pub struct Hooks {
    pub graph: usize,
    pub mode: Observe,
    pub capacity: usize,
    receipts: Option<Arc<Vec<AtomicU64>>>,
    pub(crate) probes: RefCell<Vec<(usize, ProbeView)>>,
    pub(crate) outputs: RefCell<Vec<(usize, String)>>,
    /// The scenario index of this graph's first probe, for builders that
    /// declare several observation points in a fixed order.
    probe_base: std::cell::Cell<usize>,
}

/// Receipt counter shared by an operator's per-worker clones.
#[derive(Clone)]
pub struct Receipts(Option<(Arc<Vec<AtomicU64>>, usize)>);

impl Receipts {
    pub fn add(&self, n: usize) {
        if let Some((counters, graph)) = &self.0 {
            counters[*graph].fetch_add(n as u64, Ordering::SeqCst);
        }
    }
}

impl Hooks {
    pub(crate) fn new(
        graph: usize,
        mode: &Observe,
        capacity: usize,
        receipts: &Arc<Vec<AtomicU64>>,
    ) -> Self {
        Hooks {
            graph,
            mode: mode.clone(),
            capacity,
            receipts: matches!(mode, Observe::Capture).then(|| Arc::clone(receipts)),
            probes: RefCell::new(Vec::new()),
            outputs: RefCell::new(Vec::new()),
            probe_base: std::cell::Cell::new(0),
        }
    }

    pub fn set_probe_base(&self, base: usize) {
        self.probe_base.set(base);
    }

    pub fn probe_base(&self) -> usize {
        self.probe_base.get()
    }

    pub fn receipts(&self) -> Receipts {
        Receipts(self.receipts.clone().map(|r| (r, self.graph)))
    }
}

/// Attach an observation point. The probe is directly followed by the sink.
pub fn observe<T: Timestamp + Coords>(
    pipe: Pipe<T, Out>,
    probe: usize,
    name: &str,
    hooks: &Hooks,
) -> IResult<()> {
    let (pipe, handle) = pipe.probe();
    hooks.probes.borrow_mut().push((
        probe,
        Box::new(move || {
            handle
                .frontier()
                .elements()
                .iter()
                .map(Coords::to_coords)
                .collect()
        }),
    ));
    let pipe = pipe.with_capacity(hooks.capacity)?;
    match &hooks.mode {
        Observe::Capture => {
            pipe.output(name)?;
            hooks.outputs.borrow_mut().push((probe, name.to_string()));
        }
        Observe::Count(n) => {
            let n = Arc::clone(n);
            pipe.for_each(name.to_string(), move |_t, _x| {
                n.fetch_add(1, Ordering::Relaxed);
            });
        }
        Observe::Discard => pipe.for_each(name.to_string(), |_t, _x| {}),
    }
    Ok(())
}

fn total_records<T: Coords>(time: &T, sums: BTreeMap<u64, (i64, i64)>) -> Vec<Out> {
    let coords = time.to_coords();
    sums.into_iter()
        .map(|(k, (sum, count))| (coords.clone(), vec![k as i64, sum, count]))
        .collect()
}

/// Per-time totals, each held by a retained capability. Operator closures
/// are cloned once per worker before any call, so a clone starts empty.
type Sums = BTreeMap<u64, (i64, i64)>;
struct Held<T: Timestamp>(BTreeMap<Vec<u64>, (Retained<T>, Sums)>);

impl<T: Timestamp> Default for Held<T> {
    fn default() -> Self {
        Held(BTreeMap::new())
    }
}

impl<T: Timestamp> Clone for Held<T> {
    fn clone(&self) -> Self {
        Held::default()
    }
}

fn absorb<T: Timestamp + Coords>(
    input: &mut InputHandle<T, Rec>,
    output: &mut OutputHandle<T, Out>,
    held: &mut Held<T>,
    receipts: &Receipts,
) {
    while let Some((time, data)) = input.next() {
        let entry = held
            .0
            .entry(time.to_coords())
            .or_insert_with(|| (output.retain(time.clone()), BTreeMap::new()));
        for (k, v) in &data {
            let e = entry.1.entry(*k).or_insert((0, 0));
            e.0 += v;
            e.1 += 1;
        }
        receipts.add(data.len());
    }
}

/// Exchanged two-input totals: a binary operator retains per-key sums and
/// counts per time until neither input frontier can still reach the time.
pub fn build_totals(b: &DataflowBuilder<u64>, hooks: &Hooks) -> IResult<Pipe<u64, Out>> {
    let c = hooks.capacity;
    let left = b
        .input::<Rec>("left")?
        .with_capacity(c)?
        .exchange_by_hash("left-by-key", route)
        .with_capacity(c)?;
    let right = b
        .input::<Rec>("right")?
        .with_capacity(c)?
        .exchange_by_hash("right-by-key", route)
        .with_capacity(c)?;
    let receipts = hooks.receipts();
    left.binary(right, "totals", {
        let mut held = Held::<u64>::default();
        move |in1, in2, output| {
            absorb(in1, output, &mut held, &receipts);
            absorb(in2, output, &mut held, &receipts);
            let (f1, f2) = (in1.frontier().clone(), in2.frontier().clone());
            let ready: Vec<Vec<u64>> = held
                .0
                .iter()
                .filter(|(_, (r, _))| !f1.less_equal(r.time()) && !f2.less_equal(r.time()))
                .map(|(k, _)| k.clone())
                .collect();
            for k in ready {
                let (retained, sums) = held.0.remove(&k).expect("ready");
                let time = *retained.time();
                output.push_vec(time, total_records(&time, sums));
                drop(retained);
            }
            Ok(())
        }
    })
}

/// Product-time notifications: the two inputs are merged, exchanged and
/// totalled per time by `unary_notify`.
pub fn build_product_totals(
    b: &DataflowBuilder<Product<u64, u64>>,
    hooks: &Hooks,
) -> IResult<Pipe<Product<u64, u64>, Out>> {
    let c = hooks.capacity;
    let a = b.input::<Rec>("a")?.with_capacity(c)?;
    let bb = b.input::<Rec>("b")?.with_capacity(c)?;
    let receipts = hooks.receipts();
    Ok(a.merge(bb)?
        .with_capacity(c)?
        .exchange_by_hash("by-key", route)
        .with_capacity(c)?
        .unary_notify("totals", {
            let mut stash: BTreeMap<Vec<u64>, BTreeMap<u64, (i64, i64)>> = BTreeMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    let sums = stash.entry(time.to_coords()).or_default();
                    for (k, v) in &data {
                        let e = sums.entry(*k).or_insert((0, 0));
                        e.0 += v;
                        e.1 += 1;
                    }
                    ctx.notify_at(time);
                    receipts.add(data.len());
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(sums) = stash.remove(&time.to_coords()) {
                        output.push_vec(time, total_records(&time, sums));
                    }
                }
                Ok(())
            }
        }))
}

/// The per-edge capacity for the next exposed edge of `pipe`.
fn cap<T: Timestamp, D: Clone + Send + 'static>(pipe: Pipe<T, D>, capacity: usize) -> Pipe<T, D> {
    pipe.with_capacity(capacity).expect("capacity > 0")
}

/// Timestamps of exchanged streams: with instancy's `transport` feature
/// (cluster builds) they cross the network too, so they need a codec.
#[cfg(feature = "cluster")]
pub trait ExTime: Timestamp + Coords + instancy::communication::codec::ExchangeData {}
#[cfg(feature = "cluster")]
impl<T: Timestamp + Coords + instancy::communication::codec::ExchangeData> ExTime for T {}
#[cfg(not(feature = "cluster"))]
pub trait ExTime: Timestamp + Coords {}
#[cfg(not(feature = "cluster"))]
impl<T: Timestamp + Coords> ExTime for T {}

/// Exchange observation-shaped records by key.
#[cfg(not(feature = "cluster"))]
fn exchange_out<T: ExTime>(pipe: Pipe<T, Out>, name: &str, capacity: usize) -> Pipe<T, Out> {
    cap(pipe, capacity).exchange_by_hash(format!("{name}-by-key"), route_out)
}

/// With instancy's `transport` feature, exchanged records need a codec, and
/// `(Vec<u64>, Vec<i64>)` has none: records cross the exchange as [`Wire`].
/// This adds a map on each side of the exchange in cluster builds only.
#[cfg(feature = "cluster")]
fn exchange_out<T: ExTime>(pipe: Pipe<T, Out>, name: &str, capacity: usize) -> Pipe<T, Out> {
    let wire = cap(pipe, capacity).map(format!("{name}-to-wire"), |_t, o: Out| Wire(o));
    cap(
        cap(wire, capacity).exchange_by_hash(format!("{name}-by-key"), |w: &Wire| route_out(&w.0)),
        capacity,
    )
    .map(format!("{name}-from-wire"), |_t, w: Wire| w.0)
}

/// An observation-shaped record with a length-prefixed little-endian codec.
#[cfg(feature = "cluster")]
#[derive(Clone, Debug, PartialEq)]
pub struct Wire(pub Out);

#[cfg(feature = "cluster")]
pub struct WireCodec;

#[cfg(feature = "cluster")]
impl instancy::communication::codec::Codec<Wire> for WireCodec {
    fn encode(
        &self,
        value: &Wire,
        buf: &mut Vec<u8>,
    ) -> Result<(), instancy::communication::codec::CodecError> {
        let (time, payload) = &value.0;
        buf.extend_from_slice(&(time.len() as u32).to_le_bytes());
        for t in time {
            buf.extend_from_slice(&t.to_le_bytes());
        }
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        for p in payload {
            buf.extend_from_slice(&p.to_le_bytes());
        }
        Ok(())
    }

    fn decode(
        &self,
        buf: &[u8],
    ) -> Result<(Wire, usize), instancy::communication::codec::CodecError> {
        use instancy::communication::codec::CodecError;
        let mut at = 0;
        let mut take = |n: usize| -> Result<&[u8], CodecError> {
            let slice = buf.get(at..at + n).ok_or(CodecError::InsufficientData {
                needed: at + n,
                available: buf.len(),
            })?;
            at += n;
            Ok(slice)
        };
        let words = |b: &[u8]| u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize;
        let n = words(take(4)?);
        let mut time = Vec::with_capacity(n);
        for _ in 0..n {
            time.push(u64::from_le_bytes(take(8)?.try_into().expect("8 bytes")));
        }
        let n = words(take(4)?);
        let mut payload = Vec::with_capacity(n);
        for _ in 0..n {
            payload.push(i64::from_le_bytes(take(8)?.try_into().expect("8 bytes")));
        }
        Ok((Wire((time, payload)), at))
    }
}

#[cfg(feature = "cluster")]
impl instancy::communication::codec::ExchangeData for Wire {
    type CodecType = WireCodec;
    fn codec() -> WireCodec {
        WireCodec
    }
}

/// Per-time notified totals over `[key, value]` records, exchanged by key:
/// one `[key, sum, count]` per key once the input frontier has passed the
/// time. Receipts count records at times where `count_when` holds.
pub fn notify_totals<T: ExTime>(
    pipe: Pipe<T, Out>,
    name: &str,
    capacity: usize,
    receipts: Receipts,
    count_when: fn(&[u64]) -> bool,
) -> Pipe<T, Out> {
    cap(exchange_out(pipe, name, capacity), capacity).unary_notify(name.to_string(), {
        let mut stash: BTreeMap<Vec<u64>, Sums> = BTreeMap::new();
        move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                let coords = time.to_coords();
                let sums = stash.entry(coords.clone()).or_default();
                for (_, p) in &data {
                    let e = sums.entry(p[0] as u64).or_insert((0, 0));
                    e.0 += p[1];
                    e.1 += 1;
                }
                ctx.notify_at(time);
                if count_when(&coords) {
                    receipts.add(data.len());
                }
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(sums) = stash.remove(&time.to_coords()) {
                    let records = total_records(&time, sums);
                    output.push_vec(time, records);
                }
            }
            Ok(())
        }
    })
}

/// Nested feedback with notification: an exchanged, notified aggregate in
/// an inner loop (two inner iterations) on the feedback path of an outer
/// loop, then a notified root aggregate.
pub fn build_nested(
    b: &DataflowBuilder<u64>,
    hooks: &Hooks,
    probes: (usize, usize),
    outer_iterations: u64,
) -> IResult<()> {
    let c = hooks.capacity;
    let receipts = hooks.receipts();
    let failed: RefCell<Option<instancy::Error>> = RefCell::new(None);
    let entered = cap(b.input::<Rec>("in")?, c).map("to-out", |_t: &u64, (k, v): Rec| {
        (Vec::new(), vec![k as i64, v])
    });
    let left = cap(entered, c).iterate::<u32>("outer", 1u32, |vo| {
        let from_inner = cap(vo, c).iterate::<u32>("inner", 1u32, |vi| {
            let totals = notify_totals(vi, "inner-totals", c, receipts.clone(), |t| {
                t[1] == 0 && t[2] == 0
            });
            let (observed, rest) = cap(totals, c).fork("inner-observe");
            if let Err(e) = observe(observed, probes.0, "inner-totals", hooks) {
                failed.borrow_mut().get_or_insert(e);
            }
            let (again, done) = cap(rest, c).fork("inner-split");
            // The loop exit edge takes its capacity from `output`'s override;
            // the back-edge is unbounded whatever `feedback` carries.
            IterateResult {
                feedback: cap(
                    cap(again, c).filter("inner-again", |_t, o: &Out| o.0[2] < 1),
                    c,
                )
                .map("inner-again-strip", |_t, o: Out| strip_count(o)),
                output: cap(
                    cap(
                        cap(done, c).filter("inner-done", |_t, o: &Out| o.0[2] >= 1),
                        c,
                    )
                    .map("inner-done-strip", |_t, o: Out| strip_count(o)),
                    c,
                ),
            }
        });
        let (again, done) = cap(from_inner, c).fork("outer-split");
        IterateResult {
            feedback: cap(again, c).filter("outer-again", move |_t, o: &Out| {
                o.0[1] + 1 < outer_iterations
            }),
            output: cap(
                cap(done, c).filter("outer-done", move |_t, o: &Out| {
                    o.0[1] + 1 >= outer_iterations
                }),
                c,
            ),
        }
    });
    let root = notify_totals(left, "root-totals", c, Receipts(None), |_| false);
    observe(root, probes.1, "root-totals", hooks)?;
    match failed.into_inner() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub(crate) trait IInput {
    fn send(&mut self, worker: usize, time: &[u64], records: Vec<Rec>) -> Result<(), String>;
    fn advance(&mut self, time: &[u64]) -> Result<(), String>;
    fn close(&mut self);
}

/// One logical input: a sender per worker.
pub(crate) struct Senders<T: Timestamp>(pub(crate) Vec<InputSender<T, Rec>>);

impl<T: Timestamp + Coords> IInput for Senders<T> {
    fn send(&mut self, worker: usize, time: &[u64], records: Vec<Rec>) -> Result<(), String> {
        self.0
            .get(worker)
            .ok_or("send on a closed input")?
            .send(T::from_coords(time), records)
            .map_err(|e| e.to_string())
    }
    fn advance(&mut self, time: &[u64]) -> Result<(), String> {
        for s in &self.0 {
            s.advance_to(T::from_coords(time))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn close(&mut self) {
        self.0.clear();
    }
}

pub(crate) type Drain = Box<dyn FnMut(&mut Vec<Obs>) -> Result<(), String>>;
pub(crate) type Join = Box<dyn FnOnce() -> IResult<()> + Send>;

/// A graph's join: not yet asked for, running on a helper thread (so the
/// controller keeps draining and polling), or finished with its outcome.
pub(crate) enum JoinState {
    Pending(Join),
    Running(Receiver<Result<(), String>>),
    Done(Result<(), String>),
}

impl JoinState {
    /// Start the join if needed and report whether the graph has completed
    /// successfully. Each graph is joined once; its outcome is kept.
    pub(crate) fn poll(&mut self) -> Result<bool, String> {
        if let JoinState::Pending(_) = self {
            let JoinState::Pending(join) = std::mem::replace(self, JoinState::Done(Ok(()))) else {
                unreachable!()
            };
            let (tx, rx) = channel();
            std::thread::spawn(move || {
                let _ = tx.send(join().map_err(|e| e.to_string()));
            });
            *self = JoinState::Running(rx);
        }
        if let JoinState::Running(rx) = self {
            match rx.try_recv() {
                Ok(outcome) => *self = JoinState::Done(outcome),
                Err(TryRecvError::Empty) => return Ok(false),
                Err(TryRecvError::Disconnected) => {
                    *self = JoinState::Done(Err("join thread panicked".into()))
                }
            }
        }
        match self {
            JoinState::Done(Ok(())) => Ok(true),
            JoinState::Done(Err(e)) => Err(e.clone()),
            _ => unreachable!(),
        }
    }
}

/// One logical input with `IoMode::Async` senders, driven from the
/// controller thread on the runtime's Tokio handle.
pub(crate) struct AsyncSenders<T: Timestamp>(Vec<AsyncInputSender<T, Rec>>, tokio::runtime::Handle);

impl<T: Timestamp + Coords> IInput for AsyncSenders<T> {
    fn send(&mut self, worker: usize, time: &[u64], records: Vec<Rec>) -> Result<(), String> {
        let sender = self.0.get(worker).ok_or("send on a closed input")?;
        self.1
            .block_on(sender.send(T::from_coords(time), records))
            .map_err(|e| e.to_string())
    }
    fn advance(&mut self, time: &[u64]) -> Result<(), String> {
        for s in &self.0 {
            self.1
                .block_on(s.advance_to(T::from_coords(time)))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    fn close(&mut self) {
        self.0.clear();
    }
}

/// [`output_drain`] for an `IoMode::Async` receiver.
pub(crate) fn async_output_drain<T: Timestamp + Coords>(
    probe: usize,
    worker: usize,
    mut rx: AsyncOutputReceiver<T, Out>,
) -> Drain {
    Box::new(move |into: &mut Vec<Obs>| {
        while let Some(event) = rx.try_recv() {
            if let OutputEvent::Data { time, data } = event {
                push_observed(probe, worker, time.to_coords(), data, into)?;
            }
        }
        Ok(())
    })
}

fn push_observed(
    probe: usize,
    worker: usize,
    root: Vec<u64>,
    data: Vec<Out>,
    into: &mut Vec<Obs>,
) -> Result<(), String> {
    for (native, payload) in data {
        if native.len() < root.len() || native[..root.len()] != root[..] {
            return Err(format!(
                "probe {probe} worker {worker}: native time {native:?} under root event time {root:?}"
            ));
        }
        into.push(Obs {
            probe,
            worker,
            time: native,
            payload,
        });
    }
    Ok(())
}

/// Drain one worker's receiver of an observation point, checking that each
/// record's native time projects onto the event's root time.
pub(crate) fn output_drain<T: Timestamp + Coords>(
    probe: usize,
    worker: usize,
    rx: OutputReceiver<T, Out>,
) -> Drain {
    Box::new(move |into: &mut Vec<Obs>| {
        while let Some(event) = rx.try_recv() {
            if let OutputEvent::Data { time, data } = event {
                let root = time.to_coords();
                for (native, payload) in data {
                    if native.len() < root.len() || native[..root.len()] != root[..] {
                        return Err(format!(
                            "probe {probe} worker {worker}: native time {native:?} under root event time {root:?}"
                        ));
                    }
                    into.push(Obs {
                        probe,
                        worker,
                        time: native,
                        payload,
                    });
                }
            }
        }
        Ok(())
    })
}

pub struct InstancyEngine {
    runtime: Option<RuntimeHandle>,
    config: InstancyConfig,
    workers: usize,
    inputs: Vec<Option<Box<dyn IInput>>>,
    probes: Vec<Option<ProbeView>>,
    drains: Vec<Drain>,
    joins: Vec<JoinState>,
    receipts: Arc<Vec<AtomicU64>>,
}

/// The correctness runtime: a fixed execution pool and an owned Tokio
/// runtime, both recorded in every scenario label.
pub fn runtime(pool_threads: usize, tokio_threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: pool_threads,
        name: "differential".into(),
        tokio_mode: TokioMode::Create {
            worker_threads: tokio_threads,
        },
        ..RuntimeConfig::default()
    })
    .expect("instancy runtime")
}

impl InstancyEngine {
    pub fn new(sc: &Scenario) -> Self {
        InstancyEngine {
            runtime: Some(runtime(sc.instancy.pool_threads, sc.instancy.tokio_threads)),
            config: sc.instancy,
            workers: sc.workers,
            inputs: (0..sc.inputs.len()).map(|_| None).collect(),
            probes: (0..sc.probes.len()).map(|_| None).collect(),
            drains: Vec::new(),
            joins: Vec::new(),
            receipts: Arc::new((0..sc.graphs).map(|_| AtomicU64::new(0)).collect()),
        }
    }

    /// Spawn graph `graph` on the shared runtime and its logical workers.
    pub fn spawn_graph<T, F>(
        &mut self,
        sc: &Scenario,
        graph: usize,
        mode: &Observe,
        build: F,
    ) -> IResult<()>
    where
        T: Timestamp + Coords,
        F: Fn(&DataflowBuilder<T>, &Hooks) -> IResult<()>,
    {
        let hooks = Hooks::new(graph, mode, sc.capacity, &self.receipts);
        let runtime = self.runtime.as_ref().expect("runtime");
        let mut multi = runtime.spawn_multi(
            &format!("{}-g{graph}", sc.recipe),
            self.workers,
            |b: &mut DataflowBuilder<T>| build(b, &hooks),
            SpawnOptions::new()
                .per_stage_parallelism(self.config.per_stage)
                .io_mode(if self.config.io_async {
                    IoMode::Async
                } else {
                    IoMode::Sync
                }),
        )?;
        let tokio = runtime.tokio_handle().clone();
        for (i, spec) in sc.inputs.iter().enumerate() {
            if spec.graph == graph {
                self.inputs[i] = Some(if self.config.io_async {
                    let senders = (0..self.workers)
                        .map(|w| multi.take_async_input::<Rec>(w, &spec.name))
                        .collect::<IResult<Vec<_>>>()?;
                    Box::new(AsyncSenders(senders, tokio.clone()))
                } else {
                    let senders = (0..self.workers)
                        .map(|w| multi.take_input::<Rec>(w, &spec.name))
                        .collect::<IResult<Vec<_>>>()?;
                    Box::new(Senders(senders))
                });
            }
        }
        for (p, view) in hooks.probes.into_inner() {
            self.probes[p] = Some(view);
        }
        for (probe, name) in hooks.outputs.into_inner() {
            for worker in 0..self.workers {
                self.drains.push(if self.config.io_async {
                    async_output_drain(
                        probe,
                        worker,
                        multi.take_async_output::<Out>(worker, &name)?,
                    )
                } else {
                    output_drain(probe, worker, multi.take_output::<Out>(worker, &name)?)
                });
            }
        }
        self.joins
            .push(JoinState::Pending(Box::new(move || multi.join_blocking())));
        Ok(())
    }

    fn poll_join(&mut self, graph: usize) -> Result<bool, String> {
        self.joins[graph].poll()
    }
}

impl Engine for InstancyEngine {
    fn name(&self) -> &'static str {
        "instancy"
    }

    fn apply(&mut self, action: &Action) -> Result<(), String> {
        match action {
            Action::Send {
                input,
                worker,
                time,
                records,
            } => self.inputs[*input].as_mut().ok_or("unknown input")?.send(
                *worker,
                time,
                records.clone(),
            ),
            Action::Advance { input, time } => self.inputs[*input]
                .as_mut()
                .ok_or("unknown input")?
                .advance(time),
            Action::Close { input } => {
                self.inputs[*input].as_mut().ok_or("unknown input")?.close();
                Ok(())
            }
        }
    }

    fn frontier(&mut self, probe: usize) -> Result<Option<Frontier>, String> {
        Ok(Some(self.probes[probe].as_ref().ok_or("unknown probe")?()))
    }

    fn receipts(&self, graph: usize) -> u64 {
        self.receipts[graph].load(Ordering::SeqCst)
    }

    fn graph_completed(&mut self, graph: usize) -> Result<bool, String> {
        self.poll_join(graph)
    }

    fn drain(&mut self, into: &mut Vec<Obs>) -> Result<(), String> {
        for drain in &mut self.drains {
            drain(into)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), String> {
        for input in self.inputs.iter_mut().flatten() {
            input.close();
        }
        // Graphs already joined at a completion cut keep their outcome; any
        // other graph is joined now, each exactly once.
        let deadline = std::time::Instant::now() + CHECKPOINT + Duration::from_secs(1);
        loop {
            let mut all = true;
            for g in 0..self.joins.len() {
                match self.poll_join(g) {
                    Ok(done) => all &= done,
                    Err(e) => {
                        self.abandon();
                        return Err(format!("graph {g}: {e}"));
                    }
                }
            }
            if all {
                break;
            }
            if std::time::Instant::now() > deadline {
                self.abandon();
                return Err("instancy dataflows did not join".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut rest = Vec::new();
        if let Err(e) = self.drain(&mut rest) {
            self.abandon();
            return Err(e);
        }
        if !rest.is_empty() {
            self.abandon();
            return Err(format!("records after the final cut: {rest:?}"));
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown();
        }
        Ok(())
    }

    fn abandon(self: Box<Self>) {
        // Failure-only: the runtime and live dataflows are never joined.
        std::mem::forget(self);
    }
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

pub fn start_totals(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let mut engine = InstancyEngine::new(sc);
    engine
        .spawn_graph::<u64, _>(sc, 0, &mode, |b, hooks| {
            let totals = build_totals(b, hooks)?;
            observe(totals, 0, "totals", hooks)
        })
        .expect("spawn totals");
    Box::new(engine)
}

pub fn start_product(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let mut engine = InstancyEngine::new(sc);
    engine
        .spawn_graph::<Product<u64, u64>, _>(sc, 0, &mode, |b, hooks| {
            let totals = build_product_totals(b, hooks)?;
            observe(totals, 0, "totals", hooks)
        })
        .expect("spawn product totals");
    Box::new(engine)
}

/// Nested feedback with notification; with `graphs > 1`, one `spawn_multi`
/// dataflow per graph on the same runtime and logical worker count.
pub fn start_nested(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let mut engine = InstancyEngine::new(sc);
    for g in 0..sc.graphs {
        let probes = (
            sc.probe_index(g, "inner-totals"),
            sc.probe_index(g, "root-totals"),
        );
        let outer = sc.outer_iterations;
        engine
            .spawn_graph::<u64, _>(sc, g, &mode, move |b, hooks| {
                build_nested(b, hooks, probes, outer)
            })
            .expect("spawn nested");
    }
    Box::new(engine)
}

// ---------------------------------------------------------------------------
// Operator breadth
// ---------------------------------------------------------------------------

/// Per-time totals on the worker that holds the records (no exchange).
fn local_totals<T: Timestamp + Coords>(
    pipe: Pipe<T, Out>,
    name: &str,
    capacity: usize,
) -> Pipe<T, Out> {
    cap(pipe, capacity).unary_notify(name.to_string(), {
        let mut stash: BTreeMap<Vec<u64>, Sums> = BTreeMap::new();
        move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                let sums = stash.entry(time.to_coords()).or_default();
                for (_, p) in &data {
                    let e = sums.entry(p[0] as u64).or_insert((0, 0));
                    e.0 += p[1];
                    e.1 += 1;
                }
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(sums) = stash.remove(&time.to_coords()) {
                    let records = total_records(&time, sums);
                    output.push_vec(time, records);
                }
            }
            Ok(())
        }
    })
}

fn to_out<T: Timestamp>(pipe: Pipe<T, Rec>, name: &str, capacity: usize) -> Pipe<T, Out> {
    cap(pipe, capacity).map(name.to_string(), |_t, (k, v): Rec| {
        (Vec::new(), vec![k as i64, v])
    })
}

/// Operator breadth, observed at five points:
/// - `broadcast-totals`: broadcast, then per-worker notified totals;
/// - `distinct`: branch on sign, negate one side, merge, flat_map, exchange,
///   `distinct`;
/// - `delayed-reduce`: per-record `delay` (odd keys by one epoch), exchange,
///   `reduce`;
/// - `gathered-count`: those reductions `gather`ed and `count`ed;
/// - `rebalanced-totals`: `rebalance`, map, exchange, notified totals.
pub fn build_breadth(b: &DataflowBuilder<u64>, hooks: &Hooks) -> IResult<()> {
    let c = hooks.capacity;
    let probe = |name: &str, index: usize| (name.to_string(), index);
    let base = hooks.probe_base();
    let input = cap(b.input::<Rec>("in")?, c);
    let (a, rest) = input.fork("fan-1");
    let (bb, rest) = cap(rest, c).fork("fan-2");
    let (d, e) = cap(rest, c).fork("fan-3");

    let broadcast = cap(a, c).broadcast("broadcast");
    let totals = local_totals(to_out(broadcast, "broadcast-out", c), "broadcast-totals", c);
    let (name, p) = probe("broadcast-totals", base);
    observe(totals, p, &name, hooks)?;

    let (pos, neg) = cap(bb, c).branch("sign", |_t, r: &Rec| r.1 >= 0);
    let neg = cap(neg, c).map("negate", |_t, (k, v): Rec| (k, -v));
    let merged = cap(pos, c).merge(cap(neg, c))?;
    let spread = cap(merged, c).flat_map("spread", |_t, (k, v): Rec| vec![(k, v), (k, v % 3)]);
    let distinct = cap(
        exchange_out(to_out(spread, "spread-out", c), "distinct", c),
        c,
    )
    .distinct("distinct");
    let distinct = cap(distinct, c).map("distinct-time", |t: &u64, (_, p): Out| (t.to_coords(), p));
    let (name, p) = probe("distinct", base + 1);
    observe(distinct, p, &name, hooks)?;

    let delayed = cap(d, c).delay("delay-odd", |t: &u64, r: &Rec| t + r.0 % 2);
    let reduced = cap(cap(delayed, c).exchange_by_hash("reduce-by-key", route), c)
        .reduce("reduce", |a: Rec, b: Rec| (a.0.max(b.0), a.1 + b.1));
    let (shown, counted) = cap(reduced, c).fork("reduce-fork");
    let shown = cap(shown, c).map("reduce-out", |t: &u64, (k, v): Rec| {
        (t.to_coords(), vec![k as i64, v])
    });
    let (name, p) = probe("delayed-reduce", base + 2);
    observe(shown, p, &name, hooks)?;
    let count = cap(cap(counted, c).gather("gather"), c).count("count");
    let count = cap(count, c).map("count-out", |t: &u64, n: usize| {
        (t.to_coords(), vec![n as i64])
    });
    let (name, p) = probe("gathered-count", base + 3);
    observe(count, p, &name, hooks)?;

    let spread =
        cap(cap(e, c).rebalance("rebalance"), c).map("double", |_t, (k, v): Rec| (k, 2 * v));
    let totals = notify_totals(
        to_out(spread, "rebalanced-out", c),
        "rebalanced-totals",
        c,
        hooks.receipts(),
        |_| true,
    );
    let (name, p) = probe("rebalanced-totals", base + 4);
    observe(totals, p, &name, hooks)
}

// ---------------------------------------------------------------------------
// Random programs
// ---------------------------------------------------------------------------

use crate::programs::{
    PRec, Program, Stage, add_value, duplicate, hops_at, negate, rotate_key, route_program,
    with_hops,
};

/// A non-loop stage at any scope depth.
fn program_stage<T: ExTime>(
    p: Pipe<T, PRec>,
    stage: &Stage,
    c: usize,
    id: &mut usize,
) -> Pipe<T, PRec> {
    *id += 1;
    let n = |what: &str| format!("{what}-{id}");
    let p = cap(p, c);
    match stage {
        Stage::AddValue(x) => {
            let x = *x;
            p.map(n("add"), move |_t, r: PRec| add_value(r, x))
        }
        Stage::RotateKey(x) => {
            let x = *x;
            p.map(n("rotate"), move |_t, r: PRec| rotate_key(r, x))
        }
        Stage::KeepValueAtLeast(x) => {
            let x = *x;
            p.filter(n("keep"), move |_t, r: &PRec| r.1.0 >= x)
        }
        Stage::Exchange => p.exchange_by_hash(n("exchange"), route_program),
        Stage::Duplicate => p.flat_map(n("duplicate"), |_t, r: PRec| duplicate(r)),
        Stage::BranchNegate => {
            let (pos, neg) = p.branch(n("sign"), |_t, r: &PRec| r.1.0 >= 0);
            let neg = cap(neg, c).map(n("negate"), |_t, r: PRec| negate(r));
            cap(pos, c).merge(cap(neg, c)).expect("merge")
        }
        Stage::Broadcast => p.broadcast(n("broadcast")),
        Stage::DelayOddKeys | Stage::Loop { .. } => {
            unreachable!("handled at the scope level: {stage:?}")
        }
    }
}

/// Close one loop level: count the iteration, feed back while below
/// `iterations`, exit otherwise.
fn loop_result<T: Timestamp>(
    body: Pipe<T, PRec>,
    depth: u32,
    iterations: u64,
    c: usize,
    id: &mut usize,
) -> IterateResult<T, PRec> {
    *id += 1;
    let counted = cap(body, c).map(format!("count-hops-{id}"), move |_t, r: PRec| {
        (
            r.0,
            (r.1.0, with_hops(r.1.1, depth, hops_at(r.1.1, depth) + 1)),
        )
    });
    let (again, done) = cap(counted, c).fork(format!("loop-split-{id}"));
    IterateResult {
        feedback: cap(again, c).filter(format!("again-{id}"), move |_t, r: &PRec| {
            hops_at(r.1.1, depth) < iterations
        }),
        output: cap(
            cap(done, c).filter(format!("done-{id}"), move |_t, r: &PRec| {
                hops_at(r.1.1, depth) >= iterations
            }),
            c,
        ),
    }
}

fn reset_hops<T: Timestamp>(
    p: Pipe<T, PRec>,
    depth: u32,
    c: usize,
    id: &mut usize,
) -> Pipe<T, PRec> {
    *id += 1;
    cap(p, c).map(format!("reset-hops-{id}"), move |_t, r: PRec| {
        (r.0, (r.1.0, with_hops(r.1.1, depth, 0)))
    })
}

type L1 = Product<u64, u32>;

fn program_inner(
    p: Pipe<L1, PRec>,
    iterations: u64,
    body: &[Stage],
    c: usize,
    id: &mut usize,
) -> Pipe<L1, PRec> {
    let entered = cap(reset_hops(p, 1, c, id), c);
    entered.iterate::<u32>(format!("loop1-{id}"), 1u32, |v| {
        let mut q = v;
        for s in body {
            q = program_stage(q, s, c, id);
        }
        loop_result(q, 1, iterations, c, id)
    })
}

fn program_outer(
    p: Pipe<u64, PRec>,
    iterations: u64,
    body: &[Stage],
    c: usize,
    id: &mut usize,
) -> Pipe<u64, PRec> {
    let entered = cap(reset_hops(p, 0, c, id), c);
    entered.iterate::<u32>(format!("loop0-{id}"), 1u32, |v| {
        let mut q = v;
        for s in body {
            q = match s {
                Stage::Loop { iterations, body } => program_inner(q, *iterations, body, c, id),
                _ => program_stage(q, s, c, id),
            };
        }
        loop_result(q, 0, iterations, c, id)
    })
}

/// A random program: entry receipts, the stages, then an ungated `pass`
/// observation of every record and gated per-key `totals`.
pub fn build_program(b: &DataflowBuilder<u64>, hooks: &Hooks, program: &Program) -> IResult<()> {
    let c = hooks.capacity;
    let receipts = hooks.receipts();
    let mut id = 0;
    let mut p = cap(b.input::<Rec>("in")?, c).map("entry", move |_t, (k, v): Rec| {
        receipts.add(1);
        (k, (v, 0u64))
    });
    for stage in &program.stages {
        p = match stage {
            Stage::Loop { iterations, body } => program_outer(p, *iterations, body, c, &mut id),
            Stage::DelayOddKeys => cap(p, c).delay("delay-odd", |t: &u64, r: &PRec| t + r.0 % 2),
            _ => program_stage(p, stage, c, &mut id),
        };
    }
    let (pass, totals) = cap(p, c).fork("observe");
    let pass = cap(pass, c).map("pass-out", |t: &u64, r: PRec| {
        (t.to_coords(), vec![r.0 as i64, r.1.0, r.1.1 as i64])
    });
    observe(pass, hooks.probe_base(), "pass", hooks)?;
    let totals = cap(totals, c).map("totals-in", |_t, r: PRec| {
        (Vec::new(), vec![r.0 as i64, r.1.0])
    });
    let totals = notify_totals(totals, "totals", c, Receipts(None), |_| false);
    observe(totals, hooks.probe_base() + 1, "totals", hooks)
}

pub fn start_breadth(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let mut engine = InstancyEngine::new(sc);
    for g in 0..sc.graphs {
        let base = sc.probe_index(g, "broadcast-totals");
        engine
            .spawn_graph::<u64, _>(sc, g, &mode, move |b, hooks| {
                hooks.set_probe_base(base);
                build_breadth(b, hooks)
            })
            .expect("spawn breadth");
    }
    Box::new(engine)
}

pub fn start_program(sc: &Scenario, mode: Observe) -> Box<dyn Engine> {
    let mut engine = InstancyEngine::new(sc);
    let program = sc.program().clone();
    let base = sc.probe_index(0, "pass");
    engine
        .spawn_graph::<u64, _>(sc, 0, &mode, move |b, hooks| {
            hooks.set_probe_base(base);
            build_program(b, hooks, &program)
        })
        .expect("spawn program");
    Box::new(engine)
}
