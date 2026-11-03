//! A `unary_async` operator runs its tasks on the Tokio runtime its builder
//! captured. When that runtime shuts down under a running dataflow, the
//! dataflow fails with an error naming the operator instead of waiting for
//! results that will never arrive. A live runtime still delivers every
//! result in completion order, and a drain still waits for running tasks.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::{
    CancellationToken, DataflowBuilder, Error, InputSender, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

/// Bound of each observation (diagnoses liveness; never orders events).
const BOUND: Duration = Duration::from_secs(20);

/// The dataflow's own runtime.
fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// The runtime the builder runs in (and `unary_async` captures), distinct
/// from the dataflow's.
fn host() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("unary-async-host")
        .enable_all()
        .build()
        .unwrap()
}

/// A signal carrying a batch time, given by any clone.
type Signal = Arc<Mutex<mpsc::Sender<u64>>>;

fn signal() -> (Signal, mpsc::Receiver<u64>) {
    let (tx, rx) = mpsc::channel();
    (Arc::new(Mutex::new(tx)), rx)
}

fn give(signal: &Signal, time: u64) {
    let _ = signal.lock().unwrap().send(time);
}

/// Gives its batch time when dropped: the task's future was destroyed.
struct DropSignal(Signal, u64);

impl Drop for DropSignal {
    fn drop(&mut self) {
        give(&self.0, self.1);
    }
}

/// One gate per batch time; dropping them opens them all, so every path
/// releases the tasks.
struct Gates(Vec<Arc<tokio::sync::Notify>>);

impl Gates {
    fn new(count: usize) -> Self {
        Gates((0..count).map(|_| Arc::default()).collect())
    }

    fn open(&self, time: u64) {
        self.0[time as usize].notify_one();
    }
}

impl Drop for Gates {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.notify_one();
        }
    }
}

type Task = Pin<Box<dyn Future<Output = Result<Vec<u64>>> + Send>>;

/// A task per batch that gives `entered`, waits for the gate of its time,
/// then returns its batch; its destruction gives `dropped`.
fn gated(
    gates: &Gates,
    entered: Signal,
    dropped: Signal,
) -> impl Fn(u64, Vec<u64>) -> Task + Send + Sync + 'static {
    let gates = gates.0.clone();
    move |time, batch| {
        let gate = Arc::clone(&gates[time as usize]);
        let (entered, dropped) = (Arc::clone(&entered), Arc::clone(&dropped));
        Box::pin(async move {
            let _dropped = DropSignal(dropped, time);
            give(&entered, time);
            gate.notified().await;
            Ok(batch)
        })
    }
}

/// `in -> unary_async("lookup") -> out`, built while `host` is entered.
fn dataflow<F, Fut>(
    host: &tokio::runtime::Handle,
    idle_sweeps: Option<usize>,
    logic: F,
) -> instancy::LogicalDataflow<u64>
where
    F: Fn(u64, Vec<u64>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u64>>> + Send + 'static,
{
    let _entered = host.enter();
    let builder = DataflowBuilder::<u64>::new("unary-async-lifetime");
    if let Some(sweeps) = idle_sweeps {
        builder.max_idle_sweeps(sweeps);
    }
    builder
        .input::<u64>("in")
        .unwrap()
        .unary_async("lookup", 4, Arc::new(logic))
        .output("out")
        .unwrap();
    builder.build().unwrap()
}

/// What the output delivered.
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Data(u64, Vec<u64>),
    Frontier(u64),
}

/// A spawned dataflow: its join runs on its own thread, and its output is
/// read on another (each event forwarded, then `None` once it closed).
struct Running {
    cancel: CancellationToken,
    join: mpsc::Receiver<Result<()>>,
    events: mpsc::Receiver<Option<Seen>>,
    seen: Vec<Seen>,
    closed: bool,
}

fn spawn(
    rt: &RuntimeHandle,
    dataflow: instancy::LogicalDataflow<u64>,
    options: SpawnOptions,
) -> (Running, InputSender<u64, u64>) {
    let mut handle = rt.spawn(dataflow, options).unwrap();
    let input = handle.take_input::<u64>("in").unwrap();
    let output = handle.take_output::<u64>("out").unwrap();
    let (tx, events) = mpsc::channel();
    std::thread::spawn(move || {
        while let Some(event) = output.recv() {
            let seen = match event {
                OutputEvent::Data { time, data } => Seen::Data(time, data),
                OutputEvent::Frontier(time) => Seen::Frontier(time),
            };
            if tx.send(Some(seen)).is_err() {
                return;
            }
        }
        let _ = tx.send(None);
    });
    let cancel = handle.cancel_token().clone();
    let (tx, join) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(handle.join_blocking());
    });
    let running = Running {
        cancel,
        join,
        events,
        seen: Vec::new(),
        closed: false,
    };
    (running, input)
}

/// How a dataflow ended.
#[derive(Debug)]
struct Finished {
    /// The join within the bound (`None`: unresolved).
    first: Option<Result<()>>,
    /// Only if unresolved: the join after a cleanup cancellation.
    cleanup: Option<Option<Result<()>>>,
    seen: Vec<Seen>,
    /// Whether the output closed.
    closed: bool,
}

impl Finished {
    fn data(&self) -> Vec<(u64, Vec<u64>)> {
        let mut data: Vec<_> = self
            .seen
            .iter()
            .filter_map(|seen| match seen {
                Seen::Data(time, data) => Some((*time, data.clone())),
                Seen::Frontier(_) => None,
            })
            .collect();
        data.sort();
        data
    }
}

impl Running {
    /// The next data event by `until`, recording every event.
    fn next_data(&mut self, until: Instant) -> Option<(u64, Vec<u64>)> {
        while !self.closed {
            let left = until.saturating_duration_since(Instant::now());
            match self.events.recv_timeout(left) {
                Ok(Some(seen)) => {
                    self.seen.push(seen.clone());
                    if let Seen::Data(time, data) = seen {
                        return Some((time, data));
                    }
                }
                Ok(None) | Err(mpsc::RecvTimeoutError::Disconnected) => self.closed = true,
                Err(mpsc::RecvTimeoutError::Timeout) => return None,
            }
        }
        None
    }

    /// The join within the bound; if unresolved, cancels and records the
    /// cleanup join separately. Then reads the output until it closes.
    fn finish(mut self) -> Finished {
        let first = self.join.recv_timeout(BOUND).ok();
        let cleanup = first.is_none().then(|| {
            self.cancel.cancel();
            self.join.recv_timeout(BOUND).ok()
        });
        let until = Instant::now() + BOUND;
        while self.next_data(until).is_some() {}
        Finished {
            first,
            cleanup,
            seen: self.seen,
            closed: self.closed,
        }
    }
}

/// Whether `input` refused a (frontier-only) admission by `until`: the
/// drain closed the dataflow's inputs.
fn refused(input: &InputSender<u64, u64>, until: Instant) -> bool {
    while Instant::now() < until {
        match input.advance_to(0) {
            Err(Error::ChannelClosed) => return true,
            Err(other) => panic!("unexpected admission error: {other:?}"),
            Ok(()) => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    false
}

/// The dataflow failed because a task of `lookup` ended without its result
/// (its runtime shut down): named, with the task's cancellation as cause.
fn assert_host_lost(done: &Finished, what: &str) {
    match &done.first {
        Some(Err(Error::Operator {
            operator,
            worker_index: Some(_),
            source,
        })) => {
            assert_eq!(operator, "lookup", "{what}: the failing operator");
            let cause = source
                .source()
                .and_then(|cause| cause.downcast_ref::<tokio::task::JoinError>());
            assert!(
                cause.is_some_and(|cause| cause.is_cancelled()),
                "{what}: the cause is the task's cancellation: {source:?}"
            );
        }
        _ => panic!(
            "{what}: expected `lookup` to fail for its lost task, got {:?} \
             (cleanup cancellation: {:?}); {done:?}",
            done.first, done.cleanup
        ),
    }
    assert!(done.closed, "{what}: the output closed");
    assert!(
        done.data().is_empty(),
        "{what}: nothing delivered: {done:?}"
    );
}

/// Control: with its runtime alive, every batch's result arrives at its
/// time.
#[test]
fn a_live_host_delivers_every_result() {
    let rt = runtime();
    let h = host();
    let (run, input) = spawn(
        &rt,
        dataflow(h.handle(), None, |_t, batch: Vec<u64>| async move {
            Ok(batch.into_iter().map(|x| x * 10).collect())
        }),
        SpawnOptions::new(),
    );
    input.send(0, vec![1, 2]).unwrap();
    input.send(1, vec![3]).unwrap();
    drop(input);
    let done = run.finish();
    drop(h);
    assert!(matches!(done.first, Some(Ok(()))), "{done:?}");
    assert_eq!(done.data(), vec![(0, vec![10, 20]), (1, vec![30])]);
}

/// A runtime that shut down before the dataflow ran: no task runs, and the
/// dataflow fails naming the operator instead of waiting.
#[test]
fn a_host_gone_before_the_dataflow_runs_fails_it() {
    let rt = runtime();
    let h = host();
    let ran = Arc::new(AtomicBool::new(false));
    let task_ran = Arc::clone(&ran);
    let df = dataflow(h.handle(), None, move |_t, batch: Vec<u64>| {
        let ran = Arc::clone(&task_ran);
        async move {
            ran.store(true, Ordering::SeqCst);
            Ok(batch)
        }
    });
    drop(h);
    let (run, input) = spawn(&rt, df, SpawnOptions::new());
    input.send(0, vec![1, 2, 3]).unwrap();
    drop(input);
    let done = run.finish();
    assert!(
        !ran.load(Ordering::SeqCst),
        "no task ran on the closed runtime"
    );
    assert_host_lost(&done, "host gone before the run");
}

/// A runtime that shuts down while a task runs destroys it, and the
/// dataflow fails naming the operator, also when its executor parks at
/// once (`max_idle_sweeps(0)`).
#[test]
fn a_host_lost_mid_flight_fails_the_dataflow() {
    for idle_sweeps in [None, Some(0)] {
        let what = format!("max_idle_sweeps {idle_sweeps:?}");
        let rt = runtime();
        let h = host();
        let gates = Gates::new(1);
        let (entered, entered_rx) = signal();
        let (dropped, dropped_rx) = signal();
        let (run, input) = spawn(
            &rt,
            dataflow(h.handle(), idle_sweeps, gated(&gates, entered, dropped)),
            SpawnOptions::new(),
        );
        input.send(0, vec![1, 2, 3]).unwrap();
        drop(input);
        let entered = entered_rx.recv_timeout(BOUND);
        // Outside Tokio; returns once the runtime's workers stopped.
        drop(h);
        let destroyed = dropped_rx.try_recv();
        let done = run.finish();
        drop(gates);
        assert_eq!(entered, Ok(0), "{what}: the task started");
        assert_eq!(destroyed, Ok(0), "{what}: the shutdown destroyed the task");
        assert_host_lost(&done, &what);
    }
}

/// A runtime that shuts down while a drain waits for its task fails the
/// drain: losing the task is a failure, not part of the cancellation.
#[test]
fn a_host_lost_during_a_drain_fails_the_drain() {
    let rt = runtime();
    let h = host();
    let gates = Gates::new(1);
    let (entered, entered_rx) = signal();
    let (dropped, dropped_rx) = signal();
    let (run, input) = spawn(
        &rt,
        dataflow(h.handle(), None, gated(&gates, entered, dropped)),
        SpawnOptions::new().drain_on_cancel(Duration::from_secs(30)),
    );
    input.send(0, vec![1, 2, 3]).unwrap();
    let entered = entered_rx.recv_timeout(BOUND);
    run.cancel.cancel();
    let drain_began = refused(&input, Instant::now() + BOUND);
    drop(h);
    let destroyed = dropped_rx.try_recv();
    let done = run.finish();
    drop(input);
    drop(gates);
    assert_eq!(entered, Ok(0), "the task started");
    assert!(drain_began, "the drain began (an admission was refused)");
    assert_eq!(destroyed, Ok(0), "the shutdown destroyed the task");
    assert_host_lost(&done, "host lost during a drain");
}

/// A drain leaves a running task alone and waits for it: once released,
/// its result arrives and the drain succeeds.
#[test]
fn a_drain_waits_for_a_running_task() {
    let rt = runtime();
    let h = host();
    let gates = Gates::new(1);
    let (entered, entered_rx) = signal();
    let (dropped, dropped_rx) = signal();
    let (run, input) = spawn(
        &rt,
        dataflow(h.handle(), None, gated(&gates, entered, dropped)),
        SpawnOptions::new().drain_on_cancel(Duration::from_secs(60)),
    );
    input.send(0, vec![1, 2, 3]).unwrap();
    let entered = entered_rx.recv_timeout(BOUND);
    run.cancel.cancel();
    let drain_began = refused(&input, Instant::now() + BOUND);
    // A task destroyed by now was aborted by the drain: keep it gated.
    let destroyed_early = dropped_rx.try_recv().ok();
    if drain_began && destroyed_early.is_none() {
        gates.open(0);
    }
    let done = run.finish();
    drop(input);
    drop(gates);
    drop(h);
    assert_eq!(entered, Ok(0), "the task started");
    assert!(drain_began, "the drain began (an admission was refused)");
    assert_eq!(
        destroyed_early, None,
        "the drain left the running task alone"
    );
    assert!(
        matches!(done.first, Some(Ok(()))),
        "the drain succeeds: {done:?}"
    );
    assert_eq!(done.data(), vec![(0, vec![1, 2, 3])]);
}

/// Results arrive in completion order, each at its batch's time, and the
/// output frontier never passes a batch whose task still runs; also when
/// the executor parks at once (`max_idle_sweeps(0)`).
#[test]
fn results_arrive_in_completion_order_at_their_times() {
    for idle_sweeps in [None, Some(0)] {
        let what = format!("max_idle_sweeps {idle_sweeps:?}");
        let rt = runtime();
        let h = host();
        let gates = Gates::new(3);
        let (entered, entered_rx) = signal();
        let (dropped, _dropped_rx) = signal();
        let (mut run, input) = spawn(
            &rt,
            dataflow(h.handle(), idle_sweeps, gated(&gates, entered, dropped)),
            SpawnOptions::new(),
        );
        for time in 0..3 {
            input.send(time, vec![time + 1]).unwrap();
        }
        input.advance_to(3).unwrap();
        drop(input);
        let until = Instant::now() + BOUND;
        let mut started: Vec<u64> = (0..3)
            .map_while(|_| {
                entered_rx
                    .recv_timeout(until.saturating_duration_since(Instant::now()))
                    .ok()
            })
            .collect();
        started.sort();
        let mut order = Vec::new();
        if started == [0, 1, 2] {
            for time in [2, 0, 1] {
                gates.open(time);
                order.push(run.next_data(until));
            }
        }
        let done = run.finish();
        drop(gates);
        drop(h);
        assert_eq!(started, [0, 1, 2], "{what}: every task started");
        assert_eq!(
            order,
            [Some((2, vec![3])), Some((0, vec![1])), Some((1, vec![2]))],
            "{what}: completion order"
        );
        assert!(matches!(done.first, Some(Ok(()))), "{what}: {done:?}");
        for (at, seen) in done.seen.iter().enumerate() {
            if let Seen::Data(time, _) = seen {
                let passed = done.seen[..at]
                    .iter()
                    .any(|before| matches!(before, Seen::Frontier(f) if f >= time));
                assert!(
                    !passed,
                    "{what}: the frontier passed {time} first: {:?}",
                    done.seen
                );
            }
        }
    }
}

/// A task's own error fails the dataflow with that error, naming the
/// operator.
#[test]
fn a_task_error_fails_the_dataflow_with_that_error() {
    let rt = runtime();
    let h = host();
    let (run, input) = spawn(
        &rt,
        dataflow(h.handle(), None, |_t, _batch: Vec<u64>| async move {
            Err::<Vec<u64>, _>(Error::Io(std::io::Error::other("the lookup failed")))
        }),
        SpawnOptions::new(),
    );
    input.send(0, vec![1]).unwrap();
    drop(input);
    let done = run.finish();
    drop(h);
    match &done.first {
        Some(Err(Error::Operator {
            operator, source, ..
        })) => {
            assert_eq!(operator, "lookup");
            assert!(
                matches!(source.downcast_ref::<Error>(), Some(Error::Io(e)) if e.to_string() == "the lookup failed"),
                "the task's own error: {source}"
            );
        }
        _ => panic!("expected the task's error, got {done:?}"),
    }
}

/// A task that panics is reported as the operator's panic.
#[test]
fn a_task_panic_is_reported_as_the_operators_panic() {
    let rt = runtime();
    let h = host();
    let (run, input) = spawn(
        &rt,
        dataflow(h.handle(), None, |_t, _batch: Vec<u64>| async move {
            if true {
                panic!("the lookup panicked");
            }
            Ok(Vec::new())
        }),
        SpawnOptions::new(),
    );
    input.send(0, vec![1]).unwrap();
    drop(input);
    let done = run.finish();
    drop(h);
    match &done.first {
        Some(Err(Error::OperatorPanic {
            operator, message, ..
        })) => {
            assert_eq!(operator, "lookup");
            assert!(message.contains("the lookup panicked"), "{message}");
        }
        _ => panic!("expected the operator's panic, got {done:?}"),
    }
}
