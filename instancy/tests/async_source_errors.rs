//! A `source_async` producer that returns `Err` fails its dataflow with the
//! producer's error in the source's operator context; it does not end the
//! input as if the producer had finished.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use instancy::{
    AsyncInputSender, CancellationReason, DataflowBuilder, Error, Result, RuntimeConfig,
    RuntimeHandle, SpawnOptions,
};

const TIMEOUT: Duration = Duration::from_secs(20);

fn within<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> Option<R> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(TIMEOUT).ok()
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// A signal a producer (any clone of it) gives once, read by the test.
fn signal() -> (Arc<Mutex<mpsc::Sender<()>>>, mpsc::Receiver<()>) {
    let (tx, rx) = mpsc::channel();
    (Arc::new(Mutex::new(tx)), rx)
}

fn give(signal: &Mutex<mpsc::Sender<()>>) {
    let _ = signal.lock().unwrap().send(());
}

fn producer_error(text: &str) -> Error {
    Error::Io(std::io::Error::other(text.to_string()))
}

/// `producer -> out`: the producer sends `prefix` (if any), then returns
/// `Ok`, or an I/O error with the text `fails`.
fn build(prefix: Vec<u64>, fails: Option<&'static str>) -> instancy::LogicalDataflow<u64> {
    let builder = DataflowBuilder::<u64>::new("async-source-error");
    builder
        .source_async::<u64, _, _>("producer", move |sender| async move {
            if !prefix.is_empty() {
                sender.send(0, prefix).await?;
            }
            match fails {
                Some(text) => Err(Error::Io(std::io::Error::other(text))),
                None => Ok(()),
            }
        })
        .output("out")
        .unwrap();
    builder.build().unwrap()
}

/// The join result of `handle`, with what its output delivered (consumed
/// throughout, so the source never waits on a full output).
fn run(mut handle: instancy::SpawnedDataflow<u64>) -> (Result<()>, Vec<u64>) {
    let output = handle.take_output::<u64>("out").unwrap();
    let consumer = std::thread::spawn(move || {
        output
            .collect_data()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect::<Vec<u64>>()
    });
    let result = within(move || handle.join_blocking()).expect("join never returned");
    (result, consumer.join().unwrap())
}

fn assert_producer_failed(result: &Result<()>, text: &str, what: &str) {
    match result {
        Err(Error::Operator {
            operator,
            worker_index: Some(_),
            source,
        }) => {
            assert_eq!(operator, "producer", "{what}: the failing operator");
            assert!(
                matches!(source.downcast_ref::<Error>(), Some(Error::Io(e)) if e.to_string() == text),
                "{what}: the producer's own error, got {source}"
            );
        }
        other => panic!("{what}: expected the producer's error, got {other:?}"),
    }
}

/// The three outcomes on `spawn`: an error after a prefix, an error before
/// any record, and a producer that succeeds.
fn producer_outcomes_on(
    spawn: impl Fn(instancy::LogicalDataflow<u64>) -> instancy::SpawnedDataflow<u64>,
    what: &str,
) {
    let text = "the producer failed after its prefix";
    let (result, _) = run(spawn(build(vec![1, 2, 3], Some(text))));
    assert_producer_failed(&result, text, &format!("{what}, after a prefix"));

    let text = "the producer failed before any record";
    let (result, _) = run(spawn(build(Vec::new(), Some(text))));
    assert_producer_failed(&result, text, &format!("{what}, before any record"));

    let (result, delivered) = run(spawn(build(vec![1, 2, 3], None)));
    assert!(result.is_ok(), "{what}: a successful producer: {result:?}");
    assert_eq!(
        delivered,
        vec![1, 2, 3],
        "{what}: a successful producer's records"
    );
}

#[test]
fn a_producer_error_fails_a_runtime_handle_dataflow() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();
    producer_outcomes_on(
        |dataflow| rt.spawn(dataflow, SpawnOptions::new()).unwrap(),
        "RuntimeHandle",
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn a_producer_error_fails_a_simple_runtime_dataflow() {
    let rt = instancy::SimpleRuntime::new();
    producer_outcomes_on(|dataflow| rt.spawn(dataflow).unwrap(), "SimpleRuntime");
}

/// `producer -> out` with `producer` as the async source's producer.
fn dataflow<F, Fut>(producer: F) -> instancy::LogicalDataflow<u64>
where
    F: FnOnce(AsyncInputSender<u64, u64>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let builder = DataflowBuilder::<u64>::new("async-source-error");
    builder
        .source_async::<u64, _, _>("producer", producer)
        .output("out")
        .unwrap();
    builder.build().unwrap()
}

/// A producer that closes its sender and only then returns an error: the
/// closed channel does not end the input, the error fails the dataflow.
#[test]
fn a_producer_that_closes_its_sender_before_failing_fails_the_dataflow() {
    let rt = runtime();
    let (closed, closed_rx) = signal();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let handle = rt
        .spawn(
            dataflow(move |sender| {
                let release = release.lock().unwrap().take();
                async move {
                    sender.send(0, vec![1]).await?;
                    sender.close();
                    give(&closed);
                    if let Some(release) = release {
                        let _ = release.await;
                    }
                    Err(producer_error("failed after closing its sender"))
                }
            }),
            SpawnOptions::new(),
        )
        .unwrap();
    closed_rx
        .recv_timeout(TIMEOUT)
        .expect("the producer never closed its sender");
    // Only now may the producer return.
    release_tx.send(()).unwrap();
    let (result, _) = run(handle);
    assert_producer_failed(&result, "failed after closing its sender", "closed first");
}

/// A producer error fails the dataflow while a clone of the producer's
/// sender is still alive (its channel never closes).
#[test]
fn a_producer_error_fails_the_dataflow_while_a_sender_clone_lives() {
    let rt = runtime();
    let (clone_tx, clone_rx) = mpsc::channel::<AsyncInputSender<u64, u64>>();
    let clone_tx = Arc::new(Mutex::new(clone_tx));
    let handle = rt
        .spawn(
            dataflow(move |sender| async move {
                sender.send(0, vec![1]).await?;
                let _ = clone_tx.lock().unwrap().send(sender.clone());
                Err(producer_error("failed with a sender clone alive"))
            }),
            SpawnOptions::new(),
        )
        .unwrap();
    let kept = clone_rx
        .recv_timeout(TIMEOUT)
        .expect("the producer never handed out its clone");
    let (result, _) = run(handle);
    assert_producer_failed(&result, "failed with a sender clone alive", "clone alive");
    drop(kept);
}

/// What a producer does once the drain refuses its send.
#[derive(Clone, Copy, Debug)]
enum OnRefusal {
    /// Returns the refusal with `?`.
    Propagate,
    /// Returns an error of its own.
    Fail,
    /// Panics.
    Panic,
}

/// What a producer's sends saw: the drain refusing one with
/// `ChannelClosed`, or (a fixture failure) none refused before the deadline.
#[derive(Default)]
struct Refusal {
    channel_closed: std::sync::atomic::AtomicBool,
    timed_out: std::sync::atomic::AtomicBool,
}

/// Sends until a send is refused, within one poll of the producer: each
/// fresh send future is polled once (a pending one is dropped, enqueueing
/// nothing), so the producer never yields between the drain closing its
/// input and its result. `accepted` counts the enqueued sends; `refusal`
/// records how the loop ended.
fn send_until_refused(
    sender: &AsyncInputSender<u64, u64>,
    accepted: &AtomicUsize,
    refusal: &Refusal,
) -> Result<()> {
    let deadline = Instant::now() + TIMEOUT;
    let mut next = 0u64;
    loop {
        let mut send = std::pin::pin!(sender.send(0, vec![next]));
        match send.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(())) => {
                accepted.fetch_add(1, Ordering::SeqCst);
                next += 1;
            }
            Poll::Ready(Err(error)) => {
                if matches!(error, Error::ChannelClosed) {
                    refusal.channel_closed.store(true, Ordering::SeqCst);
                }
                return Err(error);
            }
            Poll::Pending => {}
        }
        if Instant::now() > deadline {
            refusal.timed_out.store(true, Ordering::SeqCst);
            return Ok(());
        }
    }
}

/// A drain that refuses a producer's send within the producer's one poll:
/// the producer's error (the refusal, or one of its own) is classified as
/// the cancellation and the drain succeeds with every accepted record; a
/// panic still fails the drain.
#[test]
fn a_drain_that_refuses_a_send_is_a_cancellation_not_a_failure() {
    for on_refusal in [OnRefusal::Propagate, OnRefusal::Fail, OnRefusal::Panic] {
        let what = format!("{on_refusal:?}");
        let rt = runtime();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let refusal = Arc::new(Refusal::default());
        let seen = Arc::clone(&refusal);
        let (entered, entered_rx) = signal();
        let mut handle = rt
            .spawn(
                dataflow(move |sender| async move {
                    give(&entered);
                    match on_refusal {
                        OnRefusal::Propagate => send_until_refused(&sender, &counter, &seen)?,
                        OnRefusal::Fail => {
                            if send_until_refused(&sender, &counter, &seen).is_err() {
                                return Err(producer_error("its own error after the refusal"));
                            }
                        }
                        OnRefusal::Panic => {
                            if send_until_refused(&sender, &counter, &seen).is_err() {
                                panic!("the producer panicked after the refusal");
                            }
                        }
                    }
                    Ok(())
                }),
                SpawnOptions::new().drain_on_cancel(Duration::from_secs(60)),
            )
            .unwrap();
        let output = handle.take_output::<u64>("out").unwrap();
        let consumer = std::thread::spawn(move || {
            output
                .collect_data()
                .iter()
                .map(|(_, data)| data.len())
                .sum::<usize>()
        });
        entered_rx
            .recv_timeout(TIMEOUT)
            .unwrap_or_else(|_| panic!("{what}: the producer never started"));
        handle.cancel();
        let result = within(move || handle.join_blocking())
            .unwrap_or_else(|| panic!("{what}: the drain never finished"));
        let delivered = consumer.join().unwrap();
        assert!(
            !refusal.timed_out.load(Ordering::SeqCst),
            "{what}: fixture failure: the drain refused no send before the deadline"
        );
        assert!(
            refusal.channel_closed.load(Ordering::SeqCst),
            "{what}: the producer's result followed the drain refusing a send (ChannelClosed)"
        );
        match on_refusal {
            OnRefusal::Propagate | OnRefusal::Fail => {
                assert!(result.is_ok(), "{what}: the drain must succeed: {result:?}");
                assert_eq!(
                    delivered,
                    accepted.load(Ordering::SeqCst),
                    "{what}: every accepted record is delivered"
                );
            }
            OnRefusal::Panic => assert_ended_without_result(&result, &what),
        }
    }
}

/// A producer that panicked: its source fails, named.
fn assert_ended_without_result(result: &Result<()>, what: &str) {
    match result {
        Err(Error::Operator {
            operator, source, ..
        }) => {
            assert_eq!(operator, "producer", "{what}: the failing operator");
            assert!(
                source
                    .to_string()
                    .contains("producer ended without returning a result"),
                "{what}: {source}"
            );
        }
        other => panic!("{what}: expected the source to fail, got {other:?}"),
    }
}

/// A producer that dropped its sender and never returns keeps its source
/// unfinished until the dataflow is cancelled, which then ends it (with a
/// drain) or stops it.
#[test]
fn a_cancellation_resolves_a_producer_that_never_returns() {
    for drain in [true, false] {
        let rt = runtime();
        let (dropped, dropped_rx) = signal();
        let mut options = SpawnOptions::new();
        if drain {
            options = options.drain_on_cancel(Duration::from_secs(60));
        }
        let handle = rt
            .spawn(
                dataflow(move |sender| async move {
                    sender.send(0, vec![1]).await?;
                    drop(sender);
                    give(&dropped);
                    std::future::pending::<()>().await;
                    Ok(())
                }),
                options,
            )
            .unwrap();
        dropped_rx
            .recv_timeout(TIMEOUT)
            .expect("the producer never dropped its sender");
        handle.cancel();
        let (result, _) = run(handle);
        if drain {
            assert!(result.is_ok(), "drain: {result:?}");
        } else {
            assert!(
                matches!(
                    result,
                    Err(Error::Cancelled {
                        reason: Some(CancellationReason::UserRequested)
                    })
                ),
                "no drain: {result:?}"
            );
        }
    }
}

/// A producer that panics fails its dataflow, naming the source; it does
/// not end the input or leave the dataflow waiting.
#[test]
fn a_producer_that_panics_fails_the_dataflow() {
    let rt = runtime();
    let handle = rt
        .spawn(
            dataflow(|sender| async move {
                sender.send(0, vec![1]).await?;
                panic!("the producer panicked");
            }),
            SpawnOptions::new(),
        )
        .unwrap();
    let (result, _) = run(handle);
    assert_ended_without_result(&result, "panic");
}

/// A running producer that panics while a drain stops it (a value it holds
/// panics when dropped) fails the drain: the cancellation does not turn the
/// panic into an end of input.
#[test]
fn a_producer_that_panics_while_a_drain_stops_it_fails_the_drain() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("the producer panicked while it was stopped");
        }
    }
    let rt = runtime();
    let (running, running_rx) = signal();
    let handle = rt
        .spawn(
            dataflow(move |sender| async move {
                let _guard = PanicOnDrop;
                sender.send(0, vec![1]).await?;
                give(&running);
                std::future::pending::<()>().await;
                Ok(())
            }),
            SpawnOptions::new().drain_on_cancel(Duration::from_secs(60)),
        )
        .unwrap();
    running_rx
        .recv_timeout(TIMEOUT)
        .expect("the producer never ran");
    handle.cancel();
    let (result, _) = run(handle);
    assert_ended_without_result(&result, "a panic while a drain stops the producer");
}

/// A producer whose destruction blocks does not hold its dataflow past the
/// drain's deadline: the join returns cancelled while the destruction is
/// still blocked (the pump thread outlives the join; the deadline does not
/// end the destruction).
#[test]
fn a_drain_deadline_ends_a_dataflow_whose_producer_blocks_while_stopped() {
    struct BlockOnDrop {
        entered: Arc<Mutex<mpsc::Sender<()>>>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
        exited: Arc<Mutex<mpsc::Sender<()>>>,
    }
    impl Drop for BlockOnDrop {
        fn drop(&mut self) {
            give(&self.entered);
            // Until the test releases it (or its sender is gone): no timeout.
            let _ = self.release.lock().unwrap().recv();
            give(&self.exited);
        }
    }
    let rt = runtime();
    let (running, running_rx) = signal();
    let (entered, entered_rx) = signal();
    let (exited, exited_rx) = signal();
    let (release, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let mut handle = rt
        .spawn(
            dataflow(move |sender| async move {
                let _guard = BlockOnDrop {
                    entered,
                    release: release_rx,
                    exited,
                };
                sender.send(0, vec![1]).await?;
                give(&running);
                std::future::pending::<()>().await;
                Ok(())
            }),
            SpawnOptions::new().drain_on_cancel(Duration::from_millis(200)),
        )
        .unwrap();
    let output = handle.take_output::<u64>("out").unwrap();
    let consumer = std::thread::spawn(move || output.collect_data().len());
    running_rx
        .recv_timeout(TIMEOUT)
        .expect("the producer never ran");
    handle.cancel();
    let entered = entered_rx.recv_timeout(TIMEOUT);
    // The destruction stays blocked (`release` held) while the join runs.
    let result = within(move || handle.join_blocking());
    // Whatever happened: release the destruction and wait for it to end.
    drop(release);
    let exited = exited_rx.recv_timeout(TIMEOUT);
    entered.expect("the producer's destruction never started");
    exited.expect("the producer's destruction never ended once released");
    let result = result.expect("join never returned while the producer's destruction blocked");
    consumer.join().unwrap();
    assert!(
        matches!(
            result,
            Err(Error::Cancelled {
                reason: Some(CancellationReason::UserRequested)
            })
        ),
        "the drain's deadline ends the dataflow as cancelled: {result:?}"
    );
}

/// One worker's producer error stops its siblings at once (their producers
/// are cancelled), before the aggregate join is even polled.
fn one_failing_worker_stops_its_siblings_on(
    spawn: impl FnOnce(
        Box<dyn Fn(&mut DataflowBuilder<u64>) -> Result<()>>,
    ) -> instancy::MultiSpawnedDataflow<u64>,
    what: &str,
) {
    struct SignalOnDrop(Arc<Mutex<mpsc::Sender<()>>>);
    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            give(&self.0);
        }
    }
    let started = Arc::new(AtomicUsize::new(0));
    let sibling_running = Arc::new(tokio::sync::Notify::new());
    let (sibling_stopped, sibling_stopped_rx) = signal();
    let build = move |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let started = Arc::clone(&started);
        let sibling_running = Arc::clone(&sibling_running);
        let sibling_stopped = Arc::clone(&sibling_stopped);
        builder
            .source_async::<u64, _, _>("producer", move |sender| async move {
                if started.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Fail only once the sibling's producer runs.
                    sibling_running.notified().await;
                    return Err(producer_error("one worker's producer failed"));
                }
                let _stopped = SignalOnDrop(sibling_stopped);
                sender.send(0, vec![1]).await?;
                sibling_running.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            })
            .output("out")?;
        Ok(())
    };
    let multi = spawn(Box::new(build));
    let stopped = sibling_stopped_rx.recv_timeout(TIMEOUT);
    // Whatever happened: stop everything before asserting.
    multi.cancel();
    let result = within(move || multi.join_blocking());
    stopped.unwrap_or_else(|_| panic!("{what}: the sibling's producer was never stopped"));
    let result = result.unwrap_or_else(|| panic!("{what}: join never returned"));
    assert_producer_failed(&result, "one worker's producer failed", what);
}

#[test]
fn one_failing_worker_stops_its_siblings_on_a_runtime_handle() {
    let rt = runtime();
    one_failing_worker_stops_its_siblings_on(
        |build| {
            rt.spawn_multi("one-fails", 2, build, SpawnOptions::new())
                .unwrap()
        },
        "RuntimeHandle",
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn one_failing_worker_stops_its_siblings_on_a_simple_runtime() {
    let rt = instancy::SimpleRuntime::new();
    one_failing_worker_stops_its_siblings_on(
        |build| rt.spawn_multi("one-fails", 2, build).unwrap(),
        "SimpleRuntime",
    );
}
