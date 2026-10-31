//! Regression tests for runtime and executor lifecycle robustness:
//! runtime drop/shutdown with in-flight dataflows, external cancellation
//! token bridges, and graceful drain semantics.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use instancy::{
    CancellationReason, DataflowBuilder, DataflowCompletion, Error, IoMode, Result, RuntimeConfig,
    RuntimeHandle, SpawnOptions,
};

const TIMEOUT: Duration = Duration::from_secs(20);

fn runtime(threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: threads,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn within<R: Send + 'static>(
    timeout: Duration,
    f: impl FnOnce() -> R + Send + 'static,
) -> Option<R> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).ok()
}

/// Wait for every completion concurrently; panic if any stays unresolved.
fn wait_all(completions: Vec<DataflowCompletion>) -> Vec<Result<()>> {
    let n = completions.len();
    let (tx, rx) = mpsc::channel();
    for (i, completion) in completions.into_iter().enumerate() {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send((i, completion.wait()));
        });
    }
    drop(tx);
    let deadline = Instant::now() + TIMEOUT;
    let mut results: Vec<Option<Result<()>>> = (0..n).map(|_| None).collect();
    for _ in 0..n {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (i, result) = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("a DataflowCompletion never resolved"));
        results[i] = Some(result);
    }
    results.into_iter().map(Option::unwrap).collect()
}

fn is_runtime_shutdown(result: &Result<()>) -> bool {
    matches!(
        result,
        Err(Error::Cancelled {
            reason: Some(CancellationReason::RuntimeShutdown)
        })
    )
}

fn idle_dataflow(name: &str) -> instancy::LogicalDataflow<u64> {
    let builder = DataflowBuilder::<u64>::new(name);
    builder
        .input::<u64>("data")
        .unwrap()
        .map("id", |_t, v| v)
        .output("results")
        .unwrap();
    builder.build().unwrap()
}

// ---------------------------------------------------------------------------
// Runtime drop / shutdown
// ---------------------------------------------------------------------------

/// Dropping the runtime resolves every pending completion (it races with
/// executors parking, so repeat it).
#[test]
fn runtime_drop_resolves_pending_completions() {
    for round in 0..20 {
        let rt = runtime(2);
        let mut completions = Vec::new();
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for i in 0..8 {
            let mut handle = rt
                .spawn(idle_dataflow(&format!("idle-{i}")), SpawnOptions::new())
                .unwrap();
            senders.push(handle.take_input::<u64>("data").unwrap());
            receivers.push(handle.take_output::<u64>("results").unwrap());
            completions.push(handle.join());
        }
        // Let some executors park and register their wakers, others not.
        std::thread::sleep(Duration::from_millis(round % 4 * 20));
        drop(rt);

        for result in wait_all(completions) {
            assert!(
                is_runtime_shutdown(&result),
                "round {round}: expected RuntimeShutdown, got {result:?}"
            );
        }
        for sender in &senders {
            assert!(
                sender.send(0, vec![1]).is_err(),
                "round {round}: input open"
            );
        }
        for receiver in receivers {
            within(TIMEOUT, move || receiver.collect_data())
                .unwrap_or_else(|| panic!("round {round}: output never closed"));
        }
    }
}

/// Runtime drop with a mix of idle, busy, multi-worker, async, draining, and
/// already-finished dataflows. Every completion resolves; every I/O handle is
/// released, including senders blocked on full channels.
fn drop_runtime_with_mixed_load() {
    let rt = runtime(2);
    let mut completions = Vec::new();
    let mut threads = Vec::new();
    let stop = Arc::new(AtomicBool::new(false));

    // Finished before the drop.
    for i in 0..2 {
        let builder = DataflowBuilder::<u64>::new(format!("done-{i}"));
        builder
            .source("src", vec![(0u64, vec![1u64])])
            .for_each("sink", |_t, _v| {});
        let handle = rt
            .spawn(builder.build().unwrap(), SpawnOptions::new())
            .unwrap();
        completions.push(handle.join());
    }

    // Busy: producers blocked on full input channels, nobody reading output.
    for i in 0..6 {
        let mut handle = rt
            .spawn(idle_dataflow(&format!("busy-{i}")), SpawnOptions::new())
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        threads.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while sender.send(0, vec![i]).is_ok() {
                i += 1;
            }
            drop(receiver);
        }));
        completions.push(handle.join());
    }

    // Busy and consumed: steady flow through the pipeline.
    for i in 0..4 {
        let mut handle = rt
            .spawn(idle_dataflow(&format!("flowing-{i}")), SpawnOptions::new())
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let stop = Arc::clone(&stop);
        threads.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) && sender.send(i, vec![i]).is_ok() {
                i += 1;
            }
        }));
        threads.push(std::thread::spawn(move || {
            receiver.collect_data();
        }));
        completions.push(handle.join());
    }

    // Multi-worker with exchange, inputs open.
    let mut multi_inputs = Vec::new();
    for i in 0..3 {
        let mut multi = rt
            .spawn_multi(
                &format!("multi-{i}"),
                3,
                |builder: &mut DataflowBuilder<u64>| {
                    builder
                        .input::<u64>("data")?
                        .exchange("spread", |v: &u64| *v)
                        .output("results")?;
                    Ok(())
                },
                SpawnOptions::new(),
            )
            .unwrap();
        for w in 0..3 {
            multi_inputs.push(multi.take_input::<u64>(w, "data").unwrap());
        }
        let completion = multi.join();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(completion.wait());
        });
        threads.push(std::thread::spawn(move || {
            let result = rx.recv_timeout(TIMEOUT).expect("multi completion hung");
            assert!(
                result.is_err(),
                "multi-worker dataflow must report shutdown"
            );
        }));
    }

    // Async I/O, idle.
    let mut async_inputs = Vec::new();
    for i in 0..3 {
        let mut handle = rt
            .spawn(
                idle_dataflow(&format!("async-{i}")),
                SpawnOptions::new().io_mode(IoMode::Async),
            )
            .unwrap();
        async_inputs.push(handle.take_async_input::<u64>("data").unwrap());
        completions.push(handle.join());
    }

    // Draining with a long deadline and a slow operator: drop must not wait.
    for i in 0..3 {
        let builder = DataflowBuilder::<u64>::new(format!("draining-{i}"));
        builder
            .input::<u64>("data")
            .unwrap()
            .map("slow", |_t, v| {
                std::thread::sleep(Duration::from_millis(5));
                v
            })
            .output("results")
            .unwrap();
        let mut handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().drain_on_cancel(Duration::from_secs(600)),
            )
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        for v in 0..200 {
            sender.send(0, vec![v]).unwrap();
        }
        handle.cancel();
        completions.push(handle.join());
    }

    std::thread::sleep(Duration::from_millis(50));
    let dropped = Instant::now();
    drop(rt);
    assert!(
        dropped.elapsed() < TIMEOUT,
        "runtime drop blocked for {:?}",
        dropped.elapsed()
    );
    stop.store(true, Ordering::Relaxed);

    for result in wait_all(completions) {
        assert!(
            is_runtime_shutdown(&result) || result.is_ok(),
            "unexpected completion after runtime drop: {result:?}"
        );
    }
    for thread in threads {
        within(TIMEOUT, move || thread.join().unwrap())
            .expect("a producer/consumer stayed blocked after runtime drop");
    }
    for sender in multi_inputs {
        assert!(sender.send(0, vec![1]).is_err());
    }
    let async_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    async_rt.block_on(async move {
        for sender in async_inputs {
            assert!(sender.send(0, vec![1]).await.is_err());
        }
    });
}

#[test]
fn runtime_drop_with_many_in_flight_dataflows() {
    for _ in 0..3 {
        drop_runtime_with_mixed_load();
    }
}

/// Dropping the runtime from inside an async context must not hang or panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_drop_inside_async_context() {
    let rt = runtime(2);
    let mut handle = rt
        .spawn(
            idle_dataflow("async-drop"),
            SpawnOptions::new().io_mode(IoMode::Async),
        )
        .unwrap();
    let sender = handle.take_async_input::<u64>("data").unwrap();
    let mut receiver = handle.take_async_output::<u64>("results").unwrap();
    sender.send(0, vec![1, 2]).await.unwrap();
    let completion = handle.join();
    drop(rt);
    let result = tokio::time::timeout(TIMEOUT, completion)
        .await
        .expect("completion never resolved after runtime drop");
    assert!(
        is_runtime_shutdown(&result) || result.is_ok(),
        "unexpected {result:?}"
    );
    tokio::time::timeout(TIMEOUT, receiver.collect_data())
        .await
        .expect("output never closed");
    assert!(sender.send(0, vec![3]).await.is_err());
}

/// `shutdown_async` waits for every in-flight dataflow; draining ones finish
/// their accepted data because the drain closes their inputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_async_waits_for_every_in_flight_dataflow() {
    let rt = runtime(2);
    let mut completions = Vec::new();
    let mut held = Vec::new();
    let mut drained = Vec::new();
    for i in 0..10 {
        let drain = i % 2 == 0;
        let mut options = SpawnOptions::new();
        if drain {
            options = options.drain_on_cancel(Duration::from_secs(60));
        }
        let mut handle = rt
            .spawn(idle_dataflow(&format!("shutdown-{i}")), options)
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        sender.send(0, vec![i]).unwrap();
        if drain {
            drained.push((i, receiver));
        }
        held.push(sender);
        completions.push((drain, handle.join()));
    }
    let mut multi_completions = Vec::new();
    for i in 0..2 {
        let mut multi = rt
            .spawn_multi(
                &format!("shutdown-multi-{i}"),
                3,
                |builder: &mut DataflowBuilder<u64>| {
                    builder.input::<u64>("data")?.output("results")?;
                    Ok(())
                },
                SpawnOptions::new(),
            )
            .unwrap();
        for w in 0..3 {
            held.push(multi.take_input::<u64>(w, "data").unwrap());
        }
        multi_completions.push(multi.join());
    }

    tokio::time::timeout(TIMEOUT, rt.shutdown_async())
        .await
        .expect("shutdown_async never returned");
    assert_eq!(rt.active_dataflows(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    for (drain, mut completion) in completions {
        let result = match std::pin::Pin::new(&mut completion).poll(&mut cx) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("shutdown_async returned before a dataflow completed"),
        };
        if drain {
            result.expect("drain should finish once inputs are closed");
        } else {
            assert!(is_runtime_shutdown(&result), "unexpected {result:?}");
        }
    }
    for mut completion in multi_completions {
        match std::pin::Pin::new(&mut completion).poll(&mut cx) {
            Poll::Ready(result) => assert!(result.is_err(), "unexpected {result:?}"),
            Poll::Pending => panic!("shutdown_async returned before a worker completed"),
        }
    }
    for (i, receiver) in drained {
        let data: Vec<u64> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        assert_eq!(
            data,
            vec![i],
            "drain must deliver data accepted before shutdown"
        );
    }
    drop(held);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// An owned tokio runtime with no worker threads is a configuration error,
/// reported as one instead of a panic inside tokio.
#[test]
fn owned_tokio_runtime_with_zero_threads_is_an_error_not_a_panic() {
    let result = std::panic::catch_unwind(|| {
        RuntimeHandle::new(RuntimeConfig {
            tokio_mode: instancy::runtime::TokioMode::Create { worker_threads: 0 },
            ..RuntimeConfig::default()
        })
        .map(drop)
    });
    match result {
        Ok(Err(Error::Runtime(instancy::RuntimeError::InvalidConfig(message)))) => {
            assert!(message.contains("worker thread"), "{message}");
        }
        Ok(other) => panic!("expected an InvalidConfig error, got {other:?}"),
        Err(_) => panic!("RuntimeHandle::new panicked"),
    }
}

// ---------------------------------------------------------------------------
// External cancellation token bridge
// ---------------------------------------------------------------------------

#[test]
fn external_token_cancelled_before_spawn_cancels_immediately() {
    let rt = runtime(1);
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let mut handle = rt
        .spawn(
            idle_dataflow("pre-cancelled"),
            SpawnOptions::new().cancellation_token(token),
        )
        .unwrap();
    let _input = handle.take_input::<u64>("data").unwrap();
    let result = within(TIMEOUT, move || handle.join_blocking()).expect("never cancelled");
    assert!(
        matches!(
            result,
            Err(Error::Cancelled {
                reason: Some(CancellationReason::UserRequested)
            })
        ),
        "unexpected {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_cancellation_bridge_tasks_exit_when_dataflows_finish() {
    let rt = runtime(2);
    let app_token = tokio_util::sync::CancellationToken::new();
    let metrics = tokio::runtime::Handle::current().metrics();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before = metrics.num_alive_tasks();

    const N: usize = 100;
    for i in 0..N {
        // Single worker.
        let builder = DataflowBuilder::<u64>::new(format!("short-{i}"));
        builder
            .source("src", vec![(0u64, vec![1u64, 2, 3])])
            .for_each("sink", |_t, _v| {});
        let handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().cancellation_token(app_token.clone()),
            )
            .unwrap();
        handle.join().await.unwrap();

        // Multi-worker (uniform parallelism).
        let multi = rt
            .spawn_multi(
                &format!("short-multi-{i}"),
                2,
                |builder: &mut DataflowBuilder<u64>| {
                    builder
                        .source("src", vec![(0u64, vec![1u64])])
                        .for_each("sink", |_t, _v| {});
                    Ok(())
                },
                SpawnOptions::new().cancellation_token(app_token.clone()),
            )
            .unwrap();
        multi.join().await.unwrap();
    }
    // Completion is published before the active count drops; wait for both.
    tokio::time::timeout(TIMEOUT, rt.wait_idle())
        .await
        .expect("runtime never became idle");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = metrics.num_alive_tasks();
    assert!(
        after < before + N / 4,
        "completed dataflows left bridge tasks alive: before={before} after={after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_cancellation_token_still_cancels_running_dataflows() {
    let rt = runtime(2);

    let token = tokio_util::sync::CancellationToken::new();
    let mut single = rt
        .spawn(
            idle_dataflow("bridge-single"),
            SpawnOptions::new().cancellation_token(token.clone()),
        )
        .unwrap();
    let _single_input = single.take_input::<u64>("data").unwrap();

    let mut multi = rt
        .spawn_multi(
            "bridge-multi",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder.input::<u64>("data")?.output("results")?;
                Ok(())
            },
            SpawnOptions::new().cancellation_token(token.clone()),
        )
        .unwrap();
    let _multi_inputs: Vec<_> = (0..2)
        .map(|w| multi.take_input::<u64>(w, "data").unwrap())
        .collect();

    let mut staged = rt
        .spawn_multi(
            "bridge-staged",
            1,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .exchange_to("scatter", 2, |v: &u64| *v)?
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new().cancellation_token(token.clone()),
        )
        .unwrap();
    let _staged_input = staged.take_input::<u64>(0, "data").unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    token.cancel();

    for result in [
        tokio::time::timeout(TIMEOUT, single.join()).await,
        tokio::time::timeout(TIMEOUT, multi.join()).await,
        tokio::time::timeout(TIMEOUT, staged.join()).await,
    ] {
        let result = result.expect("external cancellation did not stop the dataflow");
        assert!(
            matches!(result, Err(Error::Cancelled { .. })),
            "expected cancellation, got {result:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Graceful drain
// ---------------------------------------------------------------------------

#[test]
fn drain_closes_external_inputs() {
    let rt = runtime(2);
    let mut handle = rt
        .spawn(
            idle_dataflow("drain-close"),
            SpawnOptions::new().drain_on_cancel(Duration::from_secs(30)),
        )
        .unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let consumer = std::thread::spawn(move || {
        receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect::<Vec<u64>>()
    });
    sender.send(0, vec![1]).unwrap();

    handle.cancel(); // the drain begins at the executor's next sweep
    // Sends racing the start of the drain may still be accepted; once one is
    // refused, every later send must be refused too.
    let mut accepted = vec![1];
    let started = Instant::now();
    let mut v = 100u64;
    loop {
        match sender.send(0, vec![v]) {
            Ok(()) => accepted.push(v),
            Err(Error::ChannelClosed) => break,
            Err(e) => panic!("unexpected send error {e:?}"),
        }
        assert!(started.elapsed() < TIMEOUT, "drain never closed the input");
        v += 1;
        std::thread::sleep(Duration::from_millis(1));
    }
    for _ in 0..10 {
        assert!(
            matches!(sender.send(0, vec![42]), Err(Error::ChannelClosed)),
            "a closed input accepted data again"
        );
    }
    // The drain finishes even though the caller still holds the sender.
    within(TIMEOUT, move || handle.join_blocking())
        .expect("drain never finished with the sender still held")
        .expect("drain completes successfully once accepted data is processed");
    let seen = consumer.join().unwrap();
    assert_eq!(seen, accepted, "exactly the accepted records are delivered");
    drop(sender);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_closes_async_external_inputs_and_unblocks_senders() {
    let rt = runtime(1);
    let mut handle = rt
        .spawn(
            idle_dataflow("drain-async"),
            SpawnOptions::new()
                .io_mode(IoMode::Async)
                .drain_on_cancel(Duration::from_secs(60)),
        )
        .unwrap();
    let sender = handle.take_async_input::<u64>("data").unwrap();
    let mut receiver = handle.take_async_output::<u64>("results").unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    // Nobody reads yet, so the producer ends up blocked on a full input channel.
    let producer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let mut v = 0u64;
            while sender.send(0, vec![v]).await.is_ok() {
                accepted.fetch_add(1, Ordering::SeqCst);
                v += 1;
            }
            sender
        })
    };
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = accepted.load(Ordering::SeqCst);
        if now == last {
            break;
        }
        last = now;
    }
    assert!(!producer.is_finished(), "the producer should be blocked");
    handle.cancel();
    let sender = tokio::time::timeout(TIMEOUT, producer)
        .await
        .expect("async sender stayed blocked after the drain began")
        .unwrap();
    let consumer = tokio::spawn(async move { receiver.collect_data().await });
    tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .expect("drain never finished")
        .unwrap();
    let delivered: usize = consumer.await.unwrap().iter().map(|(_, d)| d.len()).sum();
    assert_eq!(
        delivered,
        accepted.load(Ordering::SeqCst),
        "every accepted record is delivered, nothing else"
    );
    assert!(sender.send(0, vec![0]).await.is_err());
}

/// Senders racing the start of a drain: every send that returned `Ok` is
/// delivered exactly once; every later send fails.
#[test]
fn drain_delivers_exactly_the_accepted_records_under_racing_senders() {
    for round in 0..20 {
        let rt = runtime(2);
        let mut handle = rt
            .spawn(
                idle_dataflow("drain-race"),
                SpawnOptions::new().drain_on_cancel(Duration::from_secs(60)),
            )
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let consumer = std::thread::spawn(move || {
            let mut values: Vec<u64> = receiver
                .collect_data()
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect();
            values.sort_unstable();
            values
        });
        let producers: Vec<_> = (0..4u64)
            .map(|p| {
                let sender = sender.clone();
                std::thread::spawn(move || {
                    let mut accepted = Vec::new();
                    let mut v = p;
                    while sender.send(0, vec![v]).is_ok() {
                        accepted.push(v);
                        v += 4;
                    }
                    accepted
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(5 + round % 5 * 10));
        handle.cancel();
        let mut accepted: Vec<u64> = producers
            .into_iter()
            .flat_map(|p| {
                within(TIMEOUT, move || p.join().unwrap()).expect("producer stalled in drain")
            })
            .collect();
        accepted.sort_unstable();
        within(TIMEOUT, move || handle.join_blocking())
            .unwrap_or_else(|| panic!("round {round}: drain never finished"))
            .unwrap_or_else(|e| panic!("round {round}: drain failed: {e}"));
        let delivered = consumer.join().unwrap();
        assert_eq!(
            delivered, accepted,
            "round {round}: delivered records differ from accepted ones"
        );
        drop(sender);
    }
}

/// `source_async` producers are stopped by a drain; exactly what they managed
/// to send is delivered.
#[test]
fn drain_stops_async_sources() {
    let rt = runtime(2);
    let produced = Arc::new(AtomicUsize::new(0));
    let builder = DataflowBuilder::<u64>::new("drain-source-async");
    let counter = Arc::clone(&produced);
    builder
        .source_async::<u64, _, _>("events", move |sender| {
            let counter = Arc::clone(&counter);
            async move {
                let mut v = 0u64;
                // `send` either enqueues or (if cancelled) does nothing, and the
                // count update cannot be interrupted, so `counter` is exact.
                while sender.send(0, vec![v]).await.is_ok() {
                    counter.fetch_add(1, Ordering::SeqCst);
                    v += 1;
                }
                Ok(())
            }
        })
        .output("results")
        .unwrap();
    let mut handle = rt
        .spawn(
            builder.build().unwrap(),
            SpawnOptions::new().drain_on_cancel(Duration::from_secs(60)),
        )
        .unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let consumer = std::thread::spawn(move || {
        receiver
            .collect_data()
            .iter()
            .map(|(_, d)| d.len())
            .sum::<usize>()
    });
    let started = Instant::now();
    while produced.load(Ordering::SeqCst) < 100 {
        assert!(started.elapsed() < TIMEOUT, "async source never produced");
        std::thread::sleep(Duration::from_millis(5));
    }
    handle.cancel();
    within(TIMEOUT, move || handle.join_blocking())
        .expect("drain never finished with an endless async source")
        .unwrap();
    let delivered = consumer.join().unwrap();
    assert_eq!(delivered, produced.load(Ordering::SeqCst));
}

/// An operator error in one worker must stop drain-configured siblings at
/// once instead of letting them drain until the deadline.
#[test]
fn operator_error_stops_draining_siblings() {
    for catch in [false, true] {
        let rt = runtime(2);
        let mut multi = rt
            .spawn_multi(
                "drain-error",
                3,
                move |builder: &mut DataflowBuilder<u64>| {
                    builder.catch_panics(catch);
                    builder
                        .input::<u64>("data")?
                        .exchange("spread", |v: &u64| *v)
                        .unary("fail-on-7", |input, output| {
                            while let Some((time, data)) = input.next() {
                                if data.contains(&7) {
                                    return Err(Error::operator(
                                        "fail-on-7",
                                        std::io::Error::other("bad record"),
                                    ));
                                }
                                output.push_vec(time, data);
                            }
                            Ok(())
                        })
                        .output("results")?;
                    Ok(())
                },
                SpawnOptions::new().drain_on_cancel(Duration::from_secs(120)),
            )
            .unwrap();
        let inputs: Vec<_> = (0..3)
            .map(|w| multi.take_input::<u64>(w, "data").unwrap())
            .collect();
        inputs[0].send(0, (0..20).collect()).unwrap();
        let completion = multi.join();
        let started = Instant::now();
        let result = within(TIMEOUT, move || completion.wait())
            .expect("siblings drained instead of stopping");
        assert!(started.elapsed() < Duration::from_secs(15));
        assert!(
            matches!(result, Err(Error::Operator { .. })),
            "expected the originating operator error, got {result:?}"
        );
        for input in &inputs {
            assert!(input.send(1, vec![1]).is_err());
        }
    }
}

/// A user cancellation that starts a drain, followed by a sibling failure,
/// must end the drain promptly too. Worker 1 cannot finish draining on its own
/// (nobody reads its output), so only the sibling failure can stop it early.
#[test]
fn sibling_failure_during_drain_stops_the_drain() {
    let rt = runtime(2);
    let mut multi = rt
        .spawn_multi(
            "drain-then-fail",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .unary("fail-on-7", |input, output| {
                        while let Some((time, data)) = input.next() {
                            if data.contains(&7) {
                                return Err(Error::operator(
                                    "fail-on-7",
                                    std::io::Error::other("bad record"),
                                ));
                            }
                            output.push_vec(time, data);
                        }
                        Ok(())
                    })
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new().drain_on_cancel(Duration::from_secs(120)),
        )
        .unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();
    let _outputs: Vec<_> = (0..2)
        .map(|w| multi.take_output::<u64>(w, "results").unwrap())
        .collect();
    // Fill worker 1 until its unread output backs the pipeline up.
    let backlog = std::thread::spawn(move || {
        let mut v = 100u64;
        while in1.send(0, vec![v]).is_ok() {
            v += 1;
        }
    });
    std::thread::sleep(Duration::from_millis(200));
    // Accepted before the drain starts; worker 0 fails while draining.
    in0.send(0, vec![1, 2, 3, 7]).unwrap();
    multi.cancel();
    let completion = multi.join();
    let started = Instant::now();
    let result = within(TIMEOUT, move || completion.wait()).expect("drain never ended");
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(
        matches!(result, Err(Error::Operator { .. })),
        "a failed worker fails the dataflow: {result:?}"
    );
    within(TIMEOUT, move || backlog.join().unwrap()).expect("worker 1 input stayed open");
}

// ---------------------------------------------------------------------------
// Two-node loopback-TCP clusters (Dedicated and Shared transport)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport")]
#[path = "support/cluster_loopback.rs"]
mod cluster_loopback;

#[cfg(feature = "transport")]
mod cluster {
    use super::*;

    use super::cluster_loopback::{Transport, spawn_pair};
    use instancy::{ClusterTopology, DataflowId, NodeConfig};

    /// Dropping one node's runtime mid-run resolves that node's completion
    /// and stops the peer instead of leaving it waiting forever.
    async fn runtime_drop(transport: Transport) {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
            builder
                .input::<u64>("data")?
                .exchange("spread", |v: &u64| *v)
                .output("results")?;
            Ok(())
        };
        let (nodes, mut a, mut b) = spawn_pair(
            transport,
            topology,
            DataflowId::new(),
            build,
            SpawnOptions::new(),
        )
        .await;
        let (sa, sb) = (
            a.take_input::<u64>(0, "data").unwrap(),
            b.take_input::<u64>(0, "data").unwrap(),
        );
        let (oa, ob) = (
            a.take_output::<u64>(0, "results").unwrap(),
            b.take_output::<u64>(0, "results").unwrap(),
        );
        sa.send(0, vec![1, 2, 3]).unwrap();
        sb.send(0, vec![4, 5, 6]).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Node A's completion is polled while its runtime drops: resolving it
        // tears node A's bridges down, which must not race the cancellation
        // message to node B.
        let completion_a = tokio::spawn(a.join().unwrap());

        let rt_a = nodes.rt_a;
        tokio::task::spawn_blocking(move || drop(rt_a))
            .await
            .unwrap();
        assert!(sa.send(1, vec![1]).is_err());
        tokio::task::spawn_blocking(move || within(TIMEOUT, move || oa.collect_data()))
            .await
            .unwrap()
            .expect("node-a output never closed");

        // Node B keeps its input open: only node A's departure can stop it.
        tokio::time::timeout(TIMEOUT, nodes.rt_b.wait_idle())
            .await
            .unwrap_or_else(|_| panic!("{transport:?}: node-b kept running"));
        let ra = tokio::time::timeout(TIMEOUT, completion_a)
            .await
            .unwrap_or_else(|_| panic!("{transport:?}: node-a completion never resolved"))
            .expect("node-a completion task");
        assert!(
            is_runtime_shutdown(&ra),
            "{transport:?}: node-a should report RuntimeShutdown, got {ra:?}"
        );
        let rb = tokio::task::spawn_blocking(move || within(TIMEOUT, move || b.join_blocking()))
            .await
            .unwrap()
            .expect("node-b completion never resolved");
        assert!(
            rb.is_err(),
            "{transport:?}: node-b cannot succeed, got {rb:?}"
        );
        drop((sb, ob));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedicated_cluster_runtime_drop_stops_both_nodes() {
        runtime_drop(Transport::Dedicated).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_cluster_runtime_drop_stops_both_nodes() {
        runtime_drop(Transport::Shared).await;
    }

    /// A failure on node A must stop drain-configured node B at once rather
    /// than letting it drain until the deadline (B can never finish: A's
    /// workers are gone and B's input stays open).
    async fn remote_failure_skips_drain(transport: Transport) {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
            builder
                .input::<u64>("data")?
                .unary("fail-on-7", |input, output| {
                    while let Some((time, data)) = input.next() {
                        if data.contains(&7) {
                            return Err(Error::operator(
                                "fail-on-7",
                                std::io::Error::other("bad record"),
                            ));
                        }
                        output.push_vec(time, data);
                    }
                    Ok(())
                })
                .exchange("spread", |v: &u64| *v)
                .output("results")?;
            Ok(())
        };
        let (nodes, mut a, mut b) = spawn_pair(
            transport,
            topology,
            DataflowId::new(),
            build,
            SpawnOptions::new().drain_on_cancel(Duration::from_secs(120)),
        )
        .await;
        let (sa, sb) = (
            a.take_input::<u64>(0, "data").unwrap(),
            b.take_input::<u64>(0, "data").unwrap(),
        );
        sb.send(0, vec![100]).unwrap();
        sa.send(0, vec![1, 7]).unwrap();
        // Hold both completions unresolved (bridges alive) until every worker stops.
        let started = Instant::now();
        tokio::time::timeout(TIMEOUT, async {
            nodes.rt_a.wait_idle().await;
            nodes.rt_b.wait_idle().await;
        })
        .await
        .unwrap_or_else(|_| panic!("{transport:?}: node-b drained instead of stopping"));
        assert!(started.elapsed() < Duration::from_secs(15));
        let ra = tokio::task::spawn_blocking(move || within(TIMEOUT, move || a.join_blocking()))
            .await
            .unwrap()
            .expect("node-a completion never resolved");
        let rb = tokio::task::spawn_blocking(move || within(TIMEOUT, move || b.join_blocking()))
            .await
            .unwrap()
            .expect("node-b completion never resolved");
        assert!(
            matches!(ra, Err(Error::Operator { .. })),
            "{transport:?}: node-a should report the operator error, got {ra:?}"
        );
        assert!(
            matches!(rb, Err(Error::Cancelled { .. })),
            "{transport:?}: node-b should be cancelled, got {rb:?}"
        );
        drop((sa, sb));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedicated_cluster_remote_failure_skips_drain() {
        remote_failure_skips_drain(Transport::Dedicated).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_cluster_remote_failure_skips_drain() {
        remote_failure_skips_drain(Transport::Shared).await;
    }
}
