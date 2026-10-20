//! Regression tests: an operator panic must fail the dataflow with
//! `Error::OperatorPanic` and tear the executor down, releasing every I/O
//! handle — output receivers observe closure and input senders fail — with or
//! without `catch_panics`, in single-worker, multi-worker, and cluster runs.

use std::sync::{Once, mpsc};
use std::time::Duration;

use instancy::{
    DataflowBuilder, Error, IoMode, Result, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

#[cfg(feature = "transport")]
#[path = "support/cluster_loopback.rs"]
mod cluster_loopback;

const TIMEOUT: Duration = Duration::from_secs(20);
/// Records with this value make the test operators panic.
const POISON: u64 = 5;
const PANIC_MESSAGE: &str = "intentional test panic";

/// Keep intentional panics out of the test output; report anything else.
fn quiet_intentional_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let intentional = info
                .payload()
                .downcast_ref::<&str>()
                .is_some_and(|m| m.contains(PANIC_MESSAGE))
                || info
                    .payload()
                    .downcast_ref::<String>()
                    .is_some_and(|m| m.contains(PANIC_MESSAGE));
            if !intentional {
                default(info);
            }
        }));
    });
}

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

fn boom(v: u64) -> u64 {
    if v == POISON {
        panic!("{PANIC_MESSAGE}");
    }
    v
}

fn assert_operator_panic(result: Result<()>, operator: &str, context: &str) {
    match result {
        Err(Error::OperatorPanic {
            operator: op,
            message,
            ..
        }) => {
            assert_eq!(op, operator, "{context}: wrong operator");
            assert!(
                message.contains(PANIC_MESSAGE),
                "{context}: unexpected message {message:?}"
            );
        }
        other => panic!("{context}: expected OperatorPanic from '{operator}', got {other:?}"),
    }
}

/// Where in the pipeline the panicking operator sits.
#[derive(Clone, Copy, Debug)]
enum Site {
    Map,
    Filter,
    FlatMap,
    Inspect,
    Unary,
    ForEach,
}

impl Site {
    const ALL: [Site; 6] = [
        Site::Map,
        Site::Filter,
        Site::FlatMap,
        Site::Inspect,
        Site::Unary,
        Site::ForEach,
    ];

    fn operator(self) -> &'static str {
        match self {
            Site::Map => "boom-map",
            Site::Filter => "boom-filter",
            Site::FlatMap => "boom-flat-map",
            Site::Inspect => "boom-inspect",
            Site::Unary => "boom-unary",
            Site::ForEach => "boom-for-each",
        }
    }

    /// input "data" → [panicking site] → output "results" (ForEach panics in
    /// a sibling branch; "results" is fed by a pass-through map).
    fn build(self, builder: &DataflowBuilder<u64>) {
        let input = builder.input::<u64>("data").unwrap();
        let out = match self {
            Site::Map => input.map(self.operator(), |_t, v| boom(v)),
            Site::Filter => input.filter(self.operator(), |_t, v| boom(*v) < u64::MAX),
            Site::FlatMap => input.flat_map(self.operator(), |_t, v| vec![boom(v)]),
            Site::Inspect => input.inspect(self.operator(), |_t, v| {
                boom(*v);
            }),
            Site::Unary => input.unary(self.operator(), |input, output| {
                while let Some((time, data)) = input.next() {
                    output.push_vec(time, data.into_iter().map(boom).collect());
                }
                Ok(())
            }),
            Site::ForEach => {
                input.clone().for_each(self.operator(), |_t, v| {
                    boom(*v);
                });
                input.map("pass", |_t, v| v)
            }
        };
        out.output("results").unwrap();
    }
}

/// When the poison record is delivered relative to executor parking.
#[derive(Clone, Copy, Debug)]
enum Timing {
    /// Poison is part of the very first batch.
    FirstBatch,
    /// The executor processes data and parks before the poison arrives.
    AfterParking,
}

fn check_single_worker(site: Site, catch_panics: bool, io_mode: IoMode, timing: Timing) {
    let context = format!("{site:?} catch_panics={catch_panics} {io_mode:?} {timing:?}");
    let rt = runtime(2);
    let builder = DataflowBuilder::<u64>::new("panic");
    builder.catch_panics(catch_panics);
    site.build(&builder);
    let mut handle = rt
        .spawn(
            builder.build().unwrap(),
            SpawnOptions::new().io_mode(io_mode),
        )
        .unwrap();

    match io_mode {
        IoMode::Sync => {
            let receiver = handle.take_output::<u64>("results").unwrap();
            let sender = handle.take_input::<u64>("data").unwrap();
            if let Timing::AfterParking = timing {
                sender.send(0, vec![1, 2, 3]).unwrap();
                std::thread::sleep(Duration::from_millis(100));
            }
            sender.send(0, vec![POISON]).unwrap();
            let completion = handle.join();
            let result = within(TIMEOUT, move || completion.wait())
                .unwrap_or_else(|| panic!("{context}: join never returned"));
            assert_operator_panic(result, site.operator(), &context);

            // Join observed => the executor is gone: I/O fails or ends at once.
            assert!(
                sender.send(1, vec![1]).is_err(),
                "{context}: input sender accepted data after the dataflow failed"
            );
            within(TIMEOUT, move || receiver.collect_data())
                .unwrap_or_else(|| panic!("{context}: output receiver never closed"));
        }
        IoMode::Async => {
            let mut receiver = handle.take_async_output::<u64>("results").unwrap();
            let sender = handle.take_async_input::<u64>("data").unwrap();
            let completion = handle.join();
            let rt_io = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let context = context.clone();
            rt_io.block_on(async move {
                if let Timing::AfterParking = timing {
                    sender.send(0, vec![1, 2, 3]).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                sender.send(0, vec![POISON]).await.unwrap();
                let result = tokio::time::timeout(TIMEOUT, completion)
                    .await
                    .unwrap_or_else(|_| panic!("{context}: join never returned"));
                assert_operator_panic(result, site.operator(), &context);
                assert!(
                    sender.send(1, vec![1]).await.is_err(),
                    "{context}: async input sender accepted data after the dataflow failed"
                );
                tokio::time::timeout(TIMEOUT, receiver.collect_data())
                    .await
                    .unwrap_or_else(|_| panic!("{context}: async output receiver never closed"));
            });
        }
    }

    // The runtime survives the panic and keeps running new dataflows.
    let builder = DataflowBuilder::<u64>::new("after-panic");
    builder
        .source("src", vec![(0u64, vec![1u64, 2])])
        .output("results")
        .unwrap();
    let mut next = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let out = next.take_output::<u64>("results").unwrap();
    within(TIMEOUT, move || next.join_blocking())
        .unwrap_or_else(|| panic!("{context}: runtime stalled after the panic"))
        .unwrap();
    assert_eq!(out.collect_data(), vec![(0, vec![1, 2])]);
}

#[test]
fn operator_panic_releases_io_channels() {
    quiet_intentional_panics();
    check_single_worker(Site::Map, false, IoMode::Sync, Timing::AfterParking);
}

#[test]
fn operator_panic_matrix_without_catch_panics() {
    quiet_intentional_panics();
    for site in Site::ALL {
        for io_mode in [IoMode::Sync, IoMode::Async] {
            for timing in [Timing::FirstBatch, Timing::AfterParking] {
                check_single_worker(site, false, io_mode, timing);
            }
        }
    }
}

#[test]
fn operator_panic_matrix_with_catch_panics() {
    quiet_intentional_panics();
    for site in Site::ALL {
        for io_mode in [IoMode::Sync, IoMode::Async] {
            for timing in [Timing::FirstBatch, Timing::AfterParking] {
                check_single_worker(site, true, io_mode, timing);
            }
        }
    }
}

/// The sink is blocked on a full output channel (nobody reads) when an
/// upstream operator panics; the receiver must still observe closure.
#[test]
fn panic_while_output_is_backpressured() {
    quiet_intentional_panics();
    for catch_panics in [false, true] {
        let rt = runtime(1);
        let builder = DataflowBuilder::<u64>::new("panic-backpressure");
        builder.catch_panics(catch_panics);
        builder
            .input::<u64>("data")
            .unwrap()
            .map(
                "boom-late",
                |_t, v| if v == 10_000 { boom(POISON) } else { v },
            )
            .output("results")
            .unwrap();
        let mut handle = rt
            .spawn(builder.build().unwrap(), SpawnOptions::new())
            .unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        // Fill every channel without reading, then deliver the poison record.
        let producer = std::thread::spawn(move || {
            for i in 0..1_000u64 {
                if sender.send(0, vec![i]).is_err() {
                    return;
                }
            }
            let _ = sender.send(0, vec![10_000]);
        });
        std::thread::sleep(Duration::from_millis(200));
        // Unblock one slot at a time until the poison reaches the operator.
        let drained = within(TIMEOUT, move || {
            let mut n = 0usize;
            while receiver.recv().is_some() {
                n += 1;
            }
            n
        })
        .expect("output receiver never closed after the panic");
        assert!(drained <= 1_000);
        within(TIMEOUT, move || producer.join().unwrap()).expect("producer stalled");
        assert_operator_panic(
            within(TIMEOUT, move || handle.join_blocking()).expect("join stalled"),
            "boom-late",
            &format!("backpressure catch_panics={catch_panics}"),
        );
    }
}

/// Multi-worker: one worker panics; the aggregate join reports the panic and
/// every worker's I/O is released, even with a long drain configured.
#[test]
fn multi_worker_panic_stops_all_workers() {
    quiet_intentional_panics();
    for catch_panics in [false, true] {
        for drain in [None, Some(Duration::from_secs(120))] {
            let context = format!("catch_panics={catch_panics} drain={drain:?}");
            let rt = runtime(2);
            let mut options = SpawnOptions::new();
            if let Some(timeout) = drain {
                options = options.drain_on_cancel(timeout);
            }
            let mut multi = rt
                .spawn_multi(
                    "multi-panic",
                    3,
                    move |builder: &mut DataflowBuilder<u64>| {
                        builder.catch_panics(catch_panics);
                        builder
                            .input::<u64>("data")?
                            .exchange("spread", |v: &u64| *v)
                            .map("boom-exchanged", |_t, v| boom(v))
                            .output("results")?;
                        Ok(())
                    },
                    options,
                )
                .unwrap();
            let senders: Vec<_> = (0..3)
                .map(|w| multi.take_input::<u64>(w, "data").unwrap())
                .collect();
            let receivers: Vec<_> = (0..3)
                .map(|w| multi.take_output::<u64>(w, "results").unwrap())
                .collect();
            // Other workers keep their inputs open: only the failure can stop them.
            senders[0].send(0, (0..20).collect()).unwrap();
            let completion = multi.join();
            let started = std::time::Instant::now();
            let result = within(TIMEOUT, move || completion.wait())
                .unwrap_or_else(|| panic!("{context}: join never returned"));
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "{context}: siblings drained instead of stopping"
            );
            assert_operator_panic(result, "boom-exchanged", &context);
            for (w, sender) in senders.iter().enumerate() {
                assert!(
                    sender.send(1, vec![1]).is_err(),
                    "{context}: worker {w} input still open"
                );
            }
            for receiver in receivers {
                within(TIMEOUT, move || receiver.collect_data())
                    .unwrap_or_else(|| panic!("{context}: a worker's output never closed"));
            }
        }
    }
}

/// Many dataflows panic concurrently on a small pool; none may leak or hang.
#[test]
fn concurrent_panics_do_not_leak_executors() {
    quiet_intentional_panics();
    let rt = runtime(2);
    let mut completions = Vec::new();
    let mut receivers = Vec::new();
    for i in 0..40u64 {
        let builder = DataflowBuilder::<u64>::new(format!("concurrent-{i}"));
        builder.catch_panics(i % 2 == 0);
        builder
            .input::<u64>("data")
            .unwrap()
            .map("boom-concurrent", |_t, v| boom(v))
            .output("results")
            .unwrap();
        let mut handle = rt
            .spawn(builder.build().unwrap(), SpawnOptions::new())
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        receivers.push(handle.take_output::<u64>("results").unwrap());
        sender.send(0, vec![1, 2, POISON]).unwrap();
        completions.push((sender, handle.join()));
    }
    for (sender, completion) in completions {
        assert_operator_panic(
            within(TIMEOUT, move || completion.wait()).expect("join stalled"),
            "boom-concurrent",
            "concurrent",
        );
        assert!(sender.send(0, vec![1]).is_err());
    }
    for receiver in receivers {
        within(TIMEOUT, move || receiver.collect_data()).expect("output never closed");
    }
    assert_eq!(rt.active_dataflows(), 0);
}

/// A panic inside a notification callback that fires only when the input
/// closes (end of stream).
#[test]
fn panic_in_end_of_stream_notification() {
    quiet_intentional_panics();
    for catch_panics in [false, true] {
        let rt = runtime(1);
        let builder = DataflowBuilder::<u64>::new("panic-notify");
        builder.catch_panics(catch_panics);
        builder
            .input::<u64>("data")
            .unwrap()
            .unary_notify("boom-notify", |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    ctx.notify_at(time);
                    output.push_vec(time, data);
                }
                if ctx.next_notification().is_some() {
                    panic!("{PANIC_MESSAGE}");
                }
                Ok(())
            })
            .output("results")
            .unwrap();
        let mut handle = rt
            .spawn(builder.build().unwrap(), SpawnOptions::new())
            .unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        sender.send(0, vec![1, 2]).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        drop(sender); // the notification fires once the input closes
        assert_operator_panic(
            within(TIMEOUT, move || handle.join_blocking()).expect("join stalled"),
            "boom-notify",
            &format!("notify catch_panics={catch_panics}"),
        );
        within(TIMEOUT, move || receiver.collect_data()).expect("output never closed");
    }
}

/// The poison record is processed only after a graceful drain has begun.
#[test]
fn panic_during_graceful_drain() {
    quiet_intentional_panics();
    const LAST: u64 = 1_999;
    for catch_panics in [false, true] {
        let rt = runtime(1);
        let builder = DataflowBuilder::<u64>::new("panic-drain");
        builder.catch_panics(catch_panics);
        builder
            .input::<u64>("data")
            .unwrap()
            .map(
                "boom-drain",
                |_t, v| if v == LAST { boom(POISON) } else { v },
            )
            .output("results")
            .unwrap();
        let mut handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().drain_on_cancel(Duration::from_secs(120)),
            )
            .unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        // Unread output backs the pipeline up before the poison is reached.
        for v in 0..=LAST {
            sender.send(0, vec![v]).unwrap();
        }
        std::thread::sleep(Duration::from_millis(100));
        handle.cancel();
        std::thread::sleep(Duration::from_millis(100));
        let completion = handle.join();
        // Reading lets the drain proceed to the poison record.
        let reader = std::thread::spawn(move || receiver.collect_data().len());
        assert_operator_panic(
            within(TIMEOUT, move || completion.wait()).expect("join stalled"),
            "boom-drain",
            &format!("drain catch_panics={catch_panics}"),
        );
        within(TIMEOUT, move || reader.join().unwrap()).expect("output never closed");
        assert!(sender.send(0, vec![0]).is_err());
    }
}

/// One panicking dataflow must not disturb others sharing the pool.
#[test]
fn panicking_dataflow_does_not_disturb_neighbours() {
    quiet_intentional_panics();
    let rt = runtime(1);
    let mut healthy = Vec::new();
    for i in 0..8u64 {
        let mut handle = rt
            .spawn(
                {
                    let builder = DataflowBuilder::<u64>::new(format!("healthy-{i}"));
                    builder
                        .input::<u64>("data")
                        .unwrap()
                        .map("double", |_t, v| v * 2)
                        .output("results")
                        .unwrap();
                    builder.build().unwrap()
                },
                SpawnOptions::new(),
            )
            .unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        healthy.push((sender, receiver, handle));
    }
    let builder = DataflowBuilder::<u64>::new("poisoned");
    builder
        .input::<u64>("data")
        .unwrap()
        .map("boom-neighbour", |_t, v| boom(v))
        .output("results")
        .unwrap();
    let mut poisoned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let poison_sender = poisoned.take_input::<u64>("data").unwrap();
    for (sender, _, _) in &healthy {
        sender.send(0, (0..100).collect()).unwrap();
    }
    poison_sender.send(0, vec![POISON]).unwrap();
    assert_operator_panic(
        within(TIMEOUT, move || poisoned.join_blocking()).expect("join stalled"),
        "boom-neighbour",
        "neighbours",
    );
    for (sender, receiver, handle) in healthy {
        sender.send(1, (100..200).collect()).unwrap();
        drop(sender);
        within(TIMEOUT, move || handle.join_blocking())
            .expect("healthy dataflow stalled")
            .unwrap();
        let mut values: Vec<u64> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        values.sort_unstable();
        assert_eq!(values, (0..200).map(|v| v * 2).collect::<Vec<_>>());
    }
}

/// Per-stage parallelism runs a different executor; its panics must surface
/// the same way.
#[test]
fn staged_executor_panic_reports_operator() {
    quiet_intentional_panics();
    for catch_panics in [false, true] {
        let rt = runtime(2);
        let mut multi = rt
            .spawn_multi(
                "staged-panic",
                1,
                move |builder: &mut DataflowBuilder<u64>| {
                    builder.catch_panics(catch_panics);
                    builder
                        .input::<u64>("data")?
                        .exchange_to("scatter", 2, |v: &u64| *v)?
                        .map("boom-staged", |_t, v| boom(v))
                        .for_each("sink", |_t, _v| {});
                    Ok(())
                },
                SpawnOptions::new(),
            )
            .unwrap();
        let sender = multi.take_input::<u64>(0, "data").unwrap();
        sender.send(0, (0..20).collect()).unwrap();
        let completion = multi.join();
        let result = within(TIMEOUT, move || completion.wait()).expect("staged join stalled");
        assert_operator_panic(
            result,
            "boom-staged",
            &format!("staged catch_panics={catch_panics}"),
        );
        assert!(sender.send(1, vec![1]).is_err());
    }
}

/// Runtime drop racing with operator panics: every completion still resolves
/// (as the panic or as runtime shutdown) and all I/O is released.
#[test]
fn runtime_drop_racing_operator_panics() {
    quiet_intentional_panics();
    for round in 0..20 {
        let rt = runtime(2);
        let mut completions = Vec::new();
        let mut io = Vec::new();
        for i in 0..6u64 {
            let builder = DataflowBuilder::<u64>::new(format!("race-{i}"));
            builder.catch_panics(i % 2 == 0);
            builder
                .input::<u64>("data")
                .unwrap()
                .map("boom-race", |_t, v| boom(v))
                .output("results")
                .unwrap();
            let mut handle = rt
                .spawn(builder.build().unwrap(), SpawnOptions::new())
                .unwrap();
            let sender = handle.take_input::<u64>("data").unwrap();
            let receiver = handle.take_output::<u64>("results").unwrap();
            sender.send(0, vec![1, POISON]).unwrap();
            io.push((sender, receiver));
            completions.push(handle.join());
        }
        if round % 2 == 0 {
            std::thread::sleep(Duration::from_millis(round as u64));
        }
        drop(rt);
        for completion in completions {
            let result = within(TIMEOUT, move || completion.wait())
                .unwrap_or_else(|| panic!("round {round}: completion never resolved"));
            assert!(
                matches!(
                    result,
                    Err(Error::OperatorPanic { .. }) | Err(Error::Cancelled { .. })
                ),
                "round {round}: unexpected {result:?}"
            );
        }
        for (sender, receiver) in io {
            assert!(sender.send(0, vec![1]).is_err());
            within(TIMEOUT, move || receiver.collect_data()).expect("output never closed");
        }
    }
}

// ---------------------------------------------------------------------------
// Two-node loopback-TCP clusters (Dedicated and Shared transport)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport")]
mod cluster {
    use super::*;

    use super::cluster_loopback::{Transport, spawn_pair};
    use instancy::{ClusterTopology, DataflowId, NodeConfig};

    async fn run(transport: Transport, catch_panics: bool) {
        quiet_intentional_panics();
        let context = format!("{transport:?} catch_panics={catch_panics}");
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let build = move |builder: &mut DataflowBuilder<u64>| -> Result<()> {
            builder.catch_panics(catch_panics);
            builder
                .input::<u64>("data")?
                .map("boom-cluster", |_t, v| boom(v))
                .exchange("spread", |v: &u64| *v)
                .map("pass", |_t, v| v)
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
        sb.send(0, vec![100, 101]).unwrap();
        sa.send(0, vec![1, 2, 3]).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Node A panics; node B keeps its input open, so only the failure ends it.
        sa.send(0, vec![POISON]).unwrap();

        // Every worker on both nodes must stop. Waiting on the runtimes (rather
        // than resolving a ClusterCompletion, which tears its node's bridges
        // down) isolates this from cluster-teardown ordering.
        tokio::time::timeout(TIMEOUT, async {
            nodes.rt_a.wait_idle().await;
            nodes.rt_b.wait_idle().await;
        })
        .await
        .unwrap_or_else(|_| panic!("{context}: workers kept running after the panic"));
        let jb = tokio::task::spawn_blocking(move || within(TIMEOUT, move || b.join_blocking()))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{context}: node-b never completed"));
        let ja = tokio::task::spawn_blocking(move || within(TIMEOUT, move || a.join_blocking()))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{context}: node-a never completed"));
        assert_operator_panic(ja, "boom-cluster", &context);
        assert!(
            matches!(&jb, Err(Error::Cancelled { .. })),
            "{context}: node-b should be cancelled by its failed peer, got {jb:?}"
        );
        for (node, sender) in [("a", sa), ("b", sb)] {
            assert!(
                sender.send(1, vec![1]).is_err(),
                "{context}: node-{node} input still open"
            );
        }
        for receiver in [oa, ob] {
            within(TIMEOUT, move || receiver.collect_data())
                .unwrap_or_else(|| panic!("{context}: a node's output never closed"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedicated_cluster_operator_panic() {
        run(Transport::Dedicated, false).await;
        run(Transport::Dedicated, true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_cluster_operator_panic() {
        run(Transport::Shared, false).await;
        run(Transport::Shared, true).await;
    }
}
