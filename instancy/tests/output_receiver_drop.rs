//! Regression tests: dropping (or never taking) an output receiver abandons
//! that port. The dataflow must keep consuming and discarding the port's data
//! and complete normally — it must never livelock upstream operators or block
//! input senders. Covers single-worker, multi-worker, and two-node clusters.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use instancy::dataflow::OutputEvent;
use instancy::{
    DataflowBuilder, IoMode, MultiSpawnedDataflow, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions, SpawnedDataflow,
};

#[cfg(feature = "transport")]
#[path = "support/cluster_loopback.rs"]
mod cluster_loopback;

const TIMEOUT: Duration = Duration::from_secs(20);
/// More records than the input (256) and output (256) channels plus every
/// internal channel can hold, so a stalled sink always blocks the sender.
const RECORDS: u64 = 5_000;
/// Exchange tests send fewer records (well under one exchange channel's
/// capacity, so the exchange itself never backs up) and shrink the sink's
/// input edge instead, so a sink that stops consuming still stalls the
/// pipeline.
const EXCHANGED_RECORDS: u64 = 200;
const SINK_EDGE_CAPACITY: usize = 4;

fn runtime(threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: threads,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Run `f` on a helper thread and wait at most `timeout` for its result.
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

fn join(handle: SpawnedDataflow<u64>) -> Result<()> {
    within(TIMEOUT, move || handle.join_blocking())
        .expect("dataflow never completed after its output receiver was dropped")
}

fn join_multi(handle: MultiSpawnedDataflow<u64>) -> Result<()> {
    within(TIMEOUT, move || handle.join_blocking())
        .expect("multi-worker dataflow never completed after an output receiver was dropped")
}

/// Send `RECORDS` single-record batches; returns how many sends succeeded.
fn send_all(sender: instancy::dataflow::InputSender<u64, u64>) -> usize {
    within(TIMEOUT, move || {
        (0..RECORDS)
            .take_while(|i| sender.send(0, vec![*i]).is_ok())
            .count()
    })
    .expect("input sender blocked forever after the output receiver was dropped")
}

fn identity_dataflow(name: &str) -> instancy::LogicalDataflow<u64> {
    let builder = DataflowBuilder::<u64>::new(name);
    let input = builder.input::<u64>("data").unwrap();
    input.map("id", |_t, v| v).output("results").unwrap();
    builder.build().unwrap()
}

fn count_data<T: instancy::Timestamp>(rx: &instancy::dataflow::OutputReceiver<T, u64>) -> usize {
    rx.collect_data().iter().map(|(_, d)| d.len()).sum()
}

#[test]
fn dropped_output_receiver_lets_sync_dataflow_complete() {
    let rt = runtime(2);
    let mut handle = rt
        .spawn(identity_dataflow("drop-output"), SpawnOptions::new())
        .unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    drop(receiver);

    assert_eq!(send_all(sender), RECORDS as usize, "every send is accepted");
    join(handle).expect("abandoned output is discarded, not an error");
}

#[test]
fn kept_output_receiver_delivers_every_record() {
    let rt = runtime(2);
    let mut handle = rt
        .spawn(identity_dataflow("keep-output"), SpawnOptions::new())
        .unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    let collector = std::thread::spawn(move || count_data(&receiver));
    assert_eq!(send_all(sender), RECORDS as usize);
    join(handle).unwrap();
    assert_eq!(collector.join().unwrap(), RECORDS as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_async_output_receiver_lets_async_dataflow_complete() {
    let rt = runtime(2);
    let mut handle = rt
        .spawn(
            identity_dataflow("drop-output-async"),
            SpawnOptions::new().io_mode(IoMode::Async),
        )
        .unwrap();
    let receiver = handle.take_async_output::<u64>("results").unwrap();
    let sender = handle.take_async_input::<u64>("data").unwrap();
    drop(receiver);

    let send_all = async move {
        let mut sent = 0usize;
        for i in 0..RECORDS {
            if sender.send(0, vec![i]).await.is_err() {
                break;
            }
            sent += 1;
        }
        sent
    };
    let sent = tokio::time::timeout(TIMEOUT, send_all)
        .await
        .expect("async sender stuck after the output receiver was dropped");
    assert_eq!(sent, RECORDS as usize);
    tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .expect("async dataflow never completed after the output receiver was dropped")
        .unwrap();
}

/// `join()` consumes the handle, dropping receivers that were never taken.
#[test]
fn untaken_output_port_does_not_block_completion() {
    let rt = runtime(2);
    let builder = DataflowBuilder::<u64>::new("untaken-output");
    let mapped = builder.input::<u64>("data").unwrap().map("id", |_t, v| v);
    mapped.clone().output("kept").unwrap();
    mapped.output("ignored").unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let kept = handle.take_output::<u64>("kept").unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    let collector = std::thread::spawn(move || count_data(&kept));
    let completion = handle.join();
    assert_eq!(send_all(sender), RECORDS as usize);
    within(TIMEOUT, move || completion.wait())
        .expect("untaken output port stalled the dataflow")
        .unwrap();
    assert_eq!(
        collector.join().unwrap(),
        RECORDS as usize,
        "abandoning one port must not affect another"
    );
}

/// The receiver is dropped while the sink is blocked on a full output channel
/// and the sender is blocked on a full input channel.
#[test]
fn receiver_dropped_while_sink_is_blocked_on_backpressure() {
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let rt = runtime(1);
        let mut handle = rt
            .spawn(
                identity_dataflow("drop-while-blocked"),
                SpawnOptions::new().io_mode(io_mode),
            )
            .unwrap();
        let sent = Arc::new(AtomicUsize::new(0));
        let drop_receiver: Box<dyn FnOnce() + Send> = match io_mode {
            IoMode::Sync => {
                let rx = handle.take_output::<u64>("results").unwrap();
                Box::new(move || drop(rx))
            }
            IoMode::Async => {
                let rx = handle.take_async_output::<u64>("results").unwrap();
                Box::new(move || drop(rx))
            }
        };
        let producer = match io_mode {
            IoMode::Sync => {
                let sender = handle.take_input::<u64>("data").unwrap();
                let sent = Arc::clone(&sent);
                std::thread::spawn(move || {
                    for i in 0..RECORDS {
                        if sender.send(0, vec![i]).is_err() {
                            break;
                        }
                        sent.fetch_add(1, Ordering::SeqCst);
                    }
                })
            }
            IoMode::Async => {
                let sender = handle.take_async_input::<u64>("data").unwrap();
                let sent = Arc::clone(&sent);
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move {
                        for i in 0..RECORDS {
                            if sender.send(0, vec![i]).await.is_err() {
                                break;
                            }
                            sent.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                })
            }
        };
        // Wait until the pipeline is saturated: the producer stops advancing.
        let mut last = usize::MAX;
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let now = sent.load(Ordering::SeqCst);
            if now == last {
                break;
            }
            last = now;
        }
        assert!(
            last < RECORDS as usize,
            "{io_mode:?}: pipeline never filled, sent {last}"
        );
        drop_receiver();
        within(TIMEOUT, move || producer.join().unwrap())
            .unwrap_or_else(|| panic!("{io_mode:?}: sender stayed blocked after receiver drop"));
        assert_eq!(sent.load(Ordering::SeqCst), RECORDS as usize);
        join(handle).unwrap();
    }
}

/// Dropping one worker's receiver must not stall the exchange that feeds it,
/// nor the other worker.
#[test]
fn multi_worker_exchange_survives_one_dropped_receiver() {
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let rt = runtime(2);
        let mut multi = rt
            .spawn_multi(
                "multi-drop",
                2,
                |builder: &mut DataflowBuilder<u64>| {
                    builder
                        .input::<u64>("data")?
                        .exchange("by_val", |v: &u64| *v)
                        .map("id", |_t, v| v)
                        .with_capacity(SINK_EDGE_CAPACITY)?
                        .output("results")?;
                    Ok(())
                },
                SpawnOptions::new().io_mode(io_mode),
            )
            .unwrap();
        let collector = match io_mode {
            IoMode::Sync => {
                drop(multi.take_output::<u64>(0, "results").unwrap());
                let kept = multi.take_output::<u64>(1, "results").unwrap();
                std::thread::spawn(move || {
                    let mut values: Vec<u64> = kept
                        .collect_data()
                        .into_iter()
                        .flat_map(|(_, d)| d)
                        .collect();
                    values.sort_unstable();
                    values
                })
            }
            IoMode::Async => {
                drop(multi.take_async_output::<u64>(0, "results").unwrap());
                let mut kept = multi.take_async_output::<u64>(1, "results").unwrap();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let mut values: Vec<u64> = rt
                        .block_on(kept.collect_data())
                        .into_iter()
                        .flat_map(|(_, d)| d)
                        .collect();
                    values.sort_unstable();
                    values
                })
            }
        };
        let share = EXCHANGED_RECORDS as usize / 2;
        match io_mode {
            IoMode::Sync => {
                for w in 0..2 {
                    let sender = multi.take_input::<u64>(w, "data").unwrap();
                    let sent = within(TIMEOUT, move || {
                        (0..EXCHANGED_RECORDS)
                            .filter(|v| *v as usize % 2 == w)
                            .take_while(|v| sender.send(0, vec![*v]).is_ok())
                            .count()
                    })
                    .expect("sender stalled");
                    assert_eq!(sent, share);
                }
            }
            IoMode::Async => {
                for w in 0..2 {
                    let sender = multi.take_async_input::<u64>(w, "data").unwrap();
                    let sent = within(TIMEOUT, move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                        rt.block_on(async move {
                            let mut sent = 0;
                            for v in (0..EXCHANGED_RECORDS).filter(|v| *v as usize % 2 == w) {
                                if sender.send(0, vec![v]).await.is_err() {
                                    break;
                                }
                                sent += 1;
                            }
                            sent
                        })
                    })
                    .expect("async sender stalled");
                    assert_eq!(sent, share);
                }
            }
        }
        join_multi(multi).unwrap();
        let kept = collector.join().unwrap();
        // Worker 1 still receives its partition: non-empty, no duplicates.
        assert!(
            !kept.is_empty(),
            "{io_mode:?}: kept worker received nothing"
        );
        assert!(
            kept.windows(2).all(|w| w[0] < w[1]),
            "{io_mode:?}: duplicates"
        );
        assert!(kept.iter().all(|v| *v < EXCHANGED_RECORDS));
    }
}

/// Worker-local pipelines at full volume: the worker whose receiver is
/// dropped completes, and so does every sibling.
#[test]
fn multi_worker_local_pipelines_survive_dropped_receivers() {
    let rt = runtime(2);
    let mut multi = rt
        .spawn_multi(
            "multi-local-drop",
            3,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .map("id", |_t, v| v)
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    drop(multi.take_output::<u64>(0, "results").unwrap());
    drop(multi.take_output::<u64>(2, "results").unwrap());
    let kept = multi.take_output::<u64>(1, "results").unwrap();
    let collector = std::thread::spawn(move || count_data(&kept));
    let senders: Vec<_> = (0..3)
        .map(|w| multi.take_input::<u64>(w, "data").unwrap())
        .collect();
    let feeders: Vec<_> = senders
        .into_iter()
        .map(|sender| std::thread::spawn(move || send_all(sender)))
        .collect();
    for feeder in feeders {
        assert_eq!(feeder.join().unwrap(), RECORDS as usize);
    }
    join_multi(multi).unwrap();
    assert_eq!(collector.join().unwrap(), RECORDS as usize);
}

/// Dropping the receiver after the dataflow completes is harmless, and a
/// receiver taken after completion still yields everything produced.
#[test]
fn receiver_dropped_after_completion_and_late_take() {
    let rt = runtime(1);
    let builder = DataflowBuilder::<u64>::new("late-take");
    builder
        .source("src", vec![(0u64, vec![1u64, 2, 3])])
        .output("results")
        .unwrap();
    let mut handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let receiver = handle.take_output::<u64>("results").unwrap();
    join(handle).unwrap();
    let mut data = Vec::new();
    while let Some(event) = receiver.recv_timeout(Duration::from_secs(5)) {
        if let OutputEvent::Data { data: batch, .. } = event {
            data.extend(batch);
        }
    }
    assert_eq!(data, vec![1, 2, 3]);
    drop(receiver);
}

/// The receiver is dropped at an arbitrary point while the sink is actively
/// forwarding; repeated to vary the interleaving.
#[test]
fn receiver_drop_races_an_active_sink() {
    for round in 0..20u64 {
        let rt = runtime(2);
        let mut handle = rt
            .spawn(identity_dataflow("drop-race"), SpawnOptions::new())
            .unwrap();
        let receiver = handle.take_output::<u64>("results").unwrap();
        let sender = handle.take_input::<u64>("data").unwrap();
        let consumer = std::thread::spawn(move || {
            for _ in 0..round * 37 {
                if receiver.recv().is_none() {
                    break;
                }
            }
            drop(receiver);
        });
        assert_eq!(send_all(sender), RECORDS as usize, "round {round}");
        consumer.join().unwrap();
        join(handle).unwrap_or_else(|e| panic!("round {round}: {e}"));
    }
}

/// Dropping the whole handle (without joining) while its sink is blocked on
/// an unread output cancels the dataflow and releases its senders.
#[test]
fn handle_dropped_while_sink_is_blocked() {
    let rt = runtime(1);
    let mut handle = rt
        .spawn(identity_dataflow("handle-drop"), SpawnOptions::new())
        .unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    let producer = std::thread::spawn(move || {
        let mut sent = 0usize;
        for i in 0..RECORDS {
            if sender.send(0, vec![i]).is_err() {
                break;
            }
            sent += 1;
        }
        sent
    });
    std::thread::sleep(Duration::from_millis(200));
    drop(handle);
    let sent = within(TIMEOUT, move || producer.join().unwrap())
        .expect("sender stayed blocked after the handle was dropped");
    assert!(
        sent < RECORDS as usize,
        "the cancelled dataflow kept accepting input"
    );
    within(TIMEOUT, move || receiver.collect_data()).expect("output never closed");
    let idle = within(TIMEOUT, move || {
        while rt.active_dataflows() > 0 {
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    assert!(idle.is_some(), "cancelled dataflow never finished");
}

// ---------------------------------------------------------------------------
// Two-node loopback-TCP clusters (Dedicated and Shared transport)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport")]
mod cluster {
    use super::*;

    use instancy::runtime::ClusterSpawnedDataflow;
    use instancy::{ClusterTopology, DataflowId, NodeConfig};

    use super::cluster_loopback::{Transport, spawn_pair};

    fn build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
        builder
            .input::<u64>("data")?
            .exchange("by_val", |v: &u64| *v)
            .map("id", |_t, v| v)
            .with_capacity(SINK_EDGE_CAPACITY)?
            .output("results")?;
        Ok(())
    }

    async fn run(transport: Transport) {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let (nodes, mut a, mut b): (_, ClusterSpawnedDataflow<u64>, ClusterSpawnedDataflow<u64>) =
            spawn_pair(
                transport,
                topology,
                DataflowId::new(),
                build,
                SpawnOptions::new(),
            )
            .await;

        // Node A abandons its output; node B keeps reading.
        drop(a.take_output::<u64>(0, "results").unwrap());
        let out_b = b.take_output::<u64>(0, "results").unwrap();
        let collector = std::thread::spawn(move || {
            let mut values: Vec<u64> = out_b
                .collect_data()
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect();
            values.sort_unstable();
            values
        });
        let sa = a.take_input::<u64>(0, "data").unwrap();
        let sb = b.take_input::<u64>(0, "data").unwrap();
        let feed = |sender: instancy::dataflow::InputSender<u64, u64>, parity: u64| {
            within(TIMEOUT, move || {
                (0..EXCHANGED_RECORDS)
                    .filter(|v| v % 2 == parity)
                    .take_while(|v| sender.send(0, vec![*v]).is_ok())
                    .count()
            })
        };
        let share = Some(EXCHANGED_RECORDS as usize / 2);
        let (fa, fb) = (feed(sa, 0), feed(sb, 1));
        assert_eq!(fa, share, "{transport:?}: node-a sender stalled");
        assert_eq!(fb, share, "{transport:?}: node-b sender stalled");

        // Wait for every worker to finish before resolving either completion
        // (resolving one tears that node's bridges down).
        tokio::time::timeout(TIMEOUT, async {
            nodes.rt_a.wait_idle().await;
            nodes.rt_b.wait_idle().await;
        })
        .await
        .unwrap_or_else(|_| panic!("{transport:?}: cluster stalled after a receiver drop"));
        let (ja, jb) = tokio::join!(
            tokio::task::spawn_blocking(move || within(TIMEOUT, move || a.join_blocking())),
            tokio::task::spawn_blocking(move || within(TIMEOUT, move || b.join_blocking())),
        );
        let ja = ja.unwrap().expect("node-a never completed");
        let jb = jb.unwrap().expect("node-b never completed");
        ja.unwrap_or_else(|e| panic!("{transport:?}: node-a failed: {e}"));
        jb.unwrap_or_else(|e| panic!("{transport:?}: node-b failed: {e}"));
        let kept = collector.join().unwrap();
        assert!(!kept.is_empty(), "{transport:?}: node-b received nothing");
        let mut dedup = kept.clone();
        dedup.dedup();
        assert_eq!(dedup, kept, "{transport:?}: duplicate records");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedicated_cluster_survives_dropped_output_receiver() {
        run(Transport::Dedicated).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_cluster_survives_dropped_output_receiver() {
        run(Transport::Shared).await;
    }
}
