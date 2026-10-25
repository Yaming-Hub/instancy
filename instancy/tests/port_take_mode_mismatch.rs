//! Regression tests: taking a port with the wrong I/O mode (sync vs async) or
//! the wrong data type must fail without consuming the port, so the corrected
//! call still succeeds and no data is lost. Removing the endpoint before the
//! check used to drop it: a dropped receiver silently discards the port's
//! output, and a dropped sender closes the input.

use std::sync::mpsc;
use std::time::Duration;

use instancy::{
    DataflowBuilder, Error, IoMode, MultiSpawnedDataflow, RuntimeConfig, RuntimeError,
    RuntimeHandle, SpawnOptions, SpawnedDataflow,
};

#[cfg(feature = "transport")]
#[path = "support/cluster_loopback.rs"]
mod cluster_loopback;

const TIMEOUT: Duration = Duration::from_secs(20);

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn within<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(TIMEOUT).expect("operation never finished")
}

fn build(builder: &mut DataflowBuilder<u64>) -> instancy::Result<()> {
    builder
        .input::<u64>("data")?
        .map("double", |_t, v| v * 2)
        .output("results")?;
    Ok(())
}

fn dataflow() -> instancy::LogicalDataflow<u64> {
    let mut builder = DataflowBuilder::<u64>::new("mode-mismatch");
    build(&mut builder).unwrap();
    builder.build().unwrap()
}

/// A rejected take reports a mode mismatch naming the correct accessor.
fn assert_mode_mismatch<R>(result: instancy::Result<R>, hint: &str, context: &str) {
    match result {
        Err(Error::Runtime(RuntimeError::InvalidConfig(message))) => assert!(
            message.contains(hint),
            "{context}: error should point to {hint}: {message}"
        ),
        Err(other) => panic!("{context}: unexpected error {other:?}"),
        Ok(_) => panic!("{context}: a mismatched take must fail"),
    }
}

fn sorted(mut values: Vec<u64>) -> Vec<u64> {
    values.sort_unstable();
    values
}

/// Wrong-mode and wrong-type takes on a sync handle, then the correct takes.
fn sync_take_after_mistakes(
    handle: &mut SpawnedDataflow<u64>,
    context: &str,
) -> (
    instancy::InputSender<u64, u64>,
    instancy::OutputReceiver<u64, u64>,
) {
    assert_mode_mismatch(
        handle.take_async_output::<u64>("results"),
        "take_output()",
        context,
    );
    assert_mode_mismatch(
        handle.take_async_input::<u64>("data"),
        "take_input()",
        context,
    );
    assert!(handle.take_output::<i32>("results").is_err());
    assert!(handle.take_input::<i32>("data").is_err());

    let receiver = handle
        .take_output::<u64>("results")
        .unwrap_or_else(|e| panic!("{context}: corrected output take failed: {e}"));
    let sender = handle
        .take_input::<u64>("data")
        .unwrap_or_else(|e| panic!("{context}: corrected input take failed: {e}"));
    assert!(handle.take_output::<u64>("results").is_err(), "taken once");
    assert!(handle.take_input::<u64>("data").is_err(), "taken once");
    (sender, receiver)
}

/// Send `values` on `sender`, close it, and collect what `receiver` gets.
fn send_and_collect(
    sender: instancy::InputSender<u64, u64>,
    receiver: instancy::OutputReceiver<u64, u64>,
    values: Vec<u64>,
) -> std::thread::JoinHandle<Vec<u64>> {
    sender.send(0, values).unwrap();
    drop(sender);
    std::thread::spawn(move || {
        receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect()
    })
}

/// [`sync_take_after_mistakes`], then everything the dataflow emits for
/// `values`.
fn sync_after_mistakes(
    handle: &mut SpawnedDataflow<u64>,
    values: Vec<u64>,
    context: &str,
) -> Vec<u64> {
    let (sender, receiver) = sync_take_after_mistakes(handle, context);
    let collector = send_and_collect(sender, receiver, values);
    within(move || collector.join().unwrap())
}

/// Async counterpart of [`sync_take_after_mistakes`].
fn async_take_after_mistakes(
    handle: &mut SpawnedDataflow<u64>,
    context: &str,
) -> (
    instancy::AsyncInputSender<u64, u64>,
    instancy::AsyncOutputReceiver<u64, u64>,
) {
    assert_mode_mismatch(
        handle.take_output::<u64>("results"),
        "take_async_output()",
        context,
    );
    assert_mode_mismatch(
        handle.take_input::<u64>("data"),
        "take_async_input()",
        context,
    );
    assert!(handle.take_async_output::<i32>("results").is_err());
    assert!(handle.take_async_input::<i32>("data").is_err());

    let receiver = handle
        .take_async_output::<u64>("results")
        .unwrap_or_else(|e| panic!("{context}: corrected output take failed: {e}"));
    let sender = handle
        .take_async_input::<u64>("data")
        .unwrap_or_else(|e| panic!("{context}: corrected input take failed: {e}"));
    assert!(
        handle.take_async_output::<u64>("results").is_err(),
        "taken once"
    );
    assert!(
        handle.take_async_input::<u64>("data").is_err(),
        "taken once"
    );
    (sender, receiver)
}

/// Async counterpart of [`sync_after_mistakes`].
async fn async_after_mistakes(
    handle: &mut SpawnedDataflow<u64>,
    values: Vec<u64>,
    context: &str,
) -> Vec<u64> {
    let (sender, mut receiver) = async_take_after_mistakes(handle, context);
    sender.send(0, values).await.unwrap();
    drop(sender);
    tokio::time::timeout(TIMEOUT, receiver.collect_data())
        .await
        .expect("output never closed")
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect()
}

#[test]
fn async_take_on_sync_ports_leaves_them_in_place() {
    let rt = runtime();
    let mut handle = rt.spawn(dataflow(), SpawnOptions::new()).unwrap();
    let out = sync_after_mistakes(&mut handle, vec![1, 2, 3], "sync single");
    within(move || handle.join_blocking()).unwrap();
    assert_eq!(sorted(out), vec![2, 4, 6]);
}

/// Inputs alone (outputs taken correctly first): a dropped sender would close
/// the input, so the dataflow would finish without the data.
#[test]
fn mismatched_input_take_does_not_close_the_input() {
    let rt = runtime();
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let mut handle = rt
            .spawn(dataflow(), SpawnOptions::new().io_mode(io_mode))
            .unwrap();
        let io = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out: Vec<u64> = match io_mode {
            IoMode::Sync => {
                let receiver = handle.take_output::<u64>("results").unwrap();
                assert!(handle.take_async_input::<u64>("data").is_err());
                let sender = handle.take_input::<u64>("data").unwrap();
                sender.send(0, vec![5]).unwrap();
                drop(sender);
                within(move || receiver.collect_data())
                    .into_iter()
                    .flat_map(|(_, d)| d)
                    .collect()
            }
            IoMode::Async => {
                let mut receiver = handle.take_async_output::<u64>("results").unwrap();
                assert!(handle.take_input::<u64>("data").is_err());
                let sender = handle.take_async_input::<u64>("data").unwrap();
                io.block_on(async move {
                    sender.send(0, vec![5]).await.unwrap();
                    drop(sender);
                    receiver.collect_data().await
                })
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect()
            }
        };
        within(move || handle.join_blocking()).unwrap();
        assert_eq!(out, vec![10], "{io_mode:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_take_on_async_ports_leaves_them_in_place() {
    let rt = runtime();
    let mut handle = rt
        .spawn(dataflow(), SpawnOptions::new().io_mode(IoMode::Async))
        .unwrap();
    let out = async_after_mistakes(&mut handle, vec![1, 2, 3], "async single").await;
    tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .expect("join hung")
        .unwrap();
    assert_eq!(sorted(out), vec![2, 4, 6]);
}

fn multi(rt: &RuntimeHandle, io_mode: IoMode) -> MultiSpawnedDataflow<u64> {
    rt.spawn_multi(
        "mode-mismatch-multi",
        3,
        build,
        SpawnOptions::new().io_mode(io_mode),
    )
    .unwrap()
}

#[test]
fn multi_worker_async_take_on_sync_ports_leaves_them_in_place() {
    let rt = runtime();
    let mut multi = multi(&rt, IoMode::Sync);
    // Per-worker accessors.
    for w in 0..3 {
        assert_mode_mismatch(
            multi.take_async_output::<u64>(w, "results"),
            "take_output()",
            "multi sync",
        );
        assert_mode_mismatch(
            multi.take_async_input::<u64>(w, "data"),
            "take_input()",
            "multi sync",
        );
    }
    // Batch accessors are all-or-nothing.
    assert!(multi.take_all_async_outputs::<u64>("results").is_err());
    assert!(multi.take_all_async_inputs::<u64>("data").is_err());

    // Every worker's input must close before any worker's output does: a
    // worker's sink is downstream of every worker's input operator.
    let ports: Vec<_> = (0..3)
        .map(|w| sync_take_after_mistakes(multi.worker_mut(w), &format!("multi sync worker {w}")))
        .collect();
    let collectors: Vec<_> = ports
        .into_iter()
        .enumerate()
        .map(|(w, (sender, receiver))| {
            send_and_collect(sender, receiver, vec![w as u64 * 10, w as u64 * 10 + 1])
        })
        .collect();
    let out: Vec<u64> = within(move || {
        collectors
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .collect()
    });
    within(move || multi.join_blocking()).unwrap();
    assert_eq!(sorted(out), vec![0, 2, 20, 22, 40, 42]);
}

#[test]
fn multi_worker_batch_take_after_mismatch_takes_every_worker() {
    let rt = runtime();
    let mut multi = multi(&rt, IoMode::Sync);
    assert!(multi.take_async_output::<u64>(1, "results").is_err());
    assert!(multi.take_async_input::<u64>(2, "data").is_err());
    let receivers = multi.take_all_outputs::<u64>("results").unwrap();
    let senders = multi.take_all_inputs::<u64>("data").unwrap();
    assert_eq!((receivers.len(), senders.len()), (3, 3));
    for (w, sender) in senders.into_iter().enumerate() {
        sender.send(0, vec![w as u64]).unwrap();
    }
    let out: Vec<u64> = within(move || {
        receivers
            .into_iter()
            .flat_map(|r| r.collect_data())
            .flat_map(|(_, d)| d)
            .collect()
    });
    within(move || multi.join_blocking()).unwrap();
    assert_eq!(sorted(out), vec![0, 2, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_worker_sync_take_on_async_ports_leaves_them_in_place() {
    let rt = runtime();
    let mut multi = multi(&rt, IoMode::Async);
    for w in 0..3 {
        assert_mode_mismatch(
            multi.take_output::<u64>(w, "results"),
            "take_async_output()",
            "multi async",
        );
        assert_mode_mismatch(
            multi.take_input::<u64>(w, "data"),
            "take_async_input()",
            "multi async",
        );
    }
    assert!(multi.take_all_outputs::<u64>("results").is_err());
    assert!(multi.take_all_inputs::<u64>("data").is_err());

    // Every worker's input must close before any worker's output does: a
    // worker's sink is downstream of every worker's input operator.
    let ports: Vec<_> = (0..3)
        .map(|w| async_take_after_mistakes(multi.worker_mut(w), &format!("multi async worker {w}")))
        .collect();
    let mut receivers = Vec::new();
    for (w, (sender, receiver)) in ports.into_iter().enumerate() {
        sender
            .send(0, vec![w as u64 * 10, w as u64 * 10 + 1])
            .await
            .unwrap();
        drop(sender);
        receivers.push(receiver);
    }
    let mut out = Vec::new();
    for mut receiver in receivers {
        out.extend(
            tokio::time::timeout(TIMEOUT, receiver.collect_data())
                .await
                .expect("output never closed")
                .into_iter()
                .flat_map(|(_, d)| d),
        );
    }
    tokio::time::timeout(TIMEOUT, multi.join())
        .await
        .expect("join hung")
        .unwrap();
    assert_eq!(sorted(out), vec![0, 2, 20, 22, 40, 42]);
}

// ---------------------------------------------------------------------------
// Two-node loopback-TCP clusters (Dedicated and Shared transport)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport")]
mod cluster {
    use super::*;

    use super::cluster_loopback::{Transport, spawn_pair};
    use instancy::{ClusterTopology, DataflowId, NodeConfig};

    async fn run(transport: Transport) {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let (nodes, mut a, mut b) = spawn_pair(
            transport,
            topology,
            DataflowId::new(),
            build,
            SpawnOptions::new(),
        )
        .await;
        let mut outputs = Vec::new();
        for (node, cluster) in [("a", &mut a), ("b", &mut b)] {
            let context = format!("{transport:?} node-{node}");
            assert_mode_mismatch(
                cluster.take_async_output::<u64>(0, "results"),
                "take_output()",
                &context,
            );
            assert_mode_mismatch(
                cluster.take_async_input::<u64>(0, "data"),
                "take_input()",
                &context,
            );
            let receiver = cluster
                .take_output::<u64>(0, "results")
                .unwrap_or_else(|e| panic!("{context}: corrected output take failed: {e}"));
            let sender = cluster
                .take_input::<u64>(0, "data")
                .unwrap_or_else(|e| panic!("{context}: corrected input take failed: {e}"));
            let base = if node == "a" { 1 } else { 100 };
            sender.send(0, vec![base, base + 1]).unwrap();
            outputs.push(receiver);
        }
        tokio::time::timeout(TIMEOUT, async {
            nodes.rt_a.wait_idle().await;
            nodes.rt_b.wait_idle().await;
        })
        .await
        .unwrap_or_else(|_| panic!("{transport:?}: cluster never finished"));
        let out: Vec<u64> = tokio::task::spawn_blocking(move || {
            outputs
                .into_iter()
                .flat_map(|r| r.collect_data())
                .flat_map(|(_, d)| d)
                .collect()
        })
        .await
        .unwrap();
        for cluster in [a, b] {
            tokio::task::spawn_blocking(move || within(move || cluster.join_blocking()))
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("{transport:?}: join failed: {e}"));
        }
        assert_eq!(sorted(out), vec![2, 4, 200, 202]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dedicated_cluster_mode_mismatch_leaves_ports_in_place() {
        run(Transport::Dedicated).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_cluster_mode_mismatch_leaves_ports_in_place() {
        run(Transport::Shared).await;
    }
}
