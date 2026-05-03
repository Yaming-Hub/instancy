//! Integration tests for parallel dataflow execution.
//!
//! These tests validate that the runtime correctly handles multiple dataflows
//! competing for shared resources (worker threads, TCP connections).

#![cfg(feature = "transport")]

use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::DataflowBuilder;
use instancy::dataflow::id::DataflowId;
use instancy::error::Result;
use instancy::execute::{ClusterTopology, NodeConfig};
use instancy::runtime::{RuntimeConfig, RuntimeHandle};

/// Default timeout for test completion.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);

// ===========================================================================
// Test 1: Shared worker pool — multiple dataflows on a single runtime
// ===========================================================================

/// Spawn N dataflows on a single RuntimeHandle with a small thread pool,
/// feed them timestamp-by-timestamp in lockstep, and verify all complete
/// correctly.
///
/// This validates:
/// - Worker pool multiplexes multiple dataflows on limited threads
/// - Task scheduling doesn't starve any dataflow
/// - Progress tracking is independent per dataflow
/// - No deadlocks when all dataflows compete for the same pool
#[test]
fn shared_pool_parallel_dataflows_no_exchange() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let num_dataflows = 5;
    let num_epochs = 10u64;

    // Spawn N simple pipeline dataflows: input → map(double) → output
    let mut dataflows = Vec::new();
    for i in 0..num_dataflows {
        let builder = DataflowBuilder::<u64>::new(format!("df-{i}"));
        builder
            .input::<i64>("data")
            .map("double", |_t, x| x * 2)
            .output("results");
        let logical = builder.build().unwrap();
        let spawned = rt.spawn(logical).unwrap();
        dataflows.push(spawned);
    }

    // Take all inputs and outputs.
    let mut senders = Vec::new();
    let mut outputs = Vec::new();
    for df in dataflows.iter_mut() {
        senders.push(df.take_input::<i64>("data").unwrap());
        outputs.push(df.take_output::<i64>("results").unwrap());
    }

    // Feed data epoch-by-epoch in lockstep across all dataflows.
    for epoch in 0..num_epochs {
        for (df_idx, sender) in senders.iter().enumerate() {
            let base = (df_idx as i64) * 1000 + (epoch as i64) * 10;
            sender.send(epoch, (base..base + 5).collect()).unwrap();
        }
    }

    // Close all inputs.
    drop(senders);

    // Join all dataflows.
    for df in dataflows {
        df.join_blocking().unwrap();
    }

    // Verify each dataflow's output.
    for (df_idx, output) in outputs.into_iter().enumerate() {
        let mut all: Vec<(u64, Vec<i64>)> = output.collect_data();
        all.sort_by_key(|(t, _)| *t);

        let mut total_count = 0;
        for (epoch, data) in &all {
            let base = (df_idx as i64) * 1000 + (*epoch as i64) * 10;
            let expected: Vec<i64> = (base..base + 5).map(|x| x * 2).collect();
            assert_eq!(data, &expected, "df-{df_idx} epoch {epoch}");
            total_count += data.len();
        }
        assert_eq!(total_count, (num_epochs as usize) * 5, "df-{df_idx} total count");
    }
}

/// Same as above but with multi-worker exchange dataflows sharing the pool.
#[test]
fn shared_pool_parallel_dataflows_with_exchange() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let num_dataflows = 3;
    let num_workers = 2;
    let num_epochs = 8u64;

    // Spawn N multi-worker exchange dataflows.
    let mut dataflows = Vec::new();
    for i in 0..num_dataflows {
        let df = rt
            .spawn_multi(&format!("ex-df-{i}"), num_workers, |_worker_idx, builder| {
                let input = builder.input::<i64>("data");
                let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
                exchanged.map("pass", |_t, x| x).output("results");
                Ok(())
            })
            .unwrap();
        dataflows.push(df);
    }

    // Take inputs/outputs for worker 0 of each dataflow.
    let mut senders = Vec::new();
    let mut all_outputs: Vec<Vec<_>> = Vec::new();
    for df in dataflows.iter_mut() {
        let mut df_outputs = Vec::new();
        for w in 0..num_workers {
            df_outputs.push(df.take_output::<i64>(w, "results").unwrap());
        }
        all_outputs.push(df_outputs);

        // Send data only through worker 0; close worker 1's input.
        senders.push(df.take_input::<i64>(0, "data").unwrap());
        drop(df.take_input::<i64>(1, "data").unwrap());
    }

    // Feed epoch-by-epoch.
    for epoch in 0..num_epochs {
        for (df_idx, sender) in senders.iter().enumerate() {
            let base = (df_idx as i64) * 1000 + (epoch as i64) * 10;
            sender.send(epoch, (base..base + 10).collect()).unwrap();
        }
    }
    drop(senders);

    // Join all.
    for df in dataflows {
        df.join_blocking().unwrap();
    }

    // Verify each dataflow received all its data (across all workers).
    for (df_idx, df_outputs) in all_outputs.into_iter().enumerate() {
        let mut all: Vec<i64> = df_outputs
            .into_iter()
            .flat_map(|o| o.collect_data().into_iter().flat_map(|(_, d)| d))
            .collect();
        all.sort();

        let mut expected: Vec<i64> = Vec::new();
        for epoch in 0..num_epochs {
            let base = (df_idx as i64) * 1000 + (epoch as i64) * 10;
            expected.extend(base..base + 10);
        }
        expected.sort();
        assert_eq!(all, expected, "exchange df-{df_idx}");
    }
}

/// Stress variant: more dataflows, more epochs, verify no starvation.
#[test]
#[ignore] // stress test — run with `cargo test --ignored`
fn stress_shared_pool_many_dataflows() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let num_dataflows = 10;
    let num_epochs = 50u64;

    let mut dataflows = Vec::new();
    for i in 0..num_dataflows {
        let builder = DataflowBuilder::<u64>::new(format!("stress-{i}"));
        builder
            .input::<i64>("data")
            .map("triple", |_t, x| x * 3)
            .output("results");
        let logical = builder.build().unwrap();
        dataflows.push(rt.spawn(logical).unwrap());
    }

    let mut senders = Vec::new();
    let mut outputs = Vec::new();
    for df in dataflows.iter_mut() {
        senders.push(df.take_input::<i64>("data").unwrap());
        outputs.push(df.take_output::<i64>("results").unwrap());
    }

    // Lockstep feeding.
    for epoch in 0..num_epochs {
        for (df_idx, sender) in senders.iter().enumerate() {
            let val = (df_idx as i64) * 10000 + (epoch as i64);
            sender.send(epoch, vec![val]).unwrap();
        }
    }
    drop(senders);

    for df in dataflows {
        df.join_blocking().unwrap();
    }

    for (df_idx, output) in outputs.into_iter().enumerate() {
        let count: usize = output
            .collect_data()
            .into_iter()
            .map(|(_, d)| d.len())
            .sum();
        assert_eq!(
            count, num_epochs as usize,
            "stress df-{df_idx}: expected {num_epochs} records"
        );
    }
}

// ===========================================================================
// Test 2: Shared TCP transport — parallel cluster dataflows
// ===========================================================================

/// Helper: create TCP connection pairs between two nodes.
async fn make_tcp_pair(
) -> (
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>,
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();

    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();

    let (ra, wa) = accepted.0.into_split();
    let (rb, wb) = connected.into_split();

    let conn_a = PeerConnection {
        node_id: "node-b".to_string(),
        reader: ra,
        writer: wa,
    };
    let conn_b = PeerConnection {
        node_id: "node-a".to_string(),
        reader: rb,
        writer: wb,
    };
    (conn_a, conn_b)
}

/// Helper: join a cluster with timeout.
async fn join_cluster_with_timeout(
    cluster: instancy::runtime::ClusterSpawnedDataflow<u64>,
) {
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        tokio::task::spawn_blocking(move || cluster.join_blocking()),
    )
    .await;
    match result {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("cluster join failed: {e}"),
        Ok(Err(e)) => panic!("spawn_blocking panicked: {e}"),
        Err(_) => panic!("cluster did not complete within {TEST_TIMEOUT:?}"),
    }
}

/// Multiple cluster dataflows running in parallel, each with its own TCP
/// connection pair between two nodes.
///
/// Validates:
/// - Multiple independent cluster dataflows can run concurrently
/// - Each dataflow's exchange routes data correctly via its own TCP connection
/// - No cross-dataflow interference
/// - All dataflows complete successfully
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_transport_parallel_cluster_dataflows() {
    let num_dataflows = 3;
    let num_epochs = 5u64;

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    // Spawn N cluster dataflows, each with its own TCP connection
    // and its own RuntimeHandle per node.
    let mut clusters_a = Vec::new();
    let mut clusters_b = Vec::new();

    for _df_idx in 0..num_dataflows {
        let (conn_a, conn_b) = make_tcp_pair().await;
        let dataflow_id = DataflowId::new();

        let build = |_worker_idx: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
            let input = builder.input::<i64>("data");
            let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
            exchanged.map("pass", |_t, x| x).output("results");
            Ok(())
        };

        let topo_a = topology.clone();
        let topo_b = topology.clone();

        // Spawn both nodes for this dataflow.
        let ha = {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 4,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let th = tokio_handle.clone();
            tokio::task::spawn_blocking(move || {
                let cluster = rt.spawn_cluster(
                    "parallel-test",
                    topo_a,
                    "node-a",
                    dataflow_id,
                    vec![conn_a],
                    1024,
                    Duration::from_secs(10),
                    build,
                    &th,
                )?;
                Ok::<_, instancy::error::Error>((rt, cluster))
            })
        };

        let hb = {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 4,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let th = tokio_handle.clone();
            tokio::task::spawn_blocking(move || {
                let cluster = rt.spawn_cluster(
                    "parallel-test",
                    topo_b,
                    "node-b",
                    dataflow_id,
                    vec![conn_b],
                    1024,
                    Duration::from_secs(10),
                    build,
                    &th,
                )?;
                Ok::<_, instancy::error::Error>((rt, cluster))
            })
        };

        let (ra, rb) = tokio::join!(ha, hb);
        let (_rt_a_df, ca) = ra.unwrap().unwrap();
        let (_rt_b_df, cb) = rb.unwrap().unwrap();
        clusters_a.push((_rt_a_df, ca));
        clusters_b.push((_rt_b_df, cb));
    }

    // Take inputs/outputs for all dataflows.
    let mut senders_a = Vec::new();
    let mut outputs_a = Vec::new();
    let mut outputs_b = Vec::new();

    for (_, ca) in clusters_a.iter_mut() {
        outputs_a.push(ca.take_output::<i64>(0, "results").unwrap());
        senders_a.push(ca.take_input::<i64>(0, "data").unwrap());
    }
    for (_, cb) in clusters_b.iter_mut() {
        outputs_b.push(cb.take_output::<i64>(0, "results").unwrap());
        // Close node-b inputs immediately.
        drop(cb.take_input::<i64>(0, "data").unwrap());
    }

    // Feed data epoch-by-epoch to all node-a dataflows in lockstep.
    for epoch in 0..num_epochs {
        for (df_idx, sender) in senders_a.iter().enumerate() {
            let base = (df_idx as i64) * 1000 + (epoch as i64) * 10;
            sender.send(epoch, (base..base + 10).collect()).unwrap();
        }
    }
    drop(senders_a);

    // Join all clusters — keep RuntimeHandles alive until joins complete.
    let mut rts_a = Vec::new();
    let mut rts_b = Vec::new();
    let mut join_handles = Vec::new();
    for (rt, ca) in clusters_a {
        rts_a.push(rt);
        join_handles.push(tokio::spawn(join_cluster_with_timeout(ca)));
    }
    for (rt, cb) in clusters_b {
        rts_b.push(rt);
        join_handles.push(tokio::spawn(join_cluster_with_timeout(cb)));
    }
    for h in join_handles {
        h.await.unwrap();
    }
    drop(rts_a);
    drop(rts_b);

    // Verify each dataflow received all its data across both nodes.
    for df_idx in 0..num_dataflows {
        let mut all: Vec<i64> = outputs_a
            .remove(0)
            .collect_data()
            .into_iter()
            .chain(outputs_b.remove(0).collect_data().into_iter())
            .flat_map(|(_, d)| d)
            .collect();
        all.sort();

        let mut expected: Vec<i64> = Vec::new();
        for epoch in 0..num_epochs {
            let base = (df_idx as i64) * 1000 + (epoch as i64) * 10;
            expected.extend(base..base + 10);
        }
        expected.sort();
        assert_eq!(all, expected, "cluster df-{df_idx}");
    }
}

/// Stress test: more parallel cluster dataflows with higher data volume.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // stress test — run with `cargo test --ignored`
async fn stress_parallel_cluster_dataflows() {
    let num_dataflows = 5;
    let num_epochs = 20u64;
    let records_per_epoch = 50;

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    let mut clusters_a = Vec::new();
    let mut clusters_b = Vec::new();

    for _df_idx in 0..num_dataflows {
        let (conn_a, conn_b) = make_tcp_pair().await;
        let dataflow_id = DataflowId::new();

        let build = |_worker_idx: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
            let input = builder.input::<i64>("data");
            let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
            exchanged.map("pass", |_t, x| x).output("results");
            Ok(())
        };

        let topo_a = topology.clone();
        let topo_b = topology.clone();

        let ha = {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let th = tokio_handle.clone();
            tokio::task::spawn_blocking(move || {
                let cluster = rt.spawn_cluster(
                    "stress-parallel",
                    topo_a,
                    "node-a",
                    dataflow_id,
                    vec![conn_a],
                    1024,
                    Duration::from_secs(10),
                    build,
                    &th,
                )?;
                Ok::<_, instancy::error::Error>((rt, cluster))
            })
        };

        let hb = {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..RuntimeConfig::default()
            })
            .unwrap();
            let th = tokio_handle.clone();
            tokio::task::spawn_blocking(move || {
                let cluster = rt.spawn_cluster(
                    "stress-parallel",
                    topo_b,
                    "node-b",
                    dataflow_id,
                    vec![conn_b],
                    1024,
                    Duration::from_secs(10),
                    build,
                    &th,
                )?;
                Ok::<_, instancy::error::Error>((rt, cluster))
            })
        };

        let (ra, rb) = tokio::join!(ha, hb);
        clusters_a.push(ra.unwrap().unwrap());
        clusters_b.push(rb.unwrap().unwrap());
    }

    // Take inputs/outputs.
    let mut senders_a = Vec::new();
    let mut outputs_a = Vec::new();
    let mut outputs_b = Vec::new();

    for (_, ca) in clusters_a.iter_mut() {
        outputs_a.push(ca.take_output::<i64>(0, "results").unwrap());
        senders_a.push(ca.take_input::<i64>(0, "data").unwrap());
    }
    for (_, cb) in clusters_b.iter_mut() {
        outputs_b.push(cb.take_output::<i64>(0, "results").unwrap());
        drop(cb.take_input::<i64>(0, "data").unwrap());
    }

    // Feed all dataflows epoch-by-epoch.
    for epoch in 0..num_epochs {
        for (df_idx, sender) in senders_a.iter().enumerate() {
            let base = (df_idx as i64) * 100_000 + (epoch as i64) * records_per_epoch;
            sender
                .send(epoch, (base..base + records_per_epoch).collect())
                .unwrap();
        }
    }
    drop(senders_a);

    // Join all — keep RuntimeHandles alive until joins complete.
    let mut rts_a = Vec::new();
    let mut rts_b = Vec::new();
    let mut join_handles = Vec::new();
    for (rt, ca) in clusters_a {
        rts_a.push(rt);
        join_handles.push(tokio::spawn(join_cluster_with_timeout(ca)));
    }
    for (rt, cb) in clusters_b {
        rts_b.push(rt);
        join_handles.push(tokio::spawn(join_cluster_with_timeout(cb)));
    }
    for h in join_handles {
        h.await.unwrap();
    }
    drop(rts_a);
    drop(rts_b);

    // Verify totals.
    let expected_per_df = (num_epochs as usize) * (records_per_epoch as usize);
    for df_idx in 0..num_dataflows {
        let count: usize = outputs_a
            .remove(0)
            .collect_data()
            .into_iter()
            .chain(outputs_b.remove(0).collect_data().into_iter())
            .map(|(_, d)| d.len())
            .sum();
        assert_eq!(
            count, expected_per_df,
            "stress cluster df-{df_idx}: expected {expected_per_df}"
        );
    }
}
