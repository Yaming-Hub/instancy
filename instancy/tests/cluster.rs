//! Integration tests for `spawn_cluster()` — multi-node dataflow execution.
//!
//! Uses `tokio::io::duplex` to simulate network connections between nodes
//! without actual TCP sockets.

#![cfg(feature = "transport")]

use std::time::Duration;

use instancy::DataflowBuilder;
use instancy::DataflowId;
use instancy::Result;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{ClusterTopology, NodeConfig};
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::metrics::MetricsConfig;

/// Helper: create duplex connections between two nodes.
///
/// Returns `(conn_for_node_a, conn_for_node_b)` — each node gets a
/// PeerConnection pointing to the other.
fn make_duplex_pair(
    node_a: &str,
    node_b: &str,
    buffer_size: usize,
) -> (
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
) {
    let (a_to_b, b_from_a) = tokio::io::duplex(buffer_size);
    let (b_to_a, a_from_b) = tokio::io::duplex(buffer_size);

    let conn_a = PeerConnection {
        node_id: node_b.to_string(),
        reader: a_from_b,
        writer: a_to_b,
    };
    let conn_b = PeerConnection {
        node_id: node_a.to_string(),
        reader: b_from_a,
        writer: b_to_a,
    };
    (conn_a, conn_b)
}

/// Two-node cluster with no exchange edges (pure pipeline).
///
/// Each node runs 1 worker. The dataflow is: input → map(double) → output.
/// No cross-node exchange needed.
#[tokio::test]
async fn cluster_two_nodes_no_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i32>("data").unwrap();
        input
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        Ok(())
    };

    // Spawn both nodes in parallel using blocking tasks.
    let topo_a = topology.clone();
    let topo_b = topology.clone();
    let th_a = tokio_handle.clone();
    let th_b = tokio_handle.clone();

    let handle_a = tokio::task::spawn_blocking(move || {
        let cluster = rt_a.spawn_cluster(
            "test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            build,
            &th_a,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            build,
            &th_b,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_b, c))
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut cluster_a) = result_a.unwrap().unwrap();
    let (_rt_b, mut cluster_b) = result_b.unwrap().unwrap();

    // Validate worker ranges.
    assert_eq!(cluster_a.local_worker_range(), (0, 1));
    assert_eq!(cluster_b.local_worker_range(), (1, 2));
    assert_eq!(cluster_a.total_workers(), 2);
    assert_eq!(cluster_b.total_workers(), 2);

    let output_a = cluster_a.take_output::<i32>(0, "results").unwrap();
    let output_b = cluster_b.take_output::<i32>(0, "results").unwrap();

    // Feed data to node-a's worker.
    let sender_a = cluster_a.take_input::<i32>(0, "data").unwrap();
    sender_a.send(0, vec![1, 2, 3]).unwrap();
    drop(sender_a);

    // Feed data to node-b's worker.
    let sender_b = cluster_b.take_input::<i32>(0, "data").unwrap();
    sender_b.send(0, vec![10, 20]).unwrap();
    drop(sender_b);

    // Wait for completion.
    cluster_a.join_blocking().unwrap();
    cluster_b.join_blocking().unwrap();

    // Collect results.
    let data_a: Vec<i32> = output_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let data_b: Vec<i32> = output_b
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    assert_eq!(data_a, vec![2, 4, 6]);
    assert_eq!(data_b, vec![20, 40]);
}

/// Two-node cluster with exchange — data is repartitioned across nodes.
///
/// Each node runs 1 worker. The dataflow is:
/// input → exchange(by value) → map(identity) → output
///
/// Data sent to node-a may end up on node-b and vice versa.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_two_nodes_with_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        // Exchange by value — each record goes to worker (value % num_workers).
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged
            .map("identity", |_t, x| x)
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let topo_b = topology.clone();
    let th_a = tokio_handle.clone();
    let th_b = tokio_handle.clone();

    let handle_a = tokio::task::spawn_blocking(move || {
        let cluster = rt_a.spawn_cluster(
            "exchange-test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            build,
            &th_a,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "exchange-test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            build,
            &th_b,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_b, c))
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut cluster_a) = result_a.unwrap().unwrap();
    let (_rt_b, mut cluster_b) = result_b.unwrap().unwrap();

    let output_a = cluster_a.take_output::<i64>(0, "results").unwrap();
    let output_b = cluster_b.take_output::<i64>(0, "results").unwrap();

    // Send values 0..10 to node-a. After exchange(by_val), even values
    // go to worker 0 (node-a), odd values go to worker 1 (node-b).
    let sender_a = cluster_a.take_input::<i64>(0, "data").unwrap();
    sender_a.send(0, (0..10).collect()).unwrap();
    drop(sender_a);

    // Close node-b's input (empty, but must be closed for dataflow to complete).
    let sender_b = cluster_b.take_input::<i64>(0, "data").unwrap();
    drop(sender_b);

    // Wait for completion.
    cluster_a.join_blocking().unwrap();
    cluster_b.join_blocking().unwrap();

    // Collect results.
    let mut data_a: Vec<i64> = output_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let mut data_b: Vec<i64> = output_b
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    data_a.sort();
    data_b.sort();

    // All 10 values should appear across the two nodes, partitioned by hash.
    let mut all: Vec<i64> = data_a.iter().chain(data_b.iter()).copied().collect();
    all.sort();
    assert_eq!(all, (0..10).collect::<Vec<i64>>());
}

/// Validate that fingerprint mismatch is caught during handshake.
#[tokio::test]
async fn cluster_fingerprint_mismatch() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    // Node A: input → map → output (2 operators + source)
    let build_a = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder
            .input::<i32>("data")
            .unwrap()
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        Ok(())
    };

    // Node B: input → output (1 operator + source) — different graph!
    let build_b = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder
            .input::<i32>("data")
            .unwrap()
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let topo_b = topology.clone();
    let th_a = tokio_handle.clone();
    let th_b = tokio_handle.clone();

    let handle_a = tokio::task::spawn_blocking(move || {
        rt_a.spawn_cluster(
            "mismatch",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            build_a,
            &th_a,
            SpawnOptions::new(),
        )
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        rt_b.spawn_cluster(
            "mismatch",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            build_b,
            &th_b,
            SpawnOptions::new(),
        )
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);

    // At least one side should fail with fingerprint mismatch.
    let err_a = result_a.unwrap();
    let err_b = result_b.unwrap();

    let any_failed = err_a.is_err() || err_b.is_err();
    assert!(any_failed, "expected fingerprint mismatch error");

    // Check that the error message mentions fingerprint or mismatch.
    let check_err = |r: instancy::error::Result<_>| {
        if let Err(e) = r {
            let msg = format!("{e}");
            assert!(
                msg.contains("fingerprint")
                    || msg.contains("mismatch")
                    || msg.contains("handshake"),
                "unexpected error: {msg}"
            );
        }
    };
    check_err(err_a);
    check_err(err_b);
}

/// Validate connection validation (missing peer).
#[tokio::test]
async fn cluster_missing_connection() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
        NodeConfig::new("node-c", 1),
    ])
    .unwrap();

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    // Only provide connection to node-b, missing node-c.
    let (conn_a, _conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let result = rt.spawn_cluster(
        "missing",
        topology,
        "node-a",
        DataflowId::new(),
        ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
        Duration::from_secs(1),
        |builder: &mut DataflowBuilder<u64>| {
            builder.input::<i32>("data").unwrap().output("out").unwrap();
            Ok(())
        },
        &tokio_handle,
        SpawnOptions::new(),
    );

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("missing"),
        "expected 'missing' in error: {msg}"
    );
}

/// Two-node cluster: cancelling one node propagates cancellation to the other.
///
/// Node-a runs a long-running operator that blocks until cancelled.
/// After both nodes are spawned, we cancel node-a. Node-b should receive
/// `CancellationReason::PeerCancelled` and complete promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_cancel_propagates_to_peer() {
    use instancy::cancellation::CancellationReason;

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    // Build a simple pipeline. The operators themselves don't need to be
    // long-running — we control the hang by never closing inputs until
    // after cancel propagates.
    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i32>("data").unwrap();
        input.map("identity", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let topo_b = topology.clone();
    let th_a = tokio_handle.clone();
    let th_b = tokio_handle.clone();

    let handle_a = tokio::task::spawn_blocking(move || {
        let cluster = rt_a.spawn_cluster(
            "cancel_test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            build,
            &th_a,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "cancel_test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            build,
            &th_b,
            SpawnOptions::new(),
        );
        cluster.map(|c| (rt_b, c))
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (_rt_a, cluster_a) = result_a.unwrap().unwrap();
    let (_rt_b, cluster_b) = result_b.unwrap().unwrap();

    // Do NOT close inputs — operators are blocked waiting for data.
    // Without distributed cancellation, node-b would hang forever.

    // Cancel node-a with a reason.
    cluster_a.cancel_with_reason(CancellationReason::UserRequested);

    // Node-b should complete within a reasonable timeout — the distributed
    // cancel broadcaster on node-a sends Cancel, and node-b's listener fires
    // its dataflow_cancel token.
    let completion_b = cluster_b.join().unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), completion_b).await;
    assert!(
        result.is_ok(),
        "node-b should have completed after node-a cancel propagated"
    );
}

/// Verify that cluster observability metrics are collected when enabled via SpawnOptions.
///
/// Two-node cluster with `MetricsConfig::summary_only()` — after running a simple
/// pipeline, both nodes should report operator metrics (activations, records).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_observability_metrics_collected() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i32>("data").unwrap();
        input
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        Ok(())
    };

    let opts = SpawnOptions::new().metrics(MetricsConfig::summary_only());

    let topo_a = topology.clone();
    let topo_b = topology.clone();
    let th_a = tokio_handle.clone();
    let th_b = tokio_handle.clone();
    let opts_a = opts.clone();
    let opts_b = opts.clone();

    let handle_a = tokio::task::spawn_blocking(move || {
        let cluster = rt_a.spawn_cluster(
            "obs-test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            build,
            &th_a,
            opts_a,
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "obs-test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            build,
            &th_b,
            opts_b,
        );
        cluster.map(|c| (rt_b, c))
    });

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut cluster_a) = result_a.unwrap().unwrap();
    let (_rt_b, mut cluster_b) = result_b.unwrap().unwrap();

    // Feed data and close inputs.
    let sender_a = cluster_a.take_input::<i32>(0, "data").unwrap();
    sender_a.send(0, vec![1, 2, 3]).unwrap();
    drop(sender_a);

    let sender_b = cluster_b.take_input::<i32>(0, "data").unwrap();
    sender_b.send(0, vec![10, 20]).unwrap();
    drop(sender_b);

    let output_a = cluster_a.take_output::<i32>(0, "results").unwrap();
    let output_b = cluster_b.take_output::<i32>(0, "results").unwrap();

    // Verify metrics are accessible before join.
    let metrics_a = cluster_a
        .worker_metrics(0)
        .expect("metrics should be Some when summary_only() is configured")
        .clone();

    let metrics_b = cluster_b
        .worker_metrics(0)
        .expect("metrics should be Some when summary_only() is configured")
        .clone();

    assert_eq!(metrics_a.operator_count(), 3); // source + map + sink

    // Wait for completion.
    cluster_a.join_blocking().unwrap();
    cluster_b.join_blocking().unwrap();

    // After completion, metrics should reflect activations.
    assert!(
        metrics_a.total_activations() > 0,
        "node-a should have activations"
    );
    // Verify operator structure is correct.
    let snap_a = metrics_a.operator_snapshots();
    assert!(
        !snap_a.is_empty(),
        "node-a should have operator snapshots"
    );

    assert!(
        metrics_b.total_activations() > 0,
        "node-b should have activations"
    );

    // Verify outputs still work.
    let data_a: Vec<i32> = output_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let data_b: Vec<i32> = output_b
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    assert!(!data_a.is_empty(), "node-a should have output");
    assert!(!data_b.is_empty(), "node-b should have output");
}