//! Integration tests for `spawn_cluster()` — multi-node dataflow execution.
//!
//! Uses `tokio::io::duplex` to simulate network connections between nodes
//! without actual TCP sockets.

#![cfg(feature = "transport")]

use std::time::Duration;

use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::DataflowBuilder;
use instancy::dataflow::id::DataflowId;
use instancy::error::Result;
use instancy::execute::{ClusterTopology, NodeConfig};
use instancy::runtime::{RuntimeConfig, RuntimeHandle};

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

    let build = |_worker_idx: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i32>("data");
        input.map("double", |_t, x| x * 2).output("results");
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
            vec![conn_a],
            1024,
            Duration::from_secs(5),
            build,
            &th_a,
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "test",
            topo_b,
            "node-b",
            dataflow_id,
            vec![conn_b],
            1024,
            Duration::from_secs(5),
            build,
            &th_b,
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
    let data_a: Vec<i32> = output_a.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    let data_b: Vec<i32> = output_b.collect_data().into_iter().flat_map(|(_, d)| d).collect();

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

    let build = |_worker_idx: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data");
        // Exchange by value — each record goes to worker (value % num_workers).
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("identity", |_t, x| x).output("results");
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
            vec![conn_a],
            1024,
            Duration::from_secs(5),
            build,
            &th_a,
        );
        cluster.map(|c| (rt_a, c))
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        let cluster = rt_b.spawn_cluster(
            "exchange-test",
            topo_b,
            "node-b",
            dataflow_id,
            vec![conn_b],
            1024,
            Duration::from_secs(5),
            build,
            &th_b,
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
    let mut data_a: Vec<i64> = output_a.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    let mut data_b: Vec<i64> = output_b.collect_data().into_iter().flat_map(|(_, d)| d).collect();
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
    let build_a = |_: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder.input::<i32>("data").map("double", |_t, x| x * 2).output("results");
        Ok(())
    };

    // Node B: input → output (1 operator + source) — different graph!
    let build_b = |_: usize, builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder.input::<i32>("data").output("results");
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
            vec![conn_a],
            1024,
            Duration::from_secs(5),
            build_a,
            &th_a,
        )
    });

    let handle_b = tokio::task::spawn_blocking(move || {
        rt_b.spawn_cluster(
            "mismatch",
            topo_b,
            "node-b",
            dataflow_id,
            vec![conn_b],
            1024,
            Duration::from_secs(5),
            build_b,
            &th_b,
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
                msg.contains("fingerprint") || msg.contains("mismatch") || msg.contains("handshake"),
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
        vec![conn_a],
        1024,
        Duration::from_secs(1),
        |_, builder: &mut DataflowBuilder<u64>| {
            builder.input::<i32>("data").output("out");
            Ok(())
        },
        &tokio_handle,
    );

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(msg.contains("missing"), "expected 'missing' in error: {msg}");
}
