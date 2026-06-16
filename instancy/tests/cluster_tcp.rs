//! TCP-based cluster integration tests.
//!
//! Unlike `cluster.rs` (which uses `tokio::io::duplex`), these tests create
//! real TCP connections between runtime instances running in the same process.
//! This exercises the full network path including OS-level TCP buffers,
//! Nagle's algorithm, kernel scheduling, and real I/O polling.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};

use instancy::DataflowBuilder;
use instancy::DataflowId;
use instancy::Result;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{ClusterTopology, NodeConfig};
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Default timeout for cluster completion in tests.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Join a cluster with a timeout to prevent tests from hanging indefinitely.
///
/// Wraps `join_blocking()` in a `tokio::time::timeout` so that CI runners
/// get a clear failure instead of a silent hang.
async fn join_with_timeout(cluster: instancy::runtime::ClusterSpawnedDataflow<u64>) {
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

// ---------------------------------------------------------------------------
// TCP connection helpers
// ---------------------------------------------------------------------------

/// For each ordered pair `(i, j)` where `i < j` in the `node_ids` slice,
/// bind a listener on 127.0.0.1:0, then have the `j` side connect.
/// Returns a map from `node_id → Vec<PeerConnection>`.
///
/// The node with the smaller index in the `node_ids` slice listens;
/// the other connects.
async fn make_tcp_connections(
    node_ids: &[&str],
) -> HashMap<
    String,
    Vec<PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>>,
> {
    let mut result: HashMap<String, Vec<_>> = HashMap::new();
    for id in node_ids {
        result.insert(id.to_string(), Vec::new());
    }

    // For every pair (i, j) where i < j, node_ids[i] listens, node_ids[j] connects.
    for i in 0..node_ids.len() {
        for j in (i + 1)..node_ids.len() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            // Connect from j → i (listener side).
            let (accepted, connected) =
                tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
            let stream_i = accepted.0;
            let stream_j = connected;

            // Disable Nagle for lower latency in tests.
            stream_i.set_nodelay(true).unwrap();
            stream_j.set_nodelay(true).unwrap();

            let (ri, wi) = stream_i.into_split();
            let (rj, wj) = stream_j.into_split();

            // node_ids[i] gets a connection to node_ids[j].
            result.get_mut(node_ids[i]).unwrap().push(PeerConnection {
                node_id: node_ids[j].to_string(),
                reader: ri,
                writer: wi,
            });

            // node_ids[j] gets a connection to node_ids[i].
            result.get_mut(node_ids[j]).unwrap().push(PeerConnection {
                node_id: node_ids[i].to_string(),
                reader: rj,
                writer: wj,
            });
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Helper: spawn a cluster node on a blocking task, returning (rt, cluster).
// ---------------------------------------------------------------------------

/// Spawns a single node's `spawn_cluster()` on a blocking task.
///
/// Returns `(RuntimeHandle, ClusterSpawnedDataflow)` — the RuntimeHandle must
/// be kept alive to prevent worker cancellation.
fn spawn_node<F>(
    topology: ClusterTopology,
    node_id: &str,
    dataflow_id: DataflowId,
    connections: Vec<
        PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>,
    >,
    worker_threads: usize,
    tokio_handle: tokio::runtime::Handle,
    build: F,
) -> tokio::task::JoinHandle<
    Result<(
        RuntimeHandle,
        instancy::runtime::ClusterSpawnedDataflow<u64>,
    )>,
>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let node_id = node_id.to_string();
    tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "tcp-test",
            topology,
            &node_id,
            dataflow_id,
            ClusterSpawnTransport::dedicated(connections, 1024),
            Duration::from_secs(10),
            build,
            &tokio_handle,
            SpawnOptions::new(),
        )?;
        Ok((rt, cluster))
    })
}

// ===========================================================================
// Unit tests — correctness
// ===========================================================================

/// Two-node cluster over real TCP, no exchange edges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_two_nodes_no_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder
            .input::<i32>("data")
            .unwrap()
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        1,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        1,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    // Feed data.
    let out_a = ca.take_output::<i32>(0, "results").unwrap();
    let out_b = cb.take_output::<i32>(0, "results").unwrap();

    let sa = ca.take_input::<i32>(0, "data").unwrap();
    sa.send(0, vec![1, 2, 3]).unwrap();
    drop(sa);

    let sb = cb.take_input::<i32>(0, "data").unwrap();
    sb.send(0, vec![10, 20]).unwrap();
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    let mut da: Vec<i32> = out_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let mut db: Vec<i32> = out_b
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    da.sort();
    db.sort();

    assert_eq!(da, vec![2, 4, 6]);
    assert_eq!(db, vec![20, 40]);
}

/// Two-node cluster over real TCP with exchange — data repartitioned across nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_two_nodes_with_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged
            .map("identity", |_t, x| x)
            .output("results")
            .unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    // Send 0..20 to node-a.
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    sa.send(0, (0..20).collect()).unwrap();
    drop(sa);

    // Close node-b input.
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    let mut all: Vec<i64> = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data().into_iter())
        .flat_map(|(_, d)| d)
        .collect();
    all.sort();
    assert_eq!(all, (0..20).collect::<Vec<i64>>());
}

/// Three-node cluster over real TCP with exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tcp_three_nodes_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("alpha", 1),
        NodeConfig::new("beta", 1),
        NodeConfig::new("gamma", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["alpha", "beta", "gamma"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let h_alpha = spawn_node(
        topology.clone(),
        "alpha",
        dataflow_id,
        conns.remove("alpha").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let h_beta = spawn_node(
        topology.clone(),
        "beta",
        dataflow_id,
        conns.remove("beta").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let h_gamma = spawn_node(
        topology,
        "gamma",
        dataflow_id,
        conns.remove("gamma").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb, rc) = tokio::join!(h_alpha, h_beta, h_gamma);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();
    let (_rt_c, mut cc) = rc.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();
    let out_c = cc.take_output::<i64>(0, "results").unwrap();

    // Send data from alpha only; beta and gamma get empty inputs.
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    sa.send(0, (0..30).collect()).unwrap();
    drop(sa);

    drop(cb.take_input::<i64>(0, "data").unwrap());
    drop(cc.take_input::<i64>(0, "data").unwrap());

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;
    join_with_timeout(cc).await;

    let mut all: Vec<i64> = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data().into_iter())
        .chain(out_c.collect_data().into_iter())
        .flat_map(|(_, d)| d)
        .collect();
    all.sort();
    assert_eq!(all, (0..30).collect::<Vec<i64>>());
}

/// Two-node cluster with multiple workers per node and exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_multi_worker_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 2),
        NodeConfig::new("node-b", 2),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        4,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        4,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    // Node-a has workers 0, 1 — node-b has workers 2, 3.
    let out_a0 = ca.take_output::<i64>(0, "results").unwrap();
    let out_a1 = ca.take_output::<i64>(1, "results").unwrap();
    let out_b0 = cb.take_output::<i64>(0, "results").unwrap();
    let out_b1 = cb.take_output::<i64>(1, "results").unwrap();

    // Send data to worker 0 (node-a).
    let sa0 = ca.take_input::<i64>(0, "data").unwrap();
    sa0.send(0, (0..40).collect()).unwrap();
    drop(sa0);

    // Close other inputs.
    drop(ca.take_input::<i64>(1, "data").unwrap());
    drop(cb.take_input::<i64>(0, "data").unwrap());
    drop(cb.take_input::<i64>(1, "data").unwrap());

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    let mut all: Vec<i64> = [out_a0, out_a1, out_b0, out_b1]
        .into_iter()
        .flat_map(|o| o.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    all.sort();
    assert_eq!(all, (0..40).collect::<Vec<i64>>());
}

/// Two-node cluster with multiple epochs of data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_multi_epoch_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    // Send data in 5 epochs from both nodes.
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    for epoch in 0u64..5 {
        let base_a = (epoch * 10) as i64;
        let base_b = (epoch * 10 + 100) as i64;
        sa.send(epoch, (base_a..base_a + 10).collect()).unwrap();
        sb.send(epoch, (base_b..base_b + 10).collect()).unwrap();
    }
    drop(sa);
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    // Verify all data arrived (100 values total: 50 from each node).
    let mut all: Vec<i64> = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data().into_iter())
        .flat_map(|(_, d)| d)
        .collect();
    all.sort();

    let mut expected: Vec<i64> = Vec::new();
    for epoch in 0u64..5 {
        let base_a = (epoch * 10) as i64;
        let base_b = (epoch * 10 + 100) as i64;
        expected.extend(base_a..base_a + 10);
        expected.extend(base_b..base_b + 10);
    }
    expected.sort();
    assert_eq!(all, expected);
}

// ===========================================================================
// Stress tests
// ===========================================================================

/// Stress test: high volume data through TCP exchange.
///
/// Sends 100_000 records through a 2-node exchange to verify stability
/// under load with real TCP connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // stress test — run with `cargo test --ignored`
async fn stress_tcp_exchange_high_volume() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    let n = 100_000i64;

    // Send all data from node-a in batches.
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let batch_size = 1000;
    for start in (0..n).step_by(batch_size) {
        let end = (start + batch_size as i64).min(n);
        sa.send(0, (start..end).collect()).unwrap();
    }
    drop(sa);

    // Close node-b input.
    drop(cb.take_input::<i64>(0, "data").unwrap());

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    let count: usize = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data().into_iter())
        .map(|(_, d)| d.len())
        .sum();
    assert_eq!(count, n as usize, "expected all {n} records to arrive");
}

/// Stress test: repeated cluster creation and teardown.
///
/// Creates and destroys 10 independent clusters to verify no resource leaks
/// or port conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // stress test — run with `cargo test --ignored`
async fn stress_tcp_repeated_creation() {
    for iteration in 0..10 {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let dataflow_id = DataflowId::new();
        let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
        let tokio_handle = tokio::runtime::Handle::current();

        let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
            builder
                .input::<i32>("data")
                .unwrap()
                .map("double", |_t, x| x * 2)
                .output("results")
                .unwrap();
            Ok(())
        };

        let ha = spawn_node(
            topology.clone(),
            "node-a",
            dataflow_id,
            conns.remove("node-a").unwrap(),
            1,
            tokio_handle.clone(),
            build,
        );
        let hb = spawn_node(
            topology,
            "node-b",
            dataflow_id,
            conns.remove("node-b").unwrap(),
            1,
            tokio_handle,
            build,
        );

        let (ra, rb) = tokio::join!(ha, hb);
        let (_rt_a, mut ca) = ra.unwrap().unwrap();
        let (_rt_b, mut cb) = rb.unwrap().unwrap();

        let out_a = ca.take_output::<i32>(0, "results").unwrap();

        let sa = ca.take_input::<i32>(0, "data").unwrap();
        sa.send(0, vec![iteration]).unwrap();
        drop(sa);

        drop(cb.take_input::<i32>(0, "data").unwrap());

        join_with_timeout(ca).await;
        join_with_timeout(cb).await;

        let data: Vec<i32> = out_a
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        assert_eq!(data, vec![iteration * 2], "iteration {iteration}");
    }
}

/// Stress test: three-node cluster with high volume from all nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore] // stress test — run with `cargo test --ignored`
async fn stress_tcp_three_nodes_high_volume() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("alpha", 1),
        NodeConfig::new("beta", 1),
        NodeConfig::new("gamma", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["alpha", "beta", "gamma"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let h_alpha = spawn_node(
        topology.clone(),
        "alpha",
        dataflow_id,
        conns.remove("alpha").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let h_beta = spawn_node(
        topology.clone(),
        "beta",
        dataflow_id,
        conns.remove("beta").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let h_gamma = spawn_node(
        topology,
        "gamma",
        dataflow_id,
        conns.remove("gamma").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb, rc) = tokio::join!(h_alpha, h_beta, h_gamma);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();
    let (_rt_c, mut cc) = rc.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();
    let out_c = cc.take_output::<i64>(0, "results").unwrap();

    let n_per_node = 10_000i64;

    // Each node sends n_per_node records with distinct ranges.
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    let sc = cc.take_input::<i64>(0, "data").unwrap();

    for (sender, base) in [(&sa, 0i64), (&sb, n_per_node), (&sc, 2 * n_per_node)] {
        let batch_size = 500;
        for start in (0..n_per_node).step_by(batch_size) {
            let end = (start + batch_size as i64).min(n_per_node);
            sender
                .send(0, (base + start..base + end).collect())
                .unwrap();
        }
    }
    drop(sa);
    drop(sb);
    drop(sc);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;
    join_with_timeout(cc).await;

    let mut all: Vec<i64> = [out_a, out_b, out_c]
        .into_iter()
        .flat_map(|o| o.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    all.sort();
    assert_eq!(all, (0..3 * n_per_node).collect::<Vec<i64>>());
}

/// Stress test: multi-epoch with exchange across 2 nodes, many epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // stress test — run with `cargo test --ignored`
async fn stress_tcp_many_epochs() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("pass", |_t, x| x).output("results").unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    let num_epochs = 50u64;
    let records_per_epoch = 100i64;

    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    for epoch in 0..num_epochs {
        let base = (epoch as i64) * records_per_epoch;
        sa.send(epoch, (base..base + records_per_epoch).collect())
            .unwrap();
        sb.send(
            epoch,
            (base + 10_000..base + 10_000 + records_per_epoch).collect(),
        )
        .unwrap();
    }
    drop(sa);
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    let total: usize = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data().into_iter())
        .map(|(_, d)| d.len())
        .sum();
    let expected = (num_epochs as usize) * (records_per_epoch as usize) * 2;
    assert_eq!(total, expected, "expected {expected} total records");
}

// ===========================================================================
// Iterate (loop/feedback) in cluster mode
// ===========================================================================

/// Two-node cluster with iterate + exchange — data repartitioned inside a loop.
///
/// This tests that `Product<u64, u32>` timestamps (created by iterate) can be
/// serialized/deserialized across TCP connections. The loop body exchanges data
/// across nodes each iteration, proving that nested-scope timestamps work with
/// the network transport layer.
///
/// Dataflow: input → iterate(exchange → increment → filter) → output
/// Data starts at 0..10, increments each round, exits when >= 5.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_iterate_with_exchange() {
    use instancy::IterateResult;

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let threshold = 5u64;

    let build = move |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();

        let result = input.iterate::<u32>("loop", 1u32, move |stream| {
            // Exchange inside the loop — data bounces between nodes each iteration.
            let exchanged = stream.exchange_by_hash("route", |x: &u64| *x);
            let incremented = exchanged.map("incr", |_t, x| x + 1);
            let feedback = incremented
                .clone()
                .filter("keep", move |_t, &x| x < threshold);
            let output = incremented.filter("done", move |_t, &x| x >= threshold);
            IterateResult { feedback, output }
        });

        result.output("results").unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        tokio_handle,
        build,
    );

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<u64>(0, "results").unwrap();
    let out_b = cb.take_output::<u64>(0, "results").unwrap();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let collector_a = tokio::task::spawn_blocking({
        let result_tx = result_tx.clone();
        move || {
            while let Some(event) = out_a.recv() {
                if let instancy::dataflow::OutputEvent::Data { data, .. } = event {
                    for value in data {
                        result_tx
                            .send(value)
                            .expect("result receiver dropped before collectors finished");
                    }
                }
            }
        }
    });
    let collector_b = tokio::task::spawn_blocking({
        let result_tx = result_tx.clone();
        move || {
            while let Some(event) = out_b.recv() {
                if let instancy::dataflow::OutputEvent::Data { data, .. } = event {
                    for value in data {
                        result_tx
                            .send(value)
                            .expect("result receiver dropped before collectors finished");
                    }
                }
            }
        }
    });
    drop(result_tx);

    // Send 0..10 from node-a.
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();
    let expected_output_count = 10u64;
    sa.send(0u64, (0..expected_output_count).collect()).unwrap();

    let mut all = Vec::new();
    let deadline = Instant::now() + TEST_TIMEOUT;
    while all.len() < expected_output_count as usize {
        let Some(time_until_deadline) = deadline.checked_duration_since(Instant::now()) else {
            panic!(
                "test deadline exceeded before receiving all expected outputs: received {}/{}",
                all.len(),
                expected_output_count
            );
        };
        let value = match result_rx.recv_timeout(time_until_deadline) {
            Ok(value) => value,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
                "timed out waiting for iterate+exchange output: received {}/{}",
                all.len(),
                expected_output_count
            ),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!(
                "output collectors disconnected before receiving all expected outputs: received {}/{}",
                all.len(),
                expected_output_count
            ),
        };
        all.push(value);
    }

    drop(sa);
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;
    collector_a.await.unwrap();
    collector_b.await.unwrap();

    all.extend(result_rx.try_iter());
    all.sort();

    // Each value v starts at v, increments by 1 per round.
    // Exits when v + k >= threshold. Final value = threshold for v < threshold,
    // or v + 1 for v >= threshold (exits after 1 increment).
    let mut expected: Vec<u64> = (0..expected_output_count)
        .map(|v| if v < threshold { threshold } else { v + 1 })
        .collect();
    expected.sort();

    assert_eq!(
        all, expected,
        "iterate+exchange across TCP nodes produced wrong output"
    );
}
