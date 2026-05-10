//! Integration tests for shared transport mode via `ClusterSpawnTransport::shared()`.
//!
//! These tests exercise the full `spawn_cluster` path with shared (pooled)
//! connections, verifying that multiple dataflows can share connections and
//! that data exchanges work correctly end-to-end.

#![cfg(feature = "transport")]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::DataflowBuilder;
use instancy::DataflowId;
use instancy::Result;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::{ClusterTopology, NodeConfig};
use instancy::{RuntimeConfig, RuntimeHandle};

/// Default timeout for cluster completion in tests.
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default shared connection config for tests.
fn test_shared_config() -> SharedConnectionConfig {
    SharedConnectionConfig {
        min_connections: 1,
        max_connections: 2,
        probe_interval: Duration::from_secs(3600),
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(10),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    }
}

/// Join a cluster with a timeout to prevent tests from hanging indefinitely.
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
// Connection helpers
// ---------------------------------------------------------------------------

struct PreEstablishedFactory {
    connections: Mutex<
        VecDeque<(
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
        )>,
    >,
}

impl PreEstablishedFactory {
    fn new(
        connections: Vec<(
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
        )>,
    ) -> Self {
        Self {
            connections: Mutex::new(connections.into()),
        }
    }
}

impl ConnectionFactory for PreEstablishedFactory {
    type Reader = tokio::net::tcp::OwnedReadHalf;
    type Writer = tokio::net::tcp::OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.connections
            .lock()
            .expect("pre-established factory lock poisoned")
            .pop_front()
            .ok_or_else(|| {
                Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "no more pre-established connections",
                ))
            })
    }
}

/// Creates SharedPeerManagers for a two-node cluster using real TCP connections.
async fn make_tcp_shared_managers(
    config: SharedConnectionConfig,
    num_connections: usize,
    handle: &tokio::runtime::Handle,
) -> (
    HashMap<String, SharedPeerManager>,
    HashMap<String, SharedPeerManager>,
) {
    let mut conns_a: Vec<(
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    )> = Vec::new();
    let mut conns_b: Vec<(
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    )> = Vec::new();

    for _ in 0..num_connections {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (accepted, connected) =
            tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
        let stream_a = accepted.0;
        let stream_b = connected;
        stream_a.set_nodelay(true).unwrap();
        stream_b.set_nodelay(true).unwrap();
        let (ra, wa) = stream_a.into_split();
        let (rb, wb) = stream_b.into_split();
        conns_a.push((ra, wa));
        conns_b.push((rb, wb));
    }

    let factory_a: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(conns_a));
    let factory_b: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(conns_b));

    let manager_a =
        SharedPeerManager::new("node-b".to_string(), config.clone(), factory_a, handle).unwrap();
    let manager_b =
        SharedPeerManager::new("node-a".to_string(), config, factory_b, handle).unwrap();

    let mut managers_a = HashMap::new();
    managers_a.insert("node-b".to_string(), manager_a);

    let mut managers_b = HashMap::new();
    managers_b.insert("node-a".to_string(), manager_b);

    (managers_a, managers_b)
}

// ===========================================================================
// Tests
// ===========================================================================

/// Two-node cluster with shared transport, no exchange edges (just handshake).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_two_nodes_no_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let tokio_handle = tokio::runtime::Handle::current();

    let (managers_a, managers_b) =
        make_tcp_shared_managers(test_shared_config(), 1, &tokio_handle).await;
    let managers_a = Arc::new(managers_a);
    let managers_b = Arc::new(managers_b);

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        builder
            .input::<i32>("data")
            .unwrap()
            .map("double", |_t, x| x * 2)
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let th = tokio_handle.clone();
    let mgr_a = Arc::clone(&managers_a);
    let ha = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "shared-test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_a, 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let topo_b = topology;
    let th2 = tokio_handle;
    let mgr_b = Arc::clone(&managers_b);
    let hb = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "shared-test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_b, 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

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

/// Two-node cluster with shared transport and exchange edges.
/// Verifies data is correctly routed across nodes via shared connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_two_nodes_with_exchange() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let tokio_handle = tokio::runtime::Handle::current();

    let (managers_a, managers_b) =
        make_tcp_shared_managers(test_shared_config(), 2, &tokio_handle).await;
    let managers_a = Arc::new(managers_a);
    let managers_b = Arc::new(managers_b);

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged
            .map("identity", |_t, x| x)
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let th = tokio_handle.clone();
    let mgr_a = Arc::clone(&managers_a);
    let ha = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "shared-exchange",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_a, 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let topo_b = topology;
    let th2 = tokio_handle;
    let mgr_b = Arc::clone(&managers_b);
    let hb = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "shared-exchange",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_b, 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    // Send all data from node-a
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    sa.send(1, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
    drop(sa);

    // Node-b sends nothing
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    // Collect all results across both nodes
    let mut all_results: Vec<i64> = out_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .chain(out_b.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    all_results.sort();

    // All 10 values should appear exactly once (exchange just repartitions)
    assert_eq!(all_results, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

/// Multiple dataflows sharing the same pooled connections.
/// Verifies dataflow isolation — each dataflow gets its own data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_multiple_dataflows_same_connections() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let tokio_handle = tokio::runtime::Handle::current();

    let (managers_a, managers_b) =
        make_tcp_shared_managers(test_shared_config(), 2, &tokio_handle).await;
    let managers_a = Arc::new(managers_a);
    let managers_b = Arc::new(managers_b);

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();
        input
            .exchange("route", |x: &u64| *x)
            .map("inc", |_t, x| x + 1)
            .output("results")
            .unwrap();
        Ok(())
    };

    let df_id_1 = DataflowId::new();
    let df_id_2 = DataflowId::new();

    let topo_a = topology.clone();
    let th = tokio_handle.clone();
    let mgr_a = Arc::clone(&managers_a);
    let ha = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })?;
        let c1 = rt.spawn_cluster(
            "shared-multi-1",
            topo_a.clone(),
            "node-a",
            df_id_1,
            ClusterSpawnTransport::shared(Arc::clone(&mgr_a), 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        let c2 = rt.spawn_cluster(
            "shared-multi-2",
            topo_a,
            "node-a",
            df_id_2,
            ClusterSpawnTransport::shared(mgr_a, 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        Ok::<_, instancy::error::Error>((rt, c1, c2))
    });

    let topo_b = topology;
    let th2 = tokio_handle;
    let mgr_b = Arc::clone(&managers_b);
    let hb = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })?;
        let c1 = rt.spawn_cluster(
            "shared-multi-1",
            topo_b.clone(),
            "node-b",
            df_id_1,
            ClusterSpawnTransport::shared(Arc::clone(&mgr_b), 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        let c2 = rt.spawn_cluster(
            "shared-multi-2",
            topo_b,
            "node-b",
            df_id_2,
            ClusterSpawnTransport::shared(mgr_b, 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        Ok::<_, instancy::error::Error>((rt, c1, c2))
    });

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut c1_a, mut c2_a) = ra.unwrap().unwrap();
    let (_rt_b, mut c1_b, mut c2_b) = rb.unwrap().unwrap();

    // Dataflow 1: send values 0..5 from node-a
    let out_1a = c1_a.take_output::<u64>(0, "results").unwrap();
    let out_1b = c1_b.take_output::<u64>(0, "results").unwrap();
    let s1a = c1_a.take_input::<u64>(0, "data").unwrap();
    s1a.send(1, vec![0, 1, 2, 3, 4]).unwrap();
    drop(s1a);
    let s1b = c1_b.take_input::<u64>(0, "data").unwrap();
    drop(s1b);

    // Dataflow 2: send values 100..105 from node-a
    let out_2a = c2_a.take_output::<u64>(0, "results").unwrap();
    let out_2b = c2_b.take_output::<u64>(0, "results").unwrap();
    let s2a = c2_a.take_input::<u64>(0, "data").unwrap();
    s2a.send(1, vec![100, 101, 102, 103, 104]).unwrap();
    drop(s2a);
    let s2b = c2_b.take_input::<u64>(0, "data").unwrap();
    drop(s2b);

    join_with_timeout(c1_a).await;
    join_with_timeout(c1_b).await;
    join_with_timeout(c2_a).await;
    join_with_timeout(c2_b).await;

    // Dataflow 1 results: all values incremented by 1
    let mut results_1: Vec<u64> = out_1a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .chain(out_1b.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    results_1.sort();
    assert_eq!(results_1, vec![1, 2, 3, 4, 5]);

    // Dataflow 2 results: all values incremented by 1
    let mut results_2: Vec<u64> = out_2a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .chain(out_2b.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    results_2.sort();
    assert_eq!(results_2, vec![101, 102, 103, 104, 105]);
}

/// Regression test: dropping the original Arc after `spawn_cluster` must NOT
/// kill background tasks. The `ClusterSpawnedDataflow` holds its own Arc clone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_dropping_arc_after_spawn_does_not_kill_transport() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let tokio_handle = tokio::runtime::Handle::current();

    let (managers_a, managers_b) =
        make_tcp_shared_managers(test_shared_config(), 2, &tokio_handle).await;
    let managers_a = Arc::new(managers_a);
    let managers_b = Arc::new(managers_b);

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged
            .map("identity", |_t, x| x)
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let th = tokio_handle.clone();
    let mgr_a = Arc::clone(&managers_a);
    let ha = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "drop-arc-test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_a, 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let topo_b = topology;
    let th2 = tokio_handle;
    let mgr_b = Arc::clone(&managers_b);
    let hb = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "drop-arc-test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_b, 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    // Drop our original Arc handles — the dataflows must still work because
    // ClusterSpawnedDataflow holds its own clone of the Arc.
    drop(managers_a);
    drop(managers_b);

    let out_a = ca.take_output::<i64>(0, "results").unwrap();
    let out_b = cb.take_output::<i64>(0, "results").unwrap();

    // Send data from node-a
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    sa.send(1, vec![1, 2, 3, 4]).unwrap();
    drop(sa);

    // Node-b sends nothing
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    drop(sb);

    join_with_timeout(ca).await;
    join_with_timeout(cb).await;

    // All data should arrive correctly despite the original Arc being dropped.
    let mut all_results: Vec<i64> = out_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .chain(out_b.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    all_results.sort();
    assert_eq!(all_results, vec![1, 2, 3, 4]);
}

/// Test that join() keeps bridges alive until await completes (async path).
///
/// Previously join() triggered Drop immediately, killing bridges. This test
/// verifies the fix by using the async join() path with data exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_join_async_keeps_bridges_alive() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let tokio_handle = tokio::runtime::Handle::current();

    let (managers_a, managers_b) =
        make_tcp_shared_managers(test_shared_config(), 2, &tokio_handle).await;
    let managers_a = Arc::new(managers_a);
    let managers_b = Arc::new(managers_b);

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &u64| *x);
        exchanged
            .map("inc", |_t, x| x + 1)
            .output("results")
            .unwrap();
        Ok(())
    };

    let topo_a = topology.clone();
    let th = tokio_handle.clone();
    let mgr_a = Arc::clone(&managers_a);
    let ha = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "async-join-test",
            topo_a,
            "node-a",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_a, 1024),
            Duration::from_secs(10),
            build,
            &th,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let topo_b = topology;
    let th2 = tokio_handle;
    let mgr_b = Arc::clone(&managers_b);
    let hb = tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "async-join-test",
            topo_b,
            "node-b",
            dataflow_id,
            ClusterSpawnTransport::shared(mgr_b, 1024),
            Duration::from_secs(10),
            build,
            &th2,
        )?;
        Ok::<_, instancy::error::Error>((rt, cluster))
    });

    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();

    let out_a = ca.take_output::<u64>(0, "results").unwrap();
    let out_b = cb.take_output::<u64>(0, "results").unwrap();

    // Send data from node-a
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    sa.send(1, vec![0, 1, 2, 3, 4]).unwrap();
    drop(sa);

    // Node-b sends nothing
    let sb = cb.take_input::<u64>(0, "data").unwrap();
    drop(sb);

    // Use the async join() path — this was previously broken.
    let completion_a = ca.join().unwrap();
    let completion_b = cb.join().unwrap();

    let (res_a, res_b) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(completion_a, completion_b)
    })
    .await
    .expect("cluster did not complete within timeout");
    res_a.unwrap();
    res_b.unwrap();

    // Verify all data exchanged correctly
    let mut all_results: Vec<u64> = out_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .chain(out_b.collect_data().into_iter().flat_map(|(_, d)| d))
        .collect();
    all_results.sort();
    assert_eq!(all_results, vec![1, 2, 3, 4, 5]);
}

// ===========================================================================
// Reconnect integration tests
// ===========================================================================

/// Simulates a transient network interruption at the SharedPeerManager level
/// using in-memory duplex streams (no TCP). Kills the initial connection by
/// aborting the echo task, then verifies the manager reconnects via the factory
/// and data flows on the new connection.
///
/// Uses a polling loop instead of a fixed sleep to detect reconnect completion,
/// making this test both fast and deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_reconnect_after_transient_network_failure() {
    use instancy::communication::transport::Frame;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};

    fn make_echo_connection() -> (
        ReadHalf<DuplexStream>,
        WriteHalf<DuplexStream>,
        tokio::task::JoinHandle<()>,
    ) {
        let (manager_stream, remote_stream) = tokio::io::duplex(65536);
        let (manager_read, manager_write) = tokio::io::split(manager_stream);
        let (mut remote_read, mut remote_write) = tokio::io::split(remote_stream);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match remote_read.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if remote_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        (manager_read, manager_write, handle)
    }

    /// Factory that produces duplex echo connections. The first connection's
    /// echo task handle is captured so the test can abort it to simulate failure.
    struct ReconnectEchoFactory {
        first_echo_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    }

    impl ReconnectEchoFactory {
        fn new() -> Self {
            Self {
                first_echo_task: std::sync::Mutex::new(None),
            }
        }

        fn abort_first_connection(&self) {
            if let Some(handle) = self.first_echo_task.lock().unwrap().take() {
                handle.abort();
            }
        }
    }

    impl ConnectionFactory for ReconnectEchoFactory {
        type Reader = ReadHalf<DuplexStream>;
        type Writer = WriteHalf<DuplexStream>;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> std::result::Result<
            (Self::Reader, Self::Writer),
            Box<dyn std::error::Error + Send + Sync>,
        > {
            let (reader, writer, echo_task) = make_echo_connection();
            let mut guard = self.first_echo_task.lock().unwrap();
            if guard.is_none() {
                *guard = Some(echo_task);
            }
            // Subsequent echo tasks run independently (dropped handles are fine)
            Ok((reader, writer))
        }
    }

    let config = SharedConnectionConfig {
        min_connections: 1,
        max_connections: 2,
        probe_interval: Duration::from_secs(3600),
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(10),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    };

    let factory = Arc::new(ReconnectEchoFactory::new());
    let dyn_factory: Arc<dyn DynConnectionFactory> = factory.clone();

    let rt = tokio::runtime::Handle::current();
    let manager =
        SharedPeerManager::new("echo-peer".to_string(), config, dyn_factory, &rt).unwrap();

    // ---------- register a dataflow and verify initial data flow ----------
    let df_id = DataflowId::new();
    let (mut receivers, _error_rx) = manager.register_dataflow(df_id, &[1], 64).await;
    let data_rx = receivers.get_mut(&1).unwrap();

    // Send a frame — the echo server reflects it back
    let frame1 = Frame {
        dataflow_id: df_id,
        channel_id: 1,
        payload: b"before-interrupt".to_vec(),
    };
    manager
        .payload_sender()
        .send((df_id, frame1))
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), data_rx.recv())
        .await
        .expect("timed out waiting for initial echo")
        .expect("data channel closed");
    assert_eq!(payload, b"before-interrupt".to_vec());

    // ---------- simulate transient network failure ----------
    // Abort the echo task — this closes the remote side of the duplex,
    // causing the manager's reader_task to detect EOF and report failure.
    factory.abort_first_connection();

    // ---------- wait for reconnect to complete ----------
    // Poll until live_connection_count recovers to >= 1 (much faster than
    // a fixed 2s sleep). The failure → monitor → scaling handler → factory
    // reconnect pipeline typically completes in <100ms with in-memory streams.
    let reconnect_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if manager.live_connection_count() >= 1 {
            break;
        }
        if tokio::time::Instant::now() >= reconnect_deadline {
            panic!("reconnect did not complete within 5s");
        }
    }

    // ---------- verify data flows on the reconnected connection ----------
    let frame2 = Frame {
        dataflow_id: df_id,
        channel_id: 1,
        payload: b"after-reconnect".to_vec(),
    };
    manager
        .payload_sender()
        .send((df_id, frame2))
        .await
        .unwrap();

    let payload = tokio::time::timeout(Duration::from_secs(5), data_rx.recv())
        .await
        .expect("timed out waiting for post-reconnect echo")
        .expect("data channel closed after reconnect");
    assert_eq!(payload, b"after-reconnect".to_vec());

    // ---------- cleanup ----------
    drop(manager);
}
