//! # Shared TCP Transport — Parallel Cluster Dataflows
//!
//! Validates that multiple TCP-backed cluster dataflows can run in parallel
//! on the same RuntimeHandle (shared worker thread pool) without interference.
//!
//! Design:
//! - 2 fake nodes ("node-a", "node-b") in a single process
//! - Each node has its own RuntimeHandle with `worker_threads = 1`
//! - N=3 parallel dataflows, each with its own TCP connection pair
//! - Each dataflow uses `exchange` to repartition data across the 2 nodes
//! - A single worker thread per node proves cooperative async scheduling
//!   works correctly even when multiple TCP dataflows compete for it
//!
//! This ensures:
//! - TransportSession isolation: per-dataflow TCP sessions don't interfere
//! - No cross-dataflow data corruption or progress interference
//! - No deadlocks from multiple dataflows sharing one worker thread
//! - Correct exchange routing across nodes for every dataflow

#![cfg(feature = "transport")]
#![allow(clippy::needless_range_loop)]

use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::DataflowBuilder;
use instancy::DataflowId;
use instancy::Result;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{ClusterTopology, NodeConfig};
use instancy::{RuntimeConfig, RuntimeHandle};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Create one TCP connection pair between node-a and node-b.
async fn make_one_tcp_pair() -> (
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>,
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>,
) {
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

/// Three parallel dataflows with exchange across 2 TCP-connected nodes.
///
/// Each node uses `worker_threads = 1`, so all 3 dataflows on a node share
/// a single OS thread via cooperative async scheduling. This is the strongest
/// validation that the transport layer doesn't deadlock or corrupt data when
/// multiple dataflows compete for the same worker pool.
///
/// Dataflow shapes:
/// - df0: input → exchange(by_val) → map(×2) → output   (data: 0..20)
/// - df1: input → exchange(by_val) → map(×3) → output   (data: 100..120)
/// - df2: input → exchange(by_val) → map(+1000) → output (data: 200..220)
///
/// Data is fed from node-a only; node-b receives via exchange. Combined
/// outputs from both nodes must contain all transformed records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_tcp_dataflows_shared_pool() {
    let num_dataflows = 3;

    // Create N TCP connection pairs (one per dataflow).
    let mut conns_a = Vec::new();
    let mut conns_b = Vec::new();
    for _ in 0..num_dataflows {
        let (ca, cb) = make_one_tcp_pair().await;
        conns_a.push(ca);
        conns_b.push(cb);
    }

    let tokio_handle = tokio::runtime::Handle::current();
    let tokio_handle2 = tokio_handle.clone();

    // Each dataflow gets a unique DataflowId and shared topology.
    let dataflow_ids: Vec<DataflowId> = (0..num_dataflows).map(|_| DataflowId::new()).collect();
    let dataflow_ids2 = dataflow_ids.clone();

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let topology2 = topology.clone();

    // Build closures per dataflow index.
    fn build_df(
        df_idx: usize,
    ) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static {
        move |builder: &mut DataflowBuilder<u64>| -> Result<()> {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
            let mapped = match df_idx {
                0 => exchanged.map("transform", |_t, x| x * 2),
                1 => exchanged.map("transform", |_t, x| x * 3),
                2 => exchanged.map("transform", |_t, x| x + 1000),
                _ => unreachable!(),
            };
            mapped.output("results").unwrap();
            Ok(())
        }
    }

    // --- Node-A task: spawn 3 dataflows sequentially ---
    // Each spawn_blocking returns (RuntimeHandle, Vec<clusters>).
    // RuntimeHandle must stay alive to prevent worker cancellation.
    let handle_a = tokio::task::spawn_blocking(move || -> Result<(RuntimeHandle, Vec<_>)> {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;

        let mut clusters = Vec::new();
        for i in 0..num_dataflows {
            let cluster = rt.spawn_cluster(
                &format!("df-{i}"),
                topology.clone(),
                "node-a",
                dataflow_ids[i],
                ClusterSpawnTransport::dedicated(vec![conns_a.remove(0)], 1024),
                Duration::from_secs(30),
                build_df(i),
                &tokio_handle,
            )?;
            clusters.push(cluster);
        }
        Ok((rt, clusters))
    });

    // --- Node-B task: spawn 3 dataflows sequentially (same order) ---
    let handle_b = tokio::task::spawn_blocking(move || -> Result<(RuntimeHandle, Vec<_>)> {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;

        let mut clusters = Vec::new();
        for i in 0..num_dataflows {
            let cluster = rt.spawn_cluster(
                &format!("df-{i}"),
                topology2.clone(),
                "node-b",
                dataflow_ids2[i],
                ClusterSpawnTransport::dedicated(vec![conns_b.remove(0)], 1024),
                Duration::from_secs(30),
                build_df(i),
                &tokio_handle2,
            )?;
            clusters.push(cluster);
        }
        Ok((rt, clusters))
    });

    // Wait for both nodes to finish spawning all dataflows.
    let (ra, rb) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut clusters_a) = ra.unwrap().unwrap();
    let (_rt_b, mut clusters_b) = rb.unwrap().unwrap();

    // --- Take outputs before feeding data ---
    let mut outputs_a: Vec<_> = clusters_a
        .iter_mut()
        .map(|c| c.take_output::<i64>(0, "results").unwrap())
        .collect();
    let mut outputs_b: Vec<_> = clusters_b
        .iter_mut()
        .map(|c| c.take_output::<i64>(0, "results").unwrap())
        .collect();

    // --- Feed data to node-a; close node-b inputs ---
    let input_ranges: Vec<Vec<i64>> = vec![
        (0..20).collect(),
        (100..120).collect(),
        (200..220).collect(),
    ];

    for (i, cluster) in clusters_a.iter_mut().enumerate() {
        let sender = cluster.take_input::<i64>(0, "data").unwrap();
        sender.send(0u64, input_ranges[i].clone()).unwrap();
        drop(sender);
    }
    for cluster in clusters_b.iter_mut() {
        drop(cluster.take_input::<i64>(0, "data").unwrap());
    }

    // --- Join all clusters with timeout ---
    let join_all = async {
        let mut join_handles = Vec::new();
        for c in clusters_a.into_iter().chain(clusters_b.into_iter()) {
            join_handles.push(tokio::task::spawn_blocking(move || c.join_blocking()));
        }
        for h in join_handles {
            h.await.unwrap().unwrap();
        }
    };

    tokio::time::timeout(TEST_TIMEOUT, join_all)
        .await
        .expect("parallel TCP dataflows did not complete within timeout");

    // --- Verify outputs ---
    // Expected transforms per dataflow.
    let expected: Vec<Vec<i64>> = vec![
        (0..20).map(|x| x * 2).collect(),
        (100..120).map(|x| x * 3).collect(),
        (200..220).map(|x| x + 1000).collect(),
    ];

    for i in 0..num_dataflows {
        let out_a = outputs_a.remove(0);
        let out_b = outputs_b.remove(0);

        let data_a: Vec<i64> = out_a
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        let data_b: Vec<i64> = out_b
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();

        // Prove that node-b actually received exchanged records over TCP.
        // If exchange were broken (routing everything locally), data_b would be empty.
        assert!(
            !data_b.is_empty(),
            "dataflow {i}: node-b received no records — exchange may not be routing across TCP"
        );

        let mut combined: Vec<i64> = data_a.into_iter().chain(data_b).collect();
        combined.sort();

        let mut exp = expected[i].clone();
        exp.sort();

        assert_eq!(
            combined, exp,
            "dataflow {i}: output mismatch.\n  got:      {combined:?}\n  expected: {exp:?}"
        );
    }
}

/// Multi-epoch variant: each dataflow receives data across multiple epochs,
/// ensuring progress tracking works correctly across parallel TCP dataflows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_tcp_dataflows_multi_epoch() {
    let num_dataflows = 2;
    let num_epochs = 3u64;

    let mut conns_a = Vec::new();
    let mut conns_b = Vec::new();
    for _ in 0..num_dataflows {
        let (ca, cb) = make_one_tcp_pair().await;
        conns_a.push(ca);
        conns_b.push(cb);
    }

    let tokio_handle = tokio::runtime::Handle::current();
    let tokio_handle2 = tokio_handle.clone();

    let dataflow_ids: Vec<DataflowId> = (0..num_dataflows).map(|_| DataflowId::new()).collect();
    let dataflow_ids2 = dataflow_ids.clone();

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let topology2 = topology.clone();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<i64>("data").unwrap();
        let exchanged = input.exchange("by_val", |x: &i64| *x as u64);
        exchanged.map("double", |_t, x| x * 2).output("results").unwrap();
        Ok(())
    };

    let handle_a = tokio::task::spawn_blocking(move || -> Result<(RuntimeHandle, Vec<_>)> {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let mut clusters = Vec::new();
        for i in 0..num_dataflows {
            let cluster = rt.spawn_cluster(
                &format!("df-{i}"),
                topology.clone(),
                "node-a",
                dataflow_ids[i],
                ClusterSpawnTransport::dedicated(vec![conns_a.remove(0)], 1024),
                Duration::from_secs(30),
                build,
                &tokio_handle,
            )?;
            clusters.push(cluster);
        }
        Ok((rt, clusters))
    });

    let handle_b = tokio::task::spawn_blocking(move || -> Result<(RuntimeHandle, Vec<_>)> {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let mut clusters = Vec::new();
        for i in 0..num_dataflows {
            let cluster = rt.spawn_cluster(
                &format!("df-{i}"),
                topology2.clone(),
                "node-b",
                dataflow_ids2[i],
                ClusterSpawnTransport::dedicated(vec![conns_b.remove(0)], 1024),
                Duration::from_secs(30),
                build,
                &tokio_handle2,
            )?;
            clusters.push(cluster);
        }
        Ok((rt, clusters))
    });

    let (ra, rb) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut clusters_a) = ra.unwrap().unwrap();
    let (_rt_b, mut clusters_b) = rb.unwrap().unwrap();

    let mut outputs_a: Vec<_> = clusters_a
        .iter_mut()
        .map(|c| c.take_output::<i64>(0, "results").unwrap())
        .collect();
    let mut outputs_b: Vec<_> = clusters_b
        .iter_mut()
        .map(|c| c.take_output::<i64>(0, "results").unwrap())
        .collect();

    // Feed data epoch by epoch. Dataflow i gets base offset i*100.
    for (i, cluster) in clusters_a.iter_mut().enumerate() {
        let sender = cluster.take_input::<i64>(0, "data").unwrap();
        let base = (i as i64) * 100;
        for epoch in 0..num_epochs {
            let data: Vec<i64> = (0..10).map(|x| base + x).collect();
            sender.send(epoch, data).unwrap();
        }
        drop(sender);
    }
    for cluster in clusters_b.iter_mut() {
        drop(cluster.take_input::<i64>(0, "data").unwrap());
    }

    // Join all.
    let join_all = async {
        let mut handles = Vec::new();
        for c in clusters_a.into_iter().chain(clusters_b.into_iter()) {
            handles.push(tokio::task::spawn_blocking(move || c.join_blocking()));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
    };
    tokio::time::timeout(TEST_TIMEOUT, join_all)
        .await
        .expect("multi-epoch parallel TCP dataflows did not complete within timeout");

    // Verify: each dataflow should produce num_epochs × 10 records, all doubled.
    for i in 0..num_dataflows {
        let out_a = outputs_a.remove(0);
        let out_b = outputs_b.remove(0);

        let data_a: Vec<i64> = out_a
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        let data_b: Vec<i64> = out_b
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();

        // Prove that node-b actually received exchanged records over TCP.
        assert!(
            !data_b.is_empty(),
            "dataflow {i}: node-b received no records — exchange may not be routing across TCP"
        );

        let mut combined: Vec<i64> = data_a.into_iter().chain(data_b).collect();
        combined.sort();

        let base = (i as i64) * 100;
        let mut expected: Vec<i64> = (0..num_epochs)
            .flat_map(|_| (0..10).map(|x| (base + x) * 2))
            .collect();
        expected.sort();

        assert_eq!(
            combined.len(),
            expected.len(),
            "dataflow {i}: record count mismatch ({} vs {})",
            combined.len(),
            expected.len()
        );
        assert_eq!(combined, expected, "dataflow {i}: output data mismatch");
    }
}
