//! # Cluster Basic Example
//!
//! Demonstrates `spawn_cluster()` with a two-node topology using in-memory
//! duplex streams to simulate the network. Each node runs the same simple
//! dataflow, processes its own input locally, and produces independent output.
//!
//! Run with: `cargo run --all-features --example cluster_basic`

use std::time::Duration;

use instancy::communication::transport_session::PeerConnection;
use instancy::communication::ClusterSpawnTransport;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
};
use tokio::io::DuplexStream;

fn make_duplex_pair(
    node_a: &str,
    node_b: &str,
    buffer_size: usize,
) -> (
    PeerConnection<DuplexStream, DuplexStream>,
    PeerConnection<DuplexStream, DuplexStream>,
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

fn build_dataflow(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i32>("data")
        .map("double", |_t, x| x * 2)
        .output("results");
    Ok(())
}

fn spawn_node(
    topology: ClusterTopology,
    node_id: &str,
    dataflow_id: DataflowId,
    connection: PeerConnection<DuplexStream, DuplexStream>,
    tokio_handle: tokio::runtime::Handle,
) -> tokio::task::JoinHandle<
    Result<(
        RuntimeHandle,
        instancy::runtime::ClusterSpawnedDataflow<u64>,
    )>,
> {
    let node_id = node_id.to_string();
    tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "cluster-basic",
            topology,
            &node_id,
            dataflow_id,
            ClusterSpawnTransport::dedicated(vec![connection], 1024),
            Duration::from_secs(5),
            build_dataflow,
            &tokio_handle,
        )?;
        Ok((rt, cluster))
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== cluster_basic ===\n");

    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])?;
    let dataflow_id = DataflowId::new();
    let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);
    let tokio_handle = tokio::runtime::Handle::current();

    let handle_a = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conn_a,
        tokio_handle.clone(),
    );
    let handle_b = spawn_node(topology, "node-b", dataflow_id, conn_b, tokio_handle);

    let (result_a, result_b) = tokio::join!(handle_a, handle_b);
    let (_rt_a, mut cluster_a) = result_a.expect("node-a spawn task panicked")?;
    let (_rt_b, mut cluster_b) = result_b.expect("node-b spawn task panicked")?;

    let input_a = vec![1, 2, 3];
    let input_b = vec![10, 20];
    println!("node-a input: {input_a:?}");
    println!("node-b input: {input_b:?}\n");

    let output_a = cluster_a.take_output::<i32>(0, "results")?;
    let output_b = cluster_b.take_output::<i32>(0, "results")?;

    let sender_a = cluster_a.take_input::<i32>(0, "data")?;
    sender_a.send(0, input_a.clone())?;
    drop(sender_a);

    let sender_b = cluster_b.take_input::<i32>(0, "data")?;
    sender_b.send(0, input_b.clone())?;
    drop(sender_b);

    let join_a = tokio::task::spawn_blocking(move || cluster_a.join_blocking());
    let join_b = tokio::task::spawn_blocking(move || cluster_b.join_blocking());
    let (joined_a, joined_b) = tokio::join!(join_a, join_b);
    joined_a.expect("node-a join task panicked")?;
    joined_b.expect("node-b join task panicked")?;

    let mut results_a: Vec<i32> = output_a
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();
    let mut results_b: Vec<i32> = output_b
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();
    results_a.sort();
    results_b.sort();

    assert_eq!(results_a, vec![2, 4, 6]);
    assert_eq!(results_b, vec![20, 40]);

    println!("Each node processed its own records locally (no exchange edges).\n");
    println!("node-a output: {results_a:?}");
    println!("node-b output: {results_b:?}");
    println!("\n✓ Cluster completed successfully.");

    Ok(())
}
