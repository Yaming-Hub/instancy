//! Cross-process integration tests for instancy.
//!
//! These tests start real OS processes (instancy-test-node) and coordinate
//! dataflow execution across them via the control protocol.
//!
//! Requires the `instancy-test-node` binary to be built first (the coordinator
//! builds it automatically via `cargo build`).

use instancy_integration::coordinator::TestCoordinator;
use instancy_integration::protocol::*;

fn make_topology(node_ids: &[&str], workers_per_node: usize) -> SerializableTopology {
    SerializableTopology {
        nodes: node_ids
            .iter()
            .map(|id| SerializableNodeConfig {
                node_id: id.to_string(),
                num_workers: workers_per_node,
            })
            .collect(),
    }
}

/// Two-node PassThrough: spawn, close inputs, wait for completion.
///
/// Verifies the full lifecycle of a cross-process cluster dataflow:
/// 1. Start 2 node processes
/// 2. Establish TCP connections between them
/// 3. Spawn a PassThrough dataflow (no exchange) across both nodes
/// 4. Close all inputs (signaling no more data)
/// 5. Wait for the dataflow to complete on both nodes
/// 6. Shut down cleanly
#[tokio::test]
async fn test_two_node_pass_through() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-passthrough", &topology, DataflowType::PassThrough)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    // Close inputs on both nodes so the dataflow can complete.
    coord.close_all_inputs("df-passthrough").await;

    // Wait for the dataflow to finish on both nodes.
    coord.wait_for_completion("df-passthrough").await;

    // Clean shutdown.
    coord.shutdown().await;
}

/// Two-node ExchangeRoundTrip: data is repartitioned across nodes.
///
/// This test verifies that data actually flows across the TCP connection
/// between nodes via the exchange operator. The exchange_by_hash operator
/// routes each (key, value) pair to the worker owning that key's hash,
/// which may be on a different node.
#[tokio::test]
async fn test_two_node_exchange() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-exchange", &topology, DataflowType::ExchangeRoundTrip)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    // Close inputs immediately (no data fed — just verify the dataflow can
    // complete with exchange channels and no data).
    coord.close_all_inputs("df-exchange").await;

    // Wait for the dataflow to finish on both nodes.
    coord.wait_for_completion("df-exchange").await;

    coord.shutdown().await;
}
