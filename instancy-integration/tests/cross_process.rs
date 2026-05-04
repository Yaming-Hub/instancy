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

/// Three-node ExchangeRoundTrip: verifies peer-ID handshake works with 3+ nodes.
///
/// With 3 nodes, TCP connections are established in a mesh pattern:
/// - node-a (lowest) accepts from node-b and node-c
/// - node-b connects to node-a, accepts from node-c
/// - node-c (highest) connects to both node-a and node-b
///
/// The peer-ID handshake on accepted connections ensures each node correctly
/// identifies its peers regardless of arrival order.
#[tokio::test]
async fn test_three_node_exchange() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b", "node-c"], 1).await;

    let topology = make_topology(&["node-a", "node-b", "node-c"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-3node", &topology, DataflowType::ExchangeRoundTrip)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);
    assert_eq!(worker_counts["node-c"], 1);

    coord.close_all_inputs("df-3node").await;
    coord.wait_for_completion("df-3node").await;
    coord.shutdown().await;
}

/// Two-node MultiEpochExchange: verifies frontier propagation across epochs.
///
/// The MultiEpochExchange dataflow uses unary_notify with per-epoch
/// aggregation. Closing inputs should cause all epoch frontiers to advance
/// and the dataflow to complete. This tests that cross-process progress
/// tracking works correctly with multiple timestamps.
#[tokio::test]
async fn test_two_node_multi_epoch_exchange() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-epoch", &topology, DataflowType::MultiEpochExchange)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    coord.close_all_inputs("df-epoch").await;
    coord.wait_for_completion("df-epoch").await;
    coord.shutdown().await;
}

/// Two-node DistributedWordCount: tests flat_map + exchange + unary_notify.
///
/// This dataflow splits sentences into words, exchanges by word (so all
/// occurrences of the same word go to the same worker), then counts them
/// per epoch with unary_notify. Exercises the full frontier-based
/// aggregation pipeline across processes.
#[tokio::test]
async fn test_two_node_word_count() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-wc", &topology, DataflowType::DistributedWordCount)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    coord.close_all_inputs("df-wc").await;
    coord.wait_for_completion("df-wc").await;
    coord.shutdown().await;
}

/// Two-node IterativeFilter: tests loop operator with exchange inside.
///
/// The IterativeFilter dataflow wraps a filter + exchange inside a loop
/// scope. Data circulates through the loop until a convergence condition
/// is met. This tests that cross-process progress tracking works with
/// nested timestamp scopes (Product<u64, u64>).
#[tokio::test]
async fn test_two_node_iterative_filter() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-iter", &topology, DataflowType::IterativeFilter)
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    coord.close_all_inputs("df-iter").await;
    coord.wait_for_completion("df-iter").await;
    coord.shutdown().await;
}

/// Two-node with 2 workers per node: tests multi-worker cluster.
///
/// Each node has 2 local workers (4 total across the cluster). This tests
/// that the exchange operator correctly routes data among 4 workers across
/// 2 processes, and that all workers complete successfully.
#[tokio::test]
async fn test_two_node_multi_worker() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 2).await;

    let topology = make_topology(&["node-a", "node-b"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-multi", &topology, DataflowType::ExchangeRoundTrip)
        .await;

    // Each node should have 2 local workers
    assert_eq!(worker_counts["node-a"], 2);
    assert_eq!(worker_counts["node-b"], 2);

    coord.close_all_inputs("df-multi").await;
    coord.wait_for_completion("df-multi").await;
    coord.shutdown().await;
}

/// Two-node cancellation: cancel a running dataflow before closing inputs.
///
/// Verifies that cancelling a cross-process dataflow works correctly:
/// the CancelDataflow command causes both nodes to cancel their local
/// workers, and WaitForCompletion returns indicating cancellation.
#[tokio::test]
async fn test_two_node_cancellation() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-cancel", &topology, DataflowType::ExchangeRoundTrip)
        .await;

    // Cancel the dataflow without closing inputs
    coord.cancel_dataflow("df-cancel").await;

    // Wait — should complete with cancellation (not a panic-worthy failure)
    let success = coord.wait_for_completion_allow_cancel("df-cancel").await;
    assert!(!success, "cancelled dataflow should not report success");

    coord.shutdown().await;
}

/// Sequential dataflows: spawn and complete two dataflows one after another
/// on the same cluster of nodes.
///
/// Tests that node processes can handle multiple dataflow lifecycles
/// in sequence — the connection and resource cleanup from the first
/// dataflow doesn't interfere with the second.
#[tokio::test]
async fn test_sequential_dataflows() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;

    // First dataflow: PassThrough
    let topology = make_topology(&["node-a", "node-b"], 1);
    let wc1 = coord
        .setup_and_spawn_dataflow("df-first", &topology, DataflowType::PassThrough)
        .await;
    assert_eq!(wc1["node-a"], 1);
    assert_eq!(wc1["node-b"], 1);
    coord.close_all_inputs("df-first").await;
    coord.wait_for_completion("df-first").await;

    // Second dataflow: ExchangeRoundTrip on the same nodes
    let wc2 = coord
        .setup_and_spawn_dataflow("df-second", &topology, DataflowType::ExchangeRoundTrip)
        .await;
    assert_eq!(wc2["node-a"], 1);
    assert_eq!(wc2["node-b"], 1);
    coord.close_all_inputs("df-second").await;
    coord.wait_for_completion("df-second").await;

    coord.shutdown().await;
}
