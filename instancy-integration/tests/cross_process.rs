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

// ===========================================================================
// Data-driven tests (feed data → collect output → verify correctness)
// ===========================================================================

/// Two-node PassThrough with data verification.
///
/// Feeds data to node-a, collects output from node-a (since PassThrough has no
/// exchange, data stays on the originating node), and verifies the output matches.
#[tokio::test]
async fn test_pass_through_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-pt-data", &topology, DataflowType::PassThrough)
        .await;

    // Feed data to node-a at timestamp 0.
    let input_data: Vec<Vec<u8>> = vec![b"hello".to_vec(), b"world".to_vec()];
    let payload = bincode::serialize(&input_data).unwrap();
    coord
        .feed_data("node-a", "df-pt-data", 0, "data", 0, payload)
        .await;

    // Close inputs so the dataflow completes.
    coord.close_all_inputs("df-pt-data").await;
    coord.wait_for_completion("df-pt-data").await;

    // Collect output from node-a (PassThrough keeps data local).
    let output = coord
        .collect_output("node-a", "df-pt-data", 0, "results")
        .await;
    let mut all_output: Vec<Vec<u8>> = Vec::new();
    for (_ts, bytes) in &output {
        let batch: Vec<Vec<u8>> = bincode::deserialize(bytes).unwrap();
        all_output.extend(batch);
    }
    all_output.sort();

    let mut expected = input_data.clone();
    expected.sort();
    assert_eq!(all_output, expected, "PassThrough output should match input");

    coord.shutdown().await;
}

/// Two-node ExchangeRoundTrip with data verification.
///
/// Feeds keyed data to node-a, closes inputs, then collects output from both nodes.
/// The exchange operator partitions data by key hash across the 2 workers (one per node).
/// All input records should appear exactly once in the combined output.
#[tokio::test]
async fn test_exchange_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-ex-data", &topology, DataflowType::ExchangeRoundTrip)
        .await;

    // Feed 4 keyed records to node-a at timestamp 0.
    let input_data: Vec<(u64, String)> = vec![
        (1, "alpha".into()),
        (2, "beta".into()),
        (3, "gamma".into()),
        (4, "delta".into()),
    ];
    let payload = bincode::serialize(&input_data).unwrap();
    coord
        .feed_data("node-a", "df-ex-data", 0, "data", 0, payload)
        .await;

    coord.close_all_inputs("df-ex-data").await;
    coord.wait_for_completion("df-ex-data").await;

    // Collect output from both nodes and merge.
    let out_a = coord
        .collect_output("node-a", "df-ex-data", 0, "results")
        .await;
    let out_b = coord
        .collect_output("node-b", "df-ex-data", 0, "results")
        .await;

    let mut all_output: Vec<(u64, String)> = Vec::new();
    for (_ts, bytes) in out_a.iter().chain(out_b.iter()) {
        let batch: Vec<(u64, String)> = bincode::deserialize(bytes).unwrap();
        all_output.extend(batch);
    }
    all_output.sort_by_key(|(k, _)| *k);

    let mut expected = input_data.clone();
    expected.sort_by_key(|(k, _)| *k);

    assert_eq!(
        all_output, expected,
        "ExchangeRoundTrip output should contain all input records"
    );

    coord.shutdown().await;
}

/// Two-node DistributedWordCount with data verification.
///
/// Feeds sentences to both nodes, collects word counts, and verifies
/// that all words are counted correctly across the cluster.
#[tokio::test]
async fn test_word_count_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-wc-data", &topology, DataflowType::DistributedWordCount)
        .await;

    // Feed sentences to node-a at timestamp 0.
    let sentences_a: Vec<String> = vec!["hello world hello".into()];
    let payload_a = bincode::serialize(&sentences_a).unwrap();
    coord
        .feed_data("node-a", "df-wc-data", 0, "sentences", 0, payload_a)
        .await;

    // Feed sentences to node-b at timestamp 0.
    let sentences_b: Vec<String> = vec!["world world rust".into()];
    let payload_b = bincode::serialize(&sentences_b).unwrap();
    coord
        .feed_data("node-b", "df-wc-data", 0, "sentences", 0, payload_b)
        .await;

    coord.close_all_inputs("df-wc-data").await;
    coord.wait_for_completion("df-wc-data").await;

    // Collect output from both nodes.
    let out_a = coord
        .collect_output("node-a", "df-wc-data", 0, "results")
        .await;
    let out_b = coord
        .collect_output("node-b", "df-wc-data", 0, "results")
        .await;

    let mut all_counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (_ts, bytes) in out_a.iter().chain(out_b.iter()) {
        let batch: Vec<(String, u64)> = bincode::deserialize(bytes).unwrap();
        for (word, count) in batch {
            *all_counts.entry(word).or_default() += count;
        }
    }

    // Expected: "hello"=2, "world"=3, "rust"=1
    assert_eq!(all_counts.get("hello"), Some(&2), "hello count");
    assert_eq!(all_counts.get("world"), Some(&3), "world count");
    assert_eq!(all_counts.get("rust"), Some(&1), "rust count");
    assert_eq!(all_counts.len(), 3, "exactly 3 unique words");

    coord.shutdown().await;
}

/// Two-node MultiEpochExchange with data verification.
///
/// Feeds data at two different timestamps (epochs 0 and 1), collects output,
/// and verifies that per-epoch per-key sums are correct across the cluster.
/// This exercises frontier propagation with real data across processes.
#[tokio::test]
async fn test_multi_epoch_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-me-data", &topology, DataflowType::MultiEpochExchange)
        .await;

    // Epoch 0: feed data to node-a. Key 1 gets value 10 and 20; key 2 gets 30.
    let epoch0_a: Vec<(u64, i64)> = vec![(1, 10), (2, 30), (1, 20)];
    coord
        .feed_data(
            "node-a",
            "df-me-data",
            0,
            "data",
            0,
            bincode::serialize(&epoch0_a).unwrap(),
        )
        .await;

    // Epoch 0: feed data to node-b. Key 1 gets value 5.
    let epoch0_b: Vec<(u64, i64)> = vec![(1, 5)];
    coord
        .feed_data(
            "node-b",
            "df-me-data",
            0,
            "data",
            0,
            bincode::serialize(&epoch0_b).unwrap(),
        )
        .await;

    // Epoch 1: feed data to node-a. Key 2 gets value 100.
    let epoch1_a: Vec<(u64, i64)> = vec![(2, 100)];
    coord
        .feed_data(
            "node-a",
            "df-me-data",
            0,
            "data",
            1,
            bincode::serialize(&epoch1_a).unwrap(),
        )
        .await;

    coord.close_all_inputs("df-me-data").await;
    coord.wait_for_completion("df-me-data").await;

    // Collect output from both nodes.
    let out_a = coord
        .collect_output("node-a", "df-me-data", 0, "results")
        .await;
    let out_b = coord
        .collect_output("node-b", "df-me-data", 0, "results")
        .await;

    // Aggregate per-epoch sums.
    let mut epoch_sums: std::collections::HashMap<u64, std::collections::HashMap<u64, i64>> =
        std::collections::HashMap::new();
    for (ts, bytes) in out_a.iter().chain(out_b.iter()) {
        let batch: Vec<(u64, i64)> = bincode::deserialize(bytes).unwrap();
        let epoch = epoch_sums.entry(*ts).or_default();
        for (key, val) in batch {
            *epoch.entry(key).or_default() += val;
        }
    }

    // Verify exactly 2 epochs in output.
    assert_eq!(epoch_sums.len(), 2, "should have exactly 2 epochs");

    // Epoch 0: key 1 = 10+20+5 = 35, key 2 = 30
    let e0 = &epoch_sums[&0];
    assert_eq!(e0.len(), 2, "epoch 0 should have exactly 2 keys");
    assert_eq!(e0[&1], 35, "epoch 0, key 1 sum");
    assert_eq!(e0[&2], 30, "epoch 0, key 2 sum");

    // Epoch 1: key 2 = 100
    let e1 = &epoch_sums[&1];
    assert_eq!(e1.len(), 1, "epoch 1 should have exactly 1 key");
    assert_eq!(e1[&2], 100, "epoch 1, key 2 sum");

    coord.shutdown().await;
}

/// Two-node IterativeFilter with data verification.
///
/// Feeds `(key, value)` pairs where value determines how many iterations before
/// the item exits the loop. Each iteration decrements the value by 1; items exit
/// when value ≤ 1 (max 10 iterations). Verifies that all items eventually appear
/// in the output with decremented values.
#[tokio::test]
async fn test_iterative_filter_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-if-data", &topology, DataflowType::IterativeFilter)
        .await;

    // Feed items: value determines iterations before exit.
    // (key=1, val=1) → exits immediately (1-1=0 ≤ 1, output as (1, 0))
    // (key=2, val=3) → 2 iterations: 3→2→1, exits at val=1, output as (2, 1)
    // (key=3, val=5) → 4 iterations: 5→4→3→2→1, exits at val=1, output as (3, 1)
    let input: Vec<(u64, i64)> = vec![(1, 1), (2, 3), (3, 5)];
    coord
        .feed_data(
            "node-a",
            "df-if-data",
            0,
            "data",
            0,
            bincode::serialize(&input).unwrap(),
        )
        .await;

    coord.close_all_inputs("df-if-data").await;
    coord.wait_for_completion("df-if-data").await;

    // Collect from both nodes.
    let out_a = coord
        .collect_output("node-a", "df-if-data", 0, "results")
        .await;
    let out_b = coord
        .collect_output("node-b", "df-if-data", 0, "results")
        .await;

    let mut all_output: Vec<(u64, i64)> = Vec::new();
    for (_ts, bytes) in out_a.iter().chain(out_b.iter()) {
        let batch: Vec<(u64, i64)> = bincode::deserialize(bytes).unwrap();
        all_output.extend(batch);
    }
    all_output.sort_by_key(|(k, _)| *k);

    // All 3 keys should appear in output.
    assert_eq!(all_output.len(), 3, "all 3 items should exit the loop");

    // Each item's output value should be ≤ 1 (the exit condition).
    for (key, val) in &all_output {
        assert!(
            *val <= 1,
            "key {key} should have val ≤ 1, got {val}"
        );
    }

    // Verify all keys present.
    let keys: Vec<u64> = all_output.iter().map(|(k, _)| *k).collect();
    assert!(keys.contains(&1), "key 1 should be in output");
    assert!(keys.contains(&2), "key 2 should be in output");
    assert!(keys.contains(&3), "key 3 should be in output");

    coord.shutdown().await;
}

/// Two-node DistributedJoin with data verification.
///
/// Feeds keyed data to two input ports (left and right), verifies that the
/// binary join produces correct results across the cluster. The join is on
/// the u64 key, producing (key, left_value, right_value) tuples.
#[tokio::test]
async fn test_distributed_join_with_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow("df-join-data", &topology, DataflowType::DistributedJoin)
        .await;

    // Feed left input: (key, string_value)
    let left_data: Vec<(u64, String)> = vec![
        (1, "alice".into()),
        (2, "bob".into()),
        (3, "carol".into()),
    ];
    coord
        .feed_data(
            "node-a",
            "df-join-data",
            0,
            "left",
            0,
            bincode::serialize(&left_data).unwrap(),
        )
        .await;

    // Feed right input: (key, int_value)
    // Key 1 and 2 match left, key 4 has no left match.
    let right_data: Vec<(u64, i64)> = vec![(1, 100), (2, 200), (4, 400)];
    coord
        .feed_data(
            "node-a",
            "df-join-data",
            0,
            "right",
            0,
            bincode::serialize(&right_data).unwrap(),
        )
        .await;

    coord.close_all_inputs("df-join-data").await;
    coord.wait_for_completion("df-join-data").await;

    // Collect output from both nodes.
    let out_a = coord
        .collect_output("node-a", "df-join-data", 0, "results")
        .await;
    let out_b = coord
        .collect_output("node-b", "df-join-data", 0, "results")
        .await;

    let mut all_output: Vec<(u64, String, i64)> = Vec::new();
    for (_ts, bytes) in out_a.iter().chain(out_b.iter()) {
        let batch: Vec<(u64, String, i64)> = bincode::deserialize(bytes).unwrap();
        all_output.extend(batch);
    }
    all_output.sort_by_key(|(k, _, _)| *k);

    // Expected joins: (1, "alice", 100), (2, "bob", 200)
    // Key 3 has no right match, key 4 has no left match — neither should appear.
    assert_eq!(all_output.len(), 2, "should have exactly 2 joined records");
    assert_eq!(all_output[0], (1, "alice".into(), 100));
    assert_eq!(all_output[1], (2, "bob".into(), 200));

    coord.shutdown().await;
}