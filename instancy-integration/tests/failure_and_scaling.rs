//! Failure-handling and scaling integration tests.
//!
//! These tests exercise node crashes, cancellation recovery, concurrent dataflows,
//! and larger worker topologies using real `instancy-test-node` processes.

use std::collections::{BTreeMap, HashMap};

use instancy_integration::coordinator::TestCoordinator;
use instancy_integration::protocol::*;
use serde::de::DeserializeOwned;

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

fn sorted_node_ids(worker_counts: &HashMap<String, usize>) -> Vec<String> {
    let mut node_ids: Vec<_> = worker_counts.keys().cloned().collect();
    node_ids.sort();
    node_ids
}

async fn collect_all_records<T: DeserializeOwned>(
    coord: &mut TestCoordinator,
    dataflow_id: &str,
    worker_counts: &HashMap<String, usize>,
) -> Vec<(u64, T)> {
    let mut all_records = Vec::new();
    for node_id in sorted_node_ids(worker_counts) {
        for worker_idx in 0..worker_counts[&node_id] {
            let output = coord
                .collect_output(&node_id, dataflow_id, worker_idx, "results")
                .await;
            for (ts, bytes) in output {
                let batch: Vec<T> = bincode::deserialize(&bytes).unwrap();
                all_records.extend(batch.into_iter().map(|item| (ts, item)));
            }
        }
    }
    all_records
}

/// Simulates a node crashing after data has been fed into a multi-node exchange dataflow.
#[tokio::test]
async fn test_node_crash_mid_dataflow() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b", "node-c"], 1).await;
    let topology = make_topology(&["node-a", "node-b", "node-c"], 1);

    coord
        .setup_and_spawn_dataflow(
            "df-node-crash-mid",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    let batch_a = vec![(1u64, String::from("node-a-record"))];
    let batch_b = vec![(2u64, String::from("node-b-record"))];
    let batch_c = vec![(3u64, String::from("node-c-record"))];

    coord
        .feed_data(
            "node-a",
            "df-node-crash-mid",
            0,
            "data",
            0,
            bincode::serialize(&batch_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-node-crash-mid",
            0,
            "data",
            0,
            bincode::serialize(&batch_b).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-c",
            "df-node-crash-mid",
            0,
            "data",
            0,
            bincode::serialize(&batch_c).unwrap(),
        )
        .await;

    coord.kill_node("node-c").await;
    coord.close_all_inputs("df-node-crash-mid").await;

    // After killing a node, the remaining nodes may or may not succeed depending
    // on whether data exchange with the killed node was already complete.
    // The key property is: the coordinator does not panic or hang.
    let result = coord
        .wait_for_completion_tolerant("df-node-crash-mid")
        .await;
    assert!(
        result.is_ok(),
        "tolerant completion wait should not return Err after a node crash: {result:?}"
    );

    coord.shutdown().await;
}

/// Simulates a node crashing before any input is fed, ensuring the remaining node can still be cleaned up.
#[tokio::test]
async fn test_node_crash_before_data() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow(
            "df-node-crash-before-data",
            &topology,
            DataflowType::PassThrough,
        )
        .await;

    coord.kill_node("node-b").await;
    coord.close_all_inputs("df-node-crash-before-data").await;
    let result = coord
        .wait_for_completion_tolerant("df-node-crash-before-data")
        .await
        .expect("tolerant completion wait should return even if a node is gone");
    // PassThrough has no exchange, so node-a should complete successfully
    // regardless of node-b being killed.
    assert!(result, "surviving node should complete a no-exchange dataflow");

    coord.shutdown().await;
}

/// Verifies that cancelling one dataflow does not prevent a second dataflow from running on the same nodes.
#[tokio::test]
async fn test_cancellation_then_new_dataflow() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow(
            "df-cancel-first",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    let cancel_batch_a = vec![(1u64, String::from("alpha"))];
    let cancel_batch_b = vec![(2u64, String::from("beta"))];
    coord
        .feed_data(
            "node-a",
            "df-cancel-first",
            0,
            "data",
            0,
            bincode::serialize(&cancel_batch_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-cancel-first",
            0,
            "data",
            0,
            bincode::serialize(&cancel_batch_b).unwrap(),
        )
        .await;

    coord.cancel_dataflow("df-cancel-first").await;
    let first_success = coord
        .wait_for_completion_allow_cancel("df-cancel-first")
        .await;
    assert!(!first_success, "cancelled dataflow should not report success");

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-after-cancel",
            &topology,
            DataflowType::DistributedWordCount,
        )
        .await;

    let sentences_a = vec![String::from("apple banana apple")];
    let sentences_b = vec![String::from("banana carrot banana")];
    coord
        .feed_data(
            "node-a",
            "df-after-cancel",
            0,
            "sentences",
            0,
            bincode::serialize(&sentences_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-after-cancel",
            0,
            "sentences",
            0,
            bincode::serialize(&sentences_b).unwrap(),
        )
        .await;

    coord.close_all_inputs("df-after-cancel").await;
    coord.wait_for_completion("df-after-cancel").await;

    let mut actual: BTreeMap<String, u64> = BTreeMap::new();
    for (_, (word, count)) in collect_all_records::<(String, u64)>(
        &mut coord,
        "df-after-cancel",
        &worker_counts,
    )
    .await
    {
        *actual.entry(word).or_default() += count;
    }

    let expected = BTreeMap::from([
        (String::from("apple"), 2u64),
        (String::from("banana"), 3u64),
        (String::from("carrot"), 1u64),
    ]);
    assert_eq!(actual, expected);

    coord.shutdown().await;
}

/// Reuses the same two-node cluster across several different dataflow types.
#[tokio::test]
async fn test_sequential_different_types() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let pass_workers = coord
        .setup_and_spawn_dataflow("df-seq-pass", &topology, DataflowType::PassThrough)
        .await;
    let pass_a = vec![b"alpha".to_vec(), b"beta".to_vec()];
    let pass_b = vec![b"gamma".to_vec()];
    coord
        .feed_data(
            "node-a",
            "df-seq-pass",
            0,
            "data",
            0,
            bincode::serialize(&pass_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-seq-pass",
            0,
            "data",
            0,
            bincode::serialize(&pass_b).unwrap(),
        )
        .await;
    coord.close_all_inputs("df-seq-pass").await;
    coord.wait_for_completion("df-seq-pass").await;

    let mut pass_actual: Vec<Vec<u8>> = collect_all_records::<Vec<u8>>(
        &mut coord,
        "df-seq-pass",
        &pass_workers,
    )
    .await
    .into_iter()
    .map(|(_, item)| item)
    .collect();
    pass_actual.sort();
    let mut pass_expected = vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()];
    pass_expected.sort();
    assert_eq!(pass_actual, pass_expected);

    let exchange_workers = coord
        .setup_and_spawn_dataflow(
            "df-seq-exchange",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;
    let exchange_a = vec![(1u64, String::from("left")), (2u64, String::from("right"))];
    let exchange_b = vec![(3u64, String::from("up")), (4u64, String::from("down"))];
    coord
        .feed_data(
            "node-a",
            "df-seq-exchange",
            0,
            "data",
            0,
            bincode::serialize(&exchange_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-seq-exchange",
            0,
            "data",
            0,
            bincode::serialize(&exchange_b).unwrap(),
        )
        .await;
    coord.close_all_inputs("df-seq-exchange").await;
    coord.wait_for_completion("df-seq-exchange").await;

    let mut exchange_actual: Vec<(u64, String)> = collect_all_records(
        &mut coord,
        "df-seq-exchange",
        &exchange_workers,
    )
    .await
    .into_iter()
    .map(|(_, item)| item)
    .collect();
    exchange_actual.sort_by_key(|(key, _)| *key);
    let mut exchange_expected = exchange_a;
    exchange_expected.extend(exchange_b);
    exchange_expected.sort_by_key(|(key, _)| *key);
    assert_eq!(exchange_actual, exchange_expected);

    let multi_epoch_workers = coord
        .setup_and_spawn_dataflow(
            "df-seq-multi-epoch",
            &topology,
            DataflowType::MultiEpochExchange,
        )
        .await;
    let epoch0_a = vec![(1u64, 10i64), (2u64, 20)];
    let epoch0_b = vec![(1u64, 5i64)];
    let epoch1_a = vec![(2u64, 7i64)];
    let epoch1_b = vec![(1u64, 3i64), (2u64, 4)];
    for (node_id, epoch, batch) in [
        ("node-a", 0u64, epoch0_a),
        ("node-b", 0u64, epoch0_b),
        ("node-a", 1u64, epoch1_a),
        ("node-b", 1u64, epoch1_b),
    ] {
        coord
            .feed_data(
                node_id,
                "df-seq-multi-epoch",
                0,
                "data",
                epoch,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }
    coord.close_all_inputs("df-seq-multi-epoch").await;
    coord.wait_for_completion("df-seq-multi-epoch").await;

    let mut multi_epoch_actual: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for (ts, (key, value)) in collect_all_records::<(u64, i64)>(
        &mut coord,
        "df-seq-multi-epoch",
        &multi_epoch_workers,
    )
    .await
    {
        *multi_epoch_actual.entry(ts).or_default().entry(key).or_default() += value;
    }
    let multi_epoch_expected = BTreeMap::from([
        (0u64, BTreeMap::from([(1u64, 15i64), (2u64, 20i64)])),
        (1u64, BTreeMap::from([(1u64, 3i64), (2u64, 11i64)])),
    ]);
    assert_eq!(multi_epoch_actual, multi_epoch_expected);

    let filter_workers = coord
        .setup_and_spawn_dataflow(
            "df-seq-filter",
            &topology,
            DataflowType::FilterAggregate { threshold: 10 },
        )
        .await;
    let filter_a = vec![5i64, 11, 12];
    let filter_b = vec![8i64, 20, 3];
    coord
        .feed_data(
            "node-a",
            "df-seq-filter",
            0,
            "data",
            0,
            bincode::serialize(&filter_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-seq-filter",
            0,
            "data",
            0,
            bincode::serialize(&filter_b).unwrap(),
        )
        .await;
    coord.close_all_inputs("df-seq-filter").await;
    coord.wait_for_completion("df-seq-filter").await;

    let filter_actual: Vec<i64> = collect_all_records(&mut coord, "df-seq-filter", &filter_workers)
        .await
        .into_iter()
        .map(|(_, item)| item)
        .collect();
    assert!(!filter_actual.is_empty(), "filter aggregate should emit a result");
    assert_eq!(filter_actual.iter().sum::<i64>(), 43);

    coord.shutdown().await;
}

/// Verifies explicit staged fan-out parallelism on a three-node, multi-worker cluster.
#[tokio::test]
async fn test_staged_parallelism_3_nodes() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b", "node-c"], 2).await;
    let topology = make_topology(&["node-a", "node-b", "node-c"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-staged-3-nodes",
            &topology,
            DataflowType::StagedFanOutFanIn {
                fan_out_parallelism: 6,
            },
        )
        .await;

    let inputs = [
        ("node-a", 0usize, vec![1i64, 2]),
        ("node-a", 1usize, vec![3i64, 4]),
        ("node-b", 0usize, vec![5i64, 6]),
        ("node-b", 1usize, vec![7i64]),
        ("node-c", 0usize, vec![8i64, 9]),
        ("node-c", 1usize, vec![10i64]),
    ];

    let mut expected = Vec::new();
    for (node_id, worker_idx, batch) in inputs {
        expected.extend(batch.iter().copied().map(|x| x * 2 + 1));
        coord
            .feed_data(
                node_id,
                "df-staged-3-nodes",
                worker_idx,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-staged-3-nodes").await;
    coord.wait_for_completion("df-staged-3-nodes").await;

    let mut actual: Vec<i64> = collect_all_records(&mut coord, "df-staged-3-nodes", &worker_counts)
        .await
        .into_iter()
        .map(|(_, item)| item)
        .collect();
    actual.sort();
    expected.sort();

    assert_eq!(actual, expected);
    coord.shutdown().await;
}

/// Runs two dataflows concurrently on the same nodes and verifies their outputs stay isolated.
#[tokio::test]
async fn test_concurrent_dataflows_shared_nodes() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let workers_a = coord
        .setup_and_spawn_dataflow("df-a", &topology, DataflowType::PassThrough)
        .await;
    let workers_b = coord
        .setup_and_spawn_dataflow("df-b", &topology, DataflowType::PassThrough)
        .await;

    let df_a_node_a = vec![b"alpha".to_vec()];
    let df_a_node_b = vec![b"beta".to_vec()];
    let df_b_node_a = vec![b"gamma".to_vec()];
    let df_b_node_b = vec![b"delta".to_vec()];

    for (dataflow_id, node_id, batch) in [
        ("df-a", "node-a", df_a_node_a.clone()),
        ("df-a", "node-b", df_a_node_b.clone()),
        ("df-b", "node-a", df_b_node_a.clone()),
        ("df-b", "node-b", df_b_node_b.clone()),
    ] {
        coord
            .feed_data(
                node_id,
                dataflow_id,
                0,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-a").await;
    coord.close_all_inputs("df-b").await;
    coord.wait_for_completion("df-a").await;
    coord.wait_for_completion("df-b").await;

    let mut actual_a: Vec<Vec<u8>> = collect_all_records(&mut coord, "df-a", &workers_a)
        .await
        .into_iter()
        .map(|(_, item)| item)
        .collect();
    let mut actual_b: Vec<Vec<u8>> = collect_all_records(&mut coord, "df-b", &workers_b)
        .await
        .into_iter()
        .map(|(_, item)| item)
        .collect();
    actual_a.sort();
    actual_b.sort();

    let mut expected_a = vec![b"alpha".to_vec(), b"beta".to_vec()];
    let mut expected_b = vec![b"gamma".to_vec(), b"delta".to_vec()];
    expected_a.sort();
    expected_b.sort();

    assert_eq!(actual_a, expected_a);
    assert_eq!(actual_b, expected_b);

    coord.shutdown().await;
}

/// Verifies that skipped epochs do not produce spurious output in a multi-epoch dataflow.
#[tokio::test]
async fn test_empty_epoch_handling() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-empty-epochs",
            &topology,
            DataflowType::MultiEpochExchange,
        )
        .await;

    let inputs = [
        (0u64, "node-a", vec![(1u64, 10i64)]),
        (0u64, "node-b", vec![(1u64, 5i64)]),
        (2u64, "node-a", vec![(2u64, 20i64)]),
        (2u64, "node-b", vec![(2u64, 1i64)]),
        (4u64, "node-a", vec![(3u64, 7i64)]),
        (4u64, "node-b", vec![(3u64, 8i64)]),
    ];

    for (epoch, node_id, batch) in inputs {
        coord
            .feed_data(
                node_id,
                "df-empty-epochs",
                0,
                "data",
                epoch,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-empty-epochs").await;
    coord.wait_for_completion("df-empty-epochs").await;

    let mut actual: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for (ts, (key, value)) in collect_all_records::<(u64, i64)>(
        &mut coord,
        "df-empty-epochs",
        &worker_counts,
    )
    .await
    {
        *actual.entry(ts).or_default().entry(key).or_default() += value;
    }

    let expected = BTreeMap::from([
        (0u64, BTreeMap::from([(1u64, 15i64)])),
        (2u64, BTreeMap::from([(2u64, 21i64)])),
        (4u64, BTreeMap::from([(3u64, 15i64)])),
    ]);
    assert_eq!(actual, expected);
    assert_eq!(actual.keys().copied().collect::<Vec<_>>(), vec![0, 2, 4]);

    coord.shutdown().await;
}

/// Ensures exchange-based dataflows still work in the degenerate single-node cluster case.
#[tokio::test]
async fn test_single_node_cluster() {
    let mut coord = TestCoordinator::start(&["node-a"], 1).await;
    let topology = make_topology(&["node-a"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-single-node",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    let input_data = vec![
        (1u64, String::from("alpha")),
        (2u64, String::from("beta")),
        (3u64, String::from("gamma")),
    ];
    coord
        .feed_data(
            "node-a",
            "df-single-node",
            0,
            "data",
            0,
            bincode::serialize(&input_data).unwrap(),
        )
        .await;

    coord.close_all_inputs("df-single-node").await;
    coord.wait_for_completion("df-single-node").await;

    let mut actual: Vec<(u64, String)> = collect_all_records(
        &mut coord,
        "df-single-node",
        &worker_counts,
    )
    .await
    .into_iter()
    .map(|(_, item)| item)
    .collect();
    actual.sort_by_key(|(key, _)| *key);

    let mut expected = input_data;
    expected.sort_by_key(|(key, _)| *key);
    assert_eq!(actual, expected);

    coord.shutdown().await;
}
