//! Comprehensive cross-process integration tests for phases 2-5.
//!
//! These tests start real OS processes (instancy-test-node) and exercise
//! multi-worker, multi-epoch, cancellation, and higher-load dataflow patterns.

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

fn make_asymmetric_topology(nodes: &[(&str, usize)]) -> SerializableTopology {
    SerializableTopology {
        nodes: nodes
            .iter()
            .map(|(id, workers)| SerializableNodeConfig {
                node_id: (*id).to_string(),
                num_workers: *workers,
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

async fn run_exchange_round_trip_case(
    coord: &mut TestCoordinator,
    dataflow_id: &str,
    worker_counts: &HashMap<String, usize>,
) {
    let mut expected = Vec::new();
    let mut next_key = 1u64;

    for node_id in sorted_node_ids(worker_counts) {
        for worker_idx in 0..worker_counts[&node_id] {
            let batch = vec![
                (next_key, format!("{node_id}-worker-{worker_idx}-record-a")),
                (
                    next_key + 1,
                    format!("{node_id}-worker-{worker_idx}-record-b"),
                ),
            ];
            coord
                .feed_data(
                    &node_id,
                    dataflow_id,
                    worker_idx,
                    "data",
                    0,
                    bincode::serialize(&batch).unwrap(),
                )
                .await;
            expected.extend(batch);
            next_key += 2;
        }
    }

    coord.close_all_inputs(dataflow_id).await;
    coord.wait_for_completion(dataflow_id).await;

    let mut actual: Vec<(u64, String)> = collect_all_records(coord, dataflow_id, worker_counts)
        .await
        .into_iter()
        .map(|(_, item)| item)
        .collect();
    actual.sort_by_key(|(key, _)| *key);
    expected.sort_by_key(|(key, _)| *key);

    assert_eq!(
        actual, expected,
        "all exchanged records should be preserved"
    );
}

/// Two-node ExchangeRoundTrip with one worker per node.
#[tokio::test]
async fn test_varied_parallelism_1_worker() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-varied-parallelism-1",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 1);

    run_exchange_round_trip_case(&mut coord, "df-varied-parallelism-1", &worker_counts).await;
    coord.shutdown().await;
}

/// Two-node ExchangeRoundTrip with two workers per node.
#[tokio::test]
async fn test_varied_parallelism_2_workers() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 2).await;
    let topology = make_topology(&["node-a", "node-b"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-varied-parallelism-2",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    assert_eq!(worker_counts["node-a"], 2);
    assert_eq!(worker_counts["node-b"], 2);

    run_exchange_round_trip_case(&mut coord, "df-varied-parallelism-2", &worker_counts).await;
    coord.shutdown().await;
}

/// Two-node ExchangeRoundTrip with four workers per node.
#[tokio::test]
async fn test_varied_parallelism_4_workers() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 4).await;
    let topology = make_topology(&["node-a", "node-b"], 4);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-varied-parallelism-4",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    assert_eq!(worker_counts["node-a"], 4);
    assert_eq!(worker_counts["node-b"], 4);

    run_exchange_round_trip_case(&mut coord, "df-varied-parallelism-4", &worker_counts).await;
    coord.shutdown().await;
}

/// Three-node ExchangeRoundTrip with asymmetric worker counts.
#[tokio::test]
async fn test_asymmetric_workers() {
    let nodes = [("node-a", 1usize), ("node-b", 2usize), ("node-c", 4usize)];
    let mut coord = TestCoordinator::start_asymmetric(&nodes).await;
    let topology = make_asymmetric_topology(&nodes);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-asymmetric-workers",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    assert_eq!(worker_counts["node-a"], 1);
    assert_eq!(worker_counts["node-b"], 2);
    assert_eq!(worker_counts["node-c"], 4);

    run_exchange_round_trip_case(&mut coord, "df-asymmetric-workers", &worker_counts).await;
    coord.shutdown().await;
}

/// Two-node staged fan-out / fan-in pipeline.
///
/// The current integration dataflow operates on `i64` values and applies
/// `x * 2 + 1` before gathering results back.
#[tokio::test]
async fn test_staged_fan_out_fan_in() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 2).await;
    let topology = make_topology(&["node-a", "node-b"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-staged-fan-out-fan-in",
            &topology,
            DataflowType::StagedFanOutFanIn {
                fan_out_parallelism: 4,
            },
        )
        .await;

    let inputs = [
        ("node-a", 0usize, vec![1i64, 2, 3]),
        ("node-a", 1usize, vec![4i64, 5]),
        ("node-b", 0usize, vec![6i64, 7]),
        ("node-b", 1usize, vec![8i64, 9, 10]),
    ];
    let mut expected = Vec::new();
    for (node_id, worker_idx, batch) in inputs {
        expected.extend(batch.iter().copied().map(|x| x * 2 + 1));
        coord
            .feed_data(
                node_id,
                "df-staged-fan-out-fan-in",
                worker_idx,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-staged-fan-out-fan-in").await;
    coord.wait_for_completion("df-staged-fan-out-fan-in").await;

    let mut actual: Vec<i64> =
        collect_all_records(&mut coord, "df-staged-fan-out-fan-in", &worker_counts)
            .await
            .into_iter()
            .map(|(_, item)| item)
            .collect();
    actual.sort();
    expected.sort();

    assert_eq!(
        actual, expected,
        "fan-out/fan-in should preserve all transformed values"
    );
    coord.shutdown().await;
}

/// Two-node filter + aggregate pipeline with thresholding.
#[tokio::test]
async fn test_auto_parallelism_filter_aggregate() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 2).await;
    let topology = make_topology(&["node-a", "node-b"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-filter-aggregate",
            &topology,
            DataflowType::FilterAggregate { threshold: 50 },
        )
        .await;

    let inputs = [
        ("node-a", 0usize, vec![10i64, 55, 60]),
        ("node-a", 1usize, vec![49i64, 51]),
        ("node-b", 0usize, vec![75i64, 5]),
        ("node-b", 1usize, vec![100i64, 50, 80]),
    ];
    let expected_sum: i64 = inputs
        .iter()
        .flat_map(|(_, _, batch)| batch.iter())
        .copied()
        .filter(|x| *x > 50)
        .sum();

    for (node_id, worker_idx, batch) in inputs {
        coord
            .feed_data(
                node_id,
                "df-filter-aggregate",
                worker_idx,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-filter-aggregate").await;
    coord.wait_for_completion("df-filter-aggregate").await;

    let actual_values: Vec<i64> =
        collect_all_records(&mut coord, "df-filter-aggregate", &worker_counts)
            .await
            .into_iter()
            .map(|(_, item)| item)
            .collect();

    assert!(!actual_values.is_empty(), "aggregate should emit a result");
    assert_eq!(actual_values.iter().sum::<i64>(), expected_sum);
    coord.shutdown().await;
}

/// Two-node multi-epoch aggregation over ten epochs.
#[tokio::test]
async fn test_multi_epoch_many_epochs() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-multi-epoch-many",
            &topology,
            DataflowType::MultiEpochExchange,
        )
        .await;

    let mut expected: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for epoch in 0..10u64 {
        let batch_a = vec![(1u64, epoch as i64), (2u64, 10 + epoch as i64)];
        let batch_b = vec![(1u64, 100 + epoch as i64)];

        coord
            .feed_data(
                "node-a",
                "df-multi-epoch-many",
                0,
                "data",
                epoch,
                bincode::serialize(&batch_a).unwrap(),
            )
            .await;
        coord
            .feed_data(
                "node-b",
                "df-multi-epoch-many",
                0,
                "data",
                epoch,
                bincode::serialize(&batch_b).unwrap(),
            )
            .await;

        let epoch_expected = expected.entry(epoch).or_default();
        for (key, value) in batch_a.into_iter().chain(batch_b.into_iter()) {
            *epoch_expected.entry(key).or_default() += value;
        }
    }

    coord.close_all_inputs("df-multi-epoch-many").await;
    coord.wait_for_completion("df-multi-epoch-many").await;

    let mut actual: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for (ts, (key, value)) in
        collect_all_records::<(u64, i64)>(&mut coord, "df-multi-epoch-many", &worker_counts).await
    {
        *actual.entry(ts).or_default().entry(key).or_default() += value;
    }

    assert_eq!(actual, expected);
    coord.shutdown().await;
}

/// Two-node delayed aggregation with shifted output epochs.
#[tokio::test]
async fn test_delayed_aggregation() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-delayed-aggregation",
            &topology,
            DataflowType::DelayedAggregation { delay_offset: 5 },
        )
        .await;

    let batches = [
        (0u64, "node-a", vec![1i64, 2]),
        (0u64, "node-b", vec![3i64]),
        (1u64, "node-a", vec![4i64]),
        (1u64, "node-b", vec![5i64, 6]),
        (2u64, "node-a", vec![7i64, 8]),
    ];

    for (epoch, node_id, batch) in batches {
        coord
            .feed_data(
                node_id,
                "df-delayed-aggregation",
                0,
                "data",
                epoch,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-delayed-aggregation").await;
    coord.wait_for_completion("df-delayed-aggregation").await;

    let mut actual: BTreeMap<u64, i64> = BTreeMap::new();
    for (ts, value) in
        collect_all_records::<i64>(&mut coord, "df-delayed-aggregation", &worker_counts).await
    {
        *actual.entry(ts).or_default() += value;
    }

    let expected = BTreeMap::from([(5u64, 6i64), (6u64, 15i64), (7u64, 15i64)]);
    assert_eq!(actual, expected);
    coord.shutdown().await;
}

/// Three-node frontier propagation when only one node receives data.
#[tokio::test]
async fn test_frontier_stall_recovery() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b", "node-c"], 1).await;
    let topology = make_topology(&["node-a", "node-b", "node-c"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-frontier-stall-recovery",
            &topology,
            DataflowType::MultiEpochExchange,
        )
        .await;

    let mut expected: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for epoch in 0..4u64 {
        let batch = vec![
            (epoch + 1, (epoch as i64 + 1) * 10),
            (epoch + 10, epoch as i64),
        ];
        coord
            .feed_data(
                "node-a",
                "df-frontier-stall-recovery",
                0,
                "data",
                epoch,
                bincode::serialize(&batch).unwrap(),
            )
            .await;

        let epoch_expected = expected.entry(epoch).or_default();
        for (key, value) in batch {
            *epoch_expected.entry(key).or_default() += value;
        }
    }

    coord.close_all_inputs("df-frontier-stall-recovery").await;
    coord
        .wait_for_completion("df-frontier-stall-recovery")
        .await;

    let mut actual: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
    for (ts, (key, value)) in
        collect_all_records::<(u64, i64)>(&mut coord, "df-frontier-stall-recovery", &worker_counts)
            .await
    {
        *actual.entry(ts).or_default().entry(key).or_default() += value;
    }

    assert_eq!(
        actual, expected,
        "idle nodes should still advance frontiers and complete"
    );
    coord.shutdown().await;
}

/// ExchangeRoundTrip cancellation after data has been fed.
#[tokio::test]
async fn test_cancellation_mid_stream() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow(
            "df-cancellation-mid-stream",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    let batch_a: Vec<(u64, String)> = (0..100).map(|i| (i, format!("node-a-{i}"))).collect();
    let batch_b: Vec<(u64, String)> = (100..200).map(|i| (i, format!("node-b-{i}"))).collect();

    coord
        .feed_data(
            "node-a",
            "df-cancellation-mid-stream",
            0,
            "data",
            0,
            bincode::serialize(&batch_a).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-cancellation-mid-stream",
            0,
            "data",
            0,
            bincode::serialize(&batch_b).unwrap(),
        )
        .await;

    coord.cancel_dataflow("df-cancellation-mid-stream").await;
    let success = coord
        .wait_for_completion_allow_cancel("df-cancellation-mid-stream")
        .await;

    assert!(!success, "cancelled dataflow should report cancellation");
    coord.shutdown().await;
}

/// IterativeExchange cancellation for a non-converging threshold.
#[tokio::test]
async fn test_cancellation_iterative() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    coord
        .setup_and_spawn_dataflow(
            "df-cancellation-iterative",
            &topology,
            DataflowType::IterativeExchange {
                threshold: i64::MAX,
            },
        )
        .await;

    coord
        .feed_data(
            "node-a",
            "df-cancellation-iterative",
            0,
            "data",
            0,
            bincode::serialize(&vec![1i64, 2, 3]).unwrap(),
        )
        .await;

    coord.cancel_dataflow("df-cancellation-iterative").await;
    let success = coord
        .wait_for_completion_allow_cancel("df-cancellation-iterative")
        .await;

    assert!(
        !success,
        "cancelled iterative dataflow should not report success"
    );
    coord.shutdown().await;
}

/// Branch / merge / aggregate correctness across both nodes.
#[tokio::test]
async fn test_branch_merge_correctness() {
    // Use 1 worker per node: the exchange routes all records to a single
    // worker (hash key 0), so additional workers would be idle and can
    // cause frontier-stall timeouts in the cross-process reduce.
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow("df-branch-merge", &topology, DataflowType::BranchMerge)
        .await;

    let inputs = [
        ("node-a", 0usize, vec![1i64, 2, 3, 4]),
        ("node-b", 0usize, vec![5i64, 6, 7, 8]),
    ];
    let expected_sum: i64 = inputs
        .iter()
        .flat_map(|(_, _, batch)| batch.iter())
        .copied()
        .map(|x| if x % 2 == 0 { x * 2 } else { x * 3 })
        .sum();

    for (node_id, worker_idx, batch) in inputs {
        coord
            .feed_data(
                node_id,
                "df-branch-merge",
                worker_idx,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-branch-merge").await;
    coord.wait_for_completion("df-branch-merge").await;

    let actual_values: Vec<i64> =
        collect_all_records(&mut coord, "df-branch-merge", &worker_counts)
            .await
            .into_iter()
            .map(|(_, item)| item)
            .collect();

    assert!(
        !actual_values.is_empty(),
        "branch/merge should emit a reduced sum"
    );
    assert_eq!(actual_values.iter().sum::<i64>(), expected_sum);
    coord.shutdown().await;
}

/// IterativeExchange should only emit values that reach the threshold.
#[tokio::test]
async fn test_iterative_exchange_convergence() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-iterative-exchange-convergence",
            &topology,
            DataflowType::IterativeExchange { threshold: 100 },
        )
        .await;

    coord
        .feed_data(
            "node-a",
            "df-iterative-exchange-convergence",
            0,
            "data",
            0,
            bincode::serialize(&vec![3i64, 7]).unwrap(),
        )
        .await;
    coord
        .feed_data(
            "node-b",
            "df-iterative-exchange-convergence",
            0,
            "data",
            0,
            bincode::serialize(&vec![12i64]).unwrap(),
        )
        .await;

    coord
        .close_all_inputs("df-iterative-exchange-convergence")
        .await;
    coord
        .wait_for_completion("df-iterative-exchange-convergence")
        .await;

    let mut actual: Vec<i64> = collect_all_records(
        &mut coord,
        "df-iterative-exchange-convergence",
        &worker_counts,
    )
    .await
    .into_iter()
    .map(|(_, item)| item)
    .collect();
    actual.sort();

    let mut expected = vec![112i64, 192, 192];
    expected.sort();

    assert_eq!(actual, expected);
    assert!(actual.iter().all(|value| *value >= 100));
    coord.shutdown().await;
}

/// Large ExchangeRoundTrip batch across four workers.
#[tokio::test]
async fn test_large_batch_exchange() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 2).await;
    let topology = make_topology(&["node-a", "node-b"], 2);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-large-batch-exchange",
            &topology,
            DataflowType::ExchangeRoundTrip,
        )
        .await;

    assert_eq!(worker_counts["node-a"], 2);
    assert_eq!(worker_counts["node-b"], 2);

    let targets = [
        ("node-a", 0usize, 0u64..2500u64),
        ("node-a", 1usize, 2500u64..5000u64),
        ("node-b", 0usize, 5000u64..7500u64),
        ("node-b", 1usize, 7500u64..10000u64),
    ];
    let mut expected = Vec::new();
    for (node_id, worker_idx, range) in targets {
        let batch: Vec<(u64, String)> = range.map(|key| (key, format!("value-{key}"))).collect();
        expected.extend(batch.iter().cloned());
        coord
            .feed_data(
                node_id,
                "df-large-batch-exchange",
                worker_idx,
                "data",
                0,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-large-batch-exchange").await;
    coord.wait_for_completion("df-large-batch-exchange").await;

    let mut actual: Vec<(u64, String)> =
        collect_all_records(&mut coord, "df-large-batch-exchange", &worker_counts)
            .await
            .into_iter()
            .map(|(_, item)| item)
            .collect();
    actual.sort_by_key(|(key, _)| *key);
    expected.sort_by_key(|(key, _)| *key);

    assert_eq!(actual.len(), 10_000);
    assert_eq!(actual, expected);
    coord.shutdown().await;
}

/// DistributedWordCount over multiple epochs.
#[tokio::test]
async fn test_many_epochs_word_count() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b"], 1).await;
    let topology = make_topology(&["node-a", "node-b"], 1);

    let worker_counts = coord
        .setup_and_spawn_dataflow(
            "df-many-epochs-word-count",
            &topology,
            DataflowType::DistributedWordCount,
        )
        .await;

    let inputs = [
        (0u64, "node-a", vec![String::from("apple banana apple")]),
        (0u64, "node-b", vec![String::from("banana carrot")]),
        (1u64, "node-a", vec![String::from("delta echo")]),
        (1u64, "node-b", vec![String::from("echo echo")]),
        (2u64, "node-a", vec![String::from("rust rust tokio")]),
        (2u64, "node-b", vec![String::from("tokio")]),
        (3u64, "node-a", vec![String::from("alpha beta")]),
        (3u64, "node-b", vec![String::from("beta gamma beta")]),
        (4u64, "node-a", vec![String::from("final test final")]),
        (4u64, "node-b", vec![String::from("test")]),
    ];

    for (epoch, node_id, batch) in inputs {
        coord
            .feed_data(
                node_id,
                "df-many-epochs-word-count",
                0,
                "sentences",
                epoch,
                bincode::serialize(&batch).unwrap(),
            )
            .await;
    }

    coord.close_all_inputs("df-many-epochs-word-count").await;
    coord.wait_for_completion("df-many-epochs-word-count").await;

    let mut actual: BTreeMap<u64, BTreeMap<String, u64>> = BTreeMap::new();
    for (ts, (word, count)) in collect_all_records::<(String, u64)>(
        &mut coord,
        "df-many-epochs-word-count",
        &worker_counts,
    )
    .await
    {
        *actual.entry(ts).or_default().entry(word).or_default() += count;
    }

    let expected = BTreeMap::from([
        (
            0u64,
            BTreeMap::from([
                (String::from("apple"), 2u64),
                (String::from("banana"), 2u64),
                (String::from("carrot"), 1u64),
            ]),
        ),
        (
            1u64,
            BTreeMap::from([(String::from("delta"), 1u64), (String::from("echo"), 3u64)]),
        ),
        (
            2u64,
            BTreeMap::from([(String::from("rust"), 2u64), (String::from("tokio"), 2u64)]),
        ),
        (
            3u64,
            BTreeMap::from([
                (String::from("alpha"), 1u64),
                (String::from("beta"), 3u64),
                (String::from("gamma"), 1u64),
            ]),
        ),
        (
            4u64,
            BTreeMap::from([(String::from("final"), 2u64), (String::from("test"), 2u64)]),
        ),
    ]);

    assert_eq!(actual, expected);
    coord.shutdown().await;
}
