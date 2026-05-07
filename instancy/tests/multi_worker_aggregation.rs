//! Integration tests for aggregation operators (reduce, fold, distinct, count)
//! with multiple workers and exchange.
//!
//! These tests validate that aggregation operators work correctly when data is
//! distributed across multiple workers. This exercises:
//! - reduce/fold correctness with exchanged data (data arrives from multiple sources)
//! - distinct deduplication across exchanged batches
//! - count accuracy with distributed inputs
//! - Timestamp-based aggregation with multi-epoch inputs

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Helper: spawn a multi-worker dataflow, send data through all workers, collect sorted results.
async fn run_multi_worker_aggregation<D, F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
) -> Vec<D>
where
    D: Clone + Send + Sync + Ord + 'static,
    F: Fn(&mut DataflowBuilder<u64>) + Send + Sync + 'static,
{
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let build_fn = std::sync::Arc::new(build_fn);

    let mut multi = rt
        .spawn_multi(
            name,
            num_workers,
            {
                let build_fn = build_fn.clone();
                move |_worker_idx, builder| {
                    build_fn(builder);
                    Ok(())
                }
            },
            SpawnOptions::default(),
        )
        .unwrap();

    // Send data through worker 0, close all other workers' inputs.
    let sender = multi.take_input::<i64>(0, "data").unwrap();
    for w in 1..num_workers {
        drop(multi.take_input::<i64>(w, "data").unwrap());
    }

    for (epoch, data) in input_data {
        sender.send(epoch, data).unwrap();
    }
    drop(sender);

    // Collect outputs from all workers.
    let mut receivers = Vec::new();
    for w in 0..num_workers {
        receivers.push(multi.take_output::<D>(w, "results").unwrap());
    }

    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        tokio::task::spawn_blocking(move || multi.join_blocking()),
    )
    .await;

    match result {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("dataflow join failed: {e}"),
        Ok(Err(e)) => panic!("spawn_blocking panicked: {e}"),
        Err(_) => panic!("dataflow did not complete within {TEST_TIMEOUT:?}"),
    }

    let mut all_results: Vec<D> = Vec::new();
    for recv in receivers {
        for (_time, data) in recv.collect_data() {
            all_results.extend(data);
        }
    }
    all_results.sort();
    all_results
}

// =============================================================================
// reduce() tests
// =============================================================================

/// Multi-worker reduce: sum elements per timestamp after exchange.
///
/// Data enters worker 0, gets redistributed via exchange, then reduced (summed)
/// per-timestamp on each worker. Results should be the total sum per timestamp
/// (split across workers).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_reduce_sum() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-reduce-sum",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let summed = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .reduce("sum", |acc, x| acc + x);
            summed.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ],
    )
    .await;

    // The sum of all items (1..=10 = 55) should be preserved across workers
    let total: i64 = results.iter().sum();
    assert_eq!(total, 55, "total sum should be 55, got {total}");
}

/// Multi-worker reduce with multiple epochs: each epoch reduced independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_reduce_multi_epoch() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-reduce-epoch",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let summed = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .reduce("sum", |acc, x| acc + x);
            summed.output("results");
        },
        vec![
            (0, vec![1, 2, 3]),       // sum = 6
            (1, vec![10, 20, 30]),    // sum = 60
            (2, vec![100, 200]),      // sum = 300
        ],
    )
    .await;

    let total: i64 = results.iter().sum();
    assert_eq!(total, 366, "total across all epochs should be 366, got {total}");
}

/// Multi-worker reduce with 3 workers: validates correct progress tracking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_reduce_three_workers() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-reduce-3w",
        3,
        |builder| {
            let input = builder.input::<i64>("data");
            let max = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .reduce("max", |acc, x| if x > acc { x } else { acc });
            max.output("results");
        },
        vec![
            (0, vec![3, 7, 1, 9, 2, 8, 4, 6, 5, 10]),
        ],
    )
    .await;

    // The max across all workers should be 10
    let max_val = *results.iter().max().unwrap();
    assert_eq!(max_val, 10, "max should be 10, got {max_val}");
}

// =============================================================================
// fold() tests
// =============================================================================

/// Multi-worker fold: count elements per timestamp using fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_fold_count() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-fold-count",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let counted = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .fold("count", 0i64, |acc, _x| acc + 1);
            counted.output("results");
        },
        vec![
            (0, vec![10, 20, 30, 40, 50, 60, 70, 80]),
        ],
    )
    .await;

    // Total count across all workers should be 8
    let total: i64 = results.iter().sum();
    assert_eq!(total, 8, "total count should be 8, got {total}");
}

/// Multi-worker fold: product of elements (distributive operation).
///
/// Multiplication with identity 1 is distributive: the product of per-worker
/// partial products equals the total product. This validates fold correctness
/// specifically for monoid-homomorphic operations across workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_fold_product() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-fold-product",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let product = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .fold("product", 1i64, |acc, x| acc * x);
            product.output("results");
        },
        vec![
            (0, vec![2, 3, 5]),  // total product = 30
        ],
    )
    .await;

    // Product of per-worker results equals total product (distributive property)
    let total: i64 = results.iter().product();
    assert_eq!(total, 30, "product should be 30, got {total}");
}

// =============================================================================
// distinct() tests
// =============================================================================

/// Multi-worker distinct: deduplicates elements after exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_distinct() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-distinct",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            // Exchange by value so same values go to same worker, then distinct
            let unique = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .distinct("dedup");
            unique.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 2, 1, 4, 3, 5, 1, 2, 3, 4, 5]),
        ],
    )
    .await;

    assert_eq!(results, vec![1, 2, 3, 4, 5], "distinct should produce [1,2,3,4,5]");
}

/// Multi-worker distinct with multiple epochs: dedup is per-timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_distinct_multi_epoch() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-distinct-epoch",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let unique = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .distinct("dedup");
            unique.output("results");
        },
        vec![
            (0, vec![1, 1, 2, 2, 3]),  // distinct: [1, 2, 3]
            (1, vec![1, 1, 1]),         // distinct: [1] (same value OK in different epoch)
            (2, vec![5, 5, 6]),         // distinct: [5, 6]
        ],
    )
    .await;

    // Results sorted across all epochs: [1, 1, 2, 3, 5, 6]
    assert_eq!(results, vec![1, 1, 2, 3, 5, 6]);
}

/// Multi-worker distinct with 3 workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_distinct_three_workers() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-distinct-3w",
        3,
        |builder| {
            let input = builder.input::<i64>("data");
            let unique = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .distinct("dedup");
            unique.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6]),
        ],
    )
    .await;

    assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
}

// =============================================================================
// count() tests
// =============================================================================

/// Multi-worker count: counts elements per timestamp after exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_count() {
    let results = run_multi_worker_aggregation::<usize, _>(
        "mw-count",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let counted = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .count("counter");
            counted.output("results");
        },
        vec![
            (0, vec![10, 20, 30, 40, 50]),
        ],
    )
    .await;

    let total: usize = results.iter().sum();
    assert_eq!(total, 5, "total count should be 5, got {total}");
}

/// Multi-worker count with multiple epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_count_multi_epoch() {
    let results = run_multi_worker_aggregation::<usize, _>(
        "mw-count-epoch",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let counted = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .count("counter");
            counted.output("results");
        },
        vec![
            (0, vec![1, 2, 3]),             // count = 3
            (1, vec![10, 20, 30, 40]),      // count = 4
            (2, vec![100]),                 // count = 1
        ],
    )
    .await;

    let total: usize = results.iter().sum();
    assert_eq!(total, 8, "total count across all epochs should be 8, got {total}");
}

// =============================================================================
// Edge case tests
// =============================================================================

/// Single element reduce: only one item, no actual combining needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_reduce_single_element() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-reduce-single",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let summed = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .reduce("sum", |acc, x| acc + x);
            summed.output("results");
        },
        vec![
            (0, vec![42]),
        ],
    )
    .await;

    // Single element goes to one worker, reduce returns it unchanged
    assert_eq!(results, vec![42]);
}

/// Single element fold: validates fold with minimal input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_fold_single_element() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-fold-single",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let summed = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .fold("sum", 0i64, |acc, x| acc + x);
            summed.output("results");
        },
        vec![
            (0, vec![42]),
        ],
    )
    .await;

    // Only one worker receives data, only that worker emits
    let total: i64 = results.iter().sum();
    assert_eq!(total, 42, "fold of single element should be 42, got {total}");
}

/// Fold with non-distributive operation: collect into sorted vec.
/// Verifies fold works correctly per-worker even with complex accumulator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_fold_collect_vec() {
    let results = run_multi_worker_aggregation::<Vec<i64>, _>(
        "mw-fold-vec",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let collected = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .fold("collect", Vec::<i64>::new(), |mut acc, x| {
                    acc.push(x);
                    acc
                });
            collected.output("results");
        },
        vec![
            (0, vec![5, 3, 1, 4, 2]),
        ],
    )
    .await;

    // Concatenate all per-worker vecs and sort — should have all elements
    let mut all: Vec<i64> = results.into_iter().flatten().collect();
    all.sort();
    assert_eq!(all, vec![1, 2, 3, 4, 5], "all elements should be present");
}

// =============================================================================
// Combined operator tests
// =============================================================================

/// Multi-worker pipeline: distinct → count (deduplicate then count unique items).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_distinct_then_count() {
    let results = run_multi_worker_aggregation::<usize, _>(
        "mw-distinct-count",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let unique_count = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .distinct("dedup")
                .count("count-unique");
            unique_count.output("results");
        },
        vec![
            (0, vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5]),
        ],
    )
    .await;

    let total: usize = results.iter().sum();
    assert_eq!(total, 5, "should count 5 unique items, got {total}");
}

/// Multi-worker pipeline: exchange → reduce → filter (sum then keep only large sums).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_reduce_then_filter() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-reduce-filter",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let large_sums = input
                .exchange_by_hash("distribute", |x: &i64| *x as u64)
                .reduce("sum", |acc, x| acc + x)
                .filter("large-only", |_t, x| *x > 10);
            large_sums.output("results");
        },
        vec![
            // Epoch 0: sum across workers, some worker sums may be > 10
            (0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            // Epoch 1: large values ensure at least one sum > 10
            (1, vec![50, 60]),
        ],
    )
    .await;

    // All results should be > 10
    for r in &results {
        assert!(*r > 10, "filtered result should be > 10, got {r}");
    }
    // At least epoch 1 should produce results (50+60=110 or individual worker sums)
    assert!(!results.is_empty(), "should have at least one result > 10");
}

// =============================================================================
// gather() and rebalance() tests
// =============================================================================

/// Multi-worker gather: all data routes to worker 0, producing a single reduce result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_gather_then_reduce() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-gather-reduce",
        3,
        |builder| {
            let input = builder.input::<i64>("data");
            let global_sum = input
                .gather("collect-all")
                .reduce("global-sum", |acc, x| acc + x);
            global_sum.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ],
    )
    .await;

    // Gather sends everything to worker 0, so reduce produces exactly one result
    assert_eq!(results, vec![55], "gather → reduce should produce single sum 55");
}

/// Multi-worker rebalance: data is evenly distributed, all items processed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_preserves_all() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-rebalance",
        3,
        |builder| {
            let input = builder.input::<i64>("data");
            let processed = input
                .rebalance("spread")
                .map("double", |_t, x| x * 2);
            processed.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ],
    )
    .await;

    // All items should be present (doubled)
    assert_eq!(results, vec![2, 4, 6, 8, 10, 12, 14, 16, 18]);
}

/// Multi-worker rebalance → fold: each worker folds its portion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_then_fold() {
    let results = run_multi_worker_aggregation::<i64, _>(
        "mw-rebalance-fold",
        2,
        |builder| {
            let input = builder.input::<i64>("data");
            let sums = input
                .rebalance("spread")
                .fold("sum", 0i64, |acc, x| acc + x);
            sums.output("results");
        },
        vec![
            (0, vec![1, 2, 3, 4, 5, 6]),
        ],
    )
    .await;

    // Sum of per-worker folds should equal total sum
    let total: i64 = results.iter().sum();
    assert_eq!(total, 21, "rebalance → fold total should be 21, got {total}");
}

