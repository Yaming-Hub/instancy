//! Integration tests for branch and distribution operators with multiple workers.
//!
//! These tests validate:
//! - `branch()` predicate-based splitting with multi-worker exchange
//! - `gather()` routing all data to worker 0
//! - `rebalance()` round-robin distribution across workers
//! - `rebalance_to()` with target parallelism
//! - Combinations: branch → gather, exchange → branch → reduce

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Helper: spawn a multi-worker dataflow, send data, collect sorted results from a named output.
async fn run_multi_worker<D, F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
    output_name: &str,
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
                move |builder| {
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

    let out_name = output_name.to_string();
    let mut receivers = Vec::new();
    for w in 0..num_workers {
        receivers.push(multi.take_output::<D>(w, &out_name).unwrap());
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

/// Helper: spawn a multi-worker dataflow with TWO outputs (for branch tests).
async fn run_multi_worker_two_outputs<D, F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
    output_true: &str,
    output_false: &str,
) -> (Vec<D>, Vec<D>)
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
                move |builder| {
                    build_fn(builder);
                    Ok(())
                }
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let sender = multi.take_input::<i64>(0, "data").unwrap();
    for w in 1..num_workers {
        drop(multi.take_input::<i64>(w, "data").unwrap());
    }

    for (epoch, data) in input_data {
        sender.send(epoch, data).unwrap();
    }
    drop(sender);

    let mut true_receivers = Vec::new();
    let mut false_receivers = Vec::new();
    for w in 0..num_workers {
        true_receivers.push(multi.take_output::<D>(w, output_true).unwrap());
        false_receivers.push(multi.take_output::<D>(w, output_false).unwrap());
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

    let mut true_results: Vec<D> = Vec::new();
    for recv in true_receivers {
        for (_time, data) in recv.collect_data() {
            true_results.extend(data);
        }
    }
    true_results.sort();

    let mut false_results: Vec<D> = Vec::new();
    for recv in false_receivers {
        for (_time, data) in recv.collect_data() {
            false_results.extend(data);
        }
    }
    false_results.sort();

    (true_results, false_results)
}

// =============================================================================
// branch() tests
// =============================================================================

/// branch with exchange: data is distributed then split by predicate.
/// Even numbers go to the "true" branch, odd to "false".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_even_odd() {
    let (evens, odds) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-even-odd",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (evens, odds) = exchanged.branch("parity", |_t, x| x % 2 == 0);
            evens.output("evens").unwrap();
            odds.output("odds").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "evens",
        "odds",
    )
    .await;

    assert_eq!(evens, vec![2, 4, 6, 8, 10]);
    assert_eq!(odds, vec![1, 3, 5, 7, 9]);
}

/// branch with multi-epoch: each epoch is independently split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_multi_epoch() {
    let (positives, non_positives) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-multi-epoch",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (pos, non_pos) = exchanged.branch("sign", |_t, x| *x > 0);
            pos.output("positives").unwrap();
            non_pos.output("non_positives").unwrap();
        },
        vec![(0, vec![-3, -1, 0, 1, 3]), (1, vec![-5, 2, 4])],
        "positives",
        "non_positives",
    )
    .await;

    assert_eq!(positives, vec![1, 2, 3, 4]);
    assert_eq!(non_positives, vec![-5, -3, -1, 0]);
}

/// branch → reduce: split data and reduce each branch independently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_then_reduce() {
    let (even_sums, odd_sums) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-reduce",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (evens, odds) = exchanged.branch("parity", |_t, x| x % 2 == 0);
            evens
                .reduce("sum-evens", |acc, x| acc + x)
                .output("even_sums").unwrap();
            odds.reduce("sum-odds", |acc, x| acc + x).output("odd_sums").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "even_sums",
        "odd_sums",
    )
    .await;

    let even_total: i64 = even_sums.iter().sum();
    let odd_total: i64 = odd_sums.iter().sum();
    assert_eq!(even_total, 30, "sum of evens (2+4+6+8+10) = 30");
    assert_eq!(odd_total, 25, "sum of odds (1+3+5+7+9) = 25");
}

/// branch with 3 workers: validates correct fan-out with more parallelism.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_three_workers() {
    let (big, small) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-3w",
        3,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (big, small) = exchanged.branch("threshold", |_t, x| *x > 5);
            big.output("big").unwrap();
            small.output("small").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "big",
        "small",
    )
    .await;

    assert_eq!(big, vec![6, 7, 8, 9, 10]);
    assert_eq!(small, vec![1, 2, 3, 4, 5]);
}

/// branch where one side is empty: all items match predicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_all_true() {
    let (matched, unmatched) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-all-true",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (matched, unmatched) = exchanged.branch("always-true", |_t, _x| true);
            matched.output("matched").unwrap();
            unmatched.output("unmatched").unwrap();
        },
        vec![(0, vec![1, 2, 3, 4, 5])],
        "matched",
        "unmatched",
    )
    .await;

    assert_eq!(matched, vec![1, 2, 3, 4, 5]);
    assert_eq!(unmatched, Vec::<i64>::new());
}

// =============================================================================
// gather() tests
// =============================================================================

/// gather routes all data to worker 0 after exchange distributes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_gather_collects_all() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let mut multi = rt
        .spawn_multi(
            "mw-gather",
            2,
            |builder| {
                let input = builder.input::<i64>("data").unwrap();
                let gathered = input
                    .exchange_by_hash("distribute", |x| *x as u64)
                    .gather("collect");
                gathered.output("results").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let sender = multi.take_input::<i64>(0, "data").unwrap();
    drop(multi.take_input::<i64>(1, "data").unwrap());

    sender.send(0, (1..=10).collect()).unwrap();
    drop(sender);

    // Worker 0 should have all the data after gather
    let recv0 = multi.take_output::<i64>(0, "results").unwrap();
    let recv1 = multi.take_output::<i64>(1, "results").unwrap();

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

    let mut w0_data: Vec<i64> = recv0
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    w0_data.sort();

    let w1_data: Vec<i64> = recv1
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    assert_eq!(w0_data, (1..=10).collect::<Vec<_>>());
    assert!(
        w1_data.is_empty(),
        "worker 1 should have no data after gather"
    );
}

/// gather → reduce: all data gathered to worker 0 then reduced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_gather_then_reduce() {
    let results = run_multi_worker::<i64, _>(
        "mw-gather-reduce",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            input
                .exchange_by_hash("distribute", |x| *x as u64)
                .gather("collect")
                .reduce("sum", |acc, x| acc + x)
                .output("results").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "results",
    )
    .await;

    // After gather to worker 0, reduce produces a single sum there.
    // Worker 1 has no data so produces no output.
    let total: i64 = results.iter().sum();
    assert_eq!(total, 55);
}

// =============================================================================
// rebalance() tests
// =============================================================================

/// rebalance distributes data round-robin; total data is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_preserves_all() {
    let results = run_multi_worker::<i64, _>(
        "mw-rebalance-all",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            input.rebalance("redistribute").output("results").unwrap();
        },
        vec![(0, (1..=20).collect())],
        "results",
    )
    .await;

    assert_eq!(results, (1..=20).collect::<Vec<_>>());
}

/// rebalance → fold: data spread round-robin, then folded on each worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_then_fold() {
    let results = run_multi_worker::<i64, _>(
        "mw-rebalance-fold",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            input
                .rebalance("redistribute")
                .fold("count", 0i64, |acc, _x| acc + 1)
                .output("results").unwrap();
        },
        vec![(0, (1..=20).collect())],
        "results",
    )
    .await;

    // Each worker gets ~10 items via round-robin (total = 20)
    let total: i64 = results.iter().sum();
    assert_eq!(total, 20, "total count across workers should be 20");
    // Both workers should have gotten some data (round-robin)
    assert!(results.len() >= 2, "both workers should produce output");
}

/// rebalance with 3 workers: data spread more evenly than exchange hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_three_workers() {
    let results = run_multi_worker::<i64, _>(
        "mw-rebalance-3w",
        3,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            input.rebalance("redistribute").output("results").unwrap();
        },
        vec![(0, (1..=30).collect())],
        "results",
    )
    .await;

    assert_eq!(results, (1..=30).collect::<Vec<_>>());
}

/// rebalance actually distributes data across workers (per-worker assertion).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_per_worker_distribution() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let mut multi = rt
        .spawn_multi(
            "mw-rebalance-perw",
            2,
            |builder| {
                let input = builder.input::<i64>("data").unwrap();
                input.rebalance("redistribute").output("results").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let sender = multi.take_input::<i64>(0, "data").unwrap();
    drop(multi.take_input::<i64>(1, "data").unwrap());

    // Send 20 items — round-robin should give ~10 to each worker
    sender.send(0, (1..=20).collect()).unwrap();
    drop(sender);

    let recv0 = multi.take_output::<i64>(0, "results").unwrap();
    let recv1 = multi.take_output::<i64>(1, "results").unwrap();

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

    let w0_data: Vec<i64> = recv0
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let w1_data: Vec<i64> = recv1
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    // Both workers should have received data (not all on one worker)
    assert!(
        !w0_data.is_empty(),
        "worker 0 should have data from rebalance"
    );
    assert!(
        !w1_data.is_empty(),
        "worker 1 should have data from rebalance"
    );
    // Total should be 20
    assert_eq!(w0_data.len() + w1_data.len(), 20);
    // All original values present
    let mut all: Vec<i64> = [w0_data, w1_data].concat();
    all.sort();
    assert_eq!(all, (1..=20).collect::<Vec<_>>());
}

// =============================================================================
// rebalance_to() tests
// =============================================================================

/// rebalance_to with matching parallelism: round-robin to N workers.
/// Note: rebalance_to(N) where N != worker_count is rejected until per-stage
/// parallelism is implemented. This test uses matching counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_to_matching() {
    let results = run_multi_worker::<i64, _>(
        "mw-rebalance-to",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            // rebalance_to(2) with 2 workers — same as rebalance() but explicit
            input.rebalance_to("redistribute", 2).unwrap().output("results").unwrap();
        },
        vec![(0, (1..=12).collect())],
        "results",
    )
    .await;

    // All data should be preserved
    assert_eq!(results, (1..=12).collect::<Vec<_>>());
}

/// rebalance_to with per-worker assertion: data actually distributed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_rebalance_to_distributes() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let mut multi = rt
        .spawn_multi(
            "mw-rebalance-to-dist",
            2,
            |builder| {
                let input = builder.input::<i64>("data").unwrap();
                input.rebalance_to("redistribute", 2).unwrap().output("results").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    let sender = multi.take_input::<i64>(0, "data").unwrap();
    drop(multi.take_input::<i64>(1, "data").unwrap());

    sender.send(0, (1..=20).collect()).unwrap();
    drop(sender);

    let recv0 = multi.take_output::<i64>(0, "results").unwrap();
    let recv1 = multi.take_output::<i64>(1, "results").unwrap();

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

    let w0_data: Vec<i64> = recv0
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    let w1_data: Vec<i64> = recv1
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    // Both workers should receive data
    assert!(!w0_data.is_empty(), "worker 0 should have data");
    assert!(!w1_data.is_empty(), "worker 1 should have data");
    assert_eq!(w0_data.len() + w1_data.len(), 20);
}

// =============================================================================
// Edge cases
// =============================================================================

/// branch with empty input: both branches should be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_empty_input() {
    let (matched, unmatched) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-empty",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (m, u) = exchanged.branch("split", |_t, _x| true);
            m.output("matched").unwrap();
            u.output("unmatched").unwrap();
        },
        vec![], // no data
        "matched",
        "unmatched",
    )
    .await;

    assert!(matched.is_empty());
    assert!(unmatched.is_empty());
}

/// branch where all items go to false branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_all_false() {
    let (matched, unmatched) = run_multi_worker_two_outputs::<i64, _>(
        "mw-branch-all-false",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (m, u) = exchanged.branch("never", |_t, _x| false);
            m.output("matched").unwrap();
            u.output("unmatched").unwrap();
        },
        vec![(0, vec![1, 2, 3, 4, 5])],
        "matched",
        "unmatched",
    )
    .await;

    assert!(matched.is_empty());
    assert_eq!(unmatched, vec![1, 2, 3, 4, 5]);
}

// =============================================================================
// Combined branch + distribution tests
// =============================================================================

/// exchange → branch → gather: branch after exchange, then gather each branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_exchange_branch_gather() {
    let (evens, odds) = run_multi_worker_two_outputs::<i64, _>(
        "mw-ex-branch-gather",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (evens, odds) = exchanged.branch("parity", |_t, x| x % 2 == 0);
            evens.gather("gather-evens").output("evens").unwrap();
            odds.gather("gather-odds").output("odds").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "evens",
        "odds",
    )
    .await;

    assert_eq!(evens, vec![2, 4, 6, 8, 10]);
    assert_eq!(odds, vec![1, 3, 5, 7, 9]);
}

/// branch → rebalance → count: split, redistribute, then count per worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_branch_rebalance_count() {
    let (even_counts, odd_counts) = run_multi_worker_two_outputs::<usize, _>(
        "mw-branch-rebalance-count",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (evens, odds) = exchanged.branch("parity", |_t, x| x % 2 == 0);
            evens
                .rebalance("rebal-evens")
                .count("count-evens")
                .output("even_counts").unwrap();
            odds.rebalance("rebal-odds")
                .count("count-odds")
                .output("odd_counts").unwrap();
        },
        vec![(0, (1..=10).collect())],
        "even_counts",
        "odd_counts",
    )
    .await;

    let total_evens: usize = even_counts.iter().sum();
    let total_odds: usize = odd_counts.iter().sum();
    assert_eq!(total_evens, 5, "5 even numbers in 1..=10");
    assert_eq!(total_odds, 5, "5 odd numbers in 1..=10");
}

/// map → branch → map on each side: validates data transforms after branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_map_branch_map() {
    let (doubled_evens, tripled_odds) = run_multi_worker_two_outputs::<i64, _>(
        "mw-map-branch-map",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let exchanged = input.exchange_by_hash("distribute", |x| *x as u64);
            let (evens, odds) = exchanged.branch("parity", |_t, x| x % 2 == 0);
            evens.map("double", |_t, x| x * 2).output("doubled_evens").unwrap();
            odds.map("triple", |_t, x| x * 3).output("tripled_odds").unwrap();
        },
        vec![(0, (1..=6).collect())],
        "doubled_evens",
        "tripled_odds",
    )
    .await;

    // evens: 2,4,6 → doubled: 4,8,12
    // odds: 1,3,5 → tripled: 3,9,15
    assert_eq!(doubled_evens, vec![4, 8, 12]);
    assert_eq!(tripled_odds, vec![3, 9, 15]);
}
