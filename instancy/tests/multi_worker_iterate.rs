//! Integration tests for iterative (feedback loop) dataflows with multiple workers.
//!
//! These tests validate that `iterate()` works correctly when data is
//! distributed across multiple workers via `exchange`. This exercises:
//! - Progress tracking across workers inside loop scopes
//! - Exchange-based data redistribution within iteration bodies
//! - Correct convergence with parallel workers
//! - Edge cases: empty input, immediate exit, many iterations

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Helper: spawn a multi-worker dataflow, wait with timeout, collect sorted results.
async fn run_multi_worker_iterate<F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
) -> Vec<i64>
where
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

    // Collect outputs from all workers.
    let mut receivers = Vec::new();
    for w in 0..num_workers {
        receivers.push(multi.take_output::<i64>(w, "results").unwrap());
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

    let mut all_results: Vec<i64> = Vec::new();
    for recv in receivers {
        for (_time, data) in recv.collect_data() {
            all_results.extend(data);
        }
    }
    all_results.sort();
    all_results
}

/// Multi-worker doubling loop: each value doubles until >= 100.
///
/// Uses exchange inside the loop to redistribute data across workers
/// after each doubling step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_doubling() {
    let results = run_multi_worker_iterate(
        "mw-doubling",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("double-loop", 1u32, |iter_var| {
                let doubled = iter_var
                    .map("double", |_t: &Product<u64, u32>, x| x * 2)
                    .exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = doubled.clone().filter("done", |_t, x| *x >= 100);
                let again = doubled.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        vec![(0, vec![1, 2, 3, 5, 10, 50])],
    )
    .await;

    // Same expected results as single-worker version
    assert_eq!(results.len(), 6);
    assert_eq!(results, vec![100, 128, 128, 160, 160, 192]);
}

/// Multi-worker immediate exit: all values already above threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_immediate_exit() {
    let results = run_multi_worker_iterate(
        "mw-immediate-exit",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("no-loop", 1u32, |iter_var| {
                let redistributed = iter_var.exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = redistributed.clone().filter("done", |_t, _x| true);
                let again = redistributed.filter("again", |_t, _x| false);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        vec![(0, vec![10, 20, 30])],
    )
    .await;

    assert_eq!(results, vec![10, 20, 30]);
}

/// Multi-worker empty input: should complete without error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_empty_input() {
    let results = run_multi_worker_iterate(
        "mw-empty",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("empty-loop", 1u32, |iter_var| {
                let redistributed = iter_var.exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = redistributed.clone().filter("done", |_t, x| *x >= 10);
                let again = redistributed.filter("again", |_t, x| *x < 10);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        vec![],
    )
    .await;

    assert!(results.is_empty());
}

/// Multi-worker Collatz iteration with exchange redistribution.
///
/// Tests complex branching logic inside the loop with data moving
/// between workers on each iteration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_collatz() {
    let results = run_multi_worker_iterate(
        "mw-collatz",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("collatz-loop", 1u32, |iter_var| {
                let stepped = iter_var
                    .map(
                        "collatz-step",
                        |_t: &Product<u64, u32>, x| {
                            if x % 2 == 0 { x / 2 } else { 3 * x + 1 }
                        },
                    )
                    .exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = stepped.clone().filter("reached-1", |_t, x| *x == 1);
                let again = stepped.filter("continue", |_t, x| *x != 1);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        vec![(0, vec![6, 7, 12])],
    )
    .await;

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|&x| x == 1));
}

/// Multi-worker iteration with multiple epochs.
///
/// Data from different timestamps enters the loop independently,
/// each epoch iterates until convergence across parallel workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_multiple_epochs() {
    let results = run_multi_worker_iterate(
        "mw-multi-epoch",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("inc-loop", 1u32, |iter_var| {
                let incremented = iter_var
                    .map("add10", |_t: &Product<u64, u32>, x| x + 10)
                    .exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = incremented.clone().filter("done", |_t, x| *x >= 50);
                let again = incremented.filter("again", |_t, x| *x < 50);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        vec![(0, vec![0]), (1, vec![35]), (2, vec![45])],
    )
    .await;

    assert_eq!(results.len(), 3);
    assert_eq!(results, vec![50, 55, 55]);
}

/// Multi-worker iteration with more workers (3) to stress progress tracking.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_worker_iterate_three_workers() {
    let results = run_multi_worker_iterate(
        "mw-3-workers",
        3,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let output = input.iterate::<u32>("triple-loop", 1u32, |iter_var| {
                let tripled = iter_var
                    .map("triple", |_t: &Product<u64, u32>, x| x * 3)
                    .exchange_by_hash("redistribute", |x: &i64| *x as u64);
                let done = tripled.clone().filter("done", |_t, x| *x >= 100);
                let again = tripled.filter("again", |_t, x| *x < 100);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            output.output("results").unwrap();
        },
        // 1 → 3 → 9 → 27 → 81 → 243
        // 5 → 15 → 45 → 135
        // 50 → 150
        vec![(0, vec![1, 5, 50])],
    )
    .await;

    assert_eq!(results.len(), 3);
    assert_eq!(results, vec![135, 150, 243]);
}

/// Multi-worker iteration with data entering from ALL workers simultaneously.
///
/// Unlike other tests that feed data only through worker 0, this test
/// sends initial data through both workers to exercise concurrent entry
/// into the iterate scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_iterate_input_from_all_workers() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let mut multi = rt
        .spawn_multi(
            "mw-all-inputs",
            2,
            |builder| {
                let input = builder.input::<i64>("data").unwrap();
                let output = input.iterate::<u32>("double-loop", 1u32, |iter_var| {
                    let doubled = iter_var
                        .map("double", |_t: &Product<u64, u32>, x| x * 2)
                        .exchange_by_hash("redistribute", |x: &i64| *x as u64);
                    let done = doubled.clone().filter("done", |_t, x| *x >= 100);
                    let again = doubled.filter("again", |_t, x| *x < 100);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                });
                output.output("results").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    // Send data through BOTH workers concurrently.
    let sender0 = multi.take_input::<i64>(0, "data").unwrap();
    let sender1 = multi.take_input::<i64>(1, "data").unwrap();

    // Worker 0: 1 → 2 → 4 → 8 → 16 → 32 → 64 → 128
    sender0.send(0, vec![1]).unwrap();
    // Worker 1: 10 → 20 → 40 → 80 → 160
    sender1.send(0, vec![10]).unwrap();

    drop(sender0);
    drop(sender1);

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

    let mut all_results: Vec<i64> = Vec::new();
    for (_time, data) in recv0.collect_data() {
        all_results.extend(data);
    }
    for (_time, data) in recv1.collect_data() {
        all_results.extend(data);
    }
    all_results.sort();

    assert_eq!(all_results.len(), 2);
    assert_eq!(all_results, vec![128, 160]);
}
