//! Integration tests for the broadcast operator with multiple workers.
//!
//! These tests validate:
//! - `broadcast()` sends all data to every worker (fan-out)
//! - Each worker receives a complete copy of all input data
//! - Broadcast works with multiple epochs
//! - Broadcast combined with downstream operators (map, reduce)
//! - Broadcast with empty input

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Helper: spawn a multi-worker dataflow, send data, collect each worker's
/// timestamped results.
async fn run_broadcast_timed_per_worker<D, F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
    output_name: &str,
) -> Vec<Vec<(u64, D)>>
where
    D: Clone + Send + Sync + 'static,
    F: Fn(&mut DataflowBuilder<u64>) + Send + Sync + 'static,
{
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let build_fn = Arc::new(build_fn);

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
        tokio::task::spawn_blocking(move || {
            let joined = multi.join_blocking();
            let collected: Vec<_> = receivers.into_iter().map(|r| r.collect_data()).collect();
            (joined, collected)
        }),
    )
    .await;

    let collected = match result {
        Ok(Ok((Ok(()), collected))) => collected,
        Ok(Ok((Err(e), _))) => panic!("dataflow join failed: {e}"),
        Ok(Err(e)) => panic!("spawn_blocking panicked: {e}"),
        Err(_) => panic!("dataflow did not complete within {TEST_TIMEOUT:?}"),
    };

    let mut per_worker: Vec<Vec<(u64, D)>> = Vec::new();
    for batches in collected {
        let mut worker_data = Vec::new();
        for (time, data) in batches {
            worker_data.extend(data.into_iter().map(|d| (time, d)));
        }
        per_worker.push(worker_data);
    }
    per_worker
}

/// Helper: spawn a multi-worker dataflow, send data, collect results per worker.
async fn run_broadcast_per_worker<D, F>(
    name: &str,
    num_workers: usize,
    build_fn: F,
    input_data: Vec<(u64, Vec<i64>)>,
    output_name: &str,
) -> Vec<Vec<D>>
where
    D: Clone + Send + Sync + 'static,
    F: Fn(&mut DataflowBuilder<u64>) + Send + Sync + 'static,
{
    run_broadcast_timed_per_worker(name, num_workers, build_fn, input_data, output_name)
        .await
        .into_iter()
        .map(|worker| worker.into_iter().map(|(_time, d)| d).collect())
        .collect()
}

/// Helper: collect all results across workers, sorted.
async fn run_broadcast_all<D, F>(
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
    let per_worker =
        run_broadcast_per_worker(name, num_workers, build_fn, input_data, output_name).await;
    let mut all: Vec<D> = per_worker.into_iter().flatten().collect();
    all.sort();
    all
}

// ============================================================
// Test: broadcast delivers all data to every worker
// ============================================================
/// Each worker, of one or of three, receives every record exactly once, at
/// its time.
#[tokio::test]
async fn broadcast_delivers_to_all_workers() {
    for workers in [1, 3] {
        let per_worker = run_broadcast_timed_per_worker(
            &format!("broadcast_all_{workers}"),
            workers,
            |builder| {
                let input = builder.input::<i64>("data").unwrap();
                let broadcast = input.broadcast("bcast");
                broadcast.output("out").unwrap();
            },
            vec![(0, vec![10, 20, 30])],
            "out",
        )
        .await;

        assert_eq!(per_worker.len(), workers);
        // Every worker should get all 3 items.
        for (w, data) in per_worker.iter().enumerate() {
            let mut sorted: Vec<i64> = data.iter().map(|(_time, x)| *x).collect();
            sorted.sort();
            assert_eq!(sorted, vec![10, 20, 30], "worker {w} should have all items");
            let mut timed = data.clone();
            timed.sort();
            assert_eq!(
                timed,
                vec![(0, 10), (0, 20), (0, 30)],
                "worker {w} of {workers}: each record once, at its time"
            );
        }
    }
}

// ============================================================
// Test: broadcast with 4 workers — total items = input * workers
// ============================================================
#[tokio::test]
async fn broadcast_multiplies_data_by_worker_count() {
    let all: Vec<i64> = run_broadcast_all(
        "broadcast_4w",
        4,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            broadcast.output("out").unwrap();
        },
        vec![(0, vec![1, 2, 3, 4, 5])],
        "out",
    )
    .await;

    // 5 items × 4 workers = 20 total items in output.
    assert_eq!(all.len(), 20);
    // Each value appears exactly 4 times.
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for v in &all {
        *counts.entry(*v).or_insert(0) += 1;
    }
    for i in 1..=5 {
        assert_eq!(counts[&i], 4, "value {i} should appear 4 times");
    }
}

// ============================================================
// Test: broadcast with multiple epochs
// ============================================================
#[tokio::test]
async fn broadcast_multiple_epochs() {
    let per_worker: Vec<Vec<i64>> = run_broadcast_per_worker(
        "broadcast_epochs",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            broadcast.output("out").unwrap();
        },
        vec![(0, vec![100, 200]), (1, vec![300, 400]), (2, vec![500])],
        "out",
    )
    .await;

    // Each worker should receive all 5 items (across all epochs).
    for (w, data) in per_worker.iter().enumerate() {
        let mut sorted: Vec<i64> = data.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![100, 200, 300, 400, 500],
            "worker {w} should have all items from all epochs"
        );
    }
}

// ============================================================
// Test: broadcast with empty input produces no output
// ============================================================
#[tokio::test]
async fn broadcast_empty_input() {
    let all: Vec<i64> = run_broadcast_all(
        "broadcast_empty",
        3,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            broadcast.output("out").unwrap();
        },
        vec![(0, vec![])],
        "out",
    )
    .await;

    assert!(
        all.is_empty(),
        "no data should come through on broadcast of empty input"
    );
}

// ============================================================
// Test: broadcast followed by map
// ============================================================
#[tokio::test]
async fn broadcast_then_map() {
    let per_worker: Vec<Vec<i64>> = run_broadcast_per_worker(
        "broadcast_map",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            let doubled = broadcast.map("double", |_t, x| x * 2);
            doubled.output("out").unwrap();
        },
        vec![(0, vec![1, 2, 3])],
        "out",
    )
    .await;

    // Each worker gets [1,2,3] broadcast, then doubled → [2,4,6].
    for (w, data) in per_worker.iter().enumerate() {
        let mut sorted: Vec<i64> = data.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![2, 4, 6],
            "worker {w} should have doubled values"
        );
    }
}

// ============================================================
// Test: broadcast followed by gather collects multiplied data to worker 0
// ============================================================
#[tokio::test]
async fn broadcast_then_gather() {
    let per_worker: Vec<Vec<i64>> = run_broadcast_per_worker(
        "broadcast_gather",
        3,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            let gathered = broadcast.gather("collect");
            gathered.output("out").unwrap();
        },
        vec![(0, vec![7, 8])],
        "out",
    )
    .await;

    // After broadcast (3 copies), gather collects all to worker 0.
    // Worker 0 gets 2×3 = 6 items.
    let mut w0: Vec<i64> = per_worker[0].clone();
    w0.sort();
    assert_eq!(w0, vec![7, 7, 7, 8, 8, 8]);

    // Other workers have nothing after gather.
    for (w, data) in per_worker.iter().enumerate().skip(1) {
        assert!(data.is_empty(), "worker {w} should be empty after gather");
    }
}

// ============================================================
// Test: broadcast single item to 2 workers
// ============================================================
#[tokio::test]
async fn broadcast_single_item() {
    let per_worker: Vec<Vec<i64>> = run_broadcast_per_worker(
        "broadcast_single",
        2,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            broadcast.output("out").unwrap();
        },
        vec![(0, vec![42])],
        "out",
    )
    .await;

    for (w, data) in per_worker.iter().enumerate() {
        assert_eq!(data, &vec![42i64], "worker {w} should have the single item");
    }
}

// ============================================================
// Test: broadcast with large batch
// ============================================================
#[tokio::test]
async fn broadcast_large_batch() {
    let input_data: Vec<i64> = (0..1000).collect();
    let all: Vec<i64> = run_broadcast_all(
        "broadcast_large",
        4,
        |builder| {
            let input = builder.input::<i64>("data").unwrap();
            let broadcast = input.broadcast("bcast");
            broadcast.output("out").unwrap();
        },
        vec![(0, input_data.clone())],
        "out",
    )
    .await;

    // 1000 items × 4 workers = 4000 total.
    assert_eq!(all.len(), 4000);
    // Each value 0..1000 appears exactly 4 times.
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for v in &all {
        *counts.entry(*v).or_insert(0) += 1;
    }
    for i in 0..1000 {
        assert_eq!(
            counts.get(&i).copied().unwrap_or(0),
            4,
            "value {i} should appear 4 times"
        );
    }
}
