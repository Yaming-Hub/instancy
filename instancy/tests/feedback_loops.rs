//! Integration tests for iterative (feedback loop) dataflows.
//!
//! These tests validate that the `iterate()` API correctly:
//! - Creates loop scopes with enter/leave/feedback/concat operators
//! - Advances inner timestamps on each iteration
//! - Terminates when all data exits the loop
//! - Handles multiple iterations until convergence
//! - Works with the spawned dataflow API (external input/output)

use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Records observed inside a loop, with their timestamps.
type Seen<T> = Arc<Mutex<Vec<(T, u64)>>>;

/// Run `f` on another thread and fail the test if it takes over 30 s.
fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what}: did not finish within 30 s"))
}

/// Simple doubling loop: each value doubles until it exceeds a threshold.
///
/// Dataflow: input → iterate(double until >= 100) → output
///
/// Starting values: [1, 2, 3, 5, 10, 50]
/// Expected iterations:
///   1 → 2 → 4 → 8 → 16 → 32 → 64 → 128 (exits)
///   2 → 4 → 8 → 16 → 32 → 64 → 128 (exits)
///   3 → 6 → 12 → 24 → 48 → 96 → 192 (exits)
///   5 → 10 → 20 → 40 → 80 → 160 (exits)
///   10 → 20 → 40 → 80 → 160 (exits)
///   50 → 100 (exits, since 100 >= 100)
///
/// Expected output (sorted): [100, 128, 128, 160, 160, 192]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_doubling_until_threshold() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("doubling-loop");
    let input = builder.input::<i64>("data").unwrap();
    let output = input.iterate::<u32>("double-loop", 1u32, |iter_var| {
        let doubled = iter_var.map("double", |_t: &Product<u64, u32>, x| x * 2);
        let done = doubled.clone().filter("done", |_t, x| *x >= 100);
        let again = doubled.filter("again", |_t, x| *x < 100);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("data").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    sender.send(0, vec![1, 2, 3, 5, 10, 50]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();

    assert_eq!(results.len(), 6, "expected one output per input value");
    assert_eq!(results, vec![100, 128, 128, 160, 160, 192]);
}

/// Loop with immediate exit: all values already exceed threshold.
///
/// No data should loop back — everything exits immediately.
/// This tests the edge case where the feedback path is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_immediate_exit() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("immediate-exit");
    let input = builder.input::<i32>("data").unwrap();
    let output = input.iterate::<u32>("no-loop", 1u32, |iter_var| {
        // Everything passes the threshold immediately — nothing feeds back.
        let done = iter_var.clone().filter("done", |_t, x| *x >= 0);
        let again = iter_var.filter("again", |_t, _x| false);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i32>("data").unwrap();
    let receiver = spawned.take_output::<i32>("results").unwrap();

    sender.send(0, vec![10, 20, 30]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();

    assert_eq!(results, vec![10, 20, 30]);
}

/// Iterate with multiple input epochs.
///
/// Data from different timestamps enters the loop independently.
/// Each epoch's data iterates until convergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_multiple_epochs() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("multi-epoch-loop");
    let input = builder.input::<i64>("data").unwrap();
    // Increment by 10 until >= 50
    let output = input.iterate::<u32>("inc-loop", 1u32, |iter_var| {
        let incremented = iter_var.map("add10", |_t: &Product<u64, u32>, x| x + 10);
        let done = incremented.clone().filter("done", |_t, x| *x >= 50);
        let again = incremented.filter("again", |_t, x| *x < 50);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("data").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    // Epoch 0: start at 0 → 10 → 20 → 30 → 40 → 50 (exits)
    sender.send(0, vec![0]).unwrap();
    // Epoch 1: start at 35 → 45 → 55 (exits)
    sender.send(1, vec![35]).unwrap();
    // Epoch 2: start at 45 → 55 (exits)
    sender.send(2, vec![45]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();

    assert_eq!(results.len(), 3, "expected one output per input epoch");
    assert_eq!(results, vec![50, 55, 55]);
}

/// Iterate with empty input — should complete without error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_empty_input() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("empty-loop");
    let input = builder.input::<i32>("data").unwrap();
    let output = input.iterate::<u32>("empty-loop", 1u32, |iter_var| {
        let done = iter_var.clone().filter("done", |_t, x| *x >= 10);
        let again = iter_var.filter("again", |_t, x| *x < 10);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i32>("data").unwrap();
    let receiver = spawned.take_output::<i32>("results").unwrap();

    // Close input immediately with no data.
    drop(sender);

    spawned.join_blocking().unwrap();

    let results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    assert!(results.is_empty());
}

/// Iterate followed by more operators — output of the loop feeds into
/// downstream processing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_with_downstream_operators() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("loop-then-map");
    let input = builder.input::<i64>("data").unwrap();
    // Loop: double until >= 64
    let after_loop = input.iterate::<u32>("double-loop", 1u32, |iter_var| {
        let doubled = iter_var.map("double", |_t: &Product<u64, u32>, x| x * 2);
        let done = doubled.clone().filter("done", |_t, x| *x >= 64);
        let again = doubled.filter("again", |_t, x| *x < 64);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    // Post-loop: negate all results
    after_loop
        .map("negate", |_t, x: i64| -x)
        .output("results")
        .unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("data").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    // 1 → 2 → 4 → 8 → 16 → 32 → 64 (exits) → -64
    // 10 → 20 → 40 → 80 (exits) → -80
    sender.send(0, vec![1, 10]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();

    assert_eq!(results, vec![-80, -64]);
}

/// Collatz-like iteration: if even, halve; if odd, triple+1. Exit when = 1.
///
/// This tests a more complex iteration body with branching logic inside
/// the loop. Starting value: 6
///   6 → 3 → 10 → 5 → 16 → 8 → 4 → 2 → 1 (exits)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterate_collatz_sequence() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("collatz");
    let input = builder.input::<i64>("data").unwrap();
    let output = input.iterate::<u32>("collatz-loop", 1u32, |iter_var| {
        // Apply collatz step
        let stepped = iter_var.map(
            "collatz-step",
            |_t: &Product<u64, u32>, x| {
                if x % 2 == 0 { x / 2 } else { 3 * x + 1 }
            },
        );
        let done = stepped.clone().filter("reached-1", |_t, x| *x == 1);
        let again = stepped.filter("continue", |_t, x| *x != 1);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("data").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    sender.send(0, vec![6, 7, 12]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let results: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();

    // All values converge to 1
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|&x| x == 1));
}

/// An `iterate` whose output is the loop variable itself, with no operator in
/// between, passes every record through once, unchanged, at its outer time;
/// inside the loop each record is seen once, at inner time 0. With no input
/// the same loop sees and outputs nothing, and the dataflow completes. The
/// feedback branch carries nothing.
#[test]
fn iterate_with_the_loop_variable_as_output_passes_records_through_at_their_outer_time() {
    for records in [(0..10u64).collect::<Vec<_>>(), Vec::new()] {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let builder = DataflowBuilder::<u64>::new("pass-through-scope");
        let inside: Seen<Product<u64, u32>> = Arc::default();
        let observer = Arc::clone(&inside);
        builder
            .input::<u64>("data")
            .unwrap()
            .iterate::<u32>("scope", 1u32, move |var| {
                let observed =
                    var.clone()
                        .inspect("inside", move |t: &Product<u64, u32>, x: &u64| {
                            observer.lock().unwrap().push((*t, *x));
                        });
                IterateResult {
                    feedback: observed.filter("unused", |_t, _x| false),
                    output: var,
                }
            })
            .output("out")
            .unwrap();
        let mut spawned = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let sender = spawned.take_input::<u64>("data").unwrap();
        let receiver = spawned.take_output::<u64>("out").unwrap();
        // With no records, nothing is sent at all, not even an empty batch.
        if !records.is_empty() {
            sender.send(0, records.clone()).unwrap();
        }
        drop(sender);
        let (joined, collected) = with_timeout("pass-through-scope", move || {
            (spawned.join_blocking(), receiver.collect_data())
        });
        joined.unwrap();
        let mut out: Vec<(u64, u64)> = collected
            .into_iter()
            .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
            .collect();
        out.sort_unstable();
        assert_eq!(
            out,
            records.iter().map(|x| (0, *x)).collect::<Vec<_>>(),
            "{} records",
            records.len()
        );
        let mut inside = inside.lock().unwrap().clone();
        inside.sort();
        assert_eq!(
            inside,
            records
                .iter()
                .map(|x| (Product::new(0, 0), *x))
                .collect::<Vec<_>>(),
            "{} records",
            records.len()
        );
    }
}
