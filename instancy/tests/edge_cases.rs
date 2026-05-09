//! Integration tests for edge cases and boundary conditions.
//!
//! These tests validate correct behavior for scenarios that commonly
//! surface bugs in progress tracking and operator logic:
//! - Empty dataflows (no data sent before close)
//! - Single-record streams
//! - Multiple timestamps with mixed empty/non-empty batches
//! - Large batch sizes
//! - Rapid advance_to without data
//! - Filter that drops everything
//! - Chain of many operators (deep pipelines)

use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::{DataflowBuilder, Pipe, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Helper: create a runtime, build and spawn a dataflow, return join result.
#[allow(dead_code)]
fn run_dataflow_blocking(builder: DataflowBuilder<u64>) -> instancy::Result<()> {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let dataflow = builder.build().unwrap();
    let handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    std::thread::spawn(move || handle.join_blocking())
        .join()
        .unwrap()
}

// ===========================================================================
// Test 1: Empty dataflow — no data, immediate close
// ===========================================================================

/// A dataflow where the input is immediately closed without sending any data.
/// The frontier should advance to empty and the dataflow should complete cleanly.
#[test]
fn empty_input_completes_cleanly() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("empty-input");
    let input = builder.input::<i32>("data").unwrap();
    input.map("double", |_t, v| v * 2).output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Immediately close without sending data
    drop(sender);
    handle.join_blocking().unwrap();

    // Should produce no data
    assert_eq!(receiver.collect_data(), Vec::<(u64, Vec<i32>)>::new());
}

// ===========================================================================
// Test 2: Single record per timestamp
// ===========================================================================

/// Send exactly one record at each of several timestamps.
/// Verifies that single-record batches propagate correctly through operators.
#[test]
fn single_record_per_timestamp() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("single-record");
    let input = builder.input::<i32>("data").unwrap();
    input
        .map("increment", |_t, v| v + 1)
        .filter("positive", |_t, v| *v > 0)
        .output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    for t in 0..10u64 {
        sender.send(t, vec![t as i32]).unwrap();
        sender.advance_to(t + 1).unwrap();
    }
    drop(sender);
    handle.join_blocking().unwrap();

    let data = receiver.collect_data();
    // All values: 0+1=1, 1+1=2, ..., 9+1=10 — all > 0
    let total: i32 = data.iter().flat_map(|(_, vs)| vs.iter()).sum();
    assert_eq!(total, (1..=10).sum::<i32>());
    assert_eq!(data.iter().flat_map(|(_, vs)| vs.iter()).count(), 10);
}

// ===========================================================================
// Test 3: Mixed empty and non-empty timestamps
// ===========================================================================

/// Advance through timestamps where some have data and some are empty.
/// The frontier should still advance through empty timestamps correctly.
#[test]
fn mixed_empty_and_nonempty_timestamps() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("mixed-timestamps");
    let input = builder.input::<i32>("data").unwrap();
    let (stream, probe) = input.map("pass", |_t, v| v).probe();
    stream.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Send data only at even timestamps, skip odd ones
    for t in 0..20u64 {
        if t % 2 == 0 {
            sender.send(t, vec![t as i32]).unwrap();
        }
        sender.advance_to(t + 1).unwrap();
    }
    drop(sender);
    handle.join_blocking().unwrap();

    assert!(probe.is_done());
    let data = receiver.collect_data();
    // Should have exactly 10 batches (timestamps 0, 2, 4, ..., 18)
    assert_eq!(data.len(), 10);
    for (t, vs) in &data {
        assert_eq!(t % 2, 0, "unexpected odd timestamp {t}");
        assert_eq!(vs, &[*t as i32]);
    }
}

// ===========================================================================
// Test 4: Filter that drops everything
// ===========================================================================

/// A filter that rejects all records. The dataflow should still complete
/// and the frontier should advance to empty.
#[test]
fn filter_drops_all_records() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("filter-all");
    let input = builder.input::<i32>("data").unwrap();
    let (stream, probe) = input.filter("reject-all", |_t, _v| false).probe();
    stream.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![1, 2, 3, 4, 5]).unwrap();
    sender.send(1, vec![6, 7, 8, 9, 10]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();

    assert!(probe.is_done());
    assert_eq!(receiver.collect_data(), Vec::<(u64, Vec<i32>)>::new());
}

// ===========================================================================
// Test 5: Large batch at a single timestamp
// ===========================================================================

/// Send a large batch of records at one timestamp to verify no truncation
/// or overflow in buffering.
#[test]
fn large_batch_single_timestamp() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("large-batch");
    let input = builder.input::<i32>("data").unwrap();
    input.map("double", |_t, v| v * 2).output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    let batch: Vec<i32> = (0..10_000).collect();
    sender.send(0, batch).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();

    let data = receiver.collect_data();
    let total: i32 = data.iter().flat_map(|(_, vs)| vs.iter()).sum();
    // sum of 2*i for i in 0..10000 = 2 * (9999 * 10000 / 2) = 99990000
    assert_eq!(total, 99_990_000);
    let count: usize = data.iter().map(|(_, vs)| vs.len()).sum();
    assert_eq!(count, 10_000);
}

// ===========================================================================
// Test 6: Rapid advance_to without data (frontier-only progression)
// ===========================================================================

/// Advance through many timestamps without sending any data.
/// This tests that the progress tracker handles "phantom" time progression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_advance_no_data() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("rapid-advance");
    let input = builder.input::<i32>("data").unwrap();
    let (stream, probe) = input.map("pass", |_t, v| v).probe();
    stream.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Advance through 1000 timestamps with no data
    for t in 0..1000u64 {
        sender.advance_to(t + 1).unwrap();
    }
    drop(sender);

    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done())
        .await
        .unwrap()
        .unwrap();
    assert!(probe.is_done());
    assert!(probe.frontier().is_empty());

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || handle.join_blocking()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_eq!(receiver.collect_data(), Vec::<(u64, Vec<i32>)>::new());
}

// ===========================================================================
// Test 7: Deep operator pipeline
// ===========================================================================

/// Chain many operators to test deep pipelines don't cause issues
/// with progress propagation or stack overflow.
#[test]
fn deep_operator_pipeline() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("deep-pipeline");
    let input = builder.input::<i32>("data").unwrap();

    // Chain 50 map operators
    let mut pipe = input.map("map-0", |_t, v| v + 1);
    for i in 1..50 {
        pipe = pipe.map(format!("map-{i}"), |_t, v| v + 1);
    }
    pipe.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![0]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();

    let data = receiver.collect_data();
    // 0 + 50 increments = 50
    assert_eq!(data, vec![(0, vec![50])]);
}

// ===========================================================================
// Test 8: unary_notify with empty input (notification still fires)
// ===========================================================================

/// A unary_notify operator that requests notifications at specific times.
/// Tests that notifications fire correctly when frontier advances past
/// the requested time, even if no data arrived for that time.
#[test]
fn notify_fires_without_data_at_time() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-empty");
    let input = builder.input::<i32>("data").unwrap();

    let notifications_fired = Arc::new(Mutex::new(Vec::new()));
    let notifications_clone = Arc::clone(&notifications_fired);

    input
        .unary_notify("track-notifications", {
            move |input, output, ctx| {
                // When we receive data, request notifications at future times
                while let Some((time, data)) = input.next() {
                    output.push_vec(time, data);
                    // Request notifications at times 5, 10, 15 — no data will
                    // arrive at those times, but notifications should still fire
                    ctx.notify_at(5);
                    ctx.notify_at(10);
                    ctx.notify_at(15);
                }
                // Process notifications
                while let Some(time) = ctx.next_notification() {
                    notifications_clone.lock().unwrap().push(time);
                }
                Ok(())
            }
        })
        .output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let _receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Send one record at time 0 to trigger the notify_at requests
    sender.send(0, vec![42]).unwrap();
    // Advance past all notification times
    sender.advance_to(20).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();

    let mut fired = notifications_fired.lock().unwrap().clone();
    fired.sort();
    // All three notifications should have fired (plus possibly time 0)
    assert!(
        fired.contains(&5),
        "notification at 5 should fire, got: {fired:?}"
    );
    assert!(
        fired.contains(&10),
        "notification at 10 should fire, got: {fired:?}"
    );
    assert!(
        fired.contains(&15),
        "notification at 15 should fire, got: {fired:?}"
    );
}

// ===========================================================================
// Test 9: flat_map producing zero and multiple outputs
// ===========================================================================

/// flat_map that produces variable output per input record:
/// - negative values produce nothing
/// - zero produces one item
/// - positive values produce that many copies
#[test]
fn flat_map_variable_output() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("flat-map-variable");
    let input = builder.input::<i32>("data").unwrap();
    input
        .flat_map("expand", |_t, v| {
            if v < 0 {
                vec![]
            } else if v == 0 {
                vec![0]
            } else {
                vec![v; v as usize]
            }
        })
        .output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![-5, -1, 0, 1, 2, 3]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();

    let data = receiver.collect_data();
    let all_values: Vec<i32> = data.into_iter().flat_map(|(_, vs)| vs).collect();
    // -5 → [], -1 → [], 0 → [0], 1 → [1], 2 → [2,2], 3 → [3,3,3]
    assert_eq!(all_values, vec![0, 1, 2, 2, 3, 3, 3]);
}

// ===========================================================================
// Test 10: Multiple inputs merged via concat
// ===========================================================================

/// Test that concat correctly merges multiple input streams and the
/// output contains all records from all inputs.
#[test]
fn concat_multiple_inputs() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("concat-inputs");
    let input_a = builder.input::<i32>("a").unwrap();
    let input_b = builder.input::<i32>("b").unwrap();
    let input_c = builder.input::<i32>("c").unwrap();

    let merged = Pipe::concat(vec![
        input_a.map("tag-a", |_t, v| v * 10),
        input_b.map("tag-b", |_t, v| v * 100),
        input_c.map("tag-c", |_t, v| v * 1000),
    ])
    .unwrap();
    merged.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender_a = handle.take_input::<i32>("a").unwrap();
    let sender_b = handle.take_input::<i32>("b").unwrap();
    let sender_c = handle.take_input::<i32>("c").unwrap();

    sender_a.send(0, vec![1, 2]).unwrap();
    sender_b.send(0, vec![3]).unwrap();
    sender_c.send(0, vec![4]).unwrap();
    drop(sender_a);
    drop(sender_b);
    drop(sender_c);
    handle.join_blocking().unwrap();

    let data = receiver.collect_data();
    let mut all_values: Vec<i32> = data.into_iter().flat_map(|(_, vs)| vs).collect();
    all_values.sort();
    // a: [10, 20], b: [300], c: [4000]
    assert_eq!(all_values, vec![10, 20, 300, 4000]);
}
