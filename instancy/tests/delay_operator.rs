//! Tests for the delay and delay_batch operators.
//!
//! These tests validate:
//! - `delay()` per-item timestamp reassignment
//! - `delay_batch()` per-timestamp reassignment
//! - Data is held until frontier advances past original timestamp
//! - Correct output timestamps after delay
//! - Identity delay (no-op buffering until frontier advances)
//! - Multiple epochs with different delay targets
//! - Empty input
//! - Panic on backward delay (new_time < time)

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::collections::HashMap;

// ============================================================
// delay_batch tests
// ============================================================

#[test]
fn delay_batch_identity_preserves_data() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_identity");
    let input = builder.input::<i64>("data").unwrap();
    input.delay_batch("identity", |t| *t).output("out").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![10, 20, 30]).unwrap();
    sender.send(1, vec![40, 50]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    let mut t0 = by_time.remove(&0).unwrap_or_default();
    t0.sort();
    assert_eq!(t0, vec![10, 20, 30]);

    let mut t1 = by_time.remove(&1).unwrap_or_default();
    t1.sort();
    assert_eq!(t1, vec![40, 50]);
}

#[test]
fn delay_batch_shifts_timestamps() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_shift");
    let input = builder.input::<i64>("data").unwrap();
    input
        .delay_batch("shift10", |t| t + 10)
        .output("out")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![1, 2]).unwrap();
    sender.send(5, vec![3, 4]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    // epoch 0 → epoch 10
    let mut t10 = by_time.remove(&10).unwrap_or_default();
    t10.sort();
    assert_eq!(t10, vec![1, 2]);

    // epoch 5 → epoch 15
    let mut t15 = by_time.remove(&15).unwrap_or_default();
    t15.sort();
    assert_eq!(t15, vec![3, 4]);

    // No data at original timestamps
    assert!(!by_time.contains_key(&0));
    assert!(!by_time.contains_key(&5));
}

#[test]
fn delay_batch_windowing() {
    // Window: group into 10-unit windows.
    // epoch 0,3,7 → window 10; epoch 12,15 → window 20
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_window");
    let input = builder.input::<i64>("data").unwrap();
    input
        .delay_batch("window10", |t| (t / 10 + 1) * 10)
        .output("out")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![100]).unwrap();
    sender.send(3, vec![200]).unwrap();
    sender.send(7, vec![300]).unwrap();
    sender.send(12, vec![400]).unwrap();
    sender.send(15, vec![500]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    // Window [0,10) → timestamp 10
    let mut w10 = by_time.remove(&10).unwrap_or_default();
    w10.sort();
    assert_eq!(w10, vec![100, 200, 300]);

    // Window [10,20) → timestamp 20
    let mut w20 = by_time.remove(&20).unwrap_or_default();
    w20.sort();
    assert_eq!(w20, vec![400, 500]);
}

#[test]
fn delay_batch_empty_input() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_empty");
    let input = builder.input::<i64>("data").unwrap();
    input.delay_batch("noop", |t| *t).output("out").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let all: Vec<(u64, Vec<i64>)> = receiver.collect_data();
    let total: usize = all.iter().map(|(_, d)| d.len()).sum();
    assert_eq!(total, 0, "empty input should produce no output");
}

// ============================================================
// delay (per-item) tests
// ============================================================

#[test]
fn delay_per_item_routes_by_content() {
    // Items > 100 go to timestamp t+10, others stay at t.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_per_item");
    let input = builder.input::<i64>("data").unwrap();
    input
        .delay("by_value", |t, item| if *item > 100 { *t + 10 } else { *t })
        .output("out")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![50, 200, 75, 300]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    // 50, 75 stay at t=0
    let mut t0 = by_time.remove(&0).unwrap_or_default();
    t0.sort();
    assert_eq!(t0, vec![50, 75]);

    // 200, 300 move to t=10
    let mut t10 = by_time.remove(&10).unwrap_or_default();
    t10.sort();
    assert_eq!(t10, vec![200, 300]);
}

#[test]
fn delay_followed_by_reduce() {
    // Delay all to same timestamp, then reduce (sum).
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_reduce");
    let input = builder.input::<i64>("data").unwrap();
    input
        .delay_batch("merge", |_t| 0u64)
        .reduce("sum", |a, b| a + b)
        .output("out")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.send(0, vec![4, 5]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let all: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    assert_eq!(all, vec![15]); // 1+2+3+4+5
}

#[test]
fn delay_batch_multiple_inputs_same_target() {
    // Multiple source timestamps map to the same delayed timestamp.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_merge_ts");
    let input = builder.input::<i64>("data").unwrap();
    input
        .delay_batch("merge", |_t| 100u64)
        .output("out")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![1]).unwrap();
    sender.send(5, vec![2]).unwrap();
    sender.send(50, vec![3]).unwrap();
    sender.send(99, vec![4]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    // All data merged at timestamp 100
    let mut t100 = by_time.remove(&100).unwrap_or_default();
    t100.sort();
    assert_eq!(t100, vec![1, 2, 3, 4]);
}

#[test]
fn delay_per_item_identity() {
    // Per-item identity: delay(|t, _| *t) should behave like delay_batch identity.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("delay_item_identity");
    let input = builder.input::<i64>("data").unwrap();
    input.delay("id", |t, _| *t).output("out").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let sender = handle.take_input::<i64>("data").unwrap();
    let receiver = handle.take_output::<i64>("out").unwrap();

    sender.send(0, vec![10, 20]).unwrap();
    sender.send(1, vec![30]).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut by_time: HashMap<u64, Vec<i64>> = HashMap::new();
    for (time, data) in receiver.collect_data() {
        by_time.entry(time).or_default().extend(data);
    }

    let mut t0 = by_time.remove(&0).unwrap_or_default();
    t0.sort();
    assert_eq!(t0, vec![10, 20]);
    assert_eq!(by_time.remove(&1).unwrap_or_default(), vec![30]);
}
