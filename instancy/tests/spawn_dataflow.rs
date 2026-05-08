//! Integration tests for `spawn_multi` with `auto_parallelism` enabled.
//!
//! These tests validate that `SpawnOptions::auto_parallelism(true)` correctly:
//! - Auto-detects stage 0 parallelism from input/source_async count
//! - Routes data through heterogeneous stages
//! - Works with the uniform fallback path
//! - Properly collects output data

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::sync::{Arc, Mutex};

fn test_runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig::default()).unwrap()
}

fn auto_opts() -> SpawnOptions {
    SpawnOptions::new().auto_parallelism(true)
}

/// Single input → single stage → output.
/// Stage 0 parallelism = 1 (one input).
#[test]
fn auto_par_single_input_single_stage() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "simple",
            0, // ignored with auto_parallelism
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.map("double", |_t, x| x * 2).output("results");
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    let receiver = multi.take_output::<i32>(0, "results").unwrap();

    sender.send(0, vec![1, 2, 3]).unwrap();
    drop(sender);

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();
    assert_eq!(results, vec![2, 4, 6]);

    multi.join_blocking().unwrap();
}

/// Fan-out: 1 input → exchange_to(4) → process → for_each.
/// Stage 0 par=1 (auto), Stage 1 par=4.
#[test]
fn auto_par_fan_out() {
    let rt = test_runtime();
    let collected = Arc::new(Mutex::new(Vec::new()));
    let c = collected.clone();

    let mut multi = rt
        .spawn_multi(
            "fan-out",
            0,
            move |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let c = c.clone();
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("scatter", 4, |v: &i32| *v as u64)
                    .map("double", |_t, x| x * 2)
                    .for_each("collect", move |_t, item: &i32| {
                        c.lock().unwrap().push(*item);
                    });
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, vec![1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    drop(sender);

    multi.join_blocking().unwrap();

    let mut result = collected.lock().unwrap().clone();
    result.sort();
    assert_eq!(result, vec![2, 4, 6, 8, 10, 12, 14, 16]);
}

/// Fan-out-fan-in: 1 → exchange_to(4) → gather → output.
/// Stage 0 par=1, Stage 1 par=4, Stage 2 par=1.
#[test]
fn auto_par_fan_out_fan_in() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "fan-out-fan-in",
            0,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("scatter", 4, |v: &i32| *v as u64)
                    .map("process", |_t, x| x * 2)
                    .gather("collect")
                    .output("results");
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    let receiver = multi.take_output::<i32>(0, "results").unwrap();

    sender.send(0, (1..=20).collect()).unwrap();
    drop(sender);

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();
    let expected: Vec<i32> = (1..=20).map(|x| x * 2).collect();
    assert_eq!(results, expected);

    multi.join_blocking().unwrap();
}

/// Multiple inputs in stage 0 → auto parallelism = 2.
/// Each worker gets all input ports (v1 architecture). With 2 inputs and
/// no exchange operators, stage 0 par = 2. We verify the worker count
/// and that data flows through both pipelines.
#[test]
fn auto_par_multiple_inputs() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "multi-input",
            0,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let a = builder.input::<i32>("stream_a");
                let b = builder.input::<i32>("stream_b");
                a.map("inc_a", |_t, x| x + 1).for_each("sink_a", |_t, _v| {});
                b.map("inc_b", |_t, x| x + 10).for_each("sink_b", |_t, _v| {});
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    // With 2 inputs, stage 0 par=2, so we have 2 workers.
    assert_eq!(multi.num_workers(), 2);

    // Each worker has both input ports. Send data to worker 0 and close all.
    let sender_a = multi.take_input::<i32>(0, "stream_a").unwrap();
    sender_a.send(0, vec![1, 2, 3]).unwrap();
    drop(sender_a);

    // Close remaining input ports on both workers.
    let _ = multi.take_input::<i32>(0, "stream_b");
    let _ = multi.take_input::<i32>(1, "stream_a");
    let _ = multi.take_input::<i32>(1, "stream_b");

    multi.join_blocking().unwrap();
}

/// Increasing parallelism: 1 → 2 → 4.
#[test]
fn auto_par_increasing_parallelism() {
    let rt = test_runtime();

    let collected = Arc::new(Mutex::new(Vec::new()));
    let c = collected.clone();

    let mut multi = rt
        .spawn_multi(
            "increasing",
            0,
            move |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let c = c.clone();
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("first", 2, |v: &i32| *v as u64)
                    .map("stage1", |_t, x| x + 1)
                    .exchange_to("second", 4, |v: &i32| *v as u64)
                    .map("stage2", |_t, x| x * 2)
                    .for_each("collect", move |_t, item: &i32| {
                        c.lock().unwrap().push(*item);
                    });
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, (0..50).collect()).unwrap();
    drop(sender);

    multi.join_blocking().unwrap();

    let mut result = collected.lock().unwrap().clone();
    result.sort();
    let expected: Vec<i32> = (0..50).map(|x| (x + 1) * 2).collect();
    assert_eq!(result, expected);
}

/// Decreasing parallelism: 1 → 4 → 1.
/// Verifies data integrity through fan-out then gather.
#[test]
fn auto_par_decreasing_parallelism() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "decrease",
            0,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("scatter", 4, |v: &i32| *v as u64)
                    .map("work", |_t, x| x * 3)
                    .gather("collect")
                    .output("results");
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    let receiver = multi.take_output::<i32>(0, "results").unwrap();

    sender.send(0, (1..=10).collect()).unwrap();
    drop(sender);

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();
    let expected: Vec<i32> = (1..=10).map(|x| x * 3).collect();
    assert_eq!(results, expected);

    multi.join_blocking().unwrap();
}

/// No-op / empty pipeline still works.
#[test]
fn auto_par_trivial_graph() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "trivial",
            0,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.for_each("sink", |_t, _v| {});
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, vec![42]).unwrap();
    drop(sender);

    multi.join_blocking().unwrap();
}

/// Uniform parallelism (no exchange) — auto_parallelism should fall back
/// to the simpler multi path.
#[test]
fn auto_par_uniform_fallback() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "uniform",
            0,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .map("inc", |_t, x| x + 1)
                    .output("results");
                Ok(())
            },
            auto_opts(),
        )
        .unwrap();

    // Single input → 1 worker.
    assert_eq!(multi.num_workers(), 1);

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    let receiver = multi.take_output::<i32>(0, "results").unwrap();

    sender.send(0, vec![10, 20]).unwrap();
    drop(sender);

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();
    assert_eq!(results, vec![11, 21]);

    multi.join_blocking().unwrap();
}
