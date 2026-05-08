//! Integration tests for per-stage parallelism via `SpawnOptions::per_stage_parallelism`.
//!
//! Tests that dataflows with different parallelism at each stage execute
//! correctly with asymmetric exchange channels routing data between stages.

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::{RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Helper: create a runtime for testing.
fn test_runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig::default()).unwrap()
}

/// Spawn options with per-stage parallelism enabled.
fn staged_opts() -> SpawnOptions {
    SpawnOptions::new().per_stage_parallelism(true)
}

/// Basic fan-out: 1 source worker → exchange_to(4) → 4 parallel workers.
///
/// Validates that data flows correctly from a single-worker stage to a
/// multi-worker stage via asymmetric exchange.
#[test]
fn staged_fan_out() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "fan-out",
            1, // default parallelism = 1
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                // Stage 0 (par=1): source → exchange_to(4) boundary
                // Stage 1 (par=4): process in parallel
                input
                    .exchange_to("scatter", 4, |v: &i32| *v as u64)
                    .map("double", |_t, x| x * 2)
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
            staged_opts(),
        )
        .unwrap();

    // Send data through the single-worker input.
    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, vec![1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    drop(sender);

    multi.join_blocking().unwrap();
}

/// Fan-in: 4 parallel workers → gather() → 1 worker.
///
/// Validates that data converges correctly from a multi-worker stage
/// to a single-worker stage.
#[test]
fn staged_fan_in() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "fan-in",
            4, // default parallelism = 4
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                // Stage 0 (par=4): parallel processing
                // → gather() → Stage 1 (par=1): single aggregator
                input
                    .map("triple", |_t, x| x * 3)
                    .gather("collect")
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
            staged_opts(),
        )
        .unwrap();

    // Feed data to worker 0 only (for simplicity).
    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, vec![10, 20, 30]).unwrap();
    drop(sender);

    // Close other workers' inputs too.
    for i in 1..4 {
        if let Ok(s) = multi.take_input::<i32>(i, "data") {
            drop(s);
        }
    }

    multi.join_blocking().unwrap();
}

/// Fan-out-fan-in pipeline: 1 → 4 → 1.
///
/// Tests the full pattern: single source fans out to 4 parallel workers,
/// which then fan back into a single aggregator.
#[test]
fn staged_fan_out_fan_in() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "fan-out-fan-in",
            1, // default = 1 worker
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("scatter", 4, |v: &i32| *v as u64)
                    .map("process", |_t, x| x * 2)
                    .gather("collect")
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
            staged_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender
        .send(0, (0..100).collect::<Vec<i32>>())
        .unwrap();
    drop(sender);

    multi.join_blocking().unwrap();
}

/// When all stages have the same parallelism, spawn_multi with per_stage_parallelism
/// should behave identically to spawn_multi without it.
#[test]
fn staged_uniform_parallelism_fallback() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "uniform",
            2, // all stages default to 2
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .exchange("repartition", |v: &i32| *v as u64)
                    .map("inc", |_t, x| x + 1)
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
            staged_opts(),
        )
        .unwrap();

    // Both workers get input.
    for i in 0..2 {
        let sender = multi.take_input::<i32>(i, "data").unwrap();
        sender.send(0, vec![i as i32 * 10]).unwrap();
        drop(sender);
    }

    multi.join_blocking().unwrap();
}

/// Multi-stage pipeline with increasing parallelism: 1 → 2 → 4.
#[test]
fn staged_increasing_parallelism() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "increasing",
            1, // stage 0: 1 worker
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_to("first-expand", 2, |v: &i32| *v as u64)
                    .map("stage1-work", |_t, x| x + 1)
                    .exchange_to("second-expand", 4, |v: &i32| *v as u64)
                    .map("stage2-work", |_t, x| x * 2)
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
            staged_opts(),
        )
        .unwrap();

    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, (0..50).collect()).unwrap();
    drop(sender);

    multi.join_blocking().unwrap();
}

/// Validates that spawn_multi with per_stage_parallelism rejects num_workers of 0.
#[test]
fn staged_zero_parallelism_rejected() {
    let rt = test_runtime();
    let result = rt.spawn_multi(
        "zero",
        0,
        |_worker_idx, builder: &mut DataflowBuilder<u64>| {
            builder.input::<i32>("data").for_each("sink", |_t, _v| {});
            Ok(())
        },
        staged_opts(),
    );
    assert!(result.is_err());
}
