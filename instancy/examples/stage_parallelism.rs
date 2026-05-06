//! # Per-Stage Parallelism
//!
//! Demonstrates how instancy automatically infers pipeline stages and validates
//! per-stage parallelism parameters at spawn time.
//!
//! ## Concepts
//!
//! A dataflow graph is divided into **stages** separated by exchange/repartition
//! operators. Operators connected only by pipeline (in-memory) edges belong to
//! the same stage and are fused into a single `FusedStageTask` for efficient
//! scheduling.
//!
//! ```text
//! ┌─────────────────── Stage 0 ───────────────────┐   ┌──── Stage 1 ────┐
//! │  source → map("double") → flat_map("expand")  │──►│  unary("count") │
//! └───────────────────────────────────────────────-┘   └─────────────────┘
//!                                                  ▲
//!                                       exchange_by_hash_to(par=2)
//! ```
//!
//! When you call `.exchange_by_hash_to("name", parallelism, hash_fn)`, the
//! `parallelism` parameter declares the downstream stage's worker count. The
//! runtime validates that this matches the actual worker count at spawn time.
//!
//! ## What This Example Shows
//!
//! 1. **Multi-stage pipeline**: source → map → exchange → count → output
//! 2. **Stage inference**: The builder automatically detects the exchange boundary
//! 3. **Parallelism validation**: parallelism=2 with spawn_multi(..., 2, ...) passes
//! 4. **Mismatch detection**: parallelism=4 with spawn_multi(..., 2, ...) fails
//!
//! ## Future: Heterogeneous Worker Counts
//!
//! Today, all stages must have the same worker count. In the future, per-stage
//! executors will allow stage 0 to have M workers and stage 1 to have N workers
//! (M≠N), with M×N exchange channels routing data between them.
//!
//! Run with: `cargo run --example stage_parallelism`

use std::collections::HashMap;

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    println!("=== Per-Stage Parallelism Example ===\n");

    let num_workers = 2;

    // --- Create runtime with 2 physical threads ---
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("runtime creation failed");

    // ─────────────────────────────────────────────────────────────────────
    // Example 1: Valid pipeline — parallelism matches worker count
    // ─────────────────────────────────────────────────────────────────────
    println!("── Example 1: Valid multi-stage pipeline (par=2, workers=2) ──\n");

    let mut multi = rt
        .spawn_multi(
            "stage-par-demo",
            num_workers,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("numbers");

                // Stage 0: transform pipeline (all fused into one task)
                input
                    .map("double", |_t, x| x * 2)
                    .flat_map("expand", |_t, x| vec![x, x + 1])
                    // Exchange creates a stage boundary. parallelism=2 means the
                    // downstream stage expects 2 workers (matches our spawn_multi).
                    .exchange_by_hash_to("partition", num_workers, |x: &i32| *x as u64)
                    // Stage 1: aggregate after exchange
                    .unary("sum_by_parity", {
                        let mut sums: HashMap<u64, HashMap<String, i64>> = HashMap::new();
                        move |input, output| {
                            while let Some((time, items)) = input.next() {
                                let entry = sums.entry(time).or_default();
                                for val in items {
                                    let key = if val % 2 == 0 {
                                        "even".to_string()
                                    } else {
                                        "odd".to_string()
                                    };
                                    *entry.entry(key).or_insert(0) += val as i64;
                                }
                            }
                            // Emit accumulated sums.
                            for (time, map) in sums.drain() {
                                let pairs: Vec<(String, i64)> = map.into_iter().collect();
                                output.push_vec(time, pairs);
                            }
                            Ok(())
                        }
                    })
                    .output("results");

                Ok(())
            },
            SpawnOptions::default(),
        )
        .expect("spawn_multi should succeed with matching parallelism");

    println!("  ✓ Dataflow spawned successfully (2 stages × 2 workers)\n");

    // Feed data and collect results.
    let senders = multi.take_all_inputs::<i32>("numbers").unwrap();
    // Worker 0 gets [1, 2, 3], Worker 1 gets [4, 5]
    senders[0].send(0u64, vec![1, 2, 3]).unwrap();
    senders[1].send(0u64, vec![4, 5]).unwrap();
    drop(senders);

    let receivers = multi.take_all_outputs::<(String, i64)>("results").unwrap();
    multi.join_blocking().expect("dataflow completed");

    // Merge results from all workers.
    let mut totals: HashMap<String, i64> = HashMap::new();
    for receiver in receivers {
        for (_time, batch) in receiver.collect_data() {
            for (key, val) in batch {
                *totals.entry(key).or_insert(0) += val;
            }
        }
    }

    println!("  Results (sum of doubled+expanded values by parity):");
    let mut sorted: Vec<_> = totals.iter().collect();
    sorted.sort_by_key(|(k, _)| (*k).clone());
    for (key, val) in &sorted {
        println!("    {key}: {val}");
    }
    println!();

    // ─────────────────────────────────────────────────────────────────────
    // Example 2: Parallelism mismatch — caught at spawn time
    // ─────────────────────────────────────────────────────────────────────
    println!("── Example 2: Parallelism mismatch detection (par=4, workers=2) ──\n");

    let result = rt.spawn_multi(
        "stage-par-mismatch",
        num_workers,
        |_worker_idx, builder: &mut DataflowBuilder<u64>| {
            let input = builder.input::<i32>("data");
            input
                .map("inc", |_t, x| x + 1)
                // Mismatch! Declaring parallelism=4 but only 2 workers exist.
                .exchange_by_hash_to("repartition", 4, |x: &i32| *x as u64)
                .map("noop", |_t, x| x)
                .output("out");
            Ok(())
        },
        SpawnOptions::default(),
    );

    match result {
        Err(e) => {
            println!("  ✓ Correctly rejected: {e}\n");
        }
        Ok(_) => {
            println!("  ✗ Should have been rejected!");
            std::process::exit(1);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Example 3: No explicit parallelism — single stage, no validation needed
    // ─────────────────────────────────────────────────────────────────────
    println!("── Example 3: Single-stage pipeline (no exchange, no validation) ──\n");

    let mut simple = rt
        .spawn_multi(
            "single-stage",
            num_workers,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("vals");
                // All operators stay in the same stage (pipeline edges only).
                input
                    .map("triple", |_t, x| x * 3)
                    .map("add_one", |_t, x| x + 1)
                    .output("out");
                Ok(())
            },
            SpawnOptions::default(),
        )
        .expect("single-stage should always succeed");

    println!("  ✓ Single-stage dataflow spawned (no exchange = no parallelism constraint)\n");

    let senders = simple.take_all_inputs::<i32>("vals").unwrap();
    senders[0].send(0u64, vec![10]).unwrap();
    senders[1].send(0u64, vec![20]).unwrap();
    drop(senders);

    let receivers = simple.take_all_outputs::<i32>("out").unwrap();
    simple.join_blocking().expect("dataflow completed");

    for (i, receiver) in receivers.into_iter().enumerate() {
        let data: Vec<_> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, v)| v)
            .collect();
        println!("  Worker {i} output: {data:?}");
    }
    println!();

    rt.shutdown();
    println!("Done! All examples completed successfully.");
}
