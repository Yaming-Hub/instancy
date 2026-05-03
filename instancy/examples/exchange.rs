//! # Exchange Example
//!
//! Demonstrates hash-based data repartitioning across workers using
//! the `exchange_by_hash` operator. Data is distributed to workers based
//! on a hash of each value, then each worker counts its received elements.
//!
//! This is the instancy equivalent of timely-dataflow's `exchange.rs` example.
//! The original is a throughput benchmark; this version focuses on
//! demonstrating the exchange API and verifying correct routing.
//!
//! ```bash
//! cargo run --example exchange
//! ```

use std::collections::HashMap;

use instancy::runtime::{RuntimeConfig, RuntimeHandle};

fn main() {
    let num_workers = 4;
    let num_elements = 100u64;
    let num_epochs = 5u64;

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();

    let mut spawned = rt
        .spawn_multi("exchange_demo", num_workers, |_worker_idx, builder| {
            let input = builder.input::<u64>("data");

            // Exchange by value — routes each element to worker (value % num_workers).
            let exchanged = input.exchange_by_hash("by_val", |x: &u64| *x);

            // Pass through to output for verification.
            exchanged
                .map("pass", |_t, x| x)
                .output("results");

            Ok(())
        })
        .unwrap();

    // Take outputs from all workers.
    let mut outputs = Vec::new();
    for w in 0..num_workers {
        outputs.push(spawned.take_output::<u64>(w, "results").unwrap());
    }

    // Send data through worker 0 only; close other inputs.
    let sender = spawned.take_input::<u64>(0, "data").unwrap();
    for w in 1..num_workers {
        drop(spawned.take_input::<u64>(w, "data").unwrap());
    }

    // Feed data epoch by epoch.
    for epoch in 0..num_epochs {
        let data: Vec<u64> = (0..num_elements).collect();
        sender.send(epoch, data).unwrap();
    }
    drop(sender);

    spawned.join_blocking().unwrap();

    // Verify routing: each worker should receive exactly the values
    // where (value % num_workers) == worker_index.
    println!("=== Exchange: {num_elements} elements × {num_epochs} epochs across {num_workers} workers ===");
    println!();

    let mut total = 0usize;
    for (worker_idx, output) in outputs.into_iter().enumerate() {
        let data = output.collect_data();

        // Group by epoch for display.
        let mut by_epoch: HashMap<u64, Vec<u64>> = HashMap::new();
        for (t, batch) in &data {
            by_epoch.entry(*t).or_default().extend(batch.iter().copied());
        }

        let count: usize = by_epoch.values().map(|v| v.len()).sum();
        total += count;

        // Verify each value routes to this worker.
        for (epoch, values) in &by_epoch {
            for &val in values {
                assert_eq!(
                    (val % num_workers as u64) as usize,
                    worker_idx,
                    "value {val} at epoch {epoch} routed to wrong worker {worker_idx}"
                );
            }
        }

        let expected_per_epoch = (0..num_elements)
            .filter(|v| (*v % num_workers as u64) as usize == worker_idx)
            .count();

        println!(
            "  Worker {worker_idx}: {count} records ({expected_per_epoch}/epoch × {num_epochs} epochs)"
        );
    }

    assert_eq!(
        total,
        (num_elements as usize) * (num_epochs as usize),
        "total record count mismatch"
    );

    println!();
    println!("  Total: {total} records (no data lost)");
    println!("✓ All assertions passed!");
}
