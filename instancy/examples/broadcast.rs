//! # Broadcast Operator
//!
//! Demonstrates the `broadcast` operator that clones every item to
//! all workers. In single-worker mode broadcast is a no-op pass-through;
//! with multiple workers every worker receives a copy of every item.
//!
//! This example runs with 3 workers to show the replication effect.
//!
//! ```bash
//! cargo run --example broadcast
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let num_workers = 3;

    println!("=== broadcast to {num_workers} workers ===\n");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let mut multi = rt
        .spawn_multi(
            "broadcast_demo",
            num_workers,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<i32>("data").unwrap()
                    .broadcast("replicate")
                    .output("results").unwrap();
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();

    // Send data only from worker 0's input
    let sender = multi.take_input::<i32>(0, "data").unwrap();
    sender.send(0, vec![100, 200, 300]).unwrap();
    sender.send(1, vec![400]).unwrap();
    drop(sender);

    // Close other workers' inputs (they have no external data)
    for i in 1..num_workers {
        drop(multi.take_input::<i32>(i, "data").unwrap());
    }

    let mut receivers = Vec::new();
    for worker in 0..num_workers {
        receivers.push((worker, multi.take_output::<i32>(worker, "results").unwrap()));
    }

    multi.join_blocking().expect("execution failed");

    // Show results: each worker received all data
    println!("Data received by each worker:\n");
    for (worker, receiver) in receivers {
        println!("  Worker {worker}:");
        for (time, batch) in receiver.collect_data() {
            println!("    t={time}: {batch:?}");
        }
    }
    println!(
        "\nAll {num_workers} workers see all items — broadcast replicates data to every worker."
    );
}
