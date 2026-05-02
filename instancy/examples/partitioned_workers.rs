//! Example: Partitioned input with multiple logical workers.
//!
//! Demonstrates `spawn_multi()` for processing physically partitioned data.
//! Each data partition maps to one logical worker, and the hosting application
//! feeds each partition's data into its corresponding worker's input stream.
//!
//! This mirrors a real-world scenario where data is physically distributed
//! across storage partitions or nodes. The hosting application knows which
//! partition lives where and routes data accordingly.
//!
//! ```text
//! Partition 0: [1, 2, 3]       ──► Worker 0 ──map("double")──► [2, 4, 6]
//! Partition 1: [10, 20]         ──► Worker 1 ──map("double")──► [20, 40]
//! Partition 2: [100]            ──► Worker 2 ──map("double")──► [200]
//! Partition 3: [1000, 2000]     ──► Worker 3 ──map("double")──► [2000, 4000]
//! ```
//!
//! All 4 logical workers share 2 physical threads in the runtime pool.
//! The `num_workers` parameter controls logical parallelism; the runtime's
//! `worker_threads` controls physical parallelism. It is valid (and common)
//! to have more logical workers than threads — executors are scheduled
//! cooperatively on the pool.
//!
//! Run with: `cargo run --example partitioned_workers`

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::{RuntimeConfig, RuntimeHandle};

fn main() {
    println!("=== Partitioned Workers Example ===\n");

    // --- Simulated partitioned dataset ---
    // In practice, each partition might be a file shard, a database
    // partition, or data co-located with a specific node.
    let partitions: Vec<Vec<i32>> = vec![
        vec![1, 2, 3],
        vec![10, 20],
        vec![100],
        vec![1000, 2000],
    ];
    let num_workers = partitions.len();
    println!("Dataset has {num_workers} partitions:");
    for (i, p) in partitions.iter().enumerate() {
        println!("  partition {i}: {p:?}");
    }
    println!();

    // --- Create runtime with fewer threads than workers ---
    // 2 physical threads service 4 logical workers cooperatively.
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("runtime creation failed");
    println!("Runtime created with 2 worker threads for {num_workers} logical workers\n");

    // --- Spawn replicated workers ---
    // The build closure is called once per worker. Every worker gets an
    // identical graph topology. The worker_idx is available for logging
    // or per-worker configuration, but the graph structure must match.
    let mut multi = rt
        .spawn_multi(
            "partitioned-double",
            num_workers,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.map("double", |_t, x| x * 2).output("results");
                Ok(())
            },
        )
        .expect("spawn_multi failed");
    println!("Spawned {num_workers} workers\n");

    // --- Wire each partition to its worker's input stream ---
    // Use take_all_inputs / take_all_outputs for batch convenience.
    // These are all-or-nothing: if any worker is missing the port, all
    // fail without partial consumption.
    let senders = multi.take_all_inputs::<i32>("data").unwrap();
    let receivers = multi.take_all_outputs::<i32>("results").unwrap();

    println!("Feeding partitioned data:");
    for (i, partition) in partitions.into_iter().enumerate() {
        println!("  partition {i} -> worker {i}: {partition:?}");
        senders[i].send(0, partition).unwrap();
    }
    // Close all inputs to signal end-of-data.
    drop(senders);
    println!();

    // --- Collect results from each worker independently ---
    println!("Results:");
    for (i, receiver) in receivers.into_iter().enumerate() {
        let data: Vec<i32> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        println!("  worker {i}: {data:?}");
    }
    println!();

    // --- Wait for all workers to complete ---
    multi.join_blocking().expect("all workers completed");
    println!("All {num_workers} workers finished successfully!");
}
