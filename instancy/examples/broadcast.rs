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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let num_workers = 3;

    // Shared collector: worker_index → Vec<(time, data)>
    type WorkerData = Vec<(u64, Vec<i32>)>;
    let collected: Arc<Mutex<BTreeMap<usize, WorkerData>>> =
        Arc::new(Mutex::new(BTreeMap::new()));

    println!("=== broadcast to {num_workers} workers ===\n");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let collected_clone = collected.clone();
    let mut multi = rt
        .spawn_multi(
            "broadcast_demo",
            num_workers,
            move |worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                let wid = worker_idx;
                let coll = collected_clone.clone();
                input
                    .broadcast("replicate")
                    .for_each_batch("collect", move |time, batch| {
                        let mut map = coll.lock().unwrap();
                        map.entry(wid).or_default().push((*time, batch.to_vec()));
                    });
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

    multi.join_blocking().expect("execution failed");

    // Show results: each worker received all data
    let map = collected.lock().unwrap();
    println!("Data received by each worker:\n");
    for (worker, batches) in map.iter() {
        println!("  Worker {worker}:");
        for (time, batch) in batches {
            println!("    t={time}: {batch:?}");
        }
    }
    println!(
        "\nAll {num_workers} workers see all items — broadcast replicates data to every worker."
    );
}
