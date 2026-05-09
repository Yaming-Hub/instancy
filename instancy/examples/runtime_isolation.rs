//! # Runtime Isolation Example
//!
//! Demonstrates creating multiple isolated `RuntimeHandle` instances that
//! coexist in the same process, each running independent dataflows on a
//! shared worker pool.
//!
//! ```bash
//! cargo run --example runtime_isolation
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

#[allow(clippy::needless_return)]
fn main() {
    // Create two independent runtimes with different configurations.
    // Each runtime has its own worker pool, scheduling policy, and
    // cancellation scope — fully isolated from each other.
    let rt_fast = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        schedule_policy: None,
        name: "fast-pipeline".to_string(),
        ..Default::default()
    })
    .expect("failed to create fast runtime");

    let rt_batch = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "batch-pipeline".to_string(),
        ..Default::default()
    })
    .expect("failed to create batch runtime");

    println!("Created runtime '{}' (4 threads)", rt_fast.name());
    println!("Created runtime '{}' (2 threads)", rt_batch.name());

    // --- Run dataflows on each runtime ---

    // Fast runtime: spawn a pipeline with external input
    let builder = DataflowBuilder::<u64>::new("fast-double");
    let input = builder.input::<i32>("numbers").unwrap();
    input.map("double", |_t, x| x * 2).output("results").unwrap();
    let dataflow = builder.build().expect("build failed");

    let mut fast_handle = rt_fast
        .spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed");
    let sender = fast_handle.take_input::<i32>("numbers").expect("input");
    sender.send(0, vec![1, 2, 3, 4, 5]).unwrap();
    sender.send(1, vec![10, 20]).unwrap();
    sender.close();

    let receiver = fast_handle.take_output::<i32>("results").expect("output");
    let results = receiver.collect_data();
    println!("\nFast runtime results:");
    for (time, data) in &results {
        println!("  t={time}: {data:?}");
    }
    fast_handle.join_blocking().expect("fast dataflow");

    // Batch runtime: run a static source pipeline (no external input)
    let builder = DataflowBuilder::<u64>::new("batch-squares");
    let out = builder
        .source("data", vec![(0u64, vec![1i32, 2, 3]), (1, vec![4, 5, 6])])
        .map("square", |_t, x| x * x)
        .output("results").unwrap();
    let dataflow = builder.build().expect("build failed");

    rt_batch
        .spawn(dataflow, SpawnOptions::default())
        .expect("batch dataflow spawn")
        .join_blocking()
        .expect("batch dataflow");
    let collector = out.collector();
    let batch_results = collector.lock().unwrap();
    println!("\nBatch runtime results:");
    for (time, data) in batch_results.iter() {
        println!("  t={time}: {data:?}");
    }

    // --- Demonstrate isolation ---
    // Shutting down one runtime doesn't affect the other.
    rt_fast.shutdown().expect("shutdown fast runtime");
    println!("\nShut down '{}':", rt_fast.name());
    println!(
        "  {} is_shutdown: {}",
        rt_fast.name(),
        rt_fast.is_shutdown()
    );
    println!(
        "  {} is_shutdown: {}",
        rt_batch.name(),
        rt_batch.is_shutdown()
    );

    // The batch runtime can still run more dataflows.
    assert!(!rt_batch.is_shutdown());

    let builder = DataflowBuilder::<u64>::new("batch-extra");
    builder
        .source("src", vec![(0u64, vec![100i32])])
        .output("out").unwrap();
    let dataflow = builder.build().expect("build failed");
    rt_batch
        .spawn(dataflow, SpawnOptions::default())
        .expect("extra dataflow spawn")
        .join_blocking()
        .expect("extra dataflow");
    println!("\nBatch runtime ran another dataflow after fast runtime shutdown.");

    println!("\nIndependent runtimes verified: cancelling one leaves others unaffected.");
}
