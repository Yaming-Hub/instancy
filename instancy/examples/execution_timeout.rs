//! Demonstrates the execution timeout feature.
//!
//! Shows how `SpawnOptions::timeout()` automatically cancels a dataflow
//! that exceeds its time budget.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p instancy --example execution_timeout
//! ```

use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

#[tokio::main]
async fn main() {
    println!("=== Execution Timeout Examples ===\n");

    fast_completion().await;
    timeout_fires().await;
    timeout_with_drain().await;

    println!("\nAll examples completed.");
}

/// Dataflow completes well before its timeout.
async fn fast_completion() {
    println!("--- 1. Fast completion (timeout not reached) ---");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("fast");
    let input = builder.input::<i32>("data");
    input.map("double", |_t, x| x * 2).output("results");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_secs(5));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let sender = handle.take_input::<i32>("data").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.close();

    let start = Instant::now();
    let result = handle.join().await;
    println!(
        "  Result: {} (elapsed: {:?})",
        if result.is_ok() { "Ok" } else { "Err" },
        start.elapsed()
    );
}

/// Dataflow exceeds its timeout and gets cancelled.
async fn timeout_fires() {
    println!("--- 2. Timeout fires (long-running dataflow) ---");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("slow");
    let input = builder.input::<i32>("data");
    input.output("out");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_millis(300));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let cancel = handle.cancel_token().clone();

    // Keep input open — dataflow waits forever until timeout.
    let _sender = handle.take_input::<i32>("data").unwrap();

    let start = Instant::now();
    let result = handle.join().await;
    let reason = cancel.reason();
    println!(
        "  Result: Err (reason: {}) (elapsed: {:?})",
        reason.map_or("none".to_string(), |r| r.to_string()),
        start.elapsed()
    );
    assert!(result.is_err());
}

/// Timeout triggers cancellation, which enters drain phase.
async fn timeout_with_drain() {
    println!("--- 3. Timeout + drain (data completes during drain) ---");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("timeout-drain");
    let input = builder.input::<i32>("data");
    input.map("triple", |_t, x| x * 3).output("results");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new()
        .timeout(Duration::from_millis(300))
        .drain_on_cancel(Duration::from_secs(2));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let sender = handle.take_input::<i32>("data").unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    sender.send(0, vec![10, 20]).unwrap();
    sender.close();

    let start = Instant::now();
    let result = handle.join().await;
    let values: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, v)| v)
        .collect();
    println!(
        "  Result: {} (values: {values:?}) (elapsed: {:?})",
        if result.is_ok() { "Ok" } else { "Err" },
        start.elapsed()
    );
    assert!(result.is_ok());
}
