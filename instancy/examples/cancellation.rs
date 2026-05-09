//! # Cancellation Example
//!
//! Demonstrates cooperative cancellation: a dataflow is cancelled before
//! execution, showing how `CancellationToken` integrates with the runtime.
//!
//! ```bash
//! cargo run --example cancellation
//! ```

use instancy::DataflowBuilder;
use instancy::Error;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    // Create a runtime and cancel it immediately.
    // This simulates an external shutdown signal arriving before the dataflow starts.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");
    rt.cancel_token().cancel();

    let builder = DataflowBuilder::<u64>::new("cancellation_demo");
    let _port = builder
        .source("data", vec![(0u64, vec![1, 2, 3, 4, 5])])
        .output("output").unwrap();
    let dataflow = builder.build().expect("build failed");

    // Run with a pre-cancelled runtime — should fail immediately
    let result = rt
        .spawn(dataflow, SpawnOptions::default())
        .and_then(|handle| handle.join_blocking());

    match result {
        Err(Error::Cancelled { .. }) => {
            println!("Dataflow was cancelled (as expected).");
            println!("This demonstrates cooperative shutdown via CancellationToken.");
        }
        Ok(_) => println!("Unexpected: dataflow completed despite cancellation."),
        Err(e) => println!("Unexpected error: {}", e),
    }
}
