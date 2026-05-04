//! # Cancellation Example
//!
//! Demonstrates cooperative cancellation: a dataflow is cancelled before
//! execution, showing how `CancellationToken` integrates with the runtime.
//!
//! ```bash
//! cargo run --example cancellation
//! ```

use instancy::cancellation::CancellationToken;
use instancy::dataflow::DataflowBuilder;
use instancy::error::Error;
use instancy::runtime::SimpleRuntime;

fn main() {
    // Create a cancellation token and cancel it immediately.
    // This simulates an external shutdown signal arriving before the dataflow starts.
    let token = CancellationToken::new();
    token.cancel();

    let builder = DataflowBuilder::<u64>::new("cancellation_demo");
    let _port = builder
        .source("data", vec![(0u64, vec![1, 2, 3, 4, 5])])
        .output("output");
    let dataflow = builder.build().expect("build failed");

    // Run with a pre-cancelled token — should fail immediately
    let result = SimpleRuntime::with_cancel(token).run(dataflow);

    match result {
        Err(Error::Cancelled { .. }) => {
            println!("Dataflow was cancelled (as expected).");
            println!("This demonstrates cooperative shutdown via CancellationToken.");
        }
        Ok(_) => println!("Unexpected: dataflow completed despite cancellation."),
        Err(e) => println!("Unexpected error: {}", e),
    }
}
