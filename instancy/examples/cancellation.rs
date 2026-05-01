//! # Cancellation Example
//!
//! Demonstrates cooperative cancellation: a dataflow is cancelled before
//! execution, showing how `CancellationToken` integrates with the runtime.
//!
//! ```bash
//! cargo run --example cancellation
//! ```

use instancy::cancellation::CancellationToken;
use instancy::dataflow::builder::{build_and_run_with_cancel, BuilderConfig};
use instancy::error::Error;

fn main() {
    // Create a cancellation token and cancel it immediately.
    // This simulates an external shutdown signal arriving before the dataflow starts.
    let token = CancellationToken::new();
    token.cancel();

    let result = build_and_run_with_cancel::<u64, _, _>(
        BuilderConfig::default(),
        token,
        |ctx| {
            let source = ctx.add_source("data", vec![
                (0u64, vec![1, 2, 3, 4, 5]),
            ]);
            let (_sink, collector) = ctx.add_sink::<i32>("output", source);
            Ok(collector)
        },
    );

    match result {
        Err(Error::Cancelled) => {
            println!("Dataflow was cancelled (as expected).");
            println!("This demonstrates cooperative shutdown via CancellationToken.");
        }
        Ok(_) => println!("Unexpected: dataflow completed despite cancellation."),
        Err(e) => println!("Unexpected error: {}", e),
    }
}
