//! # Hello Dataflow
//!
//! Minimal example: creates a source → sink pipeline, runs it to completion,
//! and prints the collected output.
//!
//! ```bash
//! cargo run --example hello_dataflow
//! ```

use instancy::dataflow::builder::{build_and_run, BuilderConfig};

fn main() {
    // Build and run a simple source → sink dataflow.
    //
    // The closure constructs the logical dataflow graph:
    // - `add_source` registers a source operator with timestamped data batches
    // - `add_sink` registers a collecting sink wired to the source's output
    //
    // After the closure returns, the framework materializes channels and runs
    // the executor to completion.
    let collector = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
        let source = ctx.add_source("greetings", vec![
            (0u64, vec!["Hello", "World"]),
            (1u64, vec!["from", "instancy!"]),
        ]);
        let (_sink_idx, collector) = ctx.add_sink::<&str>("output", source);
        Ok(collector)
    })
    .expect("dataflow execution failed");

    // Read collected results
    let data = collector.lock().unwrap();
    println!("Dataflow produced {} batches:", data.len());
    for (time, batch) in data.iter() {
        println!("  t={}: {:?}", time, batch);
    }
}
