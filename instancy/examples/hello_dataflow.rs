//! # Hello Dataflow
//!
//! Minimal example: creates a source → sink pipeline, runs it to completion,
//! and prints the collected output.
//!
//! ```bash
//! cargo run --example hello_dataflow
//! ```

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    // Build a simple source → output dataflow using the Stream chaining API.
    let builder = DataflowBuilder::<u64>::new("hello");
    let port = builder
        .source("greetings", vec![
            (0u64, vec!["Hello", "World"]),
            (1u64, vec!["from", "instancy!"]),
        ])
        .output("output");

    let dataflow = builder.build().expect("graph construction failed");

    // Execute via SimpleRuntime
    SimpleRuntime::new().run(dataflow).expect("dataflow execution failed");

    // Read collected results
    let collector = port.collector();
    let data = collector.lock().unwrap();
    println!("Dataflow produced {} batches:", data.len());
    for (time, batch) in data.iter() {
        println!("  t={}: {:?}", time, batch);
    }
}
