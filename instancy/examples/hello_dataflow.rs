//! # Hello Dataflow
//!
//! Minimal example: creates a source → sink pipeline, runs it to completion,
//! and prints the collected output.
//!
//! ```bash
//! cargo run --example hello_dataflow
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    // Build a simple source → output dataflow using the Pipe chaining API.
    let builder = DataflowBuilder::<u64>::new("hello");
    let port = builder
        .source(
            "greetings",
            vec![
                (0u64, vec!["Hello", "World"]),
                (1u64, vec!["from", "instancy!"]),
            ],
        )
        .output("output")
        .unwrap();

    let dataflow = builder.build().expect("graph construction failed");

    // Execute via RuntimeHandle
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .expect("dataflow execution failed");

    // Read collected results
    let collector = port.collector();
    let data = collector.lock().unwrap();
    println!("Dataflow produced {} batches:", data.len());
    for (time, batch) in data.iter() {
        println!("  t={}: {:?}", time, batch);
    }
}
