//! # Simple Pipeline (Pipe Chaining API)
//!
//! Demonstrates the separated builder pattern:
//! 1. Build a logical dataflow using typed Pipe chaining
//! 2. Execute it via `RuntimeHandle::spawn()` + `join_blocking()`
//!
//! Pipeline: source(1..10) → double → filter(÷3) → describe → output
//!
//! ```bash
//! cargo run --example simple_pipeline
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    // Phase 1: Build logical dataflow (no runtime involved)
    let builder = DataflowBuilder::<u64>::new("simple_pipeline");

    let output_port = builder
        .source(
            "numbers",
            vec![(0u64, vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10])],
        )
        .map("double", |_t, x| x * 2)
        .filter("div_by_3", |_t, x| x % 3 == 0)
        .map("describe", |_t, x| format!("{x} is divisible by 3"))
        .output("results");

    // Inspect the logical graph before running
    let dataflow = builder.build().expect("graph construction failed");
    println!(
        "Dataflow: {} ({} operators, {} edges)",
        dataflow.name(),
        dataflow.operator_count(),
        dataflow.edge_count(),
    );
    println!("Outputs: {:?}", dataflow.output_names());

    // Phase 2: Execute via RuntimeHandle
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .expect("dataflow execution failed");

    // Phase 3: Read results
    let results = output_port.collector();
    let data = results.lock().unwrap();
    println!("\nPipeline: source(1..10) → double → filter(÷3) → describe → sink");
    println!("Results:");
    for (_time, batch) in data.iter() {
        for item in batch {
            println!("  {item}");
        }
    }
}
