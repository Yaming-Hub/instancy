//! Fan-out / branching example — one stream feeding two independent pipelines.
//!
//! Demonstrates how `Stream::clone()` enables splitting a data stream
//! into multiple downstream branches. Each branch processes data independently
//! using a `TeePush` adapter that clones data to all consumers.

use instancy::dataflow::dataflow_builder::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("branching_pipeline");

    // Create a source of numbers 1..=10
    let numbers = builder.source(
        "numbers",
        vec![(0u64, (1..=10i32).collect())],
    );

    // Branch 1: even numbers, doubled
    let evens = numbers
        .clone()
        .filter("keep_even", |_t, x| x % 2 == 0)
        .map("double", |_t, x| x * 2);
    let evens_port = evens.output("doubled_evens");

    // Branch 2: odd numbers, squared
    let odds = numbers
        .filter("keep_odd", |_t, x| x % 2 != 0)
        .map("square", |_t, x| x * x);
    let odds_port = odds.output("squared_odds");

    // Build and run
    let dataflow = builder.build().expect("graph construction failed");
    println!(
        "Dataflow: {} ({} operators, {} edges)",
        dataflow.name(),
        dataflow.operator_count(),
        dataflow.edge_count(),
    );
    println!("Outputs: {:?}", dataflow.output_names());

    SimpleRuntime::new().run(dataflow).expect("execution failed");

    // Read results
    let evens_c = evens_port.collector();
    let evens_data = evens_c.lock().unwrap();
    let odds_c = odds_port.collector();
    let odds_data = odds_c.lock().unwrap();

    println!("\nBranch 1 — doubled evens:");
    for (_t, batch) in evens_data.iter() {
        for val in batch {
            println!("  {val}");
        }
    }

    println!("\nBranch 2 — squared odds:");
    for (_t, batch) in odds_data.iter() {
        for val in batch {
            println!("  {val}");
        }
    }
}
