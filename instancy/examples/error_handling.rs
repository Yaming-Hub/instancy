//! # Error Handling with Result Combinators
//!
//! Demonstrates `map_ok`, `filter_ok`, and `branch_result` for fallible
//! dataflow pipelines.
//!
//! Run with: `cargo run --example error_handling --all-features`

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn print_batches<T: std::fmt::Debug>(label: &str, batches: &[(u64, Vec<T>)]) {
    println!("{label}");
    for (time, batch) in batches {
        println!("  t={time}: {batch:?}");
    }
}

fn main() {
    println!("=== Error Handling Example ===\n");
    println!("Input readings (Celsius):");
    println!(
        "  [Ok(22), Ok(45), Err(\"sensor offline\"), Ok(38), Ok(-5), Err(\"timeout\"), Ok(25)]"
    );
    println!("Keeping only readings in the 32..=100°F operating range.\n");

    let builder = DataflowBuilder::<u64>::new("error_handling");
    let input = builder.input::<Result<i32, String>>("sensor_readings");
    let processed = input
        .map_ok("celsius_to_f", |_t, c| c * 9 / 5 + 32)
        .filter_ok("valid_range", |_t, f| *f >= 32 && *f <= 100);

    processed.clone().output("processed");
    let (good, bad) = processed.branch_result("split");
    good.output("results");
    bad.output("errors");

    let dataflow = builder.build().expect("build failed");
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");
    let mut handle = rt
        .spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed");

    let sender = handle
        .take_input::<Result<i32, String>>("sensor_readings")
        .expect("input port");
    let processed = handle
        .take_output::<Result<i32, String>>("processed")
        .unwrap();
    let results = handle.take_output::<i32>("results").unwrap();
    let errors = handle.take_output::<String>("errors").unwrap();

    sender
        .send(
            0,
            vec![
                Ok(22),
                Ok(45),
                Err("sensor offline".into()),
                Ok(38),
                Ok(-5),
                Err("timeout".into()),
                Ok(25),
            ],
        )
        .expect("send failed");
    drop(sender);

    let processed_data = processed.collect_data();
    let results_data = results.collect_data();
    let errors_data = errors.collect_data();

    handle.join_blocking().expect("dataflow execution failed");

    print_batches(
        "After map_ok + filter_ok (successful readings transformed, errors preserved):",
        &processed_data,
    );
    print_batches(
        "\nResults after branch_result (Ok values only):",
        &results_data,
    );
    print_batches(
        "\nErrors after branch_result (Err values only):",
        &errors_data,
    );
}
