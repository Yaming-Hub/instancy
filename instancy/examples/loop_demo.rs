//! # Feedback Loop Demo (Pipe::iterate)
//!
//! Demonstrates iterative computation using `Pipe::iterate()`:
//! Data circulates through a loop body until a convergence condition is met.
//!
//! Pipeline: source → iterate(double until ≥100) → output
//!
//! ```bash
//! cargo run --example loop_demo
//! ```

use instancy::DataflowBuilder;
use instancy::IterateResult;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let builder = DataflowBuilder::<u64>::new("loop_demo");

    // Start with small numbers at timestamp 0
    let stream = builder.source("seeds", vec![(0u64, vec![1i32, 3, 7, 25])]);

    // Iterate: double each value until it reaches 100 or more.
    // Items that reach the threshold exit via `output`;
    // items still below threshold loop back via `feedback`.
    let output_port = stream
        .iterate::<u32>("double_until_100", 1u32, |iter_var| {
            let doubled = iter_var.map("double", |_t, x| x * 2);
            let done = doubled.clone().filter("done", |_t, x| *x >= 100);
            let again = doubled.filter("again", |_t, x| *x < 100);
            IterateResult {
                feedback: again,
                output: done,
            }
        })
        .output("results")
        .unwrap();

    let dataflow = builder.build().expect("graph construction failed");

    println!("=== Loop Demo: double until >= 100 ===");
    println!("Input: [1, 3, 7, 25]");
    println!();

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .expect("execution failed");

    let collector = output_port.collector();
    let results = collector.lock().unwrap();
    let mut all: Vec<i32> = results
        .iter()
        .flat_map(|(_, v)| v.iter().copied())
        .collect();
    all.sort();

    println!("Output values (all >= 100):");
    for val in &all {
        println!("  {val}");
    }
    // Expected:
    //   1 → 2 → 4 → 8 → 16 → 32 → 64 → 128
    //   3 → 6 → 12 → 24 → 48 → 96 → 192
    //   7 → 14 → 28 → 56 → 112
    //  25 → 50 → 100
    println!("\nExpected: [100, 112, 128, 192]");
    assert_eq!(all, vec![100, 112, 128, 192]);
    println!("✓ All assertions passed!");
}
