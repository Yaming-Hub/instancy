//! # Ping-Pong Example
//!
//! Demonstrates data elements circulating through a feedback loop.
//! Each iteration increments every element by 1; elements exit
//! the loop once they reach a threshold value.
//!
//! This is the instancy equivalent of timely-dataflow's `pingpong.rs` example.
//! The original uses `exchange` + `map_in_place` + `branch_when` with
//! `feedback/connect_loop`. Here we use `iterate()` with filter-based
//! convergence, demonstrating data-carrying iteration loops.
//!
//! ```bash
//! cargo run --example pingpong
//! cargo run --example pingpong -- 100 1000  # iterations elements
//! ```

use instancy::DataflowBuilder;
use instancy::IterateResult;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let iterations: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let elements: u64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let builder = DataflowBuilder::<u64>::new("pingpong");

    // Generate initial elements: 0, 1, 2, ..., elements-1
    let data: Vec<u64> = (0..elements).collect();
    let seed = builder.source("data", vec![(0u64, data)]);

    // Each iteration: increment all values by 1.
    // Values >= iterations exit; values < iterations loop back.
    let result = seed
        .iterate::<u32>("bounce", 1u32, move |stream| {
            let incremented = stream.map("incr", |_t, x| x + 1);
            let feedback = incremented
                .clone()
                .filter("keep", move |_t, &x| x < iterations);
            let output = incremented.filter("done", move |_t, &x| x >= iterations);
            IterateResult { feedback, output }
        })
        .output("results").unwrap();

    let dataflow = builder.build().expect("graph construction failed");

    let start = std::time::Instant::now();

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .expect("execution failed");

    let elapsed = start.elapsed();

    let collector = result.collector();
    let data = collector.lock().unwrap();
    let mut values: Vec<u64> = data
        .iter()
        .flat_map(|(_, batch)| batch.iter().copied())
        .collect();
    values.sort();

    assert_eq!(
        values.len(),
        elements as usize,
        "all elements should arrive"
    );

    // Each element v starts at v and increments by 1 per round.
    // It exits when v + k >= iterations (k rounds), so final value = iterations
    // for v < iterations, or v + 1 for v >= iterations (exits after 1 round).
    for (i, &val) in values.iter().enumerate() {
        let initial = i as u64;
        if initial < iterations {
            // Went through (iterations - initial) rounds.
            assert_eq!(val, iterations, "element starting at {initial}");
        } else {
            // Already >= iterations, exits after 1 increment.
            assert_eq!(val, initial + 1, "element starting at {initial}");
        }
    }

    println!("=== Ping-Pong: {elements} elements × up to {iterations} iterations ===");
    println!("  Elapsed: {elapsed:?}");
    if values.is_empty() {
        println!("  Output: 0 elements");
    } else {
        println!(
            "  Output: {} elements, min={}, max={}",
            values.len(),
            values[0],
            values[values.len() - 1]
        );
    }
    println!("✓ All assertions passed!");
}
