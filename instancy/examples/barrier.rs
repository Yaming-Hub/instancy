//! # Barrier Example
//!
//! Demonstrates progress tracking through many iterations with minimal data.
//! A single token circulates through an `iterate` loop, advancing the
//! iteration counter each round until a limit is reached.
//!
//! This is the instancy equivalent of timely-dataflow's `barrier.rs` example.
//! The original uses `unary_notify` with `feedback/connect_loop` to measure
//! pure notification overhead. Here we use `iterate()` with a filter-based
//! convergence check.
//!
//! ```bash
//! cargo run --example barrier
//! cargo run --example barrier -- 100000  # custom iteration count
//! ```

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    let iterations: u32 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let builder = DataflowBuilder::<u64>::new("barrier");

    // A single token enters the loop.
    let seed = builder.source("token", vec![(0u64, vec![0u32])]);

    // The token circulates through the iterate loop. Each round it
    // increments; once it reaches `iterations` it exits.
    let result = seed
        .iterate::<u32>("barrier_loop", 1u32, move |stream| {
            let incremented = stream.map("tick", |_t, x| x + 1);
            let feedback = incremented
                .clone()
                .filter("keep_going", move |_t, &x| x < iterations);
            let output = incremented.filter("done", move |_t, &x| x >= iterations);
            IterateResult { feedback, output }
        })
        .output("final");

    let dataflow = builder.build().expect("graph construction failed");

    let start = std::time::Instant::now();

    let rt = SimpleRuntime::new();
    rt.run(dataflow).expect("execution failed");

    let elapsed = start.elapsed();

    let collector = result.collector();
    let data = collector.lock().unwrap();
    let values: Vec<u32> = data
        .iter()
        .flat_map(|(_, batch)| batch.iter().copied())
        .collect();

    assert_eq!(values.len(), 1, "expected exactly one token");
    assert_eq!(
        values[0], iterations,
        "token should have value {iterations}"
    );

    println!("=== Barrier: {iterations} iterations ===");
    println!("  Elapsed: {elapsed:?}");
    println!(
        "  Per iteration: {:.0} ns",
        elapsed.as_nanos() as f64 / iterations as f64
    );
    println!("✓ All assertions passed!");
}
