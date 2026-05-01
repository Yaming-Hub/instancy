//! # Simple Pipeline
//!
//! Demonstrates a multi-stage transformation pipeline using unary operators:
//! source → double → filter_even → to_string → sink
//!
//! This is the instancy equivalent of timely-dataflow's `simple.rs` example.
//!
//! ```bash
//! cargo run --example simple_pipeline
//! ```

use instancy::dataflow::builder::{build_and_run, BuilderConfig};

fn main() {
    let collector = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
        // Source emits integers 1..=10 at timestamp 0.
        let source = ctx.add_source("numbers", vec![
            (0u64, vec![1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ]);

        // Stage 1: double each value.
        let doubled = ctx.add_unary::<i32, i32, _>("double", source, |_time, data| {
            data.into_iter().map(|x| x * 2).collect()
        });

        // Stage 2: keep only values divisible by 3.
        let filtered = ctx.add_unary::<i32, i32, _>("div_by_3", doubled, |_time, data| {
            data.into_iter().filter(|x| x % 3 == 0).collect()
        });

        // Stage 3: convert to descriptive strings.
        let described = ctx.add_unary::<i32, String, _>("describe", filtered, |_time, data| {
            data.into_iter().map(|x| format!("{x} is divisible by 3")).collect()
        });

        // Sink collects final results.
        let (_, collector) = ctx.add_sink::<String>("output", described);
        Ok(collector)
    })
    .expect("dataflow execution failed");

    let data = collector.lock().unwrap();
    println!("Pipeline: source(1..10) → double → filter(÷3) → describe → sink");
    println!("Results:");
    for (_time, batch) in data.iter() {
        for item in batch {
            println!("  {item}");
        }
    }
}
