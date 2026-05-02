//! # Distinct (Stateful Deduplication)
//!
//! Demonstrates stateful processing: deduplicates items within each
//! timestamp using a `unary` operator with a `HashSet`.
//!
//! Demonstrates:
//! - `unary` for custom stateful logic
//! - Deduplication pattern (common in stream processing)
//! - Source with duplicate data
//!
//! ```bash
//! cargo run --example distinct
//! ```

use std::collections::HashSet;

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("distinct_demo");

    let port = builder
        .source(
            "events",
            vec![
                (0u64, vec!["click", "view", "click", "purchase", "view", "click"]),
                (1u64, vec!["login", "view", "click", "login", "purchase"]),
            ],
        )
        // Deduplicate within each timestamp
        .unary("distinct", |input, output| {
            while let Some((time, data)) = input.next() {
                let mut seen = HashSet::new();
                let unique: Vec<&str> = data
                    .iter()
                    .filter(|item| seen.insert(item.to_string()))
                    .copied()
                    .collect();
                output.push_vec(time, unique);
            }
            Ok(())
        })
        .output("unique_events");

    let dataflow = builder.build().expect("build failed");
    SimpleRuntime::new().run(dataflow).expect("execution failed");

    let collector = port.collector();
    let data = collector.lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}: {batch:?}");
    }
    // Expected:
    // t=0: ["click", "view", "purchase"]
    // t=1: ["login", "view", "click", "purchase"]
}
