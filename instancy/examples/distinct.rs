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

use std::collections::{HashMap, HashSet};

use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("distinct_demo");

    let input = builder.source(
        "events",
        vec![
            (
                0u64,
                vec!["click", "view", "click", "purchase", "view", "click"],
            ),
            (1u64, vec!["login", "view", "click", "login", "purchase"]),
        ],
    );

    // State lives outside the closure so it persists across activations —
    // the unary operator may call the closure multiple times as data arrives
    // in separate batches under backpressure.
    //
    // NOTE: State grows unbounded — a production pipeline would evict
    // completed timestamps via frontier tracking / watermarks.
    let mut seen: HashMap<u64, HashSet<String>> = HashMap::new();

    let port = input
        // Deduplicate within each timestamp
        .unary("distinct", move |input, output| {
            while let Some((time, data)) = input.next() {
                let set = seen.entry(time).or_default();
                let unique: Vec<&str> = data
                    .iter()
                    .filter(|item| set.insert(item.to_string()))
                    .copied()
                    .collect();
                if !unique.is_empty() {
                    output.push_vec(time, unique);
                }
            }
            Ok(())
        })
        .output("unique_events");

    let dataflow = builder.build().expect("build failed");
    SimpleRuntime::new()
        .run(dataflow)
        .expect("execution failed");

    let collector = port.collector();
    let data = collector.lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}: {batch:?}");
    }
    // Expected:
    // t=0: ["click", "view", "purchase"]
    // t=1: ["login", "view", "click", "purchase"]
}
