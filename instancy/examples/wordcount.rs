//! # Word Count
//!
//! Classic streaming word count: splits lines into words, counts occurrences
//! per timestamp using a stateful `unary` operator.
//!
//! Demonstrates:
//! - `flat_map` for splitting lines into words
//! - `unary` with stateful aggregation (word counting per batch)
//! - Multiple timestamps representing different time windows
//!
//! ```bash
//! cargo run --example wordcount
//! ```

use std::collections::{HashMap, HashSet};

use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("wordcount");

    let port = builder
        .source(
            "lines",
            vec![
                (
                    0u64,
                    vec!["hello world".to_string(), "hello instancy".to_string()],
                ),
                (
                    1u64,
                    vec![
                        "world of dataflow".to_string(),
                        "hello world again".to_string(),
                    ],
                ),
                (2u64, vec!["instancy is fast".to_string()]),
            ],
        )
        // Split lines into individual words
        .flat_map("split_words", |_t, line| {
            line.split_whitespace().map(|w| w.to_lowercase()).collect()
        })
        // Count word occurrences per timestamp using a stateful unary operator.
        // State lives outside the closure so it persists across activations —
        // under backpressure, data for the same timestamp may arrive in
        // multiple batches.
        //
        // NOTE: State grows unbounded — a production pipeline would evict
        // completed timestamps via frontier tracking / watermarks.
        .unary("count_words", {
            let mut counts_by_time: HashMap<u64, HashMap<String, usize>> = HashMap::new();
            move |input, output| {
                // Drain all available batches first, accumulating counts.
                let mut dirty = HashSet::new();
                while let Some((time, words)) = input.next() {
                    dirty.insert(time);
                    let counts = counts_by_time.entry(time).or_default();
                    for word in words {
                        *counts.entry(word).or_insert(0) += 1;
                    }
                }
                // Emit once per touched timestamp after all batches are consumed,
                // avoiding stale intermediate snapshots.
                for time in dirty {
                    let counts = &counts_by_time[&time];
                    let mut pairs: Vec<(String, usize)> =
                        counts.iter().map(|(k, &v)| (k.clone(), v)).collect();
                    pairs.sort();
                    output.push_vec(time, pairs);
                }
                Ok(())
            }
        })
        .output("counts");

    let dataflow = builder.build().expect("build failed");
    println!(
        "Dataflow: {} ({} operators, {} edges)\n",
        dataflow.name(),
        dataflow.operator_count(),
        dataflow.edge_count(),
    );

    SimpleRuntime::new()
        .run(dataflow)
        .expect("execution failed");

    let collector = port.collector();
    let data = collector.lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}:");
        for (word, count) in batch {
            println!("  {word}: {count}");
        }
    }
}
