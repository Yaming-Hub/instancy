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

use std::collections::HashMap;

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("wordcount");

    let port = builder
        .source(
            "lines",
            vec![
                (0u64, vec![
                    "hello world".to_string(),
                    "hello instancy".to_string(),
                ]),
                (1u64, vec![
                    "world of dataflow".to_string(),
                    "hello world again".to_string(),
                ]),
                (2u64, vec![
                    "instancy is fast".to_string(),
                ]),
            ],
        )
        // Split lines into individual words
        .flat_map("split_words", |_t, line| {
            line.split_whitespace()
                .map(|w| w.to_lowercase())
                .collect()
        })
        // Count word occurrences per timestamp using a stateful unary operator
        .unary("count_words", |input, output| {
            while let Some((time, words)) = input.next() {
                let mut counts: HashMap<String, usize> = HashMap::new();
                for word in words {
                    *counts.entry(word.clone()).or_insert(0) += 1;
                }
                // Emit sorted (word, count) pairs for deterministic output
                let mut pairs: Vec<(String, usize)> = counts.into_iter().collect();
                pairs.sort();
                output.push_vec(time, pairs);
            }
            Ok(())
        })
        .output("counts");

    let dataflow = builder.build().expect("build failed");
    println!(
        "Dataflow: {} ({} operators, {} edges)\n",
        dataflow.name(),
        dataflow.operator_count(),
        dataflow.edge_count(),
    );

    SimpleRuntime::new().run(dataflow).expect("execution failed");

    let collector = port.collector();
    let data = collector.lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}:");
        for (word, count) in batch {
            println!("  {word}: {count}");
        }
    }
}
