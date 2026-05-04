//! # Exchange Word Count
//!
//! Distributed word count using multi-worker exchange channels.
//!
//! Each worker receives a partition of input lines, splits them into words,
//! then exchanges words by hash so each worker counts a disjoint subset.
//! This ensures global correctness: every occurrence of "hello" goes to the
//! same worker, regardless of which partition it appeared in.
//!
//! ```text
//! Worker 0: ["hello world", "hello instancy"]
//!     ↓ split + exchange(hash(word))
//! Worker 0 counts: {"hello": 3, "world": 4, ...}   (words hashing to worker 0)
//! Worker 1 counts: {"instancy": 2, "of": 1, ...}   (words hashing to worker 1)
//! ```
//!
//! Demonstrates:
//! - `spawn_multi()` with multiple logical workers
//! - `exchange_by_hash()` for key-partitioned cross-worker data routing
//! - `unary` with stateful aggregation after exchange
//! - Per-worker output collection
//!
//! Run with: `cargo run --example exchange_wordcount`

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle};

fn main() {
    println!("=== Exchange Word Count ===\n");

    // --- Input data: 2 partitions of text lines ---
    let partitions: Vec<Vec<String>> = vec![
        vec![
            "hello world".into(),
            "hello instancy".into(),
            "world of dataflow".into(),
        ],
        vec![
            "hello world again".into(),
            "instancy is fast".into(),
            "dataflow world".into(),
        ],
    ];
    let num_workers = partitions.len();

    println!("Input partitions:");
    for (i, p) in partitions.iter().enumerate() {
        for line in p {
            println!("  worker {i}: \"{line}\"");
        }
    }
    println!();

    // --- Create runtime ---
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("runtime creation failed");

    // --- Build dataflow: split → exchange(word) → count ---
    let mut multi = rt
        .spawn_multi(
            "exchange-wordcount",
            num_workers,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<String>("lines");

                input
                    // Split lines into individual words.
                    .flat_map("split", |_t, line| {
                        line.split_whitespace()
                            .map(|w| w.to_lowercase())
                            .collect()
                    })
                    // Exchange words by hash — all occurrences of the same word
                    // go to the same worker for correct global counting.
                    // We use exchange_by_hash (not exchange) because we compute
                    // the hash ourselves; exchange() would hash the key again.
                    .exchange_by_hash("by_word", |word: &String| {
                        let mut h = DefaultHasher::new();
                        word.hash(&mut h);
                        h.finish()
                    })
                    // Count words. State is per-worker — after exchange, each
                    // worker sees a disjoint subset of words.
                    //
                    // With exchange, data for the same timestamp may arrive in
                    // multiple batches (from different source workers across
                    // separate activations). Each emission is a COMPLETE snapshot
                    // of all accumulated counts for that timestamp. The collector
                    // uses "last write wins" per timestamp — this is safe because
                    // each snapshot subsumes all previous ones and the output
                    // channel preserves send order.
                    .unary("count", {
                        let mut counts_by_time: HashMap<u64, HashMap<String, usize>> =
                            HashMap::new();
                        move |input, output| {
                            let mut dirty = HashSet::new();
                            while let Some((time, words)) = input.next() {
                                dirty.insert(time);
                                let counts = counts_by_time.entry(time).or_default();
                                for word in words {
                                    *counts.entry(word).or_insert(0) += 1;
                                }
                            }
                            // Re-emit full snapshot for every dirty timestamp.
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

                Ok(())
            },
        )
        .expect("spawn_multi failed");

    println!("Spawned {num_workers} workers with exchange\n");

    // --- Feed partitioned data ---
    let senders = multi.take_all_inputs::<String>("lines").unwrap();
    for (i, partition) in partitions.into_iter().enumerate() {
        senders[i].send(0u64, partition).unwrap();
    }
    drop(senders);

    // --- Collect per-worker results ---
    let receivers = multi.take_all_outputs::<(String, usize)>("counts").unwrap();

    // Wait for completion.
    multi.join_blocking().expect("dataflow completed");

    // Merge results from all workers into a global count.
    // Each worker may emit multiple batches for the same timestamp as counts
    // accumulate across exchange activations. Each batch is a complete snapshot
    // that subsumes all previous ones, so we use "last write wins" per timestamp.
    // This works because: (1) each emission is a full snapshot, not a delta,
    // and (2) the output channel preserves send order within a single producer.
    let mut global_counts: HashMap<String, usize> = HashMap::new();
    for (i, receiver) in receivers.into_iter().enumerate() {
        // Group by timestamp, keep only the last batch per timestamp.
        let all_batches = receiver.collect_data();
        let mut by_time: HashMap<u64, Vec<(String, usize)>> = HashMap::new();
        for (time, batch) in all_batches {
            by_time.insert(time, batch); // last write wins
        }
        let data: Vec<(String, usize)> = by_time.into_values().flatten().collect();
        let mut sorted_data = data.clone();
        sorted_data.sort();
        println!("Worker {i} counted {} distinct words:", sorted_data.len());
        for (word, count) in &sorted_data {
            println!("  {word}: {count}");
            *global_counts.entry(word.clone()).or_insert(0) += count;
        }
        println!();
    }

    // Print global results.
    let mut sorted: Vec<_> = global_counts.into_iter().collect();
    sorted.sort();
    println!("Global word counts:");
    for (word, count) in &sorted {
        println!("  {word}: {count}");
    }

    // Verify expected counts.
    let expected: HashMap<&str, usize> = [
        ("hello", 3),
        ("world", 4),
        ("instancy", 2),
        ("of", 1),
        ("dataflow", 2),
        ("again", 1),
        ("is", 1),
        ("fast", 1),
    ]
    .into_iter()
    .collect();

    println!("\nVerification:");
    let mut all_correct = true;
    for (word, expected_count) in &expected {
        let actual = sorted.iter().find(|(w, _)| w == word).map(|(_, c)| *c);
        if actual == Some(*expected_count) {
            println!("  ✓ {word}: {expected_count}");
        } else {
            println!("  ✗ {word}: expected {expected_count}, got {actual:?}");
            all_correct = false;
        }
    }
    if all_correct {
        println!("\nAll word counts correct! Exchange routing works.");
    } else {
        println!("\nSome counts are wrong!");
        std::process::exit(1);
    }
}
