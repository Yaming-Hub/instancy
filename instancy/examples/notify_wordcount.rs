//! # Notify Word Count — Frontier-Based Aggregation
//!
//! Distributed word count using `unary_notify` for proper frontier-based
//! aggregation. Unlike `exchange_wordcount` which uses "emit on dirty"
//! (immediate emission on every activation), this example buffers word
//! counts and emits them exactly once when the input frontier advances
//! past each timestamp — the canonical streaming dataflow aggregation pattern.
//!
//! ## Why frontier-based aggregation?
//!
//! When multiple workers exchange data by key, a receiving worker may get
//! contributions for the same timestamp across multiple activations (from
//! different upstream workers). The "emit on dirty" pattern re-emits the
//! entire state on each activation, producing redundant output. With
//! `unary_notify`, the operator:
//!
//! 1. Buffers data as it arrives via `input.next()`
//! 2. Registers a notification via `ctx.notify_at(time)` — this also holds
//!    an output capability that prevents downstream from advancing past `time`
//! 3. When the frontier advances past `time`, the notification fires and
//!    the operator emits the final aggregated result exactly once
//!
//! ```text
//! Worker 0: ["hello world", "hello instancy"]
//!     ↓ split + exchange(hash(word))
//!     ↓ unary_notify: buffer words, count on frontier advance
//! Worker 0 emits: [("hello", 3), ("world", 4), ...]  (once, when t=0 is complete)
//! ```
//!
//! Run with: `cargo run --example notify_wordcount`

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle};

fn main() {
    println!("=== Notify Word Count (Frontier-Based Aggregation) ===\n");

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

    // --- Build dataflow: split → exchange(word) → count_with_notify ---
    let mut multi = rt
        .spawn_multi(
            "notify-wordcount",
            num_workers,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<String>("lines");

                input
                    // Split lines into individual words.
                    .flat_map("split", |_t, line| {
                        line.split_whitespace().map(|w| w.to_lowercase()).collect()
                    })
                    // Exchange words by hash — all occurrences of the same word
                    // go to the same worker for correct global counting.
                    .exchange_by_hash("by_word", |word: &String| {
                        let mut h = DefaultHasher::new();
                        word.hash(&mut h);
                        h.finish()
                    })
                    // Count words using unary_notify: buffer incoming words and
                    // emit final counts exactly once when the frontier advances
                    // past the timestamp. This is the canonical streaming dataflow
                    // aggregation pattern — no redundant "last write wins" output.
                    .unary_notify("count", {
                        let mut stash: HashMap<u64, HashMap<String, usize>> = HashMap::new();
                        move |input, output, ctx| {
                            // Buffer words and request per-timestamp notification.
                            while let Some((time, words)) = input.next() {
                                let counts = stash.entry(time).or_default();
                                for word in words {
                                    *counts.entry(word).or_insert(0) += 1;
                                }
                                // Hold output capability + register notification.
                                ctx.notify_at(time);
                            }
                            // Emit final counts when notifications fire.
                            while let Some(time) = ctx.next_notification() {
                                if let Some(counts) = stash.remove(&time) {
                                    let mut pairs: Vec<(String, usize)> =
                                        counts.into_iter().collect();
                                    pairs.sort();
                                    output.push_vec(time, pairs);
                                }
                                // Output capability dropped automatically here.
                            }
                            Ok(())
                        }
                    })
                    .output("counts");

                Ok(())
            },
        )
        .expect("spawn_multi failed");

    println!("Spawned {num_workers} workers with exchange + unary_notify\n");

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

    // With unary_notify, each worker emits exactly one batch per timestamp
    // when the frontier advances. No "last write wins" deduplication needed —
    // each emission is the final result.
    let mut global_counts: HashMap<String, usize> = HashMap::new();
    for (i, receiver) in receivers.into_iter().enumerate() {
        let data = receiver.collect_data();
        let mut worker_counts: Vec<(String, usize)> = Vec::new();
        for (_time, batch) in data {
            for (word, count) in batch {
                *global_counts.entry(word.clone()).or_insert(0) += count;
                worker_counts.push((word, count));
            }
        }
        worker_counts.sort();
        println!("Worker {i} counted {} distinct words:", worker_counts.len());
        for (word, count) in &worker_counts {
            println!("  {word}: {count}");
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
        let actual = sorted.iter().find(|(w, _)| w == word).map(|(_, c)| c);
        if actual == Some(expected_count) {
            println!("  ✓ {word}: {expected_count}");
        } else {
            println!("  ✗ {word}: expected {expected_count}, got {actual:?}");
            all_correct = false;
        }
    }
    if all_correct {
        println!("\nAll word counts correct! Frontier-based aggregation works.");
    } else {
        println!("\nSome counts are wrong!");
        std::process::exit(1);
    }
}
