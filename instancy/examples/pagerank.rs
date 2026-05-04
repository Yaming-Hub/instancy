//! # PageRank Example
//!
//! Demonstrates iterative graph computation using `Pipe::iterate()`:
//! PageRank computes importance scores for nodes in a directed graph by
//! iteratively distributing rank along edges.
//!
//! This is the instancy equivalent of timely-dataflow's `pagerank.rs` example,
//! simplified to a fixed-iteration PageRank on a static graph (the timely
//! version supports incremental/differential updates).
//!
//! Pipeline: source(initial ranks) → iterate(distribute + accumulate) → output
//!
//! ```bash
//! cargo run --example pagerank
//! ```

use std::collections::HashMap;

use instancy::IterateResult;
use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

/// Number of PageRank iterations.
const ITERATIONS: u32 = 20;
/// Damping factor (standard value).
const DAMPING: f64 = 0.85;

fn main() {
    // Directed graph (src → dst):
    //
    //   0 → 1 → 2
    //   ↑   ↓   ↓
    //   3 ← 4 → 5
    //
    // Node 0 links to 1; node 1 links to 2, 4; node 2 links to 5;
    // node 3 links to 0; node 4 links to 3, 5; node 5 has no outgoing edges (sink).
    let edges: Vec<(u32, u32)> = vec![
        (0, 1),
        (1, 2),
        (1, 4),
        (2, 5),
        (3, 0),
        (4, 3),
        (4, 5),
    ];

    let num_nodes: u32 = 6;

    // Build adjacency: src → list of dst
    let mut out_edges: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(src, dst) in &edges {
        out_edges.entry(src).or_default().push(dst);
    }

    let builder = DataflowBuilder::<u64>::new("pagerank");

    // Initial ranks: each node starts with rank 1.0 / num_nodes.
    let initial_rank = 1.0_f64 / num_nodes as f64;
    let initial: Vec<(u32, f64)> = (0..num_nodes).map(|n| (n, initial_rank)).collect();
    let seeds = builder.source("initial_ranks", vec![(0u64, initial)]);

    // PageRank iteration: distribute rank along edges, accumulate, apply damping.
    let results = seeds
        .iterate::<u32>("pagerank_iter", 1u32, move |ranks| {
            let adj = out_edges.clone();
            let n = num_nodes;

            let computed =
                ranks.unary::<(u32, f64, bool), _>("distribute", move |input, output| {
                while let Some((time, data)) = input.next() {
                    // Accumulate incoming rank contributions.
                    let mut incoming: HashMap<u32, f64> = HashMap::new();

                    for (node, rank) in &data {
                        if let Some(targets) = adj.get(node) {
                            let share = rank / targets.len() as f64;
                            for &dst in targets {
                                *incoming.entry(dst).or_default() += share;
                            }
                        }
                        // Sink nodes (no outgoing): rank is lost (simplified model).
                        // A full implementation would redistribute sink rank.
                    }

                    // Apply damping formula: rank(v) = (1 - d) / N + d * sum(incoming)
                    let base = (1.0 - DAMPING) / n as f64;
                    let new_ranks: Vec<(u32, f64)> = (0..n)
                        .map(|node| {
                            let contrib = incoming.get(&node).copied().unwrap_or(0.0);
                            (node, base + DAMPING * contrib)
                        })
                        .collect();

                    // Use the timestamp's inner value (iteration counter maintained by
                    // iterate()) rather than a manual counter. This is correct regardless
                    // of how many batches arrive per iteration round.
                    let iter_round = time.inner;
                    let is_feedback = iter_round < ITERATIONS;

                    let tagged: Vec<(u32, f64, bool)> = new_ranks
                        .into_iter()
                        .map(|(n, r)| (n, r, is_feedback))
                        .collect();
                    output.push_vec(time, tagged);
                }
                Ok(())
            });

            let feedback = computed
                .clone()
                .filter("keep_iterating", |_t, &(_, _, is_fb)| is_fb)
                .map("untag_fb", |_t, (n, r, _)| (n, r));
            let output = computed
                .filter("converged", |_t, &(_, _, is_fb)| !is_fb)
                .map("untag_out", |_t, (n, r, _)| (n, r));

            IterateResult { feedback, output }
        })
        .output("pagerank_results");

    let dataflow = builder.build().expect("graph construction failed");

    println!("=== PageRank ({ITERATIONS} iterations, damping={DAMPING}) ===");
    println!();
    println!("Graph:");
    println!("  0 → 1 → 2");
    println!("  ↑   ↓   ↓");
    println!("  3 ← 4 → 5");
    println!();

    let rt = SimpleRuntime::new();
    rt.run(dataflow).expect("execution failed");

    let collector = results.collector();
    let data = collector.lock().unwrap();
    let mut ranks: Vec<(u32, f64)> = data
        .iter()
        .flat_map(|(_, batch)| batch.iter().copied())
        .collect();
    ranks.sort_by_key(|(node, _)| *node);

    println!("Final PageRank scores:");
    let mut total = 0.0;
    for (node, rank) in &ranks {
        println!("  node {node}: {rank:.6}");
        total += rank;
    }
    println!("  total: {total:.6}");

    // Verify: node 5 should have the highest rank (two incoming edges, no outgoing).
    // Ranks should sum to approximately 1.0 (with sink loss it may be less).
    assert_eq!(ranks.len(), num_nodes as usize, "should have ranks for all nodes");

    let node5_rank = ranks.iter().find(|(n, _)| *n == 5).unwrap().1;
    let max_rank = ranks.iter().map(|(_, r)| *r).fold(0.0_f64, f64::max);
    assert_eq!(
        node5_rank, max_rank,
        "node 5 should have the highest rank"
    );

    println!("\n✓ All assertions passed!");
}
