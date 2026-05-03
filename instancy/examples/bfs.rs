//! # Breadth-First Search Example
//!
//! Demonstrates iterative graph computation using `Pipe::iterate()`:
//! BFS explores a graph level by level, discovering all reachable nodes
//! from a root and recording each node's distance from the root.
//!
//! This is the instancy equivalent of timely-dataflow's `bfs.rs` example,
//! adapted for the builder pattern with `iterate` and `unary` operators.
//!
//! Pipeline: source(root) → iterate(expand unvisited neighbors) → output
//!
//! ```bash
//! cargo run --example bfs
//! ```

use std::collections::{HashMap, HashSet};

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    // Build a sample undirected graph:
    //
    //  0 --- 1 --- 3 --- 4 --- 5
    //  |           |           |
    //  2 ----------+     6 --- 7
    //                    |
    //                    8 --- 9
    //
    let edges: Vec<(u32, u32)> = vec![
        (0, 1),
        (0, 2),
        (1, 3),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 7),
        (6, 7),
        (6, 8),
        (8, 9),
    ];

    let mut adjacency: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(src, dst) in &edges {
        adjacency.entry(src).or_default().push(dst);
        adjacency.entry(dst).or_default().push(src);
    }

    let builder = DataflowBuilder::<u64>::new("bfs");

    // Seed: start BFS from node 0.
    // Each record is (node, distance_from_root).
    let seeds = builder.source("roots", vec![(0u64, vec![(0u32, 0u32)])]);

    // BFS iteration: each round, take the current frontier, look up neighbors,
    // emit newly discovered nodes as feedback, and emit already-visited nodes
    // as output (they're "done").
    let mut visited: HashSet<u32> = HashSet::new();

    let results = seeds
        .iterate::<u32>("bfs_expand", 1u32, move |frontier| {
            // Inside the loop body, timestamps are Product<u64, u32> where
            // the inner u32 is the iteration counter.
            //
            // Use a unary operator with captured state to:
            // 1. Mark incoming nodes as visited
            // 2. Look up their neighbors
            // 3. Tag output: (node, dist, is_feedback) — true for new frontier,
            //    false for "done" nodes
            let adj = adjacency.clone();

            let expanded =
                frontier.unary::<(u32, u32, bool), _>("expand", move |input, output| {
                    while let Some((time, data)) = input.next() {
                        let mut tagged = Vec::new();

                        for (node, dist) in data {
                            if visited.insert(node) {
                                // Newly discovered — emit as "done" output.
                                tagged.push((node, dist, false));
                                // Expand neighbors into the next frontier.
                                if let Some(neighbors) = adj.get(&node) {
                                    for &nbr in neighbors {
                                        if !visited.contains(&nbr) {
                                            tagged.push((nbr, dist + 1, true));
                                        }
                                    }
                                }
                            }
                        }

                        if !tagged.is_empty() {
                            output.push_vec(time, tagged);
                        }
                    }

                    Ok(())
                });

            // Split tagged stream into feedback (new frontier) and output (done nodes).
            let feedback = expanded
                .clone()
                .filter("is_frontier", |_t, &(_, _, is_fb)| is_fb)
                .map("untag_fb", |_t, (n, d, _)| (n, d));
            let output = expanded
                .filter("is_done", |_t, &(_, _, is_fb)| !is_fb)
                .map("untag_done", |_t, (n, d, _)| (n, d));

            IterateResult { feedback, output }
        })
        .output("bfs_results");

    let dataflow = builder.build().expect("graph construction failed");

    println!("=== BFS: Breadth-First Search from node 0 ===");
    println!();
    println!("Graph:");
    println!("  0 --- 1 --- 3 --- 4 --- 5");
    println!("  |           |           |");
    println!("  2 ----------+     6 --- 7");
    println!("                    |");
    println!("                    8 --- 9");
    println!();

    let rt = SimpleRuntime::new();
    rt.run(dataflow).expect("execution failed");

    let collector = results.collector();
    let data = collector.lock().unwrap();
    let mut bfs_results: Vec<(u32, u32)> = data
        .iter()
        .flat_map(|(_, batch)| batch.iter().copied())
        .collect();
    bfs_results.sort_by_key(|(node, _)| *node);

    println!("BFS results (node, distance):");
    for (node, dist) in &bfs_results {
        println!("  node {node}: distance {dist}");
    }

    // Expected distances from node 0:
    //   0→0=0, 0→1=1, 0→2=1, 0→3=2, 0→4=3, 0→5=4, 0→7=5, 0→6=6, 0→8=7, 0→9=8
    let expected: Vec<(u32, u32)> = vec![
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 3),
        (5, 4),
        (6, 6),
        (7, 5),
        (8, 7),
        (9, 8),
    ];
    assert_eq!(bfs_results, expected);
    println!("\n✓ All assertions passed!");
}
