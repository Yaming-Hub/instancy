//! # Union-Find Example
//!
//! Demonstrates a streaming union-find (connected components) algorithm
//! using a stateful `unary` operator.
//!
//! Edges arrive as a stream. The operator maintains a union-find data
//! structure and only forwards edges whose endpoints are in *different*
//! components (i.e., the edge is "new" — it actually merges two components).
//! Duplicate/redundant edges are filtered out.
//!
//! This is the instancy equivalent of timely-dataflow's `unionfind.rs` example.
//!
//! ```bash
//! cargo run --example unionfind
//! ```

use std::cmp::Ordering;

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("unionfind");

    // A graph with 8 nodes and some redundant edges.
    // Connected components: {0,1,2,3}, {4,5,6}, {7}
    let edges: Vec<(usize, usize)> = vec![
        // Component 1: 0-1-2-3
        (0, 1),
        (1, 2),
        (2, 3),
        (0, 3), // redundant (already connected)
        (1, 3), // redundant
        // Component 2: 4-5-6
        (4, 5),
        (5, 6),
        (4, 6), // redundant
        // Component 3: 7 alone (no edges)
    ];

    let input = builder.source(
        "edges",
        vec![
            (0u64, edges[..3].to_vec()), // first batch: 3 edges
            (1, edges[3..6].to_vec()),   // second batch: 3 edges
            (2, edges[6..].to_vec()),    // third batch: 2 edges
        ],
    );

    // Union-find operator: stateful unary that maintains roots[] and ranks[].
    // Forwards only edges that actually merge two different components.
    let mut roots: Vec<usize> = Vec::new();
    let mut ranks: Vec<u8> = Vec::new();

    let merges = input.unary::<(usize, usize), _>("union_find", move |input, output| {
        while let Some((time, data)) = input.next() {
            let mut new_merges = Vec::new();

            for (mut x, mut y) in data {
                // Grow arrays if needed.
                let m = x.max(y);
                while roots.len() <= m {
                    let i = roots.len();
                    roots.push(i);
                    ranks.push(0);
                }

                // Find roots with path compression.
                while x != roots[x] {
                    roots[x] = roots[roots[x]]; // path halving
                    x = roots[x];
                }
                while y != roots[y] {
                    roots[y] = roots[roots[y]];
                    y = roots[y];
                }

                // If different components, merge and emit.
                if x != y {
                    new_merges.push((x, y));
                    match ranks[x].cmp(&ranks[y]) {
                        Ordering::Less => roots[x] = y,
                        Ordering::Greater => roots[y] = x,
                        Ordering::Equal => {
                            roots[y] = x;
                            ranks[x] += 1;
                        }
                    }
                }
            }

            if !new_merges.is_empty() {
                output.push_vec(time, new_merges);
            }
        }
        Ok(())
    });

    let output = merges.output("merges");

    let dataflow = builder.build().expect("graph construction failed");

    println!("=== Union-Find: Streaming Connected Components ===");
    println!();
    println!("Input edges (8 nodes, 3 components):");
    println!("  t=0: (0,1), (1,2), (2,3)");
    println!("  t=1: (0,3), (1,3), (4,5)");
    println!("  t=2: (5,6), (4,6)");
    println!();

    let rt = SimpleRuntime::new();
    rt.run(dataflow).expect("execution failed");

    let collector = output.collector();
    let data = collector.lock().unwrap();
    let mut total_merges = 0;

    println!("Merge edges (only non-redundant edges forwarded):");
    for (t, batch) in data.iter() {
        for (x, y) in batch {
            println!("  t={t}: merge({x}, {y})");
            total_merges += 1;
        }
    }

    // With 3 components of sizes {4, 3, 1}, we need exactly:
    //   (4-1) + (3-1) + (1-1) = 3 + 2 + 0 = 5 merges
    // out of the 8 input edges.
    println!("\nTotal merges: {total_merges} (filtered {}/8 redundant edges)",
        8 - total_merges);
    assert_eq!(total_merges, 5, "expected 5 merges for 3 components");

    println!("✓ All assertions passed!");
}
