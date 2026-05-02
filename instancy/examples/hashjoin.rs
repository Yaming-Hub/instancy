//! # Hash Join Example
//!
//! Demonstrates a streaming hash join using the `binary` operator.
//! Two input streams (users and orders) are joined on a shared key (user_id).
//!
//! This is the instancy equivalent of timely-dataflow's `hashjoin.rs` example,
//! adapted for the separated builder pattern and single-process execution.
//!
//! ```bash
//! cargo run --example hashjoin
//! ```

use std::collections::HashMap;

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    // -----------------------------------------------------------------------
    // Example 1: Simple equi-join (users × orders on user_id)
    // -----------------------------------------------------------------------

    let builder = DataflowBuilder::<u64>::new("hashjoin");

    // Stream 1: users (user_id, name)
    let users = builder.source(
        "users",
        vec![
            (0u64, vec![(1u64, "Alice".to_string()), (2, "Bob".to_string())]),
            (1, vec![(3, "Carol".to_string())]),
        ],
    );

    // Stream 2: orders (user_id, item)
    let orders = builder.source(
        "orders",
        vec![
            (0u64, vec![(1u64, "Widget".to_string()), (2, "Gadget".to_string())]),
            (1, vec![(1, "Sprocket".to_string()), (3, "Thingamajig".to_string())]),
        ],
    );

    // Binary hash join: for each (key, val) from one side, probe the other
    // side's hash map and emit matched pairs.
    let joined = users.binary::<(u64, String), String, _>(
        orders,
        "hash_join",
        |users_in, orders_in, out| {
            let mut user_map: HashMap<u64, Vec<String>> = HashMap::new();
            let mut order_map: HashMap<u64, Vec<String>> = HashMap::new();

            // Drain users: check order_map for matches, then insert into user_map.
            while let Some((time, data)) = users_in.next() {
                let mut results = Vec::new();
                for (uid, name) in data.iter().cloned() {
                    if let Some(items) = order_map.get(&uid) {
                        for item in items {
                            results.push(format!("{name} bought {item}"));
                        }
                    }
                    user_map.entry(uid).or_default().push(name);
                }
                if !results.is_empty() {
                    out.push_vec(time, results);
                }
            }

            // Drain orders: check user_map for matches, then insert into order_map.
            while let Some((time, data)) = orders_in.next() {
                let mut results = Vec::new();
                for (uid, item) in data.iter().cloned() {
                    if let Some(names) = user_map.get(&uid) {
                        for name in names {
                            results.push(format!("{name} bought {item}"));
                        }
                    }
                    order_map.entry(uid).or_default().push(item);
                }
                if !results.is_empty() {
                    out.push_vec(time, results);
                }
            }

            Ok(())
        },
    );

    let output = joined.output("results");

    let dataflow = builder.build().unwrap();
    SimpleRuntime::new().run(dataflow).unwrap();

    println!("=== Hash Join: users × orders ===");
    for (t, batch) in output.collector().lock().unwrap().iter() {
        println!("  t={t}: {batch:?}");
    }

    // -----------------------------------------------------------------------
    // Example 2: Self-join for graph edge composition (A→B, B→C ⟹ A→C)
    // -----------------------------------------------------------------------

    let builder = DataflowBuilder::<u64>::new("graph_join");

    // Graph edges: (src, dst)
    let edges_left = builder.source(
        "edges_left",
        vec![(
            0u64,
            vec![
                (1u64, 2u64), // 1→2
                (2, 3),       // 2→3
                (3, 4),       // 3→4
                (1, 5),       // 1→5
            ],
        )],
    );

    let edges_right = builder.source(
        "edges_right",
        vec![(
            0u64,
            vec![
                (1u64, 2u64), // 1→2
                (2, 3),       // 2→3
                (3, 4),       // 3→4
                (1, 5),       // 1→5
            ],
        )],
    );

    // Join edges on (left.dst == right.src) to find 2-hop paths: A→B→C
    let two_hop = edges_left.binary::<(u64, u64), String, _>(
        edges_right,
        "two_hop_join",
        |left_in, right_in, out| {
            // left_map: keyed by dst (the join key from left side)
            let mut left_map: HashMap<u64, Vec<u64>> = HashMap::new();
            // right_map: keyed by src (the join key from right side)
            let mut right_map: HashMap<u64, Vec<u64>> = HashMap::new();

            while let Some((time, data)) = left_in.next() {
                let mut paths = Vec::new();
                for (src, dst) in data.iter().cloned() {
                    // Check if right side has edges starting from dst
                    if let Some(dsts) = right_map.get(&dst) {
                        for &final_dst in dsts {
                            paths.push(format!("{src}→{dst}→{final_dst}"));
                        }
                    }
                    left_map.entry(dst).or_default().push(src);
                }
                if !paths.is_empty() {
                    out.push_vec(time, paths);
                }
            }

            while let Some((time, data)) = right_in.next() {
                let mut paths = Vec::new();
                for (src, dst) in data.iter().cloned() {
                    // Check if left side has edges ending at src
                    if let Some(srcs) = left_map.get(&src) {
                        for &orig_src in srcs {
                            paths.push(format!("{orig_src}→{src}→{dst}"));
                        }
                    }
                    right_map.entry(src).or_default().push(dst);
                }
                if !paths.is_empty() {
                    out.push_vec(time, paths);
                }
            }

            Ok(())
        },
    );

    let graph_output = two_hop.output("two_hop_paths");

    let dataflow = builder.build().unwrap();
    SimpleRuntime::new().run(dataflow).unwrap();

    println!("\n=== Graph Join: 2-hop paths (A→B→C) ===");
    for (t, batch) in graph_output.collector().lock().unwrap().iter() {
        for path in batch {
            println!("  t={t}: {path}");
        }
    }

    println!("\n=== Done ===");
}
