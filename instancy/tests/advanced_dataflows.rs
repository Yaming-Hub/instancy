//! Integration tests covering advanced dataflow composition patterns.
//!
//! These tests exercise iterative graph traversal, frontier-driven windowing,
//! staged parallelism, branching/error routing, and nested loop scopes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, Pipe, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn test_runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Breadth-first search expressed as an iterative dataflow.
///
/// The loop keeps feeding newly discovered vertices back into the frontier,
/// while a stateful `unary` operator deduplicates nodes so each vertex is
/// visited exactly once. The output records the shortest level discovered for
/// each reachable node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iterative_bfs_levels() {
    let rt = test_runtime();

    let edges = vec![
        (0u32, 1u32),
        (1, 0),
        (0, 2),
        (2, 0),
        (1, 3),
        (3, 1),
        (2, 3),
        (3, 2),
        (3, 4),
        (4, 3),
        (4, 5),
        (5, 4),
    ];

    let mut adjacency: HashMap<u32, Vec<u32>> = HashMap::new();
    for (src, dst) in edges {
        adjacency.entry(src).or_default().push(dst);
    }

    let builder = DataflowBuilder::<u64>::new("iterative-bfs-levels");
    let input = builder.input::<(u32, u32)>("seed").unwrap();
    let mut visited = HashSet::<u32>::new();

    let output = input.iterate::<u32>("bfs", 1u32, move |iter_var| {
        let adj = adjacency.clone();
        let expanded = iter_var.clone().flat_map(
            "expand_neighbors",
            move |_t: &Product<u64, u32>, (node, level)| {
                adj.get(&node)
                    .into_iter()
                    .flat_map(|neighbors| neighbors.iter().copied())
                    .map(|neighbor| (neighbor, level + 1))
                    .collect::<Vec<_>>()
            },
        );

        let discovered = iter_var.unary::<(u32, u32), _>("visit_current", move |input, output| {
            while let Some((time, data)) = input.next() {
                let mut unique = Vec::new();
                for (node, level) in data {
                    if visited.insert(node) {
                        unique.push((node, level));
                    }
                }
                if !unique.is_empty() {
                    output.push_vec(time, unique);
                }
            }
            Ok(())
        });

        let next_frontier = expanded.unary::<(u32, u32), _>("dedup_neighbors", {
            let mut seen_in_loop = HashSet::<u32>::new();
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut unique = Vec::new();
                    for (node, level) in data {
                        if seen_in_loop.insert(node) {
                            unique.push((node, level));
                        }
                    }
                    if !unique.is_empty() {
                        output.push_vec(time, unique);
                    }
                }
                Ok(())
            }
        });

        let feedback = next_frontier;
        let output = discovered;
        IterateResult { feedback, output }
    });
    output.output("results").unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<(u32, u32)>("seed").unwrap();
    let receiver = spawned.take_output::<(u32, u32)>("results").unwrap();

    sender.send(0, vec![(0, 0)]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<(u32, u32)> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    results.sort_by_key(|(node, _)| *node);

    assert_eq!(
        results,
        vec![(0, 0), (1, 1), (2, 1), (3, 2), (4, 3), (5, 4)]
    );
}

/// Uses `delay_batch` plus `unary_notify` to build fixed-size windows.
///
/// Each event is retimed into the end of its 5-tick window. The notify-based
/// operator buffers all values for a window and emits a single sum only after
/// the frontier proves the window is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_windowed_aggregation() {
    let rt = test_runtime();

    let builder = DataflowBuilder::<u64>::new("delay-windowed-aggregation");
    let input = builder.input::<i64>("events").unwrap();
    input
        .delay_batch("window_5", |t| (t / 5 + 1) * 5)
        .unary_notify("sum_window", {
            let mut pending: HashMap<u64, i64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    let sum = pending.entry(time).or_insert(0);
                    for value in data {
                        *sum += value;
                    }
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(sum) = pending.remove(&time) {
                        output.push_vec(time, vec![sum]);
                    }
                }
                Ok(())
            }
        })
        .output("results")
        .unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("events").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    for ts in 0u64..20 {
        sender.send(ts, vec![(ts as i64) + 1]).unwrap();
    }
    sender.advance_to(25).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results = receiver.collect_data();
    results.sort_by_key(|(time, _)| *time);
    assert_eq!(
        results,
        vec![
            (5, vec![15]),
            (10, vec![40]),
            (15, vec![65]),
            (20, vec![90])
        ]
    );
}

/// Exercises per-stage parallelism with a multi-stage pipeline.
///
/// A single parsing stage fans out to four workers for numeric work, reshards
/// into two workers for the final formatting stage, and then gathers to one
/// worker for deterministic collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_stage_pipeline() {
    let rt = test_runtime();

    let mut multi = rt
        .spawn_multi(
            "multi-stage-pipeline",
            1,
            |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<String>("data").unwrap();
                input
                    .map("parse", |_t, s: String| s.parse::<i64>().unwrap_or(0))
                    .exchange_to("scatter", 4, |v: &i64| *v)
                    .unwrap()
                    .map("square", |_t, x| x * x)
                    .exchange_to("reshard", 2, |v: &i64| v % 2)
                    .unwrap()
                    .map("format", |_t, x| x.to_string())
                    .gather("collect")
                    .output("results")
                    .unwrap();
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(true),
        )
        .unwrap();

    let sender = multi.take_input::<String>(0, "data").unwrap();
    let receiver = multi.take_output::<String>(0, "results").unwrap();

    sender
        .send(
            0,
            vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
        )
        .unwrap();
    drop(sender);

    multi.join_blocking().unwrap();

    let mut results: Vec<String> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    results.sort();

    assert_eq!(results, vec!["1", "16", "25", "4", "9"]);
}

/// Demonstrates validation, branching, fan-out, and side-channel merging.
///
/// Positive values continue through the main computation, invalid values are
/// routed to an error output, valid values are forked for logging, and an audit
/// stream is assembled with `Pipe::concat` from both success and failure paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_pipeline_error_handling() {
    let rt = test_runtime();

    let logged = Arc::new(Mutex::new(Vec::new()));
    let logged_for_operator = Arc::clone(&logged);

    let builder = DataflowBuilder::<u64>::new("deep-pipeline-error-handling");
    let input = builder.input::<i64>("data").unwrap();

    let validated = input.map("validate", |_t, x| -> Result<i64, String> {
        if x > 0 {
            Ok(x)
        } else {
            Err(format!("invalid: {x}"))
        }
    });

    let (valid, errors) = validated.branch_result("split_results");
    let valid_audit = valid
        .clone()
        .map("accepted_audit", |_t, x| format!("accepted:{x}"));
    let error_audit = errors
        .clone()
        .map("rejected_audit", |_t, e| format!("rejected:{e}"));
    let (process_pipe, log_pipe) = valid.fork("tap_valid");

    log_pipe.for_each("capture_valid", move |_t, x: &i64| {
        logged_for_operator.lock().unwrap().push(*x);
    });

    process_pipe
        .map("square", |_t, x| x * x)
        .output("results")
        .unwrap();
    errors.output("errors").unwrap();
    Pipe::concat(vec![valid_audit, error_audit])
        .unwrap()
        .output("audit")
        .unwrap();

    let logical = builder.build().unwrap();
    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<i64>("data").unwrap();
    let results_rx = spawned.take_output::<i64>("results").unwrap();
    let errors_rx = spawned.take_output::<String>("errors").unwrap();
    let audit_rx = spawned.take_output::<String>("audit").unwrap();

    sender.send(0, vec![-2, 0, 3, 4]).unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i64> = results_rx
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    results.sort();

    let mut errors: Vec<String> = errors_rx
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    errors.sort();

    let mut audit: Vec<String> = audit_rx
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    audit.sort();

    let mut logged_values = logged.lock().unwrap().clone();
    logged_values.sort();

    assert_eq!(results, vec![9, 16]);
    assert_eq!(
        errors,
        vec!["invalid: -2".to_string(), "invalid: 0".to_string()]
    );
    assert_eq!(
        audit,
        vec![
            "accepted:3".to_string(),
            "accepted:4".to_string(),
            "rejected:invalid: -2".to_string(),
            "rejected:invalid: 0".to_string(),
        ]
    );
    assert_eq!(logged_values, vec![3, 4]);
}

/// Exercises nested `Product<Product<u64, u32>, u32>` timestamps in a loop.
///
/// The root dataflow already uses `Product<u64, u32>` timestamps, and the
/// iteration adds another `u32` layer. Records model an outer/inner loop state
/// machine: the inner phase increments until a multiple of 10, and the outer
/// phase decides whether to restart another inner pass or emit the final value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_iteration() {
    let rt = test_runtime();

    let builder = DataflowBuilder::<Product<u64, u32>>::new("nested-iteration");
    let input = builder.input::<(i64, bool)>("data").unwrap();
    let output = input.iterate::<u32>("outer_inner", 1u32, |iter_var| {
        let stepped = iter_var.map(
            "step",
            |_t: &Product<Product<u64, u32>, u32>, (value, at_checkpoint): (i64, bool)| {
                if at_checkpoint {
                    (value, true)
                } else {
                    let next = value + 1;
                    (next, next % 10 == 0)
                }
            },
        );

        let continue_inner = stepped
            .clone()
            .filter("continue_inner", |_t, (_value, at_checkpoint)| {
                !*at_checkpoint
            });
        let restart_outer = stepped
            .clone()
            .filter("restart_outer", |_t, (value, at_checkpoint)| {
                *at_checkpoint && *value < 30
            })
            .map("restart_inner", |_t, (value, _)| (value, false));
        let done = stepped.filter("done", |_t, (value, at_checkpoint)| {
            *at_checkpoint && *value >= 30
        });

        IterateResult {
            feedback: Pipe::concat(vec![continue_inner, restart_outer]).unwrap(),
            output: done,
        }
    });
    output
        .map("strip_done", |_t, (value, _)| value)
        .output("results")
        .unwrap();
    let logical = builder.build().unwrap();

    let mut spawned = rt.spawn(logical, SpawnOptions::default()).unwrap();
    let sender = spawned.take_input::<(i64, bool)>("data").unwrap();
    let receiver = spawned.take_output::<i64>("results").unwrap();

    sender
        .send(
            Product::new(0u64, 0u32),
            vec![(7, false), (18, false), (28, false)],
        )
        .unwrap();
    drop(sender);

    spawned.join_blocking().unwrap();

    let mut results: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    results.sort();

    assert_eq!(results, vec![30, 30, 30]);
}
