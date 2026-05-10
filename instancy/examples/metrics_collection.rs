//! # Metrics Collection
//!
//! Demonstrates enabling per-operator metrics on a production runtime via
//! `SpawnOptions::collect_metrics(true)` and printing a summary table after
//! the dataflow completes.
//!
//! Run with: `cargo run --example metrics_collection --all-features`

use std::sync::Arc;
use std::time::Duration;

use instancy::metrics::DataflowMetrics;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");

    let builder = DataflowBuilder::<u64>::new("metrics_collection");
    let input = builder.input::<i32>("data").unwrap();
    input
        .map("double", |_t, x| x * 2)
        .filter("positive", |_t, x| *x > 0)
        .output("results")
        .unwrap();

    let dataflow = builder.build().expect("build failed");
    let mut handle = rt
        .spawn(dataflow, SpawnOptions::new().collect_metrics(true))
        .expect("spawn failed");

    let sender = handle.take_input::<i32>("data").expect("input port");
    let receiver = handle.take_output::<i32>("results").expect("output port");

    // Clone the Arc so metrics remain available after join_blocking consumes the handle.
    let metrics = Arc::clone(handle.metrics().expect("metrics collection enabled"));

    sender.send(0, vec![1, -2, 3]).expect("send at t=0 failed");
    sender
        .send(1, vec![0, 4, -5, 6])
        .expect("send at t=1 failed");
    sender.send(2, vec![-7, 8, 9]).expect("send at t=2 failed");
    sender.close();

    let results = receiver.collect_data();
    handle.join_blocking().expect("dataflow execution failed");

    assert_eq!(
        results,
        vec![(0, vec![2, 6]), (1, vec![8, 12]), (2, vec![16, 18])]
    );

    print_metrics_summary(metrics.as_ref());
}

fn print_metrics_summary(metrics: &DataflowMetrics) {
    let mut operators = metrics.operator_snapshots();
    operators.sort_by_key(|op| op.index);

    println!("=== Dataflow Metrics ===");
    println!("Wall time: {}", format_duration(metrics.wall_time()));
    println!("Total activations: {}", metrics.total_activations());
    println!(
        "Total CPU time: {}",
        format_duration(metrics.total_cpu_time())
    );
    println!(
        "Total records processed: {}",
        metrics.total_records_processed()
    );

    println!();
    println!("Per-operator breakdown:");
    println!(
        "  {:<20} {:>11}  {:>10}  {:>7}",
        "Operator", "Activations", "CPU Time", "Records"
    );
    println!("  {}", "─".repeat(56));

    for op in operators {
        println!(
            "  {:<20} {:>11}  {:>10}  {:>7}",
            op.name,
            op.activations,
            format_duration(op.cpu_time),
            op.records_processed
        );
    }
}

fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();

    if nanos >= 1_000_000_000 {
        format!("{:.2}s", duration.as_secs_f64())
    } else if nanos >= 1_000_000 {
        format!("{:.2}ms", duration.as_secs_f64() * 1_000.0)
    } else if nanos >= 1_000 {
        format!("{:.2}µs", duration.as_secs_f64() * 1_000_000.0)
    } else {
        format!("{}ns", duration.as_nanos())
    }
}
