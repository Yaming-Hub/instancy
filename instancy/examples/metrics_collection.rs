//! # Metrics Collection
//!
//! Demonstrates the three levels of metrics collection:
//!
//! 1. **Operator summary** — per-operator activation count, CPU time, records processed
//! 2. **Channel counters** — per-exchange-edge items and bytes transferred
//! 3. **Activation timeline** — per-activation timestamped events for timeline replay
//!
//! Control what is collected via `MetricsConfig` presets:
//! - `MetricsConfig::none()` — zero overhead
//! - `MetricsConfig::summary_only()` — operator stats (~1% overhead)
//! - `MetricsConfig::full()` — all categories including activation timeline
//!
//! Run with: `cargo run --example metrics_collection --all-features`

use std::sync::Arc;
use std::time::Duration;

use instancy::metrics::{DataflowMetrics, MetricsConfig};
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");

    // --- Single-worker dataflow with operator summary ---
    println!("=== Single-Worker: Operator Summary ===\n");
    run_single_worker(&rt);

    // --- Multi-worker dataflow with full metrics (including timeline) ---
    println!("\n=== Multi-Worker: Full Metrics + Timeline ===\n");
    run_multi_worker_timeline(&rt);
}

/// Demonstrates basic operator-level metrics with `collect_metrics(true)`.
fn run_single_worker(rt: &RuntimeHandle) {
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

    print_operator_summary(metrics.as_ref());
}

/// Demonstrates full metrics collection with `MetricsConfig::full()`:
/// operator summary + channel counters + activation timeline.
fn run_multi_worker_timeline(rt: &RuntimeHandle) {
    // MetricsConfig::full() enables all three categories.
    // You can also customize individual fields:
    //
    //   let config = MetricsConfig {
    //       operator_summary: true,
    //       channel_counters: true,
    //       activation_timeline: true,
    //       min_activation_duration: Duration::from_micros(5),  // skip <5µs activations
    //       max_timeline_events: 50_000,                        // ring buffer cap per worker
    //   };
    let opts = SpawnOptions::new().metrics(MetricsConfig::full());

    let mut spawned = rt
        .spawn_multi::<u64, _>(
            "timeline-demo",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .source::<i32>("src", vec![(0u64, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])])
                    .exchange("partition", |x: &i32| *x as u64)
                    .map("square", |_t, x| x * x)
                    .output("out")
                    .unwrap();
                Ok(())
            },
            opts,
        )
        .expect("spawn_multi failed");

    // Drain outputs to let the dataflow complete.
    let receivers = spawned.take_all_outputs::<i32>("out").unwrap();
    for rx in receivers {
        while rx.recv().is_some() {}
    }

    // --- Channel counters ---
    println!("Channel counters:");
    for w in 0..2 {
        if let Some(m) = spawned.worker_mut(w).metrics() {
            for ch in m.channel_snapshots() {
                println!(
                    "  worker {w}: edge[{}] '{}' — {} items, {} bytes",
                    ch.edge_index, ch.label, ch.items_transferred, ch.bytes_transferred
                );
            }
        }
    }

    // --- Activation timeline ---
    println!("\nActivation timeline events:");
    for w in 0..2 {
        if let Some(m) = spawned.worker_mut(w).metrics() {
            let events = m.drain_timeline_events();
            println!("  worker {w}: {} events", events.len());
            // Print first 5 events as a sample.
            for ev in events.iter().take(5) {
                println!(
                    "    op[{}] w{}: {}µs duration @ +{}µs",
                    ev.operator_index, ev.worker_index, ev.duration_us, ev.start_us
                );
            }
            if events.len() > 5 {
                println!("    ... and {} more", events.len() - 5);
            }
        }
    }

    // --- Operator summary (aggregate across workers) ---
    println!("\nOperator summary (per worker):");
    for w in 0..2 {
        if let Some(m) = spawned.worker_mut(w).metrics() {
            print_operator_summary(m);
        }
    }

    spawned.cancel();
    let _ = spawned.join_blocking();
}

fn print_operator_summary(metrics: &DataflowMetrics) {
    let mut operators = metrics.operator_snapshots();
    operators.sort_by_key(|op| op.index);

    println!("  Wall time: {}", format_duration(metrics.wall_time()));
    println!("  Total activations: {}", metrics.total_activations());
    println!(
        "  Total CPU time: {}",
        format_duration(metrics.total_cpu_time())
    );
    println!(
        "  Total records processed: {}",
        metrics.total_records_processed()
    );

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
