//! # Delay Operators (Timestamp Reassignment)
//!
//! Demonstrates `delay_batch` and `delay` operators that buffer data
//! and re-assign timestamps. Data is held until the input frontier
//! advances past the new (delayed) timestamp.
//!
//! Use cases:
//! - **Windowing**: group data into fixed time windows
//! - **Time shifting**: push events into the future
//! - **Per-item routing**: assign timestamps based on data content
//!
//! ```bash
//! cargo run --example delay
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    // --- Example 1: Windowing with delay_batch ---
    println!("=== delay_batch: 10-unit windowing ===");
    {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let builder = DataflowBuilder::<u64>::new("windowed_count");
        let input = builder.input::<i32>("events");

        // All events in [0,10) → t=10, [10,20) → t=20, etc.
        input
            .delay_batch("window_10", |t| (t / 10 + 1) * 10)
            .count("per_window")
            .output("counts");

        let dataflow = builder.build().expect("build failed");
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

        let sender = handle.take_input::<i32>("events").unwrap();
        let receiver = handle.take_output::<usize>("counts").unwrap();

        sender.send(0, vec![1]).unwrap();
        sender.send(3, vec![2]).unwrap();
        sender.send(7, vec![3]).unwrap();
        sender.send(12, vec![4]).unwrap();
        sender.send(15, vec![5]).unwrap();
        sender.send(25, vec![6]).unwrap();
        drop(sender);

        handle.join_blocking().expect("execution failed");

        let mut results = receiver.collect_data();
        results.sort_by_key(|(time, _)| *time);
        for (time, batch) in results {
            for count in &batch {
                let label = if *count == 1 { "event" } else { "events" };
                println!("  window ending at t={time}: {count} {label}");
            }
        }
        // Expected: t=10: 3 events, t=20: 2 events, t=30: 1 event
    }

    // --- Example 2: Per-item delay ---
    println!("\n=== delay: priority-based routing ===");
    {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let builder = DataflowBuilder::<u64>::new("priority_delay");
        let input = builder.input::<(&str, i32)>("tasks");

        // Urgent items stay at t=0, normal → t=5, low → t=10
        input
            .delay("prioritize", |t, (priority, _value)| match *priority {
                "urgent" => *t,
                "normal" => *t + 5,
                _ => *t + 10,
            })
            .output("prioritized");

        let dataflow = builder.build().expect("build failed");
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

        let sender = handle.take_input::<(&str, i32)>("tasks").unwrap();
        let receiver = handle.take_output::<(&str, i32)>("prioritized").unwrap();

        sender
            .send(
                0,
                vec![
                    ("urgent", 10),
                    ("normal", 20),
                    ("urgent", 30),
                    ("low", 40),
                ],
            )
            .unwrap();
        drop(sender);

        handle.join_blocking().expect("execution failed");

        let mut results = receiver.collect_data();
        results.sort_by_key(|(time, _)| *time);
        for (time, batch) in results {
            for (priority, value) in &batch {
                println!("  t={time}: {priority} task (value={value})");
            }
        }
        // Expected:
        //   t=0: urgent task (value=10), urgent task (value=30)
        //   t=5: normal task (value=20)
        //   t=10: low task (value=40)
    }

    // --- Example 3: Time shifting + aggregation ---
    println!("\n=== delay_batch + reduce: shifted aggregation ===");
    {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let builder = DataflowBuilder::<u64>::new("shifted_sum");
        let input = builder.input::<i32>("data");

        // Shift everything forward by 100 time units, then sum
        input
            .delay_batch("shift", |t| t + 100)
            .reduce("sum", |a, b| a + b)
            .output("sums");

        let dataflow = builder.build().expect("build failed");
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

        let sender = handle.take_input::<i32>("data").unwrap();
        let receiver = handle.take_output::<i32>("sums").unwrap();

        sender.send(0, vec![10, 20, 30]).unwrap();
        sender.send(1, vec![40, 50]).unwrap();
        drop(sender);

        handle.join_blocking().expect("execution failed");

        let mut results = receiver.collect_data();
        results.sort_by_key(|(time, _)| *time);
        for (time, batch) in results {
            for sum in &batch {
                println!("  t={time}: sum = {sum}");
            }
        }
        // Expected: t=100: sum = 60, t=101: sum = 90
    }
}
