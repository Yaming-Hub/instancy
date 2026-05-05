//! # Event-Driven Pipeline
//!
//! Demonstrates real-time event processing using `RuntimeHandle::spawn()`
//! with channel-based I/O. Events arrive with meaningful timestamps
//! representing time windows (e.g., seconds since epoch).
//!
//! This models a sensor data pipeline:
//! - External events arrive at the main thread
//! - They're sent into a spawned dataflow for processing
//! - Results are collected from the output channel
//!
//! Demonstrates:
//! - `spawn()` for channel-based I/O
//! - Real timestamps (time windows, not just 0, 1, 2)
//! - `filter` + `map` for event transformation
//!
//! ```bash
//! cargo run --example event_driven
//! ```

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Simulated sensor reading
#[derive(Clone, Debug)]
struct SensorReading {
    sensor_id: u32,
    value: f64,
}

fn main() {
    println!("=== Event-Driven Sensor Pipeline ===\n");

    // Build: input → filter(high values) → format → output
    let builder = DataflowBuilder::<u64>::new("sensor_pipeline");
    let input = builder.input::<SensorReading>("readings");

    input
        // Only keep readings above threshold
        .filter("high_values", |_t, r| r.value > 50.0)
        // Format into alert strings
        .map("format_alert", |t, r| {
            format!(
                "[window={t}] ALERT: sensor {} reading {:.1} exceeds threshold",
                r.sensor_id, r.value
            )
        })
        .output("alerts");

    let dataflow = builder.build().expect("build failed");
    println!(
        "Built '{}' ({} operators)\n",
        dataflow.name(),
        dataflow.operator_count(),
    );

    // Spawn on background thread
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");
    let mut handle = rt
        .spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed");
    let sender = handle.take_input::<SensorReading>("readings").unwrap();

    // Simulate sensor readings arriving in time windows.
    // Timestamps represent 10-second windows (e.g., epoch seconds / 10).
    let window_1000 = 1000u64; // window starting at t=10000s
    let window_1001 = 1001u64; // next 10-second window

    println!("Sending readings for window {window_1000}...");
    sender
        .send(
            window_1000,
            vec![
                SensorReading {
                    sensor_id: 1,
                    value: 23.5,
                }, // below threshold
                SensorReading {
                    sensor_id: 2,
                    value: 78.3,
                }, // above threshold
                SensorReading {
                    sensor_id: 3,
                    value: 91.0,
                }, // above threshold
                SensorReading {
                    sensor_id: 4,
                    value: 45.2,
                }, // below threshold
            ],
        )
        .unwrap();

    println!("Sending readings for window {window_1001}...");
    sender
        .send(
            window_1001,
            vec![
                SensorReading {
                    sensor_id: 1,
                    value: 55.0,
                }, // above threshold
                SensorReading {
                    sensor_id: 2,
                    value: 12.1,
                }, // below threshold
                SensorReading {
                    sensor_id: 5,
                    value: 99.9,
                }, // above threshold
            ],
        )
        .unwrap();

    println!("Closing input...\n");
    sender.close();

    // Collect alerts
    let receiver = handle.take_output::<String>("alerts").unwrap();
    let results = receiver.collect_data();
    for (_time, alerts) in &results {
        for alert in alerts {
            println!("{alert}");
        }
    }

    handle.join_blocking().expect("dataflow completed");
    println!("\nPipeline completed successfully!");
}
