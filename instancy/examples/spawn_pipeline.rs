//! Example: Spawning a dataflow with channel-based I/O.
//!
//! Demonstrates the `SimpleRuntime::spawn()` API where the dataflow runs on a background
//! thread and the main thread feeds data through input channels and
//! collects results from output channels.
//!
//! ```text
//! [main thread]                      [background thread]
//!     │                                     │
//!     │── input::<i32>("numbers") ──────────►│ ChannelSource
//!     │                                     │     │
//!     │                                     │  map("double")
//!     │                                     │     │
//!     │                                     │  filter("div_by_3")
//!     │                                     │     │
//!     │◄── output::<i32>("results") ────────│ ChannelSink
//!     │                                     │
//! ```
//!
//! Run with: `cargo run --example spawn_pipeline`

use instancy::DataflowBuilder;
use instancy::{SimpleRuntime, SpawnedDataflow};

fn main() {
    println!("=== Spawn Pipeline Example ===\n");

    // Phase 1: Build the logical dataflow graph (no execution yet)
    let builder = DataflowBuilder::<u64>::new("spawn_demo");
    let input = builder.input::<i32>("numbers");
    input
        .map("double", |_t, x| x * 2)
        .filter("div_by_3", |_t, x| x % 3 == 0)
        .output("results");

    let dataflow = builder.build().expect("build failed");
    println!(
        "Built dataflow '{}' with {} operators and {} edges\n",
        dataflow.name(),
        dataflow.operator_count(),
        dataflow.edge_count(),
    );

    // Phase 2: Spawn on a background thread via SimpleRuntime
    let rt = SimpleRuntime::new();
    let mut handle: SpawnedDataflow<u64> = rt.spawn(dataflow).expect("spawn failed");
    println!("Dataflow spawned on background thread\n");

    // Phase 3: Feed data through the input channel
    let sender = handle.take_input::<i32>("numbers").expect("input port");

    println!("Sending batch at t=0: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    sender.send(0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();

    println!("Sending batch at t=1: [11, 12, 13, 14, 15]");
    sender.send(1, vec![11, 12, 13, 14, 15]).unwrap();

    println!("Closing input...\n");
    sender.close();

    // Phase 4: Collect results from the output channel
    let receiver = handle.take_output::<i32>("results").expect("output port");
    let results = receiver.collect_data();

    for (time, data) in &results {
        println!("t={time}: {data:?}");
    }

    // Phase 5: Wait for completion
    handle.join_blocking().expect("dataflow completed");
    println!("\nDataflow completed successfully!");
}
