//! # Graceful Drain Example
//!
//! Demonstrates `drain_on_cancel`: when a dataflow is cancelled, it finishes
//! processing in-flight data instead of stopping immediately.
//!
//! ```bash
//! cargo run --example graceful_drain
//! ```

use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    // -----------------------------------------------------------------------
    // Example 1: Drain lets in-flight data complete
    // -----------------------------------------------------------------------
    println!("=== Example 1: Drain completes in-flight data ===\n");

    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime");
    let builder = DataflowBuilder::<u64>::new("drain_demo");

    let input = builder.input::<i32>("numbers").unwrap();
    input
        .map("square", |_t, x| {
            println!("  Processing: {x} → {}", x * x);
            x * x
        })
        .output("results").unwrap();

    let dataflow = builder.build().expect("build");

    // Enable drain with a 5-second timeout.
    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
    let mut handle = rt.spawn(dataflow, opts).expect("spawn");

    let sender = handle.take_input::<i32>("numbers").unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();

    // Send data and close input.
    sender.send(0, vec![1, 2, 3, 4, 5]).unwrap();
    sender.close();

    // Cancel after a short delay — drain should still finish.
    std::thread::sleep(Duration::from_millis(50));
    println!("  Cancelling dataflow...");
    handle.cancel();

    // Wait for completion.
    match handle.join_blocking() {
        Ok(()) => println!("  ✓ Dataflow completed successfully (drain worked!)\n"),
        Err(e) => println!("  ✗ Error: {e}\n"),
    }

    // Show results.
    let mut data = receiver.collect_data();
    data.sort_by_key(|(t, _)| *t);
    for (time, batch) in &data {
        println!("  t={time}: {batch:?}");
    }

    // -----------------------------------------------------------------------
    // Example 2: Without drain, cancellation stops immediately
    // -----------------------------------------------------------------------
    println!("\n=== Example 2: Without drain, cancellation is immediate ===\n");

    let rt2 = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime");
    let builder2 = DataflowBuilder::<u64>::new("no_drain_demo");

    let input2 = builder2.input::<i32>("numbers").unwrap();
    input2.map("identity", |_t, x| x);

    let dataflow2 = builder2.build().expect("build");

    // No drain — default behavior.
    let mut handle2 = rt2
        .spawn(dataflow2, SpawnOptions::default())
        .expect("spawn");
    let _sender2 = handle2.take_input::<i32>("numbers").unwrap();

    // Cancel immediately (input still open).
    handle2.cancel();

    match handle2.join_blocking() {
        Ok(()) => println!("  Completed (unexpected)"),
        Err(instancy::Error::Cancelled { reason }) => {
            println!(
                "  ✓ Cancelled immediately: {}",
                reason.map_or("(no reason)".to_string(), |r| r.to_string())
            );
        }
        Err(e) => println!("  Error: {e}"),
    }

    // -----------------------------------------------------------------------
    // Example 3: Drain timeout expiry
    // -----------------------------------------------------------------------
    println!("\n=== Example 3: Drain timeout expires ===\n");

    let rt3 = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime");
    let builder3 = DataflowBuilder::<u64>::new("drain_timeout_demo");

    let input3 = builder3.input::<i32>("numbers").unwrap();
    input3.map("identity", |_t, x| x);

    let dataflow3 = builder3.build().expect("build");

    // Short drain timeout — input never closes so drain can't finish.
    let opts3 = SpawnOptions::new().drain_on_cancel(Duration::from_millis(200));
    let mut handle3 = rt3.spawn(dataflow3, opts3).expect("spawn");
    let _sender3 = handle3.take_input::<i32>("numbers").unwrap();

    println!("  Cancelling with 200ms drain timeout (input never closed)...");
    handle3.cancel();

    match handle3.join_blocking() {
        Ok(()) => println!("  Completed (unexpected)"),
        Err(instancy::Error::Cancelled { .. }) => {
            println!("  ✓ Drain timed out → Cancelled (as expected)");
        }
        Err(e) => println!("  Error: {e}"),
    }

    println!("\nDone.");
}
