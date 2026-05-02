//! # Probe Example
//!
//! Demonstrates using a `ProbeHandle` to observe the input frontier of an
//! operator — tracking progress through the dataflow.
//!
//! ```bash
//! cargo run --example probe
//! ```

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::SimpleRuntime;

fn main() {
    // Build a dataflow with a probe attached after a map operator.
    let builder = DataflowBuilder::<u64>::new("probe_demo");
    let stream = builder.source("events", vec![
        (0u64, vec!["login", "page_view"]),
        (1u64, vec!["click", "purchase"]),
        (2u64, vec!["logout"]),
    ]);

    // Add a pass-through operator so the probe observes an input frontier
    // that actually advances (probes on source operators never advance).
    let stream = stream.map("pass", |_t, x| x);

    // Attach a probe to observe the frontier after this point.
    let (stream, probe) = stream.probe();
    let port = stream.output("sink");

    let dataflow = builder.build().expect("build failed");
    SimpleRuntime::new().run(dataflow).expect("dataflow failed");

    // After execution completes, the probe reflects the final frontier state.
    // In a real application, probes are most useful *during* execution to
    // coordinate progress (e.g., waiting for a specific timestamp to complete).
    println!("Dataflow completed.");
    println!("Probe is_done: {}", probe.is_done());
    println!("Probe done_with(0): {} (frontier advanced past t=0)", probe.done_with(&0u64));
    println!("Probe done_with(1): {} (frontier advanced past t=1)", probe.done_with(&1u64));
    println!("Probe done_with(2): {} (frontier advanced past t=2)", probe.done_with(&2u64));

    // Print collected data
    let collector = port.collector();
    let data = collector.lock().unwrap();
    println!("\nCollected {} batches, {} total events",
        data.len(),
        data.iter().map(|(_, v)| v.len()).sum::<usize>(),
    );
}
