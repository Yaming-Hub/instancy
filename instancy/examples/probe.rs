//! # Probe Example
//!
//! Demonstrates using a `ProbeHandle` to observe the input frontier of a
//! sink operator — tracking progress through the dataflow.
//!
//! ```bash
//! cargo run --example probe
//! ```

use instancy::dataflow::builder::{build_and_run, BuilderConfig};

fn main() {
    // Build a dataflow with a probe attached to the sink.
    // The probe lets us observe the frontier after execution completes.
    let (collector, probe) = build_and_run::<u64, _, _>(BuilderConfig::default(), |ctx| {
        let source = ctx.add_source("events", vec![
            (0u64, vec!["login", "page_view"]),
            (1u64, vec!["click", "purchase"]),
            (2u64, vec!["logout"]),
        ]);
        let (sink_idx, collector) = ctx.add_sink::<&str>("sink", source);

        // Attach a probe to observe the sink's input frontier.
        let probe = ctx.add_probe(sink_idx);
        Ok((collector, probe))
    })
    .expect("dataflow failed");

    // After execution, the probe reflects the final frontier state.
    println!("Dataflow completed.");
    println!("Probe is_done: {}", probe.is_done());
    println!("Probe done_with(0): {}", probe.done_with(&0u64));
    println!("Probe done_with(1): {}", probe.done_with(&1u64));
    println!("Probe done_with(2): {}", probe.done_with(&2u64));

    // Print collected data
    let data = collector.lock().unwrap();
    println!("\nCollected {} batches, {} total events",
        data.len(),
        data.iter().map(|(_, v)| v.len()).sum::<usize>(),
    );
}
