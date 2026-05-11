# Observability

instancy includes metrics, probe handles, tracing integration, and timeline exports for understanding behavior in production and during local debugging. This page gathers the monitoring and profiling workflows in one place.

[Back to the guide index](./README.md)

### Log every item passing through a pipeline stage
`inspect` observes items without modifying the stream. Chain it anywhere for debugging.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("debug-pipeline");
let input = builder.input::<i32>("data");

input
    .inspect("before-filter", |t, x| eprintln!("[t={t}] input: {x}"))
    .filter("positive", |_t, x| *x > 0)
    .inspect("after-filter", |t, x| eprintln!("[t={t}] kept: {x}"))
    .map("double", |_t, x| x * 2)
    .output("results");
```

### Count items per epoch with inspect
Accumulate side-effects in the closure's captured state.

```rust
use instancy::DataflowBuilder;
use std::sync::{Arc, Mutex};

let builder = DataflowBuilder::<u64>::new("counter");
let input = builder.input::<String>("events");
let counts = Arc::new(Mutex::new(std::collections::HashMap::<u64, usize>::new()));

let counts_ref = counts.clone();
input
    .inspect("count", move |t, _item| {
        *counts_ref.lock().unwrap().entry(*t).or_default() += 1;
    })
    .output("passthrough");

// After dataflow completes, `counts` has per-epoch totals.
```

### Track dataflow progress with a probe
`probe()` returns a handle you can poll from outside the dataflow to check completion.

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let builder = DataflowBuilder::<u64>::new("probed");
let input = builder.input::<i32>("data");

let (stream, probe) = input.map("double", |_t, x| x * 2).probe();
stream.output("results");

let dataflow = builder.build().unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();
sender.send(0, vec![1, 2, 3]).unwrap();
sender.close();

handle.join_blocking().unwrap();
assert!(probe.is_done());
```

## Performance-Focused Instrumentation

### Profile bottlenecks with built-in metrics
Sort operators by CPU time or backpressure before changing code.

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::metrics::MetricsConfig;

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
// MetricsConfig::full() enables operator summary + channel counters + all timelines.
// Use MetricsConfig::summary_only() for lower overhead (operator stats only).
let mut handle = rt.spawn(dataflow, SpawnOptions::new().metrics(MetricsConfig::full())).unwrap();
let metrics = handle.metrics().unwrap().clone();
handle.join_blocking().unwrap();

let mut ops = metrics.operator_snapshots();
ops.sort_by_key(|op| std::cmp::Reverse(op.cpu_time));
for op in ops.iter().take(3) {
    println!("{} cpu={:?} blocked={:?}", op.name, op.cpu_time, op.backpressure.blocked_duration);
}
```

### Collect activation timeline for post-execution analysis
Record per-activation timestamped events for visualization in Perfetto UI or custom tools.

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::metrics::MetricsConfig;

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let config = MetricsConfig {
    activation_timeline: true,
    min_activation_duration: std::time::Duration::from_micros(5), // skip tiny activations
    max_timeline_events: 50_000,  // ring buffer cap per worker
    ..MetricsConfig::full()
};
let mut spawned = rt.spawn_multi::<u64, _>("profiled", 4, build_fn, SpawnOptions::new().metrics(config)).unwrap();

// ... run dataflow ...

for w in 0..4 {
    if let Some(m) = spawned.worker_mut(w).metrics() {
        let events = m.drain_timeline_events(); // sorted by start_us
        for ev in &events {
            println!("op[{}] w{}: {}µs @ +{}µs", ev.operator_index, ev.worker_index, ev.duration_us, ev.start_us);
        }
    }
}
```

### Inspect exchange channel traffic
Check per-edge transfer volumes to identify data-skew hotspots.

```rust
// Requires MetricsConfig with channel_counters: true (included in full()).
for ch in metrics.channel_snapshots() {
    println!("{}: {} items, {} bytes", ch.label, ch.items_transferred, ch.bytes_transferred);
}
println!("Total items across all edges: {}", metrics.total_items_transferred());
```

### Collect frontier advance events
Track when each operator's input frontier advances — useful for debugging
progress stalls or understanding data flow timing.

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::metrics::MetricsConfig;

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let config = MetricsConfig {
    frontier_timeline: true,
    ..MetricsConfig::none()
};
let mut handle = rt.spawn(dataflow, SpawnOptions::new().metrics(config)).unwrap();
let metrics = handle.metrics().unwrap().clone();
// ... send data, close inputs ...
handle.join_blocking().unwrap();

let events = metrics.drain_frontier_events(); // sorted by timestamp_us
for ev in &events {
    println!("op[{}] w{}: frontier → {} @ +{}µs",
        ev.operator_index, ev.worker_index, ev.new_frontier, ev.timestamp_us);
}
```

### Export Chrome Trace for Perfetto UI
Save collected metrics as Chrome Trace JSON for visual timeline analysis.
Requires the `chrome-trace` feature flag.

```rust
// After dataflow completes, export from any worker's metrics:
let metrics = handle.metrics().unwrap();
let exporter = metrics.drain_to_chrome_trace("my-dataflow");
exporter.save("trace.json").unwrap();
// Drag trace.json onto https://ui.perfetto.dev/ to visualize.
```

For fine-grained control, build the exporter manually with frontiers and transfers:

```rust
use instancy::metrics::chrome_trace::ChromeTraceExporter;

let activations = metrics.drain_timeline_events();
let frontiers = metrics.drain_frontier_events();
let transfers = metrics.drain_transfer_events();
let operators: Vec<_> = metrics.operator_snapshots()
    .into_iter()
    .map(|op| (op.index, op.name.clone()))
    .collect();

let exporter = ChromeTraceExporter::new("my-dataflow")
    .with_activations(&activations, &operators)
    .with_channels(&metrics.channel_snapshots())
    .with_frontiers(&frontiers, &operators)
    .with_transfers(&transfers)
    .with_metadata(num_workers);
exporter.save("trace.json").unwrap();
```

## Diagnosing Slow Dataflows

### How do I know if my dataflow is slow?
- Enable metrics: `SpawnOptions::new().metrics(MetricsConfig::full())`
- Check per-operator CPU time and activation count
- Look for operators with high backpressure blocked_duration
- Use `drain_timeline_events()` to see individual activation timing
- Check `channel_snapshots()` for exchange edge transfer volumes
- See the [metrics_collection example](./instancy/examples/metrics_collection.rs) for a complete walkthrough

## Related Examples

- [`metrics_collection.rs`](../../instancy/examples/metrics_collection.rs)
- [`probe.rs`](../../instancy/examples/probe.rs)
- [`notify_epoch_stats.rs`](../../instancy/examples/notify_epoch_stats.rs)
- [`stress_test.rs`](../../instancy/examples/stress_test.rs)

## Next Steps

- Next: [Testing](./testing.md)
- See also: [Error Handling](./error-handling.md), [Cookbook](../cookbook.md)
