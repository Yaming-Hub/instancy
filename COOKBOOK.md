# instancy Cookbook

Practical, copy-paste patterns for common tasks in instancy. For the concepts behind these recipes, start with [GUIDE.md](./GUIDE.md); for runnable end-to-end programs, browse [instancy/examples/](./instancy/examples/).

## 1. Stateful Operators

### Buffer data until a timestamp is complete
Use `unary_notify` when you need exactly one final result per timestamp.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("epoch-sums");
let input = builder.input::<i32>("numbers");

input.unary_notify("sum_per_epoch", {
    let mut pending: HashMap<u64, Vec<i32>> = HashMap::new();
    move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            pending.entry(time).or_default().extend(data);
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            if let Some(data) = pending.remove(&time) {
                output.push_vec(time, vec![data.into_iter().sum()]);
            }
        }
        Ok(())
    }
});
```

### Keep running state across activations
Put mutable state outside the operator closure so it survives every activation.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("running-counts");
let input = builder.input::<String>("words");

input.unary("running_counts", {
    let mut counts: HashMap<String, usize> = HashMap::new();
    move |input, output| {
        while let Some((time, words)) = input.next() {
            let mut snapshot = Vec::new();
            for word in words {
                let n = counts.entry(word.clone()).or_insert(0);
                *n += 1;
                snapshot.push((word, *n));
            }
            output.push_vec(time, snapshot);
        }
        Ok(())
    }
});
```

### Emit batches or windows once an epoch closes
Accumulate all records for a timestamp, then split them into downstream-sized chunks.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("batch-on-close");
let input = builder.input::<String>("rows");

input.unary_notify("batch_500", {
    let mut pending: HashMap<u64, Vec<String>> = HashMap::new();
    move |input, output, ctx| {
        while let Some((time, rows)) = input.next() {
            pending.entry(time).or_default().extend(rows);
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            if let Some(rows) = pending.remove(&time) {
                for chunk in rows.chunks(500) {
                    output.push_vec(time, chunk.to_vec());
                }
            }
        }
        Ok(())
    }
});
```

## 2. Channel Capacity Tuning

### Set a global default and override hot edges
Use `with_capacity()` for one edge; use `DataflowBuilderConfig` when most edges need the same baseline.

```rust
use instancy::{DataflowBuilder, DataflowBuilderConfig};

let builder = DataflowBuilder::<u64>::with_config(
    "capacity-tuning",
    DataflowBuilderConfig { channel_capacity: 4096 },
);
let input = builder.input::<Vec<u8>>("packets");

input
    .with_capacity(8192)   // big batch producer -> parser
    .map("parse", |_t, bytes| bytes.len())
    .with_capacity(64)     // small buffer for low-latency fan-out
    .map("classify", |_t, len| len > 1024)
    .output("results");
```

### Use small buffers for latency, large buffers for throughput
A good rule of thumb: `32-128` for interactive paths, `1024-8192` for batchy or bursty paths.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("latency-vs-throughput");
let input = builder.input::<i32>("data");

input
    .clone()
    .with_capacity(32)   // surface backpressure quickly
    .map("fast_path", |_t, x| x)
    .output("interactive");

input
    .with_capacity(4096) // absorb bursts before a heavy operator
    .map("batch_path", |_t, x| x * 10)
    .output("bulk");
```

### Confirm backpressure before changing capacities
If `blocked_duration` keeps growing, the next operator is saturated or the buffer is too small.

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::metrics::MetricsConfig;

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::new().metrics(MetricsConfig::summary_only())).unwrap();
let metrics = handle.metrics().unwrap().clone();

handle.join_blocking().unwrap();

for op in metrics.operator_snapshots() {
    if op.backpressure.blocked_duration.as_millis() > 0 {
        println!(
            "{} blocked {} times for {:?}",
            op.name, op.backpressure.blocked_count, op.backpressure.blocked_duration
        );
    }
}
```

## 3. Async Integration

### Feed a dataflow from an async API with `source_async`
Send data, then advance the frontier so downstream `unary_notify` operators can finish the epoch.

```rust
use instancy::DataflowBuilder;

async fn fetch_batch(batch_id: u64) -> instancy::Result<Vec<i32>> { todo!() }

let builder = DataflowBuilder::<u64>::new("api-source");
builder
    .source_async::<i32, _, _>("events", |sender| async move {
        for batch_id in 0..10u64 {
            let batch = fetch_batch(batch_id).await?;
            sender.send(batch_id, batch).await?;
            sender.advance_to(batch_id + 1).await?;
        }
        Ok(())
    })
    .map("normalize", |_t, x| x * 2)
    .output("results");
```

### Poll a database or HTTP endpoint on a timer
Wrap the external client in the async source; instancy handles the backpressure boundary.

```rust
use std::time::Duration;
use instancy::DataflowBuilder;

async fn read_rows() -> instancy::Result<Vec<String>> { todo!() }

let builder = DataflowBuilder::<u64>::new("poll-db");
builder
    .source_async::<String, _, _>("rows", |sender| async move {
        for epoch in 0.. {
            let rows = read_rows().await?;
            if rows.is_empty() { break; }
            sender.send(epoch, rows).await?;
            sender.advance_to(epoch + 1).await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    })
    .output("rows_out");
```

### Make async sources cancellation-aware
Share a `CancellationToken` with the task that owns the runtime, or stop when `send()` starts failing.

```rust
use instancy::{CancellationToken, DataflowBuilder};

async fn poll_external_system() -> instancy::error::Result<Vec<String>> { todo!() }

let cancel = CancellationToken::new();
let builder = DataflowBuilder::<u64>::new("cancel-aware-source");

builder
    .source_async::<String, _, _>("events", move |sender| {
        let cancel = cancel.clone();
        async move {
            let mut epoch = 0;
            while !cancel.is_cancelled() {
                let batch = poll_external_system().await?;
                if sender.send(epoch, batch).await.is_err() { break; }
                sender.advance_to(epoch + 1).await?;
                epoch += 1;
            }
            Ok(())
        }
    })
    .output("events_out");
```

## 4. Error Handling Patterns

### Recover from operator panics instead of crashing the process
Turn panics into `join_blocking()` errors with `catch_panics(true)`.

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let builder = DataflowBuilder::<u64>::new("panic-safe");
builder.catch_panics(true);
builder
    .input::<i32>("data")
    .map("divide", |_t, x| if x == 0 { panic!("boom") } else { 100 / x })
    .output("results");

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let dataflow = builder.build().unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
handle.take_input::<i32>("data").unwrap().send(0, vec![10, 0, 5]).unwrap();
match handle.join_blocking() {
    Ok(()) => unreachable!(),
    Err(err) => eprintln!("pipeline stopped cleanly: {err}"),
}
```

### Propagate recoverable errors through the pipeline
Model failures as `Result<T, E>` when you want dataflow-level recovery instead of immediate shutdown.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("recoverable-errors");
let input = builder.input::<Result<String, String>>("raw");
let parsed = input
    .and_then("parse", |_t, s| s.parse::<i32>().map_err(|e| e.to_string()))
    .filter_ok("positive", |_t, v| *v > 0);
let (good, bad) = parsed.branch_result("split");

good.output("values");
bad.output("errors");
```

### Fail fast and let the runtime cancel sibling workers
Return an `Error` from custom operators for fatal conditions.

```rust
use std::io;
use instancy::{DataflowBuilder, Error};

let builder = DataflowBuilder::<u64>::new("fail-fast");
builder
    .input::<String>("lines")
    .unary("validate", move |input, output| {
        while let Some((time, lines)) = input.next() {
            for line in lines {
                if line.is_empty() {
                    return Err(Error::operator("validate", io::Error::other("empty line")));
                }
                output.push_vec(time, vec![line]);
            }
        }
        Ok(())
    })
    .output("clean");
```

## 5. Testing Dataflows

### Unit test operator logic with `SimpleRuntime`
Enable the `test-utils` feature for lightweight tests: `cargo test --features test-utils`.

```rust
#[cfg(feature = "test-utils")]
#[test]
fn doubles_numbers() {
    use instancy::{DataflowBuilder, SimpleRuntime};

    let rt = SimpleRuntime::new();
    let builder = DataflowBuilder::<u64>::new("unit-test");
    let port = builder
        .source("nums", vec![(0, vec![1, 2, 3])])
        .map("double", |_t, x| x * 2)
        .output("out");

    rt.run(builder.build().unwrap()).unwrap();
    assert_eq!(*port.collector().lock().unwrap(), vec![(0, vec![2, 4, 6])]);
}
```

### Integration test with real runtime handles
Use `RuntimeHandle` when you need spawned inputs/outputs and production-style wiring.

```rust
#[test]
fn end_to_end_runtime_test() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("integration-test");
    builder.input::<i32>("in").map("double", |_t, x| x * 2).output("out");

    let mut handle = rt.spawn(builder.build().unwrap(), SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("in").unwrap();
    let receiver = handle.take_output::<i32>("out").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();
    assert_eq!(receiver.collect_data(), vec![(0, vec![2, 4, 6])]);
}
```

### Test multi-worker dataflows by merging worker outputs
Use `spawn_multi()` plus `take_all_outputs()` and assert on the union of all worker results.

```rust
#[test]
fn counts_across_workers() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt.spawn_multi("mw-test", 2, |_w, builder: &mut DataflowBuilder<u64>| {
        builder.input::<i32>("data").exchange_by_hash("route", |x| *x as u64).output("out");
        Ok(())
    }, SpawnOptions::default()).unwrap();

    let senders = multi.take_all_inputs::<i32>("data").unwrap();
    senders[0].send(0, vec![1, 2]).unwrap();
    senders[1].send(0, vec![3, 4]).unwrap();
    drop(senders);
    let outputs = multi.take_all_outputs::<i32>("out").unwrap();
    multi.join_blocking().unwrap();

    let mut all: Vec<i32> = outputs.into_iter().flat_map(|r| r.collect_data().into_iter().flat_map(|(_, d)| d)).collect();
    all.sort();
    assert_eq!(all, vec![1, 2, 3, 4]);
}
```

### Verify ordering explicitly
Pipeline edges preserve arrival order within a timestamp; for exchange-heavy tests, sort before asserting.

```rust
#[test]
fn pipeline_order_is_stable() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("ordering");
    builder.input::<i32>("in").map("pass", |_t, x| x).output("out");

    let mut handle = rt.spawn(builder.build().unwrap(), SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("in").unwrap();
    let receiver = handle.take_output::<i32>("out").unwrap();
    sender.send(0, vec![3, 1, 2]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();
    assert_eq!(receiver.collect_data(), vec![(0, vec![3, 1, 2])]);
}
```

## 6. Performance Tips

### Reuse buffers inside operator state
Allocate scratch space once, then recycle it on every activation.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("reuse-buffers");
builder.input::<String>("lines").unary("tokenize", {
    let mut scratch = Vec::with_capacity(1024);
    move |input, output| {
        while let Some((time, lines)) = input.next() {
            scratch.clear();
            for line in lines {
                scratch.extend(line.split_whitespace().map(str::to_owned));
            }
            let mut session = output.session(time);
            session.give_iterator(scratch.drain(..));
        }
        Ok(())
    }
});
```

### Emit whole batches when you already have a `Vec`
Prefer `push_vec` for batch output; if you are producing records one-by-one, open an output session and `give()` into it.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("batch-output");
builder.input::<i32>("values").unary("format", move |input, output| {
    while let Some((time, values)) = input.next() {
        let batch: Vec<String> = values.into_iter().map(|v| format!("value={v}")).collect();
        output.push_vec(time, batch);
    }
    Ok(())
});
```

### Keep pipeline edges local; pay for exchange only when you need repartitioning
`exchange_by_hash` is worth it for keyed state, joins, and global aggregation; plain chained operators are cheaper.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("routing");
let input = builder.input::<(String, i32)>("events");

input
    .map("cheap_local_step", |_t, x| x)
    .exchange_by_hash("by_key", |(key, _)| {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        key.hash(&mut h);
        h.finish()
    })
    .output("partitioned");
```

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

## 7. Timing & Delay

### Shift all data forward by a fixed number of epochs
Use `delay_batch` when the new timestamp depends only on the original timestamp.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("shift-forward");
let input = builder.input::<i32>("events");

// Every item at epoch T is re-emitted at epoch T + 5.
input
    .delay_batch("shift-5", |t| t + 5)
    .output("shifted");
```

### Window data into fixed-size epochs
`delay_batch` with a rounding function groups items into windows.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("windowed");
let input = builder.input::<f64>("readings");

// Round each epoch up to the next 100-unit boundary.
input
    .delay_batch("window-100", |t| (t / 100 + 1) * 100)
    .unary_notify("avg-per-window", {
        let mut stash = std::collections::HashMap::<u64, Vec<f64>>::new();
        move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                stash.entry(time).or_default().extend(data);
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(vals) = stash.remove(&time) {
                    let avg = vals.iter().sum::<f64>() / vals.len() as f64;
                    output.push_vec(time, vec![avg]);
                }
            }
            Ok(())
        }
    })
    .output("averages");
```

### Delay individual items based on their content
Use `delay` (not `delay_batch`) when each item's new timestamp depends on its value.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("priority-delay");
let input = builder.input::<(u64, String)>("tasks");

// High-priority items (priority < 10) stay at their epoch;
// low-priority items get pushed forward by their priority value.
input
    .delay("priority-shift", |t, (priority, _msg)| {
        if *priority < 10 { *t } else { t + priority }
    })
    .output("scheduled");
```

## 8. Distribution & Broadcast

### Send reference data to every worker
`broadcast` clones each item to all workers — ideal for config, lookup tables, or small reference datasets.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("shared-config");
let config = builder.input::<(String, String)>("config");

// Every worker gets a full copy of the configuration.
config
    .broadcast("share-config")
    .inspect("log-config", |_t, (k, v)| {
        println!("worker got config: {k}={v}");
    })
    .output("local-config");
```

### Split a stream and process branches differently
Use `branch` with a predicate to route items. Each item goes to exactly one branch.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("split-pipeline");
let input = builder.input::<i32>("numbers");

let (big, small) = input.branch("size-split", |_t, x| *x > 1000);

// Heavy processing for large values.
big.map("expensive-transform", |_t, x| x * x)
   .output("big-results");

// Lightweight path for small values.
small.output("small-values");
```

## 9. Monitoring & Debugging

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

## 10. Graceful Shutdown

### Drain in-flight data before stopping
By default, cancellation drops everything immediately. Use `drain_on_cancel` to let in-flight
data finish processing within a timeout.

```rust
use std::time::Duration;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let builder = DataflowBuilder::<u64>::new("graceful");
let input = builder.input::<i32>("data");
input.map("process", |_t, x| x * 2).output("results");

let dataflow = builder.build().unwrap();
let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
let mut handle = rt.spawn(dataflow, opts).unwrap();

let sender = handle.take_input::<i32>("data").unwrap();
sender.send(0, vec![1, 2, 3]).unwrap();
sender.close(); // Close input so drain can complete.

handle.cancel(); // Triggers drain instead of immediate kill.
let result = handle.join_blocking();
assert!(result.is_ok()); // Data flowed through before shutdown.
```

### Detect drain timeout vs normal completion
When the drain timeout expires (e.g., input stays open), the result is `Err(Cancelled)`.

```rust
use std::time::Duration;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let builder = DataflowBuilder::<u64>::new("timeout-demo");
let input = builder.input::<i32>("data");
input.output("out");

let dataflow = builder.build().unwrap();
let opts = SpawnOptions::new().drain_on_cancel(Duration::from_millis(100));
let mut handle = rt.spawn(dataflow, opts).unwrap();

// Keep sender alive — dataflow can't finish draining.
let _sender = handle.take_input::<i32>("data").unwrap();

handle.cancel();
let result = handle.join_blocking();
assert!(result.is_err()); // Drain timed out → Cancelled.
```

## 11. Cluster Dataflows

### Spawn a two-node cluster with duplex streams

Simulate a multi-node cluster in a single process using `tokio::io::duplex`:

```rust
use std::time::Duration;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result,
    RuntimeConfig, RuntimeHandle, SpawnOptions,
};

fn make_duplex_pair(
    node_a: &str,
    node_b: &str,
    buffer_size: usize,
) -> (
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
) {
    let (a_to_b, b_from_a) = tokio::io::duplex(buffer_size);
    let (b_to_a, a_from_b) = tokio::io::duplex(buffer_size);
    let conn_a = PeerConnection {
        node_id: node_b.to_string(),
        reader: a_from_b,
        writer: a_to_b,
    };
    let conn_b = PeerConnection {
        node_id: node_a.to_string(),
        reader: b_from_a,
        writer: b_to_a,
    };
    (conn_a, conn_b)
}

// Both nodes must call spawn_cluster concurrently.
let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 1),
    NodeConfig::new("node-b", 1),
]).unwrap();
let dataflow_id = DataflowId::new();
let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
    builder.input::<i32>("data").unwrap()
        .map("double", |_t, x| x * 2)
        .output("results").unwrap();
    Ok(())
};

// Spawn each node on a blocking task (handshake blocks the thread).
let rt_a = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let rt_b = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let tokio_handle = tokio::runtime::Handle::current();

// ... spawn_cluster on each node with topology, connections, and a 5s timeout ...
```

### Cluster startup protocol

`spawn_cluster` follows a strict multi-phase protocol before any operators run:

1. **Build** — each node calls the `build` closure to construct its local dataflow
2. **Fingerprint** — compute a hash of the dataflow graph (operator count, edge count, exchange indices)
3. **Handshake** — exchange fingerprints with all peers; fail if any mismatch
4. **Wire channels** — create exchange channels and progress channels backed by the network transport
5. **Ready barrier** — each node sends `Ready` and waits for all peers to confirm before proceeding
6. **Materialize** — create operator tasks and begin execution

Both the handshake and ready barrier use the `handshake_timeout` parameter. If any peer doesn't respond in time, `spawn_cluster` returns `Err(HandshakeError::Timeout)`. No operators are started, and no resources are leaked.

### Collect metrics from a cluster

Enable observability on cluster dataflows with `SpawnOptions::metrics()`:

```rust
use instancy::{SpawnOptions, metrics::MetricsConfig};

let opts = SpawnOptions::new().metrics(MetricsConfig::summary_only());
let mut cluster = rt.spawn_cluster(
    "monitored", topology, "node-a", dataflow_id,
    transport, Duration::from_secs(5), build, &tokio_handle, opts,
).unwrap();

// Access metrics for a specific local worker (0-based).
if let Some(m) = cluster.worker_metrics(0) {
    println!("activations: {}", m.total_activations());
    println!("records: {}", m.total_records_processed());
}

// Or collect metrics from all local workers at once.
for (i, m) in cluster.all_worker_metrics().iter().enumerate() {
    if let Some(m) = m {
        let snaps = m.operator_snapshots();
        for snap in &snaps {
            println!("worker {i} / {}: {} records",
                snap.name, snap.records_processed);
        }
    }
}
```

### Cancel a cluster dataflow

Cancelling one node propagates to all peers via the control channel:

```rust
use instancy::cancellation::CancellationReason;

// Cancel with a reason — all peers receive PeerCancelled.
cluster_a.cancel_with_reason(CancellationReason::UserRequested);

// Or use an external cancellation token via SpawnOptions:
let token = tokio_util::sync::CancellationToken::new();
let opts = SpawnOptions::new().cancellation_token(token.clone());
// Later: token.cancel() cancels the cluster and propagates to peers.
```

## 12. Common Pitfalls

### Pitfall: forgetting to drop the input sender
If the sender stays alive, the runtime assumes more data may still arrive.

```rust
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();

sender.send(0, vec![1, 2, 3]).unwrap();
drop(sender); // or sender.close();

handle.join_blocking().unwrap();
```

### Pitfall: calling `notify_at()` without draining notifications
`notify_at()` holds progress; always pair it with `ctx.next_notification()`.

```rust
input.unary_notify("good_notify", {
    let mut pending = std::collections::HashMap::new();
    move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            pending.entry(time).or_insert_with(Vec::new).extend(data);
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            output.push_vec(time, pending.remove(&time).unwrap_or_default());
        }
        Ok(())
    }
});
```

### Pitfall: holding capabilities too long
For async inputs and async sources, advance the frontier when an epoch is done.

```rust
let sender = handle.take_async_input::<i32>("data").unwrap();

sender.send(0, vec![10, 20]).await.unwrap();
sender.advance_to(1).await.unwrap(); // releases epoch 0
sender.send(1, vec![30]).await.unwrap();
sender.advance_to(2).await.unwrap();
sender.close();
```

### Pitfall: using `unary` when you really need `unary_notify`
`unary` emits partial snapshots; `unary_notify` emits one final answer after the frontier passes.

```rust
input.unary_notify("final_sum", {
    let mut stash = std::collections::HashMap::new();
    move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            stash.entry(time).or_insert(0i32);
            *stash.get_mut(&time).unwrap() += data.into_iter().sum::<i32>();
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            output.push_vec(time, vec![stash.remove(&time).unwrap()]);
        }
        Ok(())
    }
});
```
