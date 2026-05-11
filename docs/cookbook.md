# instancy Cookbook

Practical, copy-paste patterns for common tasks in instancy. Start with the [guide](./guide/README.md) for the concepts, then use this page when you want focused recipes and tuning advice.

Recipes that now live in guide chapters:

- Stateful operators → [Custom Operators](./guide/custom-operators.md)
- Error handling and drain/shutdown → [Error Handling](./guide/error-handling.md)
- Testing → [Testing](./guide/testing.md)
- Monitoring and debugging → [Observability](./guide/observability.md)
- Cluster dataflows → [Distributed Execution](./guide/distributed.md)

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

## See Also

- [Guide](./guide/README.md)
- [API Reference](./reference/api.md)
- [Examples](../instancy/examples/)
