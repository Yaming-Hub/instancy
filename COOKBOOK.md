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

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::new().collect_metrics(true)).unwrap();
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

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::new().collect_metrics(true)).unwrap();
let metrics = handle.metrics().unwrap().clone();
handle.join_blocking().unwrap();

let mut ops = metrics.operator_snapshots();
ops.sort_by_key(|op| std::cmp::Reverse(op.cpu_time));
for op in ops.iter().take(3) {
    println!("{} cpu={:?} blocked={:?}", op.name, op.cpu_time, op.backpressure.blocked_duration);
}
```

## 7. Common Pitfalls

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
