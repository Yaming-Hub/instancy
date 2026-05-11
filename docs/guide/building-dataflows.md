# Building Dataflows

Use `DataflowBuilder` to declare sources, inputs, transforms, branching, aggregation, outputs, and shared configuration. This page covers the day-to-day builder API in one place.

[Back to the guide index](./README.md)

Building a dataflow in instancy follows a consistent pattern:

1. Create a `DataflowBuilder`
2. Define sources and inputs
3. Chain operators to transform data
4. Attach outputs or inspectors
5. Call `build()` to finalize the graph

### Creating Sources

A **source** provides static data that's known at build time:

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("my_pipeline");
let stream = builder.source("events", vec![
    (0u64, vec!["login", "page_view"]),
    (1u64, vec!["click", "purchase"]),
    (2u64, vec!["logout"]),
]);
```

Each entry is a `(timestamp, Vec<data>)` pair. The source emits all data and then closes.

### Creating Inputs

An **input** is a channel that you can feed data into at runtime:

```rust
let input_stream = builder.input::<String>("messages");
```

After spawning the dataflow, you get a sender handle to push data:

```rust
let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
let sender = handle.take_input::<String>("messages").unwrap();

sender.send(0u64, vec!["hello".into(), "world".into()]).unwrap();
sender.send(1u64, vec!["goodbye".into()]).unwrap();
sender.close();  // Signal no more data — this is critical for termination!
```

**Important**: Always close your inputs when done. If you forget, the dataflow will wait forever for more data. Dropping the sender also closes the input.

### Async Sources

An **async source** lets you define the data-producing logic at build time
using an async closure. The runtime manages the producer's lifecycle —
no manual sender management needed:

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("pipeline");
let stream = builder.source_async::<i32, _, _>("events", |sender| async move {
    // Produce data from any async source — database, API, file, etc.
    for batch_id in 0..10u64 {
        let data: Vec<i32> = fetch_batch(batch_id).await;
        sender.send(batch_id, data).await?;
        sender.advance_to(batch_id + 1).await?;
    }
    Ok(())
});
stream.map("process", |_t, x| x * 2).output("results");
```

Key differences from `input()`:
- **Self-contained**: The producer closure runs automatically — no external sender to manage.
- **Backpressure**: `sender.send()` yields when the internal channel is full.
- **Cancellation**: When the dataflow is cancelled, `send()` returns an error.
- **Frontier support**: Call `sender.advance_to(t)` to advance the input frontier, enabling downstream `unary_notify` operators to fire notifications.

The async source works with `RuntimeHandle`; `SimpleRuntime` remains available only for tests behind the `test-utils` feature.

### Observing Outputs

The simplest way to observe data is with a pass-through `map` that logs:

```rust
stream.map("debug", |_time, x| { println!("saw: {x:?}"); x });
```

To collect data for programmatic use, attach an `output`:

```rust
let port = stream.output("results");

// After execution...
let collector = port.collector();
let data = collector.lock().unwrap();
for (time, batch) in data.iter() {
    println!("t={time}: {batch:?}");
}
```

For spawned dataflows, use `take_output` for channel-based collection:

```rust
let receiver = handle.take_output::<i32>("results").unwrap();
let results = receiver.collect_data();  // Drain output before join!
handle.join_blocking().unwrap();
```

**Tip**: Always drain output channels before calling `join_blocking()`. Output channels are bounded — if they fill up and nobody is reading, the dataflow can deadlock.

### Adding Operators

instancy uses a method-chaining API where each operator returns a new `Pipe` that you can chain further:

#### Map

Transform each element:

```rust
stream
    .map("double", |_time, x| x * 2)
    .map("to_string", |_time, x| format!("value: {x}"));
```

The closure receives the timestamp and the owned data element. The return value becomes the new stream element.

#### Filter

Keep elements matching a predicate:

```rust
stream.filter("even_only", |_time, x| x % 2 == 0);
```

Unlike `map`, the predicate receives a reference (`&x`), since filter doesn't transform the data.

#### Take / Take While

Limit the number of elements or stop at a condition:

```rust
// Keep only the first 100 elements (across all timestamps)
stream.take("first_100", 100);

// Keep elements while a condition holds; stop permanently after first failure
stream.take_while("positive", |_time, x| *x > 0);
```

#### Flat Map

Transform each element into zero or more elements:

```rust
stream.flat_map("split_words", |_time, line| {
    line.split_whitespace()
        .map(|w| w.to_lowercase())
        .collect::<Vec<_>>()
});
```

#### Merge

Merge two streams into one:

```rust
let merged = stream_a.merge(stream_b);
```

Both streams must carry the same data type and timestamp type. The merged stream contains elements from both. For merging more than two streams, use the static method:

```rust
let merged = Pipe::concat(vec![stream_a, stream_b, stream_c]);
```

#### Branch (Fan-Out)

Split a stream into multiple downstream branches using `clone()`:

```rust
// Clone the pipe to create independent branches
let evens = stream.clone().filter("evens", |_t, x| x % 2 == 0);
let odds = stream.filter("odds", |_t, x| x % 2 != 0);
```

`clone()` creates a fan-out point — data is duplicated to all downstream consumers. Each branch can then apply its own operators independently.

#### Branch by Predicate

Split a stream into two outputs based on a predicate:

```rust
let (evens, odds) = stream.branch("parity", |_time, x| x % 2 == 0);
evens.map("half", |_t, x| x / 2).output("halved");
odds.output("odd_numbers");
```

Items where the predicate returns `true` go to the first output; `false` items go to the second.

> **Note:** The predicate is evaluated **twice per item** (once for each branch). Use pure, side-effect-free predicates. For stateful routing, compute the classification once with `map` (e.g., tag items as `Result` or an enum) and then split with `branch_result`.

#### Branch by Result

Split a `Result` stream into `Ok` and `Err` branches:

```rust
let results = input.map("parse", |_t, s: String| {
    s.parse::<i64>().map_err(|e| e.to_string())
});
let (ok_values, errors) = results.branch_result("split");
ok_values.output("parsed");
errors.for_each("log_errors", |_t, e| eprintln!("parse error: {e}"));
```

#### Map Batch

Transform an entire batch at once, useful when batch-level context matters:

```rust
stream.map_batch("sort_batch", |_time, mut batch| {
    batch.sort();
    batch
});
```

The closure receives the timestamp and the full `Vec<D>`, returning a new `Vec<D2>`. This is more efficient than per-item `map` when the transformation benefits from seeing all items together (sorting, dedup, windowed aggregations).

#### Inspect — Observing Data

`inspect` and `inspect_batch` are **pass-through** operators: they let you
observe data flowing through without consuming it. The stream continues
downstream unchanged.

```rust
let stream = input
    .inspect("log", |t, x| println!("[t={t}] saw: {x:?}"))
    .map("double", |_t, x| x * 2);  // data keeps flowing
```

Use `inspect_batch` when per-batch efficiency matters (e.g., acquiring a
lock once per batch instead of per element):

```rust
let stream = input
    .inspect_batch("count", |_t, batch| println!("batch size: {}", batch.len()))
    .output("results");
```

#### For Each — Terminal Side-Effects

`for_each` and `for_each_batch` are **terminal** operators: they consume
the stream and do not produce output. Use them for fire-and-forget
side-effects (writing to a database, sending metrics, etc.).

```rust
input
    .map("double", |_t, x| x * 2)
    .for_each("write", |_t, x| db.insert(x));  // no further chaining
```

**Error handling:** If the closure panics, the executor catches it via
`catch_unwind` and converts it to `Error::OperatorPanic`, failing the
dataflow gracefully. For recoverable errors, handle them inside the
closure (e.g., log and continue, or accumulate into a shared error list).

#### Aggregation Operators

Aggregation operators collect all data for a given timestamp and emit a
summary once the timestamp is complete (frontier advances past it). They
use the notification mechanism internally.

**Reduce** — combine all elements into one:

```rust
// Sum all values per timestamp
stream.reduce("sum", |acc, x| acc + x).output("totals");
```

The closure takes two values and returns their combination. Works like
`Iterator::reduce` — if a timestamp has no data, nothing is emitted.

**Fold** — aggregate with an initial value and a different output type:

```rust
// Count elements per timestamp
stream.fold("count", 0usize, |acc, _x| acc + 1).output("counts");

// Collect into a sorted Vec
stream.fold("collect", Vec::new(), |mut acc, x| {
    acc.push(x);
    acc
}).output("collected");
```

Unlike `reduce`, `fold` can change the output type. Both `reduce` and `fold`
only emit for timestamps that received data.

**Distinct** — deduplicate elements per timestamp:

```rust
stream.distinct("dedup").output("unique");
```

Requires `D: Eq + Hash`. Emits each unique value once per timestamp.

**Count** — count elements per timestamp:

```rust
stream.count("count").output("counts");  // Pipe<T, usize>
```

Convenience wrapper around `fold` that returns the element count.

#### Inspect vs Probe — Key Difference

These serve completely different purposes:

| | `inspect` | `probe` |
|---|---|---|
| **Observes** | Data (elements/batches) | Progress (timestamp frontier) |
| **Returns** | `Pipe<T, D>` (pass-through) | `(Pipe<T, D>, ProbeHandle<T>)` |
| **Use case** | Debugging, logging, metrics | Waiting until a timestamp completes |

**`inspect`** answers "what data is flowing through right now?"  
**`probe`** answers "has timestamp X finished processing?"

```rust
// Probe: track progress for coordination
let (stream, probe) = stream.probe();

// Later, check progress:
probe.done_with(&5u64);  // Has the frontier advanced past t=5?
probe.is_done();          // Has all input been processed?
```

### A Worked Example: Streaming Word Count

Here's a complete word count pipeline that demonstrates multiple operators working together:

```rust
use std::collections::{HashMap, HashSet};
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("wordcount");

    let port = builder
        .source("lines", vec![
            (0u64, vec![
                "hello world".to_string(),
                "hello instancy".to_string(),
            ]),
            (1u64, vec![
                "world of dataflow".to_string(),
                "hello world again".to_string(),
            ]),
        ])
        // Split lines into individual words
        .flat_map("split", |_t, line| {
            line.split_whitespace()
                .map(|w| w.to_lowercase())
                .collect::<Vec<_>>()
        })
        // Count word occurrences per timestamp
        .unary("count", {
            let mut counts: HashMap<u64, HashMap<String, usize>> = HashMap::new();
            move |input, output| {
                // Track which timestamps received new data
                let mut dirty = HashSet::new();
                while let Some((time, words)) = input.next() {
                    dirty.insert(time);
                    let map = counts.entry(time).or_default();
                    for word in words {
                        *map.entry(word).or_insert(0) += 1;
                    }
                }
                // Emit counts only for timestamps that changed
                for t in dirty {
                    let map = &counts[&t];
                    let mut pairs: Vec<_> = map.iter()
                        .map(|(k, &v)| (k.clone(), v))
                        .collect();
                    pairs.sort();
                    output.push_vec(t, pairs);
                }
                Ok(())
            }
        })
        .output("counts");

    let dataflow = builder.build().unwrap();
    rt.spawn(dataflow, SpawnOptions::default())
        .unwrap()
        .join_blocking()
        .unwrap();

    let data = port.collector().lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}:");
        for (word, count) in batch {
            println!("  {word}: {count}");
        }
    }
}
```

**Why the `dirty` set?** The `unary` closure may be called multiple times for the same timestamp if data arrives in batches. Without tracking which timestamps received new data, we'd re-emit stale counts for unchanged timestamps. This pattern — accumulate, track dirty, emit only changes — is fundamental to stateful streaming operators.

## Sharing Context with Operators

When building complex dataflows, operators often need access to shared configuration,
schema registries, metrics collectors, or other application-specific state. Rather
than relying on global variables or threading values through every closure, instancy
provides a typed context system on `DataflowBuilder`.

### Setting and Retrieving Context

```rust
use instancy::DataflowBuilder;

struct AppConfig {
    pub batch_size: usize,
    pub threshold: f64,
}

let mut builder = DataflowBuilder::<u64>::new("pipeline");

// Store typed context — wrapped in Arc internally
builder.with_context(AppConfig {
    batch_size: 1024,
    threshold: 0.95,
});

// Retrieve as Arc<T> — cheap to clone and capture in closures
let cfg = builder.get_context::<AppConfig>().unwrap();

let input = builder.input::<f64>("data");
input
    .filter("threshold", move |_t, x| *x > cfg.threshold)
    .output("filtered");
```

### Key Design Points

- **Type-keyed**: Each type `T` maps to one value. Use newtypes to store multiple
  values of the same underlying type (e.g., `struct InputSchema(Schema)` vs
  `struct OutputSchema(Schema)`).
- **Build-time capture**: Call `get_context()` before creating operators, then capture
  the `Arc<T>` in `move` closures. The context is immutable and shared across captures.
- **Survives `build()`**: Context is carried into `LogicalDataflow` and accessible via
  `dataflow.contexts().get::<T>()` for custom materialization logic.
- **Multi-worker friendly**: Each worker's builder call creates its own `Arc`. To share
  a single allocation across workers, use `with_context_arc(existing_arc.clone())`.

### Multi-Worker Example

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::sync::Arc;

struct WorkerConfig { pub multiplier: i32 }

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

// Share a single Arc across all workers with with_context_arc
let shared_cfg = Arc::new(WorkerConfig { multiplier: 10 });

let mut handle = rt.spawn_multi("ctx-demo", 4, |builder| {
    builder.with_context_arc(shared_cfg.clone());
    let cfg = builder.get_context::<WorkerConfig>().unwrap();

    let input = builder.input::<i32>("data");
    input
        .map("scale", move |_t, x| x * cfg.multiplier)
        .output("result");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

## Related Examples

- [`hello_dataflow.rs`](../../instancy/examples/hello_dataflow.rs)
- [`wordcount.rs`](../../instancy/examples/wordcount.rs)
- [`merge_streams.rs`](../../instancy/examples/merge_streams.rs)
- [`branching_pipeline.rs`](../../instancy/examples/branching_pipeline.rs)
- [`async_io_channels.rs`](../../instancy/examples/async_io_channels.rs)

## Next Steps

- Next: [Custom Operators](./custom-operators.md)
- See also: [Multi-Worker Execution](./multi-worker.md), [Observability](./observability.md)
