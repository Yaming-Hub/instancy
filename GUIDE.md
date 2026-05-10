# instancy User Guide

This guide walks you through building streaming dataflow programs with instancy, an async reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow). We'll start with simple examples and progressively introduce more powerful concepts.

If you're familiar with timely-dataflow, you'll find the core ideas — timestamps, frontiers, progress tracking, capabilities — are all here, but the API is different: instancy uses a builder-based chaining API, async execution, and `Result`-based error handling instead of panics.

## Origins

instancy is derived from the ideas and architecture of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow), a pioneering Rust framework for data-parallel dataflow computation created by Frank McSherry. instancy preserves timely's core theoretical model — partially ordered timestamps, progress tracking via pointstamps, frontier-based notifications, and nested scopes — while rearchitecting the runtime and API for modern async Rust.

### Key Differences from Timely

| Aspect | timely-dataflow | instancy |
|--------|----------------|----------|
| **Execution model** | Dedicated sync worker threads (one per worker) | Async task pool (Tokio) — multiple dataflows share a thread pool, enabling better resource utilization |
| **API style** | Closure-based scope nesting (`worker.dataflow(\|scope\| { ... })`) | Builder-based chaining (`DataflowBuilder::new().source(...).map(...).output(...)`) |
| **Error handling** | Panics on most errors | `Result`-based — operators return `Result<()>`, errors propagate cleanly |
| **Cancellation** | No built-in cancellation | Cooperative `CancellationToken` with per-dataflow and per-runtime granularity |
| **Networking** | Built-in TCP with pre-assigned ports | Delegated connection management — caller provides connections (supports SSL, custom topologies, connection pooling) |
| **Serialization** | Abomonation (zero-copy, unsafe) | Pluggable `Codec` trait with safe default; optional bincode via feature flag |
| **Operators** | Large built-in operator library | Focused core set (map, filter, unary, binary, exchange, iterate); composable via extension crates |
| **Multi-dataflow** | One dataflow per worker group | Multiple dataflows share a single runtime and thread pool |
| **Input model** | `InputHandle` with manual timestamp management | `InputSender` with `send(time, data)` and `close()` — also supports async channels |

### What instancy Preserves from Timely

These foundational concepts work the same way:

- **Partially ordered timestamps** — timestamps form a lattice; progress is tracked as antichains (frontiers)
- **Progress tracking** — pointstamp-based protocol ensures operators know when all data for a timestamp has arrived
- **Frontiers and capabilities** — operators hold capabilities that prevent downstream frontiers from advancing until released
- **Nested scopes** — the `iterate` operator creates a sub-scope with `Product<TOuter, TInner>` timestamps, exactly as in timely
- **Exchange (data partitioning)** — hash-based routing of data to specific workers, essential for aggregations and joins
- **Notification pattern** — `unary_notify` with `NotifyContext` mirrors timely's `Notificator` for "emit when epoch is complete" workflows

---

## Table of Contents

0. [Installation](#installation)
1. [Motivation](#1-motivation)
2. [Core Concepts](#2-core-concepts)
3. [Building Dataflows](#3-building-dataflows)
4. [Running Dataflows](#4-running-dataflows)
5. [Creating Custom Operators](#5-creating-custom-operators)
6. [Multi-Worker Execution](#6-multi-worker-execution)
7. [Iteration and Loops](#7-iteration-and-loops)
8. [Distributed Execution](#8-distributed-execution)
9. [Custom Serialization](#9-custom-serialization)

---

## Installation

Add instancy to your `Cargo.toml`:

```toml
[dependencies]
instancy = "0.1"
```

Or use the git dependency for the latest development version:

```toml
[dependencies]
instancy = { git = "https://github.com/Yaming-Hub/instancy.git" }
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `transport` | ✅ | TCP-based cross-node communication |
| `bincode-codec` | ❌ | Built-in bincode serialization for network data |
| `test-utils` | ❌ | Test-only helpers, including `SimpleRuntime` |

To use a specific feature set:

```toml
[dependencies]
instancy = { git = "https://github.com/Yaming-Hub/instancy.git", features = ["bincode-codec"] }
```

---

## 1. Motivation

Streaming dataflow is a way to structure computation as a graph of independent operators connected by typed streams. Each operator processes data as it arrives, without waiting for the entire dataset to be available. This model naturally supports:

- **Incremental computation** — process new data without re-running everything
- **Parallelism** — independent operators run concurrently without explicit synchronization
- **Distribution** — the same graph can execute across multiple machines
- **Iteration** — feedback loops let you express algorithms like PageRank or BFS

instancy makes this accessible in Rust with a clean builder API, proper error handling, and an async execution model where multiple dataflows share a thread pool.

### A Simplest Example

Let's start with the simplest possible instancy program:

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime init failed");

    let builder = DataflowBuilder::<u64>::new("hello");
    builder
        .source("numbers", vec![(0u64, vec![1, 2, 3, 4, 5])])
        .map("print", |_t, x| { println!("seen: {x}"); x })
        .output("sink");

    let dataflow = builder.build().expect("build failed");
    rt.spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed")
        .join_blocking()
        .expect("run failed");
}
```

This creates a stream of numbers and prints each one. Not very different from a simple loop — but the power comes when we make it reactive.

### A Reactive Example

With instancy's `spawn` API, the dataflow runs on a background thread while you feed it data interactively:

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let builder = DataflowBuilder::<u64>::new("reactive");
    let input = builder.input::<i32>("data");
    input
        .map("double", |_t, x| x * 2)
        .map("print", |_t, x| { println!("result: {x}"); x })
        .output("sink");

    let dataflow = builder.build().unwrap();
    let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Feed data at different timestamps
    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.send(1, vec![10, 20]).unwrap();
    sender.close();

    handle.join_blocking().unwrap();
}
```

The dataflow processes each batch as it arrives. The `sender.close()` call tells the dataflow that no more data will come, allowing it to shut down cleanly.

### When to Use instancy

instancy is a good fit when you need:

- **Streaming pipelines** — data arrives continuously and must be processed with low latency
- **Iterative algorithms** — graph algorithms, fixed-point computations, machine learning
- **Parallel data processing** — partition data across workers with exchange operators
- **Distributed computation** — spread work across multiple machines via TCP
- **Multiple concurrent dataflows** — share a thread pool across many independent pipelines

### When NOT to Use instancy

instancy may not be the best fit for:

- **Simple batch processing** — if you can load all data into memory and process it with iterators, do that
- **Request/response servers** — use a web framework like axum or actix instead
- **Single-pass transformations** — if your data flows in one direction with no feedback or coordination, a simple pipeline of iterators is simpler

---

## 2. Core Concepts

### Dataflow

A dataflow program is a directed graph where nodes are **operators** and edges are **streams**. Each operator independently processes data from its input streams and pushes results to its output streams.

```text
[source] → [map] → [filter] → [output]
```

The key property of dataflow is **independence**: operators don't call each other. They react to data arriving on their inputs. This means the runtime can schedule them in any order, on any thread, or even on different machines — as long as the data flows correctly.

### Timestamps

Every piece of data in instancy carries a **timestamp**. Timestamps represent logical time — they could be epoch numbers, iteration counts, or any ordered type.

```rust
// Data at timestamp 0
sender.send(0u64, vec![1, 2, 3]).unwrap();
// Data at timestamp 1
sender.send(1u64, vec![4, 5, 6]).unwrap();
```

Timestamps serve two purposes:

1. **Ordering** — operators can distinguish "earlier" data from "later" data, even if messages arrive out of order
2. **Progress** — the system tracks which timestamps are still possible, enabling operators to know when they've seen everything for a given time

### Progress and Frontiers

The **frontier** at any point in the dataflow is the set of timestamps that might still appear. As an input advances past timestamp 3, operators downstream know they will never see data at timestamps 0, 1, or 2 again — they can finalize any aggregations for those times.

This is the core insight of timely dataflow: **lightweight progress tracking replaces heavyweight synchronization barriers.** Operators don't need to wait for explicit "end of epoch" signals. Instead, the system automatically propagates frontier information through the graph.

### Capabilities

Internally, each operator holds **capabilities** — tokens that represent its ability to produce data at certain timestamps. When an operator is done producing data for timestamp 5, it releases that capability. This release propagates through the graph, advancing frontiers downstream.

You don't need to manage capabilities directly when using the built-in operators. They handle capability lifecycle automatically. When you create custom operators with `unary` or `binary`, capabilities are managed through the `InputHandle` and `OutputHandle` types.

---

## 3. Building Dataflows

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

---

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

---

## 4. Execution

`RuntimeHandle` is the production runtime. Create one runtime, spawn dataflows onto it, and join them when you need completion.

### Running a Dataflow

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

rt.spawn(dataflow, SpawnOptions::default())
    .unwrap()
    .join_blocking()
    .unwrap();
```

### Interactive Channel I/O

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();
let receiver = handle.take_output::<i32>("results").unwrap();

// Feed data...
sender.send(0, vec![1, 2, 3]).unwrap();
sender.close();

// Drain output BEFORE joining
let results = receiver.collect_data();
handle.join_blocking().unwrap();
```

### Shared Runtime

Multiple dataflows can share the same thread pool. This is efficient because idle dataflows do not pin dedicated threads:

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

let h1 = rt.spawn(dataflow1, SpawnOptions::default()).unwrap();
let h2 = rt.spawn(dataflow2, SpawnOptions::default()).unwrap();
let h3 = rt.spawn(dataflow3, SpawnOptions::default()).unwrap();
```

`SpawnOptions` also selects sync versus async channel I/O. `SimpleRuntime` is still available for tests behind the `test-utils` feature, but production code should use `RuntimeHandle`.

### Cancellation

instancy supports cancellation at two levels: **per-dataflow** and **per-runtime**.

#### Cancelling a Single Dataflow

Every `SpawnedDataflow` handle has a `cancel()` method:

```rust
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();

// Feed some data...
sender.send(0, vec![1, 2, 3]).unwrap();

// Cancel this specific dataflow (other dataflows on the same runtime keep running)
handle.cancel();

// join() returns the result — cancellation is not an error, it's a graceful stop
handle.join_blocking().unwrap();
```

This is useful when you want to stop a long-running or streaming dataflow without affecting others on the same runtime.

#### Cancelling All Dataflows (Runtime Shutdown)

To shut down every dataflow on a runtime at once:

```rust
let h1 = rt.spawn(dataflow1, SpawnOptions::default()).unwrap();
let h2 = rt.spawn(dataflow2, SpawnOptions::default()).unwrap();

// Shut down the entire runtime — cancels all running dataflows
rt.shutdown();

h1.join_blocking().unwrap();
h2.join_blocking().unwrap();
```

You can also obtain the runtime's cancellation token and pass it to other threads or async tasks:

```rust
let token = rt.cancel_token().clone();

std::thread::spawn(move || {
    // Some external condition triggers shutdown
    std::thread::sleep(std::time::Duration::from_secs(30));
    token.cancel();  // All dataflows on this runtime shut down gracefully
});
```

Cancellation is **cooperative**: it signals operators at their next check point. Operators wind down in an orderly fashion — they don't get forcibly killed mid-operation.

#### Cancellation Reasons

Every cancellation carries a [`CancellationReason`](instancy::cancellation::CancellationReason) that explains *why* the dataflow was cancelled. This helps distinguish user-initiated stops from system failures:

```rust
use instancy::{CancellationReason, CancellationToken, SpawnOptions};

let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

// Cancel with a specific reason
handle.cancel_with_reason(CancellationReason::UserRequested);

// After join, inspect the cancellation reason
match handle.join_blocking() {
    Err(instancy::error::Error::Cancelled { reason }) => {
        match reason {
            Some(CancellationReason::UserRequested) => println!("User stopped the dataflow"),
            Some(CancellationReason::NetworkError { detail }) => println!("Network failure: {detail}"),
            Some(CancellationReason::WorkerFailed { detail }) => println!("Worker crashed: {detail}"),
            Some(CancellationReason::RuntimeShutdown) => println!("Runtime shut down"),
            Some(CancellationReason::HandleDropped) => println!("Handle was dropped"),
            Some(CancellationReason::OperatorError { detail }) => println!("Operator error: {detail}"),
            None => println!("Cancelled (no reason available)"),
        }
    }
    Ok(()) => println!("Completed normally"),
    Err(e) => println!("Other error: {e}"),
}
```

The built-in reason variants are:

| Variant | When used |
|---------|-----------|
| `UserRequested` | Default for `cancel()` — the caller explicitly requested cancellation |
| `RuntimeShutdown` | The runtime is shutting down (`RuntimeHandle` dropped or `shutdown()` called) |
| `NetworkError(String)` | A network-level error caused cancellation (TCP disconnect, transport failure) |
| `WorkerFailed(String)` | A sibling worker failed, causing cascading cancellation |
| `HandleDropped` | The `SpawnedDataflow` handle was dropped without calling `join()` |
| `OperatorError(String)` | An operator produced an error that caused the dataflow to be cancelled |

Reasons follow **first-cancel-wins** semantics: if a token is cancelled multiple times, only the first reason is recorded. Child tokens inherit their parent's reason.

### Graceful Drain on Cancellation

By default, cancellation stops the dataflow immediately — any in-flight data in channels is lost. For pipelines where you want to finish processing buffered data before stopping, use `drain_on_cancel`:

```rust
use std::time::Duration;
use instancy::SpawnOptions;

let opts = SpawnOptions::new()
    .drain_on_cancel(Duration::from_secs(5));

let handle = rt.spawn(dataflow, opts).unwrap();
```

When cancellation is triggered with drain enabled:

1. **External inputs are closed** — no new data is accepted.
2. **In-flight data continues flowing** through operators normally.
3. **If all operators complete** within the timeout, the dataflow returns successfully (`Ok`).
4. **If the timeout expires**, the dataflow returns `Err(Cancelled)` with the original reason.

This is useful for ETL pipelines, streaming aggregations, or any workflow where partial results are worse than slightly delayed shutdown.

---

## 5. Creating Custom Operators

The built-in operators (`map`, `filter`, `flat_map`) cover simple transformations, but real applications need stateful logic. instancy provides `unary`, `binary`, and `unary_notify` for custom operators.

### Unary: One Input, One Output

`unary` gives you full control over how data flows through a single-input operator:

```rust
stream.unary("my_operator", {
    // State lives here — outside the closure, persisting across activations
    let mut seen: Vec<String> = Vec::new();

    move |input, output| {
        while let Some((time, data)) = input.next() {
            for item in data {
                if !seen.contains(&item) {
                    seen.push(item.clone());
                    output.push(time, item);
                }
            }
        }
        Ok(())
    }
});
```

The closure is called whenever new data is available. It receives:
- `input` — an `InputHandle` that yields `(timestamp, Vec<data>)` batches
- `output` — an `OutputHandle` to push results

**State placement matters.** The state (`seen` above) is defined outside the `move` closure but captured by it. This ensures it persists across invocations. If you defined it inside the closure, it would reset every time.

### Binary: Two Inputs, One Output

`binary` joins two streams in a custom operator:

```rust
let joined = left_stream.binary(right_stream, "join", {
    let mut left_state: HashMap<u64, Vec<(String, i32)>> = HashMap::new();
    let mut right_state: HashMap<u64, Vec<(String, i32)>> = HashMap::new();

    move |left_input, right_input, output| {
        // Drain both inputs
        while let Some((time, data)) = left_input.next() {
            left_state.entry(time).or_default().extend(data);
        }
        while let Some((time, data)) = right_input.next() {
            right_state.entry(time).or_default().extend(data);
        }
        // Join matching timestamps
        for (&t, left_items) in &left_state {
            if let Some(right_items) = right_state.get(&t) {
                for (lk, lv) in left_items {
                    for (rk, rv) in right_items {
                        if lk == rk {
                            output.push(t, (lk.clone(), *lv, *rv));
                        }
                    }
                }
            }
        }
        Ok(())
    }
});
```

### Unary Notify: Frontier-Aware Operators

`unary_notify` adds progress awareness — your closure receives a `NotifyContext` that lets you register interest in timestamps and receive notifications when the frontier advances past them:

```rust
stream.unary_notify("aggregate", {
    let mut pending: HashMap<u64, Vec<i32>> = HashMap::new();

    move |input, output, ctx| {
        // Buffer incoming data and request notifications
        while let Some((time, data)) = input.next() {
            pending.entry(time).or_default().extend(data);
            ctx.notify_at(time);  // "Tell me when this epoch is complete"
        }

        // Process notifications — fired when frontier advances past the time
        while let Some(time) = ctx.next_notification() {
            if let Some(data) = pending.remove(&time) {
                let sum: i32 = data.iter().sum();
                output.push(time, sum);
            }
        }
        Ok(())
    }
});
```

This is the instancy equivalent of timely's `Notificator` pattern. The key methods on `NotifyContext` are:
- `notify_at(time)` — register interest in a timestamp; the framework will notify you when the frontier passes it
- `next_notification()` — returns the next completed timestamp (if any)

Use `unary_notify` when you need to produce output only after all data for a timestamp has arrived — for example, computing aggregates, detecting completeness, or triggering downstream actions.

---

## 6. Multi-Worker Execution

For parallel processing, instancy can run multiple logical workers that partition data across them.

### spawn_multi

`spawn_multi` creates N replicated workers, each running the same dataflow graph but processing different partitions of data:

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

let mut multi = rt.spawn_multi("wordcount", 2, |builder| {
    let input = builder.input::<String>("lines");
    input
        .flat_map("split", |_t, line| {
            line.split_whitespace().map(String::from).collect()
        })
        .exchange_by_hash("partition", |word: &String| {
            // Hash the word to decide which worker handles it
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            word.hash(&mut h);
            h.finish()
        })
        .unary("count", {
            let mut counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            move |input, output| {
                while let Some((time, words)) = input.next() {
                    for w in words {
                        *counts.entry(w).or_insert(0) += 1;
                    }
                }
                // Emit current state
                let pairs: Vec<_> = counts.iter()
                    .map(|(k, v)| (k.clone(), *v)).collect();
                if !pairs.is_empty() {
                    output.push_vec(0, pairs);
                }
                Ok(())
            }
        })
        .output("counts");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

Each worker independently builds and runs the same graph. The `exchange_by_hash` operator is what makes this powerful: it repartitions data across workers by key, ensuring all occurrences of the same word end up at the same worker regardless of which input they came from.

#### Auto-Parallelism

By default, `SpawnOptions` enables **auto-parallelism** — stage 0 parallelism is automatically detected from the number of `input()` and `source_async()` calls in the graph. The `num_workers` argument acts as a minimum floor: `effective = max(auto_detected, num_workers)`.

```rust
// Auto-parallelism: 1 input → auto_detected=1, effective = max(1, 4) = 4 workers
let multi = rt.spawn_multi("pipeline", 4, |builder| {
    builder.input::<i32>("data")
        .map("inc", |_t, x| x + 1)
        .output("out");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

Pass `num_workers=0` to use only the auto-detected count. Disable auto-parallelism with `SpawnOptions::new().auto_parallelism(false)` to use the exact `num_workers` for stage 0. To force uniform parallelism across *all* stages, also set `per_stage_parallelism(false)`.

### Exchange Operators

Exchange operators physically move data between workers based on a routing function:

```rust
// Route by hash of the value
stream.exchange_by_hash("route", |x: &MyType| compute_hash(x));

// Route by a key function (applies DefaultHasher on top)
stream.exchange("route", |x: &MyType| x.key.clone());
```

**When do you need exchange?** Only when your computation requires specific data distribution. For example:
- **Word count** — all occurrences of "hello" must reach the same worker
- **Graph algorithms** — all edges for a vertex must be co-located
- **Joins** — matching keys must meet at the same worker

If your operators are stateless (like `map` or `filter`), you don't need exchange — any worker can process any element.

### Distribution Operators

Beyond `exchange`, instancy provides three convenience distribution operators
for common routing patterns:

#### Gather

Route **all** data to worker 0. Useful for global aggregation:

```rust
stream
    .exchange_by_hash("partition", |x| x.key)
    .gather("collect")
    .reduce("global_sum", |acc, x| acc + x)
    .output("total");
```

After `gather`, only worker 0 has data — other workers receive nothing.

#### Rebalance

Distribute data round-robin across all workers. Useful for evening out load
when key-based partitioning isn't needed:

```rust
// All items from worker 0 get spread evenly across all workers
stream.rebalance("spread").output("results");
```

Unlike `exchange`, `rebalance` doesn't look at the data — it assigns workers
sequentially (item 0 → worker 0, item 1 → worker 1, ..., wrapping around).

#### Rebalance To

Like `rebalance`, but with explicit target parallelism:

```rust
// Round-robin to exactly 4 workers
stream.rebalance_to("spread", 4).output("results");
```

> **Note:** With [`SpawnOptions::per_stage_parallelism`] enabled (the default),
> `rebalance_to(N)` can use a different `N` per stage. With
> `per_stage_parallelism(false)`, `N` must equal the spawned worker count.

#### Broadcast

Clone all data to **every** worker (fan-out). Useful for distributing small
reference data, configuration, or lookup tables that every worker needs:

```rust
// Every worker gets a complete copy of the config stream
let config = config_stream.broadcast("share-config");
config
    .map("use-config", |_t, cfg| apply_config(cfg))
    .output("results");
```

> **Warning:** Broadcast multiplies data volume by the worker count. Only use
> for small datasets or control signals — never for large data streams.

In single-worker mode, `broadcast` is a no-op pass-through (no cloning needed).

### Delay Operators

The `delay` operators buffer data and re-assign timestamps, releasing the data
only when the input frontier advances past the **new** (delayed) timestamp. This is
essential for windowing, time-based aggregation, and ensuring data is processed
in timestamp order.

#### delay_batch

Re-timestamp all data at a given timestamp to a new timestamp computed from the
original timestamp. Simpler version when the delay depends only on the timestamp:

```rust
// Group data into 100-unit windows
let windowed = stream.delay_batch("window-100", |t| (t / 100 + 1) * 100);

// Shift all timestamps forward by 10
let shifted = stream.delay_batch("shift", |t| t + 10);

// Identity: buffer until frontier confirms no more data at t
let ordered = stream.delay_batch("order", |t| *t);
```

#### delay

Per-item re-timestamp: each item can be assigned a different timestamp based on
its content:

```rust
// High-priority items stay at current timestamp; low-priority delayed
let prioritized = stream.delay("prioritize", |t, item| {
    if item.priority > 5 { *t } else { *t + 10 }
});
```

> **Constraint:** The delay function must return a timestamp `>=` the input
> timestamp. Returning an earlier timestamp will panic.

### Cross-Worker Error Propagation

In multi-worker dataflows, if one worker's operator fails, all sibling workers
are automatically cancelled via the built-in **control broadcast channel**.
You don't need to wire up manual error forwarding — instancy handles it:

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut multi = rt.spawn_multi("my-pipeline", 4, |builder| {
    let input = builder.input::<String>("data");
    input.map("process", |_t, line| {
        // If this panics in any worker, all 4 workers cancel promptly.
        parse_line(&line).expect("bad input")
    }).output("result");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

When worker 2's `process` operator panics, instancy:
1. Catches the error and broadcasts a `WorkerControl::WorkerError` signal.
2. Cancels the shared dataflow `CancellationToken`.
3. All other workers see `Err(Cancelled)` on their next sweep and exit.

The `join_blocking()` call returns the first error, with full operator and
worker context attached.

---

## 7. Iteration and Loops

instancy supports iterative computation through the `iterate` operator, which creates a feedback loop in the dataflow graph.

### Basic Iteration

```rust
use instancy::IterateResult;

let result = stream.iterate::<u32>("loop", 1u32, |iter_stream| {
    // Transform data each iteration
    let doubled = iter_stream.map("double", |_t, x| x * 2);

    // Split: values >= 100 exit, values < 100 loop back
    let done = doubled.clone().filter("exit", |_t, &x| x >= 100);
    let again = doubled.filter("continue", |_t, &x| x < 100);

    IterateResult {
        feedback: again,  // Goes back to the start of the loop
        output: done,     // Exits the loop
    }
});
```

The `iterate` operator:
1. Creates a nested scope with an enriched timestamp `Product<TOuter, TInner>` — the outer timestamp is your original timestamp, and the inner is the iteration counter
2. Feeds data into the loop body
3. Each iteration, the `feedback` stream circulates back to the start
4. The `output` stream exits the loop and continues downstream
5. The loop terminates when no more data circulates through `feedback`

The second argument (`1u32`) specifies the increment for the iteration counter each time around the loop.

### Understanding Product Timestamps

Inside an `iterate` loop, timestamps become `Product<TOuter, TInner>`:

```rust
stream.iterate::<u32>("my_loop", 1u32, |iter_stream| {
    iter_stream.map("debug", |time, x| {
        // time is Product<u64, u32>
        // time.outer = original timestamp (e.g., 0, 1, 2)
        // time.inner = iteration number (0, 1, 2, ...)
        println!("iteration {}, original time {}: {x}", time.inner, time.outer);
        x
    });
    // ...
});
```

This is important for stateful operators inside loops — use `time.inner` to know which iteration you're in, and `time.outer` to distinguish different input epochs.

### Iteration with Exchange

You can combine iteration with exchange for distributed iterative algorithms. This requires `Product` timestamps to be serializable, which instancy supports:

```rust
// Inside iterate, exchange data across workers each iteration
let result = stream.iterate::<u32>("distributed_loop", 1u32, |iter_stream| {
    let exchanged = iter_stream.exchange_by_hash("route", |x: &u64| *x);
    let processed = exchanged.map("step", |_t, x| x + 1);
    let done = processed.clone().filter("exit", |_t, &x| x >= threshold);
    let again = processed.filter("continue", |_t, &x| x < threshold);
    IterateResult { feedback: again, output: done }
});
```

This pattern — iterate with exchange — is the basis for graph algorithms like BFS and PageRank. See the `bfs.rs`, `pagerank.rs`, and `unionfind.rs` examples for complete implementations.

---

## 8. Distributed Execution

instancy can distribute computation across multiple machines using peer-to-peer connections. The library is **transport-agnostic** — it works with any reliable, ordered byte stream: TCP, TLS, Unix sockets, named pipes, QUIC, or even in-memory duplex channels. Unlike timely-dataflow's fixed hostfile approach, instancy delegates connection establishment to your application via a `ConnectionFactory`.

### Cluster Topology

First, describe your cluster:

```rust
use instancy::{ClusterTopology, NodeConfig};

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 2),  // 2 workers on node A
    NodeConfig::new("node-b", 2),  // 2 workers on node B
]).unwrap();
```

### Establishing Connections

You provide the connections via a `ConnectionFactory`. This means you control the transport, TLS, authentication, and discovery:

```rust
// Your application establishes connections however it likes:
// - Plain TCP, mTLS, via service mesh, through an actor framework, etc.
// - instancy just needs AsyncRead + AsyncWrite streams

use instancy::communication::transport_session::PeerConnection;

let connections: Vec<PeerConnection<_, _>> = vec![
    PeerConnection {
        peer_node_id: "node-b".to_string(),
        reader: tcp_read_half,
        writer: tcp_write_half,
    },
];
```

#### TLS Example

Since instancy accepts any `AsyncRead + AsyncWrite` stream, you can use TLS (or mTLS) by
establishing TLS connections in your application code and passing the resulting streams:

```rust
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};
use rustls::pki_types::ServerName;
use instancy::communication::transport_session::PeerConnection;
use std::sync::Arc;

// Load your certificates and build a TLS config
let tls_config = rustls::ClientConfig::builder()
    .with_root_certificates(load_ca_certs())       // your CA bundle
    .with_client_auth_cert(client_certs, client_key) // for mTLS
    .unwrap();
let connector = TlsConnector::from(Arc::new(tls_config));

// Establish a TLS connection to a peer node
let tcp_stream = TcpStream::connect("peer-b.example.com:9000").await?;
let server_name = ServerName::try_from("peer-b.example.com")?;
let tls_stream = connector.connect(server_name, tcp_stream).await?;

// Split into read/write halves and hand to instancy
let (reader, writer) = tokio::io::split(tls_stream);

let connections = vec![
    PeerConnection {
        peer_node_id: "node-b".to_string(),
        reader,
        writer,
    },
];

// Pass `connections` to rt.spawn_cluster(...) — instancy uses them as-is.
// It never opens sockets or negotiates TLS itself; that's entirely your responsibility.
```

This pattern works with any TLS library (`tokio-rustls`, `tokio-native-tls`, `s2n-tls-tokio`, etc.)
and any authentication scheme (one-way TLS, mutual TLS, custom certificate validation).

### Spawning a Cluster Dataflow

```rust
use instancy::{RuntimeConfig, RuntimeHandle};
use instancy::dataflow::id::DataflowId;
use std::time::Duration;

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

let dataflow_id = DataflowId::new();
// Requires a Tokio runtime — e.g., use #[tokio::main] or build one manually.
let tokio_handle = tokio::runtime::Handle::current();

let mut cluster_handle = rt.spawn_cluster(
    "my_distributed_df",
    topology,
    "node-a",          // This node's ID
    dataflow_id,
    connections,
    64,                // Channel capacity
    Duration::from_secs(10),  // Handshake timeout
    |builder| {
        // Build the same graph on every node
        let input = builder.input::<String>("data");
        input
            .exchange_by_hash("route", |s: &String| {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                h.finish()
            })
            .unary("process", {
                move |input, output| {
                    while let Some((t, data)) = input.next() {
                        for item in data {
                            output.push(t, item.to_uppercase());
                        }
                    }
                    Ok(())
                }
            })
            .output("results");
        Ok(())
    },
    &tokio_handle,
).unwrap();
```

**Key points:**
- Every node must call `spawn_cluster` with the same `DataflowId` concurrently
- The library performs a handshake to verify all nodes agree on the dataflow structure
- Exchange operators automatically route data across nodes via the pooled connections
- Multiple dataflows can share the same node-to-node connections

### Testing Clusters Locally

You don't need multiple machines to test distributed dataflows. Use in-memory duplex streams:

```rust
use tokio::io::duplex;

// Create a bidirectional in-memory connection
let (a_to_b, b_to_a) = duplex(8192);
let (a_read, a_write) = tokio::io::split(a_to_b);
let (b_read, b_write) = tokio::io::split(b_to_a);
```

This is how instancy's own integration tests work — see `tests/cluster.rs` for examples.

### Handling Node Failures

Cluster health monitoring (heartbeats, liveness probes) is the hosting application's responsibility — instancy does not run its own health checks. When the application detects a peer node is unreachable, it notifies the runtime:

```rust
// Application detects node-3 is unreachable (via its own health monitoring)
let cancelled = runtime.report_node_leave("node-3");
println!("Cancelled {cancelled} dataflows due to node-3 failure");
```

**What happens:**
1. All cluster dataflows with workers on `"node-3"` are cancelled with `CancellationReason::PeerDown { node_id: "node-3".into() }`.
2. Both the local worker executors and network bridge tasks are stopped.
3. `DataflowCompletion` resolves with a cancellation error — the application can match on the `PeerDown` reason.

**No automatic rescheduling:** instancy does not attempt to move computation to surviving nodes. The application retries the dataflow on healthy nodes:

```rust
match cluster_dataflow.join_blocking() {
    Err(e) if e.is_cancelled() => {
        // Check reason
        if let Some(CancellationReason::PeerDown { node_id }) = e.cancellation_reason() {
            println!("Peer {node_id} went down, retrying on healthy nodes...");
            // Rebuild topology without the failed node and re-spawn
        }
    }
    other => { /* handle normally */ }
}
```

**Peer recovery:** If a previously-down peer comes back online, notify the runtime so future dataflows can use it:

```rust
// Application detects node-3 is back online
runtime.report_node_join("node-3");
// Now safe to spawn_cluster with node-3 in the topology again
```

Already-cancelled dataflows are **not** restarted — the application must re-spawn them if desired.

### Connection Failure & Reconnection

When using `SharedTransport` for cluster networking, connection failures are handled automatically if a **connection factory** is provided.

**Automatic reconnection (with factory):**

When a connection drops (reader/writer error), `SharedTransport`:
1. Marks the connection as dead and removes it from the active pool
2. Invokes the connection factory to establish a new connection
3. Retries with exponential backoff: 100ms → 200ms → 400ms → 800ms (5 attempts total, 4 delays between them)
4. On success, the new connection is added to the pool and future sends use it
5. On permanent failure (all retries exhausted with no live connections remaining), affected dataflows receive `TransportError::ConnectionClosed`

**No factory (pre-established connections only):**

If `SharedTransport` is created with pre-established connections and no factory, a dropped connection is permanent — no reconnection is attempted. Remaining healthy connections continue serving traffic.

**Data loss during reconnection:**

Payload frames sent while no live connection exists are **dropped immediately** — `SharedTransport` does not buffer or replay them. Additionally, frames that were assigned a sequence number before the connection failed can create an unrecoverable gap in the receiver's reorder buffer. When the gap times out, the affected dataflow receives `TransportError::ReorderTimeout`.

In summary, a successful reconnect restores connectivity but does **not** guarantee seamless delivery. Applications that require exactly-once or reliable delivery should implement their own acknowledgment/retry protocol at the operator level.

**`PeerConnection` and `TransportSession`:**

These lower-level types represent pre-established connections with no built-in reconnection. Reconnection is handled at the `SharedTransport` layer, which wraps these into a managed, pooled transport.

---

## 9. Custom Serialization

When data crosses worker or node boundaries via exchange operators, it must be serialized. instancy uses a `Codec` trait for this:

```rust
use instancy::communication::codec::{Codec, CodecError};

pub trait Codec<T>: Send + Sync {
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError>;
}
```

### Built-in Codecs

instancy provides codecs for common types:
- Primitive integers (`u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`)
- `String` and `Vec<u8>`
- Tuples `(A, B)` where both components have codecs
- `Product<A, B>` timestamps (for iterate + exchange)

### Implementing ExchangeData

To use your own types with exchange operators, implement `ExchangeData`:

```rust
use instancy::communication::codec::{Codec, CodecError, ExchangeData};

#[derive(Clone, Debug, PartialEq)]
struct MyRecord {
    id: u64,
    name: String,
}

struct MyRecordCodec;

impl Codec<MyRecord> for MyRecordCodec {
    fn encode(&self, value: &MyRecord, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        // Encode id as 8 bytes
        buf.extend_from_slice(&value.id.to_le_bytes());
        // Encode name length + bytes
        let name_bytes = value.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(MyRecord, usize), CodecError> {
        if buf.len() < 16 {
            return Err(CodecError::InsufficientData);
        }
        let id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let name_len = u64::from_le_bytes(buf[8..16].try_into().unwrap()) as usize;
        if buf.len() < 16 + name_len {
            return Err(CodecError::InsufficientData);
        }
        let name = String::from_utf8(buf[16..16 + name_len].to_vec())
            .map_err(|e| CodecError::Custom(e.to_string()))?;
        Ok((MyRecord { id, name }, 16 + name_len))
    }
}

impl ExchangeData for MyRecord {
    type CodecType = MyRecordCodec;
    fn codec() -> Self::CodecType {
        MyRecordCodec
    }
}
```

With this implementation, `MyRecord` can be used with `exchange` and `exchange_by_hash` in multi-worker and cluster mode.

### Using Bincode

If you prefer automatic serialization, enable the `bincode-codec` feature:

```toml
instancy = { git = "https://github.com/Yaming-Hub/instancy.git", features = ["bincode-codec"] }
```

Then use `BincodeCodec` for any type that implements `serde::Serialize + serde::Deserialize`:

```rust
use instancy::communication::codec::BincodeCodec;

impl ExchangeData for MyRecord {
    type CodecType = BincodeCodec<Self>;
    fn codec() -> Self::CodecType {
        BincodeCodec::new()
    }
}
```

---

## Operator Quick Reference

| Category | Operator | Signature | Description |
|----------|----------|-----------|-------------|
| **Transform** | `map` | `\|&T, D\| -> D2` | Transform each element |
| | `flat_map` | `\|&T, D\| -> Vec<D2>` | One-to-many transform |
| | `map_batch` | `\|&T, Vec<D>\| -> Vec<D2>` | Batch-level transform |
| **Filter** | `filter` | `\|&T, &D\| -> bool` | Keep matching elements |
| | `take` | `(name, n)` | Keep first N elements |
| | `take_while` | `\|&T, &D\| -> bool` | Keep while condition holds |
| **Branch** | `branch` | `\|&T, &D\| -> bool` → `(Pipe, Pipe)` | Split by predicate |
| | `branch_result` | `Pipe<T, Result<V,E>>` → `(Pipe<T,V>, Pipe<T,E>)` | Split Ok/Err |
| | `clone()` | | Fan-out to multiple consumers |
| **Observe** | `inspect` | `\|&T, &D\|` | Pass-through data observation |
| | `inspect_batch` | `\|&T, &[D]\|` | Pass-through batch observation |
| **Terminal** | `for_each` | `\|&T, &D\|` | Consume stream (side-effects) |
| | `for_each_batch` | `\|&T, &[D]\|` | Consume batches |
| | `output` | `(name)` | Named output port |
| | `collect` | | Collect into `Arc<Mutex<Vec<(T, Vec<D>)>>>` |
| **Aggregation** | `reduce` | `\|D, D\| -> D` | Combine per timestamp |
| | `fold` | `(init, \|D2, D\| -> D2)` | Fold with initial value |
| | `distinct` | | Deduplicate per timestamp |
| | `count` | | Count per timestamp → `usize` |
| **Delay** | `delay` | `\|&T, &D\| -> T` | Per-item timestamp reassignment |
| | `delay_batch` | `\|&T\| -> T` | Per-timestamp reassignment |
| **Distribution** | `exchange` | `\|&D\| -> K` | Hash-based routing |
| | `exchange_by_hash` | `\|&D\| -> u64` | Direct hash routing |
| | `gather` | | All data → worker 0 |
| | `rebalance` | | Round-robin across workers |
| | `rebalance_to` | `(name, N)` | Round-robin to N workers |
| | `broadcast` | | Clone all data to every worker |
| **Progress** | `probe` | → `(Pipe, ProbeHandle)` | Track frontier progress |
| **Loop** | `iterate` | `(\|Pipe\| -> Pipe, step)` | Feedback loop |
| **Merge** | `merge` | `(other_pipe)` | Merge two streams |
| | `concat` | `(vec_of_pipes)` | Merge multiple streams |
| **Distribution** | `exchange_by_hash_to` | `(\|&D\| -> u64, N)` | Direct hash with target parallelism |
| **Custom** | `unary` | `(InputHandle, OutputHandle) -> Result<()>` | Custom single-input operator |
| | `unary_notify` | `(InputHandle, OutputHandle, NotifyContext) -> Result<()>` | Frontier-aware custom operator |

---

## 10. Troubleshooting

### My dataflow hangs and never completes
- Check that all `InputSender`s are dropped (closing the input)
- Check that `unary_notify` operators consume all notifications (`ctx.next_notification()`)
- Check that capabilities aren't held indefinitely

### Output arrives out of order
- Pipeline channels preserve ordering within a timestamp
- Exchange channels may reorder across workers — use `unary_notify` to aggregate per-timestamp

### How do I know if my dataflow is slow?
- Enable metrics: `SpawnOptions::new().metrics(MetricsConfig::full())`
- Check per-operator CPU time and activation count
- Look for operators with high backpressure blocked_duration
- Use `drain_timeline_events()` to see individual activation timing
- Check `channel_snapshots()` for exchange edge transfer volumes
- See the [metrics_collection example](./instancy/examples/metrics_collection.rs) for a complete walkthrough

---

## What's Next?

- Read the [COOKBOOK.md](./COOKBOOK.md) for copy-paste patterns and troubleshooting-adjacent recipes
- Browse the [examples/](./instancy/examples/) directory for complete runnable programs
- Check the [tests/](./instancy/tests/) directory for integration test patterns
- Read the [DESIGN.md](./DESIGN.md) for architectural details
- Run `cargo doc --open` to explore the API documentation
