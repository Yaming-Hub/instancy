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

instancy is not yet published on crates.io. Add it as a git dependency in your `Cargo.toml`:

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
| `async-io` | ❌ | Use async-io instead of tokio for transport I/O |

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
use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("hello");
    builder
        .source("numbers", vec![(0u64, vec![1, 2, 3, 4, 5])])
        .map("print", |_t, x| { println!("seen: {x}"); x })
        .output("sink");

    let dataflow = builder.build().expect("build failed");
    SimpleRuntime::new().run(dataflow).expect("run failed");
}
```

This creates a stream of numbers and prints each one. Not very different from a simple loop — but the power comes when we make it reactive.

### A Reactive Example

With instancy's `spawn` API, the dataflow runs on a background thread while you feed it data interactively:

```rust
use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

fn main() {
    let builder = DataflowBuilder::<u64>::new("reactive");
    let input = builder.input::<i32>("data");
    input
        .map("double", |_t, x| x * 2)
        .map("print", |_t, x| { println!("result: {x}"); x })
        .output("sink");

    let dataflow = builder.build().unwrap();
    let mut handle = SimpleRuntime::new().spawn(dataflow).unwrap();
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
let mut handle = SimpleRuntime::new().spawn(dataflow).unwrap();
let sender = handle.take_input::<String>("messages").unwrap();

sender.send(0u64, vec!["hello".into(), "world".into()]).unwrap();
sender.send(1u64, vec!["goodbye".into()]).unwrap();
sender.close();  // Signal no more data — this is critical for termination!
```

**Important**: Always close your inputs when done. If you forget, the dataflow will wait forever for more data. Dropping the sender also closes the input.

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

#### Inspect

For debugging, you can use `map` as a pass-through that logs:

```rust
stream.map("debug", |time, x| {
    println!("[t={time}] processing: {x:?}");
    x  // pass through unchanged
});
```

#### Probe

Track the progress frontier at a point in the dataflow:

```rust
let (stream, probe) = stream.probe();

// Later, check progress:
probe.done_with(&5u64);  // Has the frontier advanced past t=5?
probe.is_done();          // Has all input been processed?
```

### A Worked Example: Streaming Word Count

Here's a complete word count pipeline that demonstrates multiple operators working together:

```rust
use std::collections::{HashMap, HashSet};
use instancy::DataflowBuilder;
use instancy::SimpleRuntime;

fn main() {
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
    SimpleRuntime::new().run(dataflow).unwrap();

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

## 4. Running Dataflows

instancy provides two runtime modes depending on your needs.

### SimpleRuntime: Synchronous Execution

`SimpleRuntime` runs a dataflow on a single thread. It's the easiest way to get started:

```rust
use instancy::SimpleRuntime;

// Run to completion — blocks until all data is processed
SimpleRuntime::new().run(dataflow).expect("execution failed");
```

For interactive use, `spawn` runs the dataflow on a background thread:

```rust
let mut handle = SimpleRuntime::new().spawn(dataflow).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();
let receiver = handle.take_output::<i32>("results").unwrap();

// Feed data...
sender.send(0, vec![1, 2, 3]).unwrap();
sender.close();

// Drain output BEFORE joining
let results = receiver.collect_data();
handle.join_blocking().unwrap();
```

### RuntimeHandle: Multi-Worker Async Execution

For production use, `RuntimeHandle` provides a shared worker thread pool:

```rust
use instancy::{RuntimeConfig, RuntimeHandle};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();
```

Multiple dataflows share the same thread pool. This is efficient because idle dataflows don't consume threads:

```rust
// Spawn several independent dataflows on the same pool
let h1 = rt.spawn(dataflow1).unwrap();
let h2 = rt.spawn(dataflow2).unwrap();
let h3 = rt.spawn(dataflow3).unwrap();
```

### Cancellation

instancy supports cancellation at two levels: **per-dataflow** and **per-runtime**.

#### Cancelling a Single Dataflow

Every `SpawnedDataflow` handle has a `cancel()` method:

```rust
let mut handle = rt.spawn(dataflow).unwrap();
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
let h1 = rt.spawn(dataflow1).unwrap();
let h2 = rt.spawn(dataflow2).unwrap();

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
use instancy::{CancellationToken, CancellationReason};

let mut handle = rt.spawn(dataflow).unwrap();

// Cancel with a specific reason
handle.cancel_with_reason(CancellationReason::UserRequested);

// After join, inspect the cancellation reason
match handle.join_blocking() {
    Err(instancy::error::Error::Cancelled { reason }) => {
        match reason {
            Some(CancellationReason::UserRequested) => println!("User stopped the dataflow"),
            Some(CancellationReason::NetworkError(msg)) => println!("Network failure: {msg}"),
            Some(CancellationReason::WorkerFailed(msg)) => println!("Worker crashed: {msg}"),
            Some(CancellationReason::RuntimeShutdown) => println!("Runtime shut down"),
            Some(CancellationReason::HandleDropped) => println!("Handle was dropped"),
            Some(CancellationReason::OperatorError(msg)) => println!("Operator error: {msg}"),
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
use instancy::{RuntimeConfig, RuntimeHandle};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

let mut multi = rt.spawn_multi("wordcount", 2, |worker_idx, builder| {
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
}).unwrap();
```

Each worker independently builds and runs the same graph. The `exchange_by_hash` operator is what makes this powerful: it repartitions data across workers by key, ensuring all occurrences of the same word end up at the same worker regardless of which input they came from.

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

instancy can distribute computation across multiple machines using TCP connections. Unlike timely-dataflow's fixed hostfile approach, instancy delegates connection establishment to your application.

### Cluster Topology

First, describe your cluster:

```rust
use instancy::execute::{ClusterTopology, NodeConfig};

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 2),  // 2 workers on node A
    NodeConfig::new("node-b", 2),  // 2 workers on node B
]).unwrap();
```

### Establishing Connections

You provide the TCP connections. This means you control TLS, authentication, and discovery:

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
    |worker_idx, builder| {
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
- Exchange operators automatically route data across nodes via the TCP connections
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

## What's Next?

- Browse the [examples/](./instancy/examples/) directory for complete runnable programs
- Check the [tests/](./instancy/tests/) directory for integration test patterns
- Read the [DESIGN.md](./DESIGN.md) for architectural details
- Run `cargo doc --open` to explore the API documentation
