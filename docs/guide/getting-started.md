# Getting Started

This page gets you from `Cargo.toml` to a running instancy program. It also introduces the runtime handles you'll use for interactive inputs, outputs, and shared execution.

[Back to the guide index](./README.md)

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

## Motivation

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

## Running Dataflows

`RuntimeHandle` is the production runtime. Build a `LogicalDataflow`, spawn it, then either wait for completion or keep the handle around for interactive I/O and cancellation.

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

## Related Examples

- [`hello_dataflow.rs`](../../instancy/examples/hello_dataflow.rs)
- [`simple_pipeline.rs`](../../instancy/examples/simple_pipeline.rs)
- [`spawn_pipeline.rs`](../../instancy/examples/spawn_pipeline.rs)
- [`event_driven.rs`](../../instancy/examples/event_driven.rs)

## Next Steps

- Next: [Core Concepts](./core-concepts.md)
- See also: [Building Dataflows](./building-dataflows.md), [API Reference](../reference/api.md)
