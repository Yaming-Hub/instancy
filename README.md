# instancy

An async reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) in Rust, built on [Tokio](https://tokio.rs/).

instancy retains the core concepts of timely dataflow — timestamps, frontiers, progress tracking, capabilities, and nested scopes — while replacing the execution model with an async worker pool, adding proper error handling, and making networking and serialization pluggable.

## Key Differences from timely-dataflow

| Aspect | timely-dataflow | instancy |
|---|---|---|
| **Execution** | 1 OS thread per worker | Shared async worker pool — multiple dataflows share threads |
| **Networking** | Fixed TCP hostfile | Application provides connections (supports mTLS, pooling) |
| **Serialization** | Hardcoded `Abomonation` | Pluggable `Codec` trait |
| **Error handling** | Panics | `Result<T, Error>` throughout |
| **Cancellation** | Drop the worker | Cooperative `CancellationToken` |
| **Testing** | Requires multiple OS processes | Single-process multi-node testing via in-memory transport |

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
instancy = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Or use the git dependency for the latest development version:

```toml
[dependencies]
instancy = { git = "https://github.com/Yaming-Hub/instancy.git" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

### Hello World

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime init failed");

    let builder = DataflowBuilder::<u64>::new("hello");
    let port = builder
        .source("greetings", vec![
            (0u64, vec!["Hello", "World"]),
            (1u64, vec!["from", "instancy!"]),
        ])
        .output("output");

    let dataflow = builder.build().expect("build failed");
    rt.spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed")
        .join_blocking()
        .expect("execution failed");

    let data = port.collector().lock().unwrap();
    for (time, batch) in data.iter() {
        println!("t={time}: {batch:?}");
    }
}
```

### Streaming Word Count

```rust
use std::collections::{HashMap, HashSet};
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let builder = DataflowBuilder::<u64>::new("wordcount");
    let port = builder
        .source("lines", vec![
            (0u64, vec!["hello world".to_string(), "hello instancy".to_string()]),
            (1u64, vec!["world of dataflow".to_string()]),
        ])
        .flat_map("split", |_t, line| {
            line.split_whitespace().map(String::from).collect()
        })
        .unary("count", {
            let mut counts: HashMap<u64, HashMap<String, usize>> = HashMap::new();
            move |input, output| {
                let mut dirty = HashSet::new();
                while let Some((time, words)) = input.next() {
                    dirty.insert(time);
                    let map = counts.entry(time).or_default();
                    for w in words { *map.entry(w).or_insert(0) += 1; }
                }
                for t in dirty {
                    let map = &counts[&t];
                    let mut pairs: Vec<_> = map.iter()
                        .map(|(k, &v)| (k.clone(), v)).collect();
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

    for (time, batch) in port.collector().lock().unwrap().iter() {
        println!("t={time}: {batch:?}");
    }
}
```

### Spawned Dataflow with Channel I/O

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let builder = DataflowBuilder::<u64>::new("pipeline");
    let input = builder.input::<i32>("numbers");
    input
        .map("double", |_t, x| x * 2)
        .filter("positive", |_t, &x| x > 0)
        .output("results");

    let dataflow = builder.build().unwrap();
    let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

    // Feed data from the main thread
    let sender = handle.take_input::<i32>("numbers").unwrap();
    sender.send(0, vec![1, -2, 3, -4, 5]).unwrap();
    sender.close();

    // Collect results — drain output before joining to avoid backpressure deadlock
    let receiver = handle.take_output::<i32>("results").unwrap();
    let results = receiver.collect_data();

    handle.join_blocking().unwrap();

    for (time, data) in results {
        println!("t={time}: {data:?}");  // t=0: [2, 6, 10]
    }
}
```

## Core Concepts

### Timestamps and Progress

Every data element is associated with a **timestamp** that represents its logical time. Operators track which timestamps they might still produce data for via **capabilities**. The **frontier** — the set of timestamps that could still appear — advances as operators release capabilities, enabling downstream operators to finalize work.

### Operators

instancy provides a focused set of core operators:

| Operator | Description |
|---|---|
| `source` | Emit data from a static collection |
| `input` | Channel-based external input |
| `map` | Transform each element |
| `flat_map` | Transform each element into zero or more |
| `filter` | Keep elements matching a predicate |
| `unary` | General-purpose stateful operator (one input) |
| `binary` | General-purpose stateful operator (two inputs) |
| `unary_notify` | Unary with frontier-based notifications |
| `exchange` / `exchange_by_hash` | Repartition data across workers by key |
| `concat` | Merge two streams |
| `branch` | Split a stream by predicate |
| `iterate` | Feedback loop with nested scope |
| `inspect` | Side-effect observation without modifying data |
| `probe` | Track frontier progress |
| `output` | Collect results |

Higher-level operators (joins, windowing, etc.) can be composed from these primitives in extension crates.

### Execution Modes

**RuntimeHandle** is the production runtime. Create one runtime, then `spawn()` dataflows and `join()` them when you need completion.

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
})?;

// Run to completion (blocking)
rt.spawn(dataflow, SpawnOptions::default())?
    .join_blocking()?;

// Or keep the handle for channel I/O and cancellation
let handle = rt.spawn(dataflow, SpawnOptions::default())?;
```

Use `SpawnOptions` to pick sync or async channel I/O, and pass it to multi-worker execution too:

```rust
let handle = rt.spawn_multi("my-dataflow", 2, |worker_idx, builder| {
    let input = builder.input::<i32>("data");
    input.map("double", |_t, x| x * 2).output("results");
    Ok(())
}, SpawnOptions::default())?;
```

`SimpleRuntime` still exists for tests behind the `test-utils` feature, but production code should use `RuntimeHandle`.

**Cluster mode** — multi-node distributed execution over TCP:

```rust
let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 2),
    NodeConfig::new("node-b", 2),
])?;

// Application provides pre-established connections between nodes.
// See tests/cluster_tcp.rs for complete working examples.
let handle = rt.spawn_cluster(
    "my-cluster-df", topology, "node-a", dataflow_id,
    connections, capacity, handshake_timeout,
    |worker_idx, builder| { /* build graph */ Ok(()) },
    &tokio_handle,
)?;
```

## Features

| Feature | Default | Description |
|---|---|---|
| `transport` | ✅ | TCP transport layer (Tokio-based muxer/demuxer) |
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `bincode-codec` | ❌ | Bincode-based codec implementation |
| `test-utils` | ❌ | Test-only helpers, including `SimpleRuntime` |

Disable default features for a minimal build with no async runtime dependency:

```toml
instancy = { git = "https://github.com/Yaming-Hub/instancy.git", default-features = false }
```

## Networking

instancy delegates connection establishment to the application. You implement a connection provider that returns TCP streams (or any `AsyncRead + AsyncWrite`), and the library handles multiplexing, framing, and progress exchange.

This means:
- **You control TLS/mTLS** — bring your own certificate management
- **You control discovery** — connect via service mesh, DNS, actor framework, etc.
- **Connections are pooled** — multiple dataflows share the same node-to-node connections
- **Testing is easy** — use in-memory duplex streams for single-process cluster tests

## Serialization

The `Codec` trait enables pluggable serialization:

```rust
pub trait Codec<T>: Send + Sync {
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError>;
}
```

Built-in codecs exist for primitive types, tuples, strings, `Vec<u8>`, and `Product` timestamps. Custom types implement `ExchangeData` to participate in cross-worker exchange.

## Examples

Run any example with (from the workspace root):

```bash
cargo run -p instancy --example <name>
```

**Getting Started**

| Example | Description |
|---|---|
| `hello_dataflow` | Minimal source → output pipeline |
| `simple_pipeline` | Multi-stage pipeline with map/filter |
| `spawn_pipeline` | Background execution with channel I/O |
| `async_spawn` | End-to-end async dataflow with async I/O |
| `event_driven` | Real-time event processing with channel-based I/O |

**Operators & Patterns**

| Example | Description |
|---|---|
| `wordcount` | Stateful streaming word count |
| `distinct` | Deduplicate elements per timestamp |
| `hashjoin` | Two-stream hash join |
| `branching_pipeline` | Fan-out: one stream feeding independent pipelines |
| `merge_streams` | Binary and concat operators for merging streams |
| `probe` | Using `ProbeHandle` to observe frontier progress |
| `cancellation` | Cooperative cancellation with `CancellationToken` |

**Multi-Worker & Exchange**

| Example | Description |
|---|---|
| `exchange` | Hash-based data repartitioning across workers |
| `exchange_wordcount` | Multi-worker word count with exchange |
| `notify_wordcount` | Frontier-based aggregation for distributed word count |
| `notify_epoch_stats` | Multi-epoch frontier-based aggregation for statistics |
| `partitioned_workers` | Partitioned input with multiple logical workers |

**Loops & Graph Algorithms**

| Example | Description |
|---|---|
| `loop_demo` | Feedback loop with iterate |
| `pingpong` | Data elements circulating through a feedback loop |
| `barrier` | Progress tracking through many iterations with minimal data |
| `bfs` | Breadth-first search on a graph |
| `pagerank` | Iterative PageRank algorithm |
| `unionfind` | Streaming union-find connected components |

**Runtime**

| Example | Description |
|---|---|
| `runtime_isolation` | Multiple isolated `RuntimeHandle` instances in one process |

## Testing

```bash
# Run all tests
cargo test --all-features -- --test-threads=4

# Run without transport feature
cargo test --no-default-features --features tracing

# Run a specific integration test
cargo test --all-features --test cluster_tcp
```

### Test Organization

| File | Description |
|---|---|
| `tests/cluster.rs` | Multi-node cluster tests with in-memory transport |
| `tests/cluster_tcp.rs` | TCP-based cluster integration tests |
| `tests/parallel_dataflows.rs` | Shared worker pool correctness |
| `tests/parallel_cluster_tcp.rs` | Parallel TCP dataflows on shared connections |
| `tests/multi_dataflow.rs` | Multiple dataflows on one runtime |
| `tests/inter_process.rs` | Cross-process communication |
| `tests/observability.rs` | Metrics and tracing |

## Project Structure

```
instancy/
├── src/
│   ├── lib.rs                    # Public API
│   ├── runtime.rs                # RuntimeHandle, SpawnOptions, test-only SimpleRuntime, spawn_cluster
│   ├── dataflow/
│   │   ├── dataflow_builder.rs   # DataflowBuilder — operator chaining API
│   │   ├── executor.rs           # Async sweep-based executor
│   │   └── channels/             # Exchange, network, pact channels
│   ├── progress/
│   │   ├── subgraph.rs           # ProgressTracker — capability/frontier tracking
│   │   └── frontier.rs           # MutableAntichain
│   ├── communication/
│   │   ├── transport_session.rs  # TCP muxer/demuxer per peer
│   │   ├── control_protocol.rs   # Fingerprint exchange + ready barrier
│   │   └── codec.rs              # Codec trait + built-in implementations
│   └── order.rs                  # Timestamp types (Product for nested scopes)
├── examples/                     # 24 runnable examples
├── tests/                        # Integration tests
└── Cargo.toml
```

## License

MIT
