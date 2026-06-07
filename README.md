# instancy

An async reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) in Rust, built on [Tokio](https://tokio.rs/).

instancy retains the core concepts of timely dataflow — timestamps, frontiers, progress tracking, capabilities, and nested scopes — while replacing the execution model with an async worker pool, adding proper error handling, and making networking and serialization pluggable.

## Key Differences from timely-dataflow

| Aspect | timely-dataflow | instancy |
|---|---|---|
| **Execution** | 1 OS thread per worker | Shared async worker pool — multiple dataflows share threads |
| **Networking** | Fixed TCP hostfile | Application provides connections (supports mTLS, pooling) |
| **Serialization** | Hardcoded `Abomonation` | Pluggable `Codec` trait |
| **Error handling** | Panics | `Result<T, Error>` with module-aligned sub-enums |
| **Progress tracking** | N×N broadcast between all workers | Watermark-based aggregation — scales with dynamic worker parallelism |
| **Cancellation** | Drop the worker | Cooperative `CancellationToken` |
| **Cluster scaling** | Static: all nodes known at startup | Dynamic: nodes join/leave at runtime via `ClusterMembership` |
| **Testing** | Requires multiple OS processes | Single-process multi-node testing via in-memory transport |

## Documentation

- [User Guide](./docs/guide/README.md)
- [API Reference](./docs/reference/api.md)
- [Cookbook](./docs/cookbook.md)
- [Design Docs](./docs/design/README.md)

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

| Category | Operator | Description |
|---|---|---|
| **Sources** | `source` | Emit data from a static collection |
| | `source_async` | Async producer with backpressure |
| | `input` | Channel-based external input |
| **Transform** | `map` | Transform each element |
| | `flat_map` | Transform each element into zero or more |
| | `try_flat_map` | Fallible `flat_map` with operator error propagation |
| | `filter` | Keep elements matching a predicate |
| | `map_batch` | Transform an entire batch at once |
| | `try_map_batch` | Fallible batch transform with operator error propagation |
| | `take` / `take_while` | Limit element count or stop at condition |
| **Aggregation** | `reduce` | Combine all elements per timestamp |
| | `fold` | Aggregate with initial value and output type |
| | `distinct` | Deduplicate per timestamp |
| | `count` | Count elements per timestamp |
| **Delay** | `delay` | Per-item timestamp reassignment |
| | `delay_batch` | Per-timestamp reassignment |
| **Distribution** | `exchange` / `exchange_by_hash` | Repartition data across workers by key |
| | `gather` | All data → worker 0 |
| | `rebalance` / `rebalance_to` | Round-robin across workers |
| | `broadcast` | Clone all data to every worker |
| **Branching** | `branch` | Split a stream by predicate |
| | `branch_result` | Split `Result` into Ok/Err branches |
| | `clone` | Fan-out to independent downstream branches |
| **Merge** | `merge` | Merge two streams |
| | `concat` | Merge multiple streams |
| **Observation** | `inspect` / `inspect_batch` | Observe data without modifying |
| | `for_each` / `for_each_batch` | Terminal side-effect operators |
| | `probe` | Track frontier progress |
| **Loop** | `iterate` | Feedback loop with nested scope |
| **Result** | `map_ok` / `filter_ok` | Transform/filter Ok values in Result streams |
| **Custom** | `unary` | General-purpose stateful operator (one input) |
| | `unary_notify` | Unary with frontier-based notifications |
| | `unary_async` | Async custom operator |
| | `binary` | General-purpose stateful operator (two inputs) |
| **Output** | `output` | Collect results |

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
let handle = rt.spawn_multi("my-dataflow", 2, |builder| {
    let input = builder.input::<i32>("data");
    input.map("double", |_t, x| x * 2).output("results");
    Ok(())
}, SpawnOptions::default())?;
```

By default, **auto-parallelism** is enabled — stage 0 parallelism is detected from the number of `input()` / `source_async()` calls, and `num_workers` acts as a minimum floor. Disable with `SpawnOptions::new().auto_parallelism(false)` to use the exact `num_workers` for stage 0. To force uniform parallelism across *all* stages, also set `per_stage_parallelism(false)`.

`SimpleRuntime` still exists for tests behind the `test-utils` feature, but production code should use `RuntimeHandle`.

**Cluster mode** — multi-node distributed execution over TCP:

```rust
let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 2),
    NodeConfig::new("node-b", 2),
])?;

// Application provides pre-established connections between nodes.
// See instancy/tests/cluster_tcp.rs for complete working examples.
let handle = rt.spawn_cluster(
    "my-cluster-df", topology, "node-a", dataflow_id,
    connections, capacity, handshake_timeout,
    |builder| { /* build graph */ Ok(()) },
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

## Dynamic Cluster Scaling

Nodes can join or leave a running cluster at runtime. The hosting application provides a `ClusterMembership` implementation that produces node join/leave events; the runtime automatically updates the live topology.

```rust
use instancy::{
    ChannelMembership, ClusterTopology, MembershipEvent, NodeConfig,
    RuntimeConfig, RuntimeHandle,
};

// Create a membership provider and attach it to the topology.
let membership = ChannelMembership::new();
let tx = membership.sender();

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 4),
    NodeConfig::new("node-b", 4),
]).unwrap().with_membership(membership);

// Pass topology via RuntimeConfig — membership listener starts automatically.
let rt = RuntimeHandle::new(RuntimeConfig {
    topology: Some(topology),
    ..Default::default()
}).unwrap();

// Later: a new node joins (e.g., from a Kubernetes pod watch).
tx.send(MembershipEvent::NodeJoined {
    node_id: "node-c".into(),
    logical_workers: 4,
}).unwrap();

// The live topology is updated asynchronously by a background task.
// Use rt.current_topology() to get the latest snapshot when spawning
// new cluster dataflows.
```

**Key behaviors:**
- **Node join**: topology expands; callers pass `rt.current_topology()` to `spawn_cluster` to include new nodes
- **Node leave**: affected dataflows are cancelled; topology contracts
- **Already-running dataflows are not repartitioned** — only new dataflows use the updated topology
- **The application is the single source of truth** — the runtime does not perform its own discovery

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
| `async_io_channels` | End-to-end async dataflow with async I/O |
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
| `delay` | Delay operators: windowing, priority routing, time shifting |
| `broadcast` | Multi-worker broadcast replication |
| `cancellation` | Cooperative cancellation with `CancellationToken` |
| `graceful_drain` | Drain in-flight data on cancellation instead of stopping |
| `error_handling` | Result combinators: map_ok, filter_ok, branch_result |
| `panic_recovery` | Operator panic recovery with catch_panics |

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
| `metrics_collection` | Per-operator metrics collection and reporting |

**Cluster (Distributed)**

| Example | Description |
|---|---|
| `cluster_basic` | Two-node cluster with in-memory transport (no exchange) |
| `cluster_exchange` | Two-node cluster with cross-node data repartitioning |
| `cluster_shared_transport` | Multi-dataflow shared connections with connection pooling |
| `stage_parallelism` | Stage-level parallelism configuration |

## Benchmarks

### Single-Process Sequential (Criterion, 1 worker thread each)

Both libraries use identical worker counts (1 thread). Each iteration builds
a fresh dataflow, feeds data, and drains to completion — no concurrent queries.

| Scenario | Size | instancy | timely | Speedup |
|---|---|---|---|---|
| ScanFilterAgg | 100K | 5.10 ms | 5.76 ms | **1.13×** |
| ScanFilterAgg | 1M | 50.4 ms | 53.6 ms | **1.06×** |
| ScanFilterAgg | 10M | 502 ms | 525 ms | **1.05×** |
| PageRank (10 iter) | 10K edges | 430 µs | 698 µs | **1.62×** |
| PageRank (10 iter) | 100K edges | 5.64 ms | 6.27 ms | **1.11×** |
| MapChain (20 stages) | 10K | 335 µs | 1.01 ms | **3.01×** |
| MapChain (20 stages) | 100K | 848 µs | 5.19 ms | **6.12×** |
| MapChain (20 stages) | 1M | 11.8 ms | 50.5 ms | **4.28×** |
| MultiEpoch (16 epochs) | 16×256 | 83 µs | 285 µs | **3.43×** |
| MultiEpoch (16 epochs) | 16×4096 | 225 µs | 443 µs | **1.97×** |
| SmallPipeline (3 maps) | 1K | 102 µs | 297 µs | **2.91×** |
| SmallPipeline (3 maps) | 10K | 126 µs | 399 µs | **3.16×** |
| SmallPipeline (3 maps) | 100K | 368 µs | 1.28 ms | **3.48×** |

### Cross-Process TCP (600s sustained, 16 threads × 2 processes)

Both libraries use the same cores, same data, same dataflow graphs with real
TCP transport between 2 OS processes.

| Scenario | Throughput | Latency (p50) | Memory | Core Efficiency |
|---|---|---|---|---|
| **ScanFilterAgg** (100M records) | **3.5×** | **3.7×** faster | **13.6×** less | **8.7×** better |
| **PageRank** (200K vertices, 100 iter) | **1.2×** | **1.2×** faster | ~equal | **1.4×** better |
| **MapChain** (5M × 20 stages) | **2.8×** | **2.8×** faster | **1.4×** less | **2.1×** better |
| **MultiEpoch** (16 × 4096 records) | **4.1×** | **5.0×** faster | **1.2×** less | **3.6×** better |
| **SmallPipeline** (100 vals, ×64 concurrent) | **7.1×** | **6.5×** faster | **1.9×** less | **72×** better |

> **[Full benchmark results, analysis, and methodology →](./docs/benchmark-results.md)**

## Testing

```bash
# Run all tests
cargo test -p instancy --all-features -- --test-threads=4

# Run without transport feature
cargo test -p instancy --no-default-features --features tracing

# Run a specific integration test
cargo test -p instancy --all-features --test cluster_tcp
```

### Test Organization

| File | Description |
|---|---|
| `instancy/tests/cluster.rs` | Multi-node cluster tests with in-memory transport |
| `instancy/tests/cluster_tcp.rs` | TCP-based cluster integration tests |
| `instancy/tests/cluster_shared_transport.rs` | Shared transport connection tests |
| `instancy/tests/parallel_dataflows.rs` | Shared worker pool correctness |
| `instancy/tests/parallel_cluster_tcp.rs` | Parallel TCP dataflows on shared connections |
| `instancy/tests/multi_dataflow.rs` | Multiple dataflows on one runtime |
| `instancy/tests/multi_worker_aggregation.rs` | Multi-worker reduce/fold/distinct/count |
| `instancy/tests/multi_worker_broadcast.rs` | Multi-worker broadcast operator |
| `instancy/tests/multi_worker_branch_distribution.rs` | Branch and distribution operators |
| `instancy/tests/multi_worker_iterate.rs` | Multi-worker iteration loops |
| `instancy/tests/delay_operator.rs` | Delay and delay_batch operators |
| `instancy/tests/feedback_loops.rs` | Feedback loop correctness |
| `instancy/tests/progress_tracking.rs` | Frontier and progress tracking |
| `instancy/tests/edge_cases.rs` | Edge cases and boundary conditions |
| `instancy/tests/inter_process.rs` | Cross-process communication |
| `instancy/tests/observability.rs` | Metrics and tracing |
| `instancy/tests/scheduler_policies.rs` | Task scheduler policy tests |
| `instancy/tests/timeout.rs` | Timeout and cancellation tests |
| `instancy/tests/graceful_drain.rs` | Graceful drain on cancellation tests |
| `instancy/tests/rolling_upgrade.rs` | Dynamic cluster scaling lifecycle tests |
| `instancy/tests/spawn_dataflow.rs` | Spawn and auto-parallelism tests |
| `instancy/tests/staged_parallelism.rs` | Per-stage parallelism tests |

## Project Structure

```
instancy/
├── src/
│   ├── lib.rs                    # Public API and re-exports
│   ├── runtime.rs                # RuntimeHandle, SpawnOptions, spawn_cluster
│   ├── error.rs                  # Module-aligned error hierarchy
│   ├── execute.rs                # ClusterTopology, membership types, execution config
│   ├── cancellation.rs           # CancellationToken and CancellationReason
│   ├── metrics/                   # Per-operator metrics collection
│   ├── worker.rs                 # WorkerId and OperatorActivation
│   ├── order.rs                  # Timestamp types (Product for nested scopes)
│   ├── wire.rs                   # Safe byte-parsing helpers
│   ├── dataflow/
│   │   ├── dataflow_builder.rs   # DataflowBuilder — operator chaining API
│   │   ├── executor.rs           # Async sweep-based executor
│   │   ├── operators/            # Input, output, and operator handle types
│   │   └── channels/             # Exchange, bounded, network channels
│   ├── progress/
│   │   ├── subgraph.rs           # ProgressTracker — capability/frontier tracking
│   │   ├── frontier.rs           # MutableAntichain
│   │   └── reachability.rs       # Pointstamp reachability analysis
│   ├── communication/
│   │   ├── shared_transport.rs   # Multiplexed TCP transport per peer
│   │   ├── transport_session.rs  # Per-peer transport session
│   │   ├── control_protocol.rs   # Fingerprint exchange + ready barrier
│   │   └── codec.rs              # Codec trait + built-in implementations
│   └── scheduler/                # Task scheduling and priority policies
├── examples/                     # 35 runnable examples
├── tests/                        # 21 integration test files
└── Cargo.toml
```

## License

MIT
