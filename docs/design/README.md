# instancy Design Overview

This document is the entry point for the instancy design set. It keeps the architectural overview in one place and links to focused topic documents for the detailed execution, communication, progress, cluster, and API design material.

## Document Map

- [Execution model](./execution-model.md) — worker pool, logical workers, scheduling, inputs/outputs, load control, and per-stage parallelism.
- [Communication layer](./communication.md) — intra-process channels, inter-process connections, pooling, shared connection mode, wire protocol, and dataflow isolation.
- [Progress tracking](./progress-tracking.md) — reachability, frontier computation, notifications, loops, distributed progress exchange, and quiescence.
- [Error handling](./error-handling.md) — error hierarchy, propagation, poison handling, panic-removal policy, and no-global-state guarantees for failure handling.
- [Observability](./observability.md) — metrics, tracing, message envelopes, activation/frontier/transfer timelines, and cluster-level reporting.
- [Cluster & distributed execution](./cluster.md) — cancellation, startup handshake, coordinator integration, dynamic membership, multi-cluster isolation, and cross-process testing.
- [Serialization](./serialization.md) — codec trait, default bincode codec, and data/exchange bounds.
- [Operators](./operators.md) — core operators, extension model, error policy, and user-facing API examples.
- [Design decisions & trade-offs](./decisions.md) — major architectural choices, checkpointing, throughput trade-offs, task scheduling policy, and open questions.
- [DataFusion gap analysis](./datafusion-gap-analysis.md) — feature-by-feature notes on where instancy differs from DataFusion-style execution needs.

> Each topic file links back here. Detailed material has been moved out of the old monolithic root-level design documents.

## 1. Overview

**instancy** is an asynchronous, Tokio-based reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) — a low-latency cyclic dataflow computational model. It retains the core concepts of timely dataflow (timestamps, frontiers, progress tracking, capabilities, scopes) while making fundamental changes to the execution model, networking, serialization, and error handling.

### Design Principles

1. **Fully logical computation** — the dataflow graph, streams, operators, workers, and partitioning are all purely logical abstractions. Physical resources (OS threads, network connections, processes) are provided by pluggable adapters. This enables testing multi-node distributed dataflows entirely within a single process.
2. **Dual-layer execution** — operators run as synchronous tasks on a custom lightweight Worker Thread Pool (no async overhead); I/O (input streams, networking) runs on a separate Tokio runtime. Multiple dataflows share the pool for resource efficiency.
3. **Timely semantics preserved** — timestamps, partial ordering, progress tracking, frontiers, capabilities, and nested scopes all work the same way conceptually.
4. **Production-grade robustness** — `Result`-based error handling everywhere; no panics in library code. First-class cancellation via `CancellationToken`.
5. **Pluggable networking** — users supply their own connection factory (e.g., mTLS); the library manages a pooled, reusable connection layer.
6. **Pluggable serialization** — a `Codec` trait lets users choose bincode, protobuf, flatbuffers, or any other format.
7. **Minimal core operators** — only `unary`, `binary`, `branch`, `feedback` (loop), `exchange`, `rebalance`, `gather`, `broadcast`, `delay`, `input`, `probe`, `inspect`, `for_each`, `concat`. Higher-level operators live in extension crates.
8. **Structured message envelope** — messages carry either data or control signals (errors, cancellation) in a unified envelope, enabling in-band error propagation and coordinated shutdown.
9. **Configurable error policy** — each dataflow specifies whether errors should halt the pipeline or be logged and skipped, giving consumers control over fault tolerance.
10. **Observability built-in** — per-dataflow CPU time tracking, operator-level metrics, and structured tracing for understanding performance characteristics.
11. **Checkpointing support** — consumers can add checkpoint operators that persist state at timestamp boundaries, enabling recovery by fast-forwarding input to the stored frontier.
12. **Per-stage dynamic parallelism** — operators in the same stage share a parallelism level; different stages can have different parallelism. Stage boundaries are auto-inferred from repartition operators (`exchange`, `rebalance`, `gather`, `broadcast`). Each stage×worker runs as an independent `StageExecutor` with inline frontier tracking. Workers only materialize operators for stages they participate in. Cross-stage data flows through boundary exchange channels with embedded frontier updates.
13. **Dynamic cluster scaling** — nodes can join or leave the cluster at runtime. The hosting application detects membership changes and notifies the runtime via a `ClusterMembership` trait attached to `ClusterTopology`. The library updates the live topology, cancels affected dataflows on node departure, and makes new nodes available to subsequent `spawn_cluster` calls. Already-running dataflows are not repartitioned.
14. **No global state** — zero static variables, `lazy_static`, or thread-locals. All state is owned by an explicit `RuntimeHandle`. Multiple isolated clusters can coexist in a single process (e.g., interactive vs batch workloads).
15. **Pluggable task scheduling** — the task queue accepts a `SchedulePolicy` trait that determines dequeue order based on (dataflow priority, task age). Default policy uses priority-with-aging to prevent starvation of low-priority dataflows.

---

## 2. Architecture Comparison: timely-dataflow vs instancy

| Aspect | timely-dataflow | instancy |
|---|---|---|
| Abstraction level | Workers and channels tied to physical threads and TCP connections | Fully logical: workers, streams, and routing are virtual; physical resources provided by adapters. Transport-agnostic — any `AsyncRead + AsyncWrite` byte stream (TCP, TLS, Unix sockets, pipes, QUIC) |
| Execution | 1 OS thread per worker; worker owns its dataflows and steps through them synchronously | Dual-layer: Custom Worker Thread Pool (sync operator logic) + Tokio I/O runtime (network, input streams); logical `WorkerId`s for FIFO ordering |
| Worker topology | All nodes must have the same number of workers | Heterogeneous: each node declares its own worker count based on capacity; global worker set is the union |
| Scheduling | `Worker::step()` loop polls activations | Per-worker FIFO queues → shared task queue → Worker Thread Pool threads (spin/yield/park idle strategy) |
| Communication (intra-process) | `Rc<RefCell<VecDeque>>` with direct push/pull | Lock-free SPSC bounded ring buffers for exchange channels; bounded `Mutex<VecDeque>` for pipeline channels |
| Communication (inter-process) | Dedicated TCP per worker pair, pre-configured hostfile | Application-provided `ConnectionManager` establishes connections; library pools and reuses them |
| Testability | Requires multiple OS processes for multi-node tests | Single-process multi-node testing via in-memory transport adapter |
| Serialization | `Abomonation` / `bincode` hardcoded | Pluggable `Codec` trait |
| Error handling | `panic!` / `unwrap` in many places | `Result<T, Error>` throughout; `thiserror` for error types |
| Cancellation | Drop the worker | `CancellationToken` propagated to all operators |
| Operator API | Closure-based `unary` / `binary` with manual `Notificator` | `async fn` closures with `InputHandle` / `OutputHandle`; inputs are external async streams with automatic capability management |
| Worker pool | Fixed thread count, one OS thread per worker | Dynamic pool with configurable min/max; auto-scales based on load |
| Error policy | Panic on error | Per-dataflow configurable: stop or ignore (log & skip) |
| Messages | Raw data only | Structured envelope: `Data` or `Control` (error, cancellation) |
| Observability | Limited | Built-in CPU time tracking per dataflow, operator-level metrics |
| Checkpointing | Not supported | Extensible checkpoint operators using timestamp boundaries |
| Parallelism | Uniform: all operators share the same worker count | Per-stage: stages can have different parallelism; repartition operators at boundaries. Operators within a stage are fused. |
| Cluster scaling | Static: all nodes must be known at startup | Dynamic: application notifies runtime of node joins/departures via `ClusterMembership` trait attached to `ClusterTopology`; live topology updated for new dataflows |
| Multi-dataflow | One worker owns its dataflows; implicit isolation via thread-local state | Explicit DataflowId in frame headers; shared connections demux by (dataflow_id, channel_id) |

---

## 3. Crate Structure

```
instancy/
├── Cargo.toml              (workspace root)
├── instancy/            (main crate)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── error.rs         — Error types
│   │   ├── order.rs         — PartialOrder trait
│   │   ├── progress/
│   │   │   ├── mod.rs
│   │   │   ├── timestamp.rs — Timestamp, PathSummary
│   │   │   ├── frontier.rs  — Antichain, MutableAntichain
│   │   │   ├── change_batch.rs
│   │   │   ├── reachability.rs — Tracker
│   │   │   ├── operate.rs   — Operate trait
│   │   │   └── subgraph.rs  — SubgraphBuilder, progress tracking
│   │   ├── dataflow/
│   │   │   ├── mod.rs
│   │   │   ├── scope.rs     — Scope trait + ChildScope
│   │   │   ├── stream.rs    — StreamEdge<S, C>
│   │   │   ├── stage.rs     — StageId, StageInfo, infer_stages
│   │   │   ├── channels/    — Push/Pull abstractions, Envelope
│   │   │   └── operators/
│   │   │       ├── mod.rs
│   │   │       ├── capability.rs
│   │   │       ├── from_stream.rs — Binds external async streams as inputs
│   │   │       ├── unary.rs
│   │   │       ├── binary.rs
│   │   │       ├── branch.rs
│   │   │       ├── feedback.rs   — LoopVariable, ConnectLoop
│   │   │       ├── exchange.rs
│   │   │       ├── broadcast.rs
│   │   │       ├── delay.rs        — Time-based data buffering
│   │   │       ├── checkpoint.rs   — CheckpointBackend trait + operator
│   │   │       ├── concat.rs
│   │   │       ├── inspect.rs
│   │   │       └── probe.rs
│   │   ├── worker.rs        — Worker handle (async)
│   │   ├── execute.rs       — Runtime bootstrap
│   │   ├── metrics.rs       — DataflowMetrics, OperatorMetrics
│   │   └── communication/
│   │       ├── mod.rs
│   │       ├── allocator.rs — Channel allocator
│   │       ├── codec.rs     — Codec trait
│   │       ├── connection.rs— ConnectionFactory + pool
│   │       └── transport.rs — Framed async read/write
│   └── Cargo.toml
├── instancy-communication/  (optional: split networking crate)
└── examples/
    ├── hello.rs
    ├── wordcount.rs
    └── loop_example.rs
```

---

## 4. Core Concepts (Retained from timely-dataflow)

### 4.1 Timestamps & Partial Order

Identical to timely-dataflow. A `Timestamp` is a partially-ordered, cloneable, debuggable type with a `Summary` describing how timestamps advance along dataflow edges.

```rust
pub trait Timestamp: Clone + Eq + PartialOrder + Ord + Debug + Send + Sync + 'static {
    type Summary: PathSummary<Self> + Send + Sync + 'static;
    fn minimum() -> Self;
}

pub trait PathSummary<T>: Clone + Eq + PartialOrder + Debug + Default + Send + Sync {
    fn results_in(&self, src: &T) -> Option<T>;
    fn followed_by(&self, other: &Self) -> Option<Self>;
}
```

**Key difference**: `Send + Sync` bounds added everywhere because data crosses async task boundaries.

### 4.2 Frontiers & Progress Tracking

The progress tracking protocol is preserved from timely-dataflow:
- **Antichain**: the frontier of minimal outstanding timestamps.
- **ChangeBatch**: accumulated changes to timestamp counts.
- **Reachability / Tracker**: determines which timestamps can still arrive at each operator port, based on outstanding capabilities and dataflow graph path summaries.
- **SharedProgress**: consumed/produced/internals reported by operators.

Each worker runs progress tracking inline within the executor's sweep cycle. In multi-worker dataflows, capability changes are broadcast to all peer workers so that each tracker reflects **global** state — not just local capabilities. This decentralized approach enables correct frontier computation and completion detection without global barriers. See §11 for the full protocol.

### 4.3 Capabilities

Operators hold `Capability<T>` tokens that represent the ability to produce output at timestamp `T`. Dropping a capability signals that the operator will no longer produce output at or before that timestamp.

```rust
pub struct Capability<T: Timestamp> {
    time: T,
    internal: Weak<ProgressReporter<T>>,  // reports drops to progress tracker
}
```

### 4.4 Scopes & Nesting

Scopes nest exactly as in timely-dataflow. A `Scope` owns a `SubgraphBuilder` that tracks operators and their connectivity. The `enter` / `leave` operators wrap and unwrap product timestamps for nested iteration.

---

## 4.5 Logical/Physical Separation Architecture

A fundamental design choice in instancy is the complete separation between **logical computation** and **physical execution**. The dataflow graph, streams, operators, workers, and partitioning are all purely logical abstractions. Physical resources (OS threads, network connections, processes) are provided by pluggable **adapters** (also called **providers**).

### The Three Layers

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Logical Layer (Pure Computation)                   │
│                                                                     │
│  Dataflow graph, operators, streams, stages, workers, timestamps    │
│  Progress tracking, capability exchange, frontier computation       │
│  ← No knowledge of threads, network, OS, or physical topology →     │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Adapter Traits
┌─────────────────────────────▼───────────────────────────────────────┐
│                     Adapter Layer (Abstraction)                      │
│                                                                     │
│  TransportProvider    — delivers data envelopes between workers      │
│  ExecutionProvider    — maps logical workers to physical threads     │
│  ProgressProvider     — exchanges progress messages between workers  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Concrete implementations
┌─────────────────────────────▼───────────────────────────────────────┐
│                   Physical Layer (Resources)                         │
│                                                                     │
│  OS threads, TCP/QUIC connections, shared memory, in-memory loops   │
└─────────────────────────────────────────────────────────────────────┘
```

**Critical design point:** All three concerns — data transport, execution scheduling, and progress exchange — follow the same logical/physical separation. The `ProgressTracker` exchanges capability changes between *logical worker IDs*. It does not know whether those workers are on the same thread, in the same process, or on different machines. The physical layer provides the concrete delivery mechanism (shared memory buffers, network serialization, etc.). This means the same progress tracking code handles single-process and distributed deployments without modification. See §11.5 for details.

### Logical Targets

When a stream produces data for a downstream operator, it addresses a **logical target** — a combination of `(StageId, WorkerId, OperatorIndex)`. The stream never knows whether the target is:
- On the same OS thread (just write into a buffer)
- On a different thread in the same process (lock-free queue)
- On a remote machine (serialize + network send)

The **TransportProvider** resolves logical targets to physical delivery:

```rust
/// Identifies a logical destination for data delivery.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct LogicalTarget {
    /// The stage containing the target operator.
    pub stage: StageId,
    /// The logical worker index within the stage.
    pub worker: WorkerId,
    /// The operator index within the worker.
    pub operator: usize,
    /// The input slot on the target operator (e.g., 0 = left, 1 = right for binary).
    pub input_index: usize,
}

/// Resolves logical targets to physical delivery mechanisms.
///
/// The library calls `resolve()` during dataflow construction to obtain
/// a Push endpoint for each logical target. The provider implementation
/// decides how to deliver based on the physical topology.
pub trait TransportProvider: Send + Sync + 'static {
    /// Resolve a logical target into a physical Push channel.
    /// Returns a Push endpoint that the runtime uses to deliver envelopes.
    fn resolve<T: Timestamp, D: Send + 'static, M: Send + 'static>(
        &self,
        source: LogicalTarget,
        target: LogicalTarget,
    ) -> Box<dyn Push<T, D, M>>;

    /// Returns true if source and target are co-located (same process).
    /// Used to decide whether serialization is needed.
    fn is_local(&self, source: &LogicalTarget, target: &LogicalTarget) -> bool;
}

/// Maps logical workers to physical execution resources.
///
/// The default implementation uses the custom Worker Thread Pool.
/// Alternative implementations can pin workers to specific cores,
/// use NUMA-aware scheduling, or share with an application thread pool.
pub trait ExecutionProvider: Send + Sync + 'static {
    /// Submit a task for a logical worker to be executed on a physical thread.
    fn submit_task(&self, worker: WorkerId, task: Box<dyn FnOnce() + Send>);

    /// Returns the maximum concurrent tasks allowed for a stage.
    fn stage_concurrency(&self, stage: StageId) -> usize;
}
```

### Built-in Implementations

| Provider | Use case |
|----------|----------|
| `LocalTransport` | Single-process: all logical targets resolve to bounded in-memory buffers |
| `NetworkTransport` | Multi-process: co-local targets use buffers; remote targets use ConnectionManager + serialization |
| `InMemoryClusterTransport` | **Testing**: simulates multi-node cluster entirely in-memory within a single OS process |
| `WorkerPoolExecution` | Default: maps logical workers to the custom Worker Thread Pool |
| `InlineExecution` | **Testing**: runs all tasks on the calling thread (deterministic, single-threaded) |

### Key Benefits

1. **Testability**: Developers can test multi-node distributed dataflows in a single process by using `InMemoryClusterTransport`. No Docker, no port allocation, no network flakiness in CI.

2. **Portability**: The same dataflow logic runs unchanged whether deployed on a single machine, a Kubernetes cluster, or a serverless environment — only the adapter configuration changes.

3. **Flexibility**: Applications can provide custom providers that integrate with their specific infrastructure (actor frameworks, service meshes, shared memory segments).

4. **Separation of concerns**: Operator logic never deals with physical resources. Testing, debugging, and reasoning about correctness only require understanding the logical layer.

### Example: Testing a Multi-Node Dataflow in a Single Process

```rust
#[test]
fn test_distributed_word_count() {
    // Simulate a 3-node cluster entirely in-memory
    let cluster = InMemoryCluster::new(3); // 3 logical nodes
    
    let transport = InMemoryClusterTransport::new(&cluster);
    let execution = InlineExecution::new(); // deterministic, single-threaded
    
    let config = RuntimeConfig {
        transport: Box::new(transport),
        execution: Box::new(execution),
        ..Default::default()
    };
    
    // Run the exact same dataflow that would run on 3 physical machines
    let result = execute(config, |scope| {
        let input = scope.input_from(test_data());
        input
            .exchange(|word: &String| hash(word))
            .unary("count", |input, output, _notif| { /* ... */ })
            .gather()
            .output()
    });
    
    assert_eq!(result.outputs[0].collect(), expected_counts);
}
```

---

## 13. Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tokio-stream = "0.1"
futures = "0.3"
bytes = "1"
async-trait = "0.1"
thiserror = "2"
tracing = "0.1"
bincode = { version = "2", optional = true }
serde = { version = "1", features = ["derive"], optional = true }
dashmap = "6"

[features]
default = ["bincode-codec"]
bincode-codec = ["bincode", "serde"]
```

---

## 13.5 Component Cardinality & Lifetime

This section documents the cardinality (how many instances exist) and lifetime (when they are created and destroyed) of key components. This helps developers understand ownership, sharing, and resource management.

| Component | Cardinality | Lifetime | Notes |
|-----------|-------------|----------|-------|
| `WorkerPool` | 1 per process | Process | Shared across all dataflows in the process |
| `ConnectionPool` | 1 per process | Process | Manages connections to all peer nodes (via `ConnectionFactory`) |
| `ClusterTopology` | 1 per process | Process (mutable via membership events) | Updated on node join/leave; new dataflows use latest topology |
| `DataflowId` | 1 per dataflow | Dataflow | UUID, created at dataflow start |
| `DataflowHandle` | 1 per (dataflow, node) | Dataflow | Returned to caller; provides cancel/progress/result |
| `OutcomeAggregator` | 1 per dataflow on coordinator node | Dataflow | Collects per-node outcomes; host-app managed |
| `DataflowSession` | 1 per dataflow | Dataflow | Owns channel allocation for one dataflow |
| `ProgressTracker` | 1 per dataflow | Dataflow | Tracks frontier advancement |
| `ProgressExchange` | 1 per dataflow | Dataflow | Sends/receives progress to/from peers |
| `CancellationToken` | 1 per dataflow | Dataflow | Cloned into operators; shared cancellation signal |
| `RemotePush` / `FrameSender` | 1 per (dataflow, remote peer) | Dataflow | Outbound data channel to one peer |
| `Demuxer` | 1 per physical connection | Connection | Reads frames, dispatches to per-channel receivers |
| `MuxerSender` | 1 per physical connection | Connection | Collects frames from multiple channels for one connection |
| `Codec` (instance) | 1 per dataflow | Dataflow | Shared (Arc) across all operators in a dataflow |
| `RoutingTable` | 1 per (dataflow, edge) | Dataflow | Maps workers → remote endpoints for one logical edge |
| `InputHandle` / `OutputHandle` | 1 per operator activation | Transient | Created per activation, dropped after |
| `Capability<T>` | N per operator | Operator activation | Tracks held timestamps; reports on drop |
| `CapabilitySet<T>` | 1 per operator | Operator | Manages the set of capabilities an operator holds |

**Key patterns:**
- **Process-lifetime** components are typically created at startup and live until process exit. They are `Arc`-shared.
- **Dataflow-lifetime** components are created when a dataflow starts and destroyed when it completes or is cancelled.
- **Connection-lifetime** components are tied to a physical connection; they are recreated on reconnection.
- **Transient** components are created and destroyed within a single operator activation cycle.

---

## 14. Implementation Phases

> **Status**: Phases 1–7 are implemented. Detailed trade-offs and remaining questions live in [decisions.md](./decisions.md).

**Phase 1 — Foundation** ✅
- Error types, `PartialOrder`, `Timestamp`, `PathSummary`
- `Antichain`, `ChangeBatch`, `MutableAntichain`
- `Capability` and capability management
- Basic `Scope` trait and `Worker` structure

**Phase 2 — Intra-Process Dataflow** ✅
- `mpsc`-based intra-process channels with `Envelope` message type
- `TimestampedInput`, `InputEvent`, `DataflowSpec`
- `from_stream` operator (binds async streams as inputs)
- `OutputHandle`, `ProbeHandle`
- Operators: `unary`, `binary`, `inspect`, `probe`, `concat`, `delay`
- Progress tracking (single-process)
- `execute()` bootstrap with dynamic worker pool

**Phase 3 — Async I/O & Robustness** ✅
- `spawn()` with `SpawnOptions::new().io_mode(IoMode::Async)` for `tokio::sync::mpsc` channel I/O
- `AsyncInputSender` / `AsyncOutputReceiver` with WakeHandle integration
- `ChannelMode` enum (Sync | Async) selected at spawn time
- `InputRecv` / `OutputSend` enum dispatch in ChannelSourceOperator / ChannelSinkOperator
- Panic-safety audit for critical paths
- `DataflowCompletion` as real future with sync waiting support

**Phase 4 — Loops & Branching** ✅
- `feedback` / `loop_variable` / `connect_loop`
- `enter` / `leave` for nested scopes
- `branch` / `ok_err`
- Error handling policy integration

**Phase 5 — Networking** ✅
- Required `ConnectionFactory` + default `TcpConnectionFactory`
- `SharedPeerManager` / `PeerPool` with connection pooling
- Wire protocol with opt-in CRC32
- Cross-process `exchange`
- Inter-process progress tracking
- Transport-agnostic runtime over any `AsyncRead + AsyncWrite` byte stream

**Phase 6 — Observability, Performance & Polish** ✅
- `DataflowMetrics`, `OperatorMetrics`, and backpressure measurement
- Cancellation integration
- Comprehensive `Result`-based error handling
- Tracing/logging integration
- Documentation, examples, benchmarks, and scheduler/channel performance improvements

**Phase 7 — Dynamic Cluster Scaling** ✅
- `ClusterMembership` trait and `ChannelMembership` convenience implementation
- `ClusterTopology::with_membership()` integration
- Live topology snapshots via `RuntimeHandle::current_topology()`
- Node join/leave processing for future dataflows
- Cancellation of affected running dataflows on node departure
