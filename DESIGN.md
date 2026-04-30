# instancy: Design Document

## 1. Overview

**instancy** is an asynchronous, Tokio-based reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) — a low-latency cyclic dataflow computational model. It retains the core concepts of timely dataflow (timestamps, frontiers, progress tracking, capabilities, scopes) while making fundamental changes to the execution model, networking, serialization, and error handling.

### Design Principles

1. **Fully logical computation** — the dataflow graph, streams, operators, workers, and partitioning are all purely logical abstractions. Physical resources (OS threads, network connections, processes) are provided by pluggable adapters. This enables testing multi-node distributed dataflows entirely within a single process.
2. **Dual-layer execution** — operators run as synchronous tasks on a custom lightweight Worker Thread Pool (no async overhead); I/O (input streams, networking) runs on a separate Tokio runtime. Multiple dataflows share the pool for resource efficiency.
3. **Timely semantics preserved** — timestamps, partial ordering, progress tracking, frontiers, capabilities, and nested scopes all work the same way conceptually.
4. **Production-grade robustness** — `Result`-based error handling everywhere; no panics in library code. First-class cancellation via `CancellationToken`.
5. **Pluggable networking** — users supply their own connection factory (e.g., mTLS); the library manages a pooled, reusable connection layer.
6. **Pluggable serialization** — a `Codec` trait lets users choose bincode, protobuf, flatbuffers, or any other format.
7. **Minimal core operators** — only `unary`, `binary`, `branch`, `feedback` (loop), `exchange`, `rebalance`, `gather`, `broadcast`, `broadcast_local`, `delay`, `input`, `probe`, `inspect`, `concat`. Higher-level operators live in extension crates.
8. **Structured message envelope** — messages carry either data or control signals (errors, cancellation) in a unified envelope, enabling in-band error propagation and coordinated shutdown.
9. **Configurable error policy** — each dataflow specifies whether errors should halt the pipeline or be logged and skipped, giving consumers control over fault tolerance.
10. **Observability built-in** — per-dataflow CPU time tracking, operator-level metrics, and structured tracing for understanding performance characteristics.
11. **Checkpointing support** — consumers can add checkpoint operators that persist state at timestamp boundaries, enabling recovery by fast-forwarding input to the stored frontier.
12. **Per-stage dynamic parallelism** — operators in the same execution region share a parallelism level; different regions can have different parallelism. Explicit repartition operators (`exchange`, `rebalance`, `gather`, `broadcast`) connect regions with different parallelism.
13. **Dynamic cluster scaling** — nodes can join or leave the cluster at runtime. The hosting application is responsible for detecting membership changes and notifying the runtime via a `ClusterMembership` trait. The library rebuilds routing and rebalances work accordingly.

---

## 2. Architecture Comparison: timely-dataflow vs instancy

| Aspect | timely-dataflow | instancy |
|---|---|---|
| Abstraction level | Workers and channels tied to physical threads and TCP connections | Fully logical: workers, streams, and routing are virtual; physical resources provided by adapters |
| Execution | 1 OS thread per worker; worker owns its dataflows and steps through them synchronously | Dual-layer: Custom Worker Thread Pool (sync operator logic) + Tokio I/O runtime (network, input streams); logical `WorkerId`s for FIFO ordering |
| Worker topology | All nodes must have the same number of workers | Heterogeneous: each node declares its own worker count based on capacity; global worker set is the union |
| Scheduling | `Worker::step()` loop polls activations | Per-worker FIFO queues → shared task queue → Worker Thread Pool threads (spin/yield/park idle strategy) |
| Communication (intra-process) | `Rc<RefCell<VecDeque>>` with direct push/pull | Bounded in-memory buffers between operators; I/O via Tokio channels |
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
| Parallelism | Uniform: all operators share the same worker count | Per-region: execution regions can have different parallelism; explicit repartition at boundaries |
| Cluster scaling | Static: all nodes must be known at startup | Dynamic: application notifies runtime of node joins/departures; routing tables rebuild on the fly |
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
│   │   │   ├── stream.rs    — DataStream<S, C>
│   │   │   ├── region.rs    — Region, PlacementPolicy
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

The progress tracking protocol is preserved:
- **Antichain**: the frontier of minimal outstanding timestamps.
- **ChangeBatch**: accumulated changes to timestamp counts.
- **Reachability / Tracker**: determines which timestamps can still arrive at each operator port.
- **SharedProgress**: consumed/produced/internals reported by operators.

Progress tracking runs as a dedicated async task per subgraph, receiving updates via an `mpsc` channel rather than being inline in `Worker::step()`.

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
│  Dataflow graph, operators, streams, regions, workers, timestamps   │
│  ← No knowledge of threads, network, OS, or physical topology →     │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Adapter Traits
┌─────────────────────────────▼───────────────────────────────────────┐
│                     Adapter Layer (Abstraction)                      │
│                                                                     │
│  TransportProvider    — delivers envelopes between logical targets   │
│  ExecutionProvider    — maps logical workers to physical threads     │
│  ProgressProvider     — exchanges progress messages between nodes    │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ Concrete implementations
┌─────────────────────────────▼───────────────────────────────────────┐
│                   Physical Layer (Resources)                         │
│                                                                     │
│  OS threads, TCP/QUIC connections, shared memory, in-memory loops   │
└─────────────────────────────────────────────────────────────────────┘
```

### Logical Targets

When a stream produces data for a downstream operator, it addresses a **logical target** — a combination of `(RegionId, WorkerId, OperatorIndex)`. The stream never knows whether the target is:
- On the same OS thread (just write into a buffer)
- On a different thread in the same process (lock-free queue)
- On a remote machine (serialize + network send)

The **TransportProvider** resolves logical targets to physical delivery:

```rust
/// Identifies a logical destination for data delivery.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct LogicalTarget {
    /// The execution region containing the target operator.
    pub region: RegionId,
    /// The logical worker index within the region.
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

    /// Returns the maximum concurrent tasks allowed for a region.
    fn region_concurrency(&self, region: RegionId) -> usize;
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

## 5. Execution Model

### 5.1 Dual-Layer Architecture: Worker Thread Pool + I/O Runtime

instancy separates **computation** from **I/O** into two distinct layers:

1. **Worker Thread Pool** (custom, lightweight) — a dedicated thread pool that executes operator logic. Operators are pure synchronous functions: take input batches, compute, produce output batches. No async, no I/O, no `await` points in operator code.

2. **I/O Runtime** (Tokio) — handles asynchronous I/O: reading input streams, network send/recv for inter-process exchange, progress message exchange. This is a standard Tokio runtime, either provided by the caller or created internally.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Worker Thread Pool (custom)                            │
│               Lightweight worker threads for operator logic          │
│                                                                     │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                  │
│  │Thread 0 │ │Thread 1 │ │Thread 2 │ │Thread 3 │  (dynamic count)  │
│  │         │ │         │ │         │ │         │                    │
│  │ poll    │ │ poll    │ │ poll    │ │ poll    │                    │
│  │ queue   │ │ queue   │ │ queue   │ │ queue   │                    │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                   │
│       └────────────┴──────────┴────────────┘                        │
│                          │                                           │
│                 Shared Task Queue                                    │
│          (lock-free, work-stealing deque)                           │
└─────────────────────────────────────────────────────────────────────┘
                          ▲ enqueue                    │ results
                          │                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     I/O Runtime (Tokio)                              │
│        Handles async streams, networking, progress exchange          │
│                                                                     │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────────┐     │
│  │ Input reader  │  │ Network I/O   │  │ Progress exchange    │     │
│  │ (from_stream) │  │ (connections) │  │ (cross-process)     │     │
│  └───────────────┘  └───────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

**Why not Tokio for computation?**

Operator logic is purely synchronous in-memory work: take a `Vec<D>` input, apply a function, produce a `Vec<D>` output. There are no I/O operations, no `await` points, no syscalls. Using Tokio for this adds unnecessary overhead:
- Tokio's task scheduler optimizes for I/O-bound workloads with many await points
- Each `poll()` invocation in Tokio has overhead from the vtable dispatch, waker management, and cooperative scheduling
- Tokio's work-stealing across all runtime threads adds contention for CPU-bound work
- The `Send + Sync + 'static` requirements for spawned tasks add complexity

A Custom Worker Thread Pool eliminates this overhead. Each worker thread simply:
1. Dequeues a task (operator activation) from the shared queue
2. Runs the operator's synchronous closure to completion
3. Enqueues results into downstream operators' input buffers
4. Loops back to step 1

No async machinery, no wakers, no futures — just function calls.

### 5.2 Custom Worker Thread Pool

The Worker Thread Pool is a lightweight, purpose-built thread pool optimized for short-to-medium synchronous computation tasks.

```rust
/// Configuration for the compute thread pool.
pub struct WorkerPoolConfig {
    /// Minimum number of worker threads (always kept alive).
    /// Default: 2 or num_cpus::get() / 2, whichever is larger.
    pub min_threads: usize,
    /// Maximum number of worker threads the pool can grow to.
    /// Default: num_cpus::get().
    pub max_threads: usize,
    /// How long a thread can be idle before being shut down.
    /// Only threads above `min_threads` are eligible for shutdown.
    /// Uses exponential backoff: spin → yield → park(short) → park(long) → shutdown.
    /// Default: 30s.
    pub idle_shutdown: Duration,
}

/// The Worker Thread Pool manages worker threads that execute operator tasks.
pub struct WorkerPool {
    config: WorkerPoolConfig,
    /// Shared task queue. Worker threads poll from this.
    task_queue: Arc<TaskQueue>,
    /// Active thread handles for lifecycle management.
    threads: Vec<JoinHandle<()>>,
    /// Count of currently active (non-idle) threads.
    active_count: Arc<AtomicUsize>,
}
```

#### Thread Lifecycle & Dynamic Scaling

Worker threads follow a **spin → yield → park → shutdown** idle strategy:

```
┌─────────┐    no task     ┌─────────┐    still idle    ┌─────────────┐
│ POLLING │───────────────▶│ YIELDING│─────────────────▶│ PARKED      │
│ (spin)  │                │ (yield) │                  │ (condvar)   │
└─────────┘                └─────────┘                  └──────┬──────┘
     ▲                          ▲                              │
     │        new task          │       new task               │ idle > timeout
     └──────────────────────────┴──────────────────────────────│──────────┐
                                                               ▼          │
                                                         ┌───────────┐   │
                                                         │ SHUTDOWN  │◀──┘
                                                         │ (if > min)│
                                                         └───────────┘
```

1. **Spinning** (1-10 iterations): Thread checks the queue in a tight loop. Zero latency for back-to-back tasks.
2. **Yielding** (~100μs): Thread calls `std::thread::yield_now()`, giving CPU to other threads while remaining responsive.
3. **Parking** (condvar wait): Thread parks on a condition variable. Woken by a new task enqueue. Near-zero CPU usage while parked.
4. **Shutdown**: After `idle_shutdown` duration with no work, threads above `min_threads` exit. The pool shrinks to conserve resources.

**Growing the pool**: When a task is enqueued and all threads are busy (active_count == current thread count < max_threads), a new thread is spawned immediately.

```rust
impl WorkerPool {
    /// Submit a task for execution on the Worker Thread Pool.
    pub fn submit(&self, task: ComputeTask) {
        self.task_queue.push(task);
        
        // If all threads are busy and we're below max, spawn a new one
        if self.active_count.load(Ordering::Relaxed) >= self.threads.len()
            && self.threads.len() < self.config.max_threads
        {
            self.spawn_thread();
        }
        
        // Wake a parked thread if any
        self.task_queue.notify_one();
    }
}
```

#### Worker Thread Loop

```rust
fn worker_thread_loop(queue: &TaskQueue, pool_state: &PoolState) {
    let mut idle_cycles = 0u32;
    
    loop {
        // Try to dequeue a task
        if let Some(task) = queue.pop() {
            idle_cycles = 0;
            task.execute();  // Synchronous! No async, no futures.
            continue;
        }
        
        // No task available — idle strategy
        idle_cycles += 1;
        
        if idle_cycles < SPIN_LIMIT {
            // Phase 1: spin (very short, for back-to-back tasks)
            std::hint::spin_loop();
        } else if idle_cycles < YIELD_LIMIT {
            // Phase 2: yield (give CPU but stay responsive)
            std::thread::yield_now();
        } else {
            // Phase 3: park on condvar (minimal CPU, woken on new task)
            let parked_at = Instant::now();
            queue.park_thread(pool_state.idle_shutdown);
            
            // Check if we should shut down
            if parked_at.elapsed() >= pool_state.idle_shutdown
                && pool_state.thread_count() > pool_state.min_threads
            {
                // Exit this thread — pool shrinks
                pool_state.decrement_thread_count();
                return;
            }
        }
    }
}
```

### 5.3 Logical Workers & Task Queue

We retain the concept of a **logical worker ID** — a `WorkerId` — which serves three purposes:

1. **FIFO ordering**: All operator tasks assigned to the same `WorkerId` execute in FIFO sequence. This is enforced by per-worker task sub-queues that are drained into the shared pool queue sequentially.

2. **Parallelism control**: A dataflow's execution region declares its parallelism (e.g., 4). The pool ensures that **at most N tasks from that region are executing concurrently** using a lightweight counting semaphore.

3. **Data partitioning**: The `exchange` operator routes data by `hash(item) % total_workers`. The `WorkerId` determines which partition an operator instance belongs to.

#### Heterogeneous Worker Assignment

Unlike timely-dataflow where every process must run the same number of workers, instancy allows **each node to declare its own worker count** based on its available resources (CPU cores, memory, etc.). The global worker set is the union of all per-node workers, and each worker is assigned a globally unique `WorkerId`.

```
Example: 3-node cluster with heterogeneous workers

  Node 0 (8-core machine)     Node 1 (4-core machine)     Node 2 (16-core machine)
  workers: 4                  workers: 2                   workers: 6
  ┌────┬────┬────┬────┐       ┌────┬────┐                  ┌────┬────┬────┬────┬────┬────┐
  │ W0 │ W1 │ W2 │ W3 │       │ W4 │ W5 │                  │ W6 │ W7 │ W8 │ W9 │W10 │W11 │
  └────┴────┴────┴────┘       └────┴────┘                  └────┴────┴────┴────┴────┴────┘

  Total workers: 12
  exchange routes: hash(item) % 12
```

```rust
/// A globally unique logical worker identity.
/// Not tied to any physical OS thread.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct WorkerId(pub usize);

/// Describes the worker topology of a node in the cluster.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Unique index of this node in the cluster.
    pub node_index: usize,
    /// Number of logical workers hosted by this node.
    /// Can differ from other nodes based on this node's capacity.
    pub workers: usize,
}

/// Configuration for the instancy runtime.
pub struct RuntimeConfig {
    /// Worker Thread Pool configuration (the custom thread pool for operator logic).
    pub compute_pool: WorkerPoolConfig,
    /// Optional: provide an existing Tokio runtime handle for I/O tasks.
    /// If None, instancy creates a minimal Tokio runtime internally.
    /// The Tokio runtime is used ONLY for I/O: input stream reading,
    /// network connections, and inter-process progress exchange.
    pub io_runtime: Option<tokio::runtime::Handle>,
    /// Progress tracking mode.
    pub progress_mode: ProgressMode,
}
```

> **Runtime isolation**: The caller application can provide a dedicated Tokio runtime handle for instancy's I/O tasks via `RuntimeConfig::io_runtime`. This keeps instancy's network and input reading separate from the application's own async work. The Worker Thread Pool is always isolated by design (it's a separate thread pool from any Tokio runtime).
>
> ```rust
> // Application creates a minimal Tokio runtime for instancy I/O only
> let io_runtime = tokio::runtime::Builder::new_multi_thread()
>     .worker_threads(2)  // I/O doesn't need many threads
>     .thread_name("instancy-io")
>     .build()?;
>
> let config = RuntimeConfig {
>     compute_pool: WorkerPoolConfig {
>         min_threads: 4,
>         max_threads: 16,
>         idle_shutdown: Duration::from_secs(30),
>     },
>     io_runtime: Some(io_runtime.handle().clone()),
>     progress_mode: ProgressMode::Eager,
> };
>
> // The application's own Tokio runtime is completely unaffected
> ```

```rust
/// Per-dataflow configuration.
pub struct DataflowConfig {
    /// The cluster topology: describes all participating nodes and their worker counts.
    /// For single-process mode, this is just one NodeConfig.
    pub cluster: ClusterTopology,
    /// Cancellation token for this dataflow.
    pub cancellation_token: CancellationToken,
}

/// Describes the full cluster topology.
#[derive(Clone, Debug)]
pub struct ClusterTopology {
    /// All nodes in the cluster, ordered by node index.
    pub nodes: Vec<NodeConfig>,
}

impl ClusterTopology {
    /// Total number of logical workers across all nodes.
    pub fn total_workers(&self) -> usize {
        self.nodes.iter().map(|n| n.workers).sum()
    }
    
    /// Returns the global WorkerId range assigned to a given node.
    pub fn worker_range(&self, node_index: usize) -> std::ops::Range<usize> {
        let start: usize = self.nodes[..node_index].iter().map(|n| n.workers).sum();
        let count = self.nodes[node_index].workers;
        start..start + count
    }
    
    /// Determines which node hosts a given global WorkerId.
    pub fn node_for_worker(&self, worker_id: WorkerId) -> usize { /* ... */ }
}
```

**How logical workers are enforced on the Worker Thread Pool:**

Each logical `WorkerId` has a per-worker FIFO queue. The Worker Thread Pool processes these queues respecting FIFO ordering per worker and concurrency limits per execution region:

```
┌──────────────────────────────────────────────────────────────────┐
│                     Worker Thread Pool (custom threads)                 │
│             Threads poll from shared queue, run tasks             │
│                                                                  │
│  Dataflow A (region: parallelism=4)                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐│
│  │ Worker 0    │ │ Worker 1    │ │ Worker 2    │ │ Worker 3   ││
│  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌────────┐ ││
│  │ │Op: map  │ │ │ │Op: map  │ │ │ │Op: map  │ │ │ │Op: map │ ││
│  │ │Op: filter│ │ │ │Op: filter│ │ │ │Op: filter│ │ │ │Op: flt│ ││
│  │ │  (FIFO) │ │ │ │  (FIFO) │ │ │ │  (FIFO) │ │ │ │(FIFO) │ ││
│  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │ │ └────────┘ ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘│
│                                                                  │
│  Concurrency limit: at most 4 tasks from this region run at once │
│                                                                  │
│  Dataflow B (region: parallelism=2) — shares the same threads   │
│  ┌─────────────┐ ┌─────────────┐                                │
│  │ Worker 0    │ │ Worker 1    │                                │
│  └─────────────┘ └─────────────┘                                │
│                                                                  │
│  Multiple dataflows share the pool, each respecting its limits   │
└──────────────────────────────────────────────────────────────────┘
```

**Task scheduling within the Worker Thread Pool:**

```rust
/// A compute task ready for execution.
struct ComputeTask {
    /// Which logical worker this task belongs to.
    worker_id: WorkerId,
    /// The operator activation to execute.
    activation: OperatorActivation,
    /// Region concurrency permit (limits parallel tasks per region).
    region_permit: Arc<CountingSemaphore>,
}

/// Manages per-worker FIFO queues and dispatches to the pool.
struct TaskScheduler {
    /// Per-worker queues. Tasks within a worker execute in order.
    worker_queues: Vec<VecDeque<OperatorActivation>>,
    /// Shared queue fed to pool threads (ready-to-run tasks).
    ready_queue: Arc<TaskQueue>,
}

impl TaskScheduler {
    /// Called when an operator has input ready.
    /// Enqueues to the worker's FIFO queue.
    fn enqueue(&mut self, worker_id: WorkerId, activation: OperatorActivation) {
        self.worker_queues[worker_id.0].push_back(activation);
        self.try_dispatch(worker_id);
    }
    
    /// Moves the next task from a worker queue to the ready queue
    /// if the worker has no task currently in-flight.
    fn try_dispatch(&mut self, worker_id: WorkerId) {
        if !self.worker_in_flight[worker_id.0] {
            if let Some(task) = self.worker_queues[worker_id.0].pop_front() {
                self.worker_in_flight[worker_id.0] = true;
                self.ready_queue.push(ComputeTask { worker_id, activation: task, .. });
            }
        }
    }
    
    /// Called when a task completes. Dispatches the next task for that worker.
    fn task_completed(&mut self, worker_id: WorkerId) {
        self.worker_in_flight[worker_id.0] = false;
        self.try_dispatch(worker_id);
    }
}
```

### 5.4 Operator Scheduling

Each operator's logic is a **synchronous function** — not an async task. Operators take input, compute, and produce output without any `await` points.

```rust
/// Operator logic signature — purely synchronous.
type OperatorFn<T, C1, C2> = dyn FnMut(
    &mut InputHandle<T, C1>,
    &mut OutputHandle<T, C2>,
) -> Result<(), Error> + Send;
```

When an operator has input data ready, an activation is posted to its logical worker's queue. The Worker Thread Pool's task scheduler dispatches it to a thread when:
1. The worker has no other task in-flight (FIFO guarantee), and
2. The region's concurrency limit has not been reached.

This gives us:
- **FIFO within a worker**: operators on the same worker are activated in order.
- **Bounded parallelism per region**: at most N tasks from a region run concurrently.
- **Zero async overhead**: operator logic is a plain function call on a plain thread.
- **Fair sharing**: multiple dataflows share the same pool threads, with per-region concurrency limits preventing starvation.
- **Low latency**: spinning phase means back-to-back tasks execute with sub-microsecond scheduling overhead.

```
I/O Runtime (Tokio) ──► reads input stream ──► enqueues to Worker queue
                                                        │
                                              Worker Thread Pool thread picks it up
                                                        │
                                              Operator runs synchronously
                                                        │
                                              Output → downstream worker queue
                                              OR
                                              Output → I/O Runtime (network send)
```

**Operator activation flow:**

1. Data arrives on an operator's input buffer (pushed by upstream, or read by I/O runtime from network/input stream).
2. The task scheduler posts an activation to the operator's `WorkerId` queue.
3. When the worker has no in-flight task and the region has spare concurrency, the task is moved to the ready queue.
4. A Worker Thread Pool thread dequeues it and calls the operator's synchronous logic.
5. The operator produces output, which is written to downstream input buffers.
6. The thread signals task completion; the scheduler dispatches the next task for that worker.

#### 5.4.1 Who Creates Tasks? — The Orchestrator Event Loop

The **orchestrator** (also called the runtime event loop) is the component responsible
for receiving data messages and turning them into compute tasks. It runs on the I/O
runtime and performs the following for each operator input:

1. **Receives messages** — from local in-process channels (downstream output buffers)
   or from the network (remote exchange).
2. **Deposits into input buffer** — each operator input has a per-worker buffer where
   incoming messages accumulate.
3. **Feeds the BatchAccumulator** — calls `BatchAccumulator::record_message()` for
   each incoming message. The accumulator tracks count, byte size, and elapsed time
   since the first message in the current batch (see §12.6.2a).
4. **Checks dispatch threshold** — calls `BatchAccumulator::should_dispatch(policy)`.
   When any threshold is met (count, bytes, or time), the batch is ready.
5. **Creates an OperatorActivation** — wraps a closure that will invoke the operator's
   `FnMut` logic with a reference to the filled input buffer. The closure captures
   access to the operator's input/output handles.
6. **Enqueues into TaskScheduler** — calls `TaskScheduler::enqueue(activation, region_id)`.
   The scheduler places it in the per-worker FIFO queue.
7. **Resets the accumulator** — calls `BatchAccumulator::reset()` to start fresh for
   the next batch.

```
                    ┌──────────────────────────────────────┐
                    │   Orchestrator Event Loop (I/O side)  │
                    │                                      │
   messages ──────►│  1. Receive message                   │
   (channel/net)   │  2. Deposit into operator input buf   │
                    │  3. BatchAccumulator.record_message() │
                    │  4. should_dispatch(policy)?          │
                    │     NO  → wait for more messages      │
                    │     YES → create OperatorActivation   │
                    │  5. TaskScheduler.enqueue(activation)  │
                    │  6. BatchAccumulator.reset()           │
                    └──────────────────────┬───────────────┘
                                           │
                                           ▼
                    ┌──────────────────────────────────────┐
                    │   TaskScheduler (per-worker FIFO)     │
                    │                                      │
                    │  dispatch_ready() → ComputeTask      │
                    └──────────────────────┬───────────────┘
                                           │
                                           ▼
                    ┌──────────────────────────────────────┐
                    │   Worker Thread Pool (compute)        │
                    │   thread picks up task, runs closure  │
                    └──────────────────────────────────────┘
```

The orchestrator also manages a **timer wheel** (or per-operator deadlines) for the
`max_batch_wait` threshold: if no new messages arrive but time expires, the
orchestrator fires the activation anyway to ensure bounded latency.

#### 5.4.2 Where is Operator State Stored?

Operator state (e.g., a `HashMap` for aggregation, a running counter, a window buffer)
lives **inside the operator's closure**. The operator logic is declared as:

```rust
logic: Box<dyn FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send>
```

Because this is `FnMut` (not `FnOnce`), the closure is **invoked repeatedly** across
activations and retains mutable state between calls. The state is simply captured
variables in the closure:

```rust
// Example: stateful aggregation operator
let mut counts: HashMap<String, u64> = HashMap::new();  // ← operator state

stream.unary("word_count", |input, output| {
    // `counts` is moved into this FnMut closure and persists across activations
    while let Some((time, batch)) = input.next() {
        for word in batch {
            *counts.entry(word).or_insert(0) += 1;
        }
        output.session(&time).give_vec(counts.iter().collect());
    }
    Ok(())
});
```

**Key design properties:**

- **One closure instance per logical worker** — there is no sharing of state across
  workers. Each worker has its own independent operator instance with its own state.
  No locking or synchronization is needed to access operator state.

- **The orchestrator owns the operator struct** — the `UnaryOperator` (or `BinaryOperator`)
  struct lives in the orchestrator's operator registry. When an activation fires, the
  orchestrator calls the operator's `activate()` method which invokes the `FnMut` closure.
  The state persists for the operator's entire lifetime.

- **Thread safety via FIFO guarantee** — even though the Worker Thread Pool may run the
  closure on different OS threads across activations, the per-worker FIFO guarantee
  ensures the closure is **never called concurrently**. It is `Send` (can move between
  threads) but never needs to be `Sync`.

- **No external state store needed** — unlike actor frameworks that require a separate
  "state" object, the closure-capture pattern is natural Rust: the compiler enforces
  move semantics and lifetime correctness. The operator "is" its closure + captured state.

- **State lifetime** — operator state lives as long as the dataflow is running. When the
  dataflow is dropped or cancelled, the operator struct (and its closure with all captured
  state) is dropped, freeing all resources.

### 5.3 Input Streams

The executor accepts a dataflow definition that binds external async streams as inputs. Instead of the caller imperatively calling `input.send()` and `input.advance_to()`, the dataflow is driven by `TimestampedInput` streams — async streams that yield timestamped data. The library reads from these streams, manages capabilities and frontier advancement automatically, and the dataflow makes progress reactively as data arrives.

```rust
/// A timestamped item from an external input stream.
#[derive(Debug, Clone)]
pub enum InputEvent<T: Timestamp, D> {
    /// A batch of data at the given timestamp.
    Data(T, Vec<D>),
    /// Explicitly advance the frontier to this timestamp.
    /// All future Data events must have timestamps >= this value.
    Frontier(T),
}

/// An async stream that produces timestamped input for a dataflow.
pub trait TimestampedInput<T: Timestamp, D>:
    Stream<Item = InputEvent<T, D>> + Send + Unpin + 'static
{}

// Blanket implementation
impl<T, D, S> TimestampedInput<T, D> for S
where
    T: Timestamp,
    S: Stream<Item = InputEvent<T, D>> + Send + Unpin + 'static,
{}
```

**How the library drives the dataflow:**

For each input stream, the library spawns a dedicated async task that:
1. Reads `InputEvent`s from the user's async stream.
2. On `Data(t, batch)` — posts the batch into the owning worker's task queue at timestamp `t`, holding a capability for `t`.
3. On `Frontier(t)` — drops capabilities for all timestamps `< t`, advancing the input frontier.
4. When the stream ends (`None`) — drops all capabilities, signaling that this input is complete.

The caller never manually manages capabilities or frontier advancement.

### 5.5 Output: Sink-First Model

The orchestrator knows the full dataflow topology — including worker count, placement, and routing — **before** any data flows. Rather than forcing results through an intermediate async channel, the primary output path pushes data directly to a user-provided **`OutputSink`**. An async-stream convenience layer is built on top for cases where pull-based consumption is preferred.

#### OutputSink trait (push-based, primary path)

```rust
/// A timestamped output event emitted by the dataflow.
#[derive(Debug, Clone)]
pub enum OutputEvent<T: Timestamp, D> {
    /// A batch of output data at the given timestamp.
    Data(T, Vec<D>),
    /// The output frontier has advanced to this timestamp.
    /// All future Data events will have timestamps >= this value.
    Frontier(T),
}

/// Push-based sink that the dataflow writes output directly into.
///
/// The orchestrator wires the final operator's output to the sink at
/// construction time — no intermediate async channel sits between the
/// operator and the destination. One sink instance is created per worker
/// in the last execution region.
#[async_trait]
pub trait OutputSink<T: Timestamp, D: Send>: Send + Sync {
    /// Called for each output event produced by the final operator.
    /// Returning `Err` signals backpressure or a fatal write failure;
    /// the error policy decides whether to retry, skip, or halt.
    async fn write(&mut self, event: OutputEvent<T, D>) -> Result<()>;

    /// Called once when the dataflow completes for this worker.
    /// Use this to flush buffers, commit transactions, close handles, etc.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
```

This is the high-throughput path: operator → sink, no channel hop.
Backpressure flows naturally from the storage layer (e.g., a slow database write)
back through the dataflow via the `write` future.

#### OutputStream (pull-based, convenience wrapper)

For testing, small result sets, or interactive consumption, a pull-based
`OutputStream` is provided as a convenience built *on top of* `OutputSink`:

```rust
/// An async stream of output events produced by one worker of the final stage.
/// Internally backed by an OutputSink that writes into a bounded async channel.
pub type OutputStream<T, D> = Pin<Box<dyn Stream<Item = OutputEvent<T, D>> + Send>>;
```

This is implemented as a `ChannelSink` that writes events into a bounded
`mpsc` channel; the `OutputStream` is the receiver end. The caller gets
standard `Stream` combinators, and backpressure still works (a slow consumer
fills the channel, causing the `ChannelSink::write` future to block).

#### Wiring outputs at construction time

Because the orchestrator already knows the number of logical workers and
their physical placement, it can wire outputs directly to their destination
at dataflow construction time:

```rust
// High-throughput: push directly to storage
let handle = execute(config, |builder| {
    let input = builder.input("events");
    input
        .exchange(|e| hash(&e.key))
        .unary("transform", |handle, output| { /* ... */ })
        .output_to("results", BlobStoreSink::new(container));
    Ok(())
}).await?;

// Convenience: pull via async stream
let handle = execute(config, |builder| {
    let input = builder.input("events");
    let streams = input
        .exchange(|e| hash(&e.key))
        .unary("transform", |handle, output| { /* ... */ })
        .output();  // Returns Vec<OutputStream> (one per worker)
    Ok(streams)
}).await?;

// Consume output streams
for (worker_idx, stream) in handle.output_streams().enumerate() {
    tokio::spawn(async move {
        pin_mut!(stream);
        while let Some(event) = stream.next().await {
            match event {
                OutputEvent::Data(time, batch) => {
                    db.write_partition(worker_idx, time, batch).await;
                }
                OutputEvent::Frontier(time) => {
                    db.commit_up_to(worker_idx, time).await;
                }
            }
        }
    });
}
```

**Design rationale**:
- **Sink-first**: The orchestrator knows the full graph topology at construction time. Wiring the final operator directly to its destination avoids an unnecessary channel hop and gives the best throughput.
- **Multiple sinks/streams**: One sink (or stream) per final-stage worker preserves parallelism all the way to the destination. The caller can write to multiple database partitions, files, or network connections concurrently.
- **Frontier events included**: The caller knows when a timestamp is "complete" (no more data at that time will arrive), enabling commit/flush at appropriate points.
- **Backpressure**: In the sink path, backpressure comes from the async `write` future. In the stream path, the bounded channel provides equivalent backpressure. Both propagate naturally back through the dataflow.
- **Pull-based as convenience**: `OutputStream` is useful for tests and interactive use but is not the primary production path — it adds one channel hop compared to a direct sink.

### 5.6 Bootstrap: execute & DataflowSpec

```rust
/// Per-dataflow specification: input streams + graph definition + output streams.
pub struct DataflowSpec<T: Timestamp, D: Data> {
    config: DataflowConfig,
    inputs: Vec<(String, Box<dyn ErasedTimestampedInput<T>>)>,
    builder: Box<dyn FnOnce(DataflowInputs<T>, &mut Scope<T>) -> Result<Vec<Stream<_, Vec<D>>>, Error> + Send>,
}

impl<T: Timestamp, D: Data> DataflowSpec<T, D> {
    pub fn new(config: DataflowConfig) -> Self { /* ... */ }

    /// Attach a named input stream.
    pub fn input<I: Data>(
        mut self,
        name: &str,
        stream: impl TimestampedInput<T, I>,
    ) -> Self { /* ... */ }

    /// Define the dataflow graph. The builder returns the output streams
    /// from the final stage.
    pub fn build<F>(mut self, func: F) -> Self
    where
        F: FnOnce(DataflowInputs<T>, &mut Scope<T>) -> Result<Vec<Stream<_, Vec<D>>>, Error> + Send + 'static,
    { /* ... */ }
}

/// Run one or more dataflows on the Worker Thread Pool + I/O runtime.
/// Returns output streams for each dataflow (one stream per worker in the last region).
pub async fn execute<T: Timestamp, D: Data>(
    runtime_config: RuntimeConfig,
    spec: DataflowSpec<T, D>,
) -> Result<DataflowHandle<T, D>, Error> {
    // Creates the Worker Thread Pool (or reuses existing),
    // sets up the I/O runtime, builds the dataflow graph,
    // returns a handle with output streams + metrics.
    ...
}

/// Handle to a running or completed dataflow.
pub struct DataflowHandle<T: Timestamp, D: Data> {
    /// Output streams — one per worker in the last execution region.
    /// The caller consumes these to receive dataflow results.
    pub outputs: Vec<OutputStream<T, D>>,
    /// Metrics for this dataflow (available after completion).
    pub metrics: tokio::sync::watch::Receiver<DataflowMetrics>,
    /// Cancellation token to stop the dataflow.
    pub cancel: CancellationToken,
}
```

### 5.5 Cancellation

A `CancellationToken` (from `tokio-util`) is threaded through the entire dataflow:

```rust
// User code
let token = CancellationToken::new();
let config = Config { cancellation_token: token.clone(), .. };

// Later, to cancel:
token.cancel();

// Inside operators:
tokio::select! {
    msg = input.recv() => { /* process */ },
    _ = cancellation_token.cancelled() => { return Ok(()); },
}
```

When cancelled:
1. All operator tasks observe the token and exit gracefully.
2. Channel senders are dropped, causing downstream `recv()` to return `None`.
3. The progress tracker drains and shuts down.
4. `execute()` returns with partial results or a `Cancelled` error.

### 5.7 Load Control

Since multiple dataflows share the same Worker Thread Pool, we need controls to prevent one dataflow from starving others:

- **Per-region concurrency limit**: the primary mechanism. Each execution region has a counting semaphore that limits how many tasks from that region can run concurrently (equal to the region's parallelism on this node).
- **Bounded input buffers** create natural backpressure — a fast producer's enqueue blocks when the downstream buffer is full.
- **Per-worker FIFO dispatch**: the task scheduler only dispatches the next task for a worker when the current one completes, preventing a single worker from flooding the pool.
- **Cross-dataflow fairness**: since the Worker Thread Pool's shared queue is FIFO across all dataflows, no single dataflow can monopolize threads indefinitely. The round-robin effect of interleaved task completions provides natural fairness.
- **Dynamic thread count**: under-utilized pools shrink (threads shut down), while burst load causes growth up to `max_threads`. This adapts to actual demand.

### 5.7a Backpressure

Backpressure is a critical mechanism that prevents fast operators from overwhelming slow ones. instancy implements **end-to-end backpressure** that traces all the way from any blocked downstream operator back to the input streams.

#### Backpressure Chain

```
Input Stream → Op A → [buffer full] → Op B (slow) → Op C → Output Stream
                 ↑
                 └── Op A's push returns Backpressure error
                     → Op A's activation yields back to scheduler
                     → Op A is re-queued with "blocked on output" status
                     → Input stream stops pulling new data (backpressure propagates upstream)
```

#### Local Backpressure (same process)

When an operator pushes data to a downstream operator's input buffer:
1. If the buffer has capacity, the push succeeds immediately.
2. If the buffer is full, the push returns `Error::Backpressure`.
3. The upstream operator's activation **yields** — it returns to the scheduler with a "blocked" status.
4. The scheduler re-enqueues the activation with a dependency on the downstream buffer draining.
5. When the downstream operator consumes data and frees buffer space, the blocked upstream activation is re-dispatched.

This chain propagates naturally: if Op C is slow, Op B's output buffer fills, then Op B blocks, then Op A's output buffer fills, then Op A blocks, then the input stream's read is paused.

#### Remote Backpressure (cross-process)

For inter-process channels:
1. The local send buffer is bounded. When full, the sending operator gets `Error::Backpressure` just like local channels.
2. TCP flow control provides additional backpressure at the network layer — if the remote receiver is slow, the TCP send buffer fills, which blocks the local write.
3. The `ConnectionPool` tracks per-connection send buffer utilization. When a connection's buffer exceeds a configurable high-water mark, the transport layer returns `Backpressure` to the operator.

#### Backpressure Metrics

Backpressure delays are measured and reported per-operator:

```rust
pub struct BackpressureMetrics {
    /// Number of times this operator was blocked by downstream backpressure.
    pub blocked_count: u64,
    /// Total time this operator spent blocked waiting for downstream capacity.
    pub blocked_duration: Duration,
    /// Maximum single blocking duration observed.
    pub max_blocked_duration: Duration,
}
```

These metrics are included in `OperatorMetrics` so users can identify bottleneck operators:

```rust
pub struct OperatorMetrics {
    pub name: String,
    pub index: usize,
    pub activations: u64,
    pub cpu_time: Duration,
    pub records_processed: u64,
    /// Backpressure statistics for this operator.
    pub backpressure: BackpressureMetrics,
}
```

**Diagnosis pattern**: If `Op A` has high `backpressure.blocked_duration` but low `cpu_time`, the bottleneck is downstream. Follow the chain until you find the operator with high `cpu_time` — that's the actual bottleneck.

### 5.7b Observability & Metrics

For production use, understanding the performance characteristics of each dataflow run is essential. instancy provides built-in observability:

#### Per-Dataflow CPU Time Tracking

Each dataflow run collects aggregate and per-operator CPU time metrics:

```rust
/// Metrics collected during a dataflow run.
#[derive(Clone, Debug)]
pub struct DataflowMetrics {
    /// Total wall-clock time from start to completion.
    pub wall_time: Duration,
    /// Total CPU time spent in operator logic across all workers.
    /// This is the sum of time spent inside operator closures, excluding
    /// time spent waiting on channels, semaphores, or I/O.
    pub total_cpu_time: Duration,
    /// Per-operator breakdown.
    pub operator_metrics: Vec<OperatorMetrics>,
}

#[derive(Clone, Debug)]
pub struct OperatorMetrics {
    /// Human-readable operator name (e.g., "Map", "Exchange").
    pub name: String,
    /// Operator index within the dataflow graph.
    pub index: usize,
    /// Number of times this operator was activated.
    pub activations: u64,
    /// Total CPU time spent in this operator's logic.
    pub cpu_time: Duration,
    /// Number of records processed (if tracked by the operator).
    pub records_processed: u64,
}
```

**Implementation**: Each operator activation is wrapped with `Instant::now()` before/after the user closure runs. The delta is accumulated per-operator using thread-local counters (no lock contention). At dataflow completion, counters are aggregated into `DataflowMetrics`.

The `execute()` function returns both the user's result and the metrics:

```rust
pub struct DataflowResult<R> {
    /// The user's return value from the dataflow builder.
    pub result: R,
    /// Performance metrics for this dataflow run.
    pub metrics: DataflowMetrics,
}
```

#### Structured Tracing Integration

All metrics are also emitted as `tracing` spans and events for integration with external observability stacks (Jaeger, OpenTelemetry, etc.):

```rust
// Example tracing output:
// SPAN instancy::operator{name="Exchange" index=3 worker=0}
//   activation_count=42, cpu_time_us=1234, records=50000
```

### 5.8 Message Envelope

Messages flowing through the dataflow carry data, control signals, and optional user-defined metadata in a unified envelope:

```rust
/// A message flowing through the dataflow graph.
/// Carries data, control signals, and optional user-defined metadata.
#[derive(Debug, Clone)]
pub struct Envelope<T: Timestamp, D, M = ()> {
    /// The payload: data records or a control signal.
    pub payload: Payload<T, D>,
    /// User-defined metadata that flows alongside the data.
    /// Examples: current sorting order, partition strategy hints,
    /// lineage information, schema version, compression hints.
    /// Defaults to `()` (no metadata) when not needed.
    pub metadata: M,
}

/// The core payload of a message.
#[derive(Debug, Clone)]
pub enum Payload<T: Timestamp, D> {
    /// A batch of data records at the given timestamp.
    Data {
        time: T,
        data: Vec<D>,
    },
    /// A control signal propagated through the dataflow.
    Control(ControlSignal<T>),
}

/// Control signals that flow in-band with data.
#[derive(Debug, Clone)]
pub enum ControlSignal<T: Timestamp> {
    /// An error occurred upstream. Downstream operators see this and
    /// can decide how to handle it based on the dataflow's error policy.
    Error {
        /// The operator that produced the error.
        source_operator: String,
        /// Human-readable error message.
        message: String,
    },
    /// Watermark: all future data will have timestamps >= this value.
    /// (Equivalent to frontier advancement.)
    Watermark(T),
}
```

#### User-Defined Metadata

The `M` type parameter on `Envelope` allows users to attach arbitrary metadata to messages that flows through the dataflow alongside data. This metadata is **transparent to operators** by default — it passes through unchanged unless an operator explicitly reads or modifies it.

```rust
/// Example: metadata tracking data properties for optimization.
#[derive(Debug, Clone)]
pub struct DataProperties {
    /// The data is sorted by this key (if known).
    pub sort_order: Option<SortOrder>,
    /// The data is partitioned by this strategy (if known).
    pub partition_info: Option<PartitionInfo>,
    /// Schema version for evolution support.
    pub schema_version: u32,
}

#[derive(Debug, Clone)]
pub enum SortOrder {
    Ascending(String),   // sorted ascending by named field
    Descending(String),  // sorted descending by named field
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Which key the data was partitioned by.
    pub key: String,
    /// Total number of partitions.
    pub total_partitions: usize,
    /// This batch's partition index.
    pub partition_index: usize,
}
```

**Usage**: Operators that preserve sort order can propagate `sort_order` metadata, while operators that shuffle data (exchange) can clear it. Downstream operators can use this metadata to skip redundant sorting or make optimization decisions:

```rust
// An operator that knows its output is sorted can set metadata
input
    .unary_with_metadata("sort", |handle, output| {
        let mut batch = handle.take_batch()?;
        batch.sort();
        output.give_with_metadata(batch, DataProperties {
            sort_order: Some(SortOrder::Ascending("key".into())),
            ..Default::default()
        })?;
        Ok(())
    });

// A downstream merge-join can check if input is already sorted
input
    .unary_with_metadata("merge_join", |handle, output| {
        if handle.metadata().sort_order.is_some() {
            // Fast path: data is already sorted, use merge join
        } else {
            // Slow path: sort first, then join
        }
        Ok(())
    });
```

**Design rationale**:
- The `M = ()` default means existing code that doesn't need metadata pays no cost (zero-sized type, optimized away).
- Metadata is typed — the compiler ensures consistency across the pipeline.
- Metadata flows in the same envelope as data, so it's always in sync (no separate side channel that can get out of order).
- Repartition operators (`exchange`, `rebalance`) can automatically clear or transform metadata that is invalidated by the shuffle.

**Design rationale for envelope structure**: By embedding control signals in the same channel as data, we avoid the need for separate side channels and ensure that control signals are ordered relative to data. An operator receiving a control error can:
- **Stop**: if the dataflow's error policy is `ErrorPolicy::Stop`, the operator drops its capabilities and exits.
- **Skip**: if the policy is `ErrorPolicy::Ignore`, the operator logs the error and continues processing subsequent data.

This also enables future extensions like per-record error tagging or priority signals without changing the channel infrastructure.

### 5.9 Error Handling Policy

Dataflows operate in environments where many types of failures can occur. The system classifies errors into categories and provides configurable policies for each.

#### 5.9.1 Error Taxonomy

```rust
/// Categories of errors that can occur during dataflow execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// The operator's user-provided logic returned an error.
    /// Examples: invalid data format, business logic violation, missing lookup key.
    ComputeError,
    /// The operator's user-provided logic panicked (caught via catch_unwind).
    /// Examples: index out of bounds, unwrap on None, assertion failure in user code.
    ComputePanic,
    /// An operator exceeded its time budget for a single activation.
    /// Indicates runaway computation or unexpectedly large input.
    ComputeTimeout,
    /// A network operation failed: connection lost, send/receive timeout.
    /// Applies to inter-process data exchange channels.
    NetworkError,
    /// A remote worker is no longer reachable (heartbeat lost, process crashed).
    /// The affected region/partition cannot make progress.
    WorkerLost,
    /// The dataflow was explicitly cancelled via CancellationToken.
    Cancelled,
    /// An internal system error (bug in the framework, not user code).
    Internal,
}
```

#### 5.9.2 Error Policy Configuration

Each dataflow specifies how errors should be handled, with per-category granularity:

```rust
/// Determines how operator errors are handled within a dataflow.
#[derive(Clone, Debug)]
pub struct ErrorPolicy {
    /// Policy for user compute errors (operator logic returns Err).
    pub on_compute_error: ErrorAction,
    /// Policy for user compute panics (caught via catch_unwind).
    pub on_compute_panic: ErrorAction,
    /// Policy for compute timeouts.
    pub on_compute_timeout: ErrorAction,
    /// Policy for network failures.
    pub on_network_error: ErrorAction,
    /// Policy for lost workers.
    pub on_worker_lost: ErrorAction,
    /// Optional callback invoked for every error regardless of action.
    /// Used for logging, alerting, dead-letter routing, metrics.
    pub on_error_callback: Option<Arc<dyn Fn(&Error, ErrorKind) + Send + Sync>>,
}

/// What to do when an error of a specific kind occurs.
#[derive(Clone, Debug, Default)]
pub enum ErrorAction {
    /// Stop the entire dataflow immediately.
    /// The error propagates to execute() as Err(e).
    #[default]
    Stop,
    /// Skip the offending record/batch and continue processing.
    /// The error is logged and counted in DataflowMetrics.
    Skip,
    /// Retry the operation up to N times with exponential backoff.
    /// After exhausting retries, falls back to the specified action.
    Retry {
        max_retries: u32,
        backoff_base: Duration,
        fallback: Box<ErrorAction>,
    },
}

impl Default for ErrorPolicy {
    fn default() -> Self {
        Self {
            on_compute_error: ErrorAction::Stop,
            on_compute_panic: ErrorAction::Stop,
            on_compute_timeout: ErrorAction::Stop,
            on_network_error: ErrorAction::Stop,
            on_worker_lost: ErrorAction::Stop,
            on_error_callback: None,
        }
    }
}
```

Convenience constructors:

```rust
impl ErrorPolicy {
    /// Stop on any error (default, safest).
    pub fn strict() -> Self { Self::default() }

    /// Skip compute errors/panics, stop on infrastructure failures.
    pub fn best_effort() -> Self {
        Self {
            on_compute_error: ErrorAction::Skip,
            on_compute_panic: ErrorAction::Skip,
            on_compute_timeout: ErrorAction::Skip,
            on_network_error: ErrorAction::Stop,
            on_worker_lost: ErrorAction::Stop,
            on_error_callback: None,
        }
    }

    /// Retry network errors, skip compute errors, stop on worker loss.
    pub fn resilient() -> Self {
        Self {
            on_compute_error: ErrorAction::Skip,
            on_compute_panic: ErrorAction::Stop,
            on_compute_timeout: ErrorAction::Skip,
            on_network_error: ErrorAction::Retry {
                max_retries: 3,
                backoff_base: Duration::from_millis(100),
                fallback: Box::new(ErrorAction::Stop),
            },
            on_worker_lost: ErrorAction::Stop,
            on_error_callback: None,
        }
    }
}
```

The policy is set in `DataflowConfig`:

```rust
pub struct DataflowConfig {
    pub cluster: ClusterTopology,
    pub cancellation_token: CancellationToken,
    /// How to handle errors. Default: strict (stop on any error).
    pub error_policy: ErrorPolicy,
}
```

#### 5.9.3 Operator Logic Error Handling

Custom operator logic (unary, binary closures) returns `Result<()>`. When an error is returned:

```
Operator activate() returns Err(e)
  │
  ├─ Classify: ErrorKind::ComputeError
  │
  ├─ Invoke on_error_callback (if set) for observability
  │
  └─ Apply policy.on_compute_error:
       ├─ Stop → send Control::Error downstream → all operators exit → execute() returns Err
       ├─ Skip → discard current batch, log, increment error_count → continue next activation
       └─ Retry → re-invoke activate() with same input → on exhaustion, apply fallback
```

For panics, the runtime wraps operator activation in `std::panic::catch_unwind`:

```rust
// Conceptual runtime activation loop
let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    operator.activate(&mut input, &mut output)
}));

match result {
    Ok(Ok(())) => { /* success, drain output */ }
    Ok(Err(e)) => { /* ComputeError — apply policy */ }
    Err(panic_payload) => { /* ComputePanic — apply policy */ }
}
```

This ensures user logic panics never crash the worker thread pool — they are caught, classified, and handled according to policy.

#### 5.9.4 Compute Timeout

Each operator activation has an optional time budget:

```rust
pub struct DataflowConfig {
    // ...
    /// Maximum wall-clock time for a single operator activation.
    /// None means no timeout (operator runs until completion).
    pub activation_timeout: Option<Duration>,
}
```

If an activation exceeds the timeout:
1. The runtime signals the activation to abort (via a flag checked periodically).
2. If the operator does not exit within a grace period, the activation is forcibly cancelled.
3. The error is classified as `ErrorKind::ComputeTimeout` and handled per policy.

**Design note**: Since operator logic runs synchronously within an async task, true preemption is not possible. The operator must cooperate by checking a timeout flag periodically for large batches. The runtime provides a helper:

```rust
/// Check if the current activation has exceeded its time budget.
/// Call this periodically in long-running operator logic.
pub fn check_timeout(ctx: &ActivationContext) -> Result<()> {
    if ctx.is_timed_out() {
        Err(Error::timeout("operator activation exceeded time budget"))
    } else {
        Ok(())
    }
}
```

#### 5.9.5 Network and Worker Failures

| Failure | Detection | Handling |
|---|---|---|
| Connection timeout | Send/recv deadline | `ErrorKind::NetworkError` → policy |
| Connection reset | I/O error on channel | `ErrorKind::NetworkError` → reconnect via pool, then retry |
| Worker lost | Heartbeat timeout | `ErrorKind::WorkerLost` → policy |
| Remote process crash | Connection pool detects all connections to peer failed | `ErrorKind::WorkerLost` → policy |

For `ErrorAction::Retry` on network errors:
1. The failed send/receive is retried after exponential backoff.
2. If the connection is dead, the connection pool establishes a new one.
3. After `max_retries`, the fallback action (typically `Stop`) is applied.

For `ErrorAction::Stop` on worker loss:
1. The affected operator(s) are terminated.
2. A `Control::Error` message propagates through the dataflow.
3. `execute()` returns with the error describing which worker was lost.

#### 5.9.6 Error Propagation Flow

```
Error in Operator B (worker 2)
  │
  ├─ Policy says Stop:
  │    ├─ B sends Control::Error to downstream operators
  │    ├─ Downstream operators receive error, drop capabilities, exit
  │    ├─ Progress tracker detects all operators done
  │    └─ execute() returns Err(OperatorError { source: "B", worker: 2, cause: ... })
  │
  └─ Policy says Skip:
       ├─ B discards current input batch
       ├─ B increments metrics.skipped_errors
       ├─ B continues processing next batch normally
       └─ execute() eventually returns Ok(()) with metrics showing skipped count
```

#### 5.9.7 Error Context and Reporting

Errors carry rich context for debugging:

```rust
/// Error produced during dataflow execution with full context.
pub struct DataflowError {
    /// The underlying error.
    pub cause: Error,
    /// Which category of error occurred.
    pub kind: ErrorKind,
    /// The operator that produced the error.
    pub operator_name: String,
    /// The operator's index in the scope.
    pub operator_index: usize,
    /// Which worker instance hit the error.
    pub worker_index: usize,
    /// The timestamp(s) being processed when the error occurred.
    pub at_timestamp: Option<String>,
    /// Number of records in the batch being processed.
    pub batch_size: Option<usize>,
    /// How many times this operation was retried before failing.
    pub retry_count: u32,
}
```

This structured error enables:
- Pinpointing exactly where failures occur in complex dataflows
- Correlating errors with specific timestamps/epochs
- Understanding retry behavior before final failure
- Dead-letter routing with full provenance

### 5.10 Per-Stage Dynamic Parallelism (Execution Regions)

In traditional timely-dataflow, every operator in a dataflow uses the same number of workers. This is wasteful for operations like global aggregation (funneling to 1 worker) or when different stages have different computational needs.

instancy introduces **execution regions** — groups of operators that share a parallelism level. Different regions can have different parallelism, with explicit repartition operators at region boundaries.

#### Motivation

```
Problem: 4-worker uniform parallelism

  from_stream(4) → map(4) → global_sort(4) → sink(4)
                                    ↑
                         Only worker 0 has data.
                         Workers 1-3 are idle but hold
                         logical capacity.

Solution: per-region parallelism

  from_stream [region: 4] → map [region: 16] → global_sort [region: 1] → sink [region: 4]
                    ↑ rebalance          ↑ gather               ↑ rebalance
              Explicit repartition at each boundary.
```

#### Execution Regions

An **execution region** is a set of contiguous operators that share:
- The same **parallelism** (number of logical worker replicas)
- The same **placement policy** (how replicas are distributed across nodes)

Within a region, operators are connected by pipeline-local channels (no shuffle). Between regions, explicit repartition operators handle data redistribution.

```rust
/// An execution region defines a group of operators with shared parallelism.
#[derive(Clone, Debug)]
pub struct Region {
    /// Number of logical workers (replicas) for operators in this region.
    pub parallelism: usize,
    /// How replicas are placed across cluster nodes.
    /// Default: proportional to each node's declared worker count.
    pub placement: PlacementPolicy,
}

/// How region replicas are distributed across nodes.
#[derive(Clone, Debug, Default)]
pub enum PlacementPolicy {
    /// Distribute replicas proportionally to each node's capacity.
    /// A node with 8 workers gets 2x the replicas of a node with 4 workers.
    #[default]
    Proportional,
    /// Distribute replicas evenly (round-robin) across all nodes.
    RoundRobin,
    /// Pin all replicas to a specific node (e.g., for local aggregation).
    Pinned { node_index: usize },
}
```

#### Repartition Operators

When connecting operators across regions with different parallelism, the user specifies **how** data is redistributed. The system never auto-selects a distribution strategy.

| Operator | Semantics | Use case |
|---|---|---|
| `exchange(key_fn)` | Hash-partition by key to target parallelism | Shuffle for joins, group-by |
| `rebalance()` | Round-robin across target replicas | Even load distribution when key doesn't matter |
| `gather()` | All data → single replica (parallelism 1) | Global aggregation, sorting |
| `broadcast()` | Clone all data to every target replica | Reference data distribution |

These operators are **required** at parallelism boundaries. Connecting two operators with different parallelism without an explicit repartition is a compile-time error.

```rust
// ✅ Correct: explicit repartition at boundary
let result = input
    .with_parallelism(4)
    .map(|x| x * 2)
    .exchange(|x| hash(x))   // explicit: hash-partition into 16 replicas
    .with_parallelism(16)
    .filter(|x| x > 100);

// ❌ Error: parallelism changes without repartition
let result = input
    .with_parallelism(4)
    .map(|x| x * 2)
    .with_parallelism(16)    // compile error: no repartition between 4→16
    .filter(|x| x > 100);
```

#### API

There are two equivalent styles for specifying execution regions:

**Style 1: Inline `.with_parallelism(n)`** — creates a new region implicitly:

```rust
let output = scope
    .input_from(streams)              // region A: parallelism = 4
    .with_parallelism(4)
    .map(|x| expensive_compute(x))
    .filter(|x| x.is_valid())
    .exchange(|x| hash(&x.key))       // repartition: 4 → 16
    .with_parallelism(16)
    .map(|x| transform(x))
    .gather()                          // repartition: 16 → 1
    .with_parallelism(1)
    .aggregate(Vec::new, |acc, x| acc.push(x))
    .rebalance()                       // repartition: 1 → 4
    .with_parallelism(4)
    .sink(output_streams);
```

**Style 2: Named regions** — more explicit, better for complex graphs:

```rust
let ingest  = Region { parallelism: 4,  placement: PlacementPolicy::Proportional };
let compute = Region { parallelism: 16, placement: PlacementPolicy::Proportional };
let global  = Region { parallelism: 1,  placement: PlacementPolicy::Pinned { node_index: 0 } };
let egress  = Region { parallelism: 4,  placement: PlacementPolicy::Proportional };

let input = scope
    .input_from(streams)
    .in_region(&ingest);

let processed = input
    .map(|x| expensive_compute(x))
    .exchange(|x| hash(&x.key))
    .in_region(&compute)
    .map(|x| transform(x));

let aggregated = processed
    .gather()
    .in_region(&global)
    .aggregate(...);

aggregated
    .rebalance()
    .in_region(&egress)
    .sink(outputs);
```

#### Default Behavior

If no `.with_parallelism()` or `.in_region()` is specified, **all operators use the dataflow's default worker count** from `ClusterTopology`. This is fully backward-compatible with the uniform parallelism model.

```rust
// These two are equivalent:
// 1. Explicit
input.with_parallelism(cluster.total_workers()).map(|x| x * 2);
// 2. Default (uses dataflow worker count)
input.map(|x| x * 2);
```

#### Progress Tracking with Regions

Each execution region maintains its own **per-replica progress frontiers**. The reachability tracker is extended to understand region boundaries:

- **Within a region**: Progress flows through operator ports as today. Each replica tracks its own frontier independently.
- **At region boundaries**: The repartition operator aggregates upstream replicas' frontiers into the downstream region's input frontier. A downstream replica's input frontier only advances when **all** upstream replicas that send to it have advanced past that timestamp.

```
Region A (parallelism=4)          Region B (parallelism=16)
┌──────┐                          ┌──────┐
│ R0   │──┐                   ┌──▶│ R0   │
│ R1   │──┤  exchange(hash)   ├──▶│ R1   │
│ R2   │──┤  ─────────────▶   ├──▶│ ...  │
│ R3   │──┘                   └──▶│ R15  │
└──────┘                          └──────┘

Progress: B's replica input frontier = min of A's replica output
          frontiers that route to that B replica.
```

#### Restrictions

For v1, the following restrictions apply:

1. **No parallelism changes inside cycles/loops**: All operators within a `scope.iterative()` loop must share the same parallelism. Repartition must happen outside the loop boundary. This avoids complex per-replica progress accounting through feedback edges.

2. **Binary operators require co-partitioned inputs**: When a binary operator receives inputs from two different upstream regions, both must have been repartitioned to the same parallelism with compatible distribution (same key function for `exchange`). The system validates this at graph construction time.

3. **Broadcast/broadcast_local use the current region's parallelism**: These operators do not create region boundaries.

#### Implementation Notes

- **Logical WorkerId scope**: Within a region, replicas are assigned local indices `0..parallelism`. The global `WorkerId` mapping is: `global_id = region_base + local_replica_index`.
- **Channel allocation**: Within a region → pipeline channels (no shuffle). Between regions → repartition channels with `upstream_replicas × downstream_replicas` routing (multiplexed over pooled connections for inter-process).
- **Semaphore per region**: Each region has its own concurrency semaphore with `min(parallelism, local_replicas_on_this_node)` permits.

---

## 6. Communication Layer

The communication layer implements the physical delivery mechanisms behind the `TransportProvider` trait (§4.5). At the logical layer, operators only see `Push` and `Pull` endpoints. The communication layer provides the concrete implementations.

### 6.1 Intra-Process Channels

For operators within the same process (where `TransportProvider::is_local()` returns true), data is exchanged via **bounded in-memory buffers**. No serialization — data moves as owned Rust values. Since operators run on the Custom Worker Thread Pool (not Tokio), channels use a lock-free bounded queue rather than `tokio::sync::mpsc`.

```rust
/// Intra-process buffer between operators.
/// Bounded, lock-free SPSC or MPSC queue depending on topology.
pub struct OperatorBuffer<T: Timestamp, D, M = ()> {
    /// Bounded queue of envelopes.
    queue: BoundedQueue<Envelope<T, D, M>>,
    /// Capacity (backpressure kicks in when full).
    capacity: usize,
}
```

When an upstream operator produces output, it writes directly into the downstream operator's input buffer. If the buffer is full, the upstream operator's task yields (returns to the scheduler with a "blocked" status), and will be re-dispatched when space becomes available. This provides natural backpressure without async machinery.

`Envelope` (defined in §5.8) carries data batches, control signals, and user-defined metadata through the same buffer.

### 6.2 Inter-Process Connections: ConnectionManager

Connection establishment is **fully delegated to the application**. The library does not know how to open TCP ports, listen for connections, or negotiate TLS — it only knows that it needs a bidirectional byte stream to a given peer. The application provides a `ConnectionManager` component that handles the entire connection lifecycle.

This design supports arbitrarily complex networking setups:
- The application might use an actor framework that sends a command to a remote node saying "open a TCP port for me", waits for the port assignment, then connects.
- The application might use a service mesh, a QUIC transport, Unix domain sockets, or an in-memory loopback.
- The application fully controls TLS certificate management, mutual authentication, and connection negotiation.

```rust
/// Identifies a remote peer (process index in the cluster).
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct PeerId {
    pub process: usize,
    // Extensible with additional metadata if needed
}

/// A request from the connection pool to the application to establish a new connection.
/// The pool sends this when it needs a new connection to a peer (either the first connection
/// or to grow the pool / replace a dead connection).
#[derive(Debug)]
pub struct ConnectionRequest {
    /// The target peer to connect to.
    pub peer_id: PeerId,
    /// The local process identity (so the remote side knows who is connecting).
    pub local_id: PeerId,
    /// An opaque request ID for correlation.
    pub request_id: u64,
}

/// Application-implemented trait that establishes connections on behalf of the library.
///
/// The library calls `establish` when the pool needs a new connection to a peer.
/// The application is free to use any mechanism: direct TCP connect, asking a remote
/// actor to open a listener, negotiating through a control plane, etc.
///
/// # Example: Actor-framework integration
/// ```rust,ignore
/// struct ActorConnectionManager {
///     actor_system: ActorRef<NetworkCoordinator>,
/// }
///
/// #[async_trait]
/// impl ConnectionManager for ActorConnectionManager {
///     type Connection = TlsStream<TcpStream>;
///
///     async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Error> {
///         // 1. Ask the remote node's actor to open a listener
///         let port = self.actor_system
///             .ask(OpenPort { for_peer: request.local_id })
///             .await?;
///
///         // 2. Connect to the assigned port
///         let tcp = TcpStream::connect((remote_host, port)).await?;
///
///         // 3. Perform TLS handshake
///         let tls = tls_connector.connect(tcp).await?;
///         Ok(tls)
///     }
/// }
/// ```
#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    /// The bidirectional byte-stream type returned by the manager.
    /// Could be TcpStream, TlsStream, QuicStream, or anything implementing
    /// AsyncRead + AsyncWrite.
    type Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// Establish a new connection to the given peer.
    ///
    /// This is called by the connection pool when it needs a new connection — either
    /// to grow the pool, replace a failed connection, or establish the first connection
    /// to a peer. The application has complete freedom in how it creates the connection.
    ///
    /// The method should return a ready-to-use bidirectional byte stream. Any
    /// handshaking, authentication, or negotiation should be completed before returning.
    async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Error>;
}
```

**Default implementation**: A simple `TcpConnectionManager` is provided for basic use cases:

```rust
/// Default manager that does direct TCP connect to known addresses.
pub struct TcpConnectionManager {
    /// Map from peer process index to its address.
    peer_addresses: HashMap<usize, SocketAddr>,
}

#[async_trait]
impl ConnectionManager for TcpConnectionManager {
    type Connection = TcpStream;

    async fn establish(&self, request: ConnectionRequest) -> Result<TcpStream, Error> {
        let addr = self.peer_addresses.get(&request.peer_id.process)
            .ok_or_else(|| Error::Connection {
                peer_id: request.peer_id.clone(),
                source: "unknown peer".into(),
            })?;
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}
```

### 6.3 Connection Pooling

The `ConnectionPool` is the library-internal component that manages established connections. It calls `ConnectionManager::establish()` when it needs a new connection and dynamically scales the number of connections per peer based on load.

**Pool lifecycle:**
1. **First use**: When data needs to be sent to a peer, the pool calls `manager.establish(request)`.
2. **Scale up**: Under high throughput, the pool adds connections up to `max_connections_per_peer` by calling `manager.establish()` for each new connection.
3. **Reuse**: After a multiplexed channel finishes using a connection, it's returned to the pool.
4. **Health check**: The pool periodically pings idle connections; dead ones are dropped and replaced via `manager.establish()`.
5. **Scale down**: Connections idle beyond `idle_timeout` are closed, shrinking the pool back toward `min_connections_per_peer`.
6. **Reconnect**: On connection failure, the pool calls `manager.establish()` again.

```rust
pub struct ConnectionPool<M: ConnectionManager> {
    manager: Arc<M>,
    local_id: PeerId,
    pools: DashMap<PeerId, Vec<PooledConnection<M::Connection>>>,
    config: PoolConfig,
    next_request_id: AtomicU64,
}

impl<M: ConnectionManager> ConnectionPool<M> {
    /// Get or create a connection to the given peer.
    /// If no idle connection is available and the pool is not at max capacity,
    /// calls `manager.establish()` to create a new one.
    /// If the pool is at max capacity, waits for a connection to be returned.
    pub async fn acquire(&self, peer_id: &PeerId) -> Result<PoolGuard<M::Connection>, Error> {
        // Try to take an idle connection from the pool
        if let Some(conn) = self.try_take_idle(peer_id) {
            return Ok(conn);
        }
        
        // If below max, ask the application to establish a new connection
        if self.current_count(peer_id) < self.config.max_connections_per_peer {
            let request = ConnectionRequest {
                peer_id: peer_id.clone(),
                local_id: self.local_id.clone(),
                request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
            };
            let connection = self.manager.establish(request).await?;
            return Ok(self.wrap(peer_id.clone(), connection));
        }
        
        // At capacity — wait for a connection to be released
        self.wait_for_release(peer_id).await
    }
    
    /// Return a connection to the pool for reuse.
    /// Called automatically when `PoolGuard` is dropped.
    fn release(&self, peer_id: PeerId, conn: M::Connection) { ... }
}

pub struct PoolConfig {
    /// Minimum connections per peer (maintained even when idle).
    /// Default: 1.
    pub min_connections_per_peer: usize,
    /// Max connections per peer. Pool grows up to this under load.
    /// Default: 4.
    pub max_connections_per_peer: usize,
    /// Idle timeout before closing connections above min_connections_per_peer.
    /// Default: 60s.
    pub idle_timeout: Duration,
    /// Health check interval (default: 30s).
    pub health_check_interval: Duration,
    /// Max time to wait for `establish()` to complete (default: 30s).
    pub connect_timeout: Duration,
}
```

**Key design points**:
- The pool **only** calls `ConnectionManager::establish()` — it never opens sockets, binds ports, or does any networking itself.
- The pool **dynamically scales** between `min_connections_per_peer` and `max_connections_per_peer` based on demand.
- When load is low, idle connections above the minimum are reclaimed after `idle_timeout`.
- The application's `ConnectionManager` is the single point of control for all connection establishment — the pool just tells it when to create or destroy connections.

### 6.4 Wire Protocol

Each connection carries multiplexed channels using a simple framing protocol:

```
┌───────────┬───────────┬───────────┬──────────────────┐
│ dataflow_id│ channel_id│ length    │ payload (codec)  │
│ (u64)      │ (u64)     │ (u32)     │ (variable)       │
└───────────┴───────────┴───────────┴──────────────────┘
```

The `dataflow_id` field ensures that frames from different dataflows sharing the same pooled connection are never misrouted. Each dataflow is assigned a cluster-unique `DataflowId` at construction time.

A background demux task reads frames from a connection and dispatches them to the appropriate (dataflow, channel) pair's `mpsc::Sender`.

### 6.5 Dataflow Isolation

Multiple dataflows can run concurrently on the same cluster, sharing the same worker thread pool and the same pooled network connections. Isolation between dataflows is maintained at multiple levels:

#### Logical Isolation

Each dataflow is an independent computation graph with:
- Its own `DataflowId` (cluster-unique `u64`, assigned by the runtime)
- Its own operator registry (operator index 3 in dataflow A ≠ operator index 3 in dataflow B)
- Its own channel wiring (each edge gets push/pull endpoints scoped to that dataflow)
- Its own progress tracker instance (frontiers are independent)
- Its own `DataflowMetrics` and `CancellationToken`

Operators in dataflow A **never** share input/output buffers with operators in dataflow B. The `TransportProvider` resolves `LogicalTarget` using the specific dataflow's channel map — there is no global operator namespace.

#### Physical Isolation on Shared Connections

When two dataflows share a pooled TCP connection to the same peer:
- Each frame includes a `dataflow_id` field in its wire header
- The demuxer dispatches frames to the correct dataflow's channel receivers based on `(dataflow_id, channel_id)` pair
- A frame with an unknown `dataflow_id` is logged and dropped (e.g., if the dataflow was cancelled but in-flight frames remain)

#### DataflowId Assignment

```rust
/// Cluster-unique identifier for a running dataflow instance.
///
/// This is a **logical** concept — it identifies a specific computation graph
/// instance. Multiple dataflows with different IDs can run concurrently on
/// the same physical infrastructure.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DataflowId(pub u64);
```

DataflowIds are assigned by the local runtime using an atomic counter. To ensure cluster-wide uniqueness without coordination, the ID encodes `(node_index << 48) | local_sequence`. This guarantees uniqueness as long as each node processes fewer than 2^48 dataflows (≈281 trillion).

#### Worker Sharing

Logical workers (`WorkerId`) are **per-dataflow**. Dataflow A's `WorkerId(0)` and dataflow B's `WorkerId(0)` are distinct logical entities. However, they may execute on the same physical OS thread in the worker pool. The scheduler distinguishes them by `(DataflowId, WorkerId)` to maintain per-worker FIFO ordering.

#### Operator Identity

An `operator_index` (usize) is only unique **within** a single dataflow's operator registry. To globally identify an operator across the cluster, the full identity is `(DataflowId, operator_index)`. This composite key is used in metrics collection, tracing spans, and diagnostics. There is no single `GlobalOperatorId` struct — instead, the pairing is carried contextually wherever cross-dataflow disambiguation is needed.

#### Summary: Where DataflowId Appears

| Layer | How DataflowId is Used |
|---|---|
| Logical | Scopes operator/channel allocation; included in LogicalTarget |
| Scheduler | `(DataflowId, WorkerId)` ensures FIFO per logical worker per dataflow |
| Transport (intra-process) | Buffers are per-dataflow — no sharing |
| Transport (inter-process) | Frame header field for demux routing |
| Progress | Each dataflow has independent frontier tracking |
| Metrics | Each dataflow has its own DataflowMetrics |

---

## 7. Pluggable Serialization

### 7.1 Codec Trait

```rust
/// Trait for serializing/deserializing data on the wire.
pub trait Codec<T>: Send + Sync + 'static {
    /// Serializes `item` into `buf`. Returns bytes written.
    fn encode(&self, item: &T, buf: &mut BytesMut) -> Result<(), Error>;
    
    /// Deserializes an item from `buf`, advancing the cursor.
    fn decode(&self, buf: &mut Bytes) -> Result<T, Error>;
}
```

### 7.2 Default: Bincode

```rust
pub struct BincodeCodec<T> {
    _phantom: PhantomData<T>,
    config: bincode::config::Configuration,
}

impl<T: Serialize + DeserializeOwned> Codec<T> for BincodeCodec<T> { ... }
```

### 7.3 Data Bounds

```rust
/// Data that can be exchanged across workers within a process.
pub trait Data: Clone + Send + Sync + 'static {}

/// Data that can be exchanged across processes (requires serialization).
pub trait ExchangeData: Data {
    /// The codec type used for serialization.
    type Codec: Codec<Self>;
    
    /// Returns the codec to use for this data type.
    fn codec() -> Self::Codec;
}
```

**Alternative**: supply the codec at channel allocation time rather than tying it to the data type, for maximum flexibility:

```rust
fn exchange_with_codec<D, C>(
    &self,
    route: impl Fn(&D) -> u64 + Send + Sync + 'static,
    codec: C,
) -> DataStream<S, Vec<D>>
where
    C: Codec<Vec<D>>;
```

---

## 8. Error Handling

### 8.1 Error Type

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Codec(#[from] Box<dyn std::error::Error + Send + Sync>),
    
    #[error("Connection error: peer {peer_id:?}: {source}")]
    Connection {
        peer_id: PeerId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    
    #[error("Dataflow cancelled")]
    Cancelled,
    
    #[error("Progress tracking error: {0}")]
    Progress(String),
    
    #[error("Operator error in '{operator}': {source}")]
    Operator {
        operator: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    
    #[error("{0}")]
    Custom(String),
}
```

### 8.2 Error Propagation

- Operators return `Result<(), Error>`.
- When an operator task fails, it drops its output channels and capabilities.
- Downstream operators observe closed channels and can decide to propagate or handle the error.
- The `execute()` function collects errors from all worker tasks and returns them.
- **No panics** in library code. All `unwrap()` calls replaced with `?` or explicit error handling.

---

## 9. Operators

### 9.1 Core Operators (this crate)

| Operator | Description |
|---|---|
| `from_stream` | Binds an external `TimestampedInput` async stream into the dataflow; manages capabilities automatically |
| `unary` | One input, one output; user-supplied async closure |
| `binary` | Two inputs, one output; user-supplied async closure |
| `branch` / `ok_err` | One input, two outputs; partition by predicate |
| `feedback` / `loop_variable` | Creates a feedback edge for iteration with timestamp advancement |
| `exchange` | Repartitions data across workers by a routing function (hash-based); creates region boundary when parallelism changes |
| `rebalance` | Round-robin distribution across target replicas; used at region boundaries when key doesn't matter |
| `gather` | Funnels all data to a single replica (parallelism 1); used for global aggregation |
| `broadcast` | Sends each record to **all** workers across the cluster (clones data cross-process via serialization) |
| `broadcast_local` | Sends each record to all workers **within the same process** (cheap clone, no serialization) |
| `delay` | Holds data until the frontier advances past a specified timestamp; useful for windowing and time-based buffering |
| `concat` | Merges multiple streams into one |
| `inspect` | Side-effect observation (logging, debugging) |
| `probe` | Observe frontier progress; async `changed()` method |

### 9.2 Unary Operator API

```rust
pub trait Operator<S: Scope, C: Container> {
    /// Creates a unary operator with one input and one output.
    /// The closure is synchronous — it runs on the Worker Thread Pool thread directly.
    fn unary<C2, L>(
        &self,
        name: &str,
        logic: L,
    ) -> DataStream<S, C2>
    where
        C2: Container,
        L: FnMut(
            &mut InputHandle<S::Timestamp, C>,
            &mut OutputHandle<S::Timestamp, C2>,
            &Notificator<S::Timestamp>,
        ) -> Result<(), Error> + Send + 'static;
    
    /// Variant that also provides access to user-defined metadata.
    /// Operators can read upstream metadata and modify it for downstream consumers.
    fn unary_with_metadata<C2, M, L>(
        &self,
        name: &str,
        logic: L,
    ) -> DataStream<S, C2>
    where
        C2: Container,
        M: Clone + Send + Sync + 'static,
        L: FnMut(
            &mut InputHandle<S::Timestamp, C>,
            &mut OutputHandle<S::Timestamp, C2>,
            &Notificator<S::Timestamp>,
            &mut M,  // mutable reference to metadata
        ) -> Result<(), Error> + Send + 'static;
}
```

### 9.3 Binary Operator API

```rust
fn binary<C2, C3, L>(
    &self,
    other: &DataStream<S, C2>,
    name: &str,
    logic: L,
) -> DataStream<S, C3>
where
    L: FnMut(
        &mut InputHandle<S::Timestamp, C>,
        &mut InputHandle<S::Timestamp, C2>,
        &mut OutputHandle<S::Timestamp, C3>,
        &Notificator<S::Timestamp>,
    ) -> Result<(), Error> + Send + 'static;
```

### 9.4 Branch / OkErr

```rust
fn branch(
    &self,
    condition: impl Fn(&T) -> bool + Send + Sync + 'static,
) -> (DataStream<S, C>, DataStream<S, C>);

fn ok_err<O, E>(
    &self,
    logic: impl Fn(T) -> Result<O, E> + Send + Sync + 'static,
) -> (DataStream<S, Vec<O>>, DataStream<S, Vec<E>>);
```

### 9.5 Feedback / Loops

Loops use the same `enter` / `leave` / `feedback` pattern as timely-dataflow:

```rust
scope.iterative::<u32, _, _>(|inner_scope| {
    let (handle, cycle) = inner_scope.feedback(1);  // advance timestamp by 1
    
    let result = input
        .enter(inner_scope)
        .concat(&cycle)
        .unary("step", |input, output, notificator| {
            // iteration logic
            Ok(())
        });
    
    result.connect_loop(handle);
    result.leave()
});
```

### 9.6 Delay Operator

The `delay` operator buffers incoming data and re-timestamps it, releasing the data only when the frontier advances past the original timestamp. This is essential for windowing, time-based aggregation, and ensuring data is processed in timestamp order.

```rust
/// Delays data by re-assigning timestamps according to a user-supplied function.
/// Data at timestamp `t` is held until the input frontier advances past `t`,
/// then released at the new timestamp returned by `delay_fn`.
fn delay<F>(
    &self,
    delay_fn: F,
) -> DataStream<S, C>
where
    F: Fn(&T, &C::Item) -> T + Send + Sync + 'static;

/// Delays all data at timestamp `t` to a single new timestamp computed from `t`.
/// Simpler version when the delay depends only on the timestamp, not the data.
fn delay_batch<F>(
    &self,
    delay_fn: F,
) -> DataStream<S, C>
where
    F: Fn(&T) -> T + Send + Sync + 'static;
```

**Semantics**: The operator holds a capability for each output timestamp that has buffered data. When the input frontier advances past a buffered timestamp, the data is emitted at the delayed timestamp and the capability is released. This ensures downstream operators see correct frontier progress.

**Use cases**:
- **Windowing**: `delay_batch(|t| t / window_size * window_size)` groups data into fixed windows.
- **Ordering**: `delay_batch(|t| *t)` (identity) buffers data until the frontier confirms no more data at `t` will arrive.
- **Rate limiting**: delay data to spread output over time.

### 9.7 Extension Point

Extension crates add operators by implementing traits on `DataStream`:

```rust
// In crate `instancy-extras`
pub trait MapOperator<S: Scope, T: Data> {
    fn map<U: Data>(
        &self,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> DataStream<S, Vec<U>>;
}

impl<S: Scope, T: Data> MapOperator<S, T> for DataStream<S, Vec<T>> {
    fn map<U: Data>(&self, f: impl Fn(T) -> U + Send + Sync + 'static) -> DataStream<S, Vec<U>> {
        self.unary("Map", move |input, output, _notificator| {
            while let Some((time, data)) = input.next()? {
                let mut session = output.session(&time);
                for item in data.drain(..) {
                    session.give(f(item))?;
                }
            }
            Ok(())
        })
    }
}
```

---

## 10. User-Facing API Example

```rust
use instancy::prelude::*;
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let token = CancellationToken::new();

    let config = Config {
        communication: CommunicationConfig::Process { workers: 4 },
        worker: WorkerConfig {
            progress_mode: ProgressMode::Demand,
            cancellation_token: token.clone(),
            ..Default::default()
        },
        ..Default::default()
    };

    // Create an async stream of timestamped input events.
    // This could come from Kafka, a file, a network socket, a channel, etc.
    let input_stream = futures::stream::iter((0..10u64).map(|round| {
        InputEvent::Data(round, vec![round])
    }));

    execute(config, move |worker| {
        let index = worker.index();

        DataflowSpec::new()
            .input("numbers", input_stream)
            .build(move |inputs, scope| {
                let numbers = inputs.get::<u64>("numbers")?;

                numbers
                    .exchange(|x: &u64| *x)
                    .inspect(move |x| println!("worker {index}: {x}"))
                    .probe();  // completion tracking

                Ok(())
            })
    }).await?;

    Ok(())
}
```

### Driven by external async channels (e.g., from Kafka or actor messages):

```rust
let (tx, rx) = tokio::sync::mpsc::channel(1024);

// Spawn a producer — could be a Kafka consumer, gRPC stream, etc.
tokio::spawn(async move {
    for i in 0..1000u64 {
        let timestamp = i / 100;  // batch into epochs
        tx.send(InputEvent::Data(timestamp, vec![i])).await.unwrap();
        if i % 100 == 99 {
            tx.send(InputEvent::Frontier(timestamp + 1)).await.unwrap();
        }
    }
    // dropping tx signals end-of-input → library drops all capabilities
});

let input_stream = ReceiverStream::new(rx);

execute(config, move |worker| {
    DataflowSpec::new()
        .input("events", input_stream)
        .build(move |inputs, scope| {
            let events = inputs.get::<u64>("events")?;
            events
                .unary("process", |input, output, notificator| {
                    // process batches as they arrive
                    while let Some((time, data)) = input.next()? {
                        let mut session = output.session(&time);
                        for item in data {
                            session.give(item * 2)?;
                        }
                    }
                    Ok(())
                })
                .inspect(|x| println!("result: {x}"))
                .probe();
            Ok(())
        })
}).await?;
```

### Multi-process with custom connection manager:

```rust
// Application-specific connection manager using an actor framework
struct MyActorConnectionManager { /* ... */ }

#[async_trait]
impl ConnectionManager for MyActorConnectionManager {
    type Connection = TlsStream<TcpStream>;
    
    async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Error> {
        // Ask the remote node's actor to open a port for us
        let port = self.coordinator
            .send(OpenListenerFor { peer: request.local_id })
            .await?;
        
        // Connect and perform TLS handshake
        let tcp = TcpStream::connect((self.resolve_host(request.peer_id), port)).await?;
        let tls = self.tls_connector.connect(tcp).await?;
        Ok(tls)
    }
}

// Heterogeneous cluster: nodes have different worker counts based on capacity.
// Node 0 (this machine, 8 cores) → 4 workers
// Node 1 (small VM, 2 cores) → 1 worker  
// Node 2 (beefy server, 32 cores) → 8 workers
// Total: 13 workers; exchange hashes across all 13.
let cluster = ClusterTopology {
    nodes: vec![
        NodeConfig { node_index: 0, workers: 4 },
        NodeConfig { node_index: 1, workers: 1 },
        NodeConfig { node_index: 2, workers: 8 },
    ],
};

let config = DataflowConfig {
    cluster,
    cancellation_token: token.clone(),
};

let communication = CommunicationConfig::Cluster {
    this_node: 0,  // we are node 0
    connection_manager: Arc::new(MyActorConnectionManager::new(actor_system)),
    pool_config: PoolConfig {
        max_connections_per_peer: 2,
        idle_timeout: Duration::from_secs(120),
        ..Default::default()
    },
};
```

---

## 11. Progress Tracking Details

Progress tracking in instancy mirrors the timely-dataflow protocol but is adapted for async:

### 11.1 Per-Subgraph Progress Task

Each subgraph spawns a dedicated `progress_tracker` async task:

```
Operator tasks ──(progress_tx)──► Progress Tracker Task ──(broadcast)──► Peer trackers
                                         │
                                         ▼
                                  Frontier updates sent back to operators
```

- Operators report `consumed`, `produced`, `internals` changes via a dedicated `mpsc::Sender<ProgressUpdate>`.
- The tracker accumulates changes, runs the reachability algorithm, and broadcasts frontier updates.
- In `ProgressMode::Demand`, updates are buffered until they could change the global frontier.
- Inter-worker progress exchange uses the same multiplexed connections as data channels.

### 11.2 Async Probe

```rust
impl<T: Timestamp> ProbeHandle<T> {
    /// Returns true if the frontier is less than `time`.
    pub fn less_than(&self, time: &T) -> bool;
    
    /// Awaits until the frontier advances past `time`.
    pub async fn async_wait_for(&self, time: T) -> Result<(), Error>;
    
    /// Returns a watch receiver for frontier changes.
    pub fn frontier_watch(&self) -> watch::Receiver<Antichain<T>>;
}
```

---

## 12. Key Design Decisions & Trade-offs

### 12.1 Send + Sync Everywhere

Unlike timely-dataflow which uses `Rc<RefCell<...>>` extensively (single-threaded), instancy requires `Send + 'static` bounds on operator closures and data because tasks can run on any Worker Thread Pool thread. This adds some constraints but enables the shared pool model.

**Mitigation**: Use lock-free structures and channels where possible. The `progress` tracker uses channels to avoid shared mutable state. Operator state is owned by the closure (no shared references needed).

### 12.2 Batching to Amortize Scheduling Overhead

Even with the lightweight Custom Worker Thread Pool, per-task scheduling has non-trivial cost (~100-500ns for enqueue + dequeue + thread wakeup). Operators process data in batches (`Vec<T>` containers) to amortize:
- Task scheduling overhead
- Buffer transfer overhead
- Progress reporting overhead

Default batch size: 1024 items (configurable).

### 12.3 Custom Worker Thread Pool vs Tokio vs Rayon

**Chosen: Custom Worker Thread Pool** optimized for the dataflow workload.

**Tokio rejected** because:
- Operator logic is purely synchronous (no I/O, no await points)
- Tokio's cooperative scheduling (yield points) adds overhead for CPU-bound work
- Tokio's waker/future machinery is unnecessary for "run closure to completion" tasks
- Tokio's work-stealing is designed for I/O-bound workloads

**Rayon rejected** because:
- Rayon is designed for fork-join parallelism, not a persistent task queue
- Rayon doesn't support per-worker FIFO ordering or per-region concurrency limits
- Rayon doesn't support dynamic thread scaling (min/max with idle shutdown)

**Custom pool advantages**:
- Minimal overhead: dequeue → call closure → enqueue results
- Spin/yield/park idle strategy tuned for dataflow burst patterns
- Dynamic scaling between min/max threads
- Per-region concurrency limits built into the scheduler
- Per-worker FIFO guarantee without extra synchronization

**Hybrid approach (future optimization)**: Fuse chains of pipeline-local operators (e.g., `map -> filter -> map`) into a single task to eliminate intermediate buffer overhead.

### 12.4 Connection Multiplexing

Rather than one TCP connection per (worker, channel) pair, instancy multiplexes all channels to the same peer over a small number of pooled connections. The pool delegates all connection establishment to the application's `ConnectionManager`, so the library never touches sockets directly. This dramatically reduces connection count in large clusters and supports arbitrarily complex networking topologies.

### 12.5 Dynamic Cluster Scaling

instancy supports **dynamic cluster scaling** — nodes can be added to or removed from the cluster at runtime. The hosting application is responsible for detecting node changes (health checks, service discovery, autoscaler events, connection failures) and notifying the timely runtime. The library does **not** perform its own node discovery or health monitoring.

#### Responsibilities

| Responsibility | Owner |
|---|---|
| Detect node joins, departures, and failures | **Application** (hosting process) |
| Notify the runtime of topology changes | **Application** → `ClusterMembership` callback |
| Rebuild routing tables and rebalance logical workers | **Library** (runtime) |
| Migrate in-flight data for affected workers | **Library** (runtime) |
| Re-establish connections to new nodes | **Application** (via `ConnectionManager`) |
| Decide whether to retry/abort affected dataflows | **Application** (via error policy) |

#### ClusterMembership Trait

The application implements this trait and passes it to the runtime at startup. The runtime calls `subscribe()` to receive a stream of membership change events.

```rust
/// Events describing changes to the physical cluster topology.
/// The hosting application produces these events; the runtime consumes them.
pub enum MembershipEvent {
    /// A new physical node has joined the cluster and is ready to host logical workers.
    NodeJoined {
        node_index: usize,
        logical_workers: usize,
        peer_id: PeerId,
    },
    /// A physical node has left the cluster (graceful shutdown or detected failure).
    NodeLeft {
        node_index: usize,
        reason: NodeDepartureReason,
    },
}

/// Why a node departed.
pub enum NodeDepartureReason {
    /// Graceful shutdown (node drained its work before leaving).
    Graceful,
    /// Connection lost / health check failed (unexpected departure).
    ConnectionLost,
    /// Application-initiated removal (e.g., scale-down decision).
    Removed,
}

/// Application-implemented trait for providing cluster membership changes.
///
/// The runtime subscribes to membership events at startup. The application
/// is free to use any discovery mechanism: Kubernetes watch, Consul, ZooKeeper,
/// gossip protocol, or manual operator commands.
pub trait ClusterMembership: Send + Sync + 'static {
    /// Returns a stream of membership change events.
    /// The runtime processes these events to update routing tables,
    /// rebalance workers, and handle in-flight data for departing nodes.
    fn subscribe(&self) -> Box<dyn Stream<Item = MembershipEvent> + Send + Unpin>;
}
```

#### Scaling-Up (Node Joins)

When the application notifies the runtime that a new node has joined:

1. **Topology update**: `ClusterTopology` is extended with the new `NodeConfig`.
2. **Worker assignment**: New logical worker indices are allocated for the joining node's workers.
3. **Routing table rebuild**: All `RoutingTable` instances are updated to include the new remote endpoints.
4. **Connection establishment**: The pool requests connections to the new peer via `ConnectionManager`.
5. **Rebalance (optional)**: Running dataflows with `Exchange` or `Rebalance` routing can gradually redirect new data to the expanded worker set. In-flight data for the old topology continues on the original routes until its timestamp frontier advances.

**Important**: Existing in-flight data is NOT migrated. Only new data (at future timestamps) takes advantage of the expanded topology. This ensures progress tracking remains consistent — a timestamp that has already been produced cannot change its routing.

#### Scaling-Down (Node Departures)

When the application reports a node departure:

1. **Mark workers unavailable**: Logical workers on the departed node are marked as unavailable.
2. **Drain in-flight data**: For graceful departures, the runtime waits for the node to drain its pending output (bounded by a configurable timeout). For failures, in-flight data on the lost node is considered lost.
3. **Error propagation**: Depending on the dataflow's error policy:
   - **Stop**: All affected dataflows receive `Error::NodeLost` and terminate.
   - **Continue**: The runtime logs the loss, discards affected timestamps, and advances frontiers past the lost data.
4. **Routing table update**: Routes to the departed node are removed. Future exchange/rebalance targets only include surviving nodes.
5. **Connection cleanup**: The pool evicts all connections to the departed peer.

#### Consistency Guarantees

- **Progress safety**: A departed node's outstanding capabilities are treated as "released" — the frontier advances past any timestamps that only the lost node could produce. This is safe because no more data at those timestamps will arrive.
- **At-most-once by default**: If a node fails mid-computation, records being processed by that node may be lost. Applications requiring exactly-once semantics must use the checkpoint/recovery mechanism.
- **No split-brain**: The application is the single source of truth for membership. The runtime trusts the application's events and does not perform its own consensus.

#### Example: Kubernetes Integration

```rust
struct K8sClusterMembership {
    pod_watcher: kube::runtime::watcher::Watcher<Pod>,
}

impl ClusterMembership for K8sClusterMembership {
    fn subscribe(&self) -> Box<dyn Stream<Item = MembershipEvent> + Send + Unpin> {
        // Convert Kubernetes pod events into MembershipEvent stream
        // Pod Ready → NodeJoined
        // Pod Deleted/Failed → NodeLeft
        Box::new(self.pod_watcher.map(|event| match event {
            WatchEvent::Added(pod) => MembershipEvent::NodeJoined { ... },
            WatchEvent::Deleted(pod) => MembershipEvent::NodeLeft { ... },
            _ => ...
        }))
    }
}
```

### 12.6 Checkpointing

instancy supports **consumer-defined checkpointing** via a `Checkpoint` operator that can be inserted at any point in the dataflow graph. Timestamps provide a natural checkpoint boundary — all data up to a given frontier has been fully processed.

#### Checkpoint Trait

```rust
/// Consumer-implemented trait for persisting and restoring checkpoint state.
#[async_trait]
pub trait CheckpointBackend<T: Timestamp, D: Data>: Send + Sync + 'static {
    /// Persist a batch of data at the given timestamp.
    /// Called by the checkpoint operator when data passes through.
    async fn save(&self, time: &T, data: &[D]) -> Result<(), Error>;

    /// Persist the current frontier (the set of timestamps that have been
    /// fully checkpointed). Called when the frontier advances.
    async fn save_frontier(&self, frontier: &Antichain<T>) -> Result<(), Error>;

    /// Load the most recently saved frontier.
    /// Returns None if no checkpoint exists.
    async fn load_frontier(&self) -> Result<Option<Antichain<T>>, Error>;
}
```

#### Checkpoint Operator

```rust
/// Inserts a checkpoint into the dataflow.
/// All data passing through is persisted via the backend.
fn checkpoint<B>(
    &self,
    backend: B,
) -> DataStream<S, C>
where
    B: CheckpointBackend<S::Timestamp, C::Item>;
```

The checkpoint operator:
1. Passes all data through unchanged (transparent to the dataflow graph).
2. Calls `backend.save(time, data)` for each batch that flows through.
3. When the input frontier advances, calls `backend.save_frontier(frontier)`.

#### Recovery via Fast-Forward

On restart, the dataflow can skip already-checkpointed data by fast-forwarding the input stream:

```rust
/// Wraps an input stream to skip data that has already been checkpointed.
/// Loads the stored frontier from the backend and drops all InputEvents
/// with timestamps that are less_equal to any element in the stored frontier.
pub async fn resume_from_checkpoint<T, D, B>(
    input: impl TimestampedInput<T, D>,
    backend: &B,
) -> Result<impl TimestampedInput<T, D>, Error>
where
    T: Timestamp,
    D: Data,
    B: CheckpointBackend<T, D>,
{
    let frontier = backend.load_frontier().await?;
    // Filter: skip Data events where time is dominated by the stored frontier
    // Pass through all events with timestamps beyond the frontier
    Ok(FilteredInput::new(input, frontier))
}
```

**Design rationale**: Checkpointing is not built into the core runtime — it's an optional operator consumers add where needed. This keeps the core simple while giving consumers full control over what is checkpointed, how it is stored (local disk, S3, database), and how recovery works. The `Timestamp` system naturally provides consistent cut points.

### 12.6 Throughput & Resource Management

A dataflow system's value is directly proportional to its throughput under constrained resources. instancy's architecture has four major throughput domains — data ingestion, computation, network exchange, and output emission — each with distinct bottleneck patterns and tuning levers. This section describes how the system maximizes end-to-end throughput while staying within resource budgets, and how backpressure ties the domains together so no single domain overwhelms the others.

#### 12.6.1 Data Ingestion Throughput

External data sources (Kafka, files, network sockets, actor messages) feed the dataflow through `TimestampedInput` sources, bridged via bounded `ChannelInput` channels.

**Key throughput levers:**

| Lever | Mechanism | Default |
|---|---|---|
| Input parallelism | Multiple named inputs, each independently read | 1 per `add_input()` |
| Batch size | `InputEvent::Data` carries `Vec<D>` — larger batches amortize per-event overhead | Caller-defined |
| Channel buffer depth | `ChannelInput::with_capacity(name, cap)` — deeper buffers absorb bursts | 1024 |
| Reader thread count | One I/O thread per input source (Tokio); sources are independent | 1 per source |

**Throughput model:**

```
ingestion_rate = Σ (batch_size × batches_per_sec) across all inputs
effective_rate = min(ingestion_rate, first_operator_consumption_rate)
```

When the first operator cannot keep up, the `ChannelInput`'s bounded `sync_channel` blocks the I/O reader, which in turn applies backpressure to the external source (e.g., Kafka consumer pauses, TCP recv blocks). This is the first link in the end-to-end backpressure chain.

**Design guidance:**
- Size input batches to amortize per-scheduling overhead (~1024 items is a good starting point). Very small batches (1-10 items) can make scheduling cost dominate.
- Use multiple independent inputs for multi-topic or multi-partition sources — each gets its own I/O thread and does not contend with others.
- Prefer `send_blocking` in the I/O reader to naturally throttle ingestion when the pipeline is saturated.

#### 12.6.2 Computation Throughput & Worker Thread Pool Sizing

The Worker Thread Pool is the central resource. All operator tasks compete for pool threads. The goal: keep all threads busy without over-subscribing CPU cores, while responding to load changes within milliseconds.

**Thread pool dynamics:**

```
                         ┌─────────────────────────────────┐
  Incoming tasks ───────→│     Shared Task Queue            │
                         │  (lock-free injector deque)      │
                         └────────────┬────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
   ┌───────────┐               ┌───────────┐               ┌───────────┐
   │ Thread 0  │               │ Thread 1  │               │ Thread N  │
   │ spinning  │               │ parked    │               │ (spawning)│
   └───────────┘               └───────────┘               └───────────┘
         │                            │                            │
    min_threads ◄─────────── idle_timeout ──────────► max_threads
    (always alive)           (shrink back)            (burst ceiling)
```

**Sizing guidelines:**

| Workload | min_threads | max_threads | Rationale |
|---|---|---|---|
| Steady streaming | CPU cores | CPU cores × 1.5 | Fully utilize cores, small headroom for bursts |
| Bursty/batch | 2 | CPU cores × 2 | Low idle cost, fast scale-up on burst |
| Mixed (dataflow + app) | CPU cores / 2 | CPU cores | Share machine with application threads |
| Testing | 1 | 4 | Minimize contention in test harness |

**Computation throughput formula:**

```
tasks_per_sec = active_threads × (1 / avg_task_duration)
effective_throughput = tasks_per_sec × avg_batch_size_per_task

overhead_per_task ≈ dequeue_cost + dispatch_cost + enqueue_result_cost
                  ≈ 100–500ns (lock-free deque operations)

useful_fraction = avg_task_duration / (avg_task_duration + overhead_per_task)
```

For a 10μs operator processing a 1024-item batch, useful fraction ≈ 99.5%. For a 100ns operator processing 1 item, useful fraction ≈ 50% — batching matters enormously.

**Minimizing scheduling overhead:**

1. **Batch processing**: Operators always receive and produce `Vec<D>` batches. The scheduler enqueues one task per (worker, operator, batch) — not one per record.
2. **Operator fusion (future)**: Chains of pipeline-local operators (e.g., `map → filter → map`) can be fused into a single task, eliminating intermediate buffer writes and task transitions.
3. **Per-worker FIFO**: Tasks for the same logical worker are dispatched in order without extra synchronization — the scheduler's per-worker queue avoids lock contention.
4. **Region permits**: Per-region concurrency limits prevent thread starvation across dataflows sharing the pool.
5. **Time-bounded message batching**: Instead of scheduling an operator activation for every arriving message, the orchestrator accumulates messages in the operator's input buffer and dispatches a single activation once a batching threshold is reached (see below).

#### 12.6.2a Time-Bounded Message Batching

When many small data messages arrive for an operator, scheduling one activation per message creates excessive task overhead — the scheduling cost can dominate the actual compute. **Time-bounded batching** solves this by letting the orchestrator coalesce messages before dispatching.

**How it works:**

```
Messages arriving for Op B:
  msg1 ─┐
  msg2 ─┤
  msg3 ─┼──→ [Input Buffer] ──(batch threshold met)──→ Schedule activation
  msg4 ─┤                                                (processes all buffered msgs)
  msg5 ─┘
```

The orchestrator holds messages in an operator's input buffer until one of three conditions triggers a dispatch:

| Threshold | Description | Default |
|---|---|---|
| `max_batch_count` | Maximum number of messages before dispatch | 1024 |
| `max_batch_bytes` | Maximum total byte size before dispatch (requires `MessageSize` impl) | 64 KB |
| `max_batch_wait` | Maximum time since first buffered message before dispatch | 1 ms |

Whichever threshold is reached first triggers the activation. This gives bounded latency (via `max_batch_wait`) while maximizing throughput (via `max_batch_count` / `max_batch_bytes`).

**Configuration:**

Batching is configured per-dataflow execution, applying uniformly to all operators in that dataflow:

```rust
/// Batching policy for operator input message coalescing.
#[derive(Debug, Clone)]
pub struct BatchingPolicy {
    /// Maximum number of data messages before triggering activation.
    /// Set to 1 to disable batching (activate on every message).
    pub max_batch_count: usize,
    /// Maximum total byte size of buffered messages before triggering activation.
    /// Only enforced for data types that implement `MessageSize`.
    /// `None` means no byte-size limit (count and time thresholds still apply).
    pub max_batch_bytes: Option<usize>,
    /// Maximum duration to wait for more messages before triggering activation.
    /// Bounds worst-case latency. A message never waits longer than this.
    pub max_batch_wait: Duration,
}

impl Default for BatchingPolicy {
    fn default() -> Self {
        Self {
            max_batch_count: 1024,
            max_batch_bytes: Some(64 * 1024), // 64 KB
            max_batch_wait: Duration::from_millis(1),
        }
    }
}
```

**Message size measurement:**

For byte-size-based batching to work, the system needs to know the size of each message. This is provided via an optional trait:

```rust
/// Optional trait for measuring the in-memory byte size of a data message.
///
/// Implement this for data types where byte-size-based batching is desired.
/// If not implemented, only count-based and time-based thresholds are used.
pub trait MessageSize {
    /// Returns the approximate in-memory byte size of this message.
    /// Does not need to be exact — used for batching heuristics, not memory accounting.
    fn message_size(&self) -> usize;
}

// Blanket impls for common types
impl MessageSize for String {
    fn message_size(&self) -> usize { self.len() }
}

impl<T: Sized> MessageSize for Vec<T> {
    fn message_size(&self) -> usize { self.len() * std::mem::size_of::<T>() }
}
```

When `D: MessageSize`, the orchestrator tracks cumulative byte size and triggers dispatch when `max_batch_bytes` is reached. When `D` does not implement `MessageSize`, the byte-size threshold is ignored and only count/time thresholds apply.

**Batching timer lifecycle:**

```
Message arrives for Op X (buffer was empty)
  → Start batch timer (max_batch_wait countdown)
  → Check count/size thresholds

More messages arrive
  → Accumulate in buffer
  → Check count/size thresholds after each arrival

Threshold reached (count, size, OR timer fires)
  → Cancel timer (if still running)
  → Schedule operator activation
  → Operator processes all buffered messages in one activate() call
  → Buffer is empty; timer is idle until next message
```

**Interaction with backpressure:**

Batching and backpressure are complementary:
- Backpressure limits how much data flows *between* operators (bounded buffers).
- Batching limits how *often* operators are activated (coalescing messages into fewer activations).
- When an operator is backpressured (output buffer full), its input buffer continues accumulating — effectively getting "free" batching from the stall.

**Throughput impact:**

```
Without batching (activate per message):
  overhead_fraction = scheduling_cost / (scheduling_cost + per_msg_compute)
  For 100ns compute + 500ns scheduling → 83% overhead!

With batching (1024 messages per activation):
  overhead_fraction = scheduling_cost / (scheduling_cost + 1024 × per_msg_compute)
  For 100ns compute + 500ns scheduling → 0.5% overhead
```

**Design rationale:**
- **Per-dataflow configuration**: Different dataflows have different latency requirements. A real-time alerting pipeline might set `max_batch_wait: 100μs` and `max_batch_count: 16`, while a batch ETL pipeline might set `max_batch_wait: 10ms` and `max_batch_count: 65536`.
- **Optional size trait**: Not all data types have meaningful "size." Making it optional via a trait avoids imposing unnecessary bounds on simple types.
- **Bounded latency**: The `max_batch_wait` timer guarantees that even at low throughput, messages are processed within a bounded time. Without it, a nearly-idle operator could wait indefinitely for a full batch.
- **Composable with existing batching**: The `Vec<D>` data batches from input sources are independent of operator-level batching. Input sources produce batches of their own (e.g., 1000 Kafka messages); operator batching coalesces *those batches* further at the scheduling level.

**Thread lifecycle and CPU conservation:**

```
  Active (processing tasks)
      │
      ▼ no tasks for N spins
  Yielding (thread::yield_now)
      │
      ▼ no tasks for M yields
  Parked (condvar wait — zero CPU)
      │
      ▼ idle_timeout exceeded & thread_count > min_threads
  Shutdown (thread exits)
```

The spin→yield→park→shutdown progression ensures:
- Sub-microsecond response to new tasks during active processing (spinning)
- Rapid backoff when load drops (yielding within ~1μs, parking within ~100μs)
- Zero CPU consumption when idle (condvar-parked threads consume no cycles)
- Automatic scaling down to `min_threads` during quiet periods

#### 12.6.3 Network Exchange: Connection & Bandwidth Management

When a dataflow spans multiple nodes, inter-process data exchange becomes the bottleneck. The system manages throughput across three layers:

```
┌─────────────────────────────────────────────────────────┐
│  Operator Layer                                          │
│  push() / pull() — sees bounded buffers only             │
├─────────────────────────────────────────────────────────┤
│  Connection Pool Layer                                   │
│  Manages connections per peer, scales up/down             │
│  Multiplexes logical channels onto physical connections   │
├─────────────────────────────────────────────────────────┤
│  Transport Layer (application-provided)                  │
│  ConnectionManager::establish() creates the wire          │
│  Handles TLS, routing, firewall traversal                │
└─────────────────────────────────────────────────────────┘
```

**Connection pool throughput management:**

| Parameter | Effect on throughput | Trade-off |
|---|---|---|
| `max_connections_per_peer` | More connections = higher aggregate bandwidth (multiple TCP streams avoid head-of-line blocking) | More file descriptors, more memory for send/recv buffers |
| `min_connections_per_peer` | Pre-warmed connections avoid cold-start latency | Idle resource consumption |
| `idle_timeout` | Controls how quickly excess connections are reclaimed | Too aggressive = reconnection cost on next burst |
| `connect_timeout` | Bounds worst-case latency for pool growth | Too short = failed connections under network jitter |

**Bandwidth management strategy:**

1. **Multiplexed channels**: All logical (worker, channel) pairs to the same peer share pooled connections via a framing protocol. This avoids O(workers²) connection explosion.
2. **Bounded send buffers**: Each connection has a bounded write buffer. When the buffer is full, the sending operator sees `Error::Backpressure` — this is the remote backpressure trigger (see §12.6.4).
3. **Adaptive connection scaling**: The pool monitors per-connection throughput. When all connections to a peer are saturated (send buffers consistently >80% full) and the count is below `max_connections_per_peer`, the pool requests a new connection from `ConnectionManager::establish()`.
4. **TCP flow control integration**: The OS TCP stack provides an additional backpressure layer. When the remote receiver is slow, TCP's receive window shrinks, which slows the local sender, which fills the send buffer, which triggers operator-level backpressure. No application-level acknowledgment protocol is needed.
5. **Serialization cost amortization**: The `Codec` encodes entire `Vec<D>` batches at once (not individual records), amortizing the serialization overhead across the batch.

**Throughput estimation for network exchange:**

```
per_connection_throughput ≈ min(
    link_bandwidth,
    1 / (serialization_time_per_batch + network_rtt_amortized)
)

aggregate_peer_throughput = num_connections × per_connection_throughput

bottleneck = min(
    sender_computation_rate,
    aggregate_peer_throughput,
    receiver_computation_rate
)
```

#### 12.6.4 End-to-End Backpressure-Aware Design

Backpressure is not a bolt-on feature — it is the primary mechanism that ties all throughput domains together and prevents resource exhaustion. Every buffer boundary in the system is bounded and participates in the backpressure chain.

**Complete backpressure path:**

```
External Source
    │
    ▼ (ChannelInput, bounded sync_channel)
  Input Reader ──── blocks when channel full ──── I/O rate throttled
    │
    ▼ (operator input buffer, bounded)
  Operator A ──── push returns Backpressure ──── activation yields, re-queued
    │
    ▼ (operator input buffer, bounded)
  Operator B ──── push returns Backpressure ──── activation yields, re-queued
    │
    ▼ (network send buffer, bounded)         ┌────────────────────────────┐
  TCP Send ──── buffer full ─────────────────│ Remote Node                │
    │              │                          │  Operator C (slow)         │
    │         TCP flow control                │  ← processing backlog     │
    │         (window shrinks)                └────────────────────────────┘
    │
    ▼ (OutputSender, bounded sync_channel)
  Output Stream ──── try_send returns Backpressure ──── operator slows down
    │
    ▼
  Consumer (reads at its own pace)
```

**Backpressure design principles:**

1. **Every buffer is bounded**: No unbounded queues anywhere in the data path. This provides a hard memory ceiling and ensures backpressure always propagates.
2. **Backpressure is synchronous**: When an operator hits a full downstream buffer, its task yields immediately (no polling, no async wait). The scheduler re-queues the task, freeing the thread for other work.
3. **No data loss on backpressure**: `Error::Backpressure` means "try again later" — the data remains in the sending operator's buffer. The re-queued activation retries the push on its next execution.
4. **Backpressure is measurable**: Every operator tracks `BackpressureMetrics` (blocked count, total blocked duration, max single block). This makes bottleneck identification straightforward.
5. **Backpressure crosses process boundaries**: TCP flow control provides implicit network-level backpressure. The system does not require application-level ack/nack for flow control.

**Tuning for throughput vs. latency:**

| Goal | Buffer sizes | Pool size | Trade-off |
|---|---|---|---|
| Maximum throughput | Large (4096+) | max_threads = cores | Higher memory usage, higher tail latency |
| Low latency | Small (64–256) | max_threads = cores × 1.5 | Lower throughput ceiling, faster response |
| Balanced | Medium (1024) | max_threads = cores | Good default for most workloads |

**Buffer sizing rule of thumb:**

```
optimal_buffer_size ≈ producer_rate × target_absorb_time
```

Where `target_absorb_time` is how many milliseconds of burst you want to absorb before backpressure kicks in. For a producer at 100K items/sec with 10ms burst target: buffer = 1000 items.

#### 12.6.5 Resource Budget Model

The overall system resource consumption can be modeled as:

```
CPU:
  pool_threads × duty_cycle + io_threads × io_duty_cycle
  where duty_cycle = useful_compute / (useful_compute + idle + scheduling_overhead)

Memory:
  Σ (buffer_capacity × avg_item_size) across all buffers
  + thread_stacks × (pool_threads + io_threads)
  + connection_buffers × total_connections
  (thread stack default: 2MB; connection buffer default: 64KB send + 64KB recv)

Network:
  Σ (data_rate × serialization_expansion) per peer connection
  + progress_messages × progress_frequency
  (progress messages are small — typically <1KB — but sent frequently)

File descriptors:
  pool_connections × num_peers + io_sockets + internal_channels
```

**Monitoring these budgets:**

- `DataflowMetrics.total_cpu_time` → CPU utilization of the dataflow
- `OperatorMetrics.cpu_time` → per-operator CPU breakdown
- `BackpressureMetrics.blocked_duration` → time lost to backpressure (indicates capacity mismatch)
- Connection pool stats (future) → connection count, utilization, error rate
- Worker pool stats → active threads, queued tasks, idle time

**Anti-patterns to avoid:**

1. **Unbounded producer with small buffer**: A fast external source pushing into a small-buffer `ChannelInput` will spend most of its time blocked. Either increase buffer size or add flow control at the source.
2. **Under-parallelized bottleneck stage**: If one execution region has high `cpu_time` and high upstream `backpressure.blocked_duration`, increase that region's parallelism.
3. **Over-parallelized idle stage**: If an execution region has many workers but low `cpu_time`, reduce parallelism to free pool threads for bottleneck regions.
4. **Too many connections**: More connections per peer doesn't always help — contention on the serialization path can negate the benefit. Profile before adding connections.
5. **Tiny batches across network**: Sending 1-item batches over the network pays full framing + serialization overhead per item. Batch at the source or add a buffering operator.

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

## 14. Implementation Phases

**Phase 1 — Foundation**
- Error types, `PartialOrder`, `Timestamp`, `PathSummary`
- `Antichain`, `ChangeBatch`, `MutableAntichain`
- `Capability` and capability management
- Basic `Scope` trait and `Worker` structure

**Phase 2 — Intra-Process Dataflow**
- `mpsc`-based intra-process channels with `Envelope` message type
- `TimestampedInput`, `InputEvent`, `DataflowSpec`
- `from_stream` operator (binds async streams as inputs)
- `OutputHandle`, `ProbeHandle`
- Operators: `unary`, `binary`, `inspect`, `probe`, `concat`, `delay`
- Progress tracking (single-process)
- `execute()` bootstrap with dynamic worker pool

**Phase 3 — Loops & Branching**
- `feedback` / `loop_variable` / `connect_loop`
- `enter` / `leave` for nested scopes
- `branch` / `ok_err`
- Error handling policy (`ErrorPolicy::Stop` / `ErrorPolicy::Ignore`)

**Phase 4 — Networking**
- `ConnectionManager` trait + `TcpConnectionManager` default
- `ConnectionPool` with dynamic scaling (min/max connections)
- Wire protocol (framing + multiplexing)
- `exchange` operator across processes
- Inter-process progress tracking

**Phase 5 — Observability, Checkpointing & Polish**
- Per-dataflow CPU time tracking (`DataflowMetrics`, `OperatorMetrics`)
- `Checkpoint` operator + `CheckpointBackend` trait + fast-forward recovery
- Cancellation integration
- Comprehensive error handling review
- Tracing/logging integration
- Documentation + examples
- Benchmarks

---

## 15. Open Questions

1. **Operator fusion**: Should we automatically fuse pipeline-local operator chains into single tasks? This is a significant optimization but adds complexity. **Recommendation**: defer to a future optimization pass.

2. **Backpressure propagation across processes**: When an inter-process channel is full, how does backpressure flow back? TCP flow control provides some, but we may need application-level credit-based flow control. **Recommendation**: start with TCP-level backpressure + bounded send buffers; add credit-based flow control if needed.

3. **Codec at type level vs channel level**: Should `ExchangeData` carry its codec, or should the codec be specified when creating exchange channels? **Recommendation**: channel-level for maximum flexibility, with a convenience trait for types with a "default" codec.

4. **Container abstraction**: timely-dataflow recently added generic container support beyond `Vec<T>`. Should we support this from the start? **Recommendation**: start with `Vec<T>` as the only container type; add the generic `Container` trait later.
