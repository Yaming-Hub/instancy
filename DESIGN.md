# async-timely: Design Document

## 1. Overview

**async-timely** is an asynchronous, Tokio-based reimplementation of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) — a low-latency cyclic dataflow computational model. It retains the core concepts of timely dataflow (timestamps, frontiers, progress tracking, capabilities, scopes) while making fundamental changes to the execution model, networking, serialization, and error handling.

### Design Principles

1. **Async-native execution** — operators run as async tasks on a shared Tokio runtime, enabling multiple dataflows to share worker threads and improving resource utilization.
2. **Timely semantics preserved** — timestamps, partial ordering, progress tracking, frontiers, capabilities, and nested scopes all work the same way conceptually.
3. **Production-grade robustness** — `Result`-based error handling everywhere; no panics in library code. First-class cancellation via `CancellationToken`.
4. **Pluggable networking** — users supply their own connection factory (e.g., mTLS); the library manages a pooled, reusable connection layer.
5. **Pluggable serialization** — a `Codec` trait lets users choose bincode, protobuf, flatbuffers, or any other format.
6. **Minimal core operators** — only `unary`, `binary`, `branch`, `feedback` (loop), `exchange`, `input`, `probe`, `inspect`, `concat`. Higher-level operators live in extension crates.

---

## 2. Architecture Comparison: timely-dataflow vs async-timely

| Aspect | timely-dataflow | async-timely |
|---|---|---|
| Execution | 1 OS thread per worker; worker owns its dataflows and steps through them synchronously | Shared Tokio runtime; logical `WorkerId`s provide FIFO ordering and parallelism control; no dedicated threads |
| Worker topology | All nodes must have the same number of workers | Heterogeneous: each node declares its own worker count based on capacity; global worker set is the union |
| Scheduling | `Worker::step()` loop polls activations | Per-worker FIFO queues + per-dataflow concurrency semaphore on shared Tokio thread pool |
| Communication (intra-process) | `Rc<RefCell<VecDeque>>` with direct push/pull | `tokio::sync::mpsc` channels (bounded, with backpressure) |
| Communication (inter-process) | Dedicated TCP per worker pair, pre-configured hostfile | Application-provided `ConnectionManager` establishes connections; library pools and reuses them |
| Serialization | `Abomonation` / `bincode` hardcoded | Pluggable `Codec` trait |
| Error handling | `panic!` / `unwrap` in many places | `Result<T, Error>` throughout; `thiserror` for error types |
| Cancellation | Drop the worker | `CancellationToken` propagated to all operators |
| Operator API | Closure-based `unary` / `binary` with manual `Notificator` | `async fn` closures with `InputHandle` / `OutputHandle`; inputs are external async streams with automatic capability management |

---

## 3. Crate Structure

```
async-timely/
├── Cargo.toml              (workspace root)
├── async-timely/            (main crate)
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
│   │   │   ├── stream.rs    — Stream<S, C>
│   │   │   ├── channels/    — Push/Pull abstractions
│   │   │   └── operators/
│   │   │       ├── mod.rs
│   │   │       ├── capability.rs
│   │   │       ├── from_stream.rs — Binds external async streams as inputs
│   │   │       ├── unary.rs
│   │   │       ├── binary.rs
│   │   │       ├── branch.rs
│   │   │       ├── feedback.rs   — LoopVariable, ConnectLoop
│   │   │       ├── exchange.rs
│   │   │       ├── concat.rs
│   │   │       ├── inspect.rs
│   │   │       └── probe.rs
│   │   ├── worker.rs        — Worker handle (async)
│   │   ├── execute.rs       — Runtime bootstrap
│   │   └── communication/
│   │       ├── mod.rs
│   │       ├── allocator.rs — Channel allocator
│   │       ├── codec.rs     — Codec trait
│   │       ├── connection.rs— ConnectionFactory + pool
│   │       └── transport.rs — Framed async read/write
│   └── Cargo.toml
├── async-timely-communication/  (optional: split networking crate)
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

## 5. Async Execution Model

### 5.1 Shared Runtime & Logical Workers

There are **no dedicated worker threads**. All dataflows share a single Tokio multi-threaded runtime. The physical OS threads in the Tokio thread pool are interchangeable and run tasks from any dataflow.

However, we retain the concept of a **logical worker ID** — a `WorkerId` — which serves three purposes:

1. **FIFO ordering**: All operator tasks assigned to the same `WorkerId` execute their work in FIFO sequence. If operator A and operator B both belong to worker 0, and A produces output before B, that ordering is preserved. This replaces the physical guarantee that timely-dataflow got from running on a single thread.

2. **Parallelism control**: A dataflow declares how many logical workers it uses (e.g., 2). The runtime ensures that **at most N operator tasks are actively computing at any moment** for a dataflow with N workers. This prevents a single dataflow from monopolizing the shared thread pool.

3. **Data partitioning**: The `exchange` operator routes data by `hash(item) % total_workers` across all workers in the cluster. The `WorkerId` determines which partition an operator instance belongs to.

#### Heterogeneous Worker Assignment

Unlike timely-dataflow where every process must run the same number of workers, async-timely allows **each node to declare its own worker count** based on its available resources (CPU cores, memory, etc.). The global worker set is the union of all per-node workers, and each worker is assigned a globally unique `WorkerId`.

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

This means:
- **Stronger nodes get more partitions**, naturally handling more data.
- **The `exchange` operator** hashes across all 12 workers globally; data flows to the correct node based on which workers it hosts.
- **Adding/removing capacity** is done by adjusting worker counts per node rather than requiring symmetric resizing of the entire cluster.

```rust
/// A globally unique logical worker identity.
/// Not tied to any physical OS thread or specific node.
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

/// Configuration for the shared async runtime.
pub struct RuntimeConfig {
    /// Optional: provide an existing Tokio runtime handle.
    /// If None, async-timely creates its own runtime.
    pub runtime: Option<tokio::runtime::Handle>,
    /// Progress tracking mode.
    pub progress_mode: ProgressMode,
}
```

> **Runtime isolation**: The caller application can create a **dedicated Tokio runtime** for async-timely and pass its `Handle` via `RuntimeConfig::runtime`. This isolates dataflow computation tasks from the application's own async work (HTTP servers, database queries, etc.), preventing them from competing for the same thread pool. This is the **recommended deployment pattern** for production workloads where predictable latency matters.
>
> ```rust
> // Application creates a dedicated runtime for async-timely
> let compute_runtime = tokio::runtime::Builder::new_multi_thread()
>     .worker_threads(8)
>     .thread_name("async-timely")
>     .build()?;
>
> let config = RuntimeConfig {
>     runtime: Some(compute_runtime.handle().clone()),
>     progress_mode: ProgressMode::Eager,
> };
>
> // The application's own runtime is unaffected
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

**How logical workers are enforced:**

Each `WorkerId` is backed by a **worker executor** — a FIFO task queue with a concurrency permit. Internally this is a `tokio::sync::Semaphore` (one permit per logical worker on this node) combined with per-worker ordered channels:

```
┌──────────────────────────────────────────────────────────────────┐
│                     Shared Tokio Runtime                         │
│                   (N OS threads, work-stealing)                  │
│                                                                  │
│  Dataflow A (node has 2 of 5 global workers)                    │
│  ┌─────────────┐ ┌─────────────┐                                │
│  │ Worker 0    │ │ Worker 1    │     (Workers 2-4 on other nodes)│
│  │ ┌─────────┐ │ │ ┌─────────┐ │                                │
│  │ │Op: map  │ │ │ │Op: map  │ │                                │
│  │ │Op: filter│ │ │ │Op: filter│ │                                │
│  │ │  (FIFO) │ │ │ │  (FIFO) │ │                                │
│  │ └─────────┘ │ │ └─────────┘ │                                │
│  └──────┬──────┘ └──────┬──────┘                                │
│         │               │                                        │
│    Semaphore(2) — at most 2 local workers compute simultaneously │
│                                                                  │
│  Dataflow B (node has 3 of 8 global workers) — runs concurrently│
│  ┌──────┐ ┌──────┐ ┌──────┐                                    │
│  │ W0   │ │ W1   │ │ W2   │     (Workers 3-7 on other nodes)   │
│  └──────┘ └──────┘ └──────┘                                    │
│                                                                  │
│  ┌──────────────────────────────────────┐                        │
│  │     Progress Tracker Task            │                        │
│  └──────────────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────┘
```

**Worker executor internals:**

```rust
/// Manages task execution for one logical worker within a dataflow.
/// Ensures FIFO ordering: tasks queued to this worker run sequentially.
struct WorkerExecutor {
    worker_id: WorkerId,
    /// Ordered queue of ready operator activations.
    task_queue: mpsc::Receiver<OperatorActivation>,
    /// Semaphore shared across all WorkerExecutors on this node for this dataflow.
    /// Limits concurrent computation to the node's local worker count.
    concurrency_permit: Arc<Semaphore>,
}

impl WorkerExecutor {
    async fn run(&mut self) -> Result<(), Error> {
        while let Some(activation) = self.task_queue.recv().await {
            // Acquire a concurrency permit — blocks if all local workers are busy.
            let _permit = self.concurrency_permit.acquire().await?;
            // Execute the operator's work. Since we process the queue serially,
            // all work for this WorkerId is FIFO-ordered.
            activation.execute().await?;
            // Permit is released on drop, allowing another worker to proceed.
        }
        Ok(())
    }
}
```

### 5.2 Operator Scheduling

Each operator is **not** a free-floating Tokio task. Instead, operators are scheduled through their owning `WorkerExecutor`. When an operator has input data ready, it posts an activation to its worker's task queue. The worker executor processes these activations in FIFO order, gated by the dataflow's concurrency semaphore.

This gives us:
- **FIFO within a worker**: operators on the same worker are activated in order.
- **Bounded parallelism per node**: a node with `workers: 2` never has more than 2 operator activations running simultaneously for that dataflow, regardless of how many operators exist.
- **Heterogeneous scaling**: a powerful node with `workers: 6` naturally processes 3x more partitions than a node with `workers: 2`.
- **Fair sharing**: the Tokio runtime fairly schedules worker executors from different dataflows, preventing starvation.

```
Input stream ──► from_stream task ──► Worker 0 queue ──► op activations (FIFO)
                                                              │
                 exchange ──────────► Worker 1 queue ──► op activations (FIFO)
                                                              │
                              concurrency semaphore(N) gates both workers
```

**Operator activation flow:**

1. Data arrives on an operator's input channel.
2. The channel notifies the operator's `WorkerId`'s task queue: "operator X has work".
3. The worker executor dequeues the activation (FIFO order preserved).
4. The executor acquires a concurrency permit from the dataflow's semaphore.
5. The operator's logic runs (processes input, produces output, reports progress).
6. The permit is released; the next queued activation proceeds.

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

### 5.4 Bootstrap: execute & DataflowSpec

```rust
/// Per-dataflow specification: input streams + graph definition.
pub struct DataflowSpec<T: Timestamp, R> {
    config: DataflowConfig,
    inputs: Vec<(String, Box<dyn ErasedTimestampedInput<T>>)>,
    builder: Box<dyn FnOnce(DataflowInputs<T>, &mut Scope<T>) -> Result<R, Error> + Send>,
}

impl<T: Timestamp, R> DataflowSpec<T, R> {
    pub fn new(config: DataflowConfig) -> Self { /* ... */ }

    /// Attach a named input stream.
    pub fn input<D: Data>(
        mut self,
        name: &str,
        stream: impl TimestampedInput<T, D>,
    ) -> Self { /* ... */ }

    /// Define the dataflow graph.
    pub fn build<F>(mut self, func: F) -> Self
    where
        F: FnOnce(DataflowInputs<T>, &mut Scope<T>) -> Result<R, Error> + Send + 'static,
    { /* ... */ }
}

/// Run one or more dataflows on the shared runtime.
pub async fn execute<R: Send + 'static>(
    runtime_config: RuntimeConfig,
    specs: Vec<DataflowSpec<_, R>>,
) -> Result<Vec<R>, Error> {
    // For each spec, create logical workers (WorkerExecutors)
    // backed by the dataflow's concurrency semaphore.
    // All of them run on the shared Tokio thread pool.
    // Multiple dataflows can be submitted and run concurrently.
    ...
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

### 5.6 Load Control

Since multiple dataflows share the same thread pool, we need controls to prevent one dataflow from starving others:

- **Per-dataflow concurrency semaphore**: the primary mechanism. Each node's local worker count for a dataflow caps its active computations. A dataflow with 2 local workers and 50 operators still only runs 2 operator activations at a time.
- **Bounded channels** create natural backpressure — a fast producer awaits when the channel is full.
- **Yield points**: long-running operator closures should periodically `tokio::task::yield_now().await` (enforced by convention; documented).
- **Cross-dataflow fairness**: since each dataflow's workers are independent Tokio tasks, Tokio's work-stealing scheduler provides natural fairness. A bursty dataflow cannot preempt other dataflows' worker executors.
- **Dynamic adjustment** (future work): worker counts per node could be adjusted at runtime to respond to load changes, though this requires re-partitioning and is deferred.

---

## 6. Communication Layer

### 6.1 Intra-Process Channels

For workers within the same process, data is exchanged via `tokio::sync::mpsc` bounded channels carrying `Container` batches. No serialization — data moves as owned Rust values.

```rust
/// Intra-process channel pair.
pub struct LocalChannel<C> {
    tx: mpsc::Sender<Message<C>>,
    rx: mpsc::Receiver<Message<C>>,
}

pub struct Message<C> {
    pub time: /* timestamp */,
    pub data: C,  // e.g., Vec<T>
}
```

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

The `ConnectionPool` is the library-internal component that manages established connections. It calls `ConnectionManager::establish()` when it needs a new connection.

**Pool lifecycle:**
1. **First use**: When data needs to be sent to a peer, the pool calls `manager.establish(request)`.
2. **Reuse**: After a multiplexed channel finishes using a connection, it's returned to the pool.
3. **Health check**: The pool periodically pings idle connections; dead ones are dropped.
4. **Reconnect**: On connection failure, the pool calls `manager.establish()` again.
5. **Idle cleanup**: Connections idle beyond the timeout are closed.

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
    /// If no idle connection is available and the pool is not at capacity,
    /// calls `manager.establish()` to create a new one.
    pub async fn acquire(&self, peer_id: &PeerId) -> Result<PoolGuard<M::Connection>, Error> {
        // Try to take an idle connection from the pool
        if let Some(conn) = self.try_take_idle(peer_id) {
            return Ok(conn);
        }
        
        // Ask the application to establish a new connection
        let request = ConnectionRequest {
            peer_id: peer_id.clone(),
            local_id: self.local_id.clone(),
            request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
        };
        let connection = self.manager.establish(request).await?;
        Ok(self.wrap(peer_id.clone(), connection))
    }
    
    /// Return a connection to the pool for reuse.
    /// Called automatically when `PoolGuard` is dropped.
    fn release(&self, peer_id: PeerId, conn: M::Connection) { ... }
}

pub struct PoolConfig {
    /// Max connections per peer (default: 2).
    pub max_connections_per_peer: usize,
    /// Idle timeout before closing a connection (default: 60s).
    pub idle_timeout: Duration,
    /// Health check interval (default: 30s).
    pub health_check_interval: Duration,
    /// Max time to wait for `establish()` to complete (default: 30s).
    pub connect_timeout: Duration,
}
```

**Key design point**: The pool only calls `ConnectionManager::establish()` — it never opens sockets, binds ports, or does any networking itself. The application has full control over the network layer.

### 6.4 Wire Protocol

Each connection carries multiplexed channels using a simple framing protocol:

```
┌───────────┬───────────┬──────────────────┐
│ channel_id│ length    │ payload (codec)  │
│ (u64)     │ (u32)     │ (variable)       │
└───────────┴───────────┴──────────────────┘
```

A background demux task reads frames from a connection and dispatches them to the appropriate channel's `mpsc::Sender`.

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
) -> Stream<S, Vec<D>>
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
| `exchange` | Repartitions data across workers by a routing function |
| `broadcast` | Sends each record to **all** workers across the cluster (clones data cross-process via serialization) |
| `broadcast_local` | Sends each record to all workers **within the same process** (cheap clone, no serialization) |
| `concat` | Merges multiple streams into one |
| `inspect` | Side-effect observation (logging, debugging) |
| `probe` | Observe frontier progress; async `changed()` method |

### 9.2 Unary Operator API

```rust
pub trait Operator<S: Scope, C: Container> {
    /// Creates a unary operator with one input and one output.
    fn unary<C2, L>(
        &self,
        name: &str,
        logic: L,
    ) -> Stream<S, C2>
    where
        C2: Container,
        L: FnMut(
            &mut InputHandle<S::Timestamp, C>,
            &mut OutputHandle<S::Timestamp, C2>,
            &Notificator<S::Timestamp>,
        ) -> Result<(), Error> + Send + 'static;
    
    /// Async variant — the closure is an async fn.
    fn unary_async<C2, L, Fut>(
        &self,
        name: &str,
        logic: L,
    ) -> Stream<S, C2>
    where
        C2: Container,
        L: FnMut(OperatorContext<S::Timestamp, C, C2>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Error>> + Send;
}
```

### 9.3 Binary Operator API

```rust
fn binary<C2, C3, L>(
    &self,
    other: &Stream<S, C2>,
    name: &str,
    logic: L,
) -> Stream<S, C3>
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
) -> (Stream<S, C>, Stream<S, C>);

fn ok_err<O, E>(
    &self,
    logic: impl Fn(T) -> Result<O, E> + Send + Sync + 'static,
) -> (Stream<S, Vec<O>>, Stream<S, Vec<E>>);
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

### 9.6 Extension Point

Extension crates add operators by implementing traits on `Stream`:

```rust
// In crate `async-timely-extras`
pub trait MapOperator<S: Scope, T: Data> {
    fn map<U: Data>(
        &self,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> Stream<S, Vec<U>>;
}

impl<S: Scope, T: Data> MapOperator<S, T> for Stream<S, Vec<T>> {
    fn map<U: Data>(&self, f: impl Fn(T) -> U + Send + Sync + 'static) -> Stream<S, Vec<U>> {
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
use async_timely::prelude::*;
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

Progress tracking in async-timely mirrors the timely-dataflow protocol but is adapted for async:

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

Unlike timely-dataflow which uses `Rc<RefCell<...>>` extensively (single-threaded), async-timely requires `Arc<Mutex<...>>` or channel-based designs because Tokio tasks can run on any thread. This adds some overhead but is necessary for the async model.

**Mitigation**: Use lock-free structures and channels where possible. The `progress` tracker uses `mpsc` channels to avoid shared mutable state.

### 12.2 Batching to Amortize Async Overhead

Individual `await` points have non-trivial cost (~50-100ns). Operators process data in batches (`Vec<T>` containers) to amortize:
- Channel send/recv overhead
- Task wakeup overhead
- Progress reporting overhead

Default batch size: 1024 items (configurable).

### 12.3 Operator-as-Task vs Operator-in-Loop

**Chosen: Operator-as-Task.** Each operator is a separate Tokio task. This gives maximum parallelism and natural integration with Tokio's work-stealing scheduler.

**Alternative considered**: A single task per worker that loops through operators (closer to timely-dataflow's model). Rejected because it would underutilize the async runtime and prevent cross-dataflow work sharing.

**Hybrid approach (future optimization)**: Fuse chains of pipeline-local operators (e.g., `map -> filter -> map`) into a single task to eliminate intermediate channel overhead.

### 12.4 Connection Multiplexing

Rather than one TCP connection per (worker, channel) pair, async-timely multiplexes all channels to the same peer over a small number of pooled connections. The pool delegates all connection establishment to the application's `ConnectionManager`, so the library never touches sockets directly. This dramatically reduces connection count in large clusters and supports arbitrarily complex networking topologies.

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
- `mpsc`-based intra-process channels
- `TimestampedInput`, `InputEvent`, `DataflowSpec`
- `from_stream` operator (binds async streams as inputs)
- `OutputHandle`, `ProbeHandle`
- Operators: `unary`, `binary`, `inspect`, `probe`, `concat`
- Progress tracking (single-process)
- `execute()` bootstrap

**Phase 3 — Loops & Branching**
- `feedback` / `loop_variable` / `connect_loop`
- `enter` / `leave` for nested scopes
- `branch` / `ok_err`

**Phase 4 — Networking**
- `ConnectionManager` trait + `TcpConnectionManager` default
- `ConnectionPool`
- Wire protocol (framing + multiplexing)
- `exchange` operator across processes
- Inter-process progress tracking

**Phase 5 — Polish**
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
