# Cluster & Distributed Execution

This document collects the distributed-runtime design: cluster startup, distributed cancellation, membership changes, coordinator integration, multi-cluster isolation, and the testing model for true cross-process execution.

Back to the overview: [docs/DESIGN.md](./DESIGN.md)

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

#### 5.5.1 Distributed Cancellation

In a cluster dataflow, cancellation must propagate across all peer nodes so that
the entire distributed computation winds down consistently. The control channel
(channel ID 0) carries a `Cancel` message alongside the existing `Handshake` and
`Ready` messages.

**Wire format** — `Cancel` is message type `2`:

```
[type=2][reason_len: u32 LE][reason: UTF-8 bytes][crc32: 4 bytes]
```

The `reason` field is a human-readable string (e.g., `"user requested shutdown"`,
`"operator panicked"`).

**Components** — Two tasks are spawned per cluster dataflow during Phase 8.5 of
`spawn_cluster`:

| Task | Trigger | Action |
|------|---------|--------|
| **Cancel broadcaster** | Local `dataflow_cancel` token fires | Sends `Cancel` to every peer's control channel, then exits |
| **Cancel listener** | Receives `Cancel` on any peer control channel | Fires local `dataflow_cancel` with `CancellationReason::PeerCancelled { peer_id, detail }` |

Both tasks also exit when the `bridge_cancel` token fires (normal completion),
preventing resource leaks on the happy path.

**First-cancel-wins semantics** — `CancellationToken::cancel_with_reason()` is
idempotent: once the token is cancelled, subsequent calls are no-ops. This
prevents infinite echo loops:

```
Node A cancels → broadcasts Cancel to Node B
Node B receives → cancels local token → broadcasts Cancel to Node A
Node A receives → cancel_with_reason() is a no-op (already cancelled) → no broadcast
```

Each node broadcasts at most once per dataflow.

**Peer-down integration** — When the hosting application reports a node departure
via `report_node_leave()`, the `ClusterCancelHandle` cancels the shared
`dataflow_cancel` token (not just individual worker tokens). This ensures the
broadcaster fires, notifying healthy peers before bridge teardown.

**Security assumption** — `Cancel` messages carry no cryptographic signature.
Peer identity verification is the responsibility of the transport layer (e.g.,
mTLS configured in the application's `ConnectionManager`). All peers in a
cluster session are assumed to be authenticated at connection time.

#### 5.5.2 Cluster Startup Protocol

`spawn_cluster()` orchestrates a multi-phase startup sequence that synchronizes
all participating nodes before any dataflow execution begins. If any phase
fails or times out, `spawn_cluster()` returns an error — no operators are
started.

**Phases:**

```
 Node A                                    Node B
   │                                         │
   ├─ Phase 1: Build local dataflows         ├─ Phase 1: Build local dataflows
   ├─ Phase 2: Validate topologies           ├─ Phase 2: Validate topologies
   ├─ Phase 3: Compute fingerprint           ├─ Phase 3: Compute fingerprint
   │                                         │
   ├─ Phase 4: HANDSHAKE ──────────────────► │
   │ ◄──────────────────── HANDSHAKE ────────┤
   │  (verify fingerprints match)            │  (verify fingerprints match)
   │                                         │
   ├─ Phase 5: Wire exchange channels        ├─ Phase 5: Wire exchange channels
   ├─ Phase 6: Create progress channels      ├─ Phase 6: Create progress channels
   │                                         │
   ├─ Phase 6.5: READY BARRIER ────────────► │
   │ ◄──────────────────── READY ────────────┤
   │  (all channels wired, safe to execute)  │  (all channels wired, safe to execute)
   │                                         │
   ├─ Phase 7: Materialize workers           ├─ Phase 7: Materialize workers
   ├─ Phase 8: Register & start execution    ├─ Phase 8: Register & start execution
   │                                         │
   ▼  Dataflow running                       ▼  Dataflow running
```

1. **Handshake** (Phase 4) — Each node computes a fingerprint from its dataflow
   graph (operator count, edge count, exchange indices, feedback count, worker
   count) and exchanges it with all peers. If any peer's fingerprint differs,
   `spawn_cluster` fails with `HandshakeError::FingerprintMismatch`. This catches
   bugs where nodes build different dataflow topologies.

2. **Ready Barrier** (Phase 6.5) — After all exchange channels and progress
   channels are wired, each node sends `Ready` and waits for all peers to
   respond. This prevents a fast node from starting execution before a slow
   node has finished channel wiring, which would cause lost messages.

Both phases use the same `handshake_timeout` parameter. Each waits at most
`handshake_timeout` for peer responses; if any peer doesn't respond in time,
`spawn_cluster` returns `Err(HandshakeError::Timeout { peer_id })`.

**What happens when one node doesn't call `spawn_cluster`:**

If Node A calls `spawn_cluster` but Node B never does, Node A will block in
the Handshake phase waiting for B's response. After `handshake_timeout`
expires, `spawn_cluster` returns an error. No dataflow is materialized, no
operators are started, and no resources are leaked.

```rust
// Node A: will fail after 5 seconds if Node B never calls spawn_cluster
let result = rt.spawn_cluster(
    "my-dataflow",
    topology,         // includes Node B
    "node-a",
    dataflow_id,
    transport,
    Duration::from_secs(5),  // handshake_timeout
    build_fn,
    &tokio_handle,
    SpawnOptions::new(),
);
// result = Err(HandshakeError::Timeout { peer_id: "node-b" })
```

**Control channel** — Handshake, Ready, and Cancel messages share a dedicated
control channel (channel ID 0) that is separate from data and progress
channels. Control messages receive biased priority in the transport layer's
shared FIFO queue.

### 12.5 Dynamic Cluster Scaling

> **Status: Implemented.** The `ClusterMembership` trait and `ChannelMembership` convenience type are available in the `execute` module. Membership is attached to `ClusterTopology` and passed via `RuntimeConfig` at construction time. The runtime auto-starts a background listener that processes membership events to update the live topology, cancel affected dataflows, and make new nodes available. Mid-dataflow repartitioning (worker rebalancing) is not supported — existing dataflows keep their original topology.

instancy supports **dynamic cluster scaling** — nodes can be added to or removed from the cluster at runtime. The hosting application is responsible for detecting node changes (health checks, service discovery, autoscaler events, connection failures) and notifying the timely runtime. The library does **not** perform its own node discovery or health monitoring.

#### Responsibilities

| Responsibility | Owner |
|---|---|
| Detect node joins, departures, and failures | **Application** (hosting process) |
| Notify the runtime of topology changes | **Application** → `ClusterMembership` provider attached to `ClusterTopology` |
| Update live topology and cancel affected dataflows | **Library** (runtime) |
| Re-establish connections to new nodes | **Application** (via `ConnectionManager`) |
| Decide whether to retry/abort affected dataflows | **Application** (via error policy) |

#### ClusterMembership Trait

The application implements this trait and attaches it to `ClusterTopology` via `with_membership()`. When the topology is passed through `RuntimeConfig`, the runtime automatically takes the membership provider and starts a background event listener.

```rust
/// Events describing changes to the physical cluster topology.
/// The hosting application produces these events; the runtime consumes them.
pub enum MembershipEvent {
    /// A new physical node has joined the cluster and is ready to host logical workers.
    NodeJoined {
        node_id: String,
        logical_workers: usize,
    },
    /// A physical node has left the cluster (graceful shutdown or detected failure).
    NodeLeft {
        node_id: String,
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
/// The application attaches a membership provider to `ClusterTopology` via
/// `with_membership()`. The runtime takes ownership of the provider during
/// construction and calls `events()` to receive membership change events.
pub trait ClusterMembership: Send + Sync + 'static {
    /// Takes the membership event receiver.
    /// Returns `Some(receiver)` on the first call; subsequent calls return `None`
    /// (the runtime takes ownership of the receiver).
    fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>>;
}
```

#### Topology–Membership Integration

`ClusterTopology` owns an optional membership provider. This design keeps topology and membership tightly coupled since both describe the physical cluster layout:

```rust
let membership = ChannelMembership::new();
let tx = membership.sender();

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-0", 4),
    NodeConfig::new("node-1", 4),
])?.with_membership(membership);

let rt = RuntimeHandle::new(RuntimeConfig {
    topology: Some(topology),
    ..Default::default()
})?;

// Runtime auto-starts membership listener — send events via tx
tx.send(MembershipEvent::NodeJoined { node_id: "node-2".into(), logical_workers: 4 })?;
```

#### Scaling-Up (Node Joins)

When the runtime receives a `MembershipEvent::NodeJoined` event:

1. **Clear "down" state**: If the node was previously marked as down, the peer registry clears it. This allows future `spawn_cluster` calls to include the node.
2. **Connection on demand**: New connections to the node are established lazily — the connection pool calls `ConnectionManager` the first time a dataflow needs to communicate with the node.
3. **Topology update**: The live `ClusterTopology` is updated with the new node. Already-running dataflows are **not** affected.
4. **Worker assignment**: New logical worker indices are allocated for the joining node's workers in subsequent `spawn_cluster` calls.

**Important**: Existing in-flight data is NOT migrated. Only new dataflows (or re-spawned dataflows) take advantage of the expanded topology. This ensures progress tracking remains consistent — a timestamp that has already been produced cannot change its routing.

#### Scaling-Down (Node Departures)

When the runtime receives a `MembershipEvent::NodeLeft` event:

1. **Cancel affected dataflows**: All dataflows with workers on the departed node are cancelled via the peer registry. instancy does **not** attempt to reschedule work to surviving nodes — the hosting application owns retry logic.
2. **Topology update**: The node is removed from the live `ClusterTopology`. If it was the last node, the topology is cleared entirely.
3. **Application retry**: The hosting application can resubmit the dataflow targeting only healthy nodes, or send a `NodeJoined` event once the node recovers.

#### Consistency Guarantees

- **Progress safety**: A departed node's outstanding capabilities are treated as "released" — the frontier advances past any timestamps that only the lost node could produce. This is safe because no more data at those timestamps will arrive.
- **At-most-once by default**: If a node fails mid-computation, records being processed by that node may be lost. Applications requiring exactly-once semantics must use the checkpoint/recovery mechanism.
- **No split-brain**: The application is the single source of truth for cluster membership. The runtime trusts the membership events and does not perform its own consensus or health probing.
- **Zero-worker events ignored**: `NodeJoined` events with `logical_workers == 0` are silently dropped.
- **Idempotent**: Duplicate join events for an existing node or leave events for an unknown node are handled gracefully.

#### Example: Kubernetes Integration

```rust
struct K8sClusterMembership {
    rx: Mutex<Option<UnboundedReceiver<MembershipEvent>>>,
    tx: UnboundedSender<MembershipEvent>,
}

impl K8sClusterMembership {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self { rx: Mutex::new(Some(rx)), tx }
    }

    /// Spawn a background task that watches Kubernetes pod events
    /// and converts them into MembershipEvents via self.tx.
    fn start_watching(&self, client: kube::Client) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            // Watch pod events and send MembershipEvent::NodeJoined/NodeLeft
        });
    }
}

impl ClusterMembership for K8sClusterMembership {
    fn events(&self) -> Option<UnboundedReceiver<MembershipEvent>> {
        self.rx.lock().unwrap().take()
    }
}
```

## 12.6 Coordinator Integration Model

## 12.6 Coordinator Integration Model

### Execution Model: SPMD with Host-Managed Coordination

instancy uses the **SPMD (Single Program, Multiple Data)** execution model: the hosting application launches the same dataflow program on each participating node, and each node's local executor runs the local partition of the graph. instancy does NOT dispatch work from a central coordinator to worker nodes.

However, real-world services always have a **coordinator** — a component in the hosting application that receives user requests, starts distributed dataflow execution, monitors progress, and returns results to callers. **The coordinator is implemented by the hosting application, not by instancy.** instancy provides primitives (traits, structs, helpers) to make this coordinator integration straightforward, but the coordination logic — how to start dataflows on remote nodes, how to route user requests, how to aggregate results — is entirely the host application's responsibility. This is by design: different applications have fundamentally different coordination needs (actor frameworks, gRPC services, message queues, custom RPC).

### DataflowHandle — The Driver Abstraction

When a node submits a dataflow for execution, it receives a `DataflowHandle` — a lightweight handle that represents the running dataflow and provides:

```rust
/// Handle returned when a dataflow is submitted to the local executor.
/// One instance per dataflow per node.
/// Cardinality: 1 per (dataflow, node). Lifetime: dataflow execution.
pub struct DataflowHandle {
    /// Unique identifier for this dataflow across all nodes.
    dataflow_id: DataflowId,
    /// Receiver for the final outcome from the local executor.
    outcome_rx: oneshot::Receiver<DataflowOutcome>,
    /// Cancellation token — calling cancel() requests graceful shutdown.
    cancel: CancellationToken,
    /// Progress subscription — streams frontier updates to the holder.
    progress_rx: mpsc::Receiver<ProgressUpdate>,
}

impl DataflowHandle {
    /// Wait for the dataflow to complete, returning the final outcome.
    pub async fn result(self) -> DataflowOutcome { ... }

    /// Request graceful cancellation. Operators finish their current batch
    /// then drain. Returns immediately; use `result()` to wait for completion.
    pub fn cancel(&self) { self.cancel.cancel(); }

    /// Subscribe to progress updates (frontier advances).
    /// The coordinator uses this to track how far the dataflow has progressed.
    pub fn progress_stream(&mut self) -> &mut mpsc::Receiver<ProgressUpdate> {
        &mut self.progress_rx
    }

    /// Query current frontier — the set of timestamps that may still arrive.
    /// If the frontier is empty, the dataflow is complete for all timestamps.
    pub fn current_frontier(&self) -> Antichain<T> { ... }
}
```

### DataflowOutcome — Rich Completion Status

```rust
/// The final result of a dataflow execution on a single node.
/// The coordinator aggregates outcomes from all nodes.
pub enum DataflowOutcome {
    /// All operators completed successfully. All timestamps processed.
    Completed {
        /// The final frontier (empty = all timestamps done).
        final_frontier: Antichain<T>,
        /// Execution metrics (total CPU time, operator stats).
        metrics: DataflowMetrics,
    },

    /// Dataflow was cancelled by the coordinator or user.
    Cancelled {
        /// Frontier at the time of cancellation — timestamps up to (but not
        /// including) this frontier have been fully processed and committed
        /// to sinks. Timestamps at or beyond this frontier may be partial.
        progress_frontier: Antichain<T>,
        /// Metrics up to the point of cancellation.
        metrics: DataflowMetrics,
    },

    /// A fatal error occurred in one or more operators.
    Failed {
        /// The error that caused the failure.
        error: Error,
        /// Frontier at the time of failure — same semantics as Cancelled.
        progress_frontier: Antichain<T>,
        /// Which operator(s) failed.
        failed_operators: Vec<OperatorInfo>,
        /// Metrics up to the point of failure.
        metrics: DataflowMetrics,
    },

    /// Quiescent — no operator can make progress but not all are done.
    /// This happens when the dataflow is waiting for external input
    /// (e.g., a live stream that hasn't produced new data).
    Quiescent {
        /// Current frontier — timestamps beyond this may still arrive.
        progress_frontier: Antichain<T>,
        metrics: DataflowMetrics,
    },
}
```

### ProgressUpdate — Real-Time Progress Reporting

The coordinator needs to know how far the dataflow has progressed, both for user-facing progress reporting and for determining which timestamps are "safe" (fully committed to sinks).

```rust
/// A progress update emitted by the executor when the frontier advances.
/// Cardinality: streamed periodically to the DataflowHandle holder.
pub struct ProgressUpdate {
    /// The dataflow this update is for.
    pub dataflow_id: DataflowId,
    /// The new frontier after this advance.
    pub frontier: Antichain<T>,
    /// How many records have been processed since last update.
    pub records_processed: u64,
    /// Wall-clock time of this update.
    pub timestamp: Instant,
}
```

### Cross-Node Outcome Aggregation

In a distributed dataflow, the coordinator (in the host application) must aggregate outcomes from all nodes. instancy provides a helper for this:

```rust
/// Aggregates DataflowOutcomes from multiple nodes into a single result.
/// The coordinator collects outcomes from all nodes and feeds them here.
/// Cardinality: 1 per dataflow on the coordinator node. Lifetime: dataflow execution.
pub struct OutcomeAggregator {
    expected_nodes: Vec<String>,
    received: HashMap<String, DataflowOutcome>,
}

impl OutcomeAggregator {
    pub fn new(participating_nodes: Vec<String>) -> Self { ... }

    /// Record a node's outcome. Returns the aggregated result when all nodes
    /// have reported, or None if still waiting.
    pub fn record(&mut self, node_id: &str, outcome: DataflowOutcome)
        -> Option<AggregatedOutcome> { ... }
}

/// The aggregated outcome across all nodes.
pub enum AggregatedOutcome {
    /// All nodes completed successfully.
    Completed { metrics: AggregatedMetrics },
    /// One or more nodes failed. The entire dataflow is considered failed.
    /// Includes which nodes failed and which succeeded.
    Failed {
        error: Error,
        failed_nodes: Vec<(String, Error)>,
        /// The global progress frontier (min across all nodes' frontiers).
        global_progress: Antichain<T>,
    },
    /// Cancelled on all nodes.
    Cancelled { global_progress: Antichain<T> },
}
```

### Coordinator ↔ instancy Interaction Pattern

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Coordinator (in host application)                      │
│                                                                         │
│  1. Receives user request                                               │
│  2. Builds dataflow definition                                          │
│  3. Sends "start dataflow" command to all participating nodes           │
│  4. Collects DataflowHandle on the local node                          │
│  5. Monitors progress via progress_stream()                            │
│  6. On cancellation request: calls handle.cancel() + sends cancel       │
│     command to remote nodes                                             │
│  7. Collects outcomes from all nodes via OutcomeAggregator             │
│  8. Returns final result to the user                                   │
└─────────┬───────────────────────────────┬─────────────────────┬─────────┘
          │ Start + Monitor               │ Start + Monitor     │ Start + Monitor
          ▼                               ▼                     ▼
┌─────────────────┐            ┌─────────────────┐   ┌─────────────────┐
│   Node A        │            │   Node B        │   │   Node C        │
│                 │◄──────────►│                 │◄──►│                 │
│ DataflowHandle  │  exchange  │ DataflowHandle  │   │ DataflowHandle  │
│ Executor sweep  │  channels  │ Executor sweep  │   │ Executor sweep  │
└─────────────────┘            └─────────────────┘   └─────────────────┘
```

### Key Design Points

1. **The coordinator is the host application's responsibility, not instancy's** — instancy is a dataflow execution library, not a distributed job scheduler. The host application implements all coordination logic: receiving user requests, launching dataflows on remote nodes, collecting results, and returning them to callers. This separation keeps instancy focused and avoids imposing a specific coordination pattern on diverse applications.

2. **Worker placement is decided by the coordinator (host app)** — a dataflow does not necessarily run on all cluster nodes. The coordinator selects which nodes participate and how many logical workers each node contributes. It builds a per-dataflow `ClusterTopology` containing only the selected nodes and their worker assignments, then passes this topology to each node when starting the dataflow. instancy does not make placement decisions — it executes the assignment it receives. instancy may provide a `PlacementStrategy` trait and validation helpers (e.g., verify total workers match stage parallelism requirements), but actual placement logic depends on factors only the host app knows: node load, data locality, cost constraints, hardware capabilities, etc.

3. **Data locality is application knowledge, not instancy's** — expanding on #2 above: instancy is a data-locality-agnostic execution engine. It provides mechanisms — per-worker named input ports, configurable cluster topology, exchange operators — but never assumes where data resides or how partitions map to workers. The hosting application has full knowledge of data placement and configures the dataflow accordingly. For example, if a dataset has 8 partitions across 2 machines (4 per machine), the host app starts 2 nodes with 4 logical workers each, and feeds each worker its local partition by binding a separate `TimestampedInput` stream per worker (e.g., `.input("data_0", partition_0_stream)` through `.input("data_3", partition_3_stream)` on each node). First-stage operators process data locally (no exchange needed), reducing data volume before cross-node `exchange()` in later stages. This data-local-first pattern minimizes network traffic and maximizes throughput, but it is entirely the host app's decision — instancy simply executes the worker-to-partition mapping it receives. Worker IDs are global and assigned by the topology (via node ordering in `ClusterTopology`); the host should compute each node's worker-id range from the topology rather than assuming `0..N` are local.

    ```
    Host app decides placement based on data locality:

    Machine A (data partitions 0-3):       Machine B (data partitions 4-7):
    ┌────────────────────────────┐         ┌────────────────────────────┐
    │ Partition 0 → Worker 0     │         │ Partition 4 → Worker 4     │
    │ Partition 1 → Worker 1     │         │ Partition 5 → Worker 5     │
    │ Partition 2 → Worker 2     │         │ Partition 6 → Worker 6     │
    │ Partition 3 → Worker 3     │         │ Partition 7 → Worker 7     │
    │                            │         │                            │
    │ First stage: local         │         │ First stage: local         │
    │ processing (no shuffle)    │         │ processing (no shuffle)    │
    │                            │         │                            │
    │ Later stages:              │◄───────►│ Later stages:              │
    │ exchange() across nodes    │         │ exchange() across nodes    │
    │ (all-to-all by hash key)   │         │ (all-to-all by hash key)   │
    └────────────────────────────┘         └────────────────────────────┘

    instancy provides: per-worker input ports, exchange(), ConnectionPool
    Host app provides: partition-to-worker mapping, connection factory, node topology
    ```

4. **instancy provides the building blocks** — `DataflowHandle`, `ProgressUpdate`, `OutcomeAggregator`, and `CancellationToken` provide everything the coordinator needs to manage distributed execution.

5. **Progress frontier is the source of truth for "how far we got"** — when a dataflow is interrupted (cancelled or failed), the `progress_frontier` tells the coordinator which timestamps have been fully processed. This enables:
   - Resume from last committed timestamp
   - Report partial progress to the user
   - Exactly-once semantics when combined with checkpointing

6. **Cancellation is cooperative and distributed** — the coordinator cancels locally via `DataflowHandle::cancel()`, then sends a cancel command to remote nodes via the host app's communication layer. Each node's executor checks its local `CancellationToken` and drains gracefully.

7. **Error reporting flows back to the coordinator** — when an operator fails, the local executor captures the error in `DataflowOutcome::Failed` with the progress frontier. The host app collects these outcomes and the `OutcomeAggregator` determines the global result.

8. **The coordinator node is designated by the host app, not by instancy** — any node can be the coordinator. Typically it's the node that received the user request. instancy doesn't need to know which node is the coordinator.

---

## 12.7 Multi-Cluster Isolation (No Global State)

**Constraint:** The instancy crate must contain **zero static/global variables**. All state is owned by explicit runtime instances.

### Motivation

A hosting application may need to run multiple **isolated instancy clusters** within the same process. For example:

- **Interactive cluster** — low-latency queries with small worker pool, high priority
- **Batch cluster** — bulk ETL with large worker pool, best-effort priority
- **Test cluster** — in-process integration tests that must not interfere with production clusters

Each cluster must have fully independent:
- Task queue
- Worker thread pool
- Connection pool
- Progress tracking state
- Cancellation scope

### Design

```rust
/// A self-contained instancy runtime. Multiple RuntimeHandle instances
/// can coexist in the same process with full isolation.
pub struct RuntimeHandle {
    worker_pool: WorkerPool,
    task_queue: TaskQueue,
    connection_pool: ConnectionPool,
    config: RuntimeConfig,
}

impl RuntimeHandle {
    /// Create a new isolated runtime with the given configuration.
    pub fn new(config: RuntimeConfig) -> Self { ... }

    /// Submit a dataflow for execution within this runtime.
    pub fn execute(&self, spec: DataflowSpec) -> DataflowHandle { ... }
}
```

### Rules

1. **No `static`, `lazy_static`, `once_cell`, or thread-local state** in library code.
2. All shared state flows from a `RuntimeHandle` root — never from global singletons.
3. Metrics/tracing use the caller's subscriber (passed in config), not a global one.
4. Tests create their own `RuntimeHandle` instances — no test-ordering dependencies.

### Interaction Diagram

```
Process
├── RuntimeHandle "interactive"
│   ├── WorkerPool (4 threads)
│   ├── TaskQueue (priority-aware)
│   ├── ConnectionPool (to nodes A, B)
│   └── Dataflows: [query_1, query_2]
│
├── RuntimeHandle "batch"
│   ├── WorkerPool (16 threads)
│   ├── TaskQueue (FIFO)
│   ├── ConnectionPool (to nodes A, B, C, D)
│   └── Dataflows: [etl_pipeline]
│
└── RuntimeHandle "test"
    ├── WorkerPool (2 threads)
    ├── TaskQueue (FIFO)
    ├── InMemoryTransport (no real connections)
    └── Dataflows: [test_dataflow]
```


## Cross-process integration testing model

The former `CROSS_PROCESS_INTEGRATION.md` design has been merged here and updated to match the current required-connection-factory model.

### Problem and goals

instancy's in-process cluster tests validate protocol behavior, but they do not by themselves prove correctness across independent OS processes with separate address spaces, Tokio runtimes, and kernel-managed TCP state. Cross-process integration tests therefore need to:

1. Start multiple node processes, each hosting its own `RuntimeHandle`.
2. Coordinate cluster startup from a test-side coordinator.
3. Feed data, collect outputs, and assert correctness across nodes.
4. Clean up reliably on success, failure, or timeout.

### Architecture

- **Coordinator process**: starts child node processes, sends control commands, gathers progress/results, and tears everything down.
- **Node process**: hosts a long-lived runtime plus a control-plane agent that can bind listeners, establish peer connections through the current connection-factory path, spawn a cluster dataflow, feed inputs, collect outputs, and cancel/shutdown.
- **Two independent communication layers**:
  - **Control plane**: coordinator ↔ node process, for orchestration messages.
  - **Data plane**: node ↔ node, for instancy exchange/progress/control traffic carried by the runtime transport.

### Control protocol

The coordinator protocol remains length-prefixed JSON with request correlation:

- `BindListener` / `ListenerReady`
- `ConnectPeers` / `PeersConnected`
- `SpawnDataflow` / `DataflowSpawned`
- `FeedData`, `CloseInputs`, `CollectOutput`, `CancelDataflow`, and `Shutdown`
- unsolicited `DataflowCompleted` events

### Coordinator and node responsibilities

- The coordinator decides which nodes participate and in what topology.
- Each node owns its local `DataflowHandle`, input senders, and output receivers.
- Cluster spawn is still synchronized by the runtime handshake and ready barrier; the coordinator's orchestration ensures every node reaches `spawn_cluster()` with the same topology and `DataflowId`.
- Connection establishment in tests must use the same current application-facing connection factory path rather than bypassing the pool with ad-hoc pre-established sockets.

### Recommended test scenarios

- basic two-node pass-through
- cross-process exchange routing
- distributed word count over multiple timestamps
- iterative/feedback computation across processes
- three-node topologies
- multiple workers per node
- distributed cancellation
- parallel dataflows sharing the same runtime
- node crash / peer disconnect failure paths

### Notes carried forward from the original design

- the coordinator should own child-process lifecycle and timeout handling
- process cleanup must happen in `Drop`/finally paths
- predefined dataflow builders are useful because they isolate transport correctness from arbitrary user pipelines
- coordinator-side assertions should use both final output and completion/progress events
