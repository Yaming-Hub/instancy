# Design Decisions & Trade-offs

This document collects the major architectural decisions behind instancy, the trade-offs accepted by the design, and the conventions that shape the public API and implementation model.

Back to the overview: [docs/DESIGN.md](./DESIGN.md)

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
- Rayon doesn't support per-worker FIFO ordering or per-stage concurrency limits
- Rayon doesn't support dynamic thread scaling (min/max with idle shutdown)

**Custom pool advantages**:
- Minimal overhead: dequeue → call closure → enqueue results
- Spin/yield/park idle strategy tuned for dataflow burst patterns
- Dynamic scaling between min/max threads
- Per-stage concurrency limits built into the scheduler
- Per-worker FIFO guarantee without extra synchronization

**Hybrid approach (future optimization)**: Fuse chains of pipeline-local operators (e.g., `map -> filter -> map`) into a single task to eliminate intermediate buffer overhead.

### 12.4 Connection Multiplexing

Rather than one connection per (worker, channel) pair, instancy multiplexes all channels to the same peer over a small number of pooled connections. The pool delegates all connection establishment to the application's `ConnectionFactory`, so the library never touches sockets directly. The library is **transport-agnostic** — any reliable, ordered byte stream works (TCP, TLS, Unix sockets, named pipes, QUIC, etc.). This dramatically reduces connection count in large clusters and supports arbitrarily complex networking topologies.

Both transport modes — dedicated (exclusive lease per dataflow) and shared (multiplexed across dataflows) — use the same factory and pool. The factory is **required**, not optional: it is the sole mechanism for creating, replacing, and scaling connections. instancy provides a default `TcpConnectionFactory` for plain TCP; applications supply their own for other transports or custom protocols.

### 12.5 Dynamic Cluster Scaling

See [Dynamic Cluster Scaling](./cluster.md#125-dynamic-cluster-scaling) in the cluster documentation for the full design including the `ClusterMembership` trait, topology integration, scaling-up/down behavior, consistency guarantees, and a Kubernetes integration example.

**Key design decision**: instancy does not perform node discovery or health monitoring; the hosting application owns membership detection and notifies the runtime via `ClusterMembership`. This separation keeps the library transport-agnostic and avoids baking in assumptions about deployment environments.

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
) -> StreamEdge<S, C>
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

### 12.7 Throughput & Resource Management

A dataflow system's value is directly proportional to its throughput under constrained resources. instancy's architecture has four major throughput domains — data ingestion, computation, network exchange, and output emission — each with distinct bottleneck patterns and tuning levers. This section describes how the system maximizes end-to-end throughput while staying within resource budgets, and how backpressure ties the domains together so no single domain overwhelms the others.

#### 12.7.1 Data Ingestion Throughput

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

#### 12.7.2 Computation Throughput & Worker Thread Pool Sizing

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
4. **Stage permits**: Per-stage concurrency limits prevent thread starvation across dataflows sharing the pool.
5. **Time-bounded message batching**: Instead of scheduling an operator activation for every arriving message, the orchestrator accumulates messages in the operator's input buffer and dispatches a single activation once a batching threshold is reached (see below).

#### 12.7.2a Time-Bounded Message Batching

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

#### 12.7.3 Network Exchange: Connection & Bandwidth Management

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
2. **Bounded send buffers**: Each connection has a bounded write buffer. When the buffer is full, the sending operator sees `Error::Backpressure` — this is the remote backpressure trigger (see §12.7.4).
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

#### 12.7.4 End-to-End Backpressure-Aware Design

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

#### 12.7.5 Resource Budget Model

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
2. **Under-parallelized bottleneck stage**: If one stage has high `cpu_time` and high upstream `backpressure.blocked_duration`, increase that stage's parallelism.
3. **Over-parallelized idle stage**: If a stage has many workers but low `cpu_time`, reduce parallelism to free pool threads for bottleneck stages.
4. **Too many connections**: More connections per peer doesn't always help — contention on the serialization path can negate the benefit. Profile before adding connections.
5. **Tiny batches across network**: Sending 1-item batches over the network pays full framing + serialization overhead per item. Batch at the source or add a buffering operator.

---

## 12.8 Coordinator Integration Model

See [Coordinator Integration Model](./cluster.md#126-coordinator-integration-model) in the cluster documentation for the full design covering `DataflowHandle`, `DataflowOutcome`, `ProgressUpdate`, cross-node outcome aggregation, and the coordinator-to-runtime interaction pattern.

**Key design decision**: instancy remains an execution library, not a distributed job scheduler. The hosting application owns coordinator behavior: placement, remote startup, cancellation fan-out, progress monitoring, and result aggregation, while instancy provides the primitives needed to implement that coordination.

---

## 12.9 Multi-Cluster Isolation (No Global State)

See [§12.7](./cluster.md#127-multi-cluster-isolation-no-global-state) in the cluster documentation for the full isolation model, including `RuntimeHandle` ownership, no-global-state rules, and the multi-cluster interaction diagram.

**Key design decision**: all runtime state is rooted in explicit `RuntimeHandle` instances rather than globals or thread-locals. This allows multiple independent clusters to coexist safely within the same process.

---

## 12.10 Configurable Task Scheduling Policy

The task queue within a `RuntimeHandle` supports **pluggable scheduling policies** that determine the order in which operator activation tasks are dequeued by worker threads.

### Motivation

Different workloads have different latency/throughput requirements:
- Interactive queries need low latency → higher priority
- Background ETL can tolerate delays → lower priority
- Within the same priority, fairness prevents starvation

### Task Metadata

Every task carries scheduling metadata:

```rust
/// Metadata attached to each queued operator activation task.
pub struct TaskMeta {
    /// The dataflow this task belongs to.
    pub dataflow_id: DataflowId,
    /// Priority inherited from the dataflow (higher = scheduled sooner).
    pub priority: u32,
    /// Wall-clock time when this task was enqueued.
    pub created_at: Instant,
}
```

### Scheduling Policy Trait

```rust
/// Determines task ordering in the queue.
///
/// The scheduler compares two tasks and returns which should run first.
/// Implementations can use priority, age, or any combination.
pub trait SchedulePolicy: Send + Sync {
    /// Returns Ordering::Less if `a` should be scheduled before `b`.
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> std::cmp::Ordering;
}
```

### Built-in Policies

| Policy | Description |
|--------|-------------|
| `FifoPolicy` | Pure FIFO — tasks run in creation order regardless of priority. Simple and fair. |
| `PriorityPolicy` | Strict priority — higher priority always wins. Risk of starvation for low-priority tasks. |
| `PriorityWithAgingPolicy` | **(Default)** Priority-first, but tasks gain effective priority as they age. Prevents starvation. |

### PriorityWithAgingPolicy Details

```rust
pub struct PriorityWithAgingPolicy {
    /// How much effective priority a task gains per second of waiting.
    /// Default: 1 priority level per 10 seconds.
    pub aging_rate: f64,
}

impl SchedulePolicy for PriorityWithAgingPolicy {
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering {
        let now = Instant::now();
        let age_a = now.duration_since(a.created_at).as_secs_f64();
        let age_b = now.duration_since(b.created_at).as_secs_f64();

        let effective_a = a.priority as f64 + age_a * self.aging_rate;
        let effective_b = b.priority as f64 + age_b * self.aging_rate;

        // Higher effective priority → scheduled first
        effective_b.partial_cmp(&effective_a).unwrap_or(Ordering::Equal)
    }
}
```

### Configuration

```rust
pub struct RuntimeConfig {
    pub worker_pool: WorkerPoolConfig,
    pub connection_pool: ConnectionPoolConfig,
    /// Scheduling policy for the task queue. Default: PriorityWithAgingPolicy.
    pub schedule_policy: Box<dyn SchedulePolicy>,
    // ...
}
```

### Dataflow Priority Assignment

Priority is set when submitting a dataflow:

```rust
let handle = runtime.execute(DataflowSpec {
    graph: my_graph,
    priority: 100,  // higher = more important
    // ...
});
```

All operator activation tasks generated by this dataflow inherit its priority.

### Key Design Points

1. **Priority is per-dataflow, not per-operator** — simplifies reasoning; all tasks within a dataflow share the same priority level.
2. **Aging prevents starvation** — even priority-0 tasks will eventually run as their effective priority grows with wait time.
3. **Policy is per-RuntimeHandle** — different clusters can use different policies (e.g., interactive uses strict priority, batch uses FIFO).
4. **No global queue** — each `RuntimeHandle` has its own task queue with its own policy, reinforcing the isolation guarantee from [§12.7](./cluster.md#127-multi-cluster-isolation-no-global-state).
5. **`created_at` uses `Instant`** — monotonic clock, immune to wall-clock adjustments.

---


## API design notes

### Naming consistency

## Design: Unify API Naming Inconsistencies

**Item:** `api-naming`
**Priority:** P1
**Status:** Design

### Problem

The instancy public API has several naming inconsistencies that make the API harder to learn and use.

### Changes

#### 1. `RuntimeHandle::shutdown()` — keep infallible

After review, `shutdown()` stays `()`. The underlying `cancel_with_reason()` is idempotent
and can never fail. Returning `Result<()>` with always-`Ok(())` would be misleading.
This matches `shutdown_async()` which also returns `()`.

#### 2. `SpawnOptions` — consolidate to builder-only pattern

Currently `SpawnOptions` has both public fields AND builder methods. This is confusing.

**Change:** Make all fields private, keep builder methods as the only way to configure.
Add `pub fn build(self) -> Self` as a no-op terminal if needed, but the real fix is just making fields private and ensuring all fields have builder setters.

#### 3. `ClusterSpawnedDataflow` — add missing async take methods

`SpawnedDataflow` and `MultiSpawnedDataflow` have `take_async_input`/`take_async_output`, but `ClusterSpawnedDataflow` is missing them.

**Change:** Add `take_async_input` and `take_async_output` to `ClusterSpawnedDataflow` that delegate to the inner `MultiSpawnedDataflow`.

#### 4. Minor: Document `num_local_workers()` vs `num_workers()`

`ClusterSpawnedDataflow` uses `num_local_workers()` while `MultiSpawnedDataflow` uses `num_workers()`. This is actually intentional — cluster has both local and total worker counts. No rename needed, but ensure doc comments make this clear.

### Non-changes (intentionally kept as-is)

- **`take_input` vs `take_async_input`**: The naming is actually correct — `take_input` returns a sync `InputSender`, `take_async_input` returns an `AsyncInputSender`. The "async" prefix distinguishes the async channel variant. This is consistent.
- **`InputSender` vs `AsyncInputSender`**: Consistent naming with `Async` prefix for the async variant.
- **`drain_on_cancel()` vs `drain_timeout` field**: The builder method name describes the *intent* while the field name describes the *mechanism*. After making fields private, users only see the builder method name.

### Testing

- All existing tests must pass
- Clippy clean
- Examples must compile


### API visibility and surface area

## Design: Restrict Internal Types to pub(crate)

**Item:** `api-visibility`
**Priority:** P1
**Status:** Design

### Problem

instancy exposes too many internal types through `pub mod` declarations, making the public API surface large and confusing. Users can access implementation details like `ExecutorTask`, `WorkerPool`, `ProgressTracker`, `TaskScheduler`, etc. that are not part of the intended API.

### Strategy

1. Change modules that are purely internal to `pub(crate) mod`
2. For modules with mixed public/internal content, keep the module `pub` but restrict internal items
3. Re-export user-facing types that live in newly-restricted modules via `lib.rs`

### Changes

#### Modules → `pub(crate)` (no external consumers)

| Module | Reason |
|--------|--------|
| `executor_task` | Runtime internals — `TaskId`, `PollOutcome`, `ExecutorTask`, `PoolWaker`, `ExecutorRegistry` |
| `worker_pool` | Runtime internals — `WorkerPoolConfig`, `WorkerPool` |

#### Types to restrict within public modules

##### `worker` module — keep `pub mod`, restrict internals
- Keep `pub`: `WorkerId` (used in tests)
- Restrict to `pub(crate)`: `WorkerContext`, `OperatorActivation`

##### `scheduler` module — keep `pub mod` for `policy`, restrict rest
- Keep `pub`: `policy::SchedulingPolicy`, `policy::PriorityPolicy`, `policy::PriorityWithAgingPolicy` (used in tests)
- Restrict to `pub(crate)`: `batching::*`, `task_scheduler::*` (`ComputeTask`, `StagePermit`, `SchedulerConfig`, `TaskScheduler`)

##### `progress` module — keep `pub mod`, restrict deep internals
- Keep `pub`: `timestamp::Timestamp` (used in tests/examples), `capability`, `frontier`, `notificator` (used by operator authors)
- Restrict to `pub(crate)`: `subgraph::*`, `reachability::*`, `network_progress::*`, `progress_channel::*`, `operate::*`, `mutable_antichain` (if not used externally)

##### `communication` module — keep `pub mod`, restrict wire-level internals
- Keep `pub`: `Codec`, `CodecError`, `ConnectionManager`, `ConnectionPool`, `SharedConnectionConfig`, `SharedPeerManager`, `ClusterSpawnTransport`, `PeerConnection`, `Frame`, `TransportError`, `DataflowSession`, `DataflowSessionBuilder`, `DynConnectionFactory`
- Restrict to `pub(crate)`: `allocator`, `control_protocol`, `interprocess` (except `PROGRESS_CHANNEL_ID`), `probing`, `sequencing`, `remote_push`, `progress_exchange`

##### `dataflow` module — keep `pub mod`, restrict internals
- Keep `pub`: `DataflowBuilder`, `StreamEdge`, `DataflowGraph`, `Pipe`, `OutputPort`, operator traits and types
- Restrict to `pub(crate)`: `executor`, `spec`, `control` internals, `channels::*` internals (edge_materializer, exchange_channel, mock_network, wake, bounded, envelope)

#### New re-exports in `lib.rs`

Add re-exports for types that users need but live in restricted submodules:
```rust
pub use worker::WorkerId;
pub use progress::timestamp::Timestamp;
pub use scheduler::policy::{SchedulingPolicy, PriorityPolicy, PriorityWithAgingPolicy};
```

### Approach

Rather than a massive breaking change, take a conservative approach:
1. Restrict `executor_task` and `worker_pool` (zero external usage)
2. Restrict clearly-internal submodules within `progress`, `scheduler`, `communication`, `dataflow`
3. Add re-exports for anything that breaks examples/tests
4. Verify with `cargo check` + `cargo test`

### Testing

- All existing tests must pass
- All examples must compile
- `cargo clippy --all-features --tests -- -D warnings` must pass
- `cargo doc` should show a cleaner API surface


## Development approach and architecture evolution

### Development approach

The original development plan used contract-and-test-driven delivery: define traits and interfaces first, then implement them, with PRs ordered by dependency and intentionally kept reviewable. That discipline matters for future design work even though the root-level plan file has been retired.

### Generic type parameter naming

| Parameter | Meaning | Typical use |
|---|---|---|
| `T` | Timestamp type | `Timestamp`, `Capability<T>`, `InputEvent<T, D>` |
| `D` | Data record type | `StreamEdge<S, D>`, `Envelope<T, D, M>` |
| `M` | User-defined metadata | `Envelope<T, D, M>` |
| `S` | Scope or path-summary context | `StreamEdge<S, D>` |
| `TOuter` / `TInner` | Nested timestamp components | `Product<TOuter, TInner>` |

Rules preserved from the old plan:
- use `T` for timestamps, not arbitrary payload types
- use `D` for data and `M` for metadata
- keep naming consistent across docs, examples, and tests

### Three conceptual layers of a dataflow

instancy keeps three distinct conceptual layers:

1. **Dataflow graph** — the logical operator topology, scopes, connectivity, and progress semantics.
2. **Typed stream graph** — the typed edges, routing decisions, and channel metadata that bind the logical graph to actual data movement.
3. **Pipe/builder layer** — the fluent construction API used while assembling a logical dataflow.

This separation explains why the builder/runtime split matters: builder-time handles are not runtime streams, and runtime materialization is intentionally a later phase.

### ADR-001 — Separated builder and runtime

The old development plan's primary architectural decision is retained: graph construction is separate from execution.

- **Phase 1:** build a `LogicalDataflow` with `DataflowBuilder`
- **Phase 2:** submit the logical graph to a `RuntimeHandle`
- **Phase 3:** interact with execution via inputs, outputs, cancellation, completion, and metrics

Benefits retained from the ADR:
- inspectable/testable logical graphs without a running runtime
- reuse of the same logical graph across different runtime configurations
- clean separation between logical construction, physical execution, and I/O
- async-friendly handles without embedding runtime concerns into graph building
