# Operators

This document describes the operator surface of instancy: the built-in operator set, the user-extensible API, the runtime error policy seen by operators, and end-to-end examples of building and driving dataflows.

Back to the overview: [docs/DESIGN.md](./DESIGN.md)

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
    /// The affected stage/partition cannot make progress.
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
| Peer node down | **Hosting application** reports via `RuntimeHandle::report_node_leave(node_id)` | `CancellationReason::PeerDown` → cancel affected dataflows |

**Peer-Down Notification Model**

Cluster health monitoring (heartbeats, liveness probes, etc.) is **not** the responsibility of instancy — it is the hosting application's responsibility. Different applications have vastly different network topologies and health detection needs (e.g., Kubernetes pod watchers, actor framework supervision, custom heartbeat protocols).

instancy provides a notification API that the hosting application calls when it determines a peer is unreachable:

```rust
/// Report that a peer node is no longer reachable.
/// All cluster dataflows with workers on the downed peer are cancelled with
/// `CancellationReason::PeerDown { node_id: peer_node_id }`.
/// Returns the number of dataflows that were newly cancelled.
let cancelled = runtime_handle.report_node_leave("node-3");
```

**Implementation details:**
- Internally, a `PeerRegistry` maps each remote peer's `node_id` to the cancel tokens of cluster dataflows referencing that peer.
- When `spawn_cluster()` completes, the dataflow's worker and bridge cancel tokens are automatically registered for each remote peer.
- `report_node_leave()` cancels all matching tokens, removes entries, and prunes stale (already-completed) registrations.
- The method is idempotent: calling it again for the same node returns `0` once all associated dataflows are already cancelled.
- The node is remembered as "left" and any future `spawn_cluster()` that includes it will be immediately cancelled.
- `report_node_join(node_id)` removes the node from the "left" set, allowing subsequent dataflows to use it normally. Already-cancelled dataflows are not restarted.

**Design principles:**
- **No automatic rescheduling**: instancy does not attempt to shift computation to surviving nodes. The hosting application handles retry by resubmitting the dataflow on healthy nodes.
- **Application is the source of truth**: The runtime trusts the application's peer-down notification and does not perform its own consensus or heartbeat protocol.
- **Clean cancellation**: Affected dataflows receive `CancellationReason::PeerDown` through the existing `CancellationToken` mechanism, allowing operators to clean up gracefully.
- **Connection cleanup**: The connection pool evicts all connections to the downed peer.

For `ErrorAction::Retry` on network errors:
1. The failed send/receive is retried after exponential backoff.
2. If the connection is dead, the connection pool establishes a new one.
3. After `max_retries`, the fallback action (typically `Stop`) is applied.

For node leave (`report_node_leave(node_id)`):
1. The hosting application calls `report_node_leave(node_id)`.
2. All pooled connections to the departed node are dropped and evicted from the connection pool.
3. The runtime identifies all active dataflows with workers on that node.
4. Each affected dataflow's `CancellationToken` is triggered with `CancellationReason::PeerDown { node_id }`.
5. Operators observe cancellation, drop capabilities, and exit.
6. `execute()` / `DataflowHandle` returns with an error indicating peer failure.
7. The node is recorded as "left" — any future `spawn_cluster` referencing it is immediately cancelled.
8. The hosting application can retry the dataflow on healthy nodes.

For node join (`report_node_join(node_id)`):
1. The hosting application calls `report_node_join(node_id)` when a node recovers or a new node is added.
2. The node is removed from the "left" set (no-op if the node was never marked as left).
3. Future `spawn_cluster` calls that include this node proceed normally.
4. New connections to the node are established on demand via the `ConnectionManager` when a dataflow first needs to communicate with it.
5. Already-cancelled dataflows are **not** restarted — the application must re-spawn them.


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

## 9. Operators

### 9.1 Core Operators (this crate)

| Operator | Description |
|---|---|
| `from_stream` | Binds an external `TimestampedInput` async stream into the dataflow; manages capabilities automatically |
| `unary` | One input, one output; user-supplied async closure |
| `binary` | Two inputs, one output; user-supplied async closure |
| `branch` / `ok_err` | One input, two outputs; partition by predicate |
| `feedback` / `loop_variable` | Creates a feedback edge for iteration with timestamp advancement |
| `exchange` | Repartitions data across workers by a routing function (hash-based); creates a stage boundary when parallelism changes |
| `rebalance` | Round-robin distribution across target replicas; used at stage boundaries when key doesn't matter |
| `gather` | Funnels all data to a single replica (parallelism 1); used for global aggregation |
| `broadcast` | Sends each record to **all** workers (clones data); in a cluster with per-stage parallelism, the runtime automatically uses local channels when source and target stages are on the same node — no separate "local" variant needed (see [Local Broadcast](./execution-model.md#local-broadcast-via-per-stage-parallelism) in execution-model.md) |
| `delay` | Holds data until the frontier advances past a specified timestamp; useful for windowing and time-based buffering |
| `concat` | Merges multiple streams into one |
| `inspect` | Pass-through side-effect observation (logging, debugging); data continues downstream |
| `for_each` | Terminal side-effect sink; consumes the stream, no output produced. Panics are caught and converted to `Error::OperatorPanic` |
| `probe` | Observe frontier progress; returns `ProbeHandle` for async progress tracking (`done_with`, `is_done`, `wait_until_done`) |

### 9.2 Unary Operator API

```rust
pub trait Operator<S: Scope, C: Container> {
    /// Creates a unary operator with one input and one output.
    /// The closure is synchronous — it runs on the Worker Thread Pool thread directly.
    fn unary<C2, L>(
        &self,
        name: &str,
        logic: L,
    ) -> StreamEdge<S, C2>
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
    ) -> StreamEdge<S, C2>
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
    other: &StreamEdge<S, C2>,
    name: &str,
    logic: L,
) -> StreamEdge<S, C3>
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
) -> (StreamEdge<S, C>, StreamEdge<S, C>);

fn ok_err<O, E>(
    &self,
    logic: impl Fn(T) -> Result<O, E> + Send + Sync + 'static,
) -> (StreamEdge<S, Vec<O>>, StreamEdge<S, Vec<E>>);
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
) -> StreamEdge<S, C>
where
    F: Fn(&T, &C::Item) -> T + Send + Sync + 'static;

/// Delays all data at timestamp `t` to a single new timestamp computed from `t`.
/// Simpler version when the delay depends only on the timestamp, not the data.
fn delay_batch<F>(
    &self,
    delay_fn: F,
) -> StreamEdge<S, C>
where
    F: Fn(&T) -> T + Send + Sync + 'static;
```

**Semantics**: The operator holds a capability for each output timestamp that has buffered data. When the input frontier advances past a buffered timestamp, the data is emitted at the delayed timestamp and the capability is released. This ensures downstream operators see correct frontier progress.

**Use cases**:
- **Windowing**: `delay_batch(|t| t / window_size * window_size)` groups data into fixed windows.
- **Ordering**: `delay_batch(|t| *t)` (identity) buffers data until the frontier confirms no more data at `t` will arrive.
- **Rate limiting**: delay data to spread output over time.

### 9.7 Extension Point

Extension crates add operators by implementing traits on `StreamEdge`:

```rust
// In crate `instancy-extras`
pub trait MapOperator<S: Scope, T: Data> {
    fn map<U: Data>(
        &self,
        f: impl Fn(T) -> U + Send + Sync + 'static,
    ) -> StreamEdge<S, Vec<U>>;
}

impl<S: Scope, T: Data> MapOperator<S, T> for StreamEdge<S, Vec<T>> {
    fn map<U: Data>(&self, f: impl Fn(T) -> U + Send + Sync + 'static) -> StreamEdge<S, Vec<U>> {
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

### 9.8 Operator Context Injection

Operators often need access to shared configuration, schema registries, metrics
collectors, or other application-specific services. instancy provides a typed
context system via `SharedContext` and `DataflowBuilder`.

#### Current: Build-Time Capture (v0.1)

Context is set on the builder and captured by operator closures at graph construction time:

```rust
// Set context on the builder
builder.with_context(AppConfig { batch_size: 1024 });
// Or share a pre-existing Arc (avoids double-wrapping)
builder.with_context_arc(db_pool.clone());

// Capture at build time — returns Arc<T>
let cfg = builder.get_context::<AppConfig>().unwrap();
input.map("transform", move |_t, x| process(x, cfg.batch_size));
```

**Scope rules:**
- One `SharedContext` per `DataflowBuilder` (per worker replica)
- Context is inherited by `iterate()` inner scopes (cheap `Arc` sharing)
- Context survives `build()` into `LogicalDataflow` via `contexts()` accessor
- Type-keyed: one value per type; use newtypes for multiple values of the same type
- `with_context_arc()` enables true zero-copy sharing across workers

#### Planned: Runtime Operator Access (future)

A future enhancement will thread `SharedContext` through the operator runtime so
callbacks can access context directly without manual capture:

```rust
// Future API — operators receive context automatically
input.unary_with_context("transform", |ctx, input, output| {
    let cfg = ctx.get::<AppConfig>().unwrap();
    while let Some((time, data)) = input.next() {
        // Use cfg without pre-capturing it
        let mut session = output.session(&time);
        for item in data {
            session.give(process(item, cfg.batch_size))?;
        }
    }
    Ok(())
});
```

This requires:
1. Passing `SharedContext` through `DataflowExecutor::materialize()` to each operator
2. Adding context to `OperatorBlueprint::build()` or a new `MaterializationContext`
3. Making context available as a parameter in operator activation callbacks
4. Keeping `WorkerContext` separate (worker identity only) from `SharedContext` (user data)

The build-time capture pattern remains the recommended approach for simple cases.
Runtime access is intended for complex operators (e.g., custom `unary`/`binary`)
where manual capture is cumbersome.

### 9.9 Cross-Worker Control Broadcast

When multiple workers execute the same dataflow in parallel, an operator
failure in one worker must propagate to all siblings so they cancel
promptly instead of hanging or producing incomplete results.

#### Architecture

instancy provides a built-in **control broadcast channel** that operates on
the management plane (separate from the data-plane `ControlSignal` used for
watermarks/errors within edges):

```
┌─────────┐        ┌──────────────────────┐        ┌─────────┐
│ Worker 0 │──tx──►│  ControlBroadcast     │◄──tx──│ Worker 1 │
│          │◄──rx──│  (Arc<Mutex<Vec>>)    │──rx──►│          │
└─────────┘        │  + dataflow cancel    │        └─────────┘
                   └──────────────────────┘
```

- **`ControlSender`** (cloneable): any worker can broadcast signals.
- **`ControlReceiver`** (single-owner): each worker drains new signals with
  an independent read cursor.
- **`WorkerControl`** enum:
  - `WorkerError { worker_index, operator, message }` — triggers automatic cancellation.
  - `Cancel { worker_index, reason }` — explicit cancel request.
  - `LimitReached { worker_index, description }` — informational, does **not** auto-cancel.

#### Cancellation flow

1. Worker A's operator panics/errors.
2. `DataflowExecutor::run_one_sweep()` catches the error, calls
   `control_sender.broadcast_error(op_name, message)`.
3. The sender appends the signal and cancels the **shared dataflow
   `CancellationToken`** (child of the runtime token, parent of all
   worker tokens).
4. Worker B's next sweep calls `cancel.check()` → sees cancellation →
   returns `Err(Cancelled { reason: OperatorError(...) })`.

#### Token hierarchy for multi-worker dataflows

```
RuntimeToken
  └── DataflowToken  (shared by all workers in this dataflow)
        ├── WorkerToken[0]
        ├── WorkerToken[1]
        └── ...
```

Cancelling the `DataflowToken` cascades to all worker tokens without
affecting other dataflows on the same runtime.

#### Single-worker optimization

For single-worker dataflows (`num_workers == 1`), no `ControlBroadcast`
is created — zero overhead. The executor's `control_sender` and
`control_receiver` fields remain `None`.

---

### 9.10 Async Data Source Integration (`source_async`)

The `source_async` operator allows users to declare a data source at build
time using an async closure. Unlike the `input()` API where the caller
manually sends data via an `InputSender`/`AsyncInputSender`, `source_async`
encapsulates the data-producing logic within the dataflow definition.

#### API

```rust
pub fn source_async<D, F, Fut>(
    &self,
    name: impl Into<String>,
    producer: F,
) -> Pipe<T, D>
where
    D: Clone + Send + 'static,
    F: FnOnce(AsyncInputSender<T, D>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
```

The `producer` receives an `AsyncInputSender` and drives data into the
dataflow. The runtime manages the producer's lifecycle:

1. **Build time**: The producer closure is stored (type-erased) in the
   `LogicalDataflow`.
2. **Spawn time**: A tokio channel is created, a `ChannelSourceOperator`
   wired to the receiver end, and a **pump thread** spawned to run the
   producer in a dedicated single-threaded tokio runtime.
3. **Runtime**: The pump thread executes `producer(sender)`. Data flows
   through the bounded tokio channel into the operator. Backpressure
   is natural — `sender.send()` yields when the channel is full.
4. **Completion**: When the producer returns (Ok or Err), the sender is
   dropped, closing the channel. The `ChannelSourceOperator` detects
   disconnection and releases its capability, allowing the dataflow to
   complete.
5. **Cancellation**: The pump thread runs in a `tokio::select!` with the
   dataflow's `CancellationToken`. On cancellation, the sender is dropped
   immediately, and any in-flight `send()` in the producer returns an error.

#### Frontier propagation

The `ChannelSourceOperator` properly handles `InputEvent::Frontier(t)`
events by advancing its held capability. When a frontier event arrives,
the operator drops its capability at the old time and acquires one at the
new time, allowing downstream frontier-sensitive operators (e.g.,
`unary_notify`) to fire notifications for completed timestamps.

#### Quiescence tracking

Each `source_async` port increments the `external_inputs_open` counter
at spawn time (same as `input()` ports). The counter is decremented when
the pump's sender is dropped and the `ChannelSourceOperator` finishes,
allowing the executor to detect quiescence and complete.

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
        NodeConfig { node_id: "10.0.0.1:8080".into(), workers: 4 },
        NodeConfig { node_id: "10.0.0.2:8080".into(), workers: 1 },
        NodeConfig { node_id: "10.0.0.3:8080".into(), workers: 8 },
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
