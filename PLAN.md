# instancy Development Plan

## Approach
Contract-and-test-driven development. Each PR defines traits/interfaces first, then implements them, accompanied by comprehensive tests. PRs are ordered by dependency and sized under 5000 lines (including tests). Terminology renames (pending approval) will be applied via a dedicated PR early in the process.

## Coding Conventions

### Generic Type Parameter Naming

All generic type parameters must follow these codebase-wide conventions for consistency:

| Parameter | Meaning | Example |
|---|---|---|
| `T` | Timestamp type (implements `Timestamp`) | `Envelope<T, D, M>`, `Capability<T>` |
| `D` | Data record type | `StreamEdge<S, D>`, `InputEvent<T, D>` |
| `M` | User-defined metadata type (default `()`) | `Envelope<T, D, M>`, `Push<T, D, M>` |
| `S` | Scope (implements `Scope` trait) or Summary (PathSummary) | `StreamEdge<S, D>`, `PortConnectivity<S>` |
| `TOuter` / `TInner` | Nested timestamp components in `Product` | `Product<TOuter, TInner>` |

**Rules:**
- Never use `T` for non-timestamp generic data. Use `D` for data, `M` for metadata.
- When a generic container is not timestamp-specific (e.g., `Antichain<T>`, `ChangeBatch<T>`), `T` refers to the element type, which is typically a timestamp in practice.
- Maintain these conventions in all new code, tests, and documentation.

---

## Conceptual Architecture: Three Layers of a Dataflow

instancy separates concerns into three distinct conceptual layers. Understanding these
layers is essential for navigating the codebase and knowing where new code belongs.

### Layer 1 — Dataflow Graph (Abstract Topology)

The pure logical structure of a computation. Operators are vertices, edges describe
data flow between them. This layer knows about **scopes**, **regions**, **port
connectivity**, and **progress tracking** — but has no knowledge of data types.

| Concept | Description | Location |
|---|---|---|
| Operator | A vertex with typed input/output ports | `progress/operate.rs` |
| PortConnectivity | Which inputs connect to which outputs (with path summaries) | `progress/operate.rs` |
| Scope | A context that manages a set of operators and tracks progress | `dataflow/scope.rs` |
| Region | A scheduling unit grouping operators for concurrent execution | `dataflow/region.rs` |
| Reachability | Progress tracking across the operator graph | `progress/reachability.rs` |

**Lifetime**: Persists from construction through execution. Drives the progress
protocol and scheduling decisions at runtime.

### Layer 2 — Typed Stream Graph (Data-Bound Topology)

Binds the abstract graph with concrete data types and routing strategies. Describes
*what data flows where* — which operator output produces type `D`, which input
consumes it, and how data is partitioned across targets.

| Concept | Description | Location |
|---|---|---|
| `StreamEdge<S, D>` | A typed edge from an operator's output slot, carrying data `D` within scope `S` | `dataflow/stream.rs` |
| `StreamConnection<D>` | Full wiring: source slot → target slot with partition strategy | `dataflow/stream.rs` |
| `StreamTarget` | A target slot with its routing strategy name | `dataflow/stream.rs` |

**Lifetime**: Created during graph construction, stored in `LogicalDataflow`, consumed
during materialization to create physical channels.

### Layer 3 — Pipe (Construction Plumbing)

A transient, builder-time handle used to construct the dataflow via fluent method
chaining. A `Pipe` holds a shared reference to the builder's internal state and
represents "an operator's output that you can attach more processing to."

| Concept | Description | Location |
|---|---|---|
| `Pipe<T, D>` | Fluent handle: `.map()`, `.filter()`, `.binary()`, `.output()` | `dataflow/dataflow_builder.rs` |
| `DataflowBuilder<T>` | Allocates operators, records edges, produces `LogicalDataflow<T>` | `dataflow/dataflow_builder.rs` |
| `OutputPort<T, D>` | Terminal handle from `.output()`, provides result collector | `dataflow/dataflow_builder.rs` |

**Lifetime**: Ephemeral — exists only during `DataflowBuilder` construction.
Consumed by `.build()` which produces the `LogicalDataflow`. Pipes do not exist
at runtime.

### How the Layers Relate

```
  Construction time                    Runtime
  ────────────────                     ───────

  Pipe<T, D>          ──.build()──►  LogicalDataflow<T>  ──materialize()──►  Executor
  (Layer 3)                           ├─ Dataflow Graph    (Layer 1)          (physical)
  fluent chaining                     │  operators, scopes, regions
  records into ──►                    └─ Typed Stream Graph (Layer 2)
  BuilderState                           StreamEdges, StreamConnections
                                         channel factories
```

1. **You use Pipes** (Layer 3) to plumb together operators via chaining
2. **The builder records StreamEdges/Connections** (Layer 2) as the typed wiring
3. **Underneath sits the Dataflow Graph** (Layer 1) — abstract topology that drives
   progress tracking and scheduling
4. **Materialization** turns the logical graph into physical channels and schedulable
   operators for the runtime

This separation ensures that graph construction (Pipe), typed data routing
(StreamEdge), and progress/scheduling (Dataflow Graph) remain independently
testable and evolvable.

---

## PR 1 — Workspace scaffold + core types
**Goal**: Establish workspace structure, error types, `PartialOrder`, `Timestamp`, `PathSummary`.

- Workspace `Cargo.toml` (workspace members, shared dependencies)
- Crate `instancy/Cargo.toml` with initial dependencies
- `lib.rs` — module declarations, re-exports
- `error.rs` — `Error` enum with all variants (Io, Codec, Connection, Cancelled, Progress, Operator, Custom)
- `order.rs` — `PartialOrder` trait + implementations for `()`, `usize`, `u32`, `u64`, `i32`, `(A,B)` (product order)
- `progress/mod.rs`, `progress/timestamp.rs` — `Timestamp` trait, `PathSummary` trait, implementations for primitives and `Product<TOuter, TInner>`

**Tests**:
- PartialOrder: reflexivity, antisymmetry, transitivity for all impls; product order partial comparisons
- Timestamp: `minimum()`, `Send + Sync` bounds compile
- PathSummary: `results_in` correctness, `followed_by` composition, identity summary, overflow → `None`
- Error: `Display`/`Debug` formatting, `From` conversions

**Estimated size**: ~800 lines

---

## PR 2 — Progress primitives (Frontier, ChangeBatch)
**Goal**: Implement the core progress data structures.

- `progress/frontier.rs`
  - `Antichain<T>` — immutable set of incomparable elements; `new`, `from_elem`, `less_than`, `less_equal`, `dominates`, `elements`, `is_empty`, `Eq`/`PartialOrd`
  - `MutableAntichain<T>` — mutable frontier tracker; `update`, `update_iter`, `frontier()`, `is_empty`, tracks multiplicity changes
- `progress/change_batch.rs`
  - `ChangeBatch<T>` — accumulated `(T, i64)` updates; `update`, `extend`, `drain`, `is_empty`, `into_inner`, compaction

**Tests**:
- Antichain: construction, `less_than`/`less_equal` with product timestamps, merge behavior
- MutableAntichain: single updates, batch updates, frontier correctness after insert/remove sequences, empty frontier
- ChangeBatch: accumulation, cancellation (positive + negative = 0), compaction, drain semantics
- Property: after N inserts and N removes of the same timestamp, frontier returns to previous state

**Estimated size**: ~1500 lines

---

## PR 3 — Capability system + Reachability tracker
**Goal**: Implement capability management and the reachability algorithm.

- `dataflow/operators/capability.rs`
  - `Capability<T>` — holds a timestamp, reports to progress tracker on drop
  - `CapabilityRef<T>` — borrowed view
  - `CapabilitySet<T>` — manages a set of capabilities; `downgrade`, `delayed`, `retain`
  - `ProgressReporter<T>` — internal channel for capability drop notifications
- `progress/operate.rs`
  - `OperatorCore` trait — `inputs()`, `outputs()`, `get_internal_summary()`, `notify_frontier_change()`
  - `OperatorProgress` — consumed/produced/internal changes per port
- `progress/reachability.rs`
  - Port/Location types (`Source`/`Target` or `OutputPort`/`InputPort`)
  - `Tracker` — builds reachability graph from operator summaries, computes which timestamps can reach which ports
  - `Builder` — construct the reachability graph from operator connectivity

**Tests**:
- Capability: create, downgrade, drop reports progress, `delayed` produces new cap at future time
- CapabilitySet: retain semantics, downgrade-all, empty set
- Reachability: linear chain (A→B→C), diamond graph, self-loop with summary, unreachable ports
- OperatorProgress: report consumed/produced/internals, batch accumulation

**Estimated size**: ~2500 lines

---

## PR 4 — Scope, Stream, intra-process channels
**Goal**: Define the dataflow graph abstractions, local communication with structured message envelopes, and execution region types.

- `dataflow/scope.rs`
  - `Scope` trait — `name()`, `addr()`, `add_operator()`, `allocate_operator_index()`, timestamp type
  - `ChildScope<T>` — implementation for nested scopes
- `dataflow/stream.rs`
  - `Stream<S: Scope, C>` — a named edge in the dataflow graph; connects output port to downstream operators
  - `.with_parallelism(n)` — sets the execution region for subsequent operators
  - `.in_region(&Region)` — assigns stream to a named execution region
- `dataflow/region.rs`
  - `Region` — execution region with `parallelism` and `PlacementPolicy`
  - `PlacementPolicy` — `Proportional` (default), `RoundRobin`, `Pinned { node_index }`
  - Validation: connecting operators across regions without explicit repartition is an error
- `dataflow/channels/mod.rs`
  - `Envelope<T, D>` — structured message: `Data { time, data }` | `Control(ControlSignal<T>)`
  - `ControlSignal<T>` — `Error { source_operator, message }` | `Watermark(T)`
  - `Push<T>` / `Pull<T>` traits — channel send/receive abstractions
- `dataflow/channels/pact.rs`
  - `PartitionStrategy` enum/trait — `Pipeline` (no shuffle), `Exchange(fn)` (hash routing), `Rebalance` (round-robin), `Gather` (to single replica), `Broadcast`, `BroadcastLocal`
- `communication/allocator.rs`
  - `ChannelAllocator` — creates local `mpsc`-based channel pairs for intra-process communication
  - Bounded channels with configurable buffer size

**Tests**:
- Scope: operator registration, index allocation, address hierarchy
- Stream: connects source to target, metadata propagation
- Region: construction, default parallelism, placement policies
- Region: validation — cross-region connection without repartition is rejected
- Envelope: Data variant round-trip, Control variant (Error, Watermark) creation and matching
- Push/Pull: send batch through local channel, receive in order, backpressure when full
- PartitionStrategy: Pipeline routes to same worker, Exchange routes by hash, Rebalance round-robins, Gather funnels, Broadcast fans out
- ChannelAllocator: allocate N channels, each pair is independent

**Estimated size**: ~2500 lines

---

## PR 5 — Worker thread pool + execution engine + provider traits
**Goal**: Implement the custom compute thread pool, logical worker model, provider traits, and runtime bootstrap.

- `providers/mod.rs`
  - `TransportProvider` trait — resolves logical targets to Push endpoints
  - `ExecutionProvider` trait — submits tasks for logical workers
  - `LogicalTarget` — `(RegionId, WorkerId, OperatorIndex, Port)`
- `providers/local_transport.rs`
  - `LocalTransport` — all targets resolved to bounded in-memory buffers (single-process default)
- `providers/in_memory_cluster.rs`
  - `InMemoryClusterTransport` — simulates multi-node in single process (for testing)
  - `InMemoryCluster` — virtual cluster state
- `providers/inline_execution.rs`
  - `InlineExecution` — runs all tasks on calling thread (for deterministic tests)
- `compute_pool.rs`
  - `WorkerPoolConfig` — `min_threads`, `max_threads`, `idle_shutdown`
  - `WorkerPool` — dynamic thread pool with shared task queue
  - Worker thread loop: spin → yield → park → shutdown lifecycle
  - `TaskQueue` — lock-free shared queue (crossbeam deque or similar)
  - Thread scaling: spawn on demand, shutdown on idle
  - Implements `ExecutionProvider` trait
- `scheduler.rs`
  - `TaskScheduler` — per-worker FIFO queues, per-region concurrency limits
  - `ComputeTask` — worker_id + activation + region permit
  - Dispatch logic: only dispatch when worker has no in-flight task AND region has capacity
- `worker.rs`
  - `WorkerId(usize)` — globally unique logical worker identity
  - `OperatorActivation` — queued work item for an operator
- `execute.rs`
  - `RuntimeConfig` — `WorkerPoolConfig`, optional Tokio runtime handle, transport provider, execution provider
  - `DataflowConfig` — cluster topology, cancellation token, `ErrorPolicy`
  - `ErrorPolicy` — `Stop` (default) or `Ignore { on_error }` per-dataflow error handling
  - `ClusterTopology` — `nodes: Vec<NodeConfig>`, `total_workers()`, `worker_range()`, `node_for_worker()`
  - `NodeConfig` — `node_index`, `workers`
  - `execute()` — entry point; creates Worker thread pool, I/O runtime, runs dataflow
  - `DataflowHandle<T, D>` — output streams + metrics + cancel token
- `metrics.rs`
  - `DataflowMetrics` — wall_time, total_cpu_time, per-operator metrics
  - `OperatorMetrics` — name, index, activations, cpu_time, records_processed

**Tests**:
- TransportProvider: LocalTransport resolves co-local targets to buffers
- TransportProvider: InMemoryClusterTransport resolves cross-node targets
- TransportProvider: is_local() correctly identifies co-located targets
- ExecutionProvider: WorkerPool submits and executes tasks
- ExecutionProvider: InlineExecution runs tasks synchronously on calling thread
- WorkerPool: tasks execute on pool threads
- WorkerPool: dynamic scaling — starts at min, grows under load, shrinks on idle
- WorkerPool: idle threads park (low CPU usage when no tasks)
- WorkerPool: threads above min shut down after idle_shutdown
- TaskScheduler: FIFO ordering within a worker
- TaskScheduler: per-region concurrency limit enforced
- TaskScheduler: dispatch only when worker has no in-flight task
- ClusterTopology: `total_workers()` sums correctly, `worker_range()` returns correct ranges
- ClusterTopology: heterogeneous configs (4, 1, 8 workers)
- ErrorPolicy: Stop and Ignore variants construct correctly, default is Stop
- DataflowMetrics: accumulation of operator metrics
- execute(): basic smoke test — empty dataflow starts and completes
- Runtime isolation: passing an external Tokio handle for I/O works
- **Multi-node in single process**: InMemoryClusterTransport + InlineExecution runs a distributed dataflow in one test

**Estimated size**: ~3500 lines

---

## PR 6 — Input/Output system (from_stream, output, DataflowSpec)
**Goal**: Implement stream-driven input binding and async output stream emission.

**Note**: PR6 implemented the pull-based `OutputStream` approach. The sink-first
`OutputSink` trait (push-based primary path) will be added in PR 17 as part of
the full inter-process dataflow wiring, where the orchestrator knows the complete
topology and can wire outputs directly to their destination at construction time.
See DESIGN.md §5.5 for the dual-model design (OutputSink + OutputStream).

- `dataflow/operators/from_stream.rs`
  - `InputEvent<T, D>` enum — `Data(T, Vec<D>)`, `Frontier(T)`
  - `TimestampedInput<T, D>` trait (blanket impl for any matching `Stream`)
  - `from_stream` operator — spawns reader task on I/O runtime, manages capabilities, posts to Worker thread pool
- `dataflow/operators/output.rs`
  - `OutputEvent<T, D>` enum — `Data(T, Vec<D>)`, `Frontier(T)`
  - `OutputStream<T, D>` — `Pin<Box<dyn Stream<Item = OutputEvent<T, D>> + Send>>`
  - `.output()` terminal operator — produces one async stream per worker in the last region
  - Internal bounded buffer bridges Worker thread pool → async stream consumer
- `dataflow/spec.rs`
  - `DataflowSpec<T, D>` — builder for binding inputs + graph + output streams
  - `DataflowInputs<T>` — accessor for named input streams inside the builder closure
  - `DataflowHandle<T, D>` — returned by `execute()`, holds output streams + metrics + cancel
  - `ErasedTimestampedInput<T>` — type-erased input stream for heterogeneous inputs
- `dataflow/handles.rs`
  - `InputHandle<T, C>` — operator-side input reading
  - `OutputHandle<T, C>` — operator-side output writing + session API

**Tests**:
- InputEvent stream → capabilities created for each timestamp, dropped on Frontier advance
- Stream ends → all capabilities dropped, input complete
- DataflowSpec: bind multiple named inputs, access by name inside builder
- InputHandle: `next()` yields batches in order
- OutputHandle: `session(&time).give()` produces output
- Backpressure: from_stream respects buffer bounds
- OutputStream: consumer receives Data events in order
- OutputStream: Frontier events emitted when output frontier advances
- OutputStream: multiple output streams (one per worker) when parallelism > 1
- OutputStream: backpressure — slow consumer slows down pipeline
- DataflowHandle: cancel token stops dataflow, output streams end

**Estimated size**: ~2500 lines

---

## PR 7 — Operators: unary, inspect, probe
**Goal**: First working operator pipeline. End-to-end test.

- `dataflow/operators/unary.rs`
  - `unary()` — synchronous closure variant (the standard operator form)
  - `unary_with_metadata()` — variant that receives and can modify envelope metadata
  - Registers operator with scope, wires input/output buffers
- `dataflow/operators/inspect.rs`
  - `inspect()` — side-effect observation, passes data through unchanged
- `dataflow/operators/probe.rs`
  - `ProbeHandle<T>` — `less_than()`, `less_equal()`, `async_wait_for()`, `frontier_watch()`
  - `probe()` on Stream — attaches a probe to observe frontier
- `dataflow/operators/mod.rs` — trait impls on `Stream` for operator chaining

**Tests**:
- unary: identity pass-through, stateful accumulation, error propagation from closure
- unary_with_metadata: metadata flows through, can be modified
- inspect: callback receives all data, output stream equals input stream
- probe: `less_than` reflects frontier, `async_wait_for` resolves when frontier advances past target
- **End-to-end**: `from_stream → unary(double) → inspect(collect) → output` — verify output stream results

**Estimated size**: ~2500 lines

**Estimated size**: ~2500 lines

---

## PR 8 — Operators: binary, concat, delay
**Goal**: Multi-input operators and time-based buffering.

- `dataflow/operators/binary.rs`
  - `binary()` — two inputs, one output, sync closure
  - `binary_async()` — async variant
- `dataflow/operators/concat.rs`
  - `concat()` — merge multiple streams into one, preserving timestamps
- `dataflow/operators/delay.rs`
  - `delay(delay_fn)` — per-record timestamp reassignment; buffers until frontier advances
  - `delay_batch(delay_fn)` — per-timestamp reassignment (simpler variant)
  - Internal buffer keyed by output timestamp, releases on frontier advance

**Tests**:
- binary: join-like logic (match items from two streams by timestamp)
- binary: one input finishes before the other — correct completion
- concat: N streams merged, all data present, timestamps preserved
- concat: empty streams handled correctly
- delay: data buffered until frontier advances past original timestamp
- delay: delayed timestamps are correct per delay_fn
- delay_batch: all data at same timestamp delayed together
- delay: capabilities held for buffered timestamps, released on flush
- **End-to-end**: two `from_stream` inputs → `binary` → `inspect` → `probe`
- **End-to-end**: `from_stream → delay_batch → inspect` — verify output order

**Estimated size**: ~2000 lines

---

## PR 9 — Progress tracker integration
**Goal**: Wire up progress tracking so frontiers actually advance through the dataflow.

- `progress/subgraph.rs`
  - `SubgraphBuilder` — operator registry, edge connectivity, progress graph construction
  - Progress tracker async task — receives updates from operators, runs reachability, broadcasts frontier changes
  - `ProgressMode` — `Eager` (immediate propagation) vs `Demand` (batched)
- Integration wiring:
  - Operators report consumed/produced/internals after each activation
  - Frontier changes flow back to operators via watch channels
  - from_stream capabilities drive initial progress

**Tests**:
- Linear pipeline: frontier advances from input through all operators to probe
- Fan-out: one input, two consumers — both see frontier advance
- Stalled frontier: operator holds capability → downstream frontier does not advance
- Capability drop → frontier advances
- Multi-worker: progress aggregated across workers correctly
- **End-to-end**: full pipeline with progress — `from_stream → unary → unary → probe`, verify `async_wait_for` unblocks at correct timestamps

**Estimated size**: ~3000 lines

---

## PR 10 — Exchange, rebalance, gather, broadcast operators
**Goal**: Data redistribution across local workers with per-region parallelism support.

- `dataflow/operators/exchange.rs`
  - `exchange(route_fn)` — repartitions data across all workers by hash
  - Creates region boundary when target parallelism differs from source
  - Uses `PartitionStrategy::Exchange` with `ChannelAllocator` for local workers
- `dataflow/operators/rebalance.rs`
  - `rebalance()` — round-robin distribution across target replicas
  - Used at region boundaries when data distribution doesn't depend on key
- `dataflow/operators/gather.rs`
  - `gather()` — funnels all data to a single replica (parallelism 1)
  - Used for global aggregation/sorting
- `dataflow/operators/broadcast.rs`
  - `broadcast()` — sends each record to all workers (cross-process when networked)
  - `broadcast_local()` — sends each record to all workers in the same process only
  - Uses `PartitionStrategy::Broadcast` / `BroadcastLocal`

**Tests**:
- exchange: items routed to correct worker by hash, multi-worker single-process
- exchange: all data accounted for (no loss, no duplication)
- exchange: region boundary — repartition from 4 → 16 workers
- rebalance: data distributed round-robin across target replicas
- rebalance: even distribution verified (each replica gets ≈ equal share)
- gather: all data arrives at single replica
- gather: works as 16 → 1 repartition
- broadcast: every worker receives every item (single-process for now)
- broadcast_local: every local worker receives every item
- **End-to-end**: `from_stream → exchange(4→16) → unary(count per worker) → gather → probe`
- **End-to-end**: `from_stream → rebalance → unary → probe`

**Estimated size**: ~2500 lines

---

## PR 11 — Branch + ok_err operators
**Goal**: Conditional stream splitting.

- `dataflow/operators/branch.rs`
  - `branch(predicate)` → `(Stream true, Stream false)`
  - `ok_err(fn → Result)` → `(Stream Ok, Stream Err)`

**Tests**:
- branch: even/odd split, both output streams complete, empty input
- ok_err: Result-based split, error stream captures all Err values
- **End-to-end**: `from_stream → branch → (inspect true, inspect false) → probe both`

**Estimated size**: ~1000 lines

---

## PR 12 — Loops + nested scopes
**Goal**: Iterative computation support.

- `dataflow/operators/feedback.rs`
  - `loop_variable()` / `feedback(summary)` — creates a feedback edge with timestamp advancement
  - `connect_loop()` — closes the loop
- `dataflow/scope.rs` additions:
  - `iterative()` — creates a nested scope for iteration
  - `enter_scope()` / `exit_scope()` — wraps/unwraps `Product<TOuter, TInner>` timestamps
  - **Validation**: all operators inside `iterative()` must share the same execution region (no parallelism changes inside cycles)
- `progress/timestamp.rs` additions:
  - `Product<TOuter, TInner>` — nested timestamp type
  - PathSummary for Product timestamps

**Tests**:
- feedback: timestamp advances by summary on each iteration
- connect_loop: data flows through the loop, converges, exits
- enter_scope/exit_scope: timestamp wrapping/unwrapping correct
- Nested iteration: loop terminates when no data is fed back
- **End-to-end**: iterative computation (e.g., multiply until > threshold) — verify convergence and correct results

**Estimated size**: ~2500 lines

---

## PR 13 — Cancellation
**Goal**: Graceful shutdown on cancellation.

- Thread `CancellationToken` through:
  - `execute()` → WorkerExecutor → all operator activations
  - `from_stream` reader tasks
  - Progress tracker task
  - Channel operations (select with cancellation)
- `execute()` returns `Error::Cancelled` when token is triggered
- Operators use `tokio::select!` with `token.cancelled()`

**Tests**:
- Cancel before dataflow starts → immediate return with Cancelled
- Cancel mid-computation → operators exit, channels close, execute returns Cancelled
- Cancel during input stream reading → from_stream stops, pipeline drains
- Partial results available after cancellation
- Double-cancel is safe (idempotent)

**Estimated size**: ~1500 lines

---

## PR 14 — Time-bounded message batching
**Goal**: Reduce scheduling overhead by coalescing messages before operator activation.

- `scheduler/batching.rs`
  - `BatchingPolicy` struct — `max_batch_count`, `max_batch_bytes: Option<usize>`, `max_batch_wait: Duration`
  - `Default` impl: 1024 messages, 64KB, 1ms
  - `BatchingPolicy::no_batching()` — convenience for `max_batch_count: 1`
  - `MessageSize` trait — optional `fn message_size(&self) -> usize`
  - Blanket impls for `String`, `Vec<T>`, `Bytes`, common primitives
  - `BatchAccumulator<D>` — tracks count, byte size (if D: MessageSize), elapsed time since first message
  - `BatchAccumulator::should_dispatch(&self, policy: &BatchingPolicy) -> bool`
- `scheduler/mod.rs` integration:
  - Per-operator input buffer uses `BatchAccumulator` to decide when to schedule activation
  - Timer wheel or per-operator deadline for `max_batch_wait` enforcement
  - On threshold met: cancel timer, enqueue operator activation task
- `execute.rs` / `dataflow/spec.rs`:
  - `DataflowConfig` gains `batching_policy: BatchingPolicy` field
  - Policy is per-dataflow, applied uniformly to all operators in that dataflow
- Conditional compilation: `MessageSize` bound is optional — size threshold ignored when not implemented

**Tests**:
- BatchingPolicy: default values correct
- BatchingPolicy: no_batching sets count=1
- BatchAccumulator: count threshold triggers dispatch
- BatchAccumulator: byte size threshold triggers dispatch (with MessageSize impl)
- BatchAccumulator: time threshold triggers dispatch (simulated clock)
- BatchAccumulator: byte size ignored when D does not impl MessageSize
- BatchAccumulator: first-threshold-wins (whichever fires first)
- MessageSize: blanket impls return reasonable values
- MessageSize: custom impl respected
- Integration: operator receives coalesced messages in one activation
- Integration: max_batch_wait guarantees bounded latency under low throughput
- Integration: batching + backpressure interaction — stalled operator gets natural batching

**Estimated size**: ~1500 lines

---

## PR 15 — Codec trait + serialization
**Goal**: Pluggable serialization for inter-process data exchange.

- `communication/codec.rs`
  - `Codec<T>` trait — `encode(&T, &mut BytesMut)`, `decode(&mut Bytes) -> T`
  - `BincodeCodec<T>` — default implementation (behind `bincode-codec` feature)
- `communication/mod.rs`
  - `Data` trait — `Clone + Send + Sync + 'static`
  - `ExchangeData` trait — `Data` + associated codec
- Feature flags: `bincode-codec` = `["bincode", "serde"]`

**Tests**:
- BincodeCodec: round-trip encode/decode for primitives, Vec, structs, nested types
- Codec error handling: malformed bytes → `Error::Codec`
- Custom codec implementation (e.g., length-prefixed string codec)
- ExchangeData blanket usage

**Estimated size**: ~1200 lines

---

## PR 15 — ConnectionManager + ConnectionPool
**Goal**: Pluggable networking with dynamic connection scaling.

- `communication/connection.rs`
  - `PeerId` — process identity
  - `ConnectionRequest` — peer_id, local_id, request_id
  - `ConnectionManager` trait — `async fn establish(ConnectionRequest) -> Connection`
  - `TcpConnectionManager` — default impl with address map
  - `ConnectionPool<M>` — acquire/release, dynamic scaling (grow under load, shrink when idle), health checks, reconnection
  - `PoolConfig` — min_connections_per_peer, max_connections_per_peer, idle_timeout, health_check_interval, connect_timeout
  - `PoolGuard<C>` — RAII guard that returns connection on drop

**Tests**:
- MockConnectionManager: establish returns mock streams
- ConnectionPool: acquire creates new connection, second acquire reuses returned connection
- Pool: scales up to max_connections_per_peer under concurrent demand
- Pool: scales down — idle connections above min_connections_per_peer are dropped after idle_timeout
- Pool: max_per_peer limit enforced, waits for release when at capacity
- Pool: dead connection detected → re-establish via manager
- Pool: min_connections_per_peer connections are never dropped due to idle timeout
- TcpConnectionManager: integration test with localhost TCP (optional, cfg(test))

**Estimated size**: ~2500 lines

---

## PR 16 — Wire protocol + transport
**Goal**: Multiplexed framed communication over connections.

- `communication/transport.rs`
  - Frame format: `channel_id: u64 | length: u32 | payload: [u8]`
  - `FramedWriter` — writes frames to `AsyncWrite`
  - `FramedReader` — reads frames from `AsyncRead`
  - `Demuxer` — background task that reads frames and dispatches to per-channel `mpsc::Sender`
  - `Muxer` — collects from per-channel senders, writes to connection

**Tests**:
- Frame round-trip: write frame → read frame, all fields preserved
- Multiplexing: interleave frames from 3 channels, demux dispatches correctly
- Large payload: frame > 64KB handled correctly
- Connection drop mid-frame: error propagated cleanly
- Backpressure: slow reader causes writer to await

**Estimated size**: ~2000 lines

---

## PR 17 — Inter-process dataflow
**Goal**: Full multi-process data exchange, progress tracking, sink-first output, and **dataflow isolation**.

- Wire up `exchange` operator to use connections for remote workers:
  - Detect local vs remote workers from `ClusterTopology`
  - Local workers use `mpsc`, remote workers use `ConnectionPool` + `FramedWriter` + `Codec`
- Wire up `broadcast` for cross-process delivery
- Inter-process progress exchange:
  - Progress updates serialized and sent to peer processes
  - Received progress updates fed into local tracker
- `ChannelAllocator` extended to handle remote channels
- **Dataflow isolation on shared connections**:
  - `DataflowId` — wraps `uuid::Uuid`, random v4, universally unique without coordination
  - Frame header: `dataflow_id(UUID, 16B) + channel_id(u64) + payload_len(u32) + payload` = 28 byte header
  - Demuxer routes by `(dataflow_id, channel_id)` tuple
  - Scheduler distinguishes work by `(DataflowId, WorkerId)` for FIFO ordering
  - Frames for unknown/cancelled DataflowIds are logged and dropped
- **OutputSink trait (push-based output)**:
  - `OutputSink<T, D>` trait — `write(event)` + `close()`
  - `.output_to(name, sink)` terminal operator — wires final operator directly to user-provided sink
  - `ChannelSink` — built-in sink that bridges to a bounded `mpsc`, backing the pull-based `OutputStream`
  - `.output()` reimplemented as syntactic sugar: creates `ChannelSink` internally, returns `OutputStream`
  - One sink instance per worker in the final region

**Tests**:
- Two-process simulation using in-memory mock connections
- Exchange routes data to correct remote worker
- Broadcast sends to all remote workers
- Progress: frontier advances across processes
- Connection failure mid-dataflow → Error::Connection propagated
- **Dataflow isolation**: two concurrent dataflows share a connection; frames route correctly by DataflowId
- **Dataflow isolation**: cancelled dataflow's in-flight frames are dropped (not delivered to other dataflows)
- **Dataflow isolation**: DataflowId uniqueness across nodes (no collision with node_index encoding)
- OutputSink: custom sink receives all output events in order
- OutputSink: backpressure from slow sink propagates back through pipeline
- OutputSink: `close()` called on completion
- ChannelSink: OutputStream receives events matching direct sink output
- `.output_to()` wires sink at construction time (one per final-region worker)
- **End-to-end**: multi-process wordcount (simulated)

**Estimated size**: ~3500 lines

---

## PR 17B — Refactor: UUID DataflowId + String node identity + component lifetime docs
**Goal**: Replace `u64` DataflowId with UUID, replace numeric `node_index` with String `node_id`, and document component cardinality/lifetime for all key types.

### Identity changes:
- `DataflowId` → wraps `uuid::Uuid` (random v4). Remove `DataflowIdAllocator`. Just call `DataflowId::new()`.
- `NodeConfig.node_index: usize` → `NodeConfig.node_id: String` (typically IP:port or hostname)
- `ClusterTopology.worker_range(node_index)` → `worker_range(node_id: &str) -> Option<Range>`
- `ClusterTopology.node_for_worker()` → returns `Option<&str>` instead of `usize`
- `MembershipEvent` variants use `node_id: String` instead of `node_index: usize`
- `PlacementPolicy::Pinned { node_id: String }` instead of `node_index`
- Wire protocol header: 16 bytes (UUID) + 8 (channel_id) + 4 (length) = 28 bytes
- Add `uuid` crate dependency

### Component cardinality & lifetime documentation:
Add a "Component Lifecycle" section to DESIGN.md documenting each key type's:
- **Cardinality**: singleton per process, one per dataflow, one per operator, one per connection, etc.
- **Lifetime**: process lifetime, dataflow lifetime, operator activation, connection duration, etc.
- **Ownership**: who creates it, who holds the reference

Key components to document:
| Component | Cardinality | Lifetime |
|---|---|---|
| `WorkerPool` | 1 per process | Process lifetime |
| `ConnectionPool` | 1 per process | Process lifetime |
| `ClusterTopology` | 1 per process (updated on membership changes) | Process lifetime (mutable) |
| `CancellationToken` | 1 per dataflow | Dataflow lifetime |
| `DataflowId` | 1 per dataflow | Dataflow lifetime |
| `DataflowSession` | 1 per dataflow | Dataflow lifetime |
| `ProgressTracker` | 1 per dataflow | Dataflow lifetime |
| `DataflowMetrics` | 1 per dataflow | Dataflow lifetime |
| `RemotePush` | 1 per (dataflow, channel, target node) | Dataflow lifetime |
| `ProgressExchange` | 1 per dataflow | Dataflow lifetime |
| `Demuxer` | 1 per connection | Connection lifetime |
| `Muxer` / `MuxerSender` | 1 per connection / cloned per channel | Connection lifetime |
| `FrameSender` / `FrameReceiver` | 1 per (dataflow, channel, peer) | Dataflow lifetime |
| `Codec` | 1 per dataflow (shared via Arc) | Dataflow lifetime |
| `OperatorState` | 1 per (dataflow, operator, worker) | Dataflow lifetime |
| `InputHandle` / `OutputHandle` | 1 per operator activation | Activation (transient) |

**Tests**: Existing tests continue to pass after refactoring.

**Estimated size**: ~800 lines (mostly refactoring existing code + docs)

---

## PR 18 — Observability + metrics integration
**Goal**: Per-dataflow CPU time tracking, backpressure measurement, and structured tracing.

- Wire up `DataflowMetrics` collection throughout the runtime:
  - Wrap each operator activation with `Instant::now()` timing
  - Accumulate per-operator CPU time, activation counts, records processed
  - Track backpressure per-operator: `BackpressureMetrics` (blocked_count, blocked_duration, max_blocked_duration)
  - When an operator push returns `Error::Backpressure`, record the wait duration until buffer drains
  - Aggregate into `DataflowMetrics` at dataflow completion
  - `execute()` returns `DataflowResult<R>` with result + metrics
- `tracing` integration:
  - Instrument key paths: operator activation, progress updates, connection events, backpressure events
  - Structured fields: worker_id, operator_name, timestamp, backpressure_duration
  - Emit `OperatorMetrics` as tracing events at dataflow completion

**Tests**:
- DataflowMetrics: wall_time is non-zero for a completed dataflow
- DataflowMetrics: total_cpu_time ≤ wall_time × num_workers
- OperatorMetrics: each operator reports activations > 0 and cpu_time > 0
- OperatorMetrics: records_processed matches input count for pass-through operators
- BackpressureMetrics: slow consumer causes upstream blocked_count > 0 and blocked_duration > 0
- BackpressureMetrics: max_blocked_duration ≤ blocked_duration
- BackpressureMetrics: end-to-end chain — backpressure traces from slow op back to input
- Tracing: verify spans emitted for key operations (using `tracing-test`)
- **End-to-end**: run pipeline, inspect returned metrics, validate per-operator breakdown including backpressure

**Estimated size**: ~2500 lines

---

## PR 19 — Checkpoint operator + recovery
**Goal**: Consumer-defined checkpointing with timestamp-based recovery.

- `dataflow/operators/checkpoint.rs`
  - `CheckpointBackend<T, D>` trait — `save()`, `save_frontier()`, `load_frontier()`
  - `checkpoint(backend)` operator — transparent pass-through that persists data/frontier
  - `InMemoryCheckpointBackend` — default in-memory implementation (for testing)
- `dataflow/checkpoint_recovery.rs`
  - `resume_from_checkpoint(input, backend)` — wraps input stream, skips data at/before stored frontier
  - `FilteredInput<T, D>` — filtered input stream implementation

**Tests**:
- CheckpointBackend: InMemoryBackend save/load_frontier round-trip
- Checkpoint operator: data passes through unchanged (transparency)
- Checkpoint operator: save() called for each batch
- Checkpoint operator: save_frontier() called on frontier advance
- Recovery: resume_from_checkpoint skips data ≤ stored frontier
- Recovery: data beyond stored frontier passes through
- Recovery: no stored frontier → all data passes through
- **End-to-end**: run pipeline with checkpoint, "restart" with resume_from_checkpoint, verify no duplicate processing

**Estimated size**: ~2000 lines

---

## PR 20 — Error hardening, examples, docs
**Goal**: Production readiness polish.

- Error handling audit:
  - Verify no `unwrap()`/`expect()` in library code
  - Add context to errors where needed
  - Ensure all error paths are tested
  - Verify `ErrorPolicy::Stop` and `ErrorPolicy::Ignore` work end-to-end
- Error policy end-to-end wiring:
  - `ErrorPolicy::Stop`: operator error → `Envelope::Control(Error)` → pipeline stops → `execute()` returns error
  - `ErrorPolicy::Ignore`: operator error → logged + callback invoked → batch skipped → pipeline continues
  - Skipped error count in `DataflowMetrics`
- Examples:
  - `examples/hello.rs` — minimal pipeline
  - `examples/wordcount.rs` — classic word count with exchange
  - `examples/loop_example.rs` — iterative computation
  - `examples/checkpoint.rs` — checkpointing and recovery
- README.md — quickstart, feature overview, architecture summary
- API documentation pass (doc comments on all public items)

**Tests**:
- Examples compile and run (integration tests or `trybuild`)
- Error policy: Stop halts on first error, Ignore continues
- Error policy: Ignore invokes on_error callback with correct error
- Error policy: skipped_errors count in DataflowMetrics
- Error messages are descriptive and actionable

**Estimated size**: ~3000 lines

---

## PR 21 — Dynamic cluster scaling
**Goal**: Support adding/removing nodes at runtime, driven by the hosting application.

- `ClusterMembership` trait:
  - `subscribe() -> Box<dyn Stream<Item = MembershipEvent> + Send + Unpin>`
  - `MembershipEvent::NodeJoined { node_index, logical_workers, peer_id }`
  - `MembershipEvent::NodeLeft { node_index, reason: NodeDepartureReason }`
  - `NodeDepartureReason`: `Graceful`, `ConnectionLost`, `Removed`
- Runtime membership listener (async task):
  - Consumes `MembershipEvent` stream
  - Updates `ClusterTopology` (atomic swap with Arc)
  - Triggers routing table rebuild for all active dataflows
  - Notifies connection pool to establish/evict connections
- `ClusterTopology` made dynamic:
  - Wrap in `Arc<ArcSwap<ClusterTopology>>` for lock-free reads
  - `RoutingTable` references topology version; rebuilt when stale
- Scale-up handling:
  - New logical worker indices allocated for joining node
  - Future data (new timestamps) routed to expanded worker set
  - In-flight data continues on original routes (consistency)
- Scale-down handling:
  - Mark departed node's logical workers as unavailable
  - Graceful: wait for drain (configurable timeout)
  - Failure: release departed node's capabilities (advance frontier)
  - Apply `ErrorPolicy`: Stop → propagate `Error::NodeLost`, Continue → log + skip
  - Connection pool evicts connections to departed peer
- Default `StaticMembership` implementation (no changes, for single-node / fixed clusters)

**Tests**:
- StaticMembership: no events emitted, topology never changes
- NodeJoined: routing table expands, new worker indices assigned
- NodeLeft (graceful): drain timeout honored, frontier advances
- NodeLeft (failure): capabilities released, error propagated per policy
- Concurrent scale-up + data flow: new timestamps use new routes, old timestamps use old routes
- Scale-down mid-computation: ErrorPolicy::Stop returns NodeLost error
- Scale-down mid-computation: ErrorPolicy::Continue logs + continues
- Connection pool evicts on NodeLeft
- Rapid join+leave: no routing table corruption
- Single-node topology remains unchanged by membership events

**Estimated size**: ~2500 lines

---

## PR 22 — Coordinator integration primitives
**Goal**: Provide the building blocks that host applications need to build a coordinator for distributed dataflow execution.

- `dataflow/handle.rs`
  - `DataflowHandle` — returned when submitting a dataflow; provides `result()`, `cancel()`, `progress_stream()`, `current_frontier()`
  - `DataflowOutcome` enum — `Completed`, `Cancelled`, `Failed`, `Quiescent`, each carrying `progress_frontier` + `metrics`
  - `ProgressUpdate` struct — streams frontier advances with records_processed count
- `dataflow/outcome.rs`
  - `OutcomeAggregator` — collects per-node outcomes, produces `AggregatedOutcome`
  - `AggregatedOutcome` enum — `Completed`, `Failed { failed_nodes }`, `Cancelled`
  - Logic: any node failure → global failure; all cancelled → global cancelled; all completed → global completed
- Executor integration:
  - `DataflowExecutor` emits `ProgressUpdate` when frontier advances
  - `DataflowExecutor::run()` returns `DataflowOutcome` (rich) instead of `Result<bool>`
  - On cancellation: captures `progress_frontier` at the point of cancellation
  - On error: captures `progress_frontier` + failed operator info
- `CancellationToken` distributed coordination support:
  - `cancel_reason()` — why was it cancelled (user request, timeout, node failure)
  - `cancelled_at()` — timestamp of cancellation for diagnostics

**Tests**:
- DataflowHandle: cancel() triggers CancellationToken; result() returns outcome
- DataflowHandle: progress_stream() receives updates when frontier advances
- DataflowOutcome: Completed has empty frontier; Cancelled/Failed have non-empty frontier
- OutcomeAggregator: all-complete → Completed
- OutcomeAggregator: one-failed → Failed with failed_nodes list
- OutcomeAggregator: all-cancelled → Cancelled with global progress (min frontier)
- OutcomeAggregator: mixed cancelled+completed → Cancelled (conservative)
- ProgressUpdate: records_processed accumulates correctly across activations
- End-to-end: run pipeline, cancel midway, verify progress_frontier reflects processed timestamps

**Estimated size**: ~2000 lines

---

## PR 23 — RuntimeHandle & Task Scheduling Policy
**Goal**: Implement the multi-cluster isolation model and pluggable task scheduling.

- `runtime/mod.rs`
  - `RuntimeHandle` — self-contained runtime instance owning worker pool, task queue, connection pool
  - `RuntimeConfig` — configuration struct (pool sizes, schedule policy, tracing subscriber)
  - Multiple `RuntimeHandle` instances coexist with full isolation (no global state)
- `runtime/scheduler.rs`
  - `TaskMeta` — per-task metadata: `dataflow_id`, `priority: u32`, `created_at: Instant`
  - `SchedulePolicy` trait — `fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering`
  - `FifoPolicy` — pure creation-order scheduling
  - `PriorityPolicy` — strict priority (higher wins)
  - `PriorityWithAgingPolicy` — (default) priority + aging to prevent starvation
- `runtime/task_queue.rs`
  - Priority queue backed by BinaryHeap with custom comparator via `SchedulePolicy`
  - `enqueue(task, meta)`, `dequeue() -> Option<Task>` using policy ordering
- Integration:
  - `RuntimeHandle::execute(spec)` assigns dataflow priority → all tasks inherit it
  - Worker threads call `task_queue.dequeue()` to get next task per policy
  - **No static variables** anywhere — verified via `cargo clippy` and grep for `static`/`lazy_static`/`thread_local`

**Tests**:
- Two RuntimeHandles in same process: submit dataflows to each, verify full isolation
- FifoPolicy: tasks dequeued in creation order regardless of priority
- PriorityPolicy: higher priority always dequeued first
- PriorityWithAgingPolicy: high-priority first, but aged low-priority eventually overtakes
- Starvation test: continuous high-priority submissions don't permanently block low-priority
- TaskMeta: created_at is monotonic across enqueues
- No global state: grep for static/lazy_static/thread_local finds zero hits in library code

**Estimated size**: ~1500 lines

---

## Dependency Graph

```
PR1  (core types)
 ↓
PR2  (progress primitives)
 ↓
PR3  (capability + reachability)
 ↓
PR4  (scope, stream, channels + envelope + execution regions)
 ↓
PR5  (worker + execute + dynamic pool + error policy + metrics types)
 ↓
PR6  (input system)
 ↓
PR7  (unary, inspect, probe)
 ├──→ PR8  (binary, concat, delay)
 ↓
PR9  (progress tracker integration)
 ├──→ PR10 (exchange, rebalance, gather, broadcast — with region boundaries)
 ├──→ PR11 (branch + ok_err)
 ├──→ PR12 (loops + nested scopes — no parallelism changes in cycles)
 ↓
PR13 (cancellation) ← depends on PR10-12 being in
 ↓
PR14 (codec + serialization)
 ↓
PR15 (connection manager + pool with dynamic scaling)
 ↓
PR16 (wire protocol + transport)
 ↓
PR17 (inter-process dataflow)
 ↓
PR18 (observability + metrics integration)
 ↓
PR19 (checkpoint operator + recovery)
 ↓
PR20 (polish: errors, error policy wiring, examples, docs)
 ↓
PR21 (dynamic cluster scaling — ClusterMembership, scale-up/down)
 ↓
PR22 (coordinator integration — DataflowHandle, OutcomeAggregator, ProgressUpdate)
 ↓
PR23 (RuntimeHandle + SchedulePolicy — multi-cluster isolation, priority scheduling)
```

## Notes

- **Terminology renames** (Antichain→Frontier, etc.) should be applied before PR1. If approved, the new names will be used from the start, avoiding a rename refactor later.
- **Custom runtime optimization** is deferred — the WorkerExecutor design already minimizes Tokio scheduling overhead. Can revisit with benchmarks after PR20.
- **Operator fusion** is deferred to post-v1.
- PRs 10, 11, 12 can proceed in parallel after PR9.
- PR8 can proceed in parallel with PR9 (only needs PR7).
- **New additions (v2)**: Dynamic worker pool sizing (PR5), message envelope with control signals (PR4), per-dataflow error policy (PR5/PR20), observability/metrics (PR18), delay operator (PR8), checkpointing (PR19).
- **New additions (v3)**: Per-stage dynamic parallelism via execution regions (PR4), `rebalance`/`gather` operators (PR10), no parallelism changes inside cycles (PR12 restriction). Region types are defined in PR4; repartition operators in PR10; progress tracking for regions in PR9.
- **Event-driven executor** — the current `DataflowExecutor::run()` is a single-threaded test/validation helper. A future PR must implement the production event-driven model where the orchestrator event loop (on the I/O runtime) feeds operator activations into the `TaskScheduler` → Worker Thread Pool. The `run()` loop will be replaced by an event-driven `activate_operator()` method called by the orchestrator when data arrives or progress advances. This is prerequisite for multi-dataflow sharing of the Worker Thread Pool.
- **No global state** — the instancy crate must contain zero `static`, `lazy_static`, `once_cell`, or `thread_local!` declarations. All state is owned by `RuntimeHandle` instances. This enables multiple isolated clusters in a single process (§12.6).
- **Pluggable scheduling** — task dequeue order is determined by a `SchedulePolicy` trait per `RuntimeHandle`. Default policy uses priority-with-aging (§12.7).

---

## Completed PRs (Post-Original Plan)

| GH PR | Content |
|-------|---------|
| #30 | DataflowGraph registry |
| #32 | Dataflow builder (BuildContext, build_and_run, source → sink pipelines) |
| #33 | Progress integration + ProbeHandle |
| #34 | RuntimeHandle + SchedulePolicy |
| #35 | End-to-end examples (hello_dataflow, cancellation, runtime_isolation, probe) |

---

## Next PRs (Builder API Completeness)

### Architecture Decision: Separated Builder + Runtime (ADR-001)

**Decision**: Adopt a two-phase design where logical graph construction is fully
separated from physical execution. The old `build_and_run` closure pattern is replaced
with a standalone `DataflowBuilder` that produces a `LogicalDataflow`, which is then
submitted to a `Runtime` for async execution.

**Target API:**
```rust
// Phase 1: Build logical dataflow (pure, no runtime)
let mut builder = DataflowBuilder::<u64>::new("pipeline");
let input = builder.input::<i32>("numbers");
let output = input
    .map("double", |_t, x| x * 2)
    .filter("div_by_3", |_t, x| x % 3 == 0)
    .map("describe", |_t, x| format!("{x}"))
    .output::<String>("results");
let dataflow = builder.build();

// Phase 2: Execute with runtime (async, connects physical I/O)
let runtime = Runtime::new(RuntimeConfig::default());
let mut handle = runtime.spawn(dataflow).await?;

// Phase 3: Feed data and collect results via async streams
handle.input("numbers").send(0u64, vec![1,2,3,4,5]).await?;
handle.input("numbers").close().await?;
while let Some((time, batch)) = handle.output("results").recv().await {
    println!("t={time}: {batch:?}");
}
```

**Benefits:**
- Type-safe stream chaining (compile-time connection validation)
- Logical graph is inspectable/testable without a runtime
- Same `LogicalDataflow` can run on different runtimes/configs
- Async-native I/O (no closure wrapping)
- Clean separation: graph construction ≠ execution ≠ I/O

---

### PR 25 — DataflowBuilder + Stream chaining + Materializer

**Goal**: Replace closure-based `build_and_run` with separated builder/runtime.

**Phase A — DataflowBuilder + Stream:**
- `DataflowBuilder<T>`: allocates operators, tracks edges, stores factories
- `Stream<'a, T, D>`: typed handle borrowing the builder, provides chaining:
  - `.map(name, FnMut(T, D) -> D2)` → returns `Stream<T, D2>`
  - `.filter(name, FnMut(&T, &D) -> bool)` → returns `Stream<T, D>`
  - `.unary(name, FnMut(InputHandle, OutputHandle) -> Result<()>)` → general unary
  - `.output(name)` → declares logical output port
- `builder.input::<D>(name)` → declares logical input, returns `Stream<T, D>`
- `builder.build()` → produces `LogicalDataflow<T>`

**Phase B — Materializer:**
- `LogicalDataflow::materialize(config, cancel) -> DataflowExecutor`
- Extract existing materialization logic from `build_and_run_with_cancel`
- Operator/channel factories remain internal to LogicalDataflow

**Phase C — Async Runtime integration:**
- `Runtime::spawn(dataflow) -> DataflowHandle`
- `DataflowHandle.input(name) -> InputSender<T, D>` (async mpsc)
- `DataflowHandle.output(name) -> OutputReceiver<T, D>` (async mpsc)
- `DataflowHandle.cancel()`, `.await` (completion)

**Phase D — Cleanup:**
- Remove old `build_and_run` / `BuildContext` (or deprecate)
- Update all examples to new API

### PR 26 — Binary operator + concat via Stream
**Goal**: Multi-input operators in the new Stream API.

- `stream1.binary(stream2, name, logic)` → merges two streams
- `Stream::concat([s1, s2, s3])` → merge multiple streams
- Tests: join-like logic, merging streams

### PR 26b — Rename Stream → Pipe, DataStream → StreamEdge
**Goal**: Clarify naming to avoid confusion between builder-time handle and logical graph edge.

**Problem**: `Stream<T, D>` (dataflow_builder.rs) and `DataStream<S, D>` (stream.rs) have overlapping
names but completely different responsibilities. This causes confusion.

**Renames**:
| Current | New | Responsibility |
|---|---|---|
| `Stream<T, D>` | `Pipe<T, D>` | Builder-time fluent API handle. Holds shared reference to builder state; used to chain `.map()`, `.filter()`, `.binary()`, `.output()` during dataflow construction. Not the data itself — a construction-time pipe you extend. |
| `DataStream<S, D>` | `StreamEdge<S, D>` | Logical typed edge in the dataflow graph. Describes where data originates (scope, source slot, region). Used by the graph/scope layer for operator wiring and connection tracking. |

**Changes**:
- Rename `Stream` → `Pipe` in `dataflow_builder.rs` (struct + all method impls + tests + examples)
- Rename `DataStream` → `StreamEdge` in `stream.rs` (struct + impls + usages)
- Add doc-comments explaining the conceptual role of each struct
- Update `StreamConnection` and `StreamTarget` names if needed for consistency
- Update all examples and tests

### PR 27 — Feedback/loop operator via Stream
**Goal**: Iterative computation in the new Stream API.

- `builder.loop_scope(|scope| { ... })` → nested timestamp scope
- `stream.connect_loop(handle)` → feedback edge
- Product timestamps for nested scope
- Tests: iterative convergence

### PR 28 — Wire RuntimeHandle to Runtime
**Goal**: Full runtime integration with SchedulePolicy.

- Integrate SchedulePolicy into worker dispatch
- RuntimeHandle wraps Runtime for lifecycle management
- DataflowHandle for completion/cancellation
- Tests: runtime executes dataflow end-to-end

---

## Future: Migrate timely-dataflow Examples to instancy

Once the Stream chaining API supports unary, binary, loop, and exchange operators,
migrate the key timely-dataflow examples to demonstrate equivalent functionality:

| timely example | instancy equivalent | Requires |
|---|---|---|
| `hello.rs` | ✅ Done (`hello_dataflow.rs`) | — |
| `simple.rs` | ✅ Done (`simple_pipeline.rs`, `branching_pipeline.rs`) | — |
| `loopdemo.rs` | `loop_demo.rs` — iterative computation | PR 27 |
| `hashjoin.rs` | `hash_join.rs` — binary join pattern | PR 26 |
| `exchange.rs` | `exchange.rs` — data repartitioning | exchange in builder |
| `barrier.rs` | `barrier.rs` — synchronization barrier | PR 27 |
| `bfs.rs` | `bfs.rs` — breadth-first search (graph algo) | PR 26 + PR 27 |
| `pagerank.rs` | `pagerank.rs` — iterative graph algorithm | PR 26 + PR 27 |
| `distinct.rs` | ✅ Done (`distinct.rs`) — stateful deduplication | — |
| `flow_controlled.rs` | `flow_controlled.rs` — backpressure demo | backpressure API |
| `pingpong.rs` | `pingpong.rs` — multi-process messaging | PR 28 + networking |
| `event_driven.rs` | ✅ Done (`event_driven.rs`) — external input events | — |
| `capture_send/recv` | `capture.rs` — stream capture/replay | serialization |
| — | ✅ Done (`wordcount.rs`) — streaming word count | — |
| — | ✅ Done (`spawn_pipeline.rs`) — channel I/O demo | — |
| — | ✅ Done (`cancellation.rs`) — cooperative shutdown | — |
| — | ✅ Done (`probe.rs`) — progress tracking | — |

**Priority order:** simple → loopdemo → hashjoin → exchange → bfs → pagerank
(Others are lower priority and can be added incrementally.)
