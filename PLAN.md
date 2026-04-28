# async-timely Development Plan

## Approach
Contract-and-test-driven development. Each PR defines traits/interfaces first, then implements them, accompanied by comprehensive tests. PRs are ordered by dependency and sized under 5000 lines (including tests). Terminology renames (pending approval) will be applied via a dedicated PR early in the process.

---

## PR 1 — Workspace scaffold + core types
**Goal**: Establish workspace structure, error types, `PartialOrder`, `Timestamp`, `PathSummary`.

- Workspace `Cargo.toml` (workspace members, shared dependencies)
- Crate `async-timely/Cargo.toml` with initial dependencies
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

## PR 14 — Codec trait + serialization
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
**Goal**: Full multi-process data exchange and progress tracking.

- Wire up `exchange` operator to use connections for remote workers:
  - Detect local vs remote workers from `ClusterTopology`
  - Local workers use `mpsc`, remote workers use `ConnectionPool` + `FramedWriter` + `Codec`
- Wire up `broadcast` for cross-process delivery
- Inter-process progress exchange:
  - Progress updates serialized and sent to peer processes
  - Received progress updates fed into local tracker
- `ChannelAllocator` extended to handle remote channels

**Tests**:
- Two-process simulation using in-memory mock connections
- Exchange routes data to correct remote worker
- Broadcast sends to all remote workers
- Progress: frontier advances across processes
- Connection failure mid-dataflow → Error::Connection propagated
- **End-to-end**: multi-process wordcount (simulated)

**Estimated size**: ~3500 lines

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
```

## Notes

- **Terminology renames** (Antichain→Frontier, etc.) should be applied before PR1. If approved, the new names will be used from the start, avoiding a rename refactor later.
- **Custom runtime optimization** is deferred — the WorkerExecutor design already minimizes Tokio scheduling overhead. Can revisit with benchmarks after PR20.
- **Operator fusion** is deferred to post-v1.
- PRs 10, 11, 12 can proceed in parallel after PR9.
- PR8 can proceed in parallel with PR9 (only needs PR7).
- **New additions (v2)**: Dynamic worker pool sizing (PR5), message envelope with control signals (PR4), per-dataflow error policy (PR5/PR20), observability/metrics (PR18), delay operator (PR8), checkpointing (PR19).
- **New additions (v3)**: Per-stage dynamic parallelism via execution regions (PR4), `rebalance`/`gather` operators (PR10), no parallelism changes inside cycles (PR12 restriction). Region types are defined in PR4; repartition operators in PR10; progress tracking for regions in PR9.
