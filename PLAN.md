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
**Goal**: Define the dataflow graph abstractions and local communication.

- `dataflow/scope.rs`
  - `Scope` trait — `name()`, `addr()`, `add_operator()`, `allocate_operator_index()`, timestamp type
  - `ChildScope<T>` — implementation for nested scopes
- `dataflow/stream.rs`
  - `Stream<S: Scope, C>` — a named edge in the dataflow graph; connects output port to downstream operators
- `dataflow/channels/mod.rs`
  - `Push<T>` / `Pull<T>` traits — channel send/receive abstractions
  - `Message<T, C>` — timestamped container
- `dataflow/channels/pact.rs`
  - `PartitionStrategy` enum/trait — `Pipeline` (no shuffle), `Exchange(fn)` (hash routing), `Broadcast`, `BroadcastLocal`
- `communication/allocator.rs`
  - `ChannelAllocator` — creates local `mpsc`-based channel pairs for intra-process communication
  - Bounded channels with configurable buffer size

**Tests**:
- Scope: operator registration, index allocation, address hierarchy
- Stream: connects source to target, metadata propagation
- Push/Pull: send batch through local channel, receive in order, backpressure when full
- PartitionStrategy: Pipeline routes to same worker, Exchange routes by hash, Broadcast fans out
- ChannelAllocator: allocate N channels, each pair is independent

**Estimated size**: ~2000 lines

---

## PR 5 — Worker executor + execution engine
**Goal**: Implement the logical worker model and runtime bootstrap.

- `worker.rs`
  - `WorkerId(usize)` — globally unique logical worker identity
  - `WorkerExecutor` — FIFO task queue + concurrency semaphore; `run()` loop
  - `OperatorActivation` — queued work item for an operator
- `execute.rs`
  - `RuntimeConfig` — optional Tokio runtime handle, progress mode
  - `DataflowConfig` — cluster topology, cancellation token
  - `ClusterTopology` — `nodes: Vec<NodeConfig>`, `total_workers()`, `worker_range()`, `node_for_worker()`
  - `NodeConfig` — `node_index`, `workers`
  - `execute()` — async entry point; creates WorkerExecutors, runs them on the runtime

**Tests**:
- WorkerExecutor: tasks execute in FIFO order within a worker
- WorkerExecutor: concurrency semaphore limits parallel activations to N
- ClusterTopology: `total_workers()` sums correctly, `worker_range()` returns correct ranges, `node_for_worker()` maps back correctly
- ClusterTopology: heterogeneous configs (4, 1, 8 workers)
- execute(): basic smoke test — empty dataflow starts and completes
- Runtime isolation: passing an external runtime handle works

**Estimated size**: ~2000 lines

---

## PR 6 — Input system (from_stream + DataflowSpec)
**Goal**: Implement stream-driven input binding.

- `dataflow/operators/from_stream.rs`
  - `InputEvent<T, D>` enum — `Data(T, Vec<D>)`, `Frontier(T)`
  - `TimestampedInput<T, D>` trait (blanket impl for any matching `Stream`)
  - `from_stream` operator — spawns reader task, manages capabilities, posts to worker queue
- `dataflow/spec.rs`
  - `DataflowSpec<T, R>` — builder for binding inputs + graph definition
  - `DataflowInputs<T>` — accessor for named input streams inside the builder closure
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
- Backpressure: from_stream respects channel bounds

**Estimated size**: ~2000 lines

---

## PR 7 — Operators: unary, inspect, probe
**Goal**: First working operator pipeline. End-to-end test.

- `dataflow/operators/unary.rs`
  - `unary()` — sync closure variant
  - `unary_async()` — async closure variant
  - Registers operator with scope, wires input/output channels
- `dataflow/operators/inspect.rs`
  - `inspect()` — side-effect observation, passes data through unchanged
- `dataflow/operators/probe.rs`
  - `ProbeHandle<T>` — `less_than()`, `less_equal()`, `async_wait_for()`, `frontier_watch()`
  - `probe()` on Stream — attaches a probe to observe frontier
- `dataflow/operators/mod.rs` — trait impls on `Stream` for operator chaining

**Tests**:
- unary: identity pass-through, stateful accumulation, error propagation from closure
- unary_async: async closure with `.await` inside
- inspect: callback receives all data, output stream equals input stream
- probe: `less_than` reflects frontier, `async_wait_for` resolves when frontier advances past target
- **End-to-end**: `from_stream → unary(double) → inspect(collect) → probe` — verify results and completion

**Estimated size**: ~2500 lines

---

## PR 8 — Operators: binary, concat
**Goal**: Multi-input operators.

- `dataflow/operators/binary.rs`
  - `binary()` — two inputs, one output, sync closure
  - `binary_async()` — async variant
- `dataflow/operators/concat.rs`
  - `concat()` — merge multiple streams into one, preserving timestamps

**Tests**:
- binary: join-like logic (match items from two streams by timestamp)
- binary: one input finishes before the other — correct completion
- concat: N streams merged, all data present, timestamps preserved
- concat: empty streams handled correctly
- **End-to-end**: two `from_stream` inputs → `binary` → `inspect` → `probe`

**Estimated size**: ~1500 lines

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

## PR 10 — Exchange operator (intra-process) + broadcast
**Goal**: Data redistribution across local workers.

- `dataflow/operators/exchange.rs`
  - `exchange(route_fn)` — repartitions data across all workers by hash
  - Uses `PartitionStrategy::Exchange` with `ChannelAllocator` for local workers
- `dataflow/operators/broadcast.rs`
  - `broadcast()` — sends each record to all workers (cross-process when networked)
  - `broadcast_local()` — sends each record to all workers in the same process only
  - Uses `PartitionStrategy::Broadcast` / `BroadcastLocal`

**Tests**:
- exchange: items routed to correct worker by hash, multi-worker single-process
- exchange: all data accounted for (no loss, no duplication)
- broadcast: every worker receives every item (single-process for now)
- broadcast_local: every local worker receives every item
- **End-to-end**: `from_stream → exchange → unary(count per worker) → probe`

**Estimated size**: ~2000 lines

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
**Goal**: Pluggable networking with connection reuse.

- `communication/connection.rs`
  - `PeerId` — process identity
  - `ConnectionRequest` — peer_id, local_id, request_id
  - `ConnectionManager` trait — `async fn establish(ConnectionRequest) -> Connection`
  - `TcpConnectionManager` — default impl with address map
  - `ConnectionPool<M>` — acquire/release, health checks, idle cleanup, reconnection
  - `PoolConfig` — max_per_peer, idle_timeout, health_check_interval, connect_timeout
  - `PoolGuard<C>` — RAII guard that returns connection on drop

**Tests**:
- MockConnectionManager: establish returns mock streams
- ConnectionPool: acquire creates new connection, second acquire reuses returned connection
- Pool: max_per_peer limit enforced, waits for release
- Pool: dead connection detected → re-establish
- Pool: idle timeout → connection dropped
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

## PR 18 — Error hardening, tracing, examples, docs
**Goal**: Production readiness polish.

- Error handling audit:
  - Verify no `unwrap()`/`expect()` in library code
  - Add context to errors where needed
  - Ensure all error paths are tested
- `tracing` integration:
  - Instrument key paths: operator activation, progress updates, connection events
  - Structured fields: worker_id, operator_name, timestamp
- Examples:
  - `examples/hello.rs` — minimal pipeline
  - `examples/wordcount.rs` — classic word count with exchange
  - `examples/loop_example.rs` — iterative computation
- README.md — quickstart, feature overview, architecture summary
- API documentation pass (doc comments on all public items)

**Tests**:
- Examples compile and run (integration tests or `trybuild`)
- Tracing: verify spans emitted for key operations (using `tracing-test`)
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
PR4  (scope, stream, channels)
 ↓
PR5  (worker + execute)
 ↓
PR6  (input system)
 ↓
PR7  (unary, inspect, probe)
 ├──→ PR8  (binary, concat)
 ↓
PR9  (progress tracker integration)
 ├──→ PR10 (exchange + broadcast)
 ├──→ PR11 (branch + ok_err)
 ├──→ PR12 (loops + nested scopes)
 ↓
PR13 (cancellation) ← depends on PR10-12 being in
 ↓
PR14 (codec + serialization)
 ↓
PR15 (connection manager + pool)
 ↓
PR16 (wire protocol + transport)
 ↓
PR17 (inter-process dataflow)
 ↓
PR18 (polish: errors, tracing, examples, docs)
```

## Notes

- **Terminology renames** (Antichain→Frontier, etc.) should be applied before PR1. If approved, the new names will be used from the start, avoiding a rename refactor later.
- **Custom runtime optimization** is deferred — the WorkerExecutor design already minimizes Tokio scheduling overhead. Can revisit with benchmarks after PR18.
- **Operator fusion** is deferred to post-v1.
- PRs 10, 11, 12 can proceed in parallel after PR9.
- PR8 can proceed in parallel with PR9 (only needs PR7).
