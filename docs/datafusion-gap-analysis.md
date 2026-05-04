# Gap Analysis: instancy Improvements for DataFusion Integration

This document analyzes the `ms-datafusion-timely` crate — which bridges Apache DataFusion
(SQL engine) with timely-dataflow (distributed dataflow) — to identify gaps in `instancy`
that should be addressed to make a future `datafusion-instancy` crate easier to build.

## Background

`ms-datafusion-timely` converts DataFusion logical plans into timely-dataflow operators,
enabling distributed SQL execution across multiple workers and nodes. The crate works but
fights timely-dataflow's design at every turn:

- **Async/sync impedance**: timely is sync-push; DataFusion sources are async-pull
- **Error propagation**: timely has no error channel; errors must be wrapped in data
- **Serialization**: `DataFusionError` and `RecordBatch` aren't serializable; wrapper types needed
- **Panic handling**: timely panics on internal errors; the crate uses `catch_unwind` + `AssertUnwindSafe`
- **Context threading**: 13+ fields threaded through every operator closure via captures

The crate has ~190 tests, 14 operator implementations, and a 48-item improvement roadmap.

---

## 1. Async/Sync Bridge — The Biggest Pain Point

### The Problem

timely-dataflow's `execute()` is a **blocking** function that owns worker threads. DataFusion
data sources are async (returning `Stream<RecordBatch>`). The current crate bridges this with:

```
tokio::task::spawn_blocking(move || {
    timely::execute(config, move |worker| {
        // sync worker thread
        worker.dataflow(|scope| { ... });
        // manually step worker in a loop
    })
});
```

Each table provider gets a separate async reader task connected to the worker via `mpsc` channels.
For 2 tables × 3 workers = 6 async reader tasks, each with a request/response channel pair. The
`PositionSynchronizer` uses `Mutex + Condvar` to throttle workers — blocking a sync thread while
waiting for an async task to catch up. This is fragile and has caused deadlocks.

### What instancy Already Provides

- **Async worker pool**: Workers run as async tasks on Tokio — no `spawn_blocking` needed
- **Channel-based I/O**: `InputSender` / `OutputReceiver` for feeding data in and collecting results
- **`AsyncInputSender` / `AsyncOutputReceiver`**: Native async variants (with `async-io` feature)

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G1.1: Async data source integration** | The current `InputSender::send(timestamp, data)` is synchronous. A DataFusion integration needs to drive async `RecordBatchStream` sources directly into the dataflow without an intermediate channel bridge. Consider adding a `source_async` operator or an async-aware source builder that accepts `impl Stream<Item = Result<RecordBatch>>` and handles backpressure natively. | **High** |
| **G1.2: Backpressure signaling** | instancy's internal channel capacity is hardcoded to 1024. DataFusion sources can produce batches faster than operators consume them. Need configurable per-edge capacity and/or backpressure feedback so sources can pause when downstream is saturated. The `TODO: make configurable per edge` in `executor.rs` confirms this is known. | **High** |
| **G1.3: Position/progress notification** | `ms-datafusion-timely` uses `PositionSynchronizer` (Mutex+Condvar) to let the caller incrementally advance the "max timestamp" the dataflow should process. instancy should expose frontier progress callbacks or a `ProbeHandle`-equivalent that the caller can await asynchronously, avoiding the sync blocking pattern. | **Medium** |

---

## 2. Error Handling & Propagation

### The Problem

timely operators cannot return `Result` — they are infallible closures. `ms-datafusion-timely`
works around this by making the data type itself carry errors:

```rust
// Data flowing through every operator:
type ProcessResult = Result<(SerdeWrapper<RecordBatch>, DataflowControl), Box<ProcessError>>;
```

Every single operator must:
1. Check if input is `Ok` or `Err`
2. If `Err`, relay it downstream unchanged
3. If `Ok`, process data, and on failure, write error to metadata sink AND send `Err` downstream
4. Aggregate errors across workers via broadcast → inspect → cancel

This is ~20 lines of boilerplate per operator. The `ProcessError` struct wraps `DataFusionError`
because `DataFusionError` isn't serializable — it re-creates a simplified serializable error with
`message`, `code`, `plan_id`, `timestamp`, `worker_index`.

### What instancy Already Provides

- **`Result<T, Error>` throughout**: Operators return `Result<(), Error>`
- **`CancellationToken` + `CancellationReason`**: Cooperative cancellation with diagnostics
- **Proper error propagation**: Errors don't need to be encoded in the data stream

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G2.1: Per-operator error context** | When an instancy operator returns `Err`, the error should automatically capture which operator failed (name/id), at which timestamp, on which worker. Currently the `Error` type is generic. A `with_operator_context(name, timestamp)` builder or automatic injection would eliminate the manual `ProcessError` wrapper pattern. | **High** |
| **G2.2: Error-as-data relay pattern** | Some use cases (streaming SQL) want non-fatal errors: skip bad rows but continue processing. instancy should support an `error_output` side-channel on operators, or a `branch_result` combinator that splits `Result<D, E>` into two streams. This would replace the `Result<batch, error>` encoding in the data stream. | **Medium** |
| **G2.3: Cross-worker error broadcast** | When one worker hits a fatal error, all workers need to learn about it. `ms-datafusion-timely` builds a `ControlResult` aggregate → broadcast → inspect pipeline after every sink. instancy should provide a built-in mechanism for cross-worker error/control propagation — e.g., a shared `ErrorBroadcast` channel that triggers `CancellationToken` on all workers when any worker posts an error. | **High** |
| **G2.4: Panic recovery in operators** | `ms-datafusion-timely` wraps operator logic with `catch_unwind(AssertUnwindSafe(...))` because external UDFs can panic. instancy operators should optionally catch panics and convert them to errors, with a configuration flag per-operator or per-dataflow. | **Medium** |

---

## 3. Data Type & Serialization

### The Problem

timely requires data types to implement `timely::Data` (which is `Clone + Send + 'static` +
`Abomonation`). `RecordBatch` doesn't implement `Abomonation`, so `ms-datafusion-timely` wraps
it in `SerdeWrapper<RecordBatch>` that serializes via Arrow IPC (FileWriter/FileReader). This adds
~10KB overhead per batch and goes through a full serialize/deserialize cycle even for in-process
exchange.

```rust
// Every batch is wrapped:
SerdeWrapper<RecordBatch>  // implements Serialize + Deserialize via Arrow IPC
```

The `ProcessResult` type bundles data + control info, adding more serialization overhead:
```rust
Result<(SerdeWrapper<RecordBatch>, DataflowControl), Box<ProcessError>>
```

### What instancy Already Provides

- **Pluggable `Codec` trait**: Custom serialization per type
- **`ExchangeData` trait**: Types participating in cross-worker exchange
- **Zero-copy for in-process**: Local exchange doesn't serialize

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G3.1: Zero-copy in-process exchange** | Verify and document that instancy's in-process exchange (Pipeline pact) truly avoids serialization. `ms-datafusion-timely` always wraps in `SerdeWrapper` even for same-process workers. If instancy can guarantee zero-copy for `Pipeline` edges and only serialize for `Exchange` edges crossing workers, this eliminates massive overhead. | **High** |
| **G3.2: Arrow IPC codec** | Provide a built-in or extension `ArrowIpcCodec` that implements `Codec<RecordBatch>` using Arrow IPC with optional LZ4/ZSTD compression. This is the most common cross-worker data type for DataFusion integration. Consider it for an `arrow-codec` feature. | **Medium** |
| **G3.3: Sidecar metadata on data** | `ms-datafusion-timely` bundles `DataflowControl` alongside every `RecordBatch`. instancy should support typed metadata/tags attached to data items without wrapping — e.g., a `TaggedStream<D, M>` where `M` is control metadata that flows alongside `D` but can be aggregated separately. | **Low** |

---

## 4. Operator Patterns & Context Threading

### The Problem

Every `ms-datafusion-timely` operator receives a `TimelyProcessingContext` with 13+ fields:

```rust
struct TimelyProcessingContext {
    scope, session_state, random_state, reader_clients, config,
    process_index, worker_index, worker_count, timestamps,
    metadata_sink, table_senders, stage_one_probe,
}
```

Each operator's `execute()` clones or captures the fields it needs into closures. The pattern is:
```rust
fn execute(&self, ctx: &mut TimelyProcessingContext) -> Result<Stream> {
    let plan_id = self.id().clone();
    let predicate = self.predicate.clone();
    let metadata_writer = ctx.metadata_writer(Some(plan_id.clone()));
    
    input_stream
        .write_input_metrics(&metadata_writer)
        .map_metadata(&metadata_writer, move |t, result| {
            match result {
                Ok((batch, control)) => { /* process */ },
                Err(e) => Err(e),  // relay error
            }
        })
        .write_output_metrics(&metadata_writer)
}
```

Every operator has the same error-relay + metrics bookkeeping boilerplate.

### What instancy Already Provides

- **`WorkerContext`**: Threaded through operator materialization
- **`unary` / `binary` with closures**: Operator logic as closures
- **`unary_notify`**: Frontier-based notifications

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G4.1: Operator-level context injection** | Allow users to attach a custom context type to the dataflow that is automatically available inside every operator closure — without manual capture. Something like `builder.with_context::<MyCtx>(ctx)` and then `|input, output, ctx: &MyCtx|` in operator closures. This eliminates the 13-field manual threading. | **High** |
| **G4.2: Error-relay combinator** | Provide a built-in `.map_ok(|t, data| ...)` that automatically relays errors and only invokes the closure on `Ok` values. This eliminates the per-operator `match result { Ok => process, Err => relay }` pattern. Or better, handle this at the framework level so operators only ever see valid data. | **High** |
| **G4.3: Metrics/observability hooks** | Built-in per-operator input/output batch counting and processing time measurement, togglable via configuration. This replaces the manual `.write_input_metrics()` / `.write_output_metrics()` / `.begin_write_process_time_metric()` calls that wrap every operator. | **Medium** |

---

## 5. Distributed Coordination & Control Flow

### The Problem

`ms-datafusion-timely` implements a multi-stage control propagation pattern after the data sink:

```
data_sink → aggregate_control → broadcast_control → inspect_control
```

This aggregates `ControlResult` (errors + `DataflowControlRequest` like `BranchTermination`)
across all workers per timestamp, then broadcasts back so every worker can react. This is needed
for:

1. **Error propagation**: One worker's error should cancel all workers
2. **LIMIT termination**: When one worker hits the row limit, all should stop that branch
3. **Cancellation**: External cancel request should propagate to all workers

The `WorkerStepMode` enum controls how aggressively workers process data:
- `AfterLoading`: Step after each timestamp is loaded (default)
- `UntilStageOne`: Step until within-partition processing is done before loading next timestamp
- `LoadDropTo(T)`: Load until a specific timestamp, then drop remaining

### What instancy Already Provides

- **`CancellationToken`**: Cooperative cancellation across all workers
- **`CancellationReason`**: Diagnostics for why cancellation occurred
- **Frontier tracking**: Via `probe` operator

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G5.1: Cross-worker control channel** | A built-in broadcast channel for control messages (not data). When any worker posts a control message (error, limit reached, cancel), all workers receive it. This replaces the manual `aggregate → broadcast → inspect` pipeline. | **High** |
| **G5.2: Worker stepping control** | Expose a way for the caller to control how much progress each worker makes per "step" — analogous to `WorkerStepMode`. This is critical for incremental processing where the caller wants to process one timestamp at a time and check results before proceeding. | **Medium** |
| **G5.3: Branch termination** | Support for early termination of a branch within a dataflow without cancelling the entire dataflow. The `LIMIT` operator needs to signal "stop sending data down this path" while other branches continue. This could be a `terminate_branch(branch_id)` on the cancellation token or a separate mechanism. | **Medium** |

---

## 6. DataFusion-Specific Patterns

### The Problem

Several patterns in `ms-datafusion-timely` are DataFusion-specific but reveal general needs:

1. **Timestamp column extraction**: DataFusion tables have a "timestamp column" that maps to
   timely timestamps. The `delay` operator shifts rows to their correct timestamp based on
   column values. This is a general "reclassify data by time" pattern.

2. **Partition-aware planning**: DataFusion's `RepartitionExec` maps to timely's `exchange`.
   But the planner has no way to know if a stream is already partitioned by the required keys,
   so it conservatively adds repartitions. A partition/order tracking framework would help.

3. **Two-phase execution**: Planning (sync, cheap) then processing (distributed, expensive).
   instancy's `DataflowBuilder` + `RuntimeHandle::spawn_multi` already supports this pattern well.

### Gaps to Address

| Gap | Description | Priority |
|-----|-------------|----------|
| **G6.1: Timestamp extraction operator** | A built-in `delay_by` or `restamp` operator that reassigns the timestamp of each data item based on a user function `|data| -> Timestamp`. This is the `delay` operator pattern from `ms-datafusion-timely` — extremely common for any integration where the logical time comes from the data itself rather than the input sequence. | **Medium** |
| **G6.2: Stream property annotations** | Allow streams to carry metadata about their properties — e.g., "this stream is hash-partitioned by columns [a, b]" or "this stream is sorted by column c". The planner can then skip redundant exchanges. This is the partition/order tracking design from `docs/partition-order-tracking.md`. | **Low** |

---

## Priority Summary

### Must-Have for DataFusion Integration (High Priority)

| ID | Gap | instancy Impact |
|----|-----|-----------------|
| G1.1 | Async data source integration | Eliminates `spawn_blocking` + channel bridge entirely |
| G1.2 | Configurable backpressure | Prevents OOM and deadlocks |
| G2.1 | Per-operator error context | Eliminates `ProcessError` wrapper pattern |
| G2.3 | Cross-worker error broadcast | Eliminates manual aggregate→broadcast→inspect pipeline |
| G3.1 | Zero-copy in-process exchange | Eliminates `SerdeWrapper` overhead for same-process |
| G4.1 | Operator-level context injection | Eliminates 13-field manual context threading |
| G4.2 | Error-relay combinator | Eliminates per-operator error match boilerplate |
| G5.1 | Cross-worker control channel | Eliminates manual control propagation pipeline |

### Should-Have (Medium Priority)

| ID | Gap | instancy Impact |
|----|-----|-----------------|
| G1.3 | Progress notification callbacks | Async frontier observation for callers |
| G2.2 | Error-as-data side channel | Supports non-fatal error handling for streaming |
| G2.4 | Panic recovery in operators | Safety for external UDF integration |
| G3.2 | Arrow IPC codec | Ready-made serialization for RecordBatch exchange |
| G4.3 | Metrics/observability hooks | Built-in per-operator metrics without manual wrapping |
| G5.2 | Worker stepping control | Incremental processing support |
| G5.3 | Branch termination | LIMIT without full dataflow cancel |
| G6.1 | Timestamp extraction operator | Common pattern for data-driven timestamps |

### Nice-to-Have (Low Priority)

| ID | Gap | instancy Impact |
|----|-----|-----------------|
| G3.3 | Sidecar metadata on data | Clean separation of data and control |
| G6.2 | Stream property annotations | Smarter planning, fewer exchanges |

---

## Appendix: Key Files in ms-datafusion-timely

| File | Lines | Purpose | Key Pain Point |
|------|-------|---------|---------------|
| `executor.rs` | ~900 | Main executor, worker orchestration | `spawn_blocking` + Mutex+Condvar sync |
| `executor_federated.rs` | ~800 | Multi-source federation | Duplicates executor.rs patterns |
| `stream.rs` | ~500 | Async/sync channel bridge | Hardcoded buffer sizes, manual position sync |
| `timely_operators.rs` | ~210 | Metric-aware operator wrappers | Conditional metric overhead on every operator |
| `data/results.rs` | ~245 | ProcessResult + ControlResult | Error-in-data pattern, IPC serialization |
| `data/errors.rs` | ~158 | ProcessError (serializable error) | Manual error code mapping from DataFusionError |
| `data/wrapper.rs` | ~46 | SerdeWrapper for RecordBatch | Arrow IPC overhead even for in-process |
| `plans/mod.rs` | ~236 | TimelyPhysicalPlan trait + context | 13-field context struct |
| `plans/repartition.rs` | ~350 | Hash repartition via exchange | Error swallowing in hash computation |
| `plans/join.rs` | ~300 | Inner hash join | Single join type, timestamp semantics |
| `plans/sort.rs` | ~450 | Global sort via exchange + merge | Recently optimized (P1-6, P1-6b) |
| `plans/window.rs` | ~400 | Window functions | Unnecessary repartition, double materialization |
| `metadata.rs` | ~350 | Observability (events, traces, metrics) | Incomplete error path coverage |
| `docs/introduction.md` | ~540 | Architecture documentation | Documents all pain points explicitly |
| `improvement.md` | ~3000 | 48-item improvement roadmap | Comprehensive but many items blocked by timely |
