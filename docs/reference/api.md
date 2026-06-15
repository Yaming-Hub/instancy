# API Reference

This reference summarizes the public API exported from `instancy::lib` and the public modules under `instancy::communication`, `instancy::execute`, `instancy::order`, and `instancy::progress`.

[Back to the guide index](../guide/README.md)

> For tutorials and runnable walkthroughs, start with the [guide](../guide/README.md). For copy-paste patterns, see the [cookbook](../cookbook.md).

## Runtime

### `RuntimeHandle`

Production runtime for spawning single-worker, multi-worker, and distributed dataflows.

Key methods:

- `pub fn new(config: RuntimeConfig) -> Result<Self>` — create an isolated runtime and worker pool.
- `pub fn cancel_token(&self) -> &CancellationToken` — get the runtime-wide cooperative cancellation token.
- `pub fn tokio_handle(&self) -> &tokio::runtime::Handle` — reuse instancy's Tokio runtime for async support code.
- `pub fn shutdown(&self)` — cancel all running dataflows with `CancellationReason::RuntimeShutdown`.
- `pub async fn shutdown_async(&self)` — cancel all running dataflows and wait for them to finish.
- `pub async fn wait_idle(&self)` — wait for all active dataflows to complete without cancelling them.
- `pub fn active_dataflows(&self) -> usize` — count currently active dataflows.
- `pub fn name(&self) -> &str` — get the runtime name.
- `pub fn worker_pool(&self) -> &WorkerPool` — access the shared worker pool.
- `pub fn is_shutdown(&self) -> bool` — check whether the runtime has been shut down.
- `pub fn health_events(&self) -> tokio::sync::broadcast::Receiver<RuntimeEvent>` — subscribe to runtime health events.
- `pub fn health_tx(&self) -> tokio::sync::broadcast::Sender<RuntimeEvent>` — clone the health-event sender for shared transport components.
- `pub fn spawn<T: Timestamp>(&self, dataflow: LogicalDataflow<T>, options: SpawnOptions) -> Result<SpawnedDataflow<T>>` — run one logical dataflow.
- `pub fn spawn_multi<T, F>(&self, name: &str, num_workers: usize, build: F, options: SpawnOptions) -> Result<MultiSpawnedDataflow<T>> where F: Fn(&mut DataflowBuilder<T>) -> Result<()>` — build and run N replicated workers.
- `#[cfg(feature = "transport")] pub fn spawn_cluster<T, F, R, W>(&self, name: &str, topology: ClusterTopology, local_node_id: &str, dataflow_id: DataflowId, transport_config: ClusterSpawnTransport<R, W>, handshake_timeout: Duration, build: F, runtime_handle: &tokio::runtime::Handle, options: SpawnOptions) -> Result<ClusterSpawnedDataflow<T>> where T: Timestamp + ExchangeData, F: Fn(&mut DataflowBuilder<T>) -> Result<()>, R: AsyncRead + Unpin + Send + 'static, W: AsyncWrite + Unpin + Send + 'static` — run a distributed dataflow across nodes.
- `#[cfg(feature = "transport")] pub fn report_node_leave(&self, node_id: &str) -> usize` — cancel dataflows that depend on a departed peer.
- `#[cfg(feature = "transport")] pub fn report_node_join(&self, node_id: &str) -> bool` — clear the departed-peer flag for future cluster spawns.

Example:

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default())?;
let builder = DataflowBuilder::<u64>::new("hello");
builder.source("numbers", vec![(0, vec![1, 2, 3])]).output("out")?;
let handle = rt.spawn(builder.build()?, SpawnOptions::default())?;
handle.join_blocking()?;
# Ok::<(), instancy::Error>(())
```

See also: [Getting Started](../guide/getting-started.md), [Distributed Execution](../guide/distributed.md).

### `SimpleRuntime`

*Requires feature `test-utils`.*

Lightweight, single-threaded runtime for unit tests. No Tokio runtime, no worker pool — runs the dataflow synchronously on the calling thread.

Key methods:

- `pub fn new() -> Self` — create a new simple runtime.
- `pub fn with_cancel(cancel: CancellationToken) -> Self` — create with an existing cancellation token.
- `pub fn cancel_token(&self) -> &CancellationToken` — get the cancellation token.
- `pub fn run<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<()>` — run a pre-loaded dataflow to completion (blocking). Dataflow must not have `input()` ports.
- `pub fn run_with_metrics<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<Option<Arc<DataflowMetrics>>>` — run and return collected metrics.
- `pub fn spawn<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<SpawnedDataflow<T>>` — spawn on a background thread with channel-based I/O for feeding data and collecting results.
- `pub fn spawn_multi<T, F>(&self, name: &str, num_workers: usize, build: F) -> Result<MultiSpawnedDataflow<T>> where F: Fn(&mut DataflowBuilder<T>) -> Result<()>` — build and run N replicated workers on dedicated background threads.

Example:

```rust
use instancy::{DataflowBuilder, SimpleRuntime};

let rt = SimpleRuntime::new();
let builder = DataflowBuilder::<u64>::new("test");
builder.source("nums", vec![(0, vec![1, 2, 3])]).output("out")?;
rt.run(builder.build()?)?;
```

See also: [Testing](../guide/testing.md).

### `RuntimeConfig`

Runtime construction settings.

| Field | Type | Default | Notes |
|---|---|---|---|
| `worker_threads` | `usize` | `num_cpus()` | Number of instancy worker-pool threads. |
| `schedule_policy` | `Option<Box<dyn SchedulePolicy>>` | `None` | `None` means FIFO scheduling. |
| `name` | `String` | `"instancy".to_string()` | Used in thread names and diagnostics. |
| `tokio_mode` | `TokioMode` | `TokioMode::Auto` | Reuse current Tokio runtime when possible, otherwise create one. |
| `topology` | `Option<ClusterTopology>` | `None` | Feature-gated (`transport`); optional initial live topology. |

### `TokioMode`

Controls how instancy gets a Tokio runtime for async I/O and transport tasks.

- `TokioMode::Create { worker_threads }` — create an owned Tokio runtime.
- `TokioMode::External(handle)` — reuse a caller-owned runtime.
- `TokioMode::CurrentContext` — require an active Tokio context.
- `TokioMode::Auto` — prefer the current Tokio runtime, otherwise create one.

### `SpawnOptions`

Per-spawn execution settings.

Builder methods:

- `SpawnOptions::new()`
- `.io_mode(IoMode)`
- `.collect_metrics(bool)`
- `.metrics(MetricsConfig)`
- `.priority(u32)`
- `.cancellation_token(tokio_util::sync::CancellationToken)`
- `.drain_on_cancel(Duration)`
- `.per_stage_parallelism(bool)`
- `.auto_parallelism(bool)`

Defaults:

- `io_mode = IoMode::Sync`
- `metrics = MetricsConfig::none()`
- `priority = 0`
- `cancellation_token = None`
- `drain_timeout = None`
- `per_stage_parallelism = true`
- `auto_parallelism = true`

Notes:

- Parallelism is chosen by `spawn_multi`, `spawn_cluster`, and repartition operators such as `exchange_to`, `rebalance_to`, and `gather`.
- `SpawnOptions` does **not** contain an `ErrorPolicy` field; error policy is a separate type under `instancy::execute`.

### `IoMode`

- `IoMode::Sync` — blocking std channels for external I/O.
- `IoMode::Async` — Tokio channels for async send/recv.

### `RuntimeEvent`

Lifecycle and health events emitted by the runtime. Subscribe via `RuntimeHandle::health_events()`.

Variants:

- `TransportDegraded { peer_id: String, detail: String }` — a shared transport component is permanently degraded (e.g., background thread panicked while holding a lock). Recommended action: shut down the runtime and create a fresh `RuntimeHandle`.

See also: [Observability](../guide/observability.md).

### `WorkerId`

A globally unique logical worker identity (`WorkerId(pub usize)`). Workers are numbered sequentially across all nodes in the cluster. Determines data partitioning (exchange routing) and FIFO ordering for tasks assigned to the same worker.

Key methods:

- `new(index: usize) -> Self`
- `index(&self) -> usize`

## Scheduling

### `SchedulePolicy`

Trait that determines task ordering in the queue. The scheduler compares two tasks and returns which should run first.

```rust
pub trait SchedulePolicy: Send + Sync {
    fn compare(&self, a: &TaskMeta, b: &TaskMeta) -> Ordering;
}
```

`TaskMeta` carries `dataflow_id`, `priority: u32`, and `created_at: Instant`.

When no policy is set (default), the scheduler uses plain FIFO with O(1) pop. When a policy is set, it uses a `BinaryHeap` for O(log n) operations.

### `TaskMeta`

Public task metadata passed to `SchedulePolicy::compare`.

- Fields: `dataflow_id: DataflowId`, `priority: u32`, `created_at: Instant`
- `pub fn new(dataflow_id: DataflowId, priority: u32) -> Self`

### `PriorityPolicy`

Strict priority scheduling — higher priority always wins. Can starve low-priority dataflows.

### `PriorityWithAgingPolicy`

Priority with aging — prevents starvation by increasing effective priority based on wait time. Configured with `aging_rate: f64` (default: 0.1, i.e., 1 priority level per 10 seconds). Higher aging rate causes low-priority tasks to be promoted faster.

```rust
use instancy::{PriorityWithAgingPolicy, RuntimeConfig};

let config = RuntimeConfig {
    schedule_policy: Some(Box::new(PriorityWithAgingPolicy { aging_rate: 2.0 })),
    ..Default::default()
};
```

See also: [Design decisions §12.10](../design/decisions.md).

### Join Handles

`RuntimeHandle::spawn`, `spawn_multi`, and `spawn_cluster` return handle types with similar ergonomics.

#### `SpawnedDataflow<T>`

- `name(&self) -> &str`
- `metrics(&self) -> Option<&Arc<DataflowMetrics>>`
- `cancel_token(&self) -> &CancellationToken`
- `take_input<D>(&mut self, name: &str) -> Result<InputSender<T, D>>`
- `take_output<D>(&mut self, name: &str) -> Result<OutputReceiver<T, D>>`
- `take_async_input<D>(&mut self, name: &str) -> Result<AsyncInputSender<T, D>>`
- `take_async_output<D>(&mut self, name: &str) -> Result<AsyncOutputReceiver<T, D>>`
- `cancel(&self)` / `cancel_with_reason(&self, CancellationReason)`
- `join(mut self) -> DataflowCompletion`
- `join_blocking(self) -> Result<()>`

#### `MultiSpawnedDataflow<T>`

- `name(&self) -> &str`
- `num_workers(&self) -> usize`
- `worker_mut(&mut self, worker_idx: usize) -> &mut SpawnedDataflow<T>`
- `take_input<D>(&mut self, worker_idx: usize, name: &str) -> Result<InputSender<T, D>>`
- `take_output<D>(&mut self, worker_idx: usize, name: &str) -> Result<OutputReceiver<T, D>>`
- `take_async_input<D>(&mut self, worker_idx: usize, name: &str) -> Result<AsyncInputSender<T, D>>`
- `take_async_output<D>(&mut self, worker_idx: usize, name: &str) -> Result<AsyncOutputReceiver<T, D>>`
- `take_all_inputs<D>(&mut self, name: &str) -> Result<Vec<InputSender<T, D>>>`
- `take_all_outputs<D>(&mut self, name: &str) -> Result<Vec<OutputReceiver<T, D>>>`
- `take_all_async_inputs<D>(&mut self, name: &str) -> Result<Vec<AsyncInputSender<T, D>>>`
- `take_all_async_outputs<D>(&mut self, name: &str) -> Result<Vec<AsyncOutputReceiver<T, D>>>`
- `cancel(&self)` / `cancel_with_reason(&self, CancellationReason)`
- `join(mut self) -> MultiDataflowCompletion`
- `join_blocking(mut self) -> Result<()>`

#### `ClusterSpawnedDataflow<T>`

- `name(&self) -> &str`
- `num_local_workers(&self) -> usize`
- `total_workers(&self) -> usize`
- `local_worker_range(&self) -> (usize, usize)`
- `worker_metrics(&self, local_idx: usize) -> Option<&Arc<DataflowMetrics>>`
- `all_worker_metrics(&self) -> Vec<Option<&Arc<DataflowMetrics>>>`
- `take_input<D>(&mut self, local_idx: usize, name: &str) -> Result<InputSender<T, D>>`
- `take_output<D>(&mut self, local_idx: usize, name: &str) -> Result<OutputReceiver<T, D>>`
- `take_async_input<D>(&mut self, local_idx: usize, name: &str) -> Result<AsyncInputSender<T, D>>`
- `take_async_output<D>(&mut self, local_idx: usize, name: &str) -> Result<AsyncOutputReceiver<T, D>>`
- `cancel(&self)` / `cancel_with_reason(&self, CancellationReason)`
- `join(mut self) -> Result<ClusterCompletion<T>>`
- `join_blocking(self) -> Result<()>`

#### Completion handles

- `DataflowCompletion::new() -> (DataflowCompletion, CompletionNotifier)`
- `DataflowCompletion::wait(self) -> Result<()>`
- `MultiDataflowCompletion::wait(self) -> Result<()>`
- `ClusterCompletion<T>::wait(self) -> Result<()>`

## Dataflow Construction

### `DataflowBuilder<T>`

Builder for typed dataflow graphs.

Key methods:

- `pub fn new(name: impl Into<String>) -> Self`
- `pub fn with_config(name: impl Into<String>, config: DataflowBuilderConfig) -> Self`
- `pub fn with_context<C: Send + Sync + 'static>(&self, value: C) -> &Self`
- `pub fn with_context_arc<C: Send + Sync + 'static>(&self, value: Arc<C>) -> &Self`
- `pub fn get_context<C: Send + Sync + 'static>(&self) -> Option<Arc<C>>`
- `pub fn catch_panics(&self, enable: bool) -> &Self`
- `pub fn input<D: Clone + Send + 'static>(&self, name: impl Into<String>) -> Result<Pipe<T, D>>`
- `pub fn source<D: Clone + Send + 'static>(&self, name: impl Into<String>, data: Vec<(T, Vec<D>)>) -> Pipe<T, D>`
- `pub fn source_async<D, F, Fut>(&self, name: impl Into<String>, producer: F) -> Pipe<T, D> where D: Clone + Send + 'static, F: FnOnce(AsyncInputSender<T, D>) -> Fut + Clone + Send + 'static, Fut: Future<Output = Result<()>> + Send + 'static`
- `pub fn operator_count(&self) -> usize`
- `pub fn name(&self) -> &str`
- `pub fn build(self) -> Result<LogicalDataflow<T>>`

Related public types:

- `pub struct Pipe<T: Timestamp, D: Clone + Send + 'static>`
- `pub struct StreamEdge<S: Scope, D>` — lower-level typed edge metadata with `new`, `scope`, `scope_mut`, `source`, `stage_id`, and `in_stage`
- `pub struct OutputPort<T: Timestamp, D: Send + 'static>` — key methods: `name(&self) -> &str`, `collector(&self) -> Arc<Mutex<Vec<(T, Vec<D>)>>>`
- `pub struct LogicalDataflow<T: Timestamp>` — key methods: `name(&self) -> &str`, `contexts(&self) -> &SharedContext`, `operator_count(&self) -> usize`, `edge_count(&self) -> usize`, `input_names(&self) -> Vec<&str>`, `output_names(&self) -> Vec<&str>`, `has_input_ports(&self) -> bool`, `graph(&self) -> &DataflowGraph`, `exchange_edge_indices(&self) -> Vec<usize>`, `feedback_edge_count(&self) -> usize`

See also: [Building Dataflows](../guide/building-dataflows.md).

### `DataflowBuilderConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `channel_capacity` | `usize` | `1024` | Logical backpressure limit for channels. |
| `channel_preallocate` | `Option<usize>` | `None` | Optional eager channel allocation, clamped to capacity. |

## Streams and Operators

The current operator-chaining surface is implemented on `Pipe<T, D>`. `StreamEdge<S, D>` is the lower-level typed edge metadata type. The guide pages explain behavior in detail; this section summarizes the main entry points.

### Core transforms

- `map(name, |&T, D| -> D2) -> Pipe<T, D2>`
- `filter(name, |&T, &D| -> bool) -> Pipe<T, D>`
- `flat_map(name, |&T, D| -> Vec<D2>) -> Pipe<T, D2>`
- `try_flat_map(name, |&T, D| -> Result<Vec<D2>>) -> Pipe<T, D2>`
- `map_batch(name, |&T, Vec<D>| -> Vec<D2>) -> Pipe<T, D2>`
- `try_map_batch(name, |&T, Vec<D>| -> Result<Vec<D2>>) -> Pipe<T, D2>`
- `inspect(name, |&T, &D| ...) -> Pipe<T, D>`
- `inspect_batch(name, |&T, &[D]| ...) -> Pipe<T, D>`
- `for_each(name, |&T, &D| ...)` / `for_each_batch(name, |&T, &[D]| ...)`
- `merge(other) -> Result<Pipe<T, D>>`
- `Pipe::concat(Vec<Pipe<T, D>>) -> Result<Pipe<T, D>>`
- `branch(name, |&T, &D| -> bool) -> (Pipe<T, D>, Pipe<T, D>)`
- `branch_result(name) -> (Pipe<T, V>, Pipe<T, E>)`
- `output(name) -> Result<OutputPort<T, D>>`
- `probe() -> (Pipe<T, D>, ProbeHandle<T>)`

Fallible transform examples:

```rust
let words = paths.try_flat_map("read_words", |_time, path: String| {
    let contents = std::fs::read_to_string(path)?;
    Ok(contents
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>())
});

let sorted = paths.try_map_batch("read_and_sort", |_time, paths: Vec<String>| {
    let mut batch = paths
        .into_iter()
        .map(std::fs::read_to_string)
        .collect::<Result<Vec<_>, _>>()?;
    batch.sort();
    Ok(batch)
});
```

### Aggregation and flow control

- `reduce(name, |D, D| -> D) -> Pipe<T, D>`
- `fold(name, init, |Acc, D| -> Acc) -> Pipe<T, Acc>`
- `distinct(name) -> Pipe<T, D>` (`D: Eq + Hash`)
- `count(name) -> Pipe<T, usize>`
- `take(name, count) -> Pipe<T, D>`
- `take_while(name, |&T, &D| -> bool) -> Pipe<T, D>`
- `delay(name, |&T, &D| -> T) -> Pipe<T, D>`
- `delay_batch(name, |&T| -> T) -> Pipe<T, D>`

### Distribution operators

- `exchange<K: Hash + 'static>(name, |&D| -> K) -> Pipe<T, D>`
- `exchange_to<K: Hash + 'static>(name, parallelism, |&D| -> K) -> Result<Pipe<T, D>>`
- `exchange_by_hash(name, |&D| -> u64) -> Pipe<T, D>`
- `exchange_by_hash_to(name, parallelism, |&D| -> u64) -> Result<Pipe<T, D>>`
- `gather(name) -> Pipe<T, D>`
- `rebalance(name) -> Pipe<T, D>`
- `rebalance_to(name, parallelism) -> Result<Pipe<T, D>>`
- `broadcast(name) -> Pipe<T, D>`

Notes:

- In `transport` builds, exchange/repartition operators require `T` and `D` to implement `ExchangeData`.
- In non-transport builds, these operators are available for `D: Clone + Send + 'static`. 

### Custom operators and loops

- `unary(name, logic) -> Pipe<T, D2>`
- `unary_notify(name, logic) -> Pipe<T, D2>`
- `unary_async(name, logic) -> Pipe<T, D2>`
- `binary(other, name, logic) -> Pipe<T, D3>`
- `iterate<TInner>(name, summary: TInner::Summary, body) -> Pipe<T, D>`

Notes:

- There is **no public `binary_notify` method** in the current API on this branch.
- `map_ok` and `filter_ok` are available for `Pipe<T, Result<V, E>>`. 

See also: [Building Dataflows](../guide/building-dataflows.md), [Custom Operators](../guide/custom-operators.md), [Iteration](../guide/iteration.md).

## Inputs, Outputs, and Probes

### `InputSender<T, D>`

Synchronous handle returned by `SpawnedDataflow::take_input`.

- `send(&self, time: T, data: Vec<D>) -> Result<()>`
- `advance_to(&self, time: T) -> Result<()>`
- `close(self)`

### `AsyncInputSender<T, D>`

Async counterpart used with `IoMode::Async`.

- `async fn send(&self, time: T, data: Vec<D>) -> Result<()>`
- `async fn advance_to(&self, time: T) -> Result<()>`
- `close(self)`

### `OutputReceiver<T, D>`

Synchronous output handle.

- `recv(&self) -> Option<OutputEvent<T, D>>`
- `try_recv(&self) -> Option<OutputEvent<T, D>>`
- `recv_timeout(&self, timeout: Duration) -> Option<OutputEvent<T, D>>`
- `collect_data(&self) -> Vec<(T, Vec<D>)>`

`OutputEvent<T, D>` variants:

- `Data { time: T, data: Vec<D> }`
- `Frontier(T)`

### `AsyncOutputReceiver<T, D>`

Async output handle used with `IoMode::Async`.

- `async fn recv(&mut self) -> Option<OutputEvent<T, D>>`
- `fn try_recv(&mut self) -> Option<OutputEvent<T, D>>`
- `async fn collect_data(&mut self) -> Vec<(T, Vec<D>)>`

### `ProbeHandle<T>`

Track frontier progress at a point in the graph.

- `done_with(&self, time: &T) -> bool`
- `frontier(&self) -> Antichain<T>`
- `is_done(&self) -> bool`
- `async fn wait_until_done_with(&self, time: &T) -> Result<(), Error>`
- `async fn wait_until_done(&self) -> Result<(), Error>`
- `subscribe(&self) -> tokio::sync::watch::Receiver<Antichain<T>>`

See also: [Core Concepts](../guide/core-concepts.md), [Observability](../guide/observability.md).

## Timestamps and Progress

### `Timestamp`

Public trait at `instancy::Timestamp`.

```rust
pub trait Timestamp:
    Clone + Eq + PartialOrder + Ord + Debug + Default + Send + Sync + 'static
{
    type Summary: PathSummary<Self> + Send + Sync + 'static;
    fn minimum() -> Self;
}
```

Built-in implementations include `()`, `usize`, `u32`, `u64`, `i32`, `i64`, and `Product<TOuter, TInner>`.

### `PathSummary<T>`

Public trait at `instancy::progress::timestamp::PathSummary`.

```rust
pub trait PathSummary<T: Timestamp>:
    Clone + Eq + PartialOrder + Debug + Default + Send + Sync + 'static
{
    fn results_in(&self, src: &T) -> Option<T>;
    fn followed_by(&self, other: &Self) -> Option<Self>;
}
```

### `Product<TOuter, TInner>`

Nested-scope timestamp pair.

- `pub fn new(outer: TOuter, inner: TInner) -> Self`
- Fields: `outer`, `inner`

### `PartialOrder`

Public trait at `instancy::order::PartialOrder`.

- `fn less_equal(&self, other: &Self) -> bool`
- `fn less_than(&self, other: &Self) -> bool`

### `TotalOrder`

Public marker trait at `instancy::order::TotalOrder`.

- `pub trait TotalOrder: PartialOrder + Ord {}`

### `Antichain<T>`

Minimal set of mutually incomparable timestamps.

- `new()` / `from_elem(element)` / `from_elem_iter(iter)`
- `elements() -> &[T]`
- `is_empty() -> bool`
- `len() -> usize`
- `clear(&mut self)`
- `insert(element) -> bool`
- `insert_ref(&mut self, element: &T) -> bool where T: Clone`
- `less_than(&self, time: &T) -> bool`
- `less_equal(&self, time: &T) -> bool`
- `as_option(&self) -> Option<&T>` (`T: TotalOrder`)
- `into_option(self) -> Option<T>` (`T: TotalOrder`)

### `MutableAntichain<T>`

Incremental frontier tracker with multiplicity.

- `new()` / `from_elem(element)`
- `frontier() -> &[T]`
- `frontier_antichain() -> Antichain<T>`
- `is_empty() -> bool`
- `clear()`
- `less_than(&self, time: &T) -> bool`
- `less_equal(&self, time: &T) -> bool`
- `update_iter(updates) -> Vec<(T, i64)>`
- `count_for(&self, time: &T) -> i64`

### `Capability<T>` and `CapabilitySet<T>`

Progress permits for producing data at a time.

`Capability<T>` methods:

- `time(&self) -> &T`
- `delayed(&self, new_time: &T) -> Result<Self>`
- `try_delayed(&self, new_time: &T) -> Option<Self>`
- `downgrade(&mut self, new_time: &T) -> Result<()>`

`CapabilitySet<T>` methods:

- `new()` / `from_elem(cap)`
- `insert(cap)`
- `delayed(&self, time: &T) -> Result<Capability<T>>`
- `try_delayed(&self, time: &T) -> Option<Capability<T>>`
- `downgrade(&mut self, frontier: impl IntoIterator<Item = T>) -> Result<()>`
- `is_empty(&self) -> bool`
- `len(&self) -> usize`
- `frontier(&self) -> Antichain<T>`
- `iter(&self) -> impl Iterator<Item = &Capability<T>>`
- `retain<F: FnMut(&Capability<T>) -> bool>(&mut self, f: F)`

See also: [Core Concepts](../guide/core-concepts.md), [Iteration](../guide/iteration.md).

## Networking and Clusters

### `NodeConfig`

Physical node description.

- `pub fn new(node_id: impl Into<String>, logical_workers: usize) -> Self`
- Fields: `node_id`, `logical_workers`

### `ClusterTopology`

Describes the physical cluster layout.

Key methods:

- `single_node(logical_workers) -> Self`
- `multi_node(configs) -> Result<Self>`
- `total_workers() -> usize`
- `worker_range(node_id) -> Option<(usize, usize)>`
- `node_for_worker(worker_id) -> Option<&str>`
- `workers_for_node(node_id) -> Vec<WorkerId>`
- `contains_node(node_id) -> bool`
- `add_node(config) -> Result<()>`
- `remove_node(node_id) -> Result<NodeConfig>`
- `node_count() -> usize`
- `with_membership(provider) -> Self`
- `has_membership() -> bool`

### `ClusterMembership`

Application-supplied membership event stream.

- `fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>>`

Supporting enums:

- `MembershipEvent::NodeJoined { node_id, logical_workers }`
- `MembershipEvent::NodeLeft { node_id, reason }`
- `NodeDepartureReason::{Graceful, ConnectionLost, Removed}`

### `ChannelMembership`

Simple in-memory `ClusterMembership` implementation.

- `pub fn new() -> Self`
- `pub fn sender(&self) -> tokio::sync::mpsc::UnboundedSender<MembershipEvent>`
- `impl Default`
- `impl ClusterMembership`

### `ConnectionManager`

Public trait at `instancy::communication::ConnectionManager`.

```rust
pub trait ConnectionManager: Send + Sync + 'static {
    type Connection: Send + 'static;
    type Error: fmt::Debug + fmt::Display + Send + Sync + 'static;
    fn establish(&self, request: ConnectionRequest)
        -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;
    fn is_healthy(&self, _conn: &Self::Connection)
        -> impl Future<Output = bool> + Send { async { true } }
}
```

### `ConnectionFactory`

Feature-gated shared-transport trait at `instancy::communication::ConnectionFactory`.

```rust
pub trait ConnectionFactory: Send + Sync + 'static {
    type Reader: AsyncRead + Unpin + Send + 'static;
    type Writer: AsyncWrite + Unpin + Send + 'static;
    fn establish(&self, peer_node_id: &str)
        -> impl Future<Output = Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>> + Send;
}
```

`TcpConnectionFactory::new(resolver)` is the built-in plain-TCP implementation.

### `SharedPeerManager`

Feature-gated shared-transport manager at `instancy::communication::SharedPeerManager`.

Key methods:

- `pub fn new(peer_node_id: String, config: SharedConnectionConfig, connection_factory: Arc<dyn DynConnectionFactory>, runtime_handle: &tokio::runtime::Handle, health_tx: broadcast::Sender<RuntimeEvent>) -> Result<Self>`
- `pub async fn register_dataflow(&self, dataflow_id: DataflowId, channel_ids: &[u64], channel_capacity: usize) -> (HashMap<u64, tokio::sync::mpsc::Receiver<Vec<u8>>>, tokio::sync::mpsc::Receiver<TransportError>)`
- `pub async fn unregister_dataflow(&self, dataflow_id: &DataflowId)`
- `pub fn peer_node_id(&self) -> &str`

See also: [Distributed Execution](../guide/distributed.md).

## Serialization

### `Codec<T>`

```rust
pub trait Codec<T>: Send + Sync {
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError>;
}
```

Related serialization items:

- `pub const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024`
- `CodecError::{InsufficientData, InvalidData, Custom, CrcMismatch, PayloadTooLarge, External}`
- `RawBytesCodec`
- `FixedSizeCodec<T: Copy>`
- `StringCodec`
- `#[cfg(feature = "bincode-codec")] BincodeCodec<T>`

### `ExchangeData`

Types that can cross process boundaries.

```rust
pub trait ExchangeData: Data {
    type CodecType: Codec<Self>;
    fn codec() -> Self::CodecType;
}
```

`BincodeCodec<T>` is available behind the `bincode-codec` feature.

See also: [Serialization](../guide/serialization.md).

## Cancellation and Errors

### `CancellationToken`

Cooperative cancellation token re-exported at `instancy::CancellationToken`.

Key methods:

- `new() -> Self`
- `child_token(&self) -> Self`
- `cancel(&self)`
- `cancel_with_reason(&self, reason: CancellationReason)`
- `reason(&self) -> Option<CancellationReason>`
- `is_cancelled(&self) -> bool`
- `check(&self) -> Result<()>`
- `register_wake_handle(&self, wake_handle: WakeHandle)`
- `async fn cancelled_async(&self)`

### `CancellationReason`

Enum describing why a dataflow was cancelled. First-cancel-wins semantics — only the first reason is recorded. Child tokens inherit the parent's reason.

| Variant | Description |
|---|---|
| `UserRequested` | Explicit user cancellation |
| `RuntimeShutdown` | Runtime shutting down |
| `NetworkError { detail }` | TCP disconnect or transport failure |
| `WorkerFailed { detail }` | Worker failure causing cascading cancellation |
| `HandleDropped` | `SpawnedDataflow` dropped without `join()` |
| `OperatorError { detail }` | Operator error caused cancellation |
| `PeerCancelled { peer_id, detail }` | Remote peer cancelled the distributed dataflow |
| `PeerDown { node_id }` | Peer reported as down via `report_node_leave()` |
| `InternalError { detail }` | Internal runtime error (e.g., poisoned lock) |

### `ErrorPolicy`

Public at `instancy::execute::ErrorPolicy`.

- `ErrorPolicy::Stop`
- `ErrorPolicy::Ignore { description: String }`

### Root `Error`

Main error enum used throughout the crate.

Important variants:

- `Io(std::io::Error)`
- `Cancelled { reason: Option<CancellationReason> }`
- `Progress(ProgressError)`
- `Operator { operator, worker_index, source }`
- `Backpressure`
- `ChannelClosed`
- `OperatorPanic { operator, worker_index, message }`
- `LockPoisoned { context }`
- `Topology(TopologyError)`
- `Dataflow(DataflowError)`
- `Runtime(RuntimeError)`
- `Communication(CommunicationError)`

Helper constructors:

- `Error::codec(err)`
- `Error::operator(name, err)`
- `Error::operator_with_context(name, worker_index, err)`
- `with_operator_context(self, operator, worker_index)`

### Module-specific error enums

- `ProgressError::{TimeNotAdvanced, NoDominatingCapability}`
- `TopologyError::{NodeAlreadyExists, NodeNotFound, EmptyTopology, InvalidNodeConfig}`
- `DataflowError::{InvalidConfig, InvalidGraph, MissingEndpoint, TypeMismatch, EndpointTaken, MissingFactory}`
- `RuntimeError::{InvalidConfig, SpawnFailed, ClusterSetup, AlreadyConsumed, EmptyDataflow}` plus `#[cfg(feature = "transport")] Handshake`
- `CommunicationError::{Codec, InvalidConfig, InvalidSetup}` plus `#[cfg(feature = "transport")] Protocol`

See also: [Error Handling](../guide/error-handling.md).

## Related Documentation

- [Guide](../guide/README.md)
- [Cookbook](../cookbook.md)
- [Design Docs](../design/README.md)
