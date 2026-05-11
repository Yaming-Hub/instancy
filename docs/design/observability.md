# Observability & Metrics

This document covers the built-in observability surface for instancy: per-dataflow metrics, operator metrics, timelines, tracing integration, message envelopes, and the information a host can aggregate across nodes.

Back to the overview: [Design Overview](./README.md)

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

#### MetricsConfig — Granular Metrics Control

Metrics collection is controlled by `MetricsConfig`, which allows fine-grained
selection of what data to collect. This avoids the overhead of collecting
unnecessary metrics in production while keeping full instrumentation available
for debugging.

```rust
pub struct MetricsConfig {
    /// Collect per-operator summary stats (activation count, CPU time, records).
    /// Overhead: ~1 `Instant::now()` per activation.
    pub operator_summary: bool,
    /// Collect per-exchange-edge channel counters (items and bytes transferred).
    /// Overhead: ~2 atomic adds per batch push (~2-3% when enabled).
    pub channel_counters: bool,
    /// Record each operator activation as a timestamped event for timeline replay.
    /// Overhead: ~5-10% (timestamp capture + Vec push per activation).
    pub activation_timeline: bool,
    /// Record frontier advance events for each operator.
    /// Overhead: ~3% (1 Vec push per frontier change per operator).
    pub frontier_timeline: bool,
    /// Record data transfer events through exchange channels.
    /// Overhead: ~5-10% (1 Vec push per exchange batch).
    pub transfer_timeline: bool,
    /// Minimum activation duration to record in the timeline (default: 1µs).
    pub min_activation_duration: Duration,
    /// Maximum timeline events per category (ring buffer cap, 0 = unlimited).
    pub max_timeline_events: usize,
}
```

**Presets**:
- `MetricsConfig::none()` — all collection disabled (zero overhead, default)
- `MetricsConfig::summary_only()` — operator stats only (cheap)
- `MetricsConfig::full()` — operator stats + channel counters + all timelines (100K event cap per category)

**Usage**:
```rust
// Single-process multi-worker
let opts = SpawnOptions::new().metrics(MetricsConfig::full());
let mut multi = rt.spawn_multi("df", 4, build_fn, opts)?;

// Cluster mode — same MetricsConfig applies
let opts = SpawnOptions::new().metrics(MetricsConfig::full());
let cluster = rt.spawn_cluster(
    "df", topology, "node-a", id, transport, timeout, build_fn, &handle, opts,
)?;
```

**Channel counters** track per-exchange-edge transfer volumes:

```rust
pub struct ChannelMetrics {
    pub edge_index: usize,
    pub label: String,
    pub items_transferred: u64,
    pub bytes_transferred: u64,
}
```

Each exchange edge gets a shared `ChannelMetricsCollector` (atomic counters)
that all source workers push through. The collector is created during exchange
channel materialization (Phase 3 of `spawn_multi` / Phase 5 of `spawn_cluster`)
and registered in each worker's `DataflowMetrics`. Since collectors are
`Arc`-shared across workers, the counters reflect the total traffic through
the edge, not per-worker.

**Cluster support**: `spawn_cluster()` accepts `SpawnOptions` (including
`MetricsConfig`) and wires all observability features identically to
`spawn_multi()`: operator summary, channel counters (including network exchange
channels), activation/frontier/transfer timelines, and external cancellation
tokens. A shared `timeline_start` `Instant` is captured before worker
materialization so activation timestamps are comparable across local workers.

Access channel metrics after the dataflow runs:
```rust
if let Some(metrics) = handle.metrics() {
    for ch in metrics.channel_snapshots() {
        println!("{}: {} items, {} bytes", ch.label, ch.items_transferred, ch.bytes_transferred);
    }
    println!("Total items: {}", metrics.total_items_transferred());
}
```

#### Activation Timeline — Per-Activation Event Recording

When `activation_timeline` is enabled, the executor records each operator
activation as a timestamped `ActivationEvent`:

```rust
pub struct ActivationEvent {
    pub operator_index: usize,   // position in the dataflow graph
    pub worker_index: usize,     // which worker executed this activation
    pub start_us: u64,           // offset from dataflow start, in µs
    pub duration_us: u64,        // activation wall-clock duration, in µs
}
```

Events are collected per-worker in a `TimelineCollector` (thread-safe ring
buffer behind `Mutex<Vec>`). When `max_timeline_events > 0`, the oldest events
are evicted when the cap is reached. Activations shorter than
`min_activation_duration` are filtered at record time (still counted in
`operator_summary` if enabled).

Access timeline events after the dataflow runs:
```rust
if let Some(metrics) = handle.metrics() {
    let events = metrics.drain_timeline_events(); // sorted by start_us
    for ev in &events {
        println!("op[{}] w{}: {}µs @ +{}µs",
            ev.operator_index, ev.worker_index, ev.duration_us, ev.start_us);
    }
}
```

The timeline data is designed for export to Chrome Trace JSON format (Phase 3),
enabling post-execution visualization in Perfetto UI or Chrome's `chrome://tracing`.

#### Frontier Timeline — Frontier Advance Recording

When `frontier_timeline` is enabled, the executor records each operator's
frontier change as a timestamped `FrontierEvent`:

```rust
pub struct FrontierEvent {
    pub operator_index: usize,   // position in the dataflow graph
    pub worker_index: usize,     // which worker observed the frontier change
    pub timestamp_us: u64,       // offset from dataflow start, in µs
    pub new_frontier: String,    // Debug-formatted antichain value
}
```

Frontier events are recorded in `propagate_progress()` after
`update_operator_frontiers()` — whenever an operator's input frontier
advances, the new value is captured. Events use the same per-worker
`TimelineCollector` ring buffer as activation events (independent cap per
category).

Access frontier events after the dataflow runs:
```rust
if let Some(metrics) = handle.metrics() {
    let events = metrics.drain_frontier_events(); // sorted by timestamp_us
    for ev in &events {
        println!("op[{}] w{}: frontier → {} @ +{}µs",
            ev.operator_index, ev.worker_index, ev.new_frontier, ev.timestamp_us);
    }
}
```

#### Transfer Timeline — Data Transfer Recording

When `transfer_timeline` is enabled, each batch push through an exchange
channel records a `TransferEvent`:

```rust
pub struct TransferEvent {
    pub edge_index: usize,       // exchange edge in the dataflow graph
    pub source_worker: usize,    // worker that sent the batch
    pub target_worker: usize,    // worker that received the batch
    pub timestamp_us: u64,       // offset from dataflow start, in µs
    pub items: u64,              // number of items in the batch
    pub bytes: u64,              // estimated bytes in the batch
}
```

Access transfer events after the dataflow runs:
```rust
if let Some(metrics) = handle.metrics() {
    let events = metrics.drain_transfer_events(); // sorted by timestamp_us
    for ev in &events {
        println!("edge[{}] w{}→w{}: {} items @ +{}µs",
            ev.edge_index, ev.source_worker, ev.target_worker, ev.items, ev.timestamp_us);
    }
}
```

#### Chrome Trace Export (feature: `chrome-trace`)

The `chrome-trace` feature adds `ChromeTraceExporter` for converting collected
metrics into the [Chrome Trace Event Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU)
which can be opened in [Perfetto UI](https://ui.perfetto.dev/) or Chrome's
`chrome://tracing`.

```toml
[dependencies]
instancy = { version = "0.1", features = ["chrome-trace"] }
```

**Convenience method** on `DataflowMetrics`:

```rust
// After dataflow completes:
let metrics = handle.metrics(0).unwrap();
let exporter = metrics.drain_to_chrome_trace("my-dataflow");
exporter.save("trace.json").unwrap();
// Open trace.json in https://ui.perfetto.dev/
```

**Manual builder** for fine-grained control:

```rust
use instancy::metrics::chrome_trace::ChromeTraceExporter;

let events = metrics.drain_timeline_events();
let frontier_events = metrics.drain_frontier_events();
let transfer_events = metrics.drain_transfer_events();
let operators: Vec<_> = metrics.operator_snapshots()
    .into_iter()
    .map(|op| (op.index, op.name.clone()))
    .collect();
let channels = metrics.channel_snapshots();

let exporter = ChromeTraceExporter::new("my-dataflow")
    .with_activations(&events, &operators)
    .with_channels(&channels)
    .with_frontiers(&frontier_events, &operators)
    .with_transfers(&transfer_events)
    .with_metadata(num_workers);
exporter.save("trace.json").unwrap();
```

**Mapping to Chrome Trace events:**

| Instancy Concept | Chrome Trace Event | Field Mapping |
|---|---|---|
| Activation event | `"X"` (complete) | `ts=start_us`, `dur=duration_us`, `tid=worker_index` |
| Channel metrics | `"i"` (instant) | Summary of items/bytes transferred at `ts=0` |
| Frontier event | `"i"` (instant) | `ts=timestamp_us`, `tid=worker_index`, scope `"t"` |
| Transfer event | `"s"`/`"f"` (flow) | Source→target flow pair with items/bytes in args |
| Dataflow name | `"M"` (metadata) | `process_name` on `pid=0` |
| Worker index | `"M"` (metadata) | `thread_name` "worker-N" on `tid=N` |

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


## Cluster metrics and reporting

Distributed execution aggregates the same underlying metrics at the coordinator boundary:

- `DataflowHandle` exposes progress updates and final outcomes per node.
- `OutcomeAggregator` combines node-level results into a cluster-level outcome.
- The global progress frontier is the meet/minimum across participating nodes.
- Aggregated metrics should preserve both per-node detail and rolled-up totals so operators can distinguish skew from globally expensive work.
- Progress updates, frontier changes, and cancellation/failure outcomes are the authoritative inputs for cluster-level dashboards.
