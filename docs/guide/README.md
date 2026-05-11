# instancy Guide

This guide walks you through building streaming dataflow programs with instancy, from your first pipeline to multi-worker and distributed execution.

## Origins

instancy is derived from the ideas and architecture of [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow), a pioneering Rust framework for data-parallel dataflow computation created by Frank McSherry. instancy preserves timely's core theoretical model — partially ordered timestamps, progress tracking via pointstamps, frontier-based notifications, and nested scopes — while rearchitecting the runtime and API for modern async Rust.

### Key Differences from Timely

| Aspect | timely-dataflow | instancy |
|--------|----------------|----------|
| **Execution model** | Dedicated sync worker threads (one per worker) | Async task pool (Tokio) — multiple dataflows share a thread pool, enabling better resource utilization |
| **API style** | Closure-based scope nesting (`worker.dataflow(\|scope\| { ... })`) | Builder-based chaining (`DataflowBuilder::new().source(...).map(...).output(...)`) |
| **Error handling** | Panics on most errors | `Result`-based — operators return `Result<()>`, errors propagate cleanly |
| **Cancellation** | No built-in cancellation | Cooperative `CancellationToken` with per-dataflow and per-runtime granularity |
| **Networking** | Built-in TCP with pre-assigned ports | Delegated connection management — caller provides connections (supports SSL, custom topologies, connection pooling) |
| **Serialization** | Abomonation (zero-copy, unsafe) | Pluggable `Codec` trait with safe default; optional bincode via feature flag |
| **Operators** | Large built-in operator library | Focused core set (map, filter, unary, binary, exchange, iterate); composable via extension crates |
| **Multi-dataflow** | One dataflow per worker group | Multiple dataflows share a single runtime and thread pool |
| **Input model** | `InputHandle` with manual timestamp management | `InputSender` with `send(time, data)` and `close()` — also supports async channels |

### What instancy Preserves from Timely

These foundational concepts work the same way:

- **Partially ordered timestamps** — timestamps form a lattice; progress is tracked as antichains (frontiers)
- **Progress tracking** — pointstamp-based protocol ensures operators know when all data for a timestamp has arrived
- **Frontiers and capabilities** — operators hold capabilities that prevent downstream frontiers from advancing until released
- **Nested scopes** — the `iterate` operator creates a sub-scope with `Product<TOuter, TInner>` timestamps, exactly as in timely
- **Exchange (data partitioning)** — hash-based routing of data to specific workers, essential for aggregations and joins
- **Notification pattern** — `unary_notify` with `NotifyContext` mirrors timely's `Notificator` for "emit when epoch is complete" workflows

## Guide Pages

- [Getting Started](./getting-started.md) — installation, feature flags, first dataflow, and runtime basics.
- [Core Concepts](./core-concepts.md) — timestamps, frontiers, capabilities, scopes, and progress.
- [Building Dataflows](./building-dataflows.md) — sources, inputs, outputs, operators, and shared builder context.
- [Custom Operators](./custom-operators.md) — `unary`, `binary`, `unary_notify`, and common stateful patterns.
- [Multi-Worker Execution](./multi-worker.md) — `spawn_multi`, exchange, distribution, and per-stage parallelism.
- [Iteration](./iteration.md) — loops, feedback, nested scopes, and `Product` timestamps.
- [Distributed Execution](./distributed.md) — cluster topologies, transport modes, peer failure handling, and local cluster testing.
- [Error Handling](./error-handling.md) — panic recovery, cancellation, graceful drain, and shutdown patterns.
- [Serialization](./serialization.md) — codecs, `ExchangeData`, and the `bincode-codec` feature.
- [Observability](./observability.md) — metrics, tracing, probes, and debugging workflows.
- [Testing](./testing.md) — `SimpleRuntime`, end-to-end tests, and in-process cluster checks.

## Related Documentation

- [API Reference](../reference/api.md)
- [Cookbook](../cookbook.md)
- [Design Docs](../design/README.md)
- [Examples](../../instancy/examples/)

## Next Steps

- Next: [Getting Started](./getting-started.md)
- See also: [API Reference](../reference/api.md), [Cookbook](../cookbook.md)
