# Design: Restrict Internal Types to pub(crate)

**Item:** `api-visibility`
**Priority:** P1
**Status:** Design

## Problem

instancy exposes too many internal types through `pub mod` declarations, making the public API surface large and confusing. Users can access implementation details like `ExecutorTask`, `WorkerPool`, `ProgressTracker`, `TaskScheduler`, etc. that are not part of the intended API.

## Strategy

1. Change modules that are purely internal to `pub(crate) mod`
2. For modules with mixed public/internal content, keep the module `pub` but restrict internal items
3. Re-export user-facing types that live in newly-restricted modules via `lib.rs`

## Changes

### Modules → `pub(crate)` (no external consumers)

| Module | Reason |
|--------|--------|
| `executor_task` | Runtime internals — `TaskId`, `PollOutcome`, `ExecutorTask`, `PoolWaker`, `ExecutorRegistry` |
| `worker_pool` | Runtime internals — `WorkerPoolConfig`, `WorkerPool` |

### Types to restrict within public modules

#### `worker` module — keep `pub mod`, restrict internals
- Keep `pub`: `WorkerId` (used in tests)
- Restrict to `pub(crate)`: `WorkerContext`, `OperatorActivation`

#### `scheduler` module — keep `pub mod` for `policy`, restrict rest
- Keep `pub`: `policy::SchedulingPolicy`, `policy::PriorityPolicy`, `policy::PriorityWithAgingPolicy` (used in tests)
- Restrict to `pub(crate)`: `batching::*`, `task_scheduler::*` (`ComputeTask`, `StagePermit`, `SchedulerConfig`, `TaskScheduler`)

#### `progress` module — keep `pub mod`, restrict deep internals
- Keep `pub`: `timestamp::Timestamp` (used in tests/examples), `capability`, `frontier`, `notificator` (used by operator authors)
- Restrict to `pub(crate)`: `subgraph::*`, `reachability::*`, `network_progress::*`, `progress_channel::*`, `operate::*`, `mutable_antichain` (if not used externally)

#### `communication` module — keep `pub mod`, restrict wire-level internals
- Keep `pub`: `Codec`, `CodecError`, `ConnectionManager`, `ConnectionPool`, `SharedConnectionConfig`, `SharedPeerManager`, `ClusterSpawnTransport`, `PeerConnection`, `Frame`, `TransportError`, `DataflowSession`, `DataflowSessionBuilder`, `DynConnectionFactory`
- Restrict to `pub(crate)`: `allocator`, `control_protocol`, `interprocess` (except `PROGRESS_CHANNEL_ID`), `probing`, `sequencing`, `remote_push`, `progress_exchange`

#### `dataflow` module — keep `pub mod`, restrict internals
- Keep `pub`: `DataflowBuilder`, `StreamEdge`, `DataflowGraph`, `Pipe`, `OutputPort`, operator traits and types
- Restrict to `pub(crate)`: `executor`, `spec`, `control` internals, `channels::*` internals (edge_materializer, exchange_channel, mock_network, wake, bounded, envelope)

### New re-exports in `lib.rs`

Add re-exports for types that users need but live in restricted submodules:
```rust
pub use worker::WorkerId;
pub use progress::timestamp::Timestamp;
pub use scheduler::policy::{SchedulingPolicy, PriorityPolicy, PriorityWithAgingPolicy};
```

## Approach

Rather than a massive breaking change, take a conservative approach:
1. Restrict `executor_task` and `worker_pool` (zero external usage)
2. Restrict clearly-internal submodules within `progress`, `scheduler`, `communication`, `dataflow`
3. Add re-exports for anything that breaks examples/tests
4. Verify with `cargo check` + `cargo test`

## Testing

- All existing tests must pass
- All examples must compile
- `cargo clippy --all-features --tests -- -D warnings` must pass
- `cargo doc` should show a cleaner API surface
