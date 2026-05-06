# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Communication
- `sequencing` module — message sequencing primitives for shared connection mode
  - `SequenceCounter` — thread-safe monotonic sequence ID generator per logical stream
  - `ReorderBuffer<T>` — delivers frames in sequence order with gap detection and timeout
  - `SequencedFrame` — frame with attached sequence ID for wire protocol extension
  - `encode_sequenced_header` / `decode_sequenced_header` — 36-byte wire format (28 base + 8 seq_id)
- `shared_pool` module — adaptive connection pool for shared connection mode
  - `SharedConnectionConfig` — configuration (min/max connections, RTT thresholds, cooldown, probe interval)
  - `ConnectionMode` enum — `Dedicated` (default) vs `Shared(SharedConnectionConfig)`
  - `RttTracker` — exponential moving average RTT measurement per connection
  - `ConnectionMetrics` — per-connection load tracking (pending writes, RTT, bytes/frames written)
  - `PeerPool` — per-peer pool with least-loaded selection and adaptive scaling decisions
  - `ScalingDecision` enum — `None` / `ScaleUp` / `ScaleDown { connection_id }`
- `probing` module — RTT probing and adaptive scaling driver
  - `ProbeMessage` — compact 17-byte probe wire format (request/reply) with encode/decode
  - `ProbeKind` — `Request` / `Reply` discriminant
  - `ProbeCounter` — atomic probe sequence generator
  - `ProbeTimestamp` — epoch-relative nanosecond timestamps for accurate RTT measurement
  - `ScalingDriver` — orchestrates probe tracking, RTT computation, and scaling event emission
  - `ScalingEvent` enum — `ScaleUp` / `ScaleDown { connection_id }` events via mpsc channel

## [0.2.0] - 2026-05-05

### Added

#### Runtime
- `TokioMode` configuration for `RuntimeHandle` — control how instancy obtains a tokio runtime (#141)
  - `Auto` (default) — reuses the current tokio context or creates a 2-thread runtime
  - `Create { worker_threads }` — creates a dedicated multi-thread tokio runtime
  - `External(Handle)` — uses an externally-provided tokio runtime handle
  - `CurrentContext` — requires an active tokio runtime (errors if none exists)
- `RuntimeHandle::tokio_handle()` accessor for the underlying tokio runtime
- `RuntimeHandle::active_dataflows()` — returns the number of currently running dataflows (#143)
- `RuntimeHandle::wait_idle()` — async method that resolves when all dataflows complete (#143)
- `RuntimeHandle::shutdown_async()` — cancels all dataflows and awaits their completion (#143)
- `Future` impl for `MultiDataflowCompletion` — enables `.await` on multi-worker dataflows (#144)

#### Operators
- `Pipe::unary_async(name, max_concurrency, logic)` — async unary operator that spawns tokio tasks for each input batch (#145)
  - Bounded concurrency control via `max_concurrency` parameter
  - Results arrive in completion order (not input order)
  - Error propagation from async tasks to the dataflow

#### Cancellation
- External cancellation token support via `SpawnOptions::cancellation_token()` — accepts a `tokio_util::sync::CancellationToken` to cancel dataflows from user code (#139)
- `SpawnedDataflow::cancel_token()` accessor for programmatic cancellation
- Waker-based `CancellationToken::cancelled_async()` — replaces 10ms polling with instant notification via `tokio::sync::Notify` (#140)

#### Scheduler Priority & Optimization
- Configurable task scheduling policies via `RuntimeConfig::schedule_policy` (#132, #133, #134)
  - `PriorityPolicy` — schedule higher-priority dataflows first
  - `PriorityWithAgingPolicy` — priority with wait-time bonus to prevent starvation
  - `None` (default) — pure FIFO with O(1) dequeue, zero comparison overhead
  - All policies use `BinaryHeap` for O(log n) insert/dequeue

#### Documentation & Guides
- `COOKBOOK.md` with practical patterns: windowed aggregation, fan-out/fan-in, error recovery (#129)
- `GUIDE.md` troubleshooting section for common issues (#129)
- Comprehensive progress tracking module documentation (#126)

#### Reliability & Observability
- `DataflowBuilder::catch_panics(true)` converts operator panics into `Error::OperatorPanic` instead of unwinding the runtime (#117)
- Async probe notifications via `ProbeNotifier`, allowing `ProbeHandle` waiters to wake promptly on frontier changes (#118)
- `take()` and `take_while()` operators for bounded and predicate-driven stream truncation (#119)
- Per-operator metrics via `DataflowMetrics`, `OperatorMetrics`, and `BackpressureMetrics`; spawned dataflows can access the live metrics via `SpawnedDataflow::metrics()` (#120, #125)

#### CI/CD
- GitHub Actions workflow for automated testing on push and PR (#130)

#### Examples
- Added `cluster_basic` and `cluster_exchange` examples for distributed execution (#123)
- Added `error_handling` example covering `map_ok`, `filter_ok`, and `branch_result` (#124)
- Added `metrics_collection` example showing runtime metrics collection and reporting (#125)

#### Testing
- Scheduler policy integration tests: priority ordering, FIFO fairness, aging, multi-dataflow (#132)
- Progress tracking integration tests: frontier advancement, notifications, iteration (#128)
- Edge case integration tests: empty streams, large batches, deep pipelines, concat (#137)

### Fixed
- **Transport FIFO ordering violation** — data and progress frames could be reordered on the wire due to separate priority channels in the bridge task. Merged into a single FIFO payload channel per peer, preserving the timely ordering invariant (data at time T arrives before frontier advances past T). Also prevents cross-dataflow progress starvation under heavy data load. (#146)
- Control broadcast and cancellation wiring for cluster-local workers (#131)
- Compilation regression in tests/examples from schedule_policy API change (#136)

### Changed

#### Scheduler Configuration (#133)
- **BREAKING:** `RuntimeConfig::schedule_policy` changed from `Box<dyn SchedulePolicy>` to `Option<Box<dyn SchedulePolicy>>` — `None` is the new default (FIFO)

#### Runtime API Simplification (#121)
- **BREAKING:** `RuntimeHandle::spawn()` now takes `SpawnOptions` parameter
- **BREAKING:** `RuntimeHandle::spawn_multi()` now takes `SpawnOptions` parameter
- **BREAKING:** `SimpleRuntime` moved behind `test-utils` feature (use `RuntimeHandle` for production)
- Introduced `SpawnOptions` struct and `IoMode` enum to consolidate channel mode selection
- All examples updated to use `RuntimeHandle` with `SpawnOptions`

### Removed
- **BREAKING:** Removed `async-io` feature — async I/O is now always available (tokio is required)
- **BREAKING:** Removed `RuntimeHandle::run()` and `run_blocking()` — use `spawn().join()` instead
- **BREAKING:** Removed `RuntimeHandle::spawn_async()` — use `spawn(df, SpawnOptions::new().io_mode(IoMode::Async))`
- **BREAKING:** Removed `RuntimeHandle::spawn_multi_async()` — use `spawn_multi(..., SpawnOptions::new().io_mode(IoMode::Async))`

### Internal
- Resolved all clippy warnings across lib, examples, and tests (#135, #142)
- Removed unused capacity parameter from `ChannelBlueprint::build` (#127)
- Optimized scheduler queue: BinaryHeap for policy-driven scheduling (#133, #134)
- `CompletionNotifier` supports `on_complete` callback for active-dataflow tracking (#143)

### Features

| Feature | Default | Description |
|---|---|---|
| `transport` | ✅ | TCP transport layer (Tokio-based muxer/demuxer) |
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `bincode-codec` | ❌ | Bincode-based codec implementation |
| `test-utils` | ❌ | SimpleRuntime and test helpers |

## [0.1.1] - 2026-05-03

### Fixed
- Fix docs.rs build failure: replace removed `doc_auto_cfg` feature with `doc_cfg`
  (merged in Rust 1.92, see [rust-lang/rust#138907](https://github.com/rust-lang/rust/pull/138907))

## [0.1.0] - 2026-05-03

Initial release of instancy — an async reimplementation of
[timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) built on
[Tokio](https://tokio.rs/).

### Added

#### Core Dataflow Engine
- Timestamp-indexed progress tracking with frontiers and capabilities
- `MutableAntichain` and `ChangeBatch` for frontier management
- `Product<TOuter, TInner>` timestamps for nested scopes
- Cooperative `CancellationToken` with `CancellationReason` diagnostics
- `Result<T, Error>` error handling throughout (public APIs avoid panics)

#### Operator API
- `DataflowBuilder` with fluent chaining API for graph construction
- Core operators: `source`, `input`, `map`, `flat_map`, `filter`, `inspect`
- Stateful operators: `unary`, `binary`, `unary_notify`
- Stream combinators: `concat`, `branch`
- Feedback loops: `iterate` with nested scope support
- Data repartitioning: `exchange`, `exchange_by_hash`
- Observation: `probe` (frontier tracking), `output` (result collection)

#### Execution Modes
- `RuntimeHandle` — multi-worker async execution on a shared thread pool
- `spawn` / `spawn_multi` — background dataflow execution with channel I/O
- `spawn_cluster` — multi-node distributed execution over TCP
- `SpawnOptions` with `IoMode::Sync` / `IoMode::Async` for channel mode selection
- `AsyncInputSender` and `AsyncOutputReceiver` for async channel-based I/O

#### Networking (`transport` feature)
- Application-managed connections via `ConnectionManager` trait
- `TransportSession` with multiplexed framed transport
- Priority-separated channels for data and progress messages
- Fingerprint-based handshake and ready barrier protocol
- `MockNetworkEdgeMaterializer` for single-process multi-node testing

#### Serialization
- Pluggable `Codec` trait for custom serialization
- Built-in codecs for primitives, tuples, strings, `Vec<u8>`, `Product`
- `ExchangeData` trait for types participating in cross-worker exchange
- Optional `BincodeCodec` (`bincode-codec` feature)

#### Multi-Worker Exchange
- Hash-based data repartitioning across logical workers
- Cross-worker frontier aggregation
- `EdgeMaterializer` trait for pluggable exchange transport
- `NetworkEdgeMaterializer` for real TCP-based exchange

#### Documentation
- Comprehensive `README.md` with quick-start examples
- `GUIDE.md` — detailed usage guide covering all features
- `DESIGN.md` — architecture and design decisions
- 24 runnable examples covering all major features
- `doc_auto_cfg` for automatic feature-gate annotations on docs.rs
- `deny(rustdoc::broken_intra_doc_links)` for documentation correctness

#### Testing
- 1000+ unit and integration tests
- TCP-based cluster integration tests
- Parallel dataflow stress tests
- Cross-process integration test framework (`instancy-integration` crate)
- In-memory transport for deterministic multi-node testing

### Features

| Feature | Default | Description |
|---|---|---|
| `transport` | ✅ | TCP transport layer (Tokio-based muxer/demuxer) |
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `bincode-codec` | ❌ | Bincode-based codec implementation |
| `test-utils` | ❌ | SimpleRuntime and test helpers |

[0.2.0]: https://github.com/Yaming-Hub/instancy/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Yaming-Hub/instancy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Yaming-Hub/instancy/releases/tag/v0.1.0
