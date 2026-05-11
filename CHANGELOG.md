# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Observability
- `MetricsConfig` granularity controls (`None`, `SummaryOnly`, `Full`) with zero overhead
  when disabled (#239)
- Channel counters — per-edge send/receive/drop counts (#239)
- Activation timeline recording — per-operator activation timestamps and durations (#240)
- Chrome Trace JSON export for Perfetto UI behind `chrome-trace` feature flag (#241)
- Frontier and transfer timeline events for dataflow progress visualization (#242)
- Cluster observability: `SpawnOptions` propagated into `spawn_cluster()` (#243)
- Cluster metrics accessors: `worker_metrics()` and `all_worker_metrics()` on
  `ClusterSpawnedDataflow` (#244)
- Runtime health events for unrecoverable errors (#236)

#### Error Handling
- Structured error types: `CodecError`, `ControlProtocolError`, `CommunicationError`,
  `HandshakeError`, `ProgressError`, `SpawnFailed` — replacing string errors (#233, #234)

#### Progress Tracking
- Graph-topology completion propagation using graph structure for precise frontier
  advancement (#237)

#### Cancellation
- Distributed cancellation across cluster nodes via `ControlMessage::Cancel` control
  plane protocol (#238)

#### Communication
- `sequencing` module — message sequencing primitives for shared connection mode
  - `SequenceCounter`, `ReorderBuffer<T>`, `SequencedFrame`, wire format encode/decode
- `shared_pool` module — adaptive connection pool for shared connection mode
  - `SharedConnectionConfig`, `ConnectionMode`, `RttTracker`, `ConnectionMetrics`, `PeerPool`
- `probing` module — RTT probing and adaptive scaling driver
  - `ProbeMessage`, `ProbeCounter`, `ProbeTimestamp`, `ScalingDriver`, `ScalingEvent`
- `shared_transport` module — shared transport session for pooled multi-dataflow connections
  - `SharedPeerManager`, `SharedTransportSession`, `DataframeSender`, `ConnectionFactory` trait
- Dynamic cluster scaling with topology/membership consolidation (#229)
- `ConnectionFactory` made required; lazy connection initialization (#220)

#### Scheduling
- Wake-based scheduling for `unary_async` operators (#207)
  - `WakeHandle` integration, `ActivationOutcome::WaitingForAsync`, per-operator tracking

#### Performance
- Lock-free SPSC exchange channels (#226)
- Reuse per-target buckets in `ExchangePush` (#227)
- Lazy `VecDeque` allocation — defer until first push (#217)
- Opt-in CRC32 checksums for data frames (#225)
- Multi-worker scaling benchmark suite (#226)
- Comparative benchmark suite: instancy vs timely-dataflow (#212)

#### Testing
- Comprehensive integration tests: Phases 1–5 (#210)
- Failure handling and scaling integration tests (#211)
- 50-minute stress test (#213)
- Cluster observability integration test (#244)
- Async I/O cluster test with cross-node exchange (#245)

#### Documentation
- COOKBOOK §11 Cluster Dataflows recipes (#244)
- DESIGN.md §5.5.2 Cluster Startup Protocol (#243)
- GUIDE §8 Distributed Execution updates (#245)
- `lib.rs` doc comments on all crate-level re-exports (#245)
- Dynamic cluster scaling in README (#232)

### Changed

#### Runtime
- `SpawnOptions::auto_parallelism` now defaults to `true` (#208)
- Per-stage parallelism is now the default `SpawnOptions` behavior (#198)

#### API
- Unified API naming inconsistencies (#216)
- Restricted internal types to `pub(crate)` (#215)
- Removed panics from production code (#214)

### Fixed
- Resolve 24 broken rustdoc intra-doc links (#246)
- Chrome Trace flow event `id` field placement and frontier change detection (#242)
- Two flaky tests: dispatch race condition and timing jitter (#222)
- Observability tracing tests for parallel execution (#224)
- Shared transport race condition (#200)
- README inaccuracies (#218)

### Improved
- CI: build all 35 examples and lint test code (#247)
- Transport-agnostic design clarifications (#221)
- Document connection failure and reconnection responsibility (#219)
- DESIGN.md updated to reflect current state (#228)

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
