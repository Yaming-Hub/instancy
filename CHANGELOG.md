# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- `SimpleRuntime` — synchronous single-threaded execution
- `RuntimeHandle` — multi-worker async execution on a shared thread pool
- `spawn` / `spawn_multi` — background dataflow execution with channel I/O
- `spawn_cluster` — multi-node distributed execution over TCP

#### Async I/O (`async-io` feature)
- `AsyncInputSender` and `AsyncOutputReceiver` for async channel-based I/O
- `spawn_async` for fully async dataflow lifecycle management

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
| `async-io` | ❌ | Async channel-based I/O for spawned dataflows |

[0.1.0]: https://github.com/Yaming-Hub/instancy/releases/tag/v0.1.0
