# Error Handling

This document defines the error model for instancy: typed error hierarchies, propagation rules, poison handling patterns, and the design requirement that library code return errors instead of panicking.

Back to the overview: [Design Overview](./README.md)

## 8. Error Handling

### 8.1 Error Hierarchy

Errors are organized by **source module** using a hierarchical structure. This enables
hosting applications to pattern-match on specific error categories while keeping the
root `Error` enum manageable.

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ── Cross-cutting (used in 3+ modules) ──
    Io(std::io::Error),
    Cancelled { reason: Option<CancellationReason> },
    Progress(ProgressError),
    Operator { operator, worker_index, source },
    OperatorPanic { operator, worker_index, message },
    Backpressure,
    ChannelClosed,
    LockPoisoned { context: String },

    // ── Module sub-enums ──
    Topology(TopologyError),      // from execute / topology module
    Dataflow(DataflowError),      // from dataflow module
    Runtime(RuntimeError),        // from runtime module
    Communication(CommunicationError), // from communication module
}

// Progress-tracking errors (2 variants)
enum ProgressError {
    TimeNotAdvanced { from, to },
    NoDominatingCapability { time },
}

// Topology errors (4 variants)
enum TopologyError {
    NodeAlreadyExists { node_id },
    NodeNotFound { node_id },
    EmptyTopology { reason },
    InvalidNodeConfig { node_id, reason },
}

// Dataflow construction errors (6 variants)
enum DataflowError {
    InvalidConfig(String),
    InvalidGraph(String),
    MissingEndpoint { operator, port },
    TypeMismatch { operator, port },
    EndpointTaken(String),
    MissingFactory { edge_index },
}

// Runtime lifecycle errors (6 variants)
enum RuntimeError {
    InvalidConfig(String),
    SpawnFailed { context, source: Option<io::Error> },
    ClusterSetup(String),
    Handshake(HandshakeError),       // #[cfg(feature = "transport")]
    AlreadyConsumed { resource },
    EmptyDataflow,
}

// Communication errors (4 variants)
enum CommunicationError {
    Codec(Box<dyn Error + Send + Sync>),
    Protocol(ControlProtocolError),    // #[cfg(feature = "transport")]
    InvalidConfig(String),
    InvalidSetup(String),
}

// Control protocol errors (6 variants, transport-only)
enum ControlProtocolError {
    TooShort { len },
    CrcMismatch { expected, actual },
    InvalidPayloadSize { expected, actual },
    InvalidUtf8 { source: Utf8Error },
    UnknownMessageType { msg_type },
    WireRead(Box<Error>),
}
```

**`CodecError`** (separate from the hierarchy, in `communication::codec`):

```rust
enum CodecError {
    InsufficientData { needed, available },
    InvalidData(String),
    Custom(String),
    CrcMismatch { expected: u32, actual: u32 },
    PayloadTooLarge { size: usize, max: usize },
    External(Box<dyn Error + Send + Sync>),
}
```

`CodecError` is intentionally **not** nested under `CommunicationError`. It is used
directly by the `Codec<T>` trait and user-defined codecs, so it stays independent.

### 8.2 Error Organization Principles

When adding new error variants, follow these rules:

1. **Sub-enums correspond to source modules.** If an error originates from a single
   module (e.g., topology, dataflow, runtime, communication), define it in that
   module's sub-enum.

2. **Consolidate similar errors with context fields.** Use fields like `operator`,
   `port`, `node_id` to distinguish instances — do not create a separate variant
   for every unique error message.

3. **No generic catch-all.** Do not add `Internal(String)`, `Custom(String)`, or
   similar. Every error must have a specific meaning that callers can match on.

4. **Cross-cutting errors stay at the root.** An error that occurs in 3+ unrelated
   modules (e.g., `LockPoisoned`, `ChannelClosed`) belongs in the root `Error` enum.

5. **Test-only errors** should be gated with `#[cfg(feature = "test-utils")]` if
   they exist solely to support test infrastructure.

6. **Enum variant vs. `String` field.** Not every error field needs to be a
   structured type. Prefer an enum (or typed field) over a plain `String` when
   any of the following apply — listed from most to least important:

   - **Programmatic matching** — callers need to `match` on a specific error
     case to choose a recovery strategy (e.g., retry on timeout, abort on auth
     failure). If callers only log the message, a `String` is fine.
   - **Statistical analysis** — the application aggregates errors as metric
     labels or error codes in dashboards. Enum discriminants are stable keys;
     free-form strings are not.
   - **Readability** — a named variant like `CrcMismatch { expected, actual }`
     is easier to understand at the call site than `Custom(format!("CRC
     mismatch: expected {expected}, got {actual}"))`.

   When none of these apply — e.g., the field is a human-readable reason for
   a validation failure that callers never inspect programmatically — keep it
   as `String` to avoid unnecessary boilerplate.

### 8.3 Error Propagation

- Operators return `Result<(), Error>`.
- When an operator task fails, it drops its output channels and capabilities.
- Downstream operators observe closed channels and can decide to propagate or handle the error.
- The `execute()` function collects errors from all worker tasks and returns them.
- **No panics** in library code. All `unwrap()` calls replaced with `?` or explicit error handling.

### 8.4 Poisoned Lock Handling

Rust `Mutex`/`RwLock` become **poisoned** when a thread panics while holding the
lock. instancy handles poison errors using five patterns, chosen based on the
lock's scope and the caller's ability to propagate errors.

#### 8.4.1 `or_poison()` Extension Trait

The `LockResultExt::or_poison(context)` trait method converts
`PoisonError<T>` → `Error::LockPoisoned { context }`. It is the preferred
conversion mechanism for all lock sites that can return `Result`.

#### 8.4.2 Handling Patterns

| Pattern | When to use | Example sites |
|---|---|---|
| **`or_poison()` + `?`** | API returns `Result`. Poison surfaces to the caller as a typed error. | `Session::allocate_channel`, `PeerPool::select_connection`, `ProgressReceiver::drain_all` |
| **`or_poison()` + `match` + safe fallback** | Closure signature cannot return `Result` (e.g., `FnMut → bool`). Use a neutral fallback value (`false`, `return`, etc.) and add a `// NOTE:` comment explaining the constraint. | `branch()` predicate → `false`, `inspect_collected` → `return`, `ProgressSender::send` → drop batch |
| **`or_poison()` + abort** | Fire-and-forget helper storing task handles. On poison, abort the handle to prevent resource leak. | `push_task_handle` in `shared_transport.rs` |
| **`into_inner()` recovery** | Shared runtime infrastructure where the data is safe to recover (write-once, simple value types). The lock must not block future dataflows. | `PeerRegistry::state`, `live_topology`, `membership_cancel`, `CancellationToken::reason` |
| **`match lock() { Err(_) ⇒ return default }`** | Background housekeeping that can skip a cycle without harm. | `ConnectionPool::evict_idle`, `health_check`, `stats` |

#### 8.4.3 Isolation Guarantees

**Per-dataflow locks** — `Session`, `ProgressSender/Receiver`, `SharedTransportSession` —
are created per dataflow and dropped when it completes. A poisoned per-dataflow
lock affects only that dataflow; it cannot contaminate future submissions.

**Shared runtime locks** — `PeerRegistry`, `live_topology`, `membership_cancel` —
use `into_inner()` recovery. The data behind these locks is safe to recover
(registration maps, topology snapshots, cancellation tokens). This ensures that
a panic in one dataflow never poisons shared runtime state permanently.

**Shared transport locks** — `PeerPool` — propagate poison via `or_poison()` + `?`.
If a transport thread panics and poisons the pool, subsequent calls to
`select_connection()` / `add_connection()` return `Err(LockPoisoned)`. The
transport layer distinguishes poison from normal connection failures using
`TransportError::LockPoisoned` (instead of `ConnectionClosed`) and emits a
`RuntimeEvent::TransportDegraded` on the health event channel. Affected
dataflows are cancelled with `CancellationReason::InternalError`. The hosting
application monitors `RuntimeHandle::health_events()` and can decide the
appropriate recovery strategy — e.g., abandon the runtime and create a fresh
`RuntimeHandle`.

#### 8.4.4 Runtime Health Events

When the runtime encounters an error that may fail **all future dataflow
submissions** and cannot self-recover, it emits a `RuntimeEvent` on a broadcast
channel. This is the general notification mechanism for unrecoverable runtime
degradation — PeerPool poison is one example, but any future unrecoverable
condition should follow the same pattern.

```rust
// Hosting application subscribes to health events
let mut health_rx = runtime.health_events();
tokio::spawn(async move {
    while let Ok(event) = health_rx.recv().await {
        tracing::error!("Runtime degraded: {event}");
        runtime_handle.shutdown();
        // Create a fresh RuntimeHandle to replace the degraded one.
    }
});
```

Key design properties:

- **Broadcast** — multiple subscribers can coexist; each gets an independent
  receiver. If no subscriber exists, events are silently dropped.
- **Non-blocking** — event emission never blocks the transport or runtime.
- **Feature-agnostic** — `RuntimeEvent` and `health_events()` are available
  regardless of feature flags, even though currently only the transport layer
  emits events.
- **Composable** — the hosting application owns the recovery policy. The
  runtime reports the problem but does not auto-restart or self-heal.

#### 8.4.5 Resource Leak Prevention

Poison handling must never leak resources:

- **Task handles**: On poison, `abort()` the handle rather than dropping it silently.
- **Progress batches**: Dropping a batch on poison is acceptable — data is lost
  but no resource leak occurs (the dataflow is tearing down).
- **Connections**: TCP connections are managed by transport tasks, not by the
  poisoned lock's guard. Connection cleanup proceeds via task cancellation.
- **Closures returning `()`**: When a closure cannot return `Result`, use `return`
  to skip work rather than proceeding with potentially corrupt state.

#### 8.4.6 Raw `.unwrap()` on Locks

Raw `.lock().unwrap()` is allowed **only in test code** (`#[cfg(test)]` or
`tests/` directories). Production code must use one of the five patterns above.

### 8.5 No Global State

The instancy crate contains **zero** `static`, `lazy_static`, `once_cell`,
`OnceLock`, `LazyLock`, or `thread_local!` variables in production code. All
mutable state is rooted in `RuntimeHandle` instances. This guarantees:

- **Full isolation** — multiple `RuntimeHandle` instances in the same process
  share no state and cannot interfere with each other.
- **Clean shutdown** — dropping a `RuntimeHandle` releases all resources.
  No leaked state persists in global variables after the runtime is gone.
- **Restartability** — the hosting application can abandon a poisoned or
  degraded runtime and create a fresh one with no residual contamination.

`&'static str` references (e.g., type names in port descriptors) and `static`
items inside `#[cfg(test)]` blocks are permitted — they carry no mutable state.

---


## Panic-removal design notes

The old standalone panic-removal design is consolidated here. It explains how existing panic sites are classified and which conversion pattern applies to each category.

## Design: Remove Panics from Production Code

**Item:** `api-panic-removal`
**Priority:** P1
**Status:** Design

### Problem

instancy has **195 production-code panic sites** (`expect()`, `unwrap()`, `assert!()`, `panic!()`) across 42 source files. These violate the project's design requirement of "proper error handling instead of panicking." A single bad input, poisoned mutex, or unexpected state causes the entire process to abort instead of propagating an error the caller can handle.

### Scope

Every `expect()`, `unwrap()`, `assert!()`, and `panic!()` in non-test, non-doc-comment production code. Files ending in `_tests.rs` and code inside `#[cfg(test)]` blocks are excluded.

### Categories & Strategy

After auditing all 195 sites, they fall into 6 categories. Each category gets a different treatment:

#### Category 1: Lock Poisoning (31 sites) — Convert to `Error::Custom`

**Pattern:** `.expect("... lock poisoned")`
**Files:** `shared_pool.rs` (14), `shared_transport.rs` (8), `progress_channel.rs` (3), `network_progress.rs` (1), `dataflow_builder.rs` (2), `session.rs` (1), `inspect.rs` (1), `executor_task.rs` (1)

**Strategy:** Create a helper function and convert all sites:

```rust
// In error.rs:
impl Error {
    /// Create an error from a poisoned lock.
    pub fn lock_poisoned(context: &str) -> Self {
        Self::Custom(format!("lock poisoned: {context}"))
    }
}

// Helper trait to convert PoisonError:
pub(crate) trait LockResultExt<T> {
    fn or_poison(self, context: &str) -> Result<T, Error>;
}

impl<T> LockResultExt<T> for Result<T, std::sync::PoisonError<T>> {
    fn or_poison(self, context: &str) -> Result<T, Error> {
        self.map_err(|_| Error::lock_poisoned(context))
    }
}
```

**Conversion:**
```rust
// Before:
let guard = lock.lock().expect("peer pool connections lock poisoned");
// After:
let guard = lock.lock().or_poison("peer pool connections")?;
```

#### Category 2: Wire Format Parsing (35 sites) — Convert to `Error::Codec`

**Pattern:** `bytes[a..b].try_into().expect("X is N bytes")`
**Files:** `interprocess.rs` (10), `network_progress.rs` (8), `network.rs` (7), `sequencing.rs` (4), `transport.rs` (3), `control_protocol.rs` (3), `mock_network.rs` (4), `probing.rs` (2)

These parse fixed-width fields from byte buffers. The `try_into()` for `[u8; N]` is guaranteed to succeed when the slice length is correct, BUT the slice bounds are only correct if the outer length check passed. A corrupted length field could cause a panic.

**Strategy:** Create a helper for safe fixed-width extraction:

```rust
// In a new module: src/wire.rs (or in codec.rs)
pub(crate) fn read_u32(buf: &[u8], offset: usize) -> Result<u32, Error> {
    buf.get(offset..offset + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| Error::Codec(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("expected 4 bytes at offset {offset}, got {}", buf.len().saturating_sub(offset)),
        ))))
}

pub(crate) fn read_u64(buf: &[u8], offset: usize) -> Result<u64, Error> { /* similar */ }
pub(crate) fn read_bytes(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], Error> { /* similar */ }
```

**Conversion:**
```rust
// Before:
let count = u32::from_le_bytes(rest[..4].try_into().expect("batch count prefix is 4 bytes"));
// After:
let count = wire::read_u32(rest, 0)?;
```

#### Category 3: Builder Validation (28 sites) — Convert to `Result<_, Error>`

**Pattern:** `assert!(condition, "message")` in builder/configuration methods
**Files:** `dataflow_builder.rs` (16), `exchange.rs` (2), `broadcast.rs` (2), `rebalance.rs` (1), `tee.rs` (1), `worker.rs` (2), `concat.rs` (1), `pact.rs` (1), `batching.rs` (1), `control.rs` (1)

**Strategy:** Change return types from `T` to `Result<T, Error>` where the method currently returns a value, or add `Result<(), Error>` where it returns `()`. Use a new `Error::InvalidConfig` variant:

```rust
// In error.rs — new variant:
/// A configuration or topology error detected at build time.
##[error("Configuration error: {0}")]
InvalidConfig(String),
```

**Conversion:**
```rust
// Before:
assert!(target_parallelism > 0, "target_parallelism must be > 0");
// After:
if target_parallelism == 0 {
    return Err(Error::InvalidConfig("target_parallelism must be > 0".into()));
}
```

For `assert!` in `dataflow_builder.rs` for duplicate input/output port names:
```rust
// Before:
assert!(!state.input_ports.iter().any(|p| p.name == name), "duplicate input port name: {name}");
// After:
if state.input_ports.iter().any(|p| p.name == name) {
    return Err(Error::InvalidConfig(format!("duplicate input port name: {name}")));
}
```

#### Category 4: Internal Invariants (42 sites) — Split into valid vs. convertible

**Pattern:** `.expect("X exists after registration")`, `assert!(self.initialized, ...)`
**Files:** `subgraph.rs` (7), `executor.rs` (8), `scope.rs` (5), `reachability.rs` (4), `exchange_channel.rs` (4), `runtime.rs` (10+), various operators (10+)

These guard internal state that should always be consistent if the library is correct. Split into three sub-categories:

**4a. VALID — Leave as-is (mathematically guaranteed, ~20 sites):**

These are provably infallible because the access immediately follows the mutation that guarantees the value exists:

| Pattern | Example | Why valid |
|---------|---------|-----------|
| HashMap get after insert | `subgraph.rs:142` — `.get(&index)` right after `.insert(index, ...)` | Key was just inserted on line 138 |
| `pop()` after `peek().is_some_and()` | `reachability.rs:726` — `.pop().expect(...)` inside `while peek().is_some_and(...)` | peek confirmed entry exists |
| `pop()` after `len == 1` check | `tee.rs:201` — `pushers.pop()` in `1 => Some(...)` arm | match confirmed exactly 1 element |
| `find()` after validation loop | `capability.rs:227` — `.find(|c| c.time.less_equal(&t))` after frontier_times validated | prior loop validated all times exist |
| `pop_front()` after peek readiness | `task_scheduler.rs:185` — `.pop_front()` after front element checked | readiness guard on line 178 |
| Fresh graph registration | `scope.rs:302` — `.expect("fresh graph cannot fail")` | Brand new empty graph, index 0 always succeeds |
| Just-allocated index | `scope.rs:162,333` — `.expect("just allocated, cannot conflict")` | Index allocated on previous line |
| `reachability.rs:197,205` | `assert_eq!(summary.inputs(), inputs, ...)` | API contract validation in public method |

**Action:** Keep these as `expect()` but add `// SAFETY: <reason>` comments documenting why they're infallible.

**4b. VALID-ISH — Wire format after length check (~35 sites, overlaps Category 2):**

The `interprocess.rs` pattern: `bytes[offset..offset+4].try_into().expect("4 bytes")` where the bounds were checked by `if offset + 4 > bytes.len()` on the line above. These are technically valid — the length check guarantees the slice is the right size. However, they're fragile: a future refactor could move the check and introduce a bug.

**Action:** Still convert to `wire::read_*` helpers for robustness, but acknowledge they're currently correct.

**4c. CONVERTIBLE — State that could be invalid through misuse (~22 sites):**

These access state through `Option::take()` or cross-function invariants where a user could trigger the failure:

```rust
// Before:
self.inner.take().expect("join called after move")
// After:  
self.inner.take().ok_or_else(|| Error::Custom("already joined".into()))?
```

```rust
// Before:
let df_cancel = dataflow_cancel.as_ref().expect("dataflow_cancel set for multi-worker").clone();
// After:
let df_cancel = dataflow_cancel.as_ref()
    .ok_or_else(|| Error::Custom("internal: dataflow_cancel not set for multi-worker spawn".into()))?
    .clone();
```

#### Category 5: Write-to-String (15 sites) — Leave as-is (truly infallible)

**Pattern:** `writeln!(out, ...).expect("write to String is infallible")`
**Files:** `graph.rs` (15)

Writing to a `String` via `fmt::Write` cannot fail. The `expect()` message is accurate. These are the one exception where `expect()` is correct.

**Decision:** Leave these as-is. Add a code comment noting they are audited-safe.

#### Category 6: Option/Result after validation (44 sites) — Convert to `ok_or_else`

**Pattern:** `.expect("X valid after check")`, `state.result.take().expect("result available")`
**Files:** `runtime.rs` (22), `capability.rs` (2), `task_scheduler.rs` (1), `connection.rs` (2), `cancellation.rs` (1), operators (16 `.expect("operator index should be unique")`)

**Strategy:** Use `.ok_or_else(|| Error::...)` with `?` propagation:

```rust
// Before:
let df_cancel = dataflow_cancel.as_ref().expect("dataflow_cancel set for multi-worker").clone();
// After:
let df_cancel = dataflow_cancel.as_ref()
    .ok_or_else(|| Error::Custom("internal: dataflow_cancel not set for multi-worker spawn".into()))?
    .clone();
```

For the repeated `.expect("operator index should be unique")` in all operator files:
```rust
// Create a helper on DataflowGraph:
impl DataflowGraph {
    pub(crate) fn insert_stage_checked(&mut self, idx: OperatorIndex, info: StageInfo) -> Result<(), Error> {
        if self.stages.contains_key(&idx) {
            return Err(Error::InvalidConfig(format!("duplicate operator index: {:?}", idx)));
        }
        self.stages.insert(idx, info);
        Ok(())
    }
}
```

### New Error Variants

Add to `Error` enum:
```rust
/// A configuration or topology error detected at build time.
##[error("Configuration error: {0}")]
InvalidConfig(String),
```

### New Helper Module

Create `src/wire.rs` with safe byte-parsing helpers:
- `read_u32(buf, offset) -> Result<u32, Error>`
- `read_u64(buf, offset) -> Result<u64, Error>`
- `read_i64(buf, offset) -> Result<i64, Error>`
- `read_bytes(buf, offset, len) -> Result<&[u8], Error>`
- `read_array<const N: usize>(buf, offset) -> Result<[u8; N], Error>`

Create `LockResultExt` trait in `src/error.rs` for lock poisoning conversion.

Create `insert_stage_checked()` helper on `DataflowGraph` for operator index dedup.

### Signature Changes

Methods whose return type changes from `T` to `Result<T, Error>`:

| File | Method | Current Return | New Return |
|------|--------|---------------|------------|
| `dataflow_builder.rs` | `add_input_port` | `InputPortId` | `Result<InputPortId, Error>` |
| `dataflow_builder.rs` | `add_output_port` | `OutputPortId` | `Result<OutputPortId, Error>` |
| `dataflow_builder.rs` | various `_with_parallelism` | `Pipe<T>` | `Result<Pipe<T>, Error>` |
| `executor.rs` | `materialize_edges` | `()` | `Result<(), Error>` |
| `runtime.rs` | `take_input` etc | `Sender<T>` | `Result<Sender<T>, Error>` |
| `runtime.rs` | `join` | `DataflowResult` | `DataflowResult` (already Result) |
| `scope.rs` | `enter` / `leave` | `ScopeId` | `Result<ScopeId, Error>` |
| Various operators | builder methods | `Pipe<T>` | `Result<Pipe<T>, Error>` |
| `shared_pool.rs` | most methods | `T` | `Result<T, Error>` |
| `shared_transport.rs` | most methods | `T` | `Result<T, Error>` |
| Wire format parsers | `decode_*` | `T` | `Result<T, Error>` |

**Note:** Many of these methods already return `Result`. The change is to push `?` through internal calls that currently `expect()`.

### Migration Impact

- **Breaking API changes:** Some public builder methods will change return type. Since we're also doing `api-visibility` next, we can minimize breakage by restricting most of these to `pub(crate)`.
- **All existing tests** will need updating where they call changed methods.
- **Examples** will need `.unwrap()` or `?` added where they call changed methods.

### Summary

| Category | Sites | Action | Risk |
|----------|-------|--------|------|
| Lock poisoning | 31 | `LockResultExt` trait | Low |
| Wire parsing | 35 | `wire::read_*` helpers | Low |
| Builder validation | 28 | `if !cond { return Err }` | Medium (API change) |
| Internal invariants | 42 | `ok_or_else` + `?` | Low |
| Write-to-String | 15 | Leave as-is | None |
| Option after validation | 44 | `ok_or_else` + `?` | Low |
| **Total** | **195** | **180 converted, 15 kept** | |

### Testing

- All 1200+ existing tests must pass
- All 39 integration tests must pass
- `cargo clippy --all-features --tests -- -D warnings` must pass
- Add unit tests for new `wire::read_*` helpers with truncated/empty input
- Add unit tests for `LockResultExt`
- Run stress test to verify no regressions
