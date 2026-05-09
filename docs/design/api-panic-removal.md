# Design: Remove Panics from Production Code

**Item:** `api-panic-removal`
**Priority:** P1
**Status:** Design

## Problem

instancy has **195 production-code panic sites** (`expect()`, `unwrap()`, `assert!()`, `panic!()`) across 42 source files. These violate the project's design requirement of "proper error handling instead of panicking." A single bad input, poisoned mutex, or unexpected state causes the entire process to abort instead of propagating an error the caller can handle.

## Scope

Every `expect()`, `unwrap()`, `assert!()`, and `panic!()` in non-test, non-doc-comment production code. Files ending in `_tests.rs` and code inside `#[cfg(test)]` blocks are excluded.

## Categories & Strategy

After auditing all 195 sites, they fall into 6 categories. Each category gets a different treatment:

### Category 1: Lock Poisoning (31 sites) — Convert to `Error::Custom`

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

### Category 2: Wire Format Parsing (35 sites) — Convert to `Error::Codec`

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

### Category 3: Builder Validation (28 sites) — Convert to `Result<_, Error>`

**Pattern:** `assert!(condition, "message")` in builder/configuration methods
**Files:** `dataflow_builder.rs` (16), `exchange.rs` (2), `broadcast.rs` (2), `rebalance.rs` (1), `tee.rs` (1), `worker.rs` (2), `concat.rs` (1), `pact.rs` (1), `batching.rs` (1), `control.rs` (1)

**Strategy:** Change return types from `T` to `Result<T, Error>` where the method currently returns a value, or add `Result<(), Error>` where it returns `()`. Use a new `Error::InvalidConfig` variant:

```rust
// In error.rs — new variant:
/// A configuration or topology error detected at build time.
#[error("Configuration error: {0}")]
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

### Category 4: Internal Invariants (42 sites) — Split into valid vs. convertible

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

### Category 5: Write-to-String (15 sites) — Leave as-is (truly infallible)

**Pattern:** `writeln!(out, ...).expect("write to String is infallible")`
**Files:** `graph.rs` (15)

Writing to a `String` via `fmt::Write` cannot fail. The `expect()` message is accurate. These are the one exception where `expect()` is correct.

**Decision:** Leave these as-is. Add a code comment noting they are audited-safe.

### Category 6: Option/Result after validation (44 sites) — Convert to `ok_or_else`

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

## New Error Variants

Add to `Error` enum:
```rust
/// A configuration or topology error detected at build time.
#[error("Configuration error: {0}")]
InvalidConfig(String),
```

## New Helper Module

Create `src/wire.rs` with safe byte-parsing helpers:
- `read_u32(buf, offset) -> Result<u32, Error>`
- `read_u64(buf, offset) -> Result<u64, Error>`
- `read_i64(buf, offset) -> Result<i64, Error>`
- `read_bytes(buf, offset, len) -> Result<&[u8], Error>`
- `read_array<const N: usize>(buf, offset) -> Result<[u8; N], Error>`

Create `LockResultExt` trait in `src/error.rs` for lock poisoning conversion.

Create `insert_stage_checked()` helper on `DataflowGraph` for operator index dedup.

## Signature Changes

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

## Migration Impact

- **Breaking API changes:** Some public builder methods will change return type. Since we're also doing `api-visibility` next, we can minimize breakage by restricting most of these to `pub(crate)`.
- **All existing tests** will need updating where they call changed methods.
- **Examples** will need `.unwrap()` or `?` added where they call changed methods.

## Summary

| Category | Sites | Action | Risk |
|----------|-------|--------|------|
| Lock poisoning | 31 | `LockResultExt` trait | Low |
| Wire parsing | 35 | `wire::read_*` helpers | Low |
| Builder validation | 28 | `if !cond { return Err }` | Medium (API change) |
| Internal invariants | 42 | `ok_or_else` + `?` | Low |
| Write-to-String | 15 | Leave as-is | None |
| Option after validation | 44 | `ok_or_else` + `?` | Low |
| **Total** | **195** | **180 converted, 15 kept** | |

## Testing

- All 1200+ existing tests must pass
- All 39 integration tests must pass
- `cargo clippy --all-features --tests -- -D warnings` must pass
- Add unit tests for new `wire::read_*` helpers with truncated/empty input
- Add unit tests for `LockResultExt`
- Run stress test to verify no regressions
