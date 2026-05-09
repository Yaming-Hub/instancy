# Design: Unify API Naming Inconsistencies

**Item:** `api-naming`
**Priority:** P1
**Status:** Design

## Problem

The instancy public API has several naming inconsistencies that make the API harder to learn and use.

## Changes

### 1. `RuntimeHandle::shutdown()` → return `Result<()>`

Currently `shutdown()` returns `()` while all other runtime methods return `Result`. Change to:
```rust
pub fn shutdown(&self) -> Result<()>
```

This is consistent with the project goal of proper error handling. The shutdown process could fail (e.g., if the runtime is already shut down).

### 2. `SpawnOptions` — consolidate to builder-only pattern

Currently `SpawnOptions` has both public fields AND builder methods. This is confusing.

**Change:** Make all fields private, keep builder methods as the only way to configure.
Add `pub fn build(self) -> Self` as a no-op terminal if needed, but the real fix is just making fields private and ensuring all fields have builder setters.

### 3. `ClusterSpawnedDataflow` — add missing async take methods

`SpawnedDataflow` and `MultiSpawnedDataflow` have `take_async_input`/`take_async_output`, but `ClusterSpawnedDataflow` is missing them.

**Change:** Add `take_async_input` and `take_async_output` to `ClusterSpawnedDataflow` that delegate to the inner `MultiSpawnedDataflow`.

### 4. Minor: Document `num_local_workers()` vs `num_workers()`

`ClusterSpawnedDataflow` uses `num_local_workers()` while `MultiSpawnedDataflow` uses `num_workers()`. This is actually intentional — cluster has both local and total worker counts. No rename needed, but ensure doc comments make this clear.

## Non-changes (intentionally kept as-is)

- **`take_input` vs `take_async_input`**: The naming is actually correct — `take_input` returns a sync `InputSender`, `take_async_input` returns an `AsyncInputSender`. The "async" prefix distinguishes the async channel variant. This is consistent.
- **`InputSender` vs `AsyncInputSender`**: Consistent naming with `Async` prefix for the async variant.
- **`drain_on_cancel()` vs `drain_timeout` field**: The builder method name describes the *intent* while the field name describes the *mechanism*. After making fields private, users only see the builder method name.

## Testing

- All existing tests must pass
- Clippy clean
- Examples must compile
