# Testing

instancy supports fast unit tests, runtime-level integration tests, and in-process cluster checks. Use this page to choose the lightest-weight test setup that still exercises the behavior you care about.

[Back to the guide index](./README.md)

### Unit test operator logic with `SimpleRuntime`
Enable the `test-utils` feature for lightweight tests: `cargo test --features test-utils`.

```rust
#[cfg(feature = "test-utils")]
#[test]
fn doubles_numbers() {
    use instancy::{DataflowBuilder, SimpleRuntime};

    let rt = SimpleRuntime::new();
    let builder = DataflowBuilder::<u64>::new("unit-test");
    let port = builder
        .source("nums", vec![(0, vec![1, 2, 3])])
        .map("double", |_t, x| x * 2)
        .output("out");

    rt.run(builder.build().unwrap()).unwrap();
    assert_eq!(*port.collector().lock().unwrap(), vec![(0, vec![2, 4, 6])]);
}
```

### Integration test with real runtime handles
Use `RuntimeHandle` when you need spawned inputs/outputs and production-style wiring.

```rust
#[test]
fn end_to_end_runtime_test() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("integration-test");
    builder.input::<i32>("in").map("double", |_t, x| x * 2).output("out");

    let mut handle = rt.spawn(builder.build().unwrap(), SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("in").unwrap();
    let receiver = handle.take_output::<i32>("out").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();
    assert_eq!(receiver.collect_data(), vec![(0, vec![2, 4, 6])]);
}
```

### Test multi-worker dataflows by merging worker outputs
Use `spawn_multi()` plus `take_all_outputs()` and assert on the union of all worker results.

```rust
#[test]
fn counts_across_workers() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt.spawn_multi("mw-test", 2, |builder| {
        builder.input::<i32>("data").exchange_by_hash("route", |x| *x as u64).output("out");
        Ok(())
    }, SpawnOptions::default()).unwrap();

    let senders = multi.take_all_inputs::<i32>("data").unwrap();
    senders[0].send(0, vec![1, 2]).unwrap();
    senders[1].send(0, vec![3, 4]).unwrap();
    drop(senders);
    let outputs = multi.take_all_outputs::<i32>("out").unwrap();
    multi.join_blocking().unwrap();

    let mut all: Vec<i32> = outputs.into_iter().flat_map(|r| r.collect_data().into_iter().flat_map(|(_, d)| d)).collect();
    all.sort();
    assert_eq!(all, vec![1, 2, 3, 4]);
}
```

### Verify ordering explicitly
Pipeline edges preserve arrival order within a timestamp; for exchange-heavy tests, sort before asserting.

```rust
#[test]
fn pipeline_order_is_stable() {
    use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("ordering");
    builder.input::<i32>("in").map("pass", |_t, x| x).output("out");

    let mut handle = rt.spawn(builder.build().unwrap(), SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("in").unwrap();
    let receiver = handle.take_output::<i32>("out").unwrap();
    sender.send(0, vec![3, 1, 2]).unwrap();
    drop(sender);
    handle.join_blocking().unwrap();
    assert_eq!(receiver.collect_data(), vec![(0, vec![3, 1, 2])]);
}
```

## Testing Clusters Locally

### Testing Clusters Locally

You don't need multiple machines to test distributed dataflows. Use in-memory duplex streams:

```rust
use tokio::io::duplex;

// Create a bidirectional in-memory connection
let (a_to_b, b_to_a) = duplex(8192);
let (a_read, a_write) = tokio::io::split(a_to_b);
let (b_read, b_write) = tokio::io::split(b_to_a);
```

This is how instancy's own integration tests work — see `tests/cluster.rs` for examples.

## Progress invariant checker

Every debug build (including every `cargo test` build) contains a progress
invariant checker, `instancy::progress::check`. It watches progress tracking
without changing how a dataflow runs, and records a violation when:

- **V1**: an operator consumes data behind its input frontier;
- **V4**: a worker force-closes or finishes while work is outstanding;
- **V6**: a progress frame breaks the wire rules (a split batch that is not
  positives-first, a batch after end-of-stream);
- **V7**: a worker's frontier claims a location is done while records,
  queued output or capabilities are still there. V7 measures where work is
  from the channels and operator queues themselves, never from the progress
  accounting it checks.

It also reports (**V5**) the non-zero counts a worker's tracker holds when it
finishes. The environment variable `INSTANCY_PROGRESS_CHECK` chooses what a
violation does: `report` (the default: record it and log a `tracing` warning),
`verbose` (also print it), `panic`, or `off`. A test can take
`let mark = check::mark();` before a run and read
`check::violations_since(mark)` after it. Release builds contain no checker
unless the `progress-check` feature is enabled.

The checker does its work on a background thread, so it barely changes a
dataflow's timing, but observing every hook still perturbs it a little. A
test whose outcome depends on a timing bug of today's code can run its
dataflow without the checker with `builder.skip_progress_check()` (behind
`#[cfg(any(debug_assertions, feature = "progress-check"))]`), with a comment
naming the bug and when to re-enable it.

Two test suites build on it:

- `tests/progress_conformance.rs` runs dataflow shapes on every execution
  path, transport, worker count and channel capacity, and checks exact
  outputs plus the checker's findings. How each case fails today is
  recorded in the golden file `tests/progress_conformance.golden`, with
  violations located at an operator port: a case must fail exactly as
  recorded, and a case the file does not list must pass. (Frontier
  findings, V1 `late-data` and V7 `frontier-ahead-of-work`, depend on
  scheduling: one may be missing, or sit at another port the same shape and
  path showed; `CONFORMANCE_STRICT=1` judges them exactly too.) The
  `EXPECTED_FAILURES` table explains every recorded failure by audit
  finding. CI runs a bounded subset; run the whole matrix with
  `cargo test -p instancy --all-features --test progress_conformance -- --ignored --test-threads=1 full_conformance_matrix`.
  The module docs say how to record runs and regenerate the golden file;
  every change to it must cite the audit finding behind it.
- `tests/audit_repros.rs` holds the correctness audit's repro tests. They
  are timing-sensitive races, so they run on demand:
  `cargo test -p instancy --all-features --test audit_repros -- --ignored --exact audit_repros_match_table`
  runs every repro and checks that the ones listed as failing today still
  fail exactly as recorded and the rest pass.

## Related Examples

- [`runtime_isolation.rs`](../../instancy/examples/runtime_isolation.rs)
- [`partitioned_workers.rs`](../../instancy/examples/partitioned_workers.rs)
- [`cluster_basic.rs`](../../instancy/examples/cluster_basic.rs)

## Next Steps

- Next: [Cookbook](../cookbook.md)
- See also: [Distributed Execution](./distributed.md), [API Reference](../reference/api.md)
