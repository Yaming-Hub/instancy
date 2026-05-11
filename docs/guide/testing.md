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

## Related Examples

- [`runtime_isolation.rs`](../../instancy/examples/runtime_isolation.rs)
- [`partitioned_workers.rs`](../../instancy/examples/partitioned_workers.rs)
- [`cluster_basic.rs`](../../instancy/examples/cluster_basic.rs)

## Next Steps

- Next: [Cookbook](../cookbook.md)
- See also: [Distributed Execution](./distributed.md), [API Reference](../reference/api.md)
