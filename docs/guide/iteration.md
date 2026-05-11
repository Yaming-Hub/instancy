# Iteration

instancy models loops with nested scopes and `Product` timestamps. This page explains how feedback edges terminate and how to reason about outer versus inner time.

[Back to the guide index](./README.md)

instancy supports iterative computation through the `iterate` operator, which creates a feedback loop in the dataflow graph.

### Basic Iteration

```rust
use instancy::IterateResult;

let result = stream.iterate::<u32>("loop", 1u32, |iter_stream| {
    // Transform data each iteration
    let doubled = iter_stream.map("double", |_t, x| x * 2);

    // Split: values >= 100 exit, values < 100 loop back
    let done = doubled.clone().filter("exit", |_t, &x| x >= 100);
    let again = doubled.filter("continue", |_t, &x| x < 100);

    IterateResult {
        feedback: again,  // Goes back to the start of the loop
        output: done,     // Exits the loop
    }
});
```

The `iterate` operator:
1. Creates a nested scope with an enriched timestamp `Product<TOuter, TInner>` — the outer timestamp is your original timestamp, and the inner is the iteration counter
2. Feeds data into the loop body
3. Each iteration, the `feedback` stream circulates back to the start
4. The `output` stream exits the loop and continues downstream
5. The loop terminates when no more data circulates through `feedback`

The second argument (`1u32`) specifies the increment for the iteration counter each time around the loop.

### Understanding Product Timestamps

Inside an `iterate` loop, timestamps become `Product<TOuter, TInner>`:

```rust
stream.iterate::<u32>("my_loop", 1u32, |iter_stream| {
    iter_stream.map("debug", |time, x| {
        // time is Product<u64, u32>
        // time.outer = original timestamp (e.g., 0, 1, 2)
        // time.inner = iteration number (0, 1, 2, ...)
        println!("iteration {}, original time {}: {x}", time.inner, time.outer);
        x
    });
    // ...
});
```

This is important for stateful operators inside loops — use `time.inner` to know which iteration you're in, and `time.outer` to distinguish different input epochs.

### Iteration with Exchange

You can combine iteration with exchange for distributed iterative algorithms. This requires `Product` timestamps to be serializable, which instancy supports:

```rust
// Inside iterate, exchange data across workers each iteration
let result = stream.iterate::<u32>("distributed_loop", 1u32, |iter_stream| {
    let exchanged = iter_stream.exchange_by_hash("route", |x: &u64| *x);
    let processed = exchanged.map("step", |_t, x| x + 1);
    let done = processed.clone().filter("exit", |_t, &x| x >= threshold);
    let again = processed.filter("continue", |_t, &x| x < threshold);
    IterateResult { feedback: again, output: done }
});
```

This pattern — iterate with exchange — is the basis for graph algorithms like BFS and PageRank. See the `bfs.rs`, `pagerank.rs`, and `unionfind.rs` examples for complete implementations.

## Related Examples

- [`loop_demo.rs`](../../instancy/examples/loop_demo.rs)
- [`pagerank.rs`](../../instancy/examples/pagerank.rs)
- [`bfs.rs`](../../instancy/examples/bfs.rs)
- [`unionfind.rs`](../../instancy/examples/unionfind.rs)

## Next Steps

- Next: [Distributed Execution](./distributed.md)
- See also: [Core Concepts](./core-concepts.md), [API Reference](../reference/api.md)
