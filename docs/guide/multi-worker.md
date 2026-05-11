# Multi-Worker Execution

`spawn_multi` replicates a dataflow graph across logical workers and lets repartition operators move data between them. Use this page when a single worker is no longer enough.

[Back to the guide index](./README.md)

For parallel processing, instancy can run multiple logical workers that partition data across them.

### spawn_multi

`spawn_multi` creates N replicated workers, each running the same dataflow graph but processing different partitions of data:

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

let mut multi = rt.spawn_multi("wordcount", 2, |builder| {
    let input = builder.input::<String>("lines");
    input
        .flat_map("split", |_t, line| {
            line.split_whitespace().map(String::from).collect()
        })
        .exchange_by_hash("partition", |word: &String| {
            // Hash the word to decide which worker handles it
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            word.hash(&mut h);
            h.finish()
        })
        .unary("count", {
            let mut counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            move |input, output| {
                while let Some((time, words)) = input.next() {
                    for w in words {
                        *counts.entry(w).or_insert(0) += 1;
                    }
                }
                // Emit current state
                let pairs: Vec<_> = counts.iter()
                    .map(|(k, v)| (k.clone(), *v)).collect();
                if !pairs.is_empty() {
                    output.push_vec(0, pairs);
                }
                Ok(())
            }
        })
        .output("counts");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

Each worker independently builds and runs the same graph. The `exchange_by_hash` operator is what makes this powerful: it repartitions data across workers by key, ensuring all occurrences of the same word end up at the same worker regardless of which input they came from.

#### Auto-Parallelism

By default, `SpawnOptions` enables **auto-parallelism** — stage 0 parallelism is automatically detected from the number of `input()` and `source_async()` calls in the graph. The `num_workers` argument acts as a minimum floor: `effective = max(auto_detected, num_workers)`.

```rust
// Auto-parallelism: 1 input → auto_detected=1, effective = max(1, 4) = 4 workers
let multi = rt.spawn_multi("pipeline", 4, |builder| {
    builder.input::<i32>("data")
        .map("inc", |_t, x| x + 1)
        .output("out");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

Pass `num_workers=0` to use only the auto-detected count. Disable auto-parallelism with `SpawnOptions::new().auto_parallelism(false)` to use the exact `num_workers` for stage 0. To force uniform parallelism across *all* stages, also set `per_stage_parallelism(false)`.

### Exchange Operators

Exchange operators physically move data between workers based on a routing function:

```rust
// Route by hash of the value
stream.exchange_by_hash("route", |x: &MyType| compute_hash(x));

// Route by a key function (applies DefaultHasher on top)
stream.exchange("route", |x: &MyType| x.key.clone());
```

**When do you need exchange?** Only when your computation requires specific data distribution. For example:
- **Word count** — all occurrences of "hello" must reach the same worker
- **Graph algorithms** — all edges for a vertex must be co-located
- **Joins** — matching keys must meet at the same worker

If your operators are stateless (like `map` or `filter`), you don't need exchange — any worker can process any element.

### Distribution Operators

Beyond `exchange`, instancy provides three convenience distribution operators
for common routing patterns:

#### Gather

Route **all** data to worker 0. Useful for global aggregation:

```rust
stream
    .exchange_by_hash("partition", |x| x.key)
    .gather("collect")
    .reduce("global_sum", |acc, x| acc + x)
    .output("total");
```

After `gather`, only worker 0 has data — other workers receive nothing.

#### Rebalance

Distribute data round-robin across all workers. Useful for evening out load
when key-based partitioning isn't needed:

```rust
// All items from worker 0 get spread evenly across all workers
stream.rebalance("spread").output("results");
```

Unlike `exchange`, `rebalance` doesn't look at the data — it assigns workers
sequentially (item 0 → worker 0, item 1 → worker 1, ..., wrapping around).

#### Rebalance To

Like `rebalance`, but with explicit target parallelism:

```rust
// Round-robin to exactly 4 workers
stream.rebalance_to("spread", 4).output("results");
```

> **Note:** With [`SpawnOptions::per_stage_parallelism`] enabled (the default),
> `rebalance_to(N)` can use a different `N` per stage. With
> `per_stage_parallelism(false)`, `N` must equal the spawned worker count.

#### Broadcast

Clone all data to **every** worker (fan-out). Useful for distributing small
reference data, configuration, or lookup tables that every worker needs:

```rust
// Every worker gets a complete copy of the config stream
let config = config_stream.broadcast("share-config");
config
    .map("use-config", |_t, cfg| apply_config(cfg))
    .output("results");
```

> **Warning:** Broadcast multiplies data volume by the worker count. Only use
> for small datasets or control signals — never for large data streams.

In single-worker mode, `broadcast` is a no-op pass-through (no cloning needed).

### Delay Operators

The `delay` operators buffer data and re-assign timestamps, releasing the data
only when the input frontier advances past the **new** (delayed) timestamp. This is
essential for windowing, time-based aggregation, and ensuring data is processed
in timestamp order.

#### delay_batch

Re-timestamp all data at a given timestamp to a new timestamp computed from the
original timestamp. Simpler version when the delay depends only on the timestamp:

```rust
// Group data into 100-unit windows
let windowed = stream.delay_batch("window-100", |t| (t / 100 + 1) * 100);

// Shift all timestamps forward by 10
let shifted = stream.delay_batch("shift", |t| t + 10);

// Identity: buffer until frontier confirms no more data at t
let ordered = stream.delay_batch("order", |t| *t);
```

#### delay

Per-item re-timestamp: each item can be assigned a different timestamp based on
its content:

```rust
// High-priority items stay at current timestamp; low-priority delayed
let prioritized = stream.delay("prioritize", |t, item| {
    if item.priority > 5 { *t } else { *t + 10 }
});
```

> **Constraint:** The delay function must return a timestamp `>=` the input
> timestamp. Returning an earlier timestamp will panic.

### Cross-Worker Error Propagation

In multi-worker dataflows, if one worker's operator fails, all sibling workers
are automatically cancelled via the built-in **control broadcast channel**.
You don't need to wire up manual error forwarding — instancy handles it:

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let mut multi = rt.spawn_multi("my-pipeline", 4, |builder| {
    let input = builder.input::<String>("data");
    input.map("process", |_t, line| {
        // If this panics in any worker, all 4 workers cancel promptly.
        parse_line(&line).expect("bad input")
    }).output("result");
    Ok(())
}, SpawnOptions::default()).unwrap();
```

When worker 2's `process` operator panics, instancy:
1. Catches the error and broadcasts a `WorkerControl::WorkerError` signal.
2. Cancels the shared dataflow `CancellationToken`.
3. All other workers see `Err(Cancelled)` on their next sweep and exit.

The `join_blocking()` call returns the first error, with full operator and
worker context attached.

## Ordering and Completion Notes

### Output arrives out of order
- Pipeline channels preserve ordering within a timestamp
- Exchange channels may reorder across workers — use `unary_notify` to aggregate per-timestamp

## Related Examples

- [`partitioned_workers.rs`](../../instancy/examples/partitioned_workers.rs)
- [`exchange.rs`](../../instancy/examples/exchange.rs)
- [`exchange_wordcount.rs`](../../instancy/examples/exchange_wordcount.rs)
- [`stage_parallelism.rs`](../../instancy/examples/stage_parallelism.rs)
- [`runtime_isolation.rs`](../../instancy/examples/runtime_isolation.rs)

## Next Steps

- Next: [Iteration](./iteration.md)
- See also: [Distributed Execution](./distributed.md), [Cookbook](../cookbook.md)
