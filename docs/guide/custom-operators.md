# Custom Operators

When the built-in operators are not enough, instancy lets you define stateful logic with `unary`, `binary`, and `unary_notify`. This page shows the core operator hooks and the most common stateful patterns.

[Back to the guide index](./README.md)

The built-in operators (`map`, `filter`, `flat_map`) cover simple transformations, but real applications need stateful logic. instancy provides `unary`, `binary`, and `unary_notify` for custom operators.

### Unary: One Input, One Output

`unary` gives you full control over how data flows through a single-input operator:

```rust
stream.unary("my_operator", {
    // State lives here — outside the closure, persisting across activations
    let mut seen: Vec<String> = Vec::new();

    move |input, output| {
        while let Some((time, data)) = input.next() {
            for item in data {
                if !seen.contains(&item) {
                    seen.push(item.clone());
                    output.push(time, item);
                }
            }
        }
        Ok(())
    }
});
```

The closure is called whenever new data is available. It receives:
- `input` — an `InputHandle` that yields `(timestamp, Vec<data>)` batches
- `output` — an `OutputHandle` to push results

**State placement matters.** The state (`seen` above) is defined outside the `move` closure but captured by it. This ensures it persists across invocations. If you defined it inside the closure, it would reset every time.

### Binary: Two Inputs, One Output

`binary` joins two streams in a custom operator:

```rust
let joined = left_stream.binary(right_stream, "join", {
    let mut left_state: HashMap<u64, Vec<(String, i32)>> = HashMap::new();
    let mut right_state: HashMap<u64, Vec<(String, i32)>> = HashMap::new();

    move |left_input, right_input, output| {
        // Drain both inputs
        while let Some((time, data)) = left_input.next() {
            left_state.entry(time).or_default().extend(data);
        }
        while let Some((time, data)) = right_input.next() {
            right_state.entry(time).or_default().extend(data);
        }
        // Join matching timestamps
        for (&t, left_items) in &left_state {
            if let Some(right_items) = right_state.get(&t) {
                for (lk, lv) in left_items {
                    for (rk, rv) in right_items {
                        if lk == rk {
                            output.push(t, (lk.clone(), *lv, *rv));
                        }
                    }
                }
            }
        }
        Ok(())
    }
});
```

### Unary Notify: Frontier-Aware Operators

`unary_notify` adds progress awareness — your closure receives a `NotifyContext` that lets you register interest in timestamps and receive notifications when the frontier advances past them:

```rust
stream.unary_notify("aggregate", {
    let mut pending: HashMap<u64, Vec<i32>> = HashMap::new();

    move |input, output, ctx| {
        // Buffer incoming data and request notifications
        while let Some((time, data)) = input.next() {
            pending.entry(time).or_default().extend(data);
            ctx.notify_at(time);  // "Tell me when this epoch is complete"
        }

        // Process notifications — fired when frontier advances past the time
        while let Some(time) = ctx.next_notification() {
            if let Some(data) = pending.remove(&time) {
                let sum: i32 = data.iter().sum();
                output.push(time, sum);
            }
        }
        Ok(())
    }
});
```

This is the instancy equivalent of timely's `Notificator` pattern. The key methods on `NotifyContext` are:
- `notify_at(time)` — register interest in a timestamp; the framework will notify you when the frontier passes it
- `next_notification()` — returns the next completed timestamp (if any)

Use `unary_notify` when you need to produce output only after all data for a timestamp has arrived — for example, computing aggregates, detecting completeness, or triggering downstream actions.

## Stateful Operator Patterns

### Buffer data until a timestamp is complete
Use `unary_notify` when you need exactly one final result per timestamp.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("epoch-sums");
let input = builder.input::<i32>("numbers");

input.unary_notify("sum_per_epoch", {
    let mut pending: HashMap<u64, Vec<i32>> = HashMap::new();
    move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            pending.entry(time).or_default().extend(data);
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            if let Some(data) = pending.remove(&time) {
                output.push_vec(time, vec![data.into_iter().sum()]);
            }
        }
        Ok(())
    }
});
```

### Keep running state across activations
Put mutable state outside the operator closure so it survives every activation.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("running-counts");
let input = builder.input::<String>("words");

input.unary("running_counts", {
    let mut counts: HashMap<String, usize> = HashMap::new();
    move |input, output| {
        while let Some((time, words)) = input.next() {
            let mut snapshot = Vec::new();
            for word in words {
                let n = counts.entry(word.clone()).or_insert(0);
                *n += 1;
                snapshot.push((word, *n));
            }
            output.push_vec(time, snapshot);
        }
        Ok(())
    }
});
```

### Emit batches or windows once an epoch closes
Accumulate all records for a timestamp, then split them into downstream-sized chunks.

```rust
use std::collections::HashMap;
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("batch-on-close");
let input = builder.input::<String>("rows");

input.unary_notify("batch_500", {
    let mut pending: HashMap<u64, Vec<String>> = HashMap::new();
    move |input, output, ctx| {
        while let Some((time, rows)) = input.next() {
            pending.entry(time).or_default().extend(rows);
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            if let Some(rows) = pending.remove(&time) {
                for chunk in rows.chunks(500) {
                    output.push_vec(time, chunk.to_vec());
                }
            }
        }
        Ok(())
    }
});
```

## Choosing `unary` vs `unary_notify`

### Pitfall: using `unary` when you really need `unary_notify`
`unary` emits partial snapshots; `unary_notify` emits one final answer after the frontier passes.

```rust
input.unary_notify("final_sum", {
    let mut stash = std::collections::HashMap::new();
    move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            stash.entry(time).or_insert(0i32);
            *stash.get_mut(&time).unwrap() += data.into_iter().sum::<i32>();
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            output.push_vec(time, vec![stash.remove(&time).unwrap()]);
        }
        Ok(())
    }
});
```

## Related Examples

- [`notify_epoch_stats.rs`](../../instancy/examples/notify_epoch_stats.rs)
- [`notify_wordcount.rs`](../../instancy/examples/notify_wordcount.rs)
- [`hashjoin.rs`](../../instancy/examples/hashjoin.rs)
- [`error_handling.rs`](../../instancy/examples/error_handling.rs)

## Next Steps

- Next: [Multi-Worker Execution](./multi-worker.md)
- See also: [Building Dataflows](./building-dataflows.md), [Error Handling](./error-handling.md)
