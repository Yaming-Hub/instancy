# Error Handling

instancy treats failures as dataflow-level events instead of process-wide panics. This page covers panic recovery, recoverable error streams, cancellation, graceful drain, and common shutdown pitfalls.

[Back to the guide index](./README.md)

## Error Patterns

### Recover from operator panics instead of crashing the process
Turn panics into `join_blocking()` errors with `catch_panics(true)`.

```rust
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let builder = DataflowBuilder::<u64>::new("panic-safe");
builder.catch_panics(true);
builder
    .input::<i32>("data")
    .map("divide", |_t, x| if x == 0 { panic!("boom") } else { 100 / x })
    .output("results");

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let dataflow = builder.build().unwrap();
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
handle.take_input::<i32>("data").unwrap().send(0, vec![10, 0, 5]).unwrap();
match handle.join_blocking() {
    Ok(()) => unreachable!(),
    Err(err) => eprintln!("pipeline stopped cleanly: {err}"),
}
```

### Propagate recoverable errors through the pipeline
Model failures as `Result<T, E>` when you want dataflow-level recovery instead of immediate shutdown.

```rust
use instancy::DataflowBuilder;

let builder = DataflowBuilder::<u64>::new("recoverable-errors");
let input = builder.input::<Result<String, String>>("raw");
let parsed = input
    .and_then("parse", |_t, s| s.parse::<i32>().map_err(|e| e.to_string()))
    .filter_ok("positive", |_t, v| *v > 0);
let (good, bad) = parsed.branch_result("split");

good.output("values");
bad.output("errors");
```

### Fail fast and let the runtime cancel sibling workers
Return an `Error` from custom operators for fatal conditions.

```rust
use std::io;
use instancy::{DataflowBuilder, Error};

let builder = DataflowBuilder::<u64>::new("fail-fast");
builder
    .input::<String>("lines")
    .unary("validate", move |input, output| {
        while let Some((time, lines)) = input.next() {
            for line in lines {
                if line.is_empty() {
                    return Err(Error::operator("validate", io::Error::other("empty line")));
                }
                output.push_vec(time, vec![line]);
            }
        }
        Ok(())
    })
    .output("clean");
```

## Error Policy

instancy also exposes a per-dataflow error policy in `instancy::execute::ErrorPolicy`:

- `ErrorPolicy::Stop` — stop the dataflow on the first error.
- `ErrorPolicy::Ignore { description }` — keep running and record the intent for debugging or alerting.

Some older design notes describe these modes as “halt” versus “log and continue”. The current public API uses the names above.

## Cancellation and Shutdown

### Cancellation

instancy supports cancellation at two levels: **per-dataflow** and **per-runtime**.

#### Cancelling a Single Dataflow

Every `SpawnedDataflow` handle has a `cancel()` method:

```rust
let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
let sender = handle.take_input::<i32>("data").unwrap();

// Feed some data...
sender.send(0, vec![1, 2, 3]).unwrap();

// Cancel this specific dataflow (other dataflows on the same runtime keep running)
handle.cancel();

// join() returns the result — cancellation is not an error, it's a graceful stop
handle.join_blocking().unwrap();
```

This is useful when you want to stop a long-running or streaming dataflow without affecting others on the same runtime.

#### Cancelling All Dataflows (Runtime Shutdown)

To shut down every dataflow on a runtime at once:

```rust
let h1 = rt.spawn(dataflow1, SpawnOptions::default()).unwrap();
let h2 = rt.spawn(dataflow2, SpawnOptions::default()).unwrap();

// Shut down the entire runtime — cancels all running dataflows
rt.shutdown();

h1.join_blocking().unwrap();
h2.join_blocking().unwrap();
```

You can also obtain the runtime's cancellation token and pass it to other threads or async tasks:

```rust
let token = rt.cancel_token().clone();

std::thread::spawn(move || {
    // Some external condition triggers shutdown
    std::thread::sleep(std::time::Duration::from_secs(30));
    token.cancel();  // All dataflows on this runtime shut down gracefully
});
```

Cancellation is **cooperative**: it signals operators at their next check point. Operators wind down in an orderly fashion — they don't get forcibly killed mid-operation.

#### Cancellation Reasons

Every cancellation carries a [`CancellationReason`](instancy::cancellation::CancellationReason) that explains *why* the dataflow was cancelled. This helps distinguish user-initiated stops from system failures:

```rust
use instancy::{CancellationReason, CancellationToken, SpawnOptions};

let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

// Cancel with a specific reason
handle.cancel_with_reason(CancellationReason::UserRequested);

// After join, inspect the cancellation reason
match handle.join_blocking() {
    Err(instancy::error::Error::Cancelled { reason }) => {
        match reason {
            Some(CancellationReason::UserRequested) => println!("User stopped the dataflow"),
            Some(CancellationReason::NetworkError { detail }) => println!("Network failure: {detail}"),
            Some(CancellationReason::WorkerFailed { detail }) => println!("Worker crashed: {detail}"),
            Some(CancellationReason::RuntimeShutdown) => println!("Runtime shut down"),
            Some(CancellationReason::HandleDropped) => println!("Handle was dropped"),
            Some(CancellationReason::OperatorError { detail }) => println!("Operator error: {detail}"),
            None => println!("Cancelled (no reason available)"),
        }
    }
    Ok(()) => println!("Completed normally"),
    Err(e) => println!("Other error: {e}"),
}
```

The built-in reason variants are:

| Variant | When used |
|---------|-----------|
| `UserRequested` | Default for `cancel()` — the caller explicitly requested cancellation |
| `RuntimeShutdown` | The runtime is shutting down (`RuntimeHandle` dropped or `shutdown()` called) |
| `NetworkError(String)` | A network-level error caused cancellation (TCP disconnect, transport failure) |
| `WorkerFailed(String)` | A sibling worker failed, causing cascading cancellation |
| `HandleDropped` | The `SpawnedDataflow` handle was dropped without calling `join()` |
| `OperatorError(String)` | An operator produced an error that caused the dataflow to be cancelled |

Reasons follow **first-cancel-wins** semantics: if a token is cancelled multiple times, only the first reason is recorded. Child tokens inherit their parent's reason.

### Graceful Drain on Cancellation

By default, cancellation stops the dataflow immediately — any in-flight data in channels is lost. For pipelines where you want to finish processing buffered data before stopping, use `drain_on_cancel`:

```rust
use std::time::Duration;
use instancy::SpawnOptions;

let opts = SpawnOptions::new()
    .drain_on_cancel(Duration::from_secs(5));

let handle = rt.spawn(dataflow, opts).unwrap();
```

When cancellation is triggered with drain enabled:

1. **External inputs are closed** — no new data is accepted.
2. **In-flight data continues flowing** through operators normally.
3. **If all operators complete** within the timeout, the dataflow returns successfully (`Ok`).
4. **If the timeout expires**, the dataflow returns `Err(Cancelled)` with the original reason.

This is useful for ETL pipelines, streaming aggregations, or any workflow where partial results are worse than slightly delayed shutdown.

## Multi-Worker Failure Propagation

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

## Graceful Shutdown Recipes

### Drain in-flight data before stopping
By default, cancellation drops everything immediately. Use `drain_on_cancel` to let in-flight
data finish processing within a timeout.

```rust
use std::time::Duration;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let builder = DataflowBuilder::<u64>::new("graceful");
let input = builder.input::<i32>("data");
input.map("process", |_t, x| x * 2).output("results");

let dataflow = builder.build().unwrap();
let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
let mut handle = rt.spawn(dataflow, opts).unwrap();

let sender = handle.take_input::<i32>("data").unwrap();
sender.send(0, vec![1, 2, 3]).unwrap();
sender.close(); // Close input so drain can complete.

handle.cancel(); // Triggers drain instead of immediate kill.
let result = handle.join_blocking();
assert!(result.is_ok()); // Data flowed through before shutdown.
```

### Detect drain timeout vs normal completion
When the drain timeout expires (e.g., input stays open), the result is `Err(Cancelled)`.

```rust
use std::time::Duration;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let builder = DataflowBuilder::<u64>::new("timeout-demo");
let input = builder.input::<i32>("data");
input.output("out");

let dataflow = builder.build().unwrap();
let opts = SpawnOptions::new().drain_on_cancel(Duration::from_millis(100));
let mut handle = rt.spawn(dataflow, opts).unwrap();

// Keep sender alive — dataflow can't finish draining.
let _sender = handle.take_input::<i32>("data").unwrap();

handle.cancel();
let result = handle.join_blocking();
assert!(result.is_err()); // Drain timed out → Cancelled.
```

## Troubleshooting Completion

### My dataflow hangs and never completes
- Check that all `InputSender`s are dropped (closing the input)
- Check that `unary_notify` operators consume all notifications (`ctx.next_notification()`)
- Check that capabilities aren't held indefinitely

## Related Examples

- [`error_handling.rs`](../../instancy/examples/error_handling.rs)
- [`panic_recovery.rs`](../../instancy/examples/panic_recovery.rs)
- [`cancellation.rs`](../../instancy/examples/cancellation.rs)
- [`graceful_drain.rs`](../../instancy/examples/graceful_drain.rs)

## Next Steps

- Next: [Serialization](./serialization.md)
- See also: [Observability](./observability.md), [Distributed Execution](./distributed.md)
