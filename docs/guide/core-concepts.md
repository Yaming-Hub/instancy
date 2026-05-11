# Core Concepts

instancy keeps the core timely-dataflow model: timestamps, frontiers, capabilities, and logical scopes. This page defines the terms the rest of the guide builds on.

[Back to the guide index](./README.md)

### Dataflow

A dataflow program is a directed graph where nodes are **operators** and edges are **streams**. Each operator independently processes data from its input streams and pushes results to its output streams.

```text
[source] → [map] → [filter] → [output]
```

The key property of dataflow is **independence**: operators don't call each other. They react to data arriving on their inputs. This means the runtime can schedule them in any order, on any thread, or even on different machines — as long as the data flows correctly.

### Timestamps

Every piece of data in instancy carries a **timestamp**. Timestamps represent logical time — they could be epoch numbers, iteration counts, or any ordered type.

```rust
// Data at timestamp 0
sender.send(0u64, vec![1, 2, 3]).unwrap();
// Data at timestamp 1
sender.send(1u64, vec![4, 5, 6]).unwrap();
```

Timestamps serve two purposes:

1. **Ordering** — operators can distinguish "earlier" data from "later" data, even if messages arrive out of order
2. **Progress** — the system tracks which timestamps are still possible, enabling operators to know when they've seen everything for a given time

### Progress and Frontiers

The **frontier** at any point in the dataflow is the set of timestamps that might still appear. As an input advances past timestamp 3, operators downstream know they will never see data at timestamps 0, 1, or 2 again — they can finalize any aggregations for those times.

This is the core insight of timely dataflow: **lightweight progress tracking replaces heavyweight synchronization barriers.** Operators don't need to wait for explicit "end of epoch" signals. Instead, the system automatically propagates frontier information through the graph.

### Capabilities

Internally, each operator holds **capabilities** — tokens that represent its ability to produce data at certain timestamps. When an operator is done producing data for timestamp 5, it releases that capability. This release propagates through the graph, advancing frontiers downstream.

You don't need to manage capabilities directly when using the built-in operators. They handle capability lifecycle automatically. When you create custom operators with `unary` or `binary`, capabilities are managed through the `InputHandle` and `OutputHandle` types.

## Related Examples

- [`hello_dataflow.rs`](../../instancy/examples/hello_dataflow.rs)
- [`probe.rs`](../../instancy/examples/probe.rs)
- [`barrier.rs`](../../instancy/examples/barrier.rs)

## Next Steps

- Next: [Building Dataflows](./building-dataflows.md)
- See also: [Iteration](./iteration.md), [API Reference](../reference/api.md)
