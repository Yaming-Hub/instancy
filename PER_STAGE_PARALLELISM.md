# Per-Stage Parallelism Design

## Problem

Today, a dataflow runs with **uniform parallelism** — every operator has the same number of workers (determined by the cluster topology). There is no way to say "the parse stage should have 8 workers but the aggregate stage should have 2 workers."

Real workloads need different parallelism at different computation phases:
- A CPU-heavy parse stage may need 8 workers
- A network-bound lookup stage may need 32 workers
- A final aggregate stage may need 1 worker (gather)

## Key Design Decision: Stages, Not Regions

The original design used explicit **execution regions** — user-managed groups of operators with named parallelism. After analysis, we replace regions with **stages** (inspired by Spark/Flink), because:

1. **Exchange/gather already forms the natural boundary.** Within a boundary, everything is pipeline with uniform parallelism. The user shouldn't have to redundantly declare what the system can infer.

2. **Operator fusion reduces scheduling overhead.** All operators in a stage fuse into a single schedulable task per worker. A 5-operator pipeline becomes 1 poll cycle, not 5 separate ready-queue activations. This directly addresses the project goal of minimizing async task scheduling overhead.

3. **Simpler API.** No `new_region()`, no `in_region()`, no region naming. Just use `exchange(hash_fn, par)` / `gather()` / `broadcast()` and stages are auto-inferred.

## What is a Stage?

A **stage** is a maximal group of contiguous operators connected by Pipeline edges (no exchange/gather/broadcast/rebalance between them). All operators in a stage share the same parallelism (worker count).

Stage boundaries are created **implicitly** by repartition operators:

```
source → parse → validate → [exchange] → aggregate → format → [gather] → output
|________ Stage 0 (par=8) ________|     |__ Stage 1 (par=4) __|        |_ Stage 2 (par=1) _|
         auto-inferred                   auto-inferred                  auto-inferred
```

### Stage vs Spark Stage

| Aspect | Spark Stage | instancy Stage |
|---|---|---|
| Definition | Operators between shuffles | Operators between exchange/gather |
| Execution | **Batch** — stage completes before next starts | **Streaming** — stages run concurrently, data flows continuously |
| Progress | Stage completion (all tasks done) | **Frontier-based** — per-timestamp progress tracking |
| Loops | DAG only | Supports iteration across stage boundaries |
| Parallelism | Configurable per stage | Configurable per stage |

The key difference is **streaming execution** — all stages are active simultaneously, with frontier-based progress tracking coordinating data flow.

## How the System Knows Worker Counts Per Stage

### Step 1: User specifies parallelism at repartition points

```rust
let output = builder
    .source("input", |handle| { ... })      // Stage 0 begins (par from topology)
    .map(|data| parse(data))
    .filter(|rec| rec.is_valid())
    .exchange(|rec| hash(&rec.key), 4)       // Stage 0→1 boundary, Stage 1 par=4
    .unary("aggregate", |input, output| { ... })
    .gather()                                 // Stage 1→2 boundary, Stage 2 par=1
    .unary("final_sort", |input, output| { ... })
    .rebalance(8)                            // Stage 2→3 boundary, Stage 3 par=8
    .inspect(|data| println!("{data:?}"));
```

The parallelism flows like this:
- **Stage 0**: auto-detected from input sources (with `auto_parallelism`, the default), or inherited from `num_workers` / cluster topology
- **Stage 1**: set by `exchange(..., 4)` — the repartition specifies downstream parallelism
- **Stage 2**: set by `gather()` — always parallelism=1
- **Stage 3**: set by `rebalance(8)` — explicit downstream parallelism

### Auto-Parallelism (Default)

With `SpawnOptions::auto_parallelism(true)` (the default), stage 0 parallelism is automatically detected from the number of `input()` + `source_async()` calls in the graph. The `num_workers` argument to `spawn_multi` acts as a minimum floor:

```
effective_parallelism = max(auto_detected, num_workers)
```

This means:
- A graph with 3 inputs and `num_workers=2` gets `max(3, 2) = 3` stage 0 workers
- A graph with 1 input and `num_workers=4` gets `max(1, 4) = 4` stage 0 workers
- Pass `num_workers=0` to let auto-detection fully control stage 0 parallelism

Stages without explicit parallelism (no exchange/gather/rebalance) inherit the effective stage 0 count.

Disable with `SpawnOptions::new().auto_parallelism(false)` to use the exact `num_workers` value for stage 0. To force uniform parallelism across *all* stages, also set `per_stage_parallelism(false)`.

**Important: parallelism is always cluster-wide total.** The hosting application controls how workers are distributed across nodes via the `ClusterTopology` (e.g., node A: 6 workers, node B: 2 workers for a total of 8 — because node A has more local data). Pipeline edges within a stage are local-only (no network traffic). Exchange/gather/rebalance edges cross node boundaries.

### Cluster coordination at startup

The hosting application calls `spawn_cluster()` on **every node** with the same `ClusterTopology` and `DataflowId`. The runtime coordinates via:

1. **Handshake** — nodes exchange a fingerprint (hash of graph topology) over the control channel. If any node has a different graph structure → error. This confirms all nodes are running the same dataflow.
2. **Ready barrier** — after all local workers are materialized (operators constructed, channels wired), each node waits for ALL peers to also signal "ready." No node begins execution until every node has completed setup. This prevents a fast node from sending data before a slow node's receivers are ready.

```
Node A: spawn_cluster() → build workers → handshake → ... setup ... → ready barrier → execute
Node B: spawn_cluster() → build workers → handshake → ... setup ... → ready barrier → execute
                                   ↕                                        ↕
                          fingerprint match                        mutual "I'm ready"
```

### Step 2: Builder auto-infers stages during `build()`

When `builder.build()` is called, the builder walks the operator graph and:

1. **Groups operators into stages** by following Pipeline edges. Every time an exchange/gather/rebalance/broadcast edge is encountered, a new stage begins.
2. **Records each stage's parallelism** from the repartition operator that created it.
3. **Stores stage metadata** in the `LogicalDataflow`.

```rust
pub struct LogicalDataflow<T> {
    pub(crate) graph: DataflowGraph,
    pub(crate) stages: Vec<StageInfo>,     // NEW: stage metadata
    pub(crate) channel_factories: Vec<...>,
    // ...
}

pub struct StageInfo {
    pub id: StageId,
    pub parallelism: usize,
    pub operator_indices: Vec<usize>,       // operators in this stage
    pub placement: PlacementPolicy,         // optional placement hint
    pub name: Option<String>,               // optional debug name
}
```

### Step 3: Runtime reads stage metadata at spawn time

```rust
fn spawn_dataflow_internal(...) {
    let logical = builder.build()?;
    
    // Read stages from the logical dataflow
    // stages = [Stage(0, par=8, ops=[0,1,2]), Stage(1, par=4, ops=[3]), ...]
    let stages = &logical.stages;
    
    // Total workers = sum of all stage parallelisms
    // But each worker only runs ONE stage's operators (fused)
    
    for stage in stages {
        for worker_idx in 0..stage.parallelism {
            // Build an executor containing ONLY this stage's operators
            // All operators fused into a single schedulable task
            let executor = materialize_stage_executor(
                &logical,
                stage,
                worker_idx,
            );
            // Register in the worker pool
        }
    }
    
    // Wire cross-stage exchange channels
    // source_stage.parallelism push endpoints → target_stage.parallelism pull endpoints
}
```

### Step 4: Operator fusion within a stage

All operators in a stage with the same `worker_id` run as **one schedulable unit** — a fused stage-worker task. The executor schedules and polls them together in a single activation pass.

```
// Without fusion (today): each operator independently scheduled
Ready queue: [op0, op1, op2, op3, op4]
Activation:   poll op0 → re-enqueue op1 → poll op1 → re-enqueue op2 → ...
              (5 scheduler round-trips, possible interleaving with other work)

// With fusion: one stage-task polls all operators in pipeline order
Ready queue: [stage0-worker0]
Activation:   poll op0 → if output → poll op1 → if output → poll op2 → ... → op4
              (1 scheduler round-trip, all operators run together)
```

#### Fusion mechanism

The executor no longer schedules individual operators. Instead, it schedules **stage-worker tasks**:

```rust
/// A fused task representing all operators in one stage for one worker.
struct FusedStageTask {
    stage_id: StageId,
    worker_id: usize,
    /// Operators in topological (pipeline) order within this stage.
    operators: Vec<Box<dyn SchedulableOperator>>,
}

impl FusedStageTask {
    /// Called by the executor when this task is activated.
    fn poll(&mut self, budget: usize) -> PollResult {
        let mut work_done = 0;
        // Run operators in pipeline order: source → ... → sink
        for op in &mut self.operators {
            let result = op.activate();
            work_done += result.records_processed;
            if work_done >= budget {
                return PollResult::BudgetExhausted;
            }
        }
        if work_done == 0 {
            PollResult::Idle  // no data flowing, return to scheduler
        } else {
            PollResult::Progress  // re-schedule for another pass
        }
    }
}
```

#### Co-scheduling guarantees

- **All operators in the same pipeline (same stage + same worker_id) are guaranteed to run together** — they are literally one task, not separate entries that might interleave with other work.
- **Activation**: When data arrives at a stage-worker's input (from a cross-stage channel), the entire fused task is enqueued in the ready queue.
- **Yielding**: The fused task yields back to the scheduler when:
  1. No more data to process (pipeline drained)
  2. Poll budget exhausted (fairness with other stage-workers)
  3. Blocked on cross-stage input channel (upstream hasn't produced data yet)
- **No internal ready queue**: Within a fused task, operators are polled in fixed topological order — no per-operator scheduling decisions.

#### Scheduling overhead reduction

For a dataflow with 20 operators across 3 stages:
- **Without fusion**: 20 × workers schedulable tasks per stage
- **With fusion**: 1 × workers tasks per stage (e.g., 3 stages × 8 workers = 24 tasks total instead of 160)

### Step 5: Cross-stage channels

Edges crossing stage boundaries become exchange channels:

```
Stage 0 (par=8) → [exchange by hash] → Stage 1 (par=4)

Stage-0-worker-0 ──┐
Stage-0-worker-1 ──┤
Stage-0-worker-2 ──┤  8×4 routing matrix   ┌── Stage-1-worker-0
Stage-0-worker-3 ──┤  (hash determines     ├── Stage-1-worker-1
Stage-0-worker-4 ──┤   destination)         ├── Stage-1-worker-2
Stage-0-worker-5 ──┤                        └── Stage-1-worker-3
Stage-0-worker-6 ──┤
Stage-0-worker-7 ──┘
```

The existing `ExchangeChannelBuilder` already supports cross-worker routing — it just needs to be generalized from N×N (uniform) to M×N (asymmetric) routing.

### Step 6: Progress tracking across stages

Each stage's workers form an independent progress-tracking group:

- **Within a stage**: Workers exchange progress messages among themselves (same as today's multi-worker progress tracking).
- **At stage boundaries**: The exchange channel aggregates upstream workers' frontiers. A downstream worker's input frontier advances only when **all** upstream workers that can route to it have advanced past that timestamp.

```
Stage 0 (par=8)                   Stage 1 (par=4)
Workers 0-7 track progress  →    Workers 0-3 track progress
among themselves                  among themselves

Cross-stage frontier:
  Stage-1-worker-0's input frontier = 
    min(Stage-0 workers that hash to worker-0).output_frontier
```

## API Design

### Repartition operators with parallelism

**Parallelism is always cluster-wide total.** For stage 0, the hosting application specifies per-node worker counts in the `ClusterTopology` (which need not be equal — e.g., 6 on node A, 2 on node B). For subsequent stages, the repartition operator specifies the total; how those workers are placed across nodes follows a `PlacementPolicy` (proportional, round-robin, or pinned) which the application can configure. In single-node mode, total = local.

```rust
// exchange: hash-partition with explicit downstream parallelism (cluster-wide total)
stream.exchange(|record| hash(&record.key), /*par=*/ 16)

// gather: all data to 1 worker (parallelism always 1)
stream.gather()

// broadcast: clone to all downstream workers
stream.broadcast(/*par=*/ 8)

// rebalance: round-robin distribution
stream.rebalance(/*par=*/ 4)
```

### Default behavior (backward compatible)

If no repartition operators are used, the entire dataflow is one stage:
- **Single-node**: parallelism = `num_workers` parameter from the spawn call
- **Cluster** (`spawn_cluster`): parallelism = `topology.total_workers()` (sum across all nodes)

Each node runs the workers assigned to it by the topology. For example, with `total_workers=8` on a 2-node cluster, the topology might assign 6 workers to node A and 2 to node B.

```rust
// Single-node: one stage, 4 workers (all local)
runtime.spawn("my_df", 4, |builder| {
    builder.source(...).map(...).inspect(...);
})

// Cluster: one stage, 8 workers total (4 on each of 2 nodes)
// Called on EVERY node with same topology + dataflow_id
runtime.spawn_cluster("my_df", topology, local_node_id, df_id, connections, |builder| {
    builder.source(...).map(...).inspect(...);
})
```

### Named stages (optional, for observability)

```rust
stream
    .exchange(|r| hash(&r.key), 16)
    .named_stage("heavy_compute")      // optional: name for metrics/debugging
    .map(|r| expensive(r))
```

## Comparison: Stage vs Region

| Aspect | Stage (new) | Region (old) |
|---|---|---|
| Boundary | **Implicit** — exchange/gather | Explicit — `new_region()` |
| Operator grouping | **Fused** — 1 task per stage-worker | Individual — 1 task per operator |
| Scheduling overhead | **Lower** — fewer tasks | Higher — more tasks |
| API complexity | **Simpler** — no region management | More concepts |
| Flexibility | Less (can't override boundaries) | More (explicit control) |
| Naming | Optional `.named_stage()` | Built-in region names |

## Implementation Plan

### Phase 1: Operator fusion (FusedStageTask) ✅
- Introduce `FusedStageTask` that owns all operators for one stage-worker
- Single `poll()` method runs operators in topological order
- Replace per-operator ready-queue entries with per-stage-worker entries
- Budget-based yielding for fairness
- **Backward compatible**: with only one stage, this is equivalent to running all operators in a single executor (same as today but fused)

### Phase 2: Stage inference in builder ✅
- Add `StageInfo` and `StageId` types
- Builder's `build()` method auto-infers stages by walking Pipeline edges and cutting at exchange/gather/rebalance/broadcast
- `LogicalDataflow` carries `Vec<StageInfo>` with operator indices per stage
- Validation: operators within a stage have consistent parallelism

### Phase 3: Repartition operator parallelism parameter ✅
- `exchange(key_fn, par)`, `gather()`, `broadcast(par)`, `rebalance(par)` accept downstream parallelism
- Builder validates parallelism consistency at stage boundaries
- Build-time error if parallelism changes without a repartition operator

### Phase 4: Multi-stage executor ✅
- Runtime reads stage metadata from `LogicalDataflow`
- For each stage, creates `parallelism` number of `FusedStageTask` instances (one per worker in that stage)
- Each `FusedStageTask` contains only the operators belonging to its stage
- Per-stage concurrency semaphore limits how many stage-workers run simultaneously

### Phase 5: Cross-stage channels (M×N asymmetric exchange) ✅
- `ExchangeChannelSet` generalized from N×N to M×N with `new_asymmetric(M, N, capacity)`
- `EdgeMaterializer` trait extended with `materialize_source_worker()`/`materialize_target_worker()`
- `ExchangeFactoryCreatorFn` accepts `(num_sources, num_targets, capacity)`
- `ExchangePush` routes using `hash % num_targets` (semantic clarity)
- Backward compatible: symmetric (M==N) path unchanged

### Phase 6: Per-stage progress tracking & validation ✅
- Cross-stage frontier propagation handled by `ExchangePull::FrontierAggregator`
  (aggregates watermarks from M sources → emits progress to downstream)
- Each executor's `ProgressTracker` tracks only its own operators; exchange channels
  provide the boundary between stages
- Runtime validates stage parallelism compatibility at spawn time
- **Full per-stage executors** (heterogeneous worker counts, M≠N at runtime) deferred
  to future work — requires splitting dataflow into sub-dataflows and spawning
  separate worker groups per stage

## Status

**All phases complete.** Per-stage parallelism is the default behavior
(`SpawnOptions::per_stage_parallelism` defaults to `true`).

### v1 (PRs #190-#194) — superseded by v2
- `spawn_staged_internal` built max(P_i) workers with NullOperator placeholders
- Functional but architecturally impure (idle operators, no-op exchange endpoints)

### v2 (PRs #195-#198) — current implementation
- **Phase 7** (PR #195): `SpawnOptions::auto_parallelism` — build closure without
  `worker_idx`, stage 0 parallelism auto-detected from input/source_async count
- **Phase 8** (PR #196): Per-stage executor materialization — `retain_stages()`
  strips non-participating operator/channel factories per worker
- **Phase 9** (PR #197): Ghost operators — non-participating operators kept in
  reachability graph (shapes + connectivity) but without capabilities or progress
  buffers, enabling correct cross-stage frontier propagation
- **Phase 10** (PR #198): `per_stage_parallelism: true` as default, doc updates

## Redesign: Build-Once, Materialize-Per-Stage (v2)

### Core Principle

The dataflow graph is a **logical** description — it should be built **once**, not
per-worker. The runtime determines how to materialize it onto workers based on
stage parallelism. Workers only contain operators for stages they participate in.

### API Change

```rust
// v1 (old): worker_idx in build closure, manual parallelism
rt.spawn_multi("name", num_workers, |worker_idx, builder| {
    // worker_idx is a physical concern leaked into logical graph construction
    let input = builder.input::<i32>("data");
    input.exchange_to("scatter", 4, |v| *v as u64)
        .map("process", |_t, x| x * 2)
        .gather("collect")
        .for_each("sink", |_t, _v| {});
    Ok(())
}, SpawnOptions::new().per_stage_parallelism(true))

// v2 (current): no worker_idx, parallelism derived from graph structure
rt.spawn_multi("name", num_workers, |builder| {
    let input = builder.input::<i32>("data");
    input.exchange_to("scatter", 4, |v| *v as u64)
        .map("process", |_t, x| x * 2)
        .gather("collect")
        .for_each("sink", |_t, _v| {});
    Ok(())
}, SpawnOptions::new())
```

Operators that need worker identity access it via `WorkerContext` at activation
time — a runtime concern, not a build-time concern.

### Stage 0 Parallelism from Physical Inputs

Stage 0 parallelism is determined automatically:
- **Number of `input()` / `source_async()` calls in stage 0** = stage 0 parallelism
- Each physical input stream gets its own worker
- If 1 input → 1 worker; if 4 inputs → 4 workers
- Rationale: input streams are the physical constraint — having more workers
  than inputs means some workers have nothing to read

```
// 1 input → stage 0 par=1
builder.input::<i32>("data")
    .exchange_to("scatter", 4, hash_fn)  // stage 0→1 boundary
    .map("process", ...)                  // stage 1 par=4

// 3 inputs → stage 0 par=3
let a = builder.input::<i32>("stream_a");
let b = builder.input::<i32>("stream_b");
let c = builder.input::<i32>("stream_c");
a.binary(b, "merge_ab", ...).binary(c, "merge_all", ...)
    .exchange_to("scatter", 8, hash_fn)  // stage 0→1, stage 1 par=8
```

For cluster mode, stage 0 parallelism per node = number of local input streams
on that node.

### Per-Stage Materialization

Instead of building max(P_i) full copies and patching with NullOperator:

1. **Build once** (probe) → LogicalDataflow with stages, parallelism, graph
2. **Build P_i copies per stage** — call the build closure max(P_i) times,
   but for each worker only retain the operator/channel factories for its
   participating stages. Discard (not null) non-participating factories.
3. **Per-stage SubgraphBuilder** — each worker's progress tracker only covers
   its participating stages. No leaked capabilities.
4. **Exchange channels** connect M source workers to N target workers directly.
   No no-op endpoints — only participating workers have endpoints.
5. **Per-stage progress exchange** — workers only exchange progress with peers
   in the same stage. Cross-stage progress flows through exchange channels.

```
Worker allocation for stages [par=1, par=4, par=1]:

   Worker 0: Stage 0 ops + Stage 2 ops
   Worker 1: Stage 1 ops only
   Worker 2: Stage 1 ops only
   Worker 3: Stage 1 ops only

   (Worker 0 also gets Stage 1 ops — it participates in all stages
    because 0 < min(1, 4, 1))
```

### Progress Tracking Per Stage

Each stage forms an independent progress-tracking group:

- **Workers in the same stage** exchange progress among themselves
- **Cross-stage frontiers** propagate through exchange channel's
  `FrontierAggregator` (already implemented)
- **No global progress exchange** across all max(P_i) workers —
  only P_i workers per stage

This eliminates the overhead of non-participating workers exchanging
empty progress messages.

### Backward Compatibility

- `spawn_multi(name, N, |builder| ..., opts)` remains unchanged
  for uniform parallelism (all stages same N). No per-stage materialization.
- `spawn_dataflow(name, |builder| ..., opts)` is the new per-stage API.
  When all stages have the same parallelism (no exchange_to/gather), it
  degrades to the uniform path automatically.
- `per_stage_parallelism` option is no longer needed on `spawn_dataflow` —
  it's always stage-aware.

## Implementation Plan (v2) — All Phases Complete ✅

### Phase 7: `spawn_dataflow` API + stage 0 auto-parallelism ✅
- `SpawnOptions::auto_parallelism(true)` — build closure `Fn(usize, &mut DataflowBuilder)` 
- Stage 0 parallelism = count of `input()` + `source_async()` operators in stage 0
- Internally builds max(P_i) copies via `spawn_staged_internal`
- Integration tests for auto-parallelism (PR #195)

### Phase 8: Per-stage executor materialization ✅
- `LogicalDataflow::retain_stages()` strips non-participating operator/channel factories
- `SubgraphBuilder::retain_operators()` removes operators from progress tracker
- Edge index remapping for compacted edge vectors
- Workers only materialize operators for stages they participate in (PR #196)

### Phase 9: Ghost operators for cross-stage frontier propagation ✅
- `SubgraphBuilder::mark_ghost_operators()` — keeps shapes and connectivity in
  reachability graph, but removes initial_capabilities and progress_buffers
- `ProgressTracker::materialized_indices` — `collect_operator_progress` skips ghosts
- Peer progress broadcasts propagate through ghost operators to downstream stages
- Frontier-dependent operators (delay, unary_notify) work correctly across stages (PR #197)

### Phase 10: Make per_stage_parallelism the default ✅
- `SpawnOptions::default()` sets `per_stage_parallelism: true`
- Updated spawn_multi/MultiSpawnedDataflow docs
- Updated GUIDE.md, examples, validation tests (PR #198)

## Open Questions

1. **Loops/iteration**: All operators within `scope.iterative()` must be in the same stage (no repartition inside loops for v1).

2. **Binary operators**: Both inputs must come from the same stage, or both must be repartitioned to the same parallelism with compatible distribution.

3. **PlacementPolicy for subsequent stages**: Attach to the repartition operator as an optional parameter (e.g., `exchange(fn, 16).placement(Pinned("node-A"))`), or default to proportional distribution based on cluster topology.

4. **Worker assignment for multi-stage participation**: Worker 0 participates
   in stage 0 (par=1) AND stage 1 (par=4). Its executor has operators from
   both stages. How to schedule: single executor with all participating
   operators (simpler) vs. separate executors per stage (more isolation)?
   Decision: single executor with FusedStageTask per stage — already implemented.
