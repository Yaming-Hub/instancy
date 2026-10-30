# Instancy confidence matrix

Purpose: show what differential testing against timely has established so far, and what is left to build confidence in instancy.

Legend: ✅ covered differentially (exact outputs, workers, timestamps, frontiers at causal checkpoints) · ◐ partly covered · ⬜ not yet covered · 🧪 covered only by instancy's own repo tests (not compared against timely)

Pins: instancy `c692fb10`, timely `492787b7` (0.31.0), rustc 1.97.1. **No engine mismatch in any run.**

Review status: the original in-process experiment is DA-signed-off. Cluster mode and items 1–4 below were user-directed follow-ups (not DA-reviewed).

## Results at a glance

| Suite | Scenarios | Result |
|---|---|---|
| Original 4 recipes, in-process (DA-signed-off) | 32 | ✅ 14/14 tests |
| Cluster mode, 2 loopback nodes (4 recipes + breadth + 16 programs) | 64 | ✅ 8/8 tests, first attempt |
| Operator breadth + 48 random programs | 56 | ✅ |
| Scale (80–120 epochs, ≤40 records/epoch/graph, 24 live dataflows) | 5 | ✅ (after one budget timeout, see notes) |
| Config sweep (5 instancy configs × 8 settings × 3 recipes) | 120 | ✅ |
| Release build + `progress-check` (whole suite) | all of the above, in-process | ✅ 29/29, first attempt |
| 10-minute soak (release, randomized everything) | **603** | ✅ |

Non-vacuity: three deliberate instancy mutants (premature emission in-process, premature emission in cluster mode, missing delay in random programs) were all caught.

---

## 1. Execution modes

| Dimension | Status | What was done | What is left |
|---|---|---|---|
| In-process, multiple logical workers | ✅ | 1–3 workers; soak randomizes | 8–16 workers |
| Cluster, 2 nodes, loopback TCP | ✅ | 2–4 workers split 1+1, 1+2, 2+2; dedicated and shared transport; recipes, breadth, programs | 3+ nodes; uneven splits such as 1+3 |
| Cluster over real network / injected faults | ⬜ | — | Latency, loss, reordering, partition |
| Execution-pool threads | ✅ | 1, 2, 3 and 4 pool threads; Tokio 1–4; soak randomizes | Pools larger than the worker count |
| `per_stage_parallelism(true)` | ◐ | Sweep with uniform-parallelism graphs | `exchange_to` / `rebalance_to` actually changing stage parallelism |
| `IoMode::Async` inputs/outputs | ✅ | Config sweep and soak | — |
| Release build (`progress-check`) | ✅ | Whole suite plus the soak | Release without the checker (perf only) |
| MSRV 1.85 | ⬜ | Not established (toolchain 1.97.1) | Separate MSRV job |

## 2. Timestamps and progress

| Dimension | Status | What was done | What is left |
|---|---|---|---|
| `u64` epochs, open, held and advanced inputs | ✅ | Up to 120 epochs; held+extended cuts; ahead-of-time sends | Thousands of epochs |
| Incomparable `Product` frontiers | ✅ | Cut `{(0,2),(2,0)}` | 3+ element antichains |
| Nested loop time | ✅ | Fixed recipe plus random programs with loops nested 2 deep and data-driven exits | Depth 3+ |
| Probe frontiers at checkpoints | ✅ | Bounded equality, never ahead | Lossless transition history (needs a hook; design decision) |
| Premature / late output detection | ✅ | Three mutants caught | — |
| Per-graph completion | ✅ | Join plus timely retirement; up to 24 graphs | — |
| Scheduler-interleaving exploration | ⬜ | Natural scheduling only (thousands of runs via the soak) | shuttle/loom-style exploration |

## 3. Operators (differential coverage)

| Operator family | Status | Notes |
|---|---|---|
| `exchange_by_hash`, `binary`+`retain`, `unary_notify`, `iterate` | ✅ | Original recipes plus programs |
| `map`, `filter`, `fork`, `merge`/concat, `probe`, `output` | ✅ | |
| `broadcast` | ✅ | Breadth (modelled: one copy per worker) and programs |
| `gather` | ✅ | Breadth (feeding `count`) |
| `rebalance` | ◐ | Totals after re-exchange match; destination is arrival-order dependent, so not compared per worker |
| `branch` | ✅ | Breadth and programs |
| `flat_map` | ✅ | Breadth and programs |
| `distinct` | ✅ | Breadth, modelled |
| `delay` (per record) | ✅ | Breadth and programs (odd keys +1 epoch) |
| `reduce` | ✅ | Breadth, modelled per worker |
| `count` (= `fold`) | ✅ | Breadth, modelled |
| `delay_batch`, `fold` with custom init | ◐ | Same notify path as `delay`/`count`; not called directly |
| `take`, `take_while` | ⬜ | Which records are kept depends on arrival order; needs a property check, not differential |
| `exchange` (key-hash), `exchange_to`, `exchange_by_hash_to`, `rebalance_to` | ⬜ | Different hash, or a stage-parallelism change |
| `map_batch`, `try_*`, `Result` ops | ⬜ | Low progress risk |
| `source` / `source_async`, `unary_async` | ⬜ | No direct timely analogue; model-based |

## 4. Workload shape and scale

| Dimension | Status | What was done | What is left |
|---|---|---|---|
| Record counts | ✅ | Up to ~4,800 records per input per scenario | Millions of records |
| Batching / backpressure | ◐ | Batches of 1–4; capacities 1, 2 and 1024 on all exposed edges | Hidden loop channels (fixed at 1024); slow consumers |
| Concurrent dataflows | ✅ | 2, 8 and 24 graphs | Graphs started and stopped mid-run |
| Program shapes | ✅ | 4 recipes + breadth + 48 + 16 + ~201 soak random programs | Larger generator vocabulary (binary joins, notify inside programs) |
| Long-running / soak | ◐ | 10-minute soak, 603 scenarios | Hour-plus soaks; memory growth tracking |

## 5. Lifecycle and failure semantics

| Area | Status | Notes |
|---|---|---|
| Normal close / join / teardown | ✅ | Every scenario |
| Cancellation, drain, panic, dropped receivers, scheduling policies | 🧪 | Repo tests only; no timely equivalent |

## 6. Cluster-specific

| Area | Status | Notes |
|---|---|---|
| Dedicated vs shared transport | ✅ | |
| Shared pool with multiple connections, 3+ nodes | ⬜ | |
| Link loss / partitions / handshake / rolling upgrade / teardown | 🧪 | Repo tests only |
| Correct results *during* injected faults | ⬜ | |

## 7. Non-functional

| Area | Status | Notes |
|---|---|---|
| Performance vs timely | ⬜ | Not measured. Diagnostic only: in **debug** builds, nested loops cost instancy ~0.4 s per checkpoint (60 s vs timely's 15–23 s for 145 checkpoints); in release + progress-check the same tests finish in ~3 s. Worth a real benchmark. |
| Memory / resource growth | ⬜ | |
| Timely reference drift | ◐ | One pinned revision |

---

## Notes on failures during this work (all non-engine)

- Two helper tests had wrong premises (a witness-removal test; a delay bound that ignored ahead-of-time sends). Both were fixed and disclosed.
- One scale test exceeded the 120 s process budget in debug. It completed and matched in 189.5 s when run alone; I split the workload and kept the budget. The raw logs and evidence receipts were kept locally.

## Suggested next steps

1. Stage-parallelism changes (`exchange_to`, `rebalance_to`, `per_stage` with real stage boundaries).
2. Cluster depth: 3+ nodes, multi-connection pool, differential checkpoints under mock-network delay or reordering.
3. A performance benchmark (debug nested-loop latency is the first lead), plus hour-plus soaks with memory tracking.
4. Property checks for `take`/`take_while`/`rebalance`, and model-based checks for `source_async`/`unary_async`.
5. Interleaving exploration (shuttle/loom) of the progress protocol.
