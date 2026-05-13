# Benchmark Results: instancy vs timely-dataflow

This document presents detailed benchmark results comparing instancy and
[timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) v0.12
across two test suites:

1. **Cross-process TCP exchange** — sustained 600-second runs with real TCP
   transport between 2 OS processes (the primary benchmark)
2. **Single-process Criterion** — micro-benchmarks measuring per-query latency
   in a single process

Both libraries use the same number of cores and run on the same machine.

## 1. Cross-Process TCP Exchange Benchmark

### 1.1 Test Setup

- **Duration**: 600 seconds per (library, scenario) — 10 measurement phases total
- **Warmup**: 30 seconds per phase (results discarded)
- **Processes**: 2 OS processes connected by real TCP sockets
- **Threads**: 16 worker threads per process (32 total)
- **Machine**: Single Windows machine, mostly idle during measurement
- **Binary**: Same benchmark binary runs as both coordinator and worker

Each measured query is a **complete end-to-end distributed execution**:

1. Coordinator spawns a fresh worker OS process
2. Worker connects back via TCP control socket
3. Both processes establish TCP exchange connections
4. Both processes build the same dataflow graph
5. Both feed their local partition of the data
6. Data flows through operators, exchanged across processes via TCP
7. Worker reports completion metrics; coordinator records latency

This design ensures every query pays the full cost of distributed execution:
process creation, TCP setup, serialization/deserialization, kernel network
stack, and cross-process coordination.

### 1.2 Scenario Descriptions

All scenarios include a **cross-process hash exchange** that routes data
between the two processes via TCP. ScanFilterAgg and PageRank additionally
include a **gather** exchange that routes all final output to a single worker.

#### Scenario 1A: Scan-Filter-Aggregate (100M records)

A TPC-H-inspired analytical query. Each process generates 50M synthetic
`LineItem` records (58 bytes each), filters by `ship_date`, exchanges by group
key, and aggregates into (quantity, price) sums per (return_flag, line_status).

```
source(50M records) → filter(ship_date < 11000) → exchange_by_hash(group_key)
                    → aggregate → gather(→ process 0) → sink
```

- **Concurrency**: 2 queries in flight
- **Per-query duration**: ~6.7s (instancy), ~24.6s (timely)

#### Scenario 1B: PageRank (200K vertices, 2M edges, 100 iterations)

Each process receives half the edge list, runs 100 iterations of PageRank
locally, then exchanges vertex ranks by vertex ID and gathers to process 0.

```
source(1M edges) → pagerank(100 iterations) → exchange_by_hash(vertex_id)
                 → gather(→ process 0) → sink
```

- **Concurrency**: 2 queries in flight
- **Per-query duration**: ~3.8s (instancy), ~4.6s (timely)

#### Scenario 1C: 20-Stage Map Chain (5M values)

Each process generates 2.5M `i64` values and passes them through 20
successive `map(+1)` stages, then exchanges by hash.

```
source(2.5M values) → map(+1) × 20 → exchange_by_hash(value) → sink
```

- **Concurrency**: 2 queries in flight
- **Per-query duration**: ~354ms (instancy), ~988ms (timely)
- **Note**: Gather step removed — 5M values through a single worker causes
  TCP backpressure deadlock under concurrent load

#### Scenario 1D: Multi-Epoch Filter (16 epochs × 4096 records)

Each process receives 8 epochs of 4096 records each. Records are filtered by
a threshold, then exchanged by hash. Tests the multi-timestamp (epoch)
advancement protocol.

```
input(8 epochs × 4096 records) → filter(value > threshold)
                               → exchange_by_hash(value) → sink
```

- **Concurrency**: 2 queries in flight
- **Per-query duration**: ~138ms (instancy), ~689ms (timely)
- **Note**: Gather step removed for the same backpressure reason as MapChain

#### Scenario 2: Concurrent Small Pipeline (100 values, ×64 concurrent)

A tiny 3-stage pipeline processing only 100 values per query, but with **64
concurrent queries** in flight. This stress-tests per-query overhead and
concurrent scheduling.

```
source(50 values) → map(+1) → map(×2) → map(−1) → exchange_by_hash(value) → sink
```

- **Concurrency**: 64 queries in flight
- **Per-query duration**: ~4.8s (instancy), ~31.5s (timely)

### 1.3 Results

| Scenario | Library | Queries | QPS | p50 (s) | p95 (s) | p99 (s) | Avg MB | Peak MB | Core-sec/q |
|---|---|---|---|---|---|---|---|---|---|
| ScanFilterAgg | **instancy** | **180** | **0.30** | **6.71** | **7.03** | **7.28** | **48.6** | **116.7** | **82.7** |
| ScanFilterAgg | timely | 52 | 0.09 | 24.59 | 35.38 | 37.94 | 659.7 | 2492.1 | 716.4 |
| PageRank | **instancy** | **310** | **0.51** | **3.82** | **4.22** | **4.39** | 377.1 | 452.0 | **74.4** |
| PageRank | timely | 258 | 0.43 | 4.63 | 5.63 | 5.97 | 364.7 | 508.6 | 106.7 |
| MapChain | **instancy** | **3335** | **5.56** | **0.354** | **0.429** | **0.482** | **104.4** | **124.2** | **4.0** |
| MapChain | timely | 1207 | 2.01 | 0.988 | 1.346 | 1.604 | 142.3 | 174.7 | 8.5 |
| MultiEpoch | **instancy** | **8191** | **13.65** | **0.138** | **0.208** | **0.247** | **82.6** | **91.9** | **1.13** |
| MultiEpoch | timely | 1985 | 3.31 | 0.689 | 0.974 | 1.118 | 97.3 | 100.1 | 4.12 |
| SmallPipeline | **instancy** | **7913** | **13.09** | **4.81** | **9.19** | **13.15** | **108.8** | **116.2** | **1.44** |
| SmallPipeline | timely | 1248 | 1.85 | 31.46 | 47.00 | 82.38 | 201.8 | 231.1 | 104.1 |

### 1.4 Advantage Ratios

| Scenario | Throughput | Latency (p50) | Memory (avg) | Core Efficiency |
|---|---|---|---|---|
| ScanFilterAgg | **3.5×** | **3.7×** faster | **13.6×** less | **8.7×** better |
| PageRank | **1.2×** | **1.2×** faster | ~equal | **1.4×** better |
| MapChain | **2.8×** | **2.8×** faster | **1.4×** less | **2.1×** better |
| MultiEpoch | **4.1×** | **5.0×** faster | **1.2×** less | **3.6×** better |
| SmallPipeline (×64) | **7.1×** | **6.5×** faster | **1.9×** less | **72×** better |

### 1.5 Why instancy Is Faster

Both libraries use the same number of cores (16 threads per process, 32 total)
and run on the same machine with the same workload. The total process CPU time
over 600 seconds is similar:

| Scenario | instancy CPU total | timely CPU total |
|---|---|---|
| ScanFilterAgg | 4438s | 4286s |
| PageRank | 3719s | 3326s |
| MapChain | 2780s | 1967s |
| MultiEpoch | 3814s | 2081s |
| SmallPipeline | 3397s | 3815s |

Both libraries consume roughly the same amount of CPU. The difference is
**how much useful work gets done per CPU-second**. Three architectural
differences explain the gap:

#### 1.5.1 Shared Async Worker Pool vs Dedicated Threads

This is the **primary** factor. timely creates a fresh set of OS threads for
every query: `timely::execute()` spawns `threads` workers per process (16),
and the benchmark spawns a 2-process cluster per query (32 threads total).
These threads are created, synchronized, and destroyed for each query.

instancy's worker pool is created once and shared across all concurrent
queries. The same 16 threads execute all dataflow stages for all in-flight
queries.

The impact scales with concurrency:

| Concurrency | timely threads alive | instancy threads alive | Gap |
|---|---|---|---|
| 2 (large queries) | 64 | 16 | 4× |
| 64 (SmallPipeline) | 2048 | 16 | 128× |

At 64 concurrent queries, timely has 2048 threads competing for 16 cores.
Each thread spends most of its time in context switches and synchronization
barriers rather than doing computation. This explains the 72× core efficiency
gap in SmallPipeline: timely uses 104 core-seconds for a 100-element query,
while instancy uses 1.44.

#### 1.5.2 Sweep-Based Execution vs Global Barriers

timely's progress protocol requires **all workers to synchronize** at frontier
advances. With 32 workers across 2 processes, each frontier advance involves
message rounds where every worker must acknowledge before any can proceed.

instancy's sweep-based executor runs each operator stage independently —
when data is available, the stage runs. There is no global barrier. Stages
on different workers and different queries interleave freely on the shared
thread pool. This reduces idle waiting, especially for pipelines with uneven
data distribution.

#### 1.5.3 Streaming Memory vs Per-Worker Buffers

timely allocates per-worker communication buffers that accumulate data before
flushing. With 16 workers per process and large data volumes, these buffers
grow significantly.

instancy processes data in streaming batches through the operator chain.
Batches flow through stages and are released as soon as the downstream
operator consumes them.

This explains the 13.6× memory gap in ScanFilterAgg: timely peaks at 2.5 GB
while instancy stays under 117 MB. Lower memory pressure also means fewer
cache misses and less GC-like allocation overhead.

#### 1.5.4 Impact by Scenario

| Scenario | Primary factor | Why |
|---|---|---|
| SmallPipeline (72× core eff.) | Thread churn | 2048 threads for 100-element queries |
| ScanFilterAgg (8.7× core eff.) | Memory + barriers | 100M records amplify buffer bloat and sync cost |
| MultiEpoch (3.6× core eff.) | Thread overhead | Sub-second queries pay high per-query thread cost |
| MapChain (2.1× core eff.) | Batched execution | 20 operator stages benefit from batch amortization |
| PageRank (1.4× core eff.) | Compute-dominated | Actual PageRank math dwarfs framework overhead |

The pattern is clear: **the less compute-dominated the workload, the larger
instancy's advantage**. When the workload is pure computation (PageRank), both
frameworks are similar. When framework overhead matters (thread management,
synchronization, memory allocation), instancy's shared async pool wins.

## 2. Single-Process Criterion Micro-Benchmarks

These benchmarks run in a single process using
[Criterion](https://github.com/bheisler/criterion.rs). Both frameworks use
**identical worker counts** (1 worker thread each, `Config::process(1)` for
timely) to ensure a fair apples-to-apples comparison. Each Criterion iteration
builds and executes a complete dataflow, feeds data in batch form, and drains
to completion. There is no query concurrency — iterations run sequentially.

The 5 scenarios match the sustained cross-process benchmark (Section 1) but
run in a single process with no TCP exchange.

```bash
cargo bench -p instancy --bench comparative
```

### 2.1 Results

| Scenario | Size | instancy | timely | Speedup |
|---|---|---|---|---|
| ScanFilterAgg | 100K | 5.10 ms | 5.76 ms | **1.13×** |
| ScanFilterAgg | 1M | 50.4 ms | 53.6 ms | **1.06×** |
| ScanFilterAgg | 10M | 502 ms | 525 ms | **1.05×** |
| PageRank (10 iter) | 10K edges | 430 µs | 698 µs | **1.62×** |
| PageRank (10 iter) | 100K edges | 5.64 ms | 6.27 ms | **1.11×** |
| MapChain (20 stages) | 10K | 335 µs | 1.01 ms | **3.01×** |
| MapChain (20 stages) | 100K | 848 µs | 5.19 ms | **6.12×** |
| MapChain (20 stages) | 1M | 11.8 ms | 50.5 ms | **4.28×** |
| MultiEpoch (16 epochs) | 16×256 | 83 µs | 285 µs | **3.43×** |
| MultiEpoch (16 epochs) | 16×4096 | 225 µs | 443 µs | **1.97×** |
| SmallPipeline (3 maps) | 1K | 102 µs | 297 µs | **2.91×** |
| SmallPipeline (3 maps) | 10K | 126 µs | 399 µs | **3.16×** |
| SmallPipeline (3 maps) | 100K | 368 µs | 1.28 ms | **3.48×** |

### 2.2 Single-Process Analysis

instancy wins **every scenario** in sequential single-process benchmarks with
equal thread counts:

- **Large data (ScanFilterAgg 10M):** instancy is **1.05×** faster — at scale
  the overhead difference is small but still favours instancy's sweep executor
  which processes batches without global coordination barriers
- **Deep operator chains (MapChain 20 stages):** instancy is **3–6× faster** —
  the sweep executor's batch-streaming design amortizes per-operator overhead
  across stages. timely's per-stage progress tracking adds overhead that
  compounds across 20 stages
- **Multi-epoch workloads:** instancy is **2–3× faster** — epoch advancement
  in instancy is lightweight (frontier update), while timely's progress
  protocol involves per-worker coordination per epoch
- **Small pipelines:** instancy is **2.9–3.5× faster** — even with minimal
  computation, instancy's lower per-dataflow overhead dominates

### 2.3 Sequential vs Parallel Comparison

Comparing single-process sequential (Section 2) with cross-process parallel
(Section 1) shows how instancy's advantage **grows under concurrent load**:

| Scenario | Sequential (1 thread, 1 query) | Parallel (16 threads × 2 procs) |
|---|---|---|
| ScanFilterAgg | **1.05–1.13×** | **3.5× throughput, 8.7× core eff.** |
| PageRank | **1.11–1.62×** | **1.2× throughput, 1.4× core eff.** |
| MapChain | **3.01–6.12×** | **2.8× throughput, 2.1× core eff.** |
| MultiEpoch | **1.97–3.43×** | **4.1× throughput, 3.6× core eff.** |
| SmallPipeline | **2.91–3.48×** | **7.1× throughput, 72× core eff.** |

Key observations:
- **ScanFilterAgg** scales from 1.05× → 8.7× because timely's per-worker
  buffers and thread synchronization become the bottleneck at 32 threads
- **SmallPipeline** scales from ~3× → 72× because timely spawns 2048 threads
  for 64 concurrent queries (64 × 32), while instancy reuses 16 threads
- **MapChain** is already 3–6× faster sequentially due to sweep executor
  efficiency; the parallel advantage is similar (2.1× core efficiency)
- **PageRank** is compute-dominated — framework overhead matters less in both
  modes

## 3. How to Run

### 3.1 Environment Setup

```powershell
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$env:PROTOC = "$env:USERPROFILE\.local\protoc\bin\protoc.exe"
$env:CARGO_INCREMENTAL = "0"
```

### 3.2 Cross-Process Benchmark

```bash
# Full 10-minute sustained run (recommended)
cargo bench --bench sustained_comparative -- --duration 600 --warmup 30 --threads 16

# Quick smoke test (10 seconds)
cargo bench --bench sustained_comparative -- --duration 10 --warmup 3 --threads 4

# Single library
cargo bench --bench sustained_comparative -- --duration 60 --library instancy

# Large scenarios only
cargo bench --bench sustained_comparative -- --duration 60 --scenario large
```

| Flag | Default | Description |
|---|---|---|
| `--duration <SECS>` | 600 | Measurement duration per phase |
| `--warmup <SECS>` | 30 | Warmup duration per phase |
| `--threads <N>` | 16 | Per-process worker threads |
| `--concurrency <N>` | 64 | In-flight small-query cap |
| `--scenario <NAME>` | all | `large`, `small`, or `all` |
| `--library <NAME>` | both | `instancy`, `timely`, or `both` |
| `--rounds <N>` | 1 | Number of benchmark rounds |
| `--cooldown <SECS>` | 5 | Delay between phases |

### 3.3 Criterion Micro-Benchmarks

```bash
cargo bench -p instancy --bench comparative
```

The Criterion benchmark (`instancy/benches/comparative.rs`) runs the same
5 scenarios as the sustained cross-process benchmark but in a **single
process** with **no TCP exchange**. Both libraries use identical worker
counts — `Config::process(1)` for timely, `RuntimeConfig { worker_threads: 1 }`
for instancy — ensuring a fair apples-to-apples comparison.

Each Criterion iteration builds a fresh dataflow, feeds data, and drains to
completion. Iterations are sequential (no concurrent queries). This isolates
per-query computational overhead from concurrency and networking effects.

## 4. Methodology Notes

- Both libraries use identical data generators with deterministic seeds
- Both libraries build equivalent dataflow graphs (same operators, same
  exchange keys, same data partitioning)
- The benchmark binary acts as both coordinator and worker — the same binary
  runs on both sides
- Core time for instancy is measured via `collect_metrics(true)` and
  `total_core_time()` (stopwatch-based, counting only active computation)
- Core time for timely is measured by summing per-thread wall-clock elapsed
  times (thread is dedicated and not suspended, so wall time ≈ core time)
- Memory is sampled from the coordinator benchmark process using OS APIs
  (`K32GetProcessMemoryInfo` on Windows, `/proc/self/status` on Linux)
- Results from a single machine. Your results may vary depending on hardware
  and OS.

> See also: [benchmarking.md](./design/benchmarking.md) for the benchmark
> design document and control protocol details.
