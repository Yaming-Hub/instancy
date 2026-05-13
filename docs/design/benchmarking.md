# Benchmarking Plan: instancy vs timely-dataflow

This document describes the sustained benchmarking methodology for instancy and
timely-dataflow, plus two instancy-only TCP exchange scenarios.

## 1. Overview

The sustained benchmark exercises two workload groups with seven concrete
scenarios:

| Group | Scenario | Libraries | Goal | Workload |
|------|----------|-----------|------|----------|
| **Large queries** | **ScanFilterAgg** | instancy + timely | Compute-heavy batch query | 10M-record scan/filter/aggregate |
| **Large queries** | **PageRank** | instancy + timely | Compute-heavy iterative batch query | 50K vertices, 500K edges, 20 iterations |
| **Large queries** | **MapChain10** | instancy + timely | Operator-chaining throughput | 1M values through 10 `.map()` stages |
| **Large queries** | **MultiEpochFilter** | instancy + timely | Steady-state single-dataflow throughput | 1024 epochs x 64 records through one filter dataflow |
| **Large queries** | **ExchangeAggregateTcp** | instancy only | Cross-node TCP exchange + aggregation overhead | 10K records/epoch across 2 TCP nodes |
| **Small queries** | **SmallPipelineConcurrent** | instancy + timely | Small-query overhead under concurrency | 100-element 3-stage pipeline |

Each run executes continuously for a configurable duration (default **10
minutes**) after a warmup phase.

## 2. Fair Thread Budget

The comparative runs now use a shared worker-thread budget controlled by
`--threads` (default **16**).

- **instancy**: the shared `RuntimeHandle` is created with
  `RuntimeConfig { worker_threads: --threads, .. }`.
- **timely concurrent small-query scenario**: the benchmark spawns a fixed pool
  of `min(--concurrency, --threads)` OS threads. Each thread loops on
  `execute_directly` until the deadline.
- If `--concurrency > --threads`, instancy queues work behind the runtime and
  timely queues it implicitly because only the fixed thread pool can execute
  queries.
- **Sequential large-query scenarios** stay intentionally sequential per query
  for timely. instancy still runs on the same shared 16-thread runtime, so the
  measured difference is framework overhead, not an artificially larger timely
  thread budget.

## 3. What Is Measured

| Metric | How |
|--------|-----|
| **Per-query latency** | `Instant::now()` around each complete query/dataflow execution |
| **Throughput** | Queries/sec and elements/sec derived from completed query count divided by wall time |
| **Latency percentiles** | p50, p95, p99, max from sorted latency samples |
| **Memory** | Process working set / RSS sampled periodically during each run |
| **CPU time** | User + kernel CPU time delta via `GetProcessTimes` (Windows) |

## 4. Test Scenarios

### 4.1 Scenario 1A - Scan-Filter-Aggregate (Large)

Processes 10,000,000 synthetic TPC-H-like `LineItem` records through:

```text
source -> filter(ship_date < 11000) -> aggregate(group by flag/status, sum qty+price) -> sink
```

- **instancy**: `source()` -> `filter()` -> `unary_notify()` -> `for_each()`
- **timely**: `new_input()` -> `filter()` -> `unary_notify()` -> `inspect()` -> `probe()`
- **Data**: deterministic pseudo-random input, identical for both libraries

### 4.2 Scenario 1B - PageRank (Large)

Runs 20 iterations of PageRank on a 50,000-vertex, 500,000-edge random graph:

```text
source(edges) -> unary_notify(compute_pagerank) -> sink
```

Both libraries use the same sequential PageRank implementation.

### 4.3 Scenario 1C - 10-Stage Map Chain (Large)

Processes 1,000,000 `i64` values through ten consecutive `.map()` operators:

```text
source -> map(+1) x 10 -> sink
```

### 4.4 Scenario 1D - Multi-Epoch Filter (Steady State)

Builds one dataflow and feeds 1024 epochs of 64 records each through an input:

```text
input(epoch batches) -> filter(value > total/2) -> sink
```

Purpose: steady-state throughput after the dataflow already exists.

### 4.5 Scenario 1E - Instancy-only TCP Exchange + Aggregate

Uses `spawn_cluster()` directly inside the benchmark binary with a persistent
2-node TCP cluster and probe-based completion waiting.

- Topology: 2 nodes, 1 logical worker per node
- Transport: real TCP connections in-process (`TcpListener` + `TcpStream`)
- Each "query" = one epoch of 10,000 `(key, value)` records, split evenly
  across the two nodes
- Dataflow:

```text
input -> exchange_by_hash(key) -> probe -> for_each(sink)
```

A `ProbeHandle` on each node waits for frontier advancement after every epoch.
This prevents quadratic progress-tracking state accumulation that would
otherwise hang the dataflow after ~1200 epochs.

This does **not** use `instancy-integration` because the coordinator protocol
and external `instancy-test-node` process would contaminate latency
measurements.

### 4.6 Scenario 2A - Concurrent High-RPS Small Pipeline

Each query processes 100 `i64` elements through:

```text
source -> map(+1) -> map(*2) -> map(-1) -> sink
```

#### instancy methodology

Inside `tokio::runtime::Runtime::block_on`:

- create a shared `tokio::sync::Semaphore` with `--concurrency` permits
- repeatedly acquire a permit and `tokio::spawn` a task
- each task builds a small dataflow, calls `rt.spawn(...)`, and awaits
  `handle.join().await`
- completed tasks release their permit

#### timely methodology

- spawn a fixed pool of `min(--concurrency, --threads)` OS threads
- each thread loops on `execute_directly` until the deadline
- each completed query reports latency to the collector

## 5. Environment Requirements

- **Rust**: stable >= 1.85 (2024 edition)
- **Build**: `--release` mode for real measurements
- **OS**: Windows 10/11 or Linux
- **Hardware**: dedicated machine or quiet VM
- **Protobuf**: `PROTOC` environment variable set if required by your build

## 6. Running the Benchmark

### 6.1 Quick Validation Run

```powershell
$env:CARGO_INCREMENTAL = "0"
cargo bench --bench sustained_comparative --release -- --duration 30 --warmup 5 --concurrency 64 --threads 16
```

### 6.2 Full Production Run (~115 minutes)

```powershell
$env:CARGO_INCREMENTAL = "0"
cargo bench --bench sustained_comparative --release -- --duration 600 --warmup 30 --concurrency 64 --threads 16
```

With `--library both`, one round now executes 11 runs:

- 4 comparative scenarios × 2 libraries = 8 runs
- 1 instancy-only TCP exchange scenario = 1 run
- 1 small-pipeline scenario × 2 libraries = 2 runs

At 600s measurement + 30s warmup + 5s cooldown per run, total runtime is about
6,985 seconds (~116 minutes).

### 6.3 CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--duration <SECS>` | 600 | Measurement duration per run |
| `--warmup <SECS>` | 30 | Warmup duration before measurement starts |
| `--rounds <N>` | 1 | Number of complete rounds |
| `--scenario <NAME>` | all | Filter: `large`, `small`, or `all` |
| `--library <NAME>` | both | Filter: `instancy`, `timely`, or `both` |
| `--cooldown <SECS>` | 5 | Pause between runs |
| `--concurrency <N>` | 64 | In-flight query cap for the small-query scenarios |
| `--threads <N>` | 16 | Shared worker-thread budget for comparative instancy/timely runs |

### 6.4 Selective Runs

```powershell
# Only comparative large scenarios plus the instancy TCP aggregate exchange run
cargo bench --bench sustained_comparative --release -- --scenario large --duration 600 --threads 16

# Only small-query scenarios
cargo bench --bench sustained_comparative --release -- --scenario small --duration 600 --concurrency 128 --threads 16

# Only instancy, including the TCP exchange scenarios
cargo bench --bench sustained_comparative --release -- --library instancy --duration 600 --threads 16
```

## 7. Interpreting Results

### 7.1 Summary Rows

Expected scenario names are:

- `ScanFilterAgg`
- `PageRank`
- `MapChain10`
- `MultiEpochFilter`
- `ExchangeAggregateTcp`
- `SmallPipelineConcurrent`

### 7.2 Key Comparisons

| What to compare | What it tells you |
|-----------------|-------------------|
| **QPS ratio** (instancy/timely) | Overall throughput comparison for the 4 comparative scenarios |
| **p50 / p99 ratios** | Typical and tail latency comparison |
| **Memory delta** | Framework memory overhead difference |
| **CPU time delta** | CPU efficiency |
| **ExchangeAggregateTcp** | Instancy TCP transport overhead without control-plane noise |

### 7.3 Known Measurement Limitation

Memory is sampled at the process level. The tokio runtime and instancy
`RuntimeHandle` remain alive for the full benchmark process, so timely memory
numbers include idle instancy baseline overhead.

For cleaner isolation, run each library separately:

```powershell
cargo bench --bench sustained_comparative --release -- --library instancy --duration 600 --threads 16
cargo bench --bench sustained_comparative --release -- --library timely   --duration 600 --threads 16
```

## 8. Reproducing Past Results

To reproduce a prior run:

1. Check out the same git commit
2. Use the same hardware and OS
3. Close other workloads
4. Use the same CLI arguments, especially `--duration`, `--warmup`,
   `--concurrency`, and `--threads`
5. Run in `--release` mode with `CARGO_INCREMENTAL=0`
6. Use multiple rounds if you need higher confidence

## 9. Benchmark Results

### 9.1 Results — 60-second sustained run (16 threads, concurrency 64)

Hardware: Windows 11, AMD/Intel desktop, 16 logical cores.

| Scenario | Library | QPS | p50 (µs) | p95 (µs) | p99 (µs) | Ratio |
|----------|---------|----:|----------:|----------:|----------:|------:|
| ScanFilterAgg 10M | instancy | 1.9 | 536,056 | 556,738 | 573,821 | **0.68×** |
| | timely | 2.8 | 353,127 | 369,531 | 381,725 | |
| PageRank 50K/500K/20 | instancy | 16.6 | 59,598 | 70,189 | 78,225 | **0.96×** |
| | timely | 17.3 | 55,662 | 73,143 | 87,831 | |
| MapChain10 1M | instancy | **110.9** | 8,871 | 10,092 | 11,490 | **2.96×** |
| | timely | 37.5 | 26,122 | 29,616 | 33,451 | |
| MultiEpochFilter 1024×64 | instancy | **2,364** | 396 | 522 | 839 | **1.72×** |
| | timely | 1,373 | 705 | 853 | 1,163 | |
| ExchangeAggregateTcp 10K | instancy | 966 | 262 | 9,803 | 15,108 | — |
| SmallPipeline ×64 | instancy | **113,725** | 373 | 625 | 1,374 | **1.13×** |
| | timely | 101,019 | 138 | 168 | 320 | |

**Summary:**

- instancy wins on operator-chaining throughput (**MapChain10**: 2.96×) and
  steady-state epoch-based workloads (**MultiEpochFilter**: 1.72×) due to its
  async task-pool model and lower per-operator overhead.
- timely wins on single large-batch queries (**ScanFilterAgg**: 1.47× timely
  advantage) where its sync worker threads avoid task scheduling overhead.
- **PageRank** is nearly equal (0.96×), indicating similar iterative-loop
  efficiency.
- **SmallPipeline** concurrent throughput favors instancy (1.13×) though timely
  has significantly lower per-query latency (138µs vs 373µs p50).
- **Exchange TCP** achieves ~966 QPS sustained across 2 in-process TCP nodes
  with probe-based epoch completion.

## 10. File Locations

| File | Purpose |
|------|---------|
| `instancy/benches/sustained_comparative.rs` | Sustained benchmark binary |
| `instancy/benches/comparative.rs` | Criterion micro-benchmarks |
| `docs/design/benchmarking.md` | This document |
