# Benchmarking Plan: instancy vs timely-dataflow

This document describes the sustained comparative benchmarking methodology for
instancy against timely-dataflow. Follow this procedure to produce reproducible
performance numbers.

## 1. Overview

The sustained benchmark now exercises two workload groups with five concrete
scenarios:

| Group | Scenario | Goal | Workload |
|------|----------|------|----------|
| **Large queries** | **ScanFilterAgg** | Compute-heavy batch query | 10M-record scan/filter/aggregate |
| **Large queries** | **PageRank** | Compute-heavy iterative batch query | 50K vertices, 500K edges, 20 iterations |
| **Large queries** | **MapChain10** | Operator-chaining throughput | 1M values through 10 `.map()` stages |
| **Large queries** | **MultiEpochFilter** | Steady-state single-dataflow throughput | 1024 epochs x 64 records through one filter dataflow |
| **Small queries** | **SmallPipelineConcurrent** | Small-query overhead under concurrency | 100-element 3-stage pipeline with configurable concurrency |

Each (library, scenario) pair runs continuously for a configurable duration
(default **10 minutes**) after a warmup phase.

## 2. What Is Measured

| Metric | How |
|--------|-----|
| **Per-query latency** | `Instant::now()` around each complete query/dataflow execution |
| **Throughput** | Queries/sec and elements/sec derived from completed query count divided by wall time |
| **Latency percentiles** | p50, p95, p99, max from sorted latency samples |
| **Memory** | Process working set / RSS sampled periodically during each run |
| **CPU time** | User + kernel CPU time delta via `GetProcessTimes` (Windows) |

## 3. Test Scenarios

### 3.1 Scenario 1A - Scan-Filter-Aggregate (Large)

Processes 10,000,000 synthetic TPC-H-like `LineItem` records through:
```
source -> filter(ship_date < 11000) -> aggregate(group by flag/status, sum qty+price) -> sink
```

- **instancy**: `DataflowBuilder::source()` -> `filter()` -> `unary_notify()` -> `for_each()`
- **timely**: `scope.new_input()` -> `filter()` -> `unary_notify()` -> `inspect()` -> `probe()`
- **Data**: Deterministic pseudo-random (LCG seed=42), identical for both libraries

### 3.2 Scenario 1B - PageRank (Large)

Runs 20 iterations of PageRank on a 50,000-vertex, 500,000-edge random graph:
```
source(edges) -> unary_notify(compute_pagerank) -> sink
```

Both libraries use the same sequential PageRank implementation to isolate
framework overhead from algorithm differences.

### 3.3 Scenario 1C - 10-Stage Map Chain (Large)

Processes 1,000,000 `i64` values through ten consecutive `.map()` operators:
```
source -> map(+1) x 10 -> sink
```

This matches the previous criterion-style Q4 workload where instancy benefited
from efficiently running many simple operators on batched data.

### 3.4 Scenario 1D - Multi-Epoch Filter (Steady State)

Builds one dataflow and feeds 1024 epochs of 64 records each through an input:
```
input(epoch batches) -> filter(value > total/2) -> sink
```

- **instancy**: external input via `builder.input()` and `take_input()`
- **timely**: `new_input()`, `advance_to()`, `send_batch()`
- Purpose: measure steady-state throughput without per-dataflow spawn overhead

### 3.5 Scenario 2 - Concurrent High-RPS Small Pipeline

Each query processes 100 `i64` elements through:
```
source -> map(+1) -> map(*2) -> map(-1) -> sink
```

Unlike the old benchmark, the small-query scenario is deliberately concurrent.
That is the key methodology change.

#### instancy methodology

Inside `tokio::runtime::Runtime::block_on`:
- create a shared `tokio::sync::Semaphore` with `--concurrency` permits
- repeatedly acquire a permit and `tokio::spawn` a task
- each task builds a small dataflow, calls `rt.spawn(...)`, and awaits `handle.join().await`
- task latency is reported back to a collector channel
- completed tasks release their permit, allowing the next query to start

This exercises instancy's intended design point: many dataflows executing
concurrently on a shared async worker pool.

#### timely methodology

timely does not have an equivalent async shared-runtime API for this benchmark,
so fairness comes from giving it the same concurrency level with a thread pool:

- spawn `--concurrency` worker threads
- each thread repeatedly runs `execute_directly` in a loop until the run ends
- each completed query reports latency back to the collector

This compares concurrent instancy execution against concurrent timely execution,
rather than forcing instancy to run sequentially via `join_blocking()`.

## 4. Environment Requirements

- **Rust**: stable >= 1.85 (2024 edition)
- **Build**: `--release` mode for real measurements
- **OS**: Windows 10/11 or Linux
- **Hardware**: Dedicated machine or quiet VM; close other workloads
- **Protobuf**: `PROTOC` environment variable set if required by your build

## 5. Running the Benchmark

### 5.1 Quick Validation Run

```powershell
$env:CARGO_INCREMENTAL = "0"
cargo bench --bench sustained_comparative --release -- --duration 30 --warmup 5 --concurrency 64
```

### 5.2 Full Production Run (~106 minutes)

```powershell
$env:CARGO_INCREMENTAL = "0"
cargo bench --bench sustained_comparative --release -- --duration 600 --warmup 30 --concurrency 64
```

This runs 10 benchmark pairs (5 scenarios x 2 libraries). At 600s measurement
+ 30s warmup + 5s cooldown per pair, total runtime is about 6,350 seconds
(~106 minutes).

### 5.3 CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--duration <SECS>` | 600 | Measurement duration per (library, scenario) pair |
| `--warmup <SECS>` | 30 | Warmup duration before measurement starts |
| `--rounds <N>` | 1 | Number of complete rounds |
| `--scenario <NAME>` | all | Filter: `large`, `small`, or `all` |
| `--library <NAME>` | both | Filter: `instancy`, `timely`, or `both` |
| `--cooldown <SECS>` | 5 | Pause between runs |
| `--concurrency <N>` | 64 | In-flight async queries for instancy and worker-thread count for timely small-query runs |

### 5.4 Selective Runs

```powershell
# Only compute-heavy / steady-state scenarios
cargo bench --bench sustained_comparative --release -- --scenario large --duration 600

# Only concurrent small queries with higher fan-out
cargo bench --bench sustained_comparative --release -- --scenario small --duration 600 --concurrency 128
```

## 6. Interpreting Results

### 6.1 Per-Run Report

Each run prints completed queries, QPS, latency percentiles, memory, CPU, and
wall-clock time.

### 6.2 Summary Table

At the end, the benchmark prints one row per (scenario, library) pair.
Expected scenario names are:
- `ScanFilterAgg`
- `PageRank`
- `MapChain10`
- `MultiEpochFilter`
- `SmallPipelineConcurrent`

### 6.3 Key Comparisons

| What to compare | What it tells you |
|-----------------|-------------------|
| **QPS ratio** (instancy/timely) | Overall throughput comparison |
| **p50 ratio** | Typical query latency comparison |
| **p99 ratio** | Tail latency comparison |
| **Memory delta** | Framework memory overhead difference |
| **CPU time delta** | CPU efficiency |

### 6.4 Known Measurement Limitation

Memory is sampled at the process level. The tokio runtime and instancy
`RuntimeHandle` remain alive for the entire process, so timely memory numbers
include idle instancy baseline overhead. CPU deltas are still meaningful because
idle runtime threads should consume negligible CPU.

For cleaner memory isolation, run each library separately:
```powershell
cargo bench --bench sustained_comparative --release -- --library instancy --duration 600
cargo bench --bench sustained_comparative --release -- --library timely   --duration 600
```

### 6.5 Expected Characteristics

- **Large queries**: computation should dominate framework setup overhead, so
  ScanFilterAgg and PageRank should be closer to prior criterion results.
- **MapChain10**: simple operator chains can favor instancy when batched async
  execution overhead stays low relative to the work done.
- **MultiEpochFilter**: focuses on steady-state execution after one dataflow is
  already built.
- **SmallPipelineConcurrent**: instancy's intended advantage is concurrent
  execution on a shared async pool, not sequential `spawn() -> join_blocking()`.

## 7. Hang Detection

The benchmark prints periodic progress updates. If progress stops for an
extended period, a dataflow or worker may be stuck. Use `Ctrl+C` to abort and
investigate.

For automated CI, wrap the benchmark in a system-level timeout.

## 8. Reproducing Past Results

To reproduce a prior run:
1. Check out the same git commit
2. Use the same hardware and OS
3. Close other workloads
4. Use the same CLI arguments, especially `--duration`, `--warmup`, and `--concurrency`
5. Run in `--release` mode with `CARGO_INCREMENTAL=0`
6. Use multiple rounds if you need higher confidence

## 9. Extending the Benchmark

To add a new scenario:
1. Implement the instancy and timely variants
2. Add data generation if needed
3. Add the run in `main()`
4. Update this document

## 10. File Locations

| File | Purpose |
|------|---------|
| `instancy/benches/sustained_comparative.rs` | Sustained benchmark binary |
| `instancy/benches/comparative.rs` | Criterion micro-benchmarks |
| `docs/design/benchmarking.md` | This document |
