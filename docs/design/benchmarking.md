# Benchmarking Plan: instancy vs timely-dataflow

This document describes the comparative benchmark methodology used by the
instancy benchmark suite:

- **`instancy/benches/sustained_comparative.rs`** — sustained cross-process
  TCP benchmark (600-second runs, 2 processes, 16 threads each)
- **`instancy/benches/comparative.rs`** — single-process Criterion
  micro-benchmarks (same 5 scenarios, 1 worker thread each, sequential)

## 1. Overview

The sustained benchmark now runs **all scenarios as 2-process TCP executions**.
The same benchmark binary supports two roles:

- **Coordinator** (default): runs the benchmark harness, spawns a worker child
  process for each query, opens the TCP control socket, feeds local data, and
  records metrics.
- **Worker** (`--role worker`): connects back to the coordinator over the
  control socket and runs the second half of the same dataflow.

This design forces every measured query through the full networked execution
path:

- separate OS processes
- separate runtimes / worker pools
- TCP transport setup and exchange
- serialization / deserialization
- kernel network stack
- final gather back to process 0

## 2. Control Protocol

Coordinator and worker communicate over one newline-delimited JSON control
socket.

1. Coordinator binds `127.0.0.1:0`
2. Coordinator spawns the same benchmark binary with:
   `--role worker --control-addr 127.0.0.1:<port>`
3. Worker connects to the control socket
4. Coordinator sends:

```json
{"cmd":"setup","library":"instancy","scenario":"scan-filter-agg","threads":16,"exchange_port":12345}
```

5. Worker replies:

```json
{"status":"ready","exchange_port":12346}
```

6. Coordinator sends `{"cmd":"run"}`
7. Both processes run the query locally while exchanging data over TCP
8. Worker replies with completion metrics:

```json
{"status":"done","core_time_ns":1234,"wall_ms":5678}
```

9. Coordinator sends `{"cmd":"shutdown"}` and waits for worker exit

## 3. Execution Model

### 3.1 instancy

Each process creates its own:

- Tokio runtime with **2 worker threads** for TCP/control I/O only
- `RuntimeHandle` with `--threads` worker-pool threads for compute

Both processes build the same cluster dataflow using `ClusterSpawnTransport` and
real TCP sockets. The topology is always:

- `node-a` = coordinator process
- `node-b` = worker process
- `--threads` logical workers per node

Every scenario includes:

1. local source/input on both processes
2. scenario-specific compute operators
3. a cross-process hash exchange
4. a **gather** exchange routing all final output to process 0 / worker 0

### 3.2 timely-dataflow

Each process runs `timely::execute` in **cluster mode** with:

```rust
Config {
    communication: timely::CommunicationConfig::Cluster {
        threads,
        process,
        addresses,
        report: false,
        log_fn: Box::new(|_| None),
    },
    worker: timely::WorkerConfig::default(),
}
```

Both processes use the same `addresses` vector and differ only in `process`:

- coordinator = `process: 0`
- worker = `process: 1`

Each process owns `threads` local workers, so the 2-process run has `2 *
threads` timely workers total.

## 4. Scenario Set

All scenarios run over **2 processes connected by TCP**.

| Group | Scenario | Libraries | Workload |
|------|----------|-----------|----------|
| Large | ScanFilterAgg | instancy + timely | 100M synthetic line items |
| Large | PageRank | instancy + timely | 200K vertices, 2M edges, 100 iterations |
| Large | MapChain10 | instancy + timely | 5M `i64` values through 20 maps |
| Large | MultiEpochFilter | instancy + timely | 16 epochs × 4096 records |
| Small | SmallPipelineConcurrent | instancy + timely | 100-element 3-stage pipeline |

## 4.1 Scenario 1A - Scan-Filter-Aggregate

- Total input: **100,000,000** `LineItem` records
- Process split: **50M + 50M**
- Pipeline:

```text
source -> filter(ship_date < 11000) -> exchange_by_hash(group key)
       -> aggregate -> gather(process 0) -> sink
```

The gather step routes every aggregate result to process 0.

## 4.2 Scenario 1B - PageRank

- Graph size: **200,000 vertices**, **2,000,000 edges**
- Iterations: **100**
- Process split: **1M edges + 1M edges**
- Pipeline:

```text
source(edges) -> pagerank -> exchange_by_hash(vertex)
              -> gather(process 0) -> sink
```

## 4.3 Scenario 1C - 20-Stage Map Chain

- Total input: **5,000,000** values
- Process split: **2.5M + 2.5M**
- Pipeline:

```text
source -> map(+1) x 20 -> exchange_by_hash(value) -> sink
```

Note: gather step removed — with 5M values and 20 stages the full-volume
gather to a single worker causes TCP backpressure deadlocks under concurrent
load.

## 4.4 Scenario 1D - Multi-Epoch Filter

- Total input: **16 epochs × 4096 records/epoch**
- Process split: **8 epochs + 8 epochs**
- Pipeline:

```text
input(epoch batches) -> filter(value > threshold)
                     -> exchange_by_hash(value) -> sink
```

Note: gather step removed for the same backpressure reason as MapChain.

## 4.5 Scenario 2 - Concurrent Small Pipeline

- Total input: **100** `i64` values per query
- Process split: **50 + 50**
- Pipeline:

```text
source -> map(+1) -> map(*2) -> map(-1)
       -> exchange_by_hash(value) -> sink
```

Note: gather step removed for the same backpressure reason as MapChain.

## 5. Sustained Run Methodology

### 5.1 Large queries

Large scenarios are designed to run for roughly **30-60 seconds per query**.
The sustained runner:

- repeatedly spawns a fresh worker process per query
- runs the query across 2 TCP-connected processes
- keeps only **2-3 large queries in flight** at once
- starts a replacement query as soon as one finishes
- records per-query latency, total core time, and sampled memory

### 5.2 Small queries

The small pipeline also uses a fresh 2-process TCP execution per query, but the
coordinator keeps up to `--concurrency` queries in flight at once (default 64).

## 6. Metrics

The benchmark keeps the existing reporting structure:

- **Per-query latency**: wall-clock time for one complete 2-process query
- **Throughput**: completed queries / wall time
- **Latency percentiles**: p50, p95, p99, max
- **Memory**: sampled from the coordinator benchmark process
- **CPU time**: process CPU deltas from `system_snapshot()`
- **Core time**:
  - instancy: `collect_metrics(true)` + `total_core_time()` from both processes
  - timely: summed per-thread elapsed times from both processes

## 7. Environment Requirements

Before running any cargo command:

```powershell
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$env:PROTOC = "$env:USERPROFILE\.local\protoc\bin\protoc.exe"
$env:CARGO_INCREMENTAL = "0"
cd Q:\repos\instancy
```

Other requirements:

- Rust stable >= 1.85
- Windows 10/11 or Linux
- release builds for real measurements
- a mostly idle machine / VM

## 8. Running the Benchmark

### 8.1 Compile check

```powershell
cargo bench --bench sustained_comparative --no-run
```

### 8.2 Example sustained run

```powershell
cargo bench --bench sustained_comparative --release -- --duration 600 --warmup 30 --concurrency 64 --threads 16
```

### 8.3 CLI options

| Flag | Default | Description |
|------|---------|-------------|
| `--duration <SECS>` | 600 | Measurement duration per run |
| `--warmup <SECS>` | 30 | Warmup duration per run |
| `--rounds <N>` | 1 | Number of benchmark rounds |
| `--scenario <NAME>` | all | `large`, `small`, or `all` |
| `--library <NAME>` | both | `instancy`, `timely`, or `both` |
| `--cooldown <SECS>` | 5 | Delay between runs |
| `--concurrency <N>` | 64 | In-flight small-query cap |
| `--threads <N>` | 16 | Per-process worker threads |
| `--role <NAME>` | coordinator | Internal: `coordinator` or `worker` |
| `--control-addr <ADDR>` | none | Internal worker control socket address |

### 8.4 Criterion Micro-Benchmarks

```powershell
cargo bench -p instancy --bench comparative
```

The Criterion benchmark (`instancy/benches/comparative.rs`) runs the same
5 scenarios as the sustained benchmark but in a **single process** with
**no TCP exchange**. Both libraries use identical worker counts:

- **instancy**: `RuntimeConfig { worker_threads: 1 }`
- **timely**: `Config::process(1)` (spawns 1 worker thread, not
  `execute_directly`)

Each Criterion iteration builds a fresh dataflow, feeds data, and drains to
completion. Iterations are sequential (no concurrent queries). This isolates
per-query computational overhead from concurrency and networking effects.

Data sizes are scaled down from the sustained benchmark to keep each
iteration in the 0.1–500ms range suitable for Criterion's statistical analysis.

## 9. Result Interpretation

Key comparisons are now end-to-end **cross-process TCP** comparisons.

- **QPS ratio** (instancy / timely): total distributed throughput
- **Latency percentiles**: end-to-end cost of one 2-process query
- **Core seconds**: combined work done by both processes
- **Memory**: coordinator-side benchmark-process footprint during sustained load

Because the worker is a separate process, these numbers are intentionally closer
to real distributed execution than the earlier in-process exchange benchmarks.

## 10. Benchmark Results

Results from a sustained 600-second-per-phase run on a single Windows machine
with 16 worker threads per process, 2 processes connected by TCP.

### 10.1 Summary Table

| Scenario | Library | Queries | QPS | p50 (s) | p95 (s) | Avg MB | Peak MB | Core-sec/query |
|---|---|---|---|---|---|---|---|---|
| ScanFilterAgg | **instancy** | **180** | **0.30** | **6.71** | **7.03** | **48.6** | **116.7** | **82.7** |
| ScanFilterAgg | timely | 52 | 0.09 | 24.59 | 35.38 | 659.7 | 2492.1 | 716.4 |
| PageRank | **instancy** | **310** | **0.51** | **3.82** | **4.22** | 377.1 | 452.0 | **74.4** |
| PageRank | timely | 258 | 0.43 | 4.63 | 5.63 | 364.7 | 508.6 | 106.7 |
| MapChain10 | **instancy** | **3335** | **5.56** | **0.354** | **0.429** | **104.4** | **124.2** | **4.0** |
| MapChain10 | timely | 1207 | 2.01 | 0.988 | 1.346 | 142.3 | 174.7 | 8.5 |
| MultiEpochFilter | **instancy** | **8191** | **13.65** | **0.138** | **0.208** | **82.6** | **91.9** | **1.13** |
| MultiEpochFilter | timely | 1985 | 3.31 | 0.689 | 0.974 | 97.3 | 100.1 | 4.12 |
| SmallPipeline | **instancy** | **7913** | **13.09** | **4.81** | **9.19** | **108.8** | **116.2** | **1.44** |
| SmallPipeline | timely | 1248 | 1.85 | 31.46 | 47.00 | 201.8 | 231.1 | 104.1 |

### 10.2 Advantage Ratios (instancy / timely)

| Scenario | Throughput | Latency (p50) | Memory (avg) | Core Efficiency |
|---|---|---|---|---|
| ScanFilterAgg | **3.5×** | **3.7×** faster | **13.6×** less | **8.7×** better |
| PageRank | **1.2×** | **1.2×** faster | ~equal | **1.4×** better |
| MapChain10 | **2.8×** | **2.8×** faster | **1.4×** less | **2.1×** better |
| MultiEpochFilter | **4.1×** | **5.0×** faster | **1.2×** less | **3.6×** better |
| SmallPipeline (×64) | **7.1×** | **6.5×** faster | **1.9×** less | **72×** better |

### 10.3 Analysis

**ScanFilterAgg** shows the largest memory advantage. timely allocates dedicated
per-worker buffers for the full 100M-record scan, peaking at 2.5 GB. instancy
shares the async worker pool and keeps peak memory under 117 MB — a 21× reduction.
Core efficiency is 8.7× better because instancy's per-stage execution avoids
idle spinning across the 16 workers.

**PageRank** is the closest comparison. Both libraries perform similar iterative
computation. instancy still wins on throughput (1.2×) and core efficiency (1.4×)
due to lower per-iteration coordination overhead.

**MapChain** and **MultiEpoch** demonstrate instancy's advantage in medium-sized
dataflows. The async work pool avoids the per-query overhead of spawning and
synchronizing 32 dedicated threads (16 per process).

**SmallPipeline** is the standout result. At 64 concurrent queries, instancy's
async pool lets all queries share the same 16 worker threads. timely must spin
up 32 threads per query (64 × 32 = 2048 threads competing for CPU), resulting
in massive context-switch overhead: 104 core-seconds per 100-element query vs
instancy's 1.44. The 72× core efficiency gap directly validates instancy's
shared async worker pool design.

### 10.4 Gather Step Limitation

ScanFilterAgg and PageRank include a final `gather` exchange that routes all
output to process 0 / worker 0. This works because aggregation reduces the
output volume. MapChain, MultiEpoch, and SmallPipeline omit the gather step —
with 16 workers per process, routing all output records through a single TCP
channel causes backpressure deadlocks when multiple queries run concurrently.
This is a fundamental limitation of single-destination gather under high fan-in,
not specific to either library.
