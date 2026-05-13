# Benchmarking Plan: instancy vs timely-dataflow

This document describes the sustained comparative benchmark methodology used by
`instancy/benches/sustained_comparative.rs`.

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
| Large | PageRank | instancy + timely | 200K vertices, 2M edges, 50 iterations |
| Large | MapChain10 | instancy + timely | 50M `i64` values through 10 maps |
| Large | MultiEpochFilter | instancy + timely | 8192 epochs  512 records |
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
- Iterations: **50**
- Process split: **1M edges + 1M edges**
- Pipeline:

```text
source(edges) -> pagerank -> exchange_by_hash(vertex)
              -> gather(process 0) -> sink
```

## 4.3 Scenario 1C - 10-Stage Map Chain

- Total input: **50,000,000** values
- Process split: **25M + 25M**
- Pipeline:

```text
source -> map(+1) x 10 -> exchange_by_hash(value)
       -> gather(process 0) -> sink
```

## 4.4 Scenario 1D - Multi-Epoch Filter

- Total input: **8192 epochs  512 records/epoch**
- Process split: **4096 epochs + 4096 epochs**
- Pipeline:

```text
input(epoch batches) -> filter(value > threshold)
                     -> exchange_by_hash(value)
                     -> gather(process 0) -> sink
```

## 4.5 Scenario 2 - Concurrent Small Pipeline

- Total input: **100** `i64` values per query
- Process split: **50 + 50**
- Pipeline:

```text
source -> map(+1) -> map(*2) -> map(-1)
       -> exchange_by_hash(value) -> gather(process 0) -> sink
```

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

## 9. Result Interpretation

Key comparisons are now end-to-end **cross-process TCP** comparisons.

- **QPS ratio** (instancy / timely): total distributed throughput
- **Latency percentiles**: end-to-end cost of one 2-process query
- **Core seconds**: combined work done by both processes
- **Memory**: coordinator-side benchmark-process footprint during sustained load

Because the worker is a separate process, these numbers are intentionally closer
to real distributed execution than the earlier in-process exchange benchmarks.
