# Stress Testing: instancy Endurance Test

This document describes the long-running stress test for instancy, designed to
verify crash resistance and detect resource leaks under sustained mixed load.

## 1. Objectives

1. **Crash resistance**: verify the process stays alive for 6+ hours under
   continuous mixed-query load
2. **Resource leak detection**: monitor RSS memory over time; flag if memory
   grows monotonically beyond baseline + 50%
3. **Error handling**: confirm that 1% injected operator failures are handled
   gracefully without affecting other queries
4. **Cancellation**: confirm that 1% of queries cancelled immediately after
   submission clean up properly
5. **CPU saturation**: keep worker threads busy to surface race conditions and
   scheduling issues

## 2. Test Architecture

```
┌─────────────────────────────────────────────────────┐
│                   stress_test binary                 │
│                                                      │
│  ┌──────────────┐  ┌──────────────┐                 │
│  │  Runtime A    │  │  Runtime B    │                │
│  │  8 workers    │  │  8 workers    │                │
│  └──────────────┘  └──────────────┘                 │
│         ▲                 ▲                          │
│         │    mixed queries (5 types)                 │
│  ┌──────┴─────────────────┴──────┐                  │
│  │       Query Submitter         │                  │
│  │  dynamic RPS (5-80 qps)       │                  │
│  │  1% failure injection         │                  │
│  │  1% cancellation              │                  │
│  └───────────────────────────────┘                  │
│         │                                            │
│  ┌──────┴───────────────────────┐                   │
│  │       Monitor Thread          │                  │
│  │  memory/CPU every 5 min       │                  │
│  │  leak detection at end        │                  │
│  └───────────────────────────────┘                  │
└─────────────────────────────────────────────────────┘
```

Both runtimes run in the same OS process. Queries are distributed round-robin
between them. Each query runs as a complete dataflow: build → spawn → feed
data → join.

## 3. Query Mix

### 3.1 Types (matching sustained benchmark scenarios)

| Type | Description | Pipeline |
|---|---|---|
| ScanFilterAgg | Filter + aggregate | source → filter → aggregate → sink |
| PageRank | Iterative graph algo | source → pagerank(10 iter) → sink |
| MapChain20 | Deep 20-stage pipeline | source → map(+1) × 20 → sink |
| MultiEpoch | Multi-timestamp filter | source(N epochs) → filter → sink |
| SmallPipeline | Tiny 3-stage map | source → +1 → ×2 → −1 → sink |

### 3.2 Size Distribution

| Category | Weight | Queries |
|---|---|---|
| Small (60%) | SmallPipeline(100-1K), MultiEpoch(16×256), MapChain(1K) | Fast, <1ms |
| Medium (25%) | ScanFilterAgg(100K), MapChain(10K-100K), MultiEpoch(16×4096) | 1-50ms |
| Large (15%) | ScanFilterAgg(1M), PageRank(10K v, 100K e) | 50-500ms |

### 3.3 Special Handling

- **1% failure injection**: operator returns `Err(UserDefined("injected"))` —
  the dataflow should fail gracefully, no crash
- **1% cancellation**: CancellationToken cancelled immediately after spawn —
  the dataflow should abort cleanly

## 4. Dynamic RPS

The submission rate varies over time using a sine wave with periodic bursts:

```
RPS = base + amplitude × sin(2π × t / period)

base = 20, amplitude = 15, period = 600s → range [5, 35] RPS
```

Every 30 minutes, a 2-minute burst at 80 RPS stress-tests the worker pool
under peak load.

Maximum in-flight queries are capped at 200 to prevent OOM.

## 5. Monitoring

Every 5 minutes (configurable), the test prints a progress line:

```
[01:30:00] queries=12847 completed=12832 in_flight=15 | ok=12678 fail_exp=128 fail_unexp=0 cancel=126 | rps=14.2 | mem=156.3MB peak=198.7MB | cpu=87.2%
```

### 5.1 Metrics Tracked

- Total queries submitted, completed, in-flight
- Success / expected failure / unexpected failure / cancelled counts
- Per-type query counts
- Current RPS (rolling 5-minute window)
- Working set (RSS) in MB, peak RSS
- CPU utilization (user + kernel time as % of wall time × cores)

### 5.2 Resource Leak Detection

At the end of the test:

1. Baseline memory = average RSS during minutes 10-20 (after warmup)
2. Final memory = average RSS during the last 10 minutes
3. If final > baseline × 1.5, flag as **POTENTIAL LEAK**

## 6. Running

### 6.1 Default (6 hours)

```powershell
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
$env:PROTOC = "$env:USERPROFILE\.local\protoc\bin\protoc.exe"
cargo bench --bench stress_test -- --duration 21600
```

### 6.2 Quick Smoke Test (10 minutes)

```powershell
cargo bench --bench stress_test -- --duration 600 --report-interval 60
```

### 6.3 CLI Options

| Flag | Default | Description |
|---|---|---|
| `--duration <SECS>` | 21600 | Total test duration (6 hours) |
| `--report-interval <SECS>` | 300 | Progress report interval |
| `--workers <N>` | 8 | Worker threads per runtime |
| `--runtimes <N>` | 2 | Number of runtime instances |
| `--base-rps <N>` | 20 | Base queries per second |
| `--failure-rate <F>` | 0.01 | Fraction of queries with injected failure |
| `--cancel-rate <F>` | 0.01 | Fraction of queries cancelled after submit |

## 7. Pass/Fail Criteria

| Check | Pass | Fail |
|---|---|---|
| Crash resistance | Process alive at end | Any panic or abort |
| Unexpected failures | 0 | Any query fails for a non-injected reason |
| Memory leak | Final RSS < baseline × 1.5 | RSS grows monotonically beyond 1.5× |
| Cancellation cleanup | All cancel queries handled | Hangs or resource leak from cancellation |

## 8. Interpreting Results

The final report prints a summary table and a **PASS** or **FAIL** verdict.

A **PASS** means:
- Zero unexpected failures
- No memory leak detected
- Process stayed alive for the full duration
- All cancellations completed cleanly

A **FAIL** with details indicates which checks failed and the relevant metrics.
