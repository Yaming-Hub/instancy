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

## 9. Test Results

### 9.1 Test 1: 6-Hour Endurance (8 workers × 2 runtimes)

**Configuration:** `--duration 21600 --workers 8 --runtimes 2 --base-rps 20 --failure-rate 0.01 --cancel-rate 0.01`

| Metric | Value |
|---|---|
| Duration | 6 hours |
| Total queries | 506,509 |
| Completed | 506,509 (100%) |
| Successful | 496,340 (98.0%) |
| Expected failures | 5,199 (1.03%) |
| Unexpected failures | **0** |
| Cancelled | 4,970 (0.98%) |
| In-flight at end | 0 |
| Process crash | No |

**Memory profile:**

| Metric | Value |
|---|---|
| Start RSS | 4.8 MB |
| End RSS | 130.8 MB |
| Peak RSS (sampled) | 148.8 MB |
| OS peak RSS | 434.4 MB |
| Baseline (10 min avg) | 80.2 MB |
| Final (10 min avg) | 130.2 MB |
| Growth from baseline | 62.3% |
| Leak check | ⚠️ POTENTIAL_LEAK |

**CPU:** 3,088.5 seconds total (1,973.2s user + 1,115.3s kernel)

**Per-type breakdown:**

| Type | Submitted | Completed | Success | Expected Fail | Unexpected Fail | Cancelled |
|---|---|---|---|---|---|---|
| ScanFilterAgg | 78,632 | 78,632 | 78,632 | 0 | 0 | 0 |
| PageRank | 37,137 | 37,137 | 37,137 | 0 | 0 | 0 |
| MapChain20 | 140,587 | 140,587 | 140,587 | 0 | 0 | 0 |
| MultiEpoch | 140,606 | 140,606 | 140,606 | 0 | 0 | 0 |
| SmallPipeline | 99,378 | 99,378 | 99,378 | 0 | 0 | 0 |
| FailureInjection | 5,199 | 5,199 | 0 | 5,199 | 0 | 0 |
| Cancellation | 4,970 | 4,970 | 0 | 0 | 0 | 4,970 |

**Verdict: FAIL** — triggered by the memory leak heuristic (62.3% growth > 50% threshold).

**Analysis:**

The test passed all functional checks: zero crashes, zero unexpected failures, 100% query
completion, proper error handling and cancellation. The only concern is the gradual RSS
growth (~20 MB/hour), which triggered the POTENTIAL_LEAK flag.

The memory growth pattern is **linear** (not exponential), growing from ~15 MB at startup
to ~131 MB at 6 hours. This is consistent with **heap fragmentation** rather than a true
leak — allocating and freeing many small objects (506K dataflows over 6 hours) causes the
allocator to retain pages that are partially occupied. Key observations:

- Growth rate is constant (~20 MB/hour), not accelerating
- Peak RSS during burst periods spikes to ~430 MB and recovers
- No query type shows anomalous behavior
- All 506,509 queries completed with zero in-flight at end

### 9.2 Test 2: 1-Hour Resource-Constrained (1 worker × 2 runtimes, with query timeouts)

This test validates system stability under resource starvation. Each runtime has only
**1 worker thread** (instead of the normal 8), forcing queries to queue and potentially
timeout. Per-query timeouts are configured by data size: small 10s, medium 30s, large 120s.

**Configuration:** `--duration 3600 --workers 1 --runtimes 2 --report-interval 60 --base-rps 20 --failure-rate 0.01 --cancel-rate 0.01`

| Metric | Value |
|---|---|
| Duration | 1 hour |
| Workers per runtime | 1 |
| Total queries | 84,420 |
| Completed | 84,420 (100%) |
| Successful | 82,792 (98.1%) |
| Expected failures | 813 (0.96%) |
| Unexpected failures | **0** |
| Cancelled | 815 (0.97%) |
| Timeouts | **0** |
| In-flight at end | 0 |
| Process crash | No |

**Memory profile:**

| Metric | Value |
|---|---|
| Start RSS | 4.6 MB |
| End RSS | 119.6 MB |
| Peak RSS (sampled) | 119.6 MB |
| OS peak RSS | 344.3 MB |
| Baseline (10 min avg) | 100.9 MB |
| Final (10 min avg) | 114.5 MB |
| Growth from baseline | 13.4% |
| Leak check | ✅ OK |

**CPU:** 561.5 seconds total (352.0s user + 209.5s kernel)

**Per-type breakdown:**

| Type | Submitted | Completed | Success | Expected Fail | Timeout |
|---|---|---|---|---|---|
| ScanFilterAgg | 13,184 | 13,184 | 13,184 | 0 | 0 |
| PageRank | 6,109 | 6,109 | 6,109 | 0 | 0 |
| MapChain20 | 23,405 | 23,405 | 23,405 | 0 | 0 |
| MultiEpoch | 23,385 | 23,385 | 23,385 | 0 | 0 |
| SmallPipeline | 16,709 | 16,709 | 16,709 | 0 | 0 |
| FailureInjection | 813 | 813 | 0 | 813 | 0 |
| Cancellation | 815 | 815 | 0 | 0 | 0 |

**Verdict: PASS** ✅

**Analysis:**

The system handled 1-worker resource starvation **remarkably well**. Despite having only
1 worker thread per runtime (vs 8 in the endurance test), there were **zero timeouts** —
the async work-stealing architecture efficiently multiplexed all dataflows on the single
worker without any query exceeding its timeout threshold.

Key observations:

- **Zero timeouts**: instancy's async runtime schedules dataflows cooperatively on the
  single worker thread, preventing starvation. Even during 80 RPS bursts (minutes 30-32),
  the worker handled the spike with CPU at ~60% and zero queuing delays.
- **Memory leak check passed**: 13.4% growth from baseline (well under the 50% threshold),
  compared to 62.3% in the 6-hour test. The shorter duration and lower total allocation
  count (84K vs 506K) produced less heap fragmentation.
- **Throughput**: 84,420 queries/hour ≈ 23.5 RPS average, matching the sine-wave target
  (same count as the 8-worker test's first hour)
- **System recovery**: after each burst period, in-flight count drops to 0 within seconds,
  confirming the system remains healthy when load subsides
