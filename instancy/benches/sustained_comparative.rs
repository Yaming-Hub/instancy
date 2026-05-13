//! Sustained comparative benchmarks: instancy vs timely-dataflow.
//!
//! Runs two workload groups for a configurable duration (default 10 minutes each):
//!
//! - **Large queries:** Compute-heavy scan/filter/aggregate, PageRank, a 10-stage map
//!   chain, a multi-epoch filter workload, plus an instancy-only 2-node TCP exchange
//!   + aggregate benchmark. All shared-memory scenarios partition input data across
//!   `--threads` workers and use exchange operators to force cross-worker transfer.
//!
//! - **High-RPS small queries:** Many tiny dataflow executions issued concurrently.
//!   Each query also runs as an N-worker exchange pipeline where N = `--threads`.
//!   instancy uses async task fan-out on a shared runtime with bounded in-flight
//!   concurrency, timely uses a fixed worker-thread pool capped by `--threads`, and
//!   instancy also includes an instancy-only 2-node TCP exchange small-pipeline run.
//!
//! Each (library, scenario) pair runs for the configured duration. System metrics
//! (working set memory, CPU time) are sampled periodically.
//!
//! # Usage
//!
//! ```text
//! cargo bench --bench sustained_comparative --release -- [OPTIONS]
//!
//! Options:
//!   --duration <SECS>      Duration per (library, scenario) pair [default: 600]
//!   --warmup  <SECS>       Warmup duration before measurement [default: 30]
//!   --rounds  <N>          Number of full rounds [default: 1]
//!   --scenario <NAME>      Run only: "large", "small", or "all" [default: all]
//!   --library <NAME>       Run only: "instancy", "timely", or "both" [default: both]
//!   --cooldown <SECS>      Pause between runs [default: 5]
//!   --concurrency <N>      In-flight query cap for small queries [default: 64]
//!   --threads <N>          Shared worker-thread budget for both libraries [default: 16]
//! ```

use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use instancy::communication::codec::{Codec, CodecError};
use instancy::communication::transport_session::PeerConnection;
use instancy::communication::{ClusterSpawnTransport, ExchangeData};
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result as InstancyResult,
    RuntimeConfig, RuntimeHandle, SpawnOptions, TokioMode,
};
use tokio::net::{TcpListener, TcpStream};

// =============================================================================
// System metrics (platform-specific)
// =============================================================================

#[derive(Clone, Debug)]
struct SystemSnapshot {
    working_set_mb: f64,
    #[allow(dead_code)]
    peak_working_set_mb: f64,
    cpu_user_ms: f64,
    cpu_kernel_ms: f64,
}

#[cfg(windows)]
fn system_snapshot() -> SystemSnapshot {
    use std::mem;

    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    unsafe extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(h: isize, pmc: *mut ProcessMemoryCounters, cb: u32) -> i32;
        fn GetProcessTimes(
            h: isize,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
    }

    unsafe {
        let h = GetCurrentProcess();

        let mut pmc: ProcessMemoryCounters = mem::zeroed();
        pmc.cb = mem::size_of::<ProcessMemoryCounters>() as u32;
        let (ws, pws) = if K32GetProcessMemoryInfo(h, &mut pmc, pmc.cb) != 0 {
            (
                pmc.working_set_size as f64 / (1024.0 * 1024.0),
                pmc.peak_working_set_size as f64 / (1024.0 * 1024.0),
            )
        } else {
            (0.0, 0.0)
        };

        let mut creation: FileTime = mem::zeroed();
        let mut exit: FileTime = mem::zeroed();
        let mut kernel: FileTime = mem::zeroed();
        let mut user: FileTime = mem::zeroed();
        let (user_ms, kernel_ms) =
            if GetProcessTimes(h, &mut creation, &mut exit, &mut kernel, &mut user) != 0 {
                let ft_to_ms =
                    |ft: &FileTime| ((ft.high as u64) << 32 | ft.low as u64) as f64 / 10_000.0;
                (ft_to_ms(&user), ft_to_ms(&kernel))
            } else {
                (0.0, 0.0)
            };

        SystemSnapshot {
            working_set_mb: ws,
            peak_working_set_mb: pws,
            cpu_user_ms: user_ms,
            cpu_kernel_ms: kernel_ms,
        }
    }
}

#[cfg(not(windows))]
fn system_snapshot() -> SystemSnapshot {
    let mut ws = 0.0;
    if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(kb_str) = line.split_whitespace().nth(1) {
                    ws = kb_str.parse::<f64>().unwrap_or(0.0) / 1024.0;
                }
            }
        }
    }
    SystemSnapshot {
        working_set_mb: ws,
        peak_working_set_mb: ws,
        cpu_user_ms: 0.0,
        cpu_kernel_ms: 0.0,
    }
}

// =============================================================================
// Data generation (deterministic pseudo-random, same as comparative.rs)
// =============================================================================

#[derive(Clone, Debug)]
struct LineItem {
    order_key: u64,
    #[allow(dead_code)]
    part_key: u64,
    quantity: i64,
    price: i64,
    discount: i64,
    #[allow(dead_code)]
    tax: i64,
    ship_date: u64,
    return_flag: u8,
    line_status: u8,
}

impl serde::Serialize for LineItem {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("LineItem", 9)?;
        st.serialize_field("order_key", &self.order_key)?;
        st.serialize_field("part_key", &self.part_key)?;
        st.serialize_field("quantity", &self.quantity)?;
        st.serialize_field("price", &self.price)?;
        st.serialize_field("discount", &self.discount)?;
        st.serialize_field("tax", &self.tax)?;
        st.serialize_field("ship_date", &self.ship_date)?;
        st.serialize_field("return_flag", &self.return_flag)?;
        st.serialize_field("line_status", &self.line_status)?;
        st.end()
    }
}

impl<'de> serde::Deserialize<'de> for LineItem {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Helper {
            order_key: u64,
            part_key: u64,
            quantity: i64,
            price: i64,
            discount: i64,
            tax: i64,
            ship_date: u64,
            return_flag: u8,
            line_status: u8,
        }
        let h = Helper::deserialize(d)?;
        Ok(LineItem {
            order_key: h.order_key,
            part_key: h.part_key,
            quantity: h.quantity,
            price: h.price,
            discount: h.discount,
            tax: h.tax,
            ship_date: h.ship_date,
            return_flag: h.return_flag,
            line_status: h.line_status,
        })
    }
}

#[derive(Clone, Default)]
struct LineItemCodec;

impl Codec<LineItem> for LineItemCodec {
    fn encode(&self, value: &LineItem, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.extend_from_slice(&value.order_key.to_le_bytes());
        buf.extend_from_slice(&value.part_key.to_le_bytes());
        buf.extend_from_slice(&value.quantity.to_le_bytes());
        buf.extend_from_slice(&value.price.to_le_bytes());
        buf.extend_from_slice(&value.discount.to_le_bytes());
        buf.extend_from_slice(&value.tax.to_le_bytes());
        buf.extend_from_slice(&value.ship_date.to_le_bytes());
        buf.push(value.return_flag);
        buf.push(value.line_status);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(LineItem, usize), CodecError> {
        const LINE_ITEM_BYTES: usize = 58;
        if buf.len() < LINE_ITEM_BYTES {
            return Err(CodecError::InsufficientData {
                needed: LINE_ITEM_BYTES,
                available: buf.len(),
            });
        }

        let mut offset = 0usize;
        let read_u64 = |bytes: &[u8], offset: &mut usize| {
            let start = *offset;
            *offset += 8;
            u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap())
        };
        let read_i64 = |bytes: &[u8], offset: &mut usize| {
            let start = *offset;
            *offset += 8;
            i64::from_le_bytes(bytes[start..start + 8].try_into().unwrap())
        };

        let item = LineItem {
            order_key: read_u64(buf, &mut offset),
            part_key: read_u64(buf, &mut offset),
            quantity: read_i64(buf, &mut offset),
            price: read_i64(buf, &mut offset),
            discount: read_i64(buf, &mut offset),
            tax: read_i64(buf, &mut offset),
            ship_date: read_u64(buf, &mut offset),
            return_flag: buf[offset],
            line_status: buf[offset + 1],
        };
        Ok((item, LINE_ITEM_BYTES))
    }
}

impl ExchangeData for LineItem {
    type CodecType = LineItemCodec;

    fn codec() -> Self::CodecType {
        LineItemCodec
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Edge {
    src: u64,
    dst: u64,
}

fn lcg_next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn generate_lineitems(count: usize) -> Vec<LineItem> {
    let mut seed: u64 = 42;
    (0..count)
        .map(|_| LineItem {
            order_key: lcg_next(&mut seed) % 1_500_000,
            part_key: lcg_next(&mut seed) % 200_000,
            quantity: (lcg_next(&mut seed) % 50 + 1) as i64,
            price: (lcg_next(&mut seed) % 100_000 + 100) as i64,
            discount: (lcg_next(&mut seed) % 11) as i64,
            tax: (lcg_next(&mut seed) % 9) as i64,
            ship_date: 10_000 + (lcg_next(&mut seed) % 2_500),
            return_flag: (lcg_next(&mut seed) % 3) as u8,
            line_status: (lcg_next(&mut seed) % 2) as u8,
        })
        .collect()
}

fn generate_graph(num_vertices: u64, num_edges: usize) -> Vec<Edge> {
    let mut seed: u64 = 123;
    (0..num_edges)
        .map(|_| Edge {
            src: lcg_next(&mut seed) % num_vertices,
            dst: lcg_next(&mut seed) % num_vertices,
        })
        .collect()
}

#[allow(dead_code)]
fn line_revenue(item: &LineItem) -> i64 {
    item.price * item.discount
}

fn make_small_batches(iterations: u64, batch_size: u64) -> Vec<(u64, Vec<u64>)> {
    (0..iterations)
        .map(|time| {
            let base = time * batch_size;
            let batch = (0..batch_size).map(|offset| base + offset).collect();
            (time, batch)
        })
        .collect()
}

fn partition_data<T: Clone>(data: &[T], num_partitions: usize) -> Vec<Vec<T>> {
    let chunk_size = (data.len() + num_partitions - 1) / num_partitions;
    data.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

fn compute_pagerank(edges: &[Edge], num_vertices: u64, iterations: usize) -> Vec<(u64, f64)> {
    let n = num_vertices as usize;
    let mut adjacency = vec![Vec::new(); n];
    for edge in edges {
        adjacency[edge.src as usize].push(edge.dst);
    }

    let base_rank = 1.0 / n as f64;
    let mut ranks = vec![base_rank; n];

    for _ in 0..iterations {
        let dangling_sum: f64 = adjacency
            .iter()
            .enumerate()
            .filter(|(_, outs)| outs.is_empty())
            .map(|(src, _)| ranks[src])
            .sum();
        let dangling_contrib = dangling_sum * 0.85 / n as f64;
        let teleport = (1.0 - 0.85) / n as f64;
        let mut next = vec![teleport + dangling_contrib; n];

        for (src, outs) in adjacency.iter().enumerate() {
            if outs.is_empty() {
                continue;
            }
            let share = ranks[src] * 0.85 / outs.len() as f64;
            for &dst in outs {
                next[dst as usize] += share;
            }
        }
        ranks = next;
    }

    ranks
        .into_iter()
        .enumerate()
        .map(|(idx, rank)| (idx as u64, rank))
        .collect()
}

// =============================================================================
// Stats collection
// =============================================================================

struct RunStats {
    name: String,
    latencies_us: Vec<u64>,
    elements_per_query: u64,
    memory_samples_mb: Vec<f64>,
    wall_duration: Duration,
    cpu_user_delta_ms: f64,
    cpu_kernel_delta_ms: f64,
    core_time_ns: u64,
}

impl RunStats {
    fn new(name: &str, elements_per_query: u64) -> Self {
        Self {
            name: name.to_string(),
            latencies_us: Vec::with_capacity(100_000),
            elements_per_query,
            memory_samples_mb: Vec::with_capacity(1_000),
            wall_duration: Duration::ZERO,
            cpu_user_delta_ms: 0.0,
            cpu_kernel_delta_ms: 0.0,
            core_time_ns: 0,
        }
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        let mut sorted = self.latencies_us.clone();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
        sorted[idx]
    }

    fn queries_completed(&self) -> usize {
        self.latencies_us.len()
    }

    fn qps(&self) -> f64 {
        self.latencies_us.len() as f64 / self.wall_duration.as_secs_f64()
    }

    fn elements_per_sec(&self) -> f64 {
        self.elements_per_query as f64 * self.qps()
    }

    fn avg_latency_us(&self) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        self.latencies_us.iter().sum::<u64>() / self.latencies_us.len() as u64
    }

    fn core_seconds(&self) -> f64 {
        self.core_time_ns as f64 / 1_000_000_000.0
    }

    fn peak_memory_mb(&self) -> f64 {
        self.memory_samples_mb
            .iter()
            .cloned()
            .fold(0.0f64, f64::max)
    }

    fn avg_memory_mb(&self) -> f64 {
        if self.memory_samples_mb.is_empty() {
            return 0.0;
        }
        self.memory_samples_mb.iter().sum::<f64>() / self.memory_samples_mb.len() as f64
    }

    fn report(&self) {
        println!("\n  === {} ===", self.name);
        println!("  Queries completed: {}", self.queries_completed());
        println!(
            "  Throughput: {:.1} queries/sec, {:.0} elements/sec",
            self.qps(),
            self.elements_per_sec()
        );
        println!(
            "  Latency (µs): min={} avg={} p50={} p95={} p99={} max={}",
            self.percentile(0.0),
            self.avg_latency_us(),
            self.percentile(50.0),
            self.percentile(95.0),
            self.percentile(99.0),
            self.percentile(100.0),
        );
        println!(
            "  Memory (MB): avg={:.1} peak={:.1}",
            self.avg_memory_mb(),
            self.peak_memory_mb()
        );
        println!(
            "  CPU time: user={:.0}ms kernel={:.0}ms total={:.0}ms",
            self.cpu_user_delta_ms,
            self.cpu_kernel_delta_ms,
            self.cpu_user_delta_ms + self.cpu_kernel_delta_ms,
        );
        println!(
            "  Core time: {:.3}s ({:.1} core-sec/query)",
            self.core_seconds(),
            self.core_seconds() / self.queries_completed().max(1) as f64,
        );
        println!("  Wall time: {:.1}s", self.wall_duration.as_secs_f64());
    }
}

/// Summary row for the final comparison table.
struct SummaryRow {
    scenario: String,
    library: String,
    queries: usize,
    qps: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    avg_mem_mb: f64,
    peak_mem_mb: f64,
    cpu_total_ms: f64,
    core_secs: f64,
}

impl SummaryRow {
    fn from_stats(scenario: &str, library: &str, stats: &RunStats) -> Self {
        Self {
            scenario: scenario.to_string(),
            library: library.to_string(),
            queries: stats.queries_completed(),
            qps: stats.qps(),
            p50_us: stats.percentile(50.0),
            p95_us: stats.percentile(95.0),
            p99_us: stats.percentile(99.0),
            max_us: stats.percentile(100.0),
            avg_mem_mb: stats.avg_memory_mb(),
            peak_mem_mb: stats.peak_memory_mb(),
            cpu_total_ms: stats.cpu_user_delta_ms + stats.cpu_kernel_delta_ms,
            core_secs: stats.core_seconds(),
        }
    }
}

impl fmt::Display for SummaryRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "  {:<22} {:<9} {:>8} {:>10.1} {:>9} {:>9} {:>9} {:>9} {:>8.1} {:>8.1} {:>12.0} {:>9.3}",
            self.scenario,
            self.library,
            self.queries,
            self.qps,
            self.p50_us,
            self.p95_us,
            self.p99_us,
            self.max_us,
            self.avg_mem_mb,
            self.peak_mem_mb,
            self.cpu_total_ms,
            self.core_secs,
        )
    }
}

fn print_summary_table(rows: &[SummaryRow]) {
    println!("\n{}", "=".repeat(141));
    println!("  SUSTAINED BENCHMARK COMPARISON SUMMARY");
    println!("{}", "=".repeat(141));
    println!(
        "  {:<22} {:<9} {:>8} {:>10} {:>9} {:>9} {:>9} {:>9} {:>8} {:>8} {:>12} {:>9}",
        "Scenario", "Library", "Queries", "QPS", "p50µs", "p95µs", "p99µs", "maxµs", "avgMB", "peakMB", "cpuTotalMs", "coreSec"
    );
    println!("  {}", "-".repeat(137));
    for row in rows {
        println!("{row}");
    }
    println!("{}", "=".repeat(141));
}

fn collect_multi_worker_metrics(
    multi: &mut instancy::runtime::MultiSpawnedDataflow<u64>,
) -> Vec<Arc<instancy::metrics::DataflowMetrics>> {
    (0..multi.num_workers())
        .filter_map(|worker_idx| multi.worker_mut(worker_idx).metrics().cloned())
        .collect()
}

fn total_metrics_core_time(metrics: &[Arc<instancy::metrics::DataflowMetrics>]) -> Duration {
    metrics.iter().map(|metrics| metrics.total_cpu_time()).sum()
}

fn sum_thread_times(thread_times: &Arc<std::sync::Mutex<Vec<Duration>>>) -> Duration {
    thread_times.lock().unwrap().iter().copied().sum()
}

// =============================================================================
// Query implementations — instancy
// =============================================================================

fn instancy_scan_filter_agg(
    rt: &RuntimeHandle,
    items: &[LineItem],
    num_workers: usize,
) -> Duration {
    let cutoff = 11_000u64;
    let partitions = partition_data(items, num_workers);

    let mut multi = rt
        .spawn_multi(
            "scan-filter-agg",
            num_workers,
            |builder| {
                builder
                    .input::<LineItem>("data")
                    .unwrap()
                    .filter("date_filter", move |_t, item| item.ship_date < cutoff)
                    .exchange_by_hash("exchange", |item: &LineItem| {
                        (item.return_flag as u64) * 256 + item.line_status as u64
                    })
                    .unary_notify::<((u8, u8), (i64, i64)), _>("aggregate", {
                        let mut groups: HashMap<(u8, u8), (i64, i64)> = HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                for item in data {
                                    let key = (item.return_flag, item.line_status);
                                    let entry = groups.entry(key).or_default();
                                    entry.0 += item.quantity;
                                    entry.1 += item.price;
                                }
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                let results: Vec<_> = groups.drain().collect();
                                if !results.is_empty() {
                                    output.push_vec(time, results);
                                }
                            }
                            Ok(())
                        }
                    })
                    .for_each("sink", |_t, v| {
                        black_box(v);
                    });
                Ok(())
            },
            SpawnOptions::default().collect_metrics(true),
        )
        .unwrap();

    let senders: Vec<_> = (0..num_workers)
        .map(|worker_idx| multi.take_input::<LineItem>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, partition) in partitions.into_iter().enumerate() {
        senders[worker_idx].send(0, partition).unwrap();
    }
    for sender in senders {
        sender.close();
    }

    let worker_metrics = collect_multi_worker_metrics(&mut multi);
    multi.join_blocking().unwrap();
    total_metrics_core_time(&worker_metrics)
}

fn instancy_pagerank(
    rt: &RuntimeHandle,
    edges: &[Edge],
    num_vertices: u64,
    iterations: usize,
    num_workers: usize,
) -> Duration {
    let partitions = partition_data(edges, num_workers);

    let mut multi = rt
        .spawn_multi(
            "pagerank",
            num_workers,
            |builder| {
                builder
                    .input::<Edge>("data")
                    .unwrap()
                    .unary_notify::<(u64, f64), _>("pagerank-local", {
                        let mut buffered = Vec::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                buffered.extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                let results = compute_pagerank(&buffered, num_vertices, iterations);
                                if !results.is_empty() {
                                    output.push_vec(time, results);
                                }
                                buffered.clear();
                            }
                            Ok(())
                        }
                    })
                    .exchange_by_hash("exchange-ranks", |item: &(u64, f64)| item.0)
                    .for_each("sink", |_t, v| {
                        black_box(v);
                    });
                Ok(())
            },
            SpawnOptions::default().collect_metrics(true),
        )
        .unwrap();

    let senders: Vec<_> = (0..num_workers)
        .map(|worker_idx| multi.take_input::<Edge>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, partition) in partitions.into_iter().enumerate() {
        senders[worker_idx].send(0, partition).unwrap();
    }
    for sender in senders {
        sender.close();
    }

    let worker_metrics = collect_multi_worker_metrics(&mut multi);
    multi.join_blocking().unwrap();
    total_metrics_core_time(&worker_metrics)
}

fn instancy_map_chain(
    rt: &RuntimeHandle,
    values: &[i64],
    stages: usize,
    num_workers: usize,
) -> Duration {
    let partitions = partition_data(values, num_workers);

    let mut multi = rt
        .spawn_multi(
            "map-chain",
            num_workers,
            |builder| {
                let mut pipe = builder.input::<i64>("data").unwrap();
                for idx in 0..stages {
                    pipe = pipe.map(format!("step_{idx}"), |_t, value| value + 1);
                }
                pipe.exchange_by_hash("exchange", |value: &i64| *value as u64)
                    .for_each("sink", |_t, v| {
                        black_box(v);
                    });
                Ok(())
            },
            SpawnOptions::default().collect_metrics(true),
        )
        .unwrap();

    let senders: Vec<_> = (0..num_workers)
        .map(|worker_idx| multi.take_input::<i64>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, partition) in partitions.into_iter().enumerate() {
        senders[worker_idx].send(0, partition).unwrap();
    }
    for sender in senders {
        sender.close();
    }

    let worker_metrics = collect_multi_worker_metrics(&mut multi);
    multi.join_blocking().unwrap();
    total_metrics_core_time(&worker_metrics)
}

async fn instancy_small_pipeline_async(
    rt: Arc<RuntimeHandle>,
    batch: Arc<Vec<i64>>,
    num_workers: usize,
) -> Duration {
    let partitions = partition_data(batch.as_ref(), num_workers);
    let mut multi = rt
        .spawn_multi(
            "small-pipeline",
            num_workers,
            |builder| {
                builder
                    .input::<i64>("data")
                    .unwrap()
                    .map("add1", |_t, x| x + 1)
                    .map("mul2", |_t, x| x * 2)
                    .map("sub1", |_t, x| x - 1)
                    .exchange_by_hash("exchange", |value: &i64| *value as u64)
                    .for_each("sink", |_t, v| {
                        black_box(v);
                    });
                Ok(())
            },
            SpawnOptions::default().collect_metrics(true),
        )
        .unwrap();

    let senders: Vec<_> = (0..num_workers)
        .map(|worker_idx| multi.take_input::<i64>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, partition) in partitions.into_iter().enumerate() {
        senders[worker_idx].send(0, partition).unwrap();
    }
    for sender in senders {
        sender.close();
    }

    let worker_metrics = collect_multi_worker_metrics(&mut multi);
    multi.join().await.unwrap();
    total_metrics_core_time(&worker_metrics)
}

fn instancy_multi_epoch(
    rt: &RuntimeHandle,
    batches: &[(u64, Vec<u64>)],
    threshold: u64,
    num_workers: usize,
) -> Duration {
    let mut multi = rt
        .spawn_multi(
            "multi-epoch-filter",
            num_workers,
            |builder| {
                builder
                    .input::<u64>("src")
                    .unwrap()
                    .filter("threshold", move |_t, value| *value > threshold)
                    .exchange_by_hash("exchange", |value: &u64| *value)
                    .for_each("sink", |_t, value| {
                        black_box(value);
                    });
                Ok(())
            },
            SpawnOptions::default().collect_metrics(true),
        )
        .unwrap();

    let senders: Vec<_> = (0..num_workers)
        .map(|worker_idx| multi.take_input::<u64>(worker_idx, "src").unwrap())
        .collect();
    for (time, batch) in batches.iter() {
        let parts = partition_data(batch, num_workers);
        for (worker_idx, part) in parts.into_iter().enumerate() {
            senders[worker_idx].send(*time, part).unwrap();
        }
    }
    for sender in senders {
        sender.close();
    }

    let worker_metrics = collect_multi_worker_metrics(&mut multi);
    multi.join_blocking().unwrap();
    total_metrics_core_time(&worker_metrics)
}

type TcpPeerConnection = PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

async fn make_tcp_connections(node_ids: &[&str]) -> HashMap<String, Vec<TcpPeerConnection>> {
    let mut result: HashMap<String, Vec<TcpPeerConnection>> = HashMap::new();
    for node_id in node_ids {
        result.insert((*node_id).to_string(), Vec::new());
    }

    for i in 0..node_ids.len() {
        for j in (i + 1)..node_ids.len() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (accepted, connected) = tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
            let stream_i = accepted.0;
            let stream_j = connected;

            stream_i.set_nodelay(true).unwrap();
            stream_j.set_nodelay(true).unwrap();

            let (ri, wi) = stream_i.into_split();
            let (rj, wj) = stream_j.into_split();

            result.get_mut(node_ids[i]).unwrap().push(PeerConnection {
                node_id: node_ids[j].to_string(),
                reader: ri,
                writer: wi,
            });
            result.get_mut(node_ids[j]).unwrap().push(PeerConnection {
                node_id: node_ids[i].to_string(),
                reader: rj,
                writer: wj,
            });
        }
    }

    result
}

/// Persistent 2-node TCP cluster for exchange benchmarks.
/// Created once, reused for many epochs. Each epoch = one "query".
/// Uses probes to wait for each epoch's completion before sending the next,
/// preventing quadratic progress-tracking accumulation.
struct ExchangeCluster {
    sender_a: instancy::InputSender<u64, (u64, i64)>,
    sender_b: instancy::InputSender<u64, (u64, i64)>,
    probe_a: instancy::ProbeHandle<u64>,
    probe_b: instancy::ProbeHandle<u64>,
    cluster_a: Option<instancy::runtime::ClusterSpawnedDataflow<u64>>,
    cluster_b: Option<instancy::runtime::ClusterSpawnedDataflow<u64>>,
    metrics_a: Vec<Arc<instancy::metrics::DataflowMetrics>>,
    metrics_b: Vec<Arc<instancy::metrics::DataflowMetrics>>,
    _rt_a: RuntimeHandle,
    _rt_b: RuntimeHandle,
    tokio_handle: tokio::runtime::Handle,
}

impl ExchangeCluster {
    fn setup(tokio_handle: &tokio::runtime::Handle) -> Self {
        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let dataflow_id = DataflowId::new();
        let mut connections = tokio_handle.block_on(make_tcp_connections(&["node-a", "node-b"]));

        let conns_a = connections.remove("node-a").unwrap();
        let conns_b = connections.remove("node-b").unwrap();

        let topo_a = topology.clone();
        let topo_b = topology;
        let df_id = dataflow_id;
        let ha = tokio_handle.clone();
        let hb = tokio_handle.clone();
        let h_ext_a = tokio_handle.clone();
        let h_ext_b = tokio_handle.clone();

        // Shared slots for capturing probe handles from build closures.
        let probe_slot_a: Arc<std::sync::Mutex<Option<instancy::ProbeHandle<u64>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let probe_slot_b: Arc<std::sync::Mutex<Option<instancy::ProbeHandle<u64>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let pa = probe_slot_a.clone();
        let pb = probe_slot_b.clone();

        let build_a = move |builder: &mut DataflowBuilder<u64>| -> InstancyResult<()> {
            let (pipe, probe) = builder
                .input::<(u64, i64)>("data")
                .unwrap()
                .exchange_by_hash("partition", |item: &(u64, i64)| item.0)
                .probe();
            pipe.for_each("sink", |_t, values| {
                black_box(values);
            });
            *pa.lock().unwrap() = Some(probe);
            Ok(())
        };
        let build_b = move |builder: &mut DataflowBuilder<u64>| -> InstancyResult<()> {
            let (pipe, probe) = builder
                .input::<(u64, i64)>("data")
                .unwrap()
                .exchange_by_hash("partition", |item: &(u64, i64)| item.0)
                .probe();
            pipe.for_each("sink", |_t, values| {
                black_box(values);
            });
            *pb.lock().unwrap() = Some(probe);
            Ok(())
        };

        let (rt_a, mut cluster_a, rt_b, mut cluster_b) = tokio_handle.block_on(async {
            let spawn_a = tokio::task::spawn_blocking(move || {
                let rt = RuntimeHandle::new(RuntimeConfig {
                    worker_threads: 1,
                    tokio_mode: TokioMode::External(h_ext_a),
                    ..RuntimeConfig::default()
                })
                .unwrap();
                let cluster = rt
                    .spawn_cluster(
                        "exchange-bench",
                        topo_a,
                        "node-a",
                        df_id,
                        ClusterSpawnTransport::dedicated(conns_a, 1024),
                        Duration::from_secs(10),
                        build_a,
                        &ha,
                        SpawnOptions::default().collect_metrics(true),
                    )
                    .unwrap();
                (rt, cluster)
            });
            let spawn_b = tokio::task::spawn_blocking(move || {
                let rt = RuntimeHandle::new(RuntimeConfig {
                    worker_threads: 1,
                    tokio_mode: TokioMode::External(h_ext_b),
                    ..RuntimeConfig::default()
                })
                .unwrap();
                let cluster = rt
                    .spawn_cluster(
                        "exchange-bench",
                        topo_b,
                        "node-b",
                        df_id,
                        ClusterSpawnTransport::dedicated(conns_b, 1024),
                        Duration::from_secs(10),
                        build_b,
                        &hb,
                        SpawnOptions::default().collect_metrics(true),
                    )
                    .unwrap();
                (rt, cluster)
            });
            let (res_a, res_b) = tokio::join!(spawn_a, spawn_b);
            let (rt_a, cl_a) = res_a.unwrap();
            let (rt_b, cl_b) = res_b.unwrap();
            (rt_a, cl_a, rt_b, cl_b)
        });

        let metrics_a = cluster_a
            .all_worker_metrics()
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        let metrics_b = cluster_b
            .all_worker_metrics()
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        let sender_a = cluster_a.take_input::<(u64, i64)>(0, "data").unwrap();
        let sender_b = cluster_b.take_input::<(u64, i64)>(0, "data").unwrap();
        let probe_a = probe_slot_a.lock().unwrap().take().unwrap();
        let probe_b = probe_slot_b.lock().unwrap().take().unwrap();

        ExchangeCluster {
            sender_a,
            sender_b,
            probe_a,
            probe_b,
            cluster_a: Some(cluster_a),
            cluster_b: Some(cluster_b),
            metrics_a,
            metrics_b,
            _rt_a: rt_a,
            _rt_b: rt_b,
            tokio_handle: tokio_handle.clone(),
        }
    }

    /// Send one epoch of data through both nodes, advance frontier,
    /// and wait for both probes to confirm the epoch is processed.
    /// Panics if probes fail or timeout (30s) to prevent silent data loss.
    fn run_epoch(&self, epoch: u64, left: &[(u64, i64)], right: &[(u64, i64)]) {
        self.sender_a.send(epoch, left.to_vec()).unwrap();
        self.sender_b.send(epoch, right.to_vec()).unwrap();
        self.sender_a.advance_to(epoch + 1).unwrap();
        self.sender_b.advance_to(epoch + 1).unwrap();
        // Wait for both nodes to finish processing this epoch with timeout.
        self.tokio_handle.block_on(async {
            let result = tokio::time::timeout(Duration::from_secs(30), async {
                let (ra, rb) = tokio::join!(
                    self.probe_a.wait_until_done_with(&epoch),
                    self.probe_b.wait_until_done_with(&epoch),
                );
                ra.expect("probe_a failed: executor dropped notifier");
                rb.expect("probe_b failed: executor dropped notifier");
            })
            .await;
            if result.is_err() {
                panic!("exchange epoch {epoch} timed out after 30s — cluster stalled");
            }
        });
    }

    fn total_core_time(&self) -> Duration {
        total_metrics_core_time(&self.metrics_a) + total_metrics_core_time(&self.metrics_b)
    }

    /// Tear down the cluster.
    fn finish(mut self, tokio_handle: &tokio::runtime::Handle) {
        drop(self.sender_a);
        drop(self.sender_b);
        let cluster_a = self.cluster_a.take().unwrap();
        let cluster_b = self.cluster_b.take().unwrap();
        tokio_handle.block_on(async {
            let ja = tokio::task::spawn_blocking(move || cluster_a.join_blocking());
            let jb = tokio::task::spawn_blocking(move || cluster_b.join_blocking());
            let timeout = tokio::time::timeout(Duration::from_secs(10), async {
                let _ = tokio::join!(ja, jb);
            });
            let _ = timeout.await; // ignore timeout — cluster already drained
        });
    }
}

/// Runs the exchange aggregate benchmark with a persistent 2-node TCP cluster.
fn run_exchange_aggregate_benchmark(
    name: &str,
    tokio_handle: &tokio::runtime::Handle,
    left_data: &[(u64, i64)],
    right_data: &[(u64, i64)],
    elements_per_epoch: u64,
    duration: Duration,
    warmup: Duration,
) -> RunStats {
    let mut stats = RunStats::new(name, elements_per_epoch);
    let memory_sample_interval = 100u64;

    println!("    Setting up 2-node TCP cluster...");
    let cluster = ExchangeCluster::setup(tokio_handle);
    println!("    Cluster ready.");

    // Warmup
    println!("    Warming up for {:.0}s...", warmup.as_secs_f64());
    let warmup_start = Instant::now();
    let mut epoch = 0u64;
    while warmup_start.elapsed() < warmup {
        cluster.run_epoch(epoch, left_data, right_data);
        epoch += 1;
    }
    println!("    Warmup done ({epoch} epochs)");

    // Measurement
    println!("    Measuring for {:.0}s...", duration.as_secs_f64());
    let cpu_before = system_snapshot();
    let core_time_before = cluster.total_core_time();
    let measure_start = Instant::now();
    let mut query_count = 0u64;

    while measure_start.elapsed() < duration {
        let q_start = Instant::now();
        cluster.run_epoch(epoch, left_data, right_data);
        epoch += 1;
        stats.latencies_us.push(q_start.elapsed().as_micros() as u64);
        query_count += 1;

        if query_count % memory_sample_interval == 0 {
            stats.memory_samples_mb.push(system_snapshot().working_set_mb);
        }
        if query_count % 1000 == 0 {
            let elapsed = measure_start.elapsed().as_secs();
            let remaining = duration.as_secs().saturating_sub(elapsed);
            print!("\r    [{query_count} queries, {remaining}s remaining]     ");
        }
    }
    println!();

    let cpu_after = system_snapshot();
    let core_time_after = cluster.total_core_time();
    stats.wall_duration = measure_start.elapsed();
    stats.cpu_user_delta_ms = cpu_after.cpu_user_ms - cpu_before.cpu_user_ms;
    stats.cpu_kernel_delta_ms = cpu_after.cpu_kernel_ms - cpu_before.cpu_kernel_ms;
    stats.core_time_ns = core_time_after
        .saturating_sub(core_time_before)
        .as_nanos() as u64;
    stats.memory_samples_mb.push(system_snapshot().working_set_mb);

    // Tear down
    cluster.finish(tokio_handle);

    stats
}

// =============================================================================
// Query implementations  timely
// =============================================================================

fn timely_scan_filter_agg(items: &[LineItem], num_workers: usize) -> Duration {
    let cutoff = 11_000u64;
    let partitions = Arc::new(partition_data(items, num_workers));
    let thread_times: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_workers)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely::Config::process(num_workers), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Filter, Input, Inspect, Probe};

        let thread_start = Instant::now();
        let worker_index = worker.index();
        let partitions = Arc::clone(&partitions);
        let tt = Arc::clone(&tt);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<LineItem>();
            let probe = stream
                .filter(move |item| item.ship_date < cutoff)
                .exchange(|item: &LineItem| {
                    (item.return_flag as u64) * 256 + item.line_status as u64
                })
                .unary_notify(Pipeline, "aggregate", None, {
                    let mut groups: HashMap<(u8, u8), (i64, i64)> = HashMap::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            for item in data.iter() {
                                let key = (item.return_flag, item.line_status);
                                let entry = groups.entry(key).or_default();
                                entry.0 += item.quantity;
                                entry.1 += item.price;
                            }
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results: Vec<_> = groups.drain().collect();
                            if !results.is_empty() {
                                output.session(&time).give_vec(&mut results);
                            }
                        });
                    }
                })
                .inspect(|v| {
                    black_box(v);
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(worker_index) {
            let mut batch = partition.clone();
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .unwrap();
    sum_thread_times(&thread_times)
}

fn timely_pagerank(
    edges: &[Edge],
    num_vertices: u64,
    iterations: usize,
    num_workers: usize,
) -> Duration {
    let partitions = Arc::new(partition_data(edges, num_workers));
    let thread_times: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_workers)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely::Config::process(num_workers), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};

        let thread_start = Instant::now();
        let worker_index = worker.index();
        let partitions = Arc::clone(&partitions);
        let tt = Arc::clone(&tt);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<Edge>();
            let probe = stream
                .unary_notify(Pipeline, "pagerank", None, {
                    let mut buffered = Vec::new();
                    move |input, output, notificator| {
                        input.for_each(|time, data| {
                            buffered.extend(data.iter().cloned());
                            notificator.notify_at(time.retain());
                        });
                        notificator.for_each(|time, _, _| {
                            let mut results = compute_pagerank(&buffered, num_vertices, iterations);
                            if !results.is_empty() {
                                output.session(&time).give_vec(&mut results);
                            }
                            buffered.clear();
                        });
                    }
                })
                .exchange(|item: &(u64, f64)| item.0)
                .inspect(|v| {
                    black_box(v);
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(worker_index) {
            let mut batch = partition.clone();
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .unwrap();
    sum_thread_times(&thread_times)
}

fn timely_map_chain(values: &[i64], stages: usize, num_workers: usize) -> Duration {
    let partitions = Arc::new(partition_data(values, num_workers));
    let thread_times: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_workers)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely::Config::process(num_workers), move |worker| {
        let thread_start = Instant::now();
        let worker_index = worker.index();
        let partitions = Arc::clone(&partitions);
        let tt = Arc::clone(&tt);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::{Exchange, Input, Inspect, Map, Probe};

            let (input, mut stream) = scope.new_input::<i64>();
            for _ in 0..stages {
                stream = stream.map(|value| value + 1);
            }
            let probe = stream
                .exchange(|value: &i64| *value as u64)
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(worker_index) {
            let mut batch = partition.clone();
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .unwrap();
    sum_thread_times(&thread_times)
}

fn timely_small_pipeline(batch: &[i64], num_workers: usize) -> Duration {
    let partitions = Arc::new(partition_data(batch, num_workers));
    let thread_times: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_workers)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely::Config::process(num_workers), move |worker| {
        use timely::dataflow::operators::{Exchange, Input, Inspect, Map, Probe};

        let thread_start = Instant::now();
        let worker_index = worker.index();
        let partitions = Arc::clone(&partitions);
        let tt = Arc::clone(&tt);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<i64>();
            let probe = stream
                .map(|x| x + 1)
                .map(|x| x * 2)
                .map(|x| x - 1)
                .exchange(|value: &i64| *value as u64)
                .inspect(|v| {
                    black_box(v);
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(worker_index) {
            let mut batch = partition.clone();
            input.send_batch(&mut batch);
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .unwrap();
    sum_thread_times(&thread_times)
}

fn timely_multi_epoch(
    batches: &[(u64, Vec<u64>)],
    threshold: u64,
    num_workers: usize,
) -> Duration {
    let batches = Arc::new(batches.to_vec());
    let thread_times: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(num_workers)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely::Config::process(num_workers), move |worker| {
        let thread_start = Instant::now();
        let worker_index = worker.index();
        let worker_count = worker.peers();
        let batches = Arc::clone(&batches);
        let tt = Arc::clone(&tt);
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            use timely::dataflow::operators::{Exchange, Filter, Input, Inspect, Probe};

            let (input, stream) = scope.new_input::<u64>();
            let probe = stream
                .filter(move |value| *value > threshold)
                .exchange(|value: &u64| *value)
                .inspect(|value| {
                    black_box(value);
                })
                .probe();
            (input, probe)
        });

        for (time, batch) in batches.iter() {
            input.advance_to(*time);
            let chunk_size = (batch.len() + worker_count - 1) / worker_count;
            let start = worker_index * chunk_size;
            let end = (start + chunk_size).min(batch.len());
            if start < batch.len() {
                let mut slice = batch[start..end].to_vec();
                input.send_batch(&mut slice);
            }
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .unwrap();
    sum_thread_times(&thread_times)
}

// We also need serde impls for timely's LineItem and Edge usage. timely requires
// Abomonation or Data trait. The comparative.rs uses serde with timely's bincode feature.
// Since timely dev-dep already has features = ["bincode"], our LineItem/Edge derive
// serde::{Serialize, Deserialize} which timely accepts via its `serde` support.

// =============================================================================
// Sustained benchmark runner
// =============================================================================

/// Runs a closure repeatedly for `duration`, collecting latency samples and
/// memory snapshots. Calls `warmup_fn` during warmup and `query_fn` during
/// measurement. Both should execute one complete query.
fn run_sustained<F>(
    name: &str,
    elements_per_query: u64,
    duration: Duration,
    warmup: Duration,
    mut query_fn: F,
) -> RunStats
where
    F: FnMut() -> Duration,
{
    let mut stats = RunStats::new(name, elements_per_query);
    let memory_sample_interval = 100;

    println!("    Warming up for {:.0}s...", warmup.as_secs_f64());
    let warmup_start = Instant::now();
    let mut warmup_count = 0u64;
    while warmup_start.elapsed() < warmup {
        let _ = query_fn();
        warmup_count += 1;
    }
    println!("    Warmup done ({warmup_count} queries)");

    println!("    Measuring for {:.0}s...", duration.as_secs_f64());
    let cpu_before = system_snapshot();
    let measure_start = Instant::now();
    let mut query_count = 0u64;

    while measure_start.elapsed() < duration {
        let q_start = Instant::now();
        let core_time = query_fn();
        stats.latencies_us.push(q_start.elapsed().as_micros() as u64);
        stats.core_time_ns = stats.core_time_ns.saturating_add(core_time.as_nanos() as u64);
        query_count += 1;

        if query_count % memory_sample_interval as u64 == 0 {
            stats.memory_samples_mb.push(system_snapshot().working_set_mb);
            if query_count % 1000 == 0 {
                let elapsed = measure_start.elapsed().as_secs();
                let remaining = duration.as_secs().saturating_sub(elapsed);
                print!("
    [{} queries, {}s remaining]     ", query_count, remaining);
            }
        }
    }
    println!();

    let cpu_after = system_snapshot();
    stats.wall_duration = measure_start.elapsed();
    stats.cpu_user_delta_ms = cpu_after.cpu_user_ms - cpu_before.cpu_user_ms;
    stats.cpu_kernel_delta_ms = cpu_after.cpu_kernel_ms - cpu_before.cpu_kernel_ms;
    stats.memory_samples_mb.push(system_snapshot().working_set_mb);
    stats
}

enum ConcurrentWorkerMessage {
    Latency(u64, u64),
    WorkerDone,
}

async fn warmup_instancy_small_concurrent(
    rt: Arc<RuntimeHandle>,
    batch: Arc<Vec<i64>>,
    concurrency: usize,
    warmup: Duration,
    num_workers: usize,
) -> u64 {
    if warmup.is_zero() {
        return 0;
    }

    let deadline = Instant::now() + warmup;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut join_set = tokio::task::JoinSet::new();
    let mut completed = 0u64;

    while Instant::now() < deadline {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("instancy small-query semaphore closed unexpectedly");
        if Instant::now() >= deadline {
            drop(permit);
            break;
        }

        let rt = rt.clone();
        let batch = batch.clone();
        join_set.spawn(async move {
            instancy_small_pipeline_async(rt, batch, num_workers).await;
            drop(permit);
        });

        while let Some(result) = join_set.try_join_next() {
            result.expect("instancy small-query task panicked");
            completed += 1;
        }
    }

    while let Some(result) = join_set.join_next().await {
        result.expect("instancy small-query task panicked");
        completed += 1;
    }

    completed
}

async fn measure_instancy_small_concurrent(
    rt: Arc<RuntimeHandle>,
    batch: Arc<Vec<i64>>,
    concurrency: usize,
    duration: Duration,
    num_workers: usize,
) -> (Vec<u64>, Vec<f64>, Duration, u64) {
    let memory_sample_interval = 100u64;
    let measure_start = Instant::now();
    let deadline = measure_start + duration;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();

    let producer = tokio::spawn({
        let rt = rt.clone();
        let batch = batch.clone();
        let semaphore = semaphore.clone();
        async move {
            let mut join_set = tokio::task::JoinSet::new();
            while Instant::now() < deadline {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("instancy small-query semaphore closed unexpectedly");
                if Instant::now() >= deadline {
                    drop(permit);
                    break;
                }

                let rt = rt.clone();
                let batch = batch.clone();
                let tx = tx.clone();
                join_set.spawn(async move {
                    let query_start = Instant::now();
                    let core_time = instancy_small_pipeline_async(rt, batch, num_workers).await;
                    let _ = tx.send((
                        query_start.elapsed().as_micros() as u64,
                        core_time.as_nanos() as u64,
                    ));
                    drop(permit);
                });

                while let Some(result) = join_set.try_join_next() {
                    result.expect("instancy small-query task panicked");
                }
            }

            drop(tx);
            while let Some(result) = join_set.join_next().await {
                result.expect("instancy small-query task panicked");
            }
        }
    });

    let mut latencies = Vec::with_capacity(concurrency * 1024);
    let mut memory_samples = Vec::new();
    let mut core_time_ns = 0u64;
    while let Some((latency, query_core_time_ns)) = rx.recv().await {
        latencies.push(latency);
        core_time_ns = core_time_ns.saturating_add(query_core_time_ns);
        let query_count = latencies.len() as u64;
        if query_count % memory_sample_interval == 0 {
            memory_samples.push(system_snapshot().working_set_mb);
            if query_count % 1000 == 0 {
                let elapsed = measure_start.elapsed().as_secs();
                let remaining = duration.as_secs().saturating_sub(elapsed);
                print!("
    [{} queries, {}s remaining]     ", query_count, remaining);
            }
        }
    }
    println!();

    producer.await.expect("instancy small-query producer task panicked");
    (latencies, memory_samples, measure_start.elapsed(), core_time_ns)
}

fn warmup_timely_small_concurrent(
    batch: Arc<Vec<i64>>,
    concurrency: usize,
    threads: usize,
    warmup: Duration,
    num_workers: usize,
) -> u64 {
    if warmup.is_zero() {
        return 0;
    }

    let worker_count = concurrency.min(threads);
    let deadline = Instant::now() + warmup;
    let (tx, rx) = std::sync::mpsc::channel::<u64>();
    let mut handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let batch = batch.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let mut completed = 0u64;
            while Instant::now() < deadline {
                timely_small_pipeline(batch.as_ref(), num_workers);
                completed += 1;
            }
            let _ = tx.send(completed);
        }));
    }
    drop(tx);

    let total_completed = rx.into_iter().sum();
    for handle in handles {
        handle.join().expect("timely warmup worker panicked");
    }
    total_completed
}

fn measure_timely_small_concurrent(
    batch: Arc<Vec<i64>>,
    concurrency: usize,
    threads: usize,
    duration: Duration,
    num_workers: usize,
) -> (Vec<u64>, Vec<f64>, Duration, u64) {
    let worker_count = concurrency.min(threads);
    let memory_sample_interval = 100u64;
    let measure_start = Instant::now();
    let deadline = measure_start + duration;
    let (tx, rx) = std::sync::mpsc::channel::<ConcurrentWorkerMessage>();
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let batch = batch.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            while Instant::now() < deadline {
                let query_start = Instant::now();
                let core_time = timely_small_pipeline(batch.as_ref(), num_workers);
                let _ = tx.send(ConcurrentWorkerMessage::Latency(
                    query_start.elapsed().as_micros() as u64,
                    core_time.as_nanos() as u64,
                ));
            }
            let _ = tx.send(ConcurrentWorkerMessage::WorkerDone);
        }));
    }
    drop(tx);

    let mut completed_workers = 0usize;
    let mut latencies = Vec::with_capacity(worker_count * 1024);
    let mut memory_samples = Vec::new();
    let mut core_time_ns = 0u64;
    while completed_workers < worker_count {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(ConcurrentWorkerMessage::Latency(latency, query_core_time_ns)) => {
                latencies.push(latency);
                core_time_ns = core_time_ns.saturating_add(query_core_time_ns);
                let query_count = latencies.len() as u64;
                if query_count % memory_sample_interval == 0 {
                    memory_samples.push(system_snapshot().working_set_mb);
                    if query_count % 1000 == 0 {
                        let elapsed = measure_start.elapsed().as_secs();
                        let remaining = duration.as_secs().saturating_sub(elapsed);
                        print!("
    [{} queries, {}s remaining]     ", query_count, remaining);
                    }
                }
            }
            Ok(ConcurrentWorkerMessage::WorkerDone) => {
                completed_workers += 1;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    while let Ok(message) = rx.try_recv() {
        if let ConcurrentWorkerMessage::Latency(latency, query_core_time_ns) = message {
            latencies.push(latency);
            core_time_ns = core_time_ns.saturating_add(query_core_time_ns);
        }
    }
    println!();

    for handle in handles {
        handle.join().expect("timely small-query worker panicked");
    }

    (latencies, memory_samples, measure_start.elapsed(), core_time_ns)
}

fn run_sustained_instancy_small_concurrent(
    name: &str,
    elements_per_query: u64,
    duration: Duration,
    warmup: Duration,
    concurrency: usize,
    num_workers: usize,
    rt_tokio: &tokio::runtime::Runtime,
    rt: Arc<RuntimeHandle>,
    batch: Arc<Vec<i64>>,
) -> RunStats {
    let mut stats = RunStats::new(name, elements_per_query);

    println!("    Warming up for {:.0}s...", warmup.as_secs_f64());
    let warmup_count = rt_tokio.block_on(warmup_instancy_small_concurrent(
        rt.clone(),
        batch.clone(),
        concurrency,
        warmup,
        num_workers,
    ));
    println!("    Warmup done ({warmup_count} queries)");

    println!("    Measuring for {:.0}s...", duration.as_secs_f64());
    let cpu_before = system_snapshot();
    let (latencies, memory_samples, wall_duration, core_time_ns) = rt_tokio.block_on(
        measure_instancy_small_concurrent(rt, batch, concurrency, duration, num_workers),
    );
    let cpu_after = system_snapshot();

    stats.latencies_us = latencies;
    stats.memory_samples_mb = memory_samples;
    stats.wall_duration = wall_duration;
    stats.cpu_user_delta_ms = cpu_after.cpu_user_ms - cpu_before.cpu_user_ms;
    stats.cpu_kernel_delta_ms = cpu_after.cpu_kernel_ms - cpu_before.cpu_kernel_ms;
    stats.core_time_ns = core_time_ns;
    stats.memory_samples_mb.push(system_snapshot().working_set_mb);
    stats
}

fn run_sustained_timely_small_concurrent(
    name: &str,
    elements_per_query: u64,
    duration: Duration,
    warmup: Duration,
    concurrency: usize,
    threads: usize,
    num_workers: usize,
    batch: Arc<Vec<i64>>,
) -> RunStats {
    let mut stats = RunStats::new(name, elements_per_query);

    println!("    Warming up for {:.0}s...", warmup.as_secs_f64());
    let warmup_count =
        warmup_timely_small_concurrent(batch.clone(), concurrency, threads, warmup, num_workers);
    println!("    Warmup done ({warmup_count} queries)");

    println!("    Measuring for {:.0}s...", duration.as_secs_f64());
    let cpu_before = system_snapshot();
    let (latencies, memory_samples, wall_duration, core_time_ns) =
        measure_timely_small_concurrent(batch, concurrency, threads, duration, num_workers);
    let cpu_after = system_snapshot();

    stats.latencies_us = latencies;
    stats.memory_samples_mb = memory_samples;
    stats.wall_duration = wall_duration;
    stats.cpu_user_delta_ms = cpu_after.cpu_user_ms - cpu_before.cpu_user_ms;
    stats.cpu_kernel_delta_ms = cpu_after.cpu_kernel_ms - cpu_before.cpu_kernel_ms;
    stats.core_time_ns = core_time_ns;
    stats.memory_samples_mb.push(system_snapshot().working_set_mb);
    stats
}

// =============================================================================
// CLI argument parsing
// =============================================================================

struct Config {
    duration_secs: u64,
    warmup_secs: u64,
    rounds: u32,
    scenario: ScenarioFilter,
    library: LibraryFilter,
    cooldown_secs: u64,
    concurrency: usize,
    threads: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum ScenarioFilter {
    All,
    Large,
    Small,
}

#[derive(Clone, Copy, PartialEq)]
enum LibraryFilter {
    Both,
    Instancy,
    Timely,
}

impl Config {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut config = Config {
            duration_secs: 600,
            warmup_secs: 30,
            rounds: 1,
            scenario: ScenarioFilter::All,
            library: LibraryFilter::Both,
            cooldown_secs: 5,
            concurrency: 64,
            threads: 16,
        };

        let mut i = 1;
        while i < args.len() {
            let flag = args[i].as_str();
            let mut next_val = |flag_name: &str| -> String {
                i += 1;
                args.get(i)
                    .cloned()
                    .unwrap_or_else(|| panic!("missing value for {flag_name}"))
            };
            match flag {
                "--duration" => {
                    config.duration_secs = next_val("--duration").parse().expect("invalid --duration");
                }
                "--warmup" => {
                    config.warmup_secs = next_val("--warmup").parse().expect("invalid --warmup");
                }
                "--rounds" => {
                    config.rounds = next_val("--rounds").parse().expect("invalid --rounds");
                }
                "--scenario" => {
                    config.scenario = match next_val("--scenario").as_str() {
                        "large" => ScenarioFilter::Large,
                        "small" => ScenarioFilter::Small,
                        "all" => ScenarioFilter::All,
                        other => panic!("unknown scenario: {other}"),
                    };
                }
                "--library" => {
                    config.library = match next_val("--library").as_str() {
                        "instancy" => LibraryFilter::Instancy,
                        "timely" => LibraryFilter::Timely,
                        "both" => LibraryFilter::Both,
                        other => panic!("unknown library: {other}"),
                    };
                }
                "--cooldown" => {
                    config.cooldown_secs = next_val("--cooldown").parse().expect("invalid --cooldown");
                }
                "--concurrency" => {
                    config.concurrency = next_val("--concurrency").parse().expect("invalid --concurrency");
                }
                "--threads" => {
                    config.threads = next_val("--threads").parse().expect("invalid --threads");
                }
                "--bench" => {}
                other => {
                    eprintln!("Unknown argument: {other}");
                }
            }
            i += 1;
        }

        assert!(config.concurrency > 0, "--concurrency must be greater than zero");
        assert!(config.threads > 0, "--threads must be greater than zero");
        config
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let config = Config::from_args();
    let duration = Duration::from_secs(config.duration_secs);
    let warmup = Duration::from_secs(config.warmup_secs);
    let cooldown = Duration::from_secs(config.cooldown_secs);

    println!("═");
    println!("          Sustained Comparative Benchmark                   ");
    println!("          instancy vs timely-dataflow                       ");
    println!("═");
    println!("  Duration:    {:>6}s per (library, scenario)             ", config.duration_secs);
    println!("  Warmup:      {:>6}s                                  ", config.warmup_secs);
    println!("  Rounds:      {:>6}                                   ", config.rounds);
    println!("  Cooldown:    {:>6}s between runs                     ", config.cooldown_secs);
    println!("  Concurrency: {:>6} small-query in-flight cap         ", config.concurrency);
    println!("  Threads:     {:>6} shared worker-thread budget       ", config.threads);
    println!("");
    println!();

    // Tokio only drives async sweep work here, so keep it lightweight.
    let rt_tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt_tokio.enter();
    let tokio_handle = tokio::runtime::Handle::current();

    println!("Generating test data...");
    let large_items = generate_lineitems(10_000_000);
    let large_item_count = large_items.len() as u64;
    println!(
        "  Scan-filter-agg: {} line items across {} workers with exchange",
        large_items.len(),
        config.threads
    );

    let graph_vertices = 50_000u64;
    let pagerank_iterations = 20usize;
    let graph_edges = generate_graph(graph_vertices, 500_000);
    let graph_edge_count = graph_edges.len() as u64;
    println!(
        "  PageRank: {} vertices, {} edges, {} iterations across {} workers with exchange",
        graph_vertices,
        graph_edges.len(),
        pagerank_iterations,
        config.threads
    );

    let map_chain_values: Vec<i64> = (0..1_000_000).collect();
    let map_chain_count = map_chain_values.len() as u64;
    println!(
        "  10-stage map chain: {} values, {} stages across {} workers with exchange",
        map_chain_values.len(),
        10,
        config.threads
    );

    let multi_epoch_batches = make_small_batches(1_024, 64);
    let multi_epoch_count: u64 = multi_epoch_batches
        .iter()
        .map(|(_, batch)| batch.len() as u64)
        .sum();
    let multi_epoch_threshold = multi_epoch_count / 2;
    println!(
        "  Multi-epoch filter: {} epochs, {} records/epoch across {} workers with exchange",
        multi_epoch_batches.len(),
        multi_epoch_batches[0].1.len(),
        config.threads
    );

    // Exchange data: per-epoch batches for the persistent 2-node cluster.
    // Each "query" sends one epoch through both nodes via TCP exchange.
    let exchange_records_per_epoch = 10_000u64;
    let exchange_left_epoch: Vec<(u64, i64)> = (0..exchange_records_per_epoch / 2)
        .map(|i| (i % 1000, (i * 7 + 3) as i64))
        .collect();
    let exchange_right_epoch: Vec<(u64, i64)> = (exchange_records_per_epoch / 2..exchange_records_per_epoch)
        .map(|i| (i % 1000, (i * 11 + 5) as i64))
        .collect();
    println!(
        "  Exchange+aggregate TCP: {} records/epoch across 2 nodes x 1 worker",
        exchange_records_per_epoch
    );

    let small_batch = Arc::new((0..100).collect::<Vec<i64>>());
    let small_batch_count = small_batch.len() as u64;
    println!(
        "  Small pipeline: {} elements/query @ concurrency {} with {} workers/query and exchange",
        small_batch.len(),
        config.concurrency,
        config.threads
    );

    println!();

    let instancy_rt = Arc::new(
        RuntimeHandle::new(RuntimeConfig {
            worker_threads: config.threads,
            ..RuntimeConfig::default()
        })
        .unwrap(),
    );

    let mut all_rows: Vec<SummaryRow> = Vec::new();

    for round in 1..=config.rounds {
        if config.rounds > 1 {
            println!(
                "
{sep}
  ROUND {round} of {total}
{sep}",
                sep = "=".repeat(60),
                total = config.rounds
            );
        }

        if config.scenario == ScenarioFilter::All || config.scenario == ScenarioFilter::Large {
            println!(
                "\n  Scenario 1A: Large Scan-Filter-Aggregate ({large_item_count} items, {} workers, exchange) ",
                config.threads
            );

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                let stats = run_sustained(
                    "instancy/scan-filter-agg",
                    large_item_count,
                    duration,
                    warmup,
                    || instancy_scan_filter_agg(instancy_rt.as_ref(), &large_items, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("ScanFilterAgg", "instancy", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
                let stats = run_sustained(
                    "timely/scan-filter-agg",
                    large_item_count,
                    duration,
                    warmup,
                    || timely_scan_filter_agg(&large_items, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("ScanFilterAgg", "timely", &stats));
                std::thread::sleep(cooldown);
            }

            println!(
                "\n  Scenario 1B: Large PageRank ({} vertices, {} edges, {} iterations, {} workers, exchange) ",
                graph_vertices,
                graph_edge_count,
                pagerank_iterations,
                config.threads
            );

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                let stats = run_sustained(
                    "instancy/pagerank",
                    graph_edge_count,
                    duration,
                    warmup,
                    || {
                        instancy_pagerank(
                            instancy_rt.as_ref(),
                            &graph_edges,
                            graph_vertices,
                            pagerank_iterations,
                            config.threads,
                        )
                    },
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("PageRank", "instancy", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
                let stats = run_sustained(
                    "timely/pagerank",
                    graph_edge_count,
                    duration,
                    warmup,
                    || timely_pagerank(&graph_edges, graph_vertices, pagerank_iterations, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("PageRank", "timely", &stats));
                std::thread::sleep(cooldown);
            }

            println!(
                "\n  Scenario 1C: Large 10-Stage Map Chain ({} values, {} workers, exchange) ",
                map_chain_count,
                config.threads
            );

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                let stats = run_sustained(
                    "instancy/map-chain-10",
                    map_chain_count,
                    duration,
                    warmup,
                    || instancy_map_chain(instancy_rt.as_ref(), &map_chain_values, 10, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("MapChain10", "instancy", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
                let stats = run_sustained(
                    "timely/map-chain-10",
                    map_chain_count,
                    duration,
                    warmup,
                    || timely_map_chain(&map_chain_values, 10, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("MapChain10", "timely", &stats));
                std::thread::sleep(cooldown);
            }

            println!(
                "\n  Scenario 1D: Multi-Epoch Filter ({} epochs, {} records/epoch, {} workers, exchange) ",
                multi_epoch_batches.len(),
                multi_epoch_batches[0].1.len(),
                config.threads
            );

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                let stats = run_sustained(
                    "instancy/multi-epoch-filter",
                    multi_epoch_count,
                    duration,
                    warmup,
                    || {
                        instancy_multi_epoch(
                            instancy_rt.as_ref(),
                            &multi_epoch_batches,
                            multi_epoch_threshold,
                            config.threads,
                        )
                    },
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("MultiEpochFilter", "instancy", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
                let stats = run_sustained(
                    "timely/multi-epoch-filter",
                    multi_epoch_count,
                    duration,
                    warmup,
                    || timely_multi_epoch(&multi_epoch_batches, multi_epoch_threshold, config.threads),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("MultiEpochFilter", "timely", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                println!(
                    "\n── Scenario 1E: Instancy-only TCP Exchange + Aggregate ({} records/epoch) ──",
                    exchange_records_per_epoch
                );

                let stats = run_exchange_aggregate_benchmark(
                    "instancy/cluster-exchange-aggregate",
                    &tokio_handle,
                    &exchange_left_epoch,
                    &exchange_right_epoch,
                    exchange_records_per_epoch,
                    duration,
                    warmup,
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("ExchangeAggregateTcp", "instancy", &stats));
                std::thread::sleep(cooldown);
            }
        }

        if config.scenario == ScenarioFilter::All || config.scenario == ScenarioFilter::Small {
            println!(
                "\n── Scenario 2: Concurrent High-RPS Small Pipeline ({} elements/query, concurrency {}, {} workers/query, exchange) ──",
                small_batch_count,
                config.concurrency,
                config.threads
            );

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
                let stats = run_sustained_instancy_small_concurrent(
                    "instancy/small-pipeline-concurrent",
                    small_batch_count,
                    duration,
                    warmup,
                    config.concurrency,
                    config.threads,
                    &rt_tokio,
                    instancy_rt.clone(),
                    small_batch.clone(),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("SmallPipelineConcurrent", "instancy", &stats));
                std::thread::sleep(cooldown);
            }

            if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
                let stats = run_sustained_timely_small_concurrent(
                    "timely/small-pipeline-concurrent",
                    small_batch_count,
                    duration,
                    warmup,
                    config.concurrency,
                    config.threads,
                    config.threads,
                    small_batch.clone(),
                );
                stats.report();
                all_rows.push(SummaryRow::from_stats("SmallPipelineConcurrent", "timely", &stats));
                std::thread::sleep(cooldown);
            }
        }
    }

    if !all_rows.is_empty() {
        print_summary_table(&all_rows);
    }

    println!("\nBenchmark complete.");
}
