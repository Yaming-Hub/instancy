//! Sustained comparative benchmarks: instancy vs timely-dataflow.
//!
//! All scenarios run as a 2-process TCP exchange. The coordinator benchmark
//! process spawns a worker copy of this same binary, coordinates setup over a
//! JSON control socket, and both processes execute the same dataflow graph with
//! real TCP transport.

use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::communication::codec::{Codec, CodecError};
use instancy::communication::transport_session::PeerConnection;
use instancy::communication::{ClusterSpawnTransport, ExchangeData};
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, InputSender, NodeConfig,
    Result as InstancyResult, RuntimeConfig, RuntimeHandle, SpawnOptions, TokioMode,
};
use tokio::net::{TcpListener, TcpStream};

const SCAN_FILTER_AGG_RECORDS: u64 = 100_000_000;
const PAGERANK_VERTICES: u64 = 200_000;
const PAGERANK_EDGES: u64 = 2_000_000;
const PAGERANK_ITERATIONS: usize = 100;
const MAP_CHAIN_VALUES: u64 = 5_000_000;
const MAP_CHAIN_STAGES: usize = 20;
const MULTI_EPOCHS: u64 = 16;
const MULTI_EPOCH_BATCH_SIZE: u64 = 4_096;
const MULTI_EPOCH_THRESHOLD: u64 = (MULTI_EPOCHS * MULTI_EPOCH_BATCH_SIZE) / 2;
const SMALL_PIPELINE_VALUES: u64 = 100;
const LARGE_QUERY_CONCURRENCY: usize = 2;
const STREAM_BATCH_SIZE: usize = 100_000;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(120);

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
// Deterministic synthetic data
// =============================================================================

#[derive(Clone, Debug)]
struct LineItem {
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

#[derive(Clone, Default)]
struct EdgeCodec;

impl Codec<Edge> for EdgeCodec {
    fn encode(&self, value: &Edge, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.extend_from_slice(&value.src.to_le_bytes());
        buf.extend_from_slice(&value.dst.to_le_bytes());
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(Edge, usize), CodecError> {
        const EDGE_BYTES: usize = 16;
        if buf.len() < EDGE_BYTES {
            return Err(CodecError::InsufficientData {
                needed: EDGE_BYTES,
                available: buf.len(),
            });
        }
        let src = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let dst = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok((Edge { src, dst }, EDGE_BYTES))
    }
}

impl ExchangeData for Edge {
    type CodecType = EdgeCodec;

    fn codec() -> Self::CodecType {
        EdgeCodec
    }
}

fn partition_data<T: Clone>(data: &[T], num_partitions: usize) -> Vec<Vec<T>> {
    let mut partitions = vec![Vec::new(); num_partitions];
    for (idx, item) in data.iter().cloned().enumerate() {
        partitions[idx % num_partitions].push(item);
    }
    partitions
}

fn split_even(total: u64, parts: usize, idx: usize) -> (u64, u64) {
    assert!(parts > 0, "parts must be > 0");
    assert!(idx < parts, "index out of range");
    let parts_u64 = parts as u64;
    let idx_u64 = idx as u64;
    let base = total / parts_u64;
    let rem = total % parts_u64;
    let start = idx_u64 * base + rem.min(idx_u64);
    let len = base + u64::from(idx_u64 < rem);
    (start, start + len)
}

fn process_range(total: u64, process: usize) -> (u64, u64) {
    split_even(total, 2, process)
}

fn worker_range(total: u64, process: usize, workers: usize, worker_idx: usize) -> (u64, u64) {
    let (proc_start, proc_end) = process_range(total, process);
    let (local_start, local_end) = split_even(proc_end - proc_start, workers, worker_idx);
    (proc_start + local_start, proc_start + local_end)
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

fn lineitem_at(index: u64) -> LineItem {
    let a = mix64(index ^ 0x1234_5678_9abc_def0);
    let b = mix64(a ^ 0x0fed_cba9_8765_4321);
    let c = mix64(b ^ 0x55aa_aa55_f00d_beef);
    let d = mix64(c ^ 0xdead_beef_cafe_babe);
    let e = mix64(d ^ 0x1122_3344_5566_7788);
    let f = mix64(e ^ 0x8877_6655_4433_2211);

    LineItem {
        order_key: a % 1_500_000,
        part_key: b % 200_000,
        quantity: (c % 50 + 1) as i64,
        price: (d % 100_000 + 100) as i64,
        discount: (e % 11) as i64,
        tax: (f % 9) as i64,
        ship_date: 10_000 + (mix64(f) % 2_500),
        return_flag: (mix64(a ^ c) % 3) as u8,
        line_status: (mix64(b ^ d) % 2) as u8,
    }
}

fn edge_at(index: u64, num_vertices: u64) -> Edge {
    let a = mix64(index ^ 0x0000_0000_0000_007b);
    let b = mix64(a ^ 0x9e37_79b9_7f4a_7c15);
    Edge {
        src: a % num_vertices,
        dst: b % num_vertices,
    }
}

fn make_local_pagerank_edges(process: usize) -> Vec<Edge> {
    let (start, end) = process_range(PAGERANK_EDGES, process);
    (start..end)
        .map(|index| edge_at(index, PAGERANK_VERTICES))
        .collect()
}

fn make_local_multi_epoch_batches(process: usize) -> Vec<(u64, Vec<u64>)> {
    let (start, end) = process_range(MULTI_EPOCHS, process);
    (start..end)
        .map(|time| {
            let base = time * MULTI_EPOCH_BATCH_SIZE;
            let batch = (0..MULTI_EPOCH_BATCH_SIZE)
                .map(|offset| base + offset)
                .collect();
            (time, batch)
        })
        .collect()
}

fn make_local_small_batch(process: usize) -> Vec<i64> {
    let (start, end) = process_range(SMALL_PIPELINE_VALUES, process);
    (start..end).map(|value| value as i64).collect()
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
        if self.wall_duration.is_zero() {
            return 0.0;
        }
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
            "  Throughput: {:.2} queries/sec, {:.0} elements/sec",
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
            "  Core time: {:.3}s ({:.3} core-sec/query)",
            self.core_seconds(),
            self.core_seconds() / self.queries_completed().max(1) as f64,
        );
        println!("  Wall time: {:.1}s", self.wall_duration.as_secs_f64());
    }
}

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
            "  {:<22} {:<9} {:>8} {:>10.2} {:>9} {:>9} {:>9} {:>9} {:>8.1} {:>8.1} {:>12.0} {:>9.3}",
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

fn total_metrics_core_time(metrics: &[Arc<instancy::metrics::DataflowMetrics>]) -> Duration {
    metrics.iter().map(|metrics| metrics.total_core_time()).sum()
}

fn sum_thread_times(thread_times: &Arc<Mutex<Vec<Duration>>>) -> Duration {
    thread_times.lock().unwrap().iter().copied().sum()
}

// =============================================================================
// Control protocol
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Role {
    Coordinator,
    Worker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum BenchLibrary {
    Instancy,
    Timely,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ScenarioKind {
    ScanFilterAgg,
    PageRank,
    MapChain10,
    MultiEpochFilter,
    SmallPipeline,
}

impl ScenarioKind {
    fn summary_name(self) -> &'static str {
        match self {
            Self::ScanFilterAgg => "ScanFilterAgg",
            Self::PageRank => "PageRank",
            Self::MapChain10 => "MapChain10",
            Self::MultiEpochFilter => "MultiEpochFilter",
            Self::SmallPipeline => "SmallPipelineConcurrent",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::ScanFilterAgg => "Large Scan-Filter-Aggregate",
            Self::PageRank => "Large PageRank",
            Self::MapChain10 => "Large 20-Stage Map Chain",
            Self::MultiEpochFilter => "Multi-Epoch Filter",
            Self::SmallPipeline => "Concurrent High-RPS Small Pipeline",
        }
    }

    fn elements_per_query(self) -> u64 {
        match self {
            Self::ScanFilterAgg => SCAN_FILTER_AGG_RECORDS,
            Self::PageRank => PAGERANK_EDGES,
            Self::MapChain10 => MAP_CHAIN_VALUES,
            Self::MultiEpochFilter => MULTI_EPOCHS * MULTI_EPOCH_BATCH_SIZE,
            Self::SmallPipeline => SMALL_PIPELINE_VALUES,
        }
    }

    fn default_concurrency(self, small_concurrency: usize) -> usize {
        if self == Self::SmallPipeline {
            small_concurrency
        } else {
            LARGE_QUERY_CONCURRENCY
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
enum ControlCommand {
    Setup {
        library: BenchLibrary,
        scenario: ScenarioKind,
        threads: usize,
        exchange_port: u16,
        dataflow_id: [u8; 16],
    },
    Run,
    Shutdown,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
enum ControlStatus {
    Ready { exchange_port: u16 },
    Done { core_time_ns: u64, wall_ms: u64 },
    Error { message: String },
}

#[derive(Clone, Debug)]
struct PendingSetup {
    library: BenchLibrary,
    scenario: ScenarioKind,
    threads: usize,
    coordinator_exchange_port: u16,
    worker_exchange_port: u16,
    dataflow_id: [u8; 16],
}

struct JsonControlChannel {
    reader: BufReader<StdTcpStream>,
    writer: BufWriter<StdTcpStream>,
}

impl JsonControlChannel {
    fn new(stream: StdTcpStream) -> Self {
        let reader = BufReader::new(stream.try_clone().expect("failed to clone control stream"));
        let writer = BufWriter::new(stream);
        Self { reader, writer }
    }

    fn send<T: serde::Serialize>(&mut self, message: &T) {
        serde_json::to_writer(&mut self.writer, message).expect("failed to write control JSON");
        self.writer
            .write_all(b"\n")
            .expect("failed to terminate control JSON line");
        self.writer.flush().expect("failed to flush control channel");
    }

    fn recv<T: serde::de::DeserializeOwned>(&mut self) -> T {
        let mut line = String::new();
        let bytes = self
            .reader
            .read_line(&mut line)
            .expect("failed to read control JSON line");
        assert!(bytes > 0, "control socket closed unexpectedly");
        serde_json::from_str(line.trim_end()).expect("failed to parse control JSON")
    }
}

fn reserve_free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .expect("failed to reserve free TCP port")
        .local_addr()
        .expect("reserved socket missing address")
        .port()
}

fn spawn_worker_process(control_addr: SocketAddr) -> Child {
    let exe = std::env::current_exe().expect("failed to resolve benchmark executable");
    Command::new(exe)
        .arg("--role")
        .arg("worker")
        .arg("--control-addr")
        .arg(control_addr.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .stdin(Stdio::null())
        .spawn()
        .expect("failed to spawn worker benchmark process")
}

fn connect_control_socket(control_addr: &str) -> StdTcpStream {
    let deadline = Instant::now() + CONTROL_TIMEOUT;
    loop {
        match StdTcpStream::connect(control_addr) {
            Ok(stream) => {
                stream
                    .set_nodelay(true)
                    .expect("failed to enable TCP_NODELAY on control socket");
                return stream;
            }
            Err(err) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
                let _ = err;
            }
            Err(err) => panic!("failed to connect to control socket {control_addr}: {err}"),
        }
    }
}

fn run_cross_process_query(library: BenchLibrary, scenario: ScenarioKind, threads: usize) -> u64 {
    let control_listener = StdTcpListener::bind("127.0.0.1:0")
        .expect("failed to bind coordinator control listener");
    let control_addr = control_listener
        .local_addr()
        .expect("control listener missing address");
    let mut child = spawn_worker_process(control_addr);
    let (control_stream, _) = control_listener.accept().expect("worker failed to connect");
    control_stream
        .set_nodelay(true)
        .expect("failed to enable TCP_NODELAY on control stream");
    let mut channel = JsonControlChannel::new(control_stream);

    let instancy_listener = if library == BenchLibrary::Instancy {
        Some(
            StdTcpListener::bind("127.0.0.1:0")
                .expect("failed to bind instancy exchange listener"),
        )
    } else {
        None
    };
    let coordinator_exchange_port = instancy_listener
        .as_ref()
        .map(|listener| listener.local_addr().unwrap().port())
        .unwrap_or_else(reserve_free_port);

    let dataflow_id = DataflowId::new();
    channel.send(&ControlCommand::Setup {
        library,
        scenario,
        threads,
        exchange_port: coordinator_exchange_port,
        dataflow_id: *dataflow_id.as_bytes(),
    });

    let worker_exchange_port = match channel.recv::<ControlStatus>() {
        ControlStatus::Ready { exchange_port } => exchange_port,
        ControlStatus::Error { message } => panic!("worker setup failed: {message}"),
        other => panic!("unexpected worker setup response: {other:?}"),
    };

    let setup = PendingSetup {
        library,
        scenario,
        threads,
        coordinator_exchange_port,
        worker_exchange_port,
        dataflow_id: *dataflow_id.as_bytes(),
    };

    channel.send(&ControlCommand::Run);
    let local_core_time_ns = execute_process_half(0, &setup, instancy_listener);
    let remote_core_time_ns = match channel.recv::<ControlStatus>() {
        ControlStatus::Done { core_time_ns, .. } => core_time_ns,
        ControlStatus::Error { message } => panic!("worker execution failed: {message}"),
        other => panic!("unexpected worker execution response: {other:?}"),
    };
    channel.send(&ControlCommand::Shutdown);
    drop(channel);

    let status = child.wait().expect("failed to wait for worker exit");
    assert!(status.success(), "worker exited unsuccessfully: {status}");

    local_core_time_ns.saturating_add(remote_core_time_ns)
}

fn run_worker(control_addr: &str) {
    let stream = connect_control_socket(control_addr);
    let mut channel = JsonControlChannel::new(stream);
    let mut pending: Option<PendingSetup> = None;

    loop {
        match channel.recv::<ControlCommand>() {
            ControlCommand::Setup {
                library,
                scenario,
                threads,
                exchange_port,
                dataflow_id,
            } => {
                let worker_exchange_port = if library == BenchLibrary::Timely {
                    reserve_free_port()
                } else {
                    0
                };
                pending = Some(PendingSetup {
                    library,
                    scenario,
                    threads,
                    coordinator_exchange_port: exchange_port,
                    worker_exchange_port,
                    dataflow_id,
                });
                channel.send(&ControlStatus::Ready {
                    exchange_port: worker_exchange_port,
                });
            }
            ControlCommand::Run => {
                let setup = pending
                    .clone()
                    .expect("worker received run before setup");
                let wall_start = Instant::now();
                let core_time_ns = execute_process_half(1, &setup, None);
                channel.send(&ControlStatus::Done {
                    core_time_ns,
                    wall_ms: wall_start.elapsed().as_millis() as u64,
                });
            }
            ControlCommand::Shutdown => break,
        }
    }
}

// =============================================================================
// Sustained benchmark runner
// =============================================================================

struct PhaseResult {
    completed: u64,
    latencies_us: Vec<u64>,
    memory_samples_mb: Vec<f64>,
    wall_duration: Duration,
    core_time_ns: u64,
}

fn execute_phase<F>(
    label: &str,
    duration: Duration,
    concurrency: usize,
    collect_stats: bool,
    query_fn: Arc<F>,
) -> PhaseResult
where
    F: Fn() -> u64 + Send + Sync + 'static,
{
    if duration.is_zero() {
        return PhaseResult {
            completed: 0,
            latencies_us: Vec::new(),
            memory_samples_mb: Vec::new(),
            wall_duration: Duration::ZERO,
            core_time_ns: 0,
        };
    }

    let memory_sample_interval = if concurrency <= LARGE_QUERY_CONCURRENCY { 1 } else { 100 };
    let (tx, rx) = std::sync::mpsc::channel::<(u64, u64)>();
    let start = Instant::now();
    let deadline = start + duration;
    let mut completed = 0u64;
    let mut active = 0usize;
    let mut core_time_ns = 0u64;
    let mut latencies_us = Vec::with_capacity(concurrency.saturating_mul(64));
    let mut memory_samples_mb = Vec::new();
    let mut last_progress = Instant::now();

    loop {
        while active < concurrency && Instant::now() < deadline {
            let tx = tx.clone();
            let query_fn = Arc::clone(&query_fn);
            active += 1;
            std::thread::spawn(move || {
                let query_start = Instant::now();
                let core_time_ns = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    query_fn()
                }))
                .unwrap_or_else(|_| {
                    eprintln!("query thread panicked");
                    std::process::exit(1);
                });
                let latency_us = query_start.elapsed().as_micros() as u64;
                let _ = tx.send((latency_us, core_time_ns));
            });
        }

        if active == 0 && Instant::now() >= deadline {
            break;
        }

        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok((latency_us, query_core_time_ns)) => {
                active = active.saturating_sub(1);
                completed += 1;
                if collect_stats {
                    latencies_us.push(latency_us);
                    core_time_ns = core_time_ns.saturating_add(query_core_time_ns);
                    if completed % memory_sample_interval == 0 {
                        memory_samples_mb.push(system_snapshot().working_set_mb);
                    }
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if last_progress.elapsed() >= Duration::from_secs(60) {
            let elapsed = start.elapsed().as_secs();
            let remaining = duration.as_secs().saturating_sub(elapsed);
            println!(
                "    {label}: completed {completed}, active {active}, remaining {remaining}s"
            );
            last_progress = Instant::now();
        }
    }

    PhaseResult {
        completed,
        latencies_us,
        memory_samples_mb,
        wall_duration: start.elapsed(),
        core_time_ns,
    }
}

fn run_sustained<F>(
    name: &str,
    elements_per_query: u64,
    duration: Duration,
    warmup: Duration,
    concurrency: usize,
    query_fn: F,
) -> RunStats
where
    F: Fn() -> u64 + Send + Sync + 'static,
{
    let mut stats = RunStats::new(name, elements_per_query);
    let query_fn = Arc::new(query_fn);

    println!(
        "    Warming up for {:.0}s with concurrency {}...",
        warmup.as_secs_f64(),
        concurrency
    );
    let warmup_result = execute_phase("warmup", warmup, concurrency, false, Arc::clone(&query_fn));
    println!("    Warmup done ({} queries)", warmup_result.completed);

    println!(
        "    Measuring for {:.0}s with concurrency {}...",
        duration.as_secs_f64(),
        concurrency
    );
    let cpu_before = system_snapshot();
    let measure_result = execute_phase("measure", duration, concurrency, true, query_fn);
    let cpu_after = system_snapshot();

    stats.latencies_us = measure_result.latencies_us;
    stats.memory_samples_mb = measure_result.memory_samples_mb;
    stats.wall_duration = measure_result.wall_duration;
    stats.cpu_user_delta_ms = cpu_after.cpu_user_ms - cpu_before.cpu_user_ms;
    stats.cpu_kernel_delta_ms = cpu_after.cpu_kernel_ms - cpu_before.cpu_kernel_ms;
    stats.core_time_ns = measure_result.core_time_ns;
    stats.memory_samples_mb.push(system_snapshot().working_set_mb);
    stats
}

// =============================================================================
// instancy cross-process TCP execution
// =============================================================================

type TcpPeerConnection =
    PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

struct InstancyQueryContext {
    _tokio_runtime: tokio::runtime::Runtime,
    _runtime_handle: RuntimeHandle,
    cluster: instancy::runtime::ClusterSpawnedDataflow<u64>,
    metrics: Vec<Arc<instancy::metrics::DataflowMetrics>>,
}

fn instancy_topology(threads: usize) -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", threads),
        NodeConfig::new("node-b", threads),
    ])
    .expect("failed to build 2-node topology")
}

async fn connect_instancy_stream(addr: String) -> TcpStream {
    let deadline = Instant::now() + CONTROL_TIMEOUT;
    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => return stream,
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("failed to connect instancy exchange stream {addr}: {err}"),
        }
    }
}

async fn make_instancy_connections(
    process: usize,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> Vec<TcpPeerConnection> {
    let peer_node_id = if process == 0 { "node-b" } else { "node-a" };
    let stream = if process == 0 {
        let listener = coordinator_listener.expect("coordinator instancy listener missing");
        listener
            .set_nonblocking(true)
            .expect("failed to switch listener to nonblocking mode");
        let listener = TcpListener::from_std(listener).expect("failed to adopt listener into tokio");
        let (stream, _) = listener.accept().await.expect("instancy accept failed");
        stream
    } else {
        connect_instancy_stream(format!("127.0.0.1:{coordinator_port}")).await
    };

    stream
        .set_nodelay(true)
        .expect("failed to enable TCP_NODELAY on exchange stream");
    let (reader, writer) = stream.into_split();
    vec![PeerConnection {
        node_id: peer_node_id.to_string(),
        reader,
        writer,
    }]
}

fn start_instancy_cluster<F>(
    name: &str,
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
    build: F,
) -> InstancyQueryContext
where
    F: Fn(&mut DataflowBuilder<u64>) -> InstancyResult<()> + Send + 'static,
{
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for instancy query");
    let tokio_handle = tokio_runtime.handle().clone();
    // Enter the tokio runtime context so spawn_cluster (and its internal async
    // operations) can find the reactor.
    let _tokio_guard = tokio_runtime.enter();
    let connections = tokio_runtime.block_on(make_instancy_connections(
        process,
        coordinator_port,
        coordinator_listener,
    ));

    let runtime_handle = RuntimeHandle::new(RuntimeConfig {
        worker_threads: threads,
        tokio_mode: TokioMode::External(tokio_handle.clone()),
        ..RuntimeConfig::default()
    })
    .expect("failed to create instancy runtime handle");

    let local_node_id = if process == 0 { "node-a" } else { "node-b" };
    let cluster = runtime_handle
        .spawn_cluster(
            name,
            instancy_topology(threads),
            local_node_id,
            dataflow_id,
            ClusterSpawnTransport::dedicated(connections, 1024),
            CONTROL_TIMEOUT,
            build,
            &tokio_handle,
            SpawnOptions::default().collect_metrics(true),
        )
        .expect("failed to spawn instancy cluster dataflow");

    let metrics = cluster
        .all_worker_metrics()
        .into_iter()
        .flatten()
        .cloned()
        .collect();

    InstancyQueryContext {
        _tokio_runtime: tokio_runtime,
        _runtime_handle: runtime_handle,
        cluster,
        metrics,
    }
}

fn send_generated_batches<D, F>(
    sender: InputSender<u64, D>,
    start: u64,
    end: u64,
    batch_size: usize,
    mut generator: F,
) where
    D: Clone + Send + 'static,
    F: FnMut(u64) -> D,
{
    let mut cursor = start;
    while cursor < end {
        let upper = (cursor + batch_size as u64).min(end);
        let mut batch = Vec::with_capacity((upper - cursor) as usize);
        for index in cursor..upper {
            batch.push(generator(index));
        }
        if !batch.is_empty() {
            sender.send(0, batch).expect("failed to send input batch");
        }
        cursor = upper;
    }
    sender.close();
}

fn instancy_scan_filter_agg(
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> u64 {
    let mut ctx = start_instancy_cluster(
        "scan-filter-agg",
        process,
        threads,
        dataflow_id,
        coordinator_port,
        coordinator_listener,
        |builder| {
            builder
                .input::<LineItem>("data")
                .unwrap()
                .filter("date_filter", |_t, item| item.ship_date < 11_000)
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
                .exchange_by_hash("gather", |_item: &((u8, u8), (i64, i64))| 0u64)
                .for_each("sink", |_t, batch| {
                    black_box(batch);
                });
            Ok(())
        },
    );

    let senders: Vec<_> = (0..threads)
        .map(|worker_idx| ctx.cluster.take_input::<LineItem>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, sender) in senders.into_iter().enumerate() {
        let (start, end) = worker_range(SCAN_FILTER_AGG_RECORDS, process, threads, worker_idx);
        send_generated_batches(sender, start, end, STREAM_BATCH_SIZE, lineitem_at);
    }

    ctx.cluster.join_blocking().expect("instancy scan/filter/agg join failed");
    total_metrics_core_time(&ctx.metrics).as_nanos() as u64
}

fn instancy_pagerank(
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> u64 {
    let local_edges = make_local_pagerank_edges(process);
    let edge_partitions = partition_data(&local_edges, threads);
    let mut ctx = start_instancy_cluster(
        "pagerank",
        process,
        threads,
        dataflow_id,
        coordinator_port,
        coordinator_listener,
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
                            let results = compute_pagerank(
                                &buffered,
                                PAGERANK_VERTICES,
                                PAGERANK_ITERATIONS,
                            );
                            if !results.is_empty() {
                                output.push_vec(time, results);
                            }
                            buffered.clear();
                        }
                        Ok(())
                    }
                })
                .exchange_by_hash("exchange-ranks", |item: &(u64, f64)| item.0)
                .exchange_by_hash("gather", |_item: &(u64, f64)| 0u64)
                .for_each("sink", |_t, batch| {
                    black_box(batch);
                });
            Ok(())
        },
    );

    let senders: Vec<_> = (0..threads)
        .map(|worker_idx| ctx.cluster.take_input::<Edge>(worker_idx, "data").unwrap())
        .collect();
    for (sender, partition) in senders.into_iter().zip(edge_partitions.into_iter()) {
        if !partition.is_empty() {
            sender.send(0, partition).unwrap();
        }
        sender.close();
    }

    ctx.cluster.join_blocking().expect("instancy pagerank join failed");
    total_metrics_core_time(&ctx.metrics).as_nanos() as u64
}

fn instancy_map_chain(
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> u64 {
    let mut ctx = start_instancy_cluster(
        "map-chain-10",
        process,
        threads,
        dataflow_id,
        coordinator_port,
        coordinator_listener,
        |builder| {
            let mut pipe = builder.input::<i64>("data").unwrap();
            for idx in 0..MAP_CHAIN_STAGES {
                pipe = pipe.map(format!("step_{idx}"), |_t, value| value + 1);
            }
            pipe.exchange_by_hash("exchange", |value: &i64| *value as u64)
                .exchange_by_hash("gather", |_value: &i64| 0u64)
                .for_each("sink", |_t, batch| {
                    black_box(batch);
                });
            Ok(())
        },
    );

    let senders: Vec<_> = (0..threads)
        .map(|worker_idx| ctx.cluster.take_input::<i64>(worker_idx, "data").unwrap())
        .collect();
    for (worker_idx, sender) in senders.into_iter().enumerate() {
        let (start, end) = worker_range(MAP_CHAIN_VALUES, process, threads, worker_idx);
        send_generated_batches(sender, start, end, STREAM_BATCH_SIZE, |index| index as i64);
    }

    ctx.cluster.join_blocking().expect("instancy map chain join failed");
    total_metrics_core_time(&ctx.metrics).as_nanos() as u64
}

fn instancy_multi_epoch(
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> u64 {
    let local_batches = make_local_multi_epoch_batches(process);
    let mut ctx = start_instancy_cluster(
        "multi-epoch-filter",
        process,
        threads,
        dataflow_id,
        coordinator_port,
        coordinator_listener,
        |builder| {
            builder
                .input::<u64>("src")
                .unwrap()
                .filter("threshold", |_t, value| *value > MULTI_EPOCH_THRESHOLD)
                .exchange_by_hash("exchange", |value: &u64| *value)
                .exchange_by_hash("gather", |_value: &u64| 0u64)
                .for_each("sink", |_t, batch| {
                    black_box(batch);
                });
            Ok(())
        },
    );

    let senders: Vec<_> = (0..threads)
        .map(|worker_idx| ctx.cluster.take_input::<u64>(worker_idx, "src").unwrap())
        .collect();
    for (time, batch) in &local_batches {
        let partitions = partition_data(batch, threads);
        for (sender, partition) in senders.iter().zip(partitions.into_iter()) {
            if !partition.is_empty() {
                sender.send(*time, partition).unwrap();
            }
        }
    }
    for sender in senders {
        sender.close();
    }

    ctx.cluster
        .join_blocking()
        .expect("instancy multi-epoch join failed");
    total_metrics_core_time(&ctx.metrics).as_nanos() as u64
}

fn instancy_small_pipeline(
    process: usize,
    threads: usize,
    dataflow_id: DataflowId,
    coordinator_port: u16,
    coordinator_listener: Option<StdTcpListener>,
) -> u64 {
    let local_batch = make_local_small_batch(process);
    let partitions = partition_data(&local_batch, threads);
    let mut ctx = start_instancy_cluster(
        "small-pipeline",
        process,
        threads,
        dataflow_id,
        coordinator_port,
        coordinator_listener,
        |builder| {
            builder
                .input::<i64>("data")
                .unwrap()
                .map("add1", |_t, value| value + 1)
                .map("mul2", |_t, value| value * 2)
                .map("sub1", |_t, value| value - 1)
                .exchange_by_hash("exchange", |value: &i64| *value as u64)
                .exchange_by_hash("gather", |_value: &i64| 0u64)
                .for_each("sink", |_t, batch| {
                    black_box(batch);
                });
            Ok(())
        },
    );

    let senders: Vec<_> = (0..threads)
        .map(|worker_idx| ctx.cluster.take_input::<i64>(worker_idx, "data").unwrap())
        .collect();
    for (sender, partition) in senders.into_iter().zip(partitions.into_iter()) {
        if !partition.is_empty() {
            sender.send(0, partition).unwrap();
        }
        sender.close();
    }

    ctx.cluster
        .join_blocking()
        .expect("instancy small pipeline join failed");
    total_metrics_core_time(&ctx.metrics).as_nanos() as u64
}

// =============================================================================
// timely cross-process TCP execution
// =============================================================================

fn timely_cluster_config(threads: usize, process: usize, port0: u16, port1: u16) -> timely::Config {
    timely::Config {
        communication: timely::CommunicationConfig::Cluster {
            threads,
            process,
            addresses: vec![format!("127.0.0.1:{port0}"), format!("127.0.0.1:{port1}")],
            report: false,
            log_fn: Box::new(|_| None),
        },
        worker: timely::WorkerConfig::default(),
    }
}

fn timely_local_index(global_index: usize, process: usize, threads: usize) -> usize {
    global_index.saturating_sub(process * threads)
}

fn timely_scan_filter_agg(process: usize, threads: usize, port0: u16, port1: u16) -> u64 {
    let thread_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(threads)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely_cluster_config(threads, process, port0, port1), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Filter, Input, Inspect, Probe};

        let thread_start = Instant::now();
        let global_index = worker.index();
        let local_index = timely_local_index(global_index, process, threads);
        let is_gather_root = global_index == 0;
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<LineItem>();
            let probe = stream
                .filter(|item| item.ship_date < 11_000)
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
                .exchange(|_item: &((u8, u8), (i64, i64))| 0u64)
                .inspect(move |batch| {
                    if is_gather_root {
                        black_box(batch);
                    }
                })
                .probe();
            (input, probe)
        });

        let (start, end) = worker_range(SCAN_FILTER_AGG_RECORDS, process, threads, local_index);
        let mut cursor = start;
        while cursor < end {
            let upper = (cursor + STREAM_BATCH_SIZE as u64).min(end);
            let mut batch = Vec::with_capacity((upper - cursor) as usize);
            for index in cursor..upper {
                batch.push(lineitem_at(index));
            }
            if !batch.is_empty() {
                input.send_batch(&mut batch);
            }
            cursor = upper;
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .expect("timely scan/filter/agg execution failed");
    sum_thread_times(&thread_times).as_nanos() as u64
}

fn timely_pagerank(process: usize, threads: usize, port0: u16, port1: u16) -> u64 {
    let local_edges = make_local_pagerank_edges(process);
    let partitions = Arc::new(partition_data(&local_edges, threads));
    let thread_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(threads)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely_cluster_config(threads, process, port0, port1), move |worker| {
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::generic::Operator;
        use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};

        let thread_start = Instant::now();
        let global_index = worker.index();
        let local_index = timely_local_index(global_index, process, threads);
        let partitions = Arc::clone(&partitions);
        let is_gather_root = global_index == 0;
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
                            let mut results =
                                compute_pagerank(&buffered, PAGERANK_VERTICES, PAGERANK_ITERATIONS);
                            if !results.is_empty() {
                                output.session(&time).give_vec(&mut results);
                            }
                            buffered.clear();
                        });
                    }
                })
                .exchange(|item: &(u64, f64)| item.0)
                .exchange(|_item: &(u64, f64)| 0u64)
                .inspect(move |batch| {
                    if is_gather_root {
                        black_box(batch);
                    }
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(local_index) {
            let mut batch = partition.clone();
            if !batch.is_empty() {
                input.send_batch(&mut batch);
            }
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .expect("timely pagerank execution failed");
    sum_thread_times(&thread_times).as_nanos() as u64
}

fn timely_map_chain(process: usize, threads: usize, port0: u16, port1: u16) -> u64 {
    let thread_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(threads)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely_cluster_config(threads, process, port0, port1), move |worker| {
        use timely::dataflow::operators::{Exchange, Input, Inspect, Map, Probe};

        let thread_start = Instant::now();
        let global_index = worker.index();
        let local_index = timely_local_index(global_index, process, threads);
        let is_gather_root = global_index == 0;
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, mut stream) = scope.new_input::<i64>();
            for _ in 0..MAP_CHAIN_STAGES {
                stream = stream.map(|value| value + 1);
            }
            let probe = stream
                .exchange(|value: &i64| *value as u64)
                .exchange(|_value: &i64| 0u64)
                .inspect(move |batch| {
                    if is_gather_root {
                        black_box(batch);
                    }
                })
                .probe();
            (input, probe)
        });

        let (start, end) = worker_range(MAP_CHAIN_VALUES, process, threads, local_index);
        let mut cursor = start;
        while cursor < end {
            let upper = (cursor + STREAM_BATCH_SIZE as u64).min(end);
            let mut batch: Vec<i64> = (cursor..upper).map(|value| value as i64).collect();
            if !batch.is_empty() {
                input.send_batch(&mut batch);
            }
            cursor = upper;
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .expect("timely map-chain execution failed");
    sum_thread_times(&thread_times).as_nanos() as u64
}

fn timely_multi_epoch(process: usize, threads: usize, port0: u16, port1: u16) -> u64 {
    let batches = Arc::new(make_local_multi_epoch_batches(process));
    let thread_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(threads)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely_cluster_config(threads, process, port0, port1), move |worker| {
        use timely::dataflow::operators::{Exchange, Filter, Input, Inspect, Probe};

        let thread_start = Instant::now();
        let global_index = worker.index();
        let local_index = timely_local_index(global_index, process, threads);
        let batches = Arc::clone(&batches);
        let is_gather_root = global_index == 0;
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<u64>();
            let probe = stream
                .filter(|value| *value > MULTI_EPOCH_THRESHOLD)
                .exchange(|value: &u64| *value)
                .exchange(|_value: &u64| 0u64)
                .inspect(move |batch| {
                    if is_gather_root {
                        black_box(batch);
                    }
                })
                .probe();
            (input, probe)
        });

        for (time, batch) in batches.iter() {
            input.advance_to(*time);
            let partitions = partition_data(batch, threads);
            let mut local_batch = partitions[local_index].clone();
            if !local_batch.is_empty() {
                input.send_batch(&mut local_batch);
            }
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .expect("timely multi-epoch execution failed");
    sum_thread_times(&thread_times).as_nanos() as u64
}

fn timely_small_pipeline(process: usize, threads: usize, port0: u16, port1: u16) -> u64 {
    let local_batch = make_local_small_batch(process);
    let partitions = Arc::new(partition_data(&local_batch, threads));
    let thread_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(threads)));
    let tt = Arc::clone(&thread_times);
    timely::execute(timely_cluster_config(threads, process, port0, port1), move |worker| {
        use timely::dataflow::operators::{Exchange, Input, Inspect, Map, Probe};

        let thread_start = Instant::now();
        let global_index = worker.index();
        let local_index = timely_local_index(global_index, process, threads);
        let partitions = Arc::clone(&partitions);
        let is_gather_root = global_index == 0;
        let (mut input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (input, stream) = scope.new_input::<i64>();
            let probe = stream
                .map(|value| value + 1)
                .map(|value| value * 2)
                .map(|value| value - 1)
                .exchange(|value: &i64| *value as u64)
                .exchange(|_value: &i64| 0u64)
                .inspect(move |batch| {
                    if is_gather_root {
                        black_box(batch);
                    }
                })
                .probe();
            (input, probe)
        });

        if let Some(partition) = partitions.get(local_index) {
            let mut batch = partition.clone();
            if !batch.is_empty() {
                input.send_batch(&mut batch);
            }
        }
        input.close();
        worker.step_while(|| !probe.done());
        tt.lock().unwrap().push(thread_start.elapsed());
    })
    .expect("timely small pipeline execution failed");
    sum_thread_times(&thread_times).as_nanos() as u64
}

// =============================================================================
// Scenario dispatch
// =============================================================================

fn execute_process_half(
    process: usize,
    setup: &PendingSetup,
    instancy_listener: Option<StdTcpListener>,
) -> u64 {
    let dataflow_id = DataflowId::from_bytes(setup.dataflow_id);
    match setup.library {
        BenchLibrary::Instancy => match setup.scenario {
            ScenarioKind::ScanFilterAgg => instancy_scan_filter_agg(
                process,
                setup.threads,
                dataflow_id,
                setup.coordinator_exchange_port,
                instancy_listener,
            ),
            ScenarioKind::PageRank => instancy_pagerank(
                process,
                setup.threads,
                dataflow_id,
                setup.coordinator_exchange_port,
                instancy_listener,
            ),
            ScenarioKind::MapChain10 => instancy_map_chain(
                process,
                setup.threads,
                dataflow_id,
                setup.coordinator_exchange_port,
                instancy_listener,
            ),
            ScenarioKind::MultiEpochFilter => instancy_multi_epoch(
                process,
                setup.threads,
                dataflow_id,
                setup.coordinator_exchange_port,
                instancy_listener,
            ),
            ScenarioKind::SmallPipeline => instancy_small_pipeline(
                process,
                setup.threads,
                dataflow_id,
                setup.coordinator_exchange_port,
                instancy_listener,
            ),
        },
        BenchLibrary::Timely => match setup.scenario {
            ScenarioKind::ScanFilterAgg => timely_scan_filter_agg(
                process,
                setup.threads,
                setup.coordinator_exchange_port,
                setup.worker_exchange_port,
            ),
            ScenarioKind::PageRank => timely_pagerank(
                process,
                setup.threads,
                setup.coordinator_exchange_port,
                setup.worker_exchange_port,
            ),
            ScenarioKind::MapChain10 => timely_map_chain(
                process,
                setup.threads,
                setup.coordinator_exchange_port,
                setup.worker_exchange_port,
            ),
            ScenarioKind::MultiEpochFilter => timely_multi_epoch(
                process,
                setup.threads,
                setup.coordinator_exchange_port,
                setup.worker_exchange_port,
            ),
            ScenarioKind::SmallPipeline => timely_small_pipeline(
                process,
                setup.threads,
                setup.coordinator_exchange_port,
                setup.worker_exchange_port,
            ),
        },
    }
}

// =============================================================================
// CLI argument parsing
// =============================================================================

struct Config {
    role: Role,
    control_addr: Option<String>,
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
            role: Role::Coordinator,
            control_addr: None,
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
                "--role" => {
                    config.role = match next_val("--role").as_str() {
                        "coordinator" => Role::Coordinator,
                        "worker" => Role::Worker,
                        other => panic!("unknown role: {other}"),
                    };
                }
                "--control-addr" => {
                    config.control_addr = Some(next_val("--control-addr"));
                }
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
        if config.role == Role::Worker {
            assert!(
                config.control_addr.is_some(),
                "--control-addr is required in worker mode"
            );
        }
        config
    }
}

// =============================================================================
// Main
// =============================================================================

fn run_scenario(
    all_rows: &mut Vec<SummaryRow>,
    config: &Config,
    duration: Duration,
    scenario: ScenarioKind,
    cooldown: Duration,
) {
    let concurrency = scenario.default_concurrency(config.concurrency);
    let warmup = Duration::from_secs(config.warmup_secs);
    let threads = config.threads;

    if config.library == LibraryFilter::Both || config.library == LibraryFilter::Instancy {
        let stats = run_sustained(
            &format!("instancy/{}", scenario.summary_name()),
            scenario.elements_per_query(),
            duration,
            warmup,
            concurrency,
            move || run_cross_process_query(BenchLibrary::Instancy, scenario, threads),
        );
        stats.report();
        all_rows.push(SummaryRow::from_stats(
            scenario.summary_name(),
            "instancy",
            &stats,
        ));
        std::thread::sleep(cooldown);
    }

    if config.library == LibraryFilter::Both || config.library == LibraryFilter::Timely {
        let stats = run_sustained(
            &format!("timely/{}", scenario.summary_name()),
            scenario.elements_per_query(),
            duration,
            warmup,
            concurrency,
            move || run_cross_process_query(BenchLibrary::Timely, scenario, threads),
        );
        stats.report();
        all_rows.push(SummaryRow::from_stats(
            scenario.summary_name(),
            "timely",
            &stats,
        ));
        std::thread::sleep(cooldown);
    }
}

fn main() {
    let config = Config::from_args();
    if config.role == Role::Worker {
        run_worker(config.control_addr.as_deref().unwrap());
        return;
    }

    let duration = Duration::from_secs(config.duration_secs);
    let warmup = Duration::from_secs(config.warmup_secs);
    let cooldown = Duration::from_secs(config.cooldown_secs);

    println!("");
    println!("          Sustained Comparative Benchmark                   ");
    println!("          instancy vs timely-dataflow                       ");
    println!("");
    println!("  Mode:        2-process cross-TCP exchange                ");
    println!("  Duration:    {:>6}s per (library, scenario)             ", config.duration_secs);
    println!("  Warmup:      {:>6}s                                     ", config.warmup_secs);
    println!("  Rounds:      {:>6}                                      ", config.rounds);
    println!("  Cooldown:    {:>6}s between runs                        ", config.cooldown_secs);
    println!("  Concurrency: {:>6} small-query in-flight cap            ", config.concurrency);
    println!("  Threads:     {:>6} per-process worker threads           ", config.threads);
    println!();
    println!("  ScanFilterAgg: {} records total ({} + {})", SCAN_FILTER_AGG_RECORDS, SCAN_FILTER_AGG_RECORDS / 2, SCAN_FILTER_AGG_RECORDS / 2);
    println!("  PageRank:     {} vertices, {} edges, {} iterations", PAGERANK_VERTICES, PAGERANK_EDGES, PAGERANK_ITERATIONS);
    println!("  MapChain10:   {} values total", MAP_CHAIN_VALUES);
    println!("  MultiEpoch:   {} epochs x {} records", MULTI_EPOCHS, MULTI_EPOCH_BATCH_SIZE);
    println!("  SmallPipeline:{} values/query @ concurrency {}", SMALL_PIPELINE_VALUES, config.concurrency);
    println!("  Large-query sustained parallelism: {}", LARGE_QUERY_CONCURRENCY);
    println!();

    let mut all_rows: Vec<SummaryRow> = Vec::new();

    for round in 1..=config.rounds {
        if config.rounds > 1 {
            println!(
                "\n{sep}\n  ROUND {round} of {total}\n{sep}",
                sep = "=".repeat(60),
                total = config.rounds
            );
        }

        if config.scenario == ScenarioFilter::All || config.scenario == ScenarioFilter::Large {
            println!(
                "\n  Scenario 1A: {} ({} records, concurrency {}, warmup {}s, duration {}s)",
                ScenarioKind::ScanFilterAgg.display_name(),
                SCAN_FILTER_AGG_RECORDS,
                LARGE_QUERY_CONCURRENCY,
                warmup.as_secs(),
                duration.as_secs()
            );
            run_scenario(
                &mut all_rows,
                &config,
                duration,
                ScenarioKind::ScanFilterAgg,
                cooldown,
            );

            println!(
                "\n  Scenario 1B: {} ({} vertices, {} edges, {} iterations, concurrency {})",
                ScenarioKind::PageRank.display_name(),
                PAGERANK_VERTICES,
                PAGERANK_EDGES,
                PAGERANK_ITERATIONS,
                LARGE_QUERY_CONCURRENCY
            );
            run_scenario(
                &mut all_rows,
                &config,
                duration,
                ScenarioKind::PageRank,
                cooldown,
            );

            println!(
                "\n  Scenario 1C: {} ({} values, concurrency {})",
                ScenarioKind::MapChain10.display_name(),
                MAP_CHAIN_VALUES,
                LARGE_QUERY_CONCURRENCY
            );
            run_scenario(
                &mut all_rows,
                &config,
                duration,
                ScenarioKind::MapChain10,
                cooldown,
            );

            println!(
                "\n  Scenario 1D: {} ({} epochs x {} records, concurrency {})",
                ScenarioKind::MultiEpochFilter.display_name(),
                MULTI_EPOCHS,
                MULTI_EPOCH_BATCH_SIZE,
                LARGE_QUERY_CONCURRENCY
            );
            run_scenario(
                &mut all_rows,
                &config,
                duration,
                ScenarioKind::MultiEpochFilter,
                cooldown,
            );
        }

        if config.scenario == ScenarioFilter::All || config.scenario == ScenarioFilter::Small {
            println!(
                "\n  Scenario 2: {} ({} values/query, concurrency {})",
                ScenarioKind::SmallPipeline.display_name(),
                SMALL_PIPELINE_VALUES,
                config.concurrency
            );
            run_scenario(
                &mut all_rows,
                &config,
                duration,
                ScenarioKind::SmallPipeline,
                cooldown,
            );
        }
    }

    if !all_rows.is_empty() {
        print_summary_table(&all_rows);
    }

    println!("\nBenchmark complete.");
}
