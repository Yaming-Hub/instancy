//! Comprehensive stress test for instancy.
//!
//! Runs a mixed workload of dataflow queries at varying load levels for a configurable
//! duration (default 50 minutes). Verifies correctness, profiles latency/throughput,
//! tracks memory usage, and identifies bottlenecks.
//!
//! ## Usage
//!
//! ```bash
//! # Full 50-minute run
//! cargo run -p instancy --example stress_test --release --all-features
//!
//! # Quick 60-second smoke test
//! cargo run -p instancy --example stress_test --release --all-features -- --duration 60
//!
//! # Save results to JSON for comparison
//! cargo run -p instancy --example stress_test --release --all-features -- --output results.json
//!
//! # Compare two runs
//! # Run before fix:  --output before.json
//! # Run after fix:   --output after.json
//! # Then diff/compare the JSON files
//! ```
//!
//! ## Adding New Query Types
//!
//! To add a new query type (e.g., `WindowAggregate`):
//!
//! 1. Add a variant to `QueryKind` enum and update `ALL`, `index()`, `name()`
//! 2. Add expected data fields to `QueryContext` and compute them in `build_query_context()`
//! 3. Add a `run_qN()` function that builds/runs the dataflow and verifies the result
//! 4. Add the match arm in `run_query()`
//! 5. Update `choose_query()` weights (percentages must sum to 100)
//! 6. Update `SharedState::new()` array size and `QUERY_KIND_COUNT`
//! 7. Add serialization fields to `QueryReport` in `build_json_report()`

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::metrics::OperatorMetrics;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const QUERY_KIND_COUNT: usize = 5;
const DEFAULT_DURATION_SECS: u64 = 50 * 60;
const DEFAULT_WORKER_THREADS: usize = 8;
const DEFAULT_MAX_CONCURRENT: usize = 50;
const STATUS_INTERVAL_SECS: u64 = 30;
const MEMORY_SAMPLE_INTERVAL_SECS: u64 = 5;
const JOIN_WORKERS: usize = 2;
const PAGERANK_ITERATIONS: usize = 10;
const PAGERANK_TOP_K: usize = 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Edge {
    src: u64,
    dst: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryKind {
    ScanFilterAggregate,
    MapChain,
    SmallFilter,
    MultiWorkerJoin,
    PageRankBatch,
}

impl QueryKind {
    const ALL: [Self; 5] = [
        Self::ScanFilterAggregate,
        Self::MapChain,
        Self::SmallFilter,
        Self::MultiWorkerJoin,
        Self::PageRankBatch,
    ];

    fn index(self) -> usize {
        match self {
            Self::ScanFilterAggregate => 0,
            Self::MapChain => 1,
            Self::SmallFilter => 2,
            Self::MultiWorkerJoin => 3,
            Self::PageRankBatch => 4,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ScanFilterAggregate => "ScanFilterAggregate",
            Self::MapChain => "MapChain",
            Self::SmallFilter => "SmallFilter",
            Self::MultiWorkerJoin => "MultiWorkerJoin",
            Self::PageRankBatch => "PageRankBatch",
        }
    }
}

struct PhaseSpec {
    name: &'static str,
    base_secs: f64,
    start_qps: f64,
    end_qps: f64,
}

impl PhaseSpec {
    fn scaled_duration(&self, total_duration: Duration) -> Duration {
        let scale = total_duration.as_secs_f64() / DEFAULT_DURATION_SECS as f64;
        Duration::from_secs_f64(self.base_secs * scale)
    }

    fn rate_at(&self, progress: f64) -> f64 {
        self.start_qps + (self.end_qps - self.start_qps) * progress.clamp(0.0, 1.0)
    }
}

const PHASES: [PhaseSpec; 8] = [
    PhaseSpec {
        name: "Warm-up",
        base_secs: 2.0 * 60.0,
        start_qps: 1.0,
        end_qps: 1.0,
    },
    PhaseSpec {
        name: "Ramp-up",
        base_secs: 5.0 * 60.0,
        start_qps: 2.0,
        end_qps: 20.0,
    },
    PhaseSpec {
        name: "Sustained",
        base_secs: 15.0 * 60.0,
        start_qps: 20.0,
        end_qps: 20.0,
    },
    PhaseSpec {
        name: "Spike",
        base_secs: 5.0 * 60.0,
        start_qps: 40.0,
        end_qps: 40.0,
    },
    PhaseSpec {
        name: "Sustained",
        base_secs: 10.0 * 60.0,
        start_qps: 20.0,
        end_qps: 20.0,
    },
    PhaseSpec {
        name: "Ramp-down",
        base_secs: 5.0 * 60.0,
        start_qps: 20.0,
        end_qps: 1.0,
    },
    PhaseSpec {
        name: "Cool-down",
        base_secs: 3.0 * 60.0,
        start_qps: 1.0,
        end_qps: 1.0,
    },
    PhaseSpec {
        name: "Idle",
        base_secs: 5.0 * 60.0,
        start_qps: 0.0,
        end_qps: 0.0,
    },
];

struct StressTestConfig {
    total_duration: Duration,
    worker_threads: usize,
    max_concurrent: usize,
    output_path: Option<String>,
}

struct QueryStats {
    query_type: &'static str,
    count: AtomicU64,
    total_latency_us: AtomicU64,
    max_latency_us: AtomicU64,
    errors: AtomicU64,
    correctness_failures: AtomicU64,
    latencies: Mutex<Vec<u64>>,
    operator_cpu_us: Mutex<HashMap<String, u64>>,
    operator_activations: Mutex<HashMap<String, u64>>,
    operator_records: Mutex<HashMap<String, u64>>,
}

impl QueryStats {
    fn new(query_type: &'static str) -> Self {
        Self {
            query_type,
            count: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            max_latency_us: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            correctness_failures: AtomicU64::new(0),
            latencies: Mutex::new(Vec::new()),
            operator_cpu_us: Mutex::new(HashMap::new()),
            operator_activations: Mutex::new(HashMap::new()),
            operator_records: Mutex::new(HashMap::new()),
        }
    }

    fn record_latency(&self, latency_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        loop {
            let current = self.max_latency_us.load(Ordering::Relaxed);
            if latency_us <= current {
                break;
            }
            if self
                .max_latency_us
                .compare_exchange(current, latency_us, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        self.latencies.lock().unwrap().push(latency_us);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_correctness_failure(&self) {
        self.correctness_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_metrics(&self, metrics: &[OperatorMetrics]) {
        let mut cpu = self.operator_cpu_us.lock().unwrap();
        let mut activations = self.operator_activations.lock().unwrap();
        let mut records = self.operator_records.lock().unwrap();
        for metric in metrics {
            *cpu.entry(metric.name.clone()).or_default() += metric.cpu_time.as_micros() as u64;
            *activations.entry(metric.name.clone()).or_default() += metric.activations;
            *records.entry(metric.name.clone()).or_default() += metric.records_processed;
        }
    }

    fn snapshot(&self) -> QueryStatsSnapshot {
        QueryStatsSnapshot {
            query_type: self.query_type,
            count: self.count.load(Ordering::Relaxed),
            total_latency_us: self.total_latency_us.load(Ordering::Relaxed),
            max_latency_us: self.max_latency_us.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            correctness_failures: self.correctness_failures.load(Ordering::Relaxed),
            latencies: self.latencies.lock().unwrap().clone(),
            operator_cpu_us: self.operator_cpu_us.lock().unwrap().clone(),
            operator_activations: self.operator_activations.lock().unwrap().clone(),
            operator_records: self.operator_records.lock().unwrap().clone(),
        }
    }
}

struct QueryStatsSnapshot {
    query_type: &'static str,
    count: u64,
    total_latency_us: u64,
    max_latency_us: u64,
    errors: u64,
    correctness_failures: u64,
    latencies: Vec<u64>,
    operator_cpu_us: HashMap<String, u64>,
    operator_activations: HashMap<String, u64>,
    operator_records: HashMap<String, u64>,
}

struct MemorySample {
    timestamp_secs: f64,
    working_set_bytes: u64,
    phase: &'static str,
}

struct ThroughputSample {
    timestamp_secs: f64,
    queries_per_sec: f64,
}

struct SharedState {
    started_at: Instant,
    current_phase_idx: AtomicUsize,
    total_queries: AtomicU64,
    total_errors: AtomicU64,
    total_correctness_failures: AtomicU64,
    stats: [Arc<QueryStats>; QUERY_KIND_COUNT],
    memory_samples: Mutex<Vec<MemorySample>>,
    throughput_samples: Mutex<Vec<ThroughputSample>>,
}

impl SharedState {
    fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            current_phase_idx: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_correctness_failures: AtomicU64::new(0),
            stats: std::array::from_fn(|idx| Arc::new(QueryStats::new(QueryKind::ALL[idx].name()))),
            memory_samples: Mutex::new(Vec::new()),
            throughput_samples: Mutex::new(Vec::new()),
        }
    }

    fn stats_for(&self, kind: QueryKind) -> Arc<QueryStats> {
        self.stats[kind.index()].clone()
    }

    fn current_phase(&self) -> &'static str {
        PHASES[self.current_phase_idx.load(Ordering::Relaxed)].name
    }

    fn snapshot_stats(&self) -> Vec<QueryStatsSnapshot> {
        self.stats.iter().map(|stats| stats.snapshot()).collect()
    }
}

struct QueryContext {
    q1_items: Arc<Vec<LineItem>>,
    q1_cutoff: u64,
    q1_expected: HashMap<(u8, u8), (i64, i64)>,
    q2_values: Arc<Vec<i64>>,
    q2_expected: Arc<Vec<i64>>,
    q3_values: Arc<Vec<u64>>,
    q3_threshold: u64,
    q3_expected: Arc<Vec<u64>>,
    q4_left_partitions: Arc<Vec<Vec<(u64, i64)>>>,
    q4_right_partitions: Arc<Vec<Vec<u64>>>,
    q4_expected_total_revenue: i64,
    q5_edges: Arc<Vec<Edge>>,
    q5_vertices: u64,
    q5_expected_top10: Vec<(u64, f64)>,
}

struct QueryExecution {
    metrics: Vec<OperatorMetrics>,
    verification_error: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args()?;
    let started_at = Instant::now();
    let shared = Arc::new(SharedState::new(started_at));
    let context = Arc::new(build_query_context());
    let runtime = Arc::new(RuntimeHandle::new(RuntimeConfig {
        worker_threads: config.worker_threads,
        name: "stress-test".to_string(),
        ..Default::default()
    })?);
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
    let stop_background = Arc::new(AtomicBool::new(false));

    println!("=== instancy stress_test ===");
    println!(
        "duration={} | runtime_workers={} | max_concurrent={}",
        format_compact_duration(config.total_duration),
        config.worker_threads,
        config.max_concurrent
    );
    println!("query mix: q1=15% q2=25% q3=35% q4=15% q5=10%\n");

    record_memory_sample(&shared);

    let reporter = tokio::spawn(status_reporter(shared.clone(), stop_background.clone()));
    let sampler = tokio::spawn(memory_sampler(shared.clone(), stop_background.clone()));

    let mut join_set = JoinSet::new();
    schedule_queries(
        config.total_duration,
        shared.clone(),
        context.clone(),
        runtime.clone(),
        semaphore.clone(),
        &mut join_set,
    )
    .await?;

    while let Some(result) = join_set.join_next().await {
        result.map_err(|err| format!("query task join error: {err}"))?;
    }

    record_memory_sample(&shared);
    stop_background.store(true, Ordering::Relaxed);
    reporter
        .await
        .map_err(|err| format!("reporter task failed: {err}"))?;
    sampler
        .await
        .map_err(|err| format!("sampler task failed: {err}"))?;

    print_final_report(config.total_duration, &shared);

    if let Some(path) = &config.output_path {
        let report = build_json_report(config.total_duration, &config, &shared);
        let json = serde_json::to_string_pretty(&report)
            .map_err(|err| format!("failed to serialize report: {err}"))?;
        std::fs::write(path, &json)
            .map_err(|err| format!("failed to write report to {path}: {err}"))?;
        println!("\nReport saved to: {path}");
    }

    Ok(())
}

fn parse_args() -> Result<StressTestConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut duration_secs = DEFAULT_DURATION_SECS;
    let mut output_path = None;
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        if arg == "--duration" {
            let value = args.next().ok_or("missing value for --duration")?;
            duration_secs = value.parse::<u64>()?;
        } else if let Some(value) = arg.strip_prefix("--duration=") {
            duration_secs = value.parse::<u64>()?;
        } else if arg == "--output" {
            output_path = Some(args.next().ok_or("missing value for --output")?.to_string());
        } else if let Some(value) = arg.strip_prefix("--output=") {
            output_path = Some(value.to_string());
        } else {
            return Err(format!("unknown argument: {arg}").into());
        }
    }

    if duration_secs == 0 {
        return Err("--duration must be greater than zero".into());
    }

    Ok(StressTestConfig {
        total_duration: Duration::from_secs(duration_secs),
        worker_threads: DEFAULT_WORKER_THREADS,
        max_concurrent: DEFAULT_MAX_CONCURRENT,
        output_path,
    })
}

async fn schedule_queries(
    total_duration: Duration,
    shared: Arc<SharedState>,
    context: Arc<QueryContext>,
    runtime: Arc<RuntimeHandle>,
    semaphore: Arc<Semaphore>,
    join_set: &mut JoinSet<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut seed = 0x5eed_1234_5678_9abcu64;

    for (phase_idx, phase) in PHASES.iter().enumerate() {
        shared.current_phase_idx.store(phase_idx, Ordering::Relaxed);
        record_memory_sample(&shared);

        let phase_duration = phase.scaled_duration(total_duration);
        if phase_duration.is_zero() {
            continue;
        }

        let phase_started_at = Instant::now();
        let phase_ends_at = phase_started_at + phase_duration;

        if phase.start_qps == 0.0 && phase.end_qps == 0.0 {
            while Instant::now() < phase_ends_at {
                let remaining = phase_ends_at.saturating_duration_since(Instant::now());
                tokio::time::sleep(remaining.min(Duration::from_secs(1))).await;
            }
            continue;
        }

        let mut next_fire = phase_started_at;
        loop {
            let now = Instant::now();
            if now >= phase_ends_at {
                break;
            }

            let progress = if phase_duration.as_secs_f64() == 0.0 {
                1.0
            } else {
                now.duration_since(phase_started_at).as_secs_f64() / phase_duration.as_secs_f64()
            };
            let rate = phase.rate_at(progress).max(0.1);
            next_fire += Duration::from_secs_f64(1.0 / rate);

            // Backpressure: wait for a permit before spawning to avoid unbounded task queue
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed unexpectedly");

            // Check time again after potentially waiting for permit
            if Instant::now() >= phase_ends_at {
                drop(permit);
                break;
            }

            spawn_query_task(
                choose_query(&mut seed),
                shared.clone(),
                context.clone(),
                runtime.clone(),
                permit,
                join_set,
            );

            let sleep_for = next_fire.saturating_duration_since(Instant::now());
            if !sleep_for.is_zero() {
                tokio::time::sleep(sleep_for).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    Ok(())
}

fn spawn_query_task(
    query_kind: QueryKind,
    shared: Arc<SharedState>,
    context: Arc<QueryContext>,
    runtime: Arc<RuntimeHandle>,
    permit: tokio::sync::OwnedSemaphorePermit,
    join_set: &mut JoinSet<()>,
) {
    join_set.spawn(async move {
        let stats = shared.stats_for(query_kind);
        let started = Instant::now();
        let execution = tokio::task::spawn_blocking(move || {
            run_query(runtime.as_ref(), context.as_ref(), query_kind)
        })
        .await;
        let latency_us = started.elapsed().as_micros() as u64;

        shared.total_queries.fetch_add(1, Ordering::Relaxed);
        stats.record_latency(latency_us);

        match execution {
            Ok(Ok(result)) => {
                stats.record_metrics(&result.metrics);
                if let Some(message) = result.verification_error {
                    eprintln!("correctness failure for {}: {}", query_kind.name(), message);
                    shared
                        .total_correctness_failures
                        .fetch_add(1, Ordering::Relaxed);
                    stats.record_correctness_failure();
                }
            }
            Ok(Err(error)) => {
                eprintln!("query {} failed: {}", query_kind.name(), error);
                shared.total_errors.fetch_add(1, Ordering::Relaxed);
                stats.record_error();
            }
            Err(join_error) => {
                eprintln!("query {} panicked: {}", query_kind.name(), join_error);
                shared.total_errors.fetch_add(1, Ordering::Relaxed);
                stats.record_error();
            }
        }

        drop(permit);
    });
}

fn run_query(
    runtime: &RuntimeHandle,
    context: &QueryContext,
    query_kind: QueryKind,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    match query_kind {
        QueryKind::ScanFilterAggregate => run_q1(runtime, context),
        QueryKind::MapChain => run_q2(runtime, context),
        QueryKind::SmallFilter => run_q3(runtime, context),
        QueryKind::MultiWorkerJoin => run_q4(runtime, context),
        QueryKind::PageRankBatch => run_q5(runtime, context),
    }
}

fn run_q1(
    runtime: &RuntimeHandle,
    context: &QueryContext,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    let builder = DataflowBuilder::<u64>::new("stress-q1");
    builder
        .source("src", vec![(0u64, context.q1_items.as_ref().clone())])
        .filter("ship_date", {
            let cutoff = context.q1_cutoff;
            move |_t, item| item.ship_date < cutoff
        })
        .unary_notify::<((u8, u8), (i64, i64)), _>("aggregate", {
            let mut groups: HashMap<(u8, u8), (i64, i64)> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    for item in data {
                        let entry = groups
                            .entry((item.return_flag, item.line_status))
                            .or_default();
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
        .output("results")
        .unwrap();

    let dataflow = builder.build()?;
    let mut handle = runtime.spawn(dataflow, SpawnOptions::new().collect_metrics(true))?;
    let receiver = handle.take_output::<((u8, u8), (i64, i64))>("results")?;
    let metrics = Arc::clone(handle.metrics().expect("metrics enabled for q1"));
    handle.join_blocking()?;

    let actual: HashMap<(u8, u8), (i64, i64)> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();

    Ok(QueryExecution {
        metrics: metrics.operator_snapshots(),
        verification_error: verify_q1(&actual, &context.q1_expected),
    })
}

fn run_q2(
    runtime: &RuntimeHandle,
    context: &QueryContext,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    let builder = DataflowBuilder::<u64>::new("stress-q2");
    let mut pipe = builder.source("src", vec![(0u64, context.q2_values.as_ref().clone())]);
    for idx in 0..10 {
        pipe = pipe.map(format!("map_{idx}"), |_t, value| value + 1);
    }
    pipe.output("results").unwrap();

    let dataflow = builder.build()?;
    let mut handle = runtime.spawn(dataflow, SpawnOptions::new().collect_metrics(true))?;
    let receiver = handle.take_output::<i64>("results")?;
    let metrics = Arc::clone(handle.metrics().expect("metrics enabled for q2"));
    handle.join_blocking()?;

    let actual: Vec<i64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();

    Ok(QueryExecution {
        metrics: metrics.operator_snapshots(),
        verification_error: verify_q2(&actual, context.q2_expected.as_slice()),
    })
}

fn run_q3(
    runtime: &RuntimeHandle,
    context: &QueryContext,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    let builder = DataflowBuilder::<u64>::new("stress-q3");
    builder
        .source("src", vec![(0u64, context.q3_values.as_ref().clone())])
        .filter("threshold", {
            let threshold = context.q3_threshold;
            move |_t, value| *value > threshold
        })
        .output("results")
        .unwrap();

    let dataflow = builder.build()?;
    let mut handle = runtime.spawn(dataflow, SpawnOptions::new().collect_metrics(true))?;
    let receiver = handle.take_output::<u64>("results")?;
    let metrics = Arc::clone(handle.metrics().expect("metrics enabled for q3"));
    handle.join_blocking()?;

    let actual: Vec<u64> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();

    Ok(QueryExecution {
        metrics: metrics.operator_snapshots(),
        verification_error: verify_q3(&actual, context.q3_expected.as_slice()),
    })
}

fn run_q4(
    runtime: &RuntimeHandle,
    context: &QueryContext,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    let mut handle = runtime.spawn_multi(
        "stress-q4",
        JOIN_WORKERS,
        |builder| {
            let left = builder
                .input::<(u64, i64)>("left")
                .unwrap()
                .exchange_by_hash("left_exchange", |(order_key, _)| *order_key);
            let right = builder
                .input::<u64>("right")
                .unwrap()
                .exchange_by_hash("right_exchange", |order_key| *order_key);

            left.binary::<u64, (u64, i64), _>(right, "join", {
                let mut left_state: HashMap<u64, Vec<i64>> = HashMap::new();
                let mut right_counts: HashMap<u64, usize> = HashMap::new();
                move |left_in, right_in, output| {
                    while let Some((time, data)) = left_in.next() {
                        let mut matched = Vec::new();
                        for (order_key, revenue) in data {
                            if let Some(count) = right_counts.get(&order_key) {
                                for _ in 0..*count {
                                    matched.push((order_key, revenue));
                                }
                            }
                            left_state.entry(order_key).or_default().push(revenue);
                        }
                        if !matched.is_empty() {
                            output.push_vec(time, matched);
                        }
                    }

                    while let Some((time, data)) = right_in.next() {
                        let mut matched = Vec::new();
                        for order_key in data {
                            if let Some(revenues) = left_state.get(&order_key) {
                                matched.extend(
                                    revenues.iter().copied().map(|revenue| (order_key, revenue)),
                                );
                            }
                            *right_counts.entry(order_key).or_default() += 1;
                        }
                        if !matched.is_empty() {
                            output.push_vec(time, matched);
                        }
                    }
                    Ok(())
                }
            })
            .unwrap()
            .unary_notify::<(u64, i64), _>("sum_revenue", {
                let mut sums: HashMap<u64, i64> = HashMap::new();
                move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        for (order_key, revenue) in data {
                            *sums.entry(order_key).or_default() += revenue;
                        }
                        ctx.notify_at(time);
                    }
                    while let Some(time) = ctx.next_notification() {
                        let results: Vec<_> = sums.drain().collect();
                        if !results.is_empty() {
                            output.push_vec(time, results);
                        }
                    }
                    Ok(())
                }
            })
            .output("results")
            .unwrap();
            Ok(())
        },
        SpawnOptions::new().collect_metrics(true),
    )?;

    let left_senders = handle.take_all_inputs::<(u64, i64)>("left")?;
    let right_senders = handle.take_all_inputs::<u64>("right")?;
    let receivers = handle.take_all_outputs::<(u64, i64)>("results")?;
    let metrics: Vec<_> = (0..handle.num_workers())
        .filter_map(|worker| handle.worker_mut(worker).metrics().cloned())
        .collect();

    for (sender, batch) in left_senders
        .into_iter()
        .zip(context.q4_left_partitions.iter())
    {
        if !batch.is_empty() {
            sender.send(0u64, batch.clone())?;
        }
        sender.close();
    }
    for (sender, batch) in right_senders
        .into_iter()
        .zip(context.q4_right_partitions.iter())
    {
        if !batch.is_empty() {
            sender.send(0u64, batch.clone())?;
        }
        sender.close();
    }

    handle.join_blocking()?;

    let total_revenue = receivers
        .into_iter()
        .flat_map(|receiver| receiver.collect_data().into_iter())
        .flat_map(|(_, batch)| batch.into_iter())
        .map(|(_, revenue)| revenue)
        .sum::<i64>();

    let mut operator_metrics = Vec::new();
    for worker_metrics in metrics {
        operator_metrics.extend(worker_metrics.operator_snapshots());
    }

    Ok(QueryExecution {
        metrics: operator_metrics,
        verification_error: verify_q4(total_revenue, context.q4_expected_total_revenue),
    })
}

fn run_q5(
    runtime: &RuntimeHandle,
    context: &QueryContext,
) -> Result<QueryExecution, Box<dyn std::error::Error + Send + Sync>> {
    let builder = DataflowBuilder::<u64>::new("stress-q5");
    builder
        .source("src", vec![(0u64, context.q5_edges.as_ref().clone())])
        .unary_notify::<(u64, f64), _>("pagerank", {
            let num_vertices = context.q5_vertices;
            let mut buffered = Vec::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    buffered.extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    let results = compute_pagerank(&buffered, num_vertices, PAGERANK_ITERATIONS);
                    if !results.is_empty() {
                        output.push_vec(time, results);
                    }
                    buffered.clear();
                }
                Ok(())
            }
        })
        .output("results")
        .unwrap();

    let dataflow = builder.build()?;
    let mut handle = runtime.spawn(dataflow, SpawnOptions::new().collect_metrics(true))?;
    let receiver = handle.take_output::<(u64, f64)>("results")?;
    let metrics = Arc::clone(handle.metrics().expect("metrics enabled for q5"));
    handle.join_blocking()?;

    let actual: Vec<(u64, f64)> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, batch)| batch)
        .collect();

    Ok(QueryExecution {
        metrics: metrics.operator_snapshots(),
        verification_error: verify_q5(&actual, &context.q5_expected_top10),
    })
}

async fn status_reporter(shared: Arc<SharedState>, stop: Arc<AtomicBool>) {
    let mut last_queries = 0u64;
    let mut last_tick = Instant::now();

    loop {
        for _ in 0..STATUS_INTERVAL_SECS {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let total_queries = shared.total_queries.load(Ordering::Relaxed);
        let total_errors = shared.total_errors.load(Ordering::Relaxed);
        let elapsed_since_last = last_tick.elapsed().as_secs_f64().max(1e-9);
        let rate = (total_queries.saturating_sub(last_queries)) as f64 / elapsed_since_last;
        last_queries = total_queries;
        last_tick = Instant::now();

        shared
            .throughput_samples
            .lock()
            .unwrap()
            .push(ThroughputSample {
                timestamp_secs: shared.started_at.elapsed().as_secs_f64(),
                queries_per_sec: rate,
            });

        let overall = overall_latency_snapshot(&shared);
        let memory_mb = latest_memory_bytes(&shared) as f64 / (1024.0 * 1024.0);
        println!(
            "[{:>6}] Phase: {:<10} | Queries: {:>6} | Rate: {:>5.1}/s | Errors: {} | Memory: {:>6.1} MB",
            format_mm_ss(shared.started_at.elapsed()),
            shared.current_phase(),
            total_queries,
            rate,
            total_errors,
            memory_mb,
        );
        println!(
            "         Latency: p50={} p95={} p99={} max={}",
            format_latency_ms(overall.p50_us),
            format_latency_ms(overall.p95_us),
            format_latency_ms(overall.p99_us),
            format_latency_ms(overall.max_us),
        );
    }
}

async fn memory_sampler(shared: Arc<SharedState>, stop: Arc<AtomicBool>) {
    loop {
        for _ in 0..MEMORY_SAMPLE_INTERVAL_SECS {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        record_memory_sample(&shared);
    }
}

fn record_memory_sample(shared: &Arc<SharedState>) {
    shared.memory_samples.lock().unwrap().push(MemorySample {
        timestamp_secs: shared.started_at.elapsed().as_secs_f64(),
        working_set_bytes: get_memory_bytes(),
        phase: shared.current_phase(),
    });
}

fn build_query_context() -> QueryContext {
    let q1_cutoff = 11_200;
    let q1_items = Arc::new(generate_lineitems(500_000));
    let q1_expected = compute_scan_filter_aggregate(q1_items.as_slice(), q1_cutoff);
    assert!(
        !q1_expected.is_empty(),
        "q1 expected result must not be empty"
    );

    let q2_values = Arc::new((0..100_000).map(|v| v as i64).collect::<Vec<_>>());
    let q2_expected = Arc::new(q2_values.iter().map(|value| value + 10).collect::<Vec<_>>());
    assert_eq!(
        q2_expected.len(),
        q2_values.len(),
        "q2 expected length mismatch"
    );

    let q3_threshold = 275;
    let q3_values = Arc::new((0..500).map(|v| v as u64).collect::<Vec<_>>());
    let q3_expected = Arc::new(
        q3_values
            .iter()
            .copied()
            .filter(|value| *value > q3_threshold)
            .collect::<Vec<_>>(),
    );
    assert!(
        !q3_expected.is_empty(),
        "q3 expected result must not be empty"
    );

    let q4_items = generate_lineitems(50_000);
    let (q4_left_partitions, q4_right_partitions) = build_join_inputs(&q4_items, JOIN_WORKERS);
    let q4_expected_total_revenue = compute_join_total(&q4_items);

    // PageRank: expected is computed with the same algorithm. This tests that the dataflow
    // framework correctly transports data and invokes the operator, not the algorithm itself.
    let q5_vertices = 5_000;
    let q5_edges = Arc::new(generate_graph(q5_vertices, 25_000));
    let q5_expected_top10 = top_k_pagerank(
        &compute_pagerank(q5_edges.as_slice(), q5_vertices, PAGERANK_ITERATIONS),
        PAGERANK_TOP_K,
    );
    assert_eq!(
        q5_expected_top10.len(),
        PAGERANK_TOP_K,
        "q5 top-10 size mismatch"
    );

    QueryContext {
        q1_items,
        q1_cutoff,
        q1_expected,
        q2_values,
        q2_expected,
        q3_values,
        q3_threshold,
        q3_expected,
        q4_left_partitions: Arc::new(q4_left_partitions),
        q4_right_partitions: Arc::new(q4_right_partitions),
        q4_expected_total_revenue,
        q5_edges,
        q5_vertices,
        q5_expected_top10,
    }
}

fn generate_lineitems(count: usize) -> Vec<LineItem> {
    let mut seed: u64 = 42;
    let mut next = || -> u64 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    (0..count)
        .map(|_| LineItem {
            order_key: next() % 1_500_000,
            part_key: next() % 200_000,
            quantity: (next() % 50 + 1) as i64,
            price: (next() % 100_000 + 100) as i64,
            discount: (next() % 11) as i64,
            tax: (next() % 9) as i64,
            ship_date: 10_000 + (next() % 2_500),
            return_flag: (next() % 3) as u8,
            line_status: (next() % 2) as u8,
        })
        .collect()
}

fn generate_graph(num_vertices: u64, num_edges: usize) -> Vec<Edge> {
    let mut seed: u64 = 123;
    let mut next = || -> u64 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    (0..num_edges)
        .map(|_| Edge {
            src: next() % num_vertices,
            dst: next() % num_vertices,
        })
        .collect()
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

fn compute_scan_filter_aggregate(items: &[LineItem], cutoff: u64) -> HashMap<(u8, u8), (i64, i64)> {
    let mut groups = HashMap::new();
    for item in items.iter().filter(|item| item.ship_date < cutoff) {
        let entry = groups
            .entry((item.return_flag, item.line_status))
            .or_insert((0, 0));
        entry.0 += item.quantity;
        entry.1 += item.price;
    }
    groups
}

fn partition_round_robin<T: Clone>(items: &[T], workers: usize) -> Vec<Vec<T>> {
    let mut partitions = vec![Vec::new(); workers];
    for (idx, item) in items.iter().cloned().enumerate() {
        partitions[idx % workers].push(item);
    }
    partitions
}

fn build_join_inputs(items: &[LineItem], workers: usize) -> (Vec<Vec<(u64, i64)>>, Vec<Vec<u64>>) {
    let left: Vec<(u64, i64)> = items
        .iter()
        .filter(|item| item.ship_date < 11_200)
        .map(|item| (item.order_key, line_revenue(item)))
        .collect();
    let right: Vec<u64> = items
        .iter()
        .filter(|item| item.quantity >= 25)
        .map(|item| item.order_key)
        .collect();

    (
        partition_round_robin(&left, workers),
        partition_round_robin(&right, workers),
    )
}

fn compute_join_total(items: &[LineItem]) -> i64 {
    let mut right_counts = HashMap::<u64, i64>::new();
    for item in items.iter().filter(|item| item.quantity >= 25) {
        *right_counts.entry(item.order_key).or_default() += 1;
    }

    items
        .iter()
        .filter(|item| item.ship_date < 11_200)
        .map(|item| line_revenue(item) * right_counts.get(&item.order_key).copied().unwrap_or(0))
        .sum()
}

fn line_revenue(item: &LineItem) -> i64 {
    item.price * item.discount
}

fn top_k_pagerank(ranks: &[(u64, f64)], k: usize) -> Vec<(u64, f64)> {
    let mut sorted = ranks.to_vec();
    sorted.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(CmpOrdering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    sorted.truncate(k);
    sorted
}

fn verify_q1(
    actual: &HashMap<(u8, u8), (i64, i64)>,
    expected: &HashMap<(u8, u8), (i64, i64)>,
) -> Option<String> {
    if actual == expected {
        None
    } else {
        Some(format!("expected {:?}, got {:?}", expected, actual))
    }
}

fn verify_q2(actual: &[i64], expected: &[i64]) -> Option<String> {
    if actual == expected {
        None
    } else {
        let mismatches: Vec<_> = actual
            .iter()
            .zip(expected.iter())
            .enumerate()
            .filter(|(_, (a, e))| a != e)
            .take(3)
            .map(|(idx, (a, e))| format!("[{idx}]: got {a}, expected {e}"))
            .collect();
        Some(format!(
            "len={}/{} mismatches: {}",
            actual.len(),
            expected.len(),
            if mismatches.is_empty() {
                "length mismatch".to_string()
            } else {
                mismatches.join(", ")
            }
        ))
    }
}

fn verify_q3(actual: &[u64], expected: &[u64]) -> Option<String> {
    if actual == expected {
        None
    } else {
        Some(format!("expected {:?}, got {:?}", expected, actual))
    }
}

fn verify_q4(actual_total_revenue: i64, expected_total_revenue: i64) -> Option<String> {
    if actual_total_revenue == expected_total_revenue {
        None
    } else {
        Some(format!(
            "total revenue {} != expected {}",
            actual_total_revenue, expected_total_revenue
        ))
    }
}

fn verify_q5(actual: &[(u64, f64)], expected_top10: &[(u64, f64)]) -> Option<String> {
    let actual_top10 = top_k_pagerank(actual, PAGERANK_TOP_K);
    let mismatch = actual_top10
        .iter()
        .zip(expected_top10.iter())
        .enumerate()
        .find(|(_, (actual, expected))| {
            actual.0 != expected.0 || (actual.1 - expected.1).abs() > 1e-12
        })
        .map(|(idx, (actual, expected))| (idx, *actual, *expected));

    if actual_top10.len() == expected_top10.len() && mismatch.is_none() {
        None
    } else {
        Some(format!(
            "top-10 mismatch: actual={:?}, expected={:?}",
            actual_top10, expected_top10
        ))
    }
}

fn choose_query(seed: &mut u64) -> QueryKind {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    match *seed % 100 {
        0..=14 => QueryKind::ScanFilterAggregate,
        15..=39 => QueryKind::MapChain,
        40..=74 => QueryKind::SmallFilter,
        75..=89 => QueryKind::MultiWorkerJoin,
        _ => QueryKind::PageRankBatch,
    }
}

struct PercentileSnapshot {
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

fn overall_latency_snapshot(shared: &SharedState) -> PercentileSnapshot {
    let mut latencies = Vec::new();
    for stats in &shared.stats {
        latencies.extend(stats.latencies.lock().unwrap().iter().copied());
    }
    latency_percentiles(&latencies)
}

fn latency_percentiles(latencies: &[u64]) -> PercentileSnapshot {
    if latencies.is_empty() {
        return PercentileSnapshot {
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            max_us: 0,
        };
    }

    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let last = sorted.len() - 1;
    PercentileSnapshot {
        p50_us: sorted[(last * 50) / 100],
        p95_us: sorted[(last * 95) / 100],
        p99_us: sorted[(last * 99) / 100],
        max_us: *sorted.last().unwrap(),
    }
}

fn latest_memory_bytes(shared: &SharedState) -> u64 {
    shared
        .memory_samples
        .lock()
        .unwrap()
        .last()
        .map(|sample| sample.working_set_bytes)
        .unwrap_or(0)
}

fn print_final_report(total_duration: Duration, shared: &SharedState) {
    let stats = shared.snapshot_stats();
    let total_queries = shared.total_queries.load(Ordering::Relaxed);
    let total_errors = shared.total_errors.load(Ordering::Relaxed);
    let total_correctness_failures = shared.total_correctness_failures.load(Ordering::Relaxed);
    let throughput_samples = shared.throughput_samples.lock().unwrap();
    let peak_sample = throughput_samples.iter().max_by(|a, b| {
        a.queries_per_sec
            .partial_cmp(&b.queries_per_sec)
            .unwrap_or(CmpOrdering::Equal)
    });
    let peak_rate = peak_sample
        .map(|sample| sample.queries_per_sec)
        .unwrap_or(0.0);
    let peak_rate_time = peak_sample
        .map(|sample| sample.timestamp_secs)
        .unwrap_or(0.0);
    let average_rate = if total_duration.as_secs_f64() > 0.0 {
        total_queries as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };
    let memory_samples = shared.memory_samples.lock().unwrap();
    let baseline = average_memory_for_phase(&memory_samples, "Warm-up");
    let peak_memory_sample = memory_samples
        .iter()
        .max_by_key(|sample| sample.working_set_bytes);
    let post_idle = memory_samples
        .iter()
        .rev()
        .find(|sample| sample.phase == "Idle")
        .or_else(|| memory_samples.last())
        .map(|sample| sample.working_set_bytes)
        .unwrap_or(0);
    let leak_detected = baseline > 0 && post_idle > baseline + baseline / 2; // 1.5x threshold

    println!("\n=== STRESS TEST REPORT ===");
    println!("Duration: {}", format_compact_duration(total_duration));
    println!("Total Queries: {}", total_queries);
    println!("Average Throughput: {:.2}/s", average_rate);
    println!(
        "Peak Observed Throughput: {:.2}/s @ {}",
        peak_rate,
        format_mm_ss(Duration::from_secs_f64(peak_rate_time))
    );
    println!("Total Errors: {}", total_errors);
    println!("Correctness Failures: {}", total_correctness_failures);
    println!();
    println!("Per-Query Statistics:");

    let mut slowest_query: Option<(&'static str, u64)> = None;
    let mut most_variable: Option<(&'static str, f64)> = None;
    let mut highest_error_rate: Option<(&'static str, f64)> = None;
    let mut operator_hotspots: Vec<(String, u64, &'static str)> = Vec::new();

    for snapshot in &stats {
        let percentiles = latency_percentiles(&snapshot.latencies);
        let avg_latency = if snapshot.count > 0 {
            snapshot.total_latency_us as f64 / snapshot.count as f64
        } else {
            0.0
        };
        println!(
            "  {:<20} {:>6} queries, avg={} p50={} p95={} p99={} max={} errors={} correctness_failures={}",
            snapshot.query_type,
            snapshot.count,
            format_latency_ms(avg_latency as u64),
            format_latency_ms(percentiles.p50_us),
            format_latency_ms(percentiles.p95_us),
            format_latency_ms(percentiles.p99_us),
            format_latency_ms(snapshot.max_latency_us.max(percentiles.max_us)),
            snapshot.errors,
            snapshot.correctness_failures,
        );

        if slowest_query
            .as_ref()
            .map(|(_, current)| percentiles.p99_us > *current)
            .unwrap_or(true)
        {
            slowest_query = Some((snapshot.query_type, percentiles.p99_us));
        }

        let variability = if percentiles.p50_us > 0 {
            percentiles.p99_us as f64 / percentiles.p50_us as f64
        } else {
            0.0
        };
        if most_variable
            .as_ref()
            .map(|(_, current)| variability > *current)
            .unwrap_or(true)
        {
            most_variable = Some((snapshot.query_type, variability));
        }

        let total_failures = snapshot.errors + snapshot.correctness_failures;
        let error_rate = if snapshot.count > 0 {
            total_failures as f64 / snapshot.count as f64
        } else {
            0.0
        };
        if highest_error_rate
            .as_ref()
            .map(|(_, current)| error_rate > *current)
            .unwrap_or(true)
        {
            highest_error_rate = Some((snapshot.query_type, error_rate));
        }

        let total_operator_cpu: u64 = snapshot.operator_cpu_us.values().copied().sum();
        if total_operator_cpu > 0 {
            let mut ops: Vec<_> = snapshot.operator_cpu_us.iter().collect();
            ops.sort_by_key(|(_, cpu)| std::cmp::Reverse(**cpu));
            for (name, cpu) in ops.into_iter().take(2) {
                let activations = snapshot
                    .operator_activations
                    .get(name)
                    .copied()
                    .unwrap_or(0);
                let records = snapshot.operator_records.get(name).copied().unwrap_or(0);
                operator_hotspots.push((
                    format!(
                        "{} (activations={}, records={})",
                        name, activations, records
                    ),
                    *cpu,
                    snapshot.query_type,
                ));
            }
        }
    }

    operator_hotspots.sort_by_key(|(_, cpu, _)| std::cmp::Reverse(*cpu));

    println!();
    println!("Memory Profile:");
    println!("  Baseline: {}", format_bytes(baseline));
    if let Some(sample) = peak_memory_sample {
        println!(
            "  Peak ({} phase @ {}): {}",
            sample.phase,
            format_mm_ss(Duration::from_secs_f64(sample.timestamp_secs)),
            format_bytes(sample.working_set_bytes)
        );
    } else {
        println!("  Peak: n/a");
    }
    println!("  Post-idle: {}", format_bytes(post_idle));
    println!(
        "  Leak detected: {} (post-idle {} baseline)",
        if leak_detected { "YES" } else { "NO" },
        if leak_detected {
            "> 1.5x"
        } else {
            "within 1.5x"
        }
    );

    println!();
    println!("Bottleneck Analysis:");
    if let Some((query, p99)) = slowest_query {
        println!(
            "  Slowest query type: {} (p99={})",
            query,
            format_latency_ms(p99)
        );
    }
    if let Some((query, ratio)) = most_variable {
        println!("  Most variable: {} (p99/p50 = {:.2}x)", query, ratio);
    }
    if let Some((query, rate)) = highest_error_rate {
        if rate > 0.0 {
            println!("  Highest error rate: {} ({:.2}%)", query, rate * 100.0);
        } else {
            println!("  Highest error rate: None");
        }
    }
    if !operator_hotspots.is_empty() {
        println!("  Top operator hotspots:");
        for (name, cpu_us, query_type) in operator_hotspots.into_iter().take(5) {
            println!(
                "    {} / {} => {} CPU",
                query_type,
                name,
                format_latency_ms(cpu_us)
            );
        }
    }
}

fn average_memory_for_phase(samples: &[MemorySample], phase: &str) -> u64 {
    let mut total = 0u128;
    let mut count = 0u128;
    for sample in samples.iter().filter(|sample| sample.phase == phase) {
        total += sample.working_set_bytes as u128;
        count += 1;
    }
    if count == 0 {
        0
    } else {
        (total / count) as u64
    }
}

fn format_mm_ss(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let minutes = total_secs / 60;
    let seconds = total_secs % 60;
    format!("{:>2}:{:02}", minutes, seconds)
}

fn format_compact_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let minutes = total_secs / 60;
    let seconds = total_secs % 60;
    format!("{}m {}s", minutes, seconds)
}

fn format_latency_ms(latency_us: u64) -> String {
    if latency_us >= 1_000 {
        format!("{:.1}ms", latency_us as f64 / 1_000.0)
    } else {
        format!("{}µs", latency_us)
    }
}

fn format_bytes(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

#[derive(Serialize)]
struct StressTestReport {
    duration_secs: f64,
    worker_threads: usize,
    max_concurrent: usize,
    total_queries: u64,
    total_errors: u64,
    correctness_failures: u64,
    average_throughput: f64,
    peak_throughput: f64,
    queries: Vec<QueryReport>,
    memory: MemoryReport,
    bottleneck: BottleneckReport,
}

#[derive(Serialize)]
struct QueryReport {
    name: String,
    count: u64,
    errors: u64,
    correctness_failures: u64,
    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    max_latency_ms: f64,
    top_operators: Vec<OperatorReport>,
}

#[derive(Serialize)]
struct OperatorReport {
    name: String,
    total_cpu_ms: f64,
    activations: u64,
    records: u64,
}

#[derive(Serialize)]
struct MemoryReport {
    baseline_mb: f64,
    peak_mb: f64,
    peak_phase: String,
    post_idle_mb: f64,
    leak_detected: bool,
}

#[derive(Serialize)]
struct BottleneckReport {
    slowest_query: String,
    slowest_p99_ms: f64,
    most_variable_query: String,
    most_variable_ratio: f64,
}

fn build_json_report(
    total_duration: Duration,
    config: &StressTestConfig,
    shared: &SharedState,
) -> StressTestReport {
    let stats = shared.snapshot_stats();
    let total_queries = shared.total_queries.load(Ordering::Relaxed);
    let total_errors = shared.total_errors.load(Ordering::Relaxed);
    let total_correctness_failures = shared.total_correctness_failures.load(Ordering::Relaxed);

    let throughput_samples = shared.throughput_samples.lock().unwrap();
    let peak_throughput = throughput_samples
        .iter()
        .map(|s| s.queries_per_sec)
        .fold(0.0f64, f64::max);
    let average_throughput = if total_duration.as_secs_f64() > 0.0 {
        total_queries as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };
    drop(throughput_samples);

    let memory_samples = shared.memory_samples.lock().unwrap();
    let baseline = average_memory_for_phase(&memory_samples, "Warm-up");
    let (peak_bytes, peak_phase) = memory_samples
        .iter()
        .max_by_key(|s| s.working_set_bytes)
        .map(|s| (s.working_set_bytes, s.phase))
        .unwrap_or((0, "N/A"));
    let post_idle = memory_samples
        .iter()
        .rev()
        .find(|s| s.phase == "Idle")
        .or_else(|| memory_samples.last())
        .map(|s| s.working_set_bytes)
        .unwrap_or(0);
    let leak_detected = baseline > 0 && post_idle > baseline + baseline / 2; // 1.5x threshold
    drop(memory_samples);

    let mut slowest_query = String::new();
    let mut slowest_p99 = 0u64;
    let mut most_variable_query = String::new();
    let mut most_variable_ratio = 0.0f64;

    let queries: Vec<QueryReport> = stats
        .iter()
        .map(|snapshot| {
            let percentiles = latency_percentiles(&snapshot.latencies);
            let avg = if snapshot.count > 0 {
                snapshot.total_latency_us as f64 / snapshot.count as f64
            } else {
                0.0
            };

            if percentiles.p99_us > slowest_p99 {
                slowest_p99 = percentiles.p99_us;
                slowest_query = snapshot.query_type.to_string();
            }
            let variability = if percentiles.p50_us > 0 {
                percentiles.p99_us as f64 / percentiles.p50_us as f64
            } else {
                0.0
            };
            if variability > most_variable_ratio {
                most_variable_ratio = variability;
                most_variable_query = snapshot.query_type.to_string();
            }

            let mut ops: Vec<_> = snapshot.operator_cpu_us.iter().collect();
            ops.sort_by_key(|(_, cpu)| std::cmp::Reverse(**cpu));
            let top_operators = ops
                .into_iter()
                .take(3)
                .map(|(name, cpu)| OperatorReport {
                    name: name.clone(),
                    total_cpu_ms: *cpu as f64 / 1000.0,
                    activations: snapshot
                        .operator_activations
                        .get(name)
                        .copied()
                        .unwrap_or(0),
                    records: snapshot.operator_records.get(name).copied().unwrap_or(0),
                })
                .collect();

            QueryReport {
                name: snapshot.query_type.to_string(),
                count: snapshot.count,
                errors: snapshot.errors,
                correctness_failures: snapshot.correctness_failures,
                avg_latency_ms: avg / 1000.0,
                p50_latency_ms: percentiles.p50_us as f64 / 1000.0,
                p95_latency_ms: percentiles.p95_us as f64 / 1000.0,
                p99_latency_ms: percentiles.p99_us as f64 / 1000.0,
                max_latency_ms: snapshot.max_latency_us.max(percentiles.max_us) as f64 / 1000.0,
                top_operators,
            }
        })
        .collect();

    StressTestReport {
        duration_secs: total_duration.as_secs_f64(),
        worker_threads: config.worker_threads,
        max_concurrent: config.max_concurrent,
        total_queries,
        total_errors,
        correctness_failures: total_correctness_failures,
        average_throughput,
        peak_throughput,
        queries,
        memory: MemoryReport {
            baseline_mb: baseline as f64 / (1024.0 * 1024.0),
            peak_mb: peak_bytes as f64 / (1024.0 * 1024.0),
            peak_phase: peak_phase.to_string(),
            post_idle_mb: post_idle as f64 / (1024.0 * 1024.0),
            leak_detected,
        },
        bottleneck: BottleneckReport {
            slowest_query,
            slowest_p99_ms: slowest_p99 as f64 / 1000.0,
            most_variable_query,
            most_variable_ratio,
        },
    }
}

fn get_memory_bytes() -> u64 {
    #[cfg(windows)]
    {
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

        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentProcess() -> isize;
        }

        #[link(name = "psapi")]
        unsafe extern "system" {
            fn K32GetProcessMemoryInfo(h: isize, p: *mut ProcessMemoryCounters, cb: u32) -> i32;
        }

        unsafe {
            let mut counters = std::mem::zeroed::<ProcessMemoryCounters>();
            counters.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
            if K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) != 0 {
                counters.working_set_size as u64
            } else {
                0
            }
        }
    }
    #[cfg(not(windows))]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: u64 = line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0);
                    return kb * 1024;
                }
            }
        }
        0
    }
}
