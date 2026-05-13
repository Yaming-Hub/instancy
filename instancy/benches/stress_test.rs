use std::collections::{HashMap, VecDeque};
use std::env;
use std::f64::consts::PI;
use std::fmt;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

const DEFAULT_DURATION_SECS: u64 = 6 * 60 * 60;
const DEFAULT_REPORT_INTERVAL_SECS: u64 = 5 * 60;
const DEFAULT_WORKERS: usize = 8;
const DEFAULT_RUNTIMES: usize = 2;
const DEFAULT_BASE_RPS: f64 = 20.0;
const DEFAULT_FAILURE_RATE: f64 = 0.01;
const DEFAULT_CANCEL_RATE: f64 = 0.01;
const RPS_AMPLITUDE: f64 = 15.0;
const RPS_PERIOD_SECS: f64 = 600.0;
const BURST_EVERY_SECS: u64 = 30 * 60;
const BURST_DURATION_SECS: u64 = 2 * 60;
const BURST_RPS: f64 = 80.0;
const MAX_IN_FLIGHT: u64 = 200;
const MAP_CHAIN_MEDIUM_MIN: usize = 10_000;
const MAP_CHAIN_MEDIUM_MAX: usize = 100_000;
const PAGERANK_ITERATIONS: usize = 10;
const LEAK_BASELINE_SECS: u64 = 10 * 60;
const LEAK_WINDOW_SECS: u64 = 10 * 60;
const REPORT_RPS_WINDOW_SECS: u64 = 5 * 60;
const REPORT_NAMES: [&str; 7] = [
    "ScanFilterAgg",
    "PageRank",
    "MapChain20",
    "MultiEpoch",
    "SmallPipeline",
    "FailureInjection",
    "Cancellation",
];

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

#[derive(Clone, Debug)]
struct SystemSnapshot {
    working_set_mb: f64,
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
        fn K32GetProcessMemoryInfo(
            h: isize,
            pmc: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
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
        let (user_ms, kernel_ms) = if GetProcessTimes(
            h,
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        ) != 0
        {
            let ft_to_ms = |ft: &FileTime| {
                ((ft.high as u64) << 32 | ft.low as u64) as f64 / 10_000.0
            };
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
    SystemSnapshot {
        working_set_mb: 0.0,
        peak_working_set_mb: 0.0,
        cpu_user_ms: 0.0,
        cpu_kernel_ms: 0.0,
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn next_f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range_usize(&mut self, start: usize, end_inclusive: usize) -> usize {
        if start >= end_inclusive {
            return start;
        }
        start + (self.next() as usize % (end_inclusive - start + 1))
    }
}

#[derive(Clone, Copy, Debug)]
enum QueryType {
    ScanFilterAgg,
    PageRank,
    MapChain20,
    MultiEpoch,
    SmallPipeline,
    FailureInjection,
    Cancellation,
}

impl QueryType {
    fn name(self) -> &'static str {
        match self {
            Self::ScanFilterAgg => "ScanFilterAgg",
            Self::PageRank => "PageRank",
            Self::MapChain20 => "MapChain20",
            Self::MultiEpoch => "MultiEpoch",
            Self::SmallPipeline => "SmallPipeline",
            Self::FailureInjection => "FailureInjection",
            Self::Cancellation => "Cancellation",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SizeClass {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Debug)]
struct QuerySpec {
    query_type: QueryType,
    size_class: Option<SizeClass>,
    map_values: usize,
    map_stages: usize,
    multi_epochs: u64,
    multi_batch_size: u64,
    multi_threshold: u64,
    scan_records: usize,
    pagerank_vertices: u64,
    pagerank_edges: usize,
    pagerank_iterations: usize,
    small_values: usize,
}

impl QuerySpec {
    fn new(query_type: QueryType) -> Self {
        Self {
            query_type,
            size_class: None,
            map_values: 0,
            map_stages: 0,
            multi_epochs: 0,
            multi_batch_size: 0,
            multi_threshold: 0,
            scan_records: 0,
            pagerank_vertices: 0,
            pagerank_edges: 0,
            pagerank_iterations: 0,
            small_values: 0,
        }
    }

    fn label(&self) -> &'static str {
        self.query_type.name()
    }
}

#[derive(Clone, Debug)]
struct Config {
    duration: Duration,
    report_interval: Duration,
    workers: usize,
    runtimes: usize,
    base_rps: f64,
    failure_rate: f64,
    cancel_rate: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            report_interval: Duration::from_secs(DEFAULT_REPORT_INTERVAL_SECS),
            workers: DEFAULT_WORKERS,
            runtimes: DEFAULT_RUNTIMES,
            base_rps: DEFAULT_BASE_RPS,
            failure_rate: DEFAULT_FAILURE_RATE,
            cancel_rate: DEFAULT_CANCEL_RATE,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Success,
    ExpectedFailure,
    UnexpectedFailure,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default)]
struct TypeStats {
    submitted: u64,
    completed: u64,
    success: u64,
    expected_failures: u64,
    unexpected_failures: u64,
    cancelled: u64,
}

#[derive(Clone, Debug)]
struct SnapshotSample {
    elapsed_secs: u64,
    submitted: u64,
    completed: u64,
    in_flight: u64,
    snapshot: SystemSnapshot,
}

struct SharedMetrics {
    submitted: AtomicU64,
    completed: AtomicU64,
    success: AtomicU64,
    expected_failures: AtomicU64,
    unexpected_failures: AtomicU64,
    cancelled: AtomicU64,
    in_flight: Arc<AtomicU64>,
    per_type: Mutex<HashMap<&'static str, TypeStats>>,
    snapshots: Mutex<Vec<SnapshotSample>>,
    unexpected_messages: Mutex<Vec<String>>,
}

impl SharedMetrics {
    fn new(in_flight: Arc<AtomicU64>) -> Self {
        let mut per_type = HashMap::new();
        for name in REPORT_NAMES {
            per_type.insert(name, TypeStats::default());
        }
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            success: AtomicU64::new(0),
            expected_failures: AtomicU64::new(0),
            unexpected_failures: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            in_flight,
            per_type: Mutex::new(per_type),
            snapshots: Mutex::new(Vec::new()),
            unexpected_messages: Mutex::new(Vec::new()),
        }
    }

    fn record_submit(&self, name: &'static str) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let mut per_type = self.per_type.lock().unwrap();
        per_type.entry(name).or_default().submitted += 1;
    }

    fn record_completion(&self, name: &'static str, outcome: Outcome, detail: Option<String>) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let mut per_type = self.per_type.lock().unwrap();
        let stats = per_type.entry(name).or_default();
        stats.completed += 1;
        match outcome {
            Outcome::Success => {
                self.success.fetch_add(1, Ordering::Relaxed);
                stats.success += 1;
            }
            Outcome::ExpectedFailure => {
                self.expected_failures.fetch_add(1, Ordering::Relaxed);
                stats.expected_failures += 1;
            }
            Outcome::UnexpectedFailure => {
                self.unexpected_failures.fetch_add(1, Ordering::Relaxed);
                stats.unexpected_failures += 1;
                if let Some(detail) = detail {
                    let mut messages = self.unexpected_messages.lock().unwrap();
                    if messages.len() < 32 {
                        messages.push(detail);
                    }
                }
            }
            Outcome::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
                stats.cancelled += 1;
            }
        }
    }

    fn record_snapshot(&self, elapsed_secs: u64, snapshot: SystemSnapshot) {
        let sample = SnapshotSample {
            elapsed_secs,
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            snapshot,
        };
        self.snapshots.lock().unwrap().push(sample);
    }
}

#[derive(Debug)]
struct InjectedFailure;

impl fmt::Display for InjectedFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "injected failure")
    }
}

impl std::error::Error for InjectedFailure {}

fn main() {
    let config = match parse_args() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("error: {err}");
            print_usage();
            std::process::exit(2);
        }
    };

    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = tokio_rt.enter();

    println!(
        "starting stress_test duration={} report_interval={} workers={} runtimes={} base_rps={:.1} failure_rate={:.2}% cancel_rate={:.2}% max_in_flight={}",
        format_hms(config.duration.as_secs()),
        format_hms(config.report_interval.as_secs()),
        config.workers,
        config.runtimes,
        config.base_rps,
        config.failure_rate * 100.0,
        config.cancel_rate * 100.0,
        MAX_IN_FLIGHT,
    );

    let runtimes: Vec<_> = (0..config.runtimes)
        .map(|idx| {
            Arc::new(
                RuntimeHandle::new(RuntimeConfig {
                    worker_threads: config.workers,
                    name: format!("stress-rt-{idx}"),
                    ..Default::default()
                })
                .unwrap(),
            )
        })
        .collect();

    let in_flight = Arc::new(AtomicU64::new(0));
    let shared = Arc::new(SharedMetrics::new(Arc::clone(&in_flight)));
    let start_snapshot = system_snapshot();
    shared.record_snapshot(0, start_snapshot.clone());

    let start = Instant::now();
    let mut next_submit = start;
    let mut next_report = start + config.report_interval;
    let mut last_report_at = start;
    let mut last_cpu_snapshot = start_snapshot.clone();
    let mut rng = Rng(0x5eed_fade_d15c_a11e);
    let mut recent_submissions = VecDeque::new();
    let mut stopped_submitting = false;

    loop {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(start);

        if !stopped_submitting && elapsed >= config.duration {
            stopped_submitting = true;
        }

        if !stopped_submitting {
            while Instant::now() >= next_submit {
                if in_flight.load(Ordering::Relaxed) >= MAX_IN_FLIGHT {
                    break;
                }
                let spec = choose_query(&mut rng, &config);
                let runtime_idx = (rng.next() as usize) % runtimes.len();
                shared.record_submit(spec.label());
                recent_submissions.push_back(Instant::now());
                spawn_query_thread(
                    Arc::clone(&shared),
                    Arc::clone(&runtimes[runtime_idx]),
                    spec,
                    runtime_idx,
                );

                let schedule_now = Instant::now();
                let schedule_elapsed = schedule_now.saturating_duration_since(start);
                let target_rps = current_target_rps(schedule_elapsed, &config).max(1.0);
                next_submit += Duration::from_secs_f64(1.0 / target_rps);
                if schedule_elapsed >= config.duration {
                    stopped_submitting = true;
                    break;
                }
            }
        }

        while let Some(front) = recent_submissions.front() {
            if now.saturating_duration_since(*front).as_secs() > REPORT_RPS_WINDOW_SECS {
                recent_submissions.pop_front();
            } else {
                break;
            }
        }

        let should_report = now >= next_report || (stopped_submitting && in_flight.load(Ordering::Relaxed) == 0);
        if should_report {
            let snapshot = system_snapshot();
            let elapsed_secs = now.saturating_duration_since(start).as_secs();
            shared.record_snapshot(elapsed_secs, snapshot.clone());

            let cpu_now = snapshot.cpu_user_ms + snapshot.cpu_kernel_ms;
            let cpu_then = last_cpu_snapshot.cpu_user_ms + last_cpu_snapshot.cpu_kernel_ms;
            let wall_ms = now.saturating_duration_since(last_report_at).as_secs_f64() * 1000.0;
            let cpu_pct = if wall_ms > 0.0 {
                ((cpu_now - cpu_then).max(0.0) / wall_ms) * 100.0
            } else {
                0.0
            };

            println!(
                "[{}] submitted={} completed={} in_flight={} success={} expected_failure={} unexpected_failure={} cancelled={} rps_5m={:.2} rss_mb={:.1} peak_mb={:.1} cpu={:.1}%",
                format_hms(elapsed_secs),
                shared.submitted.load(Ordering::Relaxed),
                shared.completed.load(Ordering::Relaxed),
                in_flight.load(Ordering::Relaxed),
                shared.success.load(Ordering::Relaxed),
                shared.expected_failures.load(Ordering::Relaxed),
                shared.unexpected_failures.load(Ordering::Relaxed),
                shared.cancelled.load(Ordering::Relaxed),
                recent_submissions.len() as f64 / REPORT_RPS_WINDOW_SECS as f64,
                snapshot.working_set_mb,
                snapshot.peak_working_set_mb,
                cpu_pct,
            );

            last_report_at = now;
            last_cpu_snapshot = snapshot;
            while next_report <= now {
                next_report += config.report_interval;
            }
        }

        if stopped_submitting && in_flight.load(Ordering::Relaxed) == 0 {
            break;
        }

        let sleep_until = if !stopped_submitting && in_flight.load(Ordering::Relaxed) < MAX_IN_FLIGHT {
            next_submit.min(next_report)
        } else {
            next_report
        };
        let now = Instant::now();
        let sleep_for = sleep_until
            .checked_duration_since(now)
            .unwrap_or_else(|| Duration::from_millis(25))
            .min(Duration::from_millis(100));
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }

    let final_elapsed = Instant::now().saturating_duration_since(start).as_secs();
    let final_snapshot = system_snapshot();
    shared.record_snapshot(final_elapsed, final_snapshot.clone());
    print_final_report(&shared, &start_snapshot, &final_snapshot, final_elapsed);
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--duration" => {
                config.duration = Duration::from_secs(parse_value(&mut args, "--duration")?);
            }
            "--report-interval" => {
                config.report_interval =
                    Duration::from_secs(parse_value(&mut args, "--report-interval")?);
            }
            "--workers" => {
                config.workers = parse_value(&mut args, "--workers")?;
            }
            "--runtimes" => {
                config.runtimes = parse_value(&mut args, "--runtimes")?;
            }
            "--base-rps" => {
                config.base_rps = parse_value(&mut args, "--base-rps")?;
            }
            "--failure-rate" => {
                config.failure_rate = parse_value(&mut args, "--failure-rate")?;
            }
            "--cancel-rate" => {
                config.cancel_rate = parse_value(&mut args, "--cancel-rate")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                // Ignore unknown flags (e.g., --bench passed by cargo)
                if !other.starts_with("--") {
                    return Err(format!("unknown argument: {other}"));
                }
            }
        }
    }

    if config.workers == 0 {
        return Err("--workers must be > 0".into());
    }
    if config.runtimes == 0 {
        return Err("--runtimes must be > 0".into());
    }
    if config.report_interval.is_zero() {
        return Err("--report-interval must be > 0".into());
    }
    if config.base_rps <= 0.0 {
        return Err("--base-rps must be > 0".into());
    }
    if !(0.0..=1.0).contains(&config.failure_rate) {
        return Err("--failure-rate must be within [0, 1]".into());
    }
    if !(0.0..=1.0).contains(&config.cancel_rate) {
        return Err("--cancel-rate must be within [0, 1]".into());
    }
    if config.failure_rate + config.cancel_rate > 1.0 {
        return Err("failure-rate + cancel-rate must be <= 1".into());
    }

    Ok(config)
}

fn parse_value<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    let raw = args
        .next()
        .ok_or_else(|| format!("missing value for {flag}"))?;
    raw.parse::<T>()
        .map_err(|_| format!("invalid value for {flag}: {raw}"))
}

fn print_usage() {
    eprintln!(
        "usage: cargo bench --bench stress_test -- --duration <SECS> --report-interval <SECS> --workers <N> --runtimes <N> --base-rps <N> --failure-rate <F> --cancel-rate <F>"
    );
}

fn current_target_rps(elapsed: Duration, config: &Config) -> f64 {
    let t = elapsed.as_secs_f64();
    if elapsed.as_secs() % BURST_EVERY_SECS < BURST_DURATION_SECS {
        BURST_RPS
    } else {
        (config.base_rps + RPS_AMPLITUDE * (2.0 * PI * t / RPS_PERIOD_SECS).sin()).max(1.0)
    }
}

fn choose_query(rng: &mut Rng, config: &Config) -> QuerySpec {
    let special = rng.next_f64();
    if special < config.cancel_rate {
        return QuerySpec::new(QueryType::Cancellation);
    }
    if special < config.cancel_rate + config.failure_rate {
        return QuerySpec::new(QueryType::FailureInjection);
    }

    let mix = rng.next_f64();
    if mix < 0.60 {
        choose_small_query(rng)
    } else if mix < 0.85 {
        choose_medium_query(rng)
    } else {
        choose_large_query(rng)
    }
}

fn choose_small_query(rng: &mut Rng) -> QuerySpec {
    match rng.next() % 3 {
        0 => {
            let mut spec = QuerySpec::new(QueryType::SmallPipeline);
            spec.size_class = Some(SizeClass::Small);
            spec.small_values = rng.range_usize(100, 1_000);
            spec
        }
        1 => {
            let mut spec = QuerySpec::new(QueryType::MultiEpoch);
            spec.size_class = Some(SizeClass::Small);
            spec.multi_epochs = 16;
            spec.multi_batch_size = 256;
            spec.multi_threshold = (spec.multi_epochs * spec.multi_batch_size) / 2;
            spec
        }
        _ => {
            let mut spec = QuerySpec::new(QueryType::MapChain20);
            spec.size_class = Some(SizeClass::Small);
            spec.map_values = 1_000;
            spec.map_stages = 5;
            spec
        }
    }
}

fn choose_medium_query(rng: &mut Rng) -> QuerySpec {
    match rng.next() % 3 {
        0 => {
            let mut spec = QuerySpec::new(QueryType::ScanFilterAgg);
            spec.size_class = Some(SizeClass::Medium);
            spec.scan_records = 100_000;
            spec
        }
        1 => {
            let mut spec = QuerySpec::new(QueryType::MapChain20);
            spec.size_class = Some(SizeClass::Medium);
            spec.map_values = rng.range_usize(MAP_CHAIN_MEDIUM_MIN, MAP_CHAIN_MEDIUM_MAX);
            spec.map_stages = 20;
            spec
        }
        _ => {
            let mut spec = QuerySpec::new(QueryType::MultiEpoch);
            spec.size_class = Some(SizeClass::Medium);
            spec.multi_epochs = 16;
            spec.multi_batch_size = 4_096;
            spec.multi_threshold = (spec.multi_epochs * spec.multi_batch_size) / 2;
            spec
        }
    }
}

fn choose_large_query(rng: &mut Rng) -> QuerySpec {
    if rng.next() & 1 == 0 {
        let mut spec = QuerySpec::new(QueryType::ScanFilterAgg);
        spec.size_class = Some(SizeClass::Large);
        spec.scan_records = 1_000_000;
        spec
    } else {
        let mut spec = QuerySpec::new(QueryType::PageRank);
        spec.size_class = Some(SizeClass::Large);
        spec.pagerank_vertices = 10_000;
        spec.pagerank_edges = 100_000;
        spec.pagerank_iterations = PAGERANK_ITERATIONS;
        spec
    }
}

fn spawn_query_thread(
    shared: Arc<SharedMetrics>,
    runtime: Arc<RuntimeHandle>,
    spec: QuerySpec,
    runtime_idx: usize,
) {
    let name = format!("stress-query-{}-rt{runtime_idx}", spec.label());
    let label = spec.label();
    let shared_for_thread = Arc::clone(&shared);
    let spawn_result = thread::Builder::new().name(name).spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_query(runtime.as_ref(), &spec)
        }));
        let (outcome, detail) = classify_result(&spec, result);
        shared_for_thread.record_completion(label, outcome, detail);
    });

    if let Err(err) = spawn_result {
        shared.record_completion(
            label,
            Outcome::UnexpectedFailure,
            Some(format!("failed to spawn query thread: {err}")),
        );
    }
}

fn classify_result(
    spec: &QuerySpec,
    result: Result<instancy::Result<()>, Box<dyn std::any::Any + Send>>,
) -> (Outcome, Option<String>) {
    match result {
        Ok(Ok(())) => match spec.query_type {
            QueryType::Cancellation => (
                Outcome::UnexpectedFailure,
                Some("cancellation query completed without cancellation".into()),
            ),
            QueryType::FailureInjection => (
                Outcome::UnexpectedFailure,
                Some("failure-injection query completed successfully".into()),
            ),
            _ => (Outcome::Success, None),
        },
        Ok(Err(err)) => match spec.query_type {
            QueryType::FailureInjection => {
                if err.to_string().contains("injected failure") {
                    (Outcome::ExpectedFailure, None)
                } else {
                    (
                        Outcome::UnexpectedFailure,
                        Some(format!("expected injected failure, got: {err}")),
                    )
                }
            }
            QueryType::Cancellation => match err {
                instancy::Error::Cancelled { .. } => (Outcome::Cancelled, None),
                other => {
                    let text = other.to_string();
                    if text.to_ascii_lowercase().contains("cancel") {
                        (Outcome::Cancelled, None)
                    } else {
                        (
                            Outcome::UnexpectedFailure,
                            Some(format!("expected cancellation, got: {text}")),
                        )
                    }
                }
            },
            _ => (Outcome::UnexpectedFailure, Some(err.to_string())),
        },
        Err(payload) => (
            Outcome::UnexpectedFailure,
            Some(format!("query thread panicked: {}", panic_payload_to_string(payload))),
        ),
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn execute_query(runtime: &RuntimeHandle, spec: &QuerySpec) -> instancy::Result<()> {
    match spec.query_type {
        QueryType::ScanFilterAgg => run_scan_filter_agg(runtime, spec.scan_records),
        QueryType::PageRank => run_pagerank(
            runtime,
            spec.pagerank_vertices,
            spec.pagerank_edges,
            spec.pagerank_iterations,
        ),
        QueryType::MapChain20 => run_map_chain(runtime, spec.map_values, spec.map_stages),
        QueryType::MultiEpoch => run_multi_epoch(
            runtime,
            spec.multi_epochs,
            spec.multi_batch_size,
            spec.multi_threshold,
        ),
        QueryType::SmallPipeline => run_small_pipeline(runtime, spec.small_values),
        QueryType::FailureInjection => run_failure_injection(runtime),
        QueryType::Cancellation => run_cancellation(runtime),
    }
}

fn run_scan_filter_agg(runtime: &RuntimeHandle, records: usize) -> instancy::Result<()> {
    let items = generate_lineitems(records);
    let builder = DataflowBuilder::<u64>::new(format!("scan-filter-agg-{records}"));
    builder
        .source("src", vec![(0u64, items)])
        .filter("date_filter", |_t, item| item.ship_date < 11_000)
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
        .for_each("sink", |_t, item| {
            black_box(item);
        });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_pagerank(
    runtime: &RuntimeHandle,
    num_vertices: u64,
    num_edges: usize,
    iterations: usize,
) -> instancy::Result<()> {
    let edges = generate_graph(num_vertices, num_edges);
    let builder = DataflowBuilder::<u64>::new(format!("pagerank-{num_vertices}-{num_edges}"));
    builder
        .source("src", vec![(0u64, edges)])
        .unary_notify::<(u64, f64), _>("pagerank", {
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
        .for_each("sink", |_t, item| {
            black_box(item);
        });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_map_chain(
    runtime: &RuntimeHandle,
    values_count: usize,
    stages: usize,
) -> instancy::Result<()> {
    let values = generate_values(values_count);
    let builder = DataflowBuilder::<u64>::new(format!("map-chain-{values_count}-{stages}"));
    let mut pipe = builder.source("src", vec![(0u64, values)]);
    for idx in 0..stages {
        pipe = pipe.map(format!("map_{idx}"), |_t, value| value + 1);
    }
    pipe.for_each("sink", |_t, item| {
        black_box(item);
    });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_multi_epoch(
    runtime: &RuntimeHandle,
    epochs: u64,
    batch_size: u64,
    threshold: u64,
) -> instancy::Result<()> {
    let batches = generate_multi_epoch_batches(epochs, batch_size);
    let builder = DataflowBuilder::<u64>::new(format!("multi-epoch-{epochs}-{batch_size}"));
    builder
        .source("src", batches)
        .filter("threshold", move |_t, value| *value > threshold)
        .for_each("sink", |_t, item| {
            black_box(item);
        });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_small_pipeline(runtime: &RuntimeHandle, values_count: usize) -> instancy::Result<()> {
    let values = generate_values(values_count);
    let builder = DataflowBuilder::<u64>::new(format!("small-pipeline-{values_count}"));
    builder
        .source("src", vec![(0u64, values)])
        .map("add1", |_t, value| value + 1)
        .map("mul2", |_t, value| value * 2)
        .map("sub1", |_t, value| value - 1)
        .for_each("sink", |_t, item| {
            black_box(item);
        });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_failure_injection(runtime: &RuntimeHandle) -> instancy::Result<()> {
    let builder = DataflowBuilder::<u64>::new("failure-injection");
    builder
        .source("src", vec![(0u64, vec![1u64, 2, 3, 4])])
        .unary_notify::<u64, _>("inject_failure", move |input, _output, ctx| {
            while let Some((time, _data)) = input.next() {
                ctx.notify_at(time);
            }
            if ctx.next_notification().is_some() {
                return Err(instancy::Error::operator("inject_failure", InjectedFailure));
            }
            Ok(())
        })
        .for_each("sink", |_t, item| {
            black_box(item);
        });
    let dataflow = builder.build()?;
    runtime
        .spawn(dataflow, SpawnOptions::default())?
        .join_blocking()
}

fn run_cancellation(runtime: &RuntimeHandle) -> instancy::Result<()> {
    let builder = DataflowBuilder::<u64>::new("cancelled-query");
    let input = builder.input::<u64>("data")?;
    input.map("identity", |_t, value| value).for_each("sink", |_t, item| {
        black_box(item);
    });
    let dataflow = builder.build()?;
    let token = CancellationToken::new();
    let mut handle = runtime.spawn(
        dataflow,
        SpawnOptions::new().cancellation_token(token.clone()),
    )?;
    let _input = handle.take_input::<u64>("data")?;
    token.cancel();
    handle.join_blocking()
}

fn generate_lineitems(count: usize) -> Vec<LineItem> {
    let mut rng = Rng(42);
    (0..count)
        .map(|_| LineItem {
            order_key: rng.next() % 1_500_000,
            part_key: rng.next() % 200_000,
            quantity: (rng.next() % 50 + 1) as i64,
            price: (rng.next() % 100_000 + 100) as i64,
            discount: (rng.next() % 11) as i64,
            tax: (rng.next() % 9) as i64,
            ship_date: 10_000 + (rng.next() % 2_500),
            return_flag: (rng.next() % 3) as u8,
            line_status: (rng.next() % 2) as u8,
        })
        .collect()
}

fn generate_graph(num_vertices: u64, num_edges: usize) -> Vec<Edge> {
    let mut rng = Rng(123);
    (0..num_edges)
        .map(|_| Edge {
            src: rng.next() % num_vertices,
            dst: rng.next() % num_vertices,
        })
        .collect()
}

fn generate_values(count: usize) -> Vec<i64> {
    (0..count).map(|value| value as i64).collect()
}

fn generate_multi_epoch_batches(epochs: u64, batch_size: u64) -> Vec<(u64, Vec<u64>)> {
    (0..epochs)
        .map(|epoch| {
            let base = epoch * batch_size;
            let batch = (0..batch_size).map(|offset| base + offset).collect();
            (epoch, batch)
        })
        .collect()
}

fn compute_pagerank(edges: &[Edge], num_vertices: u64, iterations: usize) -> Vec<(u64, f64)> {
    let n = num_vertices as usize;
    let mut adjacency = vec![Vec::new(); n];
    for edge in edges {
        adjacency[edge.src as usize].push(edge.dst);
    }

    let damping = 0.85;
    let teleport = (1.0 - damping) / n as f64;
    let mut ranks = vec![1.0 / n as f64; n];

    for _ in 0..iterations {
        let dangling_sum: f64 = adjacency
            .iter()
            .enumerate()
            .filter(|(_, outs)| outs.is_empty())
            .map(|(src, _)| ranks[src])
            .sum();
        let dangling_contrib = damping * dangling_sum / n as f64;
        let mut next = vec![teleport + dangling_contrib; n];

        for (src, outs) in adjacency.iter().enumerate() {
            if outs.is_empty() {
                continue;
            }
            let share = damping * ranks[src] / outs.len() as f64;
            for &dst in outs {
                next[dst as usize] += share;
            }
        }
        ranks = next;
    }

    ranks
        .into_iter()
        .enumerate()
        .map(|(vertex, rank)| (vertex as u64, rank))
        .collect()
}

fn format_hms(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn print_final_report(
    shared: &SharedMetrics,
    start_snapshot: &SystemSnapshot,
    final_snapshot: &SystemSnapshot,
    elapsed_secs: u64,
) {
    let snapshots = shared.snapshots.lock().unwrap().clone();
    let per_type = shared.per_type.lock().unwrap().clone();
    let unexpected_messages = shared.unexpected_messages.lock().unwrap().clone();

    let observed_peak_rss = snapshots
        .iter()
        .map(|sample| sample.snapshot.working_set_mb)
        .fold(start_snapshot.working_set_mb, f64::max);

    let baseline_sample = snapshots
        .iter()
        .find(|sample| sample.elapsed_secs >= LEAK_BASELINE_SECS)
        .or_else(|| snapshots.first());
    let final_window_start = elapsed_secs.saturating_sub(LEAK_WINDOW_SECS);
    let final_window: Vec<_> = snapshots
        .iter()
        .filter(|sample| sample.elapsed_secs >= final_window_start)
        .collect();
    let final_window_avg = if final_window.is_empty() {
        final_snapshot.working_set_mb
    } else {
        final_window
            .iter()
            .map(|sample| sample.snapshot.working_set_mb)
            .sum::<f64>()
            / final_window.len() as f64
    };
    let baseline_rss = baseline_sample
        .map(|sample| sample.snapshot.working_set_mb)
        .unwrap_or(start_snapshot.working_set_mb);
    let leak_flag = elapsed_secs >= LEAK_BASELINE_SECS + LEAK_WINDOW_SECS
        && baseline_rss > 0.0
        && final_window_avg > baseline_rss * 1.5;

    let total_cpu_ms = (final_snapshot.cpu_user_ms + final_snapshot.cpu_kernel_ms)
        - (start_snapshot.cpu_user_ms + start_snapshot.cpu_kernel_ms);
    let unexpected = shared.unexpected_failures.load(Ordering::Relaxed);
    let verdict = if unexpected == 0 && !leak_flag { "PASS" } else { "FAIL" };

    println!("\n=== final report ===");
    println!(
        "elapsed={} total_queries={} completed={} success={} expected_failure={} unexpected_failure={} cancelled={} in_flight={}",
        format_hms(elapsed_secs),
        shared.submitted.load(Ordering::Relaxed),
        shared.completed.load(Ordering::Relaxed),
        shared.success.load(Ordering::Relaxed),
        shared.expected_failures.load(Ordering::Relaxed),
        unexpected,
        shared.cancelled.load(Ordering::Relaxed),
        shared.in_flight.load(Ordering::Relaxed),
    );
    println!(
        "memory_trend_mb start={:.1} peak={:.1} end={:.1} os_peak={:.1}",
        start_snapshot.working_set_mb,
        observed_peak_rss,
        final_snapshot.working_set_mb,
        final_snapshot.peak_working_set_mb,
    );
    println!(
        "cpu_total_ms user={:.1} kernel={:.1} total={:.1}",
        final_snapshot.cpu_user_ms - start_snapshot.cpu_user_ms,
        final_snapshot.cpu_kernel_ms - start_snapshot.cpu_kernel_ms,
        total_cpu_ms,
    );
    println!(
        "leak_check baseline_10m_mb={:.1} final_10m_avg_mb={:.1} growth={:.1}% status={}",
        baseline_rss,
        final_window_avg,
        if baseline_rss > 0.0 {
            ((final_window_avg / baseline_rss) - 1.0) * 100.0
        } else {
            0.0
        },
        if leak_flag { "POTENTIAL_LEAK" } else { "OK" },
    );
    println!("per-type breakdown:");
    for name in REPORT_NAMES {
        let stats = per_type.get(name).copied().unwrap_or_default();
        println!(
            "  {:<17} submitted={:<8} completed={:<8} success={:<8} expected_failure={:<6} unexpected_failure={:<6} cancelled={:<6}",
            name,
            stats.submitted,
            stats.completed,
            stats.success,
            stats.expected_failures,
            stats.unexpected_failures,
            stats.cancelled,
        );
    }
    if !unexpected_messages.is_empty() {
        println!("unexpected failure samples:");
        for message in unexpected_messages.iter().take(10) {
            println!("  - {message}");
        }
    }
    if let (Some(first), Some(last)) = (snapshots.first(), snapshots.last()) {
        println!(
            "snapshot_range start_t={} end_t={} start_submitted={} end_submitted={} start_completed={} end_completed={} start_in_flight={} end_in_flight={}",
            format_hms(first.elapsed_secs),
            format_hms(last.elapsed_secs),
            first.submitted,
            last.submitted,
            first.completed,
            last.completed,
            first.in_flight,
            last.in_flight,
        );
    }
    println!("verdict={verdict}");
}
