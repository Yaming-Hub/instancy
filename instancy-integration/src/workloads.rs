//! Inputs for the Shared-transport scenarios, and the exact output each
//! worker must produce from them.
//!
//! Every reference groups records by the worker that holds them after the
//! dataflow's last exchange. Workers are numbered globally in topology node
//! order, and the builders in [`crate::dataflows`] route as follows:
//!
//! - `exchange_by_hash(f)` sends a record to worker `f(record) % total_workers`;
//! - `exchange(f)` sends it to `instancy::hash::stable_hash(&f(record)) % total_workers`;
//! - `gather` is `exchange_by_hash(|_| 0)`: every record goes to worker 0
//!   (`Pipe::gather` in `instancy/src/dataflow/dataflow_builder.rs`);
//! - `exchange_by_hash_to(n, f)` routes like `exchange_by_hash(f)` and gives
//!   the stage after it `n` workers. A cluster runs every stage on all of its
//!   workers and refuses any other `n` (`validate_stage_parallelism` in
//!   `instancy/src/runtime.rs`), so `n` is the total;
//! - an `iterate` loop's edges in and out are worker-local, so a record
//!   leaves the loop on the worker its last round ran on.

use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::protocol::DataflowType;

/// Which dataflow a workload drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    /// [`DataflowType::ExchangeRoundTrip`]: records keyed by `u64`,
    /// repartitioned by `key % total_workers`.
    ExchangeRoundTrip,
    /// [`DataflowType::MultiEpochExchange`]: per-epoch, per-key sums on the
    /// worker `key % total_workers`.
    MultiEpochExchange,
    /// [`DataflowType::DistributedWordCount`]: per-epoch word counts on the
    /// worker `stable_hash(word) % total_workers`.
    WordCount,
    /// [`DataflowType::DistributedJoin`]: per-epoch join of two inputs on the
    /// worker `key % total_workers`.
    Join,
    /// [`DataflowType::IterativeExchange`]: each round of the loop sends a
    /// value to the worker `value % total_workers`, which doubles it; once
    /// the double reaches `threshold` it leaves the loop there, else it goes
    /// round again. Its inputs are positive and below `threshold`, which
    /// must be in `1..=i64::MAX / 2` (so doubling never overflows).
    IterativeExchange { threshold: i64 },
    /// [`DataflowType::StagedFanOutFanIn`]: each value is doubled, fanned
    /// out to `fan_out_parallelism` workers, incremented and gathered on
    /// worker 0. In a cluster `fan_out_parallelism` must be the total number
    /// of workers (see the module docs).
    StagedFanOutFanIn { fan_out_parallelism: usize },
}

impl WorkloadKind {
    /// Every kind, in a fixed order, for a dataflow of `total_workers`
    /// workers in a cluster: IterativeExchange with a threshold of 1,000,
    /// and StagedFanOutFanIn fanning out to every worker.
    pub fn all(total_workers: usize) -> [WorkloadKind; 6] {
        [
            WorkloadKind::ExchangeRoundTrip,
            WorkloadKind::MultiEpochExchange,
            WorkloadKind::WordCount,
            WorkloadKind::Join,
            WorkloadKind::IterativeExchange { threshold: 1_000 },
            WorkloadKind::StagedFanOutFanIn {
                fan_out_parallelism: total_workers,
            },
        ]
    }

    /// The dataflow this workload drives.
    pub fn dataflow_type(self) -> DataflowType {
        match self {
            WorkloadKind::ExchangeRoundTrip => DataflowType::ExchangeRoundTrip,
            WorkloadKind::MultiEpochExchange => DataflowType::MultiEpochExchange,
            WorkloadKind::WordCount => DataflowType::DistributedWordCount,
            WorkloadKind::Join => DataflowType::DistributedJoin,
            WorkloadKind::IterativeExchange { threshold } => {
                DataflowType::IterativeExchange { threshold }
            }
            WorkloadKind::StagedFanOutFanIn {
                fan_out_parallelism,
            } => DataflowType::StagedFanOutFanIn {
                fan_out_parallelism,
            },
        }
    }

    /// The kind's name, without its parameters.
    pub fn name(self) -> &'static str {
        match self {
            WorkloadKind::ExchangeRoundTrip => "ExchangeRoundTrip",
            WorkloadKind::MultiEpochExchange => "MultiEpochExchange",
            WorkloadKind::WordCount => "WordCount",
            WorkloadKind::Join => "Join",
            WorkloadKind::IterativeExchange { .. } => "IterativeExchange",
            WorkloadKind::StagedFanOutFanIn { .. } => "StagedFanOutFanIn",
        }
    }

    /// A number of its own for each kind, which seeds its inputs.
    fn number(self) -> u64 {
        match self {
            WorkloadKind::ExchangeRoundTrip => 0,
            WorkloadKind::MultiEpochExchange => 1,
            WorkloadKind::WordCount => 2,
            WorkloadKind::Join => 3,
            WorkloadKind::IterativeExchange { .. } => 4,
            WorkloadKind::StagedFanOutFanIn { .. } => 5,
        }
    }
}

/// One batch of input for one worker's input port.
#[derive(Debug, Clone)]
pub struct Feed {
    pub node: String,
    /// The worker's index on its node.
    pub worker_idx: usize,
    pub port: String,
    pub timestamp: u64,
    /// The bincode-serialized `Vec` of records.
    pub data: Vec<u8>,
}

/// A dataflow's whole input, and the reference its output is checked against.
#[derive(Debug, Clone)]
pub struct Workload {
    pub kind: WorkloadKind,
    pub dataflow_type: DataflowType,
    pub feeds: Vec<Feed>,
    /// Workers over all nodes the workload was made for.
    total_workers: usize,
}

/// Records in each feed made by [`Workload::new`].
pub const RECORDS_PER_FEED: usize = 6;

/// Keys of the multi-epoch sums and of the join: few enough to repeat.
const SMALL_KEYS: u64 = 12;

const WORDS: [&str; 12] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima",
];

/// A small deterministic generator (SplitMix64): the same seed gives the
/// same numbers on every platform.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`. Panics if `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// A value in `-500..500`.
    fn value(&mut self) -> i64 {
        self.below(1_000) as i64 - 500
    }
}

impl Workload {
    /// The input for a dataflow of `kind` over `nodes`, each given with its
    /// number of workers, in topology order, with [`RECORDS_PER_FEED`]
    /// records in each feed: [`Workload::new_sized`].
    pub fn new(kind: WorkloadKind, nodes: &[(&str, usize)], epochs: u64, seed: u64) -> Workload {
        Self::new_sized(kind, nodes, epochs, RECORDS_PER_FEED, seed)
    }

    /// The input for a dataflow of `kind` over `nodes`, each given with its
    /// number of workers, in topology order. It spans `epochs` timestamps,
    /// starting at 0, has `records_per_feed` records in each feed (a third
    /// as many sentences, rounded up, for WordCount), and the same `seed`
    /// gives the same input.
    ///
    /// Join feeds each of its two inputs from one worker per epoch (a
    /// different one each epoch): the join operator matches only the batches
    /// it holds at once, so each worker must get at most one batch per input
    /// and epoch for its output to be exact. The other kinds feed every
    /// worker in every epoch.
    ///
    /// # Panics
    ///
    /// If `nodes` has no workers, or an IterativeExchange threshold is
    /// outside `1..=i64::MAX / 2`.
    pub fn new_sized(
        kind: WorkloadKind,
        nodes: &[(&str, usize)],
        epochs: u64,
        records_per_feed: usize,
        seed: u64,
    ) -> Workload {
        let workers: Vec<(&str, usize)> = nodes
            .iter()
            .flat_map(|&(node, count)| (0..count).map(move |local| (node, local)))
            .collect();
        assert!(!workers.is_empty(), "a workload needs at least one worker");
        if let WorkloadKind::IterativeExchange { threshold } = kind {
            assert!(
                (1..=i64::MAX / 2).contains(&threshold),
                "an IterativeExchange threshold must be in 1..=i64::MAX / 2, not {threshold}"
            );
        }
        let mut rng = SplitMix64(seed ^ kind.number().wrapping_mul(0x51_7CC1_B727_220A));
        let mut feeds = Vec::new();
        let mut feed = |(node, worker_idx): (&str, usize), port: &str, timestamp, data| {
            feeds.push(Feed {
                node: node.to_string(),
                worker_idx,
                port: port.to_string(),
                timestamp,
                data,
            });
        };
        for t in 0..epochs {
            match kind {
                WorkloadKind::ExchangeRoundTrip => {
                    for &(node, local) in &workers {
                        let records: Vec<(u64, String)> = (0..records_per_feed)
                            .map(|i| (rng.below(10_000), format!("{node}/{local}/{t}/{i}")))
                            .collect();
                        feed((node, local), "data", t, encode(&records));
                    }
                }
                WorkloadKind::MultiEpochExchange => {
                    for &worker in &workers {
                        let records: Vec<(u64, i64)> = (0..records_per_feed)
                            .map(|_| (rng.below(SMALL_KEYS), rng.value()))
                            .collect();
                        feed(worker, "data", t, encode(&records));
                    }
                }
                WorkloadKind::WordCount => {
                    for &worker in &workers {
                        let sentences: Vec<String> = (0..records_per_feed.div_ceil(3))
                            .map(|_| {
                                let words = 3 + rng.below(4);
                                (0..words)
                                    .map(|_| WORDS[rng.below(WORDS.len() as u64) as usize])
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            })
                            .collect();
                        feed(worker, "sentences", t, encode(&sentences));
                    }
                }
                WorkloadKind::Join => {
                    let left_feeder = workers[(2 * t as usize) % workers.len()];
                    let right_feeder = workers[(2 * t as usize + 1) % workers.len()];
                    let left: Vec<(u64, String)> = (0..records_per_feed)
                        .map(|i| (rng.below(SMALL_KEYS), format!("left/{t}/{i}")))
                        .collect();
                    let right: Vec<(u64, i64)> = (0..records_per_feed)
                        .map(|_| (rng.below(SMALL_KEYS), rng.value()))
                        .collect();
                    feed(left_feeder, "left", t, encode(&left));
                    feed(right_feeder, "right", t, encode(&right));
                }
                WorkloadKind::IterativeExchange { threshold } => {
                    // Positive, so doubling reaches the threshold, and below
                    // it, so small values go round many times (ten for 1
                    // with a threshold of 1,000).
                    let below = (threshold - 1).max(1) as u64;
                    for &worker in &workers {
                        let records: Vec<i64> = (0..records_per_feed)
                            .map(|_| 1 + rng.below(below) as i64)
                            .collect();
                        feed(worker, "data", t, encode(&records));
                    }
                }
                WorkloadKind::StagedFanOutFanIn { .. } => {
                    for &worker in &workers {
                        let records: Vec<i64> =
                            (0..records_per_feed).map(|_| rng.value()).collect();
                        feed(worker, "data", t, encode(&records));
                    }
                }
            }
        }
        Workload {
            kind,
            dataflow_type: kind.dataflow_type(),
            feeds,
            total_workers: workers.len(),
        }
    }

    /// Check what one worker output: `(timestamp, bincode batch)` pairs from
    /// its output port. Passes only if, at every timestamp, the worker output
    /// exactly the records the reference puts on it (as a multiset), nothing
    /// more and nothing less.
    pub fn check_worker(
        &self,
        global_worker: usize,
        total_workers: usize,
        output: &[(u64, Vec<u8>)],
    ) -> Result<(), String> {
        if total_workers != self.total_workers {
            return Err(format!(
                "the dataflow has {total_workers} workers, but its workload was made for {}",
                self.total_workers
            ));
        }
        if global_worker >= total_workers {
            return Err(format!(
                "worker {global_worker} is outside the dataflow's {total_workers} workers"
            ));
        }
        let owner = |key: u64| (key % total_workers as u64) as usize == global_worker;
        match self.kind {
            WorkloadKind::ExchangeRoundTrip => {
                let expected = group(
                    self.records::<(u64, String)>("data")
                        .into_iter()
                        .filter(|(_, (key, _))| owner(*key)),
                );
                compare(&expected, &decode::<(u64, String)>(output)?)
            }
            WorkloadKind::MultiEpochExchange => {
                let mut sums: BTreeMap<u64, BTreeMap<u64, i64>> = BTreeMap::new();
                for (t, (key, value)) in self.records::<(u64, i64)>("data") {
                    if owner(key) {
                        *sums.entry(t).or_default().entry(key).or_default() += value;
                    }
                }
                let expected = sums
                    .into_iter()
                    .map(|(t, sums)| (t, sums.into_iter().collect()))
                    .collect();
                compare(&expected, &decode::<(u64, i64)>(output)?)
            }
            WorkloadKind::WordCount => {
                let mut counts: BTreeMap<u64, BTreeMap<String, u64>> = BTreeMap::new();
                for (t, sentence) in self.records::<String>("sentences") {
                    for word in sentence.split_whitespace().map(str::to_lowercase) {
                        if owner(instancy::hash::stable_hash(&word)) {
                            *counts.entry(t).or_default().entry(word).or_default() += 1;
                        }
                    }
                }
                let expected = counts
                    .into_iter()
                    .map(|(t, counts)| (t, counts.into_iter().collect()))
                    .collect();
                compare(&expected, &decode::<(String, u64)>(output)?)
            }
            WorkloadKind::Join => {
                let rights = self.records::<(u64, i64)>("right");
                let mut joined = Vec::new();
                for (t, (key, left)) in self.records::<(u64, String)>("left") {
                    if !owner(key) {
                        continue;
                    }
                    for (right_t, (right_key, right)) in &rights {
                        if *right_t == t && *right_key == key {
                            joined.push((t, (key, left.clone(), *right)));
                        }
                    }
                }
                compare(&group(joined), &decode::<(u64, String, i64)>(output)?)
            }
            WorkloadKind::IterativeExchange { threshold } => {
                let mut left_loop = Vec::new();
                for (t, value) in self.records::<i64>("data") {
                    let (worker, result) = leave_the_loop(value, threshold, total_workers);
                    if worker == global_worker {
                        left_loop.push((t, result));
                    }
                }
                compare(&group(left_loop), &decode::<i64>(output)?)
            }
            WorkloadKind::StagedFanOutFanIn { .. } => {
                // `gather` puts every record on worker 0.
                let expected = if global_worker == 0 {
                    group(
                        self.records::<i64>("data")
                            .into_iter()
                            .map(|(t, value)| (t, value * 2 + 1)),
                    )
                } else {
                    BTreeMap::new()
                };
                compare(&expected, &decode::<i64>(output)?)
            }
        }
    }

    /// Every record fed to `port`, with its timestamp.
    fn records<T: DeserializeOwned>(&self, port: &str) -> Vec<(u64, T)> {
        self.feeds
            .iter()
            .filter(|feed| feed.port == port)
            .flat_map(|feed| {
                let batch: Vec<T> =
                    bincode::deserialize(&feed.data).expect("a workload's own feed doesn't decode");
                batch
                    .into_iter()
                    .map(move |record| (feed.timestamp, record))
            })
            .collect()
    }
}

fn encode<T: Serialize>(records: &[T]) -> Vec<u8> {
    bincode::serialize(records).expect("failed to encode a feed")
}

/// The worker on which `value` leaves IterativeExchange's loop, and what it
/// is then. Each round sends the value to the worker `value % total_workers`
/// (`exchange_by_hash`), which doubles it; it leaves there once the double
/// reaches `threshold`, and goes round again otherwise.
///
/// # Panics
///
/// If `value` is not positive: it would never reach the threshold.
fn leave_the_loop(mut value: i64, threshold: i64, total_workers: usize) -> (usize, i64) {
    assert!(
        value > 0,
        "IterativeExchange input {value} never leaves the loop"
    );
    loop {
        let worker = (value as u64 % total_workers as u64) as usize;
        value *= 2;
        if value >= threshold {
            return (worker, value);
        }
    }
}

/// Records by timestamp, each timestamp's sorted.
fn group<T: Ord>(records: impl IntoIterator<Item = (u64, T)>) -> BTreeMap<u64, Vec<T>> {
    let mut grouped: BTreeMap<u64, Vec<T>> = BTreeMap::new();
    for (t, record) in records {
        grouped.entry(t).or_default().push(record);
    }
    for batch in grouped.values_mut() {
        batch.sort();
    }
    grouped
}

/// Decode a worker's output batches into records by timestamp.
fn decode<T: DeserializeOwned + Ord>(
    output: &[(u64, Vec<u8>)],
) -> Result<BTreeMap<u64, Vec<T>>, String> {
    let mut records = Vec::new();
    for (t, bytes) in output {
        let batch: Vec<T> = bincode::deserialize(bytes)
            .map_err(|e| format!("an output batch at time {t} doesn't decode: {e}"))?;
        records.extend(batch.into_iter().map(|record| (*t, record)));
    }
    Ok(group(records))
}

/// The most records of each kind a mismatch report lists per timestamp.
const SHOWN: usize = 5;

/// Compare sorted records by timestamp, as multisets.
fn compare<T: Ord + Debug>(
    expected: &BTreeMap<u64, Vec<T>>,
    actual: &BTreeMap<u64, Vec<T>>,
) -> Result<(), String> {
    if expected == actual {
        return Ok(());
    }
    let count = |records: &BTreeMap<u64, Vec<T>>| records.values().map(Vec::len).sum::<usize>();
    let mut report = format!(
        "expected {} records, got {}",
        count(expected),
        count(actual)
    );
    let empty = Vec::new();
    let times: std::collections::BTreeSet<u64> =
        expected.keys().chain(actual.keys()).copied().collect();
    for t in times {
        let want = expected.get(&t).unwrap_or(&empty);
        let got = actual.get(&t).unwrap_or(&empty);
        let (missing, unexpected) = difference(want, got);
        if missing.is_empty() && unexpected.is_empty() {
            continue;
        }
        report.push_str(&format!(
            "; at time {t}: {} missing {:?}, {} unexpected {:?}",
            missing.len(),
            &missing[..missing.len().min(SHOWN)],
            unexpected.len(),
            &unexpected[..unexpected.len().min(SHOWN)],
        ));
    }
    Err(report)
}

/// What only `want` holds and what only `got` holds, of two sorted lists.
fn difference<'a, T: Ord>(want: &'a [T], got: &'a [T]) -> (Vec<&'a T>, Vec<&'a T>) {
    let (mut missing, mut unexpected) = (Vec::new(), Vec::new());
    let (mut w, mut g) = (want.iter().peekable(), got.iter().peekable());
    loop {
        match (w.peek(), g.peek()) {
            (Some(a), Some(b)) => match a.cmp(b) {
                std::cmp::Ordering::Equal => {
                    w.next();
                    g.next();
                }
                std::cmp::Ordering::Less => missing.extend(w.next()),
                std::cmp::Ordering::Greater => unexpected.extend(g.next()),
            },
            (Some(_), None) => missing.extend(w.next()),
            (None, Some(_)) => unexpected.extend(g.next()),
            (None, None) => return (missing, unexpected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODES: [(&str, usize); 2] = [("a", 2), ("b", 1)];

    /// ExchangeRoundTrip's records for `worker`, as the node reports them:
    /// one batch per timestamp.
    fn exchange_output(workload: &Workload, worker: usize) -> Vec<(u64, Vec<u8>)> {
        let total = workload.total_workers as u64;
        group(
            workload
                .records::<(u64, String)>("data")
                .into_iter()
                .filter(|(_, (key, _))| key % total == worker as u64),
        )
        .into_iter()
        .map(|(t, batch)| (t, encode(&batch)))
        .collect()
    }

    #[test]
    fn the_same_seed_gives_the_same_input() {
        let data = |seed| {
            Workload::new(WorkloadKind::WordCount, &NODES, 3, seed)
                .feeds
                .into_iter()
                .map(|feed| feed.data)
                .collect::<Vec<_>>()
        };
        assert_eq!(data(7), data(7));
        assert_ne!(data(7), data(8));
    }

    #[test]
    fn every_record_belongs_to_exactly_one_worker() {
        let workload = Workload::new(WorkloadKind::ExchangeRoundTrip, &NODES, 3, 1);
        let mut checked = 0;
        for worker in 0..3 {
            let output = exchange_output(&workload, worker);
            workload.check_worker(worker, 3, &output).unwrap();
            checked += decode::<(u64, String)>(&output)
                .unwrap()
                .values()
                .map(Vec::len)
                .sum::<usize>();
        }
        assert_eq!(checked, workload.records::<(u64, String)>("data").len());
    }

    #[test]
    fn a_lost_or_duplicated_record_fails_the_check() {
        let workload = Workload::new(WorkloadKind::ExchangeRoundTrip, &NODES, 3, 1);
        let output = exchange_output(&workload, 0);
        let (t, bytes) = output.first().expect("worker 0 gets records").clone();
        let batch: Vec<(u64, String)> = bincode::deserialize(&bytes).unwrap();

        let mut lost = output.clone();
        lost[0] = (t, encode(&batch[1..]));
        let mut duplicated = output.clone();
        duplicated.push((t, encode(&batch[..1])));
        let mut misplaced = output.clone();
        misplaced.push(exchange_output(&workload, 1)[0].clone());
        for wrong in [lost, duplicated, misplaced] {
            assert!(workload.check_worker(0, 3, &wrong).is_err());
        }
        assert!(workload.check_worker(0, 4, &output).is_err());
    }

    #[test]
    fn join_feeds_each_input_from_one_worker_per_epoch() {
        let workload = Workload::new(WorkloadKind::Join, &NODES, 4, 1);
        for t in 0..4 {
            for port in ["left", "right"] {
                let feeds = workload
                    .feeds
                    .iter()
                    .filter(|feed| feed.timestamp == t && feed.port == port)
                    .count();
                assert_eq!(feeds, 1, "{port} feeds at time {t}");
            }
        }
    }

    /// A workload of `kind` over three workers whose input is `feeds`:
    /// `(timestamp, worker, values)`, the workers numbered as in [`NODES`].
    fn hand_made(kind: WorkloadKind, feeds: &[(u64, usize, &[i64])]) -> Workload {
        let workers = [("a", 0), ("a", 1), ("b", 0)];
        Workload {
            kind,
            dataflow_type: kind.dataflow_type(),
            feeds: feeds
                .iter()
                .map(|&(timestamp, worker, values)| Feed {
                    node: workers[worker].0.to_string(),
                    worker_idx: workers[worker].1,
                    port: "data".to_string(),
                    timestamp,
                    data: encode(values),
                })
                .collect(),
            total_workers: workers.len(),
        }
    }

    /// Output batches as a node reports them.
    fn batches(batches: &[(u64, &[i64])]) -> Vec<(u64, Vec<u8>)> {
        batches
            .iter()
            .map(|&(t, values)| (t, encode(values)))
            .collect()
    }

    /// Worked by hand for three workers and a threshold of 10, a value at a
    /// time: each round sends it to the worker `value % 3`, which doubles it,
    /// and it leaves on that worker once the double reaches 10.
    ///
    /// - 1: worker 1 → 2, worker 2 → 4, worker 1 → 8, worker 2 → 16: leaves
    ///   on worker 2;
    /// - 3: worker 0 → 6, worker 0 → 12: leaves on worker 0;
    /// - 7: worker 1 → 14: leaves on worker 1;
    /// - 5, at time 1: worker 2 → 10: leaves on worker 2.
    #[test]
    fn iterative_exchange_reference_follows_each_value_round_the_loop() {
        let kind = WorkloadKind::IterativeExchange { threshold: 10 };
        let workload = hand_made(kind, &[(0, 0, &[1, 3]), (0, 2, &[7]), (1, 1, &[5])]);
        let exact = [
            batches(&[(0, &[12])]),
            batches(&[(0, &[14])]),
            batches(&[(0, &[16]), (1, &[10])]),
        ];
        for (worker, output) in exact.iter().enumerate() {
            workload
                .check_worker(worker, 3, output)
                .unwrap_or_else(|e| panic!("worker {worker}: {e}"));
        }
        // The same records in other batches are still exact.
        workload
            .check_worker(2, 3, &batches(&[(1, &[10]), (0, &[]), (0, &[16])]))
            .unwrap();

        let wrong = [
            (2, batches(&[(0, &[16])]), "lost the value at time 1"),
            (2, batches(&[(0, &[16]), (0, &[10])]), "the wrong timestamp"),
            (0, batches(&[(0, &[6])]), "left the loop a round early"),
            (0, batches(&[(0, &[24])]), "went round once too often"),
            (
                1,
                batches(&[(0, &[14]), (0, &[16])]),
                "left on the wrong worker",
            ),
            (0, batches(&[(0, &[12, 12])]), "duplicated"),
        ];
        for (worker, output, what) in wrong {
            assert!(
                workload.check_worker(worker, 3, &output).is_err(),
                "worker {worker}: {what} passed the check"
            );
        }
    }

    /// Every value is doubled and incremented, and every result is gathered
    /// on worker 0; the other workers output nothing.
    #[test]
    fn staged_fan_out_fan_in_reference_gathers_everything_on_worker_0() {
        let kind = WorkloadKind::StagedFanOutFanIn {
            fan_out_parallelism: 3,
        };
        let workload = hand_made(kind, &[(0, 0, &[-3, 0]), (0, 2, &[5]), (1, 1, &[2])]);
        workload
            .check_worker(0, 3, &batches(&[(0, &[11]), (0, &[-5, 1]), (1, &[5])]))
            .unwrap();
        for worker in [1, 2] {
            workload.check_worker(worker, 3, &[]).unwrap();
        }

        let wrong = [
            (0, batches(&[(0, &[-5, 1, 11])]), "lost the value at time 1"),
            (0, batches(&[(0, &[-5, 1, 11]), (1, &[5, 5])]), "duplicated"),
            (
                0,
                batches(&[(0, &[-6, 0, 10]), (1, &[4])]),
                "skipped a stage",
            ),
            (1, batches(&[(1, &[5])]), "not gathered on worker 0"),
        ];
        for (worker, output, what) in wrong {
            assert!(
                workload.check_worker(worker, 3, &output).is_err(),
                "worker {worker}: {what} passed the check"
            );
        }
    }

    #[test]
    fn every_kind_feeds_the_records_asked_for() {
        for kind in WorkloadKind::all(3) {
            let workload = Workload::new_sized(kind, &NODES, 2, 300, 1);
            for feed in &workload.feeds {
                let records = match kind {
                    WorkloadKind::ExchangeRoundTrip => {
                        bincode::deserialize::<Vec<(u64, String)>>(&feed.data).map(|r| r.len())
                    }
                    WorkloadKind::MultiEpochExchange => {
                        bincode::deserialize::<Vec<(u64, i64)>>(&feed.data).map(|r| r.len())
                    }
                    WorkloadKind::WordCount => {
                        bincode::deserialize::<Vec<String>>(&feed.data).map(|r| r.len() * 3)
                    }
                    WorkloadKind::Join if feed.port == "left" => {
                        bincode::deserialize::<Vec<(u64, String)>>(&feed.data).map(|r| r.len())
                    }
                    WorkloadKind::Join => {
                        bincode::deserialize::<Vec<(u64, i64)>>(&feed.data).map(|r| r.len())
                    }
                    WorkloadKind::IterativeExchange { .. }
                    | WorkloadKind::StagedFanOutFanIn { .. } => {
                        bincode::deserialize::<Vec<i64>>(&feed.data).map(|r| r.len())
                    }
                };
                assert_eq!(
                    records.unwrap(),
                    300,
                    "{kind:?}: {} at time {}",
                    feed.port,
                    feed.timestamp
                );
            }
        }
    }

    /// Doubling a positive value below the threshold reaches it without
    /// overflowing, so every input leaves the loop.
    #[test]
    fn iterative_exchange_inputs_are_positive_and_below_the_threshold() {
        for threshold in [2, 10, 1_000] {
            let kind = WorkloadKind::IterativeExchange { threshold };
            let workload = Workload::new_sized(kind, &NODES, 3, 100, 5);
            let values = workload.records::<i64>("data");
            assert_eq!(values.len(), 3 * 3 * 100);
            for (t, value) in values {
                assert!(
                    (1..threshold).contains(&value),
                    "threshold {threshold}: input {value} at time {t}"
                );
            }
        }
    }

    #[test]
    fn every_kind_fits_a_cluster_of_its_workers() {
        let kinds = WorkloadKind::all(8);
        let names: std::collections::BTreeSet<&str> = kinds.iter().map(|k| k.name()).collect();
        assert_eq!(names.len(), kinds.len(), "{names:?}");
        assert!(kinds.contains(&WorkloadKind::StagedFanOutFanIn {
            fan_out_parallelism: 8
        }));
        assert_eq!(
            WorkloadKind::StagedFanOutFanIn {
                fan_out_parallelism: 8
            }
            .dataflow_type(),
            DataflowType::StagedFanOutFanIn {
                fan_out_parallelism: 8
            }
        );
    }
}
