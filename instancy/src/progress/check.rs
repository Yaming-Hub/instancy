//! Debug invariant checker for progress tracking.
//!
//! This module detects progress-accounting bugs; it never changes how a
//! dataflow runs. It is compiled only into builds with `debug_assertions`
//! (every `cargo test` build) or with the `progress-check` feature. Release
//! builds without the feature contain none of it: every hook in the runtime
//! is behind the same `cfg`.
//!
//! # What it checks
//!
//! | Check | Statement |
//! |---|---|
//! | [`Check::V1`] | Every batch a worker consumes at input `(op, port)` and time `t` is at or after that worker's input frontier: data never arrives behind the frontier. |
//! | [`Check::V4`] | A worker never force-closes, or reports completion, while work is outstanding; an input whose every channel is exhausted gets an empty frontier within a bounded number of propagations. |
//! | [`Check::V5`] | Report only: the non-zero counts left in a worker's view when it finishes. A negative count means a missing `+n`, a positive one a leak. |
//! | [`Check::V6`] | Wire rules: split progress batches are positives-first, and nothing follows a progress end-of-stream. (Frames carry no per-sender sequence number to check order with.) |
//! | [`Check::V7`] | The oracle. It measures where work physically is, independently of the accounting under test, and asserts that no worker's view claims a location is done while work is still there. At successful completion every store must be empty. |
//!
//! # The oracle (V7)
//!
//! The oracle never reads progress reporters, the tracker's counts or any
//! other ledger. It counts work from the stores themselves:
//!
//! - **channels**: every channel is wrapped where it is materialized
//!   ([`DataflowExecutor::materialize`](crate::dataflow::DataflowExecutor),
//!   the staged executor and the staged boundary wiring). An envelope counts
//!   from the moment a producer first offers it (so the inside of composite
//!   pushers, exchange buckets, network queues and receive bridges is all
//!   covered) until a consumer pulls it. Broadcast edges count one copy per
//!   target worker;
//! - **operator stores**: `InputHandle` buffers, `pending_output` retry
//!   queues, channel-sink egress (`pending_send`) and `unary_async` input,
//!   task and result queues, each measured from its own contents;
//! - **capabilities**: live [`Capability`](crate::progress::capability::Capability)
//!   objects and the capability an input source holds.
//!
//! Every worker publishes its frontiers after each propagation. The oracle
//! checks each publication against all work currently counted, and checks
//! every newly counted unit of work against every worker's last publication.
//! Work at an operator's input is checked against that input's frontier; work
//! an operator holds for its output (a retry queue, an async task) is checked
//! against the frontier of every input that output feeds; a capability is
//! checked against the frontier of the output it is for.
//!
//! All nodes of a cluster dataflow running in one process (as in every test)
//! share one oracle, keyed by [`DataflowId`], so work in network transit is
//! covered too.
//!
//! # Modes
//!
//! The environment variable `INSTANCY_PROGRESS_CHECK` selects what happens
//! on a violation:
//!
//! - unset or `report`: violations are recorded in a process-wide log (see
//!   [`violations_since`]) and emitted as `tracing` warnings;
//! - `verbose`: as `report`, and each recorded violation is also printed to
//!   standard error;
//! - `panic`: as `report`, then the violating thread panics (V5 reports never
//!   panic);
//! - `off`: no checking.
//!
//! [`set_mode`] overrides the variable for the whole process.
//!
//! Known limits: the `OutputHandle` buffer and data inside a user closure are
//! transient within one activation and are not measured; a capability whose
//! reporter is not attached to a tracker (inside `iterate`, see audit H11) is
//! invisible to the capability count.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::id::DataflowId;
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// Public API: checks, violations, modes, the log
// ---------------------------------------------------------------------------

/// The invariant a [`Violation`] breaks (see the [module docs](self)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Check {
    /// Late data: a batch consumed behind the consuming worker's frontier.
    V1,
    /// Termination: completion or force-close with work outstanding, or an
    /// exhausted input whose frontier never empties.
    V4,
    /// Residual report: non-zero counts in a finished worker's view.
    V5,
    /// Wire rules for progress traffic.
    V6,
    /// Oracle: a frontier ahead of work that is physically present, or work
    /// left in a store at successful completion.
    V7,
}

impl Check {
    /// Whether this check only reports (never fails a run, even in
    /// [`Mode::Panic`]).
    pub fn is_report(self) -> bool {
        matches!(self, Check::V5)
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Check::V1 => "V1",
            Check::V4 => "V4",
            Check::V5 => "V5",
            Check::V6 => "V6",
            Check::V7 => "V7",
        };
        f.write_str(name)
    }
}

/// One recorded violation (or, for [`Check::V5`], one report).
#[derive(Clone, Debug)]
pub struct Violation {
    /// Position in the process-wide log (see [`mark`]).
    pub seq: u64,
    /// The invariant broken.
    pub check: Check,
    /// A short, stable name for the way it was broken, for example
    /// `"frontier-ahead-of-work"` or `"end-state-residual"`.
    pub kind: &'static str,
    /// The dataflow, when the violation belongs to one.
    pub dataflow: Option<DataflowId>,
    /// The worker whose view or store is involved, when known.
    pub worker: Option<usize>,
    /// Human-readable details.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} {}]", self.check, self.kind)?;
        if let Some(id) = self.dataflow {
            write!(f, " {id}")?;
        }
        if let Some(worker) = self.worker {
            write!(f, " worker {worker}")?;
        }
        write!(f, ": {}", self.detail)
    }
}

/// What the checker does when it finds a violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No checking at all.
    Off,
    /// Record violations in the log and emit `tracing` warnings.
    Report,
    /// Like [`Mode::Report`], and also print each violation to stderr.
    Verbose,
    /// Record, then panic on any violation except V5 reports.
    Panic,
}

static MODE: AtomicU8 = AtomicU8::new(0);

fn mode_from_env() -> Mode {
    match std::env::var("INSTANCY_PROGRESS_CHECK")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "0" | "false" | "none" => Mode::Off,
        "panic" | "strict" => Mode::Panic,
        "verbose" => Mode::Verbose,
        _ => Mode::Report,
    }
}

fn encode_mode(mode: Mode) -> u8 {
    match mode {
        Mode::Off => 1,
        Mode::Report => 2,
        Mode::Verbose => 3,
        Mode::Panic => 4,
    }
}

/// The current mode (from `INSTANCY_PROGRESS_CHECK` unless overridden).
pub fn mode() -> Mode {
    match MODE.load(Ordering::Relaxed) {
        1 => Mode::Off,
        2 => Mode::Report,
        3 => Mode::Verbose,
        4 => Mode::Panic,
        _ => {
            let mode = mode_from_env();
            let _ =
                MODE.compare_exchange(0, encode_mode(mode), Ordering::Relaxed, Ordering::Relaxed);
            mode
        }
    }
}

/// Override the mode for the whole process.
pub fn set_mode(mode: Mode) {
    MODE.store(encode_mode(mode), Ordering::Relaxed);
}

/// Whether any checking is enabled.
pub(crate) fn enabled() -> bool {
    mode() != Mode::Off
}

/// One sequence shared by log entries and oracles, so a test can ask for
/// everything that happened after a [`mark`].
static SEQ: AtomicU64 = AtomicU64::new(1);

fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// A position in the process-wide sequence of log entries and oracles.
///
/// Pass it to [`violations_since`] or [`oracles_since`] to see only what
/// happened afterwards.
pub fn mark() -> u64 {
    SEQ.load(Ordering::Relaxed)
}

/// Upper bound on the log's length; later entries are counted, not kept.
const LOG_CAPACITY: usize = 100_000;

#[derive(Default)]
struct Log {
    entries: Vec<Violation>,
    dropped: u64,
}

fn log() -> MutexGuard<'static, Log> {
    static LOG: OnceLock<Mutex<Log>> = OnceLock::new();
    LOG.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Every violation recorded so far in this process.
pub fn violations() -> Vec<Violation> {
    log().entries.clone()
}

/// The violations recorded at or after `mark`.
pub fn violations_since(mark: u64) -> Vec<Violation> {
    log()
        .entries
        .iter()
        .filter(|v| v.seq >= mark)
        .cloned()
        .collect()
}

/// The violations recorded for one dataflow.
pub fn violations_for(dataflow: DataflowId) -> Vec<Violation> {
    log()
        .entries
        .iter()
        .filter(|v| v.dataflow == Some(dataflow))
        .cloned()
        .collect()
}

/// Number of violations not kept because the log was full.
pub fn dropped_violations() -> u64 {
    log().dropped
}

/// Record a violation (a V5 report is recorded but never panics).
pub(crate) fn record(
    check: Check,
    kind: &'static str,
    dataflow: Option<DataflowId>,
    worker: Option<usize>,
    detail: String,
) {
    let mode = mode();
    if mode == Mode::Off {
        return;
    }
    let violation = Violation {
        seq: next_seq(),
        check,
        kind,
        dataflow,
        worker,
        detail,
    };
    #[cfg(feature = "tracing")]
    tracing::warn!(target: "instancy::progress::check", "{violation}");
    if mode == Mode::Verbose || (mode == Mode::Panic && !check.is_report()) {
        eprintln!("instancy progress check: {violation}");
    }
    let message = (mode == Mode::Panic && !check.is_report()).then(|| violation.to_string());
    {
        let mut log = log();
        if log.entries.len() < LOG_CAPACITY {
            log.entries.push(violation);
        } else {
            log.dropped += 1;
        }
    }
    if let Some(message) = message {
        panic!("instancy progress check failed: {message}");
    }
}

/// How the executor activates a dataflow's operators. A test knob of the
/// conformance matrix (see `DataflowBuilder::check_activation`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Activation {
    /// One operator per activation, from a FIFO ready queue.
    Unfused,
    /// Every operator in one topological pass per sweep.
    Fused,
    /// Operators grouped into per-stage tasks (the runtime default).
    StageTasks,
}

// ---------------------------------------------------------------------------
// Oracle statistics (for tests: did the checker actually run?)
// ---------------------------------------------------------------------------

/// What one oracle saw. Tests use these to make sure a passing run was really
/// checked, not skipped.
#[derive(Clone, Debug, Default)]
pub struct OracleStats {
    /// Creation position in the process-wide sequence (see [`mark`]).
    pub seq: u64,
    /// The dataflow, if it has an id.
    pub dataflow: Option<DataflowId>,
    /// Workers the dataflow has in total (across all nodes).
    pub total_workers: usize,
    /// Workers that registered with this oracle.
    pub registered_workers: usize,
    /// Worker views (trackers) that attached.
    pub views: usize,
    /// Frontier publications checked.
    pub publishes: u64,
    /// Channels wrapped at a materialization point.
    pub wrapped_channels: u64,
    /// Channels or stores the oracle could not observe (each is also
    /// recorded as a V7 `"coverage-gap"` violation).
    pub coverage_gaps: u64,
    /// Units of work counted into a store (records, tasks or capabilities).
    pub units_counted: u64,
    /// Workers that finished successfully.
    pub succeeded_workers: usize,
    /// Whether the end-state check ran (every worker succeeded).
    pub end_state_checked: bool,
    /// Why checking stopped early (cancellation or failure), if it did.
    pub halted: Option<String>,
    /// Violations found but not logged because the same kind was already
    /// logged several times for this dataflow.
    pub suppressed: u64,
}

fn finished_stats() -> MutexGuard<'static, Vec<OracleStats>> {
    static FINISHED: OnceLock<Mutex<Vec<OracleStats>>> = OnceLock::new();
    FINISHED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Statistics of every oracle created at or after `mark`, live or finished.
pub fn oracles_since(mark: u64) -> Vec<OracleStats> {
    let mut stats: Vec<OracleStats> = finished_stats()
        .iter()
        .filter(|s| s.seq >= mark)
        .cloned()
        .collect();
    let live: Vec<Arc<dyn Sink>> = registry()
        .values()
        .filter_map(|w| w.sink.upgrade())
        .collect();
    for sink in live {
        let s = sink.stats();
        if s.seq >= mark && !stats.iter().any(|f| f.seq == s.seq) {
            stats.push(s);
        }
    }
    stats.sort_by_key(|s| s.seq);
    stats
}

// ---------------------------------------------------------------------------
// Locations, stores and time projection
// ---------------------------------------------------------------------------

/// An operator port: `(operator index, port index)`.
pub(crate) type Loc = (usize, usize);

/// Which worker's instance a unit of work belongs to: `Some(worker)` for an
/// operator's own stores, its capabilities and worker-local (pipeline)
/// channels; `None` for channels that cross workers (exchange, broadcast),
/// whose records may be bound for any worker.
type Owner = Option<usize>;

/// Where a unit of work sits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Store {
    /// Inside a channel (counted at its target input).
    Channel,
    /// An operator's `InputHandle` (at its input).
    InputBuffer,
    /// A channel sink's undelivered event (at its input).
    Egress,
    /// `unary_async` input waiting for a task slot (at its input).
    AsyncInput,
    /// An operator's `pending_output` (at its output).
    PendingOutput,
    /// `unary_async` tasks running or finished but not yet collected (at its
    /// output; counted in tasks).
    AsyncTask,
    /// A capability (at its output; counted in capabilities).
    Capability,
}

impl Store {
    /// Whether the store's location is an output (`Source`) port.
    fn at_output(self) -> bool {
        matches!(
            self,
            Store::PendingOutput | Store::AsyncTask | Store::Capability
        )
    }
}

/// Projects a channel's timestamp onto the dataflow's root timestamp (the
/// identity outside loops).
pub(crate) type Projector<S> = Arc<dyn Fn(&S) -> Box<dyn Any + Send> + Send + Sync>;

/// A store's location in one worker, plus how to reach its oracle.
///
/// Internal to the checker; exposed only because channel endpoints hand it
/// to the stores of the operator that owns them.
#[doc(hidden)]
pub struct Port<S: Timestamp> {
    sink: Arc<dyn Sink>,
    worker: usize,
    owner: Owner,
    loc: Loc,
    project: Option<Projector<S>>,
}

impl<S: Timestamp> Clone for Port<S> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
            worker: self.worker,
            owner: self.owner,
            loc: self.loc,
            project: self.project.clone(),
        }
    }
}

impl<S: Timestamp> fmt::Debug for Port<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Port")
            .field("worker", &self.worker)
            .field("owner", &self.owner)
            .field("loc", &self.loc)
            .finish()
    }
}

impl<S: Timestamp> Port<S> {
    /// A port of `worker`'s own instance.
    pub(crate) fn new(
        sink: Arc<dyn Sink>,
        worker: usize,
        loc: Loc,
        project: Option<Projector<S>>,
    ) -> Self {
        Self {
            sink,
            worker,
            owner: Some(worker),
            loc,
            project,
        }
    }

    /// The same oracle, worker and projection at another location, owned by
    /// this worker (an operator's own store).
    pub(crate) fn at(&self, loc: Loc) -> Self {
        Self {
            loc,
            owner: Some(self.worker),
            ..self.clone()
        }
    }

    /// This port, for work that may belong to any worker.
    fn shared(self) -> Self {
        Self {
            owner: None,
            ..self
        }
    }

    /// A port at `loc` for work that belongs to no worker in particular
    /// (counted for every global view and at the end state).
    pub(crate) fn new_shared(
        sink: Arc<dyn Sink>,
        worker: usize,
        loc: Loc,
        project: Option<Projector<S>>,
    ) -> Self {
        Self::new(sink, worker, loc, project).shared()
    }

    fn with_root<R>(&self, time: &S, f: impl FnOnce(&dyn Any) -> R) -> R {
        match &self.project {
            Some(project) => {
                let root = project(time);
                f(root.as_ref())
            }
            None => f(time),
        }
    }

    /// Add `diff` units at `time` to `store` at this port.
    pub(crate) fn count(&self, store: Store, time: &S, diff: i64) {
        if diff == 0 || !enabled() {
            return;
        }
        self.with_root(time, |root| {
            self.sink
                .count_any(store, self.loc, self.owner, root, diff, self.worker)
        });
    }

    /// A consumer at this (input) port took `n` records at `time` out of the
    /// channel: uncount them and check V1.
    fn consumed(&self, time: &S, n: i64) {
        if !enabled() {
            return;
        }
        self.with_root(time, |root| {
            self.sink
                .consumed_any(self.loc, self.owner, root, n, self.worker)
        });
    }
}

// ---------------------------------------------------------------------------
// The type-erased oracle interface
// ---------------------------------------------------------------------------

/// The oracle as seen by code that does not know the root timestamp type.
/// Times are passed as `&dyn Any` holding the root timestamp.
pub(crate) trait Sink: Send + Sync + 'static {
    fn count_any(
        &self,
        store: Store,
        loc: Loc,
        owner: Owner,
        time: &dyn Any,
        diff: i64,
        worker: usize,
    );
    fn consumed_any(&self, loc: Loc, owner: Owner, time: &dyn Any, n: i64, worker: usize);
    fn exhausted(&self, loc: Loc, worker: usize);
    fn register_channel(&self, source: Loc, target: Loc);
    fn coverage_gap(&self, worker: Option<usize>, what: String);
    fn stats(&self) -> OracleStats;
}

/// Materialization context: the oracle and the worker being materialized.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) worker: usize,
    typed: Arc<dyn Any + Send + Sync>,
}

impl Ctx {
    /// The oracle, typed by the dataflow's root timestamp `T`.
    pub(crate) fn oracle<T: Timestamp>(&self) -> Option<Arc<Oracle<T>>> {
        self.typed.clone().downcast::<Oracle<T>>().ok()
    }
}

impl fmt::Debug for Ctx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ctx").field("worker", &self.worker).finish()
    }
}

// ---------------------------------------------------------------------------
// Registry: one oracle per dataflow id, shared by all nodes in this process
// ---------------------------------------------------------------------------

struct Entry {
    sink: Weak<dyn Sink>,
    typed: Weak<dyn Any + Send + Sync>,
}

fn registry() -> MutexGuard<'static, HashMap<DataflowId, Entry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<DataflowId, Entry>>> = OnceLock::new();
    REGISTRY
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// The oracle for dataflow `id`, created on first use. Every node of a
/// cluster in this process gets the same one. Returns `None` when checking
/// is off.
pub(crate) fn oracle<T: Timestamp>(id: DataflowId, total_workers: usize) -> Option<Arc<Oracle<T>>> {
    if !enabled() {
        return None;
    }
    let mut registry = registry();
    registry.retain(|_, entry| entry.sink.strong_count() > 0);
    if let Some(existing) = registry.get(&id).and_then(|e| e.typed.upgrade()) {
        match existing.downcast::<Oracle<T>>() {
            Ok(oracle) if oracle.total_workers == total_workers => return Some(oracle),
            _ => {
                // Same id, different dataflow shape: keep them apart.
                let oracle = Arc::new(Oracle::<T>::new(Some(id), total_workers));
                oracle.coverage_gap(
                    None,
                    "another dataflow with this id and a different shape is running; \
                     this one gets a separate oracle"
                        .into(),
                );
                return Some(oracle);
            }
        }
    }
    let oracle = Arc::new(Oracle::<T>::new(Some(id), total_workers));
    let sink: Arc<dyn Sink> = oracle.clone();
    let typed: Arc<dyn Any + Send + Sync> = oracle.clone();
    registry.insert(
        id,
        Entry {
            sink: Arc::downgrade(&sink),
            typed: Arc::downgrade(&typed),
        },
    );
    Some(oracle)
}

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

/// How many violations of one kind are logged per dataflow; the rest are
/// counted in [`OracleStats::suppressed`].
const LOGGED_PER_KIND: u64 = 4;

/// A view may keep a non-empty frontier at a fully exhausted input for at
/// most this many publications and this long before V4 fires.
const EXHAUSTED_GRACE: u64 = 256;
const EXHAUSTED_GRACE_TIME: std::time::Duration = std::time::Duration::from_secs(1);

/// Zero counts are dropped every this many publications.
const PRUNE_EVERY: u64 = 1024;

/// How long an oracle waits for all of its dataflow's workers to register.
/// Until they have, it records work but checks nothing that needs every
/// worker (V7, V4); if they never do, the other workers run in another
/// process (a multi-process cluster), which the oracle cannot see, and it
/// stops.
const REGISTRATION_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// One worker's view: the frontiers of the last propagation of one tracker.
struct View<T: Timestamp> {
    worker: usize,
    label: &'static str,
    /// `Some(worker)` while the tracker sees only its own worker (no
    /// cross-worker progress channels in a multi-worker dataflow: the staged
    /// executor's per-stage trackers and `SimpleRuntime`). Such a view is
    /// checked only against its own worker's work: work in a cross-worker
    /// channel may be bound for another worker, so it is checked for this
    /// view only when this worker consumes it (V1) and at the end state.
    local: Option<usize>,
    targets: LocMap<Antichain<T>>,
    sources: LocMap<Antichain<T>>,
    /// Publications with a non-empty frontier at a fully exhausted input,
    /// and when the first of them happened.
    stuck: LocMap<(u64, std::time::Instant)>,
    /// Whether all counted work has been checked against the current
    /// frontiers (new work is checked as it is counted, so a publication
    /// that changes nothing need not scan again).
    scanned: bool,
}

impl<T: Timestamp> View<T> {
    /// Whether work owned by `owner` constrains this view.
    fn sees(&self, owner: Owner) -> bool {
        match (self.local, owner) {
            (Some(local), Some(owner)) => local == owner,
            (Some(_), None) => false,
            (None, _) => true,
        }
    }
}

type Counts<T> = HashMap<(Store, Loc, Owner), BTreeMap<T, i64>, FastHash>;

/// A cheap hasher for the oracle's small integer keys (its maps are on every
/// hook's path, and `SipHash` dominates their cost in debug builds).
#[derive(Default, Clone, Copy)]
struct FastHasher(u64);

impl std::hash::Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u64(u64::from(b));
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = (self.0.rotate_left(5) ^ n).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }

    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }

    fn write_isize(&mut self, n: isize) {
        self.write_u64(n as u64);
    }
}

type FastHash = std::hash::BuildHasherDefault<FastHasher>;
type LocMap<V> = HashMap<Loc, V, FastHash>;

struct State<T: Timestamp> {
    created: std::time::Instant,
    registered: BTreeSet<usize>,
    succeeded: BTreeSet<usize>,
    halted: Option<String>,
    out_edges: LocMap<BTreeSet<Loc>>,
    counts: Counts<T>,
    views: BTreeMap<u64, View<T>>,
    next_view: u64,
    consumers: LocMap<usize>,
    exhausted: LocMap<usize>,
    negative_reported: BTreeSet<(Store, Loc)>,
    logged: HashMap<(Check, &'static str), u64>,
    stats: OracleStats,
}

/// A violation found under the oracle's lock, recorded after releasing it.
struct Found {
    check: Check,
    kind: &'static str,
    worker: Option<usize>,
    detail: String,
}

/// The V7 oracle for one dataflow, typed by its root timestamp.
pub(crate) struct Oracle<T: Timestamp> {
    id: Option<DataflowId>,
    total_workers: usize,
    state: Mutex<State<T>>,
}

impl<T: Timestamp> Oracle<T> {
    fn new(id: Option<DataflowId>, total_workers: usize) -> Self {
        let stats = OracleStats {
            seq: next_seq(),
            dataflow: id,
            total_workers,
            ..OracleStats::default()
        };
        Self {
            id,
            total_workers,
            state: Mutex::new(State {
                created: std::time::Instant::now(),
                registered: BTreeSet::new(),
                succeeded: BTreeSet::new(),
                halted: None,
                out_edges: LocMap::default(),
                counts: Counts::default(),
                views: BTreeMap::new(),
                next_view: 0,
                consumers: LocMap::default(),
                exhausted: LocMap::default(),
                negative_reported: BTreeSet::new(),
                logged: HashMap::new(),
                stats,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether some of the dataflow's workers have not registered (yet).
    /// Past [`REGISTRATION_GRACE`] they never will: they run in another
    /// process, and the oracle halts (and forgets its counts) rather than
    /// report the other processes' half of every exchange.
    fn partial(&self, state: &mut State<T>) -> bool {
        if state.registered.len() >= self.total_workers {
            return false;
        }
        if state.halted.is_none() && state.created.elapsed() >= REGISTRATION_GRACE {
            let reason = format!(
                "only {} of {} workers run in this process; the oracle cannot see the others",
                state.registered.len(),
                self.total_workers
            );
            state.stats.halted = Some(reason.clone());
            state.halted = Some(reason);
            state.counts.clear();
        }
        true
    }

    /// Record findings (after the state lock is released), at most
    /// [`LOGGED_PER_KIND`] per kind.
    fn emit(&self, found: Vec<Found>) {
        if found.is_empty() {
            return;
        }
        let keep: Vec<Found> = {
            let mut state = self.lock();
            let mut keep = Vec::new();
            for f in found {
                let logged = state.logged.entry((f.check, f.kind)).or_insert(0);
                if *logged < LOGGED_PER_KIND {
                    *logged += 1;
                    keep.push(f);
                } else {
                    state.stats.suppressed += 1;
                }
            }
            keep
        };
        for f in keep {
            record(f.check, f.kind, self.id, f.worker, f.detail);
        }
    }

    /// A context for materializing `worker`'s channels.
    pub(crate) fn ctx(self: &Arc<Self>, worker: usize) -> Ctx {
        Ctx {
            sink: self.clone(),
            worker,
            typed: self.clone(),
        }
    }

    /// Register `worker` as one of this dataflow's workers.
    pub(crate) fn register_worker(&self, worker: usize) {
        let mut state = self.lock();
        state.registered.insert(worker);
        state.stats.registered_workers = state.registered.len();
    }

    /// Attach a worker's tracker; returns its view id.
    pub(crate) fn attach_view(&self, worker: usize, label: &'static str) -> u64 {
        let mut state = self.lock();
        let id = state.next_view;
        state.next_view += 1;
        state.views.insert(
            id,
            View {
                worker,
                label,
                local: None,
                targets: LocMap::default(),
                sources: LocMap::default(),
                stuck: LocMap::default(),
                scanned: false,
            },
        );
        state.stats.views += 1;
        id
    }

    /// Detach a view: its frontiers no longer constrain anything.
    pub(crate) fn retire_view(&self, view: u64) {
        self.lock().views.remove(&view);
    }

    /// Stop checking this dataflow (it was cancelled or failed).
    pub(crate) fn halt(&self, reason: &str) {
        let mut state = self.lock();
        if state.halted.is_none() {
            state.halted = Some(reason.to_string());
            state.stats.halted = Some(reason.to_string());
        }
    }

    /// Publish a view's frontiers (those that changed) and check the view
    /// against all counted work (V7) and exhausted inputs (V4). `global`:
    /// the view includes every worker's progress.
    pub(crate) fn publish(
        &self,
        view: u64,
        global: bool,
        targets: impl IntoIterator<Item = (Loc, Antichain<T>)>,
        sources: impl IntoIterator<Item = (Loc, Antichain<T>)>,
    ) {
        let mut found = Vec::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            let total_workers = self.total_workers;
            let Some(v) = state.views.get_mut(&view) else {
                return;
            };
            v.local = (!global && total_workers > 1).then_some(v.worker);
            let mut changed = false;
            v.targets
                .extend(targets.into_iter().inspect(|_| changed = true));
            v.sources
                .extend(sources.into_iter().inspect(|_| changed = true));
            if changed {
                v.scanned = false;
            }
            let scan = !v.scanned;
            state.stats.publishes += 1;
            if state.stats.publishes % PRUNE_EVERY == 0 {
                state.counts.retain(|_, times| {
                    times.retain(|_, n| *n != 0);
                    !times.is_empty()
                });
            }
            if self.partial(state) || state.halted.is_some() {
                return;
            }
            if scan {
                let v = &state.views[&view];
                for ((store, loc, owner), times) in &state.counts {
                    if !v.sees(*owner) {
                        continue;
                    }
                    for (time, &n) in times {
                        if n > 0 {
                            check_one(v, &state.out_edges, *store, *loc, time, n, &mut found);
                        }
                    }
                }
                if let Some(v) = state.views.get_mut(&view) {
                    v.scanned = true;
                }
            }
            // V4: a fully exhausted input must get an empty frontier.
            if state.registered.len() == total_workers {
                let exhausted: Vec<Loc> = state
                    .exhausted
                    .iter()
                    .filter(|(loc, n)| {
                        state
                            .consumers
                            .get(*loc)
                            .is_some_and(|c| *c > 0 && **n >= *c)
                    })
                    .map(|(loc, _)| *loc)
                    .collect();
                let v = state.views.get_mut(&view).expect("view exists");
                for loc in exhausted {
                    let Some(frontier) = v.targets.get(&loc) else {
                        continue;
                    };
                    if frontier.is_empty() {
                        v.stuck.remove(&loc);
                        continue;
                    }
                    let (count, since) =
                        v.stuck.entry(loc).or_insert((0, std::time::Instant::now()));
                    if *count == u64::MAX {
                        // Already reported.
                        continue;
                    }
                    *count += 1;
                    if *count >= EXHAUSTED_GRACE && since.elapsed() >= EXHAUSTED_GRACE_TIME {
                        *count = u64::MAX;
                        found.push(Found {
                            check: Check::V4,
                            kind: "exhausted-input-frontier",
                            worker: Some(v.worker),
                            detail: format!(
                                "every channel into input {loc:?} is exhausted, but the {} \
                                 view still has frontier {:?} there after {EXHAUSTED_GRACE} \
                                 propagations and {EXHAUSTED_GRACE_TIME:?}",
                                v.label,
                                frontier.elements()
                            ),
                        });
                    }
                }
            }
        }
        self.emit(found);
    }

    /// A worker force-closes its operators because its tracker reports
    /// completion: nothing may still be outstanding in its view's locations.
    pub(crate) fn force_close(&self, view: u64) {
        let mut found = Vec::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            if self.partial(state) || state.halted.is_some() {
                return;
            }
            let Some(v) = state.views.get(&view) else {
                return;
            };
            let mut outstanding = Vec::new();
            for ((store, loc, owner), times) in &state.counts {
                if !v.sees(*owner) || !view_covers(v, &state.out_edges, *store, *loc) {
                    continue;
                }
                for (time, &n) in times {
                    if n > 0 {
                        outstanding.push(format!(
                            "{n} in {store:?} at {loc:?}@{time:?}{}",
                            owner_suffix(*owner)
                        ));
                    }
                }
            }
            if !outstanding.is_empty() {
                outstanding.truncate(8);
                found.push(Found {
                    check: Check::V4,
                    kind: "force-close-with-work",
                    worker: Some(v.worker),
                    detail: format!(
                        "its {} reports completion and it force-closes its operators while \
                         work is outstanding: {}",
                        v.label,
                        outstanding.join(", ")
                    ),
                });
            }
        }
        self.emit(found);
    }

    /// A worker's executor finished (`ok`: it reports success). Its own
    /// stores must then be empty and it may hold no capability; when every
    /// worker has succeeded, the channels between workers must be empty too.
    pub(crate) fn worker_finished(&self, worker: usize, ok: bool) {
        let mut found = Vec::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            if !ok {
                if state.halted.is_none() {
                    state.halted = Some(format!("worker {worker} did not succeed"));
                    state.stats.halted = state.halted.clone();
                }
                return;
            }
            if !state.succeeded.insert(worker) {
                return;
            }
            state.stats.succeeded_workers = state.succeeded.len();
            // This worker's own work: checked now, then forgotten (whatever
            // its operators still hold can no longer produce anything).
            let own: Vec<(Store, Loc, Owner)> = state
                .counts
                .keys()
                .filter(|(_, _, owner)| *owner == Some(worker))
                .copied()
                .collect();
            for key in own {
                let times = state.counts.remove(&key).unwrap_or_default();
                if state.halted.is_some() {
                    continue;
                }
                let (store, loc, _) = key;
                for (time, n) in times {
                    if n == 0 {
                        continue;
                    }
                    found.push(residual(store, loc, Some(worker), &time, n));
                }
            }
            let all = state.succeeded.len() == self.total_workers
                && state.registered.len() == self.total_workers;
            if !all || state.halted.is_some() || state.stats.end_state_checked {
                return;
            }
            state.stats.end_state_checked = true;
            for ((store, loc, owner), times) in &state.counts {
                for (time, &n) in times {
                    if n != 0 {
                        found.push(residual(*store, *loc, *owner, time, n));
                    }
                }
            }
        }
        self.emit(found);
    }

    /// Typed `count_any`.
    pub(crate) fn count(
        &self,
        store: Store,
        loc: Loc,
        owner: Owner,
        time: &T,
        diff: i64,
        worker: usize,
    ) {
        let mut found = Vec::new();
        {
            let mut state = self.lock();
            self.count_locked(
                &mut state, store, loc, owner, time, diff, worker, &mut found,
            );
        }
        self.emit(found);
    }

    /// `count` under the state lock.
    #[allow(clippy::too_many_arguments)]
    fn count_locked(
        &self,
        state: &mut State<T>,
        store: Store,
        loc: Loc,
        owner: Owner,
        time: &T,
        diff: i64,
        worker: usize,
        found: &mut Vec<Found>,
    ) {
        // Nothing is counted once checking has halted. A finished worker's
        // stores were already checked and forgotten (its capability objects
        // are dropped after it reports success).
        if state.halted.is_some() || owner.is_some_and(|owner| state.succeeded.contains(&owner)) {
            return;
        }
        // Zero entries are kept (and pruned now and then, see `publish`):
        // most work enters and leaves a store at the same time over and over.
        let times = state.counts.entry((store, loc, owner)).or_default();
        let entry = times.entry(time.clone()).or_insert(0);
        *entry += diff;
        let now = *entry;
        if diff > 0 {
            state.stats.units_counted += diff as u64;
        }
        // Until every worker has registered, the other side of a
        // cross-worker channel may not be counted yet.
        if self.partial(state) || state.halted.is_some() {
            return;
        }
        if now < 0 && state.negative_reported.insert((store, loc)) {
            found.push(Found {
                check: Check::V7,
                kind: "negative-occupancy",
                worker: Some(worker),
                detail: format!(
                    "{store:?} at {loc:?}@{time:?}{} went to {now}: work left a store \
                     without entering it, so some producer or store is not observed",
                    owner_suffix(owner)
                ),
            });
        }
        if diff > 0 && now > 0 {
            for v in state.views.values().filter(|v| v.sees(owner)) {
                check_one(v, &state.out_edges, store, loc, time, now, found);
            }
        }
    }

    /// Typed `consumed_any`.
    fn consumed(&self, loc: Loc, owner: Owner, time: &T, n: i64, worker: usize) {
        let mut found = Vec::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            self.count_locked(
                state,
                Store::Channel,
                loc,
                owner,
                time,
                -n,
                worker,
                &mut found,
            );
            if state.halted.is_some() {
                return;
            }
            for v in state.views.values().filter(|v| v.worker == worker) {
                let Some(frontier) = v.targets.get(&loc) else {
                    continue;
                };
                if !frontier.less_equal(time) {
                    found.push(Found {
                        check: Check::V1,
                        kind: "late-data",
                        worker: Some(worker),
                        detail: format!(
                            "consumed {n} record(s) at input {loc:?}@{time:?} behind this \
                             worker's {} frontier {:?}",
                            v.label,
                            frontier.elements()
                        ),
                    });
                }
            }
        }
        self.emit(found);
    }
}

fn owner_suffix(owner: Owner) -> String {
    match owner {
        Some(worker) => format!(" (worker {worker})"),
        None => " (between workers)".into(),
    }
}

/// A unit of work left behind at a successful completion.
fn residual<T: Timestamp>(store: Store, loc: Loc, owner: Owner, time: &T, n: i64) -> Found {
    let (check, kind, what) = if store == Store::Capability {
        (
            Check::V4,
            "finished-holding-capability",
            "a capability is still held (work its operator stashed is lost)",
        )
    } else if n < 0 {
        (
            Check::V7,
            "end-state-negative",
            "the store ended negative (it was not observed whole)",
        )
    } else {
        (
            Check::V7,
            "end-state-residual",
            "records left in a store were silently dropped",
        )
    };
    Found {
        check,
        kind,
        worker: owner,
        detail: format!(
            "reported success with {n} unit(s) left in {store:?} at {loc:?}@{time:?}{}: {what}",
            owner_suffix(owner)
        ),
    }
}

/// Whether view `v` tracks the location `(store, loc)` is checked against.
fn view_covers<T: Timestamp>(
    v: &View<T>,
    out_edges: &LocMap<BTreeSet<Loc>>,
    store: Store,
    loc: Loc,
) -> bool {
    match store {
        Store::Capability => v.sources.contains_key(&loc),
        s if s.at_output() => out_edges
            .get(&loc)
            .is_some_and(|targets| targets.iter().any(|t| v.targets.contains_key(t))),
        _ => v.targets.contains_key(&loc),
    }
}

/// Check one unit of work against one view (V7).
fn check_one<T: Timestamp>(
    v: &View<T>,
    out_edges: &LocMap<BTreeSet<Loc>>,
    store: Store,
    loc: Loc,
    time: &T,
    n: i64,
    found: &mut Vec<Found>,
) {
    let mut fail = |at: String, frontier: &Antichain<T>| {
        found.push(Found {
            check: Check::V7,
            kind: "frontier-ahead-of-work",
            worker: Some(v.worker),
            detail: format!(
                "{n} unit(s) in {store:?} at {loc:?}@{time:?}, but worker {}'s {} frontier at \
                 {at} is {:?}",
                v.worker,
                v.label,
                frontier.elements()
            ),
        });
    };
    match store {
        Store::Capability => {
            if let Some(frontier) = v.sources.get(&loc) {
                if !frontier.less_equal(time) {
                    fail(format!("output {loc:?}"), frontier);
                }
            }
        }
        s if s.at_output() => {
            if let Some(targets) = out_edges.get(&loc) {
                for target in targets {
                    if let Some(frontier) = v.targets.get(target) {
                        if !frontier.less_equal(time) {
                            fail(format!("input {target:?}"), frontier);
                        }
                    }
                }
            }
        }
        _ => {
            if let Some(frontier) = v.targets.get(&loc) {
                if !frontier.less_equal(time) {
                    fail(format!("input {loc:?}"), frontier);
                }
            }
        }
    }
}

impl<T: Timestamp> Oracle<T> {
    fn root_time<'a>(&self, time: &'a dyn Any, worker: usize) -> Option<&'a T> {
        let time = time.downcast_ref::<T>();
        if time.is_none() {
            self.coverage_gap(
                Some(worker),
                format!(
                    "a store reported a time that is not the root timestamp {}; a loop-body \
                     channel or store is missing its time projection",
                    std::any::type_name::<T>()
                ),
            );
        }
        time
    }
}

impl<T: Timestamp> Sink for Oracle<T> {
    fn count_any(
        &self,
        store: Store,
        loc: Loc,
        owner: Owner,
        time: &dyn Any,
        diff: i64,
        worker: usize,
    ) {
        if let Some(time) = self.root_time(time, worker) {
            self.count(store, loc, owner, time, diff, worker);
        }
    }

    fn consumed_any(&self, loc: Loc, owner: Owner, time: &dyn Any, n: i64, worker: usize) {
        if let Some(time) = self.root_time(time, worker) {
            self.consumed(loc, owner, time, n, worker);
        }
    }

    fn exhausted(&self, loc: Loc, _worker: usize) {
        *self.lock().exhausted.entry(loc).or_insert(0) += 1;
    }

    fn register_channel(&self, source: Loc, target: Loc) {
        let mut state = self.lock();
        state.out_edges.entry(source).or_default().insert(target);
        *state.consumers.entry(target).or_insert(0) += 1;
        state.stats.wrapped_channels += 1;
    }

    fn coverage_gap(&self, worker: Option<usize>, what: String) {
        self.lock().stats.coverage_gaps += 1;
        self.emit(vec![Found {
            check: Check::V7,
            kind: "coverage-gap",
            worker,
            detail: what,
        }]);
    }

    fn stats(&self) -> OracleStats {
        self.lock().stats.clone()
    }
}

impl<T: Timestamp> Drop for Oracle<T> {
    fn drop(&mut self) {
        let stats = self
            .state
            .get_mut()
            .map(|s| s.stats.clone())
            .unwrap_or_else(|e| e.into_inner().stats.clone());
        let mut finished = finished_stats();
        if finished.len() >= LOG_CAPACITY {
            finished.remove(0);
        }
        finished.push(stats);
    }
}

// ---------------------------------------------------------------------------
// Tracker views
// ---------------------------------------------------------------------------

/// A tracker's link to its oracle: the view it publishes into.
pub(crate) struct TrackerCheck<T: Timestamp> {
    oracle: Arc<Oracle<T>>,
    view: u64,
    worker: usize,
    finished: bool,
}

impl<T: Timestamp> TrackerCheck<T> {
    pub(crate) fn new(oracle: Arc<Oracle<T>>, worker: usize, label: &'static str) -> Self {
        oracle.register_worker(worker);
        let view = oracle.attach_view(worker, label);
        Self {
            oracle,
            view,
            worker,
            finished: false,
        }
    }

    /// `global`: the tracker receives every worker's progress.
    pub(crate) fn publish(
        &self,
        global: bool,
        targets: impl IntoIterator<Item = (Loc, Antichain<T>)>,
        sources: impl IntoIterator<Item = (Loc, Antichain<T>)>,
    ) {
        self.oracle.publish(self.view, global, targets, sources);
    }

    pub(crate) fn force_close(&self) {
        self.oracle.force_close(self.view);
    }

    pub(crate) fn halt(&self, reason: &str) {
        self.oracle.halt(reason);
    }

    /// The tracker's executor finished; `residual` lists the view's non-zero
    /// counts (V5 report).
    pub(crate) fn finish(&mut self, ok: bool, residual: Vec<String>, report_worker: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if ok && !residual.is_empty() {
            let total = residual.len();
            let shown: Vec<String> = residual.into_iter().take(8).collect();
            record(
                Check::V5,
                "residual",
                self.oracle.id,
                Some(self.worker),
                format!(
                    "finished with {total} non-zero count(s) in its view (negative: a missing +n; \
                     positive: a leak): {}",
                    shown.join(", ")
                ),
            );
        }
        self.oracle.retire_view(self.view);
        if report_worker {
            self.oracle.worker_finished(self.worker, ok);
        }
    }

    pub(crate) fn oracle(&self) -> &Arc<Oracle<T>> {
        &self.oracle
    }
}

impl<T: Timestamp> Drop for TrackerCheck<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.oracle.retire_view(self.view);
        }
    }
}

// ---------------------------------------------------------------------------
// Channel wrappers
// ---------------------------------------------------------------------------

/// Wraps the push end of a materialized channel: an envelope counts in the
/// channel from the first time it is offered. A retry of the same envelope
/// (after back-pressure or a partial delivery) is not counted again.
pub(crate) struct CheckedPush<S: Timestamp, D, M> {
    inner: Box<dyn Push<S, D, M>>,
    /// At the channel's target input.
    target: Port<S>,
    /// At the producing output (handed to the producer's stores).
    source: Port<S>,
    /// Copies delivered per record (the target worker count for a broadcast).
    fanout: i64,
    /// The envelope offered and refused last: `(time, records)`.
    retry: Option<(S, usize)>,
}

impl<S: Timestamp, D, M> CheckedPush<S, D, M> {
    fn offer(&mut self, envelope: &Envelope<S, D, M>) {
        let Payload::Data { time, data } = &envelope.payload else {
            return;
        };
        if let Some((rt, rn)) = &self.retry {
            if rt == time && *rn == data.len() {
                return;
            }
        }
        if let Some((rt, rn)) = self.retry.take() {
            // A different envelope: the refused one was abandoned.
            self.target
                .count(Store::Channel, &rt, -(rn as i64) * self.fanout);
        }
        self.target
            .count(Store::Channel, time, data.len() as i64 * self.fanout);
    }

    fn refused(&mut self, envelope: &Envelope<S, D, M>) {
        if let Payload::Data { time, data } = &envelope.payload {
            self.retry = Some((time.clone(), data.len()));
        }
    }
}

impl<S: Timestamp, D: 'static, M: 'static> Push<S, D, M> for CheckedPush<S, D, M> {
    fn push(&mut self, envelope: Envelope<S, D, M>) -> crate::error::Result<()> {
        self.offer(&envelope);
        self.retry = None;
        self.inner.push(envelope)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<S, D, M>,
    ) -> std::result::Result<(), (crate::error::Error, Envelope<S, D, M>)> {
        self.offer(&envelope);
        match self.inner.try_push(envelope) {
            Ok(()) => {
                self.retry = None;
                Ok(())
            }
            Err((error, envelope)) => {
                self.refused(&envelope);
                Err((error, envelope))
            }
        }
    }

    fn flush(&mut self) -> crate::error::Result<()> {
        self.inner.flush()
    }

    fn close(&mut self) {
        self.inner.close();
    }

    fn close_on_drop(&mut self) {
        self.inner.close_on_drop();
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn available_capacity(&self) -> Option<usize> {
        self.inner.available_capacity()
    }

    fn set_inflight_reporter(&mut self, reporter: crate::progress::operate::ProgressReporter<S>) {
        self.inner.set_inflight_reporter(reporter);
    }

    fn check_port(&self) -> Option<Port<S>> {
        Some(self.source.clone())
    }
}

/// Wraps the pull end of a materialized channel: a pulled envelope leaves
/// the channel (V1 is checked here) and the first exhaustion is reported.
pub(crate) struct CheckedPull<S: Timestamp, D, M> {
    inner: Box<dyn Pull<S, D, M>>,
    target: Port<S>,
    exhausted: std::sync::atomic::AtomicBool,
}

impl<S: Timestamp, D: 'static, M: 'static> Pull<S, D, M> for CheckedPull<S, D, M> {
    fn pull(&mut self) -> Option<Envelope<S, D, M>> {
        let envelope = self.inner.pull()?;
        if let Payload::Data { time, data } = &envelope.payload {
            self.target.consumed(time, data.len() as i64);
        }
        Some(envelope)
    }

    fn is_exhausted(&self) -> bool {
        let exhausted = self.inner.is_exhausted();
        if exhausted && !self.exhausted.swap(true, Ordering::Relaxed) && enabled() {
            self.target
                .sink
                .exhausted(self.target.loc, self.target.worker);
        }
        exhausted
    }

    fn set_inflight_reporter(&mut self, reporter: crate::progress::operate::ProgressReporter<S>) {
        self.inner.set_inflight_reporter(reporter);
    }

    fn check_port(&self) -> Option<Port<S>> {
        Some(self.target.clone())
    }
}

/// Where a channel sits, for wrapping it.
pub(crate) struct ChannelSite<'a> {
    pub(crate) ctx: &'a Ctx,
    pub(crate) source: Loc,
    pub(crate) target: Loc,
}

/// Wrap a type-erased channel pair whose concrete types are
/// `Box<dyn Push<S, D, M>>` / `Box<dyn Pull<S, D, M>>`.
pub(crate) fn wrap_pair<S: Timestamp, D: 'static, M: 'static>(
    pair: crate::dataflow::schedulable::ChannelPair,
    site: &ChannelSite<'_>,
    projector: Option<&(dyn Any + Send + Sync)>,
    fanout: i64,
    cross_worker: bool,
) -> crate::dataflow::schedulable::ChannelPair {
    let (push, pull) = pair;
    let push = match push.downcast::<Box<dyn Push<S, D, M>>>() {
        Ok(push) => *push,
        Err(push) => {
            site.ctx.sink.coverage_gap(
                Some(site.ctx.worker),
                format!(
                    "channel {:?} -> {:?}: push end is not a Push<{}>; not observed",
                    site.source,
                    site.target,
                    std::any::type_name::<S>()
                ),
            );
            return (push, pull);
        }
    };
    let pull = match pull.downcast::<Box<dyn Pull<S, D, M>>>() {
        Ok(pull) => *pull,
        Err(pull) => {
            site.ctx.sink.coverage_gap(
                Some(site.ctx.worker),
                format!(
                    "channel {:?} -> {:?}: pull end is not a Pull<{}>; not observed",
                    site.source,
                    site.target,
                    std::any::type_name::<S>()
                ),
            );
            return (Box::new(push), pull);
        }
    };
    let project: Option<Projector<S>> = match projector {
        Some(any) => match any.downcast_ref::<Projector<S>>() {
            Some(p) => Some(p.clone()),
            None => {
                site.ctx.sink.coverage_gap(
                    Some(site.ctx.worker),
                    format!(
                        "channel {:?} -> {:?}: time projector has the wrong type",
                        site.source, site.target
                    ),
                );
                None
            }
        },
        None => None,
    };
    site.ctx.sink.register_channel(site.source, site.target);
    let target = Port::new(site.ctx.sink.clone(), site.ctx.worker, site.target, project);
    let source = target.at(site.source);
    let target = if cross_worker {
        target.shared()
    } else {
        target
    };
    let push: Box<dyn Push<S, D, M>> = Box::new(CheckedPush {
        inner: push,
        target: target.clone(),
        source,
        fanout,
        retry: None,
    });
    let pull: Box<dyn Pull<S, D, M>> = Box::new(CheckedPull {
        inner: pull,
        target,
        exhausted: std::sync::atomic::AtomicBool::new(false),
    });
    (Box::new(push), Box::new(pull))
}

/// The wrap function of a typed channel factory.
pub(crate) type WrapFn = fn(
    crate::dataflow::schedulable::ChannelPair,
    &ChannelSite<'_>,
    Option<&(dyn Any + Send + Sync)>,
    i64,
    bool,
) -> crate::dataflow::schedulable::ChannelPair;

// ---------------------------------------------------------------------------
// Operator stores
// ---------------------------------------------------------------------------

/// A store's link to the oracle: a port plus the store kind. Holds the count
/// it added so far so the store can be uncounted if it is dropped.
pub(crate) struct StoreCheck<S: Timestamp> {
    port: Port<S>,
    store: Store,
}

impl<S: Timestamp> StoreCheck<S> {
    /// A store of the operator that `port` (a channel end of the worker that
    /// runs it) belongs to: the store is that worker's, even when the channel
    /// crosses workers.
    pub(crate) fn new(port: Port<S>, store: Store) -> Self {
        let port = port.at(port.loc);
        Self { port, store }
    }

    pub(crate) fn add(&self, time: &S, n: usize) {
        self.port.count(self.store, time, n as i64);
    }

    pub(crate) fn remove(&self, time: &S, n: usize) {
        self.port.count(self.store, time, -(n as i64));
    }
}

impl<S: Timestamp> fmt::Debug for StoreCheck<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreCheck")
            .field("port", &self.port)
            .field("store", &self.store)
            .finish()
    }
}

/// An operator's output retry queue, measured from its contents.
pub(crate) struct PendingOutput<S: Timestamp, D, M = ()> {
    queue: std::collections::VecDeque<Envelope<S, D, M>>,
    check: Option<StoreCheck<S>>,
}

/// Records in a data envelope.
fn records<S: Timestamp, D, M>(envelope: &Envelope<S, D, M>) -> Option<(&S, usize)> {
    match &envelope.payload {
        Payload::Data { time, data } => Some((time, data.len())),
        Payload::Control(_) => None,
    }
}

impl<S: Timestamp, D, M> PendingOutput<S, D, M> {
    pub(crate) fn new() -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            check: None,
        }
    }

    /// Measure this queue as work held for `pusher`'s output.
    pub(crate) fn observe(&mut self, pusher: &dyn Push<S, D, M>) {
        self.check = pusher
            .check_port()
            .map(|port| StoreCheck::new(port, Store::PendingOutput));
    }

    fn added(&self, envelope: &Envelope<S, D, M>) {
        if let (Some(check), Some((time, n))) = (&self.check, records(envelope)) {
            check.add(time, n);
        }
    }

    fn removed(&self, envelope: &Envelope<S, D, M>) {
        if let (Some(check), Some((time, n))) = (&self.check, records(envelope)) {
            check.remove(time, n);
        }
    }

    pub(crate) fn push_back(&mut self, envelope: Envelope<S, D, M>) {
        self.added(&envelope);
        self.queue.push_back(envelope);
    }

    pub(crate) fn push_front(&mut self, envelope: Envelope<S, D, M>) {
        self.added(&envelope);
        self.queue.push_front(envelope);
    }

    pub(crate) fn pop_front(&mut self) -> Option<Envelope<S, D, M>> {
        let envelope = self.queue.pop_front()?;
        self.removed(&envelope);
        Some(envelope)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<S: Timestamp, D, M> Default for PendingOutput<S, D, M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(t: u64) -> Antichain<u64> {
        Antichain::from_elem(t)
    }

    /// A cluster whose other node runs in another process: this process
    /// sees only its half of each exchange, so the oracle must not report
    /// the other half as violations.
    #[test]
    fn oracle_with_unregistered_workers_reports_nothing() {
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 2).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        let view = oracle.attach_view(0, "worker tracker");
        // Records sent by the other process, pulled here.
        oracle.count(Store::Channel, (2, 0), None, &0, -5, 0);
        // Records sent to the other process, never pulled here.
        oracle.count(Store::Channel, (2, 0), None, &1, 3, 0);
        oracle.publish(view, true, [((2, 0), frontier(4))], []);
        oracle.force_close(view);
        assert!(
            violations_for(id).iter().all(|v| v.check.is_report()),
            "{:?}",
            violations_for(id)
        );
    }

    /// With every worker registered, the same counts are violations.
    #[test]
    fn oracle_with_every_worker_reports_negative_occupancy_and_late_frontier() {
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        let view = oracle.attach_view(0, "worker tracker");
        oracle.count(Store::Channel, (2, 0), None, &0, -5, 0);
        oracle.count(Store::Channel, (2, 0), None, &1, 3, 0);
        oracle.publish(view, true, [((2, 0), frontier(4))], []);
        let kinds: Vec<_> = violations_for(id).iter().map(|v| v.kind).collect();
        assert!(kinds.contains(&"negative-occupancy"), "{kinds:?}");
        assert!(kinds.contains(&"frontier-ahead-of-work"), "{kinds:?}");
    }

    /// An exhausted input whose frontier never empties is reported once,
    /// however long it stays stuck.
    #[test]
    fn exhausted_input_is_reported_once() {
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        oracle.exhausted((2, 0), 0);
        let view = oracle.attach_view(0, "worker tracker");
        for _ in 0..EXHAUSTED_GRACE + 10 {
            oracle.publish(view, true, [((2, 0), frontier(0))], []);
        }
        std::thread::sleep(EXHAUSTED_GRACE_TIME);
        for _ in 0..EXHAUSTED_GRACE + 10 {
            oracle.publish(view, true, [((2, 0), frontier(0))], []);
        }
        let reported = violations_for(id)
            .iter()
            .filter(|v| v.kind == "exhausted-input-frontier")
            .count();
        assert_eq!(reported, 1);
    }
}
