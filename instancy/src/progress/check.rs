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
//! | [`Check::V3`] | Recorded, never panics: a logic call emitted output, retained a capability or asked for a notification at a time it does not justify. Production code enforces this and fails the operator (`ProgressError::Unjustified`). |
//! | [`Check::V4`] | A worker never force-closes, seals, or reports completion while work it holds is outstanding; an input whose every channel is exhausted gets an empty frontier within a bounded number of propagations. |
//! | [`Check::V5`] | The non-zero counts left in a worker's view once they are final: it succeeded, or every worker's seal is applied (a proven residual). A negative count means a missing `+n`, a positive one a leak. |
//! | [`Check::V6`] | Wire rules: split progress batches are positives-first, and nothing follows a progress end-of-stream. (Frames carry no per-sender sequence number to check order with.) |
//! | [`Check::V7`] | The oracle. It measures where work physically is, independently of the accounting under test, and asserts that no worker's view claims a location is done while work is still there. At successful completion every store must be empty. |
//!
//! # The oracle (V7)
//!
//! The oracle never reads progress reporters, the tracker's counts or any
//! other ledger. It counts work from the stores themselves:
//!
//! - **channels**: every channel is wrapped where it is materialized
//!   (the executor's materialization,
//!   the one materialization point of every path). An envelope counts
//!   from the moment a producer first offers it (so the inside of composite
//!   pushers, exchange buckets, network queues and receive bridges is all
//!   covered) until a consumer pulls it. Broadcast edges count one copy per
//!   target worker;
//! - **operator stores**: the shell's input buffers (`InputHandle`), the
//!   output a logic call emitted until the shell takes it (`OutputHandle`),
//!   retry queues, channel-sink egress (`pending_send`) and `unary_async`
//!   input, task and result queues, each measured from its own contents;
//! - **capabilities**: live [`Capability`](crate::progress::capability::Capability)
//!   objects (a `Retained` holds one), counted from when the object is
//!   created until a commit drains its release from the port's release
//!   mailbox (its reporter).
//!
//! When a worker seals its progress stream, every store of its own operators
//! must be empty (V4).
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
//! Hooks run on the dataflow's own threads but do no checking there: each
//! appends an event to its oracle's queue, and a background thread
//! (`instancy-progress-check`) applies the events in queue order, which is
//! the order the hooks ran in, so the outcome is the same as checking in
//! place. Checking in place, under a lock every worker shared, changed the
//! interleavings of the dataflows being checked enough to make timing bugs
//! of today's code several times likelier. The functions that read the log
//! ([`violations_since`], [`oracles_since`], ...) apply every queued event
//! first. In `panic` mode the hook applies the queue itself, so that the
//! violating thread panics.
//!
//! # Modes
//!
//! The environment variable `INSTANCY_PROGRESS_CHECK` selects what happens
//! on a violation:
//!
//! - `report`: violations are recorded in a process-wide log (see
//!   [`violations_since`]) and emitted as `tracing` warnings;
//! - `verbose`: as `report`, and each recorded violation is also printed to
//!   standard error;
//! - `panic`: as `report`, then the violating thread panics (a V3 record
//!   never panics: its production error already fails the run);
//! - `off`: no checking.
//!
//! Unset, it is `panic` in debug builds and `report` otherwise: after
//! port-counted progress every finding is a bug. [`set_mode`] overrides the
//! variable for the whole process.
//!
//! Each violation names the operator port it is about
//! ([`Violation::location`]). The same kind of violation is logged at most
//! four times per location and dataflow; after that, a location's per-record
//! frontier violations (V1 `late-data`, V7 `frontier-ahead-of-work`) are no
//! longer described, which keeps a dataflow that breaks an invariant on every
//! record cheap to check without hiding violations at other locations.
//!
//! Known limits: the `OutputHandle` buffer and data inside a user closure are
//! transient within one activation and are not measured. An oracle checks
//! one process: a cluster whose nodes run in different processes is checked
//! by none (see `REGISTRATION_GRACE`).

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
    /// Justification: output, a retained capability or a notification at a
    /// time the logic call does not justify. Production code enforces it
    /// (`ProgressError::Unjustified`); the checker records it.
    V3,
    /// Termination: completion or force-close with work outstanding, or an
    /// exhausted input whose frontier never empties.
    V4,
    /// Residual: non-zero counts in a worker's view once they are final (it
    /// succeeded, or every worker's seal is applied).
    V5,
    /// Wire rules for progress traffic.
    V6,
    /// Oracle: a frontier ahead of work that is physically present, or work
    /// left in a store at successful completion.
    V7,
}

impl Check {
    /// Whether this check only records (never panics, even in
    /// [`Mode::Panic`]): V3, whose production error already fails the run.
    pub fn is_report(self) -> bool {
        matches!(self, Check::V3)
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Check::V1 => "V1",
            Check::V3 => "V3",
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
    /// The operator port the violation is about, as `name[in i]` or
    /// `name[out i]` (operator names that occur more than once in a
    /// dataflow get a `(k)` suffix, in operator order). `None` for
    /// violations about a whole worker or dataflow.
    pub location: Option<String>,
    /// Human-readable details.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} {}", self.check, self.kind)?;
        if let Some(location) = &self.location {
            write!(f, " @ {location}")?;
        }
        f.write_str("]")?;
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
        "report" => Mode::Report,
        // After port-counted progress every finding is a bug: debug builds
        // panic on it by default.
        _ if cfg!(debug_assertions) => Mode::Panic,
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

/// The current mode (from `INSTANCY_PROGRESS_CHECK` unless overridden; see
/// the [module docs](self#modes)).
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

/// Dump the progress trace of a dataflow (see the flight recorder,
/// `progress::trace`): for test harnesses, on any unexpected outcome.
#[doc(hidden)]
pub use crate::progress::trace::dump_traces;

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
/// happened afterwards. Every hook that ran before the call is applied
/// first, so its violations come before the mark.
pub fn mark() -> u64 {
    let _settled = settle();
    SEQ.load(Ordering::Relaxed)
}

/// Upper bound on the log's length; when it is full, the oldest entry is
/// evicted (and counted in [`dropped_violations`]).
const LOG_CAPACITY: usize = 100_000;

#[derive(Default)]
struct Log {
    entries: std::collections::VecDeque<Violation>,
    dropped: u64,
    /// The sequence number of the newest evicted entry.
    evicted_through: u64,
}

fn log() -> MutexGuard<'static, Log> {
    static LOG: OnceLock<Mutex<Log>> = OnceLock::new();
    LOG.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Every queued hook applied, and the background thread kept from dropping
/// an oracle until the guard is released: what a reader sees is complete,
/// and the flight recorder's dump of every finding applied before the call
/// is written (a finding applied concurrently, by a worker still running,
/// may still be writing its own).
fn settle() -> Settled {
    let pass = pass();
    // Dumps of findings applied before (by a worker, or by the background
    // thread after its pass) are written.
    drop(dumps_in_flight().write().unwrap_or_else(|e| e.into_inner()));
    let dumps = DeferDumps::new();
    drain_all();
    Settled {
        _pass: pass,
        _dumps: dumps,
    }
}

/// See [`settle`]. The flight recorder's dumps the drain triggered are
/// written once the pass is released (fields drop in order).
struct Settled {
    _pass: MutexGuard<'static, ()>,
    _dumps: DeferDumps,
}

/// Held (shared) by each thread from before it applies checker events (or
/// logs a finding outside them) until it has written the flight recorder
/// dumps they triggered (see [`DeferDumps`]); a reader takes it (exclusive)
/// to wait for those dumps. A thread takes it shared holding no oracle
/// lock and no pass (the background thread takes it after its pass), and
/// only once: a reader takes it exclusive holding the pass.
fn dumps_in_flight() -> &'static std::sync::RwLock<()> {
    static IN_FLIGHT: std::sync::RwLock<()> = std::sync::RwLock::new(());
    &IN_FLIGHT
}

thread_local! {
    /// The flight recorder's checker dumps [`record`] triggered on this
    /// thread while a [`DeferDumps`] is alive.
    static DEFERRED_DUMPS: std::cell::RefCell<Option<Vec<(DataflowId, String)>>> =
        const { std::cell::RefCell::new(None) };
}

/// While alive, the flight recorder's checker dumps that [`record`] triggers
/// on this thread wait, and are written when it drops. It is taken before an
/// oracle's state lock (or after the background thread's pass) and dropped
/// after it, so that writing a dump never holds up the dataflow's other
/// workers or the checker: the recorder must not perturb the run it
/// records. Unwinding (a violation in panic mode), the dump is left to the
/// thread's worker, which writes it once it has caught the panic and
/// recorded its exit.
struct DeferDumps {
    /// The thread's outermost guard (the one that writes the dumps) holds
    /// [`dumps_in_flight`] until they are written.
    outermost: Option<std::sync::RwLockReadGuard<'static, ()>>,
}

impl DeferDumps {
    fn new() -> Self {
        let outermost = crate::progress::trace::enabled()
            && DEFERRED_DUMPS
                .try_with(|d| {
                    let mut d = d.borrow_mut();
                    let outermost = d.is_none();
                    if outermost {
                        *d = Some(Vec::new());
                    }
                    outermost
                })
                .unwrap_or(false);
        Self {
            outermost: outermost
                .then(|| dumps_in_flight().read().unwrap_or_else(|e| e.into_inner())),
        }
    }
}

impl Drop for DeferDumps {
    fn drop(&mut self) {
        if self.outermost.is_none() {
            return;
        }
        use crate::progress::trace::{self, Trigger};
        // Still the outermost guard while it writes (a dump's own findings,
        // if any, are written too).
        loop {
            let dumps = DEFERRED_DUMPS
                .try_with(|d| d.borrow_mut().as_mut().map(std::mem::take))
                .ok()
                .flatten()
                .unwrap_or_default();
            if dumps.is_empty() {
                break;
            }
            for (id, text) in dumps {
                if std::thread::panicking() && trace::defer_current(id, Trigger::Checker, &text) {
                    continue;
                }
                trace::trigger(id, Trigger::Checker, &text);
            }
        }
        let _ = DEFERRED_DUMPS.try_with(|d| d.borrow_mut().take());
    }
}

/// Every violation recorded so far in this process (and still in the log).
pub fn violations() -> Vec<Violation> {
    let _settled = settle();
    log().entries.iter().cloned().collect()
}

/// The violations recorded at or after `mark`.
pub fn violations_since(mark: u64) -> Vec<Violation> {
    let _settled = settle();
    log()
        .entries
        .iter()
        .filter(|v| v.seq >= mark)
        .cloned()
        .collect()
}

/// The violations recorded for one dataflow.
pub fn violations_for(dataflow: DataflowId) -> Vec<Violation> {
    let _settled = settle();
    log()
        .entries
        .iter()
        .filter(|v| v.dataflow == Some(dataflow))
        .cloned()
        .collect()
}

/// Number of violations evicted from the log because it was full.
pub fn dropped_violations() -> u64 {
    let _settled = settle();
    log().dropped
}

/// Whether the log still holds every violation recorded at or after
/// `mark` (none of them was evicted).
pub fn log_complete_since(mark: u64) -> bool {
    let _settled = settle();
    log().evicted_through < mark
}

static SPLIT_COMMITS: AtomicU64 = AtomicU64::new(0);
static SPLIT_ACROSS_SECTIONS: AtomicU64 = AtomicU64::new(0);

/// Progress commits sent on a cluster in several frames (see
/// `DataflowBuilder::check_progress_batch_cap`), process-wide.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SplitCommits {
    /// Commits split into more than one frame.
    pub split: u64,
    /// Of those, commits with changes in more than one scope section (the
    /// parent's and a loop scope's): their frames separate the sections.
    pub across_sections: u64,
}

/// See [`SplitCommits`].
pub fn split_commits() -> SplitCommits {
    SplitCommits {
        split: SPLIT_COMMITS.load(Ordering::Relaxed),
        across_sections: SPLIT_ACROSS_SECTIONS.load(Ordering::Relaxed),
    }
}

/// A commit with changes in `sections` scope sections was split into
/// several frames.
#[cfg(feature = "transport")]
pub(crate) fn note_split_commit(sections: usize) {
    SPLIT_COMMITS.fetch_add(1, Ordering::Relaxed);
    if sections > 1 {
        SPLIT_ACROSS_SECTIONS.fetch_add(1, Ordering::Relaxed);
    }
}

thread_local! {
    /// Live [`ExpectViolations`] guards on this thread.
    static EXPECTING: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn expecting() -> bool {
    EXPECTING.with(|e| e.get() > 0)
}

/// While it is alive, violations recorded on this thread never panic (they
/// are still recorded): for a test that provokes violations on purpose and
/// checks them in the log, in a process whose mode is [`Mode::Panic`]. In
/// that mode the thread whose hook finds a violation records it.
#[doc(hidden)]
pub struct ExpectViolations(std::marker::PhantomData<*const ()>);

/// See [`ExpectViolations`].
#[doc(hidden)]
pub fn expect_violations() -> ExpectViolations {
    EXPECTING.with(|e| e.set(e.get() + 1));
    ExpectViolations(std::marker::PhantomData)
}

impl Drop for ExpectViolations {
    fn drop(&mut self) {
        EXPECTING.with(|e| e.set(e.get() - 1));
    }
}

/// Record a violation (a V3 record never panics).
pub(crate) fn record(
    check: Check,
    kind: &'static str,
    dataflow: Option<DataflowId>,
    worker: Option<usize>,
    location: Option<String>,
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
        location,
        detail,
    };
    #[cfg(feature = "tracing")]
    tracing::warn!(target: "instancy::progress::check", "{violation}");
    let panics = mode == Mode::Panic && !check.is_report() && !expecting();
    if mode == Mode::Verbose || panics {
        if std::thread::current().name() == Some(CHECK_THREAD) {
            // The background thread inherited the output capture of the
            // test that started it, which it outlives: write past it.
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr().lock(),
                "instancy progress check: {violation}"
            );
        } else {
            eprintln!("instancy progress check: {violation}");
        }
    }
    let message = panics.then(|| violation.to_string());
    let mut dump = match dataflow {
        Some(id) if crate::progress::trace::enabled() => Some((id, violation.to_string())),
        _ => None,
    };
    // The flight recorder's dump: under an oracle's lock, deferred until it
    // is released (see `DeferDumps`); otherwise written here, before a
    // panic can end the process, holding `dumps_in_flight` from before the
    // violation is logged, as a `DeferDumps` would.
    let _ = DEFERRED_DUMPS.try_with(|d| {
        if let Some(deferred) = d.borrow_mut().as_mut() {
            deferred.extend(dump.take());
        }
    });
    let _in_flight = dump
        .is_some()
        .then(|| dumps_in_flight().read().unwrap_or_else(|e| e.into_inner()));
    {
        let mut log = log();
        if log.entries.len() >= LOG_CAPACITY {
            if let Some(evicted) = log.entries.pop_front() {
                log.evicted_through = evicted.seq;
                log.dropped += 1;
            }
        }
        log.entries.push_back(violation);
    }
    if let Some((id, text)) = dump {
        crate::progress::trace::trigger(id, crate::progress::trace::Trigger::Checker, &text);
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
    /// Workers that sealed their progress stream, in the order they did.
    pub sealed_workers: Vec<usize>,
    /// Whether the end-state check ran (every worker succeeded).
    pub end_state_checked: bool,
    /// Why checking stopped early (cancellation or failure), if it did.
    pub halted: Option<String>,
    /// Violations found but not logged because the same kind was already
    /// logged several times at the same location in this dataflow. A lower
    /// bound: once a location's V1 or V7 frontier violations have been logged
    /// that often, they are no longer described.
    pub suppressed: u64,
}

fn finished_stats() -> MutexGuard<'static, std::collections::VecDeque<OracleStats>> {
    static FINISHED: OnceLock<Mutex<std::collections::VecDeque<OracleStats>>> = OnceLock::new();
    FINISHED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Statistics of every oracle created at or after `mark`, live or finished.
pub fn oracles_since(mark: u64) -> Vec<OracleStats> {
    let _settled = settle();
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

/// Where a violation is: an operator port, and whether it is an output.
type Site = (Loc, bool);

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
    /// `unary_async` input waiting for a task slot (at its output: the
    /// input is consumed, and covered by a capability).
    AsyncInput,
    /// An operator's `pending_output` (at its output).
    PendingOutput,
    /// Output a logic call emitted, until its operator shell takes it (at its
    /// output). Checked when a view publishes, not when the logic emits: the
    /// shell checks the call's output (V3) as it takes it, and output it
    /// takes is counted (and checked) in the retry queue or the channel.
    OutputSession,
    /// A retry envelope a tee has delivered to some of its branches (at each
    /// target input it has not reached yet, R46).
    PendingDelivery,
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
            Store::PendingOutput
                | Store::OutputSession
                | Store::AsyncInput
                | Store::AsyncTask
                | Store::Capability
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

    /// A logic call of the operator this port belongs to was unjustified
    /// (V3): production code fails the operator; the checker records it.
    pub(crate) fn unjustified(&self, detail: String) {
        if enabled() {
            self.sink.unjustified(self.worker, self.loc, detail);
        }
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
    /// A logic call at `loc` (an output) was unjustified (V3).
    fn unjustified(&self, worker: usize, loc: Loc, detail: String);
    fn coverage_gap(&self, worker: Option<usize>, what: String);
    fn stats(&self) -> OracleStats;
    /// Apply the queued events.
    fn drain(&self);
    /// Applying the queue panicked (a checker bug): halt and report it.
    fn failed(&self, what: String);
    /// Name the dataflow's operators (see [`Oracle::register_names`]).
    fn register_names_vec(&self, operators: Vec<(usize, String)>);
}

/// Creates (or finds) the oracle of one nested scope of a dataflow, in the
/// scope's own time: `(dataflow id, total workers)` to the oracle, as a sink
/// and as `Arc<Oracle<S>>`.
pub(crate) type ScopeOracleFn = Arc<
    dyn Fn(DataflowId, usize) -> Option<(Arc<dyn Sink>, Arc<dyn Any + Send + Sync>)> + Send + Sync,
>;

/// The [`ScopeOracleFn`] of nested scope `scope`, whose time is `S`.
pub(crate) fn scope_oracle_fn<S: Timestamp>(scope: usize) -> ScopeOracleFn {
    Arc::new(move |id, total_workers| {
        oracle_scoped::<S>(id, scope, total_workers).map(|oracle| {
            let sink: Arc<dyn Sink> = oracle.clone();
            let typed: Arc<dyn Any + Send + Sync> = oracle;
            (sink, typed)
        })
    })
}

/// Materialization context: the oracles and the worker being materialized.
///
/// Every progress scope has its own oracle in its own time (design §5.1.7):
/// the root scope's is `sink`, and each nested scope's (a loop body, in
/// `Product` time) is in `scopes`. A channel, store or tracker of a scope
/// reports to that scope's oracle, so loop-body work is judged against the
/// loop's own frontiers, never projected onto the outer time.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) worker: usize,
    id: Option<DataflowId>,
    total_workers: usize,
    typed: Arc<dyn Any + Send + Sync>,
    scopes: Arc<HashMap<usize, (Arc<dyn Sink>, Arc<dyn Any + Send + Sync>)>>,
}

impl Ctx {
    /// This context, with the oracle of every nested scope `scopes`
    /// describes. `names` names the dataflow's operators for them.
    pub(crate) fn with_scopes(
        mut self,
        scopes: &[(usize, ScopeOracleFn)],
        names: &[(usize, String)],
    ) -> Self {
        let Some(id) = self.id else {
            return self;
        };
        let mut map = HashMap::new();
        for (scope, make) in scopes {
            if let Some((sink, typed)) = make(id, self.total_workers) {
                sink.register_names_vec(names.to_vec());
                map.insert(*scope, (sink, typed));
            }
        }
        self.scopes = Arc::new(map);
        self
    }

    /// The sink of scope `scope` (0: the root). A scope without an oracle
    /// falls back to the root's, which reports the time mismatch as a
    /// coverage gap.
    pub(crate) fn sink_for(&self, scope: usize) -> Arc<dyn Sink> {
        if scope == 0 {
            return self.sink.clone();
        }
        match self.scopes.get(&scope) {
            Some((sink, _)) => sink.clone(),
            None => self.sink.clone(),
        }
    }

    /// This context with scope `scope`'s oracle as its sink.
    pub(crate) fn for_scope(&self, scope: usize) -> Ctx {
        Ctx {
            sink: self.sink_for(scope),
            ..self.clone()
        }
    }

    /// Scope `scope`'s oracle, if its time is `S`.
    pub(crate) fn typed_oracle<S: Timestamp>(&self, scope: usize) -> Option<Arc<Oracle<S>>> {
        let typed = if scope == 0 {
            self.typed.clone()
        } else {
            self.scopes.get(&scope)?.1.clone()
        };
        typed.downcast::<Oracle<S>>().ok()
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

fn registry() -> MutexGuard<'static, HashMap<(DataflowId, usize), Entry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<(DataflowId, usize), Entry>>> = OnceLock::new();
    REGISTRY
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// The oracle for dataflow `id`, created on first use. Every node of a
/// cluster in this process gets the same one. Returns `None` when checking
/// is off.
pub(crate) fn oracle<T: Timestamp>(id: DataflowId, total_workers: usize) -> Option<Arc<Oracle<T>>> {
    oracle_scoped(id, 0, total_workers)
}

/// The oracle of progress scope `scope` (0: the root) of dataflow `id`, in
/// the scope's time `T`, created on first use.
pub(crate) fn oracle_scoped<T: Timestamp>(
    id: DataflowId,
    scope: usize,
    total_workers: usize,
) -> Option<Arc<Oracle<T>>> {
    if !enabled() {
        return None;
    }
    let id_key = (id, scope);
    let mut registry = registry();
    registry.retain(|_, entry| entry.sink.strong_count() > 0);
    if let Some(existing) = registry.get(&id_key).and_then(|e| e.typed.upgrade()) {
        match existing.downcast::<Oracle<T>>() {
            Ok(oracle) if oracle.total_workers == total_workers => return Some(oracle),
            _ => {
                // Same id, different dataflow shape: keep them apart.
                let oracle = Arc::new(Oracle::<T>::new(Some(id), total_workers));
                let sink: Arc<dyn Sink> = oracle.clone();
                watch(&sink);
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
    watch(&sink);
    let typed: Arc<dyn Any + Send + Sync> = oracle.clone();
    registry.insert(
        id_key,
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

/// How many violations of one kind are logged per location and dataflow;
/// the rest are counted in [`OracleStats::suppressed`].
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
/// Every view is global: a multi-worker dataflow's trackers all receive every
/// worker's progress.
struct View<T: Timestamp> {
    worker: usize,
    label: &'static str,
    targets: LocMap<Antichain<T>>,
    sources: LocMap<Antichain<T>>,
    /// Publications with a non-empty frontier at a fully exhausted input,
    /// and when the first of them happened.
    stuck: LocMap<(u64, std::time::Instant)>,
    /// Whether all counted work has been checked against this view's
    /// frontiers. After that, new work is checked as it is counted, and a
    /// publication rescans only the work its changed frontiers constrain.
    scanned: bool,
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
    /// Violations logged, per kind and location.
    logged: HashMap<(Check, &'static str, Option<Site>), u64>,
    /// The same, for the per-record frontier checks (V1 `late-data` and V7
    /// `frontier-ahead-of-work`), keyed without the kind for speed.
    site_logged: SiteCounts,
    /// Operator names, for violation locations.
    names: HashMap<usize, String>,
    /// An empty event buffer, handed back to the queue (see `Oracle::sync`).
    spare: Vec<Event<T>>,
    stats: OracleStats,
}

type SiteCounts = HashMap<(Check, Site), u64, FastHash>;

/// Which per-record frontier violations may still be described: a location
/// whose violations were logged [`LOGGED_PER_KIND`] times is not described
/// again, and one check does not describe a location more often than that
/// either. A dataflow that breaks an invariant on every record (every loop
/// does today, audit O-9) would otherwise spend its time describing
/// violations nobody reads, while one location's violations never hide
/// another's.
struct Budget<'a> {
    logged: &'a SiteCounts,
    pending: SiteCounts,
}

impl<'a> Budget<'a> {
    fn new(logged: &'a SiteCounts) -> Self {
        Self {
            logged,
            pending: SiteCounts::default(),
        }
    }

    /// Whether one more violation of `check` at `site` may be described.
    fn take(&mut self, check: Check, site: Site) -> bool {
        let key = (check, site);
        let logged = self.logged.get(&key).copied().unwrap_or(0);
        if logged >= LOGGED_PER_KIND {
            return false;
        }
        let pending = self.pending.entry(key).or_insert(0);
        if logged + *pending >= LOGGED_PER_KIND {
            return false;
        }
        *pending += 1;
        true
    }
}

/// A violation found under the oracle's lock, recorded after releasing it.
struct Found {
    check: Check,
    kind: &'static str,
    worker: Option<usize>,
    site: Option<Site>,
    detail: String,
}

/// The kinds counted in [`State::site_logged`].
fn per_record(check: Check, kind: &'static str) -> bool {
    matches!(
        (check, kind),
        (Check::V1, "late-data") | (Check::V7, "frontier-ahead-of-work")
    )
}

/// Whether `check` described the location at `site` as often as it may.
fn saturated(logged: &SiteCounts, check: Check, site: Site) -> bool {
    logged
        .get(&(check, site))
        .is_some_and(|n| *n >= LOGGED_PER_KIND)
}

/// Whether every location work in `(store, loc)` is checked at (see
/// [`check_one`]) was described as often as it may, so that the work need
/// not be checked again. A location that breaks V7 on every record (every
/// loop's feedback input does today, audit O-9) would otherwise cost every
/// record a lookup in every view.
fn v7_saturated(
    logged: &SiteCounts,
    out_edges: &LocMap<BTreeSet<Loc>>,
    store: Store,
    loc: Loc,
) -> bool {
    match store {
        Store::Capability => saturated(logged, Check::V7, (loc, true)),
        s if s.at_output() => out_edges.get(&loc).is_some_and(|targets| {
            targets
                .iter()
                .all(|t| saturated(logged, Check::V7, (*t, false)))
        }),
        _ => saturated(logged, Check::V7, (loc, false)),
    }
}

/// Whether work in `(store, loc)` is checked against one of the given
/// frontiers (see [`check_one`]).
fn constrained_by(
    out_edges: &LocMap<BTreeSet<Loc>>,
    store: Store,
    loc: Loc,
    targets: &[Loc],
    sources: &[Loc],
) -> bool {
    match store {
        Store::Capability => sources.contains(&loc),
        s if s.at_output() => out_edges
            .get(&loc)
            .is_some_and(|edges| edges.iter().any(|t| targets.contains(t))),
        _ => targets.contains(&loc),
    }
}

/// `name[in i]` or `name[out i]`.
fn site_name(names: &HashMap<usize, String>, ((op, port), output): Site) -> String {
    let direction = if output { "out" } else { "in" };
    match names.get(&op) {
        Some(name) => format!("{name}[{direction} {port}]"),
        None => format!("op{op}[{direction} {port}]"),
    }
}

/// The V7 oracle for one dataflow, typed by its root timestamp.
///
/// Worker threads only append [`Event`]s to `queue` (a short critical
/// section); the background thread, or a reader, applies them to `state` in
/// queue order (see the module docs). Setup and every read of the state
/// apply the queue first.
pub(crate) struct Oracle<T: Timestamp> {
    id: Option<DataflowId>,
    total_workers: usize,
    queue: Mutex<Vec<Event<T>>>,
    state: Mutex<State<T>>,
}

/// One hook call, queued for the background thread (see [`Oracle`]).
enum Event<T: Timestamp> {
    Count {
        store: Store,
        loc: Loc,
        owner: Owner,
        time: T,
        diff: i64,
        worker: usize,
    },
    Consumed {
        loc: Loc,
        owner: Owner,
        time: T,
        n: i64,
        worker: usize,
    },
    Publish {
        view: u64,
        targets: Vec<(Loc, Antichain<T>)>,
        sources: Vec<(Loc, Antichain<T>)>,
        at: std::time::Instant,
    },
    Exhausted(Loc),
    WorkerFinished {
        worker: usize,
        ok: bool,
    },
    WorkerSealed(usize),
    Unjustified {
        worker: usize,
        loc: Loc,
        detail: String,
    },
    RetireView(u64),
    Halt(String),
}

/// Past this many queued events, the hook applies the queue itself (bounds
/// memory when the background thread falls behind).
const MAX_QUEUED: usize = 1 << 16;

/// How often the background thread applies the live oracles' queues.
const POLL_EVERY: std::time::Duration = std::time::Duration::from_millis(1);

/// The oracles the background thread applies queues for.
fn live() -> MutexGuard<'static, Vec<Weak<dyn Sink>>> {
    static LIVE: Mutex<Vec<Weak<dyn Sink>>> = Mutex::new(Vec::new());
    LIVE.lock().unwrap_or_else(|e| e.into_inner())
}

/// The background thread's name.
const CHECK_THREAD: &str = "instancy-progress-check";

/// Have the background thread apply `sink`'s queue (starting the thread on
/// first use).
fn watch(sink: &Arc<dyn Sink>) {
    static POLLER: OnceLock<()> = OnceLock::new();
    POLLER.get_or_init(|| {
        std::thread::Builder::new()
            .name(CHECK_THREAD.into())
            .spawn(poll)
            .expect("spawn the progress checker thread");
    });
    live().push(Arc::downgrade(sink));
}

/// The background thread: apply every live oracle's queue, then sleep.
fn poll() {
    loop {
        // In panic mode hooks apply the queue themselves, so that a
        // violation panics on the thread that caused it.
        if mode() != Mode::Panic {
            // Held while this pass holds oracles, so that an oracle this
            // pass drops last is finished before a reader looks. The
            // flight recorder's dumps are written once it is released (a
            // reader waits for them on `dumps_in_flight`).
            let pass = pass();
            let dumps = DeferDumps::new();
            let sinks: Vec<Arc<dyn Sink>> = {
                let mut live = live();
                live.retain(|sink| sink.strong_count() > 0);
                live.iter().filter_map(Weak::upgrade).collect()
            };
            for sink in sinks {
                // A bug in the checker must not stop it checking other
                // dataflows: the oracle halts and reports it instead.
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.drain()));
                if let Err(panic) = result {
                    let what = panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default();
                    sink.failed(what);
                }
            }
            drop(pass);
            drop(dumps);
        }
        std::thread::sleep(POLL_EVERY);
    }
}

/// Apply the queues of every live oracle (before reading the log). The
/// caller holds [`pass`].
fn drain_all() {
    let sinks: Vec<Arc<dyn Sink>> = live().iter().filter_map(Weak::upgrade).collect();
    for sink in sinks {
        sink.drain();
    }
}

/// Excludes the background thread's passes (see [`poll`]).
fn pass() -> MutexGuard<'static, ()> {
    static PASS: Mutex<()> = Mutex::new(());
    PASS.lock().unwrap_or_else(|e| e.into_inner())
}

/// An oracle's state lock, from [`Oracle::sync`]: released before the
/// flight recorder's dumps it deferred are written (fields drop in order).
struct Synced<'a, T: Timestamp> {
    state: MutexGuard<'a, State<T>>,
    _dumps: DeferDumps,
}

impl<T: Timestamp> std::ops::Deref for Synced<'_, T> {
    type Target = State<T>;

    fn deref(&self) -> &State<T> {
        &self.state
    }
}

impl<T: Timestamp> std::ops::DerefMut for Synced<'_, T> {
    fn deref_mut(&mut self) -> &mut State<T> {
        &mut self.state
    }
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
            queue: Mutex::new(Vec::new()),
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
                site_logged: SiteCounts::default(),
                names: HashMap::new(),
                spare: Vec::new(),
                stats,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queue an event (see [`Oracle`]).
    fn enqueue(&self, event: Event<T>) {
        let full = {
            let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            queue.push(event);
            queue.len() >= MAX_QUEUED
        };
        if full || mode() == Mode::Panic {
            self.drain();
        }
    }

    /// The state, with every queued event applied. The flight recorder's
    /// dumps a finding triggers meanwhile are written once it is released.
    fn sync(&self) -> Synced<'_, T> {
        let dumps = DeferDumps::new();
        let mut state = self.lock();
        loop {
            // The queue gets the previous batch's (empty) buffer back, so
            // hooks rarely grow it.
            let mut events = std::mem::take(&mut state.spare);
            std::mem::swap(
                &mut *self.queue.lock().unwrap_or_else(|e| e.into_inner()),
                &mut events,
            );
            if events.is_empty() {
                state.spare = events;
                return Synced {
                    state,
                    _dumps: dumps,
                };
            }
            let mut found = Vec::new();
            for event in events.drain(..) {
                self.apply(&mut state, event, &mut found);
            }
            state.spare = events;
            self.emit_locked(&mut state, found);
        }
    }

    /// Apply every queued event.
    fn drain(&self) {
        drop(self.sync());
    }

    fn apply(&self, state: &mut State<T>, event: Event<T>, found: &mut Vec<Found>) {
        match event {
            Event::Count {
                store,
                loc,
                owner,
                time,
                diff,
                worker,
            } => self.count_locked(state, store, loc, owner, &time, diff, worker, found),
            Event::Consumed {
                loc,
                owner,
                time,
                n,
                worker,
            } => self.consumed_locked(state, loc, owner, &time, n, worker, found),
            Event::Publish {
                view,
                targets,
                sources,
                at,
            } => self.publish_locked(state, view, targets, sources, at, found),
            Event::Exhausted(loc) => *state.exhausted.entry(loc).or_insert(0) += 1,
            Event::WorkerFinished { worker, ok } => {
                self.worker_finished_locked(state, worker, ok, found)
            }
            Event::WorkerSealed(worker) => self.worker_sealed_locked(state, worker, found),
            Event::Unjustified {
                worker,
                loc,
                detail,
            } => found.push(Found {
                check: Check::V3,
                kind: "unjustified",
                worker: Some(worker),
                site: Some((loc, true)),
                detail,
            }),
            Event::RetireView(view) => {
                state.views.remove(&view);
            }
            Event::Halt(reason) => {
                if state.halted.is_none() {
                    state.stats.halted = Some(reason.clone());
                    state.halted = Some(reason);
                }
            }
        }
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

    /// Record findings, at most [`LOGGED_PER_KIND`] per kind and location.
    fn emit(&self, found: Vec<Found>) {
        let mut state = self.sync();
        self.emit_locked(&mut state, found);
    }

    /// `emit` under the state lock. Recording takes only the log's lock; a
    /// flight recorder dump it triggers is written once the state lock is
    /// released (see [`DeferDumps`]).
    fn emit_locked(&self, state: &mut State<T>, found: Vec<Found>) {
        for f in found {
            let logged = state.logged.entry((f.check, f.kind, f.site)).or_insert(0);
            if *logged < LOGGED_PER_KIND {
                *logged += 1;
                if let Some(site) = f.site {
                    if per_record(f.check, f.kind) {
                        *state.site_logged.entry((f.check, site)).or_insert(0) += 1;
                    }
                }
                let location = f.site.map(|site| site_name(&state.names, site));
                record(f.check, f.kind, self.id, f.worker, location, f.detail);
            } else {
                state.stats.suppressed += 1;
            }
        }
    }

    /// Name the dataflow's operators (for violation locations). The first
    /// registration wins: every worker registers the same graph. Names that
    /// occur more than once get a `(k)` suffix, numbered in operator order.
    pub(crate) fn register_names(&self, operators: impl IntoIterator<Item = (usize, String)>) {
        let mut state = self.sync();
        if !state.names.is_empty() {
            return;
        }
        let mut operators: Vec<(usize, String)> = operators.into_iter().collect();
        operators.sort();
        let mut occurrences: HashMap<String, usize> = HashMap::new();
        for (_, name) in &operators {
            *occurrences.entry(name.clone()).or_insert(0) += 1;
        }
        let mut seen: HashMap<String, usize> = HashMap::new();
        for (index, name) in operators {
            let unique = if occurrences[&name] > 1 {
                let k = seen.entry(name.clone()).or_insert(0);
                *k += 1;
                format!("{name}({k})")
            } else {
                name
            };
            state.names.insert(index, unique);
        }
    }

    /// A context for materializing `worker`'s channels.
    pub(crate) fn ctx(self: &Arc<Self>, worker: usize) -> Ctx {
        let typed: Arc<dyn Any + Send + Sync> = self.clone();
        Ctx {
            sink: self.clone(),
            worker,
            id: self.id,
            total_workers: self.total_workers,
            typed,
            scopes: Arc::new(HashMap::new()),
        }
    }

    /// Register `worker` as one of this dataflow's workers.
    pub(crate) fn register_worker(&self, worker: usize) {
        let mut state = self.sync();
        state.registered.insert(worker);
        state.stats.registered_workers = state.registered.len();
    }

    /// Attach a worker's tracker; returns its view id.
    pub(crate) fn attach_view(&self, worker: usize, label: &'static str) -> u64 {
        let mut state = self.sync();
        let id = state.next_view;
        state.next_view += 1;
        state.views.insert(
            id,
            View {
                worker,
                label,
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
        self.enqueue(Event::RetireView(view));
    }

    /// Stop checking this dataflow (it was cancelled or failed).
    pub(crate) fn halt(&self, reason: &str) {
        self.enqueue(Event::Halt(reason.to_string()));
    }

    /// Publish a view's frontiers (those that changed) and check the view
    /// against all counted work (V7) and exhausted inputs (V4).
    pub(crate) fn publish(
        &self,
        view: u64,
        targets: impl IntoIterator<Item = (Loc, Antichain<T>)>,
        sources: impl IntoIterator<Item = (Loc, Antichain<T>)>,
    ) {
        self.enqueue(Event::Publish {
            view,
            targets: targets.into_iter().collect(),
            sources: sources.into_iter().collect(),
            at: std::time::Instant::now(),
        });
    }

    /// `publish`, applied (`at`: when it was published).
    #[allow(clippy::too_many_arguments)]
    fn publish_locked(
        &self,
        state: &mut State<T>,
        view: u64,
        targets: Vec<(Loc, Antichain<T>)>,
        sources: Vec<(Loc, Antichain<T>)>,
        at: std::time::Instant,
        found: &mut Vec<Found>,
    ) {
        let total_workers = self.total_workers;
        let Some(v) = state.views.get_mut(&view) else {
            return;
        };
        let mut changed_targets = Vec::new();
        for (loc, frontier) in targets {
            if v.targets.get(&loc) != Some(&frontier) {
                v.targets.insert(loc, frontier);
                changed_targets.push(loc);
            }
        }
        let mut changed_sources = Vec::new();
        for (loc, frontier) in sources {
            if v.sources.get(&loc) != Some(&frontier) {
                v.sources.insert(loc, frontier);
                changed_sources.push(loc);
            }
        }
        // Work is checked against every view when it is counted, so once all
        // work has been checked against this view, only work whose frontier
        // changed can have become a violation.
        let full = !v.scanned;
        let scan = full || !changed_targets.is_empty() || !changed_sources.is_empty();
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
            let mut budget = Budget::new(&state.site_logged);
            for ((store, loc, _), times) in &state.counts {
                if (!full
                    && !constrained_by(
                        &state.out_edges,
                        *store,
                        *loc,
                        &changed_targets,
                        &changed_sources,
                    ))
                    || v7_saturated(&state.site_logged, &state.out_edges, *store, *loc)
                {
                    continue;
                }
                for (time, &n) in times {
                    if n > 0 {
                        check_one(
                            v,
                            &state.out_edges,
                            &mut budget,
                            *store,
                            *loc,
                            time,
                            n,
                            found,
                        );
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
            // Records still at the input (in a channel or an input buffer, of
            // any worker: the location is shared) legitimately hold its
            // frontier; the grace starts once it is physically empty.
            let occupied: BTreeSet<Loc> = state
                .counts
                .iter()
                .filter(|((store, loc, _), times)| {
                    !store.at_output() && exhausted.contains(loc) && times.values().any(|n| *n != 0)
                })
                .map(|((_, loc, _), _)| *loc)
                .collect();
            let v = state.views.get_mut(&view).expect("view exists");
            for loc in exhausted {
                let Some(frontier) = v.targets.get(&loc) else {
                    continue;
                };
                if frontier.is_empty() || occupied.contains(&loc) {
                    v.stuck.remove(&loc);
                    continue;
                }
                let (count, since) = v.stuck.entry(loc).or_insert((0, at));
                if *count == u64::MAX {
                    // Already reported.
                    continue;
                }
                *count += 1;
                if *count >= EXHAUSTED_GRACE
                    && at.saturating_duration_since(*since) >= EXHAUSTED_GRACE_TIME
                {
                    *count = u64::MAX;
                    found.push(Found {
                        check: Check::V4,
                        kind: "exhausted-input-frontier",
                        worker: Some(v.worker),
                        site: Some((loc, false)),
                        detail: format!(
                            "every channel into input {loc:?} is exhausted and no record \
                             is left there, but the {} view still has frontier {:?} there \
                             after {EXHAUSTED_GRACE} propagations and \
                             {EXHAUSTED_GRACE_TIME:?}",
                            v.label,
                            frontier.elements()
                        ),
                    });
                }
            }
        }
    }

    /// A worker's executor finished (`ok`: it reports success). Its own
    /// stores must then be empty and it may hold no capability; when every
    /// worker has succeeded, the channels between workers must be empty too.
    pub(crate) fn worker_finished(&self, worker: usize, ok: bool) {
        self.enqueue(Event::WorkerFinished { worker, ok });
    }

    /// A worker sealed its progress stream: every operator it hosts is shut
    /// down or exhausted, so none may hold work (V4). Channels are not its own
    /// stores: records it sent may still be on their way.
    pub(crate) fn worker_sealed(&self, worker: usize) {
        self.enqueue(Event::WorkerSealed(worker));
    }

    /// `worker_sealed`, applied.
    fn worker_sealed_locked(&self, state: &mut State<T>, worker: usize, found: &mut Vec<Found>) {
        if !state.stats.sealed_workers.contains(&worker) {
            state.stats.sealed_workers.push(worker);
        }
        if state.halted.is_some() || self.partial(state) {
            return;
        }
        for ((store, loc, owner), times) in &state.counts {
            if *owner != Some(worker) || *store == Store::Channel {
                continue;
            }
            for (time, &n) in times {
                if n != 0 {
                    found.push(Found {
                        check: Check::V4,
                        kind: "sealed-holding-work",
                        worker: Some(worker),
                        site: Some((*loc, store.at_output())),
                        detail: format!(
                            "worker {worker} sealed its progress while {n} unit(s) of {store:?} \
                             at {loc:?}@{time:?} were still there"
                        ),
                    });
                }
            }
        }
    }

    /// `worker_finished`, applied.
    fn worker_finished_locked(
        &self,
        state: &mut State<T>,
        worker: usize,
        ok: bool,
        found: &mut Vec<Found>,
    ) {
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
        self.enqueue(Event::Count {
            store,
            loc,
            owner,
            time: time.clone(),
            diff,
            worker,
        });
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
                site: Some((loc, store.at_output())),
                detail: format!(
                    "{store:?} at {loc:?}@{time:?}{} went to {now}: work left a store \
                     without entering it, so some producer or store is not observed",
                    owner_suffix(owner)
                ),
            });
        }
        if diff > 0
            && now > 0
            && store != Store::OutputSession
            && !v7_saturated(&state.site_logged, &state.out_edges, store, loc)
        {
            let mut budget = Budget::new(&state.site_logged);
            for v in state.views.values() {
                check_one(
                    v,
                    &state.out_edges,
                    &mut budget,
                    store,
                    loc,
                    time,
                    now,
                    found,
                );
            }
        }
    }

    /// Typed `consumed_any`.
    fn consumed(&self, loc: Loc, owner: Owner, time: &T, n: i64, worker: usize) {
        self.enqueue(Event::Consumed {
            loc,
            owner,
            time: time.clone(),
            n,
            worker,
        });
    }

    /// `consumed`, applied.
    #[allow(clippy::too_many_arguments)]
    fn consumed_locked(
        &self,
        state: &mut State<T>,
        loc: Loc,
        owner: Owner,
        time: &T,
        n: i64,
        worker: usize,
        found: &mut Vec<Found>,
    ) {
        self.count_locked(state, Store::Channel, loc, owner, time, -n, worker, found);
        if state.halted.is_some() || saturated(&state.site_logged, Check::V1, (loc, false)) {
            return;
        }
        let mut budget = Budget::new(&state.site_logged);
        for v in state.views.values().filter(|v| v.worker == worker) {
            let Some(frontier) = v.targets.get(&loc) else {
                continue;
            };
            if !frontier.less_equal(time) && budget.take(Check::V1, (loc, false)) {
                found.push(Found {
                    check: Check::V1,
                    kind: "late-data",
                    worker: Some(worker),
                    site: Some((loc, false)),
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
        site: Some((loc, store.at_output())),
        detail: format!(
            "reported success with {n} unit(s) left in {store:?} at {loc:?}@{time:?}{}: {what}",
            owner_suffix(owner)
        ),
    }
}

/// Check one unit of work against one view (V7).
#[allow(clippy::too_many_arguments)]
fn check_one<T: Timestamp>(
    v: &View<T>,
    out_edges: &LocMap<BTreeSet<Loc>>,
    budget: &mut Budget<'_>,
    store: Store,
    loc: Loc,
    time: &T,
    n: i64,
    found: &mut Vec<Found>,
) {
    let mut fail = |site: Site, frontier: &Antichain<T>| {
        if !budget.take(Check::V7, site) {
            return;
        }
        let ((op, port), output) = site;
        let at = format!("{} ({op}, {port})", if output { "output" } else { "input" });
        found.push(Found {
            check: Check::V7,
            kind: "frontier-ahead-of-work",
            worker: Some(v.worker),
            site: Some(site),
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
                    fail((loc, true), frontier);
                }
            }
        }
        s if s.at_output() => {
            if let Some(targets) = out_edges.get(&loc) {
                for target in targets {
                    if let Some(frontier) = v.targets.get(target) {
                        if !frontier.less_equal(time) {
                            fail((*target, false), frontier);
                        }
                    }
                }
            }
        }
        _ => {
            if let Some(frontier) = v.targets.get(&loc) {
                if !frontier.less_equal(time) {
                    fail((loc, false), frontier);
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
        self.enqueue(Event::Exhausted(loc));
    }

    fn register_channel(&self, source: Loc, target: Loc) {
        let mut state = self.sync();
        if state.out_edges.entry(source).or_default().insert(target) {
            // Work already at `source` was never checked against `target`.
            for v in state.views.values_mut() {
                v.scanned = false;
            }
        }
        *state.consumers.entry(target).or_insert(0) += 1;
        state.stats.wrapped_channels += 1;
    }

    fn unjustified(&self, worker: usize, loc: Loc, detail: String) {
        self.enqueue(Event::Unjustified {
            worker,
            loc,
            detail,
        });
    }

    fn coverage_gap(&self, worker: Option<usize>, what: String) {
        self.lock().stats.coverage_gaps += 1;
        self.emit(vec![Found {
            check: Check::V7,
            kind: "coverage-gap",
            worker,
            site: None,
            detail: what,
        }]);
    }

    fn stats(&self) -> OracleStats {
        self.sync().stats.clone()
    }

    fn drain(&self) {
        Oracle::drain(self);
    }

    fn failed(&self, what: String) {
        let reason = format!("the checker failed: {what}");
        {
            let mut state = self.lock();
            if state.halted.is_none() {
                state.stats.halted = Some(reason.clone());
                state.halted = Some(reason.clone());
            }
        }
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).clear();
        record(Check::V7, "checker-failed", self.id, None, None, reason);
    }

    fn register_names_vec(&self, operators: Vec<(usize, String)>) {
        self.register_names(operators);
    }
}

impl<T: Timestamp> Drop for Oracle<T> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.drain();
        }
        let stats = self.lock().stats.clone();
        let mut finished = finished_stats();
        if finished.len() >= LOG_CAPACITY {
            finished.pop_front();
        }
        finished.push_back(stats);
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

    pub(crate) fn publish(
        &self,
        targets: impl IntoIterator<Item = (Loc, Antichain<T>)>,
        sources: impl IntoIterator<Item = (Loc, Antichain<T>)>,
    ) {
        self.oracle.publish(self.view, targets, sources);
    }

    pub(crate) fn halt(&self, reason: &str) {
        self.oracle.halt(reason);
    }

    /// The tracker's worker sealed its progress stream (see
    /// [`Oracle::worker_sealed`]).
    pub(crate) fn sealed(&self) {
        self.oracle.worker_sealed(self.worker);
    }

    /// The tracker's executor finished; `residual` lists the view's non-zero
    /// counts (V5 report), when they are final (empty otherwise).
    pub(crate) fn finish(&mut self, ok: bool, residual: Vec<String>, report_worker: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !residual.is_empty() {
            let total = residual.len();
            let shown: Vec<String> = residual.into_iter().take(8).collect();
            record(
                Check::V5,
                "residual",
                self.oracle.id,
                Some(self.worker),
                None,
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

impl<S: Timestamp, D: 'static, M: 'static> crate::dataflow::channels::pushpull::sealed::Sealed
    for CheckedPush<S, D, M>
{
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

    fn check_port(&self) -> Option<Port<S>> {
        Some(self.source.clone())
    }

    fn check_target(&self) -> Option<Port<S>> {
        Some(self.target.clone())
    }
}

/// Wraps the pull end of a materialized channel: a pulled envelope leaves
/// the channel (V1 is checked here) and the first exhaustion is reported.
pub(crate) struct CheckedPull<S: Timestamp, D, M> {
    inner: Box<dyn Pull<S, D, M>>,
    target: Port<S>,
    exhausted: std::sync::atomic::AtomicBool,
}

impl<S: Timestamp, D: 'static, M: 'static> crate::dataflow::channels::pushpull::sealed::Sealed
    for CheckedPull<S, D, M>
{
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
    /// The front envelope, if a tee delivered it to some branches: counted
    /// only at the target inputs it has not reached (R46).
    front: Option<Vec<StoreCheck<S>>>,
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
            front: None,
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
        match self.front.take() {
            Some(targets) => {
                if let Some((time, n)) = records(&envelope) {
                    for target in &targets {
                        target.remove(time, n);
                    }
                }
            }
            None => self.removed(&envelope),
        }
        Some(envelope)
    }

    /// Put back an envelope the push end refused. `unreached`: the target
    /// inputs it has not reached, if it reached some (see
    /// [`Push::check_unreached`]); it counts only there.
    pub(crate) fn push_front_refused(
        &mut self,
        envelope: Envelope<S, D, M>,
        unreached: Option<Vec<Port<S>>>,
    ) {
        match unreached {
            Some(ports) if self.check.is_some() => {
                let targets: Vec<StoreCheck<S>> = ports
                    .into_iter()
                    .map(|port| StoreCheck::new(port, Store::PendingDelivery))
                    .collect();
                if let Some((time, n)) = records(&envelope) {
                    for target in &targets {
                        target.add(time, n);
                    }
                }
                self.front = Some(targets);
                self.queue.push_front(envelope);
            }
            _ => self.push_front(envelope),
        }
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
        oracle.publish(view, [((2, 0), frontier(4))], []);
        assert!(
            violations_for(id).iter().all(|v| v.check.is_report()),
            "{:?}",
            violations_for(id)
        );
    }

    /// With every worker registered, the same counts are violations.
    #[test]
    fn oracle_with_every_worker_reports_negative_occupancy_and_late_frontier() {
        // Provokes violations on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        let view = oracle.attach_view(0, "worker tracker");
        oracle.count(Store::Channel, (2, 0), None, &0, -5, 0);
        oracle.count(Store::Channel, (2, 0), None, &1, 3, 0);
        oracle.publish(view, [((2, 0), frontier(4))], []);
        let kinds: Vec<_> = violations_for(id).iter().map(|v| v.kind).collect();
        assert!(kinds.contains(&"negative-occupancy"), "{kinds:?}");
        assert!(kinds.contains(&"frontier-ahead-of-work"), "{kinds:?}");
    }

    /// An exhausted input whose frontier never empties is reported once,
    /// however long it stays stuck.
    #[test]
    fn exhausted_input_is_reported_once() {
        // Provokes violations on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        oracle.exhausted((2, 0), 0);
        let view = oracle.attach_view(0, "worker tracker");
        for _ in 0..EXHAUSTED_GRACE + 10 {
            oracle.publish(view, [((2, 0), frontier(0))], []);
        }
        std::thread::sleep(EXHAUSTED_GRACE_TIME);
        for _ in 0..EXHAUSTED_GRACE + 10 {
            oracle.publish(view, [((2, 0), frontier(0))], []);
        }
        let reported = violations_for(id)
            .iter()
            .filter(|v| v.kind == "exhausted-input-frontier")
            .count();
        assert_eq!(reported, 1);
    }
    /// A location whose violations stop being described (it broke an
    /// invariant on every record) does not hide another location's.
    #[test]
    fn a_saturated_location_does_not_hide_another() {
        // Provokes violations on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        oracle.register_channel((2, 0), (3, 0));
        oracle.register_names([(1, "a".into()), (2, "b".into()), (3, "c".into())]);
        let view = oracle.attach_view(0, "worker tracker");
        // Every record into b is behind b's frontier.
        oracle.publish(view, [((2, 0), frontier(10)), ((3, 0), frontier(0))], []);
        for _ in 0..100 {
            oracle.count(Store::Channel, (2, 0), None, &1, 1, 0);
            oracle.consumed((2, 0), None, &1, 1, 0);
        }
        // Then one record behind c's frontier.
        oracle.publish(view, [((3, 0), frontier(10))], []);
        oracle.count(Store::Channel, (3, 0), None, &1, 1, 0);
        let logged = |check: Check, location: &str| {
            violations_for(id)
                .iter()
                .filter(|v| v.check == check && v.location.as_deref() == Some(location))
                .count() as u64
        };
        assert_eq!(logged(Check::V7, "b[in 0]"), LOGGED_PER_KIND);
        assert_eq!(logged(Check::V1, "b[in 0]"), LOGGED_PER_KIND);
        assert_eq!(logged(Check::V7, "c[in 0]"), 1);
    }

    /// Work held while frontiers stay put is found once a frontier it is
    /// checked against advances past it, whichever store holds it.
    #[test]
    fn a_frontier_advancing_past_held_work_is_found() {
        // Provokes violations on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        oracle.register_channel((1, 0), (3, 0));
        oracle.register_names([(1, "a".into()), (2, "b".into()), (3, "c".into())]);
        let view = oracle.attach_view(0, "worker tracker");
        let publish = |b: u64, c: u64, a: u64| {
            oracle.publish(
                view,
                [((2, 0), frontier(b)), ((3, 0), frontier(c))],
                [((1, 0), frontier(a))],
            );
        };
        publish(0, 0, 0);
        oracle.count(Store::InputBuffer, (2, 0), Some(0), &1, 1, 0);
        oracle.count(Store::PendingOutput, (1, 0), Some(0), &1, 1, 0);
        oracle.count(Store::Capability, (1, 0), Some(0), &1, 1, 0);
        let locations = || -> Vec<String> {
            let locations: BTreeSet<String> = violations_for(id)
                .iter()
                .filter_map(|v| v.location.clone())
                .collect();
            locations.into_iter().collect()
        };
        publish(0, 0, 0);
        assert!(locations().is_empty(), "{:?}", locations());
        // c's frontier passes the pending output bound for it.
        publish(0, 5, 0);
        assert_eq!(locations(), ["c[in 0]"]);
        // b's passes the buffered input and the pending output.
        publish(5, 5, 0);
        assert_eq!(locations(), ["b[in 0]", "c[in 0]"]);
        // a's output frontier passes its capability.
        publish(5, 5, 5);
        assert_eq!(locations(), ["a[out 0]", "b[in 0]", "c[in 0]"]);
    }

    /// Operator names that occur more than once are numbered in operator
    /// order; an unnamed operator is named by its index.
    #[test]
    fn violation_locations_name_operators() {
        // Provokes violations on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_names([(7, "x".into()), (3, "x".into()), (5, "y".into())]);
        let view = oracle.attach_view(0, "worker tracker");
        oracle.publish(
            view,
            [
                ((3, 0), frontier(5)),
                ((7, 1), frontier(5)),
                ((9, 0), frontier(5)),
            ],
            [((5, 0), frontier(5))],
        );
        for loc in [(3, 0), (7, 1), (9, 0)] {
            oracle.count(Store::Channel, loc, None, &1, 1, 0);
        }
        oracle.count(Store::Capability, (5, 0), Some(0), &1, 1, 0);
        let locations: BTreeSet<String> = violations_for(id)
            .into_iter()
            .filter_map(|v| v.location)
            .collect();
        let expected: BTreeSet<String> = ["x(1)[in 0]", "x(2)[in 1]", "op9[in 0]", "y[out 0]"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(locations, expected);
    }

    /// A worker that seals while one of its own operators still holds work
    /// is a V4 violation. Records it sent are not its store: they may still
    /// be on their way.
    #[test]
    fn sealing_with_work_held_is_found() {
        // Provokes a violation on purpose.
        let _expect = expect_violations();
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        oracle.register_channel((1, 0), (2, 0));
        oracle.count(Store::Channel, (2, 0), Some(0), &0, 3, 0);
        oracle.worker_sealed(0);
        assert!(violations_for(id).is_empty(), "{:?}", violations_for(id));
        oracle.count(Store::OutputSession, (1, 0), Some(0), &0, 1, 0);
        oracle.worker_sealed(0);
        let sealed: Vec<Violation> = violations_for(id)
            .into_iter()
            .filter(|v| v.check == Check::V4 && v.kind == "sealed-holding-work")
            .collect();
        assert_eq!(sealed.len(), 1, "{sealed:?}");
    }

    /// A released capability stays counted until a commit drains its
    /// release from the port's mailbox (its reporter).
    #[test]
    fn a_release_counts_until_a_commit_drains_it() {
        use crate::progress::capability::Capability;
        use crate::progress::operate::ProgressReporter;
        let id = DataflowId::new();
        let oracle = oracle::<u64>(id, 1).expect("checking is on in tests");
        oracle.register_worker(0);
        let sink: Arc<dyn Sink> = oracle.clone();
        let reporter = ProgressReporter::<u64>::new();
        reporter.set_check_port(Port::new(sink, 0, (1, 0), None));
        let held = |oracle: &Oracle<u64>| -> i64 {
            oracle
                .sync()
                .counts
                .get(&(Store::Capability, (1, 0), Some(0)))
                .map(|times| times.values().sum())
                .unwrap_or(0)
        };
        let capability = Capability::new(4, reporter.clone());
        assert_eq!(held(&oracle), 1);
        drop(capability);
        assert_eq!(held(&oracle), 1, "released, but not committed yet");
        let _ = reporter.drain();
        assert_eq!(held(&oracle), 0);
    }
}
