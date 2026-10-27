//! Progress trace recorder: a flight recorder for progress failures.
//!
//! A progress failure that shows up once in a CI run (a data-lost error, a
//! proven residual, a checker finding) rarely reproduces locally. The
//! recorder keeps the recent history of every worker and every network
//! bridge of a dataflow in bounded rings, and writes it out when such a
//! failure is detected, so one CI artifact is enough to tell what happened.
//!
//! It is compiled only into builds with `debug_assertions`, the
//! `progress-check` feature or the `test-utils` feature. Release builds
//! without those features contain none of it. Even where it is compiled,
//! it records only when a destination is configured
//! (`INSTANCY_TRACE_DIR`, or [`set_dir`]); otherwise nothing is allocated or
//! recorded.
//!
//! # What is recorded
//!
//! One ring per worker (written only by that worker's executor, from the
//! thread polling it) and one per network peer of each node (written by the
//! peer's receive bridges and by the node's lifecycle coordinator). Events
//! are fixed-size records of atomics; they are formatted only when dumped.
//! A worker's ring holds its commits (every change), the peer batches it
//! applied (every change, with the lane position), seals, the decision
//! module's decisions (with every non-zero raw count in scope), operator
//! shutdowns (S2a), its exit and how it was reported, and its data path:
//! output counted as produced, pushes and their outcomes, network sends and
//! receives, and input pulls. A bridge ring holds the frames received from
//! the peer, the progress markers, link events and the link's retirement.
//!
//! Each ring holds `INSTANCY_TRACE_EVENTS` events (default 16,384, about
//! 1 MiB); older events are overwritten, and a dump says how many.
//!
//! # Dumps
//!
//! A dump is written, at most once per dataflow and trigger kind, when:
//!
//! - a push finds records lost on a retired link (`data-lost`);
//! - a receive bridge or the coordinator reports records that arrived for a
//!   finished operator (`inbound-data-lost`);
//! - a worker proves a residual (`residual`);
//! - the progress checker records a finding (`checker`);
//! - a test calls [`dump_traces`] (`explicit`).
//!
//! It goes to `$INSTANCY_TRACE_DIR/<dataflow-id>/` (a later dump of another
//! kind to a subdirectory `<n>-<kind>/` of it), with `header.json` (the dump's
//! header: reason, every ring's counters and every recording gap),
//! `reason.txt`, and one JSON-lines file per ring: `<node>-w<worker>.jsonl`
//! and `<node>-bridge-<peer>.jsonl`. Each file starts with its ring's header
//! line. A dump covers every ring of the dataflow in this process: every
//! node of a cluster that runs in one process, as in tests. A cluster whose
//! nodes run in separate processes dumps each process's own rings. Writing
//! a dump never panics; I/O errors are ignored. A dump is in addition to the
//! failure that triggered it, never instead of it.
//!
//! The rings of a finished dataflow stay available for a while (the last
//! 32 finished dataflows), so a test harness can dump them after the run.
//!
//! # Gaps
//!
//! Everything the dump cannot show is counted in its header: events
//! overwritten in a ring, slots found torn while dumping, events at a site
//! that ran outside any traced worker (such as close sentinels sent when an
//! executor is dropped), commits and markers a retired link dropped, and
//! finished dataflows evicted from the registry.

// Links, bridges and network channels exist only with the `transport`
// feature; so do the events about them.
#![cfg_attr(not(feature = "transport"), allow(dead_code))]

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use crate::dataflow::id::DataflowId;
use crate::order::Product;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Events per ring unless `INSTANCY_TRACE_EVENTS` says otherwise.
pub const DEFAULT_EVENTS: usize = 16_384;

/// Slots allocated together (on first use).
const CHUNK: usize = 512;

/// Finished dataflows whose rings stay available for a dump.
const RETAINED_FINISHED: usize = 32;

/// 0: not read yet; 1: off; 2: on.
static ENABLED: AtomicU8 = AtomicU8::new(0);

fn config() -> MutexGuard<'static, Option<Option<PathBuf>>> {
    static CONFIG: OnceLock<Mutex<Option<Option<PathBuf>>>> = OnceLock::new();
    CONFIG
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn dir_from_env() -> Option<PathBuf> {
    std::env::var_os("INSTANCY_TRACE_DIR")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

/// Where dumps go: `INSTANCY_TRACE_DIR`, unless [`set_dir`] overrode it.
/// `None`: nothing is recorded or dumped.
pub fn dir() -> Option<PathBuf> {
    config().get_or_insert_with(dir_from_env).clone()
}

/// Override the dump destination for the whole process (`None`: record and
/// dump nothing). Dataflows started before the call keep recording as they
/// did.
pub fn set_dir(dir: Option<PathBuf>) {
    let on = dir.is_some();
    *config() = Some(dir);
    ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
}

/// Whether a destination is configured (recording is on).
pub(crate) fn enabled() -> bool {
    match ENABLED.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = dir().is_some();
            let _ = ENABLED.compare_exchange(
                0,
                if on { 2 } else { 1 },
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            on
        }
    }
}

/// Events per ring: `INSTANCY_TRACE_EVENTS`, rounded up to whole chunks.
fn ring_events() -> usize {
    static EVENTS: OnceLock<usize> = OnceLock::new();
    *EVENTS.get_or_init(|| {
        std::env::var("INSTANCY_TRACE_EVENTS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_EVENTS)
            .clamp(CHUNK, 1 << 24)
            .div_ceil(CHUNK)
            * CHUNK
    })
}

/// The process's trace clock: its first use. Every ring measures `t_ns`
/// from it, on the monotonic clock (`Instant`).
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Nanoseconds since [`epoch`].
fn now_ns() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Times
// ---------------------------------------------------------------------------

const T_NONE: u8 = 0;
const T_UNIT: u8 = 1;
const T_UNSIGNED: u8 = 2;
const T_SIGNED: u8 = 3;
const T_PAIR: u8 = 4;
const T_NESTED: u8 = 5;
const T_TEXT: u8 = 6;

/// A timestamp in two words: integers and the usual `Product`s exactly,
/// anything else as the first 16 bytes of its `Debug` form.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimeRepr {
    tag: u8,
    a: u64,
    b: u64,
}

/// Truncating `fmt::Write` into 16 bytes.
struct Fixed {
    buf: [u8; 16],
    len: usize,
}

impl fmt::Write for Fixed {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for &byte in s.as_bytes() {
            if self.len == self.buf.len() {
                return Err(fmt::Error);
            }
            self.buf[self.len] = byte;
            self.len += 1;
        }
        Ok(())
    }
}

/// Encode a timestamp (see [`TimeRepr`]).
pub(crate) fn time<T: Any + fmt::Debug>(t: &T) -> TimeRepr {
    let any = t as &dyn Any;
    macro_rules! scalar {
        ($($ty:ty => $tag:expr),*) => {$(
            if let Some(v) = any.downcast_ref::<$ty>() {
                return TimeRepr { tag: $tag, a: *v as u64, b: 0 };
            }
        )*};
    }
    scalar!(u64 => T_UNSIGNED, usize => T_UNSIGNED, u32 => T_UNSIGNED,
            i64 => T_SIGNED, i32 => T_SIGNED);
    if any.is::<()>() {
        return TimeRepr {
            tag: T_UNIT,
            a: 0,
            b: 0,
        };
    }
    macro_rules! pair {
        ($($o:ty, $i:ty);*) => {$(
            if let Some(p) = any.downcast_ref::<Product<$o, $i>>() {
                return TimeRepr { tag: T_PAIR, a: p.outer as u64, b: p.inner as u64 };
            }
        )*};
    }
    pair!(u64, u64; u64, u32; u32, u32; u64, usize; usize, usize; u32, u64; usize, u64);
    macro_rules! nested {
        ($($o:ty, $m:ty, $i:ty);*) => {$(
            if let Some(p) = any.downcast_ref::<Product<Product<$o, $m>, $i>>() {
                let (mid, inner) = (p.outer.inner as u64, p.inner as u64);
                if mid <= u32::MAX as u64 && inner <= u32::MAX as u64 {
                    return TimeRepr { tag: T_NESTED, a: p.outer.outer as u64, b: mid << 32 | inner };
                }
            }
        )*};
    }
    nested!(u64, u32, u32; u64, u64, u64; u64, u32, u64; u64, u64, u32);
    let mut fixed = Fixed {
        buf: [0; 16],
        len: 0,
    };
    let _ = write!(fixed, "{t:?}");
    TimeRepr {
        tag: T_TEXT,
        a: u64::from_le_bytes(fixed.buf[..8].try_into().unwrap_or_default()),
        b: u64::from_le_bytes(fixed.buf[8..].try_into().unwrap_or_default()),
    }
}

fn render_time(tag: u8, a: u64, b: u64, out: &mut String) {
    match tag {
        T_UNIT => out.push_str("\"()\""),
        T_UNSIGNED => {
            let _ = write!(out, "{a}");
        }
        T_SIGNED => {
            let _ = write!(out, "{}", a as i64);
        }
        T_PAIR => {
            let _ = write!(out, "[{a},{b}]");
        }
        T_NESTED => {
            let _ = write!(out, "[[{a},{}],{}]", b >> 32, b & 0xffff_ffff);
        }
        T_TEXT => {
            let mut bytes = a.to_le_bytes().to_vec();
            bytes.extend_from_slice(&b.to_le_bytes());
            while bytes.last() == Some(&0) {
                bytes.pop();
            }
            json_str(&String::from_utf8_lossy(&bytes), out);
        }
        _ => out.push_str("null"),
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// What an event is (see [`render`] for each kind's fields).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    /// One change of this worker's commit.
    CommitChange = 1,
    /// A commit, after it was sent to every peer.
    Commit = 2,
    /// A network lane dropped a commit or a marker.
    LaneDropped = 3,
    /// One change of a peer batch this worker applied.
    BatchChange = 4,
    /// A peer batch this worker applied (a commit, or a fragment of one).
    BatchApplied = 5,
    /// A peer lane's state changed (sealed or closed).
    LaneState = 6,
    /// This worker sealed its progress stream.
    SealSent = 7,
    /// A decision of the decision module.
    Decision = 8,
    /// A non-zero raw count in scope at the last decision.
    DecisionCount = 9,
    /// A peer lane's position at the last decision.
    DecisionLane = 10,
    /// An operator shut down (S2a).
    ShutDown = 11,
    /// The executor returned.
    Exit = 12,
    /// The worker's result was reported to the cluster coordinator.
    Reported = 13,
    /// Free text (an error message), in pieces.
    Note = 14,
    /// Output a logic call emitted, counted as produced.
    OutputTaken = 15,
    /// The output port pushed into its channel.
    OutputPush = 16,
    /// An exchange or broadcast push handed a target its part.
    TargetPush = 17,
    /// A network push sent records to a peer.
    NetSend = 18,
    /// A network pull received from a peer.
    NetRecv = 19,
    /// An exchange pull took records from a source worker.
    SourcePull = 20,
    /// An operator input pulled records from its channel.
    InputPull = 21,
    /// A data frame from the peer reached its receive bridge.
    FrameIn = 22,
    /// A progress frame from the peer reached its receive bridge.
    ProgressIn = 23,
    /// A link event the coordinator processed.
    Link = 24,
    /// The link to the peer was retired.
    Retired = 25,
    /// The coordinator acted on the link.
    Coordinator = 26,
    /// An initial capability seeded at the tracker's initialization.
    Seed = 27,
    /// A location in the worker's scope (S2b), at the tracker's
    /// initialization.
    Scope = 28,
}

impl Kind {
    fn from_u8(v: u8) -> Option<Kind> {
        use Kind::*;
        Some(match v {
            1 => CommitChange,
            2 => Commit,
            3 => LaneDropped,
            4 => BatchChange,
            5 => BatchApplied,
            6 => LaneState,
            7 => SealSent,
            8 => Decision,
            9 => DecisionCount,
            10 => DecisionLane,
            11 => ShutDown,
            12 => Exit,
            13 => Reported,
            14 => Note,
            15 => OutputTaken,
            16 => OutputPush,
            17 => TargetPush,
            18 => NetSend,
            19 => NetRecv,
            20 => SourcePull,
            21 => InputPull,
            22 => FrameIn,
            23 => ProgressIn,
            24 => Link,
            25 => Retired,
            26 => Coordinator,
            27 => Seed,
            28 => Scope,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        use Kind::*;
        match self {
            CommitChange => "commit-change",
            Commit => "commit",
            LaneDropped => "lane-dropped",
            BatchChange => "batch-change",
            BatchApplied => "batch-applied",
            LaneState => "lane-state",
            SealSent => "seal-sent",
            Decision => "decision",
            DecisionCount => "decision-count",
            DecisionLane => "decision-lane",
            ShutDown => "shut-down",
            Exit => "exit",
            Reported => "reported",
            Note => "note",
            OutputTaken => "output-taken",
            OutputPush => "output-push",
            TargetPush => "target-push",
            NetSend => "net-send",
            NetRecv => "net-recv",
            SourcePull => "source-pull",
            InputPull => "input-pull",
            FrameIn => "frame-in",
            ProgressIn => "progress-in",
            Link => "link",
            Retired => "retired",
            Coordinator => "coordinator",
            Seed => "seed",
            Scope => "scope",
        }
    }
}

/// One event: a kind, a time tag, two small fields and five words. The
/// recorder adds the timestamp.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Ev {
    kind: Kind,
    tag: u8,
    a: u16,
    b: u32,
    w: [u64; 5],
}

impl Ev {
    #[inline(always)]
    pub(crate) fn new(kind: Kind) -> Self {
        Self {
            kind,
            tag: T_NONE,
            a: 0,
            b: 0,
            w: [0; 5],
        }
    }

    #[inline(always)]
    pub(crate) fn a(mut self, a: usize) -> Self {
        self.a = a.min(u16::MAX as usize) as u16;
        self
    }

    #[inline(always)]
    pub(crate) fn b(mut self, b: usize) -> Self {
        self.b = b.min(u32::MAX as usize) as u32;
        self
    }

    /// Word `i` (0..5); a timed event uses words 0 and 1 for the time.
    #[inline(always)]
    pub(crate) fn w(mut self, i: usize, v: u64) -> Self {
        self.w[i] = v;
        self
    }

    #[inline(always)]
    pub(crate) fn time(mut self, t: TimeRepr) -> Self {
        self.tag = t.tag;
        self.w[0] = t.a;
        self.w[1] = t.b;
        self
    }

    #[inline(always)]
    fn header(&self) -> u64 {
        self.kind as u64 | (self.tag as u64) << 8 | (self.a as u64) << 16 | (self.b as u64) << 32
    }
}

/// A progress change, scope-tagged (see the change kinds in [`render`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Change {
    pub(crate) scope: usize,
    pub(crate) source: bool,
    pub(crate) node: usize,
    pub(crate) port: usize,
    pub(crate) time: TimeRepr,
    pub(crate) diff: i64,
}

impl Change {
    pub(crate) fn of<T: Any + fmt::Debug>(
        scope: usize,
        kind: crate::progress::progress_channel::ProgressKind,
        node: usize,
        port: usize,
        t: &T,
        diff: i64,
    ) -> Self {
        Self {
            scope,
            source: kind == crate::progress::progress_channel::ProgressKind::Source,
            node,
            port,
            time: time(t),
            diff,
        }
    }

    /// The event: `kind` is a change kind, `from` the worker it came from,
    /// `reference` its commit or lane position.
    pub(crate) fn ev(&self, kind: Kind, from: usize, reference: u64) -> Ev {
        Ev::new(kind)
            .a(self.scope)
            .b(from)
            .time(self.time)
            .w(
                2,
                (self.node as u64 & 0xffff_ffff)
                    | (self.port as u64 & 0xffff) << 32
                    | (self.source as u64) << 48,
            )
            .w(3, self.diff as u64)
            .w(4, reference)
    }
}

/// The event of a location in a worker's scope: progress scope `scope`,
/// an output (`source`) or input `port` of node `node`.
pub(crate) fn scope_location(scope: usize, source: bool, node: usize, port: usize) -> Ev {
    Ev::new(Kind::Scope).a(scope).w(
        2,
        (node as u64 & 0xffff_ffff) | (port as u64 & 0xffff) << 32 | (source as u64) << 48,
    )
}

/// How a push ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Outcome {
    Accepted = 0,
    Backpressure = 1,
    Closed = 2,
    /// Records refused: the peer had already finished (retired link).
    LostAfterFinish = 3,
    /// Records enqueued, then the link was found retired.
    LostWhileFinishing = 4,
    Failed = 5,
}

impl Outcome {
    pub(crate) fn of_error(error: &crate::error::Error) -> Self {
        match error {
            crate::error::Error::Backpressure => Outcome::Backpressure,
            crate::error::Error::ChannelClosed => Outcome::Closed,
            #[cfg(feature = "transport")]
            crate::error::Error::Communication(crate::error::CommunicationError::DataLoss {
                detail,
                ..
            }) => {
                if detail.contains("while the peer was finishing") {
                    Outcome::LostWhileFinishing
                } else {
                    Outcome::LostAfterFinish
                }
            }
            _ => Outcome::Failed,
        }
    }

    pub(crate) fn of<T>(result: &Result<T, crate::error::Error>) -> Self {
        match result {
            Ok(_) => Outcome::Accepted,
            Err(e) => Self::of_error(e),
        }
    }

    fn name(v: u64) -> &'static str {
        match v {
            0 => "accepted",
            1 => "backpressure",
            2 => "closed",
            3 => "lost-after-finish",
            4 => "lost-while-finishing",
            _ => "failed",
        }
    }
}

// ---------------------------------------------------------------------------
// Rings
// ---------------------------------------------------------------------------

/// One slot: a sequence word (odd while written) and the event.
#[derive(Default)]
struct Slot {
    seq: AtomicU64,
    w: [AtomicU64; 7],
}

/// Whose ring it is.
#[derive(Clone, Debug)]
enum Who {
    Worker(usize),
    Bridge(String),
}

/// A bounded ring of events. Writers claim a slot with one atomic add and
/// publish it with a sequence word, so readers (a dump) never block them
/// and detect a slot that was being written (torn) instead of reading it.
struct Ring {
    node: String,
    who: Who,
    total_workers: usize,
    capacity: u64,
    /// The process's trace clock (see [`epoch`]).
    epoch: Instant,
    next: AtomicU64,
    chunks: Box<[OnceLock<Box<[Slot]>>]>,
    /// Commits and markers a retired link dropped (a worker's ring).
    lane_drops: AtomicU64,
}

/// What a dump read from a ring.
struct Snapshot {
    events: Vec<(u64, [u64; 7])>,
    written: u64,
    torn: u64,
    /// When the ring was read: it holds every event recorded before then
    /// (or counts it as overwritten or torn).
    at_ns: u64,
}

impl Ring {
    fn new(node: &str, who: Who, total_workers: usize) -> Self {
        let events = ring_events();
        Self {
            node: node.to_string(),
            who,
            total_workers,
            capacity: events as u64,
            epoch: epoch(),
            next: AtomicU64::new(0),
            chunks: (0..events / CHUNK).map(|_| OnceLock::new()).collect(),
            lane_drops: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn push(&self, ev: &Ev) {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        let pos = (idx % self.capacity) as usize;
        let chunk = match self.chunks[pos / CHUNK].get() {
            Some(chunk) => chunk,
            None => self.chunks[pos / CHUNK]
                .get_or_init(|| (0..CHUNK).map(|_| Slot::default()).collect()),
        };
        let slot = &chunk[pos % CHUNK];
        let t_ns = self.epoch.elapsed().as_nanos() as u64;
        slot.seq.store(2 * idx + 1, Ordering::Relaxed);
        fence(Ordering::Release);
        slot.w[0].store(ev.header(), Ordering::Relaxed);
        slot.w[1].store(t_ns, Ordering::Relaxed);
        slot.w[2].store(ev.w[0], Ordering::Relaxed);
        slot.w[3].store(ev.w[1], Ordering::Relaxed);
        slot.w[4].store(ev.w[2], Ordering::Relaxed);
        slot.w[5].store(ev.w[3], Ordering::Relaxed);
        slot.w[6].store(ev.w[4], Ordering::Relaxed);
        slot.seq.store(2 * idx + 2, Ordering::Release);
    }

    fn snapshot(&self) -> Snapshot {
        // Before the slots are claimed: an event timed before this was
        // claimed before it too.
        let at_ns = self.epoch.elapsed().as_nanos() as u64;
        let written = self.next.load(Ordering::Acquire);
        let start = written.saturating_sub(self.capacity);
        let mut events = Vec::with_capacity((written - start) as usize);
        let mut torn = 0;
        for idx in start..written {
            let pos = (idx % self.capacity) as usize;
            let Some(chunk) = self.chunks[pos / CHUNK].get() else {
                torn += 1;
                continue;
            };
            let slot = &chunk[pos % CHUNK];
            let before = slot.seq.load(Ordering::Acquire);
            let mut words = [0u64; 7];
            for (dst, src) in words.iter_mut().zip(&slot.w) {
                *dst = src.load(Ordering::Relaxed);
            }
            fence(Ordering::Acquire);
            let after = slot.seq.load(Ordering::Relaxed);
            if before != 2 * idx + 2 || after != before {
                torn += 1;
                continue;
            }
            events.push((idx, words));
        }
        Snapshot {
            events,
            written,
            torn,
            at_ns,
        }
    }

    fn file_stem(&self) -> String {
        let node = sanitize(&self.node);
        match &self.who {
            Who::Worker(w) => format!("{node}-w{w}"),
            Who::Bridge(peer) => format!("{node}-bridge-{}", sanitize(peer)),
        }
    }
}

fn sanitize(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() { "_".into() } else { s }
}

// ---------------------------------------------------------------------------
// Registry: every ring of a dataflow, by id
// ---------------------------------------------------------------------------

/// Why a dump was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trigger {
    DataLost,
    InboundDataLost,
    Residual,
    /// A finding of the progress checker (compiled in only with it).
    #[cfg_attr(
        not(any(debug_assertions, feature = "progress-check")),
        allow(dead_code)
    )]
    Checker,
    Explicit,
    /// The dataflow ended after an earlier dump: every ring's final state.
    Final,
}

impl Trigger {
    fn name(self) -> &'static str {
        match self {
            Trigger::DataLost => "data-lost",
            Trigger::InboundDataLost => "inbound-data-lost",
            Trigger::Residual => "residual",
            Trigger::Checker => "checker",
            Trigger::Explicit => "explicit",
            Trigger::Final => "final",
        }
    }
}

#[derive(Default)]
struct Dumps {
    /// Each trigger kind dumped, with the directory its dump went to
    /// (`None`: it could not be written).
    kinds: Vec<(&'static str, Option<PathBuf>)>,
}

/// One dataflow's rings.
struct Entry {
    id: DataflowId,
    /// Live recorders (rings' owners).
    owners: AtomicUsize,
    rings: Mutex<Vec<Arc<Ring>>>,
    graph: Mutex<Graph>,
    dumps: Mutex<Dumps>,
    /// Where this dataflow dumps instead of the process's destination (unit
    /// tests only).
    dir: Option<PathBuf>,
}

/// What a dump needs to name locations and decode channel ids.
#[derive(Default)]
struct Graph {
    /// Operator index to name.
    names: BTreeMap<usize, String>,
    /// Edge index to `(source operator, port, target operator, port)`.
    edges: BTreeMap<usize, (usize, usize, usize, usize)>,
    /// Exchange edges in network channel ids: ordinal to edge index.
    exchanges: BTreeMap<usize, usize>,
}

#[derive(Default)]
struct Registry {
    entries: HashMap<DataflowId, Arc<Entry>>,
    finished: VecDeque<DataflowId>,
    evicted: u64,
}

fn registry() -> MutexGuard<'static, Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A ring's owner: records into it. The dataflow's rings stay registered
/// while any recorder is alive, then among the last finished dataflows.
pub(crate) struct Recorder {
    ring: Arc<Ring>,
    entry: Arc<Entry>,
    /// Dumps deferred until this ring's worker has returned its failure
    /// (see [`Recorder::defer`]): at most one per trigger kind.
    pending: Mutex<Vec<(Trigger, String)>>,
}

impl fmt::Debug for Recorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recorder")
            .field("dataflow", &self.entry.id)
            .field("ring", &self.ring.file_stem())
            .finish()
    }
}

fn attach(id: DataflowId, node: &str, who: Who, total_workers: usize) -> Option<Arc<Recorder>> {
    if !enabled() {
        return None;
    }
    Some(register(id, node, who, total_workers, None))
}

fn register(
    id: DataflowId,
    node: &str,
    who: Who,
    total_workers: usize,
    dir: Option<PathBuf>,
) -> Arc<Recorder> {
    let entry = {
        let mut registry = registry();
        let entry = registry
            .entries
            .entry(id)
            .or_insert_with(|| {
                Arc::new(Entry {
                    id,
                    owners: AtomicUsize::new(0),
                    rings: Mutex::new(Vec::new()),
                    graph: Mutex::new(Graph::default()),
                    dumps: Mutex::new(Dumps::default()),
                    dir,
                })
            })
            .clone();
        entry.owners.fetch_add(1, Ordering::SeqCst);
        entry
    };
    let ring = Arc::new(Ring::new(node, who, total_workers));
    lock(&entry.rings).push(Arc::clone(&ring));
    Arc::new(Recorder {
        ring,
        entry,
        pending: Mutex::new(Vec::new()),
    })
}

/// A recorder of dataflow `id` (`worker`, else the link to `peer`) that
/// records, and dumps to `dir`, whatever the process's destination: for
/// unit tests, which share their process with every other unit test.
#[cfg(test)]
pub(crate) fn recorder_for_test(
    id: DataflowId,
    node: &str,
    worker: Option<usize>,
    peer: &str,
    total_workers: usize,
    dir: &Path,
) -> Arc<Recorder> {
    let who = match worker {
        Some(worker) => Who::Worker(worker),
        None => Who::Bridge(peer.to_string()),
    };
    register(id, node, who, total_workers, Some(dir.to_path_buf()))
}

/// The recorder of `worker` (of `total_workers`) on `node`, for dataflow
/// `id`; `None` when recording is off.
pub(crate) fn worker(
    id: DataflowId,
    node: &str,
    worker: usize,
    total_workers: usize,
) -> Option<Arc<Recorder>> {
    attach(id, node, Who::Worker(worker), total_workers)
}

/// The recorder of `node`'s link to `peer`, for dataflow `id`.
#[cfg_attr(not(feature = "transport"), allow(dead_code))]
pub(crate) fn bridge(
    id: DataflowId,
    node: &str,
    peer: &str,
    total_workers: usize,
) -> Option<Arc<Recorder>> {
    attach(id, node, Who::Bridge(peer.to_string()), total_workers)
}

/// Name the dataflow's operators and edges in its dumps: `edges` maps an
/// edge index to its source and target `(operator, port)`.
pub(crate) fn describe_graph(
    id: DataflowId,
    names: impl IntoIterator<Item = (usize, String)>,
    edges: impl IntoIterator<Item = (usize, (usize, usize), (usize, usize))>,
) {
    if !enabled() {
        return;
    }
    let entry = registry().entries.get(&id).cloned();
    if let Some(entry) = entry {
        let mut graph = lock(&entry.graph);
        if graph.names.is_empty() {
            graph.names.extend(names);
        }
        if graph.edges.is_empty() {
            graph.edges.extend(
                edges
                    .into_iter()
                    .map(|(edge, (sop, sport), (top, tport))| (edge, (sop, sport, top, tport))),
            );
        }
    }
}

/// The exchange edges of a cluster dataflow, in the order their network
/// channel ids number them: ordinal to edge index.
#[cfg_attr(not(feature = "transport"), allow(dead_code))]
pub(crate) fn describe_exchanges(
    id: DataflowId,
    exchanges: impl IntoIterator<Item = (usize, usize)>,
) {
    if !enabled() {
        return;
    }
    let entry = registry().entries.get(&id).cloned();
    if let Some(entry) = entry {
        lock(&entry.graph).exchanges.extend(exchanges);
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        // A dump this ring deferred and nothing fired.
        self.fire_pending();
        if self.entry.owners.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        // The dataflow ended after a dump: write every ring's final state
        // too (exits and results included), before it may be evicted.
        let dumped = !lock(&self.entry.dumps).kinds.is_empty();
        if dumped {
            dump_entry(
                &self.entry,
                Trigger::Final,
                "the dataflow ended: the final state of every ring, after the dumps above",
            );
        }
        let mut registry = registry();
        registry.finished.push_back(self.entry.id);
        while registry.finished.len() > RETAINED_FINISHED {
            let Some(old) = registry.finished.pop_front() else {
                break;
            };
            let idle = registry
                .entries
                .get(&old)
                .is_some_and(|e| e.owners.load(Ordering::SeqCst) == 0);
            if idle && !registry.finished.contains(&old) {
                registry.entries.remove(&old);
                registry.evicted += 1;
            }
        }
    }
}

impl Recorder {
    /// Record one event.
    #[inline(always)]
    pub(crate) fn record(&self, ev: Ev) {
        self.ring.push(&ev);
    }

    /// Record a change (see [`Change::ev`]).
    pub(crate) fn change(&self, kind: Kind, change: &Change, from: usize, reference: u64) {
        self.ring.push(&change.ev(kind, from, reference));
    }

    /// Record `text` (its first 160 bytes) as notes; `context` says what it
    /// is (see [`render`]).
    pub(crate) fn note(&self, context: Note, text: &str) {
        let bytes = text.as_bytes();
        let bytes = &bytes[..bytes.len().min(160)];
        let pieces = bytes.len().div_ceil(40).max(1);
        for (i, piece) in bytes.chunks(40).chain(std::iter::once(&[][..])).enumerate() {
            if i >= pieces {
                break;
            }
            let mut words = [0u64; 5];
            for (w, chunk) in words.iter_mut().zip(piece.chunks(8)) {
                let mut b = [0u8; 8];
                b[..chunk.len()].copy_from_slice(chunk);
                *w = u64::from_le_bytes(b);
            }
            let mut ev = Ev::new(Kind::Note)
                .a(piece.len() | (i << 8))
                .b(context as usize);
            ev.w = words;
            self.ring.push(&ev);
        }
    }

    /// A retired link dropped a commit or marker (counted in the header too).
    pub(crate) fn lane_dropped(&self, ev: Ev) {
        self.ring.lane_drops.fetch_add(1, Ordering::Relaxed);
        self.ring.push(&ev);
    }

    /// Dump every ring of this recorder's dataflow (see [`trigger`]).
    pub(crate) fn trigger(&self, trigger: Trigger, reason: &str) -> Option<PathBuf> {
        dump_entry(&self.entry, trigger, reason)
    }

    /// Dump once this ring's worker has returned the failure it found (the
    /// executor fires it as it records its exit): a dump must never delay
    /// a failure. The first deferred dump of each trigger kind wins (a dump
    /// is written once per kind in any case).
    pub(crate) fn defer(&self, trigger: Trigger, reason: &str) {
        let mut pending = lock(&self.pending);
        if !pending.iter().any(|(t, _)| t.name() == trigger.name()) {
            pending.push((trigger, reason.to_string()));
        }
    }

    /// Write the dumps [`defer`](Self::defer)red, in order.
    pub(crate) fn fire_pending(&self) {
        let pending = std::mem::take(&mut *lock(&self.pending));
        for (trigger, reason) in pending {
            dump_entry(&self.entry, trigger, &reason);
        }
    }
}

/// What a note is about.
#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub(crate) enum Note {
    /// The error an executor returned.
    ExitError = 1,
    /// A link failure's detail.
    LinkDetail = 2,
    /// Why records were lost.
    Loss = 3,
}

impl Note {
    fn name(v: u32) -> &'static str {
        match v {
            1 => "exit-error",
            2 => "link-detail",
            3 => "loss",
            _ => "text",
        }
    }
}

// ---------------------------------------------------------------------------
// The current worker (a thread-local set while an executor runs)
// ---------------------------------------------------------------------------

thread_local! {
    static CURRENT: RefCell<Option<Arc<Recorder>>> = const { RefCell::new(None) };
    /// The output port pushing right now: `operator << 16 | port`.
    static PORT: Cell<u64> = const { Cell::new(u64::MAX) };
    /// The commit being sent: `(seq, network frames, network lanes)`.
    static SENDING: Cell<(u64, u64, u64)> = const { Cell::new((0, 0, 0)) };
}

/// Where an event was recorded from (for the untraced counts).
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Site {
    Tracker = 0,
    Output = 1,
    Exchange = 2,
    NetworkPush = 3,
    NetworkPull = 4,
    ProgressSend = 5,
    Input = 6,
}

const SITES: [&str; 7] = [
    "tracker",
    "output",
    "exchange",
    "network-push",
    "network-pull",
    "progress-send",
    "input",
];

static UNTRACED: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];

/// While alive, the thread records into `recorder` (restores the previous
/// recorder when dropped). Never held across an `.await`.
pub(crate) struct Enter {
    prev: Option<Arc<Recorder>>,
}

/// Record into `recorder` on this thread until the guard drops.
pub(crate) fn enter(recorder: &Option<Arc<Recorder>>) -> Option<Enter> {
    let recorder = recorder.as_ref()?;
    let prev = CURRENT.with(|c| c.replace(Some(Arc::clone(recorder))));
    Some(Enter { prev })
}

impl Drop for Enter {
    fn drop(&mut self) {
        let prev = self.prev.take();
        let _ = CURRENT.try_with(|c| *c.borrow_mut() = prev);
    }
}

/// Run `f` with the thread's current recorder; counts the event as
/// untraced when recording is on but no worker is current.
#[inline]
pub(crate) fn with(site: Site, f: impl FnOnce(&Recorder)) {
    let _ = CURRENT.try_with(|c| match &*c.borrow() {
        Some(recorder) => f(recorder),
        None => {
            if enabled() {
                UNTRACED[site as usize].fetch_add(1, Ordering::Relaxed);
            }
        }
    });
}

/// Whether the thread records into a worker's ring right now.
#[inline]
pub(crate) fn active() -> bool {
    CURRENT.try_with(|c| c.borrow().is_some()).unwrap_or(false)
}

/// Defer a dump of dataflow `id` to the thread's current worker, if it is
/// one of that dataflow's (see [`Recorder::defer`]); false otherwise.
pub(crate) fn defer_current(id: DataflowId, trigger: Trigger, reason: &str) -> bool {
    CURRENT
        .try_with(|c| match &*c.borrow() {
            Some(recorder) if recorder.entry.id == id => {
                recorder.defer(trigger, reason);
                true
            }
            _ => false,
        })
        .unwrap_or(false)
}

/// The output port pushing right now (for exchange and broadcast pushes).
pub(crate) struct PortScope(u64);

/// Set the pushing output port until the guard drops.
pub(crate) fn port_scope(operator: usize, port: usize) -> PortScope {
    let packed = (operator as u64) << 16 | (port as u64 & 0xffff);
    PortScope(PORT.with(|p| p.replace(packed)))
}

impl Drop for PortScope {
    fn drop(&mut self) {
        let _ = PORT.try_with(|p| p.set(self.0));
    }
}

/// The pushing output port, packed (`u64::MAX`: none).
pub(crate) fn port() -> u64 {
    PORT.try_with(Cell::get).unwrap_or(u64::MAX)
}

/// A new pusher or puller number (fan-out and fan-in endpoints are named by
/// it in events).
pub(crate) fn next_tag() -> u32 {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A commit `seq` is about to be sent: network senders count its frames.
pub(crate) fn begin_sends(seq: u64) {
    let _ = SENDING.try_with(|s| s.set((seq, 0, 0)));
}

/// A network lane queued `frames` frames of the commit being sent.
pub(crate) fn note_frames(frames: u64) {
    let _ = SENDING.try_with(|s| {
        let (seq, f, lanes) = s.get();
        s.set((seq, f.max(frames), lanes + 1));
    });
}

/// The commit being sent (its sequence number).
pub(crate) fn sending() -> u64 {
    SENDING.try_with(|s| s.get().0).unwrap_or(0)
}

/// `(network frames per lane, network lanes)` of the commit just sent.
pub(crate) fn end_sends() -> (u64, u64) {
    SENDING
        .try_with(|s| {
            let (_, frames, lanes) = s.get();
            s.set((0, 0, 0));
            (frames, lanes)
        })
        .unwrap_or((0, 0))
}

// ---------------------------------------------------------------------------
// Dumps
// ---------------------------------------------------------------------------

/// Dump every ring of dataflow `id` now (reason `reason`), unless an
/// explicit dump of it was already written. Returns the directory of the
/// explicit dump (the one written now, or earlier); `None` when recording is
/// off, the dataflow is unknown (or evicted), or nothing could be written.
///
/// For test harnesses: call it on any unexpected outcome. The runtime dumps
/// on its own triggers (see the [module docs](self)).
pub fn dump_traces(id: DataflowId, reason: &str) -> Option<PathBuf> {
    trigger(id, Trigger::Explicit, reason)
}

/// Dump every ring of dataflow `id`, at most once per trigger kind. Returns
/// the directory of that kind's dump.
pub(crate) fn trigger(id: DataflowId, trigger: Trigger, reason: &str) -> Option<PathBuf> {
    // A dataflow has an entry only if it records.
    let entry = registry().entries.get(&id).cloned()?;
    dump_entry(&entry, trigger, reason)
}

fn dump_entry(entry: &Entry, trigger: Trigger, reason: &str) -> Option<PathBuf> {
    let base = entry.dir.clone().or_else(dir)?;
    let (dir, root, first) = {
        let mut dumps = lock(&entry.dumps);
        let root = base.join(entry.id.as_uuid().to_string());
        if let Some((_, dir)) = dumps.kinds.iter().find(|(kind, _)| *kind == trigger.name()) {
            return dir.clone();
        }
        let n = dumps.kinds.len() + 1;
        let dir = if n == 1 {
            root.clone()
        } else {
            root.join(format!("{n}-{}", trigger.name()))
        };
        dumps.kinds.push((trigger.name(), Some(dir.clone())));
        (dir, root, n == 1)
    };
    let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        write_dump(entry, &dir, trigger, reason)
    }));
    let failed = || {
        let mut dumps = lock(&entry.dumps);
        if let Some((_, dir)) = dumps
            .kinds
            .iter_mut()
            .find(|(kind, _)| *kind == trigger.name())
        {
            *dir = None;
        }
    };
    match written {
        Ok(Ok(())) => {}
        Ok(Err(_e)) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "instancy::progress::trace",
                "could not write the progress trace dump to {}: {_e}",
                dir.display()
            );
            failed();
            return None;
        }
        Err(_) => {
            failed();
            return None;
        }
    }
    if !first {
        let _ = append(
            &root.join("reason.txt"),
            &format!(
                "later dump ({}): {} -> {}\n",
                trigger.name(),
                reason,
                dir.display()
            ),
        );
    }
    #[cfg(feature = "tracing")]
    tracing::warn!(
        target: "instancy::progress::trace",
        "progress trace of dataflow {} dumped to {} ({}: {reason})",
        entry.id,
        dir.display(),
        trigger.name()
    );
    Some(dir)
}

fn append(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(text.as_bytes())
}

fn write_dump(entry: &Entry, dir: &Path, trigger: Trigger, reason: &str) -> std::io::Result<()> {
    use std::io::Write;
    let rings: Vec<Arc<Ring>> = lock(&entry.rings).clone();
    // Read every ring first, so the dump is as close to one instant as it
    // can be.
    let snapshots: Vec<Snapshot> = rings.iter().map(|r| r.snapshot()).collect();
    let at_ns = now_ns();
    std::fs::create_dir_all(dir)?;
    let id = entry.id.as_uuid().to_string();
    let graph = {
        let g = lock(&entry.graph);
        Graph {
            names: g.names.clone(),
            edges: g.edges.clone(),
            exchanges: g.exchanges.clone(),
        }
    };
    let evicted = registry().evicted;

    let mut header = String::new();
    let _ = write!(header, "{{\"dataflow\":\"{id}\",\"trigger\":");
    json_str(trigger.name(), &mut header);
    header.push_str(",\"reason\":");
    json_str(reason, &mut header);
    let _ = write!(
        header,
        ",\"t_ns\":{at_ns},\"ring_events\":{},\"legend\":{LEGEND},\"untraced\":{{",
        ring_events()
    );
    for (i, site) in SITES.iter().enumerate() {
        if i > 0 {
            header.push(',');
        }
        let _ = write!(header, "\"{site}\":{}", UNTRACED[i].load(Ordering::Relaxed));
    }
    let _ = write!(
        header,
        "}},\"evicted_dataflows\":{evicted},\"operators\":{{"
    );
    for (i, (op, name)) in graph.names.iter().enumerate() {
        if i > 0 {
            header.push(',');
        }
        let _ = write!(header, "\"{op}\":");
        json_str(name, &mut header);
    }
    header.push_str("},\"edges\":{");
    for (i, (edge, (sop, sport, top, tport))) in graph.edges.iter().enumerate() {
        if i > 0 {
            header.push(',');
        }
        let _ = write!(
            header,
            "\"{edge}\":{{\"from\":[{sop},{sport}],\"to\":[{top},{tport}]}}"
        );
    }
    header.push_str("},\"exchange_edges\":{");
    for (i, (ordinal, edge)) in graph.exchanges.iter().enumerate() {
        if i > 0 {
            header.push(',');
        }
        let _ = write!(header, "\"{ordinal}\":{edge}");
    }
    header.push_str("},\"rings\":[");

    let mut used: HashMap<String, usize> = HashMap::new();
    for (i, (ring, snap)) in rings.iter().zip(&snapshots).enumerate() {
        let stem = ring.file_stem();
        let count = used.entry(stem.clone()).or_insert(0);
        *count += 1;
        let file = if *count == 1 {
            format!("{stem}.jsonl")
        } else {
            format!("{stem}-{count}.jsonl")
        };
        let overwritten = snap.written.saturating_sub(ring.capacity);
        let mut ring_header = String::new();
        let _ = write!(ring_header, "{{\"ring\":");
        json_str(&file, &mut ring_header);
        let _ = write!(ring_header, ",\"dataflow\":\"{id}\",\"node\":");
        json_str(&ring.node, &mut ring_header);
        match &ring.who {
            Who::Worker(w) => {
                let _ = write!(ring_header, ",\"worker\":{w}");
            }
            Who::Bridge(peer) => {
                ring_header.push_str(",\"peer\":");
                json_str(peer, &mut ring_header);
            }
        }
        let _ = write!(
            ring_header,
            ",\"total_workers\":{},\"capacity\":{},\"written\":{},\"retained\":{},\
             \"overwritten\":{overwritten},\"torn\":{},\"lane_drops\":{},\"snapshot_t_ns\":{}}}",
            ring.total_workers,
            ring.capacity,
            snap.written,
            snap.events.len(),
            snap.torn,
            ring.lane_drops.load(Ordering::Relaxed),
            snap.at_ns
        );
        if i > 0 {
            header.push(',');
        }
        header.push_str(&ring_header);

        let mut out = String::with_capacity(snap.events.len() * 160 + 256);
        out.push_str(&ring_header);
        out.push('\n');
        let prefix = {
            let mut p = String::new();
            let _ = write!(p, "\"df\":\"{id}\",\"node\":");
            json_str(&ring.node, &mut p);
            match &ring.who {
                Who::Worker(w) => {
                    let _ = write!(p, ",\"worker\":{w}");
                }
                Who::Bridge(peer) => {
                    p.push_str(",\"peer\":");
                    json_str(peer, &mut p);
                }
            }
            p
        };
        for (seq, words) in &snap.events {
            render(*seq, words, &prefix, ring.total_workers, &graph, &mut out);
            out.push('\n');
        }
        let mut f = std::fs::File::create(dir.join(&file))?;
        f.write_all(out.as_bytes())?;
    }
    header.push_str("]}\n");
    std::fs::write(dir.join("header.json"), &header)?;
    let mut text = format!("{}: {reason}\n", trigger.name());
    let gaps: u64 = snapshots
        .iter()
        .zip(&rings)
        .map(|(s, r)| s.written.saturating_sub(r.capacity) + s.torn)
        .sum::<u64>()
        + UNTRACED
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum::<u64>();
    let _ = writeln!(
        text,
        "dataflow {id}; {} ring(s); {gaps} event(s) not in this dump (see header.json)",
        rings.len()
    );
    std::fs::write(dir.join("reason.txt"), text)?;
    Ok(())
}

fn json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

const DECISIONS: [&str; 5] = [
    "not-ready",
    "shut-down",
    "succeeded",
    "residual",
    "s3-failed",
];

/// The dump header's `legend`: how to read the events.
const LEGEND: &str = r#"{"t_ns":"nanoseconds since the process's first recorded event, on one monotonic clock (std::time::Instant) shared by every ring of the process: events of different rings and nodes are ordered by it","seq":"the ring's event number, from 0; a ring keeps its newest `capacity` events","commit":"a worker's commit sequence number: its n-th non-empty commit, from 1","frames":"frames with changes each network lane got for this commit (a commit over the progress batch cap is split, positives first)","frames_before":"frames with changes each network lane got for the worker's earlier commits","lane_batch":"a batch's position on the lane from `from`, from 1: an in-process lane carries one batch per commit (lane_batch = commit); a network lane one per frame (frames_before < lane_batch <= frames_before + frames of the commit); markers (seal, abandon, end-of-stream, ack) are never counted","lane-state":"a peer's seal (sealed) or the end of its lane (closed; closed without sealed: abandoned), as this worker applied it, after lane_batches batches","skipped":"decisions since the previous recorded one that were not recorded: not-ready or shut-down decisions made before every hosted operator was shut down or exhausted (S2b cannot hold then), or identical to the previous recorded one (outcome, flags, lane states and view activity)","decision-count":"a non-zero raw count in the worker's scope (what exact zero judges) at the decision just before it","decision-lane":"after a success, residual or S3 decision: each peer lane's applied position and state","loc":"src: an operator output (capabilities); tgt: an operator input (records); loc_node is the node in the scope (the operator index in scope 0)","seed":"an initial capability seeded when the tracker was initialized; `operator` is the operator seeded","scope":"as a field: the progress scope (0: the root); as an event: a location in the worker's scope (S2b, what its exact zero judges), recorded when its tracker was initialized (no scope event: every location)","frame":"net-send: the data frames this channel's push enqueued so far, this one included if it was enqueued (outcome accepted or lost-while-finishing); net-recv (data): the data frames the channel received so far, this one included","data_channel_id":"2^63 + (exchange * total_workers + src) * total_workers + dst, where `exchange` indexes exchange_edges, which gives the edge","progress_channel_id":"2^62 + src * total_workers + dst","untraced":"events at sites that ran outside any traced worker, process-wide, by site","snapshot_t_ns":"when the ring was read for this dump: it holds every event it recorded before then, unless counted as overwritten or torn","goodbye-sent":"recorded before the end-of-stream frames to that peer are queued","lane_drops":"commits and markers a network lane dropped (its link was retired, the frame did not encode, or the outbox closed)"}"#;

/// The flags of a [`Kind::Decision`].
pub(crate) mod decision {
    pub(crate) const ALL_DONE: u64 = 1;
    pub(crate) const SCOPE_ZERO: u64 = 2;
    pub(crate) const SEALED: u64 = 4;
    pub(crate) const ALL_SEALED: u64 = 8;
    pub(crate) const ALL_EXHAUSTED: u64 = 16;
    pub(crate) const UNCOLLECTED: u64 = 32;
    pub(crate) const LANES_FINAL: u64 = 64;
    pub(crate) const NOT_READY: usize = 0;
    pub(crate) const SHUT_DOWN: usize = 1;
    pub(crate) const SUCCEEDED: usize = 2;
    pub(crate) const RESIDUAL: usize = 3;
    pub(crate) const S3_FAILED: usize = 4;
}

/// How an executor run ended (the source of a [`Kind::Exit`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ExitSource {
    /// Unknown: no tagged path produced it.
    Unknown = 0,
    /// The decision module's success (S2b).
    S2b = 1,
    /// Quiescence of a single worker.
    Quiescent = 2,
    /// Cancellation without a drain.
    Cancelled = 3,
    /// A drain's deadline expired (or was aborted).
    DrainDeadline = 4,
    /// A failure escalated during a drain.
    Escalation = 5,
    /// A stopped peer left the drain waiting (R48).
    StoppedPeer = 6,
    /// A proven residual.
    Residual = 7,
    /// Records reached a shut-down operator (S3).
    S3 = 8,
    /// An operator or progress error.
    Error = 9,
    /// An operator panicked out of the executor.
    Panic = 10,
}

const EXIT_SOURCES: [&str; 11] = [
    "unknown",
    "s2b",
    "quiescent",
    "cancelled",
    "drain-deadline",
    "escalation",
    "stopped-peer",
    "residual",
    "s3",
    "error",
    "panic",
];

const FRAMES: [&str; 8] = [
    "batch",
    "seal",
    "abandon",
    "end-of-stream",
    "ack",
    "decode-error",
    "protocol-violation",
    "channel-ended",
];

/// Progress frame kinds of a [`Kind::ProgressIn`].
pub(crate) mod frame {
    pub(crate) const BATCH: usize = 0;
    pub(crate) const SEAL: usize = 1;
    pub(crate) const ABANDON: usize = 2;
    pub(crate) const END_OF_STREAM: usize = 3;
    pub(crate) const ACK: usize = 4;
    pub(crate) const DECODE_ERROR: usize = 5;
    pub(crate) const VIOLATION: usize = 6;
    pub(crate) const ENDED: usize = 7;
}

const LINK_EVENTS: [&str; 11] = [
    "?",
    "progress-end-of-stream",
    "progress-channel-ended",
    "data-channel-ended",
    "outbound-failed",
    "ack-received",
    "cancel-received",
    "control-closed",
    "control-undecodable",
    "inbound-data-lost",
    "connection-failed",
];

const COORDINATOR: [&str; 5] = [
    "?",
    "worker-finished",
    "goodbye-sent",
    "ack-sent",
    "teardown",
];

/// What a [`Kind::Coordinator`] event is.
pub(crate) mod coordinator {
    pub(crate) const WORKER_FINISHED: usize = 1;
    pub(crate) const GOODBYE_SENT: usize = 2;
    pub(crate) const ACK_SENT: usize = 3;
    pub(crate) const TEARDOWN: usize = 4;
}

const DROPPED: [&str; 4] = ["commit", "seal", "abandon", "?"];
const DROP_WHY: [&str; 4] = ["?", "retired", "encode-failed", "outbox-closed"];

fn name_at(names: &[&'static str], i: u64) -> &'static str {
    names.get(i as usize).copied().unwrap_or("?")
}

/// Render one event as a JSON object (without the newline).
fn render(
    seq: u64,
    words: &[u64; 7],
    prefix: &str,
    total_workers: usize,
    graph: &Graph,
    out: &mut String,
) {
    let header = words[0];
    let kind = Kind::from_u8(header as u8);
    let tag = (header >> 8) as u8;
    let a = (header >> 16) as u16 as u64;
    let b = header >> 32;
    let t_ns = words[1];
    let w = &words[2..];
    let _ = write!(out, "{{\"seq\":{seq},\"t_ns\":{t_ns},{prefix},\"ev\":");
    let Some(kind) = kind else {
        out.push_str("\"unknown\"}");
        return;
    };
    json_str(kind.name(), out);
    let time = |out: &mut String| {
        out.push_str(",\"time\":");
        render_time(tag, w[0], w[1], out);
    };
    let location = |out: &mut String| {
        let packed = w[2];
        let _ = write!(
            out,
            ",\"scope\":{a},\"loc\":\"{}\",\"loc_node\":{},\"loc_port\":{}",
            if packed >> 48 & 1 == 1 { "src" } else { "tgt" },
            packed & 0xffff_ffff,
            packed >> 32 & 0xffff
        );
    };
    let change = |out: &mut String, from: &str, reference: &str| {
        location(out);
        out.push_str(",\"time\":");
        render_time(tag, w[0], w[1], out);
        let _ = write!(out, ",\"diff\":{}", w[3] as i64);
        if !from.is_empty() {
            let _ = write!(out, ",\"{from}\":{b}");
        }
        if !reference.is_empty() {
            let _ = write!(out, ",\"{reference}\":{}", w[4]);
        }
    };
    use Kind::*;
    match kind {
        CommitChange => change(out, "", "commit"),
        Commit => {
            let _ = write!(
                out,
                ",\"commit\":{},\"changes\":{b},\"sections\":{a},\"frames\":{},\
                 \"frames_before\":{},\"local_lanes\":{},\"network_lanes\":{}",
                w[0],
                w[1],
                w[2],
                w[3] >> 32,
                w[3] & 0xffff_ffff
            );
        }
        LaneDropped => {
            let _ = write!(
                out,
                ",\"what\":\"{}\",\"to\":{b},\"commit\":{},\"why\":\"{}\"",
                name_at(&DROPPED, a),
                w[0],
                name_at(&DROP_WHY, w[1])
            );
        }
        BatchChange => change(out, "from", "lane_batch"),
        BatchApplied => {
            let _ = write!(
                out,
                ",\"from\":{b},\"lane_batch\":{},\"changes\":{},\"sections\":{a}",
                w[0], w[1]
            );
        }
        LaneState | DecisionLane => {
            let _ = write!(
                out,
                ",\"from\":{b},\"lane_batches\":{},\"sealed\":{},\"closed\":{}",
                w[0],
                a & 1 == 1,
                a & 2 == 2
            );
        }
        SealSent => {
            let _ = write!(out, ",\"lanes\":{b},\"commits\":{}", w[0]);
        }
        Decision => {
            let flags = w[0];
            let _ = write!(
                out,
                ",\"outcome\":\"{}\",\"nonzero\":{b},\"all_done\":{},\"scope_zero\":{},\
                 \"sealed\":{},\"all_sealed\":{},\"all_exhausted\":{},\"uncollected\":{},\
                 \"lanes_final\":{},\"activity\":{},\"skipped\":{},\"stopped_peers\":{},\
                 \"commits\":{}",
                name_at(&DECISIONS, a),
                flags & decision::ALL_DONE != 0,
                flags & decision::SCOPE_ZERO != 0,
                flags & decision::SEALED != 0,
                flags & decision::ALL_SEALED != 0,
                flags & decision::ALL_EXHAUSTED != 0,
                flags & decision::UNCOLLECTED != 0,
                flags & decision::LANES_FINAL != 0,
                w[1],
                w[2],
                w[3],
                w[4]
            );
        }
        DecisionCount => change(out, "", ""),
        Seed => change(out, "", "operator"),
        Scope => location(out),
        ShutDown => {
            let _ = write!(out, ",\"operator\":{b}");
        }
        Exit => {
            let result = match a {
                0 => "ok-false",
                1 => "ok-true",
                2 => "err",
                _ => "panic",
            };
            let _ = write!(
                out,
                ",\"result\":\"{result}\",\"source\":\"{}\",\"sealed\":{},\"draining\":{}",
                name_at(&EXIT_SOURCES, b),
                w[0] & 1 == 1,
                w[0] & 2 == 2
            );
        }
        Reported => {
            let _ = write!(out, ",\"success\":{}", a == 1);
        }
        Note => {
            let len = (a & 0xff) as usize;
            let mut bytes = Vec::with_capacity(40);
            for word in w {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
            bytes.truncate(len);
            let _ = write!(
                out,
                ",\"about\":\"{}\",\"piece\":{},\"text\":",
                self::Note::name(b as u32),
                a >> 8
            );
            json_str(&String::from_utf8_lossy(&bytes), out);
        }
        OutputTaken | InputPull => {
            let _ = write!(out, ",\"operator\":{b},\"port\":{a}");
            time(out);
            let _ = write!(out, ",\"records\":{}", w[2]);
        }
        OutputPush => {
            let _ = write!(out, ",\"operator\":{b},\"port\":{a}");
            time(out);
            let _ = write!(
                out,
                ",\"records\":{},\"outcome\":\"{}\"",
                w[2],
                Outcome::name(w[3])
            );
        }
        TargetPush => {
            let kind = match w[3] >> 8 {
                1 => "exchange",
                2 => "broadcast",
                _ => "?",
            };
            let _ = write!(out, ",\"via\":\"{kind}\",\"pusher\":{b},\"target\":{a}");
            if w[4] != u64::MAX {
                let _ = write!(
                    out,
                    ",\"operator\":{},\"port\":{}",
                    w[4] >> 16,
                    w[4] & 0xffff
                );
            }
            time(out);
            let _ = write!(
                out,
                ",\"records\":{},\"outcome\":\"{}\"",
                w[2],
                Outcome::name(w[3] & 0xff)
            );
        }
        NetSend | NetRecv => {
            let _ = write!(out, ",\"channel\":{}", w[4]);
            channel_parts(w[4], total_workers, graph, out);
            if kind == NetRecv {
                let what = match a {
                    0 => "data",
                    1 => "close",
                    2 => "decode-error",
                    _ => "disconnected",
                };
                let _ = write!(out, ",\"what\":\"{what}\"");
            }
            time(out);
            let _ = write!(out, ",\"records\":{}", w[2]);
            if kind == NetSend {
                let _ = write!(out, ",\"outcome\":\"{}\"", Outcome::name(w[3]));
            }
            if kind == NetSend || a == 0 {
                let _ = write!(out, ",\"frame\":{b}");
            }
        }
        SourcePull => {
            let _ = write!(out, ",\"puller\":{b},\"source\":{a}");
            time(out);
            let _ = write!(out, ",\"records\":{}", w[2]);
        }
        FrameIn => {
            let outcome = match w[2] {
                0 => "delivered",
                1 => "lost-operator-finished",
                2 => "lost-as-operator-finished",
                3 => "after-close",
                4 => "discarded",
                _ => "?",
            };
            let _ = write!(out, ",\"channel\":{}", w[0]);
            channel_parts(w[0], total_workers, graph, out);
            let _ = write!(
                out,
                ",\"bytes\":{},\"records\":{},\"close\":{},\"outcome\":\"{outcome}\"",
                w[1],
                a & 1 == 1,
                a & 2 == 2
            );
        }
        ProgressIn => {
            let _ = write!(
                out,
                ",\"channel\":{},\"frame\":\"{}\",\"changes\":{b},\"lane_frames\":{}",
                w[0],
                name_at(&FRAMES, a),
                w[1]
            );
            progress_parts(w[0], total_workers, out);
        }
        Link => {
            let flags = w[0];
            let _ = write!(
                out,
                ",\"event\":\"{}\",\"end_of_streams\":{},\"expected\":{},\"goodbye_received\":{},\
                 \"ack_received\":{},\"retired\":{},\"lost\":{},\"retired_now\":{}",
                name_at(&LINK_EVENTS, a),
                w[1],
                w[2],
                flags & 1 != 0,
                flags & 2 != 0,
                flags & 4 != 0,
                flags & 8 != 0,
                w[3] & 1 == 1
            );
        }
        Retired => {
            let _ = write!(out, ",\"was_retired\":{}", a == 1);
        }
        Coordinator => {
            let _ = write!(out, ",\"action\":\"{}\"", name_at(&COORDINATOR, a));
            match a as usize {
                coordinator::WORKER_FINISHED => {
                    let _ = write!(out, ",\"success\":{}", w[0] == 1);
                }
                coordinator::GOODBYE_SENT => {
                    let _ = write!(out, ",\"channels\":{}", w[0]);
                }
                coordinator::TEARDOWN => {
                    let _ = write!(out, ",\"graceful\":{}", w[0] == 1);
                }
                _ => {}
            }
        }
    }
    out.push('}');
}

/// `exchange` (and its `edge`), `src` and `dst` of a data channel id (see
/// `transport_session::data_channel_id`).
fn channel_parts(channel: u64, total_workers: usize, graph: &Graph, out: &mut String) {
    #[cfg(feature = "transport")]
    {
        use crate::communication::transport_session::{DATA_CHANNEL_BASE, RESERVED_CHANNEL_BASE};
        let n = total_workers as u64;
        if (DATA_CHANNEL_BASE..RESERVED_CHANNEL_BASE).contains(&channel) && n > 0 {
            let offset = channel - DATA_CHANNEL_BASE;
            let exchange = offset / n / n;
            let _ = write!(out, ",\"exchange\":{exchange}");
            if let Some(edge) = graph.exchanges.get(&(exchange as usize)) {
                let _ = write!(out, ",\"edge\":{edge}");
            }
            let _ = write!(out, ",\"src\":{},\"dst\":{}", offset / n % n, offset % n);
        }
    }
    #[cfg(not(feature = "transport"))]
    let _ = (channel, total_workers, graph, out);
}

/// `src` and `dst` of a progress channel id (see
/// `transport_session::progress_channel_id`).
fn progress_parts(channel: u64, total_workers: usize, out: &mut String) {
    #[cfg(feature = "transport")]
    {
        let base = crate::communication::transport_session::PROGRESS_CHANNEL_BASE;
        let n = total_workers as u64;
        if channel >= base && n > 0 {
            let offset = channel - base;
            if offset < n * n {
                let _ = write!(out, ",\"src\":{},\"dst\":{}", offset / n, offset % n);
            }
        }
    }
    #[cfg(not(feature = "transport"))]
    let _ = (channel, total_workers, out);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_encode_exactly_or_as_text() {
        assert_eq!(
            time(&5u64),
            TimeRepr {
                tag: T_UNSIGNED,
                a: 5,
                b: 0
            }
        );
        assert_eq!(time(&-3i32).tag, T_SIGNED);
        let p = time(&Product::new(4u64, 7u32));
        assert_eq!((p.tag, p.a, p.b), (T_PAIR, 4, 7));
        let n = time(&Product::new(Product::new(1u64, 2u32), 3u32));
        let mut s = String::new();
        render_time(n.tag, n.a, n.b, &mut s);
        assert_eq!(s, "[[1,2],3]");
        let t = time(&"a long text timestamp".to_string());
        let mut s = String::new();
        render_time(t.tag, t.a, t.b, &mut s);
        assert_eq!(s, "\"\\\"a long text tim\"");
    }

    #[test]
    fn a_ring_keeps_the_newest_events_and_counts_the_rest() {
        let ring = Ring::new("n", Who::Worker(0), 1);
        let total = ring.capacity + 10;
        for i in 0..total {
            ring.push(&Ev::new(Kind::ShutDown).b(i as usize));
        }
        let snap = ring.snapshot();
        assert_eq!(snap.written, total);
        assert_eq!(snap.torn, 0);
        assert_eq!(snap.events.len() as u64, ring.capacity);
        assert_eq!(snap.events[0].0, 10);
        assert_eq!(snap.events[0].1[0] >> 32, 10);
    }

    #[test]
    fn notes_keep_their_text() {
        let ring = Arc::new(Ring::new("n", Who::Worker(0), 1));
        let entry = Arc::new(Entry {
            id: DataflowId::new(),
            owners: AtomicUsize::new(1),
            rings: Mutex::new(vec![ring.clone()]),
            graph: Mutex::new(Graph::default()),
            dumps: Mutex::new(Dumps::default()),
            dir: None,
        });
        let recorder = Recorder {
            ring: ring.clone(),
            entry,
            pending: Mutex::new(Vec::new()),
        };
        let text = "records sent while the peer was finishing: \"quoted\"";
        recorder.note(Note::Loss, text);
        let snap = ring.snapshot();
        let mut joined = String::new();
        for (seq, words) in &snap.events {
            let mut line = String::new();
            render(*seq, words, "\"x\":0", 1, &Graph::default(), &mut line);
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            joined.push_str(v["text"].as_str().unwrap());
        }
        assert_eq!(joined, text);
    }
}
