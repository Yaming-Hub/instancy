//! Reads one flight recorder dump (`instancy::progress::trace`) and tells
//! which of three hypotheses a progress failure fits:
//!
//! - **(a) missing `+n`**: a worker succeeded (S2b) while a `+n` its peer
//!   committed before that decision, at a location in the worker's scope,
//!   was not among the changes it had applied from that peer (the commit's
//!   lane batches, through `frames_before`/`frames`);
//! - **(b) duplicate push**: more records were pushed, or sent over the
//!   network to one worker, than were produced at that time, or a channel
//!   received more records, or more frames, than its sender sent;
//! - **(c) success outside S2b**: a worker's success was returned or
//!   reported without the decision module's S2b decision; or a coordinator
//!   applied more successes than its workers reported, said goodbye before
//!   each of its workers reported success, or retired a link on its peer's
//!   end-of-stream before every progress channel from the peer ended or
//!   before the peer said goodbye.
//!
//! It also lists the proven residual counts (location and time), the
//! records lost to a retired link, what retired it and what their sender
//! had committed at their time (the evidence for (a)'s "release without
//! `+n`" variant, listed, not judged), and every gap in
//! the dump (overwritten or torn events, untraced events, dropped commits,
//! evicted dataflows): a verdict on a truncated ring is weaker, and says
//! so.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use serde_json::Value;

/// A hypothesis about a progress failure (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Hypothesis {
    /// (a): a view at an S2b decision lacked a committed `+n`.
    MissingPlus,
    /// (b): records pushed or received more often than produced or sent.
    DuplicatePush,
    /// (c): a success without an S2b decision.
    SuccessOutsideS2b,
}

impl fmt::Display for Hypothesis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Hypothesis::MissingPlus => "(a) missing +n",
            Hypothesis::DuplicatePush => "(b) duplicate push",
            Hypothesis::SuccessOutsideS2b => "(c) success outside S2b",
        })
    }
}

/// The [`Finding::worker`] of a finding about a node's coordinator.
pub const COORDINATOR: usize = usize::MAX;

/// One piece of evidence for a hypothesis.
#[derive(Clone, Debug)]
pub struct Finding {
    pub hypothesis: Hypothesis,
    /// Where it was found: `node` and worker ([`COORDINATOR`]: the node's
    /// coordinator).
    pub node: String,
    pub worker: usize,
    pub detail: String,
}

/// A non-zero raw count a worker's proven residual left.
#[derive(Clone, Debug)]
pub struct ResidualCount {
    pub node: String,
    pub worker: usize,
    pub scope: u64,
    /// `"src"` (an output) or `"tgt"` (an input).
    pub loc: String,
    pub loc_node: u64,
    pub loc_port: u64,
    /// The operator's name, when the location is in the root scope.
    pub operator: Option<String>,
    pub time: Value,
    pub diff: i64,
}

impl fmt::Display for ResidualCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let side = if self.loc == "src" { "out" } else { "in" };
        let name = self
            .operator
            .clone()
            .unwrap_or_else(|| format!("node {}", self.loc_node));
        write!(
            f,
            "{}/w{}: scope {} {name}[{side} {}] @ {}: {:+}",
            self.node, self.worker, self.scope, self.loc_port, self.time, self.diff
        )
    }
}

/// Records a push lost to a retired link.
#[derive(Clone, Debug)]
pub struct LostPush {
    pub node: String,
    pub worker: usize,
    pub channel: u64,
    /// The worker the records were for (from the channel id).
    pub dst: Option<u64>,
    pub time: Value,
    pub records: u64,
    pub outcome: String,
    /// What the sender's node saw retire the link (link events).
    pub retired_by: Vec<String>,
    /// What the sender had committed at the records' time, at the edge's
    /// target and source (see `committed_around`).
    pub committed: String,
}

/// What [`trace_verdict`] found.
#[derive(Clone, Debug, Default)]
pub struct Verdict {
    pub findings: Vec<Finding>,
    pub residuals: Vec<ResidualCount>,
    pub lost: Vec<LostPush>,
    /// Everything the dump does not show (each weakens the verdict).
    pub gaps: Vec<String>,
    /// Rings read.
    pub rings: usize,
}

impl Verdict {
    /// The hypotheses with evidence.
    pub fn hypotheses(&self) -> BTreeSet<Hypothesis> {
        self.findings.iter().map(|f| f.hypothesis).collect()
    }

    /// The findings of one hypothesis.
    pub fn about(&self, hypothesis: Hypothesis) -> Vec<&Finding> {
        self.findings
            .iter()
            .filter(|f| f.hypothesis == hypothesis)
            .collect()
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "trace verdict ({} ring(s)):", self.rings)?;
        if self.findings.is_empty() {
            writeln!(f, "  no evidence for (a), (b) or (c)")?;
        }
        for finding in &self.findings {
            let at = if finding.worker == COORDINATOR {
                "coordinator".to_string()
            } else {
                format!("w{}", finding.worker)
            };
            writeln!(
                f,
                "  {} [{}/{at}]: {}",
                finding.hypothesis, finding.node, finding.detail
            )?;
        }
        for residual in &self.residuals {
            writeln!(f, "  residual: {residual}")?;
        }
        for lost in &self.lost {
            writeln!(
                f,
                "  lost: {}/w{} channel {} (to worker {:?}) @ {}: {} record(s), {}; {}; retired \
                 by: {}",
                lost.node,
                lost.worker,
                lost.channel,
                lost.dst,
                lost.time,
                lost.records,
                lost.outcome,
                lost.committed,
                if lost.retired_by.is_empty() {
                    "nothing recorded".to_string()
                } else {
                    lost.retired_by.join("; ")
                }
            )?;
        }
        for gap in &self.gaps {
            writeln!(f, "  gap: {gap}")?;
        }
        Ok(())
    }
}

/// One ring of the dump.
#[derive(Clone, Debug)]
struct Ring {
    node: String,
    worker: Option<usize>,
    peer: Option<String>,
    total_workers: u64,
    /// No event was overwritten or torn: an event missing from it did not
    /// happen (while it recorded).
    complete: bool,
    /// When the ring was read for the dump (`None`: unknown).
    snapshot_t_ns: Option<u64>,
    events: Vec<Value>,
}

/// Whether `ring` holds every event it recorded up to `t_ns`: it is
/// complete and was read after then (rings are read one after another). A
/// ring without its read time counts as read after every event.
fn settled(ring: &Ring, t_ns: u64) -> bool {
    ring.complete && ring.snapshot_t_ns.is_none_or(|at| at > t_ns)
}

fn u(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn i(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

fn ev(v: &Value) -> &str {
    s(v, "ev")
}

/// Read the dump in `dir` (one dump directory: `header.json`, `*.jsonl`).
fn load(dir: &Path, verdict: &mut Verdict) -> (Value, Vec<Ring>) {
    let header: Value = std::fs::read_to_string(dir.join("header.json"))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null);
    let mut rings = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map(|entries| entries.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    entries.sort();
    for path in entries {
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            verdict
                .gaps
                .push(format!("{} could not be read", path.display()));
            continue;
        };
        let mut lines = text.lines();
        let Some(ring_header) = lines
            .next()
            .and_then(|l| serde_json::from_str::<Value>(l).ok())
        else {
            verdict
                .gaps
                .push(format!("{} has no header", path.display()));
            continue;
        };
        let name = s(&ring_header, "ring").to_string();
        for (key, what) in [
            (
                "overwritten",
                "event(s) overwritten (the oldest are missing)",
            ),
            ("torn", "event(s) torn while dumping"),
            (
                "lane_drops",
                "commit(s) or marker(s) a network lane dropped",
            ),
        ] {
            let n = u(&ring_header, key);
            if n > 0 {
                verdict.gaps.push(format!("{name}: {n} {what}"));
            }
        }
        let events: Vec<Value> = lines
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .collect();
        rings.push(Ring {
            node: s(&ring_header, "node").to_string(),
            worker: ring_header
                .get("worker")
                .and_then(Value::as_u64)
                .map(|w| w as usize),
            peer: ring_header
                .get("peer")
                .and_then(Value::as_str)
                .map(str::to_string),
            total_workers: u(&ring_header, "total_workers"),
            complete: u(&ring_header, "overwritten") == 0 && u(&ring_header, "torn") == 0,
            snapshot_t_ns: ring_header.get("snapshot_t_ns").and_then(Value::as_u64),
            events,
        });
    }
    if let Some(untraced) = header.get("untraced").and_then(Value::as_object) {
        let sites: Vec<String> = untraced
            .iter()
            .filter(|(_, n)| n.as_u64().unwrap_or(0) > 0)
            .map(|(site, n)| format!("{site} {n}"))
            .collect();
        if !sites.is_empty() {
            verdict.gaps.push(format!(
                "events recorded outside any traced worker (process-wide): {}",
                sites.join(", ")
            ));
        }
    }
    let evicted = u(&header, "evicted_dataflows");
    if evicted > 0 {
        verdict.gaps.push(format!(
            "{evicted} finished dataflow(s) evicted from the registry"
        ));
    }
    verdict.rings = rings.len();
    (header, rings)
}

/// Analyse the dump in `dir` (see the module docs).
pub fn trace_verdict(dir: &Path) -> Verdict {
    let mut verdict = Verdict::default();
    let (header, rings) = load(dir, &mut verdict);
    let operators: BTreeMap<u64, String> = header
        .get("operators")
        .and_then(Value::as_object)
        .map(|ops| {
            ops.iter()
                .filter_map(|(k, v)| Some((k.parse().ok()?, v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let workers: Vec<&Ring> = rings.iter().filter(|r| r.worker.is_some()).collect();
    residuals(&workers, &operators, &mut verdict);
    let graph = graph(&header);
    missing_plus(&workers, &mut verdict);
    duplicate_push(&workers, &graph, &mut verdict);
    outside_s2b(&rings, &workers, &mut verdict);
    lost_pushes(&rings, &workers, &graph, &mut verdict);
    verdict
}

/// The counts of every worker's last proven residual (or S3) decision.
fn residuals(workers: &[&Ring], operators: &BTreeMap<u64, String>, verdict: &mut Verdict) {
    for ring in workers {
        let worker = ring.worker.unwrap_or_default();
        let Some(at) = ring
            .events
            .iter()
            .rposition(|e| ev(e) == "decision" && s(e, "outcome") == "residual")
        else {
            continue;
        };
        for e in ring.events[at + 1..]
            .iter()
            .take_while(|e| ev(e) == "decision-count")
        {
            let scope = u(e, "scope");
            let loc_node = u(e, "loc_node");
            verdict.residuals.push(ResidualCount {
                node: ring.node.clone(),
                worker,
                scope,
                loc: s(e, "loc").to_string(),
                loc_node,
                loc_port: u(e, "loc_port"),
                operator: (scope == 0)
                    .then(|| operators.get(&loc_node).cloned())
                    .flatten(),
                time: e.get("time").cloned().unwrap_or(Value::Null),
                diff: i(e, "diff"),
            });
        }
    }
}

/// A progress change, as a ring recorded it: `(scope, loc, node, port,
/// time, diff)`, with the time as its JSON text.
type ChangeKey = (u64, String, u64, u64, String, i64);

fn change_key(e: &Value) -> ChangeKey {
    (
        u(e, "scope"),
        s(e, "loc").to_string(),
        u(e, "loc_node"),
        u(e, "loc_port"),
        key(e.get("time")),
        i(e, "diff"),
    )
}

fn describe(c: &ChangeKey) -> String {
    format!(
        "scope {} {} node {} port {} @ {}: {:+}",
        c.0, c.1, c.2, c.3, c.4, c.5
    )
}

/// A sender's commit, as its ring recorded it.
#[derive(Clone, Debug)]
struct Commit {
    t_ns: u64,
    frames: u64,
    frames_before: u64,
    /// Its positive changes.
    plus: Vec<ChangeKey>,
    /// Network lanes that dropped it (by target worker).
    dropped_to: BTreeSet<u64>,
}

impl Commit {
    /// The lane positions of its batches at a receiver: one per commit
    /// in-process, one per frame over the network.
    fn lane_batches(&self, seq: u64, network: bool) -> std::ops::RangeInclusive<u64> {
        if network {
            self.frames_before + 1..=self.frames_before + self.frames
        } else {
            seq..=seq
        }
    }
}

fn commits(ring: &Ring) -> BTreeMap<u64, Commit> {
    let mut out: BTreeMap<u64, Commit> = BTreeMap::new();
    let mut plus: BTreeMap<u64, Vec<ChangeKey>> = BTreeMap::new();
    let mut dropped: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    for e in &ring.events {
        match ev(e) {
            "commit-change" if i(e, "diff") > 0 => {
                plus.entry(u(e, "commit")).or_default().push(change_key(e));
            }
            "lane-dropped" if s(e, "what") == "commit" => {
                dropped
                    .entry(u(e, "commit"))
                    .or_default()
                    .insert(u(e, "to"));
            }
            "commit" => {
                out.insert(
                    u(e, "commit"),
                    Commit {
                        t_ns: u(e, "t_ns"),
                        frames: u(e, "frames"),
                        frames_before: u(e, "frames_before"),
                        plus: Vec::new(),
                        dropped_to: BTreeSet::new(),
                    },
                );
            }
            _ => {}
        }
    }
    for (seq, commit) in out.iter_mut() {
        commit.plus = plus.remove(seq).unwrap_or_default();
        commit.dropped_to = dropped.remove(seq).unwrap_or_default();
    }
    out
}

/// A worker's scope (S2b): the locations it judges, recorded when its
/// tracker was initialized. `None` (no scope recorded): every location.
fn scope_of(ring: &Ring) -> Option<BTreeSet<(u64, String, u64, u64)>> {
    let mut scope = BTreeSet::new();
    for e in ring.events.iter().filter(|e| ev(e) == "scope") {
        scope.insert((
            u(e, "scope"),
            s(e, "loc").to_string(),
            u(e, "loc_node"),
            u(e, "loc_port"),
        ));
    }
    (!scope.is_empty()).then_some(scope)
}

/// (a): at each worker's last S2b success, every `+n` a peer committed
/// before it, at a location in the worker's scope, must have been applied
/// (judged change by change against the batches it applied).
fn missing_plus(workers: &[&Ring], verdict: &mut Verdict) {
    let sent: BTreeMap<usize, (&Ring, BTreeMap<u64, Commit>)> = workers
        .iter()
        .map(|r| (r.worker.unwrap_or_default(), (*r, commits(r))))
        .collect();
    for ring in workers {
        let receiver = ring.worker.unwrap_or_default();
        let Some(at) = ring
            .events
            .iter()
            .rposition(|e| ev(e) == "decision" && s(e, "outcome") == "succeeded")
        else {
            continue;
        };
        if !ring.complete {
            verdict.gaps.push(format!(
                "{}/w{receiver}: ring truncated, (a) not judged at its success",
                ring.node
            ));
            continue;
        }
        let decided_at = u(&ring.events[at], "t_ns");
        let scope = scope_of(ring);
        // Every positive change applied before the decision, by sender and
        // lane position.
        let mut applied: BTreeMap<(u64, u64), Vec<ChangeKey>> = BTreeMap::new();
        for e in &ring.events[..at] {
            if ev(e) == "batch-change" && i(e, "diff") > 0 {
                applied
                    .entry((u(e, "from"), u(e, "lane_batch")))
                    .or_default()
                    .push(change_key(e));
            }
        }
        for (&sender, (sender_ring, sender_commits)) in &sent {
            if sender == receiver {
                continue;
            }
            let network = sender_ring.node != ring.node;
            for (seq, commit) in sender_commits {
                if commit.t_ns >= decided_at {
                    continue;
                }
                let in_scope: Vec<&ChangeKey> = commit
                    .plus
                    .iter()
                    .filter(|c| {
                        scope
                            .as_ref()
                            .is_none_or(|scope| scope.contains(&(c.0, c.1.clone(), c.2, c.3)))
                    })
                    .collect();
                if in_scope.is_empty() {
                    continue;
                }
                let batches = commit.lane_batches(*seq, network);
                let mut got: Vec<ChangeKey> = batches
                    .clone()
                    .flat_map(|b| {
                        applied
                            .get(&(sender as u64, b))
                            .cloned()
                            .unwrap_or_default()
                    })
                    .collect();
                let mut missing = Vec::new();
                for change in in_scope {
                    match got.iter().position(|g| g == change) {
                        Some(found) => {
                            got.swap_remove(found);
                        }
                        None => missing.push(describe(change)),
                    }
                }
                if missing.is_empty() {
                    continue;
                }
                let dropped = commit.dropped_to.contains(&(receiver as u64));
                verdict.findings.push(Finding {
                    hypothesis: Hypothesis::MissingPlus,
                    node: ring.node.clone(),
                    worker: receiver,
                    detail: format!(
                        "succeeded (S2b at t_ns {decided_at}) without the +n of worker {sender}'s \
                         commit {seq} (made at t_ns {}; {} lane batch(es) {:?}{}): {}",
                        commit.t_ns,
                        if network { "network" } else { "in-process" },
                        batches,
                        if dropped {
                            ", dropped by a retired lane"
                        } else {
                            ""
                        },
                        missing.join(", ")
                    ),
                });
            }
        }
    }
}

/// Outcomes that put records on their way.
fn delivered(outcome: &str) -> bool {
    matches!(outcome, "accepted" | "lost-while-finishing")
}

fn key(time: Option<&Value>) -> String {
    time.map(Value::to_string).unwrap_or_default()
}

/// The dataflow's graph, from the dump's header: each edge's source and
/// target `(operator, port)`.
#[derive(Default)]
struct Graph {
    sources: BTreeMap<u64, (u64, u64)>,
    targets: BTreeMap<u64, (u64, u64)>,
}

fn graph(header: &Value) -> Graph {
    let mut graph = Graph::default();
    if let Some(edges) = header.get("edges").and_then(Value::as_object) {
        for (edge, ends) in edges {
            let Ok(edge) = edge.parse::<u64>() else {
                continue;
            };
            let end = |key: &str| {
                let pair = ends.get(key)?.as_array()?;
                Some((pair.first()?.as_u64()?, pair.get(1)?.as_u64()?))
            };
            if let Some(from) = end("from") {
                graph.sources.insert(edge, from);
            }
            if let Some(to) = end("to") {
                graph.targets.insert(edge, to);
            }
        }
    }
    graph
}

/// (b): no pusher delivers more records at a time than its port produced
/// then, no worker sends more records over the network to one worker than
/// the edge's port produced (an exchange splits them, a broadcast sends
/// each to every worker), and no channel receives more records, or more
/// frames, than were sent on it.
fn duplicate_push(workers: &[&Ring], graph: &Graph, verdict: &mut Verdict) {
    // Records sent per data channel and time, and the frames enqueued per
    // channel (every sender), for the receiving side's checks.
    let mut sent: BTreeMap<(u64, String), u64> = BTreeMap::new();
    let mut frames: BTreeMap<u64, u64> = BTreeMap::new();
    for ring in workers {
        // Records sent over the network per edge, destination worker and
        // time.
        let mut networked: BTreeMap<(u64, u64, String), u64> = BTreeMap::new();
        let worker = ring.worker.unwrap_or_default();
        let mut produced: BTreeMap<(u64, u64, String), u64> = BTreeMap::new();
        // (pusher, target, time) and (pusher, time) totals, as they grow.
        let mut per_target: BTreeMap<(u64, u64, String), u64> = BTreeMap::new();
        let mut per_pusher: BTreeMap<(u64, String), u64> = BTreeMap::new();
        let mut top: BTreeMap<(u64, u64, String), u64> = BTreeMap::new();
        for e in &ring.events {
            let time = key(e.get("time"));
            match ev(e) {
                "output-taken" => {
                    *produced
                        .entry((u(e, "operator"), u(e, "port"), time))
                        .or_default() += u(e, "records");
                }
                "output-push" if s(e, "outcome") == "accepted" => {
                    let port = (u(e, "operator"), u(e, "port"), time.clone());
                    let total = top.entry(port.clone()).or_default();
                    *total += u(e, "records");
                    let made = produced.get(&port).copied().unwrap_or(0);
                    if *total > made && ring.complete {
                        verdict.findings.push(Finding {
                            hypothesis: Hypothesis::DuplicatePush,
                            node: ring.node.clone(),
                            worker,
                            detail: format!(
                                "output {}[out {}] @ {time}: {} record(s) pushed, {made} produced \
                                 (push at seq {})",
                                port.0,
                                port.1,
                                *total,
                                u(e, "seq")
                            ),
                        });
                    }
                }
                "target-push" if delivered(s(e, "outcome")) => {
                    let pusher = u(e, "pusher");
                    let records = u(e, "records");
                    let made = (e.get("operator").is_some()).then(|| {
                        produced
                            .get(&(u(e, "operator"), u(e, "port"), time.clone()))
                            .copied()
                            .unwrap_or(0)
                    });
                    let (total, what) = if s(e, "via") == "broadcast" {
                        let t = per_target
                            .entry((pusher, u(e, "target"), time.clone()))
                            .or_default();
                        *t += records;
                        (*t, format!("to target {}", u(e, "target")))
                    } else {
                        let t = per_pusher.entry((pusher, time.clone())).or_default();
                        *t += records;
                        (*t, "over every target".to_string())
                    };
                    if let Some(made) = made {
                        if total > made && ring.complete {
                            verdict.findings.push(Finding {
                                hypothesis: Hypothesis::DuplicatePush,
                                node: ring.node.clone(),
                                worker,
                                detail: format!(
                                    "{} pusher {pusher} of {}[out {}] @ {time}: {total} record(s) \
                                     delivered {what}, {made} produced; the push that exceeded it: \
                                     seq {} to target {} ({} record(s), {})",
                                    s(e, "via"),
                                    u(e, "operator"),
                                    u(e, "port"),
                                    u(e, "seq"),
                                    u(e, "target"),
                                    records,
                                    s(e, "outcome")
                                ),
                            });
                        }
                    }
                }
                "net-send" if delivered(s(e, "outcome")) => {
                    let channel = u(e, "channel");
                    let records = u(e, "records");
                    *sent.entry((channel, time.clone())).or_default() += records;
                    let f = frames.entry(channel).or_default();
                    *f = (*f).max(u(e, "frame"));
                    let Some(edge) = e.get("edge").and_then(Value::as_u64) else {
                        continue;
                    };
                    let Some(&(op, port)) = graph.sources.get(&edge) else {
                        continue;
                    };
                    let dst = u(e, "dst");
                    let total = networked.entry((edge, dst, time.clone())).or_default();
                    *total += records;
                    let made = produced
                        .get(&(op, port, time.clone()))
                        .copied()
                        .unwrap_or(0);
                    if *total > made && ring.complete {
                        verdict.findings.push(Finding {
                            hypothesis: Hypothesis::DuplicatePush,
                            node: ring.node.clone(),
                            worker,
                            detail: format!(
                                "edge {edge} from {op}[out {port}] @ {time}, to worker {dst}: {} \
                                 record(s) sent over the network, {made} produced; the send that \
                                 exceeded it: seq {} on channel {channel} ({records} record(s), {})",
                                *total,
                                u(e, "seq"),
                                s(e, "outcome")
                            ),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    for ring in workers {
        let worker = ring.worker.unwrap_or_default();
        let mut received: BTreeMap<(u64, String), u64> = BTreeMap::new();
        for e in &ring.events {
            if ev(e) == "net-recv" && s(e, "what") == "data" {
                let k = (u(e, "channel"), key(e.get("time")));
                let total = received.entry(k.clone()).or_default();
                *total += u(e, "records");
                let out = sent.get(&k).copied().unwrap_or(0);
                let settled = ring.complete && sender_settled(workers, e);
                let enqueued = frames.get(&k.0).copied().unwrap_or(0);
                if u(e, "frame") > enqueued && settled {
                    verdict.findings.push(Finding {
                        hypothesis: Hypothesis::DuplicatePush,
                        node: ring.node.clone(),
                        worker,
                        detail: format!(
                            "channel {}: received data frame {}, the sender enqueued {enqueued} \
                             (receive at seq {})",
                            k.0,
                            u(e, "frame"),
                            u(e, "seq")
                        ),
                    });
                }
                if *total > out && settled {
                    verdict.findings.push(Finding {
                        hypothesis: Hypothesis::DuplicatePush,
                        node: ring.node.clone(),
                        worker,
                        detail: format!(
                            "channel {} @ {}: {} record(s) received, {out} sent (receive at seq {})",
                            k.0,
                            k.1,
                            *total,
                            u(e, "seq")
                        ),
                    });
                }
            }
        }
    }
}

/// Whether what the sender of a received frame sent can be judged: its
/// ring is in the dump, complete, and recorded past the receive. (A worker
/// records a send after enqueueing it, so a dump taken in between shows
/// the receive without the send.)
fn sender_settled(workers: &[&Ring], received: &Value) -> bool {
    let at = u(received, "t_ns");
    let src = received.get("src").and_then(Value::as_u64);
    src.is_some_and(|src| {
        workers.iter().any(|r| {
            r.worker == Some(src as usize)
                && r.complete
                && r.events.last().is_some_and(|last| u(last, "t_ns") > at)
        })
    })
}

/// (c): every success comes from the decision module.
fn outside_s2b(rings: &[Ring], workers: &[&Ring], verdict: &mut Verdict) {
    let mut reported_ok: BTreeMap<String, u64> = BTreeMap::new();
    for ring in workers {
        let worker = ring.worker.unwrap_or_default();
        let mut succeeded = false;
        let mut returned_ok = false;
        for e in &ring.events {
            match ev(e) {
                "decision" if s(e, "outcome") == "succeeded" => succeeded = true,
                "exit" if s(e, "result") == "ok-true" => {
                    returned_ok = true;
                    let source = s(e, "source");
                    if source != "s2b" {
                        verdict.findings.push(Finding {
                            hypothesis: Hypothesis::SuccessOutsideS2b,
                            node: ring.node.clone(),
                            worker,
                            detail: format!(
                                "returned success from `{source}`, not the S2b decision (seq {})",
                                u(e, "seq")
                            ),
                        });
                    } else if !succeeded && ring.complete {
                        verdict.findings.push(Finding {
                            hypothesis: Hypothesis::SuccessOutsideS2b,
                            node: ring.node.clone(),
                            worker,
                            detail: format!(
                                "returned success (seq {}) with no S2b success decision before it",
                                u(e, "seq")
                            ),
                        });
                    }
                }
                "reported" if e.get("success") == Some(&Value::Bool(true)) => {
                    *reported_ok.entry(ring.node.clone()).or_default() += 1;
                    if !returned_ok && ring.complete {
                        verdict.findings.push(Finding {
                            hypothesis: Hypothesis::SuccessOutsideS2b,
                            node: ring.node.clone(),
                            worker,
                            detail: format!(
                                "the coordinator was told it succeeded (seq {}), but it never \
                                 returned success",
                                u(e, "seq")
                            ),
                        });
                    }
                }
                _ => {}
            }
        }
    }
    // Every success a node's coordinator applied came from one of its
    // workers (each bridge ring of the node records the same ones). Only
    // what it applied before every worker ring of the node was read counts:
    // a worker reports before its coordinator applies it.
    let mut seen = BTreeSet::new();
    for ring in rings.iter().filter(|r| r.peer.is_some()) {
        if !seen.insert(ring.node.clone()) {
            continue;
        }
        let local: Vec<&&Ring> = workers.iter().filter(|w| w.node == ring.node).collect();
        let read = local
            .iter()
            .filter_map(|w| w.snapshot_t_ns)
            .min()
            .unwrap_or(u64::MAX);
        let told = ring
            .events
            .iter()
            .filter(|e| {
                ev(e) == "coordinator"
                    && s(e, "action") == "worker-finished"
                    && e.get("success") == Some(&Value::Bool(true))
                    && u(e, "t_ns") < read
            })
            .count() as u64;
        let reported = reported_ok.get(&ring.node).copied().unwrap_or(0);
        let complete = ring.complete && local.iter().all(|w| w.complete);
        if told > reported && complete {
            verdict.findings.push(Finding {
                hypothesis: Hypothesis::SuccessOutsideS2b,
                node: ring.node.clone(),
                worker: COORDINATOR,
                detail: format!(
                    "the coordinator applied {told} worker success(es), its workers reported \
                     {reported}"
                ),
            });
        }
    }
    coordinator_side(rings, workers, verdict);
}

/// The coordinator's `goodbye-sent` in `ring` (a bridge ring: to its peer).
fn goodbye(ring: &Ring) -> Option<u64> {
    ring.events
        .iter()
        .find(|e| ev(e) == "coordinator" && s(e, "action") == "goodbye-sent")
        .map(|e| u(e, "t_ns"))
}

/// (c) at the coordinators: a node says goodbye only once each of its
/// workers has reported success, and a link retires on its peer's
/// end-of-stream only once every progress channel from the peer has ended,
/// after the peer said goodbye.
fn coordinator_side(rings: &[Ring], workers: &[&Ring], verdict: &mut Verdict) {
    let bridges: Vec<&Ring> = rings.iter().filter(|r| r.peer.is_some()).collect();
    // A node's goodbye: its first, to any peer.
    let mut goodbyes: BTreeMap<&str, u64> = BTreeMap::new();
    for ring in &bridges {
        if let Some(at) = goodbye(ring) {
            let first = goodbyes.entry(ring.node.as_str()).or_insert(at);
            *first = (*first).min(at);
        }
    }
    for (&node, &at) in &goodbyes {
        for ring in workers.iter().filter(|w| w.node == node) {
            let worker = ring.worker.unwrap_or_default();
            let reported = ring.events.iter().any(|e| {
                ev(e) == "reported"
                    && e.get("success") == Some(&Value::Bool(true))
                    && u(e, "t_ns") <= at
            });
            if reported {
                continue;
            }
            if settled(ring, at) {
                verdict.findings.push(Finding {
                    hypothesis: Hypothesis::SuccessOutsideS2b,
                    node: node.to_string(),
                    worker,
                    detail: format!(
                        "{node} said goodbye (t_ns {at}) before this worker reported success"
                    ),
                });
            } else {
                verdict.gaps.push(format!(
                    "{node}/w{worker}: not read in full past {node}'s goodbye (t_ns {at}); \
                     whether it had reported success then is not judged"
                ));
            }
        }
    }
    for ring in &bridges {
        let peer = ring.peer.as_deref().unwrap_or_default();
        // The peer's ring of its link to this node (absent: it runs in
        // another process).
        let back = bridges
            .iter()
            .find(|r| r.node == peer && r.peer.as_deref() == Some(ring.node.as_str()));
        for e in &ring.events {
            if ev(e) != "link"
                || s(e, "event") != "progress-end-of-stream"
                || e.get("retired_now") != Some(&Value::Bool(true))
            {
                continue;
            }
            let at = u(e, "t_ns");
            let (ended, expected) = (u(e, "end_of_streams"), u(e, "expected"));
            if ended < expected {
                verdict.findings.push(Finding {
                    hypothesis: Hypothesis::SuccessOutsideS2b,
                    node: ring.node.clone(),
                    worker: COORDINATOR,
                    detail: format!(
                        "retired the link to {peer} on an end-of-stream (seq {}, t_ns {at}) with \
                         {ended} of its {expected} progress channels ended",
                        u(e, "seq")
                    ),
                });
            }
            let Some(back) = back else {
                continue;
            };
            let said = goodbye(back);
            if said.is_some_and(|g| g <= at) {
                continue;
            }
            if settled(back, at) {
                verdict.findings.push(Finding {
                    hypothesis: Hypothesis::SuccessOutsideS2b,
                    node: ring.node.clone(),
                    worker: COORDINATOR,
                    detail: format!(
                        "retired the link to {peer} on an end-of-stream (seq {}, t_ns {at}) before \
                         {peer} said goodbye to it ({})",
                        u(e, "seq"),
                        said.map_or("never".to_string(), |g| format!("at t_ns {g}"))
                    ),
                });
            } else {
                verdict.gaps.push(format!(
                    "{peer}'s link ring to {} not read in full past {}'s retirement of the link \
                     (t_ns {at}); whether {peer} had said goodbye then is not judged",
                    ring.node, ring.node
                ));
            }
        }
    }
}

/// What the sender of records it pushed at event `at` of its ring had
/// committed at the records' time: the changes at the edge's target (the
/// `+n` for records on it) and at its source (the capability), by commit,
/// and the last commit before the push. Evidence for (a)'s "release
/// without `+n`" variant: the `+n` for records is counted when the output
/// is taken (possibly sweeps before the push) and consolidated per commit,
/// so this is listed, not judged.
fn committed_around(ring: &Ring, at: usize, graph: &Graph) -> String {
    let e = &ring.events[at];
    let Some(edge) = e.get("edge").and_then(Value::as_u64) else {
        return "edge unknown".to_string();
    };
    let (Some(&(sop, sport)), Some(&(top, tport))) =
        (graph.sources.get(&edge), graph.targets.get(&edge))
    else {
        return format!("edge {edge} not in the header");
    };
    let time = key(e.get("time"));
    let mut target = Vec::new();
    let mut source = Vec::new();
    let mut before = None;
    for (index, c) in ring.events.iter().enumerate() {
        match ev(c) {
            "commit" if index < at => before = Some(u(c, "commit")),
            "commit-change" if u(c, "scope") == 0 && key(c.get("time")) == time => {
                let at = (u(c, "loc_node"), u(c, "loc_port"));
                let change = format!("commit {} {:+}", u(c, "commit"), i(c, "diff"));
                match s(c, "loc") {
                    "tgt" if at == (top, tport) => target.push(change),
                    "src" if at == (sop, sport) => source.push(change),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    let list = |changes: Vec<String>| {
        if changes.is_empty() {
            "none".to_string()
        } else {
            changes.join(", ")
        }
    };
    format!(
        "edge {edge}: sender's changes at {top}[in {tport}]: {}; at {sop}[out {sport}]: {}; \
         pushed after commit {}{}",
        list(target),
        list(source),
        before.map_or("none".to_string(), |c| c.to_string()),
        if ring.complete {
            ""
        } else {
            " (ring truncated)"
        }
    )
}
/// Records lost to a retired link, what the sending node saw retire it,
/// and the sender's `+n` for them.
fn lost_pushes(rings: &[Ring], workers: &[&Ring], graph: &Graph, verdict: &mut Verdict) {
    for ring in workers {
        for (at, e) in ring.events.iter().enumerate() {
            if ev(e) != "net-send" || !s(e, "outcome").starts_with("lost") {
                continue;
            }
            let dst = e.get("dst").and_then(Value::as_u64);
            // The destination worker's node, then this node's bridge to it.
            let peer = dst.and_then(|dst| {
                workers
                    .iter()
                    .find(|r| r.worker == Some(dst as usize))
                    .map(|r| r.node.clone())
            });
            let retired_by: Vec<String> = rings
                .iter()
                .filter(|r| r.node == ring.node && r.peer.is_some() && r.peer == peer)
                .flat_map(|r| r.events.iter())
                .filter(|l| {
                    (ev(l) == "link" && l.get("retired_now") == Some(&Value::Bool(true)))
                        || ev(l) == "retired"
                })
                .map(|l| {
                    if ev(l) == "link" {
                        format!("{} at t_ns {}", s(l, "event"), u(l, "t_ns"))
                    } else {
                        format!("retire() at t_ns {}", u(l, "t_ns"))
                    }
                })
                .collect();
            verdict.lost.push(LostPush {
                node: ring.node.clone(),
                worker: ring.worker.unwrap_or_default(),
                channel: u(e, "channel"),
                dst,
                time: e.get("time").cloned().unwrap_or(Value::Null),
                records: u(e, "records"),
                outcome: s(e, "outcome").to_string(),
                retired_by,
                committed: committed_around(ring, at, graph),
            });
        }
    }
}
