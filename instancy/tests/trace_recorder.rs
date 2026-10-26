//! The progress flight recorder (`progress::trace`): what it dumps, when,
//! and what its analyser reads from a dump.
//!
//! Every test here changes the process's dump destination, so they run one
//! at a time. Each fault is injected at a time no record uses (`UNUSED` and
//! up), as in `tests/seals.rs`.

#![cfg(any(debug_assertions, feature = "progress-check"))]

#[cfg(feature = "transport")]
#[path = "support/teardown.rs"]
mod support;
#[path = "support/trace_verdict.rs"]
mod trace_verdict;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use instancy::progress::check::{self, Mode};
use instancy::progress::trace;
use instancy::{DataflowBuilder, DataflowId, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};
use serde_json::Value;
use trace_verdict::{Hypothesis, trace_verdict};

/// A time no record of these tests uses.
const UNUSED: u64 = 2_000_000;

/// Tests run one at a time: the dump destination is process-wide.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A fresh, empty dump destination for one test.
fn fresh_dir(what: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "instancy-trace-{what}-{}",
        DataflowId::new().as_uuid()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// `in -> map -> sink`, with a missing `+n` (a `-1` at `map`'s input) on
/// worker 0 at `time`.
fn build(builder: &mut DataflowBuilder<u64>, time: u64) -> Result<()> {
    builder.check_inject_count(0, "map", 0, time, -1);
    builder
        .input::<u64>("in")?
        .map("map", |_t, x| x + 1)
        .for_each("sink", |_t, _x| {});
    Ok(())
}

fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .unwrap_or_else(|_| panic!("{what}: hung"))
}

fn assert_residual(what: &str, result: &Result<()>) {
    match result {
        Err(error) => assert!(
            error
                .to_string()
                .contains("progress residual at completion"),
            "{what}: expected a proven residual, got: {error}"
        ),
        Ok(()) => panic!("{what}: succeeded with a residual"),
    }
}

/// One ring of a dump: its header and its events.
struct DumpRing {
    #[cfg_attr(not(feature = "transport"), allow(dead_code))]
    header: Value,
    events: Vec<Value>,
}

/// Every ring of the dump in `dir`, by file name.
fn read_dump(dir: &Path) -> std::collections::BTreeMap<String, DumpRing> {
    let mut rings = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut lines = text
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap());
        let header = lines.next().expect("a ring header");
        rings.insert(
            path.file_name().unwrap().to_string_lossy().into_owned(),
            DumpRing {
                header,
                events: lines.collect(),
            },
        );
    }
    rings
}

fn kinds(rings: &std::collections::BTreeMap<String, DumpRing>) -> BTreeSet<String> {
    rings
        .values()
        .flat_map(|r| r.events.iter())
        .filter_map(|e| e["ev"].as_str().map(str::to_string))
        .collect()
}

fn assert_kinds(
    what: &str,
    rings: &std::collections::BTreeMap<String, DumpRing>,
    expected: &[&str],
) {
    let found = kinds(rings);
    let missing: Vec<&&str> = expected.iter().filter(|k| !found.contains(**k)).collect();
    assert!(
        missing.is_empty(),
        "{what}: the dump lacks {missing:?}; it has {found:?}"
    );
}

/// Checks a residual dump: the residual decision lists the injected count,
/// and the analyser names its location and time.
fn assert_residual_dump(what: &str, root: &Path, time: u64) {
    let reason = std::fs::read_to_string(root.join("reason.txt")).unwrap();
    assert!(reason.starts_with("residual:"), "{what}: {reason}");
    let header: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("header.json")).unwrap()).unwrap();
    assert_eq!(header["trigger"], "residual", "{what}");
    assert!(header["legend"]["t_ns"].is_string(), "{what}: the legend");
    let rings = read_dump(root);
    let residual = rings
        .values()
        .flat_map(|r| r.events.iter())
        .any(|e| e["ev"] == "decision-count" && e["time"] == time && e["diff"] == -1);
    assert!(
        residual,
        "{what}: no decision lists the injected -1 @ {time}"
    );

    let verdict = trace_verdict(root);
    let found = verdict.residuals.iter().any(|r| {
        r.operator.as_deref() == Some("map")
            && r.loc == "tgt"
            && r.loc_port == 0
            && r.time == time
            && r.diff == -1
    });
    assert!(
        found,
        "{what}: the analyser must name map[in 0] @ {time}: -1\n{verdict}"
    );
    assert!(
        verdict.hypotheses().is_empty(),
        "{what}: an injected residual is none of (a), (b), (c)\n{verdict}"
    );
}

/// The event kinds a run of `in -> map -> sink` exercises on every worker.
const WORKER_EVENTS: &[&str] = &[
    "scope",
    "seed",
    "commit-change",
    "commit",
    "batch-change",
    "batch-applied",
    "lane-state",
    "seal-sent",
    "decision",
    "decision-count",
    "decision-lane",
    "shut-down",
    "exit",
    "note",
    "output-taken",
    "output-push",
    "input-pull",
];

/// Acceptance test 1, on two in-process workers: the run fails with the
/// residual, and the dump has both workers' rings with every event kind the
/// run exercised.
#[test]
fn an_injected_residual_is_dumped_on_two_workers() {
    let _serial = serial();
    check::set_mode(Mode::Report);
    let dir = fresh_dir("residual-local");
    trace::set_dir(Some(dir.clone()));
    let time = UNUSED + 1;
    let mark = check::mark();
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "trace-residual",
            2,
            move |builder: &mut DataflowBuilder<u64>| build(builder, time),
            SpawnOptions::default(),
        )
        .unwrap();
    for worker in 0..2 {
        let input = multi.take_input::<u64>(worker, "in").unwrap();
        input.send(1, vec![worker as u64; 3]).unwrap();
        drop(input);
    }
    let result = with_timeout("two workers", move || multi.join_blocking());
    trace::set_dir(None);
    assert_residual("two workers", &result);
    let id = check::oracles_since(mark)
        .into_iter()
        .find_map(|o| o.dataflow)
        .expect("the run's dataflow");
    let root = dir.join(id.as_uuid().to_string());
    let rings = read_dump(&root);
    assert_eq!(
        rings.keys().cloned().collect::<Vec<_>>(),
        ["local-w0.jsonl", "local-w1.jsonl"],
        "a ring per worker"
    );
    assert_kinds("two workers", &rings, WORKER_EVENTS);
    assert_residual_dump("two workers", &root, time);
    drop(rt);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A checker finding made while the oracle's state is locked (V3: an
/// unjustified output) is dumped once the lock is released, whichever
/// thread applies it: the worker (panic mode) or the checker's background
/// thread (report mode, after its pass).
#[test]
fn a_checker_finding_is_dumped_in_either_mode() {
    let _serial = serial();
    for mode in [Mode::Panic, Mode::Report] {
        check::set_mode(mode);
        let dir = fresh_dir("checker");
        trace::set_dir(Some(dir.clone()));
        let mark = check::mark();
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi(
                "trace-v3",
                2,
                |builder: &mut DataflowBuilder<u64>| {
                    builder
                        .input::<u64>("in")?
                        .unary("late", |input, output| {
                            while let Some((time, data)) = input.next() {
                                output.push_vec(time - 1, data);
                            }
                            Ok(())
                        })
                        .for_each("sink", |_t, _x| {});
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        for worker in 0..2 {
            let input = multi.take_input::<u64>(worker, "in").unwrap();
            input.send(5, vec![1]).unwrap();
            drop(input);
        }
        let result = with_timeout("v3", move || multi.join_blocking());
        let error = result.expect_err("an unjustified output fails the run");
        assert!(
            error.to_string().contains("unjustified"),
            "{mode:?}: {error}"
        );
        // Every queued finding applied (and its dump written).
        let found = check::violations_since(mark);
        trace::set_dir(None);
        assert!(
            found.iter().any(|v| v.to_string().contains("unjustified")),
            "{mode:?}: the checker records V3: {found:?}"
        );
        let id = check::oracles_since(mark)
            .into_iter()
            .find_map(|o| o.dataflow)
            .expect("the run's dataflow");
        let root = dir.join(id.as_uuid().to_string());
        let checker = std::iter::once(root.clone())
            .chain(
                std::fs::read_dir(&root)
                    .unwrap_or_else(|e| panic!("{mode:?}: no dump at {}: {e}", root.display()))
                    .filter_map(|entry| Some(entry.ok()?.path()))
                    .filter(|path| path.is_dir()),
            )
            .filter_map(|dump| std::fs::read_to_string(dump.join("header.json")).ok())
            .filter_map(|text| serde_json::from_str::<Value>(&text).ok())
            .find(|header| header["trigger"] == "checker");
        let checker = checker.unwrap_or_else(|| panic!("{mode:?}: no checker dump"));
        assert!(
            checker["reason"]
                .as_str()
                .is_some_and(|r| r.contains("V3 unjustified")),
            "{mode:?}: {}",
            checker["reason"]
        );
        drop(rt);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Acceptance test 1, on a two-node cluster: the dump also has both
/// nodes' bridge rings, with the progress frames each received.
#[cfg(feature = "transport")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::await_holding_lock)]
async fn an_injected_residual_is_dumped_on_both_nodes_of_a_cluster() {
    use instancy::{ClusterTopology, NodeConfig};
    use support::{Mesh, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    let _serial = serial();
    check::set_mode(Mode::Report);
    let dir = fresh_dir("residual-cluster");
    trace::set_dir(Some(dir.clone()));
    let time = UNUSED + 2;
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let mut mesh = Mesh::new(&["node-a", "node-b"], 64 * 1024, |_, _| Duration::ZERO);
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        id,
        mesh.transport("node-a", false, 2),
        1,
        move |builder| build(builder, time),
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.transport("node-b", false, 2),
        1,
        move |builder| build(builder, time),
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    for node in [&mut ca, &mut cb] {
        let input = node.take_input::<u64>(0, "in").unwrap();
        input.send(1, vec![7; 3]).unwrap();
        drop(input);
    }
    let ja = join_with_timeout(ca, Duration::from_secs(30));
    let jb = join_with_timeout(cb, Duration::from_secs(30));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    drop_runtimes(vec![rt_a, rt_b]).await;
    let root = dir.join(id.as_uuid().to_string());
    // The final dump is written when the dataflow's last recorder drops,
    // which may be shortly after its runtimes are.
    let final_dump = {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let found = std::fs::read_dir(&root)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.path())
                .find(|p| p.to_string_lossy().ends_with("-final") && p.join("reason.txt").exists());
            if let Some(found) = found {
                break found;
            }
            assert!(std::time::Instant::now() < deadline, "a final dump");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    trace::set_dir(None);
    for (node, result) in [("node-a", &ra), ("node-b", &rb)] {
        match result {
            Some(result) => assert_residual(node, result),
            None => panic!("{node}: {}", fmt_join(result)),
        }
    }
    let rings = read_dump(&root);
    assert_eq!(
        rings.keys().cloned().collect::<Vec<_>>(),
        [
            "node-a-bridge-node-b.jsonl",
            "node-a-w0.jsonl",
            "node-b-bridge-node-a.jsonl",
            "node-b-w1.jsonl"
        ],
        "a ring per worker and per bridge"
    );
    for (name, ring) in &rings {
        assert_eq!(ring.header["dataflow"], id.as_uuid().to_string(), "{name}");
    }
    let mut expected = WORKER_EVENTS.to_vec();
    expected.push("progress-in");
    assert_kinds("cluster", &rings, &expected);
    assert_residual_dump("cluster", &root, time);
    // Once the dataflow ended, a final dump holds every ring's end: each
    // worker's exit (from the residual it proved, or cancelled by its
    // peer's) and what its coordinator was told.
    let rings = read_dump(&final_dump);
    let mut from_residual = 0;
    for worker in ["node-a-w0.jsonl", "node-b-w1.jsonl"] {
        let events = &rings[worker].events;
        let exits: Vec<&Value> = events.iter().filter(|e| e["ev"] == "exit").collect();
        assert!(
            exits.len() == 1
                && exits[0]["result"] == "err"
                && matches!(exits[0]["source"].as_str(), Some("residual" | "cancelled")),
            "{worker}: its exit, from the residual or cancelled: {exits:?}"
        );
        if exits[0]["source"] == "residual" {
            from_residual += 1;
        }
        assert!(
            events
                .iter()
                .any(|e| e["ev"] == "reported" && e["success"] == false),
            "{worker}: what its coordinator was told"
        );
    }
    assert!(from_residual > 0, "a worker proved the residual");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A successful two-node exchange, dumped on request: every success comes
/// with its S2b decision and the lanes it applied, the coordinators retire
/// their links on the goodbyes, and the analyser finds nothing.
#[cfg(feature = "transport")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::await_holding_lock)]
async fn a_successful_exchange_dumps_clean_evidence_on_request() {
    use instancy::{ClusterTopology, NodeConfig};
    use support::{Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    let _serial = serial();
    check::set_mode(Mode::Report);
    let dir = fresh_dir("success");
    trace::set_dir(Some(dir.clone()));
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 2),
    ])
    .unwrap();
    let id = DataflowId::new();
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("in")?
            .exchange_by_hash("spread", |x: &u64| *x)
            .map("inc", |_t, x| x + 1)
            .exchange_by_hash("to-w0", |_x: &u64| 0)
            .output("out")?;
        Ok(())
    };
    let mut mesh = Mesh::new(&["node-a", "node-b"], 64 * 1024, |_, _| Duration::ZERO);
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        id,
        mesh.transport("node-a", false, 2),
        2,
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.transport("node-b", false, 2),
        2,
        build,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let mut collectors = Vec::new();
    for (node, workers) in [(&mut ca, 1), (&mut cb, 2)] {
        for local in 0..workers {
            let input = node.take_input::<u64>(local, "in").unwrap();
            for epoch in 0..3u64 {
                input.send(epoch, (0..20).collect()).unwrap();
            }
            drop(input);
            collectors.push(collect(node.take_output::<u64>(local, "out").unwrap()));
        }
    }
    let ja = join_with_timeout(ca, Duration::from_secs(30));
    let jb = join_with_timeout(cb, Duration::from_secs(30));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    let records: usize = collectors
        .into_iter()
        .map(|c| c.join().unwrap().len())
        .sum();
    // Dumped while every node's coordinator may still hold its links: the
    // registry keeps the rings of a finished dataflow in any case.
    drop_runtimes(vec![rt_a, rt_b]).await;
    let root = check::dump_traces(id, "the test asked").expect("a dump");
    trace::set_dir(None);
    for (node, result) in [("node-a", &ra), ("node-b", &rb)] {
        assert!(
            matches!(result, Some(Ok(()))),
            "{node}: {}",
            fmt_join(result)
        );
    }
    assert_eq!(records, 3 * 20 * 3, "every record, once");
    let rings = read_dump(&root);
    assert_eq!(rings.len(), 3 + 2, "three workers and two bridges");
    let mut expected = WORKER_EVENTS.to_vec();
    expected.retain(|k| !matches!(*k, "decision-count" | "note" | "lane-state"));
    expected.extend([
        "target-push",
        "source-pull",
        "net-send",
        "net-recv",
        "frame-in",
        "progress-in",
        "link",
        "retired",
        "coordinator",
        "reported",
    ]);
    assert_kinds("success", &rings, &expected);
    for (name, ring) in rings.iter().filter(|(_, r)| r.header["worker"].is_u64()) {
        let decided = ring
            .events
            .iter()
            .rposition(|e| e["ev"] == "decision" && e["outcome"] == "succeeded")
            .unwrap_or_else(|| panic!("{name}: an S2b decision"));
        let lanes = ring.events[decided + 1..]
            .iter()
            .take_while(|e| e["ev"] == "decision-lane")
            .count();
        // A worker succeeds on its own scope: its peers may still run.
        assert_eq!(lanes, 2, "{name}: each peer lane's position at the success");
        assert!(
            ring.events
                .iter()
                .any(|e| e["ev"] == "exit" && e["result"] == "ok-true" && e["source"] == "s2b"),
            "{name}: returned success from S2b"
        );
    }
    let header: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("header.json")).unwrap()).unwrap();
    assert!(
        header["exchange_edges"]
            .as_object()
            .is_some_and(|e| e.len() == 2),
        "the exchange edges of the network channel ids: {}",
        header["exchange_edges"]
    );
    // Every data frame enqueued on a channel was received, under the same
    // number.
    let mut frames: std::collections::BTreeMap<(u64, &str), Vec<u64>> = Default::default();
    for ring in rings.values() {
        for e in &ring.events {
            let side = match (e["ev"].as_str(), e["what"].as_str()) {
                (Some("net-send"), _) if e["outcome"] == "accepted" => "sent",
                (Some("net-recv"), Some("data")) => "received",
                _ => continue,
            };
            frames
                .entry((e["channel"].as_u64().unwrap(), side))
                .or_default()
                .push(e["frame"].as_u64().expect("a data frame's number"));
        }
    }
    assert!(!frames.is_empty(), "data crossed the network");
    // The coordinators' side of (c) is judged on real events: each node said
    // goodbye to its peer and retired its link, on the peer's end-of-stream
    // or on its ack of ours (whichever came first). A node acks only once
    // every end-of-stream has come, so at least one link retired on them.
    let mut on_end_of_stream = 0;
    for (name, ring) in rings.iter().filter(|(_, r)| r.header["peer"].is_string()) {
        assert!(
            ring.events
                .iter()
                .any(|e| e["ev"] == "coordinator" && e["action"] == "goodbye-sent"),
            "{name}: goodbye-sent"
        );
        let retired = ring
            .events
            .iter()
            .find(|e| e["ev"] == "link" && e["retired_now"] == true)
            .unwrap_or_else(|| panic!("{name}: the link retired"));
        if retired["event"] == "progress-end-of-stream" {
            on_end_of_stream += 1;
        } else {
            assert_eq!(retired["event"], "ack-received", "{name}: {retired}");
        }
    }
    assert!(on_end_of_stream > 0, "a link retired on an end-of-stream");
    for ((channel, side), numbers) in &frames {
        assert_eq!(
            *numbers,
            (1..=numbers.len() as u64).collect::<Vec<_>>(),
            "channel {channel}: frames {side} are numbered 1, 2, ..."
        );
        assert_eq!(
            frames.get(&(*channel, "sent")).map(Vec::len),
            frames.get(&(*channel, "received")).map(Vec::len),
            "channel {channel}: every frame sent was received"
        );
    }
    let verdict = trace_verdict(&root);
    assert!(
        verdict.findings.is_empty() && verdict.lost.is_empty(),
        "a clean run has no evidence\n{verdict}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Acceptance test 4: with no destination nothing is recorded or written,
/// and the run fails the same way.
#[test]
fn without_a_destination_nothing_is_dumped() {
    let _serial = serial();
    check::set_mode(Mode::Report);
    trace::set_dir(None);
    let time = UNUSED + 3;
    let mark = check::mark();
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "trace-off",
            2,
            move |builder: &mut DataflowBuilder<u64>| build(builder, time),
            SpawnOptions::default(),
        )
        .unwrap();
    for worker in 0..2 {
        let input = multi.take_input::<u64>(worker, "in").unwrap();
        input.send(1, vec![worker as u64; 3]).unwrap();
        drop(input);
    }
    let result = with_timeout("off", move || multi.join_blocking());
    assert_residual("off", &result);
    let id = check::oracles_since(mark)
        .into_iter()
        .find_map(|o| o.dataflow)
        .expect("the run's dataflow");
    assert_eq!(
        check::dump_traces(id, "nothing to dump"),
        None,
        "no rings, no destination: nothing is written"
    );
    drop(rt);
}

// ---------------------------------------------------------------------------
// The analyser on hand-built dumps (acceptance test 2)
// ---------------------------------------------------------------------------

/// Write a dump of `rings` (`file name`, header fields, events) to a fresh
/// directory.
fn synthetic(what: &str, rings: &[(&str, Value, Vec<Value>)]) -> PathBuf {
    synthetic_with(what, Value::Object(Default::default()), rings)
}

/// [`synthetic`], with `extra` fields in the dump's header.
fn synthetic_with(what: &str, extra: Value, rings: &[(&str, Value, Vec<Value>)]) -> PathBuf {
    let dir = fresh_dir(what);
    std::fs::create_dir_all(&dir).unwrap();
    let mut header = serde_json::json!({
        "dataflow": "synthetic", "trigger": "explicit", "reason": what,
        "untraced": {}, "evicted_dataflows": 0, "operators": {"1": "in", "2": "map"},
    });
    if let (Some(header), Some(extra)) = (header.as_object_mut(), extra.as_object()) {
        header.extend(extra.clone());
    }
    std::fs::write(dir.join("header.json"), header.to_string()).unwrap();
    for (file, ring, events) in rings {
        let mut text = ring.to_string();
        text.push('\n');
        for (seq, event) in events.iter().enumerate() {
            let mut event = event.clone();
            event["seq"] = Value::from(seq);
            text.push_str(&event.to_string());
            text.push('\n');
        }
        std::fs::write(dir.join(file), text).unwrap();
    }
    dir
}

fn worker_ring(node: &str, worker: u64) -> Value {
    serde_json::json!({
        "node": node, "worker": worker, "total_workers": 2, "capacity": 16384,
        "written": 0, "retained": 0, "overwritten": 0, "torn": 0, "lane_drops": 0,
    })
}

#[test]
fn the_analyser_tells_the_three_hypotheses_apart() {
    use serde_json::json;
    // (a): node-b's worker 1 committed a +3 for node-a's worker 0 in its
    // second commit (frames 1..2 of its network lane) before worker 0
    // succeeded with only one batch of that lane applied.
    let missing = synthetic(
        "a",
        &[
            (
                "node-a-w0.jsonl",
                worker_ring("node-a", 0),
                vec![
                    json!({"ev": "batch-applied", "t_ns": 20, "from": 1, "lane_batch": 1, "changes": 1}),
                    json!({"ev": "decision", "t_ns": 100, "outcome": "succeeded", "nonzero": 0}),
                    json!({"ev": "decision-lane", "t_ns": 100, "from": 1, "lane_batches": 1, "sealed": true, "closed": false}),
                    json!({"ev": "exit", "t_ns": 101, "result": "ok-true", "source": "s2b"}),
                ],
            ),
            (
                "node-b-w1.jsonl",
                worker_ring("node-b", 1),
                vec![
                    json!({"ev": "commit-change", "t_ns": 9, "scope": 0, "loc": "src", "loc_node": 1, "loc_port": 0, "time": 0, "diff": -1, "commit": 1}),
                    json!({"ev": "commit", "t_ns": 10, "commit": 1, "frames": 1, "frames_before": 0}),
                    json!({"ev": "commit-change", "t_ns": 49, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0, "time": 4, "diff": 3, "commit": 2}),
                    json!({"ev": "commit", "t_ns": 50, "commit": 2, "frames": 1, "frames_before": 1}),
                ],
            ),
        ],
    );
    // (b): one worker produced 4 records at time 5 and its exchange
    // delivered them twice (once to each target).
    let duplicate = synthetic(
        "b",
        &[(
            "local-w0.jsonl",
            worker_ring("local", 0),
            vec![
                json!({"ev": "output-taken", "t_ns": 1, "operator": 1, "port": 0, "time": 5, "records": 4}),
                json!({"ev": "target-push", "t_ns": 2, "via": "exchange", "pusher": 7, "target": 0, "operator": 1, "port": 0, "time": 5, "records": 4, "outcome": "accepted"}),
                json!({"ev": "target-push", "t_ns": 3, "via": "exchange", "pusher": 7, "target": 1, "operator": 1, "port": 0, "time": 5, "records": 4, "outcome": "accepted"}),
            ],
        )],
    );
    // (c): a worker returned and reported success without any S2b decision.
    let outside = synthetic(
        "c",
        &[(
            "node-a-w0.jsonl",
            worker_ring("node-a", 0),
            vec![
                json!({"ev": "decision", "t_ns": 1, "outcome": "not-ready", "nonzero": 0}),
                json!({"ev": "exit", "t_ns": 2, "result": "ok-true", "source": "unknown"}),
                json!({"ev": "reported", "t_ns": 3, "success": true}),
            ],
        )],
    );
    for (dir, expected, named) in [
        (&missing, Hypothesis::MissingPlus, "commit 2"),
        (&duplicate, Hypothesis::DuplicatePush, "seq 2 to target 1"),
        (&outside, Hypothesis::SuccessOutsideS2b, "`unknown`"),
    ] {
        let verdict = trace_verdict(dir);
        assert_eq!(
            verdict.hypotheses(),
            BTreeSet::from([expected]),
            "{}\n{verdict}",
            dir.display()
        );
        assert!(
            verdict.findings.iter().any(|f| f.detail.contains(named)),
            "the finding names {named}\n{verdict}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}

fn bridge_ring(node: &str, peer: &str) -> Value {
    serde_json::json!({
        "node": node, "peer": peer, "total_workers": 2, "capacity": 16384,
        "written": 0, "retained": 0, "overwritten": 0, "torn": 0, "lane_drops": 0,
    })
}

/// (c) at the coordinators, on a two-node cluster (node-a: worker 0,
/// node-b: worker 1): a goodbye before a worker reported success, and a
/// link retired on an end-of-stream too early (not every progress channel
/// ended, or before the peer's goodbye). A worker ring read before the
/// goodbye is a gap, not a finding.
#[test]
fn the_analyser_judges_the_coordinators_side_of_c() {
    use serde_json::json;
    // A clean run: node-a's worker reports at 12, node-a says goodbye at
    // 50, and node-b retires its link to node-a on the second of two
    // end-of-streams at 60.
    let run = |reported: u64, worker_read: Option<u64>, retired: u64, ended: u64| {
        let mut worker = worker_ring("node-a", 0);
        if let Some(at) = worker_read {
            worker["snapshot_t_ns"] = Value::from(at);
        }
        let mut worker_events = vec![
            json!({"ev": "decision", "t_ns": 10, "outcome": "succeeded", "nonzero": 0}),
            json!({"ev": "exit", "t_ns": 11, "result": "ok-true", "source": "s2b"}),
        ];
        if worker_read.is_none_or(|at| at > reported) {
            worker_events.push(json!({"ev": "reported", "t_ns": reported, "success": true}));
        }
        vec![
            ("node-a-w0.jsonl", worker, worker_events),
            (
                "node-a-bridge-node-b.jsonl",
                bridge_ring("node-a", "node-b"),
                vec![
                    json!({"ev": "coordinator", "t_ns": 50, "action": "goodbye-sent", "channels": 2}),
                ],
            ),
            (
                "node-b-bridge-node-a.jsonl",
                bridge_ring("node-b", "node-a"),
                vec![
                    json!({"ev": "link", "t_ns": retired, "event": "progress-end-of-stream", "end_of_streams": ended, "expected": 2, "goodbye_received": true, "ack_received": false, "retired": true, "lost": false, "retired_now": true}),
                ],
            ),
        ]
    };
    let clean = synthetic("c-clean", &run(12, None, 60, 2));
    let verdict = trace_verdict(&clean);
    assert!(verdict.findings.is_empty(), "{verdict}");
    let _ = std::fs::remove_dir_all(&clean);
    for (what, rings, named) in [
        (
            "c-goodbye",
            run(70, None, 80, 2),
            "said goodbye (t_ns 50) before this worker",
        ),
        (
            "c-channels",
            run(12, None, 60, 1),
            "with 1 of its 2 progress channels ended",
        ),
        (
            "c-retired",
            run(12, None, 40, 2),
            "before node-a said goodbye to it (at t_ns 50)",
        ),
    ] {
        let dir = synthetic(what, &rings);
        let verdict = trace_verdict(&dir);
        assert_eq!(
            verdict.hypotheses(),
            BTreeSet::from([Hypothesis::SuccessOutsideS2b]),
            "{what}\n{verdict}"
        );
        assert!(
            verdict.findings.len() == 1 && verdict.findings[0].detail.contains(named),
            "{what}: one finding, naming {named:?}\n{verdict}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    // The worker's ring was read (at 30) before the goodbye, and before its
    // report (at 40): no evidence either way.
    let early = synthetic("c-early", &run(40, Some(30), 60, 2));
    let verdict = trace_verdict(&early);
    assert!(verdict.findings.is_empty(), "{verdict}");
    assert!(
        verdict.gaps.iter().any(|g| g.contains("not judged")),
        "{verdict}"
    );
    let _ = std::fs::remove_dir_all(&early);
}

#[test]
fn the_analyser_reports_truncated_rings() {
    use serde_json::json;
    // A success without worker 1's +3: evidence for (a), had worker 0's
    // ring not been truncated.
    let mut ring = worker_ring("local", 0);
    ring["overwritten"] = Value::from(12);
    let dir = synthetic(
        "truncated",
        &[
            (
                "local-w0.jsonl",
                ring,
                vec![
                    json!({"ev": "decision", "t_ns": 100, "outcome": "succeeded", "nonzero": 0}),
                    json!({"ev": "exit", "t_ns": 101, "result": "ok-true", "source": "s2b"}),
                ],
            ),
            (
                "local-w1.jsonl",
                worker_ring("local", 1),
                vec![
                    json!({"ev": "commit-change", "t_ns": 9, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0, "time": 4, "diff": 3, "commit": 1}),
                    json!({"ev": "commit", "t_ns": 10, "commit": 1, "frames": 0, "frames_before": 0}),
                ],
            ),
        ],
    );
    let verdict = trace_verdict(&dir);
    assert!(
        verdict.findings.is_empty(),
        "a truncated ring gives gaps, not findings\n{verdict}"
    );
    for gap in ["12 event(s) overwritten", "(a) not judged"] {
        assert!(
            verdict.gaps.iter().any(|g| g.contains(gap)),
            "{gap}\n{verdict}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// (a) is judged on the receiver's scope and change by change: a peer's
/// `+n` outside the scope, or a pending fragment with only negatives, is
/// no evidence; a missing in-scope `+n` is.
#[test]
fn the_analyser_judges_a_on_the_scope_and_the_changes_applied() {
    use serde_json::json;
    // Worker 1's commit 1, split over two frames: the +3 (frame 1) and the
    // release (frame 2); worker 0 applied frame 1 only.
    let sender = || {
        (
            "node-b-w1.jsonl",
            worker_ring("node-b", 1),
            vec![
                json!({"ev": "commit-change", "t_ns": 9, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0, "time": 4, "diff": 3, "commit": 1}),
                json!({"ev": "commit-change", "t_ns": 9, "scope": 0, "loc": "src", "loc_node": 1, "loc_port": 0, "time": 4, "diff": -1, "commit": 1}),
                json!({"ev": "commit", "t_ns": 10, "commit": 1, "frames": 2, "frames_before": 0}),
            ],
        )
    };
    let receiver = |scope: Vec<Value>, applied: Vec<Value>| {
        let mut events = scope;
        events.extend(applied);
        events.extend([
            json!({"ev": "decision", "t_ns": 100, "outcome": "succeeded", "nonzero": 0}),
            json!({"ev": "exit", "t_ns": 101, "result": "ok-true", "source": "s2b"}),
        ]);
        ("node-a-w0.jsonl", worker_ring("node-a", 0), events)
    };
    let plus_applied = vec![
        json!({"ev": "batch-change", "t_ns": 20, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0, "time": 4, "diff": 3, "from": 1, "lane_batch": 1}),
        json!({"ev": "batch-applied", "t_ns": 20, "from": 1, "lane_batch": 1, "changes": 1}),
    ];
    let in_scope = vec![
        json!({"ev": "scope", "t_ns": 1, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0}),
        json!({"ev": "scope", "t_ns": 1, "scope": 0, "loc": "src", "loc_node": 1, "loc_port": 0}),
    ];
    let out_of_scope = vec![
        json!({"ev": "scope", "t_ns": 1, "scope": 0, "loc": "src", "loc_node": 3, "loc_port": 0}),
    ];
    // Frame 2 (the release alone) still pending: every +n was applied.
    let tail = synthetic(
        "a-tail",
        &[receiver(in_scope.clone(), plus_applied), sender()],
    );
    // Nothing applied, but the +3 is outside worker 0's scope.
    let outside = synthetic("a-outside", &[receiver(out_of_scope, Vec::new()), sender()]);
    // Nothing applied and the +3 is in scope.
    let missing = synthetic("a-missing", &[receiver(in_scope, Vec::new()), sender()]);
    for dir in [&tail, &outside] {
        let verdict = trace_verdict(dir);
        assert!(verdict.findings.is_empty(), "{}\n{verdict}", dir.display());
        let _ = std::fs::remove_dir_all(dir);
    }
    let verdict = trace_verdict(&missing);
    assert_eq!(
        verdict.hypotheses(),
        BTreeSet::from([Hypothesis::MissingPlus]),
        "{verdict}"
    );
    assert!(
        verdict.findings[0].detail.contains("commit 1")
            && verdict.findings[0].detail.contains("1..=2")
            && verdict.findings[0].detail.contains("+3"),
        "names the commit, its lane batches and the +n\n{verdict}"
    );
    let _ = std::fs::remove_dir_all(&missing);
}

/// (b) at the transport: a channel that delivers a data frame twice
/// (frame numbers past what its sender enqueued) or sends more records over
/// the network to one worker than were produced; a broadcast's copies to
/// several workers, or a receive whose send the sender had not recorded yet
/// when the dump was taken, are no evidence. A lost push lists what its
/// sender had committed at its time.
#[test]
fn the_analyser_finds_transport_duplicates_and_lists_lost_pushes() {
    use serde_json::json;
    const CHANNEL: u64 = 9223372036854775809;
    let header =
        json!({"edges": {"0": {"from": [1, 0], "to": [2, 0]}}, "exchange_edges": {"0": 0}});
    let sender = |more: Vec<Value>| {
        let mut events = vec![
            json!({"ev": "output-taken", "t_ns": 1, "operator": 1, "port": 0, "time": 5, "records": 2}),
            json!({"ev": "commit-change", "t_ns": 2, "scope": 0, "loc": "tgt", "loc_node": 2, "loc_port": 0, "time": 5, "diff": 2, "commit": 1}),
            json!({"ev": "commit", "t_ns": 2, "commit": 1, "frames": 1, "frames_before": 0}),
            json!({"ev": "net-send", "t_ns": 3, "channel": CHANNEL, "exchange": 0, "edge": 0, "src": 1, "dst": 0, "time": 5, "records": 2, "outcome": "accepted", "frame": 1}),
        ];
        events.extend(more);
        ("node-b-w1.jsonl", worker_ring("node-b", 1), events)
    };
    let receiver = || {
        (
            "node-a-w0.jsonl",
            worker_ring("node-a", 0),
            vec![
                json!({"ev": "net-recv", "t_ns": 4, "what": "data", "channel": CHANNEL, "exchange": 0, "edge": 0, "src": 1, "dst": 0, "time": 5, "records": 2, "frame": 1}),
                json!({"ev": "net-recv", "t_ns": 5, "what": "data", "channel": CHANNEL, "exchange": 0, "edge": 0, "src": 1, "dst": 0, "time": 5, "records": 2, "frame": 2}),
            ],
        )
    };
    let later = json!({"ev": "decision", "t_ns": 6, "outcome": "not-ready", "nonzero": 1});
    let replayed = synthetic_with(
        "b-replayed",
        header.clone(),
        &[sender(vec![later.clone()]), receiver()],
    );
    let verdict = trace_verdict(&replayed);
    assert_eq!(
        verdict.hypotheses(),
        BTreeSet::from([Hypothesis::DuplicatePush]),
        "{verdict}"
    );
    assert!(
        verdict.findings.iter().any(|f| f
            .detail
            .contains("received data frame 2, the sender enqueued 1")),
        "names the replayed frame\n{verdict}"
    );
    let _ = std::fs::remove_dir_all(&replayed);
    // The sender's ring ends before the second receive: it may not have
    // recorded that send yet.
    let unsettled = synthetic_with(
        "b-unsettled",
        header.clone(),
        &[sender(Vec::new()), receiver()],
    );
    let verdict = trace_verdict(&unsettled);
    assert!(verdict.findings.is_empty(), "{verdict}");
    let _ = std::fs::remove_dir_all(&unsettled);
    // A broadcast sends every record to each worker: the same 2 records to
    // another remote worker are no duplicate.
    let to_another = json!({"ev": "net-send", "t_ns": 7, "channel": CHANNEL + 2, "exchange": 0, "edge": 0, "src": 1, "dst": 2, "time": 5, "records": 2, "outcome": "accepted", "frame": 1});
    let broadcast = synthetic_with("b-broadcast", header.clone(), &[sender(vec![to_another])]);
    let verdict = trace_verdict(&broadcast);
    assert!(verdict.findings.is_empty(), "{verdict}");
    let _ = std::fs::remove_dir_all(&broadcast);
    // Sent over the network twice what was produced, the second time lost
    // to a retired link.
    let resent = json!({"ev": "net-send", "t_ns": 7, "channel": CHANNEL, "exchange": 0, "edge": 0, "src": 1, "dst": 0, "time": 5, "records": 2, "outcome": "lost-while-finishing", "frame": 2});
    let doubled = synthetic_with("b-doubled", header, &[sender(vec![resent])]);
    let verdict = trace_verdict(&doubled);
    assert_eq!(
        verdict.hypotheses(),
        BTreeSet::from([Hypothesis::DuplicatePush]),
        "{verdict}"
    );
    assert!(
        verdict.findings.iter().any(|f| f
            .detail
            .contains("4 record(s) sent over the network, 2 produced")),
        "{verdict}"
    );
    assert_eq!(verdict.lost.len(), 1, "{verdict}");
    assert!(
        verdict.lost[0]
            .committed
            .contains("sender's changes at 2[in 0]: commit 1 +2")
            && verdict.lost[0].committed.contains("pushed after commit 1"),
        "{verdict}"
    );
    let _ = std::fs::remove_dir_all(&doubled);
}
