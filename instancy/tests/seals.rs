//! Seals and proven residuals (design §2.2, §5.9; R39).
//!
//! A worker seals its progress stream once every operator it hosts has
//! finished or is exhausted. Once a worker has applied every worker's seal,
//! no further progress update can arrive, so a non-zero count left in its
//! view is an accounting bug, never a wait: the run fails with "progress
//! residual at completion" instead of hanging.
//!
//! Each fault is injected into one worker's first commit at a time no record
//! uses (`UNUSED` and up, one per test: the checker's log is shared by the
//! process), so it cannot mask a real record and the outcome is
//! deterministic. The same fault also trips the checker (V4, V5); these tests
//! run it in report mode so the production error surfaces, and assert both.

#![cfg(any(debug_assertions, feature = "progress-check"))]

#[cfg(feature = "transport")]
#[path = "support/teardown.rs"]
mod support;

use std::sync::mpsc;
use std::time::Duration;

use instancy::progress::check::{self, Check, Mode};
use instancy::{DataflowBuilder, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// A time no record of these tests uses (each test adds its own offset).
const UNUSED: u64 = 1_000_000;

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// A `-1` at `map`'s input: a record consumed that no channel delivered
    /// (a missing `+n`).
    MissingPlus,
    /// A `+1` at the terminal sink's input: a record counted that is never
    /// consumed (a leaked `+n` at a sink, which keeps no frontier waiting).
    LeakAtSink,
}

impl Fault {
    fn inject(self, builder: &DataflowBuilder<u64>, worker: usize, time: u64) {
        match self {
            Fault::MissingPlus => builder.check_inject_count(worker, "map", 0, time, -1),
            Fault::LeakAtSink => builder.check_inject_count(worker, "sink", 0, time, 1),
        };
    }

    /// How the residual shows in a V5 report.
    fn residual(self, time: u64) -> String {
        match self {
            Fault::MissingPlus => format!("@{time}: -1"),
            Fault::LeakAtSink => format!("@{time}: +1"),
        }
    }
}

/// `in -> map -> sink`, with `fault` injected on worker 0 at `time`.
fn build(builder: &mut DataflowBuilder<u64>, fault: Fault, time: u64) -> Result<()> {
    fault.inject(builder, 0, time);
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
        .unwrap_or_else(|_| panic!("{what}: hung instead of reporting the residual"))
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

/// The checker reported the fault too (V5, a report even in panic mode):
/// returns the workers whose view it was left in.
fn checker_reports(what: &str, mark: u64, fault: Fault, time: u64) -> Vec<usize> {
    let residual = fault.residual(time);
    let violations = check::violations_since(mark);
    let mut workers: Vec<usize> = violations
        .iter()
        .filter(|v| v.check == Check::V5 && v.detail.contains(&residual))
        .filter_map(|v| v.worker)
        .collect();
    workers.sort_unstable();
    workers.dedup();
    assert!(
        !workers.is_empty(),
        "{what}: the checker must report the residual {residual}: {violations:#?}"
    );
    workers
}

fn run_in_process(fault: Fault, workers: usize, time: u64) {
    check::set_mode(Mode::Report);
    let what = format!("{fault:?} on {workers} worker(s)");
    let mark = check::mark();
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            &format!("seals-{fault:?}-{workers}"),
            workers,
            move |builder: &mut DataflowBuilder<u64>| build(builder, fault, time),
            SpawnOptions::default(),
        )
        .unwrap();
    for worker in 0..workers {
        let input = multi.take_input::<u64>(worker, "in").unwrap();
        input.send(1, vec![worker as u64; 3]).unwrap();
        drop(input);
    }
    let result = with_timeout(&what, move || multi.join_blocking());
    assert_residual(&what, &result);
    checker_reports(&what, mark, fault, time);
}

#[test]
fn missing_plus_is_a_residual_on_one_worker() {
    run_in_process(Fault::MissingPlus, 1, UNUSED + 1);
}

#[test]
fn missing_plus_is_a_residual_on_two_workers() {
    run_in_process(Fault::MissingPlus, 2, UNUSED + 2);
}

#[test]
fn leak_at_a_terminal_sink_is_a_residual() {
    run_in_process(Fault::LeakAtSink, 1, UNUSED + 3);
    run_in_process(Fault::LeakAtSink, 2, UNUSED + 4);
}

/// Two nodes, one worker each, both exhausted: each applies the other's seal
/// and fails with the residual, naming the count the fault left.
#[cfg(feature = "transport")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn missing_plus_is_a_residual_on_both_nodes_of_a_cluster() {
    use instancy::{ClusterTopology, DataflowId, NodeConfig};
    use support::{Mesh, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    check::set_mode(Mode::Report);
    let mark = check::mark();
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let mut mesh = Mesh::new(&["node-a", "node-b"], 64 * 1024, |_, _| Duration::ZERO);
    let fault = Fault::MissingPlus;
    let time = UNUSED + 5;
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        id,
        mesh.dedicated("node-a", 2),
        1,
        move |builder| build(builder, fault, time),
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.dedicated("node-b", 2),
        1,
        move |builder| build(builder, fault, time),
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
    for (node, result) in [("node-a", &ra), ("node-b", &rb)] {
        match result {
            Some(result) => assert_residual(node, result),
            None => panic!("{node}: {}", fmt_join(result)),
        }
    }
    // Both nodes applied every seal: each reports the residual.
    assert_eq!(checker_reports("cluster", mark, fault, time), [0, 1]);
}
