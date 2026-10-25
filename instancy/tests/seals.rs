//! Seals and proven residuals (design §2.2, §5.9; R39), and failures that
//! must win over them.
//!
//! A worker seals its progress stream once every operator it hosts has
//! finished or is exhausted. Once a worker has applied every worker's seal,
//! no further progress update can arrive, so a non-zero count left in its
//! view is an accounting bug, never a wait: the run fails with "progress
//! residual at completion" instead of hanging. A worker that fails never
//! seals (R34): the dataflow fails with its error, through cancellation.
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

/// Tests run one at a time: the checker's log and oracle statistics are
/// process-wide. (Async tests hold the guard across awaits; nothing else
/// takes it.)
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

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
    let _serial = serial();
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
    // Every exhausted worker sealed (the positive control for the R34
    // tests, which assert a failed worker does not).
    let mut sealed: Vec<usize> = check::oracles_since(mark)
        .into_iter()
        .flat_map(|o| o.sealed_workers)
        .collect();
    sealed.sort_unstable();
    assert_eq!(sealed, (0..workers).collect::<Vec<_>>(), "{what}: seals");
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
#[allow(clippy::await_holding_lock)]
async fn missing_plus_is_a_residual_on_both_nodes_of_a_cluster() {
    use instancy::{ClusterTopology, DataflowId, NodeConfig};
    use support::{Mesh, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    let _serial = serial();
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
    let mut sealed: Vec<usize> = check::oracles_since(mark)
        .into_iter()
        .flat_map(|o| o.sealed_workers)
        .collect();
    sealed.sort_unstable();
    assert_eq!(sealed, [0, 1], "cluster: seals");
}

// ---------------------------------------------------------------------------
// A decode failure wins over a residual
// ---------------------------------------------------------------------------

/// A record whose decoding fails (`BRITTLE`): exchanged over a serializing
/// transport, it fails the run with a decode error.
#[cfg(feature = "transport")]
#[derive(Clone, Debug)]
struct Brittle(u64);

#[cfg(feature = "transport")]
const BRITTLE: u64 = 13;

#[cfg(feature = "transport")]
struct BrittleCodec;

#[cfg(feature = "transport")]
impl instancy::communication::codec::Codec<Brittle> for BrittleCodec {
    fn encode(
        &self,
        value: &Brittle,
        buf: &mut Vec<u8>,
    ) -> std::result::Result<(), instancy::communication::codec::CodecError> {
        buf.extend_from_slice(&value.0.to_le_bytes());
        Ok(())
    }

    fn decode(
        &self,
        buf: &[u8],
    ) -> std::result::Result<(Brittle, usize), instancy::communication::codec::CodecError> {
        use instancy::communication::codec::CodecError;
        let bytes: [u8; 8] =
            buf.get(..8)
                .and_then(|b| b.try_into().ok())
                .ok_or(CodecError::InsufficientData {
                    needed: 8,
                    available: buf.len(),
                })?;
        match u64::from_le_bytes(bytes) {
            BRITTLE => Err(CodecError::InvalidData("brittle record".into())),
            value => Ok((Brittle(value), 8)),
        }
    }
}

#[cfg(feature = "transport")]
impl instancy::communication::codec::ExchangeData for Brittle {
    type CodecType = BrittleCodec;
    fn codec() -> BrittleCodec {
        BrittleCodec
    }
}

/// `in -> map -> exchange -> unwrap -> sink`, with a missing `+n` injected on
/// worker 0 at `time`. Every record goes to worker 1.
#[cfg(feature = "transport")]
fn build_brittle(builder: &mut DataflowBuilder<u64>, time: u64) -> Result<()> {
    Fault::MissingPlus.inject(builder, 0, time);
    builder
        .input::<u64>("in")?
        .map("map", |_t, x| Brittle(x))
        .exchange_by_hash("to-worker-1", |_b: &Brittle| 1)
        .map("unwrap", |_t, b| b.0)
        .for_each("sink", |_t, _x| {});
    Ok(())
}

#[cfg(feature = "transport")]
fn assert_decode_error(what: &str, result: &Result<()>) {
    match result {
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains("brittle record") && !text.contains("progress residual"),
                "{what}: expected the decode error, got: {text}"
            );
        }
        Ok(()) => panic!("{what}: succeeded despite the decode error"),
    }
}

/// A decode failure and an injected residual on two workers over the mock
/// network (every exchanged batch encoded and decoded): the run fails
/// promptly with the decode error, not the residual and not a hang.
#[cfg(feature = "transport")]
#[test]
fn a_decode_failure_with_a_residual_fails_with_the_decode_error() {
    let _serial = serial();
    check::set_mode(Mode::Report);
    let time = UNUSED + 6;
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "seals-decode",
            2,
            move |builder: &mut DataflowBuilder<u64>| {
                builder.check_mock_network(true);
                build_brittle(builder, time)
            },
            SpawnOptions::default().per_stage_parallelism(false),
        )
        .unwrap();
    let inputs: Vec<_> = (0..2)
        .map(|w| multi.take_input::<u64>(w, "in").unwrap())
        .collect();
    // Worker 0's records cross the mock network to worker 1.
    inputs[0].send(1, vec![1, 2, BRITTLE, 4]).unwrap();
    let started = std::time::Instant::now();
    let result = with_timeout("decode", move || multi.join_blocking());
    drop(inputs);
    assert_decode_error("mock network", &result);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "not prompt: {:?}",
        started.elapsed()
    );
}

/// The same on a two-node cluster over TCP: node-a's records reach node-b,
/// whose decoder fails.
#[cfg(feature = "transport")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::await_holding_lock)]
async fn a_decode_failure_with_a_residual_fails_a_cluster_with_the_decode_error() {
    use instancy::{ClusterTopology, DataflowId, NodeConfig};
    use support::{Mesh, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    let _serial = serial();
    check::set_mode(Mode::Report);
    let time = UNUSED + 7;
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
        mesh.dedicated("node-a", 64),
        1,
        move |builder| build_brittle(builder, time),
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.dedicated("node-b", 64),
        1,
        move |builder| build_brittle(builder, time),
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let input_a = ca.take_input::<u64>(0, "in").unwrap();
    let input_b = cb.take_input::<u64>(0, "in").unwrap();
    input_a.send(1, vec![1, 2, BRITTLE, 4]).unwrap();
    let started = std::time::Instant::now();
    let ja = join_with_timeout(ca, Duration::from_secs(20));
    let jb = join_with_timeout(cb, Duration::from_secs(20));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    let took = started.elapsed();
    drop((input_a, input_b));
    drop_runtimes(vec![rt_a, rt_b]).await;
    match &rb {
        Some(result) => assert_decode_error("node-b", result),
        None => panic!("node-b: {}", fmt_join(&rb)),
    }
    assert!(
        matches!(&ra, Some(Err(e)) if !e.to_string().contains("progress residual")),
        "node-a must fail with node-b's failure: {}",
        fmt_join(&ra)
    );
    assert!(took < Duration::from_secs(10), "not prompt: {took:?}");
}

// ---------------------------------------------------------------------------
// A failed worker never seals (R34)
// ---------------------------------------------------------------------------

/// A record that fails the operator that receives it.
const POISON: u64 = 1_000;

/// `in -> exchange -> fail -> sink`: every record goes to worker 0, whose
/// `fail` operator returns an error on `POISON`.
fn build_poisoned(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("in")?
        .exchange_by_hash("to-worker-0", |_x: &u64| 0)
        .unary("fail", |input, output| {
            while let Some((time, data)) = input.next() {
                if data.contains(&POISON) {
                    return Err(instancy::Error::Dataflow(
                        instancy::DataflowError::InvalidConfig("poisoned record".into()),
                    ));
                }
                output.push_vec(time, data);
            }
            Ok(())
        })
        .for_each("sink", |_t, _x| {});
    Ok(())
}

/// The failed worker's run: it failed with the operator's error, promptly,
/// and never sealed (its peers' frontiers must not complete on its
/// accounting); no residual is reported instead.
fn assert_failed_without_seal(what: &str, mark: u64, result: &Result<()>, took: Duration) {
    match result {
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains("poisoned record") && !text.contains("progress residual"),
                "{what}: expected the operator's error, got: {text}"
            );
        }
        Ok(()) => panic!("{what}: succeeded despite the operator error"),
    }
    assert!(
        took < Duration::from_secs(10),
        "{what}: not prompt: {took:?}"
    );
    let oracles = check::oracles_since(mark);
    assert!(
        !oracles.is_empty(),
        "{what}: the checker did not observe the run"
    );
    for oracle in &oracles {
        assert!(
            !oracle.sealed_workers.contains(&0),
            "{what}: the failed worker sealed: {oracle:?}"
        );
    }
}

/// R34: an operator error on one of two in-process workers, mid-run (worker
/// 1's input is still open), fails the dataflow promptly with that error.
#[test]
fn an_operator_error_mid_run_fails_promptly_without_a_seal() {
    let _serial = serial();
    check::set_mode(Mode::Report);
    let mark = check::mark();
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "seals-poisoned",
            2,
            build_poisoned,
            SpawnOptions::default().per_stage_parallelism(false),
        )
        .unwrap();
    let inputs: Vec<_> = (0..2)
        .map(|w| multi.take_input::<u64>(w, "in").unwrap())
        .collect();
    inputs[1].send(0, vec![1, 2, 3]).unwrap();
    inputs[1].send(0, vec![POISON]).unwrap();
    let started = std::time::Instant::now();
    let result = with_timeout("poisoned", move || multi.join_blocking());
    let took = started.elapsed();
    drop(inputs);
    assert_failed_without_seal("in-process", mark, &result, took);
}

/// R34 on a two-node cluster: node-a's operator fails while node-b's input
/// is still open. node-a fails with the operator's error, node-b fails too
/// (its peer failed), both promptly, and node-a never seals.
#[cfg(feature = "transport")]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::await_holding_lock)]
async fn an_operator_error_mid_run_fails_a_cluster_promptly_without_a_seal() {
    use instancy::{ClusterTopology, DataflowId, NodeConfig};
    use support::{Mesh, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

    let _serial = serial();
    check::set_mode(Mode::Report);
    let mark = check::mark();
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
        mesh.dedicated("node-a", 64),
        1,
        build_poisoned,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.dedicated("node-b", 64),
        1,
        build_poisoned,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let input_a = ca.take_input::<u64>(0, "in").unwrap();
    let input_b = cb.take_input::<u64>(0, "in").unwrap();
    input_b.send(0, vec![1, 2, 3]).unwrap();
    input_b.send(0, vec![POISON]).unwrap();
    let started = std::time::Instant::now();
    let ja = join_with_timeout(ca, Duration::from_secs(20));
    let jb = join_with_timeout(cb, Duration::from_secs(20));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    let took = started.elapsed();
    drop((input_a, input_b));
    drop_runtimes(vec![rt_a, rt_b]).await;
    match &ra {
        Some(result) => assert_failed_without_seal("node-a", mark, result, took),
        None => panic!("node-a hung: {}", fmt_join(&ra)),
    }
    assert!(
        matches!(&rb, Some(Err(e)) if !e.to_string().contains("progress residual")),
        "node-b must fail with node-a's failure: {}",
        fmt_join(&rb)
    );
}
