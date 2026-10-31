//! Audit (distributed progress protocol): in-flight message accounting.
//!
//! `input().exchange(..)` (and every other non-`add_unary_internal` producer:
//! source, binary, concat, unary_notify, unary_async, branch, ...) never wires
//! the producer-side `+n` in-flight reporter, while the exchange pull side
//! always records `-n` on consume. In multi-worker / multi-node mode this
//! drives the exchange-input message pointstamp count NEGATIVE. The tracker's
//! completion test is `total_counts != 0` (a plain SUM over all pointstamps),
//! so the negative in-flight counts cancel out held capabilities and the
//! tracker reports "completed" while capabilities are still outstanding.
//! The executor then force-closes all operators (without running them),
//! silently dropping buffered state, and the worker reports success while
//! peers are still running.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Pipe, Result, RuntimeConfig,
    RuntimeHandle, SpawnOptions,
};

fn count_per_epoch(p: Pipe<u64, u64>) -> Pipe<u64, u64> {
    p.unary_notify("count", {
        let mut stash: HashMap<u64, u64> = HashMap::new();
        move |input, output, ctx| {
            while let Some((t, data)) = input.next() {
                *stash.entry(t).or_default() += data.len() as u64;
                ctx.notify_at(t);
            }
            while let Some(t) = ctx.next_notification() {
                if let Some(c) = stash.remove(&t) {
                    output.push_vec(t, vec![c]);
                }
            }
            Ok(())
        }
    })
}

/// `input -> exchange(x % workers) -> count per epoch -> output` (NO map before exchange).
fn build_direct(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    let p = builder
        .input::<u64>("data")?
        .exchange_by_hash("route", |x: &u64| *x);
    count_per_epoch(p).output("out")?;
    Ok(())
}

/// Same, but with a `map` before the exchange (producer-side +n is wired).
fn build_with_map(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    let p = builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .exchange_by_hash("route", |x: &u64| *x);
    count_per_epoch(p).output("out")?;
    Ok(())
}

fn drain(rx: &instancy::OutputReceiver<u64, u64>) -> Vec<(u64, Vec<u64>)> {
    let mut v = Vec::new();
    while let Some(ev) = rx.recv_timeout(Duration::from_millis(20)) {
        if let OutputEvent::Data { time, data } = ev {
            v.push((time, data));
        }
    }
    v.sort();
    v
}

async fn spawn_two(
    build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
) -> (
    RuntimeHandle,
    RuntimeHandle,
    ClusterSpawnedDataflow<u64>,
    ClusterSpawnedDataflow<u64>,
) {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
    let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);
    let conn_a = PeerConnection {
        node_id: "node-b".to_string(),
        reader: a_from_b,
        writer: a_to_b,
    };
    let conn_b = PeerConnection {
        node_id: "node-a".to_string(),
        reader: b_from_a,
        writer: b_to_a,
    };
    let th = tokio::runtime::Handle::current();
    let spawn = |node: &'static str, conn: PeerConnection<_, _>| {
        let topology = topology.clone();
        let th = th.clone();
        tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 1,
                ..RuntimeConfig::default()
            })?;
            let c = rt.spawn_cluster(
                "audit-inflight",
                topology,
                node,
                id,
                ClusterSpawnTransport::dedicated(vec![conn], 1024),
                Duration::from_secs(10),
                build,
                &th,
                SpawnOptions::new(),
            )?;
            Ok::<_, instancy::Error>((rt, c))
        })
    };
    let (ra, rb) = tokio::join!(spawn("node-a", conn_a), spawn("node-b", conn_b));
    let (rt_a, ca) = ra.unwrap().unwrap();
    let (rt_b, cb) = rb.unwrap().unwrap();
    (rt_a, rt_b, ca, cb)
}

/// Scenario (2 nodes x 1 worker):
/// - A sends one record at epoch 1000 routed to A, then closes its input.
/// - B sends one record at epoch 1 routed to B, advances to 2, and KEEPS its
///   input open (B may still send data to A at any epoch >= 2).
///
/// At A: counts = B input cap(+1) + A's notify cap@1000(+1) + exchange-input
/// in-flight (-1 for A's consume, -1 for B's consume, never matched by +1)
/// = 0  ==> tracker "completed" ==> A force-closes and finishes while B is live.
async fn run_cluster(
    build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
) -> (bool, Vec<(u64, Vec<u64>)>) {
    let (rt_a, rt_b, mut ca, mut cb) = spawn_two(build).await;
    let out_a = ca.take_output::<u64>(0, "out").unwrap();
    let out_b = cb.take_output::<u64>(0, "out").unwrap();
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();

    sa.send(1000, vec![0]).unwrap();
    drop(sa);
    sb.send(1, vec![1]).unwrap();
    sb.advance_to(2).unwrap();

    // A must NOT complete while B still holds an input capability at epoch 2.
    let mut comp_a = ca.join().unwrap();
    let premature = tokio::time::timeout(Duration::from_secs(2), &mut comp_a)
        .await
        .is_ok();

    // B now sends a record routed to A (2 % 2 == 0) and closes.
    sb.send(5, vec![2]).unwrap();
    drop(sb);
    let a_res = tokio::time::timeout(Duration::from_secs(10), &mut comp_a).await;
    let b_res = tokio::time::timeout(Duration::from_secs(10), cb.join().unwrap()).await;
    let data_a = drain(&out_a);
    let data_b = drain(&out_b);
    eprintln!(
        "premature={premature} a_res={a_res:?} b_res={b_res:?} out_a={data_a:?} out_b={data_b:?}"
    );
    drop(comp_a);
    let _ = tokio::task::spawn_blocking(move || {
        drop(rt_a);
        drop(rt_b);
    })
    .await;
    (premature, data_a)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_map_before_exchange_is_correct() {
    let (premature, data_a) = run_cluster(build_with_map).await;
    assert!(
        !premature,
        "control: node A completed while node B's input was open"
    );
    assert_eq!(data_a, vec![(5, vec![1]), (1000, vec![1])]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_input_exchange_completes_prematurely_and_loses_data() {
    let (premature, data_a) = run_cluster(build_direct).await;
    assert!(
        !premature,
        "node A reported completion while node B's input (cap@2) was still open; out_a={data_a:?}"
    );
    assert_eq!(data_a, vec![(5, vec![1]), (1000, vec![1])]);
}

/// Same root cause, single process (`spawn_multi`, 2 workers).
#[test]
fn multi_worker_input_exchange_completes_prematurely_and_loses_data() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("audit-inflight-multi", 2, build_direct, SpawnOptions::new())
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();

    in0.send(1000, vec![0]).unwrap();
    drop(in0);
    in1.send(1, vec![1]).unwrap();
    in1.advance_to(2).unwrap();

    // Worker 0 should still be waiting: worker 1 holds an input capability.
    std::thread::sleep(Duration::from_secs(2));
    let early0 = drain(&out0);
    in1.send(5, vec![2]).unwrap(); // routed to worker 0
    drop(in1);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(10));
    let mut all0 = early0.clone();
    all0.extend(drain(&out0));
    all0.sort();
    let all1 = drain(&out1);
    eprintln!("join={join:?} early0={early0:?} out0={all0:?} out1={all1:?}");
    assert_eq!(
        all0,
        vec![(5, vec![1]), (1000, vec![1])],
        "worker 0 lost/duplicated per-epoch counts (join={join:?})"
    );
}

/// Permanent negative in-flight counts also make `total_counts` non-zero
/// forever once all capabilities are released, so `is_completed()` never
/// becomes true: dataflows whose termination relies on the quiescence
/// force-close (feedback loops) never finish.
/// `input -> exchange (no map) -> iterate(double until >= 100) -> output`.
fn run_iterate(
    with_map: bool,
) -> (
    std::result::Result<instancy::Result<()>, std::sync::mpsc::RecvTimeoutError>,
    Vec<u64>,
) {
    use instancy::IterateResult;
    use instancy::Product;
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-inflight-iterate",
            2,
            move |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<u64>("data")?;
                let pre = if with_map {
                    input.map("id", |_t, x| x)
                } else {
                    input
                };
                let exchanged = pre.exchange_by_hash("route", |x: &u64| *x);
                let out = exchanged.iterate::<u32>("loop", 1u32, |iter_var| {
                    let doubled = iter_var.map("double", |_t: &Product<u64, u32>, x| x * 2);
                    let done = doubled.clone().filter("done", |_t, x| *x >= 100);
                    let again = doubled.filter("again", |_t, x| *x < 100);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                });
                out.output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();
    in0.send(0, vec![1, 2, 3]).unwrap();
    in1.send(0, vec![7, 50]).unwrap();
    drop(in0);
    drop(in1);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(10));
    let mut all: Vec<u64> = drain(&out0)
        .into_iter()
        .chain(drain(&out1))
        .flat_map(|(_, d)| d)
        .collect();
    all.sort();
    eprintln!("with_map={with_map} join={join:?} results={all:?}");
    (join, all)
}

#[test]
fn control_map_exchange_then_iterate_completes() {
    let (join, all) = run_iterate(true);
    assert!(matches!(join, Ok(Ok(()))), "control: join={join:?}");
    assert_eq!(all, vec![100, 112, 128, 128, 192]);
}

#[test]
fn multi_worker_input_exchange_then_iterate_never_completes() {
    let (join, all) = run_iterate(false);
    assert!(
        matches!(join, Ok(Ok(()))),
        "dataflow did not complete within 10s (join={join:?}, results={all:?})"
    );
    assert_eq!(all, vec![100, 112, 128, 128, 192]);
}
