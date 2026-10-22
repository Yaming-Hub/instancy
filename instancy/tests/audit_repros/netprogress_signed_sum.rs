//! Audit (distributed progress protocol): completion is decided by the SIGNED
//! SUM of all pointstamp counts (`Tracker::total_counts != 0`), not by
//! "every pointstamp count is zero".
//!
//! In multi-worker / multi-node mode a consumer legitimately records the `-n`
//! for a consumed exchange batch BEFORE the producer's `+n` has been broadcast
//! (the producer only broadcasts at the end of its sweep / after the network
//! hop). During that window the message pointstamp is negative and cancels
//! held capabilities in the sum, so the consumer's tracker reports
//! `is_completed()`; after `max_idle_sweeps` idle sweeps (microseconds) the
//! executor force-closes every operator WITHOUT running it again and returns
//! `Completed` — silently dropping buffered results while peers are still
//! live. Uses `map` before the exchange so the producer `+n` IS wired; this
//! is independent of the missing-`+n` wiring bug.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
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

/// input -> map(id) -+-> exchange(x % 2) -> count -> output("out")
///                   +-> map(slow: sleeps 800ms on record 7) -> for_each
///
/// The slow branch runs in the SAME sweep as the exchange push on the
/// producing worker, so the producer's `+n` is not broadcast until ~800ms
/// after the consumer could already have pulled (and `-n`-ed) the batch.
fn build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    let p = builder.input::<u64>("data")?.map("id", |_t, x| x);
    p.clone()
        .map("slow", |_t, x: u64| {
            if x == 7 {
                std::thread::sleep(Duration::from_millis(800));
            }
            x
        })
        .for_each("sink2", |_t, _x| {});
    let ex = p.exchange_by_hash("route", |x: &u64| *x);
    count_per_epoch(ex).output("out")?;
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

fn run_multi(batch: Vec<u64>) -> (Vec<(u64, Vec<u64>)>, String) {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("audit-signed-sum", 2, build, SpawnOptions::new())
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let _out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();
    drop(in0);
    std::thread::sleep(Duration::from_millis(200));
    in1.send(1000, batch).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    let late = in1.send(2000, vec![4]);
    drop(in1);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(15));
    (drain(&out0), format!("late={late:?} join={join:?}"))
}

/// Control: same dataflow and timing, but no slow record, so worker 1's `+n`
/// is broadcast promptly — worker 0 produces the correct counts.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn control_multi_worker_without_slow_producer_is_correct() {
    let (d0, info) = run_multi(vec![0, 2]);
    assert_eq!(d0, vec![(1000, vec![2]), (2000, vec![1])], "{info}");
}

/// Worker 0 closes its input. Worker 1 sends [0, 2, 7] at epoch 1000
/// ([0,2] -> worker 0, [7] -> worker 1) and keeps its input open (cap @0).
/// While worker 1 sleeps in "slow", worker 0's tracker sum is
/// w1-input-cap(+1) + w0-notify-cap@1000(+1) + inflight(-2) == 0.
#[test]
#[ignore = "expected failure (C1); see EXPECTED in tests/audit_repros.rs"]
fn multi_worker_transient_negative_inflight_completes_prematurely() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("audit-signed-sum", 2, build, SpawnOptions::new())
        .unwrap();
    let out0 = multi.take_output::<u64>(0, "out").unwrap();
    let out1 = multi.take_output::<u64>(1, "out").unwrap();
    let in0 = multi.take_input::<u64>(0, "data").unwrap();
    let in1 = multi.take_input::<u64>(1, "data").unwrap();

    drop(in0);
    std::thread::sleep(Duration::from_millis(200));
    in1.send(1000, vec![0, 2, 7]).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    // Worker 1 is still open: send one more record to worker 0 at epoch 2000.
    let late = in1.send(2000, vec![4]);
    drop(in1);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(15));
    let d0 = drain(&out0);
    let d1 = drain(&out1);
    eprintln!("late_send={late:?} join={join:?} out0={d0:?} out1={d1:?}");
    assert_eq!(
        d0,
        vec![(1000, vec![2]), (2000, vec![1])],
        "worker 0 lost results (join={join:?})"
    );
}

/// Same, two nodes over an in-memory transport: node B's progress for the
/// batch it sent to node A is delayed by a slow operator in the same sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "expected failure (C1); see EXPECTED in tests/audit_repros.rs"]
async fn cluster_transient_negative_inflight_completes_prematurely() {
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
                "audit-signed-sum",
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
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();
    let out_a = ca.take_output::<u64>(0, "out").unwrap();
    let out_b = cb.take_output::<u64>(0, "out").unwrap();
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();

    drop(sa);
    tokio::time::sleep(Duration::from_millis(300)).await;
    sb.send(1000, vec![0, 2, 7]).unwrap();

    let mut comp_a = ca.join().unwrap();
    let premature = tokio::time::timeout(Duration::from_millis(1500), &mut comp_a)
        .await
        .ok();
    let late = sb.send(2000, vec![4]);
    drop(sb);
    let a_res = tokio::time::timeout(Duration::from_secs(10), &mut comp_a).await;
    let b_res = tokio::time::timeout(Duration::from_secs(10), cb.join().unwrap()).await;
    let da = drain(&out_a);
    let db = drain(&out_b);
    eprintln!(
        "premature={premature:?} late={late:?} a_res={a_res:?} b_res={b_res:?} out_a={da:?} out_b={db:?}"
    );
    drop(comp_a);
    let _ = tokio::task::spawn_blocking(move || {
        drop(rt_a);
        drop(rt_b);
    })
    .await;
    assert!(
        premature.is_none(),
        "node A completed ({premature:?}) while node B still held an input capability"
    );
    assert_eq!(da, vec![(1000, vec![2]), (2000, vec![1])]);
}
