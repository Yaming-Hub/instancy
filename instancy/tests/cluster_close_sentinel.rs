//! Regression tests: end-of-stream (close sentinel) delivery on data channels.
//!
//! `NetworkPush::close` used a best-effort `try_send`; when the per-peer queue
//! was full the sentinel was lost. `broadcast` only releases its capability
//! once its input is exhausted, so a lost sentinel wedged the cluster (shared
//! mode has no EOF fallback at all). Sentinels now travel on the ordered,
//! unbounded per-peer lane.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};

use support::{Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node, tcp_mesh};

fn broadcast_build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .with_capacity(1 << 20)?
        .broadcast("bcast")
        .map("pass", |_t, x| x)
        .output("results")?;
    Ok(())
}

/// node-a (2 workers) + node-b (1 worker); node-a worker 0 broadcasts
/// `batches × per_batch` distinct values. Returns the per-worker outputs, or
/// an error string if a node failed or hung.
async fn run_broadcast(
    capacity: usize,
    batches: u64,
    per_batch: u64,
) -> std::result::Result<Vec<Vec<u64>>, String> {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 2),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let mut conns = tcp_mesh(&["node-a", "node-b"]).await;
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        id,
        ClusterSpawnTransport::dedicated(conns.remove("node-a").unwrap(), capacity),
        2,
        broadcast_build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        ClusterSpawnTransport::dedicated(conns.remove("node-b").unwrap(), capacity),
        2,
        broadcast_build,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let outs = vec![
        collect(ca.take_output::<u64>(0, "results").unwrap()),
        collect(ca.take_output::<u64>(1, "results").unwrap()),
        collect(cb.take_output::<u64>(0, "results").unwrap()),
    ];
    let s0 = ca.take_input::<u64>(0, "data").unwrap();
    std::thread::spawn(move || {
        for b in 0..batches {
            let base = b * per_batch;
            if s0.send(0, (base..base + per_batch).collect()).is_err() {
                return;
            }
        }
    });
    drop(ca.take_input::<u64>(1, "data").unwrap());
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let ja = join_with_timeout(ca, Duration::from_secs(15));
    let jb = join_with_timeout(cb, Duration::from_secs(15));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    if !matches!(ra, Some(Ok(()))) || !matches!(rb, Some(Ok(()))) {
        std::mem::forget((rt_a, rt_b));
        return Err(format!("a={} b={}", fmt_join(&ra), fmt_join(&rb)));
    }
    let per_worker = outs
        .into_iter()
        .map(|o| {
            let mut v: Vec<u64> = o.join().unwrap().into_iter().map(|(_, x)| x).collect();
            v.sort_unstable();
            v
        })
        .collect();
    drop_runtimes(vec![rt_a, rt_b]).await;
    Ok(per_worker)
}

/// Payload capacity 1: the per-peer queue is usually full when the broadcast
/// closes its outputs. Only completion and the delivered value *set* are
/// checked: with capacity 1, broadcast's partial-push retry can duplicate
/// values to local targets (a separate exchange/broadcast bug, audit PR2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_close_sentinel_survives_full_queue() {
    let expected: Vec<u64> = (0..4).collect();
    for run in 0..5 {
        let per_worker = run_broadcast(1, 1, 4)
            .await
            .unwrap_or_else(|e| panic!("run {run}: {e}"));
        for mut values in per_worker {
            values.dedup();
            assert_eq!(values, expected, "run {run}");
        }
    }
}

/// Control: generous capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_close_sentinel_control_capacity_1024() {
    let per_worker = run_broadcast(1024, 1, 4).await.unwrap();
    assert!(per_worker.iter().all(|v| *v == (0..4).collect::<Vec<_>>()));
}

/// Shared mode: another dataflow (X) fills the shared per-peer queue while
/// the link is congested, then dataflow Y's broadcast input closes. Y's close
/// sentinel must still reach node-b once the link recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn broadcast_close_sentinel_survives_full_shared_queue() {
    let mut mesh = Mesh::new(&["node-a", "node-b"], 256, |_, _| Duration::ZERO);
    let a_to_b = mesh.valve("node-a", "node-b");
    let ma = mesh.managers("node-a");
    let mb = mesh.managers("node-b");
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let shared =
        |m: &support::Managers| ClusterSpawnTransport::shared(std::sync::Arc::clone(m), 1024);

    let y = DataflowId::new();
    let hya = spawn_node(
        topology.clone(),
        "node-a",
        y,
        shared(&ma),
        1,
        broadcast_build,
    );
    let hyb = spawn_node(
        topology.clone(),
        "node-b",
        y,
        shared(&mb),
        1,
        broadcast_build,
    );
    let ((rt_ya, ya), (rt_yb, yb)) = (hya.await.unwrap(), hyb.await.unwrap());
    let (mut ya, mut yb) = (ya.unwrap(), yb.unwrap());
    let y_outs = [
        collect(ya.take_output::<u64>(0, "results").unwrap()),
        collect(yb.take_output::<u64>(0, "results").unwrap()),
    ];
    drop(yb.take_input::<u64>(0, "data").unwrap());
    let y_in = ya.take_input::<u64>(0, "data").unwrap();
    y_in.send(0, vec![1, 2, 3]).unwrap();

    let flood_build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("data")?
            .exchange_by_hash("to-b", |_x: &u64| 1)
            .map("id", |_t, x| x)
            .output("out")?;
        Ok(())
    };
    let x = DataflowId::new();
    let hxa = spawn_node(topology.clone(), "node-a", x, shared(&ma), 1, flood_build);
    let hxb = spawn_node(topology, "node-b", x, shared(&mb), 1, flood_build);
    let ((rt_xa, xa), (rt_xb, xb)) = (hxa.await.unwrap(), hxb.await.unwrap());
    let (mut xa, mut xb) = (xa.unwrap(), xb.unwrap());
    let x_outs = [
        collect(xa.take_output::<u64>(0, "out").unwrap()),
        collect(xb.take_output::<u64>(0, "out").unwrap()),
    ];
    drop(xb.take_input::<u64>(0, "data").unwrap());
    let x_in = xa.take_input::<u64>(0, "data").unwrap();

    // Let Y's data reach node-b, then congest the link and flood it with X.
    tokio::time::sleep(Duration::from_millis(300)).await;
    a_to_b.pause();
    let flood = std::thread::spawn(move || {
        for t in 0..3000u64 {
            if x_in.send(t, vec![t]).is_err() || x_in.advance_to(t + 1).is_err() {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drop(y_in); // Y's broadcast closes its outputs while the queue is full.
    let jya = join_with_timeout(ya, Duration::from_secs(30));
    let jyb = join_with_timeout(yb, Duration::from_secs(30));
    tokio::time::sleep(Duration::from_millis(500)).await;
    a_to_b.resume();

    let (rya, ryb) = (jya.await.unwrap(), jyb.await.unwrap());
    assert!(
        matches!(rya, Some(Ok(()))) && matches!(ryb, Some(Ok(()))),
        "dataflow Y: a={} b={}",
        fmt_join(&rya),
        fmt_join(&ryb)
    );
    for out in y_outs {
        let mut got: Vec<u64> = out.join().unwrap().into_iter().map(|(_, v)| v).collect();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3]);
    }
    flood.join().unwrap();
    let (rxa, rxb) = (
        join_with_timeout(xa, Duration::from_secs(30)),
        join_with_timeout(xb, Duration::from_secs(30)),
    );
    let (rxa, rxb) = (rxa.await.unwrap(), rxb.await.unwrap());
    assert!(
        matches!(rxa, Some(Ok(()))) && matches!(rxb, Some(Ok(()))),
        "dataflow X: a={} b={}",
        fmt_join(&rxa),
        fmt_join(&rxb)
    );
    let x_records: usize = x_outs.into_iter().map(|o| o.join().unwrap().len()).sum();
    assert_eq!(x_records, 3000);
    drop_runtimes(vec![rt_ya, rt_yb, rt_xa, rt_xb]).await;
}
