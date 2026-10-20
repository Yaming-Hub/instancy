//! Regression tests: a node that finishes first must not break peers that
//! still need its frames (dedicated transport).
//!
//! Before the clean shutdown handshake, the first node to finish cancelled
//! its progress bridges (dropping queued final deltas) and closed its
//! connections; a peer's progress bridges then treated the EOF as fatal and
//! stopped every bridge of that peer, including ones carrying a third node's
//! final progress. Dropping an unresolved `ClusterCompletion` cut the network
//! while local workers kept running.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::HashMap;
use std::time::Duration;

use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};

use support::{Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

fn exchange_build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i64>("data")?
        .exchange("by_val", |x: &i64| *x as u64)
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// Frontier-driven aggregation: emits per-epoch sums only once the frontier
/// passes the epoch, so it needs every node's final capability release.
fn notify_sum_build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i64>("data")?
        .exchange("by_val", |x: &i64| *x as u64)
        .unary_notify("sum", {
            let mut stash: HashMap<u64, i64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    *stash.entry(time).or_default() += data.iter().sum::<i64>();
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(s) = stash.remove(&time) {
                        output.push_vec(time, vec![s]);
                    }
                }
                Ok(())
            }
        })
        .output("out")?;
    Ok(())
}

fn three_nodes() -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
        NodeConfig::new("node-c", 1),
    ])
    .unwrap()
}

/// 3 nodes; the c→b direction has 400 ms latency and node-c releases the last
/// capability, so node-a learns about global completion first. All
/// completions are awaited concurrently, so node-a's handle is dropped as soon
/// as it resolves (normal application behaviour).
async fn run_three_nodes(
    build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
) -> ([Option<Result<()>>; 3], Vec<i64>) {
    let topo = three_nodes();
    let df = DataflowId::new();
    let mut mesh = Mesh::new(&["node-a", "node-b", "node-c"], 1 << 20, |from, to| {
        if (from, to) == ("node-c", "node-b") {
            Duration::from_millis(400)
        } else {
            Duration::ZERO
        }
    });
    let ha = spawn_node(
        topo.clone(),
        "node-a",
        df,
        mesh.dedicated("node-a", 1024),
        2,
        build,
    );
    let hb = spawn_node(
        topo.clone(),
        "node-b",
        df,
        mesh.dedicated("node-b", 1024),
        2,
        build,
    );
    let hc = spawn_node(topo, "node-c", df, mesh.dedicated("node-c", 1024), 2, build);
    let ((rt_a, ra), (rt_b, rb), (rt_c, rc)) =
        (ha.await.unwrap(), hb.await.unwrap(), hc.await.unwrap());
    let (mut ca, mut cb, mut cc) = (ra.unwrap(), rb.unwrap(), rc.unwrap());
    let outs = [
        collect(ca.take_output::<i64>(0, "out").unwrap()),
        collect(cb.take_output::<i64>(0, "out").unwrap()),
        collect(cc.take_output::<i64>(0, "out").unwrap()),
    ];
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let sb = cb.take_input::<i64>(0, "data").unwrap();
    let sc = cc.take_input::<i64>(0, "data").unwrap();
    sa.send(0, (0..30).collect()).unwrap();
    drop(sa);
    drop(sb);

    let timeout = Duration::from_secs(15);
    let ja = join_with_timeout(ca, timeout);
    let jb = join_with_timeout(cb, timeout);
    let jc = join_with_timeout(cc, timeout);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(sc); // node-c releases the last capability
    let (ra, rb, rc) = (ja.await.unwrap(), jb.await.unwrap(), jc.await.unwrap());
    let mut got: Vec<i64> = outs
        .into_iter()
        .flat_map(|o| o.join().unwrap().into_iter().map(|(_, v)| v))
        .collect();
    got.sort_unstable();
    drop_runtimes(vec![rt_a, rt_b, rt_c]).await;
    ([ra, rb, rc], got)
}

fn assert_all_ok(joins: &[Option<Result<()>>; 3]) {
    assert!(
        joins.iter().all(|j| matches!(j, Some(Ok(())))),
        "every node must complete: a={} b={} c={}",
        fmt_join(&joins[0]),
        fmt_join(&joins[1]),
        fmt_join(&joins[2])
    );
}

/// The early finisher's teardown must not stop a slower peer's bridges before
/// a third node's final progress arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_nodes_early_finisher_keeps_slow_peers_frontier() {
    let (joins, got) = run_three_nodes(notify_sum_build).await;
    assert_all_ok(&joins);
    assert_eq!(got.iter().sum::<i64>(), 435, "per-epoch sums lost: {got:?}");
}

/// Same, with a plain exchange (completion does not wait for notifications).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_nodes_early_finisher_plain_exchange() {
    let (joins, got) = run_three_nodes(exchange_build).await;
    assert_all_ok(&joins);
    assert_eq!(got, (0..30).collect::<Vec<_>>());
}

/// Control: node-a is joined only after b and c have finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_nodes_late_join_control() {
    let topo = three_nodes();
    let df = DataflowId::new();
    let mut mesh = Mesh::new(&["node-a", "node-b", "node-c"], 1 << 20, |from, to| {
        if (from, to) == ("node-c", "node-b") {
            Duration::from_millis(400)
        } else {
            Duration::ZERO
        }
    });
    let build = notify_sum_build;
    let ha = spawn_node(
        topo.clone(),
        "node-a",
        df,
        mesh.dedicated("node-a", 1024),
        2,
        build,
    );
    let hb = spawn_node(
        topo.clone(),
        "node-b",
        df,
        mesh.dedicated("node-b", 1024),
        2,
        build,
    );
    let hc = spawn_node(topo, "node-c", df, mesh.dedicated("node-c", 1024), 2, build);
    let ((rt_a, ra), (rt_b, rb), (rt_c, rc)) =
        (ha.await.unwrap(), hb.await.unwrap(), hc.await.unwrap());
    let (mut ca, mut cb, mut cc) = (ra.unwrap(), rb.unwrap(), rc.unwrap());
    let outs = [
        collect(ca.take_output::<i64>(0, "out").unwrap()),
        collect(cb.take_output::<i64>(0, "out").unwrap()),
        collect(cc.take_output::<i64>(0, "out").unwrap()),
    ];
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    drop(cb.take_input::<i64>(0, "data").unwrap());
    let sc = cc.take_input::<i64>(0, "data").unwrap();
    sa.send(0, (0..30).collect()).unwrap();
    drop(sa);
    let timeout = Duration::from_secs(15);
    let jb = join_with_timeout(cb, timeout);
    let jc = join_with_timeout(cc, timeout);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(sc);
    let (rb, rc) = (jb.await.unwrap(), jc.await.unwrap());
    let ra = join_with_timeout(ca, timeout).await.unwrap();
    let total: i64 = outs
        .into_iter()
        .flat_map(|o| o.join().unwrap().into_iter().map(|(_, v)| v))
        .sum();
    drop_runtimes(vec![rt_a, rt_b, rt_c]).await;
    assert_all_ok(&[ra, rb, rc]);
    assert_eq!(total, 435);
}

/// 2 nodes × 4 workers, no latency: node-a releases the last capabilities, so
/// its final progress is immediately followed by its teardown. Repeated to
/// catch the race between the final deltas and the connection close.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_nodes_multiworker_early_finisher_race() {
    let iterations = 15;
    let mut failures = Vec::new();
    for it in 0..iterations {
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 4),
            NodeConfig::new("node-b", 4),
        ])
        .unwrap();
        let df = DataflowId::new();
        let mut mesh = Mesh::new(&["node-a", "node-b"], 1 << 20, |_, _| Duration::ZERO);
        let ha = spawn_node(
            topo.clone(),
            "node-a",
            df,
            mesh.dedicated("node-a", 1024),
            2,
            notify_sum_build,
        );
        let hb = spawn_node(
            topo,
            "node-b",
            df,
            mesh.dedicated("node-b", 1024),
            2,
            notify_sum_build,
        );
        let ((rt_a, ra), (rt_b, rb)) = (ha.await.unwrap(), hb.await.unwrap());
        let (mut ca, mut cb) = (ra.unwrap(), rb.unwrap());
        let mut outs = Vec::new();
        for i in 0..4 {
            outs.push(collect(ca.take_output::<i64>(i, "out").unwrap()));
            outs.push(collect(cb.take_output::<i64>(i, "out").unwrap()));
        }
        for i in 0..4 {
            drop(cb.take_input::<i64>(i, "data").unwrap());
        }
        let inputs: Vec<_> = (0..4)
            .map(|i| ca.take_input::<i64>(i, "data").unwrap())
            .collect();
        let ja = join_with_timeout(ca, Duration::from_secs(10));
        let jb = join_with_timeout(cb, Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(50)).await;
        for (i, s) in inputs.iter().enumerate() {
            s.send(0, (i as i64 * 100..(i as i64 + 1) * 100).collect())
                .unwrap();
        }
        drop(inputs);
        let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
        let total: i64 = outs
            .into_iter()
            .flat_map(|o| o.join().unwrap().into_iter().map(|(_, v)| v))
            .sum();
        if !(matches!(ra, Some(Ok(())))
            && matches!(rb, Some(Ok(())))
            && total == (0..400).sum::<i64>())
        {
            failures.push(format!(
                "iter {it}: a={} b={} total={total}",
                fmt_join(&ra),
                fmt_join(&rb)
            ));
        }
        drop_runtimes(vec![rt_a, rt_b]).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Dropping an unresolved `ClusterCompletion` (here: a timed-out await)
/// detaches it. The dataflow keeps running with its network intact, every node
/// completes, and the dropped node's workers do not linger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_unresolved_completion_keeps_dataflow_running() {
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let df = DataflowId::new();
    let mut mesh = Mesh::new(&["node-a", "node-b"], 1 << 20, |_, _| Duration::ZERO);
    let ha = spawn_node(
        topo.clone(),
        "node-a",
        df,
        mesh.dedicated("node-a", 1024),
        2,
        notify_sum_build,
    );
    let hb = spawn_node(
        topo,
        "node-b",
        df,
        mesh.dedicated("node-b", 1024),
        2,
        notify_sum_build,
    );
    let ((rt_a, ra), (rt_b, rb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ra.unwrap(), rb.unwrap());
    let oa = collect(ca.take_output::<i64>(0, "out").unwrap());
    let ob = collect(cb.take_output::<i64>(0, "out").unwrap());
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    let sb = cb.take_input::<i64>(0, "data").unwrap();

    let completion_a = ca.join().unwrap();
    let early = tokio::time::timeout(Duration::from_millis(100), completion_a).await;
    assert!(early.is_err(), "precondition: completion not ready yet");

    sa.send(0, (0..30).collect()).unwrap();
    sb.send(0, (30..60).collect()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(sa);
    drop(sb);
    let jb = join_with_timeout(cb, Duration::from_secs(10))
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while rt_a.active_dataflows() > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let active_a = rt_a.active_dataflows();
    let total: i64 = [oa, ob]
        .into_iter()
        .flat_map(|o| o.join().unwrap().into_iter().map(|(_, v)| v))
        .sum();
    drop_runtimes(vec![rt_a, rt_b]).await;
    assert!(matches!(jb, Some(Ok(()))), "node-b: {}", fmt_join(&jb));
    assert_eq!(active_a, 0, "node-a's workers must finish");
    assert_eq!(total, (0..60).sum::<i64>());
}

// ---------------------------------------------------------------------------
// Sender finishes first: input -> exchange(all to node-b) -> map -> output (TCP)
// ---------------------------------------------------------------------------

const PER_EPOCH: usize = 40;
const EPOCHS: u64 = 300;

/// One round on its own tokio runtime; returns the records seen by node-b, or
/// `None` if a node did not complete. With `keep_a_unjoined`, node-a's handle
/// is joined only after node-b finished (control).
fn sender_finishes_first_round(map_before_exchange: bool, keep_a_unjoined: bool) -> Option<usize> {
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let _guard = tokio_rt.enter();
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let df = DataflowId::new();
    let build = move |b: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = b.input::<u64>("data")?;
        let pre = if map_before_exchange {
            input.map("id", |_t, x| x)
        } else {
            input
        };
        pre.exchange_by_hash("to-w1", |_x: &u64| 1)
            .map("one", |_t, _x| 1usize)
            .output("counts")?;
        Ok(())
    };
    let (ca, cb, rt_a, rt_b) = tokio_rt.block_on(async {
        let mut conns = support::tcp_mesh(&["node-a", "node-b"]).await;
        let transport = |n: &str, conns: &mut HashMap<String, Vec<support::TcpConn>>| {
            instancy::communication::ClusterSpawnTransport::dedicated(
                conns.remove(n).unwrap(),
                1024,
            )
        };
        let ha = spawn_node(
            topo.clone(),
            "node-a",
            df,
            transport("node-a", &mut conns),
            2,
            build,
        );
        let hb = spawn_node(
            topo,
            "node-b",
            df,
            transport("node-b", &mut conns),
            2,
            build,
        );
        let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
        (ca.unwrap(), cb.unwrap(), rt_a, rt_b)
    });
    let (mut ca, mut cb) = (ca, cb);
    let out_a = collect(ca.take_output::<usize>(0, "counts").unwrap());
    let out_b = collect(cb.take_output::<usize>(0, "counts").unwrap());
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let feeder = std::thread::spawn(move || {
        for e in 0..EPOCHS {
            for chunk in 0..4 {
                sa.send(e, vec![e * 1000 + chunk; PER_EPOCH / 4]).unwrap();
            }
            sa.advance_to(e + 1).unwrap();
        }
    });
    let (txa, rxa) = std::sync::mpsc::channel();
    let (txb, rxb) = std::sync::mpsc::channel();
    let (keep_tx, keep_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if keep_a_unjoined {
            let _ = keep_rx.recv_timeout(Duration::from_secs(30));
        }
        let _ = txa.send(ca.join_blocking());
    });
    std::thread::spawn(move || {
        let _ = txb.send(cb.join_blocking());
    });
    let b = rxb.recv_timeout(Duration::from_secs(30));
    let _ = keep_tx.send(());
    let a = rxa.recv_timeout(Duration::from_secs(30));
    feeder.join().unwrap();
    let completed = matches!(a, Ok(Ok(()))) && matches!(b, Ok(Ok(())));
    let received = completed.then(|| out_a.join().unwrap().len() + out_b.join().unwrap().len());
    if completed {
        drop((rt_a, rt_b));
    } else {
        // Never block on the teardown of a hung cluster.
        std::mem::forget((rt_a, rt_b));
    }
    drop(_guard);
    tokio_rt.shutdown_background();
    received
}

fn assert_sender_finishes_first_is_lossless(map_before_exchange: bool, keep_a_unjoined: bool) {
    let rounds: Vec<Option<usize>> = (0..15)
        .map(|_| sender_finishes_first_round(map_before_exchange, keep_a_unjoined))
        .collect();
    let expected = EPOCHS as usize * PER_EPOCH;
    assert!(
        rounds.iter().all(|r| *r == Some(expected)),
        "records received per round (expected {expected}): {rounds:?}"
    );
}

/// node-a's local completion must not cut off the records it still has in
/// flight to node-b (the input operator feeds the exchange directly).
#[test]
fn sender_finishing_first_delivers_every_record_input_exchange() {
    assert_sender_finishes_first_is_lossless(false, false);
}

/// Same with a `map` producing into the exchange.
#[test]
fn sender_finishing_first_delivers_every_record_map_exchange() {
    assert_sender_finishes_first_is_lossless(true, false);
}

/// Control: node-a is joined only after node-b finished.
#[test]
fn sender_joined_after_receiver_delivers_every_record_control() {
    assert_sender_finishes_first_is_lossless(false, true);
}

// ---------------------------------------------------------------------------
// Three nodes, iterate with an exchange inside the loop (TCP)
// ---------------------------------------------------------------------------

fn iterate_build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    use instancy::{IterateResult, Product};
    let output = builder
        .input::<i64>("data")?
        .iterate::<u32>("loop", 1u32, |v| {
            let doubled = v
                .map("double", |_t: &Product<u64, u32>, x| x * 2)
                .exchange_by_hash("redistribute", |x: &i64| *x as u64);
            let done = doubled.clone().filter("done", |_t, x| *x >= 1000);
            let again = doubled.filter("again", |_t, x| *x < 1000);
            IterateResult {
                feedback: again,
                output: done,
            }
        });
    output.output("results")?;
    Ok(())
}

fn expected_iterate() -> Vec<i64> {
    let mut v: Vec<i64> = (1..=50i64)
        .map(|mut x| {
            while x < 1000 {
                x *= 2;
            }
            x
        })
        .collect();
    v.sort_unstable();
    v
}

/// Whichever node finishes the loop first must not cut the others off.
///
/// Ignored because of a separate, pre-existing executor bug: this iterate
/// dataflow occasionally hangs with one worker still active even in a single
/// process with no network (`spawn_multi`, 3 workers: about 1 in 300 runs).
/// In the cluster version the stuck node has already received every frame
/// (its peers retired their links only after it acknowledged their
/// end-of-stream), so the hang is not a teardown loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "pre-existing iterate executor hang (reproducible in-process ~1/300); not a teardown loss"]
async fn three_nodes_iterate_completes_whichever_node_holds_the_data() {
    let nodes = ["node-a", "node-b", "node-c"];
    let mut failures = Vec::new();
    for run in 0..8 {
        let data_node = nodes[run % 3];
        let topo = three_nodes();
        let df = DataflowId::new();
        let mut conns = support::tcp_mesh(&nodes).await;
        let mut handles = Vec::new();
        for n in nodes {
            let transport = instancy::communication::ClusterSpawnTransport::dedicated(
                conns.remove(n).unwrap(),
                1024,
            );
            handles.push(spawn_node(topo.clone(), n, df, transport, 2, iterate_build));
        }
        let mut rts = Vec::new();
        let mut outs = Vec::new();
        let mut joins = Vec::new();
        for (h, n) in handles.into_iter().zip(nodes) {
            let (rt, c) = h.await.unwrap();
            let mut c = c.unwrap();
            outs.push(collect(c.take_output::<i64>(0, "results").unwrap()));
            let s = c.take_input::<i64>(0, "data").unwrap();
            if n == data_node {
                s.send(0, (1..=50).collect()).unwrap();
            }
            drop(s);
            joins.push(join_with_timeout(c, Duration::from_secs(20)));
            rts.push(rt);
        }
        let mut results = Vec::new();
        for j in joins {
            results.push(j.await.unwrap());
        }
        if results.iter().any(|r| !matches!(r, Some(Ok(())))) {
            failures.push(format!(
                "run {run} (data on {data_node}): joins={:?} active_workers={:?}",
                results.iter().map(fmt_join).collect::<Vec<_>>(),
                rts.iter()
                    .map(|rt| rt.active_dataflows())
                    .collect::<Vec<_>>()
            ));
            std::mem::forget(rts);
            continue;
        }
        let mut got: Vec<i64> = outs
            .into_iter()
            .flat_map(|o| o.join().unwrap().into_iter().map(|(_, v)| v))
            .collect();
        got.sort_unstable();
        if got != expected_iterate() {
            failures.push(format!(
                "run {run} (data on {data_node}): outputs={} expected {}",
                got.len(),
                expected_iterate().len()
            ));
        }
        drop_runtimes(rts).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
