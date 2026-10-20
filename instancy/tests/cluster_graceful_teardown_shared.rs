//! Regression tests: graceful teardown with shared (pooled) transport.
//!
//! Before the clean shutdown handshake, a node that finished first
//! unregistered the dataflow from its `SharedPeerManager`s while its outbound
//! frames were still queued (the shared bridge then discarded them), and a
//! completion holding the last manager reference aborted the writer tasks.
//! Peers hung, or failed with a spurious "shared transport failure".

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use instancy::communication::ClusterSpawnTransport;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, SpawnOptions};

use support::{
    Managers, Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node,
    spawn_node_with_options, tcp_shared_mesh,
};

fn two_nodes(a: &str, b: &str) -> ClusterTopology {
    ClusterTopology::multi_node(vec![NodeConfig::new(a, 1), NodeConfig::new(b, 1)]).unwrap()
}

fn shared(managers: &Managers) -> ClusterSpawnTransport {
    ClusterSpawnTransport::shared(Arc::clone(managers), 1024)
}

/// node-a finishes dataflow Y while its frames for node-b are queued behind
/// another dataflow's (X) traffic on a congested shared connection. node-a's
/// completion must wait until node-b has them; node-b must then complete
/// with every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn early_finisher_keeps_frames_queued_behind_another_dataflow() {
    let mut mesh = Mesh::new(&["node-a", "node-b"], 256, |_, _| Duration::ZERO);
    let a_to_b = mesh.valve("node-a", "node-b");
    let ma = mesh.managers("node-a");
    let mb = mesh.managers("node-b");
    let topo = two_nodes("node-a", "node-b");
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("data")?
            .exchange("x", |x: &u64| *x)
            .map("id", |_t, x| x)
            .output("out")?;
        Ok(())
    };
    let spawn = |df: DataflowId| {
        (
            spawn_node(topo.clone(), "node-a", df, shared(&ma), 1, build),
            spawn_node(topo.clone(), "node-b", df, shared(&mb), 1, build),
        )
    };

    let (ha, hb) = spawn(DataflowId::new());
    let ((rt_ya, ya), (rt_yb, yb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ya, mut yb) = (ya.unwrap(), yb.unwrap());
    let y_out = [
        collect(ya.take_output::<u64>(0, "out").unwrap()),
        collect(yb.take_output::<u64>(0, "out").unwrap()),
    ];

    // Background dataflow X shares the same connection.
    let (hx_a, hx_b) = spawn(DataflowId::new());
    let ((rt_xa, xa), (rt_xb, xb)) = (hx_a.await.unwrap(), hx_b.await.unwrap());
    let (mut xa, mut xb) = (xa.unwrap(), xb.unwrap());
    let x_out = [
        collect(xa.take_output::<u64>(0, "out").unwrap()),
        collect(xb.take_output::<u64>(0, "out").unwrap()),
    ];
    // X's node-b keeps its (unused) input open until X's burst has arrived:
    // a receiver with no capability of its own can still complete after
    // consuming the first record, before the producer's matching progress
    // arrives (signed-sum completion predicate, audit C1/C2; see
    // `many_small_frames_then_complete_*`). X only generates congestion here.
    let xb_in = xb.take_input::<u64>(0, "data").unwrap();
    let x_in = xa.take_input::<u64>(0, "data").unwrap();

    // node-b has no Y input; let its "input closed" progress reach node-a.
    drop(yb.take_input::<u64>(0, "data").unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Congest a -> b; X's traffic queues ahead of Y's.
    a_to_b.pause();
    let x_feeder = std::thread::spawn(move || {
        for t in 0..400u64 {
            if x_in.send(t, vec![2 * t + 1, 2 * t + 3]).is_err() || x_in.advance_to(t + 1).is_err()
            {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let y_in = ya.take_input::<u64>(0, "data").unwrap();
    y_in.send(0, (0..20).collect()).unwrap();
    drop(y_in);

    let ja = join_with_timeout(ya, Duration::from_secs(20));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !ja.is_finished(),
        "node-a must not complete while node-b cannot have received its frames"
    );

    a_to_b.resume();
    let jb = join_with_timeout(yb, Duration::from_secs(20))
        .await
        .unwrap();
    let ra = ja.await.unwrap();
    assert!(matches!(ra, Some(Ok(()))), "node-a: {}", fmt_join(&ra));
    assert!(matches!(jb, Some(Ok(()))), "node-b: {}", fmt_join(&jb));
    let y_records: usize = y_out.into_iter().map(|o| o.join().unwrap().len()).sum();
    assert_eq!(y_records, 20);

    x_feeder.join().unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(xb_in);
    let (rxa, rxb) = (
        join_with_timeout(xa, Duration::from_secs(20)),
        join_with_timeout(xb, Duration::from_secs(20)),
    );
    let (rxa, rxb) = (rxa.await.unwrap(), rxb.await.unwrap());
    assert!(
        matches!(rxa, Some(Ok(()))) && matches!(rxb, Some(Ok(()))),
        "background dataflow: a={} b={}",
        fmt_join(&rxa),
        fmt_join(&rxb)
    );
    let x_counts: Vec<usize> = x_out.into_iter().map(|o| o.join().unwrap().len()).collect();
    let x_records: usize = x_counts.iter().sum();
    assert_eq!(x_records, 800, "X records per node: {x_counts:?}");
    drop_runtimes(vec![rt_ya, rt_yb, rt_xa, rt_xb]).await;
}

/// Dataflow Y is cancelled at node-b and both nodes drain (`drain_on_cancel`).
/// node-a finishes draining while its input release for Y (which node-b needs
/// to finish) is queued behind another dataflow's (X) traffic on a congested
/// connection. node-a stops waiting for node-b (which cancelled), but it must
/// not unregister Y before those frames have left its queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancelled_drain_flushes_final_progress_queued_behind_another_dataflow() {
    let mut mesh = Mesh::new(&["node-a", "node-b"], 256, |_, _| Duration::ZERO);
    let a_to_b = mesh.valve("node-a", "node-b");
    let ma = mesh.managers("node-a");
    let mb = mesh.managers("node-b");
    let topo = two_nodes("node-a", "node-b");
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("data")?
            .exchange("x", |x: &u64| *x)
            .map("id", |_t, x| x)
            .output("out")?;
        Ok(())
    };
    let drain = || SpawnOptions::new().drain_on_cancel(Duration::from_secs(8));

    let df = DataflowId::new();
    let (ha, hb) = (
        spawn_node_with_options(topo.clone(), "node-a", df, shared(&ma), 1, build, drain()),
        spawn_node_with_options(topo.clone(), "node-b", df, shared(&mb), 1, build, drain()),
    );
    let ((rt_ya, ya), (rt_yb, yb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ya, mut yb) = (ya.unwrap(), yb.unwrap());
    let _y_out = [
        collect(ya.take_output::<u64>(0, "out").unwrap()),
        collect(yb.take_output::<u64>(0, "out").unwrap()),
    ];
    // node-b's Y input closes now (its release reaches node-a); node-a's stays
    // open until after the cancellation, so its release (which node-b needs to
    // finish) is produced while a -> b is congested.
    drop(yb.take_input::<u64>(0, "data").unwrap());
    let ya_in = ya.take_input::<u64>(0, "data").unwrap();

    let x = DataflowId::new();
    let (hx_a, hx_b) = (
        spawn_node(topo.clone(), "node-a", x, shared(&ma), 1, build),
        spawn_node(topo, "node-b", x, shared(&mb), 1, build),
    );
    let ((rt_xa, xa), (rt_xb, xb)) = (hx_a.await.unwrap(), hx_b.await.unwrap());
    let (mut xa, mut xb) = (xa.unwrap(), xb.unwrap());
    let _x_out = [
        collect(xa.take_output::<u64>(0, "out").unwrap()),
        collect(xb.take_output::<u64>(0, "out").unwrap()),
    ];
    drop(xb.take_input::<u64>(0, "data").unwrap());
    let x_in = xa.take_input::<u64>(0, "data").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Congest a -> b with X's traffic.
    a_to_b.pause();
    let x_feeder = std::thread::spawn(move || {
        for t in 0..400u64 {
            if x_in.send(t, vec![2 * t + 1, 2 * t + 3]).is_err() || x_in.advance_to(t + 1).is_err()
            {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // b -> a is open: node-a receives the Cancel (node-b is now gone as far as
    // node-a is concerned) and drains as well.
    yb.cancel();
    let jb = join_with_timeout(yb, Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(ya_in);
    let ja = join_with_timeout(ya, Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    a_to_b.resume();
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    assert!(matches!(ra, Some(Ok(()))), "node-a: {}", fmt_join(&ra));
    assert!(
        matches!(rb, Some(Ok(()))),
        "node-b must finish draining with node-a's final progress: {}",
        fmt_join(&rb)
    );
    x_feeder.join().unwrap();
    let (rxa, rxb) = (
        join_with_timeout(xa, Duration::from_secs(20)),
        join_with_timeout(xb, Duration::from_secs(20)),
    );
    let (rxa, rxb) = (rxa.await.unwrap(), rxb.await.unwrap());
    assert!(
        matches!(rxa, Some(Ok(()))) && matches!(rxb, Some(Ok(()))),
        "background dataflow: a={} b={}",
        fmt_join(&rxa),
        fmt_join(&rxb)
    );
    drop_runtimes(vec![rt_ya, rt_yb, rt_xa, rt_xb]).await;
}

// ---------------------------------------------------------------------------
// Burst then complete: node-a sends a burst routed entirely to node-b
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Producer {
    /// input -> exchange
    Input,
    /// input -> map -> exchange
    Map,
}

/// How a burst round ended when it did not fail.
#[derive(Debug)]
enum Burst {
    /// Every node succeeded and every record arrived exactly once.
    Exact,
    /// A node reported the loss (see `many_small_frames_then_complete_shared`).
    LoudLoss(String),
}

/// Whether a node's failure reports lost records.
fn reports_data_loss(r: &Option<Result<()>>) -> bool {
    matches!(r, Some(Err(e)) if e.to_string().contains("data lost"))
}

/// node-a sends `records` records of `size` bytes, all routed to node-b, and
/// closes its input; node-b's input is closed at start. node-a can observe
/// global completion while its frames are still queued.
///
/// Returns an error for a hang, an unexplained failure, or a silent loss
/// (every node `Ok` but records missing).
async fn burst_then_complete<R, W>(
    ta: ClusterSpawnTransport<R, W>,
    tb: ClusterSpawnTransport<R, W>,
    records: usize,
    size: usize,
    producer: Producer,
) -> std::result::Result<Burst, String>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let topo = two_nodes("a", "b");
    let id = DataflowId::new();
    let build = move |b: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = b.input::<Vec<u8>>("data")?;
        let input = if producer == Producer::Map {
            input.map("id", |_t, x: Vec<u8>| x)
        } else {
            input
        };
        input
            .exchange_by_hash("to-b", |_x: &Vec<u8>| 1)
            .map("len", |_t, x: Vec<u8>| x.len() as u64)
            .output("out")?;
        Ok(())
    };
    let ha = spawn_node(topo.clone(), "a", id, ta, 1, build);
    let hb = spawn_node(topo, "b", id, tb, 1, build);
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (
        ca.map_err(|e| format!("spawn a: {e}"))?,
        cb.map_err(|e| format!("spawn b: {e}"))?,
    );
    let ob = collect(cb.take_output::<u64>(0, "out").unwrap());
    let oa = collect(ca.take_output::<u64>(0, "out").unwrap());
    drop(cb.take_input::<Vec<u8>>(0, "data").unwrap());
    let sa = ca.take_input::<Vec<u8>>(0, "data").unwrap();
    let feeder = std::thread::spawn(move || {
        for k in 0..records {
            sa.send(0, vec![vec![k as u8; size]])
                .map_err(|e| format!("send of record {k} failed: {e}"))?;
        }
        Ok::<_, String>(())
    });
    let ja = join_with_timeout(ca, Duration::from_secs(20));
    let jb = join_with_timeout(cb, Duration::from_secs(20));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    if ra.is_none() || rb.is_none() {
        std::mem::forget((rt_a, rt_b));
        return Err(format!("a={} b={} (hung)", fmt_join(&ra), fmt_join(&rb)));
    }
    let fed = feeder.join().unwrap();
    let got_b = ob.join().unwrap().len();
    let got_a = oa.join().unwrap().len();
    drop_runtimes(vec![rt_a, rt_b]).await;
    let report = format!(
        "feeder={fed:?} a={} b={} received_at_b={got_b}/{records} at_a={got_a}",
        fmt_join(&ra),
        fmt_join(&rb)
    );
    if fed.is_ok()
        && matches!(ra, Some(Ok(())))
        && matches!(rb, Some(Ok(())))
        && got_b == records
        && got_a == 0
    {
        return Ok(Burst::Exact);
    }
    if reports_data_loss(&ra) || reports_data_loss(&rb) {
        return Ok(Burst::LoudLoss(report));
    }
    Err(report)
}

async fn dedicated_pair(capacity: usize) -> (support::TcpTransport, support::TcpTransport) {
    let mut conns = support::tcp_mesh(&["a", "b"]).await;
    (
        ClusterSpawnTransport::dedicated(conns.remove("a").unwrap(), capacity),
        ClusterSpawnTransport::dedicated(conns.remove("b").unwrap(), capacity),
    )
}

async fn shared_pair() -> (Managers, Managers) {
    let mut managers = tcp_shared_mesh(&["a", "b"], 1).await;
    (managers.remove("a").unwrap(), managers.remove("b").unwrap())
}

/// Runs `rounds` burst rounds; fails on any hang, unexplained error or
/// silent loss, and returns how many rounds reported a loss loudly.
async fn assert_no_silent_loss<F, Fut>(what: &str, rounds: usize, mut round: F)
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<Burst, String>>,
{
    let mut failures = Vec::new();
    let mut loud = Vec::new();
    for i in 0..rounds {
        match round(i).await {
            Ok(Burst::Exact) => {}
            Ok(Burst::LoudLoss(report)) => loud.push(format!("round {i}: {report}")),
            Err(report) => failures.push(format!("round {i}: {report}")),
        }
    }
    if !loud.is_empty() {
        eprintln!(
            "{what}: {} round(s) failed loudly with data loss: {loud:#?}",
            loud.len()
        );
    }
    assert!(failures.is_empty(), "{what}: {failures:#?}");
}

/// Shared mode with long-lived managers: thousands of small frames are still
/// queued when node-a completes locally.
///
/// On the pre-handshake runtime almost every round hangs or fails with a
/// spurious transport error (the queued frames were dropped at unregister).
/// Roughly one round in 60-100 still hits the signed-sum completion bug
/// (audit C1/C2): node-b consumes the first record before node-a's matching
/// produced-message count arrives and finishes early. Before, that round
/// returned `a=Ok b=Ok received_at_b=1/3000`; now the records that reach
/// node-b after its operator finished (or that node-a sends after node-b's
/// end-of-stream) are reported as data loss and the run fails. Silent loss
/// is never acceptable.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_small_frames_then_complete_shared() {
    let (ma, mb) = shared_pair().await;
    assert_no_silent_loss("shared", 5, |_| {
        burst_then_complete(shared(&ma), shared(&mb), 3000, 64, Producer::Map)
    })
    .await;
}

/// Dedicated-mode counterpart.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_small_frames_then_complete_dedicated() {
    assert_no_silent_loss("dedicated", 5, |_| async {
        let (ta, tb) = dedicated_pair(1024).await;
        burst_then_complete(ta, tb, 3000, 64, Producer::Map).await
    })
    .await;
}

/// Large records routed to node-b while node-a finishes first.
///
/// This shape hits the C1/C2 premature completion often: node-b used to
/// report success after receiving only part of the burst (typically one
/// record). With the handshake, node-a's frames reach node-b before node-a
/// releases the connection, and records that arrive after node-b's operator
/// finished fail the run loudly instead of vanishing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_records_from_early_finisher_never_vanish() {
    for producer in [Producer::Input, Producer::Map] {
        assert_no_silent_loss(&format!("dedicated {producer:?}"), 10, |_| async {
            let (ta, tb) = dedicated_pair(1024).await;
            burst_then_complete(ta, tb, 40, 1 << 20, producer).await
        })
        .await;
        assert_no_silent_loss(&format!("shared {producer:?}"), 10, |_| async {
            let (ma, mb) = shared_pair().await;
            burst_then_complete(shared(&ma), shared(&mb), 40, 1 << 20, producer).await
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// Per-job managers released by the node that finishes first
// ---------------------------------------------------------------------------

/// `input -> exchange -> inspect(slow on b) -> output`; node-a sends `n`
/// batches and closes. The managers are owned only by the dataflow, so the
/// node that finishes first drops the last reference to its managers.
async fn per_job_managers_run(n: u64, slow_b: Duration) -> std::result::Result<(), String> {
    let (ma, mb) = shared_pair().await;
    let topo = two_nodes("a", "b");
    let id = DataflowId::new();
    let received = Arc::new(AtomicU64::new(0));
    let build = |slow: Duration, received: Arc<AtomicU64>| {
        move |b: &mut DataflowBuilder<u64>| -> Result<()> {
            let received = Arc::clone(&received);
            b.input::<u64>("data")?
                .exchange("ex", |x: &u64| *x)
                .inspect_batch("slow", move |_t, d: &[u64]| {
                    received.fetch_add(d.len() as u64, Ordering::Relaxed);
                    if !slow.is_zero() {
                        std::thread::sleep(slow);
                    }
                })
                .output("out")?;
            Ok(())
        }
    };
    // The transports hold the only manager references.
    let ha = spawn_node(
        topo.clone(),
        "a",
        id,
        ClusterSpawnTransport::shared(ma, 1024),
        1,
        build(Duration::ZERO, Arc::clone(&received)),
    );
    let hb = spawn_node(
        topo,
        "b",
        id,
        ClusterSpawnTransport::shared(mb, 1024),
        1,
        build(slow_b, Arc::clone(&received)),
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let oa = collect(ca.take_output::<u64>(0, "out").unwrap());
    let ob = collect(cb.take_output::<u64>(0, "out").unwrap());
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    for k in 0..n {
        sa.send(0, vec![2 * k, 2 * k + 1]).unwrap();
    }
    drop(sa);
    let ja = join_with_timeout(ca, Duration::from_secs(15));
    let jb = join_with_timeout(cb, Duration::from_secs(15));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    if ra.is_none() || rb.is_none() {
        std::mem::forget((rt_a, rt_b));
        return Err(format!("a={} b={} (hung)", fmt_join(&ra), fmt_join(&rb)));
    }
    let got = oa.join().unwrap().len() + ob.join().unwrap().len();
    drop_runtimes(vec![rt_a, rt_b]).await;
    if !matches!(ra, Some(Ok(()))) || !matches!(rb, Some(Ok(()))) || got as u64 != 2 * n {
        return Err(format!(
            "a={} b={} records={got}/{}",
            fmt_join(&ra),
            fmt_join(&rb),
            2 * n
        ));
    }
    Ok(())
}

/// The early finisher dropping its (per-job) managers must not hurt the
/// slower peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn per_job_managers_released_by_early_finisher() {
    let mut failures = Vec::new();
    for i in 0..5 {
        if let Err(e) = per_job_managers_run(20, Duration::from_millis(30)).await {
            failures.push(format!("iter {i}: {e}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// ---------------------------------------------------------------------------
// Four nodes, single exchange, per-job managers
// ---------------------------------------------------------------------------

fn records_for(worker: usize, epoch: u64) -> u64 {
    ((worker as u64 * 7 + epoch * 13) % 23) * 3
}

/// 4 nodes (2+1+1+2 workers), `input -> exchange -> count -> output` over
/// `epochs` epochs. `keep_managers` holds extra references to every node's
/// managers until the end; otherwise each dataflow owns its managers.
async fn four_node_exchange(epochs: u64, keep_managers: bool) -> std::result::Result<(), String> {
    let spec = [("a", 2usize), ("b", 1), ("c", 1), ("d", 2)];
    let ids: Vec<&str> = spec.iter().map(|(n, _)| *n).collect();
    let topo =
        ClusterTopology::multi_node(spec.iter().map(|(n, w)| NodeConfig::new(*n, *w)).collect())
            .unwrap();
    let total_workers: usize = spec.iter().map(|(_, w)| *w).sum();
    let mut managers = tcp_shared_mesh(&ids, 2).await;
    let kept: Vec<Managers> = if keep_managers {
        managers.values().cloned().collect()
    } else {
        Vec::new()
    };
    let received = Arc::new(AtomicUsize::new(0));
    let id = DataflowId::new();
    let mut handles = Vec::new();
    for node in ids.iter().rev() {
        let received = Arc::clone(&received);
        let transport = ClusterSpawnTransport::shared(managers.remove(*node).unwrap(), 1024);
        handles.push(spawn_node(topo.clone(), node, id, transport, 2, move |b| {
            let received = Arc::clone(&received);
            b.input::<u64>("data")?
                .exchange("ex1", |x: &u64| *x)
                .inspect("count", move |_t, _x| {
                    received.fetch_add(1, Ordering::Relaxed);
                })
                .output("out")?;
            Ok(())
        }));
    }
    let mut rts = Vec::new();
    let mut joins = Vec::new();
    let mut outs = Vec::new();
    let mut feeders = Vec::new();
    for h in handles {
        let (rt, c) = h.await.unwrap();
        let mut c = c.map_err(|e| format!("spawn: {e}"))?;
        let (start, _) = c.local_worker_range();
        for l in 0..c.num_local_workers() {
            outs.push(collect(c.take_output::<u64>(l, "out").unwrap()));
            let s = c.take_input::<u64>(l, "data").unwrap();
            let g = start + l;
            feeders.push(std::thread::spawn(move || {
                for e in 0..epochs {
                    let n = records_for(g, e);
                    let batch: Vec<u64> = (0..n).map(|i| (g as u64) << 40 | e << 20 | i).collect();
                    if (!batch.is_empty() && s.send(e, batch).is_err())
                        || s.advance_to(e + 1).is_err()
                    {
                        return;
                    }
                }
            }));
        }
        joins.push(join_with_timeout(c, Duration::from_secs(25)));
        rts.push(rt);
    }
    let expected: u64 = (0..total_workers)
        .flat_map(|g| (0..epochs).map(move |e| records_for(g, e)))
        .sum();
    let started = Instant::now();
    let mut results = Vec::new();
    for j in joins {
        results.push(j.await.unwrap());
    }
    if results.iter().any(Option::is_none) {
        std::mem::forget(rts);
        return Err(format!(
            "hang after {:?}: {:?}",
            started.elapsed(),
            results.iter().map(fmt_join).collect::<Vec<_>>()
        ));
    }
    for f in feeders {
        let _ = f.join();
    }
    for o in outs {
        let _ = o.join();
    }
    drop_runtimes(rts).await;
    drop(kept);
    let got = received.load(Ordering::Relaxed) as u64;
    if results.iter().any(|r| !matches!(r, Some(Ok(())))) || got != expected {
        return Err(format!(
            "results={:?} received={got}/{expected}",
            results.iter().map(fmt_join).collect::<Vec<_>>()
        ));
    }
    Ok(())
}

/// No spurious "shared transport failure" when nodes finish at different
/// times and drop their per-job managers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn four_nodes_single_exchange_no_spurious_failure() {
    let mut failures = Vec::new();
    for i in 0..5 {
        for keep in [false, true] {
            if let Err(e) = four_node_exchange(200, keep).await {
                failures.push(format!("iter {i} keep_managers={keep}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
