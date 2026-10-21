//! A cluster link that ends without its clean end-of-stream, or that carries
//! a corrupt frame, fails the dataflow on every node. It must never complete
//! `Ok` with missing data, and never hang.
//!
//! Regression tests for audit findings C5 (connection loss treated as a clean
//! end-of-stream), H6 (`report_node_leave` after a connection drop) and M3
//! (peer cancel with a foreign caller runtime), plus the preference for the
//! peer's own `Cancel` over the EOF that follows it.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

#[path = "support/faults.rs"]
mod faults;

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use faults::{FaultMesh, FrameKind};
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    CancellationReason, ClusterTopology, DataflowBuilder, DataflowId, Error, NodeConfig, Result,
    RuntimeConfig, RuntimeHandle, SpawnOptions,
};
use support::{drop_runtimes, fmt_join, join_with_timeout, spawn_node, tcp_pair};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    Shared,
}

const MODES: [Mode; 2] = [Mode::Dedicated, Mode::Shared];

/// Generous bound on failure detection: shared managers retry a lost
/// connection for about 3 s before reporting it.
const DETECT: Duration = Duration::from_secs(15);

fn topology(nodes: &[&str], workers: usize) -> ClusterTopology {
    ClusterTopology::multi_node(nodes.iter().map(|n| NodeConfig::new(*n, workers)).collect())
        .unwrap()
}

fn exchange(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .exchange("by_val", |x: &u64| *x)
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// `input -> map -> exchange -> unary_notify(count per epoch) -> output`:
/// every node needs every peer's progress to fire a notification.
fn notify_count(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .exchange_by_hash("route", |x: &u64| *x)
        .unary_notify("count", {
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
        .output("out")?;
    Ok(())
}

struct Node {
    name: String,
    rt: RuntimeHandle,
    df: Option<ClusterSpawnedDataflow<u64>>,
    /// Shared managers outlive the dataflow, as in a real deployment
    /// (dropping the last reference aborts their writers, including a
    /// queued `Cancel`).
    _managers: Option<support::Managers>,
}

impl Node {
    fn df(&mut self) -> &mut ClusterSpawnedDataflow<u64> {
        self.df.as_mut().expect("dataflow already joined")
    }

    fn join(&mut self, timeout: Duration) -> tokio::task::JoinHandle<Option<Result<()>>> {
        join_with_timeout(self.df.take().expect("dataflow already joined"), timeout)
    }
}

/// Spawn every node of `nodes` over `mesh` (one worker each).
async fn start(
    mesh: &mut FaultMesh,
    mode: Mode,
    nodes: &[&str],
    build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
) -> Vec<Node> {
    let topo = topology(nodes, 1);
    let id = DataflowId::new();
    let mut managers = Vec::new();
    let handles: Vec<_> = nodes
        .iter()
        .map(|n| match mode {
            Mode::Dedicated => {
                managers.push(None);
                spawn_node(topo.clone(), n, id, mesh.dedicated(n, 1024), 2, build)
            }
            Mode::Shared => {
                let m = mesh.managers(n);
                managers.push(Some(std::sync::Arc::clone(&m)));
                spawn_node(
                    topo.clone(),
                    n,
                    id,
                    ClusterSpawnTransport::shared(m, 1024),
                    2,
                    build,
                )
            }
        })
        .collect();
    let mut started = Vec::new();
    for ((name, handle), managers) in nodes.iter().zip(handles).zip(managers) {
        let (rt, df) = handle.await.unwrap();
        started.push(Node {
            name: name.to_string(),
            rt,
            df: Some(df.unwrap_or_else(|e| panic!("{mode:?}: {name} failed to spawn: {e}"))),
            _managers: managers,
        });
    }
    started
}

/// Join every node concurrently.
async fn join_all(nodes: &mut [Node], timeout: Duration) -> Vec<(String, Option<Result<()>>)> {
    let joins: Vec<_> = nodes
        .iter_mut()
        .map(|node| (node.name.clone(), node.join(timeout)))
        .collect();
    let mut results = Vec::new();
    for (name, join) in joins {
        results.push((name, join.await.unwrap()));
    }
    results
}

fn describe(results: &[(String, Option<Result<()>>)]) -> String {
    results
        .iter()
        .map(|(n, r)| format!("{n}={}", fmt_join(r)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The node reported corrupt traffic from `peer` as data loss.
fn data_loss_from(result: &Option<Result<()>>, peer: &str) -> bool {
    matches!(
        result,
        Some(Err(Error::Communication(instancy::CommunicationError::DataLoss { peer_id, .. })))
            if peer_id == peer
    )
}

fn cancel_reason(result: &Option<Result<()>>) -> Option<&CancellationReason> {
    match result {
        Some(Err(Error::Cancelled { reason })) => reason.as_ref(),
        _ => None,
    }
}

async fn shutdown(nodes: Vec<Node>) {
    let mut rts = Vec::new();
    for mut node in nodes {
        drop(node.df.take());
        drop(node._managers.take());
        rts.push(node.rt);
    }
    drop_runtimes(rts).await;
}

// ---------------------------------------------------------------------------
// C5: connection loss while running
// ---------------------------------------------------------------------------

/// Two running nodes; the link between them is cut (peer crash / partition)
/// while both inputs are open. Neither node can complete correctly, so both
/// must fail. Previously the dedicated transport treated the EOF as a clean
/// end-of-stream and both nodes reported success with records missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn connection_loss_while_running_fails_both_nodes() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
        let _oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
        let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
        sa.send(0, (0..10).collect()).unwrap();
        sb.send(0, (10..20).collect()).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        mesh.cut("node-a", "node-b");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = sa.send(1, (100..120).collect());
        let _ = sb.send(1, (200..220).collect());
        drop(sa);
        drop(sb);

        let results = join_all(&mut nodes, DETECT).await;
        assert!(
            results.iter().all(|(_, r)| matches!(r, Some(Err(_)))),
            "{mode:?}: every node must fail after losing its peer: {}",
            describe(&results)
        );
        shutdown(nodes).await;
    }
}

/// Port of the dedicated-transport loss repro: epochs keep advancing on both
/// nodes while the only connection dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn dedicated_connection_loss_mid_epoch_stream_fails_both_nodes() {
    let (a_end, mut relay_a) = tokio::io::duplex(1 << 20);
    let (b_end, mut relay_b) = tokio::io::duplex(1 << 20);
    let relay = tokio::spawn(async move {
        let _ = tokio::io::copy_bidirectional(&mut relay_a, &mut relay_b).await;
    });
    let (ar, aw) = tokio::io::split(a_end);
    let (br, bw) = tokio::io::split(b_end);

    let topo = topology(&["node-a", "node-b"], 1);
    let id = DataflowId::new();
    let handle = tokio::runtime::Handle::current();
    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..Default::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..Default::default()
    })
    .unwrap();
    let (ca, cb) = tokio::join!(
        rt_a.spawn_cluster_async(
            "dedicated-loss",
            topo.clone(),
            "node-a",
            id,
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-b".into(),
                    reader: ar,
                    writer: aw,
                }],
                1024,
            ),
            Duration::from_secs(5),
            exchange,
            &handle,
            SpawnOptions::new(),
        ),
        rt_b.spawn_cluster_async(
            "dedicated-loss",
            topo,
            "node-b",
            id,
            ClusterSpawnTransport::dedicated(
                vec![PeerConnection {
                    node_id: "node-a".into(),
                    reader: br,
                    writer: bw,
                }],
                1024,
            ),
            Duration::from_secs(5),
            exchange,
            &handle,
            SpawnOptions::new(),
        ),
    );
    let mut ca = ca.unwrap();
    let mut cb = cb.unwrap();
    let out_a = ca.take_output::<u64>(0, "out").unwrap();
    let out_b = cb.take_output::<u64>(0, "out").unwrap();
    let in_a = ca.take_input::<u64>(0, "data").unwrap();
    let in_b = cb.take_input::<u64>(0, "data").unwrap();

    for t in 0..10u64 {
        let _ = in_a.send(t, (0..10).map(|i| t * 10 + i).collect());
        let _ = in_a.advance_to(t + 1);
        let _ = in_b.advance_to(t + 1);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    relay.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for t in 10..20u64 {
        let _ = in_a.send(t, (0..10).map(|i| t * 10 + i).collect());
        let _ = in_a.advance_to(t + 1);
        let _ = in_b.advance_to(t + 1);
    }
    drop(in_a);
    drop(in_b);

    let ja = tokio::task::spawn_blocking(move || ca.join_blocking());
    let jb = tokio::task::spawn_blocking(move || cb.join_blocking());
    let res = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(ja, jb) })
        .await
        .expect("HANG: dedicated dataflow did not terminate after connection loss");
    let records: usize = out_a
        .collect_data()
        .into_iter()
        .chain(out_b.collect_data())
        .map(|(_, d)| d.len())
        .sum();
    let (ra, rb) = (res.0.unwrap(), res.1.unwrap());
    assert!(
        ra.is_err() && rb.is_err(),
        "connection loss not surfaced: a={ra:?} b={rb:?} records={records}/200"
    );
    drop_runtimes(vec![rt_a, rt_b]).await;
}

/// Node B dies behind a killable TCP proxy while its input is still open.
/// Node A must not report success with B's contribution missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxied_tcp_peer_crash_is_not_reported_as_success() {
    let (a_side, proxy_a) = tcp_pair().await;
    let (proxy_b, b_side) = tcp_pair().await;
    let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (mut ar, mut aw) = proxy_a.into_split();
        let (mut br, mut bw) = proxy_b.into_split();
        let forward_ab = async move {
            let _ = tokio::io::copy(&mut ar, &mut bw).await;
            let _ = bw.shutdown().await;
        };
        let forward_ba = async move {
            let _ = tokio::io::copy(&mut br, &mut aw).await;
            let _ = aw.shutdown().await;
        };
        tokio::select! {
            _ = kill_rx => {}
            _ = async { tokio::join!(forward_ab, forward_ba) } => {}
        }
    });
    let (ar, aw) = a_side.into_split();
    let (br, bw) = b_side.into_split();
    let topo = topology(&["node-a", "node-b"], 1);
    let id = DataflowId::new();
    let ha = spawn_node(
        topo.clone(),
        "node-a",
        id,
        ClusterSpawnTransport::dedicated(
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: ar,
                writer: aw,
            }],
            1024,
        ),
        1,
        exchange,
    );
    let hb = spawn_node(
        topo,
        "node-b",
        id,
        ClusterSpawnTransport::dedicated(
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: br,
                writer: bw,
            }],
            1024,
        ),
        1,
        exchange,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let _out_a = ca.take_output::<u64>(0, "out").unwrap();
    let _out_b = cb.take_output::<u64>(0, "out").unwrap();
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();
    sa.send(0, (0..10).collect()).unwrap();
    sb.send(0, (100..110).collect()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // B "crashes": its connection drops; B never sends anything again.
    let _ = kill_tx.send(());
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(sa);
    let ra = join_with_timeout(ca, DETECT).await.unwrap();
    assert!(
        matches!(ra, Some(Err(_))),
        "node-a must fail after its peer crashed mid-stream: {}",
        fmt_join(&ra)
    );
    drop(sb);
    drop(cb);
    drop_runtimes(vec![rt_a, rt_b]).await;
}

// ---------------------------------------------------------------------------
// C5: progress channel loss and corruption
// ---------------------------------------------------------------------------

/// Control: the fault relay itself is transparent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn clean_run_through_fault_relay_completes() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, notify_count).await;
        let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
        let oa = support::collect(nodes[0].df().take_output::<u64>(0, "out").unwrap());
        let ob = support::collect(nodes[1].df().take_output::<u64>(0, "out").unwrap());
        sa.send(5, vec![0, 2, 4]).unwrap();
        sb.send(5, vec![1, 3]).unwrap();
        drop(sa);
        drop(sb);
        let results = join_all(&mut nodes, DETECT).await;
        assert!(
            results.iter().all(|(_, r)| matches!(r, Some(Ok(())))),
            "{mode:?}: {}",
            describe(&results)
        );
        let mut counts: Vec<u64> = oa
            .join()
            .unwrap()
            .into_iter()
            .chain(ob.join().unwrap())
            .map(|(_, c)| c)
            .collect();
        counts.sort();
        assert_eq!(counts.iter().sum::<u64>(), 5, "{mode:?}: {counts:?}");
        shutdown(nodes).await;
    }
}

/// A corrupted progress frame (CRC mismatch) fails the receiving node at once
/// and, through its `Cancel`, every peer. Previously it only stopped the
/// bridges and every node hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn corrupted_progress_frame_fails_every_node() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, notify_count).await;
        let _oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
        let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
        let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
        sa.send(5, vec![0]).unwrap();
        drop(sa);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let b_to_a = mesh.ctl("node-b", "node-a");
        b_to_a.corrupt_next(FrameKind::Progress);
        sb.advance_to(10).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !b_to_a.corrupted() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            b_to_a.corrupted(),
            "{mode:?}: precondition: a progress frame was corrupted"
        );

        let results = join_all(&mut nodes, Duration::from_secs(5)).await;
        drop(sb);
        assert!(
            results.iter().all(|(_, r)| matches!(r, Some(Err(_)))),
            "{mode:?}: a corrupt progress frame must fail every node: {}",
            describe(&results)
        );
        assert!(
            data_loss_from(&results[0].1, "node-b"),
            "{mode:?}: node-a detected the corruption: {}",
            fmt_join(&results[0].1)
        );
        shutdown(nodes).await;
    }
}

/// The link is cut after node A closed its input: A needs B's progress and
/// can never complete, so it must fail rather than hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn progress_link_loss_fails_dataflow() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, notify_count).await;
        let _oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
        let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
        let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
        sa.send(5, vec![0]).unwrap();
        drop(sa);
        tokio::time::sleep(Duration::from_millis(300)).await;

        mesh.cut("node-a", "node-b");
        let ra = nodes[0].join(DETECT).await.unwrap();
        drop(sb);
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?}: link loss must fail node-a: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

// ---------------------------------------------------------------------------
// C5 / H8: data channels
// ---------------------------------------------------------------------------

/// Node A has closed its own input; node B sent a first batch and keeps its
/// input open. The link is then cut. A's data channels from B close without
/// their close sentinel: that is not an end of stream, so A must fail
/// instead of completing with B's later records missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_connection_loss_is_not_a_clean_end_of_stream() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
        let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
        drop(nodes[0].df().take_input::<u64>(0, "data").unwrap());
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
        sb.send(0, (0..20).collect()).unwrap();

        // Wait for node-a's share to arrive (the batch is routed by value).
        let mut received = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            while let Some(event) = oa.try_recv() {
                if let instancy::dataflow::OutputEvent::Data { data, .. } = event {
                    received += data.len();
                }
            }
            if received > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(received > 0, "{mode:?}: precondition: node-a got its share");

        let ja = nodes[0].join(DETECT);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !ja.is_finished(),
            "{mode:?}: node-a finished while node-b's input is open"
        );
        mesh.cut("node-a", "node-b");
        let ra = ja.await.unwrap();
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?}: node-a must fail after losing its peer mid-stream: {}",
            fmt_join(&ra)
        );
        drop(sb);
        shutdown(nodes).await;
    }
}

/// A data frame that cannot be decoded used to become an error envelope that
/// every operator ignored, silently dropping the batch. It now fails the
/// receiving node (`DataLoss`) and, through its `Cancel`, every peer that
/// is still running (node-b keeps its input open, so it cannot have
/// finished; a node that already finished keeps its own, complete, result).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn corrupted_data_frame_fails_every_node() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let oa = support::collect(nodes[0].df().take_output::<u64>(0, "out").unwrap());
        let ob = support::collect(nodes[1].df().take_output::<u64>(0, "out").unwrap());
        let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();

        let b_to_a = mesh.ctl("node-b", "node-a");
        b_to_a.corrupt_next(FrameKind::Data);
        sb.send(0, (0..20).collect()).unwrap();
        sa.send(0, (100..110).collect()).unwrap();
        drop(sa);

        let ra = nodes[0].join(DETECT).await.unwrap();
        assert!(
            b_to_a.corrupted(),
            "{mode:?}: precondition: a data frame was corrupted"
        );
        assert!(
            data_loss_from(&ra, "node-b"),
            "{mode:?}: node-a must fail on the corrupt frame: {}",
            fmt_join(&ra)
        );
        let rb = nodes[1].join(DETECT).await.unwrap();
        assert!(
            matches!(
                cancel_reason(&rb),
                Some(CancellationReason::PeerCancelled { peer_id, .. }) if peer_id == "node-a"
            ),
            "{mode:?}: node-b must be cancelled by node-a: {}",
            fmt_join(&rb)
        );
        drop(sb);
        let _ = (oa.join(), ob.join());
        shutdown(nodes).await;
    }
}

// ---------------------------------------------------------------------------
// Protocol violations: data after the end of a stream
// ---------------------------------------------------------------------------

/// A data frame that arrives on a channel after its close sentinel (here
/// injected by the relay) means the sender broke the protocol: the receiver
/// already treated the channel as complete. Node-b (still running) fails at
/// once with `DataLoss` and cancels node-a.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_after_close_sentinel_fails_the_receiver() {
    let names = ["node-a", "node-b"];
    let mut mesh = FaultMesh::new(&names, 2, false);
    let mut nodes = start(&mut mesh, Mode::Dedicated, &names, notify_count).await;
    let _oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
    let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
    let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
    // node-b keeps its input open: it is running when the violation arrives.
    let sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();

    // A valid data envelope (time 0, one u64 record) for the same channel.
    let mut late = vec![0x01];
    late.extend_from_slice(&0u64.to_le_bytes());
    late.extend_from_slice(&1u32.to_le_bytes());
    late.extend_from_slice(&42u64.to_le_bytes());
    let a_to_b = mesh.ctl("node-a", "node-b");
    a_to_b.inject_after_close_sentinel(late);

    // Odd values are routed to node-b; closing node-a's input makes its
    // pre-exchange operator close the channel (sentinel) right away.
    sa.send(0, vec![1, 3, 5]).unwrap();
    drop(sa);

    let rb = nodes[1].join(DETECT).await.unwrap();
    assert!(
        a_to_b.injected_after_sentinel(),
        "precondition: frame injected"
    );
    assert!(
        matches!(
            &rb,
            Some(Err(Error::Communication(instancy::CommunicationError::DataLoss { peer_id, detail })))
                if peer_id == "node-a" && detail.contains("close marker")
        ),
        "node-b must fail on the protocol violation: {}",
        fmt_join(&rb)
    );
    let ra = nodes[0].join(DETECT).await.unwrap();
    assert!(
        matches!(
            cancel_reason(&ra),
            Some(CancellationReason::PeerCancelled { peer_id, .. }) if peer_id == "node-b"
        ),
        "node-a must be cancelled by node-b: {}",
        fmt_join(&ra)
    );
    drop(sb);
    shutdown(nodes).await;
}

// ---------------------------------------------------------------------------
// The peer's Cancel wins over the EOF that follows it
// ---------------------------------------------------------------------------

/// Node B cancels: it broadcasts `Cancel`, stops its workers and drops its
/// transport, so node A sees the `Cancel` and then EOF on every channel.
/// Node A must report `PeerCancelled`, not a connection loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_then_eof_reports_peer_cancelled() {
    for mode in MODES {
        for iteration in 0..20 {
            let names = ["node-a", "node-b"];
            let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
            let mut nodes = start(&mut mesh, mode, &names, exchange).await;
            let sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
            let _sb = nodes[1].df().take_input::<u64>(0, "data").unwrap();
            let _oa = nodes[0].df().take_output::<u64>(0, "out").unwrap();
            let _ob = nodes[1].df().take_output::<u64>(0, "out").unwrap();
            sa.send(0, (0..10).collect()).unwrap();
            // Let the data settle so node-a has nothing in flight to the peer
            // (a push to a closed peer fails with its own error).
            tokio::time::sleep(Duration::from_millis(50)).await;
            nodes[1].df().cancel();
            let ra = nodes[0].join(DETECT).await.unwrap();
            assert!(
                matches!(
                    cancel_reason(&ra),
                    Some(CancellationReason::PeerCancelled { peer_id, .. }) if peer_id == "node-b"
                ),
                "{mode:?} iteration {iteration}: expected PeerCancelled from node-b, got {}",
                fmt_join(&ra)
            );
            drop(sa);
            shutdown(nodes).await;
        }
    }
}

// ---------------------------------------------------------------------------
// H6: report_node_leave
// ---------------------------------------------------------------------------

/// The peer crashes (its connection drops, no `Cancel`), then the health
/// monitor calls `report_node_leave`. Previously the connection drop removed
/// the registry entry without cancelling, so this was a no-op and the
/// dataflow hung. Now the link loss itself fails the dataflow; the report may
/// find it already cancelled (and then returns 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn report_node_leave_after_connection_drop_terminates() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let _sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(nodes[0].rt.report_node_leave("node-zzz"), 0);

        mesh.cut("node-a", "node-b");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let cancelled = nodes[0].rt.report_node_leave("node-b");
        let ra = nodes[0].join(Duration::from_secs(5)).await.unwrap();
        assert!(
            matches!(ra, Some(Err(_))),
            "{mode:?}: node-a must terminate with an error (report cancelled {cancelled}): {}",
            fmt_join(&ra)
        );
        assert!(
            matches!(
                cancel_reason(&ra),
                Some(CancellationReason::NetworkError { .. } | CancellationReason::PeerDown { .. })
            ),
            "{mode:?}: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// Baseline: reporting the peer down while the connection is up cancels.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn report_node_leave_baseline_connection_up() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let _sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let cancelled = nodes[0].rt.report_node_leave("node-b");
        let ra = nodes[0].join(Duration::from_secs(5)).await.unwrap();
        assert_eq!(cancelled, 1, "{mode:?}");
        assert!(
            matches!(
                cancel_reason(&ra),
                Some(CancellationReason::PeerDown { .. })
            ),
            "{mode:?}: {}",
            fmt_join(&ra)
        );
        shutdown(nodes).await;
    }
}

/// A silent partition: the connection stays up but nothing gets through, so
/// the transport cannot notice. The application's health monitor reports
/// the peer down; that must cancel the dataflow with `PeerDown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn report_node_leave_during_silent_partition_cancels_with_peer_down() {
    for mode in MODES {
        let names = ["node-a", "node-b"];
        let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
        let mut nodes = start(&mut mesh, mode, &names, exchange).await;
        let _sa = nodes[0].df().take_input::<u64>(0, "data").unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        mesh.partition("node-a", "node-b");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let cancelled = nodes[0].rt.report_node_leave("node-b");
        let ra = nodes[0].join(Duration::from_secs(5)).await.unwrap();
        assert_eq!(cancelled, 1, "{mode:?}");
        assert!(
            matches!(
                cancel_reason(&ra),
                Some(CancellationReason::PeerDown { .. })
            ),
            "{mode:?}: {}",
            fmt_join(&ra)
        );
        mesh.cut_all();
        shutdown(nodes).await;
    }
}

// ---------------------------------------------------------------------------
// M3: peer cancellation with a foreign caller runtime
// ---------------------------------------------------------------------------

/// Spawn two nodes whose cluster tasks run on `net_rt`, then cancel node-b.
/// node-a keeps its input open, so only node-b's `Cancel` can stop it. With
/// `a_via_temporary_caller_runtime`, node-a is spawned with
/// `spawn_cluster_async` driven by a short-lived runtime other than the
/// `runtime_handle` it passes: every cluster task must still run on
/// `runtime_handle`.
fn spawn_pair_and_cancel_b(a_via_temporary_caller_runtime: bool) -> Option<Result<()>> {
    let net_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
    let h = net_rt.handle().clone();
    let topo = topology(&["node-a", "node-b"], 1);
    let id = DataflowId::new();
    let (a_end, b_end) = tokio::io::duplex(1 << 16);
    let (ar, aw) = tokio::io::split(a_end);
    let (br, bw) = tokio::io::split(b_end);
    let conn_a = PeerConnection {
        node_id: "node-b".to_string(),
        reader: ar,
        writer: aw,
    };
    let conn_b = PeerConnection {
        node_id: "node-a".to_string(),
        reader: br,
        writer: bw,
    };
    let make_rt = |h: &tokio::runtime::Handle| {
        RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            tokio_mode: instancy::TokioMode::External(h.clone()),
            ..RuntimeConfig::default()
        })
        .unwrap()
    };
    let rt_a = make_rt(&h);
    let rt_b = make_rt(&h);

    let (topo_b, h_b) = (topo.clone(), h.clone());
    let b_thread = std::thread::spawn(move || {
        let c = rt_b.spawn_cluster(
            "m3",
            topo_b,
            "node-b",
            id,
            ClusterSpawnTransport::dedicated(vec![conn_b], 1024),
            Duration::from_secs(5),
            exchange,
            &h_b,
            SpawnOptions::new(),
        );
        (rt_b, c)
    });
    let mut ca = if a_via_temporary_caller_runtime {
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let c = caller
            .block_on(rt_a.spawn_cluster_async(
                "m3",
                topo,
                "node-a",
                id,
                ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
                Duration::from_secs(5),
                exchange,
                &h,
                SpawnOptions::new(),
            ))
            .unwrap();
        drop(caller);
        c
    } else {
        rt_a.spawn_cluster(
            "m3",
            topo,
            "node-a",
            id,
            ClusterSpawnTransport::dedicated(vec![conn_a], 1024),
            Duration::from_secs(5),
            exchange,
            &h,
            SpawnOptions::new(),
        )
        .unwrap()
    };
    let (rt_b, cb) = b_thread.join().unwrap();
    let cb = cb.unwrap();
    let _sa = ca.take_input::<u64>(0, "data").unwrap();
    std::thread::sleep(Duration::from_millis(300));
    cb.cancel();
    let completion = ca.join().unwrap();
    let result = net_rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), completion)
            .await
            .ok()
    });
    drop(cb);
    rt_a.shutdown();
    rt_b.shutdown();
    drop(rt_a);
    drop(rt_b);
    net_rt.shutdown_timeout(Duration::from_secs(1));
    result
}

/// Control: node-a spawned with the blocking API.
#[test]
fn peer_cancel_reaches_node_spawned_synchronously() {
    let r = spawn_pair_and_cancel_b(false);
    assert!(
        matches!(
            cancel_reason(&r),
            Some(CancellationReason::PeerCancelled { .. })
        ),
        "{}",
        fmt_join(&r)
    );
}

/// node-a's `spawn_cluster_async` is driven by a temporary runtime that is
/// dropped right after; the peer's `Cancel` must still be received.
#[test]
fn peer_cancel_reaches_node_spawned_from_other_runtime() {
    let r = spawn_pair_and_cancel_b(true);
    assert!(
        matches!(
            cancel_reason(&r),
            Some(CancellationReason::PeerCancelled { .. })
        ),
        "the peer's Cancel was not observed: {}",
        fmt_join(&r)
    );
}
