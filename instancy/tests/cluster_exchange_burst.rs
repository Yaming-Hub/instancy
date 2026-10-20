//! Multi-node regression tests for stage-task starvation (see
//! `stage_task_starvation.rs` for the single-process tests).
//!
//! Each test runs a loopback TCP cluster, in Dedicated or Shared transport
//! mode, where the workers on node "a" push thousands of single-record
//! batches through exchange edges to workers on the same node. That is far
//! more than one exchange channel holds (1024 envelopes). Before the fix, a
//! worker whose upstream stage task was blocked on a full exchange channel
//! spent its whole activation budget re-running that blocked stage, so the
//! stage task that drains the channel never ran and the cluster hung at 100%
//! CPU. The workers on the other nodes send a small number of records to
//! worker 0 over the network at the same time.
//!
//! The traffic is shaped to stay clear of known transport and progress
//! issues that are tracked separately:
//! - Records pass through a `map` before every exchange, so the producer side
//!   reports its in-flight records to progress tracking.
//! - Cross-node traffic stays well below the per-peer frame queue, so close
//!   sentinels are never dropped for lack of queue space.
//! - Shared mode uses a single pooled connection per peer.
//! - The active tests keep every input open until worker 0 has every record,
//!   then cancel the dataflow, so they don't depend on cluster completion
//!   (see the ignored `*_then_complete` tests at the bottom).

#![cfg(feature = "transport")]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

const TIMEOUT: Duration = Duration::from_secs(90);

/// Single-record batches each worker on node "a" sends.
const LOCAL_RECORDS: u64 = 4_000;
/// Single-record batches each worker on the other nodes sends.
const REMOTE_RECORDS: u64 = 100;
/// Workers on node "a" (global workers 0 and 1).
const NODE_A_WORKERS: u64 = 2;

type Transport = ClusterSpawnTransport<OwnedReadHalf, OwnedWriteHalf>;

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.0, connected)
}

struct PreEstablishedFactory {
    connections: Mutex<VecDeque<(OwnedReadHalf, OwnedWriteHalf)>>,
}

impl ConnectionFactory for PreEstablishedFactory {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.connections.lock().unwrap().pop_front().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "no more pre-established connections",
            ))
        })
    }
}

fn shared_config() -> SharedConnectionConfig {
    SharedConnectionConfig {
        min_connections: 1,
        max_connections: 1,
        probe_interval: Duration::from_secs(3600),
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(30),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    }
}

/// Builds a fully connected loopback TCP mesh between `ids`. Also returns the
/// Shared-mode peer managers so the caller can keep them alive until every
/// node is done (in production they outlive individual dataflows).
async fn transports(
    ids: &[String],
    shared: bool,
) -> (
    HashMap<String, Transport>,
    Vec<Arc<HashMap<String, SharedPeerManager>>>,
) {
    let capacity = 1024;
    let mut out = HashMap::new();
    let mut managers = Vec::new();
    if !shared {
        let mut conns: HashMap<String, Vec<PeerConnection<OwnedReadHalf, OwnedWriteHalf>>> =
            ids.iter().map(|i| (i.clone(), Vec::new())).collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let (si, sj) = tcp_pair().await;
                let (ri, wi) = si.into_split();
                let (rj, wj) = sj.into_split();
                conns.get_mut(&ids[i]).unwrap().push(PeerConnection {
                    node_id: ids[j].clone(),
                    reader: ri,
                    writer: wi,
                });
                conns.get_mut(&ids[j]).unwrap().push(PeerConnection {
                    node_id: ids[i].clone(),
                    reader: rj,
                    writer: wj,
                });
            }
        }
        for (k, v) in conns {
            out.insert(k, ClusterSpawnTransport::dedicated(v, capacity));
        }
    } else {
        let handle = tokio::runtime::Handle::current();
        let mut per: HashMap<String, HashMap<String, SharedPeerManager>> =
            ids.iter().map(|i| (i.clone(), HashMap::new())).collect();
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let (si, sj) = tcp_pair().await;
                let fi: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory {
                    connections: Mutex::new(VecDeque::from([si.into_split()])),
                });
                let fj: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory {
                    connections: Mutex::new(VecDeque::from([sj.into_split()])),
                });
                let mi = SharedPeerManager::new(
                    ids[j].clone(),
                    shared_config(),
                    fi,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap();
                let mj = SharedPeerManager::new(
                    ids[i].clone(),
                    shared_config(),
                    fj,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap();
                per.get_mut(&ids[i]).unwrap().insert(ids[j].clone(), mi);
                per.get_mut(&ids[j]).unwrap().insert(ids[i].clone(), mj);
            }
        }
        for (k, v) in per {
            let v = Arc::new(v);
            managers.push(Arc::clone(&v));
            out.insert(
                k,
                ClusterSpawnTransport::Shared {
                    peer_managers: v,
                    capacity,
                },
            );
        }
    }
    (out, managers)
}

/// Global worker that produced `record` (see `run`).
fn producer(record: &u64) -> u64 {
    record >> 32
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// `input -> map -> exchange(to global worker 0) -> map -> output`
    ToWorkerZero,
    /// Two exchanges: node "a" records first bounce between workers 0 and 1,
    /// remote records go straight to worker 0; then everything goes to
    /// worker 0.
    Chain,
}

fn build(b: &mut DataflowBuilder<u64>, shape: Shape) -> Result<()> {
    let s = b.input::<u64>("data")?.map("m0", |_t, x| x);
    let s = match shape {
        Shape::ToWorkerZero => s,
        Shape::Chain => s
            .exchange_by_hash("hop1", |x: &u64| {
                if producer(x) < NODE_A_WORKERS {
                    x & 1
                } else {
                    0
                }
            })
            .map("m1", |_t, x| x),
    };
    s.exchange_by_hash("to-w0", |_x: &u64| 0)
        .map("m2", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// How a test run ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Finish {
    /// Keep every input open until worker 0 has received every record, then
    /// cancel the dataflow. With inputs open, no executor can declare the
    /// dataflow complete, so this checks only that the burst drains.
    Delivery,
    /// Close every input and wait for every node to complete.
    Join,
}

/// Spawns the dataflow on every node of `spec` (`(node id, workers)`, node
/// "a" first with `NODE_A_WORKERS` workers), feeds single-record batches
/// spread over `epochs` epochs, and checks that every record reaches worker 0
/// exactly once.
async fn run(spec: &[(&str, usize)], shared: bool, shape: Shape, epochs: u64, finish: Finish) {
    assert_eq!(spec[0], ("a", NODE_A_WORKERS as usize));
    let what = format!("{shape:?} shared={shared} spec={spec:?} {finish:?}");
    let ids: Vec<String> = spec.iter().map(|(n, _)| n.to_string()).collect();
    let topo =
        ClusterTopology::multi_node(spec.iter().map(|(n, w)| NodeConfig::new(*n, *w)).collect())
            .unwrap();
    let (mut ts, managers) = transports(&ids, shared).await;
    let id = DataflowId::new();
    let th = tokio::runtime::Handle::current();
    let mut spawns = Vec::new();
    for node in &ids {
        let node = node.clone();
        let t = ts.remove(&node).unwrap();
        let topo = topo.clone();
        let th = th.clone();
        spawns.push(tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..Default::default()
            })
            .unwrap();
            let c: ClusterSpawnedDataflow<u64> = rt
                .spawn_cluster(
                    "cluster-exchange-burst",
                    topo,
                    &node,
                    id,
                    t,
                    Duration::from_secs(30),
                    move |b: &mut DataflowBuilder<u64>| build(b, shape),
                    &th,
                    SpawnOptions::new(),
                )
                .unwrap();
            (rt, c)
        }));
    }
    let mut nodes = Vec::new();
    for s in spawns {
        nodes.push(s.await.unwrap());
    }

    let records_for = |g: u64| {
        if g < NODE_A_WORKERS {
            LOCAL_RECORDS
        } else {
            REMOTE_RECORDS
        }
    };
    let mut expected = Vec::new();
    for (_, c) in &nodes {
        let (start, end) = c.local_worker_range();
        for g in start as u64..end as u64 {
            expected.extend((0..records_for(g)).map(|i| (g << 32) | i));
        }
    }
    expected.sort_unstable();
    let expected_len = expected.len();

    let mut collectors = Vec::new();
    let mut other_outputs = Vec::new();
    let mut feeders = Vec::new();
    for (_, c) in nodes.iter_mut() {
        let (start, _) = c.local_worker_range();
        for l in 0..c.num_local_workers() {
            let g = (start + l) as u64;
            let rx = c.take_output::<u64>(l, "out").unwrap();
            if g == 0 || finish == Finish::Join {
                collectors.push(std::thread::spawn(move || {
                    let mut got = Vec::new();
                    if finish == Finish::Join {
                        got.extend(rx.collect_data().into_iter().flat_map(|(_, d)| d));
                    } else {
                        let deadline = Instant::now() + TIMEOUT;
                        while got.len() < expected_len && Instant::now() < deadline {
                            if let Some(OutputEvent::Data { data, .. }) =
                                rx.recv_timeout(Duration::from_millis(100))
                            {
                                got.extend(data);
                            }
                        }
                    }
                    got.sort_unstable();
                    (g, got, rx)
                }));
            } else {
                other_outputs.push((g, rx));
            }
            let tx = c.take_input::<u64>(l, "data").unwrap();
            let records = records_for(g);
            let per_epoch = records.div_ceil(epochs);
            feeders.push(std::thread::spawn(move || {
                for i in 0..records {
                    let epoch = i / per_epoch;
                    tx.send(epoch, vec![(g << 32) | i]).unwrap();
                    if (i + 1) % per_epoch == 0 {
                        tx.advance_to(epoch + 1).unwrap();
                    }
                }
                match finish {
                    Finish::Delivery => Some(tx),
                    Finish::Join => None,
                }
            }));
        }
    }

    // Keep every node's completion handle (and with it its transport) until
    // all nodes are done, so a node that finishes first does not close its
    // connections while its peers are still running.
    let mut rts = Vec::new();
    let mut handles = Vec::new();
    let mut completions = Vec::new();
    for (rt, c) in nodes {
        match finish {
            Finish::Delivery => handles.push(c),
            Finish::Join => completions.push(Box::pin(c.join().unwrap())),
        }
        rts.push(rt);
    }
    let all = async {
        for c in completions.iter_mut() {
            c.as_mut().await.unwrap();
        }
    };
    if tokio::time::timeout(TIMEOUT, all).await.is_err() {
        panic!("{what}: cluster did not complete within {TIMEOUT:?} (livelock?)");
    }

    // Check the outputs before joining the feeders: if the dataflow is stuck,
    // the feeders are blocked on full input channels.
    let mut outputs = Vec::new();
    for c in collectors {
        let (g, got, rx) = c.join().unwrap();
        if g == 0 {
            assert_eq!(got.len(), expected_len, "{what}: records at worker 0");
            assert!(got == expected, "{what}: records lost or duplicated");
        } else {
            assert!(
                got.is_empty(),
                "{what}: worker {g} got {} records",
                got.len()
            );
        }
        outputs.push(rx);
    }
    for (g, rx) in &other_outputs {
        while let Some(event) = rx.try_recv() {
            assert!(
                !matches!(event, OutputEvent::Data { .. }),
                "{what}: worker {g} got records"
            );
        }
    }
    let open_inputs: Vec<_> = feeders.into_iter().map(|f| f.join().unwrap()).collect();

    // Dropping the handles cancels any dataflow that is still running.
    drop(handles);
    drop(completions);
    drop(open_inputs);
    drop(outputs);
    drop(other_outputs);
    drop(managers);
    tokio::task::spawn_blocking(move || drop(rts))
        .await
        .unwrap();
}

const TWO_NODES: &[(&str, usize)] = &[("a", 2), ("b", 2)];
const THREE_NODES: &[(&str, usize)] = &[("a", 2), ("b", 1), ("c", 2)];

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dedicated_two_nodes_burst_to_worker_zero() {
    run(TWO_NODES, false, Shape::ToWorkerZero, 1, Finish::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_two_nodes_burst_to_worker_zero() {
    run(TWO_NODES, true, Shape::ToWorkerZero, 1, Finish::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dedicated_three_nodes_burst_to_worker_zero_over_epochs() {
    run(THREE_NODES, false, Shape::ToWorkerZero, 8, Finish::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_three_nodes_burst_to_worker_zero_over_epochs() {
    run(THREE_NODES, true, Shape::ToWorkerZero, 8, Finish::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dedicated_three_nodes_exchange_chain() {
    run(THREE_NODES, false, Shape::Chain, 1, Finish::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_three_nodes_exchange_chain() {
    run(THREE_NODES, true, Shape::Chain, 1, Finish::Delivery).await;
}

// The tests below also close the inputs and wait for the whole cluster to
// complete. They fail intermittently (a few percent of runs under load) due
// to cluster completion bugs that are independent of stage-task scheduling
// and are tracked separately:
// - Completion is decided from the signed sum of capability and in-flight
//   counts, so a consumer's in-flight `-n` that is seen before the producer's
//   `+n` can make a worker declare completion early and drop records.
// - The node that finishes first cancels its progress bridges while its
//   peers may still need its final progress frames, so the peers hang.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "intermittent: early completion and first-finisher teardown in clusters; see comment"]
async fn dedicated_two_nodes_burst_to_worker_zero_then_complete() {
    run(TWO_NODES, false, Shape::ToWorkerZero, 1, Finish::Join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "intermittent: early completion and first-finisher teardown in clusters; see comment"]
async fn shared_two_nodes_burst_to_worker_zero_then_complete() {
    run(TWO_NODES, true, Shape::ToWorkerZero, 1, Finish::Join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "intermittent: early completion and first-finisher teardown in clusters; see comment"]
async fn dedicated_three_nodes_exchange_chain_then_complete() {
    run(THREE_NODES, false, Shape::Chain, 1, Finish::Join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "intermittent: early completion and first-finisher teardown in clusters; see comment"]
async fn shared_three_nodes_exchange_chain_then_complete() {
    run(THREE_NODES, true, Shape::Chain, 1, Finish::Join).await;
}
