//! Audit (netdata): cross-node broadcast correctness (partial delivery and
//! close-sentinel loss under network backpressure).

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::DataflowBuilder;
use instancy::DataflowId;
use instancy::Result;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{ClusterTopology, NodeConfig};
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

type Conn = PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

async fn make_tcp_connections(node_ids: &[&str]) -> HashMap<String, Vec<Conn>> {
    let mut result: HashMap<String, Vec<_>> = HashMap::new();
    for id in node_ids {
        result.insert(id.to_string(), Vec::new());
    }
    for i in 0..node_ids.len() {
        for j in (i + 1)..node_ids.len() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (accepted, connected) =
                tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
            let stream_i = accepted.0;
            let stream_j = connected;
            stream_i.set_nodelay(true).unwrap();
            stream_j.set_nodelay(true).unwrap();
            let (ri, wi) = stream_i.into_split();
            let (rj, wj) = stream_j.into_split();
            result.get_mut(node_ids[i]).unwrap().push(PeerConnection {
                node_id: node_ids[j].to_string(),
                reader: ri,
                writer: wi,
            });
            result.get_mut(node_ids[j]).unwrap().push(PeerConnection {
                node_id: node_ids[i].to_string(),
                reader: rj,
                writer: wj,
            });
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn spawn_node<F>(
    topology: ClusterTopology,
    node_id: &str,
    dataflow_id: DataflowId,
    connections: Vec<Conn>,
    worker_threads: usize,
    capacity: usize,
    tokio_handle: tokio::runtime::Handle,
    build: F,
) -> tokio::task::JoinHandle<
    Result<(
        RuntimeHandle,
        instancy::runtime::ClusterSpawnedDataflow<u64>,
    )>,
>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let node_id = node_id.to_string();
    tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "audit-netdata",
            topology,
            &node_id,
            dataflow_id,
            ClusterSpawnTransport::dedicated(connections, capacity),
            Duration::from_secs(10),
            build,
            &tokio_handle,
            SpawnOptions::new(),
        )?;
        Ok((rt, cluster))
    })
}

/// Join with timeout; returns Err(description) instead of panicking.
///
/// Uses a detached std thread so a hung join never blocks tokio runtime
/// shutdown (spawn_blocking tasks are awaited on runtime drop).
async fn join_timeout(
    cluster: instancy::runtime::ClusterSpawnedDataflow<u64>,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let r = cluster.join_blocking();
        let _ = tx.send(r);
    });
    tokio::task::spawn_blocking(move || match rx.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("join error: {e}")),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(format!("TIMEOUT after {timeout:?}"))
        }
        Err(e) => Err(format!("join thread died: {e}")),
    })
    .await
    .unwrap()
}

/// Spawn a std thread that drains an output receiver into a shared vec.
fn drain_output<D: Send + 'static>(
    out: instancy::OutputReceiver<u64, D>,
    sink: std::sync::Arc<std::sync::Mutex<Vec<D>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        loop {
            match out.recv_timeout(Duration::from_millis(50)) {
                Some(instancy::dataflow::OutputEvent::Data { data, .. }) => {
                    sink.lock().unwrap().extend(data);
                }
                Some(_) => {}
                None => {
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        // final non-blocking drain
                        while let Some(ev) = out.try_recv() {
                            if let instancy::dataflow::OutputEvent::Data { data, .. } = ev {
                                sink.lock().unwrap().extend(data);
                            }
                        }
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    })
}

struct BcastOutcome {
    join_a: std::result::Result<(), String>,
    join_b: std::result::Result<(), String>,
    per_sink: Vec<Vec<u64>>,
}

/// node-a (a_workers) + node-b (b_workers); node-a worker 0 sends `batches`
/// batches of `per_batch` distinct values through `broadcast`; every worker
/// should observe every value exactly once.
async fn run_broadcast(
    capacity: usize,
    a_workers: usize,
    b_workers: usize,
    batches: u64,
    per_batch: u64,
    timeout: Duration,
) -> BcastOutcome {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", a_workers),
        NodeConfig::new("node-b", b_workers),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();
        input
            .with_capacity(1 << 20)
            .unwrap()
            .broadcast("bcast")
            .map("pass", |_t, x| x)
            .output("results")
            .unwrap();
        Ok(())
    };

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        capacity,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        capacity,
        tokio_handle,
        build,
    );
    let (ra, rb) = tokio::join!(ha, hb);
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut sinks = Vec::new();
    for i in 0..a_workers {
        let s = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        drain_output(
            ca.take_output::<u64>(i, "results").unwrap(),
            s.clone(),
            stop.clone(),
        );
        sinks.push(s);
    }
    for i in 0..b_workers {
        let s = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        drain_output(
            cb.take_output::<u64>(i, "results").unwrap(),
            s.clone(),
            stop.clone(),
        );
        sinks.push(s);
    }

    let sa0 = ca.take_input::<u64>(0, "data").unwrap();
    std::thread::spawn(move || {
        for b in 0..batches {
            let base = b * per_batch;
            if sa0.send(0, (base..base + per_batch).collect()).is_err() {
                return;
            }
        }
        drop(sa0);
    });
    for i in 1..a_workers {
        drop(ca.take_input::<u64>(i, "data").unwrap());
    }
    for i in 0..b_workers {
        drop(cb.take_input::<u64>(i, "data").unwrap());
    }

    let (join_a, join_b) = tokio::join!(join_timeout(ca, timeout), join_timeout(cb, timeout));
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let per_sink = sinks.iter().map(|s| s.lock().unwrap().clone()).collect();
    std::mem::forget(rt_a);
    std::mem::forget(rt_b);
    BcastOutcome {
        join_a,
        join_b,
        per_sink,
    }
}

fn summarize(o: &BcastOutcome, expected_per_worker: usize) -> (Vec<usize>, Vec<usize>, bool) {
    let totals: Vec<usize> = o.per_sink.iter().map(|v| v.len()).collect();
    let distincts: Vec<usize> = o
        .per_sink
        .iter()
        .map(|v| {
            let mut v = v.clone();
            v.sort();
            v.dedup();
            v.len()
        })
        .collect();
    let ok = totals.iter().all(|&t| t == expected_per_worker)
        && distincts.iter().all(|&d| d == expected_per_worker);
    (totals, distincts, ok)
}

/// BroadcastPush::try_push has the same partial-delivery-then-return-whole-
/// envelope pattern as ExchangePush: a network `Backpressure` on target k
/// after targets 0..k accepted the batch makes the caller retry the whole
/// batch -> targets 0..k see it twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_partial_delivery_duplicates() {
    let o = run_broadcast(1024, 2, 1, 3000, 4, Duration::from_secs(60)).await;
    let (totals, distincts, ok) = summarize(&o, 3000 * 4);
    eprintln!(
        "broadcast cap=1024: join_a={:?} join_b={:?} totals={totals:?} distincts={distincts:?} expected_per_worker={}",
        o.join_a,
        o.join_b,
        3000 * 4
    );
    assert!(o.join_a.is_ok() && o.join_b.is_ok(), "join failed");
    assert!(ok, "broadcast delivered wrong multiset");
}

/// NetworkPush::close() sends the close sentinel with try_send; if the
/// per-peer queue is full the sentinel is dropped. The broadcast operator
/// only releases its T::minimum() capability once its input is exhausted, so
/// a lost sentinel can wedge completion cluster-wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_close_sentinel_loss_hangs() {
    let mut hangs = 0;
    let runs = 5;
    for run in 0..runs {
        // node-b has ONE worker: each batch has one remote target, so the
        // data itself cannot livelock; only the close sentinels can be lost.
        let o = run_broadcast(1, 2, 1, 1, 4, Duration::from_secs(15)).await;
        let (totals, distincts, _ok) = summarize(&o, 4);
        eprintln!(
            "run {run}: broadcast cap=1: join_a={:?} join_b={:?} totals={totals:?} distincts={distincts:?}",
            o.join_a, o.join_b
        );
        if o.join_a.is_err() || o.join_b.is_err() {
            hangs += 1;
        }
    }
    assert_eq!(hangs, 0, "{hangs}/{runs} runs failed to complete");
}
/// Control for the sentinel-loss test: same topology/data, generous capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_control_capacity_1024() {
    let o = run_broadcast(1024, 2, 1, 1, 4, Duration::from_secs(15)).await;
    let (totals, distincts, ok) = summarize(&o, 4);
    eprintln!(
        "control cap=1024: join_a={:?} join_b={:?} totals={totals:?} distincts={distincts:?}",
        o.join_a, o.join_b
    );
    assert!(o.join_a.is_ok() && o.join_b.is_ok(), "join failed");
    assert!(ok);
}
