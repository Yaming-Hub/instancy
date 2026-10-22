//! Audit (netdata): frontier vs. in-flight data across nodes (input -> exchange
//! edge has no in-flight +n accounting).

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

type Counts = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<u64, usize>>>;

fn make_build(
    counts: Counts,
    probes: std::sync::Arc<std::sync::Mutex<Vec<instancy::ProbeHandle<u64>>>>,
    via_map: bool,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + Sync + 'static {
    move |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();
        // Optionally insert a map before the exchange: unary operators DO wire
        // the in-flight +n reporter, `input()` does not.
        let pre = if via_map {
            input.map("pre", |_t, x| x)
        } else {
            input
        };
        let counts = counts.clone();
        let (s, probe) = pre
            .exchange_by_hash("to_w0", |_x: &u64| 0)
            .inspect_batch("count", move |t, batch| {
                *counts.lock().unwrap().entry(*t).or_insert(0) += batch.len();
            })
            .probe();
        probes.lock().unwrap().push(probe);
        s.output("results").unwrap();
        Ok(())
    }
}

/// node-b streams epochs to worker 0 on node-a (`input -> exchange`), advancing
/// its input frontier after each epoch. A watcher on node-a records, for each
/// epoch e, how many epoch-e records had been observed at the moment the probe
/// first reported `done_with(e)`. Any shortfall means the frontier passed e
/// while epoch-e data was still in flight across the network.
async fn run_frontier_race(
    via_map: bool,
    epochs: u64,
    per_epoch: usize,
    capacity: usize,
) -> (usize, Vec<(u64, usize)>) {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let counts_a: Counts = Default::default();
    let probes_a = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let counts_b: Counts = Default::default();
    let probes_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        2,
        capacity,
        tokio_handle.clone(),
        make_build(counts_a.clone(), probes_a.clone(), via_map),
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        capacity,
        tokio_handle,
        make_build(counts_b, probes_b, via_map),
    );
    let (ra, rb) = tokio::join!(ha, hb);
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();
    let probe_a = probes_a.lock().unwrap()[0].clone();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sink_a = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let sink_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    drain_output(
        ca.take_output::<u64>(0, "results").unwrap(),
        sink_a,
        stop.clone(),
    );
    drain_output(
        cb.take_output::<u64>(0, "results").unwrap(),
        sink_b,
        stop.clone(),
    );

    drop(ca.take_input::<u64>(0, "data").unwrap());
    let sb = cb.take_input::<u64>(0, "data").unwrap();
    std::thread::spawn(move || {
        for e in 0..epochs {
            if sb.send(e, (0..per_epoch as u64).collect()).is_err() {
                return;
            }
            if sb.advance_to(e + 1).is_err() {
                return;
            }
        }
        drop(sb);
    });

    // Watcher.
    let counts_w = counts_a.clone();
    let watcher = std::thread::spawn(move || {
        let mut violations = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        for e in 0..epochs {
            while !probe_a.done_with(&e) {
                if std::time::Instant::now() > deadline {
                    return (violations, false);
                }
                std::hint::spin_loop();
            }
            let seen = counts_w.lock().unwrap().get(&e).copied().unwrap_or(0);
            if seen < per_epoch {
                violations.push((e, seen));
            }
        }
        (violations, true)
    });

    let (violations, finished) = tokio::task::spawn_blocking(move || watcher.join().unwrap())
        .await
        .unwrap();
    let (ja, jb) = tokio::join!(
        join_timeout(ca, Duration::from_secs(30)),
        join_timeout(cb, Duration::from_secs(30))
    );
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let total: usize = counts_a.lock().unwrap().values().sum();
    eprintln!(
        "via_map={via_map} cap={capacity}: watcher_finished={finished} join_a={ja:?} join_b={jb:?} total_counted={total} expected={} violations={} first={:?}",
        epochs as usize * per_epoch,
        violations.len(),
        violations.iter().take(5).collect::<Vec<_>>()
    );
    std::mem::forget(rt_a);
    std::mem::forget(rt_b);
    (violations.len(), violations)
}

/// `input -> exchange`: no in-flight +n is recorded by the input operator's
/// ExchangePush, so remote data in transit does not hold the frontier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn frontier_passes_epoch_while_remote_data_in_flight_input_exchange() {
    let (n, _v) = run_frontier_race(false, 3000, 4, 1024).await;
    assert_eq!(
        n, 0,
        "frontier advanced past an epoch before its remote data arrived"
    );
}

/// Control: `input -> map -> exchange` (map wires the in-flight reporter).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn frontier_control_map_exchange() {
    let (n, _v) = run_frontier_race(true, 3000, 4, 1024).await;
    assert_eq!(
        n, 0,
        "frontier advanced past an epoch before its remote data arrived"
    );
}
/// Diagnostic: same as the control but with a huge transport queue, so the
/// map's ExchangePush never sees network Backpressure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn frontier_map_exchange_huge_transport_capacity() {
    let (n, _v) = run_frontier_race(true, 3000, 4, 1 << 16).await;
    assert_eq!(
        n, 0,
        "frontier advanced past an epoch before its remote data arrived"
    );
}
/// Repeat the map->exchange scenario several times (race is timing dependent).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn frontier_map_exchange_repeated() {
    let mut total = 0;
    for _ in 0..6 {
        let (n, _v) = run_frontier_race(true, 3000, 4, 1024).await;
        total += n;
    }
    assert_eq!(
        total, 0,
        "frontier advanced past epochs before their remote data arrived"
    );
}

/// Repeat the input->exchange scenario several times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn frontier_input_exchange_repeated() {
    let mut total = 0;
    for _ in 0..6 {
        let (n, _v) = run_frontier_race(false, 3000, 4, 1024).await;
        total += n;
    }
    assert_eq!(
        total, 0,
        "frontier advanced past epochs before their remote data arrived"
    );
}
/// Wrong *output* consequence: `count` (notification-based, per timestamp)
/// downstream of `input -> map -> exchange` across nodes. Each epoch is sent
/// as two batches of 2 records; every epoch must yield exactly one count of 4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "expected failure (C1, intermittent); see EXPECTED in tests/audit_repros.rs"]
async fn count_per_epoch_wrong_when_frontier_overtakes_remote_data() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let tokio_handle = tokio::runtime::Handle::current();

    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = builder.input::<u64>("data").unwrap();
        input
            .map("pre", |_t, x| x)
            .exchange_by_hash("to_w0", |_x: &u64| 0)
            .count("cnt")
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
        1024,
        tokio_handle.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        2,
        1024,
        tokio_handle,
        build,
    );
    let (ra, rb) = tokio::join!(ha, hb);
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();

    // Collect (time, count) pairs from node-a's output.
    let out_a = ca.take_output::<usize>(0, "results").unwrap();
    let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(u64, usize)>::new()));
    let col2 = collected.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    std::thread::spawn(move || {
        loop {
            match out_a.recv_timeout(Duration::from_millis(50)) {
                Some(instancy::dataflow::OutputEvent::Data { time, data }) => {
                    let mut g = col2.lock().unwrap();
                    for d in data {
                        g.push((time, d));
                    }
                }
                Some(_) => {}
                None => {
                    if stop2.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    });
    let sink_b = std::sync::Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
    drain_output(
        cb.take_output::<usize>(0, "results").unwrap(),
        sink_b,
        stop.clone(),
    );

    const EPOCHS: u64 = 3000;
    drop(ca.take_input::<u64>(0, "data").unwrap());
    let sb = cb.take_input::<u64>(0, "data").unwrap();
    std::thread::spawn(move || {
        for e in 0..EPOCHS {
            if sb.send(e, vec![1, 2]).is_err() || sb.send(e, vec![3, 4]).is_err() {
                return;
            }
            if sb.advance_to(e + 1).is_err() {
                return;
            }
        }
        drop(sb);
    });

    let (ja, jb) = tokio::join!(
        join_timeout(ca, Duration::from_secs(60)),
        join_timeout(cb, Duration::from_secs(60))
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let got = collected.lock().unwrap().clone();
    let mut per_epoch: std::collections::BTreeMap<u64, Vec<usize>> = Default::default();
    for (t, n) in &got {
        per_epoch.entry(*t).or_default().push(*n);
    }
    let bad: Vec<_> = per_epoch
        .iter()
        .filter(|(_, v)| v.as_slice() != [4])
        .take(8)
        .collect();
    let n_bad = per_epoch.values().filter(|v| v.as_slice() != [4]).count();
    let missing = EPOCHS as usize - per_epoch.len();
    eprintln!(
        "count: join_a={ja:?} join_b={jb:?} outputs={} epochs_with_output={} missing_epochs={missing} wrong_epochs={n_bad} sample={bad:?}",
        got.len(),
        per_epoch.len()
    );
    std::mem::forget(rt_a);
    std::mem::forget(rt_b);
    assert!(ja.is_ok() && jb.is_ok());
    assert_eq!(missing, 0, "epochs without a count");
    assert_eq!(n_bad, 0, "epochs with wrong/split counts");
}
