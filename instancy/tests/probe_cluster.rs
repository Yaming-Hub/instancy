//! Regression tests: probes in a two-node loopback-TCP cluster (dedicated and
//! shared transport) are monotone, conservative and live.
//!
//! Each node builds the dataflow once per local worker, so every worker owns a
//! probe. Its view is the global frontier of the probed stream (progress is
//! exchanged across nodes), which lags differently on every worker. A probe
//! must never move backwards (including while peers' initial capabilities are
//! still in flight at startup), never report a time complete while data at
//! that time still arrives on the stream, and reach the inputs' frontier.

#![cfg(feature = "transport")]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::progress::frontier::Antichain;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, ProbeHandle, Result, RuntimeConfig,
    RuntimeHandle, SpawnOptions,
};

type Reader = tokio::net::tcp::OwnedReadHalf;
type Writer = tokio::net::tcp::OwnedWriteHalf;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const EPOCHS: u64 = 40;
const WORKERS_PER_NODE: usize = 2;

async fn tcp_pair() -> ((Reader, Writer), (Reader, Writer)) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.0.into_split(), connected.into_split())
}

struct PreEstablishedFactory {
    connections: Mutex<VecDeque<(Reader, Writer)>>,
}

impl ConnectionFactory for PreEstablishedFactory {
    type Reader = Reader;
    type Writer = Writer;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Reader, Writer), Box<dyn std::error::Error + Send + Sync>> {
        self.connections
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "no more pre-established connections".into())
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
        reorder_timeout: Duration::from_secs(10),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    }
}

enum Transport {
    Dedicated(Vec<PeerConnection<Reader, Writer>>),
    Shared(Arc<HashMap<String, SharedPeerManager>>),
}

async fn transports(shared: bool) -> (Transport, Transport) {
    let ((ra, wa), (rb, wb)) = tcp_pair().await;
    if !shared {
        return (
            Transport::Dedicated(vec![PeerConnection {
                node_id: "node-b".into(),
                reader: ra,
                writer: wa,
            }]),
            Transport::Dedicated(vec![PeerConnection {
                node_id: "node-a".into(),
                reader: rb,
                writer: wb,
            }]),
        );
    }
    let handle = tokio::runtime::Handle::current();
    let health = || tokio::sync::broadcast::channel(16).0;
    let factory_a: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory {
        connections: Mutex::new(VecDeque::from([(ra, wa)])),
    });
    let factory_b: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory {
        connections: Mutex::new(VecDeque::from([(rb, wb)])),
    });
    let manager_a = SharedPeerManager::new(
        "node-b".into(),
        shared_config(),
        factory_a,
        &handle,
        health(),
    )
    .unwrap();
    let manager_b = SharedPeerManager::new(
        "node-a".into(),
        shared_config(),
        factory_b,
        &handle,
        health(),
    )
    .unwrap();
    (
        Transport::Shared(Arc::new(HashMap::from([("node-b".to_string(), manager_a)]))),
        Transport::Shared(Arc::new(HashMap::from([("node-a".to_string(), manager_b)]))),
    )
}

fn dominates(old: &Antichain<u64>, new: &Antichain<u64>) -> bool {
    new.elements().iter().all(|t| old.less_equal(t))
}

/// Polls a probe until stopped; returns the observed frontiers.
fn watch(
    probe: ProbeHandle<u64>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Vec<Antichain<u64>>> {
    std::thread::spawn(move || {
        let mut seen = vec![probe.frontier()];
        loop {
            let f = probe.frontier();
            if seen.last() != Some(&f) {
                seen.push(f);
            }
            if stop.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        seen
    })
}

struct Probes {
    probes: Mutex<Vec<(String, ProbeHandle<u64>)>>,
    violations: Mutex<Vec<String>>,
}

fn build(
    node: &'static str,
    state: Arc<Probes>,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> {
    move |b: &mut DataflowBuilder<u64>| {
        let stream = b
            .input::<u64>("data")?
            .map("inc", |_t, x| x + 1)
            .exchange("by-value", |x: &u64| *x)
            .map("id", |_t, x| x);
        let (stream, probe) = checked_probe(stream, node, "map", &state);
        stream.clone().output("out")?;
        // A capability-holding operator after the exchange: its probe must
        // wait until the per-epoch sum has been emitted.
        let sums = stream.reduce("sum", |a, b| a + b);
        let (sums, sum_probe) = checked_probe(sums, node, "reduce", &state);
        sums.output("sums")?;
        let mut probes = state.probes.lock().unwrap();
        let idx = probes.len() / 2;
        probes.push((format!("{node}#{idx}/map"), probe));
        probes.push((format!("{node}#{idx}/reduce"), sum_probe));
        Ok(())
    }
}

fn checked_probe(
    stream: instancy::Pipe<u64, u64>,
    node: &'static str,
    what: &'static str,
    state: &Arc<Probes>,
) -> (instancy::Pipe<u64, u64>, ProbeHandle<u64>) {
    let (stream, probe) = stream.probe();
    let check = probe.clone();
    let st = Arc::clone(state);
    let stream = stream.inspect_batch(format!("check-{what}"), move |t: &u64, _d: &[u64]| {
        if check.done_with(t) {
            st.violations.lock().unwrap().push(format!(
                "{node}/{what}: data at {t} after probe reported done_with({t}) (frontier {:?})",
                check.frontier().elements()
            ));
        }
    });
    (stream, probe)
}

fn spawn_node(
    node: &'static str,
    topology: ClusterTopology,
    dataflow_id: DataflowId,
    transport: Transport,
    state: Arc<Probes>,
    handle: tokio::runtime::Handle,
) -> tokio::task::JoinHandle<
    Result<(
        RuntimeHandle,
        instancy::runtime::ClusterSpawnedDataflow<u64>,
    )>,
> {
    tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })?;
        let build = build(node, state);
        let timeout = Duration::from_secs(10);
        let cluster = match transport {
            Transport::Dedicated(conns) => rt.spawn_cluster(
                "probe-cluster",
                topology,
                node,
                dataflow_id,
                ClusterSpawnTransport::dedicated(conns, 1024),
                timeout,
                build,
                &handle,
                SpawnOptions::new(),
            )?,
            Transport::Shared(managers) => rt.spawn_cluster(
                "probe-cluster",
                topology,
                node,
                dataflow_id,
                ClusterSpawnTransport::shared(managers, 1024),
                timeout,
                build,
                &handle,
                SpawnOptions::new(),
            )?,
        };
        Ok((rt, cluster))
    })
}

async fn run_cluster(shared: bool, round: u64) {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", WORKERS_PER_NODE),
        NodeConfig::new("node-b", WORKERS_PER_NODE),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let (ta, tb) = transports(shared).await;
    let state = Arc::new(Probes {
        probes: Mutex::new(Vec::new()),
        violations: Mutex::new(Vec::new()),
    });
    let handle = tokio::runtime::Handle::current();
    let ha = spawn_node(
        "node-a",
        topology.clone(),
        dataflow_id,
        ta,
        Arc::clone(&state),
        handle.clone(),
    );
    let hb = spawn_node(
        "node-b",
        topology,
        dataflow_id,
        tb,
        Arc::clone(&state),
        handle,
    );
    let (ra, rb) = tokio::join!(ha, hb);
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();

    let probes: Vec<(String, ProbeHandle<u64>)> = state.probes.lock().unwrap().clone();
    assert_eq!(probes.len(), 2 * 2 * WORKERS_PER_NODE);
    let stop = Arc::new(AtomicBool::new(false));
    let watchers: Vec<_> = probes
        .iter()
        .map(|(name, p)| (name.clone(), watch(p.clone(), Arc::clone(&stop))))
        .collect();

    let mut senders = Vec::new();
    let mut outputs = Vec::new();
    let mut sum_outputs = Vec::new();
    for w in 0..WORKERS_PER_NODE {
        senders.push(ca.take_input::<u64>(w, "data").unwrap());
        senders.push(cb.take_input::<u64>(w, "data").unwrap());
        for cluster in [&mut ca, &mut cb] {
            let o = cluster.take_output::<u64>(w, "out").unwrap();
            outputs.push(std::thread::spawn(move || o.collect_data()));
            let s = cluster.take_output::<u64>(w, "sums").unwrap();
            sum_outputs.push(std::thread::spawn(move || s.collect_data()));
        }
    }
    let expected_sum: u64 = (0..senders.len() as u64)
        .flat_map(|i| {
            (0..EPOCHS).flat_map(move |e| (0..4).map(move |k| e * 10_000 + i * 100 + k + 1))
        })
        .sum();
    // Feed immediately (while peers' initial progress may still be in flight),
    // at different paces per worker.
    let feeders: Vec<_> = senders
        .into_iter()
        .enumerate()
        .map(|(i, tx)| {
            std::thread::spawn(move || {
                for e in 0..EPOCHS {
                    tx.send(e, (0..4).map(|k| e * 10_000 + i as u64 * 100 + k).collect())
                        .unwrap();
                    tx.advance_to(e + 1).unwrap();
                    if (e + i as u64 + round) % 3 == 0 {
                        std::thread::sleep(Duration::from_micros(500 * (i as u64 + 1)));
                    }
                }
                tx
            })
        })
        .collect();
    let senders: Vec<_> = feeders.into_iter().map(|f| f.join().unwrap()).collect();

    // Liveness: with every input at EPOCHS (and still open), every worker's
    // probe must reach EPOCHS - 1 without waiting for completion.
    let start = Instant::now();
    while !probes.iter().all(|(_, p)| p.done_with(&(EPOCHS - 1)))
        && start.elapsed() < Duration::from_secs(20)
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for (name, p) in &probes {
        assert!(
            p.done_with(&(EPOCHS - 1)),
            "shared={shared}: probe {name} stuck at {:?}",
            p.frontier().elements()
        );
        assert!(
            !p.done_with(&EPOCHS),
            "shared={shared}: probe {name} passed open inputs"
        );
    }
    drop(senders);

    for cluster in [ca, cb] {
        let joined = tokio::time::timeout(
            TEST_TIMEOUT,
            tokio::task::spawn_blocking(move || cluster.join_blocking()),
        )
        .await
        .expect("cluster did not complete")
        .unwrap();
        joined.unwrap();
    }
    let total: usize = outputs
        .into_iter()
        .map(|o| {
            o.join()
                .unwrap()
                .iter()
                .map(|(_, d)| d.len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(total, (EPOCHS as usize) * 4 * 2 * WORKERS_PER_NODE);
    let sums: Vec<(u64, Vec<u64>)> = sum_outputs
        .into_iter()
        .flat_map(|o| o.join().unwrap())
        .collect();
    assert_eq!(
        sums.iter().flat_map(|(_, d)| d).sum::<u64>(),
        expected_sum,
        "shared={shared}: per-epoch sums lost or duplicated data"
    );

    stop.store(true, Ordering::SeqCst);
    for (name, w) in watchers {
        let seen = w.join().unwrap();
        for pair in seen.windows(2) {
            assert!(
                dominates(&pair[0], &pair[1]),
                "shared={shared}: probe {name} moved backwards: {:?} -> {:?}",
                pair[0].elements(),
                pair[1].elements()
            );
        }
    }
    let violations = state.violations.lock().unwrap();
    assert!(
        violations.is_empty(),
        "shared={shared}: probes reported times complete too early: {violations:#?}"
    );
    drop((rt_a, rt_b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedicated_tcp_cluster_probes_are_monotone_conservative_and_live() {
    for round in 0..3 {
        run_cluster(false, round).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_tcp_cluster_probes_are_monotone_conservative_and_live() {
    for round in 0..3 {
        run_cluster(true, round).await;
    }
}
