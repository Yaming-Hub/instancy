//! Audit: minimization of premature epoch notification (late data after
//! `unary_notify` fired) observed in multi-node stress runs.

#![cfg(feature = "transport")]
#![allow(clippy::type_complexity, dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, MultiSpawnedDataflow, NodeConfig, Result,
    RuntimeConfig, RuntimeHandle, SpawnOptions,
};

type Conn = PeerConnection<OwnedReadHalf, OwnedWriteHalf>;

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.0, connected)
}

async fn conns2() -> (Conn, Conn) {
    let (a, b) = tcp_pair().await;
    let (ra, wa) = a.into_split();
    let (rb, wb) = b.into_split();
    (
        PeerConnection {
            node_id: "b".into(),
            reader: ra,
            writer: wa,
        },
        PeerConnection {
            node_id: "a".into(),
            reader: rb,
            writer: wb,
        },
    )
}

#[derive(Default)]
struct Late {
    late: Vec<(usize, u64, usize)>,
    fired: usize,
}

/// unary_notify that flags data arriving for an epoch after its notification fired.
fn checker(
    s: instancy::Pipe<u64, u64>,
    late: Arc<Mutex<Late>>,
    tag: usize,
) -> instancy::Pipe<u64, u64> {
    s.unary_notify("check", {
        let mut fired: HashSet<u64> = HashSet::new();
        let mut stash: HashMap<u64, u64> = HashMap::new();
        move |input, output, ctx| {
            while let Some((t, d)) = input.next() {
                if fired.contains(&t) {
                    late.lock().unwrap().late.push((tag, t, d.len()));
                }
                *stash.entry(t).or_default() += d.len() as u64;
                ctx.notify_at(t);
            }
            while let Some(t) = ctx.next_notification() {
                fired.insert(t);
                late.lock().unwrap().fired += 1;
                output.push_vec(t, vec![stash.remove(&t).unwrap_or(0)]);
            }
            Ok(())
        }
    })
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// input -> exchange -> check
    Exchange,
    /// input -> exchange -> map -> exchange -> check
    DoubleExchange,
    /// input -> map -> check (no exchange)
    NoExchange,
    /// input -> map -> exchange -> check
    MapThenExchange,
    /// input -> exchange -> exchange -> check
    ExchangeExchange,
}

fn build_shape(
    b: &mut DataflowBuilder<u64>,
    shape: Shape,
    late: Arc<Mutex<Late>>,
    tag: usize,
) -> Result<()> {
    let input = b.input::<u64>("data")?;
    let s = match shape {
        Shape::Exchange => input.exchange("ex", |x: &u64| *x),
        Shape::DoubleExchange => input
            .exchange("ex1", |x: &u64| *x)
            .map("m", |_t, x| x * 3 + 1)
            .exchange("ex2", |x: &u64| *x / 5),
        Shape::NoExchange => input.map("m", |_t, x| x + 1),
        Shape::MapThenExchange => input
            .map("m", |_t, x| x * 3 + 1)
            .exchange("ex", |x: &u64| *x / 5),
        Shape::ExchangeExchange => input
            .exchange("ex1", |x: &u64| *x)
            .exchange("ex2", |x: &u64| *x / 5),
    };
    checker(s, late, tag).output("out")?;
    Ok(())
}

fn feed(
    senders: Vec<instancy::InputSender<u64, u64>>,
    epochs: u64,
    per: u64,
    sleep: Duration,
) -> Vec<std::thread::JoinHandle<()>> {
    senders
        .into_iter()
        .enumerate()
        .map(|(w, s)| {
            std::thread::spawn(move || {
                for e in 0..epochs {
                    let batch: Vec<u64> =
                        (0..per).map(|i| (w as u64) << 40 | e << 20 | i).collect();
                    if s.send(e, batch).is_err() {
                        return;
                    }
                    if s.advance_to(e + 1).is_err() {
                        return;
                    }
                    if !sleep.is_zero() {
                        std::thread::sleep(sleep);
                    }
                }
            })
        })
        .collect()
}

fn drain(rx: instancy::OutputReceiver<u64, u64>) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        rx.collect_data()
            .into_iter()
            .map(|(_, d)| d.iter().sum::<u64>())
            .sum()
    })
}

async fn wait_threads<T>(hs: Vec<std::thread::JoinHandle<T>>) -> Vec<T> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut out = Vec::new();
    for h in hs {
        while !h.is_finished() {
            assert!(Instant::now() < deadline, "thread timeout");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        out.push(h.join().unwrap());
    }
    out
}

/// Single-process multi-worker variant.
async fn run_multi(workers: usize, shape: Shape, epochs: u64, per: u64, sleep: Duration) -> Late {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let late = Arc::new(Mutex::new(Late::default()));
    let l2 = Arc::clone(&late);
    let mut multi: MultiSpawnedDataflow<u64> = rt
        .spawn_multi(
            "late-multi",
            workers,
            move |b| build_shape(b, shape, Arc::clone(&l2), 0),
            SpawnOptions::new(),
        )
        .unwrap();
    let mut outs = Vec::new();
    let mut ins = Vec::new();
    for w in 0..workers {
        outs.push(drain(multi.take_output::<u64>(w, "out").unwrap()));
        ins.push(multi.take_input::<u64>(w, "data").unwrap());
    }
    let feeders = feed(ins, epochs, per, sleep);
    tokio::time::timeout(Duration::from_secs(60), multi.join())
        .await
        .expect("multi join timeout")
        .unwrap();
    wait_threads(feeders).await;
    let total: u64 = wait_threads(outs).await.into_iter().sum();
    assert_eq!(total, workers as u64 * epochs * per, "record count");
    Arc::try_unwrap(late).ok().unwrap().into_inner().unwrap()
}

/// Two-node dedicated cluster variant.
async fn run_cluster(
    workers_per_node: usize,
    shape: Shape,
    epochs: u64,
    per: u64,
    sleep: Duration,
) -> Late {
    let topo = ClusterTopology::multi_node(vec![
        NodeConfig::new("a", workers_per_node),
        NodeConfig::new("b", workers_per_node),
    ])
    .unwrap();
    let (ca, cb) = conns2().await;
    let late = Arc::new(Mutex::new(Late::default()));
    let id = DataflowId::new();
    let th = tokio::runtime::Handle::current();
    let mut hs = Vec::new();
    for (node, conn, tag) in [("a", ca, 0usize), ("b", cb, 1usize)] {
        let topo = topo.clone();
        let th = th.clone();
        let late = Arc::clone(&late);
        hs.push(tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..Default::default()
            })
            .unwrap();
            let c: ClusterSpawnedDataflow<u64> = rt
                .spawn_cluster(
                    "late-cluster",
                    topo,
                    node,
                    id,
                    ClusterSpawnTransport::dedicated(vec![conn], 1024),
                    Duration::from_secs(10),
                    move |b: &mut DataflowBuilder<u64>| {
                        build_shape(b, shape, Arc::clone(&late), tag)
                    },
                    &th,
                    SpawnOptions::new(),
                )
                .unwrap();
            (rt, c)
        }));
    }
    let mut rts = Vec::new();
    let mut clusters = Vec::new();
    for h in hs {
        let (rt, c) = h.await.unwrap();
        rts.push(rt);
        clusters.push(c);
    }
    let mut outs = Vec::new();
    let mut ins = Vec::new();
    for c in clusters.iter_mut() {
        for l in 0..c.num_local_workers() {
            outs.push(drain(c.take_output::<u64>(l, "out").unwrap()));
            ins.push(c.take_input::<u64>(l, "data").unwrap());
        }
    }
    let feeders = feed(ins, epochs, per, sleep);
    let mut comps = Vec::new();
    for c in clusters {
        comps.push(c.join().unwrap());
    }
    for c in comps {
        tokio::time::timeout(Duration::from_secs(60), c)
            .await
            .expect("cluster join timeout")
            .unwrap();
    }
    wait_threads(feeders).await;
    let total: u64 = wait_threads(outs).await.into_iter().sum();
    assert_eq!(
        total,
        2 * workers_per_node as u64 * epochs * per,
        "record count"
    );
    drop(rts);
    Arc::try_unwrap(late).ok().unwrap().into_inner().unwrap()
}

fn report(name: &str, l: &Late) -> bool {
    println!(
        "{name}: fired={} late_events={} first={:?}",
        l.fired,
        l.late.len(),
        &l.late[..l.late.len().min(5)]
    );
    !l.late.is_empty()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn late_matrix_more_shapes() {
    let sleep = Duration::from_millis(2);
    let mut any = Vec::new();
    for rep in 0..3 {
        for shape in [Shape::MapThenExchange, Shape::ExchangeExchange] {
            let l = run_multi(4, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} multi4 {shape:?}"), &l) {
                any.push(format!("multi4 {shape:?}"));
            }
            let l = run_cluster(1, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} cluster1x2 {shape:?}"), &l) {
                any.push(format!("cluster1x2 {shape:?}"));
            }
            let l = run_cluster(2, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} cluster2x2 {shape:?}"), &l) {
                any.push(format!("cluster2x2 {shape:?}"));
            }
        }
    }
    assert!(any.is_empty(), "late data observed in: {any:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn late_matrix() {
    let sleep = Duration::from_millis(2);
    let mut any = Vec::new();
    for rep in 0..3 {
        for shape in [Shape::NoExchange, Shape::Exchange, Shape::DoubleExchange] {
            let l = run_multi(4, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} multi4 {shape:?}"), &l) {
                any.push(format!("multi4 {shape:?}"));
            }
            let l = run_multi(1, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} multi1 {shape:?}"), &l) {
                any.push(format!("multi1 {shape:?}"));
            }
            let l = run_cluster(1, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} cluster1x2 {shape:?}"), &l) {
                any.push(format!("cluster1x2 {shape:?}"));
            }
            let l = run_cluster(2, shape, 60, 5, sleep).await;
            if report(&format!("rep{rep} cluster2x2 {shape:?}"), &l) {
                any.push(format!("cluster2x2 {shape:?}"));
            }
        }
    }
    assert!(any.is_empty(), "late data observed in: {any:?}");
}
