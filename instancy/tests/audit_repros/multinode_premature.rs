//! Audit: premature completion / silent data loss caused by in-flight
//! (exchange) message counts cancelling out held capabilities in the
//! progress tracker's completion check.
//!
//! Scenario: worker/node A keeps its input open and trickles records that the
//! exchange routes to worker/node B, whose own input is closed at once. B must
//! receive every record. The tests are deterministic in practice: B pulls the
//! first record (in-flight `-1`) while A still holds its input capability
//! (`+1`).

#![cfg(feature = "transport")]
#![allow(clippy::type_complexity, dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

const RECORDS: u64 = 10;
const GAP: Duration = Duration::from_millis(30);

fn build(b: &mut DataflowBuilder<u64>, via_map: bool, received: Arc<AtomicU64>) -> Result<()> {
    let input = b.input::<u64>("data")?;
    let input = if via_map {
        input.map("id", |_t, x| x)
    } else {
        input
    };
    input
        // every record goes to global worker 1
        .exchange_by_hash("to-w1", |_x: &u64| 1)
        .inspect("seen", move |_t, _x| {
            received.fetch_add(1, Ordering::SeqCst);
        })
        .output("out")?;
    Ok(())
}

fn trickle(
    s: instancy::InputSender<u64, u64>,
) -> std::thread::JoinHandle<std::result::Result<(), String>> {
    std::thread::spawn(move || {
        for k in 0..RECORDS {
            s.send(0, vec![k]).map_err(|e| format!("send {k}: {e}"))?;
            std::thread::sleep(GAP);
        }
        Ok(())
    })
}

/// Two-node cluster (dedicated TCP), one worker per node.
async fn cluster_case(via_map: bool) -> String {
    let topo = ClusterTopology::multi_node(vec![NodeConfig::new("a", 1), NodeConfig::new("b", 1)])
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (acc, con) = tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    let (ra, wa) = acc.0.into_split();
    let (rb, wb) = con.into_split();
    let id = DataflowId::new();
    let th = tokio::runtime::Handle::current();
    let received = Arc::new(AtomicU64::new(0));
    let mut hs = Vec::new();
    for (node, conn) in [
        (
            "a",
            PeerConnection {
                node_id: "b".to_string(),
                reader: ra,
                writer: wa,
            },
        ),
        (
            "b",
            PeerConnection {
                node_id: "a".to_string(),
                reader: rb,
                writer: wb,
            },
        ),
    ] {
        let topo = topo.clone();
        let th = th.clone();
        let received = Arc::clone(&received);
        hs.push(tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 1,
                ..Default::default()
            })
            .unwrap();
            let c = rt
                .spawn_cluster(
                    "premature",
                    topo,
                    node,
                    id,
                    ClusterSpawnTransport::dedicated(vec![conn], 1024),
                    Duration::from_secs(10),
                    move |b: &mut DataflowBuilder<u64>| build(b, via_map, Arc::clone(&received)),
                    &th,
                    SpawnOptions::new(),
                )
                .unwrap();
            (rt, c)
        }));
    }
    let (rt_a, mut ca) = hs.remove(0).await.unwrap();
    let (rt_b, mut cb) = hs.remove(0).await.unwrap();
    let oa = ca.take_output::<u64>(0, "out").unwrap();
    let ob = cb.take_output::<u64>(0, "out").unwrap();
    let ta = std::thread::spawn(move || oa.collect_data());
    let tb = std::thread::spawn(move || ob.collect_data());
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let feeder = trickle(ca.take_input::<u64>(0, "data").unwrap());
    let jb = tokio::spawn(cb.join().unwrap());
    let ja = tokio::spawn(ca.join().unwrap());
    let rb = tokio::time::timeout(Duration::from_secs(20), jb).await;
    let rb_done_at = received.load(Ordering::SeqCst);
    let ra = tokio::time::timeout(Duration::from_secs(20), ja).await;
    let fed = feeder.join().unwrap();
    let out_b: usize = tb.join().unwrap().iter().map(|(_, d)| d.len()).sum();
    let out_a: usize = ta.join().unwrap().iter().map(|(_, d)| d.len()).sum();
    drop((rt_a, rt_b));
    format!(
        "node b result={:?} (records seen by b when it finished: {rb_done_at}); node a result={:?}; feeder={fed:?}; \
         output records a={out_a} b={out_b} (expected b={RECORDS})",
        rb.map(|r| r.unwrap().map_err(|e| e.to_string())),
        ra.map(|r| r.unwrap().map_err(|e| e.to_string())),
    )
}

/// Same, single process, two workers (`spawn_multi`).
async fn multi_case(via_map: bool) -> String {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let received = Arc::new(AtomicU64::new(0));
    let r2 = Arc::clone(&received);
    let mut m = rt
        .spawn_multi(
            "premature-multi",
            2,
            move |b: &mut DataflowBuilder<u64>| build(b, via_map, Arc::clone(&r2)),
            SpawnOptions::new(),
        )
        .unwrap();
    let o0 = m.take_output::<u64>(0, "out").unwrap();
    let o1 = m.take_output::<u64>(1, "out").unwrap();
    let t0 = std::thread::spawn(move || o0.collect_data());
    let t1 = std::thread::spawn(move || o1.collect_data());
    drop(m.take_input::<u64>(1, "data").unwrap());
    let feeder = trickle(m.take_input::<u64>(0, "data").unwrap());
    let start = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(20), m.join()).await;
    let fed = feeder.join().unwrap();
    let out: usize = t0
        .join()
        .unwrap()
        .into_iter()
        .chain(t1.join().unwrap())
        .map(|(_, d)| d.len())
        .sum();
    format!(
        "result={:?} after {:?}; feeder={fed:?}; output records={out} (expected {RECORDS}); seen={}",
        r.map(|r| r.map_err(|e| e.to_string())),
        start.elapsed(),
        received.load(Ordering::SeqCst)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "expected failure (C1/C2); see EXPECTED in tests/audit_repros.rs"]
async fn cluster_input_exchange_trickle_loses_data() {
    let s = cluster_case(false).await;
    println!("{s}");
    assert!(s.contains(&format!("b={RECORDS} (expected")), "{s}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn cluster_input_map_exchange_trickle_loses_data() {
    let s = cluster_case(true).await;
    println!("{s}");
    assert!(s.contains(&format!("b={RECORDS} (expected")), "{s}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "expected failure (C1/C2); see EXPECTED in tests/audit_repros.rs"]
async fn spawn_multi_input_exchange_trickle_loses_data() {
    let s = multi_case(false).await;
    println!("{s}");
    assert!(s.contains(&format!("records={RECORDS} (expected")), "{s}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn spawn_multi_input_map_exchange_trickle_loses_data() {
    let s = multi_case(true).await;
    println!("{s}");
    assert!(s.contains(&format!("records={RECORDS} (expected")), "{s}");
}
