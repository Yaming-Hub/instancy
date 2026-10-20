//! Exchange across a slow or stalled link with a tiny per-peer frame queue.
//!
//! node-a -> node-b traffic goes through a pausable relay over small in-memory
//! pipes, so `NetworkPush` hits a full queue during normal operation. Each
//! batch holds one record per node; after the local record is accepted and the
//! remote one pushes back, the retry must not deliver the local record again.

#![cfg(feature = "transport")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Notify;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

const BATCHES: u64 = 20;

#[derive(Default)]
struct Valve {
    paused: AtomicBool,
    notify: Notify,
}

impl Valve {
    async fn wait_open(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.paused.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    fn open(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

async fn relay(mut from: DuplexStream, mut to: DuplexStream, valve: Arc<Valve>) {
    let mut buf = vec![0u8; 64];
    loop {
        valve.wait_open().await;
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        valve.wait_open().await;
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

fn build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .exchange_by_hash("route", |x: &u64| *x)
        .map("id2", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// Returns the records received by node-a and node-b, sorted.
async fn run(congest: bool, capacity: usize, pipe: usize) -> (Vec<u64>, Vec<u64>) {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let valve = Arc::new(Valve::default());
    let (a_w, relay_in) = tokio::io::duplex(pipe);
    let (relay_out, b_r) = tokio::io::duplex(pipe);
    let relay_task = tokio::spawn(relay(relay_in, relay_out, Arc::clone(&valve)));
    let (b_w, a_r) = tokio::io::duplex(64 * 1024);
    let conn_a = PeerConnection {
        node_id: "node-b".to_string(),
        reader: a_r,
        writer: a_w,
    };
    let conn_b = PeerConnection {
        node_id: "node-a".to_string(),
        reader: b_r,
        writer: b_w,
    };
    let th = tokio::runtime::Handle::current();
    let spawn = |node: &'static str, conn: PeerConnection<DuplexStream, DuplexStream>| {
        let topology = topology.clone();
        let th = th.clone();
        tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 1,
                ..RuntimeConfig::default()
            })?;
            let cluster = rt.spawn_cluster(
                "congested-link",
                topology,
                node,
                id,
                ClusterSpawnTransport::dedicated(vec![conn], capacity),
                Duration::from_secs(10),
                build,
                &th,
                SpawnOptions::new(),
            )?;
            Ok::<_, instancy::Error>((rt, cluster))
        })
    };
    let (ra, rb) = tokio::join!(spawn("node-a", conn_a), spawn("node-b", conn_b));
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();
    let arrived = Arc::new(AtomicUsize::new(0));
    let collect = |rx: instancy::OutputReceiver<u64, u64>| {
        let arrived = Arc::clone(&arrived);
        std::thread::spawn(move || {
            let mut v = Vec::new();
            while let Some(ev) = rx.recv_timeout(Duration::from_secs(20)) {
                if let OutputEvent::Data { data, .. } = ev {
                    arrived.fetch_add(data.len(), Ordering::SeqCst);
                    v.extend(data);
                }
            }
            v.sort_unstable();
            v
        })
    };
    let out_a = collect(ca.take_output::<u64>(0, "out").unwrap());
    let out_b = collect(cb.take_output::<u64>(0, "out").unwrap());
    let input_a = ca.take_input::<u64>(0, "data").unwrap();
    let input_b = cb.take_input::<u64>(0, "data").unwrap();

    if congest {
        valve.paused.store(true, Ordering::SeqCst);
    }
    // One record for node-a (even) and one for node-b (odd) per batch.
    let feeder = tokio::task::spawn_blocking(move || {
        for i in 0..BATCHES {
            input_a.send(0, vec![2 * i, 2 * i + 1]).unwrap();
        }
        input_a
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    valve.open();
    let input_a = feeder.await.unwrap();

    // Keep the inputs open until every record has arrived, so no worker can
    // declare completion early while records are still in transit (audit
    // C1/H1, tracked separately).
    let deadline = Instant::now() + Duration::from_secs(15);
    while arrived.load(Ordering::SeqCst) < 2 * BATCHES as usize && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(input_a);
    drop(input_b);

    let joined_a = tokio::time::timeout(Duration::from_secs(15), ca.join().unwrap()).await;
    let joined_b = tokio::time::timeout(Duration::from_secs(15), cb.join().unwrap()).await;
    assert!(
        matches!(joined_a, Ok(Ok(()))),
        "node-a did not complete: {joined_a:?}"
    );
    assert!(
        matches!(joined_b, Ok(Ok(()))),
        "node-b did not complete: {joined_b:?}"
    );
    let received = (out_a.join().unwrap(), out_b.join().unwrap());
    relay_task.abort();
    let _ = tokio::task::spawn_blocking(move || {
        drop(rt_a);
        drop(rt_b);
    })
    .await;
    received
}

fn records_of_parity(parity: u64) -> Vec<u64> {
    (0..BATCHES).map(|i| 2 * i + parity).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_with_roomy_peer_queue_is_exact() {
    let (a, b) = run(false, 1024, 64 * 1024).await;
    assert_eq!(a, records_of_parity(0));
    assert_eq!(b, records_of_parity(1));
}

/// A two-frame peer queue over a slow link fills during normal operation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_with_small_peer_queue_delivers_each_record_once() {
    let (a, b) = run(false, 2, 64).await;
    assert_eq!(a, records_of_parity(0), "node-a got duplicates or losses");
    assert_eq!(b, records_of_parity(1), "node-b got duplicates or losses");
}

/// The link stalls while node-a keeps producing, so every batch retries its
/// remote record many times after its local record was accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_across_stalled_link_delivers_each_record_once() {
    let (a, b) = run(true, 2, 64).await;
    assert_eq!(a, records_of_parity(0), "node-a got duplicates or losses");
    assert_eq!(b, records_of_parity(1), "node-b got duplicates or losses");
}
