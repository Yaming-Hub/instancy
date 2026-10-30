//! Shared-mode transport reliability (PR9): a pooled connection that fails
//! is never silent. Adapted from the correctness audit's repros (C7, H7, M5).

#![cfg(feature = "transport")]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport::Frame;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};

/// Every cluster run here finishes (successfully or not) well within this.
const BOUND: Duration = Duration::from_secs(30);

fn health_tx() -> tokio::sync::broadcast::Sender<instancy::RuntimeEvent> {
    tokio::sync::broadcast::channel(16).0
}

fn config(connections: usize) -> SharedConnectionConfig {
    SharedConnectionConfig {
        connections,
        probe_interval: Duration::from_secs(3600),
        ..SharedConnectionConfig::default()
    }
}

/// Pairs the `establish()` calls of two nodes through in-process relays the
/// test can cut (a connection reset between the nodes).
#[derive(Default)]
struct Broker {
    pending: Mutex<[VecDeque<DuplexStream>; 2]>,
    /// The relays, in the order their connections were created.
    relays: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    calls: [AtomicUsize; 2],
}

impl Broker {
    fn establish_side(&self, side: usize) -> DuplexStream {
        self.calls[side].fetch_add(1, Ordering::SeqCst);
        let mut pending = self.pending.lock().unwrap();
        if let Some(stream) = pending[side].pop_front() {
            return stream;
        }
        let (mine, mut relay_mine) = tokio::io::duplex(1 << 20);
        let (theirs, mut relay_theirs) = tokio::io::duplex(1 << 20);
        pending[1 - side].push_back(theirs);
        self.relays.lock().unwrap().push(tokio::spawn(async move {
            let _ = tokio::io::copy_bidirectional(&mut relay_mine, &mut relay_theirs).await;
        }));
        mine
    }

    /// Cut the `index`-th connection.
    fn cut(&self, index: usize) {
        self.relays.lock().unwrap()[index].abort();
    }
}

struct Side {
    broker: Arc<Broker>,
    side: usize,
}

impl ConnectionFactory for Side {
    type Reader = ReadHalf<DuplexStream>;
    type Writer = WriteHalf<DuplexStream>;

    async fn establish(
        &self,
        _peer: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(tokio::io::split(self.broker.establish_side(self.side)))
    }
}

type Managers = Arc<HashMap<String, SharedPeerManager>>;

/// Shared managers of nodes "node-a" and "node-b", `connections` each.
fn brokered(connections: usize) -> (Arc<Broker>, Managers, Managers) {
    let broker = Arc::new(Broker::default());
    let handle = tokio::runtime::Handle::current();
    let managers = |peer: &str, side: usize| {
        let factory: Arc<dyn DynConnectionFactory> = Arc::new(Side {
            broker: broker.clone(),
            side,
        });
        Arc::new(HashMap::from([(
            peer.to_string(),
            SharedPeerManager::new(
                peer.to_string(),
                config(connections),
                factory,
                &handle,
                health_tx(),
            )
            .unwrap(),
        )]))
    };
    let a = managers("node-b", 0);
    let b = managers("node-a", 1);
    (broker, a, b)
}

fn topology() -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap()
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 1,
        ..Default::default()
    })
    .unwrap()
}

fn build(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<u64>("data")?
        .exchange("by_value", |x: &u64| *x)
        .map("id", |_t, x| x)
        .output("results")?;
    Ok(())
}

/// One cluster dataflow on both nodes: node-a feeds it, node-b's input is
/// closed at once.
struct Run {
    a: instancy::runtime::ClusterSpawnedDataflow<u64>,
    b: instancy::runtime::ClusterSpawnedDataflow<u64>,
    input: instancy::InputSender<u64, u64>,
    out_a: instancy::OutputReceiver<u64, u64>,
    out_b: instancy::OutputReceiver<u64, u64>,
}

async fn spawn(
    rt_a: &RuntimeHandle,
    rt_b: &RuntimeHandle,
    managers_a: &Managers,
    managers_b: &Managers,
) -> Run {
    spawn_id(DataflowId::new(), rt_a, rt_b, managers_a, managers_b).await
}

async fn spawn_id(
    df: DataflowId,
    rt_a: &RuntimeHandle,
    rt_b: &RuntimeHandle,
    managers_a: &Managers,
    managers_b: &Managers,
) -> Run {
    let (a, b) = spawn_both(df, rt_a, rt_b, managers_a, managers_b).await;
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    let out_a = a.take_output::<u64>(0, "results").unwrap();
    let out_b = b.take_output::<u64>(0, "results").unwrap();
    let input = a.take_input::<u64>(0, "data").unwrap();
    drop(b.take_input::<u64>(0, "data").unwrap());
    Run {
        a,
        b,
        input,
        out_a,
        out_b,
    }
}

type Spawned = Result<instancy::runtime::ClusterSpawnedDataflow<u64>>;

/// Spawn dataflow `df` on both nodes.
async fn spawn_both(
    df: DataflowId,
    rt_a: &RuntimeHandle,
    rt_b: &RuntimeHandle,
    managers_a: &Managers,
    managers_b: &Managers,
) -> (Spawned, Spawned) {
    let handle = tokio::runtime::Handle::current();
    tokio::join!(
        rt_a.spawn_cluster_async(
            "pr9",
            topology(),
            "node-a",
            df,
            ClusterSpawnTransport::shared(managers_a.clone(), 1024),
            Duration::from_secs(10),
            build,
            &handle,
            SpawnOptions::new(),
        ),
        rt_b.spawn_cluster_async(
            "pr9",
            topology(),
            "node-b",
            df,
            ClusterSpawnTransport::shared(managers_b.clone(), 1024),
            Duration::from_secs(10),
            build,
            &handle,
            SpawnOptions::new(),
        ),
    )
}

impl Run {
    /// Send `values` at `epoch` and advance past it. False once the input
    /// is closed (the dataflow failed).
    fn feed(&self, epoch: u64, values: Vec<u64>) -> bool {
        self.input.send(epoch, values).is_ok() && self.input.advance_to(epoch + 1).is_ok()
    }

    /// Close the input and wait (bounded) for both nodes. Returns their
    /// results and every record delivered, sorted.
    async fn finish(self) -> (Result<()>, Result<()>, Vec<u64>) {
        drop(self.input);
        let (a, b) = (self.a.join().unwrap(), self.b.join().unwrap());
        let (ra, rb) = tokio::time::timeout(BOUND, async { tokio::join!(a, b) })
            .await
            .expect("HANG: a node never finished");
        let mut got: Vec<u64> = self
            .out_a
            .collect_data()
            .into_iter()
            .chain(self.out_b.collect_data())
            .flat_map(|(_, d)| d)
            .collect();
        got.sort();
        (ra, rb, got)
    }
}

/// C7: a transient reset of the only pooled connection, mid-run. The audit
/// saw both nodes report success with 344-362 of 400 records (or hang).
/// Now both nodes fail, promptly, and neither reports success.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn transient_reset_fails_the_dataflow_on_both_nodes() {
    let (broker, managers_a, managers_b) = brokered(1);
    let (rt_a, rt_b) = (runtime(), runtime());
    let run = spawn(&rt_a, &rt_b, &managers_a, &managers_b).await;
    for t in 0..40u64 {
        if !run.feed(t, (0..10).map(|i| t * 10 + i).collect()) {
            break;
        }
        if t == 10 {
            // Reset an idle link: in-flight frames have drained.
            tokio::time::sleep(Duration::from_millis(150)).await;
            broker.cut(0);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (ra, rb, got) = run.finish().await;
    assert!(
        ra.is_err() && rb.is_err(),
        "a node reported success after its link failed: a={ra:?} b={rb:?} ({} of 400 records)",
        got.len()
    );
    // Nothing reconnects behind the dataflow's back.
    assert_eq!(broker.calls[0].load(Ordering::SeqCst), 1);
    assert_eq!(broker.calls[1].load(Ordering::SeqCst), 1);
}

/// DA R52: a failed pooled connection fails exactly the dataflows pinned to
/// it, on both nodes, never with `Ok` and lost records; a dataflow on the
/// pool's other connection completes with every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failed_connection_fails_only_the_dataflows_pinned_to_it() {
    let (broker, managers_a, managers_b) = brokered(2);
    let (rt_a, rt_b) = (runtime(), runtime());
    // Registered first on both nodes: pinned to the first connection.
    let hit = spawn(&rt_a, &rt_b, &managers_a, &managers_b).await;
    let spared = spawn(&rt_a, &rt_b, &managers_a, &managers_b).await;
    for t in 0..5u64 {
        assert!(hit.feed(t, (0..10).map(|i| t * 10 + i).collect()));
        assert!(spared.feed(t, (0..10).map(|i| 1000 + t * 10 + i).collect()));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    broker.cut(0);

    for t in 5..20u64 {
        assert!(
            spared.feed(t, (0..10).map(|i| 1000 + t * 10 + i).collect()),
            "the spared dataflow's input closed"
        );
    }
    let (ra, rb, got) = spared.finish().await;
    ra.unwrap();
    rb.unwrap();
    assert_eq!(got, (1000..1200).collect::<Vec<u64>>());

    let (ra, rb, got) = hit.finish().await;
    assert!(
        ra.is_err() && rb.is_err(),
        "the dataflow on the failed connection: a={ra:?} b={rb:?} ({} records)",
        got.len()
    );
    assert_eq!(managers_a["node-b"].connection_count(), 1);
}

/// One-directional pump that delivers a few MB/s (a slow link).
async fn pump(mut from: ReadHalf<DuplexStream>, mut to: WriteHalf<DuplexStream>) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn slow_link() -> (DuplexStream, DuplexStream) {
    let (a, relay_a) = tokio::io::duplex(64 * 1024);
    let (b, relay_b) = tokio::io::duplex(64 * 1024);
    let (ra_r, ra_w) = tokio::io::split(relay_a);
    let (rb_r, rb_w) = tokio::io::split(relay_b);
    tokio::spawn(pump(ra_r, rb_w));
    tokio::spawn(pump(rb_r, ra_w));
    (a, b)
}

struct Pre(Mutex<VecDeque<DuplexStream>>);

impl ConnectionFactory for Pre {
    type Reader = ReadHalf<DuplexStream>;
    type Writer = WriteHalf<DuplexStream>;
    async fn establish(
        &self,
        _peer: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        let stream = self.0.lock().unwrap().pop_front().ok_or("no more")?;
        Ok(tokio::io::split(stream))
    }
}

/// H7: with two pooled connections, small frames used to overtake a large
/// one on the other connection, and the 50 ms gap timer failed the healthy
/// dataflow with `ReorderTimeout`. A dataflow's frames now share one
/// connection: they arrive in order and nothing fails. The pool never
/// resizes itself, however its RTT looks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_frame_then_small_ones_arrive_in_order_without_failure() {
    let (a1, b1) = slow_link();
    let (a2, b2) = slow_link();
    let handle = tokio::runtime::Handle::current();
    let probing = SharedConnectionConfig {
        connections: 2,
        probe_interval: Duration::from_millis(5),
        ..SharedConnectionConfig::default()
    };
    let factory = |streams: [DuplexStream; 2]| -> Arc<dyn DynConnectionFactory> {
        Arc::new(Pre(Mutex::new(VecDeque::from(streams))))
    };
    let mgr_a = SharedPeerManager::new(
        "b".into(),
        probing.clone(),
        factory([a1, a2]),
        &handle,
        health_tx(),
    )
    .unwrap();
    let mgr_b =
        SharedPeerManager::new("a".into(), probing, factory([b1, b2]), &handle, health_tx())
            .unwrap();
    let df = DataflowId::new();
    let (_ra, mut ea) = mgr_a.register_dataflow(df, &[1], 1024).await.unwrap();
    let (mut rb, mut eb) = mgr_b.register_dataflow(df, &[1], 1024).await.unwrap();

    let sender = mgr_a.dataflow_sender(&df).unwrap();
    let send = |payload: Vec<u8>| {
        sender.send(Frame {
            dataflow_id: df,
            channel_id: 1,
            payload,
        })
    };
    send(vec![7u8; 2 * 1024 * 1024]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    for i in 0..5u8 {
        send(vec![i]).await.unwrap();
    }
    let mut sizes = Vec::new();
    while sizes.len() < 6 {
        let payload = tokio::time::timeout(BOUND, rb.get_mut(&1).unwrap().recv())
            .await
            .expect("frames stopped arriving")
            .expect("channel closed");
        sizes.push(payload.len());
    }
    assert_eq!(sizes, vec![2 * 1024 * 1024, 1, 1, 1, 1, 1]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(ea.try_recv().is_err(), "a healthy dataflow was failed");
    assert!(eb.try_recv().is_err(), "a healthy dataflow was failed");
    assert_eq!(mgr_a.connection_count(), 2);
    assert_eq!(mgr_b.connection_count(), 2);
}

/// M5: a Shared-mode spawn against an unreachable peer fails within its
/// handshake timeout instead of waiting for the connection forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_spawn_against_an_unreachable_peer_respects_the_handshake_timeout() {
    struct NeverConnects;
    impl ConnectionFactory for NeverConnects {
        type Reader = ReadHalf<DuplexStream>;
        type Writer = WriteHalf<DuplexStream>;
        async fn establish(
            &self,
            _peer: &str,
        ) -> std::result::Result<
            (Self::Reader, Self::Writer),
            Box<dyn std::error::Error + Send + Sync>,
        > {
            // A black-holed peer: the connect never completes.
            std::future::pending().await
        }
    }
    let handle = tokio::runtime::Handle::current();
    let managers = Arc::new(HashMap::from([(
        "node-b".to_string(),
        SharedPeerManager::new(
            "node-b".into(),
            config(1),
            Arc::new(NeverConnects),
            &handle,
            health_tx(),
        )
        .unwrap(),
    )]));
    let rt = runtime();
    let spawn = rt.spawn_cluster_async(
        "unreachable",
        topology(),
        "node-a",
        DataflowId::new(),
        ClusterSpawnTransport::shared(managers, 64),
        Duration::from_secs(1),
        build,
        &handle,
        SpawnOptions::new(),
    );
    let result = tokio::time::timeout(Duration::from_secs(5), spawn)
        .await
        .expect("spawn still pending after 5 s with a 1 s handshake timeout");
    assert!(result.is_err());
}

/// A `DataflowId` identifies one run. Spawning a finished run's id again on
/// the same managers (a retry that reused the id) fails promptly on both
/// nodes instead of running with the old run's late frames; a retry with a
/// fresh id runs normally on the same pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn reusing_a_finished_dataflow_id_fails_the_spawn_loudly() {
    let (_broker, managers_a, managers_b) = brokered(1);
    let (rt_a, rt_b) = (runtime(), runtime());
    let df = DataflowId::new();
    let (a, b) = spawn_both(df, &rt_a, &rt_b, &managers_a, &managers_b).await;
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    let out_a = a.take_output::<u64>(0, "results").unwrap();
    let out_b = b.take_output::<u64>(0, "results").unwrap();
    let input = a.take_input::<u64>(0, "data").unwrap();
    drop(b.take_input::<u64>(0, "data").unwrap());
    let first = Run {
        a,
        b,
        input,
        out_a,
        out_b,
    };
    assert!(first.feed(0, (0..10).collect()));
    let (ra, rb, got) = first.finish().await;
    assert!(ra.is_ok() && rb.is_ok(), "a={ra:?} b={rb:?}");
    assert_eq!(got, (0..10).collect::<Vec<_>>());

    let (a, b) = tokio::time::timeout(
        BOUND,
        spawn_both(df, &rt_a, &rt_b, &managers_a, &managers_b),
    )
    .await
    .expect("HANG: re-spawning a finished id");
    for (node, result) in [("node-a", &a), ("node-b", &b)] {
        match result {
            Err(e) => assert!(
                e.to_string().contains("fresh DataflowId"),
                "{node}: unexpected error: {e}"
            ),
            Ok(_) => panic!("{node}: a finished DataflowId was accepted again"),
        }
    }

    let retry = spawn(&rt_a, &rt_b, &managers_a, &managers_b).await;
    assert!(retry.feed(0, (0..10).collect()));
    let (ra, rb, got) = retry.finish().await;
    assert!(ra.is_ok() && rb.is_ok(), "a={ra:?} b={rb:?}");
    assert_eq!(got, (0..10).collect::<Vec<_>>());
}

/// Spawn dataflow `df` on node-a only, with `build`; bounded.
async fn spawn_a<F>(
    df: DataflowId,
    rt_a: &RuntimeHandle,
    managers_a: &Managers,
    build: F,
) -> Spawned
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    tokio::time::timeout(
        Duration::from_secs(5),
        rt_a.spawn_cluster_async(
            "spawn-failure",
            topology(),
            "node-a",
            df,
            ClusterSpawnTransport::shared(managers_a.clone(), 1024),
            Duration::from_secs(10),
            build,
            &handle,
            SpawnOptions::new(),
        ),
    )
    .await
    .expect("HANG: a refused spawn took longer than 5 s")
}

/// A spawn refused because its `DataflowId` is registered here already (a
/// duplicate of a running dataflow) sends nothing under the id: the running
/// dataflow is not cancelled, on either node, and finishes with every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_refused_duplicate_spawn_leaves_the_running_dataflow_alone() {
    let (_broker, managers_a, managers_b) = brokered(1);
    let (rt_a, rt_b) = (runtime(), runtime());
    let df = DataflowId::new();
    let run = spawn_id(df, &rt_a, &rt_b, &managers_a, &managers_b).await;
    assert!(run.feed(0, (0..10).collect()));
    match spawn_a(df, &rt_a, &managers_a, build).await {
        Err(e) => assert!(e.to_string().contains("already registered"), "{e}"),
        Ok(_) => panic!("a duplicate of a running DataflowId was accepted"),
    }
    // A Cancel sent under the id would have reached node-b's run by now.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        run.feed(1, (10..20).collect()),
        "the running dataflow failed"
    );
    let (ra, rb, got) = run.finish().await;
    assert!(ra.is_ok() && rb.is_ok(), "a={ra:?} b={rb:?}");
    assert_eq!(got, (0..20).collect::<Vec<_>>());
}

/// A retry that reused an id: node-a's run of it has finished (its manager
/// remembers the id), while node-b still has it registered (that run's last
/// sweep). Spawning the id again on node-a is refused and sends nothing
/// under it: node-b's registration receives no control frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_reuse_sends_nothing_under_the_id() {
    use instancy::communication::transport_session::CONTROL_CHANNEL_ID;

    let (_broker, managers_a, managers_b) = brokered(1);
    let rt_a = runtime();
    let df = DataflowId::new();
    let (mut b_channels, _b_events) = managers_b["node-a"]
        .register_dataflow(df, &[1], 4)
        .await
        .unwrap();
    drop(
        managers_a["node-b"]
            .register_dataflow(df, &[1], 4)
            .await
            .unwrap(),
    );
    managers_a["node-b"].unregister_dataflow(&df).await;
    match spawn_a(df, &rt_a, &managers_a, build).await {
        Err(e) => assert!(e.to_string().contains("fresh DataflowId"), "{e}"),
        Ok(_) => panic!("a finished DataflowId was accepted again"),
    }
    let control = b_channels.get_mut(&CONTROL_CHANNEL_ID).unwrap();
    let received = tokio::time::timeout(Duration::from_millis(500), control.recv()).await;
    assert!(
        received.is_err(),
        "node-b's registration of the id received a control frame: {received:?}"
    );
}

fn failing_build(_b: &mut DataflowBuilder<u64>) -> Result<()> {
    Err(instancy::Error::Runtime(
        instancy::RuntimeError::InvalidConfig("deliberate build failure".into()),
    ))
}

/// A spawn that fails on node-a (its dataflow does not build) fails that
/// dataflow on node-b promptly, and nothing else: another dataflow running
/// on the same managers keeps running and finishes with every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_failed_spawn_leaves_other_dataflows_on_the_managers_running() {
    let (_broker, managers_a, managers_b) = brokered(1);
    let (rt_a, rt_b) = (runtime(), runtime());
    let run = spawn(&rt_a, &rt_b, &managers_a, &managers_b).await;
    assert!(run.feed(0, (0..10).collect()));
    let df = DataflowId::new();
    let handle = tokio::runtime::Handle::current();
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(
        rt_a.spawn_cluster_async(
            "spawn-failure",
            topology(),
            "node-a",
            df,
            ClusterSpawnTransport::shared(managers_a.clone(), 1024),
            Duration::from_secs(10),
            failing_build,
            &handle,
            SpawnOptions::new(),
        ),
        rt_b.spawn_cluster_async(
            "spawn-failure",
            topology(),
            "node-b",
            df,
            ClusterSpawnTransport::shared(managers_b.clone(), 1024),
            Duration::from_secs(10),
            build,
            &handle,
            SpawnOptions::new(),
        ),
    );
    let took = started.elapsed();
    assert!(a.is_err(), "precondition");
    assert!(
        b.is_err() && took < Duration::from_secs(5),
        "node-b's spawn must fail promptly (took {took:?}): {:?}",
        b.as_ref().map(|_| ())
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(run.feed(1, (10..20).collect()), "the other dataflow failed");
    let (ra, rb, got) = run.finish().await;
    assert!(ra.is_ok() && rb.is_ok(), "a={ra:?} b={rb:?}");
    assert_eq!(got, (0..20).collect::<Vec<_>>());
}
