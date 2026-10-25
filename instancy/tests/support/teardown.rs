//! Shared helpers for multi-node cluster tests.
//!
//! Provides in-process links with per-direction latency and a pause valve
//! (stopping a direction applies real backpressure to the sender), TCP
//! meshes, shared-manager construction, and node spawning/joining helpers.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

pub type LinkConn = PeerConnection<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>>;
pub type TcpConn = PeerConnection<OwnedReadHalf, OwnedWriteHalf>;
pub type TcpTransport = ClusterSpawnTransport<OwnedReadHalf, OwnedWriteHalf>;
pub type Managers = Arc<HashMap<String, SharedPeerManager>>;

// ---------------------------------------------------------------------------
// In-process links
// ---------------------------------------------------------------------------

/// Pause control for one direction of a link.
#[derive(Default)]
pub struct Valve {
    paused: AtomicBool,
    notify: Notify,
    /// Armed by [`pause_before_handshake`](Self::pause_before_handshake).
    before_handshake: AtomicBool,
    /// Bytes the relay holds (read, not yet forwarded).
    held: AtomicUsize,
}

impl Valve {
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Keep forwarding whole frames until the first shutdown-handshake frame
    /// (a progress channel's end-of-stream marker or acknowledgement), then
    /// pause in front of it: everything the sender committed before its
    /// goodbye (progress batches, its seal) gets through, the goodbye and
    /// what follows is held until [`resume`](Self::resume). Nothing is
    /// dropped or reordered, so it works for shared managers' numbered frames
    /// too.
    pub fn pause_before_handshake(&self) {
        self.before_handshake.store(true, Ordering::SeqCst);
    }

    /// Bytes the relay holds now: something the sender wrote while the valve
    /// was paused (non-zero means the pause is doing something).
    pub fn held_bytes(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

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
}

/// The transport's frame header: `[..16]` (unused here), the channel id
/// `[16..24]`, the length of the payload and its trailing checksum
/// `[24..28]`. A checksum of the header follows it.
const FRAME_HEADER: usize = 28;
/// Size of each frame checksum.
const FRAME_CRC: usize = 4;

/// Whether a frame is a shutdown-handshake frame: an end-of-stream marker or
/// an acknowledgement on a progress channel (count `u32::MAX` or
/// `u32::MAX - 1`, then the CRC; shared managers prefix 8 sequence bytes).
fn is_handshake_frame(channel: u64, payload: &[u8]) -> bool {
    use instancy::communication::{PROGRESS_CHANNEL_BASE, RESERVED_CHANNEL_BASE};
    if !(PROGRESS_CHANNEL_BASE..RESERVED_CHANNEL_BASE).contains(&channel) {
        return false;
    }
    let count = match payload.len() {
        8 => &payload[..4],
        16 => &payload[8..12],
        _ => return false,
    };
    u32::from_le_bytes(count.try_into().unwrap()) >= u32::MAX - 1
}

/// The length of the whole frames at the start of `bytes` that come before
/// the first handshake frame, and whether one follows them.
fn before_handshake(bytes: &[u8]) -> (usize, bool) {
    let mut at = 0;
    while at + FRAME_HEADER <= bytes.len() {
        let channel = u64::from_le_bytes(bytes[at + 16..at + 24].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[at + 24..at + 28].try_into().unwrap()) as usize;
        let end = at + FRAME_HEADER + FRAME_CRC + len;
        if end > bytes.len() {
            break;
        }
        if is_handshake_frame(
            channel,
            &bytes[at + FRAME_HEADER + FRAME_CRC..end - FRAME_CRC],
        ) {
            return (at, true);
        }
        at = end;
    }
    (at, false)
}

/// Relay one direction: stops reading while paused (backpressure) and
/// delivers each chunk `latency` after it was read. Shuts the far side down
/// on EOF.
fn relay(
    mut from: ReadHalf<DuplexStream>,
    mut to: WriteHalf<DuplexStream>,
    latency: Duration,
    valve: Arc<Valve>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(tokio::time::Instant, Vec<u8>)>(64);
        let reader = async move {
            let mut buf = vec![0u8; 16 * 1024];
            // Bytes read but not forwarded yet: while paused the relay holds
            // one read (so a test can see that the valve held something) and
            // then stops reading, which backs the sender up.
            let mut held: Vec<u8> = Vec::new();
            loop {
                let paused = valve.paused.load(Ordering::SeqCst);
                if paused && !held.is_empty() {
                    valve.wait_open().await;
                    continue;
                }
                if !paused && !held.is_empty() && !valve.before_handshake.load(Ordering::SeqCst) {
                    valve.held.store(0, Ordering::SeqCst);
                    let due = tokio::time::Instant::now() + latency;
                    if tx.send((due, std::mem::take(&mut held))).await.is_err() {
                        break;
                    }
                }
                match from.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        held.extend_from_slice(&buf[..n]);
                        let forward = if valve.paused.load(Ordering::SeqCst) {
                            0
                        } else if valve.before_handshake.load(Ordering::SeqCst) {
                            let (forward, found) = before_handshake(&held);
                            if found {
                                valve.before_handshake.store(false, Ordering::SeqCst);
                                valve.pause();
                            }
                            forward
                        } else {
                            held.len()
                        };
                        if forward > 0 {
                            let due = tokio::time::Instant::now() + latency;
                            let bytes: Vec<u8> = held.drain(..forward).collect();
                            if tx.send((due, bytes)).await.is_err() {
                                break;
                            }
                        }
                        valve.held.store(held.len(), Ordering::SeqCst);
                    }
                }
            }
            if !held.is_empty() {
                valve.wait_open().await;
                let due = tokio::time::Instant::now() + latency;
                let _ = tx.send((due, held)).await;
            }
            valve.held.store(0, Ordering::SeqCst);
        };
        let writer = async move {
            while let Some((due, bytes)) = rx.recv().await {
                tokio::time::sleep_until(due).await;
                if to.write_all(&bytes).await.is_err() {
                    return;
                }
            }
            let _ = to.shutdown().await;
        };
        tokio::join!(reader, writer);
    })
}

/// A bidirectional in-process link between two nodes.
pub struct Link {
    /// Node `a`'s end of the connection.
    pub a_end: DuplexStream,
    /// Node `b`'s end of the connection.
    pub b_end: DuplexStream,
    pub a_to_b: Arc<Valve>,
    pub b_to_a: Arc<Valve>,
    pub relays: Vec<JoinHandle<()>>,
}

impl Link {
    pub fn new(lat_ab: Duration, lat_ba: Duration, buf: usize) -> Self {
        let (a_end, relay_a) = tokio::io::duplex(buf);
        let (b_end, relay_b) = tokio::io::duplex(buf);
        let (ra_read, ra_write) = tokio::io::split(relay_a);
        let (rb_read, rb_write) = tokio::io::split(relay_b);
        let a_to_b = Arc::new(Valve::default());
        let b_to_a = Arc::new(Valve::default());
        let relays = vec![
            relay(ra_read, rb_write, lat_ab, Arc::clone(&a_to_b)),
            relay(rb_read, ra_write, lat_ba, Arc::clone(&b_to_a)),
        ];
        Self {
            a_end,
            b_end,
            a_to_b,
            b_to_a,
            relays,
        }
    }
}

/// Writer that fails every operation once `broken` is set (see
/// [`Mesh::dedicated_breakable`]).
pub struct BreakableWriter<W> {
    inner: W,
    broken: Arc<AtomicBool>,
}

impl<W> BreakableWriter<W> {
    fn check(&self) -> std::io::Result<()> {
        if self.broken.load(Ordering::SeqCst) {
            Err(std::io::Error::other("injected write failure"))
        } else {
            Ok(())
        }
    }
}

impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for BreakableWriter<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Err(error) = self.check() {
            return std::task::Poll::Ready(Err(error));
        }
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Err(error) = self.check() {
            return std::task::Poll::Ready(Err(error));
        }
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Err(error) = self.check() {
            return std::task::Poll::Ready(Err(error));
        }
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A full mesh of in-process links.
pub struct Mesh {
    ends: HashMap<(String, String), DuplexStream>,
    valves: HashMap<(String, String), Arc<Valve>>,
    relays: HashMap<(String, String), Vec<JoinHandle<()>>>,
}

impl Mesh {
    /// `latency(from, to)` sets the delay of each direction; `buf` is the
    /// in-flight byte budget of each direction.
    pub fn new(nodes: &[&str], buf: usize, latency: impl Fn(&str, &str) -> Duration) -> Self {
        let mut mesh = Self {
            ends: HashMap::new(),
            valves: HashMap::new(),
            relays: HashMap::new(),
        };
        for (i, a) in nodes.iter().enumerate() {
            for b in &nodes[i + 1..] {
                let link = Link::new(latency(a, b), latency(b, a), buf);
                let (a, b) = (a.to_string(), b.to_string());
                mesh.ends.insert((a.clone(), b.clone()), link.a_end);
                mesh.ends.insert((b.clone(), a.clone()), link.b_end);
                mesh.valves.insert((a.clone(), b.clone()), link.a_to_b);
                mesh.valves.insert((b.clone(), a.clone()), link.b_to_a);
                mesh.relays.insert((a, b), link.relays);
            }
        }
        mesh
    }

    /// Valve for the `from -> to` direction.
    pub fn valve(&self, from: &str, to: &str) -> Arc<Valve> {
        Arc::clone(&self.valves[&(from.to_string(), to.to_string())])
    }

    /// Kill the link between `a` and `b` (both directions), like a crash.
    pub fn sever(&self, a: &str, b: &str) {
        for relay in self.link_relays(a, b) {
            relay.abort();
        }
    }

    /// Whether both directions of the link between `a` and `b` have ended:
    /// each node closed its side for writing (or the link was severed). The
    /// relays pass each end of stream on, so each node's reader has then seen
    /// it too.
    pub fn link_closed(&self, a: &str, b: &str) -> bool {
        self.link_relays(a, b).iter().all(JoinHandle::is_finished)
    }

    fn link_relays(&self, a: &str, b: &str) -> &[JoinHandle<()>] {
        let key = if self.relays.contains_key(&(a.to_string(), b.to_string())) {
            (a.to_string(), b.to_string())
        } else {
            (b.to_string(), a.to_string())
        };
        &self.relays[&key]
    }

    fn take_ends(&mut self, node: &str) -> Vec<(String, DuplexStream)> {
        let keys: Vec<_> = self
            .ends
            .keys()
            .filter(|(from, _)| from == node)
            .cloned()
            .collect();
        keys.into_iter()
            .map(|key| {
                let stream = self.ends.remove(&key).unwrap();
                (key.1, stream)
            })
            .collect()
    }

    /// Dedicated-mode transport for `node`.
    pub fn dedicated(
        &mut self,
        node: &str,
        capacity: usize,
    ) -> ClusterSpawnTransport<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>> {
        let conns = self
            .take_ends(node)
            .into_iter()
            .map(|(peer, stream)| {
                let (reader, writer) = tokio::io::split(stream);
                PeerConnection {
                    node_id: peer,
                    reader,
                    writer,
                }
            })
            .collect();
        ClusterSpawnTransport::dedicated(conns, capacity)
    }

    /// Shared-mode transport for `node` when `shared`, otherwise
    /// [`dedicated`](Self::dedicated), over the same links.
    pub fn transport(
        &mut self,
        node: &str,
        shared: bool,
        capacity: usize,
    ) -> ClusterSpawnTransport<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>> {
        if shared {
            ClusterSpawnTransport::Shared {
                peer_managers: self.managers(node),
                capacity,
            }
        } else {
            self.dedicated(node, capacity)
        }
    }

    /// [`dedicated`](Self::dedicated) whose writers fail once `broken` is
    /// set, while the readers and the peers' ends stay open. This is a local
    /// write error on split stream halves (as with TLS): dropping the failed
    /// writer sends the peer no EOF.
    pub fn dedicated_breakable(
        &mut self,
        node: &str,
        capacity: usize,
        broken: Arc<AtomicBool>,
    ) -> ClusterSpawnTransport<ReadHalf<DuplexStream>, BreakableWriter<WriteHalf<DuplexStream>>>
    {
        let conns = self
            .take_ends(node)
            .into_iter()
            .map(|(peer, stream)| {
                let (reader, writer) = tokio::io::split(stream);
                PeerConnection {
                    node_id: peer,
                    reader,
                    writer: BreakableWriter {
                        inner: writer,
                        broken: Arc::clone(&broken),
                    },
                }
            })
            .collect();
        ClusterSpawnTransport::dedicated(conns, capacity)
    }

    /// Shared-mode counterpart of [`dedicated_breakable`](Self::dedicated_breakable):
    /// `node`'s pooled connection writers fail once `broken` is set, while
    /// the readers and the peers' ends stay open. Also returns the managers,
    /// so the test can keep them alive (as long-lived managers are) after
    /// the dataflow ends.
    pub fn shared_breakable(
        &mut self,
        node: &str,
        capacity: usize,
        broken: Arc<AtomicBool>,
    ) -> (
        ClusterSpawnTransport<ReadHalf<DuplexStream>, BreakableWriter<WriteHalf<DuplexStream>>>,
        Managers,
    ) {
        let handle = tokio::runtime::Handle::current();
        let mut managers = HashMap::new();
        for (peer, stream) in self.take_ends(node) {
            let (reader, writer) = tokio::io::split(stream);
            let writer = BreakableWriter {
                inner: writer,
                broken: Arc::clone(&broken),
            };
            let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablished(Mutex::new(
                VecDeque::from([(reader, writer)]),
            )));
            managers.insert(
                peer.clone(),
                SharedPeerManager::new(
                    peer,
                    shared_config(1),
                    factory,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap(),
            );
        }
        let managers = Arc::new(managers);
        (
            ClusterSpawnTransport::Shared {
                peer_managers: Arc::clone(&managers),
                capacity,
            },
            managers,
        )
    }

    /// Shared-mode peer managers for `node` whose factories hand out the
    /// mesh connection once and then "reconnect" to a peer that accepts but
    /// never answers (see [`ReconnectToSilentPeer`]).
    pub fn managers_reconnecting(
        &mut self,
        node: &str,
    ) -> (Managers, Vec<Arc<ReconnectToSilentPeer>>) {
        let handle = tokio::runtime::Handle::current();
        let mut managers = HashMap::new();
        let mut factories = Vec::new();
        for (peer, stream) in self.take_ends(node) {
            let factory = Arc::new(ReconnectToSilentPeer {
                first: Mutex::new(Some(tokio::io::split(stream))),
                silent_peers: Mutex::new(Vec::new()),
                establishes: AtomicUsize::new(0),
            });
            factories.push(Arc::clone(&factory));
            let factory: Arc<dyn DynConnectionFactory> = factory;
            managers.insert(
                peer.clone(),
                SharedPeerManager::new(
                    peer,
                    shared_config(1),
                    factory,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap(),
            );
        }
        (Arc::new(managers), factories)
    }

    /// Shared-mode peer managers for `node` (one pooled connection per peer).
    pub fn managers(&mut self, node: &str) -> Managers {
        let handle = tokio::runtime::Handle::current();
        let mut managers = HashMap::new();
        for (peer, stream) in self.take_ends(node) {
            let factory: Arc<dyn DynConnectionFactory> =
                Arc::new(PreEstablished(Mutex::new(VecDeque::from([
                    tokio::io::split(stream),
                ]))));
            managers.insert(
                peer.clone(),
                SharedPeerManager::new(
                    peer,
                    shared_config(1),
                    factory,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap(),
            );
        }
        Arc::new(managers)
    }
}

// ---------------------------------------------------------------------------
// TCP
// ---------------------------------------------------------------------------

pub async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    accepted.0.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.0, connected)
}

/// Dedicated-mode TCP connections for a full mesh.
pub async fn tcp_mesh(nodes: &[&str]) -> HashMap<String, Vec<TcpConn>> {
    let mut result: HashMap<String, Vec<TcpConn>> =
        nodes.iter().map(|n| (n.to_string(), Vec::new())).collect();
    for (i, a) in nodes.iter().enumerate() {
        for b in &nodes[i + 1..] {
            let (sa, sb) = tcp_pair().await;
            let (ra, wa) = sa.into_split();
            let (rb, wb) = sb.into_split();
            result.get_mut(*a).unwrap().push(PeerConnection {
                node_id: b.to_string(),
                reader: ra,
                writer: wa,
            });
            result.get_mut(*b).unwrap().push(PeerConnection {
                node_id: a.to_string(),
                reader: rb,
                writer: wb,
            });
        }
    }
    result
}

/// Shared-mode managers for a full TCP mesh (`conns` pooled connections per pair).
pub async fn tcp_shared_mesh(nodes: &[&str], conns: usize) -> HashMap<String, Managers> {
    tcp_shared_mesh_with(nodes, conns, shared_config(conns)).await
}

/// [`tcp_shared_mesh`] with an explicit manager configuration. The pool is
/// pre-established: `config` must not scale beyond `conns` connections.
pub async fn tcp_shared_mesh_with(
    nodes: &[&str],
    conns: usize,
    config: SharedConnectionConfig,
) -> HashMap<String, Managers> {
    let handle = tokio::runtime::Handle::current();
    let mut per: HashMap<String, HashMap<String, SharedPeerManager>> = nodes
        .iter()
        .map(|n| (n.to_string(), HashMap::new()))
        .collect();
    for (i, a) in nodes.iter().enumerate() {
        for b in &nodes[i + 1..] {
            let mut ca = VecDeque::new();
            let mut cb = VecDeque::new();
            for _ in 0..conns {
                let (sa, sb) = tcp_pair().await;
                ca.push_back(sa.into_split());
                cb.push_back(sb.into_split());
            }
            let fa: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablished(Mutex::new(ca)));
            let fb: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablished(Mutex::new(cb)));
            let ma = SharedPeerManager::new(
                b.to_string(),
                config.clone(),
                fa,
                &handle,
                tokio::sync::broadcast::channel(16).0,
            )
            .unwrap();
            let mb = SharedPeerManager::new(
                a.to_string(),
                config.clone(),
                fb,
                &handle,
                tokio::sync::broadcast::channel(16).0,
            )
            .unwrap();
            per.get_mut(*a).unwrap().insert(b.to_string(), ma);
            per.get_mut(*b).unwrap().insert(a.to_string(), mb);
        }
    }
    per.into_iter().map(|(k, v)| (k, Arc::new(v))).collect()
}

// ---------------------------------------------------------------------------
// Shared managers
// ---------------------------------------------------------------------------

/// Connection factory handing out pre-established connections.
pub struct PreEstablished<R, W>(pub Mutex<VecDeque<(R, W)>>);

/// Connection factory that hands out one pre-established connection and then
/// "reconnects" to a peer that accepts but never reads or answers, like a
/// listener whose node has moved on. A manager using it never gives up on a
/// broken connection, so a peer waiting for frames lost with it waits
/// forever. Counts every establishment.
pub struct ReconnectToSilentPeer {
    first: Mutex<Option<(ReadHalf<DuplexStream>, WriteHalf<DuplexStream>)>>,
    silent_peers: Mutex<Vec<DuplexStream>>,
    pub establishes: AtomicUsize,
}

impl ConnectionFactory for ReconnectToSilentPeer {
    type Reader = ReadHalf<DuplexStream>;
    type Writer = WriteHalf<DuplexStream>;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<
        (ReadHalf<DuplexStream>, WriteHalf<DuplexStream>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        self.establishes.fetch_add(1, Ordering::SeqCst);
        if let Some(conn) = self.first.lock().unwrap().take() {
            return Ok(conn);
        }
        let (ours, silent) = tokio::io::duplex(1 << 16);
        self.silent_peers.lock().unwrap().push(silent);
        Ok(tokio::io::split(ours))
    }
}

impl<R, W> ConnectionFactory for PreEstablished<R, W>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    type Reader = R;
    type Writer = W;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(R, W), Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| "no more pre-established connections".into())
    }
}

/// Fixed-size pool without probing or scaling.
pub fn shared_config(conns: usize) -> SharedConnectionConfig {
    SharedConnectionConfig {
        connections: conns,
        probe_interval: Duration::from_secs(3600),
        rtt_ema_alpha: 0.2,
        ..SharedConnectionConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

pub fn runtime(worker_threads: usize) -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

pub type Spawned = (RuntimeHandle, Result<ClusterSpawnedDataflow<u64>>);

/// Spawn one node of a cluster dataflow on its own instancy runtime.
pub fn spawn_node<R, W, F>(
    topology: ClusterTopology,
    node: &str,
    dataflow_id: DataflowId,
    transport: ClusterSpawnTransport<R, W>,
    worker_threads: usize,
    build: F,
) -> JoinHandle<Spawned>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    spawn_node_with_options(
        topology,
        node,
        dataflow_id,
        transport,
        worker_threads,
        build,
        SpawnOptions::new(),
    )
}

/// [`spawn_node`] with explicit spawn options.
pub fn spawn_node_with_options<R, W, F>(
    topology: ClusterTopology,
    node: &str,
    dataflow_id: DataflowId,
    transport: ClusterSpawnTransport<R, W>,
    worker_threads: usize,
    build: F,
    options: SpawnOptions,
) -> JoinHandle<Spawned>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let node = node.to_string();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let rt = runtime(worker_threads);
        let cluster = rt.spawn_cluster(
            "teardown-test",
            topology,
            &node,
            dataflow_id,
            transport,
            Duration::from_secs(10),
            build,
            &handle,
            options,
        );
        (rt, cluster)
    })
}

/// Await a node's completion with a timeout; `None` means it hung.
pub fn join_with_timeout(
    cluster: ClusterSpawnedDataflow<u64>,
    timeout: Duration,
) -> JoinHandle<Option<Result<()>>> {
    tokio::spawn(async move {
        tokio::time::timeout(timeout, cluster.join().unwrap())
            .await
            .ok()
    })
}

pub fn fmt_join(r: &Option<Result<()>>) -> String {
    match r {
        None => "HANG".to_string(),
        Some(Ok(())) => "Ok".to_string(),
        Some(Err(e)) => format!("Err({e})"),
    }
}

/// Output collected on a background thread.
pub struct Collector<D> {
    rx: std::sync::mpsc::Receiver<Vec<(u64, D)>>,
    done: Option<Vec<(u64, D)>>,
}

impl<D> Collector<D> {
    /// Wait for the output to close; errors (instead of blocking forever) if
    /// the dataflow never closes it.
    pub fn join(mut self) -> std::result::Result<Vec<(u64, D)>, String> {
        if let Some(done) = self.done.take() {
            return Ok(done);
        }
        self.rx
            .recv_timeout(Duration::from_secs(20))
            .map_err(|_| "output never closed (dataflow hung)".to_string())
    }

    /// Whether the output closed within `timeout` (its worker finished).
    pub fn wait_closed(&mut self, timeout: Duration) -> bool {
        if self.done.is_none() {
            self.done = self.rx.recv_timeout(timeout).ok();
        }
        self.done.is_some()
    }
}

/// Collect an output on a background thread until the dataflow closes it.
pub fn collect<D: Send + 'static>(output: instancy::OutputReceiver<u64, D>) -> Collector<D> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut out = Vec::new();
        while let Some(event) = output.recv() {
            if let instancy::dataflow::OutputEvent::Data { time, data } = event {
                out.extend(data.into_iter().map(|d| (time, d)));
            }
        }
        let _ = tx.send(out);
    });
    Collector { rx, done: None }
}

/// Drop instancy runtimes off the async reactor (their drop joins threads).
pub async fn drop_runtimes(rts: Vec<RuntimeHandle>) {
    let _ = tokio::task::spawn_blocking(move || drop(rts)).await;
}
