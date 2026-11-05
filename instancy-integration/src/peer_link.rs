//! Pairs the Shared transport's pooled connections between node processes.
//!
//! instancy's [`ConnectionFactory`] contract: each connection one node
//! establishes must pair with one its peer establishes. Here the node with
//! the higher ID dials (the address the coordinator gave it, normally a relay
//! route) and the lower one accepts on its listener:
//!
//! - The dialer writes a prefix before anything else: a big-endian `u16`
//!   length and its node ID, a big-endian `u64` incarnation (which process
//!   of that ID this is), then a `u16` length and the node-set key.
//! - The acceptor reads the prefix and queues the stream under (peer, node
//!   set). Each node set has its own managers on both ends, so a stream
//!   reaches the manager that asked for it.
//! - An accepting `establish` pops from its queue, first discarding streams
//!   whose incarnation is below the highest seen from that peer: they come
//!   from a process that has since been replaced.

use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use instancy::communication::{ConnectionFactory, DynConnectionFactory};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::AbortHandle;

/// How long an accepted connection may take to send its prefix.
const PREFIX_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the accept loop waits after an accept error before trying again.
const ACCEPT_RETRY: Duration = Duration::from_millis(10);

/// The longest node ID or node-set key a prefix may carry.
const MAX_PREFIX_FIELD: usize = 4096;

/// The read half of a paired connection.
pub type LinkReader = Box<dyn AsyncRead + Unpin + Send>;
/// The write half of a paired connection.
pub type LinkWriter = Box<dyn AsyncWrite + Unpin + Send>;

type EstablishError = Box<dyn std::error::Error + Send + Sync>;

/// The key of a node set: its node IDs, sorted and joined with `,`. Both
/// ends of a connection compute the same key from the same topology.
pub fn node_set_key<'a>(nodes: impl IntoIterator<Item = &'a str>) -> String {
    let mut nodes: Vec<&str> = nodes.into_iter().collect();
    nodes.sort_unstable();
    nodes.join(",")
}

/// One node process's side of the pairing: its listener, the addresses it
/// dials, and the streams accepted from its peers.
///
/// Dropping it stops the listener.
pub struct PeerLinks {
    state: Arc<LinkState>,
    accept: AbortHandle,
}

struct LinkState {
    node_id: String,
    incarnation: AtomicU64,
    /// Peer ID → the address this node dials to reach it.
    peers: Mutex<HashMap<String, SocketAddr>>,
    inbox: Mutex<Inbox>,
    /// Woken whenever a stream is queued.
    arrived: Notify,
}

#[derive(Default)]
struct Inbox {
    /// (peer ID, node-set key) → accepted streams, oldest first.
    queues: HashMap<(String, String), VecDeque<Accepted>>,
    /// Peer ID → the highest incarnation any of its prefixes carried.
    highest: HashMap<String, u64>,
}

struct Accepted {
    incarnation: u64,
    stream: TcpStream,
}

/// What a dialer's prefix says.
struct Prefix {
    node_id: String,
    incarnation: u64,
    node_set: String,
}

impl LinkState {
    fn inbox(&self) -> MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn queue(&self, prefix: Prefix, stream: TcpStream) {
        let mut inbox = self.inbox();
        let highest = inbox.highest.entry(prefix.node_id.clone()).or_default();
        *highest = (*highest).max(prefix.incarnation);
        inbox
            .queues
            .entry((prefix.node_id, prefix.node_set))
            .or_default()
            .push_back(Accepted {
                incarnation: prefix.incarnation,
                stream,
            });
        drop(inbox);
        self.arrived.notify_waiters();
    }

    /// The oldest stream queued from `peer` for `node_set` by its newest
    /// process. Streams from older processes are dropped, which closes them.
    fn pop(&self, peer: &str, node_set: &str) -> Option<TcpStream> {
        let mut inbox = self.inbox();
        let highest = inbox.highest.get(peer).copied().unwrap_or_default();
        let queue = inbox
            .queues
            .get_mut(&(peer.to_string(), node_set.to_string()))?;
        while let Some(accepted) = queue.pop_front() {
            if accepted.incarnation >= highest {
                return Some(accepted.stream);
            }
            tracing::info!(
                "peer link {}: discarded a stream from {peer} incarnation {} for {node_set}; \
                 incarnation {highest} has connected since",
                self.node_id,
                accepted.incarnation
            );
        }
        None
    }
}

impl PeerLinks {
    /// Bind a loopback listener for `node_id`'s peers and start accepting.
    /// Returns the listener's address. `incarnation` goes into the prefix of
    /// every stream this process dials.
    pub async fn start(
        node_id: &str,
        incarnation: u64,
    ) -> std::io::Result<(PeerLinks, SocketAddr)> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(LinkState {
            node_id: node_id.to_string(),
            incarnation: AtomicU64::new(incarnation),
            peers: Mutex::default(),
            inbox: Mutex::default(),
            arrived: Notify::new(),
        });
        let accept = tokio::spawn(accept_loop(Arc::clone(&state), listener)).abort_handle();
        Ok((PeerLinks { state, accept }, addr))
    }

    /// Set the incarnation written into the prefix of streams dialed from now on.
    pub fn set_incarnation(&self, incarnation: u64) {
        self.state.incarnation.store(incarnation, Ordering::Relaxed);
    }

    /// Set the address this node dials for each peer with a lower ID.
    pub fn set_peers(&self, peers: HashMap<String, SocketAddr>) {
        *self
            .state
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = peers;
    }

    /// The connection factory for the managers of `node_set`.
    pub fn factory(&self, node_set: String) -> Arc<dyn DynConnectionFactory> {
        Arc::new(PairingFactory {
            state: Arc::clone(&self.state),
            node_set,
        })
    }

    /// How many streams from `peer` for `node_set` are queued.
    #[cfg(test)]
    fn queued(&self, peer: &str, node_set: &str) -> usize {
        self.state
            .inbox()
            .queues
            .get(&(peer.to_string(), node_set.to_string()))
            .map_or(0, VecDeque::len)
    }
}

impl Drop for PeerLinks {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Establishes the connections of one node set's managers.
struct PairingFactory {
    state: Arc<LinkState>,
    node_set: String,
}

impl PairingFactory {
    /// Dial `peer` and announce this node, its process and the node set.
    async fn dial(&self, peer: &str) -> Result<TcpStream, EstablishError> {
        let addr = self
            .state
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(peer)
            .copied()
            .ok_or_else(|| format!("no address configured for peer {peer}"))?;
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let prefix = encode_prefix(
            &self.state.node_id,
            self.state.incarnation.load(Ordering::Relaxed),
            &self.node_set,
        )?;
        stream.write_all(&prefix).await?;
        Ok(stream)
    }

    /// Wait for a stream `peer` dialed for this node set.
    async fn accept(&self, peer: &str) -> TcpStream {
        loop {
            let arrived = self.state.arrived.notified();
            tokio::pin!(arrived);
            // Registered before looking, so a stream queued in between wakes it.
            arrived.as_mut().enable();
            if let Some(stream) = self.state.pop(peer, &self.node_set) {
                return stream;
            }
            arrived.await;
        }
    }
}

impl ConnectionFactory for PairingFactory {
    type Reader = LinkReader;
    type Writer = LinkWriter;

    async fn establish(
        &self,
        peer_node_id: &str,
    ) -> Result<(LinkReader, LinkWriter), EstablishError> {
        let stream = match self.state.node_id.as_str().cmp(peer_node_id) {
            std::cmp::Ordering::Greater => self.dial(peer_node_id).await?,
            std::cmp::Ordering::Less => self.accept(peer_node_id).await,
            std::cmp::Ordering::Equal => {
                return Err(format!("node {peer_node_id} can't connect to itself").into())
            }
        };
        let (reader, writer) = stream.into_split();
        Ok((Box::new(reader), Box::new(writer)))
    }
}

fn encode_prefix(node_id: &str, incarnation: u64, node_set: &str) -> std::io::Result<Vec<u8>> {
    let field_len = |field: &str| {
        u16::try_from(field.len())
            .ok()
            .filter(|&len| usize::from(len) <= MAX_PREFIX_FIELD)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("prefix field too long: {} bytes", field.len()),
                )
            })
    };
    let mut prefix = Vec::with_capacity(12 + node_id.len() + node_set.len());
    prefix.extend_from_slice(&field_len(node_id)?.to_be_bytes());
    prefix.extend_from_slice(node_id.as_bytes());
    prefix.extend_from_slice(&incarnation.to_be_bytes());
    prefix.extend_from_slice(&field_len(node_set)?.to_be_bytes());
    prefix.extend_from_slice(node_set.as_bytes());
    Ok(prefix)
}

async fn read_prefix(stream: &mut TcpStream) -> std::io::Result<Prefix> {
    let node_id = read_field(stream).await?;
    let incarnation = stream.read_u64().await?;
    let node_set = read_field(stream).await?;
    Ok(Prefix {
        node_id,
        incarnation,
        node_set,
    })
}

async fn read_field(stream: &mut TcpStream) -> std::io::Result<String> {
    let len = usize::from(stream.read_u16().await?);
    if len > MAX_PREFIX_FIELD {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("prefix field too long: {len} bytes"),
        ));
    }
    let mut field = vec![0; len];
    stream.read_exact(&mut field).await?;
    String::from_utf8(field).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// The node-set key of the prefix at the start of `bytes`, the first bytes a
/// dialer sent: `Ok(None)` while they hold only part of a prefix, `Err` if
/// they can't start one. The harness's [`TagReader`](crate::relay::TagReader),
/// so the relay knows which node set each connection serves.
pub fn prefix_node_set(bytes: &[u8]) -> Result<Option<String>, String> {
    let mut rest = bytes;
    if take_field(&mut rest)?.is_none() {
        return Ok(None);
    }
    let Some((_incarnation, after)) = rest.split_first_chunk::<8>() else {
        return Ok(None);
    };
    rest = after;
    take_field(&mut rest)
}

/// Takes a field (a big-endian `u16` length and that many bytes of UTF-8)
/// from the front of `rest`, if all of it is there.
fn take_field(rest: &mut &[u8]) -> Result<Option<String>, String> {
    let Some((len, after)) = rest.split_first_chunk::<2>() else {
        return Ok(None);
    };
    let len = usize::from(u16::from_be_bytes(*len));
    if len > MAX_PREFIX_FIELD {
        return Err(format!("prefix field too long: {len} bytes"));
    }
    let Some(field) = after.get(..len) else {
        return Ok(None);
    };
    let field = std::str::from_utf8(field)
        .map_err(|e| format!("prefix field is not UTF-8: {e}"))?
        .to_string();
    *rest = &after[len..];
    Ok(Some(field))
}

async fn accept_loop(state: Arc<LinkState>, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(receive(Arc::clone(&state), stream));
            }
            Err(e) => {
                tracing::warn!("peer link {}: accept failed: {e}", state.node_id);
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

/// Read an accepted stream's prefix and queue the stream.
async fn receive(state: Arc<LinkState>, mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    match tokio::time::timeout(PREFIX_TIMEOUT, read_prefix(&mut stream)).await {
        Ok(Ok(prefix)) => state.queue(prefix, stream),
        Ok(Err(e)) => tracing::warn!(
            "peer link {}: dropped an accepted connection with a bad prefix: {e}",
            state.node_id
        ),
        Err(_) => tracing::warn!(
            "peer link {}: dropped an accepted connection that sent no prefix within {PREFIX_TIMEOUT:?}",
            state.node_id
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokio::time::{sleep, timeout, Instant};

    use super::*;

    /// Wait until `links` has `count` streams queued from `peer` for `node_set`.
    async fn wait_until_queued(links: &PeerLinks, peer: &str, node_set: &str, count: usize) {
        let started = Instant::now();
        while links.queued(peer, node_set) != count {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "{} streams queued from {peer} for {node_set} after 5 s, expected {count}",
                links.queued(peer, node_set)
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Write a marker the acceptor reads back to tell streams apart.
    async fn write_marker(writer: &mut (impl AsyncWrite + Unpin), marker: &str) {
        writer.write_all(&[marker.len() as u8]).await.unwrap();
        writer.write_all(marker.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
    }

    async fn read_marker(reader: &mut (impl AsyncRead + Unpin)) -> String {
        let read = async {
            let len = reader.read_u8().await.unwrap();
            let mut marker = vec![0; len as usize];
            reader.read_exact(&mut marker).await.unwrap();
            String::from_utf8(marker).unwrap()
        };
        timeout(Duration::from_secs(5), read)
            .await
            .expect("no marker within 5 s")
    }

    /// The tag reader gives nothing until the whole prefix has arrived, at
    /// whatever byte it is cut, then its node-set key, whatever follows it.
    #[test]
    fn prefix_node_set_reads_a_whole_prefix_only() {
        let prefix = encode_prefix("node-c", 3, "a,b,node-c").unwrap();
        for cut in 0..prefix.len() {
            assert_eq!(
                prefix_node_set(&prefix[..cut]),
                Ok(None),
                "{cut} of {} bytes",
                prefix.len()
            );
        }
        let mut stream = prefix.clone();
        assert_eq!(prefix_node_set(&stream), Ok(Some("a,b,node-c".into())));
        stream.extend_from_slice(b"\xff\x00 frames follow");
        assert_eq!(prefix_node_set(&stream), Ok(Some("a,b,node-c".into())));

        assert!(prefix_node_set(&[0xff, 0xff]).is_err(), "too long a field");
        assert!(
            prefix_node_set(&[0, 1, 0xff]).is_err(),
            "a node ID that is not UTF-8"
        );
    }

    #[tokio::test]
    async fn restart_discards_streams_from_the_old_process() {
        let (a, a_addr) = PeerLinks::start("a", 1).await.unwrap();
        let peers: HashMap<String, _> = [("a".to_string(), a_addr)].into();
        let (old_b, _) = PeerLinks::start("b", 1).await.unwrap();
        old_b.set_peers(peers.clone());
        let (new_b, _) = PeerLinks::start("b", 2).await.unwrap();
        new_b.set_peers(peers);
        let node_set = node_set_key(["b", "a"]);
        assert_eq!(node_set, "a,b");

        let (mut old_reader, mut old_writer) = old_b
            .factory(node_set.clone())
            .establish_dyn("a")
            .await
            .unwrap();
        write_marker(&mut old_writer, "old").await;
        wait_until_queued(&a, "b", &node_set, 1).await;

        let (_new_reader, mut new_writer) = new_b
            .factory(node_set.clone())
            .establish_dyn("a")
            .await
            .unwrap();
        write_marker(&mut new_writer, "new").await;
        wait_until_queued(&a, "b", &node_set, 2).await;

        let (mut reader, _writer) = a
            .factory(node_set.clone())
            .establish_dyn("b")
            .await
            .unwrap();
        assert_eq!(read_marker(&mut reader).await, "new");
        assert_eq!(a.queued("b", &node_set), 0, "the old stream was discarded");

        // The old process's stream was closed, not kept.
        let mut byte = [0u8; 1];
        let ended = timeout(Duration::from_secs(5), old_reader.read(&mut byte))
            .await
            .expect("the old stream stayed open");
        assert!(
            matches!(ended, Ok(0) | Err(_)),
            "the old stream delivered data: {ended:?}"
        );
    }

    #[tokio::test]
    async fn streams_pair_by_peer_and_node_set() {
        let (a, a_addr) = PeerLinks::start("a", 1).await.unwrap();
        let peers: HashMap<String, _> = [("a".to_string(), a_addr)].into();
        let wanted: BTreeMap<&str, [String; 2]> = [
            (
                "b",
                [node_set_key(["a", "b"]), node_set_key(["a", "b", "c"])],
            ),
            (
                "c",
                [node_set_key(["a", "c"]), node_set_key(["a", "b", "c"])],
            ),
        ]
        .into();

        // Each dialer dials twice for each of its node sets; the dialers'
        // halves stay open until the end.
        let mut dialed = Vec::new();
        let mut dialers = Vec::new();
        for (&dialer, node_sets) in &wanted {
            let (links, _) = PeerLinks::start(dialer, 1).await.unwrap();
            links.set_peers(peers.clone());
            for node_set in node_sets {
                for i in 0..2 {
                    let (reader, mut writer) = links
                        .factory(node_set.clone())
                        .establish_dyn("a")
                        .await
                        .unwrap();
                    write_marker(&mut writer, &format!("{dialer}|{node_set}|{i}")).await;
                    dialed.push((reader, writer));
                }
            }
            dialers.push(links);
        }
        for (&dialer, node_sets) in &wanted {
            for node_set in node_sets {
                wait_until_queued(&a, dialer, node_set, 2).await;
            }
        }

        for (&dialer, node_sets) in &wanted {
            for node_set in node_sets {
                let factory = a.factory(node_set.clone());
                let mut markers = Vec::new();
                for _ in 0..2 {
                    let (mut reader, _writer) = factory.establish_dyn(dialer).await.unwrap();
                    markers.push(read_marker(&mut reader).await);
                }
                markers.sort();
                assert_eq!(
                    markers,
                    [
                        format!("{dialer}|{node_set}|0"),
                        format!("{dialer}|{node_set}|1")
                    ],
                    "the streams a got from {dialer} for {node_set}"
                );
                assert_eq!(a.queued(dialer, node_set), 0);
            }
        }
    }
}
