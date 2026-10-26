//! Shared transport layer for pooled, multi-dataflow connections.
//!
//! Unlike [`TransportSession`](super::transport_session::TransportSession),
//! which uses dedicated per-dataflow connections, `SharedTransportSession`
//! lets many dataflows share a fixed pool of connections to each peer.
//!
//! # Architecture
//!
//! ```text
//! SharedPeerManager (per peer, owns the pooled connections)
//! ├── Pool: `connections` connections, each with a writer task and a reader task
//! ├── Bridge task: sequences payload frames and routes every frame of a
//! │   dataflow to the connection its lane is pinned to
//! ├── Probe loop: periodic RTT probes per connection (metrics only)
//! └── Per dataflow registration:
//!     ├── the pinned connection (chosen at registration, never changes)
//!     ├── send and receive sequence numbers of the payload lane
//!     └── per-channel receivers
//!
//! SharedTransportSession (per dataflow, lightweight handle)
//! ├── References a SharedPeerManager for each peer
//! ├── data_sender(peer) → shared payload channel
//! ├── progress_sender(peer) → same as data_sender (FIFO preserved)
//! └── control_sender(peer) → priority channel
//! ```
//!
//! # Pinned lanes
//!
//! Data and progress frames of a dataflow to a peer share one sequenced
//! payload lane, which preserves the timely ordering invariant: data at time
//! T is sent before the progress message releasing T. When a dataflow
//! registers, its lane (and its control frames) is pinned to one pooled
//! connection, the one with the fewest lanes, so TCP keeps its frames in
//! order. Each payload frame carries the lane's next sequence number, and
//! the receiver checks it: a frame that does not carry exactly the next
//! number (a lost or duplicated frame), or a lane whose frames arrive on two
//! connections, fails the dataflow ([`TransportError::OutOfSequence`],
//! [`TransportError::LaneViolation`]) instead of being delivered.
//!
//! # Connection failures (fail-stop)
//!
//! Frames on a connection that fails may be lost, and nothing resends them.
//! So a failed connection fails exactly the dataflows that used it, on both
//! nodes, like a lost connection in Dedicated mode (and as in timely, which
//! stops on any connection error):
//!
//! - when its writer fails, every dataflow whose lane is pinned to it is told
//!   ([`PeerIoEvent::WriteFailed`]), and the whole connection is closed
//!   (its reader stops too), so the peer sees it end even when the write
//!   half alone would tell it nothing (`tokio::io::split` halves, as with
//!   TLS);
//! - when its reader ends with an error (an I/O error, EOF mid-frame, a
//!   corrupt or oversize frame, or the writer's failure), every dataflow
//!   whose frames from the peer arrive on it, or whose lane is pinned to it,
//!   is told, the channels of a dataflow whose frames arrive on it close,
//!   and the connection leaves the pool;
//! - when the peer closes its side cleanly (EOF at a frame boundary), the
//!   same dataflows are told, but the lanes pinned to it keep writing until
//!   a write fails: a released manager keeps reading for its peers' last
//!   frames (goodbyes, acknowledgements, a `Cancel`), as in Dedicated mode.
//!   The connection takes no new lanes and is closed once its last lane
//!   unregisters;
//! - a payload frame that cannot be sent (its connection failed, or it
//!   exceeds the frame size limit) fails its dataflow and nobody else's.
//!
//! The failed connection leaves the pool. The pool is not redialled in the
//! background: the next dataflow that registers re-establishes missing
//! connections through the application's [`ConnectionFactory`].
//!
//! # One run per `DataflowId`
//!
//! Connections outlive dataflows, and frames carry only the
//! [`DataflowId`], which identifies one run. A run that reused an earlier
//! run's id could take that run's late frames for its own, so a manager
//! refuses to register an id it remembers as finished (the most recent
//! 16,384) and drops the late frames of such an id; a retry needs a fresh
//! id. Payload frames are queued with their registration, so those still
//! queued when it ends are dropped, never numbered into another
//! registration's lane.
//!
//! # Transport contract
//!
//! The application provides the connections and owns their liveness:
//! detecting that a peer is dead, partitioned or unreachable, through its
//! transport (keepalives, TLS or QUIC idle timeouts) or its membership layer
//! ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
//! instancy owns safety: it never reports success with lost, duplicated,
//! reordered or corrupted data (every frame is checksummed, and every lane is
//! sequenced), and it fails the affected dataflows promptly when a connection
//! reports an error or EOF, or when the application reports a node leaving.
//! A connection that silently stops delivering without ever failing (a
//! network partition with no keepalive) makes its dataflows wait; they are
//! never reported as successful.
//!
//! # Probe Protocol
//!
//! RTT probes use the standard [`Frame`] wire format with a reserved
//! `PROBE_CHANNEL_ID` to avoid mixing wire formats on the same TCP stream.
//! They only feed [`SharedPeerManager::metrics_snapshot`].

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc as tokio_mpsc;

use crate::communication::codec::MAX_MESSAGE_SIZE;
use crate::communication::probing::{ProbeCounter, ProbeKind, ProbeMessage, RttProber};
use crate::communication::shared_pool::{
    ConnectionMetrics, ConnectionSnapshot, SharedConnectionConfig,
};
use crate::communication::transport::{Frame, FramedReader, FramedWriter, TransportError};
use crate::communication::transport_session::{CONTROL_CHANNEL_ID, PeerIoEvent, PeerIoNotice};
use crate::dataflow::id::DataflowId;
use crate::error::LockResultExt;
use crate::wire;

/// Reserved channel ID for probe messages on shared connections.
///
/// This is distinct from `CONTROL_CHANNEL_ID` (0) and user data/progress channels.
/// Probe frames carry a [`ProbeMessage`] as their payload.
pub const PROBE_CHANNEL_ID: u64 = u64::MAX;

/// Size of the sequence number that prefixes every payload frame.
const SEQUENCE_PREFIX: usize = 8;

/// An entry of a manager's payload queue, which the bridge task processes in
/// order.
enum Outbound {
    /// A payload frame of the registration made with `token`. Dropped if
    /// that registration has ended: its frames are never sequenced into
    /// another registration's lane.
    Payload {
        dataflow_id: DataflowId,
        token: u64,
        frame: Frame,
    },
    /// A flush barrier (see [`SharedPeerManager::flush`]): everything queued
    /// before it has been processed. Never sent.
    Barrier(tokio::sync::oneshot::Sender<()>),
}

/// How long a released [`SharedPeerManager`] keeps its connections open to
/// write the frames already queued on them and to wait for the peer to close
/// its side (see [`linger`]).
const WRITER_DRAIN_GRACE: Duration = Duration::from_secs(10);

/// Most dataflows whose control frames are buffered before they register
/// here (see [`PendingControl`]).
const MAX_PENDING_DATAFLOWS: usize = 16_384;

/// Most control frames buffered for one dataflow that has not registered.
const MAX_PENDING_FRAMES_PER_DATAFLOW: usize = 64;

/// How long control frames for a dataflow that never registers are kept.
const PENDING_CONTROL_TTL: Duration = Duration::from_secs(120);

/// How many unregistered dataflows are remembered. A remembered id cannot
/// be registered again (a [`DataflowId`] identifies one run), and the late
/// frames of its finished run are dropped.
const MAX_COMPLETED_DATAFLOWS: usize = 16_384;

/// Capacity of a pooled connection's writer queue.
const WRITER_QUEUE: usize = 256;

/// Background tasks of a [`SharedPeerManager`].
///
/// Connection writers and readers are kept apart from the other tasks:
/// releasing the manager aborts the other tasks immediately but closes the
/// connections gracefully (see [`linger`]).
#[derive(Default)]
struct TaskHandles {
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    writers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    readers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TaskHandles {
    fn take(handles: &Mutex<Vec<tokio::task::JoinHandle<()>>>) -> Vec<tokio::task::JoinHandle<()>> {
        std::mem::take(
            &mut *handles
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn abort_tasks(&self) {
        for handle in self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            handle.abort();
        }
    }
}

/// Wait until every task has finished, aborting the rest at `deadline`.
async fn drain_tasks(mut tasks: Vec<tokio::task::JoinHandle<()>>, deadline: tokio::time::Instant) {
    let finished = tokio::time::timeout_at(deadline, async {
        for task in tasks.iter_mut() {
            let _ = task.await;
        }
    })
    .await;
    if finished.is_err() {
        for task in &tasks {
            task.abort();
        }
    }
}

/// Close a released manager's connections without losing what is already
/// queued on them.
///
/// The caller has dropped the writer queues' senders, which lets every
/// writer finish the frames queued on its connection (a dataflow's last
/// frames: its end-of-stream markers, acknowledgements or `Cancel`) and then
/// close its half of the connection. The readers keep reading until the peer
/// closes its half too: closing a socket with unread data makes the OS reset
/// the connection, which can destroy frames the peer has not read yet. A
/// peer that stops reading, or never closes, is cut off at the deadlines.
async fn linger(
    writers: Vec<tokio::task::JoinHandle<()>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    deadline: tokio::time::Instant,
) {
    drain_tasks(writers, deadline).await;
    drain_tasks(readers, deadline).await;
}

/// Keep `handle`, first forgetting the handles of tasks that have finished
/// (connections replaced after a failure leave them behind).
fn push_task_handle(
    task_handles: &Mutex<Vec<tokio::task::JoinHandle<()>>>,
    handle: tokio::task::JoinHandle<()>,
) {
    match task_handles.lock().or_poison("task handle") {
        Ok(mut handles) => {
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }
        Err(_) => {
            // NOTE: Cannot propagate lock poison — this is a fire-and-forget helper.
            // Aborting the task handle on poison is the correct fallback since the
            // transport is shutting down.
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectionFactory — user-provided connection establishment
// ---------------------------------------------------------------------------

/// Factory for establishing connections to a peer node.
///
/// The library is **transport-agnostic** — any reliable, ordered byte stream
/// works: TCP, TLS, Unix sockets, named pipes, QUIC, shared-memory channels,
/// etc. The factory is called when a dataflow registers and the pool is
/// missing connections (the first registration, or one after a connection
/// failed), at most [`SharedConnectionConfig::connections`] at a time.
///
/// # Contract
///
/// - **Pairing.** Both nodes of a pool establish their connections
///   independently: the factory must pair each connection one node
///   establishes with one the peer establishes (one side dials, the other
///   accepts and hands over the accepted stream, for example). A connection
///   whose other end no manager reads never delivers anything, and the
///   dataflows pinned to it fail their handshake.
/// - **Reliable, ordered bytes.** A stream must deliver bytes in order
///   without loss or duplication while it lives. instancy checksums every
///   frame and sequences every dataflow's lane, so a violation fails the
///   affected dataflows instead of being accepted.
/// - **Liveness is the application's.** A stream must eventually fail (an
///   error or EOF) when the peer is gone, through TCP keepalive, a TLS or
///   QUIC idle timeout, or similar; or the application reports the node
///   leaving ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
///   A stream that silently stops delivering without ever failing makes the
///   dataflows using it wait; they never report success.
///
/// instancy ships a default [`TcpConnectionFactory`] for plain TCP.
/// Applications that need other transports implement this trait directly.
///
/// Implement using `async fn` in the trait (Rust 1.75+).
pub trait ConnectionFactory: Send + Sync + 'static {
    /// The read half of a connection.
    type Reader: AsyncRead + Unpin + Send + 'static;
    /// The write half of a connection.
    type Writer: AsyncWrite + Unpin + Send + 'static;

    /// Establish a new connection to the specified peer.
    fn establish(
        &self,
        peer_node_id: &str,
    ) -> impl Future<
        Output = Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>,
    > + Send;
}

/// Resolves peer node IDs to socket addresses for TCP connections.
///
/// This trait is specific to [`TcpConnectionFactory`]. Custom connection
/// factories that use non-TCP transports (pipes, Unix sockets, QUIC, etc.)
/// handle peer resolution internally and do not need this trait.
pub trait PeerAddressResolver: Send + Sync + 'static {
    /// Resolve a peer node ID to its socket address.
    fn resolve(&self, peer_node_id: &str) -> Option<std::net::SocketAddr>;
}

impl PeerAddressResolver for std::collections::HashMap<String, std::net::SocketAddr> {
    fn resolve(&self, peer_node_id: &str) -> Option<std::net::SocketAddr> {
        self.get(peer_node_id).copied()
    }
}

/// Default connection factory using plain TCP (no TLS, no keepalive).
///
/// It sets no TCP keepalive, so a peer that vanishes without resetting the
/// connection (a network partition) is never detected through it: an
/// application using it in production should enable keepalive itself (with
/// its own factory) or rely on its membership layer and
/// [`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave).
///
/// This is a convenience implementation for applications that use plain TCP.
/// The application instantiates this with a resolver that maps peer node IDs
/// to socket addresses. For TLS, Unix sockets, named pipes, QUIC, or any
/// other byte-stream transport, implement [`ConnectionFactory`] directly.
pub struct TcpConnectionFactory {
    resolver: Arc<dyn PeerAddressResolver>,
}

impl TcpConnectionFactory {
    pub fn new(resolver: impl PeerAddressResolver) -> Self {
        Self {
            resolver: Arc::new(resolver),
        }
    }
}

impl ConnectionFactory for TcpConnectionFactory {
    type Reader = tokio::net::tcp::OwnedReadHalf;
    type Writer = tokio::net::tcp::OwnedWriteHalf;

    async fn establish(
        &self,
        peer_node_id: &str,
    ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>> {
        let addr = self.resolver.resolve(peer_node_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unknown peer: {peer_node_id}"),
            )
        })?;
        let stream = tokio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream.into_split())
    }
}

/// Type-erased reader half used by reconnect support.
type DynReader = Box<dyn AsyncRead + Unpin + Send>;
/// Type-erased writer half used by reconnect support.
type DynWriter = Box<dyn AsyncWrite + Unpin + Send>;
/// Type-erased connection establishment result.
type DynConnectionResult = Result<(DynReader, DynWriter), Box<dyn std::error::Error + Send + Sync>>;

/// Type-erased connection factory for reconnect support.
pub trait DynConnectionFactory: Send + Sync + 'static {
    /// Establish a new connection to the specified peer.
    fn establish_dyn<'a>(
        &'a self,
        peer_node_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = DynConnectionResult> + Send + 'a>>;
}

impl<F> DynConnectionFactory for F
where
    F: ConnectionFactory,
{
    fn establish_dyn<'a>(
        &'a self,
        peer_node_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = DynConnectionResult> + Send + 'a>> {
        Box::pin(async move {
            let (reader, writer) = self.establish(peer_node_id).await?;
            Ok((Box::new(reader) as DynReader, Box::new(writer) as DynWriter))
        })
    }
}

// ---------------------------------------------------------------------------
// Manager state
// ---------------------------------------------------------------------------

/// One pooled connection.
struct PooledConnection {
    metrics: Arc<ConnectionMetrics>,
    /// Queue of the connection's writer task. Dropping it lets the writer
    /// finish what is queued and close its half of the connection.
    writer: tokio_mpsc::Sender<Frame>,
    /// Registered dataflows whose lane is pinned to this connection.
    pinned: usize,
    /// The peer closed its side cleanly (EOF at a frame boundary): it sends
    /// nothing more but may still be reading, as a released manager does
    /// (see [`linger`]). The lanes pinned here keep writing until a write
    /// fails; no new lane is pinned here, and the connection leaves the pool
    /// (the next registration replaces it) and is closed once its last lane
    /// unregisters.
    peer_closed: bool,
}

/// Per-dataflow state tracked by the shared peer manager.
struct DataflowRegistration {
    /// Identifies this registration: a dataflow id that is registered again
    /// after it finished gets a new token, so the first registration's late
    /// cleanup cannot remove the second.
    token: u64,
    /// The pooled connection this dataflow's frames to the peer use.
    conn: usize,
    /// Sequence number of the next payload frame sent to the peer.
    next_send_seq: u64,
    /// Sequence number the next payload frame from the peer must carry.
    next_recv_seq: u64,
    /// The connection the peer's frames for this dataflow arrive on: set by
    /// the first one, normally the peer's handshake.
    inbound_conn: Option<usize>,
    /// Per-channel senders delivering the peer's frames. Taken (which closes
    /// the channels) once the inbound side has failed.
    channel_senders: Option<HashMap<u64, tokio_mpsc::Sender<Vec<u8>>>>,
    /// Where connection events of this dataflow are reported.
    events: tokio_mpsc::Sender<PeerIoNotice>,
    /// Set once someone hands the events on (see
    /// [`SharedTransportSession::hold_channels_for_io_events`]).
    hold_channels: Arc<AtomicBool>,
    /// Frames to the peer can no longer be sent (reported once).
    outbound_failed: bool,
    /// Frames from the peer can no longer arrive (reported once).
    inbound_failed: bool,
}

/// Control frames for one dataflow that has not registered here yet.
struct PendingFrames {
    /// Each frame, with the connection it arrived on.
    frames: Vec<(usize, Vec<u8>)>,
    since: tokio::time::Instant,
}

/// Control frames that arrive before their dataflow registers here.
///
/// The peer may register a dataflow and send its handshake before this node
/// has registered it; the frames wait here until it does. Bounded in the
/// number of dataflows, in frames per dataflow and in time, so frames for a
/// dataflow that never registers do not accumulate: such a dataflow's
/// handshake times out.
#[derive(Default)]
struct PendingControl {
    by_dataflow: HashMap<DataflowId, PendingFrames>,
    /// Insertion order, for expiry and eviction (may hold stale ids).
    order: VecDeque<DataflowId>,
}

impl PendingControl {
    /// Buffer `payload`.
    fn push(&mut self, dataflow_id: DataflowId, conn: usize, payload: Vec<u8>) {
        self.expire();
        if let Some(pending) = self.by_dataflow.get_mut(&dataflow_id) {
            if pending.frames.len() < MAX_PENDING_FRAMES_PER_DATAFLOW {
                pending.frames.push((conn, payload));
            } else {
                #[cfg(feature = "tracing")]
                tracing::warn!(%dataflow_id, "too many control frames for an unregistered dataflow; dropping one");
            }
            return;
        }
        while self.by_dataflow.len() >= MAX_PENDING_DATAFLOWS {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.by_dataflow.remove(&oldest);
        }
        self.by_dataflow.insert(
            dataflow_id,
            PendingFrames {
                frames: vec![(conn, payload)],
                since: tokio::time::Instant::now(),
            },
        );
        self.order.push_back(dataflow_id);
        if self.order.len() > 2 * MAX_PENDING_DATAFLOWS {
            let live = &self.by_dataflow;
            self.order.retain(|id| live.contains_key(id));
        }
    }

    fn take(&mut self, dataflow_id: &DataflowId) -> Option<PendingFrames> {
        self.by_dataflow.remove(dataflow_id)
    }

    /// Forget the frames that arrived on `conn` (it failed: what followed
    /// them on it is lost, so the handshake they start cannot complete).
    fn forget_connection(&mut self, conn: usize) {
        self.by_dataflow.retain(|_, pending| {
            pending.frames.retain(|(from, _)| *from != conn);
            !pending.frames.is_empty()
        });
    }

    fn expire(&mut self) {
        while let Some(front) = self.order.front() {
            match self.by_dataflow.get(front) {
                None => {
                    self.order.pop_front();
                }
                Some(pending) if pending.since.elapsed() >= PENDING_CONTROL_TTL => {
                    let id = *front;
                    self.by_dataflow.remove(&id);
                    self.order.pop_front();
                }
                Some(_) => break,
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_dataflow.len()
    }
}

/// The most recently unregistered dataflows (bounded).
#[derive(Default)]
struct RecentIds {
    set: HashSet<DataflowId>,
    order: VecDeque<DataflowId>,
}

impl RecentIds {
    fn insert(&mut self, id: DataflowId) {
        if self.set.insert(id) {
            self.order.push_back(id);
            while self.order.len() > MAX_COMPLETED_DATAFLOWS {
                if let Some(oldest) = self.order.pop_front() {
                    self.set.remove(&oldest);
                }
            }
        }
    }

    fn contains(&self, id: &DataflowId) -> bool {
        self.set.contains(id)
    }
}

#[derive(Default)]
struct State {
    /// Live pooled connections, by id.
    connections: BTreeMap<usize, PooledConnection>,
    next_conn_id: usize,
    registered: HashMap<DataflowId, DataflowRegistration>,
    pending_control: PendingControl,
    /// Dataflows that have unregistered (bounded): a [`DataflowId`]
    /// identifies one run, so a remembered id cannot register again, and
    /// the late frames of its finished run are dropped.
    completed: RecentIds,
}

/// How a connection's read side ended.
enum InboundEnd {
    /// The peer closed the connection at a frame boundary.
    Closed,
    /// Reading failed: an I/O error, EOF mid-frame, a corrupt or oversize
    /// frame. `owner` is the dataflow of the failing frame, when its header
    /// checksum was verified.
    Failed {
        error: TransportError,
        owner: Option<DataflowId>,
    },
}

impl InboundEnd {
    /// The event for `dataflow_id`, whose frames arrive on the connection.
    ///
    /// A corrupt or oversize frame is corruption only for the dataflow it
    /// belongs to. For the others sharing the connection it is the loss of
    /// the connection: fatal while they run, but not for one that already
    /// has everything it needs from the peer (a corrupt frame of another
    /// dataflow says nothing about what they received).
    fn event_for(&self, dataflow_id: DataflowId) -> PeerIoEvent {
        match self {
            Self::Closed => PeerIoEvent::Closed,
            Self::Failed {
                error:
                    error @ (TransportError::ChecksumMismatch { .. }
                    | TransportError::PayloadTooLarge { .. }),
                owner,
            } if *owner != Some(dataflow_id) => {
                PeerIoEvent::ReadFailed(TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    format!("the pooled connection failed on a corrupt frame: {error}"),
                )))
            }
            Self::Failed { error, .. } => PeerIoEvent::ReadFailed(error.duplicate()),
        }
    }
}

/// A connection event for one dataflow, delivered outside the state lock.
enum Report {
    /// An event that needs no ordering with the dataflow's channels.
    Event {
        events: tokio_mpsc::Sender<PeerIoNotice>,
        event: PeerIoEvent,
    },
    /// The dataflow's frames from the peer can no longer arrive: report it,
    /// then close its channels (once the report was handed on, if someone
    /// is handing reports on).
    InboundEnded {
        events: tokio_mpsc::Sender<PeerIoNotice>,
        event: PeerIoEvent,
        senders: Option<HashMap<u64, tokio_mpsc::Sender<Vec<u8>>>>,
        hold: Arc<AtomicBool>,
    },
}

// ---------------------------------------------------------------------------
// Inner: state shared with the background tasks
// ---------------------------------------------------------------------------

struct Inner {
    peer_node_id: String,
    config: SharedConnectionConfig,
    state: Mutex<State>,
    prober: RttProber,
    probe_counter: ProbeCounter,
    /// Factory used to establish the pool's connections.
    connection_factory: Arc<dyn DynConnectionFactory>,
    runtime_handle: tokio::runtime::Handle,
    /// Background task handles (aborted on drop, except for the connection
    /// writers and readers, which first finish what is queued).
    task_handles: TaskHandles,
    /// Serializes connection establishment.
    init_lock: TokioMutex<()>,
    /// Health event sender (connection failures).
    health_tx: tokio::sync::broadcast::Sender<crate::runtime_event::RuntimeEvent>,
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // Critical sections never panic; recover the data if one ever did.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn live_connections(&self) -> usize {
        self.state()
            .connections
            .values()
            .filter(|conn| !conn.peer_closed)
            .count()
    }

    /// Add an established connection to the pool and start its tasks.
    fn add_connection(self: &Arc<Self>, reader: DynReader, writer: DynWriter) -> usize {
        let (tx, rx) = tokio_mpsc::channel::<Frame>(WRITER_QUEUE);
        let (close_tx, close_rx) = tokio::sync::oneshot::channel();
        let (conn_id, metrics) = {
            let mut state = self.state();
            let conn_id = state.next_conn_id;
            state.next_conn_id += 1;
            let metrics = Arc::new(ConnectionMetrics::new(conn_id, self.config.rtt_ema_alpha));
            state.connections.insert(
                conn_id,
                PooledConnection {
                    metrics: Arc::clone(&metrics),
                    writer: tx,
                    pinned: 0,
                    peer_closed: false,
                },
            );
            (conn_id, metrics)
        };
        let writer_handle = self.runtime_handle.spawn(writer_task(
            Arc::downgrade(self),
            conn_id,
            writer,
            rx,
            metrics,
            close_tx,
        ));
        let reader_handle =
            self.runtime_handle
                .spawn(reader_task(Arc::clone(self), conn_id, reader, close_rx));
        push_task_handle(&self.task_handles.writers, writer_handle);
        push_task_handle(&self.task_handles.readers, reader_handle);
        conn_id
    }

    /// Sequence a payload frame of the registration `token` of
    /// `dataflow_id` and queue it on the dataflow's pinned connection.
    async fn send_payload(self: &Arc<Self>, dataflow_id: DataflowId, token: u64, mut frame: Frame) {
        // The receiver handles the control and probe channels outside the
        // lane, and a frame over the size limit cannot be written: reject
        // any of them without consuming a sequence number. Only this
        // dataflow fails; the connection is untouched.
        let rejected = if [CONTROL_CHANNEL_ID, PROBE_CHANNEL_ID].contains(&frame.channel_id) {
            Some(TransportError::ReservedChannel {
                channel_id: frame.channel_id,
            })
        } else if frame.payload.len() > MAX_MESSAGE_SIZE - SEQUENCE_PREFIX {
            Some(TransportError::PayloadTooLarge {
                size: frame.payload.len() + SEQUENCE_PREFIX,
                max: MAX_MESSAGE_SIZE,
            })
        } else {
            None
        };
        let (conn_id, writer, seq) = {
            let mut state = self.state();
            let State {
                registered,
                connections,
                ..
            } = &mut *state;
            // The registration has ended (the dataflow has torn down): its
            // frames still queued are dropped, never sequenced into another
            // registration's lane.
            let Some(reg) = registered
                .get_mut(&dataflow_id)
                .filter(|reg| reg.token == token)
            else {
                return;
            };
            if let Some(error) = rejected {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    peer = %self.peer_node_id,
                    %dataflow_id,
                    channel_id = frame.channel_id,
                    "payload frame rejected: {error}"
                );
                let _ = reg
                    .events
                    .try_send(PeerIoNotice::new(PeerIoEvent::FrameRejected(error)));
                return;
            }
            if reg.outbound_failed {
                return;
            }
            let Some(conn) = connections.get(&reg.conn) else {
                // Unreachable: a connection leaves the pool only after its
                // lanes are marked failed. Report it anyway.
                reg.outbound_failed = true;
                let _ = reg
                    .events
                    .try_send(PeerIoNotice::new(PeerIoEvent::WriteFailed(
                        TransportError::ConnectionClosed,
                    )));
                return;
            };
            let seq = reg.next_send_seq;
            reg.next_send_seq += 1;
            (reg.conn, conn.writer.clone(), seq)
        };
        let mut payload = Vec::with_capacity(SEQUENCE_PREFIX + frame.payload.len());
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.extend_from_slice(&frame.payload);
        frame.payload = payload;
        frame.dataflow_id = dataflow_id;
        if writer.send(frame).await.is_err() {
            // The writer stopped: its connection failed, and this frame is lost.
            self.fail_outbound(conn_id, TransportError::ConnectionClosed);
        }
    }

    /// Queue a control frame on its dataflow's pinned connection (any
    /// connection for a dataflow not registered here).
    async fn send_control(self: &Arc<Self>, frame: Frame) {
        let target = {
            let state = self.state();
            match state.registered.get(&frame.dataflow_id) {
                // Its lane failed and the dataflow was told.
                Some(reg) if reg.outbound_failed => None,
                Some(reg) => state
                    .connections
                    .get(&reg.conn)
                    .map(|conn| (reg.conn, conn.writer.clone())),
                // A dataflow that failed before registering here, or has
                // unregistered (a late Cancel): any pool connection will do.
                None => state
                    .connections
                    .iter()
                    .find(|(_, conn)| !conn.peer_closed)
                    .map(|(id, conn)| (*id, conn.writer.clone())),
            }
        };
        let Some((conn_id, writer)) = target else {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                peer = %self.peer_node_id,
                dataflow_id = %frame.dataflow_id,
                "no connection for a control frame; dropping it"
            );
            return;
        };
        if writer.send(frame).await.is_err() {
            self.fail_outbound(conn_id, TransportError::ConnectionClosed);
        }
    }

    /// Handle a frame read from connection `conn_id`.
    async fn on_frame(self: &Arc<Self>, conn_id: usize, frame: Frame) {
        if frame.channel_id == PROBE_CHANNEL_ID {
            self.on_probe(conn_id, &frame);
            return;
        }
        let dataflow_id = frame.dataflow_id;
        let mut reports = Vec::new();
        let delivery = {
            let mut state = self.state();
            let State {
                registered,
                pending_control,
                completed,
                ..
            } = &mut *state;
            let Some(reg) = registered.get_mut(&dataflow_id) else {
                // Control frames may arrive before their dataflow registers
                // here (the peer was faster): they wait, boundedly (see
                // `PendingControl`). A dataflow that finished here cannot
                // register again (a `DataflowId` identifies one run): its
                // late frames are dropped. A payload frame for a dataflow
                // not registered here is late too: the peer sends payload
                // only after this node's handshake, so it has torn down.
                if frame.channel_id == CONTROL_CHANNEL_ID && !completed.contains(&dataflow_id) {
                    pending_control.push(dataflow_id, conn_id, frame.payload);
                }
                return;
            };
            if reg.inbound_failed {
                return;
            }
            let first_conn = *reg.inbound_conn.get_or_insert(conn_id);
            if frame.channel_id == CONTROL_CHANNEL_ID {
                // Outside the lane's numbering: a Cancel may overtake payload,
                // and a dataflow that failed before registering on the peer
                // sends its Cancel on any connection.
                reg.channel_senders
                    .as_ref()
                    .and_then(|senders| senders.get(&CONTROL_CHANNEL_ID))
                    .cloned()
                    .map(|tx| (tx, frame.payload))
            } else {
                let checked = Self::check_lane(reg, first_conn, conn_id, frame);
                match checked {
                    Ok(delivery) => delivery,
                    Err(violation) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            peer = %self.peer_node_id,
                            %dataflow_id,
                            "frames from the peer cannot be trusted: {violation}"
                        );
                        reg.inbound_failed = true;
                        reports.push(Report::InboundEnded {
                            events: reg.events.clone(),
                            event: PeerIoEvent::ReadFailed(violation),
                            senders: reg.channel_senders.take(),
                            hold: Arc::clone(&reg.hold_channels),
                        });
                        None
                    }
                }
            }
        };
        self.deliver(reports);
        if let Some((tx, payload)) = delivery {
            // A dataflow whose receiver is gone is tearing down.
            let _ = tx.send(payload).await;
        }
    }

    /// Check a payload frame against its lane: it must arrive on the lane's
    /// connection, carry exactly the next sequence number, and be addressed
    /// to a registered channel. Returns the channel and the payload without
    /// its sequence number.
    #[allow(clippy::type_complexity)]
    fn check_lane(
        reg: &mut DataflowRegistration,
        lane_conn: usize,
        conn_id: usize,
        frame: Frame,
    ) -> Result<Option<(tokio_mpsc::Sender<Vec<u8>>, Vec<u8>)>, TransportError> {
        if lane_conn != conn_id {
            return Err(TransportError::LaneViolation {
                detail: format!(
                    "frames arrived on two connections ({lane_conn} and {conn_id}): \
                     their order is lost"
                ),
            });
        }
        let seq = wire::read_u64(&frame.payload, 0).map_err(|_| TransportError::LaneViolation {
            detail: "a payload frame without a sequence number".into(),
        })?;
        if seq != reg.next_recv_seq {
            return Err(TransportError::OutOfSequence {
                expected: reg.next_recv_seq,
                received: seq,
            });
        }
        reg.next_recv_seq += 1;
        let Some(senders) = reg.channel_senders.as_ref() else {
            return Ok(None);
        };
        let Some(tx) = senders.get(&frame.channel_id) else {
            return Err(TransportError::LaneViolation {
                detail: format!("a frame for unregistered channel {}", frame.channel_id),
            });
        };
        let mut payload = frame.payload;
        payload.drain(..SEQUENCE_PREFIX);
        Ok(Some((tx.clone(), payload)))
    }

    /// Answer a probe request, or record the RTT of a probe reply.
    fn on_probe(&self, conn_id: usize, frame: &Frame) {
        let Some(probe) = ProbeMessage::decode(&frame.payload) else {
            return;
        };
        let state = self.state();
        let Some(conn) = state.connections.get(&conn_id) else {
            return;
        };
        match probe.kind {
            ProbeKind::Reply => {
                self.prober.process_probe_reply(&probe, &conn.metrics);
            }
            ProbeKind::Request => {
                let reply = Frame {
                    dataflow_id: DataflowId::nil(),
                    channel_id: PROBE_CHANNEL_ID,
                    payload: ProbeMessage::reply_to(&probe).encode().to_vec(),
                };
                // Best effort: a full queue already shows the RTT.
                let _ = conn.writer.try_send(reply);
            }
        }
    }

    /// Writing to connection `conn_id` failed: the frames queued on it are
    /// lost. Fail every dataflow whose lane is pinned to it, and stop using
    /// it. Its reader is stopped next, which closes the whole connection
    /// (see [`writer_task`]) and fails the dataflows reading from it (see
    /// [`Self::fail_inbound`]).
    fn fail_outbound(self: &Arc<Self>, conn_id: usize, error: TransportError) {
        let reports = {
            let mut state = self.state();
            let removed = state.connections.remove(&conn_id).is_some();
            let mut reports = Vec::new();
            for reg in state.registered.values_mut() {
                if reg.conn == conn_id && !reg.outbound_failed {
                    reg.outbound_failed = true;
                    reports.push(Report::Event {
                        events: reg.events.clone(),
                        event: PeerIoEvent::WriteFailed(error.duplicate()),
                    });
                }
            }
            if removed || !reports.is_empty() {
                self.report_health(conn_id, &format!("writing failed: {error}"));
            }
            reports
        };
        self.deliver(reports);
    }

    /// Connection `conn_id`'s read side ended. Frames the peer sent on it
    /// after the last one read are lost: fail every dataflow whose frames
    /// from the peer arrive on it (closing its channels once that was
    /// reported).
    ///
    /// A read error (an I/O error, EOF mid-frame, a corrupt or oversize
    /// frame, or the writer's failure) breaks the whole connection: the
    /// frames queued on it may be lost too, so the dataflows whose lane is
    /// pinned to it fail as well, and it leaves the pool. A clean EOF is the
    /// peer closing its side only: it may still read (a released manager
    /// lingers for the last frames of its peers, see [`linger`]), so the
    /// lanes pinned here are told but keep writing until a write fails, and
    /// the connection only stops taking new lanes (see
    /// [`PooledConnection::peer_closed`]).
    fn fail_inbound(self: &Arc<Self>, conn_id: usize, end: InboundEnd) {
        let clean = matches!(end, InboundEnd::Closed);
        let reports = {
            let mut state = self.state();
            match state.connections.get_mut(&conn_id) {
                Some(conn) if clean && conn.pinned > 0 => conn.peer_closed = true,
                _ => {
                    state.connections.remove(&conn_id);
                }
            }
            state.pending_control.forget_connection(conn_id);
            let mut reports = Vec::new();
            for (&dataflow_id, reg) in state.registered.iter_mut() {
                if reg.inbound_conn == Some(conn_id) && !reg.inbound_failed {
                    reg.inbound_failed = true;
                    reports.push(Report::InboundEnded {
                        events: reg.events.clone(),
                        event: end.event_for(dataflow_id),
                        senders: reg.channel_senders.take(),
                        hold: Arc::clone(&reg.hold_channels),
                    });
                    if clean {
                        continue;
                    }
                }
                if reg.conn != conn_id || reg.outbound_failed {
                    continue;
                }
                if clean {
                    // Told once (above if its frames from the peer arrive
                    // here too); its frames still go out.
                    reports.push(Report::Event {
                        events: reg.events.clone(),
                        event: PeerIoEvent::Closed,
                    });
                } else {
                    reg.outbound_failed = true;
                    reports.push(Report::Event {
                        events: reg.events.clone(),
                        event: PeerIoEvent::WriteFailed(TransportError::ConnectionClosed),
                    });
                }
            }
            reports
        };
        match &end {
            InboundEnd::Closed if reports.is_empty() => {
                #[cfg(feature = "tracing")]
                tracing::debug!(peer = %self.peer_node_id, conn_id, "pooled connection closed");
            }
            InboundEnd::Closed => {
                self.report_health(conn_id, "the peer closed the connection");
            }
            InboundEnd::Failed { error, .. } => {
                self.report_health(conn_id, &format!("reading failed: {error}"));
            }
        }
        self.deliver(reports);
    }

    fn report_health(&self, conn_id: usize, what: &str) {
        let detail = format!("pooled connection {conn_id} failed: {what}");
        #[cfg(feature = "tracing")]
        tracing::warn!(peer = %self.peer_node_id, "{detail}");
        let _ = self
            .health_tx
            .send(crate::runtime_event::RuntimeEvent::TransportDegraded {
                peer_id: self.peer_node_id.clone(),
                detail,
            });
    }

    /// Deliver connection events to their dataflows.
    fn deliver(&self, reports: Vec<Report>) {
        for report in reports {
            match report {
                Report::Event { events, event } => {
                    // A full queue already carries a failure for the dataflow.
                    let _ = events.try_send(PeerIoNotice::new(event));
                }
                Report::InboundEnded {
                    events,
                    event,
                    senders,
                    hold,
                } => {
                    if hold.load(Ordering::Acquire) {
                        // Hand the event on before the channels close (see
                        // `PeerIoNotice`). A dropped receiver resolves both waits.
                        self.runtime_handle.spawn(async move {
                            let (notice, handed_on) = PeerIoNotice::held(event);
                            if events.send(notice).await.is_ok() {
                                let _ = handed_on.await;
                            }
                            drop(senders);
                        });
                    } else {
                        let _ = events.try_send(PeerIoNotice::new(event));
                        drop(senders);
                    }
                }
            }
        }
    }

    /// Remove `dataflow_id`'s registration (only `token`'s, if given).
    fn unregister(&self, dataflow_id: &DataflowId, token: Option<u64>) {
        let mut state = self.state();
        let State {
            registered,
            connections,
            pending_control,
            completed,
            ..
        } = &mut *state;
        match registered.get(dataflow_id) {
            Some(reg) if token.is_none_or(|token| token == reg.token) => {
                let conn = reg.conn;
                registered.remove(dataflow_id);
                if let Some(pooled) = connections.get_mut(&conn) {
                    pooled.pinned = pooled.pinned.saturating_sub(1);
                    // The peer closed its side and the last lane on it is
                    // done: close ours (the writer finishes what is queued).
                    if pooled.peer_closed && pooled.pinned == 0 {
                        connections.remove(&conn);
                    }
                }
                completed.insert(*dataflow_id);
            }
            // Another registration of the same id (`token` is not
            // registered): neither it nor the control frames waiting for a
            // later registration are this caller's to remove.
            Some(_) => return,
            None if token.is_some() => return,
            None => {}
        }
        pending_control.take(dataflow_id);
    }
}

/// A random registration token (see [`DataflowRegistration::token`]).
pub(crate) fn new_registration_token() -> u64 {
    uuid::Uuid::new_v4().as_u64_pair().0
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// Per-connection writer task: writes queued frames to the connection.
///
/// On a write error the frames queued on the connection are lost: it fails
/// the dataflows pinned to the connection, then has the reader stop
/// (`close`), so both halves are dropped and the whole connection closes.
/// Dropping only the write half would tell the peer nothing with halves
/// such as `tokio::io::split`'s (TLS streams, pipes): the peer would wait
/// for frames that never come, and keep the connection in its pool while
/// this side replaces it. With the connection closed, the peer fails the
/// same dataflows and both pools refill together.
async fn writer_task(
    inner: std::sync::Weak<Inner>,
    conn_id: usize,
    writer: DynWriter,
    mut rx: tokio_mpsc::Receiver<Frame>,
    metrics: Arc<ConnectionMetrics>,
    close: tokio::sync::oneshot::Sender<TransportError>,
) {
    let mut framed = FramedWriter::new(writer);
    while let Some(frame) = rx.recv().await {
        let payload_size = frame.payload.len();
        let is_user_traffic = frame.channel_id != PROBE_CHANNEL_ID;
        if let Err(error) = framed.write_frame(&frame).await {
            let detail = error.to_string();
            if let Some(inner) = inner.upgrade() {
                inner.fail_outbound(conn_id, error);
            }
            drop(framed);
            let _ = close.send(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("the pooled connection was closed after writing to it failed: {detail}"),
            )));
            return;
        }
        metrics.record_write(payload_size, is_user_traffic);
    }
    // Nothing more will be written: tell the peer. Only some write halves
    // do that when dropped (`OwnedWriteHalf`); with others (TLS streams,
    // pipes, `tokio::io::split`) the peer would not see the end of the
    // stream until the whole connection is dropped. The reader keeps going
    // (`close` is dropped unsent) until the peer closes its side.
    let _ = tokio::time::timeout(WRITER_DRAIN_GRACE, framed.into_inner().shutdown()).await;
}

/// Per-connection reader task: reads frames and hands them to
/// [`Inner::on_frame`] until the connection ends, or until the writer
/// failed and sends `close`.
async fn reader_task(
    inner: Arc<Inner>,
    conn_id: usize,
    reader: DynReader,
    close: tokio::sync::oneshot::Receiver<TransportError>,
) {
    let mut framed = FramedReader::new(reader);
    let reading = async {
        loop {
            match framed.read_frame().await {
                Ok(frame) => inner.on_frame(conn_id, frame).await,
                Err(TransportError::ConnectionClosed) => break InboundEnd::Closed,
                Err(error) => {
                    break InboundEnd::Failed {
                        error,
                        owner: framed.verified_dataflow(),
                    };
                }
            }
        }
    };
    // A frame cut off by `close` belongs to a lane on this connection,
    // which `fail_inbound` fails. A `close` dropped unsent (the writer
    // finished normally) disables its branch; reading goes on. `close` is
    // polled first: under sustained traffic `reading` uses up the task's
    // cooperative budget, and a `close` polled after it would never fire.
    let end = tokio::select! {
        biased;
        Ok(error) = close => InboundEnd::Failed { error, owner: None },
        end = reading => end,
    };
    inner.fail_inbound(conn_id, end);
}

/// Bridge task: routes control frames (with priority) and payload frames to
/// their dataflows' pinned connections, sequencing the payload frames, and
/// wakes flush barriers.
async fn bridge_task(
    inner: Arc<Inner>,
    mut payload_rx: tokio_mpsc::Receiver<Outbound>,
    mut control_rx: tokio_mpsc::Receiver<Frame>,
) {
    let mut control_open = true;
    let mut payload_open = true;
    while control_open || payload_open {
        tokio::select! {
            biased;

            result = control_rx.recv(), if control_open => match result {
                Some(frame) => inner.send_control(frame).await,
                None => control_open = false,
            },

            result = payload_rx.recv(), if payload_open => match result {
                Some(Outbound::Payload { dataflow_id, token, frame }) => {
                    inner.send_payload(dataflow_id, token, frame).await;
                }
                // Every entry queued before the barrier has been processed.
                Some(Outbound::Barrier(waiter)) => {
                    let _ = waiter.send(());
                }
                None => payload_open = false,
            },
        }
    }
}

/// Periodic probe loop: sends an RTT probe on each connection.
async fn probe_loop(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(inner.config.probe_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first probe goes out after one full interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let writers: Vec<_> = inner
            .state()
            .connections
            .values()
            .filter(|conn| !conn.peer_closed)
            .map(|conn| conn.writer.clone())
            .collect();
        for writer in writers {
            let seq = inner.probe_counter.next_seq();
            let send_ts = inner.prober.timestamp().now_nanos();
            inner.prober.record_probe_sent(seq, send_ts);
            let frame = Frame {
                dataflow_id: DataflowId::nil(),
                channel_id: PROBE_CHANNEL_ID,
                payload: ProbeMessage::new_request(seq, send_ts).encode().to_vec(),
            };
            // Best effort: a full queue already shows the RTT.
            let _ = writer.try_send(frame);
        }
    }
}

// ---------------------------------------------------------------------------
// SharedPeerManager — owns pooled connections to one peer
// ---------------------------------------------------------------------------

/// Manages a fixed pool of shared connections to a single remote peer.
///
/// Many dataflows register with the manager and share its connections; each
/// dataflow's frames to the peer are pinned to one of them (see the
/// [module documentation](self)). The connections are established through
/// the application's [`ConnectionFactory`] when the first dataflow
/// registers, and a failed connection is replaced when a later dataflow
/// registers.
pub struct SharedPeerManager {
    inner: Arc<Inner>,
    /// Payload queue: frames from every dataflow (and flush barriers), for
    /// sequencing and routing by the bridge task.
    payload_tx: tokio_mpsc::Sender<Outbound>,
    /// Control sender: high-priority frames bypass sequencing.
    control_tx: tokio_mpsc::Sender<Frame>,
}

impl Drop for SharedPeerManager {
    fn drop(&mut self) {
        self.inner.task_handles.abort_tasks();
        // Close the connections gracefully in the background (if the runtime
        // is shutting down, it ends these tasks itself): dropping the writer
        // queues lets the writers finish what is queued on them.
        let writers = TaskHandles::take(&self.inner.task_handles.writers);
        let readers = TaskHandles::take(&self.inner.task_handles.readers);
        self.inner.state().connections.clear();
        if !writers.is_empty() || !readers.is_empty() {
            let deadline = tokio::time::Instant::now() + WRITER_DRAIN_GRACE;
            self.inner
                .runtime_handle
                .spawn(linger(writers, readers, deadline));
        }
    }
}

impl SharedPeerManager {
    /// Create a new shared peer manager.
    ///
    /// Connections are established lazily through `connection_factory` when
    /// the first dataflow registers.
    ///
    /// # Errors
    ///
    /// [`RuntimeError::InvalidConfig`](crate::error::RuntimeError::InvalidConfig)
    /// if `config` is invalid (see [`SharedConnectionConfig::validate`]).
    pub fn new(
        peer_node_id: String,
        config: SharedConnectionConfig,
        connection_factory: Arc<dyn DynConnectionFactory>,
        runtime_handle: &tokio::runtime::Handle,
        health_tx: tokio::sync::broadcast::Sender<crate::runtime_event::RuntimeEvent>,
    ) -> crate::Result<Self> {
        config.validate()?;
        let inner = Arc::new(Inner {
            peer_node_id,
            config,
            state: Mutex::new(State::default()),
            prober: RttProber::new(),
            probe_counter: ProbeCounter::new(),
            connection_factory,
            runtime_handle: runtime_handle.clone(),
            task_handles: TaskHandles::default(),
            init_lock: TokioMutex::new(()),
            health_tx,
        });

        // Payload queue: dataflow tasks send their frames here.
        let (payload_tx, payload_rx) = tokio_mpsc::channel::<Outbound>(1024);
        // Control channel: high-priority frames
        let (control_tx, control_rx) = tokio_mpsc::channel::<Frame>(256);

        let bridge = runtime_handle.spawn(bridge_task(Arc::clone(&inner), payload_rx, control_rx));
        push_task_handle(&inner.task_handles.tasks, bridge);
        let probes = runtime_handle.spawn(probe_loop(Arc::clone(&inner)));
        push_task_handle(&inner.task_handles.tasks, probes);

        Ok(Self {
            inner,
            payload_tx,
            control_tx,
        })
    }

    /// Register a dataflow and its channels with this peer manager.
    ///
    /// A control channel (ID 0) is **automatically registered** for each
    /// dataflow, matching `TransportSession` behavior. Returns per-channel
    /// receivers for incoming frames and the dataflow's connection events
    /// (see [`PeerIoEvent`]): a pooled connection that fails, a frame from the
    /// peer out of sequence, or one of the dataflow's frames rejected.
    ///
    /// First establishes the pool's connections if any are missing. The
    /// dataflow's lane is then pinned to the connection with the fewest
    /// lanes. Control frames that arrived before registration are delivered
    /// to the control channel. Send the dataflow's payload frames through
    /// [`Self::dataflow_sender`].
    ///
    /// A [`DataflowId`] identifies one run: a dataflow id that finished
    /// here cannot be registered again (a retry needs a fresh id).
    ///
    /// # Errors
    ///
    /// - [`TransportError::InvalidRegistration`], without registering
    ///   anything, if `channel_capacity` is 0, `channel_ids` contains a
    ///   duplicate or [`PROBE_CHANNEL_ID`], or `dataflow_id` is registered
    ///   with this manager or finished here (the manager remembers the most
    ///   recent 16,384 finished ids).
    /// - [`TransportError::Io`] if a missing connection could not be
    ///   established within [`SharedConnectionConfig::connect_timeout`].
    pub async fn register_dataflow(
        &self,
        dataflow_id: DataflowId,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> Result<
        (
            HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>,
            tokio_mpsc::Receiver<PeerIoNotice>,
        ),
        TransportError,
    > {
        self.register(
            dataflow_id,
            new_registration_token(),
            channel_ids,
            channel_capacity,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    /// [`Self::register_dataflow`] with the registration's token (see
    /// [`Self::unregister_registration`]) and the flag that holds the
    /// channels open until a read-side event was handed on.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn register(
        &self,
        dataflow_id: DataflowId,
        token: u64,
        channel_ids: &[u64],
        channel_capacity: usize,
        hold_channels: Arc<AtomicBool>,
    ) -> Result<
        (
            HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>,
            tokio_mpsc::Receiver<PeerIoNotice>,
        ),
        TransportError,
    > {
        let invalid = |reason: String| TransportError::InvalidRegistration { reason };
        if channel_capacity == 0 {
            return Err(invalid("channel capacity must be at least 1".into()));
        }
        let mut seen = HashSet::with_capacity(channel_ids.len());
        for &ch_id in channel_ids {
            if ch_id == PROBE_CHANNEL_ID {
                return Err(invalid(format!(
                    "channel {PROBE_CHANNEL_ID} is reserved for connection probes"
                )));
            }
            if !seen.insert(ch_id) {
                return Err(invalid(format!("channel {ch_id} registered twice")));
            }
        }
        let peer = &self.inner.peer_node_id;
        let unavailable = |state: &State| {
            if state.registered.contains_key(&dataflow_id) {
                Some(invalid(format!(
                    "dataflow {dataflow_id} is already registered with peer {peer}"
                )))
            } else if state.completed.contains(&dataflow_id) {
                Some(invalid(format!(
                    "dataflow {dataflow_id} already ran with peer {peer}: a DataflowId \
                     identifies one run, so a retry needs a fresh DataflowId"
                )))
            } else {
                None
            }
        };
        if let Some(error) = unavailable(&self.inner.state()) {
            return Err(error);
        }

        self.ensure_connections().await?;

        let mut receivers = HashMap::new();
        let mut channel_senders = HashMap::new();
        // The control channel also receives the control frames that arrived
        // before registration, so it holds at least as many as are buffered.
        let control_capacity = channel_capacity.max(MAX_PENDING_FRAMES_PER_DATAFLOW);
        let (tx, rx) = tokio_mpsc::channel(control_capacity);
        channel_senders.insert(CONTROL_CHANNEL_ID, tx);
        receivers.insert(CONTROL_CHANNEL_ID, rx);
        for &ch_id in channel_ids.iter().filter(|&&id| id != CONTROL_CHANNEL_ID) {
            let (tx, rx) = tokio_mpsc::channel(channel_capacity);
            channel_senders.insert(ch_id, tx);
            receivers.insert(ch_id, rx);
        }
        // Rare events; a full queue already carries a failure.
        let (events_tx, events_rx) = tokio_mpsc::channel(8);

        let mut state = self.inner.state();
        // Re-check: a concurrent registration of the same dataflow may have
        // completed (or finished) while connections were established.
        if let Some(error) = unavailable(&state) {
            return Err(error);
        }
        let State {
            connections,
            registered,
            pending_control,
            ..
        } = &mut *state;
        // Pin the lane to the pool connection with the fewest lanes (a
        // connection may have failed, or its peer closed it, since the pool
        // was filled).
        let Some((&conn_id, conn)) = connections
            .iter_mut()
            .filter(|(_, conn)| !conn.peer_closed)
            .min_by_key(|(id, conn)| (conn.pinned, **id))
        else {
            return Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!(
                    "no connection to peer {} survived registration",
                    self.inner.peer_node_id
                ),
            )));
        };
        conn.pinned += 1;
        let mut reg = DataflowRegistration {
            token,
            conn: conn_id,
            next_send_seq: 0,
            next_recv_seq: 0,
            inbound_conn: None,
            channel_senders: Some(channel_senders),
            events: events_tx,
            hold_channels,
            outbound_failed: false,
            inbound_failed: false,
        };
        // Deliver control frames that arrived before registration (the
        // channel holds them all, see above).
        let pending = pending_control
            .take(&dataflow_id)
            .map(|pending| pending.frames)
            .unwrap_or_default();
        if let Some(&(conn, _)) = pending.first() {
            reg.inbound_conn = Some(conn);
        }
        if let Some(tx) = reg
            .channel_senders
            .as_ref()
            .and_then(|senders| senders.get(&CONTROL_CHANNEL_ID))
        {
            for (_, payload) in pending {
                let _ = tx.try_send(payload);
            }
        }
        registered.insert(dataflow_id, reg);
        drop(state);

        Ok((receivers, events_rx))
    }

    /// Establish the pool's missing connections.
    async fn ensure_connections(&self) -> Result<(), TransportError> {
        let wanted = self.inner.config.connections;
        if self.inner.live_connections() >= wanted {
            return Ok(());
        }
        let _guard = self.inner.init_lock.lock().await;
        while self.inner.live_connections() < wanted {
            let peer = &self.inner.peer_node_id;
            let established = tokio::time::timeout(
                self.inner.config.connect_timeout,
                self.inner.connection_factory.establish_dyn(peer),
            )
            .await;
            match established {
                Ok(Ok((reader, writer))) => {
                    self.inner.add_connection(reader, writer);
                }
                Ok(Err(error)) => {
                    return Err(TransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        format!("could not connect to peer {peer}: {error}"),
                    )));
                }
                Err(_) => {
                    return Err(TransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "connecting to peer {peer} took longer than {:?}",
                            self.inner.config.connect_timeout
                        ),
                    )));
                }
            }
        }
        Ok(())
    }

    /// Wait until every payload frame queued before this call has been
    /// processed by the bridge task (sequenced and handed to a connection
    /// writer, or dropped because its dataflow has failed or unregistered).
    ///
    /// Unregistering a dataflow drops its payload frames that are still
    /// queued, so a dataflow that wants its last frames delivered calls this
    /// first. The barrier waits behind every dataflow's frames queued before
    /// it; callers should bound the wait.
    pub(crate) async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.payload_tx.send(Outbound::Barrier(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    /// Release this manager once everything already queued on it has been
    /// written to its connections, bounded by `timeout`.
    ///
    /// Dropping a manager also closes its connections gracefully in the
    /// background (see [`linger`]); this waits until the writers are done, so
    /// the caller knows its frames have been handed to the connections when
    /// it returns (unless the timeout expired, in which case the remaining
    /// writers are aborted). The readers keep waiting for the peer to close
    /// in the background. Control frames are flushed too: the bridge task
    /// prefers them over the payload barrier.
    pub(crate) async fn close(self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = tokio::time::timeout_at(deadline, self.flush()).await;
        self.inner.task_handles.abort_tasks();
        let writers = TaskHandles::take(&self.inner.task_handles.writers);
        let readers = TaskHandles::take(&self.inner.task_handles.readers);
        self.inner.state().connections.clear();
        let runtime = self.inner.runtime_handle.clone();
        drop(self);
        drain_tasks(writers, deadline).await;
        runtime.spawn(linger(
            Vec::new(),
            readers,
            tokio::time::Instant::now() + WRITER_DRAIN_GRACE,
        ));
    }

    /// Unregister a dataflow, removing its channels and pending control
    /// frames; its payload frames still queued are dropped. The id is
    /// remembered (boundedly) as finished: it cannot be registered again,
    /// and the late frames of this run are dropped.
    pub async fn unregister_dataflow(&self, dataflow_id: &DataflowId) {
        self.inner.unregister(dataflow_id, None);
    }

    /// Unregister `dataflow_id` only if it is still the registration made
    /// with `token`: a cleanup must not remove another registration of the
    /// same id.
    pub(crate) async fn unregister_registration(&self, dataflow_id: &DataflowId, token: u64) {
        self.inner.unregister(dataflow_id, Some(token));
    }

    /// A sender for `dataflow_id`'s payload frames (data and progress),
    /// bound to its current registration: frames it sends after that
    /// registration ends are dropped. `None` if the dataflow is not
    /// registered.
    pub fn dataflow_sender(&self, dataflow_id: &DataflowId) -> Option<DataframeSender> {
        let token = self.inner.state().registered.get(dataflow_id)?.token;
        Some(DataframeSender {
            dataflow_id: *dataflow_id,
            token,
            tx: self.payload_tx.clone(),
        })
    }

    /// Get the control sender for submitting high-priority frames.
    pub fn control_sender(&self) -> &tokio_mpsc::Sender<Frame> {
        &self.control_tx
    }

    /// Send a control frame, first establishing the pool's connections if
    /// there are none yet (connections are otherwise made when a dataflow
    /// registers). Lets a node tell a peer about a dataflow that failed
    /// before registering here.
    pub(crate) async fn send_control_connected(&self, frame: Frame) {
        let _ = self.ensure_connections().await;
        let _ = self.control_tx.send(frame).await;
    }

    /// Get the peer node ID.
    pub fn peer_node_id(&self) -> &str {
        &self.inner.peer_node_id
    }

    /// Number of live pooled connections (not counting one whose peer closed
    /// its side while lanes pinned to it finish writing).
    pub fn connection_count(&self) -> usize {
        self.inner.live_connections()
    }

    /// Diagnostic snapshot of every live pooled connection, by id.
    pub fn metrics_snapshot(&self) -> Vec<ConnectionSnapshot> {
        self.inner
            .state()
            .connections
            .values()
            .filter(|conn| !conn.peer_closed)
            .map(|conn| ConnectionSnapshot {
                id: conn.metrics.id,
                queued_frames: conn.writer.max_capacity() - conn.writer.capacity(),
                pinned_lanes: conn.pinned,
                avg_rtt: conn.metrics.average_rtt(),
                total_bytes: conn.metrics.total_bytes_written(),
                total_frames: conn.metrics.total_frames_written(),
                idle_duration: conn.metrics.idle_duration(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// SharedTransportSession — per-dataflow handle
// ---------------------------------------------------------------------------

/// A per-dataflow transport session backed by shared, pooled connections.
///
/// This is a lightweight handle that references a [`SharedPeerManager`] for
/// each peer. It provides the same API surface as [`TransportSession`](super::transport_session::TransportSession):
/// `data_sender()`, `progress_sender()`, and `control_sender()`.
///
/// # Ordering
///
/// Data and progress frames share a single sequenced payload lane per peer,
/// pinned to one pooled connection, preserving the timely invariant that
/// data at time T arrives before the progress message releasing T.
pub struct SharedTransportSession {
    /// The dataflow this session belongs to.
    dataflow_id: DataflowId,
    /// Per-peer payload senders (data + progress share this for FIFO
    /// ordering), bound to this session's registrations.
    payload_senders: HashMap<String, DataframeSender>,
    /// Per-peer control senders (high priority, bypass sequencing).
    control_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Per-peer channel receivers (peer → channel_id → receiver).
    /// Wrapped in Option so they can be taken out once.
    receivers: Option<HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>>,
    /// Per-peer connection events.
    io_events: Option<HashMap<String, tokio_mpsc::Receiver<PeerIoNotice>>>,
    /// Set by [`Self::hold_channels_for_io_events`].
    hold_channels: Arc<AtomicBool>,
}

impl SharedTransportSession {
    /// Create a new shared transport session by registering with peer managers.
    ///
    /// # Arguments
    /// - `dataflow_id`: Identifies this dataflow
    /// - `peer_managers`: Map of peer_node_id → SharedPeerManager
    /// - `channel_ids`: Channel IDs to register for each peer
    /// - `channel_capacity`: Buffer capacity for per-channel receivers
    ///
    /// # Errors
    ///
    /// Returns the first [`SharedPeerManager::register_dataflow`] error. Peers
    /// that were already registered by this call are unregistered again.
    pub async fn new(
        dataflow_id: DataflowId,
        peer_managers: &HashMap<String, SharedPeerManager>,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> Result<Self, TransportError> {
        Self::with_token(
            dataflow_id,
            new_registration_token(),
            peer_managers,
            channel_ids,
            channel_capacity,
        )
        .await
    }

    /// [`Self::new`] registering with `token` (see
    /// [`SharedPeerManager::unregister_registration`]).
    pub(crate) async fn with_token(
        dataflow_id: DataflowId,
        token: u64,
        peer_managers: &HashMap<String, SharedPeerManager>,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> Result<Self, TransportError> {
        let hold_channels = Arc::new(AtomicBool::new(false));
        let mut payload_senders = HashMap::new();
        let mut control_senders = HashMap::new();
        let mut all_receivers = HashMap::new();
        let mut io_events = HashMap::new();

        for (peer_id, manager) in peer_managers {
            let (receivers, events) = match manager
                .register(
                    dataflow_id,
                    token,
                    channel_ids,
                    channel_capacity,
                    Arc::clone(&hold_channels),
                )
                .await
            {
                Ok(registered) => registered,
                Err(e) => {
                    for registered_peer in all_receivers.keys() {
                        if let Some(m) = peer_managers.get::<String>(registered_peer) {
                            m.unregister_registration(&dataflow_id, token).await;
                        }
                    }
                    return Err(e);
                }
            };

            payload_senders.insert(
                peer_id.clone(),
                DataframeSender {
                    dataflow_id,
                    token,
                    tx: manager.payload_tx.clone(),
                },
            );
            control_senders.insert(peer_id.clone(), manager.control_sender().clone());
            all_receivers.insert(peer_id.clone(), receivers);
            io_events.insert(peer_id.clone(), events);
        }

        Ok(Self {
            dataflow_id,
            payload_senders,
            control_senders,
            receivers: Some(all_receivers),
            io_events: Some(io_events),
            hold_channels,
        })
    }

    /// Get a data sender for a peer.
    ///
    /// Returns the shared payload sender. Data and progress share the same
    /// sequenced lane to preserve the timely ordering invariant.
    pub fn data_sender(&self, peer_node_id: &str) -> Option<DataframeSender> {
        self.payload_senders.get(peer_node_id).cloned()
    }

    /// Get a progress sender for a peer.
    ///
    /// Returns the same shared payload sender as [`data_sender`](Self::data_sender).
    pub fn progress_sender(&self, peer_node_id: &str) -> Option<DataframeSender> {
        self.data_sender(peer_node_id)
    }

    /// Get a control-priority sender for a peer.
    ///
    /// Control frames bypass sequencing and have highest priority.
    pub fn control_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.control_senders.get(peer_node_id)
    }

    /// Take the per-peer channel receivers (can only be called once).
    pub fn take_receivers(
        &mut self,
    ) -> Option<HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>> {
        self.receivers.take()
    }

    /// Take the per-peer connection events (can only be called once).
    ///
    /// Each event tells this dataflow that its link with a peer failed (a
    /// pooled connection it uses failed, a frame from the peer was out of
    /// sequence, or one of its frames was rejected). Call
    /// [`Self::hold_channels_for_io_events`] once they are being consumed.
    pub fn take_io_events(
        &mut self,
    ) -> Option<HashMap<String, tokio_mpsc::Receiver<PeerIoNotice>>> {
        self.io_events.take()
    }

    /// Call once the taken io events are being consumed (each notice dropped
    /// after its event was handed on): from then on a peer's channels stay
    /// open until its read-side notice was handed on (see [`PeerIoNotice`]).
    pub fn hold_channels_for_io_events(&self) {
        self.hold_channels.store(true, Ordering::Release);
    }

    /// Returns the set of peer node IDs this session has connections to.
    pub fn peer_node_ids(&self) -> impl Iterator<Item = &str> {
        self.payload_senders.keys().map(|s| s.as_str())
    }

    /// Get the dataflow ID.
    pub fn dataflow_id(&self) -> DataflowId {
        self.dataflow_id
    }
}

// ---------------------------------------------------------------------------
// DataframeSender — wraps payload channel with dataflow context
// ---------------------------------------------------------------------------

/// A sender of one dataflow registration's payload frames (see
/// [`SharedPeerManager::dataflow_sender`]).
///
/// Tags each frame with the dataflow id and its registration: frames it
/// sends after the registration has ended are dropped, never sequenced into
/// another registration's lane.
#[derive(Clone)]
pub struct DataframeSender {
    dataflow_id: DataflowId,
    token: u64,
    tx: tokio_mpsc::Sender<Outbound>,
}

impl DataframeSender {
    fn entry(&self, mut frame: Frame) -> Outbound {
        frame.dataflow_id = self.dataflow_id;
        Outbound::Payload {
            dataflow_id: self.dataflow_id,
            token: self.token,
            frame,
        }
    }

    fn frame_of(entry: Outbound) -> Frame {
        match entry {
            Outbound::Payload { frame, .. } => frame,
            Outbound::Barrier(_) => unreachable!("a sender only queues payload frames"),
        }
    }

    /// Send a frame through the shared transport.
    ///
    /// The frame's `dataflow_id` is normalized to this sender's dataflow, a
    /// sequence number is assigned, and the frame is queued on the
    /// dataflow's pinned connection.
    pub async fn send(&self, frame: Frame) -> Result<(), tokio_mpsc::error::SendError<Frame>> {
        self.tx
            .send(self.entry(frame))
            .await
            .map_err(|e| tokio_mpsc::error::SendError(Self::frame_of(e.0)))
    }

    /// Try to send a frame without blocking.
    pub fn try_send(&self, frame: Frame) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        self.tx.try_send(self.entry(frame)).map_err(|e| match e {
            tokio_mpsc::error::TrySendError::Full(v) => {
                tokio_mpsc::error::TrySendError::Full(Self::frame_of(v))
            }
            tokio_mpsc::error::TrySendError::Closed(v) => {
                tokio_mpsc::error::TrySendError::Closed(Self::frame_of(v))
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};
    use tokio::task::JoinHandle;

    fn health_tx() -> tokio::sync::broadcast::Sender<crate::RuntimeEvent> {
        tokio::sync::broadcast::channel(16).0
    }

    fn config(connections: usize) -> SharedConnectionConfig {
        SharedConnectionConfig {
            connections,
            probe_interval: Duration::from_secs(3600),
            ..Default::default()
        }
    }

    /// Hands out pre-established streams, then fails.
    struct Pre(Mutex<VecDeque<DuplexStream>>);

    impl Pre {
        fn factory(
            streams: impl IntoIterator<Item = DuplexStream>,
        ) -> Arc<dyn DynConnectionFactory> {
            Arc::new(Self(Mutex::new(streams.into_iter().collect())))
        }
    }

    impl ConnectionFactory for Pre {
        type Reader = tokio::io::ReadHalf<DuplexStream>;
        type Writer = tokio::io::WriteHalf<DuplexStream>;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
        {
            let stream = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .ok_or("no more connections")?;
            Ok(tokio::io::split(stream))
        }
    }

    /// A connection between two managers through a relay the test can cut
    /// (both managers then see the connection end).
    fn relayed() -> (DuplexStream, DuplexStream, JoinHandle<()>) {
        let (a, mut relay_a) = duplex(1 << 20);
        let (b, mut relay_b) = duplex(1 << 20);
        let relay = tokio::spawn(async move {
            let _ = tokio::io::copy_bidirectional(&mut relay_a, &mut relay_b).await;
        });
        (a, b, relay)
    }

    /// Two managers ("a" talks to peer "b" and vice versa) over `n` relayed
    /// connections.
    fn managers(n: usize) -> (SharedPeerManager, SharedPeerManager, Vec<JoinHandle<()>>) {
        let rt = tokio::runtime::Handle::current();
        let mut ends_a = Vec::new();
        let mut ends_b = Vec::new();
        let mut relays = Vec::new();
        for _ in 0..n {
            let (a, b, relay) = relayed();
            ends_a.push(a);
            ends_b.push(b);
            relays.push(relay);
        }
        let a = SharedPeerManager::new(
            "b".into(),
            config(n),
            Pre::factory(ends_a),
            &rt,
            health_tx(),
        )
        .unwrap();
        let b = SharedPeerManager::new(
            "a".into(),
            config(n),
            Pre::factory(ends_b),
            &rt,
            health_tx(),
        )
        .unwrap();
        (a, b, relays)
    }

    /// A manager whose only connection's other end is the returned stream,
    /// which the test drives with framed I/O.
    fn manager_with_raw_peer() -> (SharedPeerManager, DuplexStream) {
        let (ours, theirs) = duplex(1 << 20);
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            Pre::factory([ours]),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        (manager, theirs)
    }

    fn frame(dataflow_id: DataflowId, channel_id: u64, payload: Vec<u8>) -> Frame {
        Frame {
            dataflow_id,
            channel_id,
            payload,
        }
    }

    fn sequenced(dataflow_id: DataflowId, channel_id: u64, seq: u64, payload: &[u8]) -> Frame {
        let mut bytes = seq.to_le_bytes().to_vec();
        bytes.extend_from_slice(payload);
        frame(dataflow_id, channel_id, bytes)
    }

    /// Queue a payload frame of `dataflow_id`'s current registration (one
    /// that matches no registration if it is not registered).
    async fn send(
        manager: &SharedPeerManager,
        dataflow_id: DataflowId,
        channel_id: u64,
        payload: Vec<u8>,
    ) {
        let sender = manager
            .dataflow_sender(&dataflow_id)
            .unwrap_or(DataframeSender {
                dataflow_id,
                token: 0,
                tx: manager.payload_tx.clone(),
            });
        sender
            .send(frame(dataflow_id, channel_id, payload))
            .await
            .unwrap();
    }

    async fn recv<T>(rx: &mut tokio_mpsc::Receiver<T>) -> Option<T> {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("nothing arrived within 5 s")
    }

    /// The next event, or `None` if there is none within `wait`.
    async fn event_within(
        events: &mut tokio_mpsc::Receiver<PeerIoNotice>,
        wait: Duration,
    ) -> Option<PeerIoEvent> {
        tokio::time::timeout(wait, events.recv())
            .await
            .ok()
            .flatten()
            .map(|notice| notice.into_parts().0)
    }

    async fn next_event(events: &mut tokio_mpsc::Receiver<PeerIoNotice>) -> PeerIoEvent {
        event_within(events, Duration::from_secs(5))
            .await
            .expect("no connection event within 5 s")
    }

    // --- Registration -------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connections_are_established_when_the_first_dataflow_registers() {
        let (a, _b, _relays) = managers(2);
        assert_eq!(a.peer_node_id(), "b");
        assert_eq!(a.connection_count(), 0);
        let (receivers, _events) = a
            .register_dataflow(DataflowId::new(), &[1, 2], 16)
            .await
            .unwrap();
        assert_eq!(a.connection_count(), 2);
        // 2 requested + the auto-registered control channel.
        let mut ids: Vec<u64> = receivers.keys().copied().collect();
        ids.sort();
        assert_eq!(ids, vec![CONTROL_CHANNEL_ID, 1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_rejects_reserved_duplicate_and_repeated() {
        let (a, _b, _relays) = managers(1);
        let df = DataflowId::new();
        for channels in [vec![1, PROBE_CHANNEL_ID], vec![1, 1]] {
            assert!(matches!(
                a.register_dataflow(df, &channels, 4).await,
                Err(TransportError::InvalidRegistration { .. })
            ));
        }
        assert!(matches!(
            a.register_dataflow(df, &[1], 0).await,
            Err(TransportError::InvalidRegistration { .. })
        ));
        let _registered = a.register_dataflow(df, &[1], 4).await.unwrap();
        assert!(matches!(
            a.register_dataflow(df, &[1], 4).await,
            Err(TransportError::InvalidRegistration { .. })
        ));
    }

    /// A `DataflowId` identifies one run: once it finished here, registering
    /// it again (a retry with the same id) fails loudly, and the late frames
    /// of the finished run are dropped instead of waiting for a registration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_finished_dataflow_id_cannot_be_registered_again() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _first = manager.register_dataflow(df, &[1], 4).await.unwrap();
        manager.unregister_dataflow(&df).await;
        match manager.register_dataflow(df, &[1], 4).await {
            Err(TransportError::InvalidRegistration { reason }) => {
                assert!(reason.contains("fresh DataflowId"), "{reason}");
            }
            other => panic!("a reused id must be rejected: {other:?}"),
        }
        // The finished run's late control frames (its handshake, its
        // Cancel) are dropped, not buffered.
        let (_peer_read, peer_write) = tokio::io::split(peer);
        let mut peer_writer = FramedWriter::new(peer_write);
        for payload in [b"handshake".to_vec(), b"cancel".to_vec()] {
            peer_writer
                .write_frame(&frame(df, CONTROL_CHANNEL_ID, payload))
                .await
                .unwrap();
        }
        // A fresh id's early control frame is buffered (it arrives after
        // the late ones on the same connection).
        let fresh = DataflowId::new();
        peer_writer
            .write_frame(&frame(fresh, CONTROL_CHANNEL_ID, b"early".to_vec()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while manager.inner.state().pending_control.len() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let state = manager.inner.state();
        assert_eq!(state.pending_control.len(), 1);
        assert!(state.pending_control.by_dataflow.contains_key(&fresh));
    }

    /// Payload frames still queued when their registration ends are dropped:
    /// they are never sequenced into another registration of the same id
    /// (possible only once the id is no longer remembered).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_finished_registrations_queued_frames_never_reach_another() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _first = manager.register_dataflow(df, &[1], 4).await.unwrap();
        let old = manager.dataflow_sender(&df).unwrap();
        manager.unregister_dataflow(&df).await;
        // Forget the id, as after 16,384 later dataflows.
        manager.inner.state().completed = RecentIds::default();
        let (mut rx, _events) = manager.register_dataflow(df, &[1], 4).await.unwrap();
        old.send(frame(df, 1, b"old run".to_vec())).await.unwrap();
        send(&manager, df, 1, b"new run".to_vec()).await;
        manager.flush().await;
        let (peer_read, _peer_write) = tokio::io::split(peer);
        let mut peer_reader = FramedReader::new(peer_read);
        let first = peer_reader.read_frame().await.unwrap();
        assert_eq!(first.payload[..SEQUENCE_PREFIX], 0u64.to_le_bytes());
        assert_eq!(first.payload[SEQUENCE_PREFIX..], *b"new run");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer_reader.read_frame())
                .await
                .is_err(),
            "the old run's frame must not be sent"
        );
        drop(rx.remove(&1));
    }

    /// M5: an unreachable peer fails registration within `connect_timeout`
    /// instead of holding it (and the pool's init lock) forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreachable_peer_fails_registration_within_the_connect_timeout() {
        struct NeverConnects;
        impl ConnectionFactory for NeverConnects {
            type Reader = DuplexStream;
            type Writer = DuplexStream;
            async fn establish(
                &self,
                _peer_node_id: &str,
            ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
            {
                std::future::pending().await
            }
        }
        let manager = SharedPeerManager::new(
            "peer".into(),
            SharedConnectionConfig {
                connect_timeout: Duration::from_millis(100),
                ..config(1)
            },
            Arc::new(NeverConnects),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            manager.register_dataflow(DataflowId::new(), &[1], 4),
        )
        .await
        .expect("registration must give up at the connect timeout");
        assert!(
            matches!(result, Err(TransportError::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut)
        );
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn invalid_config_is_rejected() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = SharedPeerManager::new(
            "peer".into(),
            config(0),
            Pre::factory([]),
            rt.handle(),
            health_tx(),
        );
        assert!(result.is_err());
    }

    // --- Delivery, pinning and sequencing -----------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frames_arrive_in_order_between_two_managers() {
        let (a, b, _relays) = managers(1);
        let df = DataflowId::new();
        let (_ra, _ea) = a.register_dataflow(df, &[1, 2], 64).await.unwrap();
        let (mut rb, mut eb) = b.register_dataflow(df, &[1, 2], 64).await.unwrap();
        for i in 0..50u8 {
            send(&a, df, 1 + u64::from(i % 2), vec![i]).await;
        }
        let (mut one, mut two) = (Vec::new(), Vec::new());
        for _ in 0..25 {
            one.push(recv(rb.get_mut(&1).unwrap()).await.unwrap()[0]);
            two.push(recv(rb.get_mut(&2).unwrap()).await.unwrap()[0]);
        }
        assert_eq!(one, (0..50).step_by(2).collect::<Vec<u8>>());
        assert_eq!(two, (1..50).step_by(2).collect::<Vec<u8>>());
        assert!(
            event_within(&mut eb, Duration::from_millis(100))
                .await
                .is_none()
        );
    }

    /// Dataflows spread over the pool, one connection each: every frame of a
    /// dataflow travels on its connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_dataflow_lane_is_pinned_to_one_connection() {
        let (a, _b, _relays) = managers(2);
        let (d1, d2, d3) = (DataflowId::new(), DataflowId::new(), DataflowId::new());
        let _r1 = a.register_dataflow(d1, &[1], 16).await.unwrap();
        let _r2 = a.register_dataflow(d2, &[1], 16).await.unwrap();
        let _r3 = a.register_dataflow(d3, &[1], 16).await.unwrap();
        let pins = |manager: &SharedPeerManager| {
            manager
                .metrics_snapshot()
                .iter()
                .map(|s| s.pinned_lanes)
                .collect::<Vec<_>>()
        };
        assert_eq!(pins(&a), vec![2, 1]);
        let lane = |df| a.inner.state().registered[&df].conn;
        assert_ne!(lane(d1), lane(d2));
        assert_eq!(lane(d1), lane(d3));
        a.unregister_dataflow(&d1).await;
        assert_eq!(pins(&a), vec![1, 1]);
    }

    /// DA R52: a skipped or repeated sequence number fails that dataflow at
    /// once (loudly, as corruption), and another dataflow on the same
    /// connection keeps receiving exactly what was sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn out_of_sequence_frame_fails_only_its_dataflow() {
        // The healthy dataflow's frames arrive on the same connection, or on
        // the pool's other one.
        for same_connection in [true, false] {
            for (bad_seq, expected) in [(2u64, 1u64), (0, 1)] {
                let (ours_1, theirs_1) = duplex(1 << 16);
                let (ours_2, theirs_2) = duplex(1 << 16);
                let manager = SharedPeerManager::new(
                    "peer".into(),
                    config(2),
                    Pre::factory([ours_1, ours_2]),
                    &tokio::runtime::Handle::current(),
                    health_tx(),
                )
                .unwrap();
                let (bad, good) = (DataflowId::new(), DataflowId::new());
                let (mut bad_rx, mut bad_events) =
                    manager.register_dataflow(bad, &[1], 16).await.unwrap();
                let (mut good_rx, mut good_events) =
                    manager.register_dataflow(good, &[1], 16).await.unwrap();
                let (_read_1, write_1) = tokio::io::split(theirs_1);
                let (_read_2, write_2) = tokio::io::split(theirs_2);
                let mut writers = [FramedWriter::new(write_1), FramedWriter::new(write_2)];
                let good_on = if same_connection { 0 } else { 1 };
                for (on, frame) in [
                    (0, sequenced(bad, 1, 0, b"first")),
                    (good_on, sequenced(good, 1, 0, b"g0")),
                    (0, sequenced(bad, 1, bad_seq, b"bad")),
                    (good_on, sequenced(good, 1, 1, b"g1")),
                ] {
                    writers[on].write_frame(&frame).await.unwrap();
                }
                assert_eq!(recv(bad_rx.get_mut(&1).unwrap()).await.unwrap(), b"first");
                match next_event(&mut bad_events).await {
                    PeerIoEvent::ReadFailed(TransportError::OutOfSequence {
                        expected: e,
                        received,
                    }) => {
                        assert_eq!((e, received), (expected, bad_seq));
                    }
                    other => panic!("expected OutOfSequence, got {other:?}"),
                }
                // The bad frame was never delivered, and the channel closed.
                assert_eq!(recv(bad_rx.get_mut(&1).unwrap()).await, None);
                assert_eq!(recv(good_rx.get_mut(&1).unwrap()).await.unwrap(), b"g0");
                assert_eq!(recv(good_rx.get_mut(&1).unwrap()).await.unwrap(), b"g1");
                assert!(
                    event_within(&mut good_events, Duration::from_millis(100))
                        .await
                        .is_none()
                );
            }
        }
    }
    /// A frame whose payload fails its checksum is corruption for its own
    /// dataflow only; the other dataflows reading from the connection lose
    /// the connection (fatal while they run, not corruption).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_frame_is_corruption_only_for_its_dataflow() {
        let (manager, peer) = manager_with_raw_peer();
        let (owner, bystander) = (DataflowId::new(), DataflowId::new());
        let (mut owner_rx, mut owner_events) =
            manager.register_dataflow(owner, &[1], 16).await.unwrap();
        let (mut bystander_rx, mut bystander_events) = manager
            .register_dataflow(bystander, &[1], 16)
            .await
            .unwrap();
        let (_peer_read, mut peer_write) = tokio::io::split(peer);
        let mut wire = Vec::new();
        let mut encoder = FramedWriter::new(&mut wire);
        for frame in [
            sequenced(owner, 1, 0, b"o0"),
            sequenced(bystander, 1, 0, b"b0"),
            sequenced(owner, 1, 1, b"o1"),
        ] {
            encoder.write_frame(&frame).await.unwrap();
        }
        // Flip a payload bit of the last frame: its header stays valid.
        let last = wire.len() - 5;
        wire[last] ^= 0x01;
        peer_write.write_all(&wire).await.unwrap();

        assert_eq!(recv(owner_rx.get_mut(&1).unwrap()).await.unwrap(), b"o0");
        assert_eq!(
            recv(bystander_rx.get_mut(&1).unwrap()).await.unwrap(),
            b"b0"
        );
        let owner_event = loop {
            match next_event(&mut owner_events).await {
                PeerIoEvent::ReadFailed(e) => break e,
                PeerIoEvent::WriteFailed(_) => {}
                other => panic!("{other:?}"),
            }
        };
        assert!(
            matches!(owner_event, TransportError::ChecksumMismatch { .. }),
            "{owner_event:?}"
        );
        let bystander_event = loop {
            match next_event(&mut bystander_events).await {
                PeerIoEvent::ReadFailed(e) => break e,
                PeerIoEvent::WriteFailed(_) => {}
                other => panic!("{other:?}"),
            }
        };
        assert!(
            matches!(&bystander_event, TransportError::Io(e) if e.kind() == std::io::ErrorKind::ConnectionAborted),
            "{bystander_event:?}"
        );
    }

    /// A flush barrier is the manager's own queue entry, never a frame: an
    /// abandoned flush (its caller timed out) wakes nobody and fails nobody,
    /// and a later flush still completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_abandoned_flush_is_harmless() {
        let (a, _b, _relays) = managers(1);
        let df = DataflowId::new();
        let (_rx, mut events) = a.register_dataflow(df, &[1], 16).await.unwrap();
        let _ = tokio::time::timeout(Duration::ZERO, a.flush()).await;
        send(&a, df, 1, vec![1]).await;
        tokio::time::timeout(Duration::from_secs(5), a.flush())
            .await
            .expect("flush");
        assert!(
            event_within(&mut events, Duration::from_millis(100))
                .await
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frame_for_an_unregistered_channel_fails_its_dataflow() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let (mut rx, mut events) = manager.register_dataflow(df, &[1], 16).await.unwrap();
        let (_peer_read, peer_write) = tokio::io::split(peer);
        let mut writer = FramedWriter::new(peer_write);
        writer
            .write_frame(&sequenced(df, 7, 0, b"stray"))
            .await
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            PeerIoEvent::ReadFailed(TransportError::LaneViolation { .. })
        ));
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await, None);
    }

    /// A lane's frames arriving on two connections lose their order: fatal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lane_split_across_connections_fails_its_dataflow() {
        let (ours_1, theirs_1) = duplex(1 << 16);
        let (ours_2, theirs_2) = duplex(1 << 16);
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(2),
            Pre::factory([ours_1, ours_2]),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let df = DataflowId::new();
        let (_rx, mut events) = manager.register_dataflow(df, &[1], 16).await.unwrap();
        let mut first = FramedWriter::new(tokio::io::split(theirs_1).1);
        let mut second = FramedWriter::new(tokio::io::split(theirs_2).1);
        first.write_frame(&sequenced(df, 1, 0, b"a")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        second
            .write_frame(&sequenced(df, 1, 1, b"b"))
            .await
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            PeerIoEvent::ReadFailed(TransportError::LaneViolation { .. })
        ));
    }

    /// Control frames bypass the lane's numbering (DA R52, condition 1) and
    /// those that arrive before registration are delivered on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn control_frames_are_unsequenced_and_buffered_until_registration() {
        let (manager, peer) = manager_with_raw_peer();
        // Establish the connection with a first registration.
        let _other = manager
            .register_dataflow(DataflowId::new(), &[1], 4)
            .await
            .unwrap();
        let df = DataflowId::new();
        let (_peer_read, peer_write) = tokio::io::split(peer);
        let mut writer = FramedWriter::new(peer_write);
        writer
            .write_frame(&frame(df, CONTROL_CHANNEL_ID, b"early".to_vec()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (mut rx, _events) = manager.register_dataflow(df, &[1], 4).await.unwrap();
        writer
            .write_frame(&sequenced(df, 1, 0, b"data"))
            .await
            .unwrap();
        writer
            .write_frame(&frame(df, CONTROL_CHANNEL_ID, b"late".to_vec()))
            .await
            .unwrap();
        let control = rx.get_mut(&CONTROL_CHANNEL_ID).unwrap();
        assert_eq!(recv(control).await.unwrap(), b"early");
        assert_eq!(recv(control).await.unwrap(), b"late");
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await.unwrap(), b"data");
    }

    // --- Rejected frames (M4) -------------------------------------------------

    /// M4: one dataflow's oversized frame (or one on a reserved channel) is
    /// rejected and fails only that dataflow; the shared connection and the
    /// other dataflows are untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejected_frame_fails_only_its_dataflow() {
        let (a, b, _relays) = managers(1);
        let (big, small) = (DataflowId::new(), DataflowId::new());
        let (_rb, mut big_events) = a.register_dataflow(big, &[1], 16).await.unwrap();
        let (_rs, mut small_events) = a.register_dataflow(small, &[1], 16).await.unwrap();
        let (_peer_big, _peer_big_events) = b.register_dataflow(big, &[1], 16).await.unwrap();
        let (mut peer_small, _peer_small_events) =
            b.register_dataflow(small, &[1], 16).await.unwrap();

        // Fits MAX_MESSAGE_SIZE, but not once the sequence number is added.
        send(&a, big, 1, vec![0u8; MAX_MESSAGE_SIZE - 4]).await;
        match next_event(&mut big_events).await {
            PeerIoEvent::FrameRejected(TransportError::PayloadTooLarge { size, max }) => {
                assert_eq!((size, max), (MAX_MESSAGE_SIZE + 4, MAX_MESSAGE_SIZE));
            }
            other => panic!("expected FrameRejected, got {other:?}"),
        }
        send(&a, big, CONTROL_CHANNEL_ID, vec![1]).await;
        assert!(matches!(
            next_event(&mut big_events).await,
            PeerIoEvent::FrameRejected(TransportError::ReservedChannel { channel_id: 0 })
        ));
        for i in 0..20u8 {
            send(&a, small, 1, vec![i]).await;
        }
        for i in 0..20u8 {
            assert_eq!(
                recv(peer_small.get_mut(&1).unwrap()).await.unwrap(),
                vec![i]
            );
        }
        assert!(
            event_within(&mut small_events, Duration::from_millis(100))
                .await
                .is_none()
        );
        assert_eq!(a.connection_count(), 1);
    }

    // --- Connection failures (C7) ----------------------------------------------

    /// C7: frames sent after the only connection failed are never silently
    /// dropped. The dataflow is told its link failed (both directions), and
    /// nothing reaches the peer out of sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_failure_is_reported_never_silent() {
        let (a, b, relays) = managers(1);
        let df = DataflowId::new();
        let (mut ra, mut ea) = a.register_dataflow(df, &[1], 16).await.unwrap();
        let (mut rb, mut eb) = b.register_dataflow(df, &[1], 16).await.unwrap();
        send(&a, df, 1, b"a".to_vec()).await;
        send(&b, df, 1, b"z".to_vec()).await;
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await.unwrap(), b"a");
        assert_eq!(recv(ra.get_mut(&1).unwrap()).await.unwrap(), b"z");

        relays[0].abort();
        // Both sides: the inbound side ended.
        for events in [&mut ea, &mut eb] {
            let event = next_event(events).await;
            assert!(
                matches!(event, PeerIoEvent::Closed | PeerIoEvent::ReadFailed(_)),
                "{event:?}"
            );
        }
        // The peer's channels closed after the report.
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await, None);
        assert_eq!(a.connection_count(), 0);
        // The connection is gone both ways: the next write fails, and is
        // reported (its frame is lost) ...
        send(&a, df, 1, b"b".to_vec()).await;
        assert!(matches!(
            next_event(&mut ea).await,
            PeerIoEvent::WriteFailed(_)
        ));
        // ... once: frames sent now go nowhere, and the dataflow already
        // failed.
        send(&a, df, 1, b"c".to_vec()).await;
        assert!(
            event_within(&mut ea, Duration::from_millis(100))
                .await
                .is_none()
        );
    }

    /// C7: the frames of a lane still queued when its connection fails (its
    /// tail) are reported lost even though nothing follows them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_tail_lost_with_its_connection_is_reported() {
        // A peer that never reads: the frames stay queued, then the
        // connection breaks.
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let (_rx, mut events) = manager.register_dataflow(df, &[1], 16).await.unwrap();
        for i in 0..64u8 {
            send(&manager, df, 1, vec![i; 64 * 1024]).await;
        }
        drop(peer);
        assert!(matches!(
            next_event(&mut events).await,
            PeerIoEvent::WriteFailed(_) | PeerIoEvent::Closed | PeerIoEvent::ReadFailed(_)
        ));
    }

    /// A write half that fails every write once `broken` is set, like a
    /// local error on one half of a TLS stream: the read half and the peer's
    /// end stay open.
    struct BreakableWriter {
        inner: tokio::io::WriteHalf<DuplexStream>,
        broken: Arc<AtomicBool>,
    }

    impl tokio::io::AsyncWrite for BreakableWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.broken.load(Ordering::SeqCst) {
                return std::task::Poll::Ready(Err(std::io::Error::other("local write error")));
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    struct Breakable(Mutex<Option<(DuplexStream, Arc<AtomicBool>)>>);

    impl ConnectionFactory for Breakable {
        type Reader = tokio::io::ReadHalf<DuplexStream>;
        type Writer = BreakableWriter;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
        {
            let (stream, broken) = self.0.lock().unwrap().take().ok_or("no more connections")?;
            let (reader, inner) = tokio::io::split(stream);
            Ok((reader, BreakableWriter { inner, broken }))
        }
    }

    /// A write error closes the whole connection, not just its write
    /// half (which, split from a stream, tells the peer nothing): the peer
    /// sees the end and fails the same dataflows, and the dataflows reading
    /// from the connection here fail too, so the two pools stay paired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_error_closes_the_whole_connection() {
        let (ours, theirs) = duplex(1 << 20);
        let broken = Arc::new(AtomicBool::new(false));
        let factory: Arc<dyn DynConnectionFactory> =
            Arc::new(Breakable(Mutex::new(Some((ours, Arc::clone(&broken))))));
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            factory,
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let df = DataflowId::new();
        let (mut rx, mut events) = manager.register_dataflow(df, &[1], 16).await.unwrap();
        let (peer_read, peer_write) = tokio::io::split(theirs);
        let mut peer_reader = FramedReader::new(peer_read);
        let mut peer_writer = FramedWriter::new(peer_write);
        // The lane works both ways.
        send(&manager, df, 1, b"out".to_vec()).await;
        assert_eq!(
            peer_reader.read_frame().await.unwrap().payload[8..],
            *b"out"
        );
        peer_writer
            .write_frame(&sequenced(df, 1, 0, b"in"))
            .await
            .unwrap();
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await.unwrap(), b"in");

        broken.store(true, Ordering::SeqCst);
        send(&manager, df, 1, b"lost".to_vec()).await;
        let mut seen = Vec::new();
        while seen.len() < 2 {
            seen.push(next_event(&mut events).await);
        }
        assert!(
            matches!(seen[0], PeerIoEvent::WriteFailed(_)),
            "the lost frame is reported first: {seen:?}"
        );
        assert!(
            matches!(&seen[1], PeerIoEvent::ReadFailed(e) if !matches!(e, TransportError::ChecksumMismatch { .. })),
            "reading from the closed connection fails too: {seen:?}"
        );
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await, None);
        // The peer sees the connection end (it waited forever before).
        let end = tokio::time::timeout(Duration::from_secs(5), peer_reader.read_frame())
            .await
            .expect("the peer never saw the connection end");
        assert!(
            matches!(end, Err(TransportError::ConnectionClosed)),
            "{end:?}"
        );
        assert_eq!(manager.connection_count(), 0);
    }

    /// The peer closing its side cleanly (a released manager, which keeps
    /// reading for its peers' last frames) is not a broken connection: the
    /// dataflows using it are told, but a lane pinned to it keeps writing,
    /// so a goodbye queued after the peer's EOF still reaches the peer. The
    /// connection takes no new lanes, and closes once its last lane
    /// unregisters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peers_clean_close_lets_pinned_lanes_finish_writing() {
        let (ours, theirs) = duplex(1 << 20);
        let (ours_2, _theirs_2) = duplex(1 << 20);
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            Pre::factory([ours, ours_2]),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let (outbound_only, both) = (DataflowId::new(), DataflowId::new());
        let (_rx_o, mut events_o) = manager
            .register_dataflow(outbound_only, &[1], 4)
            .await
            .unwrap();
        let (mut rx_b, mut events_b) = manager.register_dataflow(both, &[1], 4).await.unwrap();
        let (peer_read, peer_write) = tokio::io::split(theirs);
        let mut peer_reader = FramedReader::new(peer_read);
        let mut peer_writer = FramedWriter::new(peer_write);
        peer_writer
            .write_frame(&sequenced(both, 1, 0, b"in"))
            .await
            .unwrap();
        assert_eq!(recv(rx_b.get_mut(&1).unwrap()).await.unwrap(), b"in");
        // The peer releases its side: it sends nothing more, but reads on.
        peer_writer.into_inner().shutdown().await.unwrap();
        assert!(matches!(
            next_event(&mut events_b).await,
            PeerIoEvent::Closed
        ));
        assert!(matches!(
            next_event(&mut events_o).await,
            PeerIoEvent::Closed
        ));
        assert_eq!(recv(rx_b.get_mut(&1).unwrap()).await, None);
        for df in [outbound_only, both] {
            send(&manager, df, 1, b"goodbye".to_vec()).await;
        }
        for df in [outbound_only, both] {
            let frame = peer_reader.read_frame().await.unwrap();
            assert_eq!(frame.dataflow_id, df);
            assert_eq!(frame.payload[SEQUENCE_PREFIX..], *b"goodbye");
        }
        for events in [&mut events_o, &mut events_b] {
            assert!(
                event_within(events, Duration::from_millis(100))
                    .await
                    .is_none(),
                "a write after the peer's clean close must not fail"
            );
        }
        // No new lane goes to it: the next registration fills the pool anew.
        assert_eq!(manager.connection_count(), 0);
        let (_rx_n, _events_n) = manager
            .register_dataflow(DataflowId::new(), &[1], 4)
            .await
            .unwrap();
        assert_eq!(manager.connection_count(), 1);
        // Once its last lane unregisters, our side closes too.
        manager.unregister_dataflow(&outbound_only).await;
        manager.unregister_dataflow(&both).await;
        let end = tokio::time::timeout(Duration::from_secs(5), peer_reader.read_frame())
            .await
            .expect("our side never closed");
        assert!(
            matches!(end, Err(TransportError::ConnectionClosed)),
            "{end:?}"
        );
    }

    /// A failed connection fails exactly the dataflows that used it, on both
    /// nodes; a dataflow pinned to another pooled connection completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connection_failure_fails_only_the_dataflows_that_used_it() {
        let (a, b, relays) = managers(2);
        let (hit, spared) = (DataflowId::new(), DataflowId::new());
        let (_ra1, mut ea_hit) = a.register_dataflow(hit, &[1], 16).await.unwrap();
        let (mut ra_spared, mut ea_spared) = a.register_dataflow(spared, &[1], 16).await.unwrap();
        let (mut rb1, mut eb_hit) = b.register_dataflow(hit, &[1], 16).await.unwrap();
        let (mut rb2, mut eb_spared) = b.register_dataflow(spared, &[1], 16).await.unwrap();
        // Both nodes pinned `hit` to their first connection (relay 0).
        assert_eq!(a.inner.state().registered[&hit].conn, 0);
        assert_eq!(b.inner.state().registered[&hit].conn, 0);
        send(&a, hit, 1, b"h".to_vec()).await;
        send(&b, spared, 1, b"s".to_vec()).await;
        assert_eq!(recv(rb1.get_mut(&1).unwrap()).await.unwrap(), b"h");

        relays[0].abort();
        let _ = next_event(&mut ea_hit).await;
        let _ = next_event(&mut eb_hit).await;
        assert_eq!(recv(rb1.get_mut(&1).unwrap()).await, None);
        // The spared dataflow still works both ways.
        assert_eq!(recv(ra_spared.get_mut(&1).unwrap()).await.unwrap(), b"s");
        for i in 0..10u8 {
            send(&a, spared, 1, vec![i]).await;
        }
        for i in 0..10u8 {
            assert_eq!(recv(rb2.get_mut(&1).unwrap()).await.unwrap(), vec![i]);
        }
        assert!(
            event_within(&mut ea_spared, Duration::from_millis(100))
                .await
                .is_none()
        );
        assert!(
            event_within(&mut eb_spared, Duration::from_millis(100))
                .await
                .is_none()
        );
        assert_eq!(a.connection_count(), 1);
    }

    /// After a failure the pool is refilled only when a dataflow registers,
    /// through the application's factory; nothing is redialled meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_pool_is_refilled_when_the_next_dataflow_registers() {
        let (first_a, first_b, relay) = relayed();
        let (second_a, second_b, _relay2) = relayed();
        let rt = tokio::runtime::Handle::current();
        let a = SharedPeerManager::new(
            "b".into(),
            config(1),
            Pre::factory([first_a, second_a]),
            &rt,
            health_tx(),
        )
        .unwrap();
        let b = SharedPeerManager::new(
            "a".into(),
            config(1),
            Pre::factory([first_b, second_b]),
            &rt,
            health_tx(),
        )
        .unwrap();
        let old = DataflowId::new();
        let (_ra, mut ea) = a.register_dataflow(old, &[1], 4).await.unwrap();
        let (_rb, _eb) = b.register_dataflow(old, &[1], 4).await.unwrap();
        relay.abort();
        let _ = next_event(&mut ea).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(a.connection_count(), 0, "nothing redials in the background");
        let new = DataflowId::new();
        let (_ra, _ea) = a.register_dataflow(new, &[1], 4).await.unwrap();
        let (mut rb, _eb) = b.register_dataflow(new, &[1], 4).await.unwrap();
        assert_eq!(a.connection_count(), 1);
        send(&a, new, 1, b"x".to_vec()).await;
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await.unwrap(), b"x");
    }

    /// The channels of a dataflow whose inbound connection ended stay open
    /// until its event was handed on, once someone hands events on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channels_stay_open_until_the_inbound_end_is_handed_on() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let hold = Arc::new(AtomicBool::new(true));
        let (mut rx, mut events) = manager
            .register(df, 1, &[1], 16, Arc::clone(&hold))
            .await
            .unwrap();
        let (peer_read, peer_write) = tokio::io::split(peer);
        let mut writer = FramedWriter::new(peer_write);
        writer
            .write_frame(&sequenced(df, 1, 0, b"x"))
            .await
            .unwrap();
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await.unwrap(), b"x");
        drop((writer, peer_read));
        let notice = loop {
            let notice = recv(&mut events).await.unwrap();
            if matches!(notice.event(), PeerIoEvent::Closed) {
                break notice;
            }
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(matches!(
            rx.get_mut(&1).unwrap().try_recv(),
            Err(tokio_mpsc::error::TryRecvError::Empty)
        ));
        drop(notice);
        assert_eq!(recv(rx.get_mut(&1).unwrap()).await, None);
    }

    // --- Teardown -----------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_then_unregister_delivers_every_queued_frame() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _registered = manager.register_dataflow(df, &[1], 16).await.unwrap();
        for i in 0..200u8 {
            send(&manager, df, 1, vec![i]).await;
        }
        manager.flush().await;
        manager.unregister_dataflow(&df).await;

        let mut reader = FramedReader::new(peer);
        for i in 0..200u8 {
            let frame = tokio::time::timeout(Duration::from_secs(5), reader.read_frame())
                .await
                .expect("frame queued before the flush was dropped")
                .unwrap();
            assert_eq!(frame.channel_id, 1);
            assert_eq!(frame.payload[..8], u64::from(i).to_le_bytes());
            assert_eq!(&frame.payload[8..], &[i]);
        }
        // The barrier itself never goes on the wire.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), reader.read_frame())
                .await
                .is_err()
        );
    }

    /// Frames the bridge already handed to a connection writer survive the
    /// manager being released, whether it is closed (waiting for its writers)
    /// or simply dropped (writers finish in the background).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn releasing_the_manager_lets_writers_finish_queued_frames() {
        for close in [true, false] {
            // A tiny pipe: the writer blocks after a frame or two, so most
            // frames are still queued on it when the manager is released.
            let (ours, theirs) = duplex(64);
            let manager = SharedPeerManager::new(
                "peer".into(),
                config(1),
                Pre::factory([ours]),
                &tokio::runtime::Handle::current(),
                health_tx(),
            )
            .unwrap();
            let df = DataflowId::new();
            let _registered = manager.register_dataflow(df, &[1], 16).await.unwrap();
            for i in 0..100u8 {
                send(&manager, df, 1, vec![i; 32]).await;
            }
            manager.flush().await;
            manager.unregister_dataflow(&df).await;
            let reading = tokio::spawn(async move {
                let mut reader = FramedReader::new(theirs);
                let mut got = Vec::new();
                while got.len() < 100 {
                    match tokio::time::timeout(Duration::from_secs(5), reader.read_frame()).await {
                        Ok(Ok(frame)) => got.push(frame.payload[8]),
                        _ => break,
                    }
                }
                got
            });
            if close {
                manager.close(Duration::from_secs(5)).await;
            } else {
                drop(manager);
            }
            let got = reading.await.unwrap();
            assert_eq!(
                got,
                (0..100u8).collect::<Vec<_>>(),
                "close={close}: queued frames were lost"
            );
        }
    }

    /// Over TCP, a released manager whose peer is still sending keeps reading
    /// until the peer closes: releasing the read half with unread data would
    /// make the OS reset the connection and could destroy the frames the
    /// manager wrote last before the peer reads them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn released_manager_keeps_reading_so_its_last_frames_survive() {
        struct Tcp(Mutex<Option<tokio::net::TcpStream>>);
        impl ConnectionFactory for Tcp {
            type Reader = tokio::net::tcp::OwnedReadHalf;
            type Writer = tokio::net::tcp::OwnedWriteHalf;
            async fn establish(
                &self,
                _peer_node_id: &str,
            ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(self.0.lock().unwrap().take().ok_or("taken")?.into_split())
            }
        }
        for close in [true, false, true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (accepted, ours) =
                tokio::try_join!(listener.accept(), tokio::net::TcpStream::connect(addr)).unwrap();
            let (peer_read, peer_write) = accepted.0.into_split();
            let manager = SharedPeerManager::new(
                "peer".into(),
                config(1),
                Arc::new(Tcp(Mutex::new(Some(ours)))),
                &tokio::runtime::Handle::current(),
                health_tx(),
            )
            .unwrap();
            let df = DataflowId::new();
            let _registered = manager.register_dataflow(df, &[1], 16).await.unwrap();
            // The peer keeps sending frames nobody here consumes.
            let flood = tokio::spawn(async move {
                let mut writer = FramedWriter::new(peer_write);
                let frame = Frame {
                    dataflow_id: DataflowId::new(),
                    channel_id: 7,
                    payload: vec![0; 4096],
                };
                while writer.write_frame(&frame).await.is_ok() {}
            });
            send(&manager, df, 1, b"last".to_vec()).await;
            manager.flush().await;
            manager.unregister_dataflow(&df).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            if close {
                manager.close(Duration::from_secs(5)).await;
            } else {
                drop(manager);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut reader = FramedReader::new(peer_read);
            let got_last = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match reader.read_frame().await {
                        Ok(frame) if frame.payload.ends_with(b"last") => return true,
                        Ok(_) => {}
                        Err(_) => return false,
                    }
                }
            })
            .await;
            flood.abort();
            assert_eq!(
                got_last,
                Ok(true),
                "close={close}: the manager's last frame was lost"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_without_connections_returns() {
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            Pre::factory([]),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let df = DataflowId::new();
        send(&manager, df, 1, vec![1]).await;
        tokio::time::timeout(Duration::from_secs(5), manager.flush())
            .await
            .expect("flush must not wait for a connection");
    }

    // --- Registration lifetime and bounded state ------------------------------

    /// A cleanup only removes its own registration: not one it never made
    /// (a rejected duplicate), nor another registration of the same id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cleanup_removes_only_its_own_registration() {
        let (manager, _peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _first = manager
            .register(df, 1, &[1], 4, Arc::default())
            .await
            .unwrap();
        assert!(
            manager
                .register(df, 2, &[1], 4, Arc::default())
                .await
                .is_err()
        );
        manager.unregister_registration(&df, 2).await;
        assert!(manager.inner.state().registered.contains_key(&df));
        manager.unregister_registration(&df, 1).await;
        assert!(!manager.inner.state().registered.contains_key(&df));
        // Once the id is no longer remembered (after 16,384 later
        // dataflows), the first registration's late cleanup leaves a new
        // registration of it alone.
        manager.inner.state().completed = RecentIds::default();
        let _second = manager
            .register(df, 3, &[1], 4, Arc::default())
            .await
            .unwrap();
        manager.unregister_registration(&df, 1).await;
        assert!(manager.inner.state().registered.contains_key(&df));
    }

    #[test]
    fn pending_control_is_bounded() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let mut pending = PendingControl::default();
            let first = DataflowId::new();
            pending.push(first, 0, vec![0]);
            for _ in 0..MAX_PENDING_FRAMES_PER_DATAFLOW + 10 {
                pending.push(first, 0, vec![1]);
            }
            assert_eq!(
                pending.by_dataflow[&first].frames.len(),
                MAX_PENDING_FRAMES_PER_DATAFLOW
            );
            for _ in 0..MAX_PENDING_DATAFLOWS {
                pending.push(DataflowId::new(), 1, vec![2]);
            }
            assert_eq!(pending.len(), MAX_PENDING_DATAFLOWS);
            assert!(
                !pending.by_dataflow.contains_key(&first),
                "the oldest is evicted"
            );
            assert!(pending.order.len() <= 2 * MAX_PENDING_DATAFLOWS);
            // Frames from a failed connection are forgotten.
            pending.forget_connection(1);
            assert_eq!(pending.len(), 0);
            // Frames for a dataflow that never registers expire.
            pending.push(first, 0, vec![3]);
            tokio::time::advance(PENDING_CONTROL_TTL).await;
            pending.push(DataflowId::new(), 0, vec![4]);
            assert_eq!(pending.len(), 1);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finished_connection_tasks_are_forgotten() {
        let mut streams = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..20 {
            let (ours, theirs) = duplex(1 << 10);
            streams.push(ours);
            peers.push(theirs);
        }
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            Pre::factory(streams),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        for peer in peers {
            let df = DataflowId::new();
            let (_rx, mut events) = manager.register_dataflow(df, &[1], 4).await.unwrap();
            drop(peer);
            let _ = next_event(&mut events).await;
            manager.unregister_dataflow(&df).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let readers = manager.inner.task_handles.readers.lock().unwrap().len();
        let writers = manager.inner.task_handles.writers.lock().unwrap().len();
        assert!(
            readers <= 2 && writers <= 2,
            "readers {readers}, writers {writers}"
        );
    }

    // --- Probes ---------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probes_measure_rtt_for_the_metrics() {
        let rt = tokio::runtime::Handle::current();
        let (a_end, b_end, _relay) = relayed();
        let probing = SharedConnectionConfig {
            probe_interval: Duration::from_millis(20),
            ..config(1)
        };
        let a = SharedPeerManager::new(
            "b".into(),
            probing.clone(),
            Pre::factory([a_end]),
            &rt,
            health_tx(),
        )
        .unwrap();
        let b =
            SharedPeerManager::new("a".into(), probing, Pre::factory([b_end]), &rt, health_tx())
                .unwrap();
        let df = DataflowId::new();
        let _ra = a.register_dataflow(df, &[1], 4).await.unwrap();
        let _rb = b.register_dataflow(df, &[1], 4).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while a.metrics_snapshot()[0].avg_rtt.is_none() {
            assert!(tokio::time::Instant::now() < deadline, "no probe came back");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let snapshot = &a.metrics_snapshot()[0];
        assert_eq!(snapshot.pinned_lanes, 1);
        assert!(snapshot.total_frames > 0);
    }

    // --- Session --------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_transport_session_api() {
        let (a, _b, _relays) = managers(1);
        let managers = HashMap::from([("b".to_string(), a)]);
        let df = DataflowId::new();
        let mut session = SharedTransportSession::new(df, &managers, &[1, 2], 16)
            .await
            .unwrap();
        assert!(session.data_sender("b").is_some());
        assert!(session.progress_sender("b").is_some());
        assert!(session.control_sender("b").is_some());
        assert!(session.data_sender("nonexistent").is_none());
        assert_eq!(session.peer_node_ids().collect::<Vec<_>>(), vec!["b"]);
        assert_eq!(session.dataflow_id(), df);
        assert!(session.take_receivers().is_some());
        assert!(session.take_receivers().is_none());
        assert!(session.take_io_events().is_some());
        assert!(session.take_io_events().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dataframe_sender_tags_with_dataflow_id_and_registration() {
        let (tx, mut rx) = tokio_mpsc::channel::<Outbound>(16);
        let df = DataflowId::new();
        let sender = DataframeSender {
            dataflow_id: df,
            token: 7,
            tx,
        };
        sender
            .send(frame(DataflowId::new(), 1, vec![1, 2, 3]))
            .await
            .unwrap();
        let Some(Outbound::Payload {
            dataflow_id,
            token,
            frame: received,
        }) = rx.recv().await
        else {
            panic!("expected a payload entry");
        };
        assert_eq!((dataflow_id, token), (df, 7));
        assert_eq!(received.dataflow_id, df);
        assert_eq!(received.payload, vec![1, 2, 3]);
        drop(rx);
        assert_eq!(
            sender
                .send(frame(df, 1, vec![4]))
                .await
                .unwrap_err()
                .0
                .payload,
            vec![4]
        );
    }
}
