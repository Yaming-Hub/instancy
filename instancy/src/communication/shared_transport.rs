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
//! ├── Pool: `connections` connections, each with
//! │   ├── a writer task with two queues: control frames, always served
//! │   │   first, and payload frames (bounded: full means backpressure)
//! │   └── a reader task, which hands each frame on without ever waiting
//! ├── Control router: puts each control frame on its dataflow's connection
//! ├── Probe loop: a probe on each connection every `probe_interval`, and
//! │   the liveness verdicts (see "Connection liveness")
//! └── Per dataflow registration:
//!     ├── the pinned connection (chosen at registration, never changes)
//!     ├── its payload lane: frames are numbered as they are queued on the
//!     │   pinned connection
//!     ├── the sequence number the next frame from the peer must carry
//!     └── per-channel receivers
//!
//! SharedTransportSession (per dataflow, lightweight handle)
//! ├── References a SharedPeerManager for each peer
//! ├── data_sender(peer) → the dataflow's lane to the peer
//! ├── progress_sender(peer) → same as data_sender (FIFO preserved)
//! └── control_sender(peer) → priority channel
//! ```
//!
//! Nothing is shared between a peer's connections: a connection that is
//! slow or stuck holds up only the dataflows pinned to it.
//!
//! # Pinned lanes
//!
//! Data and progress frames of a dataflow to a peer share one sequenced
//! payload lane, which preserves the timely ordering invariant: data at time
//! T is sent before the progress message releasing T. When a dataflow
//! registers, its lane (and its control frames) is pinned to one pooled
//! connection, the one with the fewest lanes, so TCP keeps its frames in
//! order.
//!
//! A payload frame takes its lane's next sequence number as it is queued on
//! the pinned connection, in one step under the lane's lock: a frame that
//! can't be queued (the queue is full) takes no number, and the queue holds
//! the lane's frames in number order whichever task sends them. The receiver
//! checks the number: a frame that does not carry exactly the next number (a
//! lost or duplicated frame), or a lane whose frames arrive on two
//! connections, fails the dataflow ([`TransportError::OutOfSequence`],
//! [`TransportError::LaneViolation`]) instead of being delivered.
//!
//! # Connection liveness
//!
//! A connection can stop delivering without ever failing: a partition, a
//! frozen peer, a firewall that dropped its state, a half-open TCP
//! connection after a crash. With
//! [`SharedConnectionConfig::silence_timeout`] set (T, 30 s by default):
//!
//! - every [`probe_interval`](SharedConnectionConfig::probe_interval) (P)
//!   each node queues a small probe on each pooled connection (skipped when
//!   its payload queue is full: frames are already waiting to flow), so a
//!   healthy connection always has bytes arriving;
//! - a connection whose reader has read no bytes for T is dead. It fails
//!   like a read error ([`TransportError::ConnectionSilent`]): its reader
//!   stops, its writer is aborted at once (dropping what is queued), and the
//!   dataflows using it fail as below;
//! - a writer that has had bytes waiting and written none for T fails its
//!   connection like a write error ([`TransportError::WriteStalled`]), in
//!   the pool or out of it. This is the rule of TCP's `TCP_USER_TIMEOUT`,
//!   applied by instancy so that it covers every stream a factory returns.
//!
//! Both rules count bytes, not frames: a large frame that takes longer than
//! T on a slow link keeps its connection alive while its bytes move. A
//! reader never waits for a consumer (its channels are unbounded), so a slow
//! consumer can't make a healthy connection look silent. The rules judge one
//! connection, never a node: they fail only that connection's dataflows, and
//! a node leaving stays the application's to report
//! ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
//!
//! T must exceed P + D + 2S, where D is the longest a healthy connection
//! takes to deliver a byte and S the longest either node's network tasks
//! can be paused. A connection that stops delivering is then failed within
//! T + P + S on each node: by that node's own timer, or by seeing the
//! connection end. A dataflow that has received no frame from the peer yet
//! is not bound to a connection: its handshake times out instead.
//!
//! # Connection failures (fail-stop)
//!
//! Frames on a connection that fails may be lost, and nothing resends them.
//! So a failed connection fails exactly the dataflows that used it, on both
//! nodes, like a lost connection in Dedicated mode (and as in timely, which
//! stops on any connection error):
//!
//! - when its writer fails (a write error, or the stall rule), every
//!   dataflow whose lane is pinned to it is told
//!   ([`PeerIoEvent::WriteFailed`]), and the whole connection is closed (its
//!   reader stops too), so the peer sees it end even when the write half
//!   alone would tell it nothing (`tokio::io::split` halves, as with TLS);
//! - when its reader ends with an error (an I/O error, EOF mid-frame, a
//!   corrupt or oversize frame, the silence rule, or the writer's failure),
//!   every dataflow whose frames from the peer arrive on it, or whose lane is
//!   pinned to it, is told, the channels of a dataflow whose frames arrive on
//!   it close, and the connection leaves the pool;
//! - when the peer closes its side cleanly (EOF at a frame boundary), the
//!   same dataflows are told, but the lanes pinned to it keep writing until
//!   a write fails: a released manager keeps reading for its peers' last
//!   frames (goodbyes, acknowledgements, a `Cancel`), as in Dedicated mode.
//!   The connection takes no new lanes and is closed once its last lane
//!   unregisters;
//! - a payload frame that cannot be sent (on a reserved channel, or over the
//!   frame size limit) fails its dataflow and nobody else's.
//!
//! The failed connection leaves the pool. The pool is not redialled in the
//! background: the next dataflow that registers re-establishes missing
//! connections through the application's [`ConnectionFactory`]. A
//! connection failure is reported as
//! [`RuntimeEvent::TransportDegraded`](crate::RuntimeEvent::TransportDegraded),
//! which needs no action: the pool recovers on its own.
//!
//! # One run per `DataflowId`
//!
//! Connections outlive dataflows, and frames carry only the
//! [`DataflowId`], which identifies one run. A run that reused an earlier
//! run's id could take that run's late frames for its own, so a manager
//! refuses to register an id it remembers as finished (the most recent
//! 16,384) and drops the late frames of such an id; a retry needs a fresh
//! id. A frame is numbered into its own registration's lane as it is queued,
//! so a queued frame is written even if its registration ends meanwhile (a
//! peer whose run has finished drops it as late), and a frame sent after the
//! registration ended is dropped, never numbered into another registration's
//! lane.
//!
//! # Transport contract
//!
//! The application provides the connections and the node membership: it
//! tells instancy when a peer node has left
//! ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
//! instancy detects a connection that stops delivering (see "Connection
//! liveness") and owns safety: it never reports success with lost,
//! duplicated, reordered or corrupted data (every frame is checksummed, and
//! every lane is sequenced), and it fails the affected dataflows promptly
//! when a connection fails, falls silent, or the application reports a node
//! leaving. With `silence_timeout: None` a connection that silently stops
//! delivering makes its dataflows wait forever; they are never reported as
//! successful.
//!
//! # Probe Protocol
//!
//! Probes use the standard [`Frame`] wire format with a reserved
//! `PROBE_CHANNEL_ID` to avoid mixing wire formats on the same stream. They
//! keep each connection's bytes flowing for the liveness rule, and measure
//! its round-trip time for [`SharedPeerManager::metrics_snapshot`].

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

// Schedule tests (`--cfg instancy_shuttle`, Linux) substitute Shuttle's mutex
// for a lane's lock; see `lane_shuttle` below.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
use shuttle::sync::Mutex as LaneMutex;
#[cfg(not(all(test, target_os = "linux", instancy_shuttle)))]
use std::sync::Mutex as LaneMutex;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_util::sync::CancellationToken;

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

/// Capacity of a pooled connection's payload queue. When it is full, a
/// frame offered without waiting is refused (the pushing operator sees
/// backpressure) and takes no sequence number.
const WRITER_QUEUE: usize = 256;

/// A frame for a connection's writer: a payload frame with the sequence
/// number its lane gave it as it was queued (written in front of its
/// payload), or a probe.
struct Outgoing {
    seq: Option<u64>,
    frame: Frame,
}

/// The queues of one connection's writer, which serves `control` first.
#[derive(Clone)]
struct ConnQueues {
    /// Payload frames and probes. Bounded: a full queue is backpressure.
    payload: tokio_mpsc::Sender<Outgoing>,
    /// Control frames. Unbounded: routing one never waits for a connection.
    control: tokio_mpsc::UnboundedSender<Frame>,
}

/// Background tasks of a [`SharedPeerManager`].
///
/// Connection writers and readers are kept apart from the other tasks (the
/// control router and the probe loop): releasing the manager aborts the
/// other tasks immediately but closes the connections gracefully (see
/// [`linger`]).
#[derive(Default)]
struct TaskHandles {
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    writers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    readers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl TaskHandles {
    fn take(handles: &Mutex<Vec<tokio::task::JoinHandle<()>>>) -> Vec<tokio::task::JoinHandle<()>> {
        std::mem::take(&mut *handles.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn abort_tasks(&self) {
        for handle in self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
/// The caller has closed the pool and the lanes, which drops the writer
/// queues' senders and lets every writer finish the frames queued on its
/// connection (a dataflow's last frames: its end-of-stream markers,
/// acknowledgements or `Cancel`) and then close its half of the connection.
/// The readers keep reading until the peer closes its half too: closing a
/// socket with unread data makes the OS reset the connection, which can
/// destroy frames the peer has not read yet. A peer that stops reading, or
/// never closes, is cut off at the deadlines.
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
// Connection liveness
// ---------------------------------------------------------------------------

/// What the liveness rule knows about one connection (see the module
/// documentation): when its reader last read bytes, when its writer last
/// wrote some, and since when its writer has had a frame to write.
struct ConnLiveness {
    /// Times are nanoseconds since this instant (the manager's creation).
    epoch: tokio::time::Instant,
    last_read: AtomicU64,
    last_write: AtomicU64,
    /// One more than when the writer took the frame it is writing; 0 while
    /// it has none.
    waiting_since: AtomicU64,
    reader_open: AtomicBool,
    writer_open: AtomicBool,
    /// Stops the reader and the writer at once.
    kill: CancellationToken,
    /// Why the connection was killed (the first reason wins).
    reason: Mutex<Option<TransportError>>,
}

impl ConnLiveness {
    /// A connection established now: its silence is counted from now, so
    /// one that never delivers a byte is failed too.
    fn new(epoch: tokio::time::Instant) -> Self {
        let liveness = Self {
            epoch,
            last_read: AtomicU64::new(0),
            last_write: AtomicU64::new(0),
            waiting_since: AtomicU64::new(0),
            reader_open: AtomicBool::new(true),
            writer_open: AtomicBool::new(true),
            kill: CancellationToken::new(),
            reason: Mutex::new(None),
        };
        liveness.last_read.store(liveness.now(), Ordering::Relaxed);
        liveness
    }

    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// The reader read some bytes.
    fn read(&self) {
        self.last_read.store(self.now(), Ordering::Relaxed);
    }

    /// The writer wrote some bytes.
    fn wrote(&self) {
        self.last_write.store(self.now(), Ordering::Relaxed);
    }

    /// The writer took a frame to write.
    fn begin_write(&self) {
        self.waiting_since.store(self.now() + 1, Ordering::Relaxed);
    }

    /// The writer wrote its frame.
    fn end_write(&self) {
        self.waiting_since.store(0, Ordering::Relaxed);
    }

    /// How long the open reader has read nothing, at `now`.
    fn silent_for(&self, now: u64) -> Option<Duration> {
        self.reader_open.load(Ordering::Relaxed).then(|| {
            Duration::from_nanos(now.saturating_sub(self.last_read.load(Ordering::Relaxed)))
        })
    }

    /// How long the writer has had bytes waiting and written none, at `now`.
    fn stalled_for(&self, now: u64) -> Option<Duration> {
        let waiting = self.waiting_since.load(Ordering::Relaxed);
        if waiting == 0 || !self.writer_open.load(Ordering::Relaxed) {
            return None;
        }
        let since = (waiting - 1).max(self.last_write.load(Ordering::Relaxed));
        Some(Duration::from_nanos(now.saturating_sub(since)))
    }

    fn tasks_open(&self) -> bool {
        self.reader_open.load(Ordering::Relaxed) || self.writer_open.load(Ordering::Relaxed)
    }

    /// Stop the reader and the writer at once; the reader ends with
    /// `reason` (unless the connection was killed already).
    fn kill(&self, reason: TransportError) {
        self.reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(reason);
        self.kill.cancel();
    }

    /// Why the connection was killed.
    fn reason(&self) -> TransportError {
        self.reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or(TransportError::ConnectionClosed, TransportError::duplicate)
    }
}

/// Marks a connection's reader or writer task ended when dropped, also
/// when the task is aborted.
struct TaskOpen<'a>(&'a AtomicBool);

impl Drop for TaskOpen<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// A connection's read half that records every read returning bytes.
struct TimedReader {
    inner: DynReader,
    liveness: Arc<ConnLiveness>,
}

impl AsyncRead for TimedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.liveness.read();
        }
        polled
    }
}

/// A connection's write half that records every write taking bytes.
struct TimedWriter {
    inner: DynWriter,
    liveness: Arc<ConnLiveness>,
}

impl AsyncWrite for TimedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        if matches!(polled, Poll::Ready(Ok(written)) if written > 0) {
            self.liveness.wrote();
        }
        polled
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
///   whose other end no manager reads never delivers anything: the dataflows
///   pinned to it fail their handshake, and the connection is failed once it
///   has delivered nothing for
///   [`silence_timeout`](SharedConnectionConfig::silence_timeout).
/// - **Reliable, ordered bytes.** A stream must deliver bytes in order
///   without loss or duplication while it lives. instancy checksums every
///   frame and sequences every dataflow's lane, so a violation fails the
///   affected dataflows instead of being accepted.
/// - **Liveness.** A stream that stops delivering without ever failing (a
///   partition, a frozen peer) is detected by instancy: once it has
///   delivered no bytes for
///   [`silence_timeout`](SharedConnectionConfig::silence_timeout), or its
///   writer has written nothing for that long while bytes waited, it fails
///   and exactly the dataflows using it fail. A peer node that leaves is the
///   application's to report
///   ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
///   With `silence_timeout: None`, a stream that silently stops delivering
///   makes the dataflows using it wait forever; they never report success.
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
/// It sets no TCP keepalive: a connection that stops delivering (a peer that
/// vanished without resetting it, a partition) is detected by instancy's own
/// liveness rule
/// ([`SharedConnectionConfig::silence_timeout`]), which fails exactly the
/// dataflows using it. A peer node that leaves is still the application's to
/// report ([`RuntimeHandle::report_node_leave`](crate::RuntimeHandle::report_node_leave)).
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
// Lanes: numbering at enqueue
// ---------------------------------------------------------------------------

/// Why a lane takes no more frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneEnd {
    /// Its registration ended: later frames are dropped.
    Ended,
    /// Its connection failed, and the dataflow was told: later frames are
    /// dropped.
    Failed,
    /// The manager was released: later sends fail.
    Released,
}

struct LaneState {
    /// The sequence number of the next frame queued.
    next_seq: u64,
    /// The pinned connection's payload queue, until the lane ends.
    queue: Option<tokio_mpsc::Sender<Outgoing>>,
    end: Option<LaneEnd>,
}

/// One registration's payload lane to the peer.
///
/// A frame is numbered and queued on the pinned connection in one step,
/// under the lane's lock. So only a frame that is queued takes a number, the
/// queue holds the lane's frames in number order whichever task sends them,
/// and a lane that has ended takes no numbers. A queued frame is written even
/// if the registration ends meanwhile: its number belongs to that
/// registration's lane.
struct Lane {
    dataflow_id: DataflowId,
    /// The pinned connection.
    conn: usize,
    state: LaneMutex<LaneState>,
    /// The registration's connection events, for rejected frames.
    events: tokio_mpsc::Sender<PeerIoNotice>,
    /// To fail the connection when its writer is found gone.
    inner: Weak<Inner>,
}

/// What became of a frame offered to a lane.
#[derive(Debug)]
enum Enqueued {
    /// Numbered and queued on the connection.
    Admitted,
    /// The queue is full (only when not waiting): no number was taken.
    Full(Frame),
    /// Dropped: the registration ended, or its connection failed and the
    /// dataflow was told.
    Dropped,
    /// Never sendable (a reserved channel, or too large): reported to the
    /// dataflow, and no number was taken.
    Rejected,
    /// The connection's writer is gone, so the connection failed: the frame
    /// is lost (see [`Lane::writer_gone`]).
    WriterGone,
    /// The manager was released.
    Released(Frame),
}

impl Lane {
    fn new(
        dataflow_id: DataflowId,
        conn: usize,
        queue: tokio_mpsc::Sender<Outgoing>,
        events: tokio_mpsc::Sender<PeerIoNotice>,
        inner: Weak<Inner>,
    ) -> Self {
        Self {
            dataflow_id,
            conn,
            state: LaneMutex::new(LaneState {
                next_seq: 0,
                queue: Some(queue),
                end: None,
            }),
            events,
            inner,
        }
    }

    fn lock(&self) -> impl std::ops::DerefMut<Target = LaneState> + '_ {
        // Critical sections never panic; recover the data if one ever did.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Why `frame` can never go on a lane: the receiver handles the control
    /// and probe channels outside the lane, and a frame over the size limit
    /// (with its sequence number) cannot be written.
    fn rejection(frame: &Frame) -> Option<TransportError> {
        if [CONTROL_CHANNEL_ID, PROBE_CHANNEL_ID].contains(&frame.channel_id) {
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
        }
    }

    /// Fail only this dataflow: the connection is untouched.
    fn reject(&self, frame: &Frame, error: TransportError) {
        #[cfg(feature = "tracing")]
        tracing::error!(
            dataflow_id = %self.dataflow_id,
            channel_id = frame.channel_id,
            "payload frame rejected: {error}"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = frame;
        let _ = self
            .events
            .try_send(PeerIoNotice::new(PeerIoEvent::FrameRejected(error)));
    }

    /// `frame`'s fate on a lane that has ended.
    fn ended(state: &LaneState, frame: Frame) -> Enqueued {
        match state.end {
            Some(LaneEnd::Released) => Enqueued::Released(frame),
            _ => Enqueued::Dropped,
        }
    }

    /// Number `frame` and queue it, without waiting.
    fn try_enqueue(&self, frame: Frame) -> Enqueued {
        let rejection = Self::rejection(&frame);
        let mut state = self.lock();
        if state.queue.is_none() {
            return Self::ended(&state, frame);
        }
        if let Some(error) = rejection {
            drop(state);
            self.reject(&frame, error);
            return Enqueued::Rejected;
        }
        let LaneState {
            next_seq, queue, ..
        } = &mut *state;
        let Some(queue) = queue.as_ref() else {
            return Enqueued::Dropped;
        };
        match queue.try_reserve() {
            Ok(permit) => {
                let seq = *next_seq;
                *next_seq += 1;
                permit.send(Outgoing {
                    seq: Some(seq),
                    frame,
                });
                Enqueued::Admitted
            }
            Err(tokio_mpsc::error::TrySendError::Full(())) => Enqueued::Full(frame),
            Err(tokio_mpsc::error::TrySendError::Closed(())) => Enqueued::WriterGone,
        }
    }

    /// Number `frame` and queue it, waiting for room. Room is reserved
    /// first, so the number is taken (under the lock) only once the frame
    /// can be queued.
    async fn enqueue(&self, frame: Frame) -> Enqueued {
        let rejection = Self::rejection(&frame);
        let queue = {
            let state = self.lock();
            match &state.queue {
                Some(queue) => queue.clone(),
                None => return Self::ended(&state, frame),
            }
        };
        if let Some(error) = rejection {
            self.reject(&frame, error);
            return Enqueued::Rejected;
        }
        // Tokio's queue is opaque to the schedule tests: this scheduling
        // point lets them run other producers between the lane's lock and
        // the reservation (see `lane_shuttle`).
        #[cfg(all(test, target_os = "linux", instancy_shuttle))]
        shuttle::thread::yield_now();
        let Ok(permit) = queue.reserve().await else {
            return Enqueued::WriterGone;
        };
        let mut state = self.lock();
        if state.queue.is_none() {
            // The reserved room is given back.
            return Self::ended(&state, frame);
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        permit.send(Outgoing {
            seq: Some(seq),
            frame,
        });
        Enqueued::Admitted
    }

    /// The connection's writer is gone: its connection failed, and so did
    /// this lane (the dataflow is told once).
    fn writer_gone(&self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.fail_outbound(self.conn, TransportError::ConnectionClosed);
        }
    }

    /// Take no more frames (`end` says why). Frames already queued are
    /// still written.
    fn close(&self, end: LaneEnd) {
        let mut state = self.lock();
        state.queue = None;
        if state.end.is_none() || end == LaneEnd::Released {
            state.end = Some(end);
        }
    }
}

// ---------------------------------------------------------------------------
// Manager state
// ---------------------------------------------------------------------------

/// One pooled connection.
struct PooledConnection {
    metrics: Arc<ConnectionMetrics>,
    /// The writer's queues. Dropping them (with the lanes' clones) lets the
    /// writer finish what is queued and close its half of the connection.
    queues: ConnQueues,
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
    /// The dataflow's payload lane to the peer.
    lane: Arc<Lane>,
    /// Sequence number the next payload frame from the peer must carry.
    next_recv_seq: u64,
    /// The connection the peer's frames for this dataflow arrive on: set by
    /// the first one, normally the peer's handshake.
    inbound_conn: Option<usize>,
    /// Per-channel senders delivering the peer's frames. Unbounded: the
    /// connection's reader never waits for a consumer. Taken (which closes
    /// the channels) once the inbound side has failed.
    channel_senders: Option<HashMap<u64, tokio_mpsc::UnboundedSender<Vec<u8>>>>,
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
    /// Every connection whose reader or writer still runs, in the pool or
    /// not, for the liveness verdicts.
    liveness: BTreeMap<usize, Arc<ConnLiveness>>,
    registered: HashMap<DataflowId, DataflowRegistration>,
    /// Ids a spawn has claimed (by its registration token) and not yet
    /// registered: no other token may claim or register them. A claim
    /// becomes the registration, is dropped (a refused spawn), or is retired
    /// into `completed` (a spawn that failed after claiming).
    claims: HashMap<DataflowId, u64>,
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
    /// frame, or the connection was killed (it fell silent, or its writer
    /// failed). `owner` is the dataflow of the failing frame, when its
    /// header checksum was verified.
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
        senders: Option<HashMap<u64, tokio_mpsc::UnboundedSender<Vec<u8>>>>,
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
    /// Liveness times are measured from here.
    epoch: tokio::time::Instant,
}

impl Inner {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // Critical sections never panic; recover the data if one ever did.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
        let (payload, payload_rx) = tokio_mpsc::channel::<Outgoing>(WRITER_QUEUE);
        let (control, control_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let liveness = Arc::new(ConnLiveness::new(self.epoch));
        let (conn_id, metrics) = {
            let mut state = self.state();
            let conn_id = state.next_conn_id;
            state.next_conn_id += 1;
            let metrics = Arc::new(ConnectionMetrics::new(conn_id, self.config.rtt_ema_alpha));
            state.connections.insert(
                conn_id,
                PooledConnection {
                    metrics: Arc::clone(&metrics),
                    queues: ConnQueues { payload, control },
                    pinned: 0,
                    peer_closed: false,
                },
            );
            state.liveness.insert(conn_id, Arc::clone(&liveness));
            (conn_id, metrics)
        };
        let writer = TimedWriter {
            inner: writer,
            liveness: Arc::clone(&liveness),
        };
        let reader = TimedReader {
            inner: reader,
            liveness: Arc::clone(&liveness),
        };
        let writer_handle = self.runtime_handle.spawn(writer_task(
            Arc::downgrade(self),
            conn_id,
            writer,
            payload_rx,
            control_rx,
            metrics,
            Arc::clone(&liveness),
        ));
        let reader_handle =
            self.runtime_handle
                .spawn(reader_task(Arc::clone(self), conn_id, reader, liveness));
        push_task_handle(&self.task_handles.writers, writer_handle);
        push_task_handle(&self.task_handles.readers, reader_handle);
        conn_id
    }

    /// Queue a control frame on its dataflow's pinned connection (any
    /// connection for a dataflow not registered here). Never waits: the
    /// control queues are unbounded.
    fn route_control(self: &Arc<Self>, frame: Frame) {
        let target = {
            let state = self.state();
            match state.registered.get(&frame.dataflow_id) {
                // Its lane failed and the dataflow was told.
                Some(reg) if reg.outbound_failed => None,
                Some(reg) => state
                    .connections
                    .get(&reg.conn)
                    .map(|conn| (reg.conn, conn.queues.control.clone())),
                // A dataflow that failed before registering here, or has
                // unregistered (a late Cancel): any pool connection will do.
                None => state
                    .connections
                    .iter()
                    .find(|(_, conn)| !conn.peer_closed)
                    .map(|(id, conn)| (*id, conn.queues.control.clone())),
            }
        };
        let Some((conn_id, control)) = target else {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                peer = %self.peer_node_id,
                dataflow_id = %frame.dataflow_id,
                "no connection for a control frame; dropping it"
            );
            return;
        };
        if control.send(frame).is_err() {
            // The writer stopped: its connection failed, and this frame is lost.
            self.fail_outbound(conn_id, TransportError::ConnectionClosed);
        }
    }

    /// Handle a frame read from connection `conn_id`. Never waits: the
    /// channels are unbounded, so the reader never waits for a consumer.
    fn on_frame(self: &Arc<Self>, conn_id: usize, frame: Frame) {
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
            let _ = tx.send(payload);
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
    ) -> Result<Option<(tokio_mpsc::UnboundedSender<Vec<u8>>, Vec<u8>)>, TransportError> {
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
                // Best effort: a full queue already has bytes to send.
                let _ = conn.queues.payload.try_send(Outgoing {
                    seq: None,
                    frame: reply,
                });
            }
        }
    }

    /// Writing to connection `conn_id` failed (a write error, or the stall
    /// rule): the frames queued on it are lost. Fail every dataflow whose
    /// lane is pinned to it, and stop using it. Its reader is stopped next,
    /// which closes the whole connection (see [`writer_task`]) and fails the
    /// dataflows reading from it (see [`Self::fail_inbound`]). Returns
    /// whether the failure was reported (the connection was in the pool, or
    /// lanes were pinned to it).
    fn fail_outbound(self: &Arc<Self>, conn_id: usize, error: TransportError) -> bool {
        let (reports, reported) = {
            let mut state = self.state();
            let removed = state.connections.remove(&conn_id).is_some();
            let mut reports = Vec::new();
            for reg in state.registered.values_mut() {
                if reg.conn == conn_id && !reg.outbound_failed {
                    reg.outbound_failed = true;
                    reg.lane.close(LaneEnd::Failed);
                    reports.push(Report::Event {
                        events: reg.events.clone(),
                        event: PeerIoEvent::WriteFailed(error.duplicate()),
                    });
                }
            }
            let reported = removed || !reports.is_empty();
            if reported {
                self.report_health(conn_id, &format!("writing failed: {error}"));
            }
            (reports, reported)
        };
        self.deliver(reports);
        reported
    }

    /// Connection `conn_id`'s read side ended. Frames the peer sent on it
    /// after the last one read are lost: fail every dataflow whose frames
    /// from the peer arrive on it (closing its channels once that was
    /// reported).
    ///
    /// A read error (an I/O error, EOF mid-frame, a corrupt or oversize
    /// frame, the silence rule, or the writer's failure) breaks the whole
    /// connection: the frames queued on it may be lost too, so the dataflows
    /// whose lane is pinned to it fail as well, and it leaves the pool. A
    /// clean EOF is the peer closing its side only: it may still read (a
    /// released manager lingers for the last frames of its peers, see
    /// [`linger`]), so the lanes pinned here are told but keep writing until
    /// a write fails, and the connection only stops taking new lanes (see
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
                    reg.lane.close(LaneEnd::Failed);
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

    /// Report a failed pooled connection (see
    /// [`RuntimeEvent::TransportDegraded`](crate::RuntimeEvent::TransportDegraded)).
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

    /// The liveness verdicts (see the module documentation), at one probe
    /// tick: a connection whose open reader has read nothing for `timeout`
    /// fails like a read error, and one whose writer has had bytes waiting
    /// and written none for `timeout` fails like a write error.
    fn judge_liveness(self: &Arc<Self>, timeout: Duration) {
        let watched: Vec<(usize, Arc<ConnLiveness>)> = {
            let mut state = self.state();
            state.liveness.retain(|_, liveness| liveness.tasks_open());
            state
                .liveness
                .iter()
                .map(|(&id, liveness)| (id, Arc::clone(liveness)))
                .collect()
        };
        for (conn_id, liveness) in watched {
            if liveness.kill.is_cancelled() {
                continue;
            }
            let now = liveness.now();
            if liveness
                .silent_for(now)
                .is_some_and(|silent| silent >= timeout)
            {
                // The reader ends with this error (see `reader_task`), and
                // the writer is aborted.
                liveness.kill(TransportError::ConnectionSilent { timeout });
            } else if liveness
                .stalled_for(now)
                .is_some_and(|stalled| stalled >= timeout)
            {
                let error = TransportError::WriteStalled { timeout };
                let detail = error.to_string();
                if !self.fail_outbound(conn_id, error) {
                    // Out of the pool, with no lane left: still a failure.
                    self.report_health(conn_id, &format!("writing failed: {detail}"));
                }
                liveness.kill(writer_failed(&detail));
            }
        }
    }

    /// The manager is released: its pool closes, and its lanes take no more
    /// frames. What is already queued is still written (see [`linger`]).
    fn release(&self) {
        let mut state = self.state();
        state.connections.clear();
        for reg in state.registered.values() {
            reg.lane.close(LaneEnd::Released);
        }
    }

    /// Remove `dataflow_id`'s registration (only `token`'s, if given). Its
    /// lane takes no more frames; those already queued are still written.
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
                reg.lane.close(LaneEnd::Ended);
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

    /// Why `token` cannot claim or register `dataflow_id` here: it is
    /// registered, it already ran here, or another token claimed it. `None`
    /// if the id is unknown here or `token`'s own claim.
    fn id_unavailable(
        &self,
        state: &State,
        dataflow_id: &DataflowId,
        token: u64,
    ) -> Option<TransportError> {
        let peer = &self.peer_node_id;
        let claimed_by_other = state
            .claims
            .get(dataflow_id)
            .is_some_and(|&claim| claim != token);
        let reason = if state.registered.contains_key(dataflow_id) || claimed_by_other {
            format!("dataflow {dataflow_id} is already registered with peer {peer}")
        } else if state.completed.contains(dataflow_id) {
            format!(
                "dataflow {dataflow_id} already ran with peer {peer}: a DataflowId \
                 identifies one run, so a retry needs a fresh DataflowId"
            )
        } else {
            return None;
        };
        Some(TransportError::InvalidRegistration { reason })
    }
}

/// The error a connection's reader ends with after its writer failed with
/// `detail`.
fn writer_failed(detail: &str) -> TransportError {
    TransportError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        format!("the pooled connection was closed after writing to it failed: {detail}"),
    ))
}

/// A refused [`SharedPeerManager::register`].
#[derive(Debug)]
pub(crate) struct RegisterError {
    pub(crate) error: TransportError,
    /// The id belongs to another run here (registered, or already ran): a
    /// refused spawn must send nothing under it, which would reach that run.
    pub(crate) id_unavailable: bool,
}

impl RegisterError {
    fn id_unavailable(error: TransportError) -> Self {
        Self {
            error,
            id_unavailable: true,
        }
    }
}

impl From<TransportError> for RegisterError {
    fn from(error: TransportError) -> Self {
        Self {
            error,
            id_unavailable: false,
        }
    }
}

/// Release a dataflow's or a spawn's reference to its peer managers. If it
/// was the last one (per-job managers), close them: wait, bounded by
/// `timeout`, until what was queued on them has been written to the
/// connections, since peers may still be waiting for it. (`into_inner`,
/// unlike `try_unwrap`, hands the managers to exactly one of several holders
/// releasing them at once.)
pub(crate) async fn release_managers(
    managers: Arc<HashMap<String, SharedPeerManager>>,
    timeout: Duration,
) {
    if let Some(owned) = Arc::into_inner(managers) {
        let mut closes = tokio::task::JoinSet::new();
        for manager in owned.into_values() {
            closes.spawn(manager.close(timeout));
        }
        while closes.join_next().await.is_some() {}
    }
}

/// A random registration token (see [`DataflowRegistration::token`]).
pub(crate) fn new_registration_token() -> u64 {
    uuid::Uuid::new_v4().as_u64_pair().0
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// Write one frame from a connection's queues: a payload frame with its
/// sequence number in front of its payload, or a probe.
async fn write_outgoing(
    framed: &mut FramedWriter<TimedWriter>,
    seq: Option<u64>,
    frame: &Frame,
) -> Result<(), TransportError> {
    match seq {
        Some(seq) => framed.write_prefixed_frame(frame, &seq.to_le_bytes()).await,
        None => framed.write_frame(frame).await,
    }
}

/// Per-connection writer task: writes the queued frames to the connection,
/// control frames first.
///
/// On a write error the frames queued on the connection are lost: it fails
/// the dataflows pinned to the connection, then kills the connection, which
/// stops the reader, so both halves are dropped and the whole connection
/// closes. Dropping only the write half would tell the peer nothing with
/// halves such as `tokio::io::split`'s (TLS streams, pipes): the peer would
/// wait for frames that never come, and keep the connection in its pool
/// while this side replaces it. With the connection closed, the peer fails
/// the same dataflows and both pools refill together. A killed connection
/// (it fell silent, or its writer stalled) stops writing at once.
async fn writer_task(
    inner: Weak<Inner>,
    conn_id: usize,
    writer: TimedWriter,
    mut payload_rx: tokio_mpsc::Receiver<Outgoing>,
    mut control_rx: tokio_mpsc::UnboundedReceiver<Frame>,
    metrics: Arc<ConnectionMetrics>,
    liveness: Arc<ConnLiveness>,
) {
    let _open = TaskOpen(&liveness.writer_open);
    let mut framed = FramedWriter::new(writer);
    let (mut control_open, mut payload_open) = (true, true);
    while control_open || payload_open {
        let (seq, frame) = tokio::select! {
            biased;
            () = liveness.kill.cancelled() => return,
            frame = control_rx.recv(), if control_open => match frame {
                Some(frame) => (None, frame),
                None => {
                    control_open = false;
                    continue;
                }
            },
            next = payload_rx.recv(), if payload_open => match next {
                Some(Outgoing { seq, frame }) => (seq, frame),
                None => {
                    payload_open = false;
                    continue;
                }
            },
        };
        let size = frame.payload.len() + seq.map_or(0, |_| SEQUENCE_PREFIX);
        let is_user_traffic = frame.channel_id != PROBE_CHANNEL_ID;
        liveness.begin_write();
        let written = tokio::select! {
            biased;
            () = liveness.kill.cancelled() => return,
            written = write_outgoing(&mut framed, seq, &frame) => written,
        };
        liveness.end_write();
        if let Err(error) = written {
            let detail = error.to_string();
            if let Some(inner) = inner.upgrade() {
                inner.fail_outbound(conn_id, error);
            }
            liveness.kill(writer_failed(&detail));
            return;
        }
        metrics.record_write(size, is_user_traffic);
    }
    // Nothing more will be written: tell the peer. Only some write halves
    // do that when dropped (`OwnedWriteHalf`); with others (TLS streams,
    // pipes, `tokio::io::split`) the peer would not see the end of the
    // stream until the whole connection is dropped. The reader keeps going
    // until the peer closes its side.
    let mut writer = framed.into_inner();
    let shutdown = tokio::time::timeout(WRITER_DRAIN_GRACE, writer.shutdown());
    tokio::select! {
        biased;
        () = liveness.kill.cancelled() => {}
        _ = shutdown => {}
    }
}

/// Per-connection reader task: reads frames and hands them to
/// [`Inner::on_frame`], which never waits, until the connection ends or is
/// killed (it fell silent, or its writer failed).
async fn reader_task(
    inner: Arc<Inner>,
    conn_id: usize,
    reader: TimedReader,
    liveness: Arc<ConnLiveness>,
) {
    let open = TaskOpen(&liveness.reader_open);
    let mut framed = FramedReader::new(reader);
    let reading = async {
        loop {
            match framed.read_frame().await {
                Ok(frame) => inner.on_frame(conn_id, frame),
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
    // A frame cut off by the kill belongs to a lane on this connection,
    // which `fail_inbound` fails. The kill is polled first: under sustained
    // traffic `reading` uses up the task's cooperative budget, and a kill
    // polled after it would never fire.
    let end = tokio::select! {
        biased;
        () = liveness.kill.cancelled() => InboundEnd::Failed {
            error: liveness.reason(),
            owner: None,
        },
        end = reading => end,
    };
    drop(open);
    inner.fail_inbound(conn_id, end);
}

/// Control router: puts each control frame on its connection's control
/// queue, without ever waiting, so a stuck connection holds up no other
/// dataflow's control frames. A message on `barriers` asks it to route
/// everything queued so far first (see [`SharedPeerManager::close`]).
async fn control_router(
    inner: Arc<Inner>,
    mut control_rx: tokio_mpsc::Receiver<Frame>,
    mut barriers: tokio_mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
) {
    loop {
        tokio::select! {
            biased;
            frame = control_rx.recv() => match frame {
                Some(frame) => inner.route_control(frame),
                None => return,
            },
            Some(routed) = barriers.recv() => {
                while let Ok(frame) = control_rx.try_recv() {
                    inner.route_control(frame);
                }
                let _ = routed.send(());
            }
        }
    }
}

/// Periodic probe loop: queues a probe on each pooled connection, then
/// applies the liveness verdicts.
async fn probe_loop(inner: Arc<Inner>) {
    let mut ticker = tokio::time::interval(inner.config.probe_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The first probe goes out after one full interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let queues: Vec<_> = inner
            .state()
            .connections
            .values()
            .filter(|conn| !conn.peer_closed)
            .map(|conn| conn.queues.payload.clone())
            .collect();
        for queue in queues {
            let seq = inner.probe_counter.next_seq();
            let send_ts = inner.prober.timestamp().now_nanos();
            inner.prober.record_probe_sent(seq, send_ts);
            let frame = Frame {
                dataflow_id: DataflowId::nil(),
                channel_id: PROBE_CHANNEL_ID,
                payload: ProbeMessage::new_request(seq, send_ts).encode().to_vec(),
            };
            // Best effort: a full queue already has bytes waiting to flow.
            let _ = queue.try_send(Outgoing { seq: None, frame });
        }
        if let Some(timeout) = inner.config.silence_timeout {
            inner.judge_liveness(timeout);
        }
    }
}

// ---------------------------------------------------------------------------
// SharedPeerManager — owns pooled connections to one peer
// ---------------------------------------------------------------------------

/// The channel receivers and connection events of a dataflow registered
/// with a [`SharedPeerManager`], by channel id.
type Registered = (
    HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
    tokio_mpsc::Receiver<PeerIoNotice>,
);

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
    /// Control sender: high-priority frames, routed to their dataflow's
    /// connection by the control router.
    control_tx: tokio_mpsc::Sender<Frame>,
    /// Asks the control router to route everything queued so far.
    router_barrier: tokio_mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for SharedPeerManager {
    fn drop(&mut self) {
        self.inner.task_handles.abort_tasks();
        // Close the connections gracefully in the background (if the runtime
        // is shutting down, it ends these tasks itself): closing the pool and
        // the lanes lets the writers finish what is queued on them.
        let writers = TaskHandles::take(&self.inner.task_handles.writers);
        let readers = TaskHandles::take(&self.inner.task_handles.readers);
        self.inner.release();
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
        // `Instant::now` reads the runtime's clock (paused in tests).
        let epoch = {
            let _entered = runtime_handle.enter();
            tokio::time::Instant::now()
        };
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
            epoch,
        });

        let (control_tx, control_rx) = tokio_mpsc::channel::<Frame>(256);
        let (router_barrier, barriers) = tokio_mpsc::unbounded_channel();
        let router = runtime_handle.spawn(control_router(Arc::clone(&inner), control_rx, barriers));
        push_task_handle(&inner.task_handles.tasks, router);
        let probes = runtime_handle.spawn(probe_loop(Arc::clone(&inner)));
        push_task_handle(&inner.task_handles.tasks, probes);

        Ok(Self {
            inner,
            control_tx,
            router_barrier,
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
    /// The receivers are unbounded: a pooled connection's reader never waits
    /// for a consumer, which would hold up every dataflow on the connection
    /// and make a healthy connection look silent (see the
    /// [module documentation](self)). `channel_capacity` must be at least 1
    /// and is otherwise unused.
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
    ) -> Result<Registered, TransportError> {
        self.register(
            dataflow_id,
            new_registration_token(),
            channel_ids,
            channel_capacity,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .map(|(receivers, events, _)| (receivers, events))
        .map_err(|refused| refused.error)
    }

    /// [`Self::register_dataflow`] with the registration's token (see
    /// [`Self::unregister_registration`]) and the flag that holds the
    /// channels open until a read-side event was handed on. Also returns the
    /// registration's sender. A refusal says whether the id belonged to
    /// another run (see [`RegisterError`]).
    pub(crate) async fn register(
        &self,
        dataflow_id: DataflowId,
        token: u64,
        channel_ids: &[u64],
        channel_capacity: usize,
        hold_channels: Arc<AtomicBool>,
    ) -> Result<
        (
            HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
            tokio_mpsc::Receiver<PeerIoNotice>,
            DataframeSender,
        ),
        RegisterError,
    > {
        let invalid = |reason: String| TransportError::InvalidRegistration { reason };
        if channel_capacity == 0 {
            return Err(invalid("channel capacity must be at least 1".into()).into());
        }
        let mut seen = HashSet::with_capacity(channel_ids.len());
        for &ch_id in channel_ids {
            if ch_id == PROBE_CHANNEL_ID {
                return Err(invalid(format!(
                    "channel {PROBE_CHANNEL_ID} is reserved for connection probes"
                ))
                .into());
            }
            if !seen.insert(ch_id) {
                return Err(invalid(format!("channel {ch_id} registered twice")).into());
            }
        }
        let unavailable = {
            let state = self.inner.state();
            self.inner.id_unavailable(&state, &dataflow_id, token)
        };
        if let Some(error) = unavailable {
            return Err(RegisterError::id_unavailable(error));
        }

        self.ensure_connections().await?;

        let mut receivers = HashMap::new();
        let mut channel_senders = HashMap::new();
        for ch_id in std::iter::once(CONTROL_CHANNEL_ID).chain(
            channel_ids
                .iter()
                .copied()
                .filter(|&id| id != CONTROL_CHANNEL_ID),
        ) {
            let (tx, rx) = tokio_mpsc::unbounded_channel();
            channel_senders.insert(ch_id, tx);
            receivers.insert(ch_id, rx);
        }
        // Rare events; a full queue already carries a failure.
        let (events_tx, events_rx) = tokio_mpsc::channel(8);

        let mut state = self.inner.state();
        // Re-check: a concurrent registration (or claim) of the same dataflow
        // may have completed (or finished) while connections were established.
        if let Some(error) = self.inner.id_unavailable(&state, &dataflow_id, token) {
            return Err(RegisterError::id_unavailable(error));
        }
        let State {
            connections,
            registered,
            claims,
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
            ))
            .into());
        };
        conn.pinned += 1;
        let lane = Arc::new(Lane::new(
            dataflow_id,
            conn_id,
            conn.queues.payload.clone(),
            events_tx.clone(),
            Arc::downgrade(&self.inner),
        ));
        let mut reg = DataflowRegistration {
            token,
            conn: conn_id,
            lane: Arc::clone(&lane),
            next_recv_seq: 0,
            inbound_conn: None,
            channel_senders: Some(channel_senders),
            events: events_tx,
            hold_channels,
            outbound_failed: false,
            inbound_failed: false,
        };
        // Deliver control frames that arrived before registration.
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
                let _ = tx.send(payload);
            }
        }
        // The registration takes over this token's claim, if it holds one.
        if claims.get(&dataflow_id) == Some(&token) {
            claims.remove(&dataflow_id);
        }
        registered.insert(dataflow_id, reg);
        drop(state);

        Ok((receivers, events_rx, DataframeSender { lane }))
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

    /// Wait until every control frame queued on [`Self::control_sender`]
    /// before this call has been put on its connection (whose writer sends
    /// it even once the manager is released).
    pub(crate) async fn route_queued_control(&self) {
        let (routed, done) = tokio::sync::oneshot::channel();
        if self.router_barrier.send(routed).is_ok() {
            let _ = done.await;
        }
    }

    /// Release the manager, waiting until what was queued on its connections
    /// has been written, bounded by `timeout`.
    ///
    /// Control frames still waiting for the control router (a `Cancel`, for
    /// example) are first put on their connections. Dropping a manager also
    /// closes its connections gracefully in the background (see [`linger`]);
    /// this waits until the writers are done, so the caller knows its frames
    /// have been handed to the connections when it returns (unless the
    /// timeout expired, in which case the remaining writers are aborted). The
    /// readers keep waiting for the peer to close in the background.
    pub(crate) async fn close(self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = tokio::time::timeout_at(deadline, self.route_queued_control()).await;
        self.inner.task_handles.abort_tasks();
        let writers = TaskHandles::take(&self.inner.task_handles.writers);
        let readers = TaskHandles::take(&self.inner.task_handles.readers);
        self.inner.release();
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
    /// frames. Its payload frames already queued on its connection are still
    /// written; frames sent after this are dropped. The id is remembered
    /// (boundedly) as finished: it cannot be registered again, and the late
    /// frames of this run are dropped.
    pub async fn unregister_dataflow(&self, dataflow_id: &DataflowId) {
        self.inner.unregister(dataflow_id, None);
    }

    /// Unregister `dataflow_id` only if it is still the registration made
    /// with `token`: a cleanup must not remove another registration of the
    /// same id.
    pub(crate) async fn unregister_registration(&self, dataflow_id: &DataflowId, token: u64) {
        self.inner.unregister(dataflow_id, Some(token));
    }

    /// Claim `dataflow_id` for the registration `token` will make: no other
    /// token can claim or register it until the claim is registered (it then
    /// becomes the registration), dropped or retired.
    ///
    /// # Errors
    ///
    /// [`TransportError::InvalidRegistration`] if the id is registered here,
    /// already ran here, or another token claimed it.
    pub(crate) fn claim(&self, dataflow_id: DataflowId, token: u64) -> Result<(), TransportError> {
        let mut state = self.inner.state();
        if let Some(error) = self.inner.id_unavailable(&state, &dataflow_id, token) {
            return Err(error);
        }
        state.claims.insert(dataflow_id, token);
        Ok(())
    }

    /// Drop `token`'s claim on `dataflow_id`, leaving the id available: the
    /// claim of a spawn refused before it owned the id everywhere.
    pub(crate) fn drop_claim(&self, dataflow_id: &DataflowId, token: u64) {
        let mut state = self.inner.state();
        if state.claims.get(dataflow_id) == Some(&token) {
            state.claims.remove(dataflow_id);
        }
    }

    /// Retire `token`'s claim on `dataflow_id`, if it still holds one (it did
    /// not register): the id is remembered as finished, like a dataflow that
    /// unregistered, so it can never be claimed or registered again.
    pub(crate) fn retire_claim(&self, dataflow_id: &DataflowId, token: u64) {
        let mut state = self.inner.state();
        if state.claims.get(dataflow_id) == Some(&token) {
            state.claims.remove(dataflow_id);
            state.completed.insert(*dataflow_id);
            state.pending_control.take(dataflow_id);
        }
    }

    /// A sender for `dataflow_id`'s payload frames (data and progress),
    /// bound to its current registration: frames it sends after that
    /// registration ends are dropped. `None` if the dataflow is not
    /// registered.
    pub fn dataflow_sender(&self, dataflow_id: &DataflowId) -> Option<DataframeSender> {
        let lane = Arc::clone(&self.inner.state().registered.get(dataflow_id)?.lane);
        Some(DataframeSender { lane })
    }

    /// Get the control sender for submitting high-priority frames.
    ///
    /// Control frames bypass the lanes' numbering. Each is routed to its
    /// dataflow's pinned connection (any connection for a dataflow not
    /// registered here), whose writer sends control frames before payload.
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
    /// `queued_frames` counts the payload queue only.
    pub fn metrics_snapshot(&self) -> Vec<ConnectionSnapshot> {
        self.inner
            .state()
            .connections
            .values()
            .filter(|conn| !conn.peer_closed)
            .map(|conn| ConnectionSnapshot {
                id: conn.metrics.id,
                queued_frames: conn.queues.payload.max_capacity() - conn.queues.payload.capacity(),
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
    #[allow(clippy::type_complexity)]
    receivers: Option<HashMap<String, HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>>>,
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
    /// - `channel_capacity`: Must be at least 1; the per-channel receivers
    ///   are unbounded (see [`SharedPeerManager::register_dataflow`])
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
        .map_err(|refused| refused.error)
    }

    /// [`Self::new`] registering with `token` (see
    /// [`SharedPeerManager::unregister_registration`]). A refusal says whether
    /// the id belonged to another run on some peer's manager.
    pub(crate) async fn with_token(
        dataflow_id: DataflowId,
        token: u64,
        peer_managers: &HashMap<String, SharedPeerManager>,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> Result<Self, RegisterError> {
        let hold_channels = Arc::new(AtomicBool::new(false));
        let mut payload_senders = HashMap::new();
        let mut control_senders = HashMap::new();
        let mut all_receivers = HashMap::new();
        let mut io_events = HashMap::new();

        for (peer_id, manager) in peer_managers {
            let (receivers, events, sender) = match manager
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

            payload_senders.insert(peer_id.clone(), sender);
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
    /// Returns the dataflow's lane to the peer. Data and progress share the
    /// same sequenced lane to preserve the timely ordering invariant.
    pub fn data_sender(&self, peer_node_id: &str) -> Option<DataframeSender> {
        self.payload_senders.get(peer_node_id).cloned()
    }

    /// Get a progress sender for a peer.
    ///
    /// Returns the same lane as [`data_sender`](Self::data_sender).
    pub fn progress_sender(&self, peer_node_id: &str) -> Option<DataframeSender> {
        self.data_sender(peer_node_id)
    }

    /// Get a control-priority sender for a peer.
    ///
    /// Control frames bypass sequencing and have highest priority.
    pub fn control_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.control_senders.get(peer_node_id)
    }

    /// Take the per-peer channel receivers (can only be called once). They
    /// are unbounded (see [`SharedPeerManager::register_dataflow`]).
    #[allow(clippy::type_complexity)]
    pub fn take_receivers(
        &mut self,
    ) -> Option<HashMap<String, HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>>> {
        self.receivers.take()
    }

    /// Take the per-peer connection events (can only be called once).
    ///
    /// Each event tells this dataflow that its link with a peer failed (a
    /// pooled connection it uses failed or fell silent, a frame from the
    /// peer was out of sequence, or one of its frames was rejected). Call
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
// DataframeSender — a registration's payload lane
// ---------------------------------------------------------------------------

/// A sender of one dataflow registration's payload frames (see
/// [`SharedPeerManager::dataflow_sender`]).
///
/// It queues each frame straight on the registration's pinned connection,
/// numbering it as it is queued. A frame it sends after the registration
/// has ended, or after its connection failed (the dataflow is told), is
/// dropped, never numbered into another registration's lane.
#[derive(Clone)]
pub struct DataframeSender {
    lane: Arc<Lane>,
}

impl DataframeSender {
    /// Send a frame, waiting for room in its connection's queue.
    ///
    /// The frame's `dataflow_id` is normalized to this sender's dataflow, and
    /// it is queued on the dataflow's pinned connection with the lane's next
    /// sequence number. While the connection is stuck this waits, until the
    /// liveness rule fails the connection.
    ///
    /// # Errors
    ///
    /// Returns the frame if the manager was released.
    pub async fn send(&self, mut frame: Frame) -> Result<(), tokio_mpsc::error::SendError<Frame>> {
        frame.dataflow_id = self.lane.dataflow_id;
        match self.lane.enqueue(frame).await {
            // `enqueue` waits for room, so never finds the queue full.
            Enqueued::Released(frame) | Enqueued::Full(frame) => {
                Err(tokio_mpsc::error::SendError(frame))
            }
            Enqueued::WriterGone => {
                self.lane.writer_gone();
                Ok(())
            }
            Enqueued::Admitted | Enqueued::Dropped | Enqueued::Rejected => Ok(()),
        }
    }

    /// Try to send a frame without waiting.
    ///
    /// # Errors
    ///
    /// [`TrySendError::Full`](tokio_mpsc::error::TrySendError::Full) if the
    /// connection's queue is full (no sequence number was taken; try again
    /// later), [`TrySendError::Closed`](tokio_mpsc::error::TrySendError::Closed)
    /// if the manager was released.
    pub fn try_send(&self, mut frame: Frame) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        frame.dataflow_id = self.lane.dataflow_id;
        match self.lane.try_enqueue(frame) {
            Enqueued::Full(frame) => Err(tokio_mpsc::error::TrySendError::Full(frame)),
            Enqueued::Released(frame) => Err(tokio_mpsc::error::TrySendError::Closed(frame)),
            Enqueued::WriterGone => {
                self.lane.writer_gone();
                Ok(())
            }
            Enqueued::Admitted | Enqueued::Dropped | Enqueued::Rejected => Ok(()),
        }
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
            silence_timeout: None,
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

    /// Queue a payload frame of `dataflow_id`'s current registration (none
    /// if it is not registered).
    async fn send(
        manager: &SharedPeerManager,
        dataflow_id: DataflowId,
        channel_id: u64,
        payload: Vec<u8>,
    ) {
        if let Some(sender) = manager.dataflow_sender(&dataflow_id) {
            sender
                .send(frame(dataflow_id, channel_id, payload))
                .await
                .unwrap();
        }
    }

    async fn recv<T>(rx: &mut tokio_mpsc::UnboundedReceiver<T>) -> Option<T> {
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

    /// A frame sent through a sender of a finished registration is dropped:
    /// it is never numbered into another registration of the same id
    /// (possible only once the id is no longer remembered).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_finished_registrations_frames_never_reach_another() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _first = manager.register_dataflow(df, &[1], 4).await.unwrap();
        let old = manager.dataflow_sender(&df).unwrap();
        manager.unregister_dataflow(&df).await;
        // Forget the id, as after 16,384 later dataflows.
        manager.inner.state().completed = RecentIds::default();
        let (mut rx, _events) = manager.register_dataflow(df, &[1], 4).await.unwrap();
        old.send(frame(df, 1, b"old run".to_vec())).await.unwrap();
        old.try_send(frame(df, 1, b"old run".to_vec())).unwrap();
        send(&manager, df, 1, b"new run".to_vec()).await;
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
        let (mut rx, mut events, _) = manager
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
            let notice = tokio::time::timeout(Duration::from_secs(5), events.recv())
                .await
                .expect("no connection event within 5 s")
                .unwrap();
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

    /// A frame queued on its connection is written even once its
    /// registration has ended (DA R58): its number belongs to that
    /// registration's lane. Frames sent after the end are dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_frames_are_written_after_unregistration() {
        let (manager, peer) = manager_with_raw_peer();
        let df = DataflowId::new();
        let _registered = manager.register_dataflow(df, &[1], 16).await.unwrap();
        let sender = manager.dataflow_sender(&df).unwrap();
        for i in 0..200u8 {
            sender.send(frame(df, 1, vec![i])).await.unwrap();
        }
        manager.unregister_dataflow(&df).await;
        sender.send(frame(df, 1, b"late".to_vec())).await.unwrap();
        sender.try_send(frame(df, 1, b"late".to_vec())).unwrap();

        let mut reader = FramedReader::new(peer);
        for i in 0..200u8 {
            let frame = tokio::time::timeout(Duration::from_secs(5), reader.read_frame())
                .await
                .expect("a queued frame was dropped")
                .unwrap();
            assert_eq!(frame.channel_id, 1);
            assert_eq!(frame.payload[..8], u64::from(i).to_le_bytes());
            assert_eq!(&frame.payload[8..], &[i]);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(200), reader.read_frame())
                .await
                .is_err(),
            "a frame sent after the registration ended was written"
        );
    }

    /// Frames already queued on a connection survive the manager being
    /// released, whether it is closed (waiting for its writers)
    /// or simply dropped (writers finish in the background). Then the peer
    /// sees the connection end at once, not when the readers give up waiting
    /// 10 s later.
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
                let end = tokio::time::timeout(Duration::from_secs(3), reader.read_frame()).await;
                (got, end)
            });
            if close {
                manager.close(Duration::from_secs(5)).await;
            } else {
                drop(manager);
            }
            let (got, end) = reading.await.unwrap();
            assert_eq!(
                got,
                (0..100u8).collect::<Vec<_>>(),
                "close={close}: queued frames were lost"
            );
            assert!(
                matches!(end, Ok(Err(TransportError::ConnectionClosed))),
                "close={close}: the peer did not see the connection end within 3 s: {end:?}"
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

    // --- Connection liveness (DA R54–R58) ----------------------------------------

    /// The liveness tests' silence timeout (T) and probe interval (P).
    const T: Duration = Duration::from_secs(5);
    const P: Duration = Duration::from_secs(1);

    fn live(connections: usize) -> SharedConnectionConfig {
        SharedConnectionConfig {
            connections,
            probe_interval: P,
            silence_timeout: Some(T),
            ..Default::default()
        }
    }

    /// One direction of a [`valved`] connection. Shut, it forwards nothing
    /// more and hides the end of the stream, like a partition.
    struct Valve(tokio::sync::watch::Sender<bool>);

    impl Valve {
        fn shut(&self) {
            self.0.send_replace(false);
        }

        fn open(&self) {
            self.0.send_replace(true);
        }
    }

    /// Forward `from` to `to` while `open`, `pace` bytes at a time, one
    /// chunk per period, if given.
    async fn pipe(
        mut from: tokio::io::ReadHalf<DuplexStream>,
        mut to: tokio::io::WriteHalf<DuplexStream>,
        mut open: tokio::sync::watch::Receiver<bool>,
        pace: Option<(usize, Duration)>,
    ) {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            if open.wait_for(|open| *open).await.is_err() {
                return;
            }
            let limit = pace.map_or(buf.len(), |(chunk, _)| chunk);
            let read = from.read(&mut buf[..limit]).await;
            // A valve shut meanwhile holds what was read, or the end.
            if open.wait_for(|open| *open).await.is_err() {
                return;
            }
            match read {
                Ok(0) | Err(_) => {
                    let _ = to.shutdown().await;
                    return;
                }
                Ok(read) => {
                    if to.write_all(&buf[..read]).await.is_err() {
                        return;
                    }
                }
            }
            if let Some((_, period)) = pace {
                tokio::time::sleep(period).await;
            }
        }
    }

    /// A connection between two managers through a valve per direction:
    /// the two ends, and the valves from the first end and from the second.
    fn valved(pace: Option<(usize, Duration)>) -> (DuplexStream, DuplexStream, Valve, Valve) {
        let (a, relay_a) = duplex(64 * 1024);
        let (b, relay_b) = duplex(64 * 1024);
        let (from_a, to_a) = tokio::io::split(relay_a);
        let (from_b, to_b) = tokio::io::split(relay_b);
        let (a_to_b, a_to_b_open) = tokio::sync::watch::channel(true);
        let (b_to_a, b_to_a_open) = tokio::sync::watch::channel(true);
        tokio::spawn(pipe(from_a, to_b, a_to_b_open, pace));
        tokio::spawn(pipe(from_b, to_a, b_to_a_open, pace));
        (a, b, Valve(a_to_b), Valve(b_to_a))
    }

    type Health = tokio::sync::broadcast::Receiver<crate::RuntimeEvent>;

    /// Two managers over `pairs` (a's end, b's end), with their health
    /// events.
    fn live_pair(
        pairs: Vec<(DuplexStream, DuplexStream)>,
        config_a: SharedConnectionConfig,
        config_b: SharedConnectionConfig,
    ) -> (SharedPeerManager, SharedPeerManager, Health, Health) {
        let rt = tokio::runtime::Handle::current();
        let (ends_a, ends_b): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
        let (health_a, watch_a) = tokio::sync::broadcast::channel(64);
        let (health_b, watch_b) = tokio::sync::broadcast::channel(64);
        let a = SharedPeerManager::new("b".into(), config_a, Pre::factory(ends_a), &rt, health_a)
            .unwrap();
        let b = SharedPeerManager::new("a".into(), config_b, Pre::factory(ends_b), &rt, health_b)
            .unwrap();
        (a, b, watch_a, watch_b)
    }

    /// The first event within `wait` matching `wanted`, after the ones that
    /// don't.
    async fn event_matching(
        events: &mut tokio_mpsc::Receiver<PeerIoNotice>,
        wait: Duration,
        wanted: impl Fn(&PeerIoEvent) -> bool,
    ) -> Option<PeerIoEvent> {
        tokio::time::timeout(wait, async {
            loop {
                let event = events.recv().await?.into_parts().0;
                if wanted(&event) {
                    return Some(event);
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    fn silent(event: &PeerIoEvent) -> bool {
        matches!(
            event,
            PeerIoEvent::ReadFailed(TransportError::ConnectionSilent { timeout }) if *timeout == T
        )
    }

    fn degraded(health: &mut Health) -> Vec<String> {
        let mut details = Vec::new();
        while let Ok(crate::RuntimeEvent::TransportDegraded { detail, .. }) = health.try_recv() {
            details.push(detail);
        }
        details
    }

    /// Register `df` on both managers and bind it to its connection both
    /// ways (its first frames); returns its receivers and events on a and b.
    #[allow(clippy::type_complexity)]
    async fn bound(
        a: &SharedPeerManager,
        b: &SharedPeerManager,
        df: DataflowId,
    ) -> (
        HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
        tokio_mpsc::Receiver<PeerIoNotice>,
        HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
        tokio_mpsc::Receiver<PeerIoNotice>,
    ) {
        let (mut ra, ea) = a.register_dataflow(df, &[1], 1).await.unwrap();
        let (mut rb, eb) = b.register_dataflow(df, &[1], 1).await.unwrap();
        send(a, df, 1, b"from a".to_vec()).await;
        send(b, df, 1, b"from b".to_vec()).await;
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await.unwrap(), b"from a");
        assert_eq!(recv(ra.get_mut(&1).unwrap()).await.unwrap(), b"from b");
        (ra, ea, rb, eb)
    }

    /// CL1, CL2: an idle connection is kept alive by its probes; once it
    /// stops delivering both ways, each node fails it by its own timer,
    /// within T ± P of the cut, as a read error, and fails the dataflows
    /// using it. It is reported, never as a node leaving.
    #[tokio::test(start_paused = true)]
    async fn a_connection_silent_both_ways_fails_on_both_nodes() {
        let (a_end, b_end, a_to_b, b_to_a) = valved(None);
        let (a, b, mut health_a, mut health_b) = live_pair(vec![(a_end, b_end)], live(1), live(1));
        let df = DataflowId::new();
        let (mut ra, mut ea, mut rb, mut eb) = bound(&a, &b, df).await;
        tokio::time::sleep(4 * T).await;
        assert!(ea.try_recv().is_err() && eb.try_recv().is_err());
        assert!(degraded(&mut health_a).is_empty() && degraded(&mut health_b).is_empty());

        a_to_b.shut();
        b_to_a.shut();
        let cut = tokio::time::Instant::now();
        for (events, node) in [(&mut ea, "a"), (&mut eb, "b")] {
            let event = event_matching(events, 2 * T, silent).await;
            let after = cut.elapsed();
            assert!(event.is_some(), "{node} never failed the silent connection");
            assert!(
                after >= T - P && after <= T + P,
                "{node} failed it {after:?} after the cut (T = {T:?}, P = {P:?})"
            );
        }
        assert_eq!(recv(ra.get_mut(&1).unwrap()).await, None);
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await, None);
        assert_eq!((a.connection_count(), b.connection_count()), (0, 0));
        for health in [&mut health_a, &mut health_b] {
            let details = degraded(health);
            assert!(
                details.iter().any(|d| d.contains("no bytes read")),
                "{details:?}"
            );
        }
        // Frames sent now go nowhere: the dataflow was told its lane failed.
        send(&a, df, 1, b"lost".to_vec()).await;
        assert!(matches!(
            event_matching(&mut ea, P, |_| true).await,
            None | Some(PeerIoEvent::WriteFailed(_))
        ));
    }

    /// CL1: a connection that stops delivering one way is failed by the node
    /// that reads nothing, by its timer; the other node sees it end.
    #[tokio::test(start_paused = true)]
    async fn a_connection_silent_one_way_fails_on_both_nodes() {
        let (a_end, b_end, a_to_b, _b_to_a) = valved(None);
        let (a, b, _health_a, _health_b) = live_pair(vec![(a_end, b_end)], live(1), live(1));
        let df = DataflowId::new();
        let (_ra, mut ea, mut rb, mut eb) = bound(&a, &b, df).await;
        a_to_b.shut();
        let cut = tokio::time::Instant::now();
        assert!(
            event_matching(&mut eb, 2 * T, silent).await.is_some(),
            "b never failed the connection that delivers nothing to it"
        );
        let on_b = cut.elapsed();
        assert!(on_b >= T - P && on_b <= T + P, "{on_b:?}");
        assert_eq!(recv(rb.get_mut(&1).unwrap()).await, None);
        let on_a = event_matching(&mut ea, P, |event| {
            matches!(event, PeerIoEvent::Closed | PeerIoEvent::ReadFailed(_))
        })
        .await;
        assert!(on_a.is_some(), "a never saw the connection end");
    }

    /// CL2: a frame that takes longer than T to cross a slow link keeps its
    /// connection alive while its bytes move (bytes count, not frames).
    #[tokio::test(start_paused = true)]
    async fn a_large_frame_on_a_slow_link_is_not_failed() {
        // 10 KiB/s: the frame takes about 20 s, four times T.
        let (a_end, b_end, _a_to_b, _b_to_a) = valved(Some((1024, Duration::from_millis(100))));
        let (a, b, mut health_a, mut health_b) = live_pair(vec![(a_end, b_end)], live(1), live(1));
        let df = DataflowId::new();
        let (_ra, mut ea, mut rb, mut eb) = bound(&a, &b, df).await;
        let started = tokio::time::Instant::now();
        let big = vec![7u8; 200 * 1024];
        send(&a, df, 1, big.clone()).await;
        let got = tokio::time::timeout(Duration::from_secs(60), rb.get_mut(&1).unwrap().recv())
            .await
            .expect("the frame never arrived")
            .unwrap();
        assert!(started.elapsed() > 3 * T, "{:?}", started.elapsed());
        assert_eq!(got, big);
        assert!(ea.try_recv().is_err() && eb.try_recv().is_err());
        assert!(degraded(&mut health_a).is_empty() && degraded(&mut health_b).is_empty());
    }

    /// CL6: a consumer that takes nothing never stops the connection's
    /// reader: another dataflow on it still receives at once, and the
    /// connection is never failed.
    #[tokio::test(start_paused = true)]
    async fn a_slow_consumer_does_not_stop_the_reader() {
        let (a_end, b_end, _a_to_b, _b_to_a) = valved(None);
        let (a, b, _health_a, mut health_b) = live_pair(vec![(a_end, b_end)], live(1), live(1));
        let (slow, other) = (DataflowId::new(), DataflowId::new());
        let (_r_slow, _e_slow, mut slow_rx, mut slow_events) = bound(&a, &b, slow).await;
        let (_r_other, _e_other, mut other_rx, mut other_events) = bound(&a, &b, other).await;
        for i in 0..1000u32 {
            send(&a, slow, 1, i.to_le_bytes().to_vec()).await;
        }
        send(&a, other, 1, b"behind".to_vec()).await;
        assert_eq!(
            recv(other_rx.get_mut(&1).unwrap()).await.unwrap(),
            b"behind"
        );
        tokio::time::sleep(3 * T).await;
        assert!(slow_events.try_recv().is_err() && other_events.try_recv().is_err());
        assert!(degraded(&mut health_b).is_empty());
        for i in 0..1000u32 {
            assert_eq!(
                recv(slow_rx.get_mut(&1).unwrap()).await.unwrap(),
                i.to_le_bytes()
            );
        }
    }

    /// R56: a writer that has had bytes waiting and written none for T fails
    /// its connection like a write error, while the connection still reads.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_writer_fails_its_connection_after_the_timeout() {
        let (a_end, b_end, a_to_b, _b_to_a) = valved(None);
        // b never declares the connection dead, and keeps a reading.
        let quiet = SharedConnectionConfig {
            silence_timeout: None,
            ..live(1)
        };
        let (a, b, mut health_a, _health_b) = live_pair(vec![(a_end, b_end)], live(1), quiet);
        let df = DataflowId::new();
        let (_ra, mut ea, _rb, _eb) = bound(&a, &b, df).await;
        a_to_b.shut();
        let cut = tokio::time::Instant::now();
        let sender = a.dataflow_sender(&df).unwrap();
        while sender.try_send(frame(df, 1, vec![0; 4096])).is_ok() {
            tokio::task::yield_now().await;
        }
        let event = event_matching(&mut ea, 3 * T, |event| {
            matches!(event, PeerIoEvent::WriteFailed(_))
        })
        .await;
        let after = cut.elapsed();
        assert!(
            matches!(
                event,
                Some(PeerIoEvent::WriteFailed(TransportError::WriteStalled { timeout })) if timeout == T
            ),
            "{event:?}"
        );
        assert!(after >= T && after <= T + 2 * P, "{after:?}");
        assert!(
            degraded(&mut health_a)
                .iter()
                .any(|d| d.contains("nothing written")),
        );
        assert_eq!(a.connection_count(), 0);
    }

    /// Two managers with two connections: dataflow X is pinned to the first
    /// on both nodes and Y to the second, and X's queue on a is full behind a
    /// one-way stall of the first connection from a.
    struct Stalled {
        a: SharedPeerManager,
        b: SharedPeerManager,
        y: DataflowId,
        /// The first connection, from a.
        x_valve: Valve,
        /// How many of X's frames a queued.
        x_frames: usize,
        /// X's channels on b.
        x_on_b: HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
        /// Y's channels and events on b.
        y_on_b: HashMap<u64, tokio_mpsc::UnboundedReceiver<Vec<u8>>>,
        y_events_b: tokio_mpsc::Receiver<PeerIoNotice>,
        /// The other directions, kept open.
        _open: [Valve; 3],
    }

    async fn x_stuck_behind_a_stall() -> Stalled {
        let (a1, b1, x_valve, x_back) = valved(None);
        let (a2, b2, y_valve, y_back) = valved(None);
        let (a, b, _, _) = live_pair(vec![(a1, b1), (a2, b2)], live(2), live(2));
        let (x, y) = (DataflowId::new(), DataflowId::new());
        let (_, _, x_on_b, _) = bound(&a, &b, x).await;
        let (_, _, y_on_b, y_events_b) = bound(&a, &b, y).await;
        for manager in [&a, &b] {
            let state = manager.inner.state();
            assert_eq!(
                (state.registered[&x].conn, state.registered[&y].conn),
                (0, 1)
            );
        }
        x_valve.shut();
        let sender = a.dataflow_sender(&x).unwrap();
        let mut x_frames = 0;
        while sender.try_send(frame(x, 1, vec![0; 4096])).is_ok() {
            x_frames += 1;
            tokio::task::yield_now().await;
        }
        assert_eq!(a.metrics_snapshot()[0].queued_frames, WRITER_QUEUE);
        Stalled {
            a,
            b,
            y,
            x_valve,
            x_frames,
            x_on_b,
            y_on_b,
            y_events_b,
            _open: [x_back, y_valve, y_back],
        }
    }

    /// CL4: a stuck connection holds up only its own dataflows: another
    /// dataflow's payload and control frames to the same peer cross at once.
    #[tokio::test(start_paused = true)]
    async fn a_stuck_writer_does_not_delay_another_connection() {
        let mut stalled = x_stuck_behind_a_stall().await;
        let (a, y) = (&stalled.a, stalled.y);
        let started = tokio::time::Instant::now();
        for i in 0..10u8 {
            send(a, y, 1, vec![i]).await;
        }
        a.control_sender()
            .send(frame(y, CONTROL_CHANNEL_ID, b"control".to_vec()))
            .await
            .unwrap();
        for i in 0..10u8 {
            assert_eq!(
                recv(stalled.y_on_b.get_mut(&1).unwrap()).await.unwrap(),
                [i]
            );
        }
        assert_eq!(
            recv(stalled.y_on_b.get_mut(&CONTROL_CHANNEL_ID).unwrap())
                .await
                .unwrap(),
            b"control"
        );
        assert!(started.elapsed() < P, "{:?}", started.elapsed());
        assert!(stalled.y_events_b.try_recv().is_err());
    }

    /// R58: a dataflow that finishes while another connection to the peer is
    /// stuck unregisters at once, and its last frames (its goodbye and
    /// acknowledgement) still reach the peer.
    #[tokio::test(start_paused = true)]
    async fn a_dataflow_finishing_during_another_stall_delivers_its_last_frames() {
        let mut stalled = x_stuck_behind_a_stall().await;
        let (a, y) = (&stalled.a, stalled.y);
        let started = tokio::time::Instant::now();
        let last = a.dataflow_sender(&y).unwrap();
        last.send(frame(y, 1, b"goodbye".to_vec())).await.unwrap();
        last.send(frame(y, 1, b"ack".to_vec())).await.unwrap();
        a.unregister_dataflow(&y).await;
        let on_b = stalled.y_on_b.get_mut(&1).unwrap();
        assert_eq!(recv(on_b).await.unwrap(), b"goodbye");
        assert_eq!(recv(on_b).await.unwrap(), b"ack");
        assert!(started.elapsed() < P, "{:?}", started.elapsed());
        assert!(stalled.y_events_b.try_recv().is_err());
    }

    /// A stall that heals before T loses nothing and fails nothing.
    #[tokio::test(start_paused = true)]
    async fn a_stall_shorter_than_the_timeout_loses_nothing() {
        let mut stalled = x_stuck_behind_a_stall().await;
        tokio::time::sleep(T - 2 * P).await;
        stalled.x_valve.open();
        let on_b = stalled.x_on_b.get_mut(&1).unwrap();
        for _ in 0..stalled.x_frames {
            assert_eq!(recv(on_b).await.unwrap().len(), 4096);
        }
        tokio::time::sleep(2 * T).await;
        assert!(on_b.try_recv().is_err());
        assert_eq!(
            (stalled.a.connection_count(), stalled.b.connection_count()),
            (2, 2)
        );
    }

    /// With `silence_timeout: None` nothing is ever declared dead.
    #[tokio::test(start_paused = true)]
    async fn no_silence_timeout_disables_the_rule() {
        let (a_end, b_end, a_to_b, b_to_a) = valved(None);
        let off = SharedConnectionConfig {
            silence_timeout: None,
            ..live(1)
        };
        let (a, b, mut health_a, mut health_b) = live_pair(vec![(a_end, b_end)], off.clone(), off);
        let df = DataflowId::new();
        let (_ra, mut ea, _rb, mut eb) = bound(&a, &b, df).await;
        a_to_b.shut();
        b_to_a.shut();
        tokio::time::sleep(20 * T).await;
        assert!(ea.try_recv().is_err() && eb.try_recv().is_err());
        assert!(degraded(&mut health_a).is_empty() && degraded(&mut health_b).is_empty());
        assert_eq!((a.connection_count(), b.connection_count()), (1, 1));
    }

    /// An idle pool, with no dataflow registered, stays alive on its probes;
    /// once it stops delivering it is dropped, failing no dataflow, and the
    /// next registration refills the pool.
    #[tokio::test(start_paused = true)]
    async fn an_idle_pool_stays_alive_until_it_falls_silent() {
        let (a1, b1, a_to_b, b_to_a) = valved(None);
        let (a2, b2, _valve_a, _valve_b) = valved(None);
        let (a, b, mut health_a, mut health_b) =
            live_pair(vec![(a1, b1), (a2, b2)], live(1), live(1));
        let df = DataflowId::new();
        let _bound = bound(&a, &b, df).await;
        a.unregister_dataflow(&df).await;
        b.unregister_dataflow(&df).await;
        tokio::time::sleep(10 * T).await;
        assert!(degraded(&mut health_a).is_empty() && degraded(&mut health_b).is_empty());
        assert_eq!((a.connection_count(), b.connection_count()), (1, 1));

        a_to_b.shut();
        b_to_a.shut();
        tokio::time::sleep(T + 2 * P).await;
        assert_eq!((a.connection_count(), b.connection_count()), (0, 0));
        for health in [&mut health_a, &mut health_b] {
            assert!(degraded(health).iter().any(|d| d.contains("no bytes read")));
        }
        let next = DataflowId::new();
        let (_ra, mut ea, _rb, mut eb) = bound(&a, &b, next).await;
        assert_eq!((a.connection_count(), b.connection_count()), (1, 1));
        tokio::time::sleep(2 * T).await;
        assert!(ea.try_recv().is_err() && eb.try_recv().is_err());
    }

    /// A control frame overtakes the payload frames queued before it on its
    /// connection: the writer serves the control queue first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn control_frames_overtake_queued_payload() {
        let (ours, theirs) = duplex(4096);
        let manager = SharedPeerManager::new(
            "peer".into(),
            config(1),
            Pre::factory([ours]),
            &tokio::runtime::Handle::current(),
            health_tx(),
        )
        .unwrap();
        let df = DataflowId::new();
        let _registered = manager.register_dataflow(df, &[1], 4).await.unwrap();
        let sender = manager.dataflow_sender(&df).unwrap();
        for i in 0..64u8 {
            sender.try_send(frame(df, 1, vec![i; 1024])).unwrap();
        }
        // The writer is blocked on the pipe, with most frames still queued.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(manager.metrics_snapshot()[0].queued_frames > 32);
        manager
            .control_sender()
            .send(frame(df, CONTROL_CHANNEL_ID, b"cancel".to_vec()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut reader = FramedReader::new(theirs);
        let mut order = Vec::new();
        for _ in 0..65 {
            let frame = tokio::time::timeout(Duration::from_secs(5), reader.read_frame())
                .await
                .expect("a frame is missing")
                .unwrap();
            order.push(frame.channel_id);
        }
        let control_at = order
            .iter()
            .position(|&channel| channel == CONTROL_CHANNEL_ID)
            .unwrap();
        assert!(control_at < 32, "the control frame came at {control_at}");
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

    /// A lane tags each frame with its dataflow and numbers what it queues;
    /// a frame refused for a full queue takes no number; once the manager is
    /// released, sends fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lane_numbers_what_it_queues() {
        let (queue, mut rx) = tokio_mpsc::channel::<Outgoing>(2);
        let (events, _events_rx) = tokio_mpsc::channel(8);
        let df = DataflowId::new();
        let sender = DataframeSender {
            lane: Arc::new(Lane::new(df, 0, queue, events, Weak::new())),
        };
        sender
            .send(frame(DataflowId::new(), 1, vec![1]))
            .await
            .unwrap();
        sender.try_send(frame(df, 1, vec![2])).unwrap();
        assert!(matches!(
            sender.try_send(frame(df, 1, vec![3])),
            Err(tokio_mpsc::error::TrySendError::Full(refused)) if refused.payload == [3]
        ));
        for (seq, payload) in [(0, 1u8), (1, 2)] {
            let Outgoing { seq: got, frame } = rx.recv().await.unwrap();
            assert_eq!(got, Some(seq));
            assert_eq!(frame.dataflow_id, df);
            assert_eq!(frame.payload, [payload]);
        }
        sender.try_send(frame(df, 1, vec![4])).unwrap();
        assert_eq!(rx.recv().await.unwrap().seq, Some(2));

        sender.lane.close(LaneEnd::Released);
        assert!(matches!(
            sender.try_send(frame(df, 1, vec![5])),
            Err(tokio_mpsc::error::TrySendError::Closed(_))
        ));
        assert_eq!(
            sender
                .send(frame(df, 1, vec![6]))
                .await
                .unwrap_err()
                .0
                .payload,
            [6]
        );
        assert!(rx.try_recv().is_err());
    }
}

/// Schedule tests of numbering at enqueue (DA R57), under Shuttle (Linux,
/// `RUSTFLAGS="--cfg instancy_shuttle"`; see `docs/testing/nextest.md`). A
/// lane's producers (worker threads offering frames without waiting, the
/// outbox forwarder waiting for room) and its end run on their own threads;
/// the depth-first scheduler runs every interleaving of the lane's lock.
/// They check the lane's obligations: only a frame that is queued takes a
/// number, the queue holds the frames in number order, a lane that has ended
/// takes no numbers, and a frame whose waiting send returned is in the queue.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
mod lane_shuttle {
    use super::*;
    use std::sync::atomic::AtomicUsize as Count;

    // The scheduler explores interleavings only if the lane's lock is
    // Shuttle's model.
    const _: fn(&Lane) -> &shuttle::sync::Mutex<LaneState> = |lane| &lane.state;

    fn dataflow_id() -> DataflowId {
        DataflowId::from_bytes([0x57; 16])
    }

    /// A lane whose connection queue holds `capacity` frames.
    fn lane(capacity: usize) -> (Arc<Lane>, tokio_mpsc::Receiver<Outgoing>) {
        let (queue, rx) = tokio_mpsc::channel(capacity);
        let (events, _) = tokio_mpsc::channel(8);
        let lane = Lane::new(dataflow_id(), 0, queue, events, Weak::new());
        (Arc::new(lane), rx)
    }

    /// Frame `i` of `producer`.
    fn frame(producer: u8, i: u8) -> Frame {
        Frame {
            dataflow_id: dataflow_id(),
            channel_id: 1,
            payload: vec![producer, i],
        }
    }

    /// The queued frames are numbered 0, 1, 2, ... in queue order, and each
    /// producer's frames are in its order.
    fn check_order(queued: &[Outgoing]) {
        for (position, out) in queued.iter().enumerate() {
            assert_eq!(
                out.seq,
                Some(position as u64),
                "queue order is not number order, or a number was skipped: {:?}",
                queued.iter().map(|o| o.seq).collect::<Vec<_>>()
            );
        }
        for producer in 0..2u8 {
            let mine: Vec<u8> = queued
                .iter()
                .filter(|out| out.frame.payload[0] == producer)
                .map(|out| out.frame.payload[1])
                .collect();
            assert!(mine.is_sorted(), "producer {producer}'s order: {mine:?}");
        }
    }

    fn run(test: impl Fn() + Send + Sync + 'static) -> usize {
        let mut config = shuttle::Config::new();
        config.stack_size = 0x10_0000;
        config.max_steps = shuttle::MaxSteps::FailAfter(10_000);
        config.max_time = None;
        let scheduler = shuttle::scheduler::DfsScheduler::new(None, false);
        shuttle::Runner::new(scheduler, config).run(test)
    }

    /// Two producers offer two frames each, without waiting, to a lane with
    /// room for three, while a third thread ends the lane.
    #[test]
    fn offered_frames_are_numbered_as_queued_until_the_lane_ends() {
        static PASSED: Count = Count::new(0);
        PASSED.store(0, Ordering::Relaxed);
        let executions = run(|| {
            let (lane, mut rx) = lane(3);
            // Set once `close` returned (a std flag: Shuttle runs every
            // thread on one OS thread).
            let ended = Arc::new(AtomicBool::new(false));
            let producers = [0u8, 1].map(|producer| {
                let (lane, ended) = (Arc::clone(&lane), Arc::clone(&ended));
                shuttle::thread::spawn(move || {
                    (0..2u8)
                        .map(|i| {
                            let after_end = ended.load(Ordering::SeqCst);
                            (after_end, lane.try_enqueue(frame(producer, i)))
                        })
                        .collect::<Vec<_>>()
                })
            });
            let closer = {
                let (lane, ended) = (Arc::clone(&lane), Arc::clone(&ended));
                shuttle::thread::spawn(move || {
                    lane.close(LaneEnd::Ended);
                    ended.store(true, Ordering::SeqCst);
                })
            };
            let outcomes: Vec<(bool, Enqueued)> = producers
                .into_iter()
                .flat_map(|producer| producer.join().unwrap())
                .collect();
            closer.join().unwrap();
            let mut queued = Vec::new();
            while let Ok(out) = rx.try_recv() {
                queued.push(out);
            }
            check_order(&queued);
            let admitted = outcomes
                .iter()
                .filter(|(_, outcome)| matches!(outcome, Enqueued::Admitted))
                .count();
            assert_eq!(admitted, queued.len(), "only a queued frame takes a number");
            for (after_end, outcome) in &outcomes {
                assert!(
                    !after_end || matches!(outcome, Enqueued::Dropped),
                    "a frame offered after the lane ended: {outcome:?}"
                );
                assert!(
                    matches!(
                        outcome,
                        Enqueued::Admitted | Enqueued::Full(_) | Enqueued::Dropped
                    ),
                    "{outcome:?}"
                );
            }
            PASSED.fetch_add(1, Ordering::Relaxed);
        });
        println!("{executions} interleavings");
        assert!(executions > 1, "nothing was explored");
        assert_eq!(PASSED.load(Ordering::Relaxed), executions);
    }

    /// A waiting producer (the outbox forwarder) sends two frames while
    /// another offers one without waiting, to a lane with room for one, and
    /// the writer takes frames off the queue.
    #[test]
    fn a_waiting_send_numbers_its_frame_only_once_it_is_queued() {
        static PASSED: Count = Count::new(0);
        PASSED.store(0, Ordering::Relaxed);
        let executions = run(|| {
            let (lane, mut rx) = lane(1);
            let forwarder = {
                let lane = Arc::clone(&lane);
                shuttle::thread::spawn(move || {
                    for i in 0..2u8 {
                        let outcome = shuttle::future::block_on(lane.enqueue(frame(0, i)));
                        assert!(matches!(outcome, Enqueued::Admitted), "{outcome:?}");
                    }
                })
            };
            let pusher = {
                let lane = Arc::clone(&lane);
                shuttle::thread::spawn(move || lane.try_enqueue(frame(1, 0)))
            };
            // The writer: it takes frames until it has both of the
            // forwarder's, which therefore were queued.
            let mut written = Vec::new();
            while written
                .iter()
                .filter(|out: &&Outgoing| out.frame.payload[0] == 0)
                .count()
                < 2
            {
                let out = shuttle::future::block_on(rx.recv()).expect("the queue closed");
                written.push(out);
            }
            forwarder.join().unwrap();
            let pushed = pusher.join().unwrap();
            while let Ok(out) = rx.try_recv() {
                written.push(out);
            }
            check_order(&written);
            let pushed_queued = written.iter().any(|out| out.frame.payload[0] == 1);
            assert_eq!(
                matches!(pushed, Enqueued::Admitted),
                pushed_queued,
                "{pushed:?}"
            );
            PASSED.fetch_add(1, Ordering::Relaxed);
        });
        println!("{executions} interleavings");
        assert!(executions > 1, "nothing was explored");
        assert_eq!(PASSED.load(Ordering::Relaxed), executions);
    }
}
