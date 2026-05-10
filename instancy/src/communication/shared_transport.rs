//! Shared transport layer for pooled, multi-dataflow connections.
//!
//! This module implements Phase 4 of the shared connection mode (DESIGN.md §6.3.1).
//! Unlike [`TransportSession`](super::transport_session::TransportSession) which creates dedicated per-dataflow TCP connections,
//! `SharedTransportSession` lets multiple dataflows share a pool of connections to
//! each peer, with sequenced messages for ordering.
//!
//! # Architecture
//!
//! ```text
//! SharedPeerManager (per peer-pair, owns TCP connections)
//! ├── PeerPool (connection metrics + least-loaded selection)
//! ├── Per-connection WriterTask (writes frames to TCP)
//! ├── ReaderTask (reads frames from ALL connections, demuxes)
//! ├── ProbeLoop (periodic RTT probes per connection)
//! ├── ScalingDriver (processes replies, emits ScaleUp/ScaleDown)
//! └── Per (dataflow_id) registration:
//!     ├── SequenceCounter (per payload lane, NOT per channel)
//!     ├── ReorderBuffer (per payload lane)
//!     └── Per-channel receivers
//!
//! SharedTransportSession (per dataflow, lightweight handle)
//! ├── References SharedPeerManager for each peer
//! ├── data_sender(peer) → shared payload channel
//! ├── progress_sender(peer) → same as data_sender (FIFO preserved)
//! └── control_sender(peer) → priority channel
//! ```
//!
//! # Ordering Invariant
//!
//! Data and progress messages share a single sequenced payload lane per
//! `(dataflow_id, peer)`. This preserves the timely ordering invariant:
//! data at time T is sequenced before the progress message releasing T,
//! so FIFO delivery through the reorder buffer guarantees receivers see
//! data before the frontier advances past T.
//!
//! # Probe Protocol
//!
//! RTT probes use the standard [`Frame`] wire format with a reserved
//! `PROBE_CHANNEL_ID` to avoid mixing wire formats on the same TCP stream.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc as tokio_mpsc;

use crate::communication::probing::{
    ProbeCounter, ProbeKind, ProbeMessage, ScalingDriver, ScalingEvent,
};
use crate::communication::sequencing::{InsertResult, ReorderBuffer, SequenceCounter};
use crate::communication::shared_pool::{ConnectionMetrics, PeerPool, SharedConnectionConfig};
use crate::communication::transport::{Frame, FramedReader, FramedWriter, TransportError};
use crate::communication::transport_session::CONTROL_CHANNEL_ID;
use crate::dataflow::id::DataflowId;
use crate::error::LockResultExt;
use crate::wire;

/// Reserved channel ID for probe messages on shared connections.
///
/// This is distinct from `CONTROL_CHANNEL_ID` (0) and user data/progress channels.
/// Probe frames carry a [`ProbeMessage`] as their payload.
pub const PROBE_CHANNEL_ID: u64 = u64::MAX;

fn push_task_handle(
    task_handles: &Mutex<Vec<tokio::task::JoinHandle<()>>>,
    handle: tokio::task::JoinHandle<()>,
) {
    match task_handles.lock().or_poison("task handle") {
        Ok(mut handles) => handles.push(handle),
        Err(_) => {
            // TODO: propagate poisoned task-handle locks once constructors can return Result.
            handle.abort();
        }
    }
}

// Probe frames use a nil dataflow ID (all zeros) as a sentinel.
// Obtained lazily since `DataflowId::nil()` is not const.

// ---------------------------------------------------------------------------
// ConnectionFactory — user-provided connection establishment
// ---------------------------------------------------------------------------

/// Factory for establishing new TCP connections to a peer.
///
/// The shared transport calls this when the scaling driver emits a
/// [`ScalingEvent::ScaleUp`]. Implementations can use any mechanism
/// (direct TCP, TLS, actor framework negotiation, etc.).
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

/// Resolves peer node IDs to socket addresses.
pub trait PeerAddressResolver: Send + Sync + 'static {
    /// Resolve a peer node ID to its socket address.
    fn resolve(&self, peer_node_id: &str) -> Option<std::net::SocketAddr>;
}

impl PeerAddressResolver for std::collections::HashMap<String, std::net::SocketAddr> {
    fn resolve(&self, peer_node_id: &str) -> Option<std::net::SocketAddr> {
        self.get(peer_node_id).copied()
    }
}

/// Default connection factory using plain TCP (no TLS).
///
/// The application instantiates this with a resolver that maps peer node IDs
/// to socket addresses. For TLS or custom protocols, implement
/// [`ConnectionFactory`] directly.
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
// DataflowRegistration — per-dataflow state within a SharedPeerManager
// ---------------------------------------------------------------------------

/// Per-dataflow state tracked by the shared peer manager.
struct DataflowRegistration {
    /// Sequence counter for the payload lane (data + progress share this).
    sequence_counter: SequenceCounter,
    /// Per-channel receivers for delivering reordered frames.
    channel_senders: HashMap<u64, tokio_mpsc::Sender<Vec<u8>>>,
    /// Error notification sender: transport errors are sent here.
    /// The dataflow can poll the corresponding receiver to detect peer failures.
    error_tx: tokio_mpsc::Sender<TransportError>,
}

/// Combined registration + pending-control state, guarded by a **single** lock
/// to prevent a TOCTOU race between `reader_task` and `register_dataflow`.
///
/// # Race condition (why a single lock is required)
///
/// When multiple dataflows share pooled connections, the remote peer may
/// register a new dataflow and send its Handshake *before* the local peer
/// has registered that dataflow. For example:
///
/// ```text
/// Node-A (fast)                 TCP               Node-B (slow)
/// ─────────────                 ───               ─────────────
/// 1. df1 completes
/// 2. register(df2)
/// 3. send df2 Handshake ────────────────►  reader_task receives
///                                          df2 Handshake
///
///                                          registrations[df2] → NOT FOUND
///                                          → frame DROPPED silently ✗
///
///                                       4. register(df2)
///                                       5. send df2 Handshake to node-A
///                                       6. wait for df2 Handshake from node-A
///                                          → never arrives → timeout ✗
/// ```
///
/// The fix: `reader_task` buffers unregistered control frames in
/// `pending_control`, and `register_dataflow` drains the buffer. Both
/// operations happen under this single lock. If two separate locks were
/// used (one for `registered`, one for `pending_control`), the following
/// TOCTOU race would still be possible:
///
/// ```text
/// reader_task                       register_dataflow
/// ───────────                       ─────────────────
/// lock(registrations)
///   df2 not found
/// unlock(registrations)
///                                   lock(pending_control)
///                                     drain → empty (nothing buffered yet)
///                                   unlock(pending_control)
///                                   lock(registrations)
///                                     insert df2
///                                   unlock(registrations)
/// lock(pending_control)
///   buffer Handshake frame        ← buffered AFTER drain already ran
/// unlock(pending_control)           → frame is never delivered
/// ```
///
/// With a single lock, both "check + buffer" and "drain + register" are
/// atomic, eliminating this window entirely.
struct RegistrationState {
    /// Active dataflow registrations.
    registered: HashMap<DataflowId, DataflowRegistration>,
    /// Control frames buffered for dataflows that haven't been registered yet.
    /// Drained into the control channel upon registration.
    pending_control: HashMap<DataflowId, Vec<Vec<u8>>>,
    /// Dataflows that have been unregistered (completed). Late control frames
    /// for these IDs are dropped instead of being buffered indefinitely.
    completed: HashSet<DataflowId>,
}

// ---------------------------------------------------------------------------
// SharedPeerManager — owns pooled connections to one peer
// ---------------------------------------------------------------------------

/// Manages a pool of shared connections to a single remote peer.
///
/// Multiple dataflows register with the manager and share the underlying
/// TCP connections. The manager handles:
/// - Connection pool management (PeerPool)
/// - Per-connection writer tasks
/// - Shared reader tasks (demux + reorder)
/// - RTT probing and adaptive scaling
// Fields are used via Arc clones in spawned tasks.
#[allow(dead_code)]
pub struct SharedPeerManager {
    /// The remote peer's node ID.
    peer_node_id: String,
    /// Pool configuration.
    config: SharedConnectionConfig,
    /// Connection pool for metrics and selection.
    pool: Arc<PeerPool>,
    /// Per-connection writers (connection_id → sender to writer task).
    writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
    /// Per-dataflow registrations and pending control frame buffer.
    ///
    /// A single lock protects both to avoid TOCTOU races: the reader_task
    /// can atomically check if a dataflow is registered and, if not, buffer
    /// the control frame — all under one lock acquisition.
    reg_state: Arc<TokioMutex<RegistrationState>>,
    /// Reorder buffers keyed by (dataflow_id) — one per payload lane.
    reorder_buffers: Arc<TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>>,
    /// Scaling driver for RTT probing.
    scaling_driver: Arc<ScalingDriver>,
    /// Probe counter for generating probe sequence IDs.
    probe_counter: Arc<ProbeCounter>,
    /// Factory used to establish replacement or scaled-up connections.
    connection_factory: Arc<dyn DynConnectionFactory>,
    /// Runtime handle used to spawn replacement connection tasks.
    runtime_handle: tokio::runtime::Handle,
    /// Guards lazy connection initialization so concurrent registrations don't over-connect.
    init_lock: Arc<TokioMutex<()>>,
    /// Background task handles (aborted on drop).
    task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// Payload sender: frames from any dataflow go here for sequencing + pool routing.
    payload_tx: tokio_mpsc::Sender<(DataflowId, Frame)>,
    /// Control sender: high-priority frames bypass sequencing.
    control_tx: tokio_mpsc::Sender<Frame>,
    /// Failure notification sender: writer/reader tasks send failed conn IDs here.
    failure_tx: tokio_mpsc::Sender<usize>,
}

impl Drop for SharedPeerManager {
    fn drop(&mut self) {
        if let Ok(handles) = self.task_handles.lock() {
            for handle in handles.iter() {
                handle.abort();
            }
        }
    }
}

impl SharedPeerManager {
    /// Create a new shared peer manager.
    ///
    /// Connections are established lazily through `connection_factory` when the
    /// first dataflow registers or when the scaling driver emits a scale-up event.
    pub fn new(
        peer_node_id: String,
        config: SharedConnectionConfig,
        connection_factory: Arc<dyn DynConnectionFactory>,
        runtime_handle: &tokio::runtime::Handle,
    ) -> crate::Result<Self> {
        let pool = Arc::new(PeerPool::new(0, config.clone())?);
        let (scaling_driver, scaling_event_rx) = ScalingDriver::new(config.clone());
        let scaling_driver = Arc::new(scaling_driver);
        let probe_counter = Arc::new(ProbeCounter::new());

        // Payload channel: dataflow tasks send (dataflow_id, frame) here
        let (payload_tx, payload_rx) = tokio_mpsc::channel::<(DataflowId, Frame)>(1024);
        // Control channel: high-priority frames
        let (control_tx, control_rx) = tokio_mpsc::channel::<Frame>(256);
        // Failure notification channel: writer/reader tasks report dead connections
        let (failure_tx, failure_rx) = tokio_mpsc::channel::<usize>(64);

        let reg_state: Arc<TokioMutex<RegistrationState>> =
            Arc::new(TokioMutex::new(RegistrationState {
                registered: HashMap::new(),
                pending_control: HashMap::new(),
                completed: HashSet::new(),
            }));
        let reorder_buffers = Arc::new(TokioMutex::new(HashMap::new()));
        let init_lock = Arc::new(TokioMutex::new(()));
        let task_handles = Arc::new(Mutex::new(Vec::new()));
        let writer_channels = Arc::new(TokioMutex::new(HashMap::new()));

        let bridge_handle = runtime_handle.spawn(Self::bridge_task(
            pool.clone(),
            writer_channels.clone(),
            reg_state.clone(),
            payload_rx,
            control_rx,
        ));
        push_task_handle(&task_handles, bridge_handle);

        let probe_handle = runtime_handle.spawn(Self::probe_loop(
            scaling_driver.clone(),
            probe_counter.clone(),
            writer_channels.clone(),
            pool.clone(),
            config.probe_interval,
        ));
        push_task_handle(&task_handles, probe_handle);

        let scale_handle = runtime_handle.spawn(Self::scaling_event_handler(
            scaling_event_rx,
            peer_node_id.clone(),
            connection_factory.clone(),
            runtime_handle.clone(),
            pool.clone(),
            writer_channels.clone(),
            reg_state.clone(),
            reorder_buffers.clone(),
            scaling_driver.clone(),
            failure_tx.clone(),
            task_handles.clone(),
        ));
        push_task_handle(&task_handles, scale_handle);

        let monitor_handle = runtime_handle.spawn(Self::connection_monitor(
            failure_rx,
            pool.clone(),
            writer_channels.clone(),
            scaling_driver.clone(),
        ));
        push_task_handle(&task_handles, monitor_handle);

        let sweep_handle = runtime_handle.spawn(Self::timeout_sweeper(
            reorder_buffers.clone(),
            reg_state.clone(),
            config.reorder_timeout,
        ));
        push_task_handle(&task_handles, sweep_handle);

        Ok(Self {
            peer_node_id,
            config,
            pool,
            writer_channels,
            reg_state,
            reorder_buffers,
            scaling_driver,
            probe_counter,
            connection_factory,
            runtime_handle: runtime_handle.clone(),
            init_lock,
            task_handles,
            payload_tx,
            control_tx,
            failure_tx,
        })
    }

    /// Register a dataflow and its channels with this peer manager.
    ///
    /// A control channel (ID 0) is **automatically registered** for each
    /// dataflow, matching `TransportSession` behavior. Returns per-channel
    /// receivers for incoming frames.
    ///
    /// Any control frames that arrived before registration (buffered in
    /// `pending_control`) are drained into the control channel immediately.
    /// Registration and pending-drain happen under a single lock to prevent
    /// a TOCTOU race with the reader_task.
    pub async fn register_dataflow(
        &self,
        dataflow_id: DataflowId,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> (
        HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>,
        tokio_mpsc::Receiver<TransportError>,
    ) {
        let mut receivers = HashMap::new();
        let mut channel_senders = HashMap::new();

        // Auto-register control channel
        if !channel_ids.contains(&CONTROL_CHANNEL_ID) {
            let (tx, rx) = tokio_mpsc::channel(channel_capacity);
            channel_senders.insert(CONTROL_CHANNEL_ID, tx);
            receivers.insert(CONTROL_CHANNEL_ID, rx);
        }

        for &ch_id in channel_ids {
            let (tx, rx) = tokio_mpsc::channel(channel_capacity);
            channel_senders.insert(ch_id, tx);
            receivers.insert(ch_id, rx);
        }

        // Error channel: capacity 4 (failures are rare, one notification is enough)
        let (error_tx, error_rx) = tokio_mpsc::channel(4);

        // Ensure minimum connections exist BEFORE registering the dataflow.
        // This prevents a race where the caller receives senders and immediately
        // sends frames before any connection is established — those frames would
        // be silently dropped by bridge_task.
        if let Err(e) = self.ensure_min_connections().await {
            // All connection attempts failed — surface the error immediately.
            let _ = error_tx.try_send(TransportError::ConnectionClosed);
            #[cfg(feature = "tracing")]
            tracing::error!(
                "Failed to establish connections for peer {} during dataflow registration: {e}",
                self.peer_node_id
            );
        }

        let reg = DataflowRegistration {
            sequence_counter: SequenceCounter::new(),
            channel_senders,
            error_tx,
        };

        // Atomically under one lock:
        //   1. Drain any control frames that arrived before this registration
        //   2. Insert the registration so future frames are routed directly
        //   3. Deliver drained frames into the new control channel
        //
        // All three steps happen while holding `reg_state`, so reader_task
        // cannot interleave between drain and register (see RegistrationState
        // doc comment for the full race condition explanation).
        //
        // Buffered frames are delivered with try_send (not async send) so we
        // don't yield while holding the lock. This is safe because the channel
        // was just created with channel_capacity slots and pending frames are
        // few (typically 1-2: one Handshake and/or one Ready).
        {
            let mut state = self.reg_state.lock().await;
            let pending = state
                .pending_control
                .remove(&dataflow_id)
                .unwrap_or_default();
            state.registered.insert(dataflow_id, reg);

            // Deliver buffered frames while still holding the lock.
            // try_send is safe here: the channel was just created with
            // channel_capacity slots and pending frames are few (1-2 at most).
            if !pending.is_empty() {
                if let Some(tx) = state
                    .registered
                    .get(&dataflow_id)
                    .and_then(|r| r.channel_senders.get(&CONTROL_CHANNEL_ID))
                {
                    for payload in pending {
                        let _ = tx.try_send(payload);
                    }
                }
            }
        }

        // Create reorder buffer for this dataflow's payload lane
        self.reorder_buffers.lock().await.insert(
            dataflow_id,
            ReorderBuffer::with_capacity(self.config.reorder_timeout, 4096),
        );

        (receivers, error_rx)
    }

    /// Ensure the pool has at least `min_connections` live connections.
    /// Returns Ok(()) if the pool meets the minimum, or Err if no connections
    /// could be established at all (partial success still returns Ok).
    async fn ensure_min_connections(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let live = self.pool.live_connection_count();
        if live >= self.config.min_connections {
            return Ok(());
        }

        let _guard = self.init_lock.lock().await;
        let live = self.pool.live_connection_count();
        if live >= self.config.min_connections {
            return Ok(());
        }

        let deficit = self.config.min_connections - live;
        let mut last_error = None;
        for _ in 0..deficit {
            if let Err(error) = Self::reconnect_connection(
                &self.peer_node_id,
                self.connection_factory.clone(),
                self.runtime_handle.clone(),
                self.pool.clone(),
                self.writer_channels.clone(),
                self.reg_state.clone(),
                self.reorder_buffers.clone(),
                self.scaling_driver.clone(),
                self.failure_tx.clone(),
                self.task_handles.clone(),
            )
            .await
            {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "Lazy connection establishment failed for peer {}: {error}",
                    self.peer_node_id
                );
                last_error = Some(error);
                break;
            }
        }

        // If we still have zero live connections, report the last error
        if self.pool.live_connection_count() == 0 {
            if let Some(e) = last_error {
                return Err(e);
            }
        }
        Ok(())
    }

    /// Unregister a dataflow, removing its channels, reorder buffer, and pending control frames.
    /// The dataflow ID is recorded as completed so late control frames are dropped.
    pub async fn unregister_dataflow(&self, dataflow_id: &DataflowId) {
        {
            let mut state = self.reg_state.lock().await;
            state.registered.remove(dataflow_id);
            state.pending_control.remove(dataflow_id);
            state.completed.insert(*dataflow_id);
        }
        self.reorder_buffers.lock().await.remove(dataflow_id);
    }

    /// Get the payload sender for submitting data/progress frames.
    pub fn payload_sender(&self) -> &tokio_mpsc::Sender<(DataflowId, Frame)> {
        &self.payload_tx
    }

    /// Get the control sender for submitting high-priority frames.
    pub fn control_sender(&self) -> &tokio_mpsc::Sender<Frame> {
        &self.control_tx
    }

    /// Get the peer node ID.
    pub fn peer_node_id(&self) -> &str {
        &self.peer_node_id
    }

    /// Get current connection count.
    pub fn connection_count(&self) -> usize {
        self.pool.connection_count()
    }

    /// Establish a replacement or additional connection and spawn its tasks.
    #[allow(clippy::too_many_arguments)]
    async fn reconnect_connection(
        peer_node_id: &str,
        connection_factory: Arc<dyn DynConnectionFactory>,
        runtime_handle: tokio::runtime::Handle,
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        reg_state: Arc<TokioMutex<RegistrationState>>,
        reorder_buffers: Arc<TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>>,
        scaling_driver: Arc<ScalingDriver>,
        failure_tx: tokio_mpsc::Sender<usize>,
        task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let (reader, writer) = connection_factory.establish_dyn(peer_node_id).await?;
        let conn_metrics = pool.add_connection().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::other(
                "peer pool is already at max live connections",
            ))
        })?;
        let conn_id = conn_metrics.id;
        let (tx, rx) = tokio_mpsc::channel::<Frame>(256);
        writer_channels.lock().await.insert(conn_id, tx);

        let writer_handle = runtime_handle.spawn(Self::writer_task(
            conn_id,
            writer,
            rx,
            Some(conn_metrics),
            failure_tx.clone(),
        ));
        let reader_handle = runtime_handle.spawn(Self::reader_task(
            conn_id,
            reader,
            reg_state,
            reorder_buffers,
            scaling_driver,
            pool,
            writer_channels,
            failure_tx,
        ));

        match task_handles.lock().or_poison("task handle") {
            Ok(mut handles) => {
                handles.push(writer_handle);
                handles.push(reader_handle);
            }
            Err(_) => {
                // TODO: propagate poisoned task-handle locks once reconnect_connection can return richer errors.
                writer_handle.abort();
                reader_handle.abort();
            }
        }
        Ok(conn_id)
    }

    // -----------------------------------------------------------------------
    // Background tasks
    // -----------------------------------------------------------------------

    /// Per-connection writer task: reads frames from channel and writes to TCP.
    ///
    /// After each successful write, calls `dequeue()` on the connection metrics
    /// to accurately track pending writes and throughput. On write failure,
    /// marks the connection dead and notifies via `failure_tx`.
    async fn writer_task<W: AsyncWrite + Unpin>(
        conn_id: usize,
        writer: W,
        mut rx: tokio_mpsc::Receiver<Frame>,
        conn_metrics: Option<Arc<ConnectionMetrics>>,
        failure_tx: tokio_mpsc::Sender<usize>,
    ) {
        let mut framed = FramedWriter::new(writer);
        while let Some(frame) = rx.recv().await {
            let payload_size = frame.payload.len();
            let is_user_traffic = frame.channel_id != PROBE_CHANNEL_ID;
            if let Err(_e) = framed.write_frame(&frame).await {
                #[cfg(feature = "tracing")]
                tracing::error!("Writer task conn {conn_id} write error: {_e}");
                // Mark connection dead and notify monitor
                if let Some(ref metrics) = conn_metrics {
                    metrics.mark_dead();
                }
                let _ = failure_tx.try_send(conn_id);
                break;
            }
            if let Some(ref metrics) = conn_metrics {
                metrics.dequeue(payload_size, is_user_traffic);
            }
        }
    }

    /// Bridge task: receives payload and control frames, sequences payloads,
    /// and routes to connections via the pool.
    ///
    /// Control frames get priority (biased select). Payload frames are assigned
    /// a sequence_id from the per-dataflow counter before being sent to the
    /// least-loaded connection.
    async fn bridge_task(
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        reg_state: Arc<TokioMutex<RegistrationState>>,
        mut payload_rx: tokio_mpsc::Receiver<(DataflowId, Frame)>,
        mut control_rx: tokio_mpsc::Receiver<Frame>,
    ) {
        let mut control_open = true;
        let mut payload_open = true;

        loop {
            if !control_open && !payload_open {
                break;
            }

            tokio::select! {
                biased;

                result = control_rx.recv(), if control_open => {
                    match result {
                        Some(frame) => {
                            // Control frames go to the first available connection
                            let tx = {
                                let wc = writer_channels.lock().await;
                                wc.values().next().cloned()
                            };
                            if let Some(tx) = tx {
                                let _ = tx.send(frame).await;
                            } else {
                                // No live connections — notify the dataflow via error channel
                                // so it fails fast instead of hanging on a handshake timeout.
                                let state = reg_state.lock().await;
                                if let Some(reg) = state.registered.get(&frame.dataflow_id) {
                                    let _ = reg.error_tx.try_send(TransportError::ConnectionClosed);
                                }
                            }
                        }
                        None => { control_open = false; }
                    }
                }

                result = payload_rx.recv(), if payload_open => {
                    match result {
                        Some((dataflow_id, mut frame)) => {
                            // Verify dataflow is still registered before proceeding
                            {
                                let state = reg_state.lock().await;
                                if !state.registered.contains_key(&dataflow_id) {
                                    continue; // dataflow unregistered, drop frame
                                }
                            }

                            // Select a live connection first — only assign a sequence
                            // number AFTER confirming we can actually send. This prevents
                            // sequence gaps in the reorder buffer when frames are dropped
                            // due to no available connections (e.g., during reconnect).
                            let mut exclude = HashSet::new();
                            let first_conn = match pool.select_connection_excluding(&exclude) {
                                Some(c) => {
                                    c.enqueue();
                                    c
                                }
                                None => {
                                    // No live connections available — drop frame
                                    // WITHOUT consuming a sequence number.
                                    #[cfg(feature = "tracing")]
                                    tracing::error!(
                                        "No live connections for payload frame, dropping"
                                    );
                                    continue;
                                }
                            };

                            // Now assign sequence_id — we know at least one connection
                            // is available so this frame will be attempted.
                            let seq_id = {
                                let state = reg_state.lock().await;
                                if let Some(reg) = state.registered.get(&dataflow_id) {
                                    reg.sequence_counter.next_seq()
                                } else {
                                    first_conn.rollback_reservation();
                                    continue; // dataflow unregistered between check and here
                                }
                            };

                            // Prepend sequence_id to payload
                            let mut sequenced_payload = Vec::with_capacity(8 + frame.payload.len());
                            sequenced_payload.extend_from_slice(&seq_id.to_le_bytes());
                            sequenced_payload.extend_from_slice(&frame.payload);
                            frame.payload = sequenced_payload;

                            // Try sending on the selected connection; retry on others if needed
                            let mut current_frame = frame;
                            let mut conn = first_conn;
                            loop {
                                let conn_id = conn.id;

                                // Clone sender under lock, then release before await
                                let tx = {
                                    let wc = writer_channels.lock().await;
                                    wc.get(&conn_id).cloned()
                                };
                                match tx {
                                    Some(tx) => {
                                        match tx.send(current_frame).await {
                                            Ok(()) => break, // Successfully enqueued
                                            Err(tokio_mpsc::error::SendError(returned)) => {
                                                // Recover the frame and retry on another connection
                                                current_frame = returned;
                                                conn.rollback_reservation();
                                                conn.mark_dead();
                                                exclude.insert(conn_id);
                                                #[cfg(feature = "tracing")]
                                                tracing::warn!(
                                                    "Writer channel closed for conn {conn_id}, retrying"
                                                );
                                            }
                                        }
                                    }
                                    None => {
                                        // Writer channel already removed (monitor cleaned it up)
                                        conn.rollback_reservation();
                                        exclude.insert(conn_id);
                                    }
                                }

                                // Try the next connection
                                conn = match pool.select_connection_excluding(&exclude) {
                                    Some(c) => {
                                        c.enqueue();
                                        c
                                    }
                                    None => {
                                        // Exhausted all connections after seq was assigned.
                                        // The frame is lost — this creates a sequence gap,
                                        // but it's unavoidable at this point since the seq
                                        // was already committed.
                                        #[cfg(feature = "tracing")]
                                        tracing::error!(
                                            "All connections exhausted after seq assignment, frame lost"
                                        );
                                        break;
                                    }
                                };
                            }
                        }
                        None => { payload_open = false; }
                    }
                }
            }
        }
    }

    /// Reader task: reads frames from one connection, dispatches to reorder
    /// buffers and then to per-channel receivers.
    ///
    /// Special handling:
    /// - Probe frames: process replies or generate reply to requests
    /// - Control frames (channel 0): delivered directly without sequencing;
    ///   if the target dataflow is not yet registered, frames are buffered in
    ///   `pending_control` and drained on registration.
    /// - Payload frames: stripped of sequence prefix, reordered, dispatched
    ///
    /// On read error, marks the connection dead and notifies via `failure_tx`.
    #[allow(clippy::too_many_arguments)]
    async fn reader_task<R: AsyncRead + Unpin>(
        conn_id: usize,
        reader: R,
        reg_state: Arc<TokioMutex<RegistrationState>>,
        reorder_buffers: Arc<TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>>,
        scaling_driver: Arc<ScalingDriver>,
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        failure_tx: tokio_mpsc::Sender<usize>,
    ) {
        let mut framed = FramedReader::new(reader);

        loop {
            match framed.read_frame().await {
                Ok(frame) => {
                    // Check for probe messages
                    if frame.channel_id == PROBE_CHANNEL_ID {
                        Self::handle_probe_frame(
                            &frame,
                            &scaling_driver,
                            &pool,
                            conn_id,
                            &writer_channels,
                        )
                        .await;
                        continue;
                    }

                    // Control frames (Handshake, Ready) bypass sequencing — deliver
                    // the raw payload directly to the control channel.
                    //
                    // Race condition handling: the remote peer may send a control
                    // frame for a dataflow that hasn't been registered locally yet
                    // (e.g., node-A finishes df1 and starts df2's handshake before
                    // node-B has registered df2). Instead of dropping the frame, we
                    // buffer it in `pending_control` so that `register_dataflow()`
                    // can drain it upon registration. Both the "check registration +
                    // buffer" here and the "drain + register" in register_dataflow()
                    // hold the same `reg_state` lock, preventing TOCTOU races.
                    if frame.channel_id == CONTROL_CHANNEL_ID {
                        let tx = {
                            let mut state = reg_state.lock().await;
                            match state.registered.get(&frame.dataflow_id) {
                                Some(reg) => reg.channel_senders.get(&CONTROL_CHANNEL_ID).cloned(),
                                None => {
                                    // Drop frames for completed dataflows instead
                                    // of buffering them indefinitely.
                                    if state.completed.contains(&frame.dataflow_id) {
                                        continue;
                                    }
                                    // Dataflow not registered yet — buffer for later
                                    state
                                        .pending_control
                                        .entry(frame.dataflow_id)
                                        .or_default()
                                        .push(frame.payload);
                                    continue;
                                }
                            }
                        };
                        if let Some(tx) = tx {
                            let _ = tx.send(frame.payload).await;
                        }
                        continue;
                    }

                    // Extract sequence_id from payload prefix
                    if frame.payload.len() < 8 {
                        continue; // malformed
                    }
                    let seq_id = match wire::read_u64(&frame.payload, 0) {
                        Ok(seq_id) => seq_id,
                        Err(_) => continue,
                    };
                    let inner_payload = frame.payload[8..].to_vec();

                    let inner_frame = Frame {
                        dataflow_id: frame.dataflow_id,
                        channel_id: frame.channel_id,
                        payload: inner_payload,
                    };

                    // Insert into reorder buffer for this dataflow
                    let mut buffers = reorder_buffers.lock().await;
                    if let Some(buffer) = buffers.get_mut(&frame.dataflow_id) {
                        match buffer.insert(seq_id, inner_frame) {
                            Ok(InsertResult::Ready(_count)) => {
                                // Drain ready frames and dispatch
                                let ready: Vec<Frame> = buffer.drain_ready().collect();
                                debug_assert_eq!(
                                    _count,
                                    ready.len(),
                                    "InsertResult::Ready count must match drain_ready length"
                                );
                                drop(buffers); // release lock before dispatching

                                // Clone senders under lock, then release before awaiting
                                let senders = {
                                    let state = reg_state.lock().await;
                                    state
                                        .registered
                                        .get(&frame.dataflow_id)
                                        .map(|reg| reg.channel_senders.clone())
                                };

                                if let Some(senders) = senders {
                                    for ready_frame in ready {
                                        if let Some(tx) = senders.get(&ready_frame.channel_id) {
                                            let _ = tx.send(ready_frame.payload).await;
                                        }
                                    }
                                }
                            }
                            Ok(InsertResult::Buffered) => {
                                // Waiting for earlier frames
                            }
                            Ok(InsertResult::Duplicate) => {
                                // Already delivered, ignore
                            }
                            Err(_overflow) => {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    "Reorder buffer overflow for dataflow {:?}",
                                    frame.dataflow_id
                                );
                            }
                        }
                    }
                }
                Err(TransportError::ConnectionClosed) => {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Reader task conn {conn_id} closed");
                    if let Some(metrics) = pool.connection(conn_id) {
                        metrics.mark_dead();
                    }
                    let _ = failure_tx.try_send(conn_id);
                    break;
                }
                Err(_e) => {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Reader task conn {conn_id} error: {_e}");
                    if let Some(metrics) = pool.connection(conn_id) {
                        metrics.mark_dead();
                    }
                    let _ = failure_tx.try_send(conn_id);
                    break;
                }
            }
        }
    }

    /// Handle a probe frame received from a connection.
    ///
    /// - Reply probes: compute RTT and update connection metrics
    /// - Request probes: generate a reply and send it back via the same
    ///   connection's writer channel
    async fn handle_probe_frame(
        frame: &Frame,
        scaling_driver: &Arc<ScalingDriver>,
        pool: &Arc<PeerPool>,
        conn_id: usize,
        writer_channels: &Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
    ) {
        if let Some(probe) = ProbeMessage::decode(&frame.payload) {
            match probe.kind {
                ProbeKind::Reply => {
                    // Process the reply — updates RTT on the connection
                    if let Some(conn) = pool.connection(conn_id) {
                        scaling_driver.process_probe_reply(&probe, &conn);
                    }
                }
                ProbeKind::Request => {
                    // Generate and send a reply back on the same connection
                    let reply = ProbeMessage::reply_to(&probe);
                    let reply_frame = Frame {
                        dataflow_id: DataflowId::nil(),
                        channel_id: PROBE_CHANNEL_ID,
                        payload: reply.encode().to_vec(),
                    };

                    let tx = {
                        let wc = writer_channels.lock().await;
                        wc.get(&conn_id).cloned()
                    };
                    if let Some(tx) = tx {
                        let _ = tx.try_send(reply_frame); // best-effort
                    }
                }
            }
        }
    }

    /// Periodic probe loop: sends RTT probes to each connection.
    async fn probe_loop(
        scaling_driver: Arc<ScalingDriver>,
        probe_counter: Arc<ProbeCounter>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        pool: Arc<PeerPool>,
        interval: Duration,
    ) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick — the first probe should fire after
        // one full interval, giving connections time to be established.
        ticker.tick().await;

        loop {
            ticker.tick().await;

            // Send a probe to each active connection
            let wc = writer_channels.lock().await;
            for (_conn_id, tx) in wc.iter() {
                let seq = probe_counter.next_seq();
                let send_ts = scaling_driver.timestamp().now_nanos();

                let probe = ProbeMessage::new_request(seq, send_ts);
                scaling_driver.record_probe_sent(seq, send_ts);

                let frame = Frame {
                    dataflow_id: DataflowId::nil(),
                    channel_id: PROBE_CHANNEL_ID,
                    payload: probe.encode().to_vec(),
                };

                let _ = tx.try_send(frame); // best-effort
            }

            // Evaluate scaling after probing
            scaling_driver.evaluate_and_emit(&pool).await;
        }
    }

    /// Handles scaling and failure-recovery events.
    #[allow(clippy::too_many_arguments)]
    async fn scaling_event_handler(
        mut event_rx: tokio_mpsc::Receiver<ScalingEvent>,
        peer_node_id: String,
        connection_factory: Arc<dyn DynConnectionFactory>,
        runtime_handle: tokio::runtime::Handle,
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        reg_state: Arc<TokioMutex<RegistrationState>>,
        reorder_buffers: Arc<TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>>,
        scaling_driver: Arc<ScalingDriver>,
        failure_tx: tokio_mpsc::Sender<usize>,
        task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ) {
        while let Some(event) = event_rx.recv().await {
            match event {
                ScalingEvent::ScaleUp => {
                    let factory = connection_factory.clone();

                    match Self::reconnect_connection(
                        &peer_node_id,
                        factory,
                        runtime_handle.clone(),
                        pool.clone(),
                        writer_channels.clone(),
                        reg_state.clone(),
                        reorder_buffers.clone(),
                        scaling_driver.clone(),
                        failure_tx.clone(),
                        task_handles.clone(),
                    )
                    .await
                    {
                        Ok(conn_id) => {
                            #[cfg(feature = "tracing")]
                            tracing::info!(
                                "ScaleUp established new connection {conn_id} for peer {peer_node_id}"
                            );
                        }
                        Err(error) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!("ScaleUp failed for peer {peer_node_id}: {error}");
                        }
                    }
                }
                ScalingEvent::ScaleDown { connection_id } => {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Scaling event: ScaleDown conn {connection_id}");
                    {
                        let mut wc = writer_channels.lock().await;
                        wc.remove(&connection_id);
                    }
                    if let Some(metrics) = pool.connection(connection_id) {
                        metrics.mark_dead();
                    }
                    let _ = pool.remove_connection(connection_id);
                }
                ScalingEvent::ConnectionFailed { connection_id } => {
                    // Remove the dead connection from the pool to avoid
                    // accumulating stale entries over time.
                    let _ = pool.remove_connection(connection_id);

                    let factory = connection_factory.clone();

                    // Exponential backoff reconnect: 100ms → 200ms → 400ms → 800ms → 1.6s
                    // (up to 5 attempts). If the peer is temporarily unavailable,
                    // this gives ~3s total window for recovery.
                    let max_attempts = 5u32;
                    let mut delay = Duration::from_millis(100);
                    let mut recovered = false;
                    for attempt in 1..=max_attempts {
                        match Self::reconnect_connection(
                            &peer_node_id,
                            factory.clone(),
                            runtime_handle.clone(),
                            pool.clone(),
                            writer_channels.clone(),
                            reg_state.clone(),
                            reorder_buffers.clone(),
                            scaling_driver.clone(),
                            failure_tx.clone(),
                            task_handles.clone(),
                        )
                        .await
                        {
                            Ok(new_conn_id) => {
                                recovered = true;
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Recovered peer {peer_node_id} after conn {connection_id} failed; new conn {new_conn_id}"
                                );
                                break;
                            }
                            Err(error) => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    "Reconnect attempt {attempt}/{max_attempts} failed for peer {peer_node_id} after conn {connection_id} failed: {error}"
                                );
                                if attempt < max_attempts {
                                    tokio::time::sleep(delay).await;
                                    delay = std::cmp::min(
                                        delay.saturating_mul(2),
                                        Duration::from_secs(5),
                                    );
                                }
                            }
                        }
                    }

                    // Only notify dataflows after all reconnect attempts are
                    // exhausted AND there are still no live connections.
                    if !recovered && pool.live_connection_count() == 0 {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            "Failed to recover peer {peer_node_id} after conn {connection_id} failed — notifying dataflows"
                        );
                        let state = reg_state.lock().await;
                        for (_df_id, reg) in state.registered.iter() {
                            let _ = reg.error_tx.try_send(TransportError::ConnectionClosed);
                        }
                    }
                }
            }
        }
    }

    /// Connection failure monitor: listens for dead connection notifications,
    /// removes their writer channels, and emits `ConnectionFailed` events.
    ///
    /// Deduplicates notifications (both writer and reader may report the same
    /// connection) — only processes each connection_id once.
    ///
    /// Does NOT immediately notify dataflows when all connections die — the
    /// `scaling_event_handler` attempts reconnect first and only surfaces
    /// `TransportError::ConnectionClosed` after reconnect is exhausted.
    async fn connection_monitor(
        mut failure_rx: tokio_mpsc::Receiver<usize>,
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        scaling_driver: Arc<ScalingDriver>,
    ) {
        let mut processed = HashSet::new();
        while let Some(conn_id) = failure_rx.recv().await {
            if !processed.insert(conn_id) {
                continue; // Already handled this connection
            }

            let live_count = pool.live_connection_count();

            #[cfg(feature = "tracing")]
            tracing::warn!(
                "Connection monitor: conn {conn_id} failed, removing. Live: {live_count}"
            );

            // Remove writer channel (drops the sender, which will cause
            // writer_task to exit if it hasn't already)
            {
                let mut wc = writer_channels.lock().await;
                wc.remove(&conn_id);
            }

            // Emit scaling event — the scaling_event_handler will attempt
            // reconnect and notify dataflows only if recovery fails.
            scaling_driver
                .emit_event(ScalingEvent::ConnectionFailed {
                    connection_id: conn_id,
                })
                .await;
        }
    }

    /// Periodic sweeper that checks reorder buffer timeouts.
    ///
    /// If any dataflow's reorder buffer has a gap that exceeds the timeout,
    /// sends a `TransportError::ConnectionClosed` to that dataflow's error channel.
    async fn timeout_sweeper(
        reorder_buffers: Arc<TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>>,
        reg_state: Arc<TokioMutex<RegistrationState>>,
        check_interval: Duration,
    ) {
        let mut ticker = tokio::time::interval(check_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            let mut buffers = reorder_buffers.lock().await;
            let mut timed_out_dataflows = Vec::new();
            for (&df_id, buffer) in buffers.iter_mut() {
                if let Err(_e) = buffer.check_timeout() {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Reorder gap timeout for dataflow {df_id:?}: {_e}");
                    timed_out_dataflows.push(df_id);
                }
            }
            drop(buffers);

            // Notify affected dataflows
            if !timed_out_dataflows.is_empty() {
                let state = reg_state.lock().await;
                for df_id in timed_out_dataflows {
                    if let Some(reg) = state.registered.get(&df_id) {
                        let _ = reg.error_tx.try_send(TransportError::ReorderTimeout);
                    }
                }
            }
        }
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
/// preserving the timely invariant that data at time T arrives before the
/// progress message releasing T.
pub struct SharedTransportSession {
    /// The dataflow this session belongs to.
    dataflow_id: DataflowId,
    /// Per-peer payload senders (data + progress share this for FIFO ordering).
    payload_senders: HashMap<String, tokio_mpsc::Sender<(DataflowId, Frame)>>,
    /// Per-peer control senders (high priority, bypass sequencing).
    control_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Per-peer channel receivers (peer → channel_id → receiver).
    /// Wrapped in Option so they can be taken out once.
    receivers: Option<HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>>,
    /// Per-peer error receivers (peer → error channel).
    /// Dataflows poll these to detect transport failures.
    error_receivers: Option<HashMap<String, tokio_mpsc::Receiver<TransportError>>>,
}

impl SharedTransportSession {
    /// Create a new shared transport session by registering with peer managers.
    ///
    /// # Arguments
    /// - `dataflow_id`: Identifies this dataflow
    /// - `peer_managers`: Map of peer_node_id → SharedPeerManager
    /// - `channel_ids`: Channel IDs to register for each peer
    /// - `channel_capacity`: Buffer capacity for per-channel receivers
    pub async fn new(
        dataflow_id: DataflowId,
        peer_managers: &HashMap<String, SharedPeerManager>,
        channel_ids: &[u64],
        channel_capacity: usize,
    ) -> Self {
        let mut payload_senders = HashMap::new();
        let mut control_senders = HashMap::new();
        let mut all_receivers = HashMap::new();
        let mut error_receivers = HashMap::new();

        for (peer_id, manager) in peer_managers {
            // Register this dataflow with the peer manager
            let (receivers, error_rx) = manager
                .register_dataflow(dataflow_id, channel_ids, channel_capacity)
                .await;

            payload_senders.insert(peer_id.clone(), manager.payload_sender().clone());
            control_senders.insert(peer_id.clone(), manager.control_sender().clone());
            all_receivers.insert(peer_id.clone(), receivers);
            error_receivers.insert(peer_id.clone(), error_rx);
        }

        Self {
            dataflow_id,
            payload_senders,
            control_senders,
            receivers: Some(all_receivers),
            error_receivers: Some(error_receivers),
        }
    }

    /// Get a data sender for a peer.
    ///
    /// Returns the shared payload sender. Data and progress share the same
    /// sequenced lane to preserve the timely ordering invariant.
    pub fn data_sender(&self, peer_node_id: &str) -> Option<DataframeSender> {
        self.payload_senders
            .get(peer_node_id)
            .map(|tx| DataframeSender {
                dataflow_id: self.dataflow_id,
                tx: tx.clone(),
            })
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

    /// Take the per-peer error receivers (can only be called once).
    ///
    /// Dataflows should poll these receivers to detect transport failures.
    /// A received `TransportError` indicates the peer connection has failed
    /// (either all connections dead, or reorder buffer gap timeout).
    pub fn take_error_receivers(
        &mut self,
    ) -> Option<HashMap<String, tokio_mpsc::Receiver<TransportError>>> {
        self.error_receivers.take()
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

/// A sender that automatically tags frames with the dataflow ID.
///
/// This provides a clean API: callers send `Frame`s directly without
/// needing to wrap them in `(DataflowId, Frame)` tuples.
#[derive(Clone)]
pub struct DataframeSender {
    dataflow_id: DataflowId,
    tx: tokio_mpsc::Sender<(DataflowId, Frame)>,
}

impl DataframeSender {
    /// Send a frame through the shared transport.
    ///
    /// The frame's `dataflow_id` is normalized to this sender's dataflow,
    /// a sequence ID is assigned, and the frame is routed to the
    /// least-loaded connection in the pool.
    pub async fn send(&self, mut frame: Frame) -> Result<(), tokio_mpsc::error::SendError<Frame>> {
        frame.dataflow_id = self.dataflow_id;
        self.tx
            .send((self.dataflow_id, frame))
            .await
            .map_err(|e| tokio_mpsc::error::SendError(e.0.1))
    }

    /// Try to send a frame without blocking.
    pub fn try_send(&self, mut frame: Frame) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        frame.dataflow_id = self.dataflow_id;
        self.tx
            .try_send((self.dataflow_id, frame))
            .map_err(|e| match e {
                tokio_mpsc::error::TrySendError::Full(v) => {
                    tokio_mpsc::error::TrySendError::Full(v.1)
                }
                tokio_mpsc::error::TrySendError::Closed(v) => {
                    tokio_mpsc::error::TrySendError::Closed(v.1)
                }
            })
    }
}

// ---------------------------------------------------------------------------
// Timeout sweeper (called by the reader or a dedicated task)
// ---------------------------------------------------------------------------

/// Periodically checks reorder buffer timeouts and reports gaps.
///
/// Returns the dataflow IDs that have timed-out gaps (indicating lost frames).
pub async fn check_reorder_timeouts(
    reorder_buffers: &TokioMutex<HashMap<DataflowId, ReorderBuffer<Frame>>>,
) -> Vec<DataflowId> {
    let mut timed_out = Vec::new();
    let mut buffers = reorder_buffers.lock().await;
    for (&df_id, buffer) in buffers.iter_mut() {
        if let Err(_e) = buffer.check_timeout() {
            timed_out.push(df_id);
            #[cfg(feature = "tracing")]
            tracing::error!("Reorder gap timeout for dataflow {:?}: {_e}", df_id);
        }
    }
    timed_out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::super::shared_pool::ScalingDecision;
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf, duplex};
    use tokio::task::JoinHandle;

    fn make_echo_connection() -> (
        ReadHalf<DuplexStream>,
        WriteHalf<DuplexStream>,
        JoinHandle<()>,
    ) {
        let (manager_stream, remote_stream) = duplex(65536);
        let (manager_read, manager_write) = tokio::io::split(manager_stream);
        let (mut remote_read, mut remote_write) = tokio::io::split(remote_stream);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match remote_read.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if remote_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        (manager_read, manager_write, handle)
    }

    #[derive(Default)]
    struct EchoConnectionFactory {
        remote_tasks: Mutex<Vec<JoinHandle<()>>>,
    }

    impl ConnectionFactory for EchoConnectionFactory {
        type Reader = ReadHalf<DuplexStream>;
        type Writer = WriteHalf<DuplexStream>;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
        {
            let (reader, writer, remote_task) = make_echo_connection();
            self.remote_tasks
                .lock()
                .expect("echo factory task lock poisoned")
                .push(remote_task);
            Ok((reader, writer))
        }
    }

    struct PreEstablishedFactory {
        connections: Mutex<VecDeque<(DynReader, DynWriter)>>,
    }

    impl PreEstablishedFactory {
        fn new<R, W>(connections: Vec<(R, W)>) -> Self
        where
            R: AsyncRead + Unpin + Send + 'static,
            W: AsyncWrite + Unpin + Send + 'static,
        {
            let connections = connections
                .into_iter()
                .map(|(reader, writer)| {
                    (Box::new(reader) as DynReader, Box::new(writer) as DynWriter)
                })
                .collect();
            Self {
                connections: Mutex::new(connections),
            }
        }
    }

    struct PreEstablishedOrEchoFactory {
        initial: Mutex<VecDeque<(DynReader, DynWriter)>>,
        remote_tasks: Mutex<Vec<JoinHandle<()>>>,
    }

    impl PreEstablishedOrEchoFactory {
        fn new<R, W>(connections: Vec<(R, W)>) -> Self
        where
            R: AsyncRead + Unpin + Send + 'static,
            W: AsyncWrite + Unpin + Send + 'static,
        {
            let initial = connections
                .into_iter()
                .map(|(reader, writer)| {
                    (Box::new(reader) as DynReader, Box::new(writer) as DynWriter)
                })
                .collect();
            Self {
                initial: Mutex::new(initial),
                remote_tasks: Mutex::new(Vec::new()),
            }
        }

        fn abort_all(&self) {
            if let Ok(handles) = self.remote_tasks.lock() {
                for handle in handles.iter() {
                    handle.abort();
                }
            }
        }
    }

    impl ConnectionFactory for PreEstablishedOrEchoFactory {
        type Reader = DynReader;
        type Writer = DynWriter;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
        {
            if let Some(connection) = self
                .initial
                .lock()
                .expect("pre-established echo factory lock poisoned")
                .pop_front()
            {
                return Ok(connection);
            }

            let (reader, writer, remote_task) = make_echo_connection();
            self.remote_tasks
                .lock()
                .expect("echo factory task lock poisoned")
                .push(remote_task);
            Ok((Box::new(reader) as DynReader, Box::new(writer) as DynWriter))
        }
    }

    impl ConnectionFactory for PreEstablishedFactory {
        type Reader = DynReader;
        type Writer = DynWriter;

        async fn establish(
            &self,
            _peer_node_id: &str,
        ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
        {
            self.connections
                .lock()
                .expect("pre-established factory lock poisoned")
                .pop_front()
                .ok_or_else(|| {
                    Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "no more pre-established connections",
                    ))
                })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_peer_manager_starts_without_connections() {
        let config = SharedConnectionConfig::default();
        let factory: Arc<dyn DynConnectionFactory> = Arc::new(EchoConnectionFactory::default());

        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("peer-1".to_string(), config, factory, &rt).unwrap();

        assert_eq!(manager.peer_node_id(), "peer-1");
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn register_and_unregister_dataflow() {
        let config = SharedConnectionConfig::default();
        let factory: Arc<dyn DynConnectionFactory> = Arc::new(EchoConnectionFactory::default());
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("peer-1".to_string(), config, factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let channel_ids = vec![1, 2, 3];
        let (receivers, _error_rx) = manager.register_dataflow(df_id, &channel_ids, 16).await;

        // 3 requested + 1 auto-registered control channel (ID 0)
        assert_eq!(receivers.len(), 4);
        assert!(receivers.contains_key(&CONTROL_CHANNEL_ID));
        assert!(receivers.contains_key(&1));
        assert!(receivers.contains_key(&2));
        assert!(receivers.contains_key(&3));

        // Verify registration exists
        {
            let state = manager.reg_state.lock().await;
            assert!(state.registered.contains_key(&df_id));
        }

        // Unregister
        manager.unregister_dataflow(&df_id).await;
        {
            let state = manager.reg_state.lock().await;
            assert!(!state.registered.contains_key(&df_id));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_transport_session_api() {
        let config = SharedConnectionConfig::default();
        let factory: Arc<dyn DynConnectionFactory> = Arc::new(EchoConnectionFactory::default());
        let rt = tokio::runtime::Handle::current();

        let mut managers = HashMap::new();
        managers.insert(
            "peer-1".to_string(),
            SharedPeerManager::new("peer-1".to_string(), config, factory, &rt).unwrap(),
        );

        let df_id = DataflowId::new();
        let session = SharedTransportSession::new(df_id, &managers, &[1, 2], 16).await;

        // API surface matches TransportSession
        assert!(session.data_sender("peer-1").is_some());
        assert!(session.progress_sender("peer-1").is_some());
        assert!(session.control_sender("peer-1").is_some());
        assert!(session.data_sender("nonexistent").is_none());

        let peers: Vec<_> = session.peer_node_ids().collect();
        assert_eq!(peers, vec!["peer-1"]);
        assert_eq!(session.dataflow_id(), df_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_channel_id_is_reserved() {
        // Probe channel ID should not conflict with normal channels
        assert_eq!(PROBE_CHANNEL_ID, u64::MAX);
        assert_ne!(PROBE_CHANNEL_ID, 0); // not control
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dataframe_sender_tags_with_dataflow_id() {
        let (tx, mut rx) = tokio_mpsc::channel::<(DataflowId, Frame)>(16);
        let df_id = DataflowId::new();
        let sender = DataframeSender {
            dataflow_id: df_id,
            tx,
        };

        let frame = Frame {
            dataflow_id: df_id,
            channel_id: 1,
            payload: vec![1, 2, 3],
        };
        sender.send(frame).await.unwrap();

        let (received_df_id, received_frame) = rx.recv().await.unwrap();
        assert_eq!(received_df_id, df_id);
        assert_eq!(received_frame.channel_id, 1);
        assert_eq!(received_frame.payload, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn end_to_end_send_receive() {
        // Create a duplex pair: manager writes to one end, test reads from other
        let (manager_stream, test_stream) = duplex(65536);
        let (mgr_read, mgr_write) = tokio::io::split(manager_stream);

        let config = SharedConnectionConfig {
            probe_interval: Duration::from_secs(999), // disable auto-probing
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();

        // Manager writes via mgr_write; test reads from test_stream
        let factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(mgr_read, mgr_write)]));
        let manager =
            SharedPeerManager::new("peer-1".to_string(), config.clone(), factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let _receivers = manager.register_dataflow(df_id, &[1], 16).await;

        // Send a frame via the payload channel
        let frame = Frame {
            dataflow_id: df_id,
            channel_id: 1,
            payload: b"hello world".to_vec(),
        };

        manager.payload_sender().send((df_id, frame)).await.unwrap();

        // Read the frame from the test side
        let mut reader = FramedReader::new(test_stream);

        // Give bridge task time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        let received = reader.read_frame().await.unwrap();
        assert_eq!(received.dataflow_id, df_id);
        assert_eq!(received.channel_id, 1);

        // Payload should have 8-byte sequence prefix + original payload
        assert_eq!(received.payload.len(), 8 + 11); // 8 seq + "hello world"
        let seq_id = u64::from_le_bytes(received.payload[..8].try_into().unwrap());
        assert_eq!(seq_id, 0); // first sequence ID
        assert_eq!(&received.payload[8..], b"hello world");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sequence_ids_increment_per_dataflow() {
        let (manager_stream, test_stream) = duplex(65536);
        let (mgr_read, mgr_write) = tokio::io::split(manager_stream);

        let config = SharedConnectionConfig {
            probe_interval: Duration::from_secs(999),
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();

        let factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(mgr_read, mgr_write)]));
        let manager = SharedPeerManager::new("peer-1".to_string(), config, factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let _receivers = manager.register_dataflow(df_id, &[1, 2], 16).await;

        // Send 3 frames on different channels — all should share the same sequence lane
        for ch in [1u64, 2, 1] {
            let frame = Frame {
                dataflow_id: df_id,
                channel_id: ch,
                payload: vec![ch as u8],
            };
            manager.payload_sender().send((df_id, frame)).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut reader = FramedReader::new(test_stream);
        let mut seq_ids = Vec::new();
        for _ in 0..3 {
            let f = reader.read_frame().await.unwrap();
            let seq = u64::from_le_bytes(f.payload[..8].try_into().unwrap());
            seq_ids.push(seq);
        }

        // Sequence IDs should be 0, 1, 2 (monotonically increasing per dataflow)
        assert_eq!(seq_ids, vec![0, 1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dataframe_sender_normalizes_dataflow_id() {
        // DataframeSender should overwrite frame.dataflow_id with its own
        let config = SharedConnectionConfig::default();
        let factory: Arc<dyn DynConnectionFactory> = Arc::new(EchoConnectionFactory::default());
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("test-peer".into(), config, factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let wrong_id = DataflowId::new();
        manager.register_dataflow(df_id, &[1], 16).await;

        let sender = DataframeSender {
            dataflow_id: df_id,
            tx: manager.payload_sender().clone(),
        };

        // Create a frame with the WRONG dataflow_id
        let frame = Frame {
            dataflow_id: wrong_id,
            channel_id: 1,
            payload: b"test".to_vec(),
        };

        sender.send(frame).await.unwrap();
        // If it didn't normalize, the bridge would drop it (unregistered dataflow)
        // Since the test doesn't hang/error, normalization works.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn probe_request_generates_reply() {
        use crate::communication::probing::ProbeMessage;

        // Set up a manager with 1 connection
        let (mgr_stream, test_stream) = duplex(8192);
        let (mgr_read, mgr_write) = tokio::io::split(mgr_stream);
        let (test_read, test_write) = tokio::io::split(test_stream);

        let config = SharedConnectionConfig::default();
        let rt = tokio::runtime::Handle::current();
        let factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(mgr_read, mgr_write)]));
        let manager = SharedPeerManager::new("test-peer".into(), config, factory, &rt).unwrap();

        // Register a dataflow to trigger lazy connection establishment
        let df_id = DataflowId::new();
        let _reg = manager.register_dataflow(df_id, &[1], 16).await;

        // Send a probe request FROM the test side TO the manager's reader
        let probe_req = ProbeMessage::new_request(42, 1000);
        let probe_frame = Frame {
            dataflow_id: DataflowId::nil(),
            channel_id: PROBE_CHANNEL_ID,
            payload: probe_req.encode().to_vec(),
        };

        let mut writer = FramedWriter::new(test_write);
        writer.write_frame(&probe_frame).await.unwrap();

        // The manager's reader_task should generate a reply and send it
        // back through the writer task → test_read
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut reader = FramedReader::new(test_read);
        // Use a timeout to avoid hanging if reply isn't sent
        let result = tokio::time::timeout(Duration::from_secs(2), reader.read_frame()).await;

        match result {
            Ok(Ok(reply_frame)) => {
                assert_eq!(reply_frame.channel_id, PROBE_CHANNEL_ID);
                let reply = ProbeMessage::decode(&reply_frame.payload).unwrap();
                assert_eq!(reply.kind, ProbeKind::Reply);
                assert_eq!(reply.probe_seq, 42);
            }
            Ok(Err(e)) => panic!("Read error: {e:?}"),
            Err(_) => panic!("Timed out waiting for probe reply"),
        }

        drop(manager);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn control_frame_bypasses_sequencing() {
        // Set up manager with 1 connection
        let (mgr_stream, test_stream) = duplex(8192);
        let (mgr_read, mgr_write) = tokio::io::split(mgr_stream);
        let (_test_read, test_write) = tokio::io::split(test_stream);

        let config = SharedConnectionConfig::default();
        let rt = tokio::runtime::Handle::current();
        let factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(mgr_read, mgr_write)]));
        let manager = SharedPeerManager::new("test-peer".into(), config, factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let (mut receivers, _error_rx) = manager.register_dataflow(df_id, &[1], 16).await;
        let mut control_rx = receivers.remove(&CONTROL_CHANNEL_ID).unwrap();

        // Send a control frame from the test side (no sequence prefix!)
        let control_payload = b"shutdown-request".to_vec();
        let control_frame = Frame {
            dataflow_id: df_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: control_payload.clone(),
        };

        let mut writer = FramedWriter::new(test_write);
        writer.write_frame(&control_frame).await.unwrap();

        // The reader should deliver the raw payload without stripping sequence bytes
        let result = tokio::time::timeout(Duration::from_secs(2), control_rx.recv()).await;

        match result {
            Ok(Some(payload)) => {
                // Control payload should arrive unchanged (no 8-byte prefix stripped)
                assert_eq!(payload, control_payload);
            }
            Ok(None) => panic!("Control channel closed unexpectedly"),
            Err(_) => panic!("Timed out waiting for control frame"),
        }

        drop(manager);
    }

    #[tokio::test]
    async fn writer_failure_marks_connection_dead() {
        // Create a connection where we can drop the reader to cause writer failure
        let (client_read, server_write) = duplex(8192);
        let (server_read, client_write) = duplex(8192);

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100), // disable probing
            ..Default::default()
        };

        let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(vec![(
            client_read,
            client_write,
        )]));
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("peer-fail".into(), config, factory, &rt).unwrap();

        // Drop the remote side to cause write failures
        drop(server_read);
        drop(server_write);

        // Give the writer/reader tasks time to detect the failure
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connection should be marked dead
        assert_eq!(
            manager.pool.live_connection_count(),
            0,
            "dead connection should not be counted as live"
        );
    }

    #[tokio::test]
    async fn dead_connection_removed_from_writer_channels() {
        let (client_read, _server_write) = duplex(8192);
        let (_server_read, client_write) = duplex(8192);

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100),
            ..Default::default()
        };

        let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(vec![(
            client_read,
            client_write,
        )]));
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("peer-monitor".into(), config, factory, &rt).unwrap();

        // Drop remote sides
        drop(_server_write);
        drop(_server_read);

        // Wait for monitor to process the failure
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Writer channel should have been removed by the monitor
        let wc = manager.writer_channels.lock().await;
        assert!(
            wc.is_empty(),
            "monitor should remove dead connection's writer channel"
        );
    }

    #[tokio::test]
    async fn all_connections_dead_notifies_dataflows() {
        let (client_read, server_write) = duplex(8192);
        let (server_read, client_write) = duplex(8192);

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100),
            ..Default::default()
        };

        let factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(vec![(
            client_read,
            client_write,
        )]));
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new("peer-notify".into(), config, factory, &rt).unwrap();

        // Register a dataflow
        let df_id = DataflowId::new();
        let (_receivers, mut error_rx) = manager.register_dataflow(df_id, &[1, 2], 16).await;

        // Kill the connection
        drop(server_read);
        drop(server_write);

        // Wait for error notification
        let result = tokio::time::timeout(Duration::from_secs(2), error_rx.recv()).await;

        match result {
            Ok(Some(err)) => {
                assert!(
                    matches!(err, TransportError::ConnectionClosed),
                    "expected ConnectionClosed, got {err:?}"
                );
            }
            Ok(None) => panic!("Error channel closed without sending error"),
            Err(_) => panic!("Timed out waiting for error notification"),
        }
    }

    // =======================================================================
    // Integration tests: multi-dataflow, failure mid-stream, scale-up
    // =======================================================================

    /// Helper: create a bidirectional shared transport (two managers connected).
    /// Returns (manager_a, manager_b) where A's writer connects to B's reader and vice versa.
    fn make_bidirectional_managers(
        num_connections: usize,
        config: SharedConnectionConfig,
        rt: &tokio::runtime::Handle,
    ) -> (SharedPeerManager, SharedPeerManager) {
        let mut a_connections = Vec::new();
        let mut b_connections = Vec::new();

        for _ in 0..num_connections {
            // Each "connection" is a pair of duplex streams
            let (a_to_b, b_from_a) = duplex(65536);
            let (b_to_a, a_from_b) = duplex(65536);

            // Manager A: reads from a_from_b, writes to a_to_b
            a_connections.push((a_from_b, a_to_b));
            // Manager B: reads from b_from_a, writes to b_to_a
            b_connections.push((b_from_a, b_to_a));
        }

        let a_factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(a_connections));
        let b_factory: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(b_connections));
        let manager_a = SharedPeerManager::new(
            "node-b".to_string(), // A's peer is B
            config.clone(),
            a_factory,
            rt,
        )
        .unwrap();
        let manager_b = SharedPeerManager::new(
            "node-a".to_string(), // B's peer is A
            config,
            b_factory,
            rt,
        )
        .unwrap();

        (manager_a, manager_b)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multiple_dataflows_share_connections_ordering_preserved() {
        // 3 dataflows sharing 2 connections between nodes A and B
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 4,
            probe_interval: Duration::from_secs(999),
            reorder_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();
        let (manager_a, manager_b) = make_bidirectional_managers(2, config, &rt);

        // Register 3 dataflows on both sides
        let df1 = DataflowId::new();
        let df2 = DataflowId::new();
        let df3 = DataflowId::new();

        let _reg_a1 = manager_a.register_dataflow(df1, &[1], 64).await;
        let _reg_a2 = manager_a.register_dataflow(df2, &[1], 64).await;
        let _reg_a3 = manager_a.register_dataflow(df3, &[1], 64).await;

        let (mut rx_b1, _) = manager_b.register_dataflow(df1, &[1], 64).await;
        let (mut rx_b2, _) = manager_b.register_dataflow(df2, &[1], 64).await;
        let (mut rx_b3, _) = manager_b.register_dataflow(df3, &[1], 64).await;

        // Send 10 messages per dataflow (interleaved)
        let sender = manager_a.payload_sender().clone();
        for i in 0u32..10 {
            for &df_id in &[df1, df2, df3] {
                let frame = Frame {
                    dataflow_id: df_id,
                    channel_id: 1,
                    payload: i.to_le_bytes().to_vec(),
                };
                sender.send((df_id, frame)).await.unwrap();
            }
        }

        // Verify each dataflow receives its 10 messages in order (no arbitrary sleep;
        // the 2s timeout per recv is sufficient synchronization)
        for (name, rx) in [
            ("df1", &mut rx_b1),
            ("df2", &mut rx_b2),
            ("df3", &mut rx_b3),
        ] {
            let ch_rx = rx.get_mut(&1).unwrap();
            for expected in 0u32..10 {
                let result = tokio::time::timeout(Duration::from_secs(2), ch_rx.recv()).await;
                match result {
                    Ok(Some(payload)) => {
                        let val = u32::from_le_bytes(payload[..4].try_into().unwrap());
                        assert_eq!(val, expected, "{name} frame {expected} out of order");
                    }
                    Ok(None) => panic!("{name}: channel closed at frame {expected}"),
                    Err(_) => panic!("{name}: timed out at frame {expected}"),
                }
            }
        }

        drop(manager_a);
        drop(manager_b);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connection_drop_mid_stream_uses_surviving_connection() {
        // 2 connections: kill one by dropping its remote endpoint, triggering real
        // I/O failure in the writer_task. Then verify subsequent frames arrive via
        // the surviving connection.
        //
        // We keep handles to the remote halves of connection 0 so we can drop them
        // to simulate network failure.
        let (a_to_b_1, b_from_a_1) = duplex(65536);
        let (b_to_a_1, a_from_b_1) = duplex(65536);
        let (a_to_b_2, b_from_a_2) = duplex(65536);
        let (b_to_a_2, a_from_b_2) = duplex(65536);

        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 4,
            probe_interval: Duration::from_secs(999),
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();

        // Manager A: connection 0 uses (a_from_b_1, a_to_b_1), connection 1 uses (a_from_b_2, a_to_b_2)
        let a_factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(vec![
            (a_from_b_1, a_to_b_1),
            (a_from_b_2, a_to_b_2),
        ]));
        let manager_a =
            SharedPeerManager::new("node-b".to_string(), config.clone(), a_factory, &rt).unwrap();

        // Manager B: connection 0 uses (b_from_a_1, b_to_a_1), connection 1 uses (b_from_a_2, b_to_a_2)
        let b_factory: Arc<dyn DynConnectionFactory> = Arc::new(PreEstablishedFactory::new(vec![
            (b_from_a_1, b_to_a_1),
            (b_from_a_2, b_to_a_2),
        ]));
        let manager_b =
            SharedPeerManager::new("node-a".to_string(), config, b_factory, &rt).unwrap();

        let df_id = DataflowId::new();
        let _reg_a = manager_a.register_dataflow(df_id, &[1], 64).await;
        let (mut rx_b, _) = manager_b.register_dataflow(df_id, &[1], 64).await;

        // Send first batch — both connections alive
        let sender = manager_a.payload_sender().clone();
        for i in 0u32..5 {
            let frame = Frame {
                dataflow_id: df_id,
                channel_id: 1,
                payload: i.to_le_bytes().to_vec(),
            };
            sender.send((df_id, frame)).await.unwrap();
        }

        // Receive first batch (timeout-based, no arbitrary sleep)
        let ch_rx = rx_b.get_mut(&1).unwrap();
        let mut received_count = 0;
        for _ in 0..5 {
            if tokio::time::timeout(Duration::from_secs(2), ch_rx.recv())
                .await
                .is_ok()
            {
                received_count += 1;
            }
        }
        assert_eq!(received_count, 5, "expected all 5 frames before failure");

        // Kill connection 0 by marking it dead (simulates what writer_task does on I/O error).
        // In a real scenario, the writer_task would detect write failure and call mark_dead().
        // We also close the writer channel to simulate the full failure path.
        manager_a.pool.connection(0).unwrap().mark_dead();
        {
            let mut wc = manager_a.writer_channels.lock().await;
            wc.remove(&0);
        }

        // Send more frames — bridge should route all to connection 1
        for i in 100u32..110 {
            let frame = Frame {
                dataflow_id: df_id,
                channel_id: 1,
                payload: i.to_le_bytes().to_vec(),
            };
            sender.send((df_id, frame)).await.unwrap();
        }

        // Verify post-failure frames arrive via surviving connection
        let mut post_fail_values = Vec::new();
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_secs(2), ch_rx.recv()).await {
                Ok(Some(payload)) => {
                    let val = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    post_fail_values.push(val);
                }
                _ => break,
            }
        }

        assert_eq!(
            post_fail_values.len(),
            10,
            "expected 10 post-failure frames, got {}",
            post_fail_values.len()
        );
        // Verify ordering preserved on surviving connection
        for (i, &val) in post_fail_values.iter().enumerate() {
            assert_eq!(val, 100 + i as u32, "post-failure frame {i} out of order");
        }

        // Confirm pool state
        assert_eq!(manager_a.pool.live_connection_count(), 1);

        drop(manager_a);
        drop(manager_b);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_connection_is_reconnected() {
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 2,
            probe_interval: Duration::from_secs(999),
            ..Default::default()
        };
        let (reader_a, writer_a, remote_a) = make_echo_connection();
        let (reader_b, writer_b, remote_b) = make_echo_connection();
        let factory = Arc::new(PreEstablishedOrEchoFactory::new(vec![
            (reader_a, writer_a),
            (reader_b, writer_b),
        ]));
        let reconnect_factory: Arc<dyn DynConnectionFactory> = factory.clone();
        let rt = tokio::runtime::Handle::current();
        let manager =
            SharedPeerManager::new("peer-reconnect".into(), config, reconnect_factory, &rt)
                .unwrap();

        let df_id = DataflowId::new();
        let (mut receivers, _error_rx) = manager.register_dataflow(df_id, &[1], 64).await;
        let data_rx = receivers.get_mut(&1).unwrap();

        remote_a.abort();

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let writer_ids: Vec<_> = {
                    let wc = manager.writer_channels.lock().await;
                    wc.keys().copied().collect()
                };
                if writer_ids.len() == 2 && writer_ids.iter().any(|conn_id| *conn_id >= 2) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("timed out waiting for reconnect");

        let frame = Frame {
            dataflow_id: df_id,
            channel_id: 1,
            payload: b"reconnected".to_vec(),
        };
        manager.payload_sender().send((df_id, frame)).await.unwrap();

        let payload = tokio::time::timeout(Duration::from_secs(2), data_rx.recv())
            .await
            .expect("timed out waiting for echoed payload")
            .expect("channel closed unexpectedly");
        assert_eq!(payload, b"reconnected".to_vec());
        assert_eq!(manager.pool.live_connection_count(), 2);

        remote_b.abort();
        factory.abort_all();
        drop(manager);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn high_rtt_triggers_scale_up_event() {
        // Verify that when RTT exceeds threshold, ScaleUp event is generated
        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            rtt_scale_up_threshold: Duration::from_millis(5),
            probe_interval: Duration::from_millis(50),
            ..Default::default()
        };
        let pool = PeerPool::new(1, config.clone()).unwrap();

        // Simulate high RTT
        pool.connection(0)
            .unwrap()
            .record_rtt(Duration::from_millis(10));

        // Evaluate scaling — should recommend scale up
        let decision = pool.evaluate_scaling().await;
        assert_eq!(
            decision,
            ScalingDecision::ScaleUp,
            "high RTT should trigger ScaleUp"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_transport_session_multi_peer() {
        // Test SharedTransportSession with multiple peers
        let config = SharedConnectionConfig {
            probe_interval: Duration::from_secs(999),
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();

        // Create two peer managers (simulating connections to peer-1 and peer-2)
        let (stream_1a, stream_1b) = duplex(65536);
        let (stream_2a, stream_2b) = duplex(65536);

        let factory1: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(stream_1a, stream_1b)]));
        let factory2: Arc<dyn DynConnectionFactory> =
            Arc::new(PreEstablishedFactory::new(vec![(stream_2a, stream_2b)]));
        let mgr1 =
            SharedPeerManager::new("peer-1".to_string(), config.clone(), factory1, &rt).unwrap();
        let mgr2 = SharedPeerManager::new("peer-2".to_string(), config, factory2, &rt).unwrap();

        let mut managers = HashMap::new();
        managers.insert("peer-1".to_string(), mgr1);
        managers.insert("peer-2".to_string(), mgr2);

        let df_id = DataflowId::new();
        let mut session = SharedTransportSession::new(df_id, &managers, &[1, 2], 16).await;

        // Verify API surface
        assert!(session.data_sender("peer-1").is_some());
        assert!(session.data_sender("peer-2").is_some());
        assert!(session.data_sender("peer-3").is_none());
        assert!(session.control_sender("peer-1").is_some());

        let receivers = session.take_receivers().unwrap();
        assert_eq!(receivers.len(), 2); // two peers
        assert!(receivers.contains_key("peer-1"));
        assert!(receivers.contains_key("peer-2"));

        let error_rxs = session.take_error_receivers().unwrap();
        assert_eq!(error_rxs.len(), 2);

        // Second take returns None
        assert!(session.take_receivers().is_none());
        assert!(session.take_error_receivers().is_none());

        drop(session);
        drop(managers);
    }
}
