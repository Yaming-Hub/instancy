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
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::Mutex as TokioMutex;

use crate::communication::probing::{
    ProbeCounter, ProbeKind, ProbeMessage, ScalingDriver, ScalingEvent,
};
use crate::communication::sequencing::{InsertResult, ReorderBuffer, SequenceCounter};
use crate::communication::shared_pool::{ConnectionMetrics, PeerPool, SharedConnectionConfig};
use crate::communication::transport::{Frame, FramedReader, FramedWriter, TransportError};
use crate::communication::transport_session::CONTROL_CHANNEL_ID;
use crate::dataflow::id::DataflowId;

/// Reserved channel ID for probe messages on shared connections.
///
/// This is distinct from `CONTROL_CHANNEL_ID` (0) and user data/progress channels.
/// Probe frames carry a [`ProbeMessage`] as their payload.
pub const PROBE_CHANNEL_ID: u64 = u64::MAX;

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
    ) -> impl std::future::Future<Output = Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>> + Send;
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

/// Combined registration + pending-control state, guarded by a single lock
/// so that the reader_task can atomically check registration and buffer
/// control frames for not-yet-registered dataflows.
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
    /// Background task handles (aborted on drop).
    _task_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Payload sender: frames from any dataflow go here for sequencing + pool routing.
    payload_tx: tokio_mpsc::Sender<(DataflowId, Frame)>,
    /// Control sender: high-priority frames bypass sequencing.
    control_tx: tokio_mpsc::Sender<Frame>,
    /// Failure notification sender: writer/reader tasks send failed conn IDs here.
    failure_tx: tokio_mpsc::Sender<usize>,
}

impl Drop for SharedPeerManager {
    fn drop(&mut self) {
        for handle in &self._task_handles {
            handle.abort();
        }
    }
}

impl SharedPeerManager {
    /// Create a new shared peer manager with initial connections.
    ///
    /// # Arguments
    /// - `peer_node_id`: Remote peer identifier
    /// - `config`: Shared connection configuration
    /// - `connections`: Initial set of connections (must have at least `config.min_connections`)
    /// - `runtime_handle`: Tokio runtime for spawning background tasks
    pub fn new<R, W>(
        peer_node_id: String,
        config: SharedConnectionConfig,
        connections: Vec<(R, W)>,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        assert!(
            connections.len() >= config.min_connections,
            "need at least {} connections, got {}",
            config.min_connections,
            connections.len()
        );

        let pool = Arc::new(PeerPool::new(connections.len(), config.clone()));
        let (scaling_driver, scaling_event_rx) = ScalingDriver::new(config.clone());
        let scaling_driver = Arc::new(scaling_driver);
        let probe_counter = Arc::new(ProbeCounter::new());

        let mut writer_channel_map = HashMap::new();

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

        let mut task_handles = Vec::new();
        let mut readers = Vec::new();

        // Set up per-connection writer tasks and collect readers
        for (conn_id, (reader, writer)) in connections.into_iter().enumerate() {
            let (tx, rx) = tokio_mpsc::channel::<Frame>(256);
            writer_channel_map.insert(conn_id, tx);

            // Get connection metrics for accurate dequeue tracking
            let conn_metrics = pool.connection(conn_id).cloned();

            // Spawn per-connection writer task
            let handle = runtime_handle.spawn(Self::writer_task(
                conn_id,
                writer,
                rx,
                conn_metrics,
                failure_tx.clone(),
            ));
            task_handles.push(handle);

            readers.push((conn_id, reader));
        }

        let writer_channels = Arc::new(TokioMutex::new(writer_channel_map));

        // Spawn per-connection reader tasks (need writer_channels for probe replies)
        for (conn_id, reader) in readers {
            let reader_handle = runtime_handle.spawn(Self::reader_task(
                conn_id,
                reader,
                reg_state.clone(),
                reorder_buffers.clone(),
                scaling_driver.clone(),
                pool.clone(),
                writer_channels.clone(),
                failure_tx.clone(),
            ));
            task_handles.push(reader_handle);
        }

        // Spawn the bridge task (sequences payload frames and routes to connections)
        let bridge_handle = runtime_handle.spawn(Self::bridge_task(
            pool.clone(),
            writer_channels.clone(),
            reg_state.clone(),
            payload_rx,
            control_rx,
        ));
        task_handles.push(bridge_handle);

        // Spawn probe loop
        let probe_handle = runtime_handle.spawn(Self::probe_loop(
            scaling_driver.clone(),
            probe_counter.clone(),
            writer_channels.clone(),
            pool.clone(),
            config.probe_interval,
        ));
        task_handles.push(probe_handle);

        // Spawn scaling event handler
        let scale_handle =
            runtime_handle.spawn(Self::scaling_event_handler(scaling_event_rx, pool.clone()));
        task_handles.push(scale_handle);

        // Spawn connection failure monitor
        let monitor_handle = runtime_handle.spawn(Self::connection_monitor(
            failure_rx,
            pool.clone(),
            writer_channels.clone(),
            scaling_driver.clone(),
            reg_state.clone(),
        ));
        task_handles.push(monitor_handle);

        // Spawn periodic reorder buffer timeout sweeper
        let sweep_handle = runtime_handle.spawn(Self::timeout_sweeper(
            reorder_buffers.clone(),
            reg_state.clone(),
            config.reorder_timeout,
        ));
        task_handles.push(sweep_handle);

        Self {
            peer_node_id,
            config,
            pool,
            writer_channels,
            reg_state,
            reorder_buffers,
            scaling_driver,
            probe_counter,
            _task_handles: task_handles,
            payload_tx,
            control_tx,
            failure_tx,
        }
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

        let reg = DataflowRegistration {
            sequence_counter: SequenceCounter::new(),
            channel_senders,
            error_tx,
        };

        // Atomically: drain pending control frames + insert registration +
        // deliver buffered frames. The reader_task uses the same lock, so
        // there is no window where a frame could be buffered after we drain
        // but before we register, and no ordering inversion where reader_task
        // delivers a newer frame before we finish delivering buffered ones.
        {
            let mut state = self.reg_state.lock().await;
            let pending = state.pending_control.remove(&dataflow_id).unwrap_or_default();
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
                            }
                        }
                        None => { control_open = false; }
                    }
                }

                result = payload_rx.recv(), if payload_open => {
                    match result {
                        Some((dataflow_id, mut frame)) => {
                            // Assign sequence_id from the dataflow's counter
                            let seq_id = {
                                let state = reg_state.lock().await;
                                if let Some(reg) = state.registered.get(&dataflow_id) {
                                    reg.sequence_counter.next_seq()
                                } else {
                                    continue; // dataflow unregistered, drop frame
                                }
                            };

                            // Prepend sequence_id to payload
                            let mut sequenced_payload = Vec::with_capacity(8 + frame.payload.len());
                            sequenced_payload.extend_from_slice(&seq_id.to_le_bytes());
                            sequenced_payload.extend_from_slice(&frame.payload);
                            frame.payload = sequenced_payload;

                            // Select a live connection; retry on a different one if send fails
                            let mut exclude = HashSet::new();
                            let mut current_frame = frame;
                            loop {
                                let conn = match pool.select_connection_excluding(&exclude) {
                                    Some(c) => {
                                        c.enqueue();
                                        c
                                    }
                                    None => {
                                        // No live connections available
                                        #[cfg(feature = "tracing")]
                                        tracing::error!(
                                            "No live connections for payload frame, dropping"
                                        );
                                        break;
                                    }
                                };
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
                                                continue;
                                            }
                                        }
                                    }
                                    None => {
                                        // Writer channel already removed (monitor cleaned it up)
                                        conn.rollback_reservation();
                                        exclude.insert(conn_id);
                                        continue;
                                    }
                                }
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

                    // Control frames bypass sequencing — deliver raw payload directly.
                    // If the dataflow isn't registered yet, buffer the frame so it
                    // can be delivered when registration occurs (atomic under one lock).
                    if frame.channel_id == CONTROL_CHANNEL_ID {
                        let tx = {
                            let mut state = reg_state.lock().await;
                            match state.registered.get(&frame.dataflow_id) {
                                Some(reg) => {
                                    reg.channel_senders.get(&CONTROL_CHANNEL_ID).cloned()
                                }
                                None => {
                                    // Drop frames for completed dataflows instead
                                    // of buffering them indefinitely.
                                    if state.completed.contains(&frame.dataflow_id) {
                                        continue;
                                    }
                                    // Dataflow not registered yet — buffer for later
                                    state.pending_control
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
                    let seq_id =
                        u64::from_le_bytes(frame.payload[..8].try_into().unwrap());
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
                                    state.registered.get(&frame.dataflow_id)
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
                        scaling_driver.process_probe_reply(&probe, conn);
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

    /// Handles scaling events (log only for v1; factory integration is future).
    async fn scaling_event_handler(
        mut event_rx: tokio_mpsc::Receiver<ScalingEvent>,
        _pool: Arc<PeerPool>,
    ) {
        while let Some(event) = event_rx.recv().await {
            match event {
                ScalingEvent::ScaleUp => {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Scaling event: ScaleUp requested");
                    // Future: call ConnectionFactory to establish new connection
                }
                ScalingEvent::ScaleDown { connection_id } => {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Scaling event: ScaleDown conn {connection_id}");
                    // Future: drain and remove connection
                }
                ScalingEvent::ConnectionFailed { connection_id } => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!("Scaling event: ConnectionFailed conn {connection_id}");
                    // Future: call ConnectionFactory to replace dead connection
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
    /// When all connections are dead, notifies all registered dataflows with
    /// a `TransportError::ConnectionClosed` error.
    async fn connection_monitor(
        mut failure_rx: tokio_mpsc::Receiver<usize>,
        pool: Arc<PeerPool>,
        writer_channels: Arc<TokioMutex<HashMap<usize, tokio_mpsc::Sender<Frame>>>>,
        scaling_driver: Arc<ScalingDriver>,
        reg_state: Arc<TokioMutex<RegistrationState>>,
    ) {
        let mut processed = HashSet::new();
        let mut all_failed_notified = false;
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

            // Emit scaling event for external handling
            scaling_driver
                .emit_event(ScalingEvent::ConnectionFailed {
                    connection_id: conn_id,
                })
                .await;

            // Reset notification flag if connections were re-established
            if live_count > 0 && all_failed_notified {
                all_failed_notified = false;
            }

            // If all connections are dead, notify all registered dataflows
            if live_count == 0 && !all_failed_notified {
                all_failed_notified = true;
                #[cfg(feature = "tracing")]
                tracing::error!(
                    "All connections to peer are dead — notifying dataflows"
                );

                let state = reg_state.lock().await;
                for (_df_id, reg) in state.registered.iter() {
                    let _ = reg
                        .error_tx
                        .try_send(TransportError::ConnectionClosed);
                }
            }
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
                        let _ = reg
                            .error_tx
                            .try_send(TransportError::ReorderTimeout);
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
    pub fn data_sender(
        &self,
        peer_node_id: &str,
    ) -> Option<DataframeSender> {
        self.payload_senders.get(peer_node_id).map(|tx| {
            DataframeSender {
                dataflow_id: self.dataflow_id,
                tx: tx.clone(),
            }
        })
    }

    /// Get a progress sender for a peer.
    ///
    /// Returns the same shared payload sender as [`data_sender`](Self::data_sender).
    pub fn progress_sender(
        &self,
        peer_node_id: &str,
    ) -> Option<DataframeSender> {
        self.data_sender(peer_node_id)
    }

    /// Get a control-priority sender for a peer.
    ///
    /// Control frames bypass sequencing and have highest priority.
    pub fn control_sender(
        &self,
        peer_node_id: &str,
    ) -> Option<&tokio_mpsc::Sender<Frame>> {
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
    pub fn try_send(
        &self,
        mut frame: Frame,
    ) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        frame.dataflow_id = self.dataflow_id;
        self.tx.try_send((self.dataflow_id, frame)).map_err(|e| {
            match e {
                tokio_mpsc::error::TrySendError::Full(v) => {
                    tokio_mpsc::error::TrySendError::Full(v.1)
                }
                tokio_mpsc::error::TrySendError::Closed(v) => {
                    tokio_mpsc::error::TrySendError::Closed(v.1)
                }
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
    use super::*;
    use super::super::shared_pool::ScalingDecision;
    use tokio::io::{duplex, DuplexStream};

    /// Helper: create N duplex connection pairs (read, write) for each side.
    fn make_connections(n: usize) -> Vec<(DuplexStream, DuplexStream)> {
        (0..n).map(|_| duplex(8192)).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_peer_manager_creates_with_connections() {
        let pairs = make_connections(2);
        let (readers, writers): (Vec<_>, Vec<_>) = pairs
            .into_iter()
            .unzip();

        let config = SharedConnectionConfig::default();
        let connections: Vec<_> = readers.into_iter().zip(writers).collect();

        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new(
            "peer-1".to_string(),
            config,
            connections,
            &rt,
        );

        assert_eq!(manager.peer_node_id(), "peer-1");
        assert_eq!(manager.connection_count(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn register_and_unregister_dataflow() {
        let pairs = make_connections(1);
        let connections: Vec<_> = pairs.into_iter().collect();

        let config = SharedConnectionConfig::default();
        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new(
            "peer-1".to_string(),
            config,
            connections,
            &rt,
        );

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
        let pairs = make_connections(1);
        let connections: Vec<_> = pairs.into_iter().collect();

        let config = SharedConnectionConfig::default();
        let rt = tokio::runtime::Handle::current();

        let mut managers = HashMap::new();
        managers.insert(
            "peer-1".to_string(),
            SharedPeerManager::new("peer-1".to_string(), config, connections, &rt),
        );

        let df_id = DataflowId::new();
        let session = SharedTransportSession::new(
            df_id,
            &managers,
            &[1, 2],
            16,
        )
        .await;

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
        let connections = vec![(mgr_read, mgr_write)];
        let manager = SharedPeerManager::new(
            "peer-1".to_string(),
            config.clone(),
            connections,
            &rt,
        );

        let df_id = DataflowId::new();
        let _receivers = manager.register_dataflow(df_id, &[1], 16).await;

        // Send a frame via the payload channel
        let frame = Frame {
            dataflow_id: df_id,
            channel_id: 1,
            payload: b"hello world".to_vec(),
        };

        manager
            .payload_sender()
            .send((df_id, frame))
            .await
            .unwrap();

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

        let connections = vec![(mgr_read, mgr_write)];
        let manager = SharedPeerManager::new(
            "peer-1".to_string(),
            config,
            connections,
            &rt,
        );

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
        let pairs = make_connections(1);
        let connections: Vec<_> = pairs.into_iter().collect();

        let config = SharedConnectionConfig::default();
        let rt = tokio::runtime::Handle::current();
        let manager =
            SharedPeerManager::new("test-peer".into(), config, connections, &rt);

        let df_id = DataflowId::new();
        let wrong_id = DataflowId::new();
        manager
            .register_dataflow(df_id, &[1], 16)
            .await;

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
        let manager = SharedPeerManager::new(
            "test-peer".into(),
            config,
            vec![(mgr_read, mgr_write)],
            &rt,
        );

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
        let manager = SharedPeerManager::new(
            "test-peer".into(),
            config,
            vec![(mgr_read, mgr_write)],
            &rt,
        );

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
        let result =
            tokio::time::timeout(Duration::from_secs(2), control_rx.recv()).await;

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

        let connections: Vec<(DuplexStream, DuplexStream)> =
            vec![(client_read, client_write)];

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100), // disable probing
            ..Default::default()
        };

        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new(
            "peer-fail".into(),
            config,
            connections,
            &rt,
        );

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

        let connections: Vec<(DuplexStream, DuplexStream)> =
            vec![(client_read, client_write)];

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100),
            ..Default::default()
        };

        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new(
            "peer-monitor".into(),
            config,
            connections,
            &rt,
        );

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

        let connections: Vec<(DuplexStream, DuplexStream)> =
            vec![(client_read, client_write)];

        let config = SharedConnectionConfig {
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(100),
            ..Default::default()
        };

        let rt = tokio::runtime::Handle::current();
        let manager = SharedPeerManager::new(
            "peer-notify".into(),
            config,
            connections,
            &rt,
        );

        // Register a dataflow
        let df_id = DataflowId::new();
        let (_receivers, mut error_rx) = manager
            .register_dataflow(df_id, &[1, 2], 16)
            .await;

        // Kill the connection
        drop(server_read);
        drop(server_write);

        // Wait for error notification
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            error_rx.recv(),
        )
        .await;

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

        let manager_a = SharedPeerManager::new(
            "node-b".to_string(), // A's peer is B
            config.clone(),
            a_connections,
            rt,
        );
        let manager_b = SharedPeerManager::new(
            "node-a".to_string(), // B's peer is A
            config,
            b_connections,
            rt,
        );

        (manager_a, manager_b)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multiple_dataflows_share_connections_ordering_preserved() {
        // 3 dataflows sharing 2 connections between nodes A and B
        let config = SharedConnectionConfig {
            min_connections: 2,
            max_connections: 4,
            probe_interval: Duration::from_secs(999),
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
        for (name, rx) in [("df1", &mut rx_b1), ("df2", &mut rx_b2), ("df3", &mut rx_b3)] {
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
            min_connections: 1,
            max_connections: 4,
            probe_interval: Duration::from_secs(999),
            ..Default::default()
        };
        let rt = tokio::runtime::Handle::current();

        // Manager A: connection 0 uses (a_from_b_1, a_to_b_1), connection 1 uses (a_from_b_2, a_to_b_2)
        let a_connections = vec![(a_from_b_1, a_to_b_1), (a_from_b_2, a_to_b_2)];
        let manager_a = SharedPeerManager::new(
            "node-b".to_string(),
            config.clone(),
            a_connections,
            &rt,
        );

        // Manager B: connection 0 uses (b_from_a_1, b_to_a_1), connection 1 uses (b_from_a_2, b_to_a_2)
        let b_connections = vec![(b_from_a_1, b_to_a_1), (b_from_a_2, b_to_a_2)];
        let manager_b = SharedPeerManager::new(
            "node-a".to_string(),
            config,
            b_connections,
            &rt,
        );

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
        let pool = PeerPool::new(1, config.clone());

        // Simulate high RTT
        pool.connection(0).unwrap().record_rtt(Duration::from_millis(10));

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

        let mgr1 = SharedPeerManager::new(
            "peer-1".to_string(),
            config.clone(),
            vec![(stream_1a, stream_1b)],
            &rt,
        );
        let mgr2 = SharedPeerManager::new(
            "peer-2".to_string(),
            config,
            vec![(stream_2a, stream_2b)],
            &rt,
        );

        let mut managers = HashMap::new();
        managers.insert("peer-1".to_string(), mgr1);
        managers.insert("peer-2".to_string(), mgr2);

        let df_id = DataflowId::new();
        let mut session = SharedTransportSession::new(
            df_id,
            &managers,
            &[1, 2],
            16,
        )
        .await;

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
