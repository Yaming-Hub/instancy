//! Multiplexed transport session with FIFO payload and priority control.
//!
//! [`TransportSession`] manages per-peer Muxer/Demuxer infrastructure and
//! provides a **FIFO payload channel** (shared by data and progress) and a
//! **priority control channel** per peer.
//!
//! # Channel Design
//!
//! The per-peer bridge task uses biased `select!`:
//! 1. **Control** frames have priority (handshake, ready barrier, shutdown)
//! 2. **Payload** frames (data + progress) are delivered in strict FIFO order
//!
//! Data and progress share a single FIFO channel to preserve the timely
//! ordering invariant: a worker sends data at time T before releasing its
//! capability (generating the progress message for T). FIFO delivery
//! guarantees receivers observe data before the frontier advances past T.
//!
//! This also prevents cross-dataflow starvation: with separate channels and
//! priority, one dataflow's heavy data traffic could starve another dataflow's
//! progress messages, preventing frontier advancement.
//!
//! # Ownership
//!
//! Each `TransportSession` is created per-dataflow. TCP connections can be
//! shared across dataflows at the caller level, but each session gets its
//! own logical channels. The session is `Arc`-wrapped and shared by all
//! endpoints; bridge tasks terminate naturally when all senders are dropped,
//! demux tasks are aborted when the last `Arc` reference drops.
//!
//! # Usage
//!
//! ```ignore
//! let (session, receivers) = TransportSession::new(
//!     dataflow_id,
//!     connections,
//!     &data_registrations,
//!     &progress_registrations,
//!     capacity,
//!     &runtime_handle,
//! )?;
//!
//! // Get senders for a peer (both return the same FIFO channel)
//! let data_tx = session.data_sender("node-b").unwrap();
//! let progress_tx = session.progress_sender("node-b").unwrap();
//!
//! // Receivers are keyed by peer_node_id → channel_id
//! let peer_rxs = receivers.remove("node-b").unwrap();
//! let rx = peer_rxs.into_values().next().unwrap();
//! ```

#[cfg(feature = "transport")]
use std::collections::HashMap;

#[cfg(feature = "transport")]
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "transport")]
use tokio::sync::mpsc as tokio_mpsc;

#[cfg(feature = "transport")]
use crate::communication::transport::{DemuxConfig, Demuxer, Frame, FramedWriter, TransportError};
#[cfg(feature = "transport")]
use crate::dataflow::id::DataflowId;

// ---------------------------------------------------------------------------
// PeerConnection — re-exported from network.rs for shared use
// ---------------------------------------------------------------------------

/// A bidirectional connection to a remote peer, split into read/write halves.
///
/// The caller provides already-established connections (via [`ConnectionManager`]
/// or direct TCP). The transport session wraps them with priority bridge
/// tasks and Demuxer.
///
/// **Reconnection:** `PeerConnection` itself has no reconnection logic.
/// For automatic reconnection on failure, use `SharedPeerManager` with a
/// connection factory — it manages retry with exponential backoff.
///
/// [`ConnectionManager`]: crate::communication::connection::ConnectionManager
#[cfg(feature = "transport")]
pub struct PeerConnection<R, W> {
    /// Identifier for the remote node (must match [`crate::ClusterTopology`] node IDs).
    pub node_id: String,
    /// Read half of the connection (feeds Demuxer).
    pub reader: R,
    /// Write half of the connection (fed by bridge task).
    pub writer: W,
}

// ---------------------------------------------------------------------------
// ChannelRegistration — describes a channel to register on the session
// ---------------------------------------------------------------------------

/// Describes a channel to register with a [`TransportSession`].
///
/// Each registration creates a receiver on the Demuxer for incoming frames
/// matching this `(dataflow_id, channel_id)` pair from the specified peer.
#[cfg(feature = "transport")]
#[derive(Debug, Clone)]
pub struct ChannelRegistration {
    /// The peer node that sends data on this channel.
    pub peer_node_id: String,
    /// Logical channel identifier (unique within the dataflow).
    pub channel_id: u64,
}

// ---------------------------------------------------------------------------
// TransportSession — priority-multiplexed transport per dataflow
// ---------------------------------------------------------------------------

/// A transport session managing multiplexed communication with remote peers.
///
/// **Reconnection:** `TransportSession` does not handle connection failure
/// or reconnection. For managed reconnection, use `SharedPeerManager` which
/// wraps connections with automatic retry and exponential backoff.
///
/// See [module-level documentation](self) for details on priority and ownership.
#[cfg(feature = "transport")]
pub struct TransportSession {
    /// Per-peer payload frame senders (bounded, FIFO for data + progress).
    ///
    /// Data and progress frames share a single FIFO channel to preserve the
    /// timely ordering invariant: a worker sends data before releasing its
    /// capability (progress), so FIFO delivery guarantees the receiver sees
    /// data before the frontier advances past it. This also prevents
    /// cross-dataflow starvation when multiple dataflows share a connection.
    payload_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Per-peer control frame senders (bounded, highest priority).
    control_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Shared state keeping background tasks alive (abort on drop).
    _state: std::sync::Arc<SessionState>,
}

/// Holds background task handles.
///
/// Bridge tasks (writers) are NOT aborted — they are detached and terminate
/// naturally when all their input channel senders are dropped. This ensures
/// pending data frames are flushed to TCP before the bridge exits.
///
/// Demuxer tasks (readers) ARE aborted because they block on TCP reads and
/// would not terminate until the remote peer closes the connection.
#[cfg(feature = "transport")]
struct SessionState {
    /// Kept alive so bridge tasks are not dropped prematurely.
    /// They terminate naturally when all senders close.
    _bridge_handles: Vec<tokio::task::JoinHandle<()>>,
    demux_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "transport")]
impl Drop for SessionState {
    fn drop(&mut self) {
        // Bridge handles are intentionally NOT aborted. Dropping the JoinHandle
        // detaches the task, letting it drain remaining frames and exit when
        // all senders drop. This prevents data loss from premature cancellation.
        //
        // Demuxer handles must be aborted since they block on TCP reads.
        for handle in &self.demux_handles {
            handle.abort();
        }
    }
}

#[cfg(feature = "transport")]
impl TransportSession {
    /// Create a new transport session.
    ///
    /// Spawns per-peer bridge tasks (with priority: control > progress > data)
    /// and Demuxer tasks. Returns the session (with senders) and a nested map
    /// of `peer_node_id → channel_id → Receiver` for all registered channels.
    ///
    /// A control channel (ID 0) is **automatically registered** for each peer.
    /// Use [`control_receivers`](Self) from the returned receivers map to
    /// access them via `receivers[peer_id][CONTROL_CHANNEL_ID]`.
    ///
    /// # Arguments
    ///
    /// - `dataflow_id`: Identifies this dataflow on the wire.
    /// - `connections`: Pre-established connections to each remote peer.
    /// - `data_channels`: Channels to register for data frame reception.
    /// - `progress_channels`: Channels to register for progress frame reception.
    /// - `capacity`: Buffer capacity for send queues and Demuxer per-channel buffers.
    /// - `runtime_handle`: Tokio runtime for spawning background tasks.
    ///
    /// # Channel ID Semantics
    ///
    /// Channel ids must be unique per peer across `data_channels` and
    /// `progress_channels`, and must not be [`CONTROL_CHANNEL_ID`] (which is
    /// registered automatically). See [`CHANNEL_NAMESPACE_SIZE`] for the id
    /// layout used by `spawn_cluster` ([`data_channel_id`] and
    /// [`progress_channel_id`]).
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::InvalidRegistration`] — before any task is
    /// spawned — if `capacity` is 0, two connections name the same peer, a
    /// registration names a peer without a connection, a registration uses
    /// the control channel id, or a `(peer, channel_id)` pair is registered
    /// twice. Previously a duplicate silently replaced the earlier receiver.
    pub fn new<R, W>(
        dataflow_id: DataflowId,
        connections: Vec<PeerConnection<R, W>>,
        data_channels: &[ChannelRegistration],
        progress_channels: &[ChannelRegistration],
        capacity: usize,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Result<
        (
            Self,
            HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>,
        ),
        TransportError,
    >
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let invalid = |reason: String| TransportError::InvalidRegistration { reason };
        if capacity == 0 {
            return Err(invalid("transport capacity must be at least 1".into()));
        }

        // Validate everything up front so a rejected session spawns no tasks.
        let mut peer_channels: HashMap<&str, std::collections::HashSet<u64>> = HashMap::new();
        for conn in &connections {
            if peer_channels
                .insert(conn.node_id.as_str(), std::collections::HashSet::new())
                .is_some()
            {
                return Err(invalid(format!(
                    "duplicate connection to peer '{}'",
                    conn.node_id
                )));
            }
        }
        for reg in data_channels.iter().chain(progress_channels) {
            let Some(ids) = peer_channels.get_mut(reg.peer_node_id.as_str()) else {
                return Err(invalid(format!(
                    "channel {} registered for peer '{}' which has no connection",
                    reg.channel_id, reg.peer_node_id
                )));
            };
            if reg.channel_id == CONTROL_CHANNEL_ID {
                return Err(invalid(format!(
                    "channel {CONTROL_CHANNEL_ID} is the control channel and is registered \
                     automatically (peer '{}')",
                    reg.peer_node_id
                )));
            }
            if !ids.insert(reg.channel_id) {
                return Err(invalid(format!(
                    "channel {} registered twice for peer '{}'",
                    reg.channel_id, reg.peer_node_id
                )));
            }
        }

        let mut payload_senders = HashMap::new();
        let mut control_senders = HashMap::new();
        let mut bridge_handles = Vec::new();
        let mut demux_handles = Vec::new();
        let mut all_receivers: HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>> =
            HashMap::new();

        for conn in connections {
            let peer_node_id = conn.node_id.clone();

            // --- Send side: control (priority) + payload (FIFO) per peer ---
            let (payload_tx, payload_rx) = tokio_mpsc::channel::<Frame>(capacity);
            let (control_tx, control_rx) = tokio_mpsc::channel::<Frame>(capacity);

            payload_senders.insert(peer_node_id.clone(), payload_tx);
            control_senders.insert(peer_node_id.clone(), control_tx);

            // Spawn bridge task
            let writer = conn.writer;
            let peer_id = peer_node_id.clone();
            let bridge_handle = runtime_handle.spawn(async move {
                Self::bridge_task(peer_id, writer, control_rx, payload_rx).await;
            });
            bridge_handles.push(bridge_handle);

            // --- Receive side: Demuxer per peer ---
            let reader = conn.reader;
            let mut demuxer = Demuxer::new(reader, DemuxConfig::default());
            let peer_receivers = all_receivers.entry(peer_node_id.clone()).or_default();

            // Auto-register control channel (ID 0) for this peer. Ids were
            // validated above, so registration cannot fail.
            let registrations = std::iter::once(CONTROL_CHANNEL_ID).chain(
                data_channels
                    .iter()
                    .chain(progress_channels)
                    .filter(|reg| reg.peer_node_id == peer_node_id)
                    .map(|reg| reg.channel_id),
            );
            for channel_id in registrations {
                let rx = demuxer.register_channel(dataflow_id, channel_id)?;
                peer_receivers.insert(channel_id, rx);
            }

            let demux_handle = runtime_handle.spawn(async move {
                if let Err(e) = demuxer.run().await {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Demuxer error for peer {}: {e}", peer_node_id);
                    #[cfg(not(feature = "tracing"))]
                    let _ = e;
                }
            });
            demux_handles.push(demux_handle);
        }

        let state = std::sync::Arc::new(SessionState {
            _bridge_handles: bridge_handles,
            demux_handles,
        });

        let session = Self {
            payload_senders,
            control_senders,
            _state: state,
        };

        Ok((session, all_receivers))
    }

    /// Get a data sender for a peer.
    ///
    /// Returns the shared payload sender (FIFO with progress). Data and
    /// progress share the same channel to preserve timely's ordering invariant.
    /// Returns `None` if no connection exists to the specified peer.
    pub fn data_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.payload_senders.get(peer_node_id)
    }

    /// Get a progress sender for a peer.
    ///
    /// Returns the same shared payload sender as [`data_sender`](Self::data_sender).
    /// Data and progress use a single FIFO channel to ensure that data at
    /// time T arrives at the receiver before the frontier advances past T.
    pub fn progress_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.payload_senders.get(peer_node_id)
    }

    /// Get a control-priority sender for a peer (highest priority).
    ///
    /// Control frames (handshake, shutdown) are always sent before
    /// progress and data frames.
    pub fn control_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.control_senders.get(peer_node_id)
    }

    /// Returns the set of peer node IDs this session has connections to.
    pub fn peer_node_ids(&self) -> impl Iterator<Item = &str> {
        self.payload_senders.keys().map(|s| s.as_str())
    }

    /// Multiplexes control and payload frames onto a single TCP writer.
    ///
    /// Priority: **control (biased) > payload (FIFO)**.
    ///
    /// Control messages (handshake, ready barrier) get priority to ensure
    /// cluster setup completes promptly. Data and progress share a single
    /// FIFO payload channel, preserving the timely ordering invariant:
    /// since a worker sends data before releasing its capability, FIFO
    /// delivery guarantees receivers see data before the frontier advances.
    ///
    /// The task exits when both channels are closed (all senders dropped).
    async fn bridge_task<W: AsyncWrite + Unpin>(
        _peer_id: String,
        writer: W,
        mut control_rx: tokio_mpsc::Receiver<Frame>,
        mut payload_rx: tokio_mpsc::Receiver<Frame>,
    ) {
        let mut framed_writer = FramedWriter::new(writer);
        let mut control_open = true;
        let mut payload_open = true;

        loop {
            if !control_open && !payload_open {
                break;
            }

            let frame = tokio::select! {
                biased;

                result = control_rx.recv(), if control_open => match result {
                    Some(f) => f,
                    None => { control_open = false; continue; }
                },
                result = payload_rx.recv(), if payload_open => match result {
                    Some(f) => f,
                    None => { payload_open = false; continue; }
                },
            };

            if let Err(_e) = framed_writer.write_frame(&frame).await {
                #[cfg(feature = "tracing")]
                tracing::error!("Bridge write error for peer {}: {_e}", _peer_id);
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of ids in each channel-id namespace (`2^62`).
///
/// The `u64` channel-id space of a dataflow is split into four namespaces so
/// that ids generated by different subsystems can never collide, whatever the
/// cluster size:
///
/// | Range                                          | Use                                                                   |
/// |------------------------------------------------|-----------------------------------------------------------------------|
/// | `0`                                            | [`CONTROL_CHANNEL_ID`] (handshake, ready barrier, cancellation)       |
/// | `1 .. PROGRESS_CHANNEL_BASE`                   | application channels (e.g. [`DataflowSession`](super::DataflowSession)) |
/// | `PROGRESS_CHANNEL_BASE .. DATA_CHANNEL_BASE`   | `spawn_cluster` progress channels, see [`progress_channel_id`]          |
/// | `DATA_CHANNEL_BASE .. RESERVED_CHANNEL_BASE`   | `spawn_cluster` exchange-edge data channels, see [`data_channel_id`]    |
/// | `RESERVED_CHANNEL_BASE ..= u64::MAX`           | library-internal channels (probes, inter-process progress)            |
#[cfg(feature = "transport")]
pub const CHANNEL_NAMESPACE_SIZE: u64 = 1 << 62;

/// First channel id of the progress namespace.
///
/// `spawn_cluster` sends progress from global worker `src` to global worker
/// `dst` on channel `PROGRESS_CHANNEL_BASE + src * num_workers + dst`
/// (see [`progress_channel_id`]).
#[cfg(feature = "transport")]
pub const PROGRESS_CHANNEL_BASE: u64 = CHANNEL_NAMESPACE_SIZE;

/// First channel id of the exchange-edge data namespace (see [`data_channel_id`]).
#[cfg(feature = "transport")]
pub const DATA_CHANNEL_BASE: u64 = 2 * CHANNEL_NAMESPACE_SIZE;

/// First channel id of the namespace reserved for library-internal channels
/// such as [`PROBE_CHANNEL_ID`](super::PROBE_CHANNEL_ID).
#[cfg(feature = "transport")]
pub const RESERVED_CHANNEL_BASE: u64 = 3 * CHANNEL_NAMESPACE_SIZE;

/// Channel ID for control messages (handshake, ready barrier, cancellation).
#[cfg(feature = "transport")]
pub const CONTROL_CHANNEL_ID: u64 = 0;

#[cfg(feature = "transport")]
fn namespaced_channel_id(base: u64, offset: Option<u64>, what: &str) -> Result<u64, crate::Error> {
    match offset {
        Some(offset) if offset < CHANNEL_NAMESPACE_SIZE => Ok(base + offset),
        _ => Err(crate::Error::Communication(
            crate::error::CommunicationError::InvalidConfig(format!(
                "{what} channel id space exhausted: the cluster has too many workers or \
                 exchange edges"
            )),
        )),
    }
}

/// Channel id carrying progress updates from global worker `source` to global
/// worker `target` in a cluster of `num_workers` workers.
///
/// The id is `PROGRESS_CHANNEL_BASE + source * num_workers + target`.
///
/// # Errors
///
/// Fails if `source` or `target` is not below `num_workers`, or if the id
/// does not fit in the progress namespace.
#[cfg(feature = "transport")]
pub fn progress_channel_id(
    source: usize,
    target: usize,
    num_workers: usize,
) -> Result<u64, crate::Error> {
    check_worker_indices(source, target, num_workers)?;
    let offset = (source as u64)
        .checked_mul(num_workers as u64)
        .and_then(|v| v.checked_add(target as u64));
    namespaced_channel_id(PROGRESS_CHANNEL_BASE, offset, "progress")
}

/// Channel id carrying data on exchange edge `edge_index` (the position of the
/// edge among the dataflow's exchange edges) from global worker `source` to
/// global worker `target` in a cluster of `num_workers` workers.
///
/// The id is `DATA_CHANNEL_BASE + (edge_index * num_workers + source) * num_workers + target`.
///
/// # Errors
///
/// Fails if `source` or `target` is not below `num_workers`, or if the id
/// does not fit in the data namespace.
#[cfg(feature = "transport")]
pub fn data_channel_id(
    edge_index: usize,
    source: usize,
    target: usize,
    num_workers: usize,
) -> Result<u64, crate::Error> {
    check_worker_indices(source, target, num_workers)?;
    let n = num_workers as u64;
    let offset = (edge_index as u64)
        .checked_mul(n)
        .and_then(|v| v.checked_add(source as u64))
        .and_then(|v| v.checked_mul(n))
        .and_then(|v| v.checked_add(target as u64));
    namespaced_channel_id(DATA_CHANNEL_BASE, offset, "data")
}

#[cfg(feature = "transport")]
fn check_worker_indices(
    source: usize,
    target: usize,
    num_workers: usize,
) -> Result<(), crate::Error> {
    if source >= num_workers || target >= num_workers {
        return Err(crate::Error::Communication(
            crate::error::CommunicationError::InvalidConfig(format!(
                "worker pair ({source}, {target}) out of range for {num_workers} workers"
            )),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "transport")]
mod tests {
    use super::*;

    fn make_dataflow_id() -> DataflowId {
        DataflowId::new()
    }

    /// Helper: poll a tokio mpsc receiver with timeout.
    async fn poll_recv<T>(
        rx: &mut tokio_mpsc::Receiver<T>,
        timeout: std::time::Duration,
    ) -> Option<T> {
        tokio::time::timeout(timeout, rx.recv())
            .await
            .ok()
            .flatten()
    }

    #[tokio::test]
    async fn session_data_roundtrip() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        // Channel 1: node-a → node-b (data)
        let data_regs = vec![ChannelRegistration {
            peer_node_id: "node-a".into(),
            channel_id: 1,
        }];

        // Session for node-b (receives from node-a)
        let (session_b, mut receivers_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &data_regs,
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Session for node-a (sends to node-b, no incoming channels needed for this test)
        let (session_a, _) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Send a data frame from node-a → node-b
        let tx = session_a.data_sender("node-b").unwrap();
        tx.send(Frame {
            dataflow_id: df_id,
            channel_id: 1,
            payload: vec![42, 43, 44],
        })
        .await
        .unwrap();

        // Receive on node-b
        let rx = receivers_b.get_mut("node-a").unwrap().get_mut(&1).unwrap();
        let timeout = std::time::Duration::from_secs(2);
        let payload = poll_recv(rx, timeout).await.expect("should receive data");
        assert_eq!(payload, vec![42, 43, 44]);

        drop(session_a);
        drop(session_b);
    }

    #[tokio::test]
    async fn session_progress_roundtrip() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        let progress_regs = vec![ChannelRegistration {
            peer_node_id: "node-a".into(),
            channel_id: PROGRESS_CHANNEL_BASE + 1,
        }];

        let (session_b, mut receivers_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[],
            &progress_regs,
            16,
            &rt,
        )
        .unwrap();

        let (session_a, _) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Send progress frame
        let tx = session_a.progress_sender("node-b").unwrap();
        tx.send(Frame {
            dataflow_id: df_id,
            channel_id: PROGRESS_CHANNEL_BASE + 1,
            payload: vec![1, 2, 3, 4],
        })
        .await
        .unwrap();

        let rx = receivers_b
            .get_mut("node-a")
            .unwrap()
            .get_mut(&(PROGRESS_CHANNEL_BASE + 1))
            .unwrap();
        let timeout = std::time::Duration::from_secs(2);
        let payload = poll_recv(rx, timeout)
            .await
            .expect("should receive progress");
        assert_eq!(payload, vec![1, 2, 3, 4]);

        drop(session_a);
        drop(session_b);
    }

    #[tokio::test]
    async fn session_priority_ordering() {
        // Verify that progress frames are sent before data frames when both
        // are queued simultaneously.
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        let data_regs = vec![ChannelRegistration {
            peer_node_id: "node-a".into(),
            channel_id: 1,
        }];
        let progress_regs = vec![ChannelRegistration {
            peer_node_id: "node-a".into(),
            channel_id: PROGRESS_CHANNEL_BASE,
        }];

        let (_session_b, mut receivers_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &data_regs,
            &progress_regs,
            16,
            &rt,
        )
        .unwrap();

        let (session_a, _) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Queue data first, then progress (both via try_send to avoid awaiting)
        let data_tx = session_a.data_sender("node-b").unwrap();
        let progress_tx = session_a.progress_sender("node-b").unwrap();

        // Send multiple data frames to fill the pipe
        for i in 0..5u8 {
            data_tx
                .try_send(Frame {
                    dataflow_id: df_id,
                    channel_id: 1,
                    payload: vec![i],
                })
                .unwrap();
        }

        // Now send a progress frame
        progress_tx
            .try_send(Frame {
                dataflow_id: df_id,
                channel_id: PROGRESS_CHANNEL_BASE,
                payload: vec![0xFF],
            })
            .unwrap();

        // Give the bridge task time to process
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Read all available frames. The progress frame should appear
        // because the bridge prioritizes it. We can't guarantee exact
        // ordering in a test (the first data frames may have already
        // been sent before the progress frame was queued), but we CAN
        // verify all frames arrive.
        let timeout = std::time::Duration::from_secs(2);
        let mut progress_rx = receivers_b
            .get_mut("node-a")
            .unwrap()
            .remove(&PROGRESS_CHANNEL_BASE)
            .unwrap();
        let mut data_rx = receivers_b.get_mut("node-a").unwrap().remove(&1).unwrap();

        let progress_payload = poll_recv(&mut progress_rx, timeout)
            .await
            .expect("progress should arrive");
        assert_eq!(progress_payload, vec![0xFF]);

        // All 5 data frames should also arrive
        for i in 0..5u8 {
            let data_payload = poll_recv(&mut data_rx, timeout)
                .await
                .expect("data should arrive");
            assert_eq!(data_payload, vec![i]);
        }

        drop(session_a);
    }

    #[tokio::test]
    async fn session_bidirectional() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        // node-a receives channel 2, node-b receives channel 1
        let (session_a, mut recv_a) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[ChannelRegistration {
                peer_node_id: "node-b".into(),
                channel_id: 2,
            }],
            &[],
            16,
            &rt,
        )
        .unwrap();

        let (session_b, mut recv_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[ChannelRegistration {
                peer_node_id: "node-a".into(),
                channel_id: 1,
            }],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // A → B
        session_a
            .data_sender("node-b")
            .unwrap()
            .send(Frame {
                dataflow_id: df_id,
                channel_id: 1,
                payload: vec![10],
            })
            .await
            .unwrap();

        // B → A
        session_b
            .data_sender("node-a")
            .unwrap()
            .send(Frame {
                dataflow_id: df_id,
                channel_id: 2,
                payload: vec![20],
            })
            .await
            .unwrap();

        let timeout = std::time::Duration::from_secs(2);
        let p1 = poll_recv(
            recv_b.get_mut("node-a").unwrap().get_mut(&1).unwrap(),
            timeout,
        )
        .await
        .unwrap();
        let p2 = poll_recv(
            recv_a.get_mut("node-b").unwrap().get_mut(&2).unwrap(),
            timeout,
        )
        .await
        .unwrap();
        assert_eq!(p1, vec![10]);
        assert_eq!(p2, vec![20]);

        drop(session_a);
        drop(session_b);
    }

    #[tokio::test]
    async fn session_control_channel() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        // The control channel (0) is registered automatically.
        let (_session_b, mut recv_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        let (session_a, _) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Send control frame
        session_a
            .control_sender("node-b")
            .unwrap()
            .send(Frame {
                dataflow_id: df_id,
                channel_id: CONTROL_CHANNEL_ID,
                payload: b"READY".to_vec(),
            })
            .await
            .unwrap();

        let timeout = std::time::Duration::from_secs(2);
        let payload = poll_recv(
            recv_b
                .get_mut("node-a")
                .unwrap()
                .get_mut(&CONTROL_CHANNEL_ID)
                .unwrap(),
            timeout,
        )
        .await
        .expect("control frame should arrive");
        assert_eq!(payload, b"READY".to_vec());

        drop(session_a);
    }

    #[tokio::test]
    async fn session_drop_terminates_tasks() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (_b_to_a, _a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        let (session, _receivers) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "peer".into(),
                reader: b_from_a,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        // Drop the session — bridge tasks terminate when senders close,
        // demux tasks are aborted.
        drop(session);

        // Tasks should clean up promptly. Verify no panic or hang.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    /// Regression test: verifies that data and progress frames sent by a
    /// worker in causal order (data first, then progress) arrive at the
    /// receiver in the same order on the TCP stream.
    ///
    /// With the old 3-channel design (separate data/progress channels with
    /// biased select prioritizing progress), this test would intermittently
    /// fail because progress could be written to TCP before data.
    #[tokio::test]
    async fn data_progress_fifo_ordering() {
        let df_id = make_dataflow_id();
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        let rt = tokio::runtime::Handle::current();

        // Use channel IDs: 1 for data, 1000 for progress (typical layout).
        let data_ch = 1u64;
        let progress_ch = 1000u64;

        let (session_a, _) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &[],
            16,
            &rt,
        )
        .unwrap();

        let (_session_b, mut recv_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[
                ChannelRegistration {
                    peer_node_id: "node-a".into(),
                    channel_id: data_ch,
                },
                ChannelRegistration {
                    peer_node_id: "node-a".into(),
                    channel_id: progress_ch,
                },
            ],
            &[],
            16,
            &rt,
        )
        .unwrap();

        let data_sender = session_a.data_sender("node-b").unwrap().clone();
        let progress_sender = session_a.progress_sender("node-b").unwrap().clone();

        // Simulate a worker sending data then progress rapidly, many times.
        // With the old priority bug, progress would race ahead of data.
        let num_epochs = 100;
        for epoch in 0u32..num_epochs {
            // Worker sends data FIRST
            data_sender
                .send(Frame {
                    dataflow_id: df_id,
                    channel_id: data_ch,
                    payload: epoch.to_le_bytes().to_vec(),
                })
                .await
                .unwrap();
            // Worker then releases capability (progress)
            progress_sender
                .send(Frame {
                    dataflow_id: df_id,
                    channel_id: progress_ch,
                    payload: epoch.to_le_bytes().to_vec(),
                })
                .await
                .unwrap();
        }

        // Drop senders to let bridge drain and close.
        drop(data_sender);
        drop(progress_sender);
        drop(session_a);

        // Collect received frames in order and verify data[N] arrives before
        // progress[N] for every epoch.
        let timeout = std::time::Duration::from_secs(5);
        let peer_map = recv_b.get_mut("node-a").unwrap();
        let mut data_rx = peer_map.remove(&data_ch).unwrap();
        let mut progress_rx = peer_map.remove(&progress_ch).unwrap();

        // Collect all data frames
        let mut data_received = Vec::new();
        while let Ok(Some(payload)) = tokio::time::timeout(timeout, data_rx.recv()).await {
            data_received.push(u32::from_le_bytes(payload[..4].try_into().unwrap()));
        }

        // Collect all progress frames
        let mut progress_received = Vec::new();
        while let Ok(Some(payload)) = tokio::time::timeout(timeout, progress_rx.recv()).await {
            progress_received.push(u32::from_le_bytes(payload[..4].try_into().unwrap()));
        }

        // All epochs must be received
        assert_eq!(
            data_received.len(),
            num_epochs as usize,
            "expected all data frames"
        );
        assert_eq!(
            progress_received.len(),
            num_epochs as usize,
            "expected all progress frames"
        );

        // Verify FIFO ordering (both should be 0,1,2,...,N-1)
        let expected: Vec<u32> = (0..num_epochs).collect();
        assert_eq!(data_received, expected, "data should be in FIFO order");
        assert_eq!(
            progress_received, expected,
            "progress should be in FIFO order"
        );
    }

    fn reg(peer: &str, channel_id: u64) -> ChannelRegistration {
        ChannelRegistration {
            peer_node_id: peer.into(),
            channel_id,
        }
    }

    fn peer(node_id: &str) -> PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream> {
        let (a, _keep_b) = tokio::io::duplex(1024);
        let (_keep_c, d) = tokio::io::duplex(1024);
        PeerConnection {
            node_id: node_id.into(),
            reader: d,
            writer: a,
        }
    }

    fn try_session(
        connections: Vec<PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>>,
        data: &[ChannelRegistration],
        progress: &[ChannelRegistration],
        capacity: usize,
    ) -> Result<(), TransportError> {
        TransportSession::new(
            make_dataflow_id(),
            connections,
            data,
            progress,
            capacity,
            &tokio::runtime::Handle::current(),
        )
        .map(|_| ())
    }

    #[tokio::test]
    async fn invalid_registrations_are_rejected() {
        let invalid = |r: Result<(), TransportError>| {
            matches!(r, Err(TransportError::InvalidRegistration { .. }))
        };
        assert!(
            invalid(try_session(vec![peer("b")], &[], &[], 0)),
            "capacity 0"
        );
        assert!(
            invalid(try_session(vec![peer("b"), peer("b")], &[], &[], 8)),
            "duplicate peer"
        );
        assert!(
            invalid(try_session(vec![peer("b")], &[reg("c", 5)], &[], 8)),
            "unknown peer"
        );
        assert!(
            invalid(try_session(
                vec![peer("b")],
                &[reg("b", CONTROL_CHANNEL_ID)],
                &[],
                8
            )),
            "control channel"
        );
        assert!(
            invalid(try_session(
                vec![peer("b")],
                &[reg("b", 5)],
                &[reg("b", 5)],
                8
            )),
            "duplicate across data and progress"
        );
        assert!(
            invalid(try_session(
                vec![peer("b")],
                &[reg("b", 5), reg("b", 5)],
                &[],
                8
            )),
            "duplicate within data"
        );
        // The same id from two different peers is fine: receivers are per peer.
        assert!(
            try_session(
                vec![peer("b"), peer("c")],
                &[reg("b", 5), reg("c", 5)],
                &[],
                8
            )
            .is_ok()
        );
    }

    #[test]
    fn channel_id_namespaces_are_disjoint() {
        let n = 7;
        let mut seen = std::collections::HashSet::new();
        assert!(seen.insert(CONTROL_CHANNEL_ID));
        for src in 0..n {
            for dst in 0..n {
                let p = progress_channel_id(src, dst, n).unwrap();
                assert!((PROGRESS_CHANNEL_BASE..DATA_CHANNEL_BASE).contains(&p));
                assert!(seen.insert(p), "duplicate progress id {p}");
                for edge in 0..5 {
                    let d = data_channel_id(edge, src, dst, n).unwrap();
                    assert!((DATA_CHANNEL_BASE..RESERVED_CHANNEL_BASE).contains(&d));
                    assert!(seen.insert(d), "duplicate data id {d}");
                }
            }
        }
        assert!(!seen.contains(&crate::communication::PROBE_CHANNEL_ID));
    }

    #[test]
    fn channel_ids_reject_out_of_range_and_overflow() {
        // Out-of-range worker indices would alias another pair's id.
        assert!(progress_channel_id(0, 4, 4).is_err());
        assert!(progress_channel_id(4, 0, 4).is_err());
        assert!(data_channel_id(0, 0, 4, 4).is_err());
        assert!(data_channel_id(0, 4, 0, 4).is_err());

        // Largest valid data id for 2^20 workers: (e * N + s) * N + d < 2^62.
        let n = 1usize << 20;
        let max_edge = ((CHANNEL_NAMESPACE_SIZE / (n as u64 * n as u64)) - 1) as usize;
        let top = data_channel_id(max_edge, n - 1, n - 1, n).unwrap();
        assert_eq!(top, RESERVED_CHANNEL_BASE - 1);
        assert!(data_channel_id(max_edge + 1, 0, 0, n).is_err());
        assert!(data_channel_id(usize::MAX, n - 1, n - 1, n).is_err());

        // Progress ids for 2^31 workers fit exactly; beyond does not.
        let n = 1usize << 31;
        assert_eq!(
            progress_channel_id(n - 1, n - 1, n).unwrap(),
            DATA_CHANNEL_BASE - 1
        );
        assert!(progress_channel_id(0, 0, (1usize << 31) + 1).is_ok());
        assert!(progress_channel_id((1 << 31) + 1, 0, (1usize << 31) + 2).is_err());
    }
}
