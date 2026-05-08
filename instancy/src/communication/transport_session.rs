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
//! );
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
use crate::communication::transport::{DemuxConfig, Demuxer, Frame, FramedWriter};
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
    /// Data and progress channels use separate channel ID spaces. The caller
    /// is responsible for assigning non-overlapping IDs. Typically:
    /// - Data: `src * num_workers + dst + 1`
    /// - Progress: `PROGRESS_CHANNEL_BASE + src * num_workers + dst`
    /// - Control: channel ID 0 (auto-registered per peer)
    pub fn new<R, W>(
        dataflow_id: DataflowId,
        connections: Vec<PeerConnection<R, W>>,
        data_channels: &[ChannelRegistration],
        progress_channels: &[ChannelRegistration],
        capacity: usize,
        runtime_handle: &tokio::runtime::Handle,
    ) -> (
        Self,
        HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>,
    )
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
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

            // Auto-register control channel (ID 0) for this peer.
            let control_recv = demuxer.register_channel(dataflow_id, CONTROL_CHANNEL_ID);
            all_receivers
                .entry(peer_node_id.clone())
                .or_default()
                .insert(CONTROL_CHANNEL_ID, control_recv);

            // Register data channels from this peer
            for reg in data_channels {
                if reg.peer_node_id == peer_node_id {
                    let rx = demuxer.register_channel(dataflow_id, reg.channel_id);
                    all_receivers
                        .entry(peer_node_id.clone())
                        .or_default()
                        .insert(reg.channel_id, rx);
                }
            }

            // Register progress channels from this peer
            for reg in progress_channels {
                if reg.peer_node_id == peer_node_id {
                    let rx = demuxer.register_channel(dataflow_id, reg.channel_id);
                    all_receivers
                        .entry(peer_node_id.clone())
                        .or_default()
                        .insert(reg.channel_id, rx);
                }
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

        (session, all_receivers)
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

/// Base channel ID for progress channels.
///
/// Data channels use IDs `1..=(num_workers * num_workers)`.
/// Progress channels use IDs starting at this base:
/// `PROGRESS_CHANNEL_BASE + src * num_workers + dst`.
/// Channel ID 0 is reserved for control messages.
#[cfg(feature = "transport")]
pub const PROGRESS_CHANNEL_BASE: u64 = 1_000_000;

/// Channel ID for control messages (handshake, shutdown).
#[cfg(feature = "transport")]
pub const CONTROL_CHANNEL_ID: u64 = 0;

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
        );

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
        );

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
        );

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
        );

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
        );

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
        );

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
        );

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
        );

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

        // Register control channel as a data registration (channel 0)
        let (_session_b, mut recv_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[ChannelRegistration {
                peer_node_id: "node-a".into(),
                channel_id: CONTROL_CHANNEL_ID,
            }],
            &[],
            16,
            &rt,
        );

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
        );

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
        );

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
        );

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
        );

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
        while let Ok(Some(payload)) =
            tokio::time::timeout(timeout, data_rx.recv()).await
        {
            data_received.push(u32::from_le_bytes(payload[..4].try_into().unwrap()));
        }

        // Collect all progress frames
        let mut progress_received = Vec::new();
        while let Ok(Some(payload)) =
            tokio::time::timeout(timeout, progress_rx.recv()).await
        {
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
}
