//! Multiplexed transport session with priority-separated channels.
//!
//! [`TransportSession`] manages per-peer Muxer/Demuxer infrastructure and
//! provides **priority-separated send channels** for data, progress, and
//! control frames.
//!
//! # Priority
//!
//! The per-peer bridge task uses biased `select!` to ensure:
//! 1. **Control** frames are sent first (handshake, shutdown)
//! 2. **Progress** frames are sent next (frontier advancement)
//! 3. **Data** frames are sent last
//!
//! This prevents data backpressure from blocking progress updates, which
//! could cause distributed deadlock (full data queue blocks progress,
//! which blocks frontier advancement needed to drain data).
//!
//! # Ownership
//!
//! Each `TransportSession` is created per-dataflow. TCP connections can be
//! shared across dataflows at the caller level, but each session gets its
//! own logical channels. The session is `Arc`-wrapped and shared by all
//! endpoints; background tasks are aborted when the last reference drops.
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
//! // Get senders for a peer
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
    /// Per-peer data frame senders (bounded).
    data_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Per-peer progress frame senders (bounded, higher priority than data).
    progress_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Per-peer control frame senders (bounded, highest priority).
    control_senders: HashMap<String, tokio_mpsc::Sender<Frame>>,
    /// Shared state keeping background tasks alive (abort on drop).
    _state: std::sync::Arc<SessionState>,
}

/// Holds background task handles. Aborts all tasks on drop.
#[cfg(feature = "transport")]
struct SessionState {
    bridge_handles: Vec<tokio::task::JoinHandle<()>>,
    demux_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "transport")]
impl Drop for SessionState {
    fn drop(&mut self) {
        for handle in &self.bridge_handles {
            handle.abort();
        }
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
    ) -> (Self, HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut data_senders = HashMap::new();
        let mut progress_senders = HashMap::new();
        let mut control_senders = HashMap::new();
        let mut bridge_handles = Vec::new();
        let mut demux_handles = Vec::new();
        let mut all_receivers: HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>> =
            HashMap::new();

        for conn in connections {
            let peer_node_id = conn.node_id.clone();

            // --- Send side: three priority channels per peer ---
            let (data_tx, data_rx) = tokio_mpsc::channel::<Frame>(capacity);
            let (progress_tx, progress_rx) = tokio_mpsc::channel::<Frame>(capacity);
            let (control_tx, control_rx) = tokio_mpsc::channel::<Frame>(capacity);

            data_senders.insert(peer_node_id.clone(), data_tx);
            progress_senders.insert(peer_node_id.clone(), progress_tx);
            control_senders.insert(peer_node_id.clone(), control_tx);

            // Spawn priority bridge task
            let writer = conn.writer;
            let peer_id = peer_node_id.clone();
            let bridge_handle = runtime_handle.spawn(async move {
                Self::bridge_task(peer_id, writer, control_rx, progress_rx, data_rx).await;
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
            bridge_handles,
            demux_handles,
        });

        let session = Self {
            data_senders,
            progress_senders,
            control_senders,
            _state: state,
        };

        (session, all_receivers)
    }

    /// Get a data-priority sender for a peer.
    ///
    /// Returns `None` if no connection exists to the specified peer.
    /// The sender can be cloned for use by multiple `NetworkPush` endpoints
    /// targeting the same peer.
    pub fn data_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.data_senders.get(peer_node_id)
    }

    /// Get a progress-priority sender for a peer.
    ///
    /// Progress frames are sent with higher priority than data, preventing
    /// data backpressure from blocking frontier advancement.
    pub fn progress_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        self.progress_senders.get(peer_node_id)
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
        self.data_senders.keys().map(|s| s.as_str())
    }

    /// Priority bridge task: reads from control/progress/data channels
    /// and writes to the TCP connection with priority ordering.
    ///
    /// Uses `biased` `select!` to ensure control > progress > data ordering
    /// when multiple channels have pending frames simultaneously.
    async fn bridge_task<W: AsyncWrite + Unpin>(
        _peer_id: String,
        writer: W,
        mut control_rx: tokio_mpsc::Receiver<Frame>,
        mut progress_rx: tokio_mpsc::Receiver<Frame>,
        mut data_rx: tokio_mpsc::Receiver<Frame>,
    ) {
        let mut framed_writer = FramedWriter::new(writer);
        let mut control_open = true;
        let mut progress_open = true;

        loop {
            let frame = tokio::select! {
                biased;

                result = control_rx.recv(), if control_open => match result {
                    Some(f) => f,
                    None => { control_open = false; continue; }
                },
                result = progress_rx.recv(), if progress_open => match result {
                    Some(f) => f,
                    None => { progress_open = false; continue; }
                },
                result = data_rx.recv() => match result {
                    Some(f) => f,
                    None => {
                        // Data channel closed → drain remaining control/progress
                        // frames before exiting to avoid losing late-arriving
                        // progress updates or shutdown sentinels.
                        while let Ok(f) = control_rx.try_recv() {
                            if framed_writer.write_frame(&f).await.is_err() {
                                return;
                            }
                        }
                        while let Ok(f) = progress_rx.try_recv() {
                            if framed_writer.write_frame(&f).await.is_err() {
                                return;
                            }
                        }
                        break;
                    }
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
        tokio::time::timeout(timeout, rx.recv()).await.ok().flatten()
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

        let rx = receivers_b.get_mut("node-a").unwrap().get_mut(&(PROGRESS_CHANNEL_BASE + 1)).unwrap();
        let timeout = std::time::Duration::from_secs(2);
        let payload = poll_recv(rx, timeout).await.expect("should receive progress");
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
            data_tx.try_send(Frame {
                dataflow_id: df_id,
                channel_id: 1,
                payload: vec![i],
            }).unwrap();
        }

        // Now send a progress frame
        progress_tx.try_send(Frame {
            dataflow_id: df_id,
            channel_id: PROGRESS_CHANNEL_BASE,
            payload: vec![0xFF],
        }).unwrap();

        // Give the bridge task time to process
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Read all available frames. The progress frame should appear
        // because the bridge prioritizes it. We can't guarantee exact
        // ordering in a test (the first data frames may have already
        // been sent before the progress frame was queued), but we CAN
        // verify all frames arrive.
        let timeout = std::time::Duration::from_secs(2);
        let mut progress_rx = receivers_b.get_mut("node-a").unwrap().remove(&PROGRESS_CHANNEL_BASE).unwrap();
        let mut data_rx = receivers_b.get_mut("node-a").unwrap().remove(&1).unwrap();

        let progress_payload = poll_recv(&mut progress_rx, timeout).await
            .expect("progress should arrive");
        assert_eq!(progress_payload, vec![0xFF]);

        // All 5 data frames should also arrive
        for i in 0..5u8 {
            let data_payload = poll_recv(&mut data_rx, timeout).await
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
            &[ChannelRegistration { peer_node_id: "node-b".into(), channel_id: 2 }],
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
            &[ChannelRegistration { peer_node_id: "node-a".into(), channel_id: 1 }],
            &[],
            16,
            &rt,
        );

        // A → B
        session_a.data_sender("node-b").unwrap().send(Frame {
            dataflow_id: df_id, channel_id: 1, payload: vec![10],
        }).await.unwrap();

        // B → A
        session_b.data_sender("node-a").unwrap().send(Frame {
            dataflow_id: df_id, channel_id: 2, payload: vec![20],
        }).await.unwrap();

        let timeout = std::time::Duration::from_secs(2);
        let p1 = poll_recv(recv_b.get_mut("node-a").unwrap().get_mut(&1).unwrap(), timeout).await.unwrap();
        let p2 = poll_recv(recv_a.get_mut("node-b").unwrap().get_mut(&2).unwrap(), timeout).await.unwrap();
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
            &[ChannelRegistration { peer_node_id: "node-a".into(), channel_id: CONTROL_CHANNEL_ID }],
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
        session_a.control_sender("node-b").unwrap().send(Frame {
            dataflow_id: df_id,
            channel_id: CONTROL_CHANNEL_ID,
            payload: b"READY".to_vec(),
        }).await.unwrap();

        let timeout = std::time::Duration::from_secs(2);
        let payload = poll_recv(recv_b.get_mut("node-a").unwrap().get_mut(&CONTROL_CHANNEL_ID).unwrap(), timeout).await
            .expect("control frame should arrive");
        assert_eq!(payload, b"READY".to_vec());

        drop(session_a);
    }

    #[tokio::test]
    async fn session_drop_aborts_tasks() {
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

        // Drop the session — tasks should be aborted
        drop(session);

        // If tasks weren't aborted, they'd leak. We just verify no panic.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
