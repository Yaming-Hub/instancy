//! Unified transport abstraction for cluster execution.
//!
//! [`ClusterTransport`] wraps either a dedicated [`TransportSession`] or a
//! shared [`SharedTransportSession`], providing a uniform interface for
//! [`spawn_cluster`](crate::RuntimeHandle::spawn_cluster) and the network
//! exchange materializer. This allows the execution layer to work with both
//! transport modes without code duplication.
//!
//! # Frame Sending
//!
//! [`FrameSender`] abstracts over the two send mechanisms:
//! - **Dedicated**: `tokio::sync::mpsc::Sender<Frame>` (direct to bridge task)
//! - **Shared**: [`DataframeSender`] (tags the dataflow id, and numbers each frame as it queues it on the dataflow's pooled connection)
//!
//! Both support synchronous `try_send()` for use from the worker thread pool.

#[cfg(feature = "transport")]
use std::collections::HashMap;
#[cfg(feature = "transport")]
use std::sync::Arc;

#[cfg(feature = "transport")]
use tokio::sync::mpsc as tokio_mpsc;

#[cfg(feature = "transport")]
use crate::communication::shared_transport::{DataframeSender, SharedTransportSession};
#[cfg(feature = "transport")]
use crate::communication::transport::Frame;
#[cfg(feature = "transport")]
use crate::communication::transport_session::TransportSession;

// ---------------------------------------------------------------------------
// FrameSender — unified synchronous frame sending
// ---------------------------------------------------------------------------

/// A sender that can push frames to either a dedicated or shared transport.
///
/// Used by [`NetworkPush`](crate::dataflow::channels::network::NetworkPush) to
/// send serialized data frames to remote peers without knowing the underlying
/// transport mode.
#[cfg(feature = "transport")]
#[derive(Clone)]
pub enum FrameSender {
    /// Direct channel to a dedicated bridge task (one connection per dataflow).
    Direct(tokio_mpsc::Sender<Frame>),
    /// Shared transport sender (prepends dataflow_id, sequences, pool-routes).
    Shared(DataframeSender),
}

#[cfg(feature = "transport")]
impl FrameSender {
    /// Try to send a frame without blocking.
    ///
    /// Returns `Ok(())` on success, `Err(TrySendError)` if the channel is
    /// full or closed.
    pub fn try_send(&self, frame: Frame) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        match self {
            Self::Direct(tx) => tx.try_send(frame),
            Self::Shared(tx) => tx.try_send(frame),
        }
    }

    /// Send a frame asynchronously (waits if the channel is full).
    pub async fn send(&self, frame: Frame) -> Result<(), tokio_mpsc::error::SendError<Frame>> {
        match self {
            Self::Direct(tx) => tx.send(frame).await,
            Self::Shared(tx) => tx.send(frame).await,
        }
    }
}

// ---------------------------------------------------------------------------
// PeerChannelReceiver — frames from a peer on one channel, either mode
// ---------------------------------------------------------------------------

/// The frames a peer sends on one channel of a cluster dataflow.
///
/// Bounded in Dedicated mode (each dataflow has its own connection). In
/// Shared mode a pooled connection's reader must never wait for a consumer:
/// one slow consumer would stop every dataflow on the connection, and the
/// connection would read nothing and be declared dead (see the
/// [`shared_transport`](crate::communication::shared_transport) module
/// documentation), so its channels are unbounded.
#[cfg(feature = "transport")]
#[derive(Debug)]
pub(crate) enum PeerChannelReceiver {
    Bounded(tokio_mpsc::Receiver<Vec<u8>>),
    Unbounded(tokio_mpsc::UnboundedReceiver<Vec<u8>>),
}

#[cfg(feature = "transport")]
impl PeerChannelReceiver {
    /// The next frame's payload, or `None` once the channel is closed and
    /// empty.
    pub(crate) async fn recv(&mut self) -> Option<Vec<u8>> {
        match self {
            Self::Bounded(rx) => rx.recv().await,
            Self::Unbounded(rx) => rx.recv().await,
        }
    }

    /// Poll for the next frame's payload (see [`Self::recv`]).
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Vec<u8>>> {
        match self {
            Self::Bounded(rx) => rx.poll_recv(cx),
            Self::Unbounded(rx) => rx.poll_recv(cx),
        }
    }

    /// The next frame's payload, if one is queued.
    pub(crate) fn try_recv(&mut self) -> Result<Vec<u8>, tokio_mpsc::error::TryRecvError> {
        match self {
            Self::Bounded(rx) => rx.try_recv(),
            Self::Unbounded(rx) => rx.try_recv(),
        }
    }
}

#[cfg(feature = "transport")]
impl From<tokio_mpsc::Receiver<Vec<u8>>> for PeerChannelReceiver {
    fn from(rx: tokio_mpsc::Receiver<Vec<u8>>) -> Self {
        Self::Bounded(rx)
    }
}

#[cfg(feature = "transport")]
impl From<tokio_mpsc::UnboundedReceiver<Vec<u8>>> for PeerChannelReceiver {
    fn from(rx: tokio_mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self::Unbounded(rx)
    }
}

/// Every channel receiver of a cluster dataflow, by peer, then channel id.
#[cfg(feature = "transport")]
pub(crate) type PeerReceivers = HashMap<String, HashMap<u64, PeerChannelReceiver>>;

/// [`PeerReceivers`] from the receivers of a transport session.
#[cfg(feature = "transport")]
pub(crate) fn peer_receivers<R: Into<PeerChannelReceiver>>(
    receivers: HashMap<String, HashMap<u64, R>>,
) -> PeerReceivers {
    receivers
        .into_iter()
        .map(|(peer, channels)| {
            (
                peer,
                channels
                    .into_iter()
                    .map(|(id, rx)| (id, rx.into()))
                    .collect(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// ClusterTransport — unified transport session for spawn_cluster
// ---------------------------------------------------------------------------

/// Wraps either a dedicated or shared transport session for cluster execution.
///
/// This provides a uniform interface for the execution layer:
/// - Data/progress/control senders per peer
/// - Receiver extraction
/// - Peer enumeration
/// - Lifetime management (keep-alive for dedicated, unregister for shared)
#[cfg(feature = "transport")]
pub enum ClusterTransport {
    /// Dedicated mode: one `TransportSession` per dataflow, one connection per peer.
    Dedicated(Arc<TransportSession>),
    /// Shared mode: lightweight handle into `SharedPeerManager`s shared across dataflows.
    Shared {
        session: Arc<SharedTransportSession>,
    },
}

#[cfg(feature = "transport")]
impl ClusterTransport {
    /// Get a data sender for a peer.
    ///
    /// For dedicated mode, returns a `Direct` variant.
    /// For shared mode, returns a `Shared` variant (with sequencing).
    pub fn data_sender(&self, peer_node_id: &str) -> Option<FrameSender> {
        match self {
            Self::Dedicated(session) => session
                .data_sender(peer_node_id)
                .map(|tx| FrameSender::Direct(tx.clone())),
            Self::Shared { session, .. } => {
                session.data_sender(peer_node_id).map(FrameSender::Shared)
            }
        }
    }

    /// Get a progress sender for a peer.
    ///
    /// For both modes, progress shares the same channel as data to preserve
    /// the timely ordering invariant.
    pub fn progress_sender(&self, peer_node_id: &str) -> Option<FrameSender> {
        match self {
            Self::Dedicated(session) => session
                .progress_sender(peer_node_id)
                .map(|tx| FrameSender::Direct(tx.clone())),
            Self::Shared { session, .. } => session
                .progress_sender(peer_node_id)
                .map(FrameSender::Shared),
        }
    }

    /// Get a control-priority sender for a peer.
    ///
    /// Control frames bypass sequencing in shared mode.
    pub fn control_sender(&self, peer_node_id: &str) -> Option<&tokio_mpsc::Sender<Frame>> {
        match self {
            Self::Dedicated(session) => session.control_sender(peer_node_id),
            Self::Shared { session, .. } => session.control_sender(peer_node_id),
        }
    }

    /// Whether sending to a peer has failed: its writer failed, so frames
    /// queued for it are lost and later ones are refused; or its connection
    /// failed on the read side (a reset, say), so frames already written may
    /// be lost with it. In Shared mode a read error fails the lane too; in
    /// Dedicated mode the writer keeps going until a write fails, which may
    /// be only the goodbye. The transport knows at once, before the
    /// connection event reaches the dataflow.
    pub(crate) fn outbound_failed(&self, peer_node_id: &str) -> bool {
        match self {
            Self::Dedicated(session) => {
                session
                    .data_sender(peer_node_id)
                    .is_some_and(tokio_mpsc::Sender::is_closed)
                    || session.read_failed(peer_node_id)
            }
            Self::Shared { session } => session
                .data_sender(peer_node_id)
                .is_some_and(|sender| sender.has_failed()),
        }
    }

    /// Returns the set of peer node IDs this transport has connections to.
    pub fn peer_node_ids(&self) -> Vec<String> {
        match self {
            Self::Dedicated(session) => session.peer_node_ids().map(|s| s.to_string()).collect(),
            Self::Shared { session, .. } => {
                session.peer_node_ids().map(|s| s.to_string()).collect()
            }
        }
    }

    /// Largest frame payload this transport can carry.
    ///
    /// Shared connections prefix every payload frame with an 8-byte sequence
    /// number, which counts against the frame size limit.
    pub fn max_payload_len(&self) -> usize {
        use crate::communication::codec::MAX_MESSAGE_SIZE;
        match self {
            Self::Dedicated(_) => MAX_MESSAGE_SIZE,
            Self::Shared { .. } => MAX_MESSAGE_SIZE - 8,
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterSpawnTransport — spawn_cluster transport configuration
// ---------------------------------------------------------------------------

/// Transport configuration for [`spawn_cluster`](crate::RuntimeHandle::spawn_cluster).
///
/// Determines whether the cluster dataflow uses dedicated per-dataflow connections
/// or shares pooled connections via [`SharedPeerManager`](crate::communication::shared_transport::SharedPeerManager)s.
///
/// # Examples
///
/// **Dedicated mode** (one connection per peer per dataflow):
/// ```ignore
/// let transport = ClusterSpawnTransport::dedicated(connections, 1024);
/// runtime.spawn_cluster(name, topology, node_id, df_id, transport, timeout, build, &rt)?;
/// ```
///
/// **Shared mode** (multiplexed over pooled connections):
/// ```ignore
/// let managers = Arc::new(peer_managers);
/// let transport = ClusterSpawnTransport::shared(managers, 1024);
/// runtime.spawn_cluster(name, topology, node_id, df_id, transport, timeout, build, &rt)?;
/// ```
#[cfg(feature = "transport")]
pub enum ClusterSpawnTransport<R = tokio::io::DuplexStream, W = tokio::io::DuplexStream> {
    /// Use dedicated per-dataflow connections (one TCP connection per peer).
    Dedicated {
        /// Pre-established connections to each remote peer.
        connections: Vec<crate::communication::transport_session::PeerConnection<R, W>>,
        /// Buffer capacity for transport channels.
        capacity: usize,
    },
    /// Use shared pooled connections via existing [`SharedPeerManager`](crate::communication::shared_transport::SharedPeerManager)s.
    ///
    /// The `Arc` ensures peer managers remain alive for the lifetime of the
    /// dataflow, preventing premature task abortion.
    Shared {
        /// Map of peer_node_id → SharedPeerManager (must match topology peers).
        peer_managers:
            Arc<HashMap<String, crate::communication::shared_transport::SharedPeerManager>>,
        /// Buffer capacity for per-channel receivers.
        capacity: usize,
    },
}

#[cfg(feature = "transport")]
impl<R, W> ClusterSpawnTransport<R, W> {
    /// Create a dedicated transport configuration.
    pub fn dedicated(
        connections: Vec<crate::communication::transport_session::PeerConnection<R, W>>,
        capacity: usize,
    ) -> Self {
        Self::Dedicated {
            connections,
            capacity,
        }
    }
}

#[cfg(feature = "transport")]
impl ClusterSpawnTransport {
    /// Create a shared transport configuration.
    ///
    /// The `Arc` ownership ensures peer managers stay alive for the duration of
    /// the dataflow, preventing the background tasks from being aborted prematurely.
    pub fn shared(
        peer_managers: Arc<
            HashMap<String, crate::communication::shared_transport::SharedPeerManager>,
        >,
        capacity: usize,
    ) -> Self {
        Self::Shared {
            peer_managers,
            capacity,
        }
    }
}
