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
//! - **Shared**: [`DataframeSender`] (prepends dataflow_id, sequences, routes via pool)
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
    pub fn try_send(
        &self,
        frame: Frame,
    ) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        match self {
            Self::Direct(tx) => tx.try_send(frame),
            Self::Shared(tx) => tx.try_send(frame),
        }
    }

    /// Send a frame asynchronously (waits if the channel is full).
    pub async fn send(
        &self,
        frame: Frame,
    ) -> Result<(), tokio_mpsc::error::SendError<Frame>> {
        match self {
            Self::Direct(tx) => tx.send(frame).await,
            Self::Shared(tx) => tx.send(frame).await,
        }
    }
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
    ///
    /// Error receivers are stored separately (not behind the Arc) so they can be
    /// taken even after the transport Arc is cloned to materializers.
    Shared {
        session: Arc<SharedTransportSession>,
        error_receivers: Option<
            HashMap<String, tokio_mpsc::Receiver<crate::communication::transport::TransportError>>,
        >,
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
            Self::Shared { session, .. } => session
                .data_sender(peer_node_id)
                .map(FrameSender::Shared),
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

    /// Returns the set of peer node IDs this transport has connections to.
    pub fn peer_node_ids(&self) -> Vec<String> {
        match self {
            Self::Dedicated(session) => {
                session.peer_node_ids().map(|s| s.to_string()).collect()
            }
            Self::Shared { session, .. } => {
                session.peer_node_ids().map(|s| s.to_string()).collect()
            }
        }
    }

    /// Take per-peer error receivers (shared mode only).
    ///
    /// Returns `None` for dedicated mode (failures surface via channel closure).
    /// For shared mode, returns error receivers that emit `TransportError` on
    /// connection failures or reorder timeouts. Can only be called once — subsequent
    /// calls return `None`.
    pub fn take_error_receivers(
        &mut self,
    ) -> Option<HashMap<String, tokio_mpsc::Receiver<crate::communication::transport::TransportError>>>
    {
        match self {
            Self::Dedicated(_) => None,
            Self::Shared { error_receivers, .. } => error_receivers.take(),
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterSpawnTransport — spawn_cluster transport configuration
// ---------------------------------------------------------------------------

/// Transport configuration for [`spawn_cluster`](crate::RuntimeHandle::spawn_cluster).
///
/// Determines whether the cluster dataflow uses dedicated per-dataflow connections
/// or shares pooled connections via [`SharedPeerManager`]s.
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
/// let transport = ClusterSpawnTransport::shared(&peer_managers, 1024);
/// runtime.spawn_cluster(name, topology, node_id, df_id, transport, timeout, build, &rt)?;
/// ```
#[cfg(feature = "transport")]
pub enum ClusterSpawnTransport<'a, R = tokio::io::DuplexStream, W = tokio::io::DuplexStream> {
    /// Use dedicated per-dataflow connections (one TCP connection per peer).
    Dedicated {
        /// Pre-established connections to each remote peer.
        connections: Vec<crate::communication::transport_session::PeerConnection<R, W>>,
        /// Buffer capacity for transport channels.
        capacity: usize,
    },
    /// Use shared pooled connections via existing [`SharedPeerManager`]s.
    Shared {
        /// Map of peer_node_id → SharedPeerManager (must match topology peers).
        peer_managers: &'a HashMap<String, crate::communication::shared_transport::SharedPeerManager>,
        /// Buffer capacity for per-channel receivers.
        capacity: usize,
    },
}

#[cfg(feature = "transport")]
impl<'a, R, W> ClusterSpawnTransport<'a, R, W> {
    /// Create a dedicated transport configuration.
    pub fn dedicated(
        connections: Vec<crate::communication::transport_session::PeerConnection<R, W>>,
        capacity: usize,
    ) -> Self {
        Self::Dedicated { connections, capacity }
    }
}

#[cfg(feature = "transport")]
impl<'a> ClusterSpawnTransport<'a> {
    /// Create a shared transport configuration.
    pub fn shared(
        peer_managers: &'a HashMap<String, crate::communication::shared_transport::SharedPeerManager>,
        capacity: usize,
    ) -> Self {
        Self::Shared { peer_managers, capacity }
    }
}
