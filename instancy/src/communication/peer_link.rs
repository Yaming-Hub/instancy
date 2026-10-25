//! Per-peer outbound lanes and link lifecycle state for cluster dataflows.
//!
//! A cluster dataflow sends three kinds of frames to each peer over one FIFO
//! payload lane: data (pushed directly by `NetworkPush` with `try_send`),
//! progress deltas, and end-of-stream markers (close sentinels on data
//! channels, end-of-stream frames on progress channels). The latter two must
//! never be lost, so they are queued on a [`PeerOutbox`]: an unbounded,
//! ordered lane drained by a forwarder task that awaits capacity on the
//! transport's bounded payload channel. The forwarder never drops frames on
//! cancellation; it exits when every producer has released its outbox (or the
//! transport rejects a frame).
//!
//! [`PeerLinkState`] records whether a link has been *retired*: the peer has
//! acknowledged our end-of-stream, or we have received the peer's
//! end-of-stream. Once retired, neither side needs anything more from the
//! other for this dataflow, so later transport failures on that link are
//! benign. Data must never be sent on a retired link: the peer has finished
//! (or we have), so the data could never be consumed. `NetworkPush` treats
//! that as data loss and fails the dataflow.
//!
//! [`InboundBridges`] tracks the tasks that deliver data frames from peers to
//! local operators. They report frames that can no longer be delivered (the
//! receiving operator already finished) as [`LinkEvent::InboundDataLost`],
//! and teardown lets them finish draining before the dataflow completes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::mpsc as tokio_mpsc;

use crate::communication::cluster_transport::FrameSender;
use crate::communication::transport::{Frame, TransportError};

/// Why a peer link ended without a clean end-of-stream.
#[derive(Debug)]
pub(crate) enum LinkFailure {
    /// A receive channel closed (EOF or connection loss) before its marker.
    ChannelClosed,
    /// A frame could not be decoded (CRC mismatch or malformed payload).
    Decode(String),
    /// A frame broke the protocol: it arrived after its channel's end
    /// marker, or a channel ended without its marker after the peer said
    /// goodbye. Reported as data loss.
    ProtocolViolation(String),
    /// A local lock was poisoned while delivering a frame.
    LockPoisoned,
    /// The transport rejected an outbound frame (writer gone).
    SendFailed,
    /// An outbound frame could not be encoded.
    Encode(String),
    /// The transport reported a failure for this peer.
    Transport(TransportError),
}

impl std::fmt::Display for LinkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelClosed => f.write_str("connection closed before end of stream"),
            Self::Decode(detail) => write!(f, "undecodable frame: {detail}"),
            Self::ProtocolViolation(detail) => write!(f, "unexpected frame: {detail}"),
            Self::LockPoisoned => f.write_str("lock poisoned while delivering a frame"),
            Self::SendFailed => f.write_str("outbound frame could not be sent"),
            Self::Encode(detail) => write!(f, "outbound frame could not be encoded: {detail}"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
        }
    }
}

/// Something observed on the link to one peer.
#[derive(Debug)]
pub(crate) enum LinkEvent {
    /// One progress channel from the peer delivered its end-of-stream marker.
    ProgressEndOfStream,
    /// One progress channel from the peer ended without its marker, failed
    /// to decode, or carried a frame after its marker.
    ProgressChannelEnded(LinkFailure),
    /// One data channel from the peer ended without its close sentinel,
    /// failed to decode, or carried a frame after its sentinel.
    DataChannelEnded(LinkFailure),
    /// An outbound frame to the peer could not be sent or encoded.
    OutboundFailed(LinkFailure),
    /// The peer acknowledged our end-of-stream.
    AckReceived,
    /// The peer cancelled the dataflow.
    CancelReceived,
    /// The peer's control channel closed (connection gone).
    ControlClosed,
    /// A control message from the peer could not be decoded.
    ControlUndecodable(String),
    /// A data frame from the peer could not be delivered to the local
    /// operator it was addressed to (the operator had already finished), or
    /// arrived after the channel's close marker.
    InboundDataLost(String),
    /// The connection to the peer (Dedicated mode), or the pooled connection
    /// the peer's frames arrive on (Shared mode), ended (EOF) or failed, or
    /// (Shared mode) a frame from the peer was out of sequence; every channel
    /// from the peer is closing. Reported before those channels close (see
    /// `PeerIoNotice`).
    ConnectionFailed(TransportError),
}

/// Input to the cluster lifecycle coordinator.
#[derive(Debug)]
pub(crate) enum LifecycleEvent {
    /// A local worker finished; `success` is true only for `Ok(true)`.
    /// `failure` describes an error other than a cancellation.
    WorkerFinished {
        success: bool,
        failure: Option<String>,
    },
    /// An event on the link to `peer`.
    Link { peer: String, event: LinkEvent },
}

/// Sender half of the lifecycle event channel.
pub(crate) type LifecycleEventSender = tokio_mpsc::UnboundedSender<LifecycleEvent>;

/// Report a link event, ignoring a coordinator that already finished.
pub(crate) fn report_link_event(events: &LifecycleEventSender, peer: &str, event: LinkEvent) {
    let _ = events.send(LifecycleEvent::Link {
        peer: peer.to_string(),
        event,
    });
}

/// Shared per-peer link state.
#[derive(Debug, Default)]
pub(crate) struct PeerLinkState {
    retired: AtomicBool,
    /// A data push to the peer found its queue closed: it backs off on a
    /// frame that can never leave.
    push_blocked: AtomicBool,
}

impl PeerLinkState {
    /// Whether the link no longer carries anything either side needs.
    ///
    /// Sequentially consistent with [`retire`](Self::retire): a data push
    /// that enqueues a frame and then still sees the link unretired knows
    /// the frame was enqueued before anything the coordinator queues after
    /// retiring the link (such as the acknowledgement).
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Mark the link retired. Idempotent.
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::SeqCst);
    }

    /// Whether a data push to the peer is backing off on a closed queue.
    pub(crate) fn push_blocked(&self) -> bool {
        self.push_blocked.load(Ordering::SeqCst)
    }

    /// A data push to the peer found its queue closed.
    pub(crate) fn note_push_blocked(&self) {
        self.push_blocked.store(true, Ordering::SeqCst);
    }
}

/// Producer handle for the ordered, lossless lane to one peer.
#[derive(Clone)]
pub(crate) struct PeerOutbox {
    peer: Arc<str>,
    lane: tokio_mpsc::UnboundedSender<Frame>,
    link: Arc<PeerLinkState>,
    events: LifecycleEventSender,
}

impl std::fmt::Debug for PeerOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerOutbox")
            .field("peer", &self.peer)
            .field("retired", &self.link.is_retired())
            .finish()
    }
}

impl PeerOutbox {
    /// Queue a frame behind everything already queued for this peer.
    ///
    /// Never blocks. Fails only after the forwarder stopped, which happens
    /// when the transport rejected a frame (already reported) or the dataflow
    /// was torn down; callers may ignore the error.
    pub(crate) fn send(&self, frame: Frame) -> Result<(), Frame> {
        self.lane.send(frame).map_err(|e| e.0)
    }

    /// The peer this outbox delivers to.
    pub(crate) fn peer(&self) -> &str {
        &self.peer
    }

    /// Whether the link to this peer has been retired.
    pub(crate) fn is_retired(&self) -> bool {
        self.link.is_retired()
    }

    /// Shared link state for this peer.
    pub(crate) fn link(&self) -> &Arc<PeerLinkState> {
        &self.link
    }

    /// Report an event on this peer's link.
    pub(crate) fn report(&self, event: LinkEvent) {
        report_link_event(&self.events, &self.peer, event);
    }

    /// A reporting handle for inbound frames from this peer. Unlike the
    /// outbox it does not keep the outbound lane open.
    pub(crate) fn monitor(&self) -> PeerMonitor {
        PeerMonitor {
            peer: Arc::clone(&self.peer),
            events: self.events.clone(),
        }
    }
}

/// Reports events about frames received from one peer.
#[derive(Clone)]
pub(crate) struct PeerMonitor {
    peer: Arc<str>,
    events: LifecycleEventSender,
}

impl PeerMonitor {
    /// Report an event on this peer's link.
    pub(crate) fn report(&self, event: LinkEvent) {
        report_link_event(&self.events, &self.peer, event);
    }

    /// A monitor for tests.
    #[cfg(test)]
    pub(crate) fn for_test(peer: &str, events: LifecycleEventSender) -> Self {
        Self {
            peer: Arc::from(peer),
            events,
        }
    }
}

/// Tracks the inbound data bridges of one cluster dataflow.
///
/// Teardown asks the bridges to deliver (or report) everything already
/// queued for them and then waits until all of them have exited, so frames
/// that arrived before the clean-shutdown handshake completed are never
/// discarded unseen.
#[derive(Clone, Default)]
pub(crate) struct InboundBridges {
    inner: Arc<InboundInner>,
}

#[derive(Default)]
struct InboundInner {
    drain: tokio_util::sync::CancellationToken,
    active: AtomicUsize,
    idle: tokio::sync::Notify,
}

/// Held by one inbound bridge while it runs.
pub(crate) struct InboundBridgeGuard {
    inner: Arc<InboundInner>,
}

impl Drop for InboundBridgeGuard {
    fn drop(&mut self) {
        if self.inner.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.idle.notify_waiters();
        }
    }
}

impl InboundBridges {
    /// Register a bridge before spawning it.
    pub(crate) fn register(&self) -> InboundBridgeGuard {
        self.inner.active.fetch_add(1, Ordering::SeqCst);
        InboundBridgeGuard {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Token cancelled when the bridges should drain what is queued and exit.
    pub(crate) fn drain_token(&self) -> tokio_util::sync::CancellationToken {
        self.inner.drain.clone()
    }

    /// Ask every bridge to drain its queue and exit.
    pub(crate) fn drain(&self) {
        self.inner.drain.cancel();
    }

    /// Wait until every registered bridge has exited.
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Create the outbox for `peer` and spawn its forwarder on `runtime_handle`.
pub(crate) fn spawn_peer_outbox(
    peer: &str,
    transport_tx: FrameSender,
    events: LifecycleEventSender,
    runtime_handle: &tokio::runtime::Handle,
) -> (PeerOutbox, tokio::task::JoinHandle<()>) {
    let (lane, lane_rx) = tokio_mpsc::unbounded_channel();
    let outbox = PeerOutbox {
        peer: Arc::from(peer),
        lane,
        link: Arc::new(PeerLinkState::default()),
        events,
    };
    let handle = runtime_handle.spawn(run_forwarder(
        Arc::clone(&outbox.peer),
        lane_rx,
        transport_tx,
        Arc::clone(&outbox.link),
        outbox.events.clone(),
    ));
    (outbox, handle)
}

/// Drain the lane into the transport in order, awaiting capacity.
///
/// Exits when all outboxes are dropped (after forwarding everything queued)
/// or when the transport rejects a frame. A rejection on a link that is not
/// retired is reported to the coordinator.
async fn run_forwarder(
    peer: Arc<str>,
    mut lane_rx: tokio_mpsc::UnboundedReceiver<Frame>,
    transport_tx: FrameSender,
    link: Arc<PeerLinkState>,
    events: LifecycleEventSender,
) {
    while let Some(frame) = lane_rx.recv().await {
        if transport_tx.send(frame).await.is_err() {
            if !link.is_retired() {
                #[cfg(feature = "tracing")]
                tracing::error!(peer = %peer, "peer outbox: transport closed before end-of-stream");
                report_link_event(
                    &events,
                    &peer,
                    LinkEvent::OutboundFailed(LinkFailure::SendFailed),
                );
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::id::DataflowId;

    fn frame(channel_id: u64) -> Frame {
        Frame {
            dataflow_id: DataflowId::new(),
            channel_id,
            payload: vec![channel_id as u8],
        }
    }

    #[tokio::test]
    async fn forwarder_preserves_order_and_drains_after_producers_drop() {
        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(1);
        let (events, _events_rx) = tokio_mpsc::unbounded_channel();
        let (outbox, handle) = spawn_peer_outbox(
            "peer",
            FrameSender::Direct(tx),
            events,
            &tokio::runtime::Handle::current(),
        );
        for id in 0..50 {
            outbox.send(frame(id)).unwrap();
        }
        drop(outbox);
        let mut seen = Vec::new();
        while let Some(f) = rx.recv().await {
            seen.push(f.channel_id);
        }
        handle.await.unwrap();
        assert_eq!(seen, (0..50).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn forwarder_reports_send_failure_unless_retired() {
        for retired in [false, true] {
            let (tx, rx) = tokio_mpsc::channel::<Frame>(1);
            drop(rx);
            let (events, mut events_rx) = tokio_mpsc::unbounded_channel();
            let (outbox, handle) = spawn_peer_outbox(
                "peer",
                FrameSender::Direct(tx),
                events,
                &tokio::runtime::Handle::current(),
            );
            if retired {
                outbox.link().retire();
            }
            outbox.send(frame(1)).unwrap();
            handle.await.unwrap();
            assert!(outbox.send(frame(2)).is_err(), "lane closes after failure");
            drop(outbox);
            let event = events_rx.recv().await;
            if retired {
                assert!(event.is_none(), "retired link must stay silent: {event:?}");
            } else {
                assert!(matches!(
                    event,
                    Some(LifecycleEvent::Link {
                        ref peer,
                        event: LinkEvent::OutboundFailed(LinkFailure::SendFailed),
                    }) if peer == "peer"
                ));
            }
        }
    }

    #[tokio::test]
    async fn forwarder_waits_for_capacity_instead_of_dropping() {
        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(1);
        let (events, _events_rx) = tokio_mpsc::unbounded_channel();
        let (outbox, handle) = spawn_peer_outbox(
            "peer",
            FrameSender::Direct(tx),
            events,
            &tokio::runtime::Handle::current(),
        );
        for id in 0..10 {
            outbox.send(frame(id)).unwrap();
        }
        drop(outbox);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut seen = Vec::new();
        while let Some(f) = rx.recv().await {
            seen.push(f.channel_id);
            tokio::task::yield_now().await;
        }
        handle.await.unwrap();
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
    }
}
