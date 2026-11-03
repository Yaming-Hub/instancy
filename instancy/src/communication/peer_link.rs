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
//! that as data loss and fails the dataflow. Record sends and retirement are
//! ordered by the link's admission lock (see
//! [`PeerLinkState::try_send_data`]): a record is either enqueued before the
//! link retires, and so ahead of anything queued after retirement such as the
//! acknowledgement, or refused.
//!
//! [`InboundBridges`] tracks the tasks that deliver data frames from peers to
//! local operators. They report frames that can no longer be delivered (the
//! receiving operator already finished) as [`LinkEvent::InboundDataLost`],
//! and teardown lets them finish draining before the dataflow completes.

use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

// Schedule tests (`--cfg instancy_shuttle`, Linux) substitute Shuttle's
// models for the link's flag and admission lock, and for the inbound bridge
// count; see `send_retire_shuttle` in `dataflow::channels::network` and
// `inbound_idle_shuttle` below.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
use shuttle::sync::{
    RwLock,
    atomic::{AtomicBool, AtomicUsize},
};
#[cfg(not(all(test, target_os = "linux", instancy_shuttle)))]
use std::sync::{
    RwLock,
    atomic::{AtomicBool, AtomicUsize},
};

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
    /// Orders record sends with retirement: a record send holds it shared
    /// while it checks the link and enqueues, [`retire`](Self::retire) holds
    /// it exclusively while it marks the link retired.
    admission: RwLock<()>,
    /// A data push to the peer found its queue closed: it backs off on a
    /// frame that can never leave.
    push_blocked: AtomicBool,
    /// The flight recorder's ring of this link (see `progress::trace`).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    trace: std::sync::OnceLock<Arc<crate::progress::trace::Recorder>>,
}

// The schedule tests explore interleavings only if the flag and the lock
// are Shuttle's models.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
const _: for<'a> fn(
    &'a PeerLinkState,
) -> (
    &'a shuttle::sync::atomic::AtomicBool,
    &'a shuttle::sync::RwLock<()>,
) = |link| (&link.retired, &link.admission);

impl PeerLinkState {
    /// Whether the link no longer carries anything either side needs. Once
    /// true, it stays true.
    ///
    /// Reading it before or after enqueueing a record says nothing about
    /// where the record sits relative to the acknowledgement: records are
    /// checked under the admission lock, by
    /// [`try_send_data`](Self::try_send_data).
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Mark the link retired. Idempotent.
    ///
    /// Waits for record sends that are enqueueing to finish, so each record
    /// is enqueued before anything the caller queues once this returns (such
    /// as the acknowledgement), or refused.
    pub(crate) fn retire(&self) {
        traced! {
            if let Some(recorder) = self.recorder() {
                recorder.record(
                    crate::progress::trace::Ev::new(crate::progress::trace::Kind::Retired)
                        .a(usize::from(self.retired.load(Ordering::SeqCst))),
                );
            }
        }
        // The lock guards no data, only the order of sends and retirement,
        // so a poisoned lock is still a valid one.
        let _admission = self
            .admission
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        self.retired.store(true, Ordering::SeqCst);
    }

    /// Enqueue a frame carrying records on `sender`, the transport queue to
    /// this link's peer, unless the link is retired.
    ///
    /// The check and the enqueue happen under the admission lock, so
    /// [`retire`](Self::retire) falls wholly before or after them: an
    /// enqueued record is ahead of anything queued after retirement, and a
    /// record for a retired link is returned as `Closed`, never enqueued.
    /// The lock is held only for the nonblocking `try_send`.
    pub(crate) fn try_send_data(
        &self,
        sender: &FrameSender,
        frame: Frame,
    ) -> Result<(), tokio_mpsc::error::TrySendError<Frame>> {
        let _admission = self
            .admission
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        if self.is_retired() {
            return Err(tokio_mpsc::error::TrySendError::Closed(frame));
        }
        sender.try_send(frame)
    }

    /// Record this link's events into `recorder` (the flight recorder).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    pub(crate) fn attach_trace(&self, recorder: Option<Arc<crate::progress::trace::Recorder>>) {
        if let Some(recorder) = recorder {
            let _ = self.trace.set(recorder);
        }
    }

    /// This link's flight recorder, if it records.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    pub(crate) fn recorder(&self) -> Option<&crate::progress::trace::Recorder> {
        self.trace.get().map(|recorder| &**recorder)
    }

    /// This link's flight recorder, for a task that records into it (the
    /// unit type where the recorder is compiled out).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    pub(crate) fn trace_handle(&self) -> crate::progress::TraceHandle {
        self.trace.get().cloned()
    }

    /// The unit type: the flight recorder is compiled out.
    #[cfg(not(any(debug_assertions, feature = "progress-check", feature = "test-utils")))]
    pub(crate) fn trace_handle(&self) -> crate::progress::TraceHandle {}

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
            #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
            link: Arc::clone(&self.link),
        }
    }
}

/// Reports events about frames received from one peer.
#[derive(Clone)]
pub(crate) struct PeerMonitor {
    peer: Arc<str>,
    events: LifecycleEventSender,
    /// The link's state, for its flight recorder ring.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    link: Arc<PeerLinkState>,
}

impl PeerMonitor {
    /// Report an event on this peer's link.
    pub(crate) fn report(&self, event: LinkEvent) {
        report_link_event(&self.events, &self.peer, event);
    }

    /// The link's flight recorder, if it records.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    pub(crate) fn recorder(&self) -> Option<&crate::progress::trace::Recorder> {
        self.link.recorder()
    }

    /// Records from this peer were lost (`detail`): note it in the link's
    /// ring. Only an observation: the coordinator dumps a loss once its
    /// lifecycle accepts it (a failure or a cancellation may explain it).
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    pub(crate) fn note_lost(&self, detail: &str) {
        if let Some(recorder) = self.recorder() {
            recorder.note(crate::progress::trace::Note::Loss, detail);
        }
    }

    /// A monitor for tests.
    #[cfg(test)]
    pub(crate) fn for_test(peer: &str, events: LifecycleEventSender) -> Self {
        Self {
            peer: Arc::from(peer),
            events,
            #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
            link: Arc::default(),
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

// The schedule test explores interleavings only if the count is Shuttle's
// model.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
const _: fn(&InboundInner) -> &shuttle::sync::atomic::AtomicUsize = |inner| &inner.active;

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

/// Schedule test of the inbound bridges' idle wait, under Shuttle (Linux,
/// `RUSTFLAGS="--cfg instancy_shuttle"`; see `docs/testing/nextest.md`). Two
/// bridges exit on their own threads while teardown waits in Shuttle's
/// `block_on`; the depth-first scheduler runs every interleaving of the
/// bridge count. Tokio's `Notify` is opaque to it: the waiter's `notified()`
/// and `enable()` are one step, and the exits may run between them and its
/// count check. A count check moved before that setup would have no
/// scheduling point between the two, so that order is not explored.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
mod inbound_idle_shuttle {
    use super::*;
    use std::sync::atomic::AtomicUsize as Count;

    /// The wait returns only after the last bridge started to exit, and it
    /// is never left waiting after the last one exited.
    #[test]
    fn wait_idle_returns_after_the_last_bridge_exits() {
        static PASSED: Count = Count::new(0);
        PASSED.store(0, Ordering::Relaxed);
        let mut config = shuttle::Config::new();
        config.stack_size = 0x10_0000;
        config.max_steps = shuttle::MaxSteps::FailAfter(10_000);
        config.max_time = None;
        let scheduler = shuttle::scheduler::DfsScheduler::new(None, false);
        let executions = shuttle::Runner::new(scheduler, config).run(|| {
            let bridges = InboundBridges::default();
            let guards = [bridges.register(), bridges.register()];
            // Bridges that started to exit, counted just before each drops
            // its guard.
            let exiting = Arc::new(AtomicUsize::new(0));
            let exits = guards.map(|guard| {
                let exiting = Arc::clone(&exiting);
                shuttle::thread::spawn(move || {
                    exiting.fetch_add(1, Ordering::SeqCst);
                    drop(guard);
                })
            });
            shuttle::future::block_on(bridges.wait_idle());
            assert_eq!(
                exiting.load(Ordering::SeqCst),
                2,
                "the wait returned before the last bridge started to exit"
            );
            for exit in exits {
                exit.join().unwrap();
            }
            PASSED.fetch_add(1, Ordering::Relaxed);
        });
        println!("{executions} interleavings");
        assert!(executions > 0, "nothing ran");
        assert_eq!(
            PASSED.load(Ordering::Relaxed),
            executions,
            "every interleaving passed"
        );
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

    #[test]
    fn record_sends_release_the_admission_lock_and_stop_at_retirement() {
        use tokio_mpsc::error::TrySendError;
        let link = PeerLinkState::default();
        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(1);
        let sender = FrameSender::Direct(tx);
        link.try_send_data(&sender, frame(1)).unwrap();
        assert!(link.admission.try_write().is_ok(), "released after a send");
        assert!(matches!(
            link.try_send_data(&sender, frame(2)),
            Err(TrySendError::Full(f)) if f.channel_id == 2
        ));
        assert!(link.admission.try_write().is_ok(), "released when full");
        assert_eq!(rx.try_recv().unwrap().channel_id, 1);

        link.retire();
        // Room in the queue, but the link is retired.
        assert!(matches!(
            link.try_send_data(&sender, frame(3)),
            Err(TrySendError::Closed(f)) if f.channel_id == 3
        ));
        assert!(link.admission.try_write().is_ok(), "released when refused");
        assert!(rx.try_recv().is_err(), "nothing enqueued after retirement");
    }

    #[cfg(not(all(test, target_os = "linux", instancy_shuttle)))]
    #[test]
    fn a_poisoned_admission_lock_still_retires_the_link() {
        let link = Arc::new(PeerLinkState::default());
        let poisoner = Arc::clone(&link);
        let _ = std::thread::spawn(move || {
            let _admission = poisoner.admission.write().unwrap();
            panic!("panic while holding the admission lock");
        })
        .join();
        assert!(link.admission.is_poisoned());

        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(4);
        let sender = FrameSender::Direct(tx);
        link.try_send_data(&sender, frame(1)).unwrap();
        link.retire();
        assert!(link.is_retired());
        assert!(matches!(
            link.try_send_data(&sender, frame(2)),
            Err(tokio_mpsc::error::TrySendError::Closed(_))
        ));
        assert_eq!(rx.try_recv().unwrap().channel_id, 1);
        assert!(rx.try_recv().is_err(), "nothing enqueued after retirement");
    }

    // Under `--cfg instancy_shuttle` the bridge count is Shuttle's model,
    // which works only inside a model run.
    #[cfg(not(all(target_os = "linux", instancy_shuttle)))]
    mod inbound_idle {
        use super::*;
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll, Wake, Waker};

        /// A waker that counts its wakes.
        #[derive(Default)]
        struct Wakes(std::sync::atomic::AtomicUsize);

        impl Wake for Wakes {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        /// A `wait_idle` polled by hand, with the wakes it received.
        struct Waiter<'a> {
            wait: Pin<Box<dyn Future<Output = ()> + 'a>>,
            wakes: Arc<Wakes>,
        }

        impl<'a> Waiter<'a> {
            fn new(bridges: &'a InboundBridges) -> Self {
                Self {
                    wait: Box::pin(bridges.wait_idle()),
                    wakes: Arc::default(),
                }
            }

            fn poll(&mut self) -> Poll<()> {
                let waker = Waker::from(Arc::clone(&self.wakes));
                self.wait.as_mut().poll(&mut Context::from_waker(&waker))
            }

            fn wakes(&self) -> usize {
                self.wakes.0.load(Ordering::SeqCst)
            }
        }

        #[test]
        fn idle_without_bridges() {
            let bridges = InboundBridges::default();
            assert_eq!(Waiter::new(&bridges).poll(), Poll::Ready(()));
            drop(bridges.register());
            assert_eq!(
                Waiter::new(&bridges).poll(),
                Poll::Ready(()),
                "the only bridge exited"
            );
        }

        #[test]
        fn waits_for_the_only_bridge() {
            let bridges = InboundBridges::default();
            let guard = bridges.register();
            let mut waiter = Waiter::new(&bridges);
            assert_eq!(waiter.poll(), Poll::Pending, "a bridge is running");
            assert_eq!(waiter.wakes(), 0);
            drop(guard);
            assert_eq!(waiter.wakes(), 1, "the last exit wakes the waiter once");
            assert_eq!(waiter.poll(), Poll::Ready(()));
        }

        /// Only the last exit wakes waiters; an earlier one leaves them be.
        #[test]
        fn waits_for_the_last_of_two_bridges() {
            let bridges = InboundBridges::default();
            let (first, last) = (bridges.register(), bridges.register());
            let mut waiter = Waiter::new(&bridges);
            assert_eq!(waiter.poll(), Poll::Pending);
            drop(first);
            assert_eq!(waiter.wakes(), 0, "only the last exit wakes waiters");
            assert_eq!(waiter.poll(), Poll::Pending, "a bridge is still running");
            let mut late = Waiter::new(&bridges);
            assert_eq!(late.poll(), Poll::Pending, "a bridge is still running");
            drop(last);
            assert_eq!(waiter.wakes(), 1, "the last exit wakes the waiter once");
            assert_eq!(late.wakes(), 1, "the last exit wakes the later waiter once");
            assert_eq!(waiter.poll(), Poll::Ready(()));
            assert_eq!(late.poll(), Poll::Ready(()));
        }

        #[test]
        fn the_last_exit_wakes_every_waiter() {
            let bridges = InboundBridges::default();
            let guard = bridges.register();
            let mut waiters = [Waiter::new(&bridges), Waiter::new(&bridges)];
            for waiter in &mut waiters {
                assert_eq!(waiter.poll(), Poll::Pending);
                assert_eq!(waiter.wakes(), 0);
            }
            drop(guard);
            for waiter in &waiters {
                assert_eq!(waiter.wakes(), 1, "every waiter is woken once");
            }
            for waiter in &mut waiters {
                assert_eq!(waiter.poll(), Poll::Ready(()));
            }
        }
    }
}
