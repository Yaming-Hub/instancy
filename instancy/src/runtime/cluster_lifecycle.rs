//! Cluster dataflow lifecycle: the clean-shutdown handshake and teardown.
//!
//! One coordinator task runs per cluster dataflow on each node. It owns the
//! transport, the shared-manager registration and the network bridges, so
//! teardown no longer depends on the user's join handle. See
//! `docs/design/cluster.md` §5.5.3 for the protocol.
//!
//! # Protocol summary
//!
//! - When every local worker has finished with `Ok(true)`, the node queues an
//!   end-of-stream marker on every progress channel to every peer, behind
//!   everything it has sent so far (the per-peer lanes are FIFO).
//! - A node that has received all end-of-stream markers from a peer replies
//!   with `EndOfStreamAck` on the control channel.
//! - A link is *retired* once we have received the peer's markers (the peer
//!   is done and needs nothing from us) or the peer has acknowledged ours (it
//!   holds everything we sent). Events on a retired link are benign.
//! - The node tears down once every link is retired, or once a link that is
//!   not retired has failed for good.
//!
//! The decision logic lives in [`LifecycleMachine`], a pure state machine;
//! [`ClusterLifecycle::run`] executes its actions.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::mpsc as tokio_mpsc;

use crate::cancellation::{CancellationReason, CancellationToken};
use crate::communication::cluster_transport::ClusterTransport;
use crate::communication::control_protocol::{
    ControlMessage, broadcast_cancel_with_transport, encode_control_message,
};
use crate::communication::peer_link::{
    LifecycleEvent, LinkEvent, LinkFailure, PeerLinkState, PeerOutbox,
};
use crate::communication::shared_transport::SharedPeerManager;
use crate::communication::transport::{Frame, TransportError};
use crate::communication::transport_session::CONTROL_CHANNEL_ID;
use crate::dataflow::id::DataflowId;
use crate::progress::network_progress::{
    NetworkProgressHandles, encode_progress_end_of_stream, progress_channel_id,
};

// ---------------------------------------------------------------------------
// Teardown signal
// ---------------------------------------------------------------------------

struct TeardownShared {
    state: Mutex<TeardownState>,
    condvar: Condvar,
}

struct TeardownState {
    done: bool,
    waker: Option<Waker>,
}

/// Completes the teardown signal when dropped (including when the
/// coordinator task is dropped because its runtime shut down).
pub(crate) struct TeardownNotifier {
    shared: Arc<TeardownShared>,
}

/// Waits until the coordinator has finished tearing the dataflow down.
pub(crate) struct TeardownWaiter {
    shared: Arc<TeardownShared>,
}

/// Create a connected teardown notifier/waiter pair.
pub(crate) fn teardown_signal() -> (TeardownNotifier, TeardownWaiter) {
    let shared = Arc::new(TeardownShared {
        state: Mutex::new(TeardownState {
            done: false,
            waker: None,
        }),
        condvar: Condvar::new(),
    });
    (
        TeardownNotifier {
            shared: Arc::clone(&shared),
        },
        TeardownWaiter { shared },
    )
}

impl Drop for TeardownNotifier {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.done = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        self.shared.condvar.notify_all();
    }
}

impl TeardownWaiter {
    /// Poll for teardown completion, registering the task's waker.
    pub(crate) fn poll_done(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.done {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Block the current thread until teardown completes.
    pub(crate) fn wait(&self) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.done {
            state = self
                .shared
                .condvar
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Coordinator phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Local workers are running.
    Running,
    /// All local workers succeeded and our end-of-stream markers are queued;
    /// waiting for every link to retire.
    Draining,
    /// Teardown has been requested; nothing further happens.
    Finished,
}

/// Side effect requested by the state machine.
#[derive(Debug)]
pub(crate) enum Action {
    /// Acknowledge that we received the peer's whole stream.
    SendAck(String),
    /// Tell every peer that this dataflow was cancelled (bounded).
    BroadcastCancel,
    /// Queue end-of-stream markers behind everything else and release the
    /// coordinator's lane handles.
    SendGoodbye,
    /// A link failed while local workers still need it.
    ///
    /// Current behaviour mirrors the pre-handshake runtime: bridges are torn
    /// down, and a shared-transport failure also cancels the dataflow. PR8
    /// makes every such failure cancel the dataflow cluster-wide.
    LinkFailed { peer: String, failure: LinkFailure },
    /// Release bridges, transport and registrations. `graceful` keeps the
    /// outbox forwarders draining instead of aborting them.
    Teardown { graceful: bool },
}

#[derive(Debug)]
struct PeerState {
    expected_end_of_stream: usize,
    end_of_stream: usize,
    progress_channels_ended: usize,
    goodbye_received: bool,
    ack_received: bool,
    control_closed: bool,
    lost: bool,
    link: Arc<PeerLinkState>,
}

impl PeerState {
    fn retired(&self) -> bool {
        self.goodbye_received || self.ack_received
    }

    fn resolved(&self) -> bool {
        self.retired() || self.lost
    }

    /// Every channel from the peer has ended, so no ack can still be in
    /// flight behind them.
    fn fully_closed(&self) -> bool {
        self.control_closed && self.progress_channels_ended >= self.expected_end_of_stream
    }
}

/// Pure decision logic of the lifecycle coordinator.
#[derive(Debug)]
pub(crate) struct LifecycleMachine {
    phase: Phase,
    workers_remaining: usize,
    all_workers_ok: bool,
    goodbye_sent: bool,
    cancel_broadcast: bool,
    peers: BTreeMap<String, PeerState>,
}

impl LifecycleMachine {
    /// `peers`: `(peer id, end-of-stream markers expected from it, link state)`.
    /// A peer sends one marker per (peer worker, local worker) progress channel.
    pub(crate) fn new(
        num_local_workers: usize,
        peers: impl IntoIterator<Item = (String, usize, Arc<PeerLinkState>)>,
    ) -> Self {
        Self {
            phase: Phase::Running,
            workers_remaining: num_local_workers,
            all_workers_ok: true,
            goodbye_sent: false,
            cancel_broadcast: false,
            peers: peers
                .into_iter()
                .map(|(peer, expected_end_of_stream, link)| {
                    (
                        peer,
                        PeerState {
                            expected_end_of_stream,
                            end_of_stream: 0,
                            progress_channels_ended: 0,
                            goodbye_received: false,
                            ack_received: false,
                            control_closed: false,
                            lost: false,
                            link,
                        },
                    )
                })
                .collect(),
        }
    }

    #[cfg(test)]
    fn phase(&self) -> Phase {
        self.phase
    }

    /// Whether some peer was given up on without retiring its link (it
    /// cancelled, or its link failed). Such a peer may still need frames we
    /// queued for it, e.g. while it drains after a cancellation.
    pub(crate) fn has_unretired_peers(&self) -> bool {
        self.peers.values().any(|peer| !peer.retired())
    }

    /// Whether every peer has acknowledged our stream or finished its own.
    #[cfg(test)]
    fn all_retired(&self) -> bool {
        self.peers.values().all(PeerState::retired)
    }

    pub(crate) fn on_event(&mut self, event: LifecycleEvent) -> Vec<Action> {
        match event {
            LifecycleEvent::WorkerFinished { success } => self.on_worker_finished(success),
            LifecycleEvent::Link { peer, event } => self.on_link_event(peer, event),
        }
    }

    fn on_worker_finished(&mut self, success: bool) -> Vec<Action> {
        if self.workers_remaining == 0 {
            return Vec::new();
        }
        self.workers_remaining -= 1;
        self.all_workers_ok &= success;
        if self.workers_remaining > 0 || self.phase != Phase::Running {
            return Vec::new();
        }
        if self.all_workers_ok {
            self.phase = Phase::Draining;
            self.goodbye_sent = true;
            let mut actions = vec![Action::SendGoodbye];
            if self.cancel_broadcast {
                // We already told every peer the dataflow is cancelled, and a
                // peer that receives our Cancel stops waiting for us (see
                // `CancelReceived`), so no acknowledgement is coming. The
                // goodbye is still queued so a peer that misses the Cancel
                // can retire us on our end-of-stream.
                for peer in self.peers.values_mut() {
                    if !peer.retired() {
                        peer.lost = true;
                    }
                }
            }
            actions.extend(self.finish_if_resolved());
            actions
        } else {
            let mut actions = Vec::new();
            if !self.cancel_broadcast {
                self.cancel_broadcast = true;
                actions.push(Action::BroadcastCancel);
            }
            self.phase = Phase::Finished;
            actions.push(Action::Teardown { graceful: false });
            actions
        }
    }

    /// The local dataflow token was cancelled.
    ///
    /// Before local completion this propagates the cancellation to every
    /// peer; if the local workers then still succeed, the coordinator does not
    /// wait for acknowledgements (peers stop waiting for a node whose `Cancel`
    /// they received). After our end-of-stream is committed it has no effect:
    /// our part of the dataflow is complete and the handshake still finishes.
    pub(crate) fn on_dataflow_cancelled(&mut self) -> Vec<Action> {
        if self.phase == Phase::Running && !self.cancel_broadcast {
            self.cancel_broadcast = true;
            vec![Action::BroadcastCancel]
        } else {
            Vec::new()
        }
    }

    /// Bridges were cancelled from outside (e.g. `report_node_leave`).
    pub(crate) fn on_external_abort(&mut self) -> Vec<Action> {
        if self.phase == Phase::Finished {
            return Vec::new();
        }
        self.phase = Phase::Finished;
        vec![Action::Teardown { graceful: false }]
    }

    /// Every event source is gone; nothing further can arrive.
    pub(crate) fn on_events_closed(&mut self) -> Vec<Action> {
        if self.phase == Phase::Finished {
            return Vec::new();
        }
        let graceful = self.phase == Phase::Draining;
        for peer in self.peers.values_mut() {
            if !peer.retired() {
                peer.lost = true;
            }
        }
        self.phase = Phase::Finished;
        vec![Action::Teardown { graceful }]
    }

    fn on_link_event(&mut self, peer_id: String, event: LinkEvent) -> Vec<Action> {
        let phase = self.phase;
        let goodbye_sent = self.goodbye_sent;
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return Vec::new();
        };
        let mut actions = Vec::new();
        match event {
            LinkEvent::ProgressEndOfStream => {
                peer.end_of_stream += 1;
                peer.progress_channels_ended += 1;
                if peer.end_of_stream == peer.expected_end_of_stream && !peer.goodbye_received {
                    peer.goodbye_received = true;
                    peer.link.retire();
                    if phase != Phase::Finished {
                        actions.push(Action::SendAck(peer_id.clone()));
                    }
                }
            }
            LinkEvent::AckReceived => {
                // An ack before our end-of-stream would be a protocol violation.
                if goodbye_sent {
                    peer.ack_received = true;
                    peer.link.retire();
                }
            }
            LinkEvent::CancelReceived => {
                // The peer abandoned the dataflow: it will not send an
                // end-of-stream or acknowledge ours, and (in Shared mode) it
                // may already have unregistered, so nothing from it can
                // resolve the link later. Remember this in every phase so a
                // local success that races with the cancellation does not
                // wait for it.
                if !peer.retired() {
                    peer.lost = true;
                }
            }
            LinkEvent::ControlClosed => {
                peer.control_closed = true;
                if phase == Phase::Draining && !peer.retired() && peer.fully_closed() {
                    peer.lost = true;
                }
            }
            LinkEvent::ControlUndecodable(_detail) => {
                // A corrupted ack can never be retransmitted. While workers
                // run this keeps the previous behaviour (ignored).
                if phase == Phase::Draining && !peer.retired() {
                    peer.lost = true;
                }
            }
            LinkEvent::ProgressChannelEnded(failure) => {
                peer.progress_channels_ended += 1;
                let hard = !matches!(failure, LinkFailure::ChannelClosed);
                actions.extend(self.on_link_failure(peer_id, failure, hard));
            }
            LinkEvent::OutboundFailed(failure) => {
                // Our end-of-stream is the last frame on the lane, so a failed
                // send means the peer can no longer receive it (or anything
                // before it) and will never acknowledge.
                actions.extend(self.on_link_failure(peer_id, failure, true));
            }
            LinkEvent::TransportError(error) => {
                actions.extend(self.on_link_failure(peer_id, LinkFailure::Transport(error), true));
            }
        }
        actions.extend(self.finish_if_resolved());
        actions
    }

    /// A link ended without a clean end-of-stream.
    ///
    /// `hard` failures (corruption, transport errors, our end-of-stream not
    /// being sendable) mean nothing more can be expected from the peer. An
    /// EOF on one channel is soft: an ack may still be queued behind it on the
    /// control channel, so during draining the peer is only declared lost once
    /// every channel from it has closed.
    fn on_link_failure(
        &mut self,
        peer_id: String,
        failure: LinkFailure,
        hard: bool,
    ) -> Vec<Action> {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return Vec::new();
        };
        if peer.resolved() {
            // Retired: neither side needs this link any more.
            return Vec::new();
        }
        match self.phase {
            Phase::Running if self.cancel_broadcast => {
                // The dataflow is already being cancelled; peers tearing down
                // is expected. Wait for the local workers instead.
                peer.lost = true;
                Vec::new()
            }
            Phase::Running => {
                peer.lost = true;
                let cancel_dataflow = matches!(failure, LinkFailure::Transport(_));
                let mut actions = vec![Action::LinkFailed {
                    peer: peer_id,
                    failure,
                }];
                if cancel_dataflow && !self.cancel_broadcast {
                    self.cancel_broadcast = true;
                    actions.push(Action::BroadcastCancel);
                }
                self.phase = Phase::Finished;
                actions.push(Action::Teardown { graceful: false });
                actions
            }
            Phase::Draining => {
                if hard || peer.fully_closed() {
                    peer.lost = true;
                }
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    peer = %peer_id,
                    ?failure,
                    "link failed after local completion, before the peer confirmed our end-of-stream"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = (peer_id, failure);
                Vec::new()
            }
            Phase::Finished => Vec::new(),
        }
    }

    fn finish_if_resolved(&mut self) -> Vec<Action> {
        if self.phase == Phase::Draining && self.peers.values().all(PeerState::resolved) {
            self.phase = Phase::Finished;
            vec![Action::Teardown { graceful: true }]
        } else {
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Async driver
// ---------------------------------------------------------------------------

/// Everything a cluster dataflow needs until teardown, plus the coordinator
/// state. Field order matters: `_teardown` is dropped last so waiters resolve
/// only after every resource has been released.
pub(crate) struct ClusterLifecycle {
    pub(crate) machine: LifecycleMachine,
    pub(crate) events: tokio_mpsc::UnboundedReceiver<LifecycleEvent>,
    pub(crate) dataflow_id: DataflowId,
    pub(crate) dataflow_cancel: CancellationToken,
    pub(crate) bridge_cancel: tokio_util::sync::CancellationToken,
    pub(crate) local_workers: (usize, usize),
    pub(crate) peer_workers: HashMap<String, (usize, usize)>,
    pub(crate) total_workers: usize,
    pub(crate) cancel_broadcast_timeout: Duration,
    pub(crate) outboxes: HashMap<String, PeerOutbox>,
    pub(crate) forwarders: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) progress_handles: Option<NetworkProgressHandles>,
    pub(crate) dispatcher: Option<tokio::task::JoinHandle<()>>,
    pub(crate) peer_registry: Arc<super::PeerRegistry>,
    pub(crate) peer_registration: Option<u64>,
    pub(crate) shared_managers: Option<Arc<HashMap<String, SharedPeerManager>>>,
    pub(crate) transport: Arc<ClusterTransport>,
    /// Completes waiters when dropped, after every field above.
    pub(crate) _teardown: TeardownNotifier,
}

impl ClusterLifecycle {
    /// Drive the state machine until teardown.
    pub(crate) async fn run(mut self) {
        let mut cancel_seen = false;
        loop {
            let actions = tokio::select! {
                biased;
                // Before bridge_cancel so a cancellation that arrives with an
                // external abort is still broadcast first.
                _ = self.dataflow_cancel.cancelled_async(), if !cancel_seen => {
                    cancel_seen = true;
                    // Apply worker results that are already queued first: a
                    // cancellation racing with local success must not be
                    // broadcast once every worker has succeeded.
                    let mut actions = Vec::new();
                    while let Ok(event) = self.events.try_recv() {
                        actions.extend(self.machine.on_event(event));
                    }
                    actions.extend(self.machine.on_dataflow_cancelled());
                    actions
                }
                _ = self.bridge_cancel.cancelled() => self.machine.on_external_abort(),
                event = self.events.recv() => match event {
                    Some(event) => self.machine.on_event(event),
                    None => self.machine.on_events_closed(),
                },
            };
            for action in actions {
                if self.perform(action).await {
                    return;
                }
            }
        }
    }

    /// Execute one action; returns `true` once teardown is complete.
    async fn perform(&mut self, action: Action) -> bool {
        match action {
            Action::SendAck(peer) => {
                if let Some(control) = self.transport.control_sender(&peer).cloned() {
                    let frame = Frame {
                        dataflow_id: self.dataflow_id,
                        channel_id: CONTROL_CHANNEL_ID,
                        payload: encode_control_message(&ControlMessage::EndOfStreamAck),
                    };
                    // Never block the coordinator on a congested control queue.
                    tokio::spawn(async move {
                        let _ = control.send(frame).await;
                    });
                }
                false
            }
            Action::BroadcastCancel => {
                let reason = self
                    .dataflow_cancel
                    .reason()
                    .map(|reason| reason.to_string())
                    .unwrap_or_else(|| "cluster worker failed".to_string());
                let _ = tokio::time::timeout(
                    self.cancel_broadcast_timeout,
                    broadcast_cancel_with_transport(&self.transport, self.dataflow_id, &reason),
                )
                .await;
                false
            }
            Action::SendGoodbye => {
                self.send_goodbye();
                false
            }
            Action::LinkFailed { peer, failure } => {
                #[cfg(feature = "tracing")]
                tracing::error!(peer = %peer, ?failure, "cluster link failed while workers were running");
                if let LinkFailure::Transport(error) = &failure {
                    let reason = if matches!(error, TransportError::LockPoisoned { .. }) {
                        CancellationReason::InternalError {
                            detail: format!("transport lock poisoned for peer {peer}"),
                        }
                    } else {
                        CancellationReason::WorkerFailed {
                            detail: format!("shared transport failure from peer {peer}"),
                        }
                    };
                    self.dataflow_cancel.cancel_with_reason(reason);
                }
                false
            }
            Action::Teardown { graceful } => {
                self.teardown(graceful).await;
                true
            }
        }
    }

    /// Queue an end-of-stream marker on every progress channel to every peer.
    ///
    /// Workers' executors (and with them every data push and progress sender)
    /// are dropped before their completion is reported, so these markers are
    /// the last frames on each lane.
    fn send_goodbye(&mut self) {
        let payload = encode_progress_end_of_stream();
        for (peer, outbox) in self.outboxes.drain() {
            let Some(&(peer_start, peer_end)) = self.peer_workers.get(&peer) else {
                continue;
            };
            for src in self.local_workers.0..self.local_workers.1 {
                for dst in peer_start..peer_end {
                    let Ok(channel_id) = progress_channel_id(src, dst, self.total_workers) else {
                        continue;
                    };
                    let _ = outbox.send(Frame {
                        dataflow_id: self.dataflow_id,
                        channel_id,
                        payload: payload.clone(),
                    });
                }
            }
        }
    }

    async fn teardown(&mut self, graceful: bool) {
        self.bridge_cancel.cancel();
        if let Some(id) = self.peer_registration.take() {
            self.peer_registry.unregister(id);
        }
        self.outboxes.clear();
        let forwarders = std::mem::take(&mut self.forwarders);
        let managers = self.shared_managers.take();
        if !graceful {
            for forwarder in &forwarders {
                forwarder.abort();
            }
        } else if self.machine.has_unretired_peers() {
            // A peer we stopped waiting for (it cancelled, e.g. and is still
            // draining) may need what we queued for it. Hand our lanes to the
            // transport and, in Shared mode, let the bridges sequence those
            // frames before unregistering, which would drop them. Bounded:
            // the peer may be unreachable.
            let dataflow_id = self.dataflow_id;
            let flush = async {
                for forwarder in forwarders {
                    let _ = forwarder.await;
                }
                if let Some(managers) = &managers {
                    for manager in managers.values() {
                        manager.flush_dataflow(&dataflow_id).await;
                    }
                }
            };
            let _ = tokio::time::timeout(self.cancel_broadcast_timeout, flush).await;
        }
        if let Some(managers) = managers {
            for manager in managers.values() {
                manager.unregister_dataflow(&self.dataflow_id).await;
            }
        }
        self.progress_handles = None;
        self.dispatcher = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine(
        workers: usize,
        peers: &[(&str, usize)],
    ) -> (LifecycleMachine, Vec<Arc<PeerLinkState>>) {
        let links: Vec<_> = peers
            .iter()
            .map(|_| Arc::new(PeerLinkState::default()))
            .collect();
        let machine = LifecycleMachine::new(
            workers,
            peers
                .iter()
                .zip(&links)
                .map(|((peer, eos), link)| (peer.to_string(), *eos, Arc::clone(link))),
        );
        (machine, links)
    }

    fn link_event(peer: &str, event: LinkEvent) -> LifecycleEvent {
        LifecycleEvent::Link {
            peer: peer.to_string(),
            event,
        }
    }

    fn done(success: bool) -> LifecycleEvent {
        LifecycleEvent::WorkerFinished { success }
    }

    fn eof(peer: &str) -> LifecycleEvent {
        link_event(
            peer,
            LinkEvent::ProgressChannelEnded(LinkFailure::ChannelClosed),
        )
    }

    fn is_teardown(actions: &[Action], graceful: bool) -> bool {
        matches!(actions.last(), Some(Action::Teardown { graceful: g }) if *g == graceful)
    }

    #[test]
    fn success_waits_for_ack_then_tears_down() {
        let (mut m, links) = machine(2, &[("b", 1)]);
        assert!(m.on_event(done(true)).is_empty());
        let actions = m.on_event(done(true));
        assert!(matches!(actions.as_slice(), [Action::SendGoodbye]));
        assert_eq!(m.phase(), Phase::Draining);
        // Local cancellation after commit changes nothing.
        assert!(m.on_dataflow_cancelled().is_empty());
        let actions = m.on_event(link_event("b", LinkEvent::AckReceived));
        assert!(is_teardown(&actions, true));
        assert!(links[0].is_retired());
        assert_eq!(m.phase(), Phase::Finished);
    }

    #[test]
    fn peer_goodbye_retires_link_and_is_acked_even_while_running() {
        let (mut m, links) = machine(1, &[("b", 2)]);
        assert!(
            m.on_event(link_event("b", LinkEvent::ProgressEndOfStream))
                .is_empty()
        );
        assert!(!links[0].is_retired());
        let actions = m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(matches!(actions.as_slice(), [Action::SendAck(p)] if p == "b"));
        assert!(links[0].is_retired());
        // EOF after the goodbye is benign.
        assert!(m.on_event(eof("b")).is_empty());
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        assert_eq!(m.phase(), Phase::Running);
        // Local completion tears down immediately: the only link is retired.
        let actions = m.on_event(done(true));
        assert!(matches!(actions.first(), Some(Action::SendGoodbye)));
        assert!(is_teardown(&actions, true));
    }

    #[test]
    fn ack_before_goodbye_is_ignored() {
        let (mut m, links) = machine(1, &[("b", 1)]);
        assert!(
            m.on_event(link_event("b", LinkEvent::AckReceived))
                .is_empty()
        );
        assert!(!links[0].is_retired());
        let actions = m.on_event(done(true));
        assert!(matches!(actions.as_slice(), [Action::SendGoodbye]));
    }

    #[test]
    fn three_peers_retire_in_every_order() {
        let peers = ["b", "c", "d"];
        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        for order in orders {
            for local_first in [true, false] {
                let (mut m, links) = machine(1, &[("b", 1), ("c", 1), ("d", 1)]);
                let mut teardowns = 0;
                if local_first {
                    let actions = m.on_event(done(true));
                    assert!(!is_teardown(&actions, true));
                }
                for (i, &p) in order.iter().enumerate() {
                    // Mix "peer finished" with "peer acked our stream".
                    let event = if i % 2 == 0 || !local_first {
                        LinkEvent::ProgressEndOfStream
                    } else {
                        LinkEvent::AckReceived
                    };
                    let actions = m.on_event(link_event(peers[p], event));
                    if i + 1 < order.len() {
                        assert!(!is_teardown(&actions, true), "early teardown {order:?}");
                    }
                    teardowns += usize::from(is_teardown(&actions, true));
                }
                if !local_first {
                    let actions = m.on_event(done(true));
                    teardowns += usize::from(is_teardown(&actions, true));
                }
                assert_eq!(teardowns, 1, "order {order:?} local_first={local_first}");
                assert!(m.all_retired());
                assert!(links.iter().all(|l| l.is_retired()));
            }
        }
    }

    #[test]
    fn worker_failure_broadcasts_cancel_once_and_tears_down() {
        let (mut m, _) = machine(2, &[("b", 1)]);
        let actions = m.on_dataflow_cancelled();
        assert!(matches!(actions.as_slice(), [Action::BroadcastCancel]));
        assert!(m.on_event(done(false)).is_empty());
        let actions = m.on_event(done(true));
        assert!(matches!(
            actions.as_slice(),
            [Action::Teardown { graceful: false }]
        ));
    }

    #[test]
    fn quiescent_worker_without_cancellation_still_notifies_peers() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        let actions = m.on_event(done(false));
        assert!(matches!(
            actions.as_slice(),
            [
                Action::BroadcastCancel,
                Action::Teardown { graceful: false }
            ]
        ));
    }

    #[test]
    fn link_failure_while_running_keeps_legacy_behaviour() {
        let (mut m, _) = machine(1, &[("b", 1), ("c", 1)]);
        let actions = m.on_event(eof("b"));
        assert!(matches!(
            actions.as_slice(),
            [
                Action::LinkFailed { .. },
                Action::Teardown { graceful: false }
            ]
        ));
        let (mut m, _) = machine(1, &[("b", 1)]);
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::TransportError(TransportError::ReorderTimeout),
        ));
        assert!(matches!(
            actions.as_slice(),
            [
                Action::LinkFailed { .. },
                Action::BroadcastCancel,
                Action::Teardown { graceful: false }
            ]
        ));
    }

    #[test]
    fn link_failure_during_cancellation_waits_for_workers() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_dataflow_cancelled().as_slice(),
            [Action::BroadcastCancel]
        ));
        assert!(m.on_event(eof("b")).is_empty());
        assert_eq!(m.phase(), Phase::Running);
        assert!(matches!(
            m.on_event(done(false)).as_slice(),
            [Action::Teardown { graceful: false }]
        ));
    }

    /// A peer cancels, our cancellation is propagated, and then our workers
    /// still succeed. The peer tore down without a goodbye; in Shared mode
    /// nothing else from it can arrive, so we must not wait for it.
    #[test]
    fn peer_cancel_while_running_then_local_success_does_not_wait_for_peer() {
        for cancel_first in [true, false] {
            let (mut m, links) = machine(1, &[("b", 1), ("c", 1)]);
            if cancel_first {
                assert!(matches!(
                    m.on_dataflow_cancelled().as_slice(),
                    [Action::BroadcastCancel]
                ));
            }
            assert!(
                m.on_event(link_event("b", LinkEvent::CancelReceived))
                    .is_empty()
            );
            assert_eq!(m.phase(), Phase::Running);
            // A later link failure from the cancelled peer is not a new failure.
            assert!(m.on_event(eof("b")).is_empty());
            assert_eq!(m.phase(), Phase::Running);
            let actions = m.on_event(done(true));
            assert!(matches!(actions.first(), Some(Action::SendGoodbye)));
            if cancel_first {
                // We broadcast Cancel: c stops waiting for us, so we don't wait for c.
                assert!(is_teardown(&actions, true));
            } else {
                assert_eq!(m.phase(), Phase::Draining);
                let actions = m.on_event(link_event("c", LinkEvent::AckReceived));
                assert!(is_teardown(&actions, true));
            }
            assert!(!links[0].is_retired());
        }
    }

    /// After broadcasting Cancel, a local success tears down without waiting
    /// for acknowledgements; peers that already said goodbye stay retired.
    #[test]
    fn local_success_after_broadcasting_cancel_does_not_wait_for_acks() {
        let (mut m, links) = machine(2, &[("b", 1), ("c", 1)]);
        let actions = m.on_event(link_event("c", LinkEvent::ProgressEndOfStream));
        assert!(matches!(actions.as_slice(), [Action::SendAck(p)] if p == "c"));
        assert!(matches!(
            m.on_dataflow_cancelled().as_slice(),
            [Action::BroadcastCancel]
        ));
        assert!(m.on_event(done(true)).is_empty());
        let actions = m.on_event(done(true));
        assert!(matches!(
            actions.as_slice(),
            [Action::SendGoodbye, Action::Teardown { graceful: true }]
        ));
        assert!(!links[0].is_retired());
        assert!(links[1].is_retired());
    }

    #[test]
    fn eof_after_ack_during_draining_is_benign() {
        let (mut m, _) = machine(1, &[("b", 2), ("c", 1)]);
        m.on_event(done(true));
        assert!(
            m.on_event(link_event("b", LinkEvent::AckReceived))
                .is_empty()
        );
        assert!(m.on_event(eof("b")).is_empty());
        assert!(m.on_event(eof("b")).is_empty());
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        let actions = m.on_event(link_event("c", LinkEvent::ProgressEndOfStream));
        assert!(matches!(
            actions.as_slice(),
            [Action::SendAck(_), Action::Teardown { graceful: true }]
        ));
    }

    #[test]
    fn send_failure_while_draining_resolves_peer() {
        let (mut m, links) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        // Our end-of-stream could not be handed to the transport: b will
        // never acknowledge it.
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::OutboundFailed(LinkFailure::SendFailed),
        ));
        assert!(is_teardown(&actions, true));
        assert!(!links[0].is_retired());
    }

    #[test]
    fn undecodable_control_message_resolves_peer_only_while_draining() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        let corrupt = || link_event("b", LinkEvent::ControlUndecodable("crc".into()));
        assert!(m.on_event(corrupt()).is_empty());
        assert_eq!(m.phase(), Phase::Running);
        m.on_event(done(true));
        let actions = m.on_event(corrupt());
        assert!(is_teardown(&actions, true));
    }

    #[test]
    fn peer_closing_without_goodbye_or_ack_is_lost_after_all_channels_close() {
        let (mut m, links) = machine(1, &[("b", 2)]);
        m.on_event(done(true));
        assert!(m.on_event(eof("b")).is_empty());
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        let actions = m.on_event(eof("b"));
        assert!(is_teardown(&actions, true));
        assert!(!links[0].is_retired());
    }

    #[test]
    fn hard_failure_or_peer_cancel_during_draining_resolves_peer() {
        for event in [
            LinkEvent::TransportError(TransportError::ConnectionClosed),
            LinkEvent::CancelReceived,
            LinkEvent::ProgressChannelEnded(LinkFailure::Decode("crc".into())),
        ] {
            let (mut m, _) = machine(1, &[("b", 1), ("c", 1)]);
            m.on_event(done(true));
            assert!(m.on_event(link_event("b", event)).is_empty());
            let actions = m.on_event(link_event("c", LinkEvent::AckReceived));
            assert!(is_teardown(&actions, true));
        }
    }

    #[test]
    fn retired_link_ignores_later_failures_while_running() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        for event in [
            LinkEvent::TransportError(TransportError::ReorderTimeout),
            LinkEvent::OutboundFailed(LinkFailure::SendFailed),
            LinkEvent::CancelReceived,
        ] {
            assert!(m.on_event(link_event("b", event)).is_empty());
        }
        assert_eq!(m.phase(), Phase::Running);
    }

    #[test]
    fn external_abort_and_closed_events_end_the_machine_once() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_external_abort().as_slice(),
            [Action::Teardown { graceful: false }]
        ));
        assert!(m.on_external_abort().is_empty());
        assert!(m.on_event(done(true)).is_empty());

        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        assert!(matches!(
            m.on_events_closed().as_slice(),
            [Action::Teardown { graceful: true }]
        ));
    }

    #[test]
    fn no_peers_tears_down_on_local_success() {
        let (mut m, _) = machine(1, &[]);
        let actions = m.on_event(done(true));
        assert!(matches!(
            actions.as_slice(),
            [Action::SendGoodbye, Action::Teardown { graceful: true }]
        ));
    }

    #[test]
    fn teardown_signal_resolves_on_drop() {
        let (notifier, waiter) = teardown_signal();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiter.poll_done(&mut cx).is_pending());
        let handle = std::thread::spawn(move || {
            waiter.wait();
        });
        drop(notifier);
        handle.join().unwrap();
    }
}
