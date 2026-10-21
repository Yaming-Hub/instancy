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
//!   with an acknowledgement, sent in-band on its payload lane so that the
//!   peer sees it only after every data frame sent before it.
//! - A link is *retired* once we have received the peer's markers (the peer
//!   is done and needs nothing from us) or the peer has acknowledged ours (it
//!   holds everything we sent). Events on a retired link are benign; data
//!   for a retired link is data loss and fails the dataflow.
//! - The node tears down once every link is retired, or once a link that is
//!   not retired has failed for good.
//! - A link that fails while the local workers still run cancels the
//!   dataflow on every reachable node. Records that reach a node after the
//!   operator they were addressed to finished fail the dataflow too.
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
use crate::communication::control_protocol::broadcast_cancel_with_transport;
use crate::communication::peer_link::{
    InboundBridges, LifecycleEvent, LinkEvent, LinkFailure, PeerLinkState, PeerOutbox,
};
use crate::communication::shared_transport::SharedPeerManager;
use crate::communication::transport::{Frame, TransportError};
use crate::dataflow::id::DataflowId;
use crate::error::{Error, Result, RuntimeError};
use crate::progress::network_progress::{
    NetworkProgressHandles, encode_progress_ack, encode_progress_end_of_stream, progress_channel_id,
};

// ---------------------------------------------------------------------------
// Teardown signal
// ---------------------------------------------------------------------------

/// How long a blocking wait probes the transport runtime for liveness.
const BLOCKING_WAIT_PROBE: Duration = Duration::from_secs(2);

struct TeardownShared {
    state: Mutex<TeardownState>,
    condvar: Condvar,
    /// Runtime hosting the transport and the coordinator task.
    runtime: tokio::runtime::Handle,
}

struct TeardownState {
    done: bool,
    waker: Option<Waker>,
    data_loss: Option<DataLoss>,
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

/// Create a connected teardown notifier/waiter pair for a coordinator
/// running on `runtime`.
pub(crate) fn teardown_signal(
    runtime: tokio::runtime::Handle,
) -> (TeardownNotifier, TeardownWaiter) {
    let shared = Arc::new(TeardownShared {
        state: Mutex::new(TeardownState {
            done: false,
            waker: None,
            data_loss: None,
        }),
        condvar: Condvar::new(),
        runtime,
    });
    (
        TeardownNotifier {
            shared: Arc::clone(&shared),
        },
        TeardownWaiter { shared },
    )
}

fn lock_state(shared: &TeardownShared) -> std::sync::MutexGuard<'_, TeardownState> {
    shared
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl TeardownNotifier {
    /// Record that records sent to this node were lost; the completion
    /// reports it even though the local workers succeeded.
    pub(crate) fn set_data_loss(&self, loss: DataLoss) {
        lock_state(&self.shared).data_loss = Some(loss);
    }
}

impl Drop for TeardownNotifier {
    fn drop(&mut self) {
        let mut state = lock_state(&self.shared);
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
        let mut state = lock_state(&self.shared);
        if state.done {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Block the current thread until teardown completes.
    pub(crate) fn wait(&self) {
        let mut state = lock_state(&self.shared);
        while !state.done {
            state = self
                .shared
                .condvar
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Data loss recorded by the coordinator, once teardown is done.
    pub(crate) fn data_loss(&self) -> Option<DataLoss> {
        lock_state(&self.shared).data_loss.clone()
    }

    /// Run `wait`, a blocking wait for this dataflow's local workers and
    /// teardown, without deadlocking a tokio runtime that teardown (or the
    /// network the workers depend on) needs.
    ///
    /// The coordinator and the network bridges run on the transport's tokio
    /// runtime, so a thread that the runtime needs to make progress must not
    /// simply block:
    /// - On a multi-thread runtime's worker thread (a task, even on the only
    ///   worker), the wait runs in
    ///   [`block_in_place`](tokio::task::block_in_place), which hands this
    ///   thread's other work to another thread first. This also covers every
    ///   worker of the runtime blocking at once. Other threads (a
    ///   multi-thread runtime's `block_on`, or no runtime at all) are not
    ///   workers and block directly.
    /// - A `current_thread` runtime only progresses while some thread drives
    ///   it, so when the caller is inside a `current_thread` runtime context
    ///   (it may be that thread), or the transport runtime is a
    ///   `current_thread` runtime (nobody may be driving it), the wait first
    ///   spawns a no-op probe on the transport runtime and returns
    ///   [`RuntimeError::InvalidConfig`] if it does not run within a grace
    ///   period. A `spawn_blocking` thread whose runtime another thread
    ///   drives passes.
    ///
    /// When `wait` was not run, the dataflow keeps running, as if its
    /// completion handle had been dropped.
    pub(crate) fn block<R>(&self, wait: impl FnOnce() -> R) -> Result<R> {
        use tokio::runtime::RuntimeFlavor;
        if lock_state(&self.shared).done {
            return Ok(wait());
        }
        let caller = tokio::runtime::Handle::try_current()
            .ok()
            .map(|handle| handle.runtime_flavor());
        if caller == Some(RuntimeFlavor::CurrentThread)
            || self.shared.runtime.runtime_flavor() == RuntimeFlavor::CurrentThread
        {
            self.probe_transport_runtime()?;
        }
        // Worker threads only run code inside tasks; elsewhere
        // `block_in_place` would at best just run `wait`, and at worst panic
        // (in a `current_thread` runtime's `block_on` that has entered a
        // multi-thread runtime's handle).
        if caller != Some(RuntimeFlavor::MultiThread) || tokio::task::try_id().is_none() {
            return Ok(wait());
        }
        let mut wait = Some(wait);
        let blocked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| (wait.take().expect("called once"))())
        }));
        match blocked {
            Ok(result) => Ok(result),
            Err(panic) => match wait.take() {
                // `block_in_place` refused before running anything: this
                // thread drives a `current_thread` runtime that has entered a
                // multi-thread handle. Treat it like any such caller.
                Some(wait) => {
                    self.probe_transport_runtime()?;
                    Ok(wait())
                }
                None => std::panic::resume_unwind(panic),
            },
        }
    }

    /// Spawn a no-op on the transport runtime and fail if it does not run
    /// within [`BLOCKING_WAIT_PROBE`] (see [`Self::block`]).
    fn probe_transport_runtime(&self) -> Result<()> {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let probe = self.shared.runtime.spawn(async move {
            let _ = tx.send(());
        });
        match rx.recv_timeout(BLOCKING_WAIT_PROBE) {
            // Ran, or the runtime is gone (which also completes teardown).
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(()),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                probe.abort();
                if lock_state(&self.shared).done {
                    return Ok(());
                }
                Err(Error::Runtime(RuntimeError::InvalidConfig(
                    "blocking wait on a cluster dataflow would deadlock: its transport runs on a \
                     current_thread tokio runtime that is not making progress (is this thread \
                     driving it, or is nothing driving it?); await the completion instead, or \
                     block from another thread"
                        .into(),
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Records addressed to this node that never reached their operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataLoss {
    pub(crate) peer: String,
    pub(crate) detail: String,
}

impl DataLoss {
    /// The error reported by the completion.
    pub(crate) fn into_error(self) -> Error {
        crate::error::CommunicationError::DataLoss {
            peer_id: self.peer,
            detail: self.detail,
        }
        .into()
    }
}

impl std::fmt::Display for DataLoss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data lost on the link to peer {}: {}",
            self.peer, self.detail
        )
    }
}

/// Detail of a [`DataLoss`] caused by our own frames not reaching the peer.
fn outbound_loss_detail(failure: &LinkFailure) -> String {
    format!("frames to the peer could not be sent ({failure}) while the dataflow ran")
}

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
    /// Acknowledge that we received the peer's whole stream (in-band, behind
    /// everything we already sent to it).
    SendAck(String),
    /// Tell every peer that this dataflow was cancelled (bounded).
    BroadcastCancel,
    /// Queue end-of-stream markers behind everything else.
    SendGoodbye,
    /// A link that is not retired failed while local workers still run:
    /// cancel the local dataflow. The machine also requests
    /// [`BroadcastCancel`](Self::BroadcastCancel) and tears down once the
    /// local workers have stopped.
    LinkFailed { peer: String, failure: LinkFailure },
    /// A soft link failure is waiting for the rest of the link to close.
    /// Call [`LifecycleMachine::on_soft_failure_expired`] if it has not
    /// resolved within a grace period: a link whose writer failed but whose
    /// reader stays open would otherwise never escalate.
    ArmSoftFailureTimer(String),
    /// Cancel the local dataflow with this reason.
    CancelDataflow(CancellationReason),
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
    /// A soft failure waiting for the link to close fully.
    soft_failure: Option<LinkFailure>,
    link: Arc<PeerLinkState>,
}

impl PeerState {
    fn retired(&self) -> bool {
        self.goodbye_received || self.ack_received
    }

    /// Nothing more is needed from or owed to the peer. A send that failed
    /// while the workers ran dropped frames the peer needed, and that stays
    /// undecided (see [`LifecycleMachine::settle_unsent`]) even if the peer
    /// has said goodbye since.
    fn resolved(&self) -> bool {
        (self.retired() || self.lost) && !self.unsent()
    }

    fn unsent(&self) -> bool {
        matches!(self.soft_failure, Some(LinkFailure::SendFailed))
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
    data_loss: Option<DataLoss>,
    /// Loss observed while the dataflow was being cancelled.
    pending_loss: Option<DataLoss>,
    /// Loss reported before any worker succeeded, awaiting the result of the
    /// worker that is finishing.
    deferred_loss: Option<DataLoss>,
    workers_succeeded: usize,
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
            data_loss: None,
            pending_loss: None,
            deferred_loss: None,
            workers_succeeded: 0,
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
                            soft_failure: None,
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

    /// Whether every local worker has reported its result.
    pub(crate) fn workers_done(&self) -> bool {
        self.workers_remaining == 0
    }

    /// Records addressed to this node that were lost while every local
    /// worker succeeded.
    pub(crate) fn data_loss(&self) -> Option<&DataLoss> {
        self.data_loss.as_ref()
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
        if success {
            self.workers_succeeded += 1;
        }
        // A loss reported by a finishing worker counts only if it succeeded.
        if let Some(loss) = self.deferred_loss.take() {
            if self.all_workers_ok && self.data_loss.is_none() && self.phase == Phase::Running {
                if self.cancel_broadcast {
                    self.pending_loss.get_or_insert(loss);
                } else {
                    self.data_loss = Some(loss);
                    if self.workers_remaining > 0 {
                        // Fatal while the other workers still run.
                        self.cancel_broadcast = true;
                        return vec![
                            Action::CancelDataflow(self.data_loss_reason()),
                            Action::BroadcastCancel,
                        ];
                    }
                }
            }
        }
        if self.workers_remaining > 0 || self.phase != Phase::Running {
            return Vec::new();
        }
        // Records lost while the dataflow was being cancelled only matter if
        // every worker still succeeded (`drain_on_cancel`).
        let pending = self.pending_loss.take();
        if self.all_workers_ok && self.data_loss.is_none() {
            self.data_loss = pending;
        }
        if self.all_workers_ok && self.data_loss.is_none() {
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
            if self.all_workers_ok {
                // The workers succeeded, but records addressed to them were
                // lost: the result is incomplete.
                actions.extend(self.abandon_after_data_loss());
            } else if !self.cancel_broadcast {
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

    /// Bridges were cancelled from outside (e.g. `report_node_leave`, runtime
    /// shutdown).
    ///
    /// While the local workers run, the dataflow is broken: tell the peers
    /// (Cancel) and abort. Once they have succeeded, our goodbye and
    /// acknowledgements may still sit in the lanes; tear down gracefully so
    /// the bounded flush delivers them to the reachable peers instead of
    /// leaving those peers waiting for us.
    pub(crate) fn on_external_abort(&mut self) -> Vec<Action> {
        match self.phase {
            Phase::Finished => Vec::new(),
            Phase::Draining => {
                for peer in self.peers.values_mut() {
                    if !peer.retired() {
                        peer.lost = true;
                    }
                }
                self.phase = Phase::Finished;
                vec![Action::Teardown { graceful: true }]
            }
            Phase::Running => {
                self.phase = Phase::Finished;
                let mut actions = Vec::new();
                if !self.cancel_broadcast {
                    self.cancel_broadcast = true;
                    actions.push(Action::BroadcastCancel);
                }
                actions.push(Action::Teardown { graceful: false });
                actions
            }
        }
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
        let event = match event {
            LinkEvent::InboundDataLost(detail) => {
                return self.on_inbound_data_lost(peer_id, detail, false);
            }
            event => event,
        };
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
                // wait for it. Frames a failed send dropped are not needed
                // by a peer that abandoned the dataflow.
                if !peer.retired() {
                    peer.lost = true;
                }
                peer.soft_failure = None;
            }
            LinkEvent::ControlClosed => {
                peer.control_closed = true;
                // Every channel from the peer has ended without a goodbye.
                // The dispatcher reports this after any Cancel queued before
                // the close, so a peer's Cancel is always applied first.
                if !peer.resolved() && peer.fully_closed() {
                    actions.extend(self.on_link_failure(peer_id, LinkFailure::ChannelClosed, true));
                }
            }
            LinkEvent::ControlUndecodable(detail) => {
                // A corrupted control message may have been a Cancel: the
                // link can no longer be trusted.
                actions.extend(self.on_link_failure(
                    peer_id,
                    LinkFailure::Decode(format!("control message: {detail}")),
                    true,
                ));
            }
            LinkEvent::ProgressChannelEnded(failure) => {
                peer.progress_channels_ended += 1;
                let hard = !matches!(failure, LinkFailure::ChannelClosed);
                actions.extend(self.on_link_failure(peer_id, failure, hard));
            }
            LinkEvent::OutboundFailed(failure) => {
                // Our end-of-stream is the last frame on the lane, so once we
                // are draining a failed send means the peer can no longer
                // receive it and will never acknowledge. While running, a
                // failed write usually means the connection is closing: like
                // an EOF it counts once the whole link has closed, so a Cancel
                // the peer queued before closing is applied first. The frames
                // it dropped stay owed until then (`settle_unsent`). Encoding
                // failures are always hard.
                let hard = !(phase == Phase::Running && matches!(failure, LinkFailure::SendFailed));
                actions.extend(self.on_link_failure(peer_id, failure, hard));
            }
            LinkEvent::TransportError(error) => {
                actions.extend(self.on_link_failure(peer_id, LinkFailure::Transport(error), true));
            }
            LinkEvent::InboundDataLost(_) => unreachable!("handled above"),
        }
        actions.extend(self.finish_if_resolved());
        actions
    }

    /// Records from `peer` could not be delivered to the local operator they
    /// were addressed to (it had already finished), so the dataflow's result
    /// would be incomplete. Also used when frames we sent to `peer` were
    /// dropped by a send that failed while the workers ran, and the peer or
    /// our workers then finished without them.
    ///
    /// Ignored when a local failure already explains it. While the dataflow
    /// is being cancelled (`cancelled`: the local token, which the machine
    /// cannot see), cancelled operators drop their inputs, so the loss is
    /// only stashed and applied if every worker still succeeds (drain on
    /// cancel). Otherwise the loss is fatal: while the workers run, the
    /// dataflow is cancelled on every node; once they have succeeded, the
    /// peers are told (Cancel) and the completion reports the loss.
    pub(crate) fn on_inbound_data_lost(
        &mut self,
        peer: String,
        detail: String,
        cancelled: bool,
    ) -> Vec<Action> {
        if !self.all_workers_ok || self.data_loss.is_some() {
            return Vec::new();
        }
        let loss = DataLoss { peer, detail };
        if self.phase == Phase::Running && (cancelled || self.cancel_broadcast) {
            self.pending_loss.get_or_insert(loss);
            return Vec::new();
        }
        if self.phase == Phase::Running && self.workers_succeeded == 0 {
            // No worker has succeeded, so the operator that dropped these
            // records belongs to a worker finishing right now (its inputs are
            // dropped just before its result is reported). Its result
            // decides: a failure explains the loss and must be reported as
            // itself, not as a network data loss.
            self.deferred_loss.get_or_insert(loss);
            return Vec::new();
        }
        #[cfg(feature = "tracing")]
        tracing::error!("{loss}");
        self.data_loss = Some(loss);
        match self.phase {
            Phase::Running => {
                let mut actions = vec![Action::CancelDataflow(self.data_loss_reason())];
                self.cancel_broadcast = true;
                actions.push(Action::BroadcastCancel);
                actions
            }
            Phase::Draining => {
                let mut actions = self.abandon_after_data_loss();
                actions.extend(self.finish_if_resolved());
                actions
            }
            Phase::Finished => Vec::new(),
        }
    }

    fn data_loss_reason(&self) -> CancellationReason {
        CancellationReason::NetworkError {
            detail: self
                .data_loss
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        }
    }

    /// After local success with lost data: cancel the peers and stop waiting
    /// for their acknowledgements (they stop waiting for us on our Cancel).
    fn abandon_after_data_loss(&mut self) -> Vec<Action> {
        let mut actions = vec![Action::CancelDataflow(self.data_loss_reason())];
        if !self.cancel_broadcast {
            self.cancel_broadcast = true;
            actions.push(Action::BroadcastCancel);
        }
        for peer in self.peers.values_mut() {
            if !peer.retired() {
                peer.lost = true;
            }
            // The loss is already reported; nothing else is left to decide.
            peer.soft_failure = None;
        }
        actions
    }

    /// A link ended without a clean end-of-stream.
    ///
    /// `hard` failures (corruption, transport errors, our end-of-stream not
    /// being sendable) mean nothing more can be expected from the peer. An
    /// EOF on one channel, or a failed write while running, is soft: an
    /// acknowledgement or a Cancel may still be queued behind it on another
    /// channel, so the failure only counts once every channel from the peer
    /// has closed, or once a grace period has expired without that happening.
    ///
    /// While the local workers run, a failed link that is not retired is
    /// fatal: the local dataflow is cancelled and `Cancel` is broadcast to
    /// every reachable peer (a terminal goodbye they act on), and teardown
    /// waits for the local workers to stop. Tearing down silently instead
    /// would leave the workers running without bridges and peers waiting for
    /// an acknowledgement that never comes.
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
        if peer.unsent() && (hard || peer.fully_closed()) {
            return self.settle_unsent(peer_id, Some(failure));
        }
        match self.phase {
            Phase::Running if self.cancel_broadcast => {
                // The dataflow is already being cancelled; peers tearing down
                // is expected. Wait for the local workers instead.
                peer.lost = true;
                Vec::new()
            }
            Phase::Running => {
                // An EOF on one channel is soft: the peer's Cancel may still
                // be queued on its control channel, which closes last (see
                // `ControlClosed`). Wait until the link is fully closed so a
                // cancellation is reported as such, not as a link failure.
                if !hard && !peer.fully_closed() {
                    return Self::defer_soft_failure(peer, peer_id, failure);
                }
                peer.lost = true;
                self.cancel_broadcast = true;
                vec![
                    Action::LinkFailed {
                        peer: peer_id,
                        failure,
                    },
                    Action::BroadcastCancel,
                ]
            }
            Phase::Draining => {
                if !hard && !peer.fully_closed() {
                    return Self::defer_soft_failure(peer, peer_id, failure);
                }
                peer.lost = true;
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

    /// Wait for the rest of a softly failed link to close, but only for a
    /// grace period (see [`Action::ArmSoftFailureTimer`]).
    fn defer_soft_failure(
        peer: &mut PeerState,
        peer_id: String,
        failure: LinkFailure,
    ) -> Vec<Action> {
        if peer.soft_failure.is_some() {
            return Vec::new();
        }
        peer.soft_failure = Some(failure);
        vec![Action::ArmSoftFailureTimer(peer_id)]
    }

    /// The grace period of a soft failure ran out with the link still
    /// partially open, for example because our writer failed on a local
    /// error while the connection itself stays up. Nothing guarantees the
    /// rest of the link will ever close, so the failure becomes hard.
    pub(crate) fn on_soft_failure_expired(&mut self, peer_id: String) -> Vec<Action> {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return Vec::new();
        };
        let mut actions = if peer.unsent() {
            self.settle_unsent(peer_id, None)
        } else {
            let Some(failure) = peer.soft_failure.take() else {
                return Vec::new();
            };
            self.on_link_failure(peer_id, failure, true)
        };
        actions.extend(self.finish_if_resolved());
        actions
    }

    /// Decide what a send that failed while the workers ran means, once a
    /// Cancel from the peer (which excuses it, see `CancelReceived`) can no
    /// longer be pending: the link has fully closed or failed hard
    /// (`closed_by`; the control channel closes after any queued Cancel), or
    /// the grace period expired with the link still open (`None`).
    ///
    /// - The peer said goodbye and then closed: it tore down after finishing
    ///   (without waiting for our ack), which is what made our send fail.
    /// - The peer said goodbye but the link stayed open: only our writer
    ///   failed, and the peer finished without the frames it dropped.
    /// - No goodbye, while our workers run: an ordinary fatal link failure.
    /// - No goodbye, after our workers succeeded: the frames the failed send
    ///   dropped while they ran are missing from the result.
    fn settle_unsent(&mut self, peer_id: String, closed_by: Option<LinkFailure>) -> Vec<Action> {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return Vec::new();
        };
        let Some(unsent) = peer.soft_failure.take() else {
            return Vec::new();
        };
        if peer.retired() {
            if closed_by.is_some() {
                return Vec::new();
            }
        } else if self.phase == Phase::Running && !peer.lost {
            return self.on_link_failure(peer_id, closed_by.unwrap_or(unsent), true);
        } else {
            peer.lost = true;
        }
        self.on_inbound_data_lost(peer_id, outbound_loss_detail(&unsent), false)
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

async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Reason text sent to peers with `Cancel`; peers classify it (a forwarded
/// failure is itself a failure, so they never drain into a partial result).
fn cancel_broadcast_reason(dataflow_cancel: &CancellationToken) -> String {
    match dataflow_cancel.reason() {
        Some(reason) => reason.to_string(),
        None if dataflow_cancel.is_cancelled() => "cancelled".to_string(),
        // Not cancelled: the bridges were stopped from outside while the
        // workers ran, so this node abandons a running dataflow.
        None => CancellationReason::NetworkError {
            detail: "the node stopped its network bridges while its workers were running".into(),
        }
        .to_string(),
    }
}

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
    pub(crate) forwarders: HashMap<String, tokio::task::JoinHandle<()>>,
    pub(crate) inbound: InboundBridges,
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
        // Grace deadlines of soft link failures (`ArmSoftFailureTimer`).
        let mut soft_deadlines: HashMap<String, tokio::time::Instant> = HashMap::new();
        loop {
            let next_soft = soft_deadlines
                .iter()
                .min_by_key(|(_, deadline)| **deadline)
                .map(|(peer, deadline)| (peer.clone(), *deadline));
            let soft_deadline = next_soft.as_ref().map(|(_, deadline)| *deadline);
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
                        actions.extend(self.apply(event));
                    }
                    actions.extend(self.machine.on_dataflow_cancelled());
                    actions
                }
                _ = self.bridge_cancel.cancelled() => self.machine.on_external_abort(),
                event = self.events.recv() => match event {
                    Some(event) => self.apply(event),
                    None => self.machine.on_events_closed(),
                },
                _ = sleep_until_opt(soft_deadline), if soft_deadline.is_some() => {
                    let (peer, _) = next_soft.expect("guarded by soft_deadline");
                    soft_deadlines.remove(&peer);
                    self.machine.on_soft_failure_expired(peer)
                }
            };
            for action in actions {
                if let Action::ArmSoftFailureTimer(peer) = action {
                    soft_deadlines.entry(peer).or_insert_with(|| {
                        tokio::time::Instant::now() + self.cancel_broadcast_timeout
                    });
                    continue;
                }
                if self.perform(action).await {
                    return;
                }
            }
        }
    }

    /// Feed one event to the machine.
    ///
    /// Records dropped by operators of a cancelled dataflow are expected
    /// unless the workers still succeed; the machine cannot see the token, so
    /// its state is passed along.
    fn apply(&mut self, event: LifecycleEvent) -> Vec<Action> {
        match event {
            LifecycleEvent::Link {
                peer,
                event: LinkEvent::InboundDataLost(detail),
            } => {
                let cancelled = self.dataflow_cancel.is_cancelled();
                self.machine.on_inbound_data_lost(peer, detail, cancelled)
            }
            event => self.machine.on_event(event),
        }
    }

    /// Execute one action; returns `true` once teardown is complete.
    async fn perform(&mut self, action: Action) -> bool {
        match action {
            Action::SendAck(peer) => {
                // On the payload lane, behind every data frame we already
                // sent to the peer: once it reads the ack it has read those.
                let channel = self.peer_workers.get(&peer).and_then(|&(peer_start, _)| {
                    progress_channel_id(self.local_workers.0, peer_start, self.total_workers).ok()
                });
                if let (Some(outbox), Some(channel_id)) = (self.outboxes.get(&peer), channel) {
                    let _ = outbox.send(Frame {
                        dataflow_id: self.dataflow_id,
                        channel_id,
                        payload: encode_progress_ack(),
                    });
                }
                false
            }
            Action::BroadcastCancel => {
                let reason = cancel_broadcast_reason(&self.dataflow_cancel);
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
                tracing::error!(peer = %peer, %failure, "cluster link failed while workers were running");
                let poisoned = matches!(
                    failure,
                    LinkFailure::LockPoisoned
                        | LinkFailure::Transport(TransportError::LockPoisoned { .. })
                );
                let reason = if poisoned {
                    CancellationReason::InternalError {
                        detail: format!("lock poisoned on the link to peer {peer}"),
                    }
                } else {
                    CancellationReason::NetworkError {
                        detail: format!("link to peer {peer} failed: {failure}"),
                    }
                };
                self.dataflow_cancel.cancel_with_reason(reason);
                false
            }
            Action::CancelDataflow(reason) => {
                self.dataflow_cancel.cancel_with_reason(reason);
                false
            }
            // Armed by `run`, which owns the deadlines.
            Action::ArmSoftFailureTimer(_) => false,
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
    /// the last frames on each lane except for acknowledgements.
    fn send_goodbye(&mut self) {
        let payload = encode_progress_end_of_stream();
        for (peer, outbox) in &self.outboxes {
            let Some(&(peer_start, peer_end)) = self.peer_workers.get(peer) else {
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
        // Once every local worker has finished, no operator consumes inbound
        // data any more: let the inbound bridges deliver or report what is
        // already queued, then exit. Anything a peer sent before its
        // end-of-stream or acknowledgement is queued by now.
        let drain_inbound = self.machine.workers_done();
        if drain_inbound {
            self.inbound.drain();
        }
        if !graceful {
            for forwarder in forwarders.values() {
                forwarder.abort();
            }
        } else {
            // Peers may still need what we queued: our goodbye, the
            // acknowledgements of their goodbyes (both in-band on the payload
            // lane) and, for a peer we stopped waiting for (it cancelled and
            // may still be draining), our final progress. Hand our lanes to
            // the transport and, in Shared mode, let the bridges sequence
            // those frames before unregistering, which would drop them.
            // One task per peer, all bounded together, so an unreachable peer
            // does not hold up the others.
            let dataflow_id = self.dataflow_id;
            let mut flushes = tokio::task::JoinSet::new();
            for (peer, forwarder) in forwarders {
                let managers = managers.clone();
                flushes.spawn(async move {
                    let _ = forwarder.await;
                    if let Some(manager) = managers.as_ref().and_then(|m| m.get(&peer)) {
                        manager.flush_dataflow(&dataflow_id).await;
                    }
                });
            }
            let finished = tokio::time::timeout(self.cancel_broadcast_timeout, async {
                while flushes.join_next().await.is_some() {}
            })
            .await;
            if finished.is_err() {
                // Release the managers the unfinished flushes hold before
                // checking below whether ours is the last reference.
                flushes.abort_all();
                while flushes.join_next().await.is_some() {}
            }
        }
        if drain_inbound {
            let _ =
                tokio::time::timeout(self.cancel_broadcast_timeout, self.inbound.wait_idle()).await;
            while let Ok(event) = self.events.try_recv() {
                if let LifecycleEvent::Link {
                    event: LinkEvent::InboundDataLost(_),
                    ..
                } = &event
                {
                    let _ = self.machine.on_event(event);
                }
            }
        }
        if let Some(managers) = managers {
            for manager in managers.values() {
                manager.unregister_dataflow(&self.dataflow_id).await;
            }
            // If this dataflow holds the last reference (per-job managers),
            // releasing it closes the connections. Wait (bounded) until what
            // we queued on them, such as our goodbye, our acknowledgements or
            // our Cancel, has been written: peers may still be waiting for it.
            // (`into_inner`, unlike `try_unwrap`, hands the managers to
            // exactly one of several dataflows releasing them at once.)
            if let Some(owned) = Arc::into_inner(managers) {
                let timeout = self.cancel_broadcast_timeout;
                let mut closes = tokio::task::JoinSet::new();
                for manager in owned.into_values() {
                    closes.spawn(manager.close(timeout));
                }
                while closes.join_next().await.is_some() {}
            }
        }
        self.progress_handles = None;
        self.dispatcher = None;
        if let Some(loss) = self.machine.data_loss() {
            self._teardown.set_data_loss(loss.clone());
        }
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
    fn broadcast_reasons_are_classified_like_the_local_cause() {
        let forwarded = |token: &CancellationToken| CancellationReason::PeerCancelled {
            peer_id: "a".into(),
            detail: cancel_broadcast_reason(token),
        };
        // Abandoning a running dataflow (bridges stopped from outside).
        let token = CancellationToken::new();
        assert!(forwarded(&token).is_failure());
        // Failures this coordinator cancels with stay failures at peers.
        for reason in [
            CancellationReason::NetworkError {
                detail: "data lost".into(),
            },
            CancellationReason::InternalError {
                detail: "lock poisoned".into(),
            },
            CancellationReason::WorkerFailed {
                detail: "boom".into(),
            },
        ] {
            let token = CancellationToken::new();
            token.cancel_with_reason(reason);
            assert!(forwarded(&token).is_failure());
        }
        // A requested stop lets peers drain.
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::UserRequested);
        assert!(!forwarded(&token).is_failure());
        let token = CancellationToken::new();
        token.cancel();
        assert!(!forwarded(&token).is_failure());
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

    /// Every unclean failure of an unretired link while the workers run is
    /// fatal cluster-wide: cancel locally, broadcast Cancel, then wait for the
    /// local workers before tearing down.
    #[test]
    fn link_failure_while_running_cancels_everywhere_and_waits_for_workers() {
        let failures: Vec<fn() -> LinkEvent> = vec![
            || LinkEvent::ProgressChannelEnded(LinkFailure::Decode("crc".into())),
            || LinkEvent::OutboundFailed(LinkFailure::Encode("x".into())),
            || LinkEvent::ProgressChannelEnded(LinkFailure::LockPoisoned),
            || LinkEvent::TransportError(TransportError::ReorderTimeout),
            || LinkEvent::ControlUndecodable("crc".into()),
        ];
        for failure in failures {
            let (mut m, _) = machine(2, &[("b", 1), ("c", 1)]);
            let actions = m.on_event(link_event("b", failure()));
            assert!(
                matches!(
                    actions.as_slice(),
                    [Action::LinkFailed { peer, .. }, Action::BroadcastCancel] if peer == "b"
                ),
                "{actions:?}"
            );
            assert_eq!(m.phase(), Phase::Running, "no silent teardown");
            // Further failures (c tearing down after our Cancel) are expected.
            assert!(m.on_event(eof("c")).is_empty());
            assert!(m.on_event(done(false)).is_empty());
            let actions = m.on_event(done(false));
            assert!(matches!(
                actions.as_slice(),
                [Action::Teardown { graceful: false }]
            ));
        }
        // Workers that still succeed (draining) tear down without waiting for
        // acks: every peer was told the dataflow is cancelled.
        let (mut m, _) = machine(1, &[("b", 1), ("c", 1)]);
        m.on_event(link_event(
            "b",
            LinkEvent::TransportError(TransportError::ConnectionClosed),
        ));
        let actions = m.on_event(done(true));
        assert!(matches!(
            actions.as_slice(),
            [Action::SendGoodbye, Action::Teardown { graceful: true }]
        ));
    }

    /// An EOF is a link failure only once every channel from the peer has
    /// closed: the control channel closes last, after any Cancel queued on
    /// it has been applied, so a cancelling peer is never reported as a
    /// network failure.
    #[test]
    fn eof_while_running_waits_for_the_whole_link_to_close() {
        let (mut m, _) = machine(1, &[("b", 2)]);
        let actions = m.on_event(eof("b"));
        assert!(
            matches!(actions.as_slice(), [Action::ArmSoftFailureTimer(p)] if p == "b"),
            "{actions:?}"
        );
        // A failed write while running means the connection is closing too.
        assert!(
            m.on_event(link_event(
                "b",
                LinkEvent::OutboundFailed(LinkFailure::SendFailed)
            ))
            .is_empty()
        );
        assert!(m.on_event(eof("b")).is_empty());
        let actions = m.on_event(link_event("b", LinkEvent::ControlClosed));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::LinkFailed {
                        failure: LinkFailure::ChannelClosed,
                        ..
                    },
                    Action::BroadcastCancel
                ]
            ),
            "{actions:?}"
        );
        // A Cancel queued before the close resolves the peer first.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(eof("b")).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        assert!(
            m.on_event(link_event("b", LinkEvent::CancelReceived))
                .is_empty()
        );
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        assert_eq!(m.phase(), Phase::Running);
        // The grace period expiring afterwards changes nothing.
        assert!(m.on_soft_failure_expired("b".to_string()).is_empty());
    }

    #[test]
    fn soft_failure_escalates_when_the_link_never_closes() {
        // Our writer failed on a local error (an oversized frame, say) while
        // the connection and our reader stay up: nothing else will follow.
        let (mut m, _) = machine(1, &[("b", 1)]);
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::OutboundFailed(LinkFailure::SendFailed),
        ));
        assert!(
            matches!(actions.as_slice(), [Action::ArmSoftFailureTimer(p)] if p == "b"),
            "{actions:?}"
        );
        let actions = m.on_soft_failure_expired("b".to_string());
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::LinkFailed {
                        failure: LinkFailure::SendFailed,
                        ..
                    },
                    Action::BroadcastCancel
                ]
            ),
            "{actions:?}"
        );
        assert!(m.on_soft_failure_expired("b".to_string()).is_empty());
        // While draining, the peer is declared lost once the grace expires.
        let (mut m, _) = machine(1, &[("b", 2)]);
        assert!(matches!(
            m.on_event(done(true)).as_slice(),
            [Action::SendGoodbye]
        ));
        assert!(matches!(
            m.on_event(eof("b")).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        // Armed once per peer.
        assert!(m.on_event(eof("b")).is_empty());
        assert_eq!(m.phase(), Phase::Draining);
        let actions = m.on_soft_failure_expired("b".to_string());
        assert!(is_teardown(&actions, true), "{actions:?}");
    }

    fn send_failed(peer: &str) -> LifecycleEvent {
        link_event(peer, LinkEvent::OutboundFailed(LinkFailure::SendFailed))
    }

    #[test]
    fn frames_dropped_while_running_are_data_loss_even_if_the_peer_finishes() {
        // The peer finishes (prematurely) after our frames to it were
        // dropped, and waits for our ack: only our writer failed, so the
        // link stays open until the grace period expires.
        let (mut m, _) = machine(2, &[("b", 1)]);
        assert!(m.on_event(done(true)).is_empty());
        assert!(matches!(
            m.on_event(send_failed("b")).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        let actions = m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(
            matches!(actions.as_slice(), [Action::SendAck(_)]),
            "{actions:?}"
        );
        let actions = m.on_soft_failure_expired("b".to_string());
        assert!(
            matches!(
                actions.as_slice(),
                [Action::CancelDataflow(_), Action::BroadcastCancel]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
        assert!(m.on_soft_failure_expired("b".to_string()).is_empty());

        // Our workers succeed first: the coordinator keeps waiting, and the
        // expired grace period (the writer died alone, nothing closes)
        // reports the incomplete result.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(send_failed("b")).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        let actions = m.on_event(done(true));
        assert!(
            matches!(actions.as_slice(), [Action::SendGoodbye]),
            "{actions:?}"
        );
        // A goodbye from the peer does not end the wait either.
        let actions = m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(
            matches!(actions.as_slice(), [Action::SendAck(_)]),
            "{actions:?}"
        );
        assert_eq!(m.phase(), Phase::Draining);
        let actions = m.on_soft_failure_expired("b".to_string());
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelDataflow(_),
                    Action::BroadcastCancel,
                    Action::Teardown { graceful: true }
                ]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());

        // Still running and the peer never finished: a plain link failure.
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(send_failed("b"));
        let actions = m.on_event(link_event("b", LinkEvent::ControlClosed));
        assert!(
            matches!(actions.as_slice(), [] | [Action::ArmSoftFailureTimer(_)]),
            "progress channel still open: {actions:?}"
        );
        let actions = m.on_event(eof("b"));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::LinkFailed {
                        failure: LinkFailure::ChannelClosed,
                        ..
                    },
                    Action::BroadcastCancel
                ]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_none());
    }

    #[test]
    fn frames_dropped_by_a_peer_leaving_after_its_goodbye_are_not_data_loss() {
        // The peer finished and then tore down without waiting for our ack
        // (an external abort while draining): our write failed because it
        // left, which may be reported before its last end-of-stream marker.
        let (mut m, _) = machine(2, &[("b", 1)]);
        m.on_event(done(true));
        m.on_event(send_failed("b"));
        assert!(matches!(
            m.on_event(link_event("b", LinkEvent::ProgressEndOfStream))
                .as_slice(),
            [Action::SendAck(_)]
        ));
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        assert!(m.on_soft_failure_expired("b".to_string()).is_empty());
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::SendGoodbye, Action::Teardown { graceful: true }]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_none());
    }

    #[test]
    fn frames_dropped_for_a_cancelled_peer_are_not_data_loss() {
        // The peer cancelled and closed; our write failed before its Cancel
        // was read. It does not need our frames, whichever event the
        // coordinator sees first: its Cancel, our workers' success, or its
        // goodbye (a peer that broadcast Cancel and then still succeeded).
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(send_failed("b"));
        assert!(
            m.on_event(link_event("b", LinkEvent::CancelReceived))
                .is_empty()
        );
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::SendGoodbye, Action::Teardown { graceful: true }]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_none());

        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(send_failed("b"));
        assert!(matches!(
            m.on_event(done(true)).as_slice(),
            [Action::SendGoodbye]
        ));
        let actions = m.on_event(link_event("b", LinkEvent::CancelReceived));
        assert!(is_teardown(&actions, true), "{actions:?}");
        assert!(m.on_soft_failure_expired("b".to_string()).is_empty());
        assert!(m.data_loss().is_none());

        let (mut m, _) = machine(2, &[("b", 1)]);
        m.on_event(done(true));
        m.on_event(send_failed("b"));
        assert!(matches!(
            m.on_event(link_event("b", LinkEvent::ProgressEndOfStream))
                .as_slice(),
            [Action::SendAck(_)]
        ));
        assert!(
            m.on_event(link_event("b", LinkEvent::CancelReceived))
                .is_empty()
        );
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::SendGoodbye, Action::Teardown { graceful: true }]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_none());
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
    fn undecodable_control_message_is_a_link_failure() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        let actions = m.on_event(link_event("b", LinkEvent::ControlUndecodable("crc".into())));
        assert!(is_teardown(&actions, true));
    }

    fn lost(peer: &str) -> LifecycleEvent {
        link_event(peer, LinkEvent::InboundDataLost("channel 7: late".into()))
    }

    /// Records lost while the workers run: cancel everywhere at once, and
    /// report the loss even if the workers still succeed (drain).
    #[test]
    fn data_loss_while_running_cancels_everywhere() {
        // Once a worker has succeeded, a loss is fatal at once.
        let (mut m, _) = machine(2, &[("b", 1)]);
        assert!(m.on_event(done(true)).is_empty());
        let actions = m.on_event(lost("b"));
        assert!(matches!(
            actions.as_slice(),
            [
                Action::CancelDataflow(CancellationReason::NetworkError { .. }),
                Action::BroadcastCancel
            ]
        ));
        assert_eq!(
            m.data_loss().map(|l| l.peer.as_str()),
            Some("b"),
            "recorded for the completion"
        );
        // Only the first loss acts.
        assert!(m.on_event(lost("b")).is_empty());
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelDataflow(_),
                    Action::Teardown { graceful: false }
                ]
            ),
            "no goodbye after a loss: {actions:?}"
        );
    }

    /// Before any worker succeeded, a loss comes from the worker that is
    /// finishing: its result decides, so a plain worker failure is never
    /// broadcast as a network data loss.
    #[test]
    fn data_loss_reported_by_a_finishing_worker_waits_for_its_result() {
        let (mut m, _) = machine(2, &[("b", 1)]);
        assert!(m.on_event(lost("b")).is_empty());
        assert!(m.data_loss().is_none());
        let actions = m.on_event(done(false));
        assert!(actions.is_empty(), "{actions:?}");
        assert!(m.data_loss().is_none(), "the failure explains it");

        let (mut m, _) = machine(2, &[("b", 1)]);
        assert!(m.on_event(lost("b")).is_empty());
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::CancelDataflow(_), Action::BroadcastCancel]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());

        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(m.on_event(lost("b")).is_empty());
        let actions = m.on_event(done(true));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelDataflow(_),
                    Action::BroadcastCancel,
                    Action::Teardown { graceful: false }
                ]
            ),
            "{actions:?}"
        );
    }

    /// Records arriving after the local workers succeeded (receiver finished
    /// early): Cancel the peers, stop waiting for acks, report the loss.
    #[test]
    fn data_loss_while_draining_cancels_peers_and_tears_down() {
        let (mut m, links) = machine(1, &[("b", 1), ("c", 1)]);
        m.on_event(link_event("c", LinkEvent::ProgressEndOfStream));
        m.on_event(done(true));
        assert_eq!(m.phase(), Phase::Draining);
        let actions = m.on_event(lost("b"));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::CancelDataflow(_),
                    Action::BroadcastCancel,
                    Action::Teardown { graceful: true }
                ]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
        assert!(!links[0].is_retired());
    }

    /// Losses reported during teardown are still recorded; losses explained
    /// by a local failure or a cancellation are not.
    #[test]
    fn data_loss_is_ignored_when_failure_or_cancellation_explains_it() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        m.on_event(link_event("b", LinkEvent::AckReceived));
        assert_eq!(m.phase(), Phase::Finished);
        assert!(m.on_event(lost("b")).is_empty());
        assert!(m.data_loss().is_some(), "recorded after teardown");

        let (mut m, _) = machine(2, &[("b", 1)]);
        m.on_event(done(false));
        assert!(m.on_event(lost("b")).is_empty());
        assert!(m.data_loss().is_none(), "a failed worker drops its inputs");

        // During cancellation the loss is stashed: it only counts if every
        // worker still succeeds (drain on cancel).
        for drained in [true, false] {
            let (mut m, _) = machine(1, &[("b", 1)]);
            m.on_dataflow_cancelled();
            assert!(m.on_event(lost("b")).is_empty());
            assert!(m.data_loss().is_none());
            let actions = m.on_event(done(drained));
            if drained {
                assert!(m.data_loss().is_some(), "drained Ok but records were lost");
                assert!(is_teardown(&actions, false), "{actions:?}");
            } else {
                assert!(
                    m.data_loss().is_none(),
                    "cancelled workers drop their inputs"
                );
            }
        }
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(
            m.on_inbound_data_lost("b".into(), "late".into(), true)
                .is_empty(),
            "token cancelled: stashed"
        );
        assert!(m.data_loss().is_none());
    }

    #[test]
    fn peer_closing_without_goodbye_or_ack_is_lost_after_all_channels_close() {
        let (mut m, links) = machine(1, &[("b", 2)]);
        m.on_event(done(true));
        assert!(matches!(
            m.on_event(eof("b")).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
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
        // While the workers run, an external abort tells the peers.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_external_abort().as_slice(),
            [
                Action::BroadcastCancel,
                Action::Teardown { graceful: false }
            ]
        ));
        assert!(m.on_external_abort().is_empty());
        assert!(m.on_event(done(true)).is_empty());

        // After local success it tears down gracefully: the flush delivers
        // the goodbye and acknowledgements still queued for the peers.
        let (mut m, links) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        assert_eq!(m.phase(), Phase::Draining);
        assert!(matches!(
            m.on_external_abort().as_slice(),
            [Action::Teardown { graceful: true }]
        ));
        assert!(!links[0].is_retired());

        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled();
        assert!(matches!(
            m.on_external_abort().as_slice(),
            [Action::Teardown { graceful: false }]
        ));

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
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (notifier, waiter) = teardown_signal(rt.handle().clone());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiter.poll_done(&mut cx).is_pending());
        notifier.set_data_loss(DataLoss {
            peer: "b".into(),
            detail: "late".into(),
        });
        let handle = std::thread::spawn(move || {
            waiter.wait();
            waiter.data_loss()
        });
        drop(notifier);
        assert_eq!(handle.join().unwrap().map(|l| l.peer), Some("b".into()));
    }

    /// Blocking from the thread that drives a current_thread transport
    /// runtime, or while no thread drives it, errors promptly; blocking from
    /// a thread that does not drive it, while another one does, works.
    #[test]
    fn blocking_wait_probe_detects_the_thread_driving_the_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (notifier, waiter) = teardown_signal(rt.handle().clone());
        let deadlock = |result: &Result<()>| {
            matches!(
                result,
                Err(Error::Runtime(RuntimeError::InvalidConfig(m))) if m.contains("deadlock")
            )
        };
        let started = std::time::Instant::now();
        let error = rt.block_on(async { waiter.block(|| ()) });
        assert!(deadlock(&error), "{error:?}");
        assert!(started.elapsed() < Duration::from_secs(5));

        // A spawn_blocking thread while the main thread drives the runtime.
        let (waiter, result) = rt.block_on(async move {
            tokio::task::spawn_blocking(move || {
                let result = waiter.block(|| ());
                (waiter, result)
            })
            .await
            .unwrap()
        });
        assert!(result.is_ok(), "{result:?}");

        // Outside any runtime context while nothing drives the runtime: the
        // wait could only finish if some thread started driving it.
        let result = waiter.block(|| ());
        assert!(deadlock(&result), "{result:?}");

        // Outside any runtime context while another thread drives it.
        let waiter = Arc::new(waiter);
        let blocked = Arc::clone(&waiter);
        let result = rt.block_on(async move {
            tokio::task::spawn_blocking(move || {
                std::thread::spawn(move || blocked.block(|| ()))
                    .join()
                    .unwrap()
            })
            .await
            .unwrap()
        });
        assert!(result.is_ok(), "{result:?}");

        // Done: never probes.
        drop(notifier);
        assert!(rt.block_on(async { waiter.block(|| ()) }).is_ok());
    }

    /// On a multi-thread runtime the wait hands the thread's work over
    /// (`block_in_place`): blocking the only worker thread, or the thread in
    /// `block_on`, does not stop the runtime from completing teardown.
    #[test]
    fn blocking_wait_on_a_multi_thread_runtime_keeps_it_running() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            // From a task on the only worker thread.
            let (notifier, waiter) = teardown_signal(mt.handle().clone());
            let from_task = mt.block_on(async move {
                tokio::spawn(async move {
                    // Teardown completes on this runtime, later.
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        drop(notifier);
                    });
                    waiter.block(|| waiter.wait())
                })
                .await
                .unwrap()
            });
            // From the thread in block_on.
            let (notifier, waiter) = teardown_signal(mt.handle().clone());
            let from_block_on = mt.block_on(async move {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(notifier);
                });
                waiter.block(|| waiter.wait())
            });
            let _ = done_tx.send((from_task.is_ok(), from_block_on.is_ok()));
        });
        let outcome = done_rx.recv_timeout(Duration::from_secs(20));
        assert_eq!(outcome, Ok((true, true)), "the blocking wait deadlocked");
    }

    /// A thread driving a `current_thread` runtime that has entered a
    /// multi-thread runtime's handle cannot use `block_in_place`; the wait
    /// must still work (the transport's workers make progress) without
    /// panicking, both in the `block_on` body and in a task.
    #[test]
    fn blocking_wait_in_a_current_thread_runtime_inside_a_multi_thread_handle() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let ct = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let wait_inside = |inside_task: bool| {
                let (notifier, waiter) = teardown_signal(mt.handle().clone());
                mt.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(notifier);
                });
                let handle = mt.handle().clone();
                ct.block_on(async move {
                    let wait = move || {
                        let _entered = handle.enter();
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            waiter.block(|| waiter.wait()).is_ok()
                        }))
                        .unwrap_or(false)
                    };
                    if inside_task {
                        tokio::spawn(async move { wait() }).await.unwrap()
                    } else {
                        wait()
                    }
                })
            };
            let _ = done_tx.send((wait_inside(false), wait_inside(true)));
        });
        let outcome = done_rx.recv_timeout(Duration::from_secs(20));
        assert_eq!(
            outcome,
            Ok((true, true)),
            "the blocking wait failed or deadlocked"
        );
    }
}
