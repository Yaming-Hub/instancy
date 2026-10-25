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
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Most distinct runtime thread names remembered per dataflow.
const MAX_WORKER_NAMES: usize = 16;

/// The name tokio gives the threads of a runtime built without
/// `thread_name` (documented on `tokio::runtime::Builder::thread_name`).
const TOKIO_DEFAULT_THREAD_NAME: &str = "tokio-runtime-worker";

struct TeardownShared {
    state: Mutex<TeardownState>,
    condvar: Condvar,
    /// Runtime hosting the transport and the coordinator task.
    runtime: tokio::runtime::Handle,
    /// Names of multi-thread runtime threads that ran this dataflow's tasks
    /// (see [`TeardownWaiter::block`]).
    worker_names: Mutex<Vec<String>>,
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
        worker_names: Mutex::new(Vec::new()),
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

fn lock_worker_names(shared: &TeardownShared) -> std::sync::MutexGuard<'_, Vec<String>> {
    shared
        .worker_names
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Remember the name of the current thread, which runs a task on the
/// transport runtime, if that is a multi-thread runtime (the thread driving a
/// `current_thread` runtime is no worker).
fn note_runtime_thread(shared: &TeardownShared) {
    if shared.runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return;
    }
    if let Some(name) = std::thread::current().name() {
        remember_thread_name(shared, name);
    }
}

fn remember_thread_name(shared: &TeardownShared, name: &str) {
    let mut names = lock_worker_names(shared);
    if names.len() < MAX_WORKER_NAMES && !names.iter().any(|known| known == name) {
        names.push(name.to_owned());
    }
}

/// Whether the current thread is driving a tokio scheduler: it is a
/// multi-thread runtime's worker holding the worker's core, or the thread
/// driving a `current_thread` runtime.
///
/// Decided without side effects: [`tokio::task::yield_now`] defers its
/// wake-up to the scheduler driving the thread and wakes at once when there
/// is none (outside any runtime, in a multi-thread runtime's `block_on` and
/// the `LocalSet`s it drives, on a `spawn_blocking` thread, inside
/// `block_in_place`). Deferred, the wake-up merely sets a flag later.
fn thread_drives_a_scheduler() -> bool {
    struct Woken(AtomicBool);
    impl std::task::Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let woken = Arc::new(Woken(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&woken));
    let mut yield_now = std::pin::pin!(tokio::task::yield_now());
    let _ = std::future::Future::poll(yield_now.as_mut(), &mut Context::from_waker(&waker));
    !woken.0.load(Ordering::SeqCst)
}

impl TeardownNotifier {
    /// Record that records sent to this node were lost; the completion
    /// reports it even though the local workers succeeded.
    pub(crate) fn set_data_loss(&self, loss: DataLoss) {
        lock_state(&self.shared).data_loss = Some(loss);
    }

    /// Remember the current thread as one of the transport runtime's (the
    /// coordinator runs there).
    pub(crate) fn note_runtime_thread(&self) {
        note_runtime_thread(&self.shared);
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

    /// Remember `name` as the name of a multi-thread runtime's threads (see
    /// [`Self::block`]). The coordinator adds the names of the transport
    /// runtime's threads it runs on.
    pub(crate) fn remember_runtime_thread(&self, name: &str) {
        remember_thread_name(&self.shared, name);
    }

    /// Run `wait`, a blocking wait for this dataflow's local workers and
    /// teardown, without deadlocking a tokio runtime that teardown (or the
    /// network the workers depend on) needs.
    ///
    /// The coordinator and the network bridges run on the transport's tokio
    /// runtime, so a thread that the runtime needs to make progress must not
    /// simply block:
    /// - A worker thread of a multi-thread runtime (a task, even on the only
    ///   worker; the transport's runtime or the application's) runs the wait
    ///   in [`block_in_place`](tokio::task::block_in_place), which hands its
    ///   core and queued tasks to another thread first. This also covers
    ///   every worker of the runtime blocking at once. Blocking directly
    ///   would also strand the task in the worker's LIFO slot (typically one
    ///   the caller just spawned, such as the one feeding the inputs), which
    ///   no other worker can take.
    /// - A `current_thread` runtime only progresses while some thread drives
    ///   it, so when the caller is inside a `current_thread` runtime context
    ///   (it may be that thread), or the transport runtime is a
    ///   `current_thread` runtime (nobody may be driving it), the wait first
    ///   spawns a no-op probe on the transport runtime and returns
    ///   [`RuntimeError::InvalidConfig`] if it does not run within a grace
    ///   period. A `spawn_blocking` thread whose runtime another thread
    ///   drives passes.
    /// - Every other thread holds no worker core and blocks directly.
    ///
    /// tokio has no stable way to ask whether a thread is a multi-thread
    /// worker, and `block_in_place` panics on a thread that drives a
    /// `current_thread` runtime (also when it has entered a multi-thread
    /// runtime's handle). So a thread counts as a worker only if it drives a
    /// scheduler (see [`thread_drives_a_scheduler`]) and carries the name of
    /// a multi-thread runtime's threads: tokio's default name, or a name seen
    /// on the transport runtime or on the runtime that spawned the dataflow
    /// ([`Self::remember_runtime_thread`], the coordinator). A worker whose
    /// name was not seen (a runtime naming every thread differently) blocks
    /// directly; a thread driving a `current_thread` runtime that carries one
    /// of these names and has entered a multi-thread handle would make
    /// `block_in_place` panic.
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
        if caller == Some(RuntimeFlavor::MultiThread) && self.on_a_multi_thread_worker() {
            return Ok(tokio::task::block_in_place(wait));
        }
        Ok(wait())
    }

    /// Whether the current thread is a multi-thread runtime's worker that
    /// holds its core (see [`Self::block`]).
    fn on_a_multi_thread_worker(&self) -> bool {
        if !thread_drives_a_scheduler() {
            return false;
        }
        let thread = std::thread::current();
        thread.name().is_some_and(|name| {
            name == TOKIO_DEFAULT_THREAD_NAME
                || lock_worker_names(&self.shared)
                    .iter()
                    .any(|known| known == name)
        })
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

/// Corrupt traffic received from the peer, classified by cause: a frame that
/// fails to decode or its checksum, a received frame announcing a payload
/// over the frame limit (`received`; an outbound one is ours), or a frame
/// that is out of protocol (a progress batch after the channel's
/// end-of-stream). Unlike a loss — EOF, a frame truncated by a peer's
/// teardown, a reorder timeout, a shared manager dropping or reconnecting —
/// this never happens in a healthy run, so it is fatal in every phase and
/// even on a retired link: whatever the frame carried is lost, and nothing
/// proves the local result did not need it.
fn is_inbound_corruption(failure: &LinkFailure, received: bool) -> bool {
    match failure {
        LinkFailure::Decode(_) | LinkFailure::ProtocolViolation(_) => true,
        LinkFailure::Transport(TransportError::ChecksumMismatch { .. }) => true,
        LinkFailure::Transport(TransportError::PayloadTooLarge { .. }) => received,
        _ => false,
    }
}

/// Whether `event` reports corrupt traffic (see [`is_inbound_corruption`]).
fn is_corruption_event(event: &LinkEvent) -> bool {
    match event {
        LinkEvent::DataChannelEnded(failure) | LinkEvent::ProgressChannelEnded(failure) => {
            is_inbound_corruption(failure, true)
        }
        LinkEvent::ControlUndecodable(_) => true,
        LinkEvent::ConnectionFailed(error) => matches!(
            error,
            TransportError::ChecksumMismatch { .. } | TransportError::PayloadTooLarge { .. }
        ),
        LinkEvent::TransportError(error) => {
            matches!(error, TransportError::ChecksumMismatch { .. })
        }
        _ => false,
    }
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
    /// The dataflow is being cancelled, but a failure means a graceful drain
    /// can no longer complete (a peer stopped without finishing its part):
    /// escalate the local cancellation so a drain in progress stops.
    AbortDrain(CancellationReason),
    /// The local workers did not complete after this node broadcast a
    /// requested stop: tell the peers with a second `Cancel` (at most once),
    /// carrying the failure if there was one. A peer whose drain still waits
    /// for our data stops it (see `CancelReceived`); a failure stops every
    /// peer's drain.
    AnnounceUnfinished,
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
    /// A data channel from the peer ended without its close sentinel (never
    /// in a healthy run). If the peer's goodbye completes anyway, records
    /// may be missing.
    unclean_data_channel: bool,
    /// The peer broadcast `Cancel`. If it was a requested stop the peer may
    /// still drain and say goodbye; a link that ends before that means it
    /// stopped without finishing its part.
    cancel_received: bool,
    /// The peer stopped without finishing its part: a second `Cancel`
    /// (`AnnounceUnfinished`), or its link ended before its goodbye during a
    /// requested stop.
    stopped: bool,
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

    /// The peer cancelled and its link ended (`hard`: failed, or closed
    /// fully) without its goodbye: it stopped without finishing its part.
    fn stopped_after_cancel(&self, hard: bool) -> bool {
        self.cancel_received && !self.goodbye_received && (hard || self.fully_closed())
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
    /// Our cancellation was a requested stop (not a failure), which peers
    /// and our own workers may drain on.
    requested_stop: bool,
    /// [`Action::AbortDrain`] was requested.
    drain_aborted: bool,
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
    /// The peers that stopped without finishing their part during a requested
    /// stop (`PeerState::stopped`): their progress lanes carry nothing more
    /// that a drain could use.
    pub(crate) fn stopped_peers(&self) -> impl Iterator<Item = &str> {
        self.peers
            .iter()
            .filter(|(_, peer)| peer.stopped)
            .map(|(id, _)| id.as_str())
    }

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
            requested_stop: false,
            drain_aborted: false,
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
                            unclean_data_channel: false,
                            cancel_received: false,
                            stopped: false,
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

    /// A recorded data loss that no `Cancel` has announced to the peers yet
    /// (it was recorded after the machine finished: queued behind the event
    /// that finished it). Marks it announced: the caller broadcasts `Cancel`.
    pub(crate) fn take_unannounced_loss(&mut self) -> Option<DataLoss> {
        if self.cancel_broadcast {
            return None;
        }
        let loss = self.data_loss.clone()?;
        self.cancel_broadcast = true;
        Some(loss)
    }

    /// Whether every peer has acknowledged our stream or finished its own.
    #[cfg(test)]
    fn all_retired(&self) -> bool {
        self.peers.values().all(PeerState::retired)
    }

    pub(crate) fn on_event(&mut self, event: LifecycleEvent) -> Vec<Action> {
        match event {
            LifecycleEvent::WorkerFinished { success, .. } => self.on_worker_finished(success),
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
                // The token may already hold a requested stop that this
                // coordinator has not seen yet (a worker that does not drain
                // reported first): that is what the broadcast then says. The
                // driver sends this only if the broadcast was not a failure.
                actions.push(Action::AnnounceUnfinished);
            } else if self.requested_stop {
                // Our Cancel announced a requested stop that peers may drain
                // on; our part will never complete.
                actions.push(Action::AnnounceUnfinished);
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
    ///
    /// `failure`: the cancellation reports a failure, so nothing drains on
    /// it. Otherwise it is a requested stop: a loss that prevents a drain
    /// from completing stops it (`AbortDrain`), and workers that do not
    /// complete forward their failure (`AnnounceUnfinished`).
    pub(crate) fn on_dataflow_cancelled(&mut self, failure: bool) -> Vec<Action> {
        if self.phase == Phase::Running && !self.cancel_broadcast {
            self.cancel_broadcast = true;
            self.requested_stop = !failure;
            let mut actions = vec![Action::BroadcastCancel];
            // A peer may have stopped before we saw the cancellation (its
            // `Cancel` cancelled our token, and its events were applied first).
            let stopped: Vec<String> = self
                .peers
                .iter()
                .filter(|(_, peer)| peer.stopped)
                .map(|(id, _)| id.clone())
                .collect();
            for peer in stopped {
                actions.extend(self.abort_drain_without(&peer));
            }
            actions
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
                    // A data channel ended without its sentinel before the
                    // goodbye was complete (the events come from different
                    // bridge tasks, in either order).
                    if peer.unclean_data_channel {
                        actions.extend(self.missing_close_marker(peer_id));
                    }
                }
            }
            LinkEvent::AckReceived => {
                if goodbye_sent {
                    peer.ack_received = true;
                    peer.link.retire();
                } else {
                    // A peer acknowledges only our end-of-stream, which we
                    // queue after deciding to send it: out of protocol.
                    actions.extend(self.on_corruption(
                        peer_id,
                        LinkFailure::ProtocolViolation(
                            "acknowledgement before our end-of-stream".into(),
                        ),
                    ));
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
                if peer.cancel_received && !peer.goodbye_received {
                    // A second Cancel: the peer's workers did not complete
                    // after its requested stop (`AnnounceUnfinished`).
                    peer.stopped = true;
                    actions.extend(self.abort_drain_without(&peer_id));
                } else {
                    peer.cancel_received = true;
                }
            }
            LinkEvent::ControlClosed => {
                peer.control_closed = true;
                // Every channel from the peer has ended without a goodbye.
                // The dispatcher reports this after any Cancel queued before
                // the close, so a peer's Cancel is always applied first.
                if peer.fully_closed() && (!peer.resolved() || peer.stopped_after_cancel(false)) {
                    actions.extend(self.on_link_failure(peer_id, LinkFailure::ChannelClosed, true));
                }
            }
            LinkEvent::ControlUndecodable(detail) => {
                // A corrupted control message may have been a Cancel: the
                // link can no longer be trusted.
                actions.extend(self.on_corruption(
                    peer_id,
                    LinkFailure::Decode(format!("control message: {detail}")),
                ));
            }
            LinkEvent::ProgressChannelEnded(failure) => {
                // A frame after the end-of-stream comes from a channel that
                // was already counted.
                if !matches!(failure, LinkFailure::ProtocolViolation(_)) {
                    peer.progress_channels_ended += 1;
                }
                if is_inbound_corruption(&failure, true) {
                    actions.extend(self.on_corruption(peer_id, failure));
                } else {
                    let hard = !matches!(failure, LinkFailure::ChannelClosed);
                    actions.extend(self.on_link_failure(peer_id, failure, hard));
                }
            }
            LinkEvent::DataChannelEnded(failure) => {
                if is_inbound_corruption(&failure, true) {
                    actions.extend(self.on_corruption(peer_id, failure));
                } else {
                    peer.unclean_data_channel = true;
                    if peer.goodbye_received {
                        actions.extend(self.missing_close_marker(peer_id));
                    } else {
                        // Like an EOF on any other channel: the link has
                        // failed once all of it has closed — unless the
                        // goodbye completes after all (see
                        // `missing_close_marker`).
                        actions.extend(self.on_link_failure(peer_id, failure, false));
                    }
                }
            }
            LinkEvent::ConnectionFailed(error) => {
                // The dedicated connection ended: every channel from the peer
                // closes right after this (and not before: see
                // `PeerIoNotice`), so a corrupt or oversize frame is never
                // mistaken for a clean disconnect.
                let failure = LinkFailure::Transport(error);
                if is_inbound_corruption(&failure, true) {
                    actions.extend(self.on_corruption(peer_id, failure));
                } else {
                    actions.extend(self.on_link_failure(peer_id, failure, false));
                }
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
                let failure = LinkFailure::Transport(error);
                if is_inbound_corruption(&failure, false) {
                    actions.extend(self.on_corruption(peer_id, failure));
                } else {
                    actions.extend(self.on_link_failure(peer_id, failure, true));
                }
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

    /// The peer finished (its workers succeeded, and a successful worker
    /// closes every data channel before its goodbye), yet one of its data
    /// channels ended without its close marker: records our running workers
    /// still read may be missing, and that input can never be exhausted.
    /// After local success our inputs were complete, so it does not matter.
    /// While the dataflow is being cancelled it counts only if every worker
    /// still succeeds (drain on cancel), like any other loss.
    fn missing_close_marker(&mut self, peer_id: String) -> Vec<Action> {
        if self.phase != Phase::Running {
            return Vec::new();
        }
        let loss = DataLoss {
            peer: peer_id,
            detail: "a data channel ended without its close marker although the peer finished"
                .into(),
        };
        if self.cancel_broadcast {
            // The channel can never be exhausted: a drain cannot complete.
            let reason = CancellationReason::NetworkError {
                detail: loss.to_string(),
            };
            self.pending_loss.get_or_insert(loss);
            return self.abort_drain(reason);
        }
        self.fail_running_with_data_loss(loss)
    }

    /// Records our running workers need can no longer arrive: fail the
    /// dataflow on every node now, and report the loss (not deferred: the
    /// affected input will never be exhausted, so the workers would wait).
    fn fail_running_with_data_loss(&mut self, loss: DataLoss) -> Vec<Action> {
        if self.data_loss.is_none() {
            #[cfg(feature = "tracing")]
            tracing::error!("{loss}");
            self.data_loss = Some(loss);
        }
        let mut actions = vec![Action::CancelDataflow(self.data_loss_reason())];
        if !self.cancel_broadcast {
            self.cancel_broadcast = true;
            actions.push(Action::BroadcastCancel);
        }
        actions
    }

    /// Corrupt traffic from the peer (see [`is_inbound_corruption`]). Fatal
    /// in every phase before teardown and regardless of retirement, and
    /// recorded as data loss so that a local success (one racing with the
    /// cancellation, or one already reported) still fails: while the
    /// workers run the dataflow is cancelled everywhere; after they
    /// succeeded, the peers are cancelled and the completion reports the
    /// loss.
    fn on_corruption(&mut self, peer_id: String, failure: LinkFailure) -> Vec<Action> {
        if self.phase == Phase::Finished {
            // Seen while tearing down (it was queued behind the event that
            // finished the machine): still recorded, nobody left to tell.
            self.data_loss.get_or_insert_with(|| DataLoss {
                peer: peer_id,
                detail: format!("corrupt traffic from the peer: {failure}"),
            });
            return Vec::new();
        }
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            if !peer.retired() {
                peer.lost = true;
            }
            peer.soft_failure = None;
        }
        if self.data_loss.is_none() {
            let loss = DataLoss {
                peer: peer_id.clone(),
                detail: format!("corrupt traffic from the peer: {failure}"),
            };
            #[cfg(feature = "tracing")]
            tracing::error!("{loss}");
            self.data_loss = Some(loss);
        }
        match self.phase {
            Phase::Running if self.cancel_broadcast => self.abort_drain(self.data_loss_reason()),
            Phase::Running => {
                self.cancel_broadcast = true;
                vec![
                    Action::LinkFailed {
                        peer: peer_id,
                        failure,
                    },
                    Action::BroadcastCancel,
                ]
            }
            Phase::Draining => self.abandon_after_data_loss(),
            Phase::Finished => Vec::new(),
        }
    }

    /// After local success with lost data: cancel the peers and stop waiting
    /// for their acknowledgements (they stop waiting for us on our Cancel).
    fn abandon_after_data_loss(&mut self) -> Vec<Action> {
        let mut actions = vec![Action::CancelDataflow(self.data_loss_reason())];
        if !self.cancel_broadcast {
            self.cancel_broadcast = true;
            actions.push(Action::BroadcastCancel);
        } else if self.requested_stop {
            actions.push(Action::AnnounceUnfinished);
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
            if peer.stopped || peer.stopped_after_cancel(hard) {
                // The peer cancelled, then its link ended before its goodbye
                // (or it said it stopped): it did not finish its part (its
                // runtime went away, its workers failed or did not drain).
                // Re-checked as our sends to it fail.
                peer.stopped = true;
                return self.abort_drain_without(&peer_id);
            }
            // Retired: neither side needs this link any more.
            return Vec::new();
        }
        if peer.unsent() && (hard || peer.fully_closed()) {
            return self.settle_unsent(peer_id, Some(failure));
        }
        match self.phase {
            Phase::Running if self.cancel_broadcast && !self.requested_stop => {
                // The dataflow is already failing; peers tearing down is
                // expected. Wait for the local workers instead.
                peer.lost = true;
                Vec::new()
            }
            Phase::Running if self.cancel_broadcast => {
                // A requested stop, but this peer has not finished its part:
                // a drain cannot complete without it. An EOF waits for the
                // rest of the link (a Cancel queued behind it explains the
                // loss).
                if !hard && !peer.fully_closed() {
                    return Self::defer_soft_failure(peer, peer_id, failure);
                }
                peer.lost = true;
                peer.stopped = true;
                #[cfg(not(feature = "tracing"))]
                let _ = failure;
                #[cfg(feature = "tracing")]
                tracing::warn!(peer = %peer_id, %failure, "link ended during a requested stop");
                self.abort_drain_without(&peer_id)
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

    /// A failure while the dataflow is being cancelled (`cancel_broadcast`)
    /// and the local workers still run: if they are draining, the drain can no
    /// longer complete, so it is stopped (the cancellation is escalated). The
    /// workers then fail, and [`Action::AnnounceUnfinished`] tells the peers.
    fn abort_drain(&mut self, reason: CancellationReason) -> Vec<Action> {
        if self.phase != Phase::Running || !self.requested_stop || self.drain_aborted {
            return Vec::new();
        }
        self.drain_aborted = true;
        vec![Action::AbortDrain(reason)]
    }

    /// `peer_id` stopped without finishing its part during a requested stop.
    /// A drain whose data push to the peer found its queue closed (it backs
    /// off on a frame that can never leave) can never complete, and is
    /// stopped at once. Otherwise the drain is left alone: whether it still
    /// needs anything from the peer is decided by progress, not by which
    /// channels are open (under port-counted progress a peer's channels stay
    /// open until every worker's input upstream of them closes). A draining
    /// worker fails promptly once no update can arrive and its view still
    /// holds work (R48, see the executor's drain handling).
    fn abort_drain_without(&mut self, peer_id: &str) -> Vec<Action> {
        let needed = self
            .peers
            .get(peer_id)
            .is_some_and(|peer| peer.link.push_blocked());
        if !needed {
            return Vec::new();
        }
        self.abort_drain(CancellationReason::NetworkError {
            detail: format!(
                "peer {peer_id} stopped before finishing its part of the cancelled dataflow, \
                 which the drain needs"
            ),
        })
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
    // A failure that escalated a requested stop before we broadcast it.
    if let Some(failure) = dataflow_cancel.escalation() {
        return failure.to_string();
    }
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
    /// A `Cancel` describing a failure went out.
    pub(crate) failure_announced: bool,
    /// [`Action::AnnounceUnfinished`] was sent.
    pub(crate) unfinished_announced: bool,
    /// The first local worker error other than a cancellation.
    pub(crate) worker_failure: Option<String>,
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
            self._teardown.note_runtime_thread();
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
                    let failure = self
                        .dataflow_cancel
                        .reason()
                        .is_some_and(|reason| reason.is_failure());
                    actions.extend(self.machine.on_dataflow_cancelled(failure));
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
            // A peer that stopped without finishing its part sends no more
            // progress: its lanes close (after the actions above, so a
            // forwarded failure or an aborted drain is seen first), and a
            // draining worker can tell its drain still needs that peer (R48).
            // In Shared mode nothing else closes them: the shared link outlives
            // the peer's dataflow.
            if let Some(handles) = &self.progress_handles {
                for peer in self.machine.stopped_peers() {
                    handles.close_peer(peer);
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
            LifecycleEvent::WorkerFinished { success, failure } => {
                if let Some(failure) = failure {
                    self.worker_failure.get_or_insert(failure);
                }
                self.machine.on_event(LifecycleEvent::WorkerFinished {
                    success,
                    failure: None,
                })
            }
            event => self.machine.on_event(event),
        }
    }

    /// Broadcast `Cancel` with `reason` (bounded), remembering whether it
    /// told the peers about a failure.
    async fn broadcast_cancel(&mut self, reason: String) {
        self.failure_announced |= CancellationReason::describes_failure(&reason);
        let _ = tokio::time::timeout(
            self.cancel_broadcast_timeout,
            broadcast_cancel_with_transport(&self.transport, self.dataflow_id, &reason),
        )
        .await;
    }

    /// The reason of the second `Cancel` sent when the local workers did not
    /// complete after a requested stop (see [`Action::AnnounceUnfinished`]):
    /// a worker error, or a failure that stopped the local drain (a peer's,
    /// a lost link, lost records), which stops every peer's drain; otherwise
    /// the requested stop itself, marked as unfinished, which stops only the
    /// drains that still wait for this node's data.
    fn unfinished_reason(&self) -> String {
        if let Some(detail) = &self.worker_failure {
            return CancellationReason::WorkerFailed {
                detail: detail.clone(),
            }
            .to_string();
        }
        if let Some(reason) = self.dataflow_cancel.escalation() {
            return reason.to_string();
        }
        format!(
            "{} (the node stopped before finishing its part of the dataflow)",
            cancel_broadcast_reason(&self.dataflow_cancel)
        )
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
                self.broadcast_cancel(reason).await;
                false
            }
            Action::AnnounceUnfinished => {
                if !self.failure_announced && !self.unfinished_announced {
                    self.unfinished_announced = true;
                    let reason = self.unfinished_reason();
                    self.broadcast_cancel(reason).await;
                }
                false
            }
            Action::AbortDrain(reason) => {
                self.dataflow_cancel.escalate(reason);
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
                // Escalated: this also stops a drain on an earlier requested
                // stop.
                self.dataflow_cancel.escalate(reason);
                false
            }
            Action::CancelDataflow(reason) => {
                self.dataflow_cancel.escalate(reason);
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
        }
        // Reports queued behind the event that finished the machine: data
        // lost on the way in, and corrupt traffic, still fail the result.
        while let Ok(event) = self.events.try_recv() {
            if let LifecycleEvent::Link { event: link, .. } = &event {
                let lost = drain_inbound && matches!(link, LinkEvent::InboundDataLost(_));
                if lost || is_corruption_event(link) {
                    let _ = self.machine.on_event(event);
                }
            }
        }
        let unannounced = match self.machine.take_unannounced_loss() {
            Some(loss) => Some(loss),
            // Our Cancel announced a requested stop that peers may drain on.
            None if !self.failure_announced => self.machine.data_loss().cloned(),
            None => None,
        };
        if let Some(loss) = unannounced {
            // A loss recorded once the machine had finished (in this drain,
            // or among events applied just before teardown) was announced to
            // nobody. A peer that only acknowledged us may still be running
            // on our now-invalid output: tell every peer before the
            // transport goes.
            let reason = CancellationReason::NetworkError {
                detail: loss.to_string(),
            }
            .to_string();
            self.broadcast_cancel(reason).await;
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
        LifecycleEvent::WorkerFinished {
            success,
            failure: None,
        }
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
        assert!(m.on_dataflow_cancelled(false).is_empty());
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
    fn ack_before_goodbye_is_a_protocol_violation() {
        let (mut m, links) = machine(1, &[("b", 1)]);
        let actions = m.on_event(link_event("b", LinkEvent::AckReceived));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::LinkFailed { .. }, Action::BroadcastCancel]
            ),
            "{actions:?}"
        );
        assert!(!links[0].is_retired());
        assert!(m.data_loss().is_some());
    }

    /// Corrupt traffic queued behind the event that finished the machine is
    /// still recorded when teardown drains it.
    #[test]
    fn corruption_after_the_machine_finished_is_still_recorded() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        assert!(is_teardown(
            &m.on_event(link_event("b", LinkEvent::AckReceived)),
            true
        ));
        let late = LinkEvent::ProgressChannelEnded(LinkFailure::ProtocolViolation(
            "frame after end-of-stream".into(),
        ));
        assert!(is_corruption_event(&late));
        assert!(m.on_event(link_event("b", late)).is_empty());
        assert!(m.data_loss().is_some());
        // Nobody was told yet: teardown must broadcast, once.
        assert!(m.take_unannounced_loss().is_some());
        assert!(m.take_unannounced_loss().is_none());
        // A plain loss is not.
        assert!(!is_corruption_event(&LinkEvent::ProgressChannelEnded(
            LinkFailure::ChannelClosed
        )));
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
        // A failure cancelled the dataflow: nothing more to forward (see
        // `workers_failing_after_a_broadcast_cancel_forward_the_failure`).
        let (mut m, _) = machine(2, &[("b", 1)]);
        let actions = m.on_dataflow_cancelled(true);
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
        // `AnnounceUnfinished` goes out only if the broadcast reason was a
        // requested stop the coordinator had not seen yet.
        assert!(matches!(
            actions.as_slice(),
            [
                Action::BroadcastCancel,
                Action::AnnounceUnfinished,
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
        // A failure cancelled the dataflow (nothing drains); for a requested
        // stop see `link_end_before_goodbye_during_a_cancellation_aborts_the_drain`.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_dataflow_cancelled(true).as_slice(),
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
                    m.on_dataflow_cancelled(false).as_slice(),
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
            m.on_dataflow_cancelled(false).as_slice(),
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
        // Corrupt traffic: the local result is not trusted.
        assert!(m.data_loss().is_some());
    }

    fn has_cancel_everywhere(actions: &[Action]) -> bool {
        actions.iter().any(|a| matches!(a, Action::BroadcastCancel))
    }

    /// Corrupt traffic is fatal and durable in every phase, also on a
    /// retired link: a worker that races to success still reports it.
    #[test]
    fn corrupt_traffic_is_durable_data_loss_in_every_phase() {
        let corruptions = || {
            [
                LinkEvent::ProgressChannelEnded(LinkFailure::Decode("crc".into())),
                LinkEvent::DataChannelEnded(LinkFailure::Decode("tag".into())),
                LinkEvent::ControlUndecodable("crc".into()),
                LinkEvent::ConnectionFailed(TransportError::ChecksumMismatch {
                    expected: 1,
                    actual: 2,
                }),
                LinkEvent::ConnectionFailed(TransportError::PayloadTooLarge { size: 2, max: 1 }),
                LinkEvent::TransportError(TransportError::ChecksumMismatch {
                    expected: 1,
                    actual: 2,
                }),
            ]
        };
        // While running: cancel everywhere; a racing success still fails.
        for event in corruptions() {
            let (mut m, _) = machine(1, &[("b", 1), ("c", 1)]);
            let actions = m.on_event(link_event("b", event));
            assert!(
                matches!(
                    actions.as_slice(),
                    [Action::LinkFailed { .. }, Action::BroadcastCancel]
                ),
                "{actions:?}"
            );
            assert!(m.data_loss().is_some());
            let actions = m.on_event(done(true));
            assert!(
                !actions.iter().any(|a| matches!(a, Action::SendGoodbye)),
                "{actions:?}"
            );
            assert!(is_teardown(&actions, false), "{actions:?}");
        }
        // On a retired link while running.
        for event in corruptions() {
            let (mut m, links) = machine(1, &[("b", 1), ("c", 1)]);
            m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
            assert!(links[0].is_retired());
            let actions = m.on_event(link_event("b", event));
            assert!(has_cancel_everywhere(&actions), "{actions:?}");
            assert!(m.data_loss().is_some());
        }
        // After local success: the result becomes an error, every peer is
        // told (also c, which only acknowledged us and may still run), and
        // nothing waits for them.
        for event in corruptions() {
            let (mut m, _) = machine(1, &[("b", 1), ("c", 1)]);
            m.on_event(done(true));
            m.on_event(link_event("c", LinkEvent::AckReceived));
            let actions = m.on_event(link_event("b", event));
            assert!(has_cancel_everywhere(&actions), "{actions:?}");
            assert!(is_teardown(&actions, true), "{actions:?}");
            assert!(m.data_loss().is_some());
            // Already announced by that broadcast.
            assert!(m.take_unannounced_loss().is_none());
        }
    }

    /// Losses are not corruption: an EOF (also mid-frame), a reorder timeout
    /// or a shared-transport error keep the loss rules — never durable data
    /// loss after local success or on a retired link — and our own oversize
    /// frame is not the peer's corrupt traffic.
    #[test]
    fn losses_are_never_durable_after_success_or_on_retired_links() {
        let losses = || {
            [
                LinkEvent::TransportError(TransportError::ReorderTimeout),
                LinkEvent::TransportError(TransportError::ConnectionClosed),
                LinkEvent::TransportError(TransportError::PayloadTooLarge { size: 2, max: 1 }),
                LinkEvent::ConnectionFailed(TransportError::ConnectionClosed),
                LinkEvent::ConnectionFailed(TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "frame truncated by the peer's teardown",
                ))),
                LinkEvent::ProgressChannelEnded(LinkFailure::ChannelClosed),
            ]
        };
        for event in losses() {
            let (mut m, _) = machine(1, &[("b", 1)]);
            m.on_event(done(true));
            m.on_event(link_event("b", event));
            assert!(m.data_loss().is_none());
        }
        for event in losses() {
            let (mut m, _) = machine(1, &[("b", 1)]);
            m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
            assert!(m.on_event(link_event("b", event)).is_empty());
            assert!(m.data_loss().is_none());
        }
        // A reorder timeout while running stays fatal, but not durable.
        let (mut m, _) = machine(1, &[("b", 1)]);
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::TransportError(TransportError::ReorderTimeout),
        ));
        assert!(has_cancel_everywhere(&actions), "{actions:?}");
        assert!(m.data_loss().is_none());
        // Our own frame over the limit is not the peer's corrupt traffic.
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(done(true));
        m.on_event(link_event(
            "b",
            LinkEvent::OutboundFailed(LinkFailure::Transport(TransportError::PayloadTooLarge {
                size: 2,
                max: 1,
            })),
        ));
        assert!(m.data_loss().is_none());
    }

    /// A peer that said goodbye closed every data channel first (a
    /// successful worker always does): one ending without its close marker
    /// while our workers still read it is data loss, at once.
    #[test]
    fn data_channel_without_close_marker_after_goodbye_is_data_loss_while_running() {
        let ended = || link_event("b", LinkEvent::DataChannelEnded(LinkFailure::ChannelClosed));
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        let actions = m.on_event(ended());
        assert!(
            matches!(
                actions.as_slice(),
                [Action::CancelDataflow(_), Action::BroadcastCancel]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
        // Before the goodbye it is an ordinary (soft) loss.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(ended()).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        assert!(m.data_loss().is_none());
        // After local success it is benign (our inputs were complete).
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        m.on_event(done(true));
        assert!(m.on_event(ended()).is_empty());
        assert!(m.data_loss().is_none());
        // The goodbye completing after the channel ended (other bridge task)
        // is caught too, although the link then counts as retired.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(ended()).as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        let actions = m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::CancelDataflow(_))),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
        // While cancelling, it stops a drain (the channel is never exhausted)
        // and is kept pending: it fails a drained success.
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled(false);
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(matches!(
            m.on_event(ended()).as_slice(),
            [Action::AbortDrain(_)]
        ));
        let actions = m.on_event(done(true));
        assert!(is_teardown(&actions, false), "{actions:?}");
        assert!(m.data_loss().is_some());
    }

    #[test]
    fn progress_frame_after_end_of_stream_is_corruption_counted_once() {
        let (mut m, _) = machine(1, &[("b", 2), ("c", 1)]);
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::ProgressChannelEnded(LinkFailure::ProtocolViolation("after eos".into())),
        ));
        assert!(has_cancel_everywhere(&actions), "{actions:?}");
        assert!(m.data_loss().is_some());
        assert_eq!(m.peers["b"].progress_channels_ended, 2);
    }

    /// The dedicated connection's end is a soft loss: decided once the rest
    /// of the link has closed (so a queued Cancel wins).
    #[test]
    fn connection_end_is_a_soft_loss() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(link_event(
                "b",
                LinkEvent::ConnectionFailed(TransportError::ConnectionClosed)
            ))
            .as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        assert!(m.on_event(eof("b")).is_empty());
        let actions = m.on_event(link_event("b", LinkEvent::ControlClosed));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::LinkFailed { .. }, Action::BroadcastCancel]
            ),
            "{actions:?}"
        );
        assert!(m.data_loss().is_none());
    }

    /// The workers did not complete after this node announced a cancellation
    /// that may have been a requested stop: the failure is forwarded, or
    /// peers draining on it would wait for their deadline.
    #[test]
    fn workers_failing_after_a_broadcast_cancel_forward_the_failure() {
        let (mut m, _) = machine(2, &[("b", 1)]);
        assert!(matches!(
            m.on_dataflow_cancelled(false).as_slice(),
            [Action::BroadcastCancel]
        ));
        assert!(m.on_event(done(true)).is_empty());
        let actions = m.on_event(done(false));
        assert!(
            matches!(
                actions.as_slice(),
                [
                    Action::AnnounceUnfinished,
                    Action::Teardown { graceful: false }
                ]
            ),
            "{actions:?}"
        );
        // Without an earlier Cancel the token's reason is broadcast; if that
        // is a requested stop the coordinator has not seen yet (the worker
        // reported first), the driver also announces the unfinished part.
        let (mut m, _) = machine(1, &[("b", 1)]);
        assert!(matches!(
            m.on_event(done(false)).as_slice(),
            [
                Action::BroadcastCancel,
                Action::AnnounceUnfinished,
                Action::Teardown { graceful: false }
            ]
        ));
        // A drained success forwards nothing.
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled(false);
        let actions = m.on_event(done(true));
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::AnnounceUnfinished)),
            "{actions:?}"
        );
        assert!(is_teardown(&actions, true), "{actions:?}");
        // Records lost while cancelling fail a drained success: forwarded.
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled(false);
        m.on_inbound_data_lost("b".into(), "lost".into(), true);
        let actions = m.on_event(done(true));
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, Action::AnnounceUnfinished)),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
    }

    /// During a requested stop (a drain may be running), a peer whose link
    /// ends before its goodbye did not finish its part: a drain whose push to
    /// it is blocked is stopped, once. An EOF waits for
    /// the rest of the link (a Cancel queued behind it explains the loss),
    /// and a peer that cancelled is no exception: it may still drain and say
    /// goodbye, but not after its link ended. A drain that needs nothing more
    /// from the peer is left to complete.
    #[test]
    fn link_end_before_goodbye_during_a_cancellation_aborts_the_drain() {
        let closed = || LinkEvent::TransportError(TransportError::ConnectionClosed);
        let (mut m, links) = machine(1, &[("b", 1), ("c", 1)]);
        for link in &links {
            link.note_push_blocked();
        }
        m.on_dataflow_cancelled(false);
        let actions = m.on_event(link_event("b", closed()));
        assert!(
            matches!(
                actions.as_slice(),
                [Action::AbortDrain(CancellationReason::NetworkError { .. })]
            ),
            "{actions:?}"
        );
        assert!(m.on_event(link_event("c", closed())).is_empty());

        let (mut m, links) = machine(1, &[("b", 1)]);
        links[0].note_push_blocked();
        m.on_dataflow_cancelled(false);
        assert!(matches!(
            m.on_event(link_event(
                "b",
                LinkEvent::DataChannelEnded(LinkFailure::ChannelClosed)
            ))
            .as_slice(),
            [Action::ArmSoftFailureTimer(_)]
        ));
        assert!(m.on_event(eof("b")).is_empty());
        let actions = m.on_event(link_event("b", LinkEvent::ControlClosed));
        assert!(
            matches!(actions.as_slice(), [Action::AbortDrain(_)]),
            "{actions:?}"
        );

        // The peer cancelled, then its link closed without its goodbye (or
        // failed).
        for last in [LinkEvent::ControlClosed, closed()] {
            let (mut m, links) = machine(1, &[("b", 1)]);
            links[0].note_push_blocked();
            m.on_dataflow_cancelled(false);
            assert!(
                m.on_event(link_event("b", LinkEvent::CancelReceived))
                    .is_empty()
            );
            assert!(m.on_event(eof("b")).is_empty());
            let actions = m.on_event(link_event("b", last));
            assert!(
                matches!(actions.as_slice(), [Action::AbortDrain(_)]),
                "{actions:?}"
            );
        }

        // Whether the drain still needs the peer is decided by progress (R48),
        // not by which of its channels are open; a push blocked on the peer
        // aborts it.
        for blocked in [false, true] {
            let (mut m, links) = machine(1, &[("b", 1)]);
            if blocked {
                links[0].note_push_blocked();
            }
            m.on_dataflow_cancelled(false);
            m.on_event(link_event("b", LinkEvent::CancelReceived));
            m.on_event(eof("b"));
            let actions = m.on_event(link_event("b", LinkEvent::ControlClosed));
            assert_eq!(
                matches!(actions.as_slice(), [Action::AbortDrain(_)]),
                blocked,
                "{actions:?}"
            );
        }

        // Not once it said goodbye (it drained and finished).
        let (mut m, links) = machine(1, &[("b", 1)]);
        links[0].note_push_blocked();
        m.on_dataflow_cancelled(false);
        m.on_event(link_event("b", LinkEvent::CancelReceived));
        m.on_event(link_event("b", LinkEvent::ProgressEndOfStream));
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        assert!(m.on_event(link_event("b", closed())).is_empty());

        // Nor once the local workers are done, nor for a failure (nothing
        // drains on it).
        let (mut m, links) = machine(1, &[("b", 1)]);
        links[0].note_push_blocked();
        m.on_dataflow_cancelled(false);
        m.on_event(link_event("b", LinkEvent::CancelReceived));
        m.on_event(done(true));
        assert!(m.on_event(eof("b")).is_empty());
        assert!(
            m.on_event(link_event("b", LinkEvent::ControlClosed))
                .is_empty()
        );
        let (mut m, links) = machine(1, &[("b", 1)]);
        links[0].note_push_blocked();
        m.on_dataflow_cancelled(true);
        assert!(m.on_event(link_event("b", closed())).is_empty());
    }
    /// A second Cancel from a peer means its workers did not complete after
    /// its requested stop: a drain that needs its data stops, also when the
    /// peer's Cancels were applied before our own cancellation was seen (here,
    /// a drain whose push to it is blocked). A drain that does not need it is
    /// left alone.
    #[test]
    fn second_cancel_from_a_peer_stops_drains_that_need_it() {
        for cancelled_first in [true, false] {
            let (mut m, links) = machine(1, &[("b", 1)]);
            links[0].note_push_blocked();
            if cancelled_first {
                m.on_dataflow_cancelled(false);
            }
            assert!(
                m.on_event(link_event("b", LinkEvent::CancelReceived))
                    .is_empty()
            );
            let mut actions = m.on_event(link_event("b", LinkEvent::CancelReceived));
            if !cancelled_first {
                assert!(actions.is_empty(), "{actions:?}");
                actions = m.on_dataflow_cancelled(false);
            }
            assert!(
                actions.iter().any(|a| matches!(a, Action::AbortDrain(_))),
                "cancelled_first={cancelled_first}: {actions:?}"
            );
        }
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled(false);
        m.on_event(link_event("b", LinkEvent::CancelReceived));
        assert!(
            m.on_event(link_event("b", LinkEvent::CancelReceived))
                .is_empty()
        );
    }

    /// A drain whose data push to a stopped peer found its queue closed
    /// cannot complete either: the push backs off on a frame that can never
    /// leave. Re-checked when the push reports it after the peer was seen to
    /// stop. Any other failed send (of progress, say) does not matter.
    #[test]
    fn blocked_push_to_a_stopped_peer_aborts_the_drain() {
        for blocked in [true, false] {
            let (mut m, links) = machine(1, &[("b", 1)]);
            m.on_dataflow_cancelled(false);
            m.on_event(link_event("b", LinkEvent::CancelReceived));
            m.on_event(eof("b"));
            assert!(
                m.on_event(link_event("b", LinkEvent::ControlClosed))
                    .is_empty()
            );
            if blocked {
                links[0].note_push_blocked();
            }
            let actions = m.on_event(link_event(
                "b",
                LinkEvent::OutboundFailed(LinkFailure::SendFailed),
            ));
            assert_eq!(
                matches!(actions.as_slice(), [Action::AbortDrain(_)]),
                blocked,
                "{actions:?}"
            );
        }
    }
    /// Corrupt traffic while the dataflow is being cancelled: the drain stops
    /// (the loss is durable anyway).
    #[test]
    fn corruption_during_a_cancellation_aborts_the_drain() {
        let (mut m, _) = machine(1, &[("b", 1)]);
        m.on_dataflow_cancelled(false);
        let actions = m.on_event(link_event(
            "b",
            LinkEvent::DataChannelEnded(LinkFailure::Decode("tag".into())),
        ));
        assert!(
            matches!(actions.as_slice(), [Action::AbortDrain(_)]),
            "{actions:?}"
        );
        assert!(m.data_loss().is_some());
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
            m.on_dataflow_cancelled(false);
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
            LinkEvent::TransportError(TransportError::ReorderTimeout),
            LinkEvent::CancelReceived,
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
        m.on_dataflow_cancelled(false);
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

    /// What `spawn_cluster` does: learn how `rt` names its threads.
    async fn learn_runtime_threads(waiter: &TeardownWaiter, rt: &tokio::runtime::Handle) {
        let name = rt
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        waiter.remember_runtime_thread(&name.expect("tokio names its threads"));
    }

    /// Completes the teardown signal on the current runtime a little later.
    fn complete_later(notifier: TeardownNotifier) {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(notifier);
        });
    }

    /// On a multi-thread runtime, a worker hands its work over
    /// (`block_in_place`): blocking the only worker thread does not stop the
    /// runtime from completing teardown. Threads that hold no worker core
    /// block directly, even when named like the workers: the thread in
    /// `block_on`, and a task of a `LocalSet` it drives (where
    /// `block_in_place` would panic).
    #[test]
    fn blocking_wait_on_a_multi_thread_runtime_keeps_it_running() {
        const NAME: &str = "teardown-test-worker";
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name(NAME.into())
            .spawn(move || {
                let mt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name(NAME)
                    .enable_all()
                    .build()
                    .unwrap();
                let (notifier, waiter) = teardown_signal(mt.handle().clone());
                let from_task = mt.block_on(async move {
                    learn_runtime_threads(&waiter, &tokio::runtime::Handle::current()).await;
                    tokio::spawn(async move {
                        complete_later(notifier);
                        waiter.block(|| waiter.wait())
                    })
                    .await
                    .unwrap()
                });
                let (notifier, waiter) = teardown_signal(mt.handle().clone());
                let from_block_on = mt.block_on(async move {
                    learn_runtime_threads(&waiter, &tokio::runtime::Handle::current()).await;
                    complete_later(notifier);
                    waiter.block(|| waiter.wait())
                });
                let (notifier, waiter) = teardown_signal(mt.handle().clone());
                let from_local_task = mt.block_on(async move {
                    learn_runtime_threads(&waiter, &tokio::runtime::Handle::current()).await;
                    complete_later(notifier);
                    tokio::task::LocalSet::new()
                        .run_until(async move {
                            tokio::task::spawn_local(async move { waiter.block(|| waiter.wait()) })
                                .await
                                .unwrap()
                        })
                        .await
                });
                let _ = done_tx.send((
                    from_task.is_ok(),
                    from_block_on.is_ok(),
                    from_local_task.is_ok(),
                ));
            })
            .unwrap();
        let outcome = done_rx.recv_timeout(Duration::from_secs(20));
        assert_eq!(
            outcome,
            Ok((true, true, true)),
            "the blocking wait deadlocked or panicked"
        );
    }

    /// A thread driving a `current_thread` runtime that has entered a
    /// multi-thread runtime's handle is no worker of it (`block_in_place`
    /// would panic): the wait blocks directly and completes, as the
    /// transport's workers make progress, both in the `block_on` body and in
    /// a task. A panic would end the thread without reporting.
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
                mt.block_on(learn_runtime_threads(&waiter, mt.handle()));
                mt.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(notifier);
                });
                let handle = mt.handle().clone();
                ct.block_on(async move {
                    let wait = move || {
                        let _entered = handle.enter();
                        waiter.block(|| waiter.wait()).is_ok()
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
            "the blocking wait failed, panicked or deadlocked"
        );
    }

    /// `thread_drives_a_scheduler` tells the threads that hold a scheduler
    /// (a multi-thread worker's core, a `current_thread` runtime) from the
    /// others, using tokio's public behaviour only.
    #[test]
    fn thread_drives_a_scheduler_matches_tokio() {
        assert!(!thread_drives_a_scheduler(), "outside any runtime");
        let mt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let observed = mt.block_on(async {
            let in_block_on = thread_drives_a_scheduler();
            let in_task = tokio::spawn(async { thread_drives_a_scheduler() })
                .await
                .unwrap();
            let in_block_in_place =
                tokio::spawn(async { tokio::task::block_in_place(thread_drives_a_scheduler) })
                    .await
                    .unwrap();
            let in_spawn_blocking = tokio::task::spawn_blocking(thread_drives_a_scheduler)
                .await
                .unwrap();
            let in_local_task = tokio::task::LocalSet::new()
                .run_until(async {
                    tokio::task::spawn_local(async { thread_drives_a_scheduler() })
                        .await
                        .unwrap()
                })
                .await;
            (
                in_block_on,
                in_task,
                in_block_in_place,
                in_spawn_blocking,
                in_local_task,
            )
        });
        assert_eq!(
            observed,
            (false, true, false, false, false),
            "multi-thread: block_on, task, block_in_place, spawn_blocking, LocalSet task"
        );
        let ct = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observed = ct.block_on(async {
            let in_block_on = thread_drives_a_scheduler();
            let in_task = tokio::spawn(async { thread_drives_a_scheduler() })
                .await
                .unwrap();
            (in_block_on, in_task)
        });
        assert_eq!(observed, (true, true), "current_thread: block_on, task");
    }
}
