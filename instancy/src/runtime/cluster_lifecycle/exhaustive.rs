//! The lifecycle machine checked exhaustively: the real [`LifecycleMachine`]
//! driven through every order in which its events can reach it, for one
//! peer, within small bounds.
//!
//! `ClusterTransport.tla` checks a hand-written copy of this machine
//! (`Notices = "async"`; see `docs/design/tla/README.md`, Late link events).
//! This checks the code itself against the same rules, and one about the
//! records themselves:
//! - `NoOkAfterFailedSend`: a run whose send had failed when its workers
//!   succeeded (the transport knew) ends `Ok` only if the peer retired the
//!   link or cancelled;
//! - `NoOkOverKnownLoss`: a run whose records never reached the peer ends
//!   `Ok` only if the peer cancelled, once our side had seen the connection
//!   fail when the workers succeeded (a write error, or a failed read);
//! - `NoFalseSettle`: a send owed is never settled as a failure (a link
//!   failure, or a loss recorded or held) while the peer's `Cancel`, goodbye
//!   or ack is on its way to the coordinator, nor by the grace period once
//!   the peer said goodbye and the link closed fully (a teardown from outside
//!   settles at once, and is not checked);
//! - `NoLossWithoutFailedSend`: a run reports a data loss only if a send to
//!   the peer failed (nothing else here can lose records);
//! - termination: once the peer has gone, the machine always finishes.
//!
//! What is written by hand here is the environment, not the decisions:
//! - the coordinator's event channel, one FIFO fed by producers whose sends
//!   interleave freely, each in its own order: the io forwarder (transport
//!   notices), the control dispatcher (a `Cancel`: the token escalated, then
//!   the event, separately; then the channel's close), one progress bridge
//!   per channel (an end-of-stream or an ack,
//!   then the channel's close, reported only without an end-of-stream), the
//!   outbox forwarder (a frame the transport refused), the workers;
//! - the read side's end handed on before its channels close
//!   (`PeerIoNotice`);
//! - the transport of each mode: Shared fails the lane on a write or read
//!   error and closes the whole connection; Dedicated fails only the writer
//!   on a write error, and its writer fails at the next write after a reset.
//!   Shared mode also runs with the lane and the peer's frames on different
//!   connections (`split`), where a failure of one leaves the other;
//! - our side: workers that succeed or fail (on their own, or after a
//!   cancellation), a local cancellation (a requested stop, or a failure)
//!   and the bridges cancelled from outside (a participant reported
//!   leaving; here with the token cancelled or not) at any time;
//!   workers may still finish after a
//!   teardown, and the run's result is judged as the completion makes it;
//! - the peer, which may say goodbye (only once it has our records, which it
//!   needs), ack our goodbye, cancel (a requested stop or a failure), close,
//!   and go away;
//! - at most one fault: a local write error, or a reset;
//! - the coordinator's glue (`ClusterLifecycle::run`): its biased select
//!   (the token, then the bridges' cancellation, then events, then the grace
//!   timer), the token branch
//!   applying what is queued first, and how actions are taken.
//!
//! Termination is checked as the absence of dead ends: a state where nothing
//! more can happen and the machine has not finished. A run cannot loop
//! instead: every step consumes a queued event or frame, or flips a one-way
//! flag (a fault, a worker's result, a cancellation, the peer's goodbye,
//! ack, `Cancel`, close and departure), and the grace timer is armed only by
//! an event, never by its own expiry.
//!
//! The grace period ends only once nothing is on its way to the coordinator,
//! the timing assumption of the model. Not covered: several peers,
//! corruption, inbound data lost or a data channel ending early, a frame
//! dropped outright (oversize, or not encodable: a loss at once, a local
//! bug), the peer closing the lane's connection cleanly in split mode (it
//! does only once it released its manager, after its goodbye or `Cancel`),
//! a peer that waits forever (the peer here always goes away in the end),
//! and the peer's own machine (two-node liveness is `live_ct_notice`'s).

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::rc::Rc;
use std::result::Result;

use super::*;

const PEER: &str = "b";

/// FNV-1a: a state's key is a hash, and SipHash is slow in debug builds.
struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

/// A FIFO of at most `N` items, so a state is copied without allocating.
/// Slots past `len` are always `None`, so equality compares the items.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Fifo<T: Copy, const N: usize> {
    items: [Option<T>; N],
    len: usize,
}

impl<T: Copy + Hash, const N: usize> Hash for Fifo<T, N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len);
        for item in self.iter() {
            item.hash(state);
        }
    }
}

impl<T: Copy, const N: usize> Fifo<T, N> {
    const EMPTY: Self = Self {
        items: [None; N],
        len: 0,
    };

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push_back(&mut self, item: T) {
        assert!(self.len < N, "more than {N} queued: raise the bound");
        self.items[self.len] = Some(item);
        self.len += 1;
    }

    fn pop_front(&mut self) -> Option<T> {
        let first = self.items[0]?;
        self.items.copy_within(1..self.len, 0);
        self.len -= 1;
        self.items[self.len] = None;
        Some(first)
    }

    fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.items[..self.len].iter().flatten().copied()
    }

    fn clear(&mut self) {
        *self = Self::EMPTY;
    }
}

/// At most two progress channels.
const CHANNELS: usize = 2;

/// The keys are hashes already.
#[derive(Default)]
struct Identity(u64);

impl Hasher for Identity {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys")
    }

    fn write_u64(&mut self, key: u64) {
        self.0 = key;
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Mode {
    Shared,
    Dedicated,
}

/// A cancellation: a requested stop, or a failure.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
enum Kind {
    Stop,
    Fail,
}

/// An event on the coordinator's channel.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Ev {
    Done(bool),
    SendFailed,
    InboundEnd { clean: bool },
    Cancel,
    ControlClosed,
    EndOfStream,
    Ack,
    ChannelClosed,
}

impl Ev {
    /// The peer's `Cancel`, goodbye or ack: it needs nothing more.
    fn excuses(self) -> bool {
        matches!(self, Ev::Cancel | Ev::EndOfStream | Ev::Ack)
    }

    fn into_event(self) -> LifecycleEvent {
        let link = |event| LifecycleEvent::Link {
            peer: PEER.to_string(),
            event,
        };
        match self {
            Ev::Done(success) => LifecycleEvent::WorkerFinished {
                success,
                failure: None,
            },
            Ev::SendFailed => link(LinkEvent::OutboundFailed(LinkFailure::SendFailed)),
            Ev::InboundEnd { clean: true } => link(LinkEvent::ConnectionFailed(
                TransportError::ConnectionClosed,
            )),
            Ev::InboundEnd { clean: false } => {
                link(LinkEvent::ConnectionFailed(TransportError::Io(
                    std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
                )))
            }
            Ev::Cancel => link(LinkEvent::CancelReceived),
            Ev::ControlClosed => link(LinkEvent::ControlClosed),
            Ev::EndOfStream => link(LinkEvent::ProgressEndOfStream),
            Ev::Ack => link(LinkEvent::AckReceived),
            Ev::ChannelClosed => link(LinkEvent::ProgressChannelEnded(LinkFailure::ChannelClosed)),
        }
    }
}

/// A frame we send the peer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Out {
    /// Our workers' records and progress, which the peer needs to finish.
    Records,
    Goodbye,
    Ack,
}

/// A frame the peer sends us.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum In {
    Cancel(Kind),
    /// Its end-of-stream on one progress channel.
    EndOfStream(usize),
    /// Its ack of our goodbye (on progress channel 0).
    Ack,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Forwarder {
    Idle,
    /// It holds a frame the transport will refuse.
    Refused,
    Exited,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    mode: Mode,
    workers: u8,
    channels: usize,
    /// Dedicated mode: a failed read counts as a failed send
    /// (`ClusterTransport::outbound_failed`).
    read_counts: bool,
    /// Shared mode: our lane is on one connection and the peer's frames
    /// arrive on another.
    split: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct World {
    // The coordinator.
    queue: Fifo<Ev, 16>,
    token: Option<Kind>,
    cancel_seen: bool,
    /// The bridges were cancelled from outside (`report_node_leave` of a
    /// participant).
    aborted: bool,
    timer: bool,
    /// Torn down: `Some(graceful)`.
    finished: Option<bool>,
    // Our workers.
    workers_left: u8,
    /// A worker failed (or was cancelled).
    workers_failed: bool,
    // Our side of the connection.
    io: Fifo<Ev, 4>,
    lane: Fifo<Out, 4>,
    /// The transport refuses our frames (Shared: the lane failed;
    /// Dedicated: the writer failed).
    writer_dead: bool,
    /// The next write fails (a reset, or the peer went away).
    write_fails: bool,
    /// Dedicated: the reader failed.
    read_failed: bool,
    forwarder: Forwarder,
    reader_up: bool,
    /// The read side's end was handed on: the channels from the peer close.
    channels_closed: bool,
    wire: Fifo<In, 4>,
    ctrl: Fifo<Kind, 2>,
    /// The dispatcher escalated the token for a `Cancel` it has not
    /// reported yet.
    dispatching: bool,
    prog: [Fifo<In, 4>; CHANNELS],
    end_of_stream: [bool; CHANNELS],
    ctrl_close_reported: bool,
    /// Channels past `Config::channels` count as reported.
    prog_close_reported: [bool; CHANNELS],
    // The connection.
    to_peer: bool,
    from_peer: bool,
    fault: bool,
    // The peer.
    records_delivered: bool,
    goodbye_delivered: bool,
    peer_goodbye: bool,
    peer_ack: bool,
    peer_cancel: Option<Kind>,
    peer_fin: bool,
    peer_gone: bool,
    // Ghosts, when our workers succeeded: the transport had failed our send;
    // our records had not reached the peer and our side had seen the
    // connection fail.
    failed_at_finish: bool,
    lost_at_finish: bool,
}

impl World {
    fn new(cfg: Config) -> Self {
        assert!(cfg.channels <= CHANNELS);
        Self {
            queue: Fifo::EMPTY,
            token: None,
            cancel_seen: false,
            aborted: false,
            timer: false,
            finished: None,
            workers_left: cfg.workers,
            workers_failed: false,
            io: Fifo::EMPTY,
            lane: {
                let mut lane = Fifo::EMPTY;
                lane.push_back(Out::Records);
                lane
            },
            writer_dead: false,
            write_fails: false,
            read_failed: false,
            forwarder: Forwarder::Idle,
            reader_up: true,
            channels_closed: false,
            wire: Fifo::EMPTY,
            ctrl: Fifo::EMPTY,
            dispatching: false,
            prog: [Fifo::EMPTY; CHANNELS],
            end_of_stream: [false; CHANNELS],
            ctrl_close_reported: false,
            prog_close_reported: std::array::from_fn(|ch| ch >= cfg.channels),
            to_peer: true,
            from_peer: true,
            fault: false,
            records_delivered: false,
            goodbye_delivered: false,
            peer_goodbye: false,
            peer_ack: false,
            peer_cancel: None,
            peer_fin: false,
            peer_gone: false,
            failed_at_finish: false,
            lost_at_finish: false,
        }
    }

    /// `ClusterTransport::outbound_failed`.
    fn outbound_failed(&self, cfg: Config) -> bool {
        self.writer_dead || (cfg.mode == Mode::Dedicated && cfg.read_counts && self.read_failed)
    }

    fn cancel(&mut self, kind: Kind) {
        self.token = Some(self.token.map_or(kind, |token| token.max(kind)));
    }

    /// The coordinator queues a frame for the peer.
    fn send(&mut self, frame: Out) {
        if self.writer_dead {
            if self.forwarder == Forwarder::Idle {
                self.forwarder = Forwarder::Refused;
            }
        } else {
            self.lane.push_back(frame);
        }
    }

    /// Our writer fails: what it held is lost. Shared mode then closes the
    /// whole connection (the reader is stopped, after the failed send was
    /// reported); Dedicated mode keeps reading.
    fn writer_fails(&mut self, cfg: Config) {
        self.writer_dead = true;
        // Frames still in the forwarder are refused when it hands them on.
        if !self.lane.is_empty() && self.forwarder == Forwarder::Idle {
            self.forwarder = Forwarder::Refused;
        }
        self.lane.clear();
        self.to_peer = false;
        self.io.push_back(Ev::SendFailed);
        if cfg.mode == Mode::Shared && !cfg.split {
            self.from_peer = false;
            if self.reader_up {
                self.reader_up = false;
                self.wire.clear();
                self.io.push_back(Ev::InboundEnd { clean: false });
            }
        }
    }

    /// Something is on its way to the coordinator that is not up to the
    /// peer, a worker or a fault: an event queued or about to be.
    fn busy(&self) -> bool {
        !self.queue.is_empty()
            || !self.io.is_empty()
            || !self.ctrl.is_empty()
            || self.dispatching
            || self.prog.iter().any(|queue| !queue.is_empty())
            || (self.channels_closed
                && (!self.ctrl_close_reported || self.prog_close_reported.contains(&false)))
            || self.forwarder == Forwarder::Refused
            || (self.reader_up && (!self.wire.is_empty() || self.peer_fin))
            || (self.write_fails && !self.writer_dead && !self.lane.is_empty())
    }

    /// The peer's `Cancel`, goodbye or ack is on its way to the coordinator.
    fn excuse_on_its_way(&self) -> bool {
        self.queue.iter().any(|ev| ev.excuses())
            || !self.ctrl.is_empty()
            || self.dispatching
            || self.prog.iter().any(|queue| !queue.is_empty())
            || (self.reader_up && !self.wire.is_empty())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    // The coordinator.
    TokenSeen,
    AbortSeen,
    Apply,
    GraceEnds,
    // Our side.
    Finish(bool),
    LocalCancel(Kind),
    ExternalAbort,
    // Our transport.
    Deliver,
    WriteFails,
    Read,
    ReaderEof,
    ForwardNotice,
    Dispatch,
    DispatchReport,
    DispatchClosed,
    Bridge(usize),
    BridgeClosed(usize),
    ForwarderRefused,
    // The peer.
    PeerGoodbye,
    PeerAck,
    PeerCancel(Kind),
    PeerFin,
    PeerGone,
    // Faults.
    WriteErr,
    Reset {
        write_first: bool,
    },
    /// Split: the connection the peer's frames arrive on is reset; our lane
    /// is not.
    ResetInbound,
}

fn steps(w: &World, cfg: Config) -> Vec<Step> {
    if w.finished.is_some() {
        // Torn down; workers torn down from outside may still finish (their
        // results no longer reach the coordinator).
        return if w.workers_left > 0 {
            vec![Step::Finish(true), Step::Finish(false)]
        } else {
            Vec::new()
        };
    }
    let mut steps = Vec::new();
    // The coordinator's biased select.
    if w.token.is_some() && !w.cancel_seen {
        steps.push(Step::TokenSeen);
    } else if w.aborted {
        steps.push(Step::AbortSeen);
    } else if !w.queue.is_empty() {
        steps.push(Step::Apply);
    } else if w.timer && !w.busy() {
        steps.push(Step::GraceEnds);
    }
    // A worker may have succeeded before it saw a cancellation, and may
    // fail on its own.
    if w.workers_left > 0 {
        steps.push(Step::Finish(true));
        steps.push(Step::Finish(false));
    }
    if w.token.is_none() {
        steps.push(Step::LocalCancel(Kind::Stop));
    }
    if !w.aborted {
        steps.push(Step::ExternalAbort);
    }
    if w.token != Some(Kind::Fail) {
        steps.push(Step::LocalCancel(Kind::Fail));
    }
    if w.to_peer && !w.writer_dead && !w.write_fails && !w.lane.is_empty() {
        steps.push(Step::Deliver);
    }
    if w.write_fails && !w.writer_dead && !w.lane.is_empty() {
        steps.push(Step::WriteFails);
    }
    if w.reader_up && !w.wire.is_empty() {
        steps.push(Step::Read);
    }
    if w.reader_up && w.wire.is_empty() && w.peer_fin {
        steps.push(Step::ReaderEof);
    }
    if !w.io.is_empty() {
        steps.push(Step::ForwardNotice);
    }
    if w.dispatching {
        steps.push(Step::DispatchReport);
    } else if !w.ctrl.is_empty() {
        steps.push(Step::Dispatch);
    } else if w.channels_closed && !w.ctrl_close_reported {
        steps.push(Step::DispatchClosed);
    }
    for ch in 0..cfg.channels {
        if !w.prog[ch].is_empty() {
            steps.push(Step::Bridge(ch));
        } else if w.channels_closed && !w.prog_close_reported[ch] {
            steps.push(Step::BridgeClosed(ch));
        }
    }
    if w.forwarder == Forwarder::Refused {
        steps.push(Step::ForwarderRefused);
    }
    if w.from_peer && !w.peer_fin {
        if w.records_delivered && !w.peer_goodbye && w.peer_cancel != Some(Kind::Fail) {
            steps.push(Step::PeerGoodbye);
        }
        if w.goodbye_delivered && !w.peer_ack {
            steps.push(Step::PeerAck);
        }
        if w.peer_cancel.is_none() {
            steps.push(Step::PeerCancel(Kind::Fail));
            if !w.peer_goodbye {
                steps.push(Step::PeerCancel(Kind::Stop));
            }
        }
        steps.push(Step::PeerFin);
    }
    if w.peer_fin && !w.peer_gone {
        steps.push(Step::PeerGone);
    }
    if !w.fault {
        if w.to_peer && !w.writer_dead && !w.lane.is_empty() {
            steps.push(Step::WriteErr);
        }
        if cfg.split {
            // Each connection apart: the lane's, then the inbound one.
            if w.to_peer && !w.writer_dead {
                steps.push(Step::Reset { write_first: true });
            }
            if w.from_peer {
                steps.push(Step::ResetInbound);
            }
        } else if w.to_peer || w.from_peer {
            steps.push(Step::Reset { write_first: true });
            if cfg.mode == Mode::Shared && w.reader_up && !w.writer_dead {
                steps.push(Step::Reset { write_first: false });
            }
        }
    }
    steps
}

/// What the checks counted, so a pass is known not to be vacuous.
#[derive(Default, Debug)]
struct Stats {
    states: usize,
    terminals: usize,
    ok: usize,
    /// `Ok` although the send had failed when the workers succeeded (the
    /// peer retired or cancelled): `NoOkAfterFailedSend`'s witness.
    ok_after_failed_send: usize,
    settled_excused: usize,
    settled_failed: usize,
    grace_ended: usize,
    data_loss: usize,
}

fn peer(m: &LifecycleMachine) -> &PeerState {
    &m.peers[PEER]
}

/// A loss was recorded or held (data, pending or deferred) that was not
/// before (`before`: which of them were).
fn loss_recorded(m: &LifecycleMachine, before: (bool, bool, bool)) -> bool {
    (!before.0 && m.data_loss.is_some())
        || (!before.1 && m.pending_loss.is_some())
        || (!before.2 && m.deferred_loss.is_some())
}

/// Apply one event as `ClusterLifecycle::apply` does, checking
/// `NoFalseSettle` and recording the ghosts of the `Ok` rules.
fn apply_event(
    w: &mut World,
    m: &mut LifecycleMachine,
    ev: Ev,
    cfg: Config,
    stats: &mut Stats,
) -> Result<Vec<Action>, String> {
    let owed = peer(m).unsent();
    // A send reported by this event, owed and settled at once (the machine
    // marks every settlement), is checked too.
    let settled_before = peer(m).send_failure_settled;
    let lost = (
        m.data_loss.is_some(),
        m.pending_loss.is_some(),
        m.deferred_loss.is_some(),
    );
    let goodbye_sent = m.goodbye_sent;
    let mut actions = Vec::new();
    if let Ev::Done(_) = ev {
        let failed = w.outbound_failed(cfg);
        actions.extend(m.note_failed_sends(|_| failed));
    }
    actions.extend(m.on_event(ev.into_event()));
    if !goodbye_sent && m.goodbye_sent {
        w.failed_at_finish = w.outbound_failed(cfg);
        w.lost_at_finish = !w.records_delivered && (w.writer_dead || w.read_failed);
    }
    let settled = (owed && !peer(m).unsent()) || (!settled_before && peer(m).send_failure_settled);
    if settled {
        let failed = loss_recorded(m, lost)
            || actions
                .iter()
                .any(|a| matches!(a, Action::LinkFailed { .. }));
        if failed {
            stats.settled_failed += 1;
            if w.excuse_on_its_way() {
                return Err(format!(
                    "NoFalseSettle: {ev:?} settled the failed send as a failure while the \
                     peer's Cancel, goodbye or ack was on its way"
                ));
            }
        } else if owed {
            stats.settled_excused += 1;
        }
    }
    Ok(actions)
}

/// Take the machine's actions as `ClusterLifecycle::perform` does, up to
/// teardown.
fn perform(w: &mut World, actions: Vec<Action>) {
    for action in actions {
        match action {
            Action::SendAck(_) => w.send(Out::Ack),
            Action::SendGoodbye => w.send(Out::Goodbye),
            Action::BroadcastCancel | Action::AnnounceUnfinished => {}
            Action::LinkFailed { .. } | Action::CancelDataflow(_) | Action::AbortDrain(_) => {
                w.cancel(Kind::Fail);
            }
            Action::ArmSoftFailureTimer(_) => w.timer = true,
            Action::Teardown { graceful } => {
                w.finished = Some(graceful);
                return;
            }
        }
    }
}

/// A step of the coordinator: the only steps that change the machine.
fn take_coordinator(
    w: &mut World,
    m: &mut LifecycleMachine,
    step: Step,
    cfg: Config,
    stats: &mut Stats,
) -> Result<(), String> {
    match step {
        Step::TokenSeen => {
            // Apply what is already queued, then the cancellation.
            w.cancel_seen = true;
            let mut actions = Vec::new();
            while let Some(ev) = w.queue.pop_front() {
                actions.extend(apply_event(w, m, ev, cfg, stats)?);
            }
            actions.extend(m.on_dataflow_cancelled(w.token == Some(Kind::Fail)));
            perform(w, actions);
        }
        Step::AbortSeen => {
            // Apply what is already queued, then the abort. Not checked
            // against NoFalseSettle: a teardown from outside settles an owed
            // send at once, though the peer's Cancel may be on its way.
            w.aborted = false;
            let mut actions = Vec::new();
            while let Some(ev) = w.queue.pop_front() {
                actions.extend(apply_event(w, m, ev, cfg, stats)?);
            }
            let failed = w.outbound_failed(cfg);
            actions.extend(m.on_external_abort(|_| failed));
            perform(w, actions);
        }
        Step::Apply => {
            let ev = w.queue.pop_front().expect("queued");
            let actions = apply_event(w, m, ev, cfg, stats)?;
            perform(w, actions);
        }
        Step::GraceEnds => {
            w.timer = false;
            stats.grace_ended += 1;
            let p = peer(m);
            let finished_peer = p.unsent() && p.retired() && p.fully_closed();
            let lost = (
                m.data_loss.is_some(),
                m.pending_loss.is_some(),
                m.deferred_loss.is_some(),
            );
            let actions = m.on_soft_failure_expired(PEER.to_string());
            let failed = loss_recorded(m, lost)
                || actions
                    .iter()
                    .any(|a| matches!(a, Action::LinkFailed { .. }));
            // Never fires with the code as it is: every event that closes
            // the link fully settles an owed send at once. It guards that
            // (without the goodbye's settle, it fires).
            if finished_peer && failed {
                return Err(
                    "NoFalseSettle: the grace period failed the send although the \
                            peer said goodbye and its link closed fully"
                        .into(),
                );
            }
            perform(w, actions);
        }
        _ => unreachable!("not the coordinator's: {step:?}"),
    }
    Ok(())
}

/// The run's result once nothing more can happen, as the completion makes it
/// (`merge_data_loss`): the workers' result, failed by a data loss, or by a
/// loss held for workers that finished after teardown if they all succeeded.
fn judge(w: &World, m: &LifecycleMachine, cfg: Config, stats: &mut Stats) -> Result<(), String> {
    let workers_ok = !w.workers_failed;
    let loss = m
        .data_loss()
        .or(m.loss_if_workers_succeed().filter(|_| workers_ok));
    if let Some(loss) = loss {
        stats.data_loss += 1;
        if !w.outbound_failed(cfg) && !w.writer_dead {
            return Err(format!(
                "NoLossWithoutFailedSend: a data loss ({loss:?}) although no send to the \
                 peer failed"
            ));
        }
    }
    if workers_ok && loss.is_none() {
        stats.ok += 1;
        let p = peer(m);
        let excused = p.retired() || p.cancel_received;
        if w.failed_at_finish && !excused {
            return Err(
                "NoOkAfterFailedSend: Ok although the send had failed when the \
                        workers succeeded, and the peer neither retired the link nor \
                        cancelled"
                    .into(),
            );
        }
        if w.lost_at_finish && !p.cancel_received {
            return Err(
                "NoOkOverKnownLoss: Ok although our records never reached the \
                        peer, the connection had failed when the workers succeeded, and \
                        the peer did not cancel"
                    .into(),
            );
        }
        if w.failed_at_finish {
            stats.ok_after_failed_send += 1;
        }
    }
    Ok(())
}

/// Any other step: the machine is only read.
fn take_other(w: &mut World, m: &LifecycleMachine, step: Step, cfg: Config) {
    match step {
        Step::TokenSeen | Step::AbortSeen | Step::Apply | Step::GraceEnds => {
            unreachable!("the coordinator's: {step:?}")
        }
        Step::Finish(success) => {
            w.workers_left -= 1;
            w.workers_failed |= !success;
            if w.finished.is_none() {
                w.queue.push_back(Ev::Done(success));
            } else if w.workers_left == 0 && !w.workers_failed {
                // The last worker succeeded after teardown.
                w.failed_at_finish = w.outbound_failed(cfg);
                w.lost_at_finish = !w.records_delivered && (w.writer_dead || w.read_failed);
            }
        }
        Step::LocalCancel(kind) => w.cancel(kind),
        Step::ExternalAbort => w.aborted = true,
        Step::Deliver => match w.lane.pop_front().expect("queued") {
            Out::Records => w.records_delivered = true,
            Out::Goodbye => w.goodbye_delivered = true,
            Out::Ack => {}
        },
        Step::WriteFails => w.writer_fails(cfg),
        Step::Read => match w.wire.pop_front().expect("sent") {
            In::Cancel(kind) => w.ctrl.push_back(kind),
            frame @ In::EndOfStream(ch) => w.prog[ch].push_back(frame),
            In::Ack => w.prog[0].push_back(In::Ack),
        },
        Step::ReaderEof => {
            w.reader_up = false;
            w.io.push_back(Ev::InboundEnd { clean: true });
        }
        Step::ForwardNotice => {
            let ev = w.io.pop_front().expect("queued");
            w.queue.push_back(ev);
            if let Ev::InboundEnd { .. } = ev {
                w.channels_closed = true;
            }
        }
        Step::Dispatch => {
            let kind = w.ctrl.pop_front().expect("read");
            w.cancel(kind);
            w.dispatching = true;
        }
        Step::DispatchReport => {
            w.dispatching = false;
            w.queue.push_back(Ev::Cancel);
        }
        Step::DispatchClosed => {
            w.ctrl_close_reported = true;
            w.queue.push_back(Ev::ControlClosed);
        }
        Step::Bridge(ch) => match w.prog[ch].pop_front().expect("read") {
            In::EndOfStream(_) => {
                w.end_of_stream[ch] = true;
                w.queue.push_back(Ev::EndOfStream);
            }
            In::Ack => w.queue.push_back(Ev::Ack),
            In::Cancel(_) => unreachable!("on the control channel"),
        },
        Step::BridgeClosed(ch) => {
            w.prog_close_reported[ch] = true;
            if !w.end_of_stream[ch] {
                w.queue.push_back(Ev::ChannelClosed);
            }
        }
        Step::ForwarderRefused => {
            w.forwarder = Forwarder::Exited;
            if !peer(m).link.is_retired() {
                w.queue.push_back(Ev::SendFailed);
            }
        }
        Step::PeerGoodbye => {
            w.peer_goodbye = true;
            for ch in 0..cfg.channels {
                w.wire.push_back(In::EndOfStream(ch));
            }
        }
        Step::PeerAck => {
            w.peer_ack = true;
            w.wire.push_back(In::Ack);
        }
        Step::PeerCancel(kind) => {
            w.peer_cancel = Some(kind);
            w.wire.push_back(In::Cancel(kind));
        }
        Step::PeerFin => w.peer_fin = true,
        Step::PeerGone => {
            w.peer_gone = true;
            w.to_peer = false;
            w.write_fails = true;
        }
        Step::WriteErr => {
            w.fault = true;
            w.writer_fails(cfg);
        }
        // The lane's connection: its reader (other traffic) fails, which
        // fails the lane at once (`fail_inbound`), queued frames and all.
        // The peer's frames arrive on the other connection.
        Step::Reset { .. } if cfg.split => {
            w.fault = true;
            w.writer_fails(cfg);
        }
        // The peer's frames stop; its end is reported, our lane goes on.
        Step::ResetInbound => {
            w.fault = true;
            w.wire.clear();
            w.from_peer = false;
            if w.reader_up {
                w.reader_up = false;
                w.io.push_back(Ev::InboundEnd { clean: false });
            }
        }
        Step::Reset { write_first } => {
            w.fault = true;
            w.wire.clear();
            w.to_peer = false;
            w.from_peer = false;
            match cfg.mode {
                // A failed read fails the lane at once (`fail_inbound`), and
                // reports both, in either order. A reader that already ended
                // leaves it to the next write.
                Mode::Shared if w.reader_up => {
                    w.reader_up = false;
                    let end = Ev::InboundEnd { clean: false };
                    if w.writer_dead {
                        w.io.push_back(end);
                    } else {
                        w.writer_dead = true;
                        if !w.lane.is_empty() && w.forwarder == Forwarder::Idle {
                            w.forwarder = Forwarder::Refused;
                        }
                        w.lane.clear();
                        let reports = if write_first {
                            [Ev::SendFailed, end]
                        } else {
                            [end, Ev::SendFailed]
                        };
                        for report in reports {
                            w.io.push_back(report);
                        }
                    }
                }
                Mode::Shared => w.write_fails = true,
                // The reader fails; the writer fails at its next write.
                Mode::Dedicated => {
                    if w.reader_up {
                        w.reader_up = false;
                        w.read_failed = true;
                        w.io.push_back(Ev::InboundEnd { clean: false });
                    }
                    w.write_fails = true;
                }
            }
        }
    }
}

fn dup_failure(failure: &LinkFailure) -> LinkFailure {
    match failure {
        LinkFailure::ChannelClosed => LinkFailure::ChannelClosed,
        LinkFailure::Decode(detail) => LinkFailure::Decode(detail.clone()),
        LinkFailure::ProtocolViolation(detail) => LinkFailure::ProtocolViolation(detail.clone()),
        LinkFailure::LockPoisoned => LinkFailure::LockPoisoned,
        LinkFailure::SendFailed => LinkFailure::SendFailed,
        LinkFailure::Encode(detail) => LinkFailure::Encode(detail.clone()),
        LinkFailure::Transport(error) => LinkFailure::Transport(error.duplicate()),
    }
}

/// A copy of the machine with its own link state. Every field is listed, so
/// a new one does not go uncopied.
fn fork(m: &LifecycleMachine) -> LifecycleMachine {
    LifecycleMachine {
        phase: m.phase,
        workers_remaining: m.workers_remaining,
        all_workers_ok: m.all_workers_ok,
        goodbye_sent: m.goodbye_sent,
        cancel_broadcast: m.cancel_broadcast,
        requested_stop: m.requested_stop,
        drain_aborted: m.drain_aborted,
        data_loss: m.data_loss.clone(),
        pending_loss: m.pending_loss.clone(),
        deferred_loss: m.deferred_loss.clone(),
        workers_succeeded: m.workers_succeeded,
        peers: m
            .peers
            .iter()
            .map(|(id, p)| {
                let link = Arc::new(PeerLinkState::default());
                if p.link.is_retired() {
                    link.retire();
                }
                if p.link.push_blocked() {
                    link.note_push_blocked();
                }
                let p = PeerState {
                    expected_end_of_stream: p.expected_end_of_stream,
                    end_of_stream: p.end_of_stream,
                    progress_channels_ended: p.progress_channels_ended,
                    goodbye_received: p.goodbye_received,
                    ack_received: p.ack_received,
                    control_closed: p.control_closed,
                    lost: p.lost,
                    soft_failure: p.soft_failure.as_ref().map(dup_failure),
                    cause: p.cause.as_ref().map(TransportError::duplicate),
                    unclean_data_channel: p.unclean_data_channel,
                    cancel_received: p.cancel_received,
                    stopped: p.stopped,
                    send_failure_noted: p.send_failure_noted,
                    send_failure_settled: p.send_failure_settled,
                    link,
                };
                (id.clone(), p)
            })
            .collect(),
    }
}

fn hash_failure(failure: &Option<LinkFailure>, hasher: &mut Fnv) {
    // Only the kind decides anything.
    failure.as_ref().map(std::mem::discriminant).hash(hasher);
}

/// The machine's part of a state's identity: every field. The
/// destructuring lists them all, so a new one is not left out.
fn machine_key(m: &LifecycleMachine) -> u64 {
    let mut hasher = Fnv::default();
    let LifecycleMachine {
        phase,
        workers_remaining,
        all_workers_ok,
        goodbye_sent,
        cancel_broadcast,
        requested_stop,
        drain_aborted,
        data_loss,
        pending_loss,
        deferred_loss,
        workers_succeeded,
        peers,
    } = m;
    (
        *phase as u8,
        workers_remaining,
        all_workers_ok,
        goodbye_sent,
        cancel_broadcast,
        requested_stop,
        drain_aborted,
        workers_succeeded,
    )
        .hash(&mut hasher);
    for loss in [data_loss, pending_loss, deferred_loss] {
        loss.as_ref()
            .map(|loss| (&loss.peer, &loss.detail))
            .hash(&mut hasher);
    }
    for (id, p) in peers {
        let PeerState {
            expected_end_of_stream,
            end_of_stream,
            progress_channels_ended,
            goodbye_received,
            ack_received,
            control_closed,
            lost,
            soft_failure,
            cause,
            unclean_data_channel,
            cancel_received,
            stopped,
            send_failure_noted,
            send_failure_settled,
            link,
        } = p;
        (
            id,
            expected_end_of_stream,
            end_of_stream,
            progress_channels_ended,
            goodbye_received,
            ack_received,
            control_closed,
        )
            .hash(&mut hasher);
        (
            lost,
            unclean_data_channel,
            cancel_received,
            stopped,
            send_failure_noted,
            send_failure_settled,
            link.is_retired(),
            link.push_blocked(),
        )
            .hash(&mut hasher);
        hash_failure(soft_failure, &mut hasher);
        cause.as_ref().map(std::mem::discriminant).hash(&mut hasher);
    }
    hasher.finish()
}

/// Every state reachable from the start, depth first. The first violation
/// is returned with the steps that led to it.
fn explore(cfg: Config) -> Result<Stats, String> {
    let machine = LifecycleMachine::new(
        usize::from(cfg.workers),
        [(
            PEER.to_string(),
            cfg.channels,
            Arc::new(PeerLinkState::default()),
        )],
    );
    let w = World::new(cfg);
    let key = |w: &World, machine_key: u64| {
        let mut hasher = Fnv::default();
        w.hash(&mut hasher);
        machine_key.hash(&mut hasher);
        hasher.finish()
    };
    let start = machine_key(&machine);
    let mut stats = Stats::default();
    let mut seen: HashSet<u64, BuildHasherDefault<Identity>> = HashSet::default();
    seen.insert(key(&w, start));
    // The steps taken, each with the index of the one before it.
    let mut trail: Vec<(usize, Step)> = Vec::new();
    let path = |trail: &[(usize, Step)], mut at: usize| {
        let mut steps = Vec::new();
        while at != usize::MAX {
            steps.push(trail[at].1);
            at = trail[at].0;
        }
        steps.reverse();
        steps
    };
    // Steps other than the coordinator's share the machine.
    let mut stack = vec![(w, Rc::new(machine), start, usize::MAX)];
    while let Some((w, m, m_key, at)) = stack.pop() {
        stats.states += 1;
        let next = steps(&w, cfg);
        if next.is_empty() {
            stats.terminals += 1;
            if w.finished.is_none() {
                return Err(format!(
                    "{cfg:?}: the machine never finished, though nothing more can happen\n\
                     steps: {:?}\nworld: {w:?}\nmachine: {m:?}",
                    path(&trail, at)
                ));
            }
            if let Err(violation) = judge(&w, &m, cfg, &mut stats) {
                return Err(format!(
                    "{cfg:?}: {violation}\nsteps: {:?}\nworld: {w:?}\nmachine: {m:?}",
                    path(&trail, at)
                ));
            }
            continue;
        }
        for step in next {
            let mut w2 = w;
            let (m2, m2_key) = if matches!(
                step,
                Step::TokenSeen | Step::AbortSeen | Step::Apply | Step::GraceEnds
            ) {
                let mut m2 = fork(&m);
                if let Err(violation) = take_coordinator(&mut w2, &mut m2, step, cfg, &mut stats) {
                    let mut steps = path(&trail, at);
                    steps.push(step);
                    return Err(format!(
                        "{cfg:?}: {violation}\nsteps: {steps:?}\nworld: {w2:?}\nmachine: {m2:?}"
                    ));
                }
                let m2_key = machine_key(&m2);
                (Rc::new(m2), m2_key)
            } else {
                take_other(&mut w2, &m, step, cfg);
                (Rc::clone(&m), m_key)
            };
            if seen.insert(key(&w2, m2_key)) {
                trail.push((at, step));
                stack.push((w2, m2, m2_key, trail.len() - 1));
            }
        }
    }
    Ok(stats)
}

/// Explore one configuration with the fixes in place: the rules hold, and
/// each is exercised (an `Ok` the peer excused despite a failed send, a
/// failed send settled as a failure and one excused, the grace period
/// ending, a data loss reported).
fn check(mode: Mode, split: bool, workers: u8, channels: usize) {
    let cfg = Config {
        mode,
        workers,
        channels,
        read_counts: true,
        split,
    };
    let stats = explore(cfg).unwrap_or_else(|violation| panic!("{violation}"));
    println!("{cfg:?}: {stats:?}");
    assert!(stats.ok > 0, "no run ended Ok");
    assert!(
        stats.ok_after_failed_send > 0,
        "NoOkAfterFailedSend is vacuous"
    );
    assert!(
        stats.settled_failed > 0,
        "no failed send settled as a failure"
    );
    assert!(stats.settled_excused > 0, "no failed send excused");
    assert!(stats.grace_ended > 0, "the grace period never ended");
    assert!(stats.data_loss > 0, "NoLossWithoutFailedSend is vacuous");
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_one_worker_one_channel() {
    check(Mode::Shared, false, 1, 1);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_one_worker_two_channels() {
    check(Mode::Shared, false, 1, 2);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_two_workers_one_channel() {
    check(Mode::Shared, false, 2, 1);
}

#[test]
#[ignore = "over 100 million states and 8 GB: run by hand, in release"]
fn shared_two_workers_two_channels() {
    check(Mode::Shared, false, 2, 2);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_split_one_worker_one_channel() {
    check(Mode::Shared, true, 1, 1);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_split_one_worker_two_channels() {
    check(Mode::Shared, true, 1, 2);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn shared_split_two_workers_one_channel() {
    check(Mode::Shared, true, 2, 1);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn dedicated_one_worker_one_channel() {
    check(Mode::Dedicated, false, 1, 1);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn dedicated_one_worker_two_channels() {
    check(Mode::Dedicated, false, 1, 2);
}

#[test]
#[ignore = "a minute or more in a debug build: CI runs it in release (conformance job)"]
fn dedicated_two_workers_one_channel() {
    check(Mode::Dedicated, false, 2, 1);
}

#[test]
#[ignore = "over 100 million states and 8 GB: run by hand, in release"]
fn dedicated_two_workers_two_channels() {
    check(Mode::Dedicated, false, 2, 2);
}

/// Dedicated mode with a failed read not counted as a failed send (before
/// the fix): a reset the reader sees first, while the idle writer has not
/// failed, ends Ok over the records it lost.
#[test]
fn a_dedicated_read_failure_must_count_as_a_failed_send() {
    let cfg = Config {
        mode: Mode::Dedicated,
        workers: 1,
        channels: 1,
        read_counts: false,
        split: false,
    };
    let violation = explore(cfg).expect_err("a violation without the fix");
    assert!(violation.contains("NoOkOverKnownLoss"), "{violation}");
}
