//! The lifecycle machine checked exhaustively: the real [`LifecycleMachine`]
//! driven through every order in which its events can reach it, for one
//! peer, within small bounds.
//!
//! Only the machine's environment is written here, never one of its
//! decisions: the producers that feed the coordinator's event channel and how
//! their sends interleave, today's driver glue around the machine (the
//! [`driver`] module), the transport in each mode, the peer and our workers.
//! Every state reachable from the start is visited once, breadth first. A
//! state is deduplicated on an exact copy of it (every field of the machine
//! included, see [`Key`]), so nothing is pruned by a hash collision, and
//! whatever is found comes with a shortest trace that leads to it.
//!
//! # Properties
//!
//! 1. **Termination**: once the peer has gone, the machine always finishes.
//! 2. **NoLossWithoutFailedSend**: a run reports a data loss only if a send
//!    to the peer failed (the transport failed our writer or lane).
//! 3. **NoFalseSettle**: a failed send is never settled as a failure (a link
//!    failure, or a loss recorded or held) while the peer's `Cancel`, goodbye
//!    or ack is still on its way to the coordinator, nor by the grace period
//!    once the peer has said goodbye and the link has closed fully.
//! 4. **NoOkAfterFailedSend**: a run whose send had already failed when its
//!    workers succeeded ends `Ok` only if the peer retired the link or
//!    cancelled.
//! 5. **NoOkOverKnownLoss**: a run whose records never reached the peer ends
//!    `Ok` only if the peer cancelled, provided our side had already seen the
//!    connection that carries them fail (a write error, or a failed read)
//!    when the workers succeeded.
//!
//! Termination is checked as the absence of dead ends: states where the
//! machine has not finished and nothing more is bound to happen
//! ([`Step::bound_to_happen`]). Faults, cancellations, the abort from outside,
//! and the peer's goodbye, ack and `Cancel` may never happen; everything else
//! happens once it can, and the peer always closes its side and goes away in
//! the end. No run loops: every step consumes something queued, moves it one
//! stage further (the wire, a channel, the dispatcher, the coordinator's
//! channel), or flips a one-way flag, and whatever is queued was produced by
//! a one-way change (the peer's frames, a fault, a worker's result, our
//! frames and the grace timer by the machine's one-way decisions, never by
//! the timer's own expiry). A step that changes nothing fails the check.
//!
//! A run ends once the machine has torn down and every worker has finished.
//! Its result is what the completion makes of it (`merge_data_loss`): `Ok` if
//! every worker succeeded and the machine recorded no data loss. For (2) a
//! recorded loss counts whatever the workers' result. For (4) and (5):
//! - The workers succeeded when the last of them reported its success, not
//!   when the machine applied it: a send that fails in between does not
//!   count, so in every counterexample the failure came first, and the
//!   workers outlived it.
//! - What excuses an `Ok` is what the peer did, whether or not the machine
//!   learned of it: it retired the link (it said goodbye, or acked ours: it
//!   had our records), or it cancelled.
//! - For (5) the loss is known when the workers succeed: our records had not
//!   reached the peer, and our side had seen the connection that carries them
//!   fail (our writer or lane failed, or our reader did, on a connection that
//!   carries both ways), so they never will. In a split wiring, a failure of
//!   the connection the peer's frames arrive on says nothing about our
//!   records.
//! - After teardown only our workers move: a run is judged as one in which
//!   nothing more happens on the network, as when the peer goes away at once
//!   (which it may). What the peer did by then is all it did.
//!
//! (1) and (2) hold. (3) to (5) do not: [`KNOWN_FAILURES`] says why, one line
//! each, and `exhaustive.golden` pins every way each configuration breaks
//! them (see [`GOLDEN`]): how many violations, how many distinct identities
//! (the environment's side of a violating state, [`World::identity`]), a
//! digest of that set, and a shortest trace. Each test prints a shortest
//! counterexample for every property it finds broken, and fails if what it
//! finds and the list or the pins disagree, so a change to any counterexample
//! has to update them. The identity leaves out the coordinator's channel and
//! the driver's state, so it is coarser than a state: a pin can miss a new
//! counterexample whose identity and count match an old one's.
//!
//! The configurations with two local workers and one progress channel are
//! abstract: the runtime gives each local worker a progress channel from each
//! peer worker (`(peer workers) * (local workers)` end-of-stream markers), so
//! two local workers always get at least two. They exercise two worker
//! results against a one-marker goodbye, at a fraction of the cost of the
//! real two-worker configurations.
//!
//! # The environment
//!
//! - The coordinator's event channel: one FIFO, fed by producers whose sends
//!   interleave freely, each in its own order:
//!   - the io forwarder: the transport's notices (`PeerIoNotice`), in the
//!     order the transport queued them. The end of a read side is handed on
//!     before the peer's channels close;
//!   - the control dispatcher: for each `Cancel`, the token escalated, then
//!     the event, as two steps; then the channel's close;
//!   - one progress bridge per channel: an end-of-stream or an ack, then the
//!     channel's close, reported only when no end-of-stream came;
//!   - the outbox forwarder: a frame the transport refused, unless the link
//!     has retired;
//!   - our workers' results.
//! - Today's driver glue (`ClusterLifecycle::run`), in [`driver`]: its biased
//!   `select!` (the dataflow token, the bridges' cancellation, the next
//!   event, the grace timer), the token branch applying what is queued
//!   first, and how actions are taken. The token keeps its first reason,
//!   which says whether the cancellation is a failure; a failure escalated
//!   after a requested stop decides what our `Cancel` says, and so whether
//!   `AnnounceUnfinished` sends one.
//! - The transport:
//!   - Control frames go first: each transport's `bridge_task` polls its
//!     control queue, then its lane (a biased `select!`). So a `Cancel`
//!     (`broadcast_cancel_with_transport`) can go out ahead of the frames
//!     queued on the lane before it that the transport had not taken yet.
//!     A lane frame queued after it goes out ahead of it only if the
//!     transport, having found no control frame, was about to take the
//!     lane's next frame when the `Cancel` was queued: that frame then
//!     goes first, and the `Cancel` right after it. That holds for our
//!     `Cancel`s and for the peer's, which can reach us ahead of its
//!     goodbye or ack. Writing a `Cancel` can fail like writing any other
//!     frame.
//!   - A failed write loses the frame it was writing, and everything behind
//!     it. A write that fails although its frame went out whole is left out:
//!     `write_frame` flushes after the frame's last byte, and a TCP stream's
//!     flush does nothing (see "Not covered").
//!   - Shared: a write error or a failed read fails the lane at once and for
//!     good. The manager then drops our frames (`send_payload`), so the outbox
//!     forwarder never sees one refused. A write error is reported, then
//!     stops the reader, closing the whole connection; a failed read reports
//!     both, in either order. [`Wiring::SharedSplit`] puts our lane and the
//!     peer's frames on two connections: a failure of one leaves the other,
//!     and the peer closing both also tells our lane.
//!   - Dedicated: the reader and the writer fail independently. A failed
//!     writer reports, then drops its queues (`bridge_task`), as two steps:
//!     the frame it was writing is lost, one behind it is lost unseen or
//!     refused to the forwarder, and every later one is refused. After a
//!     reset the reader fails, and the writer fails at its next write.
//! - The peer: says goodbye (only once it has our records, which it needs,
//!   and not after cancelling for a failure), acks our goodbye, cancels (a
//!   requested stop, or a failure; no requested stop after its goodbye),
//!   closes its side and goes away (while any connection links us), after
//!   which our writes fail.
//! - Our side: workers that succeed or fail at any time, also after a
//!   cancellation or teardown (what they need from the peer travels on
//!   channels the coordinator does not watch, and may have arrived at any
//!   point before); a local cancellation (a requested stop or a failure);
//!   the bridges cancelled from outside, which happens only with the token
//!   cancelled first (`ClusterCancelHandle::cancel_with_reason`).
//! - At most one fault: a local write error, or a reset.
//! - The grace period ends only once nothing is on its way to the
//!   coordinator: the model's timing assumption, stronger than what today's
//!   driver guarantees (see the timer race below).
//!
//! # Not covered
//!
//! Several peers; corruption, records that reach a finished operator, a data
//! channel that ends without its close sentinel, a frame the transport
//! rejects (oversize) and an encoding failure: no event here reports them.
//! A write reported failed although its frame went out whole, which only a
//! stream whose flush can fail after sending everything does (not TCP): the
//! peer would have the frame, and our side a `WriteFailed`. A `Cancel` the
//! peer sends after unregistering the dataflow in split Shared mode:
//! `send_control` then takes any pooled connection, where it can arrive
//! apart from the peer's other frames, or be dropped after their connection
//! has ended; here the peer's `Cancel` always travels with its lane. Data
//! pushes (`push_blocked` stays false), so a second `Cancel` from the peer
//! would change nothing. The peer's own machine. A peer that never goes away
//! (a shared manager whose connections stay up). What teardown does with
//! events queued behind the one that finished the machine: it applies only
//! lost records and corruption, which do not occur here.
//!
//! Two interleavings of today's driver are not covered either:
//! - The timer race: `select!` can find no event (`events.recv()` pending),
//!   an event can be queued next, and the expired grace deadline is then taken
//!   with that event already queued. Here the grace period ends only once
//!   nothing is on its way.
//! - The environment moving while the driver takes a step's actions: it
//!   awaits some of them (a `Cancel` broadcast, teardown's flush), and in the
//!   meantime other tasks post events, escalate the token and fail the
//!   transport. Here a step's actions are taken at once. The machine does not
//!   run in between, and the token's first reason is settled before a
//!   broadcast is awaited. A teardown fixes the run's result, and what the
//!   network does after it is left out on purpose (see above).
//!
//! Both belong to today's driver: a check of another driver has to cover them.
//!
//! # Running
//!
//! Each configuration is a test, ignored because a debug build takes too
//! long. CI runs them in release, except the largest ones (see
//! `docs/testing/nextest.md`); `--nocapture` prints each configuration's
//! counts and counterexamples. To run all twelve, one at a time (the largest,
//! split Shared mode with two workers and two channels, visits 308 million
//! states in about 9 minutes and needs about 23 GB):
//!
//! ```text
//! cargo test -p instancy --release --all-features --lib -- --include-ignored \
//!     --nocapture --test-threads 1 cluster_lifecycle::exhaustive::
//! ```

use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Write as _};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::time::Instant;

use super::*;

/// The peer's id.
const PEER: &str = "b";

// ---------------------------------------------------------------------------
// Configurations
// ---------------------------------------------------------------------------

/// How the dataflow's frames travel between us and the peer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Wiring {
    /// Shared mode: our lane and the peer's frames on one pooled connection.
    Shared,
    /// Shared mode: our lane on one pooled connection, the peer's frames on
    /// another (each node pins its own lane).
    SharedSplit,
    /// Dedicated mode: the dataflow's own connection.
    Dedicated,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    wiring: Wiring,
    /// Local workers.
    workers: u8,
    /// Progress channels from the peer: the end-of-stream markers its
    /// goodbye takes.
    channels: u8,
}

impl Config {
    fn shared(self) -> bool {
        self.wiring != Wiring::Dedicated
    }

    /// The name of its test.
    fn name(self) -> String {
        let wiring = match self.wiring {
            Wiring::Shared => "shared",
            Wiring::SharedSplit => "shared_split",
            Wiring::Dedicated => "dedicated",
        };
        let count = |n: u8, what: &str| match n {
            1 => format!("one_{what}"),
            2 => format!("two_{what}s"),
            _ => unreachable!("{n} {what}s"),
        };
        format!(
            "{wiring}_{}_{}",
            count(self.workers, "worker"),
            count(self.channels, "channel")
        )
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let wiring = match self.wiring {
            Wiring::Shared => "Shared",
            Wiring::SharedSplit => "Shared (split)",
            Wiring::Dedicated => "Dedicated",
        };
        let plural = |n: u8| if n == 1 { "" } else { "s" };
        write!(
            f,
            "{wiring}, {} worker{}, {} channel{}",
            self.workers,
            plural(self.workers),
            self.channels,
            plural(self.channels)
        )
    }
}

// ---------------------------------------------------------------------------
// Packing
// ---------------------------------------------------------------------------

/// A small value packed into a few bits of a [`Key`]. Codes are nonzero:
/// zero marks an empty slot.
trait Code: Copy {
    const BITS: u32;
    fn code(self) -> u64;
    fn from_code(code: u64) -> Self;
}

/// Bits written, or read, in order.
struct Bits {
    words: [u64; 3],
    at: u32,
}

impl Bits {
    fn new(words: [u64; 3]) -> Self {
        Self { words, at: 0 }
    }

    fn put(&mut self, value: u64, bits: u32) {
        assert!(value >> bits == 0, "{value} does not fit in {bits} bits");
        let (word, offset) = ((self.at / 64) as usize, self.at % 64);
        self.words[word] |= value << offset;
        if offset + bits > 64 {
            self.words[word + 1] |= value >> (64 - offset);
        }
        self.at += bits;
    }

    fn get(&mut self, bits: u32) -> u64 {
        let (word, offset) = ((self.at / 64) as usize, self.at % 64);
        let mut value = self.words[word] >> offset;
        if offset + bits > 64 {
            value |= self.words[word + 1] << (64 - offset);
        }
        self.at += bits;
        value & ((1 << bits) - 1)
    }

    fn put_flag(&mut self, flag: bool) {
        self.put(u64::from(flag), 1);
    }

    fn flag(&mut self) -> bool {
        self.get(1) == 1
    }

    fn put_small(&mut self, value: u8, bits: u32) {
        self.put(u64::from(value), bits);
    }

    fn small(&mut self, bits: u32) -> u8 {
        u8::try_from(self.get(bits)).expect("at most 8 bits")
    }

    fn put_item<T: Code>(&mut self, item: Option<T>) {
        self.put(item.map_or(0, T::code), T::BITS);
    }

    fn item<T: Code>(&mut self) -> Option<T> {
        match self.get(T::BITS) {
            0 => None,
            code => Some(T::from_code(code)),
        }
    }
}

/// A FIFO of at most `N` items, so a state is copied without allocating.
/// Slots past `len` are always `None`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Fifo<T: Copy, const N: usize> {
    items: [Option<T>; N],
    len: usize,
}

impl<T: Copy, const N: usize> Fifo<T, N> {
    const EMPTY: Self = Self {
        items: [None; N],
        len: 0,
    };

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn front(&self) -> Option<T> {
        self.items[0]
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

    /// Take out the first item `pick` selects; the rest keep their order.
    fn take_first(&mut self, pick: impl Fn(T) -> bool) -> Option<T> {
        let at = self.iter().position(pick)?;
        let item = self.items[at];
        self.items.copy_within(at + 1..self.len, at);
        self.len -= 1;
        self.items[self.len] = None;
        item
    }

    fn clear(&mut self) {
        *self = Self::EMPTY;
    }
}

impl<T: Code, const N: usize> Fifo<T, N> {
    fn pack(&self, bits: &mut Bits) {
        for item in self.items {
            bits.put_item(item);
        }
    }

    fn unpack(bits: &mut Bits) -> Self {
        let mut fifo = Self::EMPTY;
        for _ in 0..N {
            if let Some(item) = bits.item() {
                fifo.push_back(item);
            }
        }
        fifo
    }
}

// ---------------------------------------------------------------------------
// The environment's vocabulary
// ---------------------------------------------------------------------------

/// A cancellation: a requested stop, or a failure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Stop,
    Fail,
}

impl Code for Kind {
    const BITS: u32 = 2;

    fn code(self) -> u64 {
        match self {
            Self::Stop => 1,
            Self::Fail => 2,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Stop,
            2 => Self::Fail,
            _ => unreachable!("kind {code}"),
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stop => "a requested stop",
            Self::Fail => "a failure",
        })
    }
}

/// An event on the coordinator's channel.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ev {
    /// A worker finished: `true` for success.
    Done(bool),
    SendFailed,
    /// The connection the peer's frames arrive on (or, split, our lane's)
    /// ended: cleanly, or reset.
    ConnectionEnded {
        clean: bool,
    },
    Cancel,
    ControlClosed,
    EndOfStream,
    Ack,
    ChannelClosed,
}

impl Ev {
    /// The peer's `Cancel`, goodbye or ack: it needs nothing more from us.
    fn excuses(self) -> bool {
        matches!(self, Self::Cancel | Self::EndOfStream | Self::Ack)
    }

    fn event(self) -> LifecycleEvent {
        let link = |event| LifecycleEvent::Link {
            peer: PEER.to_string(),
            event,
        };
        match self {
            Self::Done(success) => LifecycleEvent::WorkerFinished {
                success,
                failure: None,
            },
            Self::SendFailed => link(LinkEvent::OutboundFailed(LinkFailure::SendFailed)),
            Self::ConnectionEnded { clean: true } => link(LinkEvent::ConnectionFailed(
                TransportError::ConnectionClosed,
            )),
            Self::ConnectionEnded { clean: false } => link(LinkEvent::ConnectionFailed(reset())),
            Self::Cancel => link(LinkEvent::CancelReceived),
            Self::ControlClosed => link(LinkEvent::ControlClosed),
            Self::EndOfStream => link(LinkEvent::ProgressEndOfStream),
            Self::Ack => link(LinkEvent::AckReceived),
            Self::ChannelClosed => {
                link(LinkEvent::ProgressChannelEnded(LinkFailure::ChannelClosed))
            }
        }
    }
}

impl Code for Ev {
    const BITS: u32 = 4;

    fn code(self) -> u64 {
        match self {
            Self::Done(true) => 1,
            Self::Done(false) => 2,
            Self::SendFailed => 3,
            Self::ConnectionEnded { clean: true } => 4,
            Self::ConnectionEnded { clean: false } => 5,
            Self::Cancel => 6,
            Self::ControlClosed => 7,
            Self::EndOfStream => 8,
            Self::Ack => 9,
            Self::ChannelClosed => 10,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Done(true),
            2 => Self::Done(false),
            3 => Self::SendFailed,
            4 => Self::ConnectionEnded { clean: true },
            5 => Self::ConnectionEnded { clean: false },
            6 => Self::Cancel,
            7 => Self::ControlClosed,
            8 => Self::EndOfStream,
            9 => Self::Ack,
            10 => Self::ChannelClosed,
            _ => unreachable!("event {code}"),
        }
    }
}

impl fmt::Display for Ev {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Done(true) => "WorkerFinished(success)",
            Self::Done(false) => "WorkerFinished(failure)",
            Self::SendFailed => "OutboundFailed(SendFailed)",
            Self::ConnectionEnded { clean: true } => "ConnectionFailed(closed)",
            Self::ConnectionEnded { clean: false } => "ConnectionFailed(reset)",
            Self::Cancel => "CancelReceived",
            Self::ControlClosed => "ControlClosed",
            Self::EndOfStream => "ProgressEndOfStream",
            Self::Ack => "AckReceived",
            Self::ChannelClosed => "ProgressChannelEnded(ChannelClosed)",
        })
    }
}

/// A reset connection, as a read error reports it.
fn reset() -> TransportError {
    TransportError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection reset",
    ))
}

/// A transport notice on its way to the io forwarder (`PeerIoEvent`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Notice {
    /// `WriteFailed`: frames to the peer may be lost.
    WriteFailed,
    /// `Closed` or `ReadFailed` for the read side the peer's frames arrive
    /// on: the peer's channels close once this has been handed on.
    InboundEnded { clean: bool },
    /// Split: `Closed` for our lane, whose connection the peer closed.
    LaneClosed,
}

impl Notice {
    /// The event the io forwarder reports.
    fn event(self) -> Ev {
        match self {
            Self::WriteFailed => Ev::SendFailed,
            Self::InboundEnded { clean } => Ev::ConnectionEnded { clean },
            Self::LaneClosed => Ev::ConnectionEnded { clean: true },
        }
    }
}

impl Code for Notice {
    const BITS: u32 = 3;

    fn code(self) -> u64 {
        match self {
            Self::WriteFailed => 1,
            Self::InboundEnded { clean: true } => 2,
            Self::InboundEnded { clean: false } => 3,
            Self::LaneClosed => 4,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::WriteFailed,
            2 => Self::InboundEnded { clean: true },
            3 => Self::InboundEnded { clean: false },
            4 => Self::LaneClosed,
            _ => unreachable!("notice {code}"),
        }
    }
}

impl fmt::Display for Notice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::WriteFailed => "WriteFailed",
            Self::InboundEnded { clean: true } => "Closed",
            Self::InboundEnded { clean: false } => "ReadFailed",
            Self::LaneClosed => "Closed (our lane's connection)",
        })
    }
}

/// A frame we send the peer: on our lane, or (`Cancel`) on the control
/// channel.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Out {
    /// Our workers' records and progress, which the peer needs to finish.
    Records,
    Goodbye,
    Ack,
    Cancel,
}

impl Code for Out {
    const BITS: u32 = 3;

    fn code(self) -> u64 {
        match self {
            Self::Records => 1,
            Self::Goodbye => 2,
            Self::Ack => 3,
            Self::Cancel => 4,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Records,
            2 => Self::Goodbye,
            3 => Self::Ack,
            4 => Self::Cancel,
            _ => unreachable!("frame {code}"),
        }
    }
}

/// A frame the peer sends us.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum In {
    Cancel(Kind),
    /// Its end-of-stream on one progress channel.
    EndOfStream(u8),
    /// Its ack of our goodbye, on its first progress channel to us.
    Ack,
}

impl Code for In {
    const BITS: u32 = 3;

    fn code(self) -> u64 {
        match self {
            Self::Cancel(Kind::Stop) => 1,
            Self::Cancel(Kind::Fail) => 2,
            Self::EndOfStream(0) => 3,
            Self::EndOfStream(1) => 4,
            Self::EndOfStream(ch) => unreachable!("channel {ch}"),
            Self::Ack => 5,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Cancel(Kind::Stop),
            2 => Self::Cancel(Kind::Fail),
            3 => Self::EndOfStream(0),
            4 => Self::EndOfStream(1),
            5 => Self::Ack,
            _ => unreachable!("frame {code}"),
        }
    }
}

impl fmt::Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancel(kind) => write!(f, "Cancel ({kind})"),
            Self::EndOfStream(ch) => write!(f, "end-of-stream on channel {ch}"),
            Self::Ack => f.write_str("ack"),
        }
    }
}

/// The outbox forwarder (`run_forwarder`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Forwarder {
    Idle,
    /// It holds a frame the transport will refuse.
    Refused,
    /// It exited: the outbox is closed.
    Exited,
}

impl Forwarder {
    fn code(self) -> u64 {
        match self {
            Self::Idle => 0,
            Self::Refused => 1,
            Self::Exited => 2,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            0 => Self::Idle,
            1 => Self::Refused,
            2 => Self::Exited,
            _ => unreachable!("forwarder {code}"),
        }
    }
}

// ---------------------------------------------------------------------------
// The world around the machine
// ---------------------------------------------------------------------------

/// Everything but the machine: the coordinator's channel and today's
/// driver's state, our workers, the transport, the peer, and the ghosts the
/// properties need.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct World {
    // The coordinator.
    queue: Fifo<Ev, 12>,
    /// The dataflow token's first reason (`cancel_with_reason` and
    /// `escalate` keep it).
    token: Option<Kind>,
    /// A failure was escalated on a token cancelled for a requested stop.
    escalated: bool,
    /// The driver has taken the token's branch.
    cancel_seen: bool,
    /// The bridges were cancelled from outside.
    aborted: bool,
    /// A grace deadline is armed.
    timer: bool,
    /// Torn down.
    finished: bool,
    /// The driver's `failure_announced` and `unfinished_announced`.
    failure_announced: bool,
    unfinished_announced: bool,
    // Our workers.
    workers_left: u8,
    /// A worker failed (or was cancelled).
    workers_failed: bool,
    // Our frames to the peer.
    /// Frames on their way to the peer, in the order they were queued: the
    /// lane's (in the outbox, the forwarder, the transport's queue and the
    /// writer) and our `Cancel`s (in the transport's control queue). See
    /// [`World::next_on_lane`] and [`World::cancel_out`] for which goes
    /// first.
    outbound: Fifo<Out, 7>,
    /// How many of the `Cancel`s at the front of `outbound` no lane frame
    /// may pass: a lane frame passed them while they waited in the control
    /// queue, so they go next.
    locked: u8,
    /// Our writer, or (Shared) our lane, failed: no frame reaches the peer
    /// any more, and the transport has queued its notice.
    writer_dead: bool,
    /// Dedicated: the failed writer dropped its queues, so the transport
    /// refuses the forwarder's frames.
    refusing: bool,
    /// Dedicated: a frame went to the failed writer's queue, or was behind
    /// the one it was writing, and may still be with the forwarder when the
    /// queues are dropped.
    unsure: bool,
    /// The next write fails (after a reset, or once the peer has gone).
    write_fails: bool,
    forwarder: Forwarder,
    /// Split: the reader of the connection our lane is pinned to.
    lane_reader_up: bool,
    // The peer's frames to us.
    /// Frames on their way from the peer, in the order it queued them (see
    /// `Step::ReadCancel` and `Step::ReadPastCancel` for the order they come
    /// in).
    wire: Fifo<In, 4>,
    /// A lane frame of the peer passed its `Cancel` (`Step::ReadPastCancel`),
    /// which waited in its control queue: the `Cancel` comes next.
    peer_locked: bool,
    reader_up: bool,
    /// Dedicated: the reader fails next (after a reset).
    read_fails: bool,
    /// Our reader failed on the connection that also carries our lane (in
    /// Dedicated mode, or Shared mode on one connection), so our frames can
    /// no longer reach the peer either.
    read_failed: bool,
    io: Fifo<Notice, 4>,
    /// The end of the read side was handed on: the peer's channels close.
    channels_closed: bool,
    /// `Cancel` frames read, not yet taken by the control dispatcher.
    ctrl: Fifo<Kind, 2>,
    /// The dispatcher escalated the token for a `Cancel` it has not reported
    /// yet.
    dispatching: bool,
    ctrl_close_reported: bool,
    /// Frames read, not yet taken by each progress bridge.
    prog: [Fifo<In, 2>; 2],
    end_of_stream: [bool; 2],
    prog_close_reported: [bool; 2],
    // The connection.
    /// Our frames can still reach the peer.
    to_peer: bool,
    /// The peer's frames can still reach us.
    from_peer: bool,
    /// The fault happened.
    fault: bool,
    // The peer.
    records_delivered: bool,
    goodbye_delivered: bool,
    peer_goodbye: bool,
    peer_ack: bool,
    peer_cancel: Option<Kind>,
    peer_fin: bool,
    peer_gone: bool,
    // Ghosts, latched when the last worker succeeded (if every one did): the
    // send had failed; our records had not reached the peer, and our side had
    // seen the connection that carries them fail (a write error, or a failed
    // read), so they never will.
    failed_at_finish: bool,
    lost_at_finish: bool,
}

impl World {
    fn new(cfg: Config) -> Self {
        let mut outbound = Fifo::EMPTY;
        outbound.push_back(Out::Records);
        Self {
            queue: Fifo::EMPTY,
            token: None,
            escalated: false,
            cancel_seen: false,
            aborted: false,
            timer: false,
            finished: false,
            failure_announced: false,
            unfinished_announced: false,
            workers_left: cfg.workers,
            workers_failed: false,
            outbound,
            locked: 0,
            writer_dead: false,
            refusing: false,
            unsure: false,
            write_fails: false,
            forwarder: Forwarder::Idle,
            lane_reader_up: cfg.wiring == Wiring::SharedSplit,
            wire: Fifo::EMPTY,
            peer_locked: false,
            reader_up: true,
            read_fails: false,
            read_failed: false,
            io: Fifo::EMPTY,
            channels_closed: false,
            ctrl: Fifo::EMPTY,
            dispatching: false,
            ctrl_close_reported: false,
            prog: [Fifo::EMPTY; 2],
            end_of_stream: [false; 2],
            prog_close_reported: [false; 2],
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

    /// Cancel the dataflow token (`cancel_with_reason`): its first reason
    /// stays.
    fn cancel(&mut self, kind: Kind) {
        if self.token.is_none() {
            self.token = Some(kind);
        }
    }

    /// `escalate`: cancel, and record a failure that follows a requested
    /// stop.
    fn escalate(&mut self, kind: Kind) {
        if self.token == Some(Kind::Stop) && kind == Kind::Fail {
            self.escalated = true;
        }
        self.cancel(kind);
    }

    /// The `Cancel` the driver broadcasts describes a failure
    /// (`cancel_broadcast_reason`): an escalation, the token's first reason,
    /// or, with the token not cancelled, the bridges stopped from outside.
    fn broadcast_is_failure(&self) -> bool {
        self.escalated || self.token != Some(Kind::Stop)
    }

    /// The driver broadcasts our `Cancel` (`broadcast_cancel_with_transport`:
    /// a frame on the control channel, its failure ignored).
    fn send_cancel(&mut self) {
        if !self.writer_dead {
            self.outbound.push_back(Out::Cancel);
        }
    }

    /// The coordinator queues a frame for the peer (`PeerOutbox::send`).
    fn send(&mut self, frame: Out, cfg: Config) {
        match (self.writer_dead, cfg.shared(), self.refusing) {
            (false, _, _) => self.outbound.push_back(frame),
            // The manager drops the frames of a failed lane (`send_payload`).
            (true, true, _) => {}
            // Into the queue the failed writer no longer drains: lost, unless
            // the forwarder still holds it when the queue is dropped.
            (true, false, false) => self.unsure = true,
            // Refused once the forwarder hands it on. Behind a frame it
            // already holds, or after it exited, it is lost.
            (true, false, true) => {
                if self.forwarder == Forwarder::Idle {
                    self.forwarder = Forwarder::Refused;
                }
            }
        }
    }

    /// The lane frame that can reach the peer next, and how many of our
    /// `Cancel`s queued before it it passes. Each transport's `bridge_task`
    /// takes a queued control frame first, but its biased `select!` polls
    /// the control queue, then the lane's: a `Cancel` queued between the two
    /// polls lets the lane frame the second one takes go first. So only the
    /// first lane frame behind the `Cancel`s at the front of the queue can
    /// pass them (those queued during that `select!`), and none can pass a
    /// `Cancel` a lane frame has passed already (see `locked`).
    fn next_on_lane(&self) -> Option<(Out, u8)> {
        let ahead = self
            .outbound
            .iter()
            .take_while(|&frame| frame == Out::Cancel)
            .count();
        let frame = self.outbound.iter().nth(ahead)?;
        let ahead = u8::try_from(ahead).expect("a few Cancels");
        (ahead == 0 || self.locked == 0).then_some((frame, ahead))
    }

    /// One of our `Cancel`s is on its way to the peer. The first can reach
    /// it ahead of any lane frame queued before it that the transport had
    /// not taken when the `Cancel` was queued.
    fn cancel_out(&self) -> bool {
        self.outbound.iter().any(|frame| frame == Out::Cancel)
    }

    /// The reader hands on a frame from the peer.
    fn hand_on(&mut self, frame: In) {
        match frame {
            In::Cancel(kind) => {
                self.peer_locked = false;
                self.ctrl.push_back(kind);
            }
            In::EndOfStream(ch) => self.prog[usize::from(ch)].push_back(frame),
            In::Ack => self.prog[0].push_back(frame),
        }
    }

    /// The peer's frames on their way to us are lost.
    fn lose_inbound(&mut self) {
        self.wire.clear();
        self.peer_locked = false;
    }

    /// Our frames on their way to the peer are lost.
    fn lose_outbound(&mut self) {
        self.outbound.clear();
        self.locked = 0;
    }

    /// Our writer, or (Shared) our lane, fails: the frame being written and
    /// everything behind it never reach the peer, and the transport queues
    /// `WriteFailed`.
    fn writer_fails(&mut self, cfg: Config) {
        // The failed write may be our first `Cancel` or the next lane frame:
        // a lane frame may be behind it.
        let lane = self.outbound.iter().filter(|&f| f != Out::Cancel).count();
        let behind = lane > 1 || (self.cancel_out() && lane > 0);
        self.writer_dead = true;
        self.to_peer = false;
        self.lose_outbound();
        self.io.push_back(Notice::WriteFailed);
        match cfg.wiring {
            // `bridge_task` drops its queues next (`Step::DropQueues`).
            Wiring::Dedicated => self.unsure = behind,
            // The failed connection closes (`writer_task` stops its reader);
            // the peer's frames arrive on the other one.
            Wiring::SharedSplit => self.lane_reader_up = false,
            // ... and its reader is the one the peer's frames arrive on.
            Wiring::Shared => {
                self.from_peer = false;
                self.lose_inbound();
                if self.reader_up {
                    self.reader_up = false;
                    self.io.push_back(Notice::InboundEnded { clean: false });
                }
            }
        }
    }

    /// The connection is reset (split: the one our lane is pinned to).
    fn reset(&mut self, cfg: Config, write_first: bool) {
        if cfg.wiring == Wiring::SharedSplit {
            if self.lane_reader_up {
                // Its reader or its writer fails at once, failing the lane.
                self.writer_fails(cfg);
            } else {
                // Its reader saw the peer's close: the next write fails.
                self.to_peer = false;
                self.write_fails = true;
            }
            return;
        }
        self.lose_inbound();
        self.to_peer = false;
        self.from_peer = false;
        match cfg.wiring {
            Wiring::Shared if self.reader_up => {
                // A failed read fails the lane at once (`fail_inbound`) and
                // reports both, in either order.
                assert!(!self.writer_dead, "a failed lane closes its reader");
                self.reader_up = false;
                self.read_failed = true;
                self.writer_dead = true;
                self.lose_outbound();
                let mut reports = [Notice::WriteFailed, Notice::InboundEnded { clean: false }];
                if !write_first {
                    reports.reverse();
                }
                for report in reports {
                    self.io.push_back(report);
                }
            }
            // The reader saw the peer's close: the next write fails.
            Wiring::Shared => self.write_fails = true,
            // The reader fails, and the writer at its next write, apart.
            _ => {
                self.read_fails = self.reader_up;
                self.write_fails = true;
            }
        }
    }

    /// Something is on its way to the coordinator that is up to neither the
    /// peer, a worker nor a fault. Each case enables a step bound to happen.
    fn busy(&self, cfg: Config) -> bool {
        let unreported = |ch: usize| !self.end_of_stream[ch] && !self.prog_close_reported[ch];
        !self.queue.is_empty()
            || !self.io.is_empty()
            || !self.ctrl.is_empty()
            || self.dispatching
            || self.prog.iter().any(|queue| !queue.is_empty())
            || (self.channels_closed
                && (!self.ctrl_close_reported || (0..usize::from(cfg.channels)).any(unreported)))
            || self.forwarder == Forwarder::Refused
            || (self.writer_dead && !self.refusing && !cfg.shared())
            || (self.write_fails && self.writing())
            || (self.reader_up && (!self.wire.is_empty() || self.peer_fin))
            || self.read_fails
            || (self.lane_reader_up && self.peer_fin)
    }

    /// The peer's `Cancel`, goodbye or ack is on its way to the coordinator.
    fn excuse_on_its_way(&self) -> bool {
        self.queue.iter().any(Ev::excuses)
            || !self.ctrl.is_empty()
            || self.dispatching
            || self.prog.iter().any(|queue| !queue.is_empty())
            || (self.reader_up && !self.wire.is_empty())
    }

    /// The peer has gone, or can no longer be reached either way.
    fn peer_has_gone(&self) -> bool {
        self.peer_gone || (!self.to_peer && !self.from_peer)
    }

    /// Our writer has something to write.
    fn writing(&self) -> bool {
        !self.writer_dead && !self.outbound.is_empty()
    }

    /// The last worker succeeded, and so did every other.
    fn latch(&mut self) {
        self.failed_at_finish = self.writer_dead;
        self.lost_at_finish = !self.records_delivered && (self.writer_dead || self.read_failed);
    }

    /// The identity of a violation found in this state: the environment's
    /// side of it, without the coordinator's channel and the driver's state,
    /// and nothing of the machine, whose representation may change.
    fn identity(&self) -> u64 {
        let mut env = *self;
        env.queue = Fifo::EMPTY;
        env.cancel_seen = false;
        env.timer = false;
        env.finished = false;
        env.failure_announced = false;
        env.unfinished_announced = false;
        let mut bits = Bits::new([0; 3]);
        env.pack(&mut bits);
        mix(bits.words)
    }

    fn pack(&self, bits: &mut Bits) {
        let Self {
            queue,
            token,
            escalated,
            cancel_seen,
            aborted,
            timer,
            finished,
            failure_announced,
            unfinished_announced,
            workers_left,
            workers_failed,
            outbound,
            locked,
            writer_dead,
            refusing,
            unsure,
            write_fails,
            forwarder,
            lane_reader_up,
            wire,
            peer_locked,
            reader_up,
            read_fails,
            read_failed,
            io,
            channels_closed,
            ctrl,
            dispatching,
            ctrl_close_reported,
            prog,
            end_of_stream,
            prog_close_reported,
            to_peer,
            from_peer,
            fault,
            records_delivered,
            goodbye_delivered,
            peer_goodbye,
            peer_ack,
            peer_cancel,
            peer_fin,
            peer_gone,
            failed_at_finish,
            lost_at_finish,
        } = self;
        queue.pack(bits);
        bits.put_item(*token);
        bits.put_flag(*escalated);
        bits.put_flag(*cancel_seen);
        bits.put_flag(*aborted);
        bits.put_flag(*timer);
        bits.put_flag(*finished);
        bits.put_flag(*failure_announced);
        bits.put_flag(*unfinished_announced);
        bits.put_small(*workers_left, 2);
        bits.put_flag(*workers_failed);
        outbound.pack(bits);
        bits.put_small(*locked, 2);
        bits.put_flag(*writer_dead);
        bits.put_flag(*refusing);
        bits.put_flag(*unsure);
        bits.put_flag(*write_fails);
        bits.put(forwarder.code(), 2);
        bits.put_flag(*lane_reader_up);
        wire.pack(bits);
        bits.put_flag(*peer_locked);
        bits.put_flag(*reader_up);
        bits.put_flag(*read_fails);
        bits.put_flag(*read_failed);
        io.pack(bits);
        bits.put_flag(*channels_closed);
        ctrl.pack(bits);
        bits.put_flag(*dispatching);
        bits.put_flag(*ctrl_close_reported);
        for queue in prog {
            queue.pack(bits);
        }
        for flag in end_of_stream.iter().chain(prog_close_reported) {
            bits.put_flag(*flag);
        }
        bits.put_flag(*to_peer);
        bits.put_flag(*from_peer);
        bits.put_flag(*fault);
        bits.put_flag(*records_delivered);
        bits.put_flag(*goodbye_delivered);
        bits.put_flag(*peer_goodbye);
        bits.put_flag(*peer_ack);
        bits.put_item(*peer_cancel);
        bits.put_flag(*peer_fin);
        bits.put_flag(*peer_gone);
        bits.put_flag(*failed_at_finish);
        bits.put_flag(*lost_at_finish);
    }

    /// The fields in the order [`Self::pack`] wrote them.
    fn unpack(bits: &mut Bits) -> Self {
        Self {
            queue: Fifo::unpack(bits),
            token: bits.item(),
            escalated: bits.flag(),
            cancel_seen: bits.flag(),
            aborted: bits.flag(),
            timer: bits.flag(),
            finished: bits.flag(),
            failure_announced: bits.flag(),
            unfinished_announced: bits.flag(),
            workers_left: bits.small(2),
            workers_failed: bits.flag(),
            outbound: Fifo::unpack(bits),
            locked: bits.small(2),
            writer_dead: bits.flag(),
            refusing: bits.flag(),
            unsure: bits.flag(),
            write_fails: bits.flag(),
            forwarder: Forwarder::from_code(bits.get(2)),
            lane_reader_up: bits.flag(),
            wire: Fifo::unpack(bits),
            peer_locked: bits.flag(),
            reader_up: bits.flag(),
            read_fails: bits.flag(),
            read_failed: bits.flag(),
            io: Fifo::unpack(bits),
            channels_closed: bits.flag(),
            ctrl: Fifo::unpack(bits),
            dispatching: bits.flag(),
            ctrl_close_reported: bits.flag(),
            prog: [Fifo::unpack(bits), Fifo::unpack(bits)],
            end_of_stream: [bits.flag(), bits.flag()],
            prog_close_reported: [bits.flag(), bits.flag()],
            to_peer: bits.flag(),
            from_peer: bits.flag(),
            fault: bits.flag(),
            records_delivered: bits.flag(),
            goodbye_delivered: bits.flag(),
            peer_goodbye: bits.flag(),
            peer_ack: bits.flag(),
            peer_cancel: bits.item(),
            peer_fin: bits.flag(),
            peer_gone: bits.flag(),
            failed_at_finish: bits.flag(),
            lost_at_finish: bits.flag(),
        }
    }
}

// ---------------------------------------------------------------------------
// The machine's state
// ---------------------------------------------------------------------------

/// A failure the machine can hold as a soft failure here.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Failure {
    SendFailed,
    ChannelClosed,
    ConnectionClosed,
    ConnectionReset,
}

impl Failure {
    fn of(failure: &LinkFailure) -> Self {
        match failure {
            LinkFailure::SendFailed => Self::SendFailed,
            LinkFailure::ChannelClosed => Self::ChannelClosed,
            LinkFailure::Transport(TransportError::ConnectionClosed) => Self::ConnectionClosed,
            LinkFailure::Transport(TransportError::Io(error))
                if error.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                Self::ConnectionReset
            }
            other => panic!("no event here carries this failure: {other}"),
        }
    }

    fn link_failure(self) -> LinkFailure {
        match self {
            Self::SendFailed => LinkFailure::SendFailed,
            Self::ChannelClosed => LinkFailure::ChannelClosed,
            Self::ConnectionClosed => LinkFailure::Transport(TransportError::ConnectionClosed),
            Self::ConnectionReset => LinkFailure::Transport(reset()),
        }
    }
}

impl Code for Failure {
    const BITS: u32 = 3;

    fn code(self) -> u64 {
        match self {
            Self::SendFailed => 1,
            Self::ChannelClosed => 2,
            Self::ConnectionClosed => 3,
            Self::ConnectionReset => 4,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::SendFailed,
            2 => Self::ChannelClosed,
            3 => Self::ConnectionClosed,
            4 => Self::ConnectionReset,
            _ => unreachable!("failure {code}"),
        }
    }
}

/// The data losses seen so far. A [`Mach`] holds a loss as 1 + its index
/// here, 0 for none.
#[derive(Default)]
struct Losses(Vec<DataLoss>);

impl Losses {
    fn code(&mut self, loss: &Option<DataLoss>) -> u8 {
        let Some(loss) = loss else {
            return 0;
        };
        let index = match self.0.iter().position(|known| known == loss) {
            Some(index) => index,
            None => {
                self.0.push(loss.clone());
                self.0.len() - 1
            }
        };
        u8::try_from(index + 1).expect("few losses")
    }

    fn get(&self, code: u8) -> Option<DataLoss> {
        code.checked_sub(1)
            .map(|index| self.0[usize::from(index)].clone())
    }
}

fn small(value: usize) -> u8 {
    u8::try_from(value).expect("a small count")
}

/// Every field of the machine, small: what a [`Key`] packs, and what a
/// coordinator step rebuilds the real machine from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Mach {
    phase: Phase,
    workers_remaining: u8,
    all_workers_ok: bool,
    goodbye_sent: bool,
    cancel_broadcast: bool,
    requested_stop: bool,
    drain_aborted: bool,
    data_loss: u8,
    pending_loss: u8,
    deferred_loss: u8,
    workers_succeeded: u8,
    // The peer's state.
    expected_end_of_stream: u8,
    end_of_stream: u8,
    progress_channels_ended: u8,
    goodbye_received: bool,
    ack_received: bool,
    control_closed: bool,
    lost: bool,
    soft_failure: Option<Failure>,
    unclean_data_channel: bool,
    cancel_received: bool,
    stopped: bool,
    // The link's state.
    retired: bool,
    push_blocked: bool,
}

impl Mach {
    /// The destructuring lists every field, so a new one is not left out.
    fn of(m: &LifecycleMachine, losses: &mut Losses) -> Self {
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
        assert_eq!(peers.len(), 1, "one peer");
        let PeerState {
            expected_end_of_stream,
            end_of_stream,
            progress_channels_ended,
            goodbye_received,
            ack_received,
            control_closed,
            lost,
            soft_failure,
            unclean_data_channel,
            cancel_received,
            stopped,
            link,
        } = &peers[PEER];
        Self {
            phase: *phase,
            workers_remaining: small(*workers_remaining),
            all_workers_ok: *all_workers_ok,
            goodbye_sent: *goodbye_sent,
            cancel_broadcast: *cancel_broadcast,
            requested_stop: *requested_stop,
            drain_aborted: *drain_aborted,
            data_loss: losses.code(data_loss),
            pending_loss: losses.code(pending_loss),
            deferred_loss: losses.code(deferred_loss),
            workers_succeeded: small(*workers_succeeded),
            expected_end_of_stream: small(*expected_end_of_stream),
            end_of_stream: small(*end_of_stream),
            progress_channels_ended: small(*progress_channels_ended),
            goodbye_received: *goodbye_received,
            ack_received: *ack_received,
            control_closed: *control_closed,
            lost: *lost,
            soft_failure: soft_failure.as_ref().map(Failure::of),
            unclean_data_channel: *unclean_data_channel,
            cancel_received: *cancel_received,
            stopped: *stopped,
            retired: link.is_retired(),
            push_blocked: link.push_blocked(),
        }
    }

    /// The machine, with a link of its own.
    fn machine(&self, losses: &Losses) -> LifecycleMachine {
        let link = Arc::new(PeerLinkState::default());
        if self.retired {
            link.retire();
        }
        if self.push_blocked {
            link.note_push_blocked();
        }
        let peer = PeerState {
            expected_end_of_stream: usize::from(self.expected_end_of_stream),
            end_of_stream: usize::from(self.end_of_stream),
            progress_channels_ended: usize::from(self.progress_channels_ended),
            goodbye_received: self.goodbye_received,
            ack_received: self.ack_received,
            control_closed: self.control_closed,
            lost: self.lost,
            soft_failure: self.soft_failure.map(Failure::link_failure),
            unclean_data_channel: self.unclean_data_channel,
            cancel_received: self.cancel_received,
            stopped: self.stopped,
            link,
        };
        LifecycleMachine {
            phase: self.phase,
            workers_remaining: usize::from(self.workers_remaining),
            all_workers_ok: self.all_workers_ok,
            goodbye_sent: self.goodbye_sent,
            cancel_broadcast: self.cancel_broadcast,
            requested_stop: self.requested_stop,
            drain_aborted: self.drain_aborted,
            data_loss: losses.get(self.data_loss),
            pending_loss: losses.get(self.pending_loss),
            deferred_loss: losses.get(self.deferred_loss),
            workers_succeeded: usize::from(self.workers_succeeded),
            peers: BTreeMap::from([(PEER.to_string(), peer)]),
        }
    }

    fn pack(&self, bits: &mut Bits) {
        let Self {
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
            expected_end_of_stream,
            end_of_stream,
            progress_channels_ended,
            goodbye_received,
            ack_received,
            control_closed,
            lost,
            soft_failure,
            unclean_data_channel,
            cancel_received,
            stopped,
            retired,
            push_blocked,
        } = self;
        bits.put(
            match phase {
                Phase::Running => 0,
                Phase::Draining => 1,
                Phase::Finished => 2,
            },
            2,
        );
        bits.put_small(*workers_remaining, 2);
        for flag in [
            all_workers_ok,
            goodbye_sent,
            cancel_broadcast,
            requested_stop,
            drain_aborted,
        ] {
            bits.put_flag(*flag);
        }
        for loss in [data_loss, pending_loss, deferred_loss] {
            bits.put_small(*loss, 2);
        }
        for count in [
            workers_succeeded,
            expected_end_of_stream,
            end_of_stream,
            progress_channels_ended,
        ] {
            bits.put_small(*count, 2);
        }
        for flag in [goodbye_received, ack_received, control_closed, lost] {
            bits.put_flag(*flag);
        }
        bits.put_item(*soft_failure);
        for flag in [
            unclean_data_channel,
            cancel_received,
            stopped,
            retired,
            push_blocked,
        ] {
            bits.put_flag(*flag);
        }
    }

    /// The fields in the order [`Self::pack`] wrote them.
    fn unpack(bits: &mut Bits) -> Self {
        Self {
            phase: match bits.get(2) {
                0 => Phase::Running,
                1 => Phase::Draining,
                2 => Phase::Finished,
                code => unreachable!("phase {code}"),
            },
            workers_remaining: bits.small(2),
            all_workers_ok: bits.flag(),
            goodbye_sent: bits.flag(),
            cancel_broadcast: bits.flag(),
            requested_stop: bits.flag(),
            drain_aborted: bits.flag(),
            data_loss: bits.small(2),
            pending_loss: bits.small(2),
            deferred_loss: bits.small(2),
            workers_succeeded: bits.small(2),
            expected_end_of_stream: bits.small(2),
            end_of_stream: bits.small(2),
            progress_channels_ended: bits.small(2),
            goodbye_received: bits.flag(),
            ack_received: bits.flag(),
            control_closed: bits.flag(),
            lost: bits.flag(),
            soft_failure: bits.item(),
            unclean_data_channel: bits.flag(),
            cancel_received: bits.flag(),
            stopped: bits.flag(),
            retired: bits.flag(),
            push_blocked: bits.flag(),
        }
    }
}

/// A state, packed exactly: the world and every field of the machine.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Key([u64; 3]);

impl Key {
    fn pack(w: &World, m: &Mach) -> Self {
        let mut bits = Bits::new([0; 3]);
        w.pack(&mut bits);
        m.pack(&mut bits);
        Self(bits.words)
    }

    fn unpack(self) -> (World, Mach) {
        let mut bits = Bits::new(self.0);
        let w = World::unpack(&mut bits);
        (w, Mach::unpack(&mut bits))
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(mix(self.0));
    }
}

/// A well-mixed 64-bit hash of three words.
fn mix(words: [u64; 3]) -> u64 {
    let mut h = 0x9e37_79b9_7f4a_7c15_u64;
    for word in words {
        h = (h ^ word).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h ^= h >> 31;
    }
    h.wrapping_mul(0x94d0_49bb_1331_11eb) ^ (h >> 29)
}

/// The hasher of the visited set: a [`Key`] hashes itself.
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("a key hashes itself")
    }

    fn write_u64(&mut self, hash: u64) {
        self.0 = hash;
    }
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    // The coordinator: the only steps that change the machine.
    TokenSeen,
    AbortSeen,
    Apply,
    GraceEnds,
    // Our side.
    Finish(bool),
    LocalCancel(Kind),
    ExternalAbort,
    // Our frames to the peer.
    Deliver,
    /// One of our `Cancel` frames reaches the peer (who cancels or not on
    /// its own anyway).
    DeliverCancel,
    WriteFails,
    /// Dedicated: the failed writer drops its queues.
    DropQueues {
        forwarder_holds: bool,
    },
    ForwarderRefused,
    // The peer's frames to us.
    Read,
    /// The reader hands on the peer's `Cancel` ahead of frames the peer
    /// queued before it: its transport had not taken them when the `Cancel`
    /// was queued, and takes control frames first.
    ReadCancel,
    /// The reader hands on the peer's next lane frame ahead of its `Cancel`
    /// queued before it: the peer's transport had found no control frame
    /// and was taking the lane's next one when the `Cancel` was queued (see
    /// [`World::next_on_lane`]). The `Cancel` comes next.
    ReadPastCancel,
    ReaderEof,
    ReadFails,
    LaneEof,
    ForwardNotice,
    Dispatch,
    DispatchReport,
    DispatchClosed,
    Bridge(u8),
    BridgeClosed(u8),
    // The peer.
    PeerGoodbye,
    PeerAck,
    PeerCancel(Kind),
    PeerFin,
    PeerGone,
    // Faults.
    WriteError,
    Reset {
        write_first: bool,
    },
    /// Split: the connection the peer's frames arrive on is reset.
    ResetInbound,
}

impl Step {
    fn coordinator(self) -> bool {
        matches!(
            self,
            Self::TokenSeen | Self::AbortSeen | Self::Apply | Self::GraceEnds
        )
    }

    /// Bound to happen once it can. The rest may never happen: faults, a
    /// local cancellation, the abort from outside, and the peer's goodbye,
    /// ack and `Cancel`.
    fn bound_to_happen(self) -> bool {
        !matches!(
            self,
            Self::LocalCancel(_)
                | Self::ExternalAbort
                | Self::PeerGoodbye
                | Self::PeerAck
                | Self::PeerCancel(_)
                | Self::WriteError
                | Self::Reset { .. }
                | Self::ResetInbound
        )
    }
}

/// Every step that can happen in `w`, the coordinator's first.
fn steps(w: &World, cfg: Config, out: &mut Vec<Step>) {
    out.clear();
    if w.finished {
        // Torn down: our workers may still finish, and nothing else matters.
        if w.workers_left > 0 {
            out.extend([Step::Finish(true), Step::Finish(false)]);
        }
        return;
    }
    out.extend(driver::next(w, cfg));
    // Our workers. One may succeed although the dataflow was cancelled (it
    // drained, or it finished first), and may fail at any time.
    if w.workers_left > 0 {
        out.extend([Step::Finish(true), Step::Finish(false)]);
    }
    // Our frames to the peer.
    let writing = w.writing();
    if writing && w.to_peer && !w.write_fails {
        if w.cancel_out() {
            out.push(Step::DeliverCancel);
        }
        if w.next_on_lane().is_some() {
            out.push(Step::Deliver);
        }
    }
    if writing && w.write_fails {
        out.push(Step::WriteFails);
    }
    if w.writer_dead && !w.refusing && !cfg.shared() {
        out.push(Step::DropQueues {
            forwarder_holds: false,
        });
        if w.unsure {
            out.push(Step::DropQueues {
                forwarder_holds: true,
            });
        }
    }
    if w.forwarder == Forwarder::Refused {
        out.push(Step::ForwarderRefused);
    }
    // The peer's frames to us.
    if w.reader_up && !w.read_fails {
        if let Some(front) = w.wire.front() {
            out.push(Step::Read);
            if w.wire
                .iter()
                .skip(1)
                .any(|frame| matches!(frame, In::Cancel(_)))
            {
                out.push(Step::ReadCancel);
            }
            if matches!(front, In::Cancel(_)) && !w.peer_locked && w.wire.len() > 1 {
                out.push(Step::ReadPastCancel);
            }
        } else if w.peer_fin {
            out.push(Step::ReaderEof);
        }
    }
    if w.read_fails {
        out.push(Step::ReadFails);
    }
    if w.lane_reader_up && w.peer_fin {
        out.push(Step::LaneEof);
    }
    if !w.io.is_empty() {
        out.push(Step::ForwardNotice);
    }
    if w.dispatching {
        out.push(Step::DispatchReport);
    } else if !w.ctrl.is_empty() {
        out.push(Step::Dispatch);
    } else if w.channels_closed && !w.ctrl_close_reported {
        out.push(Step::DispatchClosed);
    }
    for ch in 0..cfg.channels {
        let i = usize::from(ch);
        if !w.prog[i].is_empty() {
            out.push(Step::Bridge(ch));
        } else if w.channels_closed && !w.end_of_stream[i] && !w.prog_close_reported[i] {
            out.push(Step::BridgeClosed(ch));
        }
    }
    // The peer. Its frames need the connection they arrive on; it can close
    // its side, and go away, as long as any connection still links us.
    if w.from_peer && !w.peer_fin {
        if w.records_delivered && !w.peer_goodbye && w.peer_cancel != Some(Kind::Fail) {
            out.push(Step::PeerGoodbye);
        }
        if w.goodbye_delivered && !w.peer_ack {
            out.push(Step::PeerAck);
        }
        if w.peer_cancel.is_none() {
            out.push(Step::PeerCancel(Kind::Fail));
            if !w.peer_goodbye {
                out.push(Step::PeerCancel(Kind::Stop));
            }
        }
    }
    if !w.peer_fin && (w.from_peer || w.to_peer) {
        out.push(Step::PeerFin);
    }
    if w.peer_fin && !w.peer_gone {
        out.push(Step::PeerGone);
    }
    // At most one fault.
    if !w.fault {
        if writing && w.to_peer && !w.write_fails {
            out.push(Step::WriteError);
        }
        match cfg.wiring {
            Wiring::SharedSplit => {
                if w.to_peer && !w.writer_dead {
                    out.push(Step::Reset { write_first: true });
                }
                if w.from_peer {
                    out.push(Step::ResetInbound);
                }
            }
            _ if w.to_peer || w.from_peer => {
                out.push(Step::Reset { write_first: true });
                if cfg.wiring == Wiring::Shared && w.reader_up {
                    out.push(Step::Reset { write_first: false });
                }
            }
            _ => {}
        }
    }
    // A local cancellation, and the abort from outside, which comes with the
    // token cancelled. Last, so that of equally short traces the search finds
    // one without them first.
    if w.token.is_none() {
        out.extend([Step::LocalCancel(Kind::Stop), Step::LocalCancel(Kind::Fail)]);
    } else if !w.aborted {
        out.push(Step::ExternalAbort);
    }
}

/// A step other than the coordinator's: the machine is only read.
fn take_env(w: &mut World, m: &Mach, step: Step, cfg: Config) {
    match step {
        Step::Finish(success) => {
            w.workers_left -= 1;
            w.workers_failed |= !success;
            // After teardown the result goes to the completion only.
            if !w.finished {
                w.queue.push_back(Ev::Done(success));
            }
            if w.workers_left == 0 && !w.workers_failed {
                w.latch();
            }
        }
        Step::LocalCancel(kind) => w.cancel(kind),
        Step::ExternalAbort => w.aborted = true,
        Step::Deliver => {
            // Ahead of the `Cancel`s it passes, which wait in the control
            // queue: they go next.
            w.locked = w.next_on_lane().map_or(0, |(_, passed)| passed);
            match w
                .outbound
                .take_first(|frame| frame != Out::Cancel)
                .expect("queued")
            {
                Out::Records => w.records_delivered = true,
                Out::Goodbye => w.goodbye_delivered = true,
                Out::Ack => {}
                Out::Cancel => unreachable!("delivered by DeliverCancel"),
            }
        }
        Step::DeliverCancel => {
            w.outbound
                .take_first(|frame| frame == Out::Cancel)
                .expect("queued");
            w.locked = w.locked.saturating_sub(1);
        }
        Step::WriteFails => w.writer_fails(cfg),
        Step::DropQueues { forwarder_holds } => {
            w.refusing = true;
            w.unsure = false;
            if forwarder_holds {
                w.forwarder = Forwarder::Refused;
            }
        }
        Step::ForwarderRefused => {
            w.forwarder = Forwarder::Exited;
            // `run_forwarder` stays silent on a retired link.
            if !m.retired {
                w.queue.push_back(Ev::SendFailed);
            }
        }
        Step::Read => {
            let frame = w.wire.pop_front().expect("sent");
            w.hand_on(frame);
        }
        Step::ReadCancel => {
            let frame = w
                .wire
                .take_first(|frame| matches!(frame, In::Cancel(_)))
                .expect("sent");
            w.hand_on(frame);
        }
        Step::ReadPastCancel => {
            let frame = w
                .wire
                .take_first(|frame| !matches!(frame, In::Cancel(_)))
                .expect("sent");
            w.peer_locked = true;
            w.hand_on(frame);
        }
        Step::ReaderEof => {
            w.reader_up = false;
            w.io.push_back(Notice::InboundEnded { clean: true });
        }
        Step::ReadFails => {
            w.read_fails = false;
            w.reader_up = false;
            w.read_failed = true;
            w.io.push_back(Notice::InboundEnded { clean: false });
        }
        Step::LaneEof => {
            w.lane_reader_up = false;
            // `fail_inbound` tells a lane only if it has not failed.
            if !w.writer_dead {
                w.io.push_back(Notice::LaneClosed);
            }
        }
        Step::ForwardNotice => {
            let notice = w.io.pop_front().expect("queued");
            w.queue.push_back(notice.event());
            if let Notice::InboundEnded { .. } = notice {
                w.channels_closed = true;
            }
        }
        Step::Dispatch => {
            let kind = w.ctrl.pop_front().expect("read");
            w.escalate(kind);
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
        Step::Bridge(ch) => match w.prog[usize::from(ch)].pop_front().expect("read") {
            In::EndOfStream(_) => {
                w.end_of_stream[usize::from(ch)] = true;
                w.queue.push_back(Ev::EndOfStream);
            }
            In::Ack => w.queue.push_back(Ev::Ack),
            In::Cancel(_) => unreachable!("on the control channel"),
        },
        Step::BridgeClosed(ch) => {
            w.prog_close_reported[usize::from(ch)] = true;
            w.queue.push_back(Ev::ChannelClosed);
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
        Step::WriteError => {
            w.fault = true;
            w.writer_fails(cfg);
        }
        Step::Reset { write_first } => {
            w.fault = true;
            w.reset(cfg, write_first);
        }
        Step::ResetInbound => {
            w.fault = true;
            w.lose_inbound();
            w.from_peer = false;
            if w.reader_up {
                w.reader_up = false;
                w.io.push_back(Notice::InboundEnded { clean: false });
            }
        }
        Step::TokenSeen | Step::AbortSeen | Step::Apply | Step::GraceEnds => {
            unreachable!("the coordinator's: {step:?}")
        }
    }
}

// ---------------------------------------------------------------------------
// The machine at work
// ---------------------------------------------------------------------------

/// What the checks saw during a step and, when narrating a trace, what the
/// machine did. Its counts are the premises' witnesses.
#[derive(Default)]
struct Observer {
    narrate: bool,
    notes: Vec<String>,
    /// NoFalseSettle broke in this step.
    false_settle: Option<&'static str>,
    /// Failed sends settled as a failure, and settled excused.
    settled_failed: u64,
    settled_excused: u64,
    /// Grace periods that ended with a send owed, and of those, with the
    /// peer's goodbye in and its link fully closed.
    grace_owed: u64,
    grace_finished_peer: u64,
}

impl Observer {
    fn note(&mut self, note: impl FnOnce() -> String) {
        if self.narrate {
            self.notes.push(note());
        }
    }
}

/// Which losses the machine holds: recorded, pending and deferred.
fn losses_held(m: &LifecycleMachine) -> [bool; 3] {
    [
        m.data_loss.is_some(),
        m.pending_loss.is_some(),
        m.deferred_loss.is_some(),
    ]
}

/// A link failure, or a loss recorded or held that was not before.
fn settled_as_failure(m: &LifecycleMachine, before: [bool; 3], actions: &[Action]) -> bool {
    losses_held(m)
        .into_iter()
        .zip(before)
        .any(|(now, was)| now && !was)
        || actions
            .iter()
            .any(|action| matches!(action, Action::LinkFailed { .. }))
}

/// The losses the machine took up since `before`, for a trace.
fn losses_taken(m: &LifecycleMachine, before: [bool; 3]) -> String {
    let mut text = String::new();
    for ((now, was), what) in losses_held(m).into_iter().zip(before).zip([
        "records a data loss",
        "holds a loss pending (the dataflow is being cancelled)",
        "defers a loss to the next worker's result",
    ]) {
        if now && !was {
            let _ = write!(text, " and {what}");
        }
    }
    text
}

fn actions_text(actions: &[Action]) -> String {
    let names: Vec<String> = actions
        .iter()
        .map(|action| match action {
            Action::SendAck(_) => "SendAck".into(),
            Action::BroadcastCancel => "BroadcastCancel".into(),
            Action::SendGoodbye => "SendGoodbye".into(),
            Action::LinkFailed { failure, .. } => {
                format!("LinkFailed({:?})", Failure::of(failure))
            }
            Action::ArmSoftFailureTimer(_) => "ArmSoftFailureTimer".into(),
            Action::CancelDataflow(_) => "CancelDataflow".into(),
            Action::AbortDrain(_) => "AbortDrain".into(),
            Action::AnnounceUnfinished => "AnnounceUnfinished".into(),
            Action::Teardown { graceful } => format!("Teardown {{ graceful: {graceful} }}"),
        })
        .collect();
    if names.is_empty() {
        "no action".into()
    } else {
        names.join(", ")
    }
}

/// An event reaches the machine. Checks NoFalseSettle.
fn machine_applies(
    w: &World,
    m: &mut LifecycleMachine,
    ev: Ev,
    seen: &mut Observer,
) -> Vec<Action> {
    let owed = m.peers[PEER].unsent();
    let before = losses_held(m);
    let actions = m.on_event(ev.event());
    // A send owed before, or one this event reports, that is no longer owed.
    if (owed || ev == Ev::SendFailed) && !m.peers[PEER].unsent() {
        if settled_as_failure(m, before, &actions) {
            seen.settled_failed += 1;
            if w.excuse_on_its_way() {
                seen.false_settle = Some(
                    "the failed send was settled as a failure while the peer's Cancel, goodbye \
                     or ack was on its way to the coordinator",
                );
            }
        } else if owed {
            seen.settled_excused += 1;
        }
    }
    seen.note(|| {
        format!(
            "applies {ev}: {}{}",
            actions_text(&actions),
            losses_taken(m, before)
        )
    });
    actions
}

/// The grace period of a soft failure ends. Checks NoFalseSettle.
fn grace_expires(m: &mut LifecycleMachine, seen: &mut Observer) -> Vec<Action> {
    let peer = &m.peers[PEER];
    let owed = peer.unsent();
    let finished_peer = owed && peer.retired() && peer.fully_closed();
    let before = losses_held(m);
    let actions = m.on_soft_failure_expired(PEER.to_string());
    if owed {
        seen.grace_owed += 1;
        seen.grace_finished_peer += u64::from(finished_peer);
    }
    if finished_peer && settled_as_failure(m, before, &actions) {
        seen.false_settle = Some(
            "the grace period settled the failed send as a failure although the peer had said \
             goodbye and its link had closed fully",
        );
    }
    seen.note(|| {
        format!(
            "on_soft_failure_expired: {}{}",
            actions_text(&actions),
            losses_taken(m, before)
        )
    });
    actions
}

/// Today's driver glue: what `ClusterLifecycle::run` does around the
/// machine. Everything outside this module is the environment, which another
/// driver keeps.
mod driver {
    use super::*;

    /// The coordinator's next step, if it has one. `run`'s biased `select!`
    /// takes the dataflow token (once), then the bridges' cancellation, then
    /// the next event, then the grace timer.
    pub(super) fn next(w: &World, cfg: Config) -> Option<Step> {
        if w.token.is_some() && !w.cancel_seen {
            Some(Step::TokenSeen)
        } else if w.aborted {
            Some(Step::AbortSeen)
        } else if !w.queue.is_empty() {
            Some(Step::Apply)
        } else if w.timer && !w.busy(cfg) {
            Some(Step::GraceEnds)
        } else {
            None
        }
    }

    /// Take the coordinator's step.
    pub(super) fn take(
        w: &mut World,
        m: &mut LifecycleMachine,
        step: Step,
        cfg: Config,
        seen: &mut Observer,
    ) {
        let actions = match step {
            // Worker results already queued are applied first (and so is
            // everything else queued), then the cancellation, which counts
            // as a failure if its first reason is one.
            Step::TokenSeen => {
                w.cancel_seen = true;
                let mut actions = Vec::new();
                while let Some(ev) = w.queue.pop_front() {
                    actions.extend(machine_applies(w, m, ev, seen));
                }
                let failure = w.token == Some(Kind::Fail);
                let cancelled = m.on_dataflow_cancelled(failure);
                seen.note(|| {
                    format!(
                        "on_dataflow_cancelled({failure}): {}",
                        actions_text(&cancelled)
                    )
                });
                actions.extend(cancelled);
                actions
            }
            // At once, ahead of what is queued.
            Step::AbortSeen => {
                let actions = m.on_external_abort();
                seen.note(|| format!("on_external_abort: {}", actions_text(&actions)));
                actions
            }
            // A worker's result goes straight to the machine
            // (`ClusterLifecycle::apply`), like every other event here.
            Step::Apply => {
                let ev = w.queue.pop_front().expect("queued");
                machine_applies(w, m, ev, seen)
            }
            // The deadline of the first soft failure armed; it stays armed
            // after the failure resolves.
            Step::GraceEnds => {
                w.timer = false;
                grace_expires(m, seen)
            }
            _ => unreachable!("not the coordinator's: {step:?}"),
        };
        perform(w, actions, cfg);
    }

    /// Take the machine's actions as `ClusterLifecycle::perform` does, up to
    /// teardown.
    fn perform(w: &mut World, actions: Vec<Action>, cfg: Config) {
        for action in actions {
            match action {
                Action::SendAck(_) => w.send(Out::Ack, cfg),
                Action::SendGoodbye => w.send(Out::Goodbye, cfg),
                Action::BroadcastCancel => {
                    w.failure_announced |= w.broadcast_is_failure();
                    w.send_cancel();
                }
                // Sent at most once, and only if no `Cancel` so far told the
                // peers about a failure.
                Action::AnnounceUnfinished => {
                    if !w.failure_announced && !w.unfinished_announced {
                        w.unfinished_announced = true;
                        w.send_cancel();
                    }
                }
                // `escalate`, with a failure.
                Action::LinkFailed { .. } | Action::CancelDataflow(_) | Action::AbortDrain(_) => {
                    w.escalate(Kind::Fail);
                }
                // A deadline already armed stays.
                Action::ArmSoftFailureTimer(_) => w.timer = true,
                Action::Teardown { .. } => {
                    w.finished = true;
                    return;
                }
            }
        }
    }
}

/// Take one step from `(w, m)`.
fn take(
    w: &World,
    m: Mach,
    step: Step,
    cfg: Config,
    losses: &mut Losses,
    seen: &mut Observer,
) -> (World, Mach) {
    let mut w = *w;
    if !step.coordinator() {
        take_env(&mut w, &m, step, cfg);
        return (w, m);
    }
    let mut machine = m.machine(losses);
    driver::take(&mut w, &mut machine, step, cfg, seen);
    let m = Mach::of(&machine, losses);
    assert_eq!(
        w.finished,
        m.phase == Phase::Finished,
        "torn down exactly when the machine finished"
    );
    (w, m)
}

// ---------------------------------------------------------------------------
// The search
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Property {
    Termination,
    NoLossWithoutFailedSend,
    NoFalseSettle,
    NoOkAfterFailedSend,
    NoOkOverKnownLoss,
}

impl Property {
    /// Must hold. The others are pinned by [`KNOWN_FAILURES`].
    fn asserted(self) -> bool {
        matches!(self, Self::Termination | Self::NoLossWithoutFailedSend)
    }
}

/// How a property broke (one of its clauses), and a shortest trace from the
/// start to it.
struct Finding {
    property: Property,
    what: &'static str,
    trace: Vec<Step>,
}

/// What a search counted, the premises' witnesses among it.
#[derive(Default)]
struct Stats {
    states: u64,
    steps: u64,
    depth: u32,
    widest_level: usize,
    runs: u64,
    ok_runs: u64,
    loss_runs: u64,
    ok_after_failed_send: u64,
    ok_over_known_loss: u64,
    /// States where nothing more is bound to happen.
    quiescent: u64,
    /// States where the peer has gone and the machine still runs.
    peer_gone_running: u64,
}

struct Search {
    losses: Losses,
    visited: HashSet<Key, BuildHasherDefault<KeyHasher>>,
    /// Each state's predecessor and the step from it, by the index the state
    /// was first reached under. The start is index 0.
    trail: Vec<(u32, Step)>,
    stats: Stats,
    seen: Observer,
    /// The first finding of each way to break a property.
    found: Vec<Finding>,
    violations: BTreeMap<Property, Violations>,
    /// The first new states whose packing, and machine encoding, are checked
    /// both ways.
    round_trips: u32,
}

/// Every violation of a property one search found: how many (states, or for
/// NoFalseSettle steps), and the set of their identities
/// ([`World::identity`], with the clause broken).
#[derive(Default)]
struct Violations {
    count: u64,
    identities: HashSet<u64>,
}

impl Violations {
    /// A digest of the set of identities, whatever their order.
    fn digest(&self) -> u64 {
        self.identities
            .iter()
            .fold(0, |digest: u64, &identity| digest.wrapping_add(identity))
    }
}

/// FNV-1a: a clause's text, as part of an identity.
fn text_hash(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

impl Search {
    fn trace(&self, mut at: u32, last: Option<Step>) -> Vec<Step> {
        let mut trace: Vec<Step> = last.into_iter().collect();
        while at != 0 {
            let (from, step) = self.trail[at as usize];
            trace.push(step);
            at = from;
        }
        trace.reverse();
        trace
    }

    /// Count a violation, found in `w` (or on a step from it). Keep the first
    /// finding of each way to break a property: breadth first, a shortest
    /// one.
    fn record(
        &mut self,
        property: Property,
        what: &'static str,
        w: &World,
        at: u32,
        last: Option<Step>,
    ) {
        let violations = self.violations.entry(property).or_default();
        violations.count += 1;
        violations
            .identities
            .insert(mix([w.identity(), text_hash(what), 0]));
        if !self.found.iter().any(|finding| finding.what == what) {
            let trace = self.trace(at, last);
            self.found.push(Finding {
                property,
                what,
                trace,
            });
        }
    }

    fn check_state(&mut self, w: &World, m: &Mach, enabled: &[Step], at: u32) {
        if w.peer_has_gone() && !w.finished {
            self.stats.peer_gone_running += 1;
        }
        if !enabled.iter().any(|step| step.bound_to_happen()) {
            self.stats.quiescent += 1;
            if !w.finished {
                self.record(
                    Property::Termination,
                    "nothing more is bound to happen, yet the machine has not finished",
                    w,
                    at,
                    None,
                );
            }
        }
        if w.finished && w.workers_left == 0 {
            self.judge(w, m, at);
        }
    }

    /// The run's result, as the completion makes it. What excuses an `Ok` is
    /// what the peer did, whether or not it reached our machine: it retired
    /// the link (it said goodbye, or acked ours: it had our records) or it
    /// cancelled (it needs nothing from us).
    fn judge(&mut self, w: &World, m: &Mach, at: u32) {
        self.stats.runs += 1;
        let loss = m.data_loss != 0;
        if loss {
            self.stats.loss_runs += 1;
            if !w.writer_dead {
                self.record(
                    Property::NoLossWithoutFailedSend,
                    "the run reports a data loss although no send to the peer failed",
                    w,
                    at,
                    None,
                );
            }
        }
        if w.workers_failed || loss {
            return;
        }
        self.stats.ok_runs += 1;
        let cancelled = w.peer_cancel.is_some();
        if w.failed_at_finish {
            self.stats.ok_after_failed_send += 1;
            if !(w.peer_goodbye || w.peer_ack || cancelled) {
                self.record(
                    Property::NoOkAfterFailedSend,
                    "the run ends Ok although its send had failed when the workers succeeded, \
                     and the peer neither retired the link nor cancelled",
                    w,
                    at,
                    None,
                );
            }
        }
        if w.lost_at_finish {
            self.stats.ok_over_known_loss += 1;
            if !cancelled {
                self.record(
                    Property::NoOkOverKnownLoss,
                    "the run ends Ok although its records never reached the peer, our side had \
                     seen the connection fail when the workers succeeded, and the peer did not \
                     cancel",
                    w,
                    at,
                    None,
                );
            }
        }
    }

    /// Roughly the bytes the search's tables hold.
    fn memory(&self, level: &[(Key, u32)], next: &[(Key, u32)]) -> usize {
        self.visited.capacity() / 7 * 8 * (size_of::<Key>() + 1)
            + self.trail.capacity() * size_of::<(u32, Step)>()
            + (level.len() + next.len()) * size_of::<(Key, u32)>()
    }
}

fn initial(cfg: Config) -> LifecycleMachine {
    LifecycleMachine::new(
        usize::from(cfg.workers),
        [(
            PEER.to_string(),
            usize::from(cfg.channels),
            Arc::new(PeerLinkState::default()),
        )],
    )
}

/// Visit every state reachable from the start, breadth first. Stops early
/// once a property that must hold is broken.
fn explore(cfg: Config) -> Report {
    let started = Instant::now();
    let mut search = Search {
        losses: Losses::default(),
        visited: HashSet::default(),
        trail: vec![(0, Step::Apply)],
        stats: Stats::default(),
        seen: Observer::default(),
        found: Vec::new(),
        violations: BTreeMap::new(),
        round_trips: 1 << 20,
    };
    let start = Key::pack(
        &World::new(cfg),
        &Mach::of(&initial(cfg), &mut search.losses),
    );
    search.visited.insert(start);
    let mut level = vec![(start, 0_u32)];
    let mut next = Vec::new();
    let mut enabled = Vec::new();
    let mut memory = 0;
    while !level.is_empty() {
        search.stats.widest_level = search.stats.widest_level.max(level.len());
        for &(key, at) in &level {
            let (w, m) = key.unpack();
            search.stats.states += 1;
            steps(&w, cfg, &mut enabled);
            search.check_state(&w, &m, &enabled, at);
            for &step in &enabled {
                search.stats.steps += 1;
                let (w2, m2) = take(&w, m, step, cfg, &mut search.losses, &mut search.seen);
                if let Some(what) = search.seen.false_settle.take() {
                    search.record(Property::NoFalseSettle, what, &w, at, Some(step));
                }
                let key2 = Key::pack(&w2, &m2);
                assert_ne!(key2, key, "{cfg}: {step:?} changed nothing");
                if !search.visited.insert(key2) {
                    continue;
                }
                if search.round_trips > 0 {
                    search.round_trips -= 1;
                    assert_eq!(key2.unpack(), (w2, m2), "{cfg}: packing lost something");
                    let again = Mach::of(&m2.machine(&search.losses), &mut search.losses);
                    assert_eq!(again, m2, "{cfg}: rebuilding the machine lost something");
                }
                let index = u32::try_from(search.trail.len()).expect("fewer than 2^32 states");
                search.trail.push((at, step));
                next.push((key2, index));
            }
        }
        memory = memory.max(search.memory(&level, &next));
        if search
            .found
            .iter()
            .any(|finding| finding.property.asserted())
        {
            break;
        }
        std::mem::swap(&mut level, &mut next);
        next.clear();
        if !level.is_empty() {
            search.stats.depth += 1;
        }
    }
    Report {
        cfg,
        stats: search.stats,
        seen: search.seen,
        found: search.found,
        violations: search.violations,
        seconds: started.elapsed().as_secs_f64(),
        memory,
    }
}

struct Report {
    cfg: Config,
    stats: Stats,
    seen: Observer,
    found: Vec<Finding>,
    violations: BTreeMap<Property, Violations>,
    seconds: f64,
    /// Roughly the most bytes the search's tables held.
    memory: usize,
}

impl Report {
    fn broken(&self) -> BTreeSet<Property> {
        self.found.iter().map(|finding| finding.property).collect()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (s, o) = (&self.stats, &self.seen);
        writeln!(
            f,
            "{}: {} states, {} steps, depth {}, widest level {}; {:.1} s, about {} MB",
            self.cfg,
            s.states,
            s.steps,
            s.depth,
            s.widest_level,
            self.seconds,
            self.memory >> 20
        )?;
        writeln!(
            f,
            "  {} runs: {} end Ok, {} report a data loss",
            s.runs, s.ok_runs, s.loss_runs
        )?;
        writeln!(
            f,
            "  premises: {} states with nothing bound to happen, {} with the peer gone and the \
             machine running; {} failed sends settled as a failure, {} excused; {} grace periods \
             ended with a send owed, {} of them with the peer finished; {} Ok runs after a failed \
             send, {} over a known loss",
            s.quiescent,
            s.peer_gone_running,
            o.settled_failed,
            o.settled_excused,
            o.grace_owed,
            o.grace_finished_peer,
            s.ok_after_failed_send,
            s.ok_over_known_loss
        )?;
        for (property, violations) in &self.violations {
            writeln!(
                f,
                "  {property:?}: {} violations, {} identities, digest {:016x}",
                violations.count,
                violations.identities.len(),
                violations.digest()
            )?;
        }
        for finding in &self.found {
            writeln!(
                f,
                "  {:?} is broken: {}. A shortest trace, {} steps:",
                finding.property,
                finding.what,
                finding.trace.len()
            )?;
            for line in narrate(self.cfg, &finding.trace) {
                writeln!(f, "    {line}")?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Traces
// ---------------------------------------------------------------------------

/// A trace replayed from the start, one line per step.
fn narrate(cfg: Config, trace: &[Step]) -> Vec<String> {
    let mut losses = Losses::default();
    let mut seen = Observer {
        narrate: true,
        ..Observer::default()
    };
    let mut w = World::new(cfg);
    let mut m = Mach::of(&initial(cfg), &mut losses);
    let mut lines = Vec::new();
    for (i, &step) in trace.iter().enumerate() {
        let before = w;
        (w, m) = take(&before, m, step, cfg, &mut losses, &mut seen);
        let mut line = format!("{:>2}. {}", i + 1, describe(step, &before, &w, cfg));
        for note in seen.notes.drain(..) {
            let _ = write!(line, "; {note}");
        }
        lines.push(line);
    }
    let yes_no = |flag: bool| if flag { "yes" } else { "no" };
    let workers = if w.workers_failed {
        "failed"
    } else if w.workers_left > 0 {
        "running"
    } else {
        "succeeded"
    };
    lines.push(format!(
        "Then: the machine is {:?}, {}; workers {workers}. The peer said goodbye: {}, acked \
         ours: {}, cancelled: {}; the machine retired the link: {}, applied a Cancel: {}. Our \
         send failed: {}; our records reached the peer: {}.",
        m.phase,
        if m.data_loss != 0 {
            "with a data loss recorded"
        } else {
            "with no data loss"
        },
        yes_no(w.peer_goodbye),
        yes_no(w.peer_ack),
        yes_no(w.peer_cancel.is_some()),
        yes_no(m.goodbye_received || m.ack_received),
        yes_no(m.cancel_received),
        yes_no(w.writer_dead),
        yes_no(w.records_delivered),
    ));
    lines
}

fn describe(step: Step, before: &World, after: &World, cfg: Config) -> String {
    match step {
        Step::TokenSeen => format!(
            "the coordinator sees the dataflow token cancelled ({}) and applies what is queued \
             first",
            before.token.expect("cancelled")
        ),
        Step::AbortSeen => "the coordinator sees the bridges cancelled from outside".into(),
        Step::Apply => "the coordinator takes the next event".into(),
        Step::GraceEnds => "the grace period ends".into(),
        Step::Finish(success) => format!(
            "a worker {}{}",
            if success { "succeeds" } else { "fails" },
            if before.finished {
                " after teardown"
            } else {
                ""
            }
        ),
        Step::LocalCancel(kind) => format!("the dataflow is cancelled locally ({kind})"),
        Step::ExternalAbort => {
            "the bridges are cancelled from outside (a participant reported leaving)".into()
        }
        Step::Deliver => {
            let (frame, passed) = before.next_on_lane().expect("queued");
            format!(
                "the peer receives our {}{}",
                match frame {
                    Out::Records => "records",
                    Out::Goodbye => "goodbye",
                    Out::Ack => "ack",
                    Out::Cancel => unreachable!("not on the lane"),
                },
                if passed > 0 {
                    ", ahead of our Cancel queued before it: the transport had found no control \
                     frame and was taking the lane's next one when the Cancel was queued"
                } else {
                    ""
                }
            )
        }
        Step::DeliverCancel => {
            if before.outbound.front() == Some(Out::Cancel) {
                "the peer receives our Cancel".into()
            } else {
                "the peer receives our Cancel, ahead of lane frames queued before it".into()
            }
        }
        Step::WriteFails => "writing to the peer fails".into(),
        Step::DropQueues { forwarder_holds } => format!(
            "the failed writer drops its queues; the outbox forwarder {}",
            if forwarder_holds {
                "still holds a frame"
            } else {
                "holds none"
            }
        ),
        Step::ForwarderRefused => format!(
            "the transport refuses the outbox forwarder's frame{}",
            if after.queue.len() > before.queue.len() {
                ": it reports OutboundFailed(SendFailed)"
            } else {
                ": the link has retired, so it says nothing"
            }
        ),
        Step::Read => format!(
            "the reader hands on the peer's {}",
            before.wire.front().expect("sent")
        ),
        Step::ReadCancel => format!(
            "the reader hands on the peer's {}, which its transport wrote ahead of its {}",
            before
                .wire
                .iter()
                .find(|frame| matches!(frame, In::Cancel(_)))
                .expect("sent"),
            before.wire.front().expect("sent")
        ),
        Step::ReadPastCancel => format!(
            "the reader hands on the peer's {}, which its transport wrote ahead of its {} queued \
             before it: it had found no control frame and was taking the lane's next one when \
             the Cancel was queued",
            before
                .wire
                .iter()
                .find(|frame| !matches!(frame, In::Cancel(_)))
                .expect("sent"),
            before.wire.front().expect("sent")
        ),
        Step::ReaderEof => "the reader sees the peer's close".into(),
        Step::ReadFails => "reading fails".into(),
        Step::LaneEof => "the reader of our lane's connection sees the peer's close".into(),
        Step::ForwardNotice => {
            let notice = before.io.front().expect("queued");
            format!(
                "the io forwarder reports {notice} as {}{}",
                notice.event(),
                if after.channels_closed && !before.channels_closed {
                    "; the peer's channels close"
                } else {
                    ""
                }
            )
        }
        Step::Dispatch => format!(
            "the control dispatcher takes the peer's Cancel ({}) and escalates the token",
            before.ctrl.front().expect("read")
        ),
        Step::DispatchReport => "the control dispatcher reports CancelReceived".into(),
        Step::DispatchClosed => {
            "the control channel has closed: the dispatcher reports ControlClosed".into()
        }
        Step::Bridge(ch) => format!(
            "progress bridge {ch} reports {}",
            after.queue.iter().last().expect("reported")
        ),
        Step::BridgeClosed(ch) => format!(
            "progress channel {ch} has closed without an end-of-stream: its bridge reports \
             ProgressChannelEnded"
        ),
        Step::PeerGoodbye => "the peer says goodbye (an end-of-stream on each channel)".into(),
        Step::PeerAck => "the peer acks our goodbye".into(),
        Step::PeerCancel(kind) => format!("the peer cancels ({kind})"),
        Step::PeerFin => "the peer closes its side of the connection".into(),
        Step::PeerGone => "the peer goes away: our next write fails".into(),
        Step::WriteError => "fault: a write error".into(),
        Step::Reset { write_first } => match cfg.wiring {
            Wiring::SharedSplit => "fault: the connection our lane is pinned to is reset".into(),
            Wiring::Shared if before.reader_up => format!(
                "fault: the connection is reset; the failed read fails the lane, and {} is \
                 reported first",
                if write_first {
                    "WriteFailed"
                } else {
                    "ReadFailed"
                }
            ),
            _ => "fault: the connection is reset".into(),
        },
        Step::ResetInbound => "fault: the connection the peer's frames arrive on is reset".into(),
    }
}

// ---------------------------------------------------------------------------
// Known failures and the tests
// ---------------------------------------------------------------------------

/// A property main breaks, in which wirings, and why, in one line.
struct Known {
    property: Property,
    wirings: &'static [Wiring],
    why: &'static str,
}

/// What main breaks, and why. A test fails unless the properties it finds
/// broken are exactly those listed for its wiring, so a fix has to update
/// this list (and the pins, see [`GOLDEN`]).
const KNOWN_FAILURES: &[Known] = &[
    Known {
        property: Property::NoFalseSettle,
        wirings: &[Wiring::Shared, Wiring::SharedSplit],
        why: "a send fails and the peer's end-of-stream is applied after its ControlClosed: \
              nothing settles the send then (only ControlClosed or a link failure does), so the \
              grace period holds a loss although the peer said goodbye and its link closed",
    },
    Known {
        property: Property::NoFalseSettle,
        wirings: &[Wiring::Dedicated],
        why: "while draining, the forwarder's refusal of our goodbye is a second SendFailed, \
              which settles the first at once as a loss although the peer's Cancel is on its \
              way; the grace period also holds a loss after the peer's goodbye, as in Shared mode",
    },
    Known {
        property: Property::NoOkAfterFailedSend,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        why: "a write fails before the last worker succeeds, but its SendFailed is applied after \
              that success: draining takes it for the goodbye's failure, marks the link lost and \
              ends Ok",
    },
    Known {
        property: Property::NoOkOverKnownLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        why: "as for NoOkAfterFailedSend: the records were still queued when the write failed, so \
              they never reached the peer, and the run ends Ok",
    },
];

/// The pins: for each configuration and each property it breaks, a line
/// `configuration | property | violations | identities | digest | trace`:
/// how many violations the search found (states; steps for NoFalseSettle),
/// how many distinct identities they have ([`World::identity`]), a digest of
/// that set, and the shortest trace the search finds first. A test fails
/// unless it finds exactly its lines, so any change to a counterexample shows.
/// A fix lowers the counts; until the golden file lists identities (to check
/// that a change only removes violations), a lower count can still hide a new
/// counterexample. Read when a test runs, so a pin is updated without a
/// rebuild.
const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/runtime/cluster_lifecycle/exhaustive.golden"
);

/// A configuration's lines for [`GOLDEN`], from what its search found.
fn golden_lines(report: &Report) -> Vec<String> {
    let name = report.cfg.name();
    report
        .violations
        .iter()
        .map(|(property, violations)| {
            let finding = report
                .found
                .iter()
                .find(|finding| finding.property == *property)
                .expect("a finding for each violated property");
            let trace: Vec<String> = finding
                .trace
                .iter()
                .map(|step| format!("{step:?}"))
                .collect();
            format!(
                "{name} | {property:?} | {} | {} | {:016x} | {}",
                violations.count,
                violations.identities.len(),
                violations.digest(),
                trace.join(", ")
            )
        })
        .collect()
}

/// The pinned lines of the configuration called `name`.
fn pinned_lines(name: &str) -> Vec<String> {
    let golden = std::fs::read_to_string(GOLDEN)
        .unwrap_or_else(|error| panic!("cannot read {GOLDEN}: {error}"));
    golden
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .filter(|line| line.split('|').next().map(str::trim) == Some(name))
        .map(String::from)
        .collect()
}

/// Each property's violations and identities, from lines of [`GOLDEN`].
fn pin_counts<'a>(lines: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, (u64, u64)> {
    lines
        .into_iter()
        .filter_map(|line| {
            let columns: Vec<&str> = line.split('|').map(str::trim).collect();
            let count = |i: usize| columns.get(i)?.parse::<u64>().ok();
            Some((columns.get(1)?.to_string(), (count(2)?, count(3)?)))
        })
        .collect()
}

/// How each property's violations moved from its pins, for a failure.
fn pin_changes(pinned: &[String], found: &[String]) -> Vec<String> {
    let pinned = pin_counts(pinned.iter().map(String::as_str));
    let found = pin_counts(found.iter().map(String::as_str));
    let properties: BTreeSet<&String> = pinned.keys().chain(found.keys()).collect();
    properties
        .into_iter()
        .map(|property| match (pinned.get(property), found.get(property)) {
            (Some(pin), Some(now)) if now.0 > pin.0 || now.1 > pin.1 => {
                format!("{property}: more violations or identities than pinned: a new counterexample")
            }
            (Some(pin), Some(now)) if now == pin => {
                format!("{property}: as many as pinned, but another digest or shortest trace")
            }
            (Some(_), Some(_)) => format!("{property}: fewer violations than pinned"),
            (Some(_), None) => format!("{property}: no longer broken"),
            (None, _) => format!("{property}: newly broken"),
        })
        .collect()
}

fn check(wiring: Wiring, workers: u8, channels: u8) {
    for known in KNOWN_FAILURES {
        assert!(
            !known.property.asserted() && !known.wirings.is_empty() && !known.why.is_empty(),
            "{:?}: a known failure needs a pinned property, wirings and a reason",
            known.property
        );
    }
    let cfg = Config {
        wiring,
        workers,
        channels,
    };
    let report = explore(cfg);
    println!("{report}");
    let broken = report.broken();
    for property in [Property::Termination, Property::NoLossWithoutFailedSend] {
        assert!(
            !broken.contains(&property),
            "{cfg}: {property:?} is broken (see the trace above)"
        );
    }
    let listed: BTreeSet<Property> = KNOWN_FAILURES
        .iter()
        .filter(|known| known.wirings.contains(&wiring))
        .map(|known| known.property)
        .collect();
    assert_eq!(
        broken, listed,
        "{cfg}: the properties broken (left) are not those KNOWN_FAILURES lists (right): update \
         the list"
    );
    let (s, o) = (&report.stats, &report.seen);
    assert!(s.ok_runs > 0, "{cfg}: no run ends Ok");
    assert!(
        s.quiescent > 0 && s.peer_gone_running > 0,
        "{cfg}: Termination is vacuous: the peer never goes while the machine runs"
    );
    assert!(
        s.loss_runs > 0,
        "{cfg}: NoLossWithoutFailedSend is vacuous: no run reports a data loss"
    );
    assert!(
        o.settled_failed > 0,
        "{cfg}: NoFalseSettle is vacuous: no failed send is settled as a failure"
    );
    assert!(
        s.ok_after_failed_send > 0,
        "{cfg}: NoOkAfterFailedSend is vacuous: no run ends Ok after a failed send"
    );
    assert!(
        s.ok_over_known_loss > 0,
        "{cfg}: NoOkOverKnownLoss is vacuous: no run ends Ok over a known loss"
    );
    let pinned = pinned_lines(&cfg.name());
    let found = golden_lines(&report);
    assert!(
        pinned == found,
        "{cfg}: the violations differ from the pins in {GOLDEN}:\n{}\npinned:\n{}\n\
         found:\n{}\nIf the change is meant, replace the configuration's lines with the ones \
         found.",
        pin_changes(&pinned, &found).join("\n"),
        pinned.join("\n"),
        found.join("\n")
    );
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_one_worker_one_channel() {
    check(Wiring::Shared, 1, 1);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_one_worker_two_channels() {
    check(Wiring::Shared, 1, 2);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_two_workers_one_channel() {
    check(Wiring::Shared, 2, 1);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_two_workers_two_channels() {
    check(Wiring::Shared, 2, 2);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_split_one_worker_one_channel() {
    check(Wiring::SharedSplit, 1, 1);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn shared_split_one_worker_two_channels() {
    check(Wiring::SharedSplit, 1, 2);
}

#[test]
#[ignore = "abstract and large (67 million states, about 6 GB): CI skips it; run it by hand in release (see the module docs)"]
fn shared_split_two_workers_one_channel() {
    check(Wiring::SharedSplit, 2, 1);
}

#[test]
#[ignore = "too large for CI (308 million states, about 9 minutes and 23 GB): run it by hand in release (see the module docs)"]
fn shared_split_two_workers_two_channels() {
    check(Wiring::SharedSplit, 2, 2);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn dedicated_one_worker_one_channel() {
    check(Wiring::Dedicated, 1, 1);
}

#[test]
#[ignore = "slow in a debug build: CI runs it in release (conformance job)"]
fn dedicated_one_worker_two_channels() {
    check(Wiring::Dedicated, 1, 2);
}

#[test]
#[ignore = "abstract and large (53 million states, about 3 GB): CI skips it; run it by hand in release (see the module docs)"]
fn dedicated_two_workers_one_channel() {
    check(Wiring::Dedicated, 2, 1);
}

#[test]
#[ignore = "too large for CI (223 million states, about 7 minutes and 12 GB): run it by hand in release (see the module docs)"]
fn dedicated_two_workers_two_channels() {
    check(Wiring::Dedicated, 2, 2);
}
