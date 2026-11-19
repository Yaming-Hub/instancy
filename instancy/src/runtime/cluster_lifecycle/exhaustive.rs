//! The lifecycle coordinator checked exhaustively: the real [`Coordinator`]
//! (the [`LifecycleMachine`] and the decisions made around it), driven
//! through every order in which events can reach it, for one peer, within
//! small bounds.
//!
//! Only the coordinator's environment is written here: the producers that
//! feed the coordinator's event channel and how their sends interleave, the
//! driver's scheduling around its decisions (the [`driver`] module: its
//! biased `select!`, its awaits, its timers and teardown's phases, as
//! `ClusterLifecycle::run` and `teardown` perform them), the transport in
//! each mode, the peer and our workers. The coordinator is called through
//! the [`Decide`] interface. Every state reachable from the start is visited
//! once, breadth first. A state is deduplicated on an exact copy of it (every
//! field of the machine included, see [`Key`]), so nothing is pruned by a
//! hash collision, and whatever is found comes with a shortest trace that
//! leads to it.
//!
//! On every step, the decisions the driver made before [`Coordinator`]
//! existed (the [`glue`] module, a copy of them) take the same step from the
//! same state, with the token changing the same way, and must read the
//! token in the same order, take the same effects, publish the same result
//! and lead to the same state: an equivalence oracle ([`oracle`]). With
//! two peers, a test does the same on runs written out and on random ones,
//! with the token still between the reads of one decision ([`two_peers`]).
//!
//! # Properties
//!
//! 1. **Termination**: once the peer has gone, the machine always finishes.
//! 2. **NoLossWithoutCause**: a run reports a data loss only if a send to the
//!    peer failed (the transport failed our writer or lane), records
//!    addressed to us were lost on their way in, or corrupt traffic reached
//!    us.
//! 3. **NoFalseSettle**: a failed send is never settled as a failure (a link
//!    failure, or the loss of its frames recorded or held) while the peer's
//!    `Cancel`, goodbye or ack is still on its way to the coordinator; nor by
//!    the grace period with one already queued for the coordinator, or once
//!    the peer has said goodbye and the link has closed fully. Corrupt
//!    traffic, and another loss, are failures of their own.
//! 4. **NoOkAfterFailedSend**: a run whose send had already failed when its
//!    workers succeeded ends `Ok` only if the peer retired the link or
//!    cancelled.
//! 5. **NoOkOverKnownLoss**: a run whose records never reached the peer ends
//!    `Ok` only if the peer cancelled, provided our side had already seen the
//!    connection that carries them fail (a write error, or a failed read)
//!    when the workers succeeded.
//! 6. **NoOkOverInboundLoss**: a run never ends `Ok` if records addressed to
//!    us were lost on their way in (they reached an operator that had
//!    finished), or corrupt traffic reached us, before teardown began.
//!
//! Termination is checked as the absence of dead ends: states where the
//! machine has not finished and nothing more is bound to happen
//! ([`Step::bound_to_happen`]). Faults, cancellations, the abort from outside,
//! the peer's goodbye, ack and `Cancel`, and a write that is not ours (see
//! "The transport") may never happen; everything else happens once it can,
//! and the peer always closes its side and goes away in the end. No run
//! loops: every step consumes something queued, moves it one stage further
//! (the wire, a channel, the dispatcher, the coordinator's channel), or
//! flips a one-way flag, and whatever is queued was produced by a one-way
//! change (the peer's frames, a fault, a worker's result, our frames and the
//! grace timer by the machine's one-way decisions, never by the timer's own
//! expiry). A step that changes nothing fails the check.
//!
//! A run ends once teardown has published its result and every worker has
//! finished. Its result is what the completion makes of it
//! (`merge_data_loss`): `Ok` if every worker succeeded and teardown published
//! no data loss. For (2) a recorded loss counts whatever the workers'
//! result. For (4) and (5):
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
//! - From teardown on, nothing that happens on the network reaches the
//!   coordinator. Teardown begins while a worker still runs only when the
//!   bridges were cancelled from outside. Until the last worker has
//!   succeeded, our frames still in the transport can still reach the peer,
//!   or be lost to a write that fails (the peer goes away, or a write
//!   error), and that counts for (4) and (5) as it does before teardown.
//!   With today's driver such a write reaches the same states as one that
//!   fails just before teardown, whose notice the abort branch (taken ahead
//!   of queued events) leaves unapplied, as teardown ignores it. A change
//!   that lets teardown hear the transport's notices would tell the two
//!   apart, and would also have to stop [`World::forget`] dropping them.
//!   Frames the transport drops at teardown without a failure (the outbox
//!   forwarders are aborted, and in Shared mode a per-job manager released
//!   after teardown's bounded flush drops what is still queued on it) are
//!   judged as frames still on their way when the last worker succeeds,
//!   which they may be; with the managers kept, a Shared lane's frames are
//!   still written after it unregisters. Beyond our frames, a
//!   run is judged as one in which the peer does nothing more but go away:
//!   a goodbye, an ack or a `Cancel` after our teardown could only excuse an
//!   `Ok`. What the peer did by then is all it did, and what reached whom
//!   (the ghosts) is all that is kept of the network, which is forgotten.
//!
//! (1) and (2) hold. (3) to (6) do not: [`KNOWN_FAILURES`] says why, one line
//! each, and each configuration's golden file (see [`GOLDEN`]) lists every
//! way it breaks them: how many violations, the set of their identities (the
//! environment's side of a violating state, [`World::identity`]), and a
//! shortest trace. Each test prints a shortest counterexample for every
//! way it finds a property broken, and fails if what it finds and the list
//! or the golden file disagree: an identity the file does not list is a new
//! counterexample, so a fix has to show that it only removes identities.
//! The identity leaves out the coordinator's channel and the driver's state,
//! so it is coarser than a state: a new counterexample whose identity matches
//! one the file lists goes unnoticed.
//!
//! # Configurations
//!
//! A configuration is a wiring, one or two local workers, one or two
//! progress channels, and the family of faults it explores (at most one per
//! run): transport faults (a local write error, or a reset), or inbound
//! faults (records that reach an operator that had finished, a frame for
//! one that does not decode, or a corrupt progress frame from the peer). A
//! run never has a fault of each family, so splitting them over
//! configurations loses nothing.
//!
//! The configurations with two local workers and one progress channel are
//! abstract: the runtime gives each local worker a progress channel from each
//! peer worker (`(peer workers) * (local workers)` end-of-stream markers), so
//! two local workers always get at least two. They exercise two worker
//! results against a one-marker goodbye, at a fraction of the cost of the
//! real two-worker configurations.
//!
//! The peer's second `Cancel` (see "The environment") is explored with one
//! local worker only. What it can change is the token's escalation, which
//! our broadcasts read (the last worker's result is what asks for
//! `AnnounceUnfinished`; an earlier one only records its error, which one
//! worker's failure records too), and the peer's stop, which only closes its
//! progress lanes. A second worker multiplies its interleavings beyond what
//! the largest configurations can hold.
//!
//! # The environment
//!
//! - The coordinator's event channel: one FIFO, fed by producers whose sends
//!   interleave freely, each in its own order:
//!   - the io forwarder: the transport's notices (`PeerIoNotice`), in the
//!     order the transport queued them. The end of a read side is handed on
//!     before the peer's channels close. In Shared mode the transport hands
//!     it on from a task of its own, so a notice it queues after it can
//!     reach the forwarder first (`Step::ForwardAheadOfEnd`). In split
//!     Shared mode that adds orders but no states: the notices behind the
//!     end come from the other connection, whose events interleave freely
//!     anyway;
//!   - the control dispatcher: for each `Cancel`, the token escalated, then
//!     the event, as two steps; then the channel's close;
//!   - one progress bridge per channel: an end-of-stream or an ack, then the
//!     channel's close, reported only when no end-of-stream came; a frame
//!     that does not decode ends the channel before its end-of-stream (the
//!     bridge exits, and the transport drops what still comes for it), and is
//!     reported once (and the bridge goes on) after it. Once the bridges are
//!     cancelled (from outside, or by teardown) they report nothing more:
//!     they take the cancellation ahead of what is queued for them. A bridge
//!     still reports, after the cancellation, a frame it had taken before it,
//!     and every frame it drains once its lane closes (`close_peer`), which
//!     it does without looking at the cancellation. Here such a report comes
//!     before the cancellation instead: the cancellation moves after it, past
//!     the coordinator's steps in between, none of which looks at it but the
//!     abort's own branch, so the report keeps its place among them (one that
//!     comes after the abort's branch comes here just before it, which reads
//!     nothing queued; teardown applies what it finds). The exception is a
//!     report after a token branch that itself began teardown, which applied
//!     what was queued before it: such a report can only tell teardown about
//!     corrupt traffic (teardown ignores the rest), which fails the run, as
//!     corrupt traffic the data bridge reports does here;
//!   - the data bridge: records that reached an operator that had finished
//!     (`InboundDataLost`), or a frame for one that does not decode, reported
//!     when the bridge gets to that frame: at any point after it arrived
//!     (also after the connection's end), or not before teardown, if at all.
//!     It does not stop with the other bridges;
//!   - the outbox forwarder: a frame the transport refused, unless the link
//!     has retired;
//!   - our workers' results: a success, a failure (with its error), or (once
//!     the dataflow is cancelled) a cancellation.
//! - The dataflow token holds its reasons as values: a local stop or failure,
//!   the peer's `Cancel` as the dispatcher escalates the token with it, and
//!   the coordinator's own escalations. It keeps its first reason, and the
//!   first failure escalated after a requested stop. Once no broadcast can
//!   read them again, only whether its first reason is a failure and whether
//!   it was escalated are kept.
//! - The token changes between the coordinator's reads of it: before each
//!   read whose value it could change, the environment may cancel it (a
//!   local stop or failure, or the dispatcher with the peer's `Cancel` it
//!   holds) or escalate it (the dispatcher, with the peer's failure), and the
//!   search explores each choice ([`Script`]). Every read the coordinator
//!   makes goes through a [`TokenReader`], in the driver's order: `on_event`'s
//!   `is_cancelled`, the token branch's `reason`, and
//!   `cancel_broadcast_reason` and `unfinished_reason`, which reads the
//!   escalation twice.
//! - The driver (`ClusterLifecycle::run`), in [`driver`]:
//!   - its biased `select!`: the dataflow token (once), the bridges'
//!     cancellation, the next event, the grace timer. The branches are polled
//!     in turn, so once it has found no event and the grace deadline has
//!     passed it takes the timer's branch, with an event queued meanwhile
//!     (the timer race);
//!   - the token branch applying what is queued first, one event at a time;
//!   - each action prepared just before it is taken. A `Cancel` broadcast is
//!     awaited: the actions after it wait while the environment moves (events
//!     are posted, the token escalated, the transport fails). Our `Cancel`
//!     then is a frame in the transport's control queue like any other;
//!   - after the actions, the progress lanes of the peers that stopped close
//!     (the workers read them; nothing here does);
//!   - teardown: whether it lets the inbound bridges drain (every worker
//!     done), then its bounded waits, during which only the data bridge,
//!     our workers and our frames still in the transport move (see
//!     "Properties"), then the events it finds queued (lost records only
//!     if it drained, corrupt traffic always), the `Cancel` announcing a loss
//!     nobody was told of, and its result. What the network does during the
//!     waits cannot change that: teardown applies only lost records and
//!     corrupt traffic, and here, with the progress bridges cancelled, only
//!     the data bridge reports them.
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
//!     it. A Shared connection's writer writes the frames queued on it as a
//!     batch with one flush (`write_frames`): on a stream that buffers until
//!     it is flushed, a failure can also lose the batch's earlier frames,
//!     the same as a failure at the first frame lost. A write that fails
//!     although its frame went out whole is left out: `write_frame` and
//!     `write_frames` flush after the last byte, and a TCP stream's flush
//!     does nothing (see "Not covered").
//!   - Shared: a write error or a failed read fails the lane at once and for
//!     good. The manager then drops our frames (`send_payload`), so the outbox
//!     forwarder never sees one refused. A write error is reported, then
//!     stops the reader, closing the whole connection; a failed read reports
//!     both, in either order. [`Wiring::SharedSplit`] puts our lane and the
//!     peer's frames on two connections: a failure of one leaves the other,
//!     and the peer closing both also tells our lane.
//!   - Shared: a pooled connection also carries writes that are not ours:
//!     RTT probes (`probe_loop`), replies to the peer's, and the lanes of
//!     other dataflows pinned to it. One of them can fail with nothing of
//!     ours queued, which fails our lane all the same
//!     (`Step::OtherWriteFails`). Once the peer has gone (or after a reset
//!     our reader had seen closed) the next write fails, but nothing has to
//!     make one; before, such a failure is the write-error fault.
//!   - Dedicated: the reader and the writer fail independently. A failed
//!     writer reports, then drops its queues (`bridge_task`), as two steps:
//!     the frame it was writing is lost, one behind it is lost unseen or
//!     refused to the forwarder, and every later one is refused. After a
//!     reset the reader fails, and the writer fails at its next write.
//! - The peer: says goodbye (only once it has our records, which it needs,
//!   and not after cancelling for a failure), acks our goodbye, cancels (a
//!   requested stop, or a failure; no requested stop after its goodbye),
//!   after a requested stop and before any goodbye announces that its part
//!   is unfinished, with its failure, in a second `Cancel` (its
//!   `AnnounceUnfinished`, as its machine finishes: no goodbye or ack after
//!   it; explored with one local worker, see "Configurations"), closes its
//!   side and goes away (while any connection links us), after which our
//!   writes fail. A
//!   second `Cancel` that marks its stop unfinished without a failure is
//!   left out: the token ignores it (it cancels only a token that is not
//!   cancelled, and escalates only with a failure), and the machine does no
//!   more for it than for this one (the peer stopped, so its progress lanes
//!   close).
//! - Our side: workers that succeed or fail at any time, also after a
//!   cancellation or teardown (what they need from the peer travels on
//!   channels the coordinator does not watch, and may have arrived at any
//!   point before); a local cancellation (a requested stop or a failure);
//!   the bridges cancelled from outside, which happens only with the token
//!   cancelled first (`ClusterCancelHandle::cancel_with_reason`).
//! - The grace deadline passes only once nothing is on its way to the
//!   coordinator: the model's timing assumption, which the timer race above
//!   relaxes for the moment the driver takes it.
//!
//! # Not covered
//!
//! Several peers (the two-peer test checks only that the coordinator decides
//! as the glue did). A data channel that ends without its close sentinel, a
//! frame the transport rejects (oversize), an encoding failure, a corrupt
//! control message and corrupt framing (a checksum or sequence error the
//! transport reports): no event here reports them. A write reported failed
//! although its frame went out whole, which only a stream whose flush can
//! fail after sending everything does (not TCP): the peer would have the
//! frame, and our side a `WriteFailed`. A `Cancel` broadcast that times out:
//! it needs a control queue that stays full for the whole bound (a stalled
//! connection), and the driver then counts a `Cancel` that was never queued
//! as sent, which also keeps `AnnounceUnfinished` from sending. A `Cancel`
//! the peer sends after unregistering the dataflow in split Shared mode,
//! once its registration is gone or its lane has failed (until then the
//! registration is closing, and the `Cancel` takes the lane's connection):
//! `send_control` then takes any pooled connection, where it can arrive apart
//! from the peer's other frames, or be dropped after their connection has
//! ended; here the peer's `Cancel` always travels with its lane. In split
//! Shared mode, a write that is not ours failing on the connection the
//! peer's frames arrive on, after the peer has gone and before that
//! connection's reader has ended (its last frames may all have been read,
//! with the close still pending): it stops that reader, so our read side
//! ends with a failure, and the peer's frames not yet read are lost. The
//! reset of that connection (`Step::ResetInbound`) has a similar effect, but
//! it is the run's one fault, and it is not explored with inbound faults:
//! such a failure together with another fault, or with inbound faults, is
//! left out. Modelling it nearly tripled the split configurations with
//! inbound faults. Data pushes
//! (`push_blocked` stays false): a peer that stops unfinished never stops our
//! drain here (`abort_drain_without`); nor does a push report the
//! `SendFailed` of its own that it reports when it finds the transport's
//! queue closed, which comes from a worker that still runs, so ahead of the
//! last worker's result, as a `SendFailed` the transport reports can. Our
//! reader after teardown: a reset then shows only as our next write
//! failing. The peer's own machine. A peer that never goes away (a shared
//! manager whose connections stay up). The control dispatcher stopping on
//! the bridges' cancellation: here it goes on. The event channel closing
//! (`on_events_closed`): it cannot while the machine runs, since each
//! outbox the driver holds owns a sender, and teardown drops them.
//!
//! # Running
//!
//! Each configuration is a test, ignored because a debug build takes too
//! long. CI runs eleven of them in release, and the others are run by hand
//! (see `docs/testing/nextest.md`); `--nocapture` prints each configuration's
//! counts and counterexamples. To run all twenty-one, one at a time (the
//! largest, split Shared mode with two workers and two channels and transport
//! faults, needs about 23 GB and 20 minutes for its 520 million states; the
//! equivalence oracle adds about a quarter to the times here and in the tests'
//! `#[ignore]` reasons):
//!
//! ```text
//! cargo test -p instancy --release --all-features --lib -- --include-ignored \
//!     --nocapture --test-threads 1 cluster_lifecycle::exhaustive::
//! ```
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fmt::{self, Write as _};
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

/// The faults a configuration explores, at most one per run. A run with
/// one fault of each family is never explored, so splitting them over
/// configurations loses nothing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Faults {
    /// A local write error, or a reset.
    Transport,
    /// What the peer sends us is lost on its way in (records that reach an
    /// operator that had finished), or corrupt.
    Inbound,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    wiring: Wiring,
    /// Local workers.
    workers: u8,
    /// Progress channels from the peer: the end-of-stream markers its
    /// goodbye takes.
    channels: u8,
    faults: Faults,
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
        let faults = match self.faults {
            Faults::Transport => "",
            Faults::Inbound => "_inbound",
        };
        format!(
            "{wiring}_{}_{}{faults}",
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
            "{wiring}, {} worker{}, {} channel{}, {} faults",
            self.workers,
            plural(self.workers),
            self.channels,
            plural(self.channels),
            match self.faults {
                Faults::Transport => "transport",
                Faults::Inbound => "inbound",
            }
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

/// The 64-bit words a state packs into.
const WORDS: usize = 4;

/// Bits written, or read, in order.
struct Bits {
    words: [u64; WORDS],
    at: u32,
}

impl Bits {
    fn new(words: [u64; WORDS]) -> Self {
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

/// How a local worker finished.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Ok,
    /// With an error other than a cancellation (`WorkerFinished::failure`).
    Failed,
    /// Cancelled: only once the dataflow token is.
    Cancelled,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ok => "success",
            Self::Failed => "failure",
            Self::Cancelled => "cancelled",
        })
    }
}

/// The error a local worker that fails reports (`WorkerFinished::failure`).
const WORKER_ERROR: &str = "the local worker's error";

/// A cancellation reason, as the code of its value in [`Tables`]. Codes are
/// nonzero.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Why(u8);

impl Why {
    /// A requested stop on this node: `UserRequested`.
    const LOCAL_STOP: Self = Self(1);
    /// A failure on this node: `WorkerFailed`.
    const LOCAL_FAILURE: Self = Self(2);
    /// The peer's `Cancel` for a requested stop, as the control dispatcher
    /// escalates the token with it: `PeerCancelled`.
    const PEER_STOP: Self = Self(3);
    /// The peer's `Cancel` for a failure: `PeerCancelled`, whose detail
    /// describes a failure.
    const PEER_FAILURE: Self = Self(4);

    fn local(kind: Kind) -> Self {
        match kind {
            Kind::Stop => Self::LOCAL_STOP,
            Kind::Fail => Self::LOCAL_FAILURE,
        }
    }

    fn peer(kind: Kind) -> Self {
        match kind {
            Kind::Stop => Self::PEER_STOP,
            Kind::Fail => Self::PEER_FAILURE,
        }
    }
}

impl Code for Why {
    const BITS: u32 = 4;

    fn code(self) -> u64 {
        u64::from(self.0)
    }

    fn from_code(code: u64) -> Self {
        Self(u8::try_from(code).expect("4 bits"))
    }
}

/// The values a state holds by code: the data losses and the cancellation
/// reasons seen so far. A loss is held as 1 + its index here (0 for none), a
/// reason as a [`Why`]. The first reasons are the environment's, so their
/// codes are fixed; the coordinator's own escalations follow as they appear.
struct Tables {
    losses: Vec<DataLoss>,
    reasons: Vec<CancellationReason>,
}

impl Tables {
    fn new() -> Self {
        let peer = |kind: Kind| CancellationReason::PeerCancelled {
            peer_id: PEER.to_string(),
            detail: match kind {
                Kind::Stop => CancellationReason::UserRequested.to_string(),
                Kind::Fail => CancellationReason::WorkerFailed {
                    detail: "the peer's worker failed".into(),
                }
                .to_string(),
            },
        };
        let tables = Self {
            losses: Vec::new(),
            reasons: vec![
                CancellationReason::UserRequested,
                CancellationReason::WorkerFailed {
                    detail: "a local worker failed".into(),
                },
                peer(Kind::Stop),
                peer(Kind::Fail),
            ],
        };
        for (why, failure) in [
            (Why::LOCAL_STOP, false),
            (Why::LOCAL_FAILURE, true),
            (Why::PEER_STOP, false),
            (Why::PEER_FAILURE, true),
        ] {
            assert_eq!(tables.reason(why).is_failure(), failure, "{why:?}");
        }
        tables
    }

    fn loss_code(&mut self, loss: &Option<DataLoss>) -> u8 {
        let Some(loss) = loss else {
            return 0;
        };
        let index = match self.losses.iter().position(|known| known == loss) {
            Some(index) => index,
            None => {
                self.losses.push(loss.clone());
                self.losses.len() - 1
            }
        };
        u8::try_from(index + 1).expect("few losses")
    }

    fn loss(&self, code: u8) -> Option<DataLoss> {
        code.checked_sub(1)
            .map(|index| self.losses[usize::from(index)].clone())
    }

    fn why(&mut self, reason: &CancellationReason) -> Why {
        let index = match self.reasons.iter().position(|known| known == reason) {
            Some(index) => index,
            None => {
                self.reasons.push(reason.clone());
                self.reasons.len() - 1
            }
        };
        let why = Why(u8::try_from(index + 1).expect("few reasons"));
        assert!(
            why.0 < 1 << Why::BITS,
            "more reasons than codes: raise Why::BITS"
        );
        why
    }

    fn reason(&self, why: Why) -> CancellationReason {
        self.reasons[usize::from(why.0 - 1)].clone()
    }

    fn is_failure(&self, why: Why) -> bool {
        self.reasons[usize::from(why.0 - 1)].is_failure()
    }
}

/// An event on the coordinator's channel.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ev {
    /// A worker finished.
    Done(Outcome),
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
    /// The data bridge: records reached an operator that had finished.
    Lost,
    /// The data bridge: a frame for an operator that will never decode it
    /// does not decode.
    DataCorrupt,
    /// The progress bridge of channel 0: a frame that does not decode, before
    /// its end-of-stream (it ends the channel) or after it (out of protocol).
    ProgressCorrupt {
        after_end_of_stream: bool,
    },
}

/// The detail of the data bridge's reports.
const INBOUND_LOSS: &str = "channel 9: records arrived after the receiving operator finished";
const DATA_DECODE: &str = "data channel 9: undecodable frame";
/// The detail of the progress bridge's report of a frame that does not
/// decode, before and after its end-of-stream.
const PROGRESS_DECODE: &str = "progress frame checksum mismatch";
const PROGRESS_VIOLATION: &str = "undecodable frame on progress channel 0 after its end-of-stream: progress frame checksum mismatch";

impl Ev {
    /// The peer's `Cancel`, goodbye or ack: it needs nothing more from us.
    fn excuses(self) -> bool {
        matches!(self, Self::Cancel | Self::EndOfStream | Self::Ack)
    }

    /// Reports corrupt traffic.
    fn corrupt(self) -> bool {
        matches!(self, Self::DataCorrupt | Self::ProgressCorrupt { .. })
    }

    fn event(self) -> LifecycleEvent {
        let link = |event| LifecycleEvent::Link {
            peer: PEER.to_string(),
            event,
        };
        match self {
            Self::Done(outcome) => LifecycleEvent::WorkerFinished {
                success: outcome == Outcome::Ok,
                failure: (outcome == Outcome::Failed).then(|| WORKER_ERROR.to_string()),
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
            Self::Lost => link(LinkEvent::InboundDataLost(INBOUND_LOSS.into())),
            Self::DataCorrupt => link(LinkEvent::DataChannelEnded(LinkFailure::Decode(
                DATA_DECODE.into(),
            ))),
            Self::ProgressCorrupt {
                after_end_of_stream,
            } => link(LinkEvent::ProgressChannelEnded(if after_end_of_stream {
                LinkFailure::ProtocolViolation(PROGRESS_VIOLATION.into())
            } else {
                LinkFailure::Decode(PROGRESS_DECODE.into())
            })),
        }
    }
}

impl Code for Ev {
    const BITS: u32 = 4;

    fn code(self) -> u64 {
        match self {
            Self::Done(Outcome::Ok) => 1,
            Self::Done(Outcome::Failed) => 2,
            Self::Done(Outcome::Cancelled) => 3,
            Self::SendFailed => 4,
            Self::ConnectionEnded { clean: true } => 5,
            Self::ConnectionEnded { clean: false } => 6,
            Self::Cancel => 7,
            Self::ControlClosed => 8,
            Self::EndOfStream => 9,
            Self::Ack => 10,
            Self::ChannelClosed => 11,
            Self::Lost => 12,
            Self::DataCorrupt => 13,
            Self::ProgressCorrupt {
                after_end_of_stream: false,
            } => 14,
            Self::ProgressCorrupt {
                after_end_of_stream: true,
            } => 15,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Done(Outcome::Ok),
            2 => Self::Done(Outcome::Failed),
            3 => Self::Done(Outcome::Cancelled),
            4 => Self::SendFailed,
            5 => Self::ConnectionEnded { clean: true },
            6 => Self::ConnectionEnded { clean: false },
            7 => Self::Cancel,
            8 => Self::ControlClosed,
            9 => Self::EndOfStream,
            10 => Self::Ack,
            11 => Self::ChannelClosed,
            12 => Self::Lost,
            13 => Self::DataCorrupt,
            14 => Self::ProgressCorrupt {
                after_end_of_stream: false,
            },
            15 => Self::ProgressCorrupt {
                after_end_of_stream: true,
            },
            _ => unreachable!("event {code}"),
        }
    }
}

impl fmt::Display for Ev {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Done(Outcome::Ok) => "WorkerFinished(success)",
            Self::Done(Outcome::Failed) => "WorkerFinished(failure)",
            Self::Done(Outcome::Cancelled) => "WorkerFinished(cancelled)",
            Self::SendFailed => "OutboundFailed(SendFailed)",
            Self::ConnectionEnded { clean: true } => "ConnectionFailed(closed)",
            Self::ConnectionEnded { clean: false } => "ConnectionFailed(reset)",
            Self::Cancel => "CancelReceived",
            Self::ControlClosed => "ControlClosed",
            Self::EndOfStream => "ProgressEndOfStream",
            Self::Ack => "AckReceived",
            Self::ChannelClosed => "ProgressChannelEnded(ChannelClosed)",
            Self::Lost => "InboundDataLost",
            Self::DataCorrupt => "DataChannelEnded(Decode)",
            Self::ProgressCorrupt {
                after_end_of_stream: false,
            } => "ProgressChannelEnded(Decode)",
            Self::ProgressCorrupt {
                after_end_of_stream: true,
            } => "ProgressChannelEnded(ProtocolViolation)",
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
    /// A frame on its first progress channel to us that does not decode
    /// (corrupt on the way, say).
    Corrupt,
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
            Self::Corrupt => 6,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Cancel(Kind::Stop),
            2 => Self::Cancel(Kind::Fail),
            3 => Self::EndOfStream(0),
            4 => Self::EndOfStream(1),
            5 => Self::Ack,
            6 => Self::Corrupt,
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
            Self::Corrupt => f.write_str("corrupt frame on channel 0"),
        }
    }
}

/// What the data bridge has to report.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Late {
    /// Records that reached an operator that had finished.
    Lost,
    /// A frame for an operator that had finished, which does not decode.
    Corrupt,
}

impl Late {
    /// What the data bridge reports.
    fn event(self) -> Ev {
        match self {
            Self::Lost => Ev::Lost,
            Self::Corrupt => Ev::DataCorrupt,
        }
    }
}

impl Code for Late {
    const BITS: u32 = 2;

    fn code(self) -> u64 {
        match self {
            Self::Lost => 1,
            Self::Corrupt => 2,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::Lost,
            2 => Self::Corrupt,
            _ => unreachable!("late {code}"),
        }
    }
}

impl fmt::Display for Late {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Lost => "records that reached an operator that had finished",
            Self::Corrupt => "a frame that does not decode, for an operator that had finished",
        })
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

/// Everything but the machine: the coordinator's channel and what the
/// coordinator and the driver remember, our workers, the transport, the peer, and the ghosts the
/// properties need.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct World {
    // The coordinator.
    queue: Fifo<Ev, 12>,
    /// The dataflow token's first reason (`cancel_with_reason` and
    /// `escalate` keep it).
    token: Option<Why>,
    /// The failure escalated on a token cancelled for a requested stop
    /// (`CancellationToken::escalation`; the first one stays).
    escalation: Option<Why>,
    /// The driver has taken the token's branch.
    cancel_seen: bool,
    /// The bridges were cancelled from outside.
    aborted: bool,
    /// A grace deadline is armed.
    timer: bool,
    /// `select!` found no event, and the grace deadline it polls next has
    /// passed: it takes that branch, whatever is queued meanwhile.
    polled: bool,
    /// The actions the driver takes once the `Cancel` broadcast it awaits is
    /// done.
    pending: Fifo<Act, 3>,
    /// Teardown has begun: its bounded waits are not over. Whether it
    /// drains the inbound bridges (`drain_inbound`).
    tearing_down: bool,
    drain_inbound: bool,
    /// Torn down: teardown published its result.
    finished: bool,
    /// The driver's `failure_announced` and `unfinished_announced`.
    failure_announced: bool,
    unfinished_announced: bool,
    /// The driver's `worker_failure` holds a local worker's error.
    worker_failure: bool,
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
    wire: Fifo<In, 5>,
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
    prog: [Fifo<In, 3>; 2],
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
    /// The peer sent a second `Cancel`, with its failure (its
    /// `AnnounceUnfinished`): its workers did not complete after its
    /// requested stop.
    peer_announced: bool,
    peer_fin: bool,
    peer_gone: bool,
    /// The data bridge holds a report for the coordinator.
    late: Option<Late>,
    // Ghosts. Records addressed to us were lost on their way in; corrupt
    // traffic reached us (both before teardown began, as nothing reaches us
    // later).
    inbound_lost: bool,
    corrupt_in: bool,
    /// Corrupt traffic waited for a progress bridge when teardown began:
    /// cancelled, the bridge never reads it.
    corrupt_unheard: bool,
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
            escalation: None,
            cancel_seen: false,
            aborted: false,
            timer: false,
            polled: false,
            pending: Fifo::EMPTY,
            tearing_down: false,
            drain_inbound: false,
            finished: false,
            failure_announced: false,
            unfinished_announced: false,
            worker_failure: false,
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
            peer_announced: false,
            peer_fin: false,
            peer_gone: false,
            late: None,
            inbound_lost: false,
            corrupt_in: false,
            corrupt_unheard: false,
            failed_at_finish: false,
            lost_at_finish: false,
        }
    }

    /// Cancel the dataflow token (`cancel_with_reason`): its first reason
    /// stays.
    fn cancel(&mut self, why: Why) {
        if self.token.is_none() {
            self.token = Some(why);
        }
    }

    /// `escalate`: cancel, and record a failure that follows a requested
    /// stop (the first one stays).
    fn escalate(&mut self, why: Why, tables: &Tables) {
        self.cancel(why);
        let first = self.token.expect("cancelled");
        if tables.is_failure(why) && !tables.is_failure(first) && self.escalation.is_none() {
            self.escalation = Some(why);
        }
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
            In::EndOfStream(ch) => self.queue_for_bridge(usize::from(ch), frame),
            In::Ack => self.queue_for_bridge(0, frame),
            // It reached us.
            In::Corrupt => {
                self.corrupt_in = true;
                self.queue_for_bridge(0, frame);
            }
        }
    }

    /// Queue `frame` for the progress bridge of channel `ch`, unless that
    /// bridge has exited: it reported the end of its channel (a close, or a
    /// frame that does not decode before its end-of-stream). Its receiver
    /// is gone, and the transport drops what it routes there (the demuxer
    /// removes the channel, `shared_transport` ignores the failed send).
    fn queue_for_bridge(&mut self, ch: usize, frame: In) {
        if !self.prog_close_reported[ch] {
            self.prog[ch].push_back(frame);
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
            || self.late.is_some()
    }

    /// The peer's `Cancel`, goodbye or ack is on its way to the coordinator:
    /// it will reach it unless a fault intervenes. A frame that does not
    /// decode excuses nothing, and before its channel's end-of-stream it
    /// ends its bridge, so nothing behind it on that channel comes through.
    /// Once the bridges are cancelled from outside they report nothing more
    /// (see the module docs for a report already under way).
    fn excuse_on_its_way(&self) -> bool {
        if self.queue.iter().any(Ev::excuses)
            || !self.ctrl.is_empty()
            || self.dispatching
            || (self.reader_up && self.wire.iter().any(|frame| matches!(frame, In::Cancel(_))))
        {
            return true;
        }
        if self.aborted {
            return false;
        }
        (0..2).any(|ch| {
            if self.prog_close_reported[ch] {
                return false;
            }
            // In the order the bridge takes them: queued, then on the wire.
            let on_wire = self.wire.iter().filter(move |&frame| {
                self.reader_up
                    && match frame {
                        In::EndOfStream(c) => usize::from(c) == ch,
                        In::Ack | In::Corrupt => ch == 0,
                        In::Cancel(_) => false,
                    }
            });
            for frame in self.prog[ch].iter().chain(on_wire) {
                match frame {
                    In::EndOfStream(_) | In::Ack => return true,
                    In::Corrupt if !self.end_of_stream[ch] => return false,
                    In::Corrupt | In::Cancel(_) => {}
                }
            }
            false
        })
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

    /// Forget what can no longer matter, so that states that differ only in
    /// it are one. `AnnounceUnfinished` sends nothing once a failure was
    /// announced, or it was sent, so its reason (the worker's error) is never
    /// chosen again. From teardown on nothing from the network reaches the
    /// coordinator, and only our frames still in the transport move (see
    /// [`sent_after_teardown`]). They matter while a worker is yet to
    /// succeed and the run can still end `Ok` (no worker failed, and no data
    /// loss is recorded, `loss`): whether our send had failed, and whether
    /// our records had reached the peer, when the last one does. Beyond
    /// them, what the peer did and what reached whom (the ghosts) is all that
    /// counts.
    fn forget(&mut self, loss: bool) {
        if self.failure_announced {
            self.unfinished_announced = false;
        }
        if self.failure_announced || self.unfinished_announced {
            self.worker_failure = false;
        }
        if self.tearing_down || self.finished {
            self.corrupt_unheard |= self
                .prog
                .iter()
                .any(|queue| queue.iter().any(|frame| frame == In::Corrupt));
            self.io = Fifo::EMPTY;
            self.ctrl = Fifo::EMPTY;
            self.dispatching = false;
            self.prog = [Fifo::EMPTY; 2];
            self.lose_inbound();
            self.forwarder = Forwarder::Idle;
            self.refusing = false;
            self.unsure = false;
            self.read_fails = false;
            self.reader_up = false;
            self.lane_reader_up = false;
            self.channels_closed = false;
            self.ctrl_close_reported = false;
            self.end_of_stream = [false; 2];
            self.prog_close_reported = [false; 2];
            self.from_peer = false;
            let unsent = self.writing()
                && (self.to_peer || self.write_fails)
                && self.workers_left > 0
                && !self.workers_failed
                && !loss;
            if !unsent {
                self.lose_outbound();
                self.write_fails = false;
                self.to_peer = false;
            }
        }
        if self.finished {
            self.drain_inbound = false;
            self.cancel_seen = false;
            self.timer = false;
            self.failure_announced = false;
            self.unfinished_announced = false;
            self.worker_failure = false;
        }
    }

    /// Forget the token's reasons down to whether its first is a failure
    /// and whether it was escalated, once no broadcast can read them (see
    /// [`Self::reasons_readable`]).
    fn forget_reasons(&mut self, cancel_broadcast: bool, tables: &Tables) {
        if self.reasons_readable(cancel_broadcast) {
            return;
        }
        self.token = self.token.map(|why| {
            if tables.is_failure(why) {
                Why::LOCAL_FAILURE
            } else {
                Why::LOCAL_STOP
            }
        });
        if self.escalation.is_some() {
            self.escalation = Some(Why::LOCAL_FAILURE);
        }
    }

    /// Whether a broadcast can still read the token's reasons. Not once the
    /// machine has asked for its `Cancel` (`cancel_broadcast`, only once),
    /// none is pending, and `AnnounceUnfinished` sends nothing any more (a
    /// failure, or the unfinished stop, was announced): stable from then on,
    /// as nothing else reads the escalation, and the other reads of the
    /// token (whether it is cancelled, its first reason) do not change.
    fn reasons_readable(&self, cancel_broadcast: bool) -> bool {
        !cancel_broadcast
            || !(self.failure_announced || self.unfinished_announced)
            || self
                .pending
                .iter()
                .any(|act| matches!(act, Act::BroadcastCancel | Act::AnnounceUnfinished))
    }

    /// The identity of a violation found in this state: the environment's
    /// side of it, without the coordinator's channel and the driver's state,
    /// and nothing of the machine, whose representation may change.
    fn identity(&self) -> u64 {
        let mut env = *self;
        env.queue = Fifo::EMPTY;
        env.cancel_seen = false;
        env.timer = false;
        env.polled = false;
        env.pending = Fifo::EMPTY;
        env.tearing_down = false;
        env.drain_inbound = false;
        env.finished = false;
        env.failure_announced = false;
        env.unfinished_announced = false;
        env.worker_failure = false;
        let mut bits = Bits::new([0; WORDS]);
        env.pack(&mut bits);
        mix(bits.words)
    }

    fn pack(&self, bits: &mut Bits) {
        let Self {
            queue,
            token,
            escalation,
            cancel_seen,
            aborted,
            timer,
            polled,
            pending,
            tearing_down,
            drain_inbound,
            finished,
            failure_announced,
            unfinished_announced,
            worker_failure,
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
            peer_announced,
            peer_fin,
            peer_gone,
            late,
            inbound_lost,
            corrupt_in,
            corrupt_unheard,
            failed_at_finish,
            lost_at_finish,
        } = self;
        queue.pack(bits);
        bits.put_item(*token);
        bits.put_item(*escalation);
        bits.put_flag(*cancel_seen);
        bits.put_flag(*aborted);
        bits.put_flag(*timer);
        bits.put_flag(*polled);
        pending.pack(bits);
        bits.put_flag(*tearing_down);
        bits.put_flag(*drain_inbound);
        bits.put_flag(*finished);
        bits.put_flag(*failure_announced);
        bits.put_flag(*unfinished_announced);
        bits.put_flag(*worker_failure);
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
        bits.put_flag(*peer_announced);
        bits.put_flag(*peer_fin);
        bits.put_flag(*peer_gone);
        bits.put_item(*late);
        bits.put_flag(*inbound_lost);
        bits.put_flag(*corrupt_in);
        bits.put_flag(*corrupt_unheard);
        bits.put_flag(*failed_at_finish);
        bits.put_flag(*lost_at_finish);
    }

    /// The fields in the order [`Self::pack`] wrote them.
    fn unpack(bits: &mut Bits) -> Self {
        Self {
            queue: Fifo::unpack(bits),
            token: bits.item(),
            escalation: bits.item(),
            cancel_seen: bits.flag(),
            aborted: bits.flag(),
            timer: bits.flag(),
            polled: bits.flag(),
            pending: Fifo::unpack(bits),
            tearing_down: bits.flag(),
            drain_inbound: bits.flag(),
            finished: bits.flag(),
            failure_announced: bits.flag(),
            unfinished_announced: bits.flag(),
            worker_failure: bits.flag(),
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
            peer_announced: bits.flag(),
            peer_fin: bits.flag(),
            peer_gone: bits.flag(),
            late: bits.item(),
            inbound_lost: bits.flag(),
            corrupt_in: bits.flag(),
            corrupt_unheard: bits.flag(),
            failed_at_finish: bits.flag(),
            lost_at_finish: bits.flag(),
        }
    }
}

// ---------------------------------------------------------------------------
// The machine's state
// ---------------------------------------------------------------------------

/// A failure an event here carries, which the machine may hold as a soft
/// failure or act on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Failure {
    SendFailed,
    ChannelClosed,
    ConnectionClosed,
    ConnectionReset,
    /// The data bridge's frame that does not decode.
    DataDecode,
    /// The progress bridge's, before its end-of-stream and after it.
    ProgressDecode,
    ProgressViolation,
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
            LinkFailure::Decode(detail) if detail == DATA_DECODE => Self::DataDecode,
            LinkFailure::Decode(detail) if detail == PROGRESS_DECODE => Self::ProgressDecode,
            LinkFailure::ProtocolViolation(detail) if detail == PROGRESS_VIOLATION => {
                Self::ProgressViolation
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
            Self::DataDecode => LinkFailure::Decode(DATA_DECODE.into()),
            Self::ProgressDecode => LinkFailure::Decode(PROGRESS_DECODE.into()),
            Self::ProgressViolation => LinkFailure::ProtocolViolation(PROGRESS_VIOLATION.into()),
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
            Self::DataDecode => 5,
            Self::ProgressDecode => 6,
            Self::ProgressViolation => 7,
        }
    }

    fn from_code(code: u64) -> Self {
        match code {
            1 => Self::SendFailed,
            2 => Self::ChannelClosed,
            3 => Self::ConnectionClosed,
            4 => Self::ConnectionReset,
            5 => Self::DataDecode,
            6 => Self::ProgressDecode,
            7 => Self::ProgressViolation,
            _ => unreachable!("failure {code}"),
        }
    }
}

/// An action the driver has still to take, small: its peer is [`PEER`], and
/// its reason a [`Why`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Act {
    SendAck,
    BroadcastCancel,
    SendGoodbye,
    LinkFailed(Failure),
    ArmSoftFailureTimer,
    CancelDataflow(Why),
    AbortDrain(Why),
    AnnounceUnfinished,
    Teardown { graceful: bool },
}

impl Act {
    fn of(action: &Action, tables: &mut Tables) -> Self {
        match action {
            Action::SendAck(_) => Self::SendAck,
            Action::BroadcastCancel => Self::BroadcastCancel,
            Action::SendGoodbye => Self::SendGoodbye,
            Action::LinkFailed { failure, .. } => Self::LinkFailed(Failure::of(failure)),
            Action::ArmSoftFailureTimer(_) => Self::ArmSoftFailureTimer,
            Action::CancelDataflow(reason) => Self::CancelDataflow(tables.why(reason)),
            Action::AbortDrain(reason) => Self::AbortDrain(tables.why(reason)),
            Action::AnnounceUnfinished => Self::AnnounceUnfinished,
            Action::Teardown { graceful } => Self::Teardown {
                graceful: *graceful,
            },
        }
    }

    fn action(self, tables: &Tables) -> Action {
        match self {
            Self::SendAck => Action::SendAck(PEER.to_string()),
            Self::BroadcastCancel => Action::BroadcastCancel,
            Self::SendGoodbye => Action::SendGoodbye,
            Self::LinkFailed(failure) => Action::LinkFailed {
                peer: PEER.to_string(),
                failure: failure.link_failure(),
            },
            Self::ArmSoftFailureTimer => Action::ArmSoftFailureTimer(PEER.to_string()),
            Self::CancelDataflow(why) => Action::CancelDataflow(tables.reason(why)),
            Self::AbortDrain(why) => Action::AbortDrain(tables.reason(why)),
            Self::AnnounceUnfinished => Action::AnnounceUnfinished,
            Self::Teardown { graceful } => Action::Teardown { graceful },
        }
    }
}

impl Code for Act {
    const BITS: u32 = 8;

    fn code(self) -> u64 {
        let (kind, payload) = match self {
            Self::SendAck => (1, 0),
            Self::BroadcastCancel => (2, 0),
            Self::SendGoodbye => (3, 0),
            Self::LinkFailed(failure) => (4, failure.code()),
            Self::ArmSoftFailureTimer => (5, 0),
            Self::CancelDataflow(why) => (6, why.code()),
            Self::AbortDrain(why) => (7, why.code()),
            Self::AnnounceUnfinished => (8, 0),
            Self::Teardown { graceful } => (9, u64::from(graceful)),
        };
        kind << 4 | payload
    }

    fn from_code(code: u64) -> Self {
        let payload = code & 15;
        match code >> 4 {
            1 => Self::SendAck,
            2 => Self::BroadcastCancel,
            3 => Self::SendGoodbye,
            4 => Self::LinkFailed(Failure::from_code(payload)),
            5 => Self::ArmSoftFailureTimer,
            6 => Self::CancelDataflow(Why::from_code(payload)),
            7 => Self::AbortDrain(Why::from_code(payload)),
            8 => Self::AnnounceUnfinished,
            9 => Self::Teardown {
                graceful: payload == 1,
            },
            kind => unreachable!("action {kind}"),
        }
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
    fn of(m: &LifecycleMachine, tables: &mut Tables) -> Self {
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
            data_loss: tables.loss_code(data_loss),
            pending_loss: tables.loss_code(pending_loss),
            deferred_loss: tables.loss_code(deferred_loss),
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
    fn machine(&self, tables: &Tables) -> LifecycleMachine {
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
            data_loss: tables.loss(self.data_loss),
            pending_loss: tables.loss(self.pending_loss),
            deferred_loss: tables.loss(self.deferred_loss),
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
            bits.put_small(*loss, 3);
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
            data_loss: bits.small(3),
            pending_loss: bits.small(3),
            deferred_loss: bits.small(3),
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
struct Key([u64; WORDS]);

impl Key {
    fn pack(w: &World, m: &Mach) -> Self {
        let mut bits = Bits::new([0; WORDS]);
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

/// A well-mixed 64-bit hash of a state's words.
fn mix(words: [u64; WORDS]) -> u64 {
    let mut h = 0x9e37_79b9_7f4a_7c15_u64;
    for word in words {
        h = (h ^ word).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h ^= h >> 31;
    }
    h.wrapping_mul(0x94d0_49bb_1331_11eb) ^ (h >> 29)
}

/// Items in chunks of a fixed size, so that a push never moves (or copies)
/// what is stored.
struct Chunked<T> {
    chunks: Vec<Vec<T>>,
    len: usize,
}

impl<T: Copy> Chunked<T> {
    const CHUNK: usize = 1 << 20;

    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            len: 0,
        }
    }

    fn push(&mut self, item: T) {
        if self.len % Self::CHUNK == 0 {
            self.chunks.push(Vec::with_capacity(Self::CHUNK));
        }
        self.chunks.last_mut().expect("a chunk").push(item);
        self.len += 1;
    }

    fn get(&self, at: usize) -> T {
        self.chunks[at / Self::CHUNK][at % Self::CHUNK]
    }

    fn bytes(&self) -> usize {
        self.chunks.len() * Self::CHUNK * size_of::<T>()
    }
}

/// Every state the search reached, each once, exactly, in the order it
/// reached them, so that a level of the search is a range of them. With
/// each, the state it was first reached from and the step that took it
/// there. `index` finds a state by its key: open addressing, probing
/// linearly, of 1 + the state's number (0 marks an empty slot), never more
/// than half full. Each key is held once, here.
struct States {
    keys: Chunked<Key>,
    trail: Chunked<(u32, Step)>,
    index: Vec<u32>,
}

impl States {
    fn new() -> Self {
        Self {
            keys: Chunked::new(),
            trail: Chunked::new(),
            index: vec![0; 1 << 16],
        }
    }

    fn len(&self) -> u32 {
        u32::try_from(self.keys.len).expect("fewer than 2^32 states")
    }

    fn key(&self, at: u32) -> Key {
        self.keys.get(at as usize)
    }

    fn trail(&self, at: u32) -> (u32, Step) {
        self.trail.get(at as usize)
    }

    /// The slot of `key`: where it is, or the empty one it goes in.
    fn slot(index: &[u32], keys: &Chunked<Key>, key: Key) -> usize {
        let mask = index.len() - 1;
        let mut slot = usize::try_from(mix(key.0)).expect("64-bit") & mask;
        while index[slot] != 0 && keys.get(index[slot] as usize - 1) != key {
            slot = (slot + 1) & mask;
        }
        slot
    }

    /// Add `key`, reached from state `from` by `step`, unless it was
    /// reached before: its number if it is new.
    fn insert(&mut self, key: Key, from: u32, step: Step) -> Option<u32> {
        let slot = Self::slot(&self.index, &self.keys, key);
        if self.index[slot] != 0 {
            return None;
        }
        let at = self.len();
        self.index[slot] = at.checked_add(1).expect("fewer than 2^32 states");
        self.keys.push(key);
        self.trail.push((from, step));
        if self.keys.len * 2 > self.index.len() {
            let mut index = vec![0; self.index.len() * 2];
            for n in 0..self.keys.len {
                let slot = Self::slot(&index, &self.keys, self.keys.get(n));
                index[slot] = u32::try_from(n + 1).expect("fewer than 2^32 states");
            }
            self.index = index;
        }
        Some(at)
    }

    /// Roughly the bytes held.
    fn bytes(&self) -> usize {
        self.keys.bytes() + self.trail.bytes() + self.index.len() * size_of::<u32>()
    }
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

/// What a coordinator step's environment does where the dataflow token may
/// change before one of its reads: a choice at each such read, 2 bits each,
/// in order. Past its last choice a script changes nothing.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct Script(u16);

impl Script {
    const NONE: Self = Self(0);
    const SLOTS: u32 = 8;

    fn choice(self, slot: u32) -> u8 {
        u8::try_from((self.0 >> (2 * slot)) & 3).expect("2 bits")
    }

    fn with(self, slot: u32, choice: u8) -> Self {
        assert!(
            slot < Self::SLOTS && choice < 4,
            "slot {slot}, choice {choice}"
        );
        Self(self.0 | u16::from(choice) << (2 * slot))
    }

    /// The slots up to its last choice.
    fn len(self) -> u32 {
        (u16::BITS - self.0.leading_zeros()).div_ceil(2)
    }
}

impl fmt::Debug for Script {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let choices: Vec<u8> = (0..self.len()).map(|slot| self.choice(slot)).collect();
        write!(f, "{choices:?}")
    }
}

/// A coordinator step running a script: the choice at each read, and the
/// scripts that choose otherwise at a read past the script's last choice.
/// Running each script once, from the empty one, runs every combination of
/// choices exactly once.
struct Cursor {
    script: Script,
    slot: u32,
    others: Vec<Script>,
}

impl Cursor {
    fn new(script: Script) -> Self {
        Self {
            script,
            slot: 0,
            others: Vec::new(),
        }
    }

    /// Choose among `n` options (0 changes nothing).
    fn choose(&mut self, n: usize) -> usize {
        let slot = self.slot;
        assert!(
            slot < Script::SLOTS,
            "more choices than slots: raise Script::SLOTS"
        );
        self.slot += 1;
        if slot >= self.script.len() {
            for choice in 1..n {
                let choice = u8::try_from(choice).expect("few options");
                self.others.push(self.script.with(slot, choice));
            }
        }
        usize::from(self.script.choice(slot))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Step {
    // The coordinator: the only steps that change the machine.
    TokenSeen(Script),
    AbortSeen(Script),
    Apply(Script),
    GraceEnds(Script),
    /// The `Cancel` broadcast the driver awaits is done: it takes the
    /// actions left.
    Resume(Script),
    /// `select!` finds no event, and polls the grace deadline next, which
    /// has passed.
    Poll,
    /// Teardown's bounded waits are over: it applies what is queued and
    /// publishes its result.
    TeardownEnds(Script),
    // Our side.
    Finish(Outcome),
    LocalCancel(Kind),
    ExternalAbort,
    // Our frames to the peer.
    Deliver,
    /// One of our `Cancel` frames reaches the peer (who cancels or not on
    /// its own anyway).
    DeliverCancel,
    WriteFails,
    /// Shared: a write that is not ours fails on the connection our lane is
    /// pinned to (an RTT probe, a reply to one of the peer's, a frame of
    /// another dataflow whose lane is pinned there), with nothing of ours
    /// queued: the transport fails every lane pinned there. After the peer
    /// has gone, or a reset our reader had seen closed, the next write
    /// fails, but nothing has to make one. Before, it is the write-error
    /// fault (`WriteError`).
    OtherWriteFails,
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
    /// Shared: the io forwarder hands on a notice the transport queued after
    /// the end of the read side. The transport hands that end on from a task
    /// of its own (so that the peer's channels close only once it has been),
    /// and a notice it queues after it can reach the forwarder first.
    ForwardAheadOfEnd,
    Dispatch,
    DispatchReport,
    DispatchClosed,
    Bridge(u8),
    BridgeClosed(u8),
    // The peer.
    PeerGoodbye,
    PeerAck,
    PeerCancel(Kind),
    /// After its requested stop, and before its goodbye, the peer announces
    /// that its part is unfinished, with its failure: a second `Cancel`.
    PeerAnnounces,
    PeerFin,
    PeerGone,
    // Faults.
    WriteError,
    Reset {
        write_first: bool,
    },
    /// Split: the connection the peer's frames arrive on is reset.
    ResetInbound,
    /// A frame from the peer reaches the data bridge for an operator that
    /// had finished: records, or a frame that does not decode. The bridge
    /// reports it later (`ReportLate`): it takes the frames it buffered in
    /// turn, so its report can come after anything that happened since,
    /// such as the connection's end, or not before teardown.
    LateRecord {
        late: Late,
    },
    /// The peer's next frame to us, on its first progress channel, is
    /// corrupt.
    CorruptFrame,
    /// The data bridge reports what it holds.
    ReportLate,
}

impl Step {
    /// The coordinator's step, with its script.
    fn script(self) -> Option<(&'static str, Script)> {
        match self {
            Self::TokenSeen(script) => Some(("TokenSeen", script)),
            Self::AbortSeen(script) => Some(("AbortSeen", script)),
            Self::Apply(script) => Some(("Apply", script)),
            Self::GraceEnds(script) => Some(("GraceEnds", script)),
            Self::Resume(script) => Some(("Resume", script)),
            Self::TeardownEnds(script) => Some(("TeardownEnds", script)),
            _ => None,
        }
    }

    /// The coordinator's step with another script.
    fn with_script(self, script: Script) -> Self {
        match self {
            Self::TokenSeen(_) => Self::TokenSeen(script),
            Self::AbortSeen(_) => Self::AbortSeen(script),
            Self::Apply(_) => Self::Apply(script),
            Self::GraceEnds(_) => Self::GraceEnds(script),
            Self::Resume(_) => Self::Resume(script),
            Self::TeardownEnds(_) => Self::TeardownEnds(script),
            _ => unreachable!("not the coordinator's: {self:?}"),
        }
    }

    /// The step as traces and golden files name it: a coordinator step's
    /// script only if it makes a choice.
    fn text(self) -> String {
        match self.script() {
            Some((name, Script::NONE)) => name.to_string(),
            Some((name, script)) => format!("{name}{script:?}"),
            None => format!("{self:?}"),
        }
    }

    /// Bound to happen once it can. The rest may never happen: faults, a
    /// local cancellation, the abort from outside, the peer's goodbye, ack
    /// and `Cancel`, and a write that is not ours.
    fn bound_to_happen(self) -> bool {
        !matches!(
            self,
            Self::LocalCancel(_)
                | Self::ExternalAbort
                | Self::PeerGoodbye
                | Self::PeerAck
                | Self::PeerCancel(_)
                | Self::PeerAnnounces
                | Self::OtherWriteFails
                | Self::WriteError
                | Self::Reset { .. }
                | Self::ResetInbound
                | Self::LateRecord { .. }
                | Self::CorruptFrame
        )
    }
}

/// Every step that can happen in `w`, the coordinator's first.
fn steps(w: &World, m: &Mach, cfg: Config, tables: &Tables, out: &mut Vec<Step>) {
    out.clear();
    if w.finished {
        // Torn down: our workers may still finish, and our frames still in
        // the transport be written, or lost (see the module docs).
        sent_after_teardown(w, cfg, out);
        finish_steps(w, m, tables, out);
        return;
    }
    if w.tearing_down {
        // From teardown on, nothing reaches the coordinator from the network
        // (see the module docs): only the data bridge reports what it holds,
        // the workers finish, and our frames still in the transport are
        // written, or lost.
        out.extend(driver::next(w, cfg));
        if w.late.is_some() {
            out.push(Step::ReportLate);
        }
        sent_after_teardown(w, cfg, out);
        finish_steps(w, m, tables, out);
        return;
    }
    out.extend(driver::next(w, cfg));
    finish_steps(w, m, tables, out);
    // The data bridge's report: it is not cancelled with the other bridges.
    if w.late.is_some() {
        out.push(Step::ReportLate);
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
    // Shared: with nothing of ours queued, a write that is not ours fails
    // once the next write fails (with something of ours queued, ours does,
    // to the same effect). Before, as the write-error fault, below.
    if cfg.shared() && w.write_fails && !w.writer_dead && !writing {
        out.push(Step::OtherWriteFails);
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
            if matches!(front, In::Cancel(_))
                && !w.peer_locked
                && w.wire.iter().any(|frame| !matches!(frame, In::Cancel(_)))
            {
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
        if cfg.shared()
            && matches!(w.io.front(), Some(Notice::InboundEnded { .. }))
            && w.io.len() > 1
        {
            out.push(Step::ForwardAheadOfEnd);
        }
    }
    if w.dispatching {
        out.push(Step::DispatchReport);
    } else if !w.ctrl.is_empty() {
        out.push(Step::Dispatch);
    } else if w.channels_closed && !w.ctrl_close_reported {
        out.push(Step::DispatchClosed);
    }
    // The progress bridges, until the bridges are cancelled (from outside,
    // or by teardown): they take the cancellation ahead of what is queued.
    for ch in (0..cfg.channels).filter(|_| !w.aborted) {
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
        if w.records_delivered
            && !w.peer_goodbye
            && w.peer_cancel != Some(Kind::Fail)
            && !w.peer_announced
        {
            out.push(Step::PeerGoodbye);
        }
        // Its machine finishes as it announces its part unfinished.
        if w.goodbye_delivered && !w.peer_ack && !w.peer_announced {
            out.push(Step::PeerAck);
        }
        if w.peer_cancel.is_none() {
            out.push(Step::PeerCancel(Kind::Fail));
            if !w.peer_goodbye {
                out.push(Step::PeerCancel(Kind::Stop));
            }
        } else if w.peer_cancel == Some(Kind::Stop)
            && !w.peer_announced
            && !w.peer_goodbye
            && cfg.workers == 1
        {
            out.push(Step::PeerAnnounces);
        }
    }
    if !w.peer_fin && (w.from_peer || w.to_peer) {
        out.push(Step::PeerFin);
    }
    if w.peer_fin && !w.peer_gone {
        out.push(Step::PeerGone);
    }
    // At most one fault.
    if !w.fault && cfg.faults == Faults::Transport {
        // A write error: ours, or (Shared) a write that is not ours on our
        // lane's connection (see `Step::OtherWriteFails`).
        if (writing || cfg.shared()) && !w.writer_dead && w.to_peer && !w.write_fails {
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
    // What the peer sends us is lost on its way in, or corrupt.
    if !w.fault && cfg.faults == Faults::Inbound && w.from_peer && !w.peer_fin {
        if w.reader_up {
            out.extend([Late::Lost, Late::Corrupt].map(|late| Step::LateRecord { late }));
        }
        out.push(Step::CorruptFrame);
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

/// From teardown on, while a worker still runs (teardown came from the
/// bridges cancelled from outside), our frames still in the transport (see
/// [`World::forget`]): the transport writes them, or loses them to a write
/// that fails, as the peer goes away or a write error happens. Nothing of
/// it reaches the coordinator any more, and nothing else on the network
/// matters to the properties: the peer's goodbye, ack or `Cancel` would only
/// excuse an `Ok`, and a reset shows as our next write failing.
fn sent_after_teardown(w: &World, cfg: Config, out: &mut Vec<Step>) {
    if !w.writing() {
        return;
    }
    if w.to_peer && !w.write_fails {
        if w.cancel_out() {
            out.push(Step::DeliverCancel);
        }
        if w.next_on_lane().is_some() {
            out.push(Step::Deliver);
        }
        if !w.peer_fin {
            out.push(Step::PeerFin);
        } else if !w.peer_gone {
            out.push(Step::PeerGone);
        }
        if !w.fault && cfg.faults == Faults::Transport {
            out.push(Step::WriteError);
        }
    }
    if w.write_fails {
        out.push(Step::WriteFails);
    }
}

/// Our workers. One may succeed although the dataflow was cancelled (it
/// drained, or it finished first), and may fail at any time. It reports a
/// cancellation (no error) only once the dataflow is cancelled. That differs
/// from a failure only in the reason `AnnounceUnfinished` may give, so it is
/// explored only while that can still depend on it: `AnnounceUnfinished`
/// can still send, no worker's error is recorded or queued ahead of it, the
/// coordinator will apply it (not teardown), and a requested stop is the
/// token's first reason, or a broadcast already went out without
/// announcing its failure (it found no reason, and the token was cancelled
/// before its next read). Otherwise every broadcast to come announces a
/// failure, and `AnnounceUnfinished` then sends nothing.
fn finish_steps(w: &World, m: &Mach, tables: &Tables, out: &mut Vec<Step>) {
    if w.workers_left == 0 {
        return;
    }
    out.extend([Step::Finish(Outcome::Ok), Step::Finish(Outcome::Failed)]);
    let Some(first) = w.token else {
        return;
    };
    let broadcast_done =
        m.cancel_broadcast && !w.pending.iter().any(|act| act == Act::BroadcastCancel);
    if !(w.failure_announced || w.unfinished_announced || w.worker_failure)
        && !w.queue.iter().any(|ev| ev == Ev::Done(Outcome::Failed))
        && !(w.tearing_down || w.finished)
        && (!tables.is_failure(first) || broadcast_done)
    {
        out.push(Step::Finish(Outcome::Cancelled));
    }
}

/// A step other than the coordinator's. It reads only whether the machine
/// retired the link (`retired`).
fn take_env(w: &mut World, retired: bool, step: Step, cfg: Config, tables: &Tables) {
    match step {
        Step::Finish(outcome) => {
            w.workers_left -= 1;
            w.workers_failed |= outcome != Outcome::Ok;
            // From teardown on the result goes to the completion only:
            // teardown's loop applies no worker's result (see those queued
            // before it began).
            if !(w.tearing_down || w.finished) {
                w.queue.push_back(Ev::Done(outcome));
            }
            if w.workers_left == 0 && !w.workers_failed {
                w.latch();
            }
        }
        Step::LocalCancel(kind) => w.cancel(Why::local(kind)),
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
        Step::WriteFails | Step::OtherWriteFails => w.writer_fails(cfg),
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
            if !retired {
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
        Step::ForwardAheadOfEnd => {
            let notice =
                w.io.take_first(|notice| !matches!(notice, Notice::InboundEnded { .. }))
                    .expect("queued after the end");
            w.queue.push_back(notice.event());
        }
        Step::Dispatch => {
            let kind = w.ctrl.pop_front().expect("read");
            w.escalate(Why::peer(kind), tables);
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
            // After its end-of-stream the bridge reports it once and keeps
            // reading (`watch_after_end_of_stream`); before, it ends.
            In::Corrupt => {
                let i = usize::from(ch);
                w.queue.push_back(Ev::ProgressCorrupt {
                    after_end_of_stream: w.end_of_stream[i],
                });
                if !w.end_of_stream[i] {
                    w.prog_close_reported[i] = true;
                    w.prog[i].clear();
                }
            }
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
        Step::PeerAnnounces => {
            w.peer_announced = true;
            w.wire.push_back(In::Cancel(Kind::Fail));
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
        Step::LateRecord { late } => {
            w.fault = true;
            match late {
                Late::Lost => w.inbound_lost = true,
                Late::Corrupt => w.corrupt_in = true,
            }
            w.late = Some(late);
        }
        Step::CorruptFrame => {
            w.fault = true;
            w.wire.push_back(In::Corrupt);
        }
        Step::ReportLate => w.queue.push_back(w.late.take().expect("held").event()),
        Step::TokenSeen(_)
        | Step::AbortSeen(_)
        | Step::Apply(_)
        | Step::GraceEnds(_)
        | Step::Resume(_)
        | Step::Poll
        | Step::TeardownEnds(_) => unreachable!("the coordinator's: {step:?}"),
    }
}

// ---------------------------------------------------------------------------
// The machine at work
// ---------------------------------------------------------------------------

/// What the checks saw during a step and, when narrating a trace, what the
/// machine did and the effects the driver took. Its counts are the premises'
/// witnesses.
#[derive(Default)]
struct Observer {
    narrate: bool,
    notes: Vec<String>,
    effects: Vec<Effect>,
    /// What the coordinator's step produced (see [`Output`]).
    outputs: Vec<Output>,
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

    fn effect(&mut self, effect: &Effect) {
        if self.narrate {
            if let Some(text) = effect_text(effect) {
                self.notes.push(text);
            }
            self.effects.push(effect.clone());
        }
        self.outputs.push(Output::Effect(effect.clone()));
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
/// Which losses the machine holds (recorded, pending and deferred) are
/// the frames a failed send dropped.
fn outbound_held(m: &LifecycleMachine) -> [bool; 3] {
    let outbound = |loss: &Option<DataLoss>| {
        loss.as_ref()
            .is_some_and(|loss| loss.detail == outbound_loss_detail(&LinkFailure::SendFailed))
    };
    [
        outbound(&m.data_loss),
        outbound(&m.pending_loss),
        outbound(&m.deferred_loss),
    ]
}

/// The failed send was settled as a failure: a link failure, or the loss of
/// the frames it dropped, recorded or held, that was not before (`before`,
/// see [`outbound_held`]). Another loss (records lost on their way in, say)
/// is a failure of its own.
fn settled_as_failure(m: &LifecycleMachine, before: [bool; 3], actions: &[Action]) -> bool {
    outbound_held(m)
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

/// An event reaches the coordinator. Checks NoFalseSettle.
fn applies(
    w: &mut World,
    d: &mut impl Decide,
    ev: Ev,
    cfg: Config,
    tables: &Tables,
    cursor: &mut Cursor,
    seen: &mut Observer,
) -> Vec<Action> {
    let owed = d.machine().peers[PEER].unsent();
    let before = losses_held(d.machine());
    let outbound = outbound_held(d.machine());
    let actions = decide(w, tables, cfg, cursor, seen, |token| {
        d.on_event(ev.event(), token)
    });
    let m = d.machine();
    // A send owed before, or one this event reports, that is no longer owed.
    // Corrupt traffic is a failure of its own.
    if (owed || ev == Ev::SendFailed) && !m.peers[PEER].unsent() && !ev.corrupt() {
        if settled_as_failure(m, outbound, &actions) {
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
fn grace_expires(w: &World, d: &mut impl Decide, seen: &mut Observer) -> Vec<Action> {
    let peer = &d.machine().peers[PEER];
    let owed = peer.unsent();
    let finished_peer = owed && peer.retired() && peer.fully_closed();
    let before = losses_held(d.machine());
    let outbound = outbound_held(d.machine());
    let actions = d.on_soft_failure_expired(PEER.to_string());
    let m = d.machine();
    if owed {
        seen.grace_owed += 1;
        seen.grace_finished_peer += u64::from(finished_peer);
    }
    if owed && settled_as_failure(m, outbound, &actions) {
        if finished_peer {
            seen.false_settle = Some(
                "the grace period settled the failed send as a failure although the peer had \
                 said goodbye and its link had closed fully",
            );
        } else if w.queue.iter().any(Ev::excuses) {
            seen.false_settle = Some(
                "the grace period settled the failed send as a failure although the peer's \
                 Cancel, goodbye or ack was already queued for the coordinator",
            );
        }
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

/// A change the environment makes to the dataflow token while a coordinator
/// step runs, just before one of its reads, whose value it changes. Each
/// takes the environment's own steps, in their own order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Change {
    /// The dataflow is cancelled locally.
    LocalCancel(Kind),
    /// The control dispatcher takes the peer's next `Cancel`, and cancels the
    /// token with it.
    DispatcherCancels,
    /// The control dispatcher reports the `Cancel` it holds, and takes the
    /// peer's `Cancel`s up to a failure, which it escalates the token with.
    DispatcherEscalates,
}

/// What the coordinator reads of the token.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Read {
    Cancelled,
    Reason,
    Escalation,
}

/// The changes the environment can make to the token before `read`, each
/// one changing the value it returns. A cancellation changes whether the
/// token is cancelled and its reason; an escalation needs a requested stop
/// first and a failure from the peer.
fn changes(w: &World, tables: &Tables, read: Read) -> Vec<&'static [Change]> {
    let failure_queued = |skip: usize| w.ctrl.iter().skip(skip).any(|kind| kind == Kind::Fail);
    match (w.token, w.escalation, read) {
        (None, _, Read::Cancelled | Read::Reason) => {
            let mut options: Vec<&'static [Change]> = vec![
                &[Change::LocalCancel(Kind::Stop)],
                &[Change::LocalCancel(Kind::Fail)],
            ];
            if !w.ctrl.is_empty() {
                options.push(&[Change::DispatcherCancels]);
            }
            options
        }
        (None, _, Read::Escalation) => {
            let mut options: Vec<&'static [Change]> = Vec::new();
            if failure_queued(0) {
                options.push(&[Change::LocalCancel(Kind::Stop), Change::DispatcherEscalates]);
            }
            if w.ctrl.front() == Some(Kind::Stop) && failure_queued(1) {
                options.push(&[Change::DispatcherCancels, Change::DispatcherEscalates]);
            }
            options
        }
        (Some(first), None, Read::Escalation) if !tables.is_failure(first) && failure_queued(0) => {
            vec![&[Change::DispatcherEscalates]]
        }
        _ => Vec::new(),
    }
}

/// The dataflow token as a coordinator step reads it, in the state the step
/// runs in. Before each read whose value the environment could change, the
/// step's script chooses whether it does, and how ([`changes`]).
struct ModelToken<'a> {
    ctx: std::cell::RefCell<TokenCtx<'a>>,
}

struct TokenCtx<'a> {
    w: &'a mut World,
    tables: &'a Tables,
    cfg: Config,
    cursor: &'a mut Cursor,
    /// What the environment did, for a trace.
    notes: Option<Vec<String>>,
    /// The reads made, in order.
    reads: Vec<Read>,
}

impl TokenCtx<'_> {
    fn before(&mut self, read: Read) {
        self.reads.push(read);
        let options = changes(self.w, self.tables, read);
        if options.is_empty() {
            return;
        }
        let choice = self.cursor.choose(options.len() + 1);
        let Some(changes) = choice.checked_sub(1).map(|option| options[option]) else {
            return;
        };
        for &change in changes {
            let note = match change {
                Change::LocalCancel(kind) => {
                    take_env(
                        self.w,
                        false,
                        Step::LocalCancel(kind),
                        self.cfg,
                        self.tables,
                    );
                    format!("meanwhile the dataflow is cancelled locally ({kind})")
                }
                Change::DispatcherCancels => {
                    let kind = self.w.ctrl.front().expect("queued");
                    take_env(self.w, false, Step::Dispatch, self.cfg, self.tables);
                    format!(
                        "meanwhile the control dispatcher takes the peer's Cancel ({kind}) and \
                         cancels the token"
                    )
                }
                Change::DispatcherEscalates => {
                    loop {
                        if self.w.dispatching {
                            take_env(self.w, false, Step::DispatchReport, self.cfg, self.tables);
                        }
                        let kind = self.w.ctrl.front().expect("a failure queued");
                        take_env(self.w, false, Step::Dispatch, self.cfg, self.tables);
                        if kind == Kind::Fail {
                            break;
                        }
                    }
                    "meanwhile the control dispatcher takes the peer's Cancel (a failure) and \
                     escalates the token"
                        .to_string()
                }
            };
            if let Some(notes) = &mut self.notes {
                notes.push(note);
            }
        }
    }
}

impl<'a> ModelToken<'a> {
    fn new(
        w: &'a mut World,
        tables: &'a Tables,
        cfg: Config,
        cursor: &'a mut Cursor,
        narrate: bool,
    ) -> Self {
        Self {
            ctx: std::cell::RefCell::new(TokenCtx {
                w,
                tables,
                cfg,
                cursor,
                notes: narrate.then(Vec::new),
                reads: Vec::new(),
            }),
        }
    }

    /// What the environment did, and the reads made.
    fn into_log(self) -> (Vec<String>, Vec<Read>) {
        let ctx = self.ctx.into_inner();
        (ctx.notes.unwrap_or_default(), ctx.reads)
    }
}

impl TokenReader for ModelToken<'_> {
    fn is_cancelled(&self) -> bool {
        let mut ctx = self.ctx.borrow_mut();
        ctx.before(Read::Cancelled);
        ctx.w.token.is_some()
    }

    fn reason(&self) -> Option<CancellationReason> {
        let mut ctx = self.ctx.borrow_mut();
        ctx.before(Read::Reason);
        ctx.w.token.map(|why| ctx.tables.reason(why))
    }

    fn escalation(&self) -> Option<CancellationReason> {
        let mut ctx = self.ctx.borrow_mut();
        ctx.before(Read::Escalation);
        ctx.w.escalation.map(|why| ctx.tables.reason(why))
    }
}

/// Run one of the coordinator's decisions, `decision`, in state `w`: the
/// token it reads may change before each read, as `cursor` chooses. The
/// reads are logged in `seen`.
fn decide<T>(
    w: &mut World,
    tables: &Tables,
    cfg: Config,
    cursor: &mut Cursor,
    seen: &mut Observer,
    decision: impl FnOnce(&dyn TokenReader) -> T,
) -> T {
    let token = ModelToken::new(w, tables, cfg, cursor, seen.narrate);
    let decided = decision(&token);
    let (notes, reads) = token.into_log();
    for note in notes {
        seen.note(|| note);
    }
    seen.outputs.extend(reads.into_iter().map(Output::Read));
    decided
}

/// What a coordinator's step produced, in order: its reads of the token,
/// what the driver did for it, and what it published. The equivalence
/// oracle compares these, and the state the step led to.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Output {
    /// The coordinator read the token.
    Read(Read),
    /// The driver took this effect.
    Effect(Effect),
    /// After the actions, the progress lanes of these peers close.
    LanesClosed(Vec<String>),
    /// Teardown begins, letting the inbound bridges drain or not.
    TeardownBegins { drain_inbound: bool },
    /// Teardown ends, publishing this data loss to the completion (after
    /// its last broadcast, an [`Output::Effect`], if it sends one).
    Published(Option<DataLoss>),
}

/// The coordinator's decisions: what the driver asks of it at each of its
/// decision points. The driver keeps the order in which it asks, and every
/// await and every I/O step. Implemented by the production [`Coordinator`]
/// and by a copy of the decisions the driver made before it existed
/// ([`glue::Glue`]), its equivalence oracle.
trait Decide {
    /// The decisions in state `w`, around `machine`.
    fn restore(w: &World, machine: LifecycleMachine) -> Self
    where
        Self: Sized;
    /// Store what they remember in `w`.
    fn store(&self, w: &mut World);
    fn machine(&self) -> &LifecycleMachine;
    /// The token's branch of `select!` is taken.
    fn begin_token_noticed(&mut self);
    /// One event from the channel (in the token's branch, or the events').
    fn on_event(&mut self, event: LifecycleEvent, token: &dyn TokenReader) -> Vec<Action>;
    /// The token's branch, after the events queued before it.
    fn finish_token_noticed(&mut self, token: &dyn TokenReader) -> Vec<Action>;
    fn on_external_abort(&mut self) -> Vec<Action>;
    /// Every sender of the event channel is gone. Never while the machine
    /// runs: each outbox the driver holds owns a sender, and teardown drops
    /// them. The explorer never calls it; the two-peer test does.
    fn on_events_closed(&mut self) -> Vec<Action>;
    fn on_soft_failure_expired(&mut self, peer: String) -> Vec<Action>;
    /// An action, just before the driver takes it.
    fn prepare(&mut self, action: &Action, token: &dyn TokenReader) -> Effect;
    /// The peers whose progress lanes close after the actions.
    fn stopped_peers(&self) -> Vec<String>;
    /// Teardown begins: whether it lets the inbound bridges drain.
    fn teardown_drains_inbound(&self) -> bool;
    /// An event teardown finds queued once its bounded waits are over.
    fn on_late_event(&mut self, event: LifecycleEvent, drain_inbound: bool);
    /// The reason of the `Cancel` that announces a loss nobody was told of,
    /// teardown's last broadcast, if it sends one.
    fn unannounced_loss(&mut self) -> Option<String>;
}

/// The production coordinator, in the state a [`World`] holds.
impl Decide for Coordinator {
    fn restore(w: &World, machine: LifecycleMachine) -> Self {
        Self {
            machine,
            cancel_seen: w.cancel_seen,
            worker_failure: w.worker_failure.then(|| WORKER_ERROR.to_string()),
            failure_announced: w.failure_announced,
            unfinished_announced: w.unfinished_announced,
        }
    }

    fn store(&self, w: &mut World) {
        w.cancel_seen = self.cancel_seen;
        w.worker_failure = worker_failed(self.worker_failure.as_deref());
        w.failure_announced = self.failure_announced;
        w.unfinished_announced = self.unfinished_announced;
    }

    fn machine(&self) -> &LifecycleMachine {
        Coordinator::machine(self)
    }

    fn begin_token_noticed(&mut self) {
        Coordinator::begin_token_noticed(self);
    }

    fn on_event(&mut self, event: LifecycleEvent, token: &dyn TokenReader) -> Vec<Action> {
        Coordinator::on_event(self, event, token)
    }

    fn finish_token_noticed(&mut self, token: &dyn TokenReader) -> Vec<Action> {
        Coordinator::finish_token_noticed(self, token)
    }

    fn on_external_abort(&mut self) -> Vec<Action> {
        Coordinator::on_external_abort(self)
    }

    fn on_events_closed(&mut self) -> Vec<Action> {
        Coordinator::on_events_closed(self)
    }

    fn on_soft_failure_expired(&mut self, peer: String) -> Vec<Action> {
        Coordinator::on_soft_failure_expired(self, peer)
    }

    fn prepare(&mut self, action: &Action, token: &dyn TokenReader) -> Effect {
        Coordinator::prepare(self, action, token)
    }

    fn stopped_peers(&self) -> Vec<String> {
        Coordinator::stopped_peers(self).map(String::from).collect()
    }

    fn teardown_drains_inbound(&self) -> bool {
        Coordinator::teardown_drains_inbound(self)
    }

    fn on_late_event(&mut self, event: LifecycleEvent, drain_inbound: bool) {
        Coordinator::on_late_event(self, event, drain_inbound);
    }

    fn unannounced_loss(&mut self) -> Option<String> {
        Coordinator::unannounced_loss(self)
    }
}

/// Whether a worker failed, as [`World`] holds it: every worker that fails
/// here fails with [`WORKER_ERROR`].
fn worker_failed(failure: Option<&str>) -> bool {
    if let Some(failure) = failure {
        assert_eq!(failure, WORKER_ERROR, "a worker's error");
    }
    failure.is_some()
}

/// The driver's decisions as they were before [`Coordinator`]
/// took them over: copied from `ClusterLifecycle` (`run`, `apply`,
/// `perform`, `cancel_broadcast_reason`, `unfinished_reason` and `teardown`),
/// with every read of the token through a [`TokenReader`], in the same
/// order. The equivalence oracle for [`Coordinator`].
mod glue {
    use super::*;

    pub(super) struct Glue {
        machine: LifecycleMachine,
        cancel_seen: bool,
        worker_failure: Option<String>,
        failure_announced: bool,
        unfinished_announced: bool,
    }

    impl Glue {
        /// `broadcast_cancel`, up to its I/O: remembered before the broadcast
        /// is awaited.
        fn broadcast(&mut self, reason: String) -> Effect {
            self.failure_announced |= CancellationReason::describes_failure(&reason);
            Effect::Broadcast(reason)
        }

        /// `ClusterLifecycle::unfinished_reason`.
        fn unfinished_reason(&self, token: &dyn TokenReader) -> String {
            if let Some(detail) = &self.worker_failure {
                return CancellationReason::WorkerFailed {
                    detail: detail.clone(),
                }
                .to_string();
            }
            if let Some(reason) = token.escalation() {
                return reason.to_string();
            }
            format!(
                "{} (the node stopped before finishing its part of the dataflow)",
                cancel_broadcast_reason(token)
            )
        }
    }

    /// `cancel_broadcast_reason`.
    fn cancel_broadcast_reason(token: &dyn TokenReader) -> String {
        if let Some(failure) = token.escalation() {
            return failure.to_string();
        }
        match token.reason() {
            Some(reason) => reason.to_string(),
            None if token.is_cancelled() => "cancelled".to_string(),
            None => CancellationReason::NetworkError {
                detail: "the node stopped its network bridges while its workers were running"
                    .into(),
            }
            .to_string(),
        }
    }

    impl Decide for Glue {
        fn restore(w: &World, machine: LifecycleMachine) -> Self {
            Self {
                machine,
                cancel_seen: w.cancel_seen,
                worker_failure: w.worker_failure.then(|| WORKER_ERROR.to_string()),
                failure_announced: w.failure_announced,
                unfinished_announced: w.unfinished_announced,
            }
        }

        fn store(&self, w: &mut World) {
            w.cancel_seen = self.cancel_seen;
            w.worker_failure = worker_failed(self.worker_failure.as_deref());
            w.failure_announced = self.failure_announced;
            w.unfinished_announced = self.unfinished_announced;
        }

        fn machine(&self) -> &LifecycleMachine {
            &self.machine
        }

        fn begin_token_noticed(&mut self) {
            self.cancel_seen = true;
        }

        /// `ClusterLifecycle::apply`.
        fn on_event(&mut self, event: LifecycleEvent, token: &dyn TokenReader) -> Vec<Action> {
            match event {
                LifecycleEvent::Link {
                    peer,
                    event: LinkEvent::InboundDataLost(detail),
                } => {
                    let cancelled = token.is_cancelled();
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

        fn finish_token_noticed(&mut self, token: &dyn TokenReader) -> Vec<Action> {
            let failure = token.reason().is_some_and(|reason| reason.is_failure());
            self.machine.on_dataflow_cancelled(failure)
        }

        fn on_external_abort(&mut self) -> Vec<Action> {
            self.machine.on_external_abort()
        }

        fn on_events_closed(&mut self) -> Vec<Action> {
            self.machine.on_events_closed()
        }

        fn on_soft_failure_expired(&mut self, peer: String) -> Vec<Action> {
            self.machine.on_soft_failure_expired(peer)
        }

        /// `ClusterLifecycle::perform`, up to its I/O; `run` arms the timer.
        fn prepare(&mut self, action: &Action, token: &dyn TokenReader) -> Effect {
            match action {
                Action::SendAck(peer) => Effect::SendAck(peer.clone()),
                Action::BroadcastCancel => {
                    let reason = cancel_broadcast_reason(token);
                    self.broadcast(reason)
                }
                Action::AnnounceUnfinished => {
                    if !self.failure_announced && !self.unfinished_announced {
                        self.unfinished_announced = true;
                        let reason = self.unfinished_reason(token);
                        self.broadcast(reason)
                    } else {
                        Effect::Silent
                    }
                }
                Action::AbortDrain(reason) | Action::CancelDataflow(reason) => {
                    Effect::Escalate(reason.clone())
                }
                Action::SendGoodbye => Effect::SendGoodbye,
                Action::LinkFailed { peer, failure } => {
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
                    Effect::Escalate(reason)
                }
                Action::ArmSoftFailureTimer(peer) => Effect::ArmGrace(peer.clone()),
                Action::Teardown { graceful } => Effect::Teardown {
                    graceful: *graceful,
                },
            }
        }

        /// `ClusterLifecycle::run`: the peers whose lanes close.
        fn stopped_peers(&self) -> Vec<String> {
            self.machine.stopped_peers().map(String::from).collect()
        }

        /// `ClusterLifecycle::teardown`: `drain_inbound`.
        fn teardown_drains_inbound(&self) -> bool {
            self.machine.workers_done()
        }

        /// `ClusterLifecycle::teardown`: the events drained once its
        /// bounded waits are over.
        fn on_late_event(&mut self, event: LifecycleEvent, drain_inbound: bool) {
            if let LifecycleEvent::Link { event: link, .. } = &event {
                let lost = drain_inbound && matches!(link, LinkEvent::InboundDataLost(_));
                if lost || is_corruption_event(link) {
                    let _ = self.machine.on_event(event);
                }
            }
        }

        /// `ClusterLifecycle::teardown`: the unannounced loss, broadcast
        /// with `broadcast_cancel`.
        fn unannounced_loss(&mut self) -> Option<String> {
            let unannounced = match self.machine.take_unannounced_loss() {
                Some(loss) => Some(loss),
                None if !self.failure_announced => self.machine.data_loss().cloned(),
                None => None,
            };
            let loss = unannounced?;
            let reason = CancellationReason::NetworkError {
                detail: loss.to_string(),
            }
            .to_string();
            let Effect::Broadcast(reason) = self.broadcast(reason) else {
                unreachable!("a broadcast")
            };
            Some(reason)
        }
    }
}

/// An effect, for a trace: what is not already in the action's name.
fn effect_text(effect: &Effect) -> Option<String> {
    match effect {
        Effect::Broadcast(reason) => Some(format!("broadcasts Cancel({reason:?})")),
        Effect::Silent => Some("AnnounceUnfinished sends nothing".into()),
        Effect::Escalate(reason) => Some(format!("escalates the token ({reason})")),
        _ => None,
    }
}

/// The driver, around the coordinator's decisions: what
/// `ClusterLifecycle::run` does and in which order (its biased `select!`, its
/// timers and its I/O). Everything outside this module, [`Coordinator`] and
/// [`glue`] is the environment.
mod driver {
    use super::*;

    /// The coordinator's next step, if it has one. `run`'s biased `select!`
    /// takes the dataflow token (once), then the bridges' cancellation, then
    /// the next event, then the grace timer. Its branches are polled one
    /// after another: once it has found no event and the deadline passed, it
    /// takes the timer's branch, also with an event queued since.
    pub(super) fn next(w: &World, cfg: Config) -> Option<Step> {
        if w.tearing_down {
            Some(Step::TeardownEnds(Script::NONE))
        } else if !w.pending.is_empty() {
            Some(Step::Resume(Script::NONE))
        } else if w.polled {
            Some(Step::GraceEnds(Script::NONE))
        } else if w.token.is_some() && !w.cancel_seen {
            Some(Step::TokenSeen(Script::NONE))
        } else if w.aborted {
            Some(Step::AbortSeen(Script::NONE))
        } else if !w.queue.is_empty() {
            Some(Step::Apply(Script::NONE))
        } else if w.timer && !w.busy(cfg) {
            Some(Step::Poll)
        } else {
            None
        }
    }

    /// Take the coordinator's step, the token changing as `cursor` chooses.
    pub(super) fn take(
        w: &mut World,
        d: &mut impl Decide,
        step: Step,
        cfg: Config,
        tables: &mut Tables,
        cursor: &mut Cursor,
        seen: &mut Observer,
    ) {
        let actions = match step {
            // Every event already queued is applied first, then the
            // cancellation.
            Step::TokenSeen(_) => {
                d.begin_token_noticed();
                let mut actions = Vec::new();
                while let Some(ev) = w.queue.pop_front() {
                    actions.extend(applies(w, d, ev, cfg, tables, cursor, seen));
                }
                let cancelled = decide(w, tables, cfg, cursor, seen, |token| {
                    d.finish_token_noticed(token)
                });
                seen.note(|| format!("on_dataflow_cancelled: {}", actions_text(&cancelled)));
                actions.extend(cancelled);
                actions
            }
            // At once, ahead of what is queued.
            Step::AbortSeen(_) => {
                let actions = d.on_external_abort();
                seen.note(|| format!("on_external_abort: {}", actions_text(&actions)));
                actions
            }
            Step::Apply(_) => {
                let ev = w.queue.pop_front().expect("queued");
                applies(w, d, ev, cfg, tables, cursor, seen)
            }
            // The deadline of the first soft failure armed; it stays armed
            // after the failure resolves.
            Step::GraceEnds(_) => {
                w.timer = false;
                w.polled = false;
                grace_expires(w, d, seen)
            }
            Step::Resume(_) => {
                let mut actions = Vec::new();
                while let Some(act) = w.pending.pop_front() {
                    actions.push(act.action(tables));
                }
                actions
            }
            Step::TeardownEnds(_) => {
                // Reports queued behind the event that finished the
                // machine, and since.
                while let Some(ev) = w.queue.pop_front() {
                    let before = losses_held(d.machine());
                    d.on_late_event(ev.event(), w.drain_inbound);
                    seen.note(|| {
                        format!("teardown finds {ev}{}", losses_taken(d.machine(), before))
                    });
                }
                if let Some(reason) = d.unannounced_loss() {
                    let effect = Effect::Broadcast(reason);
                    seen.effect(&effect);
                    w.send_cancel();
                }
                seen.outputs
                    .push(Output::Published(d.machine().data_loss().cloned()));
                w.tearing_down = false;
                w.finished = true;
                return;
            }
            _ => unreachable!("not the coordinator's: {step:?}"),
        };
        perform(w, d, actions, cfg, tables, cursor, seen);
    }

    /// Take the actions as `run` does: each prepared just before it is
    /// taken, up to teardown. A broadcast is awaited: the rest waits in
    /// `pending` while the environment moves.
    fn perform(
        w: &mut World,
        d: &mut impl Decide,
        actions: Vec<Action>,
        cfg: Config,
        tables: &mut Tables,
        cursor: &mut Cursor,
        seen: &mut Observer,
    ) {
        let mut actions = actions.into_iter();
        while let Some(action) = actions.next() {
            let effect = decide(w, tables, cfg, cursor, seen, |token| {
                d.prepare(&action, token)
            });
            seen.effect(&effect);
            match effect {
                Effect::SendAck(_) => w.send(Out::Ack, cfg),
                Effect::SendGoodbye => w.send(Out::Goodbye, cfg),
                // Awaited: the environment moves until it is done.
                Effect::Broadcast(_) => {
                    w.send_cancel();
                    for action in actions.by_ref() {
                        w.pending.push_back(Act::of(&action, tables));
                    }
                    if !w.pending.is_empty() {
                        return;
                    }
                }
                Effect::Silent => {}
                Effect::Escalate(reason) => {
                    let why = tables.why(&reason);
                    w.escalate(why, tables);
                }
                // A deadline already armed stays.
                Effect::ArmGrace(_) => w.timer = true,
                // Its bounded waits: the step ends.
                Effect::Teardown { .. } => {
                    w.tearing_down = true;
                    w.drain_inbound = d.teardown_drains_inbound();
                    seen.outputs.push(Output::TeardownBegins {
                        drain_inbound: w.drain_inbound,
                    });
                    seen.note(|| {
                        format!(
                            "teardown begins{}",
                            if w.drain_inbound {
                                ", draining the inbound bridges"
                            } else {
                                ""
                            }
                        )
                    });
                    return;
                }
            }
        }
        // After the actions, unless teardown returned: the progress lanes of
        // the peers that stopped without finishing their part close. The
        // workers read them; nothing here does.
        let stopped = d.stopped_peers();
        if !stopped.is_empty() {
            seen.note(|| format!("closes the progress lanes of {}", stopped.join(", ")));
            seen.outputs.push(Output::LanesClosed(stopped));
        }
    }
}

/// Take one step from `(w, m)`. A coordinator step runs its script, and
/// adds the scripts that choose otherwise to `others`.
///
/// The production [`Coordinator`] decides, and the glue it replaced
/// ([`glue::Glue`]) takes the same step from the same state with the same
/// script: the equivalence oracle. Both must read the token in the same
/// order, take the same effects, publish the same result and lead to the
/// same state (before anything is forgotten). Every state is reached so,
/// so they reach the same states.
fn take(
    w: &World,
    m: Mach,
    step: Step,
    cfg: Config,
    tables: &mut Tables,
    seen: &mut Observer,
    others: &mut Vec<Script>,
) -> (World, Mach) {
    let before = *w;
    let mut w = *w;
    if step == Step::Poll {
        w.polled = true;
        return (w, m);
    }
    let Some((_, script)) = step.script() else {
        take_env(&mut w, m.retired, step, cfg, tables);
        w.forget(m.data_loss != 0);
        w.forget_reasons(m.cancel_broadcast, tables);
        return (w, m);
    };
    seen.outputs.clear();
    let mut cursor = Cursor::new(script);
    let mut coordinator = Coordinator::restore(&w, m.machine(tables));
    driver::take(
        &mut w,
        &mut coordinator,
        step,
        cfg,
        tables,
        &mut cursor,
        seen,
    );
    coordinator.store(&mut w);
    let after = Mach::of(coordinator.machine(), tables);
    oracle((&before, m), step, cfg, tables, seen, (&w, after, &cursor));
    let m = after;
    w.forget(m.data_loss != 0);
    others.extend(cursor.others);
    w.forget_reasons(m.cancel_broadcast, tables);
    let tearing_down = w.finished
        || w.tearing_down
        || w.pending
            .iter()
            .any(|act| matches!(act, Act::Teardown { .. }));
    assert_eq!(
        tearing_down,
        m.phase == Phase::Finished,
        "torn down exactly when the machine finished"
    );
    (w, m)
}

/// The equivalence oracle: the glue [`Coordinator`] replaced takes `step`
/// from `from` with the same script, and must have done what `took` records
/// the coordinator did: the same outputs (`seen.outputs`), the same state and
/// the same other scripts.
fn oracle(
    from: (&World, Mach),
    step: Step,
    cfg: Config,
    tables: &mut Tables,
    seen: &Observer,
    took: (&World, Mach, &Cursor),
) {
    let (w2, m2, cursor2) = took;
    let mut w = *from.0;
    let script = step.script().expect("a coordinator step").1;
    let mut cursor = Cursor::new(script);
    let mut oracle = Observer::default();
    let mut glue = glue::Glue::restore(&w, from.1.machine(tables));
    driver::take(
        &mut w,
        &mut glue,
        step,
        cfg,
        tables,
        &mut cursor,
        &mut oracle,
    );
    glue.store(&mut w);
    let m = Mach::of(glue.machine(), tables);
    assert!(
        oracle.outputs == seen.outputs && w == *w2 && m == m2 && cursor.others == cursor2.others,
        "{cfg}: the coordinator and the glue it replaced differ on {}:\n\
         coordinator: {:?}\n\
         glue:        {:?}\n\
         states equal: {}, machines equal: {}, other scripts: {:?} and {:?}",
        step.text(),
        seen.outputs,
        oracle.outputs,
        w == *w2,
        m == m2,
        cursor2.others,
        cursor.others,
    );
}
// ---------------------------------------------------------------------------
// The search
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Property {
    Termination,
    NoLossWithoutCause,
    NoFalseSettle,
    NoOkAfterFailedSend,
    NoOkOverKnownLoss,
    NoOkOverInboundLoss,
}

impl Property {
    /// Must hold. The others may fail, as [`KNOWN_FAILURES`] lists.
    fn asserted(self) -> bool {
        matches!(self, Self::Termination | Self::NoLossWithoutCause)
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
    /// Runs in which records addressed to us were lost on their way in, or
    /// corrupt traffic reached us.
    inbound_loss_runs: u64,
    /// States where nothing more is bound to happen.
    quiescent: u64,
    /// States where the peer has gone and the machine still runs.
    peer_gone_running: u64,
}

struct Search {
    tables: Tables,
    /// Every state reached. The start is state 0.
    states: States,
    stats: Stats,
    seen: Observer,
    /// The first finding of each way to break a property.
    found: Vec<Finding>,
    violations: BTreeMap<Property, Violations>,
    /// The identities this configuration's golden file lists, by property,
    /// if they are checked; and the first finding of each property whose
    /// identity is not among them (a new counterexample), breadth first, so
    /// a shortest one.
    golden: Option<BTreeMap<String, BTreeSet<u64>>>,
    unexpected: Vec<Finding>,
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

/// FNV-1a: a clause's text, as part of an identity.
fn text_hash(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// What an identity is hashed from: the environment's side of a violating
/// state, and the clause it broke.
fn identity_words(env: u64, what: &str) -> [u64; WORDS] {
    let mut words = [0; WORDS];
    words[0] = env;
    words[1] = text_hash(what);
    words
}

impl Search {
    fn trace(&self, mut at: u32, last: Option<Step>) -> Vec<Step> {
        let mut trace: Vec<Step> = last.into_iter().collect();
        while at != 0 {
            let (from, step) = self.states.trail(at);
            trace.push(step);
            at = from;
        }
        trace.reverse();
        trace
    }

    /// Count a violation, found in `w` (or on a step from it). Keep the first
    /// finding of each way to break a property, and of each property's
    /// identities that its golden file does not list: breadth first, a
    /// shortest one.
    fn record(
        &mut self,
        property: Property,
        what: &'static str,
        w: &World,
        at: u32,
        last: Option<Step>,
    ) {
        let identity = mix(identity_words(w.identity(), what));
        let violations = self.violations.entry(property).or_default();
        violations.count += 1;
        violations.identities.insert(identity);
        if !self.found.iter().any(|finding| finding.what == what) {
            let trace = self.trace(at, last);
            self.found.push(Finding {
                property,
                what,
                trace,
            });
        }
        let unexpected = self.golden.as_ref().is_some_and(|golden| {
            !golden
                .get(&format!("{property:?}"))
                .is_some_and(|identities| identities.contains(&golden_identity(identity)))
        });
        if unexpected
            && !self
                .unexpected
                .iter()
                .any(|finding| finding.property == property)
        {
            let trace = self.trace(at, last);
            self.unexpected.push(Finding {
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
        self.stats.inbound_loss_runs += u64::from(w.inbound_lost || w.corrupt_in);
        let loss = m.data_loss != 0;
        if loss {
            self.stats.loss_runs += 1;
            if !(w.writer_dead || w.inbound_lost || w.corrupt_in) {
                self.record(
                    Property::NoLossWithoutCause,
                    "the run reports a data loss although no send to the peer failed, no \
                     records were lost on their way in and no corrupt traffic reached us",
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
        if w.inbound_lost || w.corrupt_in {
            let what = if w.late.is_some() {
                "the run ends Ok although records addressed to us were lost on their way in, or \
                 corrupt traffic reached us, before teardown began, and the data bridge had \
                 still not reported it when teardown published its result"
            } else if w.corrupt_unheard {
                "the run ends Ok although corrupt traffic reached us before teardown began: the \
                 progress bridge that held it was cancelled before it read it"
            } else {
                "the run ends Ok although records addressed to us were lost on their way in, or \
                 corrupt traffic reached us, before teardown began, and the coordinator was told"
            };
            self.record(Property::NoOkOverInboundLoss, what, w, at, None);
        }
    }

    /// Roughly the bytes the search's tables hold.
    fn memory(&self) -> usize {
        self.states.bytes()
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
/// once a property that must hold is broken. With `golden`, the identities
/// a golden file lists by property, also keeps a shortest trace to a
/// violation of each property whose identity it does not list.
fn explore(cfg: Config, golden: Option<BTreeMap<String, BTreeSet<u64>>>) -> Report {
    let started = Instant::now();
    let mut search = Search {
        tables: Tables::new(),
        states: States::new(),
        stats: Stats::default(),
        seen: Observer::default(),
        found: Vec::new(),
        violations: BTreeMap::new(),
        golden,
        unexpected: Vec::new(),
        round_trips: 1 << 20,
    };
    let start = Key::pack(
        &World::new(cfg),
        &Mach::of(&initial(cfg), &mut search.tables),
    );
    search.states.insert(start, 0, Step::Apply(Script::NONE));
    // A level of the search: the states it reached first in as many steps.
    let mut level = 0..search.states.len();
    let mut enabled = Vec::new();
    let mut scripted = Vec::new();
    let mut others = Vec::new();
    let mut memory = 0;
    while !level.is_empty() {
        search.stats.widest_level = search.stats.widest_level.max(level.len());
        for at in level.clone() {
            let key = search.states.key(at);
            let (w, m) = key.unpack();
            search.stats.states += 1;
            steps(&w, &m, cfg, &search.tables, &mut enabled);
            search.check_state(&w, &m, &enabled, at);
            for &first in &enabled {
                // A coordinator step once for each script it can run.
                scripted.push(first);
                while let Some(step) = scripted.pop() {
                    search.stats.steps += 1;
                    let (w2, m2) = take(
                        &w,
                        m,
                        step,
                        cfg,
                        &mut search.tables,
                        &mut search.seen,
                        &mut others,
                    );
                    scripted.extend(others.drain(..).map(|script| step.with_script(script)));
                    if let Some(what) = search.seen.false_settle.take() {
                        search.record(Property::NoFalseSettle, what, &w, at, Some(step));
                    }
                    let key2 = Key::pack(&w2, &m2);
                    assert_ne!(key2, key, "{cfg}: {} changed nothing", step.text());
                    if search.states.insert(key2, at, step).is_none() {
                        continue;
                    }
                    if search.round_trips > 0 {
                        search.round_trips -= 1;
                        assert_eq!(key2.unpack(), (w2, m2), "{cfg}: packing lost something");
                        let again = Mach::of(&m2.machine(&search.tables), &mut search.tables);
                        assert_eq!(again, m2, "{cfg}: rebuilding the machine lost something");
                    }
                }
            }
        }
        memory = memory.max(search.memory());
        if search
            .found
            .iter()
            .any(|finding| finding.property.asserted())
        {
            break;
        }
        level = level.end..search.states.len();
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
        unexpected: search.unexpected,
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
    /// A shortest violation of each property whose identity the golden file
    /// does not list.
    unexpected: Vec<Finding>,
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
             send, {} over a known loss; {} runs with records lost on their way in or corrupt \
             traffic",
            s.quiescent,
            s.peer_gone_running,
            o.settled_failed,
            o.settled_excused,
            o.grace_owed,
            o.grace_finished_peer,
            s.ok_after_failed_send,
            s.ok_over_known_loss,
            s.inbound_loss_runs
        )?;
        for (property, violations) in &self.violations {
            writeln!(
                f,
                "  {property:?}: {} violations, {} identities",
                violations.count,
                violations.identities.len()
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
    let mut tables = Tables::new();
    let mut seen = Observer {
        narrate: true,
        ..Observer::default()
    };
    let mut w = World::new(cfg);
    let mut m = Mach::of(&initial(cfg), &mut tables);
    let mut lines = Vec::new();
    for (i, &step) in trace.iter().enumerate() {
        let before = w;
        (w, m) = take(
            &before,
            m,
            step,
            cfg,
            &mut tables,
            &mut seen,
            &mut Vec::new(),
        );
        let mut line = format!(
            "{:>2}. {}",
            i + 1,
            describe(step, &before, &w, cfg, &tables)
        );
        if (before.tearing_down || before.finished)
            && matches!(
                step,
                Step::Deliver
                    | Step::DeliverCancel
                    | Step::WriteFails
                    | Step::PeerFin
                    | Step::PeerGone
                    | Step::WriteError
            )
        {
            line.push_str(" (after teardown began: nobody hears of it)");
        }
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

fn describe(step: Step, before: &World, after: &World, cfg: Config, tables: &Tables) -> String {
    match step {
        Step::TokenSeen(_) => format!(
            "the coordinator sees the dataflow token cancelled ({}) and applies what is queued \
             first",
            if tables.is_failure(before.token.expect("cancelled")) {
                Kind::Fail
            } else {
                Kind::Stop
            }
        ),
        Step::AbortSeen(_) => "the coordinator sees the bridges cancelled from outside".into(),
        Step::Apply(_) => "the coordinator takes the next event".into(),
        Step::GraceEnds(_) => "the grace period ends".into(),
        Step::Poll => {
            "the coordinator finds no event and polls the grace deadline, which has passed".into()
        }
        Step::TeardownEnds(_) => "teardown's bounded waits are over".into(),
        Step::Resume(_) => format!(
            "the driver's broadcast is done; it takes the actions left ({})",
            actions_text(
                &before
                    .pending
                    .iter()
                    .map(|act| act.action(tables))
                    .collect::<Vec<_>>()
            )
        ),
        Step::Finish(outcome) => format!(
            "a worker {}{}",
            match outcome {
                Outcome::Ok => "succeeds",
                Outcome::Failed => "fails",
                Outcome::Cancelled => "reports a cancellation",
            },
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
        Step::OtherWriteFails => {
            "a write that is not ours fails on our lane's connection (an RTT probe, another \
             dataflow's frame): the transport fails our lane"
                .into()
        }
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
        Step::ForwardAheadOfEnd => {
            let notice = before
                .io
                .iter()
                .find(|notice| !matches!(notice, Notice::InboundEnded { .. }))
                .expect("queued after the end");
            format!(
                "the io forwarder reports {notice} as {}, ahead of the end of the read side \
                 queued before it, which the transport hands on from a task of its own",
                notice.event()
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
        Step::PeerAnnounces => {
            "the peer announces that its part is unfinished, with its failure".into()
        }
        Step::PeerFin => "the peer closes its side of the connection".into(),
        Step::PeerGone => "the peer goes away: our next write fails".into(),
        Step::WriteError if !before.writing() => {
            "fault: a write that is not ours fails on our lane's connection (an RTT probe, \
             another dataflow's frame): the transport fails our lane"
                .into()
        }
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
        Step::LateRecord { late } => {
            format!("fault: the data bridge receives {late}, and holds its report")
        }
        Step::CorruptFrame => {
            "fault: the peer's next frame to us, on progress channel 0, is corrupt".into()
        }
        Step::ReportLate => format!(
            "the data bridge reports {}",
            after.queue.iter().last().expect("reported")
        ),
    }
}

// ---------------------------------------------------------------------------
// Known failures and the tests
// ---------------------------------------------------------------------------

/// A property main breaks, in which wirings, and why, in one line.
struct Known {
    property: Property,
    wirings: &'static [Wiring],
    faults: &'static [Faults],
    why: &'static str,
}

/// What main breaks, and why. A test fails unless the properties it finds
/// broken are exactly those listed for its wiring, so a fix has to update
/// this list (and the golden files, see [`GOLDEN`]).
const KNOWN_FAILURES: &[Known] = &[
    Known {
        property: Property::NoFalseSettle,
        wirings: &[Wiring::Shared, Wiring::SharedSplit],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "a send fails (ours, or a write on our lane's connection that is not ours) and the \
              peer's end-of-stream is applied after its ControlClosed: nothing settles the send \
              then (only ControlClosed or a link failure does), so the grace period holds a loss \
              although the peer said goodbye and its link closed",
    },
    Known {
        property: Property::NoFalseSettle,
        wirings: &[Wiring::Dedicated],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "while draining, the forwarder's refusal of our goodbye is a second SendFailed, \
              which settles the first at once as a loss although the peer's Cancel is on its \
              way; the grace period also holds a loss after the peer's goodbye, as in Shared mode",
    },
    Known {
        property: Property::NoFalseSettle,
        wirings: &[Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Transport],
        why: "the timer race: select! finds no event, the peer's Cancel is queued next, and it \
              still takes the expired grace deadline, which settles the failed send as a link \
              failure with the Cancel waiting in the channel",
    },
    Known {
        property: Property::NoOkAfterFailedSend,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "a write fails before the last worker succeeds (a fault, or the peer has gone; in \
              Shared mode also a write that is not ours), but its SendFailed is applied after \
              that success: draining takes it for the goodbye's failure, marks the link lost and \
              ends Ok",
    },
    Known {
        property: Property::NoOkAfterFailedSend,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "the bridges cancelled from outside tear down while a worker still runs, and \
              teardown applies no SendFailed: a write that failed before, its SendFailed still \
              queued, or one of what we queued that the transport still writes and that fails \
              after (the peer has gone, or a write error), is heard by nobody, and the worker's \
              later success ends Ok",
    },
    Known {
        property: Property::NoOkOverKnownLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "as for NoOkAfterFailedSend: the records were still queued when the write failed, so \
              they never reached the peer, and the run ends Ok",
    },
    Known {
        property: Property::NoOkOverKnownLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Transport, Faults::Inbound],
        why: "as for NoOkAfterFailedSend after a teardown from outside: our records had not \
              reached the peer when that write failed",
    },
    Known {
        property: Property::NoOkOverInboundLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Inbound],
        why: "teardown waits for the inbound data bridges only for a bound, and applies only what \
              is queued once it is over: a bridge that reports later is not heard",
    },
    Known {
        property: Property::NoOkOverInboundLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Inbound],
        why: "teardown cancels the progress bridges first, and they take the cancellation ahead \
              of what is queued for them: a corrupt frame that reached one is never read",
    },
    Known {
        property: Property::NoOkOverInboundLoss,
        wirings: &[Wiring::Shared, Wiring::SharedSplit, Wiring::Dedicated],
        faults: &[Faults::Inbound],
        why: "the bridges cancelled from outside tear down before every worker has finished: a \
              loss held while the dataflow was cancelled (it counts only if every worker still \
              succeeds) is dropped, and teardown ignores lost records queued for it, so a \
              worker's later success ends Ok",
    },
];

/// The golden files, one per configuration: `exhaustive/<configuration>.golden`
/// next to this file, kept as `tests/progress_conformance.golden` is. For
/// each property the configuration breaks, a summary line
/// `property | violations | identities | trace`: how many violations the
/// search found (states; steps for NoFalseSettle), how many distinct
/// identities they have, and the shortest trace the search finds first. Then
/// the identities themselves ([`World::identity`] with the clause broken,
/// truncated to 48 bits), sorted, under a `[property]` header. A test fails
/// unless it finds exactly what its golden file lists, and says whether it
/// found an identity the file does not list (a new counterexample, with a
/// shortest trace to the first one of each property) or no longer finds one
/// it lists: a fix can then show that it only removes violations.
///
/// The files only shrink. A test run with [`GOLDEN_VAR`] set takes out of its
/// file the identities it no longer finds, and still fails on a new one: a
/// new counterexample is a regression to fix. Only a change to the model may
/// add identities, and it bumps [`MODEL_VERSION`]; CI rejects a pull request
/// that adds one without that bump. A change to what the environment's side
/// of a state holds changes every identity, and the files are then rewritten
/// whole. They are read when a test runs, so they can be updated without a
/// rebuild. They are never edited by hand, and every commit that changes one
/// cites, in its message, the [`KNOWN_FAILURES`] entry or the fix that
/// explains each change.
const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/runtime/cluster_lifecycle/exhaustive"
);

/// Set to make a test rewrite its configuration's golden file ([`Rewrite`]):
/// to `reset` to write whatever it finds, to anything else to only take
/// identities out.
const GOLDEN_VAR: &str = "INSTANCY_EXHAUSTIVE_GOLDEN";

/// The model's version: the environment, the driver's scheduling, the
/// configurations and the properties as this module writes them, which decide
/// what a counterexample is. CI's `golden-files` job accepts identities added
/// to the golden files only in a pull request that changes this number. Bump
/// it with a change to the model, never to record a regression.
const MODEL_VERSION: u32 = 1;

/// What a test does with its golden file, as [`GOLDEN_VAR`] says.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rewrite {
    /// Compare: fail on any difference.
    No,
    /// Take out the identities no longer found, but fail on a new one.
    Shrink,
    /// Write whatever is found, for a change to the model.
    Reset,
}

/// The path of the golden file of the configuration called `name`.
fn golden_path(name: &str) -> String {
    format!("{GOLDEN}/{name}.golden")
}

/// What a configuration breaks: a summary line and the identities of each
/// broken property.
#[derive(Default, PartialEq, Eq)]
struct Golden {
    summary: BTreeMap<String, String>,
    identities: BTreeMap<String, BTreeSet<u64>>,
}

/// An identity as a golden file lists it: its low 48 bits. Two violations of
/// a property in a configuration then share an identity by chance with
/// probability about `n / 2^48` (thousands of identities: below one in ten
/// billion).
fn golden_identity(identity: u64) -> u64 {
    identity & ((1 << 48) - 1)
}

impl Golden {
    /// What a search found, as a golden file lists it.
    fn of(report: &Report) -> Self {
        let mut golden = Self::default();
        for (property, violations) in &report.violations {
            let finding = report
                .found
                .iter()
                .find(|finding| finding.property == *property)
                .expect("a finding for each violated property");
            let trace: Vec<String> = finding.trace.iter().map(|step| step.text()).collect();
            let identities: BTreeSet<u64> = violations
                .identities
                .iter()
                .map(|&identity| golden_identity(identity))
                .collect();
            let name = format!("{property:?}");
            golden.summary.insert(
                name.clone(),
                format!(
                    "{name} | {} | {} | {}",
                    violations.count,
                    identities.len(),
                    trace.join(", ")
                ),
            );
            golden.identities.insert(name, identities);
        }
        golden
    }

    /// A golden file written by [`Self::render`].
    fn parse(text: &str) -> Self {
        let mut golden = Self::default();
        let mut section: Option<String> = None;
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with("//") {
                continue;
            }
            if let Some(property) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
                golden.identities.entry(property.to_string()).or_default();
                section = Some(property.to_string());
            } else if let Some(property) = &section {
                let identity = u64::from_str_radix(line, 16)
                    .unwrap_or_else(|error| panic!("identity {line:?} in a golden file: {error}"));
                golden
                    .identities
                    .get_mut(property)
                    .expect("its section")
                    .insert(identity);
            } else {
                let property = line.split('|').next().map_or("", str::trim);
                golden
                    .summary
                    .insert(property.to_string(), line.to_string());
            }
        }
        golden
    }

    fn render(&self, name: &str) -> String {
        let mut text = format!(
            "// Golden file of runtime::cluster_lifecycle::exhaustive, configuration {name}\n\
             // (see `GOLDEN` there): how it breaks the properties KNOWN_FAILURES lists.\n\
             // Written by a test run with {GOLDEN_VAR} set; do not edit by\n\
             // hand. Every change to this file must cite, in its commit message, the\n\
             // KNOWN_FAILURES entry or the fix that explains it.\n\
             //\n\
             // property | violations | identities | shortest trace\n"
        );
        for line in self.summary.values() {
            let _ = writeln!(text, "{line}");
        }
        for (property, identities) in &self.identities {
            let _ = write!(text, "\n[{property}]\n");
            for identity in identities {
                let _ = writeln!(text, "{identity:012x}");
            }
        }
        text
    }

    /// Whether `found` has an identity this golden file does not list.
    fn grows(&self, found: &Self) -> bool {
        found.identities.iter().any(|(property, now)| {
            self.identities
                .get(property)
                .is_none_or(|listed| !now.is_subset(listed))
        })
    }

    /// How `found` differs from this golden file, for a failure.
    fn changes(&self, found: &Self) -> Vec<String> {
        let properties: BTreeSet<&String> = self
            .summary
            .keys()
            .chain(found.summary.keys())
            .chain(self.identities.keys())
            .chain(found.identities.keys())
            .collect();
        let empty = BTreeSet::new();
        let identities = |n: usize| if n == 1 { "identity" } else { "identities" };
        let mut changes = Vec::new();
        for property in properties {
            let listed = self.identities.get(property).unwrap_or(&empty);
            let now = found.identities.get(property).unwrap_or(&empty);
            let new: Vec<String> = now
                .difference(listed)
                .map(|identity| format!("{identity:012x}"))
                .collect();
            if !new.is_empty() {
                changes.push(format!(
                    "{property}: {} {} not in the golden file, a new counterexample (first: {})",
                    new.len(),
                    identities(new.len()),
                    new[..new.len().min(4)].join(", ")
                ));
            }
            let gone = listed.difference(now).count();
            if gone > 0 {
                changes.push(format!(
                    "{property}: {gone} {} in the golden file no longer found (fewer violations)",
                    identities(gone)
                ));
            }
            let (was, is) = (self.summary.get(property), found.summary.get(property));
            if was != is {
                changes.push(format!(
                    "{property}: summary `{}` in the golden file, found `{}`",
                    was.map_or("(none)", String::as_str),
                    is.map_or("(none)", String::as_str)
                ));
            }
        }
        changes
    }
}
/// Every text a coordinator step can broadcast from `(w, m)`, whatever the
/// token does before each of its reads.
fn broadcasts(
    w: &World,
    m: Mach,
    step: Step,
    cfg: Config,
    tables: &mut Tables,
) -> BTreeSet<String> {
    let mut texts = BTreeSet::new();
    let mut scripts = vec![step];
    while let Some(step) = scripts.pop() {
        let mut seen = Observer {
            narrate: true,
            ..Observer::default()
        };
        let mut others = Vec::new();
        take(w, m, step, cfg, tables, &mut seen, &mut others);
        scripts.extend(others.into_iter().map(|script| step.with_script(script)));
        texts.extend(seen.effects.into_iter().filter_map(|effect| match effect {
            Effect::Broadcast(text) => Some(text),
            _ => None,
        }));
    }
    texts
}

/// The state `trace` leads to from the start, each of its steps enabled where
/// it is taken (the coordinator's with the token left alone), and the
/// texts broadcast on the way.
fn reach(cfg: Config, trace: &[Step]) -> (World, Mach, Tables, Vec<String>) {
    let mut tables = Tables::new();
    let mut w = World::new(cfg);
    let mut m = Mach::of(&initial(cfg), &mut tables);
    let mut enabled = Vec::new();
    let mut seen = Observer {
        narrate: true,
        ..Observer::default()
    };
    for &step in trace {
        steps(&w, &m, cfg, &tables, &mut enabled);
        assert!(
            enabled.contains(&step),
            "{} is not enabled after {:?}",
            step.text(),
            trace.iter().map(|step| step.text()).collect::<Vec<_>>()
        );
        (w, m) = take(&w, m, step, cfg, &mut tables, &mut seen, &mut Vec::new());
    }
    let texts = seen
        .effects
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Broadcast(text) => Some(text),
            _ => None,
        })
        .collect();
    (w, m, tables, texts)
}

/// The token changes between the coordinator's reads of it. A worker fails
/// while the token is not cancelled: the broadcast finds no reason, the
/// token is cancelled next, and the broadcast says only "cancelled". After a
/// requested stop was announced, a worker reports its cancellation:
/// `AnnounceUnfinished` reads the escalation twice, and the peer's failure
/// (its second `Cancel`, after its own requested stop) can be escalated
/// before either read or between them.
#[test]
fn the_token_changes_between_reads() {
    let cfg = Config {
        wiring: Wiring::Shared,
        workers: 1,
        channels: 1,
        faults: Faults::Transport,
    };
    let (w, m, mut tables, _) = reach(cfg, &[Step::Finish(Outcome::Failed)]);
    let texts = broadcasts(&w, m, Step::Apply(Script::NONE), cfg, &mut tables);
    assert!(texts.contains("cancelled"), "{texts:?}");

    let (w, m, mut tables, _) = reach(
        cfg,
        &[
            Step::LocalCancel(Kind::Stop),
            Step::TokenSeen(Script::NONE),
            Step::PeerCancel(Kind::Stop),
            Step::PeerAnnounces,
            Step::Read,
            Step::Read,
            Step::Finish(Outcome::Cancelled),
        ],
    );
    assert_eq!(w.ctrl.iter().collect::<Vec<_>>(), [Kind::Stop, Kind::Fail]);
    let texts = broadcasts(&w, m, Step::Apply(Script::NONE), cfg, &mut tables);
    let stop = CancellationReason::UserRequested.to_string();
    let failure = tables.reason(Why::PEER_FAILURE).to_string();
    let unfinished = "(the node stopped before finishing its part of the dataflow)";
    assert_eq!(
        texts,
        BTreeSet::from([
            format!("{stop} {unfinished}"),
            failure.clone(),
            format!("{failure} {unfinished}"),
        ])
    );
}

/// The peer stops, and our `Cancel` says so; then it announces that its
/// part is unfinished, with its failure, which the control dispatcher
/// escalates the token with; our workers stop, and our `AnnounceUnfinished`
/// forwards that failure.
#[test]
fn the_peer_s_failure_after_its_stop_is_forwarded() {
    let cfg = Config {
        wiring: Wiring::Shared,
        workers: 1,
        channels: 1,
        faults: Faults::Transport,
    };
    let (_, _, tables, texts) = reach(
        cfg,
        &[
            Step::PeerCancel(Kind::Stop),
            Step::Read,
            Step::Dispatch,
            Step::DispatchReport,
            Step::TokenSeen(Script::NONE),
            Step::PeerAnnounces,
            Step::Read,
            Step::Dispatch,
            Step::DispatchReport,
            Step::Apply(Script::NONE),
            Step::Finish(Outcome::Cancelled),
            Step::Apply(Script::NONE),
        ],
    );
    assert_eq!(
        texts,
        [Why::PEER_STOP, Why::PEER_FAILURE].map(|why| tables.reason(why).to_string())
    );
}

/// The data bridge reports corrupt traffic it buffered after the connection
/// it came on has ended, and after the coordinator applied that end (a soft
/// failure, waiting for the rest of the link): while the workers still run,
/// it is fatal at once.
#[test]
fn the_data_bridge_reports_after_the_connection_ends() {
    let cfg = Config {
        wiring: Wiring::Dedicated,
        workers: 1,
        channels: 1,
        faults: Faults::Inbound,
    };
    let (w, m, _, texts) = reach(
        cfg,
        &[
            Step::LateRecord {
                late: Late::Corrupt,
            },
            Step::PeerFin,
            Step::ReaderEof,
            Step::ForwardNotice,
            Step::Apply(Script::NONE),
            Step::ReportLate,
            Step::Apply(Script::NONE),
        ],
    );
    assert!(w.timer, "the connection's end armed the grace deadline");
    assert!(m.phase == Phase::Running && m.data_loss != 0);
    assert_eq!(texts.len(), 1, "{texts:?}");
}

/// Shared mode, with no fault: the peer has our records, says goodbye and
/// goes away, and a write on our lane's connection that is not ours (a
/// probe, another dataflow's frame) then fails our lane. Its `WriteFailed`
/// reaches the coordinator ahead of the end of the read side, whose notice
/// the transport hands on from a task of its own. The send is owed, the
/// link retires on the peer's end-of-stream, and the grace period holds a
/// loss: the run reports one (NoFalseSettle).
#[test]
fn a_write_that_is_not_ours_fails_our_lane_after_the_peer_has_gone() {
    let cfg = Config {
        wiring: Wiring::Shared,
        workers: 1,
        channels: 1,
        faults: Faults::Inbound,
    };
    let none = Script::NONE;
    let (w, m, _, _) = reach(
        cfg,
        &[
            Step::Deliver,
            Step::PeerGoodbye,
            Step::Read,
            Step::PeerFin,
            Step::PeerGone,
            Step::ReaderEof,
            Step::OtherWriteFails,
            Step::ForwardAheadOfEnd,
            Step::Apply(none),
            Step::ForwardNotice,
            Step::Apply(none),
            Step::DispatchClosed,
            Step::Apply(none),
            Step::Bridge(0),
            Step::Apply(none),
            Step::Poll,
            Step::GraceEnds(none),
            Step::Finish(Outcome::Ok),
            Step::Apply(none),
        ],
    );
    assert!(!w.fault && w.records_delivered && w.peer_goodbye && w.writer_dead);
    assert!(m.retired && m.data_loss != 0);
}

/// The bridges are cancelled from outside while our worker runs: teardown
/// begins with our records still in the transport. The peer then goes away,
/// so writing them fails, which nobody hears, and the worker succeeds: the
/// run ends `Ok` although its send had failed and its records never reached
/// the peer (NoOkAfterFailedSend, NoOkOverKnownLoss).
#[test]
fn a_write_that_fails_after_teardown_counts_when_the_worker_succeeds() {
    let cfg = Config {
        wiring: Wiring::Dedicated,
        workers: 1,
        channels: 1,
        faults: Faults::Inbound,
    };
    let none = Script::NONE;
    let (w, m, _, _) = reach(
        cfg,
        &[
            Step::LocalCancel(Kind::Stop),
            Step::ExternalAbort,
            Step::TokenSeen(none),
            Step::AbortSeen(none),
            Step::PeerFin,
            Step::PeerGone,
            Step::WriteFails,
            Step::Finish(Outcome::Ok),
            Step::TeardownEnds(none),
        ],
    );
    assert!(w.finished && !w.workers_failed && m.data_loss == 0);
    assert!(w.failed_at_finish && w.lost_at_finish);
}

/// One decision point of a two-peer run, for
/// [`the_coordinator_decides_like_the_glue_with_two_peers`].
#[derive(Clone, Copy, Debug)]
enum TwoPeerOp {
    /// An event about peer 0 or 1 (or a worker's result) is queued.
    Post(Ev, usize),
    /// The driver takes the next queued event.
    Deliver,
    /// The dataflow is cancelled locally.
    Cancel(Kind),
    /// The control dispatcher escalates the token with a peer's failure.
    Escalate,
    /// The token's branch is taken, if it can be.
    TokenSeen,
    /// The bridges are cancelled from outside.
    Abort,
    /// Every sender of the event channel is gone.
    EventsClosed,
    /// The grace deadline of peer 0 or 1 passes.
    Grace(usize),
    /// Teardown's bounded waits are over: it applies what is queued, in
    /// order, and publishes its result.
    TeardownEnds,
}

const TWO_PEERS: [&str; 2] = ["a", "b"];

/// The dataflow token as a two-peer run's coordinator reads it: every read
/// is logged, with what it returned.
struct LoggedToken<'a> {
    token: &'a CancellationToken,
    log: &'a std::cell::RefCell<Vec<String>>,
}

impl TokenReader for LoggedToken<'_> {
    fn is_cancelled(&self) -> bool {
        let read = self.token.is_cancelled();
        self.log.borrow_mut().push(format!("is_cancelled: {read}"));
        read
    }

    fn reason(&self) -> Option<CancellationReason> {
        let read = self.token.reason();
        self.log.borrow_mut().push(format!("reason: {read:?}"));
        read
    }

    fn escalation(&self) -> Option<CancellationReason> {
        let read = self.token.escalation();
        self.log.borrow_mut().push(format!("escalation: {read:?}"));
        read
    }
}

/// One implementation of the coordinator's decisions in a two-peer run,
/// driven as `ClusterLifecycle::run` drives it, with its own token, and a
/// log of what it read and did.
struct TwoPeerRun<D> {
    d: D,
    token: CancellationToken,
    queue: VecDeque<LifecycleEvent>,
    cancel_seen: bool,
    events_closed: bool,
    /// Teardown began: whether it lets the inbound bridges drain.
    teardown: Option<bool>,
    done: bool,
    log: std::cell::RefCell<Vec<String>>,
}

impl<D: Decide> TwoPeerRun<D> {
    fn new(cfg: Config) -> Self {
        let machine = LifecycleMachine::new(
            usize::from(cfg.workers),
            TWO_PEERS.iter().enumerate().map(|(channels, peer)| {
                (
                    peer.to_string(),
                    channels + 1,
                    Arc::new(PeerLinkState::default()),
                )
            }),
        );
        Self {
            d: D::restore(&World::new(cfg), machine),
            token: CancellationToken::new(),
            queue: VecDeque::new(),
            cancel_seen: false,
            events_closed: false,
            teardown: None,
            done: false,
            log: std::cell::RefCell::default(),
        }
    }

    fn note(&self, what: String) {
        self.log.borrow_mut().push(what);
    }

    /// What the decisions remember besides the machine, as a [`World`]
    /// holds it.
    fn remembered(&self, cfg: Config) -> World {
        let mut w = World::new(cfg);
        self.d.store(&mut w);
        w
    }

    fn take(&mut self, op: TwoPeerOp) {
        if self.done {
            return;
        }
        let token = LoggedToken {
            token: &self.token,
            log: &self.log,
        };
        if let Some(drain_inbound) = self.teardown {
            match op {
                TwoPeerOp::Post(ev, peer) => self.queue.push_back(two_peer_event(ev, peer)),
                TwoPeerOp::TeardownEnds => {
                    while let Some(event) = self.queue.pop_front() {
                        self.d.on_late_event(event, drain_inbound);
                    }
                    let broadcast = self.d.unannounced_loss();
                    let published = self.d.machine().data_loss().cloned();
                    self.note(format!("teardown ends: {broadcast:?}, {published:?}"));
                    self.done = true;
                }
                _ => {}
            }
            return;
        }
        let actions = match op {
            TwoPeerOp::Post(ev, peer) => {
                self.queue.push_back(two_peer_event(ev, peer));
                return;
            }
            TwoPeerOp::Deliver => match self.queue.pop_front() {
                Some(event) => self.d.on_event(event, &token),
                None => return,
            },
            TwoPeerOp::Cancel(kind) => {
                self.token.cancel_with_reason(match kind {
                    Kind::Stop => CancellationReason::UserRequested,
                    Kind::Fail => CancellationReason::WorkerFailed {
                        detail: "a local worker failed".into(),
                    },
                });
                return;
            }
            TwoPeerOp::Escalate => {
                self.token.escalate(CancellationReason::PeerCancelled {
                    peer_id: TWO_PEERS[1].to_string(),
                    detail: "worker failed: the peer's worker failed".into(),
                });
                return;
            }
            TwoPeerOp::TokenSeen if self.token.is_cancelled() && !self.cancel_seen => {
                self.cancel_seen = true;
                self.d.begin_token_noticed();
                let mut actions = Vec::new();
                while let Some(event) = self.queue.pop_front() {
                    actions.extend(self.d.on_event(event, &token));
                }
                actions.extend(self.d.finish_token_noticed(&token));
                actions
            }
            TwoPeerOp::Abort => self.d.on_external_abort(),
            TwoPeerOp::EventsClosed if !self.events_closed => {
                self.events_closed = true;
                self.d.on_events_closed()
            }
            TwoPeerOp::Grace(peer) => self.d.on_soft_failure_expired(TWO_PEERS[peer].into()),
            _ => return,
        };
        for action in actions {
            let effect = self.d.prepare(&action, &token);
            self.note(format!("{effect:?}"));
            match effect {
                Effect::Escalate(reason) => self.token.escalate(reason),
                Effect::Teardown { .. } => {
                    let drain_inbound = self.d.teardown_drains_inbound();
                    self.note(format!("teardown drains inbound: {drain_inbound}"));
                    self.teardown = Some(drain_inbound);
                    return;
                }
                _ => {}
            }
        }
        let stopped = self.d.stopped_peers();
        if !stopped.is_empty() {
            self.note(format!("lanes closed: {stopped:?}"));
        }
    }
}

/// `ev` about peer `peer` (or a worker's result).
fn two_peer_event(ev: Ev, peer: usize) -> LifecycleEvent {
    match ev.event() {
        LifecycleEvent::Link { event, .. } => LifecycleEvent::Link {
            peer: TWO_PEERS[peer].to_string(),
            event,
        },
        event => event,
    }
}

/// Run `ops` with two peers, on the production [`Coordinator`] and on the
/// glue it replaced, each with its own token and links: after every step
/// they must have read the token alike (each read and its value), decided
/// alike (effects in order, lane closes, teardown's choices and result),
/// and hold the same machine and the same state besides. The coordinator's
/// log.
fn two_peers(cfg: Config, ops: &[TwoPeerOp]) -> Vec<String> {
    let mut coordinator = TwoPeerRun::<Coordinator>::new(cfg);
    let mut glue = TwoPeerRun::<glue::Glue>::new(cfg);
    for (i, &op) in ops.iter().enumerate() {
        coordinator.take(op);
        glue.take(op);
        assert!(
            coordinator.log == glue.log
                && format!("{:?}", coordinator.d.machine()) == format!("{:?}", glue.d.machine())
                && coordinator.remembered(cfg) == glue.remembered(cfg),
            "the coordinator and the glue it replaced differ after {:?}:\n\
             coordinator: {:?}\nglue:        {:?}",
            &ops[..=i],
            coordinator.log.borrow(),
            glue.log.borrow()
        );
    }
    coordinator.log.into_inner()
}

/// The equivalence oracle with two peers, where the explorer has one: a
/// coordinator with two peers (one progress channel from the first, two
/// from the second) and two local workers, on runs written out (a link
/// failure on each peer, both peers stopping, late reports from both at
/// teardown) and on seeded random ones. See [`two_peers`]. The token does
/// not change between the reads of one decision here: the explorer covers
/// that.
#[test]
fn the_coordinator_decides_like_the_glue_with_two_peers() {
    use TwoPeerOp::{Deliver, Post, TeardownEnds};
    const RUNS: usize = 10_000;
    const OPS: usize = 48;
    let cfg = Config {
        wiring: Wiring::Shared,
        workers: 2,
        channels: 1,
        faults: Faults::Transport,
    };
    let corrupt = Ev::ProgressCorrupt {
        after_end_of_stream: false,
    };
    let mut logs = vec![
        two_peers(cfg, &[Post(corrupt, 0), Deliver]),
        two_peers(cfg, &[Post(corrupt, 1), Deliver]),
        two_peers(
            cfg,
            &[
                Post(Ev::Cancel, 0),
                Post(Ev::Cancel, 0),
                Post(Ev::Cancel, 1),
                Post(Ev::Cancel, 1),
                Deliver,
                Deliver,
                Deliver,
                Deliver,
            ],
        ),
    ];
    let done = Ev::Done(Outcome::Ok);
    let late = two_peers(
        cfg,
        &[
            Post(done, 0),
            Post(done, 0),
            Deliver,
            Deliver,
            Post(Ev::EndOfStream, 0),
            Post(Ev::EndOfStream, 1),
            Post(Ev::EndOfStream, 1),
            Deliver,
            Deliver,
            Deliver,
            Post(Ev::Lost, 0),
            Post(Ev::DataCorrupt, 1),
            TeardownEnds,
        ],
    );
    assert!(
        late.last().is_some_and(
            |last| last.starts_with("teardown ends: Some") && last.contains("peer: \"a\"")
        ),
        "{late:?}"
    );
    logs.push(late);
    let events = [
        Ev::Done(Outcome::Ok),
        Ev::Done(Outcome::Failed),
        Ev::Done(Outcome::Cancelled),
        Ev::SendFailed,
        Ev::ConnectionEnded { clean: true },
        Ev::ConnectionEnded { clean: false },
        Ev::Cancel,
        Ev::ControlClosed,
        Ev::EndOfStream,
        Ev::Ack,
        Ev::ChannelClosed,
        Ev::Lost,
        Ev::DataCorrupt,
        corrupt,
    ];
    let healthy = [
        Ev::Done(Outcome::Ok),
        Ev::Done(Outcome::Cancelled),
        Ev::EndOfStream,
        Ev::Ack,
        Ev::ControlClosed,
    ];
    // xorshift64*: the same runs every time.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = |below: usize| {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let value = state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 33;
        usize::try_from(value).expect("31 bits") % below
    };
    for _ in 0..RUNS {
        // Mostly a healthy run or a requested stop, so that the peers say
        // goodbye and ack, or the workers stop.
        let ops: Vec<TwoPeerOp> = (0..OPS)
            .map(|_| match next(100) {
                0..=49 => {
                    let ev = match next(4) {
                        0 => events[next(events.len())],
                        _ => healthy[next(healthy.len())],
                    };
                    Post(ev, next(2))
                }
                50..=79 => Deliver,
                80..=81 => TwoPeerOp::Cancel(if next(2) == 0 { Kind::Stop } else { Kind::Fail }),
                82..=83 => TwoPeerOp::Escalate,
                84..=87 => TwoPeerOp::TokenSeen,
                88 => TwoPeerOp::Abort,
                89 => TwoPeerOp::EventsClosed,
                90..=93 => TwoPeerOp::Grace(next(2)),
                _ => TeardownEnds,
            })
            .collect();
        logs.push(two_peers(cfg, &ops));
    }
    // What the runs must do, each at least once.
    for kind in [
        "SendGoodbye",
        "SendAck",
        "Broadcast",
        "Silent",
        "ArmGrace",
        "link to peer a failed",
        "link to peer b failed",
        "lanes closed: [\"a\", \"b\"]",
        "teardown drains inbound: true",
        "teardown ends: Some",
        "escalation: Some",
    ] {
        assert!(
            logs.iter().flatten().any(|line| line.contains(kind)),
            "no run did {kind}"
        );
    }
}

fn check(wiring: Wiring, workers: u8, channels: u8, faults: Faults) {
    for known in KNOWN_FAILURES {
        assert!(
            !known.property.asserted()
                && !known.wirings.is_empty()
                && !known.faults.is_empty()
                && !known.why.is_empty(),
            "{:?}: a known failure needs a property that may fail, wirings, faults and a reason",
            known.property
        );
    }
    let cfg = Config {
        wiring,
        workers,
        channels,
        faults,
    };
    let path = golden_path(&cfg.name());
    let rewrite = match std::env::var_os(GOLDEN_VAR) {
        None => Rewrite::No,
        Some(value) if value == "reset" => Rewrite::Reset,
        Some(_) => Rewrite::Shrink,
    };
    let golden = (rewrite != Rewrite::Reset).then(|| {
        std::fs::read_to_string(&path)
            .map(|text| Golden::parse(&text))
            .unwrap_or_else(|error| {
                panic!(
                    "cannot read {path}: {error}; a new configuration is a change to the model: \
                     bump MODEL_VERSION and run the test with {GOLDEN_VAR}=reset to write it"
                )
            })
    });
    let report = explore(cfg, golden.as_ref().map(|file| file.identities.clone()));
    println!("{report}");
    let broken = report.broken();
    for property in [Property::Termination, Property::NoLossWithoutCause] {
        assert!(
            !broken.contains(&property),
            "{cfg}: {property:?} is broken (see the trace above)"
        );
    }
    let listed: BTreeSet<Property> = KNOWN_FAILURES
        .iter()
        .filter(|known| known.wirings.contains(&wiring) && known.faults.contains(&faults))
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
        "{cfg}: NoLossWithoutCause is vacuous: no run reports a data loss"
    );
    match faults {
        Faults::Transport => {
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
        }
        Faults::Inbound => assert!(
            s.inbound_loss_runs > 0,
            "{cfg}: NoOkOverInboundLoss is vacuous: no run loses records on their way in"
        ),
    }
    let found = Golden::of(&report);
    let Some(golden) = golden else {
        std::fs::write(&path, found.render(&cfg.name()))
            .unwrap_or_else(|error| panic!("cannot write {path}: {error}"));
        println!("{cfg}: golden file written to {path}");
        return;
    };
    let changes = golden.changes(&found);
    let grows = golden.grows(&found);
    if rewrite == Rewrite::Shrink && !grows {
        if !changes.is_empty() {
            std::fs::write(&path, found.render(&cfg.name()))
                .unwrap_or_else(|error| panic!("cannot write {path}: {error}"));
            println!(
                "{cfg}: golden file {path} rewritten:\n{}",
                changes.join("\n")
            );
        }
        return;
    }
    let mut new = String::new();
    for finding in &report.unexpected {
        let _ = writeln!(
            new,
            "A shortest new counterexample of {:?} ({}), {} steps:",
            finding.property,
            finding.what,
            finding.trace.len()
        );
        for line in narrate(cfg, &finding.trace) {
            let _ = writeln!(new, "  {line}");
        }
    }
    let advice = if grows {
        format!(
            "A new counterexample is a regression to fix. Only a change to the model may add one: \
             it bumps MODEL_VERSION (now {MODEL_VERSION}) and runs the test again with \
             {GOLDEN_VAR}=reset, and CI rejects added identities without that bump."
        )
    } else {
        format!(
            "If the change is meant, run the test again with {GOLDEN_VAR}=1 to update the file (it \
             only takes identities out), and cite, in the commit message, the KNOWN_FAILURES \
             entry or the fix that explains each change."
        )
    };
    assert!(
        changes.is_empty(),
        "{cfg}: the violations differ from the golden file {path}:\n{}\n{new}{advice}",
        changes.join("\n")
    );
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_one_worker_one_channel() {
    check(Wiring::Shared, 1, 1, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_one_worker_two_channels() {
    check(Wiring::Shared, 1, 2, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_two_workers_one_channel() {
    check(Wiring::Shared, 2, 1, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_two_workers_two_channels() {
    check(Wiring::Shared, 2, 2, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_split_one_worker_one_channel() {
    check(Wiring::SharedSplit, 1, 1, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_split_one_worker_two_channels() {
    check(Wiring::SharedSplit, 1, 2, Faults::Transport);
}

#[test]
#[ignore = "abstract and large (111 million states, about 5 GB): CI skips it; run it by hand in release (see the module docs)"]
fn shared_split_two_workers_one_channel() {
    check(Wiring::SharedSplit, 2, 1, Faults::Transport);
}

#[test]
#[ignore = "too large for CI (520 million states, about 20 minutes and 23 GB): run it by hand in release (see the module docs)"]
fn shared_split_two_workers_two_channels() {
    check(Wiring::SharedSplit, 2, 2, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn dedicated_one_worker_one_channel() {
    check(Wiring::Dedicated, 1, 1, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn dedicated_one_worker_two_channels() {
    check(Wiring::Dedicated, 1, 2, Faults::Transport);
}

#[test]
#[ignore = "abstract and large (69 million states, about 4 GB): CI skips it; run it by hand in release (see the module docs)"]
fn dedicated_two_workers_one_channel() {
    check(Wiring::Dedicated, 2, 1, Faults::Transport);
}

#[test]
#[ignore = "too large for CI (282 million states, about 11 minutes and 16 GB): run it by hand in release (see the module docs)"]
fn dedicated_two_workers_two_channels() {
    check(Wiring::Dedicated, 2, 2, Faults::Transport);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_one_worker_one_channel_inbound() {
    check(Wiring::Shared, 1, 1, Faults::Inbound);
}

#[test]
#[ignore = "large (135 million states, about 8 GB): CI skips it; run it by hand in release (see the module docs)"]
fn shared_one_worker_two_channels_inbound() {
    check(Wiring::Shared, 1, 2, Faults::Inbound);
}

#[test]
#[ignore = "abstract and large (123 million states, about 6 GB): CI skips it; run it by hand in release (see the module docs)"]
fn shared_two_workers_one_channel_inbound() {
    check(Wiring::Shared, 2, 1, Faults::Inbound);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn shared_split_one_worker_one_channel_inbound() {
    check(Wiring::SharedSplit, 1, 1, Faults::Inbound);
}

#[test]
#[ignore = "too large for CI (486 million states, about 18 minutes and 22 GB): run it by hand in release (see the module docs)"]
fn shared_split_one_worker_two_channels_inbound() {
    check(Wiring::SharedSplit, 1, 2, Faults::Inbound);
}

#[test]
#[ignore = "abstract and too large for CI (433 million states, about 16 minutes and 20 GB): run it by hand in release (see the module docs)"]
fn shared_split_two_workers_one_channel_inbound() {
    check(Wiring::SharedSplit, 2, 1, Faults::Inbound);
}

#[test]
#[ignore = "slow in a debug build: CI's lifecycle job runs it in release"]
fn dedicated_one_worker_one_channel_inbound() {
    check(Wiring::Dedicated, 1, 1, Faults::Inbound);
}

#[test]
#[ignore = "large (217 million states, about 10 GB): CI skips it; run it by hand in release (see the module docs)"]
fn dedicated_one_worker_two_channels_inbound() {
    check(Wiring::Dedicated, 1, 2, Faults::Inbound);
}

#[test]
#[ignore = "abstract and large (187 million states, about 9 GB): CI skips it; run it by hand in release (see the module docs)"]
fn dedicated_two_workers_one_channel_inbound() {
    check(Wiring::Dedicated, 2, 1, Faults::Inbound);
}
