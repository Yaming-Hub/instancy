//! The operator shell: the one [`SchedulableOperator`] of every built-in
//! operator (design §5.1.2).
//!
//! An [`OperatorShell`] owns all of an operator's port I/O, so the progress
//! accounting of every operator is implemented once, here:
//!
//! - a [`CountedInput`] buffers what its channel delivers. Data waiting there
//!   (or still in the channel) stays counted at the input. Taking a batch
//!   out for the logic ([`InputHandle::next`]) consumes it: the shell records
//!   `consumed[in] += (t, n)` when the logic call returns;
//! - a [`CountedOutput`] collects the logic's output in its
//!   [`OutputHandle`]. When the call returns, the shell takes that output,
//!   records `produced[out] += (t, n)` and moves it to the retry queue, from
//!   which it goes into the channel as capacity allows. Everything between
//!   the two ports is "on the edge", counted at the consuming input, with no
//!   accounting code in any channel;
//! - work the logic keeps across calls must be covered by a capability, and
//!   every output and capability acquisition must be justified (V3, see
//!   [`Justify`]).
//!
//! The logic ([`OperatorLogic`]) gets typed access to its ports but can only
//! take input batches, read frontiers and fill output sessions: the methods
//! that move data across a port need a [`ShellToken`], which only this
//! module can create.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::dataflow::stage::StageId;
use crate::error::{Error, ProgressError, Result};
use crate::progress::PendingOutput;
use crate::progress::capability::Capability;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::{PortLedger, ProgressReporter};
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// ShellToken
// ---------------------------------------------------------------------------

/// Proof that the caller is the operator shell. Only this module can create
/// one, so only the shell can move data across a counted port.
pub(crate) struct ShellToken(());

// ---------------------------------------------------------------------------
// Justify: the justification set J of one logic call (V3)
// ---------------------------------------------------------------------------

/// The justification set `J` of an operator's current logic call (design
/// §5.1.3, R36), shared by its ports and its notification context.
///
/// `J` exists only while the logic runs: `begin` takes the times of the
/// capabilities each output holds, every batch the logic takes from an
/// input adds its time, and `end` drops it again, before the call's counts
/// reach the ledger. An output batch, a capability acquisition or a
/// downgrade at `t` is justified iff some element of `J` is at or before
/// `t`. Capabilities acquired in the call are not added: each is itself at
/// or after an element of `J`, so they justify nothing new.
#[derive(Clone)]
pub(crate) struct Justify<T: Timestamp>(Arc<Mutex<JustifyState<T>>>);

struct JustifyState<T: Timestamp> {
    /// Each output's capability reporter (its held capabilities).
    reporters: Vec<Option<ProgressReporter<T>>>,
    /// Per output: the times of the capabilities held when the call began.
    held: Vec<Antichain<T>>,
    /// Times of the batches consumed in this call. Every built-in operator
    /// that checks V3 connects each input to each output with the identity
    /// summary.
    consumed: Antichain<T>,
    in_call: bool,
    /// The first violation of this call.
    violation: Option<Error>,
}

impl<T: Timestamp> Justify<T> {
    pub(crate) fn new(outputs: usize) -> Self {
        Self(Arc::new(Mutex::new(JustifyState {
            reporters: vec![None; outputs],
            held: vec![Antichain::new(); outputs],
            consumed: Antichain::new(),
            in_call: false,
            violation: None,
        })))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, JustifyState<T>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_reporter(&self, output: usize, reporter: ProgressReporter<T>) {
        let mut state = self.state();
        if output < state.reporters.len() {
            state.reporters[output] = Some(reporter);
        }
    }

    /// A logic call starts: `J` holds the capabilities held now.
    fn begin(&self) {
        let mut state = self.state();
        let held: Vec<Antichain<T>> = state
            .reporters
            .iter()
            .map(|r| r.as_ref().map(|r| r.held_frontier()).unwrap_or_default())
            .collect();
        state.held = held;
        state.consumed = Antichain::new();
        state.violation = None;
        state.in_call = true;
    }

    /// The call took a batch at `time` from `input`.
    pub(crate) fn consumed(&self, _input: usize, time: &T) {
        let mut state = self.state();
        if state.in_call {
            state.consumed.insert_ref(time);
        }
    }

    fn justifies_locked(state: &JustifyState<T>, output: usize, time: &T) -> bool {
        state.consumed.less_equal(time)
            || state.held.get(output).is_some_and(|h| h.less_equal(time))
    }

    fn describe(state: &JustifyState<T>, output: usize) -> String {
        let mut elements: Vec<String> = state
            .held
            .get(output)
            .map(|h| h.elements().iter().map(|t| format!("held {t:?}")).collect())
            .unwrap_or_default();
        elements.extend(
            state
                .consumed
                .elements()
                .iter()
                .map(|t| format!("consumed {t:?}")),
        );
        if elements.is_empty() {
            "nothing".to_string()
        } else {
            elements.join(", ")
        }
    }

    fn violation(state: &JustifyState<T>, what: &'static str, output: usize, time: &T) -> Error {
        Error::Progress(ProgressError::Unjustified {
            what: format!("{what} at {time:?} on output {output}"),
            justified: Self::describe(state, output),
        })
    }

    /// Check, without recording, that this call justifies a capability at
    /// `time` on `output`.
    pub(crate) fn check(&self, output: usize, time: &T, what: &'static str) -> Result<()> {
        let state = self.state();
        if !state.in_call || Self::justifies_locked(&state, output, time) {
            Ok(())
        } else {
            Err(Self::violation(&state, what, output, time))
        }
    }

    /// Check a capability acquisition or downgrade at `time` on `output`.
    /// A violation is also recorded, and fails the call when it returns.
    pub(crate) fn acquire(&self, output: usize, time: &T, what: &'static str) -> Result<()> {
        let mut state = self.state();
        if !state.in_call || Self::justifies_locked(&state, output, time) {
            return Ok(());
        }
        if state.violation.is_none() {
            let recorded = Self::violation(&state, what, output, time);
            state.violation = Some(recorded);
        }
        Err(Self::violation(&state, what, output, time))
    }

    /// Check an output batch at `time` on `output` (after the call).
    fn check_output(&self, output: usize, time: &T) -> Result<()> {
        let state = self.state();
        if Self::justifies_locked(&state, output, time) {
            Ok(())
        } else {
            Err(Self::violation(&state, "output", output, time))
        }
    }

    /// The call returned: its acquisitions' first violation, if any.
    fn finish_call(&self) -> Option<Error> {
        let mut state = self.state();
        state.in_call = false;
        state.violation.take()
    }

    /// `J` is dropped (after the call's output was checked).
    fn end(&self) {
        let mut state = self.state();
        state.consumed = Antichain::new();
        for held in &mut state.held {
            held.clear();
        }
    }
}

// ---------------------------------------------------------------------------
// Retained: a capability kept across logic calls
// ---------------------------------------------------------------------------

/// A capability on an operator output, returned by
/// [`OutputHandle::retain`](crate::dataflow::OutputHandle::retain).
///
/// While it exists, downstream frontiers cannot pass its time, and the
/// operator may emit at or after that time in any later call. It is
/// released when dropped, from any thread or task; dropping it wakes the
/// operator's executor, so the release is committed promptly.
pub struct Retained<T: Timestamp> {
    capability: Option<Capability<T>>,
    time: T,
    wake: Option<WakeHandle>,
    /// The output's count of live `Retained`s, while this one holds its
    /// capability.
    live: Option<Arc<AtomicUsize>>,
}

impl<T: Timestamp> Retained<T> {
    pub(crate) fn new(
        capability: Option<Capability<T>>,
        time: T,
        wake: Option<WakeHandle>,
        live: Option<Arc<AtomicUsize>>,
    ) -> Self {
        let live = live.filter(|_| capability.is_some());
        if let Some(live) = &live {
            live.fetch_add(1, Ordering::SeqCst);
        }
        Self {
            capability,
            time,
            wake,
            live,
        }
    }

    /// The time this capability holds.
    pub fn time(&self) -> &T {
        &self.time
    }
}

impl<T: Timestamp> Drop for Retained<T> {
    fn drop(&mut self) {
        if let Some(capability) = self.capability.take() {
            drop(capability);
            if let Some(live) = self.live.take() {
                live.fetch_sub(1, Ordering::SeqCst);
            }
            if let Some(wake) = &self.wake {
                wake.notify();
            }
        }
    }
}

impl<T: Timestamp> std::fmt::Debug for Retained<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retained")
            .field("time", &self.time)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// CountedInput
// ---------------------------------------------------------------------------

/// A counted input port: the channel's pull end, the logic's
/// [`InputHandle`] and the port's consumed ledger.
pub(crate) struct CountedInput<T: Timestamp, D: Send + 'static> {
    pull: Box<dyn Pull<T, D>>,
    handle: InputHandle<T, D>,
    ledger: PortLedger<T>,
}

impl<T: Timestamp, D: Send + 'static> CountedInput<T, D> {
    /// Built only during materialization (see
    /// [`ChannelEndpoints::take_input`](crate::dataflow::schedulable::ChannelEndpoints::take_input)).
    pub(in crate::dataflow) fn new(
        name: String,
        pull: Box<dyn Pull<T, D>>,
        ledger: PortLedger<T>,
    ) -> Self {
        #[cfg_attr(
            not(any(debug_assertions, feature = "progress-check")),
            allow(unused_mut)
        )]
        let mut handle = InputHandle::new(name);
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        handle.observe(&*pull);
        Self {
            pull,
            handle,
            ledger,
        }
    }

    /// The logic's view of the port.
    pub(crate) fn handle(&mut self) -> &mut InputHandle<T, D> {
        &mut self.handle
    }

    /// Whether data is waiting for the logic.
    pub(crate) fn has_pending(&self) -> bool {
        self.handle.has_pending()
    }

    /// Whether the channel has ended (no more data will arrive).
    pub(crate) fn is_exhausted(&self) -> bool {
        self.handle.is_exhausted()
    }

    /// The progress checker's port for this input.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_port(&self) -> Option<crate::progress::check::Port<T>> {
        self.pull.check_port()
    }

    /// Record `n` records at `time` consumed from this port: a credit taken
    /// with [`InputHandle::next_on_credit`] is redeemed.
    pub(crate) fn redeem(&mut self, credit: (T, i64)) {
        self.ledger.update(credit.0, credit.1);
    }

    /// Move what the channel delivered into the handle. An error envelope
    /// is fatal; a watermark is advisory and ignored.
    fn pull(&mut self, _: &ShellToken) -> Result<bool> {
        let mut pulled = false;
        while let Some(envelope) = self.pull.pull() {
            match envelope.payload {
                Payload::Data { time, data } => {
                    self.handle.push_vec(time, data);
                    pulled = true;
                }
                Payload::Control(ControlSignal::Watermark(_)) => {}
                Payload::Control(ControlSignal::Error {
                    source_operator,
                    message,
                }) => {
                    return Err(Error::Progress(ProgressError::UpstreamError {
                        source_operator,
                        message,
                    }));
                }
            }
        }
        if !self.handle.is_exhausted() && self.pull.is_exhausted() {
            self.handle.mark_exhausted();
        }
        Ok(pulled)
    }

    fn commit(&mut self, _: &ShellToken) {
        self.ledger.absorb(self.handle.take_consumed());
    }

    fn discard(&mut self, _: &ShellToken) {
        self.handle.take_consumed().clear();
    }
}

// ---------------------------------------------------------------------------
// CountedOutput
// ---------------------------------------------------------------------------

/// A counted output port: the logic's [`OutputHandle`], the retry queue,
/// the channel's push end (a tee for several consumers), the port's
/// produced ledger and its capability reporter.
pub(crate) struct CountedOutput<T: Timestamp, D: Send + 'static> {
    handle: OutputHandle<T, D>,
    retry: PendingOutput<T, D>,
    push: Box<dyn Push<T, D>>,
    ledger: PortLedger<T>,
    reporter: ProgressReporter<T>,
    wake: Option<WakeHandle>,
    /// How many `Retained`s of this output are alive.
    retained_live: Arc<AtomicUsize>,
    closed: bool,
}

impl<T: Timestamp, D: Send + 'static> CountedOutput<T, D> {
    /// Built only during materialization (see
    /// [`ChannelEndpoints::take_output`](crate::dataflow::schedulable::ChannelEndpoints::take_output)).
    pub(in crate::dataflow) fn new(
        name: String,
        push: Box<dyn Push<T, D>>,
        ledger: PortLedger<T>,
        reporter: ProgressReporter<T>,
        wake: Option<WakeHandle>,
    ) -> Self {
        #[cfg_attr(
            not(any(debug_assertions, feature = "progress-check")),
            allow(unused_mut)
        )]
        let mut retry = PendingOutput::new();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        retry.observe(&*push);
        Self {
            handle: OutputHandle::new(name),
            retry,
            push,
            ledger,
            reporter,
            wake,
            retained_live: Arc::new(AtomicUsize::new(0)),
            closed: false,
        }
    }

    /// The logic's view of the port.
    pub(crate) fn handle(&mut self) -> &mut OutputHandle<T, D> {
        &mut self.handle
    }

    /// The capability reporter of this output, for logic that holds
    /// capabilities on it.
    pub(crate) fn reporter(&self) -> &ProgressReporter<T> {
        &self.reporter
    }

    /// Use `reporter` for this output's capabilities instead (during
    /// materialization, for operators whose reporter was bound at build).
    pub(in crate::dataflow) fn set_reporter(&mut self, reporter: ProgressReporter<T>) {
        self.reporter = reporter;
    }

    /// The executor's wake handle, for capabilities released on other tasks.
    pub(crate) fn wake_handle(&self) -> Option<&WakeHandle> {
        self.wake.as_ref()
    }

    /// The progress checker's port for work held for this output.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn check_port(&self) -> Option<crate::progress::check::Port<T>> {
        self.push.check_port()
    }

    /// Whether output is waiting to go into the channel.
    fn has_pending(&self) -> bool {
        !self.retry.is_empty() || self.handle.has_output()
    }

    /// Take the call's output: check it (V3), count it as produced and
    /// queue it for the channel.
    fn take_session(
        &mut self,
        _: &ShellToken,
        justify: Option<(&Justify<T>, usize)>,
    ) -> Result<()> {
        if !self.handle.has_output() {
            return Ok(());
        }
        if let Some((justify, output)) = justify {
            for time in self.handle.buffered_times() {
                justify.check_output(output, time)?;
            }
        }
        let mut produced = ChangeBatch::new();
        for (time, data) in self.handle.drain() {
            if data.is_empty() {
                continue;
            }
            produced.update(time.clone(), data.len() as i64);
            self.retry.push_back(Envelope::data(time, data));
        }
        self.ledger.absorb(&mut produced);
        Ok(())
    }

    fn discard_session(&mut self, _: &ShellToken) {
        self.handle.drain().for_each(drop);
    }

    /// Push the retry queue into the channel. `Ok(false)` on backpressure.
    fn flush(&mut self, _: &ShellToken) -> Result<bool> {
        while let Some(envelope) = self.retry.pop_front() {
            match self.push.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.retry.push_front(returned);
                    return Ok(false);
                }
                Err((e, _)) => return Err(e),
            }
        }
        Ok(true)
    }

    fn close(&mut self, _: &ShellToken) {
        if !self.closed {
            self.closed = true;
            self.push.close();
        }
    }
}

// ---------------------------------------------------------------------------
// Port sets
// ---------------------------------------------------------------------------

/// An operator's inputs: none, one, two, or a list of the same type.
pub(crate) trait InputPorts: Send + 'static {
    /// Number of input ports.
    fn len(&self) -> usize;
    /// Whether any input has data waiting for the logic.
    fn buffered(&self) -> bool;
    /// Whether every input's channel has ended.
    fn exhausted(&self) -> bool;
    /// Pull from every channel. `Ok(true)` if data arrived.
    fn pull(&mut self, tok: &ShellToken) -> Result<bool>;
    /// Commit the call's consumption to the ledgers.
    fn commit(&mut self, tok: &ShellToken);
    /// Forget the call's consumption (the call failed).
    fn discard(&mut self, tok: &ShellToken);

    /// Whether every input's frontier is empty: no more input can arrive.
    fn frontiers_empty(&self) -> bool;
    /// Set input `port`'s frontier (an `Antichain` of the port's time).
    /// Returns whether it changed.
    fn set_frontier(&mut self, tok: &ShellToken, port: usize, frontier: &dyn Any) -> bool;
    /// Connect inputs `first..` to the call's justification (a `Justify` of
    /// the port's time; ports of another time are left out).
    fn attach(&mut self, tok: &ShellToken, justify: &dyn Any, first: usize);
}

impl InputPorts for () {
    fn len(&self) -> usize {
        0
    }
    fn buffered(&self) -> bool {
        false
    }
    fn exhausted(&self) -> bool {
        true
    }
    fn pull(&mut self, _: &ShellToken) -> Result<bool> {
        Ok(false)
    }
    fn commit(&mut self, _: &ShellToken) {}
    fn discard(&mut self, _: &ShellToken) {}

    fn frontiers_empty(&self) -> bool {
        false
    }
    fn set_frontier(&mut self, _: &ShellToken, _: usize, _: &dyn Any) -> bool {
        false
    }
    fn attach(&mut self, _: &ShellToken, _: &dyn Any, _: usize) {}
}

impl<T: Timestamp, D: Send + 'static> InputPorts for CountedInput<T, D> {
    fn len(&self) -> usize {
        1
    }
    fn buffered(&self) -> bool {
        self.has_pending()
    }
    fn exhausted(&self) -> bool {
        self.is_exhausted()
    }
    fn pull(&mut self, tok: &ShellToken) -> Result<bool> {
        CountedInput::pull(self, tok)
    }
    fn commit(&mut self, tok: &ShellToken) {
        CountedInput::commit(self, tok);
    }
    fn discard(&mut self, tok: &ShellToken) {
        CountedInput::discard(self, tok);
    }

    fn frontiers_empty(&self) -> bool {
        self.handle.frontier().is_empty()
    }
    fn set_frontier(&mut self, _: &ShellToken, port: usize, frontier: &dyn Any) -> bool {
        port == 0
            && frontier
                .downcast_ref::<Antichain<T>>()
                .is_some_and(|frontier| self.handle.set_frontier(frontier.clone()))
    }
    fn attach(&mut self, _: &ShellToken, justify: &dyn Any, first: usize) {
        if let Some(justify) = justify.downcast_ref::<Justify<T>>() {
            self.handle.set_justify(first, justify.clone());
        }
    }
}

impl<A: InputPorts, B: InputPorts> InputPorts for (A, B) {
    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }
    fn buffered(&self) -> bool {
        self.0.buffered() || self.1.buffered()
    }
    fn exhausted(&self) -> bool {
        self.0.exhausted() && self.1.exhausted()
    }
    fn pull(&mut self, tok: &ShellToken) -> Result<bool> {
        let a = self.0.pull(tok)?;
        let b = self.1.pull(tok)?;
        Ok(a || b)
    }
    fn commit(&mut self, tok: &ShellToken) {
        self.0.commit(tok);
        self.1.commit(tok);
    }
    fn discard(&mut self, tok: &ShellToken) {
        self.0.discard(tok);
        self.1.discard(tok);
    }

    fn frontiers_empty(&self) -> bool {
        self.0.frontiers_empty() && self.1.frontiers_empty()
    }
    fn set_frontier(&mut self, tok: &ShellToken, port: usize, frontier: &dyn Any) -> bool {
        let split = self.0.len();
        if port < split {
            self.0.set_frontier(tok, port, frontier)
        } else {
            self.1.set_frontier(tok, port - split, frontier)
        }
    }
    fn attach(&mut self, tok: &ShellToken, justify: &dyn Any, first: usize) {
        let split = self.0.len();
        self.0.attach(tok, justify, first);
        self.1.attach(tok, justify, first + split);
    }
}

impl<T: Timestamp, D: Send + 'static> InputPorts for Vec<CountedInput<T, D>> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }
    fn buffered(&self) -> bool {
        self.iter().any(CountedInput::has_pending)
    }
    fn exhausted(&self) -> bool {
        self.iter().all(CountedInput::is_exhausted)
    }
    fn pull(&mut self, tok: &ShellToken) -> Result<bool> {
        let mut pulled = false;
        for input in self.iter_mut() {
            pulled |= CountedInput::pull(input, tok)?;
        }
        Ok(pulled)
    }
    fn commit(&mut self, tok: &ShellToken) {
        self.iter_mut()
            .for_each(|input| CountedInput::commit(input, tok));
    }
    fn discard(&mut self, tok: &ShellToken) {
        self.iter_mut()
            .for_each(|input| CountedInput::discard(input, tok));
    }

    fn frontiers_empty(&self) -> bool {
        self.iter().all(InputPorts::frontiers_empty)
    }
    fn set_frontier(&mut self, tok: &ShellToken, port: usize, frontier: &dyn Any) -> bool {
        self.get_mut(port)
            .is_some_and(|input| InputPorts::set_frontier(input, tok, 0, frontier))
    }
    fn attach(&mut self, tok: &ShellToken, justify: &dyn Any, first: usize) {
        for (i, input) in self.iter_mut().enumerate() {
            InputPorts::attach(input, tok, justify, first + i);
        }
    }
}

/// An operator's outputs: none, one, or two.
pub(crate) trait OutputPorts: Send + 'static {
    /// Number of output ports.
    fn len(&self) -> usize;
    /// Whether output is waiting to go into a channel.
    fn pending(&self) -> bool;
    /// Whether any output holds a capability.
    fn holds_capability(&self) -> bool;
    /// Whether a `Retained` of any output is alive (its drop, on any task,
    /// wakes the executor).
    fn retained(&self) -> bool;
    /// Take every output's call output: check it (when `justify` is a
    /// `Justify` of the port's time), count it and queue it.
    fn take_sessions(
        &mut self,
        tok: &ShellToken,
        justify: Option<&dyn Any>,
        first: usize,
    ) -> Result<()>;
    /// Forget the call's output (the call failed).
    fn discard_sessions(&mut self, tok: &ShellToken);
    /// Push every retry queue into its channel. `Ok(false)` if any blocked.
    fn flush(&mut self, tok: &ShellToken) -> Result<bool>;
    /// Close every output (end of stream).
    fn close(&mut self, tok: &ShellToken);
    /// Connect outputs `first..` to the call's justification.
    fn attach(&mut self, tok: &ShellToken, justify: &dyn Any, first: usize);
}

impl OutputPorts for () {
    fn len(&self) -> usize {
        0
    }
    fn pending(&self) -> bool {
        false
    }
    fn holds_capability(&self) -> bool {
        false
    }
    fn retained(&self) -> bool {
        false
    }
    fn take_sessions(&mut self, _: &ShellToken, _: Option<&dyn Any>, _: usize) -> Result<()> {
        Ok(())
    }
    fn discard_sessions(&mut self, _: &ShellToken) {}
    fn flush(&mut self, _: &ShellToken) -> Result<bool> {
        Ok(true)
    }
    fn close(&mut self, _: &ShellToken) {}
    fn attach(&mut self, _: &ShellToken, _: &dyn Any, _: usize) {}
}

impl<T: Timestamp, D: Send + 'static> OutputPorts for CountedOutput<T, D> {
    fn len(&self) -> usize {
        1
    }
    fn pending(&self) -> bool {
        self.has_pending()
    }
    fn holds_capability(&self) -> bool {
        self.reporter.holds_any()
    }
    fn retained(&self) -> bool {
        self.retained_live.load(Ordering::SeqCst) > 0
    }
    fn take_sessions(
        &mut self,
        tok: &ShellToken,
        justify: Option<&dyn Any>,
        first: usize,
    ) -> Result<()> {
        let justify = justify
            .and_then(|j| j.downcast_ref::<Justify<T>>())
            .map(|j| (j, first));
        self.take_session(tok, justify)
    }
    fn discard_sessions(&mut self, tok: &ShellToken) {
        self.discard_session(tok);
    }
    fn flush(&mut self, tok: &ShellToken) -> Result<bool> {
        CountedOutput::flush(self, tok)
    }
    fn close(&mut self, tok: &ShellToken) {
        CountedOutput::close(self, tok);
    }
    fn attach(&mut self, _: &ShellToken, justify: &dyn Any, first: usize) {
        if let Some(justify) = justify.downcast_ref::<Justify<T>>() {
            justify.set_reporter(first, self.reporter.clone());
            self.handle.set_retain(
                first,
                justify.clone(),
                self.reporter.clone(),
                self.wake.clone(),
                Arc::clone(&self.retained_live),
            );
        }
    }
}

impl<A: OutputPorts, B: OutputPorts> OutputPorts for (A, B) {
    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }
    fn pending(&self) -> bool {
        self.0.pending() || self.1.pending()
    }
    fn holds_capability(&self) -> bool {
        self.0.holds_capability() || self.1.holds_capability()
    }
    fn retained(&self) -> bool {
        self.0.retained() || self.1.retained()
    }
    fn take_sessions(
        &mut self,
        tok: &ShellToken,
        justify: Option<&dyn Any>,
        first: usize,
    ) -> Result<()> {
        self.0.take_sessions(tok, justify, first)?;
        self.1.take_sessions(tok, justify, first + self.0.len())
    }
    fn discard_sessions(&mut self, tok: &ShellToken) {
        self.0.discard_sessions(tok);
        self.1.discard_sessions(tok);
    }
    fn flush(&mut self, tok: &ShellToken) -> Result<bool> {
        // Flush each port even if another is blocked, so one blocked
        // consumer never starves the other.
        let a = self.0.flush(tok)?;
        let b = self.1.flush(tok)?;
        Ok(a && b)
    }
    fn close(&mut self, tok: &ShellToken) {
        self.0.close(tok);
        self.1.close(tok);
    }
    fn attach(&mut self, tok: &ShellToken, justify: &dyn Any, first: usize) {
        let split = self.0.len();
        self.0.attach(tok, justify, first);
        self.1.attach(tok, justify, first + split);
    }
}

// ---------------------------------------------------------------------------
// OperatorLogic
// ---------------------------------------------------------------------------

/// What an operator does, without any port I/O: the shell calls it with its
/// counted ports.
pub(crate) trait OperatorLogic: Send + 'static {
    /// The operator's inputs.
    type Inputs: InputPorts;
    /// The operator's outputs.
    type Outputs: OutputPorts;
    /// The time of the outputs whose output and capabilities V3 checks.
    type Time: Timestamp;

    /// Whether to call the logic in this activation. By default, when
    /// input is waiting.
    fn wants_call(&self, inputs: &Self::Inputs) -> bool {
        inputs.buffered()
    }

    /// One logic call: take input, fill output sessions, acquire or
    /// release capabilities. Returns whether it did anything.
    fn call(&mut self, inputs: &mut Self::Inputs, outputs: &mut Self::Outputs) -> Result<bool>;

    /// Whether the logic keeps work it has not handed to its outputs yet
    /// (source data, async tasks, notifications, an external event).
    fn holds_work(&self) -> bool {
        false
    }

    /// Whether the logic is waiting for asynchronous work that will wake
    /// the executor when it completes.
    fn waiting_for_async(&self) -> bool {
        false
    }

    /// Whether the logic could not hand over all of its work because an
    /// external consumer is full (the executor retries it).
    fn blocked(&self) -> bool {
        false
    }

    /// The meet of the input frontiers changed.
    fn update_frontier(&mut self, _meet: &dyn Any) {}

    /// Whether notifications are ready.
    fn has_ready_notifications(&self) -> bool {
        false
    }

    /// Stop accepting external input (graceful drain).
    fn close_external_input(&mut self) {}

    /// Whether V3 checks this operator's output against its calls'
    /// justification. Operators that translate times between scopes (enter,
    /// leave) emit exactly the translated time of the batch they consume in
    /// the same call, in another time type, so they opt out. (A loop's
    /// feedback stays in one time and is checked: its `(t, i + 1)` output is
    /// justified by the `(t, i)` batch it consumes.)
    fn checks_justification(&self) -> bool {
        true
    }

    /// The call's justification set (a `Justify` of [`Self::Time`]), for
    /// logic that acquires capabilities itself (`notify_at`). Called once,
    /// when the shell is built.
    fn attach_justify(&mut self, _justify: &dyn Any) {}

    /// Whether the logic is a user closure: it is called when its input
    /// frontier changes while one of its outputs' [`Retained`]s is alive,
    /// and once more, whether or not it holds one, when its input frontiers
    /// become empty (R45). A closure that keeps work with
    /// [`OutputHandle::retain`] flushes it then.
    fn calls_on_frontier(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// OperatorShell
// ---------------------------------------------------------------------------

/// The one [`SchedulableOperator`] of every built-in operator: runs its
/// [`OperatorLogic`] and owns all of its port I/O (see the module docs).
pub(crate) struct OperatorShell<L: OperatorLogic> {
    name: String,
    index: usize,
    stage_id: StageId,
    logic: L,
    inputs: L::Inputs,
    outputs: L::Outputs,
    justify: Justify<L::Time>,
    /// Latched (R39): every input ended, nothing is held or left to hand
    /// over. A hint only; it never closes the outputs.
    exhausted: bool,
    /// An input frontier changed since the logic was last called.
    frontier_changed: bool,
    /// The logic had its final call, the one in which its input frontiers
    /// were empty (R45).
    final_called: bool,
    /// A progress tracker delivers this operator's input frontiers (not so in
    /// executor unit tests without one).
    frontier_known: bool,
    shut_down: bool,
}

impl<L: OperatorLogic> OperatorShell<L> {
    /// Wrap `logic` with its counted ports.
    pub(crate) fn new(
        name: impl Into<String>,
        index: usize,
        stage_id: StageId,
        logic: L,
        mut inputs: L::Inputs,
        mut outputs: L::Outputs,
    ) -> Self {
        let tok = ShellToken(());
        let justify = Justify::<L::Time>::new(outputs.len());
        let mut logic = logic;
        logic.attach_justify(&justify);
        inputs.attach(&tok, &justify, 0);
        outputs.attach(&tok, &justify, 0);
        Self {
            name: name.into(),
            index,
            stage_id,
            logic,
            inputs,
            outputs,
            justify,
            exhausted: false,
            frontier_changed: false,
            final_called: false,
            frontier_known: false,
            shut_down: false,
        }
    }

    /// Box the shell as the executor's operator.
    pub(crate) fn boxed(self) -> Box<dyn SchedulableOperator> {
        Box::new(self)
    }

    /// Whether the operator holds nothing: no input waiting for its logic,
    /// no output waiting for a channel, no capability and no work its logic
    /// keeps.
    fn holds_nothing(&self) -> bool {
        !self.final_call_due()
            && !self.inputs.buffered()
            && !self.outputs.pending()
            && !self.outputs.holds_capability()
            && !self.logic.holds_work()
    }

    /// Whether the logic is owed its final call (R45): it is a closure
    /// (`calls_on_frontier`) with inputs, and has not yet been called with
    /// every input frontier empty. Until then the operator cannot shut down,
    /// so a closure that keeps data without `retain` and emits it in that
    /// call fails loudly (V3) instead of losing it.
    fn final_call_due(&self) -> bool {
        self.frontier_known
            && !self.final_called
            && self.logic.calls_on_frontier()
            && self.inputs.len() > 0
    }

    /// Run the logic once, then commit its consumption and output.
    fn run_call(&mut self, tok: &ShellToken) -> Result<bool> {
        self.justify.begin();
        let result = self.logic.call(&mut self.inputs, &mut self.outputs);
        let violation = self.justify.finish_call();
        let checks = self.logic.checks_justification();
        let result = result.and_then(|did| match violation {
            Some(error) if checks => Err(error),
            _ => Ok(did),
        });
        let result = result.and_then(|did| {
            let justify: Option<&dyn Any> = checks.then_some(&self.justify as &dyn Any);
            self.outputs.take_sessions(tok, justify, 0).map(|()| did)
        });
        self.justify.end();
        match result {
            Ok(did) => {
                self.inputs.commit(tok);
                Ok(did)
            }
            Err(error) => {
                // A failed call commits nothing.
                self.inputs.discard(tok);
                self.outputs.discard_sessions(tok);
                Err(error)
            }
        }
    }
}

impl<L: OperatorLogic> SchedulableOperator for OperatorShell<L> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.shut_down {
            self.check_after_shutdown()?;
            return Ok(ActivationOutcome::Done);
        }
        let tok = ShellToken(());

        // Output that did not fit into its channel goes first.
        if self.outputs.pending() && !self.outputs.flush(&tok)? {
            return Ok(ActivationOutcome::BlockedOnBackpressure);
        }

        // A logic blocked on an external consumer retries before any more
        // input is pulled, so a full consumer pushes back on the producer.
        let mut did = false;
        if self.logic.blocked() {
            did = self.run_call(&tok)?;
            if self.logic.blocked() {
                return Ok(if did {
                    ActivationOutcome::MadeProgress
                } else {
                    ActivationOutcome::BlockedOnBackpressure
                });
            }
        }

        self.inputs.pull(&tok)?;

        // A logic that keeps work under a `Retained` is also called when its
        // input frontier changed, so it can flush.
        let frontier_call =
            self.frontier_changed && self.logic.calls_on_frontier() && self.outputs.retained();
        // Every closure is called once more when its input frontiers become
        // empty (R45), its last chance to emit what it holds under `retain`.
        let final_call = self.final_call_due() && self.inputs.frontiers_empty();
        if self.logic.wants_call(&self.inputs) || frontier_call || final_call {
            self.frontier_changed = false;
            if self.inputs.frontiers_empty() {
                self.final_called = true;
            }
            did |= self.run_call(&tok)?;
            if !self.outputs.flush(&tok)? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        if !self.exhausted && self.inputs.exhausted() && self.holds_nothing() {
            self.exhausted = true;
        }

        if did {
            Ok(ActivationOutcome::MadeProgress)
        } else if self.logic.blocked() {
            Ok(ActivationOutcome::BlockedOnBackpressure)
        } else if self.logic.waiting_for_async() || self.outputs.retained() {
            // An async task, or the drop of a `Retained` on another task,
            // will wake the executor.
            Ok(ActivationOutcome::WaitingForAsync)
        } else {
            Ok(ActivationOutcome::Idle)
        }
    }

    fn is_done(&self) -> bool {
        self.shut_down
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn index(&self) -> usize {
        self.index
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn can_shut_down(&self) -> bool {
        self.holds_nothing()
    }

    fn shut_down(&mut self) {
        let tok = ShellToken(());
        self.outputs.close(&tok);
        self.shut_down = true;
    }

    fn exhausted(&self) -> bool {
        self.exhausted || self.shut_down
    }

    fn check_after_shutdown(&mut self) -> Result<()> {
        let tok = ShellToken(());
        if self.inputs.pull(&tok)? {
            return Err(Error::Progress(ProgressError::RecordAfterShutdown {
                operator: self.name.clone(),
                time: "(any)".into(),
            }));
        }
        Ok(())
    }

    fn close_external_input(&mut self) {
        self.logic.close_external_input();
    }

    fn update_input_frontier(&mut self, frontier: &dyn Any) {
        self.logic.update_frontier(frontier);
    }

    fn update_port_frontier(&mut self, port: usize, frontier: &dyn Any) {
        let tok = ShellToken(());
        self.frontier_known = true;
        if self.inputs.set_frontier(&tok, port, frontier) {
            self.frontier_changed = true;
        }
    }

    fn has_ready_notifications(&self) -> bool {
        self.logic.has_ready_notifications()
    }

    fn is_shell(&self) -> bool {
        true
    }
}
