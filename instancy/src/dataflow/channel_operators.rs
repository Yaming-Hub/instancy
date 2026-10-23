//! Channel-based source and sink operators for async runtime integration.
//!
//! These operators bridge external callers to the dataflow graph:
//!
//! - [`ChannelSourceOperator`] receives [`InputEvent`]s from an external
//!   channel and pushes data into the dataflow as a source operator.
//! - [`ChannelSinkOperator`] collects data from the dataflow and sends
//!   [`OutputEvent`]s to an external channel for consumption.
//!
//! Both operators support synchronous (`std::sync::mpsc`) and asynchronous
//! (`tokio::sync::mpsc`) channel backends.
//! The channel type is chosen at spawn time via `ChannelMode`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::shell::{CountedInput, CountedOutput, OperatorLogic, OperatorShell};
use crate::dataflow::stage::StageId;
use crate::error::{ProgressError, Result};
use crate::progress::capability::Capability;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// ChannelMode — sync vs async channel selection
// ---------------------------------------------------------------------------

/// Selects the channel backend for external I/O ports.
///
/// Passed to wiring closures at spawn time to determine whether
/// `std::sync::mpsc` (sync) or `tokio::sync::mpsc` (async) channels
/// are used for input/output communication with external code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChannelMode {
    /// Use `std::sync::mpsc` channels — no async runtime required.
    Sync,
    /// Use `tokio::sync::mpsc` channels — enables async send/recv.
    Async,
}

// ---------------------------------------------------------------------------
// InputRecv / OutputSend — channel backend enums
// ---------------------------------------------------------------------------

/// Unified receiver for input channels, abstracting over std and tokio backends.
pub(crate) enum InputRecv<T> {
    Std(mpsc::Receiver<T>),
    Tokio(tokio::sync::mpsc::Receiver<T>),
}

/// Error type for `InputRecv::try_recv()`.
pub(crate) enum RecvError {
    Empty,
    Disconnected,
}

impl<T> InputRecv<T> {
    pub(crate) fn try_recv(&mut self) -> std::result::Result<T, RecvError> {
        match self {
            Self::Std(rx) => rx.try_recv().map_err(|e| match e {
                mpsc::TryRecvError::Empty => RecvError::Empty,
                mpsc::TryRecvError::Disconnected => RecvError::Disconnected,
            }),
            Self::Tokio(rx) => rx.try_recv().map_err(|e| match e {
                tokio::sync::mpsc::error::TryRecvError::Empty => RecvError::Empty,
                tokio::sync::mpsc::error::TryRecvError::Disconnected => RecvError::Disconnected,
            }),
        }
    }

    /// Refuse further sends where the backend supports it, keeping buffered
    /// events receivable. `std::sync::mpsc` cannot refuse sends without
    /// dropping buffered events; [`InputGate`] covers that backend.
    fn close(&mut self) {
        if let Self::Tokio(rx) = self {
            rx.close();
        }
    }
}

// ---------------------------------------------------------------------------
// InputGate — admission control shared by an input's senders and its source
// ---------------------------------------------------------------------------

/// Admission gate shared by an external input's senders and its source
/// operator, so the source can stop accepting input without losing events
/// that a sender has already been told were accepted.
///
/// A sender enters the gate before enqueueing and leaves after the enqueue
/// completes. Once the gate is closed and no sender is inside, every
/// accepted event is already in the channel.
#[derive(Debug, Default)]
pub(crate) struct InputGate {
    closed: AtomicBool,
    in_flight: AtomicUsize,
}

impl InputGate {
    /// Admit one send, or `None` if the input no longer accepts events.
    fn enter(&self) -> Option<InputGatePermit<'_>> {
        // SeqCst pairs with `close` + `is_idle`: either this sender observes
        // the close, or the closer observes this sender in flight.
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.closed.load(Ordering::SeqCst) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(InputGatePermit(self))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn is_idle(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0
    }
}

struct InputGatePermit<'a>(&'a InputGate);

impl Drop for InputGatePermit<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One admitted enqueue. On exit — success, error, or a dropped async send
/// future — leaves the gate and then wakes the executor, so a draining source
/// waiting for in-flight senders re-checks promptly.
struct Admission<'a> {
    permit: Option<InputGatePermit<'a>>,
    wake: &'a WakeOnDrop,
}

impl<'a> Admission<'a> {
    fn enter(gate: &'a Option<Arc<InputGate>>, wake: &'a WakeOnDrop) -> Result<Self> {
        let permit = match gate {
            Some(gate) => Some(gate.enter().ok_or(crate::error::Error::ChannelClosed)?),
            None => None,
        };
        Ok(Self { permit, wake })
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.wake.notify();
    }
}

/// Unified sender for output channels, abstracting over std and tokio backends.
pub(crate) enum OutputSend<T> {
    Std(mpsc::SyncSender<T>),
    Tokio(tokio::sync::mpsc::Sender<T>),
}

/// Error from `OutputSend::try_send()`.
pub(crate) enum SendError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> OutputSend<T> {
    pub(crate) fn try_send(&self, item: T) -> std::result::Result<(), SendError<T>> {
        match self {
            Self::Std(tx) => tx.try_send(item).map_err(|e| match e {
                mpsc::TrySendError::Full(v) => SendError::Full(v),
                mpsc::TrySendError::Disconnected(v) => SendError::Disconnected(v),
            }),
            Self::Tokio(tx) => tx.try_send(item).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(v) => SendError::Full(v),
                tokio::sync::mpsc::error::TrySendError::Closed(v) => SendError::Disconnected(v),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// ChannelSourceLogic
// ---------------------------------------------------------------------------

/// The logic of a source fed by an external channel: every declared
/// `input()` port and every `source_async`.
///
/// The caller sends [`InputEvent`]s through an [`InputSender`] (or an
/// [`AsyncInputSender`]); the source emits the data and downgrades its
/// capability with each frontier event.
///
/// # Quiescence behavior
///
/// While this operator's channel is open, it prevents the executor from
/// declaring quiescence by holding a count in `external_inputs_open`.
/// When the channel is closed (sender dropped or explicit close), the
/// count is decremented and the source releases its capability.
///
/// # Graceful drain
///
/// [`close_external_input`](OperatorLogic::close_external_input) closes the
/// input's admission gate: later sends fail with [`crate::Error::ChannelClosed`],
/// while every event a sender was already told was accepted is still
/// delivered before the source finishes.
pub(crate) struct ChannelSourceLogic<T: Timestamp, D: Send + 'static> {
    receiver: InputRecv<InputEvent<T, D>>,
    /// The source's capability: its operator's initial capability at
    /// `T::minimum()`, downgraded by frontier events, released at the end
    /// of input. `None` without progress tracking.
    capability: Option<Capability<T>>,
    external_inputs_open: Arc<AtomicUsize>,
    /// The time of the source's frontier. Starts at T::minimum() and
    /// advances when Frontier events are received.
    current: T,
    /// Until the first advance, Timestamp::minimum() permits every valid time.
    frontier_advanced: bool,
    /// Admission gate shared with this input's senders (absent in unit tests
    /// that construct the operator directly).
    input_gate: Option<Arc<InputGate>>,
    /// Set once external input is closed for a graceful drain.
    external_closed: bool,
    /// Input ended: the capability is released.
    finished: bool,
    /// Set (async sources without a drain) when the producer was cut off by a
    /// cancellation: the closed channel is then not an end of input.
    abandoned: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// A protocol violation found after events that were already emitted:
    /// raised once those are committed.
    failure: Option<crate::Error>,
}

impl<T: Timestamp, D: Send + 'static> ChannelSourceLogic<T, D> {
    /// Create the logic of a channel source; with `seeded`, it holds its
    /// operator's initial capability (on `output`'s reporter).
    pub(crate) fn new(
        receiver: InputRecv<InputEvent<T, D>>,
        output: &CountedOutput<T, D>,
        seeded: bool,
        external_inputs_open: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            receiver,
            capability: seeded.then(|| Capability::seeded(T::minimum(), output.reporter().clone())),
            external_inputs_open,
            current: T::minimum(),
            frontier_advanced: false,
            input_gate: None,
            external_closed: false,
            finished: false,
            abandoned: None,
            failure: None,
        }
    }

    /// Share an admission gate with this input's senders.
    pub(crate) fn with_input_gate(mut self, gate: Arc<InputGate>) -> Self {
        self.input_gate = Some(gate);
        self
    }

    /// A channel that closes after `flag` was set did not end the input (its
    /// producer was cancelled): the source then neither finishes nor
    /// releases its capability, and the executor stops on the cancellation.
    pub(crate) fn with_abandon_flag(
        mut self,
        flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Self {
        self.abandoned = flag;
        self
    }

    /// Whether no sender is between admission and enqueue.
    fn senders_idle(&self) -> bool {
        self.input_gate.as_ref().is_none_or(|gate| gate.is_idle())
    }

    /// The input ended: stop accepting events and release the capability.
    fn finish(&mut self) {
        if self.finished {
            return;
        }
        // Senders must not report success for events nobody will read.
        if let Some(gate) = &self.input_gate {
            gate.close();
        }
        self.receiver.close();
        self.capability = None;
        self.finished = true;
        // Decrement external input counter so executor can detect quiescence
        self.external_inputs_open.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: Timestamp, D: Send + 'static> OperatorLogic for ChannelSourceLogic<T, D> {
    type Inputs = ();
    type Outputs = CountedOutput<T, D>;
    type Time = T;

    fn wants_call(&self, _: &()) -> bool {
        !self.finished || self.failure.is_some()
    }

    fn call(&mut self, _: &mut (), output: &mut Self::Outputs) -> Result<bool> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        // Cap per-call work so other operators get a turn. The cap applies
        // to data events; frontier events are lightweight and don't count.
        const ACTIVATION_BUDGET: usize = 1024;
        let output = output.handle();
        let mut made_progress = false;
        let mut data_events = 0usize;
        // After external input closes: set once no sender is mid-enqueue, so
        // the channel's contents are final and an empty receive ends the input.
        let mut drained_inputs_final = false;

        while data_events < ACTIVATION_BUDGET {
            match self.receiver.try_recv() {
                Ok(InputEvent::Data { time, data }) => {
                    if self.frontier_advanced && !self.current.less_equal(&time) {
                        // Emit what came before; fail once it is committed.
                        self.failure = Some(
                            ProgressError::InputTimeBeforeFrontier {
                                time: format!("{time:?}"),
                                frontier: format!("{:?}", self.current),
                            }
                            .into(),
                        );
                        return Ok(made_progress);
                    }
                    output.push_vec(time, data);
                    made_progress = true;
                    data_events += 1;
                }
                Ok(InputEvent::Frontier(time)) => {
                    if !self.current.less_equal(&time) {
                        self.failure = Some(
                            ProgressError::InvalidInputFrontier {
                                from: format!("{:?}", self.current),
                                to: format!("{time:?}"),
                            }
                            .into(),
                        );
                        return Ok(made_progress);
                    }
                    if time != self.current {
                        if let Some(capability) = &mut self.capability {
                            capability.downgrade(&time)?;
                        }
                        self.current = time;
                        self.frontier_advanced = true;
                    }
                    made_progress = true;
                }
                Err(RecvError::Empty) => {
                    if self.external_closed {
                        if drained_inputs_final {
                            self.finish();
                            return Ok(true);
                        }
                        if self.senders_idle() {
                            // The gate is closed and empty: every accepted event
                            // is enqueued. Receive once more to collect it.
                            drained_inputs_final = true;
                            continue;
                        }
                        // A sender is mid-enqueue; it notifies when it leaves.
                    }
                    break;
                }
                Err(RecvError::Disconnected) => {
                    if self
                        .abandoned
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::SeqCst))
                    {
                        // Cut off by a cancellation, not closed: ending the
                        // input would present a truncated stream as complete
                        // before the executor notices the cancellation.
                        break;
                    }
                    self.finish();
                    return Ok(true);
                }
            }
        }
        Ok(made_progress)
    }

    fn holds_work(&self) -> bool {
        !self.finished
    }

    fn close_external_input(&mut self) {
        if self.finished || self.external_closed {
            return;
        }
        self.external_closed = true;
        if let Some(gate) = &self.input_gate {
            gate.close();
        }
        // Fail async senders blocked on capacity now; buffered events remain.
        self.receiver.close();
    }

    fn inputs_closed(&mut self) {
        self.finish();
    }
}

/// A channel source operator: `logic` (see [`ChannelSourceLogic`]) with its
/// output.
pub(crate) fn channel_source<T: Timestamp, D: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    logic: ChannelSourceLogic<T, D>,
    output: CountedOutput<T, D>,
) -> OperatorShell<ChannelSourceLogic<T, D>> {
    OperatorShell::new(name, index, stage_id, logic, (), output)
}

// ---------------------------------------------------------------------------
// ChannelSinkLogic
// ---------------------------------------------------------------------------

/// The logic of a sink that sends output data to an external channel: every
/// declared `output()` port. Data received from upstream is forwarded as
/// events to an [`OutputReceiver`] (or an [`AsyncOutputReceiver`]).
///
/// # Egress credit
///
/// The sink takes each batch on credit
/// ([`InputHandle::next_on_credit`](crate::dataflow::InputHandle)): the
/// batch stays counted at the sink's input until the external channel
/// accepts its event (or the event is explicitly discarded), so a full or
/// paused external channel holds the input's frontier and no completion can
/// pass it.
///
/// # Dropped receivers
///
/// If the receiver is dropped, the port's output is no longer wanted: the sink
/// keeps consuming its input and discards it explicitly, so upstream
/// operators never block on the abandoned port and the dataflow completes
/// normally once its inputs close.
///
/// The event type `E` is `OutputEvent<T, D>` for ordinary outputs. Outputs
/// declared inside an `iterate` body send events whose timestamp has been
/// projected onto the dataflow's (root) timestamp and type-erased.
pub(crate) struct ChannelSinkLogic<T: Timestamp, D: Send + 'static, E: Send + 'static> {
    sender: OutputSend<E>,
    /// Builds the event sent for a `(time, data)` batch.
    make_event: Box<dyn Fn(T, Vec<D>) -> E + Send>,
    /// An event the full channel did not accept yet, with the credit of
    /// its batch.
    pending_send: Option<(E, (T, i64))>,
    /// The receiver is gone; input is consumed and discarded.
    receiver_dropped: bool,
    /// Whether the last call left an event the channel did not accept.
    blocked: bool,
    /// The progress checker's count of `pending_send`.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check_egress: Option<crate::progress::check::StoreCheck<T>>,
}

impl<T: Timestamp, D: Send + 'static, E: Send + 'static> ChannelSinkLogic<T, D, E> {
    /// Create the logic of a channel sink that builds its events with
    /// `make_event`.
    pub(crate) fn new(
        sender: OutputSend<E>,
        make_event: Box<dyn Fn(T, Vec<D>) -> E + Send>,
        input: &CountedInput<T, D>,
    ) -> Self {
        #[cfg(not(any(debug_assertions, feature = "progress-check")))]
        let _ = input;
        Self {
            sender,
            make_event,
            pending_send: None,
            receiver_dropped: false,
            blocked: false,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check_egress: input.check_port().map(|port| {
                crate::progress::check::StoreCheck::new(port, crate::progress::check::Store::Egress)
            }),
        }
    }

    /// Forward an event unless the receiver is gone; the event's credit is
    /// redeemed once the channel accepts it or it is discarded. Returns
    /// `false` (and keeps the event) if the channel is full.
    fn forward(&mut self, input: &mut CountedInput<T, D>, event: E, credit: (T, i64)) -> bool {
        if self.receiver_dropped {
            input.redeem(credit);
            return true;
        }
        match self.sender.try_send(event) {
            Ok(()) => {
                input.redeem(credit);
                true
            }
            Err(SendError::Full(event)) => {
                #[cfg(any(debug_assertions, feature = "progress-check"))]
                if let Some(check) = &self.check_egress {
                    check.add(&credit.0, credit.1 as usize);
                }
                self.pending_send = Some((event, credit));
                false
            }
            Err(SendError::Disconnected(_)) => {
                // Explicitly discarded: the output is no longer wanted.
                self.receiver_dropped = true;
                input.redeem(credit);
                true
            }
        }
    }
}

impl<T: Timestamp, D: Send + 'static, E: Send + 'static> OperatorLogic
    for ChannelSinkLogic<T, D, E>
{
    type Inputs = CountedInput<T, D>;
    type Outputs = ();
    type Time = T;

    fn wants_call(&self, input: &Self::Inputs) -> bool {
        input.has_pending() || self.pending_send.is_some()
    }

    fn call(&mut self, input: &mut Self::Inputs, _: &mut ()) -> Result<bool> {
        self.blocked = false;
        let mut made_progress = false;
        if let Some((event, credit)) = self.pending_send.take() {
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            if let Some(check) = &self.check_egress {
                check.remove(&credit.0, credit.1 as usize);
            }
            if !self.forward(input, event, credit) {
                self.blocked = true;
                return Ok(false);
            }
            made_progress = true;
        }
        while let Some((time, data, credit)) = input.handle().next_on_credit() {
            made_progress = true;
            let event = (self.make_event)(time, data);
            if !self.forward(input, event, credit) {
                // Channel full: stop taking input, retry next activation.
                self.blocked = true;
                break;
            }
        }
        Ok(made_progress)
    }

    fn holds_work(&self) -> bool {
        self.pending_send.is_some()
    }

    fn blocked(&self) -> bool {
        self.blocked
    }
}

/// A channel sink operator forwarding its input as events built by
/// `make_event` to `sender`.
pub(crate) fn channel_sink<T: Timestamp, D: Send + 'static, E: Send + 'static>(
    name: impl Into<String>,
    index: usize,
    stage_id: StageId,
    input: CountedInput<T, D>,
    sender: OutputSend<E>,
    make_event: Box<dyn Fn(T, Vec<D>) -> E + Send>,
) -> OperatorShell<ChannelSinkLogic<T, D, E>> {
    let logic = ChannelSinkLogic::new(sender, make_event, &input);
    OperatorShell::new(name, index, stage_id, logic, input, ())
}

// ---------------------------------------------------------------------------
// InputSender / OutputReceiver — user-facing handles
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct WakeOnDrop(Option<WakeHandle>);

impl WakeOnDrop {
    fn notify(&self) {
        if let Some(wake) = &self.0 {
            wake.notify();
        }
    }
}

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.notify();
    }
}

/// A handle for sending data into a dataflow input port.
///
/// Created by [`crate::SpawnedDataflow::take_input`]. Send [`InputEvent`]s to feed
/// data into the running dataflow. `InputSender` is **cloneable** — all
/// clones share one channel and one input frontier. Producers must coordinate
/// frontier advancement across all clones. The channel closes only when
/// **all** senders are dropped; [`close()`](Self::close) drops just this instance.
///
/// Data and frontier events are validated in receive order by the source operator.
/// Success from `send()` or `advance_to()` means enqueued, not validated.
/// Protocol violations fail execution with an [`crate::Error::Progress`] wrapped
/// in operator context; observe them through the dataflow's completion handle.
/// Already delivered output is not rolled back on failure.
///
/// When a `WakeHandle` is wired (set during runtime spawn), every `send()`,
/// `advance_to()`, and `Drop` notifies the executor's wake handle so
/// that a sleeping async executor wakes promptly when external data arrives.
///
/// Once the dataflow stops accepting input — it finished, failed, or began a
/// graceful drain ([`crate::SpawnOptions::drain_on_cancel`]) — `send()` and
/// `advance_to()` return [`crate::Error::ChannelClosed`]. Every call that
/// returned `Ok` before that point is still delivered during a drain.
#[derive(Clone, Debug)]
pub struct InputSender<T: Timestamp, D: Send + 'static> {
    sender: mpsc::SyncSender<InputEvent<T, D>>,
    gate: Option<Arc<InputGate>>,
    // Fields drop in declaration order: disconnect the channel before notifying.
    wake_on_drop: WakeOnDrop,
}

impl<T: Timestamp, D: Send + 'static> InputSender<T, D> {
    #[cfg(test)]
    pub(crate) fn new(sender: mpsc::SyncSender<InputEvent<T, D>>) -> Self {
        Self {
            sender,
            gate: None,
            wake_on_drop: WakeOnDrop(None),
        }
    }

    /// Create an InputSender with a wake handle for async executor notification.
    pub(crate) fn with_wake_handle(
        sender: mpsc::SyncSender<InputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            sender,
            gate: None,
            wake_on_drop: WakeOnDrop(Some(wake_handle)),
        }
    }

    /// Share an admission gate with the input's source operator.
    pub(crate) fn with_input_gate(mut self, gate: Arc<InputGate>) -> Self {
        self.gate = Some(gate);
        self
    }

    fn enqueue(&self, event: InputEvent<T, D>) -> Result<()> {
        let _admission = Admission::enter(&self.gate, &self.wake_on_drop)?;
        self.sender
            .send(event)
            .map_err(|_| crate::error::Error::ChannelClosed)
    }

    /// Send a batch of data at the given timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ChannelClosed`] if the dataflow no longer
    /// accepts input (it terminated or is draining after cancellation).
    /// The batch time must be at or beyond the committed frontier in the partial
    /// order, including for empty batches. Violations are reported by execution,
    /// not necessarily by this call. Batch timestamps need not arrive sorted.
    pub fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.enqueue(InputEvent::data(time, data))
    }

    /// Advance the input frontier to `time`.
    ///
    /// All subsequent data times `s` must satisfy `time.less_equal(&s)` in the
    /// partial order. Equality is valid; earlier or incomparable times are not.
    /// The frontier itself must advance monotonically; repeating it is a no-op.
    /// Invalid advances fail execution with [`ProgressError::InvalidInputFrontier`].
    ///
    /// Coordinate all clones before advancing: pending sends have not necessarily
    /// been enqueued. This scalar frontier cannot express an antichain of
    /// incomparable times. At the maximum timestamp, close the input to finish
    /// rather than trying to increment the timestamp.
    ///
    /// # Example
    ///
    /// ```ignore
    /// sender.send(0, vec![data_for_epoch_0]).unwrap();
    /// sender.advance_to(1).unwrap(); // epoch 0 is now sealed
    /// sender.send(1, vec![data_for_epoch_1]).unwrap();
    /// sender.advance_to(2).unwrap(); // epoch 1 is now sealed
    /// ```
    pub fn advance_to(&self, time: T) -> Result<()> {
        self.enqueue(InputEvent::frontier(time))
    }

    /// Close this input, signaling no more data will arrive.
    ///
    /// Equivalent to dropping the sender.
    pub fn close(self) {}
}

/// An output batch whose timestamp has been projected onto the dataflow's root
/// timestamp and type-erased. Sent by outputs declared inside `iterate`
/// bodies, whose stream timestamp (`Product<T, _>`) differs from the
/// dataflow's `T`.
pub(crate) type ErasedOutputEvent<D> = (Box<dyn std::any::Any + Send>, Vec<D>);

/// Converts an erased batch back into an [`OutputEvent`] at the root timestamp.
fn unerase_event<T: Timestamp, D>((time, data): ErasedOutputEvent<D>) -> OutputEvent<T, D> {
    let time = time
        .downcast::<T>()
        // The projection is built by `iterate` and always yields the root timestamp.
        .expect("nested output timestamp must project onto the dataflow timestamp");
    OutputEvent::data(*time, data)
}

/// Spawn-time receiver of an output declared inside an `iterate` body (sync mode).
/// Converted into an [`OutputReceiver`] by `take_output`.
pub(crate) struct ProjectedOutputReceiver<D> {
    pub(crate) receiver: mpsc::Receiver<ErasedOutputEvent<D>>,
    pub(crate) wake_handle: Option<WakeHandle>,
}

/// Spawn-time receiver of an output declared inside an `iterate` body (async
/// mode). Converted into an [`AsyncOutputReceiver`] by `take_async_output`.
pub(crate) struct ProjectedAsyncOutputReceiver<D> {
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ErasedOutputEvent<D>>,
    pub(crate) wake_handle: Option<WakeHandle>,
}

enum OutputRecv<T: Timestamp, D> {
    Typed(mpsc::Receiver<OutputEvent<T, D>>),
    Projected(mpsc::Receiver<ErasedOutputEvent<D>>),
}

/// A handle for receiving output from a dataflow output port.
///
/// Created by [`crate::SpawnedDataflow::take_output`]. Receives [`OutputEvent`]s
/// containing timestamped batches of results.
///
/// Dropping the receiver abandons the port: the dataflow keeps running and
/// discards the port's output instead of blocking on it. An untaken port is
/// treated the same way once the handle is joined or dropped.
///
/// For an output declared inside an `iterate` body, each batch is delivered at
/// the dataflow's (outer) timestamp: a batch produced at `Product(t, i)`
/// arrives at time `t`.
pub struct OutputReceiver<T: Timestamp, D: Send + 'static> {
    receiver: OutputRecv<T, D>,
    /// Notifies the executor when the receiver is dropped, so a sink blocked
    /// on a full channel observes the disconnect promptly. Declared after
    /// `receiver` so the channel disconnects before the wake.
    _wake_on_drop: WakeOnDrop,
}

impl<T: Timestamp, D: Send + 'static> OutputReceiver<T, D> {
    pub(crate) fn new(receiver: mpsc::Receiver<OutputEvent<T, D>>) -> Self {
        Self {
            receiver: OutputRecv::Typed(receiver),
            _wake_on_drop: WakeOnDrop(None),
        }
    }

    pub(crate) fn with_wake_handle(
        receiver: mpsc::Receiver<OutputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            receiver: OutputRecv::Typed(receiver),
            _wake_on_drop: WakeOnDrop(Some(wake_handle)),
        }
    }

    pub(crate) fn from_projected(receiver: ProjectedOutputReceiver<D>) -> Self {
        Self {
            receiver: OutputRecv::Projected(receiver.receiver),
            _wake_on_drop: WakeOnDrop(receiver.wake_handle),
        }
    }

    /// Receive the next output event (blocking).
    ///
    /// Returns `None` when the dataflow completes and the output channel is closed.
    pub fn recv(&self) -> Option<OutputEvent<T, D>> {
        match &self.receiver {
            OutputRecv::Typed(rx) => rx.recv().ok(),
            OutputRecv::Projected(rx) => rx.recv().ok().map(unerase_event),
        }
    }

    /// Try to receive the next output event (non-blocking).
    pub fn try_recv(&self) -> Option<OutputEvent<T, D>> {
        match &self.receiver {
            OutputRecv::Typed(rx) => rx.try_recv().ok(),
            OutputRecv::Projected(rx) => rx.try_recv().ok().map(unerase_event),
        }
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<OutputEvent<T, D>> {
        match &self.receiver {
            OutputRecv::Typed(rx) => rx.recv_timeout(timeout).ok(),
            OutputRecv::Projected(rx) => rx.recv_timeout(timeout).ok().map(unerase_event),
        }
    }

    /// Collect all output events into a vector (blocking until completion).
    ///
    /// Convenience method that drains the receiver until the dataflow completes.
    /// Returns only `Data` events as `(time, data)` pairs.
    pub fn collect_data(&self) -> Vec<(T, Vec<D>)> {
        let mut results = Vec::new();
        while let Some(event) = self.recv() {
            if let OutputEvent::Data { time, data } = event {
                results.push((time, data));
            }
        }
        results
    }
}

// ---------------------------------------------------------------------------
// AsyncInputSender / AsyncOutputReceiver — async user-facing handles
// ---------------------------------------------------------------------------

/// A handle for sending data into a dataflow input port asynchronously.
///
/// Created by [`crate::SpawnedDataflow::take_async_input`] when the dataflow was
/// spawned with [`crate::IoMode::Async`]. Backed by a
/// `tokio::sync::mpsc::Sender` — the `send()` method yields when the channel
/// is full (backpressure) instead of blocking the calling thread.
///
/// When a `WakeHandle` is wired (set during runtime spawn), every `send()`,
/// `advance_to()`, and `Drop` notifies the executor's wake handle so
/// that a sleeping async executor wakes promptly when external data arrives.
///
/// Uses the same shared-frontier and execution-time validation contract as
/// [`InputSender`]. Clones are writers to one logical input, not independent
/// progress holders. Only dropping the last sender closes that input.
///
/// Like [`InputSender`], sends fail with [`crate::Error::ChannelClosed`] once
/// the dataflow stops accepting input, including when a graceful drain begins;
/// a send blocked on capacity at that moment fails instead of enqueueing.
#[derive(Clone)]
pub struct AsyncInputSender<T: Timestamp, D: Send + 'static> {
    sender: tokio::sync::mpsc::Sender<InputEvent<T, D>>,
    gate: Option<Arc<InputGate>>,
    // Keep this after sender so the EOF wake cannot precede disconnection.
    wake_on_drop: WakeOnDrop,
}

impl<T: Timestamp, D: Send + 'static> AsyncInputSender<T, D> {
    pub(crate) fn with_wake_handle(
        sender: tokio::sync::mpsc::Sender<InputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            sender,
            gate: None,
            wake_on_drop: WakeOnDrop(Some(wake_handle)),
        }
    }

    /// Share an admission gate with the input's source operator.
    pub(crate) fn with_input_gate(mut self, gate: Arc<InputGate>) -> Self {
        self.gate = Some(gate);
        self
    }

    async fn enqueue(&self, event: InputEvent<T, D>) -> Result<()> {
        let _admission = Admission::enter(&self.gate, &self.wake_on_drop)?;
        self.sender
            .send(event)
            .await
            .map_err(|_| crate::error::Error::ChannelClosed)
    }

    /// Send a batch of data at the given timestamp.
    ///
    /// Yields (returns `Poll::Pending`) if the channel is full, resuming
    /// when capacity becomes available. This provides backpressure without
    /// blocking an OS thread.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ChannelClosed`] if the dataflow no longer
    /// accepts input (it terminated or is draining after cancellation).
    /// The time must be at or beyond the committed frontier in the partial order.
    /// Success means enqueued; violations surface through execution completion.
    ///
    /// # Cancellation
    ///
    /// Dropping a pending send future does not enqueue its batch (the batch is
    /// dropped). Once this call succeeds, cancelling the caller does not retract
    /// the event. Dataflow cancellation may stop execution before queued events
    /// are validated.
    pub async fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.enqueue(InputEvent::data(time, data)).await
    }

    /// Advance the input frontier to `time`.
    ///
    /// All subsequent data times `s` must satisfy `time.less_equal(&s)` in the
    /// partial order. Equal updates are no-ops; backward or incomparable advances
    /// fail execution with [`ProgressError::InvalidInputFrontier`].
    ///
    /// Coordinate all writers before advancing; a blocked send is not yet
    /// enqueued. Dropping a pending advance future does not enqueue its update.
    /// Success means enqueued, not validated or processed. Backpressure can delay
    /// validation. This scalar frontier cannot express an antichain of
    /// incomparable times. Close the last sender to finish at a maximum time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// sender.send(0, vec![data_for_epoch_0]).await?;
    /// sender.advance_to(1).await?; // epoch 0 is now sealed
    /// sender.send(1, vec![data_for_epoch_1]).await?;
    /// sender.advance_to(2).await?; // epoch 1 is now sealed
    /// ```
    pub async fn advance_to(&self, time: T) -> Result<()> {
        self.enqueue(InputEvent::frontier(time)).await
    }

    /// Close this input, signaling no more data will arrive.
    ///
    /// Equivalent to dropping the sender.
    pub fn close(self) {}
}

enum AsyncOutputRecv<T: Timestamp, D> {
    Typed(tokio::sync::mpsc::Receiver<OutputEvent<T, D>>),
    Projected(tokio::sync::mpsc::Receiver<ErasedOutputEvent<D>>),
}

/// A handle for receiving output from a dataflow output port asynchronously.
///
/// Created by [`crate::SpawnedDataflow::take_async_output`] when the dataflow was
/// spawned with [`crate::IoMode::Async`]. Backed by a
/// `tokio::sync::mpsc::Receiver` — the `recv()` method yields when no data
/// is available, resuming when the dataflow produces output.
///
/// Each successful `recv()` notifies the executor's `WakeHandle`, allowing
/// a backpressure-blocked sink operator to retry sending.
///
/// Dropping the receiver abandons the port: the dataflow keeps running and
/// discards the port's output instead of blocking on it.
///
/// For an output declared inside an `iterate` body, each batch is delivered at
/// the dataflow's (outer) timestamp, as for [`OutputReceiver`].
pub struct AsyncOutputReceiver<T: Timestamp, D: Send + 'static> {
    receiver: AsyncOutputRecv<T, D>,
    /// Wake handle to notify when capacity is freed (unblocks backpressured sinks).
    wake_handle: Option<WakeHandle>,
}

impl<T: Timestamp, D: Send + 'static> AsyncOutputReceiver<T, D> {
    pub(crate) fn new(receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>) -> Self {
        Self {
            receiver: AsyncOutputRecv::Typed(receiver),
            wake_handle: None,
        }
    }

    pub(crate) fn with_wake_handle(
        receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            receiver: AsyncOutputRecv::Typed(receiver),
            wake_handle: Some(wake_handle),
        }
    }

    pub(crate) fn from_projected(receiver: ProjectedAsyncOutputReceiver<D>) -> Self {
        Self {
            receiver: AsyncOutputRecv::Projected(receiver.receiver),
            wake_handle: receiver.wake_handle,
        }
    }

    /// Receive the next output event asynchronously.
    ///
    /// Returns `None` when the dataflow completes and the output channel is
    /// closed. Notifies the executor's wake handle after each successful
    /// receive so that backpressure-blocked sinks can retry.
    pub async fn recv(&mut self) -> Option<OutputEvent<T, D>> {
        let result = match &mut self.receiver {
            AsyncOutputRecv::Typed(rx) => rx.recv().await,
            AsyncOutputRecv::Projected(rx) => rx.recv().await.map(unerase_event),
        };
        if result.is_some() {
            if let Some(wh) = &self.wake_handle {
                wh.notify();
            }
        }
        result
    }

    /// Try to receive the next output event (non-blocking).
    pub fn try_recv(&mut self) -> Option<OutputEvent<T, D>> {
        let result = match &mut self.receiver {
            AsyncOutputRecv::Typed(rx) => rx.try_recv().ok(),
            AsyncOutputRecv::Projected(rx) => rx.try_recv().ok().map(unerase_event),
        };
        if result.is_some() {
            if let Some(wh) = &self.wake_handle {
                wh.notify();
            }
        }
        result
    }

    /// Collect all output data asynchronously until the dataflow completes.
    ///
    /// Returns only `Data` events as `(time, data)` pairs.
    pub async fn collect_data(&mut self) -> Vec<(T, Vec<D>)> {
        let mut results = Vec::new();
        while let Some(event) = self.recv().await {
            if let OutputEvent::Data { time, data } = event {
                results.push((time, data));
            }
        }
        results
    }
}

impl<T: Timestamp, D: Send + 'static> Drop for AsyncOutputReceiver<T, D> {
    fn drop(&mut self) {
        // Notify the executor that this receiver is gone so backpressure-blocked
        // sinks observe the closed channel promptly instead of waiting for a poll.
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::bounded::bounded_channel;
    use crate::dataflow::channels::envelope::Envelope;
    use crate::dataflow::channels::pushpull::{Pull, Push};
    use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
    use crate::progress::operate::{PortLedger, ProgressReporter};

    /// A source shell on `receiver` emitting into `push`; with `reporter`,
    /// it holds its initial capability through it.
    fn test_source<T: Timestamp>(
        receiver: InputRecv<InputEvent<T, i32>>,
        push: Box<dyn Push<T, i32>>,
        reporter: Option<ProgressReporter<T>>,
        counter: Arc<AtomicUsize>,
    ) -> (OperatorShell<ChannelSourceLogic<T, i32>>, PortLedger<T>) {
        let produced = PortLedger::new();
        let mut output = CountedOutput::new(
            "test:output".into(),
            push,
            produced.clone(),
            ProgressReporter::new(),
            None,
        );
        if let Some(reporter) = &reporter {
            output.set_reporter(reporter.clone());
        }
        let logic = ChannelSourceLogic::new(receiver, &output, reporter.is_some(), counter);
        (
            channel_source("data", 0, StageId::new(0), logic, output),
            produced,
        )
    }

    #[test]
    fn last_input_sender_disconnects_before_waking_executor() {
        use std::sync::Mutex;
        use std::task::{Wake, Waker};

        struct SourceWake {
            source: Mutex<OperatorShell<ChannelSourceLogic<u64, i32>>>,
            wake: WakeHandle,
        }
        impl Wake for SourceWake {
            fn wake(self: Arc<Self>) {
                self.wake.take_notification();
                self.source.lock().unwrap().activate().unwrap();
            }
        }

        for async_io in [false, true] {
            let wake = WakeHandle::new();
            let (receiver, drop_first, drop_last): (_, Box<dyn FnOnce()>, Box<dyn FnOnce()>) =
                if async_io {
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    let first = AsyncInputSender::with_wake_handle(tx, wake.clone());
                    let last = first.clone();
                    (
                        InputRecv::Tokio(rx),
                        Box::new(move || first.close()),
                        Box::new(move || drop(last)),
                    )
                } else {
                    let (tx, rx) = mpsc::sync_channel(1);
                    let first = InputSender::with_wake_handle(tx, wake.clone());
                    let last = first.clone();
                    (
                        InputRecv::Std(rx),
                        Box::new(move || first.close()),
                        Box::new(move || drop(last)),
                    )
                };
            let (push, _pull) = bounded_channel::<u64, i32, ()>(1);
            let counter = Arc::new(AtomicUsize::new(1));
            let reporter = ProgressReporter::new();
            let (source, _) = test_source(
                receiver,
                Box::new(push),
                Some(reporter.clone()),
                counter.clone(),
            );
            let executor = Arc::new(SourceWake {
                source: Mutex::new(source),
                wake: wake.clone(),
            });
            wake.take_notification();
            wake.register_waker(&Waker::from(executor.clone()));
            drop_first();
            assert!(!executor.source.lock().unwrap().is_done());
            drop_last();
            // Break the test waker cycle even if the assertion below fails.
            wake.clear_waker();
            assert!(
                executor.source.lock().unwrap().is_done() || wake.take_notification(),
                "EOF must be observed or followed by a fresh wake; async_io={async_io}"
            );
            executor.source.lock().unwrap().activate().unwrap();
            assert_eq!(counter.load(Ordering::SeqCst), 0);
            assert_eq!(
                reporter.drain(),
                vec![(0, -1)],
                "the capability is released"
            );
        }
    }

    type FrontierSource<T> = (
        OperatorShell<ChannelSourceLogic<T, i32>>,
        Box<dyn Pull<T, i32>>,
        ProgressReporter<T>,
        PortLedger<T>,
    );

    fn frontier_source<T: Timestamp>(
        events: Vec<InputEvent<T, i32>>,
        async_io: bool,
        reporting: bool,
        capacity: usize,
    ) -> FrontierSource<T> {
        let receiver = if async_io {
            let (tx, rx) = tokio::sync::mpsc::channel(events.len().max(1));
            for event in events {
                tx.try_send(event).unwrap();
            }
            InputRecv::Tokio(rx)
        } else {
            let (tx, rx) = mpsc::sync_channel(events.len());
            for event in events {
                tx.send(event).unwrap();
            }
            InputRecv::Std(rx)
        };
        let reporter = ProgressReporter::new();
        let (push, pull) = bounded_channel::<T, i32, ()>(capacity);
        let (source, produced) = test_source(
            receiver,
            Box::new(push),
            reporting.then(|| reporter.clone()),
            Arc::new(AtomicUsize::new(1)),
        );
        (source, Box::new(pull), reporter, produced)
    }

    #[test]
    fn input_frontier_rejects_late_data_before_forwarding_even_after_disconnect() {
        for async_io in [false, true] {
            for reporting in [false, true] {
                for late_batch in [vec![2], vec![]] {
                    let (mut source, mut pull, _, _) = frontier_source(
                        vec![
                            InputEvent::data(10u64, vec![1]),
                            InputEvent::frontier(11),
                            InputEvent::data(10, late_batch),
                        ],
                        async_io,
                        reporting,
                        16,
                    );
                    // The valid prefix is emitted and committed; the
                    // violation then fails the operator.
                    assert!(matches!(
                        source.activate(),
                        Ok(ActivationOutcome::MadeProgress)
                    ));
                    assert!(
                        matches!(
                            source.activate(),
                            Err(crate::Error::Progress(ProgressError::InputTimeBeforeFrontier {
                                time, frontier
                            })) if time == "10" && frontier == "11"
                        ),
                        "late data must fail execution"
                    );
                    assert_eq!(pull.pull().unwrap().as_data(), Some((&10, &vec![1])));
                    assert!(pull.pull().is_none(), "invalid data must not be forwarded");
                }
            }
        }
    }

    #[test]
    fn input_frontier_rejects_backward_advance_without_changing_progress() {
        for async_io in [false, true] {
            let (mut source, _pull, reporter, _) = frontier_source(
                vec![InputEvent::frontier(5u64), InputEvent::frontier(3)],
                async_io,
                true,
                16,
            );
            source.activate().unwrap();
            assert!(matches!(
                source.activate(),
                Err(crate::Error::Progress(ProgressError::InvalidInputFrontier { from, to }))
                    if from == "5" && to == "3"
            ));
            assert_eq!(reporter.drain(), vec![(0, -1), (5, 1)]);
        }
    }

    #[test]
    fn input_frontier_uses_partial_not_lexicographic_order() {
        use crate::order::Product;
        for async_io in [false, true] {
            for reporting in [false, true] {
                for invalid in [
                    InputEvent::data(Product::new(2u64, 2u64), vec![1]),
                    InputEvent::frontier(Product::new(2u64, 2u64)),
                ] {
                    let (mut source, mut pull, _, _) = frontier_source(
                        vec![InputEvent::frontier(Product::new(1, 3)), invalid],
                        async_io,
                        reporting,
                        16,
                    );
                    let first = source.activate();
                    assert!(
                        first.is_err() || source.activate().is_err(),
                        "incomparable times are invalid"
                    );
                    assert!(pull.pull().is_none());
                }
            }
        }
    }

    #[test]
    fn input_frontier_allows_equal_repeated_and_out_of_order_data() {
        for async_io in [false, true] {
            let (mut source, mut pull, reporter, _) = frontier_source(
                vec![
                    InputEvent::frontier(i64::MIN),
                    InputEvent::data(i64::MIN, vec![0]),
                    InputEvent::frontier(-5),
                    InputEvent::frontier(-5),
                    InputEvent::data(10, vec![1]),
                    InputEvent::data(-5, vec![2]),
                    InputEvent::data(-5, vec![3]),
                    InputEvent::frontier(i64::MAX),
                    InputEvent::data(i64::MAX, vec![4]),
                ],
                async_io,
                true,
                16,
            );
            assert!(matches!(
                source.activate().unwrap(),
                ActivationOutcome::Done
            ));
            let mut observed = Vec::new();
            while let Some(envelope) = pull.pull() {
                let (time, data) = envelope.as_data().unwrap();
                observed.push((*time, data.clone()));
            }
            assert_eq!(
                observed,
                vec![
                    (i64::MIN, vec![0]),
                    (10, vec![1]),
                    (-5, vec![2]),
                    (-5, vec![3]),
                    (i64::MAX, vec![4])
                ]
            );
            assert_eq!(
                reporter.drain(),
                vec![(i64::MIN, -1)],
                "last sender releases the capability"
            );
        }
    }

    #[test]
    fn input_frontier_may_pass_counted_backpressured_data() {
        // The source emits into its output session; the shell counts the
        // output as produced before it waits in the retry queue, so the
        // source's frontier may move past data that does not fit into the
        // channel yet: it is counted at the consuming input.
        for async_io in [false, true] {
            let (mut source, mut pull, reporter, produced) = frontier_source(
                vec![
                    InputEvent::data(10u64, vec![1]),
                    InputEvent::data(10, vec![2]),
                    InputEvent::frontier(11),
                    InputEvent::data(10, vec![3]),
                ],
                async_io,
                true,
                1,
            );
            assert!(matches!(
                source.activate().unwrap(),
                ActivationOutcome::BlockedOnBackpressure
            ));
            assert_eq!(produced.drain(), vec![(10, 2)]);
            assert_eq!(reporter.drain(), vec![(0, -1), (11, 1)]);
            assert_eq!(pull.pull().unwrap().as_data(), Some((&10, &vec![1])));
            assert!(source.activate().is_err(), "data at 10 after frontier 11");
        }
    }

    #[test]
    fn branched_input_preserves_batches_and_counts_them_under_backpressure() {
        use crate::dataflow::channels::tee::TeePush;

        let (tx, rx) = mpsc::sync_channel(4);
        let (a, mut a_rx) = bounded_channel::<u64, i32, ()>(1);
        let (mut b, mut b_rx) = bounded_channel::<u64, i32, ()>(1);
        b.push(Envelope::data(11, vec![-1])).unwrap();
        let tee = TeePush::new(vec![Box::new(a), Box::new(b)]).unwrap();
        let reporter = ProgressReporter::new();
        let (mut source, produced) = test_source(
            InputRecv::Std(rx),
            Box::new(tee),
            Some(reporter.clone()),
            Arc::new(AtomicUsize::new(1)),
        );
        tx.send(InputEvent::frontier(11)).unwrap();
        tx.send(InputEvent::data(11, vec![1, 2])).unwrap();
        tx.send(InputEvent::frontier(12)).unwrap();
        drop(tx);
        assert!(matches!(
            source.activate().unwrap(),
            ActivationOutcome::BlockedOnBackpressure
        ));
        // Produced once (the tee's edges each count it), capability gone.
        assert_eq!(produced.drain(), vec![(11, 2)]);
        assert_eq!(reporter.drain(), vec![(0, -1)]);
        assert_eq!(a_rx.pull().unwrap().as_data(), Some((&11, &vec![1, 2])));
        assert_eq!(b_rx.pull().unwrap().as_data(), Some((&11, &vec![-1])));
        assert!(matches!(
            source.activate().unwrap(),
            ActivationOutcome::Done
        ));
        assert_eq!(b_rx.pull().unwrap().as_data(), Some((&11, &vec![1, 2])));
        assert!(
            a_rx.pull().is_none() && b_rx.pull().is_none(),
            "no duplicate or minimum-time placeholder"
        );
    }

    #[tokio::test]
    async fn input_frontier_cancelled_async_calls_do_not_enqueue_events() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let sender = AsyncInputSender::with_wake_handle(tx, WakeHandle::new());
        let (push, mut pull) = bounded_channel::<u64, i32, ()>(4);
        let reporter = ProgressReporter::new();
        let (mut source, _) = test_source(
            InputRecv::Tokio(rx),
            Box::new(push),
            Some(reporter.clone()),
            Arc::new(AtomicUsize::new(1)),
        );
        sender.send(0, vec![1]).await.unwrap();
        {
            let mut advance = Box::pin(sender.advance_to(11));
            assert!(matches!(
                advance
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        source.activate().unwrap();
        assert!(reporter.drain().is_empty());
        sender.send(10, vec![2]).await.unwrap();
        {
            let mut send = Box::pin(sender.send(12, vec![3]));
            assert!(matches!(
                send.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        drop(sender);
        assert!(matches!(
            source.activate().unwrap(),
            ActivationOutcome::Done
        ));
        assert_eq!(pull.pull().unwrap().as_data(), Some((&0, &vec![1])));
        assert_eq!(pull.pull().unwrap().as_data(), Some((&10, &vec![2])));
        assert!(pull.pull().is_none());
        assert_eq!(reporter.drain(), vec![(0, -1)]);
    }

    #[test]
    fn channel_source_receives_data() {
        let (tx, rx) = mpsc::sync_channel(16);
        let (push, mut pull) = bounded_channel::<u64, i32, ()>(1024);
        let counter = Arc::new(AtomicUsize::new(1));
        let (mut op, _) = test_source(
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );

        tx.send(InputEvent::data(0, vec![1, 2, 3])).unwrap();
        tx.send(InputEvent::data(1, vec![4, 5])).unwrap();

        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::MadeProgress));

        let env1 = pull.pull().unwrap();
        let (t, d) = env1.as_data().unwrap();
        assert_eq!(*t, 0);
        assert_eq!(d, &vec![1, 2, 3]);

        let env2 = pull.pull().unwrap();
        let (t, d) = env2.as_data().unwrap();
        assert_eq!(*t, 1);
        assert_eq!(d, &vec![4, 5]);

        assert!(!op.is_done());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn channel_source_finishes_on_disconnect() {
        let (tx, rx) = mpsc::sync_channel::<InputEvent<u64, i32>>(16);
        let (push, _pull) = bounded_channel::<u64, i32, ()>(1024);
        let counter = Arc::new(AtomicUsize::new(1));
        let (mut op, _) = test_source(
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );
        drop(tx);
        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::Done));
        assert!(op.is_done());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn abandoned_source_neither_finishes_nor_closes_its_output() {
        for abandoned in [false, true] {
            let (tx, rx) = mpsc::sync_channel::<InputEvent<u64, i32>>(16);
            let (push, pull) = bounded_channel::<u64, i32, ()>(1024);
            let counter = Arc::new(AtomicUsize::new(1));
            let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut output = CountedOutput::new(
                "test:output".into(),
                Box::new(push) as Box<dyn Push<u64, i32>>,
                PortLedger::new(),
                ProgressReporter::new(),
                None,
            );
            output.set_reporter(ProgressReporter::new());
            let logic =
                ChannelSourceLogic::new(InputRecv::Std(rx), &output, false, counter.clone())
                    .with_abandon_flag(Some(Arc::clone(&flag)));
            let mut op = channel_source("async_input", 0, StageId::new(0), logic, output);
            // The pump flags a cancellation before it drops the sender.
            flag.store(abandoned, Ordering::SeqCst);
            drop(tx);
            let outcome = op.activate().unwrap();
            if abandoned {
                assert!(matches!(outcome, ActivationOutcome::Idle));
                assert!(!op.is_done());
                assert!(
                    !pull.is_exhausted(),
                    "a cut-off input must not look complete"
                );
                assert_eq!(counter.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(outcome, ActivationOutcome::Done));
                assert!(pull.is_exhausted());
                assert_eq!(counter.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[test]
    fn channel_source_idle_when_empty() {
        let (_tx, rx) = mpsc::sync_channel::<InputEvent<u64, i32>>(16);
        let (push, _pull) = bounded_channel::<u64, i32, ()>(1024);
        let counter = Arc::new(AtomicUsize::new(1));
        let (mut op, _) = test_source(
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );
        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::Idle));
        assert!(!op.is_done());
    }

    fn test_sink(
        pull: Box<dyn Pull<u64, i32>>,
        sender: OutputSend<OutputEvent<u64, i32>>,
    ) -> (
        OperatorShell<ChannelSinkLogic<u64, i32, OutputEvent<u64, i32>>>,
        PortLedger<u64>,
    ) {
        let consumed = PortLedger::new();
        let input = CountedInput::new("test:input".into(), pull, consumed.clone());
        (
            channel_sink(
                "test_output",
                0,
                StageId::new(0),
                input,
                sender,
                Box::new(OutputEvent::data),
            ),
            consumed,
        )
    }

    #[test]
    fn channel_sink_forwards_data() {
        let (push, pull) = bounded_channel::<u64, i32, ()>(1024);
        let (tx, rx) = mpsc::sync_channel::<OutputEvent<u64, i32>>(16);
        let mut pusher: Box<dyn Push<u64, i32>> = Box::new(push);
        pusher.push(Envelope::data(0, vec![10, 20])).unwrap();
        pusher.push(Envelope::data(1, vec![30])).unwrap();
        pusher.close();

        let (mut op, consumed) = test_sink(Box::new(pull), OutputSend::Std(tx));
        let _ = op.activate().unwrap();
        if !op.is_done() {
            let _ = op.activate();
        }
        assert_eq!(rx.recv().unwrap(), OutputEvent::data(0, vec![10, 20]));
        assert_eq!(rx.recv().unwrap(), OutputEvent::data(1, vec![30]));
        assert_eq!(consumed.drain(), vec![(0, 2), (1, 1)]);
    }

    #[test]
    fn channel_sink_keeps_credit_until_the_receiver_accepts() {
        let (push, pull) = bounded_channel::<u64, i32, ()>(1024);
        let (tx, rx) = mpsc::sync_channel::<OutputEvent<u64, i32>>(1);
        let mut pusher: Box<dyn Push<u64, i32>> = Box::new(push);
        pusher.push(Envelope::data(0, vec![1])).unwrap();
        pusher.push(Envelope::data(1, vec![2, 3])).unwrap();

        let (mut op, consumed) = test_sink(Box::new(pull), OutputSend::Std(tx));
        op.activate().unwrap();
        // The paused receiver holds the second batch: still counted.
        assert_eq!(consumed.drain(), vec![(0, 1)]);
        assert_eq!(rx.recv().unwrap(), OutputEvent::data(0, vec![1]));
        op.activate().unwrap();
        assert_eq!(consumed.drain(), vec![(1, 2)]);
    }

    #[test]
    fn channel_sink_discards_explicitly_when_the_receiver_is_gone() {
        let (push, pull) = bounded_channel::<u64, i32, ()>(1024);
        let (tx, rx) = mpsc::sync_channel::<OutputEvent<u64, i32>>(1);
        drop(rx);
        let mut pusher: Box<dyn Push<u64, i32>> = Box::new(push);
        pusher.push(Envelope::data(0, vec![1, 2])).unwrap();
        pusher.close();
        let (mut op, consumed) = test_sink(Box::new(pull), OutputSend::Std(tx));
        while !op.is_done() {
            op.activate().unwrap();
        }
        assert_eq!(consumed.drain(), vec![(0, 2)], "the discard is recorded");
    }

    #[test]
    fn input_sender_output_receiver_roundtrip() {
        let (tx, rx) = mpsc::sync_channel(16);
        let sender = InputSender::<u64, i32>::new(tx);
        let (out_tx, out_rx) = mpsc::sync_channel(16);
        let receiver = OutputReceiver::<u64, i32>::new(out_rx);

        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.send(1, vec![4]).unwrap();
        sender.close();

        let e1 = rx.recv().unwrap();
        assert!(matches!(e1, InputEvent::Data { time: 0, .. }));
        let e2 = rx.recv().unwrap();
        assert!(matches!(e2, InputEvent::Data { time: 1, .. }));
        assert!(rx.recv().is_err());

        out_tx.send(OutputEvent::data(0, vec![10])).unwrap();
        drop(out_tx);

        let data = receiver.collect_data();
        assert_eq!(data, vec![(0, vec![10])]);
    }
}
