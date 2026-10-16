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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::dataflow::stage::StageId;
use crate::error::{ProgressError, Result};
use crate::progress::operate::ProgressReporter;
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
// ChannelSourceOperator
// ---------------------------------------------------------------------------

/// A source operator that receives data from an external `mpsc` channel.
///
/// Created by [`crate::dataflow::DataflowHandle`] for each declared `input()`
/// port. The caller sends [`InputEvent`]s through
/// an [`InputSender`], which this operator consumes and pushes downstream.
///
/// # Quiescence behavior
///
/// While this operator's channel is open, it prevents the executor from
/// declaring quiescence by holding a count in `external_inputs_open`.
/// When the channel is closed (sender dropped or explicit close), the
/// count is decremented and the operator marks itself as done.
pub struct ChannelSourceOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    stage_id: StageId,
    receiver: InputRecv<InputEvent<T, D>>,
    output_pusher: Box<dyn Push<T, D>>,
    pending_output: VecDeque<Envelope<T, D>>,
    progress_reporter: Option<ProgressReporter<T>>,
    external_inputs_open: Arc<AtomicUsize>,
    /// The timestamp of the currently held capability. Starts at T::minimum()
    /// and advances when Frontier events are received.
    current_capability: T,
    /// Until the first advance, Timestamp::minimum() permits every valid time.
    frontier_advanced: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> ChannelSourceOperator<T, D> {
    /// Create a new channel source operator.
    pub(crate) fn new(
        name: String,
        index: usize,
        stage_id: StageId,
        receiver: InputRecv<InputEvent<T, D>>,
        output_pusher: Box<dyn Push<T, D>>,
        progress_reporter: Option<ProgressReporter<T>>,
        external_inputs_open: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            name,
            index,
            stage_id,
            receiver,
            output_pusher,
            pending_output: VecDeque::new(),
            progress_reporter,
            external_inputs_open,
            current_capability: T::minimum(),
            frontier_advanced: false,
            done: false,
        }
    }

    /// Try to flush pending output to the downstream pusher.
    /// Returns true if all pending output was flushed.
    fn flush_pending(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((crate::error::Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _)) => return Err(e),
            }
        }
        Ok(true)
    }

    /// Mark this operator as done and release resources.
    fn finish(&mut self) {
        if self.done {
            return;
        }
        // Release the currently held capability
        if let Some(ref reporter) = self.progress_reporter {
            reporter.update(self.current_capability.clone(), -1);
        }
        self.output_pusher.close();
        self.done = true;
        // Decrement external input counter so executor can detect quiescence
        self.external_inputs_open.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for ChannelSourceOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // First, try to flush any pending output from previous activation.
        if !self.flush_pending()? {
            return Ok(ActivationOutcome::BlockedOnBackpressure);
        }

        // Poll the channel for new events (non-blocking).
        // We cap per-activation work to avoid starving other operators in the
        // cooperative executor. The cap applies to data events; frontier and
        // control events are lightweight and don't count.
        const ACTIVATION_BUDGET: usize = 1024;
        let mut made_progress = false;
        let mut data_events = 0usize;

        loop {
            if data_events >= ACTIVATION_BUDGET {
                // Yield to the scheduler so other operators get a turn.
                break;
            }
            match self.receiver.try_recv() {
                Ok(InputEvent::Data { time, data }) => {
                    if self.frontier_advanced && !self.current_capability.less_equal(&time) {
                        return Err(ProgressError::InputTimeBeforeFrontier {
                            time: format!("{time:?}"),
                            frontier: format!("{:?}", self.current_capability),
                        }
                        .into());
                    }
                    let envelope = Envelope::data(time, data);
                    match self.output_pusher.try_push(envelope) {
                        Ok(()) => {
                            made_progress = true;
                            data_events += 1;
                        }
                        Err((crate::error::Error::Backpressure, returned)) => {
                            self.pending_output.push_back(returned);
                            return Ok(ActivationOutcome::BlockedOnBackpressure);
                        }
                        Err((e, _)) => return Err(e),
                    }
                }
                Ok(InputEvent::Frontier(time)) => {
                    if !self.current_capability.less_equal(&time) {
                        return Err(ProgressError::InvalidInputFrontier {
                            from: format!("{:?}", self.current_capability),
                            to: format!("{time:?}"),
                        }
                        .into());
                    }
                    if time != self.current_capability {
                        if let Some(ref reporter) = self.progress_reporter {
                            reporter.downgrade(self.current_capability.clone(), time.clone());
                        }
                        self.current_capability = time;
                        self.frontier_advanced = true;
                    }
                    made_progress = true;
                }
                Err(RecvError::Empty) => {
                    // No data available right now. Return Idle.
                    break;
                }
                Err(RecvError::Disconnected) => {
                    // Sender dropped — input is closed. Finish up.
                    self.finish();
                    return Ok(ActivationOutcome::Done);
                }
            }
        }

        if made_progress {
            Ok(ActivationOutcome::MadeProgress)
        } else {
            Ok(ActivationOutcome::Idle)
        }
    }

    fn is_done(&self) -> bool {
        self.done
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

    fn close_inputs(&mut self) {
        // Channel source has no dataflow inputs — closing means finishing.
        self.finish();
    }
}

// ---------------------------------------------------------------------------
// ChannelSinkOperator
// ---------------------------------------------------------------------------

/// A sink operator that sends output data to an external `mpsc` channel.
///
/// Created by [`crate::dataflow::DataflowHandle`] for each declared `output()`
/// port. Data received from upstream is forwarded
/// as [`OutputEvent`]s to an [`OutputReceiver`].
pub struct ChannelSinkOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    stage_id: StageId,
    input_puller: Box<dyn Pull<T, D>>,
    sender: OutputSend<OutputEvent<T, D>>,
    /// Buffered output event that couldn't be sent due to channel full.
    pending_send: Option<OutputEvent<T, D>>,
    input_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> ChannelSinkOperator<T, D> {
    /// Create a new channel sink operator.
    pub(crate) fn new(
        name: String,
        index: usize,
        stage_id: StageId,
        input_puller: Box<dyn Pull<T, D>>,
        sender: OutputSend<OutputEvent<T, D>>,
    ) -> Self {
        Self {
            name,
            index,
            stage_id,
            input_puller,
            sender,
            pending_send: None,
            input_exhausted: false,
            done: false,
        }
    }

    /// Try to send an event, returning false if the channel is full.
    /// Returns Err if the receiver was dropped (output no longer needed).
    fn try_send_event(&mut self, event: OutputEvent<T, D>) -> std::result::Result<bool, ()> {
        match self.sender.try_send(event) {
            Ok(()) => Ok(true),
            Err(SendError::Full(event)) => {
                self.pending_send = Some(event);
                Ok(false)
            }
            Err(SendError::Disconnected(_)) => Err(()),
        }
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for ChannelSinkOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // First, try to drain any pending send from a previous activation.
        if let Some(event) = self.pending_send.take() {
            match self.try_send_event(event) {
                Ok(true) => {} // sent successfully
                Ok(false) => return Ok(ActivationOutcome::BlockedOnBackpressure),
                Err(()) => {
                    self.done = true;
                    return Ok(ActivationOutcome::Done);
                }
            }
        }

        let mut made_progress = false;

        // Pull data from upstream and forward to channel (non-blocking).
        loop {
            match self.input_puller.pull() {
                Some(envelope) => {
                    match envelope.payload {
                        crate::dataflow::channels::envelope::Payload::Data { time, data } => {
                            match self.try_send_event(OutputEvent::data(time, data)) {
                                Ok(true) => {
                                    made_progress = true;
                                }
                                Ok(false) => {
                                    // Channel full — stop pulling, will retry next activation
                                    return Ok(ActivationOutcome::BlockedOnBackpressure);
                                }
                                Err(()) => {
                                    // Receiver dropped — output no longer needed
                                    self.done = true;
                                    return Ok(ActivationOutcome::Done);
                                }
                            }
                        }
                        crate::dataflow::channels::envelope::Payload::Control(_) => {
                            // Control signals are not forwarded to external consumers.
                            made_progress = true;
                        }
                    }
                }
                None => {
                    // No more data available right now.
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                    }
                    break;
                }
            }
        }

        if self.input_exhausted {
            self.done = true;
            return Ok(ActivationOutcome::Done);
        }

        if made_progress {
            Ok(ActivationOutcome::MadeProgress)
        } else {
            Ok(ActivationOutcome::Idle)
        }
    }

    fn is_done(&self) -> bool {
        self.done
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

    fn close_inputs(&mut self) {
        self.input_exhausted = true;
    }
}

// ---------------------------------------------------------------------------
// InputSender / OutputReceiver — user-facing handles
// ---------------------------------------------------------------------------

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
#[derive(Clone, Debug)]
pub struct InputSender<T: Timestamp, D: Send + 'static> {
    sender: mpsc::SyncSender<InputEvent<T, D>>,
    /// Optional wake handle to notify the executor when data is sent.
    /// Set during runtime spawn; None in standalone / test usage.
    wake_handle: Option<WakeHandle>,
}

impl<T: Timestamp, D: Send + 'static> InputSender<T, D> {
    #[cfg(test)]
    pub(crate) fn new(sender: mpsc::SyncSender<InputEvent<T, D>>) -> Self {
        Self {
            sender,
            wake_handle: None,
        }
    }

    /// Create an InputSender with a wake handle for async executor notification.
    pub(crate) fn with_wake_handle(
        sender: mpsc::SyncSender<InputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            sender,
            wake_handle: Some(wake_handle),
        }
    }

    /// Send a batch of data at the given timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataflow has already terminated.
    /// The batch time must be at or beyond the committed frontier in the partial
    /// order, including for empty batches. Violations are reported by execution,
    /// not necessarily by this call. Batch timestamps need not arrive sorted.
    pub fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.sender
            .send(InputEvent::data(time, data))
            .map_err(|_| crate::error::Error::ChannelClosed)?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
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
        self.sender
            .send(InputEvent::frontier(time))
            .map_err(|_| crate::error::Error::ChannelClosed)?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
    }

    /// Close this input, signaling no more data will arrive.
    ///
    /// Equivalent to dropping the sender.
    pub fn close(self) {
        // Drop self — the SyncSender will be dropped, triggering the
        // Drop impl which notifies the wake handle.
    }
}

impl<T: Timestamp, D: Send + 'static> Drop for InputSender<T, D> {
    fn drop(&mut self) {
        // Notify the executor that an input sender was dropped. This may
        // be the last clone, causing the channel to disconnect — the executor
        // needs to wake up to observe the closed input and potentially complete.
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
    }
}

/// A handle for receiving output from a dataflow output port.
///
/// Created by [`crate::SpawnedDataflow::take_output`]. Receives [`OutputEvent`]s
/// containing timestamped batches of results.
pub struct OutputReceiver<T: Timestamp, D: Send + 'static> {
    receiver: mpsc::Receiver<OutputEvent<T, D>>,
}

impl<T: Timestamp, D: Send + 'static> OutputReceiver<T, D> {
    pub(crate) fn new(receiver: mpsc::Receiver<OutputEvent<T, D>>) -> Self {
        Self { receiver }
    }

    /// Receive the next output event (blocking).
    ///
    /// Returns `None` when the dataflow completes and the output channel is closed.
    pub fn recv(&self) -> Option<OutputEvent<T, D>> {
        self.receiver.recv().ok()
    }

    /// Try to receive the next output event (non-blocking).
    pub fn try_recv(&self) -> Option<OutputEvent<T, D>> {
        self.receiver.try_recv().ok()
    }

    /// Receive with a timeout.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<OutputEvent<T, D>> {
        self.receiver.recv_timeout(timeout).ok()
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
#[derive(Clone)]
pub struct AsyncInputSender<T: Timestamp, D: Send + 'static> {
    sender: tokio::sync::mpsc::Sender<InputEvent<T, D>>,
    wake_handle: Option<WakeHandle>,
}

impl<T: Timestamp, D: Send + 'static> AsyncInputSender<T, D> {
    pub(crate) fn with_wake_handle(
        sender: tokio::sync::mpsc::Sender<InputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            sender,
            wake_handle: Some(wake_handle),
        }
    }

    /// Send a batch of data at the given timestamp.
    ///
    /// Yields (returns `Poll::Pending`) if the channel is full, resuming
    /// when capacity becomes available. This provides backpressure without
    /// blocking an OS thread.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataflow has already terminated.
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
        self.sender
            .send(InputEvent::data(time, data))
            .await
            .map_err(|_| crate::error::Error::ChannelClosed)?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
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
        self.sender
            .send(InputEvent::frontier(time))
            .await
            .map_err(|_| crate::error::Error::ChannelClosed)?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
    }

    /// Close this input, signaling no more data will arrive.
    ///
    /// Equivalent to dropping the sender.
    pub fn close(self) {
        // Drop self — triggers the Drop impl which notifies the wake handle.
    }
}

impl<T: Timestamp, D: Send + 'static> Drop for AsyncInputSender<T, D> {
    fn drop(&mut self) {
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
    }
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
pub struct AsyncOutputReceiver<T: Timestamp, D: Send + 'static> {
    receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>,
    /// Wake handle to notify when capacity is freed (unblocks backpressured sinks).
    wake_handle: Option<WakeHandle>,
}

impl<T: Timestamp, D: Send + 'static> AsyncOutputReceiver<T, D> {
    pub(crate) fn new(receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>) -> Self {
        Self {
            receiver,
            wake_handle: None,
        }
    }

    pub(crate) fn with_wake_handle(
        receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>,
        wake_handle: WakeHandle,
    ) -> Self {
        Self {
            receiver,
            wake_handle: Some(wake_handle),
        }
    }

    /// Receive the next output event asynchronously.
    ///
    /// Returns `None` when the dataflow completes and the output channel is
    /// closed. Notifies the executor's wake handle after each successful
    /// receive so that backpressure-blocked sinks can retry.
    pub async fn recv(&mut self) -> Option<OutputEvent<T, D>> {
        let result = self.receiver.recv().await;
        if result.is_some() {
            if let Some(wh) = &self.wake_handle {
                wh.notify();
            }
        }
        result
    }

    /// Try to receive the next output event (non-blocking).
    pub fn try_recv(&mut self) -> Option<OutputEvent<T, D>> {
        let result = self.receiver.try_recv().ok();
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
    use crate::dataflow::channels::pushpull::Push;

    fn frontier_source<T: Timestamp>(
        events: Vec<InputEvent<T, i32>>,
        async_io: bool,
        reporting: bool,
        capacity: usize,
    ) -> (
        ChannelSourceOperator<T, i32>,
        impl Pull<T, i32>,
        ProgressReporter<T>,
    ) {
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
        reporter.update(T::minimum(), 1);
        let (push, pull) = bounded_channel::<T, i32, ()>(capacity);
        let source = ChannelSourceOperator::new(
            "frontier_input".into(),
            0,
            StageId::new(0),
            receiver,
            Box::new(push),
            reporting.then(|| reporter.clone()),
            Arc::new(AtomicUsize::new(1)),
        );
        (source, pull, reporter)
    }

    #[test]
    fn input_frontier_rejects_late_data_before_forwarding_even_after_disconnect() {
        for async_io in [false, true] {
            for reporting in [false, true] {
                for late_batch in [vec![2], vec![]] {
                    let (mut source, mut pull, _) = frontier_source(
                        vec![
                            InputEvent::data(10u64, vec![1]),
                            InputEvent::frontier(11),
                            InputEvent::data(10, late_batch),
                        ],
                        async_io,
                        reporting,
                        16,
                    );
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
            let (mut source, _pull, reporter) = frontier_source(
                vec![InputEvent::frontier(5u64), InputEvent::frontier(3)],
                async_io,
                true,
                16,
            );
            assert!(matches!(
                source.activate(),
                Err(crate::Error::Progress(ProgressError::InvalidInputFrontier { from, to }))
                    if from == "5" && to == "3"
            ));
            assert_eq!(reporter.drain(), vec![(5, 1)]);
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
                    let (mut source, mut pull, _) = frontier_source(
                        vec![InputEvent::frontier(Product::new(1, 3)), invalid],
                        async_io,
                        reporting,
                        16,
                    );
                    assert!(source.activate().is_err(), "incomparable times are invalid");
                    assert!(pull.pull().is_none());
                }
            }
        }
    }

    #[test]
    fn input_frontier_allows_equal_repeated_and_out_of_order_data() {
        for async_io in [false, true] {
            let (mut source, mut pull, reporter) = frontier_source(
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
            assert!(
                reporter.drain().is_empty(),
                "last sender releases the frontier"
            );
        }
    }

    #[test]
    fn input_frontier_cannot_overtake_backpressured_data() {
        for async_io in [false, true] {
            let (mut source, mut pull, reporter) = frontier_source(
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
            assert_eq!(reporter.drain(), vec![(0, 1)]);
            assert_eq!(pull.pull().unwrap().as_data(), Some((&10, &vec![1])));
            assert!(source.activate().is_err());
            assert_eq!(pull.pull().unwrap().as_data(), Some((&10, &vec![2])));
            assert!(pull.pull().is_none());
        }
    }

    #[tokio::test]
    async fn input_frontier_cancelled_async_calls_do_not_enqueue_events() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let sender = AsyncInputSender::with_wake_handle(tx, WakeHandle::new());
        let (push, mut pull) = bounded_channel::<u64, i32, ()>(4);
        let reporter = ProgressReporter::new();
        reporter.update(0, 1);
        let mut source = ChannelSourceOperator::new(
            "data".into(),
            0,
            StageId::new(0),
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
        assert_eq!(reporter.drain(), vec![(0, 1)]);
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

        let mut op = ChannelSourceOperator::new(
            "test_input".into(),
            0,
            StageId::new(0),
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );

        // Send data
        tx.send(InputEvent::data(0, vec![1, 2, 3])).unwrap();
        tx.send(InputEvent::data(1, vec![4, 5])).unwrap();

        // Activate — should consume both events
        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::MadeProgress));

        // Pull from output
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
        let (tx, rx) = mpsc::sync_channel(16);
        let (push, _pull) = bounded_channel::<u64, i32, ()>(1024);
        let counter = Arc::new(AtomicUsize::new(1));

        let mut op = ChannelSourceOperator::new(
            "test_input".into(),
            0,
            StageId::new(0),
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );

        // Drop sender → channel disconnects
        drop(tx);

        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::Done));
        assert!(op.is_done());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn channel_source_idle_when_empty() {
        let (_tx, rx) = mpsc::sync_channel::<InputEvent<u64, i32>>(16);
        let (push, _pull) = bounded_channel::<u64, i32, ()>(1024);
        let counter = Arc::new(AtomicUsize::new(1));

        let mut op = ChannelSourceOperator::new(
            "test_input".into(),
            0,
            StageId::new(0),
            InputRecv::Std(rx),
            Box::new(push),
            None,
            Arc::clone(&counter),
        );

        let outcome = op.activate().unwrap();
        assert!(matches!(outcome, ActivationOutcome::Idle));
        assert!(!op.is_done());
    }

    #[test]
    fn channel_sink_forwards_data() {
        let (push, pull) = bounded_channel::<u64, i32, ()>(1024);
        let (tx, rx) = mpsc::sync_channel::<OutputEvent<u64, i32>>(16);

        // Push data into the channel that feeds the sink
        let mut pusher: Box<dyn Push<u64, i32>> = Box::new(push);
        pusher.push(Envelope::data(0, vec![10, 20])).unwrap();
        pusher.push(Envelope::data(1, vec![30])).unwrap();
        pusher.close();

        let mut op = ChannelSinkOperator::new(
            "test_output".into(),
            0,
            StageId::new(0),
            Box::new(pull),
            OutputSend::Std(tx),
        );

        // Activate — should forward data and detect input exhaustion
        let _outcome = op.activate().unwrap();
        // May need two activations since close propagates async
        if !op.is_done() {
            let _ = op.activate();
        }

        // Receive from channel
        let event1 = rx.recv().unwrap();
        assert_eq!(event1, OutputEvent::data(0, vec![10, 20]));
        let event2 = rx.recv().unwrap();
        assert_eq!(event2, OutputEvent::data(1, vec![30]));
    }

    #[test]
    fn input_sender_output_receiver_roundtrip() {
        let (tx, rx) = mpsc::sync_channel(16);
        let sender = InputSender::<u64, i32>::new(tx);
        let (out_tx, out_rx) = mpsc::sync_channel(16);
        let receiver = OutputReceiver::<u64, i32>::new(out_rx);

        // Send input
        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.send(1, vec![4]).unwrap();
        sender.close();

        // Verify input events
        let e1 = rx.recv().unwrap();
        assert!(matches!(e1, InputEvent::Data { time: 0, .. }));
        let e2 = rx.recv().unwrap();
        assert!(matches!(e2, InputEvent::Data { time: 1, .. }));
        assert!(rx.recv().is_err()); // closed

        // Send output
        out_tx.send(OutputEvent::data(0, vec![10])).unwrap();
        drop(out_tx);

        // Receive output
        let data = receiver.collect_data();
        assert_eq!(data, vec![(0, vec![10])]);
    }
}
