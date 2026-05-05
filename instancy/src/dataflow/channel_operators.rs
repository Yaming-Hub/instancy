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
//! (`tokio::sync::mpsc`, feature-gated behind `async-io`) channel backends.
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
use crate::error::Result;
use crate::order::PartialOrder;
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
    #[cfg(feature = "async-io")]
    Async,
}

// ---------------------------------------------------------------------------
// InputRecv / OutputSend — channel backend enums
// ---------------------------------------------------------------------------

/// Unified receiver for input channels, abstracting over std and tokio backends.
pub(crate) enum InputRecv<T> {
    Std(mpsc::Receiver<T>),
    #[cfg(feature = "async-io")]
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
            #[cfg(feature = "async-io")]
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
    #[cfg(feature = "async-io")]
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
            #[cfg(feature = "async-io")]
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
                    // Advance the held capability to the new frontier time.
                    // This releases the old capability and acquires a new one,
                    // allowing downstream frontier-sensitive operators (e.g.
                    // unary_notify) to fire notifications for completed times.
                    //
                    // Frontier must advance monotonically: the new time must be
                    // >= the current capability in the partial order.
                    if let Some(ref reporter) = self.progress_reporter {
                        if self.current_capability.less_than(&time) {
                            reporter.update(self.current_capability.clone(), -1);
                            reporter.update(time.clone(), 1);
                            self.current_capability = time;
                        } else if time != self.current_capability {
                            // Non-monotonic frontier: new time is incomparable or
                            // less than current. This is a protocol violation.
                            #[cfg(feature = "tracing")]
                            tracing::warn!(
                                operator = %self.name,
                                "ignoring non-monotonic frontier advancement"
                            );
                        }
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
/// clones share the same underlying channel. The channel closes only when
/// **all** clones (including the one held by `SpawnedDataflow`) are dropped,
/// or when [`close()`](Self::close) is called on this instance.
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
    #[allow(dead_code)]
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
    pub fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.sender
            .send(InputEvent::data(time, data))
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
    }

    /// Advance the input frontier to `time`.
    ///
    /// This declares that all data for timestamps **before** `time` has been
    /// sent. After this call, only `send()` with timestamps `>= time` is
    /// valid. Downstream `unary_notify` operators will receive notifications
    /// for any completed timestamps.
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
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))?;
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
/// spawned with [`crate::RuntimeHandle::spawn_async`]. Backed by a
/// `tokio::sync::mpsc::Sender` — the `send()` method yields when the channel
/// is full (backpressure) instead of blocking the calling thread.
///
/// When a `WakeHandle` is wired (set during runtime spawn), every `send()`,
/// `advance_to()`, and `Drop` notifies the executor's wake handle so
/// that a sleeping async executor wakes promptly when external data arrives.
#[cfg(feature = "async-io")]
#[derive(Clone)]
pub struct AsyncInputSender<T: Timestamp, D: Send + 'static> {
    sender: tokio::sync::mpsc::Sender<InputEvent<T, D>>,
    wake_handle: Option<WakeHandle>,
}

#[cfg(feature = "async-io")]
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
    pub async fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.sender
            .send(InputEvent::data(time, data))
            .await
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))?;
        if let Some(wh) = &self.wake_handle {
            wh.notify();
        }
        Ok(())
    }

    /// Advance the input frontier to `time`.
    ///
    /// This declares that all data for timestamps **before** `time` has been
    /// sent. After this call, only `send()` with timestamps `>= time` is
    /// valid. Downstream `unary_notify` operators will receive notifications
    /// for any completed timestamps.
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
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))?;
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

#[cfg(feature = "async-io")]
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
/// spawned with [`crate::RuntimeHandle::spawn_async`]. Backed by a
/// `tokio::sync::mpsc::Receiver` — the `recv()` method yields when no data
/// is available, resuming when the dataflow produces output.
///
/// Each successful `recv()` notifies the executor's `WakeHandle`, allowing
/// a backpressure-blocked sink operator to retry sending.
#[cfg(feature = "async-io")]
pub struct AsyncOutputReceiver<T: Timestamp, D: Send + 'static> {
    receiver: tokio::sync::mpsc::Receiver<OutputEvent<T, D>>,
    /// Wake handle to notify when capacity is freed (unblocks backpressured sinks).
    wake_handle: Option<WakeHandle>,
}

#[cfg(feature = "async-io")]
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

#[cfg(feature = "async-io")]
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
