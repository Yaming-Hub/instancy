//! Channel-based source and sink operators for async runtime integration.
//!
//! These operators bridge external callers to the dataflow graph:
//!
//! - [`ChannelSourceOperator`] receives [`InputEvent`]s from a `std::sync::mpsc`
//!   channel and pushes data into the dataflow as a source operator.
//! - [`ChannelSinkOperator`] collects data from the dataflow and sends
//!   [`OutputEvent`]s to a `std::sync::mpsc` channel for external consumption.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::operators::input::InputEvent;
use crate::dataflow::operators::output::OutputEvent;
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::error::Result;
use crate::progress::operate::ProgressReporter;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// ChannelSourceOperator
// ---------------------------------------------------------------------------

/// A source operator that receives data from an external `mpsc` channel.
///
/// Created by [`DataflowHandle`](super::dataflow_builder::DataflowHandle) for
/// each declared `input()` port. The caller sends [`InputEvent`]s through
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
    region_id: RegionId,
    receiver: mpsc::Receiver<InputEvent<T, D>>,
    output_pusher: Box<dyn Push<T, D>>,
    pending_output: VecDeque<Envelope<T, D>>,
    progress_reporter: Option<ProgressReporter<T>>,
    external_inputs_open: Arc<AtomicUsize>,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> ChannelSourceOperator<T, D> {
    /// Create a new channel source operator.
    pub fn new(
        name: String,
        index: usize,
        region_id: RegionId,
        receiver: mpsc::Receiver<InputEvent<T, D>>,
        output_pusher: Box<dyn Push<T, D>>,
        progress_reporter: Option<ProgressReporter<T>>,
        external_inputs_open: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            name,
            index,
            region_id,
            receiver,
            output_pusher,
            pending_output: VecDeque::new(),
            progress_reporter,
            external_inputs_open,
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
        // Release the initial capability
        if let Some(ref reporter) = self.progress_reporter {
            reporter.update(T::minimum(), -1);
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
        // We drain all available events in one activation to maximize throughput.
        let mut made_progress = false;

        loop {
            match self.receiver.try_recv() {
                Ok(InputEvent::Data { time, data }) => {
                    let envelope = Envelope::data(time, data);
                    match self.output_pusher.try_push(envelope) {
                        Ok(()) => {
                            made_progress = true;
                        }
                        Err((crate::error::Error::Backpressure, returned)) => {
                            self.pending_output.push_back(returned);
                            return Ok(ActivationOutcome::BlockedOnBackpressure);
                        }
                        Err((e, _)) => return Err(e),
                    }
                }
                Ok(InputEvent::Frontier(_time)) => {
                    // Frontier advancement — for now we don't propagate frontier
                    // events through the progress system (requires deeper integration).
                    // The operator simply advances when all data is received.
                    made_progress = true;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // No data available right now. Return Idle.
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
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

    fn region_id(&self) -> RegionId {
        self.region_id
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
/// Created by [`DataflowHandle`](super::dataflow_builder::DataflowHandle) for
/// each declared `output()` port. Data received from upstream is forwarded
/// as [`OutputEvent`]s to an [`OutputReceiver`].
pub struct ChannelSinkOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    input_puller: Box<dyn Pull<T, D>>,
    sender: mpsc::SyncSender<OutputEvent<T, D>>,
    input_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> ChannelSinkOperator<T, D> {
    /// Create a new channel sink operator.
    pub fn new(
        name: String,
        index: usize,
        region_id: RegionId,
        input_puller: Box<dyn Pull<T, D>>,
        sender: mpsc::SyncSender<OutputEvent<T, D>>,
    ) -> Self {
        Self {
            name,
            index,
            region_id,
            input_puller,
            sender,
            input_exhausted: false,
            done: false,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for ChannelSinkOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        let mut made_progress = false;

        // Pull data from upstream and forward to channel.
        loop {
            match self.input_puller.pull() {
                Some(envelope) => {
                    match envelope.payload {
                        crate::dataflow::channels::envelope::Payload::Data { time, data } => {
                            // Send to the external channel. If the receiver is dropped,
                            // we treat it as cancellation (output is no longer needed).
                            if self.sender.send(OutputEvent::data(time, data)).is_err() {
                                // Receiver dropped — stop producing
                                self.done = true;
                                return Ok(ActivationOutcome::Done);
                            }
                            made_progress = true;
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

    fn region_id(&self) -> RegionId {
        self.region_id
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
/// Created by [`SpawnedDataflow::input()`]. Send [`InputEvent`]s to feed
/// data into the running dataflow. `InputSender` is **cloneable** — all
/// clones share the same underlying channel. The channel closes only when
/// **all** clones (including the one held by `SpawnedDataflow`) are dropped,
/// or when [`close()`](Self::close) is called on this instance.
#[derive(Clone, Debug)]
pub struct InputSender<T: Timestamp, D: Send + 'static> {
    sender: mpsc::SyncSender<InputEvent<T, D>>,
}

impl<T: Timestamp, D: Send + 'static> InputSender<T, D> {
    pub(crate) fn new(sender: mpsc::SyncSender<InputEvent<T, D>>) -> Self {
        Self { sender }
    }

    /// Send a batch of data at the given timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataflow has already terminated.
    pub fn send(&self, time: T, data: Vec<D>) -> Result<()> {
        self.sender
            .send(InputEvent::data(time, data))
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))
    }

    /// Advance the input frontier past the given timestamp.
    ///
    /// After this call, no `send()` with `time <= frontier` should be made.
    pub fn advance_frontier(&self, time: T) -> Result<()> {
        self.sender
            .send(InputEvent::frontier(time))
            .map_err(|_| crate::error::Error::Custom("dataflow has terminated".into()))
    }

    /// Close this input, signaling no more data will arrive.
    ///
    /// Equivalent to dropping the sender.
    pub fn close(self) {
        // Drop self — the SyncSender will be dropped, disconnecting the channel.
    }
}

/// A handle for receiving output from a dataflow output port.
///
/// Created by [`DataflowHandle::output()`]. Receives [`OutputEvent`]s
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
            RegionId::new(0),
            rx,
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
            RegionId::new(0),
            rx,
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
            RegionId::new(0),
            rx,
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
            RegionId::new(0),
            Box::new(pull),
            tx,
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
