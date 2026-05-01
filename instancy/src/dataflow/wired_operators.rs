//! Wired operator wrappers that implement [`SchedulableOperator`].
//!
//! These wrappers own both the concrete operator (with its InputHandle/OutputHandle)
//! AND the channel endpoints (Push/Pull). When activated, they:
//! 1. Pull envelopes from input channels → feed the InputHandle.
//! 2. Call the operator's logic.
//! 3. Drain the OutputHandle → push envelopes to output channels.
//!
//! This keeps type-erasure at the outer boundary only.

use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// WiredUnaryOperator
// ---------------------------------------------------------------------------

/// A fully-wired unary operator ready for execution.
///
/// Owns:
/// - Input channel (puller) + InputHandle
/// - User logic closure
/// - OutputHandle + output channel (pusher)
/// - Retry buffer for backpressure
pub struct WiredUnaryOperator<T: Timestamp, D1: Send + 'static, D2: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    /// Input channel — pulls envelopes from upstream.
    input_puller: Box<dyn Pull<T, D1>>,
    /// Operator's typed input buffer.
    input_handle: InputHandle<T, D1>,
    /// User logic closure.
    logic: Box<dyn FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send>,
    /// Operator's typed output buffer.
    output_handle: OutputHandle<T, D2>,
    /// Output channel — pushes envelopes downstream.
    output_pusher: Box<dyn Push<T, D2>>,
    /// Pending output: batches that have been produced but not yet pushed to channel.
    /// Items are pushed front-to-back; on backpressure, remaining items stay here.
    pending_output: Vec<Envelope<T, D2>>,
    /// Whether the input channel is exhausted (sender closed + empty).
    input_exhausted: bool,
    /// Whether this operator has completed.
    done: bool,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> WiredUnaryOperator<T, D1, D2> {
    /// Create a new wired unary operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        logic: impl FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
        input_puller: Box<dyn Pull<T, D1>>,
        output_pusher: Box<dyn Push<T, D2>>,
    ) -> Self {
        let name = name.into();
        Self {
            input_handle: InputHandle::new(format!("{name}:input")),
            output_handle: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            region_id,
            input_puller,
            logic: Box::new(logic),
            output_pusher,
            pending_output: Vec::new(),
            input_exhausted: false,
            done: false,
        }
    }

    /// Try to flush pending output envelopes to the output channel.
    /// Returns true if all pending output was sent successfully.
    fn flush_pending_output(&mut self) -> Result<bool> {
        while !self.pending_output.is_empty() {
            let envelope = self.pending_output.remove(0);
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    // Re-insert at front for retry on next activation.
                    self.pending_output.insert(0, returned);
                    return Ok(false);
                }
                Err((e, _returned)) => return Err(e),
            }
        }
        Ok(true)
    }

    /// Pull from input channel and feed the InputHandle.
    fn pull_input(&mut self) {
        loop {
            match self.input_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        self.input_handle.push_vec(time, data);
                    }
                    Payload::Control(_signal) => {
                        // Control signals (watermarks, errors) are handled
                        // by the progress subsystem in PR 22.
                        // For now, just consume them.
                    }
                },
                None => {
                    // Check if input is exhausted.
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                        self.input_handle.mark_exhausted();
                    }
                    break;
                }
            }
        }
    }

    /// Drain output handle and push to output channel.
    /// Returns true if all output was sent, false if backpressure hit.
    fn push_output(&mut self) -> Result<bool> {
        // Move output handle contents into pending_output.
        for (time, data) in self.output_handle.drain() {
            self.pending_output.push(Envelope::data(time, data));
        }
        self.flush_pending_output()
    }
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> SchedulableOperator
    for WiredUnaryOperator<T, D1, D2>
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // Step 1: Try to flush any pending output first.
        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        // Step 2: Pull from input channel.
        self.pull_input();

        // Step 3: Check if there's work to do.
        let had_input = self.input_handle.has_pending();
        if !had_input && self.input_exhausted {
            // No input and no more coming — we're done.
            self.output_pusher.close();
            self.done = true;
            return Ok(ActivationOutcome::Done);
        }

        if !had_input {
            return Ok(ActivationOutcome::Idle);
        }

        // Step 4: Run user logic.
        (self.logic)(&mut self.input_handle, &mut self.output_handle)?;

        // Step 5: Push output to channel.
        if !self.push_output()? {
            return Ok(ActivationOutcome::BlockedOnBackpressure);
        }

        // Step 6: Check if we're done after processing.
        if self.input_exhausted && !self.input_handle.has_pending() && !self.output_handle.has_output() {
            self.output_pusher.close();
            self.done = true;
            return Ok(ActivationOutcome::Done);
        }

        Ok(ActivationOutcome::MadeProgress)
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
        self.input_handle.mark_exhausted();
    }
}

// ---------------------------------------------------------------------------
// WiredSourceOperator
// ---------------------------------------------------------------------------

/// A source operator that produces data from an iterator.
///
/// Has no input channels — generates data from a provided iterator and
/// pushes to its output channel.
pub struct WiredSourceOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    /// Data to emit, organized by timestamp.
    pending_data: Vec<(T, Vec<D>)>,
    /// Output channel.
    output_pusher: Box<dyn Push<T, D>>,
    /// Pending output envelopes (for backpressure retry).
    pending_output: Vec<Envelope<T, D>>,
    /// Whether all data has been emitted.
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> WiredSourceOperator<T, D> {
    /// Create a source operator that emits the given data batches.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        data: Vec<(T, Vec<D>)>,
        output_pusher: Box<dyn Push<T, D>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            pending_data: data,
            output_pusher,
            pending_output: Vec::new(),
            done: false,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for WiredSourceOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // Try to flush pending output first.
        while !self.pending_output.is_empty() {
            let envelope = self.pending_output.remove(0);
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.insert(0, returned);
                    return Ok(ActivationOutcome::BlockedOnBackpressure);
                }
                Err((e, _)) => return Err(e),
            }
        }

        // Emit pending data.
        while let Some((time, data)) = self.pending_data.pop() {
            let envelope = Envelope::data(time, data);
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push(returned);
                    return Ok(ActivationOutcome::BlockedOnBackpressure);
                }
                Err((e, _)) => return Err(e),
            }
        }

        // All data emitted.
        self.output_pusher.close();
        self.done = true;
        Ok(ActivationOutcome::Done)
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
        // Source has no inputs.
        self.done = true;
    }
}

// ---------------------------------------------------------------------------
// WiredSinkOperator
// ---------------------------------------------------------------------------

/// A sink operator that collects data from its input channel.
///
/// Has no output channels — accumulates received data for later retrieval.
pub struct WiredSinkOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    /// Input channel.
    input_puller: Box<dyn Pull<T, D>>,
    /// Collected output.
    collected: Vec<(T, Vec<D>)>,
    /// Whether input is exhausted.
    input_exhausted: bool,
    /// Whether this operator is done.
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> WiredSinkOperator<T, D> {
    /// Create a sink operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        input_puller: Box<dyn Pull<T, D>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            input_puller,
            collected: Vec::new(),
            input_exhausted: false,
            done: false,
        }
    }

    /// Get all collected data.
    pub fn collected(&self) -> &[(T, Vec<D>)] {
        &self.collected
    }

    /// Take all collected data, consuming it.
    pub fn take_collected(&mut self) -> Vec<(T, Vec<D>)> {
        std::mem::take(&mut self.collected)
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for WiredSinkOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        let mut made_progress = false;

        loop {
            match self.input_puller.pull() {
                Some(envelope) => {
                    if let Payload::Data { time, data } = envelope.payload {
                        self.collected.push((time, data));
                        made_progress = true;
                    }
                }
                None => {
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                        self.done = true;
                        return Ok(ActivationOutcome::Done);
                    }
                    break;
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
        self.input_exhausted = true;
        self.done = true;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::allocator::ChannelAllocator;

    fn make_channel<T: Timestamp, D: Send + 'static>() -> crate::dataflow::channels::pushpull::ChannelPair<T, D, ()> {
        let mut alloc = ChannelAllocator::new();
        alloc.allocate()
    }

    #[test]
    fn wired_source_emits_all_data() {
        let ch = make_channel::<u64, i32>();
        let pusher = ch.pusher;
        let mut puller = ch.puller;

        let mut source = WiredSourceOperator::new(
            "source",
            0,
            RegionId::new(0),
            vec![(0u64, vec![1, 2, 3]), (1u64, vec![4, 5])],
            pusher,
        );

        let outcome = source.activate().unwrap();
        assert_eq!(outcome, ActivationOutcome::Done);
        assert!(source.is_done());

        // Check data arrived.
        let mut received = Vec::new();
        while let Some(env) = puller.pull() {
            if let Payload::Data { time, data } = env.payload {
                received.push((time, data));
            }
        }
        // Note: source pops from back, so order is reversed.
        assert_eq!(received.len(), 2);
    }

    #[test]
    fn wired_sink_collects_data() {
        let ch = make_channel::<u64, i32>();
        let mut pusher = ch.pusher;
        let puller = ch.puller;

        let mut sink = WiredSinkOperator::new("sink", 0, RegionId::new(0), puller);

        // Push some data.
        pusher.push(Envelope::data(0, vec![10, 20])).unwrap();
        pusher.push(Envelope::data(1, vec![30])).unwrap();
        pusher.close();

        // Activate sink.
        let outcome = sink.activate().unwrap();
        assert_eq!(outcome, ActivationOutcome::Done);
        assert_eq!(sink.collected().len(), 2);
        assert_eq!(sink.collected()[0], (0, vec![10, 20]));
        assert_eq!(sink.collected()[1], (1, vec![30]));
    }

    #[test]
    fn wired_unary_processes_data() {
        let ch_in = make_channel::<u64, i32>();
        let ch_out = make_channel::<u64, i32>();
        let mut pusher_in = ch_in.pusher;
        let puller_in = ch_in.puller;
        let pusher_out = ch_out.pusher;
        let mut puller_out = ch_out.puller;

        let mut op = WiredUnaryOperator::new(
            "double",
            1,
            RegionId::new(0),
            |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            },
            puller_in,
            pusher_out,
        );

        // Push data to input channel.
        pusher_in.push(Envelope::data(0, vec![1, 2, 3])).unwrap();
        pusher_in.close();

        // Activate operator.
        let outcome = op.activate().unwrap();
        assert_eq!(outcome, ActivationOutcome::Done);

        // Check output.
        let env = puller_out.pull().unwrap();
        let (time, data) = env.as_data().unwrap();
        assert_eq!(*time, 0);
        assert_eq!(data, &vec![2, 4, 6]);
    }

    #[test]
    fn wired_unary_idle_when_no_input() {
        let ch_in = make_channel::<u64, i32>();
        let ch_out = make_channel::<u64, i32>();

        let mut op = WiredUnaryOperator::new(
            "noop",
            0,
            RegionId::new(0),
            |_input: &mut InputHandle<u64, i32>, _output: &mut OutputHandle<u64, i32>| Ok(()),
            ch_in.puller,
            ch_out.pusher,
        );

        let outcome = op.activate().unwrap();
        assert_eq!(outcome, ActivationOutcome::Idle);
    }

    #[test]
    fn source_to_unary_to_sink_pipeline() {
        // source → double → sink
        let ch1 = make_channel::<u64, i32>();
        let ch2 = make_channel::<u64, i32>();

        let mut source = WiredSourceOperator::new(
            "source",
            0,
            RegionId::new(0),
            vec![(0u64, vec![10, 20, 30])],
            ch1.pusher,
        );

        let mut transform = WiredUnaryOperator::new(
            "double",
            1,
            RegionId::new(0),
            |input: &mut InputHandle<u64, i32>, output: &mut OutputHandle<u64, i32>| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            },
            ch1.puller,
            ch2.pusher,
        );

        let mut sink = WiredSinkOperator::new("sink", 2, RegionId::new(0), ch2.puller);

        // Run pipeline manually.
        assert_eq!(source.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(transform.activate().unwrap(), ActivationOutcome::Done);
        assert_eq!(sink.activate().unwrap(), ActivationOutcome::Done);

        assert_eq!(sink.collected(), &[(0u64, vec![20, 40, 60])]);
    }
}
