//! Wired (materialized) operator implementations.
//!
//! A "wired" operator is one that has completed the **materialization phase**:
//! its input/output channels are physically connected (Push/Pull endpoints attached),
//! making it ready for execution. This contrasts with the **build phase** where
//! operators exist only as metadata in the `DataflowGraph` (name, index, port counts,
//! edges) with no real channels attached.
//!
//! **Lifecycle:**
//! 1. Build phase → logical operator (metadata in DataflowGraph)
//! 2. Materialize phase → wired operator (channels connected, implements SchedulableOperator)
//!
//! When activated, a wired operator:
//! 1. Pulls envelopes from its input channel(s) → feeds the InputHandle.
//! 2. Calls the operator's user logic.
//! 3. Drains the OutputHandle → pushes envelopes to its output channel(s).
//!
//! Type-erasure happens at the outer `SchedulableOperator` boundary only —
//! internally, operators work with concrete Rust types (no dynamic dispatch on data).

use std::collections::VecDeque;

use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::operators::handles::{InputHandle, NotifyContext, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::schedulable::{ActivationOutcome, SchedulableOperator};
use crate::error::{Error, Result};
use crate::progress::capability::Capability;
use crate::progress::frontier::Antichain;
use crate::progress::notificator::Notificator;
use crate::progress::operate::ProgressReporter;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// WiredUnaryOperator
// ---------------------------------------------------------------------------

/// A fully-wired (materialized) unary operator ready for execution.
///
/// "Wired" means its input/output channels are physically connected —
/// this operator can pull data from upstream and push results downstream.
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
    pending_output: VecDeque<Envelope<T, D2>>,
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
            pending_output: VecDeque::new(),
            input_exhausted: false,
            done: false,
        }
    }

    /// Try to flush pending output envelopes to the output channel.
    /// Returns true if all pending output was sent successfully.
    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
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
            self.pending_output.push_back(Envelope::data(time, data));
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

/// A wired (materialized) source operator that produces data from an iterator.
///
/// Has no input channels — generates data from a provided collection and
/// pushes to its output channel.
pub struct WiredSourceOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    /// Data to emit, organized by timestamp (FIFO order).
    pending_data: VecDeque<(T, Vec<D>)>,
    /// Output channel.
    output_pusher: Box<dyn Push<T, D>>,
    /// Pending output envelopes (for backpressure retry).
    pending_output: VecDeque<Envelope<T, D>>,
    /// Optional progress reporter for output port 0.
    /// When present, the source holds a capability at T::minimum() initially
    /// and drops it when all data has been emitted.
    progress_reporter: Option<ProgressReporter<T>>,
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
            pending_data: VecDeque::from(data),
            output_pusher,
            pending_output: VecDeque::new(),
            progress_reporter: None,
            done: false,
        }
    }

    /// Create a source operator with progress reporting.
    ///
    /// The source uses the provided reporter to drop its capability at `T::minimum()`
    /// when all data has been emitted, allowing downstream frontiers to advance.
    /// The initial capability is seeded by `SubgraphBuilder::add_operator_with_capabilities`.
    pub fn with_progress(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        data: Vec<(T, Vec<D>)>,
        output_pusher: Box<dyn Push<T, D>>,
        reporter: ProgressReporter<T>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            pending_data: VecDeque::from(data),
            output_pusher,
            pending_output: VecDeque::new(),
            progress_reporter: Some(reporter),
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
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(ActivationOutcome::BlockedOnBackpressure);
                }
                Err((e, _)) => return Err(e),
            }
        }

        // Emit pending data in FIFO order (front-to-back).
        while let Some((time, data)) = self.pending_data.pop_front() {
            let envelope = Envelope::data(time, data);
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_back(returned);
                    return Ok(ActivationOutcome::BlockedOnBackpressure);
                }
                Err((e, _)) => return Err(e),
            }
        }

        // All data emitted — drop capability and close output.
        if let Some(ref reporter) = self.progress_reporter {
            reporter.update(T::minimum(), -1);
        }
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
        // Source has no inputs, but close_inputs can be called during shutdown.
        // Drop capability if present to avoid leaking it.
        if !self.done {
            if let Some(ref reporter) = self.progress_reporter {
                reporter.update(T::minimum(), -1);
            }
            self.output_pusher.close();
            self.done = true;
        }
    }
}

// ---------------------------------------------------------------------------
// WiredSinkOperator
// ---------------------------------------------------------------------------

/// A wired (materialized) sink operator that collects data from its input channel.
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
// WiredBinaryOperator
// ---------------------------------------------------------------------------

/// A fully-wired (materialized) binary operator with two typed inputs and one output.
///
/// The user logic receives two `InputHandle`s and one `OutputHandle`. Data may
/// arrive on either input independently; the logic must handle partial availability.
pub struct WiredBinaryOperator<
    T: Timestamp,
    D1: Send + 'static,
    D2: Send + 'static,
    D3: Send + 'static,
> {
    name: String,
    index: usize,
    region_id: RegionId,
    input1_puller: Box<dyn Pull<T, D1>>,
    input2_puller: Box<dyn Pull<T, D2>>,
    input1_handle: InputHandle<T, D1>,
    input2_handle: InputHandle<T, D2>,
    logic: Box<
        dyn FnMut(&mut InputHandle<T, D1>, &mut InputHandle<T, D2>, &mut OutputHandle<T, D3>) -> Result<()>
            + Send,
    >,
    output_handle: OutputHandle<T, D3>,
    output_pusher: Box<dyn Push<T, D3>>,
    pending_output: VecDeque<Envelope<T, D3>>,
    input1_exhausted: bool,
    input2_exhausted: bool,
    done: bool,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static, D3: Send + 'static>
    WiredBinaryOperator<T, D1, D2, D3>
{
    /// Create a new wired binary operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        logic: impl FnMut(&mut InputHandle<T, D1>, &mut InputHandle<T, D2>, &mut OutputHandle<T, D3>) -> Result<()>
            + Send
            + 'static,
        input1_puller: Box<dyn Pull<T, D1>>,
        input2_puller: Box<dyn Pull<T, D2>>,
        output_pusher: Box<dyn Push<T, D3>>,
    ) -> Self {
        let name = name.into();
        Self {
            input1_handle: InputHandle::new(format!("{name}:input1")),
            input2_handle: InputHandle::new(format!("{name}:input2")),
            output_handle: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            region_id,
            input1_puller,
            input2_puller,
            logic: Box::new(logic),
            output_pusher,
            pending_output: VecDeque::new(),
            input1_exhausted: false,
            input2_exhausted: false,
            done: false,
        }
    }

    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _returned)) => return Err(e),
            }
        }
        Ok(true)
    }

    fn pull_input1(&mut self) {
        loop {
            match self.input1_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        self.input1_handle.push_vec(time, data);
                    }
                    Payload::Control(_) => {}
                },
                None => {
                    if self.input1_puller.is_exhausted() {
                        self.input1_exhausted = true;
                        self.input1_handle.mark_exhausted();
                    }
                    break;
                }
            }
        }
    }

    fn pull_input2(&mut self) {
        loop {
            match self.input2_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        self.input2_handle.push_vec(time, data);
                    }
                    Payload::Control(_) => {}
                },
                None => {
                    if self.input2_puller.is_exhausted() {
                        self.input2_exhausted = true;
                        self.input2_handle.mark_exhausted();
                    }
                    break;
                }
            }
        }
    }

    fn push_output(&mut self) -> Result<bool> {
        for (time, data) in self.output_handle.drain() {
            self.pending_output.push_back(Envelope::data(time, data));
        }
        self.flush_pending_output()
    }
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static, D3: Send + 'static>
    SchedulableOperator for WiredBinaryOperator<T, D1, D2, D3>
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // Step 1: Flush pending output from previous activation.
        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        // Step 2: Pull from both input channels.
        self.pull_input1();
        self.pull_input2();

        // Step 3: Check if there's work to do.
        let has_input = self.input1_handle.has_pending() || self.input2_handle.has_pending();
        let both_exhausted = self.input1_exhausted && self.input2_exhausted;

        if !has_input && both_exhausted {
            self.output_pusher.close();
            self.done = true;
            return Ok(ActivationOutcome::Done);
        }

        if !has_input {
            return Ok(ActivationOutcome::Idle);
        }

        // Step 4: Run user logic.
        (self.logic)(
            &mut self.input1_handle,
            &mut self.input2_handle,
            &mut self.output_handle,
        )?;

        // Step 5: Push output.
        if !self.push_output()? {
            return Ok(ActivationOutcome::BlockedOnBackpressure);
        }

        // Step 6: Check if done.
        if both_exhausted
            && !self.input1_handle.has_pending()
            && !self.input2_handle.has_pending()
            && !self.output_handle.has_output()
        {
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
        self.input1_exhausted = true;
        self.input1_handle.mark_exhausted();
        self.input2_exhausted = true;
        self.input2_handle.mark_exhausted();
    }
}

// ---------------------------------------------------------------------------
// WiredConcatOperator
// ---------------------------------------------------------------------------

/// A fully-wired N-input, 1-output operator that merges data from multiple
/// same-typed streams into a single output stream. Data order within a
/// timestamp is preserved per-input but interleaved across inputs.
pub struct WiredConcatOperator<T: Timestamp, D: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,
    input_pullers: Vec<Box<dyn Pull<T, D>>>,
    inputs_exhausted: Vec<bool>,
    output_pusher: Box<dyn Push<T, D>>,
    pending_output: VecDeque<Envelope<T, D>>,
    done: bool,
}

impl<T: Timestamp, D: Send + 'static> WiredConcatOperator<T, D> {
    /// Create a concat operator that merges N input streams.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        input_pullers: Vec<Box<dyn Pull<T, D>>>,
        output_pusher: Box<dyn Push<T, D>>,
    ) -> Self {
        let num_inputs = input_pullers.len();
        Self {
            name: name.into(),
            index,
            region_id,
            input_pullers,
            inputs_exhausted: vec![false; num_inputs],
            output_pusher,
            pending_output: VecDeque::new(),
            done: false,
        }
    }

    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _returned)) => return Err(e),
            }
        }
        Ok(true)
    }
}

impl<T: Timestamp, D: Send + 'static> SchedulableOperator for WiredConcatOperator<T, D> {
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // Flush pending output.
        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        // Pull from all inputs and forward directly to output.
        let mut made_progress = false;
        for i in 0..self.input_pullers.len() {
            loop {
                match self.input_pullers[i].pull() {
                    Some(envelope) => {
                        if let Payload::Data { time, data } = envelope.payload {
                            self.pending_output.push_back(Envelope::data(time, data));
                            made_progress = true;
                        }
                    }
                    None => {
                        if self.input_pullers[i].is_exhausted() {
                            self.inputs_exhausted[i] = true;
                        }
                        break;
                    }
                }
            }
        }

        // Push to output.
        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        // Check if all inputs are exhausted.
        if self.inputs_exhausted.iter().all(|e| *e) {
            self.output_pusher.close();
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
        for exhausted in &mut self.inputs_exhausted {
            *exhausted = true;
        }
    }
}

// ---------------------------------------------------------------------------
// WiredEnterOperator
// ---------------------------------------------------------------------------

/// A wired operator that wraps timestamps for entering a nested loop scope.
///
/// Transforms `Envelope<TOuter, D>` → `Envelope<Product<TOuter, TInner>, D>`
/// by wrapping each timestamp `t` into `Product(t, TInner::minimum())`.
pub struct WiredEnterOperator<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    name: String,
    index: usize,
    region_id: RegionId,
    input_puller: Box<dyn Pull<TOuter, D>>,
    output_pusher: Box<dyn Push<crate::order::Product<TOuter, TInner>, D>>,
    pending_output: VecDeque<Envelope<crate::order::Product<TOuter, TInner>, D>>,
    input_exhausted: bool,
    done: bool,
    _phantom: std::marker::PhantomData<TInner>,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
    WiredEnterOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    /// Create a new enter operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        input_puller: Box<dyn Pull<TOuter, D>>,
        output_pusher: Box<dyn Push<crate::order::Product<TOuter, TInner>, D>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            input_puller,
            output_pusher,
            pending_output: VecDeque::new(),
            input_exhausted: false,
            done: false,
            _phantom: std::marker::PhantomData,
        }
    }

    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _)) => return Err(e),
            }
        }
        Ok(true)
    }
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> SchedulableOperator
    for WiredEnterOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        let mut made_progress = false;
        loop {
            match self.input_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        let product_time =
                            crate::order::Product::new(time, TInner::minimum());
                        self.pending_output
                            .push_back(Envelope::data(product_time, data));
                        made_progress = true;
                    }
                    Payload::Control(_) => {}
                },
                None => {
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                    }
                    break;
                }
            }
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        if self.input_exhausted && self.pending_output.is_empty() {
            self.output_pusher.close();
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
// WiredLeaveOperator
// ---------------------------------------------------------------------------

/// A wired operator that strips the inner timestamp when leaving a loop scope.
///
/// Transforms `Envelope<Product<TOuter, TInner>, D>` → `Envelope<TOuter, D>`
/// by extracting the outer component: `Product(outer, _inner)` → `outer`.
pub struct WiredLeaveOperator<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    name: String,
    index: usize,
    region_id: RegionId,
    input_puller: Box<dyn Pull<crate::order::Product<TOuter, TInner>, D>>,
    output_pusher: Box<dyn Push<TOuter, D>>,
    pending_output: VecDeque<Envelope<TOuter, D>>,
    input_exhausted: bool,
    done: bool,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
    WiredLeaveOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    /// Create a new leave operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        input_puller: Box<dyn Pull<crate::order::Product<TOuter, TInner>, D>>,
        output_pusher: Box<dyn Push<TOuter, D>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            input_puller,
            output_pusher,
            pending_output: VecDeque::new(),
            input_exhausted: false,
            done: false,
        }
    }

    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _)) => return Err(e),
            }
        }
        Ok(true)
    }
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> SchedulableOperator
    for WiredLeaveOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        let mut made_progress = false;
        loop {
            match self.input_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        let outer_time = time.outer;
                        self.pending_output
                            .push_back(Envelope::data(outer_time, data));
                        made_progress = true;
                    }
                    Payload::Control(_) => {}
                },
                None => {
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                    }
                    break;
                }
            }
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        if self.input_exhausted && self.pending_output.is_empty() {
            self.output_pusher.close();
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
// WiredFeedbackOperator
// ---------------------------------------------------------------------------

/// A wired operator that advances the inner timestamp for feedback edges.
///
/// Transforms `Envelope<Product<TOuter, TInner>, D>` by applying the summary
/// to the inner timestamp: `Product(outer, inner)` → `Product(outer, summary.results_in(inner))`.
/// If `results_in()` returns `None` (overflow), the data is dropped.
pub struct WiredFeedbackOperator<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    name: String,
    index: usize,
    region_id: RegionId,
    summary: TInner::Summary,
    input_puller: Box<dyn Pull<crate::order::Product<TOuter, TInner>, D>>,
    output_pusher: Box<dyn Push<crate::order::Product<TOuter, TInner>, D>>,
    pending_output: VecDeque<Envelope<crate::order::Product<TOuter, TInner>, D>>,
    input_exhausted: bool,
    done: bool,
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static>
    WiredFeedbackOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    /// Create a new feedback operator with the given timestamp advancement summary.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        summary: TInner::Summary,
        input_puller: Box<dyn Pull<crate::order::Product<TOuter, TInner>, D>>,
        output_pusher: Box<dyn Push<crate::order::Product<TOuter, TInner>, D>>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            region_id,
            summary,
            input_puller,
            output_pusher,
            pending_output: VecDeque::new(),
            input_exhausted: false,
            done: false,
        }
    }

    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
                    return Ok(false);
                }
                Err((e, _)) => return Err(e),
            }
        }
        Ok(true)
    }
}

impl<TOuter: Timestamp, TInner: Timestamp, D: Send + 'static> SchedulableOperator
    for WiredFeedbackOperator<TOuter, TInner, D>
where
    crate::order::Product<TOuter, TInner>: Timestamp,
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        use crate::progress::timestamp::PathSummary;

        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        let mut made_progress = false;
        loop {
            match self.input_puller.pull() {
                Some(envelope) => match envelope.payload {
                    Payload::Data { time, data } => {
                        // Advance inner timestamp by summary
                        if let Some(new_inner) = self.summary.results_in(&time.inner) {
                            let new_time =
                                crate::order::Product::new(time.outer, new_inner);
                            self.pending_output
                                .push_back(Envelope::data(new_time, data));
                            made_progress = true;
                        }
                        // If results_in returns None, drop the data (overflow)
                    }
                    Payload::Control(_) => {}
                },
                None => {
                    if self.input_puller.is_exhausted() {
                        self.input_exhausted = true;
                    }
                    break;
                }
            }
        }

        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        if self.input_exhausted && self.pending_output.is_empty() {
            self.output_pusher.close();
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
// WiredUnaryNotifyOperator
// ---------------------------------------------------------------------------

/// A fully-wired unary operator that supports frontier-based notifications.
///
/// # Why this exists (vs regular WiredUnaryOperator)
///
/// A regular `WiredUnaryOperator` processes data immediately: it pulls input,
/// runs the user closure, and pushes output — all in the same activation.
/// This works for stateless operators (map, filter) and simple stateful ones
/// that can emit partial results incrementally.
///
/// However, some operators need to **buffer data and defer emission** until a
/// timestamp is "complete" — meaning all possible input at that timestamp has
/// arrived. For example:
///
/// - **Aggregation after exchange**: When data for timestamp `t` arrives from
///   multiple workers in separate batches, the operator must wait until ALL
///   batches have arrived before emitting the final aggregate. The frontier
///   advancing past `t` is the signal that no more data at `t` will arrive.
///
/// - **Window operators**: Collect all data in a time window, then process.
///
/// - **Sort/distinct**: Need all data at a timestamp before producing output.
///
/// The `WiredUnaryNotifyOperator` extends the regular unary with:
///
/// 1. **A [`Notificator`] that fires when the input frontier advances**, signaling
///    that specific timestamps are complete.
///
/// 2. **Output capabilities** (via [`ProgressReporter`]) that prevent downstream
///    frontiers from advancing while data is buffered. Without these, the progress
///    tracker would propagate upstream completion through this operator, making
///    downstream operators incorrectly believe buffered timestamps are done.
///
/// 3. **Frontier-aware activation**: Unlike the regular unary operator which skips
///    activation when input is exhausted, this operator continues activating as long
///    as it has pending or ready notifications — ensuring buffered data is eventually
///    emitted even after all input is consumed.
///
/// # Activation flow
///
/// ```text
/// 1. Flush pending output (backpressure retry)
/// 2. Pull input from channel → feed InputHandle
/// 3. Determine if there's work:
///    - has_work = has_input OR notificator.has_ready()
///    - If !has_work AND exhausted AND no pending notifications → Done
///    - If !has_work → Idle
/// 4. Run user logic with (InputHandle, OutputHandle, NotifyContext)
/// 5. Push output to channel
/// 6. Check completion
/// ```
///
/// # Progress safety contract
///
/// When the user calls `ctx.notify_at(time)`, an output capability is created
/// at `time`. This capability's existence in the progress tracker prevents
/// downstream frontiers from advancing past `time`. When `ctx.next_notification()`
/// returns `time`, the capability is dropped, allowing progress to flow.
///
/// The user MUST call `next_notification()` for every `notify_at()` — otherwise
/// capabilities accumulate and downstream never makes progress. This is enforced
/// by the notification firing mechanism: a notification only fires when the
/// input frontier advances past the time, and `next_notification()` is the
/// only way to consume it.
pub struct WiredUnaryNotifyOperator<T: Timestamp, D1: Send + 'static, D2: Send + 'static> {
    name: String,
    index: usize,
    region_id: RegionId,

    /// Input channel — pulls envelopes from upstream.
    input_puller: Box<dyn Pull<T, D1>>,
    /// Operator's typed input buffer.
    input_handle: InputHandle<T, D1>,

    /// User logic closure — receives InputHandle, OutputHandle, AND NotifyContext.
    /// The NotifyContext lets the user register notifications and consume them.
    logic: Box<
        dyn FnMut(
                &mut InputHandle<T, D1>,
                &mut OutputHandle<T, D2>,
                &mut NotifyContext<'_, T>,
            ) -> Result<()>
            + Send,
    >,

    /// Operator's typed output buffer.
    output_handle: OutputHandle<T, D2>,
    /// Output channel — pushes envelopes downstream.
    output_pusher: Box<dyn Push<T, D2>>,

    /// Pending output envelopes (for backpressure retry).
    pending_output: VecDeque<Envelope<T, D2>>,

    /// Notificator that tracks when input frontier advances past requested timestamps.
    /// Updated by the executor via `update_input_frontier()` after progress propagation.
    /// The executor calls `propagate_progress()` → computes new operator input frontier →
    /// calls `operator.update_input_frontier(&frontier)` → notificator fires ready
    /// notifications → executor re-enqueues operator if `has_ready_notifications()`.
    notificator: Notificator<T>,

    /// Progress reporter for this operator's output port (port 0).
    /// Used by NotifyContext to create output capabilities.
    /// Each Capability::new(time, reporter) increments a pointstamp count at
    /// (operator_index, output_port=0, time) in the reachability tracker.
    /// Each Capability::drop decrements it. The reachability tracker uses these
    /// pointstamps to compute downstream frontiers — a pointstamp at time `t`
    /// prevents downstream operators from advancing past `t`.
    progress_reporter: ProgressReporter<T>,

    /// Output capabilities held while data is buffered.
    /// Created in NotifyContext::notify_at(), dropped in NotifyContext::next_notification().
    /// Each capability prevents downstream frontier advancement at its timestamp.
    /// If this vec is non-empty, downstream operators are "held back" at the
    /// minimum time among the capabilities.
    held_capabilities: Vec<Capability<T>>,

    /// Whether the input channel is exhausted (sender closed + empty).
    input_exhausted: bool,
    /// Whether this operator has completed all work.
    done: bool,
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static>
    WiredUnaryNotifyOperator<T, D1, D2>
{
    /// Create a new wired unary notify operator.
    ///
    /// # Arguments
    ///
    /// - `progress_reporter`: The [`ProgressReporter`] for this operator's output port.
    ///   Obtained from `OperatorProgress::reporter(0)` during materialization.
    ///   Used to create output capabilities that hold downstream frontiers.
    ///
    /// - `initial_frontier`: The initial input frontier at construction time.
    ///   Typically `Antichain::from_elem(T::minimum())` for operators with
    ///   connected input sources. The notificator uses this to determine which
    ///   `notify_at` requests should fire immediately (for times already past
    ///   the frontier).
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        logic: impl FnMut(
                &mut InputHandle<T, D1>,
                &mut OutputHandle<T, D2>,
                &mut NotifyContext<'_, T>,
            ) -> Result<()>
            + Send
            + 'static,
        input_puller: Box<dyn Pull<T, D1>>,
        output_pusher: Box<dyn Push<T, D2>>,
        progress_reporter: ProgressReporter<T>,
        initial_frontier: Antichain<T>,
    ) -> Self {
        let name = name.into();
        Self {
            input_handle: InputHandle::new(format!("{name}:input")),
            output_handle: OutputHandle::new(format!("{name}:output")),
            notificator: Notificator::new(initial_frontier),
            name,
            index,
            region_id,
            input_puller,
            logic: Box::new(logic),
            output_pusher,
            pending_output: VecDeque::new(),
            progress_reporter,
            held_capabilities: Vec::new(),
            input_exhausted: false,
            done: false,
        }
    }

    /// Try to flush pending output envelopes to the output channel.
    fn flush_pending_output(&mut self) -> Result<bool> {
        while let Some(envelope) = self.pending_output.pop_front() {
            match self.output_pusher.try_push(envelope) {
                Ok(()) => {}
                Err((Error::Backpressure, returned)) => {
                    self.pending_output.push_front(returned);
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
                        // Control signals handled by progress subsystem.
                    }
                },
                None => {
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
    fn push_output(&mut self) -> Result<bool> {
        for (time, data) in self.output_handle.drain() {
            self.pending_output.push_back(Envelope::data(time, data));
        }
        self.flush_pending_output()
    }

    /// Whether the notificator has any outstanding work (pending or ready).
    ///
    /// Used in the completion check: the operator cannot report Done while
    /// the notificator has pending notifications (the frontier may not have
    /// advanced far enough yet) or ready notifications (buffered data hasn't
    /// been emitted yet).
    fn notificator_has_work(&self) -> bool {
        self.notificator.has_ready()
            || self.notificator.pending_count() > 0
            || !self.held_capabilities.is_empty()
    }
}

impl<T: Timestamp, D1: Send + 'static, D2: Send + 'static> SchedulableOperator
    for WiredUnaryNotifyOperator<T, D1, D2>
{
    fn activate(&mut self) -> Result<ActivationOutcome> {
        if self.done {
            return Ok(ActivationOutcome::Done);
        }

        // Step 1: Flush pending output from previous activation (backpressure retry).
        if !self.pending_output.is_empty() {
            if !self.flush_pending_output()? {
                return Ok(ActivationOutcome::BlockedOnBackpressure);
            }
        }

        // Step 2: Pull from input channel → feed InputHandle.
        self.pull_input();

        // Step 3: Determine if the operator has work to do.
        //
        // Key difference from regular WiredUnaryOperator: we also check if
        // the notificator has ready notifications. This allows the operator
        // to run user logic EVEN WHEN there is no new input data — the user
        // closure processes ready notifications and emits buffered data.
        let had_input = self.input_handle.has_pending();
        let has_notifications = self.notificator.has_ready();

        if !had_input && !has_notifications {
            if self.input_exhausted && !self.notificator_has_work() {
                // Input exhausted, no pending/ready notifications, no held
                // capabilities — the operator is truly done.
                self.output_pusher.close();
                self.done = true;
                return Ok(ActivationOutcome::Done);
            }
            // No work right now, but we might get more input or notifications
            // in a future activation.
            return Ok(ActivationOutcome::Idle);
        }

        // Step 4: Build NotifyContext and run user logic.
        //
        // The NotifyContext wraps the notificator + progress reporter +
        // held capabilities, giving the user closure a safe interface to:
        // - Register notifications via notify_at(time) [creates output capability]
        // - Consume notifications via next_notification() [drops output capability]
        {
            let mut ctx = NotifyContext::new(
                &mut self.notificator,
                &self.progress_reporter,
                &mut self.held_capabilities,
            );
            (self.logic)(
                &mut self.input_handle,
                &mut self.output_handle,
                &mut ctx,
            )?;
        }

        // Step 5: Push output to channel.
        if !self.push_output()? {
            return Ok(ActivationOutcome::BlockedOnBackpressure);
        }

        // Step 6: Check completion.
        //
        // The operator is done only when ALL of these are true:
        // - Input is exhausted (no more data from upstream)
        // - No pending input batches in the InputHandle
        // - No buffered output in the OutputHandle
        // - No pending/ready notifications (all buffered data has been emitted)
        // - No held output capabilities (all capabilities have been dropped)
        //
        // This is stricter than the regular unary operator, which only checks
        // input exhaustion + empty handles. We must also wait for all
        // notifications to fire and be consumed.
        if self.input_exhausted
            && !self.input_handle.has_pending()
            && !self.output_handle.has_output()
            && !self.notificator_has_work()
        {
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

        // When input is closed, advance the notificator frontier to empty.
        // An empty frontier means "no more data at ANY timestamp can arrive."
        // This fires ALL pending notifications immediately, because every
        // pending timestamp is now past the (empty) frontier.
        //
        // This is critical for correctness: without this, pending notifications
        // would never fire after input exhaustion, and the operator would hang
        // with buffered data that is never emitted.
        //
        // The fired notifications will be consumed in the next activation,
        // where the user closure calls next_notification() and emits the
        // buffered data.
        self.notificator.update_frontier(&Antichain::new());
    }

    /// Update the operator's input frontier from the progress tracker.
    ///
    /// Called by the executor after progress propagation when this operator's
    /// input frontier changed. The frontier is passed as `&dyn Any` because
    /// the `SchedulableOperator` trait is type-erased — the concrete type
    /// is `Antichain<T>` which we downcast here.
    ///
    /// This update may cause the notificator to fire notifications for
    /// timestamps that are now past the frontier. The executor will check
    /// `has_ready_notifications()` after this call and re-enqueue the
    /// operator if there are ready notifications.
    fn update_input_frontier(&mut self, frontier: &dyn std::any::Any) {
        if let Some(frontier) = frontier.downcast_ref::<Antichain<T>>() {
            self.notificator.update_frontier(frontier);
        }
    }

    /// Whether this operator has ready notifications that need processing.
    ///
    /// The executor calls this after progress propagation to decide whether
    /// to re-enqueue the operator. Returns true if the notificator has
    /// fired notifications that haven't been consumed by `next_notification()`.
    fn has_ready_notifications(&self) -> bool {
        self.notificator.has_ready()
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
        // Source emits in FIFO order (insertion order).
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].0, 0u64); // first inserted batch emitted first
        assert_eq!(received[1].0, 1u64);
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
