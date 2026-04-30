//! Delay operator — time-based buffering.
//!
//! The `delay` operator reassigns timestamps on records and buffers them
//! until the frontier advances past their original timestamp. This enables
//! windowing, aggregation, and time-shifting patterns.

use std::collections::BTreeMap;
use std::fmt;

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::error::Result;
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// A registered delay operator.
///
/// Buffers data and releases it when the frontier advances past the
/// original timestamp. Each record's output timestamp is determined
/// by a user-provided delay function.
pub struct DelayOperator<T: Timestamp, D, F> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region.
    region_id: RegionId,
    /// Input handle.
    input: InputHandle<T, D>,
    /// Output handle.
    output: OutputHandle<T, D>,
    /// Buffered data keyed by output timestamp.
    buffer: BTreeMap<T, Vec<D>>,
    /// The delay function: maps (data, original_time) → output_time.
    delay_fn: F,
    /// Current frontier — used to decide when to release buffered data.
    frontier: Antichain<T>,
}

impl<T, D, F> DelayOperator<T, D, F>
where
    T: Timestamp + Ord,
    F: FnMut(&D, &T) -> T,
{
    /// Create a new per-record delay operator.
    ///
    /// The `delay_fn` receives each data item and its original timestamp,
    /// and returns the output timestamp. The output timestamp must be ≥
    /// the input timestamp (the operator does not enforce this but incorrect
    /// timestamps may cause progress tracking issues).
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        delay_fn: F,
    ) -> Self {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output: OutputHandle::new(format!("{name}:output")),
            buffer: BTreeMap::new(),
            name,
            index,
            region_id,
            delay_fn,
            frontier: Antichain::from_elem(T::minimum()),
        }
    }

    /// Get the operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the region ID.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D> {
        &mut self.output
    }

    /// Update the frontier used for release decisions.
    pub fn update_frontier(&mut self, frontier: Antichain<T>) {
        self.frontier = frontier;
    }

    /// Number of buffered timestamps.
    pub fn buffered_timestamps(&self) -> usize {
        self.buffer.len()
    }

    /// Execute the operator logic once.
    ///
    /// 1. Reads all pending input, applies `delay_fn`, buffers by output timestamp.
    /// 2. Releases buffered data for timestamps that the frontier has advanced past.
    ///
    /// Returns the number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();

        // Step 1: Read input and buffer with delayed timestamps.
        while let Some((time, data)) = self.input.next() {
            for item in data {
                let output_time = (self.delay_fn)(&item, &time);
                self.buffer.entry(output_time).or_default().push(item);
            }
        }

        // Step 2: Release data whose output timestamp is no longer
        // in the frontier (frontier has advanced past it).
        // Use filter (not take_while) to handle partial orders correctly —
        // BTreeMap's Ord ordering may not match the partial order.
        let releasable: Vec<T> = self.buffer.keys()
            .filter(|t| !self.frontier.less_equal(t))
            .cloned()
            .collect();

        for time in releasable {
            if let Some(data) = self.buffer.remove(&time) {
                let mut session = self.output.session(time);
                session.give_iterator(data);
            }
        }

        // If input is exhausted and frontier is empty, flush everything.
        if self.input.is_exhausted() && self.frontier.elements().is_empty() {
            for (time, data) in std::mem::take(&mut self.buffer) {
                let mut session = self.output.session(time);
                session.give_iterator(data);
            }
        }

        Ok(self.output.buffered_count() - before)
    }

    /// Drain all buffered output batches.
    pub fn drain_output(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.output.drain()
    }

    /// Whether the input is done and all buffers are flushed.
    pub fn is_done(&self) -> bool {
        self.input.is_done() && self.buffer.is_empty()
    }
}

impl<T: Timestamp, D, F> fmt::Debug for DelayOperator<T, D, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .field("buffered_timestamps", &self.buffer.len())
            .finish()
    }
}

/// A batch-level delay operator.
///
/// Like `DelayOperator` but the delay function operates on the timestamp
/// only (not individual records). All data at a given input timestamp
/// is delayed to the same output timestamp.
pub struct DelayBatchOperator<T: Timestamp, D, F> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region.
    region_id: RegionId,
    /// Input handle.
    input: InputHandle<T, D>,
    /// Output handle.
    output: OutputHandle<T, D>,
    /// Buffered data keyed by output timestamp.
    buffer: BTreeMap<T, Vec<D>>,
    /// The delay function: maps input_timestamp → output_timestamp.
    delay_fn: F,
    /// Current frontier.
    frontier: Antichain<T>,
}

impl<T, D, F> DelayBatchOperator<T, D, F>
where
    T: Timestamp + Ord,
    F: FnMut(&T) -> T,
{
    /// Create a new batch-level delay operator.
    ///
    /// The `delay_fn` maps each input timestamp to an output timestamp.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        delay_fn: F,
    ) -> Self {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output: OutputHandle::new(format!("{name}:output")),
            buffer: BTreeMap::new(),
            name,
            index,
            region_id,
            delay_fn,
            frontier: Antichain::from_elem(T::minimum()),
        }
    }

    /// Get the operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the region ID.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D> {
        &mut self.output
    }

    /// Update the frontier.
    pub fn update_frontier(&mut self, frontier: Antichain<T>) {
        self.frontier = frontier;
    }

    /// Number of buffered timestamps.
    pub fn buffered_timestamps(&self) -> usize {
        self.buffer.len()
    }

    /// Execute the operator logic once.
    ///
    /// Returns the number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();

        // Read input and buffer with delayed timestamps.
        while let Some((time, data)) = self.input.next() {
            let output_time = (self.delay_fn)(&time);
            self.buffer.entry(output_time).or_default().extend(data);
        }

        // Release data whose output timestamp is past the frontier.
        // Use filter (not take_while) to handle partial orders correctly.
        let releasable: Vec<T> = self.buffer.keys()
            .filter(|t| !self.frontier.less_equal(t))
            .cloned()
            .collect();

        for time in releasable {
            if let Some(data) = self.buffer.remove(&time) {
                let mut session = self.output.session(time);
                session.give_iterator(data);
            }
        }

        // Flush all on exhaustion with empty frontier.
        if self.input.is_exhausted() && self.frontier.elements().is_empty() {
            for (time, data) in std::mem::take(&mut self.buffer) {
                let mut session = self.output.session(time);
                session.give_iterator(data);
            }
        }

        Ok(self.output.buffered_count() - before)
    }

    /// Drain all buffered output batches.
    pub fn drain_output(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.output.drain()
    }

    /// Whether the input is done and all buffers are flushed.
    pub fn is_done(&self) -> bool {
        self.input.is_done() && self.buffer.is_empty()
    }
}

impl<T: Timestamp, D, F> fmt::Debug for DelayBatchOperator<T, D, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayBatchOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .field("buffered_timestamps", &self.buffer.len())
            .finish()
    }
}

/// Extension trait for delay operators on `DataStream`.
pub trait DelayExt<S: Scope, D> {
    /// Delay each record by applying `delay_fn` to determine its output timestamp.
    fn delay<F>(&self, name: &str, delay_fn: F) -> DataStream<S, D>
    where
        F: FnMut(&D, &S::Timestamp) -> S::Timestamp + Send + 'static;

    /// Delay all records at a timestamp by applying `delay_fn` to the timestamp.
    fn delay_batch<F>(&self, name: &str, delay_fn: F) -> DataStream<S, D>
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Send + 'static;
}

impl<S: Scope, D: 'static> DelayExt<S, D> for DataStream<S, D>
where
    S::Timestamp: Ord,
{
    fn delay<F>(&self, name: &str, _delay_fn: F) -> DataStream<S, D>
    where
        F: FnMut(&D, &S::Timestamp) -> S::Timestamp + Send + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();
        let output_slot = Slot::new(op_index, 0);

        // TODO: Register operator in scope/graph registry (PR9).
        let _operator: DelayOperator<S::Timestamp, D, F> =
            DelayOperator::new(name, op_index, region_id, _delay_fn);

        DataStream::new(scope, output_slot, region_id)
    }

    fn delay_batch<F>(&self, name: &str, _delay_fn: F) -> DataStream<S, D>
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Send + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();
        let output_slot = Slot::new(op_index, 0);

        // TODO: Register operator in scope/graph registry (PR9).
        let _operator: DelayBatchOperator<S::Timestamp, D, F> =
            DelayBatchOperator::new(name, op_index, region_id, _delay_fn);

        DataStream::new(scope, output_slot, region_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn delay_operator_creation() {
        let op = DelayOperator::<u64, i32, _>::new(
            "delay_by_10",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| time + 10,
        );
        assert_eq!(op.name(), "delay_by_10");
        assert_eq!(op.buffered_timestamps(), 0);
    }

    #[test]
    fn delay_buffers_until_frontier_advances() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "delay_by_5",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| time + 5,
        );

        // Input at time 1 → delayed to time 6
        op.input_mut().push_vec(1, vec![10, 20]);
        op.activate().unwrap();

        // Frontier is at {0}, so time 6 is not past frontier → no output
        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 0);
        assert_eq!(op.buffered_timestamps(), 1); // time 6 buffered

        // Advance frontier to {7} → time 6 is now past
        op.update_frontier(Antichain::from_elem(7));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], (6, vec![10, 20]));
        assert_eq!(op.buffered_timestamps(), 0);
    }

    #[test]
    fn delay_correct_timestamps() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "double_time",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| time * 2,
        );

        op.input_mut().push_vec(3, vec![1]);
        op.input_mut().push_vec(5, vec![2]);
        op.activate().unwrap();

        // Both buffered: times 6 and 10
        assert_eq!(op.buffered_timestamps(), 2);

        // Advance past 6 but not 10
        op.update_frontier(Antichain::from_elem(7));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], (6, vec![1]));
        assert_eq!(op.buffered_timestamps(), 1); // time 10 still buffered

        // Advance past 10
        op.update_frontier(Antichain::from_elem(11));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], (10, vec![2]));
    }

    #[test]
    fn delay_per_record_timestamp() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "item_delay",
            0,
            RegionId::new(0),
            |item: &i32, time: &u64| time + (*item as u64),
        );

        // Items 1,2,3 at time 10 → delayed to 11, 12, 13
        op.input_mut().push_vec(10, vec![1, 2, 3]);
        op.activate().unwrap();
        assert_eq!(op.buffered_timestamps(), 3);

        // Advance past 12 but not 13
        op.update_frontier(Antichain::from_elem(13));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 2);
        assert_eq!(b[0], (11, vec![1]));
        assert_eq!(b[1], (12, vec![2]));
        assert_eq!(op.buffered_timestamps(), 1); // time 13 still buffered
    }

    #[test]
    fn delay_flush_on_exhaustion() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "delay_10",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| time + 10,
        );

        op.input_mut().push_vec(1, vec![42]);
        op.input_mut().mark_exhausted();

        // Signal empty frontier (done)
        op.update_frontier(Antichain::new());
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0], (11, vec![42]));
        assert!(op.is_done());
    }

    #[test]
    fn delay_batch_operator() {
        let mut op = DelayBatchOperator::<u64, i32, _>::new(
            "batch_delay",
            0,
            RegionId::new(0),
            |time: &u64| time + 5,
        );

        op.input_mut().push_vec(1, vec![10, 20, 30]);
        op.input_mut().push_vec(2, vec![40]);
        op.activate().unwrap();

        // All data from time 1 → time 6, from time 2 → time 7
        assert_eq!(op.buffered_timestamps(), 2);

        // Advance past both
        op.update_frontier(Antichain::from_elem(8));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b.len(), 2);
        assert_eq!(b[0], (6, vec![10, 20, 30]));
        assert_eq!(b[1], (7, vec![40]));
    }

    #[test]
    fn delay_batch_all_same_timestamp() {
        let mut op = DelayBatchOperator::<u64, i32, _>::new(
            "batch_delay",
            0,
            RegionId::new(0),
            |time: &u64| time + 1,
        );

        // Multiple batches at same time → accumulated at same output time
        op.input_mut().push_vec(5, vec![1, 2]);
        op.input_mut().push_vec(5, vec![3]);
        op.activate().unwrap();

        assert_eq!(op.buffered_timestamps(), 1); // all at time 6

        op.update_frontier(Antichain::from_elem(7));
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b[0], (6, vec![1, 2, 3]));
    }

    #[test]
    fn delay_empty_input() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "noop",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| *time,
        );

        let count = op.activate().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn delay_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: DataStream<RootScope<u64>, i32> = DataStream::new(scope, source, region_id);

        let output = stream.delay("delay_5", |_item, time| time + 5);
        assert_eq!(output.source().operator_index, 1);
    }

    #[test]
    fn delay_batch_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: DataStream<RootScope<u64>, i32> = DataStream::new(scope, source, region_id);

        let output = stream.delay_batch("delay_batch", |time| time + 10);
        assert_eq!(output.source().operator_index, 1);
    }

    #[test]
    fn delay_batch_flush_on_exhaustion() {
        let mut op = DelayBatchOperator::<u64, i32, _>::new(
            "flush_test",
            0,
            RegionId::new(0),
            |time: &u64| time + 100,
        );

        op.input_mut().push_vec(1, vec![1, 2, 3]);
        op.input_mut().mark_exhausted();
        op.update_frontier(Antichain::new());
        op.activate().unwrap();

        let b: Vec<_> = op.drain_output().collect();
        assert_eq!(b[0], (101, vec![1, 2, 3]));
        assert!(op.is_done());
    }

    #[test]
    fn delay_capabilities_held_for_buffered() {
        let mut op = DelayOperator::<u64, i32, _>::new(
            "cap_test",
            0,
            RegionId::new(0),
            |_item: &i32, time: &u64| time + 10,
        );

        op.input_mut().push_vec(1, vec![10]);
        op.input_mut().push_vec(2, vec![20]);
        op.activate().unwrap();

        // Two timestamps buffered: 11 and 12
        assert_eq!(op.buffered_timestamps(), 2);
        assert!(!op.is_done());

        // Partially release
        op.update_frontier(Antichain::from_elem(12));
        op.activate().unwrap();
        let _ = op.drain_output().count();
        assert_eq!(op.buffered_timestamps(), 1); // time 12 still held

        // Release the rest
        op.update_frontier(Antichain::from_elem(13));
        op.activate().unwrap();
        let _ = op.drain_output().count();
        assert_eq!(op.buffered_timestamps(), 0);
    }
}
