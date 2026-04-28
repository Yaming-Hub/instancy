//! Concat operator — merge multiple streams into one.
//!
//! `concat` merges data from multiple input streams into a single output
//! stream, preserving timestamps. This is the fundamental fan-in operator.

use std::fmt;

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::error::Result;
use crate::progress::timestamp::Timestamp;

/// A registered concat operator that merges N inputs into one output.
pub struct ConcatOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region.
    region_id: RegionId,
    /// Input handles — one per source stream.
    inputs: Vec<InputHandle<T, D>>,
    /// Single output handle.
    output: OutputHandle<T, D>,
}

impl<T: Timestamp, D> ConcatOperator<T, D> {
    /// Create a new concat operator with `n` inputs.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        num_inputs: usize,
    ) -> Self {
        let name = name.into();
        let inputs = (0..num_inputs)
            .map(|i| InputHandle::new(format!("{name}:input_{i}")))
            .collect();
        Self {
            output: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            region_id,
            inputs,
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

    /// Get a mutable reference to an input handle by index.
    pub fn input_mut(&mut self, idx: usize) -> &mut InputHandle<T, D> {
        &mut self.inputs[idx]
    }

    /// Number of inputs.
    pub fn num_inputs(&self) -> usize {
        self.inputs.len()
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D> {
        &mut self.output
    }

    /// Execute the operator logic once.
    ///
    /// Forwards all pending data from every input to the output.
    /// Returns the number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();
        for input in &mut self.inputs {
            while let Some((time, data)) = input.next() {
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

    /// Whether all inputs are done.
    pub fn is_done(&self) -> bool {
        self.inputs.iter().all(|i| i.is_done())
    }
}

impl<T: Timestamp, D> fmt::Debug for ConcatOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcatOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .field("num_inputs", &self.inputs.len())
            .finish()
    }
}

/// Extension trait for concatenating `DataStream`s.
pub trait ConcatExt<S: Scope, D> {
    /// Concatenate this stream with another, producing a merged output stream.
    fn concat(&self, other: &DataStream<S, D>) -> DataStream<S, D>;
}

impl<S: Scope, D: 'static> ConcatExt<S, D> for DataStream<S, D> {
    fn concat(&self, other: &DataStream<S, D>) -> DataStream<S, D> {
        debug_assert_eq!(
            self.region_id(), other.region_id(),
            "concat: both input streams must be in the same region"
        );

        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();
        let output_slot = Slot::new(op_index, 0);

        // TODO: Register operator in scope/graph registry (PR9).
        let _operator = ConcatOperator::<S::Timestamp, D>::new(
            "concat",
            op_index,
            region_id,
            2,
        );

        DataStream::new(scope, output_slot, region_id)
    }
}

/// Concatenate a vector of streams into one.
///
/// All streams must belong to the same scope and region.
pub fn concatenate<S: Scope, D: 'static>(streams: &[DataStream<S, D>]) -> DataStream<S, D> {
    assert!(!streams.is_empty(), "concatenate requires at least one stream");

    let region_id = streams[0].region_id();
    for (i, s) in streams.iter().enumerate().skip(1) {
        debug_assert_eq!(
            s.region_id(), region_id,
            "concatenate: stream {i} is in a different region than stream 0"
        );
    }

    let mut scope = streams[0].scope().clone();
    let op_index = scope.allocate_operator_index();
    let output_slot = Slot::new(op_index, 0);

    // TODO: Register operator in scope/graph registry (PR9).
    let _operator = ConcatOperator::<S::Timestamp, D>::new(
        "concatenate",
        op_index,
        region_id,
        streams.len(),
    );

    DataStream::new(scope, output_slot, region_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn concat_operator_creation() {
        let op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 3);
        assert_eq!(op.name(), "merge");
        assert_eq!(op.num_inputs(), 3);
    }

    #[test]
    fn concat_two_streams() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 2);

        op.input_mut(0).push_vec(1, vec![10, 20]);
        op.input_mut(1).push_vec(1, vec![30, 40]);
        op.input_mut(0).push_vec(2, vec![50]);

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();

        // All data forwarded, timestamps preserved
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], (1, vec![10, 20]));
        assert_eq!(batches[1], (2, vec![50]));
        assert_eq!(batches[2], (1, vec![30, 40]));
    }

    #[test]
    fn concat_preserves_timestamps() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 2);

        op.input_mut(0).push_vec(5, vec![1]);
        op.input_mut(1).push_vec(10, vec![2]);

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();

        assert_eq!(batches[0].0, 5);
        assert_eq!(batches[1].0, 10);
    }

    #[test]
    fn concat_empty_streams() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 3);

        // No data on any input
        let count = op.activate().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn concat_one_empty_one_full() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 2);

        // Only input 1 has data
        op.input_mut(1).push_vec(1, vec![99]);

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], (1, vec![99]));
    }

    #[test]
    fn concat_n_streams() {
        let mut op = ConcatOperator::<u64, i32>::new("merge_5", 0, RegionId::new(0), 5);

        for i in 0..5 {
            op.input_mut(i).push_vec(1, vec![(i * 10) as i32]);
        }

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches.len(), 5);

        let all_values: Vec<i32> = batches.into_iter().flat_map(|(_, d)| d).collect();
        assert_eq!(all_values, vec![0, 10, 20, 30, 40]);
    }

    #[test]
    fn concat_is_done() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 2);

        assert!(!op.is_done());
        op.input_mut(0).mark_exhausted();
        assert!(!op.is_done());
        op.input_mut(1).mark_exhausted();
        assert!(op.is_done());
    }

    #[test]
    fn concat_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let s1 = scope.allocate_operator_index();
        let s2 = scope.allocate_operator_index();
        let stream1: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), Slot::new(s1, 0), region_id);
        let stream2: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, Slot::new(s2, 0), region_id);

        let output = stream1.concat(&stream2);
        assert_eq!(output.source().operator_index, 2);
    }

    #[test]
    fn concatenate_function_works() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let streams: Vec<DataStream<RootScope<u64>, i32>> = (0..3)
            .map(|_| {
                let idx = scope.allocate_operator_index();
                DataStream::new(scope.clone(), Slot::new(idx, 0), region_id)
            })
            .collect();

        let output = concatenate(&streams);
        assert_eq!(output.source().operator_index, 3); // 0,1,2 for sources, 3 for concat
    }

    #[test]
    #[should_panic(expected = "at least one stream")]
    fn concatenate_panics_on_empty() {
        let streams: Vec<DataStream<RootScope<u64>, i32>> = vec![];
        let _output = concatenate(&streams);
    }

    #[test]
    fn concat_multiple_activations() {
        let mut op = ConcatOperator::<u64, i32>::new("merge", 0, RegionId::new(0), 2);

        // First activation
        op.input_mut(0).push_vec(1, vec![10]);
        op.activate().unwrap();
        let b1: Vec<_> = op.drain_output().collect();
        assert_eq!(b1.len(), 1);

        // Second activation
        op.input_mut(1).push_vec(2, vec![20]);
        op.activate().unwrap();
        let b2: Vec<_> = op.drain_output().collect();
        assert_eq!(b2.len(), 1);
        assert_eq!(b2[0], (2, vec![20]));
    }
}
