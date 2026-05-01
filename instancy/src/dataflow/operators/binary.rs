//! Binary operator — two inputs, one output.
//!
//! The `binary` operator takes two input streams and produces a single
//! output stream. This is the building block for joins, zips, and other
//! two-input computations.

use std::fmt;

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::error::Result;
use crate::progress::timestamp::Timestamp;

/// A registered binary operator with two inputs and one output.
pub struct BinaryOperator<T: Timestamp, D1, D2, D3> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region.
    region_id: RegionId,
    /// Left input handle.
    input1: InputHandle<T, D1>,
    /// Right input handle.
    input2: InputHandle<T, D2>,
    /// Output handle.
    output: OutputHandle<T, D3>,
    /// The operator logic closure.
    logic: Box<
        dyn FnMut(
                &mut InputHandle<T, D1>,
                &mut InputHandle<T, D2>,
                &mut OutputHandle<T, D3>,
            ) -> Result<()>
            + Send,
    >,
}

impl<T: Timestamp, D1, D2, D3> BinaryOperator<T, D1, D2, D3> {
    /// Create a new binary operator.
    pub fn new<L>(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        logic: L,
    ) -> Self
    where
        L: FnMut(
                &mut InputHandle<T, D1>,
                &mut InputHandle<T, D2>,
                &mut OutputHandle<T, D3>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let name = name.into();
        Self {
            input1: InputHandle::new(format!("{name}:left")),
            input2: InputHandle::new(format!("{name}:right")),
            output: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            region_id,
            logic: Box::new(logic),
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

    /// Get a mutable reference to the left input handle.
    pub fn input1_mut(&mut self) -> &mut InputHandle<T, D1> {
        &mut self.input1
    }

    /// Get a mutable reference to the right input handle.
    pub fn input2_mut(&mut self) -> &mut InputHandle<T, D2> {
        &mut self.input2
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D3> {
        &mut self.output
    }

    /// Execute the operator logic once.
    ///
    /// Returns the number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();
        (self.logic)(&mut self.input1, &mut self.input2, &mut self.output)?;
        Ok(self.output.buffered_count() - before)
    }

    /// Drain all buffered output batches.
    pub fn drain_output(&mut self) -> impl Iterator<Item = (T, Vec<D3>)> + '_ {
        self.output.drain()
    }

    /// Whether both inputs are done.
    pub fn is_done(&self) -> bool {
        self.input1.is_done() && self.input2.is_done()
    }
}

impl<T: Timestamp, D1, D2, D3> fmt::Debug for BinaryOperator<T, D1, D2, D3> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .finish()
    }
}

/// Extension trait for constructing binary operators on a pair of `DataStream`s.
pub trait BinaryExt<S: Scope, D1> {
    /// Create a binary operator with two inputs and one output.
    ///
    /// The closure receives both input handles and the output handle.
    fn binary<D2, D3, L>(
        &self,
        other: &DataStream<S, D2>,
        name: &str,
        logic: L,
    ) -> DataStream<S, D3>
    where
        D2: 'static,
        D3: 'static,
        L: FnMut(
                &mut InputHandle<S::Timestamp, D1>,
                &mut InputHandle<S::Timestamp, D2>,
                &mut OutputHandle<S::Timestamp, D3>,
            ) -> Result<()>
            + Send
            + 'static;
}

impl<S: Scope, D1: 'static> BinaryExt<S, D1> for DataStream<S, D1> {
    fn binary<D2, D3, L>(
        &self,
        other: &DataStream<S, D2>,
        name: &str,
        logic: L,
    ) -> DataStream<S, D3>
    where
        D2: 'static,
        D3: 'static,
        L: FnMut(
                &mut InputHandle<S::Timestamp, D1>,
                &mut InputHandle<S::Timestamp, D2>,
                &mut OutputHandle<S::Timestamp, D3>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        debug_assert_eq!(
            self.region_id(), other.region_id(),
            "binary operator '{name}': both input streams must be in the same region"
        );

        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();
        let output_slot = Slot::new(op_index, 0);

        // Register operator and edges in the dataflow graph.
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, name, region_id, 2, 1,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            self.region_id(),
            region_id,
        ));
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *other.source(),
            Slot::new(op_index, 1),
            other.region_id(),
            region_id,
        ));

        let _operator = BinaryOperator::new(name, op_index, region_id, logic);

        DataStream::new(scope, output_slot, region_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn binary_operator_creation() {
        let op = BinaryOperator::<u64, i32, i32, i32>::new(
            "add_pairs",
            0,
            RegionId::new(0),
            |_input1, _input2, _output| Ok(()),
        );
        assert_eq!(op.name(), "add_pairs");
        assert_eq!(op.index(), 0);
    }

    #[test]
    fn binary_operator_join_by_timestamp() {
        let mut op = BinaryOperator::<u64, i32, i32, (i32, i32)>::new(
            "zip",
            0,
            RegionId::new(0),
            |input1, input2, output| {
                // Simple: match batches by timestamp
                let mut left: Vec<(u64, Vec<i32>)> = Vec::new();
                while let Some((t, d)) = input1.next() {
                    left.push((t, d));
                }
                while let Some((t2, d2)) = input2.next() {
                    // Find matching left batch
                    if let Some(pos) = left.iter().position(|(t, _)| *t == t2) {
                        let (t, d1) = left.remove(pos);
                        let mut session = output.session(t);
                        for (a, b) in d1.into_iter().zip(d2.into_iter()) {
                            session.give((a, b));
                        }
                    }
                }
                Ok(())
            },
        );

        op.input1_mut().push_vec(1, vec![10, 20]);
        op.input2_mut().push_vec(1, vec![100, 200]);

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (1, vec![(10, 100), (20, 200)]));
    }

    #[test]
    fn binary_operator_one_finishes_first() {
        let mut op = BinaryOperator::<u64, i32, i32, i32>::new(
            "sum_both",
            0,
            RegionId::new(0),
            |input1, input2, output| {
                while let Some((time, data)) = input1.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item);
                    }
                }
                while let Some((time, data)) = input2.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item);
                    }
                }
                Ok(())
            },
        );

        // Only left input has data
        op.input1_mut().push_vec(1, vec![10, 20]);
        op.input1_mut().mark_exhausted();
        op.activate().unwrap();

        let b1: Vec<_> = op.drain_output().collect();
        assert_eq!(b1[0], (1, vec![10, 20]));
        assert!(!op.is_done()); // right not done

        // Now right finishes
        op.input2_mut().push_vec(2, vec![30]);
        op.input2_mut().mark_exhausted();
        op.activate().unwrap();

        assert!(op.is_done());

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (2, vec![30]));
    }

    #[test]
    fn binary_operator_error_propagation() {
        use crate::error::Error;

        let mut op = BinaryOperator::<u64, i32, i32, i32>::new(
            "failing",
            0,
            RegionId::new(0),
            |input1, _input2, _output| {
                while let Some((_t, data)) = input1.next() {
                    if data.contains(&-1) {
                        return Err(Error::Custom("bad value".into()));
                    }
                }
                Ok(())
            },
        );

        op.input1_mut().push_vec(1, vec![-1]);
        let result = op.activate();
        assert!(result.is_err());
    }

    #[test]
    fn binary_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let s1_idx = scope.allocate_operator_index();
        let s2_idx = scope.allocate_operator_index();
        let stream1: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), Slot::new(s1_idx, 0), region_id);
        let stream2: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, Slot::new(s2_idx, 0), region_id);

        let output = stream1.binary(&stream2, "join", |i1, i2, out| {
            while let Some((t, d)) = i1.next() {
                let mut s = out.session(t);
                for item in d { s.give(item); }
            }
            while let Some((t, d)) = i2.next() {
                let mut s = out.session(t);
                for item in d { s.give(item); }
            }
            Ok(())
        });

        assert_eq!(output.region_id(), region_id);
        assert_eq!(output.source().operator_index, 2); // 0, 1 for sources, 2 for binary
    }

    #[test]
    fn binary_operator_empty_inputs() {
        let mut op = BinaryOperator::<u64, i32, i32, i32>::new(
            "noop",
            0,
            RegionId::new(0),
            |_i1, _i2, _out| Ok(()),
        );

        let count = op.activate().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn binary_operator_cross_product() {
        let mut op = BinaryOperator::<u64, i32, String, String>::new(
            "cross",
            0,
            RegionId::new(0),
            |input1, input2, output| {
                let mut lefts = Vec::new();
                while let Some((_t, data)) = input1.next() {
                    lefts.extend(data);
                }
                while let Some((t, data)) = input2.next() {
                    let mut session = output.session(t);
                    for s in &data {
                        for n in &lefts {
                            session.give(format!("{n}:{s}"));
                        }
                    }
                }
                Ok(())
            },
        );

        op.input1_mut().push_vec(1, vec![1, 2]);
        op.input2_mut().push_vec(1, vec!["a".to_string(), "b".to_string()]);
        op.activate().unwrap();

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(
            batches[0].1,
            vec!["1:a".to_string(), "2:a".to_string(), "1:b".to_string(), "2:b".to_string()]
        );
    }
}
