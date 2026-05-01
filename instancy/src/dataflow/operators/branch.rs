//! Branch and ok_err operators — conditional stream splitting.
//!
//! The `branch` operator splits a stream into two based on a predicate.
//! The `ok_err` operator splits a stream based on a function returning `Result`.

use std::fmt;

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::error::Result;
use crate::progress::timestamp::Timestamp;

/// A registered branch operator that splits a stream into two based on a predicate.
///
/// Records matching the predicate go to the "true" output (slot 0),
/// records not matching go to the "false" output (slot 1).
pub struct BranchOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region this operator belongs to.
    region_id: RegionId,
    /// The operator's input handle.
    input: InputHandle<T, D>,
    /// Output for records matching the predicate (true branch).
    output_true: OutputHandle<T, D>,
    /// Output for records not matching the predicate (false branch).
    output_false: OutputHandle<T, D>,
    /// The predicate closure.
    predicate: Box<dyn FnMut(&D) -> bool + Send>,
}

impl<T: Timestamp, D> BranchOperator<T, D> {
    /// Create a new branch operator.
    pub fn new<P>(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        predicate: P,
    ) -> Self
    where
        P: FnMut(&D) -> bool + Send + 'static,
    {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output_true: OutputHandle::new(format!("{name}:true")),
            output_false: OutputHandle::new(format!("{name}:false")),
            name,
            index,
            region_id,
            predicate: Box::new(predicate),
        }
    }

    /// The operator's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The execution region.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Activate the operator: split input into true/false outputs.
    ///
    /// Returns the number of batches processed.
    pub fn activate(&mut self) -> Result<usize> {
        let mut batches = 0;
        while let Some((time, data)) = self.input.next() {
            let mut true_session = self.output_true.session(time.clone());
            let mut false_session = self.output_false.session(time);
            for item in data {
                if (self.predicate)(&item) {
                    true_session.give(item);
                } else {
                    false_session.give(item);
                }
            }
            batches += 1;
        }
        Ok(batches)
    }

    /// Drain the true output (slot 0).
    pub fn drain_output_true(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.output_true.drain()
    }

    /// Drain the false output (slot 1).
    pub fn drain_output_false(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.output_false.drain()
    }

    /// Check if the operator is done (input exhausted and all pending data drained).
    pub fn is_done(&self) -> bool {
        self.input.is_done()
    }
}

impl<T: Timestamp, D> fmt::Debug for BranchOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BranchOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .finish()
    }
}

/// A registered ok_err operator that splits a stream based on Result.
///
/// The function maps each record to `Result<O, E>`. Ok values go to the
/// "ok" output (slot 0), Err values go to the "err" output (slot 1).
pub struct OkErrOperator<T: Timestamp, D, O, E> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution region this operator belongs to.
    region_id: RegionId,
    /// The operator's input handle.
    input: InputHandle<T, D>,
    /// Output for Ok values.
    output_ok: OutputHandle<T, O>,
    /// Output for Err values.
    output_err: OutputHandle<T, E>,
    /// The splitting function.
    splitter: Box<dyn FnMut(D) -> std::result::Result<O, E> + Send>,
}

impl<T: Timestamp, D, O, E> OkErrOperator<T, D, O, E> {
    /// Create a new ok_err operator.
    pub fn new<F>(
        name: impl Into<String>,
        index: usize,
        region_id: RegionId,
        splitter: F,
    ) -> Self
    where
        F: FnMut(D) -> std::result::Result<O, E> + Send + 'static,
    {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output_ok: OutputHandle::new(format!("{name}:ok")),
            output_err: OutputHandle::new(format!("{name}:err")),
            name,
            index,
            region_id,
            splitter: Box::new(splitter),
        }
    }

    /// The operator's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The execution region.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Activate the operator: split input into ok/err outputs.
    ///
    /// Returns the number of batches processed.
    pub fn activate(&mut self) -> Result<usize> {
        let mut batches = 0;
        while let Some((time, data)) = self.input.next() {
            let mut ok_session = self.output_ok.session(time.clone());
            let mut err_session = self.output_err.session(time);
            for item in data {
                match (self.splitter)(item) {
                    Ok(val) => ok_session.give(val),
                    Err(val) => err_session.give(val),
                }
            }
            batches += 1;
        }
        Ok(batches)
    }

    /// Drain the ok output (slot 0).
    pub fn drain_output_ok(&mut self) -> impl Iterator<Item = (T, Vec<O>)> + '_ {
        self.output_ok.drain()
    }

    /// Drain the err output (slot 1).
    pub fn drain_output_err(&mut self) -> impl Iterator<Item = (T, Vec<E>)> + '_ {
        self.output_err.drain()
    }

    /// Check if the operator is done (input exhausted and all pending data drained).
    pub fn is_done(&self) -> bool {
        self.input.is_done()
    }
}

impl<T: Timestamp, D, O, E> fmt::Debug for OkErrOperator<T, D, O, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkErrOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("region_id", &self.region_id)
            .finish()
    }
}

/// Extension trait for constructing branch operators on a `DataStream`.
pub trait BranchExt<S: Scope, D> {
    /// Split the stream into two based on a predicate.
    ///
    /// Returns `(true_stream, false_stream)` where:
    /// - `true_stream` contains records for which the predicate returns `true`
    /// - `false_stream` contains records for which the predicate returns `false`
    ///
    /// # Example
    /// ```ignore
    /// let (evens, odds) = stream.branch(|x| x % 2 == 0);
    /// ```
    fn branch<P>(
        &self,
        predicate: P,
    ) -> (DataStream<S, D>, DataStream<S, D>)
    where
        P: FnMut(&D) -> bool + Send + 'static;
}

/// Extension trait for constructing ok_err operators on a `DataStream`.
pub trait OkErrExt<S: Scope, D> {
    /// Split the stream based on a function returning `Result`.
    ///
    /// Returns `(ok_stream, err_stream)` where:
    /// - `ok_stream` contains the `Ok` values
    /// - `err_stream` contains the `Err` values
    ///
    /// # Example
    /// ```ignore
    /// let (parsed, failures) = stream.ok_err(|s| s.parse::<i32>());
    /// ```
    fn ok_err<O, E, F>(
        &self,
        splitter: F,
    ) -> (DataStream<S, O>, DataStream<S, E>)
    where
        O: 'static,
        E: 'static,
        F: FnMut(D) -> std::result::Result<O, E> + Send + 'static;
}

impl<S: Scope, D: 'static> BranchExt<S, D> for DataStream<S, D> {
    fn branch<P>(
        &self,
        predicate: P,
    ) -> (DataStream<S, D>, DataStream<S, D>)
    where
        P: FnMut(&D) -> bool + Send + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();

        let true_slot = Slot::new(op_index, 0);
        let false_slot = Slot::new(op_index, 1);

        // Register operator (1 input, 2 outputs) and edge.
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, "branch", region_id, 1, 2,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            self.region_id(),
            region_id,
        ));

        let _operator = BranchOperator::<S::Timestamp, D>::new(
            "branch",
            op_index,
            region_id,
            predicate,
        );

        let true_stream = DataStream::new(scope.clone(), true_slot, region_id);
        let false_stream = DataStream::new(scope, false_slot, region_id);
        (true_stream, false_stream)
    }
}

impl<S: Scope, D: 'static> OkErrExt<S, D> for DataStream<S, D> {
    fn ok_err<O, E, F>(
        &self,
        splitter: F,
    ) -> (DataStream<S, O>, DataStream<S, E>)
    where
        O: 'static,
        E: 'static,
        F: FnMut(D) -> std::result::Result<O, E> + Send + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let region_id = self.region_id();

        let ok_slot = Slot::new(op_index, 0);
        let err_slot = Slot::new(op_index, 1);

        // Register operator (1 input, 2 outputs) and edge.
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, "ok_err", region_id, 1, 2,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            self.region_id(),
            region_id,
        ));

        let _operator = OkErrOperator::<S::Timestamp, D, O, E>::new(
            "ok_err",
            op_index,
            region_id,
            splitter,
        );

        let ok_stream = DataStream::new(scope.clone(), ok_slot, region_id);
        let err_stream = DataStream::new(scope, err_slot, region_id);
        (ok_stream, err_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn branch_operator_creation() {
        let op = BranchOperator::<u64, i32>::new(
            "even_odd",
            0,
            RegionId::new(0),
            |x: &i32| x % 2 == 0,
        );
        assert_eq!(op.name(), "even_odd");
        assert_eq!(op.index(), 0);
        assert_eq!(op.region_id(), RegionId::new(0));
    }

    #[test]
    fn branch_splits_data() {
        let mut op = BranchOperator::<u64, i32>::new(
            "even_odd",
            0,
            RegionId::new(0),
            |x: &i32| x % 2 == 0,
        );

        op.input_mut().push_vec(1, vec![1, 2, 3, 4, 5, 6]);
        let batches = op.activate().unwrap();
        assert_eq!(batches, 1);

        let true_output: Vec<_> = op.drain_output_true().collect();
        let false_output: Vec<_> = op.drain_output_false().collect();

        assert_eq!(true_output, vec![(1, vec![2, 4, 6])]);
        assert_eq!(false_output, vec![(1, vec![1, 3, 5])]);
    }

    #[test]
    fn branch_all_true() {
        let mut op = BranchOperator::<u64, i32>::new(
            "all_true",
            0,
            RegionId::new(0),
            |_: &i32| true,
        );

        op.input_mut().push_vec(5, vec![10, 20, 30]);
        op.activate().unwrap();

        let true_output: Vec<_> = op.drain_output_true().collect();
        let false_output: Vec<_> = op.drain_output_false().collect();

        assert_eq!(true_output, vec![(5, vec![10, 20, 30])]);
        assert!(false_output.is_empty());
    }

    #[test]
    fn branch_all_false() {
        let mut op = BranchOperator::<u64, i32>::new(
            "all_false",
            0,
            RegionId::new(0),
            |_: &i32| false,
        );

        op.input_mut().push_vec(3, vec![1, 2, 3]);
        op.activate().unwrap();

        let true_output: Vec<_> = op.drain_output_true().collect();
        let false_output: Vec<_> = op.drain_output_false().collect();

        assert!(true_output.is_empty());
        assert_eq!(false_output, vec![(3, vec![1, 2, 3])]);
    }

    #[test]
    fn branch_empty_input() {
        let mut op = BranchOperator::<u64, i32>::new(
            "empty",
            0,
            RegionId::new(0),
            |x: &i32| *x > 0,
        );

        let batches = op.activate().unwrap();
        assert_eq!(batches, 0);
    }

    #[test]
    fn branch_multiple_timestamps() {
        let mut op = BranchOperator::<u64, i32>::new(
            "pos_neg",
            0,
            RegionId::new(0),
            |x: &i32| *x > 0,
        );

        op.input_mut().push_vec(1, vec![-1, 2, -3]);
        op.input_mut().push_vec(2, vec![4, -5]);
        op.activate().unwrap();

        let true_output: Vec<_> = op.drain_output_true().collect();
        let false_output: Vec<_> = op.drain_output_false().collect();

        assert_eq!(true_output, vec![(1, vec![2]), (2, vec![4])]);
        assert_eq!(false_output, vec![(1, vec![-1, -3]), (2, vec![-5])]);
    }

    #[test]
    fn branch_is_done() {
        let mut op = BranchOperator::<u64, i32>::new(
            "test",
            0,
            RegionId::new(0),
            |_: &i32| true,
        );

        assert!(!op.is_done());

        // Exhausted but pending data → not done
        op.input_mut().push_vec(1, vec![1, 2]);
        op.input_mut().mark_exhausted();
        assert!(!op.is_done());

        // Drain pending → done
        op.activate().unwrap();
        assert!(op.is_done());
    }

    #[test]
    fn ok_err_operator_creation() {
        let op = OkErrOperator::<u64, String, i32, String>::new(
            "parse",
            1,
            RegionId::new(0),
            |s: String| s.parse::<i32>().map_err(|e| e.to_string()),
        );
        assert_eq!(op.name(), "parse");
        assert_eq!(op.index(), 1);
    }

    #[test]
    fn ok_err_splits_results() {
        let mut op = OkErrOperator::<u64, i32, i32, i32>::new(
            "split",
            0,
            RegionId::new(0),
            |x: i32| if x >= 0 { Ok(x * 2) } else { Err(x) },
        );

        op.input_mut().push_vec(1, vec![1, -2, 3, -4, 5]);
        op.activate().unwrap();

        let ok_output: Vec<_> = op.drain_output_ok().collect();
        let err_output: Vec<_> = op.drain_output_err().collect();

        assert_eq!(ok_output, vec![(1, vec![2, 6, 10])]);
        assert_eq!(err_output, vec![(1, vec![-2, -4])]);
    }

    #[test]
    fn ok_err_all_ok() {
        let mut op = OkErrOperator::<u64, i32, i32, String>::new(
            "all_ok",
            0,
            RegionId::new(0),
            |x: i32| Ok(x),
        );

        op.input_mut().push_vec(1, vec![10, 20]);
        op.activate().unwrap();

        let ok_output: Vec<_> = op.drain_output_ok().collect();
        let err_output: Vec<_> = op.drain_output_err().collect();

        assert_eq!(ok_output, vec![(1, vec![10, 20])]);
        assert!(err_output.is_empty());
    }

    #[test]
    fn ok_err_all_err() {
        let mut op = OkErrOperator::<u64, i32, String, i32>::new(
            "all_err",
            0,
            RegionId::new(0),
            |x: i32| Err(x),
        );

        op.input_mut().push_vec(1, vec![1, 2, 3]);
        op.activate().unwrap();

        let ok_output: Vec<_> = op.drain_output_ok().collect();
        let err_output: Vec<_> = op.drain_output_err().collect();

        assert!(ok_output.is_empty());
        assert_eq!(err_output, vec![(1, vec![1, 2, 3])]);
    }

    #[test]
    fn ok_err_type_transform() {
        let mut op = OkErrOperator::<u64, &str, i32, String>::new(
            "parse",
            0,
            RegionId::new(0),
            |s: &str| s.parse::<i32>().map_err(|e| e.to_string()),
        );

        op.input_mut().push_vec(1, vec!["42", "bad", "7"]);
        op.activate().unwrap();

        let ok_output: Vec<_> = op.drain_output_ok().collect();
        let err_output: Vec<_> = op.drain_output_err().collect();

        assert_eq!(ok_output, vec![(1, vec![42, 7])]);
        assert_eq!(err_output.len(), 1);
        assert_eq!(err_output[0].0, 1);
        assert_eq!(err_output[0].1.len(), 1);
        assert!(err_output[0].1[0].contains("invalid"));
    }

    #[test]
    fn ok_err_is_done() {
        let mut op = OkErrOperator::<u64, i32, i32, i32>::new(
            "test",
            0,
            RegionId::new(0),
            |x: i32| Ok(x),
        );

        assert!(!op.is_done());

        // Exhausted but pending data → not done
        op.input_mut().push_vec(1, vec![10]);
        op.input_mut().mark_exhausted();
        assert!(!op.is_done());

        // Drain pending → done
        op.activate().unwrap();
        assert!(op.is_done());
    }

    #[test]
    fn branch_ext_produces_two_streams() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let (true_stream, false_stream) = stream.branch(|x: &i32| *x > 0);

        // Both streams in same region
        assert_eq!(true_stream.region_id(), region_id);
        assert_eq!(false_stream.region_id(), region_id);
        // Different slots (0 vs 1) on same operator
        assert_eq!(true_stream.source().slot_index, 0);
        assert_eq!(false_stream.source().slot_index, 1);
        assert_eq!(true_stream.source().operator_index, false_stream.source().operator_index);
    }

    #[test]
    fn ok_err_ext_produces_two_streams() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let (ok_stream, err_stream): (DataStream<RootScope<u64>, i32>, DataStream<RootScope<u64>, String>) =
            stream.ok_err(|x: i32| if x > 0 { Ok(x) } else { Err(format!("negative: {x}")) });

        // Both streams in same region
        assert_eq!(ok_stream.region_id(), region_id);
        assert_eq!(err_stream.region_id(), region_id);
        // Different slots on same operator
        assert_eq!(ok_stream.source().slot_index, 0);
        assert_eq!(err_stream.source().slot_index, 1);
    }

    #[test]
    fn branch_debug() {
        let op = BranchOperator::<u64, i32>::new(
            "test_branch",
            5,
            RegionId::new(2),
            |_: &i32| true,
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("BranchOperator"));
        assert!(debug.contains("test_branch"));
    }

    #[test]
    fn ok_err_debug() {
        let op = OkErrOperator::<u64, i32, i32, i32>::new(
            "test_ok_err",
            3,
            RegionId::new(1),
            |x: i32| Ok(x),
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("OkErrOperator"));
        assert!(debug.contains("test_ok_err"));
    }
}
