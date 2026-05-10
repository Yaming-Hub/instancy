//! Unary operator — single input, single output.
//!
//! The `unary` operator is the fundamental building block for most dataflow
//! computations. It takes a synchronous closure that processes input batches
//! and produces output batches.

use std::fmt;

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::scope::Scope;
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::{Slot, StreamEdge};
use crate::error::Result;
use crate::progress::timestamp::Timestamp;

/// A registered unary operator that can be activated.
///
/// This struct holds the operator's metadata and its logic closure.
/// The runtime uses this to dispatch the operator when input data arrives.
pub struct UnaryOperator<T: Timestamp, D1, D2> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution stage this operator belongs to.
    stage_id: StageId,
    /// The operator's input handle.
    input: InputHandle<T, D1>,
    /// The operator's output handle.
    output: OutputHandle<T, D2>,
    /// The operator logic closure.
    logic: Box<dyn FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send>,
}

impl<T: Timestamp, D1, D2> UnaryOperator<T, D1, D2> {
    /// Create a new unary operator.
    pub fn new<L>(name: impl Into<String>, index: usize, stage_id: StageId, logic: L) -> Self
    where
        L: FnMut(&mut InputHandle<T, D1>, &mut OutputHandle<T, D2>) -> Result<()> + Send + 'static,
    {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            stage_id,
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

    /// Get the stage ID.
    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }

    /// Get a mutable reference to the input handle.
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D1> {
        &mut self.input
    }

    /// Get a reference to the output handle.
    pub fn output(&self) -> &OutputHandle<T, D2> {
        &self.output
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D2> {
        &mut self.output
    }

    /// Execute the operator logic once.
    ///
    /// Processes all pending input and produces output. Returns the
    /// number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();
        (self.logic)(&mut self.input, &mut self.output)?;
        Ok(self.output.buffered_count() - before)
    }

    /// Drain all buffered output batches.
    pub fn drain_output(&mut self) -> impl Iterator<Item = (T, Vec<D2>)> + '_ {
        self.output.drain()
    }

    /// Whether the input is done and no more data will arrive.
    pub fn is_done(&self) -> bool {
        self.input.is_done()
    }
}

impl<T: Timestamp, D1, D2> fmt::Debug for UnaryOperator<T, D1, D2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("stage_id", &self.stage_id)
            .finish()
    }
}

/// Extension trait for constructing unary operators on a `StreamEdge`.
pub trait UnaryExt<S: Scope, D1> {
    /// Create a unary operator with one input and one output.
    ///
    /// The closure receives the input and output handles and should
    /// process all available input, producing output via sessions.
    ///
    /// # Example
    /// ```ignore
    /// let doubled = stream.unary("double", |input, output| {
    ///     while let Some((time, data)) = input.next() {
    ///         let mut session = output.session(time);
    ///         for item in data {
    ///             session.give(item * 2);
    ///         }
    ///     }
    ///     Ok(())
    /// });
    /// ```
    fn unary<D2, L>(&self, name: &str, logic: L) -> StreamEdge<S, D2>
    where
        D2: 'static,
        L: FnMut(
                &mut InputHandle<S::Timestamp, D1>,
                &mut OutputHandle<S::Timestamp, D2>,
            ) -> Result<()>
            + Send
            + 'static;
}

impl<S: Scope, D1: 'static> UnaryExt<S, D1> for StreamEdge<S, D1> {
    fn unary<D2, L>(&self, name: &str, logic: L) -> StreamEdge<S, D2>
    where
        D2: 'static,
        L: FnMut(
                &mut InputHandle<S::Timestamp, D1>,
                &mut OutputHandle<S::Timestamp, D2>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let stage_id = self.stage_id();
        let source_slot = *self.source();
        let output_slot = Slot::new(op_index, 0);

        // Register operator and edge in the dataflow graph.
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_index, name, stage_id, 1, 1,
            ))
            // SAFETY: operator index freshly allocated by allocate_operator_index()
            .expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            source_slot,
            Slot::new(op_index, 0),
            self.stage_id(),
            stage_id,
        ));

        // Create the operator for validation. The runtime will
        // re-create it with channel wiring during materialization.
        let _operator = UnaryOperator::new(name, op_index, stage_id, logic);

        StreamEdge::new(scope, output_slot, stage_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;
    use crate::error::Error;

    #[test]
    fn unary_operator_creation() {
        let op =
            UnaryOperator::<u64, i32, i32>::new("double", 0, StageId::new(0), |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            });

        assert_eq!(op.name(), "double");
        assert_eq!(op.index(), 0);
        assert_eq!(op.stage_id(), StageId::new(0));
    }

    #[test]
    fn unary_operator_identity_passthrough() {
        let mut op =
            UnaryOperator::<u64, i32, i32>::new("identity", 0, StageId::new(0), |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item);
                    }
                }
                Ok(())
            });

        op.input_mut().push_vec(1, vec![10, 20, 30]);
        op.input_mut().push_vec(2, vec![40]);

        let count = op.activate().unwrap();
        assert_eq!(count, 2);

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (1, vec![10, 20, 30]));
        assert_eq!(batches[1], (2, vec![40]));
    }

    #[test]
    fn unary_operator_transform() {
        let mut op = UnaryOperator::<u64, i32, String>::new(
            "to_string",
            0,
            StageId::new(0),
            |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(format!("val:{item}"));
                    }
                }
                Ok(())
            },
        );

        op.input_mut().push_vec(1, vec![42]);
        op.activate().unwrap();

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (1, vec!["val:42".to_string()]));
    }

    #[test]
    fn unary_operator_stateful_accumulation() {
        let mut sum = 0i64;
        let mut op = UnaryOperator::<u64, i32, i64>::new(
            "running_sum",
            0,
            StageId::new(0),
            move |input, output| {
                while let Some((time, data)) = input.next() {
                    for item in data {
                        sum += item as i64;
                    }
                    let mut session = output.session(time);
                    session.give(sum);
                }
                Ok(())
            },
        );

        op.input_mut().push_vec(1, vec![10, 20]);
        op.activate().unwrap();
        let b1: Vec<_> = op.drain_output().collect();
        assert_eq!(b1[0], (1, vec![30i64]));

        op.input_mut().push_vec(2, vec![5]);
        op.activate().unwrap();
        let b2: Vec<_> = op.drain_output().collect();
        assert_eq!(b2[0], (2, vec![35i64]));
    }

    #[test]
    fn unary_operator_error_propagation() {
        let mut op = UnaryOperator::<u64, i32, i32>::new(
            "failing_op",
            0,
            StageId::new(0),
            |input, _output| {
                while let Some((_time, data)) = input.next() {
                    if data.contains(&-1) {
                        return Err(Error::operator(
                            "failing_op",
                            std::io::Error::other("negative value"),
                        ));
                    }
                }
                Ok(())
            },
        );

        op.input_mut().push_vec(1, vec![1, 2, -1, 3]);
        let result = op.activate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative value"));
    }

    #[test]
    fn unary_operator_filter() {
        let mut op = UnaryOperator::<u64, i32, i32>::new(
            "filter_even",
            0,
            StageId::new(0),
            |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        if item % 2 == 0 {
                            session.give(item);
                        }
                    }
                }
                Ok(())
            },
        );

        op.input_mut().push_vec(1, vec![1, 2, 3, 4, 5, 6]);
        op.activate().unwrap();

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (1, vec![2, 4, 6]));
    }

    #[test]
    fn unary_operator_flatmap() {
        let mut op =
            UnaryOperator::<u64, i32, i32>::new("flatmap", 0, StageId::new(0), |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        for i in 0..item {
                            session.give(i);
                        }
                    }
                }
                Ok(())
            });

        op.input_mut().push_vec(1, vec![3]);
        op.activate().unwrap();

        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0], (1, vec![0, 1, 2]));
    }

    #[test]
    fn unary_operator_empty_input() {
        let mut op =
            UnaryOperator::<u64, i32, i32>::new("noop", 0, StageId::new(0), |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item);
                    }
                }
                Ok(())
            });

        // No input pushed
        let count = op.activate().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn unary_operator_is_done() {
        let mut op =
            UnaryOperator::<u64, i32, i32>::new("test", 0, StageId::new(0), |_input, _output| {
                Ok(())
            });

        assert!(!op.is_done());
        op.input_mut().mark_exhausted();
        assert!(op.is_done());
    }

    #[test]
    fn unary_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        // Allocate index 0 for the "source" operator
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let output: StreamEdge<RootScope<u64>, i32> = stream.unary("double", |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session(time);
                for item in data {
                    session.give(item * 2);
                }
            }
            Ok(())
        });

        // The output stream should be in the same stage with operator index 2
        // (index 0 reserved, source at 1, unary at 2)
        assert_eq!(output.stage_id(), stage_id);
        assert_eq!(output.source().operator_index, 2);
    }

    #[test]
    fn unary_ext_chained() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let result = stream
            .unary("add_one", |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item + 1);
                    }
                }
                Ok(())
            })
            .unary("double", |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            });

        // Chain of 3 operators: source(1), add_one(2), double(3)
        assert_eq!(result.source().operator_index, 3);
    }
}
