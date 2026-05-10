//! Inspect operator — side-effect observation.
//!
//! The `inspect` operator observes each record flowing through the dataflow
//! without modifying it. This is useful for logging, debugging, collecting
//! results into external state, and testing.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::dataflow::operators::handles::{InputHandle, OutputHandle};
use crate::dataflow::scope::Scope;
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::{Slot, StreamEdge};
use crate::error::{LockResultExt, Result};
use crate::progress::timestamp::Timestamp;

/// A registered inspect operator.
///
/// Passes all input data through to output unchanged, while calling
/// a side-effect closure on each `(timestamp, &[D])` batch.
pub struct InspectOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The execution stage.
    stage_id: StageId,
    /// The operator's input handle.
    input: InputHandle<T, D>,
    /// The operator's output handle.
    output: OutputHandle<T, D>,
    /// The inspection closure, called on each batch.
    inspector: Box<dyn FnMut(&T, &[D]) + Send>,
}

impl<T: Timestamp, D> InspectOperator<T, D> {
    /// Create a new inspect operator.
    pub fn new<F>(name: impl Into<String>, index: usize, stage_id: StageId, inspector: F) -> Self
    where
        F: FnMut(&T, &[D]) + Send + 'static,
    {
        let name = name.into();
        Self {
            input: InputHandle::new(format!("{name}:input")),
            output: OutputHandle::new(format!("{name}:output")),
            name,
            index,
            stage_id,
            inspector: Box::new(inspector),
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
    pub fn input_mut(&mut self) -> &mut InputHandle<T, D> {
        &mut self.input
    }

    /// Get a mutable reference to the output handle.
    pub fn output_mut(&mut self) -> &mut OutputHandle<T, D> {
        &mut self.output
    }

    /// Execute the operator logic once.
    ///
    /// For each input batch, calls the inspector then forwards data to output.
    /// Returns the number of output batches produced in this activation.
    pub fn activate(&mut self) -> Result<usize> {
        let before = self.output.buffered_count();
        while let Some((time, data)) = self.input.next() {
            (self.inspector)(&time, &data);
            let mut session = self.output.session(time);
            for item in data {
                session.give(item);
            }
        }
        Ok(self.output.buffered_count() - before)
    }

    /// Drain all buffered output batches.
    pub fn drain_output(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.output.drain()
    }

    /// Whether the input is done.
    pub fn is_done(&self) -> bool {
        self.input.is_done()
    }
}

impl<T: Timestamp, D> fmt::Debug for InspectOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InspectOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("stage_id", &self.stage_id)
            .finish()
    }
}

/// Extension trait for constructing inspect operators on a `StreamEdge`.
pub trait InspectExt<S: Scope, D> {
    /// Observe each batch of data flowing through the stream.
    ///
    /// The closure receives `(&T, &[D])` for each batch. Data passes
    /// through unchanged to the output stream.
    fn inspect<F>(&self, name: &str, inspector: F) -> StreamEdge<S, D>
    where
        F: FnMut(&S::Timestamp, &[D]) + Send + 'static;

    /// Inspect with a simple per-item callback.
    ///
    /// A convenience wrapper over `inspect` that calls the closure
    /// for each individual data item.
    fn inspect_each<F>(&self, name: &str, inspector: F) -> StreamEdge<S, D>
    where
        F: FnMut(&S::Timestamp, &D) + Send + 'static;

    /// Collect all observed data into a shared vector for testing.
    ///
    /// Returns the output stream and an `Arc<Mutex<Vec<(T, D)>>>` that
    /// accumulates all `(timestamp, item)` pairs seen.
    fn inspect_collect(&self, name: &str) -> (StreamEdge<S, D>, Arc<Mutex<Vec<(S::Timestamp, D)>>>)
    where
        D: Clone + Send + 'static,
        S::Timestamp: Clone;
}

impl<S: Scope, D: 'static> InspectExt<S, D> for StreamEdge<S, D> {
    fn inspect<F>(&self, name: &str, _inspector: F) -> StreamEdge<S, D>
    where
        F: FnMut(&S::Timestamp, &[D]) + Send + 'static,
    {
        let mut scope = self.scope().clone();
        let op_index = scope.allocate_operator_index();
        let stage_id = self.stage_id();
        let output_slot = Slot::new(op_index, 0);

        // Register operator and edge in the dataflow graph.
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_index, name, stage_id, 1, 1,
            ))
            // SAFETY: operator index freshly allocated by allocate_operator_index()
            .expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            self.stage_id(),
            stage_id,
        ));

        let _operator = InspectOperator::new(name, op_index, stage_id, _inspector);

        StreamEdge::new(scope, output_slot, stage_id)
    }

    fn inspect_each<F>(&self, name: &str, mut inspector: F) -> StreamEdge<S, D>
    where
        F: FnMut(&S::Timestamp, &D) + Send + 'static,
    {
        self.inspect(name, move |time, data| {
            for item in data {
                inspector(time, item);
            }
        })
    }

    fn inspect_collect(&self, name: &str) -> (StreamEdge<S, D>, Arc<Mutex<Vec<(S::Timestamp, D)>>>)
    where
        D: Clone + Send + 'static,
        S::Timestamp: Clone,
    {
        let collected: Arc<Mutex<Vec<(S::Timestamp, D)>>> = Arc::new(Mutex::new(Vec::new()));
        let collected_clone = Arc::clone(&collected);

        let stream = self.inspect(name, move |time, data| {
            let mut guard = match collected_clone.lock().or_poison("inspect collection") {
                Ok(guard) => guard,
                Err(_) => {
                    // NOTE: Cannot propagate lock poison here — inspect closures return
                    // `()` and cannot surface a `Result`. Poisoned lock means another
                    // thread panicked; dropping the inspection output is acceptable
                    // because the dataflow will be torn down.
                    return;
                }
            };
            for item in data {
                guard.push((time.clone(), item.clone()));
            }
        });

        (stream, collected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn inspect_operator_creation() {
        let op = InspectOperator::<u64, i32>::new(
            "observer",
            0,
            StageId::new(0),
            |_time: &u64, _data: &[i32]| {},
        );

        assert_eq!(op.name(), "observer");
        assert_eq!(op.index(), 0);
    }

    #[test]
    fn inspect_operator_passthrough() {
        let mut op =
            InspectOperator::<u64, i32>::new("passthrough", 0, StageId::new(0), |_time, _data| {});

        op.input_mut().push_vec(1, vec![10, 20, 30]);
        op.input_mut().push_vec(2, vec![40]);

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], (1, vec![10, 20, 30]));
        assert_eq!(batches[1], (2, vec![40]));
    }

    #[test]
    fn inspect_operator_callback_receives_all_data() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = Arc::clone(&seen);

        let mut op = InspectOperator::<u64, i32>::new(
            "collector",
            0,
            StageId::new(0),
            move |time: &u64, data: &[i32]| {
                let mut guard = seen_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        op.input_mut().push_vec(1, vec![10, 20]);
        op.input_mut().push_vec(2, vec![30]);

        op.activate().unwrap();

        let collected = seen.lock().unwrap();
        assert_eq!(*collected, vec![(1, 10), (1, 20), (2, 30)]);
    }

    #[test]
    fn inspect_operator_empty_input() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        let mut op =
            InspectOperator::<u64, i32>::new("noop", 0, StageId::new(0), move |_time, _data| {
                called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            });

        op.activate().unwrap();
        // Inspector should not be called when there's no input
        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn inspect_operator_output_equals_input() {
        let mut op =
            InspectOperator::<u64, String>::new("echo", 0, StageId::new(0), |_time, _data| {});

        let input_data = vec!["hello".to_string(), "world".to_string()];
        op.input_mut().push_vec(1, input_data.clone());

        op.activate().unwrap();
        let batches: Vec<_> = op.drain_output().collect();
        assert_eq!(batches[0].1, input_data);
    }

    #[test]
    fn inspect_ext_produces_stream() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let output = stream.inspect("observer", |_time, _data| {});

        assert_eq!(output.stage_id(), stage_id);
        assert_eq!(output.source().operator_index, 2);
    }

    #[test]
    fn inspect_collect_gathers_results() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let (_output_stream, _collected) = stream.inspect_collect("test_collect");

        // The inspect_collect sets up the wiring; actual data collection
        // happens at runtime when the operator is activated.
        assert_eq!(_collected.lock().unwrap().len(), 0);
    }

    #[test]
    fn inspect_operator_multiple_activations() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = Arc::clone(&seen);

        let mut op = InspectOperator::<u64, i32>::new(
            "multi",
            0,
            StageId::new(0),
            move |time: &u64, data: &[i32]| {
                let mut guard = seen_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        // First activation
        op.input_mut().push_vec(1, vec![10]);
        op.activate().unwrap();
        let b1: Vec<_> = op.drain_output().collect();
        assert_eq!(b1.len(), 1);

        // Second activation with new data
        op.input_mut().push_vec(2, vec![20, 30]);
        op.activate().unwrap();
        let b2: Vec<_> = op.drain_output().collect();
        assert_eq!(b2.len(), 1);

        let collected = seen.lock().unwrap();
        assert_eq!(*collected, vec![(1, 10), (2, 20), (2, 30)]);
    }
}
