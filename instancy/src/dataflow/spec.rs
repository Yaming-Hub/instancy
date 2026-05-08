//! Dataflow specification and builder.
//!
//! The [`DataflowSpec`] provides a builder pattern for constructing a dataflow
//! by binding named inputs, defining the computation graph, and specifying
//! output streams.

use std::collections::HashMap;
use std::fmt;

use crate::error::{Error, Result};
use crate::execute::ErrorPolicy;
use crate::progress::timestamp::Timestamp;

use super::operators::input::TimestampedInput;
use super::operators::output::OutputStream;

/// Builder for constructing a dataflow.
///
/// # Usage
///
/// ```ignore
/// let spec = DataflowSpec::new("my_dataflow")
///     .add_input("source1", my_input_source)
///     .add_input("source2", another_source)
///     .with_error_policy(ErrorPolicy::Stop)
///     .with_output_buffer_size(1024);
/// ```
pub struct DataflowSpec<T: Timestamp> {
    /// Name of this dataflow.
    name: String,
    /// Named input sources, type-erased.
    inputs: HashMap<String, Box<dyn ErasedInput<T>>>,
    /// Input insertion order (for deterministic iteration).
    input_order: Vec<String>,
    /// Number of workers for the initial execution stage.
    initial_workers: usize,
    /// Error handling policy.
    error_policy: ErrorPolicy,
    /// Output buffer size per worker.
    output_buffer_size: usize,
}

impl<T: Timestamp> DataflowSpec<T> {
    /// Create a new dataflow specification.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            inputs: HashMap::new(),
            input_order: Vec::new(),
            initial_workers: 1,
            error_policy: ErrorPolicy::Stop,
            output_buffer_size: 1024,
        }
    }

    /// Add a named input source.
    ///
    /// The input name must be unique within this dataflow.
    /// Returns an error if a duplicate name is used.
    pub fn add_input<D: Send + 'static>(
        mut self,
        name: impl Into<String>,
        input: impl TimestampedInput<T, D> + 'static,
    ) -> Result<Self> {
        let name = name.into();
        if self.inputs.contains_key(&name) {
            return Err(Error::Custom(format!("duplicate input name: '{name}'")));
        }
        self.input_order.push(name.clone());
        self.inputs.insert(name, Box::new(TypedInput::new(input)));
        Ok(self)
    }

    /// Set the initial number of workers.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.initial_workers = workers;
        self
    }

    /// Set the error handling policy.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Set the output buffer size per worker stream.
    pub fn with_output_buffer_size(mut self, size: usize) -> Self {
        self.output_buffer_size = size;
        self
    }

    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the initial worker count.
    pub fn initial_workers(&self) -> usize {
        self.initial_workers
    }

    /// Get the error policy.
    pub fn error_policy(&self) -> &ErrorPolicy {
        &self.error_policy
    }

    /// Get the output buffer size.
    pub fn output_buffer_size(&self) -> usize {
        self.output_buffer_size
    }

    /// Get the ordered list of input names.
    pub fn input_names(&self) -> &[String] {
        &self.input_order
    }

    /// Check if an input with the given name exists.
    pub fn has_input(&self, name: &str) -> bool {
        self.inputs.contains_key(name)
    }

    /// Number of registered inputs.
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Validate the dataflow specification.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Custom("dataflow name cannot be empty".into()));
        }
        if self.initial_workers == 0 {
            return Err(Error::Custom("must have at least one worker".into()));
        }
        if self.output_buffer_size == 0 {
            return Err(Error::Custom("output buffer size must be > 0".into()));
        }
        Ok(())
    }

    /// Consume the spec and return the parts for execution.
    pub fn into_parts(
        self,
    ) -> (
        String,
        HashMap<String, Box<dyn ErasedInput<T>>>,
        Vec<String>,
        usize,
        ErrorPolicy,
        usize,
    ) {
        (
            self.name,
            self.inputs,
            self.input_order,
            self.initial_workers,
            self.error_policy,
            self.output_buffer_size,
        )
    }
}

impl<T: Timestamp> fmt::Debug for DataflowSpec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataflowSpec")
            .field("name", &self.name)
            .field("inputs", &self.input_order)
            .field("initial_workers", &self.initial_workers)
            .field("error_policy", &self.error_policy)
            .field("output_buffer_size", &self.output_buffer_size)
            .finish()
    }
}

/// Type-erased input source trait.
///
/// This allows storing heterogeneous input sources (with different `D` types)
/// in the same collection. The runtime downcasts to the concrete type when
/// connecting inputs to the dataflow graph.
pub trait ErasedInput<T: Timestamp>: Send {
    /// Human-readable name of this input.
    fn name(&self) -> &str;

    /// A string identifying the data type for diagnostic purposes.
    fn data_type_name(&self) -> &str;

    /// Downcast to `Any` for runtime type recovery.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcast to mutable `Any` for runtime type recovery.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Wrapper that type-erases a `TimestampedInput<T, D>`.
struct TypedInput<T: Timestamp, D> {
    inner: Box<dyn TimestampedInput<T, D>>,
    type_name: &'static str,
}

impl<T: Timestamp, D: 'static> TypedInput<T, D> {
    fn new(input: impl TimestampedInput<T, D> + 'static) -> Self {
        Self {
            inner: Box::new(input),
            type_name: std::any::type_name::<D>(),
        }
    }
}

impl<T: Timestamp + 'static, D: Send + 'static> ErasedInput<T> for TypedInput<T, D> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn data_type_name(&self) -> &str {
        self.type_name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Accessor for named input streams inside the dataflow builder closure.
///
/// Provides methods to look up inputs by name and retrieve their types.
pub struct DataflowInputs<T: Timestamp> {
    /// Map of input name → metadata.
    inputs: HashMap<String, InputMetadata>,
    /// Ordered input names.
    input_order: Vec<String>,
    /// Phantom for the timestamp type.
    _timestamp: std::marker::PhantomData<T>,
}

/// Metadata about an input source.
#[derive(Debug, Clone)]
pub struct InputMetadata {
    /// Human-readable name.
    pub name: String,
    /// Data type name (from `std::any::type_name`).
    pub data_type: String,
}

impl<T: Timestamp> DataflowInputs<T> {
    /// Create from a map of erased inputs.
    pub fn from_erased(
        inputs: &HashMap<String, Box<dyn ErasedInput<T>>>,
        order: &[String],
    ) -> Self {
        let mut metadata = HashMap::new();
        for (name, input) in inputs {
            metadata.insert(
                name.clone(),
                InputMetadata {
                    name: input.name().to_string(),
                    data_type: input.data_type_name().to_string(),
                },
            );
        }
        Self {
            inputs: metadata,
            input_order: order.to_vec(),
            _timestamp: std::marker::PhantomData,
        }
    }

    /// Get metadata for a named input.
    pub fn get(&self, name: &str) -> Option<&InputMetadata> {
        self.inputs.get(name)
    }

    /// Check if an input exists.
    pub fn has(&self, name: &str) -> bool {
        self.inputs.contains_key(name)
    }

    /// Get the ordered list of input names.
    pub fn names(&self) -> &[String] {
        &self.input_order
    }

    /// Number of inputs.
    pub fn count(&self) -> usize {
        self.inputs.len()
    }
}

impl<T: Timestamp> fmt::Debug for DataflowInputs<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataflowInputs")
            .field("inputs", &self.input_order)
            .finish()
    }
}

/// Handle for a running or completed dataflow.
///
/// Provides access to output streams, metrics, and cancellation.
pub struct DataflowHandle<T: Timestamp, D> {
    /// Name of the dataflow.
    name: String,
    /// Output streams, one per worker.
    output_streams: Vec<OutputStream<T, D>>,
    /// Whether the dataflow has completed.
    completed: bool,
}

impl<T: Timestamp, D> DataflowHandle<T, D> {
    /// Create a new dataflow handle.
    #[cfg(test)]
    pub(crate) fn new(name: String, output_streams: Vec<OutputStream<T, D>>) -> Self {
        Self {
            name,
            output_streams,
            completed: false,
        }
    }

    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of output streams (one per worker).
    pub fn num_output_streams(&self) -> usize {
        self.output_streams.len()
    }

    /// Get a reference to the output streams.
    pub fn output_streams(&self) -> &[OutputStream<T, D>] {
        &self.output_streams
    }

    /// Take ownership of the output streams.
    ///
    /// After this call, `output_streams()` will return an empty slice.
    pub fn take_output_streams(&mut self) -> Vec<OutputStream<T, D>> {
        std::mem::take(&mut self.output_streams)
    }

    /// Check if the dataflow has completed.
    pub fn is_completed(&self) -> bool {
        self.completed
    }

    /// Mark the dataflow as completed (used in tests).
    #[cfg(test)]
    pub(crate) fn mark_completed(&mut self) {
        self.completed = true;
    }
}

impl<T: Timestamp + fmt::Debug, D: fmt::Debug> fmt::Debug for DataflowHandle<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataflowHandle")
            .field("name", &self.name)
            .field("output_streams", &self.output_streams.len())
            .field("completed", &self.completed)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::operators::input::{InputEvent, VecInput};
    use crate::dataflow::operators::output;

    #[test]
    fn dataflow_spec_builder() {
        let input = VecInput::new(
            "numbers",
            vec![
                InputEvent::data(1u64, vec![10, 20]),
                InputEvent::frontier(1),
            ],
        );

        let spec = DataflowSpec::<u64>::new("test_df")
            .add_input("src", input)
            .unwrap()
            .with_workers(4)
            .with_error_policy(ErrorPolicy::Stop)
            .with_output_buffer_size(2048);

        assert_eq!(spec.name(), "test_df");
        assert_eq!(spec.initial_workers(), 4);
        assert_eq!(spec.output_buffer_size(), 2048);
        assert_eq!(spec.input_count(), 1);
        assert!(spec.has_input("src"));
        assert!(!spec.has_input("nonexistent"));
        assert_eq!(spec.input_names(), &["src"]);
    }

    #[test]
    fn dataflow_spec_multiple_inputs() {
        let input1 = VecInput::<u64, i32>::new("numbers", vec![]);
        let input2 = VecInput::<u64, String>::new("strings", vec![]);

        let spec = DataflowSpec::<u64>::new("multi")
            .add_input("ints", input1)
            .unwrap()
            .add_input("strs", input2)
            .unwrap();

        assert_eq!(spec.input_count(), 2);
        assert!(spec.has_input("ints"));
        assert!(spec.has_input("strs"));
        assert_eq!(spec.input_names(), &["ints", "strs"]);
    }

    #[test]
    fn dataflow_spec_duplicate_input_rejected() {
        let input1 = VecInput::<u64, i32>::new("a", vec![]);
        let input2 = VecInput::<u64, i32>::new("b", vec![]);

        let result = DataflowSpec::<u64>::new("test")
            .add_input("src", input1)
            .unwrap()
            .add_input("src", input2);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("duplicate"));
        assert!(err_msg.contains("src"));
    }

    #[test]
    fn dataflow_spec_validate_empty_name() {
        let spec = DataflowSpec::<u64>::new("");
        assert!(spec.validate().is_err());
    }

    #[test]
    fn dataflow_spec_validate_zero_workers() {
        let spec = DataflowSpec::<u64>::new("test").with_workers(0);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn dataflow_spec_validate_zero_buffer() {
        let spec = DataflowSpec::<u64>::new("test").with_output_buffer_size(0);
        assert!(spec.validate().is_err());
    }

    #[test]
    fn dataflow_spec_validate_ok() {
        let spec = DataflowSpec::<u64>::new("test")
            .with_workers(4)
            .with_output_buffer_size(1024);
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn dataflow_spec_into_parts() {
        let input = VecInput::new("src", vec![InputEvent::data(1u64, vec![10])]);

        let spec = DataflowSpec::<u64>::new("parts_test")
            .add_input("s1", input)
            .unwrap()
            .with_workers(2);

        let (name, inputs, order, workers, policy, buf_size) = spec.into_parts();
        assert_eq!(name, "parts_test");
        assert_eq!(inputs.len(), 1);
        assert_eq!(order, vec!["s1"]);
        assert_eq!(workers, 2);
        assert_eq!(policy, ErrorPolicy::Stop);
        assert_eq!(buf_size, 1024);
    }

    #[test]
    fn dataflow_inputs_from_erased() {
        let input = VecInput::new("nums", vec![InputEvent::data(1u64, vec![42])]);

        let spec = DataflowSpec::<u64>::new("test")
            .add_input("source", input)
            .unwrap();
        let (_, inputs, order, _, _, _) = spec.into_parts();

        let di = DataflowInputs::from_erased(&inputs, &order);
        assert!(di.has("source"));
        assert!(!di.has("other"));
        assert_eq!(di.count(), 1);
        assert_eq!(di.names(), &["source"]);

        let meta = di.get("source").unwrap();
        assert_eq!(meta.name, "nums");
        assert!(meta.data_type.contains("i32"));
    }

    #[test]
    fn erased_input_downcast() {
        let input = VecInput::new("nums", vec![InputEvent::data(1u64, vec![42i32])]);
        let spec = DataflowSpec::<u64>::new("test")
            .add_input("source", input)
            .unwrap();
        let (_, mut inputs, _, _, _, _) = spec.into_parts();

        let erased = inputs.get_mut("source").unwrap();

        // Downcast to recover the TypedInput
        assert!(erased.as_any_mut().is::<TypedInput<u64, i32>>());
        assert!(!erased.as_any_mut().is::<TypedInput<u64, String>>());
    }

    #[test]
    fn dataflow_handle_basic() {
        let (senders, streams) = output::output_pairs::<u64, i32>(3, 4);
        let handle = DataflowHandle::new("test".into(), streams);

        assert_eq!(handle.name(), "test");
        assert_eq!(handle.num_output_streams(), 3);
        assert!(!handle.is_completed());

        // Send some data through
        senders[0]
            .send(output::OutputEvent::data(1, vec![10]))
            .unwrap();

        let streams = handle.output_streams();
        let event = streams[0].try_next().unwrap().unwrap();
        assert_eq!(event, output::OutputEvent::data(1, vec![10]));
    }

    #[test]
    fn dataflow_handle_take_streams() {
        let (_, streams) = output::output_pairs::<u64, i32>(2, 4);
        let mut handle = DataflowHandle::new("test".into(), streams);

        let taken = handle.take_output_streams();
        assert_eq!(taken.len(), 2);
        assert_eq!(handle.num_output_streams(), 0);
    }

    #[test]
    fn dataflow_handle_completed() {
        let (_, streams) = output::output_pairs::<u64, i32>(1, 4);
        let mut handle = DataflowHandle::new("test".into(), streams);

        assert!(!handle.is_completed());
        handle.mark_completed();
        assert!(handle.is_completed());
    }

    #[test]
    fn dataflow_spec_debug() {
        let spec = DataflowSpec::<u64>::new("debug_test");
        let debug = format!("{spec:?}");
        assert!(debug.contains("debug_test"));
    }

    #[test]
    fn dataflow_spec_ignore_policy() {
        let spec = DataflowSpec::<u64>::new("test").with_error_policy(ErrorPolicy::Ignore {
            description: "skip errors".into(),
        });
        match spec.error_policy() {
            ErrorPolicy::Ignore { description } => {
                assert_eq!(description, "skip errors");
            }
            _ => panic!("expected Ignore policy"),
        }
    }
}
