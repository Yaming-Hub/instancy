//! Self-contained runtimes for hosting instancy dataflows.
//!
//! Instancy provides two runtime tiers:
//!
//! - [`SimpleRuntime`] — lightweight, single-thread execution for tests and
//!   simple scripts. Each dataflow gets a dedicated background thread.
//! - [`RuntimeHandle`] — production runtime with a shared worker thread pool,
//!   configurable scheduling policy, and centralized cancellation.
//!
//! Both runtimes accept a [`LogicalDataflow`](crate::dataflow::LogicalDataflow)
//! and return a [`SpawnedDataflow`] handle for channel-based I/O.
//!
//! **No global state:** All shared state flows from runtime instances.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::cancellation::CancellationToken;
use crate::dataflow::dataflow_builder::LogicalDataflow;
use crate::dataflow::executor::{DataflowExecutor, ExecutorConfig};
use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;
use crate::scheduler::policy::{PriorityWithAgingPolicy, SchedulePolicy};
use crate::worker_pool::{WorkerPool, WorkerPoolConfig};

/// Configuration for creating a [`RuntimeHandle`].
///
/// Each `RuntimeHandle` gets its own worker pool, task queue, and scheduling
/// policy — fully isolated from other runtime instances.
pub struct RuntimeConfig {
    /// Number of worker threads in the pool.
    pub worker_threads: usize,
    /// Scheduling policy for the task queue. Default: PriorityWithAgingPolicy.
    pub schedule_policy: Box<dyn SchedulePolicy>,
    /// Name for this runtime (used in thread names and diagnostics).
    pub name: String,
}

impl std::fmt::Debug for RuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("worker_threads", &self.worker_threads)
            .field("schedule_policy", &"<dyn SchedulePolicy>")
            .field("name", &self.name)
            .finish()
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus(),
            schedule_policy: Box::new(PriorityWithAgingPolicy::default()),
            name: "instancy".to_string(),
        }
    }
}

/// A self-contained instancy runtime. Multiple `RuntimeHandle` instances
/// can coexist in the same process with full isolation.
///
/// Each runtime owns:
/// - A dedicated worker thread pool
/// - A task queue with configurable scheduling policy
/// - A cancellation scope (shutting down the runtime cancels all its dataflows)
///
/// # No Global State
///
/// The instancy crate contains zero `static`, `lazy_static`, `once_cell`, or
/// `thread_local!` variables. All state is rooted in `RuntimeHandle` instances.
pub struct RuntimeHandle {
    /// The worker thread pool for this runtime.
    worker_pool: WorkerPool,
    /// Scheduling policy for task ordering.
    _schedule_policy: Box<dyn SchedulePolicy>,
    /// Cancellation token for graceful shutdown of all dataflows in this runtime.
    cancel: CancellationToken,
    /// Runtime name for diagnostics.
    name: String,
}

impl RuntimeHandle {
    /// Create a new isolated runtime with the given configuration.
    ///
    /// This spawns a dedicated worker thread pool. The runtime is ready to
    /// accept dataflow submissions immediately.
    ///
    /// # Errors
    /// Returns an error if the worker pool configuration is invalid.
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let pool_config = WorkerPoolConfig {
            min_threads: config.worker_threads,
            max_threads: config.worker_threads,
            ..Default::default()
        };
        let worker_pool = WorkerPool::new(pool_config)
            .map_err(|e| crate::error::Error::Custom(e.to_string()))?;
        Ok(Self {
            worker_pool,
            _schedule_policy: config.schedule_policy,
            cancel: CancellationToken::new(),
            name: config.name,
        })
    }

    /// Returns the cancellation token for this runtime.
    ///
    /// Cancelling this token will gracefully shut down all dataflows
    /// running within this runtime.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Shut down the runtime by cancelling all running dataflows.
    ///
    /// This is **cooperative**: it signals cancellation to all dataflows but
    /// does not forcibly terminate worker threads. Worker threads will drain
    /// once operators observe cancellation and stop producing work.
    /// Full WorkerPool shutdown integration is planned for a future PR.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Returns the runtime name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the worker pool.
    pub fn worker_pool(&self) -> &WorkerPool {
        &self.worker_pool
    }

    /// Returns true if the runtime has been shut down.
    pub fn is_shutdown(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

// ---------------------------------------------------------------------------
// SimpleRuntime — lightweight single-thread runtime
// ---------------------------------------------------------------------------

/// A lightweight runtime that runs each dataflow on a dedicated background thread.
///
/// `SimpleRuntime` is the easiest way to execute a dataflow. It provides:
/// - **[`run()`](Self::run)** — run a pre-loaded dataflow to completion (blocking)
/// - **[`spawn()`](Self::spawn)** — launch a dataflow with channel-based I/O
///
/// For production workloads where multiple dataflows share a thread pool,
/// use [`RuntimeHandle`] instead.
///
/// # Example
///
/// ```ignore
/// let rt = SimpleRuntime::new();
///
/// // Build a logical dataflow
/// let builder = DataflowBuilder::<u64>::new("demo");
/// let input = builder.input::<i32>("numbers");
/// input.map("double", |_t, x| x * 2).output("results");
/// let dataflow = builder.build()?;
///
/// // Spawn on the runtime
/// let mut handle = rt.spawn(dataflow)?;
/// let sender = handle.take_input::<i32>("numbers")?;
/// sender.send(0, vec![1, 2, 3])?;
/// sender.close();
/// let results = handle.take_output::<i32>("results")?.collect_data();
/// handle.join()?;
/// ```
pub struct SimpleRuntime {
    cancel: CancellationToken,
}

impl SimpleRuntime {
    /// Create a new simple runtime.
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
        }
    }

    /// Create a simple runtime with an existing cancellation token.
    pub fn with_cancel(cancel: CancellationToken) -> Self {
        Self { cancel }
    }

    /// Returns the cancellation token for this runtime.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Run a pre-loaded dataflow to completion (blocking).
    ///
    /// The dataflow must not have declared `input()` ports — use [`spawn()`](Self::spawn)
    /// for dataflows that receive external data at runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataflow has input ports, or if the executor
    /// encounters an error during execution.
    pub fn run<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<()> {
        if dataflow.has_input_ports() {
            return Err(Error::Custom(
                "cannot run() a dataflow with declared input ports — \
                 use spawn() for dataflows that receive external data."
                    .into(),
            ));
        }

        if dataflow.operator_factories.is_empty() {
            return Ok(());
        }

        let mut executor = materialize_executor(dataflow, self.cancel.clone())?;

        let completed = executor.run()?;
        if !completed {
            return Err(Error::Custom(
                "dataflow did not complete (quiescence without termination)".into(),
            ));
        }

        Ok(())
    }

    /// Spawn a dataflow on a dedicated background thread with channel-based I/O.
    ///
    /// Returns a [`SpawnedDataflow`] handle for feeding data and collecting results.
    ///
    /// # Channel wiring
    ///
    /// For each [`input()`](crate::dataflow::DataflowBuilder::input) port:
    /// - Creates a bounded `mpsc` channel
    /// - Installs a `ChannelSourceOperator` that drains the receiver into the graph
    /// - Returns the sender via [`SpawnedDataflow::take_input()`]
    ///
    /// For each [`output()`](crate::dataflow::Pipe::output) port:
    /// - Replaces the `CollectingSink` with a `ChannelSinkOperator`
    /// - Returns the receiver via [`SpawnedDataflow::take_output()`]
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rt = SimpleRuntime::new();
    /// let mut handle = rt.spawn(dataflow)?;
    /// let sender = handle.take_input::<i32>("numbers")?;
    /// sender.send(0, vec![1, 2, 3])?;
    /// sender.close();
    /// let results = handle.take_output::<i32>("results")?.collect_data();
    /// handle.join()?;
    /// ```
    pub fn spawn<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
    ) -> Result<SpawnedDataflow<T>> {
        if dataflow.operator_factories.is_empty() && dataflow.input_port_wiring.is_empty() {
            return Err(Error::Custom("cannot spawn an empty dataflow".into()));
        }

        let cancel = self.cancel.child_token();
        let cancel_handle = cancel.clone();
        let external_inputs_open = Arc::new(AtomicUsize::new(0));
        let name = dataflow.name().to_string();

        // --- Wire input ports ---
        let mut input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();
        let input_count = dataflow.input_port_wiring.len();

        for (info, wiring) in dataflow
            .input_ports
            .iter()
            .zip(dataflow.input_port_wiring.drain(..))
        {
            let (factory, sender_any) = wiring(Arc::clone(&external_inputs_open));
            dataflow
                .operator_factories
                .push((info.operator_index, factory));
            input_senders.push((info.name.clone(), info.type_name, sender_any));
        }

        // --- Wire output ports ---
        let mut output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();

        for (info, wiring) in dataflow
            .output_ports
            .iter()
            .zip(dataflow.output_port_wiring.drain(..))
        {
            let (replacement_factory, receiver_any) = wiring();
            if let Some(pos) = dataflow
                .operator_factories
                .iter()
                .position(|(idx, _)| *idx == info.operator_index)
            {
                dataflow.operator_factories[pos] =
                    (info.operator_index, replacement_factory);
            }
            output_receivers.push((info.name.clone(), info.type_name, receiver_any));
        }

        // --- Materialize and run on background thread ---
        let mut executor = materialize_executor(dataflow, cancel)?;

        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        let join_handle = std::thread::Builder::new()
            .name(format!("dataflow-{}", name))
            .spawn(move || -> Result<bool> { executor.run() })
            .map_err(|e| Error::Custom(format!("failed to spawn dataflow thread: {e}")))?;

        Ok(SpawnedDataflow {
            name,
            cancel: cancel_handle,
            join_handle: Some(join_handle),
            input_senders,
            output_receivers,
            _phantom: PhantomData,
        })
    }
}

impl Default for SimpleRuntime {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SpawnedDataflow — handle for a running dataflow
// ---------------------------------------------------------------------------

/// A handle to a running dataflow spawned on a background thread.
///
/// Provides typed access to input senders and output receivers for each
/// named port declared during graph construction. The dataflow runs
/// independently on its own thread; use the methods below to feed data,
/// collect results, cancel execution, or wait for completion.
///
/// # Type safety
///
/// Port types are validated at runtime: calling `take_input::<i32>("x")` on a
/// port declared as `input::<String>("x")` will return an error.
///
/// # Lifecycle
///
/// 1. Take input senders via [`take_input()`](Self::take_input)
/// 2. Send data, then close inputs (drop or call `.close()`)
/// 3. Take output receivers via [`take_output()`](Self::take_output)
/// 4. Call [`join()`](Self::join) to wait for the executor to finish
///
/// Dropping a `SpawnedDataflow` without calling `join()` will cancel the
/// dataflow and wait for the background thread to exit.
pub struct SpawnedDataflow<T: Timestamp> {
    name: String,
    cancel: CancellationToken,
    join_handle: Option<std::thread::JoinHandle<Result<bool>>>,
    /// (name, type_name, Box<InputSender<T, D>> as Box<dyn Any>)
    input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    /// (name, type_name, Box<OutputReceiver<T, D>> as Box<dyn Any>)
    output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    _phantom: PhantomData<T>,
}

impl<T: Timestamp> SpawnedDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Take the input sender for the named port (consumes it from the handle).
    ///
    /// Input senders can only be taken once — subsequent calls for the
    /// same port will return an error. Drop the returned sender (or call
    /// `.close()`) to signal that no more data will arrive on this port.
    ///
    /// # Type safety
    ///
    /// The type parameter `D` must match the type used in `builder.input::<D>(name)`.
    pub fn take_input<D: Clone + Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::InputSender<T, D>> {
        let type_name = std::any::type_name::<D>();
        let pos = self
            .input_senders
            .iter()
            .position(|(n, _, _)| n == name)
            .ok_or_else(|| Error::Custom(format!("no input port named '{name}'")))?;

        let (_, port_type, _) = &self.input_senders[pos];
        if *port_type != type_name {
            return Err(Error::Custom(format!(
                "input port '{name}' has type {port_type}, but requested {type_name}"
            )));
        }

        let (_, _, sender_any) = self.input_senders.remove(pos);
        sender_any
            .downcast::<crate::dataflow::channel_operators::InputSender<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!("input port '{name}' type downcast failed")))
    }

    /// Take the output receiver for the named port (consumes it from the handle).
    ///
    /// Output receivers can only be taken once — subsequent calls for the
    /// same port will return an error.
    ///
    /// # Type safety
    ///
    /// The type parameter `D` must match the type used in `stream.output(name)`.
    pub fn take_output<D: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::OutputReceiver<T, D>> {
        let type_name = std::any::type_name::<D>();
        let pos = self
            .output_receivers
            .iter()
            .position(|(n, _, _)| n == name)
            .ok_or_else(|| Error::Custom(format!("no output port named '{name}'")))?;

        let (_, port_type, _) = &self.output_receivers[pos];
        if *port_type != type_name {
            return Err(Error::Custom(format!(
                "output port '{name}' has type {port_type}, but requested {type_name}"
            )));
        }

        let (_, _, receiver_any) = self.output_receivers.remove(pos);
        receiver_any
            .downcast::<crate::dataflow::channel_operators::OutputReceiver<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!("output port '{name}' type downcast failed")))
    }

    /// Cancel the running dataflow.
    ///
    /// Signals the executor's cancellation token. The executor will stop
    /// at the next cancellation check point. Does not block.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Wait for the dataflow to complete and return the result.
    ///
    /// Returns `Ok(())` if the dataflow ran to completion.
    /// Returns an error if the executor encountered an error or the
    /// background thread panicked.
    pub fn join(mut self) -> Result<()> {
        if let Some(handle) = self.join_handle.take() {
            match handle.join() {
                Ok(Ok(_completed)) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(_panic) => Err(Error::Custom("dataflow thread panicked".into())),
            }
        } else {
            Ok(())
        }
    }
}

impl<T: Timestamp> Drop for SpawnedDataflow<T> {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Materialize a LogicalDataflow into a ready-to-run DataflowExecutor.
///
/// Shared by both `SimpleRuntime::run()` and `SimpleRuntime::spawn()`.
fn materialize_executor<T: Timestamp>(
    dataflow: LogicalDataflow<T>,
    cancel: CancellationToken,
) -> Result<DataflowExecutor<T>> {
    let executor_config = ExecutorConfig {
        max_activations_per_step: 1024,
        max_idle_sweeps: 64,
    };

    let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
        &dataflow.graph,
        dataflow.operator_factories,
        dataflow.channel_factories,
        executor_config,
        cancel,
    )?;

    // Build and attach progress tracker
    let mut tracker = dataflow.subgraph_builder.build();
    tracker.initialize();
    executor.set_progress_tracker(tracker);

    // Register probes
    for (op_idx, probe) in dataflow.probes {
        executor.register_probe(op_idx, probe);
    }

    Ok(executor)
}

/// Returns the number of available CPUs (minimum 1).
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::policy::FifoPolicy;

    #[test]
    fn create_default_runtime() {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        assert!(!rt.is_shutdown());
        assert_eq!(rt.name(), "instancy");
    }

    #[test]
    fn custom_runtime_config() {
        let config = RuntimeConfig {
            worker_threads: 2,
            schedule_policy: Box::new(FifoPolicy),
            name: "test-runtime".to_string(),
        };
        let rt = RuntimeHandle::new(config).unwrap();
        assert_eq!(rt.name(), "test-runtime");
        assert!(!rt.is_shutdown());
    }

    #[test]
    fn shutdown_cancels_token() {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: Box::new(FifoPolicy),
            name: "shutdown-test".to_string(),
        })
        .unwrap();
        assert!(!rt.is_shutdown());
        rt.shutdown();
        assert!(rt.is_shutdown());
        assert!(rt.cancel_token().is_cancelled());
    }

    #[test]
    fn multiple_isolated_runtimes() {
        let rt1 = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: Box::new(FifoPolicy),
            name: "rt1".to_string(),
        })
        .unwrap();
        let rt2 = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: Box::new(FifoPolicy),
            name: "rt2".to_string(),
        })
        .unwrap();

        // Shutting down rt1 doesn't affect rt2
        rt1.shutdown();
        assert!(rt1.is_shutdown());
        assert!(!rt2.is_shutdown());
    }

    // --- SimpleRuntime tests ---

    #[test]
    fn simple_runtime_run_source_pipeline() {
        use crate::dataflow::DataflowBuilder;

        let builder = DataflowBuilder::<u64>::new("rt_run");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();

        SimpleRuntime::new().run(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![2, 4, 6]);
    }

    #[test]
    fn simple_runtime_rejects_input_ports_on_run() {
        use crate::dataflow::DataflowBuilder;

        let builder = DataflowBuilder::<u64>::new("reject_test");
        let _ = builder.input::<i32>("data").output("out");
        let dataflow = builder.build().unwrap();

        let result = SimpleRuntime::new().run(dataflow);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("input ports"));
    }

    #[test]
    fn simple_runtime_spawn_pipeline() {
        use crate::dataflow::DataflowBuilder;

        let builder = DataflowBuilder::<u64>::new("rt_spawn");
        let input = builder.input::<i32>("data");
        input.map("inc", |_t, x| x + 1).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = SimpleRuntime::new().spawn(dataflow).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();
        sender.send(0, vec![10, 20]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("out").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results[0].1, vec![11, 21]);
        handle.join().unwrap();
    }

    #[test]
    fn simple_runtime_cancel_propagates() {
        use crate::dataflow::DataflowBuilder;

        let rt = SimpleRuntime::new();
        let builder = DataflowBuilder::<u64>::new("cancel_rt");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let handle = rt.spawn(dataflow).unwrap();
        // Cancel via the runtime's token
        rt.cancel_token().cancel();
        // Should not hang
        let _ = handle.join();
    }
}
