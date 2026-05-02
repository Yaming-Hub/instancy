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
//! ## Async completion
//!
//! [`RuntimeHandle::run()`] returns a [`DataflowCompletion`] future — callers
//! can `.await` it in async code or call [`.wait()`](DataflowCompletion::wait)
//! for blocking synchronous use. [`SpawnedDataflow::join()`] likewise returns
//! a `DataflowCompletion`.
//!
//! **No global state:** All shared state flows from runtime instances.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Waker};

use crate::cancellation::CancellationToken;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::channel_operators::ChannelMode;
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
    /// Executor registry for cooperative multiplexing of dataflow futures.
    /// Created lazily on first run()/spawn() call.
    registry: Arc<crate::executor_task::ExecutorRegistry>,
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
        let registry = worker_pool.create_registry();
        Ok(Self {
            worker_pool,
            _schedule_policy: config.schedule_policy,
            cancel: CancellationToken::new(),
            name: config.name,
            registry,
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

    /// Run a pre-loaded dataflow to completion on the worker pool.
    ///
    /// The dataflow must not have declared `input()` ports — use
    /// [`spawn()`](Self::spawn) for dataflows that receive external data.
    ///
    /// Returns a [`DataflowCompletion`] future that resolves when the executor
    /// finishes. The caller can `.await` it or call [`.wait()`](DataflowCompletion::wait)
    /// to block synchronously.
    ///
    /// # Execution model
    ///
    /// The executor is registered as an `ExecutorTask` in the pool's
    /// `ExecutorRegistry`. Pool threads cooperatively poll the task via the
    /// `poll_run()` future, yielding after each poll budget to allow other
    /// dataflows to make progress on the same threads.
    ///
    /// # Errors
    ///
    /// Returns an error immediately if the dataflow has input ports.
    /// The returned future resolves to an error if the executor encounters
    /// an error during execution.
    pub fn run<T: Timestamp>(
        &self,
        dataflow: LogicalDataflow<T>,
    ) -> Result<DataflowCompletion> {
        self.run_sync(dataflow)
    }

    /// Run a pre-loaded dataflow to completion, blocking the current thread.
    ///
    /// Convenience wrapper: equivalent to `run(df)?.wait()`.
    pub fn run_blocking<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<()> {
        self.run_sync(dataflow)?.wait()
    }

    /// Spawn a dataflow on the worker pool with synchronous channel-based I/O.
    ///
    /// Returns a [`SpawnedDataflow`] handle with sync [`InputSender`] and
    /// [`OutputReceiver`] handles. Use [`spawn_async()`](Self::spawn_async)
    /// for async I/O handles (feature-gated behind `async-io`).
    ///
    /// # Execution model
    ///
    /// The executor is registered as an `ExecutorTask` in the pool's
    /// `ExecutorRegistry`. Pool threads cooperatively poll the task via
    /// `poll_run()`, yielding after each poll budget to allow other
    /// dataflows to make progress on the same threads.
    pub fn spawn<T: Timestamp>(
        &self,
        dataflow: LogicalDataflow<T>,
    ) -> Result<SpawnedDataflow<T>> {
        self.spawn_internal(dataflow, ChannelMode::Sync)
    }

    /// Spawn a dataflow with async channel-based I/O.
    ///
    /// Like [`spawn()`](Self::spawn) but wires `tokio::sync::mpsc` channels
    /// for the external I/O ports. Use [`SpawnedDataflow::take_async_input()`]
    /// and [`SpawnedDataflow::take_async_output()`] to obtain the async handles.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut handle = rt.spawn_async(dataflow)?;
    /// let sender = handle.take_async_input::<i32>("data")?;
    /// let mut receiver = handle.take_async_output::<i32>("results")?;
    ///
    /// sender.send(0, vec![1, 2, 3]).await?;
    /// sender.close();
    ///
    /// let results = receiver.collect_data().await;
    /// handle.join().await?;
    /// ```
    #[cfg(feature = "async-io")]
    pub fn spawn_async<T: Timestamp>(
        &self,
        dataflow: LogicalDataflow<T>,
    ) -> Result<SpawnedDataflow<T>> {
        self.spawn_internal(dataflow, ChannelMode::Async)
    }

    // -- Private sync implementations --

    fn run_sync<T: Timestamp>(
        &self,
        dataflow: LogicalDataflow<T>,
    ) -> Result<DataflowCompletion> {
        if dataflow.has_input_ports() {
            return Err(Error::Custom(
                "cannot run() a dataflow with declared input ports — \
                 use spawn() for dataflows that receive external data."
                    .into(),
            ));
        }

        if dataflow.operator_factories.is_empty() {
            return Ok(DataflowCompletion::ready_ok());
        }

        let cancel = self.cancel.child_token();
        let wake_handle = WakeHandle::new();
        cancel.register_wake_handle(wake_handle.clone());
        let executor = materialize_executor(dataflow, cancel, Some(wake_handle))?;

        let (completion, notifier) = DataflowCompletion::new();

        // Register the executor as a cooperative task. The pool's worker threads
        // will poll it via the registry's ready queue.
        self.registry.register(Box::pin(executor), notifier);

        Ok(completion)
    }

    fn spawn_internal<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
        mode: ChannelMode,
    ) -> Result<SpawnedDataflow<T>> {
        if dataflow.operator_factories.is_empty() && dataflow.input_port_wiring.is_empty() {
            return Err(Error::Custom("cannot spawn an empty dataflow".into()));
        }

        let cancel = self.cancel.child_token();
        let cancel_handle = cancel.clone();
        let external_inputs_open = Arc::new(AtomicUsize::new(0));
        let name = dataflow.name().to_string();

        // Create the WakeHandle early so it can be shared with InputSenders,
        // CancellationToken, and the executor's internal channels.
        let wake_handle = WakeHandle::new();

        // Register wake handle on the cancellation token (and all ancestors)
        // so that cancel() wakes the sleeping executor promptly.
        cancel.register_wake_handle(wake_handle.clone());

        // --- Wire input ports ---
        let mut input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();
        let input_count = dataflow.input_port_wiring.len();

        for (info, mut wiring) in dataflow
            .input_ports
            .iter()
            .zip(dataflow.input_port_wiring.drain(..))
        {
            let (factory, sender_any) =
                wiring(Arc::clone(&external_inputs_open), wake_handle.clone(), mode);
            dataflow
                .operator_factories
                .push((info.operator_index, factory));
            input_senders.push((info.name.clone(), info.type_name, sender_any));
        }

        // --- Wire output ports ---
        let mut output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();

        for (info, mut wiring) in dataflow
            .output_ports
            .iter()
            .zip(dataflow.output_port_wiring.drain(..))
        {
            let (replacement_factory, receiver_any) = wiring(mode, Some(wake_handle.clone()));
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

        // --- Materialize and register as cooperative task ---
        let mut executor = materialize_executor(dataflow, cancel, Some(wake_handle))?;

        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        let (completion, notifier) = DataflowCompletion::new();

        self.registry.register(Box::pin(executor), notifier);

        Ok(SpawnedDataflow {
            name,
            cancel: cancel_handle,
            completion: Some(completion),
            input_senders,
            output_receivers,
            _phantom: PhantomData,
        })
    }
}

impl Drop for RuntimeHandle {
    /// Cancel all running dataflows before the worker pool shuts down.
    ///
    /// Without this, `WorkerPool::drop()` would join worker threads that are
    /// still running executor loops, causing a deadlock. Cancelling the token
    /// first ensures executors observe cancellation and exit promptly.
    ///
    /// **Note:** Cancellation is cooperative — executors check the token at the
    /// top of each sweep iteration. If an operator's `activate()` call blocks
    /// for a long time (e.g., heavy computation), the pool join will wait until
    /// that activation completes and the executor rechecks the token. Operators
    /// should avoid unbounded blocking in `activate()`.
    fn drop(&mut self) {
        self.cancel.cancel();
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

        let wake_handle = WakeHandle::new();
        self.cancel.register_wake_handle(wake_handle.clone());
        let mut executor = materialize_executor(dataflow, self.cancel.clone(), Some(wake_handle))?;

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

        // Create the WakeHandle early so it can be shared with InputSenders
        // and the executor's internal channels.
        let wake_handle = WakeHandle::new();

        // Register wake handle on the cancellation token so cancel() wakes
        // a sleeping executor.
        cancel.register_wake_handle(wake_handle.clone());

        // --- Wire input ports ---
        let mut input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();
        let input_count = dataflow.input_port_wiring.len();

        for (info, mut wiring) in dataflow
            .input_ports
            .iter()
            .zip(dataflow.input_port_wiring.drain(..))
        {
            let (factory, sender_any) =
                wiring(Arc::clone(&external_inputs_open), wake_handle.clone(), ChannelMode::Sync);
            dataflow
                .operator_factories
                .push((info.operator_index, factory));
            input_senders.push((info.name.clone(), info.type_name, sender_any));
        }

        // --- Wire output ports ---
        let mut output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();

        for (info, mut wiring) in dataflow
            .output_ports
            .iter()
            .zip(dataflow.output_port_wiring.drain(..))
        {
            let (replacement_factory, receiver_any) = wiring(ChannelMode::Sync, None);
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
        let mut executor = materialize_executor(dataflow, cancel, Some(wake_handle))?;

        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        let (completion, notifier) = DataflowCompletion::new();

        std::thread::Builder::new()
            .name(format!("dataflow-{}", name))
            .spawn(move || {
                let result = executor.run();
                notifier.complete(result);
            })
            .map_err(|e| Error::Custom(format!("failed to spawn dataflow thread: {e}")))?;

        Ok(SpawnedDataflow {
            name,
            cancel: cancel_handle,
            completion: Some(completion),
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
// DataflowCompletion — async/sync completion future
// ---------------------------------------------------------------------------

/// Shared state between the executor thread and the completion future.
struct SharedCompletionState {
    result: Option<Result<bool>>,
    waker: Option<Waker>,
}

/// Internal writer side: signals that the executor has finished.
///
/// If dropped without calling [`complete()`](Self::complete) (e.g., due to
/// a panic), the `Drop` impl publishes an error so the future never hangs.
pub struct CompletionNotifier {
    shared: Arc<Mutex<SharedCompletionState>>,
    condvar: Arc<Condvar>,
}

impl CompletionNotifier {
    /// Publish the executor result and wake any waiting future/condvar.
    pub fn complete(self, result: Result<bool>) {
        {
            let mut state = match self.shared.lock() {
                Ok(s) => s,
                Err(e) => e.into_inner(),
            };
            state.result = Some(result);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            self.condvar.notify_all();
        }
        // Prevent Drop from publishing a second result.
        // Safety: shared/condvar Arcs are leaked but the DataflowCompletion
        // side still holds clones that will clean up.
        std::mem::forget(self);
    }
}

impl Drop for CompletionNotifier {
    fn drop(&mut self) {
        // Executor panicked or was killed without publishing a result.
        let mut state = match self.shared.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        if state.result.is_none() {
            state.result = Some(Err(Error::Custom(
                "dataflow executor terminated unexpectedly (possible panic)".into(),
            )));
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            self.condvar.notify_all();
        }
    }
}

/// A future that resolves when a dataflow completes execution.
///
/// Can be used in two ways:
/// - **Async**: `.await` the future (implements [`Future`])
/// - **Sync**: call [`wait()`](Self::wait) to block the current thread
///
/// # Examples
///
/// ```ignore
/// // Async usage
/// let completion = rt.run(dataflow)?;
/// completion.await?;
///
/// // Sync usage
/// let completion = rt.run(dataflow)?;
/// completion.wait()?;
///
/// // Or use the convenience method
/// rt.run_blocking(dataflow)?;
/// ```
pub struct DataflowCompletion {
    shared: Arc<Mutex<SharedCompletionState>>,
    condvar: Arc<Condvar>,
}

impl std::fmt::Debug for DataflowCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self.shared.lock() {
            Ok(state) => {
                if state.result.is_some() {
                    "ready"
                } else {
                    "pending"
                }
            }
            Err(_) => "poisoned",
        };
        f.debug_struct("DataflowCompletion")
            .field("status", &status)
            .finish()
    }
}

impl DataflowCompletion {
    /// Create a new completion pair (future + notifier).
    pub fn new() -> (Self, CompletionNotifier) {
        let shared = Arc::new(Mutex::new(SharedCompletionState {
            result: None,
            waker: None,
        }));
        let condvar = Arc::new(Condvar::new());
        let completion = DataflowCompletion {
            shared: Arc::clone(&shared),
            condvar: Arc::clone(&condvar),
        };
        let notifier = CompletionNotifier { shared, condvar };
        (completion, notifier)
    }

    /// Create an already-completed future with a successful result.
    fn ready_ok() -> Self {
        let shared = Arc::new(Mutex::new(SharedCompletionState {
            result: Some(Ok(true)),
            waker: None,
        }));
        let condvar = Arc::new(Condvar::new());
        DataflowCompletion { shared, condvar }
    }

    /// Block the current thread until the dataflow completes.
    ///
    /// Returns `Ok(())` if the dataflow ran to completion, or an error if
    /// the executor failed or reached quiescence without completing.
    pub fn wait(self) -> Result<()> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| Error::Custom("completion mutex poisoned".into()))?;
        while state.result.is_none() {
            state = self
                .condvar
                .wait(state)
                .map_err(|_| Error::Custom("completion mutex poisoned during wait".into()))?;
        }
        interpret_completion(state.result.take().unwrap())
    }
}

impl Future for DataflowCompletion {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = match self.shared.lock() {
            Ok(s) => s,
            Err(_) => {
                return Poll::Ready(Err(Error::Custom(
                    "completion mutex poisoned".into(),
                )))
            }
        };
        if let Some(result) = state.result.take() {
            Poll::Ready(interpret_completion(result))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Translate executor's `Result<bool>` into the public `Result<()>`.
fn interpret_completion(result: Result<bool>) -> Result<()> {
    match result {
        Ok(true) => Ok(()),
        Ok(false) => Err(Error::Custom(
            "dataflow reached quiescence without completing — \
             some operators could not make progress"
                .into(),
        )),
        Err(e) => Err(e),
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
/// 4. Call [`join()`](Self::join) to get a completion future
///
/// Dropping a `SpawnedDataflow` without calling `join()` will cancel the
/// dataflow. The executor will stop at the next cancellation check point.
pub struct SpawnedDataflow<T: Timestamp> {
    name: String,
    cancel: CancellationToken,
    completion: Option<DataflowCompletion>,
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
            .map_err(|_| Error::Custom(format!(
                "input port '{name}' type downcast failed — if spawned with spawn_async(), use take_async_input()"
            )))
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
            .map_err(|_| Error::Custom(format!(
                "output port '{name}' type downcast failed — if spawned with spawn_async(), use take_async_output()"
            )))
    }

    /// Take the async input sender for the named port (consumes it).
    ///
    /// Only works when the dataflow was spawned with [`RuntimeHandle::spawn_async()`].
    /// Returns an error if the port was wired as sync or does not exist.
    #[cfg(feature = "async-io")]
    pub fn take_async_input<D: Clone + Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::AsyncInputSender<T, D>> {
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
            .downcast::<crate::dataflow::channel_operators::AsyncInputSender<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!(
                "input port '{name}' was not wired for async I/O (use spawn_async)"
            )))
    }

    /// Take the async output receiver for the named port (consumes it).
    ///
    /// Only works when the dataflow was spawned with [`RuntimeHandle::spawn_async()`].
    /// Returns an error if the port was wired as sync or does not exist.
    #[cfg(feature = "async-io")]
    pub fn take_async_output<D: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::AsyncOutputReceiver<T, D>> {
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
            .downcast::<crate::dataflow::channel_operators::AsyncOutputReceiver<T, D>>()
            .map(|boxed| *boxed)
            .map_err(|_| Error::Custom(format!(
                "output port '{name}' was not wired for async I/O (use spawn_async)"
            )))
    }

    /// Cancel the running dataflow.
    ///
    /// Signals the executor's cancellation token. The executor will stop
    /// at the next cancellation check point. Does not block.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Get a completion future for the dataflow.
    ///
    /// Returns a [`DataflowCompletion`] that resolves when the executor finishes.
    /// The caller can `.await` it or call [`.wait()`](DataflowCompletion::wait)
    /// to block synchronously.
    ///
    /// Consumes the handle — calling `join()` transfers lifecycle ownership
    /// to the returned future. The dataflow will **not** be cancelled on drop.
    pub fn join(mut self) -> DataflowCompletion {
        self.completion
            .take()
            .unwrap_or_else(DataflowCompletion::ready_ok)
    }

    /// Wait for the dataflow to complete, blocking the current thread.
    ///
    /// Convenience wrapper around [`join()`](Self::join) + [`DataflowCompletion::wait()`].
    pub fn join_blocking(self) -> Result<()> {
        self.join().wait()
    }
}

impl<T: Timestamp> Drop for SpawnedDataflow<T> {
    fn drop(&mut self) {
        // Only cancel if join() wasn't called — if it was, completion is None
        // and the caller owns the lifecycle via the returned DataflowCompletion.
        if self.completion.is_some() {
            self.cancel.cancel();
        }
        // Don't block waiting — cancel and detach. The executor will stop
        // at the next cancellation check point.
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Materialize a LogicalDataflow into a ready-to-run DataflowExecutor.
///
/// If `wake_handle` is provided, the executor uses it (shared with InputSenders
/// and CancellationTokens). Otherwise a fresh one is created internally.
fn materialize_executor<T: Timestamp>(
    dataflow: LogicalDataflow<T>,
    cancel: CancellationToken,
    wake_handle: Option<WakeHandle>,
) -> Result<DataflowExecutor<T>> {
    let executor_config = ExecutorConfig {
        max_activations_per_step: 1024,
        max_idle_sweeps: 64,
        max_sweeps_per_poll: 64,
    };

    let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
        &dataflow.graph,
        dataflow.operator_factories,
        dataflow.channel_factories,
        executor_config,
        cancel,
        wake_handle,
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
        handle.join_blocking().unwrap();
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
        let _ = handle.join_blocking();
    }

    // --- RuntimeHandle execution tests ---

    #[test]
    fn runtime_handle_run_basic() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("rt_run");
        let port = builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();

        rt.run_blocking(dataflow).unwrap();

        let c = port.collector();
        let r = c.lock().unwrap();
        assert_eq!(r[0].1, vec![2, 4, 6]);
    }

    #[test]
    fn runtime_handle_spawn_basic() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("rt_spawn");
        let input = builder.input::<i32>("data");
        input.map("double", |_t, x| x * 2).output("results");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();
        sender.send(0u64, vec![10, 20]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("results").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results[0].1, vec![20, 40]);
        handle.join_blocking().unwrap();
    }

    #[test]
    fn runtime_handle_shutdown_cancels() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("rt_cancel");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let handle = rt.spawn(dataflow).unwrap();
        rt.shutdown();
        // Should complete (cancelled), not hang
        let _ = handle.join_blocking();
    }

    #[test]
    fn runtime_handle_multiple_dataflows() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        for i in 0..3 {
            let builder = DataflowBuilder::<u64>::new(format!("df_{i}"));
            builder
                .source("data", vec![(0u64, vec![i as i32])])
                .output("out");
            let dataflow = builder.build().unwrap();
            rt.run_blocking(dataflow).unwrap();
        }
    }

    #[test]
    fn runtime_handle_run_rejects_input_ports() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("bad");
        builder.input::<i32>("x").output("y");
        let dataflow = builder.build().unwrap();

        let result = rt.run(dataflow);
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("input ports"));
    }

    #[test]
    fn dataflow_completion_future_poll() {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        // Create a minimal waker for manual polling
        fn noop_raw_waker() -> RawWaker {
            fn no_op(_: *const ()) {}
            fn clone(p: *const ()) -> RawWaker {
                RawWaker::new(p, &VTABLE)
            }
            const VTABLE: RawWakerVTable =
                RawWakerVTable::new(clone, no_op, no_op, no_op);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
        let mut cx = Context::from_waker(&waker);

        let (mut completion, notifier) = DataflowCompletion::new();

        // Before completion, polling returns Pending
        let pinned = Pin::new(&mut completion);
        assert!(matches!(pinned.poll(&mut cx), Poll::Pending));

        // After notifier signals, polling returns Ready
        notifier.complete(Ok(true));

        let pinned = Pin::new(&mut completion);
        match pinned.poll(&mut cx) {
            Poll::Ready(Ok(())) => {} // expected
            other => panic!("expected Ready(Ok(())), got {:?}", other),
        }
    }

    #[test]
    fn dataflow_completion_notifier_drop_signals_error() {
        // If the notifier is dropped without calling complete(),
        // the future should resolve to an error (panic safety).
        let (completion, notifier) = DataflowCompletion::new();
        drop(notifier);

        let result = completion.wait();
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains("unexpectedly"));
    }

    #[tokio::test]
    async fn dataflow_completion_await_async() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("await_test");
        builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .output("out");
        let dataflow = builder.build().unwrap();

        // Exercise the async completion path: .await on DataflowCompletion
        let completion = rt.run(dataflow).unwrap();
        completion.await.unwrap();
    }

    #[test]
    fn spawned_dataflow_drop_without_join_cancels() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("drop_cancel");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        // Drop without calling join() — should cancel and not hang
        let _handle = rt.spawn(dataflow).unwrap();
        // handle dropped here — cancellation + detach, no blocking
    }

    #[cfg(feature = "async-io")]
    #[tokio::test]
    async fn spawn_async_roundtrip() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("async_roundtrip");
        let input = builder.input::<i32>("data");
        input.map("mul10", |_t, x| x * 10).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn_async(dataflow).unwrap();
        let sender = handle.take_async_input::<i32>("data").unwrap();
        let mut receiver = handle.take_async_output::<i32>("out").unwrap();

        sender.send(0, vec![1, 2, 3]).await.unwrap();
        sender.advance_frontier(0).await.unwrap();
        sender.close();

        let results = receiver.collect_data().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        let mut vals = results[0].1.clone();
        vals.sort();
        assert_eq!(vals, vec![10, 20, 30]);

        handle.join().await.unwrap();
    }

    #[cfg(feature = "async-io")]
    #[tokio::test]
    async fn sync_take_on_async_port_gives_helpful_error() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("cross_mode_err");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn_async(dataflow).unwrap();

        // Using sync take_input on an async-wired port should give a helpful error
        let err = handle.take_input::<i32>("data").unwrap_err();
        assert!(
            format!("{err}").contains("spawn_async"),
            "error should hint at async mode: {err}"
        );

        let err = handle.take_output::<i32>("out");
        assert!(err.is_err());
        let msg = format!("{}", err.err().unwrap());
        assert!(
            msg.contains("spawn_async"),
            "error should hint at async mode: {msg}"
        );
    }

    #[cfg(feature = "async-io")]
    #[tokio::test]
    async fn spawn_async_multiple_timestamps() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("async_multi_ts");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn_async(dataflow).unwrap();
        let sender = handle.take_async_input::<i32>("data").unwrap();
        let mut receiver = handle.take_async_output::<i32>("out").unwrap();

        sender.send(0, vec![10, 20]).await.unwrap();
        sender.send(1, vec![30, 40]).await.unwrap();
        sender.advance_frontier(1).await.unwrap();
        sender.close();

        let results = receiver.collect_data().await;
        assert!(results.len() >= 1); // may be batched
        let all_data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        let mut sorted = all_data;
        sorted.sort();
        assert_eq!(sorted, vec![10, 20, 30, 40]);

        handle.join().await.unwrap();
    }

    #[cfg(feature = "async-io")]
    #[tokio::test]
    async fn async_input_sender_is_clone() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("clone_sender");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn_async(dataflow).unwrap();
        let sender1 = handle.take_async_input::<i32>("data").unwrap();
        let sender2 = sender1.clone();

        // Both clones can send data
        sender1.send(0, vec![1]).await.unwrap();
        sender2.send(0, vec![2]).await.unwrap();
        sender1.advance_frontier(0).await.unwrap();
        drop(sender1);
        drop(sender2); // channel closes when all clones drop

        let mut receiver = handle.take_async_output::<i32>("out").unwrap();
        let results = receiver.collect_data().await;
        let all_data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        let mut sorted = all_data;
        sorted.sort();
        assert_eq!(sorted, vec![1, 2]);

        handle.join().await.unwrap();
    }
}
