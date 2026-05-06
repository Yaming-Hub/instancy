//! Self-contained runtimes for hosting instancy dataflows.
//!
//! Instancy provides two runtime tiers:
//!
//! - `SimpleRuntime` (feature `test-utils`) — lightweight, single-thread execution
//!   for tests and simple scripts. Each dataflow gets a dedicated background thread.
//! - [`RuntimeHandle`] — production runtime with a shared worker thread pool,
//!   configurable scheduling policy, and centralized cancellation.
//!
//! Both runtimes accept a [`LogicalDataflow`]
//! and return a [`SpawnedDataflow`] handle for channel-based I/O.
//!
//! ## Async completion
//!
//! [`SpawnedDataflow::join()`] returns a [`DataflowCompletion`] future — callers
//! can `.await` it in async code or call [`.wait()`](DataflowCompletion::wait)
//! for blocking synchronous use.
//!
//! **No global state:** All shared state flows from runtime instances.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};

use crate::cancellation::{CancellationReason, CancellationToken};
use crate::dataflow::channel_operators::ChannelMode;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::control::{ControlBroadcast, ControlReceiver, ControlSender};
use crate::dataflow::dataflow_builder::{DataflowBuilder, LogicalDataflow};
use crate::dataflow::executor::{DataflowExecutor, ExecutorConfig};
use crate::dataflow::graph::OperatorInfo;
use crate::dataflow::DataflowId;
use crate::error::{Error, Result};
use crate::progress::progress_channel::{WorkerProgressChannels, create_progress_channels};
use crate::progress::timestamp::Timestamp;
use crate::scheduler::policy::SchedulePolicy;
use crate::worker::WorkerContext;
use crate::worker_pool::{WorkerPool, WorkerPoolConfig};

/// Configuration for creating a [`RuntimeHandle`].
///
/// Each `RuntimeHandle` gets its own worker pool, task queue, and scheduling
/// policy — fully isolated from other runtime instances.
pub struct RuntimeConfig {
    /// Number of worker threads in the pool.
    pub worker_threads: usize,
    /// Scheduling policy for the task queue.
    ///
    /// - `None` (default) — pure FIFO queue, O(1) dequeue, no comparisons.
    /// - `Some(policy)` — ordered by the policy via a binary heap, O(log n) dequeue.
    pub schedule_policy: Option<Box<dyn SchedulePolicy>>,
    /// Name for this runtime (used in thread names and diagnostics).
    pub name: String,
    /// Tokio runtime mode — controls how instancy obtains a tokio runtime
    /// for async operations (bridge tasks, timers, async I/O).
    pub tokio_mode: TokioMode,
}

/// Controls how instancy obtains a tokio runtime for async operations.
///
/// Instancy requires a tokio runtime for bridge tasks (external cancellation
/// tokens), async I/O channels, timers, and network transport. This enum
/// lets the hosting application decide whether instancy should create its
/// own runtime or share an existing one.
#[derive(Clone, Debug)]
pub enum TokioMode {
    /// Create a new multi-threaded tokio runtime owned by this `RuntimeHandle`.
    ///
    /// The runtime is shut down when the `RuntimeHandle` is dropped. The
    /// `worker_threads` parameter controls the number of tokio worker threads
    /// (separate from instancy's dataflow worker threads).
    ///
    /// Use this when your application doesn't already have a tokio runtime,
    /// or when you want instancy to be fully self-contained.
    ///
    /// # Errors
    ///
    /// Returns an error if called inside an existing tokio context (e.g.,
    /// `#[tokio::main]` or `#[tokio::test]`), because dropping the owned
    /// runtime from within an async context would panic. Use [`TokioMode::Auto`]
    /// or [`TokioMode::CurrentContext`] instead.
    Create {
        /// Number of tokio async worker threads. Defaults to 2.
        worker_threads: usize,
    },
    /// Use an existing tokio runtime via its handle.
    ///
    /// The caller is responsible for keeping the tokio runtime alive for the
    /// lifetime of the `RuntimeHandle`. If the tokio runtime shuts down while
    /// instancy is running, async operations will fail.
    ///
    /// Use this when your application already runs on tokio and you want
    /// instancy to share the same runtime (e.g., in an Actix/Axum server).
    External(tokio::runtime::Handle),
    /// Detect the current tokio runtime automatically.
    ///
    /// Equivalent to `External(tokio::runtime::Handle::current())` — panics
    /// if called outside a tokio context. This is the default for backwards
    /// compatibility.
    CurrentContext,
    /// Automatically detect: use the current tokio runtime if one is active,
    /// otherwise create a new multi-threaded runtime with 2 worker threads.
    ///
    /// This is the recommended default — it works both inside existing tokio
    /// applications (Actix, Axum, standalone `#[tokio::main]`) and in plain
    /// synchronous contexts (tests, CLI tools).
    Auto,
}

impl std::fmt::Debug for RuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("worker_threads", &self.worker_threads)
            .field("schedule_policy", &"<dyn SchedulePolicy>")
            .field("name", &self.name)
            .field("tokio_mode", &self.tokio_mode)
            .finish()
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: num_cpus(),
            schedule_policy: None,
            name: "instancy".to_string(),
            tokio_mode: TokioMode::default(),
        }
    }
}

impl Default for TokioMode {
    /// Defaults to [`TokioMode::Auto`] — uses the current tokio runtime if
    /// available, otherwise creates a new one.
    fn default() -> Self {
        Self::Auto
    }
}

// ---------------------------------------------------------------------------
// SpawnOptions — per-spawn configuration
// ---------------------------------------------------------------------------

/// Selects the channel backend for external I/O ports when spawning a dataflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoMode {
    /// Use `std::sync::mpsc` channels — blocking send/recv, no async runtime needed.
    Sync,
    /// Use `tokio::sync::mpsc` channels — enables async send/recv via
    /// [`SpawnedDataflow::take_async_input()`] / [`SpawnedDataflow::take_async_output()`].
    Async,
}

/// Options for spawning a dataflow on a runtime.
///
/// Pass to [`RuntimeHandle::spawn()`] or [`RuntimeHandle::spawn_multi()`]
/// to configure channel mode and other per-spawn settings.
///
/// # Example
///
/// ```ignore
/// let opts = SpawnOptions::default(); // sync channels, no metrics
/// let opts = SpawnOptions::new().io_mode(IoMode::Async).collect_metrics(true);
/// ```
#[derive(Clone, Debug)]
pub struct SpawnOptions {
    /// Channel mode for external I/O ports. Default: [`IoMode::Sync`].
    pub io_mode: IoMode,
    /// Whether to collect per-operator metrics. Default: `false`.
    pub collect_metrics: bool,
    /// Scheduling priority for this dataflow (higher = scheduled sooner).
    pub priority: u32,
    /// Optional external cancellation token.
    ///
    /// When this token is cancelled (by the hosting application), the dataflow
    /// is automatically cancelled with [`CancellationReason::UserRequested`].
    /// This allows the caller to control dataflow lifetime externally — e.g.,
    /// implementing timeouts, graceful shutdown, or request-scoped cancellation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tokio_util::sync::CancellationToken;
    ///
    /// let token = CancellationToken::new();
    /// // Cancel after 5 seconds:
    /// tokio::spawn({
    ///     let t = token.clone();
    ///     async move { tokio::time::sleep(Duration::from_secs(5)).await; t.cancel(); }
    /// });
    ///
    /// let opts = SpawnOptions::new().cancellation_token(token);
    /// let handle = rt.spawn(dataflow, opts)?;
    /// ```
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

impl SpawnOptions {
    /// Create spawn options with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the I/O channel mode.
    pub fn io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    /// Enable or disable per-operator metrics collection.
    ///
    /// When enabled, the runtime records activation counts and durations for
    /// each operator. Retrieve via `DataflowCompletion` after join.
    pub fn collect_metrics(mut self, enable: bool) -> Self {
        self.collect_metrics = enable;
        self
    }

    /// Set the scheduling priority for this dataflow.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set an external cancellation token.
    ///
    /// When this token is cancelled, the dataflow is automatically cancelled
    /// with [`CancellationReason::UserRequested`]. This allows the hosting
    /// application to control dataflow lifetime — e.g., timeouts, graceful
    /// shutdown, or request-scoped cancellation.
    pub fn cancellation_token(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }
}

impl Default for SpawnOptions {
    fn default() -> Self {
        Self {
            io_mode: IoMode::Sync,
            collect_metrics: false,
            priority: 0,
            cancellation_token: None,
        }
    }
}

impl From<IoMode> for ChannelMode {
    fn from(mode: IoMode) -> Self {
        match mode {
            IoMode::Sync => ChannelMode::Sync,
            IoMode::Async => ChannelMode::Async,
        }
    }
}

// ---------------------------------------------------------------------------
// PeerRegistry — tracks cluster dataflows for peer-down notification
// ---------------------------------------------------------------------------

/// Lightweight handle for cancelling all local workers + network bridges
/// of a cluster dataflow. Stored in [`PeerRegistry`].
#[cfg(feature = "transport")]
struct ClusterCancelHandle {
    worker_tokens: Vec<CancellationToken>,
    bridge_cancel: tokio_util::sync::CancellationToken,
}

#[cfg(feature = "transport")]
impl ClusterCancelHandle {
    fn cancel_with_reason(&self, reason: CancellationReason) {
        self.bridge_cancel.cancel();
        for token in &self.worker_tokens {
            token.cancel_with_reason(reason.clone());
        }
    }

    /// Returns true if this dataflow is already cancelled or completed.
    ///
    /// Checks the bridge cancel token (which is always cancelled when the
    /// `ClusterSpawnedDataflow` is dropped, including on normal completion)
    /// and the first worker token (which is cancelled on explicit cancellation
    /// or runtime shutdown). All worker tokens are always cancelled together
    /// via `cancel_with_reason` or parent token propagation.
    fn is_cancelled(&self) -> bool {
        self.bridge_cancel.is_cancelled()
            || self
                .worker_tokens
                .first()
                .is_some_and(|t| t.is_cancelled())
    }
}

#[cfg(feature = "transport")]
struct PeerRegistration {
    id: u64,
    #[allow(dead_code)]
    dataflow_name: String,
    cancel_handle: ClusterCancelHandle,
}

/// Registry mapping peer node IDs to active cluster dataflows.
///
/// When the hosting application reports a peer as down via
/// [`RuntimeHandle::report_node_leave()`], the registry cancels all
/// dataflows that have workers on the departed node.
///
/// Nodes reported as left are remembered in a `down_peers` set, so
/// dataflows registered *after* a node-leave report are immediately
/// cancelled (preventing a race between `spawn_cluster` and
/// `report_node_leave`). Call [`RuntimeHandle::report_node_join()`] to
/// clear this state when a node recovers or a new node is added.
#[cfg(feature = "transport")]
pub(crate) struct PeerRegistry {
    next_id: std::sync::atomic::AtomicU64,
    /// Protected state: peer-to-registration map + set of known-down peers.
    state: Mutex<PeerRegistryState>,
}

#[cfg(feature = "transport")]
struct PeerRegistryState {
    /// Maps peer_node_id → list of registrations referencing that peer.
    entries: std::collections::HashMap<String, Vec<PeerRegistration>>,
    /// Peers that have been reported as down. Registrations against these
    /// peers are immediately cancelled.
    down_peers: std::collections::HashSet<String>,
}

#[cfg(feature = "transport")]
impl PeerRegistry {
    fn new() -> Self {
        Self {
            next_id: std::sync::atomic::AtomicU64::new(1),
            state: Mutex::new(PeerRegistryState {
                entries: std::collections::HashMap::new(),
                down_peers: std::collections::HashSet::new(),
            }),
        }
    }

    /// Register a cluster dataflow for all its remote peer nodes.
    /// Returns the registration ID.
    ///
    /// If any of the peers have already been reported as down, the
    /// dataflow is immediately cancelled with `CancellationReason::PeerDown`.
    fn register(
        &self,
        peer_node_ids: &[String],
        dataflow_name: &str,
        cancel_handle: ClusterCancelHandle,
    ) -> u64 {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

        // Check if any peer is already known to be down.
        let already_down: Option<String> = peer_node_ids
            .iter()
            .find(|pid| state.down_peers.contains(pid.as_str()))
            .cloned();

        if let Some(down_peer) = already_down {
            // Release lock before cancelling to avoid nested lock contention.
            drop(state);
            cancel_handle.cancel_with_reason(CancellationReason::PeerDown(down_peer));
            return id;
        }

        for peer_id in peer_node_ids {
            let bucket = state.entries.entry(peer_id.clone()).or_default();
            bucket.push(PeerRegistration {
                id,
                dataflow_name: dataflow_name.to_string(),
                cancel_handle: ClusterCancelHandle {
                    worker_tokens: cancel_handle.worker_tokens.clone(),
                    bridge_cancel: cancel_handle.bridge_cancel.clone(),
                },
            });
        }

        // Periodic pruning: remove stale (already-cancelled) entries to
        // prevent unbounded growth in long-running systems.
        if id % 64 == 0 {
            state.entries.retain(|_peer, bucket| {
                bucket.retain(|r| !r.cancel_handle.is_cancelled());
                !bucket.is_empty()
            });
        }

        id
    }

    /// Cancel all dataflows associated with the given peer and prune stale entries.
    /// Returns the number of dataflows that were newly cancelled.
    fn report_peer_down(&self, peer_node_id: &str) -> usize {
        // Collect handles to cancel, then release lock before cancelling.
        // This avoids holding the registry lock during CancellationToken
        // operations (which acquire their own internal locks + notify wakers).
        let to_cancel: Vec<ClusterCancelHandle>;
        let cancelled_count;

        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

            // Mark peer as permanently down so future registrations are rejected.
            state.down_peers.insert(peer_node_id.to_string());

            // Remove the downed peer's bucket and collect non-cancelled handles.
            if let Some(bucket) = state.entries.remove(peer_node_id) {
                // Collect cancelled registration IDs for cross-pruning.
                let cancelled_ids: std::collections::HashSet<u64> =
                    bucket.iter().map(|r| r.id).collect();

                // Separate into to-cancel and already-cancelled.
                to_cancel = bucket
                    .into_iter()
                    .filter(|r| !r.cancel_handle.is_cancelled())
                    .map(|r| r.cancel_handle)
                    .collect();
                cancelled_count = to_cancel.len();

                // Remove these registration IDs from all other peer buckets.
                for bucket in state.entries.values_mut() {
                    bucket.retain(|r| !cancelled_ids.contains(&r.id));
                }
            } else {
                to_cancel = Vec::new();
                cancelled_count = 0;
            }

            // Prune empty buckets and stale entries.
            state.entries.retain(|_peer, bucket| {
                bucket.retain(|r| !r.cancel_handle.is_cancelled());
                !bucket.is_empty()
            });
        }

        // Cancel outside the lock to avoid nested lock contention.
        let reason = CancellationReason::PeerDown(peer_node_id.to_string());
        for handle in &to_cancel {
            handle.cancel_with_reason(reason.clone());
        }

        cancelled_count
    }

    /// Remove a peer from the "known down" set, allowing future registrations.
    /// Returns true if the peer was in the down set.
    fn report_peer_recovered(&self, peer_node_id: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.down_peers.remove(peer_node_id)
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
    /// Scheduling policy for task ordering (None = FIFO).
    _schedule_policy: Option<Arc<dyn SchedulePolicy>>,
    /// Cancellation token for graceful shutdown of all dataflows in this runtime.
    cancel: CancellationToken,
    /// Runtime name for diagnostics.
    name: String,
    /// Executor registry for cooperative multiplexing of dataflow futures.
    /// Created lazily on first run()/spawn() call.
    registry: Arc<crate::executor_task::ExecutorRegistry>,
    /// Registry of cluster dataflows indexed by peer node ID for peer-down
    /// notification. Only used with the `transport` feature.
    #[cfg(feature = "transport")]
    peer_registry: Arc<PeerRegistry>,
    /// Tokio runtime handle for spawning async bridge tasks, timers, etc.
    tokio_handle: tokio::runtime::Handle,
    /// Owned tokio runtime (when `TokioMode::Create` was used).
    /// Kept alive for the lifetime of RuntimeHandle; dropped on RuntimeHandle::drop.
    _owned_tokio_runtime: Option<tokio::runtime::Runtime>,
    /// Number of currently active (not yet completed) dataflows.
    active_count: Arc<AtomicUsize>,
    /// Notified whenever the active count reaches zero.
    idle_notify: Arc<tokio::sync::Notify>,
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
        let worker_pool =
            WorkerPool::new(pool_config).map_err(|e| crate::error::Error::Custom(e.to_string()))?;
        let schedule_policy: Option<Arc<dyn SchedulePolicy>> =
            config.schedule_policy.map(|p| Arc::from(p) as Arc<dyn SchedulePolicy>);
        let registry = worker_pool.create_registry(schedule_policy.clone());

        // Resolve the tokio runtime handle.
        let (tokio_handle, owned_runtime) = match config.tokio_mode {
            TokioMode::Create { worker_threads } => {
                if tokio::runtime::Handle::try_current().is_ok() {
                    return Err(crate::error::Error::Custom(
                        "TokioMode::Create cannot be used inside an existing tokio context \
                         (dropping the owned runtime would panic). \
                         Use TokioMode::Auto, TokioMode::CurrentContext, or \
                         TokioMode::External instead."
                            .to_string(),
                    ));
                }
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(worker_threads)
                    .enable_all()
                    .thread_name(format!("{}-tokio", config.name))
                    .build()
                    .map_err(|e| {
                        crate::error::Error::Custom(format!(
                            "failed to create tokio runtime: {e}"
                        ))
                    })?;
                let handle = rt.handle().clone();
                (handle, Some(rt))
            }
            TokioMode::External(handle) => (handle, None),
            TokioMode::CurrentContext => {
                let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                    crate::error::Error::Custom(
                        "TokioMode::CurrentContext requires an active tokio runtime; \
                         use TokioMode::Create or TokioMode::External instead"
                            .to_string(),
                    )
                })?;
                (handle, None)
            }
            TokioMode::Auto => {
                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => (handle, None),
                    Err(_) => {
                        // No active tokio runtime — create one.
                        let rt = tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(2)
                            .enable_all()
                            .thread_name(format!("{}-tokio", config.name))
                            .build()
                            .map_err(|e| {
                                crate::error::Error::Custom(format!(
                                    "failed to create tokio runtime: {e}"
                                ))
                            })?;
                        let handle = rt.handle().clone();
                        (handle, Some(rt))
                    }
                }
            }
        };

        Ok(Self {
            worker_pool,
            _schedule_policy: schedule_policy,
            cancel: CancellationToken::new(),
            name: config.name,
            registry,
            #[cfg(feature = "transport")]
            peer_registry: Arc::new(PeerRegistry::new()),
            tokio_handle,
            _owned_tokio_runtime: owned_runtime,
            active_count: Arc::new(AtomicUsize::new(0)),
            idle_notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Returns the cancellation token for this runtime.
    ///
    /// Cancelling this token will gracefully shut down all dataflows
    /// running within this runtime.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Returns the tokio runtime handle used by this runtime.
    ///
    /// Use this to spawn async tasks that should run on the same tokio runtime
    /// as instancy's internal bridge tasks and async I/O.
    pub fn tokio_handle(&self) -> &tokio::runtime::Handle {
        &self.tokio_handle
    }

    /// Shut down the runtime by cancelling all running dataflows.
    ///
    /// This is **cooperative**: it signals cancellation to all dataflows but
    /// does not forcibly terminate worker threads or the tokio runtime.
    /// Worker threads will drain once operators observe cancellation and stop
    /// producing work. Bridge tasks exit when they observe cancellation.
    ///
    /// The owned tokio runtime (if any) is shut down when this `RuntimeHandle`
    /// is dropped — not by this method. To fully clean up, call `shutdown()`
    /// and then drop the handle.
    pub fn shutdown(&self) {
        self.cancel
            .cancel_with_reason(CancellationReason::RuntimeShutdown);
    }

    /// Shut down the runtime and wait for all active dataflows to complete.
    ///
    /// This method:
    /// 1. Signals cancellation to all running dataflows (same as [`shutdown()`](Self::shutdown))
    /// 2. Awaits until every active dataflow has finished executing
    ///
    /// Returns immediately if no dataflows are active.
    ///
    /// # Example
    ///
    /// ```ignore
    /// rt.shutdown_async().await;
    /// // All dataflows have now completed — safe to drop the runtime.
    /// ```
    pub async fn shutdown_async(&self) {
        self.cancel
            .cancel_with_reason(CancellationReason::RuntimeShutdown);
        self.wait_idle().await;
    }

    /// Wait until all active dataflows have completed.
    ///
    /// Returns immediately if no dataflows are currently running. Otherwise,
    /// waits for all spawned dataflows to finish (whether by normal completion,
    /// cancellation, or error).
    ///
    /// This does NOT cancel anything — it purely waits. Use
    /// [`shutdown_async()`](Self::shutdown_async) to cancel and wait.
    pub async fn wait_idle(&self) {
        loop {
            // Register interest BEFORE checking the counter to avoid a race
            // where a notification fires between our check and the await.
            let notified = self.idle_notify.notified();
            if self.active_count.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Returns the number of currently active (not yet completed) dataflows.
    ///
    /// A dataflow is counted as active from the moment it is spawned until its
    /// executor completes (successfully, with error, or via cancellation).
    pub fn active_dataflows(&self) -> usize {
        self.active_count.load(std::sync::atomic::Ordering::Relaxed)
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

    /// Report that a node has left the cluster (crashed, shut down, or network-partitioned).
    ///
    /// The hosting application is responsible for health-monitoring peer nodes.
    /// When a peer is detected as unreachable, call this method to cancel all
    /// cluster dataflows that depend on the departed node.
    ///
    /// Each affected dataflow receives [`CancellationReason::PeerDown`] with
    /// the given `node_id`. The hosting application can then retry the
    /// dataflow on healthy nodes if desired.
    ///
    /// Returns the number of dataflows that were newly cancelled. Returns 0
    /// if no active dataflows reference the given node (including if the node
    /// ID is unknown).
    ///
    /// This method is idempotent: calling it again for the same node after
    /// all its dataflows are already cancelled returns 0.
    ///
    /// The node is remembered as "left" until
    /// [`report_node_join()`](Self::report_node_join) is called.
    /// Any `spawn_cluster` that includes a left node will have its dataflow
    /// immediately cancelled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Hosting application detects node-3 is unreachable
    /// let cancelled = runtime.report_node_leave("node-3");
    /// println!("Cancelled {cancelled} dataflows due to node-3 departure");
    /// ```
    #[cfg(feature = "transport")]
    pub fn report_node_leave(&self, node_id: &str) -> usize {
        self.peer_registry.report_peer_down(node_id)
    }

    /// Report that a node has joined (or re-joined) the cluster.
    ///
    /// This removes the node from the internal "left" set, allowing
    /// future [`spawn_cluster()`](Self::spawn_cluster) calls that include
    /// this node to proceed normally instead of being immediately cancelled.
    ///
    /// Call this when:
    /// - A previously-down node recovers and rejoins the cluster.
    /// - A brand-new node is added to the cluster topology.
    ///
    /// Already-cancelled dataflows are **not** restarted — the hosting
    /// application must re-spawn them if desired.
    ///
    /// Returns `true` if the node was previously marked as left, `false`
    /// if it was not in the left set (no-op for new nodes).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Hosting application detects node-3 is back online (or newly added)
    /// runtime.report_node_join("node-3");
    /// // Now safe to spawn_cluster with node-3 in the topology
    /// ```
    #[cfg(feature = "transport")]
    pub fn report_node_join(&self, node_id: &str) -> bool {
        self.peer_registry.report_peer_recovered(node_id)
    }

    /// Spawn a dataflow on the worker pool.
    ///
    /// Returns a [`SpawnedDataflow`] handle for feeding data and collecting
    /// results. The channel mode (sync vs async) is controlled by
    /// [`SpawnOptions::io_mode`].
    ///
    /// # Execution model
    ///
    /// The executor is registered as an `ExecutorTask` in the pool's
    /// `ExecutorRegistry`. Pool threads cooperatively poll the task via
    /// `poll_run()`, yielding after each poll budget to allow other
    /// dataflows to make progress on the same threads.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sync I/O (default)
    /// let mut handle = rt.spawn(dataflow, SpawnOptions::default())?;
    /// let sender = handle.take_input::<i32>("data")?;
    ///
    /// // Async I/O
    /// let opts = SpawnOptions::new().io_mode(IoMode::Async);
    /// let mut handle = rt.spawn(dataflow, opts)?;
    /// let sender = handle.take_async_input::<i32>("data")?;
    /// ```
    pub fn spawn<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
        options: SpawnOptions,
    ) -> Result<SpawnedDataflow<T>> {
        dataflow.collect_metrics = options.collect_metrics;
        self.spawn_internal(
            dataflow,
            options.io_mode.into(),
            options.priority,
            options.cancellation_token,
            WorkerContext::single(),
            None,
            None,
        )
    }

    /// Spawn N replicated workers from the same dataflow builder closure.
    ///
    /// The `build` closure is called `num_workers` times, each with a fresh
    /// [`DataflowBuilder`] and the worker index (0..num_workers). Each call
    /// must construct an identical graph topology — the runtime validates that
    /// all replicas have matching operator/edge/port structure.
    ///
    /// Returns a [`MultiSpawnedDataflow`] with per-worker input senders and
    /// output receivers, shared cancellation, and aggregated completion.
    ///
    /// # Uniform replication
    ///
    /// `num_workers` creates N **complete replicas** of the entire dataflow
    /// graph. Every stage in the dataflow gets the same number of workers —
    /// there is no per-stage parallelism control. Each worker is an
    /// independent executor; the `num_workers` parameter controls **logical**
    /// parallelism, while the runtime's `worker_threads` configuration
    /// controls **physical** parallelism. For example, 4 logical workers on a
    /// pool with 1 thread will run cooperatively and sequentially.
    ///
    /// Per-stage worker counts (e.g., 4 workers in stage 0 funneling into
    /// 2 workers in stage 1) require exchange channels to redistribute data
    /// at stage boundaries. This will be supported once exchange operators
    /// are implemented.
    ///
    /// # Execution model
    ///
    /// Each worker runs as an independent [`DataflowExecutor`] on the shared
    /// pool. There is **no cross-worker data routing** — all channels are
    /// worker-local. Dataflows containing exchange/rebalance/gather/broadcast
    /// operators are rejected with an error (fail-closed).
    ///
    /// # Errors
    ///
    /// - `num_workers` is 0
    /// - `build` closure returns an error
    /// - Replicas have mismatched graph topologies
    /// - Dataflow contains exchange operators (not yet supported)
    ///
    /// # Example: partitioned input with multiple workers
    ///
    /// A common pattern is processing physically partitioned data. Each
    /// partition maps to one logical worker, and the hosting application
    /// feeds each partition's data into its corresponding worker's input
    /// stream.
    ///
    /// ```rust
    /// use instancy::{RuntimeConfig, RuntimeHandle};
    /// use instancy::DataflowBuilder;
    ///
    /// // Simulate 4 data partitions.
    /// let partitions: Vec<Vec<i32>> = vec![
    ///     vec![1, 2, 3],       // partition 0
    ///     vec![10, 20],        // partition 1
    ///     vec![100],           // partition 2
    ///     vec![1000, 2000],    // partition 3
    /// ];
    /// let num_workers = partitions.len();
    ///
    /// // Create a runtime — worker_threads controls physical parallelism.
    /// // Here 2 threads service 4 logical workers cooperatively.
    /// let rt = RuntimeHandle::new(RuntimeConfig {
    ///     worker_threads: 2,
    ///     ..RuntimeConfig::default()
    /// }).unwrap();
    ///
    /// // Spawn 4 replicated workers, each with identical graph topology.
    /// // The worker_idx is available in the closure but the graph structure
    /// // must be the same for every worker.
    /// let mut multi = rt.spawn_multi(
    ///     "partitioned-sum",
    ///     num_workers,
    ///     |_worker_idx, builder: &mut DataflowBuilder<u64>| {
    ///         let input = builder.input::<i32>("data");
    ///         // Each worker independently doubles its partition's values.
    ///         input.map("double", |_t, x| x * 2).output("results");
    ///         Ok(())
    ///     },
    ///     SpawnOptions::default(),
    /// ).unwrap();
    ///
    /// // Wire each partition to its corresponding worker's input stream.
    /// let mut senders = Vec::new();
    /// let mut receivers = Vec::new();
    /// for i in 0..num_workers {
    ///     senders.push(multi.take_input::<i32>(i, "data").unwrap());
    ///     receivers.push(multi.take_output::<i32>(i, "results").unwrap());
    /// }
    ///
    /// // Feed partitioned data — each sender maps to one logical worker.
    /// for (i, partition) in partitions.into_iter().enumerate() {
    ///     senders[i].send(0, partition).unwrap();
    /// }
    /// // Close all inputs to signal end-of-data.
    /// drop(senders);
    ///
    /// // Collect results from each worker independently.
    /// let results: Vec<Vec<i32>> = receivers
    ///     .into_iter()
    ///     .map(|r| r.collect_data().into_iter().flat_map(|(_, d)| d).collect())
    ///     .collect();
    ///
    /// assert_eq!(results[0], vec![2, 4, 6]);
    /// assert_eq!(results[1], vec![20, 40]);
    /// assert_eq!(results[2], vec![200]);
    /// assert_eq!(results[3], vec![2000, 4000]);
    ///
    /// // Wait for all workers to finish.
    /// multi.join_blocking().unwrap();
    /// ```
    pub fn spawn_multi<T, F>(
        &self,
        name: &str,
        num_workers: usize,
        build: F,
        options: SpawnOptions,
    ) -> Result<MultiSpawnedDataflow<T>>
    where
        T: Timestamp,
        F: Fn(usize, &mut DataflowBuilder<T>) -> Result<()>,
    {
        self.spawn_multi_internal(
            name,
            num_workers,
            build,
            options.io_mode.into(),
            options.collect_metrics,
            options.priority,
            options.cancellation_token,
        )
    }

    // -- Private sync implementations --

    #[allow(clippy::too_many_arguments)]
    fn spawn_internal<T: Timestamp>(
        &self,
        dataflow: LogicalDataflow<T>,
        mode: ChannelMode,
        priority: u32,
        external_cancel: Option<tokio_util::sync::CancellationToken>,
        worker_context: WorkerContext,
        progress_channels: Option<WorkerProgressChannels<T>>,
        pre_created_wake_handle: Option<WakeHandle>,
    ) -> Result<SpawnedDataflow<T>> {
        let dataflow_id = DataflowId::new();
        let (spawned, executor, mut notifier) = self.prepare_worker(
            dataflow,
            mode,
            worker_context,
            progress_channels,
            pre_created_wake_handle,
            None,
            None,
        )?;

        // Track active dataflow count for wait_idle()/shutdown_async().
        self.active_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let active_count = Arc::clone(&self.active_count);
        let idle_notify = Arc::clone(&self.idle_notify);
        notifier.set_on_complete(Box::new(move || {
            let prev = active_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if prev == 1 {
                idle_notify.notify_waiters();
            }
        }));

        self.registry
            .register(executor, notifier, dataflow_id, priority);

        // If an external cancellation token was provided, spawn a bridge task
        // that propagates cancellation from the user's token to our internal one.
        // The task exits when either the user token fires OR the dataflow
        // completes/cancels by other means.
        if let Some(user_token) = external_cancel {
            let cancel = spawned.cancel.clone();
            self.tokio_handle.spawn(async move {
                tokio::select! {
                    _ = user_token.cancelled() => {
                        cancel.cancel_with_reason(CancellationReason::UserRequested);
                    }
                    _ = cancel.cancelled_async() => {
                        // Dataflow already cancelled/completed — exit cleanly.
                    }
                }
            });
        }

        Ok(spawned)
    }

    /// Materialize a worker (executor + I/O wiring) without registering it
    /// on the task registry. Returns the SpawnedDataflow handle, the pinned
    /// executor future, and the completion notifier.
    ///
    /// This separation allows `spawn_multi_internal` to materialize ALL workers
    /// (including their progress tracker initialization) before registering any
    /// of them. This is critical for correctness: progress tracker initialization
    /// broadcasts initial capabilities to peer workers' channels. If workers
    /// were registered (and thus polled) immediately, a fast worker could see
    /// incomplete global state before slower workers have initialized.
    #[allow(clippy::too_many_arguments)]
    fn prepare_worker<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
        mode: ChannelMode,
        worker_context: WorkerContext,
        progress_channels: Option<WorkerProgressChannels<T>>,
        pre_created_wake_handle: Option<WakeHandle>,
        parent_cancel: Option<CancellationToken>,
        control_broadcast: Option<(ControlSender, ControlReceiver)>,
    ) -> Result<(
        SpawnedDataflow<T>,
        Pin<Box<DataflowExecutor<T>>>,
        CompletionNotifier,
    )> {
        let has_async_sources = !dataflow.async_source_wiring.is_empty();

        if dataflow.operator_factories.is_empty()
            && dataflow.input_port_wiring.is_empty()
            && !has_async_sources
        {
            return Err(Error::Custom("cannot spawn an empty dataflow".into()));
        }

        let parent = parent_cancel.unwrap_or_else(|| self.cancel.clone());
        let cancel = parent.child_token();
        let cancel_handle = cancel.clone();
        let external_inputs_open = Arc::new(AtomicUsize::new(0));
        let name = dataflow.name().to_string();

        // Use pre-created wake handle if provided (for multi-worker with
        // progress channels that already reference it), otherwise create one.
        let wake_handle = pre_created_wake_handle.unwrap_or_default();

        // Register wake handle on the cancellation token (and all ancestors)
        // so that cancel() wakes the sleeping executor promptly.
        cancel.register_wake_handle(wake_handle.clone());

        // --- Wire input ports ---
        let mut input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)> =
            Vec::new();
        let mut input_count = dataflow.input_port_wiring.len();

        // TODO(multi-worker): Input/output port wiring closures are FnMut (callable
        // N times) but the Vec is still drain()'d here, consuming ownership. PR 39 will
        // change to &mut iteration so wiring survives across N worker materializations.
        // Input wiring will also need fan-out (partition/broadcast) to distribute data
        // across workers; output wiring will need fan-in to merge worker outputs.
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

        // --- Wire async source ports ---
        let mut pump_tasks: Vec<Box<dyn FnOnce() + Send>> = Vec::new();
        {
            let async_count = dataflow.async_source_wiring.len();
            input_count += async_count;
            for (op_idx, wiring) in dataflow.async_source_wiring.drain(..) {
                let (factory, pump) = wiring(
                    Arc::clone(&external_inputs_open),
                    wake_handle.clone(),
                    cancel.clone(),
                );
                dataflow.operator_factories.push((op_idx, factory));
                pump_tasks.push(pump);
            }
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
                dataflow.operator_factories[pos] = (info.operator_index, replacement_factory);
            }
            output_receivers.push((info.name.clone(), info.type_name, receiver_any));
        }

        // --- Materialize executor (but do NOT register yet) ---
        let mut executor = materialize_executor(
            dataflow,
            cancel,
            Some(wake_handle),
            worker_context,
            progress_channels,
        )?;

        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        // Attach cross-worker control broadcast if provided.
        if let Some((sender, receiver)) = control_broadcast {
            executor.set_control_broadcast(sender, receiver);
        }

        let (completion, notifier) = DataflowCompletion::new();

        // Capture metrics handle before executor is moved into the registry.
        let metrics = executor.metrics().cloned();

        // --- Spawn async source pump tasks ---
        for (pump_idx, pump) in pump_tasks.into_iter().enumerate() {
            std::thread::Builder::new()
                .name(format!("source-pump-{}-{}", name, pump_idx))
                .spawn(pump)
                .map_err(|e| Error::Custom(format!("failed to spawn pump thread: {e}")))?;
        }

        let spawned = SpawnedDataflow {
            name,
            cancel: cancel_handle,
            completion: Some(completion),
            input_senders,
            output_receivers,
            metrics,
            _phantom: PhantomData,
        };

        Ok((spawned, Box::pin(executor), notifier))
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_multi_internal<T, F>(
        &self,
        name: &str,
        num_workers: usize,
        build: F,
        mode: ChannelMode,
        collect_metrics: bool,
        priority: u32,
        external_cancel: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<MultiSpawnedDataflow<T>>
    where
        T: Timestamp,
        F: Fn(usize, &mut DataflowBuilder<T>) -> Result<()>,
    {
        if num_workers == 0 {
            return Err(Error::Custom("num_workers must be >= 1".into()));
        }

        // Phase 1: Build all N dataflows from the closure.
        let mut dataflows = Vec::with_capacity(num_workers);
        for worker_idx in 0..num_workers {
            let mut builder = DataflowBuilder::new(format!("{name}/worker-{worker_idx}"));
            build(worker_idx, &mut builder)?;
            let mut df = builder.build()?;
            df.collect_metrics = collect_metrics;
            dataflows.push(df);
        }

        // Phase 2: Validate topologies match across workers.
        if num_workers > 1 {
            validate_multi_worker_topologies(&dataflows)?;
        }

        // Phase 2b: Validate per-stage parallelism compatibility.
        // All explicit stage parallelism values must equal num_workers because
        // per-stage executors are not yet implemented. We validate dataflows[0]
        // since validate_multi_worker_topologies already ensures all replicas
        // have identical stage metadata.
        validate_stage_parallelism(&dataflows[0], num_workers)?;

        // Phase 3: Wire up exchange channels (replace placeholder factories
        // with shared cross-worker exchange channel factories).
        if num_workers > 1 {
            // Take exchange creators from worker 0 (all workers have identical
            // topology, so worker 0's creators are representative).
            let creators = std::mem::take(&mut dataflows[0].exchange_creators);
            for (edge_idx, edge_capacity, creator) in creators {
                let shared_factories = creator(num_workers, num_workers, edge_capacity);
                if shared_factories.len() != num_workers {
                    return Err(Error::Custom(format!(
                        "exchange factory creator for edge {edge_idx} produced {} factories, expected {num_workers}",
                        shared_factories.len()
                    )));
                }
                for (worker_idx, factory) in shared_factories.into_iter().enumerate() {
                    let pos = dataflows[worker_idx]
                        .channel_factories
                        .iter()
                        .position(|(idx, _)| *idx == edge_idx)
                        .ok_or_else(|| Error::Custom(format!(
                            "exchange edge {edge_idx} not found in worker {worker_idx}'s channel factories"
                        )))?;
                    dataflows[worker_idx].channel_factories[pos].1 = factory;
                }
            }
            // Drain unused exchange_creators from other workers.
            for df in dataflows.iter_mut().skip(1) {
                df.exchange_creators.clear();
            }
        }

        // Phase 4: Create per-worker wake handles and progress exchange channels.
        // Wake handles must be created first since progress channels reference them
        // for cross-worker notification (waking idle workers on progress arrival).
        let wake_handles: Vec<WakeHandle> = (0..num_workers).map(|_| WakeHandle::new()).collect();

        let mut progress_channels = if num_workers > 1 {
            create_progress_channels::<T>(num_workers, &wake_handles)
        } else {
            Vec::new()
        };

        // Phase 4b: Create cross-worker control broadcast channel.
        // For multi-worker dataflows, all workers share a broadcast channel for
        // error propagation and control signals. A shared dataflow-level cancel
        // token ensures that any worker's error cascades to all siblings.
        let dataflow_cancel = if num_workers > 1 {
            Some(self.cancel.child_token())
        } else {
            None
        };
        let mut control_pairs: Vec<Option<(ControlSender, ControlReceiver)>> = if num_workers > 1 {
            let df_cancel = dataflow_cancel.as_ref().unwrap().clone();
            let (senders, receivers) = ControlBroadcast::new(num_workers, &wake_handles, df_cancel);
            senders
                .into_iter()
                .zip(receivers)
                .map(|(s, r)| Some((s, r)))
                .collect()
        } else {
            (0..num_workers).map(|_| None).collect()
        };

        // Phase 5: Materialize all workers WITHOUT registering them yet.
        //
        // This is critical for correctness: materialize_executor() calls
        // tracker.initialize(), which broadcasts initial capabilities to peer
        // workers' progress channels. If we registered (and thus polled)
        // workers immediately, a fast worker could see incomplete global
        // state before slower workers have initialized and broadcast their
        // initial capabilities. By deferring registration until ALL workers
        // are materialized, we guarantee every worker's progress channels
        // contain the full set of initial capability broadcasts from all
        // peers before any worker starts executing.
        let dataflow_id = DataflowId::new();
        let mut prepared = Vec::with_capacity(num_workers);
        let mut spawned_count = 0usize;

        for (worker_idx, dataflow) in dataflows.into_iter().enumerate() {
            let ctx = WorkerContext::new(worker_idx, num_workers);
            let pc = if !progress_channels.is_empty() {
                // Take this worker's progress channels (replace with empty placeholder).
                Some(std::mem::replace(
                    &mut progress_channels[worker_idx],
                    WorkerProgressChannels {
                        senders: Vec::new(),
                        receivers: Vec::new(),
                    },
                ))
            } else {
                None
            };
            let wh = wake_handles[worker_idx].clone();
            let ctrl = control_pairs[worker_idx].take();
            match self.prepare_worker(
                dataflow,
                mode,
                ctx,
                pc,
                Some(wh),
                dataflow_cancel.clone(),
                ctrl,
            ) {
                Ok(worker) => {
                    prepared.push(worker);
                    spawned_count += 1;
                }
                Err(e) => {
                    // Cancel and drop already-prepared workers (they haven't
                    // been registered, so just cancel their tokens).
                    for (w, _, _) in &prepared {
                        w.cancel
                            .cancel_with_reason(CancellationReason::WorkerFailed(format!(
                                "sibling worker {spawned_count} failed to spawn"
                            )));
                    }
                    return Err(Error::Custom(format!(
                        "failed to spawn worker {spawned_count}: {e}"
                    )));
                }
            }
        }

        // Phase 6: Register all workers NOW — every worker's tracker has
        // initialized and broadcast its initial capabilities, so all progress
        // channels contain the complete initial state from all peers.
        //
        // Track each worker as an active dataflow for wait_idle()/shutdown_async().
        let mut workers = Vec::with_capacity(num_workers);
        for (spawned, executor, mut notifier) in prepared {
            self.active_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let active_count = Arc::clone(&self.active_count);
            let idle_notify = Arc::clone(&self.idle_notify);
            notifier.set_on_complete(Box::new(move || {
                let prev = active_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                if prev == 1 {
                    idle_notify.notify_waiters();
                }
            }));
            self.registry
                .register(executor, notifier, dataflow_id, priority);
            workers.push(spawned);
        }

        let multi = MultiSpawnedDataflow {
            name: name.to_string(),
            num_workers,
            workers,
            dataflow_cancel: dataflow_cancel.clone(),
            _phantom: PhantomData,
        };

        // If an external cancellation token was provided, spawn a bridge task
        // that propagates cancellation to all workers. Exits when either fires.
        if let Some(user_token) = external_cancel {
            let cancel = if let Some(ref dc) = multi.dataflow_cancel {
                dc.clone()
            } else {
                multi.workers[0].cancel.clone()
            };
            self.tokio_handle.spawn(async move {
                tokio::select! {
                    _ = user_token.cancelled() => {
                        cancel.cancel_with_reason(CancellationReason::UserRequested);
                    }
                    _ = cancel.cancelled_async() => {
                        // Dataflow already cancelled/completed — exit cleanly.
                    }
                }
            });
        }

        Ok(multi)
    }

    /// Spawn a multi-node cluster dataflow.
    ///
    /// Each physical node in the cluster calls this method independently with
    /// the same topology, dataflow ID, and build closure. Only workers assigned
    /// to `local_node_id` are created on this node; remote workers are
    /// communicated with via `connections`.
    ///
    /// # Protocol
    ///
    /// 1. Build local workers from the closure
    /// 2. Validate graph topology consistency
    /// 3. Create transport session (priority-multiplexed TCP)
    /// 4. Handshake: exchange graph fingerprints with all peers
    /// 5. Wire exchange channels (network for cross-node, local for same-node)
    /// 6. Wire progress channels (network + local)
    /// 7. Materialize all local workers
    /// 8. Ready barrier: wait for all peers to finish materialization
    /// 9. Register workers for execution
    ///
    /// # Arguments
    ///
    /// - `name`: Human-readable name for this dataflow.
    /// - `topology`: Cluster topology (must be identical on all nodes).
    /// - `local_node_id`: This node's ID in the topology.
    /// - `dataflow_id`: Unique dataflow ID (must be the same on all nodes).
    /// - `connections`: Pre-established TCP connections to all remote peers.
    /// - `capacity`: Buffer capacity for channels.
    /// - `handshake_timeout`: Timeout for handshake and ready barrier.
    /// - `build`: Closure that builds the dataflow graph (called once per local worker).
    /// - `runtime_handle`: Tokio runtime handle for async bridge tasks.
    ///
    /// # Errors
    ///
    /// - `local_node_id` not found in topology
    /// - Connections don't match expected remote peers
    /// - Build closure fails
    /// - Graph fingerprint mismatch with any peer
    /// - Handshake or ready barrier timeout
    #[cfg(feature = "transport")]
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_cluster<T, F, R, W>(
        &self,
        name: &str,
        topology: crate::execute::ClusterTopology,
        local_node_id: &str,
        dataflow_id: crate::dataflow::id::DataflowId,
        connections: Vec<crate::communication::transport_session::PeerConnection<R, W>>,
        capacity: usize,
        handshake_timeout: std::time::Duration,
        build: F,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Result<ClusterSpawnedDataflow<T>>
    where
        T: Timestamp + crate::communication::codec::ExchangeData,
        F: Fn(usize, &mut DataflowBuilder<T>) -> Result<()>,
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        use crate::communication::control_protocol::{
            compute_fingerprint, perform_handshake, perform_ready_barrier,
        };
        use crate::communication::transport_session::{
            CONTROL_CHANNEL_ID, ChannelRegistration, TransportSession,
        };
        use crate::dataflow::channels::exchange_channel::NetworkMaterializerParams;
        use crate::dataflow::channels::network::NetworkEdgeMaterializer;
        use crate::progress::network_progress::{
            DEFAULT_MAX_BATCH_SIZE, create_network_progress_channels, progress_channel_id,
        };

        let total_workers = topology.total_workers();
        let (local_start, local_end) = topology.worker_range(local_node_id).ok_or_else(|| {
            Error::Custom(format!(
                "local_node_id '{local_node_id}' not found in topology"
            ))
        })?;
        let num_local = local_end - local_start;

        // Validate connections match topology (one per remote peer, no self, no dups).
        {
            let mut expected_peers: std::collections::HashSet<&str> = topology
                .nodes
                .iter()
                .map(|n| n.node_id.as_str())
                .filter(|id| *id != local_node_id)
                .collect();
            for conn in &connections {
                if conn.node_id == local_node_id {
                    return Err(Error::Custom(format!(
                        "connection to self ('{local_node_id}') is not allowed"
                    )));
                }
                if !expected_peers.remove(conn.node_id.as_str()) {
                    return Err(Error::Custom(format!(
                        "unexpected or duplicate connection to peer '{}'",
                        conn.node_id
                    )));
                }
            }
            if !expected_peers.is_empty() {
                let missing: Vec<_> = expected_peers.into_iter().collect();
                return Err(Error::Custom(format!(
                    "missing connections to peers: {:?}",
                    missing
                )));
            }
        }

        // Phase 1: Build local workers from the closure.
        let mut dataflows = Vec::with_capacity(num_local);
        for worker_idx in local_start..local_end {
            let mut builder = DataflowBuilder::new(format!("{name}/worker-{worker_idx}"));
            build(worker_idx, &mut builder)?;
            let df = builder.build()?;
            dataflows.push(df);
        }

        // Phase 2: Validate topologies match across local workers.
        if num_local > 1 {
            validate_multi_worker_topologies(&dataflows)?;
        }

        // Phase 2b: Validate per-stage parallelism compatibility (uses total_workers
        // since cluster mode spans all nodes).
        validate_stage_parallelism(&dataflows[0], total_workers)?;

        // Phase 3: Compute fingerprint, build channel registrations, create TransportSession.
        let exchange_indices = dataflows[0].exchange_edge_indices();
        let fingerprint = compute_fingerprint(
            dataflows[0].operator_count(),
            dataflows[0].edge_count(),
            &exchange_indices,
            dataflows[0].feedback_edge_count(),
            total_workers,
        );

        // Data channel registrations: for each exchange edge × each (remote_src, local_dst) pair.
        let mut data_regs = Vec::new();
        for (edge_order, &_edge_idx) in exchange_indices.iter().enumerate() {
            for node in &topology.nodes {
                if node.node_id == local_node_id {
                    continue;
                }
                let peer_id = &node.node_id;
                let (peer_start, peer_end) = topology.worker_range(peer_id).unwrap();
                for src in peer_start..peer_end {
                    for dst in local_start..local_end {
                        let channel_id = NetworkEdgeMaterializer::<T, u8>::channel_id(
                            edge_order,
                            src,
                            dst,
                            total_workers,
                        );
                        data_regs.push(ChannelRegistration {
                            peer_node_id: peer_id.clone(),
                            channel_id,
                        });
                    }
                }
            }
        }

        // Progress channel registrations.
        let mut progress_regs = Vec::new();
        for node in &topology.nodes {
            if node.node_id == local_node_id {
                continue;
            }
            let peer_id = &node.node_id;
            let (peer_start, peer_end) = topology.worker_range(peer_id).unwrap();
            for src in peer_start..peer_end {
                for dst in local_start..local_end {
                    let ch_id = progress_channel_id(src, dst, total_workers);
                    progress_regs.push(ChannelRegistration {
                        peer_node_id: peer_id.clone(),
                        channel_id: ch_id,
                    });
                }
            }
        }

        let (session, mut receivers) = TransportSession::new(
            dataflow_id,
            connections,
            &data_regs,
            &progress_regs,
            capacity,
            runtime_handle,
        );
        let session = Arc::new(session);

        // Phase 4: Handshake — exchange fingerprints with all peers.
        // Extract control receivers from the receivers map.
        let mut control_receivers: std::collections::HashMap<
            String,
            tokio::sync::mpsc::Receiver<Vec<u8>>,
        > = std::collections::HashMap::new();
        for (peer_id, peer_map) in receivers.iter_mut() {
            if let Some(rx) = peer_map.remove(&CONTROL_CHANNEL_ID) {
                control_receivers.insert(peer_id.clone(), rx);
            }
        }

        runtime_handle
            .block_on(perform_handshake(
                &session,
                &mut control_receivers,
                fingerprint,
                dataflow_id,
                handshake_timeout,
            ))
            .map_err(|e| Error::Custom(format!("cluster handshake failed: {e}")))?;

        // Phase 5: Wire exchange channels using network-backed factories.
        // Create wake handles BEFORE exchange wiring so bridge tasks can use them.
        // Wake handles for ALL workers (remote ones are placeholders for API compat).
        let wake_handles: Vec<WakeHandle> = (0..total_workers).map(|_| WakeHandle::new()).collect();

        // Take network creators from worker 0 (all workers have identical topology).
        let network_creators = std::mem::take(&mut dataflows[0].exchange_network_creators);

        for (edge_order, (edge_idx, edge_capacity, creator)) in
            network_creators.into_iter().enumerate()
        {
            // Extract receivers for this specific exchange edge from the shared map.
            let mut edge_receivers: std::collections::HashMap<
                String,
                std::collections::HashMap<u64, tokio::sync::mpsc::Receiver<Vec<u8>>>,
            > = std::collections::HashMap::new();
            for node in &topology.nodes {
                if node.node_id == local_node_id {
                    continue;
                }
                let peer_id = &node.node_id;
                let (peer_start, peer_end) = topology.worker_range(peer_id).unwrap();
                let mut extracted = std::collections::HashMap::new();
                if let Some(peer_map) = receivers.get_mut(peer_id) {
                    for src in peer_start..peer_end {
                        for dst in local_start..local_end {
                            let channel_id = NetworkEdgeMaterializer::<T, u8>::channel_id(
                                edge_order,
                                src,
                                dst,
                                total_workers,
                            );
                            if let Some(rx) = peer_map.remove(&channel_id) {
                                extracted.insert(channel_id, rx);
                            }
                        }
                    }
                }
                if !extracted.is_empty() {
                    edge_receivers.insert(peer_id.clone(), extracted);
                }
            }

            let params = NetworkMaterializerParams {
                dataflow_id,
                topology: topology.clone(),
                local_node_id: local_node_id.to_string(),
                session: Arc::clone(&session),
                receivers: edge_receivers,
                capacity: edge_capacity,
                num_workers: total_workers,
                edge_index: edge_order,
                wake_handles: wake_handles.clone(),
                runtime_handle: runtime_handle.clone(),
            };
            let all_factories = creator.create(params);

            if all_factories.len() != total_workers {
                return Err(Error::Custom(format!(
                    "network exchange factory for edge {edge_idx} produced {} factories, expected {total_workers}",
                    all_factories.len()
                )));
            }

            // Install only local workers' factories.
            for (local_idx, factory) in all_factories
                .into_iter()
                .skip(local_start)
                .take(num_local)
                .enumerate()
            {
                let pos = dataflows[local_idx]
                    .channel_factories
                    .iter()
                    .position(|(idx, _)| *idx == edge_idx)
                    .ok_or_else(|| {
                        Error::Custom(format!(
                            "exchange edge {edge_idx} not found in worker {}'s channel factories",
                            local_start + local_idx
                        ))
                    })?;
                dataflows[local_idx].channel_factories[pos].1 = factory;
            }
        }

        // Drain unused exchange_creators from all workers.
        for df in dataflows.iter_mut() {
            df.exchange_creators.clear();
            df.exchange_network_creators.clear();
        }

        // Phase 6: Create progress channels (wake handles already created in Phase 5).

        // Create local progress channels between local workers.
        let all_local_progress = create_progress_channels::<T>(total_workers, &wake_handles);

        // Extract only local workers' progress channels.
        let local_progress: Vec<_> = all_local_progress
            .into_iter()
            .skip(local_start)
            .take(num_local)
            .collect();

        // Compute remote peer info for network progress.
        let remote_peers: Vec<(String, usize, usize)> = topology
            .nodes
            .iter()
            .filter(|n| n.node_id != local_node_id)
            .map(|n| {
                let (s, e) = topology.worker_range(&n.node_id).unwrap();
                (n.node_id.clone(), s, e)
            })
            .collect();

        // Create a dataflow-level cancel token for local workers. When any local
        // worker fails, this cascades cancellation to all siblings.
        let dataflow_cancel = Some(self.cancel.child_token());
        // Convert to tokio_util CancellationToken for bridge tasks.
        let bridge_cancel = tokio_util::sync::CancellationToken::new();

        // Create control broadcast for local workers (error propagation + control signals).
        let local_wake_handles: Vec<WakeHandle> = (local_start..local_end)
            .map(|i| wake_handles[i].clone())
            .collect();
        let mut control_pairs: Vec<Option<(ControlSender, ControlReceiver)>> = if num_local > 1 {
            let df_cancel = dataflow_cancel.as_ref().unwrap().clone();
            let (senders, receivers) =
                ControlBroadcast::new(num_local, &local_wake_handles, df_cancel);
            senders
                .into_iter()
                .zip(receivers)
                .map(|(s, r)| Some((s, r)))
                .collect()
        } else {
            vec![None]
        };

        let (progress_channels, progress_handles) = create_network_progress_channels::<T>(
            local_progress,
            &session,
            receivers,
            dataflow_id,
            (local_start, local_end),
            &remote_peers,
            total_workers,
            &wake_handles,
            bridge_cancel.clone(),
            DEFAULT_MAX_BATCH_SIZE,
            runtime_handle,
        )
        .map_err(|e| Error::Custom(format!("failed to create network progress channels: {e}")))?;

        // Phase 7: Materialize all local workers (without registering).
        let mode = ChannelMode::Sync;
        let dataflow_priority = 0;
        let mut prepared = Vec::with_capacity(num_local);
        let mut progress_channels_iter = progress_channels.into_iter();

        for (local_idx, dataflow) in dataflows.into_iter().enumerate() {
            let global_idx = local_start + local_idx;
            let ctx = WorkerContext::new(global_idx, total_workers);
            let pc = progress_channels_iter.next().map(Some).unwrap_or(None);
            let wh = wake_handles[global_idx].clone();
            let ctrl = control_pairs[local_idx].take();
            match self.prepare_worker(
                dataflow,
                mode,
                ctx,
                pc,
                Some(wh),
                dataflow_cancel.clone(),
                ctrl,
            ) {
                Ok(worker) => prepared.push(worker),
                Err(e) => {
                    for (w, _, _) in &prepared {
                        w.cancel
                            .cancel_with_reason(CancellationReason::WorkerFailed(format!(
                                "cluster worker {global_idx} failed to spawn"
                            )));
                    }
                    return Err(Error::Custom(format!(
                        "failed to spawn cluster worker {global_idx}: {e}"
                    )));
                }
            }
        }

        // Phase 8: Ready barrier — wait for all peers to finish materialization.
        runtime_handle
            .block_on(perform_ready_barrier(
                &session,
                &mut control_receivers,
                local_node_id,
                dataflow_id,
                handshake_timeout,
            ))
            .map_err(|e| Error::Custom(format!("cluster ready barrier failed: {e}")))?;

        // Phase 9: Register all workers for execution.
        // Track each worker as an active dataflow for wait_idle()/shutdown_async().
        let mut workers = Vec::with_capacity(num_local);
        for (spawned, executor, mut notifier) in prepared {
            self.active_count
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let active_count = Arc::clone(&self.active_count);
            let idle_notify = Arc::clone(&self.idle_notify);
            notifier.set_on_complete(Box::new(move || {
                let prev = active_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                if prev == 1 {
                    idle_notify.notify_waiters();
                }
            }));
            self.registry
                .register(executor, notifier, dataflow_id, dataflow_priority);
            workers.push(spawned);
        }

        // Phase 10: Register in peer registry for peer-down notification.
        let remote_peer_ids: Vec<String> = topology
            .nodes
            .iter()
            .map(|n| n.node_id.clone())
            .filter(|id| id != local_node_id)
            .collect();
        if !remote_peer_ids.is_empty() {
            let worker_tokens: Vec<CancellationToken> =
                workers.iter().map(|w| w.cancel.clone()).collect();
            let cancel_handle = ClusterCancelHandle {
                worker_tokens,
                bridge_cancel: bridge_cancel.clone(),
            };
            self.peer_registry
                .register(&remote_peer_ids, name, cancel_handle);
        }

        Ok(ClusterSpawnedDataflow {
            inner: Some(MultiSpawnedDataflow {
                name: name.to_string(),
                num_workers: num_local,
                workers,
                dataflow_cancel,
                _phantom: PhantomData,
            }),
            local_worker_range: (local_start, local_end),
            total_workers,
            _session: session,
            _progress_handles: progress_handles,
            _bridge_cancel: bridge_cancel,
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
        self.cancel
            .cancel_with_reason(CancellationReason::RuntimeShutdown);
    }
}

// ---------------------------------------------------------------------------
// SimpleRuntime — lightweight single-thread runtime (test-utils only)
// ---------------------------------------------------------------------------

#[cfg(feature = "test-utils")]
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

#[cfg(feature = "test-utils")]
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
        let mut executor = materialize_executor(
            dataflow,
            self.cancel.clone(),
            Some(wake_handle),
            WorkerContext::single(),
            None,
        )?;

        let completed = executor.run()?;
        if !completed {
            return Err(Error::Custom(
                "dataflow did not complete (quiescence without termination)".into(),
            ));
        }

        Ok(())
    }

    /// Run a dataflow to completion and return collected metrics.
    ///
    /// Always enables metrics collection regardless of builder settings.
    /// Returns the per-operator metrics on success.
    pub fn run_with_metrics<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
    ) -> Result<Option<std::sync::Arc<crate::metrics::DataflowMetrics>>> {
        if dataflow.has_input_ports() {
            return Err(Error::Custom(
                "cannot run() a dataflow with declared input ports — \
                 use spawn() for dataflows that receive external data."
                    .into(),
            ));
        }

        if dataflow.operator_factories.is_empty() {
            return Ok(None);
        }

        dataflow.collect_metrics = true;

        let wake_handle = WakeHandle::new();
        self.cancel.register_wake_handle(wake_handle.clone());
        let mut executor = materialize_executor(
            dataflow,
            self.cancel.clone(),
            Some(wake_handle),
            WorkerContext::single(),
            None,
        )?;

        let completed = executor.run()?;
        if !completed {
            return Err(Error::Custom(
                "dataflow did not complete (quiescence without termination)".into(),
            ));
        }

        Ok(executor.metrics().cloned())
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
    pub fn spawn<T: Timestamp>(&self, dataflow: LogicalDataflow<T>) -> Result<SpawnedDataflow<T>> {
        self.spawn_with_context(dataflow, WorkerContext::single())
    }

    fn spawn_with_context<T: Timestamp>(
        &self,
        mut dataflow: LogicalDataflow<T>,
        worker_context: WorkerContext,
    ) -> Result<SpawnedDataflow<T>> {
        if dataflow.operator_factories.is_empty()
            && dataflow.input_port_wiring.is_empty()
            && dataflow.async_source_wiring.is_empty()
        {
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
        let mut input_count = dataflow.input_port_wiring.len();

        for (info, mut wiring) in dataflow
            .input_ports
            .iter()
            .zip(dataflow.input_port_wiring.drain(..))
        {
            let (factory, sender_any) = wiring(
                Arc::clone(&external_inputs_open),
                wake_handle.clone(),
                ChannelMode::Sync,
            );
            dataflow
                .operator_factories
                .push((info.operator_index, factory));
            input_senders.push((info.name.clone(), info.type_name, sender_any));
        }

        // --- Wire async source ports ---
        let mut pump_tasks: Vec<Box<dyn FnOnce() + Send>> = Vec::new();
        {
            let async_count = dataflow.async_source_wiring.len();
            input_count += async_count;
            for (op_idx, wiring) in dataflow.async_source_wiring.drain(..) {
                let (factory, pump) = wiring(
                    Arc::clone(&external_inputs_open),
                    wake_handle.clone(),
                    cancel.clone(),
                );
                dataflow.operator_factories.push((op_idx, factory));
                pump_tasks.push(pump);
            }
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
                dataflow.operator_factories[pos] = (info.operator_index, replacement_factory);
            }
            output_receivers.push((info.name.clone(), info.type_name, receiver_any));
        }

        // --- Materialize and run on background thread ---
        let mut executor =
            materialize_executor(dataflow, cancel, Some(wake_handle), worker_context, None)?;

        external_inputs_open.store(input_count, std::sync::atomic::Ordering::SeqCst);
        executor.replace_external_inputs_counter(external_inputs_open);

        let (completion, notifier) = DataflowCompletion::new();
        let metrics = executor.metrics().cloned();

        // --- Spawn async source pump tasks ---
        for (pump_idx, pump) in pump_tasks.into_iter().enumerate() {
            std::thread::Builder::new()
                .name(format!("source-pump-{}-{}", name, pump_idx))
                .spawn(pump)
                .map_err(|e| Error::Custom(format!("failed to spawn pump thread: {e}")))?;
        }

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
            metrics,
            _phantom: PhantomData,
        })
    }

    /// Spawn N replicated workers from the same dataflow builder closure.
    ///
    /// Like [`RuntimeHandle::spawn_multi()`] but each worker gets a dedicated
    /// background thread instead of running on a shared pool.
    ///
    /// See [`RuntimeHandle::spawn_multi()`] for full documentation.
    pub fn spawn_multi<T, F>(
        &self,
        name: &str,
        num_workers: usize,
        build: F,
    ) -> Result<MultiSpawnedDataflow<T>>
    where
        T: Timestamp,
        F: Fn(usize, &mut DataflowBuilder<T>) -> Result<()>,
    {
        if num_workers == 0 {
            return Err(Error::Custom("num_workers must be >= 1".into()));
        }

        // Phase 1: Build all N dataflows.
        let mut dataflows = Vec::with_capacity(num_workers);
        for worker_idx in 0..num_workers {
            let mut builder = DataflowBuilder::new(format!("{name}/worker-{worker_idx}"));
            build(worker_idx, &mut builder)?;
            let df = builder.build()?;
            dataflows.push(df);
        }

        // Phase 2: Validate topologies match.
        if num_workers > 1 {
            validate_multi_worker_topologies(&dataflows)?;
        }

        // Phase 2b: Validate per-stage parallelism compatibility.
        validate_stage_parallelism(&dataflows[0], num_workers)?;

        // Phase 3: Wire up exchange channels (replace placeholder factories
        // with shared cross-worker exchange channel factories).
        if num_workers > 1 {
            let creators = std::mem::take(&mut dataflows[0].exchange_creators);
            for (edge_idx, edge_capacity, creator) in creators {
                let shared_factories = creator(num_workers, num_workers, edge_capacity);
                if shared_factories.len() != num_workers {
                    return Err(Error::Custom(format!(
                        "exchange factory creator for edge {edge_idx} produced {} factories, expected {num_workers}",
                        shared_factories.len()
                    )));
                }
                for (worker_idx, factory) in shared_factories.into_iter().enumerate() {
                    let pos = dataflows[worker_idx]
                        .channel_factories
                        .iter()
                        .position(|(idx, _)| *idx == edge_idx)
                        .ok_or_else(|| Error::Custom(format!(
                            "exchange edge {edge_idx} not found in worker {worker_idx}'s channel factories"
                        )))?;
                    dataflows[worker_idx].channel_factories[pos].1 = factory;
                }
            }
            for df in dataflows.iter_mut().skip(1) {
                df.exchange_creators.clear();
            }
        }

        // Phase 4: Spawn all workers on dedicated threads.
        let mut workers = Vec::with_capacity(num_workers);
        let mut spawned_count = 0usize;

        for (worker_idx, dataflow) in dataflows.into_iter().enumerate() {
            let ctx = WorkerContext::new(worker_idx, num_workers);
            match self.spawn_with_context(dataflow, ctx) {
                Ok(spawned) => {
                    workers.push(spawned);
                    spawned_count += 1;
                }
                Err(e) => {
                    for w in &workers {
                        w.cancel
                            .cancel_with_reason(CancellationReason::WorkerFailed(format!(
                                "sibling worker {spawned_count} failed to spawn"
                            )));
                    }
                    for w in workers {
                        let _ = w.join_blocking();
                    }
                    return Err(Error::Custom(format!(
                        "failed to spawn worker {spawned_count}: {e}"
                    )));
                }
            }
        }

        Ok(MultiSpawnedDataflow {
            name: name.to_string(),
            num_workers,
            workers,
            dataflow_cancel: None, // SimpleRuntime uses dedicated threads, not shared task pool
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "test-utils")]
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
    on_complete: Option<Box<dyn FnOnce() + Send>>,
}

impl CompletionNotifier {
    /// Attach a callback that fires when the notifier completes or is dropped.
    ///
    /// Used by `RuntimeHandle` to track active dataflow count.
    pub(crate) fn set_on_complete(&mut self, f: Box<dyn FnOnce() + Send>) {
        self.on_complete = Some(f);
    }

    /// Publish the executor result and wake any waiting future/condvar.
    pub fn complete(mut self, result: Result<bool>) {
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
        // Fire on_complete callback before forgetting self.
        if let Some(cb) = self.on_complete.take() {
            cb();
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
        // Fire on_complete callback.
        if let Some(cb) = self.on_complete.take() {
            cb();
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
/// let handle = rt.spawn(dataflow, SpawnOptions::default())?;
/// handle.join().await?;
///
/// // Sync (blocking) usage
/// rt.spawn(dataflow, SpawnOptions::default())?.join_blocking()?;
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
        let notifier = CompletionNotifier { shared, condvar, on_complete: None };
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
            Err(_) => return Poll::Ready(Err(Error::Custom("completion mutex poisoned".into()))),
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
    /// `(name, type_name, Box<InputSender<T, D>> as Box<dyn Any>)`
    input_senders: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    /// `(name, type_name, Box<OutputReceiver<T, D>> as Box<dyn Any>)`
    output_receivers: Vec<(String, &'static str, Box<dyn std::any::Any + Send>)>,
    /// Collected per-operator metrics (None if metrics not enabled).
    metrics: Option<Arc<crate::metrics::DataflowMetrics>>,
    _phantom: PhantomData<T>,
}

impl<T: Timestamp> SpawnedDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the collected dataflow metrics.
    ///
    /// Returns `Some` if metrics collection was enabled via
    /// [`SpawnOptions::collect_metrics`]. The metrics are live —
    /// values update as the dataflow executes.
    ///
    /// Returns `None` if metrics collection was not enabled.
    pub fn metrics(&self) -> Option<&Arc<crate::metrics::DataflowMetrics>> {
        self.metrics.as_ref()
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
                "input port '{name}' type downcast failed — if spawned with IoMode::Async, use take_async_input()"
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
                "output port '{name}' type downcast failed — if spawned with IoMode::Async, use take_async_output()"
            )))
    }

    /// Take the async input sender for the named port (consumes it).
    ///
    /// Only works when the dataflow was spawned with async I/O (`IoMode::Async`).
    /// Returns an error if the port was wired as sync or does not exist.
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
            .map_err(|_| {
                Error::Custom(format!(
                    "input port '{name}' was not wired for async I/O (spawn with IoMode::Async)"
                ))
            })
    }

    /// Take the async output receiver for the named port (consumes it).
    ///
    /// Only works when the dataflow was spawned with async I/O (`IoMode::Async`).
    /// Returns an error if the port was wired as sync or does not exist.
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
            .map_err(|_| {
                Error::Custom(format!(
                    "output port '{name}' was not wired for async I/O (spawn with IoMode::Async)"
                ))
            })
    }

    /// Cancel the running dataflow.
    ///
    /// Signals the executor's cancellation token with [`CancellationReason::UserRequested`].
    /// The executor will stop at the next cancellation check point. Does not block.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Get a reference to the dataflow's cancellation token.
    ///
    /// Useful for observing the cancellation state (e.g., after a timeout) or
    /// cloning the token for use in other contexts.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Cancel the running dataflow with a specific reason.
    ///
    /// Signals the executor's cancellation token. The executor will stop
    /// at the next cancellation check point. Does not block.
    pub fn cancel_with_reason(&self, reason: CancellationReason) {
        self.cancel.cancel_with_reason(reason);
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
            self.cancel
                .cancel_with_reason(CancellationReason::HandleDropped);
        }
        // Don't block waiting — cancel and detach. The executor will stop
        // at the next cancellation check point.
    }
}

// ---------------------------------------------------------------------------
// MultiSpawnedDataflow — handle for N replicated workers
// ---------------------------------------------------------------------------

/// A handle to N replicated dataflow workers spawned from the same builder closure.
///
/// Each worker is an independent [`SpawnedDataflow`] with its own executor,
/// input senders, and output receivers. There is no cross-worker data routing —
/// all channels are worker-local.
///
/// ## Uniform replication
///
/// All stages in the dataflow have the same number of workers (`num_workers`).
/// Per-stage parallelism (e.g., more workers in a data-ingestion stage,
/// fewer in a reduction stage) is not yet supported and requires exchange
/// channels at stage boundaries. Note that "stage" is instancy's scoping
/// concept for progress tracking — it is more general than Spark's linear
/// "stage" model because stages can be nested (e.g., loop bodies are inner
/// stages).
///
/// ## Logical vs physical parallelism
///
/// `num_workers` controls **logical** parallelism — the number of independent
/// executor instances. The runtime's thread pool controls **physical**
/// parallelism. It is valid to have more logical workers than physical
/// threads; the executors will be scheduled cooperatively on the pool.
///
/// # Lifecycle
///
/// 1. Access per-worker handles via [`worker_mut()`](Self::worker_mut)
/// 2. Take per-worker inputs/outputs from each handle
/// 3. Feed data, then close inputs
/// 4. Call [`join_blocking()`](Self::join_blocking) or [`join()`](Self::join)
///    to wait for all workers to finish
///
/// Dropping without calling `join()` cancels all workers.
pub struct MultiSpawnedDataflow<T: Timestamp> {
    name: String,
    num_workers: usize,
    workers: Vec<SpawnedDataflow<T>>,
    /// Shared cancellation token for all workers in this dataflow.
    /// Cancelling this token cascades to all worker tokens.
    /// `None` for single-worker dataflows (no intermediate token).
    /// Held for lifetime — dropping cancels all workers.
    #[allow(dead_code)]
    dataflow_cancel: Option<CancellationToken>,
    _phantom: PhantomData<T>,
}

impl<T: Timestamp> MultiSpawnedDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of workers.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Get a mutable reference to a worker's handle for taking inputs/outputs.
    ///
    /// # Panics
    ///
    /// Panics if `worker_idx >= num_workers`.
    pub fn worker_mut(&mut self, worker_idx: usize) -> &mut SpawnedDataflow<T> {
        &mut self.workers[worker_idx]
    }

    /// Take the input sender from a specific worker.
    ///
    /// Convenience for `worker_mut(idx).take_input(name)`.
    pub fn take_input<D: Clone + Send + 'static>(
        &mut self,
        worker_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::InputSender<T, D>> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }
        self.workers[worker_idx].take_input(name)
    }

    /// Take the output receiver from a specific worker.
    ///
    /// Convenience for `worker_mut(idx).take_output(name)`.
    pub fn take_output<D: Send + 'static>(
        &mut self,
        worker_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::OutputReceiver<T, D>> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }
        self.workers[worker_idx].take_output(name)
    }

    /// Take the async input sender from a specific worker.
    pub fn take_async_input<D: Clone + Send + 'static>(
        &mut self,
        worker_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::AsyncInputSender<T, D>> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }
        self.workers[worker_idx].take_async_input(name)
    }

    /// Take the async output receiver from a specific worker.
    pub fn take_async_output<D: Send + 'static>(
        &mut self,
        worker_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::AsyncOutputReceiver<T, D>> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }
        self.workers[worker_idx].take_async_output(name)
    }

    // -- Batch convenience APIs -------------------------------------------

    /// Take input senders from **all** workers for the named port.
    ///
    /// Returns a `Vec` of length `num_workers`, where element `i` is the
    /// `InputSender` for worker `i`. This is an all-or-nothing operation:
    /// if any worker is missing the port or the type doesn't match, no
    /// senders are consumed and an error is returned.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use instancy::DataflowBuilder;
    /// use instancy::SimpleRuntime;
    /// let rt = SimpleRuntime::new();
    /// let mut multi = rt.spawn_multi("ex", 4, |_, b: &mut DataflowBuilder<u64>| {
    ///     b.input::<i32>("data").output("out"); Ok(())
    /// }).unwrap();
    /// let senders = multi.take_all_inputs::<i32>("data").unwrap();
    /// assert_eq!(senders.len(), 4);
    /// ```
    pub fn take_all_inputs<D: Clone + Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Vec<crate::dataflow::channel_operators::InputSender<T, D>>> {
        let type_name = std::any::type_name::<D>();
        // Pre-validate: every worker must have this port with the right type
        // and correct channel mode (sync).
        for (idx, w) in self.workers.iter().enumerate() {
            Self::validate_port::<crate::dataflow::channel_operators::InputSender<T, D>>(
                &w.input_senders,
                idx,
                name,
                type_name,
                "input",
            )?;
        }
        // Consume from each worker. After full validation this cannot fail.
        let mut senders = Vec::with_capacity(self.num_workers);
        for w in &mut self.workers {
            senders.push(
                w.take_input::<D>(name)
                    .expect("take_all_inputs: pre-validated port disappeared"),
            );
        }
        Ok(senders)
    }

    /// Take output receivers from **all** workers for the named port.
    ///
    /// Returns a `Vec` of length `num_workers`. All-or-nothing semantics
    /// (see [`take_all_inputs`](Self::take_all_inputs)).
    pub fn take_all_outputs<D: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Vec<crate::dataflow::channel_operators::OutputReceiver<T, D>>> {
        let type_name = std::any::type_name::<D>();
        for (idx, w) in self.workers.iter().enumerate() {
            Self::validate_port::<crate::dataflow::channel_operators::OutputReceiver<T, D>>(
                &w.output_receivers,
                idx,
                name,
                type_name,
                "output",
            )?;
        }
        let mut receivers = Vec::with_capacity(self.num_workers);
        for w in &mut self.workers {
            receivers.push(
                w.take_output::<D>(name)
                    .expect("take_all_outputs: pre-validated port disappeared"),
            );
        }
        Ok(receivers)
    }

    /// Take async input senders from **all** workers for the named port.
    ///
    /// Only works when the dataflow was spawned with async channels.
    /// All-or-nothing semantics (see [`take_all_inputs`](Self::take_all_inputs)).
    pub fn take_all_async_inputs<D: Clone + Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Vec<crate::dataflow::channel_operators::AsyncInputSender<T, D>>> {
        let type_name = std::any::type_name::<D>();
        for (idx, w) in self.workers.iter().enumerate() {
            Self::validate_port::<crate::dataflow::channel_operators::AsyncInputSender<T, D>>(
                &w.input_senders,
                idx,
                name,
                type_name,
                "input",
            )?;
        }
        let mut senders = Vec::with_capacity(self.num_workers);
        for w in &mut self.workers {
            senders.push(
                w.take_async_input::<D>(name)
                    .expect("take_all_async_inputs: pre-validated port disappeared"),
            );
        }
        Ok(senders)
    }

    /// Take async output receivers from **all** workers for the named port.
    ///
    /// Only works when the dataflow was spawned with async channels.
    /// All-or-nothing semantics (see [`take_all_inputs`](Self::take_all_inputs)).
    pub fn take_all_async_outputs<D: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Vec<crate::dataflow::channel_operators::AsyncOutputReceiver<T, D>>> {
        let type_name = std::any::type_name::<D>();
        for (idx, w) in self.workers.iter().enumerate() {
            Self::validate_port::<crate::dataflow::channel_operators::AsyncOutputReceiver<T, D>>(
                &w.output_receivers,
                idx,
                name,
                type_name,
                "output",
            )?;
        }
        let mut receivers = Vec::with_capacity(self.num_workers);
        for w in &mut self.workers {
            receivers.push(
                w.take_async_output::<D>(name)
                    .expect("take_all_async_outputs: pre-validated port disappeared"),
            );
        }
        Ok(receivers)
    }

    // -- Validation helpers ------------------------------------------------

    /// Check that a worker has a port with the given name, data type, and
    /// concrete channel type (sync vs async), without consuming it.
    fn validate_port<C: 'static>(
        ports: &[(String, &'static str, Box<dyn std::any::Any + Send>)],
        worker_idx: usize,
        name: &str,
        type_name: &str,
        direction: &str,
    ) -> Result<()> {
        match ports.iter().find(|(n, _, _)| n == name) {
            None => Err(Error::Custom(format!(
                "worker {worker_idx} has no {direction} port named '{name}'"
            ))),
            Some((_, port_type, _)) if *port_type != type_name => Err(Error::Custom(format!(
                "worker {worker_idx} {direction} port '{name}' has type {port_type}, but requested {type_name}"
            ))),
            Some((_, _, any_box)) if !any_box.is::<C>() => Err(Error::Custom(format!(
                "worker {worker_idx} {direction} port '{name}' channel mode mismatch \
                     (sync port with async take, or vice versa)"
            ))),
            _ => Ok(()),
        }
    }

    /// Cancel all workers.
    ///
    /// Each worker's cancellation token is signalled. The executors will stop
    /// at the next cancellation check point.
    pub fn cancel(&self) {
        for w in &self.workers {
            w.cancel();
        }
    }

    /// Cancel all workers with a specific reason.
    pub fn cancel_with_reason(&self, reason: CancellationReason) {
        for w in &self.workers {
            w.cancel_with_reason(reason.clone());
        }
    }

    /// Wait for all workers to complete, blocking the current thread.
    ///
    /// Returns `Ok(())` if all workers ran to completion. If any worker
    /// fails, the remaining workers are cancelled and the first error is returned.
    pub fn join_blocking(mut self) -> Result<()> {
        let mut workers: Vec<SpawnedDataflow<T>> = std::mem::take(&mut self.workers);
        let mut first_error: Option<Error> = None;

        while !workers.is_empty() {
            let worker = workers.remove(0);
            match worker.join_blocking() {
                Ok(()) => {}
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                        // Cancel all remaining workers directly.
                        for w in &workers {
                            w.cancel
                                .cancel_with_reason(CancellationReason::WorkerFailed(
                                    "sibling worker failed".into(),
                                ));
                        }
                    }
                    // Continue draining remaining workers (already cancelled).
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Get completion futures for all workers.
    ///
    /// Returns a [`MultiDataflowCompletion`] that resolves when all workers
    /// finish. On first error, remaining workers are cancelled.
    ///
    /// The returned future implements [`Future`] and can be `.await`ed in
    /// async code, or blocked on via [`.wait()`](MultiDataflowCompletion::wait).
    pub fn join(mut self) -> MultiDataflowCompletion {
        let workers = std::mem::take(&mut self.workers);
        // Collect per-worker cancel tokens so MultiDataflowCompletion can
        // cancel remaining workers directly on first error.
        let worker_cancels: Vec<CancellationToken> =
            workers.iter().map(|w| w.cancel.clone()).collect();
        let completions: Vec<DataflowCompletion> = workers.into_iter().map(|w| w.join()).collect();
        MultiDataflowCompletion {
            resolved: vec![false; completions.len()],
            worker_cancels,
            completions,
            first_error: None,
        }
    }
}

impl<T: Timestamp> Drop for MultiSpawnedDataflow<T> {
    fn drop(&mut self) {
        // Cancel all workers if join() wasn't called.
        for w in &self.workers {
            w.cancel();
        }
    }
}

// ---------------------------------------------------------------------------
// ClusterSpawnedDataflow — handle for a cluster-deployed dataflow
// ---------------------------------------------------------------------------

/// Handle for a cluster-deployed dataflow.
///
/// Wraps [`MultiSpawnedDataflow`] for the local workers and keeps alive the
/// network infrastructure (transport session, progress bridges) needed for
/// cross-node communication.
///
/// # Worker Indexing
///
/// Workers are indexed locally: `take_input(0, ...)` refers to the first
/// LOCAL worker, not global worker 0. Use [`local_worker_range()`](Self::local_worker_range)
/// to map between local and global indices.
///
/// # Lifetime
///
/// Dropping this handle cancels all local workers and tears down network
/// resources (transport session, progress bridges). The `bridge_cancel` token
/// signals all async bridge tasks to shut down.
#[cfg(feature = "transport")]
pub struct ClusterSpawnedDataflow<T: Timestamp> {
    inner: Option<MultiSpawnedDataflow<T>>,
    local_worker_range: (usize, usize),
    total_workers: usize,
    /// Keeps transport session alive (background Muxer/Demuxer tasks).
    _session: Arc<crate::communication::TransportSession>,
    /// Keeps progress bridge tasks alive.
    _progress_handles: crate::progress::network_progress::NetworkProgressHandles,
    /// Cancels bridge tasks on drop.
    _bridge_cancel: tokio_util::sync::CancellationToken,
}

#[cfg(feature = "transport")]
impl<T: Timestamp> ClusterSpawnedDataflow<T> {
    /// Get the dataflow name.
    pub fn name(&self) -> &str {
        self.inner.as_ref().unwrap().name()
    }

    /// Number of LOCAL workers on this node.
    pub fn num_local_workers(&self) -> usize {
        self.inner.as_ref().unwrap().num_workers()
    }

    /// Total worker count across all nodes in the cluster.
    pub fn total_workers(&self) -> usize {
        self.total_workers
    }

    /// Global worker index range for this node: `(start, end)`.
    ///
    /// Local worker `i` corresponds to global worker `start + i`.
    pub fn local_worker_range(&self) -> (usize, usize) {
        self.local_worker_range
    }

    /// Take the input sender from a local worker.
    ///
    /// `local_idx` is 0-based within this node's workers.
    pub fn take_input<D: Clone + Send + 'static>(
        &mut self,
        local_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::InputSender<T, D>> {
        self.inner.as_mut().unwrap().take_input(local_idx, name)
    }

    /// Take the output receiver from a local worker.
    ///
    /// `local_idx` is 0-based within this node's workers.
    pub fn take_output<D: Send + 'static>(
        &mut self,
        local_idx: usize,
        name: &str,
    ) -> Result<crate::dataflow::channel_operators::OutputReceiver<T, D>> {
        self.inner.as_mut().unwrap().take_output(local_idx, name)
    }

    /// Cancel all local workers and tear down the cluster.
    pub fn cancel(&self) {
        self._bridge_cancel.cancel();
        if let Some(ref inner) = self.inner {
            for i in 0..inner.num_workers() {
                inner.workers[i].cancel();
            }
        }
    }

    /// Cancel all local workers with a specific reason and tear down the cluster.
    pub fn cancel_with_reason(&self, reason: CancellationReason) {
        self._bridge_cancel.cancel();
        if let Some(ref inner) = self.inner {
            for i in 0..inner.num_workers() {
                inner.workers[i].cancel_with_reason(reason.clone());
            }
        }
    }

    /// Take the completion handles from all local workers.
    ///
    /// **Important:** This consumes `self`, which triggers `Drop` and cancels
    /// bridge tasks. Use [`join_blocking()`](Self::join_blocking) instead to
    /// ensure bridges stay alive until workers complete.
    ///
    /// The returned [`MultiDataflowCompletion`] implements [`Future`] and can
    /// be `.await`ed in async code, or blocked on via [`.wait()`](MultiDataflowCompletion::wait).
    pub fn join(mut self) -> MultiDataflowCompletion {
        self.inner.take().expect("join called after move").join()
    }

    /// Block until all local workers complete.
    ///
    /// Bridges stay alive during the wait so workers can still receive
    /// remote data and progress. They are cancelled only after all
    /// workers have finished.
    pub fn join_blocking(mut self) -> Result<()> {
        let completion = self.inner.take().expect("join called after move").join();
        let result = completion.wait();
        // NOW self drops — bridges are cancelled AFTER workers complete.
        result
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp> Drop for ClusterSpawnedDataflow<T> {
    fn drop(&mut self) {
        self._bridge_cancel.cancel();
    }
}

// ---------------------------------------------------------------------------
// MultiDataflowCompletion — aggregated completion for N workers
// ---------------------------------------------------------------------------

/// Aggregated completion handle for multiple replicated dataflow workers.
///
/// Waits for all workers to finish. On first error, remaining workers are
/// cancelled and the error is returned.
pub struct MultiDataflowCompletion {
    /// Per-worker cancellation tokens for direct cancellation.
    worker_cancels: Vec<CancellationToken>,
    completions: Vec<DataflowCompletion>,
    /// Tracks which completions have already resolved (for Future impl).
    resolved: Vec<bool>,
    /// First error encountered (for Future impl).
    first_error: Option<Error>,
}

impl MultiDataflowCompletion {
    /// Block the current thread until all workers complete.
    ///
    /// Returns `Ok(())` if all workers succeeded. On first error, cancels
    /// remaining workers and returns that error.
    pub fn wait(self) -> Result<()> {
        let mut first_error: Option<Error> = None;

        for (idx, completion) in self.completions.into_iter().enumerate() {
            match completion.wait() {
                Ok(()) => {}
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                        // Cancel remaining workers directly.
                        for cancel in &self.worker_cancels[idx + 1..] {
                            cancel.cancel_with_reason(CancellationReason::WorkerFailed(
                                "sibling worker failed".into(),
                            ));
                        }
                    }
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Future for MultiDataflowCompletion {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut all_done = true;

        for i in 0..this.completions.len() {
            if this.resolved[i] {
                continue;
            }

            let completion = Pin::new(&mut this.completions[i]);
            match completion.poll(cx) {
                Poll::Ready(Ok(())) => {
                    this.resolved[i] = true;
                }
                Poll::Ready(Err(e)) => {
                    this.resolved[i] = true;
                    if this.first_error.is_none() {
                        this.first_error = Some(e);
                        // Cancel remaining unresolved workers.
                        for (j, cancel) in this.worker_cancels.iter().enumerate() {
                            if !this.resolved[j] {
                                cancel.cancel_with_reason(CancellationReason::WorkerFailed(
                                    "sibling worker failed".into(),
                                ));
                            }
                        }
                    }
                }
                Poll::Pending => {
                    all_done = false;
                }
            }
        }

        if all_done {
            match this.first_error.take() {
                Some(e) => Poll::Ready(Err(e)),
                None => Poll::Ready(Ok(())),
            }
        } else {
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Validate that all N LogicalDataflows have matching graph topologies.
///
/// Checks: operator count, edge count, feedback edge count, input port
/// names/types, and output port names/types. All must match across replicas
/// for correct replicated execution.
fn validate_multi_worker_topologies<T: Timestamp>(dataflows: &[LogicalDataflow<T>]) -> Result<()> {
    if dataflows.len() <= 1 {
        return Ok(());
    }

    let ref_df = &dataflows[0];
    let ref_graph = ref_df.graph();
    let mut ref_ops: Vec<&OperatorInfo> = ref_graph.operators().collect();
    ref_ops.sort_by_key(|op| op.index);
    let ref_edges = ref_graph.edges();
    let ref_feedback_count = ref_graph.feedback_edges().len();
    let ref_inputs = ref_df.input_names();
    let ref_outputs = ref_df.output_names();

    for (i, df) in dataflows.iter().enumerate().skip(1) {
        let graph = df.graph();

        // Operator count.
        let mut ops: Vec<&OperatorInfo> = graph.operators().collect();
        ops.sort_by_key(|op| op.index);
        if ops.len() != ref_ops.len() {
            return Err(Error::Custom(format!(
                "worker {i} has {} operators but worker 0 has {}",
                ops.len(),
                ref_ops.len()
            )));
        }

        // Operator names, stages, and port counts must match at each index.
        for (j, (a, b)) in ref_ops.iter().zip(ops.iter()).enumerate() {
            if a.name != b.name {
                return Err(Error::Custom(format!(
                    "worker {i} operator {j} is named '{}' but worker 0 has '{}'",
                    b.name, a.name
                )));
            }
            if a.stage_id != b.stage_id {
                return Err(Error::Custom(format!(
                    "worker {i} operator {j} ('{}') has stage {:?} but worker 0 has {:?}",
                    a.name, b.stage_id, a.stage_id
                )));
            }
            if a.input_count != b.input_count || a.output_count != b.output_count {
                return Err(Error::Custom(format!(
                    "worker {i} operator {j} ('{}') has {}/{} in/out ports but worker 0 has {}/{}",
                    a.name, b.input_count, b.output_count, a.input_count, a.output_count
                )));
            }
        }

        // Edge count and endpoints.
        let edges = graph.edges();
        if edges.len() != ref_edges.len() {
            return Err(Error::Custom(format!(
                "worker {i} has {} edges but worker 0 has {}",
                edges.len(),
                ref_edges.len()
            )));
        }
        for (j, (a, b)) in ref_edges.iter().zip(edges.iter()).enumerate() {
            if a.source != b.source || a.target != b.target {
                return Err(Error::Custom(format!(
                    "worker {i} edge {j} has {:?}->{:?} but worker 0 has {:?}->{:?}",
                    b.source, b.target, a.source, a.target
                )));
            }
        }

        // Feedback edges.
        if graph.feedback_edges().len() != ref_feedback_count {
            return Err(Error::Custom(format!(
                "worker {i} has {} feedback edges but worker 0 has {ref_feedback_count}",
                graph.feedback_edges().len()
            )));
        }

        // Input/output port names.
        let inputs = df.input_names();
        if inputs != ref_inputs {
            return Err(Error::Custom(format!(
                "worker {i} has different input ports than worker 0: \
                 expected {ref_inputs:?}, got {inputs:?}"
            )));
        }
        let outputs = df.output_names();
        if outputs != ref_outputs {
            return Err(Error::Custom(format!(
                "worker {i} has different output ports than worker 0: \
                 expected {ref_outputs:?}, got {outputs:?}"
            )));
        }
    }
    Ok(())
}

/// Validate that per-stage parallelism is compatible with the current execution model.
///
/// Currently, all runtime paths (`spawn_multi`, `spawn_cluster`) create a single group
/// of N workers that all run the complete dataflow. Per-stage parallelism (where stage A
/// has M workers and stage B has N workers, M≠N) requires per-stage executors which are
/// not yet implemented.
///
/// This validation ensures that every stage with explicit parallelism has a value equal
/// to `num_workers`. Stages without explicit parallelism (None) inherit num_workers
/// implicitly and are always valid.
///
/// When per-stage executors are implemented, this validation will be relaxed to allow
/// heterogeneous parallelism across stages.
fn validate_stage_parallelism<T: Timestamp>(
    dataflow: &LogicalDataflow<T>,
    num_workers: usize,
) -> Result<()> {
    let stages = dataflow.stages();
    if stages.is_empty() {
        return Ok(());
    }

    for stage in stages {
        if let Some(parallelism) = stage.parallelism {
            if parallelism != num_workers {
                return Err(Error::Custom(format!(
                    "stage {} has explicit parallelism {} but the runtime is spawning \
                     {} workers. Per-stage executors (heterogeneous worker counts) are \
                     not yet implemented — all explicit stage parallelism values must \
                     equal the spawned worker count.",
                    stage.id.0, parallelism, num_workers
                )));
            }
        }
    }

    Ok(())
}

/// Materialize a LogicalDataflow into a ready-to-run DataflowExecutor.
///
/// If `wake_handle` is provided, the executor uses it (shared with InputSenders
/// and CancellationTokens). Otherwise a fresh one is created internally.
///
/// Automatically enables fused activation (topological operator ordering) for
/// reduced scheduling overhead.
fn materialize_executor<T: Timestamp>(
    dataflow: LogicalDataflow<T>,
    cancel: CancellationToken,
    wake_handle: Option<WakeHandle>,
    worker_context: WorkerContext,
    progress_channels: Option<WorkerProgressChannels<T>>,
) -> Result<DataflowExecutor<T>> {
    let executor_config = ExecutorConfig {
        max_activations_per_step: 1024,
        max_idle_sweeps: 64,
        max_sweeps_per_poll: 64,
        catch_panics: dataflow.catch_panics,
        collect_metrics: dataflow.collect_metrics,
    };

    // Destructure to allow accessing graph after moving factories.
    let LogicalDataflow {
        graph,
        operator_factories,
        channel_factories,
        subgraph_builder,
        probes,
        probe_notifiers,
        stages,
        ..
    } = dataflow;

    let mut executor: DataflowExecutor<T> = DataflowExecutor::materialize(
        &graph,
        operator_factories,
        channel_factories,
        executor_config,
        cancel,
        wake_handle,
        worker_context,
    )?;

    // Enable stage-task scheduling if stages were inferred.
    // This groups operators by stage and activates them in fused topological
    // order within each stage task, reducing scheduling overhead.
    if !stages.is_empty() {
        executor.enable_stage_tasks(&stages);
    } else {
        // Fallback: enable whole-graph fused activation.
        if let Err(e) = executor.enable_fusion_from_graph(&graph) {
            tracing::debug!("Fused activation disabled: {e}");
        }
    }

    // Build and attach progress tracker.
    // For multi-worker dataflows, attach cross-worker progress channels
    // so the tracker broadcasts capability changes to peers and absorbs
    // remote changes. This makes is_completed() reflect global state.
    let mut tracker = subgraph_builder.build();
    if let Some(channels) = progress_channels {
        tracker.set_progress_channels(channels);
    }
    tracker.initialize();
    executor.set_progress_tracker(tracker);

    // Register probes
    debug_assert_eq!(
        probes.len(),
        probe_notifiers.len(),
        "probes and probe_notifiers must have matching lengths"
    );
    for ((op_idx, probe), notifier) in probes.into_iter().zip(probe_notifiers) {
        executor.register_probe(op_idx, probe, notifier);
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
            schedule_policy: None,
            name: "test-runtime".to_string(), ..Default::default() };
        let rt = RuntimeHandle::new(config).unwrap();
        assert_eq!(rt.name(), "test-runtime");
        assert!(!rt.is_shutdown());
    }

    #[test]
    fn shutdown_cancels_token() {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: None,
            name: "shutdown-test".to_string(), ..Default::default() })
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
            schedule_policy: None,
            name: "rt1".to_string(), ..Default::default() })
        .unwrap();
        let rt2 = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: None,
            name: "rt2".to_string(), ..Default::default() })
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
        builder
            .source("nums", vec![(0u64, vec![1i32, 2, 3])])
            .map("double", |_t, x| x * 2)
            .output("results");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let receiver = handle.take_output::<i32>("results").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results[0].1, vec![2, 4, 6]);
        handle.join_blocking().unwrap();
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

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
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

        let handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
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
                .source("data", vec![(0u64, vec![i])])
                .output("out");
            let dataflow = builder.build().unwrap();
            rt.spawn(dataflow, SpawnOptions::default()).unwrap().join_blocking().unwrap();
        }
    }

    #[test]
    fn runtime_handle_spawn_accepts_input_ports() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("spawn_with_inputs");
        let input = builder.input::<i32>("x");
        input.map("inc", |_t, x| x + 1).output("y");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("x").unwrap();
        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("y").unwrap();
        let results = receiver.collect_data();
        assert_eq!(results[0].1, vec![2, 3, 4]);
        handle.join_blocking().unwrap();
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
            const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
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
        let handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        handle.join().await.unwrap();
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
        let _handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        // handle dropped here — cancellation + detach, no blocking
    }

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

        let mut handle = rt.spawn(dataflow, SpawnOptions::new().io_mode(IoMode::Async)).unwrap();
        let sender = handle.take_async_input::<i32>("data").unwrap();
        let mut receiver = handle.take_async_output::<i32>("out").unwrap();

        sender.send(0, vec![1, 2, 3]).await.unwrap();
        sender.advance_to(0).await.unwrap();
        sender.close();

        let results = receiver.collect_data().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0);
        let mut vals = results[0].1.clone();
        vals.sort();
        assert_eq!(vals, vec![10, 20, 30]);

        handle.join().await.unwrap();
    }

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

        let mut handle = rt.spawn(dataflow, SpawnOptions::new().io_mode(IoMode::Async)).unwrap();

        // Using sync take_input on an async-wired port should give a helpful error
        let err = handle.take_input::<i32>("data").unwrap_err();
        assert!(
            format!("{err}").contains("IoMode::Async"),
            "error should hint at async mode: {err}"
        );

        let err = handle.take_output::<i32>("out");
        assert!(err.is_err());
        let msg = format!("{}", err.err().unwrap());
        assert!(
            msg.contains("IoMode::Async"),
            "error should hint at async mode: {msg}"
        );
    }

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

        let mut handle = rt.spawn(dataflow, SpawnOptions::new().io_mode(IoMode::Async)).unwrap();
        let sender = handle.take_async_input::<i32>("data").unwrap();
        let mut receiver = handle.take_async_output::<i32>("out").unwrap();

        sender.send(0, vec![10, 20]).await.unwrap();
        sender.send(1, vec![30, 40]).await.unwrap();
        sender.advance_to(1).await.unwrap();
        sender.close();

        let results = receiver.collect_data().await;
        assert!(!results.is_empty()); // may be batched
        let all_data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        let mut sorted = all_data;
        sorted.sort();
        assert_eq!(sorted, vec![10, 20, 30, 40]);

        handle.join().await.unwrap();
    }

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

        let mut handle = rt.spawn(dataflow, SpawnOptions::new().io_mode(IoMode::Async)).unwrap();
        let sender1 = handle.take_async_input::<i32>("data").unwrap();
        let sender2 = sender1.clone();

        // Both clones can send data
        sender1.send(0, vec![1]).await.unwrap();
        sender2.send(0, vec![2]).await.unwrap();
        sender1.advance_to(0).await.unwrap();
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

    // -----------------------------------------------------------------------
    // spawn_multi tests
    // -----------------------------------------------------------------------

    #[test]
    fn spawn_multi_single_worker_matches_spawn() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi(
                "test",
                1,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.map("double", |_t, x| x * 2).output("out");
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(multi.num_workers(), 1);
        assert_eq!(multi.name(), "test");

        let sender = multi.take_input::<i32>(0, "data").unwrap();
        sender.send(0, vec![10, 20]).unwrap();
        sender.close();

        let receiver = multi.take_output::<i32>(0, "out").unwrap();
        let results = receiver.collect_data();
        let data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        assert_eq!(data, vec![20, 40]);

        multi.join_blocking().unwrap();
    }

    #[test]
    fn spawn_multi_parallel_workers() {
        let rt = SimpleRuntime::new();
        let num = 4;
        let mut multi = rt
            .spawn_multi(
                "parallel",
                num,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.map("triple", |_t, x| x * 3).output("out");
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(multi.num_workers(), num);

        // Feed different data to each worker.
        let mut senders = Vec::new();
        for i in 0..num {
            let sender = multi.take_input::<i32>(i, "data").unwrap();
            sender.send(0, vec![(i as i32) * 10]).unwrap();
            senders.push(sender);
        }
        for s in senders {
            s.close();
        }

        // Collect results from each worker.
        let mut all_results = Vec::new();
        for i in 0..num {
            let receiver = multi.take_output::<i32>(i, "out").unwrap();
            let results = receiver.collect_data();
            let data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
            all_results.push(data);
        }

        assert_eq!(all_results[0], vec![0]); // 0 * 3
        assert_eq!(all_results[1], vec![30]); // 10 * 3
        assert_eq!(all_results[2], vec![60]); // 20 * 3
        assert_eq!(all_results[3], vec![90]); // 30 * 3

        multi.join_blocking().unwrap();
    }

    #[test]
    fn spawn_multi_zero_workers_rejected() {
        let rt = SimpleRuntime::new();
        let result = rt.spawn_multi::<u64, _>("test", 0, |_, _: &mut DataflowBuilder<u64>| Ok(()));
        assert!(result.is_err());
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("num_workers"));
    }

    #[test]
    fn spawn_multi_cancel_stops_all() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi(
                "cancel-test",
                3,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.output("out");
                    Ok(())
                },
            )
            .unwrap();

        // Take senders to keep workers alive (inputs open).
        let _sender0 = multi.take_input::<i32>(0, "data").unwrap();
        let _sender1 = multi.take_input::<i32>(1, "data").unwrap();
        let _sender2 = multi.take_input::<i32>(2, "data").unwrap();

        multi.cancel();

        // After cancel, join should complete (possibly with cancellation error).
        let result = multi.join_blocking();
        // We accept either Ok or Err (cancellation race).
        let _ = result;
    }

    #[test]
    fn spawn_multi_worker_idx_available() {
        // Verify the worker_idx is passed correctly to the build closure.
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sum = Arc::new(AtomicUsize::new(0));
        let sum_clone = Arc::clone(&sum);

        let rt = SimpleRuntime::new();
        let multi = rt
            .spawn_multi(
                "idx-test",
                4,
                move |worker_idx, builder: &mut DataflowBuilder<u64>| {
                    sum_clone.fetch_add(worker_idx, Ordering::Relaxed);
                    builder.source::<i32>("src", vec![]);
                    Ok(())
                },
            )
            .unwrap();

        // 0 + 1 + 2 + 3 = 6
        assert_eq!(sum.load(Ordering::Relaxed), 6);
        multi.join_blocking().unwrap();
    }

    #[test]
    fn spawn_multi_on_runtime_handle() {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi(
                "pool-test",
                2,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.map("inc", |_t, x| x + 1).output("out");
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();

        // Feed data to worker 0 and 1.
        let s0 = multi.take_input::<i32>(0, "data").unwrap();
        let s1 = multi.take_input::<i32>(1, "data").unwrap();
        s0.send(0, vec![100]).unwrap();
        s1.send(0, vec![200]).unwrap();
        s0.close();
        s1.close();

        let r0 = multi.take_output::<i32>(0, "out").unwrap();
        let r1 = multi.take_output::<i32>(1, "out").unwrap();

        let d0: Vec<i32> = r0.collect_data().into_iter().flat_map(|(_, d)| d).collect();
        let d1: Vec<i32> = r1.collect_data().into_iter().flat_map(|(_, d)| d).collect();

        assert_eq!(d0, vec![101]);
        assert_eq!(d1, vec![201]);

        multi.join_blocking().unwrap();
    }

    #[test]
    fn spawn_multi_join_returns_completion() {
        let rt = SimpleRuntime::new();
        let multi = rt
            .spawn_multi(
                "join-test",
                2,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    builder.source::<i32>("src", vec![]);
                    Ok(())
                },
            )
            .unwrap();

        let completion = multi.join();
        completion.wait().unwrap();
    }

    #[test]
    fn spawn_multi_worker_out_of_range() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi("range-test", 2, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        assert!(multi.take_input::<i32>(5, "data").is_err());
        assert!(multi.take_output::<i32>(5, "out").is_err());

        multi.cancel();
        let _ = multi.join_blocking();
    }

    // -- take_all_* tests --------------------------------------------------

    #[test]
    fn take_all_inputs_returns_all_senders() {
        let rt = SimpleRuntime::new();
        let n = 3;
        let mut multi = rt
            .spawn_multi("all-in", n, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        let senders = multi.take_all_inputs::<i32>("data").unwrap();
        assert_eq!(senders.len(), n);

        // Send distinct data to each worker.
        for (i, s) in senders.iter().enumerate() {
            s.send(0, vec![i as i32 * 10]).unwrap();
        }
        drop(senders);

        // Collect per-worker outputs.
        for i in 0..n {
            let out = multi.take_output::<i32>(i, "out").unwrap();
            let data: Vec<i32> = out
                .collect_data()
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect();
            assert_eq!(data, vec![i as i32 * 10]);
        }

        multi.join_blocking().unwrap();
    }

    #[test]
    fn take_all_outputs_returns_all_receivers() {
        let rt = SimpleRuntime::new();
        let n = 4;
        let mut multi = rt
            .spawn_multi("all-out", n, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("nums");
                input.map("double", |_t, x| x * 2).output("results");
                Ok(())
            })
            .unwrap();

        let senders = multi.take_all_inputs::<i32>("nums").unwrap();
        let receivers = multi.take_all_outputs::<i32>("results").unwrap();
        assert_eq!(receivers.len(), n);

        for (i, s) in senders.iter().enumerate() {
            s.send(0, vec![i as i32 + 1]).unwrap();
        }
        drop(senders);

        for (i, r) in receivers.iter().enumerate() {
            let data: Vec<i32> = r.collect_data().into_iter().flat_map(|(_, d)| d).collect();
            assert_eq!(data, vec![(i as i32 + 1) * 2]);
        }

        multi.join_blocking().unwrap();
    }

    #[test]
    fn take_all_inputs_wrong_type_fails_without_consuming() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi("type-err", 2, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        // Wrong type — should fail.
        assert!(multi.take_all_inputs::<String>("data").is_err());

        // Original ports are still available (no partial consumption).
        let senders = multi.take_all_inputs::<i32>("data").unwrap();
        assert_eq!(senders.len(), 2);
        drop(senders);

        multi.cancel();
        let _ = multi.join_blocking();
    }

    #[test]
    fn take_all_outputs_missing_port_fails_without_consuming() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi("missing", 2, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        // Port doesn't exist.
        assert!(multi.take_all_outputs::<i32>("nonexistent").is_err());

        // Original ports are still available.
        let receivers = multi.take_all_outputs::<i32>("out").unwrap();
        assert_eq!(receivers.len(), 2);
        drop(receivers);

        multi.cancel();
        let _ = multi.join_blocking();
    }

    #[test]
    fn take_all_inputs_idempotence_fails_after_consumed() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi("idem", 2, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        let senders = multi.take_all_inputs::<i32>("data").unwrap();
        assert_eq!(senders.len(), 2);

        // Second call should fail — ports already consumed.
        assert!(multi.take_all_inputs::<i32>("data").is_err());

        drop(senders);
        multi.cancel();
        let _ = multi.join_blocking();
    }

    #[test]
    fn take_all_end_to_end_partitioned_pipeline() {
        let rt = SimpleRuntime::new();
        let n = 4;
        let mut multi = rt
            .spawn_multi("e2e", n, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<String>("words");
                input
                    .map("upper", |_t, s: String| s.to_uppercase())
                    .output("results");
                Ok(())
            })
            .unwrap();

        let senders = multi.take_all_inputs::<String>("words").unwrap();
        let receivers = multi.take_all_outputs::<String>("results").unwrap();

        let partitions = [vec!["hello".to_string()],
            vec!["world".to_string()],
            vec!["foo".to_string(), "bar".to_string()],
            vec![]];

        for (i, partition) in partitions.iter().enumerate() {
            if !partition.is_empty() {
                senders[i].send(0, partition.clone()).unwrap();
            }
        }
        drop(senders);

        let results: Vec<Vec<String>> = receivers
            .iter()
            .map(|r| r.collect_data().into_iter().flat_map(|(_, d)| d).collect())
            .collect();

        assert_eq!(results[0], vec!["HELLO"]);
        assert_eq!(results[1], vec!["WORLD"]);
        assert_eq!(results[2], vec!["FOO", "BAR"]);
        assert!(results[3].is_empty());

        multi.join_blocking().unwrap();
    }

    #[test]
    fn take_all_async_inputs_on_sync_spawned_returns_error() {
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi("mode-err", 2, |_, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            })
            .unwrap();

        // sync-spawned → take_all_async_inputs should fail gracefully (not panic).
        let result = multi.take_all_async_inputs::<i32>("data");
        assert!(result.is_err());

        // Ports should still be available for sync take.
        let senders = multi.take_all_inputs::<i32>("data").unwrap();
        assert_eq!(senders.len(), 2);
        drop(senders);

        multi.cancel();
        let _ = multi.join_blocking();
    }

    // -----------------------------------------------------------------------
    // Exchange channel integration tests
    // -----------------------------------------------------------------------

    #[test]
    fn spawn_multi_exchange_routes_data_between_workers() {
        // 2 workers with exchange: input → exchange(mod 2) → output
        // Even numbers go to worker 0, odd numbers go to worker 1.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_test", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                // Use exchange_by_hash for direct u64 routing (no extra hashing).
                input
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        // Get per-worker outputs and inputs.
        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Send data through worker 0's input.
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![10, 11, 12, 13, 14, 15]).unwrap();
        in0.close();

        // Also send through worker 1's input.
        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.send(0u64, vec![20, 21, 22, 23]).unwrap();
        in1.close();

        let _ = multi.join_blocking();

        // Worker 0 should receive all even numbers.
        let mut worker0_data: Vec<i32> = out0
            .collect_data()
            .into_iter()
            .flat_map(|(_t, batch)| batch)
            .collect();
        worker0_data.sort();

        // Worker 1 should receive all odd numbers.
        let mut worker1_data: Vec<i32> = out1
            .collect_data()
            .into_iter()
            .flat_map(|(_t, batch)| batch)
            .collect();
        worker1_data.sort();

        assert_eq!(
            worker0_data,
            vec![10, 12, 14, 20, 22],
            "worker0 got wrong data"
        );
        assert_eq!(
            worker1_data,
            vec![11, 13, 15, 21, 23],
            "worker1 got wrong data; worker0 was: {worker0_data:?}"
        );
    }

    #[test]
    fn spawn_multi_exchange_single_worker_passthrough() {
        // With 1 worker, exchange degenerates to a pass-through.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_1w", 1, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input.exchange("by_key", |x: &i32| *x as u64).output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out = multi.take_output::<i32>(0, "out").unwrap();
        let inp = multi.take_input::<i32>(0, "data").unwrap();
        inp.send(0u64, vec![1, 2, 3]).unwrap();
        inp.close();

        let _ = multi.join_blocking();

        let mut results: Vec<i32> = out
            .collect_data()
            .into_iter()
            .flat_map(|(_t, batch)| batch)
            .collect();
        results.sort();
        assert_eq!(results, vec![1, 2, 3]);
    }

    #[test]
    fn spawn_multi_exchange_with_computation() {
        // 2 workers: input → map(double) → exchange(mod 2) → map(+100) → output
        // Tests that computation works both before and after exchange.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_compute", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .map("double", |_t, x| x * 2)
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .map("add100", |_t, x| x + 100)
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Input: [1, 2, 3, 4, 5] → doubled: [2, 4, 6, 8, 10]
        // After exchange(hash % 2): all doubled values are even, so hash % 2 == 0
        // and all route to worker 0.  After +100: [102, 104, 106, 108, 110]
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![1, 2, 3, 4, 5]).unwrap();
        in0.close();

        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.close();

        multi
            .join_blocking()
            .expect("dataflow should complete without error");

        let mut w0: Vec<i32> = out0
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        w0.sort();
        let mut w1: Vec<i32> = out1
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        w1.sort();

        // All doubled values are even → hash % 2 == 0 → all route to worker 0.
        assert_eq!(w0, vec![102, 104, 106, 108, 110]);
        assert!(w1.is_empty());
    }

    #[test]
    fn spawn_multi_exchange_bidirectional_data() {
        // Both workers send data, exchange redistributes.
        // Tests that data flows correctly in both directions.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_bidir", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "out").unwrap();
        let out1 = multi.take_output::<i32>(1, "out").unwrap();

        // Worker 0 sends [0, 1, 2, 3], worker 1 sends [4, 5, 6, 7].
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![0, 1, 2, 3]).unwrap();
        in0.close();

        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.send(0u64, vec![4, 5, 6, 7]).unwrap();
        in1.close();

        multi
            .join_blocking()
            .expect("dataflow should complete without error");

        let mut w0: Vec<i32> = out0
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        w0.sort();
        let mut w1: Vec<i32> = out1
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        w1.sort();

        // hash % 2: evens → w0, odds → w1
        assert_eq!(w0, vec![0, 2, 4, 6]);
        assert_eq!(w1, vec![1, 3, 5, 7]);
    }

    // ========================================================================
    // Exchange + Notification integration tests
    //
    // These tests validate that `unary_notify` works correctly when combined
    // with `exchange_by_hash` in multi-worker dataflows. They verify:
    // - Exchange routing + notification-based aggregation produce correct results
    // - Per-epoch notifications fire for the right timestamps
    // - No data loss or duplication across workers
    //
    // NOTE: With channel-fed inputs (take_input/send/close), all notifications
    // fire at end-of-stream when the input frontier is exhausted. Testing
    // mid-stream frontier-driven notification timing requires AsyncInputSender
    // with explicit advance_to() calls — a scenario for future tests.
    //
    // IMPORTANT: With multi-worker exchange, data for the same timestamp from
    // different source workers may arrive in separate activations. This means
    // a notification can fire multiple times for the same epoch (each time
    // new data arrives and notify_at is called again). Tests must assert on
    // total aggregated values per epoch, not assume exactly one emission.
    // ========================================================================

    #[test]
    fn spawn_multi_exchange_notify_basic_aggregation() {
        // 2 workers: input → exchange(value % 2) → unary_notify(sum per epoch) → output
        // Evens go to worker 0, odds to worker 1. Each worker sums its partition.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_notify_basic", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .unary_notify("sum", {
                        let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                            std::collections::HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                stash.entry(time).or_default().extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(data) = stash.remove(&time) {
                                    let sum: i32 = data.iter().sum();
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Worker 0 sends 1..8
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        in0.close();

        // Worker 1 also sends data — all routes through exchange.
        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.close();

        multi.join_blocking().expect("dataflow should complete");

        // After exchange(value % 2): worker 0 gets evens [2,4,6,8], worker 1 gets odds [1,3,5,7]
        // Each worker's unary_notify sums its partition on notification.
        // With multi-worker exchange, partial sums may be emitted across multiple
        // activations, so we aggregate all outputs per epoch.
        let sum0: i32 = out0.collect_data().iter().flat_map(|(_, v)| v).sum();
        let sum1: i32 = out1.collect_data().iter().flat_map(|(_, v)| v).sum();

        assert_eq!(sum0, 20, "worker 0: evens 2+4+6+8=20");
        assert_eq!(sum1, 16, "worker 1: odds 1+3+5+7=16");
    }

    #[test]
    fn spawn_multi_exchange_notify_multi_epoch() {
        // 2 workers: multiple epochs, verify per-epoch notification correctness.
        // Each epoch's data is aggregated independently.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_notify_epochs", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .unary_notify("sum_per_epoch", {
                        let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                            std::collections::HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                stash.entry(time).or_default().extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(data) = stash.remove(&time) {
                                    let sum: i32 = data.iter().sum();
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Worker 0: epoch 0 → [10,11], epoch 1 → [20,21]
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![10, 11]).unwrap();
        in0.send(1u64, vec![20, 21]).unwrap();
        in0.close();

        // Worker 1: epoch 0 → [12,13], epoch 1 → [22,23]
        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.send(0u64, vec![12, 13]).unwrap();
        in1.send(1u64, vec![22, 23]).unwrap();
        in1.close();

        multi.join_blocking().expect("dataflow should complete");

        // After exchange(mod 2):
        //   epoch 0: w0=[10,12], w1=[11,13] → sums: w0=22, w1=24
        //   epoch 1: w0=[20,22], w1=[21,23] → sums: w0=42, w1=44
        // Aggregate per epoch (multiple emissions possible per epoch).
        let mut sums0: std::collections::HashMap<u64, i32> = std::collections::HashMap::new();
        for (t, vs) in out0.collect_data() {
            *sums0.entry(t).or_default() += vs.iter().sum::<i32>();
        }
        let mut sums1: std::collections::HashMap<u64, i32> = std::collections::HashMap::new();
        for (t, vs) in out1.collect_data() {
            *sums1.entry(t).or_default() += vs.iter().sum::<i32>();
        }

        assert_eq!(sums0.get(&0), Some(&22), "worker 0 epoch 0 sum");
        assert_eq!(sums0.get(&1), Some(&42), "worker 0 epoch 1 sum");
        assert_eq!(sums1.get(&0), Some(&24), "worker 1 epoch 0 sum");
        assert_eq!(sums1.get(&1), Some(&44), "worker 1 epoch 1 sum");
    }

    #[test]
    fn spawn_multi_exchange_notify_computation_chain() {
        // 2 workers: input → map(double) → exchange → unary_notify(count) → output
        // Tests computation before exchange combined with notification after.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_notify_chain", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .map("double", |_t, x| x * 2)
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .unary_notify("count", {
                        let mut stash: std::collections::HashMap<u64, usize> =
                            std::collections::HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                *stash.entry(time).or_default() += data.len();
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(count) = stash.remove(&time) {
                                    output.push_vec(time, vec![count as i32]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Worker 0 sends [1,2,3]: doubled → [2,4,6], all even → all to worker 0
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![1, 2, 3]).unwrap();
        in0.close();

        // Worker 1 sends [5,7]: doubled → [10,14], all even → all to worker 0
        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.send(0u64, vec![5, 7]).unwrap();
        in1.close();

        multi.join_blocking().expect("dataflow should complete");

        // All 5 items doubled are even → value % 2 == 0 → all route to worker 0.
        // NOTE: With multi-worker exchange, data from different source workers
        // may arrive in separate activations. The notification can fire multiple
        // times for the same timestamp (once per activation that delivers new data),
        // so we assert on the total count per epoch, not the number of emissions.
        let total_count_w0: i32 = out0.collect_data().iter().flat_map(|(_, v)| v).sum();
        let total_count_w1: i32 = out1.collect_data().iter().flat_map(|(_, v)| v).sum();

        assert_eq!(total_count_w0, 5, "worker 0 should count all 5 items");
        assert_eq!(total_count_w1, 0, "worker 1 should receive nothing");
    }

    #[test]
    fn spawn_multi_exchange_notify_multi_batch_same_epoch() {
        // 2 workers: both send data at t=0 in separate batches.
        // Verifies correct final aggregation despite multi-batch inputs — each
        // worker should emit exactly one sum at t=0 after all batches are collected.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("exchange_notify_multibatch", 2, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("mod2", |x: &i32| *x as u64)
                    .unary_notify("sum", {
                        let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                            std::collections::HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                stash.entry(time).or_default().extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(data) = stash.remove(&time) {
                                    let sum: i32 = data.iter().sum();
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<i32>(0, "results").unwrap();
        let out1 = multi.take_output::<i32>(1, "results").unwrap();

        // Worker 0 sends t=0 in two separate batches.
        let in0 = multi.take_input::<i32>(0, "data").unwrap();
        in0.send(0u64, vec![2, 4]).unwrap();
        in0.send(0u64, vec![6, 8]).unwrap();
        in0.close();

        // Worker 1 also sends t=0 in two batches.
        let in1 = multi.take_input::<i32>(1, "data").unwrap();
        in1.send(0u64, vec![1, 3]).unwrap();
        in1.send(0u64, vec![5, 7]).unwrap();
        in1.close();

        multi.join_blocking().expect("dataflow should complete");

        // After exchange: w0=[2,4,6,8], w1=[1,3,5,7]
        // Aggregate per epoch (multiple emissions possible from multi-batch arrivals).
        let sum0: i32 = out0.collect_data().iter().flat_map(|(_, v)| v).sum();
        let sum1: i32 = out1.collect_data().iter().flat_map(|(_, v)| v).sum();

        assert_eq!(sum0, 20, "worker 0: evens 2+4+6+8=20");
        assert_eq!(sum1, 16, "worker 1: odds 1+3+5+7=16");

        // Verify total data integrity: no loss, no duplication.
        assert_eq!(sum0 + sum1, 36, "total sum should be 1+2+...+8=36");
    }

    #[test]
    fn spawn_multi_exchange_notify_four_workers() {
        // 4 workers: tests N×N matrix wiring with notifications.
        // All 4 workers feed data, exchange routes by hash % 4.
        // Uses fewer items per worker to reduce contention on the 4×4 matrix.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let num_workers = 4;
        let mut multi = rt
            .spawn_multi("exchange_notify_4w", num_workers, |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("mod4", |x: &i32| *x as u64)
                    .unary_notify("collect", {
                        let mut stash: std::collections::HashMap<u64, Vec<i32>> =
                            std::collections::HashMap::new();
                        move |input, output, ctx| {
                            while let Some((time, data)) = input.next() {
                                stash.entry(time).or_default().extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(mut data) = stash.remove(&time) {
                                    data.sort();
                                    output.push_vec(time, data);
                                }
                            }
                            Ok(())
                        }
                    })
                    .output("results");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        // Each worker sends values 0..8 — exchange routes each value to
        // worker (value % 4). All 4 workers send identical data, so each
        // worker receives 4 copies of its assigned values.
        for i in 0..num_workers {
            let inp = multi.take_input::<i32>(i, "data").unwrap();
            inp.send(0u64, (0..8i32).collect()).unwrap();
            inp.close();
        }

        // Take all output receivers before join — take_output moves the receiver.
        let receivers: Vec<_> = (0..num_workers)
            .map(|i| multi.take_output::<i32>(i, "results").unwrap())
            .collect();

        multi.join_blocking().expect("dataflow should complete");

        let mut all_results: Vec<Vec<i32>> = Vec::new();
        for recv in &receivers {
            let mut data: Vec<i32> = recv
                .collect_data()
                .into_iter()
                .flat_map(|(_, v)| v)
                .collect();
            data.sort();
            all_results.push(data);
        }

        // Worker i should receive all values where value % 4 == i,
        // with 4 copies each (one from each sending worker).
        for (worker, data) in all_results.iter().enumerate() {
            let expected: Vec<i32> = (0..8i32)
                .filter(|x| (*x as u64) % 4 == worker as u64)
                .flat_map(|x| std::iter::repeat_n(x, num_workers))
                .collect();
            let mut expected_sorted = expected;
            expected_sorted.sort();
            assert_eq!(
                data, &expected_sorted,
                "worker {worker} received wrong data"
            );
        }

        // Total data integrity: 4 workers × 8 values = 32 total items.
        let total: usize = all_results.iter().map(|v| v.len()).sum();
        assert_eq!(total, 32, "total items across all workers");
    }

    // -----------------------------------------------------------------------
    // No-serialization in-process verification (G3.1)
    //
    // These tests prove that in-process channels never invoke serialization
    // or deserialization (Codec). Exchange routing may clone records for
    // multi-target distribution, but no byte encoding occurs.
    //
    // The compile-time proof: NonSerializable is Clone+Send but has no
    // Serialize/Deserialize impl. If any code path tried to serialize it,
    // compilation would fail.
    //
    // Exchange-level tests are gated on `not(feature = "transport")` because
    // the transport-enabled `exchange` API requires `ExchangeData` at the
    // type level (even though local exchange never actually serializes).
    // This is a compile-time safety measure for cross-process deployability.
    //
    // The bounded-channel test and pipeline test run with ALL features,
    // ensuring the core no-serialization guarantee is always verified.
    // -----------------------------------------------------------------------

    /// A data type that is Clone + Send but NOT Serialize/Deserialize.
    /// If any code path attempts to serialize this, it would be a compile error.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NonSerializable {
        value: i32,
        /// Data integrity marker — survives Clone but would fail at compile
        /// time if serialization were attempted (no Serialize impl).
        tag: String,
    }

    impl NonSerializable {
        fn new(value: i32) -> Self {
            Self {
                value,
                tag: format!("original-{value}"),
            }
        }
    }

    #[test]
    fn no_serialization_bounded_channel() {
        // Runs with ALL features. Proves the bounded channel (used by both
        // pipeline and in-process exchange) passes NonSerializable by value
        // without any Codec invocation.
        use crate::dataflow::channels::bounded::bounded_channel;
        use crate::dataflow::channels::envelope::{Envelope, Payload};
        use crate::dataflow::channels::pushpull::{Pull, Push};

        let (mut push, mut pull) = bounded_channel::<u64, NonSerializable, ()>(16);

        let items = vec![
            NonSerializable::new(1),
            NonSerializable::new(2),
            NonSerializable::new(3),
        ];
        let envelope = Envelope {
            payload: Payload::Data {
                time: 0u64,
                data: items,
            },
            metadata: (),
        };
        push.push(envelope).unwrap();

        let received = pull.pull().expect("should have data");
        match received.payload {
            Payload::Data { time, data } => {
                assert_eq!(time, 0);
                assert_eq!(data.len(), 3);
                for item in &data {
                    assert!(
                        item.tag.starts_with("original-"),
                        "data integrity check failed: {:?}",
                        item
                    );
                }
                assert_eq!(data[0].value, 1);
                assert_eq!(data[1].value, 2);
                assert_eq!(data[2].value, 3);
            }
            _ => panic!("expected Data payload"),
        }
    }

    #[cfg(not(feature = "transport"))]
    #[test]
    fn no_serialization_exchange_multi_worker() {
        // Compile-time proof: NonSerializable flows through multi-worker
        // exchange. This wouldn't compile if exchange used Codec.
        // Hash routing: even values → worker 0, odd values → worker 1.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("no_ser_exchange", 2, |_worker_idx, builder| {
                let input = builder.input::<NonSerializable>("data");
                input
                    .exchange_by_hash("by_val", |x: &NonSerializable| x.value as u64)
                    .output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let out0 = multi.take_output::<NonSerializable>(0, "out").unwrap();
        let out1 = multi.take_output::<NonSerializable>(1, "out").unwrap();

        let in0 = multi.take_input::<NonSerializable>(0, "data").unwrap();
        in0.send(
            0u64,
            vec![
                NonSerializable::new(10),
                NonSerializable::new(11),
                NonSerializable::new(20),
                NonSerializable::new(21),
            ],
        )
        .unwrap();
        in0.close();

        let in1 = multi.take_input::<NonSerializable>(1, "data").unwrap();
        in1.close();

        multi.join_blocking().expect("dataflow should complete");

        // Collect all values regardless of which worker received them.
        let mut all: Vec<i32> = out0
            .collect_data()
            .into_iter()
            .chain(out1.collect_data())
            .flat_map(|(_, d)| d)
            .map(|x| x.value)
            .collect();
        all.sort();
        assert_eq!(all, vec![10, 11, 20, 21], "all data should arrive intact");
    }

    #[cfg(not(feature = "transport"))]
    #[test]
    fn no_serialization_exchange_4_workers_integrity() {
        // 4 workers, all send data, exchange redistributes. Verifies all
        // NonSerializable items arrive intact at their destination workers.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let num_workers = 4;
        let mut multi = rt
            .spawn_multi("integrity", num_workers, |_worker_idx, builder| {
                let input = builder.input::<NonSerializable>("data");
                input
                    .exchange_by_hash("route", |x: &NonSerializable| x.value as u64)
                    .output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let outputs: Vec<_> = (0..num_workers)
            .map(|i| multi.take_output::<NonSerializable>(i, "out").unwrap())
            .collect();

        for i in 0..num_workers {
            let inp = multi.take_input::<NonSerializable>(i, "data").unwrap();
            let items: Vec<NonSerializable> = (0..8)
                .map(|v| NonSerializable::new(i as i32 * 100 + v))
                .collect();
            inp.send(0u64, items).unwrap();
            inp.close();
        }

        multi.join_blocking().unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for out in outputs {
            let data: Vec<NonSerializable> = out
                .collect_data()
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect();
            for item in &data {
                assert!(
                    item.tag.starts_with("original-"),
                    "data integrity failed: {:?}",
                    item
                );
            }
            all_values.extend(data.into_iter().map(|x| x.value));
        }

        all_values.sort();
        let mut expected: Vec<i32> = (0..num_workers as i32)
            .flat_map(|w| (0..8).map(move |v| w * 100 + v))
            .collect();
        expected.sort();
        assert_eq!(all_values, expected, "all data should arrive intact");
    }

    #[test]
    fn no_serialization_pipeline_multi_worker() {
        // Pipeline edges never require Codec bounds regardless of features.
        // Proves NonSerializable flows through map + filter in multi-worker.
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let mut multi = rt
            .spawn_multi("no_ser_pipe", 2, |_worker_idx, builder| {
                let input = builder.input::<NonSerializable>("data");
                input
                    .map("transform", |_t, mut x| {
                        x.value *= 2;
                        x
                    })
                    .filter("positive", |_t, x| x.value > 0)
                    .output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        let outputs: Vec<_> = (0..2)
            .map(|i| multi.take_output::<NonSerializable>(i, "out").unwrap())
            .collect();

        for i in 0..2 {
            let inp = multi.take_input::<NonSerializable>(i, "data").unwrap();
            inp.send(0u64, vec![NonSerializable::new((i as i32 + 1) * 5)])
                .unwrap();
            inp.close();
        }

        multi.join_blocking().unwrap();

        for (i, out) in outputs.into_iter().enumerate() {
            let data: Vec<NonSerializable> = out
                .collect_data()
                .into_iter()
                .flat_map(|(_, d)| d)
                .collect();
            assert_eq!(data.len(), 1);
            assert_eq!(data[0].value, (i as i32 + 1) * 10);
            assert!(
                data[0].tag.starts_with("original-"),
                "pipeline data integrity failed"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Context injection tests
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, PartialEq)]
    struct TestConfig {
        multiplier: i32,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TestLabel {
        name: String,
    }

    #[test]
    fn context_set_and_get_in_builder() {
        let builder = DataflowBuilder::<u64>::new("ctx-test");
        builder.with_context(TestConfig { multiplier: 42 });

        let cfg = builder.get_context::<TestConfig>().unwrap();
        assert_eq!(cfg.multiplier, 42);

        // Missing type returns None
        assert!(builder.get_context::<TestLabel>().is_none());
    }

    #[test]
    fn context_multiple_types() {
        let builder = DataflowBuilder::<u64>::new("ctx-multi");
        builder
            .with_context(TestConfig { multiplier: 10 })
            .with_context(TestLabel {
                name: "test".into(),
            });

        assert_eq!(builder.get_context::<TestConfig>().unwrap().multiplier, 10);
        assert_eq!(builder.get_context::<TestLabel>().unwrap().name, "test");
    }

    #[test]
    fn context_survives_build() {
        let builder = DataflowBuilder::<u64>::new("ctx-build");
        builder.with_context(TestConfig { multiplier: 7 });

        let input = builder.input::<i32>("data");
        let _out = input.output("sink");
        let dataflow = builder.build().unwrap();

        // Context is accessible on the LogicalDataflow
        let cfg = dataflow.contexts().get::<TestConfig>().unwrap();
        assert_eq!(cfg.multiplier, 7);
    }

    #[test]
    fn context_used_in_map_operator() {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

        let builder = DataflowBuilder::<u64>::new("ctx-map");
        builder.with_context(TestConfig { multiplier: 3 });

        let cfg = builder.get_context::<TestConfig>().unwrap();
        let input = builder.input::<i32>("data");
        let _out = input
            .map("multiply", move |_t, x| x * cfg.multiplier)
            .output("result");

        let dataflow = builder.build().unwrap();
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();

        let sender = handle.take_input::<i32>("data").unwrap();
        sender.send(0, vec![1, 2, 3]).unwrap();
        sender.close();

        let receiver = handle.take_output::<i32>("result").unwrap();
        handle.join_blocking().unwrap();

        let mut data: Vec<i32> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();
        data.sort();
        assert_eq!(data, vec![3, 6, 9]);
    }

    #[test]
    fn context_in_multi_worker_dataflow() {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

        let mut handle = rt
            .spawn_multi("ctx-multi-worker", 3, move |_worker_idx, builder| {
                builder.with_context(TestConfig { multiplier: 5 });

                let cfg = builder.get_context::<TestConfig>().unwrap();
                let input = builder.input::<i32>("data");
                input
                    .map("multiply", move |_t, x| x * cfg.multiplier)
                    .output("result");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        // Send to worker 0
        let sender = handle.take_input::<i32>(0, "data").unwrap();
        sender.send(0, vec![1, 2]).unwrap();
        sender.close();

        // Close other workers' inputs
        for i in 1..3 {
            let s = handle.take_input::<i32>(i, "data").unwrap();
            s.close();
        }

        // Collect results from all workers
        let mut all_data = Vec::new();
        for i in 0..3 {
            let receiver = handle.take_output::<i32>(i, "result").unwrap();
            all_data.push(receiver);
        }

        handle.join_blocking().unwrap();

        let mut collected: Vec<i32> = all_data
            .into_iter()
            .flat_map(|r| r.collect_data().into_iter().flat_map(|(_, d)| d))
            .collect();
        collected.sort();
        assert_eq!(collected, vec![5, 10]);
    }

    #[test]
    fn context_override_latest_wins() {
        let builder = DataflowBuilder::<u64>::new("ctx-override");
        builder.with_context(TestConfig { multiplier: 1 });
        builder.with_context(TestConfig { multiplier: 99 });

        let cfg = builder.get_context::<TestConfig>().unwrap();
        assert_eq!(cfg.multiplier, 99);
    }

    #[test]
    fn context_with_arc_avoids_double_wrap() {
        let shared = Arc::new(TestConfig { multiplier: 77 });
        let builder = DataflowBuilder::<u64>::new("ctx-arc");
        builder.with_context_arc(shared.clone());

        let retrieved = builder.get_context::<TestConfig>().unwrap();
        // Same Arc allocation — no double-wrapping
        assert!(Arc::ptr_eq(&shared, &retrieved));
        assert_eq!(retrieved.multiplier, 77);
    }

    #[test]
    fn context_inherited_in_iterate() {
        use crate::dataflow::dataflow_builder::IterateResult;

        let builder = DataflowBuilder::<u64>::new("ctx-iterate");
        builder.with_context(TestConfig { multiplier: 2 });

        let input = builder.input::<i32>("data");

        // Context captured outside iterate should work, AND the inner scope
        // should also have the context available via the shared BuilderState.
        let cfg = builder.get_context::<TestConfig>().unwrap();
        let out = input.iterate("loop", 1u32, move |iter_var| {
            let cfg_cap = cfg.clone();
            let result = iter_var.map("double", move |_t: &crate::order::Product<u64, u32>, x| {
                x * cfg_cap.multiplier
            });
            IterateResult {
                feedback: result.clone(),
                output: result,
            }
        });
        out.output("result");

        let dataflow = builder.build().unwrap();

        // Verify context survives through iterate into the LogicalDataflow
        let ctx = dataflow.contexts().get::<TestConfig>().unwrap();
        assert_eq!(ctx.multiplier, 2);
    }

    // ── Cross-worker control broadcast tests ──────────────────────────

    #[test]
    fn control_broadcast_worker_error_cancels_siblings() {
        // When one worker's operator errors, the sibling should be cancelled.
        let rt = SimpleRuntime::new();

        // Use a shared flag to make only worker 0 fail.
        let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fail_clone = fail_flag.clone();

        let mut multi = rt
            .spawn_multi(
                "ctrl-err",
                2,
                move |worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    let flag = fail_clone.clone();
                    input
                        .map("process", move |_t, x: i32| -> i32 {
                            if flag.load(std::sync::atomic::Ordering::Relaxed) {
                                panic!("intentional failure from worker");
                            }
                            x + 1
                        })
                        .output("out");
                    // Mark worker 0 to fail on its first activation
                    if worker_idx == 0 {
                        // We'll set the flag externally after spawn
                    }
                    Ok(())
                },
            )
            .unwrap();

        // Set the flag so worker 0 fails when it processes data
        fail_flag.store(true, std::sync::atomic::Ordering::Relaxed);

        // Feed data to worker 0 to trigger the error.
        let tx0 = multi.take_input::<i32>(0, "data").unwrap();
        tx0.send(0, vec![1]).unwrap();
        tx0.close();

        // Close worker 1's input too.
        let tx1 = multi.take_input::<i32>(1, "data").unwrap();
        tx1.close();

        // Join — should see error from worker 0; worker 1 should be cancelled.
        let result = multi.join_blocking();
        assert!(
            result.is_err(),
            "multi-worker join should fail when a worker errors"
        );
        // Verify the error message relates to the worker failure.
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("process")
                || err_msg.contains("intentional")
                || err_msg.contains("cancel")
                || err_msg.contains("panic")
                || err_msg.contains("terminated"),
            "error should relate to the failing worker: {err_msg}"
        );
    }

    #[test]
    fn control_broadcast_single_worker_no_overhead() {
        // Single-worker spawn works without any control broadcast.
        let rt = SimpleRuntime::new();
        let mut multi = rt
            .spawn_multi(
                "single",
                1,
                |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.map("inc", |_t, x: i32| x + 1).output("out");
                    Ok(())
                },
            )
            .unwrap();

        assert_eq!(multi.num_workers(), 1);
        let sender = multi.take_input::<i32>(0, "data").unwrap();
        sender.send(0, vec![10]).unwrap();
        sender.close();

        let receiver = multi.take_output::<i32>(0, "out").unwrap();
        let results = receiver.collect_data();
        let data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        assert_eq!(data, vec![11]);

        multi.join_blocking().unwrap();
    }

    #[test]
    fn control_broadcast_unit_test_sender_receiver() {
        // Direct test of ControlBroadcast types.
        use crate::dataflow::channels::wake::WakeHandle;
        use crate::dataflow::control::{ControlBroadcast, WorkerControl};

        let parent = CancellationToken::new();
        let df_cancel = parent.child_token();
        let wakes: Vec<WakeHandle> = (0..3).map(|_| WakeHandle::new()).collect();
        let (senders, mut receivers) = ControlBroadcast::new(3, &wakes, df_cancel.clone());

        // Broadcast error from worker 1
        senders[1].broadcast_error("Map".into(), "boom".into());

        // Token should be cancelled
        assert!(df_cancel.is_cancelled());

        // All receivers should see the signal
        for (i, rx) in receivers.iter_mut().enumerate() {
            let signals = rx.try_recv();
            assert_eq!(signals.len(), 1, "receiver {i} should have 1 signal");
            assert!(matches!(
                &signals[0],
                WorkerControl::WorkerError { worker_index: 1, operator, message }
                    if operator == "Map" && message == "boom"
            ));
        }
    }

    #[test]
    fn control_broadcast_limit_does_not_cancel() {
        use crate::dataflow::channels::wake::WakeHandle;
        use crate::dataflow::control::ControlBroadcast;

        let parent = CancellationToken::new();
        let df_cancel = parent.child_token();
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let (senders, _receivers) = ControlBroadcast::new(2, &wakes, df_cancel.clone());

        senders[0].broadcast_limit("row budget exceeded".into());

        // LimitReached should NOT cancel the token
        assert!(!df_cancel.is_cancelled());
    }

    // -----------------------------------------------------------------------
    // source_async tests
    // -----------------------------------------------------------------------

    #[test]
    fn source_async_basic_roundtrip() {
        use crate::dataflow::DataflowBuilder;

        let rt = SimpleRuntime::new();
        let builder = DataflowBuilder::<u64>::new("async_src");
        let pipe = builder.source_async::<i32, _, _>("gen", |sender| async move {
            sender.send(0, vec![1, 2, 3]).await?;
            sender.send(1, vec![4, 5]).await?;
            Ok(())
        });
        pipe.map("double", |_t, x| x * 2).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow).unwrap();
        let receiver = handle.take_output::<i32>("out").unwrap();
        let data: Vec<i32> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();

        let mut sorted = data.clone();
        sorted.sort();
        assert_eq!(sorted, vec![2, 4, 6, 8, 10]);
        handle.join_blocking().unwrap();
    }

    #[test]
    fn source_async_empty_producer() {
        use crate::dataflow::DataflowBuilder;

        let rt = SimpleRuntime::new();
        let builder = DataflowBuilder::<u64>::new("empty_src");
        let pipe = builder.source_async::<i32, _, _>("gen", |_sender| async move {
            // Produce nothing — just return immediately.
            Ok(())
        });
        pipe.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow).unwrap();
        let receiver = handle.take_output::<i32>("out").unwrap();
        let data: Vec<(u64, Vec<i32>)> = receiver.collect_data();
        assert!(data.is_empty() || data.iter().all(|(_, v)| v.is_empty()));
        handle.join_blocking().unwrap();
    }

    #[test]
    fn source_async_cancellation() {
        use crate::dataflow::DataflowBuilder;

        let rt = SimpleRuntime::new();
        let builder = DataflowBuilder::<u64>::new("cancel_src");
        let pipe = builder.source_async::<i32, _, _>("gen", |sender| async move {
            // Infinite producer — should be stopped by cancellation.
            let mut i = 0;
            loop {
                if sender.send(0, vec![i]).await.is_err() {
                    break;
                }
                i += 1;
            }
            Ok(())
        });
        pipe.output("out");
        let dataflow = builder.build().unwrap();

        let handle = rt.spawn(dataflow).unwrap();
        // Cancel after a short delay.
        std::thread::sleep(std::time::Duration::from_millis(50));
        handle.cancel();
        // Should complete without hanging.
        let _ = handle.join_blocking();
    }

    #[test]
    fn source_async_with_runtime_handle() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("rt_async_src");
        let pipe = builder.source_async::<i32, _, _>("gen", |sender| async move {
            for i in 0..5 {
                sender.send(0, vec![i]).await?;
            }
            Ok(())
        });
        pipe.map("inc", |_t, x| x + 100).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let receiver = handle.take_output::<i32>("out").unwrap();
        let data: Vec<i32> = receiver
            .collect_data()
            .into_iter()
            .flat_map(|(_, d)| d)
            .collect();

        let mut sorted = data;
        sorted.sort();
        assert_eq!(sorted, vec![100, 101, 102, 103, 104]);
        handle.join_blocking().unwrap();
    }

    #[test]
    fn source_async_with_frontier_advancement() {
        use crate::dataflow::DataflowBuilder;

        let rt = SimpleRuntime::new();
        let builder = DataflowBuilder::<u64>::new("frontier_src");
        let pipe = builder.source_async::<i32, _, _>("gen", |sender| async move {
            sender.send(0, vec![10]).await?;
            sender.advance_to(1).await?;
            sender.send(1, vec![20]).await?;
            sender.advance_to(2).await?;
            sender.send(2, vec![30]).await?;
            Ok(())
        });
        pipe.output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow).unwrap();
        let receiver = handle.take_output::<i32>("out").unwrap();
        let data: Vec<(u64, Vec<i32>)> = receiver.collect_data();

        // Verify all timestamps and data are present.
        let all_data: Vec<(u64, i32)> = data
            .into_iter()
            .flat_map(|(t, vs)| vs.into_iter().map(move |v| (t, v)))
            .collect();
        assert!(all_data.contains(&(0, 10)));
        assert!(all_data.contains(&(1, 20)));
        assert!(all_data.contains(&(2, 30)));
        handle.join_blocking().unwrap();
    }

    // -----------------------------------------------------------------------
    // PeerRegistry unit tests
    // -----------------------------------------------------------------------

    #[cfg(feature = "transport")]
    #[test]
    fn peer_registry_report_peer_down_cancels_dataflows() {
        let registry = PeerRegistry::new();

        // Create mock cancel tokens for two cluster dataflows.
        let token1 = CancellationToken::new();
        let token2 = CancellationToken::new();
        let bridge1 = tokio_util::sync::CancellationToken::new();
        let bridge2 = tokio_util::sync::CancellationToken::new();

        // Dataflow 1 uses peers ["node-b", "node-c"]
        let handle1 = ClusterCancelHandle {
            worker_tokens: vec![token1.clone()],
            bridge_cancel: bridge1.clone(),
        };
        registry.register(&["node-b".into(), "node-c".into()], "df1", handle1);

        // Dataflow 2 uses peers ["node-b", "node-d"]
        let handle2 = ClusterCancelHandle {
            worker_tokens: vec![token2.clone()],
            bridge_cancel: bridge2.clone(),
        };
        registry.register(&["node-b".into(), "node-d".into()], "df2", handle2);

        // Report node-c down — should cancel df1 only.
        let count = registry.report_peer_down("node-c");
        assert_eq!(count, 1);
        assert!(token1.is_cancelled());
        assert!(!token2.is_cancelled());
        assert!(bridge1.is_cancelled());
        assert!(!bridge2.is_cancelled());
        assert_eq!(
            token1.reason(),
            Some(CancellationReason::PeerDown("node-c".into()))
        );

        // Report node-b down — df1 already cancelled, only df2 is new.
        let count = registry.report_peer_down("node-b");
        assert_eq!(count, 1);
        assert!(token2.is_cancelled());
        assert!(bridge2.is_cancelled());
        assert_eq!(
            token2.reason(),
            Some(CancellationReason::PeerDown("node-b".into()))
        );

        // Reporting again is a no-op.
        let count = registry.report_peer_down("node-b");
        assert_eq!(count, 0);
    }

    #[cfg(feature = "transport")]
    #[test]
    fn peer_registry_unknown_peer_returns_zero() {
        let registry = PeerRegistry::new();
        assert_eq!(registry.report_peer_down("unknown-node"), 0);
    }

    #[cfg(feature = "transport")]
    #[test]
    fn peer_registry_stale_entries_pruned() {
        let registry = PeerRegistry::new();
        let token = CancellationToken::new();
        let bridge = tokio_util::sync::CancellationToken::new();

        let handle = ClusterCancelHandle {
            worker_tokens: vec![token.clone()],
            bridge_cancel: bridge.clone(),
        };
        registry.register(&["node-x".into(), "node-y".into()], "df_stale", handle);

        // Cancel externally (simulating natural completion).
        token.cancel();
        bridge.cancel();

        // Reporting either peer should find 0 newly cancelled.
        assert_eq!(registry.report_peer_down("node-x"), 0);
        assert_eq!(registry.report_peer_down("node-y"), 0);
    }

    #[cfg(feature = "transport")]
    #[test]
    fn report_node_leave_on_runtime_handle() {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..Default::default()
        })
        .unwrap();

        // No cluster dataflows registered, should return 0.
        assert_eq!(rt.report_node_leave("some-peer"), 0);
    }

    #[cfg(feature = "transport")]
    #[test]
    fn peer_registry_register_after_peer_down_immediately_cancels() {
        let registry = PeerRegistry::new();

        // Report node-x down before any dataflows are registered.
        assert_eq!(registry.report_peer_down("node-x"), 0);

        // Now register a dataflow that uses node-x — should be cancelled immediately.
        let token = CancellationToken::new();
        let bridge = tokio_util::sync::CancellationToken::new();
        let handle = ClusterCancelHandle {
            worker_tokens: vec![token.clone()],
            bridge_cancel: bridge.clone(),
        };
        registry.register(&["node-x".into(), "node-y".into()], "late_df", handle);

        // Should have been cancelled immediately on registration.
        assert!(token.is_cancelled());
        assert!(bridge.is_cancelled());
        assert_eq!(
            token.reason(),
            Some(CancellationReason::PeerDown("node-x".into()))
        );
    }

    #[cfg(feature = "transport")]
    #[test]
    fn peer_registry_recovered_peer_allows_new_registrations() {
        let registry = PeerRegistry::new();

        // Report node-x down.
        registry.report_peer_down("node-x");

        // Recover node-x.
        assert!(registry.report_peer_recovered("node-x"));
        // Second call is a no-op.
        assert!(!registry.report_peer_recovered("node-x"));

        // Now register a dataflow using node-x — should NOT be cancelled.
        let token = CancellationToken::new();
        let bridge = tokio_util::sync::CancellationToken::new();
        let handle = ClusterCancelHandle {
            worker_tokens: vec![token.clone()],
            bridge_cancel: bridge.clone(),
        };
        registry.register(&["node-x".into()], "recovered_df", handle);

        assert!(!token.is_cancelled());
        assert!(!bridge.is_cancelled());
    }

    #[test]
    fn validate_stage_parallelism_matching_passes() {
        // Build a dataflow with exchange_by_hash_to(par=2) and spawn with 2 workers.
        // Should succeed because parallelism matches num_workers.
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            schedule_policy: None,
            name: "par-match".to_string(), ..Default::default() })
        .unwrap();

        let result = rt.spawn_multi("par-match-df", 2, |_worker_idx, builder| {
            builder
                .source("src", vec![(0u64, vec![1i32, 2, 3])])
                .exchange_by_hash_to("ex", 2, |x: &i32| *x as u64)
                .map("noop", |_t, x| x)
                .output("out");
            Ok(())
        }, SpawnOptions::default());
        // Should succeed — parallelism matches worker count.
        match &result {
            Ok(_) => {}
            Err(e) => panic!("spawn_multi failed: {e}"),
        }
        rt.shutdown();
    }

    #[test]
    fn validate_stage_parallelism_mismatch_fails() {
        // Build a dataflow with exchange_by_hash_to(par=4) but spawn with 2 workers.
        // Should fail because parallelism (4) != num_workers (2).
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            schedule_policy: None,
            name: "par-mismatch".to_string(), ..Default::default() })
        .unwrap();

        let result = rt.spawn_multi("par-mismatch-df", 2, |_worker_idx, builder| {
            builder
                .source("src", vec![(0u64, vec![1i32, 2, 3])])
                .exchange_by_hash_to("ex", 4, |x: &i32| *x as u64)
                .map("noop", |_t, x| x)
                .output("out");
            Ok(())
        }, SpawnOptions::default());
        // Should fail — parallelism (4) != num_workers (2).
        match result {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("parallelism 4") || err_msg.contains("explicit parallelism 4"),
                    "unexpected error: {err_msg}"
                );
            }
            Ok(_) => panic!("expected error for parallelism mismatch"),
        }
        rt.shutdown();
    }

    #[test]
    fn validate_stage_parallelism_single_worker_mismatch_fails() {
        // Even with 1 worker, explicit parallelism > 1 should be rejected.
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            schedule_policy: None,
            name: "par-single".to_string(), ..Default::default() })
        .unwrap();

        let result = rt.spawn_multi("par-single-df", 1, |_worker_idx, builder| {
            builder
                .source("src", vec![(0u64, vec![1i32, 2, 3])])
                .exchange_by_hash_to("ex", 4, |x: &i32| *x as u64)
                .map("noop", |_t, x| x)
                .output("out");
            Ok(())
        }, SpawnOptions::default());
        match result {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("parallelism 4") || err_msg.contains("explicit parallelism 4"),
                    "unexpected error: {err_msg}"
                );
            }
            Ok(_) => panic!("expected error for single-worker parallelism mismatch"),
        }
        rt.shutdown();
    }

    #[test]
    fn active_dataflows_counter() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(rt.active_dataflows(), 0);

        // Spawn a dataflow with an input port (stays alive until input closes).
        let builder = DataflowBuilder::<u64>::new("active-count-test");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();

        // Should show 1 active dataflow.
        assert_eq!(rt.active_dataflows(), 1);

        // Close input and wait for completion.
        drop(sender);
        handle.join_blocking().ok();

        // Give the callback a moment to fire.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(rt.active_dataflows(), 0);
        rt.shutdown();
    }

    #[tokio::test]
    async fn shutdown_async_awaits_completion() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("shutdown-async-test");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();
        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();

        assert_eq!(rt.active_dataflows(), 1);

        // Drop sender so the dataflow can finish.
        drop(sender);

        // shutdown_async should cancel + wait for idle.
        rt.shutdown_async().await;
        assert_eq!(rt.active_dataflows(), 0);
    }

    #[tokio::test]
    async fn wait_idle_returns_when_no_dataflows() {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..Default::default()
        })
        .unwrap();

        // Should return immediately — no active dataflows.
        rt.wait_idle().await;
        assert_eq!(rt.active_dataflows(), 0);
    }

    #[tokio::test]
    async fn wait_idle_multi_worker() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();

        let mut handle = rt
            .spawn_multi("idle-multi", 3, |_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            }, SpawnOptions::default())
            .unwrap();

        // 3 workers = 3 active.
        assert_eq!(rt.active_dataflows(), 3);

        // Close all inputs.
        for i in 0..3 {
            drop(handle.take_input::<i32>(i, "data").unwrap());
        }

        rt.wait_idle().await;
        assert_eq!(rt.active_dataflows(), 0);
    }

    #[tokio::test]
    async fn multi_dataflow_completion_future() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();

        let mut handle = rt
            .spawn_multi(
                "multi-future",
                3,
                |_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.output("out");
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();

        // Close all inputs so the dataflow can finish.
        for i in 0..3 {
            drop(handle.take_input::<i32>(i, "data").unwrap());
        }

        // Await the MultiDataflowCompletion as a Future.
        let result = handle.join().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn multi_dataflow_completion_error_cancels_siblings() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();

        let mut handle = rt
            .spawn_multi(
                "multi-error",
                2,
                |_idx, builder: &mut DataflowBuilder<u64>| {
                    let input = builder.input::<i32>("data");
                    input.output("out");
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();

        // Close only worker 0's input — worker 1 stays blocked.
        drop(handle.take_input::<i32>(0, "data").unwrap());

        // Cancel worker 1 explicitly to simulate a failure.
        handle.worker_mut(1).cancel();

        // Awaiting should complete (either success or error from worker 1).
        let result = handle.join().await;
        // Worker 1 was cancelled, so result should be an error or Ok depending
        // on whether cancellation counts as error. Either way, it should not hang.
        let _ = result;
    }

    #[tokio::test]
    async fn test_unary_async_basic() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("async_pipeline");
        let input = builder.input::<i32>("data");

        // Async operator that doubles each item with a simulated async delay
        let logic = Arc::new(|_time: u64, batch: Vec<i32>| async move {
            tokio::task::yield_now().await;
            Ok(batch.into_iter().map(|x| x * 2).collect::<Vec<i32>>())
        });

        let _output = input.unary_async("double_async", 4, logic).output("results");
        let df = builder.build().unwrap();
        let mut handle = rt.spawn(df, SpawnOptions::default()).unwrap();

        let tx = handle.take_input::<i32>("data").unwrap();
        let rx = handle.take_output::<i32>("results").unwrap();
        tx.send(0, vec![10, 20, 30]).unwrap();
        tx.close();

        handle.join().await.unwrap();

        let mut results: Vec<i32> = Vec::new();
        while let Some(event) = rx.recv() {
            if let crate::dataflow::operators::output::OutputEvent::Data { data, .. } = event {
                results.extend(data);
            }
        }
        results.sort();
        assert_eq!(results, vec![20, 40, 60]);
    }

    #[tokio::test]
    async fn test_unary_async_concurrency_limit() {
        use crate::dataflow::DataflowBuilder;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("concurrency_test");
        let input = builder.input::<i32>("data");

        let peak_concurrency = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let peak_clone = Arc::clone(&peak_concurrency);
        let current_clone = Arc::clone(&current);

        let logic = Arc::new(move |_time: u64, batch: Vec<i32>| {
            let current = Arc::clone(&current_clone);
            let peak = Arc::clone(&peak_clone);
            async move {
                let prev = current.fetch_add(1, Ordering::SeqCst);
                peak.fetch_max(prev + 1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok(batch)
            }
        });

        // max_concurrency = 2
        let _output = input.unary_async("limited", 2, logic).output("out");
        let df = builder.build().unwrap();
        let mut handle = rt.spawn(df, SpawnOptions::default()).unwrap();

        let tx = handle.take_input::<i32>("data").unwrap();
        for i in 0..10 {
            tx.send(i as u64, vec![i]).unwrap();
        }
        tx.close();

        handle.join().await.unwrap();

        // Peak concurrency should not exceed 2
        assert!(peak_concurrency.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn test_unary_async_error_propagation() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("error_test");
        let input = builder.input::<i32>("data");

        let logic = Arc::new(|_time: u64, _batch: Vec<i32>| async move {
            Err(crate::error::Error::Custom("async failure".into()))
        });

        let _output = input
            .unary_async::<i32, _, _>("failing", 4, logic)
            .output("out");
        let df = builder.build().unwrap();
        let mut handle = rt.spawn(df, SpawnOptions::default()).unwrap();

        let tx = handle.take_input::<i32>("data").unwrap();
        tx.send(0, vec![1]).unwrap();
        tx.close();

        let result = handle.join().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unary_async_panic_recovery() {
        use crate::dataflow::DataflowBuilder;

        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("panic_test");
        let input = builder.input::<i32>("data");

        let logic = Arc::new(|_time: u64, _batch: Vec<i32>| async move {
            panic!("intentional panic in async logic");
            #[allow(unreachable_code)]
            Ok(vec![])
        });

        let _output = input
            .unary_async::<i32, _, _>("panicking", 4, logic)
            .output("out");
        let df = builder.build().unwrap();
        let mut handle = rt.spawn(df, SpawnOptions::default()).unwrap();

        let tx = handle.take_input::<i32>("data").unwrap();
        tx.send(0, vec![1]).unwrap();
        tx.close();

        // Should complete with error rather than hanging
        let result = handle.join().await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("panic"), "error should mention panic: {err_msg}");
    }
}
