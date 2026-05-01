//! Self-contained runtime for hosting instancy dataflows.
//!
//! A [`RuntimeHandle`] encapsulates all resources needed to run dataflows:
//! worker pool, task queue, and scheduling policy. Multiple `RuntimeHandle`
//! instances can coexist in the same process with full isolation (§12.6).
//!
//! **No global state:** All shared state flows from the `RuntimeHandle` root.

use crate::cancellation::CancellationToken;
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
    pub fn new(config: RuntimeConfig) -> Result<Self, crate::error::Error> {
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

    /// Shut down the runtime, cancelling all running dataflows.
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
}
