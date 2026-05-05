//! Execution provider trait and implementations.
//!
//! The [`ExecutionProvider`] maps logical worker task submissions to physical
//! threads. Different implementations enable testing (inline), production
//! (worker pool), or custom execution strategies.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::dataflow::stage::StageId;
use crate::worker::WorkerId;

/// Maps logical workers to physical execution resources.
///
/// The default implementation uses the custom worker thread pool.
/// Alternative implementations can pin workers to specific cores,
/// use NUMA-aware scheduling, or run inline for testing.
pub trait ExecutionProvider: Send + Sync + 'static {
    /// Submit a task for a logical worker to be executed physically.
    fn submit_task(&self, worker: WorkerId, task: Box<dyn FnOnce() + Send>);

    /// Returns the maximum concurrent tasks allowed for a stage.
    fn stage_concurrency(&self, stage: StageId) -> usize;

    /// Shut down the execution provider, releasing resources.
    fn shutdown(&self);
}

/// Worker pool execution provider: submits tasks to the custom worker thread pool.
///
/// This is the default execution provider used in production.
pub struct WorkerPoolExecution {
    /// Reference to the worker pool (tasks are submitted here).
    pool: Arc<crate::worker_pool::WorkerPool>,
    /// Default stage concurrency.
    default_concurrency: usize,
}

impl WorkerPoolExecution {
    /// Create a new worker pool execution provider.
    pub fn new(pool: Arc<crate::worker_pool::WorkerPool>, default_concurrency: usize) -> Self {
        Self {
            pool,
            default_concurrency,
        }
    }

    /// Get a reference to the underlying worker pool.
    pub fn pool(&self) -> &crate::worker_pool::WorkerPool {
        &self.pool
    }
}

impl ExecutionProvider for WorkerPoolExecution {
    fn submit_task(&self, worker: WorkerId, task: Box<dyn FnOnce() + Send>) {
        use crate::worker::OperatorActivation;
        let activation = OperatorActivation::new(worker, "task", 0, task);
        self.pool.submit(activation);
    }

    fn stage_concurrency(&self, _stage: StageId) -> usize {
        self.default_concurrency
    }

    fn shutdown(&self) {
        self.pool.shutdown();
    }
}

/// Inline execution provider for testing.
///
/// Runs all tasks on the calling thread immediately (synchronous, deterministic).
/// Useful for unit tests where you want predictable execution order.
pub struct InlineExecution {
    /// Count of tasks executed (for assertions in tests).
    tasks_executed: AtomicUsize,
    /// Collected task closures (if deferred mode is used).
    deferred: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    /// Whether to execute immediately or defer.
    immediate: bool,
    /// Default stage concurrency (returned but not enforced in inline mode).
    default_concurrency: usize,
}

impl InlineExecution {
    /// Create an inline execution provider that runs tasks immediately.
    pub fn new() -> Self {
        Self {
            tasks_executed: AtomicUsize::new(0),
            deferred: Mutex::new(Vec::new()),
            immediate: true,
            default_concurrency: 1,
        }
    }

    /// Create an inline execution provider that defers tasks.
    /// Call `run_deferred()` to execute them.
    pub fn deferred() -> Self {
        Self {
            tasks_executed: AtomicUsize::new(0),
            deferred: Mutex::new(Vec::new()),
            immediate: false,
            default_concurrency: 1,
        }
    }

    /// Get the number of tasks executed so far.
    pub fn tasks_executed(&self) -> usize {
        self.tasks_executed.load(Ordering::Relaxed)
    }

    /// Run all deferred tasks. Returns the count of tasks executed.
    pub fn run_deferred(&self) -> usize {
        let tasks: Vec<_> = self
            .deferred
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        let count = tasks.len();
        for task in tasks {
            task();
            self.tasks_executed.fetch_add(1, Ordering::Relaxed);
        }
        count
    }

    /// Number of deferred tasks waiting.
    pub fn pending_count(&self) -> usize {
        self.deferred
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

impl Default for InlineExecution {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionProvider for InlineExecution {
    fn submit_task(&self, _worker: WorkerId, task: Box<dyn FnOnce() + Send>) {
        if self.immediate {
            task();
            self.tasks_executed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.deferred
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(task);
        }
    }

    fn stage_concurrency(&self, _stage: StageId) -> usize {
        self.default_concurrency
    }

    fn shutdown(&self) {
        // No resources to release for inline execution
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn inline_immediate_executes_synchronously() {
        let exec = InlineExecution::new();
        let counter = Arc::new(AtomicU32::new(0));

        let c = counter.clone();
        exec.submit_task(
            WorkerId::new(0),
            Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
        );

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(exec.tasks_executed(), 1);
    }

    #[test]
    fn inline_deferred_collects_tasks() {
        let exec = InlineExecution::deferred();
        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..5 {
            let c = counter.clone();
            exec.submit_task(
                WorkerId::new(0),
                Box::new(move || {
                    c.fetch_add(1, Ordering::SeqCst);
                }),
            );
        }

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(exec.pending_count(), 5);

        let count = exec.run_deferred();
        assert_eq!(count, 5);
        assert_eq!(counter.load(Ordering::SeqCst), 5);
        assert_eq!(exec.tasks_executed(), 5);
    }

    #[test]
    fn inline_stage_concurrency() {
        let exec = InlineExecution::new();
        assert_eq!(exec.stage_concurrency(StageId(0)), 1);
    }

    #[test]
    fn worker_pool_execution_submits_tasks() {
        use crate::worker_pool::{WorkerPool, WorkerPoolConfig};
        use std::time::Duration;

        let config = WorkerPoolConfig {
            min_threads: 2,
            max_threads: 4,
            idle_shutdown: Duration::from_secs(5),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = Arc::new(WorkerPool::new(config).unwrap());
        let exec = WorkerPoolExecution::new(pool, 4);

        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();

        exec.submit_task(
            WorkerId::new(0),
            Box::new(move || {
                c.fetch_add(1, Ordering::SeqCst);
            }),
        );

        // Wait for task to complete
        let start = std::time::Instant::now();
        while counter.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(Duration::from_millis(10));
            if start.elapsed() > Duration::from_secs(2) {
                panic!("task did not execute");
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), 1);
        exec.shutdown();
    }

    #[test]
    fn execution_provider_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InlineExecution>();
        assert_send_sync::<WorkerPoolExecution>();
    }
}
