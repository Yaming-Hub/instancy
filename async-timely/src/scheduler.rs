//! Task scheduler for managing operator activation dispatch.
//!
//! The [`TaskScheduler`] ensures:
//! - FIFO ordering within each logical worker
//! - Per-region concurrency limits (at most N tasks from a region run concurrently)
//! - Dispatch only when a worker has no in-flight task

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::dataflow::region::RegionId;
use crate::worker::{OperatorActivation, WorkerId};

/// A compute task ready for dispatch to the worker pool.
pub struct ComputeTask {
    /// The activation to execute.
    pub activation: OperatorActivation,
    /// Region this task belongs to (for concurrency limiting).
    pub region_id: RegionId,
    /// Shared permit tracker for the region.
    pub(crate) region_permit: Arc<RegionPermit>,
}

impl ComputeTask {
    /// Execute this task, releasing the region permit when done.
    pub fn execute(self) {
        self.activation.execute();
        self.region_permit.release();
    }
}

/// Tracks concurrent task count for a region.
pub struct RegionPermit {
    /// Current number of in-flight tasks for this region.
    in_flight: AtomicUsize,
    /// Maximum concurrent tasks allowed.
    max_concurrent: usize,
}

impl RegionPermit {
    /// Create a new region permit with the given concurrency limit.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            max_concurrent,
        }
    }

    /// Try to acquire a permit. Returns true if successful.
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.in_flight.load(Ordering::Acquire);
            if current >= self.max_concurrent {
                return false;
            }
            if self.in_flight
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release a permit.
    pub fn release(&self) {
        self.in_flight.fetch_sub(1, Ordering::Release);
    }

    /// Current number of in-flight tasks.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Maximum concurrent tasks allowed.
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }
}

/// Per-worker queue state.
struct WorkerQueue {
    /// Pending activations for this worker (FIFO).
    queue: VecDeque<(OperatorActivation, RegionId)>,
    /// Whether this worker has a task currently in-flight.
    has_in_flight: bool,
}

impl WorkerQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            has_in_flight: false,
        }
    }
}

/// Configuration for the task scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Default concurrency limit for regions without explicit config.
    pub default_region_concurrency: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            default_region_concurrency: cpus,
        }
    }
}

/// Task scheduler that manages dispatch of operator activations.
///
/// Ensures FIFO per-worker ordering and per-region concurrency limits.
pub struct TaskScheduler {
    /// Per-worker queues, indexed by worker ID.
    worker_queues: Mutex<Vec<WorkerQueue>>,
    /// Per-region permits, indexed by region ID.
    region_permits: Mutex<Vec<Arc<RegionPermit>>>,
    /// Configuration.
    config: SchedulerConfig,
    /// Total tasks enqueued.
    tasks_enqueued: AtomicUsize,
    /// Total tasks dispatched.
    tasks_dispatched: AtomicUsize,
}

impl TaskScheduler {
    /// Create a new task scheduler.
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            worker_queues: Mutex::new(Vec::new()),
            region_permits: Mutex::new(Vec::new()),
            config,
            tasks_enqueued: AtomicUsize::new(0),
            tasks_dispatched: AtomicUsize::new(0),
        }
    }

    /// Enqueue an operator activation for a specific worker and region.
    pub fn enqueue(
        &self,
        activation: OperatorActivation,
        region_id: RegionId,
    ) {
        let worker_idx = activation.worker_id.index();
        {
            let mut queues = self.worker_queues.lock().unwrap();
            // Grow worker queue vector if needed
            while queues.len() <= worker_idx {
                queues.push(WorkerQueue::new());
            }
            queues[worker_idx].queue.push_back((activation, region_id));
        }
        self.tasks_enqueued.fetch_add(1, Ordering::Relaxed);
    }

    /// Try to dispatch ready tasks, returning a vec of tasks that can be submitted.
    ///
    /// A task is ready when:
    /// 1. Its worker has no in-flight task (FIFO ordering)
    /// 2. The region has available concurrency permits
    pub fn dispatch_ready(&self) -> Vec<ComputeTask> {
        let mut ready = Vec::new();
        let mut queues = self.worker_queues.lock().unwrap();
        let mut permits = self.region_permits.lock().unwrap();

        for queue in queues.iter_mut() {
            if queue.has_in_flight {
                continue; // Worker busy
            }
            if let Some((_, region_id)) = queue.queue.front() {
                let region_id = *region_id;
                let permit = self.ensure_permit(&mut permits, region_id);
                if permit.try_acquire() {
                    let (activation, region_id) = queue.queue.pop_front().unwrap();
                    queue.has_in_flight = true;
                    let _ = region_id; // already used above
                    ready.push(ComputeTask {
                        activation,
                        region_id,
                        region_permit: permit,
                    });
                    self.tasks_dispatched.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        ready
    }

    /// Mark a worker as no longer having an in-flight task.
    pub fn mark_completed(&self, worker_id: WorkerId) {
        let mut queues = self.worker_queues.lock().unwrap();
        let idx = worker_id.index();
        if idx < queues.len() {
            queues[idx].has_in_flight = false;
        }
    }

    /// Set the concurrency limit for a specific region.
    pub fn set_region_concurrency(&self, region_id: RegionId, max_concurrent: usize) {
        let mut permits = self.region_permits.lock().unwrap();
        let idx = region_id.0;
        while permits.len() <= idx {
            permits.push(Arc::new(RegionPermit::new(
                self.config.default_region_concurrency,
            )));
        }
        permits[idx] = Arc::new(RegionPermit::new(max_concurrent));
    }

    /// Get the number of pending tasks across all workers.
    pub fn pending_tasks(&self) -> usize {
        let queues = self.worker_queues.lock().unwrap();
        queues.iter().map(|q| q.queue.len()).sum()
    }

    /// Total tasks enqueued since creation.
    pub fn total_enqueued(&self) -> usize {
        self.tasks_enqueued.load(Ordering::Relaxed)
    }

    /// Total tasks dispatched since creation.
    pub fn total_dispatched(&self) -> usize {
        self.tasks_dispatched.load(Ordering::Relaxed)
    }

    /// Ensure a region permit exists and return it.
    /// Grows the permits vector if needed, storing the default permit.
    fn ensure_permit(
        &self,
        permits: &mut Vec<Arc<RegionPermit>>,
        region_id: RegionId,
    ) -> Arc<RegionPermit> {
        let idx = region_id.0;
        while permits.len() <= idx {
            permits.push(Arc::new(RegionPermit::new(
                self.config.default_region_concurrency,
            )));
        }
        permits[idx].clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_activation(worker: usize, name: &str) -> OperatorActivation {
        OperatorActivation::new(WorkerId::new(worker), name, 0, || {})
    }

    #[test]
    fn fifo_ordering_within_worker() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 10,
        });
        let region = RegionId(0);

        // Enqueue 3 tasks for worker 0
        scheduler.enqueue(make_activation(0, "first"), region);
        scheduler.enqueue(make_activation(0, "second"), region);
        scheduler.enqueue(make_activation(0, "third"), region);

        // First dispatch should get only "first"
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].activation.operator_name, "first");

        // Worker 0 still has in-flight — no more dispatches
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 0);

        // Complete the task
        scheduler.mark_completed(WorkerId::new(0));

        // Now "second" should be dispatched
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].activation.operator_name, "second");
    }

    #[test]
    fn multiple_workers_dispatch_independently() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 10,
        });
        let region = RegionId(0);

        scheduler.enqueue(make_activation(0, "w0_task"), region);
        scheduler.enqueue(make_activation(1, "w1_task"), region);
        scheduler.enqueue(make_activation(2, "w2_task"), region);

        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 3);
    }

    #[test]
    fn region_concurrency_limit() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 2,
        });
        let region = RegionId(0);
        scheduler.set_region_concurrency(region, 2);

        // Enqueue tasks for 4 different workers in same region
        for i in 0..4 {
            scheduler.enqueue(make_activation(i, &format!("task_{i}")), region);
        }

        // Only 2 should be dispatched (region limit)
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 2);

        // Even after trying again, still limited
        let ready2 = scheduler.dispatch_ready();
        assert_eq!(ready2.len(), 0);
    }

    #[test]
    fn region_permit_release_allows_more() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 1,
        });
        let region = RegionId(0);
        scheduler.set_region_concurrency(region, 1);

        scheduler.enqueue(make_activation(0, "first"), region);
        scheduler.enqueue(make_activation(1, "second"), region);

        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);

        // Execute (releases permit) and mark worker complete
        ready.into_iter().for_each(|t| {
            let worker = t.activation.worker_id;
            t.execute();
            scheduler.mark_completed(worker);
        });

        // Now second task can dispatch
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].activation.operator_name, "second");
    }

    #[test]
    fn dispatch_only_when_no_in_flight() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 100,
        });
        let region = RegionId(0);

        scheduler.enqueue(make_activation(0, "task_a"), region);
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);

        // Worker 0 has in-flight task — enqueue more, but they shouldn't dispatch
        scheduler.enqueue(make_activation(0, "task_b"), region);
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 0);

        // Mark complete — now task_b dispatches
        scheduler.mark_completed(WorkerId::new(0));
        let ready = scheduler.dispatch_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].activation.operator_name, "task_b");
    }

    #[test]
    fn pending_tasks_count() {
        let scheduler = TaskScheduler::new(SchedulerConfig::default());
        let region = RegionId(0);

        assert_eq!(scheduler.pending_tasks(), 0);
        scheduler.enqueue(make_activation(0, "a"), region);
        scheduler.enqueue(make_activation(1, "b"), region);
        assert_eq!(scheduler.pending_tasks(), 2);

        scheduler.dispatch_ready();
        assert_eq!(scheduler.pending_tasks(), 0);
    }

    #[test]
    fn metrics_tracking() {
        let scheduler = TaskScheduler::new(SchedulerConfig {
            default_region_concurrency: 10,
        });
        let region = RegionId(0);

        scheduler.enqueue(make_activation(0, "x"), region);
        scheduler.enqueue(make_activation(1, "y"), region);
        assert_eq!(scheduler.total_enqueued(), 2);
        assert_eq!(scheduler.total_dispatched(), 0);

        scheduler.dispatch_ready();
        assert_eq!(scheduler.total_dispatched(), 2);
    }
}
