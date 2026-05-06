//! Task wrapper for cooperative executor multiplexing on a shared worker pool.
//!
//! # Concept Hierarchy
//!
//! - **DataflowExecutor** — the Future that drives a single dataflow's sweep loop.
//! - **ExecutorTask** — wraps a DataflowExecutor in a state machine so multiple
//!   executors can share pool threads cooperatively.
//! - **ExecutorRegistry** — owns all active ExecutorTasks and their ready queue.
//!
//! # State Machine
//!
//! Each ExecutorTask transitions through these states:
//!
//! ```text
//!   IDLE ──(wake)──▶ QUEUED ──(thread picks up)──▶ POLLING ──▶ IDLE (Pending)
//!                                                          ╰──▶ DONE (Ready)
//! ```
//!
//! - **IDLE**: The executor is waiting for a notification (channel data, timer,
//!   cancellation). It is NOT in the ready queue.
//! - **QUEUED**: A wakeup occurred (via PoolWaker). The task is in the ready queue
//!   waiting for a pool thread. Duplicate wakeups are suppressed by CAS: only the
//!   IDLE→QUEUED transition succeeds.
//! - **POLLING**: A pool thread is actively polling this executor. No other thread
//!   may poll it concurrently (enforced by CAS, not a mutex).
//! - **DONE**: The executor returned `Poll::Ready`. Terminal state.
//!
//! The CAS-based transitions guarantee:
//! - No duplicate enqueues (only one IDLE→QUEUED transition succeeds)
//! - No concurrent polling (only one QUEUED→POLLING transition succeeds)
//! - No missed wakeups (wake during POLLING sets QUEUED, re-enqueued after poll)

use std::cmp::Ordering as CmpOrdering;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake};
use std::time::Instant;

use crate::dataflow::DataflowId;
use crate::error::Result;
use crate::runtime::CompletionNotifier;
use crate::scheduler::policy::{SchedulePolicy, TaskMeta};

/// Task states for the executor state machine.
///
/// Values are chosen to be distinct u8 constants for atomic CAS operations.
const TASK_IDLE: u8 = 0;
const TASK_QUEUED: u8 = 1;
const TASK_POLLING: u8 = 2;
const TASK_DONE: u8 = 3;

/// A unique identifier for an executor task within a registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub(crate) usize);

/// Outcome of polling an executor task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// The executor completed (Poll::Ready). State is now DONE.
    Completed,
    /// The executor returned Pending and transitioned to IDLE.
    /// A future wake via try_wake() will handle re-enqueue.
    Pending,
    /// The executor returned Pending but a wake arrived *during* the poll,
    /// so the state is QUEUED. The caller MUST re-enqueue the task.
    PendingNeedsReenqueue,
}

/// Wraps a DataflowExecutor future with a state machine for cooperative
/// multiplexing on shared worker pool threads.
///
/// The per-task mutex ensures poll exclusivity without a global registry lock —
/// only the thread that successfully transitions QUEUED→POLLING may poll.
pub struct ExecutorTask {
    /// The pinned executor future.
    ///
    /// Behind a `Mutex<Option<...>>` for two reasons:
    /// 1. Interior mutability — only one thread polls at a time, enforced by
    ///    the CAS state machine (not the mutex).
    /// 2. Breaking Arc cycles — on completion or panic, the future is `.take()`n
    ///    to drop it, which breaks the PoolWaker→ExecutorTask→future→WakeHandle
    ///    →Waker→PoolWaker reference cycle that would otherwise leak the task.
    executor: Mutex<Option<Pin<Box<dyn Future<Output = Result<bool>> + Send>>>>,

    /// Completion notifier — signals DataflowCompletion when the executor finishes.
    notifier: Mutex<Option<CompletionNotifier>>,

    /// Scheduling metadata used when the task is queued.
    meta: Mutex<TaskMeta>,

    /// Atomic task state (IDLE / QUEUED / POLLING / DONE).
    state: AtomicU8,

    /// Unique task identifier within the registry.
    pub(crate) id: TaskId,
}

impl ExecutorTask {
    /// Create a new task wrapping the given executor future.
    ///
    /// The task starts in QUEUED state — it should be placed in the ready queue
    /// immediately after creation so the pool can begin polling it.
    pub fn new(
        id: TaskId,
        dataflow_id: DataflowId,
        priority: u32,
        executor: Pin<Box<dyn Future<Output = Result<bool>> + Send>>,
        notifier: CompletionNotifier,
    ) -> Self {
        Self {
            executor: Mutex::new(Some(executor)),
            notifier: Mutex::new(Some(notifier)),
            meta: Mutex::new(TaskMeta::new(dataflow_id, priority)),
            state: AtomicU8::new(TASK_QUEUED),
            id,
        }
    }

    /// Refresh the enqueue timestamp when the task enters the ready queue.
    pub fn mark_enqueued(&self) {
        if let Ok(mut meta) = self.meta.lock() {
            meta.created_at = Instant::now();
        }
    }

    /// Snapshot this task's scheduling metadata.
    pub fn task_meta(&self) -> Option<TaskMeta> {
        self.meta.lock().ok().map(|meta| meta.clone())
    }

    /// Attempt to transition from QUEUED to POLLING.
    ///
    /// Returns `true` if this thread may proceed to poll the executor.
    /// Returns `false` if another thread already claimed it or it's done.
    pub fn try_start_poll(&self) -> bool {
        self.state
            .compare_exchange(
                TASK_QUEUED,
                TASK_POLLING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Poll the executor with the given waker context.
    ///
    /// # Safety contract
    ///
    /// Caller MUST have successfully called `try_start_poll()` first.
    /// Only one thread may call this at a time.
    ///
    /// After polling, transitions state:
    /// - `Poll::Ready` → DONE, notifies CompletionNotifier
    /// - `Poll::Pending` → IDLE (or QUEUED if a wake arrived during polling)
    ///
    /// Returns a [`PollOutcome`] indicating whether the task completed,
    /// is pending-idle, or is pending and needs re-enqueue by the caller.
    pub fn poll(&self, cx: &mut Context<'_>) -> PollOutcome {
        // Note: we intentionally do NOT assert state == POLLING here.
        // Between try_start_poll() (which sets POLLING) and this call,
        // a stale PoolWaker from a previous poll may fire try_wake(),
        // which CAS transitions POLLING → QUEUED. This is benign:
        // the CAS at the end of poll() will detect QUEUED and return
        // PendingNeedsReenqueue, causing the caller to re-enqueue.

        // Helper: complete the task with an error when we detect an
        // unrecoverable internal problem (poisoned mutex, missing future).
        // Transitions to DONE and notifies the waiter.
        let complete_with_error = |this: &Self, msg: &str| {
            this.state.store(TASK_DONE, Ordering::Release);
            // Best-effort: try to take notifier and fire error. If the
            // notifier lock is also poisoned, the NotCompletionNotifier's Drop
            // (which fires on the Arc's last drop) is the final safety net.
            if let Ok(mut n) = this.notifier.lock() {
                if let Some(notifier) = n.take() {
                    notifier.complete(Err(crate::error::Error::Custom(msg.into())));
                }
            }
            PollOutcome::Completed
        };

        // Panic safety: if the executor panics during poll, transition to DONE
        // and drop the executor future to break any Arc cycles. Then take the
        // notifier so its Drop impl fires an error to the waiter.
        struct PanicGuard<'a> {
            state: &'a AtomicU8,
            executor: &'a Mutex<Option<Pin<Box<dyn Future<Output = Result<bool>> + Send>>>>,
            notifier: &'a Mutex<Option<CompletionNotifier>>,
        }
        impl Drop for PanicGuard<'_> {
            fn drop(&mut self) {
                self.state.store(TASK_DONE, Ordering::Release);
                // Drop the future to break PoolWaker→ExecutorTask→future→WakeHandle cycle.
                // Use into_inner() to recover the data even from a poisoned mutex.
                if let Ok(mut exec) = self.executor.lock() {
                    exec.take();
                }
                // Take the notifier so its Drop impl fires the error path.
                if let Ok(mut n) = self.notifier.lock() {
                    drop(n.take());
                }
            }
        }
        let guard = PanicGuard {
            state: &self.state,
            executor: &self.executor,
            notifier: &self.notifier,
        };

        // Acquire executor lock. Poisoned = a prior poll panicked while holding
        // the lock. The CAS state machine should prevent this (panicked tasks
        // transition to DONE), but handle gracefully as defense-in-depth.
        let result = {
            let mut executor_lock = match self.executor.lock() {
                Ok(lock) => lock,
                Err(_poisoned) => {
                    std::mem::forget(guard);
                    return complete_with_error(self, "executor mutex poisoned by a prior panic");
                }
            };
            match executor_lock.as_mut() {
                Some(executor) => executor.as_mut().poll(cx),
                None => {
                    // The future was already taken (completed or panicked). This
                    // should never happen because the CAS state machine prevents
                    // DONE tasks from reaching POLLING. Treat as an error rather
                    // than panicking in core scheduling code.
                    std::mem::forget(guard);
                    return complete_with_error(
                        self,
                        "executor polled after future was already consumed",
                    );
                }
            }
        };

        // Poll succeeded (no panic) — cancel the guard.
        std::mem::forget(guard);

        match result {
            Poll::Ready(outcome) => {
                self.state.store(TASK_DONE, Ordering::Release);

                // Drop the future to break the PoolWaker→ExecutorTask→future
                // →WakeHandle→Waker→PoolWaker Arc cycle. If the lock is
                // somehow poisoned here (shouldn't be — we just held it
                // successfully above), skip the take; the cycle may leak
                // but we don't panic.
                if let Ok(mut exec) = self.executor.lock() {
                    exec.take();
                }

                // Notify completion. Same defensive lock handling.
                if let Ok(mut n) = self.notifier.lock() {
                    if let Some(notifier) = n.take() {
                        notifier.complete(outcome);
                    }
                }
                PollOutcome::Completed
            }
            Poll::Pending => {
                // Try POLLING → IDLE. If a wake arrived during our poll,
                // PoolWaker CAS'd POLLING → QUEUED, so this CAS fails.
                // In that case the caller MUST re-enqueue the task.
                //
                // IMPORTANT: We do NOT use a separate is_queued() check after
                // this — that would race with try_wake(). The CAS result is
                // the single authoritative signal for re-enqueue responsibility:
                // - CAS succeeds (→IDLE): future wakes go through try_wake()
                //   which handles IDLE→QUEUED + enqueue atomically.
                // - CAS fails (state=QUEUED): a wake arrived during poll; the
                //   waker did NOT enqueue (try_wake returns false for POLLING),
                //   so the caller must enqueue now.
                let cas = self.state.compare_exchange(
                    TASK_POLLING,
                    TASK_IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                if cas.is_ok() {
                    PollOutcome::Pending
                } else {
                    PollOutcome::PendingNeedsReenqueue
                }
            }
        }
    }

    /// Attempt to wake this task: transition IDLE → QUEUED.
    ///
    /// Returns `true` if the transition succeeded (caller should enqueue the task).
    /// Returns `false` if the task is already QUEUED, POLLING, or DONE.
    ///
    /// If the task is currently POLLING, we set QUEUED so it will be re-enqueued
    /// after the current poll completes (the polling thread checks for this).
    pub fn try_wake(&self) -> bool {
        // Fast path: IDLE → QUEUED
        if self
            .state
            .compare_exchange(TASK_IDLE, TASK_QUEUED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }

        // If currently POLLING, mark QUEUED so the polling thread re-enqueues
        // after its poll returns Pending. This is NOT an enqueue by us — the
        // polling thread will handle it. The CAS result is intentionally
        // discarded: if it fails, the state is either QUEUED (another wake
        // already set it) or DONE (task finished). Either way, no action needed.
        // Multiple wakes during poll are safe: only the first POLLING→QUEUED
        // succeeds; subsequent wakes see QUEUED and are no-ops.
        let _ = self.state.compare_exchange(
            TASK_POLLING,
            TASK_QUEUED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        false
    }

    /// Check if this task is in the DONE state.
    pub fn is_done(&self) -> bool {
        self.state.load(Ordering::Acquire) == TASK_DONE
    }

    /// Get the current state (for diagnostics/testing).
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// Get the task's unique identifier (for diagnostics/logging).
    pub fn id(&self) -> TaskId {
        self.id
    }
}

// Safety: The Mutex + CAS state machine ensures that at most one thread
// accesses the executor at a time.
unsafe impl Sync for ExecutorTask {}

/// A Waker implementation that wakes an ExecutorTask by transitioning it
/// IDLE → QUEUED and pushing it onto the registry's ready queue.
///
/// This is the bridge between the executor's WakeHandle (which calls
/// `waker.wake()`) and the pool's scheduling (ready queue + condvar).
pub struct PoolWaker {
    task: Arc<ExecutorTask>,
    registry: Arc<ExecutorRegistry>,
}

impl PoolWaker {
    /// Create a new PoolWaker for the given task and registry.
    pub fn new(task: Arc<ExecutorTask>, registry: Arc<ExecutorRegistry>) -> Self {
        Self { task, registry }
    }
}

impl Wake for PoolWaker {
    fn wake(self: Arc<Self>) {
        if self.task.try_wake() {
            self.registry.enqueue(Arc::clone(&self.task));
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.task.try_wake() {
            self.registry.enqueue(Arc::clone(&self.task));
        }
    }
}

/// Owns all active ExecutorTasks and their ready queue.
///
/// The ready queue uses one of two strategies:
///
/// - **No policy (`None`)** — plain `VecDeque::pop_front()`, O(1), zero comparisons.
///   This is the default and should be used unless priority ordering is needed.
/// - **With policy (`Some`)** — `BinaryHeap` backed by the policy's `compare`,
///   giving O(log n) insert and O(log n) dequeue. This is correct because
///   task metadata (priority, created_at) is stable while in the heap —
///   `mark_enqueued()` updates `created_at` only at insertion time, before the
///   entry enters the heap, so the heap invariant is never violated.
///
/// The registry uses the pool's existing condvar for unified wake/park — when a
/// task becomes ready, it notifies the condvar so a parked worker thread wakes up.
pub struct ExecutorRegistry {
    /// Strategy-specific queue storage.
    ready_queue: Mutex<ReadyQueue>,

    /// Scheduling policy. `None` means pure FIFO (no comparisons).
    schedule_policy: Option<Arc<dyn SchedulePolicy>>,

    /// Reference to the pool's condvar for waking parked worker threads.
    /// Set during initialization; None only in tests that don't use a pool.
    pool_condvar: Option<Arc<std::sync::Condvar>>,

    /// Next task ID counter.
    next_id: AtomicUsize,
}

/// Internal queue — either a plain FIFO or a policy-ordered heap.
enum ReadyQueue {
    /// Pure FIFO — pop from front, no comparisons.
    Fifo(std::collections::VecDeque<Arc<ExecutorTask>>),
    /// Binary heap — O(log n) for policy-based ordering.
    Heap(std::collections::BinaryHeap<PolicyEntry>),
}

impl ReadyQueue {
    fn is_empty(&self) -> bool {
        match self {
            ReadyQueue::Fifo(d) => d.is_empty(),
            ReadyQueue::Heap(h) => h.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ReadyQueue::Fifo(d) => d.len(),
            ReadyQueue::Heap(h) => h.len(),
        }
    }
}

/// Wrapper for BinaryHeap that carries the policy reference for ordering.
struct PolicyEntry {
    task: Arc<ExecutorTask>,
    policy: Arc<dyn SchedulePolicy>,
}

impl PartialEq for PolicyEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for PolicyEntry {}

impl PartialOrd for PolicyEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PolicyEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        let meta_a = self.task.task_meta();
        let meta_b = other.task.task_meta();
        match (meta_a, meta_b) {
            (Some(a), Some(b)) => {
                // BinaryHeap is a max-heap: pops the *greatest* element first.
                // Policy returns Less when `a` should run before `b`.
                // Reverse so the heap pops the "best" task first.
                self.policy.compare(&a, &b).reverse()
            }
            (Some(_), None) => CmpOrdering::Greater,
            (None, Some(_)) => CmpOrdering::Less,
            (None, None) => CmpOrdering::Equal,
        }
    }
}

impl ExecutorRegistry {
    /// Create a new registry that will notify the given condvar when tasks
    /// become ready. Pass the pool's `park_condvar` here.
    ///
    /// If `schedule_policy` is `None`, a plain FIFO queue is used (O(1) dequeue,
    /// no comparisons). If `Some`, tasks are ordered by the policy via a binary
    /// heap (O(log n) insert and dequeue).
    pub fn new(
        pool_condvar: Arc<std::sync::Condvar>,
        schedule_policy: Option<Arc<dyn SchedulePolicy>>,
    ) -> Self {
        let ready_queue = match &schedule_policy {
            None => ReadyQueue::Fifo(std::collections::VecDeque::new()),
            Some(_) => ReadyQueue::Heap(std::collections::BinaryHeap::new()),
        };
        Self {
            ready_queue: Mutex::new(ready_queue),
            schedule_policy,
            pool_condvar: Some(pool_condvar),
            next_id: AtomicUsize::new(0),
        }
    }

    /// Create a registry without pool integration (for testing).
    #[cfg(test)]
    pub fn new_standalone() -> Self {
        Self {
            ready_queue: Mutex::new(ReadyQueue::Fifo(std::collections::VecDeque::new())),
            schedule_policy: None,
            pool_condvar: None,
            next_id: AtomicUsize::new(0),
        }
    }

    /// Allocate the next TaskId. IDs are diagnostic-only and don't imply
    /// any ordering or scheduling priority.
    pub fn next_task_id(&self) -> TaskId {
        TaskId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Register a new task. The task starts in QUEUED state and is immediately
    /// placed in the ready queue.
    pub fn register(
        self: &Arc<Self>,
        executor: Pin<Box<dyn Future<Output = Result<bool>> + Send>>,
        notifier: CompletionNotifier,
        dataflow_id: DataflowId,
        priority: u32,
    ) -> Arc<ExecutorTask> {
        let id = self.next_task_id();
        let task = Arc::new(ExecutorTask::new(id, dataflow_id, priority, executor, notifier));
        self.enqueue(Arc::clone(&task));
        task
    }

    /// Push a task onto the ready queue and notify the pool.
    ///
    /// If the ready queue mutex is poisoned (should never happen — the
    /// operations inside are infallible), the task is silently dropped
    /// rather than panicking the pool thread.
    pub fn enqueue(&self, task: Arc<ExecutorTask>) {
        task.mark_enqueued();
        {
            match self.ready_queue.lock() {
                Ok(mut queue) => match &mut *queue {
                    ReadyQueue::Fifo(deque) => deque.push_back(task),
                    ReadyQueue::Heap(heap) => {
                        let policy = self.schedule_policy.as_ref().unwrap().clone();
                        heap.push(PolicyEntry { task, policy });
                    }
                },
                Err(_poisoned) => {
                    return;
                }
            }
        }
        // Notify a parked worker thread that work is available
        if let Some(cv) = &self.pool_condvar {
            cv.notify_one();
        }
    }

    /// Try to dequeue a ready task. Returns None if the queue is empty
    /// or if the queue mutex is poisoned.
    pub fn dequeue(&self) -> Option<Arc<ExecutorTask>> {
        let mut queue = self.ready_queue.lock().ok()?;
        match &mut *queue {
            ReadyQueue::Fifo(deque) => deque.pop_front(),
            ReadyQueue::Heap(heap) => heap.pop().map(|entry| entry.task),
        }
    }

    /// Check if the ready queue is empty.
    pub fn is_empty(&self) -> bool {
        self.ready_queue
            .lock()
            .map(|q| q.is_empty())
            .unwrap_or(true)
    }

    /// Number of tasks currently in the ready queue.
    pub fn ready_count(&self) -> usize {
        self.ready_queue.lock().map(|q| q.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A simple future that completes after N polls.
    struct CountdownFuture {
        remaining: usize,
    }

    impl Future for CountdownFuture {
        type Output = Result<bool>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.remaining == 0 {
                return Poll::Ready(Ok(true));
            }
            self.remaining -= 1;
            if self.remaining == 0 {
                Poll::Ready(Ok(true))
            } else {
                Poll::Pending
            }
        }
    }

    /// A future that always returns Pending (for testing wake mechanics).
    struct PendingForever;

    impl Future for PendingForever {
        type Output = Result<bool>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    fn make_notifier() -> (crate::runtime::DataflowCompletion, CompletionNotifier) {
        crate::runtime::DataflowCompletion::new()
    }

    struct NoopWaker;
    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn noop_cx() -> (std::task::Waker, ()) {
        let waker: std::task::Waker = Arc::new(NoopWaker).into();
        (waker, ())
    }

    #[test]
    fn task_starts_in_queued_state() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        assert_eq!(task.state(), TASK_QUEUED);
    }

    #[test]
    fn try_start_poll_transitions_queued_to_polling() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        assert!(task.try_start_poll());
        assert_eq!(task.state(), TASK_POLLING);
    }

    #[test]
    fn try_start_poll_fails_when_not_queued() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        // First claim succeeds
        assert!(task.try_start_poll());
        // Second claim from another "thread" fails
        assert!(!task.try_start_poll());
    }

    #[test]
    fn poll_pending_transitions_to_idle() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        assert!(task.try_start_poll());

        let (waker, _) = noop_cx();
        let mut cx = Context::from_waker(&waker);
        let completed = task.poll(&mut cx);
        assert_eq!(completed, PollOutcome::Pending);
        assert_eq!(task.state(), TASK_IDLE);
    }

    #[test]
    fn poll_ready_transitions_to_done() {
        let (completion, notifier) = make_notifier();
        // Future that completes on first poll
        let task = ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(CountdownFuture { remaining: 0 }),
            notifier,
        );
        assert!(task.try_start_poll());

        let (waker, _) = noop_cx();
        let mut cx = Context::from_waker(&waker);
        let completed = task.poll(&mut cx);
        assert_eq!(completed, PollOutcome::Completed);
        assert!(task.is_done());

        // CompletionNotifier should have fired
        assert!(completion.wait().is_ok());
    }

    #[test]
    fn try_wake_from_idle_returns_true() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        // Start poll, then return Pending → IDLE
        assert!(task.try_start_poll());
        let (waker, _) = noop_cx();
        let mut cx = Context::from_waker(&waker);
        task.poll(&mut cx);
        assert_eq!(task.state(), TASK_IDLE);

        // Wake from IDLE → QUEUED
        assert!(task.try_wake());
        assert_eq!(task.state(), TASK_QUEUED);
    }

    #[test]
    fn try_wake_from_queued_returns_false_dedup() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        // Already QUEUED — duplicate wake should be suppressed
        assert!(!task.try_wake());
        assert_eq!(task.state(), TASK_QUEUED);
    }

    #[test]
    fn try_wake_during_polling_sets_queued() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(TaskId(0), DataflowId::new(), 0, Box::pin(PendingForever), notifier);
        assert!(task.try_start_poll());
        assert_eq!(task.state(), TASK_POLLING);

        // Wake during poll — should set QUEUED but return false
        // (the polling thread will handle re-enqueue)
        assert!(!task.try_wake());
        assert_eq!(task.state(), TASK_QUEUED);
    }

    #[test]
    fn try_wake_from_done_returns_false() {
        let (_, notifier) = make_notifier();
        let task = ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(CountdownFuture { remaining: 0 }),
            notifier,
        );
        assert!(task.try_start_poll());
        let (waker, _) = noop_cx();
        let mut cx = Context::from_waker(&waker);
        task.poll(&mut cx);
        assert!(task.is_done());

        assert!(!task.try_wake());
        assert_eq!(task.state(), TASK_DONE);
    }

    #[test]
    fn registry_register_enqueues_task() {
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (_, notifier) = make_notifier();
        let _task = registry.register(Box::pin(PendingForever), notifier, DataflowId::new(), 0);
        assert_eq!(registry.ready_count(), 1);
    }

    #[test]
    fn registry_dequeue_returns_task() {
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (_, notifier) = make_notifier();
        let task = registry.register(Box::pin(PendingForever), notifier, DataflowId::new(), 0);
        let dequeued = registry.dequeue().unwrap();
        assert_eq!(dequeued.id, task.id);
        assert!(registry.is_empty());
    }

    #[test]
    fn registry_priority_dequeue_order() {
        use crate::scheduler::policy::PriorityPolicy;

        let condvar = Arc::new(std::sync::Condvar::new());
        let registry = Arc::new(ExecutorRegistry::new(
            condvar,
            Some(Arc::new(PriorityPolicy)),
        ));

        let (_, n1) = make_notifier();
        let (_, n2) = make_notifier();
        let (_, n3) = make_notifier();

        let low = registry.register(Box::pin(PendingForever), n1, DataflowId::new(), 1);
        let mid = registry.register(Box::pin(PendingForever), n2, DataflowId::new(), 50);
        let high = registry.register(Box::pin(PendingForever), n3, DataflowId::new(), 100);

        // Dequeue should return highest priority first
        let first = registry.dequeue().unwrap();
        let second = registry.dequeue().unwrap();
        let third = registry.dequeue().unwrap();

        assert_eq!(first.id, high.id, "highest priority should dequeue first");
        assert_eq!(second.id, mid.id, "mid priority should dequeue second");
        assert_eq!(third.id, low.id, "lowest priority should dequeue last");
        assert!(registry.dequeue().is_none());
    }

    #[test]
    fn registry_aging_policy_dequeue_order() {
        use crate::scheduler::policy::PriorityWithAgingPolicy;

        let condvar = Arc::new(std::sync::Condvar::new());
        let policy = PriorityWithAgingPolicy { aging_rate: 1000.0 };

        let registry = Arc::new(ExecutorRegistry::new(
            condvar,
            Some(Arc::new(policy)),
        ));

        let (_, n1) = make_notifier();
        let (_, n2) = make_notifier();

        // Enqueue low-priority first (will age), then high-priority
        let low = registry.register(Box::pin(PendingForever), n1, DataflowId::new(), 0);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let high = registry.register(Box::pin(PendingForever), n2, DataflowId::new(), 5);

        // With aging_rate=1000, 10ms of age = 10 effective priority for low
        // low effective = 0 + 10 = 10, high effective = 5 + ~0 = 5
        // So low should be dequeued first (it aged past high)
        let first = registry.dequeue().unwrap();
        let second = registry.dequeue().unwrap();

        assert_eq!(first.id, low.id, "aged low-priority should dequeue first");
        assert_eq!(second.id, high.id, "fresh high-priority should dequeue second");
    }

    #[test]
    fn pool_waker_enqueues_on_idle_to_queued() {
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (_, notifier) = make_notifier();
        let task = Arc::new(ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(PendingForever),
            notifier,
        ));

        // Get task to IDLE state: QUEUED → POLLING → poll(Pending) → IDLE
        assert!(task.try_start_poll());
        let (waker_val, _) = noop_cx();
        let mut cx = Context::from_waker(&waker_val);
        task.poll(&mut cx);
        assert_eq!(task.state(), TASK_IDLE);

        // PoolWaker should transition IDLE → QUEUED and enqueue
        let waker = Arc::new(PoolWaker::new(Arc::clone(&task), Arc::clone(&registry)));
        waker.wake();
        assert_eq!(task.state(), TASK_QUEUED);
        assert_eq!(registry.ready_count(), 1);
    }

    #[test]
    fn pool_waker_dedup_no_double_enqueue() {
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (_, notifier) = make_notifier();
        let task = Arc::new(ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(PendingForever),
            notifier,
        ));

        // Task starts QUEUED — wake should NOT enqueue again
        let waker = Arc::new(PoolWaker::new(Arc::clone(&task), Arc::clone(&registry)));
        waker.wake_by_ref();
        waker.wake_by_ref();
        assert_eq!(registry.ready_count(), 0); // nothing enqueued — already QUEUED
    }

    #[test]
    fn full_lifecycle_poll_to_completion() {
        // Task with a 2-poll countdown future:
        // Poll 1: remaining 2→1, returns Pending
        // Poll 2: remaining 1→0, returns Ready
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (completion, notifier) = make_notifier();
        let _task = registry.register(
            Box::pin(CountdownFuture { remaining: 2 }),
            notifier,
            DataflowId::new(),
            0,
        );

        // Dequeue and poll #1
        let t = registry.dequeue().unwrap();
        assert!(t.try_start_poll());
        let pool_waker = Arc::new(PoolWaker::new(Arc::clone(&t), Arc::clone(&registry)));
        let waker: std::task::Waker = pool_waker.into();
        let mut cx = Context::from_waker(&waker);
        let done = t.poll(&mut cx);
        assert_eq!(done, PollOutcome::Pending);
        assert_eq!(t.state(), TASK_IDLE);

        // Simulate wake (e.g., channel pushed data)
        assert!(t.try_wake());
        registry.enqueue(Arc::clone(&t));

        // Dequeue and poll #2
        let t2 = registry.dequeue().unwrap();
        assert!(t2.try_start_poll());
        let pool_waker2 = Arc::new(PoolWaker::new(Arc::clone(&t2), Arc::clone(&registry)));
        let waker2: std::task::Waker = pool_waker2.into();
        let mut cx2 = Context::from_waker(&waker2);
        let done = t2.poll(&mut cx2);
        assert_eq!(done, PollOutcome::Completed);
        assert!(t2.is_done());

        // Completion should be signaled
        assert!(completion.wait().is_ok());
    }

    #[test]
    fn panic_in_executor_transitions_to_done_and_notifies_error() {
        /// A future that panics on first poll.
        struct PanicFuture;
        impl Future for PanicFuture {
            type Output = Result<bool>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                panic!("executor panic");
            }
        }

        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (completion, notifier) = make_notifier();
        let task = Arc::new(ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(PanicFuture),
            notifier,
        ));

        assert!(task.try_start_poll());

        // Create a PoolWaker-based waker so the Arc cycle would form
        // if the future stored the waker before panicking.
        let pool_waker = Arc::new(PoolWaker::new(Arc::clone(&task), Arc::clone(&registry)));
        let waker: std::task::Waker = pool_waker.into();
        let mut cx = Context::from_waker(&waker);

        // Poll should panic — catch it
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.poll(&mut cx)));
        assert!(result.is_err(), "expected panic");

        // PanicGuard should have set state to DONE
        assert!(task.is_done());

        // PanicGuard should have dropped the future and taken the notifier,
        // so DataflowCompletion::wait() returns an error (not hang).
        assert!(completion.wait().is_err());
    }

    #[test]
    fn completed_task_drops_future_breaking_arc_cycle() {
        // Verify that after completion, the executor future is dropped
        // (breaking any PoolWaker→ExecutorTask→future→WakeHandle cycle).
        let registry = Arc::new(ExecutorRegistry::new_standalone());
        let (completion, notifier) = make_notifier();
        let task = Arc::new(ExecutorTask::new(
            TaskId(0),
            DataflowId::new(),
            0,
            Box::pin(CountdownFuture { remaining: 0 }),
            notifier,
        ));

        assert!(task.try_start_poll());
        let pool_waker = Arc::new(PoolWaker::new(Arc::clone(&task), Arc::clone(&registry)));
        let waker: std::task::Waker = pool_waker.into();
        let mut cx = Context::from_waker(&waker);

        let outcome = task.poll(&mut cx);
        assert_eq!(outcome, PollOutcome::Completed);
        assert!(task.is_done());

        // Executor future should be None (taken to break cycle)
        assert!(task.executor.lock().unwrap().is_none());

        // Completion should still work
        assert!(completion.wait().is_ok());
    }
}
