//! Custom worker thread pool for synchronous operator execution.
//!
//! The [`WorkerPool`] is a lightweight, purpose-built thread pool optimized
//! for short-to-medium synchronous computation tasks. Unlike Tokio, it has
//! no async overhead — worker threads simply dequeue tasks and run them.
//!
//! ## Thread Lifecycle
//!
//! Worker threads follow a **spin → yield → park → shutdown** idle strategy:
//! 1. Spinning (tight loop) — zero latency for back-to-back tasks
//! 2. Yielding — gives CPU while remaining responsive  
//! 3. Parking (condvar) — near-zero CPU, woken by new tasks
//! 4. Shutdown — threads above `min_threads` exit after idle timeout

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_deque::{Injector, Steal};

use crate::error::RuntimeError;
use crate::executor_task::{ExecutorRegistry, PoolWaker};
use crate::scheduler::policy::SchedulePolicy;
use crate::worker::OperatorActivation;

/// Configuration for the worker thread pool.
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    /// Minimum number of worker threads (always kept alive).
    pub min_threads: usize,
    /// Maximum number of worker threads the pool can grow to.
    pub max_threads: usize,
    /// How long a thread above `min_threads` can be idle before shutdown.
    pub idle_shutdown: Duration,
    /// Number of spin iterations before yielding.
    pub spin_limit: u32,
    /// Number of yield iterations before parking.
    pub yield_limit: u32,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            min_threads: cpus.max(2) / 2,
            max_threads: cpus,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 10,
            yield_limit: 20,
        }
    }
}

impl WorkerPoolConfig {
    /// Validate configuration.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.min_threads == 0 {
            return Err(RuntimeError::InvalidConfig(
                "min_threads must be at least 1".into(),
            ));
        }
        if self.max_threads < self.min_threads {
            return Err(RuntimeError::InvalidConfig(
                "max_threads must be >= min_threads".into(),
            ));
        }
        if self.spin_limit == 0 {
            return Err(RuntimeError::InvalidConfig(
                "spin_limit must be at least 1".into(),
            ));
        }
        if self.yield_limit <= self.spin_limit {
            return Err(RuntimeError::InvalidConfig(
                "yield_limit must be > spin_limit".into(),
            ));
        }
        Ok(())
    }
}

/// Shared state for the worker thread pool.
struct PoolState {
    /// The global task injector queue (one-shot closures).
    injector: Injector<OperatorActivation>,
    /// Condition variable for parking idle threads. Arc-wrapped so the
    /// executor registry can share it for unified wake/park.
    park_condvar: Arc<Condvar>,
    /// Mutex paired with the condvar (only used for waiting, not data protection).
    park_mutex: Mutex<()>,
    /// Current number of active (non-idle) threads.
    active_count: AtomicUsize,
    /// Total number of live threads.
    thread_count: AtomicUsize,
    /// Whether the pool is shutting down.
    shutdown: AtomicBool,
    /// Total tasks submitted (for metrics).
    tasks_submitted: AtomicUsize,
    /// Total tasks completed (for metrics).
    tasks_completed: AtomicUsize,
    /// Pool configuration.
    config: WorkerPoolConfig,
    /// Optional executor registry for cooperative polling of dataflow futures.
    /// When set, worker threads also poll executor tasks from this registry's
    /// ready queue alongside one-shot tasks from the injector.
    executor_registry: Mutex<Option<Arc<ExecutorRegistry>>>,
}

impl PoolState {
    fn new(config: WorkerPoolConfig) -> Self {
        Self {
            injector: Injector::new(),
            park_condvar: Arc::new(Condvar::new()),
            park_mutex: Mutex::new(()),
            active_count: AtomicUsize::new(0),
            thread_count: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            tasks_submitted: AtomicUsize::new(0),
            tasks_completed: AtomicUsize::new(0),
            config,
            executor_registry: Mutex::new(None),
        }
    }
}

/// A lightweight worker thread pool for synchronous operator execution.
///
/// Dynamically scales between `min_threads` and `max_threads` based on load.
/// Threads follow a spin → yield → park → shutdown lifecycle.
pub struct WorkerPool {
    state: Arc<PoolState>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl WorkerPool {
    /// Create a new worker pool with the given configuration.
    ///
    /// Spawns `min_threads` worker threads immediately. Returns an error if
    /// configuration is invalid or any initial thread fails to spawn.
    pub fn new(config: WorkerPoolConfig) -> Result<Self, RuntimeError> {
        config.validate()?;

        let state = Arc::new(PoolState::new(config.clone()));
        let threads = Mutex::new(Vec::with_capacity(config.max_threads));

        let pool = Self { state, threads };

        // Spawn initial min_threads
        for _ in 0..config.min_threads {
            pool.spawn_thread().map_err(|e| RuntimeError::SpawnFailed {
                context: "failed to spawn initial worker thread".into(),
                source: Some(e),
            })?;
        }

        Ok(pool)
    }

    /// Submit an operator activation for execution.
    pub fn submit(&self, task: OperatorActivation) {
        self.state.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        self.state.injector.push(task);

        // If all threads are busy and below max, try to spawn a new one atomically
        let active = self.state.active_count.load(Ordering::Acquire);
        let current = self.state.thread_count.load(Ordering::Acquire);
        if active >= current && current < self.state.config.max_threads {
            self.try_spawn_thread();
        }

        // Wake a parked thread
        self.state.park_condvar.notify_one();
    }

    /// Shut down the pool, waiting for all threads to complete.
    pub fn shutdown(&self) {
        self.state.shutdown.store(true, Ordering::SeqCst);
        // Wake all parked threads so they see the shutdown flag.
        // Hold the mutex briefly to avoid the race where a thread is between
        // lock() and wait_timeout() — without the mutex, notify_all can fire
        // before the thread enters the wait, causing it to miss the signal.
        {
            let _guard = self
                .state
                .park_mutex
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.state.park_condvar.notify_all();
        }

        if let Ok(mut threads) = self.threads.lock() {
            for handle in threads.drain(..) {
                let _ = handle.join();
            }
        }
    }

    /// Returns the number of currently active (executing) threads.
    pub fn active_threads(&self) -> usize {
        self.state.active_count.load(Ordering::Relaxed)
    }

    /// Returns the total number of live threads in the pool.
    pub fn thread_count(&self) -> usize {
        self.state.thread_count.load(Ordering::Relaxed)
    }

    /// Returns total tasks submitted since pool creation.
    pub fn tasks_submitted(&self) -> usize {
        self.state.tasks_submitted.load(Ordering::Relaxed)
    }

    /// Returns total tasks completed since pool creation.
    pub fn tasks_completed(&self) -> usize {
        self.state.tasks_completed.load(Ordering::Relaxed)
    }

    /// Returns true if the pool is in shutdown mode.
    pub fn is_shutdown(&self) -> bool {
        self.state.shutdown.load(Ordering::Relaxed)
    }

    /// Create an [`ExecutorRegistry`] wired to this pool's condvar, set it
    /// as this pool's registry, and return a shared reference.
    ///
    /// Worker threads will begin polling executor tasks from the registry's
    /// ready queue alongside one-shot tasks from the injector.
    ///
    /// Panics if a registry is already set (call only once per pool).
    pub fn create_registry(
        &self,
        schedule_policy: Option<Arc<dyn SchedulePolicy>>,
    ) -> crate::Result<Arc<ExecutorRegistry>> {
        let mut guard = self
            .state
            .executor_registry
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if guard.is_some() {
            return Err(crate::Error::Runtime(RuntimeError::AlreadyConsumed {
                resource: "ExecutorRegistry for this pool".into(),
            }));
        }
        // Share the pool's condvar so executor wakeups unpark worker threads.
        let registry = Arc::new(ExecutorRegistry::new(
            Arc::clone(&self.state.park_condvar),
            schedule_policy,
        ));
        *guard = Some(Arc::clone(&registry));
        Ok(registry)
    }

    /// Get the executor registry, if one has been created.
    pub fn registry(&self) -> Option<Arc<ExecutorRegistry>> {
        self.state
            .executor_registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Attempt to spawn a new worker thread, atomically reserving a slot.
    /// Returns true if a thread was spawned, false if at max capacity or spawn failed.
    fn try_spawn_thread(&self) -> bool {
        // Atomically reserve a slot below max_threads
        loop {
            let current = self.state.thread_count.load(Ordering::Acquire);
            if current >= self.state.config.max_threads {
                return false;
            }
            if self
                .state
                .thread_count
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        let state = self.state.clone();
        let handle = match thread::Builder::new()
            .name("instancy-worker".to_string())
            .spawn(move || {
                worker_thread_loop(&state);
            }) {
            Ok(h) => h,
            Err(_) => {
                // Undo the reservation — spawn failed.
                self.state.thread_count.fetch_sub(1, Ordering::SeqCst);
                return false;
            }
        };

        if let Ok(mut threads) = self.threads.lock() {
            threads.push(handle);
        }
        true
    }

    /// Spawn a thread unconditionally (used during pool initialization).
    /// Returns an error if the OS refuses to create the thread.
    fn spawn_thread(&self) -> std::result::Result<(), std::io::Error> {
        self.state.thread_count.fetch_add(1, Ordering::SeqCst);

        let state = self.state.clone();
        let handle = match thread::Builder::new()
            .name("instancy-worker".to_string())
            .spawn(move || {
                worker_thread_loop(&state);
            }) {
            Ok(h) => h,
            Err(e) => {
                self.state.thread_count.fetch_sub(1, Ordering::SeqCst);
                return Err(e);
            }
        };

        if let Ok(mut threads) = self.threads.lock() {
            threads.push(handle);
        }
        Ok(())
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// The main loop for each worker thread.
///
/// Tries work in priority order:
/// 1. One-shot tasks from the global injector queue
/// 2. Executor tasks from the registry's ready queue (if registry is set)
///
/// On exit, decrements `thread_count`.
fn worker_thread_loop(state: &PoolState) {
    // Ensure thread_count is decremented when this thread exits, regardless of path.
    struct ThreadGuard<'a>(&'a PoolState);
    impl Drop for ThreadGuard<'_> {
        fn drop(&mut self) {
            self.0.thread_count.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _guard = ThreadGuard(state);

    let mut idle_cycles: u32 = 0;

    loop {
        // Check shutdown
        if state.shutdown.load(Ordering::Relaxed) {
            return;
        }

        // Priority 1: Try one-shot tasks from the global injector queue
        match state.injector.steal() {
            Steal::Success(task) => {
                idle_cycles = 0;
                state.active_count.fetch_add(1, Ordering::Relaxed);

                // Execute with panic safety — ensure active_count is decremented
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    task.execute();
                }));

                state.active_count.fetch_sub(1, Ordering::Relaxed);
                state.tasks_completed.fetch_add(1, Ordering::Relaxed);

                if let Err(_panic) = result {
                    // Task panicked — continue serving.
                }
                continue;
            }
            Steal::Empty | Steal::Retry => {}
        }

        // Priority 2: Try executor tasks from the registry's ready queue
        if let Some(registry) = state
            .executor_registry
            .lock()
            .ok()
            .and_then(|g| g.as_ref().cloned())
        {
            if let Some(task) = registry.dequeue() {
                if task.try_start_poll() {
                    idle_cycles = 0;
                    state.active_count.fetch_add(1, Ordering::Relaxed);

                    // Create a PoolWaker so the executor's WakeHandle can
                    // re-enqueue this task when channels push data.
                    let pool_waker =
                        Arc::new(PoolWaker::new(Arc::clone(&task), Arc::clone(&registry)));
                    let waker: std::task::Waker = pool_waker.into();
                    let mut cx = std::task::Context::from_waker(&waker);

                    // Poll with panic safety
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        task.poll(&mut cx)
                    }));

                    state.active_count.fetch_sub(1, Ordering::Relaxed);

                    match result {
                        Ok(outcome) => {
                            use crate::executor_task::PollOutcome;
                            if outcome == PollOutcome::PendingNeedsReenqueue {
                                // A wake arrived during our poll: PoolWaker set
                                // POLLING→QUEUED but couldn't enqueue. We are
                                // the sole thread responsible for re-enqueuing.
                                registry.enqueue(Arc::clone(&task));
                            }
                            // Completed: task is DONE, notifier fired.
                            // Pending: task is IDLE, future wakes handle re-enqueue.
                        }
                        Err(_panic) => {
                            // Executor panicked. PanicGuard transitions to DONE,
                            // drops the future (breaking Arc cycles), and takes
                            // the notifier (whose Drop fires the error path).
                        }
                    }
                    continue;
                }
                // try_start_poll failed — task was already claimed or done.
                // Fall through to idle strategy.
            }
        }

        // No work available — idle strategy
        idle_cycles = idle_cycles.saturating_add(1);

        if idle_cycles < state.config.spin_limit {
            // Phase 1: spin
            std::hint::spin_loop();
        } else if idle_cycles < state.config.yield_limit {
            // Phase 2: yield
            thread::yield_now();
        } else {
            // Phase 3: park on condvar
            // Re-check shutdown before parking (avoids missing a notify_all
            // that fired while we were in the spin/yield phase).
            if state.shutdown.load(Ordering::Relaxed) {
                return;
            }

            let parked_at = Instant::now();

            let guard = match state.park_mutex.lock() {
                Ok(g) => g,
                Err(_) => return, // mutex poisoned, exit thread gracefully
            };

            // Re-check shutdown while holding park_mutex. shutdown() holds
            // this same mutex during notify_all(), so if shutdown happened
            // between our earlier check and this lock acquisition, we'll
            // see it here (no lost wakeup possible).
            if state.shutdown.load(Ordering::Acquire) {
                return;
            }

            let _result = match state
                .park_condvar
                .wait_timeout(guard, state.config.idle_shutdown)
            {
                Ok(r) => r,
                Err(_) => return, // mutex poisoned during wait, exit gracefully
            };

            // After waking, check if we should shut down this thread
            if state.shutdown.load(Ordering::Relaxed) {
                return;
            }

            // Re-check queues before deciding to exit (avoid lost wake-up starvation)
            let has_injector_work = !state.injector.is_empty();
            let has_executor_work = state
                .executor_registry
                .lock()
                .ok()
                .and_then(|g| g.as_ref().map(|r| !r.is_empty()))
                .unwrap_or(false);

            if has_injector_work || has_executor_work {
                idle_cycles = 0;
                continue;
            }

            // If we timed out and we're above min_threads, exit.
            // The guard's Drop will decrement thread_count. We check if
            // current count > min_threads. (Benign race: at worst one extra
            // thread exits, but new submits will respawn it.)
            if parked_at.elapsed() >= state.config.idle_shutdown
                && state.thread_count.load(Ordering::SeqCst) > state.config.min_threads
            {
                return;
            }

            // Reset idle cycles to re-enter spin phase
            idle_cycles = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn config_default_is_valid() {
        let config = WorkerPoolConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.min_threads >= 1);
        assert!(config.max_threads >= config.min_threads);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn config_validation_errors() {
        let mut config = WorkerPoolConfig::default();

        config.min_threads = 0;
        assert!(matches!(
            config.validate(),
            Err(RuntimeError::InvalidConfig(ref message)) if message == "min_threads must be at least 1"
        ));

        config.min_threads = 4;
        config.max_threads = 2;
        assert!(matches!(
            config.validate(),
            Err(RuntimeError::InvalidConfig(ref message)) if message == "max_threads must be >= min_threads"
        ));

        config.max_threads = 8;
        config.spin_limit = 0;
        assert!(matches!(
            config.validate(),
            Err(RuntimeError::InvalidConfig(ref message)) if message == "spin_limit must be at least 1"
        ));

        config.spin_limit = 10;
        config.yield_limit = 5;
        assert!(matches!(
            config.validate(),
            Err(RuntimeError::InvalidConfig(ref message)) if message == "yield_limit must be > spin_limit"
        ));
    }

    #[test]
    fn pool_creation_spawns_min_threads() {
        let config = WorkerPoolConfig {
            min_threads: 3,
            max_threads: 8,
            idle_shutdown: Duration::from_secs(1),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        // Give threads time to start
        thread::sleep(Duration::from_millis(50));
        assert_eq!(pool.thread_count(), 3);
        pool.shutdown();
    }

    #[test]
    fn pool_executes_tasks() {
        let config = WorkerPoolConfig {
            min_threads: 2,
            max_threads: 4,
            idle_shutdown: Duration::from_secs(5),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        let counter = Arc::new(AtomicU32::new(0));

        for i in 0..10 {
            let counter = counter.clone();
            pool.submit(OperatorActivation::new(
                crate::worker::WorkerId::new(i % 2),
                "test",
                i,
                move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                },
            ));
        }

        // Wait for all tasks
        let start = Instant::now();
        while counter.load(Ordering::SeqCst) < 10 {
            thread::sleep(Duration::from_millis(10));
            if start.elapsed() > Duration::from_secs(5) {
                panic!("tasks did not complete in time");
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
        assert_eq!(pool.tasks_submitted(), 10);
        assert_eq!(pool.tasks_completed(), 10);
        pool.shutdown();
    }

    #[test]
    fn pool_grows_under_load() {
        let config = WorkerPoolConfig {
            min_threads: 1,
            max_threads: 4,
            idle_shutdown: Duration::from_secs(5),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        let running = Arc::new(AtomicU32::new(0));
        let max_concurrent = Arc::new(AtomicU32::new(0));
        let done = Arc::new(AtomicU32::new(0));

        // Submit tasks that sleep briefly — should cause pool to grow
        for i in 0..4 {
            let running = running.clone();
            let max_concurrent = max_concurrent.clone();
            let done = done.clone();
            pool.submit(OperatorActivation::new(
                crate::worker::WorkerId::new(i),
                "sleeper",
                i,
                move || {
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    // Update max concurrent
                    loop {
                        let prev_max = max_concurrent.load(Ordering::SeqCst);
                        if current <= prev_max {
                            break;
                        }
                        if max_concurrent
                            .compare_exchange(
                                prev_max,
                                current,
                                Ordering::SeqCst,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                    thread::sleep(Duration::from_millis(100));
                    running.fetch_sub(1, Ordering::SeqCst);
                    done.fetch_add(1, Ordering::SeqCst);
                },
            ));
            // Small delay between submits to let pool detect load
            thread::sleep(Duration::from_millis(10));
        }

        // Wait for tasks to complete
        let start = Instant::now();
        while done.load(Ordering::SeqCst) < 4 {
            thread::sleep(Duration::from_millis(20));
            if start.elapsed() > Duration::from_secs(5) {
                panic!("tasks did not complete in time");
            }
        }

        // Pool should have grown beyond 1
        assert!(max_concurrent.load(Ordering::SeqCst) > 1 || pool.thread_count() > 1);
        pool.shutdown();
    }

    #[test]
    fn pool_shrinks_on_idle() {
        let config = WorkerPoolConfig {
            min_threads: 1,
            max_threads: 4,
            idle_shutdown: Duration::from_millis(200),
            spin_limit: 2,
            yield_limit: 4,
        };
        let pool = WorkerPool::new(config).unwrap();
        let done = Arc::new(AtomicU32::new(0));

        // Submit tasks that sleep to force growth
        for i in 0..4 {
            let done = done.clone();
            pool.submit(OperatorActivation::new(
                crate::worker::WorkerId::new(i),
                "sleeper",
                i,
                move || {
                    thread::sleep(Duration::from_millis(50));
                    done.fetch_add(1, Ordering::SeqCst);
                },
            ));
            thread::sleep(Duration::from_millis(5));
        }

        // Wait for tasks to finish
        let start = Instant::now();
        while done.load(Ordering::SeqCst) < 4 {
            thread::sleep(Duration::from_millis(20));
            if start.elapsed() > Duration::from_secs(3) {
                panic!("tasks did not complete");
            }
        }

        // Wait for idle timeout + some buffer
        thread::sleep(Duration::from_millis(500));

        // Should have shrunk back toward min_threads
        let count = pool.thread_count();
        assert!(count <= 2, "expected <=2, got {count}");
        pool.shutdown();
    }

    #[test]
    fn pool_shutdown_stops_all_threads() {
        let config = WorkerPoolConfig {
            min_threads: 4,
            max_threads: 8,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(pool.thread_count(), 4);
        pool.shutdown();
        assert_eq!(pool.thread_count(), 0);
    }

    #[test]
    fn pool_idle_threads_low_cpu() {
        // This is a basic sanity test — idle pool shouldn't burn CPU
        let config = WorkerPoolConfig {
            min_threads: 2,
            max_threads: 4,
            idle_shutdown: Duration::from_secs(5),
            spin_limit: 2,
            yield_limit: 4,
        };
        let pool = WorkerPool::new(config).unwrap();
        // Just let it idle for a bit — no panics, no busy-wait errors
        thread::sleep(Duration::from_millis(200));
        // Threads should be parked, not burning CPU
        assert_eq!(pool.active_threads(), 0);
        pool.shutdown();
    }

    #[test]
    fn pool_polls_executor_tasks_to_completion() {
        // Verify that pool worker threads poll executor tasks from the
        // registry's ready queue and run them to completion.
        use crate::runtime::DataflowCompletion;
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct CompletesOnFirstPoll;
        impl Future for CompletesOnFirstPoll {
            type Output = crate::error::Result<bool>;
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Ready(Ok(true))
            }
        }

        let config = WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        let registry = pool.create_registry(None).unwrap();

        let (completion, notifier) = DataflowCompletion::new();
        registry.register(
            Box::pin(CompletesOnFirstPoll),
            notifier,
            crate::dataflow::DataflowId::new(),
            0,
        );

        // Wait for completion
        let result = completion.wait();
        assert!(result.is_ok());
        pool.shutdown();
    }

    #[test]
    fn pool_polls_multiple_executors_cooperatively() {
        // Two executor futures on a single-thread pool. Each counts down
        // and yields on each poll. Both should complete.
        use crate::runtime::DataflowCompletion;
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct CountdownFuture {
            remaining: usize,
        }
        impl Future for CountdownFuture {
            type Output = crate::error::Result<bool>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.remaining <= 1 {
                    return Poll::Ready(Ok(true));
                }
                self.remaining -= 1;
                // Re-schedule via waker so the pool re-polls us
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        let config = WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 5,
            yield_limit: 10,
        };
        let pool = WorkerPool::new(config).unwrap();
        let registry = pool.create_registry(None).unwrap();

        let (comp1, notifier1) = DataflowCompletion::new();
        let (comp2, notifier2) = DataflowCompletion::new();

        registry.register(
            Box::pin(CountdownFuture { remaining: 5 }),
            notifier1,
            crate::dataflow::DataflowId::new(),
            0,
        );
        registry.register(
            Box::pin(CountdownFuture { remaining: 5 }),
            notifier2,
            crate::dataflow::DataflowId::new(),
            0,
        );

        // Both should complete
        assert!(comp1.wait().is_ok());
        assert!(comp2.wait().is_ok());
        pool.shutdown();
    }
}
