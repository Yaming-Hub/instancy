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

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// Schedule tests substitute modeled synchronization and threads for the
// pool's park/wake protocol (Linux only): Loom's under `--cfg instancy_loom`
// (see `pool_loom`), Shuttle's under `--cfg instancy_shuttle` (see
// `pool_shuttle`).
#[cfg(all(test, target_os = "linux", instancy_loom))]
use loom::{
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    thread::{self, JoinHandle},
};
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
use shuttle::{
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    thread::{self, JoinHandle},
};
#[cfg(not(all(test, target_os = "linux", any(instancy_loom, instancy_shuttle))))]
use std::{
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
    thread::{self, JoinHandle},
};

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
    /// Serializes the final queue check and wait with every queue notification.
    park_mutex: Arc<Mutex<()>>,
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
    #[cfg(test)]
    before_park: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl PoolState {
    fn new(config: WorkerPoolConfig) -> Self {
        Self {
            injector: Injector::new(),
            park_condvar: Arc::new(Condvar::new()),
            park_mutex: Arc::new(Mutex::new(())),
            active_count: AtomicUsize::new(0),
            thread_count: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            tasks_submitted: AtomicUsize::new(0),
            tasks_completed: AtomicUsize::new(0),
            config,
            executor_registry: Mutex::new(None),
            #[cfg(test)]
            before_park: Mutex::new(None),
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

        // Serialize with the worker's final queue check and entry into wait.
        let _guard = self
            .state
            .park_mutex
            .lock()
            .unwrap_or_else(|e| e.into_inner());
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

        // No thread will poll executor tasks again. Complete the ones still
        // pending so their DataflowCompletions resolve and their executors
        // (and every channel endpoint they own) are released.
        if let Some(registry) = self.registry() {
            registry.abort_all();
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
        // Share both halves of the wake/park protocol with executor enqueues.
        let registry = Arc::new(
            ExecutorRegistry::new(Arc::clone(&self.state.park_condvar), schedule_policy)
                .with_pool_park_mutex(Arc::clone(&self.state.park_mutex)),
        );
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
                            // ExecutorTask::poll contains executor panics; this
                            // only guards against a panic in the task machinery.
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

            #[cfg(test)]
            if let Some(before_park) = state.before_park.lock().unwrap().take() {
                before_park();
            }

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

            // Work can arrive after the dequeue attempts above. Recheck while
            // holding the same mutex enqueuers acquire before notifying, so a
            // notification cannot fall between this check and entering wait.
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

/// One worker, never above the minimum: no thread is spawned on demand and
/// none exits idle. Used by the schedule tests below.
#[cfg(all(test, target_os = "linux", any(instancy_loom, instancy_shuttle)))]
fn one_worker() -> WorkerPoolConfig {
    WorkerPoolConfig {
        min_threads: 1,
        max_threads: 1,
        idle_shutdown: Duration::from_secs(3600),
        spin_limit: 1,
        yield_limit: 2,
    }
}

/// Model-checked tests of the park/wake protocol, under Loom (Linux,
/// `RUSTFLAGS="--cfg instancy_loom"`; see `docs/testing/nextest.md`). One
/// worker runs the real loop while the model thread registers an executor
/// and then shuts the pool down; Loom explores every interleaving of the
/// operations it sees, without a preemption bound. Loom's
/// `Condvar::wait_timeout` never times out, so a missed notification
/// deadlocks the model instead of waiting for the idle timeout. The Crossbeam
/// injector is invisible to Loom, and stays empty here; submitted
/// activations and an idle shutdown are tested under Shuttle (`pool_shuttle`).
#[cfg(all(test, target_os = "linux", instancy_loom))]
mod pool_loom {
    use super::*;
    use crate::dataflow::DataflowId;
    use crate::runtime::DataflowCompletion;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize as Count;
    use std::task::{Context, Poll};

    /// Run `body` in every interleaving Loom distinguishes; return how many
    /// executions passed. The bounds are set here only.
    fn explore(passed: &'static Count, body: impl Fn() + Sync + Send + 'static) -> usize {
        for var in [
            "LOOM_MAX_PREEMPTIONS",
            "LOOM_MAX_BRANCHES",
            "LOOM_MAX_PERMUTATIONS",
            "LOOM_MAX_DURATION",
            "LOOM_CHECKPOINT_FILE",
        ] {
            assert!(
                std::env::var_os(var).is_none(),
                "{var} would change the model's bounds"
            );
        }
        passed.store(0, Ordering::Relaxed);
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = None;
        builder.max_branches = 10_000;
        builder.max_permutations = None;
        builder.max_duration = None;
        builder.check(move || {
            body();
            passed.fetch_add(1, Ordering::Relaxed);
        });
        let executions = passed.load(Ordering::Relaxed);
        println!("{executions} executions, no preemption bound");
        assert!(executions > 0, "no execution ran");
        executions
    }

    /// Signals each poll, and completes.
    struct SignalsPoll(loom::sync::mpsc::Sender<()>);

    impl Future for SignalsPoll {
        type Output = crate::Result<bool>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let _ = self.0.send(());
            Poll::Ready(Ok(true))
        }
    }

    /// An executor registered while the worker heads for the park mutex is
    /// polled, once, to completion without the idle timeout.
    #[test]
    fn registered_executor_wakes_the_worker() {
        static PASSED: Count = Count::new(0);
        explore(&PASSED, || {
            let pool = WorkerPool::new(one_worker()).unwrap();
            let registry = pool.create_registry(None).unwrap();
            let (polled, polls) = loom::sync::mpsc::channel();
            let (completion, notifier) = DataflowCompletion::new();
            registry.register(
                Box::pin(SignalsPoll(polled)),
                notifier,
                DataflowId::from_bytes([0x28; 16]),
                0,
            );
            polls.recv().expect("the executor is polled");
            pool.shutdown();
            assert!(polls.try_recv().is_err(), "the executor is polled once");
            assert!(completion.wait().is_ok(), "the executor completed");
            assert_eq!(pool.thread_count(), 0);
        });
    }
}

/// Schedule tests of the park/wake protocol, under Shuttle (Linux,
/// `RUSTFLAGS="--cfg instancy_shuttle"`; see `docs/testing/nextest.md`). One
/// worker runs the real loop while the test thread submits an activation or
/// shuts the pool down. The Crossbeam injector is opaque to both tools:
/// Loom's pruning treats its push and steal as unrelated and misses the lost
/// wakeup, and Shuttle's exhaustive search of a submission does not finish,
/// so Shuttle samples a fixed set of its schedules with PCT. An idle
/// shutdown is searched exhaustively. Shuttle's `Condvar::wait_timeout`
/// never times out, so a missed notification deadlocks the test instead of
/// waiting for the idle timeout.
#[cfg(all(test, target_os = "linux", instancy_shuttle))]
mod pool_shuttle {
    use super::*;
    use crate::worker::{OperatorActivation, WorkerId};
    use std::sync::atomic::AtomicUsize as Count;

    /// The only seed: `SHUTTLE_RANDOM_SEED` must be unset.
    const SEED: u64 = 0x285;
    /// Sampled executions, not distinct interleavings.
    const EXECUTIONS: usize = 5_000;

    /// An activation submitted while the worker heads for the park mutex
    /// runs, once, without the idle timeout.
    #[test]
    fn submitted_activation_wakes_the_worker() {
        static PASSED: Count = Count::new(0);
        assert!(
            std::env::var_os("SHUTTLE_RANDOM_SEED").is_none(),
            "SHUTTLE_RANDOM_SEED would replace the test's seed"
        );
        PASSED.store(0, Ordering::Relaxed);
        let mut config = shuttle::Config::new();
        config.stack_size = 0x10_0000;
        config.max_steps = shuttle::MaxSteps::FailAfter(10_000);
        config.max_time = None;
        let scheduler = shuttle::scheduler::PctScheduler::new_from_seed(SEED, 3, EXECUTIONS);
        let executions = shuttle::Runner::new(scheduler, config).run(|| {
            let pool = WorkerPool::new(one_worker()).unwrap();
            let (ran, runs) = shuttle::sync::mpsc::channel();
            pool.submit(OperatorActivation::new(
                WorkerId(0),
                "submitted",
                0,
                move || {
                    let _ = ran.send(());
                },
            ));
            runs.recv().expect("the activation runs");
            pool.shutdown();
            assert!(runs.try_recv().is_err(), "the activation runs once");
            assert_eq!(pool.tasks_completed(), 1);
            assert_eq!(pool.thread_count(), 0);
            PASSED.fetch_add(1, Ordering::Relaxed);
        });
        println!("seed {SEED:#x}: {executions} sampled executions");
        assert_eq!(executions, EXECUTIONS);
        assert_eq!(PASSED.load(Ordering::Relaxed), EXECUTIONS);
    }

    /// Shutdown stops a worker anywhere on its way to the park mutex. Every
    /// interleaving, depth first: Loom's handling of the worker's yield never
    /// lets it reach the wait before the shutdown flag is set.
    #[test]
    fn shutdown_stops_an_idle_worker() {
        static PASSED: Count = Count::new(0);
        PASSED.store(0, Ordering::Relaxed);
        let mut config = shuttle::Config::new();
        config.stack_size = 0x10_0000;
        config.max_steps = shuttle::MaxSteps::FailAfter(10_000);
        config.max_time = None;
        let scheduler = shuttle::scheduler::DfsScheduler::new(None, false);
        let executions = shuttle::Runner::new(scheduler, config).run(|| {
            let pool = WorkerPool::new(one_worker()).unwrap();
            pool.shutdown();
            assert_eq!(pool.thread_count(), 0);
            PASSED.fetch_add(1, Ordering::Relaxed);
        });
        println!("{executions} interleavings");
        assert!(executions > 0, "no interleaving ran");
        assert_eq!(PASSED.load(Ordering::Relaxed), executions);
    }
}

// Native tests: under `--cfg instancy_loom` or `--cfg instancy_shuttle` the
// pool's primitives are modeled ones, which work only inside a model run.
#[cfg(all(
    test,
    not(all(target_os = "linux", any(instancy_loom, instancy_shuttle)))
))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    fn check_enqueue_between_empty_check_and_park(executor: bool) {
        let state = Arc::new(PoolState::new(WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 1,
            yield_limit: 2,
        }));
        let (before_park_tx, before_park_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
        *state.before_park.lock().unwrap() = Some(Box::new(move || {
            before_park_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }));
        let pool = WorkerPool {
            state,
            threads: Mutex::new(Vec::new()),
        };
        let registry = pool.create_registry(None).unwrap();
        pool.spawn_thread().unwrap();
        before_park_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let (polled_tx, polled_rx) = std::sync::mpsc::sync_channel(1);
        if executor {
            let (_completion, notifier) = crate::runtime::DataflowCompletion::new();
            registry.register(
                Box::pin(async move {
                    polled_tx.send(()).unwrap();
                    Ok(true)
                }),
                notifier,
                crate::dataflow::DataflowId::new(),
                0,
            );
        } else {
            pool.submit(OperatorActivation::new(
                crate::worker::WorkerId::new(0),
                "queued-before-park",
                0,
                move || polled_tx.send(()).unwrap(),
            ));
        }
        resume_tx.send(()).unwrap();
        let polled = polled_rx.recv_timeout(Duration::from_secs(1));
        pool.shutdown();
        assert!(
            polled.is_ok(),
            "work queued before parking must not wait for the 30-second idle timeout"
        );
    }

    #[test]
    fn executor_enqueued_between_empty_check_and_park_is_polled() {
        check_enqueue_between_empty_check_and_park(true);
    }

    #[test]
    fn activation_enqueued_between_empty_check_and_park_is_executed() {
        check_enqueue_between_empty_check_and_park(false);
    }

    #[cfg(feature = "transport")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wake_race_shared_cluster_completes_when_registered_before_park() {
        use crate::communication::ClusterSpawnTransport;
        use crate::communication::shared_pool::SharedConnectionConfig;
        use crate::communication::shared_transport::{ConnectionFactory, SharedPeerManager};
        use crate::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig};
        use crate::{RuntimeConfig, RuntimeHandle, SpawnOptions};
        use std::collections::HashMap;
        use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

        struct DuplexFactory(Mutex<Option<DuplexStream>>);
        impl ConnectionFactory for DuplexFactory {
            type Reader = ReadHalf<DuplexStream>;
            type Writer = WriteHalf<DuplexStream>;

            async fn establish(
                &self,
                _peer_node_id: &str,
            ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
            {
                let stream = self
                    .0
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or("connection already taken")?;
                Ok(tokio::io::split(stream))
            }
        }

        let handle = tokio::runtime::Handle::current();
        let config = SharedConnectionConfig {
            ..Default::default()
        };
        let manager = |peer: &str, stream| {
            Arc::new(HashMap::from([(
                peer.to_string(),
                SharedPeerManager::new(
                    peer.to_string(),
                    config.clone(),
                    Arc::new(DuplexFactory(Mutex::new(Some(stream)))),
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap(),
            )]))
        };
        let (stream_a, stream_b) = tokio::io::duplex(65536);
        let managers_a = manager("node-b", stream_a);
        let managers_b = manager("node-a", stream_b);
        let runtime = || {
            RuntimeHandle::new(RuntimeConfig {
                worker_threads: 1,
                ..Default::default()
            })
            .unwrap()
        };
        let rt_a = runtime();
        let rt_b = runtime();
        let pool = rt_a.worker_pool();
        let (before_park_tx, before_park_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
        *pool.state.before_park.lock().unwrap() = Some(Box::new(move || {
            before_park_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }));

        // Only the setup loop wakes an already parked worker. Stop waking it
        // once the hook has captured the empty-check/park gap.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            {
                let _guard = pool.state.park_mutex.lock().unwrap();
                pool.state.park_condvar.notify_one();
            }
            if before_park_rx
                .recv_timeout(Duration::from_millis(10))
                .is_ok()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker did not reach the park hook"
            );
        }

        let topology = ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 1),
            NodeConfig::new("node-b", 1),
        ])
        .unwrap();
        let dataflow_id = DataflowId::new();
        let build = |builder: &mut DataflowBuilder<u64>| -> crate::Result<()> {
            builder
                .input::<u64>("data")?
                .map("identity", |_time, value| value)
                .exchange("by-value", |value| *value)
                .map("increment", |_time, value| value + 1)
                .output("results")?;
            Ok(())
        };
        let (ca, cb) = tokio::join!(
            rt_a.spawn_cluster_async(
                "park-race",
                topology.clone(),
                "node-a",
                dataflow_id,
                ClusterSpawnTransport::shared(managers_a.clone(), 1024),
                Duration::from_secs(5),
                build,
                &handle,
                SpawnOptions::new(),
            ),
            rt_b.spawn_cluster_async(
                "park-race",
                topology,
                "node-b",
                dataflow_id,
                ClusterSpawnTransport::shared(managers_b.clone(), 1024),
                Duration::from_secs(5),
                build,
                &handle,
                SpawnOptions::new(),
            ),
        );
        let mut ca = ca.unwrap();
        let mut cb = cb.unwrap();
        let out_a = ca.take_output::<u64>(0, "results").unwrap();
        let out_b = cb.take_output::<u64>(0, "results").unwrap();
        let input = ca.take_input::<u64>(0, "data").unwrap();
        input.send(1, vec![0, 1, 2, 3, 4]).unwrap();
        drop(input);
        drop(cb.take_input::<u64>(0, "data").unwrap());
        let completion_a = ca.join().unwrap();
        let completion_b = cb.join().unwrap();
        assert_eq!(pool.registry().unwrap().ready_count(), 1);

        // Registration and every input wake have already happened; no timer or
        // socket shutdown may rescue node-a's missed queue notification.
        resume_tx.send(()).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(completion_a, completion_b)
        })
        .await;
        pool.shutdown();
        rt_b.worker_pool().shutdown();
        let (a, b) = joined.expect("shared cluster join stalled on work queued before parking");
        a.unwrap();
        b.unwrap();
        let mut records: Vec<_> = out_a
            .collect_data()
            .into_iter()
            .chain(out_b.collect_data())
            .flat_map(|(_, data)| data)
            .collect();
        records.sort_unstable();
        assert_eq!(records, vec![1, 2, 3, 4, 5]);
        drop((managers_a, managers_b));
    }

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

    /// Bounds waits for expected progress; separate from the one-second
    /// early-return observation. Each test shares one deadline across all its
    /// waits, so the bound is per test, not per wait.
    const LIVENESS: Duration = Duration::from_secs(30);

    /// Time left before deadline (zero once it passed).
    fn left(deadline: Instant) -> Duration {
        deadline.saturating_duration_since(Instant::now())
    }

    fn one_thread() -> WorkerPoolConfig {
        WorkerPoolConfig {
            min_threads: 1,
            max_threads: 1,
            idle_shutdown: Duration::from_secs(30),
            spin_limit: 5,
            yield_limit: 10,
        }
    }

    /// Whether `condition` held within `limit`.
    fn within(limit: Duration, condition: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + limit;
        while !condition() {
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
        true
    }

    /// Poll every completion until it resolves or `limit` passes; `None` for
    /// one still unresolved.
    fn resolve(
        completions: Vec<crate::runtime::DataflowCompletion>,
        limit: Duration,
    ) -> Vec<Option<crate::error::Result<()>>> {
        use std::future::Future;
        let mut pending: Vec<_> = completions.into_iter().map(|c| Some(Box::pin(c))).collect();
        let mut outcomes: Vec<_> = pending.iter().map(|_| None).collect();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let deadline = Instant::now() + limit;
        while outcomes.iter().any(Option::is_none) && Instant::now() < deadline {
            for (slot, outcome) in pending.iter_mut().zip(outcomes.iter_mut()) {
                if let Some(completion) = slot {
                    if let std::task::Poll::Ready(result) = completion.as_mut().poll(&mut cx) {
                        *outcome = Some(result);
                        *slot = None;
                    }
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        outcomes
    }

    fn is_runtime_shutdown(outcome: &Option<crate::error::Result<()>>) -> bool {
        matches!(
            outcome,
            Some(Err(crate::error::Error::Cancelled {
                reason: Some(crate::cancellation::CancellationReason::RuntimeShutdown)
            }))
        )
    }

    /// Reports its first poll and stays Pending, never waking itself.
    struct PendingAfterFirstPoll(Option<std::sync::mpsc::Sender<()>>);

    impl std::future::Future for PendingAfterFirstPoll {
        type Output = crate::error::Result<bool>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if let Some(polled) = self.0.take() {
                let _ = polled.send(());
            }
            std::task::Poll::Pending
        }
    }

    /// The only worker survives a panicking activation: the activation
    /// queued behind it runs once, and none is counted as running after
    /// shutdown. Nothing is submitted after the panic that could start a
    /// replacement worker.
    #[test]
    fn a_panicking_activation_does_not_stop_its_worker() {
        let deadline = Instant::now() + LIVENESS;
        let pool = WorkerPool::new(one_thread()).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel::<()>(1);
        pool.submit(OperatorActivation::new(
            crate::worker::WorkerId::new(0),
            "panics",
            0,
            move || {
                let _ = entered_tx.send(());
                // Until the driver releases it (or drops the sender).
                let _ = release_rx.recv();
                panic!("a deliberately panicking activation");
            },
        ));
        let entered = entered_rx.recv_timeout(left(deadline));
        let (ran_tx, ran_rx) = std::sync::mpsc::channel();
        pool.submit(OperatorActivation::new(
            crate::worker::WorkerId::new(0),
            "after-the-panic",
            1,
            move || {
                let _ = ran_tx.send(());
            },
        ));
        let _ = release_tx.send(());
        let ran = ran_rx.recv_timeout(left(deadline));
        pool.shutdown();
        let ran_again = ran_rx.try_recv();
        assert!(entered.is_ok(), "the panicking activation started");
        assert!(ran.is_ok(), "the worker kept serving after the panic");
        assert!(ran_again.is_err(), "the queued activation ran once");
        assert_eq!(
            pool.active_threads(),
            0,
            "no activation counts as running after shutdown"
        );
    }

    /// A pool takes one registry: a second is refused, and the first stays
    /// installed and keeps driving its tasks.
    #[test]
    fn a_second_registry_is_refused() {
        let deadline = Instant::now() + LIVENESS;
        let pool = WorkerPool::new(one_thread()).unwrap();
        let first = pool.create_registry(None).unwrap();
        let second = pool.create_registry(None);
        let installed = pool.registry();
        let (completion, notifier) = crate::runtime::DataflowCompletion::new();
        first.register(
            Box::pin(async { Ok(true) }),
            notifier,
            crate::dataflow::DataflowId::new(),
            0,
        );
        let outcome = resolve(vec![completion], left(deadline)).pop().unwrap();
        pool.shutdown();
        assert!(
            matches!(
                second,
                Err(crate::Error::Runtime(RuntimeError::AlreadyConsumed { .. }))
            ),
            "a second registry is refused"
        );
        assert!(
            installed.is_some_and(|installed| Arc::ptr_eq(&installed, &first)),
            "the first registry stays installed"
        );
        assert!(
            matches!(outcome, Some(Ok(()))),
            "the first registry's task runs: {outcome:?}"
        );
    }

    /// Shutdown completes every executor left Pending, including those
    /// registered before the live-task list was first pruned (it prunes at
    /// 32 entries). The tasks are held until the outcomes are captured, so
    /// none completes by being dropped.
    #[test]
    fn shutdown_completes_every_pending_executor() {
        let deadline = Instant::now() + LIVENESS;
        const TASKS: usize = 40;
        let pool = WorkerPool::new(one_thread()).unwrap();
        let registry = pool.create_registry(None).unwrap();
        let (polled_tx, polled_rx) = std::sync::mpsc::channel();
        let mut tasks = Vec::new();
        let mut completions = Vec::new();
        for _ in 0..TASKS {
            let (completion, notifier) = crate::runtime::DataflowCompletion::new();
            tasks.push(registry.register(
                Box::pin(PendingAfterFirstPoll(Some(polled_tx.clone()))),
                notifier,
                crate::dataflow::DataflowId::new(),
                0,
            ));
            completions.push(completion);
        }
        let mut polled = 0;
        while polled < TASKS && polled_rx.recv_timeout(left(deadline)).is_ok() {
            polled += 1;
        }
        pool.shutdown();
        let outcomes = resolve(completions, left(deadline));
        drop(tasks);
        assert_eq!(polled, TASKS, "every executor was polled once");
        let unresolved = outcomes.iter().filter(|o| o.is_none()).count();
        let shut_down = outcomes.iter().filter(|o| is_runtime_shutdown(o)).count();
        assert_eq!(
            (shut_down, unresolved),
            (TASKS, 0),
            "every pending executor completes with RuntimeShutdown"
        );
    }

    /// Enters its first poll, reports it, and stays inside it until released;
    /// then Pending.
    struct GatedPoll {
        entered: Option<std::sync::mpsc::SyncSender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl std::future::Future for GatedPoll {
        type Output = crate::error::Result<bool>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
                // Until the driver releases it (or drops the sender).
                let _ = self.release.recv();
            }
            std::task::Poll::Pending
        }
    }

    /// Shutdown waits for a worker inside a poll, then completes that
    /// executor: an executor being polled cannot be aborted, so shutdown
    /// must join the worker first. The poll is released only after shutdown
    /// started and either returned or kept waiting for a second; a shutdown
    /// that does not wait is caught only if it returns within that second.
    #[test]
    fn shutdown_waits_for_a_worker_inside_a_poll() {
        let deadline = Instant::now() + LIVENESS;
        let pool = Arc::new(WorkerPool::new(one_thread()).unwrap());
        let registry = pool.create_registry(None).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (completion, notifier) = crate::runtime::DataflowCompletion::new();
        let task = registry.register(
            Box::pin(GatedPoll {
                entered: Some(entered_tx),
                release: release_rx,
            }),
            notifier,
            crate::dataflow::DataflowId::new(),
            0,
        );
        let entered = entered_rx.recv_timeout(left(deadline));
        let shutting_down = Arc::clone(&pool);
        let shutdown = thread::spawn(move || shutting_down.shutdown());
        let started = within(left(deadline), || pool.is_shutdown());
        let returned_early = within(Duration::from_secs(1), || shutdown.is_finished());
        let _ = release_tx.send(());
        let joined = shutdown.join();
        let outcome = resolve(vec![completion], left(deadline)).pop().unwrap();
        drop(task);
        assert!(entered.is_ok(), "a worker entered the poll");
        assert!(started, "shutdown started");
        assert!(joined.is_ok(), "shutdown did not panic");
        assert!(
            !returned_early,
            "shutdown returned while a worker was inside a poll"
        );
        assert!(
            is_runtime_shutdown(&outcome),
            "the executor completes with RuntimeShutdown: {outcome:?}"
        );
    }

    /// `new` refuses an invalid configuration, with the reason `validate`
    /// gives.
    #[test]
    fn new_refuses_an_invalid_config() {
        let cases = [
            (0, 1, 5, 10, "min_threads must be at least 1"),
            (2, 1, 5, 10, "max_threads must be >= min_threads"),
            (1, 4, 0, 10, "spin_limit must be at least 1"),
            (1, 4, 5, 5, "yield_limit must be > spin_limit"),
        ];
        for (min_threads, max_threads, spin_limit, yield_limit, reason) in cases {
            let result = WorkerPool::new(WorkerPoolConfig {
                min_threads,
                max_threads,
                idle_shutdown: Duration::from_secs(30),
                spin_limit,
                yield_limit,
            });
            let refused = match &result {
                Err(RuntimeError::InvalidConfig(message)) => message == reason,
                _ => false,
            };
            if let Ok(pool) = &result {
                pool.shutdown();
            }
            assert!(refused, "{reason}: got {:?}", result.as_ref().err());
        }
    }
}
