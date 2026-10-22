//! Probe handle for observing dataflow frontier progress.
//!
//! A [`ProbeHandle`] provides external visibility into the progress of a
//! dataflow at a specific point. It tracks the frontier of the **stream it is
//! attached to**: the set of timestamps that may still appear on that stream.
//! The frontier accounts for the producing operator's own capabilities (e.g.
//! a `unary_notify` operator holding a capability for a pending
//! notification) as well as everything upstream that can still reach the
//! stream.
//!
//! Probes are useful for:
//! - Waiting until a dataflow has processed all data up to a given timestamp
//! - Monitoring progress for external scheduling decisions
//! - Testing that frontiers advance correctly
//!
//! ## Guarantees
//!
//! - **Conservative**: the probe never reports a time as complete while the
//!   progress tracker still considers it possible on the stream. A probe
//!   inside an `iterate` body, where data circulating on the feedback edge is
//!   invisible to the tracker, stays at the minimum until the workers writing
//!   it have completed instead (in a cluster it never completes; see
//!   `Pipe::probe`). (A probe *downstream* of a loop inherits the tracker's
//!   blind spot and can report an epoch complete while the loop still
//!   iterates it.)
//! - **Monotone**: the reported frontier only moves forward. When several
//!   workers of a multi-worker dataflow observe the same probe, the probe
//!   reports the *meet* of their views (a time is complete only once every
//!   still-running worker agrees), and a view that would move the probe
//!   backwards is never published.
//!
//! ## Async waiting
//!
//! [`ProbeHandle`] provides async
//! methods for awaiting frontier changes:
//!
//! - [`wait_until_done_with`](ProbeHandle::wait_until_done_with) — awaits
//!   until the frontier advances past a given timestamp
//! - [`wait_until_done`](ProbeHandle::wait_until_done) — awaits until the
//!   frontier is empty (all work complete)
//! - [`subscribe`](ProbeHandle::subscribe) — returns a
//!   [`tokio::sync::watch::Receiver`] for custom watching

use std::sync::{Arc, Mutex};

use crate::order::{PartialOrder, Product};
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// A handle for observing the progress frontier of a stream in the dataflow.
///
/// The frontier represents the set of timestamps that may still appear on the
/// probed stream. When the frontier advances past a timestamp `t`, it
/// guarantees no more data at time `t` (or any time ≤ t) will appear there.
///
/// Thread-safe: can be shared across threads via `Clone`.
///
/// Provides async methods for
/// awaiting frontier changes without polling.
#[derive(Clone, Debug)]
pub struct ProbeHandle<T: Timestamp> {
    /// Per-writer views the published frontier is aggregated from.
    slots: Arc<Mutex<WriterSlots<T>>>,
    /// Copy of the published frontier for synchronous readers. Only written
    /// while holding the watch channel's write lock, so it always equals the
    /// watch value; readers never take the watch lock, so a caller holding a
    /// `subscribe()` borrow guard cannot deadlock with a writer.
    published: Arc<Mutex<Antichain<T>>>,
    /// Async watch channel receiver for frontier change notifications.
    /// The sender is held by the writers (via `ProbeNotifier`).
    watch_rx: tokio::sync::watch::Receiver<Antichain<T>>,
}

/// Sender-side handle for notifying probe watchers of frontier changes.
///
/// Held by every probe writer. When all clones are dropped (e.g., every
/// executor observing the probe has shut down), async waiters receive a
/// channel-closed error and can detect termination.
#[derive(Clone)]
pub(crate) struct ProbeNotifier<T: Timestamp> {
    watch_tx: tokio::sync::watch::Sender<Antichain<T>>,
    published: Arc<Mutex<Antichain<T>>>,
}

impl<T: Timestamp> ProbeNotifier<T> {
    /// Overwrite the published frontier (single-writer test helper).
    #[cfg(test)]
    pub(crate) fn notify(&self, frontier: &Antichain<T>) {
        self.watch_tx.send_modify(|published| {
            *lock(&self.published) = frontier.clone();
            *published = frontier.clone();
        });
    }

    /// Publishes `frontier` if it is a forward step from the published one:
    /// every element of `frontier` is at or beyond some published element. A
    /// writer whose view regresses (or a writer that registers late, still at
    /// the minimum) can only hold the probe where it is, never move it
    /// backwards. The check and the update are atomic under the channel's
    /// lock, so concurrent writers cannot interleave into a regression.
    fn publish_if_advanced(&self, frontier: Antichain<T>) {
        self.watch_tx.send_if_modified(|published| {
            if *published != frontier && PartialOrder::less_equal(&*published, &frontier) {
                *lock(&self.published) = frontier.clone();
                *published = frontier;
                true
            } else {
                false
            }
        });
    }
}

fn lock<V>(mutex: &Mutex<V>) -> std::sync::MutexGuard<'_, V> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// One writer's (executor's) view of the probed stream.
#[derive(Debug)]
struct WriterSlot<T: Timestamp> {
    frontier: Antichain<T>,
    finished: bool,
}

/// The writer views behind a probe.
#[derive(Debug)]
struct WriterSlots<T: Timestamp> {
    /// Per-writer views. One slot per executor observing this probe.
    slots: Vec<WriterSlot<T>>,
    /// Number of writers a probe whose writers all exist would have: the
    /// number of workers that run the probed operator
    /// (`PhysicalPlan::probe_writers`; the largest value reported at attach
    /// time). Every writer's view is global. When the writers cover all of
    /// them, a finished writer is dropped from the meet: the remaining
    /// writers' views fully describe the stream, and once all writers have
    /// finished no operator instance they cover can produce on it. Otherwise
    /// (one writer per worker of a cluster node or of
    /// `SimpleRuntime::spawn_multi`, which build the dataflow per worker) a
    /// finished writer's final view keeps constraining the probe, so a probe
    /// never claims completion of work that a remote worker may still be
    /// doing.
    expected_writers: usize,
}

impl<T: Timestamp> WriterSlots<T> {
    /// The meet of the contributing writer views.
    fn meet(&self) -> Antichain<T> {
        let covers_all_workers = self.slots.len() >= self.expected_writers;
        let mut meet = Antichain::new();
        for slot in &self.slots {
            if slot.finished && covers_all_workers {
                continue;
            }
            for time in slot.frontier.elements() {
                meet.insert_ref(time);
            }
        }
        meet
    }
}

impl<T: Timestamp> ProbeHandle<T> {
    /// Create a new probe handle with an initial frontier at `T::minimum()`.
    ///
    /// Returns `(handle, notifier)` — the notifier is shared by the writers
    /// that update the probe. When every notifier clone is dropped, async
    /// methods return promptly.
    pub(crate) fn new() -> (Self, ProbeNotifier<T>) {
        let initial = Antichain::from_elem(T::minimum());
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(initial.clone());
        let published = Arc::new(Mutex::new(initial));
        let handle = Self {
            slots: Arc::new(Mutex::new(WriterSlots {
                slots: Vec::new(),
                expected_writers: 0,
            })),
            published: Arc::clone(&published),
            watch_rx,
        };
        let notifier = ProbeNotifier {
            watch_tx,
            published,
        };
        (handle, notifier)
    }

    /// Returns `true` if the frontier has advanced past `time`.
    ///
    /// When this returns `true`, no more data at timestamps ≤ `time` will
    /// appear on the probed stream.
    pub fn done_with(&self, time: &T) -> bool {
        !lock(&self.published).less_equal(time)
    }

    /// Returns the current frontier as a snapshot.
    pub fn frontier(&self) -> Antichain<T> {
        lock(&self.published).clone()
    }

    /// Returns `true` if the frontier is empty (all work complete).
    pub fn is_done(&self) -> bool {
        lock(&self.published).is_empty()
    }

    /// Registers a new writer slot, initialized to `T::minimum()`.
    fn attach_writer(&self, notifier: ProbeNotifier<T>, total_workers: usize) -> SlotWriter<T> {
        let mut slots = lock(&self.slots);
        slots.slots.push(WriterSlot {
            frontier: Antichain::from_elem(T::minimum()),
            finished: false,
        });
        slots.expected_writers = slots.expected_writers.max(total_workers);
        SlotWriter {
            slots: Arc::clone(&self.slots),
            notifier,
            slot: slots.slots.len() - 1,
            last: None,
        }
    }
}

// Async waiting methods.
impl<T: Timestamp> ProbeHandle<T> {
    /// Wait asynchronously until the frontier advances past `time`.
    ///
    /// Returns `Ok(())` when `done_with(time)` becomes true, or `Err` if every
    /// writer drops its notifier (dataflow terminated) before the condition
    /// is met.
    ///
    /// This is more efficient than polling `done_with()` in a loop — it uses
    /// a `tokio::sync::watch` channel to wake up only when the frontier changes.
    pub async fn wait_until_done_with(
        &self,
        time: &T,
    ) -> std::result::Result<(), crate::error::Error> {
        let mut rx = self.watch_rx.clone();

        // Check current value first (avoid missed-wakeup race).
        if !rx.borrow().less_equal(time) {
            return Ok(());
        }

        loop {
            match rx.changed().await {
                Ok(()) => {
                    if !rx.borrow_and_update().less_equal(time) {
                        return Ok(());
                    }
                }
                Err(_) => {
                    // Sender dropped — check one last time.
                    if !rx.borrow().less_equal(time) {
                        return Ok(());
                    }
                    return Err(crate::error::Error::ChannelClosed);
                }
            }
        }
    }

    /// Wait asynchronously until the frontier is empty (all work complete).
    ///
    /// Returns `Ok(())` when `is_done()` becomes true, or `Err` if every
    /// writer drops its notifier before completion.
    pub async fn wait_until_done(&self) -> std::result::Result<(), crate::error::Error> {
        let mut rx = self.watch_rx.clone();

        // Check current value first.
        if rx.borrow().is_empty() {
            return Ok(());
        }

        loop {
            match rx.changed().await {
                Ok(()) => {
                    if rx.borrow_and_update().is_empty() {
                        return Ok(());
                    }
                }
                Err(_) => {
                    if rx.borrow().is_empty() {
                        return Ok(());
                    }
                    return Err(crate::error::Error::ChannelClosed);
                }
            }
        }
    }

    /// Get a watch receiver for custom frontier observation.
    ///
    /// The receiver yields the latest [`Antichain<T>`] whenever the frontier
    /// changes. Note that intermediate frontier values may be skipped if
    /// updates happen faster than the receiver processes them (last-value
    /// semantics). Observed values are monotone.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Antichain<T>> {
        self.watch_rx.clone()
    }
}

impl<T: Timestamp> Default for ProbeHandle<T> {
    fn default() -> Self {
        Self::new().0
    }
}

// ---------------------------------------------------------------------------
// Writers and registrations (crate-internal)
// ---------------------------------------------------------------------------

/// The executor-side writer of one probe, in the executor tracker's timestamp
/// `T`. Each executor (worker) observing a probe owns one writer.
pub(crate) trait ProbeWriter<T: Timestamp>: Send {
    /// Report this writer's current view of the probed stream's frontier.
    fn update(&mut self, frontier: &Antichain<T>);

    /// Report that this writer's executor completed. `final_frontier` is its
    /// last view of the stream, or `None` if it has no trustworthy view.
    fn finish(&mut self, final_frontier: Option<&Antichain<T>>);
}

/// Writer bound to one slot of a [`ProbeHandle`].
struct SlotWriter<T: Timestamp> {
    slots: Arc<Mutex<WriterSlots<T>>>,
    notifier: ProbeNotifier<T>,
    slot: usize,
    /// Last frontier written, to skip redundant locking.
    last: Option<Antichain<T>>,
}

impl<T: Timestamp> SlotWriter<T> {
    fn write(&mut self, frontier: Option<&Antichain<T>>, finished: bool) {
        let meet = {
            let mut slots = lock(&self.slots);
            let slot = &mut slots.slots[self.slot];
            if let Some(frontier) = frontier {
                slot.frontier = frontier.clone();
            }
            slot.finished |= finished;
            slots.meet()
        };
        // Published after releasing the slots lock: readers only touch the
        // watch channel, and the dominance check inside it keeps concurrent
        // writers from publishing a stale meet over a newer one.
        self.notifier.publish_if_advanced(meet);
    }
}

impl<T: Timestamp> ProbeWriter<T> for SlotWriter<T> {
    fn update(&mut self, frontier: &Antichain<T>) {
        if self.last.as_ref() == Some(frontier) {
            return;
        }
        self.last = Some(frontier.clone());
        self.write(Some(frontier), false);
    }

    fn finish(&mut self, final_frontier: Option<&Antichain<T>>) {
        self.write(final_frontier, true);
    }
}

/// Writer for a probe inside an `iterate` body: the executor tracker runs at
/// the outer timestamp `T`, while the probe handle observes
/// `Product<T, TInner>`.
///
/// Data circulating on a loop's feedback edge is not visible to progress
/// tracking (enter, feedback and leave hold no capabilities), so the outer
/// frontier the tracker reports for a loop-body stream passes epoch `e` as soon
/// as the loop's input does, while `(e, i)` data may still circulate. No
/// frontier the tracker computes is therefore safe to publish, and the writer
/// holds its slot at the minimum until its executor completes (when every
/// operator of the loop on that worker is done). The probe then becomes done
/// once its writers are finished (see [`WriterSlots`]).
struct LiftedWriter<T: Timestamp, TInner: Timestamp>
where
    Product<T, TInner>: Timestamp,
{
    inner: Box<dyn ProbeWriter<Product<T, TInner>>>,
    _outer: std::marker::PhantomData<fn(T)>,
}

impl<T: Timestamp, TInner: Timestamp> ProbeWriter<T> for LiftedWriter<T, TInner>
where
    Product<T, TInner>: Timestamp,
{
    fn update(&mut self, _frontier: &Antichain<T>) {}

    fn finish(&mut self, _final_frontier: Option<&Antichain<T>>) {
        self.inner.finish(None);
    }
}

type AttachFn<T> = Arc<dyn Fn(usize) -> Box<dyn ProbeWriter<T>> + Send + Sync>;

/// Builder-side record of a probe: which stream it observes and how an
/// executor attaches a writer to it.
pub(crate) struct ProbeRegistration<T: Timestamp> {
    /// Operator producing the probed stream.
    pub(crate) operator: usize,
    /// Output port of `operator` carrying the probed stream.
    pub(crate) output: usize,
    attach: AttachFn<T>,
}

impl<T: Timestamp> Clone for ProbeRegistration<T> {
    fn clone(&self) -> Self {
        Self {
            operator: self.operator,
            output: self.output,
            attach: Arc::clone(&self.attach),
        }
    }
}

impl<T: Timestamp> std::fmt::Debug for ProbeRegistration<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeRegistration")
            .field("operator", &self.operator)
            .field("output", &self.output)
            .finish()
    }
}

impl<T: Timestamp> ProbeRegistration<T> {
    /// Creates a new probe observing output `output` of `operator`.
    pub(crate) fn new(operator: usize, output: usize) -> (Self, ProbeHandle<T>) {
        let (handle, notifier) = ProbeHandle::new();
        let attach_handle = handle.clone();
        let registration = Self {
            operator,
            output,
            attach: Arc::new(move |total_workers| {
                Box::new(attach_handle.attach_writer(notifier.clone(), total_workers))
                    as Box<dyn ProbeWriter<T>>
            }),
        };
        (registration, handle)
    }

    /// Attaches a new writer (one per executor observing the probe).
    /// `total_workers` is the number of workers in the writer's dataflow.
    pub(crate) fn attach(&self, total_workers: usize) -> Box<dyn ProbeWriter<T>> {
        (self.attach)(total_workers)
    }
}

impl<T: Timestamp, TInner: Timestamp> ProbeRegistration<Product<T, TInner>>
where
    Product<T, TInner>: Timestamp,
{
    /// Re-expresses a probe registered inside an `iterate` body in the outer
    /// scope's timestamp, so the outer executor can drive it.
    pub(crate) fn lift_to_outer(self) -> ProbeRegistration<T> {
        let inner = self.attach;
        ProbeRegistration {
            operator: self.operator,
            output: self.output,
            attach: Arc::new(move |total_workers| {
                Box::new(LiftedWriter::<T, TInner> {
                    inner: inner(total_workers),
                    _outer: std::marker::PhantomData,
                }) as Box<dyn ProbeWriter<T>>
            }),
        }
    }
}

/// A probe writer attached to an executor, with the stream it observes.
pub(crate) struct AttachedProbe<T: Timestamp> {
    pub(crate) operator: usize,
    pub(crate) output: usize,
    pub(crate) writer: Box<dyn ProbeWriter<T>>,
}

impl<T: Timestamp> AttachedProbe<T> {
    pub(crate) fn new(registration: &ProbeRegistration<T>, total_workers: usize) -> Self {
        Self {
            operator: registration.operator,
            output: registration.output,
            writer: registration.attach(total_workers),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_probe<T: Timestamp>() -> (ProbeHandle<T>, ProbeNotifier<T>) {
        ProbeHandle::new()
    }

    #[test]
    fn initial_frontier_at_minimum() {
        let (probe, _notifier) = make_probe::<u64>();
        assert!(!probe.is_done());
        assert!(!probe.done_with(&0));
        assert!(probe.frontier().less_equal(&0));
    }

    #[test]
    fn done_with_after_advance() {
        let (probe, notifier) = make_probe::<u64>();
        // Advance frontier past 0 to 5
        notifier.notify(&Antichain::from_elem(5));
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&4));
        assert!(!probe.done_with(&5));
        assert!(!probe.done_with(&10));
    }

    #[test]
    fn empty_frontier_means_done() {
        let (probe, notifier) = make_probe::<u64>();
        notifier.notify(&Antichain::new());
        assert!(probe.is_done());
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&u64::MAX));
    }

    #[test]
    fn clone_shares_state() {
        let (probe, notifier) = make_probe::<u64>();
        let probe2 = probe.clone();
        notifier.notify(&Antichain::from_elem(10));
        assert!(probe2.done_with(&5));
    }

    fn elem(t: u64) -> Antichain<u64> {
        Antichain::from_elem(t)
    }

    /// A caller holding a `subscribe()` borrow guard while querying the probe
    /// must not deadlock with a writer publishing concurrently.
    #[test]
    fn reading_while_holding_a_watch_borrow_does_not_deadlock_writers() {
        let (reg, probe) = ProbeRegistration::<u64>::new(1, 0);
        let mut writer = reg.attach(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader = probe.clone();
        std::thread::spawn(move || {
            let rx = reader.subscribe();
            for _ in 0..2_000 {
                let guard = rx.borrow();
                let _ = reader.done_with(&3);
                let _ = reader.frontier();
                drop(guard);
            }
            let _ = done_tx.send(());
        });
        for t in 1..2_000u64 {
            writer.update(&elem(t));
        }
        done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("reader deadlocked with the writer");
        assert_eq!(probe.frontier(), elem(1_999));
    }

    #[test]
    fn single_writer_publishes_its_view() {
        let (reg, probe) = ProbeRegistration::<u64>::new(1, 0);
        let mut w = reg.attach(1);
        w.update(&elem(3));
        assert_eq!(probe.frontier(), elem(3));
        w.update(&elem(7));
        assert_eq!(probe.frontier(), elem(7));
        w.finish(Some(&elem(7)));
        assert!(
            probe.is_done(),
            "a lone covering writer finishing completes the probe"
        );
    }

    #[test]
    fn multi_writer_reports_meet_and_never_regresses() {
        let (reg, probe) = ProbeRegistration::<u64>::new(1, 0);
        let mut a = reg.attach(2);
        let mut b = reg.attach(2);
        a.update(&elem(9));
        // b still at minimum: the meet is 0.
        assert_eq!(probe.frontier(), elem(0));
        b.update(&elem(4));
        assert_eq!(probe.frontier(), elem(4));
        // A lagging (or buggy) view must not move the probe backwards.
        b.update(&elem(2));
        assert_eq!(probe.frontier(), elem(4));
        b.update(&elem(12));
        assert_eq!(probe.frontier(), elem(9));
        // a finishes: excluded from the meet because writers cover all workers.
        a.finish(Some(&elem(9)));
        assert_eq!(probe.frontier(), elem(12));
        b.finish(None);
        assert!(probe.is_done());
    }

    #[test]
    fn partial_cover_keeps_finished_views() {
        // One writer for a two-worker dataflow (e.g. one node of a cluster):
        // finishing must not claim the remote worker's share is done.
        let (reg, probe) = ProbeRegistration::<u64>::new(1, 0);
        let mut a = reg.attach(2);
        a.update(&elem(5));
        a.finish(Some(&elem(6)));
        assert_eq!(probe.frontier(), elem(6));
        assert!(!probe.is_done());
    }

    #[test]
    fn partial_order_meet_and_monotone_guard() {
        type P = Product<u64, u64>;
        let (reg, probe) = ProbeRegistration::<P>::new(1, 0);
        let mut a = reg.attach(2);
        let mut b = reg.attach(2);
        a.update(&Antichain::from_elem(P::new(2, 0)));
        b.update(&Antichain::from_elem(P::new(0, 2)));
        assert_eq!(
            probe.frontier(),
            Antichain::from_elem_iter([P::new(2, 0), P::new(0, 2)])
        );
        // b moves to (1,1), which is not >= its previous view (0,2): a
        // regressed view. Publishing the meet {(2,0),(1,1)} would re-open
        // (1,1), so the probe must stay put.
        b.update(&Antichain::from_elem(P::new(1, 1)));
        assert_eq!(
            probe.frontier(),
            Antichain::from_elem_iter([P::new(2, 0), P::new(0, 2)])
        );
        b.update(&Antichain::from_elem(P::new(3, 3)));
        assert_eq!(probe.frontier(), Antichain::from_elem(P::new(2, 0)));
    }

    #[test]
    fn lifted_writer_holds_at_minimum_until_finished() {
        type P = Product<u64, u32>;
        let (reg, probe) = ProbeRegistration::<P>::new(1, 0);
        let outer = reg.lift_to_outer();
        let mut a = outer.attach(2);
        let mut b = outer.attach(2);
        // Outer frontiers never move a loop-body probe: the loop may still be
        // iterating an epoch the outer frontier has passed.
        a.update(&elem(3));
        b.update(&Antichain::new());
        assert_eq!(probe.frontier(), Antichain::from_elem(P::new(0, 0)));
        // Even a finishing writer's final view is not trusted.
        a.finish(Some(&Antichain::new()));
        assert_eq!(probe.frontier(), Antichain::from_elem(P::new(0, 0)));
        // Once every worker's writer finished, the probe is done.
        b.finish(Some(&elem(9)));
        assert!(probe.is_done());
    }

    #[test]
    fn lifted_writer_without_full_worker_cover_never_completes() {
        // One writer for a two-worker dataflow (one node of a cluster): the
        // remote worker's loop may still iterate.
        type P = Product<u64, u32>;
        let (reg, probe) = ProbeRegistration::<P>::new(1, 0);
        let mut w = reg.lift_to_outer().attach(2);
        w.update(&elem(3));
        w.finish(Some(&Antichain::new()));
        assert_eq!(probe.frontier(), Antichain::from_elem(P::new(0, 0)));
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn wait_until_done_with_already_satisfied() {
        let (probe, _notifier) = ProbeHandle::<u64>::new();
        _notifier.notify(&Antichain::from_elem(10));
        // Time 5 is already past the frontier.
        probe.wait_until_done_with(&5).await.unwrap();
    }

    #[tokio::test]
    async fn wait_until_done_with_receives_update() {
        let (probe, notifier) = ProbeHandle::<u64>::new();

        let probe2 = probe.clone();
        let handle = tokio::spawn(async move { probe2.wait_until_done_with(&5).await });

        // Let the spawned task start waiting.
        tokio::task::yield_now().await;

        // Advance frontier past time 5.
        notifier.notify(&Antichain::from_elem(10));

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn wait_until_done_completes() {
        let (probe, notifier) = ProbeHandle::<u64>::new();

        let probe2 = probe.clone();
        let handle = tokio::spawn(async move { probe2.wait_until_done().await });

        tokio::task::yield_now().await;

        // Signal completion with empty frontier.
        notifier.notify(&Antichain::new());

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn wait_returns_error_when_notifier_dropped() {
        let (probe, notifier) = ProbeHandle::<u64>::new();

        let probe2 = probe.clone();
        let handle = tokio::spawn(async move { probe2.wait_until_done().await });

        tokio::task::yield_now().await;

        // Drop notifier without completing.
        drop(notifier);

        let result = handle.await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_receives_updates() {
        let (probe, notifier) = ProbeHandle::<u64>::new();
        let mut rx = probe.subscribe();

        notifier.notify(&Antichain::from_elem(5));

        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_and_update(), Antichain::from_elem(5));
    }

    #[tokio::test]
    async fn multiple_waiters_all_notified() {
        let (probe, notifier) = ProbeHandle::<u64>::new();

        let mut handles = Vec::new();
        for _ in 0..5 {
            let p = probe.clone();
            handles.push(tokio::spawn(
                async move { p.wait_until_done_with(&3).await },
            ));
        }

        tokio::task::yield_now().await;

        notifier.notify(&Antichain::from_elem(10));

        for h in handles {
            h.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn writers_wake_async_waiters_and_closing_all_writers_errors() {
        let (reg, probe) = ProbeRegistration::<u64>::new(1, 0);
        let mut a = reg.attach(2);
        let mut b = reg.attach(2);
        drop(reg);
        let p = probe.clone();
        let waiter = tokio::spawn(async move { p.wait_until_done_with(&4).await });
        tokio::task::yield_now().await;
        a.update(&Antichain::from_elem(5));
        b.update(&Antichain::from_elem(6));
        waiter.await.unwrap().unwrap();

        let p = probe.clone();
        let waiter = tokio::spawn(async move { p.wait_until_done().await });
        tokio::task::yield_now().await;
        drop(a);
        drop(b);
        assert!(waiter.await.unwrap().is_err());
    }
}
