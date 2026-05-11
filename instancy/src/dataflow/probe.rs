//! Probe handle for observing dataflow frontier progress.
//!
//! A [`ProbeHandle`] provides external visibility into the progress of a
//! dataflow at a specific point. It tracks the frontier (the set of timestamps
//! that may still appear) at the probe's location in the graph.
//!
//! Probes are inserted into the dataflow graph as pass-through operators that
//! observe but do not modify data flow. They are useful for:
//! - Waiting until a dataflow has processed all data up to a given timestamp
//! - Monitoring progress for external scheduling decisions
//! - Testing that frontiers advance correctly
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

use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// A handle for observing the progress frontier at a specific point in the dataflow.
///
/// The frontier represents the set of timestamps that may still appear at this
/// point. When the frontier advances past a timestamp `t`, it guarantees no more
/// data at time `t` (or any time ≤ t) will arrive.
///
/// Thread-safe: can be shared across threads via `Clone`.
///
/// Provides async methods for
/// awaiting frontier changes without polling.
#[derive(Clone, Debug)]
pub struct ProbeHandle<T: Timestamp> {
    /// Shared frontier state, updated by the executor during progress propagation.
    frontier: Arc<Mutex<Antichain<T>>>,
    /// Async watch channel receiver for frontier change notifications.
    /// The sender is held separately by the executor (via `ProbeNotifier`).
    watch_rx: tokio::sync::watch::Receiver<Antichain<T>>,
}

/// Sender-side handle for notifying probe watchers of frontier changes.
///
/// Held by the executor alongside the `ProbeHandle`. When the executor
/// updates the frontier, it calls [`notify`](ProbeNotifier::notify) to
/// wake any async waiters.
///
/// When this is dropped (e.g., the executor shuts down), all async waiters
/// will receive a channel-closed error and can detect termination.
#[derive(Clone)]
pub(crate) struct ProbeNotifier<T: Timestamp> {
    watch_tx: tokio::sync::watch::Sender<Antichain<T>>,
}

impl<T: Timestamp> ProbeNotifier<T> {
    /// Notify all watchers of a new frontier value.
    pub(crate) fn notify(&self, frontier: &Antichain<T>) {
        // Ignore send errors — no receivers is fine.
        let _ = self.watch_tx.send(frontier.clone());
    }
}

impl<T: Timestamp> ProbeHandle<T> {
    /// Create a new probe handle with an initial frontier at `T::minimum()`.
    ///
    /// Returns `(handle, notifier)` — the notifier is held by the executor
    /// to send frontier updates that wake async waiters. When the notifier
    /// is dropped, async methods return promptly.
    pub(crate) fn new() -> (Self, ProbeNotifier<T>) {
        let initial = Antichain::from_elem(T::minimum());
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(initial.clone());
        let handle = Self {
            frontier: Arc::new(Mutex::new(initial)),
            watch_rx,
        };
        let notifier = ProbeNotifier { watch_tx };
        (handle, notifier)
    }

    /// Returns `true` if the frontier has advanced past `time`.
    ///
    /// When this returns `true`, no more data at timestamps ≤ `time` will arrive
    /// at this point in the dataflow.
    pub fn done_with(&self, time: &T) -> bool {
        let frontier = self.frontier.lock().unwrap_or_else(|e| e.into_inner());
        !frontier.less_equal(time)
    }

    /// Returns the current frontier as a snapshot.
    pub fn frontier(&self) -> Antichain<T> {
        self.frontier
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Returns `true` if the frontier is empty (all work complete).
    pub fn is_done(&self) -> bool {
        self.frontier
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Update the frontier. Called by the executor during progress propagation.
    pub(crate) fn update_frontier(&self, new_frontier: &Antichain<T>) {
        let mut frontier = self.frontier.lock().unwrap_or_else(|e| e.into_inner());
        *frontier = new_frontier.clone();
    }
}

// Async waiting methods.
impl<T: Timestamp> ProbeHandle<T> {
    /// Wait asynchronously until the frontier advances past `time`.
    ///
    /// Returns `Ok(())` when `done_with(time)` becomes true, or `Err` if the
    /// executor drops the notifier (dataflow terminated) before the condition
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
    /// Returns `Ok(())` when `is_done()` becomes true, or `Err` if the executor
    /// drops the notifier before completion.
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
    /// semantics).
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Antichain<T>> {
        self.watch_rx.clone()
    }
}

impl<T: Timestamp> Default for ProbeHandle<T> {
    fn default() -> Self {
        Self::new().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_probe<T: Timestamp>() -> ProbeHandle<T> {
        let (handle, _notifier) = ProbeHandle::new();
        handle
    }

    #[test]
    fn initial_frontier_at_minimum() {
        let probe = make_probe::<u64>();
        assert!(!probe.is_done());
        assert!(!probe.done_with(&0));
        assert!(probe.frontier().less_equal(&0));
    }

    #[test]
    fn done_with_after_advance() {
        let probe = make_probe::<u64>();
        // Advance frontier past 0 to 5
        probe.update_frontier(&Antichain::from_elem(5));
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&4));
        assert!(!probe.done_with(&5));
        assert!(!probe.done_with(&10));
    }

    #[test]
    fn empty_frontier_means_done() {
        let probe = make_probe::<u64>();
        probe.update_frontier(&Antichain::new());
        assert!(probe.is_done());
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&u64::MAX));
    }

    #[test]
    fn clone_shares_state() {
        let probe = make_probe::<u64>();
        let probe2 = probe.clone();
        probe.update_frontier(&Antichain::from_elem(10));
        assert!(probe2.done_with(&5));
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn wait_until_done_with_already_satisfied() {
        let (probe, _notifier) = ProbeHandle::<u64>::new();
        probe.update_frontier(&Antichain::from_elem(10));
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
        probe.update_frontier(&Antichain::from_elem(10));
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
        probe.update_frontier(&Antichain::new());
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

        probe.update_frontier(&Antichain::from_elem(5));
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

        probe.update_frontier(&Antichain::from_elem(10));
        notifier.notify(&Antichain::from_elem(10));

        for h in handles {
            h.await.unwrap().unwrap();
        }
    }
}
