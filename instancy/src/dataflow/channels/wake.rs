//! Per-dataflow notification primitive for waking an idle executor.
//!
//! A [`WakeHandle`] is shared by all channels within a single dataflow.
//! When any channel transition makes the dataflow potentially runnable
//! (new data pushed, capacity freed, channel closed), the channel calls
//! [`WakeHandle::notify()`]. The executor registers its [`Waker`] via
//! [`WakeHandle::register_waker()`] before yielding, and is woken when
//! the next notification arrives.
//!
//! # Wake sources
//!
//! All state transitions that can unblock an operator must notify:
//! - `push()` — new data available for downstream operators
//! - `close()` / sender `Drop` — downstream may now see `is_exhausted()`
//! - `pull()` that frees capacity — upstream blocked on backpressure becomes runnable
//! - Cancellation — executor must re-check `cancel.check()`
//!
//! # Race safety
//!
//! The executor must follow this protocol to avoid lost wakeups:
//! 1. Register waker via `register_waker()`
//! 2. Re-check `take_notification()` — if true, continue processing
//! 3. Only if still not notified, return `Poll::Pending`

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

/// Shared state backing a [`WakeHandle`].
struct WakeState {
    /// Flag set by channels when the dataflow may have become runnable.
    notified: AtomicBool,
    /// Waker registered by the executor before yielding.
    waker: Mutex<Option<Waker>>,
}

/// A notification handle shared across all channels in a dataflow.
///
/// Cloning a `WakeHandle` creates a new handle to the same shared state.
/// All clones notify the same executor.
#[derive(Clone)]
pub struct WakeHandle {
    inner: Arc<WakeState>,
}

impl WakeHandle {
    /// Create a new `WakeHandle` in the notified state.
    ///
    /// Starting notified ensures the executor runs at least one sweep
    /// before considering yielding.
    pub fn new() -> Self {
        WakeHandle {
            inner: Arc::new(WakeState {
                notified: AtomicBool::new(true),
                waker: Mutex::new(None),
            }),
        }
    }

    /// Signal that the dataflow may have become runnable.
    ///
    /// Sets the notification flag and wakes the executor's registered waker
    /// (if any). This is safe to call from any thread at any time.
    pub fn notify(&self) {
        // Set the flag first so the executor sees it even if the waker
        // fires before we return.
        self.inner.notified.store(true, Ordering::Release);

        // Wake the executor if it has a registered waker.
        // Clone the waker before dropping the lock to avoid blocking
        // register_waker() during the (potentially expensive) wake call.
        let waker = self.inner.waker.lock().unwrap()
            .as_ref()
            .cloned();
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Register the executor's waker for the next notification.
    ///
    /// The waker is called by [`notify()`](Self::notify) when a channel
    /// transition makes the dataflow potentially runnable. Only the most
    /// recently registered waker is kept.
    pub fn register_waker(&self, waker: &Waker) {
        let mut guard = self.inner.waker.lock().unwrap();
        // Only clone if the waker has changed (avoids unnecessary allocation).
        match guard.as_ref() {
            Some(existing) if existing.will_wake(waker) => {}
            _ => *guard = Some(waker.clone()),
        }
    }

    /// Check and clear the notification flag.
    ///
    /// Returns `true` if a notification was pending (and clears it).
    /// The executor calls this after registering its waker to detect
    /// notifications that arrived between the last sweep and waker
    /// registration (race-safe protocol).
    pub fn take_notification(&self) -> bool {
        self.inner.notified.swap(false, Ordering::AcqRel)
    }
}

impl Default for WakeHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WakeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakeHandle")
            .field("notified", &self.inner.notified.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_notified() {
        let wh = WakeHandle::new();
        assert!(wh.take_notification());
        // After take, should be false
        assert!(!wh.take_notification());
    }

    #[test]
    fn notify_sets_flag() {
        let wh = WakeHandle::new();
        wh.take_notification(); // clear initial
        assert!(!wh.take_notification());
        wh.notify();
        assert!(wh.take_notification());
    }

    #[test]
    fn clone_shares_state() {
        let wh1 = WakeHandle::new();
        let wh2 = wh1.clone();
        wh1.take_notification(); // clear initial
        wh2.notify();
        assert!(wh1.take_notification());
    }

    #[test]
    fn notify_wakes_registered_waker() {
        use std::sync::atomic::AtomicUsize;
        use std::task::{RawWaker, RawWakerVTable};

        static WAKE_COUNT: AtomicUsize = AtomicUsize::new(0);

        fn clone_fn(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        fn wake_fn(_: *const ()) {
            WAKE_COUNT.fetch_add(1, Ordering::SeqCst);
        }
        fn drop_fn(_: *const ()) {}
        static VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone_fn, wake_fn, wake_fn, drop_fn);

        WAKE_COUNT.store(0, Ordering::SeqCst);

        let wh = WakeHandle::new();
        wh.take_notification(); // clear initial

        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        wh.register_waker(&waker);

        wh.notify();
        assert_eq!(WAKE_COUNT.load(Ordering::SeqCst), 1);

        // Second notify also wakes
        wh.notify();
        assert_eq!(WAKE_COUNT.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn race_safe_protocol() {
        // Simulate: notification arrives between sweep and waker registration
        let wh = WakeHandle::new();
        wh.take_notification(); // clear initial

        // Notification arrives (simulating a push)
        wh.notify();

        // Executor registers waker (late)
        let raw = std::task::RawWaker::new(
            std::ptr::null(),
            &std::task::RawWakerVTable::new(
                |p| std::task::RawWaker::new(p, &std::task::RawWakerVTable::new(|p| std::task::RawWaker::new(p, &NOOP_VTABLE), |_|{}, |_|{}, |_|{})),
                |_| {},
                |_| {},
                |_| {},
            ),
        );
        let waker = unsafe { Waker::from_raw(raw) };
        wh.register_waker(&waker);

        // Re-check catches the notification
        assert!(wh.take_notification());
    }

    // Minimal vtable for tests
    static NOOP_VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
        |p| std::task::RawWaker::new(p, &NOOP_VTABLE),
        |_| {},
        |_| {},
        |_| {},
    );
}
