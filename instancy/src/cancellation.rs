//! Cancellation token for cooperative shutdown of dataflow computations.
//!
//! A [`CancellationToken`] is a thread-safe, clonable signal that allows
//! external code to request graceful shutdown of a running dataflow. It is
//! designed for use in synchronous operator logic (not async/await).
//!
//! Cancellation is **one-shot and permanent**: once a token is cancelled it
//! cannot be reset. Child tokens created from an already-cancelled parent
//! will immediately appear cancelled.
//!
//! # Distributed behaviour
//!
//! `CancellationToken` is an **in-process primitive** — it is not serialized
//! or sent over the network. In a distributed dataflow each process owns its
//! own local token. Cross-process cancellation is handled by the runtime's
//! control plane: the orchestrator sends a cancellation control message over
//! the wire, and the receiving process's runtime calls [`CancellationToken::cancel`]
//! on its local token. This keeps the token lightweight (pure atomics, no I/O)
//! while delegating distributed coordination to the existing progress/control
//! channel.
//!
//! # Usage
//!
//! ```
//! use instancy::cancellation::CancellationToken;
//!
//! let token = CancellationToken::new();
//! let child = token.child_token();
//!
//! // In another thread or context:
//! token.cancel();
//!
//! // In operator logic:
//! assert!(child.is_cancelled());
//! ```
//!
//! # Hierarchy
//!
//! Tokens form a tree: cancelling a parent automatically cancels all children.
//! This enables scoped cancellation — cancel a sub-dataflow without affecting
//! sibling computations.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::wake::WakeHandle;

/// The reason a dataflow was cancelled.
///
/// When a [`CancellationToken`] is cancelled, an optional reason can be
/// attached to distinguish user-initiated cancellation from system-level
/// causes. The reason follows **first-cancel-wins** semantics — if
/// `cancel()` or `cancel_with_reason()` is called multiple times, only
/// the first reason is recorded.
///
/// Child tokens inherit the reason from their parent: if a parent is
/// cancelled with a reason, children will report that same reason via
/// [`CancellationToken::reason()`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationReason {
    /// The user explicitly requested cancellation.
    UserRequested,

    /// The runtime is shutting down (e.g., [`crate::RuntimeHandle`] dropped or
    /// [`crate::worker_pool::WorkerPool`] torn down).
    RuntimeShutdown,

    /// A network-level error caused cancellation (e.g., TCP disconnect,
    /// transport session failure). The string describes the error.
    NetworkError(String),

    /// A worker in the dataflow failed, causing cascading cancellation
    /// of peer workers. The string describes which worker or what failed.
    WorkerFailed(String),

    /// The owning handle (e.g., [`crate::SpawnedDataflow`]) was dropped without
    /// calling `join()`, triggering automatic cancellation.
    HandleDropped,

    /// An operator produced an error that caused the dataflow to be cancelled.
    /// The string describes the operator and the error.
    OperatorError(String),

    /// A peer node in the cluster was reported as down by the hosting
    /// application via [`crate::RuntimeHandle::report_node_leave()`].
    /// The string is the peer's `node_id`.
    PeerDown(String),
}

impl fmt::Display for CancellationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UserRequested => write!(f, "user requested cancellation"),
            Self::RuntimeShutdown => write!(f, "runtime shutdown"),
            Self::NetworkError(msg) => write!(f, "network error: {msg}"),
            Self::WorkerFailed(msg) => write!(f, "worker failed: {msg}"),
            Self::HandleDropped => write!(f, "handle dropped"),
            Self::OperatorError(msg) => write!(f, "operator error: {msg}"),
            Self::PeerDown(node_id) => write!(f, "peer node down: {node_id}"),
        }
    }
}

/// Shared state for a cancellation token.
struct TokenInner {
    /// Whether this token has been cancelled.
    cancelled: AtomicBool,
    /// Parent token (if any). Cancelling the parent cancels this token.
    parent: Option<Arc<TokenInner>>,
    /// The reason for cancellation (first-cancel-wins).
    ///
    /// Only written once during the first `cancel()` / `cancel_with_reason()`
    /// call. Read by `reason()`.
    reason: Mutex<Option<CancellationReason>>,
    /// Wake handles to notify when this token is cancelled.
    ///
    /// Only accessed during `register_wake_handle()` (setup) and `cancel()`
    /// (one-shot). The `is_cancelled()` hot path remains lock-free.
    wake_handles: Mutex<Vec<WakeHandle>>,
    /// Async notification primitive for `cancelled_async()`.
    ///
    /// Signalled when this token is directly cancelled. Waiters on
    /// `cancelled_async()` await this instead of polling.
    async_notify: tokio::sync::Notify,
}

/// A cooperative cancellation signal for dataflow shutdown.
///
/// `CancellationToken` is cheap to clone (Arc-based) and can be shared
/// freely across threads. It is designed for polling-based cancellation:
/// operator logic periodically calls [`is_cancelled()`](CancellationToken::is_cancelled)
/// to check whether it should exit.
///
/// # Thread Safety
///
/// All operations are lock-free (atomic loads/stores). Checking cancellation
/// is a single atomic load — suitable for hot loops.
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<TokenInner>,
}

impl CancellationToken {
    /// Create a new, uncancelled token with no parent.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TokenInner {
                cancelled: AtomicBool::new(false),
                parent: None,
                reason: Mutex::new(None),
                wake_handles: Mutex::new(Vec::new()),
                async_notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Create a child token that is cancelled when either:
    /// - The child itself is cancelled, OR
    /// - This (parent) token is cancelled.
    ///
    /// Children do not affect the parent — cancelling a child does not
    /// propagate upward.
    ///
    /// When the parent is cancelled with a reason, the child inherits that
    /// reason via [`reason()`](Self::reason).
    pub fn child_token(&self) -> Self {
        Self {
            inner: Arc::new(TokenInner {
                cancelled: AtomicBool::new(false),
                parent: Some(Arc::clone(&self.inner)),
                reason: Mutex::new(None),
                wake_handles: Mutex::new(Vec::new()),
                async_notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Cancel this token with the default reason ([`CancellationReason::UserRequested`]).
    ///
    /// This is idempotent — calling cancel() multiple times is safe.
    /// Cancellation propagates to all child tokens (they observe it
    /// via their parent link).
    ///
    /// Any wake handles registered via [`register_wake_handle`](Self::register_wake_handle)
    /// are notified so that sleeping async executors wake promptly.
    pub fn cancel(&self) {
        self.cancel_with_reason(CancellationReason::UserRequested);
    }

    /// Cancel this token with a specific reason.
    ///
    /// The reason follows **first-cancel-wins** semantics: if this token
    /// is already cancelled — either directly or via an ancestor — the
    /// new reason is silently ignored. This prevents child tokens from
    /// overwriting an inherited parent reason.
    ///
    /// This is idempotent — subsequent calls do not change the state.
    pub fn cancel_with_reason(&self, reason: CancellationReason) {
        // If already cancelled (directly or via ancestor), do nothing.
        // This prevents a child from overwriting an inherited parent reason.
        if self.is_cancelled() {
            return;
        }
        // Store the reason BEFORE setting the cancelled flag so that
        // concurrent `reason()` callers never observe cancelled=true
        // with reason=None. The mutex serializes concurrent writers;
        // first writer wins (check r.is_none()).
        {
            let mut guard = self.inner.reason.lock().unwrap_or_else(|e| e.into_inner());
            if guard.is_none() {
                *guard = Some(reason);
            }
        }
        // Now set the flag and notify (only the first swap notifies).
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            // Wake synchronous executor handles.
            if let Ok(handles) = self.inner.wake_handles.lock() {
                for handle in handles.iter() {
                    handle.notify();
                }
            }
            // Wake async waiters on this token.
            self.inner.async_notify.notify_waiters();
        }
    }

    /// Returns the reason this token was cancelled, if available.
    ///
    /// If this token was directly cancelled via [`cancel()`](Self::cancel) or
    /// [`cancel_with_reason()`](Self::cancel_with_reason), returns that reason.
    /// If this token was cancelled transitively (via a parent), walks up the
    /// ancestor chain to find the reason.
    ///
    /// Returns `None` if the token is not cancelled.
    pub fn reason(&self) -> Option<CancellationReason> {
        if !self.is_cancelled() {
            return None;
        }
        // Check our own reason first (recover from poison — data is write-once)
        let guard = self.inner.reason.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(reason) = guard.as_ref() {
            return Some(reason.clone());
        }
        drop(guard);
        // Walk up the parent chain to find the reason
        let mut current = &self.inner.parent;
        while let Some(parent) = current {
            let guard = parent.reason.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(reason) = guard.as_ref() {
                return Some(reason.clone());
            }
            drop(guard);
            current = &parent.parent;
        }
        // Cancelled but no reason stored (shouldn't happen in normal usage)
        None
    }

    /// Check whether this token has been cancelled.
    ///
    /// Returns `true` if either this token or any ancestor has been cancelled.
    /// This is a cheap operation (one or two atomic loads) suitable for
    /// calling in tight loops.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        // Check self first (fast path for directly-cancelled tokens)
        if self.inner.cancelled.load(Ordering::Acquire) {
            return true;
        }
        // Walk up the parent chain
        self.check_ancestors()
    }

    /// Walk parent chain to check for cancellation.
    /// Separated from is_cancelled() to keep the fast path inlineable.
    #[cold]
    fn check_ancestors(&self) -> bool {
        let mut current = &self.inner.parent;
        while let Some(parent) = current {
            if parent.cancelled.load(Ordering::Acquire) {
                // Cache the cancellation locally so future checks are fast
                self.inner.cancelled.store(true, Ordering::Release);
                return true;
            }
            current = &parent.parent;
        }
        false
    }

    /// Check cancellation and return an error if cancelled.
    ///
    /// Convenience method for use in operator logic:
    /// ```ignore
    /// token.check()?;  // returns Err(Error::Cancelled { .. }) if cancelled
    /// ```
    #[inline]
    pub fn check(&self) -> crate::error::Result<()> {
        if self.is_cancelled() {
            Err(crate::error::Error::Cancelled {
                reason: self.reason(),
            })
        } else {
            Ok(())
        }
    }

    /// Register a wake handle to be notified when this token is cancelled.
    ///
    /// The handle is registered on this token **and all ancestor tokens** so
    /// that parent cancellation (which children detect via `is_cancelled()`
    /// polling) also wakes sleeping executors promptly.
    ///
    /// **Race safety:** If the token (or any ancestor) is already cancelled at
    /// the time of registration, the handle is notified immediately. This
    /// prevents a lost-wakeup race where cancellation fires just before
    /// registration completes.
    ///
    /// This method is called once during executor setup — it is not on the
    /// hot path. The `is_cancelled()` check remains lock-free.
    ///
    /// **Note on lifecycle:** The WakeHandle clones stored in ancestor tokens
    /// are *not* automatically removed when the child token or executor is
    /// dropped. After executor completion, `WakeHandle::clear_waker()` should
    /// be called to break the Waker→Executor reference chain, leaving only a
    /// lightweight `Arc<WakeState>` (AtomicBool + empty Mutex). For extremely
    /// long-lived runtimes that spawn millions of dataflows, consider switching
    /// to `Weak<WakeState>` in the Vec to allow full reclamation.
    pub fn register_wake_handle(&self, handle: WakeHandle) {
        // Register on self
        Self::register_on_node(&self.inner, handle.clone());

        // Register on all ancestors so parent.cancel() also wakes us
        let mut current = &self.inner.parent;
        while let Some(ancestor) = current {
            Self::register_on_node(ancestor, handle.clone());
            current = &ancestor.parent;
        }

        // Race safety: re-check cancellation after registration. If the token
        // was cancelled between our initial check and the registration, we
        // might have missed the notification. Notify now to be safe.
        if self.is_cancelled() {
            handle.notify();
        }
    }

    /// Register a wake handle on a single token node.
    /// If the node is already cancelled, notify immediately.
    fn register_on_node(node: &Arc<TokenInner>, handle: WakeHandle) {
        if let Ok(mut handles) = node.wake_handles.lock() {
            handles.push(handle.clone());
        }
        // If this node is already cancelled, notify immediately.
        // This handles the race where cancel() ran before we inserted.
        if node.cancelled.load(Ordering::Acquire) {
            handle.notify();
        }
    }

    /// Returns a [`CancellationGuard`] that cancels this token when dropped.
    ///
    /// Useful for RAII-style cancellation: when the guard goes out of scope
    /// (e.g., the owning task completes or panics), the token is cancelled.
    pub fn drop_guard(self) -> CancellationGuard {
        CancellationGuard { token: Some(self) }
    }

    /// Returns a future that resolves when this token is cancelled.
    ///
    /// Uses efficient waker-based notification — no polling. Suitable for
    /// use in `tokio::select!` branches to respond to cancellation in async code.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tokio::select! {
    ///     _ = token.cancelled_async() => { /* cancelled */ }
    ///     result = do_work() => { /* completed */ }
    /// }
    /// ```
    /// Returns a future that resolves when this token is cancelled.
    ///
    /// Uses efficient waker-based notification — no polling. Suitable for
    /// use in `tokio::select!` branches to respond to cancellation in async code.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tokio::select! {
    ///     _ = token.cancelled_async() => { /* cancelled */ }
    ///     result = do_work() => { /* completed */ }
    /// }
    /// ```
    /// Returns a future that resolves when this token is cancelled.
    ///
    /// Uses efficient waker-based notification — no polling. Suitable for
    /// use in `tokio::select!` branches to respond to cancellation in async code.
    ///
    /// # Example
    ///
    /// ```ignore
    /// tokio::select! {
    ///     _ = token.cancelled_async() => { /* cancelled */ }
    ///     result = do_work() => { /* completed */ }
    /// }
    /// ```
    pub async fn cancelled_async(&self) {
        // Fast path: already cancelled (self or ancestor).
        if self.is_cancelled() {
            return;
        }

        loop {
            // Await notification from self or any ancestor.
            // After waking, re-check the flag to confirm actual cancellation.
            //
            // Race safety: tokio's Notified future registers interest on first
            // poll (inside select!/await). If cancel_with_reason() calls
            // notify_waiters() between our is_cancelled() check above and the
            // first poll of notified(), the notification is missed. To handle
            // this, we re-check is_cancelled() immediately after the first poll
            // registers interest. The select! macro polls all branches once on
            // entry, registering interest, so the window is only between our
            // explicit check and the select! entry — which is synchronous code
            // with no yield points. The only problematic ordering is:
            //   1. We check is_cancelled() → false
            //   2. Another thread calls cancel() + notify_waiters()
            //   3. We enter select! and poll notified() (registers interest)
            // In this case the notification at step 2 is lost. To handle this,
            // we add a brief re-check after first poll using a fused approach.
            //
            // The correct pattern: create Notified, enable it (registers
            // interest), then check the flag, then await.
            let notified = self.inner.async_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.is_cancelled() {
                return;
            }

            // If there are ancestors, race our notified with ancestor notifies.
            match &self.inner.parent {
                None => notified.await,
                Some(parent) => {
                    tokio::select! {
                        biased;
                        _ = notified => {}
                        _ = Self::await_ancestor_notify(parent) => {}
                    }
                }
            }

            if self.is_cancelled() {
                return;
            }
        }
    }

    /// Await notification from an ancestor node and its parents.
    fn await_ancestor_notify(
        node: &Arc<TokenInner>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let notified = node.async_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match &node.parent {
                None => notified.await,
                Some(parent) => {
                    tokio::select! {
                        biased;
                        _ = notified => {}
                        _ = Self::await_ancestor_notify(parent) => {}
                    }
                }
            }
        })
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .field("reason", &self.reason())
            .finish()
    }
}

/// RAII guard that cancels a token when dropped.
///
/// Created by [`CancellationToken::drop_guard`].
#[derive(Debug)]
pub struct CancellationGuard {
    token: Option<CancellationToken>,
}

impl CancellationGuard {
    /// Disarm the guard — the token will NOT be cancelled on drop.
    ///
    /// Returns the inner token for continued use.
    pub fn disarm(mut self) -> CancellationToken {
        // SAFETY: token is always Some until disarm() or Drop consumes it,
        // and disarm(self) takes ownership preventing double-call.
        self.token
            .take()
            .expect("guard already disarmed")
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            token.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn new_token_is_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancel_sets_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_is_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn clone_shares_state() {
        let token = CancellationToken::new();
        let clone = token.clone();

        token.cancel();
        assert!(clone.is_cancelled());
    }

    #[test]
    fn child_inherits_parent_cancellation() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn child_cancellation_does_not_affect_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn grandchild_inherits_grandparent_cancellation() {
        let grandparent = CancellationToken::new();
        let parent = grandparent.child_token();
        let child = parent.child_token();

        assert!(!child.is_cancelled());
        grandparent.cancel();
        assert!(child.is_cancelled());
        assert!(parent.is_cancelled());
    }

    #[test]
    fn sibling_tokens_are_independent() {
        let parent = CancellationToken::new();
        let child1 = parent.child_token();
        let child2 = parent.child_token();

        child1.cancel();
        assert!(!child2.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[test]
    fn check_returns_ok_when_not_cancelled() {
        let token = CancellationToken::new();
        assert!(token.check().is_ok());
    }

    #[test]
    fn check_returns_cancelled_error() {
        let token = CancellationToken::new();
        token.cancel();
        let err = token.check().unwrap_err();
        assert!(matches!(err, crate::error::Error::Cancelled { .. }));
    }

    #[test]
    fn check_returns_cancelled_from_parent() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        parent.cancel();
        let err = child.check().unwrap_err();
        assert!(matches!(err, crate::error::Error::Cancelled { .. }));
    }

    #[test]
    fn drop_guard_cancels_on_drop() {
        let token = CancellationToken::new();
        let clone = token.clone();

        {
            let _guard = token.drop_guard();
        } // guard dropped here

        assert!(clone.is_cancelled());
    }

    #[test]
    fn drop_guard_disarm_prevents_cancellation() {
        let token = CancellationToken::new();
        let clone = token.clone();

        let guard = clone.drop_guard();
        let _recovered = guard.disarm();

        assert!(!token.is_cancelled());
    }

    #[test]
    fn cross_thread_cancellation() {
        let token = CancellationToken::new();
        let thread_token = token.clone();

        let handle = thread::spawn(move || {
            // Spin until cancelled
            while !thread_token.is_cancelled() {
                thread::yield_now();
            }
            true
        });

        // Give the thread a moment to start
        thread::sleep(std::time::Duration::from_millis(10));
        token.cancel();

        assert!(handle.join().unwrap());
    }

    #[test]
    fn default_is_not_cancelled() {
        let token = CancellationToken::default();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn token_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CancellationToken>();
        assert_send_sync::<CancellationGuard>();
    }

    #[test]
    fn cached_parent_cancellation_makes_subsequent_checks_fast() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        parent.cancel();
        // First check walks the parent chain and caches
        assert!(child.is_cancelled());
        // Verify the cached state (should be true directly now)
        assert!(child.inner.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn many_children_all_cancelled() {
        let parent = CancellationToken::new();
        let children: Vec<_> = (0..100).map(|_| parent.child_token()).collect();

        parent.cancel();

        for child in &children {
            assert!(child.is_cancelled());
        }
    }

    #[test]
    fn deep_hierarchy() {
        let root = CancellationToken::new();
        let mut current = root.child_token();
        for _ in 0..10 {
            current = current.child_token();
        }

        assert!(!current.is_cancelled());
        root.cancel();
        assert!(current.is_cancelled());
    }

    #[test]
    fn concurrent_cancel_and_check() {
        let token = CancellationToken::new();
        let checker = token.clone();

        let threads: Vec<_> = (0..8)
            .map(|i| {
                let t = if i % 2 == 0 {
                    token.clone()
                } else {
                    checker.clone()
                };
                thread::spawn(move || {
                    if i == 0 {
                        thread::sleep(std::time::Duration::from_millis(5));
                        t.cancel();
                    } else {
                        while !t.is_cancelled() {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_notifies_registered_wake_handle() {
        let token = CancellationToken::new();
        let wake = WakeHandle::new();
        // Consume the initial notification (WakeHandle starts notified)
        wake.take_notification();

        token.register_wake_handle(wake.clone());
        assert!(!wake.take_notification());

        token.cancel();
        assert!(wake.take_notification());
    }

    #[test]
    fn parent_cancel_notifies_child_wake_handle() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let wake = WakeHandle::new();
        wake.take_notification();

        // Register on child — should also register on parent
        child.register_wake_handle(wake.clone());
        assert!(!wake.take_notification());

        // Cancel parent — child's wake handle should be notified
        parent.cancel();
        assert!(wake.take_notification());
        assert!(child.is_cancelled());
    }

    #[test]
    fn register_on_already_cancelled_token_notifies_immediately() {
        let token = CancellationToken::new();
        token.cancel();

        let wake = WakeHandle::new();
        wake.take_notification();

        // Register after cancellation — should notify immediately
        token.register_wake_handle(wake.clone());
        assert!(wake.take_notification());
    }

    #[test]
    fn register_on_child_of_already_cancelled_parent_notifies_immediately() {
        let parent = CancellationToken::new();
        parent.cancel();

        let child = parent.child_token();
        let wake = WakeHandle::new();
        wake.take_notification();

        child.register_wake_handle(wake.clone());
        assert!(wake.take_notification());
    }

    #[test]
    fn multiple_wake_handles_all_notified() {
        let token = CancellationToken::new();
        let wake1 = WakeHandle::new();
        let wake2 = WakeHandle::new();
        wake1.take_notification();
        wake2.take_notification();

        token.register_wake_handle(wake1.clone());
        token.register_wake_handle(wake2.clone());

        token.cancel();
        assert!(wake1.take_notification());
        assert!(wake2.take_notification());
    }

    #[test]
    fn idempotent_cancel_only_notifies_once() {
        let token = CancellationToken::new();
        let wake = WakeHandle::new();
        wake.take_notification();

        token.register_wake_handle(wake.clone());
        token.cancel();
        assert!(wake.take_notification());

        // Second cancel — wake handle was already notified during first cancel.
        // swap(true) returns true, so notification loop is skipped.
        token.cancel();
        assert!(!wake.take_notification());
    }

    // -----------------------------------------------------------------------
    // CancellationReason tests
    // -----------------------------------------------------------------------

    #[test]
    fn cancel_default_reason_is_user_requested() {
        let token = CancellationToken::new();
        token.cancel();
        assert_eq!(token.reason(), Some(CancellationReason::UserRequested));
    }

    #[test]
    fn cancel_with_reason_stores_reason() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::RuntimeShutdown);
        assert_eq!(token.reason(), Some(CancellationReason::RuntimeShutdown));
    }

    #[test]
    fn cancel_with_network_error_reason() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::NetworkError("peer lost".into()));
        assert_eq!(
            token.reason(),
            Some(CancellationReason::NetworkError("peer lost".into()))
        );
    }

    #[test]
    fn cancel_with_worker_failed_reason() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::WorkerFailed("worker 3 panicked".into()));
        assert_eq!(
            token.reason(),
            Some(CancellationReason::WorkerFailed("worker 3 panicked".into()))
        );
    }

    #[test]
    fn cancel_with_handle_dropped_reason() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::HandleDropped);
        assert_eq!(token.reason(), Some(CancellationReason::HandleDropped));
    }

    #[test]
    fn cancel_with_operator_error_reason() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::OperatorError("division by zero".into()));
        assert_eq!(
            token.reason(),
            Some(CancellationReason::OperatorError("division by zero".into()))
        );
    }

    #[test]
    fn reason_returns_none_when_not_cancelled() {
        let token = CancellationToken::new();
        assert_eq!(token.reason(), None);
    }

    #[test]
    fn first_cancel_wins_semantics() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::NetworkError("first".into()));
        token.cancel_with_reason(CancellationReason::UserRequested);
        // First reason wins
        assert_eq!(
            token.reason(),
            Some(CancellationReason::NetworkError("first".into()))
        );
    }

    #[test]
    fn child_inherits_parent_reason() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        parent.cancel_with_reason(CancellationReason::RuntimeShutdown);
        assert!(child.is_cancelled());
        assert_eq!(child.reason(), Some(CancellationReason::RuntimeShutdown));
    }

    #[test]
    fn grandchild_inherits_grandparent_reason() {
        let gp = CancellationToken::new();
        let parent = gp.child_token();
        let child = parent.child_token();

        gp.cancel_with_reason(CancellationReason::NetworkError("link down".into()));
        assert!(child.is_cancelled());
        assert_eq!(
            child.reason(),
            Some(CancellationReason::NetworkError("link down".into()))
        );
    }

    #[test]
    fn child_direct_reason_takes_precedence_when_cancelled_first() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        // Cancel child first with its own reason (before parent)
        child.cancel_with_reason(CancellationReason::OperatorError("bad op".into()));
        // Then cancel parent
        parent.cancel_with_reason(CancellationReason::RuntimeShutdown);

        // Child's own reason takes precedence (check self before walking parent)
        assert_eq!(
            child.reason(),
            Some(CancellationReason::OperatorError("bad op".into()))
        );
    }

    #[test]
    fn child_cannot_overwrite_inherited_parent_reason() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        // Cancel parent first — child is now effectively cancelled
        parent.cancel_with_reason(CancellationReason::RuntimeShutdown);
        assert!(child.is_cancelled());

        // Attempting to cancel child with a different reason should be no-op
        child.cancel_with_reason(CancellationReason::HandleDropped);

        // Child should still report parent's reason
        assert_eq!(child.reason(), Some(CancellationReason::RuntimeShutdown));
    }

    #[test]
    fn check_includes_reason_in_error() {
        let token = CancellationToken::new();
        token.cancel_with_reason(CancellationReason::NetworkError("timeout".into()));
        let err = token.check().unwrap_err();
        match err {
            crate::error::Error::Cancelled { reason } => {
                assert_eq!(
                    reason,
                    Some(CancellationReason::NetworkError("timeout".into()))
                );
            }
            other => panic!("expected Cancelled, got {:?}", other),
        }
    }

    #[test]
    fn cancellation_reason_display() {
        assert_eq!(
            CancellationReason::UserRequested.to_string(),
            "user requested cancellation"
        );
        assert_eq!(
            CancellationReason::RuntimeShutdown.to_string(),
            "runtime shutdown"
        );
        assert_eq!(
            CancellationReason::NetworkError("peer lost".into()).to_string(),
            "network error: peer lost"
        );
        assert_eq!(
            CancellationReason::WorkerFailed("w3".into()).to_string(),
            "worker failed: w3"
        );
        assert_eq!(
            CancellationReason::HandleDropped.to_string(),
            "handle dropped"
        );
        assert_eq!(
            CancellationReason::OperatorError("oops".into()).to_string(),
            "operator error: oops"
        );
        assert_eq!(
            CancellationReason::PeerDown("node-3".into()).to_string(),
            "peer node down: node-3"
        );
    }

    #[test]
    fn error_display_cancelled_with_reason() {
        let err = crate::error::Error::Cancelled {
            reason: Some(CancellationReason::RuntimeShutdown),
        };
        assert_eq!(err.to_string(), "Dataflow cancelled: runtime shutdown");
    }

    #[test]
    fn error_display_cancelled_without_reason() {
        let err = crate::error::Error::Cancelled { reason: None };
        assert_eq!(err.to_string(), "Dataflow cancelled");
    }
}
