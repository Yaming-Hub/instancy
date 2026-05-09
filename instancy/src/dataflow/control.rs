//! Cross-worker control broadcast channel.
//!
//! When multiple workers execute a dataflow in parallel, they need a way to
//! communicate control signals (primarily errors) without using the data plane.
//! `ControlBroadcast` provides a simple broadcast mechanism:
//!
//! - Any worker can send a [`WorkerControl`] signal via its [`ControlSender`]
//! - All workers (including the sender) receive it via their [`ControlReceiver`]
//! - Error signals automatically trigger dataflow cancellation
//!
//! # Naming
//!
//! This module uses `WorkerControl` (not `ControlSignal`) to avoid confusion
//! with [`crate::dataflow::channels::ControlSignal`], which carries per-edge
//! control messages (watermarks, errors) on the data plane. `WorkerControl`
//! operates on the management plane — cross-worker coordination that is
//! orthogonal to the dataflow graph.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::cancellation::{CancellationReason, CancellationToken};
use crate::dataflow::channels::wake::WakeHandle;

// ---------------------------------------------------------------------------
// WorkerControl — the signal type
// ---------------------------------------------------------------------------

/// A control signal broadcast across all workers in a dataflow.
///
/// These signals travel out-of-band (not through the dataflow graph) and are
/// used for coordination, error propagation, and lifecycle management.
#[derive(Debug, Clone)]
pub enum WorkerControl {
    /// A worker encountered a fatal operator error.
    ///
    /// When broadcast, this automatically cancels the dataflow via the shared
    /// [`CancellationToken`]. Sibling workers see the cancellation on their
    /// next activation sweep.
    WorkerError {
        /// Index of the worker that failed.
        worker_index: usize,
        /// Name of the operator that produced the error.
        operator: String,
        /// Human-readable error description.
        message: String,
    },

    /// Explicit cancellation request from a worker.
    ///
    /// Carries a [`CancellationReason`] that is forwarded to the shared
    /// dataflow cancellation token.
    Cancel {
        /// Index of the worker requesting cancellation.
        worker_index: usize,
        /// Why cancellation was requested.
        reason: CancellationReason,
    },

    /// A worker-defined limit was reached (e.g., row count, byte budget).
    ///
    /// The dataflow is **not** automatically cancelled — the application
    /// decides how to respond (cancel, drain, ignore).
    LimitReached {
        /// Index of the worker that hit the limit.
        worker_index: usize,
        /// Human-readable description of the limit.
        description: String,
    },
}

impl fmt::Display for WorkerControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerError {
                worker_index,
                operator,
                message,
            } => write!(
                f,
                "worker {worker_index} operator '{operator}' error: {message}"
            ),
            Self::Cancel {
                worker_index,
                reason,
            } => write!(f, "worker {worker_index} cancel: {reason}"),
            Self::LimitReached {
                worker_index,
                description,
            } => write!(f, "worker {worker_index} limit reached: {description}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Broadcast internals
// ---------------------------------------------------------------------------

struct BroadcastInner {
    /// Signal log. Compacted when all receivers have advanced past a prefix.
    signals: Vec<WorkerControl>,
    /// Logical index of `signals[0]`. After compaction, this advances so
    /// receiver cursors remain valid without adjustment.
    base_offset: usize,
    /// Per-worker wake handles for non-cancelling signals (e.g., LimitReached).
    /// Cancelling signals rely on CancellationToken's wake mechanism.
    wake_handles: Vec<WakeHandle>,
    /// Per-receiver logical cursor (next signal index to return).
    /// Updated by receivers on `try_recv`; used for compaction.
    receiver_cursors: Vec<usize>,
}

// ---------------------------------------------------------------------------
// ControlSender — cloneable, any worker can send
// ---------------------------------------------------------------------------

/// Sender half of the control broadcast channel.
///
/// Cloneable: multiple senders can coexist (e.g., one per worker plus one
/// held by the runtime for external control injection).
#[derive(Clone)]
pub struct ControlSender {
    inner: Arc<Mutex<BroadcastInner>>,
    worker_index: usize,
    dataflow_cancel: CancellationToken,
}

impl ControlSender {
    /// Broadcast an operator error from this worker.
    ///
    /// This appends a [`WorkerControl::WorkerError`] to the signal log and
    /// cancels the dataflow token with [`CancellationReason::OperatorError`].
    /// Sibling workers are woken via the cancellation token's registered
    /// wake handles.
    pub fn broadcast_error(&self, operator: String, message: String) {
        let signal = WorkerControl::WorkerError {
            worker_index: self.worker_index,
            operator,
            message: message.clone(),
        };
        self.append_signal(signal);
        self.dataflow_cancel
            .cancel_with_reason(CancellationReason::OperatorError(message));
    }

    /// Broadcast a cancellation request from this worker.
    ///
    /// Appends a [`WorkerControl::Cancel`] and cancels the dataflow token
    /// with the given reason.
    pub fn broadcast_cancel(&self, reason: CancellationReason) {
        let signal = WorkerControl::Cancel {
            worker_index: self.worker_index,
            reason: reason.clone(),
        };
        self.append_signal(signal);
        self.dataflow_cancel.cancel_with_reason(reason);
    }

    /// Broadcast a limit-reached signal from this worker.
    ///
    /// This does **not** cancel the dataflow — the application decides how
    /// to respond. Sibling workers are explicitly woken so they can drain
    /// the signal promptly.
    pub fn broadcast_limit(&self, description: String) {
        let signal = WorkerControl::LimitReached {
            worker_index: self.worker_index,
            description,
        };
        // Clone wake handles under the lock, then release before notifying
        // to avoid calling into external code while holding the mutex.
        let wake_handles = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.signals.push(signal);
            inner.wake_handles.clone()
        };
        for wh in &wake_handles {
            wh.notify();
        }
    }

    /// Returns the worker index associated with this sender.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    fn append_signal(&self, signal: WorkerControl) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.signals.push(signal);
    }
}

impl fmt::Debug for ControlSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlSender")
            .field("worker_index", &self.worker_index)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ControlReceiver — single-owner, per-worker cursor
// ---------------------------------------------------------------------------

/// Receiver half of the control broadcast channel.
///
/// Not cloneable: each worker owns exactly one receiver with its own read
/// cursor. Call [`try_recv`](Self::try_recv) to drain new signals.
pub struct ControlReceiver {
    inner: Arc<Mutex<BroadcastInner>>,
    /// Receiver index in `BroadcastInner::receiver_cursors`.
    receiver_index: usize,
}

impl ControlReceiver {
    /// Drain all signals that arrived since the last call.
    ///
    /// Returns an empty vec if no new signals are available.
    /// Advances the cursor so each signal is delivered exactly once.
    /// Triggers compaction of the signal log when all receivers have
    /// advanced past a prefix.
    pub fn try_recv(&mut self) -> Vec<WorkerControl> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cursor = inner.receiver_cursors[self.receiver_index];
        let logical_end = inner.base_offset + inner.signals.len();
        if cursor >= logical_end {
            return Vec::new();
        }
        let start = cursor - inner.base_offset;
        let new = inner.signals[start..].to_vec();
        inner.receiver_cursors[self.receiver_index] = logical_end;

        // Compact: if all receivers have advanced past some prefix, drain it.
        let min_cursor = inner.receiver_cursors.iter().copied().min().unwrap_or(0);
        let drain_count = min_cursor.saturating_sub(inner.base_offset);
        if drain_count > 0 {
            inner.signals.drain(..drain_count);
            inner.base_offset += drain_count;
        }

        new
    }

    /// Returns the number of unread signals.
    pub fn pending(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cursor = inner.receiver_cursors[self.receiver_index];
        let logical_end = inner.base_offset + inner.signals.len();
        logical_end.saturating_sub(cursor)
    }
}

impl fmt::Debug for ControlReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlReceiver")
            .field("receiver_index", &self.receiver_index)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ControlBroadcast — factory
// ---------------------------------------------------------------------------

/// Factory for creating matched sender/receiver pairs for a multi-worker
/// dataflow.
///
/// Created once per dataflow in `spawn_multi_internal`. Produces one
/// [`ControlSender`] and one [`ControlReceiver`] per worker, all sharing
/// the same underlying signal log.
///
/// This type is `pub(crate)` — it is internal runtime wiring. Users
/// interact with the broadcast via [`ControlSender`] (for sending signals)
/// and [`ControlReceiver`] (for receiving them).
pub(crate) struct ControlBroadcast;

impl ControlBroadcast {
    /// Create sender/receiver pairs for `num_workers` workers.
    ///
    /// Each worker gets one sender (identified by worker index) and one
    /// receiver (with an independent read cursor starting at 0).
    ///
    /// `wake_handles` must have exactly `num_workers` entries.
    /// `dataflow_cancel` is the shared cancellation token for the dataflow.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        num_workers: usize,
        wake_handles: &[WakeHandle],
        dataflow_cancel: CancellationToken,
    ) -> crate::Result<(Vec<ControlSender>, Vec<ControlReceiver>)> {
        if wake_handles.len() != num_workers {
            return Err(crate::Error::InvalidConfig(format!(
                "wake_handles length ({}) must match num_workers ({num_workers})",
                wake_handles.len()
            )));
        }

        let inner = Arc::new(Mutex::new(BroadcastInner {
            signals: Vec::new(),
            base_offset: 0,
            wake_handles: wake_handles.to_vec(),
            receiver_cursors: vec![0; num_workers],
        }));

        let senders = (0..num_workers)
            .map(|i| ControlSender {
                inner: Arc::clone(&inner),
                worker_index: i,
                dataflow_cancel: dataflow_cancel.clone(),
            })
            .collect();

        let receivers = (0..num_workers)
            .map(|i| ControlReceiver {
                inner: Arc::clone(&inner),
                receiver_index: i,
            })
            .collect();

        Ok((senders, receivers))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_broadcast(n: usize) -> (Vec<ControlSender>, Vec<ControlReceiver>, CancellationToken) {
        let parent = CancellationToken::new();
        let df_cancel = parent.child_token();
        let wakes: Vec<WakeHandle> = (0..n).map(|_| WakeHandle::new()).collect();
        let (senders, receivers) = ControlBroadcast::new(n, &wakes, df_cancel.clone()).unwrap();
        (senders, receivers, df_cancel)
    }

    #[test]
    fn broadcast_error_cancels_token() {
        let (senders, _receivers, df_cancel) = make_broadcast(3);
        assert!(!df_cancel.is_cancelled());

        senders[1].broadcast_error("op_fail".into(), "boom".into());

        assert!(df_cancel.is_cancelled());
        assert_eq!(
            df_cancel.reason(),
            Some(CancellationReason::OperatorError("boom".into()))
        );
    }

    #[test]
    fn broadcast_cancel_cancels_token() {
        let (senders, _receivers, df_cancel) = make_broadcast(2);

        senders[0].broadcast_cancel(CancellationReason::UserRequested);

        assert!(df_cancel.is_cancelled());
        assert_eq!(df_cancel.reason(), Some(CancellationReason::UserRequested));
    }

    #[test]
    fn broadcast_limit_does_not_cancel() {
        let (senders, _receivers, df_cancel) = make_broadcast(2);

        senders[0].broadcast_limit("row limit 1000".into());

        assert!(!df_cancel.is_cancelled());
    }

    #[test]
    fn receiver_drains_all_signals() {
        let (senders, mut receivers, _cancel) = make_broadcast(2);

        senders[0].broadcast_limit("limit A".into());
        senders[1].broadcast_limit("limit B".into());

        // Worker 0's receiver sees both signals.
        let signals = receivers[0].try_recv();
        assert_eq!(signals.len(), 2);
        assert!(matches!(
            &signals[0],
            WorkerControl::LimitReached { worker_index: 0, description } if description == "limit A"
        ));
        assert!(matches!(
            &signals[1],
            WorkerControl::LimitReached { worker_index: 1, description } if description == "limit B"
        ));

        // Second call returns empty (cursor advanced).
        assert!(receivers[0].try_recv().is_empty());
    }

    #[test]
    fn receivers_independent_cursors() {
        let (senders, mut receivers, _cancel) = make_broadcast(2);

        senders[0].broadcast_limit("signal 1".into());

        // Worker 0 drains.
        let s0 = receivers[0].try_recv();
        assert_eq!(s0.len(), 1);

        // Worker 1 hasn't drained yet — still sees the signal.
        let s1 = receivers[1].try_recv();
        assert_eq!(s1.len(), 1);

        // New signal: only those who haven't drained see it.
        senders[1].broadcast_limit("signal 2".into());

        let s0_new = receivers[0].try_recv();
        assert_eq!(s0_new.len(), 1); // only signal 2

        let s1_new = receivers[1].try_recv();
        assert_eq!(s1_new.len(), 1); // only signal 2
    }

    #[test]
    fn pending_count() {
        let (senders, receivers, _cancel) = make_broadcast(2);

        assert_eq!(receivers[0].pending(), 0);

        senders[0].broadcast_limit("x".into());
        senders[1].broadcast_limit("y".into());

        assert_eq!(receivers[0].pending(), 2);
        assert_eq!(receivers[1].pending(), 2);
    }

    #[test]
    fn first_cancel_wins() {
        let (senders, _receivers, df_cancel) = make_broadcast(2);

        senders[0].broadcast_error("op_a".into(), "first".into());
        senders[1].broadcast_error("op_b".into(), "second".into());

        // First cancel wins — reason should be "first".
        assert_eq!(
            df_cancel.reason(),
            Some(CancellationReason::OperatorError("first".into()))
        );
    }

    #[test]
    fn display_formatting() {
        let err = WorkerControl::WorkerError {
            worker_index: 2,
            operator: "Map".into(),
            message: "div by zero".into(),
        };
        assert_eq!(
            format!("{err}"),
            "worker 2 operator 'Map' error: div by zero"
        );

        let cancel = WorkerControl::Cancel {
            worker_index: 0,
            reason: CancellationReason::UserRequested,
        };
        assert_eq!(
            format!("{cancel}"),
            "worker 0 cancel: user requested cancellation"
        );

        let limit = WorkerControl::LimitReached {
            worker_index: 1,
            description: "100 rows".into(),
        };
        assert_eq!(format!("{limit}"), "worker 1 limit reached: 100 rows");
    }

    #[test]
    fn sender_is_clone() {
        let (senders, _receivers, _cancel) = make_broadcast(1);
        let _clone = senders[0].clone();
    }

    #[test]
    fn sender_worker_index() {
        let (senders, _receivers, _cancel) = make_broadcast(3);
        assert_eq!(senders[0].worker_index(), 0);
        assert_eq!(senders[1].worker_index(), 1);
        assert_eq!(senders[2].worker_index(), 2);
    }

    #[test]
    fn signal_log_compacted_after_all_receivers_drain() {
        let (senders, mut receivers, _cancel) = make_broadcast(2);

        // Send 3 signals.
        senders[0].broadcast_limit("a".into());
        senders[1].broadcast_limit("b".into());
        senders[0].broadcast_limit("c".into());

        // Receiver 0 drains all 3.
        let s0 = receivers[0].try_recv();
        assert_eq!(s0.len(), 3);

        // Log is NOT compacted yet — receiver 1 hasn't drained.
        {
            let inner = receivers[0].inner.lock().unwrap();
            assert_eq!(inner.signals.len(), 3);
            assert_eq!(inner.base_offset, 0);
        }

        // Receiver 1 drains all 3.
        let s1 = receivers[1].try_recv();
        assert_eq!(s1.len(), 3);

        // Now the log should be compacted — both cursors advanced past all signals.
        {
            let inner = receivers[0].inner.lock().unwrap();
            assert_eq!(
                inner.signals.len(),
                0,
                "log should be empty after compaction"
            );
            assert_eq!(inner.base_offset, 3, "base_offset should advance");
        }

        // New signals still work after compaction.
        senders[0].broadcast_limit("d".into());
        let s0_new = receivers[0].try_recv();
        assert_eq!(s0_new.len(), 1);
        assert!(matches!(
            &s0_new[0],
            WorkerControl::LimitReached { description, .. } if description == "d"
        ));
    }
}
