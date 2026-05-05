//! Cross-worker progress exchange channels.
//!
//! When multiple workers run the same dataflow graph, each worker's
//! [`ProgressTracker`](super::subgraph::ProgressTracker) only sees local
//! capability changes. To make completion detection correct across workers,
//! capability changes must be broadcast so that every worker's tracker
//! reflects the **global** state of all capabilities.
//!
//! # Why this is needed
//!
//! Without progress exchange, worker A may release all its capabilities while
//! worker B still holds some. Worker A's tracker says "completed" and
//! force-closes operators, even though worker B's data hasn't arrived yet
//! via exchange channels. This causes silent data loss or hangs.
//!
//! With progress exchange, worker A's tracker knows about worker B's
//! capabilities (and vice versa). Completion is only reported when ALL
//! workers across the entire dataflow have released ALL capabilities.
//! No global barrier is needed — each worker independently verifies
//! completion because its tracker's implications already contain global
//! information from the broadcasts.
//!
//! # Architecture
//!
//! For N workers, we create N × (N-1) unidirectional FIFO channels.
//! Each worker gets:
//! - (N-1) senders: one to each peer worker
//! - (N-1) receivers: one from each peer worker
//!
//! Progress messages are `Vec<ProgressChange<T>>` batches containing
//! `(operator_index, output_port, timestamp, diff)` tuples.
//!
//! Channels are FIFO-ordered per sender to ensure that a capability
//! release (-1) is never observed before the corresponding acquire (+1).
//!
//! Senders notify the target worker's [`WakeHandle`] on send, ensuring
//! idle workers are woken to process incoming progress updates.
//!
//! # Network progress
//!
//! For cross-node progress exchange, the `Network` variant of
//! [`ProgressSender`] serializes changes and sends them via an unbounded
//! intermediary channel to a bridge task that feeds into the
//! [`TransportSession`](crate::communication::TransportSession)'s
//! priority progress channel. On the receiving side, a bridge task
//! reads from the Demuxer, deserializes, and pushes into the same
//! `SharedBuffer` used by local progress, waking the target worker.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::wake::WakeHandle;
use crate::progress::timestamp::Timestamp;

/// A single progress change: (operator_index, output_port, timestamp, diff).
///
/// - `diff > 0`: capability acquired (operator now holds a capability at this time)
/// - `diff < 0`: capability released (operator dropped or downgraded)
///
/// These are applied to the reachability tracker via `Tracker::update_source()`,
/// which propagates implications through the dataflow graph's path summaries.
pub type ProgressChange<T> = (usize, usize, T, i64);

/// Shared buffer between a sender/receiver pair.
pub(crate) struct SharedBuffer<T> {
    pub(crate) queue: VecDeque<Vec<ProgressChange<T>>>,
}

/// Sends progress updates to a single peer worker.
///
/// # Variants
///
/// - **Local**: Appends to a shared in-memory buffer and wakes the target
///   worker directly. Used for same-process workers.
/// - **Network** (requires `transport` feature): Serializes the batch and
///   sends it via an unbounded channel to a bridge task that feeds the
///   [`TransportSession`](crate::communication::TransportSession).
///   Delivery is reliable — the unbounded channel never drops frames.
///   The bridge task handles backpressure to the bounded transport layer.
pub struct ProgressSender<T: Timestamp> {
    pub(crate) inner: SenderInner<T>,
}

pub(crate) enum SenderInner<T: Timestamp> {
    Local {
        buffer: Arc<Mutex<SharedBuffer<T>>>,
        target_wake: WakeHandle,
    },
    #[cfg(feature = "transport")]
    Network {
        /// Type-erased send function that serializes and sends.
        /// Captures the codec, channel IDs, and unbounded sender at
        /// construction time so ProgressSender<T> doesn't require
        /// T: ExchangeData.
        send_fn: Box<dyn Fn(Vec<ProgressChange<T>>) + Send + Sync>,
    },
}

/// Receives progress updates from a single peer worker.
///
/// Drains all queued batches in FIFO order. This is called during
/// the progress propagation phase of the executor sweep.
///
/// For network progress, a background bridge task pushes deserialized
/// batches into the same shared buffer, so this type works identically
/// for both local and network progress.
pub struct ProgressReceiver<T: Timestamp> {
    buffer: Arc<Mutex<SharedBuffer<T>>>,
}

/// Per-worker collection of progress exchange channels.
///
/// Contains one sender per peer (for broadcasting local changes)
/// and one receiver per peer (for absorbing peer workers' changes).
/// Self-slots (own worker index) are `None`.
pub struct WorkerProgressChannels<T: Timestamp> {
    /// Senders indexed by target worker id. `senders[i]` sends to worker `i`.
    /// `senders[own_worker_id]` is `None` (no self-channel).
    pub senders: Vec<Option<ProgressSender<T>>>,
    /// Receivers indexed by source worker id. `receivers[i]` receives from worker `i`.
    /// `receivers[own_worker_id]` is `None`.
    pub receivers: Vec<Option<ProgressReceiver<T>>>,
}

impl<T: Timestamp> ProgressSender<T> {
    /// Create a local in-memory progress sender.
    pub(crate) fn local(buffer: Arc<Mutex<SharedBuffer<T>>>, target_wake: WakeHandle) -> Self {
        Self {
            inner: SenderInner::Local {
                buffer,
                target_wake,
            },
        }
    }

    /// Send a batch of progress changes to the peer.
    ///
    /// The batch is appended to the FIFO queue and the target worker is woken.
    /// Empty batches are ignored (no wake notification sent).
    ///
    /// For network senders, the batch is serialized and enqueued into an
    /// unbounded channel for reliable delivery. The send never fails
    /// (unless the bridge task has exited, which indicates a fatal error).
    pub fn send(&self, changes: Vec<ProgressChange<T>>) {
        if changes.is_empty() {
            return;
        }
        match &self.inner {
            SenderInner::Local {
                buffer,
                target_wake,
            } => {
                {
                    let mut buf = buffer.lock().expect("progress channel lock poisoned");
                    buf.queue.push_back(changes);
                }
                target_wake.notify();
            }
            #[cfg(feature = "transport")]
            SenderInner::Network { send_fn } => {
                send_fn(changes);
            }
        }
    }
}

impl<T: Timestamp> ProgressReceiver<T> {
    /// Create a progress receiver from a shared buffer.
    pub(crate) fn new(buffer: Arc<Mutex<SharedBuffer<T>>>) -> Self {
        Self { buffer }
    }

    /// Drain all queued progress batches from this peer.
    ///
    /// Returns an iterator over batches in FIFO order.
    /// Each batch is a `Vec<ProgressChange<T>>` as sent by the peer.
    pub fn drain_all(&self) -> Vec<Vec<ProgressChange<T>>> {
        let mut buf = self.buffer.lock().expect("progress channel lock poisoned");
        buf.queue.drain(..).collect()
    }

    /// Returns `true` if there are queued progress updates.
    pub fn has_pending(&self) -> bool {
        let buf = self.buffer.lock().expect("progress channel lock poisoned");
        !buf.queue.is_empty()
    }
}

/// Create progress exchange channels for `num_workers` workers.
///
/// Returns a `Vec` of length `num_workers`, where element `i` contains
/// the channels for worker `i`. Each worker gets senders to all peers
/// and receivers from all peers (self-slots are `None`).
///
/// The `wake_handles[i]` is the [`WakeHandle`] for worker `i`, used to
/// notify worker `i` when a peer sends it progress updates.
///
/// # Panics
///
/// Panics if `wake_handles.len() != num_workers`.
pub fn create_progress_channels<T: Timestamp>(
    num_workers: usize,
    wake_handles: &[WakeHandle],
) -> Vec<WorkerProgressChannels<T>> {
    assert_eq!(wake_handles.len(), num_workers);

    // Initialize per-worker channel collections.
    let mut all_channels: Vec<WorkerProgressChannels<T>> = (0..num_workers)
        .map(|_| WorkerProgressChannels {
            senders: (0..num_workers).map(|_| None).collect(),
            receivers: (0..num_workers).map(|_| None).collect(),
        })
        .collect();

    // Create a channel for each (sender_worker, receiver_worker) pair.
    for src in 0..num_workers {
        for dst in 0..num_workers {
            if src == dst {
                continue; // No self-channel.
            }

            let shared = Arc::new(Mutex::new(SharedBuffer {
                queue: VecDeque::new(),
            }));

            let sender = ProgressSender::local(Arc::clone(&shared), wake_handles[dst].clone());
            let receiver = ProgressReceiver::new(shared);

            all_channels[src].senders[dst] = Some(sender);
            all_channels[dst].receivers[src] = Some(receiver);
        }
    }

    all_channels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_receive_progress() {
        let wakes: Vec<WakeHandle> = (0..3).map(|_| WakeHandle::new()).collect();
        let mut channels = create_progress_channels::<u64>(3, &wakes);

        // Worker 0 sends to Worker 1.
        let w0 = &channels[0];
        w0.senders[1].as_ref().unwrap().send(vec![(0, 0, 42u64, 1)]);
        w0.senders[1]
            .as_ref()
            .unwrap()
            .send(vec![(0, 0, 42u64, -1)]);

        // Worker 1 receives from Worker 0.
        let w1 = &channels[1];
        let batches = w1.receivers[0].as_ref().unwrap().drain_all();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![(0, 0, 42, 1)]);
        assert_eq!(batches[1], vec![(0, 0, 42, -1)]);
    }

    #[test]
    fn empty_send_does_not_wake() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes);

        // Empty send — should not enqueue.
        channels[0].senders[1].as_ref().unwrap().send(vec![]);

        let batches = channels[1].receivers[0].as_ref().unwrap().drain_all();
        assert!(batches.is_empty());
    }

    #[test]
    fn fifo_ordering_preserved() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes);

        let sender = channels[0].senders[1].as_ref().unwrap();
        for i in 0..10 {
            sender.send(vec![(0, 0, i as u64, 1)]);
        }

        let batches = channels[1].receivers[0].as_ref().unwrap().drain_all();
        assert_eq!(batches.len(), 10);
        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(batch[0].2, i as u64);
        }
    }

    #[test]
    fn self_channels_are_none() {
        let wakes: Vec<WakeHandle> = (0..3).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(3, &wakes);

        for (i, ch) in channels.iter().enumerate() {
            assert!(ch.senders[i].is_none());
            assert!(ch.receivers[i].is_none());
        }
    }

    #[test]
    fn has_pending_reflects_state() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes);

        let recv = channels[1].receivers[0].as_ref().unwrap();
        assert!(!recv.has_pending());

        channels[0].senders[1]
            .as_ref()
            .unwrap()
            .send(vec![(0, 0, 1u64, 1)]);
        assert!(recv.has_pending());

        recv.drain_all();
        assert!(!recv.has_pending());
    }
}
