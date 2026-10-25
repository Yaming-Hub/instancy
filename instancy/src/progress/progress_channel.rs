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
use crate::error::{DataflowError, LockResultExt};
use crate::progress::timestamp::Timestamp;

/// Whether a [`ProgressChange`] is a capability change at an operator output
/// (a *source* pointstamp) or an in-flight message change at an operator input
/// (a *target* pointstamp).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProgressKind {
    /// Capability change at `(node, output_port)` — applied via `update_source`.
    Source,
    /// In-flight message change at `(node, input_port)` — applied via
    /// `update_target`. Used by boundary (exchange) channels: the sender
    /// broadcasts `+count` and the consumer `-count`, so every worker's tracker
    /// observes the global in-flight total.
    Target,
}

/// A single progress change broadcast between workers.
///
/// - `kind`: whether this is a source (capability) or target (in-flight message)
///   change — selects `Tracker::update_source` vs `update_target` on receipt.
/// - `node`/`port`: the operator and its output (source) or input (target) port.
/// - `time`: the timestamp.
/// - `diff`: `> 0` acquired/produced, `< 0` released/consumed.
#[derive(Clone, Debug, PartialEq)]
pub struct ProgressChange<T> {
    pub kind: ProgressKind,
    pub node: usize,
    pub port: usize,
    pub time: T,
    pub diff: i64,
}

impl<T> ProgressChange<T> {
    /// A capability change at an operator output port.
    #[cfg(test)]
    pub fn source(node: usize, port: usize, time: T, diff: i64) -> Self {
        Self {
            kind: ProgressKind::Source,
            node,
            port,
            time,
            diff,
        }
    }

    /// An in-flight message change at an operator input port.
    #[cfg(test)]
    pub fn target(node: usize, port: usize, time: T, diff: i64) -> Self {
        Self {
            kind: ProgressKind::Target,
            node,
            port,
            time,
            diff,
        }
    }
}

/// One commit of one worker: the root scope's changes, plus one section per
/// nested (loop) scope that changed, each a `Vec<ProgressChange<_>>` of that
/// scope's time. A commit's sections always travel together, so a receiver
/// applies all of them before any scope propagates (design §5.1.7).
#[derive(Clone)]
pub(crate) struct ProgressBatch<T> {
    /// The root scope's changes.
    pub(crate) root: Vec<ProgressChange<T>>,
    /// `(scope id, Vec<ProgressChange<scope time>>)` per nested scope.
    pub(crate) scopes: Vec<(usize, Arc<dyn std::any::Any + Send + Sync>)>,
}

impl<T> ProgressBatch<T> {
    /// A batch of root-scope changes only.
    #[cfg(test)]
    pub(crate) fn flat(root: Vec<ProgressChange<T>>) -> Self {
        Self {
            root,
            scopes: Vec::new(),
        }
    }

    /// Whether the batch changes nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_empty() && self.scopes.is_empty()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ProgressBatch<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressBatch")
            .field("root", &self.root)
            .field(
                "scopes",
                &self.scopes.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Shared buffer between a sender/receiver pair: the lane's queued batches
/// and how the lane ended.
pub(crate) struct SharedBuffer<T> {
    pub(crate) queue: VecDeque<ProgressBatch<T>>,
    /// The sender sealed its stream after the batches queued before it: it
    /// will send nothing more (design §2.2, seal markers).
    pub(crate) sealed: bool,
    /// The lane closed: the sender is gone (in-process: dropped; network:
    /// its end-of-stream arrived or the link ended). With `sealed`, a clean
    /// end; without it, the peer stopped without finishing its part.
    pub(crate) closed: bool,
}

impl<T> SharedBuffer<T> {
    /// An empty, open lane.
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            sealed: false,
            closed: false,
        }
    }
}

/// How a peer's progress lane stands, after the batches drained with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LaneState {
    /// The peer sealed its stream: nothing follows the drained batches.
    pub(crate) sealed: bool,
    /// The lane closed (see [`SharedBuffer::closed`]).
    pub(crate) closed: bool,
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
        send_fn: Box<dyn Fn(ProgressBatch<T>) + Send + Sync>,
        /// Sends the seal marker, in order behind every batch sent before.
        seal_fn: Box<dyn Fn() + Send + Sync>,
        /// Sends the abandon marker (the stream ends without a seal), in
        /// order behind every batch sent before.
        abandon_fn: Box<dyn Fn() + Send + Sync>,
        /// The stream was sealed (so dropping the sender abandons nothing).
        sealed: std::sync::atomic::AtomicBool,
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
    #[cfg(test)]
    pub fn send(&self, changes: Vec<ProgressChange<T>>) {
        self.send_batch(ProgressBatch::flat(changes));
    }

    /// Send one commit (every scope's section) to the peer.
    pub(crate) fn send_batch(&self, changes: ProgressBatch<T>) {
        if changes.is_empty() {
            return;
        }
        match &self.inner {
            SenderInner::Local {
                buffer,
                target_wake,
            } => {
                {
                    let mut buf = match buffer.lock().or_poison("progress channel") {
                        Ok(buf) => buf,
                        Err(_) => {
                            // NOTE: Cannot propagate lock poison here — `ProgressSender::send`
                            // returns `()` and is used as a fire-and-forget notification path.
                            // Poisoned lock means another thread panicked; dropping this batch is
                            // acceptable because the surrounding dataflow will be torn down.
                            return;
                        }
                    };
                    buf.queue.push_back(changes);
                }
                target_wake.notify();
            }
            #[cfg(feature = "transport")]
            SenderInner::Network { send_fn, .. } => {
                send_fn(changes);
            }
        }
    }
}

impl<T: Timestamp> ProgressSender<T> {
    /// Seal this worker's progress stream to the peer: the marker travels in
    /// order behind every batch sent before it, and nothing may follow it.
    pub(crate) fn seal(&self) {
        match &self.inner {
            SenderInner::Local {
                buffer,
                target_wake,
            } => {
                if let Ok(mut buf) = buffer.lock().or_poison("progress channel") {
                    debug_assert!(!buf.sealed, "a progress stream was sealed twice");
                    buf.sealed = true;
                }
                target_wake.notify();
            }
            #[cfg(feature = "transport")]
            SenderInner::Network {
                seal_fn, sealed, ..
            } => {
                sealed.store(true, std::sync::atomic::Ordering::SeqCst);
                seal_fn();
            }
        }
    }
}

impl<T: Timestamp> Drop for ProgressSender<T> {
    fn drop(&mut self) {
        // A lane closes when its sender goes away (the worker finished or
        // stopped). In-process, the shared buffer records it. On the network,
        // a sender that never sealed sends the abandon marker behind its last
        // batch; a sealed one's lane closes with its end-of-stream or the link
        // (the receive bridge records both).
        match &self.inner {
            SenderInner::Local {
                buffer,
                target_wake,
            } => {
                if let Ok(mut buf) = buffer.lock().or_poison("progress channel") {
                    buf.closed = true;
                }
                target_wake.notify();
            }
            #[cfg(feature = "transport")]
            SenderInner::Network {
                abandon_fn, sealed, ..
            } => {
                if !sealed.load(std::sync::atomic::Ordering::SeqCst) {
                    abandon_fn();
                }
            }
        }
    }
}

impl<T: Timestamp> ProgressReceiver<T> {
    /// Create a progress receiver from a shared buffer.
    pub(crate) fn new(buffer: Arc<Mutex<SharedBuffer<T>>>) -> Self {
        Self { buffer }
    }

    /// Drain all queued progress batches from this peer, in FIFO order.
    #[cfg(test)]
    pub(crate) fn drain_all(&self) -> crate::Result<Vec<ProgressBatch<T>>> {
        Ok(self.drain_with_state()?.0)
    }

    /// Drain all queued progress batches from this peer, in FIFO order, with
    /// the lane's state after them: a seal (or close) covers exactly the
    /// batches drained with it, because the sender records it after them.
    pub(crate) fn drain_with_state(&self) -> crate::Result<(Vec<ProgressBatch<T>>, LaneState)> {
        let mut buf = self.buffer.lock().or_poison("progress channel")?;
        let batches = buf.queue.drain(..).collect();
        Ok((
            batches,
            LaneState {
                sealed: buf.sealed,
                closed: buf.closed,
            },
        ))
    }

    /// Returns `true` if there are queued progress updates.
    #[cfg(test)]
    pub fn has_pending(&self) -> crate::Result<bool> {
        let buf = self.buffer.lock().or_poison("progress channel")?;
        Ok(!buf.queue.is_empty())
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
) -> crate::Result<Vec<WorkerProgressChannels<T>>> {
    if wake_handles.len() != num_workers {
        return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
            format!(
                "wake_handles length ({}) must match num_workers ({num_workers})",
                wake_handles.len()
            ),
        )));
    }

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

            let shared = Arc::new(Mutex::new(SharedBuffer::new()));

            let sender = ProgressSender::local(Arc::clone(&shared), wake_handles[dst].clone());
            let receiver = ProgressReceiver::new(shared);

            all_channels[src].senders[dst] = Some(sender);
            all_channels[dst].receivers[src] = Some(receiver);
        }
    }

    Ok(all_channels)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_receive_progress() {
        let wakes: Vec<WakeHandle> = (0..3).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(3, &wakes).unwrap();

        // Worker 0 sends to Worker 1.
        let w0 = &channels[0];
        w0.senders[1]
            .as_ref()
            .unwrap()
            .send(vec![ProgressChange::source(0, 0, 42u64, 1)]);
        w0.senders[1]
            .as_ref()
            .unwrap()
            .send(vec![ProgressChange::source(0, 0, 42u64, -1)]);

        // Worker 1 receives from Worker 0.
        let w1 = &channels[1];
        let batches = w1.receivers[0].as_ref().unwrap().drain_all().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0].root,
            vec![ProgressChange::source(0, 0, 42u64, 1)]
        );
        assert_eq!(
            batches[1].root,
            vec![ProgressChange::source(0, 0, 42u64, -1)]
        );
    }

    #[test]
    fn empty_send_does_not_wake() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes).unwrap();

        // Empty send — should not enqueue.
        channels[0].senders[1].as_ref().unwrap().send(vec![]);

        let batches = channels[1].receivers[0]
            .as_ref()
            .unwrap()
            .drain_all()
            .unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn fifo_ordering_preserved() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes).unwrap();

        let sender = channels[0].senders[1].as_ref().unwrap();
        for i in 0..10 {
            sender.send(vec![ProgressChange::source(0, 0, i as u64, 1)]);
        }

        let batches = channels[1].receivers[0]
            .as_ref()
            .unwrap()
            .drain_all()
            .unwrap();
        assert_eq!(batches.len(), 10);
        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(batch.root[0].time, i as u64);
        }
    }

    #[test]
    fn self_channels_are_none() {
        let wakes: Vec<WakeHandle> = (0..3).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(3, &wakes).unwrap();

        for (i, ch) in channels.iter().enumerate() {
            assert!(ch.senders[i].is_none());
            assert!(ch.receivers[i].is_none());
        }
    }

    #[test]
    fn has_pending_reflects_state() {
        let wakes: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let channels = create_progress_channels::<u64>(2, &wakes).unwrap();

        let recv = channels[1].receivers[0].as_ref().unwrap();
        assert!(!recv.has_pending().unwrap());

        channels[0].senders[1]
            .as_ref()
            .unwrap()
            .send(vec![ProgressChange::source(0, 0, 1u64, 1)]);
        assert!(recv.has_pending().unwrap());

        recv.drain_all().unwrap();
        assert!(!recv.has_pending().unwrap());
    }
}
