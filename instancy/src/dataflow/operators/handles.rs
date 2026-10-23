//! Operator-side input and output handles.
//!
//! Handles provide the operator implementation's view of its inputs and
//! outputs. They abstract over the channel mechanics so operator logic
//! can focus on data processing.
//!
//! # Notification support
//!
//! [`NotifyContext`] provides notification + capability management for operators
//! that buffer data and emit on frontier advance. See the type-level docs for
//! details on the progress-safety contract.

use std::collections::VecDeque;
use std::fmt;

use crate::error::{Error, Result};
use crate::progress::capability::Capability;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::notificator::Notificator;
use crate::progress::operate::ProgressReporter;
use crate::progress::timestamp::Timestamp;

/// Operator-side handle for reading input data.
///
/// An `InputHandle` presents incoming data organized by timestamp.
/// Operators call [`next`](InputHandle::next) to receive the next
/// available batch.
///
/// It is the logic's view of a counted input port: data waiting here is
/// still counted at the input (it holds the input's frontier), and taking a
/// batch with `next` is what consumes it. The handle cannot inject or
/// re-queue data.
pub struct InputHandle<T: Timestamp, D> {
    /// Name of this input (for diagnostics).
    name: String,
    /// Pending batches, stored as (time, data).
    pending: VecDeque<(T, Vec<D>)>,
    /// Whether the input is complete (no more data will arrive).
    exhausted: bool,
    /// The input's frontier in this worker's view.
    frontier: Antichain<T>,
    /// Records taken in the current logic call, not yet committed.
    consumed: ChangeBatch<T>,
    /// The current logic call's justification, and this input's index in it.
    justify: Option<(usize, crate::dataflow::shell::Justify<T>)>,
    /// The progress checker's count of `pending` (set when the handle is
    /// fed by a channel the checker observes).
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    check: Option<crate::progress::check::StoreCheck<T>>,
}

impl<T: Timestamp, D> InputHandle<T, D> {
    /// Create a new input handle.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pending: VecDeque::new(),
            exhausted: false,
            frontier: Antichain::from_elem(T::minimum()),
            consumed: ChangeBatch::new(),
            justify: None,
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            check: None,
        }
    }

    /// Measure this handle's contents for the progress checker, as data at
    /// the input `puller` delivers to.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    pub(crate) fn observe(&mut self, puller: &dyn crate::dataflow::channels::pushpull::Pull<T, D>) {
        self.check = puller.check_port().map(|port| {
            crate::progress::check::StoreCheck::new(
                port,
                crate::progress::check::Store::InputBuffer,
            )
        });
    }

    /// Push a batch of data for the given timestamp.
    /// Used internally by the runtime to feed data into the operator.
    pub(crate) fn push_vec(&mut self, time: T, data: Vec<D>) {
        if !data.is_empty() {
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            if let Some(check) = &self.check {
                check.add(&time, data.len());
            }
            self.pending.push_back((time, data));
        }
    }

    /// Mark this input as exhausted — no more data will arrive.
    pub(crate) fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }

    /// Set the input's frontier (the operator shell does, after each
    /// progress propagation). Returns whether it changed.
    pub(crate) fn set_frontier(&mut self, frontier: Antichain<T>) -> bool {
        if self.frontier == frontier {
            return false;
        }
        self.frontier = frontier;
        true
    }

    /// Attach the justification of this operator's logic calls.
    pub(crate) fn set_justify(
        &mut self,
        input: usize,
        justify: crate::dataflow::shell::Justify<T>,
    ) {
        self.justify = Some((input, justify));
    }

    /// The records taken by the current logic call, for the shell to commit.
    pub(crate) fn take_consumed(&mut self) -> &mut ChangeBatch<T> {
        &mut self.consumed
    }

    /// Takes the next batch without consuming it: the caller (a shell store
    /// that holds consumed data off the ledger, such as a sink's egress)
    /// must redeem the returned credit, with
    /// [`Credit::redeem`](crate::dataflow::shell::Credit::redeem), once the
    /// data is handed over or explicitly discarded.
    pub(crate) fn next_on_credit(&mut self) -> Option<(T, Vec<D>, (T, i64))> {
        let (time, data) = self.pending.pop_front()?;
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        if let Some(check) = &self.check {
            check.remove(&time, data.len());
        }
        let credit = (time.clone(), data.len() as i64);
        Some((time, data, credit))
    }

    /// Get the next available batch of data.
    ///
    /// Returns `Some((time, data))` if a batch is available,
    /// `None` if no data is currently pending. Taking a batch consumes it:
    /// from now on, the operator's logic is responsible for it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(T, Vec<D>)> {
        let batch = self.pending.pop_front();
        if let Some((time, data)) = &batch {
            #[cfg(any(debug_assertions, feature = "progress-check"))]
            if let Some(check) = &self.check {
                check.remove(time, data.len());
            }
            self.consumed.update(time.clone(), data.len() as i64);
            if let Some((input, justify)) = &self.justify {
                justify.consumed(*input, time);
            }
        }
        batch
    }

    /// Check if there are any pending batches.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Number of pending batches.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Whether the input is complete (exhausted and no pending data).
    pub fn is_done(&self) -> bool {
        self.exhausted && self.pending.is_empty()
    }

    /// Whether the input has been marked as exhausted: its channel has
    /// ended, so no more data will arrive. Batches may still be pending.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// The input's frontier: the times at which data may still arrive at
    /// this input (including data already pending here). Empty once no
    /// more data can arrive.
    pub fn frontier(&self) -> &Antichain<T> {
        &self.frontier
    }

    /// Name of this input handle.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T: Timestamp + fmt::Debug, D: fmt::Debug> fmt::Debug for InputHandle<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputHandle")
            .field("name", &self.name)
            .field("pending", &self.pending.len())
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

/// A session for writing output data at a specific timestamp.
///
/// Sessions are created by [`OutputHandle::session`] and provide
/// methods to emit records at the session's timestamp.
pub struct OutputSession<'a, T: Timestamp, D> {
    /// The timestamp for this session.
    time: T,
    /// Reference to the output handle's buffer.
    buffer: &'a mut Vec<(T, Vec<D>)>,
    /// Accumulated records for this session.
    session_data: Vec<D>,
}

impl<'a, T: Timestamp + Clone, D> OutputSession<'a, T, D> {
    /// Emit a single record.
    pub fn give(&mut self, item: D) {
        self.session_data.push(item);
    }

    /// Emit multiple records.
    pub fn give_vec(&mut self, items: &mut Vec<D>) {
        self.session_data.append(items);
    }

    /// Emit records from an iterator.
    pub fn give_iterator(&mut self, iter: impl IntoIterator<Item = D>) {
        self.session_data.extend(iter);
    }

    /// Get the timestamp of this session.
    pub fn time(&self) -> &T {
        &self.time
    }
}

impl<'a, T: Timestamp + Clone, D> Drop for OutputSession<'a, T, D> {
    fn drop(&mut self) {
        if !self.session_data.is_empty() {
            let data = std::mem::take(&mut self.session_data);
            self.buffer.push((self.time.clone(), data));
        }
    }
}

/// Operator-side handle for writing output data.
///
/// An `OutputHandle` collects output batches produced by operator logic.
/// Operators create [`OutputSession`]s at specific timestamps to emit records.
/// After the operator's logic returns, the runtime takes the buffered output
/// and counts it as produced.
pub struct OutputHandle<T: Timestamp, D> {
    /// Name of this output (for diagnostics).
    name: String,
    /// Buffered output batches: (timestamp, data).
    buffer: Vec<(T, Vec<D>)>,
    /// What [`retain`](Self::retain) needs: this output's index, the call's
    /// justification, the output's capability reporter and the executor's
    /// wake handle.
    retain: Option<RetainCtx<T>>,
}

struct RetainCtx<T: Timestamp> {
    output: usize,
    justify: crate::dataflow::shell::Justify<T>,
    reporter: ProgressReporter<T>,
    wake: Option<crate::dataflow::channels::wake::WakeHandle>,
    live: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<T: Timestamp, D> OutputHandle<T, D> {
    /// Create a new output handle.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            buffer: Vec::new(),
            retain: None,
        }
    }

    /// Connect the handle to its operator's capabilities (the shell does).
    pub(crate) fn set_retain(
        &mut self,
        output: usize,
        justify: crate::dataflow::shell::Justify<T>,
        reporter: ProgressReporter<T>,
        wake: Option<crate::dataflow::channels::wake::WakeHandle>,
        live: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        self.retain = Some(RetainCtx {
            output,
            justify,
            reporter,
            wake,
            live,
        });
    }

    /// Keep a capability for this output at `time`, released when the
    /// returned [`Retained`](crate::dataflow::Retained) is dropped.
    ///
    /// Data a closure keeps across calls must be covered by a capability:
    /// output at a time that nothing justifies is late and fails the
    /// operator. While the `Retained` exists, downstream frontiers cannot
    /// pass `time`, and the closure may emit at or after `time` in any later
    /// call; the operator is called again whenever its input frontier
    /// changes, including the call in which it becomes empty (the last
    /// chance to flush). The `Retained` may be dropped on any thread, so
    /// while one is alive its worker waits for that drop instead of
    /// reporting that it can make no further progress: a `Retained` that is
    /// never dropped keeps the dataflow running.
    ///
    /// `time` must be justified in the current call: at or after a
    /// capability the operator held when the call started (another
    /// `Retained`, for example), or at or after the time of a batch the call
    /// took from an input. Otherwise the operator fails once the call
    /// returns.
    ///
    /// Operator closures are `Clone` (one copy per worker, made before any
    /// call) and `Retained` is not, so keep retained work in a type whose
    /// `Clone` starts empty:
    ///
    /// ```ignore
    /// #[derive(Default)]
    /// struct Held(Vec<(Retained<u64>, Vec<u64>)>);
    /// impl Clone for Held {
    ///     fn clone(&self) -> Self {
    ///         Held::default()
    ///     }
    /// }
    ///
    /// stream.unary("hold", {
    ///     let mut held = Held::default();
    ///     move |input, output| {
    ///         let held = &mut held;
    ///         while let Some((time, data)) = input.next() {
    ///             held.0.push((output.retain(time), data));
    ///         }
    ///         let frontier = input.frontier().clone();
    ///         held.0.retain_mut(|(retained, data)| {
    ///             if frontier.less_equal(retained.time()) {
    ///                 return true;
    ///             }
    ///             output.push_vec(*retained.time(), std::mem::take(data));
    ///             false
    ///         });
    ///         Ok(())
    ///     }
    /// })
    /// ```
    pub fn retain(&mut self, time: T) -> crate::dataflow::Retained<T> {
        match &self.retain {
            Some(ctx) => {
                let capability = ctx
                    .justify
                    .acquire(ctx.output, &time, "retain")
                    .ok()
                    .map(|()| Capability::new(time.clone(), ctx.reporter.clone()));
                crate::dataflow::Retained::new(
                    capability,
                    time,
                    ctx.wake.clone(),
                    Some(ctx.live.clone()),
                )
            }
            None => crate::dataflow::Retained::new(None, time, None, None),
        }
    }

    /// Create a session for emitting records at the given timestamp.
    ///
    /// Records emitted through the session are buffered until the
    /// session is dropped, at which point they become a single batch.
    pub fn session(&mut self, time: T) -> OutputSession<'_, T, D>
    where
        T: Clone,
    {
        OutputSession {
            time,
            buffer: &mut self.buffer,
            session_data: Vec::new(),
        }
    }

    /// Directly push a batch of records at a given timestamp.
    pub fn push_vec(&mut self, time: T, data: Vec<D>) {
        if !data.is_empty() {
            self.buffer.push((time, data));
        }
    }

    /// Drain all buffered output batches.
    ///
    /// Returns an iterator over `(time, data)` pairs. This is called
    /// by the runtime after the operator completes a step.
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.buffer.drain(..)
    }

    /// Times of the buffered batches.
    pub(crate) fn buffered_times(&self) -> impl Iterator<Item = &T> {
        self.buffer.iter().map(|(time, _)| time)
    }

    /// Number of buffered batches.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Whether any output has been buffered.
    pub fn has_output(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Name of this output handle.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T: Timestamp + fmt::Debug, D: fmt::Debug> fmt::Debug for OutputHandle<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputHandle")
            .field("name", &self.name)
            .field("buffered", &self.buffer.len())
            .finish()
    }
}

/// A notification that a timestamp is complete.
///
/// When the input frontier advances past a timestamp, any operator
/// holding a capability for that timestamp receives a notification
/// indicating it can finalize work for that time.
#[derive(Debug, Clone, PartialEq)]
pub struct Notification<T: Timestamp> {
    /// The timestamp that has been completed.
    pub time: T,
    /// Number of records processed at this timestamp.
    pub record_count: usize,
}

impl<T: Timestamp> Notification<T> {
    /// Create a new notification.
    pub fn new(time: T, record_count: usize) -> Self {
        Self { time, record_count }
    }
}

/// A handle for reading operator notifications.
///
/// Notifications tell the operator that a particular timestamp is
/// "complete" — the input frontier has moved past it, so no more
/// data at that timestamp will arrive.
pub struct NotificationHandle<T: Timestamp> {
    pending: VecDeque<Notification<T>>,
}

impl<T: Timestamp> NotificationHandle<T> {
    /// Create a new notification handle.
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
        }
    }

    /// Push a notification (used internally by the runtime).
    pub fn push(&mut self, notification: Notification<T>) {
        self.pending.push_back(notification);
    }

    /// Get the next pending notification.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Notification<T>> {
        self.pending.pop_front()
    }

    /// Check if there are pending notifications.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

impl<T: Timestamp> Default for NotificationHandle<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Timestamp + fmt::Debug> fmt::Debug for NotificationHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotificationHandle")
            .field("pending", &self.pending.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// NotifyContext — notification + capability management for operators
// ---------------------------------------------------------------------------

/// Context for operators that buffer data and emit on frontier advance.
///
/// # Why this exists
///
/// In a streaming dataflow, operators sometimes need to buffer incoming data
/// and defer emission until a timestamp is "complete" — meaning no more data
/// can arrive at that timestamp. For example, a word-count operator receives
/// words from multiple upstream workers (via exchange); it must wait until
/// ALL words for timestamp `t` have arrived before emitting the final counts.
///
/// The `NotifyContext` provides two critical capabilities:
///
/// 1. **Notification registration**: The operator calls [`notify_at(time)`](Self::notify_at)
///    to request a callback when the input frontier advances past `time`. The
///    underlying [`Notificator`] fires when all input sources have advanced,
///    meaning no more data at `time` can arrive.
///
/// 2. **Output capability management**: When an operator buffers data at time `t`
///    and plans to emit later, it **must** hold an output capability at `t`.
///    Without this capability, the progress tracking system would see B's output
///    frontier advance past `t` (since B's *input* frontier advanced), and
///    downstream operator C would incorrectly believe time `t` is complete —
///    even though B hasn't emitted its buffered data yet.
///
/// # How capabilities prevent premature frontier advancement
///
/// Consider a pipeline: A → B(notify) → C
///
/// ```text
/// 1. A produces data at time t → B receives and buffers it
/// 2. A exhausts → A's output capability at t drops
/// 3. Progress propagation: without B holding a capability at t,
///    the reachability tracker would propagate A's completion through B
///    to C, making C's input frontier advance past t — BEFORE B emits!
/// 4. With B holding an output capability at t (via notify_at),
///    the tracker sees a pointstamp at (B, output, t) and keeps
///    C's frontier at t until B drops the capability.
/// ```
///
/// # Usage pattern
///
/// ```ignore
/// .unary_notify("aggregate", {
///     let mut stash: HashMap<u64, Vec<D>> = HashMap::new();
///     move |input, output, ctx| {
///         // Phase 1: Buffer input and request notifications
///         while let Some((time, data)) = input.next() {
///             stash.entry(time).or_default().extend(data);
///             ctx.notify_at(time); // holds output capability + registers notification
///         }
///         // Phase 2: Process fired notifications (frontier advanced past these times)
///         while let Some(time) = ctx.next_notification() {
///             if let Some(data) = stash.remove(&time) {
///                 output.push_vec(time, data);
///             }
///             // capability for this time is dropped → downstream can advance
///         }
///         Ok(())
///     }
/// })
/// ```
///
/// # Which times may be requested
///
/// [`notify_at(time)`](Self::notify_at) holds a capability at `time`, so it
/// must be *justified* in the current call (V3): `time` must be at or after
/// a capability the operator held when the call started (including one
/// [`next_notification`](Self::next_notification) dropped during the call,
/// so a notification handler may re-arm or delay the time it is handling),
/// or at or after the time of a batch the call took from its input. The
/// input frontier alone never justifies a request, and neither does a
/// batch that is waiting but not taken. An unjustified request is rejected
/// and fails the operator with
/// [`ProgressError::Unjustified`](crate::error::ProgressError::Unjustified). Use
/// [`try_notify_at`](Self::try_notify_at) to handle the rejection instead.
///
/// # Thread safety
///
/// `NotifyContext` is not `Send` or `Sync` — it borrows the operator's internal
/// state and is only valid for the duration of a single activation.
pub struct NotifyContext<'a, T: Timestamp> {
    /// The notificator that tracks pending/fired notifications.
    /// Updated by the executor with new input frontiers after progress propagation.
    notificator: &'a mut Notificator<T>,

    /// Progress reporter for the operator's output port.
    /// Used to create output capabilities (Capability::new increments the
    /// pointstamp count, preventing downstream frontier advancement).
    reporter: &'a ProgressReporter<T>,

    /// Output capabilities held by this operator, one per buffered timestamp.
    /// Each capability represents a promise that the operator may still produce
    /// data at that time. Dropping a capability (via next_notification or explicitly)
    /// decrements the pointstamp count, allowing downstream frontiers to advance.
    capabilities: &'a mut Vec<Capability<T>>,

    /// The current call's justification set (see
    /// [`Justify`](crate::dataflow::shell::Justify)).
    justify: crate::dataflow::shell::Justify<T>,

    /// First invalid `notify_at` request of this activation, reported as the
    /// activation's error once the operator logic returns.
    error: Option<Error>,
}

impl<'a, T: Timestamp> NotifyContext<'a, T> {
    /// Creates a new `NotifyContext` from operator-internal state.
    ///
    /// Called by the notify operator's logic before invoking the user
    /// closure. The borrows are released when the closure returns.
    /// `justify` is the call's justification set; requests are checked
    /// against it for the operator's output 0.
    pub(crate) fn new(
        notificator: &'a mut Notificator<T>,
        reporter: &'a ProgressReporter<T>,
        capabilities: &'a mut Vec<Capability<T>>,
        justify: crate::dataflow::shell::Justify<T>,
    ) -> Self {
        Self {
            notificator,
            reporter,
            capabilities,
            justify,
            error: None,
        }
    }

    /// Takes the error recorded by an invalid [`notify_at`](Self::notify_at).
    pub(crate) fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    /// Checks that the call justifies a capability at `time` (V3).
    fn check_time(&self, time: &T) -> Result<()> {
        self.justify.check(0, time, "notify_at")
    }

    /// Request notification when the input frontier advances past `time`.
    ///
    /// This does two things:
    ///
    /// 1. **Registers a notification**: The operator will be re-activated when
    ///    all input sources have advanced past `time` (i.e., no more data at
    ///    `time` can arrive). At that point, [`next_notification`](Self::next_notification)
    ///    will return this time.
    ///
    /// 2. **Holds an output capability at `time`**: Creates a `Capability<T>`
    ///    that increments the pointstamp count at (operator, output, time) in
    ///    the progress tracker. This prevents downstream operators from seeing
    ///    their input frontier advance past `time` until the capability is
    ///    dropped (which happens in [`next_notification`](Self::next_notification)).
    ///
    /// Calling `notify_at` multiple times for the same `time` is safe — the
    /// notificator coalesces duplicate requests, and additional capabilities
    /// at the same time are harmless (each will be dropped when the notification fires).
    ///
    /// # When to call this
    ///
    /// Call `notify_at(time)` whenever you buffer data at `time` that you plan
    /// to emit later. Typically called inside the `while let Some((time, data)) = input.next()`
    /// loop.
    ///
    /// # Common Pitfalls
    ///
    /// - **Not consuming notifications**: If you call `notify_at()` but never call
    ///   `next_notification()`, capabilities accumulate and downstream operators
    ///   will stall indefinitely.
    /// - **Unjustified times**: `time` must be at or after a capability held
    ///   when the call started or the time of a batch the call took from its
    ///   input (see the [type-level docs](Self#which-times-may-be-requested)).
    ///   A justified request whose time the frontier has already passed (e.g.
    ///   re-arming the time being handled) fires on the next call. An
    ///   unjustified request is ignored and fails the operator once its logic
    ///   returns; use [`try_notify_at`](Self::try_notify_at) to handle it
    ///   instead.
    pub fn notify_at(&mut self, time: T) {
        if let Err(error) = self.try_notify_at(time) {
            self.error.get_or_insert(error);
        }
    }

    /// Like [`notify_at`](Self::notify_at), but returns an error (and requests
    /// nothing) if this call does not justify `time`, instead of failing the
    /// operator.
    pub fn try_notify_at(&mut self, time: T) -> Result<()> {
        self.check_time(&time)?;
        // Only create a new capability if we don't already hold one at this time.
        // The notificator deduplicates notification requests internally, but without
        // this guard, repeated calls (e.g., once per data item) would create redundant
        // capabilities — each requiring a mutex lock on drop. One capability per time
        // is sufficient to hold the downstream frontier.
        if !self.capabilities.iter().any(|c| c.time() == &time) {
            // Create an output capability at this time.
            // This increments +1 in the ProgressReporter, which the reachability
            // tracker sees as a pointstamp at (operator, output_port, time).
            // Downstream frontiers cannot advance past `time` while this exists.
            let cap = Capability::new(time.clone(), self.reporter.clone());
            self.capabilities.push(cap);
        }

        // Register the notification request with the notificator.
        // When the input frontier advances past `time`, the notificator will
        // mark this as "ready" and the executor will re-activate the operator.
        self.notificator.notify_at(time);
        Ok(())
    }

    /// Consume the next ready notification, if any.
    ///
    /// Returns `Some(time)` when the input frontier has advanced past a
    /// previously-requested timestamp, meaning no more input data at `time`
    /// can arrive. The operator should emit any buffered data for `time` now.
    ///
    /// **Capability lifecycle**: This method drops the output capability for
    /// the returned time. After this call returns, the progress tracker's
    /// pointstamp count at (operator, output, time) is decremented. Once
    /// all capabilities at `time` are gone, downstream frontiers can advance
    /// past `time`.
    ///
    /// If the operator emits data for `time` BEFORE calling this (via
    /// `output.push_vec(time, data)`), the data is pushed into the output
    /// channel in the same activation. The capability drop happens right
    /// after, so downstream sees both the data arrival and the frontier
    /// advancement in the next progress propagation cycle.
    ///
    /// # Known limitation (backpressure)
    ///
    /// If the output channel is full when the operator's activation tries to
    /// flush `pending_output`, data may remain buffered while the capability
    /// has already been dropped. In this case, downstream could observe frontier
    /// advancement before receiving the data. This is a known design limitation
    /// that will be addressed when message-flight accounting (`consumed`/`produced`
    /// in `OperatorProgress`) is implemented. In practice, with the default 1024
    /// envelope channel capacity, this is unlikely to occur.
    ///
    /// Returns `None` when no notifications are ready. More may become ready
    /// in future activations as the frontier continues to advance.
    pub fn next_notification(&mut self) -> Option<T> {
        let fired = self.notificator.next()?;
        let time = fired.into_time();

        // Drop ALL capabilities at this time.
        // There may be multiple if notify_at was called repeatedly for the
        // same time (e.g., data arrived in multiple batches). Each capability
        // drop decrements the pointstamp count by 1.
        self.capabilities.retain(|c| c.time() != &time);

        Some(time)
    }

    /// Whether there are ready notifications waiting to be consumed.
    ///
    /// The executor uses this (via `has_ready_notifications()` on the operator)
    /// to decide whether to re-activate the operator even when no new input
    /// data is available.
    pub fn has_ready(&self) -> bool {
        self.notificator.has_ready()
    }

    /// Number of pending (not yet ready) notification requests.
    ///
    /// These are timestamps where the operator has called `notify_at` but the
    /// input frontier has not yet advanced past them. They will fire when
    /// upstream operators complete work at those timestamps.
    pub fn pending_count(&self) -> usize {
        self.notificator.pending_count()
    }

    /// The current input frontier as seen by the notificator.
    ///
    /// Timestamps in the frontier may still receive new data. Timestamps
    /// NOT in the frontier (and not dominated by any frontier element)
    /// will never receive new data — their notifications have already fired
    /// or will fire immediately on the next `notify_at` call.
    pub fn frontier(&self) -> &Antichain<T> {
        self.notificator.frontier()
    }

    /// Number of output capabilities currently held.
    ///
    /// Each held capability prevents downstream frontier advancement at its
    /// timestamp. A high count may indicate the operator is accumulating
    /// buffered timestamps without processing notifications.
    pub fn held_capabilities_count(&self) -> usize {
        self.capabilities.len()
    }
}

impl<'a, T: Timestamp + fmt::Debug> fmt::Debug for NotifyContext<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotifyContext")
            .field("ready", &self.notificator.ready_count())
            .field("pending", &self.notificator.pending_count())
            .field("capabilities", &self.capabilities.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- InputHandle tests ---

    #[test]
    fn input_handle_empty() {
        let mut handle = InputHandle::<u64, i32>::new("test");
        assert!(!handle.has_pending());
        assert_eq!(handle.pending_count(), 0);
        assert!(handle.next().is_none());
        assert!(!handle.is_exhausted());
        assert!(!handle.is_done());
    }

    #[test]
    fn input_handle_push_and_next() {
        let mut handle = InputHandle::<u64, i32>::new("test");

        handle.push_vec(1, vec![10, 20]);
        handle.push_vec(2, vec![30]);

        assert!(handle.has_pending());
        assert_eq!(handle.pending_count(), 2);

        let (t1, d1) = handle.next().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(d1, vec![10, 20]);

        let (t2, d2) = handle.next().unwrap();
        assert_eq!(t2, 2);
        assert_eq!(d2, vec![30]);

        assert!(handle.next().is_none());
    }

    #[test]
    fn input_handle_empty_batch_ignored() {
        let mut handle = InputHandle::<u64, i32>::new("test");
        handle.push_vec(1, vec![]);
        assert!(!handle.has_pending());
        assert!(handle.next().is_none());
    }

    #[test]
    fn input_handle_exhausted() {
        let mut handle = InputHandle::<u64, i32>::new("test");
        handle.push_vec(1, vec![10]);

        assert!(!handle.is_done());
        handle.mark_exhausted();
        assert!(handle.is_exhausted());
        assert!(!handle.is_done()); // still has pending data

        handle.next();
        assert!(handle.is_done()); // now done
    }

    #[test]
    fn input_handle_name() {
        let handle = InputHandle::<u64, i32>::new("my_input");
        assert_eq!(handle.name(), "my_input");
    }

    // --- OutputHandle tests ---

    #[test]
    fn output_handle_empty() {
        let handle = OutputHandle::<u64, i32>::new("test");
        assert!(!handle.has_output());
        assert_eq!(handle.buffered_count(), 0);
    }

    #[test]
    fn output_handle_session_give() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        {
            let mut session = handle.session(1u64);
            session.give(10);
            session.give(20);
        } // session dropped, data flushed

        assert_eq!(handle.buffered_count(), 1);

        let batches: Vec<_> = handle.drain().collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], (1, vec![10, 20]));
    }

    #[test]
    fn output_handle_session_give_vec() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        {
            let mut session = handle.session(2u64);
            let mut items = vec![1, 2, 3];
            session.give_vec(&mut items);
            assert!(items.is_empty()); // moved out
        }

        let batches: Vec<_> = handle.drain().collect();
        assert_eq!(batches[0], (2, vec![1, 2, 3]));
    }

    #[test]
    fn output_handle_session_give_iterator() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        {
            let mut session = handle.session(3u64);
            session.give_iterator(0..5);
        }

        let batches: Vec<_> = handle.drain().collect();
        assert_eq!(batches[0], (3, vec![0, 1, 2, 3, 4]));
    }

    #[test]
    fn output_handle_empty_session_no_batch() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        {
            let _session = handle.session(1u64);
            // no data given
        }

        assert!(!handle.has_output());
    }

    #[test]
    fn output_handle_multiple_sessions() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        {
            let mut s1 = handle.session(1u64);
            s1.give(10);
        }
        {
            let mut s2 = handle.session(2u64);
            s2.give(20);
            s2.give(30);
        }

        assert_eq!(handle.buffered_count(), 2);

        let batches: Vec<_> = handle.drain().collect();
        assert_eq!(batches[0], (1, vec![10]));
        assert_eq!(batches[1], (2, vec![20, 30]));
    }

    #[test]
    fn output_handle_push_vec_direct() {
        let mut handle = OutputHandle::<u64, i32>::new("test");

        handle.push_vec(1, vec![10, 20]);
        handle.push_vec(2, vec![]); // empty batch ignored
        handle.push_vec(3, vec![30]);

        assert_eq!(handle.buffered_count(), 2);
    }

    #[test]
    fn output_handle_drain_empties_buffer() {
        let mut handle = OutputHandle::<u64, i32>::new("test");
        handle.push_vec(1, vec![10]);

        let _: Vec<_> = handle.drain().collect();
        assert!(!handle.has_output());
        assert_eq!(handle.buffered_count(), 0);
    }

    #[test]
    fn output_handle_name() {
        let handle = OutputHandle::<u64, i32>::new("my_output");
        assert_eq!(handle.name(), "my_output");
    }

    #[test]
    fn output_session_time() {
        let mut handle = OutputHandle::<u64, i32>::new("test");
        let session = handle.session(42u64);
        assert_eq!(*session.time(), 42);
    }

    // --- NotificationHandle tests ---

    #[test]
    fn notification_handle_empty() {
        let mut handle = NotificationHandle::<u64>::new();
        assert!(!handle.has_pending());
        assert!(handle.next().is_none());
    }

    #[test]
    fn notification_handle_push_and_next() {
        let mut handle = NotificationHandle::new();

        handle.push(Notification::new(1u64, 10));
        handle.push(Notification::new(2u64, 5));

        assert!(handle.has_pending());

        let n1 = handle.next().unwrap();
        assert_eq!(n1.time, 1);
        assert_eq!(n1.record_count, 10);

        let n2 = handle.next().unwrap();
        assert_eq!(n2.time, 2);
        assert_eq!(n2.record_count, 5);

        assert!(handle.next().is_none());
    }

    #[test]
    fn notification_default() {
        let handle = NotificationHandle::<u64>::default();
        assert!(!handle.has_pending());
    }
}
