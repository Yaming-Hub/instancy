//! Operator-side input and output handles.
//!
//! Handles provide the operator implementation's view of its inputs and
//! outputs. They abstract over the channel mechanics so operator logic
//! can focus on data processing.

use std::collections::VecDeque;
use std::fmt;

use crate::progress::timestamp::Timestamp;

/// Operator-side handle for reading input data.
///
/// An `InputHandle` presents incoming data organized by timestamp.
/// Operators call [`next`](InputHandle::next) to receive the next
/// available batch.
pub struct InputHandle<T: Timestamp, D> {
    /// Name of this input (for diagnostics).
    name: String,
    /// Pending batches, stored as (time, data).
    pending: VecDeque<(T, Vec<D>)>,
    /// Whether the input is complete (no more data will arrive).
    exhausted: bool,
}

impl<T: Timestamp, D> InputHandle<T, D> {
    /// Create a new input handle.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pending: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Push a batch of data for the given timestamp.
    /// Used internally by the runtime to feed data into the operator.
    pub fn push_vec(&mut self, time: T, data: Vec<D>) {
        if !data.is_empty() {
            self.pending.push_back((time, data));
        }
    }

    /// Mark this input as exhausted — no more data will arrive.
    pub fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }

    /// Get the next available batch of data.
    ///
    /// Returns `Some((time, data))` if a batch is available,
    /// `None` if no data is currently pending.
    pub fn next(&mut self) -> Option<(T, Vec<D>)> {
        self.pending.pop_front()
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

    /// Whether the input has been marked as exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
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
/// After the operator completes, the runtime drains the buffered output.
pub struct OutputHandle<T: Timestamp, D> {
    /// Name of this output (for diagnostics).
    name: String,
    /// Buffered output batches: (timestamp, data).
    buffer: Vec<(T, Vec<D>)>,
}

impl<T: Timestamp, D> OutputHandle<T, D> {
    /// Create a new output handle.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            buffer: Vec::new(),
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
    pub fn drain(&mut self) -> impl Iterator<Item = (T, Vec<D>)> + '_ {
        self.buffer.drain(..)
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
        handle.push_vec(2, vec![]);  // empty batch ignored
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
