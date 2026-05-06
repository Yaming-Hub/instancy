//! Fan-out push adapter that clones data to multiple downstream targets.
//!
//! [`TeePush`] implements [`Push`] by cloning each envelope and distributing
//! copies to all downstream pushers. The last target receives the original
//! envelope (avoiding one clone).
//!
//! # Backpressure limitation
//!
//! Because `TeePush` distributes to multiple independent targets, atomic
//! `try_push` semantics cannot be guaranteed: if the first target accepts
//! but the second rejects, the data has already been partially delivered.
//! For this reason, `try_push` degrades to `push` semantics — the envelope
//! is always consumed, and any partial-delivery failure returns an error
//! without the envelope. Callers should not rely on retrieving the envelope
//! from `try_push` when using `TeePush`.
//!
//! This is acceptable for the current synchronous execution model where
//! backpressure in bounded channels is unlikely. Async execution (Phase C)
//! will revisit backpressure handling with a proper reserve/commit protocol.

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::channels::pushpull::Push;
use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

/// A fan-out push adapter that clones envelopes to multiple downstream targets.
///
/// Created automatically by the dataflow builder when a [`crate::Pipe`] is cloned
/// (branching). Each clone creates an additional downstream edge, and `TeePush`
/// ensures all targets receive a copy of every envelope.
///
/// # Backpressure handling
///
/// If a downstream target returns an error (e.g., backpressure) after some
/// targets have already received data, `TeePush` stores the envelope internally
/// and retries delivery to remaining targets on the next `push()` or `flush()`
/// call. This avoids both data loss and duplicate delivery.
///
/// # Type parameters
///
/// - `T`: Timestamp type
/// - `D`: Data record type (must be `Clone` for fan-out)
/// - `M`: Metadata type (default `()`, must be `Clone + Default` for fan-out)
pub struct TeePush<
    T: Timestamp,
    D: Clone + Send + 'static,
    M: Clone + Default + Send + 'static = (),
> {
    targets: Vec<Box<dyn Push<T, D, M>>>,
    /// Partially-delivered envelope: (envelope, next_target_index).
    /// When a target returns an error mid-delivery, we store the envelope
    /// and the index of the first undelivered target for retry.
    pending: Option<(Envelope<T, D, M>, usize)>,
    closed: bool,
}

impl<T: Timestamp, D: Clone + Send + 'static, M: Clone + Default + Send + 'static>
    TeePush<T, D, M>
{
    /// Create a new `TeePush` distributing to the given targets.
    ///
    /// # Panics
    ///
    /// Panics if `targets` is empty.
    pub fn new(targets: Vec<Box<dyn Push<T, D, M>>>) -> Self {
        assert!(!targets.is_empty(), "TeePush requires at least one target");
        Self {
            targets,
            pending: None,
            closed: false,
        }
    }

    /// Returns the number of downstream targets.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    /// Drain any pending partial delivery, retrying from where we left off.
    /// Returns Ok(()) if pending was drained (or there was nothing pending).
    fn drain_pending(&mut self) -> Result<()> {
        if let Some((envelope, start_idx)) = self.pending.take() {
            self.deliver(envelope, start_idx)?
        }
        Ok(())
    }

    /// Deliver an envelope to targets[start_idx..], cloning for all but the last.
    /// On partial failure, stores remaining work in `self.pending`.
    fn deliver(&mut self, envelope: Envelope<T, D, M>, start_idx: usize) -> Result<()> {
        let count = self.targets.len();
        // Deliver clones to all targets except the last one in range.
        for i in start_idx..count.saturating_sub(1) {
            if let Err(e) = self.targets[i].push(envelope.clone()) {
                self.pending = Some((envelope, i + 1));
                return Err(e);
            }
        }
        // Deliver original (moved) to the last target.
        let last_idx = count - 1;
        if last_idx >= start_idx {
            self.targets[last_idx].push(envelope)?
        }
        Ok(())
    }
}

impl<T: Timestamp, D: Clone + Send + 'static, M: Clone + Default + Send + 'static> Push<T, D, M>
    for TeePush<T, D, M>
{
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
        if self.closed {
            return Err(Error::Custom("TeePush is closed".into()));
        }

        // First drain any partially-delivered envelope from a previous call.
        self.drain_pending()?;

        // Deliver to all targets.
        self.deliver(envelope, 0)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, M>)> {
        if self.closed {
            return Err((Error::Custom("TeePush is closed".into()), envelope));
        }

        // Drain pending first; if that fails, return the NEW envelope untouched.
        if let Err(e) = self.drain_pending() {
            return Err((e, envelope));
        }

        // Deliver to all targets. On partial failure, the envelope is stored
        // in self.pending for retry on next push/flush — no data is lost.
        // We return a placeholder envelope since the original is consumed
        // (stored in pending or already delivered). Callers should NOT use
        // the returned envelope for retry — TeePush handles retry internally.
        self.deliver(envelope, 0).map_err(|e| {
            (
                e,
                Envelope::with_metadata(
                    crate::dataflow::channels::envelope::Payload::Data {
                        time: T::minimum(),
                        data: vec![],
                    },
                    M::default(),
                ),
            )
        })
    }

    fn flush(&mut self) -> Result<()> {
        // Drain any pending partial delivery first.
        self.drain_pending()?;

        let mut first_error: Option<Error> = None;
        for target in &mut self.targets {
            if let Err(e) = target.flush() {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn close(&mut self) {
        // Best-effort drain of pending data before closing.
        let _ = self.drain_pending();
        self.closed = true;
        for target in &mut self.targets {
            target.close();
        }
    }

    fn is_closed(&self) -> bool {
        self.closed
    }
}

/// Wrap multiple pushers for the same output port into a single pusher.
///
/// - If there is exactly one pusher, returns it directly (no overhead).
/// - If there are multiple, wraps them in a [`TeePush`].
/// - If empty, returns `None`.
pub fn tee_or_single<
    T: Timestamp,
    D: Clone + Send + 'static,
    M: Clone + Default + Send + 'static,
>(
    mut pushers: Vec<Box<dyn Push<T, D, M>>>,
) -> Option<Box<dyn Push<T, D, M>>> {
    match pushers.len() {
        0 => None,
        1 => Some(pushers.pop().unwrap()),
        _ => Some(Box::new(TeePush::new(pushers))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;
    use std::sync::{Arc, Mutex};

    /// A test push target that records everything pushed to it.
    struct RecordingPush<T: Timestamp, D> {
        records: Arc<Mutex<Vec<(T, Vec<D>)>>>,
        closed: bool,
    }

    impl<T: Timestamp, D> RecordingPush<T, D> {
        fn new(records: Arc<Mutex<Vec<(T, Vec<D>)>>>) -> Self {
            Self {
                records,
                closed: false,
            }
        }
    }

    impl<T: Timestamp, D: Send + 'static> Push<T, D> for RecordingPush<T, D> {
        fn push(&mut self, envelope: Envelope<T, D>) -> Result<()> {
            if let Some((time, data)) = envelope.as_data() {
                // We need to move data out of envelope, so we match the payload
                let _ = (time, data); // just to reference
            }
            match envelope.payload {
                crate::dataflow::channels::envelope::Payload::Data { time, data } => {
                    self.records.lock().unwrap().push((time, data));
                }
                _ => {}
            }
            Ok(())
        }

        fn try_push(
            &mut self,
            envelope: Envelope<T, D>,
        ) -> std::result::Result<(), (Error, Envelope<T, D>)> {
            self.push(envelope)
                .map_err(|e| (e, Envelope::data(T::minimum(), vec![])))
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn close(&mut self) {
            self.closed = true;
        }

        fn is_closed(&self) -> bool {
            self.closed
        }
    }

    #[test]
    fn tee_push_single_target() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let target = RecordingPush::<u64, i32>::new(Arc::clone(&records));
        let mut tee = TeePush::new(vec![Box::new(target)]);

        tee.push(Envelope::data(0, vec![1, 2, 3])).unwrap();
        tee.push(Envelope::data(1, vec![4, 5])).unwrap();

        let r = records.lock().unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], (0, vec![1, 2, 3]));
        assert_eq!(r[1], (1, vec![4, 5]));
    }

    #[test]
    fn tee_push_two_targets() {
        let records_a = Arc::new(Mutex::new(Vec::new()));
        let records_b = Arc::new(Mutex::new(Vec::new()));
        let target_a = RecordingPush::<u64, i32>::new(Arc::clone(&records_a));
        let target_b = RecordingPush::<u64, i32>::new(Arc::clone(&records_b));

        let mut tee = TeePush::new(vec![Box::new(target_a), Box::new(target_b)]);
        assert_eq!(tee.target_count(), 2);

        tee.push(Envelope::data(0, vec![10, 20])).unwrap();
        tee.push(Envelope::data(1, vec![30])).unwrap();

        let a = records_a.lock().unwrap();
        let b = records_b.lock().unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 2);
        assert_eq!(a[0], (0, vec![10, 20]));
        assert_eq!(a[1], (1, vec![30]));
        assert_eq!(b[0], (0, vec![10, 20]));
        assert_eq!(b[1], (1, vec![30]));
    }

    #[test]
    fn tee_push_three_targets() {
        let records: Vec<Arc<Mutex<Vec<(u64, Vec<i32>)>>>> =
            (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let targets: Vec<Box<dyn Push<u64, i32>>> = records
            .iter()
            .map(|r| Box::new(RecordingPush::new(Arc::clone(r))) as Box<dyn Push<u64, i32>>)
            .collect();

        let mut tee = TeePush::new(targets);
        tee.push(Envelope::data(5, vec![100])).unwrap();

        for r in &records {
            let data = r.lock().unwrap();
            assert_eq!(data.len(), 1);
            assert_eq!(data[0], (5, vec![100]));
        }
    }

    #[test]
    fn tee_push_close_propagates() {
        let records_a = Arc::new(Mutex::new(Vec::new()));
        let records_b = Arc::new(Mutex::new(Vec::new()));
        let target_a = RecordingPush::<u64, i32>::new(Arc::clone(&records_a));
        let target_b = RecordingPush::<u64, i32>::new(Arc::clone(&records_b));

        let mut tee = TeePush::new(vec![Box::new(target_a), Box::new(target_b)]);
        assert!(!tee.is_closed());

        tee.close();
        assert!(tee.is_closed());

        // Push after close should error
        let result = tee.push(Envelope::data(0, vec![1]));
        assert!(result.is_err());
    }

    #[test]
    fn tee_or_single_empty() {
        let result = tee_or_single::<u64, i32, ()>(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn tee_or_single_one() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let target = RecordingPush::<u64, i32>::new(Arc::clone(&records));
        let mut pusher = tee_or_single(vec![Box::new(target) as Box<dyn Push<u64, i32>>]).unwrap();

        pusher.push(Envelope::data(0, vec![42])).unwrap();
        assert_eq!(records.lock().unwrap().len(), 1);
    }

    #[test]
    fn tee_or_single_multiple() {
        let records_a = Arc::new(Mutex::new(Vec::new()));
        let records_b = Arc::new(Mutex::new(Vec::new()));
        let targets: Vec<Box<dyn Push<u64, i32>>> = vec![
            Box::new(RecordingPush::new(Arc::clone(&records_a))),
            Box::new(RecordingPush::new(Arc::clone(&records_b))),
        ];
        let mut pusher = tee_or_single(targets).unwrap();

        pusher.push(Envelope::data(0, vec![7, 8])).unwrap();
        assert_eq!(records_a.lock().unwrap()[0], (0, vec![7, 8]));
        assert_eq!(records_b.lock().unwrap()[0], (0, vec![7, 8]));
    }
}
