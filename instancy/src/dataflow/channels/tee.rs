//! Fan-out push adapter that clones data to multiple downstream targets.
//!
//! [`TeePush`] implements [`Push`] by cloning each envelope and distributing
//! copies to all downstream pushers. The last target receives the original
//! envelope (avoiding one clone).
//!
//! On backpressure, `try_push` returns the original envelope and remembers the
//! first undelivered target. Retry that envelope before submitting another one.
//! This preserves order and avoids duplicate delivery without buffering hidden
//! data or manufacturing a placeholder timestamp.

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::channels::pushpull::Push;
use crate::error::{DataflowError, Error, Result};
use crate::progress::timestamp::Timestamp;

/// A fan-out push adapter that clones envelopes to multiple downstream targets.
///
/// Created automatically by the dataflow builder when a [`crate::Pipe`] is cloned
/// (branching). Each clone creates an additional downstream edge, and `TeePush`
/// ensures all targets receive a copy of every envelope.
///
/// # Backpressure handling
///
/// `try_push` can partially deliver before returning backpressure. The caller
/// retains the original envelope and must retry it unchanged until accepted,
/// or close/drop the pusher to abandon delivery. `flush()` cannot substitute for
/// retrying: the caller, not the tee, owns the pending data and its progress.
/// Like other `Push` implementations, the consuming `push()` loses data on
/// failure; use `try_push()` for recoverable backpressure.
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
    /// The caller owns the retry envelope; only the delivery cursor stays here.
    retry_from: Option<usize>,
    closed: bool,
}

impl<T: Timestamp, D: Clone + Send + 'static, M: Clone + Default + Send + 'static>
    TeePush<T, D, M>
{
    /// Create a new `TeePush` distributing to the given targets.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `targets` is empty.
    pub fn new(targets: Vec<Box<dyn Push<T, D, M>>>) -> Result<Self> {
        if targets.is_empty() {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "TeePush requires at least one target".into(),
            )));
        }
        Ok(Self {
            targets,
            retry_from: None,
            closed: false,
        })
    }

    /// Returns the number of downstream targets.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }
}

impl<T: Timestamp, D: Clone + Send + 'static, M: Clone + Default + Send + 'static> Push<T, D, M>
    for TeePush<T, D, M>
{
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }

        if self.retry_from.is_some() {
            return Err(Error::Dataflow(DataflowError::InvalidConfig(
                "retry the pending tee envelope with try_push before submitting another".into(),
            )));
        }
        let last = self.targets.len() - 1;
        for target in &mut self.targets[..last] {
            target.push(envelope.clone())?;
        }
        self.targets[last].push(envelope)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, M>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }

        let last = self.targets.len() - 1;
        for index in self.retry_from.unwrap_or(0)..last {
            if let Err((error, _)) = self.targets[index].try_push(envelope.clone()) {
                self.retry_from = Some(index);
                return Err((error, envelope));
            }
        }
        match self.targets[last].try_push(envelope) {
            Ok(()) => {
                self.retry_from = None;
                Ok(())
            }
            Err(error) => {
                self.retry_from = Some(last);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.retry_from.is_some() {
            return Err(Error::Backpressure);
        }

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
        self.retry_from = None;
        self.closed = true;
        for target in &mut self.targets {
            target.close();
        }
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn set_inflight_reporter(&mut self, reporter: crate::progress::operate::ProgressReporter<T>) {
        // Propagate to every branch. Only exchange branches record; regular
        // branches no-op — so in-flight accounting stays tee-safe.
        for target in &mut self.targets {
            target.set_inflight_reporter(reporter.clone());
        }
    }

    #[cfg(any(debug_assertions, feature = "progress-check"))]
    fn check_port(&self) -> Option<crate::progress::check::Port<T>> {
        // Every branch shares the producing output.
        self.targets.iter().find_map(|target| target.check_port())
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
) -> Result<Option<Box<dyn Push<T, D, M>>>> {
    Ok(match pushers.len() {
        0 => None,
        1 => {
            // SAFETY: match arm guarantees exactly one element in the vec
            Some(pushers.pop().expect("single pusher exists when len is 1"))
        }
        _ => Some(Box::new(TeePush::new(pushers)?)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::bounded::bounded_channel;
    use crate::dataflow::channels::envelope::Envelope;
    use crate::dataflow::channels::pushpull::Pull;
    use std::sync::{Arc, Mutex};

    #[test]
    fn tee_retry_preserves_original_envelope_and_delivers_each_branch_once() {
        for blocked_target in 0..3 {
            let mut pushers = Vec::new();
            let mut pullers = Vec::new();
            for target in 0..3 {
                let (mut push, pull) = bounded_channel::<u64, i32, ()>(1);
                if target == blocked_target {
                    push.push(Envelope::data(11, vec![-1])).unwrap();
                }
                pushers.push(Box::new(push) as Box<dyn Push<u64, i32>>);
                pullers.push(pull);
            }
            let mut tee = TeePush::new(pushers).unwrap();
            let original = Envelope::data(11, vec![1, 2]);
            let (error, returned) = tee.try_push(original.clone()).unwrap_err();
            assert!(matches!(error, Error::Backpressure));
            assert_eq!(
                returned, original,
                "retry must not replace the batch or its time"
            );
            let (error, returned) = tee.try_push(returned).unwrap_err();
            assert!(matches!(error, Error::Backpressure));
            assert_eq!(returned, original);
            assert!(matches!(tee.flush(), Err(Error::Backpressure)));
            assert_eq!(
                pullers[blocked_target].pull().unwrap().as_data(),
                Some((&11, &vec![-1]))
            );
            tee.try_push(returned).unwrap();
            tee.flush().unwrap();
            for pull in &mut pullers {
                assert_eq!(pull.pull(), Some(original.clone()));
                assert!(
                    pull.pull().is_none(),
                    "already-delivered branches must not get retries"
                );
            }
            tee.try_push(Envelope::data(12, vec![3])).unwrap();
            for pull in &mut pullers {
                assert_eq!(pull.pull().unwrap().as_data(), Some((&12, &vec![3])));
                assert!(pull.pull().is_none());
            }
        }
    }

    #[test]
    fn nested_tee_retries_preserve_control_events_without_duplicate_delivery() {
        let (a, mut a_rx) = bounded_channel::<u64, i32, ()>(2);
        let (mut b, mut b_rx) = bounded_channel::<u64, i32, ()>(1);
        let (c, mut c_rx) = bounded_channel::<u64, i32, ()>(2);
        b.push(Envelope::data(11, vec![-1])).unwrap();
        let inner = TeePush::new(vec![Box::new(a), Box::new(b)]).unwrap();
        let mut outer = TeePush::new(vec![Box::new(inner), Box::new(c)]).unwrap();
        let watermark = Envelope::watermark(12);
        let (_, retry) = outer.try_push(watermark.clone()).unwrap_err();
        assert_eq!(retry, watermark);
        assert_eq!(a_rx.pull(), Some(watermark.clone()));
        assert!(c_rx.pull().is_none());
        assert_eq!(b_rx.pull().unwrap().as_data(), Some((&11, &vec![-1])));
        outer.try_push(retry).unwrap();
        assert!(a_rx.pull().is_none());
        assert_eq!(b_rx.pull(), Some(watermark.clone()));
        assert_eq!(c_rx.pull(), Some(watermark));
    }

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
            if let crate::dataflow::channels::envelope::Payload::Data { time, data } =
                envelope.payload
            {
                self.records.lock().unwrap().push((time, data));
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
        let mut tee = TeePush::new(vec![Box::new(target)]).unwrap();

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

        let mut tee = TeePush::new(vec![Box::new(target_a), Box::new(target_b)]).unwrap();
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

        let mut tee = TeePush::new(targets).unwrap();
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

        let mut tee = TeePush::new(vec![Box::new(target_a), Box::new(target_b)]).unwrap();
        assert!(!tee.is_closed());

        tee.close();
        assert!(tee.is_closed());

        // Push after close should error
        let result = tee.push(Envelope::data(0, vec![1]));
        assert!(result.is_err());
    }

    #[test]
    fn tee_or_single_empty() {
        let result = tee_or_single::<u64, i32, ()>(vec![]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn tee_or_single_one() {
        let records = Arc::new(Mutex::new(Vec::new()));
        let target = RecordingPush::<u64, i32>::new(Arc::clone(&records));
        let mut pusher = tee_or_single(vec![Box::new(target) as Box<dyn Push<u64, i32>>])
            .unwrap()
            .unwrap();

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
        let mut pusher = tee_or_single(targets).unwrap().unwrap();

        pusher.push(Envelope::data(0, vec![7, 8])).unwrap();
        assert_eq!(records_a.lock().unwrap()[0], (0, vec![7, 8]));
        assert_eq!(records_b.lock().unwrap()[0], (0, vec![7, 8]));
    }
}
