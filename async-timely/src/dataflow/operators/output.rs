//! Output event types and async output stream emission.
//!
//! The output system bridges the dataflow graph back to external consumers.
//! Terminal operators produce [`OutputEvent`]s through [`OutputStream`]s,
//! one per worker in the final execution region.

use std::fmt;
use std::sync::mpsc;

use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

/// An event emitted by a dataflow output stream.
///
/// Output streams produce a sequence of these events. `Data` events carry
/// timestamped batches of results, while `Frontier` events indicate progress.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputEvent<T: Timestamp, D> {
    /// A batch of result records at a given timestamp.
    Data {
        /// The timestamp for this batch.
        time: T,
        /// The result records.
        data: Vec<D>,
    },
    /// The output frontier has advanced past this timestamp.
    /// All future `Data` events will have strictly greater timestamps.
    Frontier(T),
}

impl<T: Timestamp, D> OutputEvent<T, D> {
    /// Create a data event.
    pub fn data(time: T, data: Vec<D>) -> Self {
        Self::Data { time, data }
    }

    /// Create a frontier event.
    pub fn frontier(time: T) -> Self {
        Self::Frontier(time)
    }

    /// Returns true if this is a data event.
    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data { .. })
    }

    /// Returns true if this is a frontier event.
    pub fn is_frontier(&self) -> bool {
        matches!(self, Self::Frontier(_))
    }

    /// Get a reference to the data if this is a data event.
    pub fn as_data(&self) -> Option<(&T, &[D])> {
        match self {
            Self::Data { time, data } => Some((time, data)),
            _ => None,
        }
    }

    /// Get the timestamp of this event.
    pub fn time(&self) -> &T {
        match self {
            Self::Data { time, .. } => time,
            Self::Frontier(time) => time,
        }
    }
}

impl<T: Timestamp + fmt::Display, D> fmt::Display for OutputEvent<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data { time, data } => write!(f, "Data(t={}, n={})", time, data.len()),
            Self::Frontier(time) => write!(f, "Frontier(t={})", time),
        }
    }
}

/// A stream of output events from one worker.
///
/// This is the consumer-facing async stream that bridges the dataflow's
/// internal bounded buffer to an external consumer. There is one
/// `OutputStream` per worker in the final execution region.
pub struct OutputStream<T: Timestamp, D> {
    /// The worker index that produced this stream.
    worker_index: usize,
    /// Receiver for output events.
    receiver: mpsc::Receiver<OutputEvent<T, D>>,
}

impl<T: Timestamp, D> OutputStream<T, D> {
    /// Get the next output event, blocking until one is available.
    ///
    /// Returns `None` when the dataflow has completed and all events
    /// have been consumed.
    pub fn next(&self) -> Option<OutputEvent<T, D>> {
        self.receiver.recv().ok()
    }

    /// Try to get the next output event without blocking.
    ///
    /// Returns `Ok(Some(event))` if an event is available,
    /// `Ok(None)` if no event is ready yet (channel still open),
    /// `Err(Error::ChannelClosed)` if the producer has disconnected.
    pub fn try_next(&self) -> Result<Option<OutputEvent<T, D>>> {
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(Error::ChannelClosed),
        }
    }

    /// The worker index that produced this output stream.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// Consume all remaining events into a Vec.
    /// Blocks until the stream is exhausted.
    pub fn collect_all(&self) -> Vec<OutputEvent<T, D>> {
        let mut events = Vec::new();
        while let Some(event) = self.next() {
            events.push(event);
        }
        events
    }

    /// Iterate over output events.
    pub fn iter(&self) -> OutputStreamIter<'_, T, D> {
        OutputStreamIter { stream: self }
    }
}

impl<T: Timestamp + fmt::Debug, D: fmt::Debug> fmt::Debug for OutputStream<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputStream")
            .field("worker_index", &self.worker_index)
            .finish_non_exhaustive()
    }
}

/// Iterator adapter for OutputStream.
pub struct OutputStreamIter<'a, T: Timestamp, D> {
    stream: &'a OutputStream<T, D>,
}

impl<'a, T: Timestamp, D> Iterator for OutputStreamIter<'a, T, D> {
    type Item = OutputEvent<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.next()
    }
}

/// The sender side of an output stream, used internally by operators.
pub struct OutputSender<T: Timestamp, D> {
    /// The worker index.
    worker_index: usize,
    /// The bounded sender.
    sender: mpsc::SyncSender<OutputEvent<T, D>>,
}

impl<T: Timestamp, D> OutputSender<T, D> {
    /// Send an output event to the consumer.
    ///
    /// Returns `Err(Error::Backpressure)` if the buffer is full,
    /// `Err(Error::ChannelClosed)` if the consumer has been dropped.
    pub fn send(&self, event: OutputEvent<T, D>) -> Result<()> {
        match self.sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Backpressure),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(Error::ChannelClosed),
        }
    }

    /// Send an output event, blocking if the buffer is full.
    pub fn send_blocking(&self, event: OutputEvent<T, D>) -> Result<()> {
        self.sender
            .send(event)
            .map_err(|_| Error::ChannelClosed)
    }

    /// The worker index.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }
}

impl<T: Timestamp, D> Clone for OutputSender<T, D> {
    fn clone(&self) -> Self {
        Self {
            worker_index: self.worker_index,
            sender: self.sender.clone(),
        }
    }
}

impl<T: Timestamp + fmt::Debug, D: fmt::Debug> fmt::Debug for OutputSender<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputSender")
            .field("worker_index", &self.worker_index)
            .finish_non_exhaustive()
    }
}

/// Create a paired output stream and sender with the given buffer capacity.
///
/// The `buffer_size` controls backpressure: when the buffer is full,
/// the operator will see `Error::Backpressure` on send attempts.
pub fn output_pair<T: Timestamp, D>(
    worker_index: usize,
    buffer_size: usize,
) -> (OutputSender<T, D>, OutputStream<T, D>) {
    let (sender, receiver) = mpsc::sync_channel(buffer_size);
    (
        OutputSender {
            worker_index,
            sender,
        },
        OutputStream {
            worker_index,
            receiver,
        },
    )
}

/// Create multiple output stream pairs, one per worker.
pub fn output_pairs<T: Timestamp, D>(
    num_workers: usize,
    buffer_size: usize,
) -> (Vec<OutputSender<T, D>>, Vec<OutputStream<T, D>>) {
    let mut senders = Vec::with_capacity(num_workers);
    let mut streams = Vec::with_capacity(num_workers);

    for i in 0..num_workers {
        let (sender, stream) = output_pair(i, buffer_size);
        senders.push(sender);
        streams.push(stream);
    }

    (senders, streams)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_event_data_creation() {
        let event: OutputEvent<u64, i32> = OutputEvent::data(42, vec![1, 2, 3]);
        assert!(event.is_data());
        assert!(!event.is_frontier());
        assert_eq!(*event.time(), 42);
        assert_eq!(event.as_data(), Some((&42u64, [1, 2, 3].as_slice())));
    }

    #[test]
    fn output_event_frontier_creation() {
        let event: OutputEvent<u64, i32> = OutputEvent::frontier(100);
        assert!(!event.is_data());
        assert!(event.is_frontier());
        assert_eq!(*event.time(), 100);
        assert!(event.as_data().is_none());
    }

    #[test]
    fn output_event_display() {
        let data: OutputEvent<u64, i32> = OutputEvent::data(5, vec![10, 20]);
        assert_eq!(format!("{data}"), "Data(t=5, n=2)");

        let frontier: OutputEvent<u64, i32> = OutputEvent::frontier(10);
        assert_eq!(format!("{frontier}"), "Frontier(t=10)");
    }

    #[test]
    fn output_event_clone_eq() {
        let e1: OutputEvent<u64, i32> = OutputEvent::data(1, vec![10]);
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn output_pair_basic() {
        let (sender, stream) = output_pair::<u64, i32>(0, 16);

        sender
            .send(OutputEvent::data(1, vec![10, 20]))
            .unwrap();
        sender
            .send(OutputEvent::frontier(1))
            .unwrap();
        sender
            .send(OutputEvent::data(2, vec![30]))
            .unwrap();

        let e1 = stream.next().unwrap();
        assert_eq!(e1, OutputEvent::data(1, vec![10, 20]));

        let e2 = stream.next().unwrap();
        assert_eq!(e2, OutputEvent::frontier(1));

        let e3 = stream.next().unwrap();
        assert_eq!(e3, OutputEvent::data(2, vec![30]));
    }

    #[test]
    fn output_stream_exhaustion() {
        let (sender, stream) = output_pair::<u64, i32>(0, 4);

        sender.send(OutputEvent::data(1, vec![1])).unwrap();
        drop(sender);

        // First call returns the pending event
        let e = stream.next().unwrap();
        assert_eq!(e, OutputEvent::data(1, vec![1]));

        // After sender dropped, next returns None
        assert!(stream.next().is_none());
    }

    #[test]
    fn output_stream_try_next() {
        let (sender, stream) = output_pair::<u64, i32>(0, 4);

        // Nothing available yet
        assert_eq!(stream.try_next().unwrap(), None);

        sender.send(OutputEvent::data(1, vec![1])).unwrap();
        let e = stream.try_next().unwrap().unwrap();
        assert_eq!(e, OutputEvent::data(1, vec![1]));

        // Nothing more
        assert_eq!(stream.try_next().unwrap(), None);

        // After sender drops, try_next returns ChannelClosed
        drop(sender);
        let result = stream.try_next();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ChannelClosed));
    }

    #[test]
    fn output_stream_collect_all() {
        let (sender, stream) = output_pair::<u64, i32>(0, 16);

        sender.send(OutputEvent::data(1, vec![10])).unwrap();
        sender.send(OutputEvent::data(2, vec![20])).unwrap();
        sender.send(OutputEvent::frontier(2)).unwrap();
        drop(sender);

        let events = stream.collect_all();
        assert_eq!(events.len(), 3);
        assert!(events[0].is_data());
        assert!(events[1].is_data());
        assert!(events[2].is_frontier());
    }

    #[test]
    fn output_stream_iter() {
        let (sender, stream) = output_pair::<u64, i32>(0, 8);

        sender.send(OutputEvent::data(1, vec![1])).unwrap();
        sender.send(OutputEvent::data(2, vec![2])).unwrap();
        drop(sender);

        let events: Vec<_> = stream.iter().collect();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn output_pairs_multiple_workers() {
        let (senders, streams) = output_pairs::<u64, i32>(3, 4);

        assert_eq!(senders.len(), 3);
        assert_eq!(streams.len(), 3);

        for (i, sender) in senders.iter().enumerate() {
            assert_eq!(sender.worker_index(), i);
            sender
                .send(OutputEvent::data(1, vec![i as i32]))
                .unwrap();
        }

        for (i, stream) in streams.iter().enumerate() {
            assert_eq!(stream.worker_index(), i);
            let e = stream.next().unwrap();
            assert_eq!(e, OutputEvent::data(1, vec![i as i32]));
        }
    }

    #[test]
    fn output_sender_backpressure() {
        let (sender, _stream) = output_pair::<u64, i32>(0, 1);

        // First send succeeds
        sender.send(OutputEvent::data(1, vec![1])).unwrap();

        // Buffer full — backpressure
        let result = sender.send(OutputEvent::data(2, vec![2]));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Backpressure));
    }

    #[test]
    fn output_sender_channel_closed() {
        let (sender, stream) = output_pair::<u64, i32>(0, 4);
        drop(stream);

        let result = sender.send(OutputEvent::data(1, vec![1]));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ChannelClosed));
    }

    #[test]
    fn output_sender_blocking_send() {
        let (sender, stream) = output_pair::<u64, i32>(0, 2);

        sender
            .send_blocking(OutputEvent::data(1, vec![1]))
            .unwrap();
        sender
            .send_blocking(OutputEvent::data(2, vec![2]))
            .unwrap();

        let e1 = stream.next().unwrap();
        assert_eq!(e1, OutputEvent::data(1, vec![1]));
        let e2 = stream.next().unwrap();
        assert_eq!(e2, OutputEvent::data(2, vec![2]));
    }

    #[test]
    fn output_sender_blocking_closed() {
        let (sender, stream) = output_pair::<u64, i32>(0, 4);
        drop(stream);

        let result = sender.send_blocking(OutputEvent::data(1, vec![1]));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ChannelClosed));
    }

    #[test]
    fn output_sender_clone() {
        let (sender, stream) = output_pair::<u64, i32>(0, 4);
        let sender2 = sender.clone();

        sender.send(OutputEvent::data(1, vec![10])).unwrap();
        sender2.send(OutputEvent::data(2, vec![20])).unwrap();

        let e1 = stream.next().unwrap();
        assert_eq!(e1, OutputEvent::data(1, vec![10]));
        let e2 = stream.next().unwrap();
        assert_eq!(e2, OutputEvent::data(2, vec![20]));
    }

    #[test]
    fn output_stream_worker_index() {
        let (_sender, stream) = output_pair::<u64, i32>(5, 4);
        assert_eq!(stream.worker_index(), 5);
    }

    #[test]
    fn output_stream_concurrent_producer_consumer() {
        let (sender, stream) = output_pair::<u64, i32>(0, 64);

        let producer = std::thread::spawn(move || {
            for i in 0..100 {
                sender
                    .send_blocking(OutputEvent::data(i as u64, vec![i]))
                    .unwrap();
            }
            // sender drops, closing the channel
        });

        let mut received = Vec::new();
        while let Some(event) = stream.next() {
            received.push(event);
        }

        producer.join().unwrap();
        assert_eq!(received.len(), 100);
        for (i, event) in received.iter().enumerate() {
            assert_eq!(event, &OutputEvent::data(i as u64, vec![i as i32]));
        }
    }
}
