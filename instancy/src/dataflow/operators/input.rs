//! Input event types and stream-driven input binding.
//!
//! The input system bridges external async data sources into the dataflow graph.
//! External data arrives as [`InputEvent`]s through a [`TimestampedInput`] trait,
//! which is consumed by the `from_stream` operator.

use std::fmt;

use crate::progress::timestamp::Timestamp;

/// An event from an external input source.
///
/// Input streams produce a sequence of these events. `Data` events carry
/// timestamped batches of records, while `Frontier` events advance the
/// input frontier (indicating no more data at or before that timestamp).
#[derive(Debug, Clone, PartialEq)]
pub enum InputEvent<T: Timestamp, D> {
    /// A batch of records at a given timestamp.
    Data {
        /// The timestamp for this batch.
        time: T,
        /// The records in this batch.
        data: Vec<D>,
    },
    /// Advance the input frontier past the given timestamp.
    /// After this event, no `Data` events with `time <= frontier` will arrive.
    Frontier(T),
}

impl<T: Timestamp, D> InputEvent<T, D> {
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

    /// Get the timestamp of this event.
    pub fn time(&self) -> &T {
        match self {
            Self::Data { time, .. } => time,
            Self::Frontier(time) => time,
        }
    }
}

impl<T: Timestamp + fmt::Display, D> fmt::Display for InputEvent<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data { time, data } => write!(f, "Data(t={}, n={})", time, data.len()),
            Self::Frontier(time) => write!(f, "Frontier(t={})", time),
        }
    }
}

/// A source of timestamped input events.
///
/// This trait abstracts over any source that produces [`InputEvent`]s.
/// Implementations might wrap:
/// - An async stream (e.g., Kafka consumer, file reader)
/// - A channel receiver
/// - An in-memory iterator (for testing)
///
/// The input system calls `next_event()` to pull events. When `None` is
/// returned, the input is considered complete and all capabilities are dropped.
pub trait TimestampedInput<T: Timestamp, D>: Send {
    /// Get the next input event, or `None` if the input is exhausted.
    ///
    /// This is a synchronous poll — for async sources, the caller
    /// should bridge through a bounded channel.
    fn next_event(&mut self) -> Option<InputEvent<T, D>>;

    /// Returns a human-readable name for this input source.
    fn name(&self) -> &str {
        "unnamed_input"
    }
}

/// A simple input source backed by a `Vec` of events.
///
/// Useful for testing — events are drained in order.
pub struct VecInput<T: Timestamp, D> {
    name: String,
    events: std::collections::VecDeque<InputEvent<T, D>>,
}

impl<T: Timestamp, D> VecInput<T, D> {
    /// Create a new vec-backed input source.
    pub fn new(name: impl Into<String>, events: Vec<InputEvent<T, D>>) -> Self {
        Self {
            name: name.into(),
            events: events.into(),
        }
    }
}

impl<T: Timestamp, D: Send> TimestampedInput<T, D> for VecInput<T, D> {
    fn next_event(&mut self) -> Option<InputEvent<T, D>> {
        self.events.pop_front()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A channel-based input source.
///
/// Events are received from a channel sender. Useful for bridging
/// async streams into the synchronous input system.
pub struct ChannelInput<T: Timestamp, D> {
    name: String,
    receiver: std::sync::mpsc::Receiver<InputEvent<T, D>>,
}

impl<T: Timestamp, D> ChannelInput<T, D> {
    /// Create a new channel input with the given name.
    /// Returns both the input source and the sender for pushing events.
    pub fn new(name: impl Into<String>) -> (Self, std::sync::mpsc::SyncSender<InputEvent<T, D>>) {
        Self::with_capacity(name, 1024)
    }

    /// Create a channel input with a specific buffer capacity.
    pub fn with_capacity(
        name: impl Into<String>,
        capacity: usize,
    ) -> (Self, std::sync::mpsc::SyncSender<InputEvent<T, D>>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
        (
            Self {
                name: name.into(),
                receiver,
            },
            sender,
        )
    }
}

impl<T: Timestamp, D: Send> TimestampedInput<T, D> for ChannelInput<T, D> {
    fn next_event(&mut self) -> Option<InputEvent<T, D>> {
        self.receiver.recv().ok()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_event_data_creation() {
        let event: InputEvent<u64, i32> = InputEvent::data(42, vec![1, 2, 3]);
        assert!(event.is_data());
        assert!(!event.is_frontier());
        assert_eq!(*event.time(), 42);
    }

    #[test]
    fn input_event_frontier_creation() {
        let event: InputEvent<u64, i32> = InputEvent::frontier(100);
        assert!(!event.is_data());
        assert!(event.is_frontier());
        assert_eq!(*event.time(), 100);
    }

    #[test]
    fn input_event_display() {
        let data: InputEvent<u64, i32> = InputEvent::data(5, vec![10, 20, 30]);
        assert_eq!(format!("{data}"), "Data(t=5, n=3)");

        let frontier: InputEvent<u64, i32> = InputEvent::frontier(10);
        assert_eq!(format!("{frontier}"), "Frontier(t=10)");
    }

    #[test]
    fn input_event_clone_eq() {
        let e1: InputEvent<u64, i32> = InputEvent::data(1, vec![10]);
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn vec_input_drains_in_order() {
        let mut input = VecInput::new(
            "test",
            vec![
                InputEvent::data(1, vec![10]),
                InputEvent::data(2, vec![20]),
                InputEvent::frontier(2),
            ],
        );

        assert_eq!(input.name(), "test");

        let e1 = input.next_event().unwrap();
        assert_eq!(*e1.time(), 1);

        let e2 = input.next_event().unwrap();
        assert_eq!(*e2.time(), 2);

        let e3 = input.next_event().unwrap();
        assert!(e3.is_frontier());

        assert!(input.next_event().is_none());
    }

    #[test]
    fn vec_input_empty() {
        let mut input: VecInput<u64, i32> = VecInput::new("empty", vec![]);
        assert!(input.next_event().is_none());
    }

    #[test]
    fn channel_input_receives_events() {
        let (mut input, sender) = ChannelInput::<u64, i32>::new("ch_test");

        // Send events from another thread
        let handle = std::thread::spawn(move || {
            sender.send(InputEvent::data(1, vec![10])).unwrap();
            sender.send(InputEvent::frontier(1)).unwrap();
            // Sender drops here — channel closes
        });

        let e1 = input.next_event().unwrap();
        assert!(e1.is_data());
        assert_eq!(*e1.time(), 1);

        let e2 = input.next_event().unwrap();
        assert!(e2.is_frontier());

        // After sender drops, next_event returns None
        assert!(input.next_event().is_none());

        handle.join().unwrap();
    }

    #[test]
    fn channel_input_with_capacity() {
        let (mut input, sender) = ChannelInput::<u64, String>::with_capacity("bounded", 2);

        sender.send(InputEvent::data(1, vec!["a".into()])).unwrap();
        sender.send(InputEvent::data(2, vec!["b".into()])).unwrap();
        // Buffer is now full (capacity=2)

        let e = input.next_event().unwrap();
        assert_eq!(*e.time(), 1);

        drop(sender);
        let e = input.next_event().unwrap();
        assert_eq!(*e.time(), 2);
        assert!(input.next_event().is_none());
    }

    #[test]
    fn channel_input_name() {
        let (input, _sender) = ChannelInput::<u64, i32>::new("my_source");
        assert_eq!(input.name(), "my_source");
    }
}
