//! Channel allocator for intra-process communication.
//!
//! Creates bounded in-memory channels between operators within the same process.
//! These channels use a lock-based bounded queue to provide backpressure.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

use crate::dataflow::channels::envelope::Envelope;
use crate::dataflow::channels::pushpull::{ChannelPair, Pull, Push};

/// Default buffer capacity for intra-process channels.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Configuration for the channel allocator.
#[derive(Debug, Clone)]
pub struct AllocatorConfig {
    /// Maximum number of envelopes buffered per channel before backpressure.
    pub channel_capacity: usize,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

/// Allocates intra-process channels for local communication.
///
/// Channels are bounded, lock-based MPSC queues. When a channel is full,
/// the pusher returns `Err(Error::Backpressure)` so the scheduler can
/// re-queue the task.
#[derive(Debug)]
pub struct ChannelAllocator {
    config: AllocatorConfig,
    /// Number of channels allocated.
    allocated_count: usize,
}

impl ChannelAllocator {
    /// Create a new allocator with default configuration.
    pub fn new() -> Self {
        Self {
            config: AllocatorConfig::default(),
            allocated_count: 0,
        }
    }

    /// Create a new allocator with custom configuration.
    pub fn with_config(config: AllocatorConfig) -> Self {
        Self {
            config,
            allocated_count: 0,
        }
    }

    /// Allocate a single channel pair (pusher + puller).
    pub fn allocate<T: Timestamp, D: Send + 'static, M: Send + 'static>(
        &mut self,
    ) -> ChannelPair<T, D, M> {
        let capacity = self.config.channel_capacity;
        self.allocated_count += 1;
        create_local_channel(capacity)
    }

    /// Allocate N channel pairs (e.g., one per worker in a stage).
    pub fn allocate_many<T: Timestamp, D: Send + 'static, M: Send + 'static>(
        &mut self,
        count: usize,
    ) -> Vec<ChannelPair<T, D, M>> {
        (0..count).map(|_| self.allocate()).collect()
    }

    /// The number of channels allocated so far.
    pub fn allocated_count(&self) -> usize {
        self.allocated_count
    }
}

impl Default for ChannelAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a bounded local channel pair.
fn create_local_channel<T: Timestamp, D: Send + 'static, M: Send + 'static>(
    capacity: usize,
) -> ChannelPair<T, D, M> {
    let shared = Arc::new(Mutex::new(ChannelState {
        buffer: VecDeque::with_capacity(4),
        capacity,
        sender_closed: false,
        receiver_dropped: false,
    }));

    let pusher = Box::new(LocalPush {
        shared: shared.clone(),
    });
    let puller = Box::new(LocalPull { shared });

    ChannelPair { pusher, puller }
}

/// Shared state between push and pull halves of a local channel.
#[derive(Debug)]
struct ChannelState<T: Timestamp, D, M> {
    buffer: VecDeque<Envelope<T, D, M>>,
    capacity: usize,
    sender_closed: bool,
    receiver_dropped: bool,
}

/// The push (send) half of a local bounded channel.
struct LocalPush<T: Timestamp, D, M> {
    shared: Arc<Mutex<ChannelState<T, D, M>>>,
}

impl<T: Timestamp, D: Send, M: Send> Push<T, D, M> for LocalPush<T, D, M> {
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| Error::Custom("channel mutex poisoned".into()))?;
        if state.sender_closed || state.receiver_dropped {
            return Err(Error::ChannelClosed);
        }
        if state.buffer.len() >= state.capacity {
            return Err(Error::Backpressure);
        }
        state.buffer.push_back(envelope);
        Ok(())
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, M>)> {
        let mut state = match self.shared.lock() {
            Ok(s) => s,
            Err(_) => return Err((Error::Custom("channel mutex poisoned".into()), envelope)),
        };
        if state.sender_closed || state.receiver_dropped {
            return Err((Error::ChannelClosed, envelope));
        }
        if state.buffer.len() >= state.capacity {
            return Err((Error::Backpressure, envelope));
        }
        state.buffer.push_back(envelope);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // No-op for local channels; data is immediately visible.
        Ok(())
    }

    fn close(&mut self) {
        if let Ok(mut state) = self.shared.lock() {
            state.sender_closed = true;
        }
    }

    fn is_closed(&self) -> bool {
        self.shared.lock().map_or(true, |s| s.sender_closed)
    }
}

impl<T: Timestamp, D, M> Drop for LocalPush<T, D, M> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.lock() {
            state.sender_closed = true;
        }
    }
}

/// The pull (receive) half of a local bounded channel.
struct LocalPull<T: Timestamp, D, M> {
    shared: Arc<Mutex<ChannelState<T, D, M>>>,
}

impl<T: Timestamp, D: Send, M: Send> Pull<T, D, M> for LocalPull<T, D, M> {
    fn pull(&mut self) -> Option<Envelope<T, D, M>> {
        self.shared.lock().ok()?.buffer.pop_front()
    }

    fn is_exhausted(&self) -> bool {
        self.shared
            .lock()
            .map_or(true, |s| s.sender_closed && s.buffer.is_empty())
    }
}

impl<T: Timestamp, D, M> Drop for LocalPull<T, D, M> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.lock() {
            state.receiver_dropped = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::channels::envelope::Envelope;

    #[test]
    fn allocator_creates_channels() {
        let mut alloc = ChannelAllocator::new();
        assert_eq!(alloc.allocated_count(), 0);

        let _pair: ChannelPair<u64, i32, ()> = alloc.allocate();
        assert_eq!(alloc.allocated_count(), 1);

        let pairs: Vec<ChannelPair<u64, i32, ()>> = alloc.allocate_many(3);
        assert_eq!(pairs.len(), 3);
        assert_eq!(alloc.allocated_count(), 4);
    }

    #[test]
    fn local_channel_send_receive() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, ()>();

        // Send some data
        pusher.push(Envelope::data(1, vec![10, 20])).unwrap();
        pusher.push(Envelope::data(2, vec![30])).unwrap();

        // Receive in order
        let msg1 = puller.pull().unwrap();
        assert_eq!(msg1.as_data(), Some((&1u64, &vec![10, 20])));

        let msg2 = puller.pull().unwrap();
        assert_eq!(msg2.as_data(), Some((&2u64, &vec![30])));

        // No more
        assert!(puller.pull().is_none());
    }

    #[test]
    fn local_channel_backpressure() {
        let mut alloc = ChannelAllocator::with_config(AllocatorConfig {
            channel_capacity: 2,
        });
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, ()>();

        // Fill to capacity
        pusher.push(Envelope::data(1, vec![1])).unwrap();
        pusher.push(Envelope::data(2, vec![2])).unwrap();

        // Third push should fail (backpressure)
        let result = pusher.push(Envelope::data(3, vec![3]));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::Error::Backpressure
        ));

        // Drain one, then push should succeed
        let _ = puller.pull();
        pusher.push(Envelope::data(3, vec![3])).unwrap();
    }

    #[test]
    fn local_channel_close_and_exhausted() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, ()>();

        pusher.push(Envelope::data(1, vec![42])).unwrap();
        assert!(!puller.is_exhausted());

        // Close sender
        pusher.close();
        assert!(pusher.is_closed());

        // Push after close fails
        let err = pusher.push(Envelope::data(2, vec![99])).unwrap_err();
        assert!(matches!(err, crate::error::Error::ChannelClosed));

        // Not exhausted yet — there's still data
        assert!(!puller.is_exhausted());

        // Drain the remaining data
        let msg = puller.pull().unwrap();
        assert_eq!(msg.as_data(), Some((&1u64, &vec![42])));

        // Now exhausted
        assert!(puller.is_exhausted());
        assert!(puller.pull().is_none());
    }

    #[test]
    fn local_channel_drop_closes_sender() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair { pusher, puller } = alloc.allocate::<u64, i32, ()>();

        // Drop the pusher without calling close()
        drop(pusher);

        // Receiver should see exhaustion
        assert!(puller.is_exhausted());
    }

    #[test]
    fn local_channel_push_after_receiver_drop() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair { mut pusher, puller } = alloc.allocate::<u64, i32, ()>();

        // Drop receiver
        drop(puller);

        // Push should fail with ChannelClosed
        let err = pusher.push(Envelope::data(1, vec![1])).unwrap_err();
        assert!(matches!(err, crate::error::Error::ChannelClosed));
    }

    #[test]
    fn local_channel_control_signals() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, ()>();

        // Send a mix of data and control
        pusher.push(Envelope::data(1, vec![10])).unwrap();
        pusher.push(Envelope::watermark(5)).unwrap();
        pusher
            .push(Envelope::error("op1", "something failed"))
            .unwrap();

        let msg1 = puller.pull().unwrap();
        assert!(msg1.is_data());

        let msg2 = puller.pull().unwrap();
        assert!(msg2.is_control());
        assert!(matches!(
            msg2.as_control(),
            Some(crate::dataflow::channels::ControlSignal::Watermark(5))
        ));

        let msg3 = puller.pull().unwrap();
        assert!(msg3.is_control());
        assert!(matches!(
            msg3.as_control(),
            Some(crate::dataflow::channels::ControlSignal::Error { .. })
        ));
    }

    #[test]
    fn local_channel_with_metadata() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, String>();

        let env = Envelope::with_metadata(
            crate::dataflow::channels::Payload::Data {
                time: 1u64,
                data: vec![42],
            },
            "sorted_asc".to_string(),
        );
        pusher.push(env).unwrap();

        let msg = puller.pull().unwrap();
        assert_eq!(msg.metadata, "sorted_asc");
        assert!(msg.is_data());
    }

    #[test]
    fn local_channel_drain_into() {
        let mut alloc = ChannelAllocator::new();
        let ChannelPair {
            mut pusher,
            mut puller,
        } = alloc.allocate::<u64, i32, ()>();

        for i in 0..5 {
            pusher.push(Envelope::data(i as u64, vec![i * 10])).unwrap();
        }

        let mut buffer = Vec::new();
        let count = puller.drain_into(&mut buffer);
        assert_eq!(count, 5);
        assert_eq!(buffer.len(), 5);
    }

    #[test]
    fn custom_capacity_config() {
        let config = AllocatorConfig {
            channel_capacity: 16,
        };
        let mut alloc = ChannelAllocator::with_config(config);
        let ChannelPair { mut pusher, puller } = alloc.allocate::<u64, i32, ()>();

        // Keep puller alive so receiver_dropped stays false
        let _puller = puller;

        // Should be able to push 16 items
        for i in 0..16 {
            pusher.push(Envelope::data(i, vec![i as i32])).unwrap();
        }
        // 17th should fail with backpressure
        let err = pusher.push(Envelope::data(16, vec![16])).unwrap_err();
        assert!(matches!(err, crate::error::Error::Backpressure));
    }
}
