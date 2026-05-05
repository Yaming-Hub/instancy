//! Bounded in-process channel implementation.
//!
//! Provides a fixed-capacity FIFO channel connecting an upstream operator's
//! output to a downstream operator's input within the same process.
//! When the channel is full, `try_push()` returns the envelope back to the
//! caller for retry (backpressure).
//!
//! # No-serialization transport
//!
//! Data flows through this channel by **value** — envelopes are moved into a
//! `VecDeque` on push and moved out on pull. No serialization, deserialization,
//! or byte-buffer encoding occurs. This is the transport used for all in-process
//! edges (both pipeline and local exchange).
//!
//! Note: exchange routing may still **clone** records when distributing to
//! multiple target workers. The guarantee is no *serialization* overhead —
//! the `Codec` trait is never invoked for in-process channels.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

use super::envelope::Envelope;
use super::pushpull::{Pull, Push};
use super::wake::WakeHandle;

/// Default channel capacity when not specified.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Shared state between push and pull endpoints.
struct SharedState<T: Timestamp, D, M> {
    buffer: VecDeque<Envelope<T, D, M>>,
    capacity: usize,
}

/// Creates a bounded channel pair (push + pull endpoints).
///
/// The channel has a fixed capacity. When the buffer reaches capacity,
/// pushes fail with backpressure (returning the envelope to the caller).
/// When the sender closes, the receiver will eventually see `is_exhausted()`.
pub fn bounded_channel<T: Timestamp, D: Send + 'static, M: Send + 'static>(
    capacity: usize,
) -> (BoundedPush<T, D, M>, BoundedPull<T, D, M>) {
    bounded_channel_with_wake(capacity, None)
}

/// Creates a bounded channel pair with a [`WakeHandle`] for executor notification.
///
/// The `WakeHandle` is notified when:
/// - Data is pushed into the channel (downstream may be runnable)
/// - The sender is closed or dropped (downstream may see exhaustion)
/// - Data is pulled from a full channel (upstream backpressure is relieved)
pub fn bounded_channel_with_wake<T: Timestamp, D: Send + 'static, M: Send + 'static>(
    capacity: usize,
    wake: Option<WakeHandle>,
) -> (BoundedPush<T, D, M>, BoundedPull<T, D, M>) {
    let state = Arc::new(Mutex::new(SharedState {
        buffer: VecDeque::with_capacity(capacity),
        capacity,
    }));
    let closed = Arc::new(AtomicBool::new(false));

    let push = BoundedPush {
        state: Arc::clone(&state),
        closed: Arc::clone(&closed),
        wake: wake.clone(),
    };
    let pull = BoundedPull {
        state,
        closed,
        wake,
    };
    (push, pull)
}

/// Creates a bounded channel pair with the default capacity.
pub fn default_channel<T: Timestamp, D: Send + 'static, M: Send + 'static>()
-> (BoundedPush<T, D, M>, BoundedPull<T, D, M>) {
    bounded_channel(DEFAULT_CHANNEL_CAPACITY)
}

// ---------------------------------------------------------------------------
// BoundedPush
// ---------------------------------------------------------------------------

/// The send half of a bounded in-process channel.
///
/// Single-sender: each channel has exactly one `BoundedPush` endpoint.
/// This matches the dataflow model where one operator output connects to
/// one downstream operator input via a dedicated channel.
pub struct BoundedPush<T: Timestamp, D, M = ()> {
    state: Arc<Mutex<SharedState<T, D, M>>>,
    closed: Arc<AtomicBool>,
    wake: Option<WakeHandle>,
}

impl<T: Timestamp, D: Send + 'static, M: Send + 'static> Push<T, D, M> for BoundedPush<T, D, M> {
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Custom("channel mutex poisoned".into()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::ChannelClosed);
        }
        if state.buffer.len() >= state.capacity {
            return Err(Error::Backpressure);
        }
        state.buffer.push_back(envelope);
        drop(state); // release lock before notify
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
        Ok(())
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, M>)> {
        let mut state = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return Err((Error::Custom("channel mutex poisoned".into()), envelope)),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err((Error::ChannelClosed, envelope));
        }
        if state.buffer.len() >= state.capacity {
            return Err((Error::Backpressure, envelope));
        }
        state.buffer.push_back(envelope);
        drop(state); // release lock before notify
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // No-op: data is immediately visible to the receiver through the shared buffer.
        Ok(())
    }

    fn close(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn available_capacity(&self) -> Option<usize> {
        match self.state.lock() {
            Ok(state) => Some(state.capacity.saturating_sub(state.buffer.len())),
            Err(_) => Some(0), // poisoned → treat as no capacity
        }
    }
}

// ---------------------------------------------------------------------------
// BoundedPull
// ---------------------------------------------------------------------------

/// The receive half of a bounded in-process channel.
pub struct BoundedPull<T: Timestamp, D, M = ()> {
    state: Arc<Mutex<SharedState<T, D, M>>>,
    closed: Arc<AtomicBool>,
    wake: Option<WakeHandle>,
}

/// Auto-close on drop so the receiver sees exhaustion even if the sender
/// is dropped without an explicit `close()` call.
impl<T: Timestamp, D, M> Drop for BoundedPush<T, D, M> {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
    }
}

impl<T: Timestamp, D: Send + 'static, M: Send + 'static> Pull<T, D, M> for BoundedPull<T, D, M> {
    fn pull(&mut self) -> Option<Envelope<T, D, M>> {
        let mut state = self.state.lock().ok()?;
        let was_full = state.buffer.len() >= state.capacity;
        let result = state.buffer.pop_front();
        drop(state); // release lock before notify
        // Notify if we freed capacity — upstream may be blocked on backpressure
        if result.is_some() && was_full {
            if let Some(ref wake) = self.wake {
                wake.notify();
            }
        }
        result
    }

    fn drain_into(&mut self, buffer: &mut Vec<Envelope<T, D, M>>) -> usize {
        let mut state = match self.state.lock() {
            Ok(s) => s,
            Err(_) => return 0,
        };
        let was_full = state.buffer.len() >= state.capacity;
        let count = state.buffer.len();
        buffer.extend(state.buffer.drain(..));
        drop(state); // release lock before notify
        // Notify if we freed capacity
        if count > 0 && was_full {
            if let Some(ref wake) = self.wake {
                wake.notify();
            }
        }
        count
    }

    fn is_exhausted(&self) -> bool {
        // Exhausted means: sender closed AND buffer empty.
        if !self.closed.load(Ordering::Acquire) {
            return false;
        }
        self.state.lock().map_or(true, |s| s.buffer.is_empty())
    }
}

// ---------------------------------------------------------------------------
// BoundedPush/Pull: utility methods
// ---------------------------------------------------------------------------

impl<T: Timestamp, D, M> BoundedPush<T, D, M> {
    /// Returns the current number of envelopes in the buffer.
    pub fn len(&self) -> usize {
        self.state.lock().map_or(0, |s| s.buffer.len())
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of this channel.
    pub fn capacity(&self) -> usize {
        self.state.lock().map_or(0, |s| s.capacity)
    }

    /// Set or replace the wake handle for this channel endpoint.
    pub fn set_wake(&mut self, wake: WakeHandle) {
        self.wake = Some(wake);
    }
}

impl<T: Timestamp, D, M> BoundedPull<T, D, M> {
    /// Returns the current number of envelopes available to pull.
    pub fn len(&self) -> usize {
        self.state.lock().map_or(0, |s| s.buffer.len())
    }

    /// Returns true if no envelopes are available.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set or replace the wake handle for this channel endpoint.
    pub fn set_wake(&mut self, wake: WakeHandle) {
        self.wake = Some(wake);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_pull_single_envelope() {
        let (mut push, mut pull) = bounded_channel::<u64, String, ()>(8);
        push.push(Envelope::data(1, vec!["hello".into()])).unwrap();
        let env = pull.pull().unwrap();
        match env.payload {
            super::super::envelope::Payload::Data { time, data } => {
                assert_eq!(time, 1);
                assert_eq!(data, vec!["hello".to_string()]);
            }
            _ => panic!("expected data payload"),
        }
    }

    #[test]
    fn pull_returns_none_when_empty() {
        let (_push, mut pull) = bounded_channel::<u64, i32, ()>(4);
        assert!(pull.pull().is_none());
    }

    #[test]
    fn backpressure_when_full() {
        let (mut push, mut pull) = bounded_channel::<u64, i32, ()>(2);
        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();

        // Third push should fail (capacity = 2)
        let result = push.try_push(Envelope::data(0, vec![3]));
        assert!(result.is_err());

        // Envelope is returned on failure
        let (_, returned) = result.unwrap_err();
        match returned.payload {
            super::super::envelope::Payload::Data { data, .. } => assert_eq!(data, vec![3]),
            _ => panic!("expected data"),
        }

        // After pulling one, push succeeds
        pull.pull().unwrap();
        push.push(Envelope::data(0, vec![3])).unwrap();
    }

    #[test]
    fn fifo_ordering() {
        let (mut push, mut pull) = bounded_channel::<u64, i32, ()>(8);
        for i in 0..5 {
            push.push(Envelope::data(i, vec![i as i32])).unwrap();
        }
        for i in 0..5 {
            let env = pull.pull().unwrap();
            match env.payload {
                super::super::envelope::Payload::Data { time, .. } => assert_eq!(time, i),
                _ => panic!("expected data"),
            }
        }
    }

    #[test]
    fn close_signals_exhaustion() {
        let (mut push, mut pull) = bounded_channel::<u64, i32, ()>(4);
        push.push(Envelope::data(0, vec![1])).unwrap();
        assert!(!pull.is_exhausted());

        push.close();
        // Not exhausted yet — buffer has data
        assert!(!pull.is_exhausted());

        pull.pull().unwrap();
        // Now exhausted
        assert!(pull.is_exhausted());
    }

    #[test]
    fn push_after_close_fails() {
        let (mut push, _pull) = bounded_channel::<u64, i32, ()>(4);
        push.close();
        let result = push.push(Envelope::data(0, vec![1]));
        assert!(result.is_err());
    }

    #[test]
    fn drain_into_collects_all() {
        let (mut push, mut pull) = bounded_channel::<u64, i32, ()>(8);
        for i in 0..4 {
            push.push(Envelope::data(i, vec![i as i32])).unwrap();
        }
        let mut buf = Vec::new();
        let count = pull.drain_into(&mut buf);
        assert_eq!(count, 4);
        assert_eq!(buf.len(), 4);
        assert!(pull.pull().is_none());
    }

    #[test]
    fn len_and_capacity() {
        let (mut push, pull) = bounded_channel::<u64, i32, ()>(16);
        assert_eq!(push.capacity(), 16);
        assert!(push.is_empty());
        push.push(Envelope::data(0, vec![1])).unwrap();
        assert_eq!(push.len(), 1);
        assert_eq!(pull.len(), 1);
    }

    #[test]
    fn default_channel_uses_default_capacity() {
        let (push, _pull) = default_channel::<u64, i32, ()>();
        assert_eq!(push.capacity(), DEFAULT_CHANNEL_CAPACITY);
    }

    #[test]
    fn thread_safety() {
        use std::thread;

        let (mut push, mut pull) = bounded_channel::<u64, i32, ()>(100);

        let producer = thread::spawn(move || {
            for i in 0..50 {
                push.push(Envelope::data(i, vec![i as i32])).unwrap();
            }
            push.close();
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::new();
            loop {
                if let Some(env) = pull.pull() {
                    match env.payload {
                        super::super::envelope::Payload::Data { data, .. } => {
                            received.extend(data);
                        }
                        _ => {}
                    }
                } else if pull.is_exhausted() {
                    break;
                }
                // Yield to avoid busy spin in test
                thread::yield_now();
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received.len(), 50);
    }
}
