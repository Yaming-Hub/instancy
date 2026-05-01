//! Push and Pull traits for channel communication.
//!
//! These traits abstract over the mechanism of sending and receiving
//! batches of envelopes through channels. They are used by both
//! intra-process (bounded queues) and inter-process (network) channels.

use crate::error::Result;
use crate::progress::timestamp::Timestamp;

use super::envelope::Envelope;

/// A trait for pushing messages into a channel.
///
/// Implementors buffer messages and flush them downstream. The `Push` trait
/// is the send half of a channel abstraction.
pub trait Push<T: Timestamp, D, M = ()>: Send {
    /// Push a single envelope into the channel buffer.
    ///
    /// Returns `Ok(())` if the message was accepted (possibly buffered).
    /// Returns `Err` if the channel is closed or at capacity with no room.
    /// Note: on error, the envelope is consumed and lost. Use `try_push()`
    /// when you need to preserve the envelope on failure.
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()>;

    /// Try to push an envelope, returning it back on failure.
    ///
    /// On success, returns `Ok(())`.
    /// On failure, returns `Err((error, envelope))` so the caller can retry
    /// without data loss. Implementors must check capacity/state *before*
    /// consuming the envelope.
    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (crate::error::Error, Envelope<T, D, M>)>;

    /// Flush all buffered messages, ensuring they are visible to the receiver.
    ///
    /// This should be called after a batch of pushes to ensure progress.
    fn flush(&mut self) -> Result<()>;

    /// Close this push endpoint. No more messages will be sent.
    fn close(&mut self);

    /// Returns `true` if this push endpoint has been closed.
    fn is_closed(&self) -> bool;
}

/// A trait for pulling messages from a channel.
///
/// Implementors provide access to incoming envelopes. The `Pull` trait
/// is the receive half of a channel abstraction.
pub trait Pull<T: Timestamp, D, M = ()>: Send {
    /// Try to pull the next envelope from the channel.
    ///
    /// Returns `Some(envelope)` if a message is available.
    /// Returns `None` if no message is currently available (non-blocking).
    fn pull(&mut self) -> Option<Envelope<T, D, M>>;

    /// Pull all available envelopes into the provided buffer.
    ///
    /// Returns the number of envelopes pulled.
    fn drain_into(&mut self, buffer: &mut Vec<Envelope<T, D, M>>) -> usize {
        let mut count = 0;
        while let Some(envelope) = self.pull() {
            buffer.push(envelope);
            count += 1;
        }
        count
    }

    /// Returns `true` if the channel is closed and no more messages will arrive.
    fn is_exhausted(&self) -> bool;
}

/// A paired Push + Pull channel for intra-process communication.
pub struct ChannelPair<T: Timestamp, D, M = ()> {
    /// The send half.
    pub pusher: Box<dyn Push<T, D, M>>,
    /// The receive half.
    pub puller: Box<dyn Pull<T, D, M>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple in-memory channel implementation for testing.
    struct VecPush<T: Timestamp, D, M> {
        buffer: std::sync::Arc<std::sync::Mutex<Vec<Envelope<T, D, M>>>>,
        closed: bool,
    }

    struct VecPull<T: Timestamp, D, M> {
        buffer: std::sync::Arc<std::sync::Mutex<Vec<Envelope<T, D, M>>>>,
        sender_closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl<T: Timestamp, D: Send, M: Send> Push<T, D, M> for VecPush<T, D, M> {
        fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
            if self.closed {
                return Err(crate::error::Error::Custom("channel closed".into()));
            }
            self.buffer.lock().unwrap().push(envelope);
            Ok(())
        }

        fn try_push(
            &mut self,
            envelope: Envelope<T, D, M>,
        ) -> std::result::Result<(), (crate::error::Error, Envelope<T, D, M>)> {
            if self.closed {
                return Err((crate::error::Error::Custom("channel closed".into()), envelope));
            }
            self.buffer.lock().unwrap().push(envelope);
            Ok(())
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

    impl<T: Timestamp, D: Send, M: Send> Pull<T, D, M> for VecPull<T, D, M> {
        fn pull(&mut self) -> Option<Envelope<T, D, M>> {
            let mut buf = self.buffer.lock().unwrap();
            if buf.is_empty() {
                None
            } else {
                Some(buf.remove(0))
            }
        }

        fn is_exhausted(&self) -> bool {
            let is_sender_closed = self
                .sender_closed
                .load(std::sync::atomic::Ordering::Relaxed);
            let is_empty = self.buffer.lock().unwrap().is_empty();
            is_sender_closed && is_empty
        }
    }

    #[test]
    fn push_pull_basic() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sender_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut pusher = VecPush::<u64, i32, ()> {
            buffer: buffer.clone(),
            closed: false,
        };
        let mut puller = VecPull::<u64, i32, ()> {
            buffer: buffer.clone(),
            sender_closed: sender_closed.clone(),
        };

        // Push some data
        pusher.push(Envelope::data(1, vec![10, 20])).unwrap();
        pusher.push(Envelope::data(2, vec![30])).unwrap();
        pusher.flush().unwrap();

        // Pull data
        let msg1 = puller.pull().unwrap();
        assert_eq!(msg1.as_data(), Some((&1u64, &vec![10, 20])));

        let msg2 = puller.pull().unwrap();
        assert_eq!(msg2.as_data(), Some((&2u64, &vec![30])));

        // No more data
        assert!(puller.pull().is_none());
        assert!(!puller.is_exhausted()); // sender not closed

        // Close sender
        pusher.close();
        sender_closed.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(puller.is_exhausted());
    }

    #[test]
    fn push_after_close_fails() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut pusher = VecPush::<u64, i32, ()> {
            buffer,
            closed: false,
        };

        pusher.close();
        assert!(pusher.is_closed());
        assert!(pusher.push(Envelope::data(1, vec![1])).is_err());
    }

    #[test]
    fn drain_into_collects_all() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sender_closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut pusher = VecPush::<u64, String, ()> {
            buffer: buffer.clone(),
            closed: false,
        };
        let mut puller = VecPull::<u64, String, ()> {
            buffer: buffer.clone(),
            sender_closed,
        };

        pusher
            .push(Envelope::data(1, vec!["a".into()]))
            .unwrap();
        pusher
            .push(Envelope::data(2, vec!["b".into()]))
            .unwrap();
        pusher
            .push(Envelope::data(3, vec!["c".into()]))
            .unwrap();

        let mut collected = Vec::new();
        let count = puller.drain_into(&mut collected);
        assert_eq!(count, 3);
        assert_eq!(collected.len(), 3);
    }
}
