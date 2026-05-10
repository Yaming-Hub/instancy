//! Lock-free SPSC bounded ring buffer.
//!
//! A single-producer, single-consumer bounded channel that uses atomic
//! head/tail indices instead of a mutex. This eliminates lock contention
//! in exchange channels where each (source, target) worker pair has a
//! dedicated channel.
//!
//! # Design
//!
//! The ring buffer has `capacity + 1` slots to distinguish full from empty.
//! The producer owns the `tail` index; the consumer owns the `head` index.
//! Each side caches the other's index to avoid unnecessary atomic loads.
//!
//! All slots are initialized with `UnsafeCell<MaybeUninit<T>>`. The producer
//! writes into a slot and then advances `tail`; the consumer reads from a
//! slot and then advances `head`. The atomic ordering on index updates
//! provides the necessary happens-before relationship.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

use super::envelope::Envelope;
use super::pushpull::{Pull, Push};
use super::wake::WakeHandle;

// ---------------------------------------------------------------------------
// Ring buffer core
// ---------------------------------------------------------------------------

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

// Safety: Slots are only accessed by one side at a time (SPSC invariant).
// The producer writes to slot[tail] before publishing tail; the consumer
// reads from slot[head] after observing head < tail.
unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    /// Number of usable slots (one slot is reserved as sentinel).
    capacity: usize,
    /// Total slot count = capacity + 1.
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
    closed: AtomicBool,
}

impl<T> RingBuffer<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "SPSC capacity must be > 0");
        // Use next power of two for efficient modulo via bitwise AND.
        // Guard against overflow: capacity + 1 must not wrap.
        let cap1 = capacity
            .checked_add(1)
            .expect("SPSC capacity too large");
        let size = cap1.next_power_of_two();
        assert!(
            size > capacity,
            "SPSC ring size overflow: capacity={capacity}, size={size}"
        );
        let slots: Vec<Slot<T>> = (0..size)
            .map(|_| Slot {
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            slots: slots.into_boxed_slice(),
            capacity,
            mask: size - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn wrap(&self, idx: usize) -> usize {
        idx & self.mask
    }

    #[inline]
    fn len_from(&self, head: usize, tail: usize) -> usize {
        tail.wrapping_sub(head)
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let head = *self.head.get_mut();
        let tail = *self.tail.get_mut();
        // Drop any remaining items in the buffer.
        let mut idx = head;
        while idx != tail {
            let slot = self.wrap(idx);
            // Safety: items between head and tail are initialized.
            unsafe {
                let ptr = (*self.slots[slot].value.get()).as_mut_ptr();
                std::ptr::drop_in_place(ptr);
            }
            idx = idx.wrapping_add(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Creates a lock-free SPSC bounded channel pair.
///
/// The channel has a fixed capacity. When the buffer reaches capacity,
/// pushes fail with backpressure. This is a drop-in replacement for
/// [`super::bounded::bounded_channel`] with lower contention.
pub fn spsc_channel<T: Timestamp, D: Send + 'static, M: Send + 'static>(
    capacity: usize,
) -> (SpscPush<T, D, M>, SpscPull<T, D, M>) {
    spsc_channel_with_wake(capacity, None)
}

/// Creates a lock-free SPSC bounded channel pair with a wake handle.
pub fn spsc_channel_with_wake<T: Timestamp, D: Send + 'static, M: Send + 'static>(
    capacity: usize,
    wake: Option<WakeHandle>,
) -> (SpscPush<T, D, M>, SpscPull<T, D, M>) {
    let ring = Arc::new(RingBuffer::<Envelope<T, D, M>>::new(capacity));
    let push = SpscPush {
        ring: Arc::clone(&ring),
        cached_head: 0,
        wake: wake.clone(),
    };
    let pull = SpscPull {
        ring,
        cached_tail: 0,
        wake,
    };
    (push, pull)
}

// ---------------------------------------------------------------------------
// SpscPush
// ---------------------------------------------------------------------------

/// Lock-free single-producer push endpoint.
pub struct SpscPush<T: Timestamp, D, M = ()> {
    ring: Arc<RingBuffer<Envelope<T, D, M>>>,
    /// Cached head index — only refreshed when the buffer appears full.
    cached_head: usize,
    wake: Option<WakeHandle>,
}

// Safety: SpscPush is Send because it is the sole producer.
unsafe impl<T: Timestamp, D: Send, M: Send> Send for SpscPush<T, D, M> {}

impl<T: Timestamp, D: Send + 'static, M: Send + 'static> Push<T, D, M> for SpscPush<T, D, M> {
    fn push(&mut self, envelope: Envelope<T, D, M>) -> Result<()> {
        if self.ring.closed.load(Ordering::Acquire) {
            return Err(Error::ChannelClosed);
        }
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let len = self.ring.len_from(self.cached_head, tail);
        if len >= self.ring.capacity {
            // Refresh cached head.
            self.cached_head = self.ring.head.load(Ordering::Acquire);
            let len = self.ring.len_from(self.cached_head, tail);
            if len >= self.ring.capacity {
                return Err(Error::Backpressure);
            }
        }
        let slot = self.ring.wrap(tail);
        // Safety: we are the sole producer and have verified capacity.
        unsafe {
            (*self.ring.slots[slot].value.get()).write(envelope);
        }
        self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
        Ok(())
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, M>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, M>)> {
        if self.ring.closed.load(Ordering::Acquire) {
            return Err((Error::ChannelClosed, envelope));
        }
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let len = self.ring.len_from(self.cached_head, tail);
        if len >= self.ring.capacity {
            self.cached_head = self.ring.head.load(Ordering::Acquire);
            let len = self.ring.len_from(self.cached_head, tail);
            if len >= self.ring.capacity {
                return Err((Error::Backpressure, envelope));
            }
        }
        let slot = self.ring.wrap(tail);
        unsafe {
            (*self.ring.slots[slot].value.get()).write(envelope);
        }
        self.ring.tail.store(tail.wrapping_add(1), Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(()) // Writes are immediately visible after Release store.
    }

    fn close(&mut self) {
        self.ring.closed.store(true, Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
    }

    fn is_closed(&self) -> bool {
        self.ring.closed.load(Ordering::Acquire)
    }

    fn available_capacity(&self) -> Option<usize> {
        let tail = self.ring.tail.load(Ordering::Relaxed);
        let head = self.ring.head.load(Ordering::Acquire);
        Some(self.ring.capacity.saturating_sub(self.ring.len_from(head, tail)))
    }
}

impl<T: Timestamp, D, M> Drop for SpscPush<T, D, M> {
    fn drop(&mut self) {
        self.ring.closed.store(true, Ordering::Release);
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
    }
}

// ---------------------------------------------------------------------------
// SpscPull
// ---------------------------------------------------------------------------

/// Lock-free single-consumer pull endpoint.
pub struct SpscPull<T: Timestamp, D, M = ()> {
    ring: Arc<RingBuffer<Envelope<T, D, M>>>,
    /// Cached tail index — only refreshed when the buffer appears empty.
    cached_tail: usize,
    wake: Option<WakeHandle>,
}

// Safety: SpscPull is Send because it is the sole consumer.
unsafe impl<T: Timestamp, D: Send, M: Send> Send for SpscPull<T, D, M> {}

impl<T: Timestamp, D: Send + 'static, M: Send + 'static> Pull<T, D, M> for SpscPull<T, D, M> {
    fn pull(&mut self) -> Option<Envelope<T, D, M>> {
        let head = self.ring.head.load(Ordering::Relaxed);
        if head == self.cached_tail {
            // Refresh cached tail.
            self.cached_tail = self.ring.tail.load(Ordering::Acquire);
            if head == self.cached_tail {
                return None;
            }
        }
        let slot = self.ring.wrap(head);
        // Safety: we are the sole consumer and the slot is initialized.
        let value = unsafe { (*self.ring.slots[slot].value.get()).assume_init_read() };
        let was_full = self.ring.len_from(head, self.cached_tail) >= self.ring.capacity;
        self.ring
            .head
            .store(head.wrapping_add(1), Ordering::Release);
        if was_full {
            if let Some(ref wake) = self.wake {
                wake.notify();
            }
        }
        Some(value)
    }

    fn drain_into(&mut self, buffer: &mut Vec<Envelope<T, D, M>>) -> usize {
        // Refresh tail to get latest.
        self.cached_tail = self.ring.tail.load(Ordering::Acquire);
        let head = self.ring.head.load(Ordering::Relaxed);
        let count = self.ring.len_from(head, self.cached_tail);
        if count == 0 {
            return 0;
        }
        let was_full = count >= self.ring.capacity;
        buffer.reserve(count);
        for i in 0..count {
            let slot = self.ring.wrap(head.wrapping_add(i));
            let value = unsafe { (*self.ring.slots[slot].value.get()).assume_init_read() };
            buffer.push(value);
        }
        self.ring
            .head
            .store(head.wrapping_add(count), Ordering::Release);
        if was_full {
            if let Some(ref wake) = self.wake {
                wake.notify();
            }
        }
        count
    }

    fn is_exhausted(&self) -> bool {
        if !self.ring.closed.load(Ordering::Acquire) {
            return false;
        }
        let head = self.ring.head.load(Ordering::Relaxed);
        let tail = self.ring.tail.load(Ordering::Acquire);
        head == tail
    }
}

impl<T: Timestamp, D, M> SpscPush<T, D, M> {
    /// Returns the current number of envelopes in the buffer.
    pub fn len(&self) -> usize {
        let head = self.ring.head.load(Ordering::Acquire);
        let tail = self.ring.tail.load(Ordering::Relaxed);
        self.ring.len_from(head, tail)
    }

    /// Returns true if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the capacity of this channel.
    pub fn capacity(&self) -> usize {
        self.ring.capacity
    }

    /// Set or replace the wake handle.
    pub fn set_wake(&mut self, wake: WakeHandle) {
        self.wake = Some(wake);
    }
}

impl<T: Timestamp, D, M> SpscPull<T, D, M> {
    /// Returns the current number of envelopes available.
    pub fn len(&self) -> usize {
        let head = self.ring.head.load(Ordering::Relaxed);
        let tail = self.ring.tail.load(Ordering::Acquire);
        self.ring.len_from(head, tail)
    }

    /// Returns true if no envelopes are available.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set or replace the wake handle.
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
    fn push_and_pull_single() {
        let (mut push, mut pull) = spsc_channel::<u64, String, ()>(8);
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
        let (_push, mut pull) = spsc_channel::<u64, i32, ()>(4);
        assert!(pull.pull().is_none());
    }

    #[test]
    fn backpressure_when_full() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(2);
        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();

        let result = push.try_push(Envelope::data(0, vec![3]));
        assert!(result.is_err());

        let (_, returned) = result.unwrap_err();
        match returned.payload {
            super::super::envelope::Payload::Data { data, .. } => assert_eq!(data, vec![3]),
            _ => panic!("expected data"),
        }

        pull.pull().unwrap();
        push.push(Envelope::data(0, vec![3])).unwrap();
    }

    #[test]
    fn fifo_ordering() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(8);
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
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(4);
        push.push(Envelope::data(0, vec![1])).unwrap();
        assert!(!pull.is_exhausted());

        push.close();
        assert!(!pull.is_exhausted());

        pull.pull().unwrap();
        assert!(pull.is_exhausted());
    }

    #[test]
    fn push_after_close_fails() {
        let (mut push, _pull) = spsc_channel::<u64, i32, ()>(4);
        push.close();
        let result = push.push(Envelope::data(0, vec![1]));
        assert!(result.is_err());
    }

    #[test]
    fn drain_into_collects_all() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(8);
        for i in 0..4 {
            push.push(Envelope::data(i, vec![i as i32])).unwrap();
        }
        let mut buf = Vec::new();
        let n = pull.drain_into(&mut buf);
        assert_eq!(n, 4);
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn capacity_and_len() {
        let (mut push, pull) = spsc_channel::<u64, i32, ()>(4);
        assert_eq!(push.capacity(), 4);
        assert_eq!(push.len(), 0);
        assert!(push.is_empty());

        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();
        assert_eq!(push.len(), 2);
        assert_eq!(pull.len(), 2);
    }

    #[test]
    fn wraparound_stress() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(4);
        // Push and pull many times to exercise index wrapping.
        for round in 0..1000u64 {
            for i in 0..4 {
                push.push(Envelope::data(round, vec![i])).unwrap();
            }
            assert!(push.try_push(Envelope::data(round, vec![99])).is_err());
            for _ in 0..4 {
                pull.pull().unwrap();
            }
            assert!(pull.pull().is_none());
        }
    }

    #[test]
    fn drop_on_sender_signals_exhaustion() {
        let (push, pull) = spsc_channel::<u64, i32, ()>(4);
        drop(push);
        assert!(pull.is_exhausted());
    }

    #[test]
    fn available_capacity_tracks() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(4);
        assert_eq!(push.available_capacity(), Some(4));
        push.push(Envelope::data(0, vec![1])).unwrap();
        assert_eq!(push.available_capacity(), Some(3));
        push.push(Envelope::data(0, vec![2])).unwrap();
        push.push(Envelope::data(0, vec![3])).unwrap();
        push.push(Envelope::data(0, vec![4])).unwrap();
        assert_eq!(push.available_capacity(), Some(0));
        pull.pull();
        assert_eq!(push.available_capacity(), Some(1));
    }
}
