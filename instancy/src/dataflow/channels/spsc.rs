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

use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

// Model-checked tests (`--cfg instancy_loom`, Linux) substitute Loom's models
// for the ring's indices, close flag, slots and shared ownership; see
// `spsc_loom`.
#[cfg(all(test, target_os = "linux", instancy_loom))]
use loom::{
    cell::UnsafeCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
};
#[cfg(not(all(test, target_os = "linux", instancy_loom)))]
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

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

/// std's `UnsafeCell` with the closure-based access of Loom's checked cell,
/// which replaces it under `--cfg instancy_loom` and checks each slot access
/// against the other side's.
#[cfg(not(all(test, target_os = "linux", instancy_loom)))]
struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(all(test, target_os = "linux", instancy_loom)))]
impl<T> UnsafeCell<T> {
    fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    #[inline(always)]
    fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    #[inline(always)]
    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
/// The value of an index the caller has exclusive access to.
#[inline]
fn exclusive(index: &mut AtomicUsize) -> usize {
    #[cfg(not(all(test, target_os = "linux", instancy_loom)))]
    {
        *index.get_mut()
    }
    #[cfg(all(test, target_os = "linux", instancy_loom))]
    {
        index.with_mut(|value| *value)
    }
}

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

// The model-checked tests explore the ring only if its indices, flag, slots
// and ownership are Loom's models.
#[cfg(all(test, target_os = "linux", instancy_loom))]
const _: fn(
    &RingBuffer<()>,
) -> (
    &loom::sync::atomic::AtomicUsize,
    &loom::sync::atomic::AtomicUsize,
    &loom::sync::atomic::AtomicBool,
    &loom::cell::UnsafeCell<MaybeUninit<()>>,
) = |ring| (&ring.head, &ring.tail, &ring.closed, &ring.slots[0].value);
#[cfg(all(test, target_os = "linux", instancy_loom))]
const _: fn(&SpscPush<u64, ()>) -> &loom::sync::Arc<RingBuffer<Envelope<u64, ()>>> =
    |push| &push.ring;
#[cfg(all(test, target_os = "linux", instancy_loom))]
const _: fn(&SpscPull<u64, ()>) -> &loom::sync::Arc<RingBuffer<Envelope<u64, ()>>> =
    |pull| &pull.ring;

impl<T> RingBuffer<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "SPSC capacity must be > 0");
        // Use next power of two for efficient modulo via bitwise AND.
        // Guard against overflow: capacity + 1 must not wrap.
        let cap1 = capacity.checked_add(1).expect("SPSC capacity too large");
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
        let head = exclusive(&mut self.head);
        let tail = exclusive(&mut self.tail);
        // Drop any remaining items in the buffer.
        let mut idx = head;
        while idx != tail {
            let slot = self.wrap(idx);
            // Safety: items between head and tail are initialized, and
            // `&mut self` is exclusive.
            self.slots[slot]
                .value
                .with_mut(|ptr| unsafe { std::ptr::drop_in_place((*ptr).as_mut_ptr()) });
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

impl<T: Timestamp, D: Send + 'static, M: Send + 'static>
    crate::dataflow::channels::pushpull::sealed::Sealed for SpscPush<T, D, M>
{
}

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
        self.ring.slots[slot].value.with_mut(|ptr| unsafe {
            (*ptr).write(envelope);
        });
        self.ring
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
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
        // Safety: we are the sole producer and have verified capacity.
        self.ring.slots[slot].value.with_mut(|ptr| unsafe {
            (*ptr).write(envelope);
        });
        self.ring
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
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
        Some(
            self.ring
                .capacity
                .saturating_sub(self.ring.len_from(head, tail)),
        )
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

impl<T: Timestamp, D: Send + 'static, M: Send + 'static>
    crate::dataflow::channels::pushpull::sealed::Sealed for SpscPull<T, D, M>
{
}

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
        let value = self.ring.slots[slot]
            .value
            .with(|ptr| unsafe { (*ptr).assume_init_read() });
        self.ring
            .head
            .store(head.wrapping_add(1), Ordering::Release);
        self.freed();
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
        buffer.reserve(count);
        for i in 0..count {
            let slot = self.ring.wrap(head.wrapping_add(i));
            // Safety: we are the sole consumer and the slot is initialized.
            let value = self.ring.slots[slot]
                .value
                .with(|ptr| unsafe { (*ptr).assume_init_read() });
            buffer.push(value);
        }
        self.ring
            .head
            .store(head.wrapping_add(count), Ordering::Release);
        self.freed();
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
    /// Notify the wake handle, if any, after `head` was advanced: a
    /// producer refused for a full ring may push again.
    ///
    /// Every pull and every nonempty drain notifies, not only one that found
    /// the ring full: the producer may fill the ring after this consumer last
    /// read the tail, so whether it was refused cannot be told here. Such a
    /// wake may be spurious. Without a wake handle nothing is notified.
    fn freed(&self) {
        if let Some(ref wake) = self.wake {
            wake.notify();
        }
    }

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

/// Model-checked tests of the ring under Loom (Linux, `RUSTFLAGS="--cfg
/// instancy_loom"`; see `docs/testing/nextest.md`). One producer and one
/// consumer share a real ring; Loom explores the interleavings and the
/// weak-memory outcomes it distinguishes, without a preemption bound, and
/// checks each slot access against the other side's. Its search tracks only
/// the last access to each atomic, so it can miss a race whose earlier
/// access was followed by the other thread's own access (see
/// `close_behind_an_unread_item`).
#[cfg(all(test, target_os = "linux", instancy_loom))]
mod spsc_loom {
    use super::*;
    use std::sync::atomic::AtomicUsize as Count;

    /// A sequence number and its complement: a torn or misrouted record
    /// does not pass for another.
    type Record = (u64, u64);

    /// Run `body` in every execution Loom distinguishes; return how many
    /// passed. The bounds are set here only.
    fn check(passed: &'static Count, body: impl Fn() + Sync + Send + 'static) -> usize {
        for var in [
            "LOOM_MAX_PREEMPTIONS",
            "LOOM_MAX_BRANCHES",
            "LOOM_MAX_PERMUTATIONS",
            "LOOM_MAX_DURATION",
            "LOOM_CHECKPOINT_FILE",
        ] {
            assert!(
                std::env::var_os(var).is_none(),
                "{var} would change the model's bounds"
            );
        }
        passed.store(0, Ordering::Relaxed);
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = None;
        builder.max_branches = 10_000;
        builder.max_permutations = None;
        builder.max_duration = None;
        builder.check(move || {
            body();
            passed.fetch_add(1, Ordering::Relaxed);
        });
        let executions = passed.load(Ordering::Relaxed);
        println!("{executions} executions, no preemption bound");
        assert!(
            executions > 1,
            "Loom explored more than one execution (one means no race was reversed)"
        );
        executions
    }

    /// The `seq`th item: timestamp `seq`, one record.
    fn item(seq: u64) -> Envelope<u64, Record> {
        Envelope::data(seq, vec![(seq, !seq)])
    }

    #[derive(Clone, Copy)]
    enum Producer {
        Push,
        TryPush,
    }

    #[derive(Clone, Copy)]
    enum Consumer {
        Pull,
        Drain,
    }

    /// Items sent through a ring of one usable slot (two physical slots):
    /// the third reuses the first one's slot.
    const ITEMS: u64 = 3;

    /// Executions in which the producer was refused for a full ring, and in
    /// which the consumer found the ring empty.
    struct Refusals {
        full: Count,
        empty: Count,
    }

    impl Refusals {
        const fn new() -> Self {
            Self {
                full: Count::new(0),
                empty: Count::new(0),
            }
        }
    }

    /// Send `ITEMS` items from a producer thread while the consumer takes
    /// them, retrying on a full or empty ring. No wake handle: its lock
    /// would order the two sides.
    fn transfer(producer: Producer, consumer: Consumer, refusals: &'static Refusals) {
        let (mut push, mut pull) = spsc_channel::<u64, Record, ()>(1);
        let send = loom::thread::spawn(move || {
            let mut refused = false;
            for seq in 0..ITEMS {
                loop {
                    let sent = match producer {
                        Producer::Push => match push.push(item(seq)) {
                            Ok(()) => true,
                            Err(Error::Backpressure) => false,
                            Err(e) => panic!("push failed: {e}"),
                        },
                        Producer::TryPush => match push.try_push(item(seq)) {
                            Ok(()) => true,
                            Err((Error::Backpressure, returned)) => {
                                assert_eq!(returned, item(seq), "a refused item comes back");
                                false
                            }
                            Err((e, _)) => panic!("try_push failed: {e}"),
                        },
                    };
                    if sent {
                        break;
                    }
                    refused = true;
                    loom::thread::yield_now();
                }
            }
            (push, refused)
        });
        let mut received = Vec::new();
        let mut empty = false;
        while (received.len() as u64) < ITEMS {
            let took = match consumer {
                Consumer::Pull => pull.pull().map(|item| received.push(item)).is_some(),
                Consumer::Drain => pull.drain_into(&mut received) > 0,
            };
            if !took {
                empty = true;
                loom::thread::yield_now();
            }
        }
        assert_eq!(
            received,
            (0..ITEMS).map(item).collect::<Vec<_>>(),
            "every item arrives once, in order and intact"
        );
        let (push, refused) = send.join().unwrap();
        drop((push, pull));
        refusals
            .full
            .fetch_add(usize::from(refused), Ordering::Relaxed);
        refusals
            .empty
            .fetch_add(usize::from(empty), Ordering::Relaxed);
    }

    fn explore_transfer(
        producer: Producer,
        consumer: Consumer,
        passed: &'static Count,
        refusals: &'static Refusals,
    ) {
        refusals.full.store(0, Ordering::Relaxed);
        refusals.empty.store(0, Ordering::Relaxed);
        check(passed, move || transfer(producer, consumer, refusals));
        let full = refusals.full.load(Ordering::Relaxed);
        let empty = refusals.empty.load(Ordering::Relaxed);
        println!("refused for a full ring in {full} executions, found it empty in {empty}");
        assert!(full > 0 && empty > 0, "both refusals explored");
    }

    #[test]
    fn push_to_pull() {
        static PASSED: Count = Count::new(0);
        static REFUSALS: Refusals = Refusals::new();
        explore_transfer(Producer::Push, Consumer::Pull, &PASSED, &REFUSALS);
    }

    #[test]
    fn push_to_drain() {
        static PASSED: Count = Count::new(0);
        static REFUSALS: Refusals = Refusals::new();
        explore_transfer(Producer::Push, Consumer::Drain, &PASSED, &REFUSALS);
    }

    #[test]
    fn try_push_to_pull() {
        static PASSED: Count = Count::new(0);
        static REFUSALS: Refusals = Refusals::new();
        explore_transfer(Producer::TryPush, Consumer::Pull, &PASSED, &REFUSALS);
    }

    #[test]
    fn try_push_to_drain() {
        static PASSED: Count = Count::new(0);
        static REFUSALS: Refusals = Refusals::new();
        explore_transfer(Producer::TryPush, Consumer::Drain, &PASSED, &REFUSALS);
    }

    /// The producer sends one item and closes its end (or drops it) while
    /// the consumer, before reading, asks whether the ring is exhausted: it
    /// is not, whether or not the close was seen.
    ///
    /// The consumer observes on the spawned thread: Loom runs the main
    /// thread first, so the observation also follows the close. (With the
    /// roles swapped Loom ran one execution, the observation before the
    /// push: its search keeps only the last access to the flag, and the
    /// producer's own load of it hid the observer's from its store.)
    fn close_behind_an_unread_item(drop_endpoint: bool) {
        let (mut push, pull) = spsc_channel::<u64, Record, ()>(1);
        let observe = loom::thread::spawn(move || {
            let exhausted = pull.is_exhausted();
            (pull, exhausted)
        });
        push.push(item(0)).unwrap();
        let push = if drop_endpoint {
            drop(push);
            None
        } else {
            push.close();
            Some(push)
        };
        let (mut pull, exhausted) = observe.join().unwrap();
        assert!(!exhausted, "an unread item is not exhausted");
        let mut received = Vec::new();
        assert_eq!(pull.drain_into(&mut received), 1);
        assert_eq!(received, vec![item(0)]);
        assert!(pull.is_exhausted(), "closed and read");
        drop(push);
    }

    #[test]
    fn close_behind_an_unread_item_is_not_exhausted() {
        static PASSED: Count = Count::new(0);
        check(&PASSED, || close_behind_an_unread_item(false));
    }

    #[test]
    fn drop_behind_an_unread_item_is_not_exhausted() {
        static PASSED: Count = Count::new(0);
        check(&PASSED, || close_behind_an_unread_item(true));
    }

    /// A record that counts its drops in `drops[seq]`.
    struct Counted {
        seq: usize,
        drops: std::sync::Arc<[loom::sync::atomic::AtomicUsize; 2]>,
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            self.drops[self.seq].fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The consumer drops its end while the producer sends two items into
    /// a ring of two and drops its end: whichever end goes last destroys
    /// the unread items, each exactly once.
    #[test]
    fn unread_items_are_dropped_once() {
        static PASSED: Count = Count::new(0);
        check(&PASSED, || {
            let (mut push, pull) = spsc_channel::<u64, Counted, ()>(2);
            let drops = std::sync::Arc::new([0, 0].map(loom::sync::atomic::AtomicUsize::new));
            let counters = std::sync::Arc::clone(&drops);
            let send = loom::thread::spawn(move || {
                for seq in 0..2 {
                    let record = Counted {
                        seq,
                        drops: std::sync::Arc::clone(&counters),
                    };
                    push.push(Envelope::data(seq as u64, vec![record])).unwrap();
                }
            });
            drop(pull);
            send.join().unwrap();
            for (seq, count) in drops.iter().enumerate() {
                assert_eq!(count.load(Ordering::SeqCst), 1, "item {seq} dropped once");
            }
        });
    }

    /// Counts the wakes of a registered waker.
    struct Wakes(loom::sync::atomic::AtomicUsize);

    impl std::task::Wake for Wakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// What the producer decided after its push was refused.
    #[derive(Clone, Copy, PartialEq)]
    enum Refused {
        /// It was not refused.
        No,
        /// A notification was pending when it checked: it would push again.
        Notified,
        /// No notification was pending: it waits for its waker.
        Waiting,
    }

    /// Executions per producer decision.
    struct Decisions {
        no: Count,
        notified: Count,
        waiting: Count,
    }

    impl Decisions {
        const fn new() -> Self {
            Self {
                no: Count::new(0),
                notified: Count::new(0),
                waiting: Count::new(0),
            }
        }
    }

    /// The producer fills a ring of two while the consumer pulls (or
    /// drains) the item already in it. A producer refused for the full ring
    /// registers its waker and checks for a notification, as an executor
    /// does. If none was pending and its waker was never woken, the ring must
    /// still be full: otherwise it waits for a slot that is free. The wake
    /// handle is the consumer's only: a push does not notify it.
    fn freeing_wakes_a_refused_producer(consumer: Consumer, decisions: &'static Decisions) {
        let (mut push, mut pull) = spsc_channel::<u64, Record, ()>(2);
        let wake = WakeHandle::new();
        assert!(wake.take_notification(), "a new handle starts notified");
        pull.set_wake(wake.clone());
        push.push(item(0)).unwrap();
        let wakes = std::sync::Arc::new(Wakes(loom::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(std::sync::Arc::clone(&wakes));
        let send = loom::thread::spawn(move || {
            push.push(item(1)).unwrap();
            let refused = match push.push(item(2)) {
                Ok(()) => Refused::No,
                Err(Error::Backpressure) => {
                    wake.register_waker(&waker);
                    if wake.take_notification() {
                        Refused::Notified
                    } else {
                        Refused::Waiting
                    }
                }
                Err(e) => panic!("push failed: {e}"),
            };
            (push, refused)
        });
        let took = match consumer {
            Consumer::Pull => usize::from(pull.pull().is_some()),
            Consumer::Drain => pull.drain_into(&mut Vec::new()),
        };
        assert!(took > 0, "the first item was in the ring");
        let (mut push, refused) = send.join().unwrap();
        if refused == Refused::Waiting && wakes.0.load(Ordering::SeqCst) == 0 {
            assert!(
                matches!(push.push(item(2)), Err(Error::Backpressure)),
                "a producer waiting for a slot the consumer freed was not woken"
            );
        }
        drop((push, pull));
        let decision = match refused {
            Refused::No => &decisions.no,
            Refused::Notified => &decisions.notified,
            Refused::Waiting => &decisions.waiting,
        };
        decision.fetch_add(1, Ordering::Relaxed);
    }

    fn explore_wake(consumer: Consumer, passed: &'static Count, decisions: &'static Decisions) {
        for decision in [&decisions.no, &decisions.notified, &decisions.waiting] {
            decision.store(0, Ordering::Relaxed);
        }
        check(passed, move || {
            freeing_wakes_a_refused_producer(consumer, decisions)
        });
        let no = decisions.no.load(Ordering::Relaxed);
        let notified = decisions.notified.load(Ordering::Relaxed);
        let waiting = decisions.waiting.load(Ordering::Relaxed);
        println!("producer not refused in {no}, notified in {notified}, waiting in {waiting}");
        assert!(waiting > 0, "a refused producer waited");
    }

    #[test]
    fn pull_wakes_a_refused_producer() {
        static PASSED: Count = Count::new(0);
        static DECISIONS: Decisions = Decisions::new();
        explore_wake(Consumer::Pull, &PASSED, &DECISIONS);
    }

    #[test]
    fn drain_wakes_a_refused_producer() {
        static PASSED: Count = Count::new(0);
        static DECISIONS: Decisions = Decisions::new();
        explore_wake(Consumer::Drain, &PASSED, &DECISIONS);
    }
}

// Native tests: under `--cfg instancy_loom` the ring's primitives are
// modeled ones, which work only inside a model run.
#[cfg(all(test, not(all(target_os = "linux", instancy_loom))))]
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

    /// A ring holds exactly its capacity, though it has more slots: the
    /// next item is refused and returned unchanged; closing it keeps the
    /// items until they are drained in order.
    #[test]
    fn a_ring_holds_exactly_its_capacity() {
        for capacity in [1usize, 2, 3] {
            let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(capacity);
            assert!(pull.pull().is_none(), "capacity {capacity}");
            assert_eq!(pull.drain_into(&mut Vec::new()), 0, "capacity {capacity}");
            let items: Vec<_> = (0..capacity as u64)
                .map(|i| Envelope::data(i, vec![i as i32]))
                .collect();
            for item in &items {
                push.push(item.clone()).unwrap();
            }
            let extra = Envelope::data(99, vec![99]);
            let Err((Error::Backpressure, returned)) = push.try_push(extra.clone()) else {
                panic!("capacity {capacity}: a full ring refuses");
            };
            assert_eq!(returned, extra, "capacity {capacity}");
            assert!(
                matches!(push.push(extra), Err(Error::Backpressure)),
                "capacity {capacity}"
            );
            push.close();
            assert!(!pull.is_exhausted(), "capacity {capacity}: items remain");
            let mut drained = Vec::new();
            assert_eq!(pull.drain_into(&mut drained), capacity);
            assert_eq!(drained, items, "capacity {capacity}");
            assert!(pull.is_exhausted(), "capacity {capacity}");
        }
    }

    /// A waker that counts its wakes.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A pull that frees a full ring wakes the producer, even when the
    /// consumer last read the tail before the push that filled the ring.
    #[test]
    fn pull_from_a_full_ring_wakes_the_producer() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(2);
        let wake = WakeHandle::new();
        pull.set_wake(wake.clone());
        let wakes = Arc::new(Wakes::default());
        wake.register_waker(&std::task::Waker::from(Arc::clone(&wakes)));
        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();
        pull.pull().unwrap();
        push.push(Envelope::data(0, vec![3])).unwrap();
        assert!(
            matches!(
                push.push(Envelope::data(0, vec![4])),
                Err(Error::Backpressure)
            ),
            "the ring is full"
        );
        wake.take_notification();
        let woken = wakes.0.load(Ordering::SeqCst);
        pull.pull().unwrap();
        assert!(wake.take_notification(), "freeing a full ring notifies");
        assert!(
            wakes.0.load(Ordering::SeqCst) > woken,
            "freeing a full ring wakes the producer"
        );
        push.push(Envelope::data(0, vec![4])).unwrap();
    }

    /// A drain that frees a full ring wakes the producer.
    #[test]
    fn drain_from_a_full_ring_wakes_the_producer() {
        let (mut push, mut pull) = spsc_channel::<u64, i32, ()>(2);
        let wake = WakeHandle::new();
        pull.set_wake(wake.clone());
        let wakes = Arc::new(Wakes::default());
        wake.register_waker(&std::task::Waker::from(Arc::clone(&wakes)));
        push.push(Envelope::data(0, vec![1])).unwrap();
        push.push(Envelope::data(0, vec![2])).unwrap();
        assert!(
            matches!(
                push.push(Envelope::data(0, vec![3])),
                Err(Error::Backpressure)
            ),
            "the ring is full"
        );
        wake.take_notification();
        let woken = wakes.0.load(Ordering::SeqCst);
        assert_eq!(pull.drain_into(&mut Vec::new()), 2);
        assert!(wake.take_notification(), "freeing a full ring notifies");
        assert!(
            wakes.0.load(Ordering::SeqCst) > woken,
            "freeing a full ring wakes the producer"
        );
        push.push(Envelope::data(0, vec![3])).unwrap();
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
