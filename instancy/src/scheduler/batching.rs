//! Time-bounded message batching for operator activation coalescing.
//!
//! When many data messages arrive for a target operator, scheduling an activation
//! per message can dominate overhead. This module provides [`BatchingPolicy`] and
//! [`BatchAccumulator`] to coalesce messages and dispatch activations only when a
//! threshold is met (count, byte size, or elapsed time).
//!
//! # Example
//!
//! ```
//! use instancy::scheduler::batching::{BatchingPolicy, BatchAccumulator};
//! use std::time::Duration;
//!
//! let policy = BatchingPolicy::default(); // 1024 msgs, 64KB, 1ms
//! let mut acc = BatchAccumulator::new();
//!
//! for _ in 0..1024 {
//!     acc.record_message(None); // no size info
//! }
//! assert!(acc.should_dispatch(&policy));
//! ```

use std::time::{Duration, Instant};

/// Policy controlling when accumulated messages trigger an operator activation.
///
/// A dispatch is triggered when **any** threshold is met (first-threshold-wins).
#[derive(Debug, Clone)]
pub struct BatchingPolicy {
    /// Maximum number of messages before dispatch. Default: 1024.
    pub max_batch_count: usize,
    /// Maximum cumulative byte size before dispatch.
    /// Only effective when messages implement [`MessageSize`].
    /// `None` means size-based batching is disabled.
    pub max_batch_bytes: Option<usize>,
    /// Maximum time since the first message in the current batch.
    /// Guarantees bounded latency under low throughput. Default: 1ms.
    pub max_batch_wait: Duration,
}

impl Default for BatchingPolicy {
    fn default() -> Self {
        Self {
            max_batch_count: 1024,
            max_batch_bytes: Some(64 * 1024), // 64 KB
            max_batch_wait: Duration::from_millis(1),
        }
    }
}

impl BatchingPolicy {
    /// Create a policy that disables batching (dispatch every message immediately).
    pub fn no_batching() -> Self {
        Self {
            max_batch_count: 1,
            max_batch_bytes: None,
            max_batch_wait: Duration::ZERO,
        }
    }

    /// Create a policy with only count-based batching (time threshold disabled).
    pub fn count_only(max_count: usize) -> Self {
        Self {
            max_batch_count: max_count,
            max_batch_bytes: None,
            max_batch_wait: Duration::MAX,
        }
    }

    /// Create a policy with custom parameters.
    ///
    /// # Panics
    ///
    /// Panics if `max_count` is zero.
    pub fn custom(
        max_count: usize,
        max_bytes: Option<usize>,
        max_wait: Duration,
    ) -> Self {
        assert!(max_count > 0, "max_batch_count must be positive");
        Self {
            max_batch_count: max_count,
            max_batch_bytes: max_bytes,
            max_batch_wait: max_wait,
        }
    }
}

/// Trait for messages that can report their serialized/in-memory size.
///
/// Implement this for message types where size-aware batching is desired.
/// If not implemented, the byte-size threshold in [`BatchingPolicy`] is ignored.
pub trait MessageSize {
    /// Returns the approximate size of this message in bytes.
    fn message_size(&self) -> usize;
}

// Blanket implementations for common types.

impl MessageSize for String {
    fn message_size(&self) -> usize {
        self.len()
    }
}

impl<T: MessageSize> MessageSize for Vec<T> {
    fn message_size(&self) -> usize {
        self.iter().map(|item| item.message_size()).sum()
    }
}

impl MessageSize for &[u8] {
    fn message_size(&self) -> usize {
        self.len()
    }
}

impl MessageSize for u8 {
    fn message_size(&self) -> usize {
        1
    }
}

impl MessageSize for u16 {
    fn message_size(&self) -> usize {
        2
    }
}

impl MessageSize for u32 {
    fn message_size(&self) -> usize {
        4
    }
}

impl MessageSize for u64 {
    fn message_size(&self) -> usize {
        8
    }
}

impl MessageSize for i32 {
    fn message_size(&self) -> usize {
        4
    }
}

impl MessageSize for i64 {
    fn message_size(&self) -> usize {
        8
    }
}

impl MessageSize for f32 {
    fn message_size(&self) -> usize {
        4
    }
}

impl MessageSize for f64 {
    fn message_size(&self) -> usize {
        8
    }
}

/// Accumulates message counts and sizes for batching decisions.
///
/// Each operator input maintains a `BatchAccumulator` to track the current
/// pending batch. When [`should_dispatch`](BatchAccumulator::should_dispatch)
/// returns `true`, the operator should be activated to process the batch.
#[derive(Debug)]
pub struct BatchAccumulator {
    /// Number of messages accumulated since last dispatch.
    count: usize,
    /// Cumulative byte size (if size info is available).
    bytes: usize,
    /// Whether any message provided size information.
    has_size_info: bool,
    /// Timestamp of the first message in the current batch.
    first_message_at: Option<Instant>,
}

impl BatchAccumulator {
    /// Create a new, empty accumulator.
    pub fn new() -> Self {
        Self {
            count: 0,
            bytes: 0,
            has_size_info: false,
            first_message_at: None,
        }
    }

    /// Record an incoming message.
    ///
    /// `size` is the optional byte size of the message. Pass `None` if the
    /// message type does not implement [`MessageSize`].
    pub fn record_message(&mut self, size: Option<usize>) {
        if self.first_message_at.is_none() {
            self.first_message_at = Some(Instant::now());
        }
        self.count += 1;
        if let Some(s) = size {
            self.bytes += s;
            self.has_size_info = true;
        }
    }

    /// Record a message with a known size from [`MessageSize`].
    pub fn record_sized<D: MessageSize>(&mut self, msg: &D) {
        self.record_message(Some(msg.message_size()));
    }

    /// Check whether the accumulated batch should trigger an operator activation.
    ///
    /// Returns `true` if **any** of the policy's thresholds are met.
    pub fn should_dispatch(&self, policy: &BatchingPolicy) -> bool {
        if self.count == 0 {
            return false;
        }

        // Count threshold
        if self.count >= policy.max_batch_count {
            return true;
        }

        // Byte size threshold (only if we have size info AND policy has a byte limit)
        if self.has_size_info {
            if let Some(max_bytes) = policy.max_batch_bytes {
                if self.bytes >= max_bytes {
                    return true;
                }
            }
        }

        // Time threshold
        if let Some(first_at) = self.first_message_at {
            if first_at.elapsed() >= policy.max_batch_wait {
                return true;
            }
        }

        false
    }

    /// Check if the time threshold is met, using an externally-provided "now".
    ///
    /// This variant avoids repeated `Instant::now()` calls in hot paths where
    /// the caller already has a timestamp.
    pub fn should_dispatch_at(&self, policy: &BatchingPolicy, now: Instant) -> bool {
        if self.count == 0 {
            return false;
        }

        if self.count >= policy.max_batch_count {
            return true;
        }

        if self.has_size_info {
            if let Some(max_bytes) = policy.max_batch_bytes {
                if self.bytes >= max_bytes {
                    return true;
                }
            }
        }

        if let Some(first_at) = self.first_message_at {
            if now.saturating_duration_since(first_at) >= policy.max_batch_wait {
                return true;
            }
        }

        false
    }

    /// Reset the accumulator after a dispatch, preparing for the next batch.
    pub fn reset(&mut self) {
        self.count = 0;
        self.bytes = 0;
        self.has_size_info = false;
        self.first_message_at = None;
    }

    /// Current number of accumulated messages.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Current accumulated byte size.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether any message provided size information.
    pub fn has_size_info(&self) -> bool {
        self.has_size_info
    }

    /// Timestamp of the first message in the current batch.
    pub fn first_message_at(&self) -> Option<Instant> {
        self.first_message_at
    }

    /// Whether the accumulator is empty (no pending messages).
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the deadline (instant at which max_batch_wait fires) for the
    /// current batch, or `None` if the batch is empty.
    pub fn deadline(&self, policy: &BatchingPolicy) -> Option<Instant> {
        self.first_message_at.map(|t| t + policy.max_batch_wait)
    }
}

impl Default for BatchAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    // --- BatchingPolicy tests ---

    #[test]
    fn default_policy_values() {
        let policy = BatchingPolicy::default();
        assert_eq!(policy.max_batch_count, 1024);
        assert_eq!(policy.max_batch_bytes, Some(64 * 1024));
        assert_eq!(policy.max_batch_wait, Duration::from_millis(1));
    }

    #[test]
    fn no_batching_policy() {
        let policy = BatchingPolicy::no_batching();
        assert_eq!(policy.max_batch_count, 1);
        assert_eq!(policy.max_batch_bytes, None);
        assert_eq!(policy.max_batch_wait, Duration::ZERO);
    }

    #[test]
    fn count_only_policy() {
        let policy = BatchingPolicy::count_only(256);
        assert_eq!(policy.max_batch_count, 256);
        assert_eq!(policy.max_batch_bytes, None);
        assert_eq!(policy.max_batch_wait, Duration::MAX);
    }

    #[test]
    fn custom_policy() {
        let policy = BatchingPolicy::custom(
            512,
            Some(128 * 1024),
            Duration::from_millis(5),
        );
        assert_eq!(policy.max_batch_count, 512);
        assert_eq!(policy.max_batch_bytes, Some(128 * 1024));
        assert_eq!(policy.max_batch_wait, Duration::from_millis(5));
    }

    // --- BatchAccumulator basic tests ---

    #[test]
    fn empty_accumulator_does_not_dispatch() {
        let acc = BatchAccumulator::new();
        let policy = BatchingPolicy::default();
        assert!(!acc.should_dispatch(&policy));
        assert!(acc.is_empty());
        assert_eq!(acc.count(), 0);
        assert_eq!(acc.bytes(), 0);
    }

    #[test]
    fn count_threshold_triggers_dispatch() {
        let policy = BatchingPolicy::count_only(10);
        let mut acc = BatchAccumulator::new();

        for _ in 0..9 {
            acc.record_message(None);
        }
        assert!(!acc.should_dispatch(&policy));

        acc.record_message(None);
        assert!(acc.should_dispatch(&policy));
        assert_eq!(acc.count(), 10);
    }

    #[test]
    fn byte_size_threshold_triggers_dispatch() {
        let policy = BatchingPolicy::custom(
            1_000_000, // very high count
            Some(100), // low byte threshold
            Duration::from_secs(60), // long wait
        );
        let mut acc = BatchAccumulator::new();

        // Add messages with size info
        acc.record_message(Some(40));
        acc.record_message(Some(40));
        assert!(!acc.should_dispatch(&policy));

        acc.record_message(Some(30)); // total = 110 >= 100
        assert!(acc.should_dispatch(&policy));
        assert_eq!(acc.bytes(), 110);
        assert!(acc.has_size_info());
    }

    #[test]
    fn byte_size_ignored_without_size_info() {
        let policy = BatchingPolicy::custom(
            1_000_000,
            Some(100),
            Duration::from_secs(60),
        );
        let mut acc = BatchAccumulator::new();

        // Record many messages without size info
        for _ in 0..200 {
            acc.record_message(None);
        }
        // Count is 200 < 1_000_000 and no size info, so byte threshold doesn't fire
        assert!(!acc.should_dispatch(&policy));
        assert!(!acc.has_size_info());
    }

    #[test]
    fn time_threshold_triggers_dispatch() {
        let policy = BatchingPolicy::custom(
            1_000_000,
            None,
            Duration::from_millis(10),
        );
        let mut acc = BatchAccumulator::new();
        acc.record_message(None);

        // Immediately after — should not fire
        assert!(!acc.should_dispatch(&policy));

        // Wait for the timeout
        thread::sleep(Duration::from_millis(15));
        assert!(acc.should_dispatch(&policy));
    }

    #[test]
    fn no_batching_dispatches_on_first_message() {
        let policy = BatchingPolicy::no_batching();
        let mut acc = BatchAccumulator::new();
        acc.record_message(None);
        assert!(acc.should_dispatch(&policy));
    }

    #[test]
    fn first_threshold_wins_count_before_bytes() {
        let policy = BatchingPolicy::custom(
            5,
            Some(1000),
            Duration::from_secs(60),
        );
        let mut acc = BatchAccumulator::new();
        for _ in 0..5 {
            acc.record_message(Some(10)); // total bytes = 50 < 1000
        }
        // count == 5 fires first
        assert!(acc.should_dispatch(&policy));
        assert_eq!(acc.bytes(), 50);
    }

    #[test]
    fn first_threshold_wins_bytes_before_count() {
        let policy = BatchingPolicy::custom(
            1000,
            Some(50),
            Duration::from_secs(60),
        );
        let mut acc = BatchAccumulator::new();
        for _ in 0..3 {
            acc.record_message(Some(20)); // total bytes = 60 >= 50
        }
        // bytes fires first (count is only 3 < 1000)
        assert!(acc.should_dispatch(&policy));
        assert_eq!(acc.count(), 3);
    }

    #[test]
    fn reset_clears_accumulator() {
        let mut acc = BatchAccumulator::new();
        acc.record_message(Some(42));
        acc.record_message(Some(58));
        assert_eq!(acc.count(), 2);
        assert_eq!(acc.bytes(), 100);
        assert!(acc.first_message_at().is_some());

        acc.reset();
        assert_eq!(acc.count(), 0);
        assert_eq!(acc.bytes(), 0);
        assert!(!acc.has_size_info());
        assert!(acc.first_message_at().is_none());
        assert!(acc.is_empty());
    }

    #[test]
    fn deadline_computation() {
        let policy = BatchingPolicy::custom(100, None, Duration::from_millis(5));
        let mut acc = BatchAccumulator::new();

        assert_eq!(acc.deadline(&policy), None);

        acc.record_message(None);
        let deadline = acc.deadline(&policy).unwrap();
        let first = acc.first_message_at().unwrap();
        assert_eq!(deadline, first + Duration::from_millis(5));
    }

    #[test]
    fn should_dispatch_at_uses_provided_time() {
        let policy = BatchingPolicy::custom(
            1_000_000,
            None,
            Duration::from_millis(10),
        );
        let mut acc = BatchAccumulator::new();
        acc.record_message(None);
        let first = acc.first_message_at().unwrap();

        // Before deadline
        let before = first + Duration::from_millis(5);
        assert!(!acc.should_dispatch_at(&policy, before));

        // At deadline
        let at = first + Duration::from_millis(10);
        assert!(acc.should_dispatch_at(&policy, at));

        // After deadline
        let after = first + Duration::from_millis(20);
        assert!(acc.should_dispatch_at(&policy, after));
    }

    // --- MessageSize trait tests ---

    #[test]
    fn message_size_string() {
        let s = String::from("hello");
        assert_eq!(s.message_size(), 5);
    }

    #[test]
    fn message_size_vec_u8() {
        let v: Vec<u8> = vec![1, 2, 3, 4];
        assert_eq!(v.message_size(), 4);
    }

    #[test]
    fn message_size_vec_u64() {
        let v: Vec<u64> = vec![1, 2, 3];
        assert_eq!(v.message_size(), 3 * 8);
    }

    #[test]
    fn message_size_primitives() {
        assert_eq!(42u8.message_size(), 1);
        assert_eq!(42u16.message_size(), 2);
        assert_eq!(42u32.message_size(), 4);
        assert_eq!(42u64.message_size(), 8);
        assert_eq!(42i32.message_size(), 4);
        assert_eq!(42i64.message_size(), 8);
        assert_eq!(1.0f32.message_size(), 4);
        assert_eq!(1.0f64.message_size(), 8);
    }

    #[test]
    fn message_size_slice() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        assert_eq!(data.message_size(), 5);
    }

    #[test]
    fn record_sized_uses_message_size_trait() {
        let mut acc = BatchAccumulator::new();
        let msg = String::from("hello world"); // 11 bytes
        acc.record_sized(&msg);
        assert_eq!(acc.bytes(), 11);
        assert_eq!(acc.count(), 1);
        assert!(acc.has_size_info());
    }

    // --- Custom MessageSize impl ---

    struct MyMessage {
        payload: Vec<u8>,
        header: [u8; 16],
    }

    impl MessageSize for MyMessage {
        fn message_size(&self) -> usize {
            self.payload.len() + self.header.len()
        }
    }

    #[test]
    fn custom_message_size_impl() {
        let msg = MyMessage {
            payload: vec![0u8; 100],
            header: [0u8; 16],
        };
        assert_eq!(msg.message_size(), 116);

        let mut acc = BatchAccumulator::new();
        acc.record_sized(&msg);
        assert_eq!(acc.bytes(), 116);
    }

    // --- Integration-style tests ---

    #[test]
    fn operator_receives_coalesced_batch() {
        let policy = BatchingPolicy::count_only(5);
        let mut acc = BatchAccumulator::new();
        let mut dispatches = 0;
        let total_messages = 23;

        for _ in 0..total_messages {
            acc.record_message(None);
            if acc.should_dispatch(&policy) {
                dispatches += 1;
                acc.reset();
            }
        }

        // 23 messages / batch_size 5 = 4 full dispatches (20 msgs) + 3 pending
        assert_eq!(dispatches, 4);
        assert_eq!(acc.count(), 3);
    }

    #[test]
    fn batching_with_backpressure_gives_natural_batching() {
        // Simulates a slow operator: messages pile up during processing
        let policy = BatchingPolicy::count_only(10);
        let mut acc = BatchAccumulator::new();

        // 50 messages arrive while operator is busy
        for _ in 0..50 {
            acc.record_message(None);
        }

        // Operator becomes available — immediate dispatch
        assert!(acc.should_dispatch(&policy));
        assert_eq!(acc.count(), 50); // natural mega-batch due to backpressure
    }

    #[test]
    fn max_batch_wait_guarantees_bounded_latency() {
        let policy = BatchingPolicy::custom(
            1_000_000, // very high count
            None,
            Duration::from_millis(5), // low time threshold
        );
        let mut acc = BatchAccumulator::new();

        // Single message arrives under low throughput
        acc.record_message(None);
        assert!(!acc.should_dispatch(&policy));

        // After the wait period, latency guarantee kicks in
        thread::sleep(Duration::from_millis(10));
        assert!(acc.should_dispatch(&policy));
    }

    #[test]
    #[should_panic(expected = "max_batch_count must be positive")]
    fn zero_max_batch_count_panics() {
        BatchingPolicy::custom(0, None, Duration::from_millis(1));
    }

    #[test]
    fn should_dispatch_at_with_earlier_now_does_not_panic() {
        let policy = BatchingPolicy::custom(
            1_000_000,
            None,
            Duration::from_millis(10),
        );
        let mut acc = BatchAccumulator::new();

        // Record a message, then pass a "now" that is earlier (simulated via subtraction)
        thread::sleep(Duration::from_millis(1));
        acc.record_message(None);
        let earlier = acc.first_message_at().unwrap() - Duration::from_millis(1);
        // Should not panic, should return false
        assert!(!acc.should_dispatch_at(&policy, earlier));
    }

    #[test]
    fn count_only_does_not_dispatch_on_time() {
        let policy = BatchingPolicy::count_only(100);
        let mut acc = BatchAccumulator::new();
        acc.record_message(None);

        // Even after sleeping, count_only should not trigger time-based dispatch
        thread::sleep(Duration::from_millis(5));
        assert!(!acc.should_dispatch(&policy));
    }

    #[test]
    fn vec_of_strings_message_size_sums_elements() {
        let v = vec![
            String::from("hello"),      // 5 bytes
            String::from("world!"),     // 6 bytes
        ];
        assert_eq!(v.message_size(), 11);
    }
}
