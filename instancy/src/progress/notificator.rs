//! Notification delivery for operators.
//!
//! A [`Notificator`] tracks timestamps at which an operator has requested
//! notification. When the input frontier advances past a requested timestamp
//! (meaning no more data at that time can arrive), the notification becomes
//! "ready" and can be delivered to the operator on its next activation.
//!
//! Notifications are **capability-backed**: operators request notification by
//! providing a capability (or a time derived from a capability they hold).
//! This ensures that only timestamps the operator legitimately participates in
//! can be requested.
//!
//! # Usage Pattern
//!
//! ```ignore
//! // In operator logic:
//! notificator.notify_at(cap.delayed(&time)?);
//!
//! // On next activation, after frontier advances:
//! while let Some(notification) = notificator.next() {
//!     // timestamp `notification.time()` is complete — process buffered data
//! }
//! ```

use std::collections::VecDeque;

use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// FiredNotification
// ---------------------------------------------------------------------------

/// A notification that has been triggered (frontier advanced past the time).
#[derive(Debug, Clone)]
pub struct FiredNotification<T: Timestamp> {
    time: T,
}

impl<T: Timestamp> FiredNotification<T> {
    /// The timestamp that is now complete (no more input at this time).
    pub fn time(&self) -> &T {
        &self.time
    }

    /// Consume the notification and return the timestamp.
    pub fn into_time(self) -> T {
        self.time
    }
}

// ---------------------------------------------------------------------------
// Notificator
// ---------------------------------------------------------------------------

/// Tracks requested notification timestamps and delivers them when ready.
///
/// An operator requests notification at timestamps it holds capabilities for.
/// When the input frontier advances past a requested time (i.e., no element
/// in the frontier is `<=` the time), the notification fires.
///
/// # Per-Operator
///
/// Each operator has one `Notificator<T>`. For operators with multiple input
/// ports, the notificator uses the *meet* (intersection) of all input frontiers:
/// a timestamp is complete only when ALL inputs have advanced past it.
pub struct Notificator<T: Timestamp> {
    /// Timestamps at which notifications have been requested (pending delivery).
    pending: Vec<T>,
    /// Notifications that have fired and await consumption by the operator.
    ready: VecDeque<FiredNotification<T>>,
    /// Current input frontier (meet of all input port frontiers for this operator).
    frontier: Antichain<T>,
}

impl<T: Timestamp> Notificator<T> {
    /// Creates a new notificator with an initial frontier.
    ///
    /// The initial frontier is typically `Antichain::from_elem(T::minimum())`
    /// for a freshly started dataflow.
    pub fn new(initial_frontier: Antichain<T>) -> Self {
        Self {
            pending: Vec::new(),
            ready: VecDeque::new(),
            frontier: initial_frontier,
        }
    }

    /// Request notification when timestamp `time` is complete.
    ///
    /// "Complete" means the input frontier has advanced past `time` — no more
    /// data at this timestamp can arrive.
    ///
    /// If the frontier has ALREADY advanced past `time`, the notification is
    /// immediately ready (will be returned on the next `next()` call).
    ///
    /// Duplicate requests for the same timestamp are coalesced.
    pub fn notify_at(&mut self, time: T) {
        // Check if already pending or ready
        if self.pending.iter().any(|t| t == &time) {
            return;
        }
        if self.ready.iter().any(|n| n.time == time) {
            return;
        }

        // If frontier already past this time, fire immediately
        if !self.frontier.less_equal(&time) {
            self.ready.push_back(FiredNotification { time });
        } else {
            self.pending.push(time);
        }
    }

    /// Returns the next ready notification, or `None` if no notifications have fired.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<FiredNotification<T>> {
        self.ready.pop_front()
    }

    /// Returns true if there are ready notifications waiting to be consumed.
    pub fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Returns the number of pending (not yet fired) notification requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns the number of ready (fired, awaiting consumption) notifications.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    /// Update the input frontier and fire any notifications that become ready.
    ///
    /// Called by the executor after progress propagation. Returns the number
    /// of newly fired notifications.
    pub fn update_frontier(&mut self, new_frontier: &Antichain<T>) -> usize {
        self.frontier = new_frontier.clone();

        let mut fired = 0;
        // Drain pending notifications that are now past the frontier
        let mut i = 0;
        while i < self.pending.len() {
            if !self.frontier.less_equal(&self.pending[i]) {
                let time = self.pending.swap_remove(i);
                self.ready.push_back(FiredNotification { time });
                fired += 1;
                // Don't increment i — swap_remove moved last element to i
            } else {
                i += 1;
            }
        }
        fired
    }

    /// Returns the current frontier known to this notificator.
    pub fn frontier(&self) -> &Antichain<T> {
        &self.frontier
    }

    /// Returns true if there are no pending or ready notifications.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.ready.is_empty()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_at_fires_when_frontier_advances() {
        let initial = Antichain::from_elem(0u64);
        let mut notificator = Notificator::new(initial);

        // Request notification at time 5
        notificator.notify_at(5);
        assert_eq!(notificator.pending_count(), 1);
        assert_eq!(notificator.ready_count(), 0);

        // Frontier advances to 3 — not past 5 yet
        let frontier_3 = Antichain::from_elem(3u64);
        let fired = notificator.update_frontier(&frontier_3);
        assert_eq!(fired, 0);
        assert_eq!(notificator.pending_count(), 1);

        // Frontier advances to 6 — past 5!
        let frontier_6 = Antichain::from_elem(6u64);
        let fired = notificator.update_frontier(&frontier_6);
        assert_eq!(fired, 1);
        assert_eq!(notificator.pending_count(), 0);
        assert_eq!(notificator.ready_count(), 1);

        // Consume the notification
        let n = notificator.next().unwrap();
        assert_eq!(*n.time(), 5);
        assert!(notificator.next().is_none());
    }

    #[test]
    fn notify_at_fires_immediately_if_frontier_already_past() {
        // Frontier already at 10
        let initial = Antichain::from_elem(10u64);
        let mut notificator = Notificator::new(initial);

        // Request notification at time 5 — already complete!
        notificator.notify_at(5);
        assert_eq!(notificator.pending_count(), 0);
        assert_eq!(notificator.ready_count(), 1);

        let n = notificator.next().unwrap();
        assert_eq!(*n.time(), 5);
    }

    #[test]
    fn duplicate_requests_are_coalesced() {
        let initial = Antichain::from_elem(0u64);
        let mut notificator = Notificator::new(initial);

        notificator.notify_at(5);
        notificator.notify_at(5);
        notificator.notify_at(5);
        assert_eq!(notificator.pending_count(), 1);
    }

    #[test]
    fn multiple_notifications_fire_in_batch() {
        let initial = Antichain::from_elem(0u64);
        let mut notificator = Notificator::new(initial);

        notificator.notify_at(3);
        notificator.notify_at(5);
        notificator.notify_at(7);
        assert_eq!(notificator.pending_count(), 3);

        // Advance to 6 — fires 3 and 5, but not 7
        let frontier_6 = Antichain::from_elem(6u64);
        let fired = notificator.update_frontier(&frontier_6);
        assert_eq!(fired, 2);
        assert_eq!(notificator.pending_count(), 1);
        assert_eq!(notificator.ready_count(), 2);

        // Both are available
        let n1 = notificator.next().unwrap();
        let n2 = notificator.next().unwrap();
        let mut times: Vec<u64> = vec![n1.into_time(), n2.into_time()];
        times.sort();
        assert_eq!(times, vec![3, 5]);
    }

    #[test]
    fn empty_frontier_fires_nothing() {
        // Empty frontier means "nothing is less_equal to any time" → everything fires
        let initial = Antichain::new();
        let mut notificator = Notificator::new(initial);

        // With empty frontier, less_equal returns false for all times
        notificator.notify_at(0);
        // Should fire immediately since empty frontier is past everything
        assert_eq!(notificator.ready_count(), 1);
    }

    #[test]
    fn frontier_at_same_time_does_not_fire() {
        let initial = Antichain::from_elem(5u64);
        let mut notificator = Notificator::new(initial);

        // Request notification at time 5 — frontier is AT 5 (less_equal returns true)
        notificator.notify_at(5);
        assert_eq!(notificator.pending_count(), 1);
        assert_eq!(notificator.ready_count(), 0);

        // Frontier advances to 5 (same) — still not past
        let frontier_5 = Antichain::from_elem(5u64);
        let fired = notificator.update_frontier(&frontier_5);
        assert_eq!(fired, 0);

        // Frontier advances to 6 — now past 5
        let frontier_6 = Antichain::from_elem(6u64);
        let fired = notificator.update_frontier(&frontier_6);
        assert_eq!(fired, 1);
    }

    #[test]
    fn is_empty_reflects_state() {
        let initial = Antichain::from_elem(0u64);
        let mut notificator = Notificator::new(initial);

        assert!(notificator.is_empty());

        notificator.notify_at(5);
        assert!(!notificator.is_empty());

        let frontier = Antichain::from_elem(10u64);
        notificator.update_frontier(&frontier);
        assert!(!notificator.is_empty()); // ready, not consumed

        notificator.next();
        assert!(notificator.is_empty());
    }
}
