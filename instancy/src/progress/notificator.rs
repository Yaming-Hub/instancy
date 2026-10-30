//! Notification delivery for operators.
//!
//! A [`Notificator`] tracks timestamps at which an operator has requested
//! notification. When the input frontier advances past a requested timestamp
//! (meaning no more data at that time can arrive), the notification becomes
//! "ready" and can be delivered to the operator on its next activation.
//!
//! Notifications are **capability-backed** at the operator level:
//! [`NotifyContext::notify_at`](crate::dataflow::operators::handles::NotifyContext::notify_at)
//! holds an output capability for every requested time and rejects times the
//! operator cannot legitimately hold (times the input frontier has already
//! passed and that are not covered by a held capability or a pending input
//! batch). The `Notificator` itself is the low-level bookkeeping primitive and
//! performs no such validation.
//!
//! Fired notifications are delivered in timestamp order (`Ord`, a linear
//! extension of the partial order), so an operator that carries state across
//! times (e.g. a running total) observes completed times in order even when
//! several complete in the same frontier update.
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

use std::collections::BTreeSet;

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
    pending: BTreeSet<T>,
    /// Times whose notifications have fired and await consumption by the
    /// operator, in `Ord` order.
    ready: BTreeSet<T>,
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
            pending: BTreeSet::new(),
            ready: BTreeSet::new(),
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
    /// Duplicate requests for the same timestamp are coalesced. O(log n) in
    /// the number of pending and ready notifications.
    pub fn notify_at(&mut self, time: T) {
        if self.pending.contains(&time) || self.ready.contains(&time) {
            return;
        }
        // If frontier already past this time, fire immediately
        if !self.frontier.less_equal(&time) {
            self.ready.insert(time);
        } else {
            self.pending.insert(time);
        }
    }

    /// Returns the next ready notification, or `None` if no notifications have fired.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<FiredNotification<T>> {
        self.ready
            .pop_first()
            .map(|time| FiredNotification { time })
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
    ///
    /// Ready notifications are delivered in timestamp order (`Ord`, which the
    /// [`Timestamp`] contract requires to be a linear extension of the partial
    /// order), regardless of the order in which they were requested or fired.
    pub fn update_frontier(&mut self, new_frontier: &Antichain<T>) -> usize {
        self.frontier = new_frontier.clone();

        let frontier = &self.frontier;
        let mut fired: Vec<T> = Vec::new();
        self.pending.retain(|time| {
            if frontier.less_equal(time) {
                true
            } else {
                fired.push(time.clone());
                false
            }
        });
        debug_assert!(
            fired.windows(2).all(|w| !w[1].less_than(&w[0])),
            "Timestamp Ord must be a linear extension of PartialOrder: {fired:?}"
        );
        let count = fired.len();
        self.ready.extend(fired);
        count
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
    use crate::order::PartialOrder;

    #[test]
    fn notify_at_fires_when_frontier_advances() {
        let initial = Antichain::from_elem(0u64);
        let mut notificator = Notificator::new(initial);

        // Request notification at time 5
        notificator.notify_at(5);
        assert_eq!(notificator.pending_count(), 1);
        assert_eq!(notificator.ready_count(), 0);
        // The requested time itself is pending, once, until the frontier
        // passes it.
        assert_eq!(notificator.pending, BTreeSet::from([5]));
        assert!(notificator.ready.is_empty());

        // Frontier advances to 3 — not past 5 yet
        let frontier_3 = Antichain::from_elem(3u64);
        let fired = notificator.update_frontier(&frontier_3);
        assert_eq!(fired, 0);
        assert_eq!(notificator.pending_count(), 1);
        assert_eq!(notificator.pending, BTreeSet::from([5]));

        // Frontier advances to 6 — past 5!
        let frontier_6 = Antichain::from_elem(6u64);
        let fired = notificator.update_frontier(&frontier_6);
        assert_eq!(fired, 1);
        assert_eq!(notificator.pending_count(), 0);
        assert_eq!(notificator.ready_count(), 1);
        assert!(notificator.pending.is_empty());
        assert_eq!(notificator.ready, BTreeSet::from([5]));

        // Consume the notification
        let n = notificator.next().unwrap();
        assert_eq!(*n.time(), 5);
        assert!(notificator.next().is_none());
        assert!(notificator.is_empty());
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
        // The one pending entry is the requested time.
        assert_eq!(notificator.pending, BTreeSet::from([5]));
        assert!(notificator.ready.is_empty());
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

    fn drain_times<T: Timestamp>(n: &mut Notificator<T>) -> Vec<T> {
        std::iter::from_fn(|| n.next().map(FiredNotification::into_time)).collect()
    }

    #[test]
    fn batch_fires_in_timestamp_order() {
        let mut n = Notificator::new(Antichain::from_elem(0u64));
        for t in [7u64, 3, 9, 5, 1, 8] {
            n.notify_at(t);
        }
        assert_eq!(n.update_frontier(&Antichain::from_elem(10)), 6);
        assert_eq!(drain_times(&mut n), vec![1, 3, 5, 7, 8, 9]);
    }

    #[test]
    fn product_times_fire_in_a_linear_extension_of_the_partial_order() {
        use crate::order::Product;
        let mut n = Notificator::new(Antichain::from_elem(Product::new(0u64, 0u32)));
        let times = [
            Product::new(1u64, 0u32),
            Product::new(0, 3),
            Product::new(0, 1),
            Product::new(1, 2),
            Product::new(0, 2),
            Product::new(2, 0),
        ];
        for t in times {
            n.notify_at(t);
        }
        n.update_frontier(&Antichain::new());
        let got = drain_times(&mut n);
        assert_eq!(got.len(), times.len());
        for i in 0..got.len() {
            for j in i + 1..got.len() {
                assert!(
                    !got[j].less_than(&got[i]),
                    "{:?} delivered before {:?}: {got:?}",
                    got[i],
                    got[j]
                );
            }
        }
    }

    #[test]
    fn newly_fired_times_merge_ahead_of_unconsumed_later_times() {
        use crate::order::Product;
        // (0,5) fires first and is left unconsumed; then (1,0) and (0,7) fire.
        let mut n = Notificator::new(Antichain::from_elem(Product::new(0u64, 0u32)));
        n.notify_at(Product::new(0, 5));
        n.notify_at(Product::new(0, 7));
        n.notify_at(Product::new(1, 0));
        n.update_frontier(&Antichain::from_elem_iter([
            Product::new(0, 6),
            Product::new(1, 0),
        ]));
        assert_eq!(n.ready_count(), 1);
        n.update_frontier(&Antichain::new());
        assert_eq!(
            drain_times(&mut n),
            vec![Product::new(0, 5), Product::new(0, 7), Product::new(1, 0)]
        );
    }

    #[test]
    fn immediately_fired_request_is_ordered_among_ready_notifications() {
        let mut n = Notificator::new(Antichain::from_elem(0u64));
        n.notify_at(4);
        n.notify_at(8);
        n.update_frontier(&Antichain::from_elem(10));
        // Already complete: fires immediately and must be delivered first.
        n.notify_at(2);
        assert_eq!(drain_times(&mut n), vec![2, 4, 8]);
    }

    /// On `Product` times: duplicate requests are delivered once, a time the
    /// frontier has not passed waits for a later frontier, and a request made
    /// mid-drain is delivered before the later ready time. (6, 0) precedes
    /// (5, 8) because it completes at an earlier frontier.
    #[test]
    fn product_time_notifications_follow_the_partial_order_across_frontiers() {
        use crate::order::Product;
        let p = Product::<u64, u32>::new;
        let mut n = Notificator::new(Antichain::from_elem(p(0, 0)));
        for (outer, inner) in [
            (3, 5),
            (5, 4),
            (1, 2),
            (1, 1),
            (1, 1),
            (5, 4),
            (6, 0),
            (6, 2),
            (5, 8),
        ] {
            n.notify_at(p(outer, inner));
        }
        assert!(n.next().is_none());
        assert_eq!(
            n.pending,
            BTreeSet::from([
                p(1, 1),
                p(1, 2),
                p(3, 5),
                p(5, 4),
                p(5, 8),
                p(6, 0),
                p(6, 2),
            ])
        );

        n.update_frontier(&Antichain::from_elem_iter([p(5, 7), p(6, 1)]));
        assert_eq!(
            drain_times(&mut n),
            vec![p(1, 1), p(1, 2), p(3, 5), p(5, 4), p(6, 0)]
        );
        assert_eq!(n.pending, BTreeSet::from([p(5, 8), p(6, 2)]));

        n.update_frontier(&Antichain::from_elem(p(6, 10)));
        assert_eq!(n.next().map(FiredNotification::into_time), Some(p(5, 8)));
        n.notify_at(p(5, 9));
        assert_eq!(n.next().map(FiredNotification::into_time), Some(p(5, 9)));
        assert_eq!(n.next().map(FiredNotification::into_time), Some(p(6, 2)));
        assert!(n.next().is_none());
        assert!(n.is_empty());
    }

    /// Requests in any order, repeated, delivered once each in time order,
    /// whether they were pending or already ready.
    #[test]
    fn duplicate_requests_coalesce_and_deliver_in_order() {
        let mut n = Notificator::new(Antichain::from_elem(10u64));
        for t in (0..20u64).rev().chain(0..20) {
            n.notify_at(t);
        }
        // 0..10 fired at once (the frontier is past them); 10..20 pend.
        assert_eq!(n.ready_count(), 10);
        assert_eq!(n.pending_count(), 10);
        n.update_frontier(&Antichain::from_elem(15));
        n.notify_at(12);
        n.notify_at(17);
        let fired: Vec<u64> =
            std::iter::from_fn(|| n.next().map(FiredNotification::into_time)).collect();
        assert_eq!(fired, (0..15).collect::<Vec<_>>());
        n.update_frontier(&Antichain::new());
        let fired: Vec<u64> =
            std::iter::from_fn(|| n.next().map(FiredNotification::into_time)).collect();
        assert_eq!(fired, (15..20).collect::<Vec<_>>());
        assert!(n.is_empty());
    }
}
