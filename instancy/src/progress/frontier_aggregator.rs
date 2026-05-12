#[cfg(debug_assertions)]
#[allow(unused_imports)]
use crate::order::PartialOrder;
use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// Aggregates frontiers from multiple senders into a single frontier.
///
/// In the StageExecutor model, each exchange input has a FrontierAggregator
/// that tracks the frontier of each sender independently. The aggregated
/// frontier is `min(all sender frontiers)` — the earliest timestamp that
/// any sender might still produce.
///
/// The sender count is static (known at setup time from the source stage's
/// parallelism), so the aggregator is initialized with a fixed number of
/// sender slots.
pub(crate) struct FrontierAggregator<T: Timestamp> {
    /// Per-sender frontier state. Indexed by sender index.
    sender_frontiers: Vec<Antichain<T>>,
    /// Per-sender done flag. When all senders are done, the aggregate is empty.
    sender_done: Vec<bool>,
    /// Cached aggregate frontier. Recomputed when any sender changes.
    aggregate: Antichain<T>,
    /// Whether the cached aggregate needs recomputation.
    dirty: bool,
}

impl<T: Timestamp> FrontierAggregator<T> {
    /// Creates a new aggregator for a fixed number of senders.
    ///
    /// Each sender starts at `T::minimum()`.
    ///
    /// # Panics
    ///
    /// Panics if `num_senders` is zero.
    pub fn new(num_senders: usize) -> Self {
        assert!(num_senders > 0, "FrontierAggregator requires at least one sender");
        let sender_frontiers = vec![Antichain::from_elem(T::minimum()); num_senders];

        Self {
            sender_frontiers,
            sender_done: vec![false; num_senders],
            aggregate: Antichain::from_elem(T::minimum()),
            dirty: false,
        }
    }

    /// Replaces the frontier for one sender.
    ///
    /// The new frontier must represent forward progress: every element in the
    /// old frontier must be `less_equal` to some element in the new frontier.
    /// In practice, frontiers only advance (timestamps get larger or the
    /// frontier becomes empty).
    ///
    /// # Panics
    ///
    /// Panics if `sender` is out of bounds or already marked done.
    /// In debug builds, panics if the new frontier regresses.
    pub fn update_sender(&mut self, sender: usize, frontier: Antichain<T>) {
        assert!(
            sender < self.sender_frontiers.len(),
            "sender {sender} out of bounds for {} senders",
            self.sender_frontiers.len()
        );
        assert!(
            !self.sender_done[sender],
            "sender {sender} already marked done; cannot update frontier"
        );

        // Monotonicity: every old frontier element must be <= some new element.
        // An empty new frontier (meaning "done") always satisfies this.
        debug_assert!(
            self.sender_frontiers[sender].elements().iter().all(|old_t| {
                frontier.elements().is_empty()
                    || frontier
                        .elements()
                        .iter()
                        .any(|new_t| old_t.less_equal(new_t))
            }),
            "frontier must not regress: old={:?}, new={:?}",
            self.sender_frontiers[sender],
            frontier
        );

        self.sender_frontiers[sender] = frontier;
        self.dirty = true;
    }

    /// Marks a sender as done.
    ///
    /// A done sender contributes an empty frontier and no longer constrains the
    /// aggregate frontier.
    ///
    /// # Panics
    ///
    /// Panics if `sender` is out of bounds.
    pub fn mark_sender_done(&mut self, sender: usize) {
        assert!(
            sender < self.sender_frontiers.len(),
            "sender {sender} out of bounds for {} senders",
            self.sender_frontiers.len()
        );

        self.sender_frontiers[sender].clear();
        self.sender_done[sender] = true;
        self.dirty = true;
    }

    /// Returns the aggregated frontier.
    ///
    /// The aggregate is recomputed lazily when any sender frontier changes.
    pub fn frontier(&mut self) -> &Antichain<T> {
        if self.dirty {
            self.recompute();
        }

        &self.aggregate
    }

    /// Returns `true` if all senders have been marked done.
    pub fn is_all_done(&self) -> bool {
        self.sender_done.iter().all(|done| *done)
    }

    /// Returns the number of sender slots tracked by this aggregator.
    pub fn num_senders(&self) -> usize {
        self.sender_frontiers.len()
    }

    fn recompute(&mut self) {
        if self.is_all_done() {
            self.aggregate = Antichain::new();
            self.dirty = false;
            return;
        }

        self.aggregate = Antichain::from_elem_iter(
            self.sender_frontiers
                .iter()
                .zip(self.sender_done.iter())
                .filter(|(_, done)| !**done)
                .flat_map(|(frontier, _)| frontier.elements().iter().cloned()),
        );
        self.dirty = false;
    }
}

#[cfg(test)]
mod tests {
    use super::FrontierAggregator;
    use crate::order::Product;
    use crate::progress::frontier::Antichain;

    #[test]
    fn new_starts_at_minimum() {
        let mut aggregator = FrontierAggregator::<u64>::new(3);

        assert_eq!(aggregator.num_senders(), 3);
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));
        assert!(!aggregator.is_all_done());
    }

    #[test]
    fn single_sender_update_advances_aggregate() {
        let mut aggregator = FrontierAggregator::<u64>::new(1);

        aggregator.update_sender(0, Antichain::from_elem(5));

        assert_eq!(aggregator.frontier(), &Antichain::from_elem(5));
    }

    #[test]
    fn multiple_senders_track_the_minimum() {
        let mut aggregator = FrontierAggregator::<u64>::new(3);

        aggregator.update_sender(0, Antichain::from_elem(10));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));

        aggregator.update_sender(1, Antichain::from_elem(7));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));

        aggregator.update_sender(2, Antichain::from_elem(12));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(7));

        aggregator.update_sender(1, Antichain::from_elem(15));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(10));
    }

    #[test]
    fn mark_sender_done_uses_remaining_senders() {
        let mut aggregator = FrontierAggregator::<u64>::new(2);

        aggregator.update_sender(0, Antichain::from_elem(4));
        aggregator.update_sender(1, Antichain::from_elem(9));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(4));

        aggregator.mark_sender_done(0);
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(9));
        assert!(!aggregator.is_all_done());
    }

    #[test]
    fn all_senders_done_yields_empty_frontier() {
        let mut aggregator = FrontierAggregator::<u64>::new(2);

        aggregator.mark_sender_done(0);
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));
        assert!(!aggregator.is_all_done());

        aggregator.mark_sender_done(1);
        assert!(aggregator.frontier().is_empty());
        assert!(aggregator.is_all_done());
    }

    #[test]
    fn partial_order_timestamps_keep_minimal_elements() {
        let mut aggregator = FrontierAggregator::<Product<u64, u64>>::new(2);

        aggregator.update_sender(0, Antichain::from_elem(Product::new(1, 3)));
        aggregator.update_sender(1, Antichain::from_elem(Product::new(2, 2)));

        assert_eq!(
            aggregator.frontier(),
            &Antichain::from_elem_iter([Product::new(1, 3), Product::new(2, 2),]),
        );
    }

    #[test]
    fn out_of_order_updates_are_aggregated_correctly() {
        let mut aggregator = FrontierAggregator::<u64>::new(3);

        aggregator.update_sender(2, Antichain::from_elem(8));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));

        aggregator.update_sender(1, Antichain::from_elem(6));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(0));

        aggregator.update_sender(0, Antichain::from_elem(4));
        assert_eq!(aggregator.frontier(), &Antichain::from_elem(4));
    }

    #[test]
    #[should_panic(expected = "already marked done")]
    fn update_after_done_panics() {
        let mut aggregator = FrontierAggregator::<u64>::new(1);

        aggregator.mark_sender_done(0);
        aggregator.update_sender(0, Antichain::from_elem(3));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "frontier must not regress")]
    fn frontier_regression_panics_in_debug() {
        let mut aggregator = FrontierAggregator::<u64>::new(1);

        aggregator.update_sender(0, Antichain::from_elem(9));
        aggregator.update_sender(0, Antichain::from_elem(3));
    }

    #[test]
    fn single_sender_done_makes_aggregate_empty() {
        let mut aggregator = FrontierAggregator::<u64>::new(1);

        aggregator.mark_sender_done(0);

        assert!(aggregator.frontier().is_empty());
        assert!(aggregator.is_all_done());
    }

    #[test]
    #[should_panic(expected = "at least one sender")]
    fn zero_senders_panics() {
        let _ = FrontierAggregator::<u64>::new(0);
    }
}
