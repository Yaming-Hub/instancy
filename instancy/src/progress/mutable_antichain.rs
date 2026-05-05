//! MutableAntichain — a frontier tracker that supports incremental updates.
//!
//! Unlike [`Antichain`], which is a simple
//! set of mutually incomparable elements, `MutableAntichain` tracks a **multiset**
//! of timestamps where each timestamp has an integer count (multiplicity). The
//! frontier is the set of minimal timestamps with positive total count.
//!
//! # Why multiplicity matters
//!
//! Multiple capabilities can exist at the same timestamp. For example, two operators
//! might both hold a capability at time 5. The frontier should only advance past
//! time 5 when BOTH capabilities are dropped. `MutableAntichain` tracks that time 5
//! has count 2, so dropping one capability (count → 1) doesn't remove it from the
//! frontier. Only when count reaches 0 does the timestamp leave the frontier.
//!
//! # Role in the progress tracking pipeline
//!
//! `MutableAntichain` is used in two key places:
//! 1. **Pointstamp tracking** (in `reachability::PortInformation`): tracks the direct
//!    capability counts at each port. Its frontier changes seed the propagation worklist.
//! 2. **Implication tracking** (also in `PortInformation`): tracks the propagated
//!    implications from all reachable upstream capabilities. Its frontier is what
//!    operators observe as their "input frontier".
//!
//! # Three-field design
//!
//! - `updates`: A [`ChangeBatch`] accumulating raw `(timestamp, ±count)` changes.
//!   This is the source of truth for multiplicity.
//! - `frontier`: The current minimal elements with positive count (cached for fast reads).
//! - `changes`: A [`ChangeBatch`] recording frontier deltas from the last `update_iter()`.
//!   Callers read these to know what changed (e.g., to seed the propagation worklist).

use crate::order::PartialOrder;
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;

/// A frontier tracker that supports incremental updates with multiplicity.
///
/// See the [module documentation](self) for a full explanation of the three-field
/// design and why multiplicity tracking is needed.
///
/// Updates can both advance and retreat the frontier. The frontier is rebuilt
/// from scratch when a batch of updates potentially changes it, which is efficient
/// for batched use but may be expensive for single-element updates to large sets.
#[derive(Clone, Debug)]
pub struct MutableAntichain<T> {
    /// Accumulated (timestamp, count) updates — the source of truth for multiplicity.
    /// After compaction, each timestamp appears at most once with its net count.
    updates: ChangeBatch<T>,
    /// The current frontier: minimal elements from `updates` that have positive count.
    /// This is a cached derivation of `updates`, rebuilt when updates might change it.
    frontier: Vec<T>,
    /// Frontier delta from the last `update_iter()` call: `(timestamp, +1)` for
    /// elements added to the frontier, `(timestamp, -1)` for elements removed.
    /// Consumed by callers to react to frontier changes (e.g., seeding the worklist).
    changes: ChangeBatch<T>,
}

impl<T> MutableAntichain<T> {
    /// Creates a new empty `MutableAntichain`.
    pub fn new() -> Self {
        MutableAntichain {
            updates: ChangeBatch::new(),
            frontier: Vec::new(),
            changes: ChangeBatch::new(),
        }
    }

    /// Returns the current frontier as a slice.
    #[inline]
    pub fn frontier(&self) -> &[T] {
        &self.frontier
    }

    /// Returns the current frontier as an `Antichain`.
    pub fn frontier_antichain(&self) -> Antichain<T>
    where
        T: Clone + PartialOrder,
    {
        Antichain::from_elem_iter(self.frontier.iter().cloned())
    }

    /// Returns `true` if the frontier is empty (no timestamps with positive count).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.frontier.is_empty()
    }

    /// Removes all updates and resets the frontier.
    pub fn clear(&mut self) {
        self.updates.clear();
        self.frontier.clear();
        self.changes.clear();
    }
}

impl<T: Clone + PartialOrder + Ord> MutableAntichain<T> {
    /// Creates a new `MutableAntichain` with a single element at count 1.
    pub fn from_elem(element: T) -> Self {
        let mut result = Self::new();
        result.update_iter(std::iter::once((element, 1)));
        result
    }

    /// Returns `true` if any frontier element is strictly less than `time`.
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.frontier.iter().any(|f| f.less_than(time))
    }

    /// Returns `true` if any frontier element is less than or equal to `time`.
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.frontier.iter().any(|f| f.less_equal(time))
    }

    /// Applies a batch of `(timestamp, delta)` updates.
    ///
    /// Returns an iterator over the resulting changes to the frontier:
    /// `(timestamp, +1)` for elements added and `(timestamp, -1)` for elements removed.
    ///
    /// # Rebuild heuristic
    ///
    /// Rebuilding the frontier from scratch is O(n²) in the worst case. To avoid
    /// unnecessary rebuilds, we check each update against the current frontier:
    /// - **Positive delta** (adding counts): The frontier only changes if the new
    ///   timestamp is NOT dominated by any current frontier element (it might be
    ///   a new minimal element).
    /// - **Negative delta** (removing counts): The frontier only changes if the
    ///   timestamp IS a current frontier element (removing it might reveal new
    ///   minimal elements behind it).
    ///
    /// Once a rebuild is detected as necessary, we skip the check for remaining
    /// updates (they'll all be processed in the rebuild anyway).
    pub fn update_iter<I>(&mut self, updates: I) -> Vec<(T, i64)>
    where
        I: IntoIterator<Item = (T, i64)>,
    {
        let mut rebuild_required = false;

        for (time, delta) in updates {
            if !rebuild_required {
                let dominated_by_frontier = self.frontier.iter().any(|f| f.less_equal(&time));
                if delta >= 0 {
                    // Adding counts: frontier changes only if the time is not
                    // dominated by (at or beyond) any current frontier element.
                    rebuild_required = !dominated_by_frontier;
                } else {
                    // Removing counts: frontier changes only if the time is at the
                    // frontier (could reduce its count to zero). "At the frontier"
                    // means dominated but not strictly — i.e., equal to a frontier element.
                    // Conservatively, we skip rebuild only when strictly beyond or strictly before.
                    let strictly_beyond = self.frontier.iter().any(|f| f.less_than(&time));
                    let before_frontier = !dominated_by_frontier;
                    rebuild_required = !(strictly_beyond || before_frontier);
                }
            }
            self.updates.update(time, delta);
        }

        if rebuild_required {
            self.rebuild();
        }

        self.changes.drain().collect()
    }

    /// Rebuilds the frontier from the current updates.
    ///
    /// # Correctness requirement
    ///
    /// This algorithm relies on `Ord` being a *linear extension* of `PartialOrder`:
    /// if `a.less_equal(&b)` then `a <= b` in `Ord`. This ensures that after sorting,
    /// we never encounter a timestamp that should evict an already-added frontier element.
    /// All built-in timestamp types (primitives and `Product`) satisfy this property.
    fn rebuild(&mut self) {
        // Record removal of current frontier elements.
        for time in self.frontier.drain(..) {
            self.changes.update(time, -1);
        }

        // Build new frontier from elements with positive count.
        // Since updates are sorted after compaction, we don't displace frontier elements.
        for (time, count) in self.updates.iter() {
            if *count > 0 && !self.frontier.iter().any(|f| f.less_equal(time)) {
                self.frontier.push(time.clone());
            }
        }

        // Record addition of new frontier elements.
        for time in &self.frontier {
            self.changes.update(time.clone(), 1);
        }
    }

    /// Returns the count for a specific timestamp.
    pub fn count_for(&self, time: &T) -> i64
    where
        T: PartialEq,
    {
        self.updates
            .unstable_internal_updates()
            .iter()
            .filter(|(t, _)| t == time)
            .map(|(_, c)| c)
            .sum()
    }
}

impl<T> Default for MutableAntichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + PartialOrder + Ord> From<Antichain<T>> for MutableAntichain<T> {
    fn from(antichain: Antichain<T>) -> Self {
        let mut result = MutableAntichain::new();
        result.update_iter(antichain.into_iter().map(|t| (t, 1)));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Product;

    // --- Basic operations ---

    #[test]
    fn new_is_empty() {
        let mac = MutableAntichain::<u64>::new();
        assert!(mac.is_empty());
        assert_eq!(mac.frontier(), &[]);
    }

    #[test]
    fn from_elem() {
        let mac = MutableAntichain::from_elem(5u64);
        assert!(!mac.is_empty());
        assert_eq!(mac.frontier(), &[5]);
    }

    #[test]
    fn clear_resets() {
        let mut mac = MutableAntichain::from_elem(5u64);
        mac.clear();
        assert!(mac.is_empty());
    }

    // --- Single updates ---

    #[test]
    fn single_insert_and_remove() {
        let mut mac = MutableAntichain::new();
        let changes = mac.update_iter(vec![(5u64, 1)]);
        assert_eq!(mac.frontier(), &[5]);
        assert_eq!(changes, vec![(5, 1)]);

        let changes = mac.update_iter(vec![(5u64, -1)]);
        assert!(mac.is_empty());
        assert_eq!(changes, vec![(5, -1)]);
    }

    // --- Frontier advancement ---

    #[test]
    fn frontier_advances_when_lower_removed() {
        let mut mac = MutableAntichain::new();
        mac.update_iter(vec![(1u64, 1), (2, 1)]);
        assert_eq!(mac.frontier(), &[1]);

        // Remove the lower element
        let changes = mac.update_iter(vec![(1, -1)]);
        assert_eq!(mac.frontier(), &[2]);
        // Should report: 1 removed, 2 added
        assert!(changes.contains(&(1, -1)));
        assert!(changes.contains(&(2, 1)));
    }

    #[test]
    fn frontier_retreats_when_lower_added() {
        let mut mac = MutableAntichain::from_elem(5u64);
        let changes = mac.update_iter(vec![(3, 1)]);
        assert_eq!(mac.frontier(), &[3]);
        assert!(changes.contains(&(5, -1)));
        assert!(changes.contains(&(3, 1)));
    }

    // --- Multiple counts ---

    #[test]
    fn multiple_counts_same_timestamp() {
        let mut mac = MutableAntichain::new();
        mac.update_iter(vec![(5u64, 1)]);
        mac.update_iter(vec![(5, 1)]);
        assert_eq!(mac.frontier(), &[5]);
        assert_eq!(mac.count_for(&5), 2);

        // Remove one — frontier should remain
        let changes = mac.update_iter(vec![(5, -1)]);
        assert_eq!(mac.frontier(), &[5]);
        assert!(changes.is_empty()); // frontier unchanged

        // Remove last — frontier should clear
        let changes = mac.update_iter(vec![(5, -1)]);
        assert!(mac.is_empty());
        assert_eq!(changes, vec![(5, -1)]);
    }

    // --- Product timestamps (partial order) ---

    #[test]
    fn product_frontier_incomparable() {
        let mut mac = MutableAntichain::new();
        mac.update_iter(vec![(Product::new(1u64, 3u64), 1), (Product::new(3, 1), 1)]);
        assert_eq!(mac.frontier().len(), 2);
    }

    #[test]
    fn product_frontier_eviction() {
        let mut mac = MutableAntichain::new();
        mac.update_iter(vec![(Product::new(2u64, 2u64), 1), (Product::new(3, 3), 1)]);
        // Frontier should be {(2,2)} since (2,2) ≤ (3,3)
        assert_eq!(mac.frontier(), &[Product::new(2, 2)]);

        // Add (1, 1) which dominates (2, 2)
        mac.update_iter(vec![(Product::new(1, 1), 1)]);
        assert_eq!(mac.frontier(), &[Product::new(1, 1)]);
    }

    // --- less_than / less_equal ---

    #[test]
    fn less_than_and_less_equal() {
        let mac = MutableAntichain::from_elem(5u64);
        assert!(!mac.less_than(&4));
        assert!(!mac.less_than(&5));
        assert!(mac.less_than(&6));
        assert!(mac.less_equal(&5));
        assert!(!mac.less_equal(&4));
    }

    // --- Update beyond frontier is no-op for frontier ---

    #[test]
    fn update_beyond_frontier_no_rebuild() {
        let mut mac = MutableAntichain::from_elem(1u64);
        // Adding something strictly greater should not change the frontier
        let changes = mac.update_iter(vec![(5, 1)]);
        assert_eq!(mac.frontier(), &[1]);
        assert!(changes.is_empty());
    }

    // --- From<Antichain> ---

    #[test]
    fn from_antichain() {
        let ac: Antichain<u64> = vec![3, 5].into_iter().collect();
        let mac: MutableAntichain<u64> = ac.into();
        assert_eq!(mac.frontier(), &[3]);
    }

    // --- Batch update ---

    #[test]
    fn batch_update() {
        let mut mac = MutableAntichain::from_elem(1u64);
        let changes = mac.update_iter(vec![(1, -1), (2, 7)]);
        assert_eq!(mac.frontier(), &[2]);
        assert!(changes.contains(&(1, -1)));
        assert!(changes.contains(&(2, 1)));
    }

    // --- count_for ---

    #[test]
    fn count_for_returns_accumulated() {
        let mut mac = MutableAntichain::new();
        mac.update_iter(vec![(5u64, 3), (5, 2)]);
        assert_eq!(mac.count_for(&5), 5);
        assert_eq!(mac.count_for(&3), 0);
    }
}
