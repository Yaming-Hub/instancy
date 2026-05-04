//! ChangeBatch — a collection of `(T, i64)` updates with lazy compaction.
//!
//! A `ChangeBatch` accumulates updates of the form `(T, i64)` and can consolidate
//! them by sorting and summing, removing entries whose accumulated value is zero.
//!
//! # Role in the progress tracking pipeline
//!
//! `ChangeBatch` is the fundamental building block that flows capability changes
//! through the entire progress system:
//!
//! ```text
//! Capability::new()/drop()  →  ProgressReporter (wraps ChangeBatch)
//!                           →  ProgressTracker::collect_operator_progress()
//!                           →  Tracker::update_source() (into ChangeBatch)
//!                           →  drain_pending_changes() (into MutableAntichain)
//!                           →  pushed_changes (ChangeBatch of frontier deltas)
//! ```
//!
//! # Lazy compaction strategy
//!
//! Updates are appended eagerly (O(1)) but compaction (sort + consolidate + remove
//! zeros) is deferred. The `clean` field marks how much of the `updates` vec has
//! already been compacted. Compaction is triggered automatically when the dirty
//! portion exceeds half the total size (see `maintain_bounds`),
//! or explicitly when data is read (via `iter()`, `drain()`, etc.). This amortizes
//! the O(n log n) sort cost across many O(1) appends.

/// A collection of `(T, i64)` updates with lazy compaction.
///
/// Updates are appended eagerly but compaction (sorting + consolidation) is deferred
/// until the data is read or the dirty portion grows large. This amortizes the cost
/// of compaction across many updates.
#[derive(Clone, Debug)]
pub struct ChangeBatch<T> {
    /// The raw updates buffer. Entries `[0..clean)` are compacted (sorted, consolidated,
    /// no zeros). Entries `[clean..)` are "dirty" — unprocessed appends that may contain
    /// duplicates or zero-sum pairs.
    updates: Vec<(T, i64)>,
    /// The length of the prefix of `updates` known to be compacted.
    /// Invariant: `clean <= updates.len()`. When `clean == updates.len()`, all data
    /// is compacted. When `clean == 0`, everything is dirty.
    clean: usize,
}

impl<T> ChangeBatch<T> {
    /// Creates a new empty `ChangeBatch`.
    pub fn new() -> Self {
        ChangeBatch {
            updates: Vec::new(),
            clean: 0,
        }
    }

    /// Creates a new empty `ChangeBatch` with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        ChangeBatch {
            updates: Vec::with_capacity(capacity),
            clean: 0,
        }
    }

    /// Removes all updates.
    pub fn clear(&mut self) {
        self.updates.clear();
        self.clean = 0;
    }

    /// Returns `true` if the batch has pending (non-compacted) updates.
    pub fn is_dirty(&self) -> bool {
        self.updates.len() > self.clean
    }

    /// Exposes the raw internal updates without compaction.
    ///
    /// This is intended for internal use (e.g., `MutableAntichain::count_for`).
    /// The returned data may contain duplicates or zero-sum entries.
    pub fn unstable_internal_updates(&self) -> &[(T, i64)] {
        &self.updates
    }
}

impl<T: Ord> ChangeBatch<T> {
    /// Creates a new `ChangeBatch` with a single entry.
    pub fn new_from(key: T, val: i64) -> Self {
        let mut batch = Self::new();
        batch.update(key, val);
        batch
    }

    /// Adds an update for `item` with the given `value`.
    ///
    /// The update is appended without compaction. Compaction happens lazily
    /// when the dirty portion exceeds half the total.
    #[inline]
    pub fn update(&mut self, item: T, value: i64) {
        self.updates.push((item, value));
        self.maintain_bounds();
    }

    /// Appends a sequence of updates.
    #[inline]
    pub fn extend<I: IntoIterator<Item = (T, i64)>>(&mut self, iter: I) {
        self.updates.extend(iter);
        self.maintain_bounds();
    }

    /// Compacts the internal representation.
    ///
    /// Sorts by key, sums values for identical keys, and removes zero entries.
    pub fn compact(&mut self) {
        if self.clean < self.updates.len() && self.updates.len() > 1 {
            self.updates.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            for i in 0..self.updates.len() - 1 {
                if self.updates[i].0 == self.updates[i + 1].0 {
                    self.updates[i + 1].1 += self.updates[i].1;
                    self.updates[i].1 = 0;
                }
            }
            self.updates.retain(|x| x.1 != 0);
        }
        self.clean = self.updates.len();
    }

    /// Returns `true` if all keys have value zero after compaction.
    pub fn is_empty(&mut self) -> bool {
        if self.clean > self.updates.len() / 2 {
            // More than half is already compacted and non-zero, so definitely not empty.
            false
        } else {
            self.compact();
            self.updates.is_empty()
        }
    }

    /// Returns `true` if the batch is definitely empty without mutating.
    ///
    /// This is a conservative check: it returns `true` only when the internal
    /// storage is empty (no pending updates at all). If there are uncompacted
    /// entries that might cancel, this returns `false` even though `is_empty()`
    /// (which compacts) might return `true`.
    pub fn is_empty_clean(&self) -> bool {
        self.updates.is_empty()
    }

    /// Returns the number of distinct entries after compaction.
    pub fn len(&mut self) -> usize {
        self.compact();
        self.updates.len()
    }

    /// Iterates over the compacted `(key, value)` pairs.
    pub fn iter(&mut self) -> std::slice::Iter<'_, (T, i64)> {
        self.compact();
        self.updates.iter()
    }

    /// Drains all compacted entries from the batch.
    pub fn drain(&mut self) -> std::vec::Drain<'_, (T, i64)> {
        self.compact();
        self.clean = 0;
        self.updates.drain(..)
    }

    /// Consumes the batch and returns the compacted `Vec<(T, i64)>`.
    pub fn into_inner(mut self) -> Vec<(T, i64)> {
        self.compact();
        self.updates
    }

    /// Drains self into another `ChangeBatch`.
    ///
    /// Optimized: when `other` is empty, this is a swap instead of a copy.
    pub fn drain_into(&mut self, other: &mut ChangeBatch<T>)
    where
        T: Clone,
    {
        if other.updates.is_empty() {
            std::mem::swap(self, other);
        } else {
            other.extend(self.updates.drain(..));
            self.clean = 0;
        }
    }

    /// Triggers compaction if the dirty portion exceeds half the total.
    ///
    /// The thresholds (minimum 32 entries, dirty ≥ half of total) are heuristic:
    /// - **32 minimum**: Don't bother compacting tiny batches — the overhead of
    ///   sorting isn't worth it for small N.
    /// - **Half dirty**: Ensures amortized O(1) cost per update. Each update is
    ///   "charged" for the compaction it eventually triggers. Since we compact when
    ///   half the entries are dirty, each dirty entry pays for sorting ~2 entries.
    fn maintain_bounds(&mut self) {
        if self.updates.len() > 32 && self.updates.len() / 2 >= self.clean {
            self.compact();
        }
    }
}

impl<T> Default for ChangeBatch<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Basic operations ---

    #[test]
    fn new_is_empty() {
        let mut batch = ChangeBatch::<u64>::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn new_from_single() {
        let mut batch = ChangeBatch::new_from(17u64, 1);
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        let items: Vec<_> = batch.iter().cloned().collect();
        assert_eq!(items, vec![(17, 1)]);
    }

    #[test]
    fn update_accumulates() {
        let mut batch = ChangeBatch::new();
        batch.update(5u64, 3);
        batch.update(5, 2);
        let items: Vec<_> = batch.iter().cloned().collect();
        assert_eq!(items, vec![(5, 5)]);
    }

    #[test]
    fn update_cancellation() {
        let mut batch = ChangeBatch::new_from(17u64, 1);
        batch.update(17, -1);
        assert!(batch.is_empty());
    }

    #[test]
    fn update_multiple_keys() {
        let mut batch = ChangeBatch::new();
        batch.update(3u64, 1);
        batch.update(1, 2);
        batch.update(2, 3);
        let mut items: Vec<_> = batch.iter().cloned().collect();
        items.sort();
        assert_eq!(items, vec![(1, 2), (2, 3), (3, 1)]);
    }

    // --- extend ---

    #[test]
    fn extend_from_iter() {
        let mut batch = ChangeBatch::new_from(17u64, 1);
        batch.extend(vec![(17, -1)]);
        assert!(batch.is_empty());
    }

    #[test]
    fn extend_multiple() {
        let mut batch = ChangeBatch::new();
        batch.extend(vec![(1u64, 1), (2, 1), (1, 2)]);
        let mut items: Vec<_> = batch.iter().cloned().collect();
        items.sort();
        assert_eq!(items, vec![(1, 3), (2, 1)]);
    }

    // --- drain ---

    #[test]
    fn drain_empties_batch() {
        let mut batch = ChangeBatch::new_from(5u64, 3);
        let drained: Vec<_> = batch.drain().collect();
        assert_eq!(drained, vec![(5, 3)]);
        assert!(batch.is_empty());
    }

    // --- into_inner ---

    #[test]
    fn into_inner_returns_compacted() {
        let mut batch = ChangeBatch::new();
        batch.update(3u64, 1);
        batch.update(1, 2);
        batch.update(3, -1);
        let inner = batch.into_inner();
        assert_eq!(inner, vec![(1, 2)]);
    }

    // --- drain_into ---

    #[test]
    fn drain_into_empty_target() {
        let mut src = ChangeBatch::new_from(5u64, 1);
        let mut dst = ChangeBatch::new();
        src.drain_into(&mut dst);
        assert!(src.is_empty());
        let items: Vec<_> = dst.iter().cloned().collect();
        assert_eq!(items, vec![(5, 1)]);
    }

    #[test]
    fn drain_into_nonempty_target() {
        let mut src = ChangeBatch::new_from(5u64, 1);
        let mut dst = ChangeBatch::new_from(3u64, 2);
        src.drain_into(&mut dst);
        assert!(src.is_empty());
        let mut items: Vec<_> = dst.iter().cloned().collect();
        items.sort();
        assert_eq!(items, vec![(3, 2), (5, 1)]);
    }

    // --- clear ---

    #[test]
    fn clear() {
        let mut batch = ChangeBatch::new_from(5u64, 1);
        batch.clear();
        assert!(batch.is_empty());
    }

    // --- compaction stress ---

    #[test]
    fn many_cancellations_compact() {
        let mut batch = ChangeBatch::new();
        for _ in 0..100 {
            batch.update(42u64, 1);
            batch.update(42, -1);
        }
        assert!(batch.is_empty());
    }

    #[test]
    fn maintain_bounds_triggers_compaction() {
        let mut batch = ChangeBatch::new();
        // Insert enough to trigger maintain_bounds (>32 elements, half dirty)
        for i in 0..50u64 {
            batch.update(i, 1);
        }
        // After maintain_bounds, internal state should be compact
        assert_eq!(batch.len(), 50);
    }

    // --- with_capacity ---

    #[test]
    fn with_capacity() {
        let batch = ChangeBatch::<u64>::with_capacity(100);
        assert_eq!(batch.updates.capacity(), 100);
    }

    // --- is_dirty ---

    #[test]
    fn is_dirty_after_update() {
        let mut batch = ChangeBatch::new();
        assert!(!batch.is_dirty());
        batch.update(1u64, 1);
        assert!(batch.is_dirty());
        batch.compact();
        assert!(!batch.is_dirty());
    }
}
