//! Capabilities for progress tracking.
//!
//! A [`Capability`] is a permit that allows an operator to produce data at a
//! specific timestamp on a specific output port. The progress tracking system
//! uses capabilities to determine which timestamps can still appear in the
//! dataflow, enabling downstream operators to know when a timestamp is "complete."
//!
//! # Invariants
//!
//! - Creating a capability increments `+1` in the shared [`ProgressReporter`].
//! - Dropping a capability decrements `-1`.
//! - Cloning a capability increments `+1`.
//! - Downgrading atomically does `-1` old, `+1` new in a single lock.
//! - A capability can only be delayed/downgraded to a time `>=` in the partial order.

use std::fmt;
use std::ops::Deref;

use crate::error::{Error, ProgressError, Result};
use crate::progress::frontier::Antichain;
use crate::progress::operate::ProgressReporter;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// Capability<T>
// ---------------------------------------------------------------------------

/// A permit to produce data at a specific timestamp.
///
/// Capabilities track outstanding work in the progress system. As long as any
/// capability for timestamp `t` exists on output port `p`, the system knows
/// that data at time `t` may still appear on that port.
///
/// # Thread safety
///
/// Capabilities use [`ProgressReporter`] (backed by `Arc<Mutex<>>`) so they
/// can be safely sent across async task boundaries.
pub struct Capability<T: Timestamp> {
    time: T,
    reporter: ProgressReporter<T>,
}

impl<T: Timestamp> Capability<T> {
    /// Creates a new capability at the given time, incrementing the progress counter.
    ///
    /// This is `pub(crate)` because capabilities should only be created by the
    /// runtime (for input operators) or derived from existing capabilities.
    pub(crate) fn new(time: T, reporter: ProgressReporter<T>) -> Self {
        reporter.update(time.clone(), 1);
        Self { time, reporter }
    }

    /// Returns the timestamp this capability permits.
    pub fn time(&self) -> &T {
        &self.time
    }

    /// Creates a new capability at `new_time`, which must be `>=` this capability's
    /// time in the partial order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Progress`] if `new_time` is not `>=` the current time
    /// (including incomparable times in a partial order).
    pub fn delayed(&self, new_time: &T) -> Result<Self> {
        if !self.time.less_equal(new_time) {
            return Err(Error::Progress(ProgressError::TimeNotAdvanced {
                from: format!("{:?}", self.time),
                to: format!("{:?}", new_time),
            }));
        }
        Ok(Self::new(new_time.clone(), self.reporter.clone()))
    }

    /// Like [`delayed`](Self::delayed) but returns `None` instead of an error.
    pub fn try_delayed(&self, new_time: &T) -> Option<Self> {
        self.delayed(new_time).ok()
    }

    /// Downgrades this capability in-place to `new_time`.
    ///
    /// Atomically records `-1` at the old time and `+1` at the new time.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Progress`] if `new_time` is not `>=` the current time.
    pub fn downgrade(&mut self, new_time: &T) -> Result<()> {
        if !self.time.less_equal(new_time) {
            return Err(Error::Progress(ProgressError::TimeNotAdvanced {
                from: format!("{:?}", self.time),
                to: format!("{:?}", new_time),
            }));
        }
        // Atomic paired update: no intermediate state visible.
        self.reporter.downgrade(self.time.clone(), new_time.clone());
        self.time = new_time.clone();
        Ok(())
    }
}

impl<T: Timestamp> Clone for Capability<T> {
    fn clone(&self) -> Self {
        Self::new(self.time.clone(), self.reporter.clone())
    }
}

impl<T: Timestamp> Drop for Capability<T> {
    fn drop(&mut self) {
        self.reporter.update(self.time.clone(), -1);
    }
}

/// Implements `Deref` to allow using a capability as a timestamp reference.
/// This enables ergonomic patterns like `cap.less_equal(&time)` without
/// explicit `cap.time()` calls, since `Deref` provides access to all `T` methods.
impl<T: Timestamp> Deref for Capability<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.time
    }
}

impl<T: Timestamp> fmt::Debug for Capability<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Capability({:?})", self.time)
    }
}

impl<T: Timestamp + fmt::Display> fmt::Display for Capability<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Capability({})", self.time)
    }
}

// ---------------------------------------------------------------------------
// CapabilitySet<T>
// ---------------------------------------------------------------------------

/// A set of capabilities maintained as an antichain (no two comparable).
///
/// This is the standard way operators manage multiple capabilities. The set
/// automatically maintains the antichain property: inserting a dominated
/// capability drops the dominating ones, and vice versa.
pub struct CapabilitySet<T: Timestamp> {
    elements: Vec<Capability<T>>,
}

impl<T: Timestamp> CapabilitySet<T> {
    /// Creates an empty capability set.
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Creates a set from a single capability.
    pub fn from_elem(cap: Capability<T>) -> Self {
        let mut set = Self::new();
        set.insert(cap);
        set
    }

    /// Inserts a capability, maintaining the antichain property.
    ///
    /// - If an existing capability dominates `cap` (`existing <= cap`), `cap` is dropped.
    /// - Otherwise, all capabilities dominated by `cap` are removed, and `cap` is inserted.
    pub fn insert(&mut self, cap: Capability<T>) {
        // If any existing element dominates the new one, don't insert.
        if self.elements.iter().any(|e| e.time.less_equal(&cap.time)) {
            return; // cap is dropped here, decrementing its counter
        }
        // Remove elements dominated by the new one.
        self.elements.retain(|e| !cap.time.less_equal(&e.time));
        self.elements.push(cap);
    }

    /// Creates a new capability at `time` derived from a dominating capability in the set.
    ///
    /// Finds some capability `c` where `c.time <= time` and calls `c.delayed(time)`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Progress`] if no capability in the set dominates `time`.
    pub fn delayed(&self, time: &T) -> Result<Capability<T>> {
        self.try_delayed(time).ok_or_else(|| {
            Error::Progress(ProgressError::NoDominatingCapability {
                time: format!("{:?}", time),
            })
        })
    }

    /// Like [`delayed`](Self::delayed) but returns `None` if no dominating capability exists.
    pub fn try_delayed(&self, time: &T) -> Option<Capability<T>> {
        self.elements
            .iter()
            .find(|c| c.time.less_equal(time))
            .and_then(|c| c.try_delayed(time))
    }

    /// Replaces the set with capabilities at the given frontier times.
    ///
    /// Each new time must be dominated by some existing capability. The set is
    /// replaced atomically (old capabilities are dropped after new ones are created).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Progress`] if any frontier time is not dominated by an
    /// existing capability.
    pub fn downgrade(&mut self, frontier: impl IntoIterator<Item = T>) -> Result<()> {
        let frontier_times: Vec<T> = frontier.into_iter().collect();

        // Validate: each new time must be dominated by some existing cap.
        for new_time in &frontier_times {
            if !self.elements.iter().any(|c| c.time.less_equal(new_time)) {
                return Err(Error::Progress(ProgressError::NoDominatingCapability {
                    time: format!("{:?}", new_time),
                }));
            }
        }

        // Create new capabilities from the original set before modifying it.
        let mut new_caps: Vec<Capability<T>> = Vec::with_capacity(frontier_times.len());
        for t in frontier_times {
            let cap = self
                .elements
                .iter()
                .find(|c| c.time.less_equal(&t))
                // SAFETY: frontier_times validated against existing capabilities in the loop above
                .expect("validated above")
                .delayed(&t)
                // SAFETY: frontier_times validated against existing capabilities in the loop above
                .expect("validated above");
            new_caps.push(cap);
        }

        // Replace the set. Old caps dropped here (decrementing counters).
        self.elements.clear();
        for cap in new_caps {
            self.insert(cap);
        }

        Ok(())
    }

    /// Returns `true` if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the number of capabilities in the set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns the current frontier as an `Antichain` of timestamps.
    pub fn frontier(&self) -> Antichain<T> {
        Antichain::from_elem_iter(self.elements.iter().map(|c| c.time.clone()))
    }

    /// Iterates over the capabilities.
    pub fn iter(&self) -> impl Iterator<Item = &Capability<T>> {
        self.elements.iter()
    }

    /// Retains only capabilities satisfying the predicate.
    pub fn retain<F: FnMut(&Capability<T>) -> bool>(&mut self, f: F) {
        self.elements.retain(f);
    }
}

impl<T: Timestamp> Default for CapabilitySet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Timestamp> fmt::Debug for CapabilitySet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(self.elements.iter().map(|c| c.time()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::Product;

    fn reporter() -> ProgressReporter<u64> {
        ProgressReporter::new()
    }

    fn reporter_product() -> ProgressReporter<Product<u64, u64>> {
        ProgressReporter::new()
    }

    // --- Capability basic ---

    #[test]
    fn capability_creates_and_drops_progress() {
        let r = reporter();
        {
            let _cap = Capability::new(10u64, r.clone());
            let changes = r.drain();
            assert_eq!(changes, vec![(10, 1)]);
        }
        let changes = r.drain();
        assert_eq!(changes, vec![(10, -1)]);
    }

    #[test]
    fn capability_clone_increments() {
        let r = reporter();
        let cap = Capability::new(5u64, r.clone());
        let _clone = cap.clone();
        let changes = r.drain();
        // Two +1 updates
        let total: i64 = changes.into_iter().map(|(_, d)| d).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn capability_delayed_valid() {
        let r = reporter();
        let cap = Capability::new(10u64, r.clone());
        let delayed = cap.delayed(&20).unwrap();
        assert_eq!(*delayed.time(), 20);
    }

    #[test]
    fn capability_delayed_same_time() {
        let r = reporter();
        let cap = Capability::new(10u64, r.clone());
        let delayed = cap.delayed(&10).unwrap();
        assert_eq!(*delayed.time(), 10);
    }

    #[test]
    fn capability_delayed_earlier_fails() {
        let r = reporter();
        let cap = Capability::new(10u64, r.clone());
        assert!(cap.delayed(&5).is_err());
    }

    #[test]
    fn capability_delayed_incomparable_fails() {
        let r = reporter_product();
        let cap = Capability::new(Product::new(1, 2), r.clone());
        // (2, 1) is incomparable with (1, 2)
        assert!(cap.delayed(&Product::new(2, 1)).is_err());
    }

    #[test]
    fn capability_downgrade_valid() {
        let r = reporter();
        let mut cap = Capability::new(10u64, r.clone());
        r.drain(); // clear initial +1
        cap.downgrade(&20).unwrap();
        assert_eq!(*cap.time(), 20);
        let changes = r.drain();
        // Should have (-1 at 10, +1 at 20)
        let mut sorted = changes.clone();
        sorted.sort();
        assert_eq!(sorted, vec![(10, -1), (20, 1)]);
    }

    #[test]
    fn capability_downgrade_earlier_fails() {
        let r = reporter();
        let mut cap = Capability::new(10u64, r.clone());
        assert!(cap.downgrade(&5).is_err());
        assert_eq!(*cap.time(), 10); // unchanged
    }

    #[test]
    fn capability_deref() {
        let r = reporter();
        let cap = Capability::new(42u64, r.clone());
        let val: &u64 = &cap;
        assert_eq!(*val, 42);
    }

    #[test]
    fn capability_debug_display() {
        let r = reporter();
        let cap = Capability::new(7u64, r.clone());
        assert_eq!(format!("{:?}", cap), "Capability(7)");
        assert_eq!(format!("{}", cap), "Capability(7)");
    }

    #[test]
    fn capability_net_zero_after_lifecycle() {
        let r = reporter();
        {
            let cap = Capability::new(1u64, r.clone());
            let _c2 = cap.clone();
            let _c3 = cap.delayed(&5).unwrap();
        }
        let changes = r.drain();
        let total: i64 = changes.into_iter().map(|(_, d)| d).sum();
        assert_eq!(total, 0, "all caps dropped, net should be zero");
    }

    // --- CapabilitySet ---

    #[test]
    fn capability_set_insert_maintains_antichain() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(10u64, r.clone()));
        set.insert(Capability::new(5u64, r.clone()));
        // 10 is dominated by 5, so only 5 remains
        assert_eq!(set.len(), 1);
        assert_eq!(*set.iter().next().unwrap().time(), 5);
    }

    #[test]
    fn capability_set_insert_incomparable() {
        let r = reporter_product();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(Product::new(1, 2), r.clone()));
        set.insert(Capability::new(Product::new(2, 1), r.clone()));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn capability_set_insert_dominated_is_noop() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(5u64, r.clone()));
        set.insert(Capability::new(10u64, r.clone()));
        assert_eq!(set.len(), 1);
        assert_eq!(*set.iter().next().unwrap().time(), 5);
    }

    #[test]
    fn capability_set_delayed() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(5u64, r.clone()));
        let cap = set.delayed(&10).unwrap();
        assert_eq!(*cap.time(), 10);
    }

    #[test]
    fn capability_set_delayed_no_dominator() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(10u64, r.clone()));
        assert!(set.delayed(&5).is_err());
    }

    #[test]
    fn capability_set_downgrade() {
        // Use Product timestamps to get incomparable times in the downgrade.
        let r = reporter_product();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(Product::new(1, 1), r.clone()));
        // (2, 1) and (1, 2) are both >= (1, 1) and incomparable with each other.
        set.downgrade(vec![Product::new(2, 1), Product::new(1, 2)])
            .unwrap();
        assert_eq!(set.len(), 2);
        let frontier = set.frontier();
        assert!(frontier.elements().contains(&Product::new(2, 1)));
        assert!(frontier.elements().contains(&Product::new(1, 2)));
    }

    #[test]
    fn capability_set_downgrade_total_order() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(5u64, r.clone()));
        // With total order, 10 dominates 15, so antichain keeps only 10.
        set.downgrade(vec![10, 15]).unwrap();
        assert_eq!(set.len(), 1);
        assert_eq!(*set.iter().next().unwrap().time(), 10);
    }

    #[test]
    fn capability_set_downgrade_invalid() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(10u64, r.clone()));
        // Can't downgrade to 5 (not dominated)
        assert!(set.downgrade(vec![5]).is_err());
    }

    #[test]
    fn capability_set_frontier() {
        let r = reporter_product();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(Product::new(1, 3), r.clone()));
        set.insert(Capability::new(Product::new(3, 1), r.clone()));
        let frontier = set.frontier();
        assert_eq!(frontier.len(), 2);
    }

    #[test]
    fn capability_set_retain() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(5u64, r.clone()));
        set.insert(Capability::new(10u64, r.clone()));
        // 10 is dominated so set only has 5
        set.retain(|c| *c.time() > 3);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn capability_set_empty() {
        let set = CapabilitySet::<u64>::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn capability_set_from_elem() {
        let r = reporter();
        let set = CapabilitySet::from_elem(Capability::new(7u64, r.clone()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn capability_set_debug() {
        let r = reporter();
        let mut set = CapabilitySet::new();
        set.insert(Capability::new(3u64, r.clone()));
        let dbg = format!("{:?}", set);
        assert!(dbg.contains("3"));
    }
}
