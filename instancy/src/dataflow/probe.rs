//! Probe handle for observing dataflow frontier progress.
//!
//! A [`ProbeHandle`] provides external visibility into the progress of a
//! dataflow at a specific point. It tracks the frontier (the set of timestamps
//! that may still appear) at the probe's location in the graph.
//!
//! Probes are inserted into the dataflow graph as pass-through operators that
//! observe but do not modify data flow. They are useful for:
//! - Waiting until a dataflow has processed all data up to a given timestamp
//! - Monitoring progress for external scheduling decisions
//! - Testing that frontiers advance correctly

use std::sync::{Arc, Mutex};

use crate::progress::frontier::Antichain;
use crate::progress::timestamp::Timestamp;

/// A handle for observing the progress frontier at a specific point in the dataflow.
///
/// The frontier represents the set of timestamps that may still appear at this
/// point. When the frontier advances past a timestamp `t`, it guarantees no more
/// data at time `t` (or any time ≤ t) will arrive.
///
/// Thread-safe: can be shared across threads via `Clone`.
#[derive(Clone, Debug)]
pub struct ProbeHandle<T: Timestamp> {
    /// Shared frontier state, updated by the executor during progress propagation.
    frontier: Arc<Mutex<Antichain<T>>>,
}

impl<T: Timestamp> ProbeHandle<T> {
    /// Create a new probe handle with an initial frontier at `T::minimum()`.
    pub fn new() -> Self {
        Self {
            frontier: Arc::new(Mutex::new(Antichain::from_elem(T::minimum()))),
        }
    }

    /// Returns `true` if the frontier has advanced past `time`.
    ///
    /// When this returns `true`, no more data at timestamps ≤ `time` will arrive
    /// at this point in the dataflow.
    pub fn done_with(&self, time: &T) -> bool {
        let frontier = self.frontier.lock().unwrap_or_else(|e| e.into_inner());
        !frontier.less_equal(time)
    }

    /// Returns the current frontier as a snapshot.
    pub fn frontier(&self) -> Antichain<T> {
        self.frontier.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Returns `true` if the frontier is empty (all work complete).
    pub fn is_done(&self) -> bool {
        self.frontier.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    /// Update the frontier. Called by the executor during progress propagation.
    pub(crate) fn update_frontier(&self, new_frontier: &Antichain<T>) {
        let mut frontier = self.frontier.lock().unwrap_or_else(|e| e.into_inner());
        *frontier = new_frontier.clone();
    }
}

impl<T: Timestamp> Default for ProbeHandle<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_frontier_at_minimum() {
        let probe = ProbeHandle::<u64>::new();
        assert!(!probe.is_done());
        assert!(!probe.done_with(&0));
        assert!(probe.frontier().less_equal(&0));
    }

    #[test]
    fn done_with_after_advance() {
        let probe = ProbeHandle::<u64>::new();
        // Advance frontier past 0 to 5
        probe.update_frontier(&Antichain::from_elem(5));
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&4));
        assert!(!probe.done_with(&5));
        assert!(!probe.done_with(&10));
    }

    #[test]
    fn empty_frontier_means_done() {
        let probe = ProbeHandle::<u64>::new();
        probe.update_frontier(&Antichain::new());
        assert!(probe.is_done());
        assert!(probe.done_with(&0));
        assert!(probe.done_with(&u64::MAX));
    }

    #[test]
    fn clone_shares_state() {
        let probe = ProbeHandle::<u64>::new();
        let probe2 = probe.clone();
        probe.update_frontier(&Antichain::from_elem(10));
        assert!(probe2.done_with(&5));
    }
}
