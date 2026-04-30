//! Partition strategies (Pact) for routing data between operators.
//!
//! A `PartitionStrategy` determines how data envelopes are distributed
//! from an operator's output to downstream operator inputs.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Describes how data is routed between operators across logical workers.
///
/// This is a **logical** routing strategy — it defines the data exchange pattern
/// in graph terms (which logical worker receives each record). The physical
/// delivery mechanism (in-memory buffer, TCP, etc.) is resolved separately
/// by the transport layer.
#[derive(Debug, Clone)]
pub enum PartitionStrategy<D> {
    /// No shuffle — data stays on the same worker.
    /// This is the default for operators within the same execution region.
    Pipeline,

    /// Hash-based exchange: routes each record to a worker determined by a key function.
    /// Used for operations that need all records with the same key on the same worker
    /// (e.g., group-by, join).
    Exchange(ExchangeFn<D>),

    /// Round-robin distribution across all target workers.
    /// Used at execution region boundaries to rebalance load evenly.
    Rebalance,

    /// Funnel all data to a single worker (worker index 0 in the target region).
    /// Used before global aggregations.
    Gather,

    /// Fan out each record to all workers in the target region.
    /// Used for broadcasting reference data or configuration.
    Broadcast,

    /// Fan out each record to all workers in the same process.
    /// Useful for local reference data that doesn't need network transfer.
    BroadcastLocal,
}

/// A boxed function that extracts a routing key from a data record.
/// The key is hashed to determine the target worker.
pub struct ExchangeFn<D> {
    func: Arc<dyn Fn(&D) -> u64 + Send + Sync>,
    description: String,
}

impl<D> ExchangeFn<D> {
    /// Create a new exchange function from a closure that returns a hash key.
    pub fn new(description: impl Into<String>, func: impl Fn(&D) -> u64 + Send + Sync + 'static) -> Self {
        Self {
            func: Arc::new(func),
            description: description.into(),
        }
    }

    /// Create an exchange function from a hashable key extractor.
    pub fn by_key<K: Hash>(
        description: impl Into<String>,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Self {
        Self {
            func: Arc::new(move |d| {
                let key = key_fn(d);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish()
            }),
            description: description.into(),
        }
    }

    /// Route a record: returns the hash value used to select a target worker.
    pub fn route(&self, data: &D) -> u64 {
        (self.func)(data)
    }

    /// Given a hash value and target worker count, returns the target worker index.
    ///
    /// # Panics
    /// Panics if `num_workers` is 0.
    pub fn target_worker(hash: u64, num_workers: usize) -> usize {
        debug_assert!(num_workers > 0, "num_workers must be > 0");
        (hash % num_workers as u64) as usize
    }
}

impl<D> fmt::Debug for ExchangeFn<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExchangeFn({})", self.description)
    }
}

impl<D> Clone for ExchangeFn<D> {
    fn clone(&self) -> Self {
        Self {
            func: Arc::clone(&self.func),
            description: self.description.clone(),
        }
    }
}

impl<D> PartitionStrategy<D> {
    /// Create an exchange strategy with a key-based routing function.
    pub fn exchange_by_key<K: Hash>(
        description: impl Into<String>,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> Self {
        PartitionStrategy::Exchange(ExchangeFn::by_key(description, key_fn))
    }

    /// Create an exchange strategy with a direct hash function.
    pub fn exchange(
        description: impl Into<String>,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> Self {
        PartitionStrategy::Exchange(ExchangeFn::new(description, hash_fn))
    }

    /// Returns a human-readable name for this strategy.
    pub fn name(&self) -> &str {
        match self {
            PartitionStrategy::Pipeline => "Pipeline",
            PartitionStrategy::Exchange(_) => "Exchange",
            PartitionStrategy::Rebalance => "Rebalance",
            PartitionStrategy::Gather => "Gather",
            PartitionStrategy::Broadcast => "Broadcast",
            PartitionStrategy::BroadcastLocal => "BroadcastLocal",
        }
    }

    /// Returns true if this strategy requires data movement between workers.
    pub fn requires_exchange(&self) -> bool {
        !matches!(self, PartitionStrategy::Pipeline)
    }
}

impl<D> fmt::Display for PartitionStrategy<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionStrategy::Pipeline => write!(f, "Pipeline"),
            PartitionStrategy::Exchange(ef) => write!(f, "Exchange({})", ef.description),
            PartitionStrategy::Rebalance => write!(f, "Rebalance"),
            PartitionStrategy::Gather => write!(f, "Gather"),
            PartitionStrategy::Broadcast => write!(f, "Broadcast"),
            PartitionStrategy::BroadcastLocal => write!(f, "BroadcastLocal"),
        }
    }
}

/// Determines how records are assigned to target workers during routing.
pub struct Router<D> {
    strategy: PartitionStrategy<D>,
    num_targets: usize,
    /// Counter for round-robin routing.
    rr_counter: usize,
}

impl<D> Router<D> {
    /// Create a new router with the given strategy and target worker count.
    ///
    /// # Panics
    /// Panics if `num_targets` is 0.
    pub fn new(strategy: PartitionStrategy<D>, num_targets: usize) -> Self {
        assert!(num_targets > 0, "Router requires at least one target worker");
        Self {
            strategy,
            num_targets,
            rr_counter: 0,
        }
    }

    /// Route a single record. Returns the list of target worker indices.
    ///
    /// For most strategies this returns a single target; for Broadcast
    /// it returns all targets.
    pub fn route(&mut self, record: &D) -> Vec<usize> {
        match &self.strategy {
            PartitionStrategy::Pipeline => {
                // In pipeline mode, this should not really be called for routing;
                // data stays on the same worker. Return worker 0 as placeholder.
                vec![0]
            }
            PartitionStrategy::Exchange(ef) => {
                let hash = ef.route(record);
                vec![ExchangeFn::<D>::target_worker(hash, self.num_targets)]
            }
            PartitionStrategy::Rebalance => {
                let target = self.rr_counter % self.num_targets;
                self.rr_counter = self.rr_counter.wrapping_add(1);
                vec![target]
            }
            PartitionStrategy::Gather => {
                vec![0]
            }
            PartitionStrategy::Broadcast => {
                (0..self.num_targets).collect()
            }
            PartitionStrategy::BroadcastLocal => {
                // For now, treat same as Broadcast. The distinction between
                // local and all workers is resolved at a higher level.
                (0..self.num_targets).collect()
            }
        }
    }

    /// Route a batch of records. Returns a Vec of (target_worker, record_indices).
    ///
    /// This is more efficient than routing individual records for exchange strategies.
    pub fn route_batch(&mut self, batch: &[D]) -> Vec<Vec<usize>> {
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); self.num_targets];
        for (idx, record) in batch.iter().enumerate() {
            let targets = self.route(record);
            for target in targets {
                buckets[target].push(idx);
            }
        }
        buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_no_exchange() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::Pipeline;
        assert!(!strat.requires_exchange());
        assert_eq!(strat.name(), "Pipeline");
    }

    #[test]
    fn exchange_routes_by_hash() {
        let strat: PartitionStrategy<i32> =
            PartitionStrategy::exchange("mod 4", |x: &i32| *x as u64);
        assert!(strat.requires_exchange());

        let mut router = Router::new(strat, 4);
        // Record with value 0 → worker 0
        assert_eq!(router.route(&0), vec![0]);
        // Record with value 5 → worker 5 % 4 = 1
        assert_eq!(router.route(&5), vec![1]);
        // Record with value 7 → worker 7 % 4 = 3
        assert_eq!(router.route(&7), vec![3]);
    }

    #[test]
    fn exchange_by_key_routes_deterministically() {
        #[derive(Clone)]
        struct Record {
            key: String,
            #[allow(dead_code)]
            value: i32,
        }

        let strat: PartitionStrategy<Record> =
            PartitionStrategy::exchange_by_key("by key field", |r: &Record| r.key.clone());

        let mut router = Router::new(strat, 8);
        let record = Record {
            key: "hello".to_string(),
            value: 42,
        };

        // Same key always routes to same worker
        let target1 = router.route(&record);
        let target2 = router.route(&record);
        assert_eq!(target1, target2);
    }

    #[test]
    fn rebalance_round_robins() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::Rebalance;
        let mut router = Router::new(strat, 3);

        assert_eq!(router.route(&100), vec![0]);
        assert_eq!(router.route(&200), vec![1]);
        assert_eq!(router.route(&300), vec![2]);
        assert_eq!(router.route(&400), vec![0]); // wraps
    }

    #[test]
    fn gather_sends_to_zero() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::Gather;
        let mut router = Router::new(strat, 8);

        assert_eq!(router.route(&1), vec![0]);
        assert_eq!(router.route(&2), vec![0]);
        assert_eq!(router.route(&3), vec![0]);
    }

    #[test]
    fn broadcast_sends_to_all() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::Broadcast;
        let mut router = Router::new(strat, 4);

        assert_eq!(router.route(&42), vec![0, 1, 2, 3]);
    }

    #[test]
    fn broadcast_local_sends_to_all() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::BroadcastLocal;
        let mut router = Router::new(strat, 3);

        assert_eq!(router.route(&99), vec![0, 1, 2]);
    }

    #[test]
    fn route_batch_distributes_correctly() {
        let strat: PartitionStrategy<i32> =
            PartitionStrategy::exchange("mod 3", |x: &i32| *x as u64);
        let mut router = Router::new(strat, 3);

        let batch = vec![0, 1, 2, 3, 4, 5];
        let buckets = router.route_batch(&batch);

        // 0, 3 → worker 0; 1, 4 → worker 1; 2, 5 → worker 2
        assert_eq!(buckets[0], vec![0, 3]);
        assert_eq!(buckets[1], vec![1, 4]);
        assert_eq!(buckets[2], vec![2, 5]);
    }

    #[test]
    fn partition_strategy_display() {
        let p: PartitionStrategy<i32> = PartitionStrategy::Pipeline;
        assert_eq!(format!("{}", p), "Pipeline");

        let e: PartitionStrategy<i32> = PartitionStrategy::exchange("by id", |x: &i32| *x as u64);
        assert_eq!(format!("{}", e), "Exchange(by id)");

        let r: PartitionStrategy<i32> = PartitionStrategy::Rebalance;
        assert_eq!(format!("{}", r), "Rebalance");
    }

    #[test]
    fn exchange_fn_is_cloneable() {
        let strat: PartitionStrategy<i32> =
            PartitionStrategy::exchange("mod 4", |x: &i32| *x as u64);
        let cloned = strat.clone();

        // Both should route the same way
        let mut router1 = Router::new(strat, 4);
        let mut router2 = Router::new(cloned, 4);
        assert_eq!(router1.route(&7), router2.route(&7));
        assert_eq!(router1.route(&12), router2.route(&12));
    }

    #[test]
    #[should_panic(expected = "Router requires at least one target worker")]
    fn router_panics_on_zero_targets() {
        let strat: PartitionStrategy<i32> = PartitionStrategy::Rebalance;
        let _ = Router::new(strat, 0);
    }
}
