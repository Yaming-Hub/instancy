//! Exchange operator — hash-based repartitioning across workers.
//!
//! The `exchange` operator redistributes data across workers in a target
//! execution region based on a hash function. Records with the same key
//! hash are routed to the same worker, enabling key-partitioned computations
//! like group-by and join.

use std::fmt;
use std::hash::Hash;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{StreamEdge, Slot};
use crate::progress::timestamp::Timestamp;

/// A registered exchange (repartition) operator.
///
/// This operator redistributes data across workers in the target region
/// using a hash-based routing function. It records the repartition intent;
/// actual data movement is handled by the runtime when the dataflow is
/// materialized.
pub struct ExchangeOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution region (where data comes from).
    source_region: RegionId,
    /// The target execution region (where data goes to).
    target_region: RegionId,
    /// The partition strategy (Exchange with routing function).
    strategy: PartitionStrategy<D>,
    /// Phantom for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp, D> ExchangeOperator<T, D> {
    /// Create a new exchange operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        source_region: RegionId,
        target_region: RegionId,
        strategy: PartitionStrategy<D>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            source_region,
            target_region,
            strategy,
            _phantom: std::marker::PhantomData,
        }
    }

    /// The operator's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The operator index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The source region.
    pub fn source_region(&self) -> RegionId {
        self.source_region
    }

    /// The target region.
    pub fn target_region(&self) -> RegionId {
        self.target_region
    }

    /// The partition strategy.
    pub fn strategy(&self) -> &PartitionStrategy<D> {
        &self.strategy
    }
}

impl<T: Timestamp, D> fmt::Debug for ExchangeOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_region", &self.source_region)
            .field("target_region", &self.target_region)
            .field("strategy", &self.strategy.name())
            .finish()
    }
}

/// Extension trait for constructing exchange operators on a `StreamEdge`.
pub trait ExchangeExt<S: Scope, D> {
    /// Repartition data by hashing a key extracted from each record.
    ///
    /// Records with the same key hash are routed to the same target worker.
    /// The target region has the same parallelism as the source region.
    ///
    /// # Example
    /// ```ignore
    /// let partitioned = stream.exchange(|record| &record.key);
    /// ```
    fn exchange<K: Hash + 'static>(
        &self,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> StreamEdge<S, D>;

    /// Repartition data by hashing a key, targeting a specific parallelism level.
    ///
    /// Creates a new execution region if `target_parallelism` differs from the
    /// current region's parallelism; otherwise reuses the current region.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn exchange_to<K: Hash + 'static>(
        &self,
        target_parallelism: usize,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> StreamEdge<S, D>;

    /// Repartition data using a direct hash function (returns u64).
    ///
    /// The returned u64 is reduced modulo the target worker count to
    /// determine routing. This is NOT a worker index.
    fn exchange_by_hash(
        &self,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> StreamEdge<S, D>;

    /// Repartition data using a direct hash function, targeting a specific parallelism.
    ///
    /// Creates a new execution region if `target_parallelism` differs from the
    /// current region's parallelism; otherwise reuses the current region.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn exchange_by_hash_to(
        &self,
        target_parallelism: usize,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> StreamEdge<S, D>;
}

impl<S: Scope, D: 'static> ExchangeExt<S, D> for StreamEdge<S, D> {
    fn exchange<K: Hash + 'static>(
        &self,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_region = self.region_id();
        let parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        self.build_exchange(scope, source_region, parallelism, PartitionStrategy::exchange_by_key("exchange", key_fn))
    }

    fn exchange_to<K: Hash + 'static>(
        &self,
        target_parallelism: usize,
        key_fn: impl Fn(&D) -> K + Send + Sync + 'static,
    ) -> StreamEdge<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_region = self.region_id();
        self.build_exchange(scope, source_region, target_parallelism, PartitionStrategy::exchange_by_key("exchange", key_fn))
    }

    fn exchange_by_hash(
        &self,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_region = self.region_id();
        let parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        self.build_exchange(scope, source_region, parallelism, PartitionStrategy::exchange("exchange_hash", hash_fn))
    }

    fn exchange_by_hash_to(
        &self,
        target_parallelism: usize,
        hash_fn: impl Fn(&D) -> u64 + Send + Sync + 'static,
    ) -> StreamEdge<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_region = self.region_id();
        self.build_exchange(scope, source_region, target_parallelism, PartitionStrategy::exchange("exchange_hash", hash_fn))
    }
}

impl<S: Scope, D: 'static> StreamEdge<S, D> {
    /// Internal helper to build an exchange operator.
    fn build_exchange(
        &self,
        mut scope: S,
        source_region: RegionId,
        target_parallelism: usize,
        strategy: PartitionStrategy<D>,
    ) -> StreamEdge<S, D> {
        let op_index = scope.allocate_operator_index();
        let output_slot = Slot::new(op_index, 0);

        // Determine target region: new region if parallelism differs, else same.
        let current_parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        let target_region = if target_parallelism != current_parallelism {
            scope.new_region(target_parallelism)
        } else {
            source_region
        };

        // Register operator and edge in the dataflow graph.
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, "exchange", target_region, 1, 1,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::exchange(
            *self.source(),
            Slot::new(op_index, 0),
            source_region,
            target_region,
        ));

        // Record target parallelism for stage inference.
        scope.set_exchange_parallelism(op_index, target_parallelism);

        let _operator = ExchangeOperator::<S::Timestamp, D>::new(
            "exchange",
            op_index,
            source_region,
            target_region,
            strategy,
        );

        StreamEdge::new(scope, output_slot, target_region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn exchange_ext_produces_stream_same_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, (u64, String)> =
            StreamEdge::new(scope, source, region_id);

        let exchanged = stream.exchange(|record: &(u64, String)| record.0);
        // Same parallelism → same region
        assert_eq!(exchanged.region_id(), region_id);
    }

    #[test]
    fn exchange_to_creates_new_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let exchanged = stream.exchange_to(8, |record: &i32| *record);
        // Different parallelism → new region
        assert_ne!(exchanged.region_id(), region_id);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn exchange_to_zero_parallelism_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let _ = stream.exchange_to(0, |record: &i32| *record);
    }

    #[test]
    fn exchange_by_hash_produces_stream() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let exchanged = stream.exchange_by_hash(|record: &i32| *record as u64);
        assert_eq!(exchanged.region_id(), region_id);
    }

    #[test]
    fn exchange_by_hash_to_creates_new_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let exchanged = stream.exchange_by_hash_to(16, |record: &i32| *record as u64);
        assert_ne!(exchanged.region_id(), region_id);
    }

    #[test]
    fn exchange_operator_metadata() {
        let strategy = PartitionStrategy::<i32>::exchange_by_key("by_id", |x: &i32| *x);
        let op = ExchangeOperator::<u64, i32>::new(
            "my_exchange",
            5,
            RegionId::new(0),
            RegionId::new(1),
            strategy,
        );
        assert_eq!(op.name(), "my_exchange");
        assert_eq!(op.index(), 5);
        assert_eq!(op.source_region(), RegionId::new(0));
        assert_eq!(op.target_region(), RegionId::new(1));
        assert_eq!(op.strategy().name(), "Exchange");
    }

    #[test]
    fn exchange_operator_debug() {
        let strategy = PartitionStrategy::<i32>::exchange("hash", |x: &i32| *x as u64);
        let op = ExchangeOperator::<u64, i32>::new(
            "ex",
            1,
            RegionId::new(0),
            RegionId::new(0),
            strategy,
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("ExchangeOperator"));
        assert!(debug.contains("ex"));
    }
}
