//! Rebalance operator — round-robin redistribution across workers.
//!
//! The `rebalance` operator distributes data evenly across target workers
//! using round-robin assignment. Unlike `exchange`, it does not consider
//! record content — it simply spreads load evenly.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{StreamEdge, Slot};
use crate::progress::timestamp::Timestamp;

/// A registered rebalance (round-robin redistribution) operator.
///
/// Records the repartition intent; actual data movement is handled by
/// the runtime when the dataflow is materialized.
pub struct RebalanceOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution region.
    source_region: RegionId,
    /// The target execution region.
    target_region: RegionId,
    /// The partition strategy (always Rebalance).
    strategy: PartitionStrategy<D>,
    /// Phantom for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp, D> RebalanceOperator<T, D> {
    /// Create a new rebalance operator.
    pub fn new(
        name: impl Into<String>,
        index: usize,
        source_region: RegionId,
        target_region: RegionId,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            source_region,
            target_region,
            strategy: PartitionStrategy::Rebalance,
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

impl<T: Timestamp, D> fmt::Debug for RebalanceOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RebalanceOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_region", &self.source_region)
            .field("target_region", &self.target_region)
            .finish()
    }
}

/// Extension trait for constructing rebalance operators on a `StreamEdge`.
pub trait RebalanceExt<S: Scope, D> {
    /// Redistribute data round-robin across all workers in the current region.
    ///
    /// The target region has the same parallelism as the source region.
    /// This is useful for load-balancing after skewed computations.
    fn rebalance(&self) -> StreamEdge<S, D>;

    /// Redistribute data round-robin into a new region with specified parallelism.
    ///
    /// Creates a new execution region if `target_parallelism` differs from the
    /// current region's parallelism; otherwise reuses the current region.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn rebalance_to(&self, target_parallelism: usize) -> StreamEdge<S, D>;
}

impl<S: Scope, D: 'static> RebalanceExt<S, D> for StreamEdge<S, D> {
    fn rebalance(&self) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_region = self.region_id();
        let parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        self.build_rebalance(scope, source_region, parallelism)
    }

    fn rebalance_to(&self, target_parallelism: usize) -> StreamEdge<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_region = self.region_id();
        self.build_rebalance(scope, source_region, target_parallelism)
    }
}

impl<S: Scope, D: 'static> StreamEdge<S, D> {
    /// Internal helper to build a rebalance operator.
    fn build_rebalance(
        &self,
        mut scope: S,
        source_region: RegionId,
        target_parallelism: usize,
    ) -> StreamEdge<S, D> {
        let op_index = scope.allocate_operator_index();
        let output_slot = Slot::new(op_index, 0);

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
            op_index, "rebalance", target_region, 1, 1,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            source_region,
            target_region,
        ));

        let _operator = RebalanceOperator::<S::Timestamp, D>::new(
            "rebalance",
            op_index,
            source_region,
            target_region,
        );

        StreamEdge::new(scope, output_slot, target_region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn rebalance_same_parallelism() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let rebalanced = stream.rebalance();
        assert_eq!(rebalanced.region_id(), region_id);
    }

    #[test]
    fn rebalance_to_new_parallelism() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let rebalanced = stream.rebalance_to(16);
        assert_ne!(rebalanced.region_id(), region_id);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn rebalance_to_zero_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope, source, region_id);

        let _ = stream.rebalance_to(0);
    }

    #[test]
    fn rebalance_operator_metadata() {
        let op = RebalanceOperator::<u64, i32>::new(
            "my_rebalance",
            3,
            RegionId::new(0),
            RegionId::new(1),
        );
        assert_eq!(op.name(), "my_rebalance");
        assert_eq!(op.index(), 3);
        assert_eq!(op.source_region(), RegionId::new(0));
        assert_eq!(op.target_region(), RegionId::new(1));
        assert_eq!(op.strategy().name(), "Rebalance");
    }

    #[test]
    fn rebalance_operator_debug() {
        let op = RebalanceOperator::<u64, i32>::new(
            "rb",
            1,
            RegionId::new(0),
            RegionId::new(0),
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("RebalanceOperator"));
        assert!(debug.contains("rb"));
    }
}
