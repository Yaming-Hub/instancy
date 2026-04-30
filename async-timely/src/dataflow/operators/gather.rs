//! Gather operator — funnel all data to a single worker.
//!
//! The `gather` operator routes all data to a single worker (worker 0)
//! in a new execution region with parallelism 1. This is used before
//! global aggregation or sorting operations.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::progress::timestamp::Timestamp;

/// A registered gather operator.
///
/// Routes all data to worker 0 in a new region with parallelism 1.
/// Records the repartition intent; actual data movement is handled by
/// the runtime when the dataflow is materialized.
pub struct GatherOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution region.
    source_region: RegionId,
    /// The target execution region (always parallelism 1).
    target_region: RegionId,
    /// The partition strategy (always Gather).
    strategy: PartitionStrategy<D>,
    /// Phantom for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp, D> GatherOperator<T, D> {
    /// Create a new gather operator.
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
            strategy: PartitionStrategy::Gather,
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

impl<T: Timestamp, D> fmt::Debug for GatherOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatherOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_region", &self.source_region)
            .field("target_region", &self.target_region)
            .finish()
    }
}

/// Extension trait for constructing gather operators on a `DataStream`.
pub trait GatherExt<S: Scope, D> {
    /// Funnel all data to a single worker.
    ///
    /// Creates a new execution region with parallelism 1 and routes all
    /// data to worker 0 in that region. Use this before global aggregations.
    fn gather(&self) -> DataStream<S, D>;
}

impl<S: Scope, D: 'static> GatherExt<S, D> for DataStream<S, D> {
    fn gather(&self) -> DataStream<S, D> {
        let mut scope = self.scope().clone();
        let source_region = self.region_id();
        let op_index = scope.allocate_operator_index();
        let output_slot = Slot::new(op_index, 0);

        // Gather always creates a new region with parallelism 1.
        let target_region = scope.new_region(1);

        // TODO: Register operator in scope/graph registry.
        let _operator = GatherOperator::<S::Timestamp, D>::new(
            "gather",
            op_index,
            source_region,
            target_region,
        );

        DataStream::new(scope, output_slot, target_region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn gather_creates_region_with_parallelism_1() {
        let scope = RootScope::<u64>::new("test", 8);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), source, region_id);

        let gathered = stream.gather();
        // Must be a different region
        assert_ne!(gathered.region_id(), region_id);
        // The new region should have parallelism 1
        let new_region = scope.region(gathered.region_id()).unwrap();
        assert_eq!(new_region.parallelism(), 1);
    }

    #[test]
    fn gather_from_single_worker_still_creates_new_region() {
        let scope = RootScope::<u64>::new("test", 1);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), source, region_id);

        // Even from parallelism=1, gather creates a new region (consistent behavior).
        let gathered = stream.gather();
        assert_ne!(gathered.region_id(), region_id);
        let new_region = scope.region(gathered.region_id()).unwrap();
        assert_eq!(new_region.parallelism(), 1);
    }

    #[test]
    fn gather_operator_metadata() {
        let op = GatherOperator::<u64, i32>::new(
            "my_gather",
            7,
            RegionId::new(0),
            RegionId::new(2),
        );
        assert_eq!(op.name(), "my_gather");
        assert_eq!(op.index(), 7);
        assert_eq!(op.source_region(), RegionId::new(0));
        assert_eq!(op.target_region(), RegionId::new(2));
        assert_eq!(op.strategy().name(), "Gather");
    }

    #[test]
    fn gather_operator_debug() {
        let op = GatherOperator::<u64, i32>::new(
            "g",
            2,
            RegionId::new(0),
            RegionId::new(1),
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("GatherOperator"));
        assert!(debug.contains("g"));
    }
}
