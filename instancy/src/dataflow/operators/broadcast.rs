//! Broadcast operators — fan-out data to all workers.
//!
//! The `broadcast` operator sends each record to every worker in the target
//! region. This is used for distributing reference data or configuration
//! that all workers need.
//!
//! `broadcast_local` is a variant that only fans out to workers within the
//! same process, avoiding network transfer for data that doesn't need
//! cross-process distribution.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::region::RegionId;
use crate::dataflow::scope::Scope;
use crate::dataflow::stream::{DataStream, Slot};
use crate::progress::timestamp::Timestamp;

/// A registered broadcast operator.
///
/// Fans out each record to all workers in the target region.
/// Records the repartition intent; actual data movement is handled by
/// the runtime when the dataflow is materialized.
pub struct BroadcastOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution region.
    source_region: RegionId,
    /// The target execution region.
    target_region: RegionId,
    /// The partition strategy (Broadcast or BroadcastLocal).
    strategy: PartitionStrategy<D>,
    /// Phantom for timestamp.
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Timestamp, D> BroadcastOperator<T, D> {
    /// Create a new broadcast operator.
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

impl<T: Timestamp, D> fmt::Debug for BroadcastOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_region", &self.source_region)
            .field("target_region", &self.target_region)
            .field("strategy", &self.strategy.name())
            .finish()
    }
}

/// Extension trait for constructing broadcast operators on a `DataStream`.
pub trait BroadcastExt<S: Scope, D> {
    /// Broadcast each record to all workers in the current region.
    ///
    /// Every worker in the target region receives a copy of each record.
    /// The target region has the same parallelism as the source.
    fn broadcast(&self) -> DataStream<S, D>;

    /// Broadcast each record to all workers in a new region with specified parallelism.
    ///
    /// Creates a new execution region if `target_parallelism` differs from the
    /// current region's parallelism; otherwise reuses the current region.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn broadcast_to(&self, target_parallelism: usize) -> DataStream<S, D>;

    /// Broadcast each record to all workers in the same process only.
    ///
    /// This avoids network transfer for reference data that only needs
    /// local distribution. The target region has the same parallelism as the source.
    fn broadcast_local(&self) -> DataStream<S, D>;

    /// Broadcast each record to all local workers in a new region with specified parallelism.
    ///
    /// Creates a new execution region if `target_parallelism` differs from the
    /// current region's parallelism; otherwise reuses the current region.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn broadcast_local_to(&self, target_parallelism: usize) -> DataStream<S, D>;
}

impl<S: Scope, D: 'static> BroadcastExt<S, D> for DataStream<S, D> {
    fn broadcast(&self) -> DataStream<S, D> {
        let scope = self.scope().clone();
        let source_region = self.region_id();
        let parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        self.build_broadcast(scope, source_region, parallelism, PartitionStrategy::Broadcast)
    }

    fn broadcast_to(&self, target_parallelism: usize) -> DataStream<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_region = self.region_id();
        self.build_broadcast(scope, source_region, target_parallelism, PartitionStrategy::Broadcast)
    }

    fn broadcast_local(&self) -> DataStream<S, D> {
        let scope = self.scope().clone();
        let source_region = self.region_id();
        let parallelism = scope.region(source_region)
            .map(|r| r.parallelism())
            .unwrap_or(1);
        self.build_broadcast(scope, source_region, parallelism, PartitionStrategy::BroadcastLocal)
    }

    fn broadcast_local_to(&self, target_parallelism: usize) -> DataStream<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_region = self.region_id();
        self.build_broadcast(scope, source_region, target_parallelism, PartitionStrategy::BroadcastLocal)
    }
}

impl<S: Scope, D: 'static> DataStream<S, D> {
    /// Internal helper to build a broadcast operator.
    fn build_broadcast(
        &self,
        mut scope: S,
        source_region: RegionId,
        target_parallelism: usize,
        strategy: PartitionStrategy<D>,
    ) -> DataStream<S, D> {
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
        let name = strategy.name().to_owned();
        scope.register_operator(crate::dataflow::graph::OperatorInfo::new(
            op_index, &name, target_region, 1, 1,
        )).expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::new(
            *self.source(),
            Slot::new(op_index, 0),
            source_region,
            target_region,
        ));

        let _operator = BroadcastOperator::<S::Timestamp, D>::new(
            name,
            op_index,
            source_region,
            target_region,
            strategy,
        );

        DataStream::new(scope, output_slot, target_region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn broadcast_same_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let broadcasted = stream.broadcast();
        assert_eq!(broadcasted.region_id(), region_id);
    }

    #[test]
    fn broadcast_to_new_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), source, region_id);

        let broadcasted = stream.broadcast_to(8);
        assert_ne!(broadcasted.region_id(), region_id);
        let new_region = scope.region(broadcasted.region_id()).unwrap();
        assert_eq!(new_region.parallelism(), 8);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn broadcast_to_zero_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let _ = stream.broadcast_to(0);
    }

    #[test]
    fn broadcast_local_same_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let broadcasted = stream.broadcast_local();
        assert_eq!(broadcasted.region_id(), region_id);
    }

    #[test]
    fn broadcast_local_to_new_region() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope.clone(), source, region_id);

        let broadcasted = stream.broadcast_local_to(12);
        assert_ne!(broadcasted.region_id(), region_id);
        let new_region = scope.region(broadcasted.region_id()).unwrap();
        assert_eq!(new_region.parallelism(), 12);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn broadcast_local_to_zero_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Slot::new(0, 0);
        let stream: DataStream<RootScope<u64>, i32> =
            DataStream::new(scope, source, region_id);

        let _ = stream.broadcast_local_to(0);
    }

    #[test]
    fn broadcast_operator_metadata() {
        let op = BroadcastOperator::<u64, i32>::new(
            "my_broadcast",
            4,
            RegionId::new(0),
            RegionId::new(1),
            PartitionStrategy::Broadcast,
        );
        assert_eq!(op.name(), "my_broadcast");
        assert_eq!(op.index(), 4);
        assert_eq!(op.source_region(), RegionId::new(0));
        assert_eq!(op.target_region(), RegionId::new(1));
        assert_eq!(op.strategy().name(), "Broadcast");
    }

    #[test]
    fn broadcast_local_operator_metadata() {
        let op = BroadcastOperator::<u64, i32>::new(
            "local_bc",
            6,
            RegionId::new(0),
            RegionId::new(0),
            PartitionStrategy::BroadcastLocal,
        );
        assert_eq!(op.strategy().name(), "BroadcastLocal");
    }

    #[test]
    fn broadcast_operator_debug() {
        let op = BroadcastOperator::<u64, i32>::new(
            "bc",
            1,
            RegionId::new(0),
            RegionId::new(1),
            PartitionStrategy::Broadcast,
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("BroadcastOperator"));
        assert!(debug.contains("bc"));
    }
}
