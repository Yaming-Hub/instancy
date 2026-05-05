//! Broadcast operators — fan-out data to all workers.
//!
//! The `broadcast` operator sends each record to every worker in the target
//! stage. This is used for distributing reference data or configuration
//! that all workers need.
//!
//! `broadcast_local` is a variant that only fans out to workers within the
//! same process, avoiding network transfer for data that doesn't need
//! cross-process distribution.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::scope::Scope;
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::{Slot, StreamEdge};
use crate::progress::timestamp::Timestamp;

/// A registered broadcast operator.
///
/// Fans out each record to all workers in the target stage.
/// Records the repartition intent; actual data movement is handled by
/// the runtime when the dataflow is materialized.
pub struct BroadcastOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution stage.
    source_stage: StageId,
    /// The target execution stage.
    target_stage: StageId,
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
        source_stage: StageId,
        target_stage: StageId,
        strategy: PartitionStrategy<D>,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            source_stage,
            target_stage,
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

    /// The source stage.
    pub fn source_stage(&self) -> StageId {
        self.source_stage
    }

    /// The target stage.
    pub fn target_stage(&self) -> StageId {
        self.target_stage
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
            .field("source_stage", &self.source_stage)
            .field("target_stage", &self.target_stage)
            .field("strategy", &self.strategy.name())
            .finish()
    }
}

/// Extension trait for constructing broadcast operators on a `StreamEdge`.
pub trait BroadcastExt<S: Scope, D> {
    /// Broadcast each record to all workers in the current stage.
    ///
    /// Every worker in the target stage receives a copy of each record.
    /// The target stage has the same parallelism as the source.
    fn broadcast(&self) -> StreamEdge<S, D>;

    /// Broadcast each record to all workers in a new stage with specified parallelism.
    ///
    /// Creates a new execution stage if `target_parallelism` differs from the
    /// current stage's parallelism; otherwise reuses the current stage.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn broadcast_to(&self, target_parallelism: usize) -> StreamEdge<S, D>;

    /// Broadcast each record to all workers in the same process only.
    ///
    /// This avoids network transfer for reference data that only needs
    /// local distribution. The target stage has the same parallelism as the source.
    fn broadcast_local(&self) -> StreamEdge<S, D>;

    /// Broadcast each record to all local workers in a new stage with specified parallelism.
    ///
    /// Creates a new execution stage if `target_parallelism` differs from the
    /// current stage's parallelism; otherwise reuses the current stage.
    ///
    /// # Panics
    /// Panics if `target_parallelism` is 0.
    fn broadcast_local_to(&self, target_parallelism: usize) -> StreamEdge<S, D>;
}

impl<S: Scope, D: 'static> BroadcastExt<S, D> for StreamEdge<S, D> {
    fn broadcast(&self) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        let parallelism = scope.stage_parallelism(source_stage).unwrap_or(1);
        self.build_broadcast(
            scope,
            source_stage,
            parallelism,
            PartitionStrategy::Broadcast,
        )
    }

    fn broadcast_to(&self, target_parallelism: usize) -> StreamEdge<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        self.build_broadcast(
            scope,
            source_stage,
            target_parallelism,
            PartitionStrategy::Broadcast,
        )
    }

    fn broadcast_local(&self) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        let parallelism = scope.stage_parallelism(source_stage).unwrap_or(1);
        self.build_broadcast(
            scope,
            source_stage,
            parallelism,
            PartitionStrategy::BroadcastLocal,
        )
    }

    fn broadcast_local_to(&self, target_parallelism: usize) -> StreamEdge<S, D> {
        assert!(target_parallelism > 0, "target_parallelism must be > 0");
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        self.build_broadcast(
            scope,
            source_stage,
            target_parallelism,
            PartitionStrategy::BroadcastLocal,
        )
    }
}

impl<S: Scope, D: 'static> StreamEdge<S, D> {
    /// Internal helper to build a broadcast operator.
    fn build_broadcast(
        &self,
        mut scope: S,
        source_stage: StageId,
        target_parallelism: usize,
        strategy: PartitionStrategy<D>,
    ) -> StreamEdge<S, D> {
        let op_index = scope.allocate_operator_index();
        let output_slot = Slot::new(op_index, 0);

        let current_parallelism = scope.stage_parallelism(source_stage).unwrap_or(1);
        let target_stage = if target_parallelism != current_parallelism {
            scope.new_stage(target_parallelism)
        } else {
            source_stage
        };

        // Register operator and edge in the dataflow graph.
        let name = strategy.name().to_owned();
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_index,
                &name,
                target_stage,
                1,
                1,
            ))
            .expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::exchange(
            *self.source(),
            Slot::new(op_index, 0),
            source_stage,
            target_stage,
        ));

        // Record target parallelism for stage inference.
        scope.set_exchange_parallelism(op_index, target_parallelism);

        let _operator = BroadcastOperator::<S::Timestamp, D>::new(
            name,
            op_index,
            source_stage,
            target_stage,
            strategy,
        );

        StreamEdge::new(scope, output_slot, target_stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn broadcast_same_stage() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let broadcasted = stream.broadcast();
        assert_eq!(broadcasted.stage_id(), stage_id);
    }

    #[test]
    fn broadcast_to_new_stage() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), source, stage_id);

        let broadcasted = stream.broadcast_to(8);
        assert_ne!(broadcasted.stage_id(), stage_id);
        let new_parallelism = scope.stage_parallelism(broadcasted.stage_id()).unwrap();
        assert_eq!(new_parallelism, 8);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn broadcast_to_zero_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let _ = stream.broadcast_to(0);
    }

    #[test]
    fn broadcast_local_same_stage() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let broadcasted = stream.broadcast_local();
        assert_eq!(broadcasted.stage_id(), stage_id);
    }

    #[test]
    fn broadcast_local_to_new_stage() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), source, stage_id);

        let broadcasted = stream.broadcast_local_to(12);
        assert_ne!(broadcasted.stage_id(), stage_id);
        let new_parallelism = scope.stage_parallelism(broadcasted.stage_id()).unwrap();
        assert_eq!(new_parallelism, 12);
    }

    #[test]
    #[should_panic(expected = "target_parallelism must be > 0")]
    fn broadcast_local_to_zero_panics() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let _ = stream.broadcast_local_to(0);
    }

    #[test]
    fn broadcast_operator_metadata() {
        let op = BroadcastOperator::<u64, i32>::new(
            "my_broadcast",
            4,
            StageId::new(0),
            StageId::new(1),
            PartitionStrategy::Broadcast,
        );
        assert_eq!(op.name(), "my_broadcast");
        assert_eq!(op.index(), 4);
        assert_eq!(op.source_stage(), StageId::new(0));
        assert_eq!(op.target_stage(), StageId::new(1));
        assert_eq!(op.strategy().name(), "Broadcast");
    }

    #[test]
    fn broadcast_local_operator_metadata() {
        let op = BroadcastOperator::<u64, i32>::new(
            "local_bc",
            6,
            StageId::new(0),
            StageId::new(0),
            PartitionStrategy::BroadcastLocal,
        );
        assert_eq!(op.strategy().name(), "BroadcastLocal");
    }

    #[test]
    fn broadcast_operator_debug() {
        let op = BroadcastOperator::<u64, i32>::new(
            "bc",
            1,
            StageId::new(0),
            StageId::new(1),
            PartitionStrategy::Broadcast,
        );
        let debug = format!("{:?}", op);
        assert!(debug.contains("BroadcastOperator"));
        assert!(debug.contains("bc"));
    }
}
