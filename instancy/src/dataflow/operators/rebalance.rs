//! Rebalance operator — round-robin redistribution across workers.
//!
//! The `rebalance` operator distributes data evenly across target workers
//! using round-robin assignment. Unlike `exchange`, it does not consider
//! record content — it simply spreads load evenly.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::scope::Scope;
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::{Slot, StreamEdge};
use crate::error::DataflowError;
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
    /// The source execution stage.
    source_stage: StageId,
    /// The target execution stage.
    target_stage: StageId,
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
        source_stage: StageId,
        target_stage: StageId,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            source_stage,
            target_stage,
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

impl<T: Timestamp, D> fmt::Debug for RebalanceOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RebalanceOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_stage", &self.source_stage)
            .field("target_stage", &self.target_stage)
            .finish()
    }
}

/// Extension trait for constructing rebalance operators on a `StreamEdge`.
pub trait RebalanceExt<S: Scope, D> {
    /// Redistribute data round-robin across all workers in the current stage.
    ///
    /// The target stage has the same parallelism as the source stage.
    /// This is useful for load-balancing after skewed computations.
    fn rebalance(&self) -> StreamEdge<S, D>;

    /// Redistribute data round-robin into a new stage with specified parallelism.
    ///
    /// Creates a new execution stage if `target_parallelism` differs from the
    /// current stage's parallelism; otherwise reuses the current stage.
    ///
    /// # Errors
    /// Returns an error if `target_parallelism` is 0.
    fn rebalance_to(&self, target_parallelism: usize) -> crate::Result<StreamEdge<S, D>>;
}

impl<S: Scope, D: 'static> RebalanceExt<S, D> for StreamEdge<S, D> {
    fn rebalance(&self) -> StreamEdge<S, D> {
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        let parallelism = scope.stage_parallelism(source_stage).unwrap_or(1);
        self.build_rebalance(scope, source_stage, parallelism)
    }

    fn rebalance_to(&self, target_parallelism: usize) -> crate::Result<StreamEdge<S, D>> {
        if target_parallelism == 0 {
            return Err(crate::Error::Dataflow(DataflowError::InvalidConfig(
                "target_parallelism must be > 0".into(),
            )));
        }
        let scope = self.scope().clone();
        let source_stage = self.stage_id();
        Ok(self.build_rebalance(scope, source_stage, target_parallelism))
    }
}

impl<S: Scope, D: 'static> StreamEdge<S, D> {
    /// Internal helper to build a rebalance operator.
    fn build_rebalance(
        &self,
        mut scope: S,
        source_stage: StageId,
        target_parallelism: usize,
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
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_index,
                "rebalance",
                target_stage,
                1,
                1,
            ))
            // SAFETY: operator index freshly allocated by allocate_operator_index()
            .expect("operator index should be unique");
        scope.add_edge(crate::dataflow::graph::EdgeInfo::exchange(
            *self.source(),
            Slot::new(op_index, 0),
            source_stage,
            target_stage,
        ));

        // Record target parallelism for stage inference.
        scope.set_exchange_parallelism(op_index, target_parallelism);

        let _operator = RebalanceOperator::<S::Timestamp, D>::new(
            "rebalance",
            op_index,
            source_stage,
            target_stage,
        );

        StreamEdge::new(scope, output_slot, target_stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn rebalance_same_parallelism() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let rebalanced = stream.rebalance();
        assert_eq!(rebalanced.stage_id(), stage_id);
    }

    #[test]
    fn rebalance_to_new_parallelism() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let rebalanced = stream.rebalance_to(16).unwrap();
        assert_ne!(rebalanced.stage_id(), stage_id);
    }

    #[test]
    fn rebalance_to_zero_parallelism_errors() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        let err = stream.rebalance_to(0).err().unwrap();
        assert!(matches!(
            err,
            crate::Error::Dataflow(DataflowError::InvalidConfig(_))
        ));
    }

    #[test]
    fn rebalance_operator_metadata() {
        let op =
            RebalanceOperator::<u64, i32>::new("my_rebalance", 3, StageId::new(0), StageId::new(1));
        assert_eq!(op.name(), "my_rebalance");
        assert_eq!(op.index(), 3);
        assert_eq!(op.source_stage(), StageId::new(0));
        assert_eq!(op.target_stage(), StageId::new(1));
        assert_eq!(op.strategy().name(), "Rebalance");
    }

    #[test]
    fn rebalance_operator_debug() {
        let op = RebalanceOperator::<u64, i32>::new("rb", 1, StageId::new(0), StageId::new(0));
        let debug = format!("{:?}", op);
        assert!(debug.contains("RebalanceOperator"));
        assert!(debug.contains("rb"));
    }
}
