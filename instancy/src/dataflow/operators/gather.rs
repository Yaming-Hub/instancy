//! Gather operator — funnel all data to a single worker.
//!
//! The `gather` operator routes all data to a single worker (worker 0)
//! in a new execution stage with parallelism 1. This is used before
//! global aggregation or sorting operations.

use std::fmt;

use crate::dataflow::channels::PartitionStrategy;
use crate::dataflow::scope::Scope;
use crate::dataflow::stage::StageId;
use crate::dataflow::stream::{Slot, StreamEdge};
use crate::progress::timestamp::Timestamp;

/// A registered gather operator.
///
/// Routes all data to worker 0 in a new stage with parallelism 1.
/// Records the repartition intent; actual data movement is handled by
/// the runtime when the dataflow is materialized.
pub struct GatherOperator<T: Timestamp, D> {
    /// Human-readable name.
    name: String,
    /// Operator index within the scope.
    index: usize,
    /// The source execution stage.
    source_stage: StageId,
    /// The target execution stage (always parallelism 1).
    target_stage: StageId,
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
        source_stage: StageId,
        target_stage: StageId,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            source_stage,
            target_stage,
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

impl<T: Timestamp, D> fmt::Debug for GatherOperator<T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatherOperator")
            .field("name", &self.name)
            .field("index", &self.index)
            .field("source_stage", &self.source_stage)
            .field("target_stage", &self.target_stage)
            .finish()
    }
}

/// Extension trait for constructing gather operators on a `StreamEdge`.
pub trait GatherExt<S: Scope, D> {
    /// Funnel all data to a single worker.
    ///
    /// Creates a new execution stage with parallelism 1 and routes all
    /// data to worker 0 in that stage. Use this before global aggregations.
    fn gather(&self) -> StreamEdge<S, D>;
}

impl<S: Scope, D: 'static> GatherExt<S, D> for StreamEdge<S, D> {
    fn gather(&self) -> StreamEdge<S, D> {
        let mut scope = self.scope().clone();
        let source_stage = self.stage_id();
        let op_index = scope.allocate_operator_index();
        let output_slot = Slot::new(op_index, 0);

        // Gather always creates a new stage with parallelism 1.
        let target_stage = scope.new_stage(1);

        // Register operator and edge in the dataflow graph.
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                op_index,
                "gather",
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

        // Record target parallelism (always 1 for gather) for stage inference.
        scope.set_exchange_parallelism(op_index, 1);

        let _operator =
            GatherOperator::<S::Timestamp, D>::new("gather", op_index, source_stage, target_stage);

        StreamEdge::new(scope, output_slot, target_stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn gather_creates_stage_with_parallelism_1() {
        let scope = RootScope::<u64>::new("test", 8);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), source, stage_id);

        let gathered = stream.gather();
        // Must be a different stage
        assert_ne!(gathered.stage_id(), stage_id);
        // The new stage should have parallelism 1
        let new_parallelism = scope.stage_parallelism(gathered.stage_id()).unwrap();
        assert_eq!(new_parallelism, 1);
    }

    #[test]
    fn gather_from_single_worker_still_creates_new_stage() {
        let scope = RootScope::<u64>::new("test", 1);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), source, stage_id);

        // Even from parallelism=1, gather creates a new stage (consistent behavior).
        let gathered = stream.gather();
        assert_ne!(gathered.stage_id(), stage_id);
        let new_parallelism = scope.stage_parallelism(gathered.stage_id()).unwrap();
        assert_eq!(new_parallelism, 1);
    }

    #[test]
    fn gather_operator_metadata() {
        let op = GatherOperator::<u64, i32>::new("my_gather", 7, StageId::new(0), StageId::new(2));
        assert_eq!(op.name(), "my_gather");
        assert_eq!(op.index(), 7);
        assert_eq!(op.source_stage(), StageId::new(0));
        assert_eq!(op.target_stage(), StageId::new(2));
        assert_eq!(op.strategy().name(), "Gather");
    }

    #[test]
    fn gather_operator_debug() {
        let op = GatherOperator::<u64, i32>::new("g", 2, StageId::new(0), StageId::new(1));
        let debug = format!("{:?}", op);
        assert!(debug.contains("GatherOperator"));
        assert!(debug.contains("g"));
    }
}
