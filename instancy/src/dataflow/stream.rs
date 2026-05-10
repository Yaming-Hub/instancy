//! StreamEdge — a typed edge in the dataflow graph.
//!
//! A `StreamEdge<S, D>` represents a collection of timestamped data records
//! flowing from one operator to the next. StreamEdges are the primary
//! way operators are connected.

use std::fmt;

use super::channels::PartitionStrategy;
use super::scope::Scope;
use super::stage::StageId;
use crate::error::DataflowError;

/// Identifies a specific input or output slot on a logical operator.
///
/// This is a **logical** concept — it references an operator's port in the
/// dataflow graph, not a physical processor core or memory location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Slot {
    /// The logical operator index within its scope.
    pub operator_index: usize,
    /// The logical slot number (0 for single-output operators, 0=left/1=right for binary inputs).
    pub slot_index: usize,
}

impl Slot {
    /// Create a new slot identifier.
    pub fn new(operator_index: usize, slot_index: usize) -> Self {
        Self {
            operator_index,
            slot_index,
        }
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Op{}:Slot{}", self.operator_index, self.slot_index)
    }
}

/// A logical connection target for a stream edge.
///
/// This is a **logical** concept — it describes where data flows in the graph,
/// not how it is physically delivered (that is the transport layer's job).
#[derive(Debug, Clone)]
pub struct StreamTarget {
    /// The logical target slot (operator + slot index).
    pub slot: Slot,
    /// The partition strategy used to route data to this target.
    pub pact: String, // Strategy name for debugging; actual routing is in the channel.
}

/// A typed edge in the dataflow graph (Layer 2: Typed Stream Graph).
///
/// `StreamEdge<S, D>` represents a logical stream of data records of type `D`
/// flowing at timestamps defined by scope `S`. StreamEdges connect an output
/// slot of one operator to the input slot(s) of downstream operators.
///
/// StreamEdges are created by operators (e.g., `unary`, `binary`) and consumed
/// by downstream operators or terminal operators (e.g., `output`).
///
/// See PLAN.md "Conceptual Architecture: Three Layers of a Dataflow" for how
/// StreamEdge relates to Pipe (Layer 3) and the abstract Dataflow Graph (Layer 1).
#[derive(Debug, Clone)]
pub struct StreamEdge<S: Scope, D> {
    /// The scope this stream belongs to.
    scope: S,
    /// The source slot (which operator output produced this stream).
    source: Slot,
    /// The execution stage this stream's source operator belongs to.
    stage_id: StageId,
    /// Phantom for the data type.
    _data: std::marker::PhantomData<D>,
}

impl<S: Scope, D> StreamEdge<S, D> {
    /// Create a new stream from a source operator's output slot.
    pub fn new(scope: S, source: Slot, stage_id: StageId) -> Self {
        Self {
            scope,
            source,
            stage_id,
            _data: std::marker::PhantomData,
        }
    }

    /// Get a reference to the scope this stream belongs to.
    pub fn scope(&self) -> &S {
        &self.scope
    }

    /// Get a mutable reference to the scope.
    pub fn scope_mut(&mut self) -> &mut S {
        &mut self.scope
    }

    /// Get the source slot of this stream.
    pub fn source(&self) -> &Slot {
        &self.source
    }

    /// Get the stage this stream's data originates from.
    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }

    /// Create a derived stream in a new stage.
    /// This is used internally by repartition operators.
    pub fn in_stage(mut self, new_stage_id: StageId, new_source: Slot) -> Self {
        self.stage_id = new_stage_id;
        self.source = new_source;
        self
    }
}

/// Describes how two streams connect, including the partition strategy.
#[derive(Debug)]
pub struct StreamConnection<D> {
    /// Source slot.
    pub source: Slot,
    /// Target slot.
    pub target: Slot,
    /// The source stage.
    pub source_stage: StageId,
    /// The target stage.
    pub target_stage: StageId,
    /// Routing strategy between source and target.
    pub strategy: PartitionStrategy<D>,
}

impl<D> StreamConnection<D> {
    /// Validate that a connection between stages uses an appropriate strategy.
    ///
    /// Returns an error if:
    /// - Stages differ but the strategy is Pipeline (must use a repartition).
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.source_stage != self.target_stage
            && matches!(&self.strategy, PartitionStrategy::Pipeline)
        {
            return Err(crate::error::Error::Dataflow(DataflowError::InvalidGraph(
                format!(
                    "Cannot connect {} in {} to {} in {} with Pipeline strategy. \
                     Use an explicit repartition operator (exchange, rebalance, gather, broadcast) \
                     when crossing stage boundaries.",
                    self.source, self.source_stage, self.target, self.target_stage,
                ),
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn slot_creation_and_display() {
        let slot = Slot::new(3, 1);
        assert_eq!(slot.operator_index, 3);
        assert_eq!(slot.slot_index, 1);
        assert_eq!(format!("{}", slot), "Op3:Slot1");
    }

    #[test]
    fn stream_creation() {
        let scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();
        let source = Slot::new(0, 0);

        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);
        assert_eq!(stream.source().operator_index, 0);
        assert_eq!(stream.stage_id(), stage_id);
        assert_eq!(stream.scope().name(), "test");
    }

    #[test]
    fn stream_in_new_stage() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let stage1 = scope.current_stage_id();
        let stage2 = scope.new_stage(8);
        let source = Slot::new(0, 0);

        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage1);

        let new_source = Slot::new(1, 0);
        let stream2 = stream.in_stage(stage2, new_source);
        assert_eq!(stream2.stage_id(), stage2);
        assert_eq!(stream2.source().operator_index, 1);
    }

    #[test]
    fn connection_validation_pipeline_same_stage() {
        let stage = StageId::new(0);
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: stage,
            target_stage: stage,
            strategy: PartitionStrategy::Pipeline,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_pipeline_cross_stage_fails() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: StageId::new(0),
            target_stage: StageId::new(1),
            strategy: PartitionStrategy::Pipeline,
        };
        let result = conn.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot connect"));
        assert!(err_msg.contains("repartition"));
    }

    #[test]
    fn connection_validation_exchange_cross_stage_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: StageId::new(0),
            target_stage: StageId::new(1),
            strategy: PartitionStrategy::exchange("by value", |x: &i32| *x as u64),
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_rebalance_cross_stage_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: StageId::new(0),
            target_stage: StageId::new(2),
            strategy: PartitionStrategy::Rebalance,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_gather_cross_stage_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: StageId::new(0),
            target_stage: StageId::new(1),
            strategy: PartitionStrategy::Gather,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_broadcast_cross_stage_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Slot::new(0, 0),
            target: Slot::new(1, 0),
            source_stage: StageId::new(0),
            target_stage: StageId::new(1),
            strategy: PartitionStrategy::Broadcast,
        };
        assert!(conn.validate().is_ok());
    }
}
