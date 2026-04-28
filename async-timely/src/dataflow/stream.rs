//! Stream — a typed edge in the dataflow graph.
//!
//! A `Stream<S, D>` represents a collection of timestamped data records
//! flowing from one operator to the next. Streams are the primary
//! way operators are connected.

use std::fmt;

use super::channels::PartitionStrategy;
use super::region::RegionId;
use super::scope::Scope;

/// Identifies a specific port on an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Port {
    /// The operator index within its scope.
    pub operator_index: usize,
    /// The port number (0 for single-output operators).
    pub port_index: usize,
}

impl Port {
    /// Create a new port identifier.
    pub fn new(operator_index: usize, port_index: usize) -> Self {
        Self {
            operator_index,
            port_index,
        }
    }
}

impl fmt::Display for Port {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Op{}:Port{}", self.operator_index, self.port_index)
    }
}

/// A connection target for a stream edge.
#[derive(Debug, Clone)]
pub struct StreamTarget {
    /// The target port (operator + port index).
    pub port: Port,
    /// The partition strategy used to route data to this target.
    pub pact: String, // Strategy name for debugging; actual routing is in the channel.
}

/// A typed edge in the dataflow graph.
///
/// `Stream<S, D>` represents a logical stream of data records of type `D`
/// flowing at timestamps defined by scope `S`. Streams connect an output
/// port of one operator to the input port(s) of downstream operators.
///
/// Streams are created by operators (e.g., `unary`, `binary`) and consumed
/// by downstream operators or terminal operators (e.g., `output`).
#[derive(Debug, Clone)]
pub struct Stream<S: Scope, D> {
    /// The scope this stream belongs to.
    scope: S,
    /// The source port (which operator output produced this stream).
    source: Port,
    /// The execution region this stream's source operator belongs to.
    region_id: RegionId,
    /// Phantom for the data type.
    _data: std::marker::PhantomData<D>,
}

impl<S: Scope, D> Stream<S, D> {
    /// Create a new stream from a source operator's output port.
    pub fn new(scope: S, source: Port, region_id: RegionId) -> Self {
        Self {
            scope,
            source,
            region_id,
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

    /// Get the source port of this stream.
    pub fn source(&self) -> &Port {
        &self.source
    }

    /// Get the region this stream's data originates from.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Create a derived stream in a new region.
    /// This is used internally by repartition operators.
    pub fn in_region(mut self, new_region_id: RegionId, new_source: Port) -> Self {
        self.region_id = new_region_id;
        self.source = new_source;
        self
    }
}

/// Describes how two streams connect, including the partition strategy.
#[derive(Debug)]
pub struct StreamConnection<D> {
    /// Source port.
    pub source: Port,
    /// Target port.
    pub target: Port,
    /// The source region.
    pub source_region: RegionId,
    /// The target region.
    pub target_region: RegionId,
    /// Routing strategy between source and target.
    pub strategy: PartitionStrategy<D>,
}

impl<D> StreamConnection<D> {
    /// Validate that a connection between regions uses an appropriate strategy.
    ///
    /// Returns an error if:
    /// - Regions differ but the strategy is Pipeline (must use a repartition).
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.source_region != self.target_region {
            if matches!(&self.strategy, PartitionStrategy::Pipeline) {
                return Err(crate::error::Error::Custom(format!(
                    "Cannot connect {} in {} to {} in {} with Pipeline strategy. \
                     Use an explicit repartition operator (exchange, rebalance, gather, broadcast) \
                     when crossing region boundaries.",
                    self.source, self.source_region, self.target, self.target_region,
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::scope::RootScope;

    #[test]
    fn port_creation_and_display() {
        let port = Port::new(3, 1);
        assert_eq!(port.operator_index, 3);
        assert_eq!(port.port_index, 1);
        assert_eq!(format!("{}", port), "Op3:Port1");
    }

    #[test]
    fn stream_creation() {
        let scope = RootScope::<u64>::new("test", 4);
        let region_id = scope.current_region().id();
        let source = Port::new(0, 0);

        let stream: Stream<RootScope<u64>, i32> = Stream::new(scope, source, region_id);
        assert_eq!(stream.source().operator_index, 0);
        assert_eq!(stream.region_id(), region_id);
        assert_eq!(stream.scope().name(), "test");
    }

    #[test]
    fn stream_in_new_region() {
        let mut scope = RootScope::<u64>::new("test", 4);
        let region1 = scope.current_region().id();
        let region2 = scope.new_region(8);
        let source = Port::new(0, 0);

        let stream: Stream<RootScope<u64>, i32> =
            Stream::new(scope, source, region1);

        let new_source = Port::new(1, 0);
        let stream2 = stream.in_region(region2, new_source);
        assert_eq!(stream2.region_id(), region2);
        assert_eq!(stream2.source().operator_index, 1);
    }

    #[test]
    fn connection_validation_pipeline_same_region() {
        let region = RegionId::new(0);
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: region,
            target_region: region,
            strategy: PartitionStrategy::Pipeline,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_pipeline_cross_region_fails() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: RegionId::new(0),
            target_region: RegionId::new(1),
            strategy: PartitionStrategy::Pipeline,
        };
        let result = conn.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Cannot connect"));
        assert!(err_msg.contains("repartition"));
    }

    #[test]
    fn connection_validation_exchange_cross_region_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: RegionId::new(0),
            target_region: RegionId::new(1),
            strategy: PartitionStrategy::exchange("by value", |x: &i32| *x as u64),
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_rebalance_cross_region_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: RegionId::new(0),
            target_region: RegionId::new(2),
            strategy: PartitionStrategy::Rebalance,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_gather_cross_region_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: RegionId::new(0),
            target_region: RegionId::new(1),
            strategy: PartitionStrategy::Gather,
        };
        assert!(conn.validate().is_ok());
    }

    #[test]
    fn connection_validation_broadcast_cross_region_ok() {
        let conn: StreamConnection<i32> = StreamConnection {
            source: Port::new(0, 0),
            target: Port::new(1, 0),
            source_region: RegionId::new(0),
            target_region: RegionId::new(1),
            strategy: PartitionStrategy::Broadcast,
        };
        assert!(conn.validate().is_ok());
    }
}
