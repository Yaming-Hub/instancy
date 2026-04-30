//! Execution regions for per-stage dynamic parallelism.
//!
//! An execution region defines a group of operators that share the same
//! parallelism level and placement policy. Operators within the same region
//! use Pipeline routing; crossing region boundaries requires an explicit
//! repartition operator.

use std::fmt;

/// Unique identifier for an execution region within a dataflow.
///
/// This is a **logical** concept — regions define parallelism groupings in the
/// computation graph. A region has no inherent physical location; the runtime
/// maps logical regions to physical threads via the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegionId(pub(crate) usize);

impl RegionId {
    /// Create a new region ID (used internally during graph construction).
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Get the numeric index of this region.
    pub fn index(&self) -> usize {
        self.0
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region({})", self.0)
    }
}

/// An execution region defines the parallelism level and placement policy
/// for a group of operators.
///
/// All operators within the same region share the same number of logical
/// workers. To change parallelism, data must cross a region boundary via
/// an explicit repartition operator (exchange, rebalance, gather, broadcast).
#[derive(Debug, Clone)]
pub struct Region {
    /// Unique identifier for this region.
    id: RegionId,
    /// Number of logical workers in this region.
    parallelism: usize,
    /// How workers are placed onto physical threads/nodes.
    placement: PlacementPolicy,
    /// Optional human-readable name for debugging/metrics.
    name: Option<String>,
}

impl Region {
    /// Create a new region with the given parallelism level.
    pub fn new(id: RegionId, parallelism: usize) -> Self {
        Self {
            id,
            parallelism,
            placement: PlacementPolicy::default(),
            name: None,
        }
    }

    /// Create a region with a specific placement policy.
    pub fn with_placement(mut self, placement: PlacementPolicy) -> Self {
        self.placement = placement;
        self
    }

    /// Set a human-readable name for this region.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Get this region's unique identifier.
    pub fn id(&self) -> RegionId {
        self.id
    }

    /// Get the parallelism level (number of logical workers).
    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    /// Get the placement policy.
    pub fn placement(&self) -> &PlacementPolicy {
        &self.placement
    }

    /// Get the optional name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(
                f,
                "Region({}, name={}, parallelism={})",
                self.id.0, name, self.parallelism
            )
        } else {
            write!(
                f,
                "Region({}, parallelism={})",
                self.id.0, self.parallelism
            )
        }
    }
}

/// Determines how logical workers in a region are placed onto
/// physical compute threads and cluster nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementPolicy {
    /// Distribute workers proportionally across available nodes.
    /// This is the default — if the cluster has N nodes and the region
    /// has P workers, each node gets roughly P/N workers.
    Proportional,

    /// Assign workers to nodes in round-robin order.
    RoundRobin,

    /// Pin all workers in this region to a specific node.
    /// Useful for operations that need local data access.
    Pinned {
        /// The node index to pin to.
        node_index: usize,
    },
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        PlacementPolicy::Proportional
    }
}

impl fmt::Display for PlacementPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlacementPolicy::Proportional => write!(f, "Proportional"),
            PlacementPolicy::RoundRobin => write!(f, "RoundRobin"),
            PlacementPolicy::Pinned { node_index } => {
                write!(f, "Pinned(node={})", node_index)
            }
        }
    }
}

/// Allocates RegionIds sequentially during dataflow construction.
#[derive(Debug, Clone)]
pub struct RegionAllocator {
    next_id: usize,
}

impl RegionAllocator {
    /// Create a new region allocator.
    pub fn new() -> Self {
        Self { next_id: 0 }
    }

    /// Allocate a new region with the given parallelism.
    pub fn allocate(&mut self, parallelism: usize) -> Region {
        let id = RegionId::new(self.next_id);
        self.next_id += 1;
        Region::new(id, parallelism)
    }

    /// The number of regions allocated so far.
    pub fn count(&self) -> usize {
        self.next_id
    }
}

impl Default for RegionAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_creation_and_accessors() {
        let region = Region::new(RegionId::new(0), 4);
        assert_eq!(region.id(), RegionId::new(0));
        assert_eq!(region.parallelism(), 4);
        assert_eq!(region.placement(), &PlacementPolicy::Proportional);
        assert_eq!(region.name(), None);
    }

    #[test]
    fn region_with_name_and_placement() {
        let region = Region::new(RegionId::new(1), 8)
            .with_name("aggregation")
            .with_placement(PlacementPolicy::RoundRobin);

        assert_eq!(region.name(), Some("aggregation"));
        assert_eq!(region.placement(), &PlacementPolicy::RoundRobin);
        assert_eq!(region.parallelism(), 8);
    }

    #[test]
    fn region_pinned_placement() {
        let region = Region::new(RegionId::new(2), 1)
            .with_placement(PlacementPolicy::Pinned { node_index: 3 });

        assert_eq!(
            region.placement(),
            &PlacementPolicy::Pinned { node_index: 3 }
        );
    }

    #[test]
    fn region_display() {
        let region = Region::new(RegionId::new(0), 4);
        assert_eq!(format!("{}", region), "Region(0, parallelism=4)");

        let named = Region::new(RegionId::new(1), 2).with_name("input");
        assert_eq!(
            format!("{}", named),
            "Region(1, name=input, parallelism=2)"
        );
    }

    #[test]
    fn placement_policy_default() {
        assert_eq!(PlacementPolicy::default(), PlacementPolicy::Proportional);
    }

    #[test]
    fn placement_policy_display() {
        assert_eq!(format!("{}", PlacementPolicy::Proportional), "Proportional");
        assert_eq!(format!("{}", PlacementPolicy::RoundRobin), "RoundRobin");
        assert_eq!(
            format!("{}", PlacementPolicy::Pinned { node_index: 2 }),
            "Pinned(node=2)"
        );
    }

    #[test]
    fn region_allocator_sequential() {
        let mut alloc = RegionAllocator::new();
        assert_eq!(alloc.count(), 0);

        let r1 = alloc.allocate(4);
        assert_eq!(r1.id(), RegionId::new(0));
        assert_eq!(r1.parallelism(), 4);

        let r2 = alloc.allocate(8);
        assert_eq!(r2.id(), RegionId::new(1));
        assert_eq!(r2.parallelism(), 8);

        assert_eq!(alloc.count(), 2);
    }

    #[test]
    fn region_id_equality() {
        let a = RegionId::new(5);
        let b = RegionId::new(5);
        let c = RegionId::new(6);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
