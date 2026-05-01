//! Dataflow identity and allocation.
//!
//! Each running dataflow instance gets a cluster-unique [`DataflowId`]. The ID
//! encodes the originating node index in the high bits to guarantee uniqueness
//! without cross-node coordination.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cluster-unique identifier for a running dataflow instance.
///
/// This is a **logical** concept — it identifies a specific computation graph
/// instance. Multiple dataflows with different IDs can run concurrently on
/// the same physical infrastructure.
///
/// # Encoding
///
/// The 64-bit value is structured as:
/// - Bits 63..48: originating node index (supports up to 65,536 nodes)
/// - Bits 47..0: per-node sequence number (~281 trillion dataflows per node)
///
/// This guarantees cluster-wide uniqueness as long as node indices are unique
/// (ensured by [`ClusterTopology`](crate::execute::ClusterTopology)).
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DataflowId(u64);

impl DataflowId {
    /// Maximum node index that can be encoded (16 bits).
    pub const MAX_NODE_INDEX: u16 = u16::MAX;

    /// Maximum sequence number per node (48 bits).
    pub const MAX_SEQUENCE: u64 = (1u64 << 48) - 1;

    /// Create a DataflowId from raw parts.
    ///
    /// # Panics
    ///
    /// Panics if `sequence` exceeds 48 bits.
    pub fn new(node_index: u16, sequence: u64) -> Self {
        assert!(
            sequence <= Self::MAX_SEQUENCE,
            "sequence {sequence} exceeds 48-bit limit"
        );
        Self((node_index as u64) << 48 | sequence)
    }

    /// Create a DataflowId from a raw u64 value (e.g., received from wire).
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw u64 value (for wire protocol encoding).
    pub fn as_raw(self) -> u64 {
        self.0
    }

    /// Extract the originating node index.
    pub fn node_index(self) -> u16 {
        (self.0 >> 48) as u16
    }

    /// Extract the per-node sequence number.
    pub fn sequence(self) -> u64 {
        self.0 & Self::MAX_SEQUENCE
    }
}

impl fmt::Debug for DataflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataflowId(node={}, seq={})",
            self.node_index(),
            self.sequence()
        )
    }
}

impl fmt::Display for DataflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "df-{}-{}", self.node_index(), self.sequence())
    }
}

/// Allocates cluster-unique [`DataflowId`]s for a specific node.
///
/// Each node in the cluster creates one allocator at startup. The allocator
/// uses an atomic counter to generate monotonically increasing IDs without locks.
#[derive(Debug)]
pub struct DataflowIdAllocator {
    node_index: u16,
    next_sequence: AtomicU64,
}

impl DataflowIdAllocator {
    /// Create a new allocator for the given node.
    ///
    /// # Panics
    ///
    /// Panics if `node_index` exceeds [`DataflowId::MAX_NODE_INDEX`].
    pub fn new(node_index: u16) -> Self {
        Self {
            node_index,
            next_sequence: AtomicU64::new(1), // Start at 1; 0 reserved for "no dataflow"
        }
    }

    /// Allocate the next DataflowId.
    ///
    /// # Panics
    ///
    /// Panics if the sequence counter overflows 48 bits (practically impossible).
    pub fn allocate(&self) -> DataflowId {
        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        assert!(
            seq <= DataflowId::MAX_SEQUENCE,
            "DataflowId sequence overflow on node {}",
            self.node_index
        );
        DataflowId::new(self.node_index, seq)
    }

    /// Get the node index this allocator serves.
    pub fn node_index(&self) -> u16 {
        self.node_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataflow_id_encoding() {
        let id = DataflowId::new(5, 42);
        assert_eq!(id.node_index(), 5);
        assert_eq!(id.sequence(), 42);
        assert_eq!(id.as_raw(), (5u64 << 48) | 42);
    }

    #[test]
    fn dataflow_id_from_raw_roundtrip() {
        let raw = 0x0003_0000_0000_0001u64; // node=3, seq=1
        let id = DataflowId::from_raw(raw);
        assert_eq!(id.node_index(), 3);
        assert_eq!(id.sequence(), 1);
        assert_eq!(id.as_raw(), raw);
    }

    #[test]
    fn dataflow_id_max_values() {
        let id = DataflowId::new(u16::MAX, DataflowId::MAX_SEQUENCE);
        assert_eq!(id.node_index(), u16::MAX);
        assert_eq!(id.sequence(), DataflowId::MAX_SEQUENCE);
    }

    #[test]
    #[should_panic(expected = "exceeds 48-bit limit")]
    fn dataflow_id_sequence_overflow_panics() {
        DataflowId::new(0, DataflowId::MAX_SEQUENCE + 1);
    }

    #[test]
    fn dataflow_id_equality() {
        let a = DataflowId::new(1, 100);
        let b = DataflowId::new(1, 100);
        let c = DataflowId::new(1, 101);
        let d = DataflowId::new(2, 100);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }

    #[test]
    fn dataflow_id_ordering() {
        let a = DataflowId::new(0, 1);
        let b = DataflowId::new(0, 2);
        let c = DataflowId::new(1, 1);
        assert!(a < b);
        assert!(b < c); // node 1 > node 0 in high bits
    }

    #[test]
    fn dataflow_id_display() {
        let id = DataflowId::new(7, 123);
        assert_eq!(format!("{id}"), "df-7-123");
    }

    #[test]
    fn dataflow_id_debug() {
        let id = DataflowId::new(2, 50);
        let dbg = format!("{id:?}");
        assert!(dbg.contains("node=2"));
        assert!(dbg.contains("seq=50"));
    }

    #[test]
    fn allocator_generates_unique_ids() {
        let alloc = DataflowIdAllocator::new(3);
        let id1 = alloc.allocate();
        let id2 = alloc.allocate();
        let id3 = alloc.allocate();

        assert_eq!(id1.node_index(), 3);
        assert_eq!(id2.node_index(), 3);
        assert_eq!(id3.node_index(), 3);

        assert_eq!(id1.sequence(), 1);
        assert_eq!(id2.sequence(), 2);
        assert_eq!(id3.sequence(), 3);

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
    }

    #[test]
    fn allocator_different_nodes_no_collision() {
        let alloc_a = DataflowIdAllocator::new(0);
        let alloc_b = DataflowIdAllocator::new(1);

        let ids_a: Vec<_> = (0..100).map(|_| alloc_a.allocate()).collect();
        let ids_b: Vec<_> = (0..100).map(|_| alloc_b.allocate()).collect();

        // No overlap between nodes
        for a in &ids_a {
            for b in &ids_b {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn allocator_concurrent_allocation() {
        use std::sync::Arc;
        use std::collections::HashSet;

        let alloc = Arc::new(DataflowIdAllocator::new(5));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let a = alloc.clone();
            handles.push(std::thread::spawn(move || {
                (0..1000).map(|_| a.allocate()).collect::<Vec<_>>()
            }));
        }

        let mut all_ids = HashSet::new();
        for h in handles {
            for id in h.join().unwrap() {
                assert!(all_ids.insert(id), "duplicate DataflowId: {id:?}");
            }
        }
        assert_eq!(all_ids.len(), 8000);
    }

    #[test]
    fn allocator_starts_at_one() {
        let alloc = DataflowIdAllocator::new(0);
        let first = alloc.allocate();
        assert_eq!(first.sequence(), 1, "sequence 0 is reserved");
    }

    #[test]
    fn dataflow_id_hash_usable_in_collections() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let id = DataflowId::new(1, 42);
        map.insert(id, "test");
        assert_eq!(map.get(&id), Some(&"test"));
    }
}
