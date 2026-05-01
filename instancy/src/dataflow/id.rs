//! Dataflow identity.
//!
//! Each running dataflow instance gets a universally unique [`DataflowId`]
//! backed by a random UUID v4. No coordination or allocation is needed —
//! any node can create a new dataflow independently.

use std::fmt;

use uuid::Uuid;

/// Universally unique identifier for a running dataflow instance.
///
/// This is a **logical** concept — it identifies a specific computation graph
/// instance. Multiple dataflows with different IDs can run concurrently on
/// the same physical infrastructure.
///
/// # Uniqueness
///
/// Uses UUID v4 (122 bits of randomness). Collision probability is negligible
/// even across billions of dataflows — no coordination between nodes is needed.
///
/// # Wire format
///
/// Serialized as 16 bytes (big-endian UUID) in the frame header.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DataflowId(Uuid);

impl DataflowId {
    /// Create a new random DataflowId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a DataflowId from raw 16 bytes (e.g., received from wire).
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Get the raw 16-byte representation (for wire protocol encoding).
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Create a DataflowId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Create a nil (all-zeros) DataflowId, used as a sentinel/placeholder.
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Check if this is the nil DataflowId.
    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl Default for DataflowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DataflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataflowId({})", &self.0.to_string()[..8])
    }
}

impl fmt::Display for DataflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "df-{}", &self.0.to_string()[..8])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn new_generates_unique_ids() {
        let ids: HashSet<_> = (0..1000).map(|_| DataflowId::new()).collect();
        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn from_bytes_roundtrip() {
        let id = DataflowId::new();
        let bytes = *id.as_bytes();
        let restored = DataflowId::from_bytes(bytes);
        assert_eq!(id, restored);
    }

    #[test]
    fn nil_is_distinguishable() {
        let nil = DataflowId::nil();
        assert!(nil.is_nil());
        let real = DataflowId::new();
        assert!(!real.is_nil());
        assert_ne!(nil, real);
    }

    #[test]
    fn equality_and_hash() {
        let id = DataflowId::new();
        let same = DataflowId::from_bytes(*id.as_bytes());
        let different = DataflowId::new();
        assert_eq!(id, same);
        assert_ne!(id, different);

        let mut map = HashMap::new();
        map.insert(id, "test");
        assert_eq!(map.get(&id), Some(&"test"));
        assert_eq!(map.get(&same), Some(&"test"));
        assert_eq!(map.get(&different), None);
    }

    #[test]
    fn ordering_is_deterministic() {
        let a = DataflowId::new();
        let b = DataflowId::new();
        // Just verify Ord doesn't panic and is consistent
        let cmp1 = a.cmp(&b);
        let cmp2 = a.cmp(&b);
        assert_eq!(cmp1, cmp2);
    }

    #[test]
    fn display_format() {
        let id = DataflowId::new();
        let s = format!("{id}");
        assert!(s.starts_with("df-"));
        assert_eq!(s.len(), 11); // "df-" + 8 hex chars
    }

    #[test]
    fn debug_format() {
        let id = DataflowId::new();
        let s = format!("{id:?}");
        assert!(s.starts_with("DataflowId("));
        assert!(s.ends_with(')'));
    }

    #[test]
    fn concurrent_creation_no_duplicates() {
        use std::sync::Arc;
        use std::sync::Mutex;

        let ids = Arc::new(Mutex::new(HashSet::new()));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let ids = ids.clone();
            handles.push(std::thread::spawn(move || {
                let local: Vec<_> = (0..1000).map(|_| DataflowId::new()).collect();
                ids.lock().unwrap().extend(local);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(ids.lock().unwrap().len(), 8000);
    }

    #[test]
    fn from_uuid_roundtrip() {
        let uuid = Uuid::new_v4();
        let id = DataflowId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn default_creates_new() {
        let a = DataflowId::default();
        let b = DataflowId::default();
        assert_ne!(a, b); // two defaults are different (both random)
    }
}
