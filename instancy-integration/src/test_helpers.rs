//! Test helper utilities for integration tests.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::protocol::{SerializableNodeConfig, SerializableTopology};

/// Default timeout for integration test operations.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Make a SerializableTopology with uniform workers per node.
pub fn uniform_topology(node_ids: &[&str], workers_per_node: usize) -> SerializableTopology {
    SerializableTopology {
        nodes: node_ids
            .iter()
            .map(|node_id| SerializableNodeConfig {
                node_id: (*node_id).to_string(),
                num_workers: workers_per_node,
            })
            .collect(),
    }
}

/// Make a SerializableTopology with per-node worker counts.
pub fn asymmetric_topology(nodes: &[(&str, usize)]) -> SerializableTopology {
    SerializableTopology {
        nodes: nodes
            .iter()
            .map(|(node_id, num_workers)| SerializableNodeConfig {
                node_id: (*node_id).to_string(),
                num_workers: *num_workers,
            })
            .collect(),
    }
}

/// Encode test data as bincode for FeedData commands.
pub fn encode_data<T: serde::Serialize>(data: &[T]) -> Vec<u8> {
    bincode::serialize(data).expect("failed to encode test data")
}

/// Decode output data from CollectOutput responses.
pub fn decode_data<T: serde::de::DeserializeOwned>(data: &[(u64, Vec<u8>)]) -> Vec<(u64, Vec<T>)> {
    data.iter()
        .map(|(timestamp, bytes)| {
            (
                *timestamp,
                bincode::deserialize(bytes).expect("failed to decode output data"),
            )
        })
        .collect()
}

/// Assert that collected output contains expected values (order-independent within each timestamp).
pub fn assert_output_matches<T: PartialEq + Ord + std::fmt::Debug>(
    actual: &[(u64, Vec<T>)],
    expected: &[(u64, Vec<T>)],
) {
    fn normalize<'a, T: Ord>(data: &'a [(u64, Vec<T>)]) -> BTreeMap<u64, Vec<&'a T>> {
        let mut grouped = BTreeMap::new();
        for (timestamp, batch) in data {
            grouped
                .entry(*timestamp)
                .or_insert_with(Vec::new)
                .extend(batch.iter());
        }
        for batch in grouped.values_mut() {
            batch.sort();
        }
        grouped
    }

    assert_eq!(normalize(actual), normalize(expected));
}
