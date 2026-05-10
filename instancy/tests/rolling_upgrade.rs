//! Integration test for rolling upgrade scenario.
//!
//! Simulates a typical rolling upgrade where:
//! 1. Cluster starts with node-A — run a dataflow on it.
//! 2. Node-B joins the cluster — verify topology expands.
//! 3. Node-A leaves the cluster — verify topology contracts.
//! 4. Run another dataflow on the surviving runtime (node-B perspective).
//!
//! This validates that the membership event flow correctly updates the live
//! topology and that the runtime remains fully operational throughout the
//! transition.

#![cfg(feature = "transport")]

use std::time::Duration;

use instancy::dataflow::DataflowBuilder;
use instancy::runtime::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::{
    ChannelMembership, ClusterTopology, MembershipEvent, NodeConfig, NodeDepartureReason,
};

/// Helper to run a single-node dataflow, feed data, and collect results.
fn run_local_dataflow(rt: &RuntimeHandle, input: Vec<i32>) -> Vec<i32> {
    let builder = DataflowBuilder::<u64>::new("local-df");
    let inp = builder.input::<i32>("data").unwrap();
    inp.map("double", |_t, x| x * 2).output("results").unwrap();
    let df = builder.build().unwrap();

    let mut handle = rt.spawn(df, SpawnOptions::default()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, input).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut results: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    results.sort();
    results
}

/// Rolling upgrade: node-A → add node-B → remove node-A → continue on node-B.
///
/// This tests the full lifecycle of a cluster topology change driven by
/// membership events, simulating the perspective of a single runtime instance
/// that observes the cluster evolving around it.
#[tokio::test]
async fn rolling_upgrade_topology_lifecycle() {
    // ── Phase 1: Start with node-A only ──
    let membership = ChannelMembership::new();
    let tx = membership.sender();

    let initial_topology = ClusterTopology::multi_node(vec![NodeConfig::new("node-a", 2)]).unwrap();

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        name: "rolling-upgrade".to_string(),
        topology: Some(initial_topology.with_membership(membership)),
        ..Default::default()
    })
    .unwrap();

    // Verify initial topology.
    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 1);
    assert!(topo.contains_node("node-a"));
    assert_eq!(topo.total_workers(), 2);

    // Run a dataflow on node-A — system is fully operational.
    let results = run_local_dataflow(&rt, vec![1, 2, 3]);
    assert_eq!(results, vec![2, 4, 6]);

    // ── Phase 2: Node-B joins ──
    tx.send(MembershipEvent::NodeJoined {
        node_id: "node-b".into(),
        logical_workers: 2,
    })
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 2);
    assert!(topo.contains_node("node-a"));
    assert!(topo.contains_node("node-b"));
    assert_eq!(topo.total_workers(), 4);

    // Runtime still works — run another local dataflow while both nodes are in topology.
    let results = run_local_dataflow(&rt, vec![10, 20]);
    assert_eq!(results, vec![20, 40]);

    // ── Phase 3: Node-A leaves (simulates the old node draining and shutting down) ──
    tx.send(MembershipEvent::NodeLeft {
        node_id: "node-a".into(),
        reason: NodeDepartureReason::Graceful,
    })
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 1);
    assert!(!topo.contains_node("node-a"));
    assert!(topo.contains_node("node-b"));
    assert_eq!(topo.total_workers(), 2);

    // ── Phase 4: Continue running on surviving topology ──
    // From node-B's perspective, it's still fully operational.
    let results = run_local_dataflow(&rt, vec![100, 200, 300]);
    assert_eq!(results, vec![200, 400, 600]);

    // Runtime is healthy throughout.
    assert!(!rt.is_shutdown());
}

/// Rolling upgrade with rapid node churn — multiple joins and leaves in sequence.
///
/// Simulates a deployment where nodes are replaced one at a time:
/// A,B → A,B,C → B,C (A removed) → B,C,D → C,D (B removed)
#[tokio::test]
async fn rolling_upgrade_sequential_replacements() {
    let membership = ChannelMembership::new();
    let tx = membership.sender();

    let initial_topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        name: "sequential-replace".to_string(),
        topology: Some(initial_topology.with_membership(membership)),
        ..Default::default()
    })
    .unwrap();

    let wait = || tokio::time::sleep(Duration::from_millis(100));

    // Initial: {A, B}
    assert_eq!(rt.current_topology().unwrap().node_count(), 2);

    // Run a dataflow — system works.
    let results = run_local_dataflow(&rt, vec![5]);
    assert_eq!(results, vec![10]);

    // Step 1: C joins → {A, B, C}
    tx.send(MembershipEvent::NodeJoined {
        node_id: "node-c".into(),
        logical_workers: 1,
    })
    .unwrap();
    wait().await;
    assert_eq!(rt.current_topology().unwrap().node_count(), 3);

    // Step 2: A leaves → {B, C}
    tx.send(MembershipEvent::NodeLeft {
        node_id: "node-a".into(),
        reason: NodeDepartureReason::Graceful,
    })
    .unwrap();
    wait().await;
    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 2);
    assert!(!topo.contains_node("node-a"));

    // Dataflow still works after first replacement.
    let results = run_local_dataflow(&rt, vec![7]);
    assert_eq!(results, vec![14]);

    // Step 3: D joins → {B, C, D}
    tx.send(MembershipEvent::NodeJoined {
        node_id: "node-d".into(),
        logical_workers: 1,
    })
    .unwrap();
    wait().await;
    assert_eq!(rt.current_topology().unwrap().node_count(), 3);

    // Step 4: B leaves → {C, D}
    tx.send(MembershipEvent::NodeLeft {
        node_id: "node-b".into(),
        reason: NodeDepartureReason::Graceful,
    })
    .unwrap();
    wait().await;
    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 2);
    assert!(topo.contains_node("node-c"));
    assert!(topo.contains_node("node-d"));
    assert!(!topo.contains_node("node-a"));
    assert!(!topo.contains_node("node-b"));
    assert_eq!(topo.total_workers(), 2);

    // Final dataflow — system fully operational with entirely new node set.
    let results = run_local_dataflow(&rt, vec![42]);
    assert_eq!(results, vec![84]);

    assert!(!rt.is_shutdown());
}

/// Node failure during rolling upgrade — ConnectionLost departure reason.
///
/// Simulates a crash during rolling upgrade: new node joins, old node crashes
/// (rather than graceful shutdown).
#[tokio::test]
async fn rolling_upgrade_with_node_failure() {
    let membership = ChannelMembership::new();
    let tx = membership.sender();

    let initial_topology = ClusterTopology::multi_node(vec![NodeConfig::new("node-a", 2)]).unwrap();

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        name: "failure-upgrade".to_string(),
        topology: Some(initial_topology.with_membership(membership)),
        ..Default::default()
    })
    .unwrap();

    // Run initial dataflow.
    let results = run_local_dataflow(&rt, vec![1, 2]);
    assert_eq!(results, vec![2, 4]);

    // New node joins.
    tx.send(MembershipEvent::NodeJoined {
        node_id: "node-b".into(),
        logical_workers: 2,
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(rt.current_topology().unwrap().node_count(), 2);

    // Old node crashes (connection lost, not graceful).
    tx.send(MembershipEvent::NodeLeft {
        node_id: "node-a".into(),
        reason: NodeDepartureReason::ConnectionLost,
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 1);
    assert!(topo.contains_node("node-b"));
    assert!(!topo.contains_node("node-a"));

    // Runtime recovers — new dataflows can still run.
    let results = run_local_dataflow(&rt, vec![10, 20, 30]);
    assert_eq!(results, vec![20, 40, 60]);

    assert!(!rt.is_shutdown());
}

/// Node rejoins after being removed — tests recovery path.
#[tokio::test]
async fn rolling_upgrade_node_rejoin() {
    let membership = ChannelMembership::new();
    let tx = membership.sender();

    let initial_topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        name: "rejoin-test".to_string(),
        topology: Some(initial_topology.with_membership(membership)),
        ..Default::default()
    })
    .unwrap();

    // Remove node-a.
    tx.send(MembershipEvent::NodeLeft {
        node_id: "node-a".into(),
        reason: NodeDepartureReason::Graceful,
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(rt.current_topology().unwrap().node_count(), 1);

    // Node-a comes back (e.g., pod restarted with new version).
    tx.send(MembershipEvent::NodeJoined {
        node_id: "node-a".into(),
        logical_workers: 2, // may rejoin with different capacity
    })
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let topo = rt.current_topology().unwrap();
    assert_eq!(topo.node_count(), 2);
    assert!(topo.contains_node("node-a"));
    assert!(topo.contains_node("node-b"));
    assert_eq!(topo.total_workers(), 3); // 2 (new a) + 1 (b)

    // Everything works.
    let results = run_local_dataflow(&rt, vec![3, 6, 9]);
    assert_eq!(results, vec![6, 12, 18]);

    assert!(!rt.is_shutdown());
}
