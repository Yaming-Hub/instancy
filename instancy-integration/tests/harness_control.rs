//! Tests of the harness itself: nodes serve commands while a dataflow runs,
//! report completions as events, drain their outputs, and have their logs read.

use std::time::Duration;

use instancy_integration::coordinator::{Hang, Outcome, TestCoordinator, OUTCOME_DEADLINE};
use instancy_integration::protocol::*;
use instancy_integration::test_helpers::{decode_data, encode_data, uniform_topology};
use instancy_integration::workloads::{Workload, WorkloadKind};
use tokio::time::{sleep, timeout, Instant};

const NODES: [&str; 2] = ["node-a", "node-b"];

/// The output channel holds 256 events, so 2,000 batches only get through if
/// the node drains its outputs without waiting for the coordinator to collect.
#[tokio::test]
async fn large_output_completes_without_collect() {
    let mut coord = TestCoordinator::start(&NODES, 1).await;
    let topology = uniform_topology(&NODES, 1);
    coord
        .setup_and_spawn_dataflow("df-large-output", &topology, DataflowType::PassThrough)
        .await;

    let records: Vec<Vec<u8>> = (0..2_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    for record in &records {
        coord
            .feed_data(
                "node-a",
                "df-large-output",
                0,
                "data",
                0,
                encode_data(std::slice::from_ref(record)),
            )
            .await;
    }
    coord.close_all_inputs("df-large-output").await;

    let outcomes = coord
        .wait_for_outcomes("df-large-output", &NODES, OUTCOME_DEADLINE)
        .await
        .unwrap_or_else(|hang| panic!("df-large-output hung: {hang:?}"));
    assert_eq!(outcomes["node-a"], Outcome::Ok, "{outcomes:?}");
    assert_eq!(outcomes["node-b"], Outcome::Ok, "{outcomes:?}");

    let output = coord
        .collect_output("node-a", "df-large-output", 0, "results")
        .await;
    let mut actual: Vec<Vec<u8>> = Vec::new();
    for (timestamp, batch) in decode_data::<Vec<u8>>(&output) {
        assert_eq!(timestamp, 0, "every record was fed at timestamp 0");
        actual.extend(batch);
    }
    actual.sort();
    assert_eq!(actual.len(), records.len());
    assert_eq!(actual, records);

    coord.shutdown().await;
}

/// A node answers commands while one of its dataflows is running and the
/// coordinator is waiting for it.
#[tokio::test]
async fn commands_are_served_while_a_dataflow_runs() {
    let mut coord = TestCoordinator::start(&NODES, 1).await;
    let topology = uniform_topology(&NODES, 1);
    coord
        .setup_and_spawn_dataflow("df-running", &topology, DataflowType::ExchangeRoundTrip)
        .await;
    for (key, node_id) in NODES.iter().enumerate() {
        coord
            .feed_data(
                node_id,
                "df-running",
                0,
                "data",
                0,
                encode_data(&[(key as u64, format!("{node_id}-record"))]),
            )
            .await;
    }

    // The inputs are open, so the dataflow can't finish.
    let hang = coord
        .wait_for_outcomes("df-running", &NODES, Duration::from_secs(1))
        .await
        .expect_err("df-running finished with its inputs open");
    assert_eq!(
        hang,
        Hang {
            missing: vec!["node-a".to_string(), "node-b".to_string()]
        }
    );

    for node_id in NODES {
        let response = timeout(
            Duration::from_secs(5),
            coord.send_command(
                node_id,
                NodeCommand::CollectOutput {
                    dataflow_id: "df-running".into(),
                    worker_idx: 0,
                    port_name: "results".into(),
                },
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("{node_id} didn't answer CollectOutput within 5 s"));
        assert!(
            matches!(response, NodeResponse::OutputData { .. }),
            "unexpected CollectOutput response from {node_id}: {response:?}"
        );
    }

    coord.shutdown().await;
}

/// A killed node's outcome is `Dead`; its peer fails instead of hanging.
#[tokio::test]
async fn killed_node_is_reported_dead() {
    let mut coord = TestCoordinator::start(&NODES, 1).await;
    let topology = uniform_topology(&NODES, 1);
    coord
        .setup_and_spawn_dataflow("df-killed", &topology, DataflowType::ExchangeRoundTrip)
        .await;
    for (key, node_id) in NODES.iter().enumerate() {
        coord
            .feed_data(
                node_id,
                "df-killed",
                0,
                "data",
                0,
                encode_data(&[(key as u64, format!("{node_id}-record"))]),
            )
            .await;
    }

    coord.kill_node("node-b").await;

    let outcomes = coord
        .wait_for_outcomes("df-killed", &NODES, OUTCOME_DEADLINE)
        .await
        .unwrap_or_else(|hang| panic!("node-a hung after node-b was killed: {hang:?}"));
    assert_eq!(outcomes["node-b"], Outcome::Dead, "{outcomes:?}");
    assert!(
        matches!(outcomes["node-a"], Outcome::Err(_)),
        "node-a should fail after node-b was killed: {outcomes:?}"
    );

    coord.shutdown().await;
}

/// A node's stdout and stderr are read while it runs, so a chatty node never
/// blocks on a full pipe.
#[tokio::test]
async fn node_logs_are_drained_while_it_runs() {
    let started = Instant::now();
    let coord = TestCoordinator::start(&NODES, 1).await;

    loop {
        let log = coord.node_log("node-a");
        if log.contains("instancy-test-node node-a ready") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "node-a's log has no ready line 5 s after startup:\n{log}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    coord.shutdown().await;
}

/// The deadlines the misbehaving-shutdown tests shut down with.
const SHORT: Duration = Duration::from_secs(2);

/// Start node-a and node-b, have node-b answer `Shutdown` as `behavior`
/// says, and shut both down with [`SHORT`] deadlines. Panics unless that
/// fails, naming node-b and not node-a; returns the failure.
async fn shutdown_fails_on_node_b(behavior: ShutdownBehavior) -> String {
    let mut coord = TestCoordinator::start(&NODES, 1).await;
    match coord
        .send_command("node-b", NodeCommand::SetShutdownBehavior(behavior))
        .await
    {
        NodeResponse::ShutdownBehaviorSet => {}
        other => panic!("SetShutdownBehavior failed on node-b: {other:?}"),
    }
    let problem = coord
        .try_shutdown_within(SHORT, SHORT)
        .await
        .expect_err("node-b misbehaved at shutdown, which must fail it");
    assert!(
        problem.contains("node-b") && !problem.contains("node-a"),
        "the failure must name node-b, and only node-b: {problem}"
    );
    problem
}

/// A node that crashes as it shuts down, before it answers, fails the
/// shutdown.
#[tokio::test]
async fn shutdown_fails_when_a_node_crashes_before_answering() {
    let problem = shutdown_fails_on_node_b(ShutdownBehavior {
        reply: false,
        exit_code: Some(3),
    })
    .await;
    assert!(
        problem.contains("closed its control connection"),
        "{problem}"
    );
}

/// A node that never answers `Shutdown` (its shutdown hangs) fails the
/// shutdown at the deadline.
#[tokio::test]
async fn shutdown_fails_when_a_node_never_answers() {
    let problem = shutdown_fails_on_node_b(ShutdownBehavior {
        reply: false,
        exit_code: None,
    })
    .await;
    assert!(problem.contains("did not answer Shutdown"), "{problem}");
}

/// A node that answers and then exits with a failure fails the shutdown.
#[tokio::test]
async fn shutdown_fails_when_a_node_exits_with_failure() {
    let problem = shutdown_fails_on_node_b(ShutdownBehavior {
        reply: true,
        exit_code: Some(3),
    })
    .await;
    assert!(problem.contains("exited with"), "{problem}");
}

/// A node that answers and then never exits fails the shutdown at the
/// deadline.
#[tokio::test]
async fn shutdown_fails_when_a_node_never_exits() {
    let problem = shutdown_fails_on_node_b(ShutdownBehavior {
        reply: true,
        exit_code: None,
    })
    .await;
    assert!(problem.contains("did not exit"), "{problem}");
}

/// Only processes the test left running are held to the shutdown: a killed
/// node, and one the test already shut down, are not sent `Shutdown` again.
#[tokio::test]
async fn shutdown_holds_only_nodes_the_test_left_running() {
    let mut coord = TestCoordinator::start(&["node-a", "node-b", "node-c"], 1).await;
    coord.kill_node("node-b").await;
    match coord.send_command("node-c", NodeCommand::Shutdown).await {
        NodeResponse::Error { message } if message == "shutdown" => {}
        other => panic!("node-c did not shut down: {other:?}"),
    }
    coord
        .try_shutdown()
        .await
        .unwrap_or_else(|problem| panic!("{problem}"));
}

/// The pinned lanes of each of `node`'s managers, as (node set, peer, lanes
/// on each connection by id).
async fn pins(coord: &mut TestCoordinator, node: &str) -> Vec<(String, String, Vec<usize>)> {
    coord
        .pool_snapshots(node)
        .await
        .unwrap_or_else(|problem| panic!("{problem}"))
        .into_iter()
        .map(|pool| {
            let lanes = pool.connections.iter().map(|c| c.pinned_lanes).collect();
            (pool.node_set, pool.peer, lanes)
        })
        .collect()
}

/// `SnapshotPools` shows each Shared-transport manager a node holds, with
/// its connections and the lanes pinned to each: no manager before the first
/// dataflow; then, on each node, the one manager of node set {a, b}, with its
/// two connections, each dataflow's lane on the one with the fewest; and no
/// lane once the dataflows have ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_snapshots_show_each_managers_connections_and_pinned_lanes() {
    let nodes = ["a", "b"];
    let mut coord = TestCoordinator::start_shared(&nodes, 1, 2).await;
    for node in nodes {
        assert_eq!(
            pins(&mut coord, node).await,
            [],
            "{node} before any dataflow"
        );
    }
    let topology = uniform_topology(&nodes, 1);
    let workload = Workload::new(WorkloadKind::ExchangeRoundTrip, &[("a", 1), ("b", 1)], 1, 1);
    let pool = |node: &str, lanes: Vec<usize>| {
        let peer = if node == "a" { "b" } else { "a" };
        vec![("a,b".to_string(), peer.to_string(), lanes)]
    };

    coord.spawn_shared("first", &topology, &workload).await;
    for node in nodes {
        let mut found = pins(&mut coord, node).await;
        if let Some((_, _, lanes)) = found.first_mut() {
            lanes.sort_unstable();
        }
        assert_eq!(
            found,
            pool(node, vec![0, 1]),
            "{node} after the first dataflow"
        );
    }
    coord.spawn_shared("second", &topology, &workload).await;
    for node in nodes {
        assert_eq!(
            pins(&mut coord, node).await,
            pool(node, vec![1, 1]),
            "{node} after the second dataflow"
        );
    }

    for id in ["first", "second"] {
        coord.feed(id, &workload, 0..1).await;
        coord.close_inputs(id, &nodes).await;
        coord.wait_for_completion(id).await;
    }
    for node in nodes {
        assert_eq!(
            pins(&mut coord, node).await,
            pool(node, vec![0, 0]),
            "{node} once both dataflows have ended"
        );
    }
    coord.shutdown().await;
}
