//! Shared-transport clusters of real node processes: a healthy run, a killed
//! node, a bystander dataflow, a restart, and kills during spawn and teardown
//! (scenarios 1–5).
//!
//! Every node-to-node connection goes through the coordinator's relay. The
//! coordinator acts as the membership layer: after a kill it reports the
//! node left to every survivor, and after a restart it reports it joined.

mod support;

use std::time::Duration;

use instancy_integration::coordinator::{Outcome, TestCoordinator, OUTCOME_DEADLINE};
use instancy_integration::node_actor::SPAWN_HANDSHAKE_TIMEOUT;
use instancy_integration::protocol::NodeResponse;
use instancy_integration::workloads::WorkloadKind;
use support::scenarios::{
    self, all_ok, check, check_all, fail, finish, spawn_every_kind, spawn_run, topology, workload,
    CONNECTIONS, EPOCHS, WORKERS,
};
use tokio::time::{timeout_at, Instant};

const NODES: [&str; 3] = ["a", "b", "c"];

async fn start(nodes: &[&str]) -> TestCoordinator {
    TestCoordinator::start_shared(nodes, WORKERS, CONNECTIONS).await
}

/// Scenario 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_shared_cluster_gives_exact_output() {
    let mut coord = start(&["a", "b", "c", "d"]).await;
    scenarios::healthy_cluster(&mut coord)
        .await
        .unwrap_or_else(|e| fail(e));
    finish(coord).await;
}

/// Scenario 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_node_fails_its_dataflows_on_every_survivor() {
    let mut coord = start(&NODES).await;
    scenarios::killed_node(&mut coord)
        .await
        .unwrap_or_else(|e| fail(e));
    finish(coord).await;
}

/// Scenario 8: b is killed behind a partition (every connection to and from
/// it stalled), so a and c learn of it only from the membership layer: each
/// must cancel, and fail, every dataflow b was in. b dials a and c dials b,
/// so the partition cuts a route each way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_killed_behind_a_partition_is_cancelled_by_the_leave() {
    let mut coord = start(&NODES).await;
    let cancelled = scenarios::partition_kill(&mut coord, "b")
        .await
        .unwrap_or_else(|e| fail(e));
    println!("dataflows each survivor's leave report cancelled: {cancelled:?}");
    finish(coord).await;
}

/// Scenario 3: a dataflow whose topology leaves out the killed node finishes
/// exact, while one that includes it fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_without_the_killed_node_finishes_exact() {
    let mut coord = start(&NODES).await;
    let df1 = spawn_run(
        &mut coord,
        "df1",
        &NODES,
        WorkloadKind::ExchangeRoundTrip,
        1,
    )
    .await;
    let df2 = spawn_run(
        &mut coord,
        "df2",
        &["a", "b"],
        WorkloadKind::MultiEpochExchange,
        2,
    )
    .await;
    coord.feed(&df1.id, &df1.workload, 0..EPOCHS).await;
    coord.feed(&df2.id, &df2.workload, 0..EPOCHS).await;

    coord.kill_node("c").await;
    coord.report_leave("c").await;
    coord.close_inputs(&df2.id, &["a", "b"]).await;

    check(&mut coord, &df2, false)
        .await
        .unwrap_or_else(|e| fail(e));
    let outcomes = coord
        .wait_for_outcomes(&df1.id, &NODES, OUTCOME_DEADLINE)
        .await
        .unwrap_or_else(|hang| panic!("df1 hung after c was killed: {hang:?}"));
    assert!(
        matches!(outcomes["a"], Outcome::Err(_)) && matches!(outcomes["b"], Outcome::Err(_)),
        "df1 used the killed node with its inputs open, so a and b must fail: {outcomes:?}"
    );
    assert_eq!(outcomes["c"], Outcome::Dead, "{outcomes:?}");

    finish(coord).await;
}

/// Scenario 3, after the fact: once a node is killed and reported left, the
/// survivors run a new dataflow among themselves, exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn survivors_run_a_new_dataflow_before_rejoin() {
    let mut coord = start(&NODES).await;
    let df1 = spawn_run(&mut coord, "df1", &NODES, WorkloadKind::Join, 1).await;
    coord.feed(&df1.id, &df1.workload, 0..EPOCHS).await;

    coord.kill_node("c").await;
    coord.report_leave("c").await;
    let outcomes = coord
        .wait_for_outcomes(&df1.id, &NODES, OUTCOME_DEADLINE)
        .await
        .unwrap_or_else(|hang| panic!("df1 hung after c was killed: {hang:?}"));
    assert!(
        matches!(outcomes["a"], Outcome::Err(_)) && matches!(outcomes["b"], Outcome::Err(_)),
        "df1 used the killed node with its inputs open, so a and b must fail: {outcomes:?}"
    );

    let df2 = spawn_run(&mut coord, "df2", &["a", "b"], WorkloadKind::WordCount, 2).await;
    coord.feed(&df2.id, &df2.workload, 0..EPOCHS).await;
    coord.close_inputs(&df2.id, &["a", "b"]).await;
    check(&mut coord, &df2, false)
        .await
        .unwrap_or_else(|e| fail(e));

    finish(coord).await;
}

/// Scenario 4: a killed node restarts under the same ID as a new member. Its
/// running dataflows failed; new ones include it and finish exact. Reusing
/// the id of a dataflow that finished is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restarted_node_runs_new_dataflows() {
    let mut coord = start(&NODES).await;
    let before = spawn_run(
        &mut coord,
        "df-before",
        &NODES,
        WorkloadKind::ExchangeRoundTrip,
        1,
    )
    .await;
    coord.feed(&before.id, &before.workload, 0..EPOCHS).await;
    coord.close_inputs(&before.id, &NODES).await;
    check(&mut coord, &before, false)
        .await
        .unwrap_or_else(|e| fail(e));

    let running = spawn_run(
        &mut coord,
        "df-running",
        &NODES,
        WorkloadKind::MultiEpochExchange,
        2,
    )
    .await;
    coord.feed(&running.id, &running.workload, 0..EPOCHS).await;
    coord.kill_node("c").await;
    coord.report_leave("c").await;
    let outcomes = coord
        .wait_for_outcomes(&running.id, &["a", "b"], OUTCOME_DEADLINE)
        .await
        .unwrap_or_else(|hang| panic!("df-running hung after c was killed: {hang:?}"));
    assert!(
        outcomes
            .values()
            .all(|outcome| matches!(outcome, Outcome::Err(_))),
        "df-running used the killed node with its inputs open, so a and b must fail: {outcomes:?}"
    );

    coord.restart_node("c").await;
    let joined = coord.report_join("c").await;
    assert_eq!(
        joined,
        [("a".to_string(), true), ("b".to_string(), true)].into(),
        "a and b had c as left"
    );

    let after = spawn_run(&mut coord, "df-after", &NODES, WorkloadKind::Join, 3).await;
    coord.feed(&after.id, &after.workload, 0..EPOCHS).await;
    coord.close_inputs(&after.id, &NODES).await;
    check(&mut coord, &after, false)
        .await
        .unwrap_or_else(|e| fail(e));

    // a and b remember df-before as finished, so claiming the id fails at
    // once and they send nothing. The refusal reads "dataflow ... already ran
    // with peer ...": `id_unavailable` in
    // instancy/src/communication/shared_transport.rs, returned by the claim
    // loop of `spawn_cluster_async` in instancy/src/runtime.rs.
    // The new c never ran df-before: it registers the id and fails at its
    // handshake timeout ("handshake/ready timeout waiting for peer", in
    // instancy/src/communication/control_protocol.rs), since a and b send
    // nothing for it.
    const HANDSHAKE_MS: u64 = 3_000;
    let handshake = Duration::from_millis(HANDSHAKE_MS);
    let sent = Instant::now();
    let replies = coord
        .spawn_shared_requests(
            &before.id,
            &topology(&NODES),
            before.workload.dataflow_type.clone(),
            HANDSHAKE_MS,
        )
        .await;
    for (node, reply) in replies {
        let response = timeout_at(sent + handshake + Duration::from_secs(5), reply)
            .await
            .unwrap_or_else(|_| {
                panic!("{node} gave no response to the reused df-before within 8 s")
            })
            .expect("the control reader ended without answering");
        let NodeResponse::Error { message } = response else {
            panic!("SpawnShared reusing the finished df-before must fail on {node}: {response:?}");
        };
        assert!(
            message.starts_with("spawn_cluster failed:"),
            "{node}'s error must come from its spawn: {message}"
        );
        if node == "c" {
            assert!(
                message.contains("handshake/ready timeout waiting for peer"),
                "c must fail at its handshake timeout: {message}"
            );
        } else {
            assert!(
                message.contains("already ran with peer"),
                "{node} must refuse the id of a finished run: {message}"
            );
        }
    }

    finish(coord).await;
}

/// Scenario 5: a node killed while every node is spawning a dataflow. The
/// survivors end with a failed spawn or a failed dataflow within the
/// deadline, never `Ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_during_spawn_ends_in_error() {
    let mut coord = start(&NODES).await;
    let id = "df-spawning";
    let spawning = workload(WorkloadKind::ExchangeRoundTrip, &NODES, 1);
    let replies = coord
        .spawn_shared_requests(
            id,
            &topology(&NODES),
            spawning.dataflow_type.clone(),
            SPAWN_HANDSHAKE_TIMEOUT.as_millis() as u64,
        )
        .await;

    coord.kill_node("c").await;
    coord.report_leave("c").await;

    let deadline = Instant::now() + OUTCOME_DEADLINE;
    for (node, reply) in replies {
        if node == "c" {
            continue;
        }
        let response = timeout_at(deadline, reply)
            .await
            .unwrap_or_else(|_| panic!("{node} gave no spawn response within the deadline"))
            .expect("the control reader ended without answering");
        match response {
            // The node's own spawn failed: the prefix is the node's, never
            // the coordinator's error for a closed connection.
            NodeResponse::Error { message } if message.starts_with("spawn_cluster failed:") => {}
            NodeResponse::DataflowSpawned { .. } => {
                let left = deadline.saturating_duration_since(Instant::now());
                let outcomes = coord
                    .wait_for_outcomes(id, &[node.as_str()], left)
                    .await
                    .unwrap_or_else(|hang| {
                        panic!("{node} spawned {id} but gave no outcome in time: {hang:?}")
                    });
                assert!(
                    matches!(outcomes[&node], Outcome::Err(_)),
                    "{id} must fail on {node}, since c was killed during its spawn: {outcomes:?}"
                );
            }
            other => panic!("unexpected SpawnShared response from {node}: {other:?}"),
        }
    }

    finish(coord).await;
}

/// Scenario 5: a node killed right after every input closed. Each dataflow
/// ends exact or failed on every node, within the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_during_teardown_ends_exact_or_error() {
    let mut coord = start(&NODES).await;
    let runs = spawn_every_kind(&mut coord, "teardown", &NODES).await;
    for run in &runs {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }
    for run in &runs {
        coord.close_inputs(&run.id, &NODES).await;
    }

    coord.kill_node("c").await;
    coord.report_leave("c").await;

    all_ok(check_all(&mut coord, &runs, true).await).unwrap_or_else(|e| fail(e));

    finish(coord).await;
}
