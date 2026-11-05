//! Shared-transport clusters with one slow, stalled or failed node-to-node
//! connection (scenarios 6, 7, 9 and 10).
//!
//! Faults go to the connections on the relay route from b to a: b has the
//! higher ID, so it dials a. A failed connection is not a node departure,
//! so these tests report no membership events.

mod support;

use std::time::Duration;

use instancy_integration::coordinator::TestCoordinator;
use instancy_integration::relay::{LinkAction, Relay, RelayConnId};
use support::scenarios::{self, fail, finish, StallFrom, CONNECTIONS, WORKERS};

const NODES: [&str; 3] = ["a", "b", "c"];

/// A fault a scenario aims at a connection that has ended did not happen:
/// the scenario fails, naming itself, the route and the connection, instead
/// of passing without the fault.
#[tokio::test]
async fn a_fault_on_an_ended_connection_fails_the_scenario() {
    let relay = Relay::start().await;
    let ended = RelayConnId(7);
    let problem = scenarios::apply(&relay, "slow_link", ("b", "a"), &[ended], LinkAction::Stall)
        .expect_err("a stall of a connection that is not live must fail the scenario");
    for part in ["slow_link", "b->a", "RelayConnId(7)", "Stall"] {
        assert!(problem.contains(part), "{problem:?} must name {part}");
    }
}

/// Scenario 6: `action` slows the connections from b to a while every
/// workload kind runs on a, b and c.
async fn slow_link(action: LinkAction) {
    let mut coord = TestCoordinator::start_shared(&NODES, WORKERS, CONNECTIONS).await;
    scenarios::slow_link(&mut coord, action)
        .await
        .unwrap_or_else(|e| fail(e));
    finish(coord).await;
}

/// Scenario 7: `action` ends the one connection from b to a, which two
/// dataflows on {a, b} share.
async fn failed_connection(action: LinkAction) {
    let mut coord = TestCoordinator::start_shared(&NODES, WORKERS, 1).await;
    scenarios::failed_connection(&mut coord, action)
        .await
        .unwrap_or_else(|e| fail(e));
    finish(coord).await;
}

/// Scenario 6, delayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_link_gives_exact_output() {
    slow_link(LinkAction::Delay(Duration::from_millis(50))).await;
}

/// Scenario 6, throttled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn throttled_link_gives_exact_output() {
    slow_link(LinkAction::Throttle {
        bytes_per_sec: 65_536,
    })
    .await;
}

/// Scenario 6, stalled and then healed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_then_healed_link_gives_exact_output() {
    slow_link(LinkAction::Stall).await;
}

/// Scenario 7, reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_connection_fails_only_dataflows_between_its_nodes() {
    failed_connection(LinkAction::Reset).await;
}

/// Scenario 7, closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_connection_fails_only_dataflows_between_its_nodes() {
    failed_connection(LinkAction::Close).await;
}

/// Scenario 9: one of the two connections from b to a stalls for longer
/// than the spawn handshake timeout while every dataflow on {a, b} is fed,
/// closed and finishing, then heals. A stall touches nothing: every dataflow
/// must still end `Ok` with exact output on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_stall_then_heal_gives_exact_output() {
    long_stall(StallFrom::BeforeInput).await;
}

/// Scenario 9, stalled right after every input closed, as the dataflows
/// finish, so a side that has ended may have its last frames held for
/// longer than the handshake timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_stall_as_dataflows_finish_gives_exact_output() {
    long_stall(StallFrom::AfterClose).await;
}

async fn long_stall(from: StallFrom) {
    let mut coord = TestCoordinator::start_shared(&NODES, WORKERS, CONNECTIONS).await;
    let ended = scenarios::long_stall(&mut coord, from)
        .await
        .unwrap_or_else(|e| fail(e));
    println!("{from:?}: nodes each dataflow on {{a, b}} had ended on before the heal: {ended:?}");
    finish(coord).await;
}

/// Scenario 10 (R58 of the connection-liveness design): with kept managers,
/// dataflow Y ends on a while a one-way stall of the connection carrying
/// dataflow X from a to b has blocked a's bridge task, holding Y's goodbye
/// and acknowledgement to b behind X's frames, for longer than the
/// handshake timeout; Y's own connection stays healthy. After the heal, Y
/// must still end `Ok` with exact output on b, and X on both nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "reproduces R58 (connection-liveness design): fails until that fix lands"]
async fn finished_dataflow_survives_a_one_way_stall_longer_than_handshake() {
    let mut coord = TestCoordinator::start_shared(&NODES[..2], WORKERS, CONNECTIONS).await;
    let steps = scenarios::finished_behind_a_one_way_stall(&mut coord)
        .await
        .unwrap_or_else(|e| fail(e));
    println!("steps:\n  {}", steps.join("\n  "));
    finish(coord).await;
}
