//! Scenario bodies for Shared-transport clusters, and the check every
//! dataflow of a scenario passes.
//!
//! Each body takes a coordinator that is already started with
//! `TestCoordinator::start_shared` and returns what went wrong as an `Err`.

use std::collections::BTreeMap;
use std::time::Duration;

use instancy_integration::coordinator::{Outcome, SpawnInfo, TestCoordinator, OUTCOME_DEADLINE};
use instancy_integration::dataflows::port_names;
use instancy_integration::node_actor::SPAWN_HANDSHAKE_TIMEOUT;
use instancy_integration::peer_link::node_set_key;
use instancy_integration::protocol::{
    NodeCommand, NodeResponse, PooledConnection, SerializableTopology,
};
use instancy_integration::relay::{Forwarded, LinkAction, Relay, RelayConnId};
use instancy_integration::test_helpers::uniform_topology;
use instancy_integration::workloads::{Feed, SplitMix64, Workload, WorkloadKind};
use tokio::time::{sleep, sleep_until, Instant};

/// Workers per node in every scenario's topology.
pub const WORKERS: usize = 2;

/// Pooled connections per pair of nodes.
pub const CONNECTIONS: usize = 2;

/// Epochs (timestamps) of input each dataflow gets.
pub const EPOCHS: u64 = 4;

/// How long [`slow_link`] keeps a stalled connection stalled before it heals it.
pub const STALL: Duration = Duration::from_secs(2);

/// How long [`long_stall`] keeps its connection stalled: longer than the
/// spawn handshake timeout, which bounds a node's waits on its peers.
pub const LONG_STALL: Duration = Duration::from_secs(15);

const _: () = assert!(LONG_STALL.as_secs() > SPAWN_HANDSHAKE_TIMEOUT.as_secs());

/// Every workload kind, in a fixed order, for a dataflow on `nodes` nodes of
/// [`WORKERS`] workers each (see [`WorkloadKind::all`]).
pub fn kinds(nodes: usize) -> [WorkloadKind; 6] {
    WorkloadKind::all(nodes * WORKERS)
}

/// The topology of `nodes`, with [`WORKERS`] workers each.
pub fn topology(nodes: &[&str]) -> SerializableTopology {
    uniform_topology(nodes, WORKERS)
}

/// A workload over `nodes`, with [`WORKERS`] workers each and [`EPOCHS`] epochs.
pub fn workload(kind: WorkloadKind, nodes: &[&str], seed: u64) -> Workload {
    let nodes: Vec<(&str, usize)> = nodes.iter().map(|&node| (node, WORKERS)).collect();
    Workload::new(kind, &nodes, EPOCHS, seed)
}

/// One spawned dataflow of a scenario.
pub struct Run {
    pub id: String,
    pub nodes: Vec<String>,
    pub workload: Workload,
    pub spawn: SpawnInfo,
}

impl Run {
    pub fn nodes(&self) -> Vec<&str> {
        self.nodes.iter().map(String::as_str).collect()
    }
}

/// Spawn a dataflow of `kind` on `nodes`. Panics unless every node spawns it.
pub async fn spawn_run(
    coord: &mut TestCoordinator,
    id: &str,
    nodes: &[&str],
    kind: WorkloadKind,
    seed: u64,
) -> Run {
    let workload = workload(kind, nodes, seed);
    let spawn = coord.spawn_shared(id, &topology(nodes), &workload).await;
    Run {
        id: id.to_string(),
        nodes: nodes.iter().map(|node| node.to_string()).collect(),
        workload,
        spawn,
    }
}

/// Spawn one dataflow of each workload kind on `nodes`, named `prefix-` and
/// the kind's name, seeded 1, 2, …. Panics unless every node spawns them.
pub async fn spawn_every_kind(
    coord: &mut TestCoordinator,
    prefix: &str,
    nodes: &[&str],
) -> Vec<Run> {
    let mut runs = Vec::new();
    for (seed, kind) in (1..).zip(kinds(nodes.len())) {
        let id = format!("{prefix}-{}", kind.name());
        runs.push(spawn_run(coord, &id, nodes, kind, seed).await);
    }
    runs
}

/// [`check_run`] for a [`Run`].
pub async fn check(coord: &mut TestCoordinator, run: &Run, touched: bool) -> Result<(), String> {
    check_run(
        coord,
        &run.id,
        &run.nodes(),
        &run.workload,
        &run.spawn,
        touched,
    )
    .await
}

/// [`check`] each of `runs`, their outcomes sharing one [`OUTCOME_DEADLINE`]
/// from now: several hung runs fail within it, not one after another.
pub async fn check_all(
    coord: &mut TestCoordinator,
    runs: &[Run],
    touched: bool,
) -> Vec<Result<(), String>> {
    let deadline = Instant::now() + OUTCOME_DEADLINE;
    let mut results = Vec::new();
    for run in runs {
        let left = deadline.saturating_duration_since(Instant::now());
        results.push(
            match coord.wait_for_outcomes(&run.id, &run.nodes(), left).await {
                Ok(_) => check(coord, run, touched).await,
                Err(hang) => Err(format!(
                    "{}: no outcome within {} s from {:?}",
                    run.id,
                    OUTCOME_DEADLINE.as_secs(),
                    hang.missing
                )),
            },
        );
    }
    results
}

/// Wait for every node's outcome of `dataflow_id` and check it:
///
/// - every node gives an outcome within [`OUTCOME_DEADLINE`]: a hang is an
///   error;
/// - each local worker of an `Ok` node output exactly what the workload says
///   it should;
/// - `Err` is allowed only if a fault `touched` the dataflow;
/// - a dead node, or one that reported `Ok` but no longer has its output, is
///   allowed only if `touched` and the test stopped the process that ran the
///   dataflow, by killing it or sending it `Shutdown` (its output went with
///   it);
/// - no node of the dataflow has a process that exited on its own.
pub async fn check_run(
    coord: &mut TestCoordinator,
    dataflow_id: &str,
    nodes: &[&str],
    workload: &Workload,
    spawn: &SpawnInfo,
    touched: bool,
) -> Result<(), String> {
    let outcomes = coord
        .wait_for_outcomes(dataflow_id, nodes, OUTCOME_DEADLINE)
        .await
        .map_err(|hang| {
            format!(
                "{dataflow_id}: no outcome within {} s from {:?}",
                OUTCOME_DEADLINE.as_secs(),
                hang.missing
            )
        })?;
    let mut problems = Vec::new();
    for (node, outcome) in &outcomes {
        let stopped = coord.stopped_by_test(node, dataflow_id);
        match outcome {
            Outcome::Ok if coord.holds_output(node, dataflow_id) => {
                problems.extend(check_output(coord, dataflow_id, node, workload, spawn).await);
            }
            Outcome::Ok | Outcome::Dead if !stopped => problems.push(format!(
                "{node} ended {outcome:?}, and its process ended without being killed or \
                 shut down by the test: it crashed"
            )),
            Outcome::Ok | Outcome::Dead if !touched => problems.push(format!(
                "{node} was killed or shut down by the test, but the dataflow is not marked \
                 touched"
            )),
            Outcome::Err(error) if !touched => {
                problems.push(format!("{node} failed with no fault: {error}"));
            }
            Outcome::Ok | Outcome::Dead | Outcome::Err(_) => {}
        }
    }
    let crashed: Vec<String> = coord
        .crashed_nodes()
        .into_iter()
        .filter(|node| nodes.contains(&node.as_str()))
        .collect();
    if !crashed.is_empty() {
        problems.push(format!(
            "processes of {crashed:?} exited without being killed or shut down"
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{dataflow_id} (touched: {touched}):\n  {}\noutcomes: {outcomes:?}",
            problems.join("\n  ")
        ))
    }
}

/// Collect the output of every local worker of `node` for `dataflow_id` and
/// check it against `workload`. Returns what is wrong with it.
pub async fn check_output(
    coord: &mut TestCoordinator,
    dataflow_id: &str,
    node: &str,
    workload: &Workload,
    spawn: &SpawnInfo,
) -> Vec<String> {
    let (_, port) = port_names(&workload.dataflow_type);
    let mut problems = Vec::new();
    for local in 0..spawn.local_workers[node] {
        let global = spawn.first_worker[node] + local;
        let response = coord
            .send_command(
                node,
                NodeCommand::CollectOutput {
                    dataflow_id: dataflow_id.to_string(),
                    worker_idx: local,
                    port_name: port.clone(),
                },
            )
            .await;
        let checked = match response {
            NodeResponse::OutputData { data } => {
                workload.check_worker(global, spawn.total_workers, &data)
            }
            other => Err(format!("CollectOutput failed: {other:?}")),
        };
        if let Err(problem) = checked {
            problems.push(format!(
                "{node} worker {local} (global {global}) reported Ok: {problem}"
            ));
        }
    }
    problems
}

/// Fails if any node's process exited without being killed or shut down by
/// the test.
pub fn no_crashes(coord: &mut TestCoordinator) -> Result<(), String> {
    let crashed = coord.crashed_nodes();
    if crashed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "processes of {crashed:?} exited without being killed or shut down"
        ))
    }
}

/// Fail the test with `problem`.
pub fn fail(problem: String) -> ! {
    panic!("{problem}")
}

/// Fail if a node's process exited without being killed or shut down by the
/// test, then shut down.
pub async fn finish(mut coord: TestCoordinator) {
    no_crashes(&mut coord).unwrap_or_else(|e| fail(e));
    coord.shutdown().await;
}

/// Collect every `Err` of `results` into one.
pub fn all_ok(results: Vec<Result<(), String>>) -> Result<(), String> {
    let problems: Vec<String> = results.into_iter().filter_map(Result::err).collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

/// Apply `action` to each of `conns`, connections from `from` to `to` that
/// `scenario` aims it at. Fails, naming the scenario, the route and the
/// connection, if one had ended before it: the fault did not happen, and a
/// scenario that went on would pass without it.
pub fn apply(
    relay: &Relay,
    scenario: &str,
    (from, to): (&str, &str),
    conns: &[RelayConnId],
    action: LinkAction,
) -> Result<(), String> {
    let ended: Vec<RelayConnId> = conns
        .iter()
        .copied()
        .filter(|&conn| !relay.apply(conn, action))
        .collect();
    if ended.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{scenario}: {action:?} was not applied to {ended:?} on the route {from}->{to}: \
             they had ended before it"
        ))
    }
}

/// Scenario 1: one dataflow of each workload kind on every node, all running
/// at once, each giving exact output on every worker.
pub async fn healthy_cluster(coord: &mut TestCoordinator) -> Result<(), String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let runs = spawn_every_kind(coord, "healthy", &nodes).await;
    for run in &runs {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }
    for run in &runs {
        coord.close_inputs(&run.id, &nodes).await;
    }
    let mut results = check_all(coord, &runs, false).await;
    results.push(no_crashes(coord));
    all_ok(results)
}

/// Scenario 2: one dataflow of each workload kind on every node, fed with its
/// inputs left open. The node with the highest ID is killed and reported
/// left. Every survivor must fail every dataflow within the deadline (`Ok` is
/// impossible: the killed node's inputs never closed), and the killed node is
/// `Dead`.
pub async fn killed_node(coord: &mut TestCoordinator) -> Result<(), String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let victim = *nodes.last().expect("killed_node needs a started cluster");
    let runs = spawn_every_kind(coord, "killed", &nodes).await;
    for run in &runs {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }

    coord.kill_node(victim).await;
    coord.report_leave(victim).await;

    let deadline = Instant::now() + OUTCOME_DEADLINE;
    let mut problems = Vec::new();
    for run in &runs {
        let left = deadline.saturating_duration_since(Instant::now());
        let outcomes = match coord.wait_for_outcomes(&run.id, &nodes, left).await {
            Ok(outcomes) => outcomes,
            Err(hang) => {
                problems.push(format!(
                    "{}: no outcome within {} s from {:?} after {victim} was killed",
                    run.id,
                    OUTCOME_DEADLINE.as_secs(),
                    hang.missing
                ));
                continue;
            }
        };
        for (node, outcome) in &outcomes {
            let expected = if node == victim {
                matches!(outcome, Outcome::Dead)
            } else {
                matches!(outcome, Outcome::Err(_))
            };
            if !expected {
                problems.push(format!(
                    "{}: {node} ended {outcome:?} after {victim} was killed with inputs open \
                     (survivors must fail, {victim} must be Dead)",
                    run.id
                ));
            }
        }
    }
    if let Err(problem) = no_crashes(coord) {
        problems.push(problem);
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

/// How long [`partition_kill`] watches the survivors after the kill, before
/// the leave: far longer than a death the relay let through takes to fail
/// them.
pub const QUIET: Duration = Duration::from_secs(1);

/// The relay route between nodes `x` and `y`: the higher ID dials the lower.
pub fn route<'a>(x: &'a str, y: &'a str) -> (&'a str, &'a str) {
    if x > y {
        (x, y)
    } else {
        (y, x)
    }
}

/// The live connections on each of `routes`, which `scenario` stalls.
type Stalled<'a> = Vec<((&'a str, &'a str), Vec<RelayConnId>)>;

/// Stall every live connection on each of `routes`. Fails if a route has
/// none, so would not be cut, or one ended before the stall.
fn stall_routes<'a>(
    coord: &TestCoordinator,
    scenario: &str,
    routes: &[(&'a str, &'a str)],
) -> Result<Stalled<'a>, String> {
    let mut stalled = Vec::new();
    for &(from, to) in routes {
        let conns = coord.relay().connections(from, to);
        if conns.is_empty() {
            return Err(format!(
                "{scenario}: no connection on the route {from}->{to} to stall"
            ));
        }
        apply(
            coord.relay(),
            scenario,
            (from, to),
            &conns,
            LinkAction::Stall,
        )?;
        stalled.push(((from, to), conns));
    }
    Ok(stalled)
}

/// Fails unless every connection of `stalled` is still live, `when`.
fn stalls_held(
    coord: &TestCoordinator,
    scenario: &str,
    stalled: &Stalled<'_>,
    when: &str,
) -> Result<(), String> {
    let mut ended = Vec::new();
    for ((from, to), conns) in stalled {
        let live = coord.relay().connections(from, to);
        ended.extend(
            conns
                .iter()
                .filter(|conn| !live.contains(conn))
                .map(|conn| format!("{conn:?} on {from}->{to}")),
        );
    }
    if ended.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{scenario}: stalled connections ended {when}, so the relay let an end through: \
             {ended:?}"
        ))
    }
}

/// Heal every connection of `stalled`.
fn heal(coord: &TestCoordinator, scenario: &str, stalled: &Stalled<'_>) -> Result<(), String> {
    all_ok(
        stalled
            .iter()
            .map(|&(route, ref conns)| {
                apply(coord.relay(), scenario, route, conns, LinkAction::Heal)
            })
            .collect(),
    )
}

/// Scenario 8: a node killed behind a partition, so only the membership
/// layer can tell its peers. One dataflow of each workload kind runs on every
/// node, fed with its inputs open. Every connection to and from `victim` is
/// stalled, which hides its death from the survivors, and it is killed:
///
/// - for [`QUIET`] no survivor ends a dataflow, and every stalled connection
///   is still live: the death did not get through;
/// - `victim` is reported left to every survivor, while the connections
///   among the survivors are stalled too, so each learns of the departure
///   only from its own report: otherwise the first survivor to cancel a
///   dataflow tells the others (instancy's distributed cancel), and their
///   reports find nothing left to cancel. Each survivor's report must cancel
///   every dataflow;
/// - the survivors' connections are healed, and every survivor must end
///   every dataflow `Err` within the deadline, `victim` `Dead`, while the
///   connections to `victim` are still stalled: the leave did it;
/// - the connections to `victim` are healed, and no process crashed.
///
/// Returns each survivor's count of cancelled dataflows.
pub async fn partition_kill(
    coord: &mut TestCoordinator,
    victim: &str,
) -> Result<BTreeMap<String, usize>, String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let survivors: Vec<&str> = nodes.iter().copied().filter(|&n| n != victim).collect();
    if !nodes.contains(&victim) || survivors.len() < 2 {
        return Err(format!(
            "partition_kill needs {victim} and two other nodes, not {nodes:?}"
        ));
    }
    let runs = spawn_every_kind(coord, "partition", &nodes).await;
    for run in &runs {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }

    let victim_routes: Vec<(&str, &str)> = survivors.iter().map(|&s| route(victim, s)).collect();
    let partition = stall_routes(coord, "partition_kill", &victim_routes)?;
    coord.kill_node(victim).await;
    sleep(QUIET).await;
    let when = format!(
        "{} s after {victim} was killed behind the partition",
        QUIET.as_secs()
    );
    for run in &runs {
        still_running(coord, &run.id, &survivors, &when).await?;
    }
    stalls_held(coord, "partition_kill", &partition, "before the leave")?;

    let survivor_routes: Vec<(&str, &str)> = survivors
        .iter()
        .enumerate()
        .flat_map(|(i, &x)| survivors[i + 1..].iter().map(move |&y| route(x, y)))
        .collect();
    let held = stall_routes(coord, "partition_kill", &survivor_routes)?;
    let cancelled = coord.report_leave(victim).await;
    heal(coord, "partition_kill", &held)?;
    let every_dataflow: BTreeMap<String, usize> = survivors
        .iter()
        .map(|&s| (s.to_string(), runs.len()))
        .collect();
    if cancelled != every_dataflow {
        return Err(format!(
            "partition_kill: reporting {victim} left cancelled {cancelled:?} dataflows on the \
             survivors; each runs {} with {victim}, all still running, so each must cancel \
             them all",
            runs.len()
        ));
    }

    // A survivor that cancelled through its report fails with this reason
    // (`CancellationReason::PeerDown` in instancy/src/cancellation.rs).
    let reason = format!("peer node down: {victim}");
    let deadline = Instant::now() + OUTCOME_DEADLINE;
    let mut problems = Vec::new();
    for run in &runs {
        let left = deadline.saturating_duration_since(Instant::now());
        match coord.wait_for_outcomes(&run.id, &nodes, left).await {
            Ok(outcomes) => {
                for (node, outcome) in &outcomes {
                    let expected = match outcome {
                        Outcome::Dead => node == victim,
                        Outcome::Err(error) => node != victim && error.contains(&reason),
                        Outcome::Ok => false,
                    };
                    if !expected {
                        problems.push(format!(
                            "{}: {node} ended {outcome:?} after {victim} was reported left \
                             behind the partition (survivors must fail with \"{reason}\", \
                             {victim} must be Dead)",
                            run.id
                        ));
                    }
                }
            }
            Err(hang) => problems.push(format!(
                "{}: no outcome within {} s from {:?} after {victim} was reported left",
                run.id,
                OUTCOME_DEADLINE.as_secs(),
                hang.missing
            )),
        }
    }
    problems.extend(stalls_held(coord, "partition_kill", &partition, "before the heal").err());
    problems.extend(heal(coord, "partition_kill", &partition).err());
    problems.extend(no_crashes(coord).err());
    if problems.is_empty() {
        Ok(cancelled)
    } else {
        Err(problems.join("\n"))
    }
}

/// Scenario 6: a slow link. One dataflow of each workload kind runs on every
/// node. Once half their inputs are fed, `action` (a delay, a throttle or a
/// stall) goes to every connection from the second node to the first (b to
/// a: the higher ID dials), and the rest of the inputs are fed and closed
/// while it is in force. A stall is healed [`STALL`] after it began. Slowing
/// a connection touches nothing, so every dataflow gives exact output on
/// every worker.
pub async fn slow_link(coord: &mut TestCoordinator, action: LinkAction) -> Result<(), String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let [acceptor, dialer, ..] = nodes[..] else {
        return Err(format!("slow_link needs at least two nodes, not {nodes:?}"));
    };
    let runs = spawn_every_kind(coord, "slow", &nodes).await;
    let half = EPOCHS / 2;
    for run in &runs {
        coord.feed(&run.id, &run.workload, 0..half).await;
    }

    let slowed = coord.relay().connections(dialer, acceptor);
    if slowed.is_empty() {
        return Err(format!(
            "no connection from {dialer} to {acceptor} to apply {action:?} to"
        ));
    }
    apply(
        coord.relay(),
        "slow_link",
        (dialer, acceptor),
        &slowed,
        action,
    )?;
    let applied = Instant::now();

    for run in &runs {
        coord.feed(&run.id, &run.workload, half..EPOCHS).await;
    }
    for run in &runs {
        coord.close_inputs(&run.id, &nodes).await;
    }

    let mut results = Vec::new();
    if action == LinkAction::Stall {
        sleep_until(applied + STALL).await;
        let live = coord.relay().connections(dialer, acceptor);
        let ended: Vec<_> = slowed.iter().filter(|conn| !live.contains(conn)).collect();
        if !ended.is_empty() {
            results.push(Err(format!(
                "stalled connections {ended:?} from {dialer} to {acceptor} ended before the heal"
            )));
        }
        results.push(apply(
            coord.relay(),
            "slow_link",
            (dialer, acceptor),
            &slowed,
            LinkAction::Heal,
        ));
    }
    results.extend(check_all(coord, &runs, false).await);
    results.push(no_crashes(coord));
    all_ok(results)
}

/// When [`long_stall`] stalls its connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallFrom {
    /// Before any input is fed: every input is fed and closed while the
    /// connection is stalled.
    BeforeInput,
    /// Right after every input has closed, as the dataflows finish: one that
    /// has ended on one side may have its last frames to the other (its
    /// goodbyes and acknowledgements) held by the stall.
    AfterClose,
}

/// Scenario 9: a stall longer than the handshake timeout, then a heal.
/// Needs three nodes, a, b and c, started with two pooled connections per
/// pair. On {a, b} run one dataflow of each workload kind, and four
/// ExchangeRoundTrip dataflows with input at one node only, kept on its own
/// workers (two at a, two at b), so only progress crosses between a and b.
/// They all share node set {a, b}'s two connections, and each node pins each
/// dataflow's lane to the connection with the fewest, so both carry lanes.
/// A bystander dataflow runs on {b, c}. The first of the two connections
/// from b to a is stalled from `from` on, for [`LONG_STALL`]:
///
/// - the bystander must end `Ok` with exact output before the heal: it uses
///   none of {a, b}'s connections;
/// - at the heal the stalled connection must still be live, and, if it was
///   stalled before any input, some {a, b} dataflow must still be without an
///   outcome on a or b: the stalled connection carries lanes, and a dataflow
///   can't end on the side a stalled lane feeds;
/// - after the heal every {a, b} dataflow must end `Ok` on a and b with
///   exact output within the deadline: a stall only delays, however long it
///   held a dataflow's last frames;
/// - no process crashed.
///
/// Returns the nodes each {a, b} dataflow had ended on before the heal.
pub async fn long_stall(
    coord: &mut TestCoordinator,
    from: StallFrom,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let [a, b, c] = nodes[..] else {
        return Err(format!("long_stall needs three nodes, not {nodes:?}"));
    };
    let mut pair = spawn_every_kind(coord, &format!("long-stall-{a}-{b}"), &[a, b]).await;
    for (seed, only) in (11..).zip([a, b, a, b]) {
        let id = format!("long-stall-{a}-{b}-input-at-{only}-{seed}");
        let mut run = spawn_run(coord, &id, &[a, b], WorkloadKind::ExchangeRoundTrip, seed).await;
        input_only_at(&mut run, only);
        pair.push(run);
    }
    let bystander = spawn_run(
        coord,
        &format!("long-stall-{b}-{c}"),
        &[b, c],
        WorkloadKind::Join,
        7,
    )
    .await;

    let on_route = coord.relay().connections(b, a);
    let [stalled, _] = on_route[..] else {
        return Err(format!(
            "long_stall: {} connections from {b} to {a}, expected the two of node set \
             {{{a}, {b}}}: {on_route:?}",
            on_route.len()
        ));
    };
    let stall = |coord: &TestCoordinator| {
        apply(
            coord.relay(),
            "long_stall",
            (b, a),
            &[stalled],
            LinkAction::Stall,
        )
    };
    if from == StallFrom::BeforeInput {
        stall(coord)?;
    }
    let mut heal_at = Instant::now() + LONG_STALL;
    for run in pair.iter().chain([&bystander]) {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }
    for run in pair.iter().chain([&bystander]) {
        coord.close_inputs(&run.id, &run.nodes()).await;
    }
    if from == StallFrom::AfterClose {
        stall(coord)?;
        heal_at = Instant::now() + LONG_STALL;
    }

    let mut results = Vec::new();
    let left = heal_at.saturating_duration_since(Instant::now());
    results.push(
        match coord.wait_for_outcomes(&bystander.id, &[b, c], left).await {
            Ok(_) => check(coord, &bystander, false).await,
            Err(hang) => Err(format!(
                "{}: no outcome from {:?} while {stalled:?} from {b} to {a} was stalled, \
                 though the dataflow uses none of {{{a}, {b}}}'s connections",
                bystander.id, hang.missing
            )),
        },
    );

    sleep_until(heal_at).await;
    let mut ended = BTreeMap::new();
    for run in &pair {
        let mut on = Vec::new();
        for node in [a, b] {
            let outcome = coord
                .wait_for_outcomes(&run.id, &[node], Duration::ZERO)
                .await;
            if outcome.is_ok() {
                on.push(node.to_string());
            }
        }
        ended.insert(run.id.clone(), on);
    }
    if from == StallFrom::BeforeInput && ended.values().all(|on| on.len() == 2) {
        results.push(Err(format!(
            "long_stall: every dataflow on {{{a}, {b}}} ended on both nodes while {stalled:?} \
             from {b} to {a} was stalled, so it carried none of their lanes and held nothing"
        )));
    }
    if !coord.relay().connections(b, a).contains(&stalled) {
        results.push(Err(format!(
            "long_stall: the stalled connection {stalled:?} from {b} to {a} ended before the heal"
        )));
    }
    results.push(apply(
        coord.relay(),
        "long_stall",
        (b, a),
        &[stalled],
        LinkAction::Heal,
    ));

    results.extend(check_all(coord, &pair, false).await);
    results.push(no_crashes(coord));
    all_ok(results).map(|()| ended)
}

/// The capacity of a pooled connection's writer queue in instancy
/// (`WRITER_QUEUE`, instancy/src/communication/shared_transport.rs). A
/// connection whose `queued_frames` stays there, with no frame written, has
/// its writer stuck behind a full socket. Once one more frame for it reaches
/// its manager's bridge task, the bridge waits for room in that queue, and
/// every frame queued on the manager behind that one, for any dataflow and
/// any connection, waits with it.
pub const WRITER_QUEUE: usize = 256;

/// How long [`finished_behind_a_one_way_stall`] holds its stall after Y has
/// ended on a: longer than the handshake timeout, which bounds a's flush of
/// Y's last frames as Y tears down there.
pub const HOLD_AFTER_Y: Duration = Duration::from_secs(SPAWN_HANDSHAKE_TIMEOUT.as_secs() + 2);

/// Records in each feed of the dataflow [`finished_behind_a_one_way_stall`]
/// streams through its stall.
const X_RECORDS_PER_FEED: usize = 4096;

/// How long X may stream through the one-way stall before a's bridge must be
/// blocked.
const BLOCK_DEADLINE: Duration = Duration::from_secs(30);

/// Rounds of X fed after its connection's writer queue on a was first seen
/// full: their frames reach the bridge after the queue filled, so the bridge
/// holds one of them for that connection.
const EXTRA_ROUNDS: u64 = 2;

/// How long a full writer queue must stay full, with no frame written, to
/// count as blocked.
const BLOCK_SETTLE: Duration = Duration::from_secs(1);

/// How long Y's last frames from a get to cross before X streams.
const SETTLE: Duration = Duration::from_millis(500);

/// What a scenario did, and when.
struct Steps {
    started: Instant,
    steps: Vec<String>,
}

impl Steps {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            steps: Vec::new(),
        }
    }

    fn step(&mut self, what: String) {
        let at = self.started.elapsed().as_secs_f64();
        self.steps.push(format!("[{at:6.2} s] {what}"));
    }

    /// `problem`, followed by every step so far.
    fn failed(&self, problem: String) -> String {
        format!("{problem}\nsteps:\n  {}", self.steps.join("\n  "))
    }
}

/// `node`'s live pooled connections to `peer`, in the managers of `node_set`.
async fn pool_of(
    coord: &mut TestCoordinator,
    node_set: &str,
    node: &str,
    peer: &str,
) -> Result<Vec<PooledConnection>, String> {
    coord
        .pool_snapshots(node)
        .await?
        .into_iter()
        .find(|pool| pool.node_set == node_set && pool.peer == peer)
        .map(|pool| pool.connections)
        .ok_or_else(|| format!("{node} holds no manager for {peer} in node set {node_set}"))
}

/// The lanes pinned to each connection of `pool`, by connection id.
fn lanes(pool: &[PooledConnection]) -> BTreeMap<usize, usize> {
    pool.iter().map(|c| (c.id, c.pinned_lanes)).collect()
}

/// The connections X and Y are pinned to on a node, from its pool after X
/// was spawned and after Y was: X's had the only lane after X, and Y's is
/// the other one that gained a lane after Y. `None` unless that is so.
fn pinned_apart(
    after_x: &[PooledConnection],
    after_y: &[PooledConnection],
) -> Option<(usize, usize)> {
    let (after_x, after_y) = (lanes(after_x), lanes(after_y));
    if !after_x.keys().eq(after_y.keys()) || after_x.values().sum::<usize>() != 1 {
        return None;
    }
    let x_conn = *after_x.iter().find(|(_, &n)| n == 1)?.0;
    let gained: Vec<usize> = after_y
        .iter()
        .filter(|(id, &n)| n != after_x[*id])
        .map(|(&id, _)| id)
        .collect();
    match gained[..] {
        [y_conn] if y_conn != x_conn && after_y[&y_conn] == 1 => Some((x_conn, y_conn)),
        _ => None,
    }
}

/// Add round `round` of X's input to `run`, a MultiEpochExchange run: on
/// each of `from`'s workers, at timestamp `round`, [`X_RECORDS_PER_FEED`]
/// records keyed onto `onto`'s workers, so every one crosses from `from` to
/// `onto`. Returns the bytes of the records added (their bincode encoding,
/// without the batches' length prefixes): instancy's frames carry at least
/// that many.
fn add_x_round(run: &mut Run, from: &str, onto: &str, round: u64) -> usize {
    let total = run.spawn.total_workers as u64;
    let first = run.spawn.first_worker[onto] as u64;
    let local = run.spawn.local_workers[onto] as u64;
    let mut rng = SplitMix64::new(round ^ 0x58_0058);
    let feeds = run.spawn.local_workers[from];
    for worker_idx in 0..feeds {
        let records: Vec<(u64, i64)> = (0..X_RECORDS_PER_FEED)
            .map(|_| {
                let key = first + rng.below(local) + total * rng.below(3);
                (key, rng.below(1_000) as i64 - 500)
            })
            .collect();
        run.workload.feeds.push(Feed {
            node: from.to_string(),
            worker_idx,
            port: "data".to_string(),
            timestamp: round,
            data: bincode::serialize(&records).expect("failed to encode a feed"),
        });
    }
    feeds * X_RECORDS_PER_FEED * std::mem::size_of::<(u64, i64)>()
}

/// What [`finished_behind_a_one_way_stall`]'s stall must still hold: a's
/// connection `x_conn` to `b` (in `node_set`) with its writer queue full,
/// and the relay connection `c1` with no byte from a forwarded since the
/// stall (`from_a` before it).
struct HeldBridge<'a> {
    node_set: &'a str,
    a: &'a str,
    b: &'a str,
    x_conn: usize,
    c1: RelayConnId,
    from_a: u64,
}

impl HeldBridge<'_> {
    /// Fails, saying `when`, unless the stall still holds a's bridge.
    /// Returns a's connection `x_conn`.
    async fn check(
        &self,
        coord: &mut TestCoordinator,
        when: &str,
    ) -> Result<PooledConnection, String> {
        let (a, b) = (self.a, self.b);
        let conn = pool_of(coord, self.node_set, a, b)
            .await?
            .into_iter()
            .find(|conn| conn.id == self.x_conn);
        let from_a = coord.relay().forwarded(self.c1).map(|f| f.from_acceptor);
        match conn {
            Some(conn) if conn.queued_frames == WRITER_QUEUE && from_a == Some(self.from_a) => {
                Ok(conn)
            }
            _ => Err(format!(
                "precondition not reached: {when}, the stall no longer held {a}'s bridge: \
                 {a}'s connection {} to {b}: {conn:?} (blocked: {WRITER_QUEUE} frames \
                 queued); bytes forwarded from {a} on {:?}: {from_a:?} ({} at the stall)",
                self.x_conn, self.c1, self.from_a
            )),
        }
    }
}

/// How `node` has ended `dataflow_id` so far.
async fn outcome_so_far(coord: &mut TestCoordinator, dataflow_id: &str, node: &str) -> String {
    match coord
        .wait_for_outcomes(dataflow_id, &[node], Duration::ZERO)
        .await
    {
        Ok(outcomes) => format!("{:?}", outcomes[node]),
        Err(_) => "no outcome yet".to_string(),
    }
}

/// The lanes pinned to each connection of each node's manager for the other
/// node of `pair`, by connection id: a registered dataflow holds one there.
async fn pinned_lanes(coord: &mut TestCoordinator, pair: [&str; 2]) -> String {
    let node_set = node_set_key(pair);
    let mut by_node = Vec::new();
    for (node, peer) in [(pair[0], pair[1]), (pair[1], pair[0])] {
        let pinned = match pool_of(coord, &node_set, node, peer).await {
            Ok(pool) => format!("{:?}", lanes(&pool)),
            Err(problem) => problem,
        };
        by_node.push(format!("{node} {pinned}"));
    }
    by_node.join(", ")
}

/// Scenario 10 (R58 of the connection-liveness design): a dataflow that has
/// ended on one node while a one-way stall holds its last frames to the
/// other, with kept managers, for longer than the handshake timeout. Needs
/// two nodes, a and b, with two pooled connections per pair. b dials, so
/// both connections are on the relay route from b to a.
///
/// 1. Pin: dataflow X (MultiEpochExchange), then Y (ExchangeRoundTrip), on
///    {a, b}. Each node pins a lane to the connection with the fewest, so X
///    and Y must be on different connections on both nodes (checked with
///    `SnapshotPools`).
/// 2. Y's input is fed at a only, keyed onto b's workers, and closed on a,
///    so a's records and final progress for Y reach b before the stall.
/// 3. c1, the relay connection carrying X from a (the one that forwards X's
///    warm-up round from a), is stalled one way: the relay stops reading a's
///    bytes on it. b to a on c1, and the other connection c2 both ways, keep
///    flowing.
/// 4. X streams from a, round after round, until a's connection for X has
///    its writer queue full ([`WRITER_QUEUE`] frames), then
///    [`EXTRA_ROUNDS`] more, and it must write nothing for
///    [`BLOCK_SETTLE`]: a's writer is stuck behind a full socket, and a's
///    bridge task behind it, holding every frame a queues for b after it.
/// 5. Y's input closes on b. Y must end `Ok` on a while the stall holds: its
///    goodbye and acknowledgement to b, which go through the blocked bridge,
///    wait behind X's frames.
/// 6. The stall is held [`HOLD_AFTER_Y`] after Y ended on a, longer than
///    the handshake timeout, and still holding a's bridge, then healed.
///
/// Each precondition (pins apart, the bridge blocked, Y `Ok` on a while the
/// stall holds a's bridge, the stall held past the handshake timeout after
/// that) fails the scenario, naming it, if it is not reached. Then, within
/// [`OUTCOME_DEADLINE`] of the heal (three handshake timeouts), Y and X must
/// end `Ok` with exact output on a and b; and no process may crash.
///
/// Returns the steps it took, with when.
pub async fn finished_behind_a_one_way_stall(
    coord: &mut TestCoordinator,
) -> Result<Vec<String>, String> {
    let mut steps = Steps::new();
    match one_way_stall_schedule(coord, &mut steps).await {
        Ok(()) => Ok(steps.steps),
        Err(problem) => Err(steps.failed(problem)),
    }
}

async fn one_way_stall_schedule(
    coord: &mut TestCoordinator,
    steps: &mut Steps,
) -> Result<(), String> {
    const SCENARIO: &str = "finished_behind_a_one_way_stall";
    const MIB: f64 = 1024.0 * 1024.0;
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let [a, b] = nodes[..] else {
        return Err(format!("{SCENARIO} needs two nodes, not {nodes:?}"));
    };
    let pair = [a, b];
    let node_set = node_set_key(pair);
    let peer = |node: &str| if node == a { b } else { a };

    // 1. Pin X, then Y.
    let mut x = spawn_run(coord, "r58-x", &pair, WorkloadKind::MultiEpochExchange, 1).await;
    x.workload.feeds.clear();
    let mut after_x = BTreeMap::new();
    for node in pair {
        after_x.insert(node, pool_of(coord, &node_set, node, peer(node)).await?);
    }
    let mut y = spawn_run(coord, "r58-y", &pair, WorkloadKind::ExchangeRoundTrip, 2).await;
    route_input(&mut y, a, b);
    let mut pinned = BTreeMap::new();
    for node in pair {
        let after_y = pool_of(coord, &node_set, node, peer(node)).await?;
        let Some(conns) = pinned_apart(&after_x[node], &after_y) else {
            return Err(format!(
                "precondition not reached: X and Y are not pinned to different connections \
                 on {node}: lanes by connection after X {:?}, after Y {:?}",
                lanes(&after_x[node]),
                lanes(&after_y)
            ));
        };
        pinned.insert(node, conns);
    }
    let (x_conn, y_conn) = pinned[a];
    steps.step(format!(
        "spawned X ({}), then Y ({}); (X's connection, Y's connection) on each node: \
         {pinned:?}",
        x.id, y.id
    ));

    // 2. Y's output from a goes out early.
    coord.feed(&y.id, &y.workload, 0..EPOCHS).await;
    coord.close_inputs(&y.id, &[a]).await;
    let give_up = Instant::now() + OUTCOME_DEADLINE;
    loop {
        let queued = pool_of(coord, &node_set, a, b)
            .await?
            .iter()
            .find(|conn| conn.id == y_conn)
            .map(|conn| conn.queued_frames);
        if queued == Some(0) {
            break;
        }
        if Instant::now() >= give_up {
            return Err(format!(
                "precondition not reached: Y's connection {y_conn} on {a} had {queued:?} \
                 frames queued {} s after Y's input closed there",
                OUTCOME_DEADLINE.as_secs()
            ));
        }
        sleep(Duration::from_millis(10)).await;
    }
    sleep(SETTLE).await;
    steps.step(format!(
        "fed Y at {a} only, keyed onto {b}'s workers, and closed its input on {a}; Y's \
         connection {y_conn} on {a} has nothing queued"
    ));

    // 3. Find c1, the relay connection that carries X from a.
    let on_route = coord.relay().connections(b, a);
    let [first, second] = on_route[..] else {
        return Err(format!(
            "{SCENARIO}: {} connections from {b} to {a}, expected the two of node set \
             {node_set}: {on_route:?}",
            on_route.len()
        ));
    };
    let from_a = |coord: &TestCoordinator, conn: RelayConnId| {
        coord
            .relay()
            .forwarded(conn)
            .map(|forwarded: Forwarded| forwarded.from_acceptor)
            .ok_or_else(|| format!("{SCENARIO}: relay connection {conn:?} from {b} to {a} ended"))
    };
    let before = [from_a(coord, first)?, from_a(coord, second)?];
    let mut round = 0;
    let warm_up = add_x_round(&mut x, a, b, round) as u64;
    coord.feed(&x.id, &x.workload, round..round + 1).await;
    round += 1;
    let give_up = Instant::now() + OUTCOME_DEADLINE;
    let grown = loop {
        let grown = [
            from_a(coord, first)? - before[0],
            from_a(coord, second)? - before[1],
        ];
        if grown.iter().any(|&bytes| bytes >= warm_up) {
            break grown;
        }
        if Instant::now() >= give_up {
            return Err(format!(
                "precondition not reached: X's warm-up ({warm_up} bytes of records) did not \
                 cross from {a} within {} s: bytes forwarded from {a} since, on {on_route:?}: \
                 {grown:?}",
                OUTCOME_DEADLINE.as_secs()
            ));
        }
        sleep(Duration::from_millis(10)).await;
    };
    let (c1, c2) = match grown {
        [x_bytes, other] if x_bytes >= warm_up && other < warm_up => (first, second),
        [other, x_bytes] if x_bytes >= warm_up && other < warm_up => (second, first),
        _ => {
            return Err(format!(
                "precondition not reached: can't tell which connection carries X: during X's \
                 warm-up ({warm_up} bytes of records) both forwarded that much from {a}: \
                 {grown:?} on {on_route:?}"
            ))
        }
    };
    steps.step(format!(
        "c1 = {c1:?}, c2 = {c2:?}: while X's warm-up ({warm_up} bytes of records) crossed \
         from {a}, {on_route:?} forwarded {grown:?} bytes from {a}"
    ));

    // 4. Stall c1 from a, and stream X until a's bridge is blocked.
    apply(
        coord.relay(),
        SCENARIO,
        (b, a),
        &[c1],
        LinkAction::StallOneWay { from_dialer: false },
    )?;
    let stalled = Instant::now();
    let held = HeldBridge {
        node_set: &node_set,
        a,
        b,
        x_conn,
        c1,
        from_a: from_a(coord, c1)?,
    };
    steps.step(format!(
        "stalled c1 one way: the relay reads nothing more from {a} on it; {b} to {a} on c1, \
         and c2 both ways, keep flowing"
    ));
    let mut streamed = 0;
    let mut full_from = None;
    let give_up = Instant::now() + BLOCK_DEADLINE;
    loop {
        streamed += add_x_round(&mut x, a, b, round);
        coord.feed(&x.id, &x.workload, round..round + 1).await;
        round += 1;
        let conn = pool_of(coord, &node_set, a, b)
            .await?
            .into_iter()
            .find(|conn| conn.id == x_conn);
        if conn.is_some_and(|conn| conn.queued_frames == WRITER_QUEUE) {
            let (first_full, _) = *full_from.get_or_insert((round, stalled.elapsed()));
            if round >= first_full + EXTRA_ROUNDS {
                break;
            }
        }
        if Instant::now() >= give_up {
            return Err(format!(
                "precondition not reached: {a}'s bridge never blocked: {} s after the one-way \
                 stall of c1, with {round} rounds ({:.1} MiB of X's records) streamed through \
                 it, X's connection {x_conn} on {a} was {conn:?}, not at the writer queue's \
                 {WRITER_QUEUE} frames",
                BLOCK_DEADLINE.as_secs(),
                streamed as f64 / MIB
            ));
        }
    }
    let (first_full, full_after) = full_from.expect("the loop ends once the queue was full");
    let before_settle = held.check(coord, "as X's last round was fed").await?;
    sleep(BLOCK_SETTLE).await;
    let blocked = held.check(coord, "once X's stream stopped").await?;
    if blocked.total_frames != before_settle.total_frames {
        return Err(format!(
            "precondition not reached: {a}'s connection {x_conn} for X still writes: \
             {before_settle:?}, then {blocked:?} {BLOCK_SETTLE:?} later"
        ));
    }
    steps.step(format!(
        "{a}'s bridge blocked: X's connection {x_conn} on {a} had its writer queue full \
         ({WRITER_QUEUE} frames) {:.2} s after the stall, at round {first_full}; {EXTRA_ROUNDS} \
         more rounds were fed, and it wrote nothing for {BLOCK_SETTLE:?} ({blocked:?}). \
         {round} rounds, {:.1} MiB of X's records, streamed in {:.2} s since the stall",
        full_after.as_secs_f64(),
        streamed as f64 / MIB,
        stalled.elapsed().as_secs_f64()
    ));

    // 5. Let Y finish: it must end Ok on a while the stall holds.
    coord.close_inputs(&y.id, &[b]).await;
    let closed = Instant::now();
    steps.step(format!("closed Y's input on {b}"));
    let outcomes = coord
        .wait_for_outcomes(&y.id, &[a], OUTCOME_DEADLINE)
        .await
        .map_err(|hang| {
            format!(
                "precondition not reached: Y did not end on {a} within {} s of its input \
                 closing on {b}, while the stall held: {hang:?}",
                OUTCOME_DEADLINE.as_secs()
            )
        })?;
    let y_ended = Instant::now();
    if outcomes[a] != Outcome::Ok {
        return Err(format!(
            "{}: {a} ended {:?} behind the one-way stall of c1; no fault touched Y",
            y.id, outcomes[a]
        ));
    }
    held.check(coord, "when Y ended on a").await?;
    let on_b = outcome_so_far(coord, &y.id, b).await;
    let pins = pinned_lanes(coord, pair).await;
    steps.step(format!(
        "Y ended Ok on {a}, {:.2} s after its input closed on {b}, the stall still holding \
         {a}'s bridge; Y on {b}: {on_b}; lanes pinned by connection: {pins}",
        (y_ended - closed).as_secs_f64()
    ));

    // 6. Hold the stall past the handshake timeout, then heal.
    sleep_until(y_ended + HOLD_AFTER_Y).await;
    let blocked = held.check(coord, "at the heal").await?;
    let held_for = y_ended.elapsed();
    if held_for <= SPAWN_HANDSHAKE_TIMEOUT {
        return Err(format!(
            "precondition not reached: the stall held only {held_for:?} after Y ended on {a}, \
             not longer than the handshake timeout ({SPAWN_HANDSHAKE_TIMEOUT:?})"
        ));
    }
    let on_b = outcome_so_far(coord, &y.id, b).await;
    apply(coord.relay(), SCENARIO, (b, a), &[c1], LinkAction::Heal)?;
    let healed = Instant::now();
    steps.step(format!(
        "healed c1, {:.2} s after Y ended on {a} and {:.2} s after the stall began, the stall \
         holding {a}'s bridge to the end ({blocked:?}); Y on {b} at the heal: {on_b}",
        held_for.as_secs_f64(),
        stalled.elapsed().as_secs_f64()
    ));

    // After the heal: Y and X must end Ok and exact on both nodes.
    coord.close_inputs(&x.id, &pair).await;
    let deadline = healed + OUTCOME_DEADLINE;
    let mut results = Vec::new();
    for run in [&y, &x] {
        let left = deadline.saturating_duration_since(Instant::now());
        match coord.wait_for_outcomes(&run.id, &pair, left).await {
            Ok(outcomes) => {
                steps.step(format!(
                    "{} ended {outcomes:?}, by {:.2} s after the heal",
                    run.id,
                    healed.elapsed().as_secs_f64()
                ));
                results.push(check(coord, run, false).await);
            }
            Err(hang) => {
                let problem = format!(
                    "{}: no outcome from {:?} within {} s of the heal",
                    run.id,
                    hang.missing,
                    OUTCOME_DEADLINE.as_secs()
                );
                steps.step(problem.clone());
                results.push(Err(problem));
            }
        }
    }
    let pins = pinned_lanes(coord, pair).await;
    steps.step(format!("lanes pinned by connection at the end: {pins}"));
    results.push(no_crashes(coord));
    all_ok(results)
}
/// Scenario 7: one connection fails. Needs a cluster of three nodes started
/// with one pooled connection per pair. Two dataflows run on {a, b}: they
/// share a manager set, and so b's one connection to a. One runs on {a, c}
/// and one on {b, c}. All are fed with their inputs open; `action` (a reset
/// or a close) ends the connection from b to a; then every input closes.
///
/// Both {a, b} dataflows must fail on a and on b within the deadline: their
/// inputs were open at the fault, so neither could have finished. The
/// others use no connection between a and b and give exact output.
pub async fn failed_connection(
    coord: &mut TestCoordinator,
    action: LinkAction,
) -> Result<(), String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let [a, b, c] = nodes[..] else {
        return Err(format!(
            "failed_connection needs three nodes, not {nodes:?}"
        ));
    };
    let between = [
        spawn_run(
            coord,
            &format!("failed-{a}-{b}-1"),
            &[a, b],
            WorkloadKind::ExchangeRoundTrip,
            1,
        )
        .await,
        spawn_run(
            coord,
            &format!("failed-{a}-{b}-2"),
            &[a, b],
            WorkloadKind::MultiEpochExchange,
            2,
        )
        .await,
    ];
    let others = [
        spawn_run(
            coord,
            &format!("failed-{a}-{c}"),
            &[a, c],
            WorkloadKind::WordCount,
            3,
        )
        .await,
        spawn_run(
            coord,
            &format!("failed-{b}-{c}"),
            &[b, c],
            WorkloadKind::Join,
            4,
        )
        .await,
    ];
    for run in between.iter().chain(&others) {
        coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    }

    let connections = coord.relay().connections(b, a);
    let [failed] = connections[..] else {
        return Err(format!(
            "{} connections from {b} to {a}, expected the one both dataflows on {{{a}, {b}}} \
             share: {connections:?}",
            connections.len()
        ));
    };
    apply(
        coord.relay(),
        "failed_connection",
        (b, a),
        &[failed],
        action,
    )?;
    for run in between.iter().chain(&others) {
        coord.close_inputs(&run.id, &run.nodes()).await;
    }

    let deadline = Instant::now() + OUTCOME_DEADLINE;
    let mut results = Vec::new();
    for run in &between {
        let left = deadline.saturating_duration_since(Instant::now());
        let outcomes = match coord.wait_for_outcomes(&run.id, &[a, b], left).await {
            Ok(outcomes) => outcomes,
            Err(hang) => {
                results.push(Err(format!(
                    "{}: no outcome within {} s from {:?} after {action:?} of the connection \
                     from {b} to {a}",
                    run.id,
                    OUTCOME_DEADLINE.as_secs(),
                    hang.missing
                )));
                continue;
            }
        };
        for (node, outcome) in &outcomes {
            if !matches!(outcome, Outcome::Err(_)) {
                results.push(Err(format!(
                    "{}: {node} ended {outcome:?} after {action:?} of its connection from {b} \
                     to {a} with inputs open (both nodes must fail): {outcomes:?}",
                    run.id
                )));
            }
        }
    }
    results.extend(check_all(coord, &others, false).await);
    results.push(no_crashes(coord));
    all_ok(results)
}

/// Graceful exit while its peers still run: a node that finished a dataflow
/// `Ok` and then shuts down must not fail the dataflow on the peers still
/// running it. Needs a cluster of three nodes (a, b, c).
///
/// An ExchangeRoundTrip dataflow runs on all three with input only at b,
/// keyed onto b's own workers, so no record crosses between a and c. Once it
/// is spawned, every connection from c to a (c dials: the higher ID) is
/// stalled. That holds the progress between a and c, so neither can finish:
/// no node finishes until every node's input has closed and every record is
/// consumed (decision R47, docs/design/progress-accounting-decisions.md).
/// b hears from a and c directly, so it can. After every input closes:
///
/// - b must end `Ok` with exact output while a and c have no outcome;
/// - b is sent `Shutdown` (it drops its managers, waits for its runtime to
///   shut down, replies and exits), and its process must exit successfully
///   while a and c still have no outcome;
/// - the stall is healed, and a and c must then end `Ok` with exact output;
/// - no process may have crashed: b's exit was requested.
pub async fn graceful_exit(coord: &mut TestCoordinator) -> Result<(), String> {
    let nodes = coord.live_nodes();
    let nodes: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let [a, b, c] = nodes[..] else {
        return Err(format!("graceful_exit needs three nodes, not {nodes:?}"));
    };
    let mut run = spawn_run(
        coord,
        "graceful-exit",
        &nodes,
        WorkloadKind::ExchangeRoundTrip,
        1,
    )
    .await;
    input_only_at(&mut run, b);

    let stalled = coord.relay().connections(c, a);
    if stalled.is_empty() {
        return Err(format!(
            "{}: no connection from {c} to {a} to stall after the spawn",
            run.id
        ));
    }
    apply(
        coord.relay(),
        "graceful_exit",
        (c, a),
        &stalled,
        LinkAction::Stall,
    )?;

    coord.feed(&run.id, &run.workload, 0..EPOCHS).await;
    coord.close_inputs(&run.id, &nodes).await;

    let outcomes = coord
        .wait_for_outcomes(&run.id, &[b], OUTCOME_DEADLINE)
        .await
        .map_err(|hang| {
            format!(
                "{}: no outcome within {} s from {:?} while the connections from {c} to {a} \
                 were stalled",
                run.id,
                OUTCOME_DEADLINE.as_secs(),
                hang.missing
            )
        })?;
    if outcomes[b] != Outcome::Ok {
        return Err(format!(
            "{}: {b} ended {:?} with no fault on its own connections",
            run.id, outcomes[b]
        ));
    }
    still_running(coord, &run.id, &[a, c], &format!("{b} ended Ok")).await?;
    stall_held(coord, c, a, &stalled)?;
    let problems = check_output(coord, &run.id, b, &run.workload, &run.spawn).await;
    if !problems.is_empty() {
        return Err(format!("{}:\n  {}", run.id, problems.join("\n  ")));
    }

    match coord.send_command(b, NodeCommand::Shutdown).await {
        NodeResponse::Error { message } if message == "shutdown" => {}
        other => {
            return Err(format!(
                "{b} did not shut down cleanly after {} ended Ok: {other:?}",
                run.id
            ))
        }
    }
    match coord.wait_for_exit(b, OUTCOME_DEADLINE).await {
        Some(status) if status.success() => {}
        Some(status) => return Err(format!("{b} exited with {status} after Shutdown")),
        None => {
            return Err(format!(
                "{b} did not exit within {} s of replying to Shutdown",
                OUTCOME_DEADLINE.as_secs()
            ))
        }
    }
    still_running(coord, &run.id, &[a, c], &format!("{b} exited")).await?;
    stall_held(coord, c, a, &stalled)?;

    let results = vec![
        apply(
            coord.relay(),
            "graceful_exit",
            (c, a),
            &stalled,
            LinkAction::Heal,
        ),
        check_run(coord, &run.id, &[a, c], &run.workload, &run.spawn, false).await,
        no_crashes(coord),
    ];
    all_ok(results)
}

/// Keep only `node`'s input to an ExchangeRoundTrip run, with each record's
/// key moved onto one of `node`'s own workers, so no record leaves `node`.
/// The run's reference follows, since it is made from the feeds.
fn input_only_at(run: &mut Run, node: &str) {
    route_input(run, node, node);
}

/// Keep only `from`'s input to an ExchangeRoundTrip run, with each record's
/// key moved onto one of `onto`'s workers. The run's reference follows,
/// since it is made from the feeds.
fn route_input(run: &mut Run, from: &str, onto: &str) {
    assert_eq!(
        run.workload.kind,
        WorkloadKind::ExchangeRoundTrip,
        "route_input rewrites ExchangeRoundTrip records"
    );
    let first = run.spawn.first_worker[onto] as u64;
    let local = run.spawn.local_workers[onto] as u64;
    let total = run.spawn.total_workers as u64;
    run.workload.feeds.retain(|feed| feed.node == from);
    assert!(
        !run.workload.feeds.is_empty(),
        "{from} has no input to keep"
    );
    for feed in &mut run.workload.feeds {
        let records: Vec<(u64, String)> =
            bincode::deserialize(&feed.data).expect("an ExchangeRoundTrip feed doesn't decode");
        let kept: Vec<(u64, String)> = records
            .into_iter()
            .map(|(key, value)| (key - key % total + first + key % local, value))
            .collect();
        feed.data = bincode::serialize(&kept).expect("failed to encode a feed");
    }
}

/// Fails unless none of `nodes` has reported an outcome of `dataflow_id`.
async fn still_running(
    coord: &mut TestCoordinator,
    dataflow_id: &str,
    nodes: &[&str],
    when: &str,
) -> Result<(), String> {
    let mut expected = nodes.to_vec();
    expected.sort_unstable();
    expected.dedup();
    match coord
        .wait_for_outcomes(dataflow_id, nodes, Duration::ZERO)
        .await
    {
        Err(hang) if hang.missing == expected => Ok(()),
        reported => Err(format!(
            "{dataflow_id}: when {when}, {nodes:?} had to be still running, with no outcome; \
             got {reported:?}"
        )),
    }
}

/// Fails unless every connection from `from` to `to` is one of `stalled`
/// and all of them are still live.
fn stall_held(
    coord: &TestCoordinator,
    from: &str,
    to: &str,
    stalled: &[RelayConnId],
) -> Result<(), String> {
    let live = coord.relay().connections(from, to);
    if live == stalled {
        Ok(())
    } else {
        Err(format!(
            "the connections from {from} to {to} changed while stalled: {stalled:?} were \
             stalled, {live:?} are live"
        ))
    }
}
