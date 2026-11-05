//! A seeded randomized fault campaign over a Shared-transport cluster.
//!
//! [`schedule`] turns a seed into a [`Schedule`]: three dataflows, each a
//! random workload on a random three or four of the nodes a–d, and one to
//! three faults, each fired once a random epoch's input has been fed to every
//! dataflow. [`run`] carries a seed's schedule out on a cluster of real node
//! processes, two workers each with two pooled connections per pair, and
//! checks every dataflow with [`check_run`](super::scenarios::check_run):
//!
//! - a dataflow no fault touched gives exact output on every worker;
//! - one a fault touched may fail, but every `Ok` is still exact;
//! - no dataflow hangs and no process crashes.
//!
//! Each node set (the nodes of a dataflow's topology) has its own pooled
//! connections. A link fault names a [`Link`]: a relay route, the node set of
//! one of the schedule's dataflows, and which of that node set's live
//! connections on the route it hits. The relay reads each connection's node
//! set from the peer-link prefix its dialer sends. A kill touches the
//! dataflows on the killed node; a reset or close touches the dataflows of
//! the node set whose connection it hit. A delay, throttle or stall only
//! slows a connection and touches nothing. A fault that finds no live
//! connection to hit did not happen: it is logged as not applied and touches
//! nothing.
//!
//! After a kill the node is reported left. A partition kill first stalls
//! every live connection to and from the node (but one an earlier close or
//! reset ended, which a stall no longer holds), so its death reaches the
//! survivors only through the leave report, then kills it. The stalled
//! connections must still be live when it is reported left (the death did
//! not get through), and the survivors' reports together must cancel at
//! least every dataflow on the node that no earlier fault touched: those are
//! still running, and nothing but a report can end them. Then they are
//! healed. A restart (after either kill) brings the node back as a new
//! process, reports it joined, and runs one more dataflow on all four nodes,
//! to completion, which no fault touches. A node the campaign killed is
//! never fed, nor are its inputs closed, for a dataflow spawned before the
//! kill. A feed or close the node refuses is allowed only on a touched
//! dataflow; one it never answers is a hang, a failure on any dataflow.
//!
//! Everything a run does, shutting the cluster down included, finishes
//! within [`RUN_DEADLINE`]. If it doesn't, or a check fails, or the harness
//! panics, `run` returns the seed's bug report: the problems, the seed, the
//! schedule, what the run did, every dataflow's outcomes and every node's
//! log.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use instancy_integration::coordinator::{InputFailure, Outcome, TestCoordinator, OUTCOME_DEADLINE};
use instancy_integration::peer_link::node_set_key;
use instancy_integration::relay::{LinkAction, RelayConnId};
pub use instancy_integration::workloads::SplitMix64;
use instancy_integration::workloads::{Workload, WorkloadKind};
use tokio::time::{sleep_until, timeout_at, Instant};

use super::scenarios::{check, kinds, no_crashes, route, topology, Run, CONNECTIONS, WORKERS};

/// The cluster's nodes.
pub const NODES: [&str; 4] = ["a", "b", "c", "d"];

/// Epochs (timestamps) of input each dataflow gets.
pub const EPOCHS: u64 = 5;

/// Records in each feed (a third as many sentences for WordCount): enough
/// that data is still in flight on the connections when a fault hits.
pub const RECORDS_PER_FEED: usize = 300;

/// Dataflows a schedule runs, besides the one after a restart.
const DATAFLOWS: usize = 3;

/// The ID of the dataflow run after a restart.
const AFTER_RESTART: &str = "df-after-restart";

/// Everything one seed's run does, from starting its cluster to shutting it
/// down, must finish within this, or the run fails with its report. It
/// leaves room inside nextest's 120 s for a run in which every wait times
/// out: one after a restart and the final one, [`OUTCOME_DEADLINE`] each,
/// and the shutdown.
pub const RUN_DEADLINE: Duration = Duration::from_secs(90);

/// The connection a link fault hits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    /// The relay route's dialer: the higher node ID.
    pub from: String,
    /// The route's acceptor.
    pub to: String,
    /// The node set whose pooled connection it hits, as its key: the node
    /// IDs in order, joined by `,`.
    pub node_set: String,
    /// Which of the node set's live connections on the route, in accept
    /// order: the one at `conn` modulo how many are live.
    pub conn: u64,
}

impl fmt::Display for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}->{}, connection #{} of node set [{}]",
            self.from, self.to, self.conn, self.node_set
        )
    }
}

/// A fault the campaign injects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Kill the node's process and report it left. If `restart`, start it
    /// again, report it joined, and run one more dataflow on every node.
    Kill { node: String, restart: bool },
    /// Stall every live connection to and from the node, kill its process,
    /// report it left, and heal the connections: its death reaches the
    /// survivors only through the leave report. `restart` as for a kill.
    PartitionKill { node: String, restart: bool },
    /// Reset the connection (RST both ways).
    Reset { link: Link },
    /// Close the connection (FIN both ways).
    Close { link: Link },
    /// Hold each chunk for `ms` milliseconds, for the rest of the run.
    Delay { link: Link, ms: u64 },
    /// Cap each direction at `bytes_per_sec`, for the rest of the run.
    Throttle { link: Link, bytes_per_sec: u64 },
    /// Forward nothing for `ms` milliseconds, then heal (which also clears
    /// any delay or throttle on the connection).
    StallThenHeal { link: Link, ms: u64 },
}

impl Fault {
    /// Whether the fault, once it has happened, touches a dataflow on
    /// `nodes`: a kill touches those on its node, a reset or close those of
    /// the node set whose connection it hit.
    pub fn touches(&self, nodes: &[String]) -> bool {
        match self {
            Fault::Kill { node, .. } | Fault::PartitionKill { node, .. } => nodes.contains(node),
            Fault::Reset { link } | Fault::Close { link } => {
                node_set_key(nodes.iter().map(String::as_str)) == link.node_set
            }
            Fault::Delay { .. } | Fault::Throttle { .. } | Fault::StallThenHeal { .. } => false,
        }
    }

    /// A link fault's connection, and what it does to it.
    pub fn link(&self) -> Option<(&Link, LinkAction)> {
        match self {
            Fault::Kill { .. } | Fault::PartitionKill { .. } => None,
            Fault::Reset { link } => Some((link, LinkAction::Reset)),
            Fault::Close { link } => Some((link, LinkAction::Close)),
            Fault::Delay { link, ms } => {
                Some((link, LinkAction::Delay(Duration::from_millis(*ms))))
            }
            Fault::Throttle {
                link,
                bytes_per_sec,
            } => Some((
                link,
                LinkAction::Throttle {
                    bytes_per_sec: *bytes_per_sec,
                },
            )),
            Fault::StallThenHeal { link, .. } => Some((link, LinkAction::Stall)),
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Kill { node, restart } => {
                write!(f, "kill {node}")?;
                restarted(f, *restart)
            }
            Fault::PartitionKill { node, restart } => {
                write!(f, "kill {node} behind a partition")?;
                restarted(f, *restart)
            }
            Fault::Reset { link } => write!(f, "reset {link}"),
            Fault::Close { link } => write!(f, "close {link}"),
            Fault::Delay { link, ms } => write!(f, "delay {ms} ms {link}"),
            Fault::Throttle {
                link,
                bytes_per_sec,
            } => write!(f, "throttle to {bytes_per_sec} B/s {link}"),
            Fault::StallThenHeal { link, ms } => write!(f, "stall for {ms} ms {link}"),
        }
    }
}

fn restarted(f: &mut fmt::Formatter<'_>, restart: bool) -> fmt::Result {
    if restart {
        write!(f, ", then restart it")
    } else {
        Ok(())
    }
}

/// What one seed's run does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// Each dataflow's ID, its nodes (in ID order: its topology) and its
    /// workload.
    pub dataflows: Vec<(String, Vec<String>, WorkloadKind)>,
    /// Each fault, with the epoch after whose input it fires, in epoch order.
    pub faults: Vec<(u64 /* after this epoch */, Fault)>,
}

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "dataflows:")?;
        for (id, nodes, kind) in &self.dataflows {
            writeln!(f, "  {id}: {kind:?} on [{}]", nodes.join(", "))?;
        }
        writeln!(f, "faults:")?;
        for (epoch, fault) in &self.faults {
            writeln!(f, "  after epoch {epoch}: {fault}")?;
        }
        Ok(())
    }
}

/// The schedule of `seed`.
pub fn schedule(seed: u64) -> Schedule {
    let mut rng = SplitMix64::new(seed);
    let mut dataflows = Vec::new();
    for i in 1..=DATAFLOWS {
        let kind = pick(&mut rng, kinds(NODES.len()).len());
        let mut nodes: Vec<String> = NODES.iter().map(|node| node.to_string()).collect();
        if rng.below(2) == 0 {
            nodes.remove(pick(&mut rng, NODES.len()));
        }
        let kind = kinds(nodes.len())[kind];
        dataflows.push((format!("df{i}"), nodes, kind));
    }

    let mut faults = Vec::new();
    let mut killed = false;
    for _ in 0..1 + rng.below(3) {
        let epoch = rng.below(EPOCHS);
        // Seven kinds of fault, numbered from the two kills, which only come
        // up while there has been neither.
        let first = if killed { 2 } else { 0 };
        let fault = match first + rng.below(7 - first) {
            kind @ (0 | 1) => {
                killed = true;
                let node = NODES[pick(&mut rng, NODES.len())].to_string();
                let restart = rng.below(2) == 1;
                if kind == 0 {
                    Fault::Kill { node, restart }
                } else {
                    Fault::PartitionKill { node, restart }
                }
            }
            kind => {
                let (_, nodes, _) = &dataflows[pick(&mut rng, dataflows.len())];
                let link = random_link(&mut rng, nodes);
                match kind {
                    2 => Fault::Reset { link },
                    3 => Fault::Close { link },
                    4 => Fault::Delay {
                        link,
                        ms: 1 + rng.below(100),
                    },
                    5 => Fault::Throttle {
                        link,
                        bytes_per_sec: (32 + rng.below(225)) * 1024,
                    },
                    _ => {
                        // One draw, so a seed's other draws stay as they
                        // were: 0.5–2 s, or for a quarter of the stalls
                        // 11–13 s, longer than the spawn handshake timeout.
                        let draw = rng.below(2_001);
                        let ms = if draw < 1_501 {
                            500 + draw
                        } else {
                            11_000 + (draw - 1_501) * 4
                        };
                        Fault::StallThenHeal { link, ms }
                    }
                }
            }
        };
        faults.push((epoch, fault));
    }
    faults.sort_by_key(|(epoch, _)| *epoch);
    Schedule { dataflows, faults }
}

/// An index in `0..len`.
fn pick(rng: &mut SplitMix64, len: usize) -> usize {
    rng.below(len as u64) as usize
}

/// One of the connections of the node set `nodes` (in ID order) on the
/// route between two of them, the higher ID dialing.
fn random_link(rng: &mut SplitMix64, nodes: &[String]) -> Link {
    let first = pick(rng, nodes.len());
    let mut second = pick(rng, nodes.len() - 1);
    if second >= first {
        second += 1;
    }
    Link {
        from: nodes[first.max(second)].clone(),
        to: nodes[first.min(second)].clone(),
        node_set: node_set_key(nodes.iter().map(String::as_str)),
        conn: rng.below(CONNECTIONS as u64),
    }
}

/// Run the schedule of `seed` on a new cluster. `Err` is the seed's report:
/// its problems, the seed, the schedule, what the run did, every dataflow's
/// outcomes and every node's log.
pub async fn run(seed: u64) -> Result<(), String> {
    run_schedule(seed, schedule(seed)).await.map(drop)
}

/// Run `schedule` on a new cluster, as [`run`] does for a seed's; `seed`
/// names the run and seeds its workloads. Returns whether a fault touched
/// each scheduled dataflow, by ID.
pub async fn run_schedule(seed: u64, schedule: Schedule) -> Result<BTreeMap<String, bool>, String> {
    let deadline = Instant::now() + RUN_DEADLINE;
    let started = timeout_at(
        deadline,
        Caught::new(TestCoordinator::start_shared(&NODES, WORKERS, CONNECTIONS)),
    )
    .await;
    let mut coord = match started {
        Ok(Ok(coord)) => coord,
        Ok(Err(panic)) => {
            return Err(format!(
                "fault campaign seed {seed}: the cluster did not start: {panic}\n\n\
                 schedule:\n{schedule}"
            ))
        }
        Err(_) => {
            return Err(format!(
                "fault campaign seed {seed}: the cluster did not start within {} s\n\n\
                 schedule:\n{schedule}",
                RUN_DEADLINE.as_secs()
            ))
        }
    };

    let mut campaign = Campaign::new(seed, schedule);
    let ended = timeout_at(deadline, Caught::new(campaign.body(&mut coord))).await;
    match ended {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => campaign.problem(format!("the run panicked: {panic}")),
        Err(_) => campaign.problem(format!(
            "the run did not finish within {} s",
            RUN_DEADLINE.as_secs()
        )),
    }
    if campaign.problems.is_empty() {
        Ok(campaign
            .dataflows
            .iter()
            .map(|dataflow| (dataflow.run.id.clone(), dataflow.touched))
            .collect())
    } else {
        Err(campaign.report(&mut coord).await)
    }
}

/// One scheduled dataflow of a run.
struct Dataflow {
    run: Run,
    /// Whether a fault that happened touches it.
    touched: bool,
    /// Nodes whose feed failed: they are fed no more.
    unfed: BTreeSet<String>,
}

/// One seed's run: what it has done so far, kept for its report.
struct Campaign {
    seed: u64,
    schedule: Schedule,
    /// Draws the workload of the dataflow after a restart.
    rng: SplitMix64,
    started: Instant,
    /// The scheduled dataflows spawned so far.
    dataflows: Vec<Dataflow>,
    /// The dataflow run after a restart.
    after_restart: Option<Run>,
    /// The node the campaign killed, if any.
    killed: Option<String>,
    /// The killed node while it has no process.
    down: Option<String>,
    /// Stalled connections, and when each is due to be healed.
    stalls: Vec<(Instant, RelayConnId)>,
    /// Connections a close or reset fault ended. The relay keeps a closed
    /// one until its sides close their ends, reading and discarding, and a
    /// stall no longer holds it.
    ended_by_faults: BTreeSet<RelayConnId>,
    /// What the run did, in order.
    steps: Vec<String>,
    /// What went wrong, in order.
    problems: Vec<String>,
}

impl Campaign {
    fn new(seed: u64, schedule: Schedule) -> Self {
        Self {
            seed,
            schedule,
            rng: SplitMix64::new(seed ^ 0xCA3F_A16E_0000_0001),
            started: Instant::now(),
            dataflows: Vec::new(),
            after_restart: None,
            killed: None,
            down: None,
            stalls: Vec::new(),
            ended_by_faults: BTreeSet::new(),
            steps: Vec::new(),
            problems: Vec::new(),
        }
    }

    fn step(&mut self, what: String) {
        let step = format!("[{:6.2} s] {what}", self.started.elapsed().as_secs_f64());
        println!("campaign seed {}: {step}", self.seed);
        self.steps.push(step);
    }

    fn problem(&mut self, problem: String) {
        self.step(format!("PROBLEM: {problem}"));
        self.problems.push(problem);
    }

    /// Spawn every scheduled dataflow, feed them an epoch at a time, firing
    /// each fault after its epoch, then close every input, check every
    /// dataflow and, if all is well, shut the cluster down, which must go
    /// cleanly. Records what goes wrong in `problems`.
    async fn body(&mut self, coord: &mut TestCoordinator) {
        for (index, (id, nodes, kind)) in self.schedule.dataflows.clone().into_iter().enumerate() {
            let run = spawn(coord, self.seed, index, &id, &nodes, kind).await;
            self.step(format!("spawned {id}: {kind:?} on [{}]", nodes.join(", ")));
            self.dataflows.push(Dataflow {
                run,
                touched: false,
                unfed: BTreeSet::new(),
            });
        }

        let faults = self.schedule.faults.clone();
        for epoch in 0..EPOCHS {
            self.heal_stalls(coord, false).await;
            for index in 0..self.dataflows.len() {
                self.feed(coord, index, epoch).await;
            }
            self.step(format!("fed epoch {epoch}"));
            for (_, fault) in faults.iter().filter(|(after, _)| *after == epoch) {
                self.fire(coord, epoch, fault).await;
            }
        }

        self.heal_stalls(coord, true).await;
        for index in 0..self.dataflows.len() {
            self.close(coord, index).await;
        }
        self.step("closed every input".to_string());
        self.check_dataflows(coord).await;
        if let Err(problem) = no_crashes(coord) {
            self.problem(problem);
        }
        if self.problems.is_empty() {
            match coord.try_shutdown().await {
                Ok(()) => self.step("shut the cluster down cleanly".to_string()),
                Err(problem) => self.problem(problem),
            }
        }
    }

    /// Feed `epoch` to dataflow `index` on its nodes that the campaign
    /// didn't kill and whose feeds haven't failed.
    async fn feed(&mut self, coord: &mut TestCoordinator, index: usize, epoch: u64) {
        let dataflow = &self.dataflows[index];
        let nodes: Vec<&str> = dataflow
            .run
            .nodes
            .iter()
            .filter(|&node| self.killed.as_ref() != Some(node) && !dataflow.unfed.contains(node))
            .map(String::as_str)
            .collect();
        let results = coord
            .try_feed(
                &dataflow.run.id,
                &dataflow.run.workload,
                epoch..epoch + 1,
                &nodes,
            )
            .await;
        for (node, result) in results {
            if let Err(failure) = result {
                self.dataflows[index].unfed.insert(node);
                self.input_failed(index, failure);
            }
        }
    }

    /// Close every input of dataflow `index` on its nodes that the campaign
    /// didn't kill.
    async fn close(&mut self, coord: &mut TestCoordinator, index: usize) {
        let dataflow = &self.dataflows[index];
        let nodes: Vec<&str> = dataflow
            .run
            .nodes
            .iter()
            .filter(|&node| self.killed.as_ref() != Some(node))
            .map(String::as_str)
            .collect();
        let results = coord.try_close_inputs(&dataflow.run.id, &nodes).await;
        for failure in results.into_values().filter_map(Result::err) {
            self.input_failed(index, failure);
        }
    }

    /// A feed or close of dataflow `index` failed. A refusal is allowed only
    /// if a fault touched it; no answer at all is a hang, never allowed.
    fn input_failed(&mut self, index: usize, failure: InputFailure) {
        let dataflow = &self.dataflows[index];
        let id = dataflow.run.id.clone();
        if input_failure_allowed(&failure, dataflow.touched) {
            self.step(format!("{failure} (allowed: a fault touched {id})"));
            return;
        }
        match failure {
            InputFailure::NoResponse(message) => {
                self.problem(format!("{id}: {message}: the command hung"));
            }
            InputFailure::Refused(message) => {
                self.problem(format!("{id}: no fault touched it, but {message}"));
            }
        }
    }

    async fn fire(&mut self, coord: &mut TestCoordinator, epoch: u64, fault: &Fault) {
        let Some((link, action)) = fault.link() else {
            let (Fault::Kill { node, restart } | Fault::PartitionKill { node, restart }) = fault
            else {
                unreachable!("every fault but a kill is a link fault");
            };
            if let Fault::PartitionKill { .. } = fault {
                self.partition_kill(coord, epoch, fault, node).await;
            } else {
                coord.kill_node(node).await;
                let cancelled = coord.report_leave(node).await;
                self.killed = Some(node.clone());
                self.down = Some(node.clone());
                let touched = self.touch(fault);
                self.step(format!(
                    "after epoch {epoch}: killed {node} and reported it left; dataflows each \
                     member cancelled: {cancelled:?}; touches {touched:?}"
                ));
            }
            if *restart {
                self.restart(coord, node).await;
            }
            return;
        };

        if let Some(down) = self
            .down
            .as_ref()
            .filter(|&down| *down == link.from || *down == link.to)
        {
            let down = down.clone();
            self.step(format!(
                "after epoch {epoch}: {fault}: not applied: {down} is down"
            ));
            return;
        }
        let relay = coord.relay();
        let on_route = relay.connections(&link.from, &link.to);
        let of_node_set: Vec<RelayConnId> = on_route
            .iter()
            .copied()
            .filter(|&conn| relay.connection_tag(conn).as_deref() == Some(link.node_set.as_str()))
            .collect();
        if of_node_set.is_empty() {
            self.step(format!(
                "after epoch {epoch}: {fault}: not applied: none of the {} live connections \
                 on the route is node set [{}]'s",
                on_route.len(),
                link.node_set
            ));
            return;
        }
        let index = (link.conn % of_node_set.len() as u64) as usize;
        let conn = of_node_set[index];
        if !relay.apply(conn, action) {
            self.step(format!(
                "after epoch {epoch}: {fault}: not applied: {conn:?} ended before the fault"
            ));
            return;
        }
        if let Fault::StallThenHeal { ms, .. } = fault {
            self.stalls
                .push((Instant::now() + Duration::from_millis(*ms), conn));
        }
        if matches!(action, LinkAction::Close | LinkAction::Reset) {
            self.ended_by_faults.insert(conn);
        }
        let touched = self.touch(fault);
        self.step(format!(
            "after epoch {epoch}: {fault}: {action:?} on {conn:?}, #{index} of the node set's \
             {} live connections on the route ({} on the route in all); touches {touched:?}",
            of_node_set.len(),
            on_route.len()
        ));
    }

    /// Mark the dataflows `fault`, which has happened, touches. Returns their IDs.
    fn touch(&mut self, fault: &Fault) -> Vec<String> {
        let mut touched = Vec::new();
        for dataflow in &mut self.dataflows {
            if fault.touches(&dataflow.run.nodes) {
                dataflow.touched = true;
                touched.push(dataflow.run.id.clone());
            }
        }
        touched
    }

    /// Partition `node` from the cluster (stall every live connection to
    /// and from it), kill it, report it left and heal the partition. A
    /// connection an earlier close or reset ended is left out: its sides have
    /// had their end already, and a stall no longer holds it.
    ///
    /// The stalled connections must still be live when `node` is reported
    /// left: its death did not get through. The dataflows on `node` that no
    /// fault has touched yet are running on every survivor, and nothing but
    /// a leave report can end them now, so the survivors' reports must
    /// cancel each at least once: their counts must add up to at least as
    /// many. (One survivor's count alone can be lower: a survivor that
    /// cancels a dataflow tells the others, and their reports find it
    /// cancelled already.)
    async fn partition_kill(
        &mut self,
        coord: &mut TestCoordinator,
        epoch: u64,
        fault: &Fault,
        node: &str,
    ) {
        let mut partition = Vec::new();
        let mut left_out = Vec::new();
        for &other in NODES.iter().filter(|&&other| other != node) {
            let (from, to) = route(node, other);
            for conn in coord.relay().connections(from, to) {
                if self.ended_by_faults.contains(&conn) {
                    left_out.push(conn);
                } else if coord.relay().apply(conn, LinkAction::Stall) {
                    // (One that ended since it was listed lets nothing through.)
                    partition.push((from, to, conn));
                }
            }
        }
        let running: Vec<String> = self
            .dataflows
            .iter()
            .filter(|dataflow| !dataflow.touched && dataflow.run.nodes.iter().any(|n| n == node))
            .map(|dataflow| dataflow.run.id.clone())
            .collect();

        coord.kill_node(node).await;
        let ended: Vec<String> = partition
            .iter()
            .filter(|&&(from, to, conn)| !coord.relay().connections(from, to).contains(&conn))
            .map(|(from, to, conn)| format!("{conn:?} on {from}->{to}"))
            .collect();
        if !ended.is_empty() {
            self.problem(format!(
                "{fault}: stalled connections {ended:?} ended before {node} was reported left: \
                 its death got through the partition"
            ));
        }
        let cancelled = coord.report_leave(node).await;
        let total: usize = cancelled.values().sum();
        if total < running.len() {
            self.problem(format!(
                "{fault}: reporting {node} left cancelled {cancelled:?} dataflows on the \
                 survivors, {total} in all, but {running:?} ran on {node} untouched by any \
                 fault: each was still running, and nothing but a leave report could end it"
            ));
        }
        self.killed = Some(node.to_string());
        self.down = Some(node.to_string());
        let touched = self.touch(fault);
        self.step(format!(
            "after epoch {epoch}: stalled {} connections to and from {node} (left out, ended by \
             earlier faults: {left_out:?}), killed it and reported it left; dataflows each \
             member cancelled: {cancelled:?} ({total} in all; untouched dataflows on {node}: \
             {running:?}); touches {touched:?}",
            partition.len()
        ));

        let healed = partition
            .iter()
            .filter(|&&(_, _, conn)| coord.relay().apply(conn, LinkAction::Heal))
            .count();
        self.step(format!(
            "healed the partition of {node}: {healed} of its {} stalled connections were \
             still live",
            partition.len()
        ));
    }

    /// Restart the killed `node`, report it joined, and run one dataflow on
    /// every node to completion: no fault touches it, so it must give exact
    /// output on every worker.
    async fn restart(&mut self, coord: &mut TestCoordinator, node: &str) {
        // The new dataflow's spawn handshake needs every connection forwarding.
        self.heal_stalls(coord, true).await;
        coord.restart_node(node).await;
        self.down = None;
        let joined = coord.report_join(node).await;
        self.step(format!(
            "restarted {node} and reported it joined; whether each member had it as left: \
             {joined:?}"
        ));
        if !joined.values().all(|&was_left| was_left) {
            self.problem(format!(
                "{node} was reported left, but not every member had it as left when it \
                 rejoined: {joined:?}"
            ));
        }

        let every_kind = kinds(NODES.len());
        let kind = every_kind[pick(&mut self.rng, every_kind.len())];
        let nodes: Vec<String> = NODES.iter().map(|node| node.to_string()).collect();
        let run = spawn(coord, self.seed, DATAFLOWS, AFTER_RESTART, &nodes, kind).await;
        self.step(format!(
            "spawned {AFTER_RESTART}: {kind:?} on [{}]",
            nodes.join(", ")
        ));
        let run = self.after_restart.insert(run);
        let mut failures: Vec<InputFailure> = coord
            .try_feed(&run.id, &run.workload, 0..EPOCHS, &NODES)
            .await
            .into_values()
            .filter_map(Result::err)
            .collect();
        failures.extend(
            coord
                .try_close_inputs(&run.id, &NODES)
                .await
                .into_values()
                .filter_map(Result::err),
        );
        let checked = check(coord, run, false).await;
        for failure in failures {
            self.problem(format!(
                "{AFTER_RESTART}: no fault touched it, but {failure}"
            ));
        }
        match checked {
            Ok(()) => self.step(format!("{AFTER_RESTART}: exact on every worker")),
            Err(problem) => self.problem(problem),
        }
    }

    /// Heal the stalls that are due, or, if `all`, wait until every stall is
    /// due and heal them all.
    async fn heal_stalls(&mut self, coord: &TestCoordinator, all: bool) {
        if all {
            if let Some(last) = self.stalls.iter().map(|&(due, _)| due).max() {
                sleep_until(last).await;
            }
        }
        let now = Instant::now();
        let (due, pending): (Vec<_>, Vec<_>) =
            self.stalls.drain(..).partition(|&(due, _)| due <= now);
        self.stalls = pending;
        for (_, conn) in due {
            if coord.relay().apply(conn, LinkAction::Heal) {
                self.step(format!("healed {conn:?}"));
            } else {
                self.step(format!("{conn:?} ended before its heal"));
            }
        }
    }

    /// Check every scheduled dataflow. Their outcomes share one
    /// [`OUTCOME_DEADLINE`], from when every input has closed.
    async fn check_dataflows(&mut self, coord: &mut TestCoordinator) {
        let deadline = Instant::now() + OUTCOME_DEADLINE;
        for index in 0..self.dataflows.len() {
            let dataflow = &self.dataflows[index];
            let id = dataflow.run.id.clone();
            let touched = dataflow.touched;
            let left = deadline.saturating_duration_since(Instant::now());
            let checked = match coord
                .wait_for_outcomes(&id, &dataflow.run.nodes(), left)
                .await
            {
                Ok(outcomes) => check(coord, &dataflow.run, touched)
                    .await
                    .map(|()| outcomes),
                Err(hang) => Err(format!(
                    "{id}: no outcome within {} s of its inputs closing from {:?}",
                    OUTCOME_DEADLINE.as_secs(),
                    hang.missing
                )),
            };
            match checked {
                Ok(outcomes) => {
                    let ends: Vec<String> = outcomes
                        .iter()
                        .map(|(node, outcome)| {
                            let end = match outcome {
                                Outcome::Ok => "Ok",
                                Outcome::Err(_) => "Err",
                                Outcome::Dead => "Dead",
                            };
                            format!("{node}: {end}")
                        })
                        .collect();
                    self.step(format!(
                        "{id}: checked (touched: {touched}); {}",
                        ends.join(", ")
                    ));
                }
                Err(problem) => self.problem(problem),
            }
        }
    }

    /// The seed's report: its problems, the seed, the schedule, what the run
    /// did, every dataflow's outcomes and every node's log.
    async fn report(&self, coord: &mut TestCoordinator) -> String {
        let mut report = format!(
            "fault campaign seed {} failed:\n  {}\n\nschedule (seed {}):\n{}\nsteps:\n",
            self.seed,
            self.problems.join("\n  "),
            self.seed,
            self.schedule
        );
        for step in &self.steps {
            let _ = writeln!(report, "  {step}");
        }
        report.push_str("\noutcomes:\n");
        let runs = self
            .dataflows
            .iter()
            .map(|dataflow| (&dataflow.run, dataflow.touched))
            .chain(self.after_restart.iter().map(|run| (run, false)));
        for (run, touched) in runs {
            let mut outcomes = Vec::new();
            for node in &run.nodes {
                let outcome = match coord
                    .wait_for_outcomes(&run.id, &[node.as_str()], Duration::ZERO)
                    .await
                {
                    Ok(outcomes) => format!("{:?}", outcomes[node]),
                    Err(_) => "no outcome".to_string(),
                };
                outcomes.push(format!("{node}: {outcome}"));
            }
            let _ = writeln!(
                report,
                "  {} ({:?} on [{}], touched: {touched}): {}",
                run.id,
                run.workload.kind,
                run.nodes.join(", "),
                outcomes.join(", ")
            );
        }
        for (id, ..) in &self.schedule.dataflows {
            if !self.dataflows.iter().any(|dataflow| &dataflow.run.id == id) {
                let _ = writeln!(report, "  {id}: not spawned");
            }
        }
        let restarts = self.schedule.faults.iter().any(|(_, fault)| {
            matches!(
                fault,
                Fault::Kill { restart: true, .. } | Fault::PartitionKill { restart: true, .. }
            )
        });
        if restarts && self.after_restart.is_none() {
            let _ = writeln!(report, "  {AFTER_RESTART}: not spawned");
        }
        for node in NODES {
            let _ = write!(
                report,
                "\n===== log of {node} =====\n{}",
                coord.node_log(node)
            );
        }
        report
    }
}

/// Whether a feed or close that failed with `failure` is allowed on a
/// dataflow: a refusal only if a fault `touched` it, and no answer never,
/// since that is a hang.
pub fn input_failure_allowed(failure: &InputFailure, touched: bool) -> bool {
    match failure {
        InputFailure::Refused(_) => touched,
        InputFailure::NoResponse(_) => false,
    }
}

/// Spawn a dataflow of `kind` on `nodes`, with [`EPOCHS`] epochs of
/// [`RECORDS_PER_FEED`] records a feed, made from the seed and the
/// dataflow's `index`. Panics unless every node spawns it.
async fn spawn(
    coord: &mut TestCoordinator,
    seed: u64,
    index: usize,
    id: &str,
    nodes: &[String],
    kind: WorkloadKind,
) -> Run {
    let names: Vec<&str> = nodes.iter().map(String::as_str).collect();
    let workers: Vec<(&str, usize)> = names.iter().map(|&node| (node, WORKERS)).collect();
    let workload_seed = seed.wrapping_mul(16).wrapping_add(index as u64);
    let workload = Workload::new_sized(kind, &workers, EPOCHS, RECORDS_PER_FEED, workload_seed);
    let spawn = coord.spawn_shared(id, &topology(&names), &workload).await;
    Run {
        id: id.to_string(),
        nodes: nodes.to_vec(),
        workload,
        spawn,
    }
}

/// Polls the future it wraps, turning a panic into an `Err` with the
/// panic's message, so that a run the harness panicked in still reports.
struct Caught<F>(Pin<Box<F>>);

impl<F: Future> Caught<F> {
    fn new(future: F) -> Self {
        Caught(Box::pin(future))
    }
}

impl<F: Future> Future for Caught<F> {
    type Output = Result<F::Output, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.0.as_mut();
        match panic::catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(payload) => Poll::Ready(Err(panic_message(payload.as_ref()))),
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message.to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "a panic with no message".to_string()
    }
}
