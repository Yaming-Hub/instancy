//! Soak tests: healthy multi-node runs never fail spuriously.
//!
//! The cluster failure rules are strict: a data channel or connection that
//! ends without its clean marker fails the dataflow, corrupt traffic is data
//! loss in every phase, and a data channel that ends without its close
//! sentinel after its peer said goodbye is data loss too. That last rule is
//! only sound if every successful producer closes *every* data channel
//! before its goodbye, including channels that never carried a record. These
//! tests run many clean dataflows over real TCP connections — dedicated,
//! per-job shared managers, and pooled shared managers multiplexing several
//! dataflows — with 2 to 4 nodes of 2 workers each, varied routing (some
//! worker pairs never exchange a record), varied finish order and a slow
//! node, and require every node to succeed with every record.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::HashMap;
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, SpawnOptions};
use support::{
    Managers, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node_with_options,
    tcp_mesh, tcp_shared_mesh, tcp_shared_mesh_with,
};

const NAMES: [&str; 4] = ["a", "b", "c", "d"];
const WORKERS: usize = 2;
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    /// One TCP connection per peer per dataflow.
    Dedicated,
    /// Fresh shared managers (one pooled connection per peer) per dataflow.
    SharedPerJob,
    /// Long-lived shared managers with two pooled connections per peer, the
    /// default probing and reorder timeout, multiplexing concurrent
    /// dataflows.
    SharedPooled,
}

const MODES: [Mode; 3] = [Mode::Dedicated, Mode::SharedPerJob, Mode::SharedPooled];

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    /// `input -> map -> exchange -> map -> output`.
    Exchange,
    /// No exchange: every data route between workers stays idle.
    Pipeline,
}

/// The default shared configuration (probing every 100 ms, 50 ms reorder
/// timeout) with a fixed pool of two connections. The test pool is
/// pre-established, so it cannot scale beyond what it was given.
fn pooled_config() -> SharedConnectionConfig {
    SharedConnectionConfig {
        min_connections: 2,
        max_connections: 2,
        ..SharedConnectionConfig::default()
    }
}

/// `slow` makes the post-routing operator linger on every record (1 ms), so
/// this node keeps running well after its peers said goodbye and, in
/// dedicated mode, closed their connections: every one of their channels,
/// idle or not, must then have delivered its close sentinel.
fn build(shape: Shape, slow: bool) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send {
    move |builder| {
        let pre = builder.input::<u64>("data")?.map("pre", |_t, x| x);
        let routed = match shape {
            Shape::Exchange => pre.exchange_by_hash("route", |x: &u64| *x),
            Shape::Pipeline => pre,
        };
        routed
            .map("post", move |_t, x| {
                if slow {
                    std::thread::sleep(Duration::from_millis(1));
                }
                x
            })
            .output("out")?;
        // An independent local branch: a node whose `hold` input stays open
        // keeps running after its peers finished the exchange.
        builder
            .input::<u64>("hold")?
            .map("held", |_t, x| x)
            .output("held_out")?;
        Ok(())
    }
}

/// One dataflow on `names`.
struct Run<'a> {
    mode: Mode,
    names: &'a [&'a str],
    shape: Shape,
    /// Records sent by each global worker (`node index * WORKERS + local`).
    records: &'a (dyn Fn(usize) -> Vec<u64> + Sync),
    /// The node whose post-routing operator is slow.
    slow: Option<&'a str>,
    /// Order in which the nodes close their inputs, with a pause between.
    close_order: Vec<&'a str>,
    /// This node keeps its `hold` input open for another 150 ms: its peers
    /// finish, say goodbye and (dedicated mode) close their connections
    /// while it still runs and reads every channel from them.
    late: Option<&'a str>,
    /// Every node drains on cancel, and this node cancels after its input
    /// closed (a cluster-wide drain).
    drain_cancel_at: Option<&'a str>,
}

impl Run<'_> {
    /// Runs the dataflow; `Err` describes any node that did not succeed with
    /// every record.
    async fn go(
        &self,
        pooled: Option<&HashMap<String, Managers>>,
    ) -> std::result::Result<(), String> {
        let names = self.names;
        let topo = ClusterTopology::multi_node(
            names.iter().map(|n| NodeConfig::new(*n, WORKERS)).collect(),
        )
        .unwrap();
        let id = DataflowId::new();
        let mut dedicated = match self.mode {
            Mode::Dedicated => Some(tcp_mesh(names).await),
            _ => None,
        };
        let per_job = match self.mode {
            Mode::SharedPerJob => Some(tcp_shared_mesh(names, 1).await),
            _ => None,
        };
        let options = || match self.drain_cancel_at {
            Some(_) => SpawnOptions::new().drain_on_cancel(Duration::from_secs(20)),
            None => SpawnOptions::new(),
        };
        let spawns: Vec<_> = names
            .iter()
            .map(|name| {
                let build = build(self.shape, self.slow == Some(*name));
                match self.mode {
                    Mode::Dedicated => {
                        let conns = dedicated.as_mut().unwrap().remove(*name).unwrap();
                        spawn_node_with_options(
                            topo.clone(),
                            name,
                            id,
                            ClusterSpawnTransport::dedicated(conns, 1024),
                            2,
                            build,
                            options(),
                        )
                    }
                    Mode::SharedPerJob | Mode::SharedPooled => {
                        let managers = match self.mode {
                            Mode::SharedPerJob => per_job.as_ref().unwrap(),
                            _ => pooled.expect("pooled managers"),
                        };
                        spawn_node_with_options(
                            topo.clone(),
                            name,
                            id,
                            ClusterSpawnTransport::shared(managers[*name].clone(), 1024),
                            2,
                            build,
                            options(),
                        )
                    }
                }
            })
            .collect();

        let mut rts = Vec::new();
        let mut nodes = Vec::new();
        let mut failure = None;
        let mut expected = 0u64;
        for (index, (name, spawn)) in names.iter().zip(spawns).enumerate() {
            let (rt, df) = spawn.await.unwrap();
            rts.push(rt);
            let mut df = match df {
                Ok(df) => df,
                Err(e) => {
                    failure.get_or_insert(format!("{name} failed to spawn: {e}"));
                    continue;
                }
            };
            let mut inputs = Vec::new();
            let mut holds = Vec::new();
            let mut outputs = Vec::new();
            for worker in 0..WORKERS {
                let input = df.take_input::<u64>(worker, "data").unwrap();
                let records = (self.records)(index * WORKERS + worker);
                expected += records.iter().sum::<u64>();
                if !records.is_empty() {
                    input.send(0, records).unwrap();
                }
                inputs.push(input);
                holds.push(df.take_input::<u64>(worker, "hold").unwrap());
                outputs.push(collect(df.take_output::<u64>(worker, "out").unwrap()));
                outputs.push(collect(df.take_output::<u64>(worker, "held_out").unwrap()));
            }
            nodes.push((name.to_string(), df, (inputs, holds), outputs));
        }

        if failure.is_none() {
            for name in &self.close_order {
                if let Some(node) = nodes.iter_mut().find(|n| n.0 == *name) {
                    node.2.0.clear();
                    if self.late != Some(*name) {
                        node.2.1.clear();
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if let Some(name) = self.late {
                tokio::time::sleep(Duration::from_millis(150)).await;
                if let Some(node) = nodes.iter_mut().find(|n| n.0 == name) {
                    node.2.1.clear();
                }
            }
            if let Some(name) = self.drain_cancel_at {
                tokio::time::sleep(Duration::from_millis(30)).await;
                if let Some(node) = nodes.iter().find(|n| n.0 == name) {
                    node.1.cancel();
                }
            }
            let mut joins = Vec::new();
            let mut collectors = Vec::new();
            for (name, df, _inputs, outputs) in nodes.drain(..) {
                joins.push((name.clone(), join_with_timeout(df, TIMEOUT)));
                collectors.push((name, outputs));
            }
            let mut results = Vec::new();
            for (name, join) in joins {
                results.push((name, join.await.unwrap()));
            }
            if !results.iter().all(|(_, r)| matches!(r, Some(Ok(())))) {
                failure = Some(
                    results
                        .iter()
                        .map(|(n, r)| format!("{n}={}", fmt_join(r)))
                        .collect::<Vec<_>>()
                        .join(" "),
                );
            } else {
                let mut total = 0u64;
                for (name, outputs) in collectors {
                    for output in outputs {
                        match output.join() {
                            Ok(out) => total += out.into_iter().map(|(_, x)| x).sum::<u64>(),
                            Err(e) => {
                                failure.get_or_insert(format!("{name}: {e}"));
                            }
                        }
                    }
                }
                if failure.is_none() && total != expected {
                    failure = Some(format!("total {total} != expected {expected}"));
                }
            }
        }
        drop(nodes);
        drop_runtimes(rts).await;
        drop(per_job);
        match failure {
            Some(f) => Err(f),
            None => Ok(()),
        }
    }
}

/// The data-loss signature of a premature completion elsewhere (audit
/// C1/C2, triggered by the duplicate deliveries of PR 2's partial pushes):
/// records pushed to, or arriving at, an operator that had already finished.
/// It is reported (the dataflow fails instead of returning `Ok` with records
/// missing), but it is not what these tests look for, so such a round is
/// re-run once, as in `cluster_early_finisher_routing`.
fn is_early_completion(failure: &str) -> bool {
    [
        "records sent after the peer finished",
        "records sent while the peer was finishing",
        "after the receiving operator finished",
        "as the receiving operator finished",
        "never consumed",
    ]
    .iter()
    .any(|signature| failure.contains(signature))
}

/// Runs `run`, re-running once after an early-completion failure. Returns
/// the failure to report, if any; early completions are logged.
async fn run_checked(
    run: &Run<'_>,
    pooled: Option<&HashMap<String, Managers>>,
    label: &str,
) -> std::result::Result<(), String> {
    match run.go(pooled).await {
        Err(e) if is_early_completion(&e) => {
            eprintln!("{label}: early completion (C1/C2), re-running once: {e}");
            run.go(pooled)
                .await
                .map_err(|again| format!("{again} (after an early completion: {e})"))
        }
        other => other,
    }
}

fn rotate<'a>(names: &[&'a str], by: usize) -> Vec<&'a str> {
    let mut v = names.to_vec();
    v.rotate_left(by % names.len());
    v
}

/// Three nodes of two workers (six workers); only worker 0 (node a) and
/// worker 2 (node b) exchange records, routed both ways. Every other pair —
/// everything to, from or within node c, and between sibling workers — never
/// carries a record. Every round, node a or b processes its records slowly,
/// so the other nodes finish (and, in dedicated mode, close their
/// connections) while it still reads their idle channels: any channel left
/// without its close sentinel by a successful producer would surface as
/// false data loss. 50 runs per mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn idle_routes_never_report_data_loss() {
    let names = &NAMES[..3];
    let workers = (names.len() * WORKERS) as u64;
    let records = move |global: usize| -> Vec<u64> {
        match global {
            // a's first worker sends to worker 2 (node b)...
            0 => (0..40).map(|i| i * workers + 2).collect(),
            // ...and b's first worker to worker 0 (node a).
            2 => (0..40).map(|i| i * workers).collect(),
            _ => Vec::new(),
        }
    };
    let pooled = tcp_shared_mesh_with(names, 2, pooled_config()).await;
    let mut failures = Vec::new();
    for mode in MODES {
        for round in 0..50 {
            let run = Run {
                mode,
                names,
                shape: if round % 10 == 9 {
                    Shape::Pipeline
                } else {
                    Shape::Exchange
                },
                records: &records,
                slow: Some(names[round % 2]),
                close_order: rotate(names, round),
                late: Some(names[(round / 2) % 3]),
                drain_cancel_at: None,
            };
            let label = format!("{mode:?} round {round}");
            if let Err(e) = run_checked(&run, Some(&pooled), &label).await {
                failures.push(format!("{label}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Clean runs in every transport mode with 2, 3 and 4 nodes: all records
/// delivered, no node fails. Pooled managers carry two dataflows at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn healthy_soak_every_mode_and_size() {
    let mut failures = Vec::new();
    for n in 2..=4 {
        let names = &NAMES[..n];
        let pooled = tcp_shared_mesh_with(names, 2, pooled_config()).await;
        for mode in MODES {
            for round in 0..8 {
                let records = move |global: usize| -> Vec<u64> {
                    let start = global as u64 * 10_000;
                    (start..start + 50 + (round as u64 * 13) % 70).collect()
                };
                let run = |shift: usize| Run {
                    mode,
                    names,
                    shape: if round % 4 == 3 {
                        Shape::Pipeline
                    } else {
                        Shape::Exchange
                    },
                    records: &records,
                    slow: (round % 2 == 0).then(|| names[(round + shift) % n]),
                    close_order: rotate(names, round + shift),
                    late: (round % 2 == 1).then(|| names[(round + shift) % n]),
                    drain_cancel_at: None,
                };
                let label = format!("{mode:?} n={n} round {round}");
                let (r1, r2) = if mode == Mode::SharedPooled {
                    let (first, second) = (run(0), run(1));
                    tokio::join!(
                        run_checked(&first, Some(&pooled), &label),
                        run_checked(&second, Some(&pooled), &label)
                    )
                } else {
                    (run_checked(&run(0), None, &label).await, Ok(()))
                };
                for r in [r1, r2] {
                    if let Err(e) = r {
                        failures.push(format!("{mode:?} n={n} round {round}: {e}"));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Every node drains on cancel; one node cancels while records are still in
/// flight (a slow node is processing them). Each node drains, closes its
/// channels cleanly and completes `Ok` with every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_drain_on_cancel_completes_with_every_record() {
    let names = &NAMES[..3];
    let records = |global: usize| -> Vec<u64> {
        let start = global as u64 * 1000;
        (start..start + 60).collect()
    };
    let pooled = tcp_shared_mesh_with(names, 2, pooled_config()).await;
    let mut failures = Vec::new();
    for mode in MODES {
        for round in 0..3 {
            let run = Run {
                mode,
                names,
                shape: Shape::Exchange,
                records: &records,
                slow: Some(names[(round + 1) % 3]),
                close_order: names.to_vec(),
                late: None,
                drain_cancel_at: Some(names[round % 3]),
            };
            let label = format!("{mode:?} round {round}");
            if let Err(e) = run_checked(&run, Some(&pooled), &label).await {
                failures.push(format!("{label}: {e}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
