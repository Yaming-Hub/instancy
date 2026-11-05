//! A seeded randomized fault campaign over a Shared-transport cluster of four
//! node processes: kills, plain or behind a partition (some followed by a
//! restart), connection resets and closes, delays, throttles and stalls
//! (some longer than the spawn handshake timeout), mixed by a seed. See
//! `support/campaign.rs` for the rules.
//!
//! A failing seed's panic message is its bug report: the seed, the
//! schedule, what the run did, every dataflow's outcomes and every node's
//! log. To rerun one seed, run `campaign_long` with
//! `INSTANCY_CAMPAIGN_FIRST_SEED` set to it and `INSTANCY_CAMPAIGN_SEEDS=1`.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use instancy_integration::coordinator::InputFailure;
use instancy_integration::peer_link::node_set_key;
use instancy_integration::workloads::WorkloadKind;
use support::campaign::{
    input_failure_allowed, run, run_schedule, schedule, Fault, Link, Schedule, EPOCHS, NODES,
};
use support::scenarios::{CONNECTIONS, WORKERS};

#[test]
fn schedule_is_deterministic() {
    let first = schedule(7);
    let second = schedule(7);
    assert_eq!(format!("{first}"), format!("{second}"));
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
}

/// Every schedule keeps to the campaign's table, and across many seeds every
/// kind of dataflow and fault comes up.
#[test]
fn schedules_follow_the_rules() {
    let mut kinds = BTreeSet::new();
    let mut sizes = BTreeSet::new();
    let mut faults = BTreeSet::new();
    for seed in 0..1_000 {
        let schedule = schedule(seed);
        assert_eq!(schedule.dataflows.len(), 3, "seed {seed}: {schedule}");
        let ids: BTreeSet<&String> = schedule.dataflows.iter().map(|(id, ..)| id).collect();
        assert_eq!(ids.len(), 3, "seed {seed}: dataflow ids repeat: {schedule}");
        for (id, nodes, kind) in &schedule.dataflows {
            assert!(
                (3..=4).contains(&nodes.len())
                    && nodes.windows(2).all(|pair| pair[0] < pair[1])
                    && nodes.iter().all(|node| NODES.contains(&node.as_str())),
                "seed {seed}: {id} must run on 3 or 4 distinct nodes, in order: {schedule}"
            );
            kinds.insert(kind.name());
            sizes.insert(nodes.len());
            if let WorkloadKind::StagedFanOutFanIn {
                fan_out_parallelism,
            } = kind
            {
                assert_eq!(
                    *fan_out_parallelism,
                    nodes.len() * WORKERS,
                    "seed {seed}: {id} must fan out to every worker: {schedule}"
                );
            }
        }

        assert!(
            (1..=3).contains(&schedule.faults.len()),
            "seed {seed}: {schedule}"
        );
        assert!(
            schedule
                .faults
                .windows(2)
                .all(|pair| pair[0].0 <= pair[1].0),
            "seed {seed}: faults out of epoch order: {schedule}"
        );
        let kills = schedule
            .faults
            .iter()
            .filter(|(_, fault)| matches!(fault, Fault::Kill { .. } | Fault::PartitionKill { .. }))
            .count();
        assert!(kills <= 1, "seed {seed}: {kills} kills: {schedule}");
        for (epoch, fault) in &schedule.faults {
            assert!(*epoch < EPOCHS, "seed {seed}: {schedule}");
            let (name, in_range) = match fault {
                Fault::Kill { node, restart } => (
                    format!("Kill {{ restart: {restart} }}"),
                    NODES.contains(&node.as_str()),
                ),
                Fault::PartitionKill { node, restart } => (
                    format!("PartitionKill {{ restart: {restart} }}"),
                    NODES.contains(&node.as_str()),
                ),
                Fault::Reset { link } => ("Reset".into(), on_a_node_set(link, &schedule)),
                Fault::Close { link } => ("Close".into(), on_a_node_set(link, &schedule)),
                Fault::Delay { link, ms } => (
                    "Delay".into(),
                    on_a_node_set(link, &schedule) && (1..=100).contains(ms),
                ),
                Fault::Throttle {
                    link,
                    bytes_per_sec,
                } => (
                    "Throttle".into(),
                    on_a_node_set(link, &schedule)
                        && (32 * 1024..=256 * 1024).contains(bytes_per_sec),
                ),
                Fault::StallThenHeal { link, ms } => (
                    if *ms > 10_000 {
                        "StallThenHeal (long)".into()
                    } else {
                        "StallThenHeal".into()
                    },
                    on_a_node_set(link, &schedule)
                        && ((500..=2_000).contains(ms) || (11_000..=13_000).contains(ms)),
                ),
            };
            assert!(in_range, "seed {seed}: {fault:?} is out of range");
            faults.insert(name);
        }
    }
    assert_eq!(kinds.len(), 6, "workload kinds seen: {kinds:?}");
    assert_eq!(sizes, BTreeSet::from([3, 4]));
    assert_eq!(faults.len(), 10, "faults seen: {faults:?}");
}

/// Whether `link` is one of the pooled connections of the node set of one of
/// `schedule`'s dataflows, on a relay route between two of its nodes (the
/// higher ID dials the lower).
fn on_a_node_set(link: &Link, schedule: &Schedule) -> bool {
    schedule.dataflows.iter().any(|(_, nodes, _)| {
        node_set_key(nodes.iter().map(String::as_str)) == link.node_set
            && nodes.contains(&link.from)
            && nodes.contains(&link.to)
            && link.from > link.to
    }) && link.conn < CONNECTIONS as u64
}

fn nodes(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

fn link(from: &str, to: &str, node_set: &[&str], conn: u64) -> Link {
    Link {
        from: from.into(),
        to: to.into(),
        node_set: node_set_key(node_set.iter().copied()),
        conn,
    }
}

/// A kill touches the dataflows on its node. A reset or close touches only
/// the dataflows of the node set whose connection it hit, not others on the
/// same pair of nodes, which have their own connections. Slowing a
/// connection touches nothing.
#[test]
fn faults_touch_only_what_they_hit() {
    let abc = nodes(&["a", "b", "c"]);
    let abcd = nodes(&["a", "b", "c", "d"]);
    let abd = nodes(&["a", "b", "d"]);
    let on_abc = link("b", "a", &["a", "b", "c"], 0);
    for fault in [
        Fault::Reset {
            link: on_abc.clone(),
        },
        Fault::Close {
            link: on_abc.clone(),
        },
    ] {
        assert!(fault.touches(&abc), "{fault}");
        assert!(!fault.touches(&abcd), "{fault}");
        assert!(!fault.touches(&abd), "{fault}");
    }
    for fault in [
        Fault::Delay {
            link: on_abc.clone(),
            ms: 50,
        },
        Fault::Throttle {
            link: on_abc.clone(),
            bytes_per_sec: 65_536,
        },
        Fault::StallThenHeal {
            link: on_abc.clone(),
            ms: 500,
        },
    ] {
        assert!(!fault.touches(&abc), "{fault}");
    }
    let kill = Fault::Kill {
        node: "c".into(),
        restart: false,
    };
    assert!(kill.touches(&abc) && kill.touches(&abcd) && !kill.touches(&abd));
    let partition_kill = Fault::PartitionKill {
        node: "c".into(),
        restart: true,
    };
    assert!(
        partition_kill.touches(&abc)
            && partition_kill.touches(&abcd)
            && !partition_kill.touches(&abd)
    );
}

/// A feed or close a node refuses is allowed only on a touched dataflow. One
/// it never answers is a hang, never allowed, touched or not.
#[test]
fn a_hung_feed_or_close_is_never_allowed() {
    let refused = InputFailure::Refused("FeedData of df1 failed on a: closed".into());
    let hung = InputFailure::NoResponse("FeedData of df1: no response from a within 30 s".into());
    assert!(input_failure_allowed(&refused, true));
    assert!(!input_failure_allowed(&refused, false));
    assert!(!input_failure_allowed(&hung, true));
    assert!(!input_failure_allowed(&hung, false));
}
/// Both connections of node set {a, b, c} from b to a are reset while every
/// dataflow's inputs are open. The dataflows on {a, b, c, d} and {a, b, d}
/// also run on a and b, but over their own node sets' connections: they are
/// untouched, so they must give exact output on every worker, and the run
/// must find the {a, b, c} dataflow touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reset_touches_only_the_node_set_of_its_connection() {
    let abc = ["a", "b", "c"];
    let schedule = Schedule {
        dataflows: vec![
            ("df1".into(), nodes(&abc), WorkloadKind::ExchangeRoundTrip),
            (
                "df2".into(),
                nodes(&["a", "b", "c", "d"]),
                WorkloadKind::MultiEpochExchange,
            ),
            (
                "df3".into(),
                nodes(&["a", "b", "d"]),
                WorkloadKind::WordCount,
            ),
        ],
        faults: vec![
            (
                1,
                Fault::Reset {
                    link: link("b", "a", &abc, 0),
                },
            ),
            (
                1,
                Fault::Reset {
                    link: link("b", "a", &abc, 1),
                },
            ),
        ],
    };
    let touched = run_schedule(0, schedule)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let expected: BTreeMap<String, bool> = [("df1", true), ("df2", false), ("df3", false)]
        .into_iter()
        .map(|(id, touched)| (id.to_string(), touched))
        .collect();
    assert_eq!(touched, expected);
}

/// The campaign's partition kill, on a: every connection to and from a
/// (each one into a: a has the lowest ID) is stalled, a is killed and
/// reported left, then restarted. The run itself fails unless the stalls
/// held until the leave and the survivors' reports cancelled both
/// dataflows a was in, which no other fault touched; the one without a
/// must give exact output, as must the one after the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_kill_is_cancelled_by_the_leave_reports() {
    let schedule = Schedule {
        dataflows: vec![
            (
                "df1".into(),
                nodes(&["a", "b", "c", "d"]),
                WorkloadKind::ExchangeRoundTrip,
            ),
            (
                "df2".into(),
                nodes(&["a", "b", "c"]),
                WorkloadKind::IterativeExchange { threshold: 1_000 },
            ),
            (
                "df3".into(),
                nodes(&["b", "c", "d"]),
                WorkloadKind::StagedFanOutFanIn {
                    fan_out_parallelism: 3 * WORKERS,
                },
            ),
        ],
        faults: vec![(
            1,
            Fault::PartitionKill {
                node: "a".into(),
                restart: true,
            },
        )],
    };
    let touched = run_schedule(0, schedule)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let expected: BTreeMap<String, bool> = [("df1", true), ("df2", true), ("df3", false)]
        .into_iter()
        .map(|(id, touched)| (id.to_string(), touched))
        .collect();
    assert_eq!(touched, expected);
}

macro_rules! campaign_seeds {
    ($($name:ident: $seed:expr),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() {
                run($seed).await.unwrap_or_else(|e| panic!("{e}"));
            }
        )*
    };
}

campaign_seeds! {
    campaign_seed_1: 1,
    campaign_seed_2: 2,
    campaign_seed_3: 3,
    campaign_seed_4: 4,
    campaign_seed_5: 5,
    campaign_seed_6: 6,
}

/// The long campaign: `INSTANCY_CAMPAIGN_SEEDS` seeds (200 by default) from
/// `INSTANCY_CAMPAIGN_FIRST_SEED` (1000 by default), in order. Stops at the
/// first failing seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "long run, outside the ci profile's 120 s limit: run it on purpose with --run-ignored only"]
async fn campaign_long() {
    let seeds = env_u64("INSTANCY_CAMPAIGN_SEEDS", 200);
    let first = env_u64("INSTANCY_CAMPAIGN_FIRST_SEED", 1000);
    let started = Instant::now();
    for seed in first..first + seeds {
        let run_started = Instant::now();
        run(seed).await.unwrap_or_else(|e| panic!("{e}"));
        println!(
            "campaign seed {seed}: passed in {:.1} s",
            run_started.elapsed().as_secs_f64()
        );
    }
    println!(
        "campaign: {seeds} seeds from {first} passed in {:.1} s",
        started.elapsed().as_secs_f64()
    );
}

fn env_u64(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|e| panic!("{name}={value:?} is not a number: {e}")),
        Err(_) => default,
    }
}
