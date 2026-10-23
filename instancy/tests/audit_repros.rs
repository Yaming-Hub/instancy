//! The correctness audit's repro tests, run against a table of today's
//! failures (step 1 of the progress-accounting redesign; Appendix A of the
//! design).
//!
//! The modules below are the audit's integration tests for the findings that
//! steps 2 and 3 of the redesign fix. They assert *correct* behaviour and are
//! unchanged except for an `#[ignore]` on each: many are races with their own
//! deadlines, so they run on demand rather than in CI (on one core, or on a
//! loaded machine, their deadlines expire and they fail differently).
//! [`EXPECTED`] records, by audit finding, every repro that fails today and
//! exactly how: the panic location and message (a repro that hangs gives up
//! at its own deadline with a panic). Repros that fail only sometimes are
//! listed as such.
//!
//! `audit_repros_match_table` runs every repro in a child process (this same
//! test binary, `--ignored --exact`) under a timeout, a few at a time, and
//! checks that each listed repro fails as recorded and every other repro
//! passes. A listed repro that starts passing, or fails differently (say a
//! hang turns into a loss), fails the run: update or remove its entry.
//!
//! Run the table with:
//!
//! ```text
//! cargo test -p instancy --all-features --test audit_repros -- --ignored --exact audit_repros_match_table --nocapture
//! ```
//!
//! (`AUDIT_REPROS_REPORT=1` prints every repro's outcome; `AUDIT_REPROS_JOBS`
//! sets how many run at once, 4 by default.) Run one repro with:
//!
//! ```text
//! cargo test -p instancy --all-features --test audit_repros -- --ignored --exact <module>::<test>
//! ```

#![allow(clippy::all, unused)]

#[path = "audit_repros/builder_broadcast.rs"]
mod builder_broadcast;
#[path = "audit_repros/builder_iterate.rs"]
mod builder_iterate;
#[path = "audit_repros/builder_stages.rs"]
mod builder_stages;
#[path = "audit_repros/executor_pipeline_inflight.rs"]
mod executor_pipeline_inflight;
#[path = "audit_repros/executor_staged.rs"]
mod executor_staged;
#[path = "audit_repros/multinode_late.rs"]
mod multinode_late;
#[path = "audit_repros/multinode_premature.rs"]
mod multinode_premature;
#[path = "audit_repros/netdata_broadcast.rs"]
mod netdata_broadcast;
#[path = "audit_repros/netdata_frontier.rs"]
mod netdata_frontier;
#[path = "audit_repros/netprogress_inflight.rs"]
mod netprogress_inflight;
#[path = "audit_repros/netprogress_pending.rs"]
mod netprogress_pending;
#[path = "audit_repros/netprogress_signed_sum.rs"]
mod netprogress_signed_sum;
#[path = "audit_repros/netprogress_stage_starvation.rs"]
mod netprogress_stage_starvation;
#[path = "audit_repros/netprogress_staged_loop.rs"]
mod netprogress_staged_loop;
#[path = "audit_repros/netprogress_startup.rs"]
mod netprogress_startup;
#[path = "audit_repros/operators_broadcast.rs"]
mod operators_broadcast;
#[path = "audit_repros/operators_core.rs"]
mod operators_core;
#[path = "audit_repros/operators_iterate.rs"]
mod operators_iterate;
#[path = "audit_repros/progress_cluster_spurious_complete.rs"]
mod progress_cluster_spurious_complete;
#[path = "audit_repros/progress_cluster_unwired.rs"]
mod progress_cluster_unwired;
#[path = "audit_repros/progress_fanout_exchange.rs"]
mod progress_fanout_exchange;
#[path = "audit_repros/progress_fanout_probe.rs"]
mod progress_fanout_probe;
#[path = "audit_repros/progress_loop_frontier.rs"]
mod progress_loop_frontier;
#[path = "audit_repros/progress_notify_backpressure.rs"]
mod progress_notify_backpressure;
#[path = "audit_repros/progress_total_counts.rs"]
mod progress_total_counts;
#[path = "audit_repros/progress_unwired_exchange.rs"]
mod progress_unwired_exchange;

use std::collections::BTreeSet;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// How a repro fails today: it panics at `at` (`file.rs:line`) with a
/// message containing `text`. (A repro that hangs gives up at its own
/// deadline with such a panic, so a fix that only makes it slow still shows.)
/// A race may decide which of several symptoms of one finding shows: such a
/// repro lists each signature (`panics_any`).
#[derive(Clone, Copy, Debug)]
enum Fails {
    Panics {
        at: &'static str,
        text: &'static str,
    },
    /// Panics with any one of these `(at, text)` signatures.
    PanicsAny(&'static [(&'static str, &'static str)]),
}

const fn panics(at: &'static str, text: &'static str) -> Fails {
    Fails::Panics { at, text }
}

const fn panics_any(signatures: &'static [(&'static str, &'static str)]) -> Fails {
    Fails::PanicsAny(signatures)
}

impl Fails {
    /// Whether `output` shows this failure.
    fn shown_by(self, output: &str) -> bool {
        match self {
            Fails::Panics { at, text } => panicked_as(output, at, text),
            Fails::PanicsAny(signatures) => signatures
                .iter()
                .any(|(at, text)| panicked_as(output, at, text)),
        }
    }
}

/// One repro that fails on today's code.
struct Repro {
    /// `<module>::<test>`.
    test: &'static str,
    /// The audit finding (or design inventory row) behind the failure.
    audit: &'static str,
    fails: Fails,
    /// The failure depends on scheduling: the repro may also pass.
    intermittent: bool,
}

/// Every repro must finish within this long (the slowest give up on their
/// own after 20 s).
const TIMEOUT: Duration = Duration::from_secs(120);
/// Repros run at once (override with `AUDIT_REPROS_JOBS`).
const JOBS: usize = 4;

const fn repro(test: &'static str, audit: &'static str, fails: Fails) -> Repro {
    Repro {
        test,
        audit,
        fails,
        intermittent: false,
    }
}

/// A repro that fails as recorded only sometimes (and otherwise passes).
const fn sometimes(test: &'static str, audit: &'static str, fails: Fails) -> Repro {
    Repro {
        test,
        audit,
        fails,
        intermittent: true,
    }
}

/// Today's failing repros, by module.
const EXPECTED: &[Repro] = &[
    repro(
        "builder_broadcast::audit_builder_broadcast_epoch_never_completes_while_input_open_multi",
        "H10",
        panics(
            "builder_broadcast.rs:88",
            "count for epoch 0 downstream of broadcast never emitted",
        ),
    ),
    repro(
        "builder_broadcast::audit_builder_broadcast_epoch_never_completes_while_input_open_single",
        "H10",
        panics(
            "builder_broadcast.rs:99",
            "count for epoch 0 downstream of broadcast never emitted",
        ),
    ),
    repro(
        "executor_pipeline_inflight::notify_after_pipeline_backlog_fires_once_with_full_count",
        "H1",
        panics(
            "executor_pipeline_inflight.rs:63",
            "fired before all epoch-0 data arrived",
        ),
    ),
    repro(
        "executor_pipeline_inflight::notify_after_unary_async_fires_once_with_full_count",
        "H1/O-4",
        panics(
            "executor_pipeline_inflight.rs:106",
            "async results were in flight",
        ),
    ),
    repro(
        "multinode_late::late_matrix",
        "C2",
        panics("multinode_late.rs:351", "late data observed in"),
    ),
    repro(
        "multinode_late::late_matrix_more_shapes",
        "C2",
        panics("multinode_late.rs:323", "late data observed in"),
    ),
    repro(
        "multinode_premature::cluster_input_exchange_trickle_loses_data",
        "C1/C2",
        panics("multinode_premature.rs:186", "node b result=Ok(Ok(()))"),
    ),
    repro(
        "multinode_premature::spawn_multi_input_exchange_trickle_loses_data",
        "C1/C2",
        panics("multinode_premature.rs:202", "result=Ok(Ok(()))"),
    ),
    repro(
        "netprogress_inflight::cluster_input_exchange_completes_prematurely_and_loses_data",
        "C2",
        panics(
            "netprogress_inflight.rs:194",
            "node A reported completion while node B's input",
        ),
    ),
    repro(
        "netprogress_inflight::multi_worker_input_exchange_completes_prematurely_and_loses_data",
        "C2",
        panics(
            "netprogress_inflight.rs:239",
            "lost/duplicated per-epoch counts",
        ),
    ),
    repro(
        "netprogress_inflight::multi_worker_input_exchange_then_iterate_never_completes",
        "C2",
        panics(
            "netprogress_inflight.rs:326",
            "dataflow did not complete within 10s",
        ),
    ),
    sometimes(
        "netprogress_pending::notification_fires_while_consumed_exchange_data_is_still_buffered",
        "H1",
        panics(
            "netprogress_pending.rs:130",
            "epoch 0 must be counted exactly once",
        ),
    ),
    repro(
        "netprogress_signed_sum::cluster_transient_negative_inflight_completes_prematurely",
        "C1",
        panics(
            "netprogress_signed_sum.rs:235",
            "still held an input capability",
        ),
    ),
    repro(
        "netprogress_signed_sum::multi_worker_transient_negative_inflight_completes_prematurely",
        "C1",
        panics("netprogress_signed_sum.rs:151", "lost results"),
    ),
    repro(
        "netprogress_staged_loop::loop_exit_on_other_worker_never_converges",
        "C2",
        panics(
            "netprogress_staged_loop.rs:88",
            "staged loop never converged",
        ),
    ),
    repro(
        "operators_broadcast::broadcast_then_count_emits_before_close_1_worker",
        "H10",
        panics("operators_broadcast.rs:76", "produced no epoch-0 count"),
    ),
    repro(
        "operators_broadcast::broadcast_then_count_emits_before_close_2_workers",
        "H10",
        panics("operators_broadcast.rs:76", "produced no epoch-0 count"),
    ),
    repro(
        "operators_broadcast::unary_async_map_then_exchange_loses_nothing",
        "O-4/H1",
        panics("operators_broadcast.rs:291", "with_map=true"),
    ),
    repro(
        "operators_broadcast::unary_async_then_exchange_loses_nothing",
        "O-4/C2",
        panics("operators_broadcast.rs:291", "with_map=false"),
    ),
    repro(
        "operators_core::count_emits_single_result_per_epoch_with_input_advance",
        "C2",
        panics("operators_core.rs:107", "expected one count of 1000"),
    ),
    repro(
        "operators_iterate::branch_then_exchange_inside_iterate_two_workers",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "branch_xchg_2: did not finish within 20s",
        ),
    ),
    sometimes(
        "operators_iterate::collatz_map_exchange_matches_oracle",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "collatz: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::count_after_long_running_loop_single_value_per_epoch",
        "H11",
        panics("operators_iterate.rs:234", "epoch 0: all ="),
    ),
    repro(
        "operators_iterate::diag_loop_data_crossing_sweep_counters",
        "C2",
        panics("operators_iterate.rs:524", "hang: see counters above"),
    ),
    repro(
        "operators_iterate::distinct_inside_iterate_body",
        "H11",
        panics(
            "operators_iterate.rs:100",
            "assertion `left == right` failed",
        ),
    ),
    repro(
        "operators_iterate::identity_delay_inside_iterate_body",
        "H11",
        panics(
            "operators_iterate.rs:129",
            "assertion `left == right` failed",
        ),
    ),
    repro(
        "operators_iterate::input_exchange_then_iterate_two_workers",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "in_xchg_loop_2: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::iterate_then_exchange_two_workers",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "loop_xchg_2: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::loop_data_crossing_sweep_not_lost_two_workers",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "loop_sweep_2: did not finish within 20s",
        ),
    ),
    sometimes(
        "operators_iterate::staged_loop_control_without_drops",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "staged join: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::staged_loop_exit_on_other_worker_terminates",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "staged join: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::staged_loop_with_dropped_records_terminates",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "staged join: did not finish within 20s",
        ),
    ),
    repro(
        "operators_iterate::unary_then_exchange_inside_iterate_two_workers",
        "C2",
        panics(
            "operators_iterate.rs:26",
            "unary_xchg_2: did not finish within 20s",
        ),
    ),
    repro(
        "progress_cluster_spurious_complete::tcp_input_exchange_idle_node_does_not_drop_late_records",
        "C1",
        panics(
            "progress_cluster_spurious_complete.rs:220",
            "data lost on the link to peer",
        ),
    ),
    sometimes(
        "progress_cluster_spurious_complete::tcp_map_exchange_loss_frequency",
        "C1",
        panics(
            "progress_cluster_spurious_complete.rs:254",
            "records sent after the peer finished",
        ),
    ),
    repro(
        "progress_cluster_unwired::tcp_input_exchange_iterate_completes",
        "C2",
        panics(
            "progress_cluster_unwired.rs:190",
            "HANG: cluster did not complete within 20s",
        ),
    ),
    repro(
        "progress_fanout_probe::two_exchanges_one_output_two_workers_frontier_passes_zero",
        "H12",
        panics("progress_fanout_probe.rs:115", "probes stuck at <= 0"),
    ),
    repro(
        "progress_total_counts::input_exchange_late_records_to_idle_worker_are_delivered",
        "C1",
        panics("progress_total_counts.rs:128", "spuriously completed"),
    ),
    repro(
        "progress_unwired_exchange::input_exchange_iterate_two_workers_completes",
        "C2",
        panics(
            "progress_unwired_exchange.rs:106",
            "HANG: Input -> exchange -> iterate",
        ),
    ),
    repro(
        "progress_unwired_exchange::notify_exchange_iterate_two_workers_completes",
        "C2",
        panics(
            "progress_unwired_exchange.rs:112",
            "HANG: Notify -> exchange -> iterate",
        ),
    ),
    sometimes(
        "netdata_frontier::count_per_epoch_wrong_when_frontier_overtakes_remote_data",
        "C1",
        panics_any(&[
            (
                "netdata_frontier.rs:478",
                "records sent after the peer finished",
            ),
            ("netdata_frontier.rs:480", "epochs with wrong/split counts"),
        ]),
    ),
    sometimes(
        "progress_cluster_spurious_complete::tcp_map_exchange_idle_node_does_not_drop_late_records",
        "C1",
        panics(
            "progress_cluster_spurious_complete.rs:228",
            "records sent after the peer finished",
        ),
    ),
    sometimes(
        "progress_total_counts::map_exchange_slow_sibling_late_records_delivered",
        "C1",
        panics("progress_total_counts.rs:170", "records lost in"),
    ),
    sometimes(
        "progress_cluster_spurious_complete::tcp_map_exchange_slow_sibling_loses_records",
        "C1",
        panics(
            "progress_cluster_spurious_complete.rs:271",
            "data lost on the link to peer",
        ),
    ),
    sometimes(
        "netprogress_staged_loop::control_loop_exit_on_entering_worker_completes",
        "C2",
        panics("netprogress_staged_loop.rs:79", "join=Err(Timeout)"),
    ),
    sometimes(
        "netdata_frontier::frontier_passes_epoch_while_remote_data_in_flight_input_exchange",
        "C2",
        panics(
            "netdata_frontier.rs:301",
            "frontier advanced past an epoch before its remote data arrived",
        ),
    ),
    sometimes(
        "netdata_frontier::frontier_input_exchange_repeated",
        "C2",
        panics(
            "netdata_frontier.rs:352",
            "frontier advanced past epochs before their remote data arrived",
        ),
    ),
    sometimes(
        "netdata_frontier::frontier_control_map_exchange",
        "H1",
        panics(
            "netdata_frontier.rs:312",
            "frontier advanced past an epoch before its remote data arrived",
        ),
    ),
    sometimes(
        "netdata_frontier::frontier_map_exchange_repeated",
        "H1",
        panics(
            "netdata_frontier.rs:337",
            "frontier advanced past epochs before their remote data arrived",
        ),
    ),
];
/// What one child run showed.
enum Outcome {
    Passed,
    Panicked(String),
    TimedOut,
    /// The test did not run (a wrong name) or the child failed oddly.
    Broken(String),
}

fn run_child(test: &str, timeout: Duration) -> (Outcome, Duration) {
    let exe = std::env::current_exe().expect("test binary path");
    let start = Instant::now();
    let mut child = Command::new(exe)
        .args([
            test,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RUST_BACKTRACE", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the test binary");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout.read_to_string(&mut s);
        s
    });
    let err = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr.read_to_string(&mut s);
        s
    });
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for the child") {
            break Some(status);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let elapsed = start.elapsed();
    let output = format!(
        "{}{}",
        out.join().unwrap_or_default(),
        err.join().unwrap_or_default()
    );
    let outcome = match status {
        None => Outcome::TimedOut,
        Some(status) if status.success() => {
            if output.contains("1 passed") {
                Outcome::Passed
            } else {
                Outcome::Broken(format!("ran no test:\n{output}"))
            }
        }
        Some(_) if output.contains("panicked at") => Outcome::Panicked(output),
        Some(status) => Outcome::Broken(format!("exited with {status}:\n{output}")),
    };
    (outcome, elapsed)
}

/// The repros of this binary (all ignored), as `<module>::<test>`.
fn ignored_tests() -> BTreeSet<String> {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .args(["--list", "--ignored", "--format", "terse"])
        .output()
        .expect("list the ignored tests");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .filter(|test| *test != "audit_repros_match_table")
        .map(str::to_string)
        .collect()
}

/// The panic line and the message after it, for a report.
fn panic_message(output: &str) -> String {
    let lines = output.lines().skip_while(|l| !l.contains("panicked at"));
    lines.take(3).collect::<Vec<_>>().join(" / ")
}

/// Whether `output` shows a panic at `at` (`file.rs:line`) with `text`.
fn panicked_as(output: &str, at: &str, text: &str) -> bool {
    let location = format!("{at}:");
    output
        .lines()
        .any(|l| l.contains("panicked at") && l.contains(&location))
        && output.contains(text)
}

/// Run a repro that is not in the table: it must pass.
fn judge_passing(test: &str) -> (Result<(), String>, Duration) {
    let (outcome, elapsed) = run_child(test, TIMEOUT);
    let verdict = match outcome {
        Outcome::Passed => Ok(()),
        Outcome::Panicked(output) => Err(format!("FAILS: {}", panic_message(&output))),
        Outcome::TimedOut => Err(format!("still running after {TIMEOUT:?}")),
        Outcome::Broken(why) => Err(why),
    };
    (verdict, elapsed)
}

/// Run `repro` and judge it against its entry.
fn judge(repro: &Repro) -> (Result<(), String>, Duration) {
    let timeout = TIMEOUT;
    let (outcome, elapsed) = run_child(repro.test, timeout);
    let verdict = match (&outcome, repro.fails) {
        (Outcome::Panicked(output), fails) if fails.shown_by(output) => Ok(()),
        (Outcome::Passed, _) if repro.intermittent => Ok(()),
        (Outcome::Passed, _) => Err("now PASSES: remove its entry".to_string()),
        (Outcome::Panicked(output), _) => {
            Err(format!("fails differently: {}", panic_message(output)))
        }
        (Outcome::TimedOut, _) => Err(format!(
            "fails differently: still running after {timeout:?}"
        )),
        (Outcome::Broken(why), _) => Err(why.clone()),
    };
    (verdict, elapsed)
}

#[test]
#[ignore = "audit repros are timing-sensitive races; run on demand (see the module docs)"]
fn audit_repros_match_table() {
    let report = std::env::var("AUDIT_REPROS_REPORT").is_ok();
    let jobs = std::env::var("AUDIT_REPROS_JOBS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(JOBS)
        .max(1);
    let mut problems = Vec::new();

    let tests = ignored_tests();
    let listed: BTreeSet<String> = EXPECTED.iter().map(|r| r.test.to_string()).collect();
    assert_eq!(listed.len(), EXPECTED.len(), "a repro is listed twice");
    for test in listed.difference(&tests) {
        problems.push(format!(
            "{test}: in EXPECTED but not a repro of this binary"
        ));
    }

    // Every repro, with its entry if it has one.
    let runs: Vec<(String, Option<&Repro>)> = tests
        .iter()
        .map(|test| (test.clone(), EXPECTED.iter().find(|r| r.test == test)))
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..jobs {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((test, entry)) = runs.get(i) else {
                        break;
                    };
                    let (verdict, elapsed) = match entry {
                        Some(repro) => judge(repro),
                        None => judge_passing(test),
                    };
                    results.lock().unwrap().push((i, verdict, elapsed));
                }
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|(i, _, _)| *i);
    for (i, verdict, elapsed) in results {
        let (test, entry) = &runs[i];
        let expected = match entry {
            Some(repro) => format!(
                "{}, {}{:?}",
                repro.audit,
                if repro.intermittent { "sometimes " } else { "" },
                repro.fails
            ),
            None => "passes".to_string(),
        };
        if report {
            println!(
                "{} {test} ({expected}) in {:.1}s",
                if verdict.is_ok() { "ok  " } else { "FAIL" },
                elapsed.as_secs_f64()
            );
        }
        if let Err(why) = verdict {
            problems.push(format!("{test} (expected: {expected}): {why}"));
        }
    }
    assert!(
        problems.is_empty(),
        "audit repros do not match EXPECTED:\n{}",
        problems.join("\n")
    );
}
