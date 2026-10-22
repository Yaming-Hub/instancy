//! Audit: frontier downstream of `iterate` vs data still circulating in the loop.
//!
//! The loop's feedback edge is deliberately NOT added to the reachability graph
//! (dataflow_builder.rs "We do NOT add the feedback edge to subgraph_builder"),
//! and loop-body operators are registered with identity `T` summaries. Nothing
//! in the tracker therefore represents records sitting in the feedback channel.
//! Once the input capability for epoch `e` is released, the frontier after
//! `leave` may pass `e` while records at epoch `e` are still iterating.

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, ProbeHandle, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ITEMS: u64 = 40;

fn build(
    builder: &DataflowBuilder<u64>,
    slow: bool,
    arrivals: Arc<Mutex<Vec<(Instant, u64, usize)>>>,
) -> ProbeHandle<u64> {
    let input = builder.input::<u64>("data").unwrap();
    let looped = input.iterate::<u32>("loop", 1u32, move |v| {
        let stepped = v.map("inc", move |_t: &Product<u64, u32>, x: u64| {
            if slow {
                std::thread::sleep(Duration::from_millis(20));
            }
            x + 1
        });
        let out = stepped.clone().filter("done", |_t, x| *x % 20 == 0);
        let fb = stepped.filter("again", |_t, x| *x % 20 != 0);
        IterateResult {
            feedback: fb,
            output: out,
        }
    });
    let looped = looped.map("arrive", move |t: &u64, x: u64| {
        arrivals.lock().unwrap().push((Instant::now(), *t, 1));
        x
    });
    let (looped, probe) = looped.probe();
    looped
        .unary_notify("count", {
            let mut stash: HashMap<u64, u64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    *stash.entry(time).or_default() += data.len() as u64;
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    let n = stash.remove(&time).unwrap_or(0);
                    output.push_vec(time, vec![(time, n)]);
                }
                Ok(())
            }
        })
        .output("out")
        .unwrap();
    probe
}

/// Records leave after (20 - x % 20) iterations, i.e. between 1 and 19 rounds.
/// The per-epoch count after `leave` must be emitted exactly once with all ITEMS.
fn run(slow: bool) -> (Vec<(u64, u64)>, Vec<String>) {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("audit-loop-frontier");
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let probe = build(&builder, slow, arrivals.clone());
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let sender = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<(u64, u64)>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());

    let observations = Arc::new(Mutex::new(Vec::new()));
    sender.send(0, (1..=ITEMS).collect()).unwrap();
    sender.advance_to(1).unwrap();
    // While the input is still open, watch the probe after `leave`.
    let deadline = Instant::now() + Duration::from_secs(if slow { 10 } else { 3 });
    let mut first_done = None;
    while Instant::now() < deadline {
        if probe.done_with(&0) && first_done.is_none() {
            first_done = Some(Instant::now());
            observations.lock().unwrap().push(format!(
                "probe after leave passed epoch 0 at {:?}",
                first_done
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(sender);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(h.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(30))
        .expect("join hang")
        .expect("join error");
    let mut recs: Vec<(u64, u64)> = collector
        .join()
        .unwrap()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    recs.sort();
    let mut obs = observations.lock().unwrap().clone();
    let a = arrivals.lock().unwrap();
    obs.push(format!(
        "arrived-before-close={} probe_passed_while_open={}",
        a.iter().filter(|(when, _, _)| *when < deadline).count(),
        first_done.is_some()
    ));
    let late: Vec<_> = a
        .iter()
        .filter(|(when, t, _)| *t == 0 && first_done.is_some_and(|fd| *when > fd))
        .map(|(when, _, _)| {
            format!(
                "{:?} after probe passed 0",
                when.duration_since(first_done.unwrap())
            )
        })
        .collect();
    obs.push(format!(
        "records at epoch 0 arriving after probe passed 0: {} {:?}",
        late.len(),
        &late[..late.len().min(5)]
    ));
    if !late.is_empty() {
        obs.push("VIOLATION".into());
    }
    (recs, obs)
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn loop_downstream_notification_fires_once_with_full_count() {
    let (recs, obs) = run(false);
    eprintln!("records: {recs:?}\nobs: {obs:?}");
    assert_eq!(
        recs,
        vec![(0, ITEMS)],
        "epoch 0 aggregate after iterate was split/emitted early"
    );
}

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn loop_downstream_notification_fires_once_with_full_count_slow_body() {
    let (recs, obs) = run(true);
    eprintln!("records: {recs:?}\nobs: {obs:?}");
    assert_eq!(
        recs,
        vec![(0, ITEMS)],
        "epoch 0 aggregate after iterate was split/emitted early"
    );
}
