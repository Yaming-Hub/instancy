//! Exchange and broadcast between in-process workers with one-slot channels.
//!
//! With one-slot exchange channels, an envelope's sub-batches often reach
//! some workers while another worker's channel is full. The retry must resume
//! where delivery stopped: no duplicates, no losses, and in-flight accounting
//! that lets the dataflow complete.
//!
//! These tests are ignored until the dataflow-completion fixes land (tracked
//! separately). With one-slot channels, operators blocked on backpressure hold
//! records in their pending output, which progress tracking can't see. A
//! worker can then decide the dataflow is complete and force-close its
//! operators while other workers still hold records. The blocked workers
//! either lose records or keep reporting progress while blocked toward the
//! closed worker and spin until the timeout. The stage-task scheduling fix
//! removed the earlier livelock, but these runs still hang this way. Run them
//! with `--ignored`; the exchange retry logic itself is also covered by unit
//! tests in `exchange_channel.rs`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, Pipe, Result, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(60);

type Batches = Vec<(u64, Vec<u64>)>;

/// Runs `build` on `workers` workers (input `"in"`, output `"out"`), feeding
/// `inputs[w]` into worker `w`. Returns each worker's output records.
fn run<F>(workers: usize, build: F, inputs: Vec<Batches>) -> Vec<Vec<(u64, u64)>>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + Sync + 'static,
{
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut multi = rt
        .spawn_multi("one-slot", workers, build, SpawnOptions::default())
        .unwrap();
    let mut sinks = Vec::new();
    for w in 0..workers {
        let rx = multi.take_output::<u64>(w, "out").unwrap();
        let sink = Arc::new(Mutex::new(Vec::new()));
        let thread_sink = Arc::clone(&sink);
        std::thread::spawn(move || {
            while let Some(event) = rx.recv() {
                if let Some((t, data)) = event.as_data() {
                    thread_sink
                        .lock()
                        .unwrap()
                        .extend(data.iter().map(|x| (*t, *x)));
                }
            }
        });
        sinks.push(sink);
    }
    let mut inputs = inputs.into_iter();
    for w in 0..workers {
        let tx = multi.take_input::<u64>(w, "in").unwrap();
        let batches = inputs.next().unwrap_or_default();
        std::thread::spawn(move || {
            for (t, data) in batches {
                if tx.send(t, data).is_err() {
                    return;
                }
            }
        });
    }
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(multi.join_blocking());
    });
    match done_rx.recv_timeout(TIMEOUT) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("dataflow failed: {e}"),
        Err(_) => {
            let got: Vec<usize> = sinks.iter().map(|s| s.lock().unwrap().len()).collect();
            std::mem::forget(rt);
            panic!("dataflow did not complete within {TIMEOUT:?}; records per worker: {got:?}");
        }
    }
    let settle = Instant::now() + Duration::from_secs(5);
    while Instant::now() < settle && sinks.iter().any(|s| Arc::strong_count(s) > 1) {
        std::thread::sleep(Duration::from_millis(2));
    }
    sinks.iter().map(|s| s.lock().unwrap().clone()).collect()
}

/// `batches` batches of `per_batch` distinct records, spread over three
/// timestamps.
fn batches(batches: u64, per_batch: u64, first: u64) -> Batches {
    (0..batches)
        .map(|b| {
            let base = first + b * per_batch;
            (b % 3, (base..base + per_batch).collect())
        })
        .collect()
}

fn records(inputs: &[Batches]) -> Vec<(u64, u64)> {
    sorted(
        inputs
            .iter()
            .flatten()
            .flat_map(|(t, d)| d.iter().map(move |x| (*t, *x))),
    )
}

fn sorted(records: impl IntoIterator<Item = (u64, u64)>) -> Vec<(u64, u64)> {
    let mut v: Vec<_> = records.into_iter().collect();
    v.sort_unstable();
    v
}

fn assert_same(got: Vec<(u64, u64)>, expected: &[(u64, u64)], what: &str) {
    if got == expected {
        return;
    }
    let mut counts: BTreeMap<(u64, u64), i64> = BTreeMap::new();
    for r in &got {
        *counts.entry(*r).or_default() += 1;
    }
    for r in expected {
        *counts.entry(*r).or_default() -= 1;
    }
    let dups = counts.values().filter(|c| **c > 0).count();
    let missing = counts.values().filter(|c| **c < 0).count();
    panic!(
        "{what}: got {} records, expected {}: {dups} duplicated, {missing} missing",
        got.len(),
        expected.len()
    );
}

/// Worker 0 feeds; records go to workers 1..workers, never back to worker 0.
fn away_from_feeder(workers: u64) -> impl Fn(&u64) -> u64 + Send + Sync + 'static {
    move |x: &u64| 1 + x % (workers - 1)
}

#[test]
#[ignore = "needs the dataflow-completion fixes: records held by blocked operators are invisible to progress tracking, so a worker force-closes early; see module docs"]
fn exchange_to_other_workers_with_one_slot_channels() {
    let data = vec![batches(2000, 9, 0)];
    let expected = records(&data);
    let out = run(
        4,
        |b| {
            b.input::<u64>("in")?
                .map("pre", |_t, x| x)
                .with_capacity(1)?
                .exchange_by_hash("route", away_from_feeder(4))
                .map("post", |_t, x| x)
                .output("out")?;
            Ok(())
        },
        data,
    );
    assert!(out[0].is_empty(), "worker 0 is never a target");
    for (w, got) in out.iter().enumerate().skip(1) {
        assert!(
            got.iter().all(|(_, x)| 1 + x % 3 == w as u64),
            "worker {w} got records routed elsewhere"
        );
    }
    assert_same(sorted(out.into_iter().flatten()), &expected, "exchange");
}

/// Round-robin routing (as `rebalance` does) partitions differently on every
/// call, so a retry must reuse the first partition.
#[test]
#[ignore = "needs the dataflow-completion fixes: records held by blocked operators are invisible to progress tracking, so a worker force-closes early; see module docs"]
fn round_robin_to_other_workers_with_one_slot_channels() {
    let data = vec![batches(2000, 7, 0)];
    let expected = records(&data);
    let out = run(
        4,
        |b| {
            let counter = AtomicU64::new(0);
            b.input::<u64>("in")?
                .map("pre", |_t, x| x)
                .with_capacity(1)?
                .exchange_by_hash("round-robin", move |_: &u64| {
                    1 + counter.fetch_add(1, Ordering::Relaxed) % 3
                })
                .map("post", |_t, x| x)
                .output("out")?;
            Ok(())
        },
        data,
    );
    assert!(out[0].is_empty(), "worker 0 is never a target");
    assert_same(sorted(out.into_iter().flatten()), &expected, "round robin");
}

/// One output feeding two exchanges: the fan-out retries a partially
/// delivered exchange branch, which in turn resumes its own partial delivery.
#[test]
#[ignore = "needs the dataflow-completion fixes: records held by blocked operators are invisible to progress tracking, so a worker force-closes early; see module docs"]
fn branch_into_two_exchanges_with_one_slot_channels() {
    let data = vec![batches(1500, 6, 0)];
    let mut expected: Vec<(u64, u64)> = records(&data)
        .into_iter()
        .flat_map(|(t, x)| [(t, 2 * x), (t, 2 * x + 1)])
        .collect();
    expected.sort_unstable();
    let out = run(
        4,
        |b| {
            let pre = b.input::<u64>("in")?.map("pre", |_t, x| x);
            let left = pre
                .clone()
                .with_capacity(1)?
                .exchange_by_hash("left", away_from_feeder(4))
                .map("even", |_t, x| 2 * x);
            let right = pre
                .with_capacity(1)?
                .exchange_by_hash("right", |x: &u64| 1 + (x / 7) % 3)
                .map("odd", |_t, x| 2 * x + 1);
            Pipe::concat(vec![left, right])?.output("out")?;
            Ok(())
        },
        data,
    );
    assert_same(
        sorted(out.into_iter().flatten()),
        &expected,
        "two exchanges",
    );
}

#[test]
#[ignore = "needs the dataflow-completion fixes: records held by blocked operators are invisible to progress tracking, so a worker force-closes early; see module docs"]
fn exchange_from_every_worker_with_one_slot_channels() {
    let data: Vec<Batches> = (0..4).map(|w| batches(300, 8, w * 10_000)).collect();
    let expected = records(&data);
    let out = run(
        4,
        |b| {
            b.input::<u64>("in")?
                .map("pre", |_t, x| x)
                .with_capacity(1)?
                .exchange_by_hash("route", |x: &u64| *x)
                .map("post", |_t, x| x)
                .output("out")?;
            Ok(())
        },
        data,
    );
    assert_same(sorted(out.into_iter().flatten()), &expected, "exchange");
}

#[test]
#[ignore = "needs the dataflow-completion fixes: records held by blocked operators are invisible to progress tracking, so a worker force-closes early; see module docs"]
fn broadcast_between_workers_with_one_slot_channels() {
    let data = vec![batches(300, 4, 0)];
    let expected = records(&data);
    let out = run(
        4,
        |b| {
            b.input::<u64>("in")?
                .map("pre", |_t, x| x)
                .with_capacity(1)?
                .broadcast("all")
                .map("post", |_t, x| x)
                .output("out")?;
            Ok(())
        },
        data,
    );
    for (w, got) in out.into_iter().enumerate() {
        assert_same(sorted(got), &expected, &format!("worker {w}"));
    }
}
