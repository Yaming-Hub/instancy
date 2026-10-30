//! Many distinct epochs outstanding at once.
//!
//! A source emits 65,536 epochs in one call into a bounded channel, so the
//! progress tracker holds a count at tens of thousands of distinct times at
//! one location, and `count` holds a notification capability at each. Every
//! sweep's progress work must stay proportional to what changed (or, where a
//! frontier is rebuilt, to the number of outstanding times), never quadratic
//! in it: the run must complete promptly, with an exact count per epoch.
//!
//! The last tests are the base cases of the epochs and sources these runs
//! rely on: on three workers, every worker runs a preloaded source itself,
//! and epochs fed from outside without waiting each reach their worker.

use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

const EPOCHS: u64 = 65_536;
/// The two-worker variant's epochs. The progress checker, on in debug
/// builds, observes every record and capability of both workers, which
/// costs several times the run itself; without it this variant completes
/// 65,536 epochs about as fast as the one-worker test.
const EXCHANGE_EPOCHS: u64 = 16_384;
const TIMEOUT: Duration = Duration::from_secs(60);

/// Each operator instance's (time, record) observations.
type PerInstance = Arc<Mutex<Vec<Vec<(u64, u64)>>>>;

fn with_timeout<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(TIMEOUT)
        .unwrap_or_else(|_| panic!("{what}: did not finish within {TIMEOUT:?}"))
}

/// Every epoch's count, sorted by epoch.
fn sorted(out: Vec<(u64, Vec<u64>)>) -> Vec<(u64, u64)> {
    let mut counts: Vec<(u64, u64)> = out
        .into_iter()
        .flat_map(|(t, data)| data.into_iter().map(move |n| (t, n)))
        .collect();
    counts.sort_unstable();
    counts
}

#[test]
fn many_epochs_outstanding_at_once_complete_promptly() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("many-epochs");
    let data: Vec<(u64, Vec<u64>)> = (0..EPOCHS).map(|e| (e, vec![e])).collect();
    builder
        .source("epochs", data)
        .map("inc", |_t, x| x + 1)
        .count("count")
        .output("out")
        .unwrap();
    let mut spawned = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let receiver = spawned.take_output::<usize>("out").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    let started = Instant::now();
    with_timeout("many epochs", move || spawned.join_blocking()).unwrap();
    eprintln!("{EPOCHS} epochs on one worker: {:?}", started.elapsed());
    let counts: Vec<(u64, u64)> = sorted(
        collector
            .join()
            .unwrap()
            .into_iter()
            .map(|(t, data)| (t, data.into_iter().map(|n| n as u64).collect()))
            .collect(),
    );
    let expected: Vec<(u64, u64)> = (0..EPOCHS).map(|e| (e, 1)).collect();
    assert!(counts == expected, "not one count of 1 per epoch");
}

/// The same on two workers, through an exchange: each worker's view holds
/// both workers' counts at every epoch, and the exchange carries them all.
#[test]
fn many_epochs_through_an_exchange_on_two_workers_complete_promptly() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "many-epochs-exchange",
            2,
            |b: &mut DataflowBuilder<u64>| {
                let data: Vec<(u64, Vec<u64>)> =
                    (0..EXCHANGE_EPOCHS).map(|e| (e, vec![e])).collect();
                b.source("epochs", data)
                    .map("inc", |_t, x| x + 1)
                    .exchange_by_hash("spread", |x: &u64| *x)
                    .count("count")
                    .map("as-u64", |_t, n| n as u64)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(false),
        )
        .unwrap();
    let receivers = multi.take_all_outputs::<u64>("out").unwrap();
    let collectors: Vec<_> = receivers
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    let started = Instant::now();
    with_timeout("many epochs, two workers", move || multi.join_blocking()).unwrap();
    eprintln!(
        "{EXCHANGE_EPOCHS} epochs on two workers: {:?}",
        started.elapsed()
    );
    let counts = sorted(
        collectors
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .collect(),
    );
    // Both workers' sources emit every epoch; each epoch's two records land
    // on one worker (same value), counted once there.
    let expected: Vec<(u64, u64)> = (0..EXCHANGE_EPOCHS).map(|e| (e, 2)).collect();
    assert!(counts == expected, "not one count of 2 per epoch");
}

/// With three workers, `spawn_multi` replicates the preloaded source, so each
/// worker runs its own source and inspect: each worker's inspect sees `0..10`
/// once, and the outputs together hold every value three times, all at
/// epoch 0.
#[test]
fn three_workers_each_run_the_source_and_inspect_every_record() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    // One list of observations per inspect instance (one per worker), in the
    // order the instances first ran.
    let seen: PerInstance = Arc::default();
    let seen_by_inspect = Arc::clone(&seen);
    let mut multi = rt
        .spawn_multi(
            "three-sources",
            3,
            move |b: &mut DataflowBuilder<u64>| {
                let seen = Arc::clone(&seen_by_inspect);
                let mut instance = None;
                b.source("numbers", vec![(0u64, (0..10).collect::<Vec<u64>>())])
                    .inspect("inspect", move |t, x| {
                        let mut observed = seen.lock().unwrap();
                        let i = *instance.get_or_insert_with(|| {
                            observed.push(Vec::new());
                            observed.len() - 1
                        });
                        observed[i].push((*t, *x));
                    })
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let receivers = multi.take_all_outputs::<u64>("out").unwrap();
    assert_eq!(receivers.len(), 3);
    let collectors: Vec<_> = receivers
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    let (joined, collected) = with_timeout("three sources", move || {
        let joined = multi.join_blocking();
        let collected: Vec<_> = collectors.into_iter().map(|c| c.join()).collect();
        (joined, collected)
    });
    joined.unwrap();

    let once: Vec<(u64, u64)> = (0..10).map(|x| (0, x)).collect();
    let outputs: Vec<Vec<(u64, u64)>> = collected.into_iter().map(|c| sorted(c.unwrap())).collect();
    for (worker, output) in outputs.iter().enumerate() {
        assert_eq!(output, &once, "worker {worker}'s output");
    }
    let mut all: Vec<(u64, u64)> = outputs.into_iter().flatten().collect();
    all.sort_unstable();
    assert_eq!(all, (0..30).map(|x| (0, x / 3)).collect::<Vec<_>>());

    let mut seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "one inspect per worker: {seen:?}");
    for (instance, observed) in seen.iter_mut().enumerate() {
        observed.sort_unstable();
        assert_eq!(observed, &once, "inspect {instance}");
    }
}

/// Epochs fed from outside, without waiting between them: on three workers,
/// worker 0 sends `r` at time `r` and every input advances to `r + 1`, for
/// ten rounds, with no wait for any probe; then the inputs close. Each record
/// reaches the worker the exchange routes it to, exactly once, at its time.
#[test]
fn input_advanced_every_epoch_without_waiting_routes_each_record_through_an_exchange() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "epochs-without-waiting",
            3,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .exchange_by_hash("route", |x: &u64| *x)
                    .output("out")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let inputs = multi.take_all_inputs::<u64>("in").unwrap();
    let collectors: Vec<_> = multi
        .take_all_outputs::<u64>("out")
        .unwrap()
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    for round in 0..10u64 {
        inputs[0].send(round, vec![round]).unwrap();
        for input in &inputs {
            input.advance_to(round + 1).unwrap();
        }
    }
    drop(inputs);
    let (joined, collected) = with_timeout("epochs without waiting", move || {
        let joined = multi.join_blocking();
        let collected: Vec<_> = collectors.into_iter().map(|c| c.join()).collect();
        (joined, collected)
    });
    joined.unwrap();
    for (worker, output) in collected.into_iter().enumerate() {
        let want: Vec<(u64, u64)> = (0..10u64)
            .filter(|round| round % 3 == worker as u64)
            .map(|round| (round, round))
            .collect();
        assert_eq!(sorted(output.unwrap()), want, "worker {worker}'s output");
    }
}
