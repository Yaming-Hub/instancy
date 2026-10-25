//! Many distinct epochs outstanding at once.
//!
//! A source emits 65,536 epochs in one call into a bounded channel, so the
//! progress tracker holds a count at tens of thousands of distinct times at
//! one location, and `count` holds a notification capability at each. Every
//! sweep's progress work must stay proportional to what changed (or, where a
//! frontier is rebuilt, to the number of outstanding times), never quadratic
//! in it: the run must complete promptly, with an exact count per epoch.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

const EPOCHS: u64 = 65_536;
/// The two-worker variant's epochs. The progress checker, on in debug
/// builds, observes every record and capability of both workers, which
/// costs several times the run itself; without it this variant completes
/// 65,536 epochs about as fast as the one-worker test.
const EXCHANGE_EPOCHS: u64 = 16_384;
const TIMEOUT: Duration = Duration::from_secs(60);

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
