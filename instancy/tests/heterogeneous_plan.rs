//! Stages with different parallelism on the one executor core: the physical
//! plan's hosting rules seen through the public API.
//!
//! - a worker-local edge into fewer workers is rejected at spawn, naming the
//!   edge;
//! - a probe on an operator that runs on one of two workers completes once,
//!   after the inputs close, and not before;
//! - a worker that runs only upstream operators may finish first without
//!   holding up or losing anything downstream.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::{DataflowBuilder, ProbeHandle, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(20);

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn per_stage() -> SpawnOptions {
    SpawnOptions::new()
        .per_stage_parallelism(true)
        .auto_parallelism(false)
}

fn join_within(multi: instancy::MultiSpawnedDataflow<u64>) -> instancy::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    rx.recv_timeout(TIMEOUT)
        .expect("dataflow did not complete (hang)")
}

/// A pipeline edge from a stage on 3 workers into one on 2: worker 2's
/// records would have nowhere to go. The spawn fails and names the edge.
#[test]
fn worker_local_edge_into_fewer_workers_fails_to_spawn() {
    let rt = runtime();
    let result = rt.spawn_multi(
        "narrowing-pipeline",
        1,
        |b: &mut DataflowBuilder<u64>| {
            let input = b.input::<u64>("in")?;
            let wide = input
                .clone()
                .exchange_to("wide", 3, |x: &u64| *x)?
                .map("wide-map", |_t, x| x);
            let narrow = input.exchange_to("narrow", 2, |x: &u64| *x)?;
            narrow.merge(wide)?.output("out")?;
            Ok(())
        },
        per_stage(),
    );
    let err = match result {
        Ok(_) => panic!("a pipeline edge from 3 workers into 2 spawned"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("pipeline edge from 'wide-map'") && err.contains("parallelism 3"),
        "{err}"
    );
}

/// The reverse direction (2 workers into 3) spawns: worker 2 runs only the
/// merge, whose input from the 2-worker stage is already exhausted there,
/// and every record arrives.
#[test]
fn worker_local_edge_into_more_workers_delivers_everything() {
    let rt = runtime();
    let mut multi = rt
        .spawn_multi(
            "widening-pipeline",
            1,
            |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("in")?;
                let narrow = input
                    .clone()
                    .exchange_to("narrow", 2, |x: &u64| *x)?
                    .map("narrow-map", |_t, x| x);
                let wide = input.exchange_to("wide", 3, |x: &u64| *x)?;
                wide.merge(narrow)?.output("out")?;
                Ok(())
            },
            per_stage(),
        )
        .unwrap();
    assert_eq!(multi.num_workers(), 3);
    let collectors: Vec<_> = multi
        .take_all_outputs::<u64>("out")
        .unwrap()
        .into_iter()
        .map(|r| std::thread::spawn(move || r.collect_data()))
        .collect();
    let senders = multi.take_all_inputs::<u64>("in").unwrap();
    senders[0].send(0, (0..10).collect()).unwrap();
    drop(senders);
    join_within(multi).unwrap();
    let mut got: Vec<u64> = collectors
        .into_iter()
        .flat_map(|c| c.join().unwrap())
        .flat_map(|(_, d)| d)
        .collect();
    got.sort();
    let mut expected: Vec<u64> = (0..10).chain(0..10).collect();
    expected.sort();
    assert_eq!(got, expected);
}

/// Samples a probe's frontier until stopped, recording every distinct value
/// and when it was first seen.
struct Watcher {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handle: std::thread::JoinHandle<Vec<(Instant, Vec<u64>)>>,
}

impl Watcher {
    fn start(probe: ProbeHandle<u64>) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut seen: Vec<(Instant, Vec<u64>)> = Vec::new();
            loop {
                let frontier = probe.frontier().elements().to_vec();
                if seen.last().map(|(_, f)| f) != Some(&frontier) {
                    seen.push((Instant::now(), frontier));
                }
                if s.load(std::sync::atomic::Ordering::SeqCst) {
                    return seen;
                }
                std::thread::sleep(Duration::from_micros(200));
            }
        });
        Self { stop, handle }
    }

    fn finish(self) -> Vec<(Instant, Vec<u64>)> {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.handle.join().unwrap()
    }
}

/// A probe on an operator that only worker 0 of 2 runs (2 → 1) has one
/// writer. It holds while the inputs are open, passes an epoch only once
/// both inputs advanced past it, and becomes done exactly once, after the
/// inputs close.
#[test]
fn probe_on_operator_run_by_one_of_two_workers_completes_once() {
    for round in 0..10 {
        let rt = runtime();
        let probes: Arc<Mutex<Vec<ProbeHandle<u64>>>> = Arc::default();
        let p = Arc::clone(&probes);
        let mut multi = rt
            .spawn_multi(
                "probe-2-to-1",
                2,
                move |b: &mut DataflowBuilder<u64>| {
                    let (downstream, probe) = b
                        .input::<u64>("in")?
                        .map("inc", |_t, x| x + 1)
                        .exchange_to("to-one", 1, |x: &u64| *x)?
                        .map("downstream", |_t, x| x)
                        .probe();
                    p.lock().unwrap().push(probe);
                    downstream.output("out")?;
                    Ok(())
                },
                per_stage(),
            )
            .unwrap();
        assert_eq!(multi.num_workers(), 2);
        let probe = {
            let probes = probes.lock().unwrap();
            assert_eq!(probes.len(), 1, "the dataflow is built once");
            probes[0].clone()
        };
        let watcher = Watcher::start(probe.clone());
        let collectors: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|r| std::thread::spawn(move || r.collect_data()))
            .collect();
        let senders = multi.take_all_inputs::<u64>("in").unwrap();
        assert_eq!(senders.len(), 2);
        senders[0].send(0, vec![1, 2, 3]).unwrap();
        senders[1].send(0, vec![4, 5]).unwrap();

        // Only one input advanced: epoch 0 is still open on the other.
        senders[0].advance_to(1).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !probe.done_with(&0),
            "round {round}: epoch 0 complete while worker 1's input is open at 0"
        );

        // Both advanced: epoch 0 completes, the probe is not done.
        senders[1].advance_to(1).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        while !probe.done_with(&0) {
            assert!(
                Instant::now() < deadline,
                "round {round}: epoch 0 never completed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !probe.is_done(),
            "round {round}: done while inputs are open"
        );

        let closed = Instant::now();
        drop(senders);
        join_within(multi).unwrap();
        let mut got: Vec<u64> = collectors
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .flat_map(|(_, d)| d)
            .collect();
        got.sort();
        assert_eq!(got, vec![2, 3, 4, 5, 6], "round {round}");
        assert!(
            probe.is_done(),
            "round {round}: probe at {:?}",
            probe.frontier()
        );

        let seen = watcher.finish();
        let first_done = seen
            .iter()
            .position(|(_, f)| f.is_empty())
            .expect("the probe became done");
        assert!(
            seen[first_done].0 >= closed,
            "round {round}: done before the inputs closed: {seen:?}"
        );
        assert_eq!(
            first_done,
            seen.len() - 1,
            "round {round}: the probe left done: {seen:?}"
        );
    }
}

/// T5: a probe on an operator that runs on 1 of 2 workers expects 1 writer,
/// not 2. Worker 0 runs the probed operator; worker 1 runs only the input
/// stage and lingers (30 ms) in its last sweep after closing its exchange
/// output, before it propagates its final capability release. Worker 0 can
/// then finish with a view that still holds worker 1's input capability. A
/// probe that expected 2 writers would keep that stale view forever; one that
/// expects 1 drops it once its only writer has finished.
#[test]
fn probe_expects_one_writer_per_worker_running_its_operator() {
    for round in 0..10 {
        let rt = runtime();
        let probes: Arc<Mutex<Vec<ProbeHandle<u64>>>> = Arc::default();
        let p = Arc::clone(&probes);
        let mut multi = rt
            .spawn_multi(
                "probe-writers",
                2,
                move |b: &mut DataflowBuilder<u64>| {
                    let input = b.input::<u64>("in")?;
                    let (downstream, probe) = input
                        .clone()
                        .map("inc", |_t, x| x + 1)
                        .exchange_to("to-one", 1, |x: &u64| *x)?
                        .map("downstream", |_t, x| x)
                        .probe();
                    p.lock().unwrap().push(probe);
                    downstream.output("out")?;
                    input
                        .unary::<u64, _>("linger", |input, _output| {
                            let mut seen = false;
                            while input.next().is_some() {
                                seen = true;
                            }
                            if seen {
                                std::thread::sleep(Duration::from_millis(30));
                            }
                            Ok(())
                        })
                        .for_each("drop", |_t, _x| {});
                    Ok(())
                },
                per_stage(),
            )
            .unwrap();
        let probe = probes.lock().unwrap()[0].clone();
        let collectors: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|r| std::thread::spawn(move || r.collect_data()))
            .collect();
        let mut senders = multi.take_all_inputs::<u64>("in").unwrap().into_iter();
        // Worker 0's input closes at once; worker 1's, later, right after its
        // last batch (so its last sweep both closes and lingers).
        drop(senders.next());
        let second = senders.next().unwrap();
        second.send(0, vec![1, 2, 3]).unwrap();
        second.advance_to(1).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        second.send(1, vec![4, 5]).unwrap();
        drop(second);
        join_within(multi).unwrap();
        let delivered: usize = collectors
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .map(|(_, d)| d.len())
            .sum();
        assert_eq!(delivered, 5, "round {round}");
        assert!(
            probe.is_done(),
            "round {round}: probe stuck at {:?} after a successful run",
            probe.frontier()
        );
    }
}

/// 2 → 1: worker 1 runs only the input stage, so it finishes as soon as its
/// input has closed and its records are on their way to worker 0. Worker 0
/// must still receive every record, and the dataflow must complete.
#[test]
fn worker_running_only_upstream_finishes_first() {
    for round in 0..20 {
        let rt = runtime();
        let mut multi = rt
            .spawn_multi(
                "upstream-only-worker",
                2,
                |b: &mut DataflowBuilder<u64>| {
                    b.input::<u64>("in")?
                        .map("inc", |_t, x| x + 1)
                        .exchange_to("to-one", 1, |x: &u64| *x)?
                        .output("out")?;
                    Ok(())
                },
                per_stage(),
            )
            .unwrap();
        let collectors: Vec<_> = multi
            .take_all_outputs::<u64>("out")
            .unwrap()
            .into_iter()
            .map(|r| std::thread::spawn(move || r.collect_data()))
            .collect();
        let senders = multi.take_all_inputs::<u64>("in").unwrap();
        for epoch in 0..3u64 {
            senders[0].send(epoch, (0..100).collect()).unwrap();
            senders[1].send(epoch, (100..150).collect()).unwrap();
            for s in &senders {
                s.advance_to(epoch + 1).unwrap();
            }
        }
        // Worker 1's input closes first; worker 0's a little later.
        let mut senders = senders.into_iter();
        let first = senders.next().unwrap();
        drop(senders);
        std::thread::sleep(Duration::from_millis(5));
        first.send(3, vec![7]).unwrap();
        drop(first);
        join_within(multi).unwrap_or_else(|e| panic!("round {round}: {e}"));
        let delivered: usize = collectors
            .into_iter()
            .flat_map(|c| c.join().unwrap())
            .map(|(_, d)| d.len())
            .sum();
        assert_eq!(delivered, 3 * 150 + 1, "round {round}");
    }
}
