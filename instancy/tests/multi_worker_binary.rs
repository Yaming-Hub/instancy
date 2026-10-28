//! Two-input operators on several workers.
//!
//! Each input of a `binary` operator can be exchanged on its own. Records
//! with the same key then meet at the same worker's operator, whichever
//! worker sent them and whichever input they came in on.

use std::cell::OnceCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, OutputReceiver, RuntimeConfig, RuntimeHandle, SpawnOptions};

const WORKERS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(10);

/// A `(key, value)` record on one of the two inputs.
type Keyed = (u64, u64);
/// A pair that met: `(key, left value, right value)`.
type Met = (u64, u64, u64);
/// One worker's records in an epoch: `(worker, left records, right records)`.
type Sends = (usize, Vec<Keyed>, Vec<Keyed>);

/// Append the records `output` has delivered so far to `received`, each with
/// its time.
fn drain(output: &OutputReceiver<u64, Met>, received: &mut Vec<(u64, Met)>) {
    while let Some(event) = output.try_recv() {
        if let OutputEvent::Data { time, data } = event {
            received.extend(data.into_iter().map(|x| (time, x)));
        }
    }
}

/// Both inputs are exchanged by key into a `binary` operator on three
/// workers. The operator keeps every record it has seen from each side and,
/// for each new record, gives `(key, left, right)` for each record with the
/// same key already seen on the other side, at the new record's time. Each
/// pair can only form if both records reached the same worker.
///
/// Over three epochs, records of each key are sent from workers other than
/// the one the key routes to, on both inputs, in different epochs. After
/// each epoch every input advances; the probe after the operator then
/// reaches exactly the next epoch, and each worker's output is exactly the
/// pairs of its keys formed so far, at the time of the later record of each
/// pair. Key 4 never meets anything. After both inputs close, the dataflow
/// completes with nothing more.
#[test]
fn binary_inputs_exchanged_by_key_meet_at_the_keys_worker() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .expect("runtime");
    let probe = OnceCell::new();
    let mut dataflow = rt
        .spawn_multi(
            "keyed-meeting",
            WORKERS,
            |b: &mut DataflowBuilder<u64>| {
                let left = b
                    .input::<Keyed>("left")?
                    .exchange_by_hash("left-by-key", |(key, _): &Keyed| *key);
                let right = b
                    .input::<Keyed>("right")?
                    .exchange_by_hash("right-by-key", |(key, _): &Keyed| *key);
                let (met, handle) = left
                    .binary(right, "meet", {
                        let mut lefts: HashMap<u64, Vec<u64>> = HashMap::new();
                        let mut rights: HashMap<u64, Vec<u64>> = HashMap::new();
                        move |input1, input2, output| {
                            while let Some((time, data)) = input1.next() {
                                let mut pairs = Vec::new();
                                for (key, l) in data {
                                    for r in rights.get(&key).into_iter().flatten() {
                                        pairs.push((key, l, *r));
                                    }
                                    lefts.entry(key).or_default().push(l);
                                }
                                if !pairs.is_empty() {
                                    output.push_vec(time, pairs);
                                }
                            }
                            while let Some((time, data)) = input2.next() {
                                let mut pairs = Vec::new();
                                for (key, r) in data {
                                    for l in lefts.get(&key).into_iter().flatten() {
                                        pairs.push((key, *l, r));
                                    }
                                    rights.entry(key).or_default().push(r);
                                }
                                if !pairs.is_empty() {
                                    output.push_vec(time, pairs);
                                }
                            }
                            Ok(())
                        }
                    })?
                    .probe();
                probe.set(handle).expect("the dataflow is built once");
                met.output("out")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(false),
        )
        .expect("spawn");
    let probe = probe.into_inner().expect("probe");
    let lefts = dataflow.take_all_inputs::<Keyed>("left").expect("left");
    let rights = dataflow.take_all_inputs::<Keyed>("right").expect("right");
    let outputs = dataflow.take_all_outputs::<Met>("out").expect("outputs");

    // Per epoch: (sending worker, left records, right records). Keys route to
    // worker `key % 3`: key 1 to worker 1, key 2 to worker 2, key 3 to worker
    // 0, and key 4 to worker 1. No record is sent by the worker its key
    // routes to.
    let epochs: [Vec<Sends>; 3] = [
        vec![
            (0, vec![(1, 10), (2, 20)], vec![]),
            (2, vec![], vec![(1, 100)]),
        ],
        vec![
            (1, vec![], vec![(2, 200)]),
            (2, vec![(3, 30)], vec![(1, 101)]),
        ],
        vec![
            (0, vec![(1, 11)], vec![]),
            (1, vec![], vec![(3, 300)]),
            (2, vec![(4, 40)], vec![]),
        ],
    ];
    // Each worker's pairs, at the time of the later record of each pair.
    let mut expected: Vec<Vec<(u64, Met)>> = vec![Vec::new(); WORKERS];
    expected[1].extend([
        (0, (1, 10, 100)),
        (1, (1, 10, 101)),
        (2, (1, 11, 100)),
        (2, (1, 11, 101)),
    ]);
    expected[2].push((1, (2, 20, 200)));
    expected[0].push((2, (3, 30, 300)));

    let mut received: Vec<Vec<(u64, Met)>> = vec![Vec::new(); WORKERS];
    for (epoch, sends) in epochs.into_iter().enumerate() {
        let epoch = epoch as u64;
        for (worker, left, right) in sends {
            if !left.is_empty() {
                lefts[worker].send(epoch, left).expect("send left");
            }
            if !right.is_empty() {
                rights[worker].send(epoch, right).expect("send right");
            }
        }
        for input in lefts.iter().chain(&rights) {
            input.advance_to(epoch + 1).expect("advance");
        }
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let frontier = probe.frontier();
            assert!(
                !frontier.is_empty() && frontier.elements().iter().all(|t| *t <= epoch + 1),
                "probe at {frontier:?} while the inputs are open at {}",
                epoch + 1
            );
            if frontier.elements() == [epoch + 1] {
                break;
            }
            assert!(Instant::now() < deadline, "probe stuck at {frontier:?}");
            std::thread::sleep(Duration::from_millis(1));
        }
        for (output, received) in outputs.iter().zip(&mut received) {
            drain(output, received);
        }
        for (worker, (got, want)) in received.iter().zip(&expected).enumerate() {
            let mut got = got.clone();
            got.sort_unstable();
            let mut want: Vec<(u64, Met)> =
                want.iter().filter(|(t, _)| *t <= epoch).copied().collect();
            want.sort_unstable();
            assert_eq!(
                got, want,
                "worker {worker}'s pairs once epoch {epoch} completed"
            );
        }
    }

    drop(lefts);
    drop(rights);
    let join = std::thread::spawn(move || dataflow.join_blocking());
    let deadline = Instant::now() + TIMEOUT;
    while !(probe.is_done() && join.is_finished()) {
        assert!(
            Instant::now() < deadline,
            "unfinished, probe at {:?}",
            probe.frontier()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    join.join().expect("join panicked").expect("dataflow");
    for (output, received) in outputs.iter().zip(&mut received) {
        drain(output, received);
    }
    for (worker, (got, want)) in received.into_iter().zip(expected).enumerate() {
        let mut got = got;
        got.sort_unstable();
        let mut want = want;
        want.sort_unstable();
        assert_eq!(got, want, "worker {worker}'s pairs");
    }
}
