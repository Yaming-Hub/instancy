use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::progress::frontier::Antichain;
use instancy::{
    DataflowBuilder, IterateResult, OutputReceiver, Product, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

/// Each time an operator gave output at, with its input frontier then,
/// recorded by the operator itself as it gives.
type Given = Arc<Mutex<Vec<(u64, Antichain<u64>)>>>;

fn recv_next_data<T, D>(receiver: &OutputReceiver<T, D>, timeout: Duration) -> Option<(T, Vec<D>)>
where
    T: instancy::progress::timestamp::Timestamp,
    D: Send + 'static,
{
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        match receiver.recv_timeout(remaining) {
            Some(OutputEvent::Data { time, data }) => return Some((time, data)),
            Some(OutputEvent::Frontier(_)) => continue,
            None => return None,
        }
    }
}

/// Input → inspect → probe: one record is sent at the initial time, then the
/// input is dropped without advancing. The inspect sees the record before
/// the probe completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontier_advances_after_input_close() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("frontier-close");
    let input = builder.input::<i32>("data").unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_inspect = Arc::clone(&seen);
    let (stream, probe) = input
        .inspect("inspect", move |time, value| {
            seen_by_inspect.lock().unwrap().push((*time, *value))
        })
        .probe();
    stream.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![1, 2, 3]).unwrap();
    drop(sender);

    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done())
        .await
        .unwrap()
        .unwrap();
    assert!(probe.is_done());
    assert!(probe.frontier().is_empty());
    assert_eq!(*seen.lock().unwrap(), vec![(0, 1), (0, 2), (0, 3)]);

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || handle.join_blocking()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_eq!(receiver.collect_data(), vec![(0, vec![1, 2, 3])]);
}

#[test]
fn advance_to_moves_frontier() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("advance-frontier");
    let input = builder.input::<i32>("data").unwrap();
    input
        .map("identity", |_time, value| value)
        .output("results")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![1, 2]).unwrap();
    sender.advance_to(5).unwrap();
    sender.send(5, vec![3, 4]).unwrap();
    sender.advance_to(10).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    assert_eq!(
        receiver.collect_data(),
        vec![(0, vec![1, 2]), (5, vec![3, 4])]
    );
}

#[test]
fn notification_fires_at_correct_time() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-order");
    let input = builder.input::<i32>("data").unwrap();
    input
        .unary_notify("aggregate", {
            let mut stash: HashMap<u64, Vec<i32>> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    stash.entry(time).or_default().extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(data) = stash.remove(&time) {
                        let sum: i32 = data.iter().sum();
                        output.push_vec(time, vec![sum]);
                    }
                }
                Ok(())
            }
        })
        .output("results")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![1, 2]).unwrap();
    sender.advance_to(1).unwrap();
    sender.send(1, vec![3, 4]).unwrap();
    sender.advance_to(2).unwrap();
    sender.send(2, vec![5, 6]).unwrap();
    sender.advance_to(3).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut results = receiver.collect_data();
    results.sort_by_key(|(time, _)| *time);
    assert_eq!(results, vec![(0, vec![3]), (1, vec![7]), (2, vec![11])]);
}

#[test]
fn notification_waits_for_all_input() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-waits");
    let input = builder.input::<i32>("data").unwrap();
    input
        .unary_notify("buffer", {
            let mut stash: HashMap<u64, Vec<i32>> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    stash.entry(time).or_default().extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(data) = stash.remove(&time) {
                        let sum: i32 = data.iter().sum();
                        output.push_vec(time, vec![sum]);
                    }
                }
                Ok(())
            }
        })
        .output("results")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![1, 2]).unwrap();
    sender.send(0, vec![3, 4, 5]).unwrap();

    // Give the executor time to process the input (operators run, buffer data)
    // but notification should NOT fire because frontier hasn't advanced past time 0.
    std::thread::sleep(Duration::from_millis(100));
    assert!(recv_next_data(&receiver, Duration::from_millis(300)).is_none());

    sender.advance_to(1).unwrap();
    let notified = recv_next_data(&receiver, Duration::from_secs(5)).unwrap();
    assert_eq!(notified, (0, vec![15]));

    drop(sender);
    handle.join_blocking().unwrap();
    assert!(receiver.collect_data().is_empty());
}

/// A ten-round input → inspect → probe driver, with one probe before the
/// inspect and one after it. Each of rounds 1 to 9 sends, advances and waits
/// until both probes pass the round; the probe after the inspect proves the
/// inspect has seen the round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_reflects_progress() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("probe-progress");
    let (input, probe_before) = builder.input::<i32>("data").unwrap().probe();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_inspect = Arc::clone(&seen);
    let (stream, probe) = input
        .inspect("inspect", move |time, value| {
            seen_by_inspect.lock().unwrap().push((*time, *value))
        })
        .probe();
    stream.output("results").unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    assert!(probe.frontier().less_than(&1));
    assert!(probe.frontier().less_equal(&0));

    sender.send(0, vec![10, 20]).unwrap();
    assert!(probe.frontier().less_than(&1));

    assert!(
        tokio::time::timeout(
            Duration::from_millis(200),
            probe.clone().wait_until_done_with(&0),
        )
        .await
        .is_err()
    );

    sender.advance_to(1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done_with(&0))
        .await
        .unwrap()
        .unwrap();

    assert!(!probe.frontier().less_than(&1));
    assert!(probe.frontier().less_equal(&1));
    assert!(!probe.is_done());

    tokio::time::timeout(
        Duration::from_secs(5),
        probe_before.wait_until_done_with(&0),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(probe_before.frontier(), Antichain::from_elem(1));
    assert_eq!(probe.frontier(), Antichain::from_elem(1));
    let mut inspected = vec![(0, 10), (0, 20)];
    assert_eq!(*seen.lock().unwrap(), inspected);
    let mut expected = vec![(0, vec![10, 20])];

    for round in 1..10u64 {
        sender.send(round, vec![round as i32]).unwrap();
        for p in [&probe_before, &probe] {
            assert_eq!(p.frontier(), Antichain::from_elem(round), "round {round}");
        }
        sender.advance_to(round + 1).unwrap();
        for p in [&probe_before, &probe] {
            tokio::time::timeout(Duration::from_secs(5), p.wait_until_done_with(&round))
                .await
                .unwrap()
                .unwrap();
            // Past the round, and no further while the input is open.
            assert_eq!(
                p.frontier(),
                Antichain::from_elem(round + 1),
                "round {round}"
            );
        }
        inspected.push((round, round as i32));
        assert_eq!(
            *seen.lock().unwrap(),
            inspected,
            "inspected by round {round}"
        );
        expected.push((round, vec![round as i32]));
    }

    drop(sender);
    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe_before.wait_until_done())
        .await
        .unwrap()
        .unwrap();
    assert!(probe_before.is_done());

    let (joined, collected) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || (handle.join_blocking(), receiver.collect_data())),
    )
    .await
    .unwrap()
    .unwrap();
    joined.unwrap();

    assert_eq!(collected, expected);
    assert_eq!(*seen.lock().unwrap(), inspected);
}

/// On two workers, records exchanged by value feed a per-time sum, and a
/// notification fires only once every worker's input has passed its time.
///
/// Evens go to worker 0 and odds to worker 1. Worker 0 advances first: once
/// the operators have taken every record (their own count, updated at the
/// end of each call), none has given a sum and the probe after the sums is
/// exactly at 0, since worker 1's input still holds 0. Once worker 1 advances
/// too, the probe reaches exactly 1 while both inputs are open, each worker
/// has delivered its sum at 0, and the operator's own log shows each
/// worker's notification fired once, with its input frontier past 0.
#[test]
fn multi_worker_frontier_coordination() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    // Records the operators have taken, updated at the end of each call, once
    // the call has decided what to give.
    let taken = Arc::new(AtomicUsize::new(0));
    let given: Given = Arc::default();
    let probe = OnceCell::new();
    let mut multi = rt
        .spawn_multi(
            "exchange-frontier-coordination",
            2,
            |builder| {
                let input = builder.input::<i32>("data").unwrap();
                let (sums, handle) = input
                    .exchange_by_hash("distribute", |value: &i32| *value as u64)
                    .unary_notify("aggregate", {
                        let mut stash: HashMap<u64, Vec<i32>> = HashMap::new();
                        let taken = Arc::clone(&taken);
                        let given = Arc::clone(&given);
                        move |input, output, ctx| {
                            let mut records = 0;
                            while let Some((time, data)) = input.next() {
                                records += data.len();
                                stash.entry(time).or_default().extend(data);
                                ctx.notify_at(time);
                            }
                            while let Some(time) = ctx.next_notification() {
                                if let Some(data) = stash.remove(&time) {
                                    given.lock().unwrap().push((time, input.frontier().clone()));
                                    let sum: i32 = data.iter().sum();
                                    output.push_vec(time, vec![sum]);
                                }
                            }
                            taken.fetch_add(records, Ordering::SeqCst);
                            Ok(())
                        }
                    })
                    .probe();
                probe.set(handle).expect("the dataflow is built once");
                sums.output("results").unwrap();
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let probe = probe.into_inner().expect("probe");

    let out0 = multi.take_output::<i32>(0, "results").unwrap();
    let out1 = multi.take_output::<i32>(1, "results").unwrap();
    let in0 = multi.take_input::<i32>(0, "data").unwrap();
    let in1 = multi.take_input::<i32>(1, "data").unwrap();

    // Send mix of even and odd values from both workers at time 0.
    // Exchange distributes: evens (2,4,6) → worker 0, odds (1,3,5) → worker 1.
    in0.send(0, vec![1, 2, 3]).unwrap();
    in1.send(0, vec![4, 5, 6]).unwrap();

    // Only advance worker 0 — notification should NOT fire on either worker
    // because worker 1's frontier still includes time 0, even once both
    // operators have taken every record.
    in0.advance_to(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while taken.load(Ordering::SeqCst) < 6 {
        assert!(
            Instant::now() < deadline,
            "records taken: {}",
            taken.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let early = given.lock().unwrap().clone();
    assert!(
        early.is_empty(),
        "sums given while worker 1 is at 0: {early:?}"
    );
    assert_eq!(
        probe.frontier(),
        Antichain::from_elem(0),
        "probe while worker 1 is at 0"
    );
    assert!(recv_next_data(&out0, Duration::from_millis(200)).is_none());
    assert!(recv_next_data(&out1, Duration::from_millis(50)).is_none());

    // Now advance worker 1 — both frontiers past time 0, notifications fire,
    // and the probe stops at exactly 1 while both inputs are open.
    in1.advance_to(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let frontier = probe.frontier();
        assert!(
            !frontier.is_empty() && frontier.elements().iter().all(|t| *t <= 1),
            "probe at {frontier:?} while the inputs are open at 1"
        );
        if frontier.elements() == [1] {
            break;
        }
        assert!(Instant::now() < deadline, "probe stuck at {frontier:?}");
        std::thread::sleep(Duration::from_millis(1));
    }
    // Even sum = 2+4+6=12 at worker 0, odd sum = 1+3+5=9 at worker 1.
    let mut sums = Vec::new();
    for (worker, output) in [&out0, &out1].into_iter().enumerate() {
        while let Some(event) = output.try_recv() {
            if let OutputEvent::Data { time, data } = event {
                sums.push((worker, time, data));
            }
        }
    }
    assert_eq!(
        sums,
        vec![(0, 0, vec![12]), (1, 0, vec![9])],
        "sums once time 0 completed"
    );

    drop(in0);
    drop(in1);
    // Completion and the rest of both outputs, failing after 10 s instead of
    // hanging.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let joined = multi.join_blocking();
        let _ = done_tx.send((joined, out0.collect_data(), out1.collect_data()));
    });
    let (joined, rest0, rest1) = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the dataflow did not finish within 10 s");
    joined.unwrap();

    assert!(probe.is_done());
    assert!(
        rest0.is_empty() && rest1.is_empty(),
        "output after the sums: {rest0:?} {rest1:?}"
    );
    // Each worker gave its sum once, at 0, and only with its input frontier
    // past 0: time 0 had completed when the operator itself decided to give.
    assert_eq!(
        *given.lock().unwrap(),
        vec![(0, Antichain::from_elem(1)); 2],
        "times given, with the input frontier then"
    );
}

#[test]
fn nested_loop_timestamps() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let seen_times = Arc::new(Mutex::new(Vec::<Product<u64, u32>>::new()));

    let builder = DataflowBuilder::<u64>::new("nested-loop-timestamps");
    let input = builder.input::<i32>("data").unwrap();
    let seen_for_loop = Arc::clone(&seen_times);
    input
        .iterate::<u32>("loop", 1u32, move |iter_var| {
            let seen_for_map = Arc::clone(&seen_for_loop);
            let doubled = iter_var.map("record_and_double", move |time, value| {
                seen_for_map.lock().unwrap().push(*time);
                value * 2
            });
            let done = doubled.clone().filter("done", |_time, value| *value >= 100);
            let again = doubled.filter("again", |_time, value| *value < 100);
            IterateResult {
                feedback: again,
                output: done,
            }
        })
        .output("results")
        .unwrap();
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    sender.send(0, vec![5]).unwrap();
    sender.advance_to(1).unwrap();
    sender.send(1, vec![50]).unwrap();
    sender.advance_to(2).unwrap();
    drop(sender);

    handle.join_blocking().unwrap();

    let mut results = receiver.collect_data();
    results.sort_by_key(|(time, _)| *time);
    assert_eq!(results, vec![(0, vec![160]), (1, vec![100])]);

    let seen = seen_times.lock().unwrap().clone();
    let mut outer0: Vec<u32> = seen
        .iter()
        .filter(|time| time.outer == 0)
        .map(|time| time.inner)
        .collect();
    outer0.sort();
    let mut outer1: Vec<u32> = seen
        .iter()
        .filter(|time| time.outer == 1)
        .map(|time| time.inner)
        .collect();
    outer1.sort();

    assert_eq!(outer0, vec![0, 1, 2, 3, 4]);
    assert_eq!(outer1, vec![0]);
}
