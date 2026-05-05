use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::{
    DataflowBuilder, IterateResult, OutputReceiver, Product, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontier_advances_after_input_close() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("frontier-close");
    let input = builder.input::<i32>("data");
    let (stream, probe) = input.map("identity", |_time, value| value).probe();
    stream.output("results");
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
    let input = builder.input::<i32>("data");
    input.map("identity", |_time, value| value).output("results");
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

    assert_eq!(receiver.collect_data(), vec![(0, vec![1, 2]), (5, vec![3, 4])]);
}

#[test]
fn notification_fires_at_correct_time() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-order");
    let input = builder.input::<i32>("data");
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
        .output("results");
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
    let input = builder.input::<i32>("data");
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
        .output("results");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probe_reflects_progress() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("probe-progress");
    let input = builder.input::<i32>("data");
    let (stream, probe) = input.map("identity", |_time, value| value).probe();
    stream.output("results");
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    assert!(probe.frontier().less_than(&1));
    assert!(probe.frontier().less_equal(&0));

    sender.send(0, vec![10, 20]).unwrap();
    assert!(probe.frontier().less_than(&1));

    assert!(tokio::time::timeout(
        Duration::from_millis(200),
        probe.clone().wait_until_done_with(&0),
    )
    .await
    .is_err());

    sender.advance_to(1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done_with(&0))
        .await
        .unwrap()
        .unwrap();

    assert!(!probe.frontier().less_than(&1));
    assert!(probe.frontier().less_equal(&1));
    assert!(!probe.is_done());

    drop(sender);
    tokio::time::timeout(Duration::from_secs(5), probe.wait_until_done())
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || handle.join_blocking()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_eq!(receiver.collect_data(), vec![(0, vec![10, 20])]);
}

#[test]
fn multi_worker_frontier_coordination() {
    // Use exchange to distribute data across 2 workers (even values → worker 0,
    // odd values → worker 1). Each worker aggregates its subset independently.
    // Verify that notifications only fire after BOTH workers advance their frontiers.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "exchange-frontier-coordination",
            2,
            |_worker_idx, builder| {
                let input = builder.input::<i32>("data");
                input
                    .exchange_by_hash("distribute", |value: &i32| *value as u64)
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
                    .output("results");
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();

    let out0 = multi.take_output::<i32>(0, "results").unwrap();
    let out1 = multi.take_output::<i32>(1, "results").unwrap();
    let in0 = multi.take_input::<i32>(0, "data").unwrap();
    let in1 = multi.take_input::<i32>(1, "data").unwrap();

    // Send mix of even and odd values from both workers at time 0.
    // Exchange distributes: evens (2,4,6) → worker 0, odds (1,3,5) → worker 1.
    in0.send(0, vec![1, 2, 3]).unwrap();
    in1.send(0, vec![4, 5, 6]).unwrap();

    // Only advance worker 0 — notification should NOT fire on either worker
    // because worker 1's frontier still includes time 0.
    in0.advance_to(1).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(recv_next_data(&out0, Duration::from_millis(200)).is_none());
    assert!(recv_next_data(&out1, Duration::from_millis(50)).is_none());

    // Now advance worker 1 — both frontiers past time 0, notifications fire.
    in1.advance_to(1).unwrap();

    drop(in0);
    drop(in1);
    multi.join_blocking().unwrap();

    // Collect results from both workers. Even sum = 2+4+6=12, odd sum = 1+3+5=9.
    let mut all_sums: Vec<i32> = Vec::new();
    for (_time, data) in out0.collect_data() {
        all_sums.extend(data);
    }
    for (_time, data) in out1.collect_data() {
        all_sums.extend(data);
    }
    all_sums.sort();
    assert_eq!(all_sums, vec![9, 12]);
}

#[test]
fn nested_loop_timestamps() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let seen_times = Arc::new(Mutex::new(Vec::<Product<u64, u32>>::new()));

    let builder = DataflowBuilder::<u64>::new("nested-loop-timestamps");
    let input = builder.input::<i32>("data");
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
        .output("results");
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
