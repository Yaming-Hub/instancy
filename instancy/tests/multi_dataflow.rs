//! Integration tests for concurrent multi-dataflow execution on shared worker pools.
//!
//! These tests validate the async executor stack (PRs 29/29B/30) under realistic
//! concurrent conditions:
//! - Multiple dataflows spawned on a shared RuntimeHandle
//! - Concurrent InputSender writes and OutputReceiver reads
//! - Poll budget fairness between heavy and light workloads
//! - Cancellation propagation under load

use std::thread;
use std::time::{Duration, Instant};

use instancy::DataflowBuilder;
use instancy::Error;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

/// Spawn 4 dataflows concurrently on a 2-thread pool. Each receives unique data
/// via InputSender and produces results on OutputReceiver. Verify all complete
/// with correct output.
#[test]
fn concurrent_spawn_four_dataflows() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "concurrent-4".into(),
    })
    .unwrap();

    let mut handles = Vec::new();
    let mut senders = Vec::new();
    let mut receivers = Vec::new();

    // Spawn 4 dataflows: input → map(triple) → output
    for i in 0..4u32 {
        let builder = DataflowBuilder::<u64>::new(format!("df_{i}"));
        let input = builder.input::<i32>("data");
        input.map("triple", |_t, x| x * 3).output("results");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();
        let receiver = handle.take_output::<i32>("results").unwrap();

        senders.push((i, sender));
        receivers.push((i, receiver));
        handles.push(handle);
    }

    // Send unique data to each dataflow from separate threads
    let send_threads: Vec<_> = senders
        .into_iter()
        .map(|(i, sender)| {
            thread::spawn(move || {
                let data: Vec<i32> = (0..10).map(|x| (i as i32) * 100 + x).collect();
                sender.send(0u64, data).unwrap();
                sender.close();
            })
        })
        .collect();

    for t in send_threads {
        t.join().unwrap();
    }

    // Collect and verify results from each dataflow
    for (i, receiver) in receivers {
        let results = receiver.collect_data();
        let mut all_data: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
        all_data.sort();

        let expected: Vec<i32> = (0..10).map(|x| ((i as i32) * 100 + x) * 3).collect();
        assert_eq!(all_data, expected, "dataflow {i} produced wrong results");
    }

    // All should complete successfully
    for handle in handles {
        handle.join_blocking().unwrap();
    }
}

/// Spawn 8 dataflows on a 2-thread pool. Verifies the poll budget ensures all
/// dataflows eventually complete (no starvation).
#[test]
fn eight_dataflows_on_two_threads_no_starvation() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "starvation-8".into(),
    })
    .unwrap();

    let mut handles = Vec::new();

    for i in 0..8u32 {
        let builder = DataflowBuilder::<u64>::new(format!("df_{i}"));
        let data: Vec<(u64, Vec<i32>)> = vec![(0, vec![i as i32])];
        builder
            .source("src", data)
            .map("inc", |_t, x| x + 1)
            .output("out");
        let dataflow = builder.build().unwrap();
        handles.push(rt.spawn(dataflow, SpawnOptions::default()).unwrap().join());
    }

    // All 8 should complete within a reasonable time
    let start = Instant::now();
    for (i, completion) in handles.into_iter().enumerate() {
        completion.wait().unwrap_or_else(|e| {
            panic!("dataflow {i} failed: {e}");
        });
    }
    let elapsed = start.elapsed();

    // Sanity check: shouldn't take more than 30 seconds (generous for CI)
    assert!(
        elapsed < Duration::from_secs(30),
        "8 dataflows took {elapsed:?} — possible starvation"
    );
}

/// Spawn a mix of "heavy" (multi-round) and "light" (single-batch) dataflows.
/// Verify light dataflows complete without being starved by heavy ones.
#[test]
fn mixed_workload_fairness() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "mixed-workload".into(),
    })
    .unwrap();

    // 2 "heavy" dataflows: receive multiple rounds of data
    let mut heavy_handles = Vec::new();
    let mut heavy_senders = Vec::new();

    for i in 0..2 {
        let builder = DataflowBuilder::<u64>::new(format!("heavy_{i}"));
        let input = builder.input::<i32>("data");
        input.map("square", |_t, x| x * x).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();
        heavy_senders.push(sender);
        heavy_handles.push(handle);
    }

    // 4 "light" dataflows: static source, no input ports
    let mut light_completions = Vec::new();

    for i in 0..4 {
        let builder = DataflowBuilder::<u64>::new(format!("light_{i}"));
        builder
            .source("src", vec![(0u64, vec![1i32, 2, 3])])
            .output("out");
        let dataflow = builder.build().unwrap();
        light_completions.push(rt.spawn(dataflow, SpawnOptions::default()).unwrap().join());
    }

    // Send multiple rounds to heavy dataflows
    for round in 0..5u64 {
        for sender in &heavy_senders {
            sender.send(round, vec![round as i32]).unwrap();
        }
    }

    // Light dataflows should complete even while heavy ones are active.
    // Use generous timeout as hang guard, not perf benchmark.
    let start = Instant::now();
    for (i, c) in light_completions.into_iter().enumerate() {
        c.wait().unwrap_or_else(|e| panic!("light_{i} failed: {e}"));
    }
    let light_elapsed = start.elapsed();
    assert!(
        light_elapsed < Duration::from_secs(30),
        "light dataflows took {light_elapsed:?} — fairness issue"
    );

    // Now close heavy inputs and wait for them too
    for sender in heavy_senders {
        sender.close();
    }
    for (i, mut handle) in heavy_handles.into_iter().enumerate() {
        let receiver = handle.take_output::<i32>("out").unwrap();
        let results = receiver.collect_data();
        // Should have 5 rounds of data
        assert_eq!(
            results.len(),
            5,
            "heavy_{i} expected 5 batches, got {}",
            results.len()
        );
        handle.join_blocking().unwrap();
    }
}

/// Cancel a RuntimeHandle while multiple dataflows are actively processing.
/// All should terminate without hanging.
#[test]
fn shutdown_cancels_all_running_dataflows() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "shutdown-test".into(),
    })
    .unwrap();

    let mut spawned = Vec::new();

    // Spawn 4 dataflows with open input ports (will block waiting for data)
    for i in 0..4 {
        let builder = DataflowBuilder::<u64>::new(format!("blocked_{i}"));
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();
        let handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        spawned.push(handle);
    }

    // Spawn registration is synchronous — all executors are registered
    // by the time spawn() returns. No sleep needed.

    // Shutdown should cancel all
    rt.shutdown();

    // All should complete with Cancelled error — no hangs.
    // Use a generous timeout (30s) as a hang guard, not a perf benchmark.
    let start = Instant::now();
    for (i, handle) in spawned.into_iter().enumerate() {
        let result = handle.join_blocking();
        assert!(
            matches!(&result, Err(Error::Cancelled { .. })),
            "dataflow {i} expected Cancelled, got {result:?}"
        );
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "shutdown took {elapsed:?} — possible hang"
    );
}

/// Spawn dataflows, cancel individual ones via SpawnedDataflow::cancel(),
/// and verify others continue running unaffected.
#[test]
fn individual_cancel_preserves_siblings() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "individual-cancel".into(),
    })
    .unwrap();

    // Spawn 3 dataflows with input ports
    let mut handles = Vec::new();
    let mut senders = Vec::new();

    for i in 0..3 {
        let builder = DataflowBuilder::<u64>::new(format!("df_{i}"));
        let input = builder.input::<i32>("data");
        input.map("inc", |_t, x| x + 1).output("out");
        let dataflow = builder.build().unwrap();

        let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        let sender = handle.take_input::<i32>("data").unwrap();
        senders.push(sender);
        handles.push(handle);
    }

    // Cancel dataflow 1
    handles[1].cancel();

    // Dataflows 0 and 2 should still work fine
    senders[0].send(0u64, vec![10]).unwrap();
    senders[2].send(0u64, vec![20]).unwrap();

    // Close all senders by draining the vec
    let sender_0 = senders.remove(0); // index 0 → df_0
    let sender_1 = senders.remove(0); // was index 1, now 0 → df_1
    let sender_2 = senders.remove(0); // was index 2, now 0 → df_2
    sender_0.close();
    sender_1.close();
    sender_2.close();

    let receiver_0 = handles[0].take_output::<i32>("out").unwrap();
    let results_0 = receiver_0.collect_data();
    assert_eq!(results_0[0].1, vec![11]);

    let receiver_2 = handles[2].take_output::<i32>("out").unwrap();
    let results_2 = receiver_2.collect_data();
    assert_eq!(results_2[0].1, vec![21]);

    // Verify cancellation outcomes: df_1 should be Cancelled, 0 and 2 should be Ok
    let handle_0 = handles.remove(0);
    let handle_1 = handles.remove(0);
    let handle_2 = handles.remove(0);

    assert!(handle_0.join_blocking().is_ok(), "df_0 should succeed");
    assert!(
        matches!(handle_1.join_blocking(), Err(Error::Cancelled { .. })),
        "df_1 should be Cancelled"
    );
    assert!(handle_2.join_blocking().is_ok(), "df_2 should succeed");
}

/// Stress test: spawn many dataflows rapidly and verify all complete.
#[test]
fn stress_spawn_twenty_dataflows() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        schedule_policy: None,
        name: "stress-20".into(),
    })
    .unwrap();

    let mut completions = Vec::new();

    for i in 0..20 {
        let builder = DataflowBuilder::<u64>::new(format!("stress_{i}"));
        builder
            .source("src", vec![(0u64, vec![i as i32])])
            .map("double", |_t, x| x * 2)
            .output("out");
        let dataflow = builder.build().unwrap();
        completions.push(rt.spawn(dataflow, SpawnOptions::default()).unwrap().join());
    }

    let start = Instant::now();
    for (i, c) in completions.into_iter().enumerate() {
        c.wait()
            .unwrap_or_else(|e| panic!("stress_{i} failed: {e}"));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(30),
        "20 dataflows took {elapsed:?}"
    );
}

/// Verify concurrent InputSender writes from multiple threads don't cause races.
#[test]
fn concurrent_input_from_multiple_threads() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "concurrent-input".into(),
    })
    .unwrap();

    let builder = DataflowBuilder::<u64>::new("multi-sender");
    let input = builder.input::<i32>("data");
    input.map("inc", |_t, x| x + 1).output("out");
    let dataflow = builder.build().unwrap();

    let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Clone sender and send from 4 threads
    let threads: Vec<_> = (0..4)
        .map(|tid| {
            let s = sender.clone();
            thread::spawn(move || {
                for round in 0..5u64 {
                    let time = (tid as u64) * 10 + round;
                    s.send(time, vec![tid as i32 * 100 + round as i32]).unwrap();
                }
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }
    // Drop the original sender to close the input
    sender.close();

    let receiver = handle.take_output::<i32>("out").unwrap();
    let results = receiver.collect_data();

    // Should have received 20 batches total (4 threads × 5 rounds)
    let total_items: usize = results.iter().map(|(_, d)| d.len()).sum();
    assert_eq!(total_items, 20, "expected 20 items, got {total_items}");

    // Batch integrity: each send was a single item, so each output batch
    // should also have exactly 1 item (no cross-batch corruption).
    for (time, data) in &results {
        assert_eq!(
            data.len(),
            1,
            "batch corruption at t={time}: expected 1 item, got {}",
            data.len()
        );
    }

    // Each value should be original + 1
    let mut all_values: Vec<i32> = results.into_iter().flat_map(|(_, d)| d).collect();
    all_values.sort();

    let mut expected: Vec<i32> = (0..4i32)
        .flat_map(|tid| (0..5).map(move |r| tid * 100 + r + 1))
        .collect();
    expected.sort();

    assert_eq!(all_values, expected);

    handle.join_blocking().unwrap();
}

/// Verify that an operator panic propagates as an error through join_blocking(),
/// and doesn't crash the runtime or affect other dataflows.
#[test]
fn operator_panic_propagates_error() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "panic-test".into(),
    })
    .unwrap();

    // Spawn a "good" dataflow alongside the failing one
    let good_builder = DataflowBuilder::<u64>::new("good");
    good_builder
        .source("src", vec![(0u64, vec![1i32, 2, 3])])
        .map("double", |_t, x| x * 2)
        .output("out");
    let good_df = good_builder.build().unwrap();
    let good_completion = rt.spawn(good_df, SpawnOptions::default()).unwrap().join();

    // Spawn a dataflow with a panicking operator
    let bad_builder = DataflowBuilder::<u64>::new("panicking");
    bad_builder
        .source("src", vec![(0u64, vec![1i32])])
        .map("boom", |_t, _x: i32| -> i32 {
            panic!("intentional test panic")
        })
        .output("out");
    let bad_df = bad_builder.build().unwrap();
    let bad_completion = rt.spawn(bad_df, SpawnOptions::default()).unwrap().join();

    // The panicking dataflow should return an error (not crash the process)
    let bad_result = bad_completion.wait();
    assert!(
        bad_result.is_err(),
        "expected error from panicking operator"
    );

    // The good dataflow should still complete successfully
    let good_result = good_completion.wait();
    assert!(
        good_result.is_ok(),
        "good dataflow should succeed despite sibling panic, got: {good_result:?}"
    );

    // Runtime should still be usable
    assert!(!rt.is_shutdown());
}

/// Verify that dropping a RuntimeHandle while dataflows are running cancels them
/// (implicit cancellation via Drop, not explicit shutdown()).
#[test]
fn drop_runtime_cancels_active_dataflows() {
    let handle = {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            schedule_policy: None,
            name: "drop-cancel".into(),
        })
        .unwrap();

        let builder = DataflowBuilder::<u64>::new("blocked");
        let input = builder.input::<i32>("data");
        input.output("out");
        let dataflow = builder.build().unwrap();

        let spawned = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
        // rt dropped here → implicit cancel via Drop
        spawned
    };

    // Should complete (cancelled), not hang
    let start = Instant::now();
    let result = handle.join_blocking();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(30),
        "drop-cancel took {elapsed:?} — possible hang"
    );
    // Result should be cancelled or error (not a silent success)
    assert!(result.is_err(), "expected error after runtime drop, got Ok");
}
