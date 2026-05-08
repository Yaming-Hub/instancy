//! Integration tests for the graceful drain on cancellation feature.
//!
//! Validates that:
//! - `drain_on_cancel` lets in-flight data complete before stopping.
//! - Without drain, cancellation stops the dataflow immediately.
//! - Drain timeout expiry still returns `Cancelled`.
//! - Drain works with multi-worker dataflows.

use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// A dataflow with drain enabled completes successfully when cancelled
/// after sending data, because the drain phase lets in-flight records
/// flow through to the output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_completes_inflight_data() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("drain-complete");

    let input = builder.input::<i32>("data");
    input.map("double", |_t, v| v * 2).output("results");

    let dataflow = builder.build().unwrap();

    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let sender = handle.take_input::<i32>("data").unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();

    // Send data, close input, then cancel.
    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.close();

    // Give the runtime a moment to process before cancelling.
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.cancel();

    // With drain enabled, the dataflow should complete successfully.
    let result = handle.join().await;
    assert!(
        result.is_ok(),
        "drain should let dataflow complete: {result:?}"
    );

    // Verify data was processed.
    let mut data = receiver.collect_data();
    data.sort_by_key(|(t, _)| *t);
    assert_eq!(data, vec![(0, vec![2, 4, 6])]);
}

/// Without drain, cancelling a dataflow with open inputs returns Cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_drain_cancels_immediately() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("no-drain");

    let input = builder.input::<i32>("data");
    input.map("identity", |_t, v| v);

    let dataflow = builder.build().unwrap();

    // No drain — default behavior.
    let mut handle = rt.spawn(dataflow, SpawnOptions::default()).unwrap();
    let _sender = handle.take_input::<i32>("data").unwrap();

    // Cancel immediately.
    handle.cancel();

    let result = handle.join().await;
    assert!(result.is_err(), "should return Cancelled without drain");
}

/// When drain timeout expires (dataflow can't complete in time), the
/// result is Err(Cancelled).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_timeout_returns_cancelled() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("drain-timeout");

    // Input port that we never close — dataflow can't finish.
    let input = builder.input::<i32>("data");
    input.map("identity", |_t, v| v);

    let dataflow = builder.build().unwrap();

    // Very short drain timeout.
    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_millis(100));
    let mut handle = rt.spawn(dataflow, opts).unwrap();
    let _sender = handle.take_input::<i32>("data").unwrap();

    // Cancel — drain starts but input never closes, so timeout expires.
    handle.cancel();

    let result = handle.join().await;
    assert!(
        result.is_err(),
        "drain timeout should return Cancelled: {result:?}"
    );
}

/// Drain works correctly with multi-worker dataflows (spawn_multi).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_with_multi_worker() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
    let mut multi = rt
        .spawn_multi(
            "drain-multi",
            2,
            |_worker_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.map("triple", |_t, v| v * 3).output("results");
                Ok(())
            },
            opts,
        )
        .unwrap();

    // Send data to both workers and close.
    let s0 = multi.take_input::<i32>(0, "data").unwrap();
    let s1 = multi.take_input::<i32>(1, "data").unwrap();
    s0.send(0, vec![1, 2]).unwrap();
    s1.send(0, vec![3, 4]).unwrap();
    s0.close();
    s1.close();

    // Give a moment for processing, then cancel to trigger drain.
    tokio::time::sleep(Duration::from_millis(50)).await;
    multi.cancel();

    let completion = multi.join();
    let result =
        tokio::task::spawn_blocking(move || completion.wait())
            .await
            .unwrap();

    assert!(
        result.is_ok(),
        "multi-worker drain should complete: {result:?}"
    );
}
