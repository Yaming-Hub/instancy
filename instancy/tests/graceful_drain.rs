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

    let input = builder.input::<i32>("data").unwrap();
    input
        .map("double", |_t, v| v * 2)
        .output("results")
        .unwrap();

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

    let input = builder.input::<i32>("data").unwrap();
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

    let input = builder.input::<i32>("data").unwrap();
    input.map("identity", |_t, v| v).output("results").unwrap();

    let dataflow = builder.build().unwrap();

    // Very short drain timeout.
    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_millis(100));
    let mut handle = rt.spawn(dataflow, opts).unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();
    // The consumer never reads, so the accepted backlog can never drain.
    let _stalled_consumer = handle.take_output::<i32>("results").unwrap();
    for v in 0..1_000 {
        sender.send(0, vec![v]).unwrap();
    }

    // Cancel — the drain closes the input, but the backlog outlasts the timeout.
    handle.cancel();

    let result = handle.join().await;
    assert!(
        matches!(result, Err(instancy::Error::Cancelled { .. })),
        "drain timeout should return Cancelled: {result:?}"
    );
}

/// The drain closes external inputs: later sends fail, and the drain finishes
/// even while the caller still holds the sender.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_closes_inputs_still_held_by_the_caller() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("drain-held-input");
    let input = builder.input::<i32>("data").unwrap();
    input.map("identity", |_t, v| v).output("results").unwrap();

    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
    let mut handle = rt.spawn(builder.build().unwrap(), opts).unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    sender.send(0, vec![1, 2]).unwrap();

    handle.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), handle.join())
        .await
        .expect("drain must not wait for the caller to close the input");
    assert!(result.is_ok(), "drain should complete: {result:?}");
    assert!(
        sender.send(0, vec![3]).is_err(),
        "input closed by the drain"
    );
    let data: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    assert_eq!(data, vec![1, 2]);
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
            |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data").unwrap();
                input
                    .map("triple", |_t, v| v * 3)
                    .output("results")
                    .unwrap();
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
    let result = tokio::task::spawn_blocking(move || completion.wait())
        .await
        .unwrap();

    assert!(
        result.is_ok(),
        "multi-worker drain should complete: {result:?}"
    );
}

/// Worker exchange with a drain: closing channels across workers on the
/// drained (successful) path still exhausts every exchange input, so the
/// dataflow completes `Ok` with every record (the cluster-only close gate
/// must not affect single-node dataflows).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_with_multi_worker_exchange() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let opts = SpawnOptions::new().drain_on_cancel(Duration::from_secs(5));
    let mut multi = rt
        .spawn_multi(
            "drain-multi-exchange",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .map("pre", |_t, v| v)
                    .exchange_by_hash("route", |v: &u64| *v)
                    .map("post", |_t, v| v)
                    .output("results")?;
                Ok(())
            },
            opts,
        )
        .unwrap();
    let s0 = multi.take_input::<u64>(0, "data").unwrap();
    let s1 = multi.take_input::<u64>(1, "data").unwrap();
    let r0 = multi.take_output::<u64>(0, "results").unwrap();
    let r1 = multi.take_output::<u64>(1, "results").unwrap();
    s0.send(0, (0..100).collect()).unwrap();
    s1.send(0, (100..200).collect()).unwrap();
    s0.close();
    s1.close();
    tokio::time::sleep(Duration::from_millis(20)).await;
    multi.cancel();
    let completion = multi.join();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || completion.wait()),
    )
    .await
    .expect("drain must not hang")
    .unwrap();
    assert!(result.is_ok(), "drained exchange must complete: {result:?}");
    let total: u64 = [r0, r1]
        .into_iter()
        .flat_map(|r| r.collect_data())
        .flat_map(|(_, batch)| batch)
        .sum();
    assert_eq!(total, (0..200).sum::<u64>());
}

/// A multi-worker exchange whose sibling still has an open input: one
/// worker's operator fails; the whole dataflow must stop promptly with that
/// error. (Off the cluster path dropped outputs still close on drop, as
/// before; the cancellation is what ends the sibling.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_failure_terminates_promptly() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "fail-multi-exchange",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .unary_notify("maybe_fail", |input, output, _ctx| {
                        while let Some((t, data)) = input.next() {
                            if data.contains(&u64::MAX) {
                                return Err(instancy::Error::Runtime(
                                    instancy::RuntimeError::InvalidConfig(
                                        "deliberate operator failure".into(),
                                    ),
                                ));
                            }
                            output.push_vec(t, data);
                        }
                        Ok(())
                    })
                    .exchange_by_hash("route", |v: &u64| *v)
                    .map("post", |_t, v| v)
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let s0 = multi.take_input::<u64>(0, "data").unwrap();
    // Worker 1's input stays open: only the failure can end the dataflow.
    let _s1 = multi.take_input::<u64>(1, "data").unwrap();
    let _r0 = multi.take_output::<u64>(0, "results").unwrap();
    let _r1 = multi.take_output::<u64>(1, "results").unwrap();
    s0.send(0, (0..50).collect()).unwrap();
    s0.send(0, vec![u64::MAX]).unwrap();
    let completion = multi.join();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || completion.wait()),
    )
    .await
    .expect("a worker failure must end the dataflow promptly")
    .unwrap();
    let err = result.expect_err("the failure must be reported");
    assert!(
        err.to_string().contains("deliberate operator failure")
            || matches!(err, instancy::Error::Cancelled { .. }),
        "{err}"
    );
}

/// Cancelling a multi-worker exchange with open inputs (no drain) ends it
/// promptly with `Cancelled`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_worker_cancel_terminates_promptly() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "cancel-multi-exchange",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<u64>("data")?
                    .map("pre", |_t, v| v)
                    .exchange_by_hash("route", |v: &u64| *v)
                    .map("post", |_t, v| v)
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new(),
        )
        .unwrap();
    let s0 = multi.take_input::<u64>(0, "data").unwrap();
    let _s1 = multi.take_input::<u64>(1, "data").unwrap();
    let _r0 = multi.take_output::<u64>(0, "results").unwrap();
    let _r1 = multi.take_output::<u64>(1, "results").unwrap();
    s0.send(0, (0..50).collect()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    multi.cancel();
    let completion = multi.join();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || completion.wait()),
    )
    .await
    .expect("cancellation must end the dataflow promptly")
    .unwrap();
    assert!(
        matches!(result, Err(instancy::Error::Cancelled { .. })),
        "{result:?}"
    );
}
