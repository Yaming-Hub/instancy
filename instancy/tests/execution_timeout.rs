//! Integration tests for dataflow execution timeout.

use std::time::{Duration, Instant};

use instancy::{
    CancellationReason, DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

/// A dataflow that never closes its input times out correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_cancels_long_running_dataflow() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("timeout-test");
    let input = builder.input::<i32>("data");
    input.output("out");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_millis(200));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    // Keep input open so dataflow never completes naturally.
    let _sender = handle.take_input::<i32>("data").unwrap();

    let start = Instant::now();
    let result = handle.join().await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "should be cancelled by timeout: {result:?}");
    assert!(
        elapsed >= Duration::from_millis(150),
        "should wait ~200ms, got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "should not wait too long, got {elapsed:?}"
    );
}

/// Dataflow that completes before timeout returns Ok.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_completes_before_timeout() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("fast-finish");
    let input = builder.input::<i32>("data");
    input.map("double", |_t, x| x * 2).output("results");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_secs(5));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let sender = handle.take_input::<i32>("data").unwrap();
    sender.send(0, vec![1, 2, 3]).unwrap();
    sender.close();

    let result = handle.join().await;
    assert!(result.is_ok(), "should complete before timeout: {result:?}");
}

/// Timeout reason is CancellationReason::Timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_reports_correct_reason() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("reason-test");
    let input = builder.input::<i32>("data");
    input.output("out");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_millis(100));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let cancel = handle.cancel_token().clone();
    let _sender = handle.take_input::<i32>("data").unwrap();
    handle.join().await.unwrap_err();

    let reason = cancel.reason();
    assert_eq!(
        reason,
        Some(CancellationReason::Timeout),
        "reason should be Timeout, got {reason:?}"
    );
}

/// Timeout + drain_on_cancel: timeout fires, then drain phase runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_with_drain() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("timeout-drain");
    let input = builder.input::<i32>("data");
    input.map("triple", |_t, x| x * 3).output("results");

    let dataflow = builder.build().unwrap();
    let opts = SpawnOptions::new()
        .timeout(Duration::from_millis(200))
        .drain_on_cancel(Duration::from_secs(2));
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let sender = handle.take_input::<i32>("data").unwrap();
    let receiver = handle.take_output::<i32>("results").unwrap();
    sender.send(0, vec![10, 20]).unwrap();
    sender.close(); // Close input so drain can succeed.

    // Dataflow should complete via drain before the drain timeout (2s).
    let result = handle.join().await;
    assert!(
        result.is_ok(),
        "timeout triggers drain which completes: {result:?}"
    );

    let values: Vec<i32> = receiver
        .collect_data()
        .into_iter()
        .flat_map(|(_, v)| v)
        .collect();
    assert_eq!(values, vec![30, 60]);
}

/// Multi-worker timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_with_multi_worker() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let opts = SpawnOptions::new().timeout(Duration::from_millis(200));

    let mut multi = rt
        .spawn_multi(
            "timeout-multi",
            2,
            |_idx, builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.output("out");
                Ok(())
            },
            opts,
        )
        .unwrap();

    // Keep inputs open so dataflow never completes.
    let _s0 = multi.take_input::<i32>(0, "data").unwrap();
    let _s1 = multi.take_input::<i32>(1, "data").unwrap();

    let completion = multi.join();
    let result =
        tokio::task::spawn_blocking(move || completion.wait())
            .await
            .unwrap();

    assert!(
        result.is_err(),
        "multi-worker should be cancelled by timeout: {result:?}"
    );
}
