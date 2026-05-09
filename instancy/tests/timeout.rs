//! Integration tests for the external cancellation token feature.
//!
//! Validates that:
//! - A user-provided tokio CancellationToken cancels the dataflow.
//! - A dataflow that completes before the token is cancelled is NOT affected.
//! - Multi-worker dataflows are cancelled via external token.

use std::time::Duration;

use instancy::{
    cancellation::CancellationReason, DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_cancelled_by_external_token() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("cancel-test");

    // Create a dataflow with an input port that we never close.
    let input = builder.input::<i32>("data");
    input.map("identity", |_t, v| v);

    let dataflow = builder.build().unwrap();

    // User creates their own cancellation token and triggers it after 100ms.
    let user_token = CancellationToken::new();
    let trigger = user_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        trigger.cancel();
    });

    let opts = SpawnOptions::new().cancellation_token(user_token);
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let cancel_token = handle.cancel_token().clone();
    let _input = handle.take_input::<i32>("data").unwrap();

    // Wait for completion (should be cancelled by external token).
    let result = handle.join().await;

    assert!(
        result.is_err() || cancel_token.is_cancelled(),
        "dataflow should be cancelled by external token"
    );

    if cancel_token.is_cancelled() {
        let reason = cancel_token.reason();
        assert_eq!(
            reason,
            Some(CancellationReason::UserRequested),
            "cancellation reason should be UserRequested"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_completes_before_token_cancelled() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("no-cancel-test");

    let input = builder.input::<i32>("data");
    input.map("double", |_t, v| v * 2).output("results");

    let dataflow = builder.build().unwrap();

    // Token that will never be cancelled during the test.
    let user_token = CancellationToken::new();
    let opts = SpawnOptions::new().cancellation_token(user_token.clone());
    let mut handle = rt.spawn(dataflow, opts).unwrap();

    let cancel_token = handle.cancel_token().clone();
    let receiver = handle.take_output::<i32>("results").unwrap();
    let sender = handle.take_input::<i32>("data").unwrap();

    // Send data and close immediately.
    let _ = sender.send(0, vec![1, 2, 3]);
    sender.close();

    // Wait for completion.
    let result = handle.join().await;
    assert!(result.is_ok(), "dataflow should complete successfully");

    // Verify data was processed.
    let data = receiver.collect_data();
    assert_eq!(data, vec![(0, vec![2, 4, 6])]);

    // Internal token should NOT be cancelled with UserRequested.
    assert!(
        !cancel_token.is_cancelled()
            || cancel_token.reason() != Some(CancellationReason::UserRequested),
        "external token should not have caused cancellation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_multi_cancelled_by_external_token() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();

    let user_token = CancellationToken::new();
    let trigger = user_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        trigger.cancel();
    });

    let opts = SpawnOptions::new().cancellation_token(user_token);
    let mut multi = rt
        .spawn_multi(
            "multi-cancel",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i32>("data");
                input.map("identity", |_t, v| v);
                Ok(())
            },
            opts,
        )
        .unwrap();

    // Don't close inputs — workers will hang until token fires.
    let _input0 = multi.take_input::<i32>(0, "data").unwrap();
    let _input1 = multi.take_input::<i32>(1, "data").unwrap();

    // Wait for completion.
    let completion = multi.join();
    let result = tokio::task::spawn_blocking(move || completion.wait())
        .await
        .unwrap();

    assert!(
        result.is_err(),
        "multi-worker dataflow should be cancelled by external token"
    );
}
