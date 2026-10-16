use std::time::Duration;

use instancy::{
    AsyncInputSender, DataflowBuilder, Error, InputSender, IoMode, ProgressError, RuntimeConfig,
    RuntimeHandle, SpawnOptions,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
enum Sender {
    Sync(InputSender<u64, i32>),
    Async(AsyncInputSender<u64, i32>),
}

impl Sender {
    async fn send(&self, time: u64, batch: Vec<i32>) {
        match self {
            Self::Sync(sender) => sender.send(time, batch).unwrap(),
            Self::Async(sender) => sender.send(time, batch).await.unwrap(),
        }
    }

    async fn advance_to(&self, time: u64) {
        match self {
            Self::Sync(sender) => sender.advance_to(time).unwrap(),
            Self::Async(sender) => sender.advance_to(time).await.unwrap(),
        }
    }
}

fn assert_late_data_error(error: Error, worker: usize) {
    let Error::Operator {
        operator,
        worker_index,
        source,
    } = error
    else {
        panic!("expected operator context, got {error:?}");
    };
    assert_eq!(operator, "data");
    assert_eq!(worker_index, Some(worker));
    assert!(
        matches!(
            source.downcast_ref::<Error>(),
            Some(Error::Progress(ProgressError::InputTimeBeforeFrontier { time, frontier }))
                if time == "10" && frontier == "11"
        ),
        "expected typed input violation, got {source:?}"
    );
}

#[tokio::test]
async fn delayed_repeated_batches_and_clones_keep_epoch_open_until_coordinated_advance() {
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();
        let builder = DataflowBuilder::<u64>::new("coordinated-input");
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        builder
            .input::<i32>("data")
            .unwrap()
            .inspect("seen", move |_t, value| seen_tx.send(*value).unwrap())
            .fold("sum", 0, |sum, value| sum + value)
            .for_each("results", move |time, value| {
                result_tx.send((*time, *value)).unwrap()
            });
        let mut handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().io_mode(io_mode),
            )
            .unwrap();
        let sender = match io_mode {
            IoMode::Sync => Sender::Sync(handle.take_input("data").unwrap()),
            IoMode::Async => Sender::Async(handle.take_async_input("data").unwrap()),
        };
        let second = sender.clone();
        sender.send(10, vec![1]).await;
        assert_eq!(
            tokio::time::timeout(TIMEOUT, seen_rx.recv()).await.unwrap(),
            Some(1)
        );
        assert!(result_rx.try_recv().is_err());
        drop(sender);

        // The second producer runs only after the first batch was processed and its sender dropped.
        let second = tokio::spawn(async move {
            second.send(10, vec![2]).await;
            second
        })
        .await
        .unwrap();
        assert_eq!(
            tokio::time::timeout(TIMEOUT, seen_rx.recv()).await.unwrap(),
            Some(2)
        );
        assert!(result_rx.try_recv().is_err());
        second.advance_to(11).await;
        assert_eq!(
            tokio::time::timeout(TIMEOUT, result_rx.recv())
                .await
                .unwrap(),
            Some((10, 3))
        );
        drop(second);
        tokio::time::timeout(TIMEOUT, handle.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result_rx.recv().await, None);
    }
}

#[tokio::test]
async fn data_after_observed_epoch_completion_fails_with_context_in_both_io_modes() {
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();
        let builder = DataflowBuilder::<u64>::new("late-input");
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        builder
            .input::<i32>("data")
            .unwrap()
            .fold("sum", 0, |sum, value| sum + value)
            .for_each("results", move |time, value| {
                result_tx.send((*time, *value)).unwrap()
            });
        let mut handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().io_mode(io_mode),
            )
            .unwrap();
        let sender = match io_mode {
            IoMode::Sync => Sender::Sync(handle.take_input("data").unwrap()),
            IoMode::Async => Sender::Async(handle.take_async_input("data").unwrap()),
        };
        let late_sender = sender.clone();
        sender.send(10, vec![1]).await;
        sender.advance_to(11).await;
        assert_eq!(
            tokio::time::timeout(TIMEOUT, result_rx.recv())
                .await
                .unwrap(),
            Some((10, 1))
        );
        drop(sender);
        tokio::spawn(async move { late_sender.send(10, vec![2]).await })
            .await
            .unwrap();
        let error = tokio::time::timeout(TIMEOUT, handle.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_late_data_error(error, 0);
        assert_eq!(result_rx.recv().await, None);
    }
}

#[tokio::test]
async fn invalid_frontier_update_fails_execution_in_both_io_modes() {
    for io_mode in [IoMode::Sync, IoMode::Async] {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..Default::default()
        })
        .unwrap();
        let builder = DataflowBuilder::<u64>::new("backward-frontier");
        builder
            .input::<i32>("data")
            .unwrap()
            .for_each("sink", |_t, _v| {});
        let mut handle = rt
            .spawn(
                builder.build().unwrap(),
                SpawnOptions::new().io_mode(io_mode),
            )
            .unwrap();
        let sender = match io_mode {
            IoMode::Sync => Sender::Sync(handle.take_input("data").unwrap()),
            IoMode::Async => Sender::Async(handle.take_async_input("data").unwrap()),
        };
        sender.advance_to(11).await;
        sender.advance_to(10).await;
        drop(sender);
        let error = tokio::time::timeout(TIMEOUT, handle.join())
            .await
            .unwrap()
            .unwrap_err();
        let Error::Operator {
            operator,
            worker_index,
            source,
        } = error
        else {
            panic!("expected input operator context, got {error:?}");
        };
        assert_eq!(operator, "data");
        assert_eq!(worker_index, Some(0));
        assert!(matches!(source.downcast_ref::<Error>(),
            Some(Error::Progress(ProgressError::InvalidInputFrontier { from, to }))
                if from == "11" && to == "10"));
    }
}

#[tokio::test]
async fn source_async_protocol_error_fails_execution_even_when_producer_returns_ok() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let builder = DataflowBuilder::<u64>::new("async-producer");
    let released = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = released.clone();
    builder
        .source_async("data", move |sender| {
            let released = released.clone();
            async move {
                sender.send(10, vec![1i32]).await?;
                sender.advance_to(11).await?;
                released.notified().await;
                sender.send(10, vec![2]).await?;
                Ok(())
            }
        })
        .fold("sum", 0, |sum, value| sum + value)
        .for_each("release", move |_time, _value| release.notify_one());
    let handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let error = tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .unwrap()
        .unwrap_err();
    assert_late_data_error(error, 0);
}

#[tokio::test]
async fn high_index_input_failure_completes_all_join_modes_with_idle_sibling() {
    for staged in [false, true] {
        for join_mode in ["blocking", "wait", "async"] {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..Default::default()
            })
            .unwrap();
            let mut handle = rt
                .spawn_multi(
                    "multi-input",
                    2,
                    |builder: &mut DataflowBuilder<u64>| {
                        let input = builder.input::<i32>("data")?;
                        let input = if staged {
                            input.exchange_by_hash_to("gather", 1, |_| 0)?
                        } else {
                            input
                        };
                        input.for_each("sink", |_t, _value| {});
                        Ok(())
                    },
                    SpawnOptions::new()
                        .auto_parallelism(false)
                        .per_stage_parallelism(staged),
                )
                .unwrap();
            let cancels: Vec<_> = (0..2)
                .map(|index| handle.worker_mut(index).cancel_token().clone())
                .collect();
            let held_input = handle.take_input::<i32>(0, "data").unwrap();
            let failing_input = handle.take_input::<i32>(1, "data").unwrap();
            failing_input.advance_to(11).unwrap();
            failing_input.send(10, vec![1]).unwrap();
            drop(failing_input);
            let mut join = match join_mode {
                "blocking" => tokio::task::spawn_blocking(move || handle.join_blocking()),
                "wait" => tokio::task::spawn_blocking(move || handle.join().wait()),
                _ => tokio::spawn(handle.join()),
            };
            let result = tokio::time::timeout(TIMEOUT, &mut join).await;
            // Clean up even on regression: never leave a blocked join on Tokio's blocking pool.
            if result.is_err() {
                for cancel in &cancels {
                    cancel.cancel();
                }
                drop(held_input);
                let _ = tokio::time::timeout(TIMEOUT, join).await;
                panic!("join {join_mode} hung with staged={staged}");
            }
            let error = result.unwrap().unwrap().unwrap_err();
            assert_late_data_error(error, 1);
            drop(held_input);
        }
    }
}

#[tokio::test]
async fn source_failure_cancels_pending_producer_without_stopping_shared_runtime() {
    use std::sync::Arc;

    struct ProducerDrop(Arc<tokio::sync::Notify>);
    impl Drop for ProducerDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let dropped = Arc::new(tokio::sync::Notify::new());
    let producer_dropped = dropped.clone();
    let builder = DataflowBuilder::<u64>::new("pending-producer");
    builder
        .source_async("data", move |sender| {
            let producer_dropped = producer_dropped.clone();
            async move {
                let _drop_signal = ProducerDrop(producer_dropped);
                sender.advance_to(11).await?;
                sender.send(10, vec![1i32]).await?;
                std::future::pending::<instancy::Result<()>>().await
            }
        })
        .for_each("sink", |_time, _value| {});
    let handle = rt
        .spawn(builder.build().unwrap(), SpawnOptions::new())
        .unwrap();
    let cancel = handle.cancel_token().clone();
    let error = tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .unwrap()
        .unwrap_err();
    assert_late_data_error(error, 0);
    let cleanup = tokio::time::timeout(TIMEOUT, dropped.notified()).await;
    if cleanup.is_err() {
        cancel.cancel();
        tokio::time::timeout(TIMEOUT, dropped.notified())
            .await
            .unwrap();
    }
    assert!(
        cleanup.is_ok(),
        "execution failure must cancel a producer waiting outside send()"
    );
    assert!(!rt.is_shutdown());
    let next = DataflowBuilder::<u64>::new("still-usable");
    next.source("data", vec![(0, vec![1i32])])
        .for_each("sink", |_t, _v| {});
    tokio::time::timeout(
        TIMEOUT,
        rt.spawn(next.build().unwrap(), SpawnOptions::new())
            .unwrap()
            .join(),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn assert_sibling_cancelled_before_join(mut handle: instancy::MultiSpawnedDataflow<u64>) {
    let sibling = handle.worker_mut(0).cancel_token().clone();
    let held_input = handle.take_input::<i32>(0, "data").unwrap();
    let failing_input = handle.take_input::<i32>(1, "data").unwrap();
    failing_input.advance_to(11).unwrap();
    failing_input.send(10, vec![1]).unwrap();
    drop(failing_input);
    let cancelled = tokio::time::timeout(TIMEOUT, sibling.cancelled_async()).await;
    if cancelled.is_err() {
        handle.cancel();
    }
    let error = tokio::time::timeout(TIMEOUT, handle.join())
        .await
        .unwrap()
        .unwrap_err();
    drop(held_input);
    assert_late_data_error(error, 1);
    assert!(
        cancelled.is_ok(),
        "a worker error must stop siblings before aggregate join is polled"
    );
}

#[tokio::test]
async fn heterogeneous_stage_failure_cancels_sibling_without_join_polling() {
    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..Default::default()
    })
    .unwrap();
    let build = |builder: &mut DataflowBuilder<u64>| -> instancy::Result<()> {
        builder
            .input::<i32>("data")?
            .exchange_by_hash_to("gather", 1, |_| 0)?
            .for_each("sink", |_t, _v| {});
        Ok(())
    };
    let mut probe = DataflowBuilder::new("staged-shape");
    build(&mut probe).unwrap();
    let shape = probe.build().unwrap();
    assert_eq!(
        shape
            .stages()
            .iter()
            .map(|stage| stage.parallelism.unwrap_or(2))
            .collect::<Vec<_>>(),
        vec![2, 1],
        "this regression must not fall back to uniform-worker execution"
    );
    let handle = rt
        .spawn_multi(
            "staged-failure",
            2,
            build,
            SpawnOptions::new()
                .auto_parallelism(false)
                .per_stage_parallelism(true),
        )
        .unwrap();
    assert_sibling_cancelled_before_join(handle).await;
    assert!(!rt.is_shutdown());
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn dedicated_worker_failure_cancels_sibling_without_join_polling() {
    let rt = instancy::SimpleRuntime::new();
    let handle = rt
        .spawn_multi(
            "dedicated-failure",
            2,
            |builder: &mut DataflowBuilder<u64>| {
                builder
                    .input::<i32>("data")?
                    .exchange_by_hash("exchange", |v| *v as u64)
                    .for_each("sink", |_t, _v| {});
                Ok(())
            },
        )
        .unwrap();
    assert_sibling_cancelled_before_join(handle).await;
    assert!(!rt.cancel_token().is_cancelled());
}
