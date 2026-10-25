//! Audit: pipeline (non-exchange) in-flight data is invisible to progress
//! tracking, so a downstream notification can fire before all data for that
//! timestamp has reached the notifying operator.

use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn sum_per_epoch_notify(input: instancy::Pipe<u64, u64>) -> instancy::Pipe<u64, u64> {
    input.unary_notify("sum-per-epoch", {
        let mut stash: HashMap<u64, u64> = HashMap::new();
        move |input, output, ctx| {
            while let Some((time, data)) = input.next() {
                *stash.entry(time).or_default() += data.len() as u64;
                ctx.notify_at(time);
            }
            while let Some(time) = ctx.next_notification() {
                if let Some(count) = stash.remove(&time) {
                    output.push_vec(time, vec![count]);
                }
            }
            Ok(())
        }
    })
}

fn run(n: u64, cap: usize) -> Vec<(u64, Vec<u64>)> {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("pipeline-inflight");
    let data: Vec<(u64, Vec<u64>)> = (0..n).map(|i| (0u64, vec![i])).collect();
    let src = builder.source("src", data);
    let mapped = src.map("id", |_t, v| v).with_capacity(cap).unwrap();
    sum_per_epoch_notify(mapped).output("results").unwrap();
    let dataflow = builder.build().unwrap();
    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let collector = std::thread::spawn(move || receiver.collect_data());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(handle.join_blocking());
    });
    let res = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("dataflow hung");
    res.unwrap();
    collector.join().unwrap()
}

/// Expect exactly one notification output for epoch 0 carrying the total count.
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn notify_after_pipeline_backlog_fires_once_with_full_count() {
    let n = 3_000u64;
    let out = run(n, 1);
    let total: u64 = out.iter().flat_map(|(_, d)| d.iter()).sum();
    eprintln!(
        "outputs: {} batches, total={total}, first few: {:?}",
        out.len(),
        &out[..out.len().min(5)]
    );
    assert!(
        out.len() == 1 && out[0] == (0, vec![n]),
        "notification for epoch 0 fired before all epoch-0 data arrived: {} outputs for epoch 0, first = {:?}",
        out.len(),
        out.first()
    );
}

/// unary_async holds no capability while its tasks are in flight, so the
/// downstream frontier passes epoch 0 while epoch-0 results are still pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn notify_after_unary_async_fires_once_with_full_count() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("async-inflight");
    let input = builder.input::<u64>("data").unwrap();
    let slow = input.unary_async(
        "slow",
        4,
        std::sync::Arc::new(|_t: u64, data: Vec<u64>| async move {
            // Different batches finish at different times.
            tokio::time::sleep(Duration::from_millis(150 * data.len() as u64)).await;
            Ok(data)
        }),
    );
    sum_per_epoch_notify(slow).output("results").unwrap();
    let dataflow = builder.build().unwrap();
    let mut handle = rt.spawn(dataflow, SpawnOptions::new()).unwrap();
    let receiver = handle.take_output::<u64>("results").unwrap();
    let sender = handle.take_input::<u64>("data").unwrap();
    sender.send(0, vec![1]).unwrap();
    sender.send(0, vec![2, 3]).unwrap();
    sender.advance_to(1).unwrap();
    // Keep the input open while the async task is still running so that the
    // only thing holding epoch 0 open would be the in-flight async task.
    tokio::time::sleep(Duration::from_millis(600)).await;
    drop(sender);
    let res = tokio::time::timeout(Duration::from_secs(20), handle.join()).await;
    assert!(matches!(res, Ok(Ok(()))), "join: {res:?}");
    let out = tokio::task::spawn_blocking(move || receiver.collect_data())
        .await
        .unwrap();
    eprintln!("unary_async outputs: {out:?}");
    assert_eq!(
        out,
        vec![(0, vec![3])],
        "notification for epoch 0 fired while epoch-0 async results were in flight"
    );
}
