//! Example: End-to-end async dataflow execution with async I/O.
//!
//! Demonstrates the full async story:
//! - `spawn()` is sync (fast CPU work, no reason to be async)
//! - Input feeding uses `AsyncInputSender::send().await` (yields on backpressure)
//! - Output collection uses `AsyncOutputReceiver::recv().await` (yields waiting for data)
//! - Completion uses `DataflowCompletion.await` (real Future)
//!
//! ```text
//! [tokio async task]                     [instancy worker pool]
//!     │                                         │
//!     │── rt.spawn(df, SpawnOptions::new().io_mode(IoMode::Async))? ─►│ registers ExecutorTask
//!     │                                         │
//!     │── sender.send(data).await ────────────►│ WakeHandle notifies pool
//!     │   (yields on backpressure)               │     │
//!     │                                         │  map("double")
//!     │                                         │     │
//!     │◄── receiver.recv().await ──────────────│ ChannelSink → tokio channel
//!     │   (yields waiting for data)              │
//!     │                                         │
//!     │── handle.join().await ─────────────────►│ DataflowCompletion future
//!     │◄── Ok(()) ─────────────────────────────│
//! ```
//!
//! Run with: `cargo run --all-features --example async_io_channels`

use instancy::DataflowBuilder;
use instancy::{IoMode, RuntimeConfig, RuntimeHandle, SpawnOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("=== Async I/O Example ===\n");

    let rt = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: None,
        name: "async-demo".into(),
        ..Default::default()
    })
    .expect("failed to create runtime");

    // --- Pipeline 1: async input/output with async SpawnOptions ---

    let builder = DataflowBuilder::<u64>::new("double-pipeline");
    let input = builder.input::<i32>("numbers").unwrap();
    input
        .map("double", |_t, x| x * 2)
        .filter("positive", |_t, x| x > &0)
        .output("results")
        .unwrap();
    let dataflow = builder.build().expect("build failed");

    // Async SpawnOptions wires tokio::sync::mpsc channels for I/O
    let mut handle = rt
        .spawn(dataflow, SpawnOptions::new().io_mode(IoMode::Async))
        .expect("spawn failed");

    // Async input: yields on backpressure instead of blocking
    let sender = handle
        .take_async_input::<i32>("numbers")
        .expect("input port");

    // Async output: yields waiting for data instead of blocking
    let mut receiver = handle
        .take_async_output::<i32>("results")
        .expect("output port");

    // Producer task: feeds data asynchronously
    let producer = tokio::spawn(async move {
        sender.send(0, vec![-1, 2, 3, -4, 5]).await.unwrap();
        sender.send(1, vec![10, 20]).await.unwrap();
        sender.close();
    });

    // Consumer task: collects results asynchronously
    let consumer = tokio::spawn(async move {
        let results = receiver.collect_data().await;
        println!("Pipeline 1 results (async I/O):");
        for (time, data) in &results {
            println!("  t={time}: {data:?}");
        }
    });

    // Await producer + consumer, then await dataflow completion
    producer.await.expect("producer task failed");
    consumer.await.expect("consumer task failed");
    handle.join().await.expect("pipeline 1 failed");
    println!("Pipeline 1 completed\n");

    // --- Pipeline 2: sync spawn + async completion ---

    let builder = DataflowBuilder::<u64>::new("squares");
    let out = builder
        .source("data", vec![(0u64, vec![1i32, 2, 3, 4, 5])])
        .map("square", |_t, x| x * x)
        .output("results")
        .unwrap();
    let dataflow = builder.build().expect("build failed");

    // spawn() is sync — join() returns DataflowCompletion which IS a real Future
    let completion = rt
        .spawn(dataflow, SpawnOptions::default())
        .expect("run failed")
        .join();
    completion.await.expect("pipeline 2 failed");

    let collector = out.collector();
    {
        let data = collector.lock().unwrap();
        println!("Pipeline 2 results (sync spawn + async await):");
        for (time, vals) in data.iter() {
            println!("  t={time}: {vals:?}");
        }
    }
    println!("Pipeline 2 completed\n");

    // --- Pipeline 3: concurrent async completions ---

    println!("Spawning 3 concurrent dataflows...");
    let mut completions = Vec::new();
    for i in 0..3 {
        let builder = DataflowBuilder::<u64>::new(format!("concurrent_{i}"));
        builder
            .source("src", vec![(0u64, vec![i * 10 + 1, i * 10 + 2])])
            .map("inc", |_t, x| x + 100)
            .output("out")
            .unwrap();
        let dataflow = builder.build().expect("build failed");
        // spawn() returns sync, join() yields a real Future
        completions.push(
            rt.spawn(dataflow, SpawnOptions::default())
                .expect("run failed")
                .join(),
        );
    }

    // Await all three concurrently via tokio::join!
    let mut drain = completions.into_iter();
    let c0 = drain.next().unwrap();
    let c1 = drain.next().unwrap();
    let c2 = drain.next().unwrap();
    let (r0, r1, r2) = tokio::join!(c0, c1, c2);
    r0.expect("concurrent_0 failed");
    r1.expect("concurrent_1 failed");
    r2.expect("concurrent_2 failed");
    println!("All 3 concurrent dataflows completed via tokio::join!\n");

    println!("=== Done ===");
}
