//! # Shared Transport Example
//!
//! Demonstrates multiple dataflows sharing pooled connections via
//! `SharedPeerManager` and `SharedTransportSession`.
//!
//! Two dataflows run concurrently on a simulated two-node cluster, sharing
//! the same pooled TCP connections. This shows how the shared transport mode
//! reduces connection count while maintaining correct per-dataflow ordering.
//!
//! Run with: `cargo run --all-features --example cluster_shared_transport`

use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport::Frame;
use instancy::dataflow::id::DataflowId;

#[derive(Default)]
struct EchoConnectionFactory {
    remote_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl ConnectionFactory for EchoConnectionFactory {
    type Reader = tokio::io::ReadHalf<tokio::io::DuplexStream>;
    type Writer = tokio::io::WriteHalf<tokio::io::DuplexStream>;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>> {
        let (manager_stream, remote_stream) = tokio::io::duplex(256 * 1024);
        let (manager_read, manager_write) = tokio::io::split(manager_stream);
        let (mut remote_read, mut remote_write) = tokio::io::split(remote_stream);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match tokio::io::AsyncReadExt::read(&mut remote_read, &mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if tokio::io::AsyncWriteExt::write_all(&mut remote_write, &buf[..n])
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        self.remote_tasks
            .lock()
            .expect("echo factory task lock poisoned")
            .push(handle);
        Ok((manager_read, manager_write))
    }
}

#[tokio::main]
async fn main() {
    println!("=== cluster_shared_transport ===\n");
    println!("Demonstrating multiple dataflows sharing pooled connections.\n");

    let handle = tokio::runtime::Handle::current();

    // Configuration: 2 pooled connections, no scaling during this demo
    let config = SharedConnectionConfig {
        min_connections: 2,
        max_connections: 2,
        probe_interval: Duration::from_secs(3600),
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(5),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    };

    let factory = Arc::new(EchoConnectionFactory::default());
    let connection_factory: Arc<dyn DynConnectionFactory> = factory.clone();

    // Create shared peer manager for node-a → node-b
    let manager =
        SharedPeerManager::new("node-b".to_string(), config, connection_factory, &handle).unwrap();

    println!(
        "Created SharedPeerManager with {} eager connections to node-b\n",
        manager.connection_count().unwrap()
    );

    // Register two independent dataflows on the same pooled connections
    let dataflow_1 = DataflowId::new();
    let dataflow_2 = DataflowId::new();
    let channel_id = 1u64;

    let (mut receivers_1, mut err_rx_1) = manager
        .register_dataflow(dataflow_1, &[channel_id], 256)
        .await;
    let (mut receivers_2, mut err_rx_2) = manager
        .register_dataflow(dataflow_2, &[channel_id], 256)
        .await;

    let mut rx_1 = receivers_1.remove(&channel_id).unwrap();
    let mut rx_2 = receivers_2.remove(&channel_id).unwrap();

    // Monitor transport errors (demonstrates error handling pattern)
    tokio::spawn(async move {
        while let Some(err) = err_rx_1.recv().await {
            eprintln!("  [dataflow-1] transport error: {err:?}");
        }
    });
    tokio::spawn(async move {
        while let Some(err) = err_rx_2.recv().await {
            eprintln!("  [dataflow-2] transport error: {err:?}");
        }
    });

    println!("Registered dataflow-1: {dataflow_1}");
    println!("Registered dataflow-2: {dataflow_2}\n");

    // We'll use payload_sender directly for this demo
    let payload_tx = manager.payload_sender().clone();

    // Dataflow-1 sends messages tagged with "DF1"
    let tx1 = payload_tx.clone();
    let df1 = dataflow_1;
    let send_task_1 = tokio::spawn(async move {
        for i in 0..5u32 {
            let payload = format!("DF1-msg-{i}").into_bytes();
            let frame = Frame {
                dataflow_id: df1,
                channel_id,
                payload,
            };
            tx1.send((df1, frame)).await.unwrap();
        }
        println!("  [dataflow-1] sent 5 messages");
    });

    // Dataflow-2 sends messages tagged with "DF2"
    let tx2 = payload_tx.clone();
    let df2 = dataflow_2;
    let send_task_2 = tokio::spawn(async move {
        for i in 0..5u32 {
            let payload = format!("DF2-msg-{i}").into_bytes();
            let frame = Frame {
                dataflow_id: df2,
                channel_id,
                payload,
            };
            tx2.send((df2, frame)).await.unwrap();
        }
        println!("  [dataflow-2] sent 5 messages");
    });

    // Receive messages for each dataflow
    let recv_task_1 = tokio::spawn(async move {
        let mut messages = Vec::new();
        for _ in 0..5 {
            if let Some(data) = rx_1.recv().await {
                messages.push(String::from_utf8_lossy(&data).to_string());
            }
        }
        messages
    });

    let recv_task_2 = tokio::spawn(async move {
        let mut messages = Vec::new();
        for _ in 0..5 {
            if let Some(data) = rx_2.recv().await {
                messages.push(String::from_utf8_lossy(&data).to_string());
            }
        }
        messages
    });

    // Wait for all tasks
    send_task_1.await.unwrap();
    send_task_2.await.unwrap();
    let messages_1 = recv_task_1.await.unwrap();
    let messages_2 = recv_task_2.await.unwrap();

    println!("\n--- Results ---");
    println!("Dataflow-1 received {} messages:", messages_1.len());
    for msg in &messages_1 {
        println!("  {msg}");
    }
    println!("Dataflow-2 received {} messages:", messages_2.len());
    for msg in &messages_2 {
        println!("  {msg}");
    }

    // Verify correctness: each dataflow only receives its own messages
    assert_eq!(
        messages_1.len(),
        5,
        "dataflow-1 should receive exactly 5 messages"
    );
    assert_eq!(
        messages_2.len(),
        5,
        "dataflow-2 should receive exactly 5 messages"
    );
    for msg in &messages_1 {
        assert!(
            msg.starts_with("DF1-"),
            "dataflow-1 got wrong message: {msg}"
        );
    }
    for msg in &messages_2 {
        assert!(
            msg.starts_with("DF2-"),
            "dataflow-2 got wrong message: {msg}"
        );
    }

    // Verify ordering within each dataflow
    for (i, msg) in messages_1.iter().enumerate() {
        assert_eq!(*msg, format!("DF1-msg-{i}"), "dataflow-1 ordering violated");
    }
    for (i, msg) in messages_2.iter().enumerate() {
        assert_eq!(*msg, format!("DF2-msg-{i}"), "dataflow-2 ordering violated");
    }

    println!("\n✓ Both dataflows correctly received their messages in order");
    println!("✓ Messages were isolated (no cross-dataflow contamination)");
    println!(
        "✓ All data shared {} pooled connections",
        manager.connection_count().unwrap()
    );

    // Cleanup
    manager.unregister_dataflow(&dataflow_1).await;
    manager.unregister_dataflow(&dataflow_2).await;
    drop(manager);
    if let Ok(handles) = factory.remote_tasks.lock() {
        for handle in handles.iter() {
            handle.abort();
        }
    }

    println!("\n=== done ===");
}
