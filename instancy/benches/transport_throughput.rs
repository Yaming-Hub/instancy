//! Throughput and latency benchmarks comparing dedicated vs shared transport.
//!
//! Each benchmark uses in-memory `tokio::io::DuplexStream` pairs to isolate
//! transport overhead from actual network I/O.

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::SharedPeerManager;
use instancy::communication::transport::Frame;
use instancy::communication::transport_session::{
    ChannelRegistration, PeerConnection, TransportSession,
};
use instancy::dataflow::id::DataflowId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const PAYLOAD_SIZE: usize = 256;
const CHANNEL_ID: u64 = 1;

fn make_payload(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

fn make_frame(dataflow_id: DataflowId, payload: &[u8]) -> Frame {
    Frame {
        dataflow_id,
        channel_id: CHANNEL_ID,
        payload: payload.to_vec(),
    }
}

fn shared_config(num_connections: usize) -> SharedConnectionConfig {
    SharedConnectionConfig {
        min_connections: num_connections,
        max_connections: num_connections,
        probe_interval: Duration::from_secs(3600), // disable probing during bench
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(60),
        rtt_ema_alpha: 0.2,
        idle_timeout: None, // disable idle cleanup during bench
    }
}

// ---------------------------------------------------------------------------
// Dedicated transport benchmark (1 connection per dataflow)
// ---------------------------------------------------------------------------

fn bench_dedicated_single_dataflow(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("dedicated_single_dataflow");

    for &msg_count in &[100u64, 1000, 10000] {
        group.throughput(Throughput::Elements(msg_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(msg_count),
            &msg_count,
            |b, &count| {
                b.to_async(&rt).iter(|| async {
                    let dataflow_id = DataflowId::new();
                    // Two duplex streams: one for each direction
                    // Stream A: local writes → remote reads (sender side)
                    // Stream B: remote writes → local reads (receiver side)
                    // For loopback: we echo A's remote end back to B's remote end
                    let (local_write, remote_read) = tokio::io::duplex(1024 * 1024);
                    let (remote_write, local_read) = tokio::io::duplex(1024 * 1024);

                    // Echo task: read frames from remote_read and write them to remote_write
                    let echo_handle = tokio::spawn(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        let (mut reader, mut writer) = (remote_read, remote_write);
                        loop {
                            match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    if tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
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

                    let data_channels = vec![ChannelRegistration {
                        peer_node_id: "peer".to_string(),
                        channel_id: CHANNEL_ID,
                    }];

                    let conn = PeerConnection {
                        node_id: "peer".to_string(),
                        reader: local_read,
                        writer: local_write,
                    };

                    let handle = tokio::runtime::Handle::current();
                    let (session, mut receivers) = TransportSession::new(
                        dataflow_id,
                        vec![conn],
                        &data_channels,
                        &[],
                        4096,
                        &handle,
                    );

                    let peer_rxs = receivers.remove("peer").unwrap();
                    let mut data_rx = peer_rxs
                        .into_iter()
                        .find(|(id, _)| *id == CHANNEL_ID)
                        .map(|(_, rx)| rx)
                        .unwrap();

                    let payload = make_payload(PAYLOAD_SIZE);
                    let sender = session.data_sender("peer").unwrap().clone();

                    // Send all messages
                    let send_task = tokio::spawn(async move {
                        for _ in 0..count {
                            let frame = make_frame(dataflow_id, &payload);
                            sender.send(frame).await.unwrap();
                        }
                    });

                    // Receive all messages
                    let mut received = 0u64;
                    while received < count {
                        if data_rx.recv().await.is_some() {
                            received += 1;
                        } else {
                            break;
                        }
                    }

                    send_task.await.unwrap();
                    drop(session); // drop after receiving to keep Demuxer alive
                    echo_handle.abort();
                    assert_eq!(received, count);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Shared transport benchmark (1 connection shared by N dataflows)
// ---------------------------------------------------------------------------

fn bench_shared_single_dataflow(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("shared_single_dataflow");

    for &msg_count in &[100u64, 1000, 10000] {
        group.throughput(Throughput::Elements(msg_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(msg_count),
            &msg_count,
            |b, &count| {
                b.to_async(&rt).iter(|| async {
                    let dataflow_id = DataflowId::new();
                    // Two duplex streams for loopback
                    let (local_write, remote_read) = tokio::io::duplex(1024 * 1024);
                    let (remote_write, local_read) = tokio::io::duplex(1024 * 1024);

                    // Echo task: forward frames from remote_read → remote_write
                    let echo_handle = tokio::spawn(async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        let (mut reader, mut writer) = (remote_read, remote_write);
                        loop {
                            match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    if tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n])
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

                    let handle = tokio::runtime::Handle::current();

                    // SharedPeerManager takes (Reader, Writer) — reader for incoming, writer for outgoing
                    let manager = SharedPeerManager::new(
                        "peer".to_string(),
                        shared_config(1),
                        vec![(local_read, local_write)],
                        None,
                        &handle,
                    ).unwrap();

                    let (mut receivers, _error_rx) = manager
                        .register_dataflow(dataflow_id, &[CHANNEL_ID], 4096)
                        .await;

                    let mut data_rx = receivers.remove(&CHANNEL_ID).unwrap();
                    let payload_tx = manager.payload_sender().clone();

                    let payload = make_payload(PAYLOAD_SIZE);

                    // Send all messages
                    let send_task = tokio::spawn(async move {
                        for _ in 0..count {
                            let frame = make_frame(dataflow_id, &payload);
                            payload_tx.send((dataflow_id, frame)).await.unwrap();
                        }
                    });

                    // Receive all messages
                    let mut received = 0u64;
                    while received < count {
                        if data_rx.recv().await.is_some() {
                            received += 1;
                        } else {
                            break;
                        }
                    }

                    send_task.await.unwrap();
                    manager.unregister_dataflow(&dataflow_id).await;
                    drop(manager);
                    echo_handle.abort();
                    assert_eq!(received, count);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Multi-dataflow contention benchmark: shared vs N dedicated
// ---------------------------------------------------------------------------

fn bench_dedicated_multi_dataflow(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("dedicated_multi_dataflow");

    for &num_dataflows in &[2u64, 4, 8] {
        let total_msgs = 1000u64 * num_dataflows;
        group.throughput(Throughput::Elements(total_msgs));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_dataflows),
            &num_dataflows,
            |b, &n_df| {
                b.to_async(&rt).iter(|| async {
                    let msgs_per_df = 1000u64;
                    let handle = tokio::runtime::Handle::current();
                    let mut send_tasks = Vec::new();
                    let mut recv_tasks = Vec::new();
                    let mut echo_handles = Vec::new();

                    for _ in 0..n_df {
                        let dataflow_id = DataflowId::new();
                        let (local_write, remote_read) = tokio::io::duplex(1024 * 1024);
                        let (remote_write, local_read) = tokio::io::duplex(1024 * 1024);

                        echo_handles.push(tokio::spawn(async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            let (mut reader, mut writer) = (remote_read, remote_write);
                            loop {
                                match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        if tokio::io::AsyncWriteExt::write_all(
                                            &mut writer,
                                            &buf[..n],
                                        )
                                        .await
                                        .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }));

                        let data_channels = vec![ChannelRegistration {
                            peer_node_id: "peer".to_string(),
                            channel_id: CHANNEL_ID,
                        }];

                        let conn = PeerConnection {
                            node_id: "peer".to_string(),
                            reader: local_read,
                            writer: local_write,
                        };

                        let (session, mut receivers) = TransportSession::new(
                            dataflow_id,
                            vec![conn],
                            &data_channels,
                            &[],
                            4096,
                            &handle,
                        );

                        let peer_rxs = receivers.remove("peer").unwrap();
                        let mut data_rx = peer_rxs
                            .into_iter()
                            .find(|(id, _)| *id == CHANNEL_ID)
                            .map(|(_, rx)| rx)
                            .unwrap();

                        let payload = make_payload(PAYLOAD_SIZE);
                        let sender = session.data_sender("peer").unwrap().clone();

                        send_tasks.push(tokio::spawn(async move {
                            for _ in 0..msgs_per_df {
                                let frame = make_frame(dataflow_id, &payload);
                                sender.send(frame).await.unwrap();
                            }
                        }));

                        recv_tasks.push(tokio::spawn(async move {
                            let mut received = 0u64;
                            while received < msgs_per_df {
                                if data_rx.recv().await.is_some() {
                                    received += 1;
                                } else {
                                    break;
                                }
                            }
                            // Keep session alive until receiving completes
                            drop(session);
                            received
                        }));
                    }

                    for t in send_tasks {
                        t.await.unwrap();
                    }
                    let mut total = 0u64;
                    for t in recv_tasks {
                        total += t.await.unwrap();
                    }
                    for h in echo_handles {
                        h.abort();
                    }
                    assert_eq!(total, msgs_per_df * n_df);
                });
            },
        );
    }
    group.finish();
}

fn bench_shared_multi_dataflow(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("shared_multi_dataflow");

    for &num_dataflows in &[2u64, 4, 8] {
        let total_msgs = 1000u64 * num_dataflows;
        group.throughput(Throughput::Elements(total_msgs));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_dataflows),
            &num_dataflows,
            |b, &n_df| {
                b.to_async(&rt).iter(|| async {
                    let msgs_per_df = 1000u64;
                    let handle = tokio::runtime::Handle::current();

                    // Create shared manager with same connection count as dedicated
                    // for a fair overhead comparison
                    let num_conns = n_df as usize;
                    let mut connections = Vec::new();
                    let mut server_writers = Vec::new();

                    // For shared transport, we need paired duplex streams:
                    // Writer side: manager writes to client_write → server reads from server_read
                    // Reader side: server writes to server_write → manager reads from client_read
                    // But SharedPeerManager takes (Reader, Writer) pairs for each connection.
                    // The reader reads incoming frames; the writer sends outgoing frames.
                    // We need a loop-back: writer output → reader input (for same-process bench).

                    for _ in 0..num_conns {
                        let (s1, s2) = tokio::io::duplex(1024 * 1024);
                        let (r1, w1) = tokio::io::split(s1);
                        let (r2, w2) = tokio::io::split(s2);
                        // Manager writes on w2, remote reads on r1
                        // Remote writes on w1, manager reads on r2
                        // For loopback: we need to forward r1 → w1 (echo)
                        connections.push((r2, w2));
                        server_writers.push((r1, w1));
                    }

                    // Echo tasks: forward frames back (simulates remote peer echoing)
                    let mut echo_handles = Vec::new();
                    for (mut reader, mut writer) in server_writers {
                        echo_handles.push(tokio::spawn(async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            loop {
                                match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        if tokio::io::AsyncWriteExt::write_all(
                                            &mut writer,
                                            &buf[..n],
                                        )
                                        .await
                                        .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }));
                    }

                    let manager = SharedPeerManager::new(
                        "peer".to_string(),
                        shared_config(num_conns),
                        connections,
                        None,
                        &handle,
                    ).unwrap();

                    let mut send_tasks = Vec::new();
                    let mut recv_tasks = Vec::new();
                    let mut dataflow_ids = Vec::new();

                    for _ in 0..n_df {
                        let dataflow_id = DataflowId::new();
                        dataflow_ids.push(dataflow_id);
                        let (mut receivers, _err_rx) = manager
                            .register_dataflow(dataflow_id, &[CHANNEL_ID], 4096)
                            .await;
                        let mut data_rx = receivers.remove(&CHANNEL_ID).unwrap();
                        let payload_tx = manager.payload_sender().clone();
                        let payload = make_payload(PAYLOAD_SIZE);

                        send_tasks.push(tokio::spawn(async move {
                            for _ in 0..msgs_per_df {
                                let frame = make_frame(dataflow_id, &payload);
                                payload_tx.send((dataflow_id, frame)).await.unwrap();
                            }
                        }));

                        recv_tasks.push(tokio::spawn(async move {
                            let mut received = 0u64;
                            while received < msgs_per_df {
                                if data_rx.recv().await.is_some() {
                                    received += 1;
                                } else {
                                    break;
                                }
                            }
                            received
                        }));
                    }

                    for t in send_tasks {
                        t.await.unwrap();
                    }
                    let mut total = 0u64;
                    for t in recv_tasks {
                        total += t.await.unwrap();
                    }
                    for df_id in &dataflow_ids {
                        manager.unregister_dataflow(df_id).await;
                    }
                    drop(manager);
                    for h in echo_handles {
                        h.abort();
                    }
                    assert_eq!(total, msgs_per_df * n_df);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Shared transport with varying connection counts
// ---------------------------------------------------------------------------

fn bench_shared_scaling_connections(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("shared_connection_scaling");

    for &num_conns in &[1usize, 2, 4] {
        let msg_count = 5000u64;
        group.throughput(Throughput::Elements(msg_count));
        group.bench_with_input(
            BenchmarkId::new("connections", num_conns),
            &num_conns,
            |b, &n_conns| {
                b.to_async(&rt).iter(|| async {
                    let dataflow_id = DataflowId::new();
                    let handle = tokio::runtime::Handle::current();

                    let mut connections = Vec::new();
                    let mut echo_handles = Vec::new();

                    for _ in 0..n_conns {
                        let (s1, s2) = tokio::io::duplex(1024 * 1024);
                        let (r1, w1) = tokio::io::split(s1);
                        let (r2, w2) = tokio::io::split(s2);
                        connections.push((r2, w2));
                        echo_handles.push(tokio::spawn(async move {
                            let mut buf = vec![0u8; 64 * 1024];
                            let (mut reader, mut writer) = (r1, w1);
                            loop {
                                match tokio::io::AsyncReadExt::read(&mut reader, &mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        if tokio::io::AsyncWriteExt::write_all(
                                            &mut writer,
                                            &buf[..n],
                                        )
                                        .await
                                        .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        }));
                    }

                    let manager = SharedPeerManager::new(
                        "peer".to_string(),
                        shared_config(n_conns),
                        connections,
                        None,
                        &handle,
                    ).unwrap();

                    let (mut receivers, _err_rx) = manager
                        .register_dataflow(dataflow_id, &[CHANNEL_ID], 4096)
                        .await;
                    let mut data_rx = receivers.remove(&CHANNEL_ID).unwrap();
                    let payload_tx = manager.payload_sender().clone();
                    let payload = make_payload(PAYLOAD_SIZE);

                    let send_task = tokio::spawn(async move {
                        for _ in 0..msg_count {
                            let frame = make_frame(dataflow_id, &payload);
                            payload_tx.send((dataflow_id, frame)).await.unwrap();
                        }
                    });

                    let mut received = 0u64;
                    while received < msg_count {
                        if data_rx.recv().await.is_some() {
                            received += 1;
                        } else {
                            break;
                        }
                    }

                    send_task.await.unwrap();
                    manager.unregister_dataflow(&dataflow_id).await;
                    drop(manager);
                    for h in echo_handles {
                        h.abort();
                    }
                    assert_eq!(received, msg_count);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_dedicated_single_dataflow,
    bench_shared_single_dataflow,
    bench_dedicated_multi_dataflow,
    bench_shared_multi_dataflow,
    bench_shared_scaling_connections,
);
criterion_main!(benches);
