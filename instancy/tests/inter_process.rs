//! Integration tests for inter-process dataflow transport.
//!
//! These tests exercise the full pipeline:
//! - DataflowId allocation and uniqueness
//! - DataflowSession channel wiring (local vs remote)
//! - RemotePush serialization and backpressure
//! - ProgressExchange broadcast and decode roundtrip
//! - Dataflow isolation on shared connections
//! - End-to-end frame delivery through muxer/demuxer

use std::sync::Arc;

use instancy::communication::codec::{Codec, CodecError};
use instancy::communication::interprocess::PROGRESS_CHANNEL_ID;
use instancy::communication::remote_push::{FrameSender, OutboundFrame, RemotePush};
use instancy::communication::session::DataflowSession;
use instancy::communication::progress_exchange::{PeerProgressSender, ProgressExchange};
use instancy::dataflow::id::{DataflowId, DataflowIdAllocator};
use instancy::execute::{ClusterTopology, NodeConfig};
use instancy::providers::transport::{InMemoryClusterTransport, PushEndpoint, TransportProvider};
use instancy::worker::WorkerId;

/// A test codec for (u64, Vec<u32>) — timestamp + data batch.
#[derive(Clone)]
struct TestBatchCodec;

impl Codec<(u64, Vec<u32>)> for TestBatchCodec {
    fn encode(&self, value: &(u64, Vec<u32>), buf: &mut Vec<u8>) -> Result<(), CodecError> {
        let (time, data) = value;
        buf.extend_from_slice(&time.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        for d in data {
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<((u64, Vec<u32>), usize), CodecError> {
        if buf.len() < 12 {
            return Err(CodecError::InsufficientData {
                needed: 12,
                available: buf.len(),
            });
        }
        let time = u64::from_le_bytes(buf[..8].try_into().unwrap());
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        let needed = 12 + count * 4;
        if buf.len() < needed {
            return Err(CodecError::InsufficientData {
                needed,
                available: buf.len(),
            });
        }
        let mut data = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 12 + i * 4;
            data.push(u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()));
        }
        Ok(((time, data), needed))
    }
}

/// Simple u64 timestamp codec.
struct U64Codec;

impl Codec<u64> for U64Codec {
    fn encode(&self, value: &u64, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(u64, usize), CodecError> {
        if buf.len() < 8 {
            return Err(CodecError::InsufficientData {
                needed: 8,
                available: buf.len(),
            });
        }
        Ok((u64::from_le_bytes(buf[..8].try_into().unwrap()), 8))
    }
}

fn two_node_topology() -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new(0, 2), // node 0: workers 0, 1
        NodeConfig::new(1, 2), // node 1: workers 2, 3
    ])
    .unwrap()
}

// ─── DataflowId uniqueness tests ───────────────────────────────────────────

#[test]
fn dataflow_id_uniqueness_across_nodes() {
    let alloc_node0 = DataflowIdAllocator::new(0);
    let alloc_node1 = DataflowIdAllocator::new(1);
    let alloc_node2 = DataflowIdAllocator::new(2);

    // Generate IDs from each node
    let ids0: Vec<_> = (0..100).map(|_| alloc_node0.allocate()).collect();
    let ids1: Vec<_> = (0..100).map(|_| alloc_node1.allocate()).collect();
    let ids2: Vec<_> = (0..100).map(|_| alloc_node2.allocate()).collect();

    // No overlap
    let mut all: std::collections::HashSet<DataflowId> = std::collections::HashSet::new();
    for id in ids0.iter().chain(ids1.iter()).chain(ids2.iter()) {
        assert!(all.insert(*id), "duplicate DataflowId across nodes: {id:?}");
    }
    assert_eq!(all.len(), 300);
}

#[test]
fn dataflow_id_encoding_preserves_node_index() {
    let alloc = DataflowIdAllocator::new(42);
    for _ in 0..50 {
        let id = alloc.allocate();
        assert_eq!(id.node_index(), 42);
    }
}

// ─── DataflowSession wiring tests ──────────────────────────────────────────

#[test]
fn session_wires_local_and_remote_channels() {
    let topo = two_node_topology();
    let session = DataflowSession::new(DataflowId::new(0, 1), topo, 0);

    // Local: both workers on node 0
    let ch_local = session.allocate_channel(WorkerId::new(0), WorkerId::new(1));
    assert!(ch_local.is_local);

    // Remote: worker 0 (node 0) → worker 2 (node 1)
    let ch_remote = session.allocate_channel(WorkerId::new(0), WorkerId::new(2));
    assert!(!ch_remote.is_local);

    // Channel IDs are distinct
    assert_ne!(ch_local.channel_id, ch_remote.channel_id);
}

#[test]
fn session_remote_channels_lists_only_cross_node() {
    let topo = two_node_topology();
    let session = DataflowSession::new(DataflowId::new(0, 1), topo, 0);

    session.allocate_channel(WorkerId::new(0), WorkerId::new(1)); // local
    session.allocate_channel(WorkerId::new(0), WorkerId::new(2)); // remote
    session.allocate_channel(WorkerId::new(1), WorkerId::new(3)); // remote

    let remote = session.remote_channels();
    assert_eq!(remote.len(), 2);
    for ch in &remote {
        assert!(!ch.is_local);
    }
}

// ─── RemotePush delivery tests ─────────────────────────────────────────────

#[test]
fn remote_push_routes_data_to_correct_channel() {
    let (sender, receiver) = FrameSender::channel(32);
    let codec = Arc::new(TestBatchCodec);
    let dataflow_id = DataflowId::new(0, 1);

    let push = RemotePush::<u64, u32, (), TestBatchCodec>::new(
        dataflow_id,
        7, // channel_id
        codec,
        sender,
    );

    use instancy::dataflow::channels::envelope::{Envelope, Payload};

    let envelope = Envelope {
        payload: Payload::Data {
            time: 100u64,
            data: vec![1, 2, 3],
        },
        metadata: (),
    };

    push.push(envelope).unwrap();

    let frame = receiver.recv().unwrap();
    assert_eq!(frame.dataflow_id, dataflow_id.as_raw());
    assert_eq!(frame.channel_id, 7);

    // Decode and verify
    let ((time, data), _) = TestBatchCodec.decode(&frame.payload).unwrap();
    assert_eq!(time, 100);
    assert_eq!(data, vec![1, 2, 3]);
}

#[test]
fn remote_push_backpressure_does_not_block() {
    let (sender, _receiver) = FrameSender::channel(1);
    let codec = Arc::new(TestBatchCodec);

    let push = RemotePush::<u64, u32, (), TestBatchCodec>::new(
        DataflowId::new(0, 1),
        1,
        codec,
        sender,
    );

    use instancy::dataflow::channels::envelope::{Envelope, Payload};
    let envelope = Envelope {
        payload: Payload::Data {
            time: 1u64,
            data: vec![42],
        },
        metadata: (),
    };

    // First push succeeds
    push.push(envelope.clone()).unwrap();

    // Second push returns backpressure (not blocking)
    let start = std::time::Instant::now();
    let result = push.push(envelope);
    let elapsed = start.elapsed();

    assert!(matches!(result, Err(instancy::error::Error::Backpressure)));
    assert!(elapsed.as_millis() < 100, "should not block");
}

// ─── Progress exchange tests ────────────────────────────────────────────────

#[test]
fn progress_broadcast_and_decode_roundtrip() {
    let dataflow_id = DataflowId::new(0, 1);
    let (sender, receiver) = PeerProgressSender::new(1, 64);

    let exchange = ProgressExchange::new(
        dataflow_id,
        0, // local node
        vec![sender],
        Arc::new(U64Codec),
    );

    // Broadcast changes
    let changes = vec![
        (0, 5u64, 1),   // op 0, time 5, +1 capability
        (2, 10u64, -1), // op 2, time 10, -1 capability
    ];
    let accepted = exchange.broadcast_local_changes(&changes).unwrap();
    assert_eq!(accepted, 1);

    // Receive and decode
    let frame = receiver.recv().unwrap();
    assert_eq!(frame.channel_id, PROGRESS_CHANNEL_ID);
    assert_eq!(frame.dataflow_id, dataflow_id.as_raw());

    let msg = exchange.decode_remote_progress(&frame.payload).unwrap();
    assert_eq!(msg.source_node, 0);
    assert_eq!(msg.changes, changes);
}

#[test]
fn progress_frontier_advances_across_simulated_nodes() {
    // Simulate: node 0 sends progress to node 1, node 1 decodes it
    let dataflow_id = DataflowId::new(0, 1);

    // Node 0's exchange
    let (sender_to_1, receiver_at_1) = PeerProgressSender::new(1, 64);
    let node0_exchange = ProgressExchange::new(
        dataflow_id,
        0,
        vec![sender_to_1],
        Arc::new(U64Codec),
    );

    // Node 1's exchange (for decoding)
    let node1_exchange: ProgressExchange<u64, U64Codec> = ProgressExchange::new(
        dataflow_id,
        1,
        vec![],
        Arc::new(U64Codec),
    );

    // Node 0 reports: operator 0 advanced frontier to time 5
    let changes = vec![(0, 5u64, -1), (0, 10u64, 1)];
    node0_exchange.broadcast_local_changes(&changes).unwrap();

    // Node 1 receives
    let frame = receiver_at_1.recv().unwrap();
    let msg = node1_exchange.decode_remote_progress(&frame.payload).unwrap();

    assert_eq!(msg.source_node, 0);
    assert_eq!(msg.changes[0], (0, 5u64, -1)); // released time 5
    assert_eq!(msg.changes[1], (0, 10u64, 1)); // acquired time 10
}

// ─── Dataflow isolation tests ───────────────────────────────────────────────

#[test]
fn isolation_two_dataflows_share_connection_frames_routed_correctly() {
    // Two dataflows produce frames — they should have different dataflow_ids
    let alloc = DataflowIdAllocator::new(0);
    let id_a = alloc.allocate();
    let id_b = alloc.allocate();
    assert_ne!(id_a, id_b);

    // Both use channel_id=1 but different dataflow_ids
    let (sender, receiver) = FrameSender::channel(32);
    let codec = Arc::new(TestBatchCodec);

    let push_a = RemotePush::<u64, u32, (), TestBatchCodec>::new(
        id_a, 1, codec.clone(), sender.clone(),
    );
    let push_b = RemotePush::<u64, u32, (), TestBatchCodec>::new(
        id_b, 1, codec, sender,
    );

    use instancy::dataflow::channels::envelope::{Envelope, Payload};

    push_a
        .push(Envelope {
            payload: Payload::Data { time: 1, data: vec![100] },
            metadata: (),
        })
        .unwrap();

    push_b
        .push(Envelope {
            payload: Payload::Data { time: 2, data: vec![200] },
            metadata: (),
        })
        .unwrap();

    let frame1 = receiver.recv().unwrap();
    let frame2 = receiver.recv().unwrap();

    // Same channel_id, different dataflow_ids
    assert_eq!(frame1.channel_id, 1);
    assert_eq!(frame2.channel_id, 1);
    assert_eq!(frame1.dataflow_id, id_a.as_raw());
    assert_eq!(frame2.dataflow_id, id_b.as_raw());
    assert_ne!(frame1.dataflow_id, frame2.dataflow_id);
}

#[test]
fn isolation_cancelled_dataflow_frames_distinguishable() {
    // Frames from a cancelled dataflow still carry its DataflowId —
    // the demuxer can identify and drop them.
    let cancelled_id = DataflowId::new(0, 99);
    let active_id = DataflowId::new(0, 100);

    let frame_cancelled = OutboundFrame {
        dataflow_id: cancelled_id.as_raw(),
        channel_id: 5,
        payload: vec![1, 2, 3],
    };
    let frame_active = OutboundFrame {
        dataflow_id: active_id.as_raw(),
        channel_id: 5,
        payload: vec![4, 5, 6],
    };

    // A filter (simulating demuxer behavior) can distinguish them
    let active_set = std::collections::HashSet::from([active_id.as_raw()]);

    assert!(!active_set.contains(&frame_cancelled.dataflow_id));
    assert!(active_set.contains(&frame_active.dataflow_id));
}

// ─── InMemoryClusterTransport locality tests ────────────────────────────────

#[test]
fn cluster_transport_local_vs_remote() {
    let topo = two_node_topology();
    let transport = InMemoryClusterTransport::new(topo, 0);

    use instancy::dataflow::region::RegionId;
    use instancy::providers::transport::LogicalTarget;

    let src = LogicalTarget {
        region: RegionId::new(0),
        worker: WorkerId::new(0),
        operator: 0,
        input_index: 0,
    };

    // Worker 1 is on same node (node 0)
    let local_target = LogicalTarget {
        region: RegionId::new(0),
        worker: WorkerId::new(1),
        operator: 1,
        input_index: 0,
    };
    assert!(transport.is_local(&src, &local_target));

    // Worker 2 is on node 1 — remote
    let remote_target = LogicalTarget {
        region: RegionId::new(0),
        worker: WorkerId::new(2),
        operator: 0,
        input_index: 0,
    };
    assert!(!transport.is_local(&src, &remote_target));
}

// ─── End-to-end: simulated two-process data exchange ────────────────────────

#[cfg(feature = "transport")]
#[tokio::test]
async fn end_to_end_frame_delivery_through_muxer_demuxer() {
    use instancy::communication::{DemuxConfig, Demuxer, MuxConfig, Muxer};
    use tokio::io::duplex;

    let dataflow_id = DataflowId::new(0, 1);

    // Create a bidirectional connection
    let (node0_write, node1_read) = duplex(65536);

    // Node 0: muxer writes frames
    let (muxer, mux_sender) = Muxer::new(node0_write, MuxConfig { buffer_size: 64 });

    // Node 1: demuxer reads frames
    let config = DemuxConfig { channel_buffer: 32 };
    let mut demuxer = Demuxer::new(node1_read, config);
    let mut rx_data = demuxer.register_channel(dataflow_id.as_raw(), 1);
    let mut rx_progress = demuxer.register_channel(dataflow_id.as_raw(), PROGRESS_CHANNEL_ID);

    let mux_handle = tokio::spawn(async move { muxer.run().await });
    let demux_handle = tokio::spawn(async move { demuxer.run().await });

    // Send a data frame and a progress frame
    mux_sender
        .send_payload(dataflow_id.as_raw(), 1, b"data-payload".to_vec())
        .await
        .unwrap();
    mux_sender
        .send_payload(
            dataflow_id.as_raw(),
            PROGRESS_CHANNEL_ID,
            b"progress-payload".to_vec(),
        )
        .await
        .unwrap();

    // Verify data arrives at correct receivers
    let data = rx_data.recv().await.unwrap();
    assert_eq!(data, b"data-payload");

    let progress = rx_progress.recv().await.unwrap();
    assert_eq!(progress, b"progress-payload");

    // Shutdown
    drop(mux_sender);
    mux_handle.await.unwrap().unwrap();
    demux_handle.await.unwrap().unwrap();
}

#[cfg(feature = "transport")]
#[tokio::test]
async fn end_to_end_two_dataflows_isolated_on_same_connection() {
    use instancy::communication::{DemuxConfig, Demuxer, MuxConfig, Muxer};
    use tokio::io::duplex;

    let id_a = DataflowId::new(0, 1);
    let id_b = DataflowId::new(0, 2);

    let (node0_write, node1_read) = duplex(65536);

    let (muxer, mux_sender) = Muxer::new(node0_write, MuxConfig { buffer_size: 64 });
    let config = DemuxConfig { channel_buffer: 32 };
    let mut demuxer = Demuxer::new(node1_read, config);

    // Register same channel_id (1) for two different dataflows
    let mut rx_a = demuxer.register_channel(id_a.as_raw(), 1);
    let mut rx_b = demuxer.register_channel(id_b.as_raw(), 1);

    let mux_handle = tokio::spawn(async move { muxer.run().await });
    let demux_handle = tokio::spawn(async move { demuxer.run().await });

    // Send frames for both dataflows on channel_id=1
    mux_sender
        .send_payload(id_a.as_raw(), 1, b"for-A".to_vec())
        .await
        .unwrap();
    mux_sender
        .send_payload(id_b.as_raw(), 1, b"for-B".to_vec())
        .await
        .unwrap();

    // Each dataflow receives only its own frames
    let data_a = rx_a.recv().await.unwrap();
    assert_eq!(data_a, b"for-A");

    let data_b = rx_b.recv().await.unwrap();
    assert_eq!(data_b, b"for-B");

    drop(mux_sender);
    mux_handle.await.unwrap().unwrap();
    demux_handle.await.unwrap().unwrap();
}
