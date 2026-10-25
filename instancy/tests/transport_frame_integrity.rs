//! Regression tests for wire-frame integrity and reserved channel ids.

#![cfg(feature = "transport")]

use std::sync::Arc;
use std::time::Duration;

use instancy::DataflowId;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport::{Frame, FramedReader, FramedWriter};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};

async fn crc_wire(frame: &Frame) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut w = FramedWriter::new(&mut wire);
    w.write_frame(frame).await.unwrap();
    wire
}

/// With CRC enabled the reader must reject a frame whose header was corrupted.
/// Previously the CRC covered only the payload, so a flipped `channel_id` bit
/// silently misrouted the frame.
#[tokio::test]
async fn crc_detects_corrupted_channel_id() {
    let frame = Frame {
        dataflow_id: DataflowId::new(),
        channel_id: 1,
        payload: b"hello".to_vec(),
    };
    let mut wire = crc_wire(&frame).await;
    wire[16] ^= 0x02; // channel_id occupies header bytes 16..24
    let got = FramedReader::new(&wire[..]).read_frame().await;
    assert!(
        got.is_err(),
        "corrupted channel_id accepted: delivered on channel {:?}",
        got.as_ref().map(|f| f.channel_id)
    );
}

#[tokio::test]
async fn crc_detects_corrupted_dataflow_id() {
    let frame = Frame {
        dataflow_id: DataflowId::new(),
        channel_id: 7,
        payload: b"payload".to_vec(),
    };
    let mut wire = crc_wire(&frame).await;
    wire[3] ^= 0x80; // dataflow_id occupies header bytes 0..16
    let got = FramedReader::new(&wire[..]).read_frame().await;
    assert!(got.is_err(), "corrupted dataflow_id accepted: {got:?}");
}

/// A flipped length bit must be detected before the reader consumes the
/// wrong number of bytes (which would desynchronise the stream).
#[tokio::test]
async fn crc_detects_corrupted_length() {
    let frame = Frame {
        dataflow_id: DataflowId::new(),
        channel_id: 7,
        payload: vec![0xAB; 64],
    };
    let mut wire = crc_wire(&frame).await;
    // Append a second, valid frame so a desynchronised reader has bytes to eat.
    wire.extend(crc_wire(&frame).await);
    wire[24] ^= 0x01; // length occupies header bytes 24..28
    let got = tokio::time::timeout(
        Duration::from_secs(1),
        FramedReader::new(&wire[..]).read_frame(),
    )
    .await
    .expect("reader blocked on a corrupted length");
    assert!(got.is_err(), "corrupted length accepted: {got:?}");
}

/// Uncorrupted CRC frames still round-trip, including empty payloads.
#[tokio::test]
async fn crc_frames_round_trip() {
    for payload in [Vec::new(), b"x".to_vec(), vec![7u8; 4096]] {
        let frame = Frame {
            dataflow_id: DataflowId::new(),
            channel_id: u64::MAX - 3,
            payload,
        };
        let wire = crc_wire(&frame).await;
        let got = FramedReader::new(&wire[..]).read_frame().await.unwrap();
        assert_eq!(got, frame);
    }
}

struct Echo;
impl ConnectionFactory for Echo {
    type Reader = ReadHalf<DuplexStream>;
    type Writer = WriteHalf<DuplexStream>;
    async fn establish(
        &self,
        _p: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        let (m, r) = tokio::io::duplex(1 << 16);
        let (mut rr, mut rw) = tokio::io::split(r);
        tokio::spawn(async move {
            let mut b = vec![0u8; 1 << 16];
            while let Ok(n) = rr.read(&mut b).await {
                if n == 0 || rw.write_all(&b[..n]).await.is_err() {
                    break;
                }
            }
        });
        Ok(tokio::io::split(m))
    }
}

/// Channel 0 is the unsequenced control channel. A payload frame sent on it
/// through the sequenced payload lane used to consume a sequence number that
/// the receiver never saw (the reader treats channel 0 as control), so every
/// later frame of the dataflow was stuck behind a permanent gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_on_control_channel_does_not_break_sequenced_lane() {
    let cfg = SharedConnectionConfig {
        probe_interval: Duration::from_secs(3600),
        ..SharedConnectionConfig::default()
    };
    let f: Arc<dyn DynConnectionFactory> = Arc::new(Echo);
    let mgr = SharedPeerManager::new(
        "echo".into(),
        cfg,
        f,
        &tokio::runtime::Handle::current(),
        tokio::sync::broadcast::channel(4).0,
    )
    .unwrap();
    let df = DataflowId::new();
    let (mut rxs, _err) = mgr.register_dataflow(df, &[1], 16).await.unwrap();
    let tx = mgr.payload_sender().clone();
    let _ = tx
        .send((
            df,
            Frame {
                dataflow_id: df,
                channel_id: 0,
                payload: b"P".to_vec(),
            },
        ))
        .await;
    tx.send((
        df,
        Frame {
            dataflow_id: df,
            channel_id: 1,
            payload: b"D".to_vec(),
        },
    ))
    .await
    .unwrap();
    let ch1 = tokio::time::timeout(Duration::from_secs(2), rxs.get_mut(&1).unwrap().recv())
        .await
        .ok()
        .flatten();
    assert_eq!(
        ch1.as_deref(),
        Some(&b"D"[..]),
        "frame on channel 1 stuck behind a sequence gap"
    );
}
