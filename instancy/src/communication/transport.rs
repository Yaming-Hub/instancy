//! Multiplexed framed communication over async byte streams.
//!
//! This module provides the wire protocol for exchanging data between processes
//! in a distributed dataflow. Connections carry multiple logical channels
//! (one per dataflow edge) multiplexed over a single byte stream.
//!
//! # Wire format
//!
//! Each frame on the wire has the following layout:
//!
//! ```text
//! ┌────────────────┬────────────────┬─────────────────────┐
//! │ channel_id: u64│ length: u32    │ payload: [u8; length]│
//! └────────────────┴────────────────┴─────────────────────┘
//! ```
//!
//! - `channel_id` (8 bytes, little-endian): identifies the logical channel
//! - `length` (4 bytes, little-endian): byte length of the payload (max 256 MB)
//! - `payload` (variable): serialized message data
//!
//! # Components
//!
//! - [`Frame`]: A single wire-protocol frame (channel + payload)
//! - [`FramedWriter`]: Writes frames to an `AsyncWrite` stream
//! - [`FramedReader`]: Reads frames from an `AsyncRead` stream
//! - [`Demuxer`]: Background task dispatching incoming frames to per-channel receivers
//! - [`Muxer`]: Collects frames from multiple channel senders and writes them out

use std::io;

use crate::communication::codec::MAX_MESSAGE_SIZE;
use crate::dataflow::id::DataflowId;
use crate::wire;

/// A single frame in the wire protocol.
///
/// Each frame carries a `dataflow_id` to isolate concurrent dataflows sharing
/// the same pooled connection, plus a `channel_id` for edge-level demuxing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Identifies which dataflow this frame belongs to (isolation key).
    pub dataflow_id: DataflowId,
    /// Logical channel identifier within the dataflow.
    pub channel_id: u64,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

/// Header size: 16 (dataflow_id UUID) + 8 (channel_id) + 4 (length) = 28 bytes.
const HEADER_SIZE: usize = 28;

/// Errors that can occur during framed transport operations.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// I/O error from the underlying stream.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Frame payload exceeds the maximum allowed size.
    #[error("frame payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },

    /// The connection was closed by the remote peer.
    #[error("connection closed")]
    ConnectionClosed,

    /// A channel receiver was dropped (demuxer cannot deliver).
    #[error("channel {channel_id} has no receiver")]
    ChannelDropped { channel_id: u64 },

    /// The muxer received a shutdown signal.
    #[error("muxer shut down")]
    MuxerShutdown,

    /// A reorder buffer gap exceeded the configured timeout.
    ///
    /// This indicates that one or more sequenced frames were lost in transit
    /// (e.g., due to a connection failure mid-write). The affected dataflow
    /// cannot continue receiving ordered data from this peer.
    #[error("reorder buffer gap timeout")]
    ReorderTimeout,
}

// ─── Feature-gated implementations using tokio ───────────────────────────────

#[cfg(feature = "transport")]
mod tokio_impl {
    use super::*;
    use std::collections::HashMap;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::sync::mpsc;

    /// Writes [`Frame`]s to an `AsyncWrite` stream.
    ///
    /// Frames are written with the wire header (channel_id + length) followed
    /// by the payload. Writes are flushed after each frame for low latency.
    pub struct FramedWriter<W> {
        writer: W,
    }

    impl<W: AsyncWrite + Unpin> FramedWriter<W> {
        /// Create a new framed writer wrapping the given stream.
        pub fn new(writer: W) -> Self {
            Self { writer }
        }

        /// Write a single frame to the stream.
        ///
        /// # Errors
        ///
        /// Returns [`TransportError::PayloadTooLarge`] if the payload exceeds
        /// [`MAX_MESSAGE_SIZE`], or [`TransportError::Io`] on write failure.
        pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), TransportError> {
            let len = frame.payload.len();
            if len > MAX_MESSAGE_SIZE {
                return Err(TransportError::PayloadTooLarge {
                    size: len,
                    max: MAX_MESSAGE_SIZE,
                });
            }

            // Write header: dataflow_id (16 UUID) + channel_id (8 LE) + length (4 LE)
            let mut header = [0u8; HEADER_SIZE];
            header[..16].copy_from_slice(frame.dataflow_id.as_bytes());
            header[16..24].copy_from_slice(&frame.channel_id.to_le_bytes());
            header[24..28].copy_from_slice(&(len as u32).to_le_bytes());

            self.writer.write_all(&header).await?;
            self.writer.write_all(&frame.payload).await?;
            self.writer.flush().await?;
            Ok(())
        }

        /// Write multiple frames, flushing once at the end for better throughput.
        ///
        /// All frames are validated for size before any data is written, ensuring
        /// atomicity: either all frames are written or none are (on size error).
        /// I/O errors mid-batch are still possible and should be treated as fatal.
        pub async fn write_frames(&mut self, frames: &[Frame]) -> Result<(), TransportError> {
            // Pre-validate all sizes before writing any bytes
            for frame in frames {
                let len = frame.payload.len();
                if len > MAX_MESSAGE_SIZE {
                    return Err(TransportError::PayloadTooLarge {
                        size: len,
                        max: MAX_MESSAGE_SIZE,
                    });
                }
            }

            for frame in frames {
                let len = frame.payload.len();
                let mut header = [0u8; HEADER_SIZE];
                header[..16].copy_from_slice(frame.dataflow_id.as_bytes());
                header[16..24].copy_from_slice(&frame.channel_id.to_le_bytes());
                header[24..28].copy_from_slice(&(len as u32).to_le_bytes());

                self.writer.write_all(&header).await?;
                self.writer.write_all(&frame.payload).await?;
            }
            self.writer.flush().await?;
            Ok(())
        }

        /// Consume the writer, returning the inner stream.
        pub fn into_inner(self) -> W {
            self.writer
        }
    }

    /// Reads [`Frame`]s from an `AsyncRead` stream.
    ///
    /// Reads the wire header to determine payload size, then reads the full payload.
    pub struct FramedReader<R> {
        reader: R,
    }

    impl<R: AsyncRead + Unpin> FramedReader<R> {
        /// Create a new framed reader wrapping the given stream.
        pub fn new(reader: R) -> Self {
            Self { reader }
        }

        /// Read a single frame from the stream.
        ///
        /// # Errors
        ///
        /// - [`TransportError::ConnectionClosed`] if EOF is reached cleanly at a frame boundary
        /// - [`TransportError::PayloadTooLarge`] if the declared length exceeds max
        /// - [`TransportError::Io`] on read failure (including unexpected EOF mid-frame)
        pub async fn read_frame(&mut self) -> Result<Frame, TransportError> {
            // Read header byte-by-byte to distinguish clean EOF from mid-header disconnect.
            // A clean close is when we get 0 bytes before reading any header data.
            // A partial header read indicates a protocol error or peer crash.
            let mut header = [0u8; HEADER_SIZE];
            let mut pos = 0;
            while pos < HEADER_SIZE {
                match self.reader.read(&mut header[pos..]).await {
                    Ok(0) => {
                        if pos == 0 {
                            return Err(TransportError::ConnectionClosed);
                        } else {
                            return Err(TransportError::Io(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "connection closed mid-header",
                            )));
                        }
                    }
                    Ok(n) => pos += n,
                    Err(e) => return Err(TransportError::Io(e)),
                }
            }

            let dataflow_id =
                DataflowId::from_bytes(wire::read_array::<16>(&header, 0).map_err(|e| {
                    TransportError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string()))
                })?);
            let channel_id = wire::read_u64(&header, 16).map_err(|e| {
                TransportError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string()))
            })?;
            let length = wire::read_u32(&header, 24).map_err(|e| {
                TransportError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string()))
            })? as usize;

            if length > MAX_MESSAGE_SIZE {
                return Err(TransportError::PayloadTooLarge {
                    size: length,
                    max: MAX_MESSAGE_SIZE,
                });
            }

            // Read payload
            let mut payload = vec![0u8; length];
            self.reader.read_exact(&mut payload).await.map_err(|e| {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    TransportError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-frame",
                    ))
                } else {
                    TransportError::Io(e)
                }
            })?;

            Ok(Frame {
                dataflow_id,
                channel_id,
                payload,
            })
        }

        /// Consume the reader, returning the inner stream.
        pub fn into_inner(self) -> R {
            self.reader
        }
    }

    /// Configuration for the [`Demuxer`].
    #[derive(Debug, Clone)]
    pub struct DemuxConfig {
        /// Per-channel buffer capacity (number of frames).
        pub channel_buffer: usize,
    }

    impl Default for DemuxConfig {
        fn default() -> Self {
            Self {
                channel_buffer: 256,
            }
        }
    }

    /// A demultiplexer that reads frames from a connection and dispatches them
    /// to per-(dataflow, channel) receivers.
    ///
    /// The demuxer runs as a background task. It reads frames from the underlying
    /// stream and routes them to registered channel receivers based on the
    /// `(dataflow_id, channel_id)` pair. If a channel receiver is dropped, the
    /// frame is discarded.
    ///
    /// # Backpressure
    ///
    /// The demuxer applies **per-channel backpressure**: when a channel's buffer
    /// is full, the demuxer awaits until the receiver drains capacity. This means
    /// a single slow consumer can block delivery to all other channels on the same
    /// connection. This is intentional — in a dataflow system, dropping frames
    /// would violate correctness. Callers that need isolation should use separate
    /// connections per channel or spawn per-channel forwarding tasks.
    pub struct Demuxer<R> {
        reader: FramedReader<R>,
        channels: HashMap<(DataflowId, u64), mpsc::Sender<Vec<u8>>>,
        config: DemuxConfig,
    }

    /// Handle returned when registering a channel with a [`Demuxer`].
    /// Receives frame payloads for the registered channel.
    pub type ChannelReceiver = mpsc::Receiver<Vec<u8>>;

    impl<R: AsyncRead + Unpin> Demuxer<R> {
        /// Create a new demuxer wrapping the given reader.
        pub fn new(reader: R, config: DemuxConfig) -> Self {
            Self {
                reader: FramedReader::new(reader),
                channels: HashMap::new(),
                config,
            }
        }

        /// Register a channel to receive frames for a specific dataflow.
        ///
        /// Returns a receiver that will get the payload bytes for frames
        /// matching the given `(dataflow_id, channel_id)` pair.
        pub fn register_channel(
            &mut self,
            dataflow_id: DataflowId,
            channel_id: u64,
        ) -> ChannelReceiver {
            let (tx, rx) = mpsc::channel(self.config.channel_buffer);
            self.channels.insert((dataflow_id, channel_id), tx);
            rx
        }

        /// Run the demuxer, reading frames until the connection closes or an error occurs.
        ///
        /// This method should be spawned as a background task. It returns when:
        /// - The connection is cleanly closed ([`TransportError::ConnectionClosed`])
        /// - An I/O error occurs
        /// - All channel senders are dropped (no one to deliver to)
        pub async fn run(mut self) -> Result<(), TransportError> {
            loop {
                let frame = match self.reader.read_frame().await {
                    Ok(f) => f,
                    Err(TransportError::ConnectionClosed) => return Ok(()),
                    Err(e) => return Err(e),
                };

                let key = (frame.dataflow_id, frame.channel_id);
                if let Some(tx) = self.channels.get(&key) {
                    // If the receiver is dropped, remove the channel
                    if tx.send(frame.payload).await.is_err() {
                        self.channels.remove(&key);
                    }
                }
                // Frames for unregistered (dataflow, channel) pairs are silently dropped.

                // If all channels are gone, stop
                if self.channels.is_empty() {
                    return Ok(());
                }
            }
        }
    }

    /// A multiplexer that collects frames from multiple channel senders and
    /// writes them to a connection.
    ///
    /// Each channel gets a sender handle. The muxer runs as a background task,
    /// selecting from all channel senders and writing frames to the wire.
    pub struct Muxer<W> {
        writer: FramedWriter<W>,
        receiver: mpsc::Receiver<Frame>,
    }

    /// A sender handle for submitting frames to the [`Muxer`].
    #[derive(Clone)]
    pub struct MuxerSender {
        sender: mpsc::Sender<Frame>,
    }

    impl MuxerSender {
        /// Send a frame to be written to the connection.
        ///
        /// Returns an error if the muxer has shut down.
        pub async fn send(&self, frame: Frame) -> Result<(), TransportError> {
            self.sender
                .send(frame)
                .await
                .map_err(|_| TransportError::MuxerShutdown)
        }

        /// Send a payload on a specific (dataflow, channel) pair.
        pub async fn send_payload(
            &self,
            dataflow_id: DataflowId,
            channel_id: u64,
            payload: Vec<u8>,
        ) -> Result<(), TransportError> {
            self.send(Frame {
                dataflow_id,
                channel_id,
                payload,
            })
            .await
        }
    }

    /// Configuration for the [`Muxer`].
    #[derive(Debug, Clone)]
    pub struct MuxConfig {
        /// Buffer capacity for the internal frame queue.
        pub buffer_size: usize,
    }

    impl Default for MuxConfig {
        fn default() -> Self {
            Self { buffer_size: 256 }
        }
    }

    impl<W: AsyncWrite + Unpin> Muxer<W> {
        /// Create a new muxer and its sender handle.
        ///
        /// The sender can be cloned and shared across tasks. Each clone can
        /// submit frames for any channel_id.
        pub fn new(writer: W, config: MuxConfig) -> (Self, MuxerSender) {
            let (tx, rx) = mpsc::channel(config.buffer_size);
            let muxer = Self {
                writer: FramedWriter::new(writer),
                receiver: rx,
            };
            let sender = MuxerSender { sender: tx };
            (muxer, sender)
        }

        /// Run the muxer, writing frames until all senders are dropped or an error occurs.
        ///
        /// This method should be spawned as a background task.
        pub async fn run(mut self) -> Result<(), TransportError> {
            while let Some(frame) = self.receiver.recv().await {
                self.writer.write_frame(&frame).await?;
            }
            Ok(())
        }

        /// Run the muxer with batched writes for better throughput.
        ///
        /// Collects available frames and writes them in a single flush.
        pub async fn run_batched(mut self) -> Result<(), TransportError> {
            let mut batch = Vec::new();
            loop {
                // Wait for at least one frame
                match self.receiver.recv().await {
                    Some(frame) => batch.push(frame),
                    None => return Ok(()), // All senders dropped
                }

                // Drain any additional immediately-available frames
                while batch.len() < 64 {
                    match self.receiver.try_recv() {
                        Ok(frame) => batch.push(frame),
                        Err(_) => break,
                    }
                }

                // Write batch
                self.writer.write_frames(&batch).await?;
                batch.clear();
            }
        }
    }
}

#[cfg(feature = "transport")]
pub use tokio_impl::*;

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_equality() {
        let f1 = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 42,
            payload: vec![1, 2, 3],
        };
        let f2 = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 42,
            payload: vec![1, 2, 3],
        };
        assert_eq!(f1, f2);
    }

    #[test]
    fn frame_debug() {
        let f = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 1,
            payload: vec![0xAA],
        };
        let debug = format!("{f:?}");
        assert!(debug.contains("channel_id: 1"));
    }

    #[test]
    fn transport_error_display() {
        let e = TransportError::PayloadTooLarge {
            size: 300_000_000,
            max: MAX_MESSAGE_SIZE,
        };
        assert!(format!("{e}").contains("300000000"));

        let e = TransportError::ConnectionClosed;
        assert_eq!(format!("{e}"), "connection closed");

        let e = TransportError::ChannelDropped { channel_id: 7 };
        assert!(format!("{e}").contains("7"));

        let e = TransportError::MuxerShutdown;
        assert_eq!(format!("{e}"), "muxer shut down");
    }
}

#[cfg(all(test, feature = "transport"))]
mod transport_tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn write_read_single_frame() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        let frame = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 123,
            payload: b"hello world".to_vec(),
        };
        writer.write_frame(&frame).await.unwrap();
        drop(writer);

        let received = reader.read_frame().await.unwrap();
        assert_eq!(received, frame);
    }

    #[tokio::test]
    async fn write_read_multiple_frames() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        let frames: Vec<Frame> = (0..5)
            .map(|i| Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: i,
                payload: format!("message {i}").into_bytes(),
            })
            .collect();

        for f in &frames {
            writer.write_frame(f).await.unwrap();
        }
        drop(writer);

        for expected in &frames {
            let received = reader.read_frame().await.unwrap();
            assert_eq!(&received, expected);
        }
    }

    #[tokio::test]
    async fn write_frames_batch() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        let frames: Vec<Frame> = (0..3)
            .map(|i| Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: i * 10,
                payload: vec![i as u8; 100],
            })
            .collect();

        writer.write_frames(&frames).await.unwrap();
        drop(writer);

        for expected in &frames {
            let received = reader.read_frame().await.unwrap();
            assert_eq!(&received, expected);
        }
    }

    #[tokio::test]
    async fn empty_payload() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        let frame = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 0,
            payload: vec![],
        };
        writer.write_frame(&frame).await.unwrap();
        drop(writer);

        let received = reader.read_frame().await.unwrap();
        assert_eq!(received.payload.len(), 0);
        assert_eq!(received.channel_id, 0);
    }

    #[tokio::test]
    async fn large_payload() {
        let (client, server) = duplex(1024 * 1024);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        // 100KB payload
        let payload = vec![0xAB; 100_000];
        let frame = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 99,
            payload: payload.clone(),
        };
        writer.write_frame(&frame).await.unwrap();
        drop(writer);

        let received = reader.read_frame().await.unwrap();
        assert_eq!(received.payload.len(), 100_000);
        assert_eq!(received.payload, payload);
    }

    #[tokio::test]
    async fn payload_too_large_rejected() {
        let (client, _server) = duplex(8192);
        let mut writer = FramedWriter::new(client);

        // Exceeds MAX_MESSAGE_SIZE (256 MB)
        let frame = Frame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 1,
            payload: vec![0; MAX_MESSAGE_SIZE + 1],
        };
        let result = writer.write_frame(&frame).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            TransportError::PayloadTooLarge { size, max } => {
                assert_eq!(max, MAX_MESSAGE_SIZE);
                assert_eq!(size, MAX_MESSAGE_SIZE + 1);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn connection_closed_cleanly() {
        let (client, server) = duplex(8192);
        drop(client); // Close write side immediately

        let mut reader = FramedReader::new(server);
        let result = reader.read_frame().await;
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn connection_closed_after_frames() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);
        let mut reader = FramedReader::new(server);

        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 1,
                payload: b"data".to_vec(),
            })
            .await
            .unwrap();
        drop(writer);

        // First read succeeds
        let f = reader.read_frame().await.unwrap();
        assert_eq!(f.channel_id, 1);

        // Second read gets connection closed
        let result = reader.read_frame().await;
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn demuxer_dispatches_to_channels() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);

        let config = DemuxConfig { channel_buffer: 16 };
        let mut demuxer = Demuxer::new(server, config);

        let mut rx1 = demuxer.register_channel(DataflowId::from_bytes([1u8; 16]), 1);
        let mut rx2 = demuxer.register_channel(DataflowId::from_bytes([1u8; 16]), 2);
        let mut rx3 = demuxer.register_channel(DataflowId::from_bytes([1u8; 16]), 3);

        // Write interleaved frames
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 1,
                payload: b"ch1-a".to_vec(),
            })
            .await
            .unwrap();
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 2,
                payload: b"ch2-a".to_vec(),
            })
            .await
            .unwrap();
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 3,
                payload: b"ch3-a".to_vec(),
            })
            .await
            .unwrap();
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 1,
                payload: b"ch1-b".to_vec(),
            })
            .await
            .unwrap();
        drop(writer);

        // Run demuxer to completion
        demuxer.run().await.unwrap();

        // Verify each channel received correct frames
        assert_eq!(rx1.recv().await.unwrap(), b"ch1-a");
        assert_eq!(rx1.recv().await.unwrap(), b"ch1-b");
        assert_eq!(rx2.recv().await.unwrap(), b"ch2-a");
        assert_eq!(rx3.recv().await.unwrap(), b"ch3-a");

        // Channels are empty
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());
        assert!(rx3.try_recv().is_err());
    }

    #[tokio::test]
    async fn demuxer_unregistered_channel_silently_dropped() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);

        let config = DemuxConfig::default();
        let mut demuxer = Demuxer::new(server, config);
        let mut rx1 = demuxer.register_channel(DataflowId::from_bytes([1u8; 16]), 1);

        // Write to registered and unregistered channels
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 99,
                payload: b"unknown".to_vec(),
            })
            .await
            .unwrap();
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 1,
                payload: b"known".to_vec(),
            })
            .await
            .unwrap();
        drop(writer);

        demuxer.run().await.unwrap();

        // Only channel 1 receives its frame
        assert_eq!(rx1.recv().await.unwrap(), b"known");
    }

    #[tokio::test]
    async fn demuxer_stops_when_all_channels_dropped() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);

        let config = DemuxConfig::default();
        let mut demuxer = Demuxer::new(server, config);
        let rx1 = demuxer.register_channel(DataflowId::from_bytes([1u8; 16]), 1);

        // Drop the receiver before sending
        drop(rx1);

        // Send a frame — demuxer should stop because no receivers left
        writer
            .write_frame(&Frame {
                dataflow_id: DataflowId::from_bytes([1u8; 16]),
                channel_id: 1,
                payload: b"orphan".to_vec(),
            })
            .await
            .unwrap();
        // Keep writer alive so connection doesn't close
        // The demuxer should exit because all channels are removed
        let handle = tokio::spawn(async move { demuxer.run().await });

        let result = handle.await.unwrap();
        assert!(result.is_ok()); // Clean exit
    }

    #[tokio::test]
    async fn muxer_writes_frames() {
        let (client, server) = duplex(8192);
        let mut reader = FramedReader::new(server);

        let config = MuxConfig { buffer_size: 16 };
        let (muxer, sender) = Muxer::new(client, config);

        let muxer_handle = tokio::spawn(async move { muxer.run().await });

        sender
            .send_payload(DataflowId::from_bytes([1u8; 16]), 1, b"hello".to_vec())
            .await
            .unwrap();
        sender
            .send_payload(DataflowId::from_bytes([1u8; 16]), 2, b"world".to_vec())
            .await
            .unwrap();
        drop(sender); // Signal muxer to stop

        muxer_handle.await.unwrap().unwrap();

        let f1 = reader.read_frame().await.unwrap();
        assert_eq!(f1.channel_id, 1);
        assert_eq!(f1.payload, b"hello");

        let f2 = reader.read_frame().await.unwrap();
        assert_eq!(f2.channel_id, 2);
        assert_eq!(f2.payload, b"world");
    }

    #[tokio::test]
    async fn muxer_batched_writes_frames() {
        let (client, server) = duplex(8192);
        let mut reader = FramedReader::new(server);

        let config = MuxConfig { buffer_size: 64 };
        let (muxer, sender) = Muxer::new(client, config);

        let muxer_handle = tokio::spawn(async move { muxer.run_batched().await });

        for i in 0..10u64 {
            sender
                .send_payload(
                    DataflowId::from_bytes([1u8; 16]),
                    i,
                    format!("msg-{i}").into_bytes(),
                )
                .await
                .unwrap();
        }
        drop(sender);

        muxer_handle.await.unwrap().unwrap();

        for i in 0..10u64 {
            let f = reader.read_frame().await.unwrap();
            assert_eq!(f.channel_id, i);
            assert_eq!(f.payload, format!("msg-{i}").into_bytes());
        }
    }

    #[tokio::test]
    async fn muxer_sender_clone() {
        let (client, server) = duplex(8192);
        let mut reader = FramedReader::new(server);

        let config = MuxConfig::default();
        let (muxer, sender) = Muxer::new(client, config);
        let sender2 = sender.clone();

        let muxer_handle = tokio::spawn(async move { muxer.run().await });

        sender
            .send_payload(DataflowId::from_bytes([1u8; 16]), 1, b"from-1".to_vec())
            .await
            .unwrap();
        sender2
            .send_payload(DataflowId::from_bytes([1u8; 16]), 2, b"from-2".to_vec())
            .await
            .unwrap();
        drop(sender);
        drop(sender2);

        muxer_handle.await.unwrap().unwrap();

        let f1 = reader.read_frame().await.unwrap();
        let f2 = reader.read_frame().await.unwrap();
        // Both frames arrive (order may vary since both senders share the channel)
        let mut ids: Vec<u64> = vec![f1.channel_id, f2.channel_id];
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn muxer_send_after_shutdown() {
        let (client, _server) = duplex(8192);

        let config = MuxConfig { buffer_size: 4 };
        let (muxer, sender) = Muxer::new(client, config);

        // Drop muxer immediately (simulates shutdown)
        drop(muxer);

        // Sending should fail
        let result = sender
            .send_payload(DataflowId::from_bytes([1u8; 16]), 1, b"late".to_vec())
            .await;
        assert!(matches!(result, Err(TransportError::MuxerShutdown)));
    }

    #[tokio::test]
    async fn end_to_end_bidirectional() {
        // Simulate two peers communicating over a full-duplex connection
        let (a_write, b_read) = duplex(8192);
        let (b_write, a_read) = duplex(8192);

        // Peer A: muxer on a_write, demuxer on a_read
        let mux_config = MuxConfig { buffer_size: 32 };
        let (muxer_a, sender_a) = Muxer::new(a_write, mux_config.clone());
        let demux_config = DemuxConfig { channel_buffer: 32 };
        let mut demuxer_a = Demuxer::new(a_read, demux_config.clone());
        let mut rx_a = demuxer_a.register_channel(DataflowId::from_bytes([1u8; 16]), 100);

        // Peer B: muxer on b_write, demuxer on b_read
        let (muxer_b, sender_b) = Muxer::new(b_write, mux_config);
        let mut demuxer_b = Demuxer::new(b_read, demux_config);
        let mut rx_b = demuxer_b.register_channel(DataflowId::from_bytes([1u8; 16]), 200);

        // Start background tasks
        let ha = tokio::spawn(async move { muxer_a.run().await });
        let hb = tokio::spawn(async move { muxer_b.run().await });
        let da = tokio::spawn(async move { demuxer_a.run().await });
        let db = tokio::spawn(async move { demuxer_b.run().await });

        // A sends to B on channel 200
        sender_a
            .send_payload(
                DataflowId::from_bytes([1u8; 16]),
                200,
                b"hello from A".to_vec(),
            )
            .await
            .unwrap();
        // B sends to A on channel 100
        sender_b
            .send_payload(
                DataflowId::from_bytes([1u8; 16]),
                100,
                b"hello from B".to_vec(),
            )
            .await
            .unwrap();

        // Verify receipt
        let msg_b = rx_b.recv().await.unwrap();
        assert_eq!(msg_b, b"hello from A");
        let msg_a = rx_a.recv().await.unwrap();
        assert_eq!(msg_a, b"hello from B");

        // Shutdown
        drop(sender_a);
        drop(sender_b);
        ha.await.unwrap().unwrap();
        hb.await.unwrap().unwrap();
        da.await.unwrap().unwrap();
        db.await.unwrap().unwrap();
    }
}
