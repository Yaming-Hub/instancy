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
//! ┌─────────────────┬────────────────┬────────────┬────────────────┬────────────────────┬────────────────┐
//! │ dataflow_id:UUID│ channel_id: u64│ length: u32│ header_crc: u32│ payload: [u8; ...] │ frame_crc: u32 │
//! └─────────────────┴────────────────┴────────────┴────────────────┴────────────────────┴────────────────┘
//! ```
//!
//! - `dataflow_id` (16 bytes, UUID): identifies the dataflow
//! - `channel_id` (8 bytes, little-endian): identifies the logical channel
//! - `length` (4 bytes, little-endian): byte length of payload + `frame_crc`
//! - `header_crc` (4 bytes, little-endian): CRC32 of the 28 header
//!   bytes, verified before the payload is read so a corrupted `length`,
//!   `channel_id` or `dataflow_id` is rejected instead of misrouting the frame
//!   or desynchronising the stream.
//! - `payload` (variable): serialized message data
//! - `frame_crc` (4 bytes, little-endian): CRC32 of the header followed by
//!   the payload.
//!
//! Every frame carries both checksums (cluster protocol version 6 onwards):
//! a corrupted frame is rejected with [`TransportError::ChecksumMismatch`]
//! instead of being delivered.
//!
//! # Components
//!
//! - [`Frame`]: A single wire-protocol frame (channel + payload)
//! - [`FramedWriter`]: Writes frames to an `AsyncWrite` stream
//! - [`FramedReader`]: Reads frames from an `AsyncRead` stream
//! - [`Demuxer`]: Background task dispatching incoming frames to per-channel receivers
//! - [`Muxer`]: Collects frames from multiple channel senders and writes them out

use std::io;

#[cfg(any(test, feature = "transport"))]
use crate::communication::codec::MAX_MESSAGE_SIZE;
use crate::dataflow::id::DataflowId;
#[cfg(feature = "transport")]
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
#[cfg(feature = "transport")]
const HEADER_SIZE: usize = 28;

/// Size of each optional CRC32 field (header checksum and frame checksum).
#[cfg(feature = "transport")]
const CRC_SIZE: usize = 4;

#[cfg(feature = "transport")]
fn encode_header(frame: &Frame, wire_len: u32) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[..16].copy_from_slice(frame.dataflow_id.as_bytes());
    header[16..24].copy_from_slice(&frame.channel_id.to_le_bytes());
    header[24..28].copy_from_slice(&wire_len.to_le_bytes());
    header
}

/// CRC32 of the header followed by the payload (the trailing frame checksum).
#[cfg(feature = "transport")]
fn frame_crc(header: &[u8; HEADER_SIZE], payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(header);
    hasher.update(payload);
    hasher.finalize()
}

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

    /// A frame of a sequenced lane (Shared mode) did not carry the next
    /// sequence number: frames were lost or duplicated in transit. The
    /// dataflow cannot trust what it receives from this peer.
    #[error(
        "frame {received} of a sequenced lane arrived where {expected} was expected: frames were lost or duplicated"
    )]
    OutOfSequence {
        /// The sequence number the receiver expected.
        expected: u64,
        /// The sequence number the frame carried.
        received: u64,
    },

    /// A sequenced lane (Shared mode) broke its contract: its frames arrived
    /// on two connections (their order is lost), or a frame was addressed to
    /// a channel the dataflow never registered.
    #[error("sequenced lane violated: {detail}")]
    LaneViolation { detail: String },

    /// CRC32 checksum mismatch — the frame payload was corrupted in transit.
    ///
    /// Only emitted when CRC verification is enabled on the reader.
    #[error("CRC32 checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// The CRC32 value read from the wire (appended by the writer).
        expected: u32,
        /// The CRC32 value computed from the received payload.
        actual: u32,
    },

    /// A shared lock was poisoned (another thread panicked while holding it).
    ///
    /// This is an unrecoverable transport error — the hosting application
    /// should be notified via [`RuntimeEvent::TransportDegraded`](crate::RuntimeEvent::TransportDegraded).
    #[error("transport lock poisoned: {detail}")]
    LockPoisoned { detail: String },

    /// A channel or dataflow registration was rejected: a duplicate or
    /// reserved channel id, a duplicate dataflow registration, a zero buffer
    /// capacity, or a registration for an unknown peer.
    #[error("invalid channel registration: {reason}")]
    InvalidRegistration { reason: String },

    /// A frame was sent on a channel id reserved for transport-internal use
    /// (the control or probe channel) through the payload path.
    #[error("channel {channel_id} is reserved and cannot carry payload frames")]
    ReservedChannel { channel_id: u64 },
}

#[cfg(feature = "transport")]
impl TransportError {
    /// A copy of this error, for reporting one failure to several dataflows
    /// (an I/O error keeps its kind and message).
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            Self::Io(e) => Self::Io(io::Error::new(e.kind(), e.to_string())),
            Self::PayloadTooLarge { size, max } => Self::PayloadTooLarge {
                size: *size,
                max: *max,
            },
            Self::ConnectionClosed => Self::ConnectionClosed,
            Self::ChannelDropped { channel_id } => Self::ChannelDropped {
                channel_id: *channel_id,
            },
            Self::MuxerShutdown => Self::MuxerShutdown,
            Self::OutOfSequence { expected, received } => Self::OutOfSequence {
                expected: *expected,
                received: *received,
            },
            Self::LaneViolation { detail } => Self::LaneViolation {
                detail: detail.clone(),
            },
            Self::ChecksumMismatch { expected, actual } => Self::ChecksumMismatch {
                expected: *expected,
                actual: *actual,
            },
            Self::LockPoisoned { detail } => Self::LockPoisoned {
                detail: detail.clone(),
            },
            Self::InvalidRegistration { reason } => Self::InvalidRegistration {
                reason: reason.clone(),
            },
            Self::ReservedChannel { channel_id } => Self::ReservedChannel {
                channel_id: *channel_id,
            },
        }
    }
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
    /// Frames are written with the wire header (dataflow_id + channel_id +
    /// length), a CRC32 of the header, the payload, and a CRC32 of header +
    /// payload (see the [module documentation](super) for the layout).
    ///
    /// Writes are flushed after each frame for low latency.
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
            self.write_unflushed(frame).await?;
            self.writer.flush().await?;
            Ok(())
        }

        /// Write one frame (already size-checked) without flushing.
        async fn write_unflushed(&mut self, frame: &Frame) -> Result<(), TransportError> {
            // The wire length includes the frame CRC trailer.
            let wire_len = frame.payload.len() + CRC_SIZE;
            let header = encode_header(frame, wire_len as u32);

            self.writer.write_all(&header).await?;
            self.writer
                .write_all(&crc32fast::hash(&header).to_le_bytes())
                .await?;
            self.writer.write_all(&frame.payload).await?;
            self.writer
                .write_all(&frame_crc(&header, &frame.payload).to_le_bytes())
                .await?;
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
                self.write_unflushed(frame).await?;
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
    /// Reads the wire header to determine payload size, then reads the full
    /// payload. The header checksum is verified before the payload is read,
    /// and the trailing frame checksum is verified against header and payload;
    /// both are stripped from the returned frame.
    pub struct FramedReader<R> {
        reader: R,
        /// The dataflow of the frame being read, once its header checksum
        /// was verified (see [`Self::verified_dataflow`]).
        verified_dataflow: Option<DataflowId>,
    }

    impl<R: AsyncRead + Unpin> FramedReader<R> {
        /// Create a new framed reader wrapping the given stream.
        pub fn new(reader: R) -> Self {
            Self {
                reader,
                verified_dataflow: None,
            }
        }

        /// The dataflow of the last frame [`Self::read_frame`] read or failed
        /// to read, if its header checksum was verified: a frame whose
        /// payload fails its checksum, or that announces a payload over the
        /// limit, belongs to that dataflow. `None` if the header itself
        /// could not be read or verified.
        pub fn verified_dataflow(&self) -> Option<DataflowId> {
            self.verified_dataflow
        }

        /// Read a single frame from the stream.
        ///
        /// # Errors
        ///
        /// - [`TransportError::ConnectionClosed`] if EOF is reached cleanly at a frame boundary
        /// - [`TransportError::PayloadTooLarge`] if the declared length exceeds max
        /// - [`TransportError::ChecksumMismatch`] if a checksum does not match
        /// - [`TransportError::Io`] on read failure (including unexpected EOF mid-frame)
        pub async fn read_frame(&mut self) -> Result<Frame, TransportError> {
            self.verified_dataflow = None;
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
            let wire_length = wire::read_u32(&header, 24).map_err(|e| {
                TransportError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string()))
            })? as usize;

            // Verify the header before trusting `length`: a corrupted length
            // would otherwise make the reader consume the wrong number of bytes
            // (or wait for bytes that never arrive).
            let mut crc_bytes = [0u8; CRC_SIZE];
            self.reader.read_exact(&mut crc_bytes).await.map_err(|e| {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    TransportError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-header",
                    ))
                } else {
                    TransportError::Io(e)
                }
            })?;
            let expected = u32::from_le_bytes(crc_bytes);
            let actual = crc32fast::hash(&header);
            if actual != expected {
                return Err(TransportError::ChecksumMismatch { expected, actual });
            }
            self.verified_dataflow = Some(dataflow_id);

            if wire_length > MAX_MESSAGE_SIZE + CRC_SIZE {
                return Err(TransportError::PayloadTooLarge {
                    size: wire_length,
                    max: MAX_MESSAGE_SIZE,
                });
            }

            // Read the full wire payload (includes the CRC trailer)
            let mut wire_data = vec![0u8; wire_length];
            self.reader.read_exact(&mut wire_data).await.map_err(|e| {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    TransportError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed mid-frame",
                    ))
                } else {
                    TransportError::Io(e)
                }
            })?;

            if wire_length < CRC_SIZE {
                return Err(TransportError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame too short for CRC32 trailer",
                )));
            }
            let payload_len = wire_length - CRC_SIZE;
            let expected = u32::from_le_bytes([
                wire_data[payload_len],
                wire_data[payload_len + 1],
                wire_data[payload_len + 2],
                wire_data[payload_len + 3],
            ]);
            let actual = frame_crc(&header, &wire_data[..payload_len]);
            if actual != expected {
                return Err(TransportError::ChecksumMismatch { expected, actual });
            }
            wire_data.truncate(payload_len);
            let payload = wire_data;

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
        ///
        /// # Errors
        ///
        /// Returns [`TransportError::InvalidRegistration`] if the pair is
        /// already registered (a second registration used to silently replace
        /// the first receiver) or if [`DemuxConfig::channel_buffer`] is 0.
        pub fn register_channel(
            &mut self,
            dataflow_id: DataflowId,
            channel_id: u64,
        ) -> Result<ChannelReceiver, TransportError> {
            if self.config.channel_buffer == 0 {
                return Err(TransportError::InvalidRegistration {
                    reason: "demuxer channel buffer must be at least 1".into(),
                });
            }
            match self.channels.entry((dataflow_id, channel_id)) {
                std::collections::hash_map::Entry::Occupied(_) => {
                    Err(TransportError::InvalidRegistration {
                        reason: format!(
                            "channel {channel_id} of dataflow {dataflow_id} is already registered"
                        ),
                    })
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    let (tx, rx) = mpsc::channel(self.config.channel_buffer);
                    slot.insert(tx);
                    Ok(rx)
                }
            }
        }

        /// Run the demuxer, reading frames until the connection closes or an error occurs.
        ///
        /// This method should be spawned as a background task. It returns when:
        /// - The connection is cleanly closed ([`TransportError::ConnectionClosed`])
        /// - An I/O error occurs
        /// - All channel senders are dropped (no one to deliver to)
        pub async fn run(mut self) -> Result<(), TransportError> {
            match self.run_until_end().await {
                DemuxEnd::Eof | DemuxEnd::ReceiversDropped | DemuxEnd::Released => Ok(()),
                DemuxEnd::Failed(e) => Err(e),
            }
        }

        /// Like [`run`](Self::run), but tells a peer's EOF apart from every
        /// receiver having been dropped. The registered channels stay open
        /// until the demuxer is dropped, so the caller can report why the
        /// connection ended before anyone sees them close.
        pub(crate) async fn run_until_end(&mut self) -> DemuxEnd {
            loop {
                let frame = match self.reader.read_frame().await {
                    Ok(f) => f,
                    Err(TransportError::ConnectionClosed) => return DemuxEnd::Eof,
                    Err(e) => return DemuxEnd::Failed(e),
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
                    return DemuxEnd::ReceiversDropped;
                }
            }
        }

        /// Like [`run`](Self::run), but keeps reading (and discarding frames
        /// nobody receives any more) until the peer closes the connection,
        /// even after every channel is gone, and stops only `linger` after
        /// `closed` fires.
        ///
        /// The read half of a connection must not be released while the peer
        /// may still be sending: closing a socket with unread data makes the
        /// OS reset the connection, which can destroy frames this node wrote
        /// last (an acknowledgement, an end-of-stream marker, a `Cancel`)
        /// before the peer has read them.
        pub async fn run_lingering(
            mut self,
            closed: tokio_util::sync::CancellationToken,
            linger: std::time::Duration,
        ) -> Result<(), TransportError> {
            match self.run_lingering_until_end(closed, linger).await {
                DemuxEnd::Failed(e) => Err(e),
                DemuxEnd::Eof | DemuxEnd::ReceiversDropped | DemuxEnd::Released => Ok(()),
            }
        }

        /// Like [`run_lingering`](Self::run_lingering), but tells why it
        /// stopped, and leaves the registered channels open until the demuxer
        /// is dropped, so the caller can report why the connection ended
        /// before anyone sees those channels close.
        pub(crate) async fn run_lingering_until_end(
            &mut self,
            closed: tokio_util::sync::CancellationToken,
            linger: std::time::Duration,
        ) -> DemuxEnd {
            /// How often (in frames) a peer that never lets this task block
            /// is checked against the linger deadline.
            const CHECK_EVERY: u32 = 64;
            // The read and the send usually complete at once; `expired` is
            // only polled (it takes locks) when this task would block anyway.
            let expired = async {
                closed.cancelled().await;
                tokio::time::sleep(linger).await;
            };
            tokio::pin!(expired);
            let mut frames: u32 = 0;
            let mut deadline = None;
            loop {
                let frame = tokio::select! {
                    biased;
                    frame = self.reader.read_frame() => match frame {
                        Ok(f) => f,
                        Err(TransportError::ConnectionClosed) => return DemuxEnd::Eof,
                        Err(e) => return DemuxEnd::Failed(e),
                    },
                    _ = &mut expired => return DemuxEnd::Released,
                };
                frames = frames.wrapping_add(1);
                if frames % CHECK_EVERY == 0 {
                    if deadline.is_none() && closed.is_cancelled() {
                        deadline = Some(tokio::time::Instant::now() + linger);
                    }
                    if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                        return DemuxEnd::Released;
                    }
                }
                let key = (frame.dataflow_id, frame.channel_id);
                if let Some(tx) = self.channels.get(&key) {
                    let sent = tokio::select! {
                        biased;
                        sent = tx.send(frame.payload) => sent,
                        _ = &mut expired => return DemuxEnd::Released,
                    };
                    if sent.is_err() {
                        self.channels.remove(&key);
                    }
                }
            }
        }
    }

    /// Why a [`Demuxer`] stopped.
    #[derive(Debug)]
    pub(crate) enum DemuxEnd {
        /// The peer closed the connection.
        Eof,
        /// Every registered receiver was dropped.
        ReceiversDropped,
        /// Reading failed (I/O error, corrupt or oversize frame).
        Failed(TransportError),
        /// The owner released the connection and the linger period expired
        /// (see [`Demuxer::run_lingering`]).
        Released,
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
        ///
        /// # Errors
        ///
        /// [`TransportError::InvalidRegistration`] if
        /// [`MuxConfig::buffer_size`] is 0.
        pub fn new(writer: W, config: MuxConfig) -> Result<(Self, MuxerSender), TransportError> {
            if config.buffer_size == 0 {
                return Err(TransportError::InvalidRegistration {
                    reason: "MuxConfig::buffer_size must be at least 1".into(),
                });
            }
            let (tx, rx) = mpsc::channel(config.buffer_size);
            let muxer = Self {
                writer: FramedWriter::new(writer),
                receiver: rx,
            };
            let sender = MuxerSender { sender: tx };
            Ok((muxer, sender))
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

        let mut rx1 = demuxer
            .register_channel(DataflowId::from_bytes([1u8; 16]), 1)
            .unwrap();
        let mut rx2 = demuxer
            .register_channel(DataflowId::from_bytes([1u8; 16]), 2)
            .unwrap();
        let mut rx3 = demuxer
            .register_channel(DataflowId::from_bytes([1u8; 16]), 3)
            .unwrap();

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
    async fn demuxer_rejects_duplicate_and_zero_buffer_registrations() {
        let (_client, server) = duplex(64);
        let df = DataflowId::from_bytes([1u8; 16]);
        let mut demuxer = Demuxer::new(server, DemuxConfig::default());
        let _rx = demuxer.register_channel(df, 1).unwrap();
        assert!(matches!(
            demuxer.register_channel(df, 1),
            Err(TransportError::InvalidRegistration { .. })
        ));
        // Same channel id of another dataflow is a different key.
        assert!(
            demuxer
                .register_channel(DataflowId::from_bytes([2u8; 16]), 1)
                .is_ok()
        );

        let (_client, server) = duplex(64);
        let mut zero = Demuxer::new(server, DemuxConfig { channel_buffer: 0 });
        assert!(matches!(
            zero.register_channel(df, 1),
            Err(TransportError::InvalidRegistration { .. })
        ));
    }

    #[tokio::test]
    async fn demuxer_unregistered_channel_silently_dropped() {
        let (client, server) = duplex(8192);
        let mut writer = FramedWriter::new(client);

        let config = DemuxConfig::default();
        let mut demuxer = Demuxer::new(server, config);
        let mut rx1 = demuxer
            .register_channel(DataflowId::from_bytes([1u8; 16]), 1)
            .unwrap();

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
        let rx1 = demuxer
            .register_channel(DataflowId::from_bytes([1u8; 16]), 1)
            .unwrap();

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
        let (muxer, sender) = Muxer::new(client, config).unwrap();

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
        let (muxer, sender) = Muxer::new(client, config).unwrap();

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
        let (muxer, sender) = Muxer::new(client, config).unwrap();
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

    /// A zero-capacity queue used to panic inside `mpsc::channel(0)`.
    #[tokio::test]
    async fn muxer_rejects_zero_buffer() {
        let (client, _server) = duplex(64);
        assert!(matches!(
            Muxer::new(client, MuxConfig { buffer_size: 0 }),
            Err(TransportError::InvalidRegistration { .. })
        ));
    }

    #[tokio::test]
    async fn muxer_send_after_shutdown() {
        let (client, _server) = duplex(8192);

        let config = MuxConfig { buffer_size: 4 };
        let (muxer, sender) = Muxer::new(client, config).unwrap();

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
        let (muxer_a, sender_a) = Muxer::new(a_write, mux_config.clone()).unwrap();
        let demux_config = DemuxConfig { channel_buffer: 32 };
        let mut demuxer_a = Demuxer::new(a_read, demux_config.clone());
        let mut rx_a = demuxer_a
            .register_channel(DataflowId::from_bytes([1u8; 16]), 100)
            .unwrap();

        // Peer B: muxer on b_write, demuxer on b_read
        let (muxer_b, sender_b) = Muxer::new(b_write, mux_config).unwrap();
        let mut demuxer_b = Demuxer::new(b_read, demux_config);
        let mut rx_b = demuxer_b
            .register_channel(DataflowId::from_bytes([1u8; 16]), 200)
            .unwrap();

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

    /// A released demuxer must stop `linger` after `closed` even when the
    /// peer keeps sending so fast that reading never blocks.
    #[test]
    fn lingering_demuxer_stops_even_if_reading_never_blocks() {
        use std::task::Poll;
        use tokio::io::{AsyncRead, ReadBuf};

        /// Serves the same encoded frame forever, always ready.
        struct Endless {
            frame: Vec<u8>,
            at: usize,
        }

        impl AsyncRead for Endless {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                while buf.remaining() > 0 {
                    let at = self.at;
                    let n = buf.remaining().min(self.frame.len() - at);
                    buf.put_slice(&self.frame[at..at + n]);
                    self.at = (at + n) % self.frame.len();
                }
                Poll::Ready(Ok(()))
            }
        }

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let result = rt.block_on(async {
                let mut writer = FramedWriter::new(Vec::new());
                let frame = Frame {
                    dataflow_id: DataflowId::from_bytes([1u8; 16]),
                    channel_id: 1,
                    payload: vec![0; 16],
                };
                writer.write_frame(&frame).await.unwrap();
                let demuxer = Demuxer::new(
                    Endless {
                        frame: writer.into_inner(),
                        at: 0,
                    },
                    DemuxConfig { channel_buffer: 4 },
                );
                let closed = tokio_util::sync::CancellationToken::new();
                closed.cancel();
                tokio::spawn(demuxer.run_lingering(closed, std::time::Duration::from_millis(100)))
                    .await
                    .unwrap()
            });
            let _ = done_tx.send(result.is_ok());
        });
        let stopped = done_rx.recv_timeout(std::time::Duration::from_secs(10));
        assert_eq!(
            stopped,
            Ok(true),
            "the demuxer kept reading past its linger"
        );
    }

    #[cfg(feature = "transport")]
    mod crc_tests {
        use super::*;
        use tokio_impl::{FramedReader, FramedWriter};

        #[tokio::test]
        async fn crc_round_trip() {
            let (client, server) = tokio::io::duplex(65536);

            let frame = Frame {
                dataflow_id: DataflowId::new(),
                channel_id: 7,
                payload: b"hello crc".to_vec(),
            };

            let frame_clone = frame.clone();
            let write_handle = tokio::spawn(async move {
                let mut writer = FramedWriter::new(client);
                writer.write_frame(&frame_clone).await.unwrap();
            });

            let mut reader = FramedReader::new(server);
            let received = reader.read_frame().await.unwrap();

            assert_eq!(received.channel_id, frame.channel_id);
            assert_eq!(received.payload, frame.payload);
            assert_eq!(received.dataflow_id, frame.dataflow_id);

            write_handle.await.unwrap();
        }

        #[tokio::test]
        async fn crc_detects_corruption() {
            let frame = Frame {
                dataflow_id: DataflowId::from_bytes([0xAB; 16]),
                channel_id: 3,
                payload: b"important data".to_vec(),
            };

            let mut wire = Vec::new();
            let header = encode_header(&frame, (frame.payload.len() + CRC_SIZE) as u32);
            wire.extend_from_slice(&header);
            wire.extend_from_slice(&crc32fast::hash(&header).to_le_bytes());
            wire.extend_from_slice(&frame.payload);
            let crc = frame_crc(&header, &frame.payload);
            wire.extend_from_slice(&crc.to_le_bytes());

            // Corrupt one byte in the payload area (after header + header CRC)
            wire[HEADER_SIZE + CRC_SIZE] ^= 0xFF;

            let cursor = std::io::Cursor::new(wire);
            let mut reader = FramedReader::new(cursor);
            let result = reader.read_frame().await;

            match result {
                Err(TransportError::ChecksumMismatch { expected, actual }) => {
                    assert_eq!(expected, crc);
                    assert_ne!(actual, crc);
                }
                other => panic!("expected ChecksumMismatch, got: {other:?}"),
            }
        }

        #[tokio::test]
        async fn crc_rejects_every_single_bit_header_flip() {
            let frame = Frame {
                dataflow_id: DataflowId::from_bytes([0x5A; 16]),
                channel_id: 0x0102_0304_0506_0708,
                payload: b"payload".to_vec(),
            };
            let mut wire = Vec::new();
            FramedWriter::new(&mut wire)
                .write_frame(&frame)
                .await
                .unwrap();
            for byte in 0..HEADER_SIZE + CRC_SIZE {
                for bit in 0..8 {
                    let mut corrupted = wire.clone();
                    corrupted[byte] ^= 1 << bit;
                    let result = FramedReader::new(&corrupted[..]).read_frame().await;
                    assert!(
                        matches!(result, Err(TransportError::ChecksumMismatch { .. })),
                        "bit {bit} of byte {byte}: {result:?}"
                    );
                }
            }
        }

        #[tokio::test]
        async fn crc_batch_round_trip() {
            let (client, server) = tokio::io::duplex(65536);

            let df_id = DataflowId::new();
            let frames: Vec<Frame> = (0..5)
                .map(|i| Frame {
                    dataflow_id: df_id,
                    channel_id: i,
                    payload: format!("batch-{i}").into_bytes(),
                })
                .collect();

            let frames_clone = frames.clone();
            let write_handle = tokio::spawn(async move {
                let mut writer = FramedWriter::new(client);
                writer.write_frames(&frames_clone).await.unwrap();
            });

            let mut reader = FramedReader::new(server);
            for expected in &frames {
                let received = reader.read_frame().await.unwrap();
                assert_eq!(received, *expected);
            }

            write_handle.await.unwrap();
        }

        /// After a payload checksum failure the reader still knows (from the
        /// verified header) whose frame it was; after a header failure it
        /// does not.
        #[tokio::test]
        async fn reader_reports_the_dataflow_of_a_frame_that_failed_after_its_header() {
            let df = DataflowId::new();
            let frame = Frame {
                dataflow_id: df,
                channel_id: 3,
                payload: b"payload".to_vec(),
            };
            let mut wire = Vec::new();
            FramedWriter::new(&mut wire)
                .write_frame(&frame)
                .await
                .unwrap();

            let mut payload_flip = wire.clone();
            payload_flip[HEADER_SIZE + CRC_SIZE] ^= 0x01;
            let mut reader = FramedReader::new(&payload_flip[..]);
            assert!(reader.read_frame().await.is_err());
            assert_eq!(reader.verified_dataflow(), Some(df));

            let mut header_flip = wire.clone();
            header_flip[20] ^= 0x01;
            let mut reader = FramedReader::new(&header_flip[..]);
            assert!(reader.read_frame().await.is_err());
            assert_eq!(reader.verified_dataflow(), None);
        }
        /// Checksums are not optional: every frame carries both of them.
        #[tokio::test]
        async fn every_frame_carries_both_checksums() {
            let frame = Frame {
                dataflow_id: DataflowId::new(),
                channel_id: 1,
                payload: b"checked".to_vec(),
            };
            let mut wire = Vec::new();
            FramedWriter::new(&mut wire)
                .write_frame(&frame)
                .await
                .unwrap();
            assert_eq!(
                wire.len(),
                HEADER_SIZE + CRC_SIZE + frame.payload.len() + CRC_SIZE
            );
            let header: [u8; HEADER_SIZE] = wire[..HEADER_SIZE].try_into().unwrap();
            assert_eq!(
                wire[HEADER_SIZE..HEADER_SIZE + CRC_SIZE],
                crc32fast::hash(&header).to_le_bytes()
            );
            assert_eq!(
                wire[wire.len() - CRC_SIZE..],
                frame_crc(&header, &frame.payload).to_le_bytes()
            );
            assert_eq!(
                FramedReader::new(&wire[..]).read_frame().await.unwrap(),
                frame
            );
        }
    }
}
