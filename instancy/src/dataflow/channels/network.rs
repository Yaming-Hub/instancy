//! Network-backed edge materializer for real cross-node exchange.
//!
//! Provides [`NetworkEdgeMaterializer`], which implements [`EdgeMaterializer`]
//! using real TCP transport via the Muxer/Demuxer infrastructure:
//!
//! - **Same-node** worker pairs: direct `BoundedPush`/`BoundedPull` (shared
//!   memory, zero serialization overhead).
//! - **Cross-node** worker pairs: [`NetworkPush`]/[`NetworkPull`] that
//!   serialize data through the [`Codec`] trait and transport over TCP
//!   via Muxer/Demuxer.
//!
//! # Architecture
//!
//! ```text
//! ┌─── Node A ─────────────────┐       ┌─── Node B ─────────────────┐
//! │                             │       │                             │
//! │  Worker 0 ──► NetworkPush ──┤       ├── NetworkPull ──► Worker 2  │
//! │                │            │       │          ▲                  │
//! │  encode + tag  │            │  TCP  │          │  decode          │
//! │                ▼            │       │          │                  │
//! │           FrameSender       │       │    tokio::mpsc::Rx          │
//! │  (std::sync::mpsc)          │       │          ▲                  │
//! │                │            │       │          │                  │
//! │                ▼            │       │    Demuxer (background)     │
//! │         bridge task ────────┼───────┼──► FramedReader             │
//! │  (FrameReceiver→FramedWriter)       │                             │
//! └─────────────────────────────┘       └─────────────────────────────┘
//! ```
//!
//! # Transport state lifetime
//!
//! The Muxer/Demuxer background tasks must outlive the materializer (which
//! is dropped after Phase 5 materialization). Transport state is held in
//! `Arc<TransportState>` and shared by all `NetworkPush`/`NetworkPull`
//! endpoints returned from `materialize_worker()`.
//!
//! # Ownership and sharing
//!
//! Each [`NetworkEdgeMaterializer`] is created **per-dataflow** (bound to a
//! [`DataflowId`]), and each `NetworkPush`/`NetworkPull` is created per
//! (source_worker, target_worker) pair within that dataflow. They are **not**
//! shared across dataflows.
//!
//! The underlying TCP connections to peer nodes *can* be shared across
//! dataflows (the wire [`Frame`] carries the `dataflow_id`, and the
//! [`Demuxer`] dispatches to the correct per-dataflow, per-channel receiver).
//! However, the Push/Pull endpoints themselves are dataflow-specific.
//!
//! # Close semantics
//!
//! Each `NetworkPush` sends an explicit close frame (empty payload with a
//! reserved close-sentinel channel ID) when `close()` is called. The
//! corresponding `NetworkPull` detects this and marks itself exhausted.
//!
//! [`Codec`]: crate::communication::codec::Codec
//! [`EdgeMaterializer`]: crate::dataflow::channels::edge_materializer::EdgeMaterializer

use std::sync::{mpsc as std_mpsc, Arc, Mutex};

use crate::communication::codec::{Codec, CodecError, ExchangeData};
use crate::dataflow::channels::bounded::{bounded_channel, BoundedPull, BoundedPush};
use crate::dataflow::channels::edge_materializer::EdgeMaterializer;
use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::id::DataflowId;
use crate::error::{Error, Result};
use crate::execute::ClusterTopology;
use crate::progress::timestamp::Timestamp;
use crate::worker::WorkerId;

#[cfg(feature = "transport")]
use crate::communication::transport::{
    DemuxConfig, Demuxer, Frame, FramedWriter,
};

#[cfg(feature = "transport")]
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "transport")]
use tokio::sync::mpsc as tokio_mpsc;

// ---------------------------------------------------------------------------
// Wire-format tags (shared with mock_network for consistency)
// ---------------------------------------------------------------------------

const TAG_DATA: u8 = 0x01;
const TAG_WATERMARK: u8 = 0x02;
const TAG_ERROR: u8 = 0x03;
const TAG_CLOSE: u8 = 0xFF;

/// Encode an envelope into bytes using the provided codecs.
fn encode_envelope<T, D, TC, DC>(
    time_codec: &TC,
    data_codec: &DC,
    envelope: &Envelope<T, D, ()>,
    buf: &mut Vec<u8>,
) -> std::result::Result<(), CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
    DC: Codec<D>,
{
    match &envelope.payload {
        Payload::Data { time, data } => {
            buf.push(TAG_DATA);
            time_codec.encode(time, buf)?;
            let count: u32 = data.len().try_into().map_err(|_| {
                CodecError::InvalidData(format!(
                    "batch too large for wire format: {} records",
                    data.len()
                ))
            })?;
            buf.extend_from_slice(&count.to_le_bytes());
            for record in data {
                data_codec.encode(record, buf)?;
            }
        }
        Payload::Control(ControlSignal::Watermark(t)) => {
            buf.push(TAG_WATERMARK);
            time_codec.encode(t, buf)?;
        }
        Payload::Control(ControlSignal::Error {
            source_operator,
            message,
        }) => {
            buf.push(TAG_ERROR);
            let src_bytes = source_operator.as_bytes();
            buf.extend_from_slice(&(src_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(src_bytes);
            let msg_bytes = message.as_bytes();
            buf.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(msg_bytes);
        }
    }
    Ok(())
}

/// Encode a close sentinel (TAG_CLOSE with no payload).
fn encode_close(buf: &mut Vec<u8>) {
    buf.push(TAG_CLOSE);
}

/// Decode an envelope from bytes, returning `None` for close sentinel.
fn decode_envelope<T, D, TC, DC>(
    time_codec: &TC,
    data_codec: &DC,
    buf: &[u8],
) -> std::result::Result<Option<Envelope<T, D, ()>>, CodecError>
where
    T: Timestamp,
    TC: Codec<T>,
    DC: Codec<D>,
{
    if buf.is_empty() {
        return Err(CodecError::InsufficientData {
            needed: 1,
            available: 0,
        });
    }

    let tag = buf[0];
    let rest = &buf[1..];

    match tag {
        TAG_CLOSE => {
            // Close sentinel — no further data on this channel.
            Ok(None)
        }
        TAG_DATA => {
            let (time, consumed) = time_codec.decode(rest)?;
            let rest = &rest[consumed..];
            if rest.len() < 4 {
                return Err(CodecError::InsufficientData {
                    needed: 4,
                    available: rest.len(),
                });
            }
            let count = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            const MAX_BATCH_SIZE: usize = 10_000_000;
            if count > MAX_BATCH_SIZE {
                return Err(CodecError::InvalidData(format!(
                    "batch size {count} exceeds maximum {MAX_BATCH_SIZE}"
                )));
            }
            let mut pos = 4;
            let mut data = Vec::with_capacity(count);
            for _ in 0..count {
                let (record, consumed) = data_codec.decode(&rest[pos..])?;
                data.push(record);
                pos += consumed;
            }
            if pos != rest.len() {
                return Err(CodecError::InvalidData(format!(
                    "trailing bytes in data envelope: consumed {pos}, total {}",
                    rest.len()
                )));
            }
            Ok(Some(Envelope::data(time, data)))
        }
        TAG_WATERMARK => {
            let (time, consumed) = time_codec.decode(rest)?;
            if consumed != rest.len() {
                return Err(CodecError::InvalidData(format!(
                    "trailing bytes in watermark envelope: consumed {consumed}, total {}",
                    rest.len()
                )));
            }
            Ok(Some(Envelope::watermark(time)))
        }
        TAG_ERROR => {
            if rest.len() < 4 {
                return Err(CodecError::InsufficientData {
                    needed: 4,
                    available: rest.len(),
                });
            }
            let src_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() < 4 + src_len + 4 {
                return Err(CodecError::InsufficientData {
                    needed: 4 + src_len + 4,
                    available: rest.len(),
                });
            }
            let source_operator = String::from_utf8(rest[4..4 + src_len].to_vec())
                .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8: {e}")))?;
            let msg_offset = 4 + src_len;
            let msg_len = u32::from_le_bytes(
                rest[msg_offset..msg_offset + 4].try_into().unwrap(),
            ) as usize;
            let total_consumed = msg_offset + 4 + msg_len;
            if rest.len() < total_consumed {
                return Err(CodecError::InsufficientData {
                    needed: total_consumed,
                    available: rest.len(),
                });
            }
            if total_consumed != rest.len() {
                return Err(CodecError::InvalidData(format!(
                    "trailing bytes in error envelope: consumed {total_consumed}, total {}",
                    rest.len()
                )));
            }
            let message = String::from_utf8(rest[msg_offset + 4..total_consumed].to_vec())
                .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8: {e}")))?;
            Ok(Some(Envelope::error(source_operator, message)))
        }
        _ => Err(CodecError::InvalidData(format!("unknown tag: {tag:#x}"))),
    }
}

// ---------------------------------------------------------------------------
// NetworkPush — Push<T, D, ()> that serializes and sends over the wire
// ---------------------------------------------------------------------------

/// A `Push` endpoint that serializes envelopes through a [`Codec`] and
/// sends the resulting bytes as frames to a Muxer via a sync channel.
///
/// The Muxer background task multiplexes frames from multiple `NetworkPush`
/// endpoints onto a single TCP connection.
///
/// On `close()`, an explicit close-sentinel frame is sent so the remote
/// [`NetworkPull`] can detect per-channel exhaustion.
#[cfg(feature = "transport")]
pub struct NetworkPush<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    dataflow_id: DataflowId,
    channel_id: u64,
    sender: std_mpsc::SyncSender<Frame>,
    closed: bool,
    /// Shared transport state that keeps background tasks alive.
    _transport: Arc<TransportState>,
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> NetworkPush<T, D> {
    fn encode(&self, envelope: &Envelope<T, D, ()>) -> std::result::Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        encode_envelope(&self.time_codec, &self.data_codec, envelope, &mut buf)
            .map_err(|e| Error::Custom(format!("network encode: {e}")))?;
        Ok(buf)
    }

    fn send_frame(&self, payload: Vec<u8>) -> std::result::Result<(), Error> {
        let frame = Frame {
            dataflow_id: self.dataflow_id,
            channel_id: self.channel_id,
            payload,
        };
        self.sender.try_send(frame).map_err(|e| match e {
            std_mpsc::TrySendError::Full(_) => Error::Backpressure,
            std_mpsc::TrySendError::Disconnected(_) => Error::ChannelClosed,
        })
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> Push<T, D, ()>
    for NetworkPush<T, D>
{
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }
        let bytes = self.encode(&envelope)?;
        self.send_frame(bytes)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, ()>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, ()>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }
        let bytes = match self.encode(&envelope) {
            Ok(b) => b,
            Err(e) => return Err((e, envelope)),
        };
        match self.send_frame(bytes) {
            Ok(()) => Ok(()),
            Err(e) => Err((e, envelope)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        // Flushing is handled by the background mux task.
        Ok(())
    }

    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            // Send close sentinel using blocking send to ensure delivery.
            // This is a terminal one-shot operation; blocking is acceptable.
            let mut buf = Vec::new();
            encode_close(&mut buf);
            let frame = Frame {
                dataflow_id: self.dataflow_id,
                channel_id: self.channel_id,
                payload: buf,
            };
            let _ = self.sender.send(frame); // blocks until space or disconnected
        }
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn available_capacity(&self) -> Option<usize> {
        // Report the shared per-peer queue capacity. This is an approximation
        // since multiple NetworkPush endpoints share the same queue, but it
        // enables ExchangePush's atomic pre-check to avoid partial delivery.
        // std::sync::mpsc::SyncSender doesn't expose remaining capacity,
        // so we probe with a zero-cost try: attempt a dummy check.
        // Since we can't query capacity directly, return None and let
        // ExchangePush assume capacity (network buffers internally).
        // The bridge task drains continuously, so backpressure is transient.
        None
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> Drop for NetworkPush<T, D> {
    fn drop(&mut self) {
        if !self.closed {
            self.closed = true;
            // Best-effort close sentinel in Drop (non-blocking to avoid
            // blocking in destructor). Callers should call close() explicitly.
            let mut buf = Vec::new();
            encode_close(&mut buf);
            let frame = Frame {
                dataflow_id: self.dataflow_id,
                channel_id: self.channel_id,
                payload: buf,
            };
            let _ = self.sender.try_send(frame);
        }
    }
}

// ---------------------------------------------------------------------------
// NetworkPull — Pull<T, D, ()> that receives and deserializes from the wire
// ---------------------------------------------------------------------------

/// A `Pull` endpoint that receives bytes from a Demuxer channel and
/// deserializes them through a [`Codec`] back into typed envelopes.
///
/// The Demuxer background task reads frames from TCP and dispatches
/// payloads to per-channel tokio mpsc receivers. `NetworkPull` uses
/// `try_recv()` (non-blocking) to poll for available data.
///
/// When a close-sentinel frame is received, this endpoint marks itself
/// as exhausted. Since the protocol guarantees FIFO ordering with a single
/// sender per channel, the close sentinel is always the last frame.
#[cfg(feature = "transport")]
pub struct NetworkPull<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    receiver: tokio_mpsc::Receiver<Vec<u8>>,
    exhausted: bool,
    /// Shared transport state that keeps background tasks alive.
    _transport: Arc<TransportState>,
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> Pull<T, D, ()>
    for NetworkPull<T, D>
{
    fn pull(&mut self) -> Option<Envelope<T, D, ()>> {
        if self.exhausted {
            return None;
        }
        let bytes = match self.receiver.try_recv() {
            Ok(b) => b,
            Err(tokio_mpsc::error::TryRecvError::Empty) => return None,
            Err(tokio_mpsc::error::TryRecvError::Disconnected) => {
                self.exhausted = true;
                return None;
            }
        };
        match decode_envelope(&self.time_codec, &self.data_codec, &bytes) {
            Ok(Some(env)) => Some(env),
            Ok(None) => {
                // Close sentinel — single sender per channel, FIFO order,
                // so no more data follows.
                self.exhausted = true;
                None
            }
            Err(e) => {
                // Propagate decode errors as control signals so the dataflow
                // can detect and handle data corruption.
                Some(Envelope::error("NetworkPull", format!("decode error: {e}")))
            }
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

// ---------------------------------------------------------------------------
// TransportState — shared lifetime anchor for background tasks
// ---------------------------------------------------------------------------

/// Holds references that keep Muxer/Demuxer background tasks alive.
///
/// This is wrapped in `Arc` and shared by all `NetworkPush`/`NetworkPull`
/// endpoints. When the last reference is dropped (all Push/Pull endpoints
/// gone), background tasks are aborted to prevent leaks.
#[cfg(feature = "transport")]
struct TransportState {
    /// Muxer bridge task handles.
    mux_handles: Vec<tokio::task::JoinHandle<()>>,
    /// Demuxer task handles.
    demux_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "transport")]
impl TransportState {
    fn new(
        mux_handles: Vec<tokio::task::JoinHandle<()>>,
        demux_handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            mux_handles,
            demux_handles,
        }
    }
}

#[cfg(feature = "transport")]
impl Drop for TransportState {
    fn drop(&mut self) {
        // Abort background tasks to prevent leaks. Mux tasks will also
        // terminate naturally when all SyncSenders are dropped, but
        // Demuxer tasks may block on reads indefinitely without abort.
        for handle in &self.mux_handles {
            handle.abort();
        }
        for handle in &self.demux_handles {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// PeerConnection — a bidirectional connection to a remote node
// ---------------------------------------------------------------------------

/// A bidirectional connection to a remote peer, split into read/write halves.
///
/// The caller provides already-established connections (via [`ConnectionManager`]
/// or direct TCP). The materializer wraps them with Muxer/Demuxer.
///
/// [`ConnectionManager`]: crate::communication::connection::ConnectionManager
#[cfg(feature = "transport")]
pub struct PeerConnection<R: AsyncRead + Unpin + Send + 'static, W: AsyncWrite + Unpin + Send + 'static> {
    /// The node ID this connection leads to.
    pub node_id: String,
    /// Read half of the connection (feeds the Demuxer).
    pub reader: R,
    /// Write half of the connection (feeds the Muxer).
    pub writer: W,
}

// ---------------------------------------------------------------------------
// NetworkEdgeMaterializer
// ---------------------------------------------------------------------------

/// Edge materializer that uses real network transport for cross-node exchange.
///
/// Same-node worker pairs use direct `BoundedPush`/`BoundedPull` (zero-copy
/// shared memory). Cross-node worker pairs use [`NetworkPush`]/[`NetworkPull`]
/// that serialize through [`Codec`] and transport data over TCP via the
/// Muxer/Demuxer infrastructure.
///
/// # Connection setup
///
/// The caller provides pre-established bidirectional connections to each
/// remote peer via [`PeerConnection`]. The materializer wraps each
/// connection with a Muxer (write side) and Demuxer (read side), spawning
/// background tokio tasks for I/O.
///
/// # Channel ID scheme
///
/// Each `(source_worker, target_worker)` pair gets a deterministic channel ID:
/// `channel_id = source_worker * num_workers + target_worker + 1` (offset by 1
/// because channel 0 is reserved for progress messages).
///
/// # Example
///
/// ```ignore
/// use tokio::net::TcpStream;
/// use tokio::io::split;
///
/// let topology = ClusterTopology::multi_node(vec![
///     NodeConfig::new("node-a", 2),
///     NodeConfig::new("node-b", 2),
/// ]).unwrap();
///
/// // Establish TCP connection to node-b
/// let stream = TcpStream::connect("node-b:9000").await.unwrap();
/// let (reader, writer) = split(stream);
///
/// let connections = vec![PeerConnection {
///     node_id: "node-b".to_string(),
///     reader,
///     writer,
/// }];
///
/// let mat = NetworkEdgeMaterializer::<u64, String>::new(
///     dataflow_id, topology, "node-a", connections, 1024,
/// );
/// ```
#[cfg(feature = "transport")]
pub struct NetworkEdgeMaterializer<T: Timestamp + ExchangeData, D: ExchangeData> {
    num_workers: usize,
    local_node_id: String,
    topology: ClusterTopology,
    dataflow_id: DataflowId,

    /// Shared transport state (Arc so it outlives this materializer).
    transport: Arc<TransportState>,

    /// Per-peer sync senders: node_id → SyncSender<Frame>
    /// Multiple NetworkPush endpoints sharing the same peer clone from here.
    peer_senders: std::collections::HashMap<String, std_mpsc::SyncSender<Frame>>,

    /// Demuxer channel receivers for remote pull endpoints.
    /// Key: (source_worker, target_worker) → tokio::mpsc::Receiver<Vec<u8>>
    demux_receivers: std::collections::HashMap<(usize, usize), tokio_mpsc::Receiver<Vec<u8>>>,

    /// Local channels for same-node pairs: [src][dst]
    local_push: Vec<Vec<Option<BoundedPush<T, D, ()>>>>,
    local_pull: Vec<Vec<Option<BoundedPull<T, D, ()>>>>,

    /// Track which workers have been materialized.
    taken: Vec<bool>,
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> NetworkEdgeMaterializer<T, D> {
    /// Create a network edge materializer.
    ///
    /// # Arguments
    /// - `dataflow_id`: Unique ID for this dataflow (used in frame routing).
    /// - `topology`: Cluster topology describing all nodes and workers.
    /// - `local_node_id`: The node ID of this process.
    /// - `connections`: Pre-established connections to each remote peer.
    /// - `capacity`: Buffer capacity for local channels and frame queues.
    /// - `runtime_handle`: Tokio runtime handle for spawning Muxer/Demuxer tasks.
    pub fn new<R, W>(
        dataflow_id: DataflowId,
        topology: ClusterTopology,
        local_node_id: impl Into<String>,
        connections: Vec<PeerConnection<R, W>>,
        capacity: usize,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let local_node_id = local_node_id.into();
        let num_workers = topology.total_workers();

        // --- Set up local channels for same-node pairs ---
        let mut local_push: Vec<Vec<Option<BoundedPush<T, D, ()>>>> =
            (0..num_workers).map(|_| (0..num_workers).map(|_| None).collect()).collect();
        let mut local_pull: Vec<Vec<Option<BoundedPull<T, D, ()>>>> =
            (0..num_workers).map(|_| (0..num_workers).map(|_| None).collect()).collect();

        for src in 0..num_workers {
            let src_node = topology.node_for_worker(WorkerId::new(src));
            for dst in 0..num_workers {
                let dst_node = topology.node_for_worker(WorkerId::new(dst));
                if src_node == dst_node && src_node == Some(local_node_id.as_str()) {
                    let (push, pull) = bounded_channel::<T, D, ()>(capacity);
                    local_push[src][dst] = Some(push);
                    local_pull[src][dst] = Some(pull);
                }
            }
        }

        // --- Set up Muxer/Demuxer per peer ---
        let mut mux_handles = Vec::new();
        let mut demux_handles = Vec::new();
        let mut peer_senders = std::collections::HashMap::new();
        let mut demux_receivers = std::collections::HashMap::new();

        for conn in connections {
            let peer_node_id = conn.node_id.clone();

            // --- Muxer (write side) ---
            // Use a sync channel as the bridge: NetworkPush calls try_send(),
            // a bridge task reads and forwards to FramedWriter.
            let (sync_tx, sync_rx) = std_mpsc::sync_channel::<Frame>(capacity);
            peer_senders.insert(peer_node_id.clone(), sync_tx);

            let writer = conn.writer;
            let sync_rx = Arc::new(Mutex::new(sync_rx));
            let mux_handle = runtime_handle.spawn(async move {
                let mut framed_writer = FramedWriter::new(writer);
                // Bridge: read from sync channel, write to framed writer.
                // We use spawn_blocking to avoid blocking the async runtime.
                loop {
                    let rx_clone = sync_rx.clone();
                    let frame = match tokio::task::spawn_blocking(move || {
                        let rx = rx_clone.lock().unwrap();
                        rx.recv().ok()
                    })
                    .await
                    {
                        Ok(Some(frame)) => frame,
                        Ok(None) | Err(_) => break, // All senders dropped or task cancelled
                    };

                    if let Err(e) = framed_writer.write_frame(&frame).await {
                        #[cfg(feature = "tracing")]
                        tracing::error!("Muxer write error for peer {}: {e}", peer_node_id);
                        break;
                    }
                }
            });
            mux_handles.push(mux_handle);

            // --- Demuxer (read side) ---
            let reader = conn.reader;
            let mut demuxer = Demuxer::new(reader, DemuxConfig::default());

            // Register channels for all remote worker pairs where
            // source is on the peer and destination is on this node.
            let local_range = topology.worker_range(&local_node_id);
            let peer_range = topology.worker_range(&conn.node_id);

            if let (Some((local_start, local_end)), Some((peer_start, peer_end))) =
                (local_range, peer_range)
            {
                for src in peer_start..peer_end {
                    for dst in local_start..local_end {
                        let channel_id = Self::channel_id(src, dst, num_workers);
                        let rx = demuxer.register_channel(dataflow_id, channel_id);
                        demux_receivers.insert((src, dst), rx);
                    }
                }
            }

            let demux_handle = runtime_handle.spawn(async move {
                if let Err(e) = demuxer.run().await {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Demuxer error: {e}");
                }
            });
            demux_handles.push(demux_handle);
        }

        let transport = Arc::new(TransportState::new(mux_handles, demux_handles));

        Self {
            num_workers,
            local_node_id,
            topology,
            dataflow_id,
            transport,
            peer_senders,
            demux_receivers,
            local_push,
            local_pull,
            taken: vec![false; num_workers],
        }
    }

    /// Deterministic channel ID for a (source, target) worker pair.
    ///
    /// Channel 0 is reserved for progress messages, so IDs start at 1.
    fn channel_id(source: usize, target: usize, num_workers: usize) -> u64 {
        (source * num_workers + target + 1) as u64
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> EdgeMaterializer<T, D>
    for NetworkEdgeMaterializer<T, D>
{
    fn num_workers(&self) -> usize {
        self.num_workers
    }

    fn materialize_worker(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }
        if self.taken[worker_idx] {
            return Err(Error::Custom(format!(
                "worker {worker_idx} already materialized"
            )));
        }
        self.taken[worker_idx] = true;

        let worker_node = self
            .topology
            .node_for_worker(WorkerId::new(worker_idx))
            .expect("worker index valid for topology");

        // Build push endpoints
        let mut pushers: Vec<Box<dyn Push<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for dst in 0..self.num_workers {
            let dst_node = self
                .topology
                .node_for_worker(WorkerId::new(dst))
                .expect("worker index valid for topology");

            if worker_node == dst_node && worker_node == self.local_node_id {
                // Local pair — use BoundedPush
                let push = self.local_push[worker_idx][dst]
                    .take()
                    .ok_or_else(|| Error::Custom(format!(
                        "local push [{worker_idx}][{dst}] already taken"
                    )))?;
                pushers.push(Box::new(push));
            } else {
                // Remote pair — use NetworkPush
                let sender = self.peer_senders.get(dst_node)
                    .ok_or_else(|| Error::Custom(format!(
                        "no connection to peer node '{dst_node}'"
                    )))?
                    .clone();

                let channel_id = Self::channel_id(worker_idx, dst, self.num_workers);
                pushers.push(Box::new(NetworkPush::<T, D> {
                    time_codec: T::codec(),
                    data_codec: D::codec(),
                    dataflow_id: self.dataflow_id,
                    channel_id,
                    sender,
                    closed: false,
                    _transport: self.transport.clone(),
                }));
            }
        }

        // Build pull endpoints
        let mut pullers: Vec<Box<dyn Pull<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for src in 0..self.num_workers {
            let src_node = self
                .topology
                .node_for_worker(WorkerId::new(src))
                .expect("worker index valid for topology");

            if src_node == worker_node && src_node == self.local_node_id {
                // Local pair — use BoundedPull
                let pull = self.local_pull[src][worker_idx]
                    .take()
                    .ok_or_else(|| Error::Custom(format!(
                        "local pull [{src}][{worker_idx}] already taken"
                    )))?;
                pullers.push(Box::new(pull));
            } else {
                // Remote pair — use NetworkPull
                let receiver = self.demux_receivers.remove(&(src, worker_idx))
                    .ok_or_else(|| Error::Custom(format!(
                        "no demux receiver for [{src}][{worker_idx}]"
                    )))?;

                pullers.push(Box::new(NetworkPull::<T, D> {
                    time_codec: T::codec(),
                    data_codec: D::codec(),
                    receiver,
                    exhausted: false,
                    _transport: self.transport.clone(),
                }));
            }
        }

        Ok((pushers, pullers))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Codec round-trip tests (same as mock_network but with close support) ---

    #[test]
    fn encode_decode_data_roundtrip() {
        let tc = u64::codec();
        let dc = u64::codec();
        let env: Envelope<u64, u64, ()> = Envelope::data(42, vec![10, 20, 30]);
        let mut buf = Vec::new();
        encode_envelope(&tc, &dc, &env, &mut buf).unwrap();
        let decoded = decode_envelope(&tc, &dc, &buf).unwrap().unwrap();
        assert_eq!(decoded.as_data(), Some((&42u64, &vec![10u64, 20, 30])));
    }

    #[test]
    fn encode_decode_watermark_roundtrip() {
        let tc = u64::codec();
        let dc = u64::codec();
        let env: Envelope<u64, u64, ()> = Envelope::watermark(99);
        let mut buf = Vec::new();
        encode_envelope(&tc, &dc, &env, &mut buf).unwrap();
        let decoded = decode_envelope(&tc, &dc, &buf).unwrap().unwrap();
        match decoded.payload {
            Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 99),
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn encode_decode_error_roundtrip() {
        let tc = u64::codec();
        let dc = u64::codec();
        let env: Envelope<u64, u64, ()> = Envelope::error("op1", "bad data");
        let mut buf = Vec::new();
        encode_envelope(&tc, &dc, &env, &mut buf).unwrap();
        let decoded = decode_envelope(&tc, &dc, &buf).unwrap().unwrap();
        match decoded.payload {
            Payload::Control(ControlSignal::Error { source_operator, message }) => {
                assert_eq!(source_operator, "op1");
                assert_eq!(message, "bad data");
            }
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn encode_decode_close_sentinel() {
        let tc = u64::codec();
        let dc = u64::codec();
        let mut buf = Vec::new();
        encode_close(&mut buf);
        let result = decode_envelope::<u64, u64, _, _>(&tc, &dc, &buf).unwrap();
        assert!(result.is_none(), "close sentinel should decode to None");
    }

    // --- TCP integration tests (require tokio runtime + transport feature) ---

    #[cfg(feature = "transport")]
    mod transport_tests {
        use super::*;
        use crate::execute::NodeConfig;

        fn two_node_topology() -> ClusterTopology {
            ClusterTopology::multi_node(vec![
                NodeConfig::new("node-a", 2), // workers 0, 1
                NodeConfig::new("node-b", 2), // workers 2, 3
            ])
            .unwrap()
        }

        /// Helper: create a pair of NetworkEdgeMaterializers connected via
        /// in-memory duplex streams (simulates TCP without actual sockets).
        fn create_connected_materializers(
            dataflow_id: DataflowId,
            topology: ClusterTopology,
            capacity: usize,
            rt: &tokio::runtime::Handle,
        ) -> (
            NetworkEdgeMaterializer<u64, u64>,
            NetworkEdgeMaterializer<u64, u64>,
        ) {
            // Create duplex streams (bidirectional in-memory byte pipes)
            let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
            let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

            let mat_a = NetworkEdgeMaterializer::<u64, u64>::new(
                dataflow_id,
                topology.clone(),
                "node-a",
                vec![PeerConnection {
                    node_id: "node-b".to_string(),
                    reader: a_from_b,
                    writer: a_to_b,
                }],
                capacity,
                rt,
            );

            let mat_b = NetworkEdgeMaterializer::<u64, u64>::new(
                dataflow_id,
                topology,
                "node-b",
                vec![PeerConnection {
                    node_id: "node-a".to_string(),
                    reader: b_from_a,
                    writer: b_to_a,
                }],
                capacity,
                rt,
            );

            (mat_a, mat_b)
        }

        /// Poll a pull endpoint with timeout, retrying until data arrives or
        /// the deadline expires. Avoids fragile fixed-duration sleeps.
        async fn poll_pull<T, D>(
            pull: &mut dyn Pull<T, D, ()>,
            timeout: std::time::Duration,
        ) -> Option<Envelope<T, D, ()>>
        where
            T: Timestamp,
            D: Clone + Send + 'static,
        {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if let Some(env) = pull.pull() {
                    return Some(env);
                }
                if tokio::time::Instant::now() >= deadline {
                    return None;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        #[tokio::test]
        async fn network_materializer_local_pair() {
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (mut mat_a, _mat_b) = create_connected_materializers(df_id, topo, 16, &rt);

            // Workers 0 and 1 are both on node-a → local BoundedPush/Pull
            let (mut push0, _pull0) = mat_a.materialize_worker(0).unwrap();
            let (_push1, mut pull1) = mat_a.materialize_worker(1).unwrap();

            push0[1].push(Envelope::data(10, vec![42])).unwrap();
            let env = pull1[0].pull().unwrap();
            assert_eq!(env.as_data(), Some((&10u64, &vec![42u64])));
        }

        #[tokio::test]
        async fn network_materializer_cross_node_data() {
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (mut mat_a, mut mat_b) = create_connected_materializers(df_id, topo, 16, &rt);

            // Materialize all workers
            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            // Worker 0 (node-a) → Worker 2 (node-b): cross-node
            push0[2].push(Envelope::data(99, vec![1, 2, 3])).unwrap();

            let timeout = std::time::Duration::from_secs(2);
            let env = poll_pull(pull2[0].as_mut(), timeout).await.expect("data should arrive");
            assert_eq!(env.as_data(), Some((&99u64, &vec![1u64, 2, 3])));
        }

        #[tokio::test]
        async fn network_materializer_cross_node_watermark() {
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (mut mat_a, mut mat_b) = create_connected_materializers(df_id, topo, 16, &rt);

            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            push0[2].push(Envelope::watermark(50)).unwrap();

            let timeout = std::time::Duration::from_secs(2);
            let env = poll_pull(pull2[0].as_mut(), timeout).await.expect("watermark should arrive");
            match env.payload {
                Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 50),
                _ => panic!("expected watermark"),
            }
        }

        #[tokio::test]
        async fn network_materializer_close_exhaustion() {
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (mut mat_a, mut mat_b) = create_connected_materializers(df_id, topo, 16, &rt);

            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            // Send data then close
            push0[2].push(Envelope::data(1, vec![10])).unwrap();
            push0[2].close();

            let timeout = std::time::Duration::from_secs(2);

            // Pull the data
            let env = poll_pull(pull2[0].as_mut(), timeout).await.expect("data should arrive");
            assert_eq!(env.as_data(), Some((&1u64, &vec![10u64])));

            // Poll until exhausted (close sentinel must arrive)
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if pull2[0].is_exhausted() {
                    break;
                }
                let _ = pull2[0].pull();
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for exhaustion"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        #[tokio::test]
        async fn network_materializer_bidirectional() {
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (mut mat_a, mut mat_b) = create_connected_materializers(df_id, topo, 16, &rt);

            let (mut push0, mut pull0) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (mut push2, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            // A→B: worker 0 → worker 2
            push0[2].push(Envelope::data(1, vec![100])).unwrap();
            // B→A: worker 2 → worker 0
            push2[0].push(Envelope::data(2, vec![200])).unwrap();

            let timeout = std::time::Duration::from_secs(2);
            let env_at_b = poll_pull(pull2[0].as_mut(), timeout).await.expect("A→B data");
            assert_eq!(env_at_b.as_data(), Some((&1u64, &vec![100u64])));

            let env_at_a = poll_pull(pull0[2].as_mut(), timeout).await.expect("B→A data");
            assert_eq!(env_at_a.as_data(), Some((&2u64, &vec![200u64])));
        }

        #[tokio::test]
        async fn network_materializer_with_string_data() {
            let topo = ClusterTopology::multi_node(vec![
                NodeConfig::new("a", 1),
                NodeConfig::new("b", 1),
            ])
            .unwrap();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
            let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

            let mut mat_a = NetworkEdgeMaterializer::<u64, String>::new(
                df_id, topo.clone(), "a",
                vec![PeerConnection { node_id: "b".to_string(), reader: a_from_b, writer: a_to_b }],
                16, &rt,
            );
            let mut mat_b = NetworkEdgeMaterializer::<u64, String>::new(
                df_id, topo, "b",
                vec![PeerConnection { node_id: "a".to_string(), reader: b_from_a, writer: b_to_a }],
                16, &rt,
            );

            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let (_, mut pull1) = mat_b.materialize_worker(1).unwrap();

            push0[1].push(Envelope::data(7, vec!["hello".into(), "world".into()])).unwrap();

            let timeout = std::time::Duration::from_secs(2);
            let env = poll_pull(pull1[0].as_mut(), timeout).await.expect("string data should arrive");
            assert_eq!(
                env.as_data(),
                Some((&7u64, &vec!["hello".to_string(), "world".to_string()]))
            );
        }
    }
}
