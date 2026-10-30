//! Network-backed edge materializer for real cross-node exchange.
//!
//! Provides [`NetworkEdgeMaterializer`], which implements [`EdgeMaterializer`]
//! using real TCP transport via the [`crate::communication::Muxer`]/[`crate::communication::Demuxer`] infrastructure:
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
//! The [`crate::communication::Muxer`]/[`crate::communication::Demuxer`] background tasks must outlive the materializer (which
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
//! [`crate::communication::Demuxer`] dispatches to the correct per-dataflow,
//! per-channel receiver).
//! However, the Push/Pull endpoints themselves are dataflow-specific.
//!
//! # Close semantics
//!
//! Each `NetworkPush` sends an explicit close frame (a payload consisting of
//! the close tag) on its channel when `close()` is called or the push is
//! dropped. The corresponding `NetworkPull` detects this and marks itself
//! exhausted. In a cluster dataflow the close frame is queued on the peer's
//! ordered outbox lane (the runtime's per-peer forwarder that awaits queue
//! capacity), so it is never lost to a full transport queue and still arrives
//! after all of this push's data. A push without an outbox (standalone use of
//! [`NetworkEdgeMaterializer`]) sends it directly and, if the transport queue
//! is full, waits for room on the runtime: it is lost only with the
//! connection.
//!
//! # Data loss
//!
//! In a cluster dataflow, records are never dropped silently. A push of
//! records to a peer whose link is retired (the peer finished) fails with
//! `CommunicationError::DataLoss`. On the receiving side, records that can no
//! longer be delivered because the consuming operator finished (or that
//! arrive after the channel's close frame) are reported to the dataflow's
//! lifecycle coordinator, which fails the dataflow.
//!
//! [`Codec`]: crate::communication::codec::Codec
//! [`EdgeMaterializer`]: crate::dataflow::channels::edge_materializer::EdgeMaterializer

use std::sync::Arc;

use crate::communication::codec::{Codec, CodecError, ExchangeData};
use crate::dataflow::channels::bounded::{BoundedPull, BoundedPush, bounded_channel};
use crate::dataflow::channels::edge_materializer::EdgeMaterializer;
use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::id::DataflowId;
use crate::error::{DataflowError, Error, Result};
use crate::execute::ClusterTopology;
use crate::progress::timestamp::Timestamp;
use crate::wire;
use crate::worker::WorkerId;

#[cfg(feature = "transport")]
use crate::communication::transport::Frame;
#[cfg(feature = "transport")]
use crate::communication::transport_session::{
    ChannelRegistration, PeerConnection, TransportSession,
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

/// Whether an envelope carries records (as opposed to a control signal).
fn carries_records<T: Timestamp, D>(envelope: &Envelope<T, D, ()>) -> bool {
    matches!(&envelope.payload, Payload::Data { data, .. } if !data.is_empty())
}

/// Whether an encoded frame carries, or may carry, records: a data frame,
/// and any frame that is neither a control signal nor exactly the close
/// sentinel (a corrupt frame may well have carried records).
fn frame_carries_records(bytes: &[u8]) -> bool {
    match bytes.first() {
        Some(&TAG_WATERMARK) | Some(&TAG_ERROR) => false,
        Some(&TAG_CLOSE) => bytes.len() != 1,
        _ => true,
    }
}

/// Whether `bytes` is exactly a close sentinel. A close tag followed by
/// anything is malformed, not an end of stream.
fn is_close_sentinel(bytes: &[u8]) -> bool {
    bytes == [TAG_CLOSE]
}

/// Most memory a data envelope's decoder reserves up front; a larger batch
/// grows as its records are decoded.
const DECODE_PREALLOC_BYTES: usize = 1 << 20;

/// Records to reserve for a batch that announces `count` records in
/// `available` bytes: never more records than bytes present, nor more than
/// [`DECODE_PREALLOC_BYTES`] of memory, so a corrupt count cannot reserve a
/// large allocation whatever the record type's in-memory size.
fn decode_capacity<D>(count: usize, available: usize) -> usize {
    count
        .min(available)
        .min(DECODE_PREALLOC_BYTES / std::mem::size_of::<D>().max(1))
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
            if !rest.is_empty() {
                return Err(CodecError::InvalidData(format!(
                    "close sentinel followed by {} bytes",
                    rest.len()
                )));
            }
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
            let count = wire::read_u32(rest, 0)
                .map_err(|e| CodecError::InvalidData(e.to_string()))?
                as usize;
            // Any count the encoder can produce (up to `u32::MAX`) is valid.
            // The preallocation is bounded (see `decode_capacity`), so a
            // corrupt count cannot reserve a large allocation; truncated input
            // fails below.
            let mut pos = 4;
            let mut data = Vec::with_capacity(decode_capacity::<D>(count, rest.len() - pos));
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
            let src_len = wire::read_u32(rest, 0)
                .map_err(|e| CodecError::InvalidData(e.to_string()))?
                as usize;
            if rest.len() < 4 + src_len + 4 {
                return Err(CodecError::InsufficientData {
                    needed: 4 + src_len + 4,
                    available: rest.len(),
                });
            }
            let source_operator = String::from_utf8(rest[4..4 + src_len].to_vec())
                .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8: {e}")))?;
            let msg_offset = 4 + src_len;
            let msg_len = wire::read_u32(rest, msg_offset)
                .map_err(|e| CodecError::InvalidData(e.to_string()))?
                as usize;
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
/// sends the resulting bytes as frames via a [`TransportSession`]'s
/// data-priority channel.
///
/// On `close()`, an explicit close-sentinel frame is sent so the remote
/// [`NetworkPull`] can detect per-channel exhaustion.
#[cfg(feature = "transport")]
pub struct NetworkPush<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    dataflow_id: DataflowId,
    channel_id: u64,
    sender: crate::communication::cluster_transport::FrameSender,
    /// Ordered lossless lane to the peer (cluster dataflows only).
    outbox: Option<crate::communication::peer_link::PeerOutbox>,
    /// The closed transport queue was reported to the coordinator.
    link_failure_reported: std::sync::atomic::AtomicBool,
    /// The peer this push sends to (for errors).
    peer_id: Arc<str>,
    /// Largest payload the transport carries; larger frames are rejected
    /// here with a clear error instead of failing in the transport.
    max_payload: usize,
    /// Set when this push's worker completed successfully (cluster
    /// dataflows). A push dropped without `close()` closes its channel
    /// cleanly only then: a failed, cancelled or never-run worker's stream is
    /// incomplete, and a sentinel would let the peer treat it as finished.
    worker_completed: Option<Arc<std::sync::atomic::AtomicBool>>,
    closed: bool,
    /// Standalone use (no outbox): the runtime on which the close sentinel
    /// waits for room in a full queue.
    close_runtime: Option<tokio::runtime::Handle>,
    /// The flight recorder's count of data frames enqueued (`frame`).
    #[cfg_attr(
        not(any(debug_assertions, feature = "progress-check", feature = "test-utils")),
        allow(dead_code)
    )]
    trace_frames: TraceFrames,
}

/// A network channel end's count of data frames, for the flight recorder
/// (`progress::trace`); nothing without it.
#[cfg(all(
    feature = "transport",
    any(debug_assertions, feature = "progress-check", feature = "test-utils")
))]
type TraceFrames = u32;
#[cfg(all(
    feature = "transport",
    not(any(debug_assertions, feature = "progress-check", feature = "test-utils"))
))]
type TraceFrames = ();

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> NetworkPush<T, D> {
    /// Whether a push dropped without `close()` may close its channel
    /// cleanly: its worker completed successfully (or is not tracked).
    fn worker_succeeded(&self) -> bool {
        self.worker_completed
            .as_ref()
            .is_none_or(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
    }

    fn encode(&self, envelope: &Envelope<T, D, ()>) -> std::result::Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        encode_envelope(&self.time_codec, &self.data_codec, envelope, &mut buf).map_err(|e| {
            Error::Dataflow(DataflowError::InvalidGraph(format!("network encode: {e}")))
        })?;
        if buf.len() > self.max_payload {
            return Err(Error::Communication(
                crate::error::CommunicationError::FrameTooLarge {
                    peer_id: self.peer_id.to_string(),
                    channel_id: self.channel_id,
                    size: buf.len(),
                    max: self.max_payload,
                },
            ));
        }
        Ok(buf)
    }

    /// Whether the peer's link is retired: it has finished, or it already
    /// holds everything this node will send.
    fn peer_retired(&self) -> bool {
        self.outbox
            .as_ref()
            .is_some_and(|outbox| outbox.is_retired())
    }

    /// The error for data that can no longer reach the peer.
    fn data_lost(&self, detail: &str) -> Error {
        crate::error::CommunicationError::DataLoss {
            peer_id: self
                .outbox
                .as_ref()
                .map(|outbox| outbox.peer().to_string())
                .unwrap_or_default(),
            detail: format!("channel {}: {detail}", self.channel_id),
        }
        .into()
    }

    /// Send one encoded envelope. `is_data` marks envelopes carrying records.
    ///
    /// A retired link means the peer finished (we cannot have finished:
    /// this push still exists), so records sent to it would never be
    /// consumed: that is data loss, not a benign race, and fails the
    /// dataflow. Control signals for a finished peer are irrelevant and
    /// discarded. Records are checked against retirement only as they are
    /// enqueued, under the link's admission lock
    /// (`PeerLinkState::try_send_data`): an enqueued record is ahead of the
    /// acknowledgement the coordinator queues after retiring the link, so
    /// the link retiring before this returns does not make it lost.
    fn send_frame(&self, payload: Vec<u8>, is_data: bool) -> std::result::Result<(), Error> {
        if !is_data && self.peer_retired() {
            return Ok(());
        }
        let frame = Frame {
            dataflow_id: self.dataflow_id,
            channel_id: self.channel_id,
            payload,
        };
        let sent = match &self.outbox {
            Some(outbox) if is_data => outbox.link().try_send_data(&self.sender, frame),
            _ => self.sender.try_send(frame),
        };
        match sent {
            Ok(()) => Ok(()),
            Err(tokio_mpsc::error::TrySendError::Full(_)) => Err(Error::Backpressure),
            Err(tokio_mpsc::error::TrySendError::Closed(_)) if self.peer_retired() => {
                if is_data {
                    Err(self.data_lost("records sent after the peer finished"))
                } else {
                    Ok(())
                }
            }
            Err(tokio_mpsc::error::TrySendError::Closed(_)) => match &self.outbox {
                // The connection to a peer that has not finished is gone while
                // this dataflow runs. That is not this operator's failure: the
                // peer may have cancelled (its Cancel may still be unread), it
                // may have crashed, or a local write failed. The coordinator
                // finds out which and cancels the dataflow with that reason;
                // until then the operator waits, as for a full queue.
                Some(outbox) => {
                    // Set before reporting: the coordinator decides whether a
                    // drain can still complete from it.
                    outbox.link().note_push_blocked();
                    if !self
                        .link_failure_reported
                        .swap(true, std::sync::atomic::Ordering::Relaxed)
                    {
                        outbox.report(crate::communication::peer_link::LinkEvent::OutboundFailed(
                            crate::communication::peer_link::LinkFailure::SendFailed,
                        ));
                    }
                    Err(Error::Backpressure)
                }
                None => Err(Error::ChannelClosed),
            },
        }
    }

    /// The flight recorder (see `progress::trace`): what a push sent, with
    /// the data frame's number on the channel, and a dump when its records
    /// were lost to a retired link. The dump is in addition to the error,
    /// which the push still returns.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_sent(&mut self, envelope: &Envelope<T, D, ()>, sent: &std::result::Result<(), Error>) {
        use crate::progress::trace::{self, Ev, Kind, Outcome, Site};
        if let Payload::Data { time, data } = &envelope.payload {
            let channel = self.channel_id;
            let outcome = Outcome::of(sent);
            if outcome == Outcome::Accepted {
                self.trace_frames = self.trace_frames.wrapping_add(1);
            }
            let frame = self.trace_frames as usize;
            trace::with(Site::NetworkPush, |recorder| {
                recorder.record(
                    Ev::new(Kind::NetSend)
                        .b(frame)
                        .time(trace::time(time))
                        .w(2, data.len() as u64)
                        .w(3, outcome as u64)
                        .w(4, channel),
                );
            });
        }
        if let Err(
            error @ Error::Communication(crate::error::CommunicationError::DataLoss { .. }),
        ) = sent
        {
            // Dumped once the worker has returned this error (outside a
            // traced worker: now).
            if trace::active() || trace::enabled() {
                let reason = error.to_string();
                if !trace::defer_current(self.dataflow_id, trace::Trigger::DataLost, &reason) {
                    trace::trigger(self.dataflow_id, trace::Trigger::DataLost, &reason);
                }
            }
        }
    }

    /// Queue the close sentinel behind this push's data.
    fn send_close(&mut self) {
        self.closed = true;
        let mut buf = Vec::new();
        encode_close(&mut buf);
        let frame = Frame {
            dataflow_id: self.dataflow_id,
            channel_id: self.channel_id,
            payload: buf,
        };
        match &self.outbox {
            // Unbounded and ordered: the sentinel cannot be lost to a full
            // queue, and the forwarder enqueues it after the data this push
            // already handed to the transport.
            Some(outbox) => {
                let _ = outbox.send(frame);
            }
            // Standalone use without an outbox. The sentinel is the channel's
            // last frame and the peer never counts a disconnect as the end of
            // its input, so it must not be dropped for a full queue: wait for
            // room behind the data already queued, on the runtime (blocking
            // here could deadlock a single-threaded runtime). The writer keeps
            // draining until every sender is gone, so this resolves unless the
            // connection fails (a genuine loss).
            None => match self.sender.try_send(frame) {
                Ok(()) => {}
                Err(tokio_mpsc::error::TrySendError::Full(frame)) => match &self.close_runtime {
                    Some(runtime) => {
                        let sender = self.sender.clone();
                        let _channel_id = self.channel_id;
                        runtime.spawn(async move {
                            if let Err(_e) = sender.send(frame).await {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    "close sentinel lost for channel {_channel_id} (connection closed)"
                                );
                            }
                        });
                    }
                    None => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "close sentinel lost for channel {} (queue full)",
                            self.channel_id
                        );
                    }
                },
                Err(tokio_mpsc::error::TrySendError::Closed(_)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        "close sentinel lost for channel {} (connection closed)",
                        self.channel_id
                    );
                }
            },
        }
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData>
    crate::dataflow::channels::pushpull::sealed::Sealed for NetworkPush<T, D>
{
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> Push<T, D, ()> for NetworkPush<T, D> {
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }
        let is_data = carries_records(&envelope);
        let bytes = self.encode(&envelope)?;
        let sent = self.send_frame(bytes, is_data);
        traced! {
            self.trace_sent(&envelope, &sent);
        }
        sent
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, ()>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, ()>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }
        let is_data = carries_records(&envelope);
        let bytes = match self.encode(&envelope) {
            Ok(b) => b,
            Err(e) => return Err((e, envelope)),
        };
        let sent = self.send_frame(bytes, is_data);
        traced! {
            self.trace_sent(&envelope, &sent);
        }
        match sent {
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
            self.send_close();
        }
    }

    fn close_on_drop(&mut self) {
        if !self.closed && self.worker_succeeded() {
            self.send_close();
        }
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn available_capacity(&self) -> Option<usize> {
        // Unknown: the per-peer queue is shared by every channel to that peer,
        // so any number would be stale by the time it is used.
        None
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> Drop for NetworkPush<T, D> {
    fn drop(&mut self) {
        if !self.closed && self.worker_succeeded() {
            // Never blocks: the outbox lane is unbounded and the fallback
            // path uses try_send. Callers should still call close().
            self.send_close();
        }
    }
}

// ---------------------------------------------------------------------------
// data_recv_bridge — async bridge from tokio mpsc to std::sync::mpsc + wake
// ---------------------------------------------------------------------------

/// Bridge task that forwards bytes from a Demuxer's tokio mpsc channel to a
/// `std::sync::mpsc` channel, notifying the executor's `WakeHandle` after
/// each forwarded frame so the sleeping executor wakes up promptly.
///
/// Without an [`InboundMonitor`] (standalone use) the bridge terminates when
/// the tokio receiver yields `None` (Demuxer closed) or when the std sender
/// is disconnected (`NetworkPull` dropped).
///
/// In a cluster dataflow the bridge keeps receiving after the `NetworkPull`
/// was dropped and reports every record-carrying frame it can no longer
/// deliver, or that arrives after the channel's close marker, as
/// [`LinkEvent::InboundDataLost`](crate::communication::peer_link::LinkEvent):
/// the receiving operator finished while the peer was still sending to it,
/// so the records would otherwise vanish silently. A channel that ends before
/// its close sentinel is reported as
/// [`LinkEvent::DataChannelEnded`](crate::communication::peer_link::LinkEvent):
/// a connection loss, not a clean end of stream. It exits when the Demuxer
/// closes the channel or, during teardown, after draining what is queued.
///
/// A final wake notification is always sent on exit so the executor can
/// observe channel closure.
#[cfg(feature = "transport")]
async fn data_recv_bridge(
    mut rx: tokio_mpsc::Receiver<Vec<u8>>,
    tx: std::sync::mpsc::Sender<Vec<u8>>,
    wake_handle: WakeHandle,
    monitor: Option<InboundMonitor>,
) {
    let Some(monitor) = monitor else {
        while let Some(bytes) = rx.recv().await {
            if tx.send(bytes).is_err() {
                // NetworkPull was dropped — stop forwarding.
                break;
            }
            wake_handle.notify();
        }
        wake_handle.notify();
        return;
    };
    let drain = monitor.bridges.drain_token();
    let _guard = monitor.guard;
    let mut channel = InboundChannel {
        tx: Some(tx),
        wake_handle: &wake_handle,
        peer: &monitor.peer,
        channel_id: monitor.channel_id,
        pull_dropped: &monitor.pull_dropped,
        validate: monitor.validate,
        closed: false,
        reported: false,
        corrupt_reported: false,
    };
    loop {
        tokio::select! {
            biased;
            bytes = rx.recv() => match bytes {
                Some(bytes) => channel.deliver(bytes),
                None => {
                    channel.ended();
                    break;
                }
            },
            _ = drain.cancelled() => {
                while let Ok(bytes) = rx.try_recv() {
                    channel.deliver(bytes);
                }
                break;
            }
        }
    }
    // Final wake so the executor re-polls and observes channel closure.
    wake_handle.notify();
}

/// Inbound delivery context of one cluster data channel.
#[cfg(feature = "transport")]
pub(crate) struct InboundMonitor {
    peer: crate::communication::peer_link::PeerMonitor,
    channel_id: u64,
    bridges: crate::communication::peer_link::InboundBridges,
    guard: crate::communication::peer_link::InboundBridgeGuard,
    /// Set by the `NetworkPull` before it drains and drops its receiver.
    pull_dropped: Arc<std::sync::atomic::AtomicBool>,
    /// Whether a frame decodes with this channel's codecs; used only for
    /// frames the consuming operator will never decode itself. Built from
    /// fresh codec instances, which is equivalent because codecs are
    /// stateless (see [`Codec`]).
    validate: fn(&[u8]) -> bool,
}

#[cfg(feature = "transport")]
struct InboundChannel<'a> {
    /// `None` once the `NetworkPull` has been dropped.
    tx: Option<std::sync::mpsc::Sender<Vec<u8>>>,
    wake_handle: &'a WakeHandle,
    peer: &'a crate::communication::peer_link::PeerMonitor,
    channel_id: u64,
    pull_dropped: &'a std::sync::atomic::AtomicBool,
    validate: fn(&[u8]) -> bool,
    closed: bool,
    reported: bool,
    corrupt_reported: bool,
}

#[cfg(feature = "transport")]
impl InboundChannel<'_> {
    fn deliver(&mut self, bytes: Vec<u8>) {
        let records = frame_carries_records(&bytes);
        #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
        let (channel, size, close) = (self.channel_id, bytes.len(), is_close_sentinel(&bytes));
        // How the frame went (see `trace::Kind::FrameIn`).
        #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
        let trace_frame = |peer: &crate::communication::peer_link::PeerMonitor, outcome: u64| {
            if let Some(recorder) = peer.recorder() {
                recorder.record(
                    crate::progress::trace::Ev::new(crate::progress::trace::Kind::FrameIn)
                        .a(usize::from(records) | usize::from(close) << 1)
                        .w(0, channel)
                        .w(1, size as u64)
                        .w(2, outcome),
                );
            }
        };
        if self.closed {
            traced! {
                trace_frame(self.peer, 3);
            }
            // Nothing follows a close sentinel (one sender per channel): the
            // sender broke the protocol (a failed sender never closes its
            // channels). Fatal at once, whatever our workers are doing.
            if !self.reported {
                self.reported = true;
                self.peer.report(
                    crate::communication::peer_link::LinkEvent::DataChannelEnded(
                        crate::communication::peer_link::LinkFailure::ProtocolViolation(format!(
                            "channel {}: a frame arrived after the channel's close marker",
                            self.channel_id
                        )),
                    ),
                );
            }
            return;
        }
        if is_close_sentinel(&bytes) {
            self.closed = true;
        }
        // A frame the operator may never decode itself (it finished, or is
        // finishing) is checked here; never on the regular path.
        let unseen =
            self.tx.is_none() || self.pull_dropped.load(std::sync::atomic::Ordering::SeqCst);
        if unseen {
            self.check(&bytes);
        }
        if let Some(tx) = &self.tx {
            match tx.send(bytes) {
                Ok(()) => {
                    // The pull may be dropping right now: it may already have
                    // drained its receiver, and this frame would vanish with it.
                    if records && self.pull_dropped.load(std::sync::atomic::Ordering::SeqCst) {
                        traced! {
                            trace_frame(self.peer, 2);
                        }
                        self.lost("records arrived as the receiving operator finished");
                    } else {
                        traced! {
                            trace_frame(self.peer, 0);
                        }
                    }
                    self.wake_handle.notify();
                    return;
                }
                Err(std::sync::mpsc::SendError(bytes)) => {
                    self.tx = None;
                    if !unseen {
                        self.check(&bytes);
                    }
                }
            }
        }
        traced! {
            trace_frame(self.peer, if records { 1 } else { 4 });
        }
        if records {
            self.lost("records arrived after the receiving operator finished");
        }
    }

    /// Report a frame that does not decode (once).
    fn check(&mut self, bytes: &[u8]) {
        if !self.corrupt_reported && !(self.validate)(bytes) {
            self.corrupt_reported = true;
            self.peer.report(
                crate::communication::peer_link::LinkEvent::DataChannelEnded(
                    crate::communication::peer_link::LinkFailure::Decode(format!(
                        "data channel {}: undecodable frame",
                        self.channel_id
                    )),
                ),
            );
        }
    }

    /// The channel closed (connection gone): without its close sentinel
    /// that is a loss, never a clean end of stream.
    fn ended(&self) {
        if !self.closed {
            self.peer.report(
                crate::communication::peer_link::LinkEvent::DataChannelEnded(
                    crate::communication::peer_link::LinkFailure::ChannelClosed,
                ),
            );
        }
    }

    fn lost(&mut self, why: &str) {
        if !self.reported {
            self.reported = true;
            let detail = format!("channel {}: {why}", self.channel_id);
            // The report first: a dump must never delay the failure.
            #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
            let traced = detail.clone();
            self.peer
                .report(crate::communication::peer_link::LinkEvent::InboundDataLost(
                    detail,
                ));
            traced! {
                self.peer.trace_lost(&traced);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NetworkPull — Pull<T, D, ()> that receives and deserializes from the wire
// ---------------------------------------------------------------------------

/// A `Pull` endpoint that receives bytes forwarded by a bridge task and
/// deserializes them through a [`Codec`] back into typed envelopes.
///
/// The bridge task receives from the Demuxer's tokio mpsc channel (async)
/// and forwards to a `std::sync::mpsc` channel (sync), notifying the
/// executor's WakeHandle on each frame. This allows `NetworkPull` to use
/// non-blocking `try_recv()` while still waking the executor promptly
/// when remote data arrives.
///
/// The endpoint is exhausted only by the close sentinel. Since the protocol
/// guarantees FIFO ordering with a single sender per channel, the close
/// sentinel is always the last frame. A channel that closes before its
/// sentinel, or a frame that fails to decode, never counts as exhaustion:
/// data may be missing, so the endpoint stops delivering and the failure is
/// reported to the cluster lifecycle coordinator, which fails the dataflow.
/// Without a coordinator (standalone use of [`NetworkEdgeMaterializer`]) the
/// endpoint fails closed: it stays unexhausted, so the dataflow cannot
/// complete with missing data. That happens only on a genuine loss: a
/// standalone push never drops its sentinel for a full queue.
#[cfg(feature = "transport")]
pub struct NetworkPull<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    receiver: std::sync::mpsc::Receiver<Vec<u8>>,
    exhausted: bool,
    /// The channel ended without its sentinel or delivered an undecodable
    /// frame; nothing more is delivered and it is never exhausted.
    failed: bool,
    channel_id: u64,
    /// Cluster dataflows: reports failures and records left unconsumed.
    monitor: Option<PullMonitor>,
    /// Keeps transport alive (background tasks survive as long as this exists).
    _transport: Option<Arc<crate::communication::cluster_transport::ClusterTransport>>,
    /// The flight recorder's count of data frames received (`frame`).
    #[cfg_attr(
        not(any(debug_assertions, feature = "progress-check", feature = "test-utils")),
        allow(dead_code)
    )]
    trace_frames: TraceFrames,
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> NetworkPull<T, D> {
    /// The flight recorder (`progress::trace`): what this pull received
    /// (`what`: 0 records, 1 the close sentinel, 2 an undecodable frame, 3
    /// the channel closed without its sentinel), with the data frame's
    /// number on the channel.
    #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
    fn trace_received(&mut self, what: usize, records: Option<(&T, usize)>) {
        use crate::progress::trace::{self, Ev, Kind, Site};
        let channel = self.channel_id;
        if what == 0 {
            self.trace_frames = self.trace_frames.wrapping_add(1);
        }
        let frame = self.trace_frames as usize;
        trace::with(Site::NetworkPull, |recorder| {
            let mut ev = Ev::new(Kind::NetRecv).a(what).b(frame).w(4, channel);
            if let Some((time, n)) = records {
                ev = ev.time(trace::time(time)).w(2, n as u64);
            }
            recorder.record(ev);
        });
    }

    fn fail(&mut self, failure: crate::communication::peer_link::LinkFailure) {
        self.failed = true;
        match &self.monitor {
            Some(monitor) => {
                monitor
                    .peer
                    .report(crate::communication::peer_link::LinkEvent::DataChannelEnded(failure));
            }
            None => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    channel_id = self.channel_id,
                    %failure,
                    "network data channel failed; it will never be exhausted"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = failure;
            }
        }
    }
}

/// Data-loss reporting for a `NetworkPull`.
#[cfg(feature = "transport")]
struct PullMonitor {
    peer: crate::communication::peer_link::PeerMonitor,
    channel_id: u64,
    /// Shared with the inbound bridge (see [`InboundMonitor`]).
    dropped: Arc<std::sync::atomic::AtomicBool>,
}

// Records still buffered when the consuming operator is dropped never reach
// it. In a correct run an operator cannot finish while records addressed to
// it are in flight, so this is reported as data loss (see `data_recv_bridge`).
#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> Drop for NetworkPull<T, D> {
    fn drop(&mut self) {
        let Some(monitor) = &self.monitor else {
            return;
        };
        // Before draining: a frame the bridge hands over from now on is
        // reported by the bridge itself.
        monitor
            .dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut unconsumed = 0usize;
        let mut corrupt = None;
        while let Ok(bytes) = self.receiver.try_recv() {
            match decode_envelope(&self.time_codec, &self.data_codec, &bytes) {
                Ok(Some(envelope)) if carries_records(&envelope) => unconsumed += 1,
                Ok(_) => {}
                Err(e) => {
                    corrupt.get_or_insert(e);
                }
            }
        }
        if let Some(e) = corrupt {
            monitor.peer.report(
                crate::communication::peer_link::LinkEvent::DataChannelEnded(
                    crate::communication::peer_link::LinkFailure::Decode(format!(
                        "data channel {}: {e}",
                        monitor.channel_id
                    )),
                ),
            );
        }
        if unconsumed > 0 {
            let detail = format!(
                "channel {}: {unconsumed} frame(s) of records were received but never \
                 consumed",
                monitor.channel_id
            );
            // The report first: a dump must never delay the failure.
            #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
            let traced = detail.clone();
            monitor
                .peer
                .report(crate::communication::peer_link::LinkEvent::InboundDataLost(
                    detail,
                ));
            traced! {
                monitor.peer.trace_lost(&traced);
            }
        }
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData>
    crate::dataflow::channels::pushpull::sealed::Sealed for NetworkPull<T, D>
{
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> Pull<T, D, ()> for NetworkPull<T, D> {
    fn pull(&mut self) -> Option<Envelope<T, D, ()>> {
        if self.exhausted || self.failed {
            return None;
        }
        let bytes = match self.receiver.try_recv() {
            Ok(b) => b,
            Err(std::sync::mpsc::TryRecvError::Empty) => return None,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                // The channel closed before its close sentinel (the bridge
                // already reported it). Not an end of stream: data may be
                // missing.
                self.failed = true;
                traced! {
                    self.trace_received(3, None);
                }
                #[cfg(feature = "tracing")]
                tracing::error!(
                    channel_id = self.channel_id,
                    "network data channel closed before its close sentinel"
                );
                return None;
            }
        };
        match decode_envelope(&self.time_codec, &self.data_codec, &bytes) {
            Ok(Some(env)) => {
                traced! {
                    if let Payload::Data { time, data } = &env.payload {
                        self.trace_received(0, Some((time, data.len())));
                    }
                }
                Some(env)
            }
            Ok(None) => {
                // Close sentinel — single sender per channel, FIFO order,
                // so no more data follows.
                self.exhausted = true;
                traced! {
                    self.trace_received(1, None);
                }
                None
            }
            Err(e) => {
                traced! {
                    self.trace_received(2, None);
                }
                self.fail(crate::communication::peer_link::LinkFailure::Decode(
                    format!("data channel {}: {e}", self.channel_id),
                ));
                None
            }
        }
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

// ---------------------------------------------------------------------------
// NetworkEdgeMaterializer
// ---------------------------------------------------------------------------

/// Edge materializer that uses real network transport for cross-node exchange.
///
/// Same-node worker pairs use direct `BoundedPush`/`BoundedPull` (zero-copy
/// shared memory). Cross-node worker pairs use [`NetworkPush`]/[`NetworkPull`]
/// that serialize through [`Codec`] and transport data over TCP via
/// [`TransportSession`].
///
/// # Connection setup
///
/// The caller provides a [`TransportSession`] (wrapped in `Arc`) that owns
/// the per-peer Muxer/Demuxer infrastructure. This allows the session to be
/// shared with progress channels and control protocols.
///
/// # Channel ID scheme
///
/// Each `(edge, source_worker, target_worker)` triple gets a deterministic
/// channel ID in the data namespace, see [`channel_id`](Self::channel_id) and
/// [`data_channel_id`](crate::communication::transport_session::data_channel_id).
///
/// # Example
///
/// ```ignore
/// use instancy::communication::{PeerConnection, TransportSession, ChannelRegistration};
///
/// // Create transport session first
/// let (session, receivers) = TransportSession::new(
///     dataflow_id, connections, &data_regs, &[], capacity, &rt,
/// )?;
/// let session = Arc::new(session);
///
/// // Then create materializer using the session
/// let mat = NetworkEdgeMaterializer::<u64, String>::new(
///     dataflow_id, topology, "node-a", session, receivers, capacity,
/// )?;
/// ```
#[cfg(feature = "transport")]
pub struct NetworkEdgeMaterializer<T: Timestamp + ExchangeData, D: ExchangeData> {
    num_workers: usize,
    local_node_id: String,
    topology: ClusterTopology,
    dataflow_id: DataflowId,
    /// Index of this exchange edge in the dataflow graph.
    /// Used to compute per-edge channel IDs that avoid collisions when
    /// multiple exchange edges exist in the same dataflow.
    edge_index: usize,

    /// Unified transport (dedicated or shared).
    transport: Arc<crate::communication::cluster_transport::ClusterTransport>,

    /// Demuxer channel receivers for remote pull endpoints.
    /// Key: (source_worker, target_worker) → `tokio::mpsc::Receiver<Vec<u8>>`
    demux_receivers: std::collections::HashMap<(usize, usize), tokio_mpsc::Receiver<Vec<u8>>>,

    /// Local channels for same-node pairs: [src][dst]
    local_push: Vec<Vec<Option<BoundedPush<T, D, ()>>>>,
    local_pull: Vec<Vec<Option<BoundedPull<T, D, ()>>>>,

    /// Track which workers have had push endpoints materialized.
    taken_push: Vec<bool>,
    /// Track which workers have had pull endpoints materialized.
    taken_pull: Vec<bool>,

    /// Wake handles for all workers (indexed by global worker ID).
    /// Used to notify the executor when remote data arrives via bridge tasks.
    wake_handles: Vec<WakeHandle>,

    /// Tokio runtime handle for spawning bridge tasks that forward remote
    /// data from tokio mpsc channels and notify the executor's WakeHandle.
    runtime_handle: tokio::runtime::Handle,

    /// Per-peer ordered lanes used for close sentinels (cluster dataflows).
    peer_outboxes: std::collections::HashMap<String, crate::communication::peer_link::PeerOutbox>,

    /// Inbound bridge tracking (cluster dataflows): undeliverable records are
    /// reported, and teardown waits for the bridges.
    inbound_bridges: Option<crate::communication::peer_link::InboundBridges>,
    /// Per-worker success flags (global index; empty for standalone use).
    worker_completed: Vec<Arc<std::sync::atomic::AtomicBool>>,
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> NetworkEdgeMaterializer<T, D> {
    /// Create a network edge materializer from a pre-built [`ClusterTransport`](crate::communication::cluster_transport::ClusterTransport).
    ///
    /// # Arguments
    /// - `dataflow_id`: Unique ID for this dataflow (used in frame routing).
    /// - `topology`: Cluster topology describing all nodes and workers.
    /// - `local_node_id`: The node ID of this process.
    /// - `transport`: Unified transport (dedicated or shared).
    /// - `receivers`: Channel receivers keyed by `peer_node_id → channel_id`.
    ///   Only data channel receivers are consumed here.
    /// - `capacity`: Buffer capacity for local bounded channels.
    ///
    /// # Errors
    ///
    /// Fails if the edge's channel ids do not fit in the data channel-id
    /// namespace (see [`channel_id`](Self::channel_id)).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataflow_id: DataflowId,
        topology: ClusterTopology,
        local_node_id: impl Into<String>,
        transport: Arc<crate::communication::cluster_transport::ClusterTransport>,
        mut receivers: std::collections::HashMap<
            String,
            std::collections::HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>,
        >,
        capacity: usize,
        edge_index: usize,
        wake_handles: Vec<WakeHandle>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Self> {
        let local_node_id = local_node_id.into();
        let num_workers = topology.total_workers();
        if num_workers > 0 {
            // The largest id of this edge; every other id is smaller.
            Self::channel_id(edge_index, num_workers - 1, num_workers - 1, num_workers)?;
        }

        // --- Set up local channels for same-node pairs ---
        let mut local_push: Vec<Vec<Option<BoundedPush<T, D, ()>>>> = (0..num_workers)
            .map(|_| (0..num_workers).map(|_| None).collect())
            .collect();
        let mut local_pull: Vec<Vec<Option<BoundedPull<T, D, ()>>>> = (0..num_workers)
            .map(|_| (0..num_workers).map(|_| None).collect())
            .collect();

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

        // --- Extract demux receivers for remote pull endpoints ---
        let mut demux_receivers = std::collections::HashMap::new();
        let local_range = topology.worker_range(&local_node_id);
        if let Some((local_start, local_end)) = local_range {
            for src in 0..num_workers {
                let src_node = topology.node_for_worker(WorkerId::new(src));
                if src_node != Some(local_node_id.as_str()) {
                    let peer_id = src_node.unwrap_or("");
                    for dst in local_start..local_end {
                        let channel_id = Self::channel_id(edge_index, src, dst, num_workers)?;
                        if let Some(peer_map) = receivers.get_mut(peer_id) {
                            if let Some(rx) = peer_map.remove(&channel_id) {
                                demux_receivers.insert((src, dst), rx);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            num_workers,
            local_node_id,
            topology,
            dataflow_id,
            edge_index,
            transport,
            demux_receivers,
            local_push,
            local_pull,
            taken_push: vec![false; num_workers],
            taken_pull: vec![false; num_workers],
            wake_handles,
            runtime_handle,
            peer_outboxes: std::collections::HashMap::new(),
            inbound_bridges: None,
            worker_completed: Vec::new(),
        })
    }

    /// Route close sentinels through the given per-peer ordered lanes.
    pub(crate) fn with_peer_outboxes(
        mut self,
        peer_outboxes: std::collections::HashMap<
            String,
            crate::communication::peer_link::PeerOutbox,
        >,
    ) -> Self {
        self.peer_outboxes = peer_outboxes;
        self
    }

    /// Track inbound bridges and report records that can no longer be
    /// delivered (requires the per-peer outboxes for reporting).
    pub(crate) fn with_inbound_bridges(
        mut self,
        inbound_bridges: crate::communication::peer_link::InboundBridges,
    ) -> Self {
        self.inbound_bridges = Some(inbound_bridges);
        self
    }

    /// Per-worker success flags (indexed by global worker ID): a push that is
    /// dropped without an explicit `close()` sends its close sentinel only if
    /// its worker completed successfully.
    pub(crate) fn with_worker_completion(
        mut self,
        worker_completed: Vec<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Self {
        self.worker_completed = worker_completed;
        self
    }

    /// Convenience constructor that creates a [`TransportSession`] internally.
    ///
    /// Use this when you don't need to share the session with progress channels
    /// or control protocols (e.g., single-dataflow setups or testing).
    ///
    /// # Errors
    ///
    /// Fails if the transport session rejects its registrations (see
    /// [`TransportSession::new`]) or the channel ids overflow.
    pub fn with_connections<R, W>(
        dataflow_id: DataflowId,
        topology: ClusterTopology,
        local_node_id: impl Into<String>,
        connections: Vec<PeerConnection<R, W>>,
        capacity: usize,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Result<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let local_node_id_str: String = local_node_id.into();
        let num_workers = topology.total_workers();

        // Build data channel registrations for all remote pairs where
        // source is on a peer and destination is on this node.
        let local_range = topology.worker_range(&local_node_id_str);
        let peer_node_ids: Vec<String> = connections.iter().map(|c| c.node_id.clone()).collect();

        let mut data_regs = Vec::new();
        if let Some((local_start, local_end)) = local_range {
            for peer_id in &peer_node_ids {
                if let Some((peer_start, peer_end)) = topology.worker_range(peer_id) {
                    for src in peer_start..peer_end {
                        for dst in local_start..local_end {
                            let channel_id = Self::channel_id(0, src, dst, num_workers)?;
                            data_regs.push(ChannelRegistration {
                                peer_node_id: peer_id.clone(),
                                channel_id,
                            });
                        }
                    }
                }
            }
        }

        let (session, receivers) = TransportSession::new(
            dataflow_id,
            connections,
            &data_regs,
            &[],
            capacity,
            runtime_handle,
        )
        .map_err(crate::error::CommunicationError::from)?;

        let transport = Arc::new(
            crate::communication::cluster_transport::ClusterTransport::Dedicated(Arc::new(session)),
        );
        let wake_handles: Vec<WakeHandle> = (0..num_workers).map(|_| WakeHandle::new()).collect();
        Self::new(
            dataflow_id,
            topology,
            local_node_id_str,
            transport,
            receivers,
            capacity,
            0,
            wake_handles,
            runtime_handle.clone(),
        )
    }

    /// Deterministic channel ID for a (source, target) worker pair on a given
    /// exchange edge (`edge_index` is the edge's position among the dataflow's
    /// exchange edges).
    ///
    /// Equivalent to
    /// [`data_channel_id`](crate::communication::transport_session::data_channel_id):
    /// `DATA_CHANNEL_BASE + (edge_index * num_workers + source) * num_workers + target`.
    /// Data ids live in their own namespace, so they can never collide with the
    /// control, progress, or probe channels.
    ///
    /// # Errors
    ///
    /// Fails if `source` or `target` is not below `num_workers` or the id does
    /// not fit in the data namespace.
    pub fn channel_id(
        edge_index: usize,
        source: usize,
        target: usize,
        num_workers: usize,
    ) -> Result<u64> {
        crate::communication::transport_session::data_channel_id(
            edge_index,
            source,
            target,
            num_workers,
        )
    }
}

#[cfg(feature = "transport")]
impl<T: Timestamp + ExchangeData, D: ExchangeData> EdgeMaterializer<T, D>
    for NetworkEdgeMaterializer<T, D>
{
    fn num_source_workers(&self) -> usize {
        self.num_workers
    }

    fn num_target_workers(&self) -> usize {
        self.num_workers
    }

    fn materialize_source_worker(
        &mut self,
        src_idx: usize,
    ) -> Result<Vec<Box<dyn Push<T, D, ()>>>> {
        if src_idx >= self.num_workers {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "source worker index {src_idx} out of range (num_workers={})",
                self.num_workers
            ))));
        }
        if self.taken_push[src_idx] {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "source worker {src_idx} push already materialized"
            ))));
        }
        self.taken_push[src_idx] = true;

        let worker_node = self
            .topology
            .node_for_worker(WorkerId::new(src_idx))
            .ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidConfig(format!(
                    "topology missing worker mapping for source worker {src_idx}"
                )))
            })?;

        let mut pushers: Vec<Box<dyn Push<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for dst in 0..self.num_workers {
            let dst_node = self
                .topology
                .node_for_worker(WorkerId::new(dst))
                .ok_or_else(|| {
                    Error::Dataflow(DataflowError::InvalidConfig(format!(
                        "topology missing worker mapping for target worker {dst}"
                    )))
                })?;

            if worker_node == dst_node && worker_node == self.local_node_id {
                let push = self.local_push[src_idx][dst].take().ok_or_else(|| {
                    Error::Dataflow(DataflowError::EndpointTaken(format!(
                        "local push [{src_idx}][{dst}] already taken"
                    )))
                })?;
                // A sibling worker must not see a failed worker's stream as
                // complete either: it would finish (and close its own
                // outputs to peers cleanly) without that worker's data.
                pushers.push(Box::new(
                    push.with_close_gate(self.worker_completed.get(src_idx).cloned()),
                ));
            } else {
                let sender = self.transport.data_sender(dst_node).ok_or_else(|| {
                    Error::Dataflow(DataflowError::InvalidGraph(format!(
                        "no connection to peer node '{dst_node}'"
                    )))
                })?;

                let channel_id = Self::channel_id(self.edge_index, src_idx, dst, self.num_workers)?;
                pushers.push(Box::new(NetworkPush::<T, D> {
                    time_codec: T::codec(),
                    data_codec: D::codec(),
                    dataflow_id: self.dataflow_id,
                    channel_id,
                    sender,
                    outbox: self.peer_outboxes.get(dst_node).cloned(),
                    link_failure_reported: std::sync::atomic::AtomicBool::new(false),
                    peer_id: Arc::from(dst_node),
                    max_payload: self.transport.max_payload_len(),
                    worker_completed: self.worker_completed.get(src_idx).cloned(),
                    closed: false,
                    close_runtime: (!self.peer_outboxes.contains_key(dst_node))
                        .then(|| self.runtime_handle.clone()),
                    trace_frames: Default::default(),
                }));
            }
        }

        Ok(pushers)
    }

    fn materialize_target_worker(
        &mut self,
        dst_idx: usize,
    ) -> Result<Vec<Box<dyn Pull<T, D, ()>>>> {
        if dst_idx >= self.num_workers {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "target worker index {dst_idx} out of range (num_workers={})",
                self.num_workers
            ))));
        }
        if self.taken_pull[dst_idx] {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "target worker {dst_idx} pull already materialized"
            ))));
        }
        self.taken_pull[dst_idx] = true;

        let worker_node = self
            .topology
            .node_for_worker(WorkerId::new(dst_idx))
            .ok_or_else(|| {
                Error::Dataflow(DataflowError::InvalidConfig(format!(
                    "topology missing worker mapping for target worker {dst_idx}"
                )))
            })?;

        let mut pullers: Vec<Box<dyn Pull<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for src in 0..self.num_workers {
            let src_node = self
                .topology
                .node_for_worker(WorkerId::new(src))
                .ok_or_else(|| {
                    Error::Dataflow(DataflowError::InvalidConfig(format!(
                        "topology missing worker mapping for source worker {src}"
                    )))
                })?;

            if src_node == worker_node && src_node == self.local_node_id {
                let pull = self.local_pull[src][dst_idx].take().ok_or_else(|| {
                    Error::Dataflow(DataflowError::EndpointTaken(format!(
                        "local pull [{src}][{dst_idx}] already taken"
                    )))
                })?;
                pullers.push(Box::new(pull));
            } else {
                let tokio_receiver =
                    self.demux_receivers
                        .remove(&(src, dst_idx))
                        .ok_or_else(|| {
                            Error::Dataflow(DataflowError::InvalidGraph(format!(
                                "no demux receiver for [{src}][{dst_idx}]"
                            )))
                        })?;

                let (std_tx, std_rx) = std::sync::mpsc::channel::<Vec<u8>>();
                let wake = self.wake_handles[dst_idx].clone();
                let channel_id = Self::channel_id(self.edge_index, src, dst_idx, self.num_workers)?;
                let peer = self
                    .peer_outboxes
                    .get(src_node)
                    .map(crate::communication::peer_link::PeerOutbox::monitor);
                let pull_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let monitor = match (&self.inbound_bridges, &peer) {
                    (Some(bridges), Some(peer)) => Some(InboundMonitor {
                        peer: peer.clone(),
                        channel_id,
                        bridges: bridges.clone(),
                        guard: bridges.register(),
                        pull_dropped: Arc::clone(&pull_dropped),
                        validate: |bytes: &[u8]| {
                            decode_envelope::<T, D, _, _>(&T::codec(), &D::codec(), bytes).is_ok()
                        },
                    }),
                    _ => None,
                };
                self.runtime_handle.spawn(async move {
                    data_recv_bridge(tokio_receiver, std_tx, wake, monitor).await;
                });

                pullers.push(Box::new(NetworkPull::<T, D> {
                    time_codec: T::codec(),
                    data_codec: D::codec(),
                    receiver: std_rx,
                    exhausted: false,
                    failed: false,
                    channel_id,
                    monitor: peer.map(|peer| PullMonitor {
                        peer,
                        channel_id,
                        dropped: pull_dropped,
                    }),
                    _transport: Some(self.transport.clone()),
                    trace_frames: Default::default(),
                }));
            }
        }

        Ok(pullers)
    }

    fn materialize_worker(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)> {
        let pushers = self.materialize_source_worker(worker_idx)?;
        let pullers = self.materialize_target_worker(worker_idx)?;
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
            Payload::Control(ControlSignal::Error {
                source_operator,
                message,
            }) => {
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

            let mat_a = NetworkEdgeMaterializer::<u64, u64>::with_connections(
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
            )
            .unwrap();

            let mat_b = NetworkEdgeMaterializer::<u64, u64>::with_connections(
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
            )
            .unwrap();

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
            let env = poll_pull(pull2[0].as_mut(), timeout)
                .await
                .expect("data should arrive");
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
            let env = poll_pull(pull2[0].as_mut(), timeout)
                .await
                .expect("watermark should arrive");
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
            let env = poll_pull(pull2[0].as_mut(), timeout)
                .await
                .expect("data should arrive");
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

        /// Standalone use (no coordinator): a push whose queue is full when it
        /// closes still delivers its close sentinel, so the peer's pull is
        /// exhausted with every record. Capacity 1 and the single-threaded
        /// test runtime make the queue full at the close: the writer cannot
        /// run in between.
        #[tokio::test]
        async fn standalone_close_sentinel_survives_a_full_queue() {
            const BATCHES: u64 = 50;
            let topo = two_node_topology();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();
            let (mut mat_a, mut mat_b) = create_connected_materializers(df_id, topo, 1, &rt);
            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            for i in 0..BATCHES {
                loop {
                    match push0[2].push(Envelope::data(1, vec![i])) {
                        Ok(()) => break,
                        Err(Error::Backpressure) => tokio::task::yield_now().await,
                        Err(e) => panic!("push failed: {e}"),
                    }
                }
            }
            // The queue holds the last batch: the sentinel cannot go in now.
            push0[2].close();

            let mut received = Vec::new();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            while !pull2[0].is_exhausted() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never exhausted, with {} of {BATCHES} records",
                    received.len()
                );
                match pull2[0].pull() {
                    Some(env) => {
                        if let Some((_, data)) = env.as_data() {
                            received.extend(data.iter().copied());
                        }
                    }
                    None => tokio::time::sleep(std::time::Duration::from_millis(2)).await,
                }
            }
            assert_eq!(received, (0..BATCHES).collect::<Vec<_>>());
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
            let env_at_b = poll_pull(pull2[0].as_mut(), timeout)
                .await
                .expect("A→B data");
            assert_eq!(env_at_b.as_data(), Some((&1u64, &vec![100u64])));

            let env_at_a = poll_pull(pull0[2].as_mut(), timeout)
                .await
                .expect("B→A data");
            assert_eq!(env_at_a.as_data(), Some((&2u64, &vec![200u64])));
        }

        #[tokio::test]
        async fn network_materializer_with_string_data() {
            let topo =
                ClusterTopology::multi_node(vec![NodeConfig::new("a", 1), NodeConfig::new("b", 1)])
                    .unwrap();
            let df_id = DataflowId::new();
            let rt = tokio::runtime::Handle::current();

            let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
            let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

            let mut mat_a = NetworkEdgeMaterializer::<u64, String>::with_connections(
                df_id,
                topo.clone(),
                "a",
                vec![PeerConnection {
                    node_id: "b".to_string(),
                    reader: a_from_b,
                    writer: a_to_b,
                }],
                16,
                &rt,
            )
            .unwrap();
            let mut mat_b = NetworkEdgeMaterializer::<u64, String>::with_connections(
                df_id,
                topo,
                "b",
                vec![PeerConnection {
                    node_id: "a".to_string(),
                    reader: b_from_a,
                    writer: b_to_a,
                }],
                16,
                &rt,
            )
            .unwrap();

            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let (_, mut pull1) = mat_b.materialize_worker(1).unwrap();

            push0[1]
                .push(Envelope::data(7, vec!["hello".into(), "world".into()]))
                .unwrap();

            let timeout = std::time::Duration::from_secs(2);
            let env = poll_pull(pull1[0].as_mut(), timeout)
                .await
                .expect("string data should arrive");
            assert_eq!(
                env.as_data(),
                Some((&7u64, &vec!["hello".to_string(), "world".to_string()]))
            );
        }

        use crate::communication::peer_link::{
            InboundBridges, LifecycleEvent, LinkEvent, PeerOutbox, spawn_peer_outbox,
        };

        /// An outbox for `peer` whose events are observable; the frames it
        /// forwards go to a sink.
        fn monitored_outbox(
            peer: &str,
        ) -> (
            PeerOutbox,
            tokio_mpsc::UnboundedReceiver<LifecycleEvent>,
            tokio_mpsc::Receiver<Frame>,
        ) {
            let (events, events_rx) = tokio_mpsc::unbounded_channel();
            let (sink_tx, sink_rx) = tokio_mpsc::channel(64);
            let (outbox, _forwarder) = spawn_peer_outbox(
                peer,
                crate::communication::cluster_transport::FrameSender::Direct(sink_tx),
                events,
                &tokio::runtime::Handle::current(),
            );
            (outbox, events_rx, sink_rx)
        }

        fn push_to(
            outbox: &PeerOutbox,
            sender: tokio_mpsc::Sender<Frame>,
        ) -> NetworkPush<u64, u64> {
            NetworkPush {
                time_codec: u64::codec(),
                data_codec: u64::codec(),
                dataflow_id: DataflowId::new(),
                channel_id: 5,
                sender: crate::communication::cluster_transport::FrameSender::Direct(sender),
                outbox: Some(outbox.clone()),
                link_failure_reported: std::sync::atomic::AtomicBool::new(false),
                close_runtime: None,
                peer_id: Arc::from(outbox.peer()),
                max_payload: usize::MAX,
                worker_completed: None,
                closed: false,
                trace_frames: Default::default(),
            }
        }

        #[tokio::test]
        async fn push_of_records_to_a_retired_peer_is_data_loss() {
            let (outbox, _events, _sink) = monitored_outbox("node-b");
            let (tx, mut rx) = tokio_mpsc::channel(4);
            let mut push = push_to(&outbox, tx);
            push.push(Envelope::data(1, vec![1])).unwrap();
            assert!(rx.try_recv().is_ok());

            outbox.link().retire();
            let error = push.push(Envelope::data(2, vec![2])).unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Communication(crate::error::CommunicationError::DataLoss {
                        ref peer_id,
                        ..
                    }) if peer_id == "node-b"
                ),
                "{error:?}"
            );
            let (error, returned) = push.try_push(Envelope::data(3, vec![3])).unwrap_err();
            assert!(matches!(error, Error::Communication(_)));
            assert_eq!(returned.as_data(), Some((&3u64, &vec![3u64])));
            assert!(rx.try_recv().is_err(), "nothing sent to a finished peer");
            // Control signals and empty batches for a finished peer are irrelevant.
            push.push(Envelope::watermark(4)).unwrap();
            push.push(Envelope::data(5, Vec::new())).unwrap();
        }

        /// The channel and payload of the next frame on `rx`.
        async fn next_frame(rx: &mut tokio_mpsc::Receiver<Frame>) -> (u64, Vec<u8>) {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
                .await
                .expect("a frame arrives")
                .expect("the queue is open");
            (frame.channel_id, frame.payload)
        }

        /// Records a push enqueued before the link retired are ahead of the
        /// acknowledgement queued after it, whether or not the peer read them
        /// first; a record after retirement is refused, not queued behind it.
        #[tokio::test]
        async fn admitted_records_precede_the_retirement_ack() {
            const ACK_CHANNEL: u64 = 1;
            let encoded = |envelope: Envelope<u64, u64, ()>| {
                let mut buf = Vec::new();
                encode_envelope(&u64::codec(), &u64::codec(), &envelope, &mut buf).unwrap();
                buf
            };
            let ack = || {
                (
                    ACK_CHANNEL,
                    crate::progress::network_progress::encode_progress_ack(),
                )
            };
            for read_before_retirement in [false, true] {
                // The push and the outbox share one transport queue, as in a
                // cluster dataflow.
                let (tx, mut rx) = tokio_mpsc::channel(8);
                let (events, _events) = tokio_mpsc::unbounded_channel();
                let (outbox, _forwarder) = spawn_peer_outbox(
                    "node-b",
                    crate::communication::cluster_transport::FrameSender::Direct(tx.clone()),
                    events,
                    &tokio::runtime::Handle::current(),
                );
                let mut push = push_to(&outbox, tx);
                push.push(Envelope::data(1, vec![10, 11])).unwrap();
                push.push(Envelope::data(2, vec![12])).unwrap();
                let mut before = Vec::new();
                if read_before_retirement {
                    before.push(next_frame(&mut rx).await);
                    before.push(next_frame(&mut rx).await);
                }

                outbox.link().retire();
                let (channel_id, payload) = ack();
                outbox
                    .send(Frame {
                        dataflow_id: push.dataflow_id,
                        channel_id,
                        payload,
                    })
                    .unwrap();
                let late = push.push(Envelope::data(3, vec![13]));
                assert!(
                    matches!(
                        late,
                        Err(Error::Communication(
                            crate::error::CommunicationError::DataLoss { .. }
                        ))
                    ),
                    "{late:?}"
                );

                let mut after = Vec::new();
                loop {
                    let frame = next_frame(&mut rx).await;
                    let is_ack = frame.0 == ACK_CHANNEL;
                    after.push(frame);
                    if is_ack {
                        break;
                    }
                }
                assert!(rx.try_recv().is_err(), "nothing behind the ack");
                let records = vec![
                    (push.channel_id, encoded(Envelope::data(1, vec![10, 11]))),
                    (push.channel_id, encoded(Envelope::data(2, vec![12]))),
                ];
                if read_before_retirement {
                    assert_eq!(before, records);
                    assert_eq!(after, vec![ack()]);
                } else {
                    let mut expected = records;
                    expected.push(ack());
                    assert_eq!(after, expected);
                }
            }
        }

        /// The flight recorder (`progress::trace`): records pushed to a
        /// retired link fail the push and write a dump at once, which names
        /// the data loss and shows what retired the link.
        #[cfg(any(debug_assertions, feature = "progress-check", feature = "test-utils"))]
        #[tokio::test]
        async fn data_lost_to_a_retired_link_is_dumped() {
            use crate::progress::trace;
            let dir = std::env::temp_dir().join(format!(
                "instancy-trace-data-lost-{}",
                DataflowId::new().as_uuid()
            ));
            let id = DataflowId::new();
            let worker = trace::recorder_for_test(id, "node-a", Some(0), "", 2, &dir);
            let link = trace::recorder_for_test(id, "node-a", None, "node-b", 2, &dir);
            let (outbox, _events, _sink) = monitored_outbox("node-b");
            outbox.link().attach_trace(Some(link));
            let (tx, _rx) = tokio_mpsc::channel(4);
            let mut push = push_to(&outbox, tx);
            push.dataflow_id = id;
            let recording = trace::enter(&Some(Arc::clone(&worker)));
            push.push(Envelope::data(1, vec![1, 2])).unwrap();
            // The test hook: the link retires (as on the peer's goodbye).
            outbox.link().retire();
            let error = push.push(Envelope::data(2, vec![3])).unwrap_err();
            drop(recording);
            assert!(error.to_string().contains("data lost"), "{error}");
            // The dump waits for the worker to return the error (its
            // executor fires it as it records its exit).
            let root = dir.join(id.as_uuid().to_string());
            assert!(!root.exists(), "no dump before the failure is returned");
            worker.fire_pending();

            let reason = std::fs::read_to_string(root.join("reason.txt")).unwrap();
            assert!(
                reason.starts_with("data-lost:")
                    && reason.contains("records sent after the peer finished"),
                "{reason}"
            );
            let sent = std::fs::read_to_string(root.join("node-a-w0.jsonl")).unwrap();
            let outcomes: Vec<&str> = sent
                .lines()
                .filter(|l| l.contains("\"ev\":\"net-send\""))
                .map(|l| {
                    if l.contains("\"outcome\":\"accepted\"") {
                        "accepted"
                    } else if l.contains("\"outcome\":\"lost-after-finish\"") {
                        "lost"
                    } else {
                        l
                    }
                })
                .collect();
            assert_eq!(outcomes, ["accepted", "lost"], "{sent}");
            let link = std::fs::read_to_string(root.join("node-a-bridge-node-b.jsonl")).unwrap();
            assert!(
                link.contains("\"ev\":\"retired\",\"was_retired\":false"),
                "the dump shows the link retired: {link}"
            );
            // The last recorder (the link's, held by the outbox's forwarder
            // task until it ends) writes the final dump.
            drop((worker, push, outbox));
            let last = root.join("2-final").join("node-a-bridge-node-b.jsonl");
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !last.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("a final dump once the dataflow's last recorder drops");
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[tokio::test]
        async fn push_of_records_to_a_retired_closed_link_is_data_loss() {
            let (outbox, mut events, _sink) = monitored_outbox("node-b");
            let (tx, rx) = tokio_mpsc::channel(4);
            drop(rx);
            let mut push = push_to(&outbox, tx);
            // A closed link to a peer that has not finished is the
            // coordinator's to judge (reported once); the operator waits.
            for _ in 0..3 {
                assert!(matches!(
                    push.push(Envelope::data(1, vec![1])),
                    Err(Error::Backpressure)
                ));
            }
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::Link {
                    event: LinkEvent::OutboundFailed(
                        crate::communication::peer_link::LinkFailure::SendFailed
                    ),
                    ..
                })
            ));
            assert!(events.try_recv().is_err(), "reported once");
            outbox.link().retire();
            assert!(matches!(
                push.push(Envelope::data(1, vec![1])),
                Err(Error::Communication(_))
            ));
            push.push(Envelope::watermark(2)).unwrap();
        }

        fn materializers_with_monitoring(
            rt: &tokio::runtime::Handle,
        ) -> (
            NetworkEdgeMaterializer<u64, u64>,
            NetworkEdgeMaterializer<u64, u64>,
            tokio_mpsc::UnboundedReceiver<LifecycleEvent>,
            InboundBridges,
        ) {
            let (mat_a, mat_b) =
                create_connected_materializers(DataflowId::new(), two_node_topology(), 16, rt);
            let (outbox, events, sink) = monitored_outbox("node-a");
            // Keep the outbox's sink open for the rest of the test.
            std::mem::forget(sink);
            let bridges = InboundBridges::default();
            let mat_b = mat_b
                .with_peer_outboxes(std::collections::HashMap::from([(
                    "node-a".to_string(),
                    outbox,
                )]))
                .with_inbound_bridges(bridges.clone());
            (mat_a, mat_b, events, bridges)
        }

        async fn next_loss(events: &mut tokio_mpsc::UnboundedReceiver<LifecycleEvent>) -> String {
            let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .expect("a data loss is reported");
            match event {
                Some(LifecycleEvent::Link {
                    peer,
                    event: LinkEvent::InboundDataLost(detail),
                }) if peer == "node-a" => detail,
                other => panic!("unexpected event {other:?}"),
            }
        }

        #[tokio::test]
        async fn records_left_unconsumed_or_arriving_after_the_pull_are_reported() {
            let rt = tokio::runtime::Handle::current();
            let (mut mat_a, mut mat_b, mut events, bridges) = materializers_with_monitoring(&rt);
            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, mut pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();

            // Delivered and consumed: no report.
            push0[2].push(Envelope::data(1, vec![1])).unwrap();
            let timeout = std::time::Duration::from_secs(2);
            assert!(poll_pull(pull2[0].as_mut(), timeout).await.is_some());

            // Received but never consumed before the pull is dropped.
            push0[2].push(Envelope::data(2, vec![2])).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            drop(pull2);
            assert!(next_loss(&mut events).await.contains("never consumed"));

            // Arriving after the pull was dropped.
            push0[2].push(Envelope::data(3, vec![3])).unwrap();
            assert!(
                next_loss(&mut events)
                    .await
                    .contains("after the receiving operator finished")
            );

            // A close marker after the operator finished is not data loss.
            drop(push0);
            bridges.drain();
            tokio::time::timeout(timeout, bridges.wait_idle())
                .await
                .expect("bridges exit on drain");
            assert!(events.try_recv().is_err(), "one report per channel");
        }

        #[tokio::test]
        async fn records_after_the_close_marker_are_reported() {
            let rt = tokio::runtime::Handle::current();
            let (mut mat_a, mut mat_b, mut events, _bridges) = materializers_with_monitoring(&rt);
            let (mut push0, _) = mat_a.materialize_worker(0).unwrap();
            let _ = mat_a.materialize_worker(1).unwrap();
            let (_, _pull2) = mat_b.materialize_worker(2).unwrap();
            let _ = mat_b.materialize_worker(3).unwrap();
            // Simulate a sender that keeps going after closing the channel.
            let sender = mat_a.transport.data_sender("node-b").unwrap();
            push0[2].close();
            let channel_id = NetworkEdgeMaterializer::<u64, u64>::channel_id(0, 0, 2, 4).unwrap();
            let mut payload = Vec::new();
            encode_envelope(
                &u64::codec(),
                &u64::codec(),
                &Envelope::data(9, vec![9]),
                &mut payload,
            )
            .unwrap();
            sender
                .try_send(Frame {
                    dataflow_id: mat_a.dataflow_id,
                    channel_id,
                    payload,
                })
                .unwrap();
            // A protocol violation (corrupt traffic), not a deferred loss.
            let event = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .expect("a violation is reported");
            assert!(
                matches!(
                    &event,
                    Some(LifecycleEvent::Link {
                        peer,
                        event: LinkEvent::DataChannelEnded(
                            crate::communication::peer_link::LinkFailure::ProtocolViolation(detail)
                        ),
                    }) if peer == "node-a" && detail.contains("close marker")
                ),
                "{event:?}"
            );
        }

        #[tokio::test]
        async fn inbound_bridges_wait_until_every_bridge_drained() {
            let rt = tokio::runtime::Handle::current();
            let (_mat_a, mut mat_b, _events, bridges) = materializers_with_monitoring(&rt);
            let (_, pulls) = mat_b.materialize_worker(2).unwrap();
            let timeout = std::time::Duration::from_secs(2);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), bridges.wait_idle())
                    .await
                    .is_err(),
                "bridges still running"
            );
            bridges.drain();
            tokio::time::timeout(timeout, bridges.wait_idle())
                .await
                .expect("drained bridges exit");
            drop(pulls);
        }
    }

    // --- Unclean close, corruption and protocol violations (cluster lifecycle) ---

    #[cfg(feature = "transport")]
    mod link_failures {
        use super::*;
        use crate::communication::peer_link::{
            InboundBridges, LifecycleEvent, LinkEvent, LinkFailure, PeerMonitor,
        };

        type Events = tokio_mpsc::UnboundedReceiver<LifecycleEvent>;

        fn reporter() -> (PeerMonitor, Events) {
            let (tx, rx) = tokio_mpsc::unbounded_channel();
            (PeerMonitor::for_test("peer", tx), rx)
        }

        fn data_frame(time: u64, records: Vec<u64>) -> Vec<u8> {
            let mut buf = Vec::new();
            let env: Envelope<u64, u64, ()> = Envelope::data(time, records);
            encode_envelope(&u64::codec(), &u64::codec(), &env, &mut buf).unwrap();
            buf
        }

        fn close_frame() -> Vec<u8> {
            let mut buf = Vec::new();
            encode_close(&mut buf);
            buf
        }

        /// Run a bridge over `frames` (the channel then closes) and return
        /// the events it reported. `drop_consumer` drops the pull side first.
        async fn run_bridge(frames: Vec<Vec<u8>>, drop_consumer: bool) -> Vec<LinkEvent> {
            let (tx, rx) = tokio_mpsc::channel(16);
            let (std_tx, std_rx) = std::sync::mpsc::channel();
            if drop_consumer {
                drop(std_rx);
            } else {
                std::mem::forget(std_rx);
            }
            let (peer, mut events) = reporter();
            for frame in frames {
                tx.send(frame).await.unwrap();
            }
            drop(tx);
            let bridges = InboundBridges::default();
            let monitor = InboundMonitor {
                peer,
                channel_id: 7,
                guard: bridges.register(),
                bridges,
                pull_dropped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                validate: |bytes: &[u8]| {
                    decode_envelope::<u64, u64, _, _>(&u64::codec(), &u64::codec(), bytes).is_ok()
                },
            };
            data_recv_bridge(rx, std_tx, WakeHandle::new(), Some(monitor)).await;
            let mut seen = Vec::new();
            while let Ok(LifecycleEvent::Link { event, .. }) = events.try_recv() {
                seen.push(event);
            }
            seen
        }

        #[tokio::test]
        async fn bridge_is_silent_after_clean_close() {
            let events = run_bridge(vec![data_frame(1, vec![1, 2]), close_frame()], false).await;
            assert!(events.is_empty(), "{events:?}");
        }

        #[tokio::test]
        async fn bridge_reports_close_without_sentinel() {
            let events = run_bridge(vec![data_frame(1, vec![1])], false).await;
            assert!(
                matches!(
                    events.as_slice(),
                    [LinkEvent::DataChannelEnded(LinkFailure::ChannelClosed)]
                ),
                "{events:?}"
            );
        }

        #[tokio::test]
        async fn bridge_reports_records_after_sentinel_once() {
            let events = run_bridge(
                vec![
                    close_frame(),
                    data_frame(1, vec![1]),
                    data_frame(2, vec![2]),
                ],
                false,
            )
            .await;
            assert!(
                matches!(
                    events.as_slice(),
                    [LinkEvent::DataChannelEnded(LinkFailure::ProtocolViolation(
                        _
                    ))]
                ),
                "{events:?}"
            );
        }

        #[tokio::test]
        async fn bridge_does_not_treat_a_malformed_close_as_the_sentinel() {
            // A close tag followed by bytes is not an end of stream: the
            // channel then closes without its sentinel.
            let events = run_bridge(vec![vec![TAG_CLOSE, 0x01, 0x02]], false).await;
            assert!(
                matches!(
                    events.as_slice(),
                    [LinkEvent::DataChannelEnded(LinkFailure::ChannelClosed)]
                ),
                "{events:?}"
            );
            // The pull side rejects it instead of exhausting.
            let (mut pull, mut events) = pull_with(vec![vec![TAG_CLOSE, 0x01]], true);
            assert!(pull.pull().is_none());
            assert!(!pull.is_exhausted());
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::Link {
                    event: LinkEvent::DataChannelEnded(LinkFailure::Decode(_)),
                    ..
                })
            ));
        }

        #[tokio::test]
        async fn bridge_reports_records_for_a_finished_consumer() {
            let events = run_bridge(
                vec![data_frame(1, vec![]), data_frame(1, vec![5]), close_frame()],
                true,
            )
            .await;
            assert!(
                matches!(events.as_slice(), [LinkEvent::InboundDataLost(_)]),
                "{events:?}"
            );
            // Only the sentinel after the consumer finished is benign.
            let events = run_bridge(vec![close_frame()], true).await;
            assert!(events.is_empty(), "{events:?}");
            // A malformed control-tag frame the consumer never decodes is
            // still reported as corrupt.
            let events = run_bridge(vec![vec![TAG_WATERMARK], close_frame()], true).await;
            assert!(
                matches!(
                    events.as_slice(),
                    [LinkEvent::DataChannelEnded(LinkFailure::Decode(_))]
                ),
                "{events:?}"
            );
        }

        fn pull_with(frames: Vec<Vec<u8>>, close: bool) -> (NetworkPull<u64, u64>, Events) {
            let (tx, rx) = std::sync::mpsc::channel();
            for frame in frames {
                tx.send(frame).unwrap();
            }
            if close {
                drop(tx);
            } else {
                std::mem::forget(tx);
            }
            let (peer, events) = reporter();
            let pull = NetworkPull {
                time_codec: u64::codec(),
                data_codec: u64::codec(),
                receiver: rx,
                exhausted: false,
                failed: false,
                channel_id: 7,
                monitor: Some(PullMonitor {
                    peer,
                    channel_id: 7,
                    dropped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                }),
                _transport: None,
                trace_frames: Default::default(),
            };
            (pull, events)
        }

        #[test]
        fn pull_is_not_exhausted_when_channel_closes_without_sentinel() {
            let (mut pull, _events) = pull_with(vec![data_frame(1, vec![1])], true);
            assert!(pull.pull().is_some());
            assert!(pull.pull().is_none());
            assert!(
                !pull.is_exhausted(),
                "an unclean close is not an end of stream"
            );
            assert!(pull.pull().is_none());
            assert!(!pull.is_exhausted());
        }

        #[test]
        fn pull_is_exhausted_by_the_sentinel() {
            let (mut pull, _events) = pull_with(vec![close_frame()], true);
            assert!(pull.pull().is_none());
            assert!(pull.is_exhausted());
        }

        #[test]
        fn pull_reports_decode_error_and_never_exhausts() {
            let (mut pull, mut events) = pull_with(
                vec![vec![0x7E, 1, 2], data_frame(1, vec![1]), close_frame()],
                true,
            );
            assert!(pull.pull().is_none(), "no error envelope is delivered");
            assert!(pull.pull().is_none(), "nothing after a corrupt frame");
            assert!(!pull.is_exhausted());
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::Link {
                    event: LinkEvent::DataChannelEnded(LinkFailure::Decode(_)),
                    ..
                })
            ));
            assert!(events.try_recv().is_err(), "reported once");
        }

        /// Frames buffered when the consuming operator finishes are decoded:
        /// a corrupt one (even with a control tag) is reported, not dropped.
        #[test]
        fn dropped_pull_reports_corrupt_buffered_frames() {
            let (pull, mut events) = pull_with(vec![vec![TAG_WATERMARK]], false);
            drop(pull);
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleEvent::Link {
                    event: LinkEvent::DataChannelEnded(LinkFailure::Decode(_)),
                    ..
                })
            ));
        }

        #[tokio::test]
        async fn bridge_reports_any_frame_after_sentinel() {
            let mut watermark = Vec::new();
            encode_envelope(
                &u64::codec(),
                &u64::codec(),
                &Envelope::<u64, u64, ()>::watermark(3),
                &mut watermark,
            )
            .unwrap();
            let events = run_bridge(vec![close_frame(), watermark], false).await;
            assert!(
                matches!(
                    events.as_slice(),
                    [LinkEvent::DataChannelEnded(LinkFailure::ProtocolViolation(
                        _
                    ))]
                ),
                "{events:?}"
            );
        }
    }

    #[cfg(feature = "transport")]
    mod frame_limits {
        use super::*;

        #[test]
        fn decode_accepts_more_than_ten_million_records() {
            let n = 10_000_001;
            let env: Envelope<u64, u8, ()> = Envelope::data(3, vec![7u8; n]);
            let mut buf = Vec::new();
            encode_envelope(&u64::codec(), &u8::codec(), &env, &mut buf).unwrap();
            let decoded = decode_envelope::<u64, u8, _, _>(&u64::codec(), &u8::codec(), &buf)
                .unwrap()
                .unwrap();
            let (time, data) = decoded.as_data().unwrap();
            assert_eq!((*time, data.len()), (3, n));
        }

        /// A corrupt count reserves at most `DECODE_PREALLOC_BYTES`, whatever
        /// the record type: a 256 MiB frame claiming `u32::MAX` 24-byte
        /// records used to reserve 256 Mi of them (6 GiB).
        #[test]
        fn decode_preallocation_is_bounded_in_bytes() {
            let frame = 256 << 20;
            for (capacity, size) in [
                (
                    decode_capacity::<String>(u32::MAX as usize, frame),
                    std::mem::size_of::<String>(),
                ),
                (decode_capacity::<[u64; 64]>(u32::MAX as usize, frame), 512),
            ] {
                assert!(capacity > 0);
                assert!(
                    capacity * size <= DECODE_PREALLOC_BYTES,
                    "{capacity} x {size} bytes"
                );
            }
            // Small batches are still reserved exactly.
            assert_eq!(decode_capacity::<u64>(10, 80), 10);
            // A garbage count over a real frame fails cleanly.
            let env: Envelope<u64, String, ()> = Envelope::data(3, vec![String::new(); 1000]);
            let mut buf = Vec::new();
            encode_envelope(&u64::codec(), &String::codec(), &env, &mut buf).unwrap();
            buf[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(
                decode_envelope::<u64, String, _, _>(&u64::codec(), &String::codec(), &buf)
                    .is_err()
            );
        }

        #[test]
        fn decode_rejects_a_count_larger_than_the_frame() {
            let env: Envelope<u64, u64, ()> = Envelope::data(3, vec![1, 2]);
            let mut buf = Vec::new();
            encode_envelope(&u64::codec(), &u64::codec(), &env, &mut buf).unwrap();
            // Claim u32::MAX records: decoding must fail, not reserve memory.
            buf[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(decode_envelope::<u64, u64, _, _>(&u64::codec(), &u64::codec(), &buf).is_err());
        }

        #[test]
        fn push_rejects_frame_over_transport_limit() {
            let (tx, mut rx) = tokio_mpsc::channel(4);
            let mut push = NetworkPush::<u64, u64> {
                time_codec: u64::codec(),
                data_codec: u64::codec(),
                dataflow_id: DataflowId::new(),
                channel_id: 9,
                sender: crate::communication::cluster_transport::FrameSender::Direct(tx),
                outbox: None,
                peer_id: Arc::from("peer"),
                max_payload: 16,
                worker_completed: None,
                link_failure_reported: std::sync::atomic::AtomicBool::new(false),
                close_runtime: None,
                closed: false,
                trace_frames: Default::default(),
            };
            // tag + time + count = 13 bytes: fits.
            push.push(Envelope::data(1, vec![])).unwrap();
            let err = push.push(Envelope::data(1, vec![1, 2, 3])).unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::Communication(crate::error::CommunicationError::FrameTooLarge {
                        channel_id: 9,
                        size: 37,
                        max: 16,
                        ..
                    })
                ),
                "{err:?}"
            );
            let (err, _envelope) = push.try_push(Envelope::data(1, vec![4, 5, 6])).unwrap_err();
            assert!(matches!(
                err,
                Error::Communication(crate::error::CommunicationError::FrameTooLarge { .. })
            ));
            assert_eq!(rx.try_recv().unwrap().payload.len(), 13);
            assert!(
                rx.try_recv().is_err(),
                "oversize frames never reach the transport"
            );
            push.close();
        }

        #[test]
        fn dropped_push_closes_cleanly_only_after_worker_success() {
            for succeeded in [false, true] {
                let (tx, mut rx) = tokio_mpsc::channel(4);
                let flag = Arc::new(std::sync::atomic::AtomicBool::new(succeeded));
                let push = NetworkPush::<u64, u64> {
                    time_codec: u64::codec(),
                    data_codec: u64::codec(),
                    dataflow_id: DataflowId::new(),
                    channel_id: 9,
                    sender: crate::communication::cluster_transport::FrameSender::Direct(tx),
                    outbox: None,
                    peer_id: Arc::from("peer"),
                    max_payload: usize::MAX,
                    worker_completed: Some(flag),
                    link_failure_reported: std::sync::atomic::AtomicBool::new(false),
                    close_runtime: None,
                    closed: false,
                    trace_frames: Default::default(),
                };
                drop(push);
                let sentinel = rx.try_recv().ok().map(|f| f.payload);
                if succeeded {
                    assert_eq!(sentinel, Some(vec![TAG_CLOSE]));
                } else {
                    assert_eq!(
                        sentinel, None,
                        "a failed or never-run worker must not close its channels cleanly"
                    );
                }
            }
        }

        /// Workers hold their targets inside an exchange (or broadcast)
        /// pusher, whose drop closes every target: the worker's outcome must
        /// still decide, for network and same-node targets alike.
        #[test]
        fn dropped_exchange_closes_targets_only_after_worker_success() {
            use crate::dataflow::channels::exchange_channel::{
                BroadcastPush, ExchangePush, SharedWakeRegistry,
            };
            use crate::dataflow::channels::pact::ExchangeFn;

            for (succeeded, broadcast) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let (local, local_pull) = bounded_channel::<u64, u64, ()>(4);
                let local = local.with_close_gate(Some(Arc::clone(&flag)));
                let (tx, mut rx) = tokio_mpsc::channel(4);
                let remote = NetworkPush::<u64, u64> {
                    time_codec: u64::codec(),
                    data_codec: u64::codec(),
                    dataflow_id: DataflowId::new(),
                    channel_id: 9,
                    sender: crate::communication::cluster_transport::FrameSender::Direct(tx),
                    outbox: None,
                    peer_id: Arc::from("peer"),
                    max_payload: usize::MAX,
                    worker_completed: Some(Arc::clone(&flag)),
                    link_failure_reported: std::sync::atomic::AtomicBool::new(false),
                    close_runtime: None,
                    closed: false,
                    trace_frames: Default::default(),
                };
                let targets: Vec<Box<dyn Push<u64, u64, ()>>> =
                    vec![Box::new(local), Box::new(remote)];
                let wakes = Arc::new(SharedWakeRegistry::new(2));
                let push: Box<dyn Push<u64, u64>> = if broadcast {
                    Box::new(BroadcastPush::new(targets, wakes))
                } else {
                    Box::new(ExchangePush::new(
                        targets,
                        ExchangeFn::new("id", |x: &u64| *x),
                        wakes,
                    ))
                };
                // The runtime sets the flag before it drops the executor.
                flag.store(succeeded, std::sync::atomic::Ordering::Release);
                drop(push);
                let sentinel = rx.try_recv().ok().map(|f| f.payload);
                if succeeded {
                    assert!(local_pull.is_exhausted(), "broadcast={broadcast}");
                    assert_eq!(sentinel, Some(vec![TAG_CLOSE]), "broadcast={broadcast}");
                } else {
                    assert!(!local_pull.is_exhausted(), "broadcast={broadcast}");
                    assert_eq!(sentinel, None, "broadcast={broadcast}");
                }
            }
        }

        /// An operator closing its output closes every target, whatever its
        /// worker's eventual outcome: the stream is complete.
        #[test]
        fn explicit_close_is_always_clean() {
            let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (mut local, local_pull) = bounded_channel::<u64, u64, ()>(4);
            local = local.with_close_gate(Some(Arc::clone(&flag)));
            local.close();
            drop(local);
            assert!(local_pull.is_exhausted());
        }

        #[test]
        fn close_tag_with_trailing_bytes_is_not_a_sentinel() {
            let bad = vec![TAG_CLOSE, 1, 2, 3];
            assert!(!is_close_sentinel(&bad));
            assert!(is_close_sentinel(&[TAG_CLOSE]));
            assert!(decode_envelope::<u64, u64, _, _>(&u64::codec(), &u64::codec(), &bad).is_err());
            // Unknown tags and malformed frames may have carried records.
            assert!(frame_carries_records(&bad));
            assert!(frame_carries_records(&[0x7E, 1]));
            assert!(!frame_carries_records(&[TAG_CLOSE]));
            assert!(!frame_carries_records(&[TAG_WATERMARK, 0]));
        }
    }
}
