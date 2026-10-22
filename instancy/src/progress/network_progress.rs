//! Network progress exchange: serialization, bridge tasks, and factory functions.
//!
//! This module provides the infrastructure for exchanging progress updates
//! across network nodes via [`TransportSession`](crate::communication::transport_session::TransportSession).
//!
//! # Architecture
//!
//! ```text
//! Send path:
//!   ProgressSender::send()
//!     → serialize Vec<ProgressChange<T>> into bytes
//!     → PeerOutbox (unbounded, ordered per peer; never fails)
//!     → [outbox forwarder task] awaits on the bounded transport payload channel
//!     → [TransportSession bridge] writes to TCP with priority ordering
//!
//! Receive path:
//!   TCP → [Demuxer] → tokio_mpsc::Receiver<Vec<u8>>
//!     → [recv bridge task] deserializes into Vec<ProgressChange<T>>
//!     → Arc<Mutex<SharedBuffer<T>>> + wake_handle.notify()
//!     → ProgressReceiver::drain_all() (same as local)
//! ```
//!
//! # Reliability
//!
//! Progress deltas are **non-idempotent** — losing a delta corrupts global
//! frontier state. The send path queues frames on the per-peer
//! [`PeerOutbox`](crate::communication::peer_link::PeerOutbox) lane, so
//! `ProgressSender::send()` never drops frames and the forwarder applies
//! backpressure to the bounded transport layer via async await. The lane is
//! drained even after the dataflow completes; it is never cut short by
//! cancellation.
//!
//! # End of stream
//!
//! When all local workers have finished successfully, the cluster lifecycle
//! coordinator sends one end-of-stream frame
//! ([`encode_progress_end_of_stream`]) on every progress channel to every
//! peer, behind everything else on the lane. A receive bridge that sees it
//! reports [`LinkEvent::ProgressEndOfStream`] and exits. The acknowledgement
//! of a peer's complete end-of-stream ([`encode_progress_ack`]) also travels
//! on a progress channel, so it is ordered behind the sender's data; the
//! receive bridge reports it as [`LinkEvent::AckReceived`]. A channel that
//! ends (EOF), or fails to decode, before its end-of-stream marker is reported
//! as [`LinkEvent::ProgressChannelEnded`]; the coordinator decides whether
//! that is benign (the link was already retired) or a failure.

#[cfg(feature = "transport")]
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "transport")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "transport")]
use tokio::sync::mpsc as tokio_mpsc;

#[cfg(feature = "transport")]
use crate::communication::codec::{Codec, CodecError, ExchangeData};
#[cfg(feature = "transport")]
use crate::communication::peer_link::{
    LifecycleEventSender, LinkEvent, LinkFailure, PeerOutbox, report_link_event,
};
#[cfg(feature = "transport")]
use crate::communication::transport::Frame;
#[cfg(feature = "transport")]
use crate::dataflow::channels::wake::WakeHandle;
#[cfg(feature = "transport")]
use crate::dataflow::id::DataflowId;
#[cfg(feature = "transport")]
use crate::error::LockResultExt;
#[cfg(feature = "transport")]
use crate::progress::progress_channel::{
    ProgressChange, ProgressKind, ProgressReceiver, ProgressSender, SharedBuffer,
    WorkerProgressChannels,
};
#[cfg(feature = "transport")]
use crate::progress::timestamp::Timestamp;
#[cfg(feature = "transport")]
use crate::wire;

// ---------------------------------------------------------------------------
// Serialization: Vec<ProgressChange<T>> ↔ bytes
// ---------------------------------------------------------------------------

/// Default maximum number of progress changes per batch.
///
/// Both the encoder and decoder enforce this limit:
/// - The encoder splits large batches into multiple frames.
/// - The decoder rejects batches exceeding this limit to prevent OOM.
///
/// Override via `max_batch_size` parameter in [`encode_progress_batch`],
/// [`decode_progress_batch`], and [`create_network_progress_channels`].
#[cfg(feature = "transport")]
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1_000_000;

/// Encode a batch of progress changes into bytes.
///
/// Wire format (per frame):
/// ```text
/// [count: u32]
/// for each change:
///   [kind: u8]   ← 0 = Source (capability), 1 = Target (in-flight message)
///   [operator_index: u64]
///   [port: u64]
///   [timestamp: T encoded by T::codec()]
///   [diff: i64]
/// [crc32: u32]   ← CRC32 of all preceding bytes
/// ```
///
/// If `changes.len()` exceeds `max_batch_size`, the batch is split into
/// multiple frames appended sequentially to `bufs`. Each frame is a
/// self-contained encoded batch with its own CRC32. A split batch sends its
/// positive changes first (otherwise in order): a receiver applies each frame
/// as it arrives, and a capability released in an earlier frame than the one
/// that replaces it (or an in-flight count) could let its frontier advance
/// too early. Positives first only ever hold the frontier back.
#[cfg(feature = "transport")]
pub fn encode_progress_batch<T: Timestamp + ExchangeData>(
    changes: &[ProgressChange<T>],
    bufs: &mut Vec<Vec<u8>>,
    max_batch_size: usize,
) -> Result<(), CodecError> {
    // Cap below the reserved counts: the wire format uses a u32 count field,
    // and `u32::MAX` / `u32::MAX - 1` mark end-of-stream / acknowledgement.
    let effective_max = max_batch_size.min(PROGRESS_ACK_COUNT as usize - 1).max(1);

    // An empty batch is still one frame (count=0 + CRC).
    if changes.len() <= effective_max {
        bufs.push(encode_progress_frame(changes)?);
        return Ok(());
    }
    let mut ordered = changes
        .iter()
        .filter(|change| change.diff > 0)
        .chain(changes.iter().filter(|change| change.diff <= 0))
        .peekable();
    // V6: no fragment may carry a positive change after an earlier fragment
    // carried a negative one.
    #[cfg(any(debug_assertions, feature = "progress-check"))]
    let mut negative_sent = false;
    while ordered.peek().is_some() {
        let chunk: Vec<&ProgressChange<T>> = ordered.by_ref().take(effective_max).collect();
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        {
            if negative_sent && chunk.iter().any(|change| change.diff > 0) {
                crate::progress::check::record(
                    crate::progress::check::Check::V6,
                    "fragment-order",
                    None,
                    None,
                    None,
                    format!(
                        "a progress batch of {} changes was split with a positive change \
                         after a fragment that carried a negative one",
                        changes.len()
                    ),
                );
            }
            negative_sent |= chunk.iter().any(|change| change.diff < 0);
        }
        bufs.push(encode_progress_frame(chunk)?);
    }
    Ok(())
}

/// Encode one frame (`count` + changes + CRC32); `changes` must fit the
/// count field (see [`encode_progress_batch`]).
#[cfg(feature = "transport")]
fn encode_progress_frame<'a, T: Timestamp + ExchangeData>(
    changes: impl IntoIterator<Item = &'a ProgressChange<T>, IntoIter: ExactSizeIterator>,
) -> Result<Vec<u8>, CodecError> {
    let changes = changes.into_iter();
    let mut buf = Vec::new();
    // Callers cap the length below the reserved counts (u32::MAX - 1).
    let count = changes.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());

    let codec = T::codec();
    for change in changes {
        let kind_byte: u8 = match change.kind {
            ProgressKind::Source => 0,
            ProgressKind::Target => 1,
        };
        buf.push(kind_byte);
        buf.extend_from_slice(&(change.node as u64).to_le_bytes());
        buf.extend_from_slice(&(change.port as u64).to_le_bytes());
        codec.encode(&change.time, &mut buf)?;
        buf.extend_from_slice(&change.diff.to_le_bytes());
    }

    // Append CRC32 checksum of the payload.
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Decode a batch of progress changes from bytes.
///
/// Verifies the trailing CRC32 checksum before parsing. Returns the
/// decoded batch or a codec error on malformed/corrupted input.
/// Rejects batches with `count > max_batch_size` to prevent OOM.
#[cfg(feature = "transport")]
pub fn decode_progress_batch<T: Timestamp + ExchangeData>(
    data: &[u8],
    max_batch_size: usize,
) -> Result<Vec<ProgressChange<T>>, CodecError> {
    // Minimum: 4 bytes count + 4 bytes CRC32.
    if data.len() < 8 {
        return Err(CodecError::InsufficientData {
            needed: 8,
            available: data.len(),
        });
    }

    // Verify CRC32: last 4 bytes are the checksum of everything before them.
    let (payload, crc_bytes) = data.split_at(data.len() - 4);
    let expected_crc =
        wire::read_u32(crc_bytes, 0).map_err(|e| CodecError::InvalidData(e.to_string()))?;
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(CodecError::CrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    // Now decode the verified payload.
    let count =
        wire::read_u32(payload, 0).map_err(|e| CodecError::InvalidData(e.to_string()))? as usize;

    if count > max_batch_size {
        return Err(CodecError::InvalidData(format!(
            "progress batch count {count} exceeds maximum {max_batch_size}"
        )));
    }

    let mut changes = Vec::with_capacity(count);
    let codec = T::codec();
    let mut offset = 4;

    for _ in 0..count {
        // kind (u8): 0 = Source, 1 = Target
        if offset + 1 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 1,
                available: payload.len(),
            });
        }
        let kind = match payload[offset] {
            0 => ProgressKind::Source,
            1 => ProgressKind::Target,
            other => {
                return Err(CodecError::InvalidData(format!(
                    "invalid progress change kind byte: {other}"
                )));
            }
        };
        offset += 1;

        // operator_index (u64)
        if offset + 8 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: payload.len(),
            });
        }
        let op_idx = wire::read_u64(payload, offset)
            .map_err(|e| CodecError::InvalidData(e.to_string()))? as usize;
        offset += 8;

        // output_port (u64)
        if offset + 8 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: payload.len(),
            });
        }
        let output_port = wire::read_u64(payload, offset)
            .map_err(|e| CodecError::InvalidData(e.to_string()))?
            as usize;
        offset += 8;

        // timestamp (variable size via codec)
        let (time, consumed) = codec.decode(&payload[offset..])?;
        offset += consumed;

        // diff (i64)
        if offset + 8 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: payload.len(),
            });
        }
        let diff =
            wire::read_i64(payload, offset).map_err(|e| CodecError::InvalidData(e.to_string()))?;
        offset += 8;

        changes.push(ProgressChange {
            kind,
            node: op_idx,
            port: output_port,
            time,
            diff,
        });
    }

    // Reject trailing bytes (before the CRC32 that we already stripped).
    if offset != payload.len() {
        return Err(CodecError::InvalidData(format!(
            "trailing bytes: consumed {offset} of {} payload bytes",
            payload.len()
        )));
    }

    Ok(changes)
}

/// Count value reserved for the progress end-of-stream marker.
#[cfg(feature = "transport")]
pub(crate) const PROGRESS_END_OF_STREAM_COUNT: u32 = u32::MAX;

/// Count value reserved for the end-of-stream acknowledgement.
#[cfg(feature = "transport")]
pub(crate) const PROGRESS_ACK_COUNT: u32 = u32::MAX - 1;

/// Encode a marker frame: `[count][crc32]` with a reserved count.
#[cfg(feature = "transport")]
fn encode_progress_marker(count: u32) -> Vec<u8> {
    let mut buf = count.to_le_bytes().to_vec();
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Encode the end-of-stream marker for a progress channel.
///
/// Wire format: `[count = u32::MAX][crc32]`. Real batches never use this
/// count (the encoder caps batches below the reserved counts), so the marker
/// is unambiguous. It is the last frame a node sends on a progress channel
/// during a clean shutdown.
#[cfg(feature = "transport")]
pub(crate) fn encode_progress_end_of_stream() -> Vec<u8> {
    encode_progress_marker(PROGRESS_END_OF_STREAM_COUNT)
}

/// Encode the acknowledgement of a peer's complete end-of-stream.
///
/// Wire format: `[count = u32::MAX - 1][crc32]`. It travels on the sender's
/// payload lane, so the receiver sees it only after every data frame the
/// sender queued before it.
#[cfg(feature = "transport")]
pub(crate) fn encode_progress_ack() -> Vec<u8> {
    encode_progress_marker(PROGRESS_ACK_COUNT)
}

/// A decoded progress-channel frame.
#[cfg(feature = "transport")]
#[derive(Debug, PartialEq)]
pub(crate) enum ProgressFrame<T> {
    /// A batch of progress changes.
    Batch(Vec<ProgressChange<T>>),
    /// The sender will send nothing more on this channel.
    EndOfStream,
    /// The sender has received our complete end-of-stream.
    Ack,
}

/// Decode a progress-channel frame: a batch, the end-of-stream marker or the
/// acknowledgement.
#[cfg(feature = "transport")]
pub(crate) fn decode_progress_frame<T: Timestamp + ExchangeData>(
    data: &[u8],
    max_batch_size: usize,
) -> Result<ProgressFrame<T>, CodecError> {
    let marker = |count: u32| data.len() == 8 && data[..4] == count.to_le_bytes();
    let is_end_of_stream = marker(PROGRESS_END_OF_STREAM_COUNT);
    if is_end_of_stream || marker(PROGRESS_ACK_COUNT) {
        let expected_crc =
            wire::read_u32(data, 4).map_err(|e| CodecError::InvalidData(e.to_string()))?;
        let actual_crc = crc32fast::hash(&data[..4]);
        if actual_crc != expected_crc {
            return Err(CodecError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }
        return Ok(if is_end_of_stream {
            ProgressFrame::EndOfStream
        } else {
            ProgressFrame::Ack
        });
    }
    decode_progress_batch(data, max_batch_size).map(ProgressFrame::Batch)
}

// ---------------------------------------------------------------------------
// Bridge tasks
// ---------------------------------------------------------------------------

/// Bridge task: reads serialized progress frames from a Demuxer channel,
/// deserializes them, and pushes into the local SharedBuffer for the
/// target worker's ProgressReceiver.
///
/// Wakes the target worker on each received batch so the executor
/// processes the incoming progress updates promptly.
///
/// Reports [`LinkEvent::ProgressEndOfStream`] on the end-of-stream marker and
/// then keeps reading until the channel closes: the peer may still
/// acknowledge our end-of-stream on it, but any other frame after the marker
/// breaks the protocol ([`LinkEvent::ProgressChannelEnded`] with
/// `ProtocolViolation` or `Decode`, reported once). If the channel closes, a frame fails
/// to decode, or the buffer lock is poisoned before that marker, the bridge
/// reports [`LinkEvent::ProgressChannelEnded`] and exits; the lifecycle
/// coordinator decides whether that is a failure. `cancel` stops the bridge
/// during teardown.
#[cfg(feature = "transport")]
#[allow(clippy::too_many_arguments)]
async fn progress_recv_bridge<T: Timestamp + ExchangeData>(
    mut rx: tokio_mpsc::Receiver<Vec<u8>>,
    buffer: Arc<Mutex<SharedBuffer<T>>>,
    wake_handle: WakeHandle,
    cancel: tokio_util::sync::CancellationToken,
    peer_id: String,
    channel_id: u64,
    max_batch_size: usize,
    events: LifecycleEventSender,
) {
    let failure = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            data = rx.recv() => match data {
                Some(bytes) => match decode_progress_frame::<T>(&bytes, max_batch_size) {
                    Ok(ProgressFrame::Batch(changes)) => {
                        if !changes.is_empty() {
                            match buffer.lock().or_poison("progress buffer") {
                                Ok(mut buf) => buf.queue.push_back(changes),
                                Err(_) => break LinkFailure::LockPoisoned,
                            }
                            wake_handle.notify();
                        }
                    }
                    Ok(ProgressFrame::EndOfStream) => {
                        report_link_event(&events, &peer_id, LinkEvent::ProgressEndOfStream);
                        watch_after_end_of_stream::<T>(
                            &mut rx,
                            &cancel,
                            &peer_id,
                            channel_id,
                            max_batch_size,
                            &events,
                        )
                        .await;
                        return;
                    }
                    Ok(ProgressFrame::Ack) => {
                        report_link_event(&events, &peer_id, LinkEvent::AckReceived);
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            %peer_id,
                            channel_id,
                            "progress recv bridge: decode error: {e}"
                        );
                        break LinkFailure::Decode(e.to_string());
                    }
                },
                None => break LinkFailure::ChannelClosed,
            }
        }
    };
    report_link_event(&events, &peer_id, LinkEvent::ProgressChannelEnded(failure));
}

/// After a progress channel's end-of-stream, wait for the channel to close
/// (or teardown). The peer's acknowledgement of our end-of-stream may still
/// arrive on it; the first other frame is reported as a protocol violation.
#[cfg(feature = "transport")]
async fn watch_after_end_of_stream<T: Timestamp + ExchangeData>(
    rx: &mut tokio_mpsc::Receiver<Vec<u8>>,
    cancel: &tokio_util::sync::CancellationToken,
    peer_id: &str,
    channel_id: u64,
    max_batch_size: usize,
    events: &LifecycleEventSender,
) {
    let mut reported = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            data = rx.recv() => match data {
                Some(bytes) => {
                    let failure = match decode_progress_frame::<T>(&bytes, max_batch_size) {
                        Ok(ProgressFrame::Ack) => {
                            report_link_event(events, peer_id, LinkEvent::AckReceived);
                            continue;
                        }
                        // Both kinds: the channel was already counted at its
                        // end-of-stream.
                        Ok(frame) => {
                            #[cfg(any(debug_assertions, feature = "progress-check"))]
                            if matches!(frame, ProgressFrame::Batch(_)) {
                                crate::progress::check::record(
                                    crate::progress::check::Check::V6,
                                    "batch-after-end-of-stream",
                                    None,
                                    None, None,
                                    format!(
                                        "peer {peer_id} sent a progress batch on channel \
                                         {channel_id} after its end-of-stream"
                                    ),
                                );
                            }
                            #[cfg(not(any(debug_assertions, feature = "progress-check")))]
                            let _ = frame;
                            LinkFailure::ProtocolViolation(format!(
                                "frame on progress channel {channel_id} after its end-of-stream"
                            ))
                        }
                        Err(e) => LinkFailure::ProtocolViolation(format!(
                            "undecodable frame on progress channel {channel_id} after its \
                             end-of-stream: {e}"
                        )),
                    };
                    if !reported {
                        reported = true;
                        report_link_event(
                            events,
                            peer_id,
                            LinkEvent::ProgressChannelEnded(failure),
                        );
                    }
                }
                None => return,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Progress channel ID helpers
// ---------------------------------------------------------------------------

/// Compute the progress channel ID for a (source, target) worker pair.
///
/// Delegates to the public
/// [`transport_session::progress_channel_id`](crate::communication::transport_session::progress_channel_id):
/// `PROGRESS_CHANNEL_BASE + source * num_workers + target`, overflow-checked
/// and confined to the progress namespace.
#[cfg(feature = "transport")]
pub fn progress_channel_id(
    source: usize,
    target: usize,
    num_workers: usize,
) -> Result<u64, crate::Error> {
    crate::communication::transport_session::progress_channel_id(source, target, num_workers)
}

// ---------------------------------------------------------------------------
// Factory: create network progress channels
// ---------------------------------------------------------------------------

/// Holds background task handles for network progress receive bridges.
///
/// On drop, cancels the shared `CancellationToken` (signalling all bridges
/// to stop) and then aborts any remaining tasks. Send-side forwarding lives
/// in the per-peer [`PeerOutbox`] forwarders, which are owned separately so
/// that they can drain after the receive side is torn down.
///
/// # Important
///
/// Callers must keep this struct alive for the lifetime of the dataflow.
/// Dropping it prematurely will cancel progress exchange.
#[cfg(feature = "transport")]
#[must_use = "dropping NetworkProgressHandles cancels progress exchange"]
pub struct NetworkProgressHandles {
    cancel: tokio_util::sync::CancellationToken,
    recv_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "transport")]
impl Drop for NetworkProgressHandles {
    fn drop(&mut self) {
        // Signal all bridges to shut down gracefully first.
        self.cancel.cancel();
        // Then abort any that haven't exited yet.
        for h in &self.recv_handles {
            h.abort();
        }
    }
}

/// Create network progress senders and receiver bridges for cross-node
/// progress exchange.
///
/// This function creates:
/// - **Network senders** for each `(local_worker, remote_worker)` pair. They
///   queue frames on the peer's [`PeerOutbox`] (shared by all local workers
///   sending to that peer), so data, close sentinels and progress to a peer
///   stay in one ordered lane.
/// - **Receiver bridges** for each `(remote_worker, local_worker)` pair,
///   reading from TransportSession's Demuxer channels, deserializing, and
///   pushing into SharedBuffers with wake notification.
///
/// Returns updated `WorkerProgressChannels` (with network senders/receivers
/// merged into the existing local channels) and handle holder for bridge tasks.
///
/// # Arguments
///
/// - `local_channels`: Existing local progress channels (from `create_progress_channels`).
/// - `outboxes`: Per-peer ordered outbound lanes (keyed by peer node ID).
/// - `receivers`: Per-peer channel receivers from `TransportSession::new()`.
/// - `dataflow_id`: Dataflow ID for frame construction.
/// - `local_worker_range`: `(start, end)` of local worker indices.
/// - `remote_peers`: List of `(peer_node_id, worker_start, worker_end)`.
/// - `num_workers`: Total worker count across all nodes.
/// - `wake_handles`: Wake handles for ALL workers (indexed by global worker ID).
///   Only local workers' handles are used for receiver bridges.
/// - `cancel`: Stops receive bridges at teardown.
/// - `events`: Lifecycle events (end-of-stream, channel failures).
/// - `max_batch_size`: Maximum number of progress changes per serialized
///   batch. The encoder splits larger batches into multiple frames; the
///   decoder rejects batches exceeding this limit. Use
///   [`DEFAULT_MAX_BATCH_SIZE`] for the default (1,000,000).
/// - `runtime_handle`: Tokio runtime for spawning bridge tasks.
///
/// # Errors
///
/// Returns an error if expected receiver channels are missing from the
/// TransportSession (indicating misconfigured channel registrations).
#[cfg(feature = "transport")]
#[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
pub(crate) fn create_network_progress_channels<T: Timestamp + ExchangeData>(
    mut local_channels: Vec<WorkerProgressChannels<T>>,
    outboxes: &HashMap<String, PeerOutbox>,
    mut receivers: HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>,
    dataflow_id: DataflowId,
    local_worker_range: (usize, usize),
    remote_peers: &[(String, usize, usize)],
    num_workers: usize,
    wake_handles: &[WakeHandle],
    cancel: tokio_util::sync::CancellationToken,
    events: LifecycleEventSender,
    max_batch_size: usize,
    runtime_handle: &tokio::runtime::Handle,
) -> Result<
    (Vec<WorkerProgressChannels<T>>, NetworkProgressHandles),
    crate::error::CommunicationError,
> {
    let (local_start, local_end) = local_worker_range;

    // --- Input validation ---
    if local_start > local_end {
        return Err(crate::error::CommunicationError::InvalidSetup(format!(
            "invalid local_worker_range: start ({local_start}) > end ({local_end})"
        )));
    }
    if local_end > num_workers {
        return Err(crate::error::CommunicationError::InvalidSetup(format!(
            "local_worker_range end ({local_end}) exceeds num_workers ({num_workers})"
        )));
    }
    let expected_local = local_end - local_start;
    if local_channels.len() != expected_local {
        return Err(crate::error::CommunicationError::InvalidSetup(format!(
            "local_channels.len() ({}) != local_worker_range size ({expected_local})",
            local_channels.len()
        )));
    }
    if wake_handles.len() < num_workers {
        return Err(crate::error::CommunicationError::InvalidSetup(format!(
            "wake_handles.len() ({}) < num_workers ({num_workers})",
            wake_handles.len()
        )));
    }
    for (peer_id, peer_start, peer_end) in remote_peers {
        if *peer_start > *peer_end || *peer_end > num_workers {
            return Err(crate::error::CommunicationError::InvalidSetup(format!(
                "invalid peer range for {peer_id}: ({peer_start}, {peer_end}), num_workers={num_workers}"
            )));
        }
    }

    let mut recv_handles = Vec::new();

    // --- Send side: the peer's ordered outbox, shared by all local workers ---
    for (peer_id, peer_start, peer_end) in remote_peers {
        let outbox = outboxes.get(peer_id.as_str()).ok_or_else(|| {
            crate::error::CommunicationError::InvalidSetup(format!(
                "missing outbox for peer {peer_id}"
            ))
        })?;

        // Create network senders for each (local_worker → remote_worker) pair.
        for local_w in local_start..local_end {
            let local_idx = local_w - local_start;
            for remote_w in *peer_start..*peer_end {
                let ch_id = progress_channel_id(local_w, remote_w, num_workers)
                    .map_err(|e| crate::error::CommunicationError::InvalidSetup(e.to_string()))?;
                let df_id = dataflow_id;
                let outbox = outbox.clone();

                let sender = ProgressSender::network(move |changes: Vec<ProgressChange<T>>| {
                    // A retired peer has finished or already holds everything
                    // it needs from us; deltas to it are irrelevant.
                    if outbox.is_retired() {
                        return;
                    }
                    let mut bufs = Vec::new();
                    if let Err(e) = encode_progress_batch::<T>(&changes, &mut bufs, max_batch_size)
                    {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            peer = %outbox.peer(),
                            channel_id = ch_id,
                            "progress send: encode error {e}"
                        );
                        outbox.report(LinkEvent::OutboundFailed(LinkFailure::Encode(
                            e.to_string(),
                        )));
                        return;
                    }
                    for buf in bufs {
                        let frame = Frame {
                            dataflow_id: df_id,
                            channel_id: ch_id,
                            payload: buf,
                        };
                        // The lane only closes after its forwarder reported a
                        // transport failure, or after teardown.
                        if outbox.send(frame).is_err() {
                            return;
                        }
                    }
                });

                local_channels[local_idx].senders[remote_w] = Some(sender);
            }
        }
    }

    // --- Receive side: one bridge task per (remote_src, local_dst) pair ---
    for (peer_id, peer_start, peer_end) in remote_peers {
        let peer_map = receivers.get_mut(peer_id.as_str()).ok_or_else(|| {
            crate::error::CommunicationError::InvalidSetup(format!(
                "missing receiver map for peer {peer_id}"
            ))
        })?;

        for remote_w in *peer_start..*peer_end {
            for local_w in local_start..local_end {
                let local_idx = local_w - local_start;
                let ch_id = progress_channel_id(remote_w, local_w, num_workers)
                    .map_err(|e| crate::error::CommunicationError::InvalidSetup(e.to_string()))?;

                let rx = peer_map.remove(&ch_id).ok_or_else(|| {
                    crate::error::CommunicationError::InvalidSetup(format!(
                        "missing receiver for peer {peer_id}, channel {ch_id} \
                         (remote_worker={remote_w} → local_worker={local_w})"
                    ))
                })?;

                // Create shared buffer + receiver for this (remote→local) pair.
                let shared = Arc::new(Mutex::new(SharedBuffer {
                    queue: VecDeque::new(),
                }));
                let receiver = ProgressReceiver::new(Arc::clone(&shared));

                // Spawn receive bridge task.
                let wake = wake_handles[local_w].clone();
                let cancel_clone = cancel.clone();
                let peer_id_clone = peer_id.clone();
                let handle = runtime_handle.spawn(progress_recv_bridge::<T>(
                    rx,
                    shared,
                    wake,
                    cancel_clone,
                    peer_id_clone,
                    ch_id,
                    max_batch_size,
                    events.clone(),
                ));
                recv_handles.push(handle);

                local_channels[local_idx].receivers[remote_w] = Some(receiver);
            }
        }
    }

    let handles = NetworkProgressHandles {
        cancel: cancel.clone(),
        recv_handles,
    };

    Ok((local_channels, handles))
}

// ---------------------------------------------------------------------------
// ProgressSender::network constructor (cfg-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport")]
impl<T: Timestamp> ProgressSender<T> {
    /// Create a network progress sender with a type-erased send function.
    ///
    /// The closure captures the codec, channel ID, and unbounded sender
    /// at construction time. This allows `ProgressSender<T>` to work
    /// without requiring `T: ExchangeData` on the struct itself.
    pub(crate) fn network<F>(send_fn: F) -> Self
    where
        F: Fn(Vec<ProgressChange<T>>) + Send + Sync + 'static,
    {
        Self {
            inner: super::progress_channel::SenderInner::Network {
                send_fn: Box::new(send_fn),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "transport")]
mod tests {
    use super::*;
    use crate::communication::transport_session::PROGRESS_CHANNEL_BASE;

    #[test]
    fn encode_decode_roundtrip_u64() {
        let changes: Vec<ProgressChange<u64>> = vec![
            ProgressChange::source(0, 0, 42u64, 1),
            ProgressChange::target(1, 2, 100u64, -1),
            ProgressChange::source(3, 0, 0u64, 5),
        ];

        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(bufs.len(), 1);

        let decoded = decode_progress_batch::<u64>(&bufs[0], DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(decoded, changes);
    }

    #[test]
    fn encode_decode_empty_batch() {
        let changes: Vec<ProgressChange<u64>> = vec![];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(bufs.len(), 1);

        let decoded = decode_progress_batch::<u64>(&bufs[0], DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_rejects_truncated_data() {
        let changes: Vec<ProgressChange<u64>> = vec![ProgressChange::source(0, 0, 42u64, 1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();

        // Truncate the buffer (removes part of CRC or payload).
        let buf = &bufs[0];
        let result = decode_progress_batch::<u64>(&buf[..buf.len() - 2], DEFAULT_MAX_BATCH_SIZE);
        assert!(result.is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let changes: Vec<ProgressChange<u64>> = vec![ProgressChange::source(0, 0, 42u64, 1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        let mut buf = bufs.into_iter().next().unwrap();
        buf.push(0xFF); // trailing byte after CRC

        let result = decode_progress_batch::<u64>(&buf, DEFAULT_MAX_BATCH_SIZE);
        assert!(result.is_err());
    }

    #[test]
    fn decode_rejects_too_short() {
        let result = decode_progress_batch::<u64>(&[1, 2], DEFAULT_MAX_BATCH_SIZE);
        assert!(result.is_err());
    }

    #[test]
    fn decode_detects_crc32_corruption() {
        let changes: Vec<ProgressChange<u64>> = vec![
            ProgressChange::source(0, 0, 42u64, 1),
            ProgressChange::source(1, 0, 100u64, -1),
        ];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        let mut buf = bufs.into_iter().next().unwrap();

        // Flip a bit in the payload (not the CRC).
        buf[8] ^= 0x01;

        let result = decode_progress_batch::<u64>(&buf, DEFAULT_MAX_BATCH_SIZE);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CRC32 mismatch"),
            "expected 'CRC32 mismatch' in error, got: {err}"
        );
    }

    #[test]
    fn encode_splits_large_batches() {
        let changes: Vec<ProgressChange<u64>> = (0..5)
            .map(|i| ProgressChange::source(i, 0, i as u64, 1))
            .collect();

        // max_batch_size=2 → should produce 3 frames (2+2+1).
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, 2).unwrap();
        assert_eq!(bufs.len(), 3);

        // Decode each and reassemble.
        let mut all = Vec::new();
        for buf in &bufs {
            let decoded = decode_progress_batch::<u64>(buf, 2).unwrap();
            all.extend(decoded);
        }
        assert_eq!(all, changes);
    }

    /// A split batch sends its positive changes first, otherwise in order, so
    /// a receiver applying the frames one by one never releases a capability
    /// (or an in-flight count) before what replaces it. An unsplit batch is
    /// sent as is.
    #[test]
    fn split_batches_send_positive_changes_first() {
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        let mark = crate::progress::check::mark();
        let changes: Vec<ProgressChange<u64>> = vec![
            ProgressChange::source(0, 0, 0, -1),
            ProgressChange::source(0, 0, 1, 1),
            ProgressChange::target(1, 0, 0, -1),
            ProgressChange::target(1, 0, 1, 2),
            ProgressChange::source(2, 0, 3, 1),
        ];
        let decode = |bufs: &[Vec<u8>], max| -> Vec<Vec<ProgressChange<u64>>> {
            bufs.iter()
                .map(|buf| decode_progress_batch::<u64>(buf, max).unwrap())
                .collect()
        };
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, 2).unwrap();
        let frames = decode(&bufs, 2);
        assert_eq!(
            frames,
            vec![
                vec![changes[1].clone(), changes[3].clone()],
                vec![changes[4].clone(), changes[0].clone()],
                vec![changes[2].clone()],
            ]
        );
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, 5).unwrap();
        assert_eq!(decode(&bufs, 5), vec![changes.clone()]);
        // The checker's V6 agrees: no fragment carried a positive change
        // after a negative one.
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        assert!(
            !crate::progress::check::violations_since(mark)
                .iter()
                .any(|v| v.kind == "fragment-order"),
            "V6 flagged a positives-first split"
        );
    }

    #[test]
    fn progress_channel_id_formula() {
        // 4 workers total, src=1, dst=3 → PROGRESS_CHANNEL_BASE + 1*4 + 3
        assert_eq!(
            progress_channel_id(1, 3, 4).unwrap(),
            PROGRESS_CHANNEL_BASE + 7
        );
        assert_eq!(progress_channel_id(0, 0, 4).unwrap(), PROGRESS_CHANNEL_BASE);
    }

    #[test]
    fn end_of_stream_roundtrip_and_is_distinct_from_batches() {
        let eos = encode_progress_end_of_stream();
        assert_eq!(
            decode_progress_frame::<u64>(&eos, DEFAULT_MAX_BATCH_SIZE).unwrap(),
            ProgressFrame::EndOfStream
        );
        let ack = encode_progress_ack();
        assert_eq!(
            decode_progress_frame::<u64>(&ack, DEFAULT_MAX_BATCH_SIZE).unwrap(),
            ProgressFrame::Ack
        );
        // Legacy batch decoding never accepts the markers as batches.
        assert!(decode_progress_batch::<u64>(&eos, DEFAULT_MAX_BATCH_SIZE).is_err());
        assert!(decode_progress_batch::<u64>(&ack, DEFAULT_MAX_BATCH_SIZE).is_err());

        for marker in [&eos, &ack] {
            let mut corrupted = marker.clone();
            corrupted[5] ^= 0x01;
            assert!(decode_progress_frame::<u64>(&corrupted, DEFAULT_MAX_BATCH_SIZE).is_err());
        }

        let mut bufs = Vec::new();
        encode_progress_batch(
            &[ProgressChange::source(0, 0, 1u64, 1)],
            &mut bufs,
            DEFAULT_MAX_BATCH_SIZE,
        )
        .unwrap();
        assert_eq!(
            decode_progress_frame::<u64>(&bufs[0], DEFAULT_MAX_BATCH_SIZE).unwrap(),
            ProgressFrame::Batch(vec![ProgressChange::source(0, 0, 1u64, 1)])
        );
    }

    #[test]
    fn encoder_never_emits_a_reserved_count() {
        // Even with an unbounded batch limit, the count field stays below the
        // reserved marker values.
        let changes = vec![ProgressChange::source(0, 0, 1u64, 1); 3];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, usize::MAX).unwrap();
        for buf in bufs {
            assert_ne!(buf[..4], PROGRESS_END_OF_STREAM_COUNT.to_le_bytes());
            assert_ne!(buf[..4], PROGRESS_ACK_COUNT.to_le_bytes());
        }
    }

    fn spawn_recv_bridge(
        rx: tokio_mpsc::Receiver<Vec<u8>>,
        shared: &Arc<Mutex<SharedBuffer<u64>>>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio_mpsc::UnboundedReceiver<crate::communication::peer_link::LifecycleEvent>,
    ) {
        let (events, events_rx) = tokio_mpsc::unbounded_channel();
        let handle = tokio::spawn(progress_recv_bridge::<u64>(
            rx,
            Arc::clone(shared),
            WakeHandle::new(),
            cancel.clone(),
            "test-peer".into(),
            42,
            DEFAULT_MAX_BATCH_SIZE,
            events,
        ));
        (handle, events_rx)
    }

    fn new_shared() -> Arc<Mutex<SharedBuffer<u64>>> {
        Arc::new(Mutex::new(SharedBuffer::<u64> {
            queue: VecDeque::new(),
        }))
    }

    #[tokio::test]
    async fn recv_bridge_deserializes_and_wakes() {
        use crate::communication::peer_link::LifecycleEvent;

        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = new_shared();
        let wake = WakeHandle::new();
        let cancel = tokio_util::sync::CancellationToken::new();
        let (events, mut events_rx) = tokio_mpsc::unbounded_channel::<LifecycleEvent>();

        let handle = tokio::spawn(progress_recv_bridge::<u64>(
            rx,
            Arc::clone(&shared),
            wake.clone(),
            cancel.clone(),
            "test-peer".into(),
            42,
            DEFAULT_MAX_BATCH_SIZE,
            events,
        ));

        // Serialize a batch and send it, followed by the end-of-stream marker.
        let changes: Vec<ProgressChange<u64>> = vec![
            ProgressChange::source(0, 0, 42u64, 1),
            ProgressChange::source(1, 0, 100u64, -1),
        ];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        tx.send(encode_progress_ack()).await.unwrap();
        tx.send(bufs.into_iter().next().unwrap()).await.unwrap();
        tx.send(encode_progress_end_of_stream()).await.unwrap();
        // The bridge keeps watching the channel after its end-of-stream and
        // exits once the channel closes.
        drop(tx);

        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("bridge exits when the channel closes after end-of-stream")
            .unwrap();

        let receiver = ProgressReceiver::<u64>::new(Arc::clone(&shared));
        let batches = receiver.drain_all().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], changes);
        // The ack does not end the channel.
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::AckReceived,
                ..
            })
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ProgressEndOfStream,
                ..
            })
        ));
        assert!(!cancel.is_cancelled(), "bridge never cancels on its own");
    }

    #[tokio::test]
    async fn recv_bridge_reports_frame_after_end_of_stream_once() {
        use crate::communication::peer_link::LifecycleEvent;

        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = new_shared();
        let cancel = tokio_util::sync::CancellationToken::new();
        let (handle, mut events_rx) = spawn_recv_bridge(rx, &shared, &cancel);

        #[cfg(any(debug_assertions, feature = "progress-check"))]
        let mark = crate::progress::check::mark();
        let changes = vec![ProgressChange::source(0, 0, 1u64, 1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
        tx.send(encode_progress_end_of_stream()).await.unwrap();
        tx.send(bufs[0].clone()).await.unwrap();
        tx.send(bufs[0].clone()).await.unwrap();
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("bridge exits when the channel closes")
            .unwrap();
        let mut events = Vec::new();
        while let Ok(LifecycleEvent::Link { event, .. }) = events_rx.try_recv() {
            events.push(event);
        }
        assert!(
            matches!(
                events.as_slice(),
                [
                    LinkEvent::ProgressEndOfStream,
                    LinkEvent::ProgressChannelEnded(LinkFailure::ProtocolViolation(_))
                ]
            ),
            "{events:?}"
        );
        let receiver = ProgressReceiver::<u64>::new(Arc::clone(&shared));
        assert!(
            receiver.drain_all().unwrap().is_empty(),
            "frames after end-of-stream are not applied"
        );
        // The checker's V6 recorded the batch after end-of-stream.
        #[cfg(any(debug_assertions, feature = "progress-check"))]
        assert!(
            crate::progress::check::violations_since(mark)
                .iter()
                .any(|v| v.check == crate::progress::check::Check::V6
                    && v.kind == "batch-after-end-of-stream"),
            "V6 missed a progress batch after end-of-stream"
        );
    }

    #[tokio::test]
    async fn recv_bridge_reports_decode_error() {
        use crate::communication::peer_link::LifecycleEvent;

        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = new_shared();
        let cancel = tokio_util::sync::CancellationToken::new();
        let (handle, mut events_rx) = spawn_recv_bridge(rx, &shared, &cancel);

        tx.send(vec![0xFF, 0xFF]).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("bridge should exit promptly")
            .unwrap();
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ProgressChannelEnded(LinkFailure::Decode(_)),
                ..
            })
        ));
        assert!(!cancel.is_cancelled());
    }

    #[tokio::test]
    async fn recv_bridge_reports_connection_drop_before_end_of_stream() {
        use crate::communication::peer_link::LifecycleEvent;

        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = new_shared();
        let cancel = tokio_util::sync::CancellationToken::new();
        let (handle, mut events_rx) = spawn_recv_bridge(rx, &shared, &cancel);

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("bridge should exit promptly")
            .unwrap();
        assert!(matches!(
            events_rx.recv().await,
            Some(LifecycleEvent::Link {
                event: LinkEvent::ProgressChannelEnded(LinkFailure::ChannelClosed),
                ..
            })
        ));
    }

    #[tokio::test]
    async fn recv_bridge_exits_silently_on_cancel() {
        let (_tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = new_shared();
        let cancel = tokio_util::sync::CancellationToken::new();
        let (handle, mut events_rx) = spawn_recv_bridge(rx, &shared, &cancel);

        cancel.cancel();
        handle.await.unwrap();
        assert!(events_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn network_sender_serializes_and_sends() {
        let (unbounded_tx, mut unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let df_id = DataflowId::new();
        let ch_id = PROGRESS_CHANNEL_BASE + 5;

        let tx_clone = unbounded_tx.clone();
        let sender = ProgressSender::<u64>::network(move |changes: Vec<ProgressChange<u64>>| {
            let mut bufs = Vec::new();
            encode_progress_batch::<u64>(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
            for buf in bufs {
                let frame = Frame {
                    dataflow_id: df_id,
                    channel_id: ch_id,
                    payload: buf,
                };
                let _ = tx_clone.send(frame);
            }
        });

        sender.send(vec![ProgressChange::source(0, 0, 99u64, 1)]);

        let frame = unbounded_rx.recv().await.unwrap();
        assert_eq!(frame.channel_id, ch_id);
        let decoded = decode_progress_batch::<u64>(&frame.payload, DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(decoded, vec![ProgressChange::source(0, 0, 99u64, 1)]);
    }

    #[tokio::test]
    async fn network_sender_skips_empty_batch() {
        let (unbounded_tx, mut unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();

        let tx_clone = unbounded_tx.clone();
        let sender = ProgressSender::<u64>::network(move |changes: Vec<ProgressChange<u64>>| {
            let mut bufs = Vec::new();
            encode_progress_batch::<u64>(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE).unwrap();
            for buf in bufs {
                let frame = Frame {
                    dataflow_id: DataflowId::new(),
                    channel_id: 0,
                    payload: buf,
                };
                let _ = tx_clone.send(frame);
            }
        });

        // Empty batch — should not send (ProgressSender::send filters empty).
        sender.send(vec![]);

        // Verify no frame was sent.
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), unbounded_rx.recv()).await;
        assert!(result.is_err(), "no frame should be sent for empty batch");
    }

    /// End-to-end test: progress exchange over a simulated network connection
    /// using TransportSession + bridge tasks.
    #[tokio::test]
    async fn end_to_end_network_progress() {
        use crate::communication::cluster_transport::ClusterTransport;
        use crate::communication::transport_session::{
            ChannelRegistration, PeerConnection, TransportSession,
        };

        let df_id = DataflowId::new();
        let rt = tokio::runtime::Handle::current();

        // Simulated network: two nodes, 2 workers each (4 total).
        // Node A: workers 0, 1. Node B: workers 2, 3.
        let num_workers = 4;

        // Set up duplex connections.
        let (a_to_b, b_from_a) = tokio::io::duplex(64 * 1024);
        let (b_to_a, a_from_b) = tokio::io::duplex(64 * 1024);

        // Progress channel registrations: node-B receives progress from node-A workers.
        let mut b_progress_regs = Vec::new();
        for src in 0..2 {
            for dst in 2..4 {
                b_progress_regs.push(ChannelRegistration {
                    peer_node_id: "node-a".into(),
                    channel_id: progress_channel_id(src, dst, num_workers).unwrap(),
                });
            }
        }

        let mut a_progress_regs = Vec::new();
        for src in 2..4 {
            for dst in 0..2 {
                a_progress_regs.push(ChannelRegistration {
                    peer_node_id: "node-b".into(),
                    channel_id: progress_channel_id(src, dst, num_workers).unwrap(),
                });
            }
        }

        // Create transport sessions.
        let (session_a, recv_a) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-b".into(),
                reader: a_from_b,
                writer: a_to_b,
            }],
            &[],
            &a_progress_regs,
            16,
            &rt,
        )
        .unwrap();

        let (session_b, recv_b) = TransportSession::new(
            df_id,
            vec![PeerConnection {
                node_id: "node-a".into(),
                reader: b_from_a,
                writer: b_to_a,
            }],
            &[],
            &b_progress_regs,
            16,
            &rt,
        )
        .unwrap();

        let transport_a = ClusterTransport::Dedicated(std::sync::Arc::new(session_a));
        let transport_b = ClusterTransport::Dedicated(std::sync::Arc::new(session_b));

        // Create local-only progress channels (we only need the structure;
        // local pairs won't be used in this cross-node test).
        let wake_handles: Vec<WakeHandle> = (0..num_workers).map(|_| WakeHandle::new()).collect();

        let cancel = tokio_util::sync::CancellationToken::new();
        let (events, mut events_rx) = tokio_mpsc::unbounded_channel();
        let outbox = |transport: &ClusterTransport, peer: &str| {
            let (outbox, _forwarder) = crate::communication::peer_link::spawn_peer_outbox(
                peer,
                transport.progress_sender(peer).unwrap(),
                events.clone(),
                &rt,
            );
            HashMap::from([(peer.to_string(), outbox)])
        };
        let outboxes_a = outbox(&transport_a, "node-b");
        let outboxes_b = outbox(&transport_b, "node-a");

        // --- Node A setup: workers 0,1 ---
        let local_a = crate::progress::progress_channel::create_progress_channels::<u64>(
            num_workers,
            &wake_handles,
        )
        .unwrap();
        // Take only node-A's workers (0 and 1).
        let a_worker_channels: Vec<WorkerProgressChannels<u64>> =
            local_a.into_iter().take(2).collect();

        let (a_channels, _handles_a) = create_network_progress_channels::<u64>(
            a_worker_channels,
            &outboxes_a,
            recv_a,
            df_id,
            (0, 2),
            &[("node-b".to_string(), 2, 4)],
            num_workers,
            &wake_handles,
            cancel.clone(),
            events.clone(),
            DEFAULT_MAX_BATCH_SIZE,
            &rt,
        )
        .expect("node-A progress channel setup should succeed");

        // --- Node B setup: workers 2,3 ---
        let local_b = crate::progress::progress_channel::create_progress_channels::<u64>(
            num_workers,
            &wake_handles,
        )
        .unwrap();
        let b_worker_channels: Vec<WorkerProgressChannels<u64>> =
            local_b.into_iter().skip(2).take(2).collect();

        let (b_channels, _handles_b) = create_network_progress_channels::<u64>(
            b_worker_channels,
            &outboxes_b,
            recv_b,
            df_id,
            (2, 4),
            &[("node-a".to_string(), 0, 2)],
            num_workers,
            &wake_handles,
            cancel.clone(),
            events.clone(),
            DEFAULT_MAX_BATCH_SIZE,
            &rt,
        )
        .expect("node-B progress channel setup should succeed");

        // Node-A worker 0 sends progress to node-B worker 2.
        let sender = a_channels[0].senders[2]
            .as_ref()
            .expect("should have network sender for remote worker 2");
        sender.send(vec![
            ProgressChange::source(0, 0, 42u64, 1),
            ProgressChange::source(1, 0, 100u64, -1),
        ]);

        // Allow bridge + transport to process.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Node-B worker 2 (index 0 in b_channels) should have received
        // progress from node-A worker 0.
        let receiver = b_channels[0].receivers[0]
            .as_ref()
            .expect("should have network receiver for worker 0");
        let batches = receiver.drain_all().unwrap();
        assert_eq!(batches.len(), 1, "expected 1 batch, got {}", batches.len());
        assert_eq!(
            batches[0],
            vec![
                ProgressChange::source(0, 0, 42u64, 1),
                ProgressChange::source(1, 0, 100u64, -1),
            ]
        );

        // Verify no spurious failure events.
        assert!(events_rx.try_recv().is_err());
        assert!(!cancel.is_cancelled());

        drop(transport_a);
        drop(transport_b);
    }

    #[test]
    fn decode_rejects_exceeding_max_changes() {
        // Craft a frame with count=100 but max_batch_size=10.
        // We need a valid CRC for the count header.
        let count: u32 = 100;
        let mut payload = count.to_le_bytes().to_vec();
        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());

        let result = decode_progress_batch::<u64>(&payload, 10);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum"),
            "expected 'exceeds maximum' in error, got: {err}"
        );
    }

    #[tokio::test]
    async fn network_sender_discards_progress_for_retired_peer() {
        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(16);
        let (events, _events_rx) = tokio_mpsc::unbounded_channel();
        let (outbox, _forwarder) = crate::communication::peer_link::spawn_peer_outbox(
            "node-b",
            crate::communication::cluster_transport::FrameSender::Direct(tx),
            events.clone(),
            &tokio::runtime::Handle::current(),
        );
        let wake_handles: Vec<WakeHandle> = (0..2).map(|_| WakeHandle::new()).collect();
        let local =
            crate::progress::progress_channel::create_progress_channels::<u64>(2, &wake_handles)
                .unwrap();
        let (channels, _handles) = create_network_progress_channels::<u64>(
            local.into_iter().take(1).collect(),
            &HashMap::from([("node-b".to_string(), outbox.clone())]),
            HashMap::from([(
                "node-b".to_string(),
                HashMap::from([(progress_channel_id(1, 0, 2).unwrap(), {
                    let (_tx, rx) = tokio_mpsc::channel(1);
                    rx
                })]),
            )]),
            DataflowId::new(),
            (0, 1),
            &[("node-b".to_string(), 1, 2)],
            2,
            &wake_handles,
            tokio_util::sync::CancellationToken::new(),
            events,
            DEFAULT_MAX_BATCH_SIZE,
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let sender = channels[0].senders[1].as_ref().unwrap();

        sender.send(vec![ProgressChange::source(0, 0, 1u64, 1)]);
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.channel_id, progress_channel_id(0, 1, 2).unwrap());

        outbox.link().retire();
        sender.send(vec![ProgressChange::source(0, 0, 1u64, -1)]);
        let second = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;
        assert!(second.is_err(), "no progress to a retired peer");
    }
}
