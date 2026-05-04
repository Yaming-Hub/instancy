//! Network progress exchange: serialization, bridge tasks, and factory functions.
//!
//! This module provides the infrastructure for exchanging progress updates
//! across network nodes via [`TransportSession`].
//!
//! # Architecture
//!
//! ```text
//! Send path:
//!   ProgressSender::send()
//!     → serialize Vec<ProgressChange<T>> into bytes
//!     → UnboundedSender<Frame> (never fails)
//!     → [send bridge task] awaits on bounded TransportSession progress channel
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
//! frontier state. The send path uses an unbounded intermediary channel
//! so `ProgressSender::send()` never drops frames. The bridge task handles
//! backpressure to the bounded transport layer via async await.

#[cfg(feature = "transport")]
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "transport")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "transport")]
use tokio::sync::mpsc as tokio_mpsc;

#[cfg(feature = "transport")]
use crate::communication::codec::{Codec, CodecError, ExchangeData};
#[cfg(feature = "transport")]
use crate::communication::transport::Frame;
#[cfg(feature = "transport")]
use crate::communication::transport_session::{TransportSession, PROGRESS_CHANNEL_BASE};
#[cfg(feature = "transport")]
use crate::dataflow::channels::wake::WakeHandle;
#[cfg(feature = "transport")]
use crate::dataflow::id::DataflowId;
#[cfg(feature = "transport")]
use crate::progress::progress_channel::{
    ProgressChange, ProgressReceiver, ProgressSender, SharedBuffer, WorkerProgressChannels,
};
#[cfg(feature = "transport")]
use crate::progress::timestamp::Timestamp;

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
///   [operator_index: u64]
///   [output_port: u64]
///   [timestamp: T encoded by T::codec()]
///   [diff: i64]
/// [crc32: u32]   ← CRC32 of all preceding bytes
/// ```
///
/// If `changes.len()` exceeds `max_batch_size`, the batch is split into
/// multiple frames appended sequentially to `bufs`. Each frame is a
/// self-contained encoded batch with its own CRC32.
#[cfg(feature = "transport")]
pub fn encode_progress_batch<T: Timestamp + ExchangeData>(
    changes: &[ProgressChange<T>],
    bufs: &mut Vec<Vec<u8>>,
    max_batch_size: usize,
) {
    // Cap at u32::MAX since the wire format uses a u32 count field.
    let effective_max = max_batch_size.min(u32::MAX as usize).max(1);

    // Handle empty batch: still emit one frame with count=0 + CRC.
    if changes.is_empty() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        bufs.push(buf);
        return;
    }

    for chunk in changes.chunks(effective_max) {
        let mut buf = Vec::new();
        // Safe: effective_max <= u32::MAX, so chunk.len() <= u32::MAX.
        let count = chunk.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());

        let codec = T::codec();
        for (op_idx, output_port, time, diff) in chunk {
            buf.extend_from_slice(&(*op_idx as u64).to_le_bytes());
            buf.extend_from_slice(&(*output_port as u64).to_le_bytes());
            codec
                .encode(time, &mut buf)
                .expect("progress timestamp encode should not fail");
            buf.extend_from_slice(&diff.to_le_bytes());
        }

        // Append CRC32 checksum of the payload.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        bufs.push(buf);
    }
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
    let expected_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(CodecError::InvalidData(format!(
            "CRC32 mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        )));
    }

    // Now decode the verified payload.
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;

    if count > max_batch_size {
        return Err(CodecError::InvalidData(format!(
            "progress batch count {count} exceeds maximum {max_batch_size}"
        )));
    }

    let mut changes = Vec::with_capacity(count);
    let codec = T::codec();
    let mut offset = 4;

    for _ in 0..count {
        // operator_index (u64)
        if offset + 8 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: payload.len(),
            });
        }
        let op_idx = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        // output_port (u64)
        if offset + 8 > payload.len() {
            return Err(CodecError::InsufficientData {
                needed: offset + 8,
                available: payload.len(),
            });
        }
        let output_port =
            u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap()) as usize;
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
        let diff = i64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;

        changes.push((op_idx, output_port, time, diff));
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

// ---------------------------------------------------------------------------
// Bridge tasks
// ---------------------------------------------------------------------------

/// Bridge task: forwards frames from an unbounded intermediary channel
/// to the TransportSession's bounded progress channel.
///
/// This provides backpressure to the transport layer while keeping
/// `ProgressSender::send()` non-blocking and reliable.
///
/// If the transport session closes (send fails), the dataflow is cancelled
/// via the shared `CancellationToken` because any frames still queued
/// represent non-idempotent progress deltas that would be lost.
#[cfg(feature = "transport")]
async fn progress_send_bridge(
    mut rx: tokio_mpsc::UnboundedReceiver<Frame>,
    session_tx: tokio_mpsc::Sender<Frame>,
    cancel: tokio_util::sync::CancellationToken,
    peer_id: String,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            frame = rx.recv() => match frame {
                Some(frame) => {
                    if session_tx.send(frame).await.is_err() {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            peer = %peer_id,
                            "progress send bridge: transport session closed, cancelling dataflow"
                        );
                        cancel.cancel();
                        break;
                    }
                }
                None => {
                    // All senders dropped — normal shutdown path.
                    break;
                }
            }
        }
    }
}

/// Bridge task: reads serialized progress frames from a Demuxer channel,
/// deserializes them, and pushes into the local SharedBuffer for the
/// target worker's ProgressReceiver.
///
/// Wakes the target worker on each received batch so the executor
/// processes the incoming progress updates promptly.
///
/// On decode error or connection drop, cancels the dataflow via the
/// shared `CancellationToken`. Connection drop (`rx.recv()` returning
/// `None`) means the peer is gone and any in-flight deltas may be lost,
/// which is fatal for non-idempotent progress tracking.
#[cfg(feature = "transport")]
async fn progress_recv_bridge<T: Timestamp + ExchangeData>(
    mut rx: tokio_mpsc::Receiver<Vec<u8>>,
    buffer: Arc<Mutex<SharedBuffer<T>>>,
    wake_handle: WakeHandle,
    cancel: tokio_util::sync::CancellationToken,
    peer_id: String,
    channel_id: u64,
    max_batch_size: usize,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            data = rx.recv() => match data {
                Some(bytes) => {
                    match decode_progress_batch::<T>(&bytes, max_batch_size) {
                        Ok(changes) => {
                            if !changes.is_empty() {
                                {
                                    let mut buf = buffer.lock()
                                        .expect("progress buffer lock poisoned");
                                    buf.queue.push_back(changes);
                                }
                                wake_handle.notify();
                            }
                        }
                        Err(e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                %peer_id,
                                channel_id,
                                "Fatal: progress decode error: {e}"
                            );
                            cancel.cancel();
                            break;
                        }
                    }
                }
                None => {
                    // Connection to peer lost — progress deltas may have been lost.
                    #[cfg(feature = "tracing")]
                    tracing::error!(
                        %peer_id,
                        channel_id,
                        "progress recv bridge: peer connection lost, cancelling dataflow"
                    );
                    cancel.cancel();
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Progress channel ID helpers
// ---------------------------------------------------------------------------

/// Compute the progress channel ID for a (source, target) worker pair.
///
/// Uses the same formula as data channels but offset by `PROGRESS_CHANNEL_BASE`:
/// `PROGRESS_CHANNEL_BASE + source * num_workers + target`.
///
/// # Panics
///
/// Panics if the arithmetic overflows (extremely large worker counts).
#[cfg(feature = "transport")]
pub fn progress_channel_id(source: usize, target: usize, num_workers: usize) -> u64 {
    let offset = (source as u64)
        .checked_mul(num_workers as u64)
        .and_then(|v| v.checked_add(target as u64))
        .expect("progress_channel_id overflow: too many workers");
    PROGRESS_CHANNEL_BASE
        .checked_add(offset)
        .expect("progress_channel_id overflow: exceeds u64 range")
}

// ---------------------------------------------------------------------------
// Factory: create network progress channels
// ---------------------------------------------------------------------------

/// Holds background task handles for network progress bridges.
///
/// On drop, cancels the shared `CancellationToken` (signalling all bridges
/// to stop) and then aborts any remaining tasks. This ensures graceful
/// shutdown when the dataflow is still active, and prevents leaked tasks.
///
/// # Important
///
/// Callers must keep this struct alive for the lifetime of the dataflow.
/// Dropping it prematurely will cancel progress exchange.
#[cfg(feature = "transport")]
#[must_use = "dropping NetworkProgressHandles cancels progress exchange"]
pub struct NetworkProgressHandles {
    cancel: tokio_util::sync::CancellationToken,
    send_handles: Vec<tokio::task::JoinHandle<()>>,
    recv_handles: Vec<tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "transport")]
impl Drop for NetworkProgressHandles {
    fn drop(&mut self) {
        // Signal all bridges to shut down gracefully first.
        self.cancel.cancel();
        // Then abort any that haven't exited yet.
        for h in &self.send_handles {
            h.abort();
        }
        for h in &self.recv_handles {
            h.abort();
        }
    }
}

/// Create network progress senders and receiver bridges for cross-node
/// progress exchange.
///
/// This function creates:
/// - **Network senders** for each `(local_worker, remote_worker)` pair,
///   with one send bridge task per remote peer (shared by all local workers
///   sending to that peer).
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
/// - `session`: The TransportSession providing progress-priority channels.
/// - `receivers`: Per-peer channel receivers from `TransportSession::new()`.
/// - `dataflow_id`: Dataflow ID for frame construction.
/// - `local_worker_range`: `(start, end)` of local worker indices.
/// - `remote_peers`: List of `(peer_node_id, worker_start, worker_end)`.
/// - `num_workers`: Total worker count across all nodes.
/// - `wake_handles`: Wake handles for ALL workers (indexed by global worker ID).
///   Only local workers' handles are used for receiver bridges.
/// - `cancel`: CancellationToken for fatal error propagation.
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
pub fn create_network_progress_channels<T: Timestamp + ExchangeData>(
    mut local_channels: Vec<WorkerProgressChannels<T>>,
    session: &TransportSession,
    mut receivers: HashMap<String, HashMap<u64, tokio_mpsc::Receiver<Vec<u8>>>>,
    dataflow_id: DataflowId,
    local_worker_range: (usize, usize),
    remote_peers: &[(String, usize, usize)],
    num_workers: usize,
    wake_handles: &[WakeHandle],
    cancel: tokio_util::sync::CancellationToken,
    max_batch_size: usize,
    runtime_handle: &tokio::runtime::Handle,
) -> Result<(Vec<WorkerProgressChannels<T>>, NetworkProgressHandles), String> {
    let (local_start, local_end) = local_worker_range;

    // --- Input validation ---
    if local_start > local_end {
        return Err(format!(
            "invalid local_worker_range: start ({local_start}) > end ({local_end})"
        ));
    }
    if local_end > num_workers {
        return Err(format!(
            "local_worker_range end ({local_end}) exceeds num_workers ({num_workers})"
        ));
    }
    let expected_local = local_end - local_start;
    if local_channels.len() != expected_local {
        return Err(format!(
            "local_channels.len() ({}) != local_worker_range size ({expected_local})",
            local_channels.len()
        ));
    }
    if wake_handles.len() < num_workers {
        return Err(format!(
            "wake_handles.len() ({}) < num_workers ({num_workers})",
            wake_handles.len()
        ));
    }
    for (peer_id, peer_start, peer_end) in remote_peers {
        if *peer_start > *peer_end || *peer_end > num_workers {
            return Err(format!(
                "invalid peer range for {peer_id}: ({peer_start}, {peer_end}), num_workers={num_workers}"
            ));
        }
    }

    let mut send_handles = Vec::new();
    let mut recv_handles = Vec::new();

    // --- Send side: one unbounded channel per peer, shared by all local workers ---
    for (peer_id, peer_start, peer_end) in remote_peers {
        // Get the TransportSession's progress sender for this peer.
        let session_tx = session
            .progress_sender(peer_id)
            .ok_or_else(|| format!("missing progress sender for peer {peer_id}"))?
            .clone();

        // Create unbounded intermediary channel for this peer.
        let (unbounded_tx, unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();

        // Spawn send bridge task for this peer.
        let cancel_clone = cancel.clone();
        let peer_id_clone = peer_id.clone();
        let handle = runtime_handle.spawn(progress_send_bridge(
            unbounded_rx,
            session_tx,
            cancel_clone,
            peer_id_clone,
        ));
        send_handles.push(handle);

        // Create network senders for each (local_worker → remote_worker) pair.
        for local_w in local_start..local_end {
            let local_idx = local_w - local_start;
            for remote_w in *peer_start..*peer_end {
                let ch_id = progress_channel_id(local_w, remote_w, num_workers);
                let df_id = dataflow_id;
                let tx = unbounded_tx.clone();
                let cancel_for_closure = cancel.clone();
                let peer_id_for_closure = peer_id.clone();

                let sender = ProgressSender::network(move |changes: Vec<ProgressChange<T>>| {
                    // Pre-check: if bridge is already gone, cancel immediately
                    // before encoding/sending any frames (prevents partial batch).
                    if tx.is_closed() {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            peer = %peer_id_for_closure,
                            channel_id = ch_id,
                            "progress send: bridge closed, cancelling dataflow"
                        );
                        cancel_for_closure.cancel();
                        return;
                    }
                    let mut bufs = Vec::new();
                    encode_progress_batch::<T>(&changes, &mut bufs, max_batch_size);
                    for buf in bufs {
                        let frame = Frame {
                            dataflow_id: df_id,
                            channel_id: ch_id,
                            payload: buf,
                        };
                        if tx.send(frame).is_err() {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                peer = %peer_id_for_closure,
                                channel_id = ch_id,
                                "progress send: bridge closed, cancelling dataflow"
                            );
                            cancel_for_closure.cancel();
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
            format!("missing receiver map for peer {peer_id}")
        })?;

        for remote_w in *peer_start..*peer_end {
            for local_w in local_start..local_end {
                let local_idx = local_w - local_start;
                let ch_id = progress_channel_id(remote_w, local_w, num_workers);

                let rx = peer_map.remove(&ch_id).ok_or_else(|| {
                    format!(
                        "missing receiver for peer {peer_id}, channel {ch_id} \
                         (remote_worker={remote_w} → local_worker={local_w})"
                    )
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
                ));
                recv_handles.push(handle);

                local_channels[local_idx].receivers[remote_w] = Some(receiver);
            }
        }
    }

    let handles = NetworkProgressHandles {
        cancel: cancel.clone(),
        send_handles,
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

    #[test]
    fn encode_decode_roundtrip_u64() {
        let changes: Vec<ProgressChange<u64>> = vec![
            (0, 0, 42u64, 1),
            (1, 2, 100u64, -1),
            (3, 0, 0u64, 5),
        ];

        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
        assert_eq!(bufs.len(), 1);

        let decoded = decode_progress_batch::<u64>(&bufs[0], DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(decoded, changes);
    }

    #[test]
    fn encode_decode_empty_batch() {
        let changes: Vec<ProgressChange<u64>> = vec![];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
        assert_eq!(bufs.len(), 1);

        let decoded = decode_progress_batch::<u64>(&bufs[0], DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn decode_rejects_truncated_data() {
        let changes: Vec<ProgressChange<u64>> = vec![(0, 0, 42u64, 1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);

        // Truncate the buffer (removes part of CRC or payload).
        let buf = &bufs[0];
        let result = decode_progress_batch::<u64>(&buf[..buf.len() - 2], DEFAULT_MAX_BATCH_SIZE);
        assert!(result.is_err());
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let changes: Vec<ProgressChange<u64>> = vec![(0, 0, 42u64, 1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
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
        let changes: Vec<ProgressChange<u64>> = vec![(0, 0, 42u64, 1), (1, 0, 100u64, -1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
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
            .map(|i| (i, 0, i as u64, 1))
            .collect();

        // max_batch_size=2 → should produce 3 frames (2+2+1).
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, 2);
        assert_eq!(bufs.len(), 3);

        // Decode each and reassemble.
        let mut all = Vec::new();
        for buf in &bufs {
            let decoded = decode_progress_batch::<u64>(buf, 2).unwrap();
            all.extend(decoded);
        }
        assert_eq!(all, changes);
    }

    #[test]
    fn progress_channel_id_formula() {
        // 4 workers total, src=1, dst=3 → PROGRESS_CHANNEL_BASE + 1*4 + 3 = 1_000_007
        assert_eq!(progress_channel_id(1, 3, 4), PROGRESS_CHANNEL_BASE + 7);
        assert_eq!(progress_channel_id(0, 0, 4), PROGRESS_CHANNEL_BASE);
    }

    #[tokio::test]
    async fn send_bridge_forwards_frames() {
        let (unbounded_tx, unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let (session_tx, mut session_rx) = tokio_mpsc::channel::<Frame>(16);
        let cancel = tokio_util::sync::CancellationToken::new();

        // Spawn bridge task.
        let handle = tokio::spawn(progress_send_bridge(
            unbounded_rx,
            session_tx,
            cancel.clone(),
            "test-peer".into(),
        ));

        // Send a frame through the unbounded channel.
        let frame = Frame {
            dataflow_id: DataflowId::new(),
            channel_id: 42,
            payload: vec![1, 2, 3],
        };
        unbounded_tx.send(frame.clone()).unwrap();

        // Should arrive via the bridge.
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            session_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received.payload, vec![1, 2, 3]);
        assert_eq!(received.channel_id, 42);

        // No cancellation on normal forwarding.
        assert!(!cancel.is_cancelled());

        drop(unbounded_tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn send_bridge_cancels_on_transport_close() {
        let (unbounded_tx, unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let (session_tx, session_rx) = tokio_mpsc::channel::<Frame>(1);
        let cancel = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(progress_send_bridge(
            unbounded_rx,
            session_tx,
            cancel.clone(),
            "test-peer".into(),
        ));

        // Drop the session receiver to simulate transport close.
        drop(session_rx);

        // Send a frame — bridge should detect closed transport and cancel.
        let frame = Frame {
            dataflow_id: DataflowId::new(),
            channel_id: 1,
            payload: vec![1],
        };
        unbounded_tx.send(frame).unwrap();

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .expect("bridge should exit promptly");

        assert!(cancel.is_cancelled(), "should cancel dataflow on transport close");
    }

    #[tokio::test]
    async fn recv_bridge_deserializes_and_wakes() {
        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = Arc::new(Mutex::new(SharedBuffer::<u64> {
            queue: VecDeque::new(),
        }));
        let wake = WakeHandle::new();
        let cancel = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(progress_recv_bridge::<u64>(
            rx,
            Arc::clone(&shared),
            wake.clone(),
            cancel.clone(),
            "test-peer".into(),
            42,
            DEFAULT_MAX_BATCH_SIZE,
        ));

        // Serialize a batch and send it.
        let changes: Vec<ProgressChange<u64>> = vec![(0, 0, 42u64, 1), (1, 0, 100u64, -1)];
        let mut bufs = Vec::new();
        encode_progress_batch(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
        tx.send(bufs.into_iter().next().unwrap()).await.unwrap();

        // Wait for the bridge to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let receiver = ProgressReceiver::<u64>::new(Arc::clone(&shared));
        let batches = receiver.drain_all();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], changes);

        drop(tx);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn recv_bridge_cancels_on_decode_error() {
        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = Arc::new(Mutex::new(SharedBuffer::<u64> {
            queue: VecDeque::new(),
        }));
        let wake = WakeHandle::new();
        let cancel = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(progress_recv_bridge::<u64>(
            rx,
            Arc::clone(&shared),
            wake,
            cancel.clone(),
            "test-peer".into(),
            99,
            DEFAULT_MAX_BATCH_SIZE,
        ));

        // Send garbage bytes.
        tx.send(vec![0xFF, 0xFF]).await.unwrap();

        // Wait for the bridge to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Cancellation token should be triggered.
        assert!(cancel.is_cancelled());

        let _ = handle.await;
    }

    #[tokio::test]
    async fn recv_bridge_cancels_on_connection_drop() {
        let (tx, rx) = tokio_mpsc::channel::<Vec<u8>>(16);
        let shared = Arc::new(Mutex::new(SharedBuffer::<u64> {
            queue: VecDeque::new(),
        }));
        let wake = WakeHandle::new();
        let cancel = tokio_util::sync::CancellationToken::new();

        let handle = tokio::spawn(progress_recv_bridge::<u64>(
            rx,
            Arc::clone(&shared),
            wake,
            cancel.clone(),
            "test-peer".into(),
            42,
            DEFAULT_MAX_BATCH_SIZE,
        ));

        // Drop sender to simulate connection loss.
        drop(tx);

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle,
        )
        .await
        .expect("bridge should exit promptly");

        assert!(cancel.is_cancelled(), "should cancel dataflow on connection drop");
    }

    #[tokio::test]
    async fn network_sender_serializes_and_sends() {
        let (unbounded_tx, mut unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let df_id = DataflowId::new();
        let ch_id = PROGRESS_CHANNEL_BASE + 5;

        let tx_clone = unbounded_tx.clone();
        let sender = ProgressSender::<u64>::network(move |changes: Vec<ProgressChange<u64>>| {
            let mut bufs = Vec::new();
            encode_progress_batch::<u64>(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
            for buf in bufs {
                let frame = Frame {
                    dataflow_id: df_id,
                    channel_id: ch_id,
                    payload: buf,
                };
                let _ = tx_clone.send(frame);
            }
        });

        sender.send(vec![(0, 0, 99u64, 1)]);

        let frame = unbounded_rx.recv().await.unwrap();
        assert_eq!(frame.channel_id, ch_id);
        let decoded = decode_progress_batch::<u64>(&frame.payload, DEFAULT_MAX_BATCH_SIZE).unwrap();
        assert_eq!(decoded, vec![(0, 0, 99u64, 1)]);
    }

    #[tokio::test]
    async fn network_sender_skips_empty_batch() {
        let (unbounded_tx, mut unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();

        let tx_clone = unbounded_tx.clone();
        let sender = ProgressSender::<u64>::network(move |changes: Vec<ProgressChange<u64>>| {
            let mut bufs = Vec::new();
            encode_progress_batch::<u64>(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
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
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            unbounded_rx.recv(),
        )
        .await;
        assert!(result.is_err(), "no frame should be sent for empty batch");
    }

    /// End-to-end test: progress exchange over a simulated network connection
    /// using TransportSession + bridge tasks.
    #[tokio::test]
    async fn end_to_end_network_progress() {
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
                    channel_id: progress_channel_id(src, dst, num_workers),
                });
            }
        }

        let mut a_progress_regs = Vec::new();
        for src in 2..4 {
            for dst in 0..2 {
                a_progress_regs.push(ChannelRegistration {
                    peer_node_id: "node-b".into(),
                    channel_id: progress_channel_id(src, dst, num_workers),
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
        );

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
        );

        // Create local-only progress channels (we only need the structure;
        // local pairs won't be used in this cross-node test).
        let wake_handles: Vec<WakeHandle> = (0..num_workers).map(|_| WakeHandle::new()).collect();

        let cancel = tokio_util::sync::CancellationToken::new();

        // --- Node A setup: workers 0,1 ---
        let local_a = crate::progress::progress_channel::create_progress_channels::<u64>(
            num_workers,
            &wake_handles,
        );
        // Take only node-A's workers (0 and 1).
        let a_worker_channels: Vec<WorkerProgressChannels<u64>> = local_a
            .into_iter()
            .take(2)
            .collect();

        let (a_channels, _handles_a) = create_network_progress_channels::<u64>(
            a_worker_channels,
            &session_a,
            recv_a,
            df_id,
            (0, 2),
            &[("node-b".to_string(), 2, 4)],
            num_workers,
            &wake_handles,
            cancel.clone(),
            DEFAULT_MAX_BATCH_SIZE,
            &rt,
        )
        .expect("node-A progress channel setup should succeed");

        // --- Node B setup: workers 2,3 ---
        let local_b = crate::progress::progress_channel::create_progress_channels::<u64>(
            num_workers,
            &wake_handles,
        );
        let b_worker_channels: Vec<WorkerProgressChannels<u64>> = local_b
            .into_iter()
            .skip(2)
            .take(2)
            .collect();

        let (b_channels, _handles_b) = create_network_progress_channels::<u64>(
            b_worker_channels,
            &session_b,
            recv_b,
            df_id,
            (2, 4),
            &[("node-a".to_string(), 0, 2)],
            num_workers,
            &wake_handles,
            cancel.clone(),
            DEFAULT_MAX_BATCH_SIZE,
            &rt,
        )
        .expect("node-B progress channel setup should succeed");

        // Node-A worker 0 sends progress to node-B worker 2.
        let sender = a_channels[0].senders[2]
            .as_ref()
            .expect("should have network sender for remote worker 2");
        sender.send(vec![(0, 0, 42u64, 1), (1, 0, 100u64, -1)]);

        // Allow bridge + transport to process.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Node-B worker 2 (index 0 in b_channels) should have received
        // progress from node-A worker 0.
        let receiver = b_channels[0].receivers[0]
            .as_ref()
            .expect("should have network receiver for worker 0");
        let batches = receiver.drain_all();
        assert_eq!(batches.len(), 1, "expected 1 batch, got {}", batches.len());
        assert_eq!(batches[0], vec![(0, 0, 42u64, 1), (1, 0, 100u64, -1)]);

        // Verify no spurious cancellation.
        assert!(!cancel.is_cancelled());

        drop(session_a);
        drop(session_b);
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
    async fn send_closure_cancels_on_bridge_exit() {
        let (unbounded_tx, unbounded_rx) = tokio_mpsc::unbounded_channel::<Frame>();
        let cancel = tokio_util::sync::CancellationToken::new();

        let cancel_for_closure = cancel.clone();
        let sender = ProgressSender::<u64>::network(move |changes: Vec<ProgressChange<u64>>| {
            let mut bufs = Vec::new();
            encode_progress_batch::<u64>(&changes, &mut bufs, DEFAULT_MAX_BATCH_SIZE);
            for buf in bufs {
                let frame = Frame {
                    dataflow_id: DataflowId::new(),
                    channel_id: 1,
                    payload: buf,
                };
                if unbounded_tx.send(frame).is_err() {
                    cancel_for_closure.cancel();
                    return;
                }
            }
        });

        // Drop the receiver to simulate bridge exit.
        drop(unbounded_rx);

        // Sending should trigger cancellation.
        sender.send(vec![(0, 0, 1u64, 1)]);
        assert!(cancel.is_cancelled(), "should cancel when bridge is gone");
    }
}
