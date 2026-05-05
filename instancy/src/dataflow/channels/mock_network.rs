//! Mock network edge materializer for testing cross-node exchange.
//!
//! Provides [`MockNetworkEdgeMaterializer`], which implements
//! [`EdgeMaterializer`] using a mix of local and serializing channels:
//!
//! - **Same-node** worker pairs: direct in-process `BoundedPush`/`BoundedPull`
//!   (shared memory, zero serialization overhead).
//! - **Cross-node** worker pairs: `SerializingPush` / `DeserializingPull`
//!   that encode/decode data through the [`Codec`] trait over in-memory byte
//!   channels.
//!
//! This validates the full logical/physical separation story: the same
//! `ExchangePush`/`ExchangePull` wrappers work identically whether the
//! underlying transport is shared memory or serialized bytes, because they
//! only interact through the `Push`/`Pull` trait interface.
//!
//! # What this exercises
//!
//! - Real [`Codec`] encode/decode round-trips (proves data survives
//!   serialization)
//! - Mixed local + remote endpoints in a single `ExchangePush`/`ExchangePull`
//! - [`ClusterTopology`] for worker-to-node mapping
//! - The [`EdgeMaterializer`] abstraction for pluggable transport
//!
//! # What this does NOT exercise
//!
//! - Real network I/O (TCP, QUIC)
//! - Muxer/Demuxer frame multiplexing
//! - Connection pooling
//! - Async transport tasks
//!
//! Those are deferred to `NetworkEdgeMaterializer` (PR 43D).
//!
//! [`Codec`]: crate::communication::codec::Codec
//! [`ClusterTopology`]: crate::execute::ClusterTopology

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::communication::codec::{Codec, CodecError, ExchangeData};
use crate::dataflow::channels::edge_materializer::EdgeMaterializer;
use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::error::{Error, Result};
use crate::execute::ClusterTopology;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// ByteChannel — bounded in-memory byte transport
// ---------------------------------------------------------------------------

/// A simple bounded FIFO byte channel for mock network transport.
///
/// Each message is a `Vec<u8>` (one serialized envelope). The channel has
/// a fixed capacity (number of messages, not bytes). This simulates network
/// backpressure without actual I/O.
struct ByteChannelState {
    buffer: VecDeque<Vec<u8>>,
    capacity: usize,
    closed: bool,
}

/// Send half of a byte channel.
struct ByteSender {
    state: Arc<Mutex<ByteChannelState>>,
    closed: Arc<AtomicBool>,
}

/// Receive half of a byte channel.
struct ByteReceiver {
    state: Arc<Mutex<ByteChannelState>>,
    closed: Arc<AtomicBool>,
}

fn byte_channel(capacity: usize) -> (ByteSender, ByteReceiver) {
    let state = Arc::new(Mutex::new(ByteChannelState {
        buffer: VecDeque::with_capacity(capacity),
        capacity,
        closed: false,
    }));
    let closed = Arc::new(AtomicBool::new(false));
    (
        ByteSender {
            state: state.clone(),
            closed: closed.clone(),
        },
        ByteReceiver { state, closed },
    )
}

impl ByteSender {
    fn send(&self, data: Vec<u8>) -> std::result::Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::Custom("poisoned".into()))?;
        if state.closed {
            return Err(Error::ChannelClosed);
        }
        if state.buffer.len() >= state.capacity {
            return Err(Error::Backpressure);
        }
        state.buffer.push_back(data);
        Ok(())
    }

    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
        self.closed.store(true, Ordering::Release);
    }
}

impl ByteReceiver {
    fn recv(&self) -> Option<Vec<u8>> {
        let mut state = self.state.lock().ok()?;
        state.buffer.pop_front()
    }

    fn is_exhausted(&self) -> bool {
        let is_closed = self.closed.load(Ordering::Acquire);
        let is_empty = self
            .state
            .lock()
            .map_or(true, |s| s.buffer.is_empty());
        is_closed && is_empty
    }
}

impl Drop for ByteSender {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------------------
// MockFrame — serialized envelope wire format
// ---------------------------------------------------------------------------

/// Tag byte for the mock wire format.
const TAG_DATA: u8 = 0x01;
const TAG_WATERMARK: u8 = 0x02;
const TAG_ERROR: u8 = 0x03;

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
            // Encode record count as u32 LE.
            let count: u32 = data.len().try_into().map_err(|_| {
                CodecError::InvalidData(format!(
                    "batch too large for mock wire format: {} records",
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
            // Encode source_operator as length-prefixed string.
            let src_bytes = source_operator.as_bytes();
            buf.extend_from_slice(&(src_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(src_bytes);
            // Encode message as length-prefixed string.
            let msg_bytes = message.as_bytes();
            buf.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(msg_bytes);
        }
    }
    Ok(())
}

/// Decode an envelope from bytes using the provided codecs.
fn decode_envelope<T, D, TC, DC>(
    time_codec: &TC,
    data_codec: &DC,
    buf: &[u8],
) -> std::result::Result<Envelope<T, D, ()>, CodecError>
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
            // Guard against unreasonable allocation from malformed data.
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
            Ok(Envelope::data(time, data))
        }
        TAG_WATERMARK => {
            let (time, consumed) = time_codec.decode(rest)?;
            if consumed != rest.len() {
                return Err(CodecError::InvalidData(format!(
                    "trailing bytes in watermark envelope: consumed {consumed}, total {}",
                    rest.len()
                )));
            }
            Ok(Envelope::watermark(time))
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
                .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8 in source_operator: {e}")))?;
            let msg_offset = 4 + src_len;
            let msg_len = u32::from_le_bytes(
                rest[msg_offset..msg_offset + 4].try_into().unwrap(),
            ) as usize;
            if rest.len() < msg_offset + 4 + msg_len {
                return Err(CodecError::InsufficientData {
                    needed: msg_offset + 4 + msg_len,
                    available: rest.len(),
                });
            }
            let total_consumed = msg_offset + 4 + msg_len;
            if total_consumed != rest.len() {
                return Err(CodecError::InvalidData(format!(
                    "trailing bytes in error envelope: consumed {total_consumed}, total {}",
                    rest.len()
                )));
            }
            let message = String::from_utf8(rest[msg_offset + 4..total_consumed].to_vec())
                .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8 in message: {e}")))?;
            Ok(Envelope::error(source_operator, message))
        }
        _ => Err(CodecError::InvalidData(format!("unknown tag: {tag:#x}"))),
    }
}

// ---------------------------------------------------------------------------
// SerializingPush — Push<T, D, ()> that serializes to bytes
// ---------------------------------------------------------------------------

/// A `Push` endpoint that serializes envelopes through a [`Codec`] and
/// sends the resulting bytes over an in-memory byte channel.
///
/// This simulates the send side of a network-backed channel: data is
/// serialized (exercising the production Codec path) and transmitted
/// as opaque bytes. The paired [`DeserializingPull`] deserializes on
/// the other end.
pub(crate) struct SerializingPush<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    sender: ByteSender,
    closed: bool,
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> SerializingPush<T, D> {
    fn new(sender: ByteSender) -> Self {
        Self {
            time_codec: T::codec(),
            data_codec: D::codec(),
            sender,
            closed: false,
        }
    }

    fn encode_envelope(
        &self,
        envelope: &Envelope<T, D, ()>,
    ) -> std::result::Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        encode_envelope(&self.time_codec, &self.data_codec, envelope, &mut buf)
            .map_err(|e| Error::Custom(format!("mock network encode: {e}")))?;
        Ok(buf)
    }
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> Push<T, D, ()>
    for SerializingPush<T, D>
{
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }
        let bytes = self.encode_envelope(&envelope)?;
        self.sender.send(bytes)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, ()>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, ()>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }
        let bytes = match self.encode_envelope(&envelope) {
            Ok(b) => b,
            Err(e) => return Err((e, envelope)),
        };
        match self.sender.send(bytes) {
            Ok(()) => Ok(()),
            Err(e) => Err((e, envelope)),
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(()) // Byte channel is synchronous — data is immediately visible.
    }

    fn close(&mut self) {
        self.closed = true;
        self.sender.close();
    }

    fn is_closed(&self) -> bool {
        self.closed
    }

    fn available_capacity(&self) -> Option<usize> {
        // Report capacity so ExchangePush can do atomic pre-check.
        // Return None on poisoned lock (unknown state).
        match self.sender.state.lock() {
            Ok(state) => Some(state.capacity.saturating_sub(state.buffer.len())),
            Err(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// DeserializingPull — Pull<T, D, ()> that deserializes from bytes
// ---------------------------------------------------------------------------

/// A `Pull` endpoint that receives bytes from an in-memory byte channel
/// and deserializes them through a [`Codec`] back into typed envelopes.
///
/// This simulates the receive side of a network-backed channel.
pub(crate) struct DeserializingPull<T: Timestamp + ExchangeData, D: ExchangeData> {
    time_codec: T::CodecType,
    data_codec: D::CodecType,
    receiver: ByteReceiver,
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> DeserializingPull<T, D> {
    fn new(receiver: ByteReceiver) -> Self {
        Self {
            time_codec: T::codec(),
            data_codec: D::codec(),
            receiver,
        }
    }
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> Pull<T, D, ()>
    for DeserializingPull<T, D>
{
    fn pull(&mut self) -> Option<Envelope<T, D, ()>> {
        let bytes = self.receiver.recv()?;
        // Panic on decode error: this is a test mock, so a decode failure
        // always indicates a codec bug that must be surfaced immediately.
        Some(
            decode_envelope(&self.time_codec, &self.data_codec, &bytes)
                .expect("MockNetworkEdgeMaterializer: decode error indicates a codec bug"),
        )
    }

    fn is_exhausted(&self) -> bool {
        self.receiver.is_exhausted()
    }
}

// ---------------------------------------------------------------------------
// MockNetworkEdgeMaterializer
// ---------------------------------------------------------------------------

/// Edge materializer that simulates multi-node exchange in a single process.
///
/// **All** worker pairs — whether same-node or cross-node — communicate
/// through `SerializingPush`/`DeserializingPull`, which encode and
/// decode every envelope through the [`Codec`] trait over in-memory byte
/// channels. This ensures the mock exercises the full serialization
/// round-trip for every message, catching codec bugs that a direct
/// in-memory channel would miss.
///
/// The [`ClusterTopology`] is retained for informational purposes (e.g.,
/// to verify worker-to-node mapping in tests) but does not affect the
/// transport path — serialization is always used.
///
/// # Usage
///
/// ```ignore
/// let topology = ClusterTopology::multi_node(vec![
///     NodeConfig::new("node-a", 2),  // workers 0, 1
///     NodeConfig::new("node-b", 2),  // workers 2, 3
/// ]).unwrap();
///
/// let mut mat = MockNetworkEdgeMaterializer::<u64, String>::new(
///     topology, 1024,
/// );
///
/// // All pairs use SerializingPush → DeserializingPull (Codec round-trip)
/// ```
pub struct MockNetworkEdgeMaterializer<T: Timestamp + ExchangeData, D: ExchangeData> {
    _topology: ClusterTopology,
    num_workers: usize,

    /// Byte channel senders: senders[src][dst].
    senders: Vec<Vec<Option<ByteSender>>>,
    /// Byte channel receivers: receivers[src][dst].
    receivers: Vec<Vec<Option<ByteReceiver>>>,

    /// Track which workers have been materialized.
    taken: Vec<bool>,

    _phantom: std::marker::PhantomData<(T, D)>,
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> MockNetworkEdgeMaterializer<T, D> {
    /// Create a mock network materializer for the given topology.
    ///
    /// All N×N worker-pair channels are byte channels with the given
    /// capacity (number of serialized messages). Every push/pull goes
    /// through Codec encode/decode.
    pub fn new(topology: ClusterTopology, capacity: usize) -> Self {
        let num_workers = topology.total_workers();

        let mut senders: Vec<Vec<Option<ByteSender>>> =
            (0..num_workers).map(|_| (0..num_workers).map(|_| None).collect()).collect();
        let mut receivers: Vec<Vec<Option<ByteReceiver>>> =
            (0..num_workers).map(|_| (0..num_workers).map(|_| None).collect()).collect();

        for src in 0..num_workers {
            for dst in 0..num_workers {
                let (sender, receiver) = byte_channel(capacity);
                senders[src][dst] = Some(sender);
                receivers[src][dst] = Some(receiver);
            }
        }

        Self {
            _topology: topology,
            num_workers,
            senders,
            receivers,
            taken: vec![false; num_workers],
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Timestamp + ExchangeData, D: ExchangeData> EdgeMaterializer<T, D>
    for MockNetworkEdgeMaterializer<T, D>
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
            return Err(Error::Custom(format!(
                "source index {src_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }

        let mut pushers: Vec<Box<dyn Push<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for dst in 0..self.num_workers {
            let sender = self.senders[src_idx][dst]
                .take()
                .ok_or_else(|| Error::Custom(format!(
                    "sender [{src_idx}][{dst}] already taken"
                )))?;
            pushers.push(Box::new(SerializingPush::<T, D>::new(sender)));
        }
        Ok(pushers)
    }

    fn materialize_target_worker(
        &mut self,
        dst_idx: usize,
    ) -> Result<Vec<Box<dyn Pull<T, D, ()>>>> {
        if dst_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "target index {dst_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }

        let mut pullers: Vec<Box<dyn Pull<T, D, ()>>> = Vec::with_capacity(self.num_workers);
        for src in 0..self.num_workers {
            let receiver = self.receivers[src][dst_idx]
                .take()
                .ok_or_else(|| Error::Custom(format!(
                    "receiver [{src}][{dst_idx}] already taken"
                )))?;
            pullers.push(Box::new(DeserializingPull::<T, D>::new(receiver)));
        }
        Ok(pullers)
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
    use crate::execute::NodeConfig;

    fn two_node_topology() -> ClusterTopology {
        ClusterTopology::multi_node(vec![
            NodeConfig::new("node-a", 2), // workers 0, 1
            NodeConfig::new("node-b", 2), // workers 2, 3
        ])
        .unwrap()
    }

    // --- Codec round-trip tests ---

    #[test]
    fn encode_decode_data_roundtrip() {
        let time_codec = u64::codec();
        let data_codec = u64::codec();

        let env: Envelope<u64, u64, ()> = Envelope::data(42, vec![10, 20, 30]);
        let mut buf = Vec::new();
        encode_envelope(&time_codec, &data_codec, &env, &mut buf).unwrap();

        let decoded = decode_envelope(&time_codec, &data_codec, &buf).unwrap();
        assert_eq!(decoded.as_data(), Some((&42u64, &vec![10u64, 20, 30])));
    }

    #[test]
    fn encode_decode_watermark_roundtrip() {
        let time_codec = u64::codec();
        let data_codec = u64::codec();

        let env: Envelope<u64, u64, ()> = Envelope::watermark(99);
        let mut buf = Vec::new();
        encode_envelope(&time_codec, &data_codec, &env, &mut buf).unwrap();

        let decoded = decode_envelope(&time_codec, &data_codec, &buf).unwrap();
        match decoded.payload {
            Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 99),
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn encode_decode_error_roundtrip() {
        let time_codec = u64::codec();
        let data_codec = u64::codec();

        let env: Envelope<u64, u64, ()> = Envelope::error("test_op", "test error");
        let mut buf = Vec::new();
        encode_envelope(&time_codec, &data_codec, &env, &mut buf).unwrap();

        let decoded = decode_envelope(&time_codec, &data_codec, &buf).unwrap();
        match decoded.payload {
            Payload::Control(ControlSignal::Error {
                source_operator,
                message,
            }) => {
                assert_eq!(source_operator, "test_op");
                assert_eq!(message, "test error");
            }
            _ => panic!("expected error"),
        }
    }

    // --- SerializingPush / DeserializingPull tests ---

    #[test]
    fn serializing_push_pull_data_roundtrip() {
        let (sender, receiver) = byte_channel(16);
        let mut push = SerializingPush::<u64, u64>::new(sender);
        let mut pull = DeserializingPull::<u64, u64>::new(receiver);

        push.push(Envelope::data(5, vec![100, 200])).unwrap();
        push.flush().unwrap();

        let env = pull.pull().unwrap();
        assert_eq!(env.as_data(), Some((&5u64, &vec![100u64, 200])));
    }

    #[test]
    fn serializing_push_pull_watermark() {
        let (sender, receiver) = byte_channel(16);
        let mut push = SerializingPush::<u64, u64>::new(sender);
        let mut pull = DeserializingPull::<u64, u64>::new(receiver);

        push.push(Envelope::watermark(42)).unwrap();

        let env = pull.pull().unwrap();
        match env.payload {
            Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 42),
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn serializing_push_backpressure() {
        let (sender, _receiver) = byte_channel(2);
        let mut push = SerializingPush::<u64, u64>::new(sender);

        push.push(Envelope::data(1, vec![1])).unwrap();
        push.push(Envelope::data(2, vec![2])).unwrap();
        // Channel full — should get Backpressure.
        let result = push.push(Envelope::data(3, vec![3]));
        assert!(result.is_err());
    }

    #[test]
    fn serializing_push_try_push_returns_envelope() {
        let (sender, _receiver) = byte_channel(1);
        let mut push = SerializingPush::<u64, u64>::new(sender);

        push.push(Envelope::data(1, vec![1])).unwrap();
        // try_push should return the envelope on backpressure.
        let env = Envelope::data(2, vec![2]);
        let result = push.try_push(env);
        assert!(result.is_err());
        let (err, returned) = result.unwrap_err();
        assert!(matches!(err, Error::Backpressure));
        assert_eq!(returned.as_data(), Some((&2u64, &vec![2u64])));
    }

    #[test]
    fn serializing_push_available_capacity() {
        let (sender, _receiver) = byte_channel(4);
        let mut push = SerializingPush::<u64, u64>::new(sender);

        assert_eq!(push.available_capacity(), Some(4));
        push.push(Envelope::data(1, vec![1])).unwrap();
        assert_eq!(push.available_capacity(), Some(3));
    }

    #[test]
    fn deserializing_pull_exhaustion() {
        let (sender, receiver) = byte_channel(16);
        let mut push = SerializingPush::<u64, u64>::new(sender);
        let mut pull = DeserializingPull::<u64, u64>::new(receiver);

        push.push(Envelope::data(1, vec![10])).unwrap();
        push.close();

        // Pull the data.
        assert!(pull.pull().is_some());
        // No more data and sender closed → exhausted.
        assert!(pull.pull().is_none());
        assert!(pull.is_exhausted());
    }

    // --- MockNetworkEdgeMaterializer tests ---

    #[test]
    fn mock_materializer_num_workers() {
        let topo = two_node_topology();
        let mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);
        assert_eq!(mat.num_workers(), 4);
    }

    #[test]
    fn mock_materializer_local_pair() {
        // Workers 0 and 1 are on node-a — still serialized through Codec.
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);

        let (mut push0, _pull0) = mat.materialize_worker(0).unwrap();
        let (_push1, mut pull1) = mat.materialize_worker(1).unwrap();

        // push0[1] → pull1[0]: goes through Codec encode/decode even for same-node.
        push0[1].push(Envelope::data(10, vec![42])).unwrap();
        let env = pull1[0].pull().unwrap();
        assert_eq!(env.as_data(), Some((&10u64, &vec![42u64])));
    }

    #[test]
    fn mock_materializer_remote_pair() {
        // Workers 0 (node-a) and 2 (node-b) — serialized through Codec.
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);

        let (mut push0, _pull0) = mat.materialize_worker(0).unwrap();
        let _ = mat.materialize_worker(1).unwrap(); // need to materialize all
        let (_push2, mut pull2) = mat.materialize_worker(2).unwrap();

        // push0[2] → pull2[0]: data goes through Codec encode/decode.
        push0[2]
            .push(Envelope::data(99, vec![1, 2, 3]))
            .unwrap();
        let env = pull2[0].pull().unwrap();
        assert_eq!(env.as_data(), Some((&99u64, &vec![1u64, 2, 3])));
    }

    #[test]
    fn mock_materializer_remote_watermark() {
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);

        let (mut push0, _) = mat.materialize_worker(0).unwrap();
        let _ = mat.materialize_worker(1).unwrap();
        let (_, mut pull2) = mat.materialize_worker(2).unwrap();

        push0[2].push(Envelope::watermark(50)).unwrap();
        let env = pull2[0].pull().unwrap();
        match env.payload {
            Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 50),
            _ => panic!("expected watermark"),
        }
    }

    #[test]
    fn mock_materializer_mixed_local_and_remote() {
        // Full 4-worker test: workers 0,1 on node-a; workers 2,3 on node-b.
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);

        let (mut push0, mut pull0) = mat.materialize_worker(0).unwrap();
        let (mut push1, mut pull1) = mat.materialize_worker(1).unwrap();
        let (mut push2, mut pull2) = mat.materialize_worker(2).unwrap();
        let (mut push3, mut pull3) = mat.materialize_worker(3).unwrap();

        // Local: worker 0 → worker 1 (same node)
        push0[1].push(Envelope::data(1, vec![10])).unwrap();
        assert_eq!(pull1[0].pull().unwrap().as_data(), Some((&1u64, &vec![10u64])));

        // Remote: worker 0 → worker 2 (cross-node, serialized)
        push0[2].push(Envelope::data(2, vec![20])).unwrap();
        assert_eq!(pull2[0].pull().unwrap().as_data(), Some((&2u64, &vec![20u64])));

        // Remote: worker 3 → worker 0 (cross-node, serialized)
        push3[0].push(Envelope::data(3, vec![30])).unwrap();
        assert_eq!(pull0[3].pull().unwrap().as_data(), Some((&3u64, &vec![30u64])));

        // Local: worker 2 → worker 3 (same node)
        push2[3].push(Envelope::data(4, vec![40])).unwrap();
        assert_eq!(pull3[2].pull().unwrap().as_data(), Some((&4u64, &vec![40u64])));

        // Self-loop: worker 1 → worker 1 (local)
        push1[1].push(Envelope::data(5, vec![50])).unwrap();
        assert_eq!(pull1[1].pull().unwrap().as_data(), Some((&5u64, &vec![50u64])));
    }

    #[test]
    fn mock_materializer_double_take_fails() {
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);
        mat.materialize_worker(0).unwrap();
        assert!(mat.materialize_worker(0).is_err());
    }

    #[test]
    fn mock_materializer_out_of_range_fails() {
        let topo = two_node_topology();
        let mut mat = MockNetworkEdgeMaterializer::<u64, u64>::new(topo, 16);
        assert!(mat.materialize_worker(10).is_err());
    }

    #[test]
    fn mock_materializer_with_string_data() {
        // Proves Codec round-trip works with String (StringCodec).
        let topo = ClusterTopology::multi_node(vec![
            NodeConfig::new("a", 1),
            NodeConfig::new("b", 1),
        ])
        .unwrap();
        let mut mat = MockNetworkEdgeMaterializer::<u64, String>::new(topo, 16);

        let (mut push0, _) = mat.materialize_worker(0).unwrap();
        let (_, mut pull1) = mat.materialize_worker(1).unwrap();

        push0[1]
            .push(Envelope::data(7, vec!["hello".to_string(), "world".to_string()]))
            .unwrap();
        let env = pull1[0].pull().unwrap();
        assert_eq!(
            env.as_data(),
            Some((&7u64, &vec!["hello".to_string(), "world".to_string()]))
        );
    }
}

