//! Remote push endpoint for inter-process data delivery.
//!
//! [`RemotePush`] implements [`PushEndpoint`] for delivering envelopes to
//! remote workers across process boundaries. It serializes the envelope
//! using a codec and enqueues the resulting frame into a bounded channel
//! consumed by the background muxer task.
//!
//! # Backpressure
//!
//! `RemotePush` uses `try_send` on the internal channel. When the channel
//! is full (muxer cannot flush fast enough), it returns [`Error::Backpressure`]
//! rather than blocking the caller's worker thread.

use std::sync::mpsc;
use std::sync::Arc;

use crate::communication::codec::Codec;
use crate::communication::interprocess::ChannelId;
use crate::communication::transport::Frame;
use crate::dataflow::id::DataflowId;
use crate::error::Error;
use crate::providers::transport::PushEndpoint;

/// Configuration for a remote push endpoint.
#[derive(Debug, Clone)]
pub struct RemotePushConfig {
    /// Maximum number of frames buffered before backpressure is applied.
    /// Default: 1024.
    pub buffer_capacity: usize,
}

impl Default for RemotePushConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 1024,
        }
    }
}

/// A serialized frame ready to be sent by the muxer.
///
/// This is the output of `RemotePush::push()` — the envelope has been
/// serialized into bytes and tagged with routing information.
#[derive(Debug, Clone)]
pub struct OutboundFrame {
    /// The dataflow this frame belongs to.
    pub dataflow_id: DataflowId,
    /// The channel within the dataflow.
    pub channel_id: ChannelId,
    /// Serialized payload bytes.
    pub payload: Vec<u8>,
}

impl OutboundFrame {
    /// Convert to a wire [`Frame`].
    pub fn into_frame(self) -> Frame {
        Frame {
            dataflow_id: self.dataflow_id,
            channel_id: self.channel_id,
            payload: self.payload,
        }
    }
}

/// Sender handle for outbound frames (given to RemotePush instances).
///
/// Multiple `RemotePush` endpoints can share a single `FrameSender` when
/// they target the same peer (all frames go through one muxer).
#[derive(Debug, Clone)]
pub struct FrameSender {
    sender: mpsc::SyncSender<OutboundFrame>,
}

impl FrameSender {
    /// Create a new frame sender/receiver pair with the given capacity.
    pub fn channel(capacity: usize) -> (Self, FrameReceiver) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (Self { sender: tx }, FrameReceiver { receiver: rx })
    }

    /// Try to send a frame without blocking.
    ///
    /// Returns `Ok(())` if the frame was enqueued, or `Err(Error::Backpressure)`
    /// if the buffer is full.
    pub fn try_send(&self, frame: OutboundFrame) -> Result<(), Error> {
        self.sender.try_send(frame).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => Error::Backpressure,
            mpsc::TrySendError::Disconnected(_) => Error::ChannelClosed,
        })
    }
}

/// Receiver handle for outbound frames (consumed by the mux background task).
pub struct FrameReceiver {
    receiver: mpsc::Receiver<OutboundFrame>,
}

impl FrameReceiver {
    /// Receive the next outbound frame, blocking until available.
    ///
    /// Returns `None` if all senders have been dropped.
    pub fn recv(&self) -> Option<OutboundFrame> {
        self.receiver.recv().ok()
    }

    /// Try to receive without blocking.
    pub fn try_recv(&self) -> Option<OutboundFrame> {
        self.receiver.try_recv().ok()
    }

    /// Drain all currently available frames into a Vec (non-blocking).
    pub fn drain(&self) -> Vec<OutboundFrame> {
        let mut frames = Vec::new();
        while let Ok(f) = self.receiver.try_recv() {
            frames.push(f);
        }
        frames
    }
}

/// A push endpoint that serializes envelopes and sends them to a remote peer.
///
/// This is the inter-process counterpart of `InMemoryPush`. It:
/// 1. Serializes the envelope using the provided codec
/// 2. Wraps the bytes in an `OutboundFrame` with routing info
/// 3. Enqueues into a bounded channel (non-blocking, backpressure on full)
///
/// The background mux task drains the `FrameReceiver` and writes to the wire.
pub struct RemotePush<T, D, M, C> {
    /// Dataflow ID for frame tagging.
    dataflow_id: DataflowId,
    /// Channel ID for this edge.
    channel_id: ChannelId,
    /// Codec for serializing envelopes.
    codec: Arc<C>,
    /// Sender for outbound frames.
    sender: FrameSender,
    /// Type witnesses.
    _phantom: std::marker::PhantomData<(T, D, M)>,
}

impl<T, D, M, C> RemotePush<T, D, M, C>
where
    C: Codec<(T, Vec<D>)>,
{
    /// Create a new remote push endpoint.
    pub fn new(
        dataflow_id: DataflowId,
        channel_id: ChannelId,
        codec: Arc<C>,
        sender: FrameSender,
    ) -> Self {
        Self {
            dataflow_id,
            channel_id,
            codec,
            sender,
            _phantom: std::marker::PhantomData,
        }
    }
}

use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::progress::timestamp::Timestamp;

impl<T, D, M, C> PushEndpoint<T, D, M> for RemotePush<T, D, M, C>
where
    T: Timestamp + Send + Sync + 'static,
    D: Send + Sync + 'static,
    M: Send + Sync + 'static,
    C: Codec<(T, Vec<D>)> + Send + Sync + 'static,
{
    fn push(&self, envelope: Envelope<T, D, M>) -> Result<(), Error> {
        // Only serialize data payloads; control signals handled separately
        let payload_bytes = match envelope.payload {
            Payload::Data { time, data } => {
                let mut buf = Vec::new();
                self.codec
                    .encode(&(time, data), &mut buf)
                    .map_err(|e| Error::codec(e))?;
                buf
            }
            Payload::Control(_) => {
                // Control signals are not sent over the wire via RemotePush;
                // they are handled by the progress exchange layer.
                return Ok(());
            }
        };

        let frame = OutboundFrame {
            dataflow_id: self.dataflow_id,
            channel_id: self.channel_id,
            payload: payload_bytes,
        };

        self.sender.try_send(frame)
    }

    fn flush(&self) -> Result<(), Error> {
        // Flushing is handled by the background mux task
        Ok(())
    }

    fn close(&self) -> Result<(), Error> {
        // Closing is handled by dropping the sender
        Ok(())
    }
}

impl<T, D, M, C> std::fmt::Debug for RemotePush<T, D, M, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemotePush")
            .field("dataflow_id", &self.dataflow_id)
            .field("channel_id", &self.channel_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::communication::codec::CodecError;
    use crate::dataflow::channels::envelope::{Envelope, Payload};

    /// A simple test codec that encodes (u64, Vec<u32>) as timestamp + data.
    #[derive(Clone)]
    struct TestCodec;

    impl Codec<(u64, Vec<u32>)> for TestCodec {
        fn encode(
            &self,
            value: &(u64, Vec<u32>),
            buf: &mut Vec<u8>,
        ) -> Result<(), CodecError> {
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

    #[test]
    fn remote_push_sends_frame() {
        let (sender, receiver) = FrameSender::channel(16);
        let codec = Arc::new(TestCodec);
        let dataflow_id = DataflowId::from_bytes([1u8; 16]);
        let channel_id = 42;

        let push = RemotePush::<u64, u32, (), TestCodec>::new(
            dataflow_id,
            channel_id,
            codec,
            sender,
        );

        let envelope = Envelope {
            payload: Payload::Data {
                time: 10u64,
                data: vec![12345u32],
            },
            metadata: (),
        };

        push.push(envelope).unwrap();

        let frame = receiver.recv().unwrap();
        assert_eq!(frame.dataflow_id, dataflow_id);
        assert_eq!(frame.channel_id, 42);
        // Decode and verify
        let mut expected = Vec::new();
        expected.extend_from_slice(&10u64.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes()); // count=1
        expected.extend_from_slice(&12345u32.to_le_bytes());
        assert_eq!(frame.payload, expected);
    }

    #[test]
    fn remote_push_backpressure_on_full() {
        let (sender, _receiver) = FrameSender::channel(2);
        let codec = Arc::new(TestCodec);
        let dataflow_id = DataflowId::from_bytes([1u8; 16]);

        let push = RemotePush::<u64, u32, (), TestCodec>::new(
            dataflow_id,
            1,
            codec,
            sender,
        );

        let envelope = Envelope {
            payload: Payload::Data {
                time: 1u64,
                data: vec![1u32],
            },
            metadata: (),
        };

        // Fill the buffer
        push.push(envelope.clone()).unwrap();
        push.push(envelope.clone()).unwrap();

        // Third should get backpressure
        let result = push.push(envelope);
        assert!(matches!(result, Err(Error::Backpressure)));
    }

    #[test]
    fn remote_push_channel_closed() {
        let (sender, receiver) = FrameSender::channel(16);
        let codec = Arc::new(TestCodec);
        let dataflow_id = DataflowId::from_bytes([1u8; 16]);

        let push = RemotePush::<u64, u32, (), TestCodec>::new(
            dataflow_id,
            1,
            codec,
            sender,
        );

        // Drop receiver to simulate mux shutdown
        drop(receiver);

        let envelope = Envelope {
            payload: Payload::Data {
                time: 1u64,
                data: vec![1u32],
            },
            metadata: (),
        };
        let result = push.push(envelope);
        assert!(matches!(result, Err(Error::ChannelClosed)));
    }

    #[test]
    fn remote_push_control_signal_skipped() {
        use crate::dataflow::channels::envelope::ControlSignal;

        let (sender, receiver) = FrameSender::channel(16);
        let codec = Arc::new(TestCodec);
        let dataflow_id = DataflowId::from_bytes([1u8; 16]);

        let push = RemotePush::<u64, u32, (), TestCodec>::new(
            dataflow_id,
            1,
            codec,
            sender,
        );

        let envelope = Envelope {
            payload: Payload::Control(ControlSignal::Watermark(5u64)),
            metadata: (),
        };

        // Control signals are silently skipped (not sent over wire)
        push.push(envelope).unwrap();
        assert!(receiver.try_recv().is_none());
    }

    #[test]
    fn frame_sender_channel_capacity() {
        let (sender, receiver) = FrameSender::channel(3);

        for i in 0..3 {
            sender
                .try_send(OutboundFrame {
                    dataflow_id: DataflowId::from_bytes([1u8; 16]),
                    channel_id: i,
                    payload: vec![],
                })
                .unwrap();
        }

        // Full
        let result = sender.try_send(OutboundFrame {
            dataflow_id: DataflowId::from_bytes([1u8; 16]),
            channel_id: 99,
            payload: vec![],
        });
        assert!(matches!(result, Err(Error::Backpressure)));

        // Drain
        let frames = receiver.drain();
        assert_eq!(frames.len(), 3);
    }

    #[test]
    fn outbound_frame_into_wire_frame() {
        let frame = OutboundFrame {
            dataflow_id: DataflowId::from_bytes([100u8; 16]),
            channel_id: 42,
            payload: vec![1, 2, 3],
        };
        let wire = frame.into_frame();
        assert_eq!(wire.dataflow_id, DataflowId::from_bytes([100u8; 16]));
        assert_eq!(wire.channel_id, 42);
        assert_eq!(wire.payload, vec![1, 2, 3]);
    }

    #[test]
    fn frame_receiver_try_recv_empty() {
        let (_sender, receiver) = FrameSender::channel(8);
        assert!(receiver.try_recv().is_none());
    }

    #[test]
    fn remote_push_debug() {
        let (sender, _receiver) = FrameSender::channel(8);
        let codec = Arc::new(TestCodec);
        let push = RemotePush::<u64, u32, (), TestCodec>::new(
            DataflowId::from_bytes([1u8; 16]),
            7,
            codec,
            sender,
        );
        let dbg = format!("{push:?}");
        assert!(dbg.contains("RemotePush"));
        assert!(dbg.contains("channel_id: 7"));
    }
}
