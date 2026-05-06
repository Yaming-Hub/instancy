//! Communication infrastructure for instancy.
//!
//! This module provides channel allocation for intra-process communication
//! between operators, codec infrastructure for inter-process serialization,
//! connection pooling for managing peer-to-peer connections, multiplexed
//! framed transport for wire communication, and inter-process routing/encoding.

pub mod allocator;
pub mod codec;
pub mod connection;
pub mod control_protocol;
pub mod interprocess;
pub mod probing;
pub mod progress_exchange;
pub mod remote_push;
pub mod sequencing;
pub mod session;
pub mod shared_pool;
#[cfg(feature = "transport")]
pub mod shared_transport;
pub mod transport;
pub mod transport_session;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
pub use codec::{
    Codec, CodecError, Data, ExchangeData, FixedSizeCodec, MAX_MESSAGE_SIZE, RawBytesCodec,
    StringCodec,
};
pub use connection::{
    ConnectionManager, ConnectionPool, ConnectionRequest, PeerId, PoolConfig, PoolError, PoolGuard,
    PoolStats,
};
pub use interprocess::{
    ChannelId, DataBatch, PROGRESS_CHANNEL_ID, ProgressMessage, RemoteEndpoint, RoutingTable,
    decode_data_batch, decode_progress, encode_data_batch, encode_progress,
};
pub use progress_exchange::{PeerProgressSender, ProgressExchange, ProgressExchangeConfig};
pub use probing::{
    PROBE_MESSAGE_SIZE, PROBE_REPLY_TYPE, PROBE_REQUEST_TYPE, ProbeCounter, ProbeKind,
    ProbeMessage, ProbeTimestamp, ScalingDriver, ScalingEvent,
};
pub use remote_push::{FrameReceiver, FrameSender, OutboundFrame, RemotePush, RemotePushConfig};
pub use sequencing::{
    BufferOverflow, InsertResult, ReorderBuffer, ReorderError, SequenceCounter, SequencedFrame,
    SEQUENCED_HEADER_SIZE, decode_sequenced_header, encode_sequenced_header,
};
pub use session::{ChannelInfo, DataflowSession, DataflowSessionBuilder, SharedSession};
pub use shared_pool::{
    ConnectionMetrics, ConnectionMode, ConnectionSnapshot, PeerPool, RttTracker, ScalingDecision,
    SharedConnectionConfig,
};
pub use transport::{Frame, TransportError};

#[cfg(feature = "transport")]
pub use transport::{
    ChannelReceiver, DemuxConfig, Demuxer, FramedReader, FramedWriter, MuxConfig, Muxer,
    MuxerSender,
};

#[cfg(feature = "transport")]
pub use transport_session::{
    CONTROL_CHANNEL_ID, ChannelRegistration, PROGRESS_CHANNEL_BASE, PeerConnection,
    TransportSession,
};

#[cfg(feature = "transport")]
pub use shared_transport::{
    ConnectionFactory, DataframeSender, PROBE_CHANNEL_ID, SharedPeerManager,
    SharedTransportSession,
};
