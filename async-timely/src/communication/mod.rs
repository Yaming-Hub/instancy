//! Communication infrastructure for async-timely.
//!
//! This module provides channel allocation for intra-process communication
//! between operators, codec infrastructure for inter-process serialization,
//! connection pooling for managing peer-to-peer connections, multiplexed
//! framed transport for wire communication, and inter-process routing/encoding.

pub mod allocator;
pub mod codec;
pub mod connection;
pub mod interprocess;
pub mod transport;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
pub use codec::{Codec, CodecError, Data, ExchangeData, FixedSizeCodec, RawBytesCodec, StringCodec, MAX_MESSAGE_SIZE};
pub use connection::{ConnectionManager, ConnectionPool, ConnectionRequest, PeerId, PoolConfig, PoolError, PoolGuard, PoolStats};
pub use interprocess::{
    ChannelId, DataBatch, ProgressMessage, RemoteEndpoint, RoutingTable, PROGRESS_CHANNEL_ID,
    decode_data_batch, decode_progress, encode_data_batch, encode_progress,
};
pub use transport::{Frame, TransportError};

#[cfg(feature = "transport")]
pub use transport::{
    ChannelReceiver, DemuxConfig, Demuxer, FramedReader, FramedWriter, MuxConfig, Muxer,
    MuxerSender,
};
