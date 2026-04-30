//! Communication infrastructure for async-timely.
//!
//! This module provides channel allocation for intra-process communication
//! between operators, codec infrastructure for inter-process serialization,
//! connection pooling for managing peer-to-peer connections, and multiplexed
//! framed transport for wire communication.

pub mod allocator;
pub mod codec;
pub mod connection;
pub mod transport;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
pub use codec::{Codec, CodecError, Data, ExchangeData, FixedSizeCodec, RawBytesCodec, StringCodec, MAX_MESSAGE_SIZE};
pub use connection::{ConnectionManager, ConnectionPool, ConnectionRequest, PeerId, PoolConfig, PoolError, PoolGuard, PoolStats};
pub use transport::{Frame, TransportError};

#[cfg(feature = "transport")]
pub use transport::{
    ChannelReceiver, DemuxConfig, Demuxer, FramedReader, FramedWriter, MuxConfig, Muxer,
    MuxerSender,
};
