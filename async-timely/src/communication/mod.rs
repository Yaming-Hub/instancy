//! Communication infrastructure for async-timely.
//!
//! This module provides channel allocation for intra-process communication
//! between operators, codec infrastructure for inter-process serialization,
//! and connection pooling for managing peer-to-peer connections.

pub mod allocator;
pub mod codec;
pub mod connection;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
pub use codec::{Codec, CodecError, Data, ExchangeData, FixedSizeCodec, RawBytesCodec, StringCodec, MAX_MESSAGE_SIZE};
pub use connection::{ConnectionManager, ConnectionPool, ConnectionRequest, PeerId, PoolConfig, PoolError, PoolGuard, PoolStats};
