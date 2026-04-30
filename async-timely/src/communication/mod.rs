//! Communication infrastructure for async-timely.
//!
//! This module provides channel allocation for intra-process communication
//! between operators, and codec infrastructure for inter-process serialization.

pub mod allocator;
pub mod codec;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
pub use codec::{Codec, CodecError, Data, ExchangeData, FixedSizeCodec, RawBytesCodec, StringCodec, MAX_MESSAGE_SIZE};
