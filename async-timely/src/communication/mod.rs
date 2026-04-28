//! Communication infrastructure for async-timely.
//!
//! This module provides channel allocation for intra-process communication
//! between operators. Inter-process communication (networking) is handled
//! in later PRs.

pub mod allocator;

pub use allocator::{AllocatorConfig, ChannelAllocator, DEFAULT_CHANNEL_CAPACITY};
