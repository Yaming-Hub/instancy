//! Task scheduling infrastructure for operator activation dispatch.
//!
//! This module contains:
//! - [`TaskScheduler`] — manages FIFO per-worker dispatch with region concurrency limits.
//! - [`batching`] — time-bounded message batching to reduce scheduling overhead.

pub mod batching;
mod task_scheduler;

pub use task_scheduler::*;
