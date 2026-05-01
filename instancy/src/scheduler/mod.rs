//! Task scheduling infrastructure for operator activation dispatch.
//!
//! This module contains:
//! - [`TaskScheduler`] — manages FIFO per-worker dispatch with region concurrency limits.
//! - [`policy`] — pluggable scheduling policies (FIFO, Priority, PriorityWithAging).
//! - [`batching`] — time-bounded message batching to reduce scheduling overhead.

pub mod batching;
pub mod policy;
mod task_scheduler;

pub use task_scheduler::*;
