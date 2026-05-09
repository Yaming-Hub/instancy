//! Task scheduling infrastructure for operator activation dispatch.
//!
//! This module contains:
//! - [`TaskScheduler`] — manages FIFO per-worker dispatch with stage concurrency limits.
//! - [`policy`] — pluggable scheduling policies (FIFO, Priority, PriorityWithAging).
//! - [`batching`] — time-bounded message batching to reduce scheduling overhead.

#[allow(dead_code)]
pub(crate) mod batching;
pub mod policy;
pub(crate) mod task_scheduler;

pub use task_scheduler::*;
