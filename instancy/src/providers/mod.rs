//! Provider traits and implementations for the logical/physical separation.
//!
//! The provider system decouples logical computation from physical resources:
//! - [`TransportProvider`]: resolves logical targets to physical delivery mechanisms
//! - [`ExecutionProvider`]: maps logical worker tasks to physical threads
//!
//! Built-in implementations:
//! - [`LocalTransport`]: single-process in-memory delivery
//! - [`WorkerPoolExecution`]: submits tasks to a worker thread pool
//! - [`InlineExecution`]: testing — runs everything on the calling thread
//!
//! [`RuntimeHandle`](crate::RuntimeHandle) does not use these providers: it
//! schedules and connects its dataflows itself.

pub mod execution;
pub mod transport;

pub use execution::{ExecutionProvider, InlineExecution, WorkerPoolExecution};
pub use transport::{
    InMemoryClusterTransport, InMemoryPush, LocalTransport, LogicalTarget, PushEndpoint,
    TransportProvider,
};
