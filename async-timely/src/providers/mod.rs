//! Provider traits and implementations for the logical/physical separation.
//!
//! The provider system decouples logical computation from physical resources:
//! - [`TransportProvider`]: resolves logical targets to physical delivery mechanisms
//! - [`ExecutionProvider`]: maps logical worker tasks to physical threads
//!
//! Built-in implementations:
//! - [`LocalTransport`]: single-process in-memory delivery
//! - [`WorkerPoolExecution`]: uses the custom worker thread pool
//! - [`InlineExecution`]: testing — runs everything on the calling thread

pub mod execution;
pub mod transport;

pub use execution::{ExecutionProvider, InlineExecution, WorkerPoolExecution};
pub use transport::{LocalTransport, LogicalTarget, TransportProvider};
