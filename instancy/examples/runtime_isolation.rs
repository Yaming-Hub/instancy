//! # Runtime Isolation Example
//!
//! Demonstrates creating multiple isolated `RuntimeHandle` instances that
//! coexist in the same process without shared state.
//!
//! ```bash
//! cargo run --example runtime_isolation
//! ```

use instancy::runtime::{RuntimeConfig, RuntimeHandle};
use instancy::scheduler::policy::FifoPolicy;

fn main() {
    // Create two independent runtimes with different configurations.
    // Each runtime has its own worker pool, scheduling policy, and
    // cancellation scope — fully isolated from each other.
    let rt_fast = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 4,
        schedule_policy: Box::new(FifoPolicy),
        name: "fast-pipeline".to_string(),
    })
    .expect("failed to create fast runtime");

    let rt_batch = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        schedule_policy: Box::new(FifoPolicy),
        name: "batch-pipeline".to_string(),
    })
    .expect("failed to create batch runtime");

    println!("Created runtime '{}' (4 threads)", rt_fast.name());
    println!("Created runtime '{}' (2 threads)", rt_batch.name());

    // Shutting down one runtime doesn't affect the other.
    rt_fast.shutdown();
    println!("\nShut down '{}':", rt_fast.name());
    println!("  {} is_shutdown: {}", rt_fast.name(), rt_fast.is_shutdown());
    println!("  {} is_shutdown: {}", rt_batch.name(), rt_batch.is_shutdown());

    // The batch runtime continues to operate independently.
    assert!(!rt_batch.is_shutdown());
    println!("\nIndependent shutdown verified: cancelling one runtime's token leaves others unaffected.");
}
