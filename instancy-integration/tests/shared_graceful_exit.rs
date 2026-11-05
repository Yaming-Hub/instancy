//! A Shared-transport node that exits gracefully after its dataflow ended
//! `Ok` while its peers are still running it: they must still end `Ok` with
//! exact output.
//!
//! The relay holds the peers back by stalling the connections between them,
//! and the coordinator stops the node with `Shutdown`, which is a requested
//! exit, not a crash: the node drops its managers, waits for its runtime to
//! shut down, replies and exits.

mod support;

use instancy_integration::coordinator::TestCoordinator;
use support::scenarios::{self, fail, finish, CONNECTIONS, WORKERS};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_exit_after_ok_never_fails_the_peer() {
    let mut coord = TestCoordinator::start_shared(&["a", "b", "c"], WORKERS, CONNECTIONS).await;
    scenarios::graceful_exit(&mut coord)
        .await
        .unwrap_or_else(|e| fail(e));
    finish(coord).await;
}
