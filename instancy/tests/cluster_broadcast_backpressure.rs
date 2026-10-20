//! Broadcast under network backpressure on loopback-TCP clusters.
//!
//! A broadcast hands every target worker its own copy of each batch. When a
//! later target's peer queue is full, earlier targets already have theirs, and
//! the operator's retry must only go to the targets still owed the batch.
//!
//! Every pipeline keeps its inputs open until every copy has arrived:
//! broadcast does not yet account its records as in flight, so completion
//! relies on close sentinels, which a full queue can drop, and in Shared mode
//! the node that finishes first can drop frames its peers still need (both
//! tracked separately).

#![cfg(feature = "transport")]

mod support;

use instancy::{DataflowBuilder, Result};
use support::cluster::{self, ClusterSpec, Mode};

fn broadcast_pipeline(b: &mut DataflowBuilder<u64>) -> Result<()> {
    b.input::<u64>("in")?
        .broadcast("bcast")
        .map("post", |_t, x| x)
        .output("out")?;
    Ok(())
}

async fn broadcast_exactly_once(
    workers: &[usize],
    mode: Mode,
    capacity: usize,
    data: cluster::Batches,
) {
    let mut spec = ClusterSpec::new(workers, mode, capacity);
    let expected = cluster::expected_records(&data);
    spec.hold_inputs_until = Some(expected.len() * spec.total_workers());
    let out = cluster::run(&spec, broadcast_pipeline, vec![(0, data)]).await;
    out.assert_each_worker_exactly(
        &expected,
        &format!("{workers:?} {mode:?} capacity {capacity}"),
    );
}

/// Two local targets and one remote: the remote copy pushes back after the
/// local copies were accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_retry_does_not_duplicate_local_copies() {
    broadcast_exactly_once(
        &[2, 1],
        Mode::Dedicated,
        1024,
        cluster::numbered_batches(3000, 4),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn broadcast_with_tiny_peer_queue_delivers_one_copy_per_worker() {
    broadcast_exactly_once(
        &[1, 1],
        Mode::Dedicated,
        2,
        cluster::numbered_batches(2000, 16),
    )
    .await;
}

/// Several remote copies per batch, one free slot per retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "completion needs close sentinels that a single-slot queue drops (tracked separately)"]
async fn broadcast_with_single_slot_peer_queue_makes_progress() {
    broadcast_exactly_once(
        &[1, 3],
        Mode::Dedicated,
        1,
        cluster::numbered_batches(50, 4),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn broadcast_to_many_workers_on_three_nodes() {
    for mode in [Mode::Dedicated, Mode::Shared] {
        broadcast_exactly_once(&[2, 1, 2], mode, 8, cluster::numbered_batches(1500, 6)).await;
    }
}

/// Shared transport: bursts fill the 1024-frame per-peer payload queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_broadcast_burst_delivers_one_copy_per_worker() {
    broadcast_exactly_once(
        &[1, 2],
        Mode::Shared,
        1024,
        cluster::numbered_batches(6000, 4),
    )
    .await;
}
