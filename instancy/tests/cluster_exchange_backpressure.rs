//! Exchange under network backpressure on loopback-TCP clusters.
//!
//! An exchange hands each target worker its own sub-batch. When a later
//! target's peer queue is full, earlier targets have already accepted theirs.
//! These tests check that the operator's retry neither re-delivers those
//! sub-batches nor livelocks, in Dedicated and Shared transport modes.
//!
//! Workarounds for issues tracked separately keep these tests about network
//! backpressure:
//! - A `map` precedes each exchange: only some producers account their
//!   records as in flight (the `map` does, which also exercises the
//!   exchange's in-flight accounting), and without that accounting completion
//!   relies on close sentinels, which a full peer queue can drop.
//! - Inputs stay open until every record has arrived: once the inputs close,
//!   a worker can declare completion early while records are still in flight
//!   or held by a blocked operator (audit C1/H1, tracked separately). The
//!   first-finisher teardown race (audit C6) is fixed, so every node must
//!   join.

#![cfg(feature = "transport")]

mod support;

use std::time::Duration;

use instancy::{DataflowBuilder, Result};
use support::cluster::{self, ClusterSpec, Mode};

/// `input -> map -> exchange(route) -> map -> output`.
fn exchange_pipeline(
    route: fn(&u64) -> u64,
) -> impl Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static {
    move |b: &mut DataflowBuilder<u64>| {
        b.input::<u64>("in")?
            .map("pre", |_t, x| x)
            .exchange_by_hash("route", route)
            .map("post", |_t, x| x)
            .output("out")?;
        Ok(())
    }
}

fn by_value(x: &u64) -> u64 {
    *x
}

/// Runs `build` with `batches` fed into worker 0 and checks that every record
/// arrives exactly once.
async fn exchange_exactly_once<F>(mut spec: ClusterSpec, build: F, batches: cluster::Batches)
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone + Send + 'static,
{
    let expected = cluster::expected_records(&batches);
    spec.hold_inputs_until = Some(expected.len());
    let what = format!(
        "{:?} {:?} capacity {}",
        spec.workers, spec.mode, spec.capacity
    );
    let out = cluster::run(&spec, build, vec![(0, batches)]).await;
    out.assert_exactly(&expected, &what);
}

/// One worker per node, tiny per-peer queue, every batch split between nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_with_tiny_peer_queue_delivers_each_record_once() {
    exchange_exactly_once(
        ClusterSpec::new(&[1, 1], Mode::Dedicated, 4),
        exchange_pipeline(by_value),
        cluster::numbered_batches(200, 8),
    )
    .await;
}

/// The local sub-batch is accepted, the remote one pushes back: the retry
/// must not deliver the local records again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_retry_does_not_redeliver_local_records() {
    exchange_exactly_once(
        ClusterSpec::new(&[1, 1], Mode::Dedicated, 2),
        exchange_pipeline(by_value),
        cluster::numbered_batches(2000, 16),
    )
    .await;
}

/// Same as above over the Shared transport, whose per-peer payload queue
/// (1024 frames) fills under a sustained burst.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_exchange_retry_does_not_redeliver_local_records() {
    exchange_exactly_once(
        ClusterSpec::new(&[1, 1], Mode::Shared, 1024),
        exchange_pipeline(by_value),
        cluster::numbered_batches(40_000, 16),
    )
    .await;
}

/// Two workers per node: each batch has two remote targets sharing one peer
/// queue, so a retry that re-sent earlier sub-batches would grab the freed
/// slot again and again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_with_two_remote_targets_per_peer_delivers_each_record_once() {
    for mode in [Mode::Dedicated, Mode::Shared] {
        exchange_exactly_once(
            ClusterSpec::new(&[2, 2], mode, 2),
            exchange_pipeline(by_value),
            cluster::numbered_batches(2000, 16),
        )
        .await;
    }
}

/// A single-slot peer queue with two remote targets per batch: each retry
/// gets exactly one freed slot and must use it to make progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_with_single_slot_peer_queue_makes_progress() {
    exchange_exactly_once(
        ClusterSpec::new(&[2, 2], Mode::Dedicated, 1),
        exchange_pipeline(by_value),
        cluster::numbered_batches(20, 8),
    )
    .await;
}

/// Node-0 has two workers, node-1 has `remote` workers; every batch holds two
/// records for every worker.
async fn mixed_targets(capacity: usize, remote: usize, batches: u64) {
    let spec = ClusterSpec::new(&[2, remote], Mode::Dedicated, capacity);
    let per_batch = 2 * spec.total_workers() as u64;
    exchange_exactly_once(
        spec,
        exchange_pipeline(by_value),
        cluster::numbered_batches(batches, per_batch),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_to_four_remote_workers_with_tiny_peer_queue() {
    mixed_targets(4, 4, 300).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_with_default_peer_queue_delivers_each_record_once() {
    mixed_targets(1024, 1, 2000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_to_one_remote_worker_under_sustained_backpressure() {
    mixed_targets(64, 1, 20_000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exchange_to_two_remote_workers_under_sustained_backpressure() {
    mixed_targets(64, 2, 20_000).await;
}

/// All records local, all remote, or split: every routing shape must be
/// exactly once under a tiny peer queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_routing_variants_with_tiny_peer_queue() {
    let routes: [fn(&u64) -> u64; 3] = [|_| 0, |_| 1, by_value];
    for route in routes {
        exchange_exactly_once(
            ClusterSpec::new(&[1, 1], Mode::Dedicated, 2),
            exchange_pipeline(route),
            cluster::numbered_batches(1000, 16),
        )
        .await;
    }
}

/// Bursts big enough to fill the default-sized peer queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_burst_fills_default_peer_queue() {
    for mode in [Mode::Dedicated, Mode::Shared] {
        exchange_exactly_once(
            ClusterSpec::new(&[2, 2], mode, 1024),
            exchange_pipeline(by_value),
            cluster::numbered_batches(20_000, 8),
        )
        .await;
    }
}

/// `rebalance` routes round-robin, so its partition differs on every call: a
/// retry that re-partitioned would send records to the wrong (or a second)
/// target.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rebalance_under_backpressure_delivers_each_record_once() {
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("in")?
            .map("pre", |_t, x| x)
            .rebalance("spread")
            .map("post", |_t, x| x)
            .output("out")?;
        Ok(())
    };
    for mode in [Mode::Dedicated, Mode::Shared] {
        exchange_exactly_once(
            ClusterSpec::new(&[2, 2], mode, 2),
            build,
            cluster::numbered_batches(3000, 7),
        )
        .await;
    }
}

/// Multi-megabyte records: few frames, each one large.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_of_large_records_delivers_each_record_once() {
    const LEN: usize = 3 * 1024 * 1024;
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("in")?
            .map("inflate", |_t, id| {
                let mut v = vec![(id % 7) as u8 + 1; LEN + id as usize];
                v[..8].copy_from_slice(&id.to_le_bytes());
                v
            })
            .exchange("route", |v: &Vec<u8>| u64::from(v[0]))
            .map("digest", |_t, v: Vec<u8>| {
                let id = u64::from_le_bytes(v[..8].try_into().unwrap());
                let fill_ok = v[8..].iter().all(|b| *b == (id % 7) as u8 + 1);
                assert!(
                    fill_ok && v.len() == LEN + id as usize,
                    "record {id} corrupted"
                );
                id
            })
            .output("out")?;
        Ok(())
    };
    for mode in [Mode::Dedicated, Mode::Shared] {
        let mut spec = ClusterSpec::new(&[2, 2], mode, 16);
        let inputs: cluster::WorkerInputs = (0..4)
            .map(|w| (w, (0..6u64).map(|k| (0, vec![w as u64 * 10 + k])).collect()))
            .collect();
        let batches: Vec<_> = inputs.iter().flat_map(|(_, b)| b.clone()).collect();
        let expected = cluster::expected_records(&batches);
        spec.hold_inputs_until = Some(expected.len());
        let out = cluster::run(&spec, build, inputs).await;
        out.assert_exactly(&expected, &format!("{mode:?}"));
    }
}

/// Every worker of a three-node cluster feeds data at once, at several
/// timestamps, through two chained exchanges. Every record reaches, exactly
/// once, the worker its last exchange routes it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_chained_exchanges_from_every_worker() {
    let build = |b: &mut DataflowBuilder<u64>| -> Result<()> {
        b.input::<u64>("in")?
            .map("pre", |_t, x| x)
            .exchange_by_hash("first", |x: &u64| *x)
            .map("between", |_t, x| x)
            .exchange_by_hash("second", |x: &u64| x / 3)
            .map("post", |_t, x| x)
            .output("out")?;
        Ok(())
    };
    for mode in [Mode::Dedicated, Mode::Shared] {
        let mut spec = ClusterSpec::new(&[2, 1, 2], mode, 2);
        spec.timeout = Duration::from_secs(90);
        let inputs: cluster::WorkerInputs = (0..spec.total_workers())
            .map(|w| {
                let base = w as u64 * 1_000_000;
                let batches = (0..400u64)
                    .map(|b| (b % 3, (base + b * 10..base + b * 10 + 10).collect()))
                    .collect();
                (w, batches)
            })
            .collect();
        let batches: Vec<_> = inputs.iter().flat_map(|(_, b)| b.clone()).collect();
        let expected = cluster::expected_records(&batches);
        spec.hold_inputs_until = Some(expected.len());
        let out = cluster::run(&spec, build, inputs).await;
        out.assert_exactly(&expected, &format!("{mode:?}"));
        let workers = spec.total_workers() as u64;
        for (worker, records) in out.outputs.iter().enumerate() {
            let misrouted: Vec<_> = records
                .iter()
                .filter(|(_, x)| (x / 3) % workers != worker as u64)
                .take(5)
                .collect();
            assert!(
                misrouted.is_empty(),
                "{mode:?}: worker {worker} received records routed to another worker, \
                 e.g. {misrouted:?}"
            );
        }
    }
}
