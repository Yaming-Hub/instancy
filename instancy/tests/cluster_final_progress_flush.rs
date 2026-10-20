//! Regression tests: a node's final progress deltas survive congestion at the
//! moment it completes.
//!
//! Before the clean shutdown handshake, node B's completion fired the bridge
//! cancellation token while its last progress delta (its input-capability
//! release) was still queued behind a congested link; the send bridge exited
//! without draining and node A waited forever.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::HashMap;
use std::time::Duration;

use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};

use support::{Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node};

/// `input -> map -> exchange(value % 2) -> count per epoch (notify) -> output`.
fn build(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u64>("data")?
        .map("id", |_t, x| x)
        .exchange_by_hash("route", |x: &u64| *x)
        .unary_notify("count", {
            let mut stash: HashMap<u64, u64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((t, data)) = input.next() {
                    *stash.entry(t).or_default() += data.len() as u64;
                    ctx.notify_at(t);
                }
                while let Some(t) = ctx.next_notification() {
                    if let Some(c) = stash.remove(&t) {
                        output.push_vec(t, vec![c]);
                    }
                }
                Ok(())
            }
        })
        .output("out")?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Congestion {
    None,
    /// B→A is paused while B finishes and cleared before B is joined.
    ClearedBeforeJoin,
    /// B→A stays paused until after B's completion has been checked.
    HeldThroughCompletion,
}

async fn run(congestion: Congestion) {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    // Tiny in-flight budget and payload capacity 2: a few frames back up B's
    // per-peer queue quickly once B→A is paused.
    let mut mesh = Mesh::new(&["node-a", "node-b"], 64, |_, _| Duration::ZERO);
    let b_to_a = mesh.valve("node-b", "node-a");
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        id,
        mesh.dedicated("node-a", 2),
        1,
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        id,
        mesh.dedicated("node-b", 2),
        1,
        build,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let out_a = collect(ca.take_output::<u64>(0, "out").unwrap());
    let _out_b = collect(cb.take_output::<u64>(0, "out").unwrap());
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();

    // A: one record at epoch 1000 routed to A itself, then close.
    sa.send(1000, vec![0]).unwrap();
    drop(sa);
    tokio::time::sleep(Duration::from_millis(300)).await;

    if congestion != Congestion::None {
        b_to_a.pause();
    }

    // B: local-only work over many epochs (many progress frames to A), then
    // close; B's capability release is its last progress delta.
    tokio::task::spawn_blocking(move || {
        for e in 1..=30u64 {
            sb.send(e, vec![1]).unwrap();
            sb.advance_to(e + 1).unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
    })
    .await
    .unwrap();

    if congestion == Congestion::ClearedBeforeJoin {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        b_to_a.resume();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let jb = join_with_timeout(cb, Duration::from_secs(15));
    if congestion == Congestion::HeldThroughCompletion {
        // B's workers are done, but A cannot have its final delta yet: B's
        // completion must wait instead of tearing the link down.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            !jb.is_finished(),
            "node B completed while its final progress was undeliverable"
        );
        b_to_a.resume();
    }
    let rb = jb.await.unwrap();
    let ra = join_with_timeout(ca, Duration::from_secs(15))
        .await
        .unwrap();
    let data = out_a.join().unwrap();
    drop_runtimes(vec![rt_a, rt_b]).await;
    assert!(matches!(rb, Some(Ok(()))), "node B: {}", fmt_join(&rb));
    assert!(
        matches!(ra, Some(Ok(()))),
        "node A (needs B's final delta): {}",
        fmt_join(&ra)
    );
    assert_eq!(data, vec![(1000, 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_progress_delta_survives_congestion_at_peer_completion() {
    run(Congestion::HeldThroughCompletion).await;
}

/// Control: no congestion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_progress_flush_without_congestion_control() {
    run(Congestion::None).await;
}

/// Control: congestion clears before the peer is joined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn congestion_cleared_before_peer_join_control() {
    run(Congestion::ClearedBeforeJoin).await;
}
