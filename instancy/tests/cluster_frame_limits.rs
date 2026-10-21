//! Frame size limits on cluster data channels (audit H8, M4).
//!
//! A batch the encoder accepts must arrive: the decoder used to reject any
//! batch over 10M records, silently dropping it. A batch too large for one
//! transport frame must fail the sending dataflow with a clear error, and the
//! failure must reach every peer, instead of killing the connection.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

#[path = "support/faults.rs"]
mod faults;

use std::time::Duration;

use faults::FaultMesh;
use instancy::communication::ClusterSpawnTransport;
use instancy::{
    CancellationReason, ClusterTopology, DataflowBuilder, DataflowId, Error, NodeConfig, Result,
};
use support::{drop_runtimes, fmt_join, join_with_timeout, spawn_node};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    Shared,
}

fn topology() -> ClusterTopology {
    ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap()
}

/// Every record goes to node-b's worker (global worker 1), in one envelope
/// per input batch; node-b counts what it receives.
fn bytes_to_node_b(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<u8>("data")?
        // A unary operator before the exchange records the in-flight count
        // (audit C2: an input feeding an exchange directly does not).
        .map("id", |_t, x| x)
        .exchange_by_hash("to_b", |_: &u8| 1)
        .map("one", |_t, _x| 1u64)
        .output("out")?;
    Ok(())
}

fn blobs_to_node_b(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<Vec<u8>>("data")?
        .map("id", |_t, x| x)
        .exchange_by_hash("to_b", |_: &Vec<u8>| 1)
        .map("len", |_t, blob: Vec<u8>| blob.len() as u64)
        .output("out")?;
    Ok(())
}

/// Whether `error` (or an error it wraps, e.g. an operator error) is
/// `FrameTooLarge`.
fn is_frame_too_large(error: &Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(e) = current {
        if matches!(
            e.downcast_ref::<Error>(),
            Some(Error::Communication(
                instancy::CommunicationError::FrameTooLarge { .. }
            ))
        ) {
            return true;
        }
        current = e.source();
    }
    false
}

/// Send `records` single-byte records from node-a in one batch and return
/// how many node-b received, plus both join results.
async fn send_big_batch(
    mode: Mode,
    records: usize,
) -> (usize, Option<Result<()>>, Option<Result<()>>) {
    let names = ["node-a", "node-b"];
    let mut mesh = FaultMesh::new(&names, 2, mode == Mode::Shared);
    let id = DataflowId::new();
    let mut managers = Vec::new();
    let mut handles = Vec::new();
    for name in names {
        handles.push(match mode {
            Mode::Dedicated => spawn_node(
                topology(),
                name,
                id,
                mesh.dedicated(name, 1024),
                2,
                bytes_to_node_b,
            ),
            Mode::Shared => {
                let m = mesh.managers(name);
                managers.push(std::sync::Arc::clone(&m));
                spawn_node(
                    topology(),
                    name,
                    id,
                    ClusterSpawnTransport::shared(m, 1024),
                    2,
                    bytes_to_node_b,
                )
            }
        });
    }
    let (rt_a, ca) = handles.remove(0).await.unwrap();
    let (rt_b, cb) = handles.remove(0).await.unwrap();
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let _oa = ca.take_output::<u64>(0, "out").unwrap();
    let ob = support::collect(cb.take_output::<u64>(0, "out").unwrap());
    let sa = ca.take_input::<u8>(0, "data").unwrap();
    drop(cb.take_input::<u8>(0, "data").unwrap());
    sa.send(0, vec![7u8; records]).unwrap();
    drop(sa);
    let ja = join_with_timeout(ca, Duration::from_secs(120));
    let jb = join_with_timeout(cb, Duration::from_secs(120));
    let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
    let received = ob.join().map(|out| out.len()).unwrap_or(0);
    drop_runtimes(vec![rt_a, rt_b]).await;
    drop(managers);
    (received, ra, rb)
}

/// Control: a batch of exactly 10M records always arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_batch_at_ten_million_records_arrives() {
    let (received, ra, rb) = send_big_batch(Mode::Dedicated, 10_000_000).await;
    assert!(
        matches!(ra, Some(Ok(()))) && matches!(rb, Some(Ok(()))),
        "a={} b={}",
        fmt_join(&ra),
        fmt_join(&rb)
    );
    assert_eq!(received, 10_000_000);
}

/// One record over the old decoder limit: previously the batch was silently
/// dropped and both nodes reported success.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exchange_batch_over_ten_million_records_arrives() {
    for mode in [Mode::Dedicated, Mode::Shared] {
        let (received, ra, rb) = send_big_batch(mode, 10_000_001).await;
        assert!(
            matches!(ra, Some(Ok(()))) && matches!(rb, Some(Ok(()))),
            "{mode:?}: a={} b={}",
            fmt_join(&ra),
            fmt_join(&rb)
        );
        assert_eq!(received, 10_000_001, "{mode:?}");
    }
}

/// A batch whose encoding exceeds the 256 MiB frame limit fails the sending
/// node with `FrameTooLarge`; its peer (still running) is cancelled by it,
/// rather than seeing the connection die.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn oversize_envelope_fails_sender_and_peers_promptly() {
    let names = ["node-a", "node-b"];
    let mut mesh = FaultMesh::new(&names, 2, false);
    let id = DataflowId::new();
    let ha = spawn_node(
        topology(),
        "node-a",
        id,
        mesh.dedicated("node-a", 1024),
        2,
        blobs_to_node_b,
    );
    let hb = spawn_node(
        topology(),
        "node-b",
        id,
        mesh.dedicated("node-b", 1024),
        2,
        blobs_to_node_b,
    );
    let ((rt_a, ca), (rt_b, cb)) = (ha.await.unwrap(), hb.await.unwrap());
    let (mut ca, mut cb) = (ca.unwrap(), cb.unwrap());
    let _oa = ca.take_output::<u64>(0, "out").unwrap();
    let _ob = cb.take_output::<u64>(0, "out").unwrap();
    let sa = ca.take_input::<Vec<u8>>(0, "data").unwrap();
    // node-b keeps its input open: only node-a's failure can stop it.
    let sb = cb.take_input::<Vec<u8>>(0, "data").unwrap();
    const MIB: usize = 1 << 20;
    sa.send(0, (0..3).map(|_| vec![1u8; 90 * MIB]).collect())
        .unwrap();
    drop(sa);

    let ra = join_with_timeout(ca, Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        matches!(&ra, Some(Err(e)) if is_frame_too_large(e)),
        "node-a must fail with FrameTooLarge: {}",
        fmt_join(&ra)
    );
    let rb = join_with_timeout(cb, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        matches!(
            &rb,
            Some(Err(Error::Cancelled {
                reason: Some(CancellationReason::PeerCancelled { peer_id, .. })
            })) if peer_id == "node-a"
        ),
        "node-b must be cancelled by node-a: {}",
        fmt_join(&rb)
    );
    drop(sb);
    drop_runtimes(vec![rt_a, rt_b]).await;
}
