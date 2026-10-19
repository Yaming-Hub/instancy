//! Regression tests for the cluster wire channel-id space.
//!
//! Data channels, progress channels, the control channel and transport
//! internals (probes) share one `u64` channel-id space per dataflow. These
//! tests check that the namespaces can never collide, that id arithmetic is
//! overflow-checked, and that duplicate registrations are rejected instead of
//! silently replacing a receiver.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport::TransportError;
use instancy::communication::transport_session::{
    ChannelRegistration, PROGRESS_CHANNEL_BASE, PeerConnection, TransportSession, data_channel_id,
    progress_channel_id,
};
use instancy::dataflow::channels::network::NetworkEdgeMaterializer;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

type Conn = PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

/// Progress channel formula used by spawn_cluster:
/// `PROGRESS_CHANNEL_BASE + src * N + dst`.
fn progress_id(src: usize, dst: usize, n: usize) -> u64 {
    let id = PROGRESS_CHANNEL_BASE + (src * n + dst) as u64;
    assert_eq!(progress_channel_id(src, dst, n).unwrap(), id);
    id
}

fn data_id(edge: usize, src: usize, dst: usize, n: usize) -> u64 {
    let id = NetworkEdgeMaterializer::<u64, u64>::channel_id(edge, src, dst, n).unwrap();
    assert_eq!(data_channel_id(edge, src, dst, n).unwrap(), id);
    id
}

/// 2 nodes x 50 workers, edge 100: data (100, 0->50) used to equal progress (0->51).
#[test]
fn data_ids_do_not_collide_with_progress_ids_many_edges() {
    let n = 100;
    assert_ne!(data_id(100, 0, 50, n), progress_id(0, 51, n));
}

/// node-a has 1 worker, node-b has 1000 (N = 1001), one exchange edge:
/// data (1000->0) used to equal progress (1->0).
#[test]
fn data_ids_do_not_collide_with_progress_ids_many_workers() {
    let n = 1001;
    assert_ne!(data_id(0, 1000, 0, n), progress_id(1, 0, n));
}

/// 2 nodes x 64 workers (N = 128): data (edge 61, 68->63) used to equal
/// progress (64->0), both registered for the same peer.
#[test]
fn data_ids_do_not_collide_with_progress_ids_two_by_64() {
    let n = 128;
    assert_ne!(data_id(61, 68, 63, n), progress_id(64, 0, n));
}

/// 10 nodes x 100 workers: the second exchange edge's (0->0) data channel used
/// to equal progress (0->1).
#[test]
fn data_ids_do_not_collide_with_progress_ids_second_edge() {
    let n = 1000;
    assert_ne!(data_id(1, 0, 0, n), progress_id(0, 1, n));
}

/// Exhaustive check over the cluster shapes from the original reports: every
/// data id of every exchange edge and every progress id is distinct, and none
/// is the control (0) or probe (`u64::MAX`) channel.
#[test]
fn all_ids_are_distinct_for_large_cluster_shapes() {
    for (n, edges) in [(100usize, 101usize), (128, 62), (1001, 1), (3, 5000)] {
        let mut seen = std::collections::HashSet::with_capacity(n * n * (edges + 1));
        for src in 0..n {
            for dst in 0..n {
                assert!(seen.insert(progress_id(src, dst, n)));
                for edge in 0..edges {
                    assert!(seen.insert(data_id(edge, src, dst, n)));
                }
            }
        }
        assert!(!seen.contains(&0));
        assert!(!seen.contains(&u64::MAX));
    }
}

/// Ids that would leave their namespace are an error instead of wrapping into
/// another namespace (the old arithmetic was unchecked `usize` math).
#[test]
fn channel_id_overflow_is_an_error() {
    assert!(data_channel_id(usize::MAX, 0, 0, 2).is_err());
    assert!(data_channel_id(1 << 61, 1, 1, 2).is_err());
    assert!(data_channel_id(0, 2, 0, 2).is_err(), "source out of range");
    assert!(progress_channel_id(0, 2, 2).is_err(), "target out of range");
}

/// The demuxer used to keep only the LAST registration for a duplicated id,
/// silently replacing the data receiver with the progress receiver. A
/// colliding registration is now rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn colliding_registration_is_rejected_instead_of_replacing_a_receiver() {
    let (a_to_b, _b_from_a) = tokio::io::duplex(1 << 16);
    let (_b_to_a, a_from_b) = tokio::io::duplex(1 << 16);
    let id = 1_000_051u64;
    let regs = vec![ChannelRegistration {
        peer_node_id: "node-b".into(),
        channel_id: id,
    }];
    let r = TransportSession::new(
        DataflowId::new(),
        vec![PeerConnection {
            node_id: "node-b".into(),
            reader: a_from_b,
            writer: a_to_b,
        }],
        &regs,
        &regs,
        16,
        &tokio::runtime::Handle::current(),
    );
    assert!(
        matches!(r, Err(TransportError::InvalidRegistration { .. })),
        "duplicate registration was not rejected"
    );
}

async fn make_tcp_connections(node_ids: &[&str]) -> HashMap<String, Vec<Conn>> {
    let mut result: HashMap<String, Vec<_>> = HashMap::new();
    for id in node_ids {
        result.insert(id.to_string(), Vec::new());
    }
    for i in 0..node_ids.len() {
        for j in (i + 1)..node_ids.len() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (accepted, connected) =
                tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
            let (ri, wi) = accepted.0.into_split();
            let (rj, wj) = connected.into_split();
            result.get_mut(node_ids[i]).unwrap().push(PeerConnection {
                node_id: node_ids[j].to_string(),
                reader: ri,
                writer: wi,
            });
            result.get_mut(node_ids[j]).unwrap().push(PeerConnection {
                node_id: node_ids[i].to_string(),
                reader: rj,
                writer: wj,
            });
        }
    }
    result
}

/// End to end: a valid 2 x 50-worker cluster with 101 chained exchange edges
/// used to fail to spawn ("missing receiver") because a data registration
/// consumed a progress channel id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_with_many_exchange_edges_spawns() {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 50),
        NodeConfig::new("node-b", 50),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let handle = tokio::runtime::Handle::current();
    let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
        let mut s = builder.input::<u64>("data")?;
        for i in 0..101 {
            s = s.exchange_by_hash(format!("ex{i}"), |x: &u64| *x);
        }
        s.output("results")?;
        Ok(())
    };
    let mut joins = Vec::new();
    for node in ["node-a", "node-b"] {
        let c = conns.remove(node).unwrap();
        let topo = topology.clone();
        let h = handle.clone();
        joins.push(tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                tokio_mode: instancy::TokioMode::External(h.clone()),
                ..RuntimeConfig::default()
            })
            .unwrap();
            let r = rt.spawn_cluster(
                "many-edges",
                topo,
                node,
                dataflow_id,
                ClusterSpawnTransport::dedicated(c, 1024),
                Duration::from_secs(60),
                build,
                &h,
                SpawnOptions::new(),
            );
            let err = r.as_ref().err().map(|e| e.to_string());
            if let Ok(c) = r {
                c.cancel();
            }
            rt.shutdown();
            err
        }));
    }
    for j in joins {
        let err = tokio::time::timeout(Duration::from_secs(120), j)
            .await
            .expect("spawn hung")
            .unwrap();
        assert!(err.is_none(), "valid cluster failed to spawn: {err:?}");
    }
}
