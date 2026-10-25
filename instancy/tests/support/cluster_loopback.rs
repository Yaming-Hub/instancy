//! Test support: two-node clusters over loopback TCP, using either Dedicated
//! (per-dataflow connections) or Shared (pooled connections) transport.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::runtime::ClusterSpawnedDataflow;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Dedicated,
    Shared,
}

type PeerManagers = Arc<HashMap<String, SharedPeerManager>>;

/// Runtimes and transport state that must outlive the cluster handles.
pub struct Nodes {
    pub rt_a: RuntimeHandle,
    pub rt_b: RuntimeHandle,
    _managers: Option<(PeerManagers, PeerManagers)>,
}

type Halves = (OwnedReadHalf, OwnedWriteHalf);

async fn tcp_pair() -> (Halves, Halves) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    let accepted = accepted.0;
    accepted.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.into_split(), connected.into_split())
}

struct PreEstablished(Mutex<VecDeque<Halves>>);

impl ConnectionFactory for PreEstablished {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.0.lock().unwrap().pop_front().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "no more pre-established connections",
            ))
        })
    }
}

fn shared_config() -> SharedConnectionConfig {
    SharedConnectionConfig {
        connections: 1,
        probe_interval: Duration::from_secs(3600),
        rtt_ema_alpha: 0.2,
        ..SharedConnectionConfig::default()
    }
}

fn manager(peer: &str, halves: Halves) -> PeerManagers {
    let factory: Arc<dyn DynConnectionFactory> =
        Arc::new(PreEstablished(Mutex::new(VecDeque::from([halves]))));
    let manager = SharedPeerManager::new(
        peer.to_string(),
        shared_config(),
        factory,
        &tokio::runtime::Handle::current(),
        tokio::sync::broadcast::channel(16).0,
    )
    .unwrap();
    Arc::new(HashMap::from([(peer.to_string(), manager)]))
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

/// Spawn the same dataflow on `node-a` and `node-b` of `topology`.
pub async fn spawn_pair<F>(
    transport: Transport,
    topology: ClusterTopology,
    id: DataflowId,
    build: F,
    options: SpawnOptions,
) -> (
    Nodes,
    ClusterSpawnedDataflow<u64>,
    ClusterSpawnedDataflow<u64>,
)
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Clone,
{
    let handle = tokio::runtime::Handle::current();
    let (rt_a, rt_b) = (runtime(), runtime());
    let ((ra, wa), (rb, wb)) = tcp_pair().await;
    let (a, b, managers) = match transport {
        Transport::Dedicated => {
            let to_b = vec![PeerConnection {
                node_id: "node-b".to_string(),
                reader: ra,
                writer: wa,
            }];
            let to_a = vec![PeerConnection {
                node_id: "node-a".to_string(),
                reader: rb,
                writer: wb,
            }];
            let (a, b) = tokio::join!(
                rt_a.spawn_cluster_async(
                    "loopback",
                    topology.clone(),
                    "node-a",
                    id,
                    ClusterSpawnTransport::dedicated(to_b, 1024),
                    HANDSHAKE_TIMEOUT,
                    build.clone(),
                    &handle,
                    options.clone(),
                ),
                rt_b.spawn_cluster_async(
                    "loopback",
                    topology,
                    "node-b",
                    id,
                    ClusterSpawnTransport::dedicated(to_a, 1024),
                    HANDSHAKE_TIMEOUT,
                    build,
                    &handle,
                    options,
                ),
            );
            (a, b, None)
        }
        Transport::Shared => {
            let managers_a = manager("node-b", (ra, wa));
            let managers_b = manager("node-a", (rb, wb));
            let (a, b) = tokio::join!(
                rt_a.spawn_cluster_async(
                    "loopback",
                    topology.clone(),
                    "node-a",
                    id,
                    ClusterSpawnTransport::shared(Arc::clone(&managers_a), 1024),
                    HANDSHAKE_TIMEOUT,
                    build.clone(),
                    &handle,
                    options.clone(),
                ),
                rt_b.spawn_cluster_async(
                    "loopback",
                    topology,
                    "node-b",
                    id,
                    ClusterSpawnTransport::shared(Arc::clone(&managers_b), 1024),
                    HANDSHAKE_TIMEOUT,
                    build,
                    &handle,
                    options,
                ),
            );
            (a, b, Some((managers_a, managers_b)))
        }
    };
    (
        Nodes {
            rt_a,
            rt_b,
            _managers: managers,
        },
        a.expect("node-a spawn_cluster failed"),
        b.expect("node-b spawn_cluster failed"),
    )
}
