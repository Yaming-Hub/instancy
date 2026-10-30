//! Audit (cluster/TCP): `input -> exchange -> iterate` never completes because
//! the input source does not record the producer-side `+n` of the exchange
//! in-flight accounting while the consumer records `-n`.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result};
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};

type Conn = PeerConnection<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

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
            let stream_i = accepted.0;
            let stream_j = connected;
            stream_i.set_nodelay(true).unwrap();
            stream_j.set_nodelay(true).unwrap();
            let (ri, wi) = stream_i.into_split();
            let (rj, wj) = stream_j.into_split();
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

fn spawn_node<F>(
    topology: ClusterTopology,
    node_id: &str,
    dataflow_id: DataflowId,
    connections: Vec<Conn>,
    tokio_handle: tokio::runtime::Handle,
    build: F,
) -> tokio::task::JoinHandle<
    Result<(
        RuntimeHandle,
        instancy::runtime::ClusterSpawnedDataflow<u64>,
    )>,
>
where
    F: Fn(&mut DataflowBuilder<u64>) -> Result<()> + Send + 'static,
{
    let node_id = node_id.to_string();
    tokio::task::spawn_blocking(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 1,
            ..RuntimeConfig::default()
        })?;
        let cluster = rt.spawn_cluster(
            "audit-tcp",
            topology,
            &node_id,
            dataflow_id,
            ClusterSpawnTransport::dedicated(connections, 1024),
            Duration::from_secs(10),
            build,
            &tokio_handle,
            SpawnOptions::new(),
        )?;
        Ok((rt, cluster))
    })
}

async fn run(map_first: bool) -> std::result::Result<Vec<u64>, String> {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let dataflow_id = DataflowId::new();
    let mut conns = make_tcp_connections(&["node-a", "node-b"]).await;
    let h = tokio::runtime::Handle::current();
    let build = move |b: &mut DataflowBuilder<u64>| -> Result<()> {
        let input = b.input::<u64>("data")?;
        let s = if map_first {
            input.map("m", |_t, x| x)
        } else {
            input
        };
        let out = s
            .exchange_by_hash("x", |x: &u64| *x)
            .iterate::<u32>("loop", 1u32, |v| {
                let inc = v.map("inc", |_t: &Product<u64, u32>, x: u64| x + 1);
                let done = inc.clone().filter("done", |_t, x| *x % 4 == 0);
                let again = inc.filter("again", |_t, x| *x % 4 != 0);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
        out.output("out")?;
        Ok(())
    };
    let ha = spawn_node(
        topology.clone(),
        "node-a",
        dataflow_id,
        conns.remove("node-a").unwrap(),
        h.clone(),
        build,
    );
    let hb = spawn_node(
        topology,
        "node-b",
        dataflow_id,
        conns.remove("node-b").unwrap(),
        h,
        build,
    );
    let (ra, rb) = tokio::join!(ha, hb);
    let (_rt_a, mut ca) = ra.unwrap().unwrap();
    let (_rt_b, mut cb) = rb.unwrap().unwrap();
    let oa = ca.take_output::<u64>(0, "out").unwrap();
    let ob = cb.take_output::<u64>(0, "out").unwrap();
    let ta = std::thread::spawn(move || oa.collect_data());
    let tb = std::thread::spawn(move || ob.collect_data());
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    sa.send(0, (1..=20).collect()).unwrap();
    drop(sa);
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    let tx2 = tx.clone();
    std::thread::spawn(move || {
        let _ = tx.send(ca.join_blocking().map_err(|e| e.to_string()));
    });
    std::thread::spawn(move || {
        let _ = tx2.send(cb.join_blocking().map_err(|e| e.to_string()));
    });
    for _ in 0..2 {
        match tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_secs(20))) {
            Ok(r) => r?,
            Err(_) => {
                // Avoid blocking the test process on runtime drop.
                std::mem::forget(_rt_a);
                std::mem::forget(_rt_b);
                return Err("HANG: cluster did not complete within 20s".into());
            }
        }
    }
    let mut v: Vec<u64> = ta
        .join()
        .unwrap()
        .into_iter()
        .chain(tb.join().unwrap())
        .flat_map(|(_, d)| d)
        .collect();
    v.sort();
    Ok(v)
}

fn expected() -> Vec<u64> {
    let mut v: Vec<u64> = (1..=20u64).map(|x| (x / 4 + 1) * 4).collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_map_exchange_iterate_completes() {
    assert_eq!(run(true).await.unwrap(), expected());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_input_exchange_iterate_completes() {
    assert_eq!(run(false).await.unwrap(), expected());
}
