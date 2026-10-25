//! Audit (cluster/TCP): spurious completion of an idle node via the zero-sum`n//! `total_counts` check, with the in-flight producer side CORRECTLY wired`n//! (`map -> exchange`). Node B consumes one record from node A before A's`n//! `+1` in-flight progress frame arrives; B's tracker then sums`n//! `+1 (A's open input cap) - 1 (consumed)` == 0 and, after `max_idle_sweeps`,`n//! B force-closes all operators. Later records from A to B are dropped.`n
use std::collections::HashMap;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
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

async fn run(map_first: bool, initial_to_b: usize) -> std::result::Result<Vec<u64>, String> {
    run_ext(map_first, initial_to_b, false).await
}

/// `slow_sibling`: node A additionally runs a slow sibling operator on the
/// input in the same sweep, which delays A's progress broadcast (the `+1`
/// in-flight frame) after the exchanged record has already been sent to B.
async fn run_ext(
    map_first: bool,
    initial_to_b: usize,
    slow_sibling: bool,
) -> std::result::Result<Vec<u64>, String> {
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
        if slow_sibling {
            input
                .clone()
                .filter("slow", |_t, _x| {
                    std::thread::sleep(Duration::from_millis(100));
                    false
                })
                .output("sink")?;
        }
        let s = if map_first {
            input.map("m", |_t, x| x)
        } else {
            input
        };
        s.exchange_by_hash("x", |x: &u64| *x).output("out")?;
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
    if slow_sibling {
        let sa_ = ca.take_output::<u64>(0, "sink").unwrap();
        let sb_ = cb.take_output::<u64>(0, "sink").unwrap();
        std::thread::spawn(move || sa_.collect_data());
        std::thread::spawn(move || sb_.collect_data());
    }
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    drop(cb.take_input::<u64>(0, "data").unwrap());
    let mut got: Vec<u64> = Vec::new();
    let res: std::result::Result<(), String> = tokio::task::block_in_place(|| {
        // Find a value routed to node B.
        let mut to_b = None;
        for v in 1..50u64 {
            sa.send(0, vec![v]).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            let mut where_ = None;
            while std::time::Instant::now() < deadline && where_.is_none() {
                if let Some(OutputEvent::Data { data, .. }) =
                    ob.recv_timeout(Duration::from_millis(5))
                {
                    got.extend(data);
                    where_ = Some(1);
                } else if let Some(OutputEvent::Data { data, .. }) =
                    oa.recv_timeout(Duration::from_millis(5))
                {
                    got.extend(data);
                    where_ = Some(0);
                }
            }
            if where_ == Some(1) {
                to_b = Some(v);
                break;
            }
        }
        let first = to_b.ok_or("no value routed to node B")?;
        for k in 1..initial_to_b as u64 {
            sa.send(0, vec![first + 2 * k * 100]).unwrap();
        }
        std::thread::sleep(Duration::from_millis(500));
        for k in 1..=5u64 {
            sa.send(0, vec![first + 2 * k * 1000])
                .map_err(|e| format!("send: {e}"))?;
        }
        std::thread::sleep(Duration::from_millis(300));
        Ok(())
    });
    drop(sa);
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
                std::mem::forget(_rt_a);
                std::mem::forget(_rt_b);
                return Err("HANG: cluster did not complete within 20s".into());
            }
        }
    }
    res?;
    for (_, d) in oa.collect_data().into_iter().chain(ob.collect_data()) {
        got.extend(d);
    }
    got.sort();
    Ok(got)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn tcp_input_exchange_idle_node_does_not_drop_late_records() {
    let got = run(false, 1).await.unwrap();
    assert_eq!(got.len(), 6, "records lost: {got:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn tcp_map_exchange_idle_node_does_not_drop_late_records() {
    for round in 0..10 {
        let got = run(true, 1).await.unwrap();
        assert_eq!(got.len(), 6, "round {round}: records lost: {got:?}");
    }
}

/// Control: two records consumed by B before going idle. Once A's progress
/// frames arrive B's sum is `+1` (A's input capability) and B stays alive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn tcp_map_exchange_control_two_initial_records() {
    let mut losses = 0;
    for _ in 0..10 {
        let got = run(true, 2).await.unwrap();
        if got.len() != 7 {
            losses += 1;
        }
    }
    assert_eq!(losses, 0, "records lost in {losses}/10 rounds");
}

/// Frequency probe for the wired (map -> exchange) case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn tcp_map_exchange_loss_frequency() {
    let mut losses = 0;
    for _ in 0..20 {
        let got = run(true, 1).await.unwrap();
        if got.len() != 6 {
            losses += 1;
        }
    }
    eprintln!("map->exchange: rounds with lost records = {losses}/20");
    assert_eq!(losses, 0, "records lost in {losses}/20 rounds");
}

/// Amplified race: correctly wired `map -> exchange`, with a slow sibling
/// operator on node A delaying A's progress broadcast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn tcp_map_exchange_slow_sibling_loses_records() {
    let mut losses = 0;
    let mut sample = Vec::new();
    for _ in 0..5 {
        let got = run_ext(true, 1, true).await.unwrap();
        if got.len() != 6 {
            losses += 1;
            sample = got;
        }
    }
    assert_eq!(
        losses, 0,
        "records lost in {losses}/5 rounds (e.g. got {sample:?})"
    );
}
