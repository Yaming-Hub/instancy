//! Audit (distributed progress protocol): start-up synchronisation.
//!
//! Each worker broadcasts its initial capabilities from `tracker.initialize()`
//! during materialisation, then the ready barrier runs on the CONTROL lane,
//! which has strict priority over the payload lane (data + progress) in the
//! dedicated transport bridge. Nothing makes a worker wait for its peers'
//! initial capabilities before computing frontiers: `all_peers_synced()` only
//! gates the force-close path. So a node whose peer's initial-capability
//! frame is still queued behind the control lane computes frontiers WITHOUT
//! the peer's input capability, fires notifications prematurely, and later
//! data at the same epoch produces a second, partial result.

#![cfg(feature = "transport")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Notify;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::OutputEvent;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

const HEADER: usize = 28;

#[derive(Default)]
struct Gate {
    hold_payload: AtomicBool,
    notify: Notify,
}

impl Gate {
    async fn wait_payload_open(&self) {
        loop {
            let n = self.notify.notified();
            tokio::pin!(n);
            n.as_mut().enable();
            if !self.hold_payload.load(Ordering::SeqCst) {
                return;
            }
            n.await;
        }
    }
    fn release(&self) {
        self.hold_payload.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

/// Relay that forwards control frames (channel 0) immediately and holds
/// payload frames (data + progress) while the gate is closed, preserving the
/// payload lane's FIFO order. Models the bridge's "control > payload" priority
/// with a slow payload lane.
async fn relay(mut from: DuplexStream, mut to: DuplexStream, gate: Arc<Gate>) {
    let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let (ctrl_tx, mut ctrl_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let reader = tokio::spawn(async move {
        loop {
            let mut header = [0u8; HEADER];
            if from.read_exact(&mut header).await.is_err() {
                break;
            }
            let ch = u64::from_le_bytes(header[16..24].try_into().unwrap());
            let len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
            let mut buf = header.to_vec();
            buf.resize(HEADER + len, 0);
            if from.read_exact(&mut buf[HEADER..]).await.is_err() {
                break;
            }
            let _ = if ch == 0 {
                ctrl_tx.send(buf)
            } else {
                frames_tx.send(buf)
            };
        }
    });
    let mut payload_open = false;
    loop {
        tokio::select! {
            biased;
            c = ctrl_rx.recv() => match c {
                Some(b) => { if to.write_all(&b).await.is_err() { break; } }
                None => break,
            },
            _ = gate.wait_payload_open(), if !payload_open => { payload_open = true; }
            p = frames_rx.recv(), if payload_open => match p {
                Some(b) => { if to.write_all(&b).await.is_err() { break; } }
                None => break,
            },
        }
    }
    reader.abort();
    let _ = to.shutdown().await;
}

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

async fn scenario(hold: bool) -> Vec<(u64, Vec<u64>)> {
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", 1),
    ])
    .unwrap();
    let id = DataflowId::new();
    let gate = Arc::new(Gate::default());
    gate.hold_payload.store(hold, Ordering::SeqCst);
    // A -> B direct; B -> A through the gated relay.
    let (a_w, b_r) = tokio::io::duplex(64 * 1024);
    let (b_w, relay_in) = tokio::io::duplex(64 * 1024);
    let (relay_out, a_r) = tokio::io::duplex(64 * 1024);
    let relay_task = tokio::spawn(relay(relay_in, relay_out, Arc::clone(&gate)));
    let conn_a = PeerConnection {
        node_id: "node-b".to_string(),
        reader: a_r,
        writer: a_w,
    };
    let conn_b = PeerConnection {
        node_id: "node-a".to_string(),
        reader: b_r,
        writer: b_w,
    };
    let th = tokio::runtime::Handle::current();
    let spawn = |node: &'static str, conn: PeerConnection<DuplexStream, DuplexStream>| {
        let topology = topology.clone();
        let th = th.clone();
        tokio::task::spawn_blocking(move || {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 1,
                ..RuntimeConfig::default()
            })?;
            let c = rt.spawn_cluster(
                "audit-startup",
                topology,
                node,
                id,
                ClusterSpawnTransport::dedicated(vec![conn], 1024),
                Duration::from_secs(10),
                build,
                &th,
                SpawnOptions::new(),
            )?;
            Ok::<_, instancy::Error>((rt, c))
        })
    };
    let (ra, rb) = tokio::join!(spawn("node-a", conn_a), spawn("node-b", conn_b));
    let (rt_a, mut ca) = ra.unwrap().unwrap();
    let (rt_b, mut cb) = rb.unwrap().unwrap();
    let out_a = ca.take_output::<u64>(0, "out").unwrap();
    let _out_b = cb.take_output::<u64>(0, "out").unwrap();
    let sa = ca.take_input::<u64>(0, "data").unwrap();
    let sb = cb.take_input::<u64>(0, "data").unwrap();

    // A: two records at epoch 5 routed to A, then close.
    sa.send(5, vec![0, 2]).unwrap();
    drop(sa);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Release B's payload lane; B sends one more record at epoch 5 routed to A.
    gate.release();
    sb.send(5, vec![4]).unwrap();
    drop(sb);

    let a_res = tokio::time::timeout(Duration::from_secs(10), ca.join().unwrap()).await;
    let b_res = tokio::time::timeout(Duration::from_secs(10), cb.join().unwrap()).await;
    let mut out = Vec::new();
    while let Some(ev) = out_a.recv_timeout(Duration::from_millis(20)) {
        if let OutputEvent::Data { time, data } = ev {
            out.push((time, data));
        }
    }
    eprintln!("hold={hold} a_res={a_res:?} b_res={b_res:?} out_a={out:?}");
    relay_task.abort();
    let _ = tokio::task::spawn_blocking(move || {
        drop(rt_a);
        drop(rt_b);
    })
    .await;
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn control_startup_without_payload_delay() {
    assert_eq!(scenario(false).await, vec![(5, vec![3])]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "expected failure (H13); see EXPECTED in tests/audit_repros.rs"]
async fn premature_notification_before_peer_initial_capabilities_arrive() {
    assert_eq!(
        scenario(true).await,
        vec![(5, vec![3])],
        "epoch 5 must be reported once with all 3 records"
    );
}
