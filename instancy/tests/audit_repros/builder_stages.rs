//! Audit (builder/stages): per-stage parallelism wiring of pipeline edges that
//! cross an inferred stage boundary.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn rt() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig::default()).unwrap()
}

fn join_with_timeout(multi: instancy::runtime::MultiSpawnedDataflow<u64>) -> instancy::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("dataflow did not complete within 20s (hang)")
}

/// `a.merge(b)` where `a` and `b` come out of exchanges with different
/// explicit parallelism. Stage inference puts the concat in b's stage, so the
/// edge a -> concat is a *pipeline* edge crossing stages. The staged spawn path
/// only wires intra-stage edges and cross-stage *exchange* edges, so this edge is
/// never materialized: everything routed through `a` is silently dropped.
#[test]
#[ignore = "expected failure (H14); see EXPECTED in tests/audit_repros.rs"]
fn audit_builder_cross_stage_pipeline_edge_drops_data() {
    let sink: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let rt = rt();
    let s = sink.clone();
    let mut multi = rt
        .spawn_multi(
            "cross-stage-concat",
            1,
            move |b: &mut DataflowBuilder<u64>| {
                let input = b.input::<u64>("data")?;
                let a = input.clone().exchange_to("a", 2, |x: &u64| *x)?;
                let bb = input.exchange_to("b", 3, |x: &u64| *x)?;
                let s = s.clone();
                a.merge(bb)?
                    .for_each("sink", move |_t, x: &u64| s.lock().unwrap().push(*x));
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let n = multi.num_workers();
    eprintln!("num_workers = {n}");
    let tx = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..n {
        if let Ok(s) = multi.take_input::<u64>(w, "data") {
            drop(s);
        }
    }
    tx.send(0, (0..10).collect()).unwrap();
    drop(tx);
    join_with_timeout(multi).unwrap();

    let mut got = sink.lock().unwrap().clone();
    got.sort();
    let mut expected: Vec<u64> = (0..10).chain(0..10).collect();
    expected.sort();
    eprintln!("got = {got:?}");
    assert_eq!(got, expected, "data routed through exchange 'a' was lost");
}

/// Same shape with `binary`: stage-0 stream joined with an exchanged stream
/// whose stage has a different parallelism. The stage-0 -> binary pipeline
/// edge is not wired in the staged path.
#[test]
#[ignore = "expected failure (H14); see EXPECTED in tests/audit_repros.rs"]
fn audit_builder_cross_stage_binary_input_missing() {
    let sink: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let rt = rt();
    let s = sink.clone();
    let res = rt.spawn_multi(
        "cross-stage-binary",
        1,
        move |b: &mut DataflowBuilder<u64>| {
            let input = b.input::<u64>("data")?;
            let ex = input.clone().exchange_to("ex", 2, |x: &u64| *x)?;
            let s = s.clone();
            input
                .binary(ex, "join", |l, r, out| {
                    while let Some((t, d)) = l.next() {
                        out.push_vec(t, d);
                    }
                    while let Some((t, d)) = r.next() {
                        out.push_vec(t, d);
                    }
                    Ok(())
                })?
                .for_each("sink", move |_t, x: &u64| s.lock().unwrap().push(*x));
            Ok(())
        },
        SpawnOptions::default(),
    );
    match res {
        Err(e) => panic!("spawn failed (documented as a build-time error?): {e}"),
        Ok(mut multi) => {
            let n = multi.num_workers();
            let tx = multi.take_input::<u64>(0, "data").unwrap();
            for w in 1..n {
                if let Ok(s) = multi.take_input::<u64>(w, "data") {
                    drop(s);
                }
            }
            tx.send(0, (0..10).collect()).unwrap();
            drop(tx);
            let r = join_with_timeout(multi);
            let mut got = sink.lock().unwrap().clone();
            got.sort();
            eprintln!("join = {r:?}; got = {got:?}");
            r.unwrap();
            let mut expected: Vec<u64> = (0..10).chain(0..10).collect();
            expected.sort();
            assert_eq!(got, expected);
        }
    }
}
