//! Audit: one output stream fanned out into TWO exchange edges.
//!
//! `SubgraphBuilder::materialization_reporters` keys in-flight reporters by the
//! exchange edge's *source* `(op, output)`. Two exchanges hanging off the same
//! upstream output share that key, so the HashMap keeps only the last binding.

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(20);

fn build(builder: &mut DataflowBuilder<u64>) {
    let input = builder.input::<u64>("data").unwrap();
    let a = input.clone().exchange_by_hash("x1", |x: &u64| *x);
    let b = input.exchange_by_hash("x2", |x: &u64| x.wrapping_mul(7));
    a.output("o1").unwrap();
    b.output("o2").unwrap();
}

async fn run(workers: usize) -> (Vec<u64>, Vec<u64>) {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-fanout-exchange",
            workers,
            |b| {
                build(b);
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    let sender = multi.take_input::<u64>(0, "data").unwrap();
    for w in 1..workers {
        drop(multi.take_input::<u64>(w, "data").unwrap());
    }
    for epoch in 0..3u64 {
        sender
            .send(epoch, (0..20).map(|i| epoch * 100 + i).collect())
            .unwrap();
    }
    drop(sender);
    let mut r1 = Vec::new();
    let mut r2 = Vec::new();
    for w in 0..workers {
        r1.push(multi.take_output::<u64>(w, "o1").unwrap());
        r2.push(multi.take_output::<u64>(w, "o2").unwrap());
    }
    let res = tokio::time::timeout(
        TIMEOUT,
        tokio::task::spawn_blocking(move || multi.join_blocking()),
    )
    .await;
    match res {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("join failed: {e}"),
        Ok(Err(e)) => panic!("join panicked: {e}"),
        Err(_) => panic!(
            "HANG: dataflow with two exchanges off one output did not complete within {TIMEOUT:?}"
        ),
    }
    let mut o1 = Vec::new();
    for r in r1 {
        for (_t, d) in r.collect_data() {
            o1.extend(d);
        }
    }
    let mut o2 = Vec::new();
    for r in r2 {
        for (_t, d) in r.collect_data() {
            o2.extend(d);
        }
    }
    o1.sort();
    o2.sort();
    (o1, o2)
}

fn expected() -> Vec<u64> {
    let mut v: Vec<u64> = (0..3u64)
        .flat_map(|e| (0..20).map(move |i| e * 100 + i))
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn two_exchanges_from_one_output_single_worker() {
    let (o1, o2) = run(1).await;
    assert_eq!(o1, expected());
    assert_eq!(o2, expected());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
async fn two_exchanges_from_one_output_two_workers() {
    let (o1, o2) = run(2).await;
    assert_eq!(o1, expected());
    assert_eq!(o2, expected());
}
