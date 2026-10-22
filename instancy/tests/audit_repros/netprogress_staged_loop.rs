//! Audit (consumer side, staged executor): loop in-flight counters used for
//! feedback-loop convergence in `CombinedStageExecutor` are created by each
//! worker's own `build()` (one `Arc<AtomicI64>` per worker), but a record can
//! ENTER a loop on one worker and LEAVE it on another (exchange inside the
//! loop body). The entering worker's counter then stays > 0 forever (never
//! "drained" -> the loop never converges -> hang), while the leaving worker's
//! counter goes negative (`<= 0` is treated as drained -> may converge while
//! records are still circulating).

use std::time::Duration;

use instancy::{
    DataflowBuilder, IterateResult, Product, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

fn run(
    threshold: i64,
) -> (
    Result<instancy::Result<()>, std::sync::mpsc::RecvTimeoutError>,
    Vec<i64>,
) {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "audit-staged-loop",
            2,
            move |builder: &mut DataflowBuilder<u64>| {
                let input = builder.input::<i64>("data")?;
                let output = input.iterate::<u32>("inc-loop", 1u32, |iter_var| {
                    // x -> x+1, routed by parity: the record alternates workers.
                    let next = iter_var
                        .map("inc", |_t: &Product<u64, u32>, x| x + 1)
                        .exchange_by_hash("by-parity", |x: &i64| *x as u64);
                    let done = next.clone().filter("done", move |_t, x| *x >= threshold);
                    let again = next.filter("again", move |_t, x| *x < threshold);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                });
                output
                    .exchange_to("gather", 1, |x: &i64| *x as u64)?
                    .output("results")?;
                Ok(())
            },
            SpawnOptions::new().per_stage_parallelism(true),
        )
        .unwrap();
    let sender = multi.take_input::<i64>(0, "data").unwrap();
    sender.send(0, vec![1]).unwrap();
    drop(sender);
    drop(multi.take_input::<i64>(1, "data").unwrap());
    let mut receivers = Vec::new();
    for w in 0..multi.num_workers() {
        receivers.push(multi.take_output::<i64>(w, "results").unwrap());
    }
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    let join = rx.recv_timeout(Duration::from_secs(10));
    let mut all = Vec::new();
    for r in &receivers {
        while let Some(ev) = r.recv_timeout(Duration::from_millis(20)) {
            if let instancy::dataflow::OutputEvent::Data { data, .. } = ev {
                all.extend(data);
            }
        }
    }
    eprintln!("threshold={threshold} join={join:?} results={all:?}");
    (join, all)
}

/// Control: record enters on worker 0 and leaves on worker 0 (even exit value).
#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn control_loop_exit_on_entering_worker_completes() {
    let (join, all) = run(10);
    assert!(matches!(join, Ok(Ok(()))), "control: join={join:?}");
    assert_eq!(all, vec![10]);
}

/// Record enters the loop on worker 0 and leaves it on worker 1 (odd exit value).
#[test]
#[ignore = "expected failure (H14); see EXPECTED in tests/audit_repros.rs"]
fn loop_exit_on_other_worker_never_converges() {
    let (join, all) = run(11);
    assert!(
        matches!(join, Ok(Ok(()))),
        "staged loop never converged (join={join:?}, results={all:?})"
    );
    assert_eq!(all, vec![11]);
}
