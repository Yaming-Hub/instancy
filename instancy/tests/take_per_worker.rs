//! Regression tests for `take`: the limit is per worker (as documented), the
//! operator keeps draining its input so the dataflow completes, and a global
//! limit is obtained by routing to one worker first.

use std::time::Duration;

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn run(workers: usize, global: bool, per_worker: u64, limit: usize) -> Vec<Vec<u64>> {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let mut multi = rt
            .spawn_multi(
                "take",
                workers,
                move |b: &mut DataflowBuilder<u64>| {
                    let mut s = b.input::<u64>("data")?.map("id", |_t, x| x);
                    if global {
                        s = s.exchange("one", |_x: &u64| 0u8);
                    }
                    s.take("limit", limit).output("out")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let senders = multi.take_all_inputs::<u64>("data").unwrap();
        let outs = multi.take_all_outputs::<u64>("out").unwrap();
        for (w, tx) in senders.into_iter().enumerate() {
            for e in 0..per_worker {
                tx.send(e, vec![w as u64 * 1000 + e]).unwrap();
                tx.advance_to(e + 1).unwrap();
            }
        }
        let result = multi.join_blocking();
        let per_worker_out: Vec<Vec<u64>> = outs
            .into_iter()
            .map(|o| o.collect_data().into_iter().flat_map(|(_, d)| d).collect())
            .collect();
        let _ = done_tx.send(result.map(|_| per_worker_out));
    });
    done_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("take dataflow did not complete (it must keep draining its input)")
        .unwrap()
}

#[test]
fn take_limit_is_per_worker_and_completes() {
    let out = run(4, false, 10, 3);
    for (w, got) in out.iter().enumerate() {
        let w = w as u64;
        assert_eq!(
            got,
            &vec![w * 1000, w * 1000 + 1, w * 1000 + 2],
            "worker {w}"
        );
    }
}

#[test]
fn take_after_routing_to_one_worker_is_a_global_limit() {
    let out = run(4, true, 10, 3);
    let total: usize = out.iter().map(Vec::len).sum();
    assert_eq!(total, 3, "global limit emitted {out:?}");
    assert_eq!(
        out.iter().filter(|o| !o.is_empty()).count(),
        1,
        "all output on one worker: {out:?}"
    );
}
