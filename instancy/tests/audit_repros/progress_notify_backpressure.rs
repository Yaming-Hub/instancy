//! Audit: capability released while output is still buffered by backpressure.
//!
//! `NotifyContext::next_notification` drops the output capability for `t`
//! immediately, but the records the operator emits for `t` may remain in its
//! `pending_output` queue when the downstream channel is full. The tracker
//! then advances the downstream frontier past `t` while records at `t` are
//! still queued; when they are finally delivered, downstream notifications for
//! `t` have already fired. (Documented as a "Known limitation" on
//! `next_notification`, but reachable with a small `with_capacity`.)

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};
use std::collections::HashMap;
use std::time::Duration;

const BURST: u64 = 50;

#[test]
#[ignore = "audit repro; run by audit_repros_match_table (tests/audit_repros.rs)"]
fn downstream_aggregate_not_split_under_backpressure() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("audit-notify-backpressure");
    let input = builder.input::<u64>("data").unwrap();
    input
        .unary_notify("burst", {
            let mut seen: HashMap<u64, ()> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, _data)) = input.next() {
                    seen.insert(time, ());
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if seen.remove(&time).is_some() {
                        for i in 0..BURST {
                            output.push_vec(time, vec![i]);
                        }
                    }
                }
                Ok(())
            }
        })
        .with_capacity(2)
        .unwrap()
        .unary_notify("count", {
            let mut stash: HashMap<u64, u64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    *stash.entry(time).or_default() += data.len() as u64;
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    let n = stash.remove(&time).unwrap_or(0);
                    output.push_vec(time, vec![(time, n)]);
                }
                Ok(())
            }
        })
        .output("out")
        .unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let s = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<(u64, u64)>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());
    for e in 0..3u64 {
        s.send(e, vec![e]).unwrap();
        s.advance_to(e + 1).unwrap();
    }
    std::thread::sleep(Duration::from_millis(300));
    drop(s);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(h.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("join hang")
        .expect("join error");
    let mut recs: Vec<(u64, u64)> = collector
        .join()
        .unwrap()
        .into_iter()
        .flat_map(|(_, d)| d)
        .collect();
    recs.sort();
    assert_eq!(
        recs,
        vec![(0, BURST), (1, BURST), (2, BURST)],
        "downstream per-epoch aggregate was split: frontier passed t while records at t were still buffered upstream"
    );
}
