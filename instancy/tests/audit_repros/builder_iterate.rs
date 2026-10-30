//! Audit (builder/iterate): scope merging in `Pipe::iterate`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::order::Product;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

fn rt() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn join_spawned<T: instancy::progress::timestamp::Timestamp>(
    s: instancy::runtime::SpawnedDataflow<T>,
) -> instancy::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(s.join_blocking());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("dataflow did not complete within 20s (hang)")
}

/// Doubly-nested iterate WITHOUT any exchange. The docs claim nesting only
/// breaks when an exchange is present. `iterate` computes the feedback-factory
/// offset (`fb_offset`) *after* it already appended the body's feedback edges to
/// the parent graph, so the body's feedback factories are shifted by one slot
/// and the inner loop's feedback edge has no factory.
#[test]
fn audit_builder_nested_iterate_without_exchange() {
    let builder = DataflowBuilder::<u64>::new("nested-no-exchange");
    let input = builder.input::<i64>("data").unwrap();
    let out = input.iterate::<u32>("outer", 1u32, |outer| {
        let inner_out = outer.iterate::<u32>("inner", 1u32, |inner| {
            let stepped = inner.map("inc", |_t: &Product<Product<u64, u32>, u32>, x: i64| x + 1);
            let done = stepped.clone().filter("idone", |_t, x| *x % 5 == 0);
            let again = stepped.filter("iagain", |_t, x| *x % 5 != 0);
            IterateResult {
                feedback: again,
                output: done,
            }
        });
        let stepped = inner_out.map("plus1", |_t: &Product<u64, u32>, x: i64| x + 1);
        let done = stepped.clone().filter("odone", |_t, x| *x >= 20);
        let again = stepped.filter("oagain", |_t, x| *x < 20);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    out.output("results").unwrap();
    let df = builder.build().expect("build");
    eprintln!(
        "edges={} feedback_edges={}",
        df.edge_count(),
        df.feedback_edge_count()
    );

    let rt = rt();
    let mut spawned = match rt.spawn(df, SpawnOptions::default()) {
        Ok(s) => s,
        Err(e) => panic!("nested iterate (no exchange) failed to spawn: {e} / {e:?}"),
    };
    let tx = spawned.take_input::<i64>("data").unwrap();
    let rx = spawned.take_output::<i64>("results").unwrap();
    tx.send(0, vec![1, 7]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    let mut got: Vec<i64> = rx.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    got.sort();
    // 1 -> inner to 5 -> +1=6 -> inner to 10 -> 11 -> 15 -> 16 -> 20 -> 21 >= 20 done
    // 7 -> 10 -> 11 -> 15 -> 16 -> 20 -> 21
    assert_eq!(got, vec![21, 21]);
}

/// `output()` called inside an iterate body registers the port only in the
/// throw-away inner BuilderState; the port metadata/wiring is never merged, so
/// the named output is unreachable via `take_output`.
#[test]
fn audit_builder_output_inside_iterate_body_is_lost() {
    let builder = DataflowBuilder::<u64>::new("output-in-loop");
    let input = builder.input::<i64>("data").unwrap();
    let out = input.iterate::<u32>("loop", 1u32, |v| {
        let stepped = v.map("inc", |_t: &Product<u64, u32>, x: i64| x + 1);
        stepped
            .clone()
            .map("tap", |_t, x| x)
            .output("inner_tap")
            .expect("output inside loop");
        let done = stepped.clone().filter("done", |_t, x| *x >= 3);
        let again = stepped.filter("again", |_t, x| *x < 3);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    out.output("results").unwrap();
    let df = builder.build().expect("build");
    eprintln!("output_names = {:?}", df.output_names());
    let has_inner = df.output_names().contains(&"inner_tap");
    let rt = rt();
    let mut spawned = rt.spawn(df, SpawnOptions::default()).unwrap();
    let tap = spawned.take_output::<i64>("inner_tap");
    let tx = spawned.take_input::<i64>("data").unwrap();
    tx.send(0, vec![0]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    assert!(
        has_inner && tap.is_ok(),
        "output declared inside iterate body is not exposed: has_inner={has_inner}, take_output err={:?}",
        tap.err()
    );
}

/// `probe()` inside an iterate body: the probe is registered only in the inner
/// state and never merged, so it is never wired to a tracker.
#[test]
fn audit_builder_probe_inside_iterate_never_done() {
    let probe_slot: Arc<Mutex<Option<instancy::dataflow::probe::ProbeHandle<Product<u64, u32>>>>> =
        Arc::new(Mutex::new(None));
    let builder = DataflowBuilder::<u64>::new("probe-in-loop");
    let input = builder.input::<i64>("data").unwrap();
    let ps = probe_slot.clone();
    let out = input.iterate::<u32>("loop", 1u32, move |v| {
        let stepped = v.map("inc", |_t: &Product<u64, u32>, x: i64| x + 1);
        let (stepped, probe) = stepped.probe();
        *ps.lock().unwrap() = Some(probe);
        let done = stepped.clone().filter("done", |_t, x| *x >= 3);
        let again = stepped.filter("again", |_t, x| *x < 3);
        IterateResult {
            feedback: again,
            output: done,
        }
    });
    out.output("results").unwrap();
    let df = builder.build().expect("build");
    let rt = rt();
    let mut spawned = rt.spawn(df, SpawnOptions::default()).unwrap();
    let tx = spawned.take_input::<i64>("data").unwrap();
    let rx = spawned.take_output::<i64>("results").unwrap();
    tx.send(0, vec![0]).unwrap();
    drop(tx);
    join_spawned(spawned).unwrap();
    let got: Vec<i64> = rx.collect_data().into_iter().flat_map(|(_, d)| d).collect();
    assert_eq!(got, vec![3]);
    let probe = probe_slot.lock().unwrap().take().unwrap();
    assert!(
        probe.is_done(),
        "probe inside iterate body never reports done after the dataflow completed"
    );
}
