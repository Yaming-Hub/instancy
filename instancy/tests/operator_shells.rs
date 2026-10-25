//! Construction by type (design §5.2): every operator the builder creates is
//! an operator shell, which counts every record at its ports, so no operator
//! can bypass the progress accounting. `OperatorFactory::build` checks each
//! operator it materializes in debug builds; this test materializes every kind
//! of operator the public builder can create, on one worker and on two, and
//! runs them to completion.

use std::sync::Arc;

use instancy::{
    DataflowBuilder, IterateResult, Pipe, Result, RuntimeConfig, RuntimeHandle, SpawnOptions,
};

/// Every operator-creating method of the builder and its pipes.
fn every_operator(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    let input = builder.input::<u64>("data")?;
    let source = builder.source("source", vec![(0u64, vec![100u64, 101])]);
    let async_source = builder.source_async("async-source", |sender| async move {
        sender.send(0, vec![200u64]).await?;
        Ok(())
    });
    let merged = input.merge(source)?;
    let all = Pipe::concat(vec![merged, async_source])?;

    let mapped = all
        .map("map", |_t, x| x + 1)
        .map_batch("map-batch", |_t, batch: Vec<u64>| batch)
        .flat_map("flat-map", |_t, x| vec![x])
        .try_flat_map("try-flat-map", |_t, x| Ok(vec![x]))
        .try_map_batch("try-map-batch", |_t, batch: Vec<u64>| Ok(batch))
        .filter("filter", |_t, _x| true)
        .inspect("inspect", |_t, _x| {})
        .inspect_batch("inspect-batch", |_t, _batch| {});

    let (left, right) = mapped.branch("branch", |_t, x| x % 2 == 0);
    let (fork_a, fork_b) = left.fork("fork");
    let routed = fork_a
        .exchange_by_hash("exchange-by-hash", |x: &u64| *x)
        .exchange("exchange", |x: &u64| *x)
        .rebalance("rebalance")
        .delay("delay", |t, _x| *t)
        .delay_batch("delay-batch", |t| *t);
    let broadcast = fork_b.broadcast("broadcast").gather("gather");
    broadcast.for_each("broadcast-sink", |_t, _x| {});

    let unary = routed.unary("unary", |input, output| {
        while let Some((time, data)) = input.next() {
            output.push_vec(time, data);
        }
        Ok(())
    });
    let notify = unary.unary_notify("unary-notify", |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            ctx.notify_at(time);
            output.push_vec(time, data);
        }
        while ctx.next_notification().is_some() {}
        Ok(())
    });
    let async_mapped = notify.unary_async(
        "unary-async",
        2,
        Arc::new(|_t: u64, batch: Vec<u64>| async move { Ok(batch) }),
    );
    let joined = async_mapped.binary(right, "binary", |a, b, output| {
        while let Some((time, data)) = a.next() {
            output.push_vec(time, data);
        }
        while let Some((time, data)) = b.next() {
            output.push_vec(time, data);
        }
        Ok(())
    })?;

    let looped = joined.iterate::<u32>("loop", 1u32, |var| {
        let stepped = var.map("step", |_t, x| x + 1);
        IterateResult {
            feedback: stepped.clone().filter("again", |_t, x| x % 3 != 0),
            output: stepped.filter("done", |_t, x| x % 3 == 0),
        }
    });

    let (looped, _probe) = looped.probe();
    let (a, b) = looped.fork("fan-out");
    a.count("count").for_each("count-sink", |_t, _n| {});
    let (c, d) = b.fork("fan-out-2");
    c.distinct("distinct")
        .reduce("reduce", |x, y| x.max(y))
        .fold("fold", 0u64, |acc, x| acc + x)
        .take("take", 10)
        .take_while("take-while", |_t, _x| true)
        .for_each_batch("batch-sink", |_t, _batch| {});

    let results = d.map("to-result", |_t, x| {
        if x % 2 == 0 {
            Ok(x)
        } else {
            Err(format!("odd {x}"))
        }
    });
    let (ok, err) = results
        .map_ok("map-ok", |_t, x| x)
        .filter_ok("filter-ok", |_t, _x| true)
        .and_then("and-then", |_t, x| Ok(x))
        .branch_result("branch-result");
    err.for_each("err-sink", |_t, _e| {});
    let staged = ok.exchange_to("exchange-to", 1, |x: &u64| *x)?;
    let _collected = staged.collect();
    builder
        .input::<u64>("data-2")?
        .rebalance_to("rebalance-to", 1)?
        .exchange_by_hash_to("exchange-by-hash-to", 1, |x: &u64| *x)?
        .output("out")?;
    Ok(())
}

fn run(workers: usize) {
    // `unary_async` spawns its tasks on the Tokio runtime current when the
    // dataflow is built.
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _context = tokio_rt.enter();
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            &format!("every-operator-{workers}"),
            workers,
            every_operator,
            SpawnOptions::default(),
        )
        .unwrap();
    let mut collectors = Vec::new();
    for worker in 0..workers {
        if let Ok(output) = multi.take_output::<u64>(worker, "out") {
            collectors.push(std::thread::spawn(move || output.collect_data()));
        }
        for name in ["data", "data-2"] {
            if let Ok(input) = multi.take_input::<u64>(worker, name) {
                input.send(0, vec![1, 2, 3, 4, 5, 6]).unwrap();
                input.send(1, vec![7, 8, 9]).unwrap();
            }
        }
    }
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(multi.join_blocking());
    });
    rx.recv_timeout(std::time::Duration::from_secs(60))
        .expect("the dataflow did not finish within 60 s")
        .expect("every operator kind runs to completion");
    let records: usize = collectors
        .into_iter()
        .map(|c| {
            c.join()
                .unwrap()
                .iter()
                .map(|(_, d)| d.len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(
        records,
        9 * workers,
        "`out` carries every record of `data-2`"
    );
}

#[test]
fn every_operator_kind_is_an_operator_shell_on_one_worker() {
    run(1);
}

#[test]
fn every_operator_kind_is_an_operator_shell_on_two_workers() {
    run(2);
}
