//! Adversarial tests for the clean cluster shutdown handshake.
//!
//! Each test tries to break the guarantee that a node never tears down while
//! a peer still needs its frames, in both transport modes: every finish
//! order, latency on single links, congested queues at shutdown, many
//! dataflows sharing managers, dropping handles at every stage, runtime
//! shutdown mid-teardown, and a peer crashing after the link was retired.

#![cfg(feature = "transport")]

#[path = "support/teardown.rs"]
mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::{
    CancellationReason, ClusterSpawnedDataflow, ClusterTopology, DataflowBuilder, DataflowId,
    InputSender, NodeConfig, Result, RuntimeHandle, SpawnOptions,
};

use support::{
    Collector, Mesh, collect, drop_runtimes, fmt_join, join_with_timeout, spawn_node_with_options,
};

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Dedicated,
    /// Per-dataflow shared managers: the dataflow holds the last reference.
    Shared,
    /// Long-lived shared managers (a connection pool) that outlive every
    /// dataflow, so nothing but the protocol itself ends a link.
    SharedPooled,
}

const MODES: [Mode; 3] = [Mode::Dedicated, Mode::Shared, Mode::SharedPooled];

/// Frontier-driven per-epoch sum: needs every node's final capability release.
fn notify_sum(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i64>("data")?
        .exchange("by_val", |x: &i64| *x as u64)
        .unary_notify("sum", {
            let mut stash: HashMap<u64, i64> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    *stash.entry(time).or_default() += data.iter().sum::<i64>();
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(s) = stash.remove(&time) {
                        output.push_vec(time, vec![s]);
                    }
                }
                Ok(())
            }
        })
        .output("out")?;
    Ok(())
}

struct Node {
    name: String,
    rt: RuntimeHandle,
    cluster: Option<ClusterSpawnedDataflow<u64>>,
    inputs: Vec<InputSender<u64, i64>>,
    outs: Vec<Collector<i64>>,
}

struct Cluster {
    mesh: Mesh,
    nodes: Vec<Node>,
    /// `SharedPooled`: the managers, kept alive until `shutdown`.
    pool: Vec<support::Managers>,
}

impl Cluster {
    async fn start(
        spec: &[(&str, usize)],
        mode: Mode,
        capacity: usize,
        latency: impl Fn(&str, &str) -> Duration,
        options: impl Fn(&str) -> SpawnOptions,
    ) -> Self {
        Self::start_with(spec, mode, capacity, latency, options, notify_sum).await
    }

    async fn start_with(
        spec: &[(&str, usize)],
        mode: Mode,
        capacity: usize,
        latency: impl Fn(&str, &str) -> Duration,
        options: impl Fn(&str) -> SpawnOptions,
        build: fn(&mut DataflowBuilder<u64>) -> Result<()>,
    ) -> Self {
        let names: Vec<&str> = spec.iter().map(|(n, _)| *n).collect();
        let topology = ClusterTopology::multi_node(
            spec.iter().map(|(n, w)| NodeConfig::new(*n, *w)).collect(),
        )
        .unwrap();
        let mut mesh = Mesh::new(&names, 1 << 16, latency);
        let id = DataflowId::new();
        let mut handles = Vec::new();
        let mut pool = Vec::new();
        for name in &names {
            let handle = match mode {
                Mode::Dedicated => spawn_node_with_options(
                    topology.clone(),
                    name,
                    id,
                    mesh.dedicated(name, capacity),
                    2,
                    build,
                    options(name),
                ),
                Mode::Shared | Mode::SharedPooled => {
                    let managers = mesh.managers(name);
                    if mode == Mode::SharedPooled {
                        pool.push(Arc::clone(&managers));
                    }
                    spawn_node_with_options(
                        topology.clone(),
                        name,
                        id,
                        ClusterSpawnTransport::shared(managers, capacity),
                        2,
                        build,
                        options(name),
                    )
                }
            };
            handles.push((name.to_string(), handle));
        }
        let mut nodes = Vec::new();
        for (name, handle) in handles {
            let (rt, cluster) = handle.await.unwrap();
            let mut cluster = cluster.unwrap();
            let workers = cluster.num_local_workers();
            let outs = (0..workers)
                .map(|w| collect(cluster.take_output::<i64>(w, "out").unwrap()))
                .collect();
            let inputs = (0..workers)
                .map(|w| cluster.take_input::<i64>(w, "data").unwrap())
                .collect();
            nodes.push(Node {
                name,
                rt,
                cluster: Some(cluster),
                inputs,
                outs,
            });
        }
        Self { mesh, nodes, pool }
    }

    fn node(&mut self, name: &str) -> &mut Node {
        self.nodes.iter_mut().find(|n| n.name == name).unwrap()
    }

    /// Node `name` sends `values` at epoch 0 (spread over its workers) and
    /// closes its inputs.
    fn send_and_close(&mut self, name: &str, values: std::ops::Range<i64>) {
        let node = self.node(name);
        let workers = node.inputs.len() as i64;
        for (w, input) in node.inputs.drain(..).enumerate() {
            let mine: Vec<i64> = values.clone().filter(|v| v % workers == w as i64).collect();
            if !mine.is_empty() {
                input.send(0, mine).unwrap();
            }
        }
    }

    fn join(
        &mut self,
        name: &str,
        timeout: Duration,
    ) -> tokio::task::JoinHandle<Option<Result<()>>> {
        join_with_timeout(self.node(name).cluster.take().unwrap(), timeout)
    }

    /// Collect every node's output sum; errors if an output never closed.
    fn total(&mut self) -> std::result::Result<i64, String> {
        let mut total = 0;
        for node in &mut self.nodes {
            for out in node.outs.drain(..) {
                total += out.join()?.into_iter().map(|(_, v)| v).sum::<i64>();
            }
        }
        Ok(total)
    }

    async fn shutdown(self) {
        drop_runtimes(self.nodes.into_iter().map(|n| n.rt).collect()).await;
        drop(self.pool);
    }

    fn leak(self) {
        std::mem::forget(self.nodes);
        std::mem::forget(self.pool);
    }
}

fn all_ok(results: &[(String, Option<Result<()>>)]) -> bool {
    results.iter().all(|(_, r)| matches!(r, Some(Ok(()))))
}

fn describe(results: &[(String, Option<Result<()>>)]) -> String {
    results
        .iter()
        .map(|(n, r)| format!("{n}={}", fmt_join(r)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Close inputs in `order` with `gap` between nodes while every completion is
/// awaited concurrently; returns a failure description, if any.
async fn finish_in_order(
    spec: &[(&str, usize)],
    mode: Mode,
    order: &[&str],
    gap: Duration,
    latency: impl Fn(&str, &str) -> Duration,
) -> Option<String> {
    let mut cluster = Cluster::start(spec, mode, 1024, latency, |_| SpawnOptions::new()).await;
    let names: Vec<String> = spec.iter().map(|(n, _)| n.to_string()).collect();
    let joins: Vec<_> = names
        .iter()
        .map(|n| (n.clone(), cluster.join(n, Duration::from_secs(20))))
        .collect();
    let mut expected = 0;
    for (i, name) in order.iter().enumerate() {
        let lo = i as i64 * 100;
        expected += (lo..lo + 50).sum::<i64>();
        cluster.send_and_close(name, lo..lo + 50);
        tokio::time::sleep(gap).await;
    }
    let mut results = Vec::new();
    for (n, j) in joins {
        results.push((n, j.await.unwrap()));
    }
    if !all_ok(&results) {
        let msg = format!("{mode:?} order {order:?}: {}", describe(&results));
        cluster.leak();
        return Some(msg);
    }
    let total = cluster.total();
    cluster.shutdown().await;
    match total {
        Ok(t) if t == expected => None,
        other => Some(format!(
            "{mode:?} order {order:?}: total {other:?} != {expected}"
        )),
    }
}

fn permutations(items: &[&'static str]) -> Vec<Vec<&'static str>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let first = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, first);
            out.push(tail);
        }
    }
    out
}

/// Every finish order of 3 nodes, in both modes, with a slow link that
/// rotates with the order.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_nodes_every_finish_order() {
    let spec = [("a", 1), ("b", 2), ("c", 1)];
    let mut failures = Vec::new();
    for mode in MODES {
        for (i, order) in permutations(&["a", "b", "c"]).into_iter().enumerate() {
            let slow = [("a", "b"), ("b", "c"), ("c", "a")][i % 3];
            let latency = move |from: &str, to: &str| {
                if (from, to) == slow {
                    Duration::from_millis(150)
                } else {
                    Duration::ZERO
                }
            };
            if let Some(f) =
                finish_in_order(&spec, mode, &order, Duration::from_millis(60), latency).await
            {
                failures.push(f);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Sampled finish orders of 4 nodes, each with latency injected on a single,
/// different link (both directions), in both modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn four_nodes_sampled_orders_with_single_slow_link() {
    let spec = [("a", 2), ("b", 1), ("c", 1), ("d", 2)];
    let all = permutations(&["a", "b", "c", "d"]);
    let links = [
        ("a", "b"),
        ("a", "c"),
        ("a", "d"),
        ("b", "c"),
        ("b", "d"),
        ("c", "d"),
    ];
    let mut failures = Vec::new();
    for mode in MODES {
        for (i, order) in all.iter().step_by(4).enumerate() {
            let slow = links[i % links.len()];
            let latency = move |from: &str, to: &str| {
                if (from, to) == slow || (to, from) == slow {
                    Duration::from_millis(120)
                } else {
                    Duration::ZERO
                }
            };
            if let Some(f) =
                finish_in_order(&spec, mode, order, Duration::from_millis(30), latency).await
            {
                failures.push(f);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The first finisher's outbound links are congested (paused, tiny queues)
/// at the moment it completes. Its completion must wait; once the links
/// recover every node completes with the full result.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn congested_links_at_shutdown_delay_but_never_lose_the_goodbye() {
    for mode in MODES {
        let spec = [("a", 1), ("b", 1), ("c", 1)];
        let mut cluster = Cluster::start(
            &spec,
            mode,
            2,
            |_, _| Duration::ZERO,
            |_| SpawnOptions::new(),
        )
        .await;
        // b and c close first; a sends data to everyone and closes last.
        cluster.send_and_close("b", 0..0);
        cluster.send_and_close("c", 0..0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let valves = [cluster.mesh.valve("a", "b"), cluster.mesh.valve("a", "c")];
        for v in &valves {
            v.pause();
        }
        let ja = cluster.join("a", Duration::from_secs(20));
        let jb = cluster.join("b", Duration::from_secs(20));
        let jc = cluster.join("c", Duration::from_secs(20));
        cluster.send_and_close("a", 0..300);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        if ja.is_finished() || jb.is_finished() || jc.is_finished() {
            let mut finished = Vec::new();
            for (n, j) in [("a", ja), ("b", jb), ("c", jc)] {
                if j.is_finished() {
                    finished.push(format!("{n}={}", fmt_join(&j.await.unwrap())));
                }
            }
            panic!("{mode:?}: completed while a's frames were stuck: {finished:?}");
        }
        for v in &valves {
            v.resume();
        }
        let results = vec![
            ("a".to_string(), ja.await.unwrap()),
            ("b".to_string(), jb.await.unwrap()),
            ("c".to_string(), jc.await.unwrap()),
        ];
        assert!(all_ok(&results), "{mode:?}: {}", describe(&results));
        assert_eq!(cluster.total(), Ok((0..300).sum::<i64>()), "{mode:?}");
        cluster.shutdown().await;
    }
}

/// Pipeline without an exchange: each node's workers finish on their own.
fn pipeline(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i64>("data")?
        .map("id", |_t, x| x)
        .output("out")?;
    Ok(())
}

/// A node whose workers have finished resolves its join while a peer is still
/// running (input open): the peer acknowledges the goodbye without finishing
/// itself, so sequential joins never deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn early_finisher_join_does_not_wait_for_peer_completion() {
    for mode in MODES {
        let spec = [("a", 1), ("b", 1)];
        let mut cluster = Cluster::start_with(
            &spec,
            mode,
            1024,
            |_, _| Duration::ZERO,
            |_| SpawnOptions::new(),
            pipeline,
        )
        .await;
        cluster.send_and_close("a", 0..10);
        let a = cluster.node("a").cluster.take().unwrap();
        let blocking = tokio::task::spawn_blocking(move || a.join_blocking());
        let ra = tokio::time::timeout(Duration::from_secs(10), blocking)
            .await
            .map(|r| r.unwrap());
        assert!(
            matches!(ra, Ok(Ok(()))),
            "{mode:?}: a must resolve while b is still running: {ra:?}"
        );
        // b is still running; only now does it finish.
        cluster.send_and_close("b", 1000..1010);
        let rb = cluster.join("b", Duration::from_secs(10)).await.unwrap();
        assert!(matches!(rb, Some(Ok(()))), "{mode:?}: b {}", fmt_join(&rb));
        assert_eq!(
            cluster.total(),
            Ok((0..10).sum::<i64>() + (1000..1010).sum::<i64>())
        );
        cluster.shutdown().await;
    }
}

/// Many dataflows share the same managers; one finishes immediately (and its
/// completion handles are dropped right away) while the others keep
/// exchanging data and finish later.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_managers_many_dataflows_one_finishes_early() {
    let names = ["a", "b", "c"];
    let mut mesh = Mesh::new(&names, 1 << 16, |_, _| Duration::ZERO);
    let managers: HashMap<&str, support::Managers> =
        names.iter().map(|n| (*n, mesh.managers(n))).collect();
    let topology =
        ClusterTopology::multi_node(names.iter().map(|n| NodeConfig::new(*n, 1)).collect())
            .unwrap();
    let mut dataflows = Vec::new();
    for _ in 0..6 {
        let id = DataflowId::new();
        let mut nodes = Vec::new();
        for n in names {
            nodes.push(spawn_node_with_options(
                topology.clone(),
                n,
                id,
                ClusterSpawnTransport::shared(Arc::clone(&managers[n]), 1024),
                1,
                notify_sum,
                SpawnOptions::new(),
            ));
        }
        let mut spawned = Vec::new();
        for h in nodes {
            let (rt, c) = h.await.unwrap();
            spawned.push((rt, c.unwrap()));
        }
        dataflows.push(spawned);
    }
    let mut rts = Vec::new();
    let mut pending = Vec::new();
    for (i, nodes) in dataflows.into_iter().enumerate() {
        let mut outs = Vec::new();
        let mut inputs = Vec::new();
        let mut handles = Vec::new();
        for (rt, mut c) in nodes {
            outs.push(collect(c.take_output::<i64>(0, "out").unwrap()));
            inputs.push(c.take_input::<i64>(0, "data").unwrap());
            handles.push(c);
            rts.push(rt);
        }
        if i == 0 {
            // Finishes immediately; completions detached right away.
            drop(inputs);
            for c in handles {
                drop(c.join().unwrap());
            }
            pending.push((i, Vec::new(), outs, 0i64));
        } else {
            let feeder = std::thread::spawn(move || {
                for e in 0..40u64 {
                    for (k, input) in inputs.iter().enumerate() {
                        let v = (e * 10 + k as u64) as i64;
                        input.send(e, vec![v, v + 1000]).unwrap();
                        input.advance_to(e + 1).unwrap();
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            let joins: Vec<_> = handles
                .into_iter()
                .map(|c| join_with_timeout(c, Duration::from_secs(30)))
                .collect();
            let expected: i64 = (0..40u64)
                .flat_map(|e| (0..3u64).map(move |k| (e * 10 + k) as i64))
                .map(|v| 2 * v + 1000)
                .sum();
            feeder.join().unwrap();
            pending.push((i, joins, outs, expected));
        }
    }
    let mut failures = Vec::new();
    for (i, joins, outs, expected) in pending {
        let mut results = Vec::new();
        for (k, j) in joins.into_iter().enumerate() {
            results.push((names[k].to_string(), j.await.unwrap()));
        }
        if !all_ok(&results) {
            failures.push(format!("dataflow {i}: {}", describe(&results)));
            continue;
        }
        let total: std::result::Result<i64, String> = outs
            .into_iter()
            .map(|o| o.join().map(|v| v.into_iter().map(|(_, x)| x).sum::<i64>()))
            .sum();
        if total != Ok(expected) {
            failures.push(format!("dataflow {i}: total {total:?} != {expected}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
    drop_runtimes(rts).await;
}

/// Dropping handles at every stage:
/// - dropping `ClusterSpawnedDataflow` before `join()` cancels the dataflow on
///   every node (no orphaned workers, peers see `PeerCancelled`);
/// - dropping the completion before, during or after local completion
///   detaches it and the dataflow finishes normally everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn dropping_handles_at_every_stage() {
    for mode in MODES {
        // Stage 0: handle dropped without join.
        let spec = [("a", 1), ("b", 1)];
        let mut cluster = Cluster::start(
            &spec,
            mode,
            1024,
            |_, _| Duration::ZERO,
            |_| SpawnOptions::new(),
        )
        .await;
        drop(cluster.node("a").cluster.take());
        let rb = cluster.join("b", Duration::from_secs(10)).await.unwrap();
        // Shared mode: when the dropped handle's lifecycle task releases the
        // last manager reference, the manager aborts its writers and the
        // queued Cancel can be lost; the peer is then cancelled by the
        // transport failure instead. Either way it must be cancelled.
        let cancelled = matches!(
            (mode, &rb),
            (
                _,
                Some(Err(instancy::Error::Cancelled {
                    reason: Some(CancellationReason::PeerCancelled { .. }),
                })),
            ) | (Mode::Shared, Some(Err(instancy::Error::Cancelled { .. })))
        );
        assert!(
            cancelled,
            "{mode:?}: peer of a dropped handle must be cancelled, got {}",
            fmt_join(&rb)
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while cluster.node("a").rt.active_dataflows() > 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            cluster.node("a").rt.active_dataflows(),
            0,
            "{mode:?}: orphaned workers"
        );
        cluster.shutdown().await;

        // Stages 1-3: completion dropped before inputs close, while the peer
        // is still running, and after it resolved.
        for stage in 1..=3 {
            let mut cluster = Cluster::start(
                &spec,
                mode,
                1024,
                |_, _| Duration::ZERO,
                |_| SpawnOptions::new(),
            )
            .await;
            let completion_a = cluster.node("a").cluster.take().unwrap().join().unwrap();
            match stage {
                1 => {
                    drop(completion_a);
                    cluster.send_and_close("a", 0..20);
                }
                2 => {
                    cluster.send_and_close("a", 0..20);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(completion_a);
                }
                _ => {
                    cluster.send_and_close("a", 0..20);
                    cluster.send_and_close("b", 20..40);
                    let r = tokio::time::timeout(Duration::from_secs(10), completion_a).await;
                    assert!(matches!(r, Ok(Ok(()))), "{mode:?} stage 3: {r:?}");
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            if stage != 3 {
                cluster.send_and_close("b", 20..40);
            }
            let rb = cluster.join("b", Duration::from_secs(10)).await.unwrap();
            assert!(
                matches!(rb, Some(Ok(()))),
                "{mode:?} stage {stage}: b {}",
                fmt_join(&rb)
            );
            assert_eq!(
                cluster.total(),
                Ok((0..40).sum::<i64>()),
                "{mode:?} stage {stage}"
            );
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while cluster.node("a").rt.active_dataflows() > 0
                && std::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(cluster.node("a").rt.active_dataflows(), 0);
            cluster.shutdown().await;
        }
    }
}

/// `cancel()` on one node races with the whole cluster completing: the cancel
/// is issued at many points around completion time, from a rotating node, in
/// both modes. Whichever wins, every node must resolve (`Ok` or `Cancelled`)
/// and release its workers; in particular, a node whose workers still succeed
/// after receiving a peer's `Cancel` must not wait for a goodbye or ack from a
/// peer that already tore down.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_racing_completion_never_hangs() {
    let spec = [("a", 1), ("b", 2), ("c", 1)];
    let mut failures = Vec::new();
    for mode in MODES {
        for step in 0..48u64 {
            let delay = Duration::from_micros(step * 250);
            let canceller = ["a", "b", "c"][step as usize % 3];
            let mut cluster = Cluster::start(
                &spec,
                mode,
                1024,
                |_, _| Duration::ZERO,
                |_| SpawnOptions::new(),
            )
            .await;
            let handle = cluster.node(canceller).cluster.take().unwrap();
            let joins: Vec<_> = ["a", "b", "c"]
                .iter()
                .filter(|n| **n != canceller)
                .map(|n| (n.to_string(), cluster.join(n, Duration::from_secs(15))))
                .collect();
            for (i, name) in ["a", "b", "c"].iter().enumerate() {
                let lo = i as i64 * 100;
                cluster.send_and_close(name, lo..lo + 50);
            }
            tokio::time::sleep(delay).await;
            handle.cancel();
            let mut results = vec![(
                canceller.to_string(),
                join_with_timeout(handle, Duration::from_secs(15))
                    .await
                    .unwrap(),
            )];
            for (n, j) in joins {
                results.push((n, j.await.unwrap()));
            }
            let resolved = results
                .iter()
                .all(|(_, r)| matches!(r, Some(Ok(()) | Err(instancy::Error::Cancelled { .. }))));
            if !resolved {
                failures.push(format!(
                    "{mode:?} delay {delay:?} cancel@{canceller}: {}",
                    describe(&results)
                ));
                cluster.leak();
                continue;
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let active: usize = cluster.nodes.iter().map(|n| n.rt.active_dataflows()).sum();
                if active == 0 {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    failures.push(format!(
                        "{mode:?} delay {delay:?} cancel@{canceller}: {active} dataflows still active ({})",
                        describe(&results)
                    ));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            cluster.shutdown().await;
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

fn drain_on_cancel_at_a(node: &str) -> SpawnOptions {
    if node == "a" {
        SpawnOptions::new().drain_on_cancel(Duration::from_secs(20))
    } else {
        SpawnOptions::new()
    }
}

fn slow_pipeline(builder: &mut DataflowBuilder<u64>) -> Result<()> {
    builder
        .input::<i64>("data")?
        .map("slow", |_t, x| {
            std::thread::sleep(Duration::from_millis(400));
            x
        })
        .output("out")?;
    Ok(())
}

/// node-b is cancelled while node-a (which drains on cancel) is busy; node-a's
/// workers then still succeed. node-b tore down without a goodbye, so node-a
/// must not wait for one (in Shared mode nothing else from node-b could ever
/// resolve that wait).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_cancel_followed_by_local_drain_success_does_not_hang() {
    for mode in MODES {
        let spec = [("a", 1), ("b", 1)];
        let mut cluster = Cluster::start_with(
            &spec,
            mode,
            1024,
            |_, _| Duration::ZERO,
            drain_on_cancel_at_a,
            slow_pipeline,
        )
        .await;
        let ja = cluster.join("a", Duration::from_secs(10));
        // node-a's only worker is busy in the slow map when the Cancel lands,
        // so it reports success well after the Cancel was processed.
        cluster.send_and_close("a", 0..1);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let b = cluster.node("b").cluster.take().unwrap();
        b.cancel();
        let rb = join_with_timeout(b, Duration::from_secs(10)).await.unwrap();
        assert!(
            matches!(rb, Some(Err(instancy::Error::Cancelled { .. }))),
            "{mode:?}: b {}",
            fmt_join(&rb)
        );
        let ra = ja.await.unwrap();
        if !matches!(ra, Some(Ok(()))) {
            let msg = format!("{mode:?}: a {}", fmt_join(&ra));
            cluster.leak();
            panic!("{msg}");
        }
        cluster.shutdown().await;
    }
}

/// node-a broadcasts Cancel after node-b already finished (node-b's goodbye
/// is stuck behind a paused link, and node-b, already draining, does not
/// echo the Cancel). node-a's workers drain and succeed: having told every
/// peer the dataflow is cancelled, it must not wait for their goodbyes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_success_after_broadcasting_cancel_does_not_wait_for_peers() {
    for mode in MODES {
        let spec = [("a", 1), ("b", 1)];
        let mut cluster = Cluster::start_with(
            &spec,
            mode,
            1024,
            |_, _| Duration::ZERO,
            drain_on_cancel_at_a,
            pipeline,
        )
        .await;
        let valve = cluster.mesh.valve("b", "a");
        valve.pause();
        let jb = cluster.join("b", Duration::from_secs(15));
        cluster.send_and_close("b", 0..10);
        // node-b finishes; its goodbye to node-a is held by the valve.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let a = cluster.node("a").cluster.take().unwrap();
        a.cancel();
        // node-b receives the Cancel (a -> b is open) and stops waiting for us.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let ja = join_with_timeout(a, Duration::from_secs(5));
        cluster.send_and_close("a", 0..10);
        let ra = ja.await.unwrap();
        valve.resume();
        let rb = jb.await.unwrap();
        if !matches!(ra, Some(Ok(()))) || !matches!(rb, Some(Ok(()))) {
            let msg = format!("{mode:?}: a {} b {}", fmt_join(&ra), fmt_join(&rb));
            cluster.leak();
            panic!("{msg}");
        }
        cluster.shutdown().await;
    }
}

/// A node's instancy runtime is dropped (or its dataflow cancelled through the
/// user token) after its workers finished, while its goodbye is still stuck
/// on a congested link. That must neither cancel the peers nor lose the
/// goodbye.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn local_shutdown_mid_teardown_does_not_cancel_peers() {
    for mode in MODES {
        for via_token in [false, true] {
            let token = tokio_util::sync::CancellationToken::new();
            let spec = [("a", 1), ("b", 1)];
            let a_token = token.clone();
            let mut cluster = Cluster::start(
                &spec,
                mode,
                1024,
                |_, _| Duration::ZERO,
                |n| {
                    if n == "a" {
                        SpawnOptions::new().cancellation_token(a_token.clone())
                    } else {
                        SpawnOptions::new()
                    }
                },
            )
            .await;
            cluster.send_and_close("b", 1000..1010);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let a_to_b = cluster.mesh.valve("a", "b");
            a_to_b.pause();
            let ja = cluster.join("a", Duration::from_secs(20));
            let jb = cluster.join("b", Duration::from_secs(20));
            cluster.send_and_close("a", 0..10);
            tokio::time::sleep(Duration::from_millis(500)).await;
            if via_token {
                token.cancel();
            } else {
                // Shut down a's instancy runtime (its workers are done).
                let node = cluster
                    .nodes
                    .iter_mut()
                    .position(|n| n.name == "a")
                    .unwrap();
                let rt = std::mem::replace(&mut cluster.nodes[node].rt, support::runtime(1));
                drop_runtimes(vec![rt]).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            a_to_b.resume();
            let (ra, rb) = (ja.await.unwrap(), jb.await.unwrap());
            assert!(
                matches!(rb, Some(Ok(()))),
                "{mode:?} token={via_token}: peer must complete, got {}",
                fmt_join(&rb)
            );
            assert!(
                matches!(ra, Some(Ok(()))),
                "{mode:?} token={via_token}: a {}",
                fmt_join(&ra)
            );
            assert_eq!(
                cluster.total(),
                Ok((0..10).sum::<i64>() + (1000..1010).sum::<i64>()),
                "{mode:?} token={via_token}"
            );
            cluster.shutdown().await;
        }
    }
}

/// The tokio runtime hosting a node's transport shuts down mid-teardown. Its
/// blocked `wait()` must return instead of hanging forever.
#[test]
fn tokio_runtime_shutdown_mid_teardown_releases_waiters() {
    let peer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let node_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let topology =
        ClusterTopology::multi_node(vec![NodeConfig::new("a", 1), NodeConfig::new("b", 1)])
            .unwrap();
    let id = DataflowId::new();
    let (mut mesh, valve) = peer_rt.block_on(async {
        let mesh = Mesh::new(&["a", "b"], 1 << 16, |_, _| Duration::ZERO);
        let valve = mesh.valve("a", "b");
        (mesh, valve)
    });
    let (ta, tb) =
        peer_rt.block_on(async { (mesh.dedicated("a", 1024), mesh.dedicated("b", 1024)) });
    let rt_a = support::runtime(1);
    let rt_b = support::runtime(1);
    let node_handle = node_rt.handle().clone();
    let peer_handle = peer_rt.handle().clone();
    let (ca, cb) = std::thread::scope(|s| {
        let a = s.spawn(|| {
            rt_a.spawn_cluster(
                "rt-drop",
                topology.clone(),
                "a",
                id,
                ta,
                Duration::from_secs(10),
                pipeline,
                &node_handle,
                SpawnOptions::new(),
            )
        });
        let b = s.spawn(|| {
            rt_b.spawn_cluster(
                "rt-drop",
                topology.clone(),
                "b",
                id,
                tb,
                Duration::from_secs(10),
                pipeline,
                &peer_handle,
                SpawnOptions::new(),
            )
        });
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    let (mut ca, mut cb) = (ca, cb);
    let _oa = collect(ca.take_output::<i64>(0, "out").unwrap());
    let _ob = collect(cb.take_output::<i64>(0, "out").unwrap());
    // b keeps running (input open), so it never sends its own goodbye.
    let _b_input = cb.take_input::<i64>(0, "data").unwrap();
    std::thread::sleep(Duration::from_millis(200));
    valve.pause();
    let sa = ca.take_input::<i64>(0, "data").unwrap();
    // No exchange: a's workers finish while a→b is paused, leaving a waiting
    // for b's acknowledgement.
    sa.send(0, vec![0, 2, 4]).unwrap();
    drop(sa);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ca.join_blocking());
    });
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        rx.try_recv().is_err(),
        "a cannot finish its handshake while paused"
    );
    node_rt.shutdown_background();
    let result = rx.recv_timeout(Duration::from_secs(10));
    assert!(
        result.is_ok(),
        "waiter must be released when the runtime dies"
    );
    // b lost its peer before the goodbye; cancel it to clean up.
    cb.cancel();
    drop(cb);
    drop(rt_b);
    peer_rt.shutdown_background();
    std::mem::forget(rt_a);
}

/// After a node retired every link (its peers acknowledged its goodbye), its
/// connections die abruptly, like a process exiting right after `join`. The
/// peers are still running and must neither fail nor hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn peer_vanishing_after_retirement_is_benign() {
    for mode in MODES {
        let spec = [("a", 1), ("b", 1), ("c", 1)];
        // Without an exchange, a's workers finish while b and c keep running.
        let mut cluster = Cluster::start_with(
            &spec,
            mode,
            1024,
            |_, _| Duration::ZERO,
            |_| SpawnOptions::new(),
            pipeline,
        )
        .await;
        let jb = cluster.join("b", Duration::from_secs(20));
        let jc = cluster.join("c", Duration::from_secs(20));
        // a finishes; b and c keep their inputs open.
        cluster.send_and_close("a", 0..30);
        let ra = cluster.join("a", Duration::from_secs(10)).await.unwrap();
        assert!(matches!(ra, Some(Ok(()))), "{mode:?}: a {}", fmt_join(&ra));
        // a "crashes" after its successful join.
        cluster.mesh.sever("a", "b");
        cluster.mesh.sever("a", "c");
        tokio::time::sleep(Duration::from_millis(300)).await;
        cluster.send_and_close("b", 100..110);
        cluster.send_and_close("c", 200..210);
        let results = vec![
            ("b".to_string(), jb.await.unwrap()),
            ("c".to_string(), jc.await.unwrap()),
        ];
        assert!(all_ok(&results), "{mode:?}: {}", describe(&results));
        let expected = (0..30).sum::<i64>() + (100..110).sum::<i64>() + (200..210).sum::<i64>();
        assert_eq!(cluster.total(), Ok(expected), "{mode:?}");
        cluster.shutdown().await;
    }
}
