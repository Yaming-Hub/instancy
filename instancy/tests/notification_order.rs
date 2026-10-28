//! Regression tests: notifications are delivered in timestamp order, and an
//! operator cannot request a notification (capability) at a time it does not
//! hold.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::order::{PartialOrder, Product};
use instancy::progress::frontier::Antichain;
use instancy::progress::notificator::Notificator;
use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

const TIMEOUT: Duration = Duration::from_secs(30);

fn with_timeout<R: Send + 'static>(
    what: &str,
    timeout: Duration,
    f: impl FnOnce() -> R + Send + 'static,
) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(timeout) {
        Ok(r) => r,
        Err(_) => panic!("{what}: did not finish within {timeout:?} (hang)"),
    }
}

fn assert_linear_extension<T: PartialOrder + std::fmt::Debug>(got: &[T]) {
    for i in 0..got.len() {
        for j in i + 1..got.len() {
            assert!(
                !got[j].less_than(&got[i]),
                "delivered {:?} before {:?} although {:?} < {:?}; order = {:?}",
                got[i],
                got[j],
                got[j],
                got[i],
                got
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Moved repros (audit M9: delivery order)
// ---------------------------------------------------------------------------

#[test]
fn notificator_delivers_in_timestamp_order_total_order() {
    let mut n = Notificator::new(Antichain::from_elem(0u64));
    for t in [3u64, 5, 7] {
        n.notify_at(t);
    }
    n.update_frontier(&Antichain::from_elem(10));
    let mut got = Vec::new();
    while let Some(f) = n.next() {
        got.push(f.into_time());
    }
    assert_eq!(got, vec![3, 5, 7], "notifications delivered out of order");
}

#[test]
fn notificator_delivery_respects_partial_order() {
    let mut n = Notificator::new(Antichain::from_elem(Product::new(0u64, 0u32)));
    // (0,1) < (0,2) < (0,3) in the partial order; request in that order.
    for t in [
        Product::new(0u64, 1u32),
        Product::new(0, 2),
        Product::new(0, 3),
    ] {
        n.notify_at(t);
    }
    n.update_frontier(&Antichain::new());
    let mut got = Vec::new();
    while let Some(f) = n.next() {
        got.push(f.into_time());
    }
    assert_eq!(got.len(), 3);
    assert_linear_extension(&got);
}

/// End-to-end: a running-total operator (state carried across epochs) that
/// relies on notifications arriving in epoch order.
#[test]
fn unary_notify_running_total_is_correct_when_several_epochs_complete_together() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-order");
    let input = builder.input::<u64>("data").unwrap();
    input
        .unary_notify("running-total", {
            let mut stash: HashMap<u64, u64> = Default::default();
            let mut running = 0u64;
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    *stash.entry(time).or_default() += data.iter().sum::<u64>();
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    running += stash.remove(&time).unwrap_or(0);
                    output.push_vec(time, vec![(time, running)]);
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
    // Epoch e contributes value 1 each; all of them complete at once.
    for e in 0..6u64 {
        s.send(e, vec![1]).unwrap();
    }
    std::thread::sleep(Duration::from_millis(200));
    s.advance_to(100).unwrap();
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < 6 && Instant::now() < deadline {
        if let Some(OutputEvent::Data { data, .. }) = out.recv_timeout(Duration::from_millis(100)) {
            got.extend(data);
        }
    }
    drop(s);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    // Correct running total at epoch e is e+1.
    let expected: Vec<(u64, u64)> = (0..6).map(|e| (e, e + 1)).collect();
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(
        sorted, expected,
        "running totals per epoch wrong (emission order was {got:?})"
    );
}

// ---------------------------------------------------------------------------
// Adversarial: many epochs, Product times
// ---------------------------------------------------------------------------

/// Hundreds of epochs, requested in a scrambled order, complete in one
/// frontier update; the operator must see them in order.
#[test]
fn many_epochs_completing_together_are_delivered_in_order() {
    for round in 0..3u64 {
        let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
        let builder = DataflowBuilder::<u64>::new("many-epochs");
        let order: Arc<Mutex<Vec<u64>>> = Arc::default();
        let o2 = Arc::clone(&order);
        builder
            .input::<u64>("data")
            .unwrap()
            .unary_notify("record", move |input, output, ctx| {
                while let Some((t, d)) = input.next() {
                    ctx.notify_at(t);
                    output.push_vec(t, d);
                }
                while let Some(t) = ctx.next_notification() {
                    o2.lock().unwrap().push(t);
                }
                Ok(())
            })
            .output("out")
            .unwrap();
        let mut h = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let tx = h.take_input::<u64>("data").unwrap();
        let rx = h.take_output::<u64>("out").unwrap();
        let c = std::thread::spawn(move || rx.collect_data());
        // A permutation of 0..500 (stride 7 is coprime with 500).
        let epochs = 500u64;
        for i in 0..epochs {
            let e = (i * 7 + round) % epochs;
            tx.send(e, vec![e]).unwrap();
        }
        tx.advance_to(epochs).unwrap();
        drop(tx);
        let (joined, forwarded) =
            with_timeout("join", TIMEOUT, move || (h.join_blocking(), c.join()));
        joined.unwrap();
        let forwarded = forwarded.unwrap();
        let order = order.lock().unwrap();
        assert_eq!(*order, (0..epochs).collect::<Vec<_>>(), "round {round}");
        // Every batch is forwarded unchanged, at its own time.
        let mut forwarded: Vec<(u64, u64)> = forwarded
            .into_iter()
            .flat_map(|(t, d)| d.into_iter().map(move |x| (t, x)))
            .collect();
        forwarded.sort_unstable();
        assert_eq!(
            forwarded,
            (0..epochs).map(|e| (e, e)).collect::<Vec<_>>(),
            "round {round}"
        );
    }
}

/// Running totals over `Product` times, where the notificator must deliver a
/// linear extension of the partial order: totals are only meaningful along
/// chains, so check that each delivered time saw every smaller time first.
#[test]
fn product_time_notifications_respect_the_partial_order_end_to_end() {
    type P = Product<u64, u32>;
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<P>::new("product-order");
    let order: Arc<Mutex<Vec<P>>> = Arc::default();
    let o2 = Arc::clone(&order);
    builder
        .input::<u64>("data")
        .unwrap()
        .unary_notify("record", move |input, output, ctx| {
            while let Some((t, d)) = input.next() {
                ctx.notify_at(t);
                output.push_vec(t, d);
            }
            while let Some(t) = ctx.next_notification() {
                o2.lock().unwrap().push(t);
            }
            Ok(())
        })
        .output("out")
        .unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let c = std::thread::spawn(move || rx.collect_data());
    let mut times = Vec::new();
    for outer in (0..8u64).rev() {
        for inner in [5u32, 0, 3, 7, 1] {
            times.push(P::new(outer, inner));
        }
    }
    for t in &times {
        tx.send(*t, vec![1]).unwrap();
    }
    // Complete part of the grid, then the rest.
    tx.advance_to(P::new(4, 0)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    c.join().unwrap();
    let order = order.lock().unwrap();
    assert_eq!(order.len(), times.len());
    assert_linear_extension(&order);
}

// ---------------------------------------------------------------------------
// Moved repro (audit M9: notify_at at a past time) — now rejected
// ---------------------------------------------------------------------------

/// `notify_at(time)` for a time the input frontier has already passed (and the
/// operator holds nothing at or before) would mint a capability that moves the
/// downstream frontier backwards after a probe reported `done_with(time)`, and
/// let the operator emit data at that completed time. It must be rejected:
/// the operator fails with `Unjustified` (V3), the downstream frontier
/// never regresses, and no data is emitted at the completed time.
#[test]
fn notify_at_past_time_is_rejected_and_never_regresses_downstream_frontier() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("notify-past");
    let input = builder.input::<u64>("data").unwrap();
    let (s, probe) = input
        .unary_notify("late", {
            let mut requested = false;
            move |input, output, ctx| {
                while let Some((_time, _data)) = input.next() {
                    if !requested {
                        // Request a notification for a time that is already complete.
                        ctx.notify_at(0);
                        requested = true;
                    }
                }
                if input.is_exhausted() {
                    while let Some(t) = ctx.next_notification() {
                        output.push_vec(t, vec![t]);
                    }
                }
                Ok(())
            }
        })
        .probe();
    s.output("out").unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let sender = h.take_input::<u64>("data").unwrap();
    let out = h.take_output::<u64>("out").unwrap();
    let collector = std::thread::spawn(move || out.collect_data());

    sender.advance_to(5).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !probe.done_with(&0) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(probe.done_with(&0), "probe never passed 0");

    sender.send(5, vec![1]).unwrap();
    let mut regressed = None;
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        let f = probe.frontier();
        if f.less_equal(&0) {
            regressed = Some(f);
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(sender);
    let result = with_timeout("join", TIMEOUT, move || h.join_blocking());
    let data = collector.join().unwrap();
    assert!(
        regressed.is_none(),
        "downstream frontier regressed to {:?} after probe reported done_with(0)",
        regressed.map(|f| f.elements().to_vec())
    );
    assert!(
        data.iter().all(|(t, _)| *t >= 5),
        "operator emitted data at a time the downstream frontier had already passed: {data:?}"
    );
    let err = result.expect_err("an invalid notify_at must fail the dataflow");
    let msg = err.to_string();
    assert!(
        msg.contains("unjustified notify_at at 0"),
        "unexpected error: {msg}"
    );
}

/// `try_notify_at` reports the same rejection to the operator instead of
/// failing it, and valid requests (at a pending input time, or at/after a held
/// capability) still succeed after the frontier has moved on.
#[test]
fn try_notify_at_reports_invalid_times_and_accepts_held_ones() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("try-notify");
    let outcomes: Arc<Mutex<Vec<(u64, bool)>>> = Arc::default();
    let o2 = Arc::clone(&outcomes);
    let fired: Arc<Mutex<Vec<u64>>> = Arc::default();
    let f2 = Arc::clone(&fired);
    let (s, probe) = builder
        .input::<u64>("data")
        .unwrap()
        .unary_notify::<u64, _>("try", move |input, _output, ctx| {
            while let Some((t, _d)) = input.next() {
                // The input time itself, and a later time, are always valid.
                o2.lock().unwrap().push((t, ctx.try_notify_at(t).is_ok()));
                o2.lock()
                    .unwrap()
                    .push((t + 100, ctx.try_notify_at(t + 100).is_ok()));
                if t >= 5 {
                    // 1 is behind the frontier, the batch time and every held
                    // capability (102 and 105).
                    o2.lock().unwrap().push((1, ctx.try_notify_at(1).is_ok()));
                }
            }
            while let Some(t) = ctx.next_notification() {
                f2.lock().unwrap().push(t);
            }
            Ok(())
        })
        .probe();
    s.output("out").unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let c = std::thread::spawn(move || rx.collect_data());
    tx.send(2, vec![1]).unwrap();
    tx.advance_to(5).unwrap();
    // Once the probe passes 2, the notification at 2 has been consumed (its
    // capability dropped) and the operator's input frontier is 5.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !probe.done_with(&2) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(probe.done_with(&2));
    tx.send(5, vec![1]).unwrap();
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    c.join().unwrap();
    let outcomes = outcomes.lock().unwrap();
    assert_eq!(
        *outcomes,
        vec![(2, true), (102, true), (5, true), (105, true), (1, false)]
    );
    let fired = fired.lock().unwrap();
    assert_eq!(*fired, vec![2, 5, 102, 105]);
}

/// A notification handler may re-arm the time it is handling (here: emit a
/// large stash in chunks) or schedule a follow-up at a later time, even though
/// the frontier has passed both and `next_notification` already dropped the
/// handled time's capability: the capability is handed over within one
/// activation, so downstream frontiers never move backwards.
#[test]
fn notification_handler_can_rearm_and_delay_the_handled_time() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<u64>::new("rearm");
    let (s, probe) = builder
        .input::<u64>("data")
        .unwrap()
        .unary_notify::<u64, _>("chunked", {
            let mut stash: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
            let mut followed_up = std::collections::BTreeSet::new();
            move |input, output, ctx| {
                while let Some((t, d)) = input.next() {
                    stash.entry(t).or_default().extend(d);
                    ctx.notify_at(t);
                }
                while let Some(t) = ctx.next_notification() {
                    let mut chunk = stash.remove(&t).unwrap_or_default();
                    if chunk.len() > 2 {
                        let rest = chunk.split_off(2);
                        stash.insert(t, rest);
                        // Re-arm the handled time: emit the rest later.
                        ctx.notify_at(t);
                    } else if t < 100 && followed_up.insert(t) {
                        // A follow-up "timer" well past the input frontier.
                        ctx.notify_at(t + 100);
                    }
                    output.push_vec(t, chunk);
                }
                Ok(())
            }
        })
        .probe();
    let check = probe.clone();
    let early: Arc<Mutex<Vec<u64>>> = Arc::default();
    let e2 = Arc::clone(&early);
    s.inspect_batch("check", move |t: &u64, _| {
        if check.done_with(t) {
            e2.lock().unwrap().push(*t);
        }
    })
    .output("out")
    .unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let c = std::thread::spawn(move || rx.collect_data());
    tx.send(0, (0..7).collect()).unwrap();
    tx.send(1, vec![10, 11]).unwrap();
    tx.advance_to(50).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !probe.done_with(&49) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    // The follow-up timers at 100 and 101 still hold the stream open.
    assert!(probe.done_with(&49));
    assert!(!probe.done_with(&100));
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let got = c.join().unwrap();
    let mut by_time: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (t, d) in got {
        by_time.entry(t).or_default().extend(d);
    }
    assert_eq!(
        by_time,
        BTreeMap::from([(0, (0..7).collect::<Vec<_>>()), (1, vec![10, 11])])
    );
    assert!(
        early.lock().unwrap().is_empty(),
        "{:?}",
        early.lock().unwrap()
    );
    assert!(probe.is_done());
}

/// `delay` must reject delays to an earlier or incomparable time; with
/// `Product` times the lexicographic `Ord` alone would accept `(1,5) -> (2,0)`.
#[test]
fn delay_rejects_incomparable_product_times() {
    type P = Product<u64, u32>;
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<P>::new("delay-incomparable");
    builder
        .input::<u64>("data")
        .unwrap()
        .delay_batch("bad", |t: &P| P::new(t.outer + 1, 0))
        .output("out")
        .unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let c = std::thread::spawn(move || rx.collect_data());
    tx.send(P::new(1, 5), vec![1]).unwrap();
    drop(tx);
    let err = with_timeout("join", TIMEOUT, move || h.join_blocking())
        .expect_err("delay to an incomparable time must fail");
    assert!(
        err.to_string().contains("delay_fn must return a timestamp"),
        "{err}"
    );
    assert!(c.join().unwrap().is_empty());

    // The valid direction still works.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let builder = DataflowBuilder::<P>::new("delay-ok");
    builder
        .input::<u64>("data")
        .unwrap()
        .delay_batch("ok", |t: &P| P::new(t.outer + 1, t.inner))
        .output("out")
        .unwrap();
    let mut h = rt
        .spawn(builder.build().unwrap(), SpawnOptions::default())
        .unwrap();
    let tx = h.take_input::<u64>("data").unwrap();
    let rx = h.take_output::<u64>("out").unwrap();
    let c = std::thread::spawn(move || rx.collect_data());
    tx.send(P::new(1, 5), vec![1]).unwrap();
    drop(tx);
    with_timeout("join", TIMEOUT, move || h.join_blocking()).unwrap();
    let got: BTreeMap<P, Vec<u64>> = c.join().unwrap().into_iter().collect();
    assert_eq!(got, BTreeMap::from([(P::new(2, 5), vec![1])]));
}
