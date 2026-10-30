//! The exchange hands a target only the records routed to it.
//!
//! [`ExchangePush`] partitions each batch into one bucket per target and
//! delivers only the non-empty buckets. An empty batch, or the empty share of
//! a target in a non-empty batch, must never reach that target, wake it, or
//! be refused because that target is full. These tests guard against an
//! exchange that forwards empty buckets, on the first call or on a later one
//! made while the targets are full, and against one that is left owing a
//! retry, holding records or closed afterwards, which would break the data
//! sent next.
//!
//! Dataflow-level tests cannot reach this path: the output handle drops empty
//! batches before the exchange. Here batches go straight into an
//! [`ExchangePush`] whose targets are real bounded channels, with a
//! registered wake handle each.
//!
//! Each failure message says what went wrong: an empty batch reached a
//! target, an empty batch was refused because a target was full, or a
//! target with nothing in the batch received an envelope.

use super::*;
use crate::dataflow::channels::pushpull::sealed::Sealed;

/// A prefill record, per target.
const SENTINEL: u64 = 1000;

/// A target as the exchange sees it: the real bounded channel's push end,
/// shared with the test so it can fill the target directly, without a call
/// through the exchange.
struct Target(Arc<Mutex<SpscPush<u64, u64, ()>>>);

impl Target {
    fn lock(&self) -> std::sync::MutexGuard<'_, SpscPush<u64, u64, ()>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Sealed for Target {}

impl Push<u64, u64> for Target {
    fn push(&mut self, envelope: Envelope<u64, u64>) -> Result<()> {
        self.lock().push(envelope)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<u64, u64>,
    ) -> std::result::Result<(), (Error, Envelope<u64, u64>)> {
        self.lock().try_push(envelope)
    }

    fn flush(&mut self) -> Result<()> {
        self.lock().flush()
    }

    fn close(&mut self) {
        self.lock().close();
    }

    fn close_on_drop(&mut self) {
        self.lock().close_on_drop();
    }

    fn is_closed(&self) -> bool {
        self.lock().is_closed()
    }
}

struct Fixture {
    push: ExchangePush<u64, u64>,
    /// The test's handle on each target's push end.
    setup: Vec<Arc<Mutex<SpscPush<u64, u64, ()>>>>,
    pulls: Vec<SpscPull<u64, u64, ()>>,
    wakes: Vec<WakeHandle>,
}

/// An exchange over `targets` bounded channels of `capacity` envelopes,
/// routing record `x` to target `x % targets`.
fn fixture(targets: usize, capacity: usize) -> Fixture {
    let registry = Arc::new(SharedWakeRegistry::new(targets));
    let mut boxed: Vec<Box<dyn Push<u64, u64>>> = Vec::new();
    let mut setup = Vec::new();
    let mut pulls = Vec::new();
    let mut wakes = Vec::new();
    for target in 0..targets {
        let wake = WakeHandle::new();
        registry.register(target, Some(wake.clone()));
        // A new handle starts notified; clear it so a wake is a fresh one.
        assert!(wake.take_notification());
        let (push, pull) = spsc_channel::<u64, u64, ()>(capacity);
        let push = Arc::new(Mutex::new(push));
        boxed.push(Box::new(Target(Arc::clone(&push))));
        setup.push(push);
        pulls.push(pull);
        wakes.push(wake);
    }
    let push = ExchangePush::new(boxed, ExchangeFn::new("identity", |x: &u64| *x), registry);
    Fixture {
        push,
        setup,
        pulls,
        wakes,
    }
}

impl Fixture {
    fn targets(&self) -> usize {
        self.pulls.len()
    }

    /// Everything `target` received since the last call.
    fn received(&mut self, target: usize) -> Vec<Envelope<u64, u64>> {
        let mut envelopes = Vec::new();
        while let Some(envelope) = self.pulls[target].pull() {
            envelopes.push(envelope);
        }
        envelopes
    }

    /// Whether `target` was woken since the last call.
    fn woken(&self, target: usize) -> bool {
        self.wakes[target].take_notification()
    }

    fn is_full(&self, target: usize) -> bool {
        let push = self.setup[target].lock().unwrap_or_else(|e| e.into_inner());
        push.len() == push.capacity()
    }

    /// Nothing owed, nothing buffered, nothing closed.
    fn assert_idle(&self, after: &str) {
        assert!(
            self.push.pending.is_none(),
            "{after}: the exchange still owes {:?}",
            self.push.pending
        );
        assert!(
            self.push.buckets.iter().all(Vec::is_empty),
            "{after}: records left in the buckets: {:?}",
            self.push.buckets
        );
        assert!(!self.push.closed, "{after}: the exchange closed");
        for (target, setup) in self.setup.iter().enumerate() {
            let closed = setup.lock().unwrap_or_else(|e| e.into_inner()).is_closed();
            assert!(!closed, "{after}: target {target} closed");
        }
    }

    /// Push `envelope` until every target accepted its part, draining the
    /// targets between attempts and retrying the same envelope, as the
    /// exchange requires. Adds every target's records to `into`.
    fn deliver(&mut self, mut envelope: Envelope<u64, u64>, into: &mut [Vec<(u64, u64)>]) {
        for _attempt in 0..16 {
            match self.push.try_push(envelope) {
                Ok(()) => {
                    self.drain_into(into);
                    return;
                }
                Err((Error::Backpressure, returned)) => {
                    self.drain_into(into);
                    envelope = returned;
                }
                Err((error, _)) => panic!("data push failed: {error}"),
            }
        }
        panic!("data push still refused after 16 attempts");
    }

    fn drain_into(&mut self, into: &mut [Vec<(u64, u64)>]) {
        for (target, records) in into.iter_mut().enumerate() {
            for envelope in self.received(target) {
                let (time, data) = envelope.as_data().expect("a data envelope");
                assert!(
                    !data.is_empty(),
                    "target {target} received an empty envelope"
                );
                records.extend(data.iter().map(|x| (*time, *x)));
            }
        }
    }
}

/// The data sent after the empty batches: `[0, 1, 2, 3]` at 0 and `[4, 5]`
/// at 1, each record exactly once at target `x % targets`, at its time; then
/// flush and close with nothing left over.
fn deliver_real_data(f: &mut Fixture) {
    let targets = f.targets();
    let mut records = vec![Vec::new(); targets];
    f.deliver(Envelope::data(0, vec![0, 1, 2, 3]), &mut records);
    f.deliver(Envelope::data(1, vec![4, 5]), &mut records);
    for (target, got) in records.iter_mut().enumerate() {
        got.sort_unstable();
        let want: Vec<(u64, u64)> = [(0, 0), (0, 1), (0, 2), (0, 3), (1, 4), (1, 5)]
            .into_iter()
            .filter(|(_, x)| *x % targets as u64 == target as u64)
            .collect();
        assert_eq!(*got, want, "target {target}: routed records");
    }
    f.assert_idle("after the data");
    f.push.flush().expect("flush");
    f.push.close();
    for target in 0..targets {
        let rest = f.received(target);
        assert!(rest.is_empty(), "target {target}: left over {rest:?}");
        assert!(f.pulls[target].is_exhausted(), "target {target} not closed");
    }
}

/// A whole empty batch into `targets` targets of `capacity`: into empty
/// targets, then (capacity 1) into full ones, then real data. The empty batch
/// must reach no target, wake none, never be refused, and leave the exchange
/// idle so the real data is routed exactly once.
fn empty_case(targets: usize, capacity: usize) {
    let mut f = fixture(targets, capacity);

    // Data call 1: an empty batch while every target is empty.
    if let Err((error, _)) = f.push.try_push(Envelope::data(0, vec![])) {
        panic!("an empty batch into empty targets failed: {error}");
    }
    for target in 0..targets {
        let got = f.received(target);
        assert!(
            got.is_empty(),
            "an empty batch reached a target: target {target} received {got:?}"
        );
    }
    for target in 0..targets {
        assert!(!f.woken(target), "an empty batch woke target {target}");
    }
    f.assert_idle("after an empty batch into empty targets");

    if capacity == 1 {
        // Data call 2: an empty batch while every target is full.
        for (target, setup) in f.setup.iter().enumerate() {
            let prefill = Envelope::data(0, vec![SENTINEL + target as u64]);
            let mut push = setup.lock().unwrap_or_else(|e| e.into_inner());
            if push.try_push(prefill).is_err() {
                panic!("prefilling target {target} failed");
            }
        }
        for target in 0..targets {
            assert!(
                f.is_full(target),
                "target {target} not full after its prefill"
            );
            // Only the empty batch's wakes count.
            f.woken(target);
        }
        if let Err((error, _)) = f.push.try_push(Envelope::data(0, vec![])) {
            panic!(
                "an empty batch was refused because a target \
                 was full: {error}"
            );
        }
        f.assert_idle("after an empty batch into full targets");
        for target in 0..targets {
            assert!(!f.woken(target), "an empty batch woke full target {target}");
        }
        for target in 0..targets {
            let got = f.received(target);
            assert_eq!(
                got,
                vec![Envelope::data(0, vec![SENTINEL + target as u64])],
                "an empty batch reached a full target: target {target} must hold only its prefill"
            );
        }
    }
    deliver_real_data(&mut f);
}

/// One target of capacity 1: an empty batch skips it while it is empty and
/// while it is full.
#[test]
fn empty_batch_skips_one_target_of_capacity_1() {
    empty_case(1, 1);
}

/// One target with room to spare: an empty batch skips it.
#[test]
fn empty_batch_skips_one_target_of_capacity_1024() {
    empty_case(1, 1024);
}

/// Two targets of capacity 1: an empty batch skips both while they are empty
/// and while they are full.
#[test]
fn empty_batch_skips_two_targets_of_capacity_1() {
    empty_case(2, 1);
}

/// Two targets with room to spare: an empty batch skips both.
#[test]
fn empty_batch_skips_two_targets_of_capacity_1024() {
    empty_case(2, 1024);
}

/// The case that occurs at runtime: a non-empty batch with no records for
/// one target. `[0, 2]` at 0 over two capacity-1 targets goes entirely to
/// target 0, once; target 1 must receive nothing and not be woken.
#[test]
fn target_with_no_records_in_a_batch_is_skipped() {
    let mut f = fixture(2, 1);
    if let Err((error, _)) = f.push.try_push(Envelope::data(0, vec![0, 2])) {
        panic!("[0, 2] at 0 into empty targets failed: {error}");
    }
    let got = f.received(1);
    assert!(
        got.is_empty(),
        "a target with nothing in the batch received an envelope: target 1 has no records in [0, 2] at 0 but received {got:?}"
    );
    assert!(
        !f.woken(1),
        "target 1 was woken by a batch with nothing for it"
    );
    assert_eq!(
        f.received(0),
        vec![Envelope::data(0, vec![0, 2])],
        "target 0 receives the whole batch once"
    );
    assert!(f.woken(0), "target 0 must be woken for its records");
    f.assert_idle("after the skewed batch");
    deliver_real_data(&mut f);
}
