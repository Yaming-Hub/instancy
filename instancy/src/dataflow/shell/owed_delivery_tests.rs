//! A record the output owes is delivered by a later flush, and holds the
//! frontier until then.
//!
//! A [`CountedOutput`] counts a batch as produced when it takes it, before
//! the channel accepts it. The exchange buffers nothing unbounded, so a
//! record is owed only when a full bounded target refuses it; the counted
//! output keeps it for a later flush. By then its produced count may already
//! have been drained into the progress tracker. These tests guard against an
//! output that drops a drained produced count, so the frontier passes a
//! record that was never delivered (completion before delivery), and against
//! one that skips the flush because the produced ledger is empty, so the
//! owed record is never delivered.
//!
//! The check is made on the output port and the progress trackers; the full
//! runtime path is a documented limit. The real pieces are connected
//! directly:
//!
//! - a [`CountedOutput`] whose produced ledger and capability reporter are
//!   worker 0's, pushing through a real [`ExchangePush`] (wrapped only to
//!   record each attempt and its result) into real bounded targets of
//!   capacity 1, target `s` read by worker `s`'s [`CountedInput`];
//! - two real progress trackers (workers 0 and 1, both running the producer
//!   and the target operator) joined by in-process progress channels.
//!
//! Each test fills every target through the counted output, has the full
//! exchange refuse the selected record `[v]` at 1, consumes the fill,
//! releases the source capabilities and settles both trackers. Both views
//! must then hold the target's frontier at exactly `{1}`, with the owed
//! record's `+1` as the only count, while the record is still pending. The
//! next flush must deliver it once; then both trackers complete.
//!
//! Each failure message says what went wrong: the frontier passed an
//! undelivered record (completion before delivery), or the owed record was
//! not delivered by the flush after the produced ledger was drained.
//!
//! These component tests attach no progress checker oracle (it observes a
//! whole dataflow); the trackers' frontiers and counts are the oracle.

use std::sync::{Arc, Mutex};

use super::{CountedInput, CountedOutput, ShellToken};
use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::dataflow::channels::exchange_channel::{ExchangePush, SharedWakeRegistry};
use crate::dataflow::channels::pact::ExchangeFn;
use crate::dataflow::channels::pushpull::sealed::Sealed;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::spsc::spsc_channel;
use crate::dataflow::channels::wake::WakeHandle;
use crate::dataflow::plan::PhysicalPlan;
use crate::error::{Error, Result};
use crate::progress::change_batch::ChangeBatch;
use crate::progress::frontier::Antichain;
use crate::progress::operate::PortConnectivity;
use crate::progress::progress_channel::create_progress_channels;
use crate::progress::reachability::Location;
use crate::progress::subgraph::{ProgressTracker, SubgraphBuilder};

/// The producer and the target operator in every tracker.
const PRODUCER: usize = 1;
const TARGET: usize = 2;
/// The time of the selected record.
const OWED_AT: u64 = 1;

/// One attempt of the exchange, as the counted output made it.
#[derive(Clone, Debug, PartialEq)]
enum Attempt {
    Accepted {
        time: u64,
        data: Vec<u64>,
    },
    Refused {
        time: u64,
        data: Vec<u64>,
        error: String,
    },
}

/// Delegates to the real exchange and records every attempt and its result.
struct Recorded {
    exchange: ExchangePush<u64, u64>,
    attempts: Arc<Mutex<Vec<Attempt>>>,
}

impl Recorded {
    fn record(&self, envelope: &Envelope<u64, u64>, result: std::result::Result<(), &Error>) {
        let Payload::Data { time, data } = &envelope.payload else {
            return;
        };
        let (time, data) = (*time, data.clone());
        let attempt = match result {
            Ok(()) => Attempt::Accepted { time, data },
            Err(error) => Attempt::Refused {
                time,
                data,
                error: error.to_string(),
            },
        };
        self.attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(attempt);
    }
}

impl Sealed for Recorded {}

impl Push<u64, u64> for Recorded {
    fn push(&mut self, envelope: Envelope<u64, u64>) -> Result<()> {
        self.try_push(envelope).map_err(|(error, _)| error)
    }

    fn try_push(
        &mut self,
        envelope: Envelope<u64, u64>,
    ) -> std::result::Result<(), (Error, Envelope<u64, u64>)> {
        let copy = envelope.clone();
        let result = self.exchange.try_push(envelope);
        self.record(
            &copy,
            result.as_ref().map(|_| ()).map_err(|(error, _)| error),
        );
        result
    }

    fn flush(&mut self) -> Result<()> {
        self.exchange.flush()
    }

    fn close(&mut self) {
        self.exchange.close();
    }

    fn is_closed(&self) -> bool {
        self.exchange.is_closed()
    }
}

/// Both workers' trackers, initialized from the plan: each declares its own
/// producer capability at 0, so each view starts with both.
fn trackers() -> Vec<ProgressTracker<u64>> {
    let channels =
        create_progress_channels::<u64>(2, &[WakeHandle::new(), WakeHandle::new()]).unwrap();
    let plan = PhysicalPlan::for_hosts(2, &[(PRODUCER, 2), (TARGET, 2)]);
    let mut trackers = Vec::new();
    for channels in channels {
        let mut builder = SubgraphBuilder::<u64>::new(1, 1);
        let mut held = ChangeBatch::new();
        held.update(0u64, 1);
        builder
            .add_operator_with_capabilities(
                PRODUCER,
                "producer",
                1,
                1,
                PortConnectivity::identity(0u64),
                vec![held],
            )
            .unwrap();
        builder
            .add_operator(TARGET, "target", 1, 1, PortConnectivity::identity(0u64))
            .unwrap();
        builder.add_edge(Location::source(0, 0), Location::target(PRODUCER, 0));
        builder.add_edge(Location::source(PRODUCER, 0), Location::target(TARGET, 0));
        builder.add_edge(Location::source(TARGET, 0), Location::target(0, 0));
        let mut tracker = builder.build();
        tracker.set_progress_channels(channels).unwrap();
        trackers.push(tracker);
    }
    for (worker, tracker) in trackers.iter_mut().enumerate() {
        tracker.initialize_for_plan(&plan, None, worker).unwrap();
    }
    settle(&mut trackers);
    trackers
}

/// Propagate every tracker until the peers' changes have been applied.
fn settle(trackers: &mut [ProgressTracker<u64>]) {
    for _round in 0..3 {
        for tracker in trackers.iter_mut() {
            tracker.propagate().expect("propagate");
        }
    }
}

/// Everything `input` pulls from its channel, consumed and committed.
fn consume(input: &mut CountedInput<u64, u64>, tok: &ShellToken) -> Vec<(u64, Vec<u64>)> {
    input.pull(tok).expect("pull");
    let mut batches = Vec::new();
    while let Some(batch) = input.handle().next() {
        batches.push(batch);
    }
    input.commit(tok);
    batches
}

fn attempts(log: &Arc<Mutex<Vec<Attempt>>>) -> Vec<Attempt> {
    log.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

/// The owed-delivery scenario with `targets` capacity-1 targets and the
/// selected record `value` at 1.
fn owed_delivery(targets: usize, value: u64) {
    let tok = ShellToken(());
    let mut trackers = trackers();

    // The data plane: the exchange routes `x` to target `x % targets`;
    // target `s` is read by worker `s`.
    let registry = Arc::new(SharedWakeRegistry::new(targets));
    let mut pushes: Vec<Box<dyn Push<u64, u64>>> = Vec::new();
    let mut inputs = Vec::new();
    for (slot, tracker) in trackers.iter().enumerate().take(targets) {
        let (push, pull) = spsc_channel::<u64, u64, ()>(1);
        pushes.push(Box::new(push));
        let consumed = tracker.operator_progress(TARGET).unwrap().consumed[0].clone();
        inputs.push(CountedInput::new(
            format!("target.in0 on worker {slot}"),
            Box::new(pull) as Box<dyn Pull<u64, u64>>,
            consumed,
        ));
    }
    let log = Arc::new(Mutex::new(Vec::new()));
    let exchange = Recorded {
        exchange: ExchangePush::new(
            pushes,
            ExchangeFn::new("x % targets", |x: &u64| *x),
            registry,
        ),
        attempts: Arc::clone(&log),
    };
    let producer = trackers[0].operator_progress(PRODUCER).unwrap();
    let produced = producer.produced[0].clone();
    let mut output = CountedOutput::new(
        "producer.out0 on worker 0".into(),
        Box::new(exchange),
        produced.clone(),
        producer.reporter(0).clone(),
        None,
    );

    // 1. Fill every target through the counted output: `[100 + s]` at 0
    //    for target `s`, taken and flushed at once.
    let fill: Vec<u64> = (100..100 + targets as u64).collect();
    output.handle().push_vec(0, fill.clone());
    output.take_session(&tok, None).expect("take the fill");
    assert!(
        output.flush(&tok).expect("flush the fill"),
        "the fill must be accepted"
    );
    assert_eq!(
        attempts(&log),
        vec![Attempt::Accepted {
            time: 0,
            data: fill.clone()
        }],
        "the fill goes out in one accepted attempt"
    );

    // 2. The selected record, taken and flushed at once: its target is full.
    output.handle().push_vec(OWED_AT, vec![value]);
    output
        .take_session(&tok, None)
        .expect("take the selected record");
    let flushed = output.flush(&tok).expect("flush the selected record");
    let refused = Attempt::Refused {
        time: OWED_AT,
        data: vec![value],
        error: Error::Backpressure.to_string(),
    };
    assert!(!flushed, "a full target must refuse [{value}] at {OWED_AT}");
    assert_eq!(
        attempts(&log)[1..],
        *std::slice::from_ref(&refused),
        "one refused attempt"
    );
    assert!(
        output.has_pending(),
        "the refused record stays owed by the output"
    );

    // 3. Consume only the fill, release both workers' capabilities at 0 and
    //    settle both views.
    for (slot, input) in inputs.iter_mut().enumerate() {
        assert_eq!(
            consume(input, &tok),
            vec![(0, vec![100 + slot as u64])],
            "target {slot} holds exactly its fill"
        );
    }
    for tracker in &trackers {
        tracker
            .operator_progress(PRODUCER)
            .unwrap()
            .reporter(0)
            .update(0, -1);
    }
    settle(&mut trackers);

    // 4. Both views hold the target at the owed record's time.
    let owed = format!("{:?}@{OWED_AT}: +1", Location::target(TARGET, 0));
    let pending = output.has_pending();
    let views: Vec<String> = trackers
        .iter()
        .enumerate()
        .map(|(worker, tracker)| {
            format!(
                "worker {worker}: target frontier {:?}, residual {:?}, completed {}",
                tracker.input_frontier(TARGET, 0),
                tracker.scope_residual(),
                tracker.is_completed()
            )
        })
        .collect();
    let held = trackers.iter().all(|tracker| {
        *tracker.input_frontier(TARGET, 0) == Antichain::from_elem(OWED_AT)
            && tracker.scope_residual() == [owed.clone()]
            && !tracker.is_completed()
    });
    assert!(
        held,
        "the frontier passed an undelivered record: a view passed the owed record [{value}] at \
         {OWED_AT} while it is still pending in the output ({pending}); each view must hold \
         the target at exactly {{{OWED_AT}}} with only `{owed}`: {}",
        views.join("; ")
    );
    assert!(
        produced.is_empty(),
        "the tracker drained the producer's ledger"
    );
    assert!(pending, "the record is still owed");
    assert_eq!(
        attempts(&log)[1..],
        [refused],
        "nothing delivered the record yet"
    );

    // 5. One more flush: the target has room, the record goes out once.
    let flushed = output.flush(&tok).expect("flush the owed record");
    let pending = output.has_pending();
    let new = attempts(&log)[2..].to_vec();
    assert!(
        flushed
            && !pending
            && new
                == [Attempt::Accepted {
                    time: OWED_AT,
                    data: vec![value],
                }],
        "the owed record was not delivered: the flush after the produced ledger was drained did \
         not deliver [{value}] at {OWED_AT}: returned {flushed}, still pending {pending}, \
         new attempts {new:?}"
    );
    let routed = (value % targets as u64) as usize;
    for (slot, input) in inputs.iter_mut().enumerate() {
        let want = if slot == routed {
            vec![(OWED_AT, vec![value])]
        } else {
            Vec::new()
        };
        assert_eq!(consume(input, &tok), want, "target {slot} after the retry");
    }
    settle(&mut trackers);
    for (worker, tracker) in trackers.iter().enumerate() {
        assert!(
            tracker.input_frontier(TARGET, 0).is_empty(),
            "worker {worker}: target frontier {:?} after the delivery",
            tracker.input_frontier(TARGET, 0)
        );
        assert_eq!(
            tracker.scope_residual(),
            Vec::<String>::new(),
            "worker {worker}: counts after the delivery"
        );
        assert!(tracker.is_completed(), "worker {worker} did not complete");
    }
    assert_eq!(attempts(&log).len(), 3, "no further attempt");
}

/// One target: the owed `[0]` at 1 holds both views at `{1}` until the next
/// flush delivers it once.
#[test]
fn owed_record_holds_frontier_until_flushed_to_one_target_value_0() {
    owed_delivery(1, 0);
}

/// One target: the owed `[1]` at 1 holds both views at `{1}` until the next
/// flush delivers it once.
#[test]
fn owed_record_holds_frontier_until_flushed_to_one_target_value_1() {
    owed_delivery(1, 1);
}

/// Two targets: the owed `[0]` at 1, routed to worker 0's target, holds both
/// views at `{1}` until the next flush delivers it once.
#[test]
fn owed_record_holds_frontier_until_flushed_to_target_0_of_2() {
    owed_delivery(2, 0);
}

/// Two targets: the owed `[1]` at 1, routed to worker 1's target, holds both
/// views at `{1}` until the next flush delivers it once.
#[test]
fn owed_record_holds_frontier_until_flushed_to_target_1_of_2() {
    owed_delivery(2, 1);
}
