//! Progress conformance matrix (step 1 of the progress-accounting redesign).
//!
//! Each case builds one dataflow *shape* and runs it on one execution *path*,
//! *transport*, worker count and channel capacity. A case passes when:
//!
//! - the run completes (no hang within the timeout) and returns `Ok`, unless
//!   the shape must fail;
//! - the output is the exact expected multiset: nothing lost, nothing
//!   duplicated, and every per-epoch aggregate emitted once, whole;
//! - the progress checker ([`instancy::progress::check`]) recorded no V1,
//!   V4, V6 or V7 violation for the run, and actually observed it (an oracle
//!   was attached, every channel was wrapped, and on success the end-state
//!   check ran).
//!
//! Today's code fails many cases. How each case fails is recorded in the
//! *golden file*, `tests/progress_conformance.golden`: per case, the
//! failures every recorded run showed (`always`) and those only some runs
//! showed (`sometimes`), with violations located at an operator port
//! (`V7:frontier-ahead-of-work@loop::concat[in 1]`). A case the file does
//! not list must pass cleanly. A case must show every `always` failure and
//! nothing outside `always` and `sometimes`, with one exception: per-record
//! frontier violations (V1 `late-data`, V7 `frontier-ahead-of-work`) are
//! detected only when an interleaving exposes the frontier passing a record,
//! so whether and where they occur varies from run to run. They are not
//! required, and they are accepted at any location where some case of the
//! same shape and path showed them; a frontier violation at a location no
//! such case ever showed fails the case. Outcomes (hang, loss, duplicates,
//! split aggregates, errors) and every other violation are judged exactly.
//! `CONFORMANCE_STRICT=1` judges frontier violations exactly too.
//!
//! So a later step that fixes an outcome, or changes how a case fails (a
//! hang into a loss, a violation at a new location), fails the matrix until
//! the golden file is regenerated. A mismatching case is re-run up to
//! [`ATTEMPTS`] times and passes if any attempt matches (it is then reported
//! as `FLAKY`): a deterministic change always fails.
//!
//! [`EXPECTED_FAILURES`] explains the golden file by audit finding: every
//! failure the golden file records must be of a kind that a table entry
//! matching the case lists, and every kind an entry lists must occur in a
//! case it matches (the `golden_file_agrees_with_the_table` test, which
//! runs in CI).
//!
//! CI runs a bounded subset (the `ci_*` tests below). Run the whole matrix
//! with:
//!
//! ```text
//! cargo test -p instancy --all-features --test progress_conformance -- --ignored --test-threads=1 full_conformance_matrix
//! ```
//!
//! Set `CONFORMANCE_FILTER` to a substring of a case's name (for example
//! `Iterate/StageTasks`) to run only matching cases, and
//! `CONFORMANCE_REPORT=1` to print every case's outcome.
//!
//! # Regenerating the golden file
//!
//! Record at least three full runs (and, for the CI subset, runs pinned to
//! one core), then merge them:
//!
//! ```text
//! CONFORMANCE_RECORD=run1.txt cargo test -p instancy --all-features --test progress_conformance -- --ignored --test-threads=1 full_conformance_matrix
//! (repeat for run2.txt, run3.txt, ...)
//! CONFORMANCE_GOLDEN_INPUTS="run1.txt;run2.txt;run3.txt" cargo test -p instancy --all-features --test progress_conformance -- --ignored --exact regenerate_golden_file
//! ```
//!
//! In recording mode every case runs once and nothing is judged. Merging
//! refuses blind runs and failures no table entry explains. Every commit that
//! changes the golden file must cite, in its message, the audit finding that
//! explains each change.
#![cfg(all(
    feature = "transport",
    feature = "test-utils",
    any(debug_assertions, feature = "progress-check")
))]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::shared_pool::SharedConnectionConfig;
use instancy::communication::shared_transport::{
    ConnectionFactory, DynConnectionFactory, SharedPeerManager,
};
use instancy::communication::transport_session::PeerConnection;
use instancy::dataflow::dataflow_builder::IterateResult;
use instancy::dataflow::{InputSender, OutputReceiver};
use instancy::execute::NodeConfig;
use instancy::order::Product;
use instancy::progress::check::{self, Activation, Check, Mode};
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, MultiSpawnedDataflow, Pipe, Result,
    RuntimeConfig, RuntimeHandle, SimpleRuntime, SpawnOptions,
};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

// ===========================================================================
// The matrix's dimensions
// ===========================================================================

/// A dataflow shape (§5.8 of the design).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Shape {
    /// input → map → output.
    Pipeline,
    /// input → exchange → map → output.
    Exchange,
    /// input → exchange → exchange → output.
    ExchangeTwice,
    /// One output feeding two exchanges; the second feeds a per-epoch count
    /// (audit H12).
    FanoutExchanges,
    /// input → broadcast → output.
    Broadcast,
    /// Two inputs → binary → output.
    Binary,
    /// Two inputs → concat → output.
    Concat,
    /// input → branch → two outputs.
    Branch,
    /// input → fork → two outputs.
    Fork,
    /// input → `Result` → ok/err branch → two outputs.
    OkErr,
    /// input → gather → `unary_notify` sum per epoch.
    NotifySum,
    /// input → gather → reduce.
    Reduce,
    /// input → gather → count.
    Count,
    /// input → exchange → distinct.
    Distinct,
    /// input → delay by one epoch.
    Delay,
    /// input → `unary_async` (a suspended task) → gather → count.
    UnaryAsync,
    /// input → `unary_async` whose task fails: the run must fail.
    AsyncFailure,
    /// A loop without exchange.
    Iterate,
    /// A loop with an exchange inside, plus a count after the loop.
    IterateExchange,
    /// A loop inside a loop.
    NestedIterate,
    /// A long loop followed by a per-epoch count (audit H11).
    LoopThenCount,
    /// A finite loop feeding an operator that holds everything and flushes
    /// once its input frontier is empty (a stand-in for the planned
    /// `retain`). Must complete with all records.
    LoopThenFlush,
    /// The output receiver is read only after a pause (egress is full).
    OutputPaused,
    /// The output receiver is dropped at once (the run must still succeed).
    OutputDropped,
    /// Graceful drain on cancellation: everything accepted is delivered.
    DrainOnCancel,
    /// An operator acquires a notification at a time justified only by its
    /// input frontier. The design rejects this (audit P-9).
    NotifyAtFrontier,
}

const ALL_SHAPES: &[Shape] = &[
    Shape::Pipeline,
    Shape::Exchange,
    Shape::ExchangeTwice,
    Shape::FanoutExchanges,
    Shape::Broadcast,
    Shape::Binary,
    Shape::Concat,
    Shape::Branch,
    Shape::Fork,
    Shape::OkErr,
    Shape::NotifySum,
    Shape::Reduce,
    Shape::Count,
    Shape::Distinct,
    Shape::Delay,
    Shape::UnaryAsync,
    Shape::AsyncFailure,
    Shape::Iterate,
    Shape::IterateExchange,
    Shape::NestedIterate,
    Shape::LoopThenCount,
    Shape::LoopThenFlush,
    Shape::OutputPaused,
    Shape::OutputDropped,
    Shape::DrainOnCancel,
    Shape::NotifyAtFrontier,
];

/// How the dataflow is executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Path {
    /// Uniform executor, one operator per activation (U-unfused).
    Unfused,
    /// Uniform executor, one fused topological pass (U-fused).
    Fused,
    /// Uniform executor, per-stage tasks (U-stage-task, the default).
    StageTasks,
    /// Stages with different parallelism (S): the first exchange changes
    /// parallelism (2 → 1 or 2 → 3), on the uniform executor core.
    Staged,
    /// `SimpleRuntime` (SR).
    Simple,
}

/// Where the workers' channels run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Transport {
    /// In-process channels.
    InProcess,
    /// The in-memory mock network (every batch encoded and decoded).
    Mock,
    /// A two-node cluster over loopback TCP, one connection per dataflow.
    Dedicated,
    /// A two-node cluster over loopback TCP, pooled connections.
    Shared,
}

/// Worker count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Workers {
    One,
    Two,
    /// Three, unevenly placed: on a cluster one node has 1 worker and the
    /// other 2; on the staged path 2 source workers feed 3 downstream.
    Three,
}

/// Channel capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Cap {
    /// Every channel (and the cluster transport) holds one batch.
    One,
    /// The runtime's default.
    Default,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Case {
    shape: Shape,
    path: Path,
    transport: Transport,
    workers: Workers,
    cap: Cap,
}

impl fmt::Display for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}/{:?}/{:?}/{:?}/cap-{:?}",
            self.shape, self.path, self.transport, self.workers, self.cap
        )
    }
}

impl Case {
    const fn new(
        shape: Shape,
        path: Path,
        transport: Transport,
        workers: Workers,
        cap: Cap,
    ) -> Self {
        Self {
            shape,
            path,
            transport,
            workers,
            cap,
        }
    }

    fn total_workers(&self) -> usize {
        match self.workers {
            Workers::One => 1,
            Workers::Two => 2,
            Workers::Three => 3,
        }
    }

    /// Whether the combination exists.
    fn applicable(&self) -> bool {
        let cluster = matches!(self.transport, Transport::Dedicated | Transport::Shared);
        match self.path {
            Path::Staged => {
                // Two parallelisms need two workers and an exchange to split
                // stages at, on the in-process transport.
                self.transport == Transport::InProcess
                    && self.workers != Workers::One
                    && has_stage_boundary(self.shape)
            }
            // `SimpleRuntime` has no drain on cancellation.
            Path::Simple => {
                self.transport == Transport::InProcess && self.shape != Shape::DrainOnCancel
            }
            Path::Unfused | Path::Fused | Path::StageTasks => {
                if cluster {
                    // A cluster needs two nodes.
                    self.workers != Workers::One && self.shape != Shape::DrainOnCancel
                } else {
                    true
                }
            }
        }
    }
}

/// Whether the shape has an exchange outside any loop, where the staged
/// path can change parallelism.
fn has_stage_boundary(shape: Shape) -> bool {
    matches!(
        shape,
        Shape::Exchange
            | Shape::ExchangeTwice
            | Shape::FanoutExchanges
            | Shape::NotifySum
            | Shape::Reduce
            | Shape::Count
            | Shape::Distinct
            | Shape::UnaryAsync
            | Shape::IterateExchange
            | Shape::LoopThenCount
    )
}

// ===========================================================================
// Failures and the expected-failure table
// ===========================================================================

/// How a case can fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Failure {
    /// No completion within the timeout.
    Hang,
    /// The run returned an error it should not have; the signature names
    /// which (see [`error_signature`]).
    Error(&'static str),
    /// The run succeeded with records missing (silent loss).
    Loss,
    /// The run produced records that should not exist (duplicates).
    Extra,
    /// A per-epoch aggregate was emitted in pieces or with a wrong value: a
    /// notification fired before all of its epoch's data arrived.
    Split,
    /// A run that must fail succeeded.
    NotRejected,
    /// The progress checker recorded a violation: the check, its kind and
    /// where (an operator port, as `name[in i]` or `name[out i]`; `None` for
    /// violations about a whole worker). In the golden file's "sometimes"
    /// column a location of `*` matches any location.
    Violation(Check, &'static str, Option<&'static str>),
    /// The progress checker did not observe the run.
    Blind(&'static str),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Error(signature) => write!(f, "Error:{signature}"),
            Failure::Violation(check, kind, None) => write!(f, "{check}:{kind}"),
            Failure::Violation(check, kind, Some(at)) => write!(f, "{check}:{kind}@{at}"),
            Failure::Blind(why) => write!(f, "blind:{why}"),
            other => write!(f, "{other:?}"),
        }
    }
}

impl Failure {
    /// The failure without its location: what the table explains.
    fn kind(self) -> Failure {
        match self {
            Failure::Violation(check, kind, _) => Failure::Violation(check, kind, None),
            other => other,
        }
    }

    /// Parse the [`Display`](fmt::Display) form.
    fn parse(text: &str) -> std::result::Result<Failure, String> {
        let text = text.trim();
        Ok(match text {
            "Hang" => Failure::Hang,
            "Loss" => Failure::Loss,
            "Extra" => Failure::Extra,
            "Split" => Failure::Split,
            "NotRejected" => Failure::NotRejected,
            _ => {
                if let Some(signature) = text.strip_prefix("Error:") {
                    Failure::Error(intern(signature))
                } else if let Some(why) = text.strip_prefix("blind:") {
                    Failure::Blind(intern(why))
                } else {
                    let (check, rest) = text
                        .split_once(':')
                        .ok_or_else(|| format!("not a failure: {text:?}"))?;
                    let check = [Check::V1, Check::V4, Check::V5, Check::V6, Check::V7]
                        .into_iter()
                        .find(|c| c.to_string() == check)
                        .ok_or_else(|| format!("not a check: {check:?}"))?;
                    match rest.split_once('@') {
                        Some((kind, at)) => {
                            Failure::Violation(check, intern(kind), Some(intern(at)))
                        }
                        None => Failure::Violation(check, intern(rest), None),
                    }
                }
            }
        })
    }

    /// Whether this is a per-record frontier violation (V1 `late-data` or V7
    /// `frontier-ahead-of-work`): whether one occurs, and where, depends on
    /// the interleaving that exposes a frontier passing a record.
    fn is_frontier(self) -> bool {
        matches!(
            self,
            Failure::Violation(Check::V1, "late-data", _)
                | Failure::Violation(Check::V7, "frontier-ahead-of-work", _)
        )
    }

    /// Whether this (expected) failure accepts the observed failure `seen`.
    fn accepts(self, seen: Failure) -> bool {
        match (self, seen) {
            (Failure::Violation(c1, k1, Some("*")), Failure::Violation(c2, k2, _)) => {
                c1 == c2 && k1 == k2
            }
            _ => self == seen,
        }
    }
}

/// A `&'static str` for `text` (failure kinds and locations are compared
/// and stored as such).
fn intern(text: &str) -> &'static str {
    static INTERNED: OnceLock<Mutex<BTreeSet<&'static str>>> = OnceLock::new();
    let mut interned = INTERNED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = interned.get(text) {
        return existing;
    }
    let leaked: &'static str = Box::leak(text.to_string().into_boxed_str());
    interned.insert(leaked);
    leaked
}

const V1_LATE: Failure = Failure::Violation(Check::V1, "late-data", None);
const V7_AHEAD: Failure = Failure::Violation(Check::V7, "frontier-ahead-of-work", None);
const V7_RESIDUAL: Failure = Failure::Violation(Check::V7, "end-state-residual", None);
const V4_FORCE: Failure = Failure::Violation(Check::V4, "force-close-with-work", None);
const V4_EXHAUSTED: Failure = Failure::Violation(Check::V4, "exhausted-input-frontier", None);
const V4_HOLDING: Failure = Failure::Violation(Check::V4, "finished-holding-capability", None);
const HANG: Failure = Failure::Hang;
const LOSS: Failure = Failure::Loss;
const SPLIT: Failure = Failure::Split;
const INFLIGHT_NEGATIVE: Failure = Failure::Error("inflight-negative");
const DATA_LOST: Failure = Failure::Error("data-lost");

/// Known errors: text the error contains, and its signature. Anything else
/// is `Error:other`.
const ERROR_SIGNATURES: &[(&str, &str)] = &[
    // The single-worker in-flight symmetry assertion (`subgraph.rs`): a
    // `-count` without a matching `+count`.
    ("went negative", "inflight-negative"),
    // `SimpleRuntime`: a worker thread died (the same assertion; the panic
    // message does not reach the error).
    ("terminated unexpectedly", "worker-died"),
    // A cluster node's records reached a peer operator that had finished
    // (PR8's data-loss detection).
    ("data lost on the link", "data-lost"),
];

fn error_signature(error: &str) -> &'static str {
    ERROR_SIGNATURES
        .iter()
        .find(|(text, _)| error.contains(text))
        .map_or("other", |(_, signature)| signature)
}

/// One finding of the correctness audit (or design inventory row) and the
/// kinds of failure it causes in the cases it matches (an empty list matches
/// everything). The table *explains* the golden file: every failure the
/// golden file records must be of a kind a matching entry lists, and every
/// kind an entry lists must occur in some case it matches
/// ([`golden_file_agrees_with_the_table`]). Exactly how each case fails is
/// in the golden file.
struct Expected {
    /// The audit finding (or design inventory row).
    audit: &'static str,
    shapes: &'static [Shape],
    paths: &'static [Path],
    transports: &'static [Transport],
    workers: &'static [Workers],
    caps: &'static [Cap],
    /// The kinds of failure (without location) the finding causes here.
    failures: &'static [Failure],
    /// Why, in one line.
    why: &'static str,
}

impl Expected {
    fn matches(&self, case: &Case) -> bool {
        fn any<T: PartialEq>(list: &[T], value: &T) -> bool {
            list.is_empty() || list.contains(value)
        }
        any(self.shapes, &case.shape)
            && any(self.paths, &case.path)
            && any(self.transports, &case.transport)
            && any(self.workers, &case.workers)
            && any(self.caps, &case.cap)
    }

    /// Whether this entry explains `failure` in `case`.
    fn explains(&self, case: &Case, failure: Failure) -> bool {
        self.matches(case) && self.failures.contains(&failure.kind())
    }
}

/// The table entries that explain `failure` in `case`.
fn explanations(case: &Case, failure: Failure) -> Vec<&'static Expected> {
    EXPECTED_FAILURES
        .iter()
        .filter(|e| e.explains(case, failure))
        .collect()
}

const ANY_PATH: &[Path] = &[];
const ANY_TRANSPORT: &[Transport] = &[];
const ANY_WORKERS: &[Workers] = &[];
const ANY_CAP: &[Cap] = &[];
const MULTI: &[Workers] = &[Workers::Two, Workers::Three];
const UNIFORM: &[Path] = &[Path::Unfused, Path::Fused, Path::StageTasks];
const UNIFORM_AND_SIMPLE: &[Path] = &[Path::Unfused, Path::Fused, Path::StageTasks, Path::Simple];
/// Every path of the one executor core: the uniform paths, stages with
/// different parallelism, and `SimpleRuntime`.
const UNIFORM_STAGED_AND_SIMPLE: &[Path] = &[
    Path::Unfused,
    Path::Fused,
    Path::StageTasks,
    Path::Staged,
    Path::Simple,
];
const LOOPS: &[Shape] = &[
    Shape::Iterate,
    Shape::IterateExchange,
    Shape::NestedIterate,
    Shape::LoopThenCount,
    Shape::LoopThenFlush,
];

/// Today's known failures, by audit finding. See [`Expected`].
const EXPECTED_FAILURES: &[Expected] = &[
    Expected {
        audit: "P-9",
        shapes: &[Shape::NotifyAtFrontier],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[Failure::NotRejected],
        why: "notify_at accepts a time only the input frontier justifies",
    },
    Expected {
        audit: "H1/P-2",
        shapes: &[
            Shape::Binary,
            Shape::Concat,
            Shape::OkErr,
            Shape::Delay,
            Shape::NotifyAtFrontier,
        ],
        paths: &[Path::Unfused],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD],
        why: "pipeline queues and pending_output are invisible to progress: an input \
              capability downgrades past records still queued downstream",
    },
    Expected {
        audit: "H1/P-2",
        shapes: &[Shape::OkErr],
        paths: &[Path::Unfused],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED],
        why: "a worker blocked on a full channel can spin after its peers force-close",
    },
    Expected {
        audit: "C1/C2",
        shapes: &[
            Shape::Exchange,
            Shape::ExchangeTwice,
            Shape::FanoutExchanges,
            Shape::Distinct,
            Shape::NotifySum,
            Shape::Reduce,
            Shape::Count,
        ],
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD, SPLIT],
        why: "records in an exchange are counted only when the producer is of the map \
              family, and completion sums signed counts: frontiers pass records in transit, \
              and per-epoch aggregates fire early",
    },
    Expected {
        audit: "C1/C2",
        shapes: &[
            Shape::Exchange,
            Shape::ExchangeTwice,
            Shape::Distinct,
            Shape::NotifySum,
            Shape::Reduce,
            Shape::Count,
        ],
        paths: &[Path::Unfused],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED],
        why: "with one-batch channels, a worker whose peers completed early spins on a \
              full channel",
    },
    Expected {
        audit: "C2",
        shapes: &[
            Shape::Exchange,
            Shape::ExchangeTwice,
            Shape::FanoutExchanges,
            Shape::NotifySum,
            Shape::Reduce,
            Shape::Count,
            Shape::UnaryAsync,
            Shape::IterateExchange,
            Shape::LoopThenCount,
        ],
        paths: UNIFORM,
        transports: &[Transport::Mock],
        workers: &[Workers::One],
        caps: ANY_CAP,
        failures: &[INFLIGHT_NEGATIVE],
        why: "the network pull records -n but a producer outside the map family never \
              records +n; one worker trips the in-flight symmetry assertion",
    },
    Expected {
        audit: "H10",
        shapes: &[Shape::Broadcast],
        paths: UNIFORM_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD],
        why: "broadcast records are not counted in flight (a guard capability stands in)",
    },
    Expected {
        audit: "H10",
        shapes: &[Shape::Broadcast],
        paths: &[Path::Unfused],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED],
        why: "with one-batch channels the broadcast guard keeps a worker spinning",
    },
    Expected {
        audit: "H12",
        shapes: &[Shape::FanoutExchanges],
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        failures: &[HANG, V4_EXHAUSTED],
        why: "two exchanges fed by one output share one in-flight key",
    },
    Expected {
        audit: "O-9/H11",
        shapes: LOOPS,
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD],
        why: "feedback edges are not in the reachability graph: the loop's concat sees an \
              empty frontier on its feedback input while records circulate",
    },
    Expected {
        audit: "O-9/P-2",
        shapes: LOOPS,
        paths: &[Path::Unfused, Path::Staged],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V4_FORCE],
        why: "a worker force-closes while loop records circulate (its tracker sees no \
              feedback work); Iterate and LoopThenFlush only when pinned to one or two cores",
    },
    Expected {
        audit: "C2/P-2",
        shapes: &[Shape::Distinct],
        paths: &[Path::Staged],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED, V4_FORCE],
        why: "worker 0, the only host of stage 1, sees an empty view while records it never \
              counted sit in the exchange (C2) and in worker 1's pending_output (H1); idle \
              force-close then fires on the empty view (P-2)",
    },
    Expected {
        audit: "C1/C2",
        shapes: &[Shape::Distinct],
        paths: UNIFORM,
        transports: &[Transport::Shared],
        workers: MULTI,
        caps: ANY_CAP,
        failures: &[DATA_LOST, V4_HOLDING, V4_FORCE, LOSS, V7_RESIDUAL],
        why: "under full-matrix load, Shared's pooled connections let progress overtake \
              data: a node's view sums to zero through cancellation (the exchange input \
              holds -n with no +n against distinct's and the input's capabilities; traced, \
              every peer heard from, no seed missing), it force-closes, and records are lost \
              or reach finished operators (rare; never seen isolated)",
    },
    Expected {
        audit: "H11",
        shapes: &[Shape::IterateExchange, Shape::LoopThenCount],
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[SPLIT],
        why: "a loop does not hold the outer epoch: the count after it fires while records \
              still circulate",
    },
    Expected {
        audit: "H11",
        shapes: &[Shape::LoopThenFlush],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: &[Cap::Default],
        failures: &[LOSS],
        why: "a loop does not hold the outer epoch: the frontier after it empties while \
              records still circulate, so a flush-at-empty-frontier operator flushes early",
    },
    Expected {
        audit: "C2",
        shapes: LOOPS,
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED],
        why: "a loop with one-batch channels never completes (loops hang, C2)",
    },
    Expected {
        audit: "C2",
        shapes: &[Shape::IterateExchange, Shape::LoopThenCount],
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        failures: &[HANG, V4_EXHAUSTED],
        why: "a multi-worker loop with an exchange inside, or followed by one from its \
              leave, never completes",
    },
    Expected {
        audit: "O-4/H1",
        shapes: &[Shape::UnaryAsync],
        paths: UNIFORM_STAGED_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD, SPLIT],
        why: "unary_async's queued input, running tasks and results are invisible to \
              progress: the count after it fires early",
    },
    Expected {
        audit: "O-4/P-2",
        shapes: &[Shape::UnaryAsync],
        paths: &[Path::Unfused],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: &[Cap::One],
        failures: &[HANG, V4_EXHAUSTED],
        why: "with one-batch channels, after a worker force-closes while tasks hold \
              records, its peers spin",
    },
    Expected {
        audit: "O-4/P-2",
        shapes: &[Shape::UnaryAsync],
        paths: UNIFORM,
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        failures: &[V4_FORCE],
        why: "a worker force-closes on its tracker's completion while unary_async tasks \
              hold records",
    },
    Expected {
        audit: "O-4/H1",
        shapes: &[Shape::AsyncFailure],
        paths: UNIFORM_AND_SIMPLE,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        failures: &[V1_LATE, V7_AHEAD],
        why: "unary_async's queued input and running tasks are invisible to progress",
    },
    Expected {
        audit: "C1",
        shapes: &[Shape::IterateExchange],
        paths: UNIFORM,
        transports: &[Transport::Dedicated, Transport::Shared],
        workers: MULTI,
        caps: ANY_CAP,
        failures: &[V4_FORCE, V4_HOLDING],
        why: "a node force-closes while loop records are still in flight to it (completion \
              sums signed counts) and finishes holding the count's per-epoch capabilities \
              (pinned to one core)",
    },
];

// ===========================================================================
// Building the shapes
// ===========================================================================

/// What the shape builder needs to know about the case.
#[derive(Clone, Copy, Debug)]
struct BuildCfg {
    shape: Shape,
    activation: Option<Activation>,
    capacity: Option<usize>,
    mock: bool,
    /// On the staged path: the parallelism of the stage after the first
    /// exchange.
    staged_par: Option<usize>,
}

const EPOCHS: u64 = 4;
const PER_EPOCH: i64 = 24;
const LOOP_LAPS: i64 = 12;

/// `(epoch, value)` records.
type Records = Vec<(u64, i64)>;

/// A thread collecting one output receiver.
type Collector = std::thread::JoinHandle<Records>;

/// Records of input `in` (and `in2`): `(epoch, value)`.
fn input_records(second: bool) -> Vec<(u64, i64)> {
    let mut records = Vec::new();
    for epoch in 0..EPOCHS {
        for i in 0..PER_EPOCH {
            let value = epoch as i64 * 1000 + i + if second { 500 } else { 0 };
            records.push((epoch, value));
        }
    }
    records
}

fn two_inputs(shape: Shape) -> bool {
    matches!(shape, Shape::Binary | Shape::Concat)
}

fn two_outputs(shape: Shape) -> bool {
    matches!(
        shape,
        Shape::FanoutExchanges
            | Shape::Branch
            | Shape::Fork
            | Shape::OkErr
            | Shape::IterateExchange
    )
}

/// The first exchange of a shape: a plain exchange, or on the staged path
/// an `exchange_to` that changes parallelism.
fn first_exchange(pipe: Pipe<u64, i64>, cfg: &BuildCfg) -> Result<Pipe<u64, i64>> {
    match cfg.staged_par {
        Some(par) => pipe.exchange_to("first-exchange", par, |x: &i64| *x),
        None => Ok(pipe.exchange("first-exchange", |x: &i64| *x)),
    }
}

/// Route everything to one worker (on the staged path, through a stage of
/// the staged parallelism, keyed so one worker gets everything).
fn gather(pipe: Pipe<u64, i64>, cfg: &BuildCfg) -> Result<Pipe<u64, i64>> {
    match cfg.staged_par {
        Some(par) => pipe.exchange_by_hash_to("gather", par, |_x: &i64| 0),
        None => Ok(pipe.gather("gather")),
    }
}

fn next_multiple_of_5(x: i64) -> i64 {
    (x / 5 + 1) * 5
}

/// Sums its input per epoch and emits the sum when the epoch is complete.
fn notify_sum(pipe: Pipe<u64, i64>) -> Pipe<u64, i64> {
    let mut sums: BTreeMap<u64, i64> = BTreeMap::new();
    pipe.unary_notify("sum", move |input, output, ctx| {
        while let Some((time, data)) = input.next() {
            *sums.entry(time).or_insert(0) += data.iter().sum::<i64>();
            ctx.notify_at(time);
        }
        while let Some(time) = ctx.next_notification() {
            if let Some(sum) = sums.remove(&time) {
                output.session(time).give(sum);
            }
        }
        Ok(())
    })
}

/// Counts its input per epoch, emitted when the epoch is complete.
fn count_per_epoch(pipe: Pipe<u64, i64>) -> Pipe<u64, i64> {
    pipe.count("count").map("count-as-i64", |_t, n| n as i64)
}

fn build_shape(cfg: BuildCfg, b: &mut DataflowBuilder<u64>) -> Result<()> {
    if let Some(activation) = cfg.activation {
        b.check_activation(activation);
    }
    if let Some(capacity) = cfg.capacity {
        b.check_channel_capacity(capacity);
    }
    if cfg.mock {
        b.check_mock_network(true);
    }
    let input = b.input::<i64>("in")?;
    match cfg.shape {
        Shape::Pipeline | Shape::OutputPaused | Shape::OutputDropped | Shape::DrainOnCancel => {
            input.map("inc", |_t, x| x + 1).output("out")?;
        }
        Shape::Exchange => {
            first_exchange(input, &cfg)?
                .map("double", |_t, x| x * 2)
                .output("out")?;
        }
        Shape::ExchangeTwice => {
            first_exchange(input, &cfg)?
                .exchange("second-exchange", |x: &i64| *x / 3)
                .output("out")?;
        }
        Shape::FanoutExchanges => {
            let shared = input.map("identity", |_t, x| x);
            first_exchange(shared.clone(), &cfg)?.output("out")?;
            count_per_epoch(shared.exchange_by_hash("to-one", |_x: &i64| 0)).output("out2")?;
        }
        Shape::Broadcast => {
            input.broadcast("broadcast").output("out")?;
        }
        Shape::Binary => {
            let second = b.input::<i64>("in2")?;
            input
                .binary(second, "binary", |left, right, output| {
                    while let Some((time, data)) = left.next() {
                        output.session(time).give_iterator(data);
                    }
                    while let Some((time, data)) = right.next() {
                        output
                            .session(time)
                            .give_iterator(data.into_iter().map(|x| -x));
                    }
                    Ok(())
                })?
                .output("out")?;
        }
        Shape::Concat => {
            let second = b.input::<i64>("in2")?;
            Pipe::concat(vec![input, second])?.output("out")?;
        }
        Shape::Branch => {
            let (even, odd) = input.branch("parity", |_t, x| x % 2 == 0);
            even.output("out")?;
            odd.output("out2")?;
        }
        Shape::Fork => {
            let (left, right) = input.fork("fork");
            left.output("out")?;
            right.output("out2")?;
        }
        Shape::OkErr => {
            let (ok, err) = input
                .map("classify", |_t, x| if x % 3 == 0 { Err(x) } else { Ok(x) })
                .branch_result("ok-err");
            ok.output("out")?;
            err.output("out2")?;
        }
        Shape::NotifySum => {
            notify_sum(gather(input, &cfg)?).output("out")?;
        }
        Shape::Reduce => {
            gather(input, &cfg)?
                .reduce("sum", |a, b| a + b)
                .output("out")?;
        }
        Shape::Count => {
            count_per_epoch(gather(input, &cfg)?).output("out")?;
        }
        Shape::Distinct => {
            first_exchange(input.map("mod7", |_t, x| x % 7), &cfg)?
                .distinct("distinct")
                .output("out")?;
        }
        Shape::Delay => {
            input.delay("delay", |t, _x| t + 1).output("out")?;
        }
        Shape::UnaryAsync => {
            let tasks = input.unary_async(
                "async",
                2,
                Arc::new(|_time: u64, data: Vec<i64>| async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    Ok(data.into_iter().map(|x| x + 1).collect::<Vec<i64>>())
                }),
            );
            count_per_epoch(gather(tasks, &cfg)?).output("out")?;
        }
        Shape::AsyncFailure => {
            input
                .unary_async(
                    "async-fails",
                    2,
                    Arc::new(|_time: u64, data: Vec<i64>| async move {
                        if data.contains(&3) {
                            Err(instancy::Error::Dataflow(
                                instancy::DataflowError::InvalidConfig("task failed".into()),
                            ))
                        } else {
                            Ok(data)
                        }
                    }),
                )
                .output("out")?;
        }
        Shape::Iterate | Shape::LoopThenFlush => {
            let out = input.iterate::<u32>("loop", 1u32, |var| {
                let stepped = var.map("inc", |_t: &Product<u64, u32>, x| x + 1);
                let done = stepped.clone().filter("done", |_t, x| x % 5 == 0);
                let again = stepped.filter("again", |_t, x| x % 5 != 0);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            if cfg.shape == Shape::LoopThenFlush {
                let mut held: Vec<(u64, i64)> = Vec::new();
                let mut flushed = false;
                out.unary_notify("hold-then-flush", move |input, output, ctx| {
                    while let Some((time, data)) = input.next() {
                        held.extend(data.into_iter().map(|x| (time, x)));
                    }
                    if !flushed && ctx.frontier().is_empty() {
                        flushed = true;
                        for (time, x) in held.drain(..) {
                            output.session(time).give(x);
                        }
                    }
                    Ok(())
                })
                .output("out")?;
            } else {
                out.output("out")?;
            }
        }
        Shape::IterateExchange => {
            let out = input.iterate::<u32>("loop", 1u32, |var| {
                let stepped = var
                    .map("inc", |_t: &Product<u64, u32>, x| x + 1)
                    .exchange("in-loop-exchange", |x: &i64| *x);
                let done = stepped.clone().filter("done", |_t, x| x % 5 == 0);
                let again = stepped.filter("again", |_t, x| x % 5 != 0);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            out.clone().output("out")?;
            count_per_epoch(gather(out, &cfg)?).output("out2")?;
        }
        Shape::NestedIterate => {
            input
                .iterate::<u32>("outer", 1u32, |outer| {
                    let inner_out = outer.iterate::<u32>("inner", 1u32, |inner| {
                        let stepped =
                            inner.map("inner-inc", |_t: &Product<Product<u64, u32>, u32>, x| x + 1);
                        let done = stepped.clone().filter("inner-done", |_t, x| x % 5 == 0);
                        let again = stepped.filter("inner-again", |_t, x| x % 5 != 0);
                        IterateResult {
                            feedback: again,
                            output: done,
                        }
                    });
                    let done = inner_out.clone().filter("outer-done", |_t, x| x % 10 == 0);
                    let again = inner_out.filter("outer-again", |_t, x| x % 10 != 0);
                    IterateResult {
                        feedback: again,
                        output: done,
                    }
                })
                .output("out")?;
        }
        Shape::LoopThenCount => {
            let out = input.iterate::<u32>("long-loop", 1u32, |var| {
                // value * 100 + lap; leaves after LOOP_LAPS laps.
                let stepped = var.map("lap", |_t: &Product<u64, u32>, x| x + 1);
                let done = stepped
                    .clone()
                    .filter("done", |_t, x| x.rem_euclid(100) >= LOOP_LAPS);
                let again = stepped.filter("again", |_t, x| x.rem_euclid(100) < LOOP_LAPS);
                IterateResult {
                    feedback: again,
                    output: done,
                }
            });
            count_per_epoch(gather(out, &cfg)?).output("out")?;
        }
        Shape::NotifyAtFrontier => {
            let mut asked = false;
            input
                .unary_notify("notify-at-frontier", move |input, output, ctx| {
                    while input.next().is_some() {}
                    // No input consumed in this activation and no capability
                    // held: a time the frontier merely permits.
                    if !asked {
                        if let Some(time) = ctx.frontier().elements().first().cloned() {
                            asked = true;
                            ctx.try_notify_at(time)?;
                        }
                    }
                    while let Some(time) = ctx.next_notification() {
                        output.session(time).give(0i64);
                    }
                    Ok(())
                })
                .output("out")?;
        }
    }
    Ok(())
}

/// The exact expected output of ports `out` and `out2`, from every record
/// sent to `in` and `in2`.
fn expected_output(
    shape: Shape,
    first: &[(u64, i64)],
    second: &[(u64, i64)],
    workers: usize,
) -> [Vec<(u64, i64)>; 2] {
    let per_epoch = |f: &dyn Fn(&[i64]) -> Option<i64>| -> Vec<(u64, i64)> {
        let mut by_epoch: BTreeMap<u64, Vec<i64>> = BTreeMap::new();
        for (t, x) in first {
            by_epoch.entry(*t).or_default().push(*x);
        }
        by_epoch
            .into_iter()
            .filter_map(|(t, xs)| f(&xs).map(|v| (t, v)))
            .collect()
    };
    let map = |f: &dyn Fn(i64) -> i64| first.iter().map(|(t, x)| (*t, f(*x))).collect::<Vec<_>>();
    let count = per_epoch(&|xs| Some(xs.len() as i64));
    match shape {
        Shape::Pipeline | Shape::OutputPaused | Shape::DrainOnCancel => [map(&|x| x + 1), vec![]],
        Shape::OutputDropped => [vec![], vec![]],
        Shape::Exchange => [map(&|x| x * 2), vec![]],
        Shape::ExchangeTwice => [first.to_vec(), vec![]],
        Shape::FanoutExchanges => [first.to_vec(), count],
        Shape::Broadcast => [
            first
                .iter()
                .flat_map(|r| std::iter::repeat_n(*r, workers))
                .collect(),
            vec![],
        ],
        Shape::Binary => [
            first
                .iter()
                .copied()
                .chain(second.iter().map(|(t, x)| (*t, -x)))
                .collect(),
            vec![],
        ],
        Shape::Concat => [first.iter().chain(second).copied().collect(), vec![]],
        Shape::Branch => [
            first.iter().filter(|(_, x)| x % 2 == 0).copied().collect(),
            first.iter().filter(|(_, x)| x % 2 != 0).copied().collect(),
        ],
        Shape::Fork => [first.to_vec(), first.to_vec()],
        Shape::OkErr => [
            first.iter().filter(|(_, x)| x % 3 != 0).copied().collect(),
            first.iter().filter(|(_, x)| x % 3 == 0).copied().collect(),
        ],
        Shape::NotifySum | Shape::Reduce => [per_epoch(&|xs| Some(xs.iter().sum())), vec![]],
        Shape::Count | Shape::UnaryAsync | Shape::LoopThenCount => [count, vec![]],
        Shape::Distinct => {
            let mut distinct = BTreeSet::new();
            for (t, x) in first {
                distinct.insert((*t, x % 7));
            }
            [distinct.into_iter().collect(), vec![]]
        }
        Shape::Delay => [first.iter().map(|(t, x)| (t + 1, *x)).collect(), vec![]],
        Shape::AsyncFailure | Shape::NotifyAtFrontier => [vec![], vec![]],
        Shape::Iterate | Shape::LoopThenFlush => [map(&next_multiple_of_5), vec![]],
        Shape::IterateExchange => [map(&next_multiple_of_5), count],
        Shape::NestedIterate => [map(&|x| (x / 10 + 1) * 10), vec![]],
    }
}

/// Which output ports hold per-epoch aggregates (where a premature
/// notification shows up as a split).
fn aggregate_ports(shape: Shape) -> [bool; 2] {
    match shape {
        Shape::FanoutExchanges | Shape::IterateExchange => [false, true],
        Shape::NotifySum
        | Shape::Reduce
        | Shape::Count
        | Shape::Distinct
        | Shape::UnaryAsync
        | Shape::LoopThenCount => [true, false],
        _ => [false, false],
    }
}

/// Shapes whose run must end in an error, and text that error must contain.
fn must_fail(shape: Shape) -> bool {
    must_fail_with(shape).is_some()
}

fn must_fail_with(shape: Shape) -> Option<&'static str> {
    match shape {
        Shape::AsyncFailure => Some("task failed"),
        Shape::NotifyAtFrontier => Some("notify"),
        _ => None,
    }
}

// ===========================================================================
// Running a case
// ===========================================================================

/// What a run produced.
enum RunResult {
    Ok,
    Err(String),
    Hang,
}

struct Observed {
    result: RunResult,
    outputs: [Vec<(u64, i64)>; 2],
}

/// Normal timeout; a case that hangs costs this much. Override with
/// `CONFORMANCE_TIMEOUT_SECS`.
const TIMEOUT: Duration = Duration::from_secs(20);
/// Timeout for a case expected to hang.
const HANG_TIMEOUT: Duration = Duration::from_secs(3);
/// Timeout for a case that may hang.
const MAY_HANG_TIMEOUT: Duration = Duration::from_secs(8);
/// Re-runs of a case whose required failure has not shown up yet.
const ATTEMPTS: usize = 3;

fn timeout() -> Duration {
    std::env::var("CONFORMANCE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(TIMEOUT)
}

fn input_values(shape: Shape) -> (Records, Records) {
    let first = if shape == Shape::LoopThenCount {
        // Each value loops LOOP_LAPS times (value * 100 + lap).
        input_records(false)
            .into_iter()
            .map(|(t, x)| (t, x * 100))
            .collect()
    } else {
        input_records(false)
    };
    let second = if two_inputs(shape) {
        input_records(true)
    } else {
        Vec::new()
    };
    (first, second)
}

/// Send `records` through `senders` (round-robin per record), advancing
/// every sender after each epoch.
fn feed(
    senders: &[InputSender<u64, i64>],
    records: &[(u64, i64)],
) -> std::result::Result<(), String> {
    if senders.is_empty() {
        return Ok(());
    }
    let mut by_epoch: BTreeMap<u64, Vec<Vec<i64>>> = BTreeMap::new();
    for (i, (t, x)) in records.iter().enumerate() {
        let slots = by_epoch
            .entry(*t)
            .or_insert_with(|| vec![Vec::new(); senders.len()]);
        slots[i % senders.len()].push(*x);
    }
    for (epoch, slots) in by_epoch {
        for (sender, batch) in senders.iter().zip(slots) {
            if !batch.is_empty() {
                // Two batches per epoch per sender.
                let half = batch.len() / 2;
                sender
                    .send(epoch, batch[..half].to_vec())
                    .map_err(|e| e.to_string())?;
                sender
                    .send(epoch, batch[half..].to_vec())
                    .map_err(|e| e.to_string())?;
            }
        }
        for sender in senders {
            sender.advance_to(epoch + 1).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Collect every receiver on its own thread.
fn collect(receivers: Vec<OutputReceiver<u64, i64>>, pause: Option<Duration>) -> Vec<Collector> {
    receivers
        .into_iter()
        .map(|receiver| {
            std::thread::spawn(move || {
                if let Some(pause) = pause {
                    std::thread::sleep(pause);
                }
                receiver
                    .collect_data()
                    .into_iter()
                    .flat_map(|(t, data)| data.into_iter().map(move |x| (t, x)))
                    .collect()
            })
        })
        .collect()
}

/// Join the collectors, giving up at `deadline` (a dataflow that never
/// closes its outputs leaves them blocked; they are abandoned).
fn join_collectors(
    handles: Vec<Collector>,
    deadline: std::time::Instant,
) -> Option<Vec<(u64, i64)>> {
    let mut records = Vec::new();
    for handle in handles {
        while !handle.is_finished() {
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        records.extend(handle.join().unwrap_or_default());
    }
    Some(records)
}

/// How long collectors may take to finish once the run has ended.
const COLLECT_GRACE: Duration = Duration::from_secs(10);

/// The outcome of a run whose collectors are `collectors`: a run whose
/// outputs never close is a hang, whatever it returned.
fn observe(result: RunResult, collectors: [Vec<Collector>; 2]) -> Observed {
    let deadline = std::time::Instant::now() + COLLECT_GRACE;
    let [c1, c2] = collectors;
    match (join_collectors(c1, deadline), join_collectors(c2, deadline)) {
        (Some(o1), Some(o2)) => Observed {
            result,
            outputs: [o1, o2],
        },
        _ => Observed {
            result: RunResult::Hang,
            outputs: [vec![], vec![]],
        },
    }
}

/// Wait for `join` with a timeout; on timeout call `cancel` and give the run
/// a moment to stop.
fn wait(
    join: impl FnOnce() -> Result<()> + Send + 'static,
    timeout: Duration,
    cancel: impl FnOnce(),
) -> RunResult {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(join());
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(())) => RunResult::Ok,
        Ok(Err(e)) => RunResult::Err(e.to_string()),
        Err(_) => {
            cancel();
            let _ = rx.recv_timeout(Duration::from_secs(5));
            RunResult::Hang
        }
    }
}

fn build_cfg(case: &Case) -> BuildCfg {
    BuildCfg {
        shape: case.shape,
        activation: match case.path {
            Path::Unfused => Some(Activation::Unfused),
            Path::Fused => Some(Activation::Fused),
            Path::StageTasks => Some(Activation::StageTasks),
            Path::Staged | Path::Simple => None,
        },
        capacity: (case.cap == Cap::One).then_some(1),
        mock: case.transport == Transport::Mock,
        staged_par: (case.path == Path::Staged).then_some(match case.workers {
            // 2 source workers feed 1, or 2 feed 3.
            Workers::Two => 1,
            _ => 3,
        }),
    }
}

/// A cancel function for a run.
type Cancel = Arc<dyn Fn() + Send + Sync>;

/// Inputs and outputs of a multi-worker spawn, then wait.
fn drive_multi(
    case: &Case,
    mut multi: MultiSpawnedDataflow<u64>,
    cancel: Cancel,
    timeout: Duration,
    input_workers: usize,
) -> Observed {
    let (first, second) = input_values(case.shape);
    let take_inputs = |multi: &mut MultiSpawnedDataflow<u64>, name: &str| {
        let mut senders = multi.take_all_inputs::<i64>(name).unwrap_or_default();
        senders.truncate(input_workers);
        senders
    };
    let senders = take_inputs(&mut multi, "in");
    let senders2 = if two_inputs(case.shape) {
        take_inputs(&mut multi, "in2")
    } else {
        Vec::new()
    };
    let pause = (case.shape == Shape::OutputPaused).then(|| Duration::from_millis(300));
    let mut collectors = [Vec::new(), Vec::new()];
    for (port, name) in ["out", "out2"].into_iter().enumerate() {
        if port == 1 && !two_outputs(case.shape) {
            continue;
        }
        let receivers = multi.take_all_outputs::<i64>(name).unwrap_or_default();
        if case.shape == Shape::OutputDropped {
            drop(receivers);
        } else {
            collectors[port] = collect(receivers, pause);
        }
    }
    let (fed_tx, fed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let fed = feed(&senders, &first).and(feed(&senders2, &second));
        // Dropping the senders (when nobody keeps them) closes the inputs.
        let _ = fed_tx.send((fed, senders, senders2));
    });
    let mut keep_inputs_open = None;
    if case.shape == Shape::DrainOnCancel {
        // Cancel once everything is fed, with the inputs still open: the
        // drain closes them and must deliver everything accepted.
        if let Ok(fed) = fed_rx.recv_timeout(timeout) {
            keep_inputs_open = Some(fed);
        }
        cancel();
    } else {
        drop(fed_rx);
    }
    let result = wait(move || multi.join_blocking(), timeout, move || cancel());
    drop(keep_inputs_open);
    observe(result, collectors)
}

/// Run the case once on the in-process transports.
fn run_local(case: &Case, timeout: Duration) -> Observed {
    let cfg = build_cfg(case);
    let workers = case.total_workers();
    let build = move |b: &mut DataflowBuilder<u64>| build_shape(cfg, b);
    match case.path {
        Path::Simple => {
            // `unary_async` captures the tokio runtime it is built in.
            let tokio_rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let cancel = instancy::CancellationToken::new();
            let rt = SimpleRuntime::with_cancel(cancel.clone());
            let spawned = {
                let _context = tokio_rt.enter();
                rt.spawn_multi("conformance", workers, build)
            };
            let observed = match spawned {
                Ok(multi) => drive_multi(
                    case,
                    multi,
                    Arc::new(move || cancel.cancel()),
                    timeout,
                    workers,
                ),
                Err(e) => Observed {
                    result: RunResult::Err(e.to_string()),
                    outputs: [vec![], vec![]],
                },
            };
            tokio_rt.shutdown_timeout(Duration::from_secs(1));
            observed
        }
        _ => {
            let rt = RuntimeHandle::new(RuntimeConfig {
                worker_threads: 2,
                ..RuntimeConfig::default()
            })
            .expect("runtime");
            let token = tokio_util::sync::CancellationToken::new();
            let mut options = SpawnOptions::new().cancellation_token(token.clone());
            let (spawn_workers, input_workers) = if case.path == Path::Staged {
                // Stage 0 runs on 2 workers; later stages use `staged_par`.
                options = options.per_stage_parallelism(true).auto_parallelism(false);
                (2, 2)
            } else {
                options = options.per_stage_parallelism(false);
                (workers, workers)
            };
            if case.shape == Shape::DrainOnCancel {
                options = options.drain_on_cancel(Duration::from_secs(10));
            }
            let spawned = {
                // `unary_async` captures the tokio runtime it is built in.
                let _context = rt.tokio_handle().enter();
                rt.spawn_multi("conformance", spawn_workers, build, options)
            };
            let observed = match spawned {
                Ok(multi) => drive_multi(
                    case,
                    multi,
                    Arc::new(move || token.cancel()),
                    timeout,
                    input_workers,
                ),
                Err(e) => Observed {
                    result: RunResult::Err(e.to_string()),
                    outputs: [vec![], vec![]],
                },
            };
            rt.shutdown();
            observed
        }
    }
}

// ---------------------------------------------------------------------------
// Two-node clusters over loopback TCP
// ---------------------------------------------------------------------------

type Halves = (OwnedReadHalf, OwnedWriteHalf);

async fn tcp_pair() -> (Halves, Halves) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, connected) =
        tokio::try_join!(listener.accept(), TcpStream::connect(addr)).unwrap();
    let accepted = accepted.0;
    accepted.set_nodelay(true).unwrap();
    connected.set_nodelay(true).unwrap();
    (accepted.into_split(), connected.into_split())
}

struct PreEstablished(Mutex<VecDeque<Halves>>);

impl ConnectionFactory for PreEstablished {
    type Reader = OwnedReadHalf;
    type Writer = OwnedWriteHalf;

    async fn establish(
        &self,
        _peer_node_id: &str,
    ) -> std::result::Result<(Self::Reader, Self::Writer), Box<dyn std::error::Error + Send + Sync>>
    {
        self.0.lock().unwrap().pop_front().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "no more pre-established connections",
            ))
        })
    }
}

fn peer_manager(peer: &str, halves: Halves) -> Arc<HashMap<String, SharedPeerManager>> {
    let config = SharedConnectionConfig {
        min_connections: 1,
        max_connections: 1,
        probe_interval: Duration::from_secs(3600),
        rtt_scale_up_threshold: Duration::from_secs(3600),
        rtt_scale_down_threshold: Duration::from_secs(3600),
        cooldown_period: Duration::from_secs(3600),
        reorder_timeout: Duration::from_secs(10),
        rtt_ema_alpha: 0.2,
        idle_timeout: None,
        enable_frame_crc: false,
    };
    let factory: Arc<dyn DynConnectionFactory> =
        Arc::new(PreEstablished(Mutex::new(VecDeque::from([halves]))));
    let manager = SharedPeerManager::new(
        peer.to_string(),
        config,
        factory,
        &tokio::runtime::Handle::current(),
        tokio::sync::broadcast::channel(16).0,
    )
    .unwrap();
    Arc::new(HashMap::from([(peer.to_string(), manager)]))
}

fn run_cluster(case: &Case, timeout: Duration) -> Observed {
    let cfg = build_cfg(case);
    let topology = ClusterTopology::multi_node(vec![
        NodeConfig::new("node-a", 1),
        NodeConfig::new("node-b", if case.workers == Workers::Three { 2 } else { 1 }),
    ])
    .unwrap();
    let transport_capacity = if case.cap == Cap::One { 1 } else { 1024 };
    let id = DataflowId::new();
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let handle = tokio_rt.handle().clone();
    let token = tokio_util::sync::CancellationToken::new();
    let options = SpawnOptions::new().cancellation_token(token.clone());
    let rt_a = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let rt_b = RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let build = move |b: &mut DataflowBuilder<u64>| build_shape(cfg, b);
    let shared = case.transport == Transport::Shared;
    let spawned = tokio_rt.block_on(async {
        let ((ra, wa), (rb, wb)) = tcp_pair().await;
        if shared {
            let managers_a = peer_manager("node-b", (ra, wa));
            let managers_b = peer_manager("node-a", (rb, wb));
            let (a, b) = tokio::join!(
                rt_a.spawn_cluster_async(
                    "conformance",
                    topology.clone(),
                    "node-a",
                    id,
                    ClusterSpawnTransport::shared(Arc::clone(&managers_a), transport_capacity),
                    Duration::from_secs(20),
                    build,
                    &handle,
                    options.clone(),
                ),
                rt_b.spawn_cluster_async(
                    "conformance",
                    topology,
                    "node-b",
                    id,
                    ClusterSpawnTransport::shared(Arc::clone(&managers_b), transport_capacity),
                    Duration::from_secs(20),
                    build,
                    &handle,
                    options,
                ),
            );
            (a, b, Some((managers_a, managers_b)))
        } else {
            let to_b = vec![PeerConnection {
                node_id: "node-b".to_string(),
                reader: ra,
                writer: wa,
            }];
            let to_a = vec![PeerConnection {
                node_id: "node-a".to_string(),
                reader: rb,
                writer: wb,
            }];
            let (a, b) = tokio::join!(
                rt_a.spawn_cluster_async(
                    "conformance",
                    topology.clone(),
                    "node-a",
                    id,
                    ClusterSpawnTransport::dedicated(to_b, transport_capacity),
                    Duration::from_secs(20),
                    build,
                    &handle,
                    options.clone(),
                ),
                rt_b.spawn_cluster_async(
                    "conformance",
                    topology,
                    "node-b",
                    id,
                    ClusterSpawnTransport::dedicated(to_a, transport_capacity),
                    Duration::from_secs(20),
                    build,
                    &handle,
                    options,
                ),
            );
            (a, b, None)
        }
    });
    let (a, b, managers) = spawned;
    let (mut a, mut b) = match (a, b) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            let error = a
                .err()
                .or(b.err())
                .map(|e| e.to_string())
                .unwrap_or_default();
            return Observed {
                result: RunResult::Err(error),
                outputs: [vec![], vec![]],
            };
        }
    };

    let (first, second) = input_values(case.shape);
    let mut senders = Vec::new();
    let mut senders2 = Vec::new();
    let mut collectors = [Vec::new(), Vec::new()];
    let pause = (case.shape == Shape::OutputPaused).then(|| Duration::from_millis(300));
    for node in [&mut a, &mut b] {
        for local in 0..node.num_local_workers() {
            senders.push(node.take_input::<i64>(local, "in").unwrap());
            if two_inputs(case.shape) {
                senders2.push(node.take_input::<i64>(local, "in2").unwrap());
            }
            for (port, name) in ["out", "out2"].into_iter().enumerate() {
                if port == 1 && !two_outputs(case.shape) {
                    continue;
                }
                let receiver = node.take_output::<i64>(local, name).unwrap();
                if case.shape == Shape::OutputDropped {
                    drop(receiver);
                } else {
                    collectors[port].extend(collect(vec![receiver], pause));
                }
            }
        }
    }
    std::thread::spawn(move || {
        let _ = feed(&senders, &first);
        let _ = feed(&senders2, &second);
    });
    let result = wait(
        move || {
            let a = std::thread::spawn(move || a.join_blocking());
            let b = std::thread::spawn(move || b.join_blocking());
            let a = a.join().unwrap_or_else(|_| {
                Err(instancy::Error::Dataflow(
                    instancy::DataflowError::InvalidConfig("node a panicked".into()),
                ))
            });
            let b = b.join().unwrap_or_else(|_| {
                Err(instancy::Error::Dataflow(
                    instancy::DataflowError::InvalidConfig("node b panicked".into()),
                ))
            });
            a.and(b)
        },
        timeout,
        move || token.cancel(),
    );
    let observed = observe(result, collectors);
    drop(managers);
    rt_a.shutdown();
    rt_b.shutdown();
    tokio_rt.shutdown_timeout(Duration::from_secs(1));
    observed
}

// ---------------------------------------------------------------------------
// Classifying an outcome
// ---------------------------------------------------------------------------

/// Multiset difference `a - b`.
fn minus(a: &[(u64, i64)], b: &[(u64, i64)]) -> Vec<(u64, i64)> {
    let mut counts: BTreeMap<(u64, i64), i64> = BTreeMap::new();
    for r in a {
        *counts.entry(*r).or_insert(0) += 1;
    }
    for r in b {
        *counts.entry(*r).or_insert(0) -= 1;
    }
    counts
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .flat_map(|(r, n)| std::iter::repeat_n(r, n as usize))
        .collect()
}

struct Outcome {
    failures: BTreeSet<Failure>,
    details: Vec<String>,
}

fn classify(case: &Case, observed: &Observed, mark: u64) -> Outcome {
    let mut failures = BTreeSet::new();
    let mut details = Vec::new();
    let completed_ok = matches!(observed.result, RunResult::Ok);
    match &observed.result {
        RunResult::Ok if must_fail(case.shape) => {
            failures.insert(Failure::NotRejected);
        }
        RunResult::Ok => {}
        RunResult::Err(e) => {
            let expected = must_fail_with(case.shape).is_some_and(|text| e.contains(text));
            if !expected {
                failures.insert(Failure::Error(error_signature(e)));
                details.push(format!("error: {e}"));
            }
        }
        RunResult::Hang => {
            failures.insert(Failure::Hang);
        }
    }
    if completed_ok && !must_fail(case.shape) {
        let (first, second) = input_values(case.shape);
        let expected = expected_output(case.shape, &first, &second, case.total_workers());
        let aggregates = aggregate_ports(case.shape);
        for port in 0..2 {
            let missing = minus(&expected[port], &observed.outputs[port]);
            let extra = minus(&observed.outputs[port], &expected[port]);
            if missing.is_empty() && extra.is_empty() {
                continue;
            }
            details.push(format!(
                "port {port}: {} missing (e.g. {:?}), {} extra (e.g. {:?})",
                missing.len(),
                missing.iter().take(3).collect::<Vec<_>>(),
                extra.len(),
                extra.iter().take(3).collect::<Vec<_>>()
            ));
            if aggregates[port] && !extra.is_empty() {
                failures.insert(Failure::Split);
            } else {
                if !missing.is_empty() {
                    failures.insert(Failure::Loss);
                }
                if !extra.is_empty() {
                    failures.insert(Failure::Extra);
                }
            }
        }
    }

    // The checker's findings for this case's dataflows.
    let oracles = check::oracles_since(mark);
    let ids: BTreeSet<DataflowId> = oracles.iter().filter_map(|o| o.dataflow).collect();
    for violation in check::violations_since(mark) {
        if violation.check.is_report() {
            continue;
        }
        let ours = match violation.dataflow {
            Some(id) => ids.contains(&id),
            None => true,
        };
        if ours {
            failures.insert(Failure::Violation(
                violation.check,
                violation.kind,
                violation.location.as_deref().map(intern),
            ));
            if details.len() < 12 {
                details.push(violation.to_string());
            }
        }
    }
    // Did the checker observe the run?
    if !check::log_complete_since(mark) {
        failures.insert(Failure::Blind("log overflowed"));
    }
    if oracles.is_empty() {
        failures.insert(Failure::Blind("no oracle"));
    }
    for oracle in &oracles {
        if oracle.wrapped_channels == 0 || oracle.publishes == 0 {
            failures.insert(Failure::Blind("nothing observed"));
        }
        if oracle.registered_workers != oracle.total_workers {
            failures.insert(Failure::Blind("not every worker registered"));
        }
        if completed_ok && !must_fail(case.shape) && !oracle.end_state_checked {
            failures.insert(Failure::Blind("end state not checked"));
            details.push(format!("oracle: {oracle:?}"));
        }
    }
    Outcome { failures, details }
}

// ---------------------------------------------------------------------------
// The golden file
// ---------------------------------------------------------------------------

/// How every case fails today; a case it does not list must pass cleanly.
/// See the module docs for how it is generated and when it may change.
const GOLDEN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/progress_conformance.golden"
);

/// One case's line in the golden file:
/// `<case> | <always> | <sometimes> | <audit findings>`.
#[derive(Clone, Debug, Default, PartialEq)]
struct Golden {
    /// Failures every recorded run showed: the case must show each of them.
    always: BTreeSet<Failure>,
    /// Failures only some recorded runs showed, plus the margin (see
    /// [`merge_observations`]): the case may show them.
    sometimes: BTreeSet<Failure>,
    /// The audit findings that explain the failures (from the table).
    audits: String,
}

impl Golden {
    /// Observed failures this line does not accept. Unless `strict`, a
    /// per-record frontier violation is also accepted at any location where
    /// a case of the same shape and path showed it (`frontier`).
    fn unexpected(
        &self,
        observed: &BTreeSet<Failure>,
        frontier: &BTreeSet<Failure>,
        strict: bool,
    ) -> Vec<Failure> {
        observed
            .iter()
            .filter(|f| {
                !self.always.contains(f)
                    && !self.sometimes.iter().any(|s| s.accepts(**f))
                    && (strict || !(f.is_frontier() && frontier.contains(f)))
            })
            .copied()
            .collect()
    }

    /// Failures this line requires that were not observed. Unless `strict`,
    /// per-record frontier violations are not required.
    fn missing(&self, observed: &BTreeSet<Failure>, strict: bool) -> Vec<Failure> {
        self.always
            .iter()
            .filter(|f| !observed.contains(f) && (strict || !f.is_frontier()))
            .copied()
            .collect()
    }

    fn failures(&self) -> impl Iterator<Item = Failure> + '_ {
        self.always.iter().chain(&self.sometimes).copied()
    }
}

fn parse_failures(text: &str) -> std::result::Result<BTreeSet<Failure>, String> {
    text.split(", ")
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .map(Failure::parse)
        .collect()
}

fn parse_golden(text: &str) -> std::result::Result<BTreeMap<String, Golden>, String> {
    let mut golden = BTreeMap::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        let columns: Vec<&str> = line.split('|').collect();
        let [case, always, sometimes, audits] = columns[..] else {
            return Err(format!("line {}: expected 4 columns: {line}", number + 1));
        };
        let entry = Golden {
            always: parse_failures(always).map_err(|e| format!("line {}: {e}", number + 1))?,
            sometimes: parse_failures(sometimes)
                .map_err(|e| format!("line {}: {e}", number + 1))?,
            audits: audits.trim().to_string(),
        };
        if golden.insert(case.trim().to_string(), entry).is_some() {
            return Err(format!(
                "line {}: {} is listed twice",
                number + 1,
                case.trim()
            ));
        }
    }
    Ok(golden)
}

fn golden() -> &'static BTreeMap<String, Golden> {
    static GOLDEN: OnceLock<BTreeMap<String, Golden>> = OnceLock::new();
    GOLDEN.get_or_init(|| match std::fs::read_to_string(GOLDEN_PATH) {
        Ok(text) => parse_golden(&text).unwrap_or_else(|e| panic!("{GOLDEN_PATH}: {e}")),
        Err(_) if recording().is_some() => BTreeMap::new(),
        Err(e) => panic!("cannot read {GOLDEN_PATH}: {e}"),
    })
}

/// The per-record frontier violations, with location, that any case of
/// `case`'s shape and path shows in the golden file.
fn frontier_locations(case: &Case) -> BTreeSet<Failure> {
    static BY_SHAPE_AND_PATH: OnceLock<BTreeMap<(Shape, Path), BTreeSet<Failure>>> =
        OnceLock::new();
    let by_shape_and_path = BY_SHAPE_AND_PATH.get_or_init(|| {
        let mut map: BTreeMap<(Shape, Path), BTreeSet<Failure>> = BTreeMap::new();
        for other in full_matrix() {
            if let Some(line) = golden().get(&other.to_string()) {
                map.entry((other.shape, other.path))
                    .or_default()
                    .extend(line.failures().filter(|f| f.is_frontier()));
            }
        }
        map
    });
    by_shape_and_path
        .get(&(case.shape, case.path))
        .cloned()
        .unwrap_or_default()
}

/// `CONFORMANCE_STRICT=1`: judge per-record frontier violations per case,
/// exactly like every other failure.
fn strict() -> bool {
    std::env::var("CONFORMANCE_STRICT").is_ok_and(|v| v != "0")
}

fn golden_for(case: &Case) -> Golden {
    golden().get(&case.to_string()).cloned().unwrap_or_default()
}

/// The audit findings that explain `failures` in `case`, in table order.
fn audits_for(case: &Case, failures: impl IntoIterator<Item = Failure>) -> String {
    let failures: Vec<Failure> = failures.into_iter().collect();
    let mut audits: Vec<&str> = Vec::new();
    for entry in EXPECTED_FAILURES {
        if failures.iter().any(|f| entry.explains(case, *f)) && !audits.contains(&entry.audit) {
            audits.push(entry.audit);
        }
    }
    audits.join(", ")
}

/// A V4 `exhausted-input-frontier` report, anywhere.
const V4_EXHAUSTED_ANYWHERE: Failure =
    Failure::Violation(Check::V4, "exhausted-input-frontier", Some("*"));

/// A case's golden line from the failures each recorded run showed: those
/// every run showed are `always`, the others `sometimes`. The one margin: a
/// case that may hang may also report V4 `exhausted-input-frontier`
/// anywhere, because whether that report (1 s and 256 propagations after
/// the inputs are exhausted) beats the hang timeout is a matter of timing.
fn merge_observations(case: &Case, runs: &[BTreeSet<Failure>]) -> Golden {
    let mut always: BTreeSet<Failure> = runs.first().cloned().unwrap_or_default();
    for run in runs {
        always.retain(|f| run.contains(f));
    }
    let mut sometimes: BTreeSet<Failure> = runs
        .iter()
        .flatten()
        .filter(|f| !always.contains(f))
        .copied()
        .collect();
    if always.contains(&Failure::Hang) || sometimes.contains(&Failure::Hang) {
        sometimes.retain(|f| !V4_EXHAUSTED_ANYWHERE.accepts(*f));
        sometimes.insert(V4_EXHAUSTED_ANYWHERE);
    }
    let audits = audits_for(case, always.iter().chain(&sometimes).copied());
    Golden {
        always,
        sometimes,
        audits,
    }
}

fn golden_line(case: &Case, golden: &Golden) -> String {
    format!(
        "{case} | {} | {} | {}",
        join(&golden.always),
        join(&golden.sometimes),
        golden.audits
    )
}

/// `CONFORMANCE_RECORD=<file>`: run every case once and append what it
/// showed to `<file>`, instead of judging it.
fn recording() -> Option<String> {
    std::env::var("CONFORMANCE_RECORD").ok()
}

fn record_observation(path: &str, case: &Case, failures: &BTreeSet<Failure>) {
    use std::io::Write;
    // The `ci_*` tests run on parallel threads: one line, one write, one
    // writer at a time.
    static WRITER: Mutex<()> = Mutex::new(());
    let _writer = WRITER.lock().unwrap_or_else(|e| e.into_inner());
    let line = format!("{case} | {}\n", join(failures));
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(line.as_bytes()))
        .unwrap_or_else(|e| panic!("cannot record to {path}: {e}"));
}

// ---------------------------------------------------------------------------
// Running and judging cases
// ---------------------------------------------------------------------------

/// Cases run one at a time: the checker's log is process-wide.
fn case_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// How long a case may run: short when the golden file says it always
/// hangs, longer when it or the table says it may, and the normal timeout
/// otherwise.
fn timeout_for(case: &Case, expected: &Golden) -> Duration {
    if expected.always.contains(&Failure::Hang) {
        HANG_TIMEOUT
    } else if expected.sometimes.contains(&Failure::Hang)
        || !explanations(case, Failure::Hang).is_empty()
    {
        MAY_HANG_TIMEOUT.min(timeout())
    } else {
        timeout()
    }
}

/// Run `case` once and classify what it showed.
fn observe_case(case: &Case, timeout: Duration) -> (Outcome, Duration) {
    let _guard = case_lock();
    check::set_mode(Mode::Report);
    let mark = check::mark();
    let started = std::time::Instant::now();
    let observed = match case.transport {
        Transport::InProcess | Transport::Mock => run_local(case, timeout),
        Transport::Dedicated | Transport::Shared => run_cluster(case, timeout),
    };
    // Let late reports of a run that was cut short land in the log.
    std::thread::sleep(Duration::from_millis(10));
    (classify(case, &observed, mark), started.elapsed())
}

/// The result of running one case against the golden file.
struct Verdict {
    ok: bool,
    message: String,
}

fn run_case(case: Case) -> Verdict {
    let expected = golden_for(&case);
    let timeout = timeout_for(&case, &expected);
    if let Some(path) = recording() {
        let (outcome, elapsed) = observe_case(&case, timeout);
        record_observation(&path, &case, &outcome.failures);
        return Verdict {
            ok: true,
            message: format!(
                "{case}: recorded [{}] [{:.1}s]",
                join(&outcome.failures),
                elapsed.as_secs_f64()
            ),
        };
    }
    let frontier = frontier_locations(&case);
    let strict = strict();
    let mut mismatches = Vec::new();
    for attempt in 1..=ATTEMPTS {
        let (outcome, elapsed) = observe_case(&case, timeout);
        let unexpected = expected.unexpected(&outcome.failures, &frontier, strict);
        let missing = expected.missing(&outcome.failures, strict);
        let ok = unexpected.is_empty() && missing.is_empty();
        let message = format!(
            "{case}: got [{}]; golden [{}]{}{}{}{} [{:.1}s]\n    {}",
            join(&outcome.failures),
            join(&expected.always),
            if expected.sometimes.is_empty() {
                String::new()
            } else {
                format!(" sometimes [{}]", join(&expected.sometimes))
            },
            if unexpected.is_empty() {
                String::new()
            } else {
                format!("; UNEXPECTED [{}]", join(&unexpected))
            },
            if missing.is_empty() {
                String::new()
            } else {
                format!("; DID NOT REPRODUCE [{}]", join(&missing))
            },
            if expected.audits.is_empty() {
                String::new()
            } else {
                format!(" (audit {})", expected.audits)
            },
            elapsed.as_secs_f64(),
            outcome.details.join("\n    ")
        );
        if ok {
            let message = if mismatches.is_empty() {
                message
            } else {
                format!(
                    "{message}\n    FLAKY: matched on attempt {attempt}; earlier attempts:\n    {}",
                    mismatches.join("\n    ")
                )
            };
            return Verdict { ok, message };
        }
        mismatches.push(message);
    }
    Verdict {
        ok: false,
        message: mismatches.join("\n  retry: "),
    }
}

fn join<'a>(failures: impl IntoIterator<Item = &'a Failure>) -> String {
    failures
        .into_iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Run `cases` (those matching `CONFORMANCE_FILTER`).
fn run_matrix(cases: &[Case]) -> Vec<Verdict> {
    let filter = std::env::var("CONFORMANCE_FILTER").ok();
    let report = std::env::var("CONFORMANCE_REPORT").is_ok();
    let mut verdicts = Vec::new();
    for case in cases.iter().filter(|c| c.applicable()) {
        if let Some(filter) = &filter {
            if !case.to_string().contains(filter.as_str()) {
                continue;
            }
        }
        let verdict = run_case(*case);
        if report || !verdict.ok || verdict.message.contains("FLAKY") {
            println!(
                "{}{}",
                if verdict.ok { "ok   " } else { "FAIL " },
                verdict.message
            );
        }
        verdicts.push(verdict);
    }
    verdicts
}

/// Fail with every case that did not match the golden file.
fn assert_all_matched(verdicts: &[Verdict]) {
    let failed: Vec<&Verdict> = verdicts.iter().filter(|v| !v.ok).collect();
    assert!(
        failed.is_empty(),
        "{} of {} conformance cases did not match the golden file:\n{}",
        failed.len(),
        verdicts.len(),
        failed
            .iter()
            .map(|v| v.message.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ===========================================================================
// The matrix
// ===========================================================================

const PATHS: &[Path] = &[
    Path::Unfused,
    Path::Fused,
    Path::StageTasks,
    Path::Staged,
    Path::Simple,
];
const TRANSPORTS: &[Transport] = &[
    Transport::InProcess,
    Transport::Mock,
    Transport::Dedicated,
    Transport::Shared,
];
const WORKERS: &[Workers] = &[Workers::One, Workers::Two, Workers::Three];
const CAPS: &[Cap] = &[Cap::One, Cap::Default];

fn full_matrix() -> Vec<Case> {
    let mut cases = Vec::new();
    for &shape in ALL_SHAPES {
        for &path in PATHS {
            for &transport in TRANSPORTS {
                for &workers in WORKERS {
                    for &cap in CAPS {
                        let case = Case::new(shape, path, transport, workers, cap);
                        if case.applicable() {
                            cases.push(case);
                        }
                    }
                }
            }
        }
    }
    cases
}

/// The CI subset: every shape on the default path (one and two workers,
/// both capacities); every path and transport on a few core shapes.
fn ci_cases(group: &str) -> Vec<Case> {
    let mut cases = Vec::new();
    match group {
        "shapes" => {
            for &shape in ALL_SHAPES {
                for workers in [Workers::One, Workers::Two] {
                    for cap in [Cap::One, Cap::Default] {
                        cases.push(Case::new(
                            shape,
                            Path::StageTasks,
                            Transport::InProcess,
                            workers,
                            cap,
                        ));
                    }
                }
            }
        }
        "paths" => {
            let core = [
                Shape::Pipeline,
                Shape::Exchange,
                Shape::Count,
                Shape::IterateExchange,
                Shape::Broadcast,
            ];
            for shape in core {
                for path in [Path::Unfused, Path::Fused, Path::Staged, Path::Simple] {
                    for workers in [Workers::Two, Workers::Three] {
                        cases.push(Case::new(
                            shape,
                            path,
                            Transport::InProcess,
                            workers,
                            Cap::Default,
                        ));
                    }
                }
                cases.push(Case::new(
                    shape,
                    Path::StageTasks,
                    Transport::InProcess,
                    Workers::Three,
                    Cap::One,
                ));
            }
        }
        "transports" => {
            let core = [
                Shape::Pipeline,
                Shape::Exchange,
                Shape::Count,
                Shape::IterateExchange,
                Shape::Broadcast,
                Shape::FanoutExchanges,
            ];
            for shape in core {
                for transport in [Transport::Mock, Transport::Dedicated, Transport::Shared] {
                    cases.push(Case::new(
                        shape,
                        Path::StageTasks,
                        transport,
                        Workers::Two,
                        Cap::Default,
                    ));
                }
                cases.push(Case::new(
                    shape,
                    Path::StageTasks,
                    Transport::Dedicated,
                    Workers::Three,
                    Cap::One,
                ));
                cases.push(Case::new(
                    shape,
                    Path::StageTasks,
                    Transport::Shared,
                    Workers::Three,
                    Cap::Default,
                ));
            }
        }
        _ => unreachable!(),
    }
    cases
}

#[test]
fn ci_every_shape_on_the_default_path() {
    assert_all_matched(&run_matrix(&ci_cases("shapes")));
}

#[test]
fn ci_every_path() {
    assert_all_matched(&run_matrix(&ci_cases("paths")));
}

#[test]
fn ci_every_transport() {
    assert_all_matched(&run_matrix(&ci_cases("transports")));
}

/// `DataflowBuilder::skip_progress_check` (for tests outside the harness
/// whose outcome the checker's timing changes) runs a dataflow without an
/// oracle, and only that dataflow.
#[test]
fn skip_progress_check_runs_without_the_checker() {
    let _guard = case_lock();
    check::set_mode(Mode::Report);
    let oracles = |skip: bool| {
        let mark = check::mark();
        let rt = RuntimeHandle::new(RuntimeConfig {
            worker_threads: 2,
            ..RuntimeConfig::default()
        })
        .expect("runtime");
        let mut multi = rt
            .spawn_multi(
                "skip-check",
                2,
                move |b: &mut DataflowBuilder<u64>| {
                    if skip {
                        b.skip_progress_check();
                    }
                    b.input::<i64>("in")?
                        .exchange_by_hash("spread", |x: &i64| *x as u64)
                        .output("out")?;
                    Ok(())
                },
                SpawnOptions::new().per_stage_parallelism(false),
            )
            .expect("spawn");
        let senders = multi.take_all_inputs::<i64>("in").expect("inputs");
        let receivers = multi.take_all_outputs::<i64>("out").expect("outputs");
        senders[0].send(0, vec![1, 2, 3]).expect("send");
        drop(senders);
        multi.join_blocking().expect("join");
        let records: usize = receivers
            .into_iter()
            .flat_map(|r| r.collect_data())
            .map(|(_, data)| data.len())
            .sum();
        assert_eq!(records, 3);
        let oracles = check::oracles_since(mark).len();
        rt.shutdown();
        oracles
    };
    assert_eq!(oracles(true), 0, "skip_progress_check still made an oracle");
    assert_eq!(
        oracles(false),
        1,
        "the checker did not observe the dataflow"
    );
}

/// The staged path and `SimpleRuntime` run every worker with one progress
/// tracker over the whole graph, attached to every other worker: the
/// checker gets exactly one view per worker (no per-stage trackers), and
/// every view is global (a multi-worker tracker without progress channels
/// trips a debug assertion).
#[test]
fn staged_and_simple_workers_have_one_global_view_each() {
    for path in [Path::Staged, Path::Simple] {
        let _guard = case_lock();
        check::set_mode(Mode::Report);
        let mark = check::mark();
        let case = Case::new(
            Shape::Exchange,
            path,
            Transport::InProcess,
            Workers::Three,
            Cap::Default,
        );
        let observed = run_local(&case, timeout());
        if let RunResult::Err(e) = &observed.result {
            panic!("{case}: {e}");
        }
        assert!(matches!(observed.result, RunResult::Ok), "{case}: hang");
        let oracles = check::oracles_since(mark);
        assert_eq!(oracles.len(), 1, "{case}: {oracles:?}");
        let oracle = &oracles[0];
        assert_eq!(oracle.total_workers, 3, "{case}: {oracle:?}");
        assert_eq!(oracle.registered_workers, 3, "{case}: {oracle:?}");
        assert_eq!(
            oracle.views, 3,
            "{case}: one tracker per worker: {oracle:?}"
        );
        assert!(oracle.end_state_checked, "{case}: {oracle:?}");
    }
}

/// The whole matrix (several minutes). See the module docs for the command.
#[test]
#[ignore = "full conformance matrix; run explicitly (see module docs)"]
fn full_conformance_matrix() {
    assert_all_matched(&run_matrix(&full_matrix()));
}

/// The golden file and the table agree: every case the golden file lists
/// exists, every failure it records is explained by a table entry matching
/// the case, its audit column is current, and every kind of failure a table
/// entry lists occurs in some case the entry matches.
#[test]
fn golden_file_agrees_with_the_table() {
    let golden = golden();
    let cases: BTreeMap<String, Case> = full_matrix()
        .into_iter()
        .map(|case| (case.to_string(), case))
        .collect();
    let mut problems = Vec::new();
    for (name, line) in golden {
        let Some(case) = cases.get(name) else {
            problems.push(format!("{name}: not a case of the matrix"));
            continue;
        };
        for failure in line.failures() {
            if matches!(failure, Failure::Blind(_)) {
                problems.push(format!("{name}: records a blind run ({failure})"));
            } else if explanations(case, failure).is_empty() {
                problems.push(format!("{name}: no table entry explains {failure}"));
            }
        }
        if let Some(both) = line.always.intersection(&line.sometimes).next() {
            problems.push(format!("{name}: {both} is both always and sometimes"));
        }
        let audits = audits_for(case, line.failures());
        if line.audits != audits {
            problems.push(format!(
                "{name}: audit column says [{}], the table says [{audits}]",
                line.audits
            ));
        }
    }
    for entry in EXPECTED_FAILURES {
        if entry.audit.is_empty() || entry.why.is_empty() || entry.failures.is_empty() {
            problems.push(format!(
                "{}: an entry needs an audit, a reason and failures",
                entry.audit
            ));
        }
        for kind in entry.failures {
            let occurs = golden.iter().any(|(name, line)| {
                cases.get(name).is_some_and(|case| entry.matches(case))
                    && line.failures().any(|f| f.kind() == *kind)
            });
            if !occurs {
                problems.push(format!(
                    "{}: lists {kind}, which no case it matches shows ({})",
                    entry.audit, entry.why
                ));
            }
        }
    }
    assert!(
        problems.is_empty(),
        "the golden file ({GOLDEN_PATH}) and EXPECTED_FAILURES disagree:\n{}",
        problems.join("\n")
    );
}

/// Regenerate the golden file from recorded runs. See the module docs.
#[test]
#[ignore = "regenerates the golden file from recorded runs (see module docs)"]
fn regenerate_golden_file() {
    let inputs = std::env::var("CONFORMANCE_GOLDEN_INPUTS")
        .expect("set CONFORMANCE_GOLDEN_INPUTS to the recorded runs, separated by ';'");
    let mut runs: BTreeMap<String, Vec<BTreeSet<Failure>>> = BTreeMap::new();
    for path in inputs.split(';').map(str::trim).filter(|p| !p.is_empty()) {
        let text =
            std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (case, failures) = line
                .split_once('|')
                .unwrap_or_else(|| panic!("{path}: not a recorded case: {line}"));
            let failures = parse_failures(failures).unwrap_or_else(|e| panic!("{path}: {e}"));
            runs.entry(case.trim().to_string())
                .or_default()
                .push(failures);
        }
    }
    let mut problems = Vec::new();
    let mut lines = vec![
        "// How every case of tests/progress_conformance.rs fails today. Cases not".to_string(),
        "// listed must pass cleanly. Generated by `regenerate_golden_file` from".to_string(),
        "// recorded runs; do not edit by hand (see the module docs of".to_string(),
        "// progress_conformance.rs). Every change to this file must cite, in its".to_string(),
        "// commit message, the audit finding that explains it.".to_string(),
        "//".to_string(),
        "// <case> | <failures every recorded run showed> | <failures some runs".to_string(),
        "// showed, plus the margin> | <audit findings>".to_string(),
    ];
    let (mut exact, mut variable, mut clean) = (0, 0, 0);
    for case in full_matrix() {
        let name = case.to_string();
        let Some(observations) = runs.remove(&name) else {
            problems.push(format!("{name}: never recorded"));
            continue;
        };
        let line = merge_observations(&case, &observations);
        for failure in line.failures() {
            if matches!(failure, Failure::Blind(_)) {
                problems.push(format!("{name}: a recorded run was blind ({failure})"));
            } else if explanations(&case, failure).is_empty() {
                problems.push(format!("{name}: no table entry explains {failure}"));
            }
        }
        if line.always.is_empty() && line.sometimes.is_empty() {
            clean += 1;
            continue;
        }
        if line.sometimes.is_empty() {
            exact += 1;
        } else {
            variable += 1;
        }
        lines.push(golden_line(&case, &line));
    }
    for name in runs.keys() {
        problems.push(format!("{name}: recorded, but not a case of the matrix"));
    }
    assert!(
        problems.is_empty(),
        "cannot regenerate the golden file:\n{}",
        problems.join("\n")
    );
    std::fs::write(GOLDEN_PATH, lines.join("\n") + "\n").expect("write the golden file");
    println!(
        "wrote {GOLDEN_PATH}: {clean} cases clean, {exact} failing the same way in every \
         run, {variable} varying"
    );
}
