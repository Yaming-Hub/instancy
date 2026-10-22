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
//! Today's known failures are listed in [`EXPECTED_FAILURES`], keyed by
//! audit ID. A case covered by an entry must fail *exactly* as listed: if a
//! later step fixes it, or it starts failing differently (say from a hang to
//! a loss), the case fails and the table must be updated. Some failures
//! depend on scheduling; an entry lists those as `allowed` rather than
//! `required`, and a case is re-run (up to [`ATTEMPTS`] times) only while a
//! required failure has not shown up yet.
//!
//! CI runs a bounded subset (the `ci_*` tests below). Run the whole matrix
//! with:
//!
//! ```text
//! cargo test -p instancy --all-features --test progress_conformance -- --ignored --test-threads=1
//! ```
//!
//! Set `CONFORMANCE_FILTER` to a substring of a case's name (for example
//! `Iterate/StageTasks`) to run only matching cases, and
//! `CONFORMANCE_REPORT=1` to print every case's outcome.
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
    /// The separate staged executor (S): stages with different parallelism.
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

    /// Fused or stage-task activation, in-process, one worker, default
    /// capacity.
    fn is_default_configuration(&self) -> bool {
        matches!(self.path, Path::Fused | Path::StageTasks)
            && self.transport == Transport::InProcess
            && self.workers == Workers::One
            && self.cap == Cap::Default
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
    /// The progress checker recorded a violation of this kind.
    Violation(Check, &'static str),
    /// The progress checker did not observe the run.
    Blind(&'static str),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Error(signature) => write!(f, "Error:{signature}"),
            Failure::Violation(check, kind) => write!(f, "{check}:{kind}"),
            Failure::Blind(why) => write!(f, "blind:{why}"),
            other => write!(f, "{other:?}"),
        }
    }
}

const V1_LATE: Failure = Failure::Violation(Check::V1, "late-data");
const V7_AHEAD: Failure = Failure::Violation(Check::V7, "frontier-ahead-of-work");
const V7_RESIDUAL: Failure = Failure::Violation(Check::V7, "end-state-residual");
const V4_FORCE: Failure = Failure::Violation(Check::V4, "force-close-with-work");
const V4_EXHAUSTED: Failure = Failure::Violation(Check::V4, "exhausted-input-frontier");
const V4_HOLDING: Failure = Failure::Violation(Check::V4, "finished-holding-capability");
const HANG: Failure = Failure::Hang;
const LOSS: Failure = Failure::Loss;
const SPLIT: Failure = Failure::Split;
const INFLIGHT_NEGATIVE: Failure = Failure::Error("inflight-negative");
const WORKER_DIED: Failure = Failure::Error("worker-died");

/// Known errors: text the error contains, and its signature. Anything else
/// is `Error:other`.
const ERROR_SIGNATURES: &[(&str, &str)] = &[
    // The single-worker in-flight symmetry assertion (`subgraph.rs`): a
    // `-count` without a matching `+count`.
    ("went negative", "inflight-negative"),
    // `SimpleRuntime`: a worker thread died (the same assertion; the panic
    // message does not reach the error).
    ("terminated unexpectedly", "worker-died"),
];

fn error_signature(error: &str) -> &'static str {
    ERROR_SIGNATURES
        .iter()
        .find(|(text, _)| error.contains(text))
        .map_or("other", |(_, signature)| signature)
}

/// One known failure of today's code: which cases it covers (an empty
/// list matches everything) and how they fail.
struct Expected {
    /// The audit finding (or design inventory row) that causes it.
    audit: &'static str,
    shapes: &'static [Shape],
    paths: &'static [Path],
    transports: &'static [Transport],
    workers: &'static [Workers],
    caps: &'static [Cap],
    /// Failures every matching case must show.
    required: &'static [Failure],
    /// Failures a matching case may show (they depend on scheduling).
    allowed: &'static [Failure],
    /// The entry does not cover the default single-worker configuration
    /// (fused or stage-task activation, in-process, default capacity), where
    /// its shapes run clean today: there they must stay clean.
    except_default: bool,
    /// Why, in one line.
    why: &'static str,
}

impl Expected {
    fn matches(&self, case: &Case) -> bool {
        fn any<T: PartialEq>(list: &[T], value: &T) -> bool {
            list.is_empty() || list.contains(value)
        }
        if self.except_default && case.is_default_configuration() {
            return false;
        }
        any(self.shapes, &case.shape)
            && any(self.paths, &case.path)
            && any(self.transports, &case.transport)
            && any(self.workers, &case.workers)
            && any(self.caps, &case.cap)
    }
}

const ANY_SHAPE: &[Shape] = &[];
const ANY_PATH: &[Path] = &[];
const ANY_TRANSPORT: &[Transport] = &[];
const ANY_WORKERS: &[Workers] = &[];
const ANY_CAP: &[Cap] = &[];
const MULTI: &[Workers] = &[Workers::Two, Workers::Three];
const UNIFORM: &[Path] = &[Path::Unfused, Path::Fused, Path::StageTasks];
const LOOPS: &[Shape] = &[
    Shape::Iterate,
    Shape::IterateExchange,
    Shape::NestedIterate,
    Shape::LoopThenCount,
    Shape::LoopThenFlush,
];
/// Shapes with an exchange whose producer records no `+n` (it is not of
/// the map family): an input, an exchange, a loop's leave or `unary_async`.
const UNWIRED_EXCHANGE: &[Shape] = &[
    Shape::Exchange,
    Shape::ExchangeTwice,
    Shape::FanoutExchanges,
    Shape::NotifySum,
    Shape::Reduce,
    Shape::Count,
    Shape::IterateExchange,
];
/// Shapes with a cross-worker exchange (that do not hang first at any
/// capacity).
const EXCHANGES: &[Shape] = &[
    Shape::Exchange,
    Shape::ExchangeTwice,
    Shape::FanoutExchanges,
    Shape::NotifySum,
    Shape::Reduce,
    Shape::Count,
    Shape::Distinct,
    Shape::UnaryAsync,
    Shape::IterateExchange,
];

/// Today's known failures. See the module docs for how entries are judged.
///
/// `required` failures are deterministic consequences of the finding;
/// `allowed` ones are its scheduling-dependent symptoms (a hang whose inputs
/// are all exhausted also shows as V4 `exhausted-input-frontier`, so that is
/// allowed wherever a hang is). Every entry must reproduce in the full matrix
/// ([`assert_table_exercised`]).
const EXPECTED_FAILURES: &[Expected] = &[
    Expected {
        audit: "P-9",
        shapes: &[Shape::NotifyAtFrontier],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[Failure::NotRejected],
        allowed: &[],
        except_default: false,
        why: "notify_at accepts a time only the input frontier justifies",
    },
    Expected {
        audit: "C2",
        shapes: UNWIRED_EXCHANGE,
        paths: UNIFORM,
        transports: &[Transport::Mock],
        workers: &[Workers::One],
        caps: ANY_CAP,
        required: &[INFLIGHT_NEGATIVE],
        allowed: &[V1_LATE, V7_AHEAD, SPLIT, HANG, V4_EXHAUSTED],
        except_default: false,
        why: "the network pull records -n but a producer outside the map family never \
              records +n; one worker trips the in-flight symmetry assertion",
    },
    Expected {
        audit: "C2",
        shapes: &[Shape::LoopThenCount],
        paths: UNIFORM,
        transports: &[Transport::Mock],
        workers: &[Workers::One],
        caps: &[Cap::Default],
        required: &[INFLIGHT_NEGATIVE],
        allowed: &[],
        except_default: false,
        why: "as above, for the exchange after a loop (at capacity 1 the loop hangs first)",
    },
    Expected {
        audit: "P-7",
        shapes: EXCHANGES,
        paths: &[Path::Simple],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        required: &[WORKER_DIED],
        allowed: &[V1_LATE, V7_AHEAD, SPLIT],
        except_default: false,
        why: "SimpleRuntime wires cross-worker exchanges but no progress channels: one \
              worker's tracker gets -n for records whose +n another worker recorded",
    },
    Expected {
        audit: "P-7",
        shapes: &[Shape::LoopThenCount],
        paths: &[Path::Simple],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: &[Cap::Default],
        required: &[WORKER_DIED],
        allowed: &[],
        except_default: false,
        why: "as above, for the exchange after a loop (at capacity 1 the loop hangs first)",
    },
    Expected {
        audit: "P-7",
        shapes: &[Shape::Broadcast],
        paths: &[Path::Simple],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        required: &[],
        allowed: &[V1_LATE, V7_AHEAD],
        except_default: false,
        why: "SimpleRuntime has no cross-worker progress: a worker's frontier ignores \
              records broadcast by its peers",
    },
    Expected {
        audit: "H12",
        shapes: &[Shape::FanoutExchanges],
        paths: UNIFORM,
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        required: &[HANG],
        allowed: &[V1_LATE, V7_AHEAD, V4_EXHAUSTED],
        except_default: false,
        why: "two exchanges fed by one output share one in-flight key",
    },
    Expected {
        audit: "H14",
        shapes: ANY_SHAPE,
        paths: &[Path::Staged],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[V7_AHEAD],
        allowed: &[
            V1_LATE,
            SPLIT,
            LOSS,
            HANG,
            V4_FORCE,
            V7_RESIDUAL,
            V4_HOLDING,
            V4_EXHAUSTED,
        ],
        except_default: false,
        why: "the staged executor's per-stage trackers see nothing across stages",
    },
    Expected {
        audit: "O-4/P-2",
        shapes: &[Shape::UnaryAsync],
        paths: &[Path::Staged],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[LOSS, V4_FORCE],
        allowed: &[],
        except_default: false,
        why: "the staged path force-closes while unary_async tasks still hold records, \
              and reports success without them",
    },
    Expected {
        audit: "O-9/H11",
        shapes: LOOPS,
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[V7_AHEAD],
        allowed: &[
            V1_LATE,
            HANG,
            SPLIT,
            LOSS,
            V4_FORCE,
            V7_RESIDUAL,
            V4_HOLDING,
            V4_EXHAUSTED,
        ],
        except_default: false,
        why: "feedback edges are not in the reachability graph: the loop's concat sees an \
              empty frontier on its feedback input while records circulate",
    },
    Expected {
        audit: "C2",
        shapes: &[
            Shape::Iterate,
            Shape::NestedIterate,
            Shape::LoopThenCount,
            Shape::LoopThenFlush,
        ],
        paths: &[Path::Unfused, Path::Fused, Path::StageTasks, Path::Simple],
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: &[Cap::One],
        required: &[HANG],
        allowed: &[V4_EXHAUSTED],
        except_default: false,
        why: "a loop with one-batch channels never completes (loops hang, C2)",
    },
    Expected {
        audit: "C2",
        shapes: &[Shape::IterateExchange],
        paths: &[Path::Unfused, Path::Fused, Path::StageTasks, Path::Staged],
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        required: &[HANG],
        allowed: &[V4_EXHAUSTED],
        except_default: false,
        why: "a multi-worker loop with an exchange inside, or followed by one from its \
              leave, never completes",
    },
    Expected {
        audit: "C2",
        shapes: &[Shape::LoopThenCount],
        paths: UNIFORM,
        transports: ANY_TRANSPORT,
        workers: MULTI,
        caps: ANY_CAP,
        required: &[HANG],
        allowed: &[V4_EXHAUSTED],
        except_default: false,
        why: "a multi-worker loop followed by an exchange from its leave never completes",
    },
    Expected {
        audit: "H11",
        shapes: &[Shape::LoopThenFlush],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: &[Cap::Default],
        required: &[],
        allowed: &[LOSS],
        except_default: false,
        why: "a loop does not hold the outer epoch: the frontier after it empties while \
              records still circulate, so a flush-at-empty-frontier operator flushes early",
    },
    Expected {
        audit: "O-4/H1",
        shapes: &[Shape::UnaryAsync],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[],
        allowed: &[
            V1_LATE,
            V7_AHEAD,
            SPLIT,
            HANG,
            LOSS,
            V4_FORCE,
            V7_RESIDUAL,
            INFLIGHT_NEGATIVE,
            V4_EXHAUSTED,
        ],
        except_default: false,
        why: "unary_async's queued input, running tasks and results are invisible to progress",
    },
    Expected {
        audit: "O-4/H1",
        shapes: &[Shape::UnaryAsync],
        paths: ANY_PATH,
        transports: &[
            Transport::InProcess,
            Transport::Dedicated,
            Transport::Shared,
        ],
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[V7_AHEAD],
        allowed: &[],
        except_default: false,
        why: "records held by unary_async are invisible to progress, so the frontier \
              after it passes them",
    },
    Expected {
        audit: "C2",
        shapes: &[Shape::UnaryAsync],
        paths: &[Path::Fused, Path::StageTasks],
        transports: &[Transport::Mock],
        workers: &[Workers::One],
        caps: ANY_CAP,
        required: &[INFLIGHT_NEGATIVE],
        allowed: &[],
        except_default: false,
        why: "unary_async feeds an exchange without recording +n; one worker trips the \
              in-flight symmetry assertion",
    },
    Expected {
        audit: "O-4/H1",
        shapes: &[Shape::AsyncFailure],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[],
        allowed: &[V1_LATE, V7_AHEAD],
        except_default: false,
        why: "unary_async's queued input and running tasks are invisible to progress",
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
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[],
        allowed: &[V1_LATE, V7_AHEAD, SPLIT, HANG, V4_EXHAUSTED],
        except_default: true,
        why: "records in an exchange are counted only when the producer is of the map \
              family, and completion sums signed counts: frontiers pass records in transit",
    },
    Expected {
        audit: "H10",
        shapes: &[Shape::Broadcast],
        paths: ANY_PATH,
        transports: ANY_TRANSPORT,
        workers: ANY_WORKERS,
        caps: ANY_CAP,
        required: &[],
        allowed: &[V1_LATE, V7_AHEAD, HANG, V4_EXHAUSTED],
        except_default: true,
        why: "broadcast records are not counted in flight (a guard capability stands in)",
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
        required: &[],
        allowed: &[V1_LATE, V7_AHEAD, HANG, V4_EXHAUSTED],
        except_default: false,
        why: "pipeline queues and pending_output are invisible to progress: an input \
              capability downgrades past records still queued downstream; a worker \
              blocked on a full channel can spin after its peers force-close",
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
            failures.insert(Failure::Violation(violation.check, violation.kind));
            if details.len() < 12 {
                details.push(violation.to_string());
            }
        }
    }
    // Did the checker observe the run?
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
// Running and judging cases
// ---------------------------------------------------------------------------

/// Cases run one at a time: the checker's log is process-wide.
fn case_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn expected_for(case: &Case) -> (Vec<&'static Expected>, BTreeSet<Failure>, BTreeSet<Failure>) {
    let entries: Vec<&Expected> = EXPECTED_FAILURES
        .iter()
        .filter(|e| e.matches(case))
        .collect();
    let required: BTreeSet<Failure> = entries
        .iter()
        .flat_map(|e| e.required.iter().copied())
        .collect();
    let allowed: BTreeSet<Failure> = entries
        .iter()
        .flat_map(|e| e.required.iter().chain(e.allowed).copied())
        .collect();
    (entries, required, allowed)
}

/// The result of running one case against the table.
struct Verdict {
    case: Case,
    failures: BTreeSet<Failure>,
    ok: bool,
    message: String,
}

fn run_case(case: Case) -> Verdict {
    let (entries, required, allowed) = expected_for(&case);
    let timeout = if required.contains(&Failure::Hang) {
        HANG_TIMEOUT
    } else if allowed.contains(&Failure::Hang) {
        MAY_HANG_TIMEOUT.min(timeout())
    } else {
        timeout()
    };
    let mut last = None;
    for _attempt in 0..ATTEMPTS {
        let _guard = case_lock();
        check::set_mode(Mode::Report);
        let mark = check::mark();
        let started = std::time::Instant::now();
        let observed = match case.transport {
            Transport::InProcess | Transport::Mock => run_local(&case, timeout),
            Transport::Dedicated | Transport::Shared => run_cluster(&case, timeout),
        };
        // Let late reports of a run that was cut short land in the log.
        std::thread::sleep(Duration::from_millis(10));
        let outcome = classify(&case, &observed, mark);
        let unexpected: Vec<Failure> = outcome
            .failures
            .iter()
            .filter(|f| !allowed.contains(f))
            .copied()
            .collect();
        let missing: Vec<Failure> = required
            .iter()
            .filter(|f| !outcome.failures.contains(f))
            .copied()
            .collect();
        let ok = unexpected.is_empty() && missing.is_empty();
        let message = format!(
            "{case}: got [{}]; expected [{}]{}{}{} [{:.1}s]\n    {}",
            join(&outcome.failures),
            join(&required),
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
            if entries.is_empty() {
                String::new()
            } else {
                format!(
                    " (audit {})",
                    entries
                        .iter()
                        .map(|e| e.audit)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            },
            started.elapsed().as_secs_f64(),
            outcome.details.join("\n    ")
        );
        let done = ok || !unexpected.is_empty();
        last = Some(Verdict {
            case,
            failures: outcome.failures,
            ok,
            message,
        });
        if done {
            break;
        }
    }
    last.expect("at least one attempt")
}

fn join<'a>(failures: impl IntoIterator<Item = &'a Failure>) -> String {
    failures
        .into_iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Run `cases` and fail with every mismatch.
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
        if report || !verdict.ok {
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

/// Fail with every case that did not match the table.
fn assert_all_matched(verdicts: &[Verdict]) {
    let failed: Vec<&Verdict> = verdicts.iter().filter(|v| !v.ok).collect();
    assert!(
        failed.is_empty(),
        "{} of {} conformance cases did not match EXPECTED_FAILURES:\n{}",
        failed.len(),
        verdicts.len(),
        failed
            .iter()
            .map(|v| v.message.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// Every entry of the table must have matched a run case, and each of its
/// required failures must have shown up in one (an entry with only allowed
/// failures must have shown at least one of them somewhere).
fn assert_table_exercised(verdicts: &[Verdict]) {
    let mut unexercised = Vec::new();
    for entry in EXPECTED_FAILURES {
        let matched: Vec<&Verdict> = verdicts.iter().filter(|v| entry.matches(&v.case)).collect();
        if matched.is_empty() {
            unexercised.push(format!("{}: matches no case ({})", entry.audit, entry.why));
            continue;
        }
        for failure in entry.required {
            if !matched.iter().any(|v| v.failures.contains(failure)) {
                unexercised.push(format!("{}: {failure} never reproduced", entry.audit));
            }
        }
        if entry.required.is_empty()
            && !matched
                .iter()
                .any(|v| entry.allowed.iter().any(|f| v.failures.contains(f)))
        {
            unexercised.push(format!(
                "{}: none of [{}] ever reproduced ({})",
                entry.audit,
                join(entry.allowed),
                entry.why
            ));
        }
    }
    assert!(
        unexercised.is_empty(),
        "EXPECTED_FAILURES entries not reproduced:\n{}",
        unexercised.join("\n")
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

/// The whole matrix (several minutes). See the module docs for the command.
#[test]
#[ignore = "full conformance matrix; run explicitly (see module docs)"]
fn full_conformance_matrix() {
    let verdicts = run_matrix(&full_matrix());
    let exercised = std::panic::catch_unwind(|| {
        if std::env::var("CONFORMANCE_FILTER").is_err() {
            assert_table_exercised(&verdicts);
        }
    });
    assert_all_matched(&verdicts);
    if let Err(panic) = exercised {
        std::panic::resume_unwind(panic);
    }
}

/// The table is well formed: no entry is empty, every required failure is
/// distinct from its allowed ones.
#[test]
fn expected_failures_table_is_well_formed() {
    for entry in EXPECTED_FAILURES {
        assert!(!entry.audit.is_empty() && !entry.why.is_empty());
        assert!(
            !entry.required.is_empty() || !entry.allowed.is_empty(),
            "{}: an entry must list a failure",
            entry.audit
        );
        for failure in entry.required {
            assert!(
                !entry.allowed.contains(failure),
                "{}: {failure} is both required and allowed",
                entry.audit
            );
        }
    }
}
