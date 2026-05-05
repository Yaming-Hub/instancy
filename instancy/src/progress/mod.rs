//! Progress tracking — the backbone of timely dataflow's correctness guarantees.
//!
//! This module implements the distributed progress tracking protocol that enables
//! operators to know when a timestamp is "complete" — meaning no more data at that
//! timestamp can ever arrive. This is the fundamental mechanism that allows streaming
//! dataflows to produce correct, eventually-consistent results without global barriers.
//!
//! # Core Concepts
//!
//! ## Timestamps and Partial Order
//!
//! Every record in a dataflow carries a logical [`Timestamp`](timestamp::Timestamp).
//! Timestamps form a partial order (via [`PartialOrder`](crate::order::PartialOrder)),
//! which generalizes the notion of "earlier" and "later" to multi-dimensional time
//! (e.g., `(epoch, iteration)` for nested loops).
//!
//! ## Capabilities
//!
//! A [`Capability<T>`](capability::Capability) is a permit proving that an operator
//! may still produce data at timestamp `T`. The progress system tracks all outstanding
//! capabilities to determine what timestamps remain "open." Key rules:
//!
//! - Creating or cloning a capability increments a reference count.
//! - Dropping a capability decrements it.
//! - Capabilities can be *delayed* (creating a new one at a later time) or
//!   *downgraded* (atomically moved forward), but never moved backward.
//! - When all capabilities at or before time `T` are gone, `T` is complete.
//!
//! ## Frontiers (Antichains)
//!
//! A [`Frontier`](frontier::Antichain) (antichain) represents the "lower bound" of
//! timestamps that may still appear at a given point in the graph. It is the minimal
//! set of incomparable timestamps below which everything is complete. When the frontier
//! advances past `T`, downstream operators know `T` is done.
//!
//! ## Reachability
//!
//! The [`reachability`] module determines which capabilities can influence which ports.
//! A capability at `(operator A, time T)` can "reach" operator B's input at time `T'`
//! if there exists a graph path from A to B whose [`PathSummary`](timestamp::PathSummary)
//! transforms `T` into `T'`. When no reachable capabilities remain for a timestamp at
//! a port, that port's frontier advances.
//!
//! ## Notifications
//!
//! The [`Notificator`](notificator::Notificator) delivers callbacks to operators when
//! requested timestamps become complete. This is the primary interface for operators
//! that need to buffer data and emit aggregated results (see `unary_notify`).
//!
//! # How It All Fits Together
//!
//! 1. **Construction**: The dataflow builder registers operators and edges with the
//!    reachability [`Builder`](reachability::Builder), recording path summaries for
//!    each edge (including loop-back edges that increment timestamps).
//!
//! 2. **Compilation**: The builder compiles into a [`Tracker`](reachability::Tracker)
//!    that can propagate capability changes through the graph.
//!
//! 3. **Execution**: As operators produce/consume data:
//!    - Creating data at time `T` requires holding a `Capability<T>`.
//!    - Finishing work at `T` means dropping or downgrading the capability.
//!    - The tracker propagates these changes and updates per-port frontiers.
//!    - Operators observe frontier advances and fire notifications.
//!
//! 4. **Termination**: When all input sources close and all capabilities are dropped,
//!    all frontiers advance to the empty antichain, signaling completion.
//!
//! # Module Guide
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`timestamp`] | `Timestamp` and `PathSummary` traits |
//! | [`capability`] | `Capability<T>` — permits for producing data |
//! | [`frontier`] | `Antichain<T>` — minimal incomparable timestamp sets |
//! | [`mutable_antichain`] | `MutableAntichain<T>` — incremental frontier maintenance |
//! | [`change_batch`] | `ChangeBatch<T>` — batched +1/−1 capability updates |
//! | [`reachability`] | Graph-aware propagation of capability implications |
//! | [`notificator`] | Notification delivery when timestamps complete |
//! | [`operate`] | `ProgressReporter` — operator interface to the tracker |
//! | [`progress_channel`] | Cross-worker progress message transport |
//! | [`network_progress`] | Serializable progress messages for cluster mode |
//! | [`subgraph`] | Nested scope progress tracking |

pub mod capability;
pub mod change_batch;
pub mod frontier;
pub mod mutable_antichain;
pub mod network_progress;
pub mod notificator;
pub mod operate;
pub mod progress_channel;
pub mod reachability;
pub mod subgraph;
pub mod timestamp;
