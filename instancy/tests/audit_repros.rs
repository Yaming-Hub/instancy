//! The correctness audit's repro tests.
//!
//! Each module holds the audit's integration tests for one area. Each test
//! asserts the correct behaviour for a bug the audit found
//! (`docs/design/progress-accounting-design.md`, Appendix A, maps each file
//! to the change that fixed it). They run with the rest of the suite. Many
//! are races with their own deadlines: a repro that hangs gives up at its
//! deadline with a panic, or is stopped by the test runner's timeout.
//! `.config/nextest.toml` runs this binary in the `tcp-cluster` group (at most
//! two of its tests at a time): several repros open TCP connections, and the
//! timing-sensitive ones need the CPU.

#![allow(clippy::all, unused)]

#[path = "audit_repros/builder_broadcast.rs"]
mod builder_broadcast;
#[path = "audit_repros/builder_iterate.rs"]
mod builder_iterate;
#[path = "audit_repros/builder_stages.rs"]
mod builder_stages;
#[path = "audit_repros/executor_pipeline_inflight.rs"]
mod executor_pipeline_inflight;
#[path = "audit_repros/executor_staged.rs"]
mod executor_staged;
#[path = "audit_repros/multinode_late.rs"]
mod multinode_late;
#[path = "audit_repros/multinode_premature.rs"]
mod multinode_premature;
#[path = "audit_repros/netdata_broadcast.rs"]
mod netdata_broadcast;
#[path = "audit_repros/netdata_frontier.rs"]
mod netdata_frontier;
#[path = "audit_repros/netprogress_inflight.rs"]
mod netprogress_inflight;
#[path = "audit_repros/netprogress_pending.rs"]
mod netprogress_pending;
#[path = "audit_repros/netprogress_signed_sum.rs"]
mod netprogress_signed_sum;
#[path = "audit_repros/netprogress_stage_starvation.rs"]
mod netprogress_stage_starvation;
#[path = "audit_repros/netprogress_staged_loop.rs"]
mod netprogress_staged_loop;
#[path = "audit_repros/netprogress_startup.rs"]
mod netprogress_startup;
#[path = "audit_repros/operators_broadcast.rs"]
mod operators_broadcast;
#[path = "audit_repros/operators_core.rs"]
mod operators_core;
#[path = "audit_repros/operators_iterate.rs"]
mod operators_iterate;
#[path = "audit_repros/progress_cluster_spurious_complete.rs"]
mod progress_cluster_spurious_complete;
#[path = "audit_repros/progress_cluster_unwired.rs"]
mod progress_cluster_unwired;
#[path = "audit_repros/progress_fanout_exchange.rs"]
mod progress_fanout_exchange;
#[path = "audit_repros/progress_fanout_probe.rs"]
mod progress_fanout_probe;
#[path = "audit_repros/progress_loop_frontier.rs"]
mod progress_loop_frontier;
#[path = "audit_repros/progress_notify_backpressure.rs"]
mod progress_notify_backpressure;
#[path = "audit_repros/progress_total_counts.rs"]
mod progress_total_counts;
#[path = "audit_repros/progress_unwired_exchange.rs"]
mod progress_unwired_exchange;
