# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Progress trace recorder

- **A flight recorder for progress failures.** In debug builds and
  builds with the `progress-check` or `test-utils` feature,
  `progress::trace` keeps a bounded ring of recent progress events per
  worker and per peer link:
  - commits and applied batches, with every change;
  - seals, decisions (S2a, S2b, residuals) and executor exits;
  - pushes and receives, with their outcomes;
  - link events, including what retired a link.
  When `INSTANCY_TRACE_DIR` is set, a data-lost push, records that reach
  a finished operator, a proven residual or a checker finding dumps every
  ring of the dataflow to `$INSTANCY_TRACE_DIR/<dataflow-id>/`, in
  addition to the failure. `progress::check::dump_traces` dumps on
  request, and the conformance harness does so for every unexpected
  outcome. `INSTANCY_TRACE_EVENTS` sizes the rings (default 16,384
  events). Release builds without those features contain none of it.
  See `docs/design/progress-tracking.md` §11.6.6.
- CI's test and conformance jobs record, and upload the dumps when they
  fail (retained 14 days).

### Shared transport reliability

- **Fixed silent data loss in Shared mode (audit C7).** A pooled connection
  that failed and was redialled used to drop the frames queued on it, and
  the frames sent while it was being redialled, without anyone noticing:
  both nodes could report success with records missing, or hang. Shared mode
  is now fail-stop on link loss, like Dedicated mode and like timely: a
  pooled connection that fails (a write error, or the read side ending)
  fails every dataflow that used it, on both nodes, and a frame that cannot
  be sent fails its dataflow. A write error closes the whole connection, so
  the peer sees it end even with split stream halves (TLS) whose write half
  alone would tell it nothing. The failed connection leaves the pool; the
  next dataflow that registers re-establishes it through the application's
  `ConnectionFactory`. There is no background redial any more.
- **Each dataflow's lane is pinned to one pooled connection.** A dataflow's
  frames to a peer (data, progress and control) all use the connection it
  was given when it registered, so TCP keeps them in order and a failed
  connection fails only the dataflows that used it. The receiver checks
  every payload frame's sequence number: a lost or duplicated frame
  (`TransportError::OutOfSequence`), or a lane whose frames arrive on two
  connections (`TransportError::LaneViolation`), fails the dataflow as
  corrupt traffic instead of being delivered. The reorder buffer and its
  gap timer are gone, which also removes the spurious `ReorderTimeout`
  failures of healthy dataflows with 2 or more pooled connections (audit
  H7).
- **The pool has a fixed size.** It used to grow and shrink on its own,
  from each node's own RTT probes, but the peer was never told: frames on a
  connection only one side opened were lost, and a connection one side
  closed looked like a failure to the other (audit H7). The pool now holds
  exactly `SharedConnectionConfig::connections` connections. Coordinated
  resizing, agreed by both nodes, may come later. RTT probes remain, as
  metrics only (`SharedPeerManager::metrics_snapshot`).
- One dataflow's oversized frame (or a payload frame on a reserved channel)
  is rejected and fails only that dataflow; the shared connection and the
  other dataflows are untouched (audit M4).
- Establishing a pooled connection is bounded by the new
  `SharedConnectionConfig::connect_timeout`, and a Shared-mode
  `spawn_cluster` bounds registration by its handshake timeout, so an
  unreachable peer fails the spawn instead of hanging it (audit M5).
  `ConnectionPool::acquire` enforces `PoolConfig::connect_timeout`.
- **Frame checksums are always on**, in both modes: every frame carries a
  CRC32 of its header and one of header plus payload. Dedicated sessions
  never checksummed their frames before, so a flipped payload bit was
  accepted unless it broke decoding.
- **A `DataflowId` identifies one run; a retry needs a fresh id.** Pooled
  connections outlive dataflows and frames carry only the dataflow id, so a
  run that reused an earlier run's id could take that run's late frames
  (data, progress, its seal and end-of-stream) for its own. A
  `SharedPeerManager` now refuses to register an id it remembers as
  finished (the most recent 16,384), with
  `TransportError::InvalidRegistration`, and drops the late frames of such
  an id. Payload frames queued before a dataflow unregistered are dropped,
  never numbered into another registration's lane, and a cleanup removes
  only its own registration (so a reused id's late cleanup cannot
  unregister the new run). The manager's per-dataflow bookkeeping is
  bounded: control frames of dataflows that never register expire, the set
  of finished dataflow ids is bounded, and the handles of finished
  connection tasks are released. The runtime's set of departed nodes is
  bounded too.
- **BREAKING:** `SharedPeerManager::payload_sender()` is replaced by
  `dataflow_sender(&id)`, a `DataframeSender` bound to the dataflow's
  registration; flush barriers are internal queue entries, so channel
  `u64::MAX - 1` is no longer reserved.
- A corrupt frame on a pooled connection is corruption (fatal in every
  phase) only for the dataflow it belongs to; the other dataflows reading
  from the connection lose the connection, which is fatal only while they
  still need the peer's frames.
- `Muxer::new` returns an error instead of panicking when
  `MuxConfig::buffer_size` is 0.
- The transport contract is documented: the application provides the
  connections and owns their liveness (detecting a dead or partitioned
  peer, through keepalives or its membership layer and
  `report_node_leave`); instancy never reports success with lost,
  duplicated, reordered or corrupted data, and fails the affected
  dataflows when a connection reports an error or EOF.
- **Cluster protocol version 6** (frame checksums always present; pinned,
  strictly sequenced Shared lanes). Nodes on version 5 are rejected at the
  handshake.
- **BREAKING:** `SharedConnectionConfig` is now `{ connections,
  connect_timeout, probe_interval, rtt_ema_alpha }` (`min_connections`
  became `connections`; `max_connections`, the RTT thresholds,
  `cooldown_period`, `reorder_timeout`, `idle_timeout` and
  `enable_frame_crc` are gone). `PeerPool`, `ScalingDecision`,
  `ScalingDriver`, `ScalingEvent`, `ReorderBuffer` and its result types are
  removed (`RttProber` replaces the probe bookkeeping).
  `SharedPeerManager::register_dataflow` and
  `SharedTransportSession::take_io_events` deliver `PeerIoEvent`
  notices (now public, shared with Dedicated mode) instead of
  `TransportError`s; `connection_count` returns a `usize` and
  `live_connection_count` is gone. `FramedWriter::with_crc` and
  `FramedReader::with_crc` are gone (`new` always checksums).
  `TransportError::ReorderTimeout` is replaced by `OutOfSequence` and
  `LaneViolation`. `Muxer::new` returns a `Result`.

### CI

- CI runs every audit repro (each in its own process), the full progress
  conformance matrix (in its own job), a build and test run with
  `--no-default-features`, and a release-mode check.
- `--no-default-features` builds again, and release builds no longer
  warn about code that only debug builds use.

### Port-counted progress (step 3 of the progress-accounting redesign)

- Progress is now counted at operator ports, as in timely dataflow. Every
  operator runs inside an operator shell that counts the records it consumes
  and produces at each port. A record is counted at its target from the
  commit that produces it until the commit that consumes it, wherever it is:
  in a channel, a network queue, or an operator's input buffer. A frontier
  therefore never passes a record that still exists. This fixes premature
  completion and lost or late records in multi-worker and cluster
  dataflows, including loops, exchanges after loops, broadcasts and
  fan-outs (audit C1, C2, H1, H10–H13). The executor no longer force-closes
  quiescent operators after idle sweeps, and no longer reports success when
  every local operator is done. A worker finishes only once its view of
  every location that can reach its operators is empty, and the decision is
  taken after its progress is propagated.
- Each `iterate` loop, nested loops included, is its own progress scope in
  `Product` time, bridged to its parent. Probes, `retain` and
  `unary_notify` inside a loop body observe the body's own frontiers.
- A worker that has finished seals its progress stream. Once a worker has
  applied every worker's seal, no further update can arrive. A count still
  left in its view is an accounting bug, reported as
  `ProgressError::Residual` ("progress residual at completion") instead of
  a hang. A worker that fails never seals: the dataflow fails with its
  error, through cancellation.
- **Behaviour change:** output at a time that no capability the operator
  holds, and no input it consumed in the same call, justifies is an error
  in every build (`ProgressError::Unjustified`). So is a capability request
  (`notify_at`, `retain`, a downgrade) at such a time. A notification
  justified only by the input frontier, which used to be accepted, is now
  rejected. To emit stashed data in a later call, hold it with
  `OutputHandle::retain`.
- **Behaviour change:** every closure-based operator (`unary`, `binary`,
  the `notify` family) gets exactly one final call when its input
  frontiers become empty, whether or not it holds a `Retained`. Stashing
  data without `retain` and emitting it in that call fails loudly
  (`Unjustified`) instead of silently dropping it.
- **Behaviour change:** error envelopes are fatal at an operator's input on
  every transport (`ProgressError::UpstreamError`).
- **Behaviour change:** data in a closure's input queue holds that input's
  frontier, and a probe includes data still queued on the probed stream.
- **Behaviour change:** every probe is done after a successful run,
  including on `SimpleRuntime::spawn_multi`, on clusters, and inside loop
  bodies. Probes used to keep a stale frontier there.
- **Behaviour change:** a loop's back-edge is never bounded, and a capacity
  set on the pipe returned as `feedback` is ignored. A cycle of bounded
  channels can always fill up and deadlock. A loop body therefore gets no
  internal backpressure: inflow is still throttled at the loop's entry, and
  memory is bounded by the loop's working set, as in timely.
- **Behaviour change:** on a cluster, a node finishes only once every
  node's input has closed and every record is consumed, as in timely.
  `join` still doesn't wait for a peer's teardown. Early finish still
  applies to in-process plans whose stages run on different workers. On a
  cluster, early finish only ever mattered when no records could flow from
  the still-open node to the finished one: with any exchange or broadcast,
  it was reachable only through premature completion.
- **Behaviour change:** when a peer stops in the middle of a graceful drain
  (`drain_on_cancel`) while the drain still needs it, the draining node
  fails promptly with "peer … stopped before finishing its part of the
  cancelled dataflow, which the drain needs", instead of waiting for the
  drain deadline. A stopped peer whose late records or progress are still
  arriving is given a 500 ms quiet grace.
- **Behaviour change:** a loop whose summary doesn't advance the iteration
  counter (for example `0u32`) is an identity cycle, and the dataflow
  fails to build.
- Add `OutputHandle::retain(time)`, which returns a `Retained` capability
  that keeps `time` open across calls. `Retained` is not `Clone`. Dropping
  it releases the time, from any thread, and wakes the operator.
- Add `InputHandle::frontier()`, and `Delivery` on graph edges (how many
  copies of a record its edge delivers; part of the plan digest).
- Fix the quadratic cost of `unary_notify` and the operators built on it
  (`count`, `reduce`, `fold`, `distinct`, `delay`) over many distinct
  times: each `notify_at` and each delivered notification scanned every
  outstanding time. Both now take O(log n) in the times outstanding.
- **BREAKING:** with the `transport` feature, `Pipe::iterate`'s inner
  timestamp must be `ExchangeData` (the new `IterateInner` trait), like the
  timestamps of exchanged data: a loop's progress crosses the network on a
  cluster. `u32`, `u64` and the other built-in integer types already
  qualify. `Product<A, B>` is `ExchangeData` when `A` and `B` are, so nested
  loops work too.
- **BREAKING:** `ProgressError::InvalidNotificationTime` is replaced by
  `ProgressError::Unjustified`. New variants: `UpstreamError`,
  `RecordAfterShutdown` and `Residual`.
- **BREAKING:** operators are built only through `DataflowBuilder`. The
  re-exports of `DataflowExecutor`, `ExecutorConfig`, `SchedulableOperator`,
  `OperatorFactory`, `ChannelEndpoints`, `ActivationOutcome`,
  `ChannelSourceOperator` and `ChannelSinkOperator` are removed. `Push` and
  `Pull` are sealed, and their `set_inflight_reporter` is removed.
  `InputHandle::{new, push_vec, mark_exhausted}` and `OutputHandle::drain`
  are crate-private.
- **BREAKING (wire format):** cluster protocol version 5
  (`PROTOCOL_VERSION`). A progress commit carries one section per loop
  scope, and a commit split into several frames sends every positive change,
  of every section, before any negative one. Progress streams carry seal and
  abandon markers.
- The progress checker panics on a violation by default in debug builds.
  `INSTANCY_PROGRESS_CHECK` still selects the mode. It checks output
  justification (V3, recorded alongside the production error), proven
  residuals (V5), and that a sealing worker holds no work (V4). The
  conformance matrix and the audit repros are judged strictly: the golden
  file and the expected-failure tables are empty. The protocol property test
  models loop scopes and catches five more mutants.

### One executor core (step 2 of the progress-accounting redesign)

- Every path now runs on one executor core. A **physical plan**, computed
  once per spawn, decides which workers run which operators: a stage with
  parallelism `p` runs on workers `0..p`, each operator sees its stage's
  worker context, and exchanges connect the stages' workers. Stages with
  different parallelism (`exchange_to`, `gather`, `rebalance_to`) no longer
  use a separate staged executor with per-stage progress trackers; every
  worker runs the operators of its stages on the uniform executor, with one
  progress tracker over the whole graph connected to every other worker.
  `SimpleRuntime::spawn_multi` workers now exchange progress too.
- The staged path now honours `SpawnOptions::drain_on_cancel`, and metrics
  and timelines.
- **Behaviour change:** a pipeline edge from a stage with more workers into
  one with fewer now fails at spawn, naming the edge (its records used to be
  silently dropped). A worker that runs only the target of a pipeline edge
  (into a stage with more workers) sees that input as already ended; such an
  edge used to be dropped too. Feedback edges still need equal parallelism.
- `RuntimeHandle::spawn_multi` calls the `build` closure once per spawn on
  every path (the per-stage path used to call it once per worker, plus once
  more). `SimpleRuntime::spawn_multi` still calls it once per worker.
- Initial capabilities are seeded on every worker from the physical plan
  instead of being sent on the progress streams, so no worker can compute a
  frontier before a peer's initial capabilities arrive (audit H13).
- **BREAKING (wire format):** cluster protocol version 4
  (`CLUSTER_PROTOCOL_VERSION`): a progress stream no longer carries initial
  capabilities, and the handshake fingerprint covers the physical plan, so
  nodes that would seed different capabilities fail the handshake.
- Remove `ExchangeMessage::FrontierUpdate` and `ExchangeMessage::SenderDone`,
  which only the staged executor used.

### Progress test harness

- Add a progress invariant checker, `instancy::progress::check`, compiled
  into every debug build and, with the new `progress-check` feature, into
  release builds. It checks late data (V1), termination with work outstanding
  (V4), the progress wire rules (V6), and, with an oracle that measures
  occupancy from channels, operator queues and capability objects rather than
  from the progress accounting, that no frontier passes outstanding work
  (V7). It also reports a worker's residual counts at finish (V5). Select its
  behaviour with `INSTANCY_PROGRESS_CHECK` (`panic`, the default in debug
  builds; `report`, the default otherwise; `verbose`; `off`). It never
  changes how a dataflow runs.
- Add a progress conformance matrix (`tests/progress_conformance.rs`, a
  bounded subset in CI) whose golden file records exactly how each case fails
  today, by operator port, explained by audit finding; and the correctness
  audit's repro tests (`tests/audit_repros.rs`, on demand). A protocol
  property test checks the real reachability tracker under random
  interleavings and catches four protocol mutants, and a two-worker tracker
  test checks that in-flight counts hold every worker's frontier.
- Each violation the checker records (`Violation::location`) names the
  operator port it is about, and the checker's log keeps the newest
  violations when it is full (`log_complete_since`).
- The checker does its checking on a background thread
  (`instancy-progress-check`): hooks on the dataflow's threads only queue
  events, which keeps it from changing the interleavings it observes.

### Exchange and broadcast retry fixes

- Fix duplicated records when an exchange or broadcast `try_push` is accepted
  by some targets and pushed back by another (for example a full per-peer
  network queue). The returned envelope's retry now delivers only the
  sub-batches or copies still owed, from the partition computed on the first
  attempt, so `rebalance` and other stateful routing keep each record on one
  target. This also removes the livelock when several sub-batches compete for
  one free slot in a peer queue.
- Account exchange records in flight per delivered sub-batch, so partially
  delivered envelopes are counted exactly once.
- `ExchangePush` and `BroadcastPush` reject a different envelope while a
  partially delivered one is waiting for its retry, instead of duplicating or
  reordering data. `ProgressExchange::broadcast_local_changes` answers
  different changes with `Error::Backpressure`, sending nothing, until the
  partially delivered update has been retried, and reports
  `Error::ChannelClosed` for every call once a peer's channel has closed.
- Fix `ProgressExchange::broadcast_local_changes` sending a progress delta
  again to peers that had already accepted it when the documented retry
  follows backpressure from another peer.

### Graceful cluster teardown

- A cluster node no longer tears down while peers still need its frames. Each
  node ends every cross-node stream with an in-band end-of-stream marker
  (data close sentinels and a new progress end-of-stream frame), peers
  acknowledge it with a new in-band acknowledgement frame (ordered behind
  their data), and a per-dataflow lifecycle task owns the transport, bridges
  and shared-manager registration until every link is retired. Final progress
  deltas queued behind congestion are no longer dropped, a peer's clean close
  no longer stops a slower node's bridges to third nodes, and shared mode no
  longer discards frames queued for a dataflow that finished first.
- Close sentinels on network data channels are delivered reliably instead of
  being dropped when the per-peer queue is full (broadcast no longer hangs).
- **BREAKING:** Cross-node data loss is never silent. Records pushed to a
  peer that has already finished, and records that reach a node after the
  operator they are addressed to finished, fail the dataflow with the new
  `CommunicationError::DataLoss` variant (downstream exhaustive matches must
  handle it). A premature completion elsewhere in the cluster now makes some
  node fail instead of every node returning `Ok` with missing output.
- A push whose records were enqueued before the peer finished no longer
  fails with `DataLoss` when the peer finishes before the push returns. The
  push used to check the link again after enqueueing, but the peer could
  already have read the records. Record sends and the link's retirement are
  now ordered by a per-link lock: a record is either enqueued ahead of the
  peer's acknowledgement or refused with `DataLoss`.
- A link that fails while a node's workers are still running (EOF, corrupt
  frame or control message, send failure, transport error) now cancels the
  dataflow on that node and on every reachable peer, instead of silently
  tearing down the node's network while its workers keep running. An EOF or
  send failure on part of a link escalates once the whole link has closed,
  or after `handshake_timeout` if it never does. Frames dropped by a failed
  send are reported as data loss if the peer or the local workers finish
  without them, and a dedicated connection whose writer failed is kept open
  until teardown instead of being half-closed. A push whose queue to a peer
  that has not finished is closed now waits (as for a full queue) for the
  dataflow to be cancelled with the actual cause (the peer's `Cancel` or the
  link failure) instead of failing the operator with `ChannelClosed`.
- Connections are closed without losing their last frames. A node that
  releases a connection writes what is queued, shuts its write half down
  (so the peer sees the end of the stream at once, also over split TLS or
  pipe halves, whose drop sends nothing) and keeps reading until the peer
  closes its side (at most 10 s), instead of closing a TCP socket with
  unread data, which resets the connection and can destroy the frames the
  peer has not read yet. A per-job shared manager released by a dataflow's
  teardown first writes everything queued on it (bounded by
  `handshake_timeout`) instead of aborting its writers.
- `ClusterCompletion` resolves once the local workers have finished and the
  shutdown handshake has completed (each peer has received this node's
  frames or has finished itself). Its result is the local workers' result,
  or `CommunicationError::DataLoss` (see above). Blocking waits
  (`join_blocking`, `wait`) need the transport's tokio runtime to keep
  running: on a worker thread of a multi-thread runtime they use
  `block_in_place` (workers are recognised by their thread names), so
  they work with a single worker thread or with every worker waiting; when
  the caller is inside a `current_thread` runtime, or the transport runs on
  one, that is not making progress (e.g. the caller is the thread driving
  it), they return `RuntimeError::InvalidConfig` promptly instead of
  deadlocking.
- Dropping an unresolved `ClusterCompletion` now detaches it: the dataflow
  keeps running with its network intact (previously the network was cut while
  local workers kept running). Dropping `ClusterSpawnedDataflow` without
  `join()`, or calling `cancel()`, still cancels the dataflow cluster-wide.
- A node whose workers still succeed after a cancellation (e.g. with
  `drain_on_cancel`) no longer waits forever for a peer that cancelled and
  tore down, and flushes its queued frames before unregistering, so a peer
  that is still draining receives its final progress.
- Wire protocol: new progress end-of-stream and acknowledgement frames
  (reserved progress counts `u32::MAX` and `u32::MAX - 1`).
  `CLUSTER_PROTOCOL_VERSION` is now 3, so the handshake rejects peers that
  lack them; nodes must run the same version.

### Unclean link loss and corruption are fatal

- A dedicated-mode connection loss is no longer treated as a clean end of
  stream: data channels are exhausted only by their close sentinel, so a
  node can no longer report success with a peer's records missing. Lost or
  corrupted progress channels no longer leave the dataflow hanging.
- A data frame that fails to decode no longer becomes an error envelope that
  operators silently ignore. Batches with more than 10M records are no longer
  always dropped by the receiver, and the decoder reserves at most 1 MiB up
  front, so a corrupt record count cannot trigger a huge allocation. An
  envelope too large for one transport frame fails with the new
  `CommunicationError::FrameTooLarge` instead of killing the connection.
- A standalone `NetworkEdgeMaterializer` (`with_connections`, no cluster
  coordinator) delivers a channel's close sentinel even when the queue is full
  as the channel closes; it waits for room on the runtime instead of dropping
  it (the receiver never counts a disconnect as the end of its input).
- Corrupt traffic (a frame that fails its CRC or does not decode, a received
  frame over the size limit, a progress frame after the channel's
  end-of-stream) fails the dataflow with `CommunicationError::DataLoss` in
  every phase, even on a retired link and even if every local worker already
  succeeded; the peers that may still be running are cancelled. A data
  channel that ends without its close sentinel after the peer finished is
  data loss too. A data-channel close tag followed by extra bytes is rejected
  as corrupt instead of being accepted as a clean end of stream.
- Dedicated transport sessions report EOF, read and write failures, and
  dropped oversize frames, and keep a peer's channels open until that report
  reached the lifecycle coordinator; the writer no longer stops on an
  oversize frame.
- **BREAKING:** new error variants `CommunicationError::FrameTooLarge` and
  `HandshakeError::PeerCancelled`.
- A worker that fails, is cancelled or never runs no longer closes its
  outgoing exchange and broadcast channels cleanly (network and same-node
  targets), so peers cannot mistake its truncated streams for complete ones.
  New `Push::close_on_drop` (defaults to `close()`) lets an endpoint decide
  how it closes when the composite pusher holding it is dropped.
- An async source whose producer is cut off by a cancellation (without
  `drain_on_cancel`), or never started, no longer finishes as if its input
  had ended.
- `report_node_leave` works after the peer's connection already dropped
  (the dataflow stays registered until its workers stop).
- `spawn_cluster` failures are announced to every peer (`Cancel`), so peers
  in the handshake or ready barrier fail promptly with
  `HandshakeError::PeerCancelled`, and a peer that already passed the barrier
  no longer runs (or reports success) without the failed node. The handshake
  and barrier wait on all peers at once.
- `source_async` producers start only after the ready barrier, so a failed
  `spawn_cluster` never runs them.
- The cluster `Cancel` broadcast sends to all peers concurrently.
- A cluster-wide requested stop (`drain_on_cancel`) that can no longer
  complete ends at once instead of at each node's drain deadline. A node whose
  workers do not complete after announcing a requested stop (a worker failed
  during the drain, the drain timed out, the node did not drain, or its
  runtime was dropped) says so with a second `Cancel`: a failure stops every
  peer's drain; otherwise only drains that still need the node's data (or
  whose data push to it is blocked) stop. A draining node also stops when such a peer's
  link ends before its goodbye (also after that peer's `Cancel`), or when
  corrupt traffic arrives. Stopped workers report the failure as their
  cancellation reason; workers that do not drain keep the first reason.
- A progress batch split over several frames (more than 1,000,000 changes)
  sends its positive changes first, so a receiver applying the frames one by
  one never sees a capability released before the one that replaces it.

### Input contract hardening

- **BREAKING:** Invalid input/frontier sequences that were previously accepted
  now fail execution. `ProgressError` gains two variants; downstream exhaustive
  matches must handle them. The input API and valid timestamp semantics are unchanged.
- Reject input data below or incomparable with the committed frontier, including
  empty batches, with `ProgressError::InputTimeBeforeFrontier`.
- Reject backward or incomparable input frontier updates with
  `ProgressError::InvalidInputFrontier` instead of only logging a warning.
  Equal frontiers and equal-time data remain valid.
- Apply validation in receive order for sync inputs, async inputs, and
  `source_async`. Sending successfully means enqueued; protocol errors reach
  execution completion with operator context. Clones share one frontier.
- Make blocking multi-worker joins observe failures across all workers, and
  preserve originating errors instead of masking them with sibling cancellation.
- Cancel execution-owned producers and sibling workers on failure even when no
  join is being polled, including heterogeneous stages, dedicated workers, and
  one-worker-per-node clusters. A failed query does not cancel the shared runtime.
- Disconnect the last input sender before waking the executor to prevent a lost
  EOF wakeup. Release completion notifier resources after publishing results.
- Recheck ancestor cancellation after registering async notifications so failure
  cleanup cannot miss a parent-token cancellation.
- Fix fan-out retries to return the original envelope and resume at the first
  undelivered branch instead of losing data or injecting empty minimum-time batches.
  **BREAKING for custom channel callers:** retry an envelope returned by `try_push`
  unchanged before submitting another, or close/drop the pusher to abandon it;
  `TeePush::flush` no longer retries internally retained data.
- Add external-input throughput benchmarks for sync/async batch submission with
  and without per-batch frontier advancement.

### Cluster protocol and topology hardening

**BREAKING (wire format):** this release speaks cluster protocol version 3
(`instancy::communication::CLUSTER_PROTOCOL_VERSION`; version 2 added the
changes below, version 3 the shutdown handshake frames described under
"Graceful cluster teardown"). Nodes running 0.1.x
cannot join a cluster with nodes running this release; without frame CRC the
handshake rejects them with `HandshakeError::ProtocolVersionMismatch` (an
unversioned 0.1.x handshake is reported as version 1). With
`enable_frame_crc` the frame layouts already differ, so the mismatch surfaces
as checksum failures or a handshake timeout. Upgrade all nodes of a cluster
together. Wire-level changes:

- The handshake carries a protocol version (first field after the message
  type) and the sender's node id:
  `[0][version: u32][fingerprint: u64][dataflow_id: 16][node_id_len: u32][node_id]`.
- The graph fingerprint is a stable hash of the timestamp type, the topology
  (node ids, order and per-node worker counts), every operator's kind, name and
  ports, every edge's endpoints and channel kind, every exchange's kind, name,
  timestamp type (including loop timestamps inside `iterate`) and record type,
  and the input/output ports. Previously it covered only
  operator/edge/feedback counts, exchange edge indices and the total worker
  count, and used `DefaultHasher`, which is not stable across Rust releases or
  32/64-bit targets.
- Channel ids live in disjoint `2^62`-id namespaces: progress ids are
  `PROGRESS_CHANNEL_BASE (2^62) + src * N + dst`, exchange data ids are
  `DATA_CHANNEL_BASE (2^63) + (edge * N + src) * N + dst`, and
  `interprocess::PROGRESS_CHANNEL_ID` moved from 0 to `3 * 2^62`. Previously
  data ids (`edge * N² + src * N + dst + 1`) collided with progress ids
  (`1_000_000 + ...`), e.g. for 2 × 64 workers with ≥ 15 exchange edges.
- With `enable_frame_crc`, frames carry a CRC32 of the header right after the
  header and a CRC32 of header + payload after the payload. Previously the CRC
  covered only the payload, so a corrupted `channel_id`/`dataflow_id` misrouted
  a frame and a corrupted `length` desynchronised the stream.
- **Key routing changed:** `Pipe::exchange` / `exchange_to` and
  `ExchangeFn::by_key` hash keys with the new `instancy::hash::StableHasher`
  instead of `DefaultHasher`, so a key may go to a different worker than in
  0.1.x. For integers, strings, and tuples/structs of them the mapping is now
  identical on every node, platform and Rust release (`usize`/`isize` values
  are hashed as 64-bit). Slices, arrays and `Vec`s of multi-byte integers are
  still hashed by `std` as native-endian bytes; see the `instancy::hash` docs.
  Hashing is also faster: about 6× for `u64` keys and 2× for 16-byte strings
  (`benches/exchange_hash.rs`).

Fixes:

- Reject nodes whose graphs or topologies differ during the handshake instead
  of passing it and then hanging, cross-wiring branch outputs (branches
  declared in a different order), or reading `u64` records as `i64`.
- Verify peer identity: a connection whose remote end identifies as a different
  node than it was assigned to (handshake and `Ready`) fails with
  `HandshakeError::PeerIdentityMismatch` instead of hanging every node.
- Reject duplicate node ids: `ClusterTopology::multi_node` returns
  `TopologyError::NodeAlreadyExists`, and `spawn_cluster` validates the
  (public) `nodes` list with the new `ClusterTopology::validate`. A duplicate
  used to create a phantom worker whose records were lost while the run
  returned `Ok`.
- A membership `NodeJoined` event for a known node with a different worker
  count now updates the live topology instead of being ignored.
- Reject duplicate channel registrations instead of silently replacing a
  receiver: `Demuxer::register_channel`, `TransportSession::new`, and
  `SharedPeerManager::register_dataflow` (which also rejects re-registering a
  live dataflow id) now return `TransportError::InvalidRegistration`.
- Channel-id arithmetic is overflow-checked; `spawn_cluster` fails fast with an
  error when a cluster's ids do not fit their namespace.
- In the shared transport, a payload frame sent on the control or probe channel
  is rejected with `TransportError::ReservedChannel` instead of consuming a
  sequence number the receiver never sees, which stalled the dataflow's lane.
- Return errors instead of panicking for a zero-worker node, a transport
  capacity of 0 (dedicated and shared), and a `Pipe::collect` whose automatic
  output name is already taken (it now picks the next free name).

**BREAKING (API):** `TransportSession::new`, `SharedTransportSession::new`,
`SharedPeerManager::register_dataflow`, `Demuxer::register_channel`,
`NetworkEdgeMaterializer::new`/`with_connections`/`channel_id` now return
`Result`. `TransportError`, `HandshakeError`, `ControlProtocolError` and
`CommunicationError` gain variants. New: `instancy::hash`,
`ClusterTopology::validate`, `DataflowGraph::operator_kind`,
`transport_session::{data_channel_id, progress_channel_id, DATA_CHANNEL_BASE,
RESERVED_CHANNEL_BASE, CHANNEL_NAMESPACE_SIZE}`.

### Executor and runtime lifecycle robustness

- Dropping an output receiver (or never taking it before `join()`) abandons the
  port: the sink keeps consuming and discards its input. Previously the sink
  stopped consuming, livelocking upstream operators and blocking input senders
  (and every peer in multi-worker and cluster runs).
- `take_input` / `take_output` / `take_async_input` / `take_async_output` no
  longer consume the port when they fail (for example, a sync take on a port
  spawned with `IoMode::Async`). Previously the endpoint was dropped, closing
  the input or abandoning the output, and the corrected call failed.
- An operator panic (with or without `catch_panics`) now fails the dataflow with
  `Error::OperatorPanic` naming the operator, and tears the executor down so its
  input senders fail and output receivers end. Previously the panic poisoned the
  executor lock and leaked the executor, blocking `collect_data()` and `send()`
  forever, and the error was reported as `RuntimeError::InvalidConfig`.
- Dropping a `RuntimeHandle` resolves every pending completion with
  `Cancelled { reason: RuntimeShutdown }` and releases the dataflows' channels,
  including dataflows that had not been polled since cancellation.
- **Behavior change:** graceful drain (`drain_on_cancel`) now closes external
  inputs as documented: sends after the drain begins fail with `ChannelClosed`,
  every previously accepted event is still delivered, and the drain no longer
  waits for callers to drop their senders. Failure-driven cancellation (operator
  error, failed sibling worker, lost peer) no longer drains, and a sibling
  failure during a drain ends it immediately.
- The task that forwards a `SpawnOptions::cancellation_token` now exits when the
  dataflow finishes instead of living as long as the caller's token.
- `wait_idle()` / `shutdown_async()` no longer return while a multi-worker or
  cluster dataflow is only partially registered.
- Cancellation tokens hold weak wake-handle registrations, so a runtime's root
  token no longer grows with every dataflow ever spawned.

### Probe, notification, and iterate correctness

- Probes report the frontier of the stream they are attached to (the producing
  operator's capabilities plus everything upstream), not the producing
  operator's input frontier. A probe on an input now advances, and a probe
  after `unary_notify`/`reduce`/`delay` waits for that operator's output.
- Probes shared by several workers report the monotone meet of the workers'
  views instead of the last writer's view, so they never move backwards. In
  modes that build the dataflow once per worker with per-worker progress
  tracking (`spawn_multi` when stages have differing parallelism,
  `SimpleRuntime::spawn_multi`), each worker's probe becomes done when that
  worker completes. In a cluster, each worker's probe keeps its last (global)
  view when that worker completes (worker
  completion does not yet wait for peers' final progress), so it may not
  become done after a successful run.
- Notifications that complete together are delivered in timestamp order (a
  linear extension of the partial order). `Timestamp`'s `Ord` must extend the
  partial order, as the built-in timestamps do.
- **BREAKING:** `NotifyContext::notify_at` fails the operator with the new
  `ProgressError::InvalidNotificationTime` when the requested time is neither
  at/after the input frontier, a capability held at the start of the
  activation, nor a pending input time, instead of minting a capability that
  moves downstream frontiers backwards. Re-arming or delaying the time being
  handled inside a notification handler remains valid.
  `NotifyContext::try_notify_at` returns the error instead. `delay` rejects
  delays to incomparable (not only earlier) times.
- Nested `iterate` scopes build and run (the feedback-factory offset was
  computed after the body's feedback edges were appended), and in-flight
  accounting for an `exchange` in a doubly-nested body is projected through
  every scope level (it previously ran unaccounted, which could complete the
  outer epoch early and drop results with several workers). `probe()` and
  `output()` inside an `iterate` body are no longer silently dropped. Inner
  outputs deliver batches at the dataflow's outer timestamp. Inner probes stay
  at the minimum time until the workers writing them have completed, then
  become done (never, in a cluster): data circulating on a loop's feedback
  edge is not yet visible to progress tracking, so no earlier answer is safe.
  Probes downstream of a loop can still report an epoch complete while the
  loop iterates it (pre-existing).
- `take`/`take_while` documentation now states their per-worker semantics.
- `ChangeBatch` drops a lone zero-valued update; the `FrontierAggregator`
  debug monotonicity check no longer rejects valid partial-order advances.

### Stage-task scheduling

- Fix a livelock where a stage task blocked on a full exchange channel used up
  the whole sweep budget, so the stage task that drains that channel never ran.
  Any exchange holding about 1024 queued envelopes hung at 100% CPU, with one
  worker, several workers, or a multi-node cluster. Operators blocked on
  backpressure no longer count as progress.
- A stage task that is skipped because the sweep budget ran out now runs first
  in the next sweep, so a busy stage task can't starve the ones after it.
- Cancellation now takes effect between passes of a sweep instead of after the
  whole sweep budget (up to 1024 activations) is spent.

## [0.1.3] - 2026-10-15

### Added
- Async `spawn_cluster` API (#275)
- Fallible pipe batch combinators (#274)
- Stress benchmarks covering endurance, resource-constrained, and ramp-up workloads

#### Documentation
- Clarify watermark-based progress tracking in the README comparison table (#268)

### Fixed
- Prevent data loss in iterate-plus-exchange flows by accounting for in-flight messages (#280)
- Resolve rustfmt and clippy CI failures (#283)

## [0.1.2] - 2026-05-13

### Added

#### Observability
- `MetricsConfig` granularity controls (`None`, `SummaryOnly`, `Full`) with zero overhead
  when disabled (#239)
- Channel counters — per-edge send/receive/drop counts (#239)
- Activation timeline recording — per-operator activation timestamps and durations (#240)
- Chrome Trace JSON export for Perfetto UI behind `chrome-trace` feature flag (#241)
- Frontier and transfer timeline events for dataflow progress visualization (#242)
- Cluster observability: `SpawnOptions` propagated into `spawn_cluster()` (#243)
- Cluster metrics accessors: `worker_metrics()` and `all_worker_metrics()` on
  `ClusterSpawnedDataflow` (#244)
- Runtime health events for unrecoverable errors (#236)

#### Error Handling
- Structured error types: `CodecError`, `ControlProtocolError`, `CommunicationError`,
  `HandshakeError`, `ProgressError`, `SpawnFailed` — replacing string errors (#233, #234)
- Deferred error collection in dataflow builder — replaces panics (#205)
- Deferred `unary_async` tokio panic handling — converts to operator error (#206)

#### Progress Tracking
- Graph-topology completion propagation using graph structure for precise frontier
  advancement (#237)

#### Cancellation
- Distributed cancellation across cluster nodes via `ControlMessage::Cancel` control
  plane protocol (#238)

#### Communication
- Connection failure recovery with automatic reconnect (#202)
- `sequencing` module — message sequencing primitives for shared connection mode
  - `SequenceCounter`, `ReorderBuffer<T>`, `SequencedFrame`, wire format encode/decode
- `shared_pool` module — adaptive connection pool for shared connection mode
  - `SharedConnectionConfig`, `ConnectionMode`, `RttTracker`, `ConnectionMetrics`, `PeerPool`
- `probing` module — RTT probing and adaptive scaling driver
  - `ProbeMessage`, `ProbeCounter`, `ProbeTimestamp`, `ScalingDriver`, `ScalingEvent`
- `shared_transport` module — shared transport session for pooled multi-dataflow connections
  - `SharedPeerManager`, `SharedTransportSession`, `DataframeSender`, `ConnectionFactory` trait
- Dynamic cluster scaling with topology/membership consolidation (#229)
- `ConnectionFactory` made required; lazy connection initialization (#220)

#### Scheduling
- Wake-based scheduling for `unary_async` operators (#207)
  - `WakeHandle` integration, `ActivationOutcome::WaitingForAsync`, per-operator tracking

#### Performance
- Lock-free SPSC exchange channels (#226)
- Reuse per-target buckets in `ExchangePush` (#227)
- Lazy `VecDeque` allocation — defer until first push (#217)
- Opt-in CRC32 checksums for data frames (#225)
- Multi-worker scaling benchmark suite (#226)
- Comparative benchmark suite: instancy vs timely-dataflow (#212)

#### Testing
- Comprehensive integration tests: Phases 1–5 (#210)
- Failure handling and scaling integration tests (#211)
- 50-minute stress test (#213)
- Cluster observability integration test (#244)
- Async I/O cluster test with cross-node exchange (#245)
- Replace TCP-based reconnect test with in-memory duplex streams (#223)

#### Documentation
- COOKBOOK §11 Cluster Dataflows recipes (#244)
- DESIGN.md §5.5.2 Cluster Startup Protocol (#243)
- GUIDE §8 Distributed Execution updates (#245)
- `lib.rs` doc comments on all crate-level re-exports (#245)
- Dynamic cluster scaling in README (#232)
- Auto-parallelism and wake scheduling docs (#209)
- Transport-agnostic design clarifications (#221)
- Document connection failure and reconnection responsibility (#219)
- DESIGN.md updated to reflect current state (#228)
- LICENSE attribution to timely-dataflow (#248)
- Restructured design docs: split monolithic DESIGN.md into 10 focused topic files
  under `docs/design/` (#249)
- Restructured user docs: split GUIDE.md into 11 tutorial pages under `docs/guide/`,
  new API reference at `docs/reference/api.md`, moved COOKBOOK to `docs/cookbook.md` (#249)

### Changed

#### Runtime
- `SpawnOptions::auto_parallelism` now defaults to `true` (#208)
- Per-stage parallelism is now the default `SpawnOptions` behavior (#198)

#### API
- **BREAKING**: Removed `worker_idx` parameter from `spawn_multi` and `spawn_cluster`
  closure signatures (#204)
- Unified API naming inconsistencies (#216)
- Restricted internal types to `pub(crate)` (#215)
- Removed panics from production code (#214)
- Code quality cleanup: remove production `.unwrap()` calls and dead code (#199, #201)
- CI: build all 35 examples and lint test code (#247)

### Fixed
- Resolve 24 broken rustdoc intra-doc links (#246)
- CHANGELOG: add 8 missing PRs and mark #204 as BREAKING (#248)
- Chrome Trace flow event `id` field placement and frontier change detection (#242)
- Two flaky tests: dispatch race condition and timing jitter (#222)
- Observability tracing tests for parallel execution (#224)
- Shared transport race condition (#200)
- README inaccuracies (#218)

### Added

#### Runtime
- `TokioMode` configuration for `RuntimeHandle` — control how instancy obtains a tokio runtime (#141)
  - `Auto` (default) — reuses the current tokio context or creates a 2-thread runtime
  - `Create { worker_threads }` — creates a dedicated multi-thread tokio runtime
  - `External(Handle)` — uses an externally-provided tokio runtime handle
  - `CurrentContext` — requires an active tokio runtime (errors if none exists)
- `RuntimeHandle::tokio_handle()` accessor for the underlying tokio runtime
- `RuntimeHandle::active_dataflows()` — returns the number of currently running dataflows (#143)
- `RuntimeHandle::wait_idle()` — async method that resolves when all dataflows complete (#143)
- `RuntimeHandle::shutdown_async()` — cancels all dataflows and awaits their completion (#143)
- `Future` impl for `MultiDataflowCompletion` — enables `.await` on multi-worker dataflows (#144)

#### Operators
- `Pipe::unary_async(name, max_concurrency, logic)` — async unary operator that spawns tokio tasks for each input batch (#145)
  - Bounded concurrency control via `max_concurrency` parameter
  - Results arrive in completion order (not input order)
  - Error propagation from async tasks to the dataflow

#### Cancellation
- External cancellation token support via `SpawnOptions::cancellation_token()` — accepts a `tokio_util::sync::CancellationToken` to cancel dataflows from user code (#139)
- `SpawnedDataflow::cancel_token()` accessor for programmatic cancellation
- Waker-based `CancellationToken::cancelled_async()` — replaces 10ms polling with instant notification via `tokio::sync::Notify` (#140)

#### Scheduler Priority & Optimization
- Configurable task scheduling policies via `RuntimeConfig::schedule_policy` (#132, #133, #134)
  - `PriorityPolicy` — schedule higher-priority dataflows first
  - `PriorityWithAgingPolicy` — priority with wait-time bonus to prevent starvation
  - `None` (default) — pure FIFO with O(1) dequeue, zero comparison overhead
  - All policies use `BinaryHeap` for O(log n) insert/dequeue

#### Documentation & Guides
- `COOKBOOK.md` with practical patterns: windowed aggregation, fan-out/fan-in, error recovery (#129)
- `GUIDE.md` troubleshooting section for common issues (#129)
- Comprehensive progress tracking module documentation (#126)

#### Reliability & Observability
- `DataflowBuilder::catch_panics(true)` converts operator panics into `Error::OperatorPanic` instead of unwinding the runtime (#117)
- Async probe notifications via `ProbeNotifier`, allowing `ProbeHandle` waiters to wake promptly on frontier changes (#118)
- `take()` and `take_while()` operators for bounded and predicate-driven stream truncation (#119)
- Per-operator metrics via `DataflowMetrics`, `OperatorMetrics`, and `BackpressureMetrics`; spawned dataflows can access the live metrics via `SpawnedDataflow::metrics()` (#120, #125)

#### CI/CD
- GitHub Actions workflow for automated testing on push and PR (#130)

#### Examples
- Added `cluster_basic` and `cluster_exchange` examples for distributed execution (#123)
- Added `error_handling` example covering `map_ok`, `filter_ok`, and `branch_result` (#124)
- Added `metrics_collection` example showing runtime metrics collection and reporting (#125)

#### Testing
- Scheduler policy integration tests: priority ordering, FIFO fairness, aging, multi-dataflow (#132)
- Progress tracking integration tests: frontier advancement, notifications, iteration (#128)
- Edge case integration tests: empty streams, large batches, deep pipelines, concat (#137)

### Fixed
- **Transport FIFO ordering violation** — data and progress frames could be reordered on the wire due to separate priority channels in the bridge task. Merged into a single FIFO payload channel per peer, preserving the timely ordering invariant (data at time T arrives before frontier advances past T). Also prevents cross-dataflow progress starvation under heavy data load. (#146)
- Control broadcast and cancellation wiring for cluster-local workers (#131)
- Compilation regression in tests/examples from schedule_policy API change (#136)

### Changed

#### Scheduler Configuration (#133)
- **BREAKING:** `RuntimeConfig::schedule_policy` changed from `Box<dyn SchedulePolicy>` to `Option<Box<dyn SchedulePolicy>>` — `None` is the new default (FIFO)

#### Runtime API Simplification (#121)
- **BREAKING:** `RuntimeHandle::spawn()` now takes `SpawnOptions` parameter
- **BREAKING:** `RuntimeHandle::spawn_multi()` now takes `SpawnOptions` parameter
- **BREAKING:** `SimpleRuntime` moved behind `test-utils` feature (use `RuntimeHandle` for production)
- Introduced `SpawnOptions` struct and `IoMode` enum to consolidate channel mode selection
- All examples updated to use `RuntimeHandle` with `SpawnOptions`

### Removed
- **BREAKING:** Removed `async-io` feature — async I/O is now always available (tokio is required)
- **BREAKING:** Removed `RuntimeHandle::run()` and `run_blocking()` — use `spawn().join()` instead
- **BREAKING:** Removed `RuntimeHandle::spawn_async()` — use `spawn(df, SpawnOptions::new().io_mode(IoMode::Async))`
- **BREAKING:** Removed `RuntimeHandle::spawn_multi_async()` — use `spawn_multi(..., SpawnOptions::new().io_mode(IoMode::Async))`

### Internal
- Resolved all clippy warnings across lib, examples, and tests (#135, #142)
- Removed unused capacity parameter from `ChannelFactory::build` (#127)
- Optimized scheduler queue: BinaryHeap for policy-driven scheduling (#133, #134)
- `CompletionNotifier` supports `on_complete` callback for active-dataflow tracking (#143)

### Features

| Feature | Default | Description |
|---|---|---|
| `transport` | ✅ | TCP transport layer (Tokio-based muxer/demuxer) |
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `bincode-codec` | ❌ | Bincode-based codec implementation |
| `test-utils` | ❌ | SimpleRuntime and test helpers |

## [0.1.1] - 2026-05-03

### Fixed
- Fix docs.rs build failure: replace removed `doc_auto_cfg` feature with `doc_cfg`
  (merged in Rust 1.92, see [rust-lang/rust#138907](https://github.com/rust-lang/rust/pull/138907))

## [0.1.0] - 2026-05-03

Initial release of instancy — an async reimplementation of
[timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow) built on
[Tokio](https://tokio.rs/).

### Added

#### Core Dataflow Engine
- Timestamp-indexed progress tracking with frontiers and capabilities
- `MutableAntichain` and `ChangeBatch` for frontier management
- `Product<TOuter, TInner>` timestamps for nested scopes
- Cooperative `CancellationToken` with `CancellationReason` diagnostics
- `Result<T, Error>` error handling throughout (public APIs avoid panics)

#### Operator API
- `DataflowBuilder` with fluent chaining API for graph construction
- Core operators: `source`, `input`, `map`, `flat_map`, `filter`, `inspect`
- Stateful operators: `unary`, `binary`, `unary_notify`
- Stream combinators: `concat`, `branch`
- Feedback loops: `iterate` with nested scope support
- Data repartitioning: `exchange`, `exchange_by_hash`
- Observation: `probe` (frontier tracking), `output` (result collection)

#### Execution Modes
- `RuntimeHandle` — multi-worker async execution on a shared thread pool
- `spawn` / `spawn_multi` — background dataflow execution with channel I/O
- `spawn_cluster` — multi-node distributed execution over TCP
- `SpawnOptions` with `IoMode::Sync` / `IoMode::Async` for channel mode selection
- `AsyncInputSender` and `AsyncOutputReceiver` for async channel-based I/O

#### Networking (`transport` feature)
- Application-managed connections via `ConnectionManager` trait
- `TransportSession` with multiplexed framed transport
- Priority-separated channels for data and progress messages
- Fingerprint-based handshake and ready barrier protocol
- `MockNetworkEdgeMaterializer` for single-process multi-node testing

#### Serialization
- Pluggable `Codec` trait for custom serialization
- Built-in codecs for primitives, tuples, strings, `Vec<u8>`, `Product`
- `ExchangeData` trait for types participating in cross-worker exchange
- Optional `BincodeCodec` (`bincode-codec` feature)

#### Multi-Worker Exchange
- Hash-based data repartitioning across logical workers
- Cross-worker frontier aggregation
- `EdgeMaterializer` trait for pluggable exchange transport
- `NetworkEdgeMaterializer` for real TCP-based exchange

#### Documentation
- Comprehensive `README.md` with quick-start examples
- `GUIDE.md` — detailed usage guide covering all features
- `DESIGN.md` — architecture and design decisions
- 24 runnable examples covering all major features
- `doc_auto_cfg` for automatic feature-gate annotations on docs.rs
- `deny(rustdoc::broken_intra_doc_links)` for documentation correctness

#### Testing
- 1000+ unit and integration tests
- TCP-based cluster integration tests
- Parallel dataflow stress tests
- Cross-process integration test framework (`instancy-integration` crate)
- In-memory transport for deterministic multi-node testing

### Features

| Feature | Default | Description |
|---|---|---|
| `transport` | ✅ | TCP transport layer (Tokio-based muxer/demuxer) |
| `tracing` | ✅ | Structured logging via the `tracing` crate |
| `bincode-codec` | ❌ | Bincode-based codec implementation |
| `test-utils` | ❌ | SimpleRuntime and test helpers |

[0.2.0]: https://github.com/Yaming-Hub/instancy/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/Yaming-Hub/instancy/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Yaming-Hub/instancy/releases/tag/v0.1.0
