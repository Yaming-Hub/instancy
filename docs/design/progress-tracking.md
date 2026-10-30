# Progress Tracking

This document explains how instancy decides that no more data can arrive at a timestamp, when an operator may shut down, and when a dataflow has finished. It covers single-worker, multi-worker and cluster execution, feedback loops and probes.

It is an overview. The full design is [Progress accounting: invariants, inventory and redesign](./progress-accounting-design.md): the invariants (S1–S3, A1–A5), the proofs they rely on, the alternatives considered and the verification plan. Decisions taken while implementing it are in the [decision log](./progress-accounting-decisions.md). "Design §x" below points into the design doc.

Back to the overview: [Design Overview](./README.md)

## 11. Progress Tracking Details

Progress tracking is the heart of instancy's execution model. It determines when timestamps are complete (no more data can arrive), when operators should be notified, when feedback loops should terminate, and when the entire dataflow has finished.

instancy uses **port-counted progress (PCP)**, the same model as timely dataflow. Every record is counted at an operator port from the commit that produces it until the commit that consumes it. Everything an operator keeps across calls is covered by a capability. Progress is tracked per location, not per worker.

### 11.1 Core Concepts Recap

**Locations and pointstamps.** A location is an operator port: `Source(op, out)` is an output, `Target(op, in)` is an input. A pointstamp `(location, t)` carries a count:
- at `Source(op, out)@t`, the capabilities `op` holds: it may still produce at `t` on `out`;
- at `Target(op, in)@t`, the records at `t` produced towards that input and not yet consumed.

The reachability `Tracker` inside each `ProgressTracker` keeps these counts and computes their implications through path summaries.

**Capabilities.** A `Capability<T>` is permission to produce output at `t`. Creating, downgrading and dropping one records `+1`/`-1` at `Source(op, out)`. Closure operators never see a capability. They hold one through `notify_at`, `OutputHandle::retain` or implicitly (sources, `unary_async`), and every request must be justified (§11.2.1).

**Port counts.** When an operator produces `n` records at `t` on an output, it counts `+n` at the target of every edge of that output. When the target consumes them, it counts `-n` there. The count covers the records wherever they are in between (§11.3.1).

**Path summaries.** The graph's structure defines summary functions from each output port to reachable downstream input ports. For a simple pipeline, the summary is identity (timestamp passes through unchanged). For a feedback loop, the summary advances the loop counter (e.g., `t → t + 1`). The tracker uses these to compute: "if work exists at location A at time T, what is the earliest time data could arrive at input B?"

**Frontiers.** An input's frontier is the antichain of minimal timestamps that could still arrive there. Only **positive** counts contribute, so a transiently negative count (§11.3) neither holds a frontier back nor lets it advance. When `t` leaves an operator's input frontier, the operator is notified that `t` is complete.

**Completion.** Three separate decisions (§11.6). An operator shuts down when its input frontiers are empty and it holds nothing. A worker succeeds when all its operators are shut down and its view of every location that can reach them is exactly zero. The dataflow completes when every worker has succeeded. Counts are checked one pointstamp at a time, never summed: a positive count at one location and a negative count at another never cancel.

### 11.2 Single-Worker Progress Flow

```
┌────────────────────┐   port counts and       ┌──────────────────┐
│ Operators, each in │   capability changes    │ ProgressTracker  │
│ an operator shell  │ ──────────────────────► │                  │
│ (counted ports)    │                         │  Reachability    │
│                    │ ◄────────────────────── │  Tracker         │
└────────────────────┘    frontier updates     └──────────────────┘
```

Each executor sweep:
1. **Activate**: scheduled operators run. Each operator's shell records what it consumed and produced in its port ledgers. Capability changes go to its capability reporters.
2. **Commit**: `propagate_progress` drains every hosted port ledger and capability reporter into one batch and applies it to the tracker.
3. **Propagate**: the tracker computes the new frontiers. Operators whose input frontiers changed are scheduled.
4. **Decide**: the decision module (`decide()`) shuts down operators and checks whether the worker has finished (§11.6).

Decisions only ever see committed, propagated state.

#### 11.2.1 Capabilities and Justification

An operator may emit output at `t`, or request a capability at `t` (`notify_at`, `retain`, a downgrade), only if the current logic call **justifies** `t` (check V3, design §5.1.3). Each call has a justification set `J`:
- the times of the capabilities the operator held when the call started (including one that `next_notification()` drops during the call);
- the times of the batches the call consumed from its inputs.

The output or request is allowed if some element of `J` is at or before `t`. Otherwise the operator fails with `ProgressError::Unjustified`. The check runs in every build. The input frontier alone never justifies anything, and neither does a batch that is waiting but not taken.

The rule ensures every positive update is committed together with the consumption or the held capability that justifies it. A frontier-based rule is unsafe across workers. Worker A holds a capability at 0 and sends B a record at 10. B calls `notify_at(0)`, which its frontier still permits. A then releases its capability. Worker C applies A's release before B's new `+1`, sees nothing left at 0 and notifies 0. B later emits at 0.

Keeping data across calls:
- **`OutputHandle::retain(t)`** returns a `Retained`: a capability at `t` that keeps `t` open across calls. It is `Send` but not `Clone`. Dropping it releases `t` from any thread and wakes the operator. An operator that holds a `Retained` is re-activated when its input frontier changes.
- **Final call.** Every closure operator (`unary`, `binary`, the notify family) gets exactly one final call when its input frontiers become empty. Data stashed without `retain` and emitted in that call fails loudly (`Unjustified`) instead of arriving behind the frontier.
- **`unary_async`** holds a capability from the call that takes a batch until the call that hands its result on.

One limit remains, as in timely: a closure that stashes consumed data and never emits it looks exactly like a filter.

### 11.3 Multi-Worker Progress Exchange

When several workers run a dataflow, each has its own `ProgressTracker` over the whole graph. Records cross workers: worker A may produce records that worker B consumes. A counts `+n` at B's input, and B counts `-n` there when it consumes them. Each worker sends every commit to every peer, so every view eventually holds both. Following timely dataflow, there is no global barrier: each worker decides from its own view.

```
                    ┌─────────────────────────────────────────────┐
                    │     Progress lanes (one per worker pair)     │
                    └─────────────────────────────────────────────┘
                             ▲                       ▲
                     commits │               commits │
                             │                       │
Worker 0:                    │         Worker 1:     │
┌────────────────┐           │         ┌────────────────┐
│ Operator shells│           │         │ Operator shells│
│   ▼ ledgers    │           │         │   ▼ ledgers    │
│ ProgressTracker│──send────►│◄──send──│ ProgressTracker│
│   │            │           │         │   │            │
│   │  ◄─receive─┼───────────┘         │   │  ◄─receive─│
│   ▼            │                     │   ▼            │
│ (global view)  │                     │ (global view)  │
└────────────────┘                     └────────────────┘
```

Each worker's sweep becomes:

1. **Activate** operators.
2. **Commit**: drain every hosted port ledger and capability reporter into one batch, compacted worker-wide, and apply it to the worker's own view.
3. **Send** that batch to every peer.
4. **Receive** the peers' batches and apply them, in commit order per sender.
5. **Propagate**, with the loop-scope import and export (§11.4.3).
6. **Decide** (§11.6).

**Transient negative counts.** B's `-n` may reach worker C before A's `+n`. C's view then holds `-n` at B's input for a while. This is expected (Naiad, timely). Frontiers use positive counts only, and completion needs every count to be exactly zero, so the negative count neither advances a frontier nor completes anything. While A's `+n` is in transit, C still sees what justified it (A's capability, or the records A consumed): their release, if any, travels in the same commit as the `+n` (§11.2.1).

#### 11.3.1 Port Counting

Every operator built by `DataflowBuilder` runs inside an operator shell (`dataflow::shell`) that owns all its port I/O. The shell does the accounting once, for every operator kind:

- A **counted input** buffers what its channel delivers. Taking a batch out (`InputHandle::next`) consumes it: the shell records `-n` at `Target(op, in)@t` when the call returns. Data still in the buffer or the channel stays counted.
- A **counted output** collects the logic's output. When the call returns, the shell takes it, records `+n` and moves it to a retry queue, from which it goes into the channel as capacity allows.
- `+n` lands at the target of every edge of the output, keyed by the edge. A broadcast edge (`Delivery::AllTargetHosts`) counts `n` once per worker that hosts the target.

Everything between the two ports is **on the edge** and is counted at the consuming input: retry queues, channel queues, exchange buckets, network outboxes, receive bridges and the operator's own input buffer. No channel or transport contains accounting code. A new channel or transport only has to honour the edge contract: deliver every accepted batch exactly once, keep its timestamp, and keep the record count per timestamp (times the edge's multiplicity).

Other rules:
- A failed or panicking activation commits nothing. Its consumed input stays counted upstream, so no view can complete on it.
- A sink that hands records to an external channel keeps its input credit until the channel accepts each event.
- An error envelope at an input fails the operator on every transport (`ProgressError::UpstreamError`).
- A record that reaches an operator after it shut down is an error (`ProgressError::RecordAfterShutdown`).
- Logic cannot bypass the shell: `Push` and `Pull` are sealed, and the port handles can only take input, read frontiers and fill output sessions.

Port counting replaced the earlier in-flight reporters, which counted only exchange channels, and only where the producer side was wired by hand (audit C2, H1, H12). `set_inflight_reporter`, `register_inflight_reporter` and the in-flight recording in `ExchangePush`/`ExchangePull` were deleted.

#### 11.3.2 Progress Channel Architecture

For N workers, there are N × (N-1) unidirectional FIFO lanes:
- Each worker gets (N-1) senders (one to each peer) and (N-1) receivers (one from each peer).
- A message is one commit, a `ProgressBatch`: the root scope's changes plus one section per loop scope that changed (§11.4.3). Each `ProgressChange` has a `kind` (`Source`: a capability change; `Target`: a record count at an input), `node`, `port`, `time` and `diff`. A commit's sections travel together.
- A receiver applies each sender's batches in commit order. There is no ordering constraint between senders.
- Senders wake the target worker's `WakeHandle`, so progress arrival wakes an idle worker.
- A finished worker seals its stream (§11.6.3). In-process, the lane records the seal and, when the sender is dropped, that it closed. On a cluster, both are marker frames.
- Across nodes, progress frames travel on the sender's ordered per-peer outbox, which is never cut short by cancellation, so a node's final deltas (e.g. its last capability release) are delivered even when it finishes first. Each cross-node progress channel ends with an explicit end-of-stream frame on a clean shutdown (see [cluster.md §5.5.3](./cluster.md#553-cluster-shutdown-protocol-clean-teardown)).

#### 11.3.3 Initialization: Seeding from the Physical Plan

Every worker's view must hold every peer's initial capabilities before it computes a frontier. Otherwise a fast worker could see incomplete global state and make incorrect frontier or completion decisions (audit H13: in a cluster, a peer's initial capabilities, if they were sent, could still be in transit).

instancy does not send initial capabilities. Every operator declares them at build time (`add_operator_with_capabilities`), and the spawn computes a **physical plan** (which workers run which operator). Initialization seeds each declared capability once per worker that runs its operator, in every scope, into every worker's tracker, whether or not that worker runs the operator. Nothing is sent. Each instance's later release (`-1`) travels on the progress channels as usual. Seeding fewer copies would let a frontier pass work; seeding more would never complete. The plan's digest is part of the cluster handshake fingerprint, so two nodes cannot seed different counts. A debug assertion rejects a capability an operator creates at materialization without declaring it.

The runtime registers workers on the task pool only after all of them are materialized, so a failed materialization never leaves siblings running.

### 11.4 Progress Tracking in Feedback Loops

A feedback edge creates a cycle in the dataflow graph with a summary that *advances* the timestamp (e.g., `t → t + 1` for a loop counter). This is what lets the frontier advance with each iteration and the loop terminate.

#### 11.4.1 How Feedback Loops Terminate

Consider an iterative computation with a loop:

```
Input ──► Operator A ──► Exchange ──► Operator B ──┐
                ▲                                   │
                └───── Feedback (t → t+1) ──────────┘
```

Times inside the loop are `Product<T, TInner>`: `(epoch, iteration)`.

1. **Iteration 0**: input enters at `(0, 0)`. A processes it and sends records through the exchange to B. Each batch is counted at B's input until B consumes it. B sends records back on the feedback edge. They stay counted until A consumes them, and the back-edge's summary moves them to `(0, 1)`.
2. **Iteration 1**: A consumes the feedback records at `(0, 1)` and produces more. The pattern repeats one iteration higher.
3. **Convergence**: eventually B sends nothing back. No record is left on the feedback edge or in the body, and no body operator holds a capability at epoch 0. The loop's scope then holds no internal work at epoch 0, so the loop's output frontier passes epoch 0 (§11.4.3).
4. **Cross-worker correctness**: worker 0 sees worker 1's records and capabilities in its own view, so it only sees the loop finish epoch 0 once no worker has anything left in it.

#### 11.4.2 Why Global Barriers Are Not Needed

A naive approach would use a global barrier: "wait until all workers agree the loop is done." This is expensive and serializes workers across iterations.

instancy (following timely dataflow) avoids barriers entirely:
- Each commit is sent to all peers as soon as it is made.
- Each worker's tracker computes implications from all the counts it knows.
- If worker 0 holds a capability, or has unconsumed records, at `(0, 5)`, every worker's view shows it, and no frontier in the loop passes iteration 5.
- Only when that work is gone does each tracker independently conclude that the frontier has advanced.

This is decentralized agreement through broadcast: no coordination, no leader, no barrier.

#### 11.4.3 Nested Loop Scopes

Each `iterate` is its own progress scope (design §5.1.7), as in timely's subgraphs:
- Each worker keeps one child tracker per loop, in `Product<T, TInner>` time, with its own summaries. Every worker keeps every scope's tracker. Nested loops nest further.
- In the parent scope, the loop is one node `S` with one input and one output. `enter`'s input is counted at the parent's `Target(S, 0)`. `leave`'s records are counted on the parent's edges from `Source(S, 0)`. The body's operators, `concat`, `feedback` and `leave` are nodes of the child scope.
- **Import**: the parent's frontier at `Target(S, i)` becomes the child's pointstamps at its boundary `Source(0, i)`, at `(t, TInner::minimum())`. The child's frontiers, and so its notifications, respect parent records that have not entered yet.
- **Export**: the child's internal work, excluding the imported pointstamps, is projected onto the outer time and becomes the parent's pointstamps at `Source(S, o)`. Exporting the ordinary child frontier would turn the parent's own future input into self-sustaining work, which never drains on an outer feedback path.
- Import and export are derived by each worker from its own view and are never sent. Commits carry raw port counts only, one section per scope. A propagation applies every scope's raw counts, then the exports bottom-up, then propagates top-down, importing each parent's frontiers before the child propagates. Exact-zero completion looks at raw counts only.

Consequences:
- Probes, `retain` and `unary_notify` inside a loop body use the body's frontiers, in `Product` time. Records circulating on the feedback edge hold them.
- Outside the loop, the loop's output frontier holds each outer time until no record of that time remains anywhere in the loop.
- A loop whose summary doesn't advance the iteration counter (for example `0u32`) is an identity cycle: its frontier could never advance. The dataflow fails to build. (timely only warns.)
- A loop's back-edge is never bounded, and a capacity set on the pipe returned as `feedback` is ignored. A cycle of bounded channels can fill up and deadlock (liveness rule L6). Inflow is still throttled at the loop's entry, and memory is bounded by the loop's working set, as in timely.

#### 11.4.4 Exchange + Feedback Interaction

The case that combines exchange with feedback:

```
Worker 0:  Input ──► Op A ──► Exchange ──► Op B ──┐
Worker 1:  Input ──► Op A ──► Exchange ──► Op B ──┤
                      ▲                            │
                      └──── Feedback (t→t+1) ──────┘
```

Data from worker 0's Op A may be routed to worker 1's Op B, and vice versa. Progress tracking must ensure:

- Worker 0 doesn't conclude iteration N is complete until worker 1 has also finished iteration N. Every view holds every worker's counts (§11.3).
- Data in transit through the exchange is accounted for, even after the sender has released its capability. Port counting covers it at Op B's input until Op B consumes it (§11.3.1). Loop-body operators need no capability across a push.
- Feedback data doesn't cause a premature frontier advance. Records on the back-edge stay counted at the `feedback` operator's input in the loop's scope until it consumes them. Its summary advances the iteration counter, so records sent back from iteration N hold iteration N+1 at the loop head.

This combination used to lose data under the old in-flight accounting (issue #277). Under port counting it needs no special handling.

### 11.5 Logical Progress Exchange (Physical-Layer Independence)

Progress exchange is a purely logical concept. The `ProgressTracker` exchanges commits between logical workers without knowing whether those workers are:
- on the same OS thread (in-process shared buffers);
- on different threads in the same process (same mechanism);
- on different machines (serialized frames over the cluster transport).

The tracker uses one sender and one receiver per peer (crate-private, simplified here):

```rust
impl<T: Timestamp> ProgressSender<T> {
    /// Sends one commit (every scope's section) to the peer.
    fn send_batch(&self, batch: ProgressBatch<T>);
    /// Seals the stream, in order behind every batch sent before.
    fn seal(&self);
}
// Dropping a sender closes the lane; an unsealed close means the worker stopped unfinished.

impl<T: Timestamp> ProgressReceiver<T> {
    /// Drains queued batches in FIFO order, with the lane's state (sealed, closed).
    fn drain_with_state(&self) -> Result<(Vec<ProgressBatch<T>>, LaneState)>;
}
```

The physical layer provides the concrete implementation:

| Scenario | Physical Implementation |
|----------|----------------------|
| Same process | `Arc<Mutex<SharedBuffer>>` + `WakeHandle::notify()` |
| Cross-process | Serialize the commit → per-peer outbox → transport → receive bridge → the same `SharedBuffer` |

This mirrors the logical/physical separation for data channels (§4.5): the `TransportProvider` resolves logical data targets to physical delivery, and the progress exchange resolves logical progress targets to physical progress delivery.

#### 11.5.1 Cross-Process Progress Exchange

Across nodes, each worker pair has a network progress lane (`progress/network_progress.rs`). The wire protocol is version 5 (`PROTOCOL_VERSION`):

```
Batch frame:
  [count: u32]
  for each change:
    [scope: u32]      0 = the root scope, else a loop scope's id
    [kind: u8]        0 = Source (capability), 1 = Target (records at an input)
    [node: u64]
    [port: u64]
    [timestamp]       encoded with the scope's codec
    [diff: i64]
  [crc32: u32]

Marker frames: [reserved count][crc32]
  u32::MAX       end-of-stream
  u32::MAX - 1   end-of-stream acknowledgement
  u32::MAX - 2   seal
  u32::MAX - 3   abandon (the stream ended without a seal)
```

**Split commits go positives first.** A commit with more than `max_batch_size` changes is split into several self-contained frames. Every positive change, of every scope section, is sent before any negative one. A receiver applies each frame as it arrives, so every intermediate view is at least as large as the view before the commit or the view after it, and both are safe (design §2.5). A release sent ahead of the count it supports could let a frontier advance too early.

**Ordering of data against progress.** Progress frames go through the per-peer outbox, which is unbounded, FIFO and never drops a frame. Data frames go straight into the bounded transport queue. A record can therefore reach a peer before the batch that counts it. That is safe: if the receiver consumes it first, its `-n` is transiently negative (§11.3), and the sender's justification is still counted in every view that hasn't applied the sender's commit. Safety does not need data and progress on one FIFO lane. Only teardown relies on lane order: the seal follows the worker's last commit, and the end-of-stream follows the seal.

#### 11.5.2 Progress and the Adapter Layer

The progress exchange fits the three-layer architecture (§4.5):

```
┌─────────────────────────────────────────────────────────────────┐
│                   Logical Layer                                 │
│                                                                 │
│  ProgressTracker: sends and applies commits between logical     │
│  worker IDs. No knowledge of physical topology.                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │  ProgressSender / ProgressReceiver
┌──────────────────────────▼──────────────────────────────────────┐
│                   Adapter Layer                                 │
│                                                                 │
│  Progress channel creation: resolves (source_worker,            │
│  target_worker) to a local lane or a network progress lane.     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   Physical Layer                                │
│                                                                 │
│  In-process: Arc<Mutex<SharedBuffer>> + WakeHandle              │
│  Cluster: per-peer outbox, transport session, receive bridge    │
└─────────────────────────────────────────────────────────────────┘
```

### 11.6 Completion: Shutdown, Worker Success and Seals

Every completion decision goes through one decision module (`DataflowExecutor::decide`), which runs right after each commit and propagation (design §5.1.1, §5.1.5). It takes three kinds of decision.

#### 11.6.1 Operator Shutdown (S2a)

An operator instance shuts down when every input frontier is empty in this worker's view and it holds nothing: no capability, no `Retained`, nothing left to hand over (retry queue, async task, external egress). It then closes its outputs. An operator's own `Done` or its channels' end of stream is only a hint. By S1, an empty input frontier means no record can ever arrive there. A record that does arrive is an error (`ProgressError::RecordAfterShutdown`).

#### 11.6.2 Worker Success (S2b) and Dataflow Completion (S2c)

A worker succeeds and retires when every hosted operator is shut down and its view holds no non-zero count, positive or negative, at any location in its **scope**. The scope is every location from which a location the worker hosts is reachable in the data graph, with feedback edges and loop-scope boundaries included and timestamps ignored. Hosted locations include the worker's probes and output ports. A non-zero count means "not ready", not an error: a peer's batch may still be in transit.

Progress is per location, not per worker, as in timely. On a uniform plan every worker hosts every operator, so every worker's scope contains every input. On a cluster, no node finishes until every node's input has closed and every record is consumed. A worker finishes early only when no remaining work can reach anything it hosts, for example an in-process plan whose stages run on different workers (§11.8). `join` doesn't wait for a peer's teardown.

The dataflow completes when every worker has succeeded. No worker has to stay behind as an observer: every location is hosted somewhere, so the scoped checks together cover the whole graph.

Two older rules are gone. Idle-sweep force-close closed every operator after `max_idle_sweeps` idle sweeps once a signed sum of counts reached zero. A success shortcut reported a worker done once every local operator was done. Both could complete while records were still in flight (audit C1, issue #277).

#### 11.6.3 Seals and Residuals

A worker **seals** its progress stream once it can produce no further update: every hosted operator is shut down or exhausted (every input at end of stream, buffers empty, no capability, nothing to hand over), and its last commit has been sent. The seal travels in order behind that commit, and ahead of the end-of-stream on a cluster. A receiver counts a peer as sealed only once it has applied the seal, after every earlier batch from that peer.

Once a worker has applied every worker's seal, its own included, no further update can arrive. A non-zero count still left in its scope is then an accounting bug. The worker fails with `ProgressError::Residual` ("progress residual at completion"), listing the counts, instead of hanging. Two workers stuck on the same residual both reach this point and both report it.

A failed worker never seals. Its lane ends unsealed (with an abandon marker on a cluster), and the dataflow fails with the worker's error through cancellation. During a graceful drain (`drain_on_cancel`), if a peer stops without sealing while this worker's view still holds work the drain needs, the worker fails promptly ("peer … stopped before finishing its part of the cancelled dataflow, which the drain needs") after a 500 ms quiet grace for records the peer sent before it stopped.

#### 11.6.4 Parking

`max_idle_sweeps` (`DataflowBuilder::max_idle_sweeps`; the runtime default is 64) only decides when an idle worker parks. After that many sweeps without progress, the worker parks until something wakes it: data, a peer's progress, a released `Retained`, an async task or an external input. It is a CPU/latency knob. Results never depend on it.

#### 11.6.5 The Debug Checker

`progress/check.rs` is an independent oracle, compiled into debug builds (and release builds with the `progress-check` feature). It measures where work physically is (channels, operator stores, live capabilities) without reading any ledger, and checks every worker's published frontiers against it: late data (V1), unjustified output (V3), sealing or completing with work held (V4), residuals (V5), wire rules (V6) and the occupancy oracle (V7). It panics on a violation by default; `INSTANCY_PROGRESS_CHECK` selects the mode.

#### 11.6.6 Diagnosing a Progress Failure

A progress failure that shows up once in CI rarely reproduces locally. Examples are an S3 data loss ("records sent after the peer finished"), a proven residual and a checker finding. The flight recorder (`progress/trace.rs`) makes one CI artifact enough to attribute such a failure. Like the checker, it is compiled only into debug builds and builds with the `progress-check` or `test-utils` feature, never into a release build without them. It records only when `INSTANCY_TRACE_DIR` names a dump directory. CI sets it for the `test` and conformance jobs. It uploads the `test` job's dumps when that job fails, and the conformance job's whenever there are any: a case that passes only on a later attempt (flaky) still leaves the dump of its failed attempt.

**What it records.** Each worker has a bounded ring of fixed-size events (`INSTANCY_TRACE_EVENTS` per ring, default 16,384, about 1 MiB). Each node also has one ring per peer link. Rings are lock-free: only the executor's thread writes a worker's ring, and a dump reads it without blocking that thread.

A worker's ring holds:

- every commit, with each change, its sequence number and the frames each network lane got;
- every peer batch applied, with each change and its position on the lane;
- seals sent and applied, and the lane states;
- the worker's scope (the locations S2b judges) and the seeded initial capabilities;
- the decisions:
  - S2a shutdowns;
  - S2b success or not-ready, with every non-zero raw count in scope (not-ready only once every hosted operator is shut down or exhausted);
  - residuals and S3 failures, with each peer lane's position at that moment;
- the executor's exit, with the path that produced it, and what the coordinator was told;
- the data path: output counted as produced, pushes and their outcomes per target, network sends and receives (each data frame numbered on its channel), and input pulls.

A link's ring holds:

- the frames received;
- the progress markers (seal, abandon, end-of-stream, ack);
- each link event and whether it retired the link;
- the coordinator's goodbye (recorded before its end-of-stream frames are queued), acks and teardown.

Commit numbers never travel on the wire. Each side derives them: an in-process lane carries one batch per commit, and a network lane one batch per frame, which the sender's `frames_before`/`frames` locate.

**When it dumps.** The runtime dumps at detection, at most once per dataflow and trigger:

- a push of records to a retired link (data loss);
- records that arrive for a finished operator;
- a proven residual;
- any checker finding;
- an explicit call to `check::dump_traces(id, reason)`.

A dataflow that was dumped writes a final dump when it ends. The conformance harness dumps every case whose outcome does not match the golden file, and prints the dump directory and the analyser's verdict.

A dump goes to `$INSTANCY_TRACE_DIR/<dataflow-id>/` and contains:

- `header.json`: the reason, a legend for every field, operator and edge names, and how a network channel id decodes;
- `reason.txt`;
- one JSON-lines file per ring: `<node>-w<worker>.jsonl` and `<node>-bridge-<peer>.jsonl`.

A later dump of another trigger goes to a numbered subdirectory; `check::dump_traces` returns the directory of the dump it wrote. The header counts every gap:

- events overwritten in a ring, and slots torn while dumping;
- events at a site outside any traced worker;
- commits and markers a retired link dropped;
- finished dataflows evicted from the registry (the last 32 stay).

A dump covers every ring of the dataflow in the process: every node of a cluster that runs in one process, as in tests. A cluster whose nodes run in separate processes dumps each process's own rings.

**Reading a dump.** `tests/support/trace_verdict.rs` (`trace_verdict(dir)`) reads one dump. It names the proven residual counts (operator, port, time) and the records lost to a retired link, with what retired it and what their sender had committed at their time. It also gives the evidence for three hypotheses about an S3 or residual failure:

- **(a) A missing `+n`.** At a worker's S2b success, a `+n` that a peer committed before it, at a location in the worker's scope, was not among the changes the worker had applied from that peer. The commit's batches on the lane are located through `frames_before`/`frames`, so a fragment still pending that holds only negatives is not evidence. A commit that releases a capability without the `+n` for records already sent is listed with the lost records, not judged: a `+n` is counted when output is taken, possibly sweeps before the push, and is consolidated within its commit.
- **(b) A duplicate push.** A pusher delivered more records at a time than its port produced, a worker sent more records over the network to one worker than its port produced (a broadcast sends each record to every worker), or a channel received more records, or a higher frame number, than its sender sent. A receive is judged only once the sender's ring has recorded past it, so a dump taken between a send and its record is not evidence.
- **(c) A success outside S2b.** A success was returned or reported without the decision module's S2b decision. At the coordinators: a node applied more worker successes than its workers reported, said goodbye before each of its workers reported success, or retired a link on its peer's end-of-stream before every progress channel from the peer ended or before the peer said goodbye.

Checks that need a complete ring are skipped on a truncated one, and the verdict says so. Rings are read one after another, so each ring header gives the time it was read (`snapshot_t_ns`): a check that needs a ring's events up to some moment is made only if the ring was read after it.

### 11.7 Probes

```rust
impl<T: Timestamp> ProbeHandle<T> {
    /// True once no more data at times ≤ `time` can appear on the stream.
    pub fn done_with(&self, time: &T) -> bool;
    /// The current frontier, as a snapshot.
    pub fn frontier(&self) -> Antichain<T>;
    /// True once the frontier is empty.
    pub fn is_done(&self) -> bool;
    /// Waits until the frontier passes `time`.
    pub async fn wait_until_done_with(&self, time: &T) -> Result<(), Error>;
    /// Waits until the frontier is empty.
    pub async fn wait_until_done(&self) -> Result<(), Error>;
    /// A watch receiver for frontier changes.
    pub fn subscribe(&self) -> watch::Receiver<Antichain<T>>;
}
```

Probes are read-only projections. Nothing is counted at a probe, so nothing can leak there.
- A probe on `(op, out)` reports the meet of the input frontiers of that output's targets. That includes records still queued on the stream, the operator's own capabilities and everything upstream. If the stream has no consumer, it reports the output's own frontier.
- Inside a loop body, a probe reports the body's frontier, in `Product` time (§11.4.3).
- Each worker that hosts the probed operator writes to the probe. The probe reports the meet of the writers' views and never moves backwards. A finished writer is dropped from the meet.
- When a worker retires successfully, its writers finish with an empty frontier. Every probe is therefore done after a successful run, on every path: in-process, `SimpleRuntime::spawn_multi`, clusters and loop bodies. A writer whose worker fails finishes with its last view.

### 11.8 Stages, Hosting and Heterogeneous Parallelism

Every worker runs one `ProgressTracker` over the whole graph (the root scope), plus one child tracker per loop scope (§11.4.3), whatever stages it hosts. One executor core (`DataflowExecutor`) hosts the operators the physical plan assigns to the worker. Fusion groups and stage tasks are scheduling groups inside it and don't affect progress. (Per-stage trackers, `StageExecutor` and the `FrontierAggregator` design were removed in step 2 of the redesign, design §5.1.8.)

The physical plan is the only thing that decides hosting. Materialization, seeding, probes, the handshake digest and the checker all read it:
- `hosts(op)` is workers `0..par(stage(op))`. Each hosted operator keeps its stage's worker context.
- Each initial capability is seeded `|hosts(op)|` times (§11.3.3).
- An exchange or broadcast edge is built on every worker that hosts either end. A broadcast counts each batch once per worker that hosts the target.
- A pipeline or feedback edge needs every worker that hosts its source to also host its target; otherwise spawn fails. Workers that host only the target get an input that is already exhausted.
- A probe on `op` expects `|hosts(op)|` writers.

Stages can have different parallelism in-process (`RuntimeHandle::spawn_multi` with per-stage parallelism, the default). Every worker's view still covers the whole graph, but each worker's scope (§11.6.2) covers only what can reach the operators it hosts. A worker that hosts only upstream stages can therefore retire before downstream stages finish. Clusters and `SimpleRuntime::spawn_multi` still require every explicit stage parallelism to equal the total worker count.

### 11.9 Comparison: instancy vs timely-dataflow Progress Tracking

PCP follows timely's progress model: counts at ports, positive-count frontiers, nested scopes for loops, and per-location progress. The differences are in execution, enforcement and failure reporting.

| Aspect | timely-dataflow | instancy |
|--------|----------------|----------|
| Where work is counted | Operator ports: produced, consumed and internal changes, published after each `schedule()` | The same, recorded by the operator shell; one commit per sweep |
| Trackers | One per worker per scope (subgraph) | One per worker for the root scope, plus one per loop scope; each covers its whole scope |
| Progress messages | Each subgraph sends its own progress | One batch per commit with one section per scope; split commits send positives first across sections |
| Initial capabilities | Seeded `peers` times on every worker | Seeded once per hosting worker, from the physical plan; the plan digest is in the cluster handshake |
| Frontiers | Positive counts | Positive counts |
| Worker completion | The worker's tracker tracks nothing (positive count) | Exact zero per pointstamp over the worker's scope, decided by one decision module |
| A lost or leaked count | A leaked count hangs; a missing one can go unnoticed | Either hangs until every seal is applied, then fails with `ProgressError::Residual` |
| Justification | By API shape: capabilities come from input messages or held capabilities | Closures see no capabilities; the shell checks every output and capability request against `J` in every build (V3); `retain` keeps data across calls |
| Loops | Nested subgraph, executed as one operator of its parent | Nested progress scope; execution stays flat (body operators are fused and scheduled with the parent's); identity cycles rejected at build time (timely warns) |
| Execution | One OS thread per worker | Executor futures on a shared Tokio pool; fusion; per-stage and heterogeneous parallelism |
| Sending | Demand-driven by default | Every commit sent; demand-driven sending is an optional later optimization |

#### What instancy preserves from timely

- **Capabilities** represent permission to produce data at a timestamp.
- **Pointstamps** count outstanding work: capabilities at outputs, records at inputs.
- **Path summaries** describe timestamp transformation across edges.
- **Antichains** represent frontiers compactly.
- **Reachability** determines how progress changes propagate through operators.
- **Shutdown**: an operator shuts down when its input frontiers are empty and it holds no capability.
- **No barriers**: each worker decides from its own view of everyone's commits.

#### What instancy changes

- **Execution model.** Workers are async executor futures on a shared Tokio pool, not OS threads. Stages can be fused, and stages can run on different numbers of workers in-process. The progress protocol is independent of threading: its state lives in the executor future, which may move between pool threads.
- **Enforced justification.** timely makes unjustified output hard by API shape. instancy's closure API hides capabilities, so the shell enforces justification at runtime (V3) and offers `retain` for data kept across calls.
- **Stricter completion.** Exact zero over the worker's scope, instead of a positive count, turns a leaked or missing count into a visible failure: a hang, and then a residual error once every seal is applied.
- **Seals and abandon markers.** Each progress stream ends with a seal (finished) or ends unsealed (stopped). This gives a final accounting boundary that timely doesn't have.
- **Per-scope sections on the wire.** All of a worker's scopes travel in one commit, so a receiver applies the parent section before importing into the child.
- **Debug oracle.** Debug builds, and so every test, run an independent checker of the accounting (§11.6.5).
