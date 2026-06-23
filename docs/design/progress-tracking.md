# Progress Tracking

This document explains how instancy preserves timely-style progress semantics while supporting fused stages, shared worker pools, feedback loops, and distributed execution.

Back to the overview: [Design Overview](./README.md)

## 11. Progress Tracking Details

Progress tracking is the heart of instancy's execution model. It determines when timestamps are complete (no more data can arrive), when operators should be notified, when feedback loops should terminate, and when the entire dataflow has finished. Getting this right for multi-worker, multi-process, and feedback-loop scenarios is the most complex part of the system.

### 11.1 Core Concepts Recap

**Capabilities.** An operator holds a `Capability<T>` for each timestamp it may still produce output at. Creating, cloning, downgrading, and dropping capabilities are the *only* way an operator communicates its progress intentions to the system. Each capability change generates a `(operator_index, output_port, timestamp, diff)` update:
- `diff = +1`: capability acquired (operator can produce at this time)
- `diff = -1`: capability released (operator will no longer produce at this time)

**Pointstamps.** A pointstamp `(location, timestamp)` represents outstanding work at a specific point in the dataflow graph. There are two kinds: a **source** pointstamp on an output port (an operator capability — "X may still produce at t") and a **target** pointstamp on an input port (an in-flight message — "a batch for t has been handed to a channel but not yet consumed"; see §11.4.4). The reachability `Tracker` maintains counts of all active pointstamps of both kinds and computes their implications through path summaries.

**Path summaries.** The graph's structure defines summary functions from each output port to reachable downstream input ports. For a simple pipeline, the summary is identity (timestamp passes through unchanged). For a feedback loop, the summary includes a timestamp increment (e.g., `t → t + 1`). The tracker uses these to compute: "if a capability exists at output port A with time T, what is the earliest time data could arrive at input port B?"

**Frontiers.** An operator's input frontier is the set of minimal timestamps that could still arrive. When a time `t` is no longer in the frontier (no pointstamp can reach input at `≤ t`), the operator is notified that `t` is complete. The frontier is an `Antichain<T>` — the incomparable minimal elements under the partial order.

**Completion.** A dataflow is complete when `tracker.tracking_anything() == false` — there are no outstanding capabilities **and no unconsumed in-flight messages** anywhere in the graph. For multi-worker dataflows, "anywhere" must mean across ALL workers, not just the local one.

### 11.2 Single-Worker Progress Flow

For a single worker, progress tracking is straightforward:

```
┌──────────────┐     capability changes     ┌──────────────────┐
│  Operators   │ ─────────────────────────► │ ProgressTracker  │
│  (hold/drop  │   (op, port, time, diff)   │                  │
│  capabilities)│                            │  Reachability    │
│              │ ◄───────────────────────── │  Tracker         │
│              │     frontier updates        │                  │
└──────────────┘                            └──────────────────┘
```

Each executor sweep:
1. **Collect**: Drain capability changes from all operators' `ProgressReporter` buffers.
2. **Propagate**: Run the reachability algorithm — compute which pointstamps still have implications.
3. **Update frontiers**: For each operator, compute the new input/output frontiers from the tracker's implications.
4. **Check completion**: If `tracking_anything() == false`, the dataflow is complete.

### 11.3 Multi-Worker Progress Exchange

When multiple workers run the same dataflow graph (e.g., for exchange/partition parallelism), each worker has its own `ProgressTracker` with its own `Reachability::Tracker`. The challenge: worker A's tracker only sees worker A's capability changes. If worker A releases all capabilities while worker B still holds some, worker A would incorrectly report "completed" — potentially force-closing operators before worker B's exchanged data arrives.

**Solution: Cross-worker capability broadcasting.** Following timely-dataflow's design, every capability change is broadcast to all peer workers. Each worker's tracker then reflects the **global** state of all capabilities across all workers. Completion is independently verifiable by each worker — no global barrier is needed.

```
                    ┌─────────────────────────────────────────────┐
                    │              Logical Exchange                │
                    │   (progress channels between logical workers)│
                    └─────────────────────────────────────────────┘
                             ▲                       ▲
                             │                       │
                    broadcast │            broadcast  │
                    changes   │            changes    │
                             │                       │
Worker 0:                    │         Worker 1:     │
┌────────────────┐           │         ┌────────────────┐
│   Operators    │           │         │   Operators    │
│   ▼            │           │         │   ▼            │
│ ProgressTracker│──send────►│◄──send──│ ProgressTracker│
│   │            │           │         │   │            │
│   │  ◄─receive─┼───────────┘         │   │  ◄─receive─│
│   ▼            │                     │   ▼            │
│ Reachability   │                     │ Reachability   │
│ Tracker        │                     │ Tracker        │
│ (global view)  │                     │ (global view)  │
└────────────────┘                     └────────────────┘
```

Each worker's propagation cycle becomes:

1. **Collect** local capability changes from operators.
2. **Broadcast** local changes to all peer workers via progress channels.
3. **Receive** remote workers' changes from progress channels.
4. **Propagate** all changes (local + remote) through the reachability graph.
5. **Update** per-operator frontiers.
6. **Check** completion — now reflects global state.

#### 11.3.1 Progress Channel Architecture

For N workers, we create N × (N-1) unidirectional FIFO channels:
- Each worker gets (N-1) senders (one to each peer) and (N-1) receivers (one from each peer)
- Messages are `Vec<ProgressChange<T>>` batches. Each `ProgressChange` carries a `kind` — `Source` (a capability change on an output port) or `Target` (an in-flight message count on an input port, see §11.4.4) — plus `(node, port, timestamp, diff)`. Both kinds are broadcast over the same channels.
- FIFO ordering per sender ensures a release (`-1`) is never seen before the corresponding acquire (`+1`)
- Senders notify the target worker's `WakeHandle`, waking idle workers on progress arrival

#### 11.3.2 Initialization Ordering

A subtle correctness requirement: all workers must complete initialization (including broadcasting their initial capabilities) **before any worker starts executing**. Otherwise, a fast worker could see incomplete global state and make incorrect frontier/completion decisions.

instancy enforces this via deferred task registration:

```
Phase 4: Create wake handles and progress channels for all N workers
Phase 5: Materialize all workers (builds executor, attaches tracker,
         calls tracker.initialize() which broadcasts initial caps)
         ── NO worker is registered on the task pool yet ──
Phase 6: Register ALL workers on the task pool
         ── Now workers can be polled; all progress channels contain
            complete initial state from all peers ──
```

This two-phase approach guarantees every worker's progress channels contain the full set of initial capability broadcasts from all peers before any worker starts executing.

### 11.4 Progress Tracking in Feedback Loops

Feedback loops (iterative computations) are where progress tracking becomes most critical. A feedback edge creates a cycle in the dataflow graph with a timestamp summary that *advances* the timestamp (e.g., `t → t + 1` for a loop counter). This is what prevents the loop from running forever — the frontier advances with each iteration.

#### 11.4.1 How Feedback Loops Terminate

Consider an iterative computation with a loop:

```
Input ──► Operator A ──► Exchange ──► Operator B ──┐
                ▲                                   │
                └───── Feedback (t → t+1) ──────────┘
```

1. **Epoch 0**: Input injects data at time `(0, 0)`. Operator A holds capability at `(0, 0)`, processes data, sends results via exchange to B. B holds capability at `(0, 0)`, feeds back at time `(0, 1)`.

2. **Epoch 1**: A receives feedback data at `(0, 1)`. The path summary `t → t+1` means A's capability at `(0, 1)` can reach B's input at `(0, 2)`. A processes, B feeds back at `(0, 2)`.

3. **Convergence**: Eventually B decides not to feed back (convergence detected). B drops its capability. A sees its input frontier advance past the last iteration timestamp. A drops its capability. The tracker sees no more outstanding capabilities for this outer epoch — the loop has terminated.

4. **Cross-worker correctness**: In a multi-worker exchange loop, worker 0 cannot know if worker 1 still plans to send more feedback data unless it sees worker 1's capabilities. The progress exchange ensures each worker's tracker knows about ALL workers' capabilities — so a worker only reports the loop as complete when ALL workers have dropped their loop capabilities.

#### 11.4.2 Why Global Barriers Are Not Needed

A naive approach would use a global barrier: "wait until all workers agree the loop is done." This is expensive and serializes workers across iterations.

instancy (following timely-dataflow) avoids barriers entirely:
- Each capability change is broadcast immediately to all peers.
- Each worker's reachability tracker computes implications from ALL known capabilities.
- If worker 0 holds a capability at `(0, 5)` in the feedback loop, ALL workers' trackers see this and know the frontier at the loop input hasn't advanced past iteration 5.
- Only when ALL workers release their iteration-5 capabilities does each tracker independently conclude that the frontier has advanced.

This is a **decentralized consensus** achieved through broadcast — no coordination, no leader, no barrier.

#### 11.4.3 Exchange + Feedback Interaction

The most complex case combines exchange (cross-worker data movement) with feedback loops:

```
Worker 0:  Input ──► Op A ──► Exchange ──► Op B ──┐
Worker 1:  Input ──► Op A ──► Exchange ──► Op B ──┤
                      ▲                            │
                      └──── Feedback (t→t+1) ──────┘
```

Data from worker 0's Op A may be routed to worker 1's Op B (and vice versa). Feedback from worker 1's Op B arrives at worker 1's Op A. The progress tracking must ensure:

- Worker 0 doesn't conclude iteration N is complete until worker 1 has also finished iteration N.
- Data in transit via exchange channels is accounted for, even after the sending operator has released its capability.
- Feedback data at iteration N+1 doesn't cause premature frontier advance at iteration N.

The first and third points fall out of the capability protocol + progress exchange: a worker only sees the loop-input frontier advance past iteration N once **all** workers have released their iteration-N capabilities (§11.4.2). The second point — data handed to an exchange but not yet consumed — is **not** covered by capabilities alone, because the loop-body operators (enter/concat/map/filter/feedback) are pure pass-throughs that hold no per-timestamp capability across a push. It is covered by explicit in-flight message accounting on the exchange channel, described next.

#### 11.4.4 In-Flight Message Accounting (Exchange Channels)

Capabilities answer "could an operator still *produce* at time t?" They do not, on their own, answer "is there a *message* for t already handed to a channel but not yet consumed?" For pipeline channels inside a single operator that gap is harmless. For **exchange** channels — especially inside a feedback loop, where the loop-body operators hold no capability across a push — it is not: a batch sent to another worker but not yet pulled would be invisible to progress, and the tracker could declare the loop complete while data is still in flight. This was the cause of premature completion / data loss in iterate+exchange dataflows (issue #277).

instancy closes the gap with explicit message accounting on the exchange channel, mirroring timely-dataflow's per-channel message counters:

- `ExchangePush` records `+n` at the downstream input pointstamp `(target_op, target_port, time)` when it sends a batch of `n` records.
- `ExchangePull` records `-n` at the same pointstamp when it delivers that batch to the consumer.
- The tracker drains these as **target** (message) pointstamps via `update_target`, alongside the **source** (capability) pointstamps. While the net count at a pointstamp is positive, that input's frontier — and therefore global completion — cannot advance past the message's timestamp.

A few properties make this robust:

- **Tee-safe.** `Push`/`Pull::set_inflight_reporter` is a no-op by default; only `ExchangePush`/`ExchangePull` record. A `TeePush` propagates the reporter to its branches, so a tee'd output accounts only its exchange branch and never double-counts.
- **Loop-body timestamp projection.** Inside `iterate`, the loop body runs in `Product<T, TInner>` time while the merged parent tracker is `T`. The in-loop exchange's reporter is `ProgressReporter<Product<T, TInner>>`; the tracker drains it through a projection mapping each `(p, diff)` to `(p.outer, diff)`, so an unconsumed in-loop exchange message holds the **outer epoch** incomplete.
- **Producer/consumer symmetry.** The `+n` and `-n` must always be wired together. The consumer side is wired for every exchange; the producer side must be wired wherever the exchange is *sourced* — not only after a unary operator, but also when an exchange is fed directly by the loop's `concat` (`iter_var.exchange(...)`). A missing producer side records `-n` with no matching `+n`, driving the count negative and hanging the loop.
- **Cross-worker / cross-process.** Because `+n` and `-n` may be recorded on different workers or nodes, the counts ride the same progress broadcast (and network bridge) as capability changes; the `ProgressChange` `kind` distinguishes source from target pointstamps (§11.3.1, §11.5.1).

### 11.5 Logical Progress Exchange (Physical-Layer Independence)

A key architectural principle: **progress exchange is a purely logical concept**. The `ProgressTracker` exchanges capability changes between logical workers/executors without any knowledge of whether those workers are:
- On the same OS thread (in-process shared memory channels)
- On different threads in the same process (same mechanism)
- On different machines across a network (serialize + network transport via `SharedTransportSession`)

The `ProgressTracker` interacts with progress channels through a simple interface:

```rust
/// Send capability changes to a peer worker.
trait ProgressSend<T: Timestamp> {
    fn send(&self, changes: Vec<ProgressChange<T>>);
}

/// Receive capability changes from a peer worker.
trait ProgressReceive<T: Timestamp> {
    fn drain_all(&self) -> Vec<Vec<ProgressChange<T>>>;
    fn has_pending(&self) -> bool;
}
```

The physical layer provides the concrete implementation:

| Scenario | Physical Implementation |
|----------|----------------------|
| Same process | `Arc<Mutex<VecDeque>>` + `WakeHandle::notify()` |
| Cross-process | Serialize `ProgressChange` → wire protocol → TCP/QUIC → deserialize |
| Testing | In-memory channels with deterministic ordering |

This mirrors the logical/physical separation already established for data channels (§4.5): the `TransportProvider` resolves logical data targets to physical delivery, and the progress exchange resolves logical progress targets to physical progress delivery. The same pluggable architecture applies.

#### 11.5.1 Cross-Process Progress Exchange

When workers run on different machines, progress exchange uses the same shared transport infrastructure as data channels. The wire protocol is defined in `communication/progress_exchange.rs`:

```
┌──────────┬─────────────────────────────────────┐
│ Header   │ Payload (Vec<ProgressChange>)       │
│ (8 bytes)│ (Codec-serialized)                  │
├──────────┼─────────────────────────────────────┤
│ msg_type │ [(kind, node, port, time, diff)]    │
│ length   │  kind: 1 byte (0=source, 1=target)  │
└──────────┴─────────────────────────────────────┘
```

**Critical ordering guarantee for cross-process:** Data messages and progress messages share connections through the `SharedTransportSession`. The implementation ensures that data pushed to a channel is transmitted before the corresponding capability release by using a **single FIFO payload channel** per peer in the `TransportSession`. Both data and progress frames are sent through the same bounded `mpsc` channel, preserving the causal order: a worker sends data at time T before releasing its capability for T. The bridge task writes from this shared channel to TCP in FIFO order, with only control messages (handshake, ready barrier) receiving biased priority. This design also prevents cross-dataflow starvation — one dataflow's heavy data cannot block another dataflow's progress messages since they interleave naturally in the shared queue.

#### 11.5.2 Progress and the Adapter Layer

The progress exchange fits naturally into the three-layer architecture (§4.5):

```
┌─────────────────────────────────────────────────────────────────┐
│                   Logical Layer                                  │
│                                                                  │
│  ProgressTracker: broadcasts/receives capability changes         │
│  between logical worker IDs. No knowledge of physical topology.  │
└──────────────────────────┬──────────────────────────────────────┘
                           │  ProgressChannel trait
┌──────────────────────────▼──────────────────────────────────────┐
│                   Adapter Layer                                   │
│                                                                  │
│  ProgressProvider: resolves (source_worker, target_worker) to    │
│  concrete send/receive endpoints. Decides serialization needs.   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   Physical Layer                                  │
│                                                                  │
│  SharedMemoryProgress: Arc<Mutex<VecDeque>> (in-process)         │
│  NetworkProgress: SharedTransportSession + wire protocol (cross-node) │
│  InMemoryClusterProgress: simulated cross-node (testing)         │
└─────────────────────────────────────────────────────────────────┘
```

### 11.6 Force-Close and Quiescence

When the executor detects that an operator has been idle for many consecutive sweeps (quiescence), it checks whether the progress tracker reports completion. If `is_completed()` returns true AND no remote progress is pending, operators are force-closed:

```rust
if consecutive_idle >= max_idle_sweeps {
    if tracker.is_completed() && !tracker.has_pending_remote() {
        // All capabilities released AND all in-flight exchange messages
        // consumed (§11.4.4), no pending remote progress.
        // Safe to force-close remaining operators (feedback cycle quiesced).
        force_close_all_operators();
        return Completed;
    }
}
```

`max_idle_sweeps` is configurable via `DataflowBuilder::max_idle_sweeps` (default 64); it is a latency knob, not a correctness knob — `is_completed()` is the safety gate.

With in-flight message accounting (§11.4.4), `is_completed()` already reflects unconsumed exchange data, so force-close cannot tear down a loop while data is still in flight — the source of the #277 data loss is removed regardless of the idle threshold. The `has_pending_remote()` check remains defense-in-depth: after many idle sweeps (each draining remote progress) it should always be empty, but checking guards against the narrow race where a peer sends progress between the last `propagate()` and the force-close decision.

> **Known limitation (tracked separately).** The idle-sweep heuristic is a *backstop* for declaring quiescence, distinct from the progress-based safety gate above. At a very low `max_idle_sweeps` (≈1–3, well below the default 64) a worker can park in the narrow window between a peer pushing the loop's final record into an exchange channel and the consumer pulling it — the loop then hangs rather than completing. It does **not** lose data (the in-flight accounting correctly refuses completion). Resolving it means deriving quiescence from the progress tracker rather than an idle-sweep count; see the `*_eager_quiescence` tests in `multi_worker_iterate.rs` for the repro and analysis.

### 11.7 Async Probe

```rust
impl<T: Timestamp> ProbeHandle<T> {
    /// Returns true if the frontier is less than `time`.
    pub fn less_than(&self, time: &T) -> bool;

    /// Awaits until the frontier advances past `time`.
    pub async fn async_wait_for(&self, time: T) -> Result<(), Error>;

    /// Returns a watch receiver for frontier changes.
    pub fn frontier_watch(&self) -> watch::Receiver<Antichain<T>>;
}
```


### 11.8 Stage-Aware Progress Tracking

Per-stage execution keeps progress tracking local to each stage's
`StageExecutor`. Instead of one worker-wide tracker spanning the full
dataflow graph, instancy creates one `ProgressTracker` per `(stage,
worker)` pair. That tracker only contains the operators and internal
pipeline edges for its own stage.

This means **no global broadcast is needed within a stage**. A stage's
operators share pipeline channels inside one executor, so frontier changes
propagate through the local reachability graph exactly as they do today,
just over a much smaller graph.

#### Local stage tracking

Within a stage:

- Operators hold and drop capabilities locally.
- The stage-local `ProgressTracker` propagates pointstamp changes through
  only that stage's operators.
- Completion of a timestamp inside the stage is determined without any
  peer-wide broadcast.

Workers in the same stage do **not** exchange progress directly with one
another. Each worker tracks its own partition independently. The only place
where those worker-local frontiers must be combined is at an exchange
boundary leading into another stage.

#### Exchange boundaries and `FrontierAggregator`

At every cross-stage exchange boundary, the receiver maintains a
`FrontierAggregator`. Each upstream sender contributes its current frontier,
and the aggregator computes `min(all senders)` for that incoming edge.

Exchange channels therefore carry three kinds of messages inline:

- `DataBatch(timestamp, Vec<D>)`
- `FrontierUpdate(Antichain<T>)`
- `SenderDone`

`FrontierUpdate` tells the downstream stage that a sender's output frontier
has advanced. `SenderDone` is the terminal signal: that sender will never
produce more data on the channel.

Because the sender count is known statically when the exchange is wired,
the aggregator can keep one frontier per sender and recompute the aggregate
minimum whenever any sender advances or finishes.

#### Completion cascading across stage boundaries

Stage completion is no longer a single global tracker saying
`tracking_anything() == false` for the entire graph. Instead, completion
cascades downstream:

1. An upstream stage drains its operators and advances its output frontier
   to empty.
2. It emits final `FrontierUpdate` messages and then `SenderDone` on all
   outgoing exchange channels.
3. The downstream `FrontierAggregator` observes all senders done for that
   input, so the input frontier eventually becomes empty.
4. Once that downstream stage has no remaining input frontier and no local
   buffered work, it completes and repeats the process for its own outputs.

`DataflowCompletionBarrier` coordinates the node-local bookkeeping around
this cascade: each `StageExecutor` decrements the barrier when it reaches
local completion, while `SenderDone` and empty frontiers drive completion
through the stage graph itself.

#### Cross-stage feedback loops

Within-stage feedback works the same way as timely-style local progress
tracking: the stage-local reachability graph handles the cycle.

Cross-stage feedback loops are treated as exchange boundaries with the same
message protocol. The feedback edge carries data plus `FrontierUpdate`
messages back to the earlier stage. That stage combines:

- the frontier from the normal loop-entry input, and
- the aggregated frontier from the feedback channel.

As the loop body finishes an iteration and advances its feedback frontier,
that `FrontierUpdate` flows back across the boundary. When all feedback
senders eventually report `SenderDone`, the earlier stage knows no more
iterations can arrive, its input frontier can become empty, and completion
continues cascading forward.

### 11.9 Comparison: instancy vs timely-dataflow Progress Tracking

instancy preserves the core timely-dataflow model inside each stage:
capabilities, pointstamps, path summaries, antichains, and reachability are
still the fundamental tools for reasoning about logical time. What changes
is the **scope** of tracking and how frontier information crosses worker and
stage boundaries.

| Aspect | timely-dataflow | instancy |
|--------|----------------|----------|
| Tracker scope | One per worker, covers entire dataflow graph | One per stage×worker, covers only stage's operators |
| Progress broadcast | All-to-all broadcast of capability changes to all peer workers | No global broadcast — frontier updates flow inline with data through exchange channels |
| Ghost operators | Required for stages where a worker has no real operators | Eliminated — each StageExecutor only contains its stage's operators |
| Frontier propagation | Reachability algorithm across the full graph | Local reachability within stage + FrontierAggregator at exchange boundaries |
| Completion detection | Single tracker's `tracking_anything() == false` | Per-stage completion cascading via SenderDone, coordinated by DataflowCompletionBarrier |
| Feedback loops | Handled within the single global tracker | Within-stage: same as timely. Cross-stage: feedback as exchange channel with frontier updates |

#### What instancy preserves from timely

instancy keeps the same logical-time foundations:

- **Capabilities** still represent permission to produce data at a timestamp.
- **Pointstamps** still summarize outstanding work in the progress graph.
- **In-flight message counts** on exchange channels (§11.4.4) follow timely's
  per-channel counter model: a sent-but-unconsumed batch holds the downstream
  frontier back, independent of capabilities.
- **Path summaries** still describe timestamp transformation across edges.
- **Antichains** still represent frontiers compactly.
- **Reachability** still determines how progress changes propagate through
  operators.

For operators inside a single stage, the behavior is intentionally the same
as the classic timely model — frontier reasoning remains local, precise,
and incremental.

#### What instancy changes

The major change is that progress tracking becomes **stage-local** instead
of worker-global.

A timely worker tracks the full graph and learns peer progress via an
all-to-all broadcast of capability deltas. instancy instead gives each
`StageExecutor` a small local tracker and pushes cross-stage frontier
movement through ordinary exchange channels using `FrontierUpdate` and
`SenderDone` messages.

That change removes the need for ghost operators. If a worker does not
participate in a stage, there is simply no StageExecutor and no progress
subgraph for that worker-stage pair.

#### Why instancy's model is better for staged execution

For staged execution with varying per-stage parallelism, the instancy model
has several advantages:

- **Smaller tracker graphs**: each tracker only contains one stage's
  operators, which reduces bookkeeping and simplifies reasoning.
- **No ghost operators**: non-participating workers do not need placeholder
  nodes just to keep progress connected.
- **Better isolation**: unrelated stages do not exchange progress traffic or
  delay one another's frontier visibility.
- **Natural completion cascade**: `SenderDone` and empty frontiers propagate
  completion through the actual stage topology.

The result is a design that keeps timely's correctness machinery where it is
most valuable, while replacing the global broadcast protocol with a
stage-aware, exchange-driven progress flow better matched to instancy's
per-stage executor architecture.
