# Progress Tracking

This document explains how instancy preserves timely-style progress semantics while supporting fused stages, shared worker pools, feedback loops, and distributed execution.

Back to the overview: [Design Overview](./README.md)

## 11. Progress Tracking Details

Progress tracking is the heart of instancy's execution model. It determines when timestamps are complete (no more data can arrive), when operators should be notified, when feedback loops should terminate, and when the entire dataflow has finished. Getting this right for multi-worker, multi-process, and feedback-loop scenarios is the most complex part of the system.

### 11.1 Core Concepts Recap

**Capabilities.** An operator holds a `Capability<T>` for each timestamp it may still produce output at. Creating, cloning, downgrading, and dropping capabilities are the *only* way an operator communicates its progress intentions to the system. Each capability change generates a `(operator_index, output_port, timestamp, diff)` update:
- `diff = +1`: capability acquired (operator can produce at this time)
- `diff = -1`: capability released (operator will no longer produce at this time)

**Pointstamps.** A pointstamp `(location, timestamp)` represents an outstanding capability at a specific point in the dataflow graph. The reachability `Tracker` maintains counts of all active pointstamps and computes their implications through path summaries.

**Path summaries.** The graph's structure defines summary functions from each output port to reachable downstream input ports. For a simple pipeline, the summary is identity (timestamp passes through unchanged). For a feedback loop, the summary includes a timestamp increment (e.g., `t → t + 1`). The tracker uses these to compute: "if a capability exists at output port A with time T, what is the earliest time data could arrive at input port B?"

**Frontiers.** An operator's input frontier is the set of minimal timestamps that could still arrive. When a time `t` is no longer in the frontier (no pointstamp can reach input at `≤ t`), the operator is notified that `t` is complete. The frontier is an `Antichain<T>` — the incomparable minimal elements under the partial order.

**Completion.** A dataflow is complete when `tracker.tracking_anything() == false` — there are no outstanding capabilities anywhere in the graph. For multi-worker dataflows, "anywhere" must mean across ALL workers, not just the local one.

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
- Messages are `Vec<ProgressChange<T>>` batches: `(operator_index, output_port, timestamp, diff)`
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
- Data in transit via exchange channels is accounted for (capabilities are held until data is pushed).
- Feedback data at iteration N+1 doesn't cause premature frontier advance at iteration N.

All of this works automatically through the capability protocol + progress exchange:
1. Before pushing data to exchange, the operator holds a capability at the output timestamp.
2. The capability is broadcast to all peers.
3. Peers see the capability and know data may still arrive at that timestamp.
4. Only when the operator releases the capability (after push completes) do peers see the frontier advance.
5. For in-process shared memory: data writes happen-before capability release, so data is always visible in the channel buffer when the peer processes the release.

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
┌──────────┬────────────────────────────────┐
│ Header   │ Payload (Vec<ProgressChange>)  │
│ (8 bytes)│ (Codec-serialized)             │
├──────────┼────────────────────────────────┤
│ msg_type │ [(op_idx, port, time, diff)]   │
│ length   │                                │
└──────────┴────────────────────────────────┘
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
if consecutive_idle >= MAX_IDLE_SWEEPS {
    if tracker.is_completed() && !tracker.has_pending_remote() {
        // All capabilities globally released, no pending remote progress.
        // Safe to force-close remaining operators (feedback cycle quiesced).
        force_close_all_operators();
        return Completed;
    }
}
```

The `has_pending_remote()` check is defense-in-depth: after 64+ idle sweeps (each draining remote progress), it should always be empty. But checking guards against the narrow race where a peer sends progress between the last `propagate()` and the force-close decision.

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


## Additional stage-aware progress notes

Per-stage execution keeps progress tracking local to each stage's worker group. Cross-stage frontier movement happens through the exchange boundary, and ghost operators preserve reachability information even for workers that do not materialize a stage locally.

- Workers in the same stage exchange capability updates with their stage peers.
- Cross-stage frontiers are aggregated at the exchange boundary rather than by a global all-worker protocol.
- Ghost operators keep graph shape and path summaries so downstream frontiers remain correct even when a worker does not materialize a stage's executable operators.
- Loop restrictions remain explicit: parallelism changes inside iterative scopes are intentionally constrained.
