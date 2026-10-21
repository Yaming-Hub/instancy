# Cluster & Distributed Execution

This document collects the distributed-runtime design: cluster startup, distributed cancellation, membership changes, coordinator integration, multi-cluster isolation, and the testing model for true cross-process execution.

Back to the overview: [Design Overview](./README.md)

### 5.5 Cancellation

A `CancellationToken` (from `tokio-util`) is threaded through the entire dataflow:

```rust
// User code
let token = CancellationToken::new();
let config = Config { cancellation_token: token.clone(), .. };

// Later, to cancel:
token.cancel();

// Inside operators:
tokio::select! {
    msg = input.recv() => { /* process */ },
    _ = cancellation_token.cancelled() => { return Ok(()); },
}
```

When cancelled:
1. All operator tasks observe the token and exit gracefully.
2. Channel senders are dropped, causing downstream `recv()` to return `None`.
3. The progress tracker drains and shuts down.
4. `execute()` returns with partial results or a `Cancelled` error.

#### 5.5.1 Distributed Cancellation

In a cluster dataflow, cancellation must propagate across all peer nodes so that
the entire distributed computation winds down consistently. The control channel
(channel ID 0) carries a `Cancel` message alongside the existing `Handshake` and
`Ready` messages.

**Wire format** — `Cancel` is message type `2`:

```
[type=2][reason_len: u32 LE][reason: UTF-8 bytes][crc32: 4 bytes]
```

The `reason` field is a human-readable string (e.g., `"user requested shutdown"`,
`"operator panicked"`).

**Components** — Two pieces handle cancellation for each cluster dataflow:

| Component | Trigger | Action |
|-----------|---------|--------|
| **Lifecycle coordinator** (spawned in Phase 11) | Local `dataflow_cancel` token fires while local workers are still running | Sends `Cancel` to every peer's control channel (bounded by `handshake_timeout`), then tears down once the local workers have stopped |
| **Control dispatcher** (spawned in Phase 8.5) | Receives `Cancel` on any peer control channel | Fires local `dataflow_cancel` with `CancellationReason::PeerCancelled { peer_id, detail }` |

Both run on the tokio runtime passed to `spawn_cluster`. The dispatcher keeps
running until teardown (it also receives the shutdown handshake's
acknowledgements, §5.5.3) and handles any message already queued before it
exits. A local cancellation that happens *after* this node's workers completed
successfully is not broadcast: the node's part of the dataflow is done, and
the shutdown handshake still runs to completion.

**First-cancel-wins semantics** — `CancellationToken::cancel_with_reason()` is
idempotent: once the token is cancelled, subsequent calls are no-ops. This
prevents infinite echo loops:

```
Node A cancels → broadcasts Cancel to Node B
Node B receives → cancels local token → broadcasts Cancel to Node A
Node A receives → cancel_with_reason() is a no-op (already cancelled) → no broadcast
```

Each node broadcasts at most once per dataflow.

**Peer-down integration** — When the hosting application reports a node departure
via `report_node_leave()`, the `ClusterCancelHandle` cancels the shared
`dataflow_cancel` token (not just individual worker tokens). This ensures the
coordinator broadcasts `Cancel` to healthy peers before bridge teardown.

**Security assumption** — `Cancel` messages carry no cryptographic signature.
Peer identity verification is the responsibility of the transport layer (e.g.,
mTLS configured in the application's `ConnectionManager`). All peers in a
cluster session are assumed to be authenticated at connection time.

#### 5.5.2 Cluster Startup Protocol

`spawn_cluster()` orchestrates a multi-phase startup sequence that synchronizes
all participating nodes before any dataflow execution begins. If any phase
fails or times out, `spawn_cluster()` returns an error — no operators are
started.

**Phases:**

```
 Node A                                    Node B
   │                                         │
   ├─ Phase 1: Build local dataflows         ├─ Phase 1: Build local dataflows
   ├─ Phase 2: Validate topologies           ├─ Phase 2: Validate topologies
   ├─ Phase 3: Compute fingerprint           ├─ Phase 3: Compute fingerprint
   │                                         │
   ├─ Phase 4: HANDSHAKE ──────────────────► │
   │ ◄──────────────────── HANDSHAKE ────────┤
   │  (verify fingerprints match)            │  (verify fingerprints match)
   │                                         │
   ├─ Phase 5: Wire exchange channels        ├─ Phase 5: Wire exchange channels
   ├─ Phase 6: Create progress channels      ├─ Phase 6: Create progress channels
   │                                         │
   ├─ Phase 6.5: READY BARRIER ────────────► │
   │ ◄──────────────────── READY ────────────┤
   │  (all channels wired, safe to execute)  │  (all channels wired, safe to execute)
   │                                         │
   ├─ Phase 7: Materialize workers           ├─ Phase 7: Materialize workers
   ├─ Phase 8: Register & start execution    ├─ Phase 8: Register & start execution
   │                                         │
   ▼  Dataflow running                       ▼  Dataflow running
```

1. **Handshake** (Phase 4) — Each node sends every peer its cluster protocol
   version (`CLUSTER_PROTOCOL_VERSION`), its own node id, the dataflow id, and a
   graph fingerprint. The fingerprint is a stable hash (`instancy::hash::StableHasher`)
   of everything nodes must agree on: the timestamp type; every topology node id,
   in order, with its worker count; every operator (kind — the builder method
   that created it — name, port counts, explicit exchange parallelism); every
   edge and feedback edge in order (endpoints and channel kind); every network
   exchange edge in order (routing kind, name, timestamp type — the loop's
   `Product` type inside `iterate` — and record type); and every named
   input/output port with its record type. Each node checks, per connection:
   - the protocol version (`HandshakeError::ProtocolVersionMismatch`; an
     unversioned 0.1.x handshake is reported as version 1),
   - that the peer identifies as the node the connection was assigned to
     (`HandshakeError::PeerIdentityMismatch`; catches miswired connections),
   - the dataflow id (`HandshakeError::DataflowIdMismatch`),
   - the fingerprint (`HandshakeError::FingerprintMismatch`).

   Exchange edges are paired across nodes by position and progress updates
   name operators by index, so any of these differences would otherwise
   misroute data (e.g. branches declared in a different order, `u64` records
   read as `i64`, or a different per-node worker split). Record and timestamp
   types are compared by `std::any::type_name`, so nodes must be built against
   the same versions of the record types.

   Before the handshake, `spawn_cluster` also validates the topology itself
   (`ClusterTopology::validate`: no duplicate node ids, no zero-worker nodes)
   and checks that every data and progress channel id of the cluster fits its
   namespace (see the channel-id layout in `communication.md`).

2. **Ready Barrier** (Phase 6.5) — After all exchange channels and progress
   channels are wired, each node sends `Ready` (carrying its node id, which is
   checked against the connection again) and waits for all peers to
   respond. This prevents a fast node from starting execution before a slow
   node has finished channel wiring, which would cause lost messages.

Both phases use the same `handshake_timeout` parameter. Each waits at most
`handshake_timeout` for peer responses; if any peer doesn't respond in time,
`spawn_cluster` returns `Err(HandshakeError::Timeout { peer_id })`.

**What happens when one node doesn't call `spawn_cluster`:**

If Node A calls `spawn_cluster` but Node B never does, Node A will block in
the Handshake phase waiting for B's response. After `handshake_timeout`
expires, `spawn_cluster` returns an error. No dataflow is materialized, no
operators are started, and no resources are leaked.

```rust
// Node A: will fail after 5 seconds if Node B never calls spawn_cluster
let result = rt.spawn_cluster(
    "my-dataflow",
    topology,         // includes Node B
    "node-a",
    dataflow_id,
    transport,
    Duration::from_secs(5),  // handshake_timeout
    build_fn,
    &tokio_handle,
    SpawnOptions::new(),
);
// result = Err(HandshakeError::Timeout { peer_id: "node-b" })
```

**Control channel** — Handshake, Ready and Cancel messages share a dedicated
control channel (channel ID 0) that is separate from data and progress
channels. Control messages receive biased priority in the transport layer's
shared FIFO queue. (The clean-shutdown acknowledgement deliberately does
*not* use it; see §5.5.3.) Wire formats (each followed by a CRC32 trailer):

```
Handshake: [type=0][protocol_version: u32 LE][fingerprint: u64 LE][dataflow_id: 16 bytes]
           [node_id_len: u32 LE][node_id: UTF-8 bytes]
Ready:     [type=1][node_id_len: u32 LE][node_id: UTF-8 bytes]
```

#### 5.5.3 Cluster Shutdown Protocol (clean teardown)

A node whose local workers finish first must not tear down while peers still
need its frames, and a clean close must be distinguishable on the wire from a
connection loss. Each cluster dataflow therefore runs a **lifecycle
coordinator** task per node (spawned at the end of `spawn_cluster`). It owns
the transport session (or the shared-manager registration), the network
bridges and the peer-registry entry, so their lifetime no longer depends on
the user's join handle.

**Ordered per-peer lanes.** Data frames go straight to the transport's
bounded per-peer payload queue (`try_send`, with backpressure). Everything
that must never be lost — progress deltas, data-channel close sentinels and
end-of-stream markers — is queued on an unbounded, ordered per-peer *outbox*
whose forwarder awaits capacity on that same payload queue. The forwarder is
never cut short by cancellation; it exits once every producer has released
its outbox. Because both paths feed one FIFO payload lane per peer (dedicated:
the peer's payload channel; shared: the dataflow's sequenced lane and reorder
buffer), a marker queued after a frame is delivered after it.

**Wire messages.**

| Message | Channel | Format | Meaning |
|---|---|---|---|
| Close sentinel | each data channel (payload lane) | `[0xFF]` | No more data on this channel |
| Progress end-of-stream | each progress channel (payload lane) | `[count = 0xFFFF_FFFF u32 LE][crc32]` | No more progress on this channel |
| End-of-stream ack | one progress channel (payload lane) | `[count = 0xFFFF_FFFE u32 LE][crc32]` | "I received everything you sent, including your end-of-stream markers" |

Real progress batches are capped below both reserved counts, so the markers
are unambiguous. The acknowledgement travels on the sender's payload lane,
not the priority control channel: the receiver of an ack has therefore
already received (demultiplexed into its inbound queues) every data frame the
acknowledging node sent before it. A data push that races with retirement
re-checks the link after enqueueing and fails instead (see step 3), so no
data frame can follow the ack unnoticed.

**Protocol.**

1. When every local worker has finished with `Ok(true)` (each worker's
   executor — and with it every network push and progress sender — is
   dropped before its completion is reported), the coordinator queues one
   end-of-stream marker on every progress channel to every peer, behind
   everything else on the lane. These markers are the node's *goodbye*.
2. A node that has received the end-of-stream marker on every progress channel
   from a peer replies with the acknowledgement — immediately, even if its
   own workers are still running.
3. A link is **retired** once either the peer has acknowledged our goodbye
   (it holds everything we sent) or we have received the peer's goodbye (its
   workers are done, so it needs nothing from us). Transport failures on a
   retired link (EOF, write errors, reorder timeouts) are benign. Progress
   and control signals still addressed to a retired peer are discarded, but
   *records* are not: a node cannot push data after its own goodbye, so a
   record for a retired peer means that peer finished while data was still
   on its way to it. The push fails with `CommunicationError::DataLoss`,
   failing the dataflow, instead of dropping the record.
4. The node tears down — stops its bridges, unregisters from the shared
   managers and the peer registry, releases the transport — once every link is
   retired, or once a link that is not retired has ended for good (all its
   channels closed, a corrupt frame or control message, a transport error, our
   goodbye could not be handed to the transport, or the peer's `Cancel`).
   On a graceful teardown the node first flushes what it queued (bounded by
   `handshake_timeout`): it waits for its outbox forwarders to hand their
   lanes to the transport and, in Shared mode, for a flush barrier on each
   manager's payload queue (`SharedPeerManager::flush_dataflow`) before
   unregistering, because the shared bridge drops queued frames of
   unregistered dataflows. The lanes can still hold frames peers need: our
   goodbye and our acknowledgements of their goodbyes (a peer whose goodbye
   retired our link may still be waiting for our ack), or our final progress
   for a peer that cancelled and is still draining (`drain_on_cancel`). Once every local worker has finished, teardown also lets the
   inbound data bridges deliver or report whatever is already queued before
   they exit (bounded by `handshake_timeout`). A flush barrier only proves
   that the frames reached a connection's writer queue, so when the dataflow
   holds the last reference to its shared managers (per-job `Mode::Shared`),
   teardown then closes them (`SharedPeerManager::close`, bounded by
   `handshake_timeout`), which waits until their writers have written what is
   queued, instead of letting the drop discard it.

**Closing connections.** Releasing a connection never resets it. Closing a
TCP socket while inbound data is still unread makes the OS reset the
connection, and the peer's OS may then discard bytes it has received but not
yet delivered: our last frames (goodbye, acknowledgements, `Cancel`). A
released connection therefore closes in two halves: its writers write what is
queued and then shut their half down explicitly (dropping a write half sends
the end of stream only for some streams, such as `OwnedWriteHalf`; split
halves of TLS streams or pipes send nothing), and its readers keep reading
(and discarding) until the peer closes its half, for at most 10 s. Since both
sides shut down their write halves, both readers see the end of stream and
the connection is gone as soon as both nodes have released it. In Dedicated
mode the session's demultiplexers keep reading after the session is dropped
(`Demuxer::run_lingering`); in Shared mode a released manager — closed or
dropped — closes its connections this way in the background.

**Inbound data loss.** In a correct run an operator cannot finish while
records addressed to it are in flight. Records that nevertheless reach a node
after the operator they are addressed to has finished — left unconsumed when
its input was dropped, arriving afterwards, or arriving after the channel's
close sentinel — are reported by the inbound bridge (or the dropped input) as
data loss. While the local workers run, that cancels the dataflow on every
node; after they have succeeded, the node broadcasts `Cancel`, stops waiting
for acknowledgements and its `ClusterCompletion` returns
`CommunicationError::DataLoss` instead of `Ok`. Losses observed while the
dataflow is being cancelled (cancelled operators drop their inputs) count
only if every local worker still succeeds (`drain_on_cancel`); losses
explained by a local worker failure are not reported. A failing worker drops
its inputs before its result reaches the coordinator, so a loss reported
before any local worker has succeeded is held until the next worker result:
a failure keeps the worker's own error, a success makes the loss fatal.
Together with step 3 this means a premature completion (audit
C1/C2) can no longer return `Ok` with missing records on every node: some
node fails with a data-loss error.

No node waits for another node's *completion*, only for the receipt of its own
goodbye (one round trip), so sequential joins across nodes cannot deadlock.

```text
 Node A (done first)                       Node B (still running)
   │ workers Ok                              │
   │── progress EOS × (A workers × B workers) ──►│ all EOS received
   │                                         │   → retire link A, send ack
   │◄──── ack (in-band, behind B's data) ────│
   │ every link retired → teardown           │
   │ (join resolves)                         │ ... workers finish later
   │                                         │── EOS to A (discarded) ──►
   │                                         │ link A already retired → teardown
```

**Completion handles.** `ClusterCompletion` resolves once the local workers
have finished *and* the coordinator has finished teardown; its result is the
local workers' result, or `CommunicationError::DataLoss` if records addressed
to this node were lost (see above). Dropping an unresolved
`ClusterCompletion` detaches it: the dataflow keeps running and the handshake
still happens. Dropping a `ClusterSpawnedDataflow` without calling `join()`
cancels the dataflow, and the coordinator broadcasts the cancellation to every
peer. Blocking waits (`join_blocking`, `ClusterCompletion::wait`) need the
transport's tokio runtime to make progress. On a worker thread of a
multi-thread runtime (inside a task; the transport's runtime or the
application's) the wait runs in
`tokio::task::block_in_place`, which moves the thread's other work elsewhere
first, so it works even with a single worker thread or with every worker
blocked in a wait, and does not strand a task the caller just spawned in the
worker's LIFO slot. tokio offers no stable way to recognise a worker, and
`block_in_place` panics on a thread that drives a `current_thread` runtime
(also one that has entered a multi-thread runtime's handle), so a thread
counts as a worker if it drives a scheduler (`tokio::task::yield_now` defers
its wake-up there, and wakes at once on a `block_on` thread, in a `LocalSet`
it drives, on a `spawn_blocking` thread or outside any runtime) and carries
the name of a multi-thread runtime's threads: tokio's default name, or a
name seen on the transport runtime or on the runtime that called
`spawn_cluster` (learned during `spawn_cluster` and by the coordinator). A
worker of a runtime that names every thread differently may go
unrecognised and block without handing its work over; a `current_thread`
runtime driven by a thread carrying one of these names, with a multi-thread
handle entered, would make `block_in_place` panic. When the caller is inside a
`current_thread` runtime
context, or the transport runs on a `current_thread` runtime, the wait first
spawns a no-op probe on the transport runtime; if the probe does not run
within 2 s (typically because the caller is the thread driving that runtime,
or nothing drives it), the wait returns `RuntimeError::InvalidConfig`
promptly instead of deadlocking (a thread that drives the transport's
`current_thread` runtime but pauses for longer than that also gets the
error). The dataflow keeps running, as if the completion had been dropped.
Other callers (outside any runtime, in a multi-thread runtime's `block_on`
or a `LocalSet` it drives, a `current_thread` runtime's thread that entered
a multi-thread handle, or a `spawn_blocking` thread) block normally once the
probe, if any, has passed.

**Failure paths.** If a local worker fails (or the dataflow is cancelled
before local completion), the coordinator broadcasts `Cancel` (bounded by
`handshake_timeout`), waits for the local workers to stop, and tears down
without sending a goodbye. The `Cancel` carries the local cancellation
reason, and a peer treats a forwarded failure as a failure (it never drains
into a partial result); a node whose bridges are stopped from outside while
its workers run reports a network error. `Cancel` therefore acts as a terminal goodbye in
both directions: a node that receives a peer's `Cancel` (in any phase) stops
waiting for that peer, and a node that broadcast `Cancel` but whose workers
still succeed (the cancellation raced with completion) queues its goodbye and
tears down without waiting for acknowledgements. Otherwise two nodes that
both finish successfully after a crossing cancellation could each wait for a
goodbye or ack from a peer that had already torn down; in Shared mode nothing
else could resolve that wait.
A link that is not retired and fails while the local workers still run (EOF,
a corrupt frame or control message, a send or encode failure, a poisoned
lock, a shared-transport error) is fatal: the coordinator cancels the local
dataflow (`CancellationReason::NetworkError`, or `InternalError` for a
poisoned lock), broadcasts `Cancel` to every reachable peer and tears down
once the local workers have stopped. An EOF, or a send failure (usually the
peer's side of the connection is gone), counts only once every channel
from the peer has closed; the control channel closes last, after any
`Cancel` queued on it has been applied, so a cancelling peer is reported as
`PeerCancelled` rather than as a network failure. Such a soft failure is
escalated anyway if the link has not fully closed within `handshake_timeout`:
a local write error (an oversized frame, say) leaves our reader and the
peer's side open, so nothing else would follow. In Dedicated mode the failed
writer is kept (not dropped) until the session is dropped, so a local write
error never half-closes the connection and the peer never mistakes it for
this node leaving. The same
grace period applies while draining. A failed send dropped frames the peer
needs, so it stays undecided, and the node does not tear down, until the
link has fully closed or the grace period has expired; by then any `Cancel`
the peer queued has been applied. A peer that cancelled, or that said
goodbye and then closed the link (it left after finishing, which is what
made the send fail), does not need the frames. A peer that said goodbye
while the link stayed open (only our writer failed), or a failure that our
own workers outlived, leaves the result incomplete and is reported as data
loss. It never tears down silently: that
would leave the workers running without bridges and peers that already
finished waiting for an acknowledgement that never comes. An operator whose
push finds the queue to a peer that has not finished closed does not fail
with `ChannelClosed` either, which would hide why the link closed: the push
reports the link failure to the coordinator (once) and backs off as for a
full queue until the coordinator cancels the dataflow with the actual reason
(the peer's `Cancel`, the link ending, or the grace period). The single hook
for this is `LifecycleMachine::on_link_failure` (`Action::LinkFailed`); PR8
extends failure *detection* from there.
If the dataflow is already being cancelled, such failures are expected and the
coordinator simply waits for the local workers to stop.
A local cancellation or runtime shutdown that happens after the goodbye was
committed is not broadcast and does not cut the handshake short. The same
holds for an external abort of the network bridges (`report_node_leave`,
runtime shutdown): after local success the node still tears down gracefully,
so the bounded flush delivers its queued goodbye and acknowledgements to the
reachable peers; while its workers still run, it broadcasts `Cancel` first.

**Residual limits.** The last message of any handshake cannot itself be
confirmed. If a node's transport is *aborted* (its tokio runtime dropped) at
the instant its peer also finishes, that peer may miss both the goodbye and
the ack. It then detects the link loss (dedicated: EOF; shared: after
reconnect attempts are exhausted) and still reports its own local result.
Frames still waiting in a shared manager's bridge queue (not yet on a
connection's writer queue) are lost when an application drops the last
reference to that manager outside a dataflow's teardown. A peer that is
partitioned but whose connection never fails delays the handshake until the
connection fails. If a `Cancel` broadcast times out (its control queue stayed
full for `handshake_timeout`) and the local workers then still succeed, a
Shared-mode peer that never received the `Cancel` may miss the goodbye too,
because the goodbye is dropped when the dataflow unregisters.

### 12.5 Dynamic Cluster Scaling

> **Status: Implemented.** The `ClusterMembership` trait and `ChannelMembership` convenience type are available in the `execute` module. Membership is attached to `ClusterTopology` and passed via `RuntimeConfig` at construction time. The runtime auto-starts a background listener that processes membership events to update the live topology, cancel affected dataflows, and make new nodes available. Mid-dataflow repartitioning (worker rebalancing) is not supported — existing dataflows keep their original topology.

instancy supports **dynamic cluster scaling** — nodes can be added to or removed from the cluster at runtime. The hosting application is responsible for detecting node changes (health checks, service discovery, autoscaler events, connection failures) and notifying the timely runtime. The library does **not** perform its own node discovery or health monitoring.

#### Responsibilities

| Responsibility | Owner |
|---|---|
| Detect node joins, departures, and failures | **Application** (hosting process) |
| Notify the runtime of topology changes | **Application** → `ClusterMembership` provider attached to `ClusterTopology` |
| Update live topology and cancel affected dataflows | **Library** (runtime) |
| Re-establish connections to new nodes | **Application** (via `ConnectionManager`) |
| Decide whether to retry/abort affected dataflows | **Application** (via error policy) |

#### ClusterMembership Trait

The application implements this trait and attaches it to `ClusterTopology` via `with_membership()`. When the topology is passed through `RuntimeConfig`, the runtime automatically takes the membership provider and starts a background event listener.

```rust
/// Events describing changes to the physical cluster topology.
/// The hosting application produces these events; the runtime consumes them.
pub enum MembershipEvent {
    /// A new physical node has joined the cluster and is ready to host logical workers.
    NodeJoined {
        node_id: String,
        logical_workers: usize,
    },
    /// A physical node has left the cluster (graceful shutdown or detected failure).
    NodeLeft {
        node_id: String,
        reason: NodeDepartureReason,
    },
}

/// Why a node departed.
pub enum NodeDepartureReason {
    /// Graceful shutdown (node drained its work before leaving).
    Graceful,
    /// Connection lost / health check failed (unexpected departure).
    ConnectionLost,
    /// Application-initiated removal (e.g., scale-down decision).
    Removed,
}

/// Application-implemented trait for providing cluster membership changes.
///
/// The application attaches a membership provider to `ClusterTopology` via
/// `with_membership()`. The runtime takes ownership of the provider during
/// construction and calls `events()` to receive membership change events.
pub trait ClusterMembership: Send + Sync + 'static {
    /// Takes the membership event receiver.
    /// Returns `Some(receiver)` on the first call; subsequent calls return `None`
    /// (the runtime takes ownership of the receiver).
    fn events(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<MembershipEvent>>;
}
```

#### Topology–Membership Integration

`ClusterTopology` owns an optional membership provider. This design keeps topology and membership tightly coupled since both describe the physical cluster layout:

```rust
let membership = ChannelMembership::new();
let tx = membership.sender();

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-0", 4),
    NodeConfig::new("node-1", 4),
])?.with_membership(membership);

let rt = RuntimeHandle::new(RuntimeConfig {
    topology: Some(topology),
    ..Default::default()
})?;

// Runtime auto-starts membership listener — send events via tx
tx.send(MembershipEvent::NodeJoined { node_id: "node-2".into(), logical_workers: 4 })?;
```

#### Scaling-Up (Node Joins)

When the runtime receives a `MembershipEvent::NodeJoined` event:

1. **Clear "down" state**: If the node was previously marked as down, the peer registry clears it. This allows future `spawn_cluster` calls to include the node.
2. **Connection on demand**: New connections to the node are established lazily — the connection pool calls `ConnectionManager` the first time a dataflow needs to communicate with the node.
3. **Topology update**: The live `ClusterTopology` is updated with the new node. Already-running dataflows are **not** affected.
4. **Worker assignment**: New logical worker indices are allocated for the joining node's workers in subsequent `spawn_cluster` calls.

**Important**: Existing in-flight data is NOT migrated. Only new dataflows (or re-spawned dataflows) take advantage of the expanded topology. This ensures progress tracking remains consistent — a timestamp that has already been produced cannot change its routing.

#### Scaling-Down (Node Departures)

When the runtime receives a `MembershipEvent::NodeLeft` event:

1. **Cancel affected dataflows**: All dataflows with workers on the departed node are cancelled via the peer registry. instancy does **not** attempt to reschedule work to surviving nodes — the hosting application owns retry logic.
2. **Topology update**: The node is removed from the live `ClusterTopology`. If it was the last node, the topology is cleared entirely.
3. **Application retry**: The hosting application can resubmit the dataflow targeting only healthy nodes, or send a `NodeJoined` event once the node recovers.

#### Consistency Guarantees

- **Progress safety**: A departed node's outstanding capabilities are treated as "released" — the frontier advances past any timestamps that only the lost node could produce. This is safe because no more data at those timestamps will arrive.
- **At-most-once by default**: If a node fails mid-computation, records being processed by that node may be lost. Applications requiring exactly-once semantics must use the checkpoint/recovery mechanism.
- **No split-brain**: The application is the single source of truth for cluster membership. The runtime trusts the membership events and does not perform its own consensus or health probing.
- **Zero-worker events ignored**: `NodeJoined` events with `logical_workers == 0` are silently dropped.
- **Idempotent**: Duplicate join events for an existing node or leave events for an unknown node are handled gracefully.

#### Example: Kubernetes Integration

```rust
struct K8sClusterMembership {
    rx: Mutex<Option<UnboundedReceiver<MembershipEvent>>>,
    tx: UnboundedSender<MembershipEvent>,
}

impl K8sClusterMembership {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self { rx: Mutex::new(Some(rx)), tx }
    }

    /// Spawn a background task that watches Kubernetes pod events
    /// and converts them into MembershipEvents via self.tx.
    fn start_watching(&self, client: kube::Client) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            // Watch pod events and send MembershipEvent::NodeJoined/NodeLeft
        });
    }
}

impl ClusterMembership for K8sClusterMembership {
    fn events(&self) -> Option<UnboundedReceiver<MembershipEvent>> {
        self.rx.lock().unwrap().take()
    }
}
```

## 12.6 Coordinator Integration Model

## 12.6 Coordinator Integration Model

### Execution Model: SPMD with Host-Managed Coordination

instancy uses the **SPMD (Single Program, Multiple Data)** execution model: the hosting application launches the same dataflow program on each participating node, and each node's local executor runs the local partition of the graph. instancy does NOT dispatch work from a central coordinator to worker nodes.

However, real-world services always have a **coordinator** — a component in the hosting application that receives user requests, starts distributed dataflow execution, monitors progress, and returns results to callers. **The coordinator is implemented by the hosting application, not by instancy.** instancy provides primitives (traits, structs, helpers) to make this coordinator integration straightforward, but the coordination logic — how to start dataflows on remote nodes, how to route user requests, how to aggregate results — is entirely the host application's responsibility. This is by design: different applications have fundamentally different coordination needs (actor frameworks, gRPC services, message queues, custom RPC).

### DataflowHandle — The Driver Abstraction

When a node submits a dataflow for execution, it receives a `DataflowHandle` — a lightweight handle that represents the running dataflow and provides:

```rust
/// Handle returned when a dataflow is submitted to the local executor.
/// One instance per dataflow per node.
/// Cardinality: 1 per (dataflow, node). Lifetime: dataflow execution.
pub struct DataflowHandle {
    /// Unique identifier for this dataflow across all nodes.
    dataflow_id: DataflowId,
    /// Receiver for the final outcome from the local executor.
    outcome_rx: oneshot::Receiver<DataflowOutcome>,
    /// Cancellation token — calling cancel() requests graceful shutdown.
    cancel: CancellationToken,
    /// Progress subscription — streams frontier updates to the holder.
    progress_rx: mpsc::Receiver<ProgressUpdate>,
}

impl DataflowHandle {
    /// Wait for the dataflow to complete, returning the final outcome.
    pub async fn result(self) -> DataflowOutcome { ... }

    /// Request graceful cancellation. Operators finish their current batch
    /// then drain. Returns immediately; use `result()` to wait for completion.
    pub fn cancel(&self) { self.cancel.cancel(); }

    /// Subscribe to progress updates (frontier advances).
    /// The coordinator uses this to track how far the dataflow has progressed.
    pub fn progress_stream(&mut self) -> &mut mpsc::Receiver<ProgressUpdate> {
        &mut self.progress_rx
    }

    /// Query current frontier — the set of timestamps that may still arrive.
    /// If the frontier is empty, the dataflow is complete for all timestamps.
    pub fn current_frontier(&self) -> Antichain<T> { ... }
}
```

### DataflowOutcome — Rich Completion Status

```rust
/// The final result of a dataflow execution on a single node.
/// The coordinator aggregates outcomes from all nodes.
pub enum DataflowOutcome {
    /// All operators completed successfully. All timestamps processed.
    Completed {
        /// The final frontier (empty = all timestamps done).
        final_frontier: Antichain<T>,
        /// Execution metrics (total CPU time, operator stats).
        metrics: DataflowMetrics,
    },

    /// Dataflow was cancelled by the coordinator or user.
    Cancelled {
        /// Frontier at the time of cancellation — timestamps up to (but not
        /// including) this frontier have been fully processed and committed
        /// to sinks. Timestamps at or beyond this frontier may be partial.
        progress_frontier: Antichain<T>,
        /// Metrics up to the point of cancellation.
        metrics: DataflowMetrics,
    },

    /// A fatal error occurred in one or more operators.
    Failed {
        /// The error that caused the failure.
        error: Error,
        /// Frontier at the time of failure — same semantics as Cancelled.
        progress_frontier: Antichain<T>,
        /// Which operator(s) failed.
        failed_operators: Vec<OperatorInfo>,
        /// Metrics up to the point of failure.
        metrics: DataflowMetrics,
    },

    /// Quiescent — no operator can make progress but not all are done.
    /// This happens when the dataflow is waiting for external input
    /// (e.g., a live stream that hasn't produced new data).
    Quiescent {
        /// Current frontier — timestamps beyond this may still arrive.
        progress_frontier: Antichain<T>,
        metrics: DataflowMetrics,
    },
}
```

### ProgressUpdate — Real-Time Progress Reporting

The coordinator needs to know how far the dataflow has progressed, both for user-facing progress reporting and for determining which timestamps are "safe" (fully committed to sinks).

```rust
/// A progress update emitted by the executor when the frontier advances.
/// Cardinality: streamed periodically to the DataflowHandle holder.
pub struct ProgressUpdate {
    /// The dataflow this update is for.
    pub dataflow_id: DataflowId,
    /// The new frontier after this advance.
    pub frontier: Antichain<T>,
    /// How many records have been processed since last update.
    pub records_processed: u64,
    /// Wall-clock time of this update.
    pub timestamp: Instant,
}
```

### Cross-Node Outcome Aggregation

In a distributed dataflow, the coordinator (in the host application) must aggregate outcomes from all nodes. instancy provides a helper for this:

```rust
/// Aggregates DataflowOutcomes from multiple nodes into a single result.
/// The coordinator collects outcomes from all nodes and feeds them here.
/// Cardinality: 1 per dataflow on the coordinator node. Lifetime: dataflow execution.
pub struct OutcomeAggregator {
    expected_nodes: Vec<String>,
    received: HashMap<String, DataflowOutcome>,
}

impl OutcomeAggregator {
    pub fn new(participating_nodes: Vec<String>) -> Self { ... }

    /// Record a node's outcome. Returns the aggregated result when all nodes
    /// have reported, or None if still waiting.
    pub fn record(&mut self, node_id: &str, outcome: DataflowOutcome)
        -> Option<AggregatedOutcome> { ... }
}

/// The aggregated outcome across all nodes.
pub enum AggregatedOutcome {
    /// All nodes completed successfully.
    Completed { metrics: AggregatedMetrics },
    /// One or more nodes failed. The entire dataflow is considered failed.
    /// Includes which nodes failed and which succeeded.
    Failed {
        error: Error,
        failed_nodes: Vec<(String, Error)>,
        /// The global progress frontier (min across all nodes' frontiers).
        global_progress: Antichain<T>,
    },
    /// Cancelled on all nodes.
    Cancelled { global_progress: Antichain<T> },
}
```

### Coordinator ↔ instancy Interaction Pattern

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Coordinator (in host application)                      │
│                                                                         │
│  1. Receives user request                                               │
│  2. Builds dataflow definition                                          │
│  3. Sends "start dataflow" command to all participating nodes           │
│  4. Collects DataflowHandle on the local node                          │
│  5. Monitors progress via progress_stream()                            │
│  6. On cancellation request: calls handle.cancel() + sends cancel       │
│     command to remote nodes                                             │
│  7. Collects outcomes from all nodes via OutcomeAggregator             │
│  8. Returns final result to the user                                   │
└─────────┬───────────────────────────────┬─────────────────────┬─────────┘
          │ Start + Monitor               │ Start + Monitor     │ Start + Monitor
          ▼                               ▼                     ▼
┌─────────────────┐            ┌─────────────────┐   ┌─────────────────┐
│   Node A        │            │   Node B        │   │   Node C        │
│                 │◄──────────►│                 │◄──►│                 │
│ DataflowHandle  │  exchange  │ DataflowHandle  │   │ DataflowHandle  │
│ Executor sweep  │  channels  │ Executor sweep  │   │ Executor sweep  │
└─────────────────┘            └─────────────────┘   └─────────────────┘
```

### Key Design Points

1. **The coordinator is the host application's responsibility, not instancy's** — instancy is a dataflow execution library, not a distributed job scheduler. The host application implements all coordination logic: receiving user requests, launching dataflows on remote nodes, collecting results, and returning them to callers. This separation keeps instancy focused and avoids imposing a specific coordination pattern on diverse applications.

2. **Worker placement is decided by the coordinator (host app)** — a dataflow does not necessarily run on all cluster nodes. The coordinator selects which nodes participate and how many logical workers each node contributes. It builds a per-dataflow `ClusterTopology` containing only the selected nodes and their worker assignments, then passes this topology to each node when starting the dataflow. instancy does not make placement decisions — it executes the assignment it receives. instancy may provide a `PlacementStrategy` trait and validation helpers (e.g., verify total workers match stage parallelism requirements), but actual placement logic depends on factors only the host app knows: node load, data locality, cost constraints, hardware capabilities, etc.

3. **Data locality is application knowledge, not instancy's** — expanding on #2 above: instancy is a data-locality-agnostic execution engine. It provides mechanisms — per-worker named input ports, configurable cluster topology, exchange operators — but never assumes where data resides or how partitions map to workers. The hosting application has full knowledge of data placement and configures the dataflow accordingly. For example, if a dataset has 8 partitions across 2 machines (4 per machine), the host app starts 2 nodes with 4 logical workers each, and feeds each worker its local partition by binding a separate `TimestampedInput` stream per worker (e.g., `.input("data_0", partition_0_stream)` through `.input("data_3", partition_3_stream)` on each node). First-stage operators process data locally (no exchange needed), reducing data volume before cross-node `exchange()` in later stages. This data-local-first pattern minimizes network traffic and maximizes throughput, but it is entirely the host app's decision — instancy simply executes the worker-to-partition mapping it receives. Worker IDs are global and assigned by the topology (via node ordering in `ClusterTopology`); the host should compute each node's worker-id range from the topology rather than assuming `0..N` are local.

    ```
    Host app decides placement based on data locality:

    Machine A (data partitions 0-3):       Machine B (data partitions 4-7):
    ┌────────────────────────────┐         ┌────────────────────────────┐
    │ Partition 0 → Worker 0     │         │ Partition 4 → Worker 4     │
    │ Partition 1 → Worker 1     │         │ Partition 5 → Worker 5     │
    │ Partition 2 → Worker 2     │         │ Partition 6 → Worker 6     │
    │ Partition 3 → Worker 3     │         │ Partition 7 → Worker 7     │
    │                            │         │                            │
    │ First stage: local         │         │ First stage: local         │
    │ processing (no shuffle)    │         │ processing (no shuffle)    │
    │                            │         │                            │
    │ Later stages:              │◄───────►│ Later stages:              │
    │ exchange() across nodes    │         │ exchange() across nodes    │
    │ (all-to-all by hash key)   │         │ (all-to-all by hash key)   │
    └────────────────────────────┘         └────────────────────────────┘

    instancy provides: per-worker input ports, exchange(), ConnectionPool
    Host app provides: partition-to-worker mapping, connection factory, node topology
    ```

4. **instancy provides the building blocks** — `DataflowHandle`, `ProgressUpdate`, `OutcomeAggregator`, and `CancellationToken` provide everything the coordinator needs to manage distributed execution.

5. **Progress frontier is the source of truth for "how far we got"** — when a dataflow is interrupted (cancelled or failed), the `progress_frontier` tells the coordinator which timestamps have been fully processed. This enables:
   - Resume from last committed timestamp
   - Report partial progress to the user
   - Exactly-once semantics when combined with checkpointing

6. **Cancellation is cooperative and distributed** — the coordinator cancels locally via `DataflowHandle::cancel()`, then sends a cancel command to remote nodes via the host app's communication layer. Each node's executor checks its local `CancellationToken` and drains gracefully.

7. **Error reporting flows back to the coordinator** — when an operator fails, the local executor captures the error in `DataflowOutcome::Failed` with the progress frontier. The host app collects these outcomes and the `OutcomeAggregator` determines the global result.

8. **The coordinator node is designated by the host app, not by instancy** — any node can be the coordinator. Typically it's the node that received the user request. instancy doesn't need to know which node is the coordinator.

---

## 12.7 Multi-Cluster Isolation (No Global State)

**Constraint:** The instancy crate must contain **zero static/global variables**. All state is owned by explicit runtime instances.

### Motivation

A hosting application may need to run multiple **isolated instancy clusters** within the same process. For example:

- **Interactive cluster** — low-latency queries with small worker pool, high priority
- **Batch cluster** — bulk ETL with large worker pool, best-effort priority
- **Test cluster** — in-process integration tests that must not interfere with production clusters

Each cluster must have fully independent:
- Task queue
- Worker thread pool
- Connection pool
- Progress tracking state
- Cancellation scope

### Design

```rust
/// A self-contained instancy runtime. Multiple RuntimeHandle instances
/// can coexist in the same process with full isolation.
pub struct RuntimeHandle {
    worker_pool: WorkerPool,
    task_queue: TaskQueue,
    connection_pool: ConnectionPool,
    config: RuntimeConfig,
}

impl RuntimeHandle {
    /// Create a new isolated runtime with the given configuration.
    pub fn new(config: RuntimeConfig) -> Self { ... }

    /// Submit a dataflow for execution within this runtime.
    pub fn execute(&self, spec: DataflowSpec) -> DataflowHandle { ... }
}
```

### Rules

1. **No `static`, `lazy_static`, `once_cell`, or thread-local state** in library code.
2. All shared state flows from a `RuntimeHandle` root — never from global singletons.
3. Metrics/tracing use the caller's subscriber (passed in config), not a global one.
4. Tests create their own `RuntimeHandle` instances — no test-ordering dependencies.

### Interaction Diagram

```
Process
├── RuntimeHandle "interactive"
│   ├── WorkerPool (4 threads)
│   ├── TaskQueue (priority-aware)
│   ├── ConnectionPool (to nodes A, B)
│   └── Dataflows: [query_1, query_2]
│
├── RuntimeHandle "batch"
│   ├── WorkerPool (16 threads)
│   ├── TaskQueue (FIFO)
│   ├── ConnectionPool (to nodes A, B, C, D)
│   └── Dataflows: [etl_pipeline]
│
└── RuntimeHandle "test"
    ├── WorkerPool (2 threads)
    ├── TaskQueue (FIFO)
    ├── InMemoryTransport (no real connections)
    └── Dataflows: [test_dataflow]
```


## Cross-process integration testing model

The former `CROSS_PROCESS_INTEGRATION.md` design has been merged here and updated to match the current required-connection-factory model.

### Problem and goals

instancy's in-process cluster tests validate protocol behavior, but they do not by themselves prove correctness across independent OS processes with separate address spaces, Tokio runtimes, and kernel-managed TCP state. Cross-process integration tests therefore need to:

1. Start multiple node processes, each hosting its own `RuntimeHandle`.
2. Coordinate cluster startup from a test-side coordinator.
3. Feed data, collect outputs, and assert correctness across nodes.
4. Clean up reliably on success, failure, or timeout.

### Architecture

- **Coordinator process**: starts child node processes, sends control commands, gathers progress/results, and tears everything down.
- **Node process**: hosts a long-lived runtime plus a control-plane agent that can bind listeners, establish peer connections through the current connection-factory path, spawn a cluster dataflow, feed inputs, collect outputs, and cancel/shutdown.
- **Two independent communication layers**:
  - **Control plane**: coordinator ↔ node process, for orchestration messages.
  - **Data plane**: node ↔ node, for instancy exchange/progress/control traffic carried by the runtime transport.

### Control protocol

The coordinator protocol remains length-prefixed JSON with request correlation:

- `BindListener` / `ListenerReady`
- `ConnectPeers` / `PeersConnected`
- `SpawnDataflow` / `DataflowSpawned`
- `FeedData`, `CloseInputs`, `CollectOutput`, `CancelDataflow`, and `Shutdown`
- unsolicited `DataflowCompleted` events

### Coordinator and node responsibilities

- The coordinator decides which nodes participate and in what topology.
- Each node owns its local `DataflowHandle`, input senders, and output receivers.
- Cluster spawn is still synchronized by the runtime handshake and ready barrier; the coordinator's orchestration ensures every node reaches `spawn_cluster()` with the same topology and `DataflowId`.
- Connection establishment in tests must use the same current application-facing connection factory path rather than bypassing the pool with ad-hoc pre-established sockets.

### Recommended test scenarios

- basic two-node pass-through
- cross-process exchange routing
- distributed word count over multiple timestamps
- iterative/feedback computation across processes
- three-node topologies
- multiple workers per node
- distributed cancellation
- parallel dataflows sharing the same runtime
- node crash / peer disconnect failure paths

### Notes carried forward from the original design

- the coordinator should own child-process lifecycle and timeout handling
- process cleanup must happen in `Drop`/finally paths
- predefined dataflow builders are useful because they isolate transport correctness from arbitrary user pipelines
- coordinator-side assertions should use both final output and completion/progress events
