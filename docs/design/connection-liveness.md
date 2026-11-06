# Connection liveness in the Shared transport

Status: implemented in `instancy/src/communication/shared_transport.rs`. Decision log R54–R59 and O17. Models: [`tla/ClusterTransport.tla`](./tla/ClusterTransport.tla) (extended) and [`tla/ConnSilence.tla`](./tla/ConnSilence.tla); see the [model README](./tla/README.md#connection-liveness-clustertransporttla-and-connsilencetla-o17). Cross-node backpressure (M7) is a separate, later design. It must keep CL6 below.

## 1. The problem

A pooled connection can stop delivering without ever failing:
- a network partition;
- a frozen host;
- a firewall that drops the connection's state;
- a half-open TCP connection after a crash that sent no reset.

Nothing in instancy notices. The dataflows using that connection wait forever. They are never reported OK, but they hang.

The application's membership layer watches nodes, over its own heartbeat. It doesn't see one dead data connection, and a connection's death must not become a node leaving. The transport contract (shared_transport.rs, `ConnectionFactory`) leaves liveness to the application's streams. But `TcpConnectionFactory` sets no keepalive, and timeouts can't be assumed for every stream. So instancy needs its own rule, scoped to one connection.

There is a second problem. Once one node notices, the other can still get stuck:
1. Its lanes keep writing after a clean EOF, by design (a released manager still reads its peers' last frames).
2. If the direction it writes into is the dead one, its writer blocks inside `write_frame`, which has no timeout.
3. One bridge task per peer waits on every connection's writer queue (shared_transport.rs `send_payload`), so a stuck writer stalls every dataflow to that peer, handshakes and Cancels included. That lasts until TCP gives up (about 15 minutes of retransmissions on Linux), or forever if the peer's kernel keeps answering.

**Scope:** Shared mode (`SharedPeerManager`), any stream a `ConnectionFactory` returns (TCP, TLS or other).

**Out of scope:**
- Dedicated mode. It has the same gap and can reuse the rule later.
- M7.
- Node membership. `report_node_leave` and `report_node_join` stay the authority on nodes.

## 2. Requirements

| # | Requirement |
|---|---|
| CL1 | A connection that stops delivering, in one direction or both, is failed on both nodes within a known bound. |
| CL2 | A healthy connection is never failed: not when busy, not when idle, not when a consumer is slow. |
| CL3 | It fails exactly the dataflows whose lanes use that connection, in either direction. No other connection fails, and no node leaves. |
| CL4 | A dead or stuck connection never holds up dataflows on other connections. |
| CL5 | instancy is never OK with lost, crossed or corrupted data. PR9's guarantees still hold. |
| CL6 | A connection's reader never waits on a consumer. Today this holds only in practice: `on_frame` awaits a bounded per-channel queue, which an inbound bridge drains into an unbounded queue at once. Nothing guarantees it, for example for frames that arrive before the bridge runs. The implementation makes that hand-off unbounded, so CL6 holds by construction; memory is already unbounded one step later, until M7. M7 must keep CL6. |

## 3. The rule

**Keepalive.** Every `probe_interval` (P, default 1 s), each node's probe loop queues a probe on each pooled connection. The probe loop exists already. Probes stay best effort: a probe is skipped when the writer queue is full, because then frames are already queued to flow.

**Silence.** Each connection's reader records the last time it read any bytes. A thin wrapper around the stream's read half updates the time on every read that returns data. The rule counts bytes, not frames: a frame can be up to 256 MiB, and on a slow link it can take longer than the timeout to arrive.

**Verdict.** On each tick, the probe loop checks every connection whose reader is still open. A connection from which no bytes have been read for `silence_timeout` (T) is dead.

**Timing condition.** T > P + D + 2S, where:
- D is the longest a healthy connection takes to deliver a byte;
- S is the longest a node's network tasks can be paused (a busy runtime, a stop-the-world event).

The 2S is because a pause on the writing node and a pause on the reading node can add up. Under this condition and CL6, a connection is declared dead only once its inbound direction has stopped delivering, and then within T + P + S. `ConnSilence.tla` checks both claims. It also shows the condition is tight: with T = P + D + 2S, a healthy connection is declared dead.

D and S describe the deployment, and instancy can't enforce them. T must be sized for them; the 3P check below only rejects values that can never work.

**Configuration:** `SharedConnectionConfig::silence_timeout: Option<Duration>`.
- The default is `Some(30 s)`. With P = 1 s, that leaves D + 2S up to 29 s.
- `None` disables the rule, for an application that brings its own keepalive. The documentation must say this brings back the hang.
- Validation rejects T < 3 × P.

## 4. The reaction

A node that declares a connection dead treats it as a read error, with the reason "no bytes for T":

1. **It fails the dataflows that use it.** Those are the dataflows whose lanes are pinned to it and those whose frames arrive on it, through the existing `fail_inbound` and `fail_outbound` paths.
2. **It removes the connection from the pool.** The next registration refills the pool, as after any connection failure.
3. **It stops the reader and aborts the writer at once,** dropping whatever is still queued. A dead connection gets no linger.
4. **It reports the failure** through the path every connection failure uses today (`report_health`). It never reports a node leave.

   That path emits `RuntimeEvent::TransportDegraded`, whose documentation says the runtime can't recover and should be restarted. That is already wrong today for any single pooled-connection failure, since the pool recovers from one. The implementation must fix it, with a recoverable connection-failure event or corrected documentation. Otherwise an application following the advice would restart and fail every dataflow, breaking CL3.

**What the peer sees.** The bounds apply once a dataflow has received its first frame on the connection. Until then it isn't bound to its inbound connection (`inbound_conn`), and silence ends it at `handshake_timeout`, as today.

| Case | Who notices, and how | Bound |
|---|---|---|
| a → b dead, b → a alive | b by its timer. b's close reaches a as an EOF or a reset. On a clean EOF, PR9's rules fail a's dataflows on that connection that were still running and not retired: a clean EOF is never taken as completion. | b: T + P + S. a: plus D. |
| Both directions dead | Each node by its own timer | T + P + S on each |
| Peer process frozen, its kernel still answering | The live node by its timer. The frozen node sees the close when it resumes. | T + P + S |
| Peer gone (crash, power loss) | The live node by its timer. Membership also reports the node. | T + P + S |

**The peer's writer.** In the first case, a's lanes pinned to the connection may keep writing after the clean EOF, into the dead direction, until a write fails. a's writer can block there. Two changes keep that harmless:

- **The writer stall rule.** A writer that has had bytes waiting and written none for T fails its connection, exactly as a write error does today: its lanes fail, the whole connection closes, and the dataflows whose frames arrive on it are told. It applies at any time, in the pool or out of it.
  - **The precedent:** this is TCP_USER_TIMEOUT's rule (RFC 5482, Linux tcp(7): data unacknowledged, or unsent because of a zero window, for longer than the limit closes the connection). gRPC sets it equal to its keepalive timeout precisely to stop writes staying stuck while the network is down (gRFC A18).
  - **Why in the application rather than the kernel:** instancy applies it itself, so it works for every stream a `ConnectionFactory` returns, TLS and pipes included, not only for TCP sockets on Linux. A thin wrapper on the write half records progress, as on the read half. It counts bytes, so a frame that was taken off the queue but is still being written counts too.
  - **Why progress, not each write:** Netty's `WriteTimeoutHandler` and Erlang's `gen_tcp` `send_timeout` (RabbitMQ sets 15 s) time each write instead. That would fail a healthy connection slowly sending one large frame (up to 256 MiB).
  - **Why it doesn't kill a healthy writer:** under CL6 the peer always reads, so the socket keeps accepting bytes, and the same timing argument as the read rule applies. This is argued, not model-checked: `ConnSilence.tla` models the read side.
  - **Without the rule,** a kept manager's connection stays open on a stuck writer until TCP gives up (`live_ct_silence_noexit`). A release keeps its existing 10 s cap.
- **Per-connection queues (section 5),** so nothing else waits on that writer.

**No new wire frame.** A GoAway frame announcing the death, as HTTP/2 GOAWAY and QUIC CONNECTION_CLOSE do, isn't needed for correctness:
- the peer already fails the affected dataflows on the EOF;
- the stall rule bounds its writer.

GoAway would only let the peer notice sooner, with a precise reason. It remains an option for later, as a protocol change.

## 5. Isolation: no shared bridge

**Today.** Each `SharedPeerManager` (one per peer node) has one bridge task. It takes every dataflow's payload and control frames, numbers each payload frame within its lane, and waits for room in the writer queue of the lane's connection. While it waits on one connection, it moves nothing for the others.

**Why the bridge exists.** Before PR9, a lane was spread over every connection, so something had to choose a connection for each frame. Since PR9 pins each lane to one connection at registration, the bridge only numbers frames and forwards them. It is a shared choke point left over from that design.

**The change: nothing shared between connections.** Each connection gets its own outbound queues, as in Flink (a Netty handler per channel), HTTP/2 implementations (a task per connection), Spark, timely and Hazelcast.

1. **Number at enqueue.** A payload frame gets its lane sequence number when it is enqueued, under the lane's lock, atomically with the enqueue. Async senders such as the outbox forwarder reserve a queue slot first, so numbering never waits.
2. **Straight to the connection.** Frames go into the pinned connection's writer queue. It is bounded, so a full queue gives the pushing operator `Backpressure`, as the manager's payload queue does today. Only lanes pinned to a stuck connection feel it.
3. **Control first.** Each writer has two queues and always serves the control queue first, keeping today's priority for control frames, as HTTP/2 puts control frames ahead of DATA. A control frame for a dataflow not registered here goes to any live connection, as today.
4. **Admitted frames are written.** A frame is numbered into its own registration's lane when it is enqueued. So once it is in a writer queue, it is written even if the registration ends meanwhile; a peer whose own run has finished drops it as a late frame, as today. PR9 dropped a registration's queued frames when it ended only because the bridge numbered frames later and could have numbered them into another registration's lane, which can't happen any more. Frames not yet admitted (still in an outbox) when the registration ends are dropped, as today.
5. **Per-connection flush, with no fixed deadline.** Teardown's flush waits until the run's outbox has been handed to its connection's writer queue, or until that connection ends (it fails, or the peer closes it). It needs no barrier through the writer, since admitted frames are written anyway (item 4). The liveness rule bounds the wait: a writer can only stay stuck if its connection has stopped delivering in some direction. Then a node's verdict closes the connection, and the other node sees it end.

   Today the flush gives up after `handshake_timeout` and unregisters, and the bridge then drops the run's remaining frames. With kept managers, a peer waiting for that run's goodbye or ack waits forever. This happens whenever the shared bridge stalls for longer than the flush's deadline, behind a silent connection or just a very slow one (`ct_silence_sharedbridge_stall`). The multi-process harness reproduces it on real processes (`finished_dataflow_survives_a_one_way_stall_longer_than_handshake` in `instancy-integration/tests/shared_connection_faults.rs`, scenario `one_way_stall_schedule`; it passes with this design): a one-way stall that stops reading a→b on one connection blocks a's bridge, and a dataflow on the other connection ends `Ok` on a at the flush deadline while its peer never finishes. With this design, that dataflow's frames go straight to its own, healthy connection, so it ends promptly on both nodes.

The public API doesn't change; the work is inside `shared_transport.rs`.

**Ordering obligations.** A lane has several producers: worker threads pushing records under the admission lock, and the async outbox forwarder carrying progress, close sentinels, goodbyes and acks. Numbering at enqueue must keep today's per-lane FIFO:
- a number is taken only by a successful enqueue (a full queue consumes none);
- numbering and enqueueing are one step under the lane's lock, so queue order is number order;
- a registration that has ended takes no numbers;
- a flush completes only once every frame its run admitted is in the writer queue;
- a record is enqueued before the progress that counts it, by program order, as today.

The model numbers each lane atomically from one ordered outbox, so it doesn't check these. A Loom or Shuttle test of the lane lock should.

**Why the stall rule alone isn't enough.** With the shared bridge, a writer stuck on one connection holds up every dataflow to that peer until the stall rule fails its connection, about T after the writer stopped making progress. Two things go wrong in that window:
- **A failure on a healthy connection.** A new dataflow's handshake (its timeout is chosen by the caller, often a few seconds) or a drain's grace (R50, 500 ms) can fail.
- **A hang on a healthy connection.** A dataflow that finishes during the stall times out its flush, and its last frames are dropped. The model finds this hang (`ct_silence_sharedbridge_stall`).

Removing the bridge removes the stall.

## 6. How it fits with the rest

- **Membership.** It stays authoritative for nodes. A connection's death fails only that connection's dataflows, and never implies `report_node_leave`. The two detectors judge different things, so they don't conflict. That answers the objection that a second failure detector could disagree with membership (communication.md, "instancy runs no heartbeat").
- **M7.** Backpressure must keep CL6: credit is checked before a record enters a lane, so a reader never waits for a consumer. A reader that could block makes this rule kill healthy connections (`cs_blocking`).
- **PR9.** Fail-stop is unchanged: frames lost with a connection are never resent, and a failed connection fails exactly the dataflows that used it. This design adds one more way for a connection to fail, and bounds every close.
- **Prior art.** No dataflow engine we looked at (Flink, Hazelcast Jet, timely) detects a silent data connection; they rely on node heartbeats or on the OS. Section 7 maps each part of this design to the mature system it follows.

## 7. Proven practice

Every rule here follows a mature, widely deployed system. None is invented for instancy. What is instancy's own is the mapping onto its lanes, and that reuses PR9's fail-stop paths unchanged.

| Part of the design | Proven precedent | Here |
|---|---|---|
| Probe each connection; it is dead when nothing has been read for T | **Erlang distribution's `net_ticktime`**: per connection, a node is ticked when nothing else was sent, and it is down when nothing has arrived for 4 tick intervals (60 s by default). **Netty's** `ReadTimeoutHandler`/`IdleStateHandler`: close on reader idle, ping on writer idle. **QUIC's idle timeout** (RFC 9000 §10.1): any packet received resets it. **gRPC keepalive** (gRFC A8): timed from the last byte read. | Same rule. Probes run at a fixed 1 s, through the existing RTT probe loop, rather than only when idle; that costs about 17 bytes a second per connection. |
| Count bytes, not frames | A Netty idle handler placed before the frame decoder; gRPC A8's "last byte read" | Same |
| On timeout, close with no goodbye frame | QUIC: "the connection is silently closed and its state is discarded"; Netty closes the channel | Same |
| A writer with bytes waiting and no progress for T fails the connection | **TCP_USER_TIMEOUT** (RFC 5482), which gRPC sets equal to its keepalive timeout so that writes can't stay stuck (gRFC A18) | Same rule, applied in instancy so that it works for any stream. Not timed per write, as Netty's `WriteTimeoutHandler` and Erlang's `send_timeout` are, because one large frame on a slow link can take longer than T. |
| Fail the connection's users, not the node | **Elasticsearch** removes a node only through fault detection (leader and follower checks), not because one transport connection broke. **ZooKeeper** drops and reconnects a silent connection without expiring the session. **gRPC** fails the RPCs on a dead transport, and the channel reconnects. | Same: membership stays authoritative for nodes |
| The reader never waits on a consumer | HTTP/2 (RFC 9113 §5.2.2): an endpoint "MUST read and process HTTP/2 frames ... as soon as data is available" | Same (CL6) |
| One outbound queue and writer per connection, control first | Netty (a pipeline per channel); HTTP/2 implementations (the `h2` crate writes PING and SETTINGS acks before DATA); Flink (credit per channel, events first) | Same |
| Frames already queued are written; a graceful flush has no fixed deadline | Closing a socket without SO_LINGER: the kernel keeps sending queued data in the background until it is acknowledged or the connection times out (socket(7)) | Same, with the liveness rule as the timeout |
| Timeout default | Erlang 60 s; gRPC 20 s for a ping ack; Flink heartbeat 50 s; Spark 120 s | 30 s, configurable |

## 8. The models

**`ClusterTransport.tla`, extended without clocks.** New switches, all off in every earlier scenario:

| Switch | Meaning |
|---|---|
| `"silence"` in `Faults` | One direction of a connection stops delivering anything. A writer into it buffers `StuckCap` frames, then blocks. |
| `Detect` | `"none"` is today. `"silence"` lets a node declare a connection dead exactly when its inbound direction is silent. |
| `StallAbort` | `TRUE`: a writer stuck on a silent direction fails its connection like a write error, at any time (the untimed form of "nothing written for T"). |
| `SharedBridge`, `WQCap` | Today's single bridge per node, with bounded writer queues. Without it, each lane is forwarded on its own, which is the proposal. |

**What it doesn't model:**
- The inbound binding. `Arrive` reads the peer's pin, so the model doesn't show that a dataflow which hasn't yet received a frame ends by `handshake_timeout` rather than by the verdict.
- The flush's wait for a connection failure. Its untimed form: the flush gives up only when its own connection's writer is stuck, which matches the design's flush with no fixed deadline (section 5).
- Races between a lane's producers (section 5).

**`ConnSilence.tla`, a new timed model of one direction of one connection.** It covers:
- probes every P, skipped when the writer queue is full;
- application frames up to `F` bytes;
- delivery within D;
- a pause of up to S on each node;
- the byte timer and the verdict;
- the variants: a frame-counting timer, and a reader that blocks.

It justifies the untimed model's "dead exactly when the inbound is silent".

**Scenarios and results** are in the [model README](./tla/README.md#connection-liveness-clustertransporttla-and-connsilencetla-o17). In short:
- the rule passes, with only the silent connection's dataflows failing, on both nodes, and everything ending;
- today's design hangs;
- dropping the writer stall rule leaves a connection that never closes;
- the shared bridge hangs a dataflow on a healthy connection;
- a too-short timeout, a frame-counting timer and a blocking reader each kill a healthy connection.

## 9. Implementation outline (after approval)

1. **Detection.** The reader and writer wrappers, the verdict in the probe loop, the config field with validation, and an error variant for a silent connection.
2. **CL6 by construction.** The reader hands each frame to an unbounded per-channel queue and never awaits a consumer.
3. **The reaction.** On a dead verdict, stop the reader and abort the writer. Apply the writer stall rule to every connection. Fix `TransportDegraded`: a recoverable connection-failure event, or corrected documentation.
4. **Remove the bridge.** Number at enqueue, two-queue writers, admitted frames always written, and a flush with no fixed deadline, with the ordering obligations of section 5 under a Loom or Shuttle test.
5. **Documentation:**
   - the transport contract (shared_transport.rs module and `ConnectionFactory` docs);
   - communication.md ("instancy runs no heartbeat");
   - the distributed guide's liveness section.
6. **Tests:**
   - paused tokio time over duplex streams;
   - silence in one direction and in both;
   - a busy connection sending large frames, which must not be failed;
   - a stuck writer, which must not delay another connection's dataflow;
   - a dataflow that finishes during another connection's stall, whose goodbye and ack must still reach its peer;
   - a slow consumer, which must not stop the reader.

   The multi-process harness's silent-partition scenarios exercise the whole rule.
