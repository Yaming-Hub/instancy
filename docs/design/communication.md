# Communication Layer

This document covers the physical transport layer behind instancy's logical channels: in-process queues, application-provided connection establishment, pooled/shared connections, framing, and dataflow isolation across shared resources.

Back to the overview: [Design Overview](./README.md)

## 6. Communication Layer

The communication layer implements the physical delivery mechanisms behind the [TransportProvider trait (§4.5)](./README.md#45-logicalphysical-separation-architecture). At the logical layer, operators only see `Push` and `Pull` endpoints. The communication layer provides the concrete implementations.

### 6.1 Intra-Process Channels

For operators within the same process (where `TransportProvider::is_local()` returns true), data is exchanged via **bounded in-memory buffers**. No serialization — data moves as owned Rust values. Since operators run on the Custom Worker Thread Pool (not Tokio), exchange channels use a **lock-free SPSC (Single-Producer, Single-Consumer) ring buffer** with atomic head/tail indices, power-of-two masking, and cached indices for minimal contention. Pipeline-local channels use `Mutex<VecDeque>` bounded queues.

```rust
/// Lock-free SPSC ring buffer for exchange channels.
/// Power-of-two capacity with bitwise-AND masking.
/// Producer and consumer each cache the other's index to
/// minimize atomic loads (refresh only on apparent full/empty).
pub struct RingBuffer<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: AtomicUsize,  // consumer reads here
    tail: AtomicUsize,  // producer writes here
    mask: usize,        // capacity - 1 (power-of-two masking)
}
```

When an upstream operator produces output, it writes directly into the downstream operator's input buffer. If the buffer is full, the upstream operator's task yields (returns to the scheduler with a "blocked" status), and will be re-dispatched when space becomes available. This provides natural backpressure without async machinery.

`Envelope` (defined in [§5.8](./observability.md#58-message-envelope)) carries data batches, control signals, and user-defined metadata through the same buffer.

#### 6.1.1 Force-Network Mode (Testing Transport Fidelity)

By default, intra-process channels bypass serialization for performance. However, the hosting application can configure instancy to use **TCP loopback connections** (or any network transport) even for operators colocated in the same process. This is configured per-`RuntimeHandle` via:

```rust
let config = RuntimeConfig::builder()
    .local_transport(LocalTransportMode::Network)  // force TCP even locally
    .build();
```

**`LocalTransportMode`** variants:
- **`InMemory`** (default): Bounded in-memory buffers, zero-copy, no serialization.
- **`Network`**: Route local channels through the same `ConnectionManager` + codec path used for inter-process communication. Messages are serialized and deserialized exactly as they would be over the wire.

**Use cases:**
- **Unit/integration testing**: Verify that all message types serialize and deserialize correctly without needing a multi-process deployment.
- **Fuzz testing**: Catch codec edge cases (e.g., large payloads, special characters, boundary timestamps) in a single-process test harness.
- **Deterministic replay**: Record and replay wire-format messages for debugging.

The transport mode is transparent to operator logic — operators always see typed `InputHandle`/`OutputHandle`. The mode only affects the physical channel implementation chosen during graph materialization.

### 6.2 Inter-Process Connections: ConnectionManager

Connection establishment is **fully delegated to the application**. The library does not know how to open TCP ports, listen for connections, or negotiate TLS — it only knows that it needs a bidirectional byte stream to a given peer. The application provides a `ConnectionManager` component that handles the entire connection lifecycle.

This design supports arbitrarily complex networking setups:
- The application might use an actor framework that sends a command to a remote node saying "open a TCP port for me", waits for the port assignment, then connects.
- The application might use a service mesh, a QUIC transport, Unix domain sockets, or an in-memory loopback.
- The application fully controls TLS certificate management, mutual authentication, and connection negotiation.

```rust
/// Identifies a remote peer (process index in the cluster).
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct PeerId {
    pub process: usize,
    // Extensible with additional metadata if needed
}

/// A request from the connection pool to the application to establish a new connection.
/// The pool sends this when it needs a new connection to a peer (either the first connection
/// or to grow the pool / replace a dead connection).
#[derive(Debug)]
pub struct ConnectionRequest {
    /// The target peer to connect to.
    pub peer_id: PeerId,
    /// The local process identity (so the remote side knows who is connecting).
    pub local_id: PeerId,
    /// An opaque request ID for correlation.
    pub request_id: u64,
}

/// Application-implemented trait that establishes connections on behalf of the library.
///
/// The library calls `establish` when the pool needs a new connection to a peer.
/// The application is free to use any mechanism: direct TCP connect, asking a remote
/// actor to open a listener, negotiating through a control plane, etc.
///
/// # Example: Actor-framework integration
/// ```rust,ignore
/// struct ActorConnectionManager {
///     actor_system: ActorRef<NetworkCoordinator>,
/// }
///
/// #[async_trait]
/// impl ConnectionManager for ActorConnectionManager {
///     type Connection = TlsStream<TcpStream>;
///
///     async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Error> {
///         // 1. Ask the remote node's actor to open a listener
///         let port = self.actor_system
///             .ask(OpenPort { for_peer: request.local_id })
///             .await?;
///
///         // 2. Connect to the assigned port
///         let tcp = TcpStream::connect((remote_host, port)).await?;
///
///         // 3. Perform TLS handshake
///         let tls = tls_connector.connect(tcp).await?;
///         Ok(tls)
///     }
/// }
/// ```
#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    /// The bidirectional byte-stream type returned by the manager.
    /// Could be TcpStream, TlsStream, QuicStream, or anything implementing
    /// AsyncRead + AsyncWrite.
    type Connection: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// Establish a new connection to the given peer.
    ///
    /// This is called by the connection pool when it needs a new connection — either
    /// to grow the pool, replace a failed connection, or establish the first connection
    /// to a peer. The application has complete freedom in how it creates the connection.
    ///
    /// The method should return a ready-to-use bidirectional byte stream. Any
    /// handshaking, authentication, or negotiation should be completed before returning.
    async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Error>;
}
```

**Default implementation**: A simple `TcpConnectionManager` is provided for basic use cases:

```rust
/// Default manager that does direct TCP connect to known addresses.
pub struct TcpConnectionManager {
    /// Map from peer process index to its address.
    peer_addresses: HashMap<usize, SocketAddr>,
}

#[async_trait]
impl ConnectionManager for TcpConnectionManager {
    type Connection = TcpStream;

    async fn establish(&self, request: ConnectionRequest) -> Result<TcpStream, Error> {
        let addr = self.peer_addresses.get(&request.peer_id.process)
            .ok_or_else(|| Error::Connection {
                peer_id: request.peer_id.clone(),
                source: "unknown peer".into(),
            })?;
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}
```

### 6.3 Connection Pooling

> **Current state:** `ConnectionPool` (`communication::connection`) is a
> standalone utility; neither transport mode uses it. Shared mode keeps its
> own fixed pool per peer (§6.3.1), and Dedicated mode takes connections the
> application already established (`PeerConnection`). The scaling pool below
> describes `ConnectionPool` only.

The `ConnectionPool` is the library-internal component that manages established connections. It calls `ConnectionManager::establish()` when it needs a new connection and dynamically scales the number of connections per peer based on load.

**Pool lifecycle:**
1. **First use**: When data needs to be sent to a peer, the pool calls `manager.establish(request)`.
2. **Scale up**: Under high throughput, the pool adds connections up to `max_connections_per_peer` by calling `manager.establish()` for each new connection.
3. **Reuse**: After a multiplexed channel finishes using a connection, it's returned to the pool.
4. **Health check**: The pool periodically pings idle connections; dead ones are dropped and replaced via `manager.establish()`.
5. **Scale down**: Connections idle beyond `idle_timeout` are closed, shrinking the pool back toward `min_connections_per_peer`.
6. **Reconnect**: On connection failure, the pool calls `manager.establish()` again.

```rust
pub struct ConnectionPool<M: ConnectionManager> {
    manager: Arc<M>,
    local_id: PeerId,
    pools: DashMap<PeerId, Vec<PooledConnection<M::Connection>>>,
    config: PoolConfig,
    next_request_id: AtomicU64,
}

impl<M: ConnectionManager> ConnectionPool<M> {
    /// Get or create a connection to the given peer.
    /// If no idle connection is available and the pool is not at max capacity,
    /// calls `manager.establish()` to create a new one.
    /// If the pool is at max capacity, waits for a connection to be returned.
    pub async fn acquire(&self, peer_id: &PeerId) -> Result<PoolGuard<M::Connection>, Error> {
        // Try to take an idle connection from the pool
        if let Some(conn) = self.try_take_idle(peer_id) {
            return Ok(conn);
        }
        
        // If below max, ask the application to establish a new connection
        if self.current_count(peer_id) < self.config.max_connections_per_peer {
            let request = ConnectionRequest {
                peer_id: peer_id.clone(),
                local_id: self.local_id.clone(),
                request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
            };
            let connection = self.manager.establish(request).await?;
            return Ok(self.wrap(peer_id.clone(), connection));
        }
        
        // At capacity — wait for a connection to be released
        self.wait_for_release(peer_id).await
    }
    
    /// Return a connection to the pool for reuse.
    /// Called automatically when `PoolGuard` is dropped.
    fn release(&self, peer_id: PeerId, conn: M::Connection) { ... }
}

pub struct PoolConfig {
    /// Minimum connections per peer (maintained even when idle).
    /// Default: 1.
    pub min_connections_per_peer: usize,
    /// Max connections per peer. Pool grows up to this under load.
    /// Default: 4.
    pub max_connections_per_peer: usize,
    /// Idle timeout before closing connections above min_connections_per_peer.
    /// Default: 60s.
    pub idle_timeout: Duration,
    /// Health check interval (default: 30s).
    pub health_check_interval: Duration,
    /// Max time to wait for `establish()` to complete (default: 30s).
    pub connect_timeout: Duration,
}
```

**Key design points**:
- The pool **only** calls `ConnectionManager::establish()` — it never opens sockets, binds ports, or does any networking itself.
- The pool **dynamically scales** between `min_connections_per_peer` and `max_connections_per_peer` based on demand.
- When load is low, idle connections above the minimum are reclaimed after `idle_timeout`.
- The application's `ConnectionManager` is the single point of control for all connection establishment — the pool just tells it when to create or destroy connections.

> **Design invariant: the application establishes every connection.**
> Shared mode establishes its connections through the application's
> `ConnectionFactory`, and multiplexes them across dataflows. Dedicated mode
> takes connections the application already established, one set per
> dataflow. The library is **transport-agnostic** — `ConnectionFactory` returns
> `(AsyncRead, AsyncWrite)` byte streams and works with any transport:
> TCP, TLS, Unix sockets, named pipes, QUIC, etc. instancy ships a default
> `TcpConnectionFactory` for plain TCP; applications that need other
> transports implement `ConnectionFactory` directly.
>
> **Lazy initialization:** The `SharedPeerManager` constructor is synchronous
> and creates no connections. Connections are established lazily in
> `register_dataflow()`, which first fills the pool to
> `SharedConnectionConfig::connections` (each attempt bounded by
> `connect_timeout`) and fails if it cannot, so the caller cannot send frames
> before the pool exists. A later registration replaces connections that
> failed in the meantime; nothing is redialled in the background (see
> §6.3.1).

### 6.3.1 Shared Connection Mode

#### Motivation

Assigning a dedicated connection per (dataflow, peer) pair is simple — a single ordered stream guarantees FIFO, so message ordering is free. However, it limits scalability:
- 100 concurrent dataflows across 10 nodes = 900 connections per node
- Connection setup latency for each new dataflow
- Underutilization of connections when dataflows have bursty traffic

In **shared connection mode** many dataflows share a small, fixed pool of connections to each peer, managed by a long-lived `SharedPeerManager` per peer (similar to how HTTP/2 multiplexes streams over a single connection, or how instancy's worker pool shares OS threads across dataflows).

#### Pinned, sequenced lanes

Data and progress frames of one dataflow to one peer share a single **payload lane**, which preserves the timely ordering invariant (data at time T is sent before the progress message releasing T). When a dataflow registers with a manager, its lane — and its control frames (handshake, ready, cancel) — is **pinned** to one pooled connection, the one with the fewest pinned lanes, and never moves. TCP therefore keeps the lane's frames in order, exactly as in Dedicated mode; different dataflows spread over the pool's connections.

Every payload frame carries the lane's next sequence number (`u64`, 8 bytes in front of the payload). The receiver checks it:

- a frame that does not carry exactly the next number — a lost or duplicated frame — fails the dataflow (`TransportError::OutOfSequence`);
- a lane whose payload frames arrive on two connections (a peer breaking the pinning contract), or a frame for a channel the dataflow never registered, fails it too (`TransportError::LaneViolation`).

Both are corruption for the cluster lifecycle: fatal in every phase. Control frames are outside the lane's numbering: a `Cancel` may overtake queued payload, and a dataflow that failed before registering on a peer sends its `Cancel` on any connection. Everything a worker's progress stream carries — batches, fragments (positives first), its seal, an abandon, its end-of-stream and the acknowledgement of the peer's end-of-stream — travels in the lane, through one FIFO from the progress outbox to the socket, so none of it can overtake the frames sent before it. (`Cancel` is never taken as the end of the peer's stream: in-lane progress that follows it is still applied.)

An earlier design spread one lane's frames over all pooled connections and restored their order with a per-lane reorder buffer that failed the dataflow when a gap outlived a 50 ms timer. Healthy runs failed when a small frame overtook a large one in flight on another connection, or when a reader was starved of CPU (audit H7), and a frame lost with a failed connection went unnoticed if nothing followed it (audit C7). Pinning removes both: nothing is ever reordered, so a gap is always a loss and is reported at once.

#### Fail-stop on connection loss

Frames queued on, or in flight in, a connection that fails may be lost, and nothing resends them. Shared mode is therefore fail-stop, like Dedicated mode and timely:

- when a pooled connection's **writer** fails, every dataflow whose lane is pinned to it is told (`PeerIoEvent::WriteFailed`), the connection leaves the pool, and the whole connection is closed: its reader stops too, and the dataflows whose frames from the peer arrive on it are told as below. Dropping only the write half would tell the peer nothing when the halves come from `tokio::io::split` (TLS streams, pipes): the peer would wait for frames that never come, and keep the connection in its pool while this node replaced it. Closing the whole connection makes the peer fail the same dataflows, and both pools refill together;
- when its **reader** fails (an I/O error, EOF mid-frame, a corrupt or oversize frame), every dataflow whose frames from the peer arrive on it is told (`PeerIoEvent::ReadFailed`) and its channels close once that was handed on, and every dataflow whose lane is pinned to it is told too. A corrupt or oversize frame is reported as such (corruption, fatal in every phase) only to the dataflow named in its verified header; the others sharing the connection are told the connection was lost, which is fatal only while they still need the peer's frames;
- when the peer **closes its side cleanly** (EOF at a frame boundary), the same dataflows are told (`PeerIoEvent::Closed`), but the lanes pinned to the connection keep writing until a write fails. A released manager shuts its writers down and keeps reading for its peers' last frames (goodbyes, acknowledgements, a `Cancel`), so dropping them would defeat that; Dedicated mode behaves the same way. The connection takes no new lanes, leaves the pool (the next registration replaces it), and is closed once its last lane unregisters. Safety rests on the `Closed` event, not on a later write error: like any link failure it is fatal unless the link is already retired (the peer then holds everything we sent and needs nothing more). If the peer is in fact gone, a later write fails and is reported (`WriteFailed`), but with TCP the first writes after the peer's close may still succeed locally;
- a payload frame that cannot be sent — over the frame size limit, or on a reserved channel — fails its own dataflow (`PeerIoEvent::FrameRejected`) without consuming a sequence number; the connection and every other dataflow are untouched.

Both nodes fail the same dataflows: each node pins its own lanes, and a dataflow's frames from the peer arrive on the connection the peer pinned them to, starting with its handshake. With a pool of `connections` connections, one failure fails about `1/connections` of the running dataflows.

There is no background redial. The failed connection leaves the pool, and the next `register_dataflow` re-establishes missing connections through the application's `ConnectionFactory`, each bounded by `connect_timeout`; registration fails if the pool cannot be filled. A dataflow that failed is re-run by the application if it wants a retry, with a fresh `DataflowId` (see "One run per `DataflowId`" below; the same applies to Dedicated mode).

#### Fixed pool

```rust
pub struct SharedConnectionConfig {
    /// Pooled connections per peer (at least 1; both peers use the same value).
    pub connections: usize,          // default 1
    /// How long establishing one connection may take.
    pub connect_timeout: Duration,   // default 10 s
    /// Interval between RTT probes on each connection (metrics only).
    pub probe_interval: Duration,    // default 1 s
    /// EMA smoothing factor for the RTT.
    pub rtt_ema_alpha: f64,          // default 0.2
}
```

The pool does not resize itself. An earlier design grew and shrank it from each node's own RTT probes and idle time, but neither node told the other: frames on a connection only one side opened were never read, and a connection one side closed looked like a failure to the other (audit H7). Resizing needs both nodes to agree on each connection (a coordinated protocol over the control lane); it may come later, together with performance work. RTT probes still run, at data priority, and feed `SharedPeerManager::metrics_snapshot` (per-connection RTT, queued frames, pinned lanes, bytes and frames written); nothing is decided from them.

#### Transport contract

The application provides the connections and owns their **liveness**: detecting that a peer is dead, partitioned or unreachable, through its transport (TCP keepalive, TLS or QUIC idle timeouts) or its membership layer (`RuntimeHandle::report_node_leave`). instancy runs no heartbeat: a second failure detector could disagree with the application's membership, and a false positive would fail every dataflow on a connection at once.

instancy owns **safety**: it never reports success with lost, duplicated, reordered or corrupted data — every frame is checksummed, every lane is sequenced, a stream that ends without its close markers fails the dataflow, and the handshake verifies the peer's identity — and it fails the affected dataflows promptly when a connection reports an error or EOF or the application reports a node leaving. With a healthy transport, a hang inside instancy is a bug.

A connection that silently stops delivering without ever failing makes its dataflows wait; they are never reported as successful. The factory must also pair the connections the two nodes establish (one dials, the other accepts and hands over the accepted stream, for example): a connection whose other end no manager reads delivers nothing, and the dataflows pinned to it fail their handshake.

#### Per-dataflow state at scale

A manager may carry thousands of dataflows. Its per-dataflow state is a registration (pinned connection, two sequence counters, channel senders), removed when the dataflow unregisters. Control frames that arrive before their dataflow registers (the peer was faster) wait in a buffer bounded in dataflows, frames per dataflow and time (and are dropped if their connection fails; the handshake they start then times out). Connection failures cost a scan of the registrations; nothing is per-frame per-dataflow beyond the sequence check.

#### One run per `DataflowId`

A `DataflowId` identifies one run of a dataflow, as an execution-attempt id does in other distributed engines (Flink's `ExecutionAttemptID`, MapReduce's `TaskAttemptID`); timely never reuses a dataflow's identifier either. Frames carry only the `DataflowId`, and pooled connections outlive dataflows, so a run that reused the id of an earlier one could receive that run's late frames as its own: data, progress, a seal, an end-of-stream, numbered from 0 like its own, which is a wrong result no check would catch. A retry therefore needs a fresh `DataflowId`. Shared mode enforces this as far as it can:

- each manager remembers the ids of the dataflows that unregistered (the most recent 16,384), and refuses to register one again, or one that is registered already (`TransportError::InvalidRegistration`). The spawn is refused before anything else, and it sends nothing under the id: a `Cancel` would reach the run that owns it. A peer that did register the id freshly fails at its handshake timeout;
- the late frames of a remembered id are dropped instead of being buffered for a registration;
- the payload frames a dataflow queued before it unregistered carry its registration token, and the bridge drops them, so they are never numbered into another registration's lane; and a cleanup removes only its own registration.

An id reused after it has left the window (16,384 later dataflows on that peer manager) is not detected: that is a contract violation. Dedicated mode opens fresh connections per run, so nothing crosses between runs there. Linking the runs that are retries of one logical computation (a stable logical id and an attempt number) is planned as a separate feature.

#### Comparison: Dedicated vs Shared Connection Mode

| Aspect | Dedicated | Shared |
|--------|-----------|--------|
| **Ordering** | TCP FIFO | TCP FIFO per pinned lane, checked by sequence numbers |
| **Connection count** | O(dataflows × peers) | O(peers × `connections`) |
| **Connection setup** | Per dataflow | Once per pool (and after a failure) |
| **Failure handling** | Fail-stop: the dataflow fails | Fail-stop: the dataflows pinned to the failed connection fail |
| **Wire overhead** | 36 bytes/frame (header, two CRCs) | 44 bytes/frame (+8-byte sequence number) |
| **Throughput per dataflow** | One connection | One connection (pinned) |
| **Cross-dataflow fairness** | Perfect isolation | Shared bandwidth on a connection |

### 6.4 Wire Protocol

Each connection carries multiplexed channels using a simple framing protocol:

```
┌───────────────┬───────────┬───────────┬───────────────┬──────────────────┬───────────────┐
│ dataflow_id   │ channel_id│ length    │ header CRC32  │ payload (codec)  │ frame CRC32   │
│ (UUID, 16B)   │ (u64)     │ (u32)     │ (4B)          │ (variable)       │ (4B)          │
└───────────────┴───────────┴───────────┴───────────────┴──────────────────┴───────────────┘
```

Header size: 16 (dataflow_id UUID) + 8 (channel_id) + 4 (length) = **28 bytes**.

Every frame carries two checksums (cluster protocol version 6 onwards, in both transport modes): a CRC32 of the 28 header bytes follows the header, and a CRC32 of the header followed by the payload follows the payload. The `length` field counts the payload and the trailing frame CRC (i.e., `length = payload_len + 4`). The reader verifies the header CRC *before* it trusts `length`, so a corrupted `length`, `channel_id` or `dataflow_id` is rejected with `ChecksumMismatch` instead of misrouting the frame or desynchronising the stream. (Before cluster protocol version 2 the single trailer covered only the payload; before version 6 the checksums were optional, off by default, and never used by Dedicated mode, so a flipped payload bit was accepted unless it broke decoding.)

The `dataflow_id` field ensures that frames from different dataflows sharing the same pooled connection are never misrouted. Each dataflow is assigned a random UUID at construction time — universally unique without any coordination.

A background demux task reads frames from a connection and dispatches them to the appropriate (dataflow, channel) pair's `mpsc::Sender`. Registering the same `(dataflow, channel)` pair twice is an error (`TransportError::InvalidRegistration`); it used to silently replace the first receiver.

#### Channel-id layout

The `u64` channel-id space of a dataflow is split into namespaces of `2^62` ids (`CHANNEL_NAMESPACE_SIZE`) so ids generated by different subsystems can never collide, whatever the cluster size:

| Range | Use |
|---|---|
| `0` | control channel (`CONTROL_CHANNEL_ID`): handshake, ready barrier, cancellation |
| `1 .. 2^62` | application channels (e.g. `DataflowSession`) |
| `2^62 .. 2^63` | `spawn_cluster` progress: `PROGRESS_CHANNEL_BASE + src * N + dst` |
| `2^63 .. 3·2^62` | `spawn_cluster` exchange data: `DATA_CHANNEL_BASE + (edge * N + src) * N + dst` |
| `3·2^62 ..= u64::MAX` | library-internal (`interprocess::PROGRESS_CHANNEL_ID = 3·2^62`, `PROBE_CHANNEL_ID = u64::MAX`) |

`N` is the total worker count and `edge` the position of the exchange edge among the dataflow's exchange edges. `data_channel_id` / `progress_channel_id` compute ids with overflow-checked arithmetic and return an error if an id would leave its namespace; `spawn_cluster` checks the largest ids of the cluster before allocating any per-worker-pair state. (Before cluster protocol version 2, data ids were `edge * N² + src * N + dst + 1` and progress ids started at `1_000_000`, so they collided for clusters such as 2 × 64 workers with ≥ 15 exchange edges.)

The shared transport delivers the control channel and the probe channel outside the per-dataflow sequenced payload lane. A payload frame sent on either of them is rejected (`TransportError::ReservedChannel` on the dataflow's error channel) without consuming a sequence number; previously it consumed one the receiver never saw, stalling the dataflow's lane behind a permanent gap.

#### End of stream and clean close

Every logical channel of a cluster dataflow ends with an explicit in-band
marker on a clean shutdown: data channels with the close sentinel (`[0xFF]`),
progress channels with the progress end-of-stream frame (`[0xFFFF_FFFF][crc32]`).
Both are queued on the sender's ordered per-peer outbox, behind every frame
that channel carried, so a full transport queue cannot drop them (the outbox is
unbounded; data itself stays on the bounded, backpressured path). The
receiver acknowledges a peer's complete set of progress end-of-stream frames
with an acknowledgement frame (`[0xFFFF_FFFE][crc32]`) on a progress channel,
queued on its own outbox: unlike a control message it cannot overtake the
data the acknowledging node sent before it. Records addressed to a peer whose
link is retired (it finished) fail the push with
`CommunicationError::DataLoss`, and records that reach a node after the
operator they are addressed to finished are reported by the inbound bridge as
data loss; neither is ever dropped silently. A channel or connection that
ends before its marker, on a link that has not been retired by that handshake,
is a connection loss rather than a clean close: the receiving endpoint never
counts it as exhausted, and the dataflow fails cluster-wide (a corrupt or
undecodable frame too). A dedicated `TransportSession` reports each peer's
connection events — EOF, read and write failures, and outbound frames dropped
because they exceed the frame limit (the writer keeps running; nothing was
written) — to the cluster runtime, and keeps the peer's channels open until
the event that ended the read side was handed on. `NetworkPush` rejects an
envelope whose encoding exceeds the transport's frame limit
(`ClusterTransport::max_payload_len`; shared frames carry an 8-byte sequence
prefix) with `CommunicationError::FrameTooLarge`, and the data decoder accepts
any record count the encoder can produce. With shared connections, a
dataflow that must not lose what it has queued (its end-of-stream markers and
acknowledgements, or final progress for a peer that is still draining) calls
`SharedPeerManager::flush` before
unregistering: it puts a local-only barrier (an entry of the manager's own
queue, never a frame) on the payload queue and returns once
the bridge task reaches it, that is, once every earlier frame has been
sequenced and handed to a connection writer. See
[cluster.md §5.5.3](./cluster.md#553-cluster-shutdown-protocol-clean-teardown).

### 6.5 Dataflow Isolation

Multiple dataflows can run concurrently on the same cluster, sharing the same worker thread pool and the same pooled network connections. Isolation between dataflows is maintained at multiple levels:

#### Logical Isolation

Each dataflow is an independent computation graph with:
- Its own `DataflowId` (a random UUID, universally unique)
- Its own operator registry (operator index 3 in dataflow A ≠ operator index 3 in dataflow B)
- Its own channel wiring (each edge gets push/pull endpoints scoped to that dataflow)
- Its own progress tracker instance (frontiers are independent)
- Its own `DataflowMetrics` and `CancellationToken`

Operators in dataflow A **never** share input/output buffers with operators in dataflow B. The `TransportProvider` resolves `LogicalTarget` using the specific dataflow's channel map — there is no global operator namespace.

#### Physical Isolation on Shared Connections

When two dataflows share a pooled connection to the same peer:
- Each frame includes a `dataflow_id` (UUID) field in its wire header
- The demuxer dispatches frames to the correct dataflow's channel receivers based on `(dataflow_id, channel_id)` pair
- A frame with an unknown `dataflow_id` is logged and dropped (e.g., if the dataflow was cancelled but in-flight frames remain)

#### DataflowId Assignment

```rust
/// Cluster-unique identifier for a running dataflow instance.
///
/// Uses a random UUID (v4) — universally unique without coordination.
/// Any node can create a new dataflow without communicating with other nodes.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DataflowId(pub uuid::Uuid);

impl DataflowId {
    /// Create a new random DataflowId.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
```

DataflowIds are random UUIDs generated locally when a dataflow is constructed. No allocator, counter, or coordination is needed. UUID v4 provides 122 bits of randomness — collision probability is negligible even across billions of dataflows.

#### Worker Sharing

Logical workers (`WorkerId`) are **per-dataflow**. Dataflow A's `WorkerId(0)` and dataflow B's `WorkerId(0)` are distinct logical entities. However, they may execute on the same physical OS thread in the worker pool. The scheduler distinguishes them by `(DataflowId, WorkerId)` to maintain per-worker FIFO ordering.

#### Operator Identity

An `operator_index` (usize) is only unique **within** a single dataflow's operator registry. To globally identify an operator across the cluster, the full identity is `(DataflowId, operator_index)`. This composite key is used in metrics collection, tracing spans, and diagnostics. There is no single `GlobalOperatorId` struct — instead, the pairing is carried contextually wherever cross-dataflow disambiguation is needed.

#### Summary: Where DataflowId Appears

| Layer | How DataflowId is Used |
|---|---|
| Logical | Scopes operator/channel allocation; included in LogicalTarget |
| Scheduler | `(DataflowId, WorkerId)` ensures FIFO per logical worker per dataflow |
| Transport (intra-process) | Buffers are per-dataflow — no sharing |
| Transport (inter-process) | Frame header field (UUID) for demux routing |
| Progress | Each dataflow has independent frontier tracking |
| Metrics | Each dataflow has its own DataflowMetrics |


## Integrated design notes

### Connection factory is required

Shared mode acquires its connections from an application-provided connection factory. (Dedicated mode still takes pre-established connections: the Dedicated part of the design below was not adopted.)

## Design: Connection Factory Required

> **Partly superseded:** Shared mode no longer scales its pool, redials in the
> background or reports `ReorderTimeout`; it is fail-stop with a fixed pool of
> `SharedConnectionConfig::connections` connections (filled by
> `ensure_connections()` when a dataflow registers) and pinned lanes (see
> §6.3.1). Dedicated mode still takes `PeerConnection`s. The factory
> requirement for Shared mode still holds.

**Item:** `connection-factory-required`
**Priority:** P1
**Status:** Implemented (PR #220)

### Problem

The current networking design has two independent transport paths:

1. **Dedicated** (`ClusterSpawnTransport::Dedicated`): Takes pre-established
   `PeerConnection` objects. No reconnection, no pooling, no scaling.
   Connections are created externally and consumed by one dataflow.

2. **Shared** (`ClusterSpawnTransport::Shared`): Uses `SharedPeerManager`
   with an *optional* `ConnectionFactory`. Supports reconnection and
   scaling only when a factory is provided.

This creates several problems:

- **Inconsistent reconnection**: Dedicated mode has none. Shared mode has it
  only if a factory happens to be passed.
- **No connection reuse in dedicated mode**: Each dataflow opens fresh
  connections and discards them on completion.
- **Factory is optional but practically required**: Without a factory,
  `SharedPeerManager` cannot scale up or reconnect — the "optional" API is
  misleading.
- **Two code paths**: The runtime, `ClusterTransport`, and `FrameSender` all
  branch on Dedicated vs Shared, doubling maintenance surface.

### Design

#### Unified Connection Model

All connections — regardless of transport mode — are created through a
**required** `ConnectionFactory`. The factory is the single source of truth
for how connections are established.

```
                 ┌──────────────────────┐
                 │  ConnectionFactory   │  ← application-provided (or default TCP)
                 │  (always required)   │
                 └─────────┬───────────┘
                           │ establish()
                           ▼
                 ┌──────────────────────┐
                 │   Connection Pool    │  ← library-managed
                 │  (per-peer, dynamic) │
                 └──┬───────────────┬───┘
                    │               │
            lease(exclusive)   lease(shared)
                    │               │
                    ▼               ▼
             ┌──────────┐   ┌──────────┐
             │Dedicated │   │  Shared   │
             │Dataflow A│   │DF B, C, D │
             └──────────┘   └──────────┘
```

#### Transport Modes

**Dedicated**: A connection is exclusively leased to one dataflow. While
that dataflow runs, no other dataflow uses the connection. When the dataflow
completes, the connection is returned to the pool for reuse. If the
connection breaks, the factory creates a replacement.

**Shared**: Connections are multiplexed across multiple dataflows with
sequenced messages. The pool scales dynamically based on traffic load.

Both modes get:
- Automatic reconnection via factory
- Connection reuse via pool
- Same error semantics (`TransportError::ConnectionClosed`, `ReorderTimeout`)

#### API Changes

##### 1. `SharedPeerManager::new` — factory required, no pre-established connections

```rust
// Before: caller pre-creates connections, factory optional
pub fn new<R, W>(
    peer_node_id: String,
    config: SharedConnectionConfig,
    connections: Vec<(R, W)>,
    connection_factory: Option<Arc<dyn DynConnectionFactory>>,
    runtime_handle: &tokio::runtime::Handle,
) -> Result<Self>

// After: factory required, no pre-established connections
pub fn new(
    peer_node_id: String,
    config: SharedConnectionConfig,
    connection_factory: Arc<dyn DynConnectionFactory>,
    runtime_handle: &tokio::runtime::Handle,
) -> Result<Self>
```

Initial connections are created **lazily** — the constructor stores the
factory but does not call it. When the first dataflow registers,
`ensure_min_connections()` calls `connection_factory.establish_dyn(&peer_node_id)`
to create `config.min_connections` connections on demand. This keeps the
constructor synchronous and defers connection cost until actually needed.

Connection establishment happens **before** the dataflow registration
completes — `ensure_min_connections()` runs at the start of
`register_dataflow()`, so the caller cannot send frames before at least
`min_connections` connections exist. If all connection attempts fail,
the error is immediately surfaced on the dataflow's `error_rx` channel.

Subsequent `register_dataflow()` calls also check the pool against
`min_connections` and top up any deficit (e.g., if a connection died
between registrations). This ensures the configured redundancy floor
is always maintained.

##### 2. `ClusterSpawnTransport::Dedicated` — uses factory + pool

```rust
// Before: takes pre-established PeerConnection objects
Dedicated {
    connections: Vec<PeerConnection<R, W>>,
    capacity: usize,
}

// After: takes factory, pool acquires connections on demand
Dedicated {
    connection_factory: Arc<dyn DynConnectionFactory>,
    peer_node_ids: Vec<String>,
    capacity: usize,
}
```

##### 3. Default `TcpConnectionFactory`

instancy provides a built-in implementation for plain TCP (no TLS):

```rust
/// Default connection factory using plain TCP.
///
/// The application instantiates this with a resolver that maps peer node IDs
/// to socket addresses. For TLS or custom protocols, implement
/// `ConnectionFactory` directly.
pub struct TcpConnectionFactory {
    resolver: Arc<dyn PeerAddressResolver>,
}

pub trait PeerAddressResolver: Send + Sync + 'static {
    fn resolve(&self, peer_node_id: &str) -> Option<SocketAddr>;
}
```

Applications that need TLS, actor-framework integration, Unix sockets,
named pipes, QUIC, or any other reliable byte-stream transport implement
`ConnectionFactory` themselves — the library is transport-agnostic.

##### 4. Remove `Option` checks in scaling handler

The scaling handler (`handle_scaling_events`) currently checks
`let Some(factory) = connection_factory.clone()` for both `ScaleUp` and
`ConnectionFailed`. With factory required, these become direct calls —
simpler code, no silent ignore paths.

#### What Stays the Same

- `TransportSession` — remains as an internal component used by dedicated
  mode under the hood (connections obtained from factory, not caller)
- `ClusterTransport` enum with Dedicated/Shared variants — the variants
  stay, but both now have reconnection
- `FrameSender::Direct` / `FrameSender::Shared` — stays, as the send
  mechanics differ between modes

#### What Gets Removed

- `PeerConnection` struct — no longer needed since callers never provide
  pre-established connections. Internal code uses raw `(reader, writer)`
  pairs returned by the factory.
- `connections` parameter from `SharedPeerManager::new`
- `Option` wrapper on `connection_factory` throughout `shared_transport.rs`

### Reconnection responsibility

The hosting application owns connection establishment (its connection factory or manager: TLS, identity, network topology) and liveness (detecting a dead or partitioned peer, through its transport or `report_node_leave`). instancy does not retry or redial a failed connection in the background: the dataflows that used it fail, and a later dataflow re-establishes the pool through the factory (see §6.3.1, Transport contract).

## Design: Document Reconnection Responsibility

> **Superseded:** the background reconnect described here was removed; see
> §6.3.1 and the "Transport Contract" section of `docs/guide/distributed.md`.

**Item:** `net-doc-reconnect`
**Priority:** P2
**Status:** Design

### Problem

instancy has reconnection logic in `SharedTransport` (exponential backoff,
connection factory retry) but none of this is documented for users. The
user guide, `README.md`, and key struct doc comments are silent on:

- What happens when a TCP connection drops mid-dataflow
- Who is responsible for reconnection (library vs application)
- How the connection factory enables automatic reconnection
- What errors the application sees on permanent failure

### Changes

1. **Add a "Connection Failure & Reconnection" section to `docs/guide/distributed.md`** covering:
   - SharedTransport automatic reconnect with backoff (100ms→800ms, 5 attempts, 4 delays)
   - Connection factory role (if provided, library retries; if not, failure is permanent)
   - Application-level errors: `TransportError::ConnectionClosed` and `TransportError::ReorderTimeout`
   - Payload frames dropped during reconnection; lost sequenced frames cause reorder gaps

2. **Add doc comments to `PeerConnection` and `TransportSession`** noting
   that these are pre-established connections with no built-in reconnection —
   reconnection is handled at the `SharedTransport` layer.

3. **Add doc comments to `SharedTransport` reconnect methods** summarizing
   the retry behavior inline.


### Lazy bounded-channel allocation

Bounded channels still enforce the same logical capacity limits, but they no longer eagerly allocate their full logical capacity at construction time. This keeps wide topologies from paying the worst-case memory cost up front.

## Design: Lazy-Allocate Bounded Channels

**Item:** `mem-lazy-channels`
**Priority:** P1
**Status:** Design

### Problem

Bounded channels pre-allocate their internal `VecDeque` buffer at creation
time using `VecDeque::with_capacity(capacity)`. The default capacity is 1024.

A multi-worker dataflow with many exchange edges creates dozens of channels,
most of which may carry little or no data. Pre-allocating 1024 entries per
channel wastes memory — especially for wide fan-out topologies.

### Change

Replace `VecDeque::with_capacity(capacity)` with `VecDeque::with_capacity(4)` in all
channel constructors. The initial allocation of 4 covers typical minimum traffic
(data message + progress message + control messages) without triggering immediate
reallocation, while being much smaller than the default logical capacity of 1024.

The logical capacity limit (used for backpressure) is unchanged — only the initial
physical allocation is reduced. `VecDeque` grows via the standard doubling strategy
as data arrives, stabilizing at actual usage rather than the logical maximum.

#### Sites to change

1. `dataflow/channels/bounded.rs:61` — main bounded channel
2. `communication/allocator.rs:96` — allocator local channel
3. `dataflow/channels/mock_network.rs:79` — mock byte channel

### Testing

- All existing tests must pass (backpressure behavior is unchanged)
- Clippy clean
