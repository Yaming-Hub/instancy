# Communication Layer

This document covers the physical transport layer behind instancy's logical channels: in-process queues, application-provided connection establishment, pooled/shared connections, framing, and dataflow isolation across shared resources.

Back to the overview: [docs/DESIGN.md](./DESIGN.md)

## 6. Communication Layer

The communication layer implements the physical delivery mechanisms behind the `TransportProvider` trait (§4.5). At the logical layer, operators only see `Push` and `Pull` endpoints. The communication layer provides the concrete implementations.

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

`Envelope` (defined in §5.8) carries data batches, control signals, and user-defined metadata through the same buffer.

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

> **Design invariant: Factory is always required.** Both dedicated and shared
> transport modes use the connection factory. In dedicated mode the pool
> leases a connection exclusively to one dataflow and returns it on
> completion. In shared mode connections are multiplexed across dataflows.
> Either way, reconnection and scale-up go through the same factory.
> The library is **transport-agnostic** — `ConnectionFactory` returns
> `(AsyncRead, AsyncWrite)` byte streams and works with any transport:
> TCP, TLS, Unix sockets, named pipes, QUIC, etc. instancy ships a default
> `TcpConnectionFactory` for plain TCP; applications that need other
> transports implement `ConnectionFactory` directly.
>
> **Lazy initialization:** The `SharedPeerManager` constructor is synchronous
> and creates no connections. Connections are established lazily in
> `register_dataflow()` — specifically, `ensure_min_connections()` runs
> **before** the dataflow is registered, guaranteeing that the caller cannot
> send frames until at least `min_connections` connections exist. Each
> subsequent registration also tops up the pool to the configured floor.
> If all connection attempts fail, a `ConnectionClosed` error is immediately
> delivered on the dataflow's error channel.

### 6.3.1 Shared Connection Mode with Sequenced Messages

#### Motivation

Assigning a dedicated connection per (dataflow, peer) pair is simple — a single ordered stream guarantees FIFO, so message ordering is free. However, it limits scalability:
- 100 concurrent dataflows across 10 nodes = 900 connections per node
- Connection setup latency for each new dataflow
- Underutilization of connections when dataflows have bursty traffic

A **shared connection mode** would allow multiple dataflows (and multiple workers within a dataflow) to share the same node-to-node connection pool, similar to how HTTP/2 multiplexes streams over a single connection, or how instancy's worker thread pool shares OS threads across dataflows.

#### The Ordering Challenge

With shared connections, a single worker's messages may travel over **different TCP connections** (e.g., load-balanced across pool connections, or after a connection failure triggers failover). TCP only guarantees FIFO within a single stream — **not across streams**. This breaks the timely ordering invariant.

Example scenario:
```
Worker 0 sends: [data(epoch=5), progress(epoch=5 done)]
                     │                      │
                     ▼                      ▼
              Connection A            Connection B   (load-balanced)
                     │                      │
                     ▼                      ▼
Receiver sees: progress(epoch=5 done)  THEN  data(epoch=5)  ← ORDERING VIOLATION
```

Additionally, connection failures introduce:
- **Lost messages** — in-flight frames on a broken connection
- **Duplicate messages** — retried frames that were actually delivered before the failure was detected

#### Proposed Design: Sequenced Messages

Each frame is stamped with a **sequence ID** scoped to its logical stream:

```
Message Identity: (dataflow_id, channel_id, sequence_id)
```

The `channel_id` already encodes `(edge/stage, source_worker, dest_worker)` — this is necessary because different stages can have different worker counts (per-stage parallelism). A stage with 4 workers and a downstream stage with 2 workers produce different sets of logical streams. The sequence is per logical stream, not per worker globally.

**Wire protocol extension:**

```
┌───────────────┬───────────┬──────────────┬───────────┬──────────────────┐
│ dataflow_id   │ channel_id│ sequence_id  │ length    │ payload (codec)  │
│ (UUID, 16B)   │ (u64)     │ (u64)        │ (u32)     │ (variable)       │
└───────────────┴───────────┴──────────────┴───────────┴──────────────────┘
```

Header size: 16 + 8 + 8 + 4 = **36 bytes** (8 bytes overhead vs current 28).

**Sender behavior:**
- Each `(dataflow_id, channel_id)` pair maintains a monotonically increasing sequence counter
- Every frame sent on any connection is stamped with the next sequence number
- On send failure, the sender retries the same frame (same sequence_id) on a different connection

**Receiver behavior:**
- Per `(dataflow_id, channel_id)`, tracks `next_expected_seq`
- If received frame's `sequence_id == next_expected_seq`: deliver immediately, increment counter
- If `sequence_id > next_expected_seq` (gap): buffer the frame, wait up to `reorder_timeout` for the missing frame(s)
- If `sequence_id < next_expected_seq` (duplicate): discard silently (already delivered)
- If timeout expires with gap: fail the dataflow (data loss detected — unrecoverable)

```rust
/// Receiver-side reorder buffer per logical stream.
struct ReorderBuffer {
    next_expected: u64,
    /// Buffered out-of-order frames, keyed by sequence_id.
    pending: BTreeMap<u64, Frame>,
    /// How long to wait for a missing frame before failing.
    reorder_timeout: Duration,
    /// Timestamp when the gap was first detected.
    gap_detected_at: Option<Instant>,
}

impl ReorderBuffer {
    fn receive(&mut self, seq: u64, frame: Frame) -> ReorderAction {
        if seq < self.next_expected {
            return ReorderAction::Duplicate; // discard
        }
        if seq == self.next_expected {
            // Deliver this frame and any consecutive buffered frames
            self.next_expected += 1;
            self.gap_detected_at = None;
            let mut deliver = vec![frame];
            while let Some(f) = self.pending.remove(&self.next_expected) {
                deliver.push(f);
                self.next_expected += 1;
            }
            return ReorderAction::Deliver(deliver);
        }
        // seq > next_expected: gap detected
        self.pending.insert(seq, frame);
        if self.gap_detected_at.is_none() {
            self.gap_detected_at = Some(Instant::now());
        }
        ReorderAction::Wait
    }

    fn check_timeout(&self) -> bool {
        self.gap_detected_at
            .map(|t| t.elapsed() > self.reorder_timeout)
            .unwrap_or(false)
    }
}
```

#### Comparison: Dedicated vs Shared Connection Mode

| Aspect | Dedicated | Shared (Current) |
|--------|---------------------|------------------------------|
| **Ordering** | Free (TCP FIFO) | Explicit via sequence numbers |
| **Connection count** | O(dataflows × peers) | O(peers) — bounded by pool size |
| **Connection setup** | Per-dataflow latency | Amortized — pool pre-warms |
| **Failure handling** | Dataflow fails immediately | Retry on another connection; fail only on timeout |
| **Duplicate detection** | Not needed | Free via sequence_id comparison |
| **Wire overhead** | 28 bytes/frame | 36 bytes/frame (+8 bytes seq_id) |
| **Receiver complexity** | Zero buffering | Reorder buffer per logical stream |
| **Memory overhead** | Minimal | Reorder buffers + pending maps |
| **Latency** | Minimal (direct write) | Possible reorder wait on out-of-order delivery |
| **Throughput** | Limited by single connection | Higher — parallel writes across pool connections |
| **Cross-dataflow fairness** | Perfect isolation | Shared bandwidth — needs fair scheduling |
| **Implementation complexity** | Simple | Moderate (sequencing, buffering, timeout, retry) |

#### Pros of Shared Connection Mode (with Adaptive Scaling)

1. **Resource efficiency** — O(peers) base connections instead of O(dataflows × peers). Critical at scale.
2. **Connection reuse** — new dataflows start instantly on existing pool connections.
3. **Resilience** — connection failure doesn't kill the dataflow; retry on alternate connection.
4. **Higher throughput** — parallel connections per peer with independent congestion windows. A single connection cannot fully utilize high-bandwidth links due to bandwidth-delay product limits.
5. **Simpler lifecycle** — no need to establish/teardown connections per dataflow.
6. **Self-tuning latency** — RTT probes detect congestion early; adaptive scaling adds connections to maintain latency target. Under heavy load, shared mode achieves *lower* latency than dedicated mode (which is stuck with a single saturated connection).
7. **Graceful degradation** — under light load, operates with min_connections (essentially dedicated mode behavior with negligible overhead). Scales up only when measured RTT justifies it.

#### Cons of Shared Connection Mode (with Adaptive Scaling)

1. **Implementation complexity** — sequence management, gap detection, timeout handling, duplicate filtering, RTT probing, and scaling logic. Significantly more code than dedicated mode.
2. **Memory overhead** — per-stream reorder buffers with pending frame storage. Bounded by `max_connections × max_in_flight_per_connection`.
3. **Failure semantics change** — "connection broken" no longer means "dataflow dead" — must propagate failure differently (timeout-based after all retry paths exhausted).
4. **Wire overhead** — 8 extra bytes per frame for sequence_id. Negligible for data payloads; ~20% overhead for small progress messages (~40 bytes). Sub-microsecond parsing cost — irrelevant vs network RTT.
5. **Brief reorder windows** — during connection scale-up/scale-down transitions, frames may arrive out-of-order for a short period. The reorder buffer handles this transparently but adds a brief latency spike (~probe_interval duration).

#### Performance Analysis: Adaptive Scaling Mitigates Original Concerns

The original fixed-pool design had real performance concerns. Adaptive scaling addresses each:

| Concern | Fixed Pool (no scaling) | With Adaptive Scaling |
|---------|------------------------|----------------------|
| **Latency under light load** | Same as dedicated | Same as dedicated (min_connections ≈ 1-2, near-zero overhead) |
| **Latency under heavy load** | Reorder waits when frames contend | **Better than dedicated** — scales connections to maintain RTT below threshold |
| **Head-of-line blocking** | Real risk — stalled connection blocks all streams | Detected via RTT probe in ~100ms; load balancer routes around stalled connection |
| **Throughput ceiling** | Fixed by pool size | Scales dynamically — each new connection adds an independent TCP congestion window |
| **Connection overhead at rest** | Fixed pool wastes resources | Scales down to min_connections during idle periods |

**Key insight:** Dedicated mode has a fundamental limitation — under heavy load, a single connection saturates with no recovery path. The adaptive shared mode is the only design that maintains latency invariants across all load levels, because it uses measured feedback (RTT probes) to trigger corrective action (add connections) before saturation causes visible delays.

**When does shared mode equal or beat dedicated?**
- **Light load:** Equivalent (min_connections, no reorder waits, negligible sequence overhead)
- **Moderate load:** Equivalent or better (single connection handles load, probes confirm healthy RTT)
- **Heavy load:** Significantly better (scales to multiple connections, parallel throughput, bounded latency)
- **Connection failure:** Significantly better (retry on alternate, no dataflow death)

The only scenario where dedicated mode wins is **zero-overhead simplicity** for deployments that never scale beyond moderate load and don't need resilience.

#### Adaptive Connection Scaling

The shared connection pool does **not** use a fixed number of connections per peer. Instead, it dynamically scales connections based on measured load — similar to how the worker pool scales threads within a min/max range.

**Configuration:**
```rust
pub struct SharedConnectionConfig {
    /// Minimum connections to maintain per peer (pre-warmed).
    pub min_connections: usize,           // e.g., 1
    /// Maximum connections allowed per peer.
    pub max_connections: usize,           // e.g., 16
    /// RTT threshold: scale up when probe RTT exceeds this.
    pub rtt_scale_up_threshold: Duration, // e.g., 5ms
    /// RTT target: scale down when probe RTT is below this for sustained period.
    pub rtt_scale_down_threshold: Duration, // e.g., 1ms
    /// How long RTT must stay below scale-down threshold before removing a connection.
    pub cooldown_period: Duration,        // e.g., 30s
    /// Interval between probe messages.
    pub probe_interval: Duration,         // e.g., 100ms
    /// Timeout for reorder buffer gap detection.
    pub reorder_timeout: Duration,        // e.g., 50ms
    /// Close idle connections after this duration of inactivity.
    /// Connections with no write activity for longer than this are
    /// removed (down to min_connections). Set to None to disable.
    pub idle_timeout: Option<Duration>,   // e.g., Some(60s)
}
```

**Load measurement signals:**

1. **RTT probes** (primary signal) — Lightweight probe messages sent at `probe_interval` with the **same priority as data** (travel through the same FIFO path). Measures true end-to-end latency including TCP buffer congestion. When RTT exceeds `rtt_scale_up_threshold`, it indicates the connection is saturated.

2. **Send queue depth** — Number of frames buffered in the write queue waiting to be flushed to TCP. High queue depth means the connection can't drain fast enough.

3. **Throughput per connection** — Bytes/sec actually written. When throughput plateaus while queue depth grows, the connection is at capacity.

4. **TCP kernel metrics** (optional, platform-specific) — `TCP_INFO` on Linux provides `tcpi_rtt`, `tcpi_retransmits`, `tcpi_snd_cwnd`. Direct visibility into TCP congestion state.

5. **Idle detection** — Each connection tracks its last write activity timestamp. When a connection has had no frames written for longer than `idle_timeout`, it is considered idle and a candidate for removal (down to `min_connections`). This prevents resource waste when traffic subsides after a burst.

**Scaling algorithm:**

```
On each probe response:
  1. Update exponential moving average of RTT for this connection
  2. If avg_rtt > rtt_scale_up_threshold AND current_connections < max_connections:
       - Establish new connection to peer
       - Begin load-balancing frames across all connections (round-robin or least-loaded)
  3. If any connection has been idle > idle_timeout
     AND current_connections > min_connections:
       - Close the longest-idle connection (no drain needed — no pending writes)
  4. If avg_rtt < rtt_scale_down_threshold for > cooldown_period
     AND current_connections > min_connections:
       - Drain one connection (stop sending new frames, wait for in-flight to complete)
       - Close the drained connection
```

**Probe message design:**

```
┌──────────────┬──────────────┬───────────────┐
│ PROBE_REQUEST│ probe_seq: u64│ send_ts: u64  │
└──────────────┴──────────────┴───────────────┘
         ↓ peer echoes back:
┌──────────────┬──────────────┬───────────────┐
│ PROBE_REPLY  │ probe_seq: u64│ send_ts: u64  │
└──────────────┴──────────────┴───────────────┘
```

Probes are sent at data priority (not control priority) because we want to measure the latency that **data actually experiences**. A probe bypassing the data queue would underestimate congestion.

**Load-balancing frames across connections:**

When multiple connections exist to the same peer, frames are distributed using a **load-aware packing** strategy:

- **Low load** (total pending writes < connection count): traffic is *concentrated* onto the fewest connections. The busiest connection is selected, packing frames onto it. This leaves other connections idle so they can be cleaned up by the idle timeout, naturally shrinking the pool when demand subsides.
- **High load** (total pending writes ≥ connection count): traffic is *spread* across connections using least-loaded selection (smallest pending write queue). This maximizes throughput by utilizing all connections' independent congestion windows.
- Sequence IDs ensure ordering is reconstructed at the receiver regardless of which connection carried each frame.

**Why not just one connection?**

A single connection has fundamental throughput limits:
- Congestion window limits in-flight bytes (TCP, QUIC, etc.)
- High-bandwidth links with significant RTT ("bandwidth-delay product") need large windows
- A single stream cannot fully utilize a 10 Gbps link with 1ms RTT without ~1.25 MB in-flight
- Multiple connections achieve better utilization by having independent congestion windows

#### Recommendation

**Phase 1:** Dedicated connections — each dataflow gets its own connection(s) per peer. Simple and correct.

**Phase 2 (current):** Shared mode via `SharedPeerManager` / `PeerPool` — dataflows share adaptive pooled connections managed by `ConnectionFactory`. Frames are sequenced for ordering/dedup. Configured via `SharedConnectionConfig`:
```rust
pub struct SharedConnectionConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub enable_frame_crc: bool,
    // ...
}
```

The sequencing and adaptive scaling layers should be implemented **below** the `TransportSession` abstraction — `TransportSession` continues to see a reliable ordered stream regardless of the underlying connection mode. This keeps operator code and progress tracking unchanged.


### 6.4 Wire Protocol

Each connection carries multiplexed channels using a simple framing protocol:

```
┌───────────────┬───────────┬───────────┬──────────────────┬──────────────┐
│ dataflow_id   │ channel_id│ length    │ payload (codec)  │ CRC32 (opt)  │
│ (UUID, 16B)   │ (u64)     │ (u32)     │ (variable)       │ (4B)         │
└───────────────┴───────────┴───────────┴──────────────────┴──────────────┘
```

Header size: 16 (dataflow_id UUID) + 8 (channel_id) + 4 (length) = **28 bytes**.

When CRC is enabled, a 4-byte CRC32 trailer follows each payload. The `length` field includes the CRC bytes (i.e., `length = payload_len + 4`). CRC is **opt-in** via `SharedConnectionConfig::enable_frame_crc` (default: `false`), letting the hosting application trade performance for integrity on unreliable networks. The reader validates the checksum and returns a `ChecksumMismatch` error on corruption.

The `dataflow_id` field ensures that frames from different dataflows sharing the same pooled connection are never misrouted. Each dataflow is assigned a random UUID at construction time — universally unique without any coordination.

A background demux task reads frames from a connection and dispatches them to the appropriate (dataflow, channel) pair's `mpsc::Sender`.

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

The connection model has been unified around a required application-provided connection factory. Both dedicated and shared modes now acquire connections from the same pool, which gives both modes the same reconnection and reuse story.

## Design: Connection Factory Required

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

#### Test Strategy

- Tests using in-memory duplex streams: implement a mock `ConnectionFactory`
  that creates `tokio::io::duplex()` pairs internally (each `establish()`
  call creates a new duplex pair and sends the other half to the peer's
  factory via a shared channel)
- Tests using real TCP: instantiate `TcpConnectionFactory` with a static
  address resolver pointing to `127.0.0.1`
- `cluster_shared_transport.rs`: update `SharedPeerManager::new` calls —
  remove pre-created connections, pass factory instead

### Files to Change

1. `shared_transport.rs` — `connection_factory` field and constructor: `Option<Arc>` → `Arc`
2. `shared_transport.rs` — `handle_scaling_events`: remove `Option` unwrap checks
3. `cluster_transport.rs` — `ClusterSpawnTransport::Dedicated` variant: add factory
4. `runtime.rs` — `spawn_cluster` Dedicated branch: use factory to establish connections
5. `shared_transport.rs` — add `TcpConnectionFactory` + `PeerAddressResolver`
6. Tests: update all `SharedPeerManager::new` calls and Dedicated-mode tests
7. `GUIDE.md` — update "Connection Failure & Reconnection" section
8. `DESIGN.md` — update §6.2, §6.3, §6.3.1, §12.4

### Non-Goals

- Removing `ClusterSpawnTransport::Dedicated` variant entirely — the
  exclusive-lease model is still useful for isolation
- Changing the `ConnectionFactory` trait signature
- Implementing connection health checks or idle timeout (future work)


### Reconnection responsibility

instancy owns retrying and reconnecting transport sessions that were created through the shared transport layer. The hosting application owns the actual connection-establishment logic by implementing the connection factory/manager, including TLS, identity, and network topology decisions.

## Design: Document Reconnection Responsibility

**Item:** `net-doc-reconnect`
**Priority:** P2
**Status:** Design

### Problem

instancy has reconnection logic in `SharedTransport` (exponential backoff,
connection factory retry) but none of this is documented for users. The
`GUIDE.md`, `README.md`, and key struct doc comments are silent on:

- What happens when a TCP connection drops mid-dataflow
- Who is responsible for reconnection (library vs application)
- How the connection factory enables automatic reconnection
- What errors the application sees on permanent failure

### Changes

1. **Add "Connection Failure & Reconnection" section to GUIDE.md** covering:
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
