# Design: Connection Factory Required

**Item:** `connection-factory-required`
**Priority:** P1
**Status:** Design

## Problem

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

## Design

### Unified Connection Model

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

### Transport Modes

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

### API Changes

#### 1. `SharedPeerManager::new` — factory becomes required

```rust
// Before
pub fn new<R, W>(
    peer_node_id: String,
    config: SharedConnectionConfig,
    connections: Vec<(R, W)>,
    connection_factory: Option<Arc<dyn DynConnectionFactory>>,  // optional
    runtime_handle: &tokio::runtime::Handle,
) -> Result<Self>

// After
pub fn new<R, W>(
    peer_node_id: String,
    config: SharedConnectionConfig,
    connections: Vec<(R, W)>,
    connection_factory: Arc<dyn DynConnectionFactory>,  // required
    runtime_handle: &tokio::runtime::Handle,
) -> Result<Self>
```

#### 2. `ClusterSpawnTransport::Dedicated` — uses factory + pool

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

#### 3. Default `TcpConnectionFactory`

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

Applications that need TLS, actor-framework integration, or custom
protocols implement `ConnectionFactory` themselves.

#### 4. Remove `Option` checks in scaling handler

The scaling handler (`handle_scaling_events`) currently checks
`let Some(factory) = connection_factory.clone()` for both `ScaleUp` and
`ConnectionFailed`. With factory required, these become direct calls —
simpler code, no silent ignore paths.

### What Stays the Same

- `PeerConnection` struct — still used for pre-established connections in
  `TransportSession` (internal / test usage)
- `TransportSession` — remains as an internal component, may be used
  by dedicated mode under the hood
- `ClusterTransport` enum with Dedicated/Shared variants — the variants
  stay, but both now have reconnection
- `FrameSender::Direct` / `FrameSender::Shared` — stays, as the send
  mechanics differ between modes

### Test Strategy

- Tests using in-memory duplex streams: create a mock factory that wraps
  pre-created duplex pairs, or use a local TCP listener on 127.0.0.1
- Tests using real TCP: instantiate `TcpConnectionFactory` with a static
  address resolver
- `cluster_shared_transport.rs`: update `SharedPeerManager::new` calls to
  pass a factory (currently passes `None`)

## Files to Change

1. `shared_transport.rs` — `connection_factory` field and constructor: `Option<Arc>` → `Arc`
2. `shared_transport.rs` — `handle_scaling_events`: remove `Option` unwrap checks
3. `cluster_transport.rs` — `ClusterSpawnTransport::Dedicated` variant: add factory
4. `runtime.rs` — `spawn_cluster` Dedicated branch: use factory to establish connections
5. `shared_transport.rs` — add `TcpConnectionFactory` + `PeerAddressResolver`
6. Tests: update all `SharedPeerManager::new` calls and Dedicated-mode tests
7. `GUIDE.md` — update "Connection Failure & Reconnection" section
8. `DESIGN.md` — update §6.2, §6.3, §6.3.1, §12.4

## Non-Goals

- Removing `ClusterSpawnTransport::Dedicated` variant entirely — the
  exclusive-lease model is still useful for isolation
- Changing the `ConnectionFactory` trait signature
- Implementing connection health checks or idle timeout (future work)
