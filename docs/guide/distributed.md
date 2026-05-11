# Distributed Execution

instancy can run the same logical graph across multiple nodes using application-supplied connections. This page covers topology setup, transport choices, local test harnesses, and peer-failure handling.

[Back to the guide index](./README.md)

instancy can distribute computation across multiple machines using peer-to-peer connections. The library is **transport-agnostic** — it works with any reliable, ordered byte stream: TCP, TLS, Unix sockets, named pipes, QUIC, or even in-memory duplex channels. Unlike timely-dataflow's fixed hostfile approach, instancy delegates connection establishment to your application via a `ConnectionFactory`.

### Cluster Topology

First, describe your cluster:

```rust
use instancy::{ClusterTopology, NodeConfig};

let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 2),  // 2 workers on node A
    NodeConfig::new("node-b", 2),  // 2 workers on node B
]).unwrap();
```

### Establishing Connections

You provide the connections via a `ConnectionFactory`. This means you control the transport, TLS, authentication, and discovery:

```rust
// Your application establishes connections however it likes:
// - Plain TCP, mTLS, via service mesh, through an actor framework, etc.
// - instancy just needs AsyncRead + AsyncWrite streams

use instancy::communication::transport_session::PeerConnection;

let connections: Vec<PeerConnection<_, _>> = vec![
    PeerConnection {
        node_id: "node-b".to_string(),
        reader: tcp_read_half,
        writer: tcp_write_half,
    },
];
```

#### TLS Example

Since instancy accepts any `AsyncRead + AsyncWrite` stream, you can use TLS (or mTLS) by
establishing TLS connections in your application code and passing the resulting streams:

```rust
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls};
use rustls::pki_types::ServerName;
use instancy::communication::transport_session::PeerConnection;
use std::sync::Arc;

// Load your certificates and build a TLS config
let tls_config = rustls::ClientConfig::builder()
    .with_root_certificates(load_ca_certs())       // your CA bundle
    .with_client_auth_cert(client_certs, client_key) // for mTLS
    .unwrap();
let connector = TlsConnector::from(Arc::new(tls_config));

// Establish a TLS connection to a peer node
let tcp_stream = TcpStream::connect("peer-b.example.com:9000").await?;
let server_name = ServerName::try_from("peer-b.example.com")?;
let tls_stream = connector.connect(server_name, tcp_stream).await?;

// Split into read/write halves and hand to instancy
let (reader, writer) = tokio::io::split(tls_stream);

let connections = vec![
    PeerConnection {
        node_id: "node-b".to_string(),
        reader,
        writer,
    },
];

// Pass `connections` to rt.spawn_cluster(...) — instancy uses them as-is.
// It never opens sockets or negotiates TLS itself; that's entirely your responsibility.
```

This pattern works with any TLS library (`tokio-rustls`, `tokio-native-tls`, `s2n-tls-tokio`, etc.)
and any authentication scheme (one-way TLS, mutual TLS, custom certificate validation).

### Spawning a Cluster Dataflow

```rust
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions};
use instancy::communication::ClusterSpawnTransport;
use instancy::DataflowId;
use std::time::Duration;

let rt = RuntimeHandle::new(RuntimeConfig {
    worker_threads: 4,
    ..Default::default()
}).unwrap();

let dataflow_id = DataflowId::new();
// Requires a Tokio runtime — e.g., use #[tokio::main] or build one manually.
let tokio_handle = tokio::runtime::Handle::current();

// Wrap connections in a transport config (dedicated = one connection per peer).
let transport = ClusterSpawnTransport::dedicated(connections, 1024);

let mut cluster_handle = rt.spawn_cluster(
    "my_distributed_df",
    topology,
    "node-a",                          // This node's ID
    dataflow_id,
    transport,
    Duration::from_secs(10),           // Handshake timeout
    |builder| {
        // Build the same graph on every node
        let input = builder.input::<String>("data").unwrap();
        input
            .exchange_by_hash("route", |s: &String| {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                h.finish()
            })
            .unary("process", {
                move |input, output| {
                    while let Some((t, data)) = input.next() {
                        for item in data {
                            output.push(t, item.to_uppercase());
                        }
                    }
                    Ok(())
                }
            })
            .output("results").unwrap();
        Ok(())
    },
    &tokio_handle,
    SpawnOptions::new(),
).unwrap();
```

**Key points:**
- Every node must call `spawn_cluster` with the same `DataflowId` concurrently
- The library performs a handshake to verify all nodes agree on the dataflow structure
- Exchange operators automatically route data across nodes via the connections
- `SpawnOptions` controls observability, cancellation, I/O mode, and priority

### Dedicated vs Shared Transport

`ClusterSpawnTransport` supports two modes:

**Dedicated** — each `spawn_cluster` call gets its own exclusive connections. Simple but uses one connection per peer per dataflow:

```rust
use instancy::communication::ClusterSpawnTransport;

let transport = ClusterSpawnTransport::dedicated(connections, 1024);
```

**Shared** — multiple dataflows multiplex over pooled connections managed by `SharedPeerManager`s. Ideal when running many concurrent dataflows:

```rust
use instancy::communication::ClusterSpawnTransport;
use std::sync::Arc;

// Create peer managers once, share across dataflows.
// peer_managers: HashMap<String, SharedPeerManager>
let managers = Arc::new(peer_managers);
let transport = ClusterSpawnTransport::shared(managers, 1024);
```

With shared transport, connection pooling, reconnection, and multiplexing are handled automatically. See `tests/cluster_shared_transport.rs` for a complete example.

### Testing Clusters Locally

You don't need multiple machines to test distributed dataflows. Use in-memory duplex streams:

```rust
use tokio::io::duplex;

// Create a bidirectional in-memory connection
let (a_to_b, b_to_a) = duplex(8192);
let (a_read, a_write) = tokio::io::split(a_to_b);
let (b_read, b_write) = tokio::io::split(b_to_a);
```

This is how instancy's own integration tests work — see `tests/cluster.rs` for examples.

### Handling Node Failures

Cluster health monitoring (heartbeats, liveness probes) is the hosting application's responsibility — instancy does not run its own health checks. When the application detects a peer node is unreachable, it notifies the runtime:

```rust
// Application detects node-3 is unreachable (via its own health monitoring)
let cancelled = runtime.report_node_leave("node-3");
println!("Cancelled {cancelled} dataflows due to node-3 failure");
```

**What happens:**
1. All cluster dataflows with workers on `"node-3"` are cancelled with `CancellationReason::PeerDown { node_id: "node-3".into() }`.
2. Both the local worker executors and network bridge tasks are stopped.
3. `DataflowCompletion` resolves with a cancellation error — the application can match on the `PeerDown` reason.

**No automatic rescheduling:** instancy does not attempt to move computation to surviving nodes. The application retries the dataflow on healthy nodes:

```rust
match cluster_dataflow.join_blocking() {
    Err(e) if e.is_cancelled() => {
        // Check reason
        if let Some(CancellationReason::PeerDown { node_id }) = e.cancellation_reason() {
            println!("Peer {node_id} went down, retrying on healthy nodes...");
            // Rebuild topology without the failed node and re-spawn
        }
    }
    other => { /* handle normally */ }
}
```

**Peer recovery:** If a previously-down peer comes back online, notify the runtime so future dataflows can use it:

```rust
// Application detects node-3 is back online
runtime.report_node_join("node-3");
// Now safe to spawn_cluster with node-3 in the topology again
```

Already-cancelled dataflows are **not** restarted — the application must re-spawn them if desired.

### Connection Failure & Reconnection

When using `SharedTransport` for cluster networking, connection failures are handled automatically if a **connection factory** is provided.

**Automatic reconnection (with factory):**

When a connection drops (reader/writer error), `SharedTransport`:
1. Marks the connection as dead and removes it from the active pool
2. Invokes the connection factory to establish a new connection
3. Retries with exponential backoff: 100ms → 200ms → 400ms → 800ms (5 attempts total, 4 delays between them)
4. On success, the new connection is added to the pool and future sends use it
5. On permanent failure (all retries exhausted with no live connections remaining), affected dataflows receive `TransportError::ConnectionClosed`

**No factory (pre-established connections only):**

If `SharedTransport` is created with pre-established connections and no factory, a dropped connection is permanent — no reconnection is attempted. Remaining healthy connections continue serving traffic.

**Data loss during reconnection:**

Payload frames sent while no live connection exists are **dropped immediately** — `SharedTransport` does not buffer or replay them. Additionally, frames that were assigned a sequence number before the connection failed can create an unrecoverable gap in the receiver's reorder buffer. When the gap times out, the affected dataflow receives `TransportError::ReorderTimeout`.

In summary, a successful reconnect restores connectivity but does **not** guarantee seamless delivery. Applications that require exactly-once or reliable delivery should implement their own acknowledgment/retry protocol at the operator level.

**`PeerConnection` and `TransportSession`:**

These lower-level types represent pre-established connections with no built-in reconnection. Reconnection is handled at the `SharedTransport` layer, which wraps these into a managed, pooled transport.

## In-Process Cluster Recipes

### Spawn a two-node cluster with duplex streams

Simulate a multi-node cluster in a single process using `tokio::io::duplex`:

```rust
use std::time::Duration;
use instancy::communication::ClusterSpawnTransport;
use instancy::communication::transport_session::PeerConnection;
use instancy::{
    ClusterTopology, DataflowBuilder, DataflowId, NodeConfig, Result,
    RuntimeConfig, RuntimeHandle, SpawnOptions,
};

fn make_duplex_pair(
    node_a: &str,
    node_b: &str,
    buffer_size: usize,
) -> (
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    PeerConnection<tokio::io::DuplexStream, tokio::io::DuplexStream>,
) {
    let (a_to_b, b_from_a) = tokio::io::duplex(buffer_size);
    let (b_to_a, a_from_b) = tokio::io::duplex(buffer_size);
    let conn_a = PeerConnection {
        node_id: node_b.to_string(),
        reader: a_from_b,
        writer: a_to_b,
    };
    let conn_b = PeerConnection {
        node_id: node_a.to_string(),
        reader: b_from_a,
        writer: b_to_a,
    };
    (conn_a, conn_b)
}

// Both nodes must call spawn_cluster concurrently.
let topology = ClusterTopology::multi_node(vec![
    NodeConfig::new("node-a", 1),
    NodeConfig::new("node-b", 1),
]).unwrap();
let dataflow_id = DataflowId::new();
let (conn_a, conn_b) = make_duplex_pair("node-a", "node-b", 64 * 1024);

let build = |builder: &mut DataflowBuilder<u64>| -> Result<()> {
    builder.input::<i32>("data").unwrap()
        .map("double", |_t, x| x * 2)
        .output("results").unwrap();
    Ok(())
};

// Spawn each node on a blocking task (handshake blocks the thread).
let rt_a = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let rt_b = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
let tokio_handle = tokio::runtime::Handle::current();

// ... spawn_cluster on each node with topology, connections, and a 5s timeout ...
```

### Cluster startup protocol

`spawn_cluster` follows a strict multi-phase protocol before any operators run:

1. **Build** — each node calls the `build` closure to construct its local dataflow
2. **Fingerprint** — compute a hash of the dataflow graph (operator count, edge count, exchange indices)
3. **Handshake** — exchange fingerprints with all peers; fail if any mismatch
4. **Wire channels** — create exchange channels and progress channels backed by the network transport
5. **Ready barrier** — each node sends `Ready` and waits for all peers to confirm before proceeding
6. **Materialize** — create operator tasks and begin execution

Both the handshake and ready barrier use the `handshake_timeout` parameter. If any peer doesn't respond in time, `spawn_cluster` returns an error (the `HandshakeError::Timeout` variant wrapped in the crate's `Error` type). No operators are started, and no resources are leaked.

### Collect metrics from a cluster

Enable observability on cluster dataflows with `SpawnOptions::metrics()`:

```rust
use instancy::{SpawnOptions, metrics::MetricsConfig};

let opts = SpawnOptions::new().metrics(MetricsConfig::summary_only());
let mut cluster = rt.spawn_cluster(
    "monitored", topology, "node-a", dataflow_id,
    transport, Duration::from_secs(5), build, &tokio_handle, opts,
).unwrap();

// Access metrics for a specific local worker (0-based).
if let Some(m) = cluster.worker_metrics(0) {
    println!("activations: {}", m.total_activations());
    println!("records: {}", m.total_records_processed());
}

// Or collect metrics from all local workers at once.
for (i, m) in cluster.all_worker_metrics().iter().enumerate() {
    if let Some(m) = m {
        let snaps = m.operator_snapshots();
        for snap in &snaps {
            println!("worker {i} / {}: {} records",
                snap.name, snap.records_processed);
        }
    }
}
```

### Cancel a cluster dataflow

Cancelling one node propagates to all peers via the control channel:

```rust
use instancy::cancellation::CancellationReason;

// Cancel with a reason — all peers receive PeerCancelled.
cluster_a.cancel_with_reason(CancellationReason::UserRequested);

// Or use an external cancellation token via SpawnOptions:
let token = tokio_util::sync::CancellationToken::new();
let opts = SpawnOptions::new().cancellation_token(token.clone());
// Later: token.cancel() cancels the cluster and propagates to peers.
```

## Related Examples

- [`cluster_basic.rs`](../../instancy/examples/cluster_basic.rs)
- [`cluster_exchange.rs`](../../instancy/examples/cluster_exchange.rs)
- [`cluster_shared_transport.rs`](../../instancy/examples/cluster_shared_transport.rs)

## Next Steps

- Next: [Error Handling](./error-handling.md)
- See also: [Serialization](./serialization.md), [Testing](./testing.md)
