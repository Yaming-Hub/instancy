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
            // Every node must route a key to the same worker, so use a hash
            // that is stable across processes, platforms and Rust releases
            // (`std`'s DefaultHasher is not). `exchange` does this for you.
            .exchange_by_hash("route", |s: &String| instancy::hash::stable_hash(s))
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

With shared transport, many dataflows share a fixed pool of connections to each peer (`SharedConnectionConfig::connections`, default 1, the same on both nodes). Each dataflow's frames to a peer are pinned to one pooled connection when it registers, so they stay in order. The pool is established through your `ConnectionFactory` when the first dataflow registers, and it does not resize itself. See `tests/cluster_shared_transport.rs` for a complete example.

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

### Transport Contract

instancy does not open or own the connections between nodes: your application provides them (a `ConnectionFactory` in Shared mode, `PeerConnection` reader/writer pairs in Dedicated mode). The responsibilities are split like this.

**Your application provides:**

- **Reliable, ordered byte streams.** Any transport works (TCP, TLS, QUIC, pipes) as long as each stream delivers bytes in order, without loss or duplication, while it lives.
- **Paired connections (Shared mode).** Both nodes of a pool establish their connections independently; your factory must pair each connection one node establishes with one the peer establishes (one side dials, the other accepts and hands the accepted stream to its factory, for example). Both nodes must use the same `SharedConnectionConfig::connections`.
- **Liveness.** Detecting that a peer is dead, partitioned or unreachable is yours: make your streams fail when the peer is gone (TCP keepalive with short timers, a TLS or QUIC idle timeout), or tell instancy through `report_node_leave` (see above). instancy runs no heartbeat of its own. The bundled `TcpConnectionFactory` sets no keepalive.

**instancy guarantees:**

- **Never a wrong result.** A dataflow never reports success with lost, duplicated, reordered or corrupted data. Every frame is checksummed (CRC32 of its header and of its payload), every Shared-mode lane is sequenced, a stream that ends before its close markers fails the dataflow, and a connection to the wrong peer is rejected at the handshake.
- **Prompt, loud failure when told.** When a connection reports an error or EOF, or you report a node leaving, the affected dataflows fail (on every node) with an error you can inspect; they never hang waiting for frames that were lost.
- **No hangs of its own.** With a healthy transport, a dataflow finishes or fails.

**If your side isn't met:** a stream that silently stops delivering without ever failing (a network partition with no keepalive) makes the dataflows using it wait. They never report success; they wait until the stream fails or you report the node leaving.

### Connection Failures

Both modes are **fail-stop**, like timely: frames on a connection that fails may be lost, and nothing resends them, so a failed connection fails the dataflows that used it.

- **Dedicated:** each dataflow has its own connection to each peer; if it fails while the dataflow runs, the dataflow fails on every node.
- **Shared:** a pooled connection that fails (a write error, or its read side ending) fails exactly the dataflows pinned to it, on both nodes. Other dataflows on the pool's other connections are unaffected. With the default pool of one connection, a failure fails every dataflow running between the two nodes.

The failed connection leaves the pool. instancy does not redial in the background: the next dataflow that registers re-establishes the missing connections through your factory, bounded by `SharedConnectionConfig::connect_timeout` (and the spawn by its handshake timeout). Re-run failed dataflows from your application if you want them retried.

A dataflow whose own frame cannot be sent (one larger than the frame size limit) fails alone; the shared connection and the other dataflows are untouched.
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
2. **Fingerprint** — compute a stable hash of the dataflow graph and topology: timestamp type, node ids/order/worker counts, every operator's kind, name and ports, every edge's endpoints and channel kind, every exchange's kind, name, timestamp and record type, and the input/output ports
3. **Handshake** — exchange the cluster protocol version, node id, dataflow id and fingerprint with all peers; fail if a peer speaks another protocol version, identifies as a different node than the connection was assigned to (a miswired connection), or reports a different dataflow id or fingerprint
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
