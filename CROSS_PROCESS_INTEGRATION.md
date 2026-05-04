# Cross-Process Integration Testing Design

## Problem

All existing instancy cluster tests run in a single process — multiple "nodes" are simulated
using `tokio::io::duplex` or real TCP connections, but everything shares the same process.
This validates the protocol layer but **does not prove** that instancy works when nodes are
truly separate OS processes with independent memory spaces, independent Tokio runtimes, and
real TCP connections routed through the OS network stack.

We need a cross-process integration test framework that:

1. Starts multiple node processes (each running an instancy runtime)
2. Coordinates dataflow setup across nodes from a test-side coordinator
3. Feeds data, collects results, and verifies correctness
4. Cleans up processes on success or failure

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Test Process (coordinator)                │
│                                                             │
│  1. Start N node processes (instancy-test-node binary)      │
│  2. Send commands via actor messages:                       │
│     - "create dataflow X with topology T"                   │
│     - "feed data to input port P at timestamp T"            │
│     - "close inputs"                                        │
│  3. Collect results from output ports                       │
│  4. Assert correctness                                      │
│  5. Shut down node processes                                │
└──────────┬──────────────────────────────┬───────────────────┘
           │  control channel (TCP)        │  control channel (TCP)
           ▼                               ▼
┌─────────────────────┐     ┌─────────────────────┐
│   Node Process A    │     │   Node Process B    │
│                     │     │                     │
│  dactor actor:      │◄───►│  dactor actor:      │
│  - DataflowAgent    │ TCP │  - DataflowAgent    │
│    (instancy        │(data│    (instancy         │
│     runtime)        │  +  │     runtime)        │
│                     │prog)│                     │
│  Manages:           │     │  Manages:           │
│  - RuntimeHandle    │     │  - RuntimeHandle    │
│  - Cluster spawning │     │  - Cluster spawning │
│  - Input feeding    │     │  - Input feeding    │
│  - Output collect   │     │  - Output collect   │
└─────────────────────┘     └─────────────────────┘
```

### Communication Layers

There are **two independent TCP connection layers**:

1. **Control channel** — coordinator ↔ node processes. Used to send commands
   ("create dataflow", "feed data", "collect results") and receive responses.
   This is the dactor actor message channel.

2. **Data channel** — node ↔ node. The instancy `PeerConnection` TCP streams
   for exchange data and progress messages. These are established by the
   node processes themselves when setting up a cluster dataflow.

## Components

### 1. Integration Crate: `instancy-integration`

A workspace member crate (not published) containing:

```
instancy-integration/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Shared types (commands, responses, dataflow definitions)
│   ├── protocol.rs         # Control protocol messages (serde)
│   ├── node_actor.rs       # DataflowAgent actor (runs in node process)
│   ├── coordinator.rs      # Test-side coordinator (starts nodes, sends commands)
│   └── dataflows.rs        # Predefined dataflow builders for tests
├── src/bin/
│   └── instancy-test-node.rs   # Node process binary
└── tests/
    └── cross_process.rs    # Integration tests
```

### 2. Control Protocol

Length-prefixed JSON messages over TCP. Each message is framed as:

```
[4 bytes: payload length (u32 big-endian)] [JSON payload]
```

Every message includes a `request_id` for correlation and a `dataflow_id` where applicable.

```rust
/// Envelope wrapping all control messages.
#[derive(Serialize, Deserialize)]
pub struct Envelope {
    pub request_id: u64,
    pub kind: MessageKind,
}

#[derive(Serialize, Deserialize)]
pub enum MessageKind {
    Command(NodeCommand),
    Response(NodeResponse),
    Event(NodeEvent),  // unsolicited: DataflowCompleted, errors
}

/// Commands sent from coordinator to node processes.
#[derive(Serialize, Deserialize)]
pub enum NodeCommand {
    /// Phase 1: Bind a TCP listener for peer connections.
    BindListener {
        dataflow_id: String,
        topology: SerializableTopology,
    },

    /// Phase 2: Connect to peers using their reported addresses.
    ConnectPeers {
        dataflow_id: String,
        peer_addresses: HashMap<String, SocketAddr>,
    },

    /// Phase 3: Build and spawn a cluster dataflow.
    SpawnDataflow {
        dataflow_id: String,
        dataflow_type: DataflowType,
    },

    /// Feed data to a specific worker's named input port at a timestamp.
    FeedData {
        dataflow_id: String,
        worker_idx: usize,
        port_name: String,
        timestamp: u64,
        data: Vec<u8>,  // serialized via bincode
    },

    /// Close all input ports for a specific worker (or all workers if None).
    CloseInputs {
        dataflow_id: String,
        worker_idx: Option<usize>,
    },

    /// Collect output from a specific worker's named output port.
    CollectOutput {
        dataflow_id: String,
        worker_idx: usize,
        port_name: String,
    },

    /// Cancel a running dataflow.
    CancelDataflow {
        dataflow_id: String,
    },

    /// Shut down the node process.
    Shutdown,
}

/// Responses from node processes to coordinator (correlated by request_id).
#[derive(Serialize, Deserialize)]
pub enum NodeResponse {
    /// Listener bound, reporting address.
    ListenerReady {
        dataflow_id: String,
        listen_addr: SocketAddr,
    },

    /// All peer connections established.
    PeersConnected {
        dataflow_id: String,
    },

    /// Dataflow spawned successfully.
    DataflowSpawned {
        dataflow_id: String,
        num_local_workers: usize,
    },

    /// Data fed successfully.
    DataFed,

    /// Inputs closed.
    InputsClosed,

    /// Output data collected.
    OutputData {
        data: Vec<(u64, Vec<u8>)>,  // (timestamp, serialized records)
    },

    /// Error response.
    Error {
        message: String,
    },
}

/// Unsolicited events from node processes.
#[derive(Serialize, Deserialize)]
pub enum NodeEvent {
    /// Dataflow completed (naturally or via cancellation).
    DataflowCompleted {
        dataflow_id: String,
        success: bool,
        error: Option<String>,
    },
}
```

### 3. DataflowAgent Actor

A dactor actor running in each node process. It manages:

- An instancy `RuntimeHandle` (created once at startup)
- Active dataflows (HashMap of dataflow_id → state)
- TCP listeners for peer connections
- Input senders and output receivers per dataflow

```rust
pub struct DataflowAgent {
    runtime: RuntimeHandle,
    tokio_handle: tokio::runtime::Handle,
    node_id: String,
    active_dataflows: HashMap<String, ActiveDataflow>,
}

struct ActiveDataflow {
    handle: ClusterSpawnedDataflow<u64>,
    input_senders: HashMap<String, InputSender<u64, Vec<u8>>>,
    output_receivers: HashMap<String, OutputReceiver<u64, Vec<u8>>>,
}
```

The actor handles each `NodeCommand` variant:

- **BindListener**: Binds a TCP listener on `127.0.0.1:0`, reports the
  ephemeral port back to the coordinator.

- **ConnectPeers**: Uses the peer addresses from the coordinator to establish
  TCP connections (lower node_id listens, higher connects). Reports `PeersConnected`
  once all peer sockets are established.

- **SpawnDataflow**: Builds the dataflow graph using a `DataflowType` enum
  that selects from predefined builders (see below). Dispatches `rt.spawn_cluster(...)`
  onto a blocking task (via `tokio::task::spawn_blocking`) to avoid blocking the
  actor mailbox — instancy's cluster spawn performs synchronous handshake/barrier.

- **FeedData**: Deserializes and sends data via the appropriate `InputSender`
  for the specified `worker_idx` and `port_name`.

- **CloseInputs**: Drops input senders for the dataflow (specific worker or all).

- **CollectOutput**: Drains output receivers on a blocking task and serializes
  results back. `OutputReceiver::collect_data()` is blocking, so it must not
  run directly in the actor handler.

### 4. Node Process Binary (`instancy-test-node`)

A minimal binary that:

1. Reads its node ID and coordinator address from command-line args
2. Connects to the coordinator's control channel
3. Spawns a `DataflowAgent` actor using `dactor-ractor`
4. Enters a message loop, forwarding `NodeCommand`s to the actor
5. Exits on `Shutdown` command

```
instancy-test-node --node-id node-a --coordinator 127.0.0.1:9000
```

### 5. Coordinator (Test Side)

A helper struct used in test functions:

```rust
pub struct TestCoordinator {
    nodes: Vec<NodeProcess>,
    control_connections: HashMap<String, TcpStream>,
}

impl TestCoordinator {
    /// Start N node processes, each connecting back to the coordinator.
    pub async fn start(node_ids: &[&str]) -> Self { ... }

    /// Orchestrate a full dataflow test:
    /// prepare connections → spawn dataflow → feed data → close → collect → verify
    pub async fn run_dataflow_test(
        &mut self,
        dataflow_type: DataflowType,
        topology: &ClusterTopology,
        input_data: HashMap<String, Vec<(u64, Vec<u8>)>>,
    ) -> HashMap<String, Vec<(u64, Vec<u8>)>> { ... }

    /// Shut down all node processes.
    pub async fn shutdown(self) { ... }
}
```

Process management:
- Uses `tokio::process::Command` to spawn child processes
- Each child inherits the same `cargo build` target for the `instancy-test-node` binary
- Coordinator listens on `127.0.0.1:0` (ephemeral port), passes its address to children
- On test failure or panic, `Drop` impl kills child processes

## Predefined Dataflows

The `DataflowType` enum selects from a set of dataflow builders that exercise
different instancy features across process boundaries:

```rust
pub enum DataflowType {
    /// Simple pipeline: source → map → output (no exchange).
    /// Verifies basic cross-process cluster setup and completion.
    PassThrough,

    /// Exchange pipeline: source → exchange_by_hash → map → output.
    /// Data is repartitioned across nodes by hash key.
    ExchangeRoundTrip,

    /// Multi-epoch exchange: source feeds data across many timestamps,
    /// exchange routes by key, unary_notify emits per-epoch aggregates.
    /// Tests frontier/progress propagation across processes over many epochs.
    MultiEpochExchange,

    /// Word count: source → flat_map (split words) → exchange (by word) →
    /// unary_notify (count per epoch) → output.
    /// Classic MapReduce pattern across nodes.
    DistributedWordCount,

    /// Iterative computation: source → iterate(filter + exchange) → output.
    /// Tests loop + exchange across process boundaries.
    IterativeFilter,

    /// Multi-input: two sources → binary join → output.
    /// Tests binary operator with cross-node exchange on both inputs.
    DistributedJoin,
}
```

## Test Scenarios

### Test 1: Basic Two-Node Pass-Through

- 2 nodes, 1 worker each
- `PassThrough` dataflow
- Feed 10 records at timestamps 0..5
- Verify all records arrive at output (some on each node)
- **Validates**: cluster setup, handshake, data channel, completion

### Test 2: Exchange Across Processes

- 2 nodes, 1 worker each
- `ExchangeRoundTrip` dataflow
- Feed records with keys that hash to different workers
- Verify each record arrives at the correct node based on hash
- **Validates**: exchange routing, data serialization/deserialization

### Test 3: Distributed Word Count

- 2 nodes, 1 worker each
- `DistributedWordCount` dataflow
- Feed sentences to one node
- Verify word counts are correct (words shuffled via exchange)
- **Validates**: exchange + unary_notify + frontier-based completion across processes

### Test 4: Iterative Computation

- 2 nodes, 1 worker each
- `IterativeFilter` dataflow
- Feed numbers, iterate to filter converged values
- Verify final output matches expected converged set
- **Validates**: iterate + Product timestamps + exchange in loop body across processes

### Test 5: Three-Node Cluster

- 3 nodes, 1 worker each
- `ExchangeRoundTrip` dataflow
- Feed data, verify correct 3-way partitioning
- **Validates**: multi-peer topology, N>2 node communication

### Test 6: Multi-Worker Per Node

- 2 nodes, 2 workers each (4 total workers)
- `DistributedWordCount` dataflow
- Verify correct results with both intra-node and cross-node exchange
- **Validates**: multi-worker + multi-node combined

### Test 7: Cancellation Across Processes

- 2 nodes, 1 worker each
- Start a long-running dataflow (feed data slowly)
- Cancel from coordinator
- Verify both nodes shut down cleanly
- **Validates**: cross-process cancellation propagation

### Test 8: Parallel Dataflows

- 2 nodes, 1 worker each
- Start 3 different dataflows simultaneously on the same cluster
- Each dataflow gets its own set of TCP connections
- Feed data to all, verify all complete independently
- **Validates**: multiple concurrent dataflows on same node pair with independent connections

### Test 9: Node Crash Mid-Run (Negative)

- 2 nodes, 1 worker each
- Start `ExchangeRoundTrip` dataflow, begin feeding data
- Kill one node process mid-stream (coordinator sends SIGKILL)
- Verify the surviving node's dataflow fails with an error (not hang)
- **Validates**: error propagation on peer disconnect across processes

## Implementation Order

1. **Shared types** (`protocol.rs`, `dataflows.rs`) — command/response enums, predefined builders
2. **Node actor** (`node_actor.rs`) — DataflowAgent with dactor
3. **Node binary** (`instancy-test-node.rs`) — minimal process entry point
4. **Coordinator** (`coordinator.rs`) — process management, command orchestration
5. **Test 1-2** — basic pass-through and exchange (validates framework works)
6. **Test 3-4** — word count and iteration (validates complex patterns)
7. **Test 5-8** — multi-node, multi-worker, cancellation, parallel

## Dependencies

```toml
[package]
name = "instancy-integration"
publish = false

[dependencies]
instancy = { path = "../instancy", features = ["transport", "bincode-codec"] }
dactor = "0.3"
dactor-ractor = "0.2"
ractor = "0.15"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bincode = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
async-trait = "0.1"

[dev-dependencies]
tokio-test = "0.4"
```

## Connection Establishment Flow

Connection setup uses explicit phases with acknowledgments to prevent races:

```
Coordinator                    Node A                    Node B
    │                            │                          │
    │  Phase 1: Bind listeners   │                          │
    ├─ BindListener ────────────►│                          │
    │   (df_id, topology)        │ bind 127.0.0.1:0        │
    ├─ BindListener ─────────────┼─────────────────────────►│
    │                            │                          │ bind 127.0.0.1:0
    │◄─ ListenerReady ───────────┤                          │
    │   (df_id, addr=:54321)     │                          │
    │◄─ ListenerReady ───────────┼──────────────────────────┤
    │   (df_id, addr=:54322)     │                          │
    │                            │                          │
    │  Phase 2: Connect peers    │                          │
    ├─ ConnectPeers ────────────►│                          │
    │   (df_id, {B→:54322})      │─── TCP connect ─────────►│
    │                            │                          │
    ├─ ConnectPeers ─────────────┼─────────────────────────►│
    │   (df_id, {A→:54321})      │◄── TCP accept ───────────│
    │                            │                          │
    │◄─ PeersConnected ─────────┤                          │
    │   (df_id)                  │                          │
    │◄─ PeersConnected ─────────┼──────────────────────────┤
    │   (df_id)                  │                          │
    │                            │                          │
    │  Phase 3: Spawn dataflow   │                          │
    │  (only after ALL nodes     │                          │
    │   report PeersConnected)   │                          │
    ├─ SpawnDataflow ───────────►│                          │
    ├─ SpawnDataflow ────────────┼─────────────────────────►│
    │                            │◄═══ handshake + data ═══►│
```

**Dial/listen rule**: Lexically lower `node_id` listens; higher connects. This matches
`ClusterTopology::multi_node()` sort order. Each node knows its role from the topology.

**One connection set per dataflow**: For parallel dataflows on the same node pair,
separate TCP connections are established per dataflow (no shared mux). This matches
existing test patterns in `parallel_cluster_tcp.rs`.

## Error Handling

- **Node process crash**: Coordinator detects broken control connection,
  fails the test with a descriptive error including stderr output from the crashed process.
- **Test timeout**: Each test has a configurable timeout (default 60s).
  On timeout, coordinator kills all node processes and fails.
- **Connection failure**: If a node can't connect to a peer, it reports an error
  to the coordinator which fails the test.
- **Coordinator Drop**: `Drop` impl sends `Shutdown` to all nodes, then `kill()`s
  any that don't exit within 5 seconds.
