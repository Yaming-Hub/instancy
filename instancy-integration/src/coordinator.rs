//! TestCoordinator — orchestrates multi-process integration tests.
//!
//! Starts node processes, sends control commands, collects results.
//! One reader task per node routes each response to the request waiting for
//! it and records the node's events, and each node's stdout and stderr are
//! read continuously into its log.
//!
//! A Shared-transport cluster ([`TestCoordinator::start_shared`]) sends every
//! node-to-node connection through a [`Relay`], and the coordinator acts as
//! its membership layer: it reports a killed node left and a restarted one
//! joined. A restarted node is a new process under the same ID; its outcomes
//! and log follow on from the old process's.
//! Cleans up child processes on drop (even on panic).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant};

use crate::node_actor::SPAWN_HANDSHAKE_TIMEOUT;
use crate::peer_link::prefix_node_set;
use crate::protocol::*;
use crate::relay::Relay;
use crate::workloads::Workload;

/// How long any wait for a dataflow's outcome may take before it counts as a hang.
pub const OUTCOME_DEADLINE: Duration = Duration::from_secs(30);

/// How long [`TestCoordinator::shutdown`] waits for the live nodes to answer
/// `Shutdown`, which each does once its runtime has shut down.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(20);

/// How long a node's process may take to exit after answering `Shutdown`.
pub const EXIT_DEADLINE: Duration = Duration::from_secs(10);

/// How much of each node's stdout and stderr [`TestCoordinator::node_log`] keeps.
const LOG_CAPACITY: usize = 256 * 1024;

/// How a node's run of a dataflow ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The node reported that the dataflow completed successfully.
    Ok,
    /// The node reported that the dataflow failed, with its error.
    Err(String),
    /// The node's control connection closed, or its process was killed,
    /// before it reported how the dataflow ended.
    Dead,
}

/// Nodes that gave no outcome before the deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hang {
    pub missing: Vec<String>,
}

/// Why a node's feed or input close failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputFailure {
    /// The node answered, with an error or an unexpected response.
    Refused(String),
    /// The node gave no answer within [`OUTCOME_DEADLINE`]: the command hung.
    NoResponse(String),
}

impl std::fmt::Display for InputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputFailure::Refused(message) | InputFailure::NoResponse(message) => {
                f.write_str(message)
            }
        }
    }
}

/// Where a Shared-transport dataflow's workers are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnInfo {
    /// Node ID → its number of workers.
    pub local_workers: BTreeMap<String, usize>,
    /// Node ID → the global index of its first worker.
    pub first_worker: BTreeMap<String, usize>,
    pub total_workers: usize,
}

/// Manages node processes and control connections for a test.
pub struct TestCoordinator {
    /// Every node this coordinator started, alive or not.
    started: BTreeSet<String>,
    /// Node ID → child process.
    processes: HashMap<String, Child>,
    /// Node ID → control connection, for every node not killed.
    links: HashMap<String, ControlLink>,
    /// Node ID → what has been read from the node's current process. Kept
    /// after it dies.
    nodes: BTreeMap<String, Arc<NodeState>>,
    /// Signalled whenever a node reports an event or its connection ends.
    changes: watch::Sender<()>,
    /// Next request ID for correlation.
    next_request_id: AtomicU64,
    /// Accepts the control connection of every node process, restarted ones too.
    control_listener: TcpListener,
    /// Node ID → its worker threads, to restart it with.
    worker_threads: HashMap<String, usize>,
    /// Set by [`Self::start_shared`].
    shared: Option<SharedCluster>,
}

/// A Shared-transport cluster: the relay every node-to-node connection goes
/// through, and what a node needs to (re)join it.
struct SharedCluster {
    relay: Relay,
    /// Pooled connections per pair of nodes.
    connections: usize,
    /// Node ID → the relay address it dials for each peer with a lower ID.
    dials: BTreeMap<String, HashMap<String, SocketAddr>>,
    /// Node ID → incarnation of its current process, counting from 1.
    incarnations: BTreeMap<String, u64>,
}

struct ControlLink {
    writer: BufWriter<OwnedWriteHalf>,
    /// Reads the connection until it ends.
    reader: JoinHandle<()>,
}

/// What the coordinator has read from one node process.
struct NodeState {
    node_id: String,
    /// The process this one replaced, if the node was restarted.
    previous: Option<Arc<NodeState>>,
    control: Mutex<ControlState>,
    /// The last [`LOG_CAPACITY`] bytes of the node's stdout and stderr.
    log: Mutex<VecDeque<u8>>,
}

#[derive(Default)]
struct ControlState {
    /// Request ID → where its response goes.
    pending: HashMap<u64, oneshot::Sender<NodeResponse>>,
    /// Dataflow ID → how the node reported that it ended.
    outcomes: HashMap<String, Outcome>,
    /// Dataflows this process was asked to spawn.
    spawned: HashSet<String>,
    /// [`TestCoordinator::kill_node`] killed this process while it ran.
    killed: bool,
    /// The process was sent `Shutdown`, after which it exits.
    shut_down: bool,
    /// The control connection has ended.
    closed: bool,
    /// The node sent something the protocol doesn't allow.
    protocol_error: Option<String>,
}

impl NodeState {
    fn new(node_id: &str, previous: Option<Arc<NodeState>>) -> Self {
        Self {
            node_id: node_id.to_string(),
            previous,
            control: Mutex::default(),
            log: Mutex::default(),
        }
    }

    /// The response to a request the node can no longer answer.
    fn closed_error(&self, protocol_error: Option<&str>) -> NodeResponse {
        let message = match protocol_error {
            Some(problem) => format!(
                "node {} broke the control protocol: {problem}",
                self.node_id
            ),
            None => format!("node {} closed its control connection", self.node_id),
        };
        NodeResponse::Error { message }
    }

    /// Route one message from the node: a response to its request, an event
    /// to the outcomes. Errs if the protocol doesn't allow the message.
    fn deliver(&self, envelope: Envelope) -> Result<(), String> {
        let mut control = self.control.lock().unwrap();
        match envelope.kind {
            MessageKind::Response(response) => match control.pending.remove(&envelope.request_id) {
                Some(reply) => {
                    let _ = reply.send(response);
                    Ok(())
                }
                None => Err(format!(
                    "response to unknown request {}: {response:?}",
                    envelope.request_id
                )),
            },
            MessageKind::Event(NodeEvent::DataflowCompleted {
                dataflow_id,
                success,
                error,
            }) => {
                let outcome = if success {
                    Outcome::Ok
                } else {
                    Outcome::Err(error.unwrap_or_default())
                };
                match control.outcomes.insert(dataflow_id.clone(), outcome) {
                    None => Ok(()),
                    Some(earlier) => Err(format!(
                        "reported {dataflow_id} completed twice, first {earlier:?}"
                    )),
                }
            }
            MessageKind::Command(command) => Err(format!("sent a command: {command:?}")),
        }
    }

    /// Mark the control connection ended and answer every pending request.
    fn close(&self, problem: Option<String>) {
        let mut control = self.control.lock().unwrap();
        control.closed = true;
        if let Some(problem) = problem {
            control.protocol_error.get_or_insert(problem);
        }
        let error = self.closed_error(control.protocol_error.as_deref());
        for (_, reply) in control.pending.drain() {
            let _ = reply.send(error.clone());
        }
    }

    /// How the node's run of `dataflow_id` ended, if it has. A process whose
    /// connection ended without reporting it is `Dead`. If this process was
    /// never asked to spawn it but an earlier process of the node was, the
    /// earlier process's outcome.
    fn outcome(&self, dataflow_id: &str) -> Option<Outcome> {
        let (reported, closed, spawned, problem) = {
            let control = self.control.lock().unwrap();
            (
                control.outcomes.get(dataflow_id).cloned(),
                control.closed,
                control.spawned.contains(dataflow_id),
                control.protocol_error.clone(),
            )
        };
        if let Some(problem) = problem {
            panic!(
                "node {} broke the control protocol: {problem}",
                self.node_id
            );
        }
        if reported.is_some() {
            return reported;
        }
        if !spawned {
            if let Some(previous) = self.previous.as_ref().filter(|p| p.ran(dataflow_id)) {
                return previous.outcome(dataflow_id);
            }
        }
        closed.then_some(Outcome::Dead)
    }

    /// Whether this process or an earlier one of the node was asked to spawn
    /// `dataflow_id` or reported it.
    fn ran(&self, dataflow_id: &str) -> bool {
        self.ran_here(dataflow_id) || self.previous.as_ref().is_some_and(|p| p.ran(dataflow_id))
    }

    /// Whether this process was asked to spawn `dataflow_id` or reported it.
    fn ran_here(&self, dataflow_id: &str) -> bool {
        let control = self.control.lock().unwrap();
        control.spawned.contains(dataflow_id) || control.outcomes.contains_key(dataflow_id)
    }

    /// Whether the process whose outcome [`Self::outcome`] gives for
    /// `dataflow_id` was stopped by the test: killed by
    /// [`TestCoordinator::kill_node`] or sent `Shutdown`.
    fn stopped_running(&self, dataflow_id: &str) -> bool {
        if !self.ran_here(dataflow_id) {
            if let Some(previous) = self.previous.as_ref().filter(|p| p.ran(dataflow_id)) {
                return previous.stopped_running(dataflow_id);
            }
        }
        let control = self.control.lock().unwrap();
        control.killed || control.shut_down
    }

    /// Whether this process or an earlier one of the node ended its control
    /// connection without being killed by [`TestCoordinator::kill_node`] or
    /// sent `Shutdown`.
    fn any_crashed(&self) -> bool {
        let crashed = {
            let control = self.control.lock().unwrap();
            control.closed && !control.killed && !control.shut_down
        };
        crashed || self.previous.as_ref().is_some_and(|p| p.any_crashed())
    }

    fn append_log(&self, bytes: &[u8]) {
        let mut log = self.log.lock().unwrap();
        log.extend(bytes);
        let excess = log.len().saturating_sub(LOG_CAPACITY);
        log.drain(..excess);
    }

    /// The node's log, earlier processes' first.
    fn log(&self) -> String {
        let own = {
            let log = self.log.lock().unwrap();
            let (front, back) = log.as_slices();
            String::from_utf8_lossy(&[front, back].concat()).into_owned()
        };
        match &self.previous {
            Some(previous) => format!(
                "{}\n===== {} restarted as a new process =====\n{own}",
                previous.log(),
                self.node_id
            ),
            None => own,
        }
    }
}

/// Read a node's control connection until it ends, then mark the node closed.
async fn read_control(
    node: Arc<NodeState>,
    mut reader: BufReader<OwnedReadHalf>,
    changes: watch::Sender<()>,
) {
    let problem = loop {
        match read_envelope(&mut reader).await {
            Ok(Some(envelope)) => {
                if let Err(problem) = node.deliver(envelope) {
                    break Some(problem);
                }
                changes.send_replace(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                break Some(format!("malformed control message: {e}"));
            }
            // A clean close, or a reset from a killed process.
            Ok(None) | Err(_) => break None,
        }
    };
    node.close(problem);
    changes.send_replace(());
}

/// Append everything read from a node's stdout or stderr to its log. Whole
/// lines are appended, so lines from the two pipes never interleave, however
/// many writes the node made each line with.
fn drain_log(node: Arc<NodeState>, pipe: impl AsyncRead + Unpin + Send + 'static) {
    tokio::spawn(async move {
        let mut pipe = BufReader::new(pipe);
        let mut line = Vec::new();
        loop {
            line.clear();
            // A line longer than the log is appended in parts.
            match (&mut pipe)
                .take(LOG_CAPACITY as u64)
                .read_until(b'\n', &mut line)
                .await
            {
                Ok(0) => break,
                Ok(_) => node.append_log(&line),
                Err(_) => {
                    node.append_log(&line);
                    break;
                }
            }
        }
    });
}

impl TestCoordinator {
    /// Path of the `instancy-test-node` binary.
    ///
    /// Test binaries live in `target/<profile>/deps`, and cargo builds a
    /// package's binaries into `target/<profile>` before its integration tests.
    pub fn node_binary() -> PathBuf {
        let test_exe = std::env::current_exe().expect("failed to get the test binary's path");
        let profile_dir = test_exe
            .parent()
            .and_then(|deps| deps.parent())
            .unwrap_or_else(|| panic!("no profile directory above {}", test_exe.display()));
        let binary = profile_dir.join(format!(
            "instancy-test-node{}",
            std::env::consts::EXE_SUFFIX
        ));
        assert!(
            binary.is_file(),
            "instancy-test-node not found at {}",
            binary.display()
        );
        binary
    }

    /// Start a coordinator with N node processes.
    ///
    /// Each node connects back to the coordinator's control listener.
    pub async fn start(node_ids: &[&str], worker_threads: usize) -> Self {
        let nodes: Vec<(&str, usize)> = node_ids
            .iter()
            .copied()
            .map(|node_id| (node_id, worker_threads))
            .collect();
        Self::start_asymmetric(&nodes).await
    }

    /// Start node processes with per-node worker thread counts.
    ///
    /// `nodes` is a slice of `(node_id, worker_threads)` pairs, allowing
    /// asymmetric configurations (e.g., node-a with 4 threads, node-b with 1).
    pub async fn start_asymmetric(nodes: &[(&str, usize)]) -> Self {
        // Bind the coordinator's control listener
        let control_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind coordinator listener");

        // Built before any node connects, so a failed start still kills the
        // processes and shows their logs.
        let mut coord = Self {
            started: BTreeSet::new(),
            processes: HashMap::new(),
            links: HashMap::new(),
            nodes: BTreeMap::new(),
            changes: watch::channel(()).0,
            next_request_id: AtomicU64::new(1),
            control_listener,
            worker_threads: HashMap::new(),
            shared: None,
        };

        for &(node_id, worker_threads) in nodes {
            coord.launch(node_id, worker_threads);
        }
        coord.accept_nodes(nodes.len()).await;
        coord
    }

    /// Start a node process. A node started before gets a new process, whose
    /// outcomes and log follow on from the old one's.
    fn launch(&mut self, node_id: &str, worker_threads: usize) {
        let coord_addr = self
            .control_listener
            .local_addr()
            .expect("the control listener has an address");
        let mut child = Command::new(Self::node_binary())
            .args([
                "--node-id",
                node_id,
                "--coordinator",
                &coord_addr.to_string(),
                "--worker-threads",
                &worker_threads.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start node {node_id}: {e}"));

        let previous = self.nodes.get(node_id).cloned();
        let state = Arc::new(NodeState::new(node_id, previous));
        drain_log(
            Arc::clone(&state),
            child.stdout.take().expect("stdout is piped"),
        );
        drain_log(
            Arc::clone(&state),
            child.stderr.take().expect("stderr is piped"),
        );
        self.started.insert(node_id.to_string());
        self.processes.insert(node_id.to_string(), child);
        self.nodes.insert(node_id.to_string(), state);
        self.worker_threads
            .insert(node_id.to_string(), worker_threads);
    }

    /// Accept the control connections of `count` launched nodes.
    async fn accept_nodes(&mut self, count: usize) {
        for _ in 0..count {
            let (stream, _addr) = timeout(Duration::from_secs(30), self.control_listener.accept())
                .await
                .expect("timeout waiting for node connection")
                .expect("failed to accept node connection");

            let (reader, writer) = stream.into_split();
            let mut reader = BufReader::new(reader);

            // First message from node is a handshake announcing its node_id.
            // Convention: node sends Response(Error { message: "<node_id>" }).
            let envelope = read_envelope(&mut reader)
                .await
                .expect("failed to read node announcement")
                .expect("node closed connection before announcement");

            let node_id = match envelope.kind {
                MessageKind::Response(NodeResponse::Error { message }) => message,
                other => panic!("unexpected first message from node: {other:?}"),
            };
            let state = self
                .nodes
                .get(&node_id)
                .unwrap_or_else(|| panic!("unknown node {node_id} connected"));
            assert!(
                !self.links.contains_key(&node_id),
                "node {node_id} connected twice"
            );

            let reader = tokio::spawn(read_control(
                Arc::clone(state),
                reader,
                self.changes.clone(),
            ));
            let writer = BufWriter::new(writer);
            self.links.insert(node_id, ControlLink { writer, reader });
        }
    }

    /// Nodes that were started and not killed, in order.
    pub fn live_nodes(&self) -> Vec<String> {
        let mut node_ids: Vec<String> = self.links.keys().cloned().collect();
        node_ids.sort();
        node_ids
    }
    fn node(&self, node_id: &str) -> Arc<NodeState> {
        Arc::clone(
            self.nodes
                .get(node_id)
                .unwrap_or_else(|| panic!("unknown node {node_id}")),
        )
    }

    /// Send a command to a node without waiting for the response.
    /// Returns where the response will arrive.
    async fn send_command_fire(
        &mut self,
        node_id: &str,
        cmd: NodeCommand,
    ) -> oneshot::Receiver<NodeResponse> {
        let node = self.node(node_id);
        let (reply, response) = oneshot::channel();
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut control = node.control.lock().unwrap();
            if control.closed {
                let _ = reply.send(node.closed_error(control.protocol_error.as_deref()));
                return response;
            }
            if let NodeCommand::SpawnDataflow { dataflow_id, .. }
            | NodeCommand::SpawnShared { dataflow_id, .. } = &cmd
            {
                control.spawned.insert(dataflow_id.clone());
            }
            if matches!(cmd, NodeCommand::Shutdown) {
                control.shut_down = true;
            }
            control.pending.insert(request_id, reply);
        }

        let written = match self.links.get_mut(node_id) {
            Some(link) => write_envelope(&mut link.writer, &command_envelope(request_id, cmd))
                .await
                .is_ok(),
            None => false,
        };
        if !written {
            // The connection is gone, though its reader may not have seen it end yet.
            let reply = node.control.lock().unwrap().pending.remove(&request_id);
            if let Some(reply) = reply {
                let _ = reply.send(node.closed_error(None));
            }
        }
        response
    }

    /// Send a command to a specific node and wait for its response. After
    /// `Shutdown`, closes the coordinator's end of the node's control
    /// connection, so the node, which waits for that, exits.
    pub async fn send_command(&mut self, node_id: &str, cmd: NodeCommand) -> NodeResponse {
        let shutdown = matches!(cmd, NodeCommand::Shutdown);
        let response = self.send_command_fire(node_id, cmd).await;
        let response = recv(response).await;
        if shutdown {
            self.close_control(node_id).await;
        }
        response
    }

    /// Close the coordinator's end of `node`'s control connection (FIN),
    /// once it has the node's answer to `Shutdown`. The node exits once it
    /// reads the end: exiting earlier could lose the answer.
    async fn close_control(&mut self, node: &str) {
        if let Some(link) = self.links.get_mut(node) {
            let _ = link.writer.shutdown().await;
        }
    }

    /// Start a Shared-transport cluster of `nodes`, with `worker_threads`
    /// each.
    ///
    /// Every node binds its peer listener. The relay gets a route for each
    /// pair of nodes, from the higher ID, which dials, to the lower, which
    /// accepts. Then each node learns the relay address of each peer it
    /// dials, that its process is incarnation 1, and that its pools hold
    /// `connections` connections per peer.
    pub async fn start_shared(nodes: &[&str], worker_threads: usize, connections: usize) -> Self {
        let mut coord = Self::start(nodes, worker_threads).await;
        let mut listeners = BTreeMap::new();
        for &node in nodes {
            listeners.insert(node, coord.bind_peer_listener(node).await);
        }
        let relay = Relay::start_tagged(prefix_node_set).await;
        let mut dials = BTreeMap::new();
        for &dialer in nodes {
            let mut peers = HashMap::new();
            for &acceptor in nodes.iter().filter(|&&acceptor| acceptor < dialer) {
                let addr = relay.add_route(dialer, acceptor, listeners[acceptor]).await;
                peers.insert(acceptor.to_string(), addr);
            }
            dials.insert(dialer.to_string(), peers);
        }
        coord.shared = Some(SharedCluster {
            relay,
            connections,
            dials,
            incarnations: nodes.iter().map(|&node| (node.to_string(), 1)).collect(),
        });
        for &node in nodes {
            coord.configure_peers(node).await;
        }
        coord
    }

    /// The relay of a cluster started with [`Self::start_shared`].
    pub fn relay(&self) -> &Relay {
        &self.shared_cluster().relay
    }

    fn shared_cluster(&self) -> &SharedCluster {
        self.shared
            .as_ref()
            .expect("not a Shared-transport cluster: start it with start_shared")
    }

    async fn bind_peer_listener(&mut self, node: &str) -> SocketAddr {
        match self.send_command(node, NodeCommand::BindPeerListener).await {
            NodeResponse::PeerListenerReady { addr } => addr,
            other => panic!("BindPeerListener failed on {node}: {other:?}"),
        }
    }

    /// Tell `node` the relay address of each peer it dials, the incarnation
    /// of its current process and its pool size.
    async fn configure_peers(&mut self, node: &str) {
        let shared = self.shared_cluster();
        let cmd = NodeCommand::ConfigurePeers {
            peers: shared.dials[node].clone(),
            incarnation: shared.incarnations[node],
            connections: shared.connections,
        };
        match self.send_command(node, cmd).await {
            NodeResponse::PeersConfigured => {}
            other => panic!("ConfigurePeers failed on {node}: {other:?}"),
        }
    }

    /// Restart a killed node of a Shared-transport cluster: a new process
    /// under the same ID, as the next incarnation, with a new peer listener
    /// that the relay's routes into the node now lead to. Its peers keep it
    /// as left until [`Self::report_join`].
    pub async fn restart_node(&mut self, node: &str) {
        assert!(
            self.started.contains(node),
            "restart_node: unknown node {node}"
        );
        assert!(
            !self.links.contains_key(node),
            "restart_node: {node} is still running; kill it first"
        );
        assert!(
            self.shared.is_some(),
            "restart_node: not a Shared-transport cluster: start it with start_shared"
        );
        let worker_threads = self.worker_threads[node];
        self.launch(node, worker_threads);
        self.accept_nodes(1).await;
        let addr = self.bind_peer_listener(node).await;
        let shared = self.shared.as_mut().expect("checked above");
        *shared
            .incarnations
            .get_mut(node)
            .expect("every node of the cluster has an incarnation") += 1;
        shared.relay.retarget(node, addr);
        self.configure_peers(node).await;
    }

    /// Send `SpawnShared` for `dataflow_id` to every node of `topology`, in
    /// topology order, without waiting for the responses. Returns where each
    /// node's response will arrive.
    pub async fn spawn_shared_requests(
        &mut self,
        dataflow_id: &str,
        topology: &SerializableTopology,
        dataflow_type: DataflowType,
        handshake_timeout_ms: u64,
    ) -> Vec<(String, oneshot::Receiver<NodeResponse>)> {
        let mut requests = Vec::new();
        for node in &topology.nodes {
            let cmd = NodeCommand::SpawnShared {
                dataflow_id: dataflow_id.into(),
                dataflow_type: dataflow_type.clone(),
                topology: topology.clone(),
                handshake_timeout_ms,
            };
            let response = self.send_command_fire(&node.node_id, cmd).await;
            requests.push((node.node_id.clone(), response));
        }
        requests
    }

    /// Spawn `workload`'s dataflow over the Shared transport on every node of
    /// `topology`, with the default handshake timeout.
    ///
    /// Panics unless every node spawns it within [`OUTCOME_DEADLINE`] with
    /// its workers numbered in topology order.
    pub async fn spawn_shared(
        &mut self,
        dataflow_id: &str,
        topology: &SerializableTopology,
        workload: &Workload,
    ) -> SpawnInfo {
        let requests = self
            .spawn_shared_requests(
                dataflow_id,
                topology,
                workload.dataflow_type.clone(),
                SPAWN_HANDSHAKE_TIMEOUT.as_millis() as u64,
            )
            .await;
        let deadline = Instant::now() + OUTCOME_DEADLINE;
        let mut local_workers = BTreeMap::new();
        let mut first_worker = BTreeMap::new();
        for (node, response) in requests {
            let response = timeout_at(deadline, recv(response))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "SpawnShared of {dataflow_id}: no response from {node} within {} s",
                        OUTCOME_DEADLINE.as_secs()
                    )
                });
            match response {
                NodeResponse::DataflowSpawned {
                    num_local_workers,
                    first_worker: first,
                    ..
                } => {
                    local_workers.insert(node.clone(), num_local_workers);
                    first_worker.insert(node, first);
                }
                other => panic!("SpawnShared of {dataflow_id} failed on {node}: {other:?}"),
            }
        }
        let mut total_workers = 0;
        for node in &topology.nodes {
            let id = &node.node_id;
            assert_eq!(
                (first_worker[id], local_workers[id]),
                (total_workers, node.num_workers),
                "{dataflow_id}: {id}'s (first worker, workers) don't follow the topology"
            );
            total_workers += node.num_workers;
        }
        SpawnInfo {
            local_workers,
            first_worker,
            total_workers,
        }
    }

    /// Feed `workload`'s input at `timestamps` to `dataflow_id`, a timestamp
    /// at a time: all feeds of one timestamp are sent at once, and the next
    /// timestamp's once they are all answered. Panics if a feed fails or
    /// isn't acknowledged within [`OUTCOME_DEADLINE`].
    pub async fn feed(&mut self, dataflow_id: &str, workload: &Workload, timestamps: Range<u64>) {
        let nodes: BTreeSet<&str> = workload.feeds.iter().map(|f| f.node.as_str()).collect();
        let nodes: Vec<&str> = nodes.into_iter().collect();
        let results = self
            .try_feed(dataflow_id, workload, timestamps, &nodes)
            .await;
        panic_on_failures(results);
    }

    /// [`Self::feed`], to `nodes` only, without panicking. A node whose feed
    /// fails, or isn't acknowledged within [`OUTCOME_DEADLINE`], is fed no
    /// more. Returns each of `nodes`' result: its first failure, if it had
    /// one.
    pub async fn try_feed(
        &mut self,
        dataflow_id: &str,
        workload: &Workload,
        timestamps: Range<u64>,
        nodes: &[&str],
    ) -> BTreeMap<String, Result<(), InputFailure>> {
        let mut results: BTreeMap<String, Result<(), InputFailure>> = nodes
            .iter()
            .map(|&node| (node.to_string(), Ok(())))
            .collect();
        for timestamp in timestamps {
            let mut responses = Vec::new();
            for feed in workload.feeds.iter().filter(|f| f.timestamp == timestamp) {
                if !matches!(results.get(&feed.node), Some(Ok(()))) {
                    continue;
                }
                let cmd = NodeCommand::FeedData {
                    dataflow_id: dataflow_id.into(),
                    worker_idx: feed.worker_idx,
                    port_name: feed.port.clone(),
                    timestamp,
                    data: feed.data.clone(),
                };
                responses.push((&feed.node, self.send_command_fire(&feed.node, cmd).await));
            }
            let deadline = Instant::now() + OUTCOME_DEADLINE;
            for (node, response) in responses {
                let failure = match timeout_at(deadline, recv(response)).await {
                    Ok(NodeResponse::DataFed) => continue,
                    Ok(other) => InputFailure::Refused(format!(
                        "FeedData of {dataflow_id} at time {timestamp} failed on {node}: {other:?}"
                    )),
                    Err(_) => InputFailure::NoResponse(format!(
                        "FeedData of {dataflow_id} at time {timestamp}: no response from {node} \
                         within {} s",
                        OUTCOME_DEADLINE.as_secs()
                    )),
                };
                let result = results
                    .get_mut(node)
                    .expect("only nodes in `nodes` are fed");
                if result.is_ok() {
                    *result = Err(failure);
                }
            }
        }
        results
    }

    /// Close every input of `dataflow_id` on each of `nodes`. Panics if a
    /// node fails to, or doesn't answer within [`OUTCOME_DEADLINE`].
    pub async fn close_inputs(&mut self, dataflow_id: &str, nodes: &[&str]) {
        let results = self.try_close_inputs(dataflow_id, nodes).await;
        panic_on_failures(results);
    }

    /// [`Self::close_inputs`] without panicking. Returns each of `nodes`'
    /// result.
    pub async fn try_close_inputs(
        &mut self,
        dataflow_id: &str,
        nodes: &[&str],
    ) -> BTreeMap<String, Result<(), InputFailure>> {
        let mut responses = Vec::new();
        for &node in nodes {
            let cmd = NodeCommand::CloseInputs {
                dataflow_id: dataflow_id.into(),
                worker_idx: None,
            };
            responses.push((node, self.send_command_fire(node, cmd).await));
        }
        let deadline = Instant::now() + OUTCOME_DEADLINE;
        let mut results = BTreeMap::new();
        for (node, response) in responses {
            let result = match timeout_at(deadline, recv(response)).await {
                Ok(NodeResponse::InputsClosed) => Ok(()),
                Ok(other) => Err(InputFailure::Refused(format!(
                    "CloseInputs of {dataflow_id} failed on {node}: {other:?}"
                ))),
                Err(_) => Err(InputFailure::NoResponse(format!(
                    "CloseInputs of {dataflow_id}: no response from {node} within {} s",
                    OUTCOME_DEADLINE.as_secs()
                ))),
            };
            results.insert(node.to_string(), result);
        }
        results
    }

    /// Report `node` left to every other live node, as the membership layer
    /// does once a peer is down. Returns how many dataflows each cancelled.
    pub async fn report_leave(&mut self, node: &str) -> BTreeMap<String, usize> {
        let mut responses = Vec::new();
        for member in self.live_nodes().into_iter().filter(|m| m != node) {
            let cmd = NodeCommand::ReportNodeLeave {
                node_id: node.into(),
            };
            let response = self.send_command_fire(&member, cmd).await;
            responses.push((member, response));
        }
        let mut cancelled = BTreeMap::new();
        for (member, response) in responses {
            match recv(response).await {
                NodeResponse::NodeLeaveReported { cancelled: count } => {
                    cancelled.insert(member, count);
                }
                other => panic!("ReportNodeLeave({node}) failed on {member}: {other:?}"),
            }
        }
        cancelled
    }

    /// Report `node` joined to every other live node, as the membership layer
    /// does once a peer is back. Returns whether each had it as left.
    pub async fn report_join(&mut self, node: &str) -> BTreeMap<String, bool> {
        let mut responses = Vec::new();
        for member in self.live_nodes().into_iter().filter(|m| m != node) {
            let cmd = NodeCommand::ReportNodeJoin {
                node_id: node.into(),
            };
            let response = self.send_command_fire(&member, cmd).await;
            responses.push((member, response));
        }
        let mut was_left = BTreeMap::new();
        for (member, response) in responses {
            match recv(response).await {
                NodeResponse::NodeJoinReported { was_left: left } => {
                    was_left.insert(member, left);
                }
                other => panic!("ReportNodeJoin({node}) failed on {member}: {other:?}"),
            }
        }
        was_left
    }

    /// Every Shared-transport peer manager `node` holds, with its live
    /// pooled connections (`SharedPeerManager::metrics_snapshot`), ordered
    /// by node set and peer.
    pub async fn pool_snapshots(&mut self, node: &str) -> Result<Vec<PoolSnapshot>, String> {
        match self.send_command(node, NodeCommand::SnapshotPools).await {
            NodeResponse::Pools { pools } => Ok(pools),
            other => Err(format!("SnapshotPools failed on {node}: {other:?}")),
        }
    }

    /// Whether `node`'s running process was asked to spawn `dataflow_id`, so
    /// its output can be collected there. False once the process is killed,
    /// sent `Shutdown` or gone, including when a restarted node's earlier
    /// process ran it.
    pub fn holds_output(&self, node: &str, dataflow_id: &str) -> bool {
        self.links.contains_key(node)
            && self.nodes.get(node).is_some_and(|state| {
                let control = state.control.lock().unwrap();
                !control.closed && !control.shut_down && control.spawned.contains(dataflow_id)
            })
    }

    /// Whether the process of `node` whose outcome of `dataflow_id`
    /// [`Self::wait_for_outcomes`] gives was stopped by the test: killed by
    /// [`Self::kill_node`] or sent `Shutdown`.
    pub fn stopped_by_test(&self, node: &str, dataflow_id: &str) -> bool {
        self.node(node).stopped_running(dataflow_id)
    }

    /// Nodes with a process that ended without [`Self::kill_node`] or a
    /// `Shutdown` command: it exited, or ended its control connection, on its
    /// own. Any is a failure.
    pub fn crashed_nodes(&mut self) -> Vec<String> {
        let mut crashed = BTreeSet::new();
        for (node_id, child) in self.processes.iter_mut() {
            let shut_down = self.nodes[node_id].control.lock().unwrap().shut_down;
            if !shut_down && !matches!(child.try_wait(), Ok(None)) {
                crashed.insert(node_id.clone());
            }
        }
        for (node_id, state) in &self.nodes {
            if state.any_crashed() {
                crashed.insert(node_id.clone());
            }
        }
        crashed.into_iter().collect()
    }

    /// Wait until `node`'s current process has exited, for at most
    /// `deadline`. Returns its exit status, or `None` if it is still running
    /// (or its status can't be read) at the deadline.
    pub async fn wait_for_exit(&mut self, node: &str, deadline: Duration) -> Option<ExitStatus> {
        let child = self
            .processes
            .get_mut(node)
            .unwrap_or_else(|| panic!("wait_for_exit: no process of {node}"));
        timeout(deadline, child.wait()).await.ok()?.ok()
    }

    /// Phase 1-2-3: Set up connections and spawn a dataflow across all nodes.
    pub async fn setup_and_spawn_dataflow(
        &mut self,
        dataflow_id: &str,
        topology: &SerializableTopology,
        dataflow_type: DataflowType,
    ) -> HashMap<String, usize> {
        let node_ids: Vec<String> = topology.nodes.iter().map(|n| n.node_id.clone()).collect();

        // Phase 1: BindListener on all nodes
        let mut responses = Vec::new();
        for node_id in &node_ids {
            let cmd = NodeCommand::BindListener {
                dataflow_id: dataflow_id.into(),
                topology: topology.clone(),
            };
            responses.push(self.send_command_fire(node_id, cmd).await);
        }
        let mut listen_addrs: HashMap<String, SocketAddr> = HashMap::new();
        for (node_id, response) in node_ids.iter().zip(responses) {
            match recv(response).await {
                NodeResponse::ListenerReady { listen_addr, .. } => {
                    listen_addrs.insert(node_id.clone(), listen_addr);
                }
                NodeResponse::Error { message } => {
                    panic!("BindListener failed on {node_id}: {message}");
                }
                other => panic!("unexpected BindListener response from {node_id}: {other:?}"),
            }
        }

        // Phase 2: ConnectPeers on all nodes CONCURRENTLY.
        // We must send the command to ALL nodes before waiting for any response,
        // because nodes cooperate: lower node_id accepts, higher connects.
        // If we waited sequentially, the first node would timeout waiting for
        // a connection from a node that hasn't been told to connect yet.
        let mut responses = Vec::new();
        for node_id in &node_ids {
            let peer_addrs: HashMap<String, SocketAddr> = listen_addrs
                .iter()
                .filter(|(id, _)| *id != node_id)
                .map(|(id, addr)| (id.clone(), *addr))
                .collect();
            let cmd = NodeCommand::ConnectPeers {
                dataflow_id: dataflow_id.into(),
                peer_addresses: peer_addrs,
            };
            responses.push(self.send_command_fire(node_id, cmd).await);
        }
        for (node_id, response) in node_ids.iter().zip(responses) {
            match recv(response).await {
                NodeResponse::PeersConnected { .. } => {}
                NodeResponse::Error { message } => {
                    panic!("ConnectPeers failed on {node_id}: {message}");
                }
                other => panic!("unexpected ConnectPeers response from {node_id}: {other:?}"),
            }
        }

        // Phase 3: SpawnDataflow on all nodes CONCURRENTLY.
        // spawn_cluster performs a handshake + ready barrier between peers,
        // so all nodes must be in spawn_cluster simultaneously.
        let mut responses = Vec::new();
        for node_id in &node_ids {
            let cmd = NodeCommand::SpawnDataflow {
                dataflow_id: dataflow_id.into(),
                dataflow_type: dataflow_type.clone(),
            };
            responses.push(self.send_command_fire(node_id, cmd).await);
        }
        let mut worker_counts = HashMap::new();
        for (node_id, response) in node_ids.iter().zip(responses) {
            match recv(response).await {
                NodeResponse::DataflowSpawned {
                    num_local_workers, ..
                } => {
                    worker_counts.insert(node_id.clone(), num_local_workers);
                }
                NodeResponse::Error { message } => {
                    panic!("SpawnDataflow failed on {node_id}: {message}");
                }
                other => panic!("unexpected SpawnDataflow response from {node_id}: {other:?}"),
            }
        }

        worker_counts
    }

    /// Feed data to a specific node's worker input port.
    ///
    /// `data` is bincode-serialized `Vec<T>` matching the dataflow type's input type.
    pub async fn feed_data(
        &mut self,
        node_id: &str,
        dataflow_id: &str,
        worker_idx: usize,
        port_name: &str,
        timestamp: u64,
        data: Vec<u8>,
    ) {
        let resp = self
            .send_command(
                node_id,
                NodeCommand::FeedData {
                    dataflow_id: dataflow_id.into(),
                    worker_idx,
                    port_name: port_name.into(),
                    timestamp,
                    data,
                },
            )
            .await;
        match resp {
            NodeResponse::DataFed => {}
            NodeResponse::Error { message } => {
                panic!("FeedData failed on {node_id}: {message}");
            }
            other => panic!("unexpected FeedData response from {node_id}: {other:?}"),
        }
    }

    /// Collect output from a specific node's worker output port.
    ///
    /// Returns the `(timestamp, bincode_bytes)` pairs the node has drained from
    /// the port since the last collect. The caller must deserialize the bytes
    /// according to the dataflow type's output type.
    pub async fn collect_output(
        &mut self,
        node_id: &str,
        dataflow_id: &str,
        worker_idx: usize,
        port_name: &str,
    ) -> Vec<(u64, Vec<u8>)> {
        let resp = self
            .send_command(
                node_id,
                NodeCommand::CollectOutput {
                    dataflow_id: dataflow_id.into(),
                    worker_idx,
                    port_name: port_name.into(),
                },
            )
            .await;
        match resp {
            NodeResponse::OutputData { data } => data,
            NodeResponse::Error { message } => {
                panic!("CollectOutput failed on {node_id}: {message}");
            }
            other => panic!("unexpected CollectOutput response from {node_id}: {other:?}"),
        }
    }

    /// Close inputs for all workers on every node not killed, for the given dataflow.
    pub async fn close_all_inputs(&mut self, dataflow_id: &str) {
        let node_ids = self.live_nodes();
        let mut responses = Vec::new();
        for node_id in &node_ids {
            let cmd = NodeCommand::CloseInputs {
                dataflow_id: dataflow_id.into(),
                worker_idx: None,
            };
            responses.push(self.send_command_fire(node_id, cmd).await);
        }
        for (node_id, response) in node_ids.iter().zip(responses) {
            match recv(response).await {
                NodeResponse::InputsClosed => {}
                NodeResponse::Error { message } => {
                    panic!("CloseInputs failed on {node_id}: {message}");
                }
                other => panic!("unexpected CloseInputs response from {node_id}: {other:?}"),
            }
        }
    }

    /// Wait for the dataflow to complete on every node not killed.
    ///
    /// Panics unless each of them reports success within [`OUTCOME_DEADLINE`].
    pub async fn wait_for_completion(&mut self, dataflow_id: &str) {
        for (node_id, outcome) in self.wait_for_live_outcomes(dataflow_id).await {
            match outcome {
                Outcome::Ok => {}
                Outcome::Err(error) => panic!("Dataflow failed on {node_id}: {error}"),
                Outcome::Dead => panic!("node {node_id} died before {dataflow_id} completed"),
            }
        }
    }

    /// Cancel a running dataflow on all nodes not killed.
    ///
    /// Validates that each node acknowledged the cancellation. Panics if any
    /// node returns an unexpected response (e.g., "no active dataflow").
    pub async fn cancel_dataflow(&mut self, dataflow_id: &str) {
        let node_ids = self.live_nodes();
        let mut responses = Vec::new();
        for node_id in &node_ids {
            let cmd = NodeCommand::CancelDataflow {
                dataflow_id: dataflow_id.into(),
            };
            responses.push(self.send_command_fire(node_id, cmd).await);
        }
        for (node_id, response) in node_ids.iter().zip(responses) {
            let resp = recv(response).await;
            match resp {
                NodeResponse::Error { ref message }
                    if message.to_lowercase().contains("cancel") =>
                {
                    // Expected: node_actor returns Error with "cancelled <id>"
                }
                _ => panic!("Unexpected CancelDataflow response from {node_id}: {resp:?}"),
            }
        }
    }

    /// Wait for a dataflow to complete on every node not killed, accepting
    /// cancellation as a valid outcome.
    ///
    /// Returns `true` if all nodes completed successfully, `false` if any node
    /// reported cancellation. Panics on any other error, or if a node gives no
    /// outcome within [`OUTCOME_DEADLINE`].
    pub async fn wait_for_completion_allow_cancel(&mut self, dataflow_id: &str) -> bool {
        let mut all_success = true;
        for (node_id, outcome) in self.wait_for_live_outcomes(dataflow_id).await {
            match outcome {
                Outcome::Ok => {}
                // Cancellation is expected — not a test failure
                Outcome::Err(error) if error.to_lowercase().contains("cancel") => {
                    all_success = false;
                }
                Outcome::Err(error) => {
                    panic!("Dataflow failed on {node_id} (non-cancellation): {error}")
                }
                Outcome::Dead => panic!("node {node_id} died before {dataflow_id} completed"),
            }
        }
        all_success
    }

    /// The outcomes of every node not killed, or a panic on a hang.
    async fn wait_for_live_outcomes(&mut self, dataflow_id: &str) -> BTreeMap<String, Outcome> {
        let node_ids = self.live_nodes();
        let node_ids: Vec<&str> = node_ids.iter().map(String::as_str).collect();
        self.wait_for_outcomes(dataflow_id, &node_ids, OUTCOME_DEADLINE)
            .await
            .unwrap_or_else(|hang| {
                panic!(
                    "Timed out waiting for {dataflow_id} to complete ({}s): {hang:?}",
                    OUTCOME_DEADLINE.as_secs()
                )
            })
    }

    /// Kill a specific node process (simulates node crash).
    ///
    /// Returns once the process has exited and its control connection has
    /// ended. Commands sent to the node afterwards get an error response.
    /// Its outcome of a dataflow it had not reported is then `Dead`; one it
    /// reported before the kill stays what it reported.
    pub async fn kill_node(&mut self, node_id: &str) {
        if let Some(mut child) = self.processes.remove(node_id) {
            // A process that already exited, or ended its connection, did so
            // on its own: it stays unkilled, so `crashed_nodes` reports it.
            if matches!(child.try_wait(), Ok(None)) {
                if let Some(state) = self.nodes.get(node_id) {
                    let mut control = state.control.lock().unwrap();
                    control.killed = !control.closed;
                }
            }
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        if let Some(link) = self.links.remove(node_id) {
            drop(link.writer);
            timeout(Duration::from_secs(10), link.reader)
                .await
                .unwrap_or_else(|_| {
                    panic!("node {node_id}'s control connection stayed open after it was killed")
                })
                .expect("control reader task panicked");
        }
    }

    /// Wait for each of `nodes` to report how its run of `dataflow_id` ended.
    ///
    /// Each node reports a completion as an event, which its reader task
    /// records whether or not anyone is waiting. A node whose control
    /// connection ended (it was killed, say) before it reported one is
    /// `Dead`; a node killed after it reported one gives what it reported.
    /// Returns `Err(Hang)` naming every node that gave no outcome within
    /// `deadline`.
    pub async fn wait_for_outcomes(
        &mut self,
        dataflow_id: &str,
        nodes: &[&str],
        deadline: Duration,
    ) -> Result<BTreeMap<String, Outcome>, Hang> {
        let deadline = Instant::now() + deadline;
        for &node_id in nodes {
            assert!(
                self.started.contains(node_id),
                "wait_for_outcomes: unknown node {node_id}"
            );
        }
        let mut changes = self.changes.subscribe();
        loop {
            let mut outcomes = BTreeMap::new();
            let mut missing = Vec::new();
            for &node_id in nodes {
                match self.nodes[node_id].outcome(dataflow_id) {
                    Some(outcome) => {
                        outcomes.insert(node_id.to_string(), outcome);
                    }
                    None => missing.push(node_id.to_string()),
                }
            }
            if missing.is_empty() {
                return Ok(outcomes);
            }
            if timeout_at(deadline, changes.changed()).await.is_err() {
                missing.sort();
                missing.dedup();
                return Err(Hang { missing });
            }
        }
    }

    /// The last 256 KiB of a node's stdout and stderr, as read so far.
    pub fn node_log(&self, node: &str) -> String {
        self.nodes
            .get(node)
            .unwrap_or_else(|| panic!("node_log: unknown node {node}"))
            .log()
    }

    /// Shut down every node process the test left running and check that
    /// each went cleanly: it must answer `Shutdown` within
    /// [`SHUTDOWN_DEADLINE`], and its process must then exit with success
    /// within [`EXIT_DEADLINE`]. A process the test killed, or already sent
    /// `Shutdown`, is not held to it.
    ///
    /// # Panics
    ///
    /// If a node does not shut down cleanly: a hang or a crash at exit is a
    /// failure. The panic prints every node's log (see `Drop`).
    pub async fn shutdown(mut self) {
        if let Err(problem) = self.try_shutdown().await {
            panic!("{problem}");
        }
    }

    /// [`Self::shutdown`], returning what went wrong instead of panicking.
    /// The processes still running afterwards are killed when the
    /// coordinator is dropped.
    pub async fn try_shutdown(&mut self) -> Result<(), String> {
        self.try_shutdown_within(SHUTDOWN_DEADLINE, EXIT_DEADLINE)
            .await
    }

    /// [`Self::try_shutdown`], waiting `reply_deadline` for the answers and
    /// then `exit_deadline` for the exits.
    pub async fn try_shutdown_within(
        &mut self,
        reply_deadline: Duration,
        exit_deadline: Duration,
    ) -> Result<(), String> {
        let mut requests = Vec::new();
        for node in self.live_nodes() {
            if self.nodes[&node].control.lock().unwrap().shut_down {
                continue;
            }
            let reply = self.send_command_fire(&node, NodeCommand::Shutdown).await;
            requests.push((node, reply));
        }

        let mut problems = Vec::new();
        let mut answered = Vec::new();
        let deadline = Instant::now() + reply_deadline;
        for (node, reply) in requests {
            let problem = match timeout_at(deadline, recv(reply)).await {
                Ok(NodeResponse::Error { message }) if message == "shutdown" => {
                    self.close_control(&node).await;
                    answered.push(node);
                    continue;
                }
                Ok(other) => format!("{node} gave no clean answer to Shutdown: {other:?}"),
                Err(_) => format!(
                    "{node} did not answer Shutdown within {} s: its shutdown hung",
                    reply_deadline.as_secs_f64()
                ),
            };
            let exited = match self.processes.get_mut(&node).map(Child::try_wait) {
                Some(Ok(Some(status))) => format!(" (its process exited with {status})"),
                _ => String::new(),
            };
            problems.push(format!("{problem}{exited}"));
        }

        let deadline = Instant::now() + exit_deadline;
        for node in answered {
            let child = self
                .processes
                .get_mut(&node)
                .expect("a node not killed has a process");
            match timeout_at(deadline, child.wait()).await {
                Ok(Ok(status)) if status.success() => {}
                Ok(Ok(status)) => problems.push(format!(
                    "{node} exited with {status} after answering Shutdown"
                )),
                Ok(Err(e)) => problems.push(format!("{node}'s exit status can't be read: {e}")),
                Err(_) => problems.push(format!(
                    "{node} did not exit within {} s of answering Shutdown",
                    exit_deadline.as_secs_f64()
                )),
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "the cluster did not shut down cleanly:\n  {}",
                problems.join("\n  ")
            ))
        }
    }
}

impl Drop for TestCoordinator {
    fn drop(&mut self) {
        // Kill all child processes on drop (even on panic)
        for child in self.processes.values_mut() {
            let _ = child.start_kill();
        }
        if std::thread::panicking() {
            for (node_id, node) in &self.nodes {
                eprintln!("===== log of {node_id} =====\n{}", node.log());
            }
        }
    }
}

/// Wait for a response. A node's reader task answers every pending request,
/// with an error once the connection ends, so the channel only closes if that
/// task is gone.
async fn recv(response: oneshot::Receiver<NodeResponse>) -> NodeResponse {
    response
        .await
        .expect("a control reader task ended without answering a request")
}

/// Panic with every failure among per-node `results`, if there is one.
fn panic_on_failures(results: BTreeMap<String, Result<(), InputFailure>>) {
    let failures: Vec<String> = results
        .into_values()
        .filter_map(|result| result.err().map(|failure| failure.to_string()))
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
