//! TestCoordinator — orchestrates multi-process integration tests.
//!
//! Starts node processes, sends control commands, collects results.
//! Cleans up child processes on drop (even on panic).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::OnceCell;
use tokio::time::timeout;

use crate::protocol::*;

/// Cached path to the built `instancy-test-node` binary.
/// Built once per test process and reused across all coordinators.
static NODE_BINARY: OnceCell<PathBuf> = OnceCell::const_new();

/// Manages node processes and control connections for a test.
///
/// Timeout for waiting on node responses (prevents hung CI on wedged nodes).
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);
pub struct TestCoordinator {
    /// Node ID → child process.
    processes: HashMap<String, Child>,
    /// Node ID → (reader, writer) halves of control connection.
    connections: HashMap<String, ControlConn>,
    /// Next request ID for correlation.
    next_request_id: AtomicU64,
}

struct ControlConn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl TestCoordinator {
    /// Build the `instancy-test-node` binary (once per process) and return its path.
    pub async fn build_node_binary() -> PathBuf {
        NODE_BINARY
            .get_or_init(|| async {
                let output = Command::new("cargo")
                    .args([
                        "build",
                        "-p",
                        "instancy-integration",
                        "--bin",
                        "instancy-test-node",
                    ])
                    .env(
                        "PROTOC",
                        format!(
                            "{}/.local/protoc/bin/protoc.exe",
                            std::env::var("USERPROFILE").unwrap_or_default()
                        ),
                    )
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()
                    .await
                    .expect("failed to run cargo build");

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    panic!("Failed to build instancy-test-node:\n{stderr}");
                }

                // Find the binary in the target directory
                let target_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .join("target")
                    .join("debug");

                #[cfg(windows)]
                let binary = target_dir.join("instancy-test-node.exe");
                #[cfg(not(windows))]
                let binary = target_dir.join("instancy-test-node");

                assert!(binary.exists(), "Binary not found at {}", binary.display());
                binary
            })
            .await
            .clone()
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
        let binary_path = Self::build_node_binary().await;

        // Bind the coordinator's control listener
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind coordinator listener");
        let coord_addr = listener.local_addr().unwrap();

        let mut processes = HashMap::new();

        // Start each node process
        for &(node_id, worker_threads) in nodes {
            let child = Command::new(&binary_path)
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

            processes.insert(node_id.to_string(), child);
        }

        // Accept control connections from all nodes (with timeout)
        let mut connections = HashMap::new();
        for _ in 0..nodes.len() {
            let (stream, _addr) = tokio::time::timeout(Duration::from_secs(30), listener.accept())
                .await
                .expect("timeout waiting for node connection")
                .expect("failed to accept node connection");

            let (reader, writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let writer = BufWriter::new(writer);

            // First message from node is a handshake announcing its node_id.
            // Convention: node sends Response(Error { message: "<node_id>" }).
            let envelope = read_envelope(&mut reader)
                .await
                .expect("failed to read node announcement")
                .expect("node closed connection before announcement");

            let node_id = match &envelope.kind {
                MessageKind::Response(NodeResponse::Error { message }) => message.clone(),
                other => panic!("unexpected first message from node: {other:?}"),
            };

            connections.insert(node_id, ControlConn { reader, writer });
        }

        Self {
            processes,
            connections,
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Send a command to a node without waiting for the response.
    /// Returns the request_id for later correlation.
    async fn send_command_fire(&mut self, node_id: &str, cmd: NodeCommand) -> u64 {
        let conn = self
            .connections
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("no connection to node {node_id}"));

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let envelope = command_envelope(request_id, cmd);
        write_envelope(&mut conn.writer, &envelope)
            .await
            .unwrap_or_else(|e| panic!("failed to send to {node_id}: {e}"));
        request_id
    }

    /// Wait for a response from a specific node.
    /// If the node disconnects, captures and prints its stderr before panicking.
    async fn recv_response(&mut self, node_id: &str) -> NodeResponse {
        let conn = self
            .connections
            .get_mut(node_id)
            .unwrap_or_else(|| panic!("no connection to node {node_id}"));

        loop {
            let resp = read_envelope(&mut conn.reader).await;
            match resp {
                Ok(Some(env)) => match env.kind {
                    MessageKind::Response(r) => return r,
                    MessageKind::Event(_) => continue,
                    MessageKind::Command(_) => panic!("received command from node"),
                },
                Ok(None) => {
                    // Node disconnected — try to get stderr
                    let stderr = self.capture_stderr(node_id).await;
                    panic!("node {node_id} closed connection. stderr:\n{stderr}");
                }
                Err(e) => {
                    let stderr = self.capture_stderr(node_id).await;
                    panic!("failed to read from {node_id}: {e}. stderr:\n{stderr}");
                }
            }
        }
    }

    /// Wait for a response from a specific node without panicking on disconnects.
    async fn recv_response_tolerant(
        &mut self,
        node_id: &str,
    ) -> std::result::Result<NodeResponse, String> {
        let conn = self
            .connections
            .get_mut(node_id)
            .ok_or_else(|| format!("no connection to node {node_id}"))?;

        loop {
            match read_envelope(&mut conn.reader).await {
                Ok(Some(env)) => match env.kind {
                    MessageKind::Response(r) => return Ok(r),
                    MessageKind::Event(_) => continue,
                    MessageKind::Command(_) => {
                        return Err(format!("received command from node {node_id}"));
                    }
                },
                Ok(None) => return Err(format!("node {node_id} closed connection")),
                Err(e) => return Err(format!("failed to read from {node_id}: {e}")),
            }
        }
    }

    /// Try to capture stderr from a node process (best-effort).
    async fn capture_stderr(&mut self, node_id: &str) -> String {
        if let Some(child) = self.processes.get_mut(node_id) {
            if let Some(stderr) = child.stderr.take() {
                let mut buf = String::new();
                let mut reader = tokio::io::BufReader::new(stderr);
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio::io::AsyncReadExt::read_to_string(&mut reader, &mut buf),
                )
                .await;
                return buf;
            }
        }
        "(stderr not available)".into()
    }

    /// Send a command to a specific node and wait for its response.
    pub async fn send_command(&mut self, node_id: &str, cmd: NodeCommand) -> NodeResponse {
        self.send_command_fire(node_id, cmd).await;
        self.recv_response(node_id).await
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
        let mut listen_addrs: HashMap<String, SocketAddr> = HashMap::new();
        for node_id in &node_ids {
            let resp = self
                .send_command(
                    node_id,
                    NodeCommand::BindListener {
                        dataflow_id: dataflow_id.into(),
                        topology: topology.clone(),
                    },
                )
                .await;
            match resp {
                NodeResponse::ListenerReady { listen_addr, .. } => {
                    listen_addrs.insert(node_id.clone(), listen_addr);
                }
                NodeResponse::Error { message } => {
                    panic!("BindListener failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
            }
        }

        // Phase 2: ConnectPeers on all nodes CONCURRENTLY.
        // We must send the command to ALL nodes before waiting for any response,
        // because nodes cooperate: lower node_id accepts, higher connects.
        // If we waited sequentially, the first node would timeout waiting for
        // a connection from a node that hasn't been told to connect yet.
        for node_id in &node_ids {
            let peer_addrs: HashMap<String, SocketAddr> = listen_addrs
                .iter()
                .filter(|(id, _)| *id != node_id)
                .map(|(id, addr)| (id.clone(), *addr))
                .collect();

            self.send_command_fire(
                node_id,
                NodeCommand::ConnectPeers {
                    dataflow_id: dataflow_id.into(),
                    peer_addresses: peer_addrs,
                },
            )
            .await;
        }
        // Now collect all responses
        for node_id in &node_ids {
            let resp = self.recv_response(node_id).await;
            match resp {
                NodeResponse::PeersConnected { .. } => {}
                NodeResponse::Error { message } => {
                    panic!("ConnectPeers failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
            }
        }

        // Phase 3: SpawnDataflow on all nodes CONCURRENTLY.
        // spawn_cluster performs a handshake + ready barrier between peers,
        // so all nodes must be in spawn_cluster simultaneously.
        for node_id in &node_ids {
            self.send_command_fire(
                node_id,
                NodeCommand::SpawnDataflow {
                    dataflow_id: dataflow_id.into(),
                    dataflow_type: dataflow_type.clone(),
                },
            )
            .await;
        }
        let mut worker_counts = HashMap::new();
        for node_id in &node_ids {
            let resp = self.recv_response(node_id).await;
            match resp {
                NodeResponse::DataflowSpawned {
                    num_local_workers, ..
                } => {
                    worker_counts.insert(node_id.clone(), num_local_workers);
                }
                NodeResponse::Error { message } => {
                    panic!("SpawnDataflow failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
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
            _ => panic!("unexpected response from {node_id}"),
        }
    }

    /// Collect output from a specific node's worker output port.
    ///
    /// Returns `(timestamp, bincode_bytes)` pairs. The caller must deserialize
    /// the bytes according to the dataflow type's output type.
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
            _ => panic!("unexpected response from {node_id}"),
        }
    }

    /// Close inputs for all workers on all nodes for the given dataflow.
    pub async fn close_all_inputs(&mut self, dataflow_id: &str) {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        for node_id in &node_ids {
            let resp = self
                .send_command(
                    node_id,
                    NodeCommand::CloseInputs {
                        dataflow_id: dataflow_id.into(),
                        worker_idx: None,
                    },
                )
                .await;
            match resp {
                NodeResponse::InputsClosed => {}
                NodeResponse::Error { message } => {
                    panic!("CloseInputs failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
            }
        }
    }

    /// Wait for the dataflow to complete on all nodes.
    /// Sends WaitForCompletion concurrently (all nodes must join).
    pub async fn wait_for_completion(&mut self, dataflow_id: &str) {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        // Fire all concurrently
        for node_id in &node_ids {
            self.send_command_fire(
                node_id,
                NodeCommand::WaitForCompletion {
                    dataflow_id: dataflow_id.into(),
                },
            )
            .await;
        }
        // Collect responses (with timeout to prevent hung CI)
        for node_id in &node_ids {
            let resp = timeout(WAIT_TIMEOUT, self.recv_response(node_id))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Timed out waiting for completion on {node_id} ({}s)",
                        WAIT_TIMEOUT.as_secs()
                    )
                });
            match resp {
                NodeResponse::DataflowCompleted { success, error, .. } => {
                    if !success {
                        panic!(
                            "Dataflow failed on {node_id}: {}",
                            error.unwrap_or_default()
                        );
                    }
                }
                NodeResponse::Error { message } => {
                    panic!("WaitForCompletion failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
            }
        }
    }

    /// Cancel a running dataflow on all nodes.
    ///
    /// Validates that each node acknowledged the cancellation. Panics if any
    /// node returns an unexpected response (e.g., "no active dataflow").
    pub async fn cancel_dataflow(&mut self, dataflow_id: &str) {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        for node_id in &node_ids {
            self.send_command_fire(
                node_id,
                NodeCommand::CancelDataflow {
                    dataflow_id: dataflow_id.into(),
                },
            )
            .await;
        }
        for node_id in &node_ids {
            let resp = self.recv_response(node_id).await;
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

    /// Wait for a dataflow to complete, accepting cancellation as a valid outcome.
    ///
    /// Returns `true` if all nodes completed successfully, `false` if any node
    /// reported cancellation. Panics on unexpected errors.
    pub async fn wait_for_completion_allow_cancel(&mut self, dataflow_id: &str) -> bool {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        for node_id in &node_ids {
            self.send_command_fire(
                node_id,
                NodeCommand::WaitForCompletion {
                    dataflow_id: dataflow_id.into(),
                },
            )
            .await;
        }
        let mut all_success = true;
        for node_id in &node_ids {
            let resp = timeout(WAIT_TIMEOUT, self.recv_response(node_id))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Timed out waiting for completion on {node_id} ({}s)",
                        WAIT_TIMEOUT.as_secs()
                    )
                });
            match resp {
                NodeResponse::DataflowCompleted { success, error, .. } => {
                    if !success {
                        let err_msg = error.unwrap_or_default();
                        // Cancellation is expected — not a test failure
                        if !err_msg.to_lowercase().contains("cancel") {
                            panic!("Dataflow failed on {node_id} (non-cancellation): {err_msg}");
                        }
                        all_success = false;
                    }
                }
                NodeResponse::Error { message } => {
                    panic!("WaitForCompletion failed on {node_id}: {message}");
                }
                _ => panic!("unexpected response from {node_id}"),
            }
        }
        all_success
    }

    /// Kill a specific node process (simulates node crash).
    /// Removes the process and connection, so subsequent commands to this node will fail.
    pub async fn kill_node(&mut self, node_id: &str) {
        // Drop the connection first (flushes/closes the TCP stream)
        if let Some(conn) = self.connections.remove(node_id) {
            drop(conn);
        }
        if let Some(mut child) = self.processes.remove(node_id) {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    /// Wait for dataflow completion, tolerating errors from crashed/failed nodes.
    /// Returns Ok(true) if all alive nodes completed, Ok(false) if cancelled or failed,
    /// Err(msg) if an unexpected protocol error occurred.
    pub async fn wait_for_completion_tolerant(
        &mut self,
        dataflow_id: &str,
    ) -> std::result::Result<bool, String> {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        for node_id in &node_ids {
            self.send_command_fire(
                node_id,
                NodeCommand::WaitForCompletion {
                    dataflow_id: dataflow_id.into(),
                },
            )
            .await;
        }

        let mut all_success = true;
        for node_id in &node_ids {
            match timeout(WAIT_TIMEOUT, self.recv_response_tolerant(node_id)).await {
                Ok(Ok(NodeResponse::DataflowCompleted { success, .. })) => {
                    if !success {
                        all_success = false;
                    }
                }
                Ok(Ok(NodeResponse::Error { .. })) => {
                    all_success = false;
                }
                Ok(Ok(other)) => {
                    return Err(format!(
                        "unexpected WaitForCompletion response from {node_id}: {other:?}"
                    ));
                }
                Ok(Err(e)) => {
                    // I/O error, EOF, or missing connection — expected when a node has crashed.
                    // Only propagate as Err for genuine protocol violations (e.g., receiving a
                    // Command message instead of a Response).
                    if e.contains("closed connection")
                        || e.contains("no connection")
                        || e.contains("failed to read")
                    {
                        all_success = false;
                    } else {
                        return Err(format!("protocol error from {node_id}: {e}"));
                    }
                }
                Err(_) => {
                    // Timeout — node is likely dead or hung
                    all_success = false;
                }
            }
        }

        Ok(all_success)
    }

    /// Backward-compatible alias for tolerant completion waits that allow node errors.
    pub async fn wait_for_completion_allow_error(
        &mut self,
        dataflow_id: &str,
    ) -> std::result::Result<bool, String> {
        self.wait_for_completion_tolerant(dataflow_id).await
    }

    /// Send shutdown to all nodes and wait for processes to exit.
    pub async fn shutdown(mut self) {
        let node_ids: Vec<String> = self.connections.keys().cloned().collect();
        for node_id in &node_ids {
            if let Some(conn) = self.connections.get_mut(node_id) {
                let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
                let envelope = command_envelope(request_id, NodeCommand::Shutdown);
                let _ = write_envelope(&mut conn.writer, &envelope).await;
            }
        }
        // Wait for processes to exit
        for (_id, mut child) in self.processes.drain() {
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            let _ = child.start_kill();
        }
    }
}

impl Drop for TestCoordinator {
    fn drop(&mut self) {
        // Kill all child processes on drop (even on panic)
        for (_id, child) in &mut self.processes {
            let _ = child.start_kill();
        }
    }
}
