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

use crate::protocol::*;

/// Manages node processes and control connections for a test.
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
    /// Build the `instancy-test-node` binary and return its path.
    pub async fn build_node_binary() -> PathBuf {
        let output = Command::new("cargo")
            .args(["build", "-p", "instancy-integration", "--bin", "instancy-test-node"])
            .env("PROTOC", format!(
                "{}/.local/protoc/bin/protoc.exe",
                std::env::var("USERPROFILE").unwrap_or_default()
            ))
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

        assert!(
            binary.exists(),
            "Binary not found at {}",
            binary.display()
        );
        binary
    }

    /// Start a coordinator with N node processes.
    ///
    /// Each node connects back to the coordinator's control listener.
    pub async fn start(node_ids: &[&str], worker_threads: usize) -> Self {
        let binary_path = Self::build_node_binary().await;

        // Bind the coordinator's control listener
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind coordinator listener");
        let coord_addr = listener.local_addr().unwrap();

        let mut processes = HashMap::new();

        // Start each node process
        for &node_id in node_ids {
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
        for _ in 0..node_ids.len() {
            let (stream, _addr) = tokio::time::timeout(
                Duration::from_secs(30),
                listener.accept(),
            )
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
    async fn send_command_fire(
        &mut self,
        node_id: &str,
        cmd: NodeCommand,
    ) -> u64 {
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
    pub async fn send_command(
        &mut self,
        node_id: &str,
        cmd: NodeCommand,
    ) -> NodeResponse {
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
        let node_ids: Vec<String> = topology
            .nodes
            .iter()
            .map(|n| n.node_id.clone())
            .collect();

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
                NodeResponse::ListenerReady {
                    listen_addr,
                    ..
                } => {
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
                    dataflow_type,
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
        // Collect responses
        for node_id in &node_ids {
            let resp = self.recv_response(node_id).await;
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
