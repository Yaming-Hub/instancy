//! instancy-test-node — node process binary for cross-process integration tests.
//!
//! Connects to the coordinator, spawns a DataflowAgent actor, and enters
//! a command loop.
//!
//! Usage: `instancy-test-node --node-id <ID> --coordinator <ADDR> [--worker-threads N]`

use std::net::SocketAddr;

use clap::Parser;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;

use dactor_ractor::RactorRuntime;
use dactor::prelude::*;

use instancy_integration::node_actor::{DataflowAgent, DataflowAgentArgs, HandleCommand};
use instancy_integration::protocol::*;

#[derive(Parser)]
struct Args {
    /// Unique identifier for this node.
    #[arg(long)]
    node_id: String,

    /// Coordinator address (host:port).
    #[arg(long)]
    coordinator: SocketAddr,

    /// Number of instancy worker threads.
    #[arg(long, default_value = "2")]
    worker_threads: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("instancy=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // Connect to the coordinator
    let stream = TcpStream::connect(args.coordinator)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "failed to connect to coordinator at {}: {e}",
                args.coordinator
            )
        });

    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Send handshake: announce our node_id
    let handshake = response_envelope(
        0,
        NodeResponse::Error {
            message: args.node_id.clone(),
        },
    );
    write_envelope(&mut writer, &handshake)
        .await
        .expect("failed to send handshake");

    // Spawn the DataflowAgent actor
    let runtime = RactorRuntime::new();
    let actor_ref = runtime
        .spawn::<DataflowAgent>(
            &format!("dataflow-agent-{}", args.node_id),
            DataflowAgentArgs {
                node_id: args.node_id.clone(),
                worker_threads: args.worker_threads,
                tokio_handle: tokio::runtime::Handle::current(),
            },
        )
        .await
        .expect("failed to spawn DataflowAgent");

    tracing::info!(node_id = %args.node_id, "node started, entering command loop");

    // Command loop: read commands from coordinator, dispatch to actor, send response
    loop {
        let envelope = match read_envelope(&mut reader).await {
            Ok(Some(env)) => env,
            Ok(None) => {
                tracing::info!("coordinator disconnected, shutting down");
                break;
            }
            Err(e) => {
                tracing::error!("failed to read from coordinator: {e}");
                break;
            }
        };

        // Check for shutdown before dispatching
        let is_shutdown = matches!(
            &envelope.kind,
            MessageKind::Command(NodeCommand::Shutdown)
        );

        // Dispatch to actor via ask (request-response)
        let response = actor_ref
            .ask(HandleCommand { envelope }, None)
            .expect("actor ask send failed")
            .await
            .expect("actor ask reply failed");

        // Send response back to coordinator
        if let Err(e) = write_envelope(&mut writer, &response).await {
            tracing::error!("failed to send response to coordinator: {e}");
            break;
        }

        if is_shutdown {
            tracing::info!("shutdown requested, exiting");
            break;
        }
    }
}

