//! instancy-test-node — node process binary for cross-process integration tests.
//!
//! Connects to the coordinator, spawns a DataflowAgent actor, and serves
//! commands concurrently: each command runs in its own task, and responses and
//! events reach the coordinator through a single writer task.
//!
//! On `Shutdown` it answers once its runtime has shut down, closes its end of
//! the control connection, and exits once the coordinator has closed its end
//! too (or after 5 s): the coordinator then has the answer for sure.
//!
//! Usage: `instancy-test-node --node-id <ID> --coordinator <ADDR> [--worker-threads N]`

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;

use dactor::prelude::*;
use dactor_ractor::RactorRuntime;

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

/// A message for the coordinator. The writer stops after the message marked
/// `last`, with its end of the connection shut down.
struct Outgoing {
    envelope: Envelope,
    last: bool,
}

/// How long a node that has answered `Shutdown` waits for the coordinator to
/// close its end of the control connection before it exits anyway.
const CLOSE_WAIT: Duration = Duration::from_secs(5);

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
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let runtime = RactorRuntime::new();
    let actor_ref = runtime
        .spawn::<DataflowAgent>(
            &format!("dataflow-agent-{}", args.node_id),
            DataflowAgentArgs {
                node_id: args.node_id.clone(),
                worker_threads: args.worker_threads,
                tokio_handle: tokio::runtime::Handle::current(),
                events: events_tx,
            },
        )
        .await
        .expect("failed to spawn DataflowAgent");

    // The coordinator's tests look for this line; the tracing filter doesn't
    // cover this binary's own target.
    eprintln!("instancy-test-node {} ready", args.node_id);

    // Responses and events go through one channel to a single writer.
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Outgoing>();
    let writer_task = tokio::spawn(async move {
        while let Some(outgoing) = outgoing_rx.recv().await {
            if let Err(e) = write_envelope(&mut writer, &outgoing.envelope).await {
                tracing::error!("failed to write to coordinator: {e}");
                std::process::exit(1);
            }
            if outgoing.last {
                let _ = writer.shutdown().await;
                return;
            }
        }
    });
    let events_out = outgoing_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            let outgoing = Outgoing {
                envelope: event_envelope(event),
                last: false,
            };
            if events_out.send(outgoing).is_err() {
                break;
            }
        }
    });

    tracing::info!(node_id = %args.node_id, "node started, entering command loop");

    // Command loop: each command runs in its own task. Shutdown and
    // SetShutdownBehavior are handled here.
    let mut on_shutdown = ShutdownBehavior::NORMAL;
    loop {
        let envelope = match read_envelope(&mut reader).await {
            Ok(Some(env)) => env,
            Ok(None) => {
                tracing::info!("coordinator disconnected, exiting");
                std::process::exit(0);
            }
            Err(e) => {
                tracing::error!("failed to read from coordinator: {e}");
                std::process::exit(1);
            }
        };

        let request_id = envelope.request_id;
        let MessageKind::Command(command) = envelope.kind else {
            let outgoing = Outgoing {
                envelope: response_envelope(
                    request_id,
                    NodeResponse::Error {
                        message: "expected Command, got Response/Event".into(),
                    },
                ),
                last: false,
            };
            let _ = outgoing_tx.send(outgoing);
            continue;
        };

        if let NodeCommand::SetShutdownBehavior(behavior) = command {
            on_shutdown = behavior;
            let _ = outgoing_tx.send(Outgoing {
                envelope: response_envelope(request_id, NodeResponse::ShutdownBehaviorSet),
                last: false,
            });
            continue;
        }

        if matches!(command, NodeCommand::Shutdown) {
            let response = run_command(&actor_ref, command).await;
            if on_shutdown.reply {
                let _ = outgoing_tx.send(Outgoing {
                    envelope: response_envelope(request_id, response),
                    last: true,
                });
                let _ = writer_task.await;
            }
            match on_shutdown.exit_code {
                Some(code) => {
                    if on_shutdown.reply {
                        wait_for_coordinator_close(&mut reader).await;
                    }
                    tracing::info!("shut down, exiting with code {code}");
                    std::process::exit(code);
                }
                None => {
                    tracing::info!("shut down, staying up as told");
                    match std::future::pending::<Infallible>().await {}
                }
            }
        }

        let actor_ref = actor_ref.clone();
        let outgoing_tx = outgoing_tx.clone();
        tokio::spawn(async move {
            let response = run_command(&actor_ref, command).await;
            let _ = outgoing_tx.send(Outgoing {
                envelope: response_envelope(request_id, response),
                last: false,
            });
        });
    }
}

/// Read the control connection until the coordinator closes its end, which
/// it does once it has read the answer to `Shutdown`, or for at most
/// [`CLOSE_WAIT`]. Exiting at once could lose the answer: a process that
/// exits with a connection open may reset it (Windows does), and a reset
/// can overtake bytes the coordinator has not read yet.
async fn wait_for_coordinator_close(reader: &mut BufReader<OwnedReadHalf>) {
    let until_closed = async {
        let mut buf = [0u8; 1024];
        while let Ok(n) = reader.read(&mut buf).await {
            if n == 0 {
                return;
            }
        }
    };
    if timeout(CLOSE_WAIT, until_closed).await.is_err() {
        tracing::warn!(
            "the coordinator kept the control connection open {} s after the Shutdown answer",
            CLOSE_WAIT.as_secs()
        );
    }
}

/// Hand a command to the agent and wait for its response, outside the agent's mailbox.
async fn run_command<R: ActorRef<DataflowAgent>>(actor: &R, command: NodeCommand) -> NodeResponse {
    let reply = match actor.ask(HandleCommand { command }, None) {
        Ok(reply) => reply,
        Err(e) => {
            return NodeResponse::Error {
                message: format!("failed to send the command to the agent: {e}"),
            }
        }
    };
    match reply.await {
        Ok(response) => response.await.unwrap_or_else(|_| NodeResponse::Error {
            message: "the agent dropped the command without a response".into(),
        }),
        Err(e) => NodeResponse::Error {
            message: format!("the agent failed to handle the command: {e}"),
        },
    }
}
