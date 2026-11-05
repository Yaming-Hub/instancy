//! Tests of the fault-injecting relay, over plain loopback peers. Each test
//! pins one behaviour: what the relay forwards, and what each action does to
//! one connection.

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

use instancy_integration::peer_link::prefix_node_set;
use instancy_integration::relay::{Forwarded, LinkAction, Relay, RelayConnId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Instant};

/// How long any wait for an outcome the test expects may take.
const DEADLINE: Duration = Duration::from_secs(20);

const MIB: usize = 1024 * 1024;

/// How long a write may take before its writer counts as blocked.
const BLOCKED: Duration = Duration::from_secs(1);

/// The most a writer may send through a one-way stall before its writes
/// must block: far more than the socket buffers between it and the relay
/// hold on any platform.
const MAX_BEFORE_BLOCKING: usize = 256 * MIB;

async fn listen() -> TcpListener {
    TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback listener")
}

fn addr(listener: &TcpListener) -> SocketAddr {
    listener.local_addr().expect("listener address")
}

/// Dials `relay_addr` and accepts the relay's upstream connection on `server`.
async fn connect(relay_addr: SocketAddr, server: &TcpListener) -> (TcpStream, TcpStream) {
    let client = timeout(DEADLINE, TcpStream::connect(relay_addr))
        .await
        .expect("dialing the relay timed out")
        .expect("dial the relay");
    let (upstream, _) = timeout(DEADLINE, server.accept())
        .await
        .expect("the relay never connected to the server")
        .expect("accept the relay's connection");
    (client, upstream)
}

/// Bytes that differ from any shifted or reordered copy of themselves.
fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 32) as u8
        })
        .collect()
}

fn assert_same(actual: &[u8], expected: &[u8], what: &str) {
    if actual != expected {
        let first_difference = actual
            .iter()
            .zip(expected)
            .position(|(a, e)| a != e)
            .unwrap_or(actual.len().min(expected.len()));
        panic!(
            "{what}: got {} bytes, expected {}; first difference at byte {first_difference}",
            actual.len(),
            expected.len()
        );
    }
}

async fn read_exactly(stream: &mut TcpStream, len: usize, what: &str) -> Vec<u8> {
    let mut buf = vec![0; len];
    timeout(DEADLINE, stream.read_exact(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{what}: timed out reading {len} bytes"))
        .unwrap_or_else(|e| panic!("{what}: reading {len} bytes failed: {e}"));
    buf
}

async fn expect_eof(stream: &mut TcpStream, what: &str) {
    let mut buf = [0u8; 1];
    let read = timeout(DEADLINE, stream.read(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{what}: no end of stream within {DEADLINE:?}"));
    match read {
        Ok(0) => {}
        other => panic!("{what}: expected Ok(0), got {other:?}"),
    }
}

async fn expect_reset(stream: &mut TcpStream, what: &str) {
    let mut buf = [0u8; 1];
    let read = timeout(DEADLINE, stream.read(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{what}: no reset within {DEADLINE:?}"));
    match read {
        Err(e) if e.kind() == ErrorKind::ConnectionReset => {}
        other => panic!("{what}: expected ConnectionReset, got {other:?}"),
    }
}

/// A message each way, so both directions are shown to forward.
async fn round_trip(client: &mut TcpStream, server: &mut TcpStream, tag: &str) {
    let ping = format!("ping {tag}").into_bytes();
    let pong = format!("pong {tag}").into_bytes();
    client.write_all(&ping).await.expect("client writes");
    assert_same(
        &read_exactly(server, ping.len(), tag).await,
        &ping,
        &format!("{tag}: client to server"),
    );
    server.write_all(&pong).await.expect("server writes");
    assert_same(
        &read_exactly(client, pong.len(), tag).await,
        &pong,
        &format!("{tag}: server to client"),
    );
}

async fn write_blocks(stream: &mut TcpStream, block: &[u8], count: usize) -> std::io::Result<()> {
    for _ in 0..count {
        stream.write_all(block).await?;
    }
    Ok(())
}

fn only_connection(relay: &Relay, from: &str, to: &str) -> RelayConnId {
    let connections = relay.connections(from, to);
    assert_eq!(
        connections.len(),
        1,
        "{from}->{to} should have exactly one connection: {connections:?}"
    );
    connections[0]
}

async fn wait_until_no_connections(relay: &Relay, from: &str, to: &str) {
    let give_up = Instant::now() + DEADLINE;
    loop {
        let connections = relay.connections(from, to);
        if connections.is_empty() {
            return;
        }
        assert!(
            Instant::now() < give_up,
            "{from}->{to} still has connections {connections:?} after {DEADLINE:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn forwards_bytes_both_ways_in_order() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    only_connection(&relay, "a", "b");

    let up = pattern(MIB, 1);
    let down = pattern(MIB, 2);
    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();
    let (at_server, at_client) = timeout(DEADLINE, async {
        let (_, _, at_server, at_client) = tokio::join!(
            async {
                client_write.write_all(&up).await.expect("client writes");
                client_write.shutdown().await.expect("client shuts down");
            },
            async {
                server_write.write_all(&down).await.expect("server writes");
                server_write.shutdown().await.expect("server shuts down");
            },
            async {
                let mut got = Vec::new();
                server_read
                    .read_to_end(&mut got)
                    .await
                    .expect("server reads");
                got
            },
            async {
                let mut got = Vec::new();
                client_read
                    .read_to_end(&mut got)
                    .await
                    .expect("client reads");
                got
            },
        );
        (at_server, at_client)
    })
    .await
    .expect("1 MiB each way did not get through");
    assert_same(&at_server, &up, "client to server");
    assert_same(&at_client, &down, "server to client");

    // Both ends have sent FIN and the relay forwarded both: it lets go.
    wait_until_no_connections(&relay, "a", "b").await;
}

#[tokio::test]
async fn half_close_is_forwarded_and_the_other_direction_keeps_flowing() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;

    let last_words = pattern(1024, 3);
    client.write_all(&last_words).await.expect("client writes");
    client
        .shutdown()
        .await
        .expect("client shuts down its write half");

    assert_same(
        &read_exactly(&mut server, last_words.len(), "server").await,
        &last_words,
        "client to server before the half-close",
    );
    expect_eof(&mut server, "server after the client's half-close").await;

    let reply = pattern(64 * 1024, 4);
    server
        .write_all(&reply)
        .await
        .expect("server still writes after the client's half-close");
    assert_same(
        &read_exactly(&mut client, reply.len(), "client").await,
        &reply,
        "server to client after the half-close",
    );
    server.shutdown().await.expect("server shuts down");
    expect_eof(&mut client, "client after the server's shutdown").await;
}

#[tokio::test]
async fn reset_from_one_side_reaches_the_other_as_a_reset() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    round_trip(&mut client, &mut server, "before the reset").await;

    server.set_zero_linger().expect("set zero linger");
    drop(server);

    expect_reset(&mut client, "client after the server reset").await;
}

#[tokio::test]
async fn stall_holds_bytes_until_heal() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // The action lands on a connection that is already forwarding.
    round_trip(&mut client, &mut server, "before the action").await;

    relay.apply(conn, LinkAction::Stall);
    let up = pattern(4096, 5);
    let down = pattern(4096, 6);
    client.write_all(&up).await.expect("client writes");
    server.write_all(&down).await.expect("server writes");

    let mut server_buf = [0u8; 1];
    let mut client_buf = [0u8; 1];
    let (at_server, at_client) = tokio::join!(
        timeout(Duration::from_millis(300), server.read(&mut server_buf)),
        timeout(Duration::from_millis(300), client.read(&mut client_buf)),
    );
    assert!(
        at_server.is_err(),
        "the server read {at_server:?} through a stalled link"
    );
    assert!(
        at_client.is_err(),
        "the client read {at_client:?} through a stalled link"
    );

    relay.apply(conn, LinkAction::Heal);
    assert_same(
        &read_exactly(&mut server, up.len(), "server").await,
        &up,
        "client to server after heal",
    );
    assert_same(
        &read_exactly(&mut client, down.len(), "client").await,
        &down,
        "server to client after heal",
    );
}

/// How one side of a stalled connection ends.
#[derive(Debug, Clone, Copy)]
enum PeerEnd {
    /// It shuts down its write half (FIN).
    Fin,
    /// It closes with a reset (RST).
    Reset,
}

/// Which side of a relayed connection.
#[derive(Debug, Clone, Copy)]
enum Side {
    /// The client, which dialed the relay.
    Dialer,
    /// The server, which accepted the relay's upstream connection.
    Acceptor,
}

/// While a connection is stalled the relay does no I/O on either socket, so
/// one side ending (a FIN, or the reset of a killed process) does not reach
/// the other side, and the connection stays live, until the heal: a stall
/// can hide a node's death behind a partition. After the heal the end goes
/// through as it came.
async fn stalled_end_is_hidden_until_heal(ending_side: Side, end: PeerEnd) {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // Both directions are forwarding, so each has a read pending on its
    // source when the stall comes.
    round_trip(&mut client, &mut server, "before the stall").await;

    assert!(
        relay.apply(conn, LinkAction::Stall),
        "the connection is live"
    );
    let (mut ending, mut other) = match ending_side {
        Side::Acceptor => (server, client),
        Side::Dialer => (client, server),
    };
    let what = format!("{end:?} from the {ending_side:?} while stalled");
    let ending = match end {
        PeerEnd::Fin => {
            ending.shutdown().await.expect("shut down the write half");
            Some(ending)
        }
        PeerEnd::Reset => {
            ending.set_zero_linger().expect("set zero linger");
            drop(ending);
            None
        }
    };

    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_millis(500), other.read(&mut buf)).await;
    assert!(
        read.is_err(),
        "{what}: the other side read {read:?} through a stalled link"
    );
    assert_eq!(
        relay.connections("a", "b"),
        [conn],
        "{what}: the stalled connection must stay live"
    );

    assert!(relay.apply(conn, LinkAction::Heal), "{what}: still live");
    match end {
        PeerEnd::Fin => expect_eof(&mut other, &format!("{what}, after the heal")).await,
        PeerEnd::Reset => expect_reset(&mut other, &format!("{what}, after the heal")).await,
    }
    drop(ending);
}

#[tokio::test]
async fn stall_hides_a_reset_from_the_acceptor_until_heal() {
    stalled_end_is_hidden_until_heal(Side::Acceptor, PeerEnd::Reset).await;
}

#[tokio::test]
async fn stall_hides_a_reset_from_the_dialer_until_heal() {
    stalled_end_is_hidden_until_heal(Side::Dialer, PeerEnd::Reset).await;
}

#[tokio::test]
async fn stall_hides_a_fin_until_heal() {
    stalled_end_is_hidden_until_heal(Side::Acceptor, PeerEnd::Fin).await;
}

/// Waits until the relay has forwarded `expected` bytes each way on `conn`.
async fn wait_for_forwarded(relay: &Relay, conn: RelayConnId, expected: Forwarded, what: &str) {
    let give_up = Instant::now() + DEADLINE;
    loop {
        let forwarded = relay.forwarded(conn);
        if forwarded == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < give_up,
            "{what}: forwarded {forwarded:?}, expected {expected:?} within {DEADLINE:?}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

/// `forwarded` plus `bytes` from `side`.
fn plus(forwarded: Forwarded, side: Side, bytes: usize) -> Forwarded {
    let bytes = bytes as u64;
    match side {
        Side::Dialer => Forwarded {
            from_dialer: forwarded.from_dialer + bytes,
            ..forwarded
        },
        Side::Acceptor => Forwarded {
            from_acceptor: forwarded.from_acceptor + bytes,
            ..forwarded
        },
    }
}

/// A one-way stall from `stalled_side` stops the relay reading what that
/// side sends (TCP backpressure): its writes stop completing once the
/// socket buffers between it and the relay are full, and none of its bytes
/// reach the other side, which reads all along. The other direction keeps
/// flowing. After the heal every byte arrives, in order, then the side's FIN.
async fn one_way_stall_holds_one_direction_until_heal(stalled_side: Side) {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // Both directions are forwarding, so each has a read pending on its
    // source when the stall comes.
    round_trip(&mut client, &mut server, "before the stall").await;
    let before = relay.forwarded(conn).expect("the connection is live");

    let from_dialer = matches!(stalled_side, Side::Dialer);
    assert!(
        relay.apply(conn, LinkAction::StallOneWay { from_dialer }),
        "the connection is live"
    );
    let other_side = match stalled_side {
        Side::Dialer => Side::Acceptor,
        Side::Acceptor => Side::Dialer,
    };
    let (stalled, other) = match stalled_side {
        Side::Dialer => (client, server),
        Side::Acceptor => (server, client),
    };
    let (mut stalled_read, mut stalled_write) = stalled.into_split();
    let (mut other_read, mut other_write) = other.into_split();
    let what = format!("one-way stall from the {stalled_side:?}");

    // The other side reads all along, so only the stall can hold the bytes.
    let (received_tx, received_rx) = watch::channel(0usize);
    let reader = tokio::spawn(async move {
        let mut received = Vec::new();
        let mut buf = vec![0; 64 * 1024];
        loop {
            let n = other_read.read(&mut buf).await?;
            if n == 0 {
                return std::io::Result::Ok(received);
            }
            received.extend_from_slice(&buf[..n]);
            received_tx.send_replace(received.len());
        }
    });

    // Write until a write no longer completes: the stalled side is blocked.
    let mut sent = Vec::new();
    let mut seed = 100;
    let mut block = pattern(64 * 1024, seed);
    loop {
        let arrived = *received_rx.borrow();
        assert_eq!(
            arrived, 0,
            "{what}: the other side read {arrived} bytes through the stalled direction"
        );
        assert!(
            sent.len() <= MAX_BEFORE_BLOCKING,
            "{what}: {} bytes written and the writes never blocked",
            sent.len()
        );
        match timeout(BLOCKED, stalled_write.write(&block)).await {
            Err(_) => break,
            Ok(Ok(n)) if n > 0 => {
                sent.extend_from_slice(&block[..n]);
                block.drain(..n);
                if block.is_empty() {
                    seed += 1;
                    block = pattern(64 * 1024, seed);
                }
            }
            Ok(written) => panic!("{what}: a write failed: {written:?}"),
        }
    }
    assert!(!sent.is_empty(), "{what}: the very first write blocked");

    // The other direction flows while the stalled side is blocked.
    let reply = pattern(MIB, 99);
    let mut got = vec![0; reply.len()];
    timeout(DEADLINE, async {
        tokio::try_join!(
            other_write.write_all(&reply),
            stalled_read.read_exact(&mut got)
        )
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: the other direction did not flow"))
    .unwrap_or_else(|e| panic!("{what}: the other direction failed: {e}"));
    assert_same(&got, &reply, &format!("{what}: the other direction"));
    wait_for_forwarded(
        &relay,
        conn,
        plus(before, other_side, reply.len()),
        &format!("{what}: only the other direction forwards"),
    )
    .await;
    let arrived = *received_rx.borrow();
    assert_eq!(
        arrived, 0,
        "{what}: the other side read {arrived} bytes through the stalled direction"
    );

    assert!(relay.apply(conn, LinkAction::Heal), "{what}: still live");
    timeout(DEADLINE, stalled_write.write_all(&block))
        .await
        .unwrap_or_else(|_| panic!("{what}: the blocked write did not complete after the heal"))
        .unwrap_or_else(|e| panic!("{what}: the blocked write failed after the heal: {e}"));
    sent.extend_from_slice(&block);
    stalled_write
        .shutdown()
        .await
        .unwrap_or_else(|e| panic!("{what}: shut down the stalled side's write half: {e}"));
    let received = timeout(DEADLINE, reader)
        .await
        .unwrap_or_else(|_| panic!("{what}: no end of stream after the heal"))
        .expect("the reader task panicked")
        .unwrap_or_else(|e| panic!("{what}: reading after the heal failed: {e}"));
    assert_same(&received, &sent, &format!("{what}: after the heal"));
    wait_for_forwarded(
        &relay,
        conn,
        plus(
            plus(before, other_side, reply.len()),
            stalled_side,
            sent.len(),
        ),
        &format!("{what}: after the heal"),
    )
    .await;
}

#[tokio::test]
async fn one_way_stall_from_the_dialer_blocks_it_while_the_other_way_flows() {
    one_way_stall_holds_one_direction_until_heal(Side::Dialer).await;
}

#[tokio::test]
async fn one_way_stall_from_the_acceptor_blocks_it_while_the_other_way_flows() {
    one_way_stall_holds_one_direction_until_heal(Side::Acceptor).await;
}

/// The relay counts the bytes it forwards from each side, while the
/// connection is live.
#[tokio::test]
async fn forwarded_counts_the_bytes_written_each_way() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    let none = Forwarded {
        from_dialer: 0,
        from_acceptor: 0,
    };
    assert_eq!(relay.forwarded(conn), Some(none), "nothing sent yet");

    let up = pattern(3 * 1024, 20);
    let down = pattern(MIB, 21);
    client.write_all(&up).await.expect("client writes");
    assert_same(
        &read_exactly(&mut server, up.len(), "server").await,
        &up,
        "client to server",
    );
    server.write_all(&down).await.expect("server writes");
    assert_same(
        &read_exactly(&mut client, down.len(), "client").await,
        &down,
        "server to client",
    );
    wait_for_forwarded(
        &relay,
        conn,
        plus(
            plus(none, Side::Dialer, up.len()),
            Side::Acceptor,
            down.len(),
        ),
        "after a message each way",
    )
    .await;

    drop(client);
    drop(server);
    wait_until_no_connections(&relay, "a", "b").await;
    assert_eq!(relay.forwarded(conn), None, "the connection has ended");
}

#[tokio::test]
async fn delay_holds_each_chunk() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // The action lands on a connection that is already forwarding.
    round_trip(&mut client, &mut server, "before the action").await;

    relay.apply(conn, LinkAction::Delay(Duration::from_millis(200)));
    let ping = pattern(1024, 7);
    let pong = pattern(1024, 8);
    let start = Instant::now();
    client.write_all(&ping).await.expect("client writes");
    assert_same(
        &read_exactly(&mut server, ping.len(), "server").await,
        &ping,
        "client to server",
    );
    server.write_all(&pong).await.expect("server writes");
    assert_same(
        &read_exactly(&mut client, pong.len(), "client").await,
        &pong,
        "server to client",
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400),
        "a round trip through a 200 ms delay each way took only {elapsed:?}"
    );
}

#[tokio::test]
async fn throttle_caps_rate() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // The action lands on a connection that is already forwarding.
    round_trip(&mut client, &mut server, "before the action").await;

    relay.apply(
        conn,
        LinkAction::Throttle {
            bytes_per_sec: 256 * 1024,
        },
    );
    let up = pattern(MIB, 9);
    let down = pattern(MIB, 10);
    let start = Instant::now();
    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();
    let (_, _, (at_server, up_took), (at_client, down_took)) = tokio::join!(
        async { client_write.write_all(&up).await.expect("client writes") },
        async { server_write.write_all(&down).await.expect("server writes") },
        async {
            let mut got = vec![0; MIB];
            timeout(DEADLINE, server_read.read_exact(&mut got))
                .await
                .expect("1 MiB at 256 KiB/s did not reach the server")
                .expect("server reads");
            (got, start.elapsed())
        },
        async {
            let mut got = vec![0; MIB];
            timeout(DEADLINE, client_read.read_exact(&mut got))
                .await
                .expect("1 MiB at 256 KiB/s did not reach the client")
                .expect("client reads");
            (got, start.elapsed())
        },
    );
    assert_same(&at_server, &up, "client to server");
    assert_same(&at_client, &down, "server to client");
    assert!(
        up_took >= Duration::from_secs(3),
        "1 MiB reached the server in {up_took:?} at 256 KiB/s"
    );
    assert!(
        down_took >= Duration::from_secs(3),
        "1 MiB reached the client in {down_took:?} at 256 KiB/s"
    );
}

#[tokio::test]
async fn close_sends_fin_both_ways() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // The action lands on a connection that is already forwarding.
    round_trip(&mut client, &mut server, "before the action").await;

    relay.apply(conn, LinkAction::Close);
    expect_eof(&mut client, "client after Close").await;
    expect_eof(&mut server, "server after Close").await;

    // After its FIN the relay keeps reading and discards, so a peer that
    // still writes is never blocked by a relay that stopped reading: 64 MiB
    // is far more than the socket buffers between a peer and the relay hold.
    let block = pattern(MIB, 11);
    let (from_client, from_server) = timeout(DEADLINE, async {
        tokio::join!(
            write_blocks(&mut client, &block, 64),
            write_blocks(&mut server, &block, 64),
        )
    })
    .await
    .expect("a peer's writes blocked after Close");
    from_client.expect("the client writes after Close");
    from_server.expect("the server writes after Close");

    // Once each peer closes its end, the relay drops the connection.
    drop(client);
    drop(server);
    wait_until_no_connections(&relay, "a", "b").await;
}

#[tokio::test]
async fn reset_resets_both_sides() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    // The action lands on a connection that is already forwarding.
    round_trip(&mut client, &mut server, "before the action").await;

    relay.apply(conn, LinkAction::Reset);
    tokio::join!(
        expect_reset(&mut client, "client after Reset"),
        expect_reset(&mut server, "server after Reset"),
    );
}

#[tokio::test]
async fn retarget_routes_new_connections_to_the_new_listener() {
    let relay = Relay::start().await;
    let old_b = listen().await;
    let new_b = listen().await;
    let a = listen().await;
    let a_to_b = relay.add_route("a", "b", addr(&old_b)).await;
    let c_to_b = relay.add_route("c", "b", addr(&old_b)).await;
    let b_to_a = relay.add_route("b", "a", addr(&a)).await;

    let (mut before_client, mut before_server) = connect(a_to_b, &old_b).await;
    relay.retarget("b", addr(&new_b));

    let (mut a_client, mut a_server) = connect(a_to_b, &new_b).await;
    let (mut c_client, mut c_server) = connect(c_to_b, &new_b).await;
    let (mut b_client, mut b_server) = connect(b_to_a, &a).await;

    round_trip(&mut a_client, &mut a_server, "a->b after retarget").await;
    round_trip(&mut c_client, &mut c_server, "c->b after retarget").await;
    round_trip(&mut b_client, &mut b_server, "b->a, a route out of b").await;
    round_trip(
        &mut before_client,
        &mut before_server,
        "a->b opened before retarget",
    )
    .await;
}

#[tokio::test]
async fn apply_hits_only_the_named_connection() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut first_client, mut first_server) = connect(relay_addr, &server_listener).await;
    let (mut second_client, mut second_server) = connect(relay_addr, &server_listener).await;

    let connections = relay.connections("a", "b");
    assert_eq!(connections.len(), 2, "{connections:?}");
    // In accept order: the first is the connection dialed first.
    relay.apply(connections[0], LinkAction::Reset);
    tokio::join!(
        expect_reset(&mut first_client, "first client"),
        expect_reset(&mut first_server, "first server"),
    );

    round_trip(&mut second_client, &mut second_server, "second connection").await;
    let left = relay.connections("a", "b");
    assert_eq!(
        left,
        [connections[1]],
        "only the reset connection is gone; before the reset: {connections:?}"
    );
}

/// A dial to a route whose target refuses connections fails as a reset, the
/// nearest a relay that has already accepted can get to a refused connect.
#[tokio::test]
async fn unreachable_target_resets_the_dialer() {
    let relay = Relay::start().await;
    // Nothing can listen on port 0, so connecting to it always fails.
    let nowhere = SocketAddr::from(([127, 0, 0, 1], 0));
    let relay_addr = relay.add_route("a", "b", nowhere).await;
    let mut client = timeout(DEADLINE, TcpStream::connect(relay_addr))
        .await
        .expect("dialing the relay timed out")
        .expect("dial the relay");

    expect_reset(&mut client, "client of an unreachable target").await;
    wait_until_no_connections(&relay, "a", "b").await;
}

/// The bytes of a peer-link prefix: a `u16` length and the dialer's node ID,
/// its `u64` incarnation, then a `u16` length and the node-set key.
fn peer_link_prefix(node_id: &str, incarnation: u64, node_set: &str) -> Vec<u8> {
    let mut prefix = Vec::new();
    prefix.extend_from_slice(&(node_id.len() as u16).to_be_bytes());
    prefix.extend_from_slice(node_id.as_bytes());
    prefix.extend_from_slice(&incarnation.to_be_bytes());
    prefix.extend_from_slice(&(node_set.len() as u16).to_be_bytes());
    prefix.extend_from_slice(node_set.as_bytes());
    prefix
}

/// A tagged relay reads a connection's tag from the first bytes its dialer
/// sends (here a peer-link prefix, in two parts) and records it once all of
/// it has arrived, while every byte goes through unchanged. A dialer whose
/// first bytes are no prefix gets no tag, and its bytes go through too.
#[tokio::test]
async fn tag_is_read_from_the_dialers_first_bytes_which_pass_unchanged() {
    let relay = Relay::start_tagged(prefix_node_set).await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("c", "a", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "c", "a");
    assert_eq!(relay.connection_tag(conn), None, "nothing sent yet");

    let mut sent = peer_link_prefix("c", 2, "a,b,c");
    let cut = sent.len() - 3;
    client.write_all(&sent[..cut]).await.expect("client writes");
    assert_same(
        &read_exactly(&mut server, cut, "the first part of the prefix").await,
        &sent[..cut],
        "the first part of the prefix",
    );
    assert_eq!(
        relay.connection_tag(conn),
        None,
        "only part of the prefix has arrived"
    );

    let payload = pattern(MIB, 4);
    client.write_all(&sent[cut..]).await.expect("client writes");
    client.write_all(&payload).await.expect("client writes");
    sent.extend_from_slice(&payload);
    assert_same(
        &read_exactly(&mut server, sent.len() - cut, "the rest").await,
        &sent[cut..],
        "the rest of the prefix and what follows it",
    );
    assert_eq!(relay.connection_tag(conn).as_deref(), Some("a,b,c"));
    round_trip(&mut client, &mut server, "after the tag").await;

    let (mut other_client, mut other_server) = connect(relay_addr, &server_listener).await;
    let other = relay
        .connections("c", "a")
        .into_iter()
        .find(|&id| id != conn)
        .expect("the second connection is listed");
    let mut garbage = pattern(4096, 5);
    // A field length over the prefix's limit.
    garbage[..2].copy_from_slice(&[0xff, 0xff]);
    other_client
        .write_all(&garbage)
        .await
        .expect("client writes");
    assert_same(
        &read_exactly(&mut other_server, garbage.len(), "no prefix").await,
        &garbage,
        "bytes that are no prefix",
    );
    assert_eq!(relay.connection_tag(other), None, "no prefix, no tag");
    assert_eq!(relay.connection_tag(conn).as_deref(), Some("a,b,c"));
}

/// `apply` says whether it found the connection live.
#[tokio::test]
async fn apply_reports_whether_the_connection_was_live() {
    let relay = Relay::start().await;
    let server_listener = listen().await;
    let relay_addr = relay.add_route("a", "b", addr(&server_listener)).await;
    let (mut client, mut server) = connect(relay_addr, &server_listener).await;
    let conn = only_connection(&relay, "a", "b");
    assert!(
        relay.apply(conn, LinkAction::Delay(Duration::from_millis(1))),
        "the connection is live"
    );
    round_trip(&mut client, &mut server, "after the delay").await;

    drop(client);
    drop(server);
    wait_until_no_connections(&relay, "a", "b").await;
    assert!(
        !relay.apply(conn, LinkAction::Reset),
        "the connection has ended"
    );
}
