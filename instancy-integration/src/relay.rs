//! A TCP relay that can inject faults into any single connection.
//!
//! In a shared-transport test every node-to-node connection dials the relay
//! instead of the peer. The relay listens on one port per route (an ordered
//! pair of nodes: the dialer and the acceptor), opens a matching upstream
//! connection to the acceptor's listener for each connection it accepts, and
//! copies bytes between the two. It never alters, drops or reorders them
//! except as a [`LinkAction`] says: a close or a reset drops the bytes the
//! relay has read and not yet written (see [`LinkAction::Close`]). Without
//! actions it behaves like the wire:
//!
//! - End of stream read from one side shuts down the other side's write half
//!   (FIN), after the bytes before it. The other direction keeps flowing.
//! - A read or write error on one side resets both sockets (RST), so a reset
//!   reaches the other side as a reset, never as a clean close.
//! - If the upstream connection can't be opened, the dialer is reset, the
//!   nearest the relay can get to a refused connection once it has accepted.
//!
//! A stall stops all I/O on both of a connection's sockets, so a stalled
//! connection hides even a side's end (FIN, reset, a killed process) from the
//! other side until it is healed: it can partition a node. A one-way stall
//! stops only the direction from one side: the relay stops reading that
//! side's socket, so once the socket buffers between it and the relay are
//! full its writes block (TCP backpressure), while the other direction keeps
//! flowing.
//!
//! A connection is live, and listed by [`Relay::connections`], from when the
//! relay accepts it until the relay lets go of its sockets. While it is live,
//! [`Relay::forwarded`] counts the bytes forwarded from each side.
//!
//! A relay started with [`Relay::start_tagged`] also shows the first bytes
//! each dialer sends to a [`TagReader`], as it forwards them, and records the
//! tag it reads ([`Relay::connection_tag`]). It alters, holds back and
//! reorders none of them for it.

use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tokio::time::{sleep, sleep_until, Instant};

/// The most bytes read from a socket at once.
const CHUNK_SIZE: usize = 16 * 1024;

/// How many chunks one direction holds between reading and writing them.
const QUEUED_CHUNKS: usize = 16;

/// How long the accept loop waits after an accept error before trying again.
const ACCEPT_RETRY: Duration = Duration::from_millis(10);

/// The most bytes from a dialer a [`TagReader`] is shown.
const MAX_TAG_BYTES: usize = 64 * 1024;

/// Reads a connection's tag from the first bytes its dialer sends. It is
/// given every byte read from the dialer so far, and returns `Ok(Some(tag))`
/// once they hold a whole tag, `Ok(None)` while more are needed, or `Err` if
/// they can't start one (the relay then stops looking).
pub type TagReader = fn(&[u8]) -> Result<Option<String>, String>;

/// Identifies one relayed connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelayConnId(pub u64);

/// Bytes the relay has forwarded on one connection, by the side they came
/// from: written to the other side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forwarded {
    pub from_dialer: u64,
    pub from_acceptor: u64,
}

/// A fault to inject into one relayed connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkAction {
    /// Stop all I/O both ways until [`LinkAction::Heal`]. From when
    /// [`Relay::apply`] returns, the relay reads nothing from and writes
    /// nothing to either socket: nothing is forwarded, and neither side
    /// learns that the other closed, reset or died until the heal. The
    /// connection stays live meanwhile. A stall does not hold a connection
    /// that a [`LinkAction::Close`] has ended: the relay goes on reading and
    /// discarding what its sides send, and lets go of it once they close.
    Stall,
    /// Stop forwarding the direction from one side, the dialer's
    /// (`from_dialer`) or the acceptor's, until [`LinkAction::Heal`]. From
    /// when [`Relay::apply`] returns, the relay reads nothing from that
    /// side's socket and writes nothing it had read from it: once the socket
    /// buffers between that side and the relay are full, the side's writes
    /// block (TCP backpressure), and its end (FIN, reset) is not read. The
    /// other direction keeps flowing; if the stalled side has reset, a write
    /// that direction makes to it fails, which resets both sides as usual.
    /// The connection stays live meanwhile. Adds to [`LinkAction::Stall`]
    /// and the other direction's one-way stall.
    StallOneWay { from_dialer: bool },
    /// Clear every stall, the delay and the throttle.
    Heal,
    /// Hold each chunk for this long after reading it, keeping order.
    Delay(Duration),
    /// Cap each direction's throughput at `bytes_per_sec`, which must be
    /// above zero.
    Throttle { bytes_per_sec: u64 },
    /// Shut down both write halves (FIN both ways). Bytes the relay has read
    /// and not yet written (its queue between reading and writing: a few
    /// hundred KiB at most each way) are dropped, not flushed: each side may
    /// get a stream cut short at any byte and then a clean FIN, which real
    /// TCP never does (its FIN follows every byte sent before it), though a
    /// proxy that drops its buffers as it closes can. The relay then reads
    /// and discards whatever each side still sends until that side closes
    /// its end, so no peer's writes block, and then drops the sockets.
    Close,
    /// Reset both sockets (RST both ways), dropping the bytes the relay has
    /// read and not yet written, as a reset drops what is in flight.
    Reset,
}

/// Forwards connections between nodes and injects faults into them.
///
/// Dropping the relay stops it: its listeners close and the sockets of live
/// connections are dropped.
pub struct Relay {
    shared: Arc<Shared>,
}

impl Relay {
    /// Starts a relay with no routes.
    pub async fn start() -> Relay {
        Self::with_tags(None)
    }

    /// Starts a relay with no routes that reads each connection's tag with
    /// `read_tag`.
    pub async fn start_tagged(read_tag: TagReader) -> Relay {
        Self::with_tags(Some(read_tag))
    }

    fn with_tags(read_tag: Option<TagReader>) -> Relay {
        Relay {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                read_tag,
            }),
        }
    }

    /// Adds the route from `from` to `to`, forwarding to `target`, and returns
    /// the address `from` dials to reach `to`.
    ///
    /// # Panics
    ///
    /// If the route already exists, or no loopback listener can be bound.
    pub async fn add_route(&self, from: &str, to: &str, target: SocketAddr) -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap_or_else(|e| panic!("relay route {from}->{to}: bind a listener: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("relay route {from}->{to}: listener address: {e}"));
        let key = (from.to_owned(), to.to_owned());
        let mut state = self.shared.lock();
        assert!(
            !state.routes.contains_key(&key),
            "relay route {from}->{to} already exists"
        );
        let accept = tokio::spawn(accept_loop(Arc::clone(&self.shared), key.clone(), listener));
        state.routes.insert(
            key,
            Route {
                target,
                accept: accept.abort_handle(),
            },
        );
        addr
    }

    /// Sends new connections on every route into `to` to `target`. Live
    /// connections keep the target they were opened to.
    pub fn retarget(&self, to: &str, target: SocketAddr) {
        let mut state = self.shared.lock();
        for ((_, route_to), route) in state.routes.iter_mut() {
            if route_to == to {
                route.target = target;
            }
        }
    }

    /// The live connections on the route from `from` to `to`, in accept order.
    pub fn connections(&self, from: &str, to: &str) -> Vec<RelayConnId> {
        self.shared
            .lock()
            .connections
            .iter()
            .filter(|(_, conn)| conn.from == from && conn.to == to)
            .map(|(id, _)| RelayConnId(*id))
            .collect()
    }

    /// The tag read from the first bytes `conn`'s dialer sent: `None` unless
    /// the relay was started with [`Relay::start_tagged`], `conn` is live and
    /// a whole tag has arrived.
    pub fn connection_tag(&self, conn: RelayConnId) -> Option<String> {
        self.shared.lock().connections.get(&conn.0)?.tag.clone()
    }

    /// The bytes forwarded on `conn` from each side so far: `None` unless
    /// `conn` is live.
    pub fn forwarded(&self, conn: RelayConnId) -> Option<Forwarded> {
        let forwarded = Arc::clone(&self.shared.lock().connections.get(&conn.0)?.forwarded);
        Some(Forwarded {
            from_dialer: forwarded[Source::Dialer.index()].load(Ordering::Relaxed),
            from_acceptor: forwarded[Source::Acceptor.index()].load(Ordering::Relaxed),
        })
    }

    /// Applies `action` to the connection `conn`. Returns whether `conn` was
    /// live; if it wasn't, does nothing.
    ///
    /// # Panics
    ///
    /// On `Throttle { bytes_per_sec: 0 }`.
    pub fn apply(&self, conn: RelayConnId, action: LinkAction) -> bool {
        if let LinkAction::Throttle { bytes_per_sec } = action {
            assert!(bytes_per_sec > 0, "a throttle needs a rate above zero");
        }
        let control = match self.shared.lock().connections.get(&conn.0) {
            Some(live) => Arc::clone(&live.control),
            None => return false,
        };
        control.send_modify(|control| control.apply(action));
        true
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let tasks: Vec<AbortHandle> = {
            let mut state = self.shared.lock();
            state.stopped = true;
            let accepts = state.routes.values().map(|route| route.accept.clone());
            let connections = state
                .connections
                .values()
                .filter_map(|conn| conn.task.clone());
            accepts.chain(connections).collect()
        };
        for task in tasks {
            task.abort();
        }
    }
}

struct Shared {
    state: Mutex<State>,
    read_tag: Option<TagReader>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Default)]
struct State {
    /// Keyed by (from, to).
    routes: HashMap<(String, String), Route>,
    /// Keyed by connection id, which counts up in accept order.
    connections: BTreeMap<u64, LiveConnection>,
    next_id: u64,
    /// Set when the relay is dropped; a connection accepted since is stopped.
    stopped: bool,
}

struct Route {
    target: SocketAddr,
    accept: AbortHandle,
}

struct LiveConnection {
    from: String,
    to: String,
    control: Arc<watch::Sender<Control>>,
    task: Option<AbortHandle>,
    /// Read from the dialer's first bytes, once they hold it.
    tag: Option<String>,
    forwarded: Counters,
}

/// Bytes forwarded from each side, indexed by [`Source::index`].
type Counters = Arc<[AtomicU64; 2]>;

/// The side of a connection one direction's bytes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Dialer,
    Acceptor,
}

impl Source {
    fn index(self) -> usize {
        match self {
            Source::Dialer => 0,
            Source::Acceptor => 1,
        }
    }
}

/// The actions in force on one connection.
#[derive(Debug, Clone, Copy, Default)]
struct Control {
    /// Whether the direction from each side is stalled, indexed by
    /// [`Source::index`].
    stalled: [bool; 2],
    delay: Option<Duration>,
    throttle: Option<u64>,
    end: Option<End>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    Close,
    Reset,
}

impl Control {
    fn apply(&mut self, action: LinkAction) {
        match action {
            LinkAction::Stall => self.stalled = [true; 2],
            LinkAction::StallOneWay { from_dialer } => {
                let from = if from_dialer {
                    Source::Dialer
                } else {
                    Source::Acceptor
                };
                self.stalled[from.index()] = true;
            }
            LinkAction::Heal => {
                self.stalled = [false; 2];
                self.delay = None;
                self.throttle = None;
            }
            LinkAction::Delay(delay) => self.delay = Some(delay),
            LinkAction::Throttle { bytes_per_sec } => self.throttle = Some(bytes_per_sec),
            LinkAction::Close => {
                self.end.get_or_insert(End::Close);
            }
            // A reset also cuts short a close that is still discarding.
            LinkAction::Reset => self.end = Some(End::Reset),
        }
    }

    /// Whether the direction from `from` is stalled.
    fn stalled(&self, from: Source) -> bool {
        self.stalled[from.index()]
    }
}

/// Removes a connection from the relay's list when dropped, so it leaves the
/// list however its task ends.
struct Listed {
    shared: Arc<Shared>,
    id: u64,
}

impl Drop for Listed {
    fn drop(&mut self) {
        self.shared.lock().connections.remove(&self.id);
    }
}

async fn accept_loop(shared: Arc<Shared>, route: (String, String), listener: TcpListener) {
    let (from, to) = &route;
    loop {
        let client = match listener.accept().await {
            Ok((client, _)) => client,
            Err(e) => {
                tracing::warn!("relay route {from}->{to}: accept failed: {e}");
                sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
        let (id, target, control, forwarded) = {
            let mut state = shared.lock();
            let Some(target) = state.routes.get(&route).map(|r| r.target) else {
                return;
            };
            let id = state.next_id;
            state.next_id += 1;
            let control = Arc::new(watch::Sender::new(Control::default()));
            let forwarded = Counters::default();
            state.connections.insert(
                id,
                LiveConnection {
                    from: from.clone(),
                    to: to.clone(),
                    control: Arc::clone(&control),
                    task: None,
                    tag: None,
                    forwarded: Arc::clone(&forwarded),
                },
            );
            (id, target, control, forwarded)
        };
        // Spawned without the lock held: a task spawned while the runtime
        // shuts down is dropped at once, and dropping `Listed` takes the lock.
        let listed = Listed {
            shared: Arc::clone(&shared),
            id,
        };
        let task = tokio::spawn(relay_connection(listed, client, target, control, forwarded));
        let stopped = {
            let mut state = shared.lock();
            if let Some(live) = state.connections.get_mut(&id) {
                live.task = Some(task.abort_handle());
            }
            state.stopped
        };
        if stopped {
            task.abort();
            return;
        }
    }
}

/// How forwarding in both directions stopped.
enum Stop {
    /// Both sides sent end of stream and both were forwarded.
    Finished,
    /// A read or write failed on one side.
    Broken,
    /// An action ended the connection.
    Action(End),
}

/// A read or write failed: the connection is broken.
struct Broken;

async fn relay_connection(
    listed: Listed,
    mut client: TcpStream,
    target: SocketAddr,
    control: Arc<watch::Sender<Control>>,
    forwarded: Counters,
) {
    let mut upstream = match TcpStream::connect(target).await {
        Ok(upstream) => upstream,
        Err(e) => {
            tracing::debug!("relay connection {}: connect to {target}: {e}", listed.id);
            drop(listed);
            reset(client);
            return;
        }
    };
    // Forward each chunk as soon as it is read, as the wire would.
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);

    let mut ctl = control.subscribe();
    let tagging = listed.shared.read_tag.map(|read_tag| Tagging {
        read_tag,
        shared: Arc::clone(&listed.shared),
        id: listed.id,
        seen: Vec::new(),
    });
    let stop = {
        let (client_read, client_write) = client.split();
        let (upstream_read, upstream_write) = upstream.split();
        let both_ways = async {
            let from_dialer = Direction {
                source: Source::Dialer,
                forwarded: Arc::clone(&forwarded),
            };
            let from_acceptor = Direction {
                source: Source::Acceptor,
                forwarded,
            };
            tokio::try_join!(
                one_way(
                    client_read,
                    upstream_write,
                    from_dialer,
                    control.subscribe(),
                    tagging
                ),
                one_way(
                    upstream_read,
                    client_write,
                    from_acceptor,
                    control.subscribe(),
                    None
                ),
            )
        };
        tokio::select! {
            forwarded = both_ways => match forwarded {
                Ok(_) => Stop::Finished,
                Err(Broken) => Stop::Broken,
            },
            end = ended(&mut ctl) => Stop::Action(end),
        }
    };

    match stop {
        // Both FINs went through: nothing is left to send or read.
        Stop::Finished => drop(listed),
        Stop::Broken | Stop::Action(End::Reset) => {
            drop(listed);
            reset(client);
            reset(upstream);
        }
        Stop::Action(End::Close) => {
            let _ = client.shutdown().await;
            let _ = upstream.shutdown().await;
            let reset_requested = tokio::select! {
                _ = async { tokio::join!(discard(&mut client), discard(&mut upstream)) } => false,
                () = until(&mut ctl, |c| c.end == Some(End::Reset)) => true,
            };
            drop(listed);
            if reset_requested {
                reset(client);
                reset(upstream);
            }
        }
    }
}

/// One direction of a connection: the side its bytes come from, and the
/// connection's counts of forwarded bytes.
struct Direction {
    source: Source,
    forwarded: Counters,
}

impl Direction {
    fn stalled(&self, control: &Control) -> bool {
        control.stalled(self.source)
    }

    fn count(&self, bytes: usize) {
        self.forwarded[self.source.index()].fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

/// Forwards one direction until end of stream has been forwarded, or until a
/// read or write fails. A reader and a writer run concurrently, joined by a
/// short queue, so a delay holds each chunk without holding the ones behind it.
/// `tagging`, if any, is shown what is read.
async fn one_way(
    from: ReadHalf<'_>,
    to: WriteHalf<'_>,
    direction: Direction,
    ctl: watch::Receiver<Control>,
    tagging: Option<Tagging>,
) -> Result<(), Broken> {
    let (chunks_tx, chunks_rx) = mpsc::channel(QUEUED_CHUNKS);
    tokio::try_join!(
        read_chunks(from, chunks_tx, &direction, ctl.clone(), tagging),
        write_chunks(to, chunks_rx, &direction, ctl),
    )?;
    Ok(())
}

/// Reads a connection's tag from what its dialer sends, and records it in
/// the connection's listing.
struct Tagging {
    read_tag: TagReader,
    shared: Arc<Shared>,
    id: u64,
    /// Everything read from the dialer so far, up to [`MAX_TAG_BYTES`].
    seen: Vec<u8>,
}

impl Tagging {
    /// Shows the reader `bytes`, the next ones read from the dialer (none at
    /// end of stream). Returns whether to keep looking.
    fn scan(&mut self, bytes: &[u8]) -> bool {
        let room = MAX_TAG_BYTES - self.seen.len();
        self.seen.extend_from_slice(&bytes[..bytes.len().min(room)]);
        match (self.read_tag)(&self.seen) {
            Ok(Some(tag)) => {
                if let Some(live) = self.shared.lock().connections.get_mut(&self.id) {
                    live.tag = Some(tag);
                }
                false
            }
            Ok(None) => !bytes.is_empty() && self.seen.len() < MAX_TAG_BYTES,
            Err(problem) => {
                tracing::debug!("relay connection {}: no tag: {problem}", self.id);
                false
            }
        }
    }
}

/// Bytes read from one side, or end of stream when `bytes` is empty.
struct Chunk {
    read_at: Instant,
    bytes: Vec<u8>,
}

async fn read_chunks(
    mut from: ReadHalf<'_>,
    chunks: mpsc::Sender<Chunk>,
    direction: &Direction,
    mut ctl: watch::Receiver<Control>,
    mut tagging: Option<Tagging>,
) -> Result<(), Broken> {
    let mut buf = vec![0; CHUNK_SIZE];
    loop {
        until(&mut ctl, |c| !direction.stalled(c)).await;
        // A stall drops a pending read, so nothing (bytes, end of stream or
        // a reset) is read while this direction is stalled. The stall is
        // checked first: a read polled after `apply` returns never runs.
        let n = tokio::select! {
            biased;
            () = until(&mut ctl, |c| direction.stalled(c)) => continue,
            read = from.read(&mut buf) => read.map_err(|_| Broken)?,
        };
        // Before the bytes are forwarded, so a tag is recorded by the time
        // the other side has what carries it.
        if tagging.as_mut().is_some_and(|t| !t.scan(&buf[..n])) {
            tagging = None;
        }
        let chunk = Chunk {
            read_at: Instant::now(),
            bytes: buf[..n].to_vec(),
        };
        // The writer only goes away after it failed, which ends this direction.
        if chunks.send(chunk).await.is_err() || n == 0 {
            return Ok(());
        }
    }
}

async fn write_chunks(
    mut to: WriteHalf<'_>,
    mut chunks: mpsc::Receiver<Chunk>,
    direction: &Direction,
    mut ctl: watch::Receiver<Control>,
) -> Result<(), Broken> {
    let mut pace = None;
    while let Some(chunk) = chunks.recv().await {
        if chunk.bytes.is_empty() {
            hold(&mut ctl, direction, chunk.read_at, &mut pace, 0).await;
            // As for a write below.
            loop {
                until(&mut ctl, |c| !direction.stalled(c)).await;
                tokio::select! {
                    biased;
                    () = until(&mut ctl, |c| direction.stalled(c)) => {}
                    shut = to.shutdown() => {
                        shut.map_err(|_| Broken)?;
                        return Ok(());
                    }
                }
            }
        }
        let mut rest = chunk.bytes.as_slice();
        while !rest.is_empty() {
            let n = hold(&mut ctl, direction, chunk.read_at, &mut pace, rest.len()).await;
            let mut paced = &rest[..n];
            while !paced.is_empty() {
                until(&mut ctl, |c| !direction.stalled(c)).await;
                // A stall drops a pending write, which resumes after the
                // heal, so nothing is written (and no write fails) while
                // this direction is stalled. The stall is checked first: a
                // write polled after `apply` returns never runs.
                tokio::select! {
                    biased;
                    () = until(&mut ctl, |c| direction.stalled(c)) => {}
                    written = to.write(paced) => match written {
                        Ok(0) | Err(_) => return Err(Broken),
                        Ok(written) => {
                            direction.count(written);
                            paced = &paced[written..];
                        }
                    },
                }
            }
            rest = &rest[n..];
        }
    }
    Ok(())
}

/// Waits until up to `want` bytes of a chunk read at `read_at` may be written
/// in `direction`, and returns how many may go now: all of them unless
/// throttled. `want` 0 is end of stream, which waits out a stall and the
/// delay but not the throttle.
///
/// `pace` is the throttle's clock: the rate it runs at, and when the next
/// bytes may go. It restarts whenever the rate changes.
async fn hold(
    ctl: &mut watch::Receiver<Control>,
    direction: &Direction,
    read_at: Instant,
    pace: &mut Option<(u64, Instant)>,
    want: usize,
) -> usize {
    loop {
        let control = *ctl.borrow_and_update();
        let now = Instant::now();
        if direction.stalled(&control) {
            changed(ctl).await;
            continue;
        }
        let mut due = control.delay.map(|delay| read_at + delay);
        let rate = control.throttle.filter(|_| want > 0);
        match rate {
            Some(rate) => {
                let next = match *pace {
                    Some((paced, next)) if paced == rate => next,
                    _ => now,
                };
                *pace = Some((rate, next));
                due = due.max(Some(next));
            }
            None => *pace = None,
        }
        match due {
            Some(due) if due > now => {
                tokio::select! {
                    () = sleep_until(due) => {}
                    () = changed(ctl) => {}
                }
            }
            _ => {
                let Some(rate) = rate else {
                    return want;
                };
                // A tenth of a second's worth at a time keeps the rate smooth.
                let n = want.min(usize::try_from(rate / 10).unwrap_or(usize::MAX).max(1));
                let took = Duration::from_secs_f64(n as f64 / rate as f64);
                *pace = Some((rate, now + took));
                return n;
            }
        }
    }
}

/// Waits for an action that ends the connection.
async fn ended(ctl: &mut watch::Receiver<Control>) -> End {
    loop {
        let end = ctl.borrow_and_update().end;
        if let Some(end) = end {
            return end;
        }
        changed(ctl).await;
    }
}

/// Waits until the actions in force satisfy `ready`.
async fn until(ctl: &mut watch::Receiver<Control>, ready: impl FnMut(&Control) -> bool) {
    // The connection's task holds the sender, so this never fails while the
    // task runs.
    if ctl.wait_for(ready).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Waits for the actions in force to change.
async fn changed(ctl: &mut watch::Receiver<Control>) {
    if ctl.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Reads and drops what `stream` sends until it closes its end or fails.
async fn discard(stream: &mut TcpStream) {
    let mut buf = vec![0; CHUNK_SIZE];
    while let Ok(n) = stream.read(&mut buf).await {
        if n == 0 {
            return;
        }
    }
}

/// Closes `stream` with a reset (RST) instead of a FIN.
fn reset(stream: TcpStream) {
    if let Err(e) = stream.set_zero_linger() {
        tracing::error!("relay: zero linger before a reset failed: {e}");
    }
    drop(stream);
}
