//! Pluggable connection management and pooling for inter-process communication.
//!
//! This module provides the [`ConnectionManager`] trait for establishing connections
//! to remote peers, and [`ConnectionPool`] for managing a pool of reusable connections
//! with dynamic scaling, health checks, and idle timeout.
//!
//! # Design Philosophy
//!
//! The library delegates connection establishment to the caller's application. This
//! means the application can use any networking architecture (actor frameworks, custom
//! TLS, SSH tunnels, etc.) to create connections. The pool simply manages the lifecycle
//! of whatever connections the manager produces.
//!
//! # Example
//!
//! ```ignore
//! use instancy::communication::connection::*;
//!
//! // Application implements ConnectionManager for its networking
//! struct MyAppConnectionManager { /* ... */ }
//!
//! impl ConnectionManager for MyAppConnectionManager {
//!     type Connection = tokio::net::TcpStream;
//!     type Error = std::io::Error;
//!
//!     async fn establish(&self, request: ConnectionRequest) -> Result<Self::Connection, Self::Error> {
//!         // Application-specific logic: resolve address, establish TLS, etc.
//!         todo!()
//!     }
//!
//!     async fn is_healthy(&self, conn: &Self::Connection) -> bool {
//!         // Check if the connection is still alive
//!         true
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Unique identifier for a physical process (peer) in the cluster.
///
/// This is a **physical** concept — each `PeerId` identifies a distinct OS process
/// that participates in distributed dataflow execution. The connection pool uses
/// `PeerId` to manage physical TCP connections between processes.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct PeerId(pub usize);

impl PeerId {
    /// Create a new peer ID.
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    /// Get the raw index.
    pub fn index(&self) -> usize {
        self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer({})", self.0)
    }
}

impl From<usize> for PeerId {
    fn from(index: usize) -> Self {
        Self(index)
    }
}

/// A request to establish a connection to a remote peer.
///
/// The [`ConnectionManager`] receives this request and uses the information
/// to create the appropriate connection (TCP, TLS, etc.).
#[derive(Clone, Debug)]
pub struct ConnectionRequest {
    /// The remote peer to connect to.
    pub peer_id: PeerId,
    /// The local peer identity (who is initiating).
    pub local_id: PeerId,
    /// A unique request identifier for correlation/logging.
    pub request_id: u64,
}

/// Trait for establishing connections to remote peers.
///
/// Implementations are provided by the application, allowing full control over
/// the networking architecture. For example:
/// - An actor-based system might send a message to the remote node asking it to
///   open a TCP listener, then connect to it.
/// - A TLS-based system would handle certificate validation here.
/// - A simple system might just look up an address map and call `TcpStream::connect`.
///
/// The trait is async to support connection establishment that involves I/O.
pub trait ConnectionManager: Send + Sync + 'static {
    /// The connection type produced by this manager.
    /// Must be Send to allow moving between threads.
    type Connection: Send + 'static;

    /// The error type for connection failures.
    type Error: fmt::Debug + fmt::Display + Send + Sync + 'static;

    /// Establish a new connection to the specified peer.
    ///
    /// This is called by the pool when it needs a new connection (initial fill,
    /// scale-up, or reconnection after failure).
    fn establish(
        &self,
        request: ConnectionRequest,
    ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    /// Check if an existing connection is still healthy.
    ///
    /// Called periodically by the pool to detect dead connections.
    /// The default implementation always returns `true` (no health check).
    fn is_healthy(&self, _conn: &Self::Connection) -> impl Future<Output = bool> + Send {
        async { true }
    }
}

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum connections to maintain per peer (never dropped due to idle timeout).
    /// Default: 1.
    pub min_connections_per_peer: usize,
    /// Maximum connections allowed per peer. Requests beyond this will wait.
    /// Default: 8.
    pub max_connections_per_peer: usize,
    /// How long an idle connection (above min) is kept before being dropped.
    /// Default: 60 seconds.
    pub idle_timeout: Duration,
    /// Interval between health checks on idle connections.
    /// Default: 30 seconds.
    pub health_check_interval: Duration,
    /// Timeout for establishing a new connection.
    /// Default: 10 seconds.
    pub connect_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections_per_peer: 1,
            max_connections_per_peer: 8,
            idle_timeout: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

impl PoolConfig {
    /// Validate the configuration. Returns error message if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_connections_per_peer == 0 {
            return Err("min_connections_per_peer must be at least 1".into());
        }
        if self.max_connections_per_peer < self.min_connections_per_peer {
            return Err(format!(
                "max_connections_per_peer ({}) must be >= min_connections_per_peer ({})",
                self.max_connections_per_peer, self.min_connections_per_peer
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err("connect_timeout must be positive".into());
        }
        Ok(())
    }
}

/// A pooled connection entry with metadata for lifecycle management.
struct PooledConnection<C> {
    /// The actual connection.
    connection: C,
    /// When this connection was last used (returned to pool or created).
    last_used: Instant,
    /// When this connection was last health-checked.
    last_health_check: Instant,
}

/// Per-peer state in the connection pool.
struct PeerState<C> {
    /// Available (idle) connections ready for use.
    idle: Vec<PooledConnection<C>>,
    /// Number of connections currently checked out (in use).
    in_use: usize,
    /// Total connections (idle + in_use).
    total: usize,
}

impl<C> PeerState<C> {
    fn new() -> Self {
        Self {
            idle: Vec::new(),
            in_use: 0,
            total: 0,
        }
    }
}

/// A connection pool that manages reusable connections to remote peers.
///
/// The pool:
/// - Maintains at least `min_connections_per_peer` connections to each known peer
/// - Scales up to `max_connections_per_peer` under load
/// - Drops idle connections (above min) after `idle_timeout`
/// - Performs periodic health checks on idle connections
/// - Delegates actual connection creation to the [`ConnectionManager`]
///
/// # Thread Safety
///
/// The pool uses interior mutability (Mutex) and is safe to share across threads
/// via `Arc<ConnectionPool<M>>`.
pub struct ConnectionPool<M: ConnectionManager> {
    /// The connection manager for establishing new connections.
    manager: Arc<M>,
    /// Pool configuration.
    config: PoolConfig,
    /// Per-peer connection state, protected by a mutex.
    peers: std::sync::Mutex<HashMap<PeerId, PeerState<M::Connection>>>,
    /// Local peer identity (used in connection requests).
    local_id: PeerId,
    /// Counter for generating unique request IDs.
    next_request_id: std::sync::atomic::AtomicU64,
}

/// RAII guard that rolls back pool counters if a connection establishment
/// is cancelled (task dropped) or panics. Call `commit()` on success.
struct SlotReservation<'a, M: ConnectionManager> {
    pool: &'a ConnectionPool<M>,
    peer_id: PeerId,
    committed: bool,
}

impl<'a, M: ConnectionManager> SlotReservation<'a, M> {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl<'a, M: ConnectionManager> Drop for SlotReservation<'a, M> {
    fn drop(&mut self) {
        if !self.committed {
            let mut peers = self.pool.peers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(state) = peers.get_mut(&self.peer_id) {
                state.in_use -= 1;
                state.total -= 1;
            }
        }
    }
}

impl<M: ConnectionManager> ConnectionPool<M> {
    /// Create a new connection pool with the given manager and configuration.
    ///
    /// # Errors
    ///
    /// Returns an error string if the configuration is invalid.
    pub fn new(manager: M, config: PoolConfig, local_id: PeerId) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            manager: Arc::new(manager),
            config,
            peers: std::sync::Mutex::new(HashMap::new()),
            local_id,
            next_request_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Acquire a connection to the specified peer.
    ///
    /// Returns a [`PoolGuard`] that automatically returns the connection to the pool
    /// when dropped.
    ///
    /// If an idle connection is available, it is returned immediately.
    /// If no idle connection is available and the pool is below max capacity, a new
    /// connection is established.
    /// If the pool is at max capacity, returns `None` (caller should retry or wait).
    pub async fn acquire(&self, peer_id: PeerId) -> Result<PoolGuard<'_, M>, PoolError<M::Error>> {
        // Try to get an idle connection
        let conn = {
            let mut peers = self.peers.lock().unwrap_or_else(|e| e.into_inner());
            let state = peers.entry(peer_id).or_insert_with(PeerState::new);

            if let Some(pooled) = state.idle.pop() {
                state.in_use += 1;
                Some(pooled.connection)
            } else if state.total < self.config.max_connections_per_peer {
                state.in_use += 1;
                state.total += 1;
                None // Need to create new connection
            } else {
                return Err(PoolError::AtCapacity {
                    peer_id,
                    max: self.config.max_connections_per_peer,
                });
            }
        };

        let connection = match conn {
            Some(c) => c,
            None => {
                // Establish a new connection.
                // Use a reservation guard to ensure counters are rolled back if
                // establish() panics or the task is cancelled.
                let reservation = SlotReservation {
                    pool: self,
                    peer_id,
                    committed: false,
                };
                let request_id = self
                    .next_request_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let request = ConnectionRequest {
                    peer_id,
                    local_id: self.local_id,
                    request_id,
                };
                match self.manager.establish(request).await {
                    Ok(c) => {
                        reservation.commit();
                        c
                    }
                    Err(e) => {
                        // reservation drops here and rolls back counters
                        return Err(PoolError::ConnectionFailed(e));
                    }
                }
            }
        };

        Ok(PoolGuard {
            pool: self,
            peer_id,
            connection: Some(connection),
        })
    }

    /// Return a connection to the pool.
    fn release(&self, peer_id: PeerId, connection: M::Connection) {
        let mut peers = self.peers.lock().unwrap_or_else(|e| e.into_inner());
        let state = peers.entry(peer_id).or_insert_with(PeerState::new);
        state.in_use -= 1;
        state.idle.push(PooledConnection {
            connection,
            last_used: Instant::now(),
            last_health_check: Instant::now(),
        });
    }

    /// Drop a connection without returning it to the pool (e.g., dead connection).
    fn discard(&self, peer_id: PeerId) {
        let mut peers = self.peers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = peers.get_mut(&peer_id) {
            state.in_use -= 1;
            state.total -= 1;
        }
    }

    /// Evict idle connections that have exceeded their timeout.
    ///
    /// Connections below `min_connections_per_peer` are never evicted.
    /// Call this periodically (e.g., from a background timer task).
    pub fn evict_idle(&self) {
        let now = Instant::now();
        let mut peers = match self.peers.lock() {
            Ok(p) => p,
            Err(_) => return,
        };
        for state in peers.values_mut() {
            let min = self.config.min_connections_per_peer;
            // Evict from the back (newest idle first are kept, oldest evicted)
            let mut new_idle = Vec::new();
            let mut evicted = 0usize;
            for pooled in state.idle.drain(..) {
                let would_remain = state.total - evicted;
                if would_remain <= min {
                    // Must keep — at or below minimum
                    new_idle.push(pooled);
                } else if now.duration_since(pooled.last_used) > self.config.idle_timeout {
                    // Expired — evict
                    evicted += 1;
                } else {
                    // Still fresh — keep
                    new_idle.push(pooled);
                }
            }
            state.total -= evicted;
            state.idle = new_idle;
        }
    }

    /// Run health checks on idle connections, removing dead ones.
    ///
    /// Call this periodically (e.g., from a background timer task).
    pub async fn health_check(&self) {
        let now = Instant::now();

        // Collect connections that need checking (preserving last_used for idle timeout)
        let mut to_check: Vec<(PeerId, M::Connection, Instant)> = Vec::new();
        {
            let mut peers = match self.peers.lock() {
                Ok(p) => p,
                Err(_) => return,
            };
            for (&peer_id, state) in peers.iter_mut() {
                let mut i = 0;
                while i < state.idle.len() {
                    if now.duration_since(state.idle[i].last_health_check)
                        >= self.config.health_check_interval
                    {
                        let pooled = state.idle.swap_remove(i);
                        to_check.push((peer_id, pooled.connection, pooled.last_used));
                        // Don't increment i since swap_remove replaced it
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // Check health outside the lock
        for (peer_id, conn, last_used) in to_check {
            if self.manager.is_healthy(&conn).await {
                // Return healthy connection, preserving original last_used
                if let Ok(mut peers) = self.peers.lock() {
                    if let Some(state) = peers.get_mut(&peer_id) {
                        state.idle.push(PooledConnection {
                            connection: conn,
                            last_used,
                            last_health_check: Instant::now(),
                        });
                    }
                }
            } else {
                // Dead connection — discard
                if let Ok(mut peers) = self.peers.lock() {
                    if let Some(state) = peers.get_mut(&peer_id) {
                        state.total -= 1;
                    }
                }
            }
        }
    }

    /// Get pool statistics for a specific peer.
    pub fn stats(&self, peer_id: PeerId) -> PoolStats {
        let peers = match self.peers.lock() {
            Ok(p) => p,
            Err(_) => {
                return PoolStats {
                    idle: 0,
                    in_use: 0,
                    total: 0,
                };
            }
        };
        match peers.get(&peer_id) {
            Some(state) => PoolStats {
                idle: state.idle.len(),
                in_use: state.in_use,
                total: state.total,
            },
            None => PoolStats {
                idle: 0,
                in_use: 0,
                total: 0,
            },
        }
    }

    /// Get the pool configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }
}

/// Statistics for a peer's connections in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Number of idle (available) connections.
    pub idle: usize,
    /// Number of connections currently in use.
    pub in_use: usize,
    /// Total connections (idle + in_use).
    pub total: usize,
}

/// Errors that can occur during pool operations.
#[derive(Debug)]
pub enum PoolError<E: fmt::Debug + fmt::Display> {
    /// All connections to this peer are in use and the pool is at max capacity.
    AtCapacity {
        /// The peer that is at capacity.
        peer_id: PeerId,
        /// The maximum allowed connections.
        max: usize,
    },
    /// Failed to establish a new connection.
    ConnectionFailed(E),
}

impl<E: fmt::Debug + fmt::Display> fmt::Display for PoolError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtCapacity { peer_id, max } => {
                write!(f, "connection pool at capacity for {peer_id} (max: {max})")
            }
            Self::ConnectionFailed(e) => write!(f, "connection failed: {e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display + Send + Sync + 'static> std::error::Error for PoolError<E> {}

/// RAII guard that returns a connection to the pool when dropped.
///
/// Access the underlying connection via [`std::ops::Deref`]/[`std::ops::DerefMut`].
pub struct PoolGuard<'pool, M: ConnectionManager> {
    pool: &'pool ConnectionPool<M>,
    peer_id: PeerId,
    connection: Option<M::Connection>,
}

impl<'pool, M: ConnectionManager> PoolGuard<'pool, M> {
    /// Get a reference to the underlying connection.
    pub fn connection(&self) -> &M::Connection {
        self.connection.as_ref().expect("guard already consumed")
    }

    /// Get a mutable reference to the underlying connection.
    pub fn connection_mut(&mut self) -> &mut M::Connection {
        self.connection.as_mut().expect("guard already consumed")
    }

    /// Explicitly discard this connection (e.g., after detecting it's broken).
    ///
    /// The connection will NOT be returned to the pool.
    pub fn discard(mut self) {
        self.connection.take();
        self.pool.discard(self.peer_id);
    }

    /// The peer this connection is to.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

impl<M: ConnectionManager> Drop for PoolGuard<'_, M> {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.release(self.peer_id, conn);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A mock connection for testing.
    #[derive(Debug)]
    #[allow(dead_code)]
    struct MockConnection {
        id: u64,
        healthy: bool,
    }

    /// A mock connection manager that tracks establish calls.
    struct MockConnectionManager {
        establish_count: AtomicUsize,
        /// If true, connections report as unhealthy.
        produce_unhealthy: std::sync::atomic::AtomicBool,
        /// If true, establish() returns an error.
        fail_establish: std::sync::atomic::AtomicBool,
    }

    impl MockConnectionManager {
        fn new() -> Self {
            Self {
                establish_count: AtomicUsize::new(0),
                produce_unhealthy: std::sync::atomic::AtomicBool::new(false),
                fail_establish: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[derive(Debug)]
    struct MockError(String);
    impl fmt::Display for MockError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "MockError: {}", self.0)
        }
    }

    impl ConnectionManager for MockConnectionManager {
        type Connection = MockConnection;
        type Error = MockError;

        async fn establish(
            &self,
            request: ConnectionRequest,
        ) -> Result<Self::Connection, Self::Error> {
            if self.fail_establish.load(Ordering::Relaxed) {
                return Err(MockError("establish failed".into()));
            }
            let id = self.establish_count.fetch_add(1, Ordering::Relaxed) as u64;
            let healthy = !self.produce_unhealthy.load(Ordering::Relaxed);
            Ok(MockConnection {
                id: request.request_id + id,
                healthy,
            })
        }

        async fn is_healthy(&self, conn: &Self::Connection) -> bool {
            conn.healthy
        }
    }

    fn make_pool(config: PoolConfig) -> ConnectionPool<MockConnectionManager> {
        ConnectionPool::new(MockConnectionManager::new(), config, PeerId(0)).unwrap()
    }

    fn default_pool() -> ConnectionPool<MockConnectionManager> {
        make_pool(PoolConfig::default())
    }

    // --- PoolConfig validation tests ---

    #[test]
    fn pool_config_default_is_valid() {
        assert!(PoolConfig::default().validate().is_ok());
    }

    #[test]
    fn pool_config_min_zero_rejected() {
        let cfg = PoolConfig {
            min_connections_per_peer: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pool_config_max_less_than_min_rejected() {
        let cfg = PoolConfig {
            min_connections_per_peer: 5,
            max_connections_per_peer: 3,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pool_config_zero_timeout_rejected() {
        let cfg = PoolConfig {
            connect_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    // --- PeerId tests ---

    #[test]
    fn peer_id_display() {
        assert_eq!(PeerId(3).to_string(), "Peer(3)");
    }

    #[test]
    fn peer_id_from_usize() {
        let id: PeerId = 5.into();
        assert_eq!(id.index(), 5);
    }

    // --- ConnectionPool basic tests ---

    #[tokio::test]
    async fn acquire_creates_new_connection() {
        let pool = default_pool();
        let guard = pool.acquire(PeerId(1)).await.unwrap();
        assert_eq!(guard.peer_id(), PeerId(1));
        let stats = pool.stats(PeerId(1));
        assert_eq!(stats.in_use, 1);
        assert_eq!(stats.total, 1);
        assert_eq!(stats.idle, 0);
    }

    #[tokio::test]
    async fn release_returns_connection_to_pool() {
        let pool = default_pool();
        {
            let _guard = pool.acquire(PeerId(1)).await.unwrap();
        } // guard dropped — connection returned
        let stats = pool.stats(PeerId(1));
        assert_eq!(stats.in_use, 0);
        assert_eq!(stats.idle, 1);
        assert_eq!(stats.total, 1);
    }

    #[tokio::test]
    async fn second_acquire_reuses_returned_connection() {
        let pool = default_pool();
        {
            let _guard = pool.acquire(PeerId(1)).await.unwrap();
        }
        // Second acquire should reuse the idle connection (no new establish call)
        let guard = pool.acquire(PeerId(1)).await.unwrap();
        let stats = pool.stats(PeerId(1));
        assert_eq!(stats.in_use, 1);
        assert_eq!(stats.total, 1);
        // Only 1 establish call total
        assert_eq!(pool.manager.establish_count.load(Ordering::Relaxed), 1);
        drop(guard);
    }

    #[tokio::test]
    async fn scales_up_under_demand() {
        let config = PoolConfig {
            max_connections_per_peer: 4,
            ..Default::default()
        };
        let pool = make_pool(config);
        let peer = PeerId(1);

        let g1 = pool.acquire(peer).await.unwrap();
        let g2 = pool.acquire(peer).await.unwrap();
        let g3 = pool.acquire(peer).await.unwrap();

        let stats = pool.stats(peer);
        assert_eq!(stats.in_use, 3);
        assert_eq!(stats.total, 3);
        assert_eq!(pool.manager.establish_count.load(Ordering::Relaxed), 3);

        drop(g1);
        drop(g2);
        drop(g3);
    }

    #[tokio::test]
    async fn at_capacity_returns_error() {
        let config = PoolConfig {
            max_connections_per_peer: 2,
            ..Default::default()
        };
        let pool = make_pool(config);
        let peer = PeerId(1);

        let _g1 = pool.acquire(peer).await.unwrap();
        let _g2 = pool.acquire(peer).await.unwrap();

        // Third acquire should fail
        let result = pool.acquire(peer).await;
        match result {
            Err(PoolError::AtCapacity { peer_id, max }) => {
                assert_eq!(peer_id, peer);
                assert_eq!(max, 2);
            }
            Err(other) => panic!("expected AtCapacity, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn discard_removes_connection_from_pool() {
        let pool = default_pool();
        let peer = PeerId(1);

        let guard = pool.acquire(peer).await.unwrap();
        guard.discard();

        let stats = pool.stats(peer);
        assert_eq!(stats.in_use, 0);
        assert_eq!(stats.idle, 0);
        assert_eq!(stats.total, 0);
    }

    #[tokio::test]
    async fn establish_failure_rolls_back_counters() {
        let pool = default_pool();
        let peer = PeerId(1);

        pool.manager.fail_establish.store(true, Ordering::Relaxed);

        let result = pool.acquire(peer).await;
        assert!(result.is_err());

        let stats = pool.stats(peer);
        assert_eq!(stats.in_use, 0);
        assert_eq!(stats.total, 0);
    }

    #[tokio::test]
    async fn health_check_removes_dead_connections() {
        let config = PoolConfig {
            health_check_interval: Duration::from_millis(0), // check immediately
            ..Default::default()
        };
        let pool = make_pool(config);
        let peer = PeerId(1);

        // Acquire and release a connection
        {
            let _guard = pool.acquire(peer).await.unwrap();
        }

        // Mark future connections unhealthy
        pool.manager
            .produce_unhealthy
            .store(true, Ordering::Relaxed);

        // Manually set existing idle connection to unhealthy
        {
            let mut peers = pool.peers.lock().unwrap();
            if let Some(state) = peers.get_mut(&peer) {
                for pooled in &mut state.idle {
                    pooled.connection.healthy = false;
                    pooled.last_health_check = Instant::now() - Duration::from_secs(100);
                }
            }
        }

        pool.health_check().await;

        let stats = pool.stats(peer);
        assert_eq!(stats.idle, 0);
        assert_eq!(stats.total, 0);
    }

    #[tokio::test]
    async fn evict_idle_respects_min_connections() {
        let config = PoolConfig {
            min_connections_per_peer: 2,
            max_connections_per_peer: 5,
            idle_timeout: Duration::from_millis(0), // everything is "expired"
            ..Default::default()
        };
        let pool = make_pool(config);
        let peer = PeerId(1);

        // Create 4 connections then release them all
        let g1 = pool.acquire(peer).await.unwrap();
        let g2 = pool.acquire(peer).await.unwrap();
        let g3 = pool.acquire(peer).await.unwrap();
        let g4 = pool.acquire(peer).await.unwrap();
        drop(g1);
        drop(g2);
        drop(g3);
        drop(g4);

        let stats_before = pool.stats(peer);
        assert_eq!(stats_before.idle, 4);
        assert_eq!(stats_before.total, 4);

        pool.evict_idle();

        let stats_after = pool.stats(peer);
        // Should keep min_connections_per_peer = 2
        assert!(stats_after.total >= 2);
        assert!(stats_after.total <= 2);
    }

    #[tokio::test]
    async fn multiple_peers_are_independent() {
        let pool = default_pool();

        let _g1 = pool.acquire(PeerId(1)).await.unwrap();
        let _g2 = pool.acquire(PeerId(2)).await.unwrap();

        assert_eq!(pool.stats(PeerId(1)).in_use, 1);
        assert_eq!(pool.stats(PeerId(2)).in_use, 1);
        assert_eq!(pool.stats(PeerId(3)).in_use, 0);
    }

    #[tokio::test]
    async fn guard_connection_access() {
        let pool = default_pool();
        let mut guard = pool.acquire(PeerId(1)).await.unwrap();

        // Can access connection via guard
        let conn = guard.connection();
        assert!(conn.healthy);

        let conn_mut = guard.connection_mut();
        conn_mut.healthy = false;
        assert!(!guard.connection().healthy);
    }

    #[test]
    fn connection_request_fields() {
        let req = ConnectionRequest {
            peer_id: PeerId(5),
            local_id: PeerId(0),
            request_id: 42,
        };
        assert_eq!(req.peer_id.index(), 5);
        assert_eq!(req.local_id.index(), 0);
        assert_eq!(req.request_id, 42);
    }

    #[test]
    fn pool_error_display() {
        let err: PoolError<MockError> = PoolError::AtCapacity {
            peer_id: PeerId(3),
            max: 8,
        };
        assert!(err.to_string().contains("Peer(3)"));
        assert!(err.to_string().contains("max: 8"));

        let err: PoolError<MockError> = PoolError::ConnectionFailed(MockError("timeout".into()));
        assert!(err.to_string().contains("timeout"));
    }
}
