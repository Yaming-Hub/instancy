//! Exchange channel — cross-worker data routing for multi-worker dataflows.
//!
//! This module provides the **physical transport** for exchange edges. A single
//! logical exchange edge in the dataflow graph (declared via [`crate::Pipe::exchange`])
//! is materialized into an N×N matrix of channels — one per (source worker,
//! target worker) pair. These are real data-carrying channels, not graph-level
//! abstractions.
//!
//! Each source worker's [`ExchangePush`] partitions and routes records across
//! the channels based on a hash function. Each target worker's
//! [`ExchangePull`] merges data from its N input channels using round-robin.
//!
//! # No-serialization guarantee (in-process)
//!
//! When all workers run in the same process (the default `spawn_multi` mode),
//! exchange channels use in-memory bounded channels ([`super::bounded`]). Data
//! is moved by value through a `VecDeque<Envelope>` — **no serialization or
//! deserialization occurs**. The `Codec` trait is only invoked for network-backed
//! channels (cross-process exchange via `spawn_cluster`).
//!
//! Note: exchange routing may **clone** records when distributing to multiple
//! target workers. The guarantee is no *byte encoding* overhead, not no-clone.
//!
//! **API bounds vs runtime behavior (with `transport` feature):** When the
//! `transport` feature is enabled, the `exchange` API requires `ExchangeData`
//! at the type level — even for `spawn_multi` (single-process). This is a
//! compile-time safety measure ensuring types are serialization-capable for
//! when a dataflow is deployed cross-process. At runtime, in-process exchange
//! still uses bounded channels and never invokes `Codec`.
//!
//! Without the `transport` feature, exchange only needs `Clone + Send + 'static`.
//!
//! # Transport independence
//!
//! `ExchangePush` and `ExchangePull` hold `Vec<Box<dyn Push/Pull>>` —
//! the concrete transport per worker pair is injected at materialization time.
//! Local workers get bounded in-memory channels; remote workers (future) get
//! network-backed channels via serialization + wire protocol. The dataflow
//! layer is agnostic to worker placement — that is a physical concern owned
//! by the runtime.
//!
//! # Wake semantics
//!
//! A `SharedWakeRegistry` provides bidirectional wake-up:
//! - **Push→target**: when worker i pushes to worker j, worker j is woken
//! - **Pull→source**: when worker j pulls (freeing capacity), worker i is woken
//!   so a backpressured sender can retry
//! - **Close→all**: when a push end closes, all target workers are woken

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pact::ExchangeFn;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::spsc::{SpscPull, SpscPush, spsc_channel};
use crate::dataflow::channels::wake::WakeHandle;
use crate::error::{DataflowError, Error, Result};
#[cfg(test)]
use crate::progress::frontier::Antichain;
use crate::progress::mutable_antichain::MutableAntichain;
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// SharedWakeRegistry
// ---------------------------------------------------------------------------

/// Registry of per-worker wake handles for cross-worker notification.
///
/// Each `SharedWakeRegistry` is scoped to a **single exchange edge** within
/// one `spawn_multi` group — it is *not* shared across different dataflows
/// or different exchange edges. Created by [`create_exchange_factory_creator`],
/// it holds one slot per worker in that group.
///
/// Workers register their wake handles during materialization. During
/// execution, exchange push/pull endpoints use this registry to wake
/// the appropriate worker when data arrives or capacity frees up.
pub(crate) struct SharedWakeRegistry {
    handles: Vec<Mutex<Option<WakeHandle>>>,
}

impl SharedWakeRegistry {
    /// Create a registry for `num_workers` workers with no handles registered.
    pub fn new(num_workers: usize) -> Self {
        let handles = (0..num_workers).map(|_| Mutex::new(None)).collect();
        Self { handles }
    }

    /// Register a worker's wake handle. Called during channel materialization.
    pub fn register(&self, worker_idx: usize, wake: Option<WakeHandle>) {
        if let Some(slot) = self.handles.get(worker_idx) {
            if let Ok(mut guard) = slot.lock() {
                *guard = wake;
            }
        }
    }

    /// Wake a specific worker's executor.
    pub fn wake(&self, worker_idx: usize) {
        if let Some(slot) = self.handles.get(worker_idx) {
            if let Ok(guard) = slot.lock() {
                if let Some(ref wh) = *guard {
                    wh.notify();
                }
            }
        }
    }

    /// Wake all workers (used on close).
    pub fn wake_all(&self) {
        for slot in &self.handles {
            if let Ok(guard) = slot.lock() {
                if let Some(ref wh) = *guard {
                    wh.notify();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ExchangeChannelSet
// ---------------------------------------------------------------------------

/// Pre-allocated N×N matrix of bounded channels for one exchange edge.
///
/// # Intra-node only
///
/// This implementation uses in-process [`SpscPush`]/[`SpscPull`] pairs
/// backed by shared memory (`Arc<Mutex>`), so **all N workers must reside in
/// the same OS process**. For cross-node (distributed) exchange, the matrix
/// entries for remote worker pairs would be replaced with network-backed
/// [`Push`]/[`Pull`] implementations using the `communication::codec` and
/// `communication::connection_pool` layers. The [`ExchangePush`] and
/// [`ExchangePull`] wrappers are transport-agnostic — they depend only on
/// the `Push`/`Pull` trait interface and would work unchanged with remote
/// transports.
///
/// # M×N support
///
/// Supports both symmetric (N×N, all workers are sources and targets) and
/// asymmetric (M×N, different source/target counts) configurations.
/// Use [`new`] for symmetric and [`new_asymmetric`] for M≠N cases.
/// For symmetric channels, [`take_pair`] returns both push and pull;
/// for asymmetric, use [`take_source_endpoints`] and [`take_target_endpoints`].
///
/// Created once per exchange edge during `spawn_multi`. Each worker
/// calls [`take_pair`] to get its `ExchangePush` and `ExchangePull`.
pub(crate) struct ExchangeChannelSet<T: Timestamp, D> {
    num_sources: usize,
    num_targets: usize,
    /// push_ends\[src\]\[dst\] — source worker pushes to destination worker.
    /// Dimensions: num_sources × num_targets.
    push_ends: Vec<Vec<Option<SpscPush<T, D, ()>>>>,
    /// pull_ends\[src\]\[dst\] — destination worker pulls from source worker.
    /// Dimensions: num_sources × num_targets.
    pull_ends: Vec<Vec<Option<SpscPull<T, D, ()>>>>,
}

impl<T: Timestamp, D: Send + 'static> ExchangeChannelSet<T, D> {
    /// Create an N×N matrix of bounded channels (symmetric).
    ///
    /// Channels are created without wake handles — waking is handled
    /// externally by [`SharedWakeRegistry`] through [`ExchangePush`] and
    /// [`ExchangePull`].
    #[cfg(test)]
    pub fn new(num_workers: usize, capacity: usize) -> Self {
        Self::new_asymmetric(num_workers, num_workers, capacity)
    }

    /// Create an M×N matrix of bounded channels (asymmetric).
    ///
    /// M source workers push to N target workers. Each source worker gets
    /// N push endpoints; each target worker gets M pull endpoints.
    pub fn new_asymmetric(num_sources: usize, num_targets: usize, capacity: usize) -> Self {
        let mut push_ends = Vec::with_capacity(num_sources);
        let mut pull_ends = Vec::with_capacity(num_sources);

        for _src in 0..num_sources {
            let mut src_pushers = Vec::with_capacity(num_targets);
            let mut src_pullers = Vec::with_capacity(num_targets);
            for _dst in 0..num_targets {
                let (push, pull) = spsc_channel::<T, D, ()>(capacity);
                src_pushers.push(Some(push));
                src_pullers.push(Some(pull));
            }
            push_ends.push(src_pushers);
            pull_ends.push(src_pullers);
        }

        Self {
            num_sources,
            num_targets,
            push_ends,
            pull_ends,
        }
    }

    /// Take the Push/Pull endpoints for a specific worker (symmetric M==N only).
    ///
    /// Worker `idx` gets:
    /// - Push ends: `push_ends[idx][0..N]` (sends to all target workers)
    /// - Pull ends: `pull_ends[0..M][idx]` (receives from all source workers)
    ///
    /// Returns boxed trait objects so `ExchangePush`/`ExchangePull` are
    /// transport-agnostic. For this in-process implementation, the concrete
    /// types behind the boxes are `SpscPush`/`SpscPull`.
    ///
    /// Each worker's pair can only be taken once.
    ///
    /// # Panics
    /// Panics if `num_sources != num_targets` (use `take_source_endpoints`/
    /// `take_target_endpoints` for asymmetric channels).
    #[cfg(test)]
    pub fn take_pair(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<Box<dyn Push<T, D, ()>>>, Vec<Box<dyn Pull<T, D, ()>>>)> {
        assert_eq!(
            self.num_sources, self.num_targets,
            "take_pair requires symmetric channel (M==N), got M={} N={}",
            self.num_sources, self.num_targets
        );
        let pushers = self.take_source_endpoints(worker_idx)?;
        let pullers = self.take_target_endpoints(worker_idx)?;
        Ok((pushers, pullers))
    }

    /// Take the push endpoints for source worker `src_idx`.
    ///
    /// Returns N push endpoints (one per target worker) from row `src_idx`.
    /// Each source worker's endpoints can only be taken once.
    pub fn take_source_endpoints(
        &mut self,
        src_idx: usize,
    ) -> Result<Vec<Box<dyn Push<T, D, ()>>>> {
        if src_idx >= self.num_sources {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "source index {src_idx} out of range (num_sources={})",
                self.num_sources
            ))));
        }

        let mut pushers: Vec<Box<dyn Push<T, D, ()>>> = Vec::with_capacity(self.num_targets);
        for dst in 0..self.num_targets {
            let push = self.push_ends[src_idx][dst].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::EndpointTaken(format!(
                    "push end [{src_idx}][{dst}] already taken"
                )))
            })?;
            pushers.push(Box::new(push));
        }

        Ok(pushers)
    }

    /// Take the pull endpoints for target worker `dst_idx`.
    ///
    /// Returns M pull endpoints (one per source worker) from column `dst_idx`.
    /// Each target worker's endpoints can only be taken once.
    pub fn take_target_endpoints(
        &mut self,
        dst_idx: usize,
    ) -> Result<Vec<Box<dyn Pull<T, D, ()>>>> {
        if dst_idx >= self.num_targets {
            return Err(Error::Dataflow(DataflowError::InvalidGraph(format!(
                "target index {dst_idx} out of range (num_targets={})",
                self.num_targets
            ))));
        }

        let mut pullers: Vec<Box<dyn Pull<T, D, ()>>> = Vec::with_capacity(self.num_sources);
        for src in 0..self.num_sources {
            let pull = self.pull_ends[src][dst_idx].take().ok_or_else(|| {
                Error::Dataflow(DataflowError::EndpointTaken(format!(
                    "pull end [{src}][{dst_idx}] already taken"
                )))
            })?;
            pullers.push(Box::new(pull));
        }

        Ok(pullers)
    }
}

// ---------------------------------------------------------------------------
// ExchangePush
// ---------------------------------------------------------------------------

/// Push endpoint for an exchange channel.
///
/// Routes each record in a batch to the appropriate target worker based on
/// the exchange hash function. After pushing, wakes the target worker's
/// executor via `SharedWakeRegistry`.
///
/// # Transport independence
///
/// Holds `Vec<Box<dyn Push<T, D>>>` — the concrete transport per target
/// worker is injected at construction time. This enables the runtime to
/// mix local (bounded in-memory) and remote (network-backed) channels
/// transparently. The exchange logic (partitioning, routing) is identical
/// regardless of physical transport.
pub struct ExchangePush<T: Timestamp, D: Send + 'static> {
    /// One push endpoint per target worker (trait object — may be local or remote).
    targets: Vec<Box<dyn Push<T, D, ()>>>,
    /// Hash function for routing records.
    exchange_fn: ExchangeFn<D>,
    /// Number of target workers.
    num_targets: usize,
    /// Shared wake registry for cross-worker notification.
    wakes: Arc<SharedWakeRegistry>,
    /// Whether this push endpoint has been closed.
    closed: bool,
    /// Reusable per-target buckets to avoid allocation on every push.
    buckets: Vec<Vec<D>>,
    /// Optional channel metrics collector (enabled via `MetricsConfig::channel_counters`).
    channel_metrics: Option<Arc<crate::metrics::ChannelMetricsCollector>>,
}

impl<T: Timestamp, D: Send + 'static> ExchangePush<T, D> {
    /// Create a new exchange push endpoint.
    pub(crate) fn new(
        targets: Vec<Box<dyn Push<T, D, ()>>>,
        exchange_fn: ExchangeFn<D>,
        wakes: Arc<SharedWakeRegistry>,
    ) -> Self {
        let num_targets = targets.len();
        let buckets = (0..num_targets).map(|_| Vec::new()).collect();
        Self {
            targets,
            exchange_fn,
            num_targets,
            wakes,
            closed: false,
            buckets,
            channel_metrics: None,
        }
    }

    /// Create a new exchange push endpoint with channel metrics collection.
    pub(crate) fn with_metrics(
        targets: Vec<Box<dyn Push<T, D, ()>>>,
        exchange_fn: ExchangeFn<D>,
        wakes: Arc<SharedWakeRegistry>,
        channel_metrics: Arc<crate::metrics::ChannelMetricsCollector>,
    ) -> Self {
        let num_targets = targets.len();
        let buckets = (0..num_targets).map(|_| Vec::new()).collect();
        Self {
            targets,
            exchange_fn,
            num_targets,
            wakes,
            closed: false,
            buckets,
            channel_metrics: Some(channel_metrics),
        }
    }
}

impl<T: Timestamp, D: Clone + Send + 'static> Push<T, D> for ExchangePush<T, D> {
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }

        // No-op when there are no targets (idle worker in staged execution).
        if self.num_targets == 0 {
            return Ok(());
        }

        match envelope.payload {
            Payload::Data { time, data } => {
                // Record channel metrics before partitioning (count items + bytes).
                if let Some(ref metrics) = self.channel_metrics {
                    let items = data.len() as u64;
                    let bytes = (items as usize * std::mem::size_of::<D>()) as u64;
                    metrics.record_push(items, bytes);
                }

                // Partition records into reusable per-target buckets.
                for record in data {
                    let hash = self.exchange_fn.route(&record);
                    let target = ExchangeFn::<D>::target_worker(hash, self.num_targets);
                    self.buckets[target].push(record);
                }

                // Push non-empty buckets and wake target workers.
                for target_idx in 0..self.num_targets {
                    if !self.buckets[target_idx].is_empty() {
                        let bucket = std::mem::take(&mut self.buckets[target_idx]);
                        match self.targets[target_idx].push(Envelope::data(time.clone(), bucket)) {
                            Ok(()) => self.wakes.wake(target_idx),
                            Err(err) => {
                                // Clear remaining buckets to prevent stale data
                                // leaking into the next push call.
                                for b in &mut self.buckets {
                                    b.clear();
                                }
                                return Err(err);
                            }
                        }
                    }
                }
            }
            Payload::Control(signal) => {
                // Control signals (watermarks, errors) MUST reach ALL targets
                // to avoid progress deadlocks. Use try_push with retry to
                // avoid partial delivery of control signals.
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    // Retry loop: control signals are rare and small, so
                    // spinning briefly is acceptable to ensure atomicity.
                    loop {
                        match target.try_push(Envelope::control(signal.clone())) {
                            Ok(()) => {
                                self.wakes.wake(target_idx);
                                break;
                            }
                            Err((Error::Backpressure, _)) => {
                                // Wake the target so it drains, then retry.
                                self.wakes.wake(target_idx);
                                std::thread::yield_now();
                            }
                            Err((err, _)) => return Err(err),
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, ()>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, ()>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }

        match &envelope.payload {
            Payload::Data { time, data } => {
                // Record channel metrics before partitioning (count items + bytes).
                if let Some(ref metrics) = self.channel_metrics {
                    let items = data.len() as u64;
                    let bytes = (items as usize * std::mem::size_of::<D>()) as u64;
                    metrics.record_push(items, bytes);
                }

                // Partition records into reusable per-target buckets.
                for record in data {
                    let hash = self.exchange_fn.route(record);
                    let target = ExchangeFn::<D>::target_worker(hash, self.num_targets);
                    self.buckets[target].push(record.clone());
                }

                // Pre-check ALL targets for capacity before pushing anything.
                // This preserves all-or-nothing delivery semantics: if any
                // target cannot accept its sub-batch, the original envelope
                // is returned intact for retry — no partial delivery.
                //
                // Targets that report available_capacity() == None (e.g.,
                // future network channels) are assumed to have capacity.
                // This is safe because network-backed Push implementations
                // buffer internally and their try_push handles backpressure.
                for target_idx in 0..self.num_targets {
                    if self.buckets[target_idx].is_empty() {
                        continue;
                    }
                    if self.targets[target_idx].is_closed() {
                        // Clear buckets before returning.
                        for b in &mut self.buckets {
                            b.clear();
                        }
                        return Err((Error::ChannelClosed, envelope));
                    }
                    if let Some(available) = self.targets[target_idx].available_capacity() {
                        if available == 0 {
                            for b in &mut self.buckets {
                                b.clear();
                            }
                            return Err((Error::Backpressure, envelope));
                        }
                    }
                }

                // Pre-check passed — push sub-batches. For local channels
                // (single-producer per exchange matrix slot), no concurrent
                // writer can fill the channel between check and push, so
                // this is effectively atomic. If it fails anyway (e.g.,
                // future concurrent access), we accept partial delivery
                // as a last resort.
                for target_idx in 0..self.num_targets {
                    if !self.buckets[target_idx].is_empty() {
                        let bucket = std::mem::take(&mut self.buckets[target_idx]);
                        match self.targets[target_idx]
                            .try_push(Envelope::data(time.clone(), bucket))
                        {
                            Ok(()) => self.wakes.wake(target_idx),
                            Err((err, _sub)) => {
                                // Clear remaining buckets.
                                for b in &mut self.buckets {
                                    b.clear();
                                }
                                return Err((err, envelope));
                            }
                        }
                    }
                }
            }
            Payload::Control(signal) => {
                // Pre-check all targets for capacity before broadcasting.
                for target in self.targets.iter() {
                    if target.is_closed() {
                        return Err((Error::ChannelClosed, envelope));
                    }
                    if let Some(available) = target.available_capacity() {
                        if available == 0 {
                            return Err((Error::Backpressure, envelope));
                        }
                    }
                }

                // All targets ready — broadcast control signal.
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    match target.try_push(Envelope::control(signal.clone())) {
                        Ok(()) => self.wakes.wake(target_idx),
                        Err((err, _)) => return Err((err, envelope)),
                    }
                }
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        for target in &mut self.targets {
            target.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) {
        self.closed = true;
        for target in &mut self.targets {
            target.close();
        }
        self.wakes.wake_all();
    }

    fn is_closed(&self) -> bool {
        self.closed
    }
}

impl<T: Timestamp, D: Send + 'static> Drop for ExchangePush<T, D> {
    fn drop(&mut self) {
        // Ensure all targets are closed so pull sides see exhaustion.
        for target in &mut self.targets {
            target.close();
        }
        self.wakes.wake_all();
    }
}

// ---------------------------------------------------------------------------
// BroadcastPush
// ---------------------------------------------------------------------------

/// Push endpoint for a broadcast channel.
///
/// Clones each record in a batch to ALL target workers. Unlike `ExchangePush`
/// which routes each item to exactly one target, `BroadcastPush` sends every
/// item to every target (fan-out). This is useful for distributing reference
/// data, configuration updates, or small datasets to all workers.
///
/// Requires `D: Clone` since each record is duplicated N-1 times.
pub struct BroadcastPush<T: Timestamp, D: Clone + Send + 'static> {
    /// One push endpoint per target worker.
    targets: Vec<Box<dyn Push<T, D, ()>>>,
    /// Number of target workers.
    num_targets: usize,
    /// Shared wake registry for cross-worker notification.
    wakes: Arc<SharedWakeRegistry>,
    /// Whether this push endpoint has been closed.
    closed: bool,
}

impl<T: Timestamp, D: Clone + Send + 'static> BroadcastPush<T, D> {
    /// Create a new broadcast push endpoint.
    pub(crate) fn new(
        targets: Vec<Box<dyn Push<T, D, ()>>>,
        wakes: Arc<SharedWakeRegistry>,
    ) -> Self {
        let num_targets = targets.len();
        Self {
            targets,
            num_targets,
            wakes,
            closed: false,
        }
    }
}

impl<T: Timestamp, D: Clone + Send + 'static> Push<T, D> for BroadcastPush<T, D> {
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }

        // No-op when there are no targets (idle worker in staged execution).
        if self.num_targets == 0 {
            return Ok(());
        }

        match envelope.payload {
            Payload::Data { time, data } => {
                // Send a clone to each target except the last, which gets the owned data.
                let last_idx = self.num_targets - 1;
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    let batch = if target_idx == last_idx {
                        // Last target gets the owned data (avoid final clone).
                        break;
                    } else {
                        data.clone()
                    };
                    if !batch.is_empty() {
                        target.push(Envelope::data(time.clone(), batch))?;
                        self.wakes.wake(target_idx);
                    }
                }
                // Send owned data to the last target.
                if !data.is_empty() {
                    self.targets[last_idx].push(Envelope::data(time, data))?;
                    self.wakes.wake(last_idx);
                }
            }
            Payload::Control(signal) => {
                // Control signals broadcast to all targets (same as ExchangePush).
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    loop {
                        match target.try_push(Envelope::control(signal.clone())) {
                            Ok(()) => {
                                self.wakes.wake(target_idx);
                                break;
                            }
                            Err((Error::Backpressure, _)) => {
                                self.wakes.wake(target_idx);
                                std::thread::yield_now();
                            }
                            Err((err, _)) => return Err(err),
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn try_push(
        &mut self,
        envelope: Envelope<T, D, ()>,
    ) -> std::result::Result<(), (Error, Envelope<T, D, ()>)> {
        if self.closed {
            return Err((Error::ChannelClosed, envelope));
        }

        match &envelope.payload {
            Payload::Data { time, data } => {
                // Pre-check all targets for capacity.
                for target in self.targets.iter() {
                    if target.is_closed() {
                        return Err((Error::ChannelClosed, envelope));
                    }
                    if let Some(available) = target.available_capacity() {
                        if available == 0 {
                            return Err((Error::Backpressure, envelope));
                        }
                    }
                }

                // All targets ready — broadcast data.
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    let batch = data.clone();
                    if !batch.is_empty() {
                        match target.try_push(Envelope::data(time.clone(), batch)) {
                            Ok(()) => self.wakes.wake(target_idx),
                            Err((err, _)) => return Err((err, envelope)),
                        }
                    }
                }
            }
            Payload::Control(signal) => {
                // Pre-check all targets for capacity.
                for target in self.targets.iter() {
                    if target.is_closed() {
                        return Err((Error::ChannelClosed, envelope));
                    }
                    if let Some(available) = target.available_capacity() {
                        if available == 0 {
                            return Err((Error::Backpressure, envelope));
                        }
                    }
                }

                // All targets ready — broadcast control signal.
                for (target_idx, target) in self.targets.iter_mut().enumerate() {
                    match target.try_push(Envelope::control(signal.clone())) {
                        Ok(()) => self.wakes.wake(target_idx),
                        Err((err, _)) => return Err((err, envelope)),
                    }
                }
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        for target in &mut self.targets {
            target.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) {
        self.closed = true;
        for target in &mut self.targets {
            target.close();
        }
        self.wakes.wake_all();
    }

    fn is_closed(&self) -> bool {
        self.closed
    }
}

impl<T: Timestamp, D: Clone + Send + 'static> Drop for BroadcastPush<T, D> {
    fn drop(&mut self) {
        for target in &mut self.targets {
            target.close();
        }
        self.wakes.wake_all();
    }
}

// ---------------------------------------------------------------------------
// ExchangePull
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FrontierAggregator — per-source watermark tracking
// ---------------------------------------------------------------------------

/// Aggregates watermarks from N source workers into a single frontier.
///
/// Each source worker independently advances its watermark. The aggregated
/// frontier is the **meet** (minimal antichain) across all per-source frontiers.
/// A downstream operator should only see a watermark advance when ALL sources
/// have advanced past that timestamp.
///
/// Uses [`MutableAntichain`] internally: each source contributes +1 count for
/// its current watermark. The frontier = minimal timestamps with positive count
/// = the set of timestamps where at least one source might still produce data.
///
/// Handles both total-order and partial-order timestamps correctly, though the
/// current [`ControlSignal::Watermark`] carries a single `T` (natural for
/// total-order). For partial-order timestamps, multiple watermark emissions may
/// be needed per frontier change.
struct FrontierAggregator<T: Timestamp> {
    /// Current watermark per source. `None` = no watermark received yet, meaning
    /// the source's frontier is at `T::minimum()` (data at any time possible).
    per_source: Vec<Option<T>>,
    /// Tracks the aggregated frontier using change-based accounting.
    aggregated: MutableAntichain<T>,
}

impl<T: Timestamp> FrontierAggregator<T> {
    /// Create a new aggregator for `num_sources` upstream workers.
    ///
    /// Each source starts with its frontier at `T::minimum()`, meaning data at
    /// any timestamp could still arrive. The initial aggregated frontier is
    /// `{T::minimum()}`.
    ///
    /// # Contract
    ///
    /// Sources **must** send monotonically non-decreasing watermarks. A source
    /// that closes without advancing its watermark will hold back the aggregated
    /// frontier. Higher-level close/exhaustion handling (removing a closed
    /// source's contribution) is deferred to a future PR.
    ///
    /// # Panics (debug only)
    ///
    /// Panics in debug builds if `num_sources == 0`.
    fn new(num_sources: usize) -> Self {
        let mut aggregated = MutableAntichain::new();
        // Each source contributes T::minimum() with count +1.
        let inits: Vec<(T, i64)> = (0..num_sources).map(|_| (T::minimum(), 1)).collect();
        aggregated.update_iter(inits);

        FrontierAggregator {
            per_source: vec![None; num_sources],
            aggregated,
        }
    }

    /// Update the watermark for a specific source and return any new frontier
    /// timestamps to emit downstream.
    ///
    /// Returns timestamps that were **added** to the aggregated frontier (i.e.,
    /// the frontier advanced). Empty if the overall frontier didn't change.
    ///
    /// # Panics (debug only)
    ///
    /// Panics in debug builds if `source_idx >= num_sources` or if `new_watermark`
    /// is less than the source's previous watermark (monotonicity violation).
    fn update(&mut self, source_idx: usize, new_watermark: T) -> Vec<T> {
        debug_assert!(
            source_idx < self.per_source.len(),
            "source_idx {} out of bounds (num_sources = {})",
            source_idx,
            self.per_source.len()
        );

        let old = self.per_source[source_idx].take();
        let old_time = old.unwrap_or_else(T::minimum);

        // Watermarks must be monotonically non-decreasing.
        debug_assert!(
            old_time.less_equal(&new_watermark),
            "watermark must not decrease: old={:?}, new={:?}",
            old_time,
            new_watermark
        );

        self.per_source[source_idx] = Some(new_watermark.clone());

        let changes = self
            .aggregated
            .update_iter(vec![(old_time, -1), (new_watermark, 1)]);

        // Return only the frontier additions (new watermarks to emit).
        // Removals (frontier retreats) are not expressible with the current
        // Watermark(T) control signal and should not occur in practice since
        // watermarks only advance.
        changes
            .into_iter()
            .filter(|(_, delta)| *delta > 0)
            .map(|(t, _)| t)
            .collect()
    }

    /// Returns the current aggregated frontier.
    #[cfg(test)]
    fn frontier(&self) -> Antichain<T> {
        self.aggregated.frontier_antichain()
    }
}

// ---------------------------------------------------------------------------
// ExchangePull — merges data from all source workers with frontier aggregation
// ---------------------------------------------------------------------------

/// Pull endpoint for exchange channels.
///
/// Merges data from all source workers using round-robin polling.
/// After pulling (which frees channel capacity), wakes the source
/// worker so a backpressured sender can retry.
///
/// ## Frontier aggregation
///
/// Watermarks from individual source workers are **not** passed through directly.
/// Instead, a `FrontierAggregator` tracks per-source watermarks and only emits
/// an aggregated watermark downstream when ALL sources have advanced past a
/// timestamp. This prevents premature frontier advancement that could cause
/// operators to incorrectly discard data or deadlock.
///
/// Error control signals pass through immediately (they are not aggregated).
///
/// # Transport independence
///
/// Holds `Vec<Box<dyn Pull<T, D>>>` — the concrete transport per source
/// worker is injected at construction time. This enables the runtime to
/// mix local (bounded in-memory) and remote (network-backed) channels
/// transparently.
pub struct ExchangePull<T: Timestamp, D> {
    /// One pull endpoint per source worker (trait object — may be local or remote).
    sources: Vec<Box<dyn Pull<T, D, ()>>>,
    /// Round-robin index for fair merging.
    next_source: usize,
    /// Number of source workers.
    num_sources: usize,
    /// Shared wake registry for notifying source workers on capacity freed.
    wakes: Arc<SharedWakeRegistry>,
    /// Aggregates watermarks from all source workers.
    frontier_agg: FrontierAggregator<T>,
    /// Buffered watermark envelopes produced by frontier aggregation.
    pending_watermarks: VecDeque<T>,
}

impl<T: Timestamp, D> ExchangePull<T, D> {
    /// Create a new exchange pull endpoint.
    pub(crate) fn new(
        sources: Vec<Box<dyn Pull<T, D, ()>>>,
        wakes: Arc<SharedWakeRegistry>,
    ) -> Self {
        let num_sources = sources.len();
        let frontier_agg = FrontierAggregator::new(num_sources);
        Self {
            sources,
            next_source: 0,
            num_sources,
            wakes,
            frontier_agg,
            pending_watermarks: VecDeque::new(),
        }
    }
}

impl<T: Timestamp, D: Send + 'static> Pull<T, D> for ExchangePull<T, D> {
    fn pull(&mut self) -> Option<Envelope<T, D, ()>> {
        // No sources means this is an idle worker in staged execution.
        if self.num_sources == 0 {
            return None;
        }

        // First, emit any buffered aggregated watermarks.
        if let Some(t) = self.pending_watermarks.pop_front() {
            return Some(Envelope::watermark(t));
        }

        // Round-robin with retry: if watermarks are absorbed (frontier didn't
        // advance), data might be queued behind them. Restart the scan so we
        // don't return None while deliverable messages exist. Each iteration
        // consumes ≥1 item from a finite-capacity channel, guaranteeing
        // termination.
        loop {
            let mut absorbed_watermark = false;
            for _ in 0..self.num_sources {
                let idx = self.next_source;
                self.next_source = (self.next_source + 1) % self.num_sources;
                if let Some(env) = self.sources[idx].pull() {
                    // Wake the source worker — we freed capacity, so a
                    // backpressured sender can retry.
                    self.wakes.wake(idx);

                    match env.payload {
                        Payload::Data { .. } => return Some(env),
                        Payload::Control(ControlSignal::Watermark(t)) => {
                            // Aggregate: update source frontier, emit only if
                            // the overall frontier advanced.
                            let new_watermarks = self.frontier_agg.update(idx, t);
                            for wm in new_watermarks {
                                self.pending_watermarks.push_back(wm);
                            }
                            // Return a buffered watermark if available, else continue.
                            if let Some(t) = self.pending_watermarks.pop_front() {
                                return Some(Envelope::watermark(t));
                            }
                            // Frontier didn't advance — mark for retry.
                            absorbed_watermark = true;
                        }
                        Payload::Control(_) => {
                            // Errors pass through immediately.
                            return Some(env);
                        }
                    }
                }
            }
            if !absorbed_watermark {
                return None;
            }
            // Watermarks absorbed — data might follow. Retry round-robin.
        }
    }

    fn drain_into(&mut self, buffer: &mut Vec<Envelope<T, D, ()>>) -> usize {
        let mut count = 0;

        // Emit any buffered aggregated watermarks first.
        while let Some(t) = self.pending_watermarks.pop_front() {
            buffer.push(Envelope::watermark(t));
            count += 1;
        }

        // Drain round-robin until no source has data.
        loop {
            let mut found_any = false;
            for src_idx in 0..self.num_sources {
                if let Some(env) = self.sources[src_idx].pull() {
                    found_any = true;
                    self.wakes.wake(src_idx);

                    match env.payload {
                        Payload::Data { .. } => {
                            buffer.push(env);
                            count += 1;
                        }
                        Payload::Control(ControlSignal::Watermark(t)) => {
                            let new_watermarks = self.frontier_agg.update(src_idx, t);
                            for wm in new_watermarks {
                                buffer.push(Envelope::watermark(wm));
                                count += 1;
                            }
                        }
                        Payload::Control(_) => {
                            // Errors pass through immediately.
                            buffer.push(env);
                            count += 1;
                        }
                    }
                }
            }
            if !found_any {
                break;
            }
        }
        count
    }

    fn is_exhausted(&self) -> bool {
        self.sources.iter().all(|s| s.is_exhausted())
    }
}

// ---------------------------------------------------------------------------
// ExchangeChannelCreator — type-erased factory for spawn_multi
// ---------------------------------------------------------------------------

/// Type-erased creator for exchange channel factories (local mode).
///
/// Created by the builder (which knows T, D, and ExchangeFn), stored on
/// [`LogicalDataflow`], and consumed by `spawn_multi` to produce channel
/// factories that all reference the same underlying channel infrastructure.
///
/// Parameters: `(num_source_workers, num_target_workers, capacity, channel_metrics)`.
/// For symmetric exchanges (most common), pass the same value for both counts.
/// Pass `None` for `channel_metrics` when channel counters are disabled.
pub(crate) type ExchangeFactoryCreatorFn = Box<
    dyn FnOnce(
            usize,
            usize,
            usize,
            Option<Arc<crate::metrics::ChannelMetricsCollector>>,
        ) -> Vec<super::super::schedulable::ChannelFactory>
        + Send,
>;

/// Create a type-erased exchange factory creator using the default local
/// transport ([`LocalEdgeMaterializer`]).
///
/// The returned closure captures the `ExchangeFn<D>` and concrete types
/// `T`, `D`. When called with `(num_source_workers, num_target_workers, capacity)`,
/// it produces channel factories backed by in-process bounded channels.
pub(crate) fn create_exchange_factory_creator<T, D>(
    exchange_fn: ExchangeFn<D>,
) -> ExchangeFactoryCreatorFn
where
    T: Timestamp,
    D: Clone + Send + 'static,
{
    Box::new(
        move |num_source_workers: usize,
              num_target_workers: usize,
              capacity: usize,
              channel_metrics: Option<Arc<crate::metrics::ChannelMetricsCollector>>| {
            let materializer = Arc::new(Mutex::new(
                super::edge_materializer::LocalEdgeMaterializer::<T, D>::new_asymmetric(
                    num_source_workers,
                    num_target_workers,
                    capacity,
                ),
            ));
            build_exchange_factories(
                num_source_workers,
                num_target_workers,
                exchange_fn,
                materializer,
                channel_metrics,
            )
        },
    )
}

/// Parameters for creating a network-backed edge materializer.
///
/// Passed by `spawn_cluster` to each [`NetworkExchangeCreator`].
/// The creator uses these to construct a `NetworkEdgeMaterializer<T, D>`
/// (it knows the concrete types T, D from its generic parameters).
#[cfg(feature = "transport")]
pub(crate) struct NetworkMaterializerParams {
    pub dataflow_id: crate::dataflow::id::DataflowId,
    pub topology: crate::execute::ClusterTopology,
    pub local_node_id: String,
    pub transport: Arc<crate::communication::cluster_transport::ClusterTransport>,
    /// Pre-extracted receivers for this specific exchange edge.
    pub receivers: std::collections::HashMap<
        String,
        std::collections::HashMap<u64, tokio::sync::mpsc::Receiver<Vec<u8>>>,
    >,
    pub capacity: usize,
    pub num_workers: usize,
    pub edge_index: usize,
    /// Wake handles for all workers (indexed by global worker ID).
    /// Used to notify the executor when remote data arrives.
    pub wake_handles: Vec<crate::dataflow::channels::wake::WakeHandle>,
    /// Tokio runtime handle for spawning bridge tasks.
    pub runtime_handle: tokio::runtime::Handle,
    /// Optional channel metrics collector for this exchange edge.
    pub channel_metrics: Option<Arc<crate::metrics::ChannelMetricsCollector>>,
}

/// Trait for creating network-backed exchange channel factories.
///
/// Implemented by [`NetworkExchangeCreatorImpl`] which captures the concrete
/// types T, D and the `ExchangeFn<D>`. This allows `spawn_cluster` to create
/// network-backed factories without knowing the concrete data types — the
/// virtual method dispatch handles the type erasure.
#[cfg(feature = "transport")]
pub(crate) trait NetworkExchangeCreator: Send {
    /// Create exchange channel factories using network transport.
    fn create(
        self: Box<Self>,
        params: NetworkMaterializerParams,
    ) -> Vec<super::super::schedulable::ChannelFactory>;
}

/// Concrete implementation of [`NetworkExchangeCreator`] for specific T, D types.
#[cfg(feature = "transport")]
pub(crate) struct NetworkExchangeCreatorImpl<T, D>
where
    T: Timestamp + crate::communication::codec::ExchangeData,
    D: Clone + crate::communication::codec::ExchangeData,
{
    pub exchange_fn: ExchangeFn<D>,
    pub _phantom: std::marker::PhantomData<T>,
}

#[cfg(feature = "transport")]
impl<T, D> NetworkExchangeCreator for NetworkExchangeCreatorImpl<T, D>
where
    T: Timestamp + crate::communication::codec::ExchangeData,
    D: Clone + crate::communication::codec::ExchangeData,
{
    fn create(
        self: Box<Self>,
        params: NetworkMaterializerParams,
    ) -> Vec<super::super::schedulable::ChannelFactory> {
        let materializer = Arc::new(Mutex::new(
            super::network::NetworkEdgeMaterializer::<T, D>::new(
                params.dataflow_id,
                params.topology,
                params.local_node_id,
                params.transport,
                params.receivers,
                params.capacity,
                params.edge_index,
                params.wake_handles,
                params.runtime_handle,
            ),
        ));
        build_exchange_factories(
            params.num_workers,
            params.num_workers,
            self.exchange_fn,
            materializer,
            params.channel_metrics,
        )
    }
}

/// Concrete implementation of [`NetworkExchangeCreator`] for broadcast channels.
///
/// Unlike `NetworkExchangeCreatorImpl` which routes to one target,
/// this creates broadcast factories that clone data to all targets.
#[cfg(feature = "transport")]
pub(crate) struct NetworkBroadcastCreatorImpl<T, D>
where
    T: Timestamp + crate::communication::codec::ExchangeData,
    D: Clone + crate::communication::codec::ExchangeData,
{
    pub _phantom: std::marker::PhantomData<(T, D)>,
}

#[cfg(feature = "transport")]
impl<T, D> NetworkExchangeCreator for NetworkBroadcastCreatorImpl<T, D>
where
    T: Timestamp + crate::communication::codec::ExchangeData,
    D: Clone + crate::communication::codec::ExchangeData,
{
    fn create(
        self: Box<Self>,
        params: NetworkMaterializerParams,
    ) -> Vec<super::super::schedulable::ChannelFactory> {
        let materializer = Arc::new(Mutex::new(
            super::network::NetworkEdgeMaterializer::<T, D>::new(
                params.dataflow_id,
                params.topology,
                params.local_node_id,
                params.transport,
                params.receivers,
                params.capacity,
                params.edge_index,
                params.wake_handles,
                params.runtime_handle,
            ),
        ));
        build_broadcast_factories(params.num_workers, params.num_workers, materializer)
    }
}

/// Create exchange channel factories using a custom [`EdgeMaterializer`].
///
/// This is the extension point for cross-node exchange: the runtime
/// creates a materializer that mixes local and network-backed channels
/// based on cluster topology, then passes it here. The exchange routing
/// logic (`ExchangePush`/`ExchangePull`) is identical regardless of
/// Internal helper: build channel factories from a shared materializer.
///
/// Creates `num_source_workers` factories. In symmetric mode (M==N), each
/// factory materializes both push and pull endpoints. In asymmetric mode,
/// source factories get push endpoints, and the first N factories also get
/// pull endpoints for the target side.
///
/// Currently, all callers use symmetric mode (same workers serve as both
/// source and target). Asymmetric materialization will be activated when
/// per-stage executors are implemented.
fn build_exchange_factories<T, D>(
    num_source_workers: usize,
    num_target_workers: usize,
    exchange_fn: ExchangeFn<D>,
    materializer: Arc<Mutex<dyn super::edge_materializer::EdgeMaterializer<T, D>>>,
    channel_metrics: Option<Arc<crate::metrics::ChannelMetricsCollector>>,
) -> Vec<super::super::schedulable::ChannelFactory>
where
    T: Timestamp,
    D: Clone + Send + 'static,
{
    // Validate consistency.
    {
        let mat = materializer.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: worker counts validated during graph construction
        assert_eq!(
            mat.num_source_workers(),
            num_source_workers,
            "EdgeMaterializer num_source_workers ({}) != expected ({})",
            mat.num_source_workers(),
            num_source_workers
        );
        // SAFETY: worker counts validated during graph construction
        assert_eq!(
            mat.num_target_workers(),
            num_target_workers,
            "EdgeMaterializer num_target_workers ({}) != expected ({})",
            mat.num_target_workers(),
            num_target_workers
        );
    }

    // The wake registry is sized for the maximum of source/target counts
    // to support both source and target workers' wake handles.
    let wake_count = std::cmp::max(num_source_workers, num_target_workers);
    let wakes = Arc::new(SharedWakeRegistry::new(wake_count));
    let total_factories = std::cmp::max(num_source_workers, num_target_workers);

    (0..total_factories)
        .map(|worker_slot| {
            let wakes = wakes.clone();
            let materializer = materializer.clone();
            let exchange_fn = exchange_fn.clone();
            let ch_metrics = channel_metrics.clone();
            let m = num_source_workers;
            let n = num_target_workers;
            super::super::schedulable::channel_factory(
                move |ctx: &crate::worker::WorkerContext, wake: Option<WakeHandle>| {
                    let slot = worker_slot;
                    wakes.register(slot, wake);

                    let mut mat = materializer.lock().unwrap_or_else(|e| e.into_inner());

                    let pushers: Vec<Box<dyn crate::dataflow::channels::Push<T, D, ()>>> =
                        if slot < m {
                            mat.materialize_source_worker(slot)?
                        } else {
                            Vec::new()
                        };

                    let pullers: Vec<Box<dyn crate::dataflow::channels::Pull<T, D, ()>>> =
                        if slot < n {
                            mat.materialize_target_worker(slot)?
                        } else {
                            Vec::new()
                        };

                    drop(mat);

                    let _ = ctx;
                    let push = if let Some(ref metrics) = ch_metrics {
                        ExchangePush::with_metrics(
                            pushers,
                            exchange_fn.clone(),
                            wakes.clone(),
                            Arc::clone(metrics),
                        )
                    } else {
                        ExchangePush::new(pushers, exchange_fn.clone(), wakes.clone())
                    };
                    let pull = ExchangePull::new(pullers, wakes.clone());

                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                },
            )
        })
        .collect()
}

/// Create a type-erased broadcast factory creator using the default local transport.
///
/// Similar to [`create_exchange_factory_creator`] but produces broadcast channels
/// where each item is cloned to ALL target workers (fan-out).
pub(crate) fn create_broadcast_factory_creator<T, D>() -> ExchangeFactoryCreatorFn
where
    T: Timestamp,
    D: Clone + Send + 'static,
{
    Box::new(
        move |num_source_workers: usize,
              num_target_workers: usize,
              capacity: usize,
              _channel_metrics: Option<Arc<crate::metrics::ChannelMetricsCollector>>| {
            let materializer = Arc::new(Mutex::new(
                super::edge_materializer::LocalEdgeMaterializer::<T, D>::new_asymmetric(
                    num_source_workers,
                    num_target_workers,
                    capacity,
                ),
            ));
            build_broadcast_factories(num_source_workers, num_target_workers, materializer)
        },
    )
}

/// Internal helper: build broadcast channel factories from a shared materializer.
///
/// Like `build_exchange_factories` but uses `BroadcastPush` (clone to all)
/// instead of `ExchangePush` (route to one).
fn build_broadcast_factories<T, D>(
    num_source_workers: usize,
    num_target_workers: usize,
    materializer: Arc<Mutex<dyn super::edge_materializer::EdgeMaterializer<T, D>>>,
) -> Vec<super::super::schedulable::ChannelFactory>
where
    T: Timestamp,
    D: Clone + Send + 'static,
{
    {
        let mat = materializer.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: worker counts validated during graph construction
        assert_eq!(mat.num_source_workers(), num_source_workers);
        // SAFETY: worker counts validated during graph construction
        assert_eq!(mat.num_target_workers(), num_target_workers);
    }

    let wake_count = std::cmp::max(num_source_workers, num_target_workers);
    let wakes = Arc::new(SharedWakeRegistry::new(wake_count));
    let total_factories = std::cmp::max(num_source_workers, num_target_workers);

    (0..total_factories)
        .map(|worker_slot| {
            let wakes = wakes.clone();
            let materializer = materializer.clone();
            let m = num_source_workers;
            let n = num_target_workers;
            super::super::schedulable::channel_factory(
                move |ctx: &crate::worker::WorkerContext, wake: Option<WakeHandle>| {
                    let slot = worker_slot;
                    wakes.register(slot, wake);

                    let mut mat = materializer.lock().unwrap_or_else(|e| e.into_inner());

                    let pushers: Vec<Box<dyn crate::dataflow::channels::Push<T, D, ()>>> =
                        if slot < m {
                            mat.materialize_source_worker(slot)?
                        } else {
                            Vec::new()
                        };

                    let pullers: Vec<Box<dyn crate::dataflow::channels::Pull<T, D, ()>>> =
                        if slot < n {
                            mat.materialize_target_worker(slot)?
                        } else {
                            Vec::new()
                        };

                    drop(mat);

                    let _ = ctx;
                    let push = BroadcastPush::new(pushers, wakes.clone());
                    let pull = ExchangePull::new(pullers, wakes.clone());

                    Ok((
                        Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                        Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                            as Box<dyn std::any::Any + Send>,
                    ))
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_wake_registry_basic() {
        let reg = SharedWakeRegistry::new(3);
        let wake = WakeHandle::new();
        reg.register(0, Some(wake.clone()));
        reg.register(1, None);
        // Waking should not panic even with None handles.
        reg.wake(0);
        reg.wake(1);
        reg.wake(2); // not registered
        reg.wake_all();
    }

    #[test]
    fn exchange_channel_set_creation() {
        let set = ExchangeChannelSet::<u64, i32>::new(3, 16);
        assert_eq!(set.num_sources, 3);
        assert_eq!(set.num_targets, 3);
        assert_eq!(set.push_ends.len(), 3);
        assert_eq!(set.pull_ends.len(), 3);
    }

    #[test]
    fn exchange_channel_set_take_pair() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);

        let (pushers0, pullers0) = set.take_pair(0).unwrap();
        assert_eq!(pushers0.len(), 2); // worker 0 pushes to 0 and 1
        assert_eq!(pullers0.len(), 2); // worker 0 pulls from 0 and 1

        let (pushers1, pullers1) = set.take_pair(1).unwrap();
        assert_eq!(pushers1.len(), 2);
        assert_eq!(pullers1.len(), 2);
    }

    #[test]
    fn exchange_push_routes_by_hash() {
        // 2 workers. Hash function: even → worker 0, odd → worker 1.
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (pushers, _pullers0) = set.take_pair(0).unwrap();
        let (_pushers1, pullers1) = set.take_pair(1).unwrap();

        let exchange_fn = ExchangeFn::new("mod2", |x: &i32| *x as u64);
        let mut push = ExchangePush::new(pushers, exchange_fn, wakes.clone());

        // Push batch: [10, 11, 12, 13]
        // 10 % 2 = 0 → worker 0, 11 % 2 = 1 → worker 1,
        // 12 % 2 = 0 → worker 0, 13 % 2 = 1 → worker 1
        push.push(Envelope::data(0u64, vec![10, 11, 12, 13]))
            .unwrap();

        // Worker 0's pull (from source 0): should see [10, 12]
        let mut pull0 = ExchangePull::new(_pullers0, wakes.clone());
        let env = pull0.pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![10, 12]);

        // Worker 1's pull (from source 0): should see [11, 13]
        let mut pull1_from0 = pullers1;
        let env = pull1_from0[0].pull().unwrap(); // source 0 → dest 1
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![11, 13]);
    }

    #[test]
    fn exchange_pull_round_robin() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        // Both workers push some data to worker 0.
        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (mut pushers1, _pullers1) = set.take_pair(1).unwrap();

        // Worker 0 pushes to self (target 0).
        pushers0[0].push(Envelope::data(0u64, vec![100])).unwrap();
        // Worker 1 pushes to worker 0 (target 0).
        pushers1[0].push(Envelope::data(0u64, vec![200])).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes);

        // Round-robin: first from source 0, then source 1.
        let first = pull.pull().unwrap();
        let second = pull.pull().unwrap();
        assert!(pull.pull().is_none());

        // One should be [100], the other [200].
        let (_, d1) = first.as_data().unwrap();
        let (_, d2) = second.as_data().unwrap();
        let mut values: Vec<i32> = vec![d1[0], d2[0]];
        values.sort();
        assert_eq!(values, vec![100, 200]);
    }

    #[test]
    fn exchange_push_close_exhausts_pull() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (pushers0, pullers0) = set.take_pair(0).unwrap();
        let (pushers1, _pullers1) = set.take_pair(1).unwrap();

        let exchange_fn = ExchangeFn::new("id", |x: &i32| *x as u64);
        let mut push0 = ExchangePush::new(pushers0, exchange_fn.clone(), wakes.clone());
        let mut push1 = ExchangePush::new(pushers1, exchange_fn, wakes.clone());

        let pull0 = ExchangePull::new(pullers0, wakes);
        assert!(!pull0.is_exhausted());

        push0.close();
        push1.close();

        assert!(pull0.is_exhausted());
    }

    #[test]
    fn exchange_push_try_push_backpressure() {
        // Capacity 1 — second push should fail.
        let mut set = ExchangeChannelSet::<u64, i32>::new(1, 1);
        let wakes = Arc::new(SharedWakeRegistry::new(1));

        let (pushers, _pullers) = set.take_pair(0).unwrap();
        let exchange_fn = ExchangeFn::new("id", |x: &i32| *x as u64);
        let mut push = ExchangePush::new(pushers, exchange_fn, wakes);

        // First push succeeds.
        push.push(Envelope::data(0u64, vec![1])).unwrap();

        // Second push should fail with backpressure.
        let result = push.try_push(Envelope::data(0u64, vec![2]));
        assert!(result.is_err());
        let (err, env) = result.unwrap_err();
        assert!(matches!(err, Error::Backpressure));
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![2]);
    }

    #[test]
    fn exchange_drain_into_all_sources() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (mut pushers1, _pullers1) = set.take_pair(1).unwrap();

        // Both workers send to worker 0.
        pushers0[0].push(Envelope::data(0u64, vec![1, 2])).unwrap();
        pushers1[0].push(Envelope::data(0u64, vec![3, 4])).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes);
        let mut buffer = Vec::new();
        let count = pull.drain_into(&mut buffer);
        assert_eq!(count, 2);

        let mut all_data: Vec<i32> = buffer
            .into_iter()
            .flat_map(|e| {
                let (_, data) = e.as_data().unwrap();
                data.clone()
            })
            .collect();
        all_data.sort();
        assert_eq!(all_data, vec![1, 2, 3, 4]);
    }

    // -------------------------------------------------------------------
    // FrontierAggregator tests
    // -------------------------------------------------------------------

    #[test]
    fn frontier_aggregator_initial_frontier() {
        let agg = FrontierAggregator::<u64>::new(3);
        // Initial frontier = {T::minimum()} = {0}
        assert_eq!(agg.frontier().elements(), &[0u64]);
    }

    #[test]
    fn frontier_aggregator_single_source_advance() {
        let mut agg = FrontierAggregator::<u64>::new(1);
        // Source 0 advances from minimum(0) to 5.
        let emitted = agg.update(0, 5);
        // Frontier should now be {5}.
        assert_eq!(agg.frontier().elements(), &[5u64]);
        assert_eq!(emitted, vec![5]);
    }

    #[test]
    fn frontier_aggregator_two_sources_staggered() {
        let mut agg = FrontierAggregator::<u64>::new(2);
        // Initial frontier = {0} (both at minimum).

        // Source 0 advances to 5 — but source 1 is still at 0.
        let emitted = agg.update(0, 5);
        assert_eq!(agg.frontier().elements(), &[0u64]); // held back by source 1
        assert!(emitted.is_empty());

        // Source 1 advances to 3 — overall frontier should be {3}.
        let emitted = agg.update(1, 3);
        assert_eq!(agg.frontier().elements(), &[3u64]);
        assert_eq!(emitted, vec![3]);

        // Source 1 advances to 7 — overall frontier should be {5} (source 0 = 5).
        let emitted = agg.update(1, 7);
        assert_eq!(agg.frontier().elements(), &[5u64]);
        assert_eq!(emitted, vec![5]);

        // Source 0 advances to 7 — overall frontier should be {7}.
        let emitted = agg.update(0, 7);
        assert_eq!(agg.frontier().elements(), &[7u64]);
        assert_eq!(emitted, vec![7]);
    }

    #[test]
    fn frontier_aggregator_all_close() {
        let mut agg = FrontierAggregator::<u64>::new(2);

        // Both advance to 10 — only the second triggers emission.
        let e1 = agg.update(0, 10);
        let e2 = agg.update(1, 10);
        assert!(e1.is_empty());
        assert_eq!(e2, vec![10]);
        assert_eq!(agg.frontier().elements(), &[10u64]);
    }

    // -------------------------------------------------------------------
    // ExchangePull frontier aggregation integration tests
    // -------------------------------------------------------------------

    #[test]
    fn exchange_pull_aggregates_watermarks() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (mut pushers1, _pullers1) = set.take_pair(1).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes.clone());

        // Source 0 sends watermark(5) to worker 0.
        pushers0[0].push(Envelope::watermark(5u64)).unwrap();
        // Should NOT emit — source 1 hasn't advanced, so frontier stays at 0.
        // The watermark is absorbed by the aggregator without advancing the frontier.
        assert!(pull.pull().is_none());

        // Source 1 sends watermark(3) to worker 0.
        pushers1[0].push(Envelope::watermark(3u64)).unwrap();
        // Now both have advanced: min(5, 3) = 3. Should emit watermark(3).
        let env = pull.pull().unwrap();
        match env.payload {
            Payload::Control(ControlSignal::Watermark(t)) => assert_eq!(t, 3),
            _ => panic!("expected aggregated watermark(3), got {:?}", env.payload),
        }

        // No more messages.
        assert!(pull.pull().is_none());
    }

    #[test]
    fn exchange_pull_passes_data_through() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (_pushers1, _pullers1) = set.take_pair(1).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes.clone());

        // Data passes through unchanged.
        pushers0[0].push(Envelope::data(1u64, vec![42])).unwrap();
        let env = pull.pull().unwrap();
        let (t, d) = env.as_data().unwrap();
        assert_eq!(*t, 1);
        assert_eq!(d, &vec![42]);
    }

    #[test]
    fn exchange_pull_errors_pass_through_immediately() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (_pushers1, _pullers1) = set.take_pair(1).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes.clone());

        // Error passes through immediately without aggregation.
        pushers0[0]
            .push(Envelope::error("op1", "test error"))
            .unwrap();
        let env = pull.pull().unwrap();
        match &env.payload {
            Payload::Control(ControlSignal::Error { message, .. }) => {
                assert_eq!(message, "test error");
            }
            _ => panic!("expected error, got {:?}", env.payload),
        }
    }

    #[test]
    fn exchange_pull_drain_aggregates_watermarks() {
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (mut pushers1, _pullers1) = set.take_pair(1).unwrap();

        // Source 0: data + watermark(10)
        pushers0[0].push(Envelope::data(1u64, vec![1])).unwrap();
        pushers0[0].push(Envelope::watermark(10u64)).unwrap();
        // Source 1: watermark(7)
        pushers1[0].push(Envelope::watermark(7u64)).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes.clone());
        let mut buffer = Vec::new();
        let count = pull.drain_into(&mut buffer);

        // Should have: 1 data + 1 aggregated watermark(7).
        // Source 0 watermark(10) + source 1 watermark(7) → frontier = {7}.
        let data_count = buffer.iter().filter(|e| e.as_data().is_some()).count();
        let watermark_count = buffer
            .iter()
            .filter(|e| matches!(e.payload, Payload::Control(ControlSignal::Watermark(_))))
            .count();

        assert_eq!(data_count, 1);
        assert_eq!(watermark_count, 1);
        assert_eq!(count, 2);

        // Verify the watermark value.
        let wm = buffer
            .iter()
            .find_map(|e| match &e.payload {
                Payload::Control(ControlSignal::Watermark(t)) => Some(*t),
                _ => None,
            })
            .unwrap();
        assert_eq!(wm, 7);
    }

    #[test]
    fn exchange_pull_data_behind_absorbed_watermark() {
        // Regression test: pull() must not return None when data is queued
        // behind an absorbed watermark. Source 0 sends [watermark(5), data],
        // source 1 hasn't advanced → watermark absorbed, but data must still
        // be returned in the same pull() call.
        let mut set = ExchangeChannelSet::<u64, i32>::new(2, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(2));

        let (mut pushers0, pullers0) = set.take_pair(0).unwrap();
        let (_pushers1, _pullers1) = set.take_pair(1).unwrap();

        let mut pull = ExchangePull::new(pullers0, wakes.clone());

        // Source 0: watermark(5) followed by data.
        pushers0[0].push(Envelope::watermark(5u64)).unwrap();
        pushers0[0].push(Envelope::data(6u64, vec![99])).unwrap();

        // pull() should absorb the watermark (source 1 still at 0) and
        // then return the data — NOT None.
        let env = pull.pull().unwrap();
        let (t, d) = env.as_data().unwrap();
        assert_eq!(*t, 6);
        assert_eq!(d, &vec![99]);
    }

    // -------------------------------------------------------------------
    // M×N Asymmetric channel tests
    // -------------------------------------------------------------------

    #[test]
    fn asymmetric_channel_set_creation() {
        // 2 sources, 3 targets
        let set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);
        assert_eq!(set.num_sources, 2);
        assert_eq!(set.num_targets, 3);
        assert_eq!(set.push_ends.len(), 2); // M rows
        assert_eq!(set.push_ends[0].len(), 3); // N columns
        assert_eq!(set.pull_ends.len(), 2); // M rows
        assert_eq!(set.pull_ends[0].len(), 3); // N columns
    }

    #[test]
    fn asymmetric_take_source_endpoints() {
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);

        // Source 0 gets 3 push endpoints (one per target)
        let pushers = set.take_source_endpoints(0).unwrap();
        assert_eq!(pushers.len(), 3);

        // Source 1 gets 3 push endpoints
        let pushers1 = set.take_source_endpoints(1).unwrap();
        assert_eq!(pushers1.len(), 3);

        // Taking again should fail
        assert!(set.take_source_endpoints(0).is_err());
    }

    #[test]
    fn asymmetric_take_target_endpoints() {
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);

        // Target 0 gets 2 pull endpoints (one per source)
        let pullers = set.take_target_endpoints(0).unwrap();
        assert_eq!(pullers.len(), 2);

        // Target 1 and 2
        let pullers1 = set.take_target_endpoints(1).unwrap();
        assert_eq!(pullers1.len(), 2);
        let pullers2 = set.take_target_endpoints(2).unwrap();
        assert_eq!(pullers2.len(), 2);

        // Taking again should fail
        assert!(set.take_target_endpoints(0).is_err());
    }

    #[test]
    fn asymmetric_data_flows_source_to_target() {
        // 2 sources → 3 targets
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);

        let mut pushers0 = set.take_source_endpoints(0).unwrap();
        let mut pushers1 = set.take_source_endpoints(1).unwrap();
        let mut pullers0 = set.take_target_endpoints(0).unwrap();
        let mut pullers1 = set.take_target_endpoints(1).unwrap();
        let mut pullers2 = set.take_target_endpoints(2).unwrap();

        // Source 0 pushes to target 1
        pushers0[1].push(Envelope::data(0u64, vec![42])).unwrap();
        // Source 1 pushes to target 2
        pushers1[2].push(Envelope::data(0u64, vec![99])).unwrap();

        // Target 1 pulls from source 0
        let env = pullers1[0].pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![42]);

        // Target 2 pulls from source 1
        let env = pullers2[1].pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![99]);

        // Target 0 should have nothing
        assert!(pullers0[0].pull().is_none());
        assert!(pullers0[1].pull().is_none());
    }

    #[test]
    fn asymmetric_exchange_push_routes_to_targets() {
        // 2 sources, 3 targets. Exchange routes by hash % 3.
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);
        let wakes = Arc::new(SharedWakeRegistry::new(3));

        let pushers = set.take_source_endpoints(0).unwrap();
        let _pushers1 = set.take_source_endpoints(1).unwrap();
        let mut pullers0 = set.take_target_endpoints(0).unwrap();
        let mut pullers1 = set.take_target_endpoints(1).unwrap();
        let mut pullers2 = set.take_target_endpoints(2).unwrap();

        let exchange_fn = ExchangeFn::new("mod3", |x: &i32| *x as u64);
        let mut push = ExchangePush::new(pushers, exchange_fn, wakes);

        // Push [0, 1, 2, 3, 4, 5]
        // 0%3=0, 1%3=1, 2%3=2, 3%3=0, 4%3=1, 5%3=2
        push.push(Envelope::data(0u64, vec![0, 1, 2, 3, 4, 5]))
            .unwrap();

        // Target 0 (from source 0): [0, 3]
        let env = pullers0[0].pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![0, 3]);

        // Target 1 (from source 0): [1, 4]
        let env = pullers1[0].pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![1, 4]);

        // Target 2 (from source 0): [2, 5]
        let env = pullers2[0].pull().unwrap();
        let (_, data) = env.as_data().unwrap();
        assert_eq!(data, &vec![2, 5]);
    }

    #[test]
    fn asymmetric_out_of_range_errors() {
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);

        // Source index out of range
        assert!(set.take_source_endpoints(2).is_err());
        assert!(set.take_source_endpoints(5).is_err());

        // Target index out of range
        assert!(set.take_target_endpoints(3).is_err());
        assert!(set.take_target_endpoints(10).is_err());
    }

    #[test]
    #[should_panic(expected = "take_pair requires symmetric channel")]
    fn take_pair_panics_on_asymmetric() {
        let mut set = ExchangeChannelSet::<u64, i32>::new_asymmetric(2, 3, 16);
        let _ = set.take_pair(0);
    }
}
