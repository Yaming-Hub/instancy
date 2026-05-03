//! Exchange channel — cross-worker data routing for multi-worker dataflows.
//!
//! This module provides the **physical transport** for exchange edges. A single
//! logical exchange edge in the dataflow graph (declared via [`Pipe::exchange`])
//! is materialized into an N×N matrix of **physical** in-process bounded SPSC
//! channels — one per (source worker, target worker) pair. These are real
//! data-carrying channels, not graph-level abstractions.
//!
//! Each source worker's [`ExchangePush`] partitions and routes records across
//! the physical channels based on a hash function. Each target worker's
//! [`ExchangePull`] merges data from its N physical input channels using
//! round-robin.
//!
//! # Short-term implementation
//!
//! This is an **intra-process-only** implementation that uses concrete
//! `BoundedPush`/`BoundedPull` (shared-memory) channels. In the future,
//! `ExchangePush` and `ExchangePull` will be refactored to hold
//! `Vec<Box<dyn Push/Pull>>` so that the **runtime** (not the dataflow) decides
//! the physical transport per worker pair — local workers get bounded channels,
//! remote workers get network-backed channels via `TransportProvider` (see
//! DESIGN.md §4.5). The dataflow layer should be agnostic to worker placement.
//!
//! # Wake semantics
//!
//! Bounded channels in the matrix are created without per-channel wake handles.
//! Instead, a [`SharedWakeRegistry`] provides bidirectional wake-up:
//! - **Push→target**: when worker i pushes to worker j, worker j is woken
//! - **Pull→source**: when worker j pulls (freeing capacity), worker i is woken
//!   so a backpressured sender can retry
//! - **Close→all**: when a push end closes, all target workers are woken

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::dataflow::channels::bounded::{bounded_channel, BoundedPull, BoundedPush};
use crate::dataflow::channels::envelope::{ControlSignal, Envelope, Payload};
use crate::dataflow::channels::pact::ExchangeFn;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::wake::WakeHandle;
use crate::error::{Error, Result};
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
        let handles = (0..num_workers)
            .map(|_| Mutex::new(None))
            .collect();
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
/// This implementation uses in-process [`BoundedPush`]/[`BoundedPull`] pairs
/// backed by shared memory (`Arc<Mutex>`), so **all N workers must reside in
/// the same OS process**. For cross-node (distributed) exchange, the matrix
/// entries for remote worker pairs would be replaced with network-backed
/// [`Push`]/[`Pull`] implementations using the `communication::codec` and
/// `communication::connection_pool` layers. The [`ExchangePush`] and
/// [`ExchangePull`] wrappers are transport-agnostic — they depend only on
/// the `Push`/`Pull` trait interface and would work unchanged with remote
/// transports.
///
/// # N×N assumption
///
/// Currently assumes the same number of source and target workers (N×N),
/// because `spawn_multi` creates a single group where all workers share the
/// same topology. A future N×M variant will be needed when per-region
/// parallelism is supported (e.g., region A with 4 workers exchanging into
/// region B with 2 workers).
///
/// Created once per exchange edge during `spawn_multi`. Each worker
/// calls [`take_pair`] to get its `ExchangePush` and `ExchangePull`.
pub(crate) struct ExchangeChannelSet<T: Timestamp, D> {
    num_workers: usize,
    /// push_ends\[src\]\[dst\] — source worker pushes to destination worker.
    push_ends: Vec<Vec<Option<BoundedPush<T, D, ()>>>>,
    /// pull_ends\[src\]\[dst\] — destination worker pulls from source worker.
    pull_ends: Vec<Vec<Option<BoundedPull<T, D, ()>>>>,
}

impl<T: Timestamp, D: Send + 'static> ExchangeChannelSet<T, D> {
    /// Create an N×N matrix of bounded channels.
    ///
    /// Channels are created without wake handles — waking is handled
    /// externally by [`SharedWakeRegistry`] through [`ExchangePush`] and
    /// [`ExchangePull`].
    pub fn new(num_workers: usize, capacity: usize) -> Self {
        let mut push_ends = Vec::with_capacity(num_workers);
        let mut pull_ends = Vec::with_capacity(num_workers);

        for _src in 0..num_workers {
            let mut src_pushers = Vec::with_capacity(num_workers);
            let mut src_pullers = Vec::with_capacity(num_workers);
            for _dst in 0..num_workers {
                let (push, pull) = bounded_channel::<T, D, ()>(capacity);
                src_pushers.push(Some(push));
                src_pullers.push(Some(pull));
            }
            push_ends.push(src_pushers);
            pull_ends.push(src_pullers);
        }

        Self {
            num_workers,
            push_ends,
            pull_ends,
        }
    }

    /// Take the Push/Pull endpoints for a specific worker.
    ///
    /// Worker `idx` gets:
    /// - Push ends: `push_ends[idx][0..N]` (sends to all target workers)
    /// - Pull ends: `pull_ends[0..N][idx]` (receives from all source workers)
    ///
    /// Each worker's pair can only be taken once.
    pub fn take_pair(
        &mut self,
        worker_idx: usize,
    ) -> Result<(Vec<BoundedPush<T, D, ()>>, Vec<BoundedPull<T, D, ()>>)> {
        if worker_idx >= self.num_workers {
            return Err(Error::Custom(format!(
                "worker index {worker_idx} out of range (num_workers={})",
                self.num_workers
            )));
        }

        // Take row of pushers: push_ends[worker_idx][*]
        let mut pushers = Vec::with_capacity(self.num_workers);
        for dst in 0..self.num_workers {
            let push = self.push_ends[worker_idx][dst].take().ok_or_else(|| {
                Error::Custom(format!(
                    "push end [{worker_idx}][{dst}] already taken"
                ))
            })?;
            pushers.push(push);
        }

        // Take column of pullers: pull_ends[*][worker_idx]
        let mut pullers = Vec::with_capacity(self.num_workers);
        for src in 0..self.num_workers {
            let pull = self.pull_ends[src][worker_idx].take().ok_or_else(|| {
                Error::Custom(format!(
                    "pull end [{src}][{worker_idx}] already taken"
                ))
            })?;
            pullers.push(pull);
        }

        Ok((pushers, pullers))
    }
}

// ---------------------------------------------------------------------------
// ExchangePush
// ---------------------------------------------------------------------------

/// Push endpoint for an exchange channel.
///
/// Routes each record in a batch to the appropriate target worker based on
/// the exchange hash function. After pushing, wakes the target worker's
/// executor via [`SharedWakeRegistry`].
///
/// # Transport abstraction (future work)
///
/// Currently holds concrete `BoundedPush` endpoints (in-process shared memory).
/// A future refactor will change `targets` to `Vec<Box<dyn Push<T, D>>>` so
/// the runtime can mix local (bounded channel) and remote (network) transports
/// per target worker. The dataflow layer should not know whether a target
/// worker is local or remote — that is a physical concern owned by the runtime.
pub struct ExchangePush<T: Timestamp, D: Send + 'static> {
    /// One push endpoint per target worker.
    targets: Vec<BoundedPush<T, D, ()>>,
    /// Hash function for routing records.
    exchange_fn: ExchangeFn<D>,
    /// Number of target workers.
    num_workers: usize,
    /// Shared wake registry for cross-worker notification.
    wakes: Arc<SharedWakeRegistry>,
    /// Whether this push endpoint has been closed.
    closed: bool,
}

impl<T: Timestamp, D: Send + 'static> ExchangePush<T, D> {
    /// Create a new exchange push endpoint.
    pub(crate) fn new(
        targets: Vec<BoundedPush<T, D, ()>>,
        exchange_fn: ExchangeFn<D>,
        wakes: Arc<SharedWakeRegistry>,
    ) -> Self {
        let num_workers = targets.len();
        Self {
            targets,
            exchange_fn,
            num_workers,
            wakes,
            closed: false,
        }
    }
}

impl<T: Timestamp, D: Clone + Send + 'static> Push<T, D> for ExchangePush<T, D> {
    fn push(&mut self, envelope: Envelope<T, D, ()>) -> Result<()> {
        if self.closed {
            return Err(Error::ChannelClosed);
        }

        match envelope.payload {
            Payload::Data { time, data } => {
                // Partition records by target worker.
                let mut buckets: Vec<Vec<D>> =
                    (0..self.num_workers).map(|_| Vec::new()).collect();
                for record in data {
                    let hash = self.exchange_fn.route(&record);
                    let target = ExchangeFn::<D>::target_worker(hash, self.num_workers);
                    buckets[target].push(record);
                }

                // Push non-empty buckets and wake target workers.
                for (target_idx, bucket) in buckets.into_iter().enumerate() {
                    if !bucket.is_empty() {
                        self.targets[target_idx].push(Envelope::data(
                            time.clone(),
                            bucket,
                        ))?;
                        self.wakes.wake(target_idx);
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
                // Partition records by target worker.
                let mut buckets: Vec<Vec<D>> =
                    (0..self.num_workers).map(|_| Vec::new()).collect();
                for record in data {
                    let hash = self.exchange_fn.route(record);
                    let target = ExchangeFn::<D>::target_worker(hash, self.num_workers);
                    buckets[target].push(record.clone());
                }

                // Pre-check ALL targets: closed or insufficient capacity.
                // This avoids partial delivery — we only push if all targets
                // can accept their sub-batch.
                for (target_idx, bucket) in buckets.iter().enumerate() {
                    if bucket.is_empty() {
                        continue;
                    }
                    if self.targets[target_idx].is_closed() {
                        return Err((Error::ChannelClosed, envelope));
                    }
                    let available = self.targets[target_idx].capacity()
                        .saturating_sub(self.targets[target_idx].len());
                    if available == 0 {
                        return Err((Error::Backpressure, envelope));
                    }
                }

                // All targets have capacity — push sub-batches.
                for (target_idx, bucket) in buckets.into_iter().enumerate() {
                    if !bucket.is_empty() {
                        // Pre-check passed, so this should succeed. If it
                        // doesn't (e.g., concurrent access in future), we
                        // accept partial delivery as a last resort.
                        match self.targets[target_idx].try_push(Envelope::data(
                            time.clone(),
                            bucket,
                        )) {
                            Ok(()) => self.wakes.wake(target_idx),
                            Err((err, _sub)) => {
                                return Err((err, envelope));
                            }
                        }
                    }
                }
            }
            Payload::Control(signal) => {
                // Pre-check: all targets must have capacity for control signal.
                for target in self.targets.iter() {
                    if target.is_closed() {
                        return Err((Error::ChannelClosed, envelope));
                    }
                    let available = target.capacity().saturating_sub(target.len());
                    if available == 0 {
                        return Err((Error::Backpressure, envelope));
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
            Push::<T, D>::close(target);
        }
        self.wakes.wake_all();
    }
}

// ---------------------------------------------------------------------------
// ExchangePull
// ---------------------------------------------------------------------------

/// Pull endpoint for an exchange channel.
///
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
        debug_assert!(num_sources > 0, "FrontierAggregator requires at least one source");

        let mut aggregated = MutableAntichain::new();
        // Each source contributes T::minimum() with count +1.
        let inits: Vec<(T, i64)> = (0..num_sources)
            .map(|_| (T::minimum(), 1))
            .collect();
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

        let changes = self.aggregated.update_iter(vec![
            (old_time, -1),
            (new_watermark, 1),
        ]);

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
/// Instead, a [`FrontierAggregator`] tracks per-source watermarks and only emits
/// an aggregated watermark downstream when ALL sources have advanced past a
/// timestamp. This prevents premature frontier advancement that could cause
/// operators to incorrectly discard data or deadlock.
///
/// Error control signals pass through immediately (they are not aggregated).
///
/// # Transport abstraction (future work)
///
/// Currently holds concrete `BoundedPull` endpoints. Will be refactored to
/// `Vec<Box<dyn Pull<T, D>>>` so the runtime can provide local or remote
/// transports per source worker transparently.
pub struct ExchangePull<T: Timestamp, D> {
    /// One pull endpoint per source worker.
    sources: Vec<BoundedPull<T, D, ()>>,
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
        sources: Vec<BoundedPull<T, D, ()>>,
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

/// Type-erased creator for exchange channel factories.
///
/// Created by the builder (which knows T, D, and ExchangeFn), stored on
/// [`LogicalDataflow`], and consumed by `spawn_multi` to produce N shared
/// channel factories — one per worker — that all reference the same
/// underlying N×N channel matrix.
pub(crate) type ExchangeFactoryCreatorFn =
    Box<dyn FnOnce(usize, usize) -> Vec<super::super::schedulable::ChannelFactory> + Send>;

/// Create a type-erased exchange factory creator.
///
/// The returned closure captures the `ExchangeFn<D>` and concrete types
/// `T`, `D`. When called with `(num_workers, capacity)`, it produces
/// N channel factories that share an underlying [`ExchangeChannelSet`].
pub(crate) fn create_exchange_factory_creator<T, D>(
    exchange_fn: ExchangeFn<D>,
) -> ExchangeFactoryCreatorFn
where
    T: Timestamp,
    D: Clone + Send + 'static,
{
    Box::new(move |num_workers: usize, capacity: usize| {
        let wakes = Arc::new(SharedWakeRegistry::new(num_workers));
        let channel_set = Arc::new(Mutex::new(ExchangeChannelSet::<T, D>::new(
            num_workers, capacity,
        )));

        (0..num_workers)
            .map(|_| {
                let wakes = wakes.clone();
                let channel_set = channel_set.clone();
                let exchange_fn = exchange_fn.clone();
                super::super::schedulable::channel_factory(
                    move |ctx: &crate::worker::WorkerContext,
                          _cap: usize,
                          wake: Option<WakeHandle>| {
                        wakes.register(ctx.worker_index(), wake);

                        let (pushers, pullers) = channel_set
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .take_pair(ctx.worker_index())
                            .unwrap_or_else(|e| {
                                // Materialization-time invariant: each worker's pair
                                // is taken exactly once. This should never fail if
                                // spawn_multi assigns one factory per worker correctly.
                                panic!("exchange channel take_pair failed during materialization: {e}")
                            });

                        let push = ExchangePush::new(pushers, exchange_fn.clone(), wakes.clone());
                        let pull = ExchangePull::new(pullers, wakes.clone());

                        (
                            Box::new(Box::new(push) as Box<dyn Push<T, D>>)
                                as Box<dyn std::any::Any + Send>,
                            Box::new(Box::new(pull) as Box<dyn Pull<T, D>>)
                                as Box<dyn std::any::Any + Send>,
                        )
                    },
                )
            })
            .collect()
    })
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
        assert_eq!(set.num_workers, 3);
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
        pushers0[0]
            .push(Envelope::data(0u64, vec![100]))
            .unwrap();
        // Worker 1 pushes to worker 0 (target 0).
        pushers1[0]
            .push(Envelope::data(0u64, vec![200]))
            .unwrap();

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
        pushers0[0]
            .push(Envelope::data(0u64, vec![1, 2]))
            .unwrap();
        pushers1[0]
            .push(Envelope::data(0u64, vec![3, 4]))
            .unwrap();

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
        pushers0[0]
            .push(Envelope::watermark(5u64))
            .unwrap();
        // Should NOT emit — source 1 hasn't advanced, so frontier stays at 0.
        // The watermark is absorbed by the aggregator without advancing the frontier.
        assert!(pull.pull().is_none());

        // Source 1 sends watermark(3) to worker 0.
        pushers1[0]
            .push(Envelope::watermark(3u64))
            .unwrap();
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
        pushers0[0]
            .push(Envelope::data(1u64, vec![42]))
            .unwrap();
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
        pushers0[0]
            .push(Envelope::data(1u64, vec![1]))
            .unwrap();
        pushers0[0]
            .push(Envelope::watermark(10u64))
            .unwrap();
        // Source 1: watermark(7)
        pushers1[0]
            .push(Envelope::watermark(7u64))
            .unwrap();

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
        pushers0[0]
            .push(Envelope::watermark(5u64))
            .unwrap();
        pushers0[0]
            .push(Envelope::data(6u64, vec![99]))
            .unwrap();

        // pull() should absorb the watermark (source 1 still at 0) and
        // then return the data — NOT None.
        let env = pull.pull().unwrap();
        let (t, d) = env.as_data().unwrap();
        assert_eq!(*t, 6);
        assert_eq!(d, &vec![99]);
    }
}
