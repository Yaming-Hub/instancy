//! Exchange channel — cross-worker data routing for multi-worker dataflows.
//!
//! An exchange channel connects N source workers to N target workers via an
//! N×N matrix of bounded channels. Each source worker's [`ExchangePush`]
//! routes records to target workers based on a hash function. Each target
//! worker's [`ExchangePull`] merges data from all source workers using
//! round-robin.
//!
//! # Wake semantics
//!
//! Bounded channels in the matrix are created without per-channel wake handles.
//! Instead, a [`SharedWakeRegistry`] provides bidirectional wake-up:
//! - **Push→target**: when worker i pushes to worker j, worker j is woken
//! - **Pull→source**: when worker j pulls (freeing capacity), worker i is woken
//!   so a backpressured sender can retry
//! - **Close→all**: when a push end closes, all target workers are woken

use std::sync::{Arc, Mutex};

use crate::dataflow::channels::bounded::{bounded_channel, BoundedPull, BoundedPush};
use crate::dataflow::channels::envelope::{Envelope, Payload};
use crate::dataflow::channels::pact::ExchangeFn;
use crate::dataflow::channels::pushpull::{Pull, Push};
use crate::dataflow::channels::wake::WakeHandle;
use crate::error::{Error, Result};
use crate::progress::timestamp::Timestamp;

// ---------------------------------------------------------------------------
// SharedWakeRegistry
// ---------------------------------------------------------------------------

/// Registry of per-worker wake handles for cross-worker notification.
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
/// Merges data from all source workers using round-robin polling.
/// After pulling (which frees channel capacity), wakes the source
/// worker so a backpressured sender can retry.
pub struct ExchangePull<T: Timestamp, D> {
    /// One pull endpoint per source worker.
    sources: Vec<BoundedPull<T, D, ()>>,
    /// Round-robin index for fair merging.
    next_source: usize,
    /// Number of source workers.
    num_sources: usize,
    /// Shared wake registry for notifying source workers on capacity freed.
    wakes: Arc<SharedWakeRegistry>,
}

impl<T: Timestamp, D> ExchangePull<T, D> {
    /// Create a new exchange pull endpoint.
    pub(crate) fn new(
        sources: Vec<BoundedPull<T, D, ()>>,
        wakes: Arc<SharedWakeRegistry>,
    ) -> Self {
        let num_sources = sources.len();
        Self {
            sources,
            next_source: 0,
            num_sources,
            wakes,
        }
    }
}

impl<T: Timestamp, D: Send + 'static> Pull<T, D> for ExchangePull<T, D> {
    fn pull(&mut self) -> Option<Envelope<T, D, ()>> {
        // Round-robin: try each source starting from next_source.
        for _ in 0..self.num_sources {
            let idx = self.next_source;
            self.next_source = (self.next_source + 1) % self.num_sources;
            if let Some(env) = self.sources[idx].pull() {
                // Wake the source worker — we freed capacity, so a
                // backpressured sender can retry.
                self.wakes.wake(idx);
                return Some(env);
            }
        }
        None
    }

    fn drain_into(&mut self, buffer: &mut Vec<Envelope<T, D, ()>>) -> usize {
        let mut count = 0;
        // Drain round-robin until no source has data.
        loop {
            let mut found_any = false;
            for src_idx in 0..self.num_sources {
                if let Some(env) = self.sources[src_idx].pull() {
                    buffer.push(env);
                    count += 1;
                    found_any = true;
                    self.wakes.wake(src_idx);
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
}
