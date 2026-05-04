//! # Epoch Statistics — Multi-Epoch Frontier-Based Aggregation
//!
//! Demonstrates `unary_notify` with **multiple timestamps (epochs)** to compute
//! per-epoch statistics (sum, count, min, max, mean) for streaming numeric data.
//!
//! ## Why this matters
//!
//! In a streaming system, data for the same epoch may arrive in multiple batches
//! (from different upstream workers, or because the source sends data incrementally).
//! The operator needs to buffer partial results and emit final statistics **exactly
//! once** when the epoch is complete — i.e., when the input frontier advances past
//! that epoch's timestamp.
//!
//! This is the standard "aggregate on frontier advance" pattern, and `unary_notify`
//! makes it clean:
//!
//! ```text
//! Epoch 0: batch1=[10, 20, 30], batch2=[40, 50]   →  Stats { sum=150, count=5, min=10, max=50 }
//! Epoch 1: batch1=[5, 15], batch2=[25], batch3=[35, 45] → Stats { sum=125, count=5, min=5, max=45 }
//! Epoch 2: batch1=[100, 200, 300, 400]              →  Stats { sum=1000, count=4, min=100, max=400 }
//! ```
//!
//! Without notifications, the operator would have to guess when to emit (e.g., emit
//! on every activation, producing redundant partial results). With `unary_notify`,
//! the operator buffers data, and the framework tells it exactly when each epoch is
//! complete via a notification callback.
//!
//! ## Note on frontier behavior in this example
//!
//! The preloaded source operator holds a single capability at `T::minimum()` and
//! drops it only when all data is emitted. This means the input frontier advances
//! past ALL epochs at once (when the source finishes), so all notifications fire
//! together. In a real streaming system with incremental frontier advancement
//! (e.g., using `AsyncInputSender::advance_to()`), notifications would fire
//! per-epoch as each epoch completes. The aggregation pattern is identical either
//! way — the operator correctly buffers and emits per-epoch regardless of when
//! notifications fire.
//!
//! Run with: `cargo run --example notify_epoch_stats`

use std::collections::HashMap;
use std::fmt;

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle};

/// Per-epoch running statistics, maintained incrementally as data arrives.
#[derive(Clone)]
struct EpochStats {
    sum: i64,
    count: u64,
    min: i64,
    max: i64,
}

impl EpochStats {
    fn new() -> Self {
        Self {
            sum: 0,
            count: 0,
            min: i64::MAX,
            max: i64::MIN,
        }
    }

    /// Incorporate a new value into the running statistics.
    fn absorb(&mut self, value: i64) {
        self.sum += value;
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Compute the mean (only valid when count > 0).
    fn mean(&self) -> f64 {
        debug_assert!(self.count > 0, "mean() called on empty stats");
        self.sum as f64 / self.count as f64
    }
}

impl fmt::Display for EpochStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "count={}, sum={}, min={}, max={}, mean={:.1}",
            self.count,
            self.sum,
            self.min,
            self.max,
            self.mean()
        )
    }
}

fn main() {
    println!("=== Epoch Statistics (Multi-Epoch Frontier-Based Aggregation) ===\n");

    // --- Input data: 3 epochs, each arriving in multiple batches ---
    //
    // This simulates a realistic streaming scenario where data for the same
    // epoch arrives across multiple activations. The operator must buffer all
    // data for an epoch and only emit when it's certain no more data will arrive.
    let input_batches: Vec<(u64, Vec<i64>)> = vec![
        // Epoch 0: two batches of latency measurements
        (0, vec![10, 20, 30]),
        (0, vec![40, 50]),
        // Epoch 1: three batches (interleaved with epoch 0 above in real streaming)
        (1, vec![5, 15]),
        (1, vec![25]),
        (1, vec![35, 45]),
        // Epoch 2: single batch
        (2, vec![100, 200, 300, 400]),
    ];

    // --- Build the dataflow ---
    let builder = DataflowBuilder::<u64>::new("epoch_stats");
    let port = builder
        .source("latencies", input_batches)
        // Buffer per-epoch data, emit final statistics on frontier advance.
        //
        // The key pattern:
        // 1. On each activation, drain all input into per-epoch accumulators
        // 2. Register notify_at(epoch) so the framework tracks when that epoch is complete
        // 3. When the notification fires (frontier advanced past epoch), emit final stats
        // 4. Remove the epoch's state to free memory
        .unary_notify("compute_stats", {
            let mut accumulators: HashMap<u64, EpochStats> = HashMap::new();
            move |input, output, ctx| {
                // Step 1: Absorb all arriving data into per-epoch accumulators.
                while let Some((epoch, values)) = input.next() {
                    let stats = accumulators.entry(epoch).or_insert_with(EpochStats::new);
                    for &v in &values {
                        stats.absorb(v);
                    }
                    // Step 2: Request notification when this epoch is complete.
                    // If we already requested notification for this epoch, the
                    // notificator deduplicates it. The capability dedup in
                    // NotifyContext ensures only one output capability per epoch.
                    ctx.notify_at(epoch);
                }

                // Step 3: Process all ready notifications (epochs that are complete).
                while let Some(epoch) = ctx.next_notification() {
                    // Step 4: Emit final stats and remove state for this epoch.
                    if let Some(stats) = accumulators.remove(&epoch) {
                        output.push_vec(
                            epoch,
                            vec![format!("epoch {epoch}: {stats}")],
                        );
                    }
                    // After this, the output capability for `epoch` is dropped,
                    // allowing downstream frontiers to advance past it.
                }
                Ok(())
            }
        })
        .output("stats");

    let dataflow = builder.build().unwrap();

    // --- Execute ---
    let config = RuntimeConfig::default();
    let rt = RuntimeHandle::new(config).unwrap();
    rt.run_blocking(dataflow).unwrap();

    // --- Print results ---
    println!("Results:");
    let collector = port.collector();
    let results = collector.lock().unwrap_or_else(|e| e.into_inner());
    // Sort by epoch for deterministic display (notifications may fire in any order).
    let mut all: Vec<(u64, String)> = results
        .iter()
        .flat_map(|(t, vs)| vs.iter().map(move |v| (*t, v.clone())))
        .collect();
    all.sort_by_key(|(epoch, _)| *epoch);

    for (epoch, stats_line) in &all {
        println!("  [t={epoch}] {stats_line}");
    }

    // --- Verify correctness ---
    // Each epoch should produce exactly one output (no duplicates, no missing).
    let epoch_count = all.len();
    assert_eq!(epoch_count, 3, "expected exactly 3 epoch results, got {epoch_count}");

    // Verify epoch 0: values [10, 20, 30, 40, 50] → sum=150, count=5, min=10, max=50
    assert!(all[0].1.contains("count=5"), "epoch 0 count");
    assert!(all[0].1.contains("sum=150"), "epoch 0 sum");
    assert!(all[0].1.contains("min=10"), "epoch 0 min");
    assert!(all[0].1.contains("max=50"), "epoch 0 max");

    // Verify epoch 1: values [5, 15, 25, 35, 45] → sum=125, count=5, min=5, max=45
    assert!(all[1].1.contains("count=5"), "epoch 1 count");
    assert!(all[1].1.contains("sum=125"), "epoch 1 sum");
    assert!(all[1].1.contains("min=5"), "epoch 1 min");
    assert!(all[1].1.contains("max=45"), "epoch 1 max");

    // Verify epoch 2: values [100, 200, 300, 400] → sum=1000, count=4, min=100, max=400
    assert!(all[2].1.contains("count=4"), "epoch 2 count");
    assert!(all[2].1.contains("sum=1000"), "epoch 2 sum");
    assert!(all[2].1.contains("min=100"), "epoch 2 min");
    assert!(all[2].1.contains("max=400"), "epoch 2 max");

    println!("\n✓ All epoch statistics verified — each epoch emitted exactly once!");
}
